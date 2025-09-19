import json
import logging
import os
import asyncio
import time
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from typing import List, Tuple, Optional

from messaging.base import Messenger
from core.scheduler import Scheduler
from core.exchange import Exchange
from core.strategy import (
    ohlcv_to_df,
    detect_trend,
    plan_trade,
    _atr,
    Trend,
)
from core.backtest import _simulate_after_fill
from core.config import settings
from core.orders import register_order_commands

log = logging.getLogger("handlers")
STATE_FILE = os.path.join("logs", "state.json")

DEFAULT_TOP = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT", "XRP/USDT:USDT",
    "DOGE/USDT:USDT", "ADA/USDT:USDT", "TON/USDT:USDT", "TRX/USDT:USDT", "LINK/USDT:USDT",
    "ARB/USDT:USDT", "OP/USDT:USDT", "APT/USDT:USDT", "NEAR/USDT:USDT", "BCH/USDT:USDT",
    "LTC/USDT:USDT", "MATIC/USDT:USDT", "DOT/USDT:USDT", "SUI/USDT:USDT", "ATOM/USDT:USDT",
]

# ----------------- utils -----------------
def _load_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_state(st: dict):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False)

def _is_number(x: str) -> bool:
    try:
        float(str(x).replace("%", "").replace("$", "").replace(" ATR", ""))
        return True
    except Exception:
        return False

def _render_table(headers, rows, align_right=True) -> str:
    headers = [str(h) for h in headers]
    rows = [[("" if c is None else str(c)) for c in r] for r in rows]
    widths = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(c))
    def fmt(r):
        out = []
        for i, c in enumerate(r):
            if align_right and _is_number(c):
                out.append(str(c).rjust(widths[i]))
            else:
                out.append(str(c).ljust(widths[i]))
        return " | ".join(out)
    sep = "-+-".join("-" * w for w in widths)
    return "```\n" + "\n".join([fmt(headers), sep] + [fmt(r) for r in rows]) + "\n```"

def _fetch_ohlcv_safe(ex: Exchange, tf: str, *, limit: int,
                      symbol: str | None = None, attempts: int = 3,
                      sleep_base: float = 0.7, since: int | None = None):
    last_err = None
    for k in range(attempts):
        try:
            return ex.fetch_ohlcv(tf, limit=limit, symbol=symbol, since=since)
        except Exception as e:
            last_err = e
            msg = str(e).lower()
            if any(s in msg for s in ("timeout", "timed out", "market/time", "read operation timed out")) and k < attempts - 1:
                time.sleep(sleep_base * (k + 1))
                continue
            break
    raise last_err

TF_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000
}

def _fmt_ohlc(r):
    return f"O={float(r['o']):.2f}  H={float(r['h']):.2f}  L={float(r['l']):.2f}  C={float(r['c']):.2f}"

# ----------------- universe helpers -----------------
def _build_linear_perp_set(ex: Exchange) -> set:
    try:
        mkts = ex.x.load_markets()
    except Exception as e:
        log.warning("load_markets failed: %s", e)
        mkts = ex.x.markets or {}
    allowed = set()
    for sym, m in mkts.items():
        try:
            if (m.get("swap") is True) and (m.get("linear") in (True, "USDT")) and (m.get("active", True) is True):
                allowed.add(sym.upper())
        except Exception:
            continue
    return allowed

def _sanitize_symbols(symbols: List[str], allowed: set) -> List[str]:
    out = []
    for s in symbols:
        u = s.strip().upper()
        if not u:
            continue
        if u in allowed:
            out.append(u)
        else:
            log.warning("Symbol '%s' skipped (not linear swap / unknown on Bybit)", u)
    base = settings.SYMBOL.upper()
    if base in allowed and base not in out:
        out.append(base)
    uniq = []
    for s in out:
        if s not in uniq:
            uniq.append(s)
    return uniq

def _universe_symbols(ex: Exchange, allowed: set) -> List[str]:
    manual = os.getenv("UNIVERSE_SYMBOLS", "")
    if manual:
        arr = [s.strip().upper() for s in manual.split(",") if s.strip()]
        arr = _sanitize_symbols(arr, allowed)
        if arr:
            return arr
    top_n = int(os.getenv("UNIVERSE_TOP_N", "0") or "0")
    if top_n > 0:
        base = _sanitize_symbols(DEFAULT_TOP[:top_n], allowed)
        if base:
            return base
    return _sanitize_symbols([settings.SYMBOL], allowed)

# ----------------- стоп-логика -----------------
def _tick_size(ex: Exchange, symbol: str) -> float:
    try:
        m = ex.x.market(symbol) or {}
        p = (m.get("precision") or {}).get("price")
        if isinstance(p, int):
            return 10.0 ** (-p)
        info = m.get("info") or {}
        pf = (info.get("priceFilter") or {})
        ts = pf.get("tickSize")
        if ts:
            return float(ts)
    except Exception:
        pass
    return 0.01

def _recalc_sl_tp_with_new_rules(
    ex: Exchange,
    symbol: str,
    sig: dict,
    info: dict,
    h1_df,
    atr_series,
) -> Tuple[float, float, float, str]:
    """
    Правило:
      - если прокол слабый: стоп за прокол на N тиков (STOP_TICKS_BEHIND_PROBE)
      - если прокол сильный (pen_atr_pct >= STRONG_PEN_THRESHOLD_ATR_PCT):
            стоп глубиной MAX_STOP_ATR_PCT * ATR от входа
      - TP по RR
    """
    entry = float(sig["entry"])
    rr = float(settings.RR)

    atr_here = 0.0
    if info["idx"] in atr_series.index:
        atr_here = float(atr_series.loc[info["idx"]])

    probe_price = float(info["probe_price"])
    pen_atr_pct = float(info.get("pen_atr_pct") or 0.0)

    # параметры из .env
    ticks_behind = int(os.getenv("STOP_TICKS_BEHIND_PROBE", "5") or "5")
    max_stop_pct = float(os.getenv("MAX_STOP_ATR_PCT", "0.30") or "0.30")
    strong_thr = float(os.getenv("STRONG_PEN_THRESHOLD_ATR_PCT", "0.30") or "0.30")

    tick = _tick_size(ex, symbol)

    # базовый "probe" стоп
    if sig["side"] == "long":
        sl_probe = probe_price - ticks_behind * tick
        dist_probe = max(0.0, entry - sl_probe)
    else:
        sl_probe = probe_price + ticks_behind * tick
        dist_probe = max(0.0, sl_probe - entry)

    # сильный прокол? -> ATR-стоп
    use_atr_stop = (atr_here > 0.0) and (pen_atr_pct >= strong_thr)
    if use_atr_stop:
        final_dist = atr_here * max_stop_pct
        sl_type = "atr"
    else:
        final_dist = dist_probe
        sl_type = "probe"

    if sig["side"] == "long":
        sl = entry - final_dist
        tp = entry + rr * (entry - sl)
    else:
        sl = entry + final_dist
        tp = entry - rr * (sl - entry)

    # округление по маркету (через CCXT)
    entry = float(ex.x.price_to_precision(symbol, entry))
    sl = float(ex.x.price_to_precision(symbol, sl))
    tp = float(ex.x.price_to_precision(symbol, tp))
    return entry, sl, tp, sl_type

# ----------------- qty (простой риск) -----------------
def _calc_qty_from_risk_linear_usdt(
    ex: Exchange,
    symbol: str,
    free_usdt: float,
    risk_pct: float,
    entry: float,
    sl: float,
) -> float:
    risk_usdt = max(0.0, float(free_usdt) * float(risk_pct))
    dist = abs(float(entry) - float(sl))
    if risk_usdt <= 0 or dist <= 0:
        return 0.0
    qty = risk_usdt / dist
    try:
        qty = float(ex.x.amount_to_precision(symbol, qty))
    except Exception:
        pass
    return max(0.0, qty)

# ----------------- strategy helpers -----------------
def _reason_if_reject(tr_day: Trend, levels, h1_df, info) -> str | None:
    max_back = int(settings.H1_FALSE_BREAKOUT_MAX_CANDLES)
    if info.get("candles_back") is None or info["candles_back"] > max_back:
        return f"candles_back>{max_back}"
    max_retest = int(settings.H1_RETEST_MAX_CANDLES)
    if max_retest > 0:
        idx_back = info["idx"] + (info["candles_back"] or 0)
        level = levels.prev_low if info["side"] == "long" else levels.prev_high
        window = h1_df.loc[idx_back: idx_back + max_retest]
        touched = any((row["l"] <= level <= row["h"]) for _, row in window.iterrows())
        if not touched:
            return f"no retest≤{max_retest} H1"
    pen_limit = float(settings.H1_MAX_PENETRATION_ATR_PCT)
    if info.get("pen_atr_pct") is None:
        return "ATR<=0"
    if float(info["pen_atr_pct"]) > pen_limit:
        return f"pen>MAX_PEN_ATR({pen_limit:.2f})"
    return None

def _detect_today_fakeout_and_signal(d1_df, h4_df, h1_df) -> Tuple[Optional[dict], Optional[dict], Optional[Trend], Optional[SimpleNamespace], Optional[str]]:
    if len(d1_df) < 2:
        return None, None, None, None, "not enough D1"
    i = len(d1_df) - 1
    day_row = d1_df.iloc[i]
    prev_row = d1_df.iloc[i - 1]
    next_ts = int(h1_df["ts"].max()) + 1
    tr_day = detect_trend(d1_df.loc[:i], h4_df[h4_df["ts"] <= next_ts])
    levels = SimpleNamespace(prev_high=float(prev_row["h"]), prev_low=float(prev_row["l"]))
    side = "long" if tr_day.d1 == "up" else "short"
    level = levels.prev_low if side == "long" else levels.prev_high
    h1_day = h1_df[(h1_df["ts"] >= int(day_row["ts"])) & (h1_df["ts"] < next_ts)]
    if h1_day.empty:
        return None, None, tr_day, levels, "no H1 today"
    atr_series = _atr(h1_df, int(settings.ATR_PERIOD_H1))
    idx_list = list(h1_day.index)
    info = None
    for pos, i1 in enumerate(idx_list):
        hi = float(h1_day.loc[i1, "h"]); lo = float(h1_day.loc[i1, "l"])
        broken = (side == "long" and lo < level) or (side == "short" and hi > level)
        if not broken:
            continue
        back_i = None
        for pos2 in range(pos + 1, min(pos + 1 + int(settings.H1_FALSE_BREAKOUT_MAX_CANDLES), len(idx_list))):
            j = idx_list[pos2]; c = float(h1_day.loc[j, "c"])
            if (side == "long" and c >= level) or (side == "short" and c <= level):
                back_i = j; break
        if back_i is None:
            continue
        seg = h1_day.loc[i1: back_i]
        if side == "long":
            probe_price = float(seg["l"].min())
            pen_abs = (level - probe_price)
        else:
            probe_price = float(seg["h"].max())
            pen_abs = (probe_price - level)
        atr = float(atr_series.loc[i1]) if i1 in atr_series.index else 0.0
        pen_atr_pct = (pen_abs / atr) if atr > 0 else 0.0
        info = {
            "side": side,
            "idx": int(i1),
            "came_back": True,
            "candles_back": idx_list.index(back_i) - idx_list.index(i1),
            "pen_atr_pct": float(pen_atr_pct),
            "probe_price": float(probe_price),
            "level": float(level),
        }
        break
    if not info:
        return None, None, tr_day, levels, "no fakeout today"
    rej = _reason_if_reject(tr_day, levels, h1_df, info)
    if rej:
        return info, None, tr_day, levels, rej
    sig = plan_trade(tr_day, levels, h1_df, info)
    if not sig:
        return info, None, tr_day, levels, "build signal fail"
    return info, sig, tr_day, levels, None

# ----------------- handlers -----------------
def register_handlers(m: Messenger, sched: Scheduler):
    ex = Exchange()
    ex.load()
    log.info("Exchange warmed")

    allowed_linear = _build_linear_perp_set(ex)

    st = _load_state()
    st.setdefault("consec_losses", 0)
    st.setdefault("armed", False)
    st.setdefault("symbols", [])
    st.setdefault("last_events", {})       # {symbol: event_id}
    st.setdefault("last_notified_at", {})  # {symbol: epoch_ms}
    st["symbols"] = _sanitize_symbols(st.get("symbols") or [settings.SYMBOL], allowed_linear)
    _save_state(st)

    async def cmd_help(_: str):
        lines = [
            "/help",
            "/param — параметры стратегии",
            "/days [N] — анализ N дней по универcуму (покажет тип стопа)",
            "/run — включить ожидание сигналов",
            "/stop — выключить ожидание",
            "/scheduler_on | /scheduler_off | /scheduler_status",
            "",
            "/order — управление лимитными ордерами c TP/SL (Bybit, перпет.)",
            "  Примеры:",
            "  /order place ETH/USDT:USDT long entry=2410 sl=2380 tp=2480 risk=0.01",
            "  /order place BTC/USDT:USDT short entry=61250 sl=61600 tp=60000 qty=0.05 post=1 tif=GTC",
            "  /order cancel ETH/USDT:USDT",
            "  Параметры: qty=<число> | risk=0..1 (или в процентах: 1=1%), post=0|1, tif=GTC|IOC|FOK, tp_mode=limit|market, sl_mode=market|limit",
            "  Примечание: если qty и risk не заданы — берётся RISK_PCT из .env (по умолчанию 1% депозита)",
        ]
        await m.send_text("\n".join(lines))


    async def cmd_param(_: str):
        try:
            uni = _universe_symbols(ex, allowed_linear)
            params = {
                "UNIVERSE": uni,
                "SYMBOL_BASE": settings.SYMBOL,
                "TF": {"D1": settings.TF_D1, "H4": settings.TF_H4, "H1": settings.TF_H1},
                "TREND": {"D1_SMA": settings.D1_SMA, "H4_FAST": settings.H4_FAST, "H4_SLOW": settings.H4_SLOW, "STRICT_TREND": bool(settings.STRICT_TREND)},
                "FAKEOUT/H1": {"MAX_CANDLES": settings.H1_FALSE_BREAKOUT_MAX_CANDLES, "MAX_PEN_ATR": settings.H1_MAX_PENETRATION_ATR_PCT, "RETEST_MAX_H1": settings.H1_RETEST_MAX_CANDLES},
                "ATR": {"ATR_PERIOD_H1": settings.ATR_PERIOD_H1},
                "ENTRY/SL/TP": {"ENTRY_OFFSET_ATR": settings.ENTRY_OFFSET_ATR_PCT, "RR": settings.RR},
                "Stops": {
                    "STOP_TICKS_BEHIND_PROBE": int(os.getenv("STOP_TICKS_BEHIND_PROBE", "5") or "5"),
                    "MAX_STOP_ATR_PCT": float(os.getenv("MAX_STOP_ATR_PCT", "0.30") or "0.30"),
                    "STRONG_PEN_THRESHOLD_ATR_PCT": float(os.getenv("STRONG_PEN_THRESHOLD_ATR_PCT", "0.30") or "0.30"),
                },
                "Risk": {"RISK_PCT": settings.RISK_PCT, "MAX_CONSECUTIVE_LOSSES": settings.MAX_CONSECUTIVE_LOSSES},
                "Protections": {"ONE_TRADE_PER_DAY": bool(settings.ONE_TRADE_PER_DAY), "DEDUP_BY_BREAKOUT": bool(settings.DEDUP_BY_BREAKOUT), "COOLDOWN_HOURS": settings.COOLDOWN_HOURS},
                "Mode": {"DRY_RUN": bool(settings.DRY_RUN), "LOGLEVEL": settings.LOGLEVEL},
                "Analysis": {"INCLUDE_TODAY": bool(settings.INCLUDE_TODAY)},
            }
            rows = [[k, json.dumps(v, ensure_ascii=False)] for k, v in params.items()]
            await m.send_text("=== PARAM ===\n" + _render_table(["Параметр", "Значение"], rows, align_right=False))
        except Exception as e:
            await m.send_text(f"Ошибка /param: {e}")

    async def cmd_days(args: str):
        try:
            scan_days = int(args.strip()) if args.strip().isdigit() else 5
            universe = _universe_symbols(ex, allowed_linear)

            header = ["Символ", "Дата", "Время", "Напр.D1/H4", "%проколаATR", "Возврат(H1)", "ТипSL", "Лимит", "Стоп", "Профит", "Результат", "Причина"]
            rows = []
            rr = float(settings.RR)
            total_tp = total_sl = total_nf = 0
            pnl_r_sum = 0.0

            need_h1 = (scan_days + 1) * 24 + 96
            for symbol in universe:
                try:
                    h1_raw = _fetch_ohlcv_safe(ex, settings.TF_H1, limit=min(1000, max(need_h1, 200)), symbol=symbol)
                    d1_raw = _fetch_ohlcv_safe(ex, settings.TF_D1, limit=max(110, scan_days + 10), symbol=symbol)
                    h4_raw = _fetch_ohlcv_safe(ex, settings.TF_H4, limit=max(320, scan_days * 6 + 50), symbol=symbol)
                except Exception as e:
                    log.warning("Skip %s in /days: %s", symbol, e)
                    continue

                h1 = ohlcv_to_df(h1_raw); d1 = ohlcv_to_df(d1_raw); h4 = ohlcv_to_df(h4_raw)
                atr_series = _atr(h1, int(settings.ATR_PERIOD_H1))
                if len(d1) < 2 or h1.empty:
                    continue

                last_idx = (len(d1) - 1) if int(settings.INCLUDE_TODAY) else (len(d1) - 2)
                first_idx = max(1, last_idx - scan_days + 1)

                for i in range(last_idx, first_idx - 1, -1):
                    day_row = d1.iloc[i]; prev_row = d1.iloc[i - 1]
                    next_ts = int(d1.iloc[i + 1]["ts"]) if i + 1 < len(d1) else (int(h1["ts"].max()) + 1)
                    tr_day = detect_trend(d1.loc[:i], h4[h4["ts"] <= next_ts])
                    if int(settings.STRICT_TREND) and tr_day.d1 != tr_day.h4:
                        continue
                    side = "long" if tr_day.d1 == "up" else "short"
                    level = float(prev_row["l"] if side == "long" else prev_row["h"])
                    h1_day = h1[(h1["ts"] >= int(day_row["ts"])) & (h1["ts"] < next_ts)]
                    if h1_day.empty:
                        continue

                    idx_list = list(h1_day.index); info = None
                    for pos, i1 in enumerate(idx_list):
                        hi = float(h1_day.loc[i1, "h"]); lo = float(h1_day.loc[i1, "l"])
                        broken = (side == "long" and lo < level) or (side == "short" and hi > level)
                        if not broken:
                            continue
                        back_i = None
                        for pos2 in range(pos + 1, min(pos + 1 + int(settings.H1_FALSE_BREAKOUT_MAX_CANDLES), len(idx_list))):
                            j = idx_list[pos2]; c = float(h1_day.loc[j, "c"])
                            if (side == "long" and c >= level) or (side == "short" and c <= level):
                                back_i = j; break
                        if back_i is None:
                            continue
                        seg = h1_day.loc[i1: back_i]
                        if side == "long":
                            probe_price = float(seg["l"].min())
                            pen_abs = (level - probe_price)
                        else:
                            probe_price = float(seg["h"].max())
                            pen_abs = (probe_price - level)
                        atr = float(atr_series.loc[i1]) if i1 in atr_series.index else 0.0
                        pen_atr_pct = (pen_abs / atr) if atr > 0 else 0.0
                        info = {
                            "side": side,
                            "idx": int(i1),
                            "came_back": True,
                            "candles_back": idx_list.index(back_i) - idx_list.index(i1),
                            "pen_atr_pct": float(pen_atr_pct),
                            "probe_price": float(probe_price),
                            "level": float(level),
                        }
                        break
                    if not info:
                        continue

                    levels = SimpleNamespace(prev_high=float(prev_row["h"]), prev_low=float(prev_row["l"]))
                    rej = _reason_if_reject(tr_day, levels, h1, info)
                    ts = int(h1.loc[info["idx"], "ts"]); dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)

                    if rej:
                        rows.append([symbol, dt.date().isoformat(), dt.strftime("%H:%M"), f"{tr_day.d1}/{tr_day.h4}",
                                    f"{info['pen_atr_pct']:.1f}", info["candles_back"], "—", "—", "—", "—", "—", rej])
                        continue

                    orig_sig = plan_trade(tr_day, levels, h1, info)
                    if not orig_sig:
                        rows.append([symbol, dt.date().isoformat(), dt.strftime("%H:%M"), f"{tr_day.d1}/{tr_day.h4}",
                                    f"{info['pen_atr_pct']:.1f}", info["candles_back"], "—", "—", "—", "—", "—", "other"])
                        continue

                    entry, sl, tp, sl_type = _recalc_sl_tp_with_new_rules(ex, symbol, orig_sig, info, h1, atr_series)

                    idx_back = info["idx"] + info["candles_back"]
                    back_ts = int(h1.loc[idx_back, "ts"])
                    h1_fw = h1[h1["ts"] > back_ts].head(24)
                    outcome = _simulate_after_fill(h1_fw, orig_sig["side"], entry, sl, tp)
                    if outcome == "TP":
                        total_tp += 1; pnl_r_sum += rr
                    elif outcome == "SL":
                        total_sl += 1; pnl_r_sum -= 1.0
                    elif outcome == "NoFill":
                        total_nf += 1

                    rows.append([symbol, dt.date().isoformat(), dt.strftime("%H:%M"),
                                f"{tr_day.d1}/{tr_day.h4}",
                                f"{info['pen_atr_pct']:.1f}", info["candles_back"], sl_type,
                                f"{entry:.2f}", f"{sl:.2f}", f"{tp:.2f}",
                                outcome, "ok"])

            msg = _render_table(header, rows)
            closed = total_tp + total_sl
            winrate = (total_tp / closed * 100.0) if closed > 0 else 0.0
            summary = _render_table(["Символов", "Сигналов", "TP", "SL", "NoFill", "Winrate%", "Pnl(R)"],
                                    [[len(universe), len(rows), total_tp, total_sl, total_nf, f"{winrate:.1f}", f"{pnl_r_sum:.2f}"]])
            await m.send_text("=== DAYS ===\n" + msg + "\n" + summary)
        except Exception as e:
            await m.send_text(f"Ошибка /days: {e}")

    # --------- scheduler loop ---------
    def _make_event_id(symbol: str, info: dict, tr_day: Trend, levels: SimpleNamespace, h1):
        ts = int(h1.loc[info["idx"], "ts"])
        d = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date().isoformat()
        side = info.get("side")
        level = levels.prev_low if side == "long" else levels.prev_high
        return f"{d}|{side}|{round(level, 2)}|{info['idx']}"

    def _cooldown_passed(symbol: str) -> bool:
        cd_h = float(settings.COOLDOWN_HOURS or 0)
        if cd_h <= 0:
            return True
        last_at = _load_state().get("last_notified_at", {}).get(symbol)
        if not last_at:
            return True
        last_dt = datetime.fromtimestamp(last_at / 1000, tz=timezone.utc)
        return datetime.now(timezone.utc) - last_dt >= timedelta(hours=cd_h)

    async def _trade_step_symbol(symbol: str) -> Optional[str]:
        try:
            h1_raw = _fetch_ohlcv_safe(ex, settings.TF_H1, limit=300, symbol=symbol)
            d1 = ohlcv_to_df(_fetch_ohlcv_safe(ex, settings.TF_D1, limit=200, symbol=symbol))
            h4 = ohlcv_to_df(_fetch_ohlcv_safe(ex, settings.TF_H4, limit=400, symbol=symbol))
        except Exception as e:
            log.warning("Skip symbol %s: %s", symbol, e)
            return None

        h1 = ohlcv_to_df(h1_raw)
        info, sig, tr_day, levels, reason = _detect_today_fakeout_and_signal(d1, h4, h1)
        if reason or not sig:
            return None

        # ---- анти-спам: dedup + cooldown ----
        st_local = _load_state()
        last_events = st_local.get("last_events", {})
        last_id = last_events.get(symbol)
        this_id = _make_event_id(symbol, info, tr_day, levels, h1)
        if last_id == this_id:
            return None
        if not _cooldown_passed(symbol):
            return None

        st_local.setdefault("last_events", {})[symbol] = this_id
        st_local.setdefault("last_notified_at", {})[symbol] = int(time.time() * 1000)
        _save_state(st_local)

        atr_series = _atr(h1, int(settings.ATR_PERIOD_H1))
        entry, sl, tp, sl_type = _recalc_sl_tp_with_new_rules(ex, symbol, sig, info, h1, atr_series)

        msg = (f"[{symbol}] Сигнал {sig['side'].upper()} @ {entry:.2f} "
               f"SL={sl:.2f}({sl_type}) TP={tp:.2f}  "
               f"pen≈{info.get('pen_atr_pct', 0):.2f} ATR")

        if int(settings.DRY_RUN):
            return msg + "  [DRY_RUN]"

        # === ЖИВОЕ ИСПОЛНЕНИЕ ===
        try:
            bal = ex.x.fetch_balance().get('USDT', {})
            free = float(bal.get('free') or bal.get('total') or 0.0)

            qty = _calc_qty_from_risk_linear_usdt(
                ex, symbol, free, float(settings.RISK_PCT), entry, sl
            )
            if qty <= 0:
                return msg + "  [SKIP: qty<=0]"

            order = ex.place_bracket_order(
                symbol, sig["side"], qty, entry, sl, tp,
                post_only=bool(int(os.getenv("POST_ONLY", "1") or "1")),
                tif=os.getenv("TIME_IN_FORCE", "GTC") or "GTC",
                tp_mode=os.getenv("TP_MODE", "limit") or "limit",
                sl_mode=os.getenv("SL_MODE", "market") or "market",
            )
            log.info("Order sent %s: %s", symbol, order)
            return msg + f"  [ORDER SENT qty={qty}]"
        except Exception as e:
            log.error("order send failed %s: %s", symbol, e)
            return msg + f"  [ORDER ERROR: {e}]"

    async def _scheduled_job():
        st = _load_state()
        if not st.get("armed", False):
            return
        symbols = _sanitize_symbols(st.get("symbols") or [], allowed_linear)
        if not symbols:
            symbols = _universe_symbols(ex, allowed_linear)
            st["symbols"] = symbols; _save_state(st)
        for sym in symbols:
            msg = await _trade_step_symbol(sym)
            if msg:
                await m.send_text(msg)

    async def cmd_run(_: str):
        uni = _universe_symbols(ex, allowed_linear)
        st = _load_state(); st["armed"] = True; st["symbols"] = uni; _save_state(st)
        sched.enable()
        await m.send_text(f"Режим ожидания включён. Символов: {len(uni)}. Проверяю каждые {settings.RUN_EVERY_SEC}с.")

    async def cmd_stop(_: str):
        st = _load_state(); st["armed"] = False; _save_state(st)
        sched.disable()
        await m.send_text("Режим ожидания выключен. Планировщик остановлен.")

    async def cmd_scheduler_on(_: str):
        sched.enable()
        await m.send_text(f"Планировщик: ON, interval={settings.RUN_EVERY_SEC}s")

    async def cmd_scheduler_off(_: str):
        sched.disable()
        await m.send_text("Планировщик: OFF")

    async def cmd_scheduler_status(_: str):
        await m.send_text(f"Планировщик: {'ON' if sched.is_enabled() else 'OFF'}")

    # регистрация
    sched.set_job(_scheduled_job)
    m.add_command("/help", cmd_help)
    m.add_command("/param", cmd_param)
    m.add_command("/days", cmd_days)
    m.add_command("/run", cmd_run)
    m.add_command("/stop", cmd_stop)
    m.add_command("/scheduler_on", cmd_scheduler_on)
    m.add_command("/scheduler_off", cmd_scheduler_off)
    m.add_command("/scheduler_status", cmd_scheduler_status)
    register_order_commands(m, ex)
