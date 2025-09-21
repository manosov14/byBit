from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import List

from messaging.base import Messenger

from bot import (
    FalseBreakoutRunner,
    MarketDataService,
    StateStore,
    build_linear_perp_set,
    render_table,
    resolve_universe,
    sanitize_symbols,
)
from bot.risk import tick_size
from core.backtest import _simulate_after_fill
from core.config import settings
from core.exchange import Exchange
from core.orders import register_order_commands
from core.scheduler import Scheduler
from core.strategy import FalseBreakoutStrategy, StrategyContext

log = logging.getLogger("handlers")


def register_handlers(m: Messenger, sched: Scheduler):
    exchange = Exchange()
    exchange.load()
    log.info("Exchange warmed")

    market_data = MarketDataService(exchange)
    state_store = StateStore()
    runner = FalseBreakoutRunner(exchange, market_data, state_store)
    strategy = FalseBreakoutStrategy()

    allowed_symbols = build_linear_perp_set(exchange)

    state = state_store.load()
    if not state.symbols:
        state.symbols = [settings.SYMBOL]
    state.symbols = sanitize_symbols(state.symbols, allowed_symbols)
    state_store.save(state)

    def current_universe() -> List[str]:
        stored = sanitize_symbols(state_store.load().symbols, allowed_symbols)
        return stored or resolve_universe(exchange, allowed_symbols)

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
            uni = current_universe()
            params = {
                "UNIVERSE": uni,
                "SYMBOL_BASE": settings.SYMBOL,
                "TF": {"D1": settings.TF_D1, "H4": settings.TF_H4, "H1": settings.TF_H1},
                "TREND": {
                    "D1_SMA": settings.D1_SMA,
                    "H4_FAST": settings.H4_FAST,
                    "H4_SLOW": settings.H4_SLOW,
                    "STRICT_TREND": bool(settings.STRICT_TREND),
                },
                "FAKEOUT/H1": {
                    "LEVEL_LOOKBACK_DAYS": settings.LEVEL_LOOKBACK_DAYS,
                    "PEN_MIN_PCT": settings.PENETRATION_MIN_PCT,
                    "PEN_MAX_PCT": settings.PENETRATION_MAX_PCT,
                    "CLOSE_BACK_MAX_PCT": settings.CLOSE_BACK_MAX_PCT,
                    "VOLUME_MA_LENGTH": settings.VOLUME_MA_LENGTH,
                    "VOLUME_MAX_RATIO": settings.VOLUME_MAX_RATIO,
                },
                "ENTRY": {
                    "MODE": settings.ENTRY_MODE,
                    "UNDERFILL_PCT": settings.ENTRY_UNDERFILL_PCT,
                    "RR": settings.RR,
                },
                "Stops": {
                    "MODE": settings.STOP_MODE,
                    "STOP_WICK_TICKS": settings.STOP_WICK_TICKS,
                    "STOP_LEVEL_PCT": settings.STOP_LEVEL_PCT,
                },
                "Risk": {
                    "RISK_PCT": settings.RISK_PCT,
                    "MAX_CONSECUTIVE_LOSSES": settings.MAX_CONSECUTIVE_LOSSES,
                    "MAX_OPEN_POSITIONS": settings.MAX_OPEN_POSITIONS,
                },
                "Protections": {
                    "ONE_TRADE_PER_DAY": bool(settings.ONE_TRADE_PER_DAY),
                    "DEDUP_BY_BREAKOUT": bool(settings.DEDUP_BY_BREAKOUT),
                    "COOLDOWN_HOURS": settings.COOLDOWN_HOURS,
                },
                "Mode": {"DRY_RUN": bool(settings.DRY_RUN), "LOGLEVEL": settings.LOGLEVEL},
                "Analysis": {"INCLUDE_TODAY": bool(settings.INCLUDE_TODAY)},
            }
            rows = [[k, json.dumps(v, ensure_ascii=False)] for k, v in params.items()]
            await m.send_text("=== PARAM ===\n" + render_table(["Параметр", "Значение"], rows, align_right=False))
        except Exception as exc:
            await m.send_text(f"Ошибка /param: {exc}")

    async def cmd_days(args: str):
        try:
            scan_days = int(args.strip()) if args.strip().isdigit() else 5
            universe = current_universe()

            header = [
                "Символ",
                "Дата",
                "Время",
                "Тренд D1/H4",
                "Пробой%",
                "Возврат%",
                "Vol/MA20",
                "Уровень",
                "Entry",
                "SL",
                "TP",
                "Результат",
                "Комментарий",
            ]
            rows = []
            rr = float(settings.RR)
            total_tp = total_sl = total_nf = 0
            pnl_r_sum = 0.0

            need_h1 = (scan_days + 1) * 24 + 96
            for symbol in universe:
                try:
                    bundle = market_data.fetch_bundle_with_timeframes(
                        symbol,
                        tf_h1=settings.TF_H1,
                        tf_d1=settings.TF_D1,
                        tf_h4=settings.TF_H4,
                        h1_limit=min(1000, max(need_h1, 200)),
                        d1_limit=max(110, scan_days + 10),
                        h4_limit=max(320, scan_days * 6 + 50),
                    )
                except Exception as exc:
                    log.warning("Skip %s in /days: %s", symbol, exc)
                    continue

                h1 = bundle.h1
                d1 = bundle.d1
                h4 = bundle.h4
                if len(d1) < 2 or h1.empty:
                    continue

                tick = tick_size(exchange, symbol)
                last_idx = (len(d1) - 1) if int(settings.INCLUDE_TODAY) else (len(d1) - 2)
                first_idx = max(1, last_idx - scan_days + 1)

                for i in range(last_idx, first_idx - 1, -1):
                    day_row = d1.iloc[i]
                    next_ts = int(d1.iloc[i + 1]["ts"]) if i + 1 < len(d1) else None
                    h4_scope = h4[h4["ts"] <= (next_ts or int(h1["ts"].max()) + 1)]
                    ctx = StrategyContext(d1=d1.iloc[: i + 1], h4=h4_scope, h1=h1)
                    trend = strategy.detect_trend(ctx)
                    side = strategy.determine_side(trend)
                    if not side:
                        continue

                    levels = strategy.collect_levels(ctx, i, int(settings.LEVEL_LOOKBACK_DAYS))
                    start_ts = int(day_row["ts"])
                    breakouts = strategy.find_breakouts(ctx, levels, start_ts, next_ts, allowed_side=side)
                    if not breakouts:
                        continue

                    breakout = breakouts[0]
                    trade = strategy.plan_trade(breakout, tick_size=tick)
                    if not trade:
                        continue

                    meta = trade.meta or {}
                    entry = float(trade.entry)
                    sl_price = float(trade.sl)
                    tp_price = float(trade.tp)

                    h1_after = h1[h1["ts"] >= int(breakout.ts)]
                    filled = False
                    entry_hit_ts = None
                    for _, row in h1_after.iterrows():
                        lo = float(row["l"])
                        hi = float(row["h"])
                        if lo <= entry <= hi:
                            filled = True
                            entry_hit_ts = int(row["ts"])
                            break
                    if filled:
                        future = h1[h1["ts"] > entry_hit_ts].head(24)
                        outcome = _simulate_after_fill(future, trade.side, entry, sl_price, tp_price)
                    else:
                        outcome = "NoFill"

                    if outcome == "TP":
                        total_tp += 1
                        pnl_r_sum += rr
                    elif outcome == "SL":
                        total_sl += 1
                        pnl_r_sum -= 1.0
                    else:
                        total_nf += 1

                    dt = datetime.fromtimestamp(int(breakout.ts) / 1000, tz=timezone.utc)
                    vol_ratio = meta.get("volume_ratio")
                    rows.append([
                        symbol,
                        dt.date().isoformat(),
                        dt.strftime("%H:%M"),
                        f"{trend.d1}/{trend.h4}",
                        f"{breakout.break_pct * 100:.2f}",
                        f"{breakout.close_back_pct * 100:.2f}",
                        f"{vol_ratio:.2f}" if isinstance(vol_ratio, (int, float)) else "—",
                        breakout.level_source,
                        f"{entry:.4f}",
                        f"{sl_price:.4f}",
                        f"{tp_price:.4f}",
                        outcome,
                        trade.reason,
                    ])

            msg = render_table(header, rows)
            closed = total_tp + total_sl
            winrate = (total_tp / closed * 100.0) if closed > 0 else 0.0
            summary = render_table(
                ["Символов", "Сигналов", "TP", "SL", "NoFill", "Winrate%", "Pnl(R)"],
                [[len(universe), len(rows), total_tp, total_sl, total_nf, f"{winrate:.1f}", f"{pnl_r_sum:.2f}"]],
            )
            await m.send_text("=== DAYS ===\n" + msg + "\n" + summary)
        except Exception as exc:
            await m.send_text(f"Ошибка /days: {exc}")

    async def cmd_run(_: str):
        uni = resolve_universe(exchange, allowed_symbols)
        def enable(st):
            st.armed = True
            st.symbols = uni

        state_store.update(enable)
        sched.enable()
        await m.send_text(
            f"Режим ожидания включён. Символов: {len(uni)}. Проверяю каждые {settings.RUN_EVERY_SEC}с."
        )

    async def cmd_stop(_: str):
        def disable(st):
            st.armed = False

        state_store.update(disable)
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

    async def _scheduled_job():
        st = state_store.load()
        if not st.armed:
            return
        symbols = sanitize_symbols(st.symbols, allowed_symbols)
        if not symbols:
            symbols = resolve_universe(exchange, allowed_symbols)
            st.symbols = symbols
            state_store.save(st)
        for sym in symbols:
            decision = runner.check_symbol(sym)
            if decision:
                await m.send_text(decision.message)

    sched.set_job(_scheduled_job)
    m.add_command("/help", cmd_help)
    m.add_command("/param", cmd_param)
    m.add_command("/days", cmd_days)
    m.add_command("/run", cmd_run)
    m.add_command("/stop", cmd_stop)
    m.add_command("/scheduler_on", cmd_scheduler_on)
    m.add_command("/scheduler_off", cmd_scheduler_off)
    m.add_command("/scheduler_status", cmd_scheduler_status)
    register_order_commands(m, exchange)

