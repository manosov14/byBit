from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
import pandas as pd

from core.config import settings
from core.strategy import _atr, detect_trend, plan_trade

@dataclass
class BacktestResult:
    signals: int
    filled: int
    tp: int
    sl: int
    nofill: int
    pnl_r: float
    log_rows: list  # список словарей с последними сигналами (для таблицы)

def _first_fakeout_in_day(
    h1_day: pd.DataFrame,
    level: float,
    side: str,
    max_back: int,
    atr_series: pd.Series,
):
    """
    Возвращает первую структуру фейкаута в пределах дня:
      - бар B0 пробил уровень (минимум < level для лонга, максимум > level для шорта)
      - в пределах max_back H1 баров закрытие вернулось внутрь диапазона (C >= level для лонга, C <= level для шорта)
    """
    if h1_day.empty:
        return None
    idx = list(h1_day.index)
    for p, i in enumerate(idx):
        hi = float(h1_day.loc[i, "h"])
        lo = float(h1_day.loc[i, "l"])
        broke = (side == "long" and lo < level) or (side == "short" and hi > level)
        if not broke:
            continue

        # ищем возврат внутрь
        back = None
        for p2 in range(p + 1, min(p + 1 + max_back, len(idx))):
            j = idx[p2]
            c = float(h1_day.loc[j, "c"])
            if (side == "long" and c >= level) or (side == "short" and c <= level):
                back = j
                break
        if back is None:
            # первый прокол без возврата — ищем дальше
            continue

        # глубина прокола в ATR
        seg = h1_day.loc[i:back]
        pen_abs = (level - float(seg["l"].min())) if side == "long" else (float(seg["h"].max()) - level)
        atr = float(atr_series.loc[i]) if i in atr_series.index else 0.0
        pen_atr = (pen_abs / atr) if atr > 0 else 0.0

        return {
            "side": side,
            "idx_break": int(i),
            "idx_back": int(back),
            "candles_back": idx.index(back) - idx.index(i),
            "pen_atr": float(pen_atr),
        }
    return None

def _simulate_after_fill(h1_fw: pd.DataFrame, side: str, entry: float, sl: float, tp: float) -> str:
    """
    После факта входа (entry достигнут ретестом) симулируем до 24 H1:
    возвращает "TP" | "SL" | "NoFill" (если дальше не было касания TP/SL — маловероятно).
    """
    if h1_fw.empty:
        return "NoFill"
    for _, r in h1_fw.iterrows():
        lo = float(r["l"]); hi = float(r["h"])
        hit_sl = (lo <= sl) if side == "long" else (hi >= sl)
        hit_tp = (hi >= tp) if side == "long" else (lo <= tp)
        if hit_sl and hit_tp:
            # если оба в одном баре — считаем, что первым сработает более близкий к цене входа
            return "SL" if abs(entry - sl) <= abs(tp - entry) else "TP"
        if hit_sl:
            return "SL"
        if hit_tp:
            return "TP"
    return "NoFill"

def backtest(d1: pd.DataFrame, h4: pd.DataFrame, h1: pd.DataFrame) -> BacktestResult:
    """
    Бэктест по правилам стратегии:
    - ≤1 сигнал на день
    - сторона по D1 (при STRICT_TREND=1 требуем совпадение D1/H4)
    - фильтры: возврат ≤ N H1, глубина прокола <= MAX_PEN_ATR, опц. ретест ≤ K H1
    - вход лимиткой на ретесте с допуском ENTRY_OFFSET_ATR_PCT
    - симуляция исхода на следующих 24 H1
    """
    atr_h1 = _atr(h1, int(settings.ATR_PERIOD_H1))

    signals = 0
    filled = 0
    tp = 0
    sl = 0
    nofill = 0
    pnl_r = 0.0
    rows = []

    # перебор по закрытым дням в пределах покрытия H1
    # начинаем со второго дня, чтобы был "предыдущий"
    for i in range(1, len(d1) - 1):
        day = d1.iloc[i]
        prev = d1.iloc[i - 1]
        next_ts = int(d1.iloc[i + 1]["ts"])

        # тренд на конец дня
        tr = detect_trend(d1.loc[:i], h4[h4["ts"] <= next_ts])
        if int(settings.STRICT_TREND) and tr.d1 != tr.h4:
            continue

        # цель фейкаута: по D1
        if tr.d1 == "up":
            side = "long"; level = float(prev["l"])
        else:
            side = "short"; level = float(prev["h"])

        h1_day = h1[(h1["ts"] >= int(day["ts"])) & (h1["ts"] < next_ts)]
        if h1_day.empty:
            continue

        info = _first_fakeout_in_day(
            h1_day,
            level,
            side,
            int(settings.H1_FALSE_BREAKOUT_MAX_CANDLES),
            atr_h1,
        )
        if not info:
            continue

        # фильтр по глубине прокола
        if info["pen_atr"] > float(settings.H1_MAX_PENETRATION_ATR_PCT):
            continue

        # увеличиваем счётчик сигналов — сетап валидный
        signals += 1

        # ретест и вход
        retest_max = int(settings.H1_RETEST_MAX_CANDLES)
        entry_price = (
            level + float(settings.ENTRY_OFFSET_ATR_PCT) * float(atr_h1.loc[info["idx_back"]])
            if side == "long" else
            level - float(settings.ENTRY_OFFSET_ATR_PCT) * float(atr_h1.loc[info["idx_back"]])
        )

        # окно ретеста после возврата внутрь
        h1_after_back = h1[h1["ts"] >= int(h1.loc[info["idx_back"], "ts"])]
        h1_retest_window = h1_after_back.head(retest_max) if retest_max > 0 else h1_after_back.head(24)

        # достигли ли цены входа?
        touched = any(
            (float(r["l"]) <= entry_price <= float(r["h"]))
            for _, r in h1_retest_window.iterrows()
        )
        if not touched:
            nofill += 1
            dt = datetime.fromtimestamp(int(h1.loc[info["idx_back"], "ts"]) / 1000, tz=timezone.utc)
            rows.append({
                "time_utc": dt.strftime("%Y-%m-%d %H:%M"),
                "side": "long" if side == "long" else "short",
                "entry": entry_price,
                "sl": None,
                "tp": None,
                "outcome": "NoFill",
            })
            continue

        # строим уровни входа/сл/тп по тем же правилам, что и в онлайне
        levels = SimpleNamespace(prev_high=float(prev["h"]), prev_low=float(prev["l"]))
        sig = plan_trade(tr, levels, h1, {
            "side": side,
            "idx": info["idx_back"],     # точка подтверждения (возврат внутрь)
            "came_back": True,
            "candles_back": info["candles_back"],
            "pen_atr_pct": info["pen_atr"],
        })
        if not sig:
            nofill += 1  # на всякий случай относим к NoFill
            continue

        filled += 1

        # исход на 24 H1 после фактического входа
        idx_back_ts = int(h1.loc[info["idx_back"], "ts"])
        h1_fw = h1[h1["ts"] > idx_back_ts].head(24)
        outcome = _simulate_after_fill(h1_fw, sig["side"], sig["entry"], sig["sl"], sig["tp"])
        if outcome == "TP":
            tp += 1
            pnl_r += float(settings.RR)
        elif outcome == "SL":
            sl += 1
            pnl_r -= 1.0
        else:
            nofill += 1  # на всякий случай, но такое почти не встретится

        dt = datetime.fromtimestamp(idx_back_ts / 1000, tz=timezone.utc)
        rows.append({
            "time_utc": dt.strftime("%Y-%m-%d %H:%M"),
            "side": "long" if side == "long" else "short",
            "entry": sig["entry"],
            "sl": sig["sl"],
            "tp": sig["tp"],
            "outcome": outcome,
        })

    return BacktestResult(
        signals=signals, filled=filled, tp=tp, sl=sl, nofill=nofill, pnl_r=pnl_r, log_rows=rows
    )
