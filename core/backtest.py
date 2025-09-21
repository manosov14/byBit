from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List

import pandas as pd

from core.config import settings
from core.strategy import (
    collect_level_candidates,
    detect_trend,
    determine_trade_side,
    find_false_breakouts_for_day,
    plan_trade,
)


@dataclass(slots=True)
class BacktestResult:
    signals: int
    filled: int
    tp: int
    sl: int
    nofill: int
    pnl_r: float
    log_rows: List[dict]


def _simulate_after_fill(h1_fw: pd.DataFrame, side: str, entry: float, sl: float, tp: float) -> str:
    if h1_fw.empty:
        return "NoFill"
    for _, r in h1_fw.iterrows():
        lo = float(r["l"])
        hi = float(r["h"])
        hit_sl = (lo <= sl) if side == "long" else (hi >= sl)
        hit_tp = (hi >= tp) if side == "long" else (lo <= tp)
        if hit_sl and hit_tp:
            return "SL" if abs(entry - sl) <= abs(tp - entry) else "TP"
        if hit_sl:
            return "SL"
        if hit_tp:
            return "TP"
    return "NoFill"
def _approx_tick(level: float) -> float:
    tick = abs(level) * 0.0001
    if tick <= 0:
        tick = 0.01
    return tick


def backtest(d1: pd.DataFrame, h4: pd.DataFrame, h1: pd.DataFrame) -> BacktestResult:
    signals = 0
    filled = 0
    tp = 0
    sl = 0
    nofill = 0
    pnl_r = 0.0
    rows: List[dict] = []

    if len(d1) < 2 or h1.empty:
        return BacktestResult(signals, filled, tp, sl, nofill, pnl_r, rows)

    rr = float(settings.RR)

    for i in range(1, len(d1)):
        day_row = d1.iloc[i]
        start_ts = int(day_row["ts"])
        end_ts = int(d1.iloc[i + 1]["ts"]) if i + 1 < len(d1) else None

        h4_scope = h4
        if end_ts is not None:
            h4_scope = h4_scope[h4_scope["ts"] <= end_ts]

        trend = detect_trend(d1.iloc[: i + 1], h4_scope)
        side = determine_trade_side(trend)
        if not side:
            continue

        levels = collect_level_candidates(d1, i, int(settings.LEVEL_LOOKBACK_DAYS))
        breakouts = find_false_breakouts_for_day(h1, levels, start_ts, end_ts, allowed_side=side)
        if not breakouts:
            continue

        breakout = breakouts[0]
        signals += 1

        trade = plan_trade(breakout, tick_size=_approx_tick(breakout.level))
        if not trade:
            continue

        entry = float(trade.entry)
        sl_price = float(trade.sl)
        tp_price = float(trade.tp)
        meta = trade.meta or {}

        h1_after = h1[h1["ts"] >= int(breakout.ts)]
        entry_hit_ts = None
        for _, row in h1_after.iterrows():
            lo = float(row["l"])
            hi = float(row["h"])
            if lo <= entry <= hi:
                entry_hit_ts = int(row["ts"])
                break

        if entry_hit_ts is None:
            nofill += 1
            continue

        filled += 1
        future = h1[h1["ts"] > entry_hit_ts].head(24)
        outcome = _simulate_after_fill(future, trade.side, entry, sl_price, tp_price)
        if outcome == "TP":
            tp += 1
            pnl_r += rr
        elif outcome == "SL":
            sl += 1
            pnl_r -= 1.0
        else:
            nofill += 1

        dt = datetime.fromtimestamp(int(breakout.ts) / 1000, tz=timezone.utc)
        rows.append({
            "time_utc": dt.strftime("%Y-%m-%d %H:%M"),
            "side": trade.side,
            "entry": entry,
            "sl": sl_price,
            "tp": tp_price,
            "break_pct": float(meta.get("break_pct", breakout.break_pct) * 100),
            "close_back_pct": float(meta.get("close_back_pct", breakout.close_back_pct) * 100),
            "volume_ratio": meta.get("volume_ratio"),
            "level_source": meta.get("level_source", breakout.level_source),
            "outcome": outcome,
        })

    return BacktestResult(signals, filled, tp, sl, nofill, pnl_r, rows)

