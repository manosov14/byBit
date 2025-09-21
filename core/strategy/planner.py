from __future__ import annotations

import math

from core.config import settings
from .models import BreakoutInfo, PlannedTrade


def plan_trade(breakout: BreakoutInfo, *, tick_size: float) -> PlannedTrade | None:
    entry_mode = (settings.ENTRY_MODE or "next_open").lower()
    if entry_mode == "level_offset":
        offset = max(0.0, float(settings.ENTRY_UNDERFILL_PCT))
        if breakout.side == "long":
            entry = breakout.level * (1 + offset)
        else:
            entry = breakout.level * (1 - offset)
    else:
        entry = breakout.next_open if breakout.next_open is not None else breakout.candle_close

    stop_mode = (settings.STOP_MODE or "wick").lower()
    tick = float(tick_size or 0.0)
    if tick <= 0:
        tick = breakout.level * 0.0001
        if tick <= 0:
            tick = 0.01

    ticks = max(0, int(settings.STOP_WICK_TICKS))
    if stop_mode in ("level", "level_pct"):
        pct = max(0.0, float(settings.STOP_LEVEL_PCT))
        if breakout.side == "long":
            sl = breakout.level * (1 - pct)
        else:
            sl = breakout.level * (1 + pct)
    else:
        if breakout.side == "long":
            sl = breakout.wick_price - ticks * tick
        else:
            sl = breakout.wick_price + ticks * tick

    entry = float(entry)
    sl = float(sl)

    if breakout.side == "long" and entry <= sl:
        entry = max(entry, breakout.level)
        if entry <= sl:
            entry = sl + tick
    if breakout.side == "short" and entry >= sl:
        entry = min(entry, breakout.level)
        if entry >= sl:
            entry = sl - tick

    risk = (entry - sl) if breakout.side == "long" else (sl - entry)
    if risk <= 0 or not math.isfinite(risk):
        return None

    rr = max(0.1, float(settings.RR))
    tp = entry + rr * risk if breakout.side == "long" else entry - rr * risk

    volume_ratio = (breakout.volume / breakout.volume_ma) if breakout.volume_ma else None

    meta = {
        "level": float(breakout.level),
        "level_source": breakout.level_source,
        "break_pct": float(breakout.break_pct),
        "close_back_pct": float(breakout.close_back_pct),
        "volume_ratio": float(volume_ratio) if volume_ratio is not None else None,
        "candle_ts": int(breakout.ts),
    }

    return PlannedTrade(
        side=breakout.side,
        entry=float(entry),
        sl=float(sl),
        tp=float(tp),
        meta=meta,
    )

