from __future__ import annotations

import math
from typing import List, Optional

import pandas as pd

from core.config import settings
from .models import BreakoutInfo, LevelCandidate
from .utils import volume_ma


def _find_breakout_for_level(
    h1_day: pd.DataFrame,
    level: LevelCandidate,
    vol_ma: pd.Series,
) -> Optional[BreakoutInfo]:
    if h1_day.empty:
        return None

    min_pen = float(settings.PENETRATION_MIN_PCT)
    max_pen = float(settings.PENETRATION_MAX_PCT)
    close_max = float(settings.CLOSE_BACK_MAX_PCT)
    vol_ratio = float(settings.VOLUME_MAX_RATIO)

    idx_list = list(h1_day.index)
    for pos, idx in enumerate(idx_list):
        row = h1_day.loc[idx]
        o = float(row["o"])
        h = float(row["h"])
        l = float(row["l"])
        c = float(row["c"])
        v = float(row["v"])
        ma = float(vol_ma.loc[idx]) if idx in vol_ma.index else math.nan

        if not math.isfinite(ma) or ma <= 0:
            continue
        if v > vol_ratio * ma:
            continue

        if level.side == "long":
            penetration = (level.value - l) / level.value if level.value else 0.0
            close_back = (c - level.value) / level.value if level.value else 0.0
            closed_inside = c >= level.value
            wick_price = l
        else:
            penetration = (h - level.value) / level.value if level.value else 0.0
            close_back = (level.value - c) / level.value if level.value else 0.0
            closed_inside = c <= level.value
            wick_price = h

        if penetration < min_pen or penetration > max_pen:
            continue
        if not closed_inside:
            continue
        if close_back < 0 or close_back > close_max:
            continue

        next_idx = idx_list[pos + 1] if pos + 1 < len(idx_list) else None
        next_open = float(h1_day.loc[next_idx, "o"]) if next_idx is not None else None

        return BreakoutInfo(
            side=level.side,
            level=float(level.value),
            level_source=level.label,
            level_age=int(level.age),
            idx=int(idx),
            ts=int(row["ts"]),
            next_idx=int(next_idx) if next_idx is not None else None,
            next_open=float(next_open) if next_open is not None else None,
            break_pct=float(penetration),
            close_back_pct=float(close_back),
            candle_open=o,
            candle_high=h,
            candle_low=l,
            candle_close=c,
            volume=v,
            volume_ma=ma,
            wick_price=float(wick_price),
        )

    return None


def find_false_breakouts_for_day(
    h1_df: pd.DataFrame,
    levels: List[LevelCandidate],
    start_ts: int,
    end_ts: Optional[int],
    allowed_side: Optional[str] = None,
) -> List[BreakoutInfo]:
    if h1_df.empty or not levels:
        return []

    if end_ts is None:
        end_ts = int(h1_df["ts"].max()) + 1

    mask = (h1_df["ts"] >= int(start_ts)) & (h1_df["ts"] < int(end_ts))
    h1_day = h1_df[mask]
    if h1_day.empty:
        return []

    vol_ma = volume_ma(h1_df["v"].astype(float), int(settings.VOLUME_MA_LENGTH))
    results: List[BreakoutInfo] = []

    for level in levels:
        if allowed_side and level.side != allowed_side:
            continue
        info = _find_breakout_for_level(h1_day, level, vol_ma)
        if info:
            results.append(info)

    results.sort(key=lambda b: (b.ts, b.level_age))
    return results

