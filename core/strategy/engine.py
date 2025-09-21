from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import pandas as pd

from .models import BreakoutInfo, LevelCandidate, PlannedTrade, Trend
from .trend import detect_trend, determine_trade_side
from .levels import collect_level_candidates
from .breakouts import find_false_breakouts_for_day
from .planner import plan_trade


@dataclass(slots=True)
class StrategyContext:
    d1: pd.DataFrame
    h4: pd.DataFrame
    h1: pd.DataFrame


class FalseBreakoutStrategy:
    """High-level orchestrator for the false-breakout trading rules."""

    def detect_trend(self, ctx: StrategyContext) -> Trend:
        return detect_trend(ctx.d1, ctx.h4)

    def determine_side(self, trend: Trend) -> Optional[str]:
        return determine_trade_side(trend)

    def collect_levels(self, ctx: StrategyContext, day_idx: int, lookback: int) -> List[LevelCandidate]:
        return collect_level_candidates(ctx.d1, day_idx, lookback)

    def find_breakouts(
        self,
        ctx: StrategyContext,
        levels: List[LevelCandidate],
        start_ts: int,
        end_ts: Optional[int],
        allowed_side: Optional[str] = None,
    ) -> List[BreakoutInfo]:
        return find_false_breakouts_for_day(ctx.h1, levels, start_ts, end_ts, allowed_side=allowed_side)

    def plan_trade(self, breakout: BreakoutInfo, *, tick_size: float) -> Optional[PlannedTrade]:
        return plan_trade(breakout, tick_size=tick_size)

