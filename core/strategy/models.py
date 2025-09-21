from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass(slots=True)
class Trend:
    """Stores directional bias for the strategy on higher timeframes."""

    d1: str
    h4: str

    def as_tuple(self) -> tuple[str, str]:
        return self.d1, self.h4


@dataclass(slots=True)
class LevelCandidate:
    """Candidate breakout level sourced from previous daily extremes."""

    value: float
    side: str
    ts: int
    label: str
    age: int


@dataclass(slots=True)
class BreakoutInfo:
    """Describes a detected false-breakout candle on H1."""

    side: str
    level: float
    level_source: str
    level_age: int
    idx: int
    ts: int
    next_idx: Optional[int]
    next_open: Optional[float]
    break_pct: float
    close_back_pct: float
    candle_open: float
    candle_high: float
    candle_low: float
    candle_close: float
    volume: float
    volume_ma: float
    wick_price: float


@dataclass(slots=True)
class PlannedTrade:
    """Normalized trade plan returned by the strategy planner."""

    side: str
    entry: float
    sl: float
    tp: float
    reason: str = "ok"
    meta: Dict[str, float | int | str | None] = field(default_factory=dict)

    def risk_amount(self) -> float:
        if self.side == "long":
            return self.entry - self.sl
        return self.sl - self.entry

