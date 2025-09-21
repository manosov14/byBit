from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from core.config import settings
from .models import Trend
from .utils import sma


def detect_trend(d1_df: pd.DataFrame, h4_df: pd.DataFrame) -> Trend:
    d1 = d1_df.copy().reset_index(drop=True)
    h4 = h4_df.copy().reset_index(drop=True)

    need_d1 = max(int(settings.D1_SMA) + 50, 250)
    if len(d1) < need_d1:
        pass

    d1_close = d1["c"].astype(float)
    sma_d1 = sma(d1_close, int(settings.D1_SMA))
    last_close = float(d1_close.iloc[-1])
    last_sma = float(sma_d1.iloc[-1]) if not np.isnan(sma_d1.iloc[-1]) else last_close
    d1_trend = "up" if last_close >= last_sma else "down"

    h4_close = h4["c"].astype(float)
    sma_fast = sma(h4_close, int(settings.H4_FAST))
    sma_slow = sma(h4_close, int(settings.H4_SLOW))
    last_fast = float(sma_fast.iloc[-1]) if not np.isnan(sma_fast.iloc[-1]) else h4_close.iloc[-1]
    last_slow = float(sma_slow.iloc[-1]) if not np.isnan(sma_slow.iloc[-1]) else h4_close.iloc[-1]
    h4_trend = "up" if last_fast >= last_slow else "down"

    return Trend(d1=d1_trend, h4=h4_trend)


def determine_trade_side(trend: Trend | None, *, strict: Optional[bool] = None) -> Optional[str]:
    if trend is None:
        return None
    strict = bool(settings.STRICT_TREND if strict is None else strict)
    if trend.d1 == "up" and trend.h4 == "up":
        return "long"
    if trend.d1 == "down" and trend.h4 == "down":
        return "short"
    if strict:
        return None
    return "long" if trend.d1 == "up" else "short"

