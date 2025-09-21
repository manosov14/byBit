from __future__ import annotations

from typing import List
import pandas as pd

from .models import LevelCandidate


def collect_level_candidates(d1_df: pd.DataFrame, day_idx: int, lookback: int) -> List[LevelCandidate]:
    d1 = d1_df.reset_index(drop=True)
    if day_idx <= 0 or day_idx >= len(d1):
        return []

    start = max(0, day_idx - int(lookback))
    levels: List[LevelCandidate] = []

    for i in range(day_idx - 1, start - 1, -1):
        row = d1.iloc[i]
        age = day_idx - i
        ts = int(row["ts"])
        day = pd.to_datetime(ts, unit="ms")
        label_high = "prev_high" if age == 1 else "high"
        label_low = "prev_low" if age == 1 else "low"
        levels.append(LevelCandidate(
            value=float(row["h"]),
            side="short",
            ts=ts,
            label=f"{label_high}@{day.date()}",
            age=int(age),
        ))
        levels.append(LevelCandidate(
            value=float(row["l"]),
            side="long",
            ts=ts,
            label=f"{label_low}@{day.date()}",
            age=int(age),
        ))

    return levels

