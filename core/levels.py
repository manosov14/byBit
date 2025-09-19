
import pandas as pd
from dataclasses import dataclass

@dataclass
class DayLevels:
    prev_high: float
    prev_low: float

def previous_day_levels(df_d1: pd.DataFrame) -> DayLevels:
    # df columns: ts o h l c v (ascending)
    if len(df_d1) < 2:
        return DayLevels(float("nan"), float("nan"))
    prev = df_d1.iloc[-2]
    return DayLevels(prev_high=float(prev["h"]), prev_low=float(prev["l"]))
