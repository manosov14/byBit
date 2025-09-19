
import numpy as np
import pandas as pd

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def atr(df: pd.DataFrame, period: int = 14) -> float:
    high = df["h"].astype(float).values
    low = df["l"].astype(float).values
    close = df["c"].astype(float).values
    tr = np.maximum(high[1:], close[:-1]) - np.minimum(low[1:], close[:-1])
    if len(tr) < period:
        return float(np.mean(tr)) if len(tr) else float("nan")
    return float(pd.Series(tr).rolling(period).mean().iloc[-1])
