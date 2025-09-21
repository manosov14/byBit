from __future__ import annotations

import pandas as pd


def ohlcv_to_df(ohlcv) -> pd.DataFrame:
    """Normalize raw OHLCV data into a typed pandas DataFrame."""

    if isinstance(ohlcv, pd.DataFrame):
        df = ohlcv.copy()
    else:
        df = pd.DataFrame(ohlcv, columns=["ts", "o", "h", "l", "c", "v"])
    for col in ("o", "h", "l", "c", "v"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["ts"] = pd.to_numeric(df["ts"], errors="coerce").astype("int64")
    return df.dropna().reset_index(drop=True)


def sma(series: pd.Series, length: int) -> pd.Series:
    length = max(1, int(length))
    if length <= 1:
        return series.copy()
    return series.rolling(length, min_periods=length).mean()


def atr(df: pd.DataFrame, length: int) -> pd.Series:
    """Average True Range using classic Wilder logic."""

    h = df["h"].astype(float)
    l = df["l"].astype(float)
    c = df["c"].astype(float)
    prev_c = c.shift(1)
    tr = pd.concat([
        (h - l),
        (h - prev_c).abs(),
        (l - prev_c).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(int(length), min_periods=int(length)).mean()


def volume_ma(series: pd.Series, length: int) -> pd.Series:
    length = max(1, int(length))
    return series.rolling(length, min_periods=length).mean()

