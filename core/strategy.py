from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple
import pandas as pd
import numpy as np

from core.config import settings


# === базовые утилиты ===

def ohlcv_to_df(ohlcv) -> pd.DataFrame:
    """
    Преобразует список OHLCV в DataFrame со столбцами:
    ts (ms), o, h, l, c, v
    """
    if isinstance(ohlcv, pd.DataFrame):
        df = ohlcv.copy()
    else:
        df = pd.DataFrame(ohlcv, columns=["ts", "o", "h", "l", "c", "v"])
    for col in ["o", "h", "l", "c", "v"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["ts"] = pd.to_numeric(df["ts"], errors="coerce").astype("int64")
    df = df.dropna().reset_index(drop=True)
    return df


def _sma(series: pd.Series, length: int) -> pd.Series:
    length = int(length)
    if length <= 1:
        return series.copy()
    return series.rolling(length, min_periods=length).mean()


def _atr(df: pd.DataFrame, length: int) -> pd.Series:
    """
    ATR по классике (Wilder), но с простым rolling-mean для стабильности.
    Требует столбцы h,l,c
    """
    h = df["h"].astype(float)
    l = df["l"].astype(float)
    c = df["c"].astype(float)
    prev_c = c.shift(1)
    tr = pd.concat([
        (h - l),
        (h - prev_c).abs(),
        (l - prev_c).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(int(length), min_periods=int(length)).mean()


# === тренд ===

@dataclass
class Trend:
    d1: str  # "up"/"down"
    h4: str  # "up"/"down"


def detect_trend(d1_df: pd.DataFrame, h4_df: pd.DataFrame) -> Trend:
    """
    D1: сравнение CLOSE vs SMA(D1_SMA, по-умолчанию 200).
        ВАЖНО: требуем достаточную глубину истории, иначе SMA200 может быть завышена.
        Берём не менее 250 дневных свечей (если есть), считаем SMA200 по close.
    H4: пересечение SMA(H4_FAST/H4_SLOW) по close (последнее значение).
    """
    d1 = d1_df.copy().reset_index(drop=True)
    h4 = h4_df.copy().reset_index(drop=True)

    # --- D1 тренд по SMA200 ---
    need_d1 = max(int(settings.D1_SMA) + 50, 250)  # запас истории
    if len(d1) < need_d1:
        # если истории мало, всё равно считаем, но min_periods учитывает длину
        pass
    d1_close = d1["c"].astype(float)
    sma_d1 = _sma(d1_close, int(settings.D1_SMA))
    last_close = float(d1_close.iloc[-1])
    last_sma = float(sma_d1.iloc[-1]) if not np.isnan(sma_d1.iloc[-1]) else last_close
    d1_trend = "up" if last_close >= last_sma else "down"

    # --- H4 тренд по пересечению SMA(FAST) и SMA(SLOW) ---
    h4_close = h4["c"].astype(float)
    sma_fast = _sma(h4_close, int(settings.H4_FAST))
    sma_slow = _sma(h4_close, int(settings.H4_SLOW))
    last_fast = float(sma_fast.iloc[-1]) if not np.isnan(sma_fast.iloc[-1]) else h4_close.iloc[-1]
    last_slow = float(sma_slow.iloc[-1]) if not np.isnan(sma_slow.iloc[-1]) else h4_close.iloc[-1]
    h4_trend = "up" if last_fast >= last_slow else "down"

    return Trend(d1=d1_trend, h4=h4_trend)


# === статусы и уровни ===

def previous_day_levels(d1_df: pd.DataFrame):
    """
    Возвращает уровни High/Low предыдущего дня.
    Предполагается, что d1_df отсортирован по времени по возрастанию.
    """
    if len(d1_df) < 2:
        raise ValueError("Not enough D1 bars for previous day levels")
    prev = d1_df.iloc[-2]
    class L:
        prev_high = float(prev["h"])
        prev_low = float(prev["l"])
    return L()


def build_status(d1_df: pd.DataFrame, h4_df: pd.DataFrame, h1_df: pd.DataFrame):
    """
    Возвращает (trend, levels, extra), где extra содержит дополнительные сведения.
    """
    tr = detect_trend(d1_df, h4_df)
    levels = previous_day_levels(d1_df)

    # Для удобства в статусе посчитаем SMA200 и последний close на D1
    d1_close = d1_df["c"].astype(float)
    sma200 = _sma(d1_close, int(settings.D1_SMA))
    extra = {
        "d1_last_close": float(d1_close.iloc[-1]),
        "d1_sma": float(sma200.iloc[-1]) if not np.isnan(sma200.iloc[-1]) else float(d1_close.iloc[-1]),
    }
    return tr, levels, extra


# === построение сделки ===

def plan_trade(trend: Trend, levels, h1_df: pd.DataFrame, info: dict) -> Optional[dict]:
    """
    Рассчитывает лимит, SL, TP по правилам (допуск по ATR, RR и т.п.)
    Возвращает dict со значениями или None, если построить нельзя.
    """
    side = info["side"]  # "long"/"short"
    idx_back = info["idx"] + info["candles_back"]
    level = levels.prev_low if side == "long" else levels.prev_high

    # ATR и допуски
    atr_series = _atr(h1_df, int(settings.ATR_PERIOD_H1))
    if idx_back not in atr_series.index:
        return None
    atr = float(atr_series.loc[idx_back])
    if atr <= 0:
        return None

    # вход: ретест уровня с недоходом ENTRY_OFFSET_ATR_PCT*ATR
    entry_offset = float(settings.ENTRY_OFFSET_ATR_PCT) * atr
    if side == "long":
        entry = level + entry_offset   # недоход к уровню снизу
    else:
        entry = level - entry_offset   # недоход к уровню сверху

    # стоп: за уровень +/− буфер
    stop_buf = float(settings.STOP_BUFFER_ATR_PCT) * atr
    if side == "long":
        sl = (levels.prev_low - stop_buf) if settings.STOP_MODE == "level" else (entry - stop_buf)
    else:
        sl = (levels.prev_high + stop_buf) if settings.STOP_MODE == "level" else (entry + stop_buf)

    # тейк из RR
    rr = float(settings.RR)
    if side == "long":
        tp = entry + rr * (entry - sl)
    else:
        tp = entry - rr * (sl - entry)

    return {
        "side": side,
        "entry": float(entry),
        "sl": float(sl),
        "tp": float(tp),
        "reason": "ok",
    }
