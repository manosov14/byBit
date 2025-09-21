from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from core.config import settings
from core.exchange import Exchange
from core.strategy import ohlcv_to_df


@dataclass(slots=True)
class MarketDataBundle:
    d1: pd.DataFrame
    h4: pd.DataFrame
    h1: pd.DataFrame


class MarketDataService:
    def __init__(self, exchange: Exchange):
        self.exchange = exchange

    def fetch_ohlcv(
        self,
        timeframe: str,
        *,
        limit: int,
        symbol: str | None = None,
        since: Optional[int] = None,
        attempts: int = 3,
        sleep_base: float = 0.7,
    ):
        last_err = None
        for attempt in range(attempts):
            try:
                return self.exchange.fetch_ohlcv(
                    timeframe,
                    limit=limit,
                    symbol=symbol,
                    since=since,
                )
            except Exception as exc:  # pragma: no cover - network errors
                last_err = exc
                msg = str(exc).lower()
                if any(token in msg for token in ("timeout", "timed out", "market/time", "read operation timed out")) and attempt < attempts - 1:
                    time.sleep(sleep_base * (attempt + 1))
                    continue
                break
        raise last_err  # type: ignore[misc]

    def fetch_bundle(
        self,
        symbol: str,
        *,
        h1_limit: int = 300,
        d1_limit: int = 200,
        h4_limit: int = 400,
    ) -> MarketDataBundle:
        h1_raw = self.fetch_ohlcv(settings.TF_H1, limit=h1_limit, symbol=symbol)
        d1_raw = self.fetch_ohlcv(settings.TF_D1, limit=d1_limit, symbol=symbol)
        h4_raw = self.fetch_ohlcv(settings.TF_H4, limit=h4_limit, symbol=symbol)
        return MarketDataBundle(
            d1=ohlcv_to_df(d1_raw),
            h4=ohlcv_to_df(h4_raw),
            h1=ohlcv_to_df(h1_raw),
        )

    def fetch_bundle_with_timeframes(
        self,
        symbol: str,
        *,
        tf_h1: str,
        tf_d1: str,
        tf_h4: str,
        h1_limit: int,
        d1_limit: int,
        h4_limit: int,
    ) -> MarketDataBundle:
        h1_raw = self.fetch_ohlcv(tf_h1, limit=h1_limit, symbol=symbol)
        d1_raw = self.fetch_ohlcv(tf_d1, limit=d1_limit, symbol=symbol)
        h4_raw = self.fetch_ohlcv(tf_h4, limit=h4_limit, symbol=symbol)
        return MarketDataBundle(
            d1=ohlcv_to_df(d1_raw),
            h4=ohlcv_to_df(h4_raw),
            h1=ohlcv_to_df(h1_raw),
        )

