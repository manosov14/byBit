from __future__ import annotations

from typing import Optional

from core.config import settings
from core.exchange import Exchange


def tick_size(ex: Exchange, symbol: str) -> float:
    try:
        market = ex.x.market(symbol) or {}
        precision = (market.get("precision") or {}).get("price")
        if isinstance(precision, int):
            return 10.0 ** (-precision)
        info = market.get("info") or {}
        price_filter = info.get("priceFilter") or {}
        ts = price_filter.get("tickSize")
        if ts:
            return float(ts)
    except Exception:
        pass
    return 0.01


def round_price(ex: Exchange, symbol: str, value: float) -> float:
    try:
        return float(ex.x.price_to_precision(symbol, value))
    except Exception:
        return float(value)


def open_position_count(ex: Exchange) -> int:
    try:
        raw = ex.x.fetch_positions(params={"category": "linear"})
    except Exception:
        try:
            raw = ex.x.fetch_positions()
        except Exception:
            return 0

    if not raw:
        return 0

    count = 0
    for pos in raw:
        size = None
        for key in ("contracts", "contractSize", "size", "positionSize"):
            if key in pos:
                size = pos.get(key)
                break
        if size is None:
            info = pos.get("info") or {}
            for key in ("size", "positionSize", "positionAmt", "positionValue"):
                if info.get(key) is not None:
                    size = info.get(key)
                    break
        try:
            if size is not None and abs(float(size)) > 0:
                count += 1
        except Exception:
            continue
    return count


def calc_qty_from_risk(
    ex: Exchange,
    symbol: str,
    free_usdt: float,
    entry: float,
    sl: float,
    *,
    risk_pct: Optional[float] = None,
) -> float:
    risk_pct = float(risk_pct if risk_pct is not None else settings.RISK_PCT)
    risk_usdt = max(0.0, float(free_usdt) * risk_pct)
    distance = abs(float(entry) - float(sl))
    if risk_usdt <= 0 or distance <= 0:
        return 0.0
    qty = risk_usdt / distance
    try:
        qty = float(ex.x.amount_to_precision(symbol, qty))
    except Exception:
        pass
    return max(0.0, qty)

