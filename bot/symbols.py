from __future__ import annotations

import os
from typing import Iterable, List, Sequence, Set

from core.config import settings
from core.exchange import Exchange

DEFAULT_TOP: Sequence[str] = (
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT", "XRP/USDT:USDT",
    "DOGE/USDT:USDT", "ADA/USDT:USDT", "TON/USDT:USDT", "TRX/USDT:USDT", "LINK/USDT:USDT",
    "ARB/USDT:USDT", "OP/USDT:USDT", "APT/USDT:USDT", "NEAR/USDT:USDT", "BCH/USDT:USDT",
    "LTC/USDT:USDT", "MATIC/USDT:USDT", "DOT/USDT:USDT", "SUI/USDT:USDT", "ATOM/USDT:USDT",
)


def build_linear_perp_set(ex: Exchange) -> Set[str]:
    try:
        mkts = ex.x.load_markets()
    except Exception:
        mkts = ex.x.markets or {}
    allowed: Set[str] = set()
    for sym, meta in mkts.items():
        try:
            if (
                meta.get("swap") is True
                and meta.get("linear") in (True, "USDT")
                and meta.get("active", True) is True
            ):
                allowed.add(sym.upper())
        except Exception:
            continue
    return allowed


def sanitize_symbols(symbols: Iterable[str], allowed: Set[str]) -> List[str]:
    out: List[str] = []
    for raw in symbols:
        symbol = raw.strip().upper()
        if not symbol:
            continue
        if symbol not in allowed:
            continue
        if symbol not in out:
            out.append(symbol)
    base = settings.SYMBOL.upper()
    if base in allowed and base not in out:
        out.append(base)
    return out


def resolve_universe(ex: Exchange, allowed: Set[str]) -> List[str]:
    manual = os.getenv("UNIVERSE_SYMBOLS", "")
    if manual:
        arr = sanitize_symbols(manual.split(","), allowed)
        if arr:
            return arr

    top_n = int(os.getenv("UNIVERSE_TOP_N", "0") or "0")
    if top_n > 0:
        return sanitize_symbols(DEFAULT_TOP[:top_n], allowed)

    return sanitize_symbols([settings.SYMBOL], allowed)

