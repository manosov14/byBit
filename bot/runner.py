from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

import logging

from core.config import settings
from core.exchange import Exchange
from core.strategy import (
    BreakoutInfo,
    FalseBreakoutStrategy,
    PlannedTrade,
    StrategyContext,
)
from .marketdata import MarketDataService
from .reporting import format_signal_message, make_event_id
from .risk import calc_qty_from_risk, open_position_count, round_price, tick_size
from .state import PersistentState, StateStore


log = logging.getLogger("bot.runner")


@dataclass(slots=True)
class SignalCandidate:
    symbol: str
    breakout: BreakoutInfo
    trade: PlannedTrade


@dataclass(slots=True)
class SignalDecision:
    symbol: str
    message: str
    executed: bool = False
    skip_reason: Optional[str] = None


class FalseBreakoutRunner:
    def __init__(self, exchange: Exchange, market_data: MarketDataService, state_store: StateStore):
        self.exchange = exchange
        self.market_data = market_data
        self.state_store = state_store
        self.strategy = FalseBreakoutStrategy()

    def _cooldown_passed(self, state: PersistentState, symbol: str) -> bool:
        cooldown_hours = float(settings.COOLDOWN_HOURS or 0)
        if cooldown_hours <= 0:
            return True
        last_at = state.last_notified_at.get(symbol)
        if not last_at:
            return True
        last_dt = datetime.fromtimestamp(last_at / 1000, tz=timezone.utc)
        return datetime.now(timezone.utc) - last_dt >= timedelta(hours=cooldown_hours)

    def analyze_symbol(self, symbol: str) -> Optional[SignalCandidate]:
        bundle = self.market_data.fetch_bundle_with_timeframes(
            symbol,
            tf_h1=settings.TF_H1,
            tf_d1=settings.TF_D1,
            tf_h4=settings.TF_H4,
            h1_limit=300,
            d1_limit=200,
            h4_limit=400,
        )

        d1 = bundle.d1
        if len(d1) < 2:
            return None

        day_idx = len(d1) - 1
        day_row = d1.iloc[day_idx]
        next_ts = int(d1.iloc[day_idx + 1]["ts"]) if day_idx + 1 < len(d1) else None

        h4_scope = bundle.h4
        if next_ts is not None:
            h4_scope = h4_scope[h4_scope["ts"] <= next_ts]

        ctx = StrategyContext(d1=d1.iloc[: day_idx + 1], h4=h4_scope, h1=bundle.h1)
        trend = self.strategy.detect_trend(ctx)
        side = self.strategy.determine_side(trend)
        if not side:
            return None

        levels = self.strategy.collect_levels(ctx, day_idx, int(settings.LEVEL_LOOKBACK_DAYS))
        start_ts = int(day_row["ts"])
        breakouts = self.strategy.find_breakouts(ctx, levels, start_ts, next_ts, allowed_side=side)
        if not breakouts:
            return None

        breakout = breakouts[0]
        trade = self.strategy.plan_trade(breakout, tick_size=tick_size(self.exchange, symbol))
        if not trade:
            return None

        trade.entry = round_price(self.exchange, symbol, trade.entry)
        trade.sl = round_price(self.exchange, symbol, trade.sl)
        trade.tp = round_price(self.exchange, symbol, trade.tp)
        return SignalCandidate(symbol=symbol, breakout=breakout, trade=trade)

    def process_signal(self, candidate: SignalCandidate) -> Optional[SignalDecision]:
        state = self.state_store.load()
        event_id = make_event_id(candidate.symbol, candidate.breakout)
        if bool(settings.DEDUP_BY_BREAKOUT) and state.last_events.get(candidate.symbol) == event_id:
            return None
        if not self._cooldown_passed(state, candidate.symbol):
            return None

        state.last_events[candidate.symbol] = event_id
        state.last_notified_at[candidate.symbol] = int(time.time() * 1000)
        self.state_store.save(state)

        message = format_signal_message(candidate.symbol, candidate.trade, candidate.breakout)

        if int(settings.DRY_RUN):
            return SignalDecision(candidate.symbol, message + "  [DRY_RUN]", executed=False)

        max_positions = int(settings.MAX_OPEN_POSITIONS or 0)
        if max_positions > 0 and open_position_count(self.exchange) >= max_positions:
            reason = f"positions>={max_positions}"
            return SignalDecision(candidate.symbol, message + f"  [SKIP: {reason}]", executed=False, skip_reason=reason)

        try:
            bal = self.exchange.x.fetch_balance().get("USDT", {})
            free = float(bal.get("free") or bal.get("total") or 0.0)
        except Exception as exc:
            return SignalDecision(candidate.symbol, message + f"  [BALANCE ERROR: {exc}]", executed=False, skip_reason="balance")

        qty = calc_qty_from_risk(
            self.exchange,
            candidate.symbol,
            free,
            candidate.trade.entry,
            candidate.trade.sl,
            risk_pct=settings.RISK_PCT,
        )
        if qty <= 0:
            return SignalDecision(candidate.symbol, message + "  [SKIP: qty<=0]", executed=False, skip_reason="qty")

        try:
            order = self.exchange.place_bracket_order(
                candidate.symbol,
                candidate.trade.side,
                qty,
                candidate.trade.entry,
                candidate.trade.sl,
                candidate.trade.tp,
                post_only=bool(int(os.getenv("POST_ONLY", "1") or "1")),
                tif=os.getenv("TIME_IN_FORCE", "GTC") or "GTC",
                tp_mode=os.getenv("TP_MODE", "limit") or "limit",
                sl_mode=os.getenv("SL_MODE", "market") or "market",
            )
            log.info("Order sent for %s: %s", candidate.symbol, order)
            return SignalDecision(
                candidate.symbol,
                message + f"  [ORDER SENT qty={qty}]",
                executed=True,
            )
        except Exception as exc:
            return SignalDecision(
                candidate.symbol,
                message + f"  [ORDER ERROR: {exc}]",
                executed=False,
                skip_reason="order",
            )

    def check_symbol(self, symbol: str) -> Optional[SignalDecision]:
        candidate = self.analyze_symbol(symbol)
        if not candidate:
            return None
        return self.process_signal(candidate)

