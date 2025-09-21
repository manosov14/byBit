# core/orders.py
from __future__ import annotations

import os
import logging
from typing import Any, Dict, Optional, List

from core.exchange import Exchange
from core.config import settings

log = logging.getLogger("orders")

# For Bybit USDT linear perps
LINEAR_PARAMS = {"category": "linear"}


def _pp(ex: Exchange, symbol: str, price: float) -> float:
    try:
        return float(ex.x.price_to_precision(symbol, price))
    except Exception:
        return float(price)


def _ap(ex: Exchange, symbol: str, amount: float) -> float:
    try:
        return float(ex.x.amount_to_precision(symbol, amount))
    except Exception:
        return float(amount)


def _norm_risk(x) -> float:
    """
    Нормализует значение риска: поддерживает доли (0.01 = 1%) и проценты (1 = 1%).
    Любое значение >= 1 трактуется как процент и делится на 100.
    Ограничивает результат диапазоном 0..1.
    """
    try:
        v = float(x)
    except Exception:
        return 0.0
    if v <= 0:
        return 0.0
    if v >= 1.0:
        v = v / 100.0
    return max(0.0, min(1.0, v))


def _try_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def _ensure_auth(ex: Exchange) -> bool:
    """
    Страховка: проставляем apiKey/secret в ex.x из .env/settings,
    если по какой-то причине обменник их не увидел.
    Поддерживаем опциональный тестнет через BYBIT_TESTNET=1.
    """
    # sandbox (по желанию)
    try:
        if str(os.getenv("BYBIT_TESTNET", "0")).lower() in ("1", "true", "yes"):
            try:
                ex.x.set_sandbox_mode(True)
            except Exception:
                pass
    except Exception:
        pass

    # ищем ключи в .env и в settings
    k = os.getenv("BYBIT_API_KEY") or getattr(settings, "BYBIT_API_KEY", None) or os.getenv("API_KEY")
    s = os.getenv("BYBIT_API_SECRET") or getattr(settings, "BYBIT_API_SECRET", None) or os.getenv("API_SECRET")

    if k and s:
        try:
            ex.x.apiKey = k
            ex.x.secret = s
        except Exception:
            pass

    return bool(getattr(ex.x, "apiKey", None) and getattr(ex.x, "secret", None))


def _account_equity_usdt_ccxt(ex: Exchange) -> float:
    """Основной путь: ccxt.fetch_balance(...)."""
    bal = {}
    try:
        bal = ex.x.fetch_balance(params=LINEAR_PARAMS) or {}
    except Exception:
        try:
            bal = ex.x.fetch_balance() or {}
        except Exception:
            return 0.0

    usdt = bal.get("USDT") or {}
    candidates: List[float] = []
    for k in ("equity", "total", "free", "used", "walletBalance"):
        v = _try_float(usdt.get(k))
        if v is not None:
            candidates.append(v)

    info = bal.get("info") or {}
    for k in ("equity", "totalEquity", "walletBalance"):
        v = _try_float(info.get(k))
        if v is not None:
            candidates.append(v)

    return max(candidates) if candidates else 0.0


def _account_equity_usdt_v5(ex: Exchange) -> float:
    """Резервный путь: сырой баланс кошелька Bybit v5 (UNIFIED, затем CONTRACT)."""
    try:
        raw = getattr(ex.x, "privateGetV5AccountWalletBalance", None)
        if raw is None:
            return 0.0

        for acc_type in ("UNIFIED", "CONTRACT"):
            try:
                resp = raw({"accountType": acc_type})
                res = (resp or {}).get("result") or {}
                arr = res.get("list") or []
                if not arr:
                    continue
                coins = arr[0].get("coin") or []
                for c in coins:
                    if (c.get("coin") or "").upper() == "USDT":
                        candidates = []
                        for k in ("equity", "walletBalance", "availableToWithdraw", "availableBalance", "cashBalance"):
                            v = _try_float(c.get(k))
                            if v is not None:
                                candidates.append(v)
                        if candidates:
                            return max(candidates)
            except Exception:
                continue
    except Exception:
        pass
    return 0.0


def _account_equity_usdt(ex: Exchange) -> float:
    """
    Компонуем получение equity:
      1) ccxt.fetch_balance(params={'category':'linear'})
      2) сырой баланс кошелька v5 (UNIFIED/CONTRACT)
      3) EQUITY_FALLBACK_USDT из окружения (опционально)
    """
    eq = _account_equity_usdt_ccxt(ex)
    if eq and eq > 0:
        return eq

    eq = _account_equity_usdt_v5(ex)
    if eq and eq > 0:
        return eq

    fb = _try_float(os.getenv("EQUITY_FALLBACK_USDT"))
    if fb and fb > 0:
        log.warning("Equity fallback used: %.2f USDT (EQUITY_FALLBACK_USDT)", fb)
        return fb

    return 0.0


def _get_min_qty(ex: "Exchange", symbol: str) -> float:
    """Возвращает минимальный размер лота на бирже для символа или 0.0, если неизвестно."""
    try:
        ex.load()
        m = ex.x.markets.get(symbol) or ex.x.market(symbol)
        limits = (m or {}).get("limits", {})
        amount = limits.get("amount", {})
        mn = amount.get("min")
        return float(mn) if mn is not None else 0.0
    except Exception:
        return 0.0


def _calc_qty_from_risk_linear_usdt(
    ex: Exchange,
    symbol: str,
    balance_usdt: float,
    risk_frac: float,
    entry: float,
    sl: float,
) -> float:
    """
    qty = (risk_frac * balance_usdt) / |entry - sl|, округляется по точности биржи.
    Если qty < минимального лота биржи и ALLOW_MIN_LOT_OVERRIDE=1 (по умолчанию) -> увеличиваем до min lot.
    """
    rf = max(0.0, min(1.0, float(risk_frac or 0.0)))
    risk_usdt = max(0.0, float(balance_usdt) * rf)
    dist = abs(float(entry) - float(sl))
    if risk_usdt <= 0 or dist <= 0:
        return 0.0
    qty = risk_usdt / dist
    try:
        qty = float(ex.x.amount_to_precision(symbol, qty))
    except Exception:
        pass

    # Принудительно применяем min lot, если включено
    try:
        min_qty = _get_min_qty(ex, symbol)
    except Exception:
        min_qty = 0.0
    allow_min_override = bool(int(os.getenv("ALLOW_MIN_LOT_OVERRIDE", "1") or "1"))
    if allow_min_override and min_qty and qty > 0 and qty < min_qty:
        qty = _ap(ex, symbol, min_qty)

    return max(0.0, qty)


def cancel_all_for_symbol(ex: Exchange, symbol: str) -> Any:
    """Отменяет ВСЕ активные ордера по символу (включая TP/SL)."""
    _ensure_auth(ex)
    return ex.x.cancel_all_orders(symbol, params=LINEAR_PARAMS)


def place_bracket_order(
    ex: Exchange,
    symbol: str,
    side: str,               # "long" | "short"
    qty: float,
    entry: float,
    sl: float,
    tp: float,
    *,
    post_only: bool = True,
    tif: str = "GTC",
    tp_mode: str = "limit",  # "limit"|"market"
    sl_mode: str = "market", # "limit"|"market"
) -> Dict[str, Any]:
    """
    Пытаемся выставить лимитный вход со встроенными TP/SL. Если отклонено -> переходим к
    лимиту на вход + отдельным reduceOnly TP/SL.
    """
    try:
        ex.load()
    except Exception as e:
        log.warning("ex.load() failed: %s", e)

    # ensure credentials
    if not _ensure_auth(ex):
        raise RuntimeError('BYBIT_API_KEY/BYBIT_API_SECRET не найдены в окружении (или не подхватились).')

    side_ccxt = "buy" if side.lower() == "long" else "sell"
    qty = _ap(ex, symbol, float(qty))
    price = _pp(ex, symbol, float(entry))
    tp_price = _pp(ex, symbol, float(tp))
    sl_price = _pp(ex, symbol, float(sl))

    tif = (tif or "GTC").upper()
    if tif not in ("GTC", "IOC", "FOK"):
        tif = "GTC"

    base_params: Dict[str, Any] = {
        **LINEAR_PARAMS,
        "timeInForce": tif,
        "postOnly": bool(post_only),
    }

    try:
        params_one = {**base_params, "takeProfit": tp_price, "stopLoss": sl_price, "reduceOnly": False}
        order = ex.x.create_order(symbol, type="limit", side=side_ccxt, amount=qty, price=price, params=params_one)
        return {"entry": order, "tp": {"inline": True, "price": tp_price}, "sl": {"inline": True, "price": sl_price}}
    except Exception as e:
        log.info("Inline TP/SL failed or unsupported: %s. Fallback to multi-step.", e)

    entry_order = ex.x.create_order(
        symbol, type="limit", side=side_ccxt, amount=qty, price=price, params={**base_params, "reduceOnly": False}
    )

    tp_side = "sell" if side_ccxt == "buy" else "buy"
    try:
        if tp_mode == "limit":
            tp_order = ex.x.create_order(
                symbol, "limit", tp_side, qty, tp_price, params={**LINEAR_PARAMS, "reduceOnly": True}
            )
        else:
            tp_order = ex.x.create_order(
                symbol, "take_profit_market", tp_side, qty, tp_price, params={**LINEAR_PARAMS, "reduceOnly": True}
            )
    except Exception as e:
        log.warning("TP create failed: %s", e)
        tp_order = {"error": str(e)}

    sl_side = "sell" if side_ccxt == "buy" else "buy"
    try:
        if sl_mode == "limit":
            sl_order = ex.x.create_order(
                symbol, "limit", sl_side, qty, sl_price, params={**LINEAR_PARAMS, "reduceOnly": True}
            )
        else:
            sl_order = ex.x.create_order(
                symbol, "stop_market", sl_side, qty, sl_price, params={**LINEAR_PARAMS, "reduceOnly": True}
            )
    except Exception as e:
        log.warning("SL create failed: %s", e)
        sl_order = {"error": str(e)}

    return {"entry": entry_order, "tp": tp_order, "sl": sl_order}


async def _cmd_order_impl(messenger, ex: Exchange, args: str):
    """
    Команды:
    /order place <SYMBOL> <long|short> entry=.. sl=.. tp=.. [qty=.. | risk=..] [post=0|1] [tif=GTC|IOC|FOK] [tp_mode=limit|market] [sl_mode=market|limit]
    /order cancel <SYMBOL>
    """
    try:
        parts = [p for p in (args or "").strip().split() if p.strip()]
        if not parts:
            await messenger.send_text(
                "Формат:\n"
                "/order place <SYMBOL> <long|short> entry=.. sl=.. tp=.. [qty=.. | risk=..] [post=0|1] [tif=GTC|IOC|FOK] [tp_mode=limit|market] [sl_mode=market|limit]\n"
                "/order cancel <SYMBOL>"
            )
            return

        action = parts[0].lower()
        if action == "cancel":
            if len(parts) < 2:
                await messenger.send_text("Укажи символ: /order cancel <SYMBOL>")
                return
            symbol = parts[1].upper()
            _ensure_auth(ex)
            cancel_all_for_symbol(ex, symbol)
            await messenger.send_text(f"[{symbol}] Все активные ордера (включая TP/SL) отменены.")
            return

        if action != "place":
            await messenger.send_text("Неизвестное действие. Доступно: place | cancel")
            return

        if len(parts) < 3:
            await messenger.send_text("Формат:\n/order place <SYMBOL> <long|short> entry=.. sl=.. tp=.. [qty=.. | risk=..]")
            return

        symbol = parts[1].upper()
        side = parts[2].lower()
        if side not in ("long", "short"):
            await messenger.send_text("side должен быть long|short")
            return

        post_only = bool(int(os.getenv("POST_ONLY", "1") or "1"))
        tif = os.getenv("TIME_IN_FORCE", "GTC") or "GTC"
        tp_mode = (os.getenv("TP_MODE", "limit") or "limit").lower()
        sl_mode = (os.getenv("SL_MODE", "market") or "market").lower()
        debug = bool(int(os.getenv("ORDER_DEBUG", "1") or "1"))

        kv: Dict[str, str] = {}
        for p in parts[3:]:
            if "=" in p:
                k, v = p.split("=", 1)
                kv[k.lower()] = v

        for req in ("entry", "sl", "tp"):
            if req not in kv:
                await messenger.send_text("Нужно указать entry=, sl=, tp=")
                return

        entry = float(kv["entry"]); sl = float(kv["sl"]); tp = float(kv["tp"])

        qty = float(kv["qty"]) if "qty" in kv else None
        risk = _norm_risk(kv["risk"]) if "risk" in kv else None

        if "post" in kv:
            post_only = bool(int(kv["post"]))
        if "tif" in kv:
            tif = kv["tif"].upper()
        if "tp_mode" in kv:
            tp_mode = kv["tp_mode"].lower()
        if "sl_mode" in kv:
            sl_mode = kv["sl_mode"].lower()

        equity_used = None
        risk_used = None

        # 1) risk=... provided
        if qty is None and risk is not None:
            equity_used = _account_equity_usdt(ex)
            risk_used = risk
            qty = _calc_qty_from_risk_linear_usdt(ex, symbol, equity_used, risk, entry, sl)

        # 2) neither qty nor risk -> use RISK_PCT from .env/settings
        if qty is None and risk is None:
            try:
                default_risk = _norm_risk(getattr(settings, "RISK_PCT", 0.01) or 0.01)
            except Exception:
                default_risk = 0.01
            equity_used = _account_equity_usdt(ex)
            risk_used = default_risk
            qty = _calc_qty_from_risk_linear_usdt(ex, symbol, equity_used, default_risk, entry, sl)

        if qty is None or qty <= 0:
            await messenger.send_text("Укажи qty= или risk= (0..1)")
            return

        result = place_bracket_order(
            ex, symbol, side, qty, entry, sl, tp,
            post_only=post_only, tif=tif, tp_mode=tp_mode, sl_mode=sl_mode
        )

        if debug:
            min_qty = _get_min_qty(ex, symbol)
            key_tail = (getattr(ex.x, "apiKey", "") or "")[-4:]
            await messenger.send_text(
                f"[{symbol}] ORDER SENT:\n"
                f"  side={side}, entry={entry:.6f}, sl={sl:.6f}, tp={tp:.6f}\n"
                f"  equity_used={equity_used}, risk_used={risk_used}, qty={qty} (min_qty={min_qty})\n"
                f"  tif={tif}, post_only={int(post_only)}, tp_mode={tp_mode}, sl_mode={sl_mode}\n"
                f"  auth={'OK' if _ensure_auth(ex) else 'MISSING'} key=***{key_tail}\n"
                f"  raw={result}"
            )
        else:
            await messenger.send_text(
                f"[{symbol}] ORDER SENT: side={side} qty={qty} entry={entry:.6f} SL={sl:.6f} TP={tp:.6f}\n{result}"
            )

    except Exception as e:
        await messenger.send_text(f"Ошибка /order: {e}")


def register_order_commands(messenger, ex: Exchange):
    """Регистрирует команду /order в мессенджере бота."""
    async def _handler(args: str):
        await _cmd_order_impl(messenger, ex, args)

    messenger.add_command("/order", _handler)
    messenger.add_command("/Order", _handler)  # alias
