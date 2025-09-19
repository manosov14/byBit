# core/exchange.py
from __future__ import annotations

import ccxt
import logging
from typing import Iterable, List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

from core.config import settings

log = logging.getLogger("exchange")


class Exchange:
    """
    Тонкая обёртка над ccxt.bybit, заточенная под деривативы (linear swap).
    - Не лезем на спот-эндпоинты (которые иногда таймаутят)
    - Грузим маркеты с ретраями
    - Для OHLCV добавляем params(category='linear')
    """

    def __init__(self):
        # Включаем лимитер CCXT и увеличиваем таймаут
        self.x = ccxt.bybit({
            "enableRateLimit": True,
            "timeout": 20000,   # 20s
            "options": {
                # По умолчанию работаем со свопами
                "defaultType": "swap",
                "defaultSubType": "linear",
                # некоторые методы bybit завязаны на category
                "defaultSettle": "USDT",
            },
        })
        self._loaded = False

    def _params(self):
        # Везде подставляем категорию линейных деривативов
        return {"category": "linear"}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.7, min=1, max=4))
    def _load_markets_once(self):
        # Пытаемся загрузить только нужный тип. Параметр type="swap" + options выше —
        # это подсказка для bybit, чтобы не трогать spot.
        log.info("Loading Bybit markets (swap/linear only)…")
        self.x.load_markets({"type": "swap"})
        self._loaded = True
        log.info("Markets loaded: %d", len(self.x.markets))

    def load(self):
        if self._loaded:
            return
        try:
            self._load_markets_once()
        except Exception as e:
            log.warning("load_markets retry exhausted: %s", e)
            # как фолбэк: попробуем ещё раз с увеличенным таймаутом
            try:
                self.x.timeout = 30000
                self._load_markets_once()
            except Exception as ee:
                log.error("load_markets failed: %s", ee)
                raise

    # --------- публичные методы, используемые хендлерами ---------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.7, min=1, max=4))
    def fetch_ohlcv(
        self,
        timeframe: str,
        *,
        limit: int = 200,
        since: Optional[int] = None,
        symbol: Optional[str] = None,
    ):
        if not self._loaded:
            self.load()
        sym = symbol or settings.SYMBOL
        # В bybit для перпов лучше указывать символ в формате "XXX/USDT:USDT"
        return self.x.fetch_ohlcv(sym, timeframe, since=since, limit=limit, params=self._params())

    def top_perp_symbols(self, n: int) -> Iterable[str]:
        """
        Возвращает n самых ликвидных линейных контрактов USDT-маржи.
        Если рынки не загружены — загрузим. Если по какой-то причине нет маркетов,
        вернём базовый SYMBOL.
        """
        try:
            if not self._loaded:
                self.load()
        except Exception as e:
            log.warning("top_perp_symbols: load failed (%s), fallback to base symbol", e)
            return [settings.SYMBOL]

        # Фильтруем по деривативам USDT (linear swap)
        filtered: List[str] = []
        for m in self.x.markets.values():
            try:
                if not m.get("active", True):
                    continue
                if m.get("type") != "swap":
                    continue
                if m.get("linear") is not True:
                    continue
                if str(m.get("quote", "")).upper() != "USDT":
                    continue
                # unified символ в ccxt выглядит как 'BTC/USDT:USDT'
                filtered.append(m["symbol"])
            except Exception:
                continue

        # Сортируем по приблизительной «ликвидности» (берём info['lotSizeFilter']['maxOrderQty'] или tickSize/contractSize как суррогат)
        # Если данных мало — просто по алфавиту.
        try:
            def score(m):
                md = self.x.markets.get(m, {})
                info = md.get("info") or {}
                # попытаемся взять оборот как оценку
                v = float(info.get("turnover24h", 0) or 0)
                return -v
            filtered.sort(key=score)
        except Exception:
            filtered.sort()

        return filtered[: max(1, int(n or 1))]
