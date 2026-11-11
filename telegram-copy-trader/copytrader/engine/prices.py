"""Market price lookup for validation (never used for order prices).

Order of sources: the first connected broker that can resolve the symbol
(mid of its quote), then a public ccxt exchange (USDT perpetuals). Results
are cached for a few seconds. ``price()`` never raises: unknown -> None.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Callable, Sequence
from typing import Any

from ..brokers.base import Broker
from ..models import MarketType, Quote

logger = logging.getLogger(__name__)

_CacheKey = tuple[str, str | None, str | None, str | None]
BrokerSource = Sequence[Broker] | Callable[[], Sequence[Broker]]


def _valid(x: Any) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(x) and x > 0


def quote_mid(q: Quote) -> float | None:
    """Mid of bid/ask; falls back to last when the book side is missing."""
    if _valid(q.bid) and _valid(q.ask):
        return (q.bid + q.ask) / 2.0
    return float(q.last) if _valid(q.last) else None


def ticker_price(ticker: dict[str, Any] | None) -> float | None:
    if not ticker:
        return None
    bid, ask = ticker.get("bid"), ticker.get("ask")
    if _valid(bid) and _valid(ask):
        return (float(bid) + float(ask)) / 2.0
    for key in ("last", "close", "mark"):
        if _valid(ticker.get(key)):
            return float(ticker[key])
    return None


async def fetch_public_price(ex: Any, base: str, timeout_sec: float) -> float | None:
    """Mid of ``<BASE>/USDT:USDT`` on a ccxt-like exchange in the SIGNAL's price scale (1000-prefix aware)."""
    for prefix, mult in (("", 1.0), ("1000", 1000.0)):  # PEPE trades as 1000PEPE on most venues
        symbol = f"{prefix}{base}/USDT:USDT"
        try:
            ticker = await asyncio.wait_for(ex.fetch_ticker(symbol), timeout_sec + 5)
        except Exception as exc:
            logger.debug("Public price for %s unavailable: %s", symbol, exc)
            continue
        price = ticker_price(ticker)
        if price is not None:
            return price / mult
    return None


class ReferencePriceOracle:
    """Mid price of a coin's USDT perpetual on a named public exchange (the venue the signal charts come
    from), used to shift signal levels onto a broker whose price is offset. One public ccxt client per
    exchange id, results cached a few seconds. ``mid()`` never raises: unknown -> None."""

    def __init__(self, *, cache_ttl_sec: float = 3.0, timeout_sec: float = 10.0,
                 exchanges: dict[str, Any] | None = None) -> None:
        """``exchanges``: prebuilt ccxt-like objects by id (tests)."""
        self._ttl = cache_ttl_sec
        self._timeout = timeout_sec
        self._clients: dict[str, Any] = dict(exchanges or {})
        self._cache: dict[tuple[str, str], tuple[float, float | None]] = {}

    async def mid(self, exchange_id: str, base_asset: str, *, fresh: bool = False) -> float | None:
        if not exchange_id or not base_asset:
            return None
        key = (exchange_id, base_asset.upper())
        hit = self._cache.get(key)
        if not fresh and hit is not None and time.monotonic() - hit[0] <= self._ttl:
            return hit[1]
        try:
            value = await self._lookup(exchange_id, base_asset.upper())
        except Exception:  # never raise
            logger.exception("Reference price lookup failed for %s on %s", base_asset, exchange_id)
            value = None
        self._cache[key] = (time.monotonic(), value)
        return value

    async def _lookup(self, exchange_id: str, base: str) -> float | None:
        ex = self._clients.get(exchange_id)
        if ex is None:
            import ccxt.async_support as ccxt_async

            klass = getattr(ccxt_async, exchange_id, None)
            if klass is None:
                logger.warning("Unknown reference_exchange %r", exchange_id)
                return None
            ex = self._clients[exchange_id] = klass(
                {"enableRateLimit": True, "timeout": int(self._timeout * 1000), "options": {"defaultType": "swap"}}
            )
        return await fetch_public_price(ex, base, self._timeout)

    async def close(self) -> None:
        clients, self._clients = self._clients, {}
        for ex in clients.values():
            try:
                await ex.close()
            except Exception:
                logger.debug("Error closing reference exchange", exc_info=True)


class PriceOracle:
    def __init__(
        self,
        brokers: BrokerSource = (),
        exchange_id: str | None = "binanceusdm",
        *,
        cache_ttl_sec: float = 5.0,
        timeout_sec: float = 10.0,
        public_exchange: Any | None = None,
    ) -> None:
        """``brokers``: list or zero-arg callable returning the CONNECTED brokers.
        ``exchange_id``: public ccxt fallback (None disables it).
        ``public_exchange``: an already built ccxt-like object (tests)."""
        self._brokers = brokers
        self._exchange_id = exchange_id
        self._ttl = cache_ttl_sec
        self._timeout = timeout_sec
        self._public = public_exchange
        self._owns_public = public_exchange is None
        self._cache: dict[_CacheKey, tuple[float, float | None]] = {}

    def _broker_list(self) -> list[Broker]:
        src = self._brokers
        return list(src() if callable(src) else src)

    async def price(
        self,
        base_asset: str | None,
        quote_asset: str | None = None,
        market_type: MarketType | None = None,
        raw_symbol: str | None = None,
        *,
        fresh: bool = False,
    ) -> float | None:
        """Current price of ``base_asset`` or None. ``fresh`` bypasses the cache."""
        if not base_asset:
            return None
        key: _CacheKey = (
            base_asset.upper(),
            quote_asset.upper() if quote_asset else None,
            market_type.value if market_type else None,
            raw_symbol,
        )
        now = time.monotonic()
        hit = self._cache.get(key)
        if not fresh and hit is not None and now - hit[0] <= self._ttl:
            return hit[1]
        try:
            value = await self._lookup(base_asset.upper(), quote_asset, market_type, raw_symbol)
        except Exception:  # never raise
            logger.exception("Price lookup failed for %s", base_asset)
            value = None
        self._cache[key] = (time.monotonic(), value)
        return value

    async def _lookup(
        self, base: str, quote: str | None, market_type: MarketType | None, raw_symbol: str | None
    ) -> float | None:
        for broker in self._broker_list():
            try:
                if not broker.accepts_asset(base):
                    continue
                info = await asyncio.wait_for(
                    broker.resolve_symbol(base, quote, market_type, raw_symbol), self._timeout
                )
                if info is None:
                    continue
                q = await asyncio.wait_for(broker.get_quote(info), self._timeout)
                mid = quote_mid(q)
                if mid is not None:
                    # Back to the signal's price scale (1000PEPE contract price / 1000 = PEPE price).
                    mult = float((info.extra or {}).get("price_multiplier") or 1.0)
                    return mid / mult if mult > 0 else mid
            except Exception as exc:
                logger.debug("%s: no price for %s: %s", broker.name, base, exc)
        if market_type in (MarketType.FOREX, MarketType.CFD):
            return None  # a crypto exchange price would be a different instrument
        return await self._public_price(base)

    async def _public_price(self, base: str) -> float | None:
        ex = self._public
        if ex is None:
            if not self._exchange_id:
                return None
            import ccxt.async_support as ccxt_async

            klass = getattr(ccxt_async, self._exchange_id, None)
            if klass is None:
                logger.warning("Unknown price_source_exchange %r", self._exchange_id)
                self._exchange_id = None
                return None
            ex = self._public = klass(
                {"enableRateLimit": True, "timeout": int(self._timeout * 1000), "options": {"defaultType": "swap"}}
            )
        return await fetch_public_price(ex, base, self._timeout)

    async def close(self) -> None:
        if self._public is not None and self._owns_public:
            try:
                await self._public.close()
            except Exception:
                logger.debug("Error closing public price exchange", exc_info=True)
            self._public = None
