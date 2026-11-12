"""Market price lookup for validation (never used for order prices).

Order of sources: the first connected broker that can resolve the symbol
(mid of its quote), then a public ccxt exchange (USDT perpetuals). Results
are cached for a few seconds. ``price()`` never raises: unknown -> None.

A broker SDK may raise ``asyncio.CancelledError`` although nobody cancelled our
task (see :func:`stray_cancel`): here that is one more "price unknown", never a
reason to abort the caller.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import time
from collections.abc import Callable, Sequence
from typing import Any

from ..brokers.base import Broker
from ..models import MarketType, Quote

logger = logging.getLogger(__name__)

_CacheKey = tuple[str, str | None, str | None, str | None]
BrokerSource = Sequence[Broker] | Callable[[], Sequence[Broker]]
_MINUTE_MS = 60_000
CANDLE_PAGE = 1000          # 1-minute candles per request
CANDLE_MAX_PAGES = 4        # 4000 minutes: more than the longest wait of a virtual entry (48 h)


def stray_cancel() -> bool:
    """For ``except asyncio.CancelledError:`` blocks around broker / exchange calls. True when nobody asked
    the CURRENT task to stop (``asyncio.current_task().cancelling() == 0``): the CancelledError then comes
    out of the called library (e.g. an SDK awaiting a future that somebody else cancelled, as MetaApi does
    while its account is disconnected) and is a failure of that one call. False for a real cancellation
    (shutdown, the timeout of an enclosing ``asyncio.wait_for``): that one must be re-raised.

        try:
            await broker.something()
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            ...treat it like any other error of the call...
    """
    task = asyncio.current_task()
    return task is not None and task.cancelling() == 0


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


# Contract prefixes exchanges use for low-priced coins (1000PEPE = 1000 PEPE, 1MBABYDOGE = 1000000 BABYDOGE):
# the same names and the same rule as the exchange adapter (brokers/ccxt_broker.py: _base_variants).
_MULTIPLIER_PREFIXES = (1000, 10000, 100000, 1000000)
_PREFIX_RE = re.compile(r"^(1000000|100000|10000|1000)([A-Z0-9]+)$")
_PREFIX_1M_RE = re.compile(r"^1M([A-Z][A-Z0-9]*)$")     # only before a letter: "1MBABYDOGE", not "1M0..."


def contract_names(base: str) -> list[tuple[str, float]]:
    """(contract base name, exchange price / signal price) candidates for a coin, the name as written first:
    PEPE -> PEPE (x1), 1000PEPE (x1000), 10000PEPE, 100000PEPE, 1000000PEPE, 1MPEPE (x1000000);
    1000PEPE -> 1000PEPE (x1), PEPE (x0.001), ...; 1000000BABYDOGE -> ..., 1MBABYDOGE (x1)."""
    base = base.strip().upper()
    own, coin = 1.0, base
    match = _PREFIX_1M_RE.match(base)
    if match:
        own, coin = 1_000_000.0, match.group(1)
    else:
        match = _PREFIX_RE.match(base)
        if match:
            own, coin = float(match.group(1)), match.group(2)
    names: list[tuple[str, float]] = [(coin, 1.0)] if match else []
    names += [(f"{prefix}{coin}", float(prefix)) for prefix in _MULTIPLIER_PREFIXES]
    names.append((f"1M{coin}", 1_000_000.0))
    return [(base, 1.0)] + [(name, mult / own) for name, mult in names if name != base]


def _unreachable(exc: BaseException) -> bool:
    """Does this failure say that the exchange cannot be reached right now (timeout, network, rate limit;
    ccxt.NetworkError and its subclasses)? Then asking for another contract name would fail the same way.
    Not: an unknown symbol, which is the reason to try the next name."""
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError, ConnectionError)):
        return True
    return any(klass.__name__ == "NetworkError" for klass in type(exc).__mro__)


async def fetch_public_price(ex: Any, base: str, timeout_sec: float) -> float | None:
    """Mid of ``<BASE>/USDT:USDT`` on a ccxt-like exchange in the SIGNAL's price scale (contract prefixes
    1000 / 10000 / 100000 / 1000000 / 1M are tried and scaled back, see :func:`contract_names`)."""
    for name, mult in contract_names(base):     # PEPE trades as 1000PEPE on most venues
        symbol = f"{name}/USDT:USDT"
        try:
            ticker = await asyncio.wait_for(ex.fetch_ticker(symbol), timeout_sec + 5)
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            logger.warning("Public price for %s: the request was cancelled without a cancel request", symbol)
            continue
        except Exception as exc:
            logger.debug("Public price for %s unavailable: %s", symbol, exc)
            if _unreachable(exc):
                return None         # the exchange does not answer: the other names would wait just as long
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
        # (exchange id, base) -> (symbol, price multiplier) that answered a candle request ("1000PEPE/USDT:USDT", 1000)
        self._candle_symbols: dict[tuple[str, str], tuple[str, float]] = {}

    async def mid(self, exchange_id: str, base_asset: str, *, fresh: bool = False) -> float | None:
        if not exchange_id or not base_asset:
            return None
        key = (exchange_id, base_asset.upper())
        hit = self._cache.get(key)
        if not fresh and hit is not None and time.monotonic() - hit[0] <= self._ttl:
            return hit[1]
        try:
            value = await self._lookup(exchange_id, base_asset.upper())
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            logger.warning("Reference price lookup for %s on %s was cancelled without a cancel request",
                           base_asset, exchange_id)
            value = None
        except Exception:  # never raise
            logger.exception("Reference price lookup failed for %s on %s", base_asset, exchange_id)
            value = None
        self._cache[key] = (time.monotonic(), value)
        return value

    def _client(self, exchange_id: str) -> Any | None:
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
        return ex

    async def _lookup(self, exchange_id: str, base: str) -> float | None:
        ex = self._client(exchange_id)
        if ex is None:
            return None
        return await fetch_public_price(ex, base, self._timeout)

    async def candles(
        self, exchange_id: str, base_asset: str, since_ms: int, until_ms: int | None = None
    ) -> list[tuple[int, float, float]] | None:
        """1-minute candles of the coin's USDT perpetual on ``exchange_id`` from the one that opens at or after
        ``since_ms`` up to now (the running minute included), oldest first, as (open time ms, low, high) in the
        SIGNAL's price scale (1000-prefix aware, like :meth:`mid`). ``until_ms``: "now" on the caller's clock
        (only used to ask for no more candles than needed). An empty list: no such candle yet. None: the
        candles cannot be read (unknown exchange or symbol, network error...). Never raises."""
        if not exchange_id or not base_asset:
            return None
        base = base_asset.upper()
        try:
            return await self._candles(exchange_id, base, int(since_ms), until_ms)
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            logger.warning("Reference candles for %s on %s: the request was cancelled without a cancel request",
                           base, exchange_id)
            return None
        except Exception as exc:  # never raise
            logger.debug("Reference candles for %s on %s unavailable: %s", base, exchange_id, exc)
            return None

    async def _candles(
        self, exchange_id: str, base: str, since_ms: int, until_ms: int | None
    ) -> list[tuple[int, float, float]] | None:
        ex = self._client(exchange_id)
        if ex is None or not hasattr(ex, "fetch_ohlcv"):
            return None
        until = int(until_ms) if until_ms is not None else int(time.time() * 1000)
        known = self._candle_symbols.get((exchange_id, base))
        tries = [known] if known is not None else [(f"{name}/USDT:USDT", mult) for name, mult in contract_names(base)]
        error: Exception | None = None
        for symbol, mult in tries:        # PEPE trades as 1000PEPE on most venues
            out: list[tuple[int, float, float]] = []
            since = since_ms
            try:
                for _page in range(CANDLE_MAX_PAGES):
                    want = min(CANDLE_PAGE, max(2, (until - since) // _MINUTE_MS + 2))
                    rows = await asyncio.wait_for(ex.fetch_ohlcv(symbol, "1m", since, want), self._timeout + 5)
                    rows = [r for r in rows or [] if len(r) >= 4 and r[0] is not None and r[0] >= since]
                    for ts, _open, high, low, *_rest in rows:
                        if _valid(high) and _valid(low):
                            out.append((int(ts), float(low) / mult, float(high) / mult))
                    if len(rows) < want or not rows:
                        break
                    since = int(rows[-1][0]) + _MINUTE_MS
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = exc
                if _unreachable(exc):
                    break               # the exchange does not answer: the other names would wait just as long
                continue
            self._candle_symbols[(exchange_id, base)] = (symbol, mult)
            return sorted(set(out))
        if error is not None:
            raise error
        return None

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
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            logger.warning("Price lookup for %s was cancelled without a cancel request", base_asset)
            value = None
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
            except asyncio.CancelledError:
                if not stray_cancel():
                    raise
                # e.g. MetaApi while its account is disconnected: this broker has no price, the next one is asked
                logger.warning("%s: price request for %s was cancelled without a cancel request", broker.name, base)
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
