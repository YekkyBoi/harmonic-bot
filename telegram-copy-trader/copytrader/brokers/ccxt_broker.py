"""Crypto exchange adapter on top of ccxt (async), USDT-margined perpetual swaps by default.

Order model (per trade):
  * one entry order (market or limit, tagged with a client order id from ``client_tag``),
  * one reduce-only stop-market order for the protected quantity,
  * one reduce-only take-profit-market order per TP level (sizes from ``close_fraction``).

Fills are detected through OUR order ids (not only the net position), because in one-way
mode several trades on the same symbol share one exchange position. When the SL fills or
all TPs fill, the leftover reduce-only orders are cancelled (OCO emulation).

Exchange specifics verified against ccxt 4.5.x:
  * binance(usdm): stopLossPrice/takeProfitPrice -> STOP_MARKET/TAKE_PROFIT_MARKET on the
    algo endpoint; fetch/cancel need ``{"trigger": True}``; hedge via ``hedged`` -> positionSide.
  * bybit: stopLossPrice/takeProfitPrice -> conditional market order with triggerDirection;
    fetchOrder is limited on UTA, so open/closed order lookups are used.
  * okx: stopLossPrice/takeProfitPrice -> "conditional" algo order (tdMode from marginMode);
    fetch/cancel need ``{"trigger": True}``.
  * bitget: stopLossPrice/takeProfitPrice default to position-level pos_loss/pos_profit (they
    close the WHOLE position), so planType loss_plan/profit_plan is forced (sized orders);
    fetchOrder cannot read plan orders, so they are looked up in plan order lists.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, TypeVar

import ccxt.async_support as ccxt_async
from ccxt.base import errors as cx
from ccxt.base.decimal_to_precision import DECIMAL_PLACES, SIGNIFICANT_DIGITS, TICK_SIZE

from ..config import CcxtBrokerConfig, RiskConfig
from ..models import (
    BrokerPosition,
    EntryType,
    MarketType,
    OpenRequest,
    PositionStatus,
    Quote,
    Side,
    SizingResult,
    SymbolInfo,
    TakeProfit,
)
from ..risk.sizing import SizingError, choose_leverage, compute_linear_size, floor_to_step, round_to_tick
from .base import OUTCOME_UNKNOWN_MARK, Broker, BrokerError, OrderOutcomeUnknownError

logger = logging.getLogger(__name__)

# Public contract with the monitor (Monitor._retry_close, which does not import adapters and reads the same key):
# ``pos.data[FLAT_SEEN]`` = how many venue reads in a row found no position in the trade's direction (0 after a read
# that shows it; two in a row close the trade, see CcxtBroker._gone_on_venue). While it is above 0 a retried
# reduce-only close is not sent: the earlier close may have gone through. Renaming it breaks that guard; it is pinned
# by tests/test_review1_glue_engine.py::test_g4_*.
FLAT_SEEN = "flat_seen"
# ``pos.data[PNL_INCOMPLETE]`` (True, or absent): ``realized_pnl`` is known not to cover every exit the adapter
# booked for the position - the exchange's trade list held less than the booked exits, or an exit without a price
# was left out of the estimate. Only a note for texts (the engine's closing headline then gives no result in R
# for the account, monitor.PNL_INCOMPLETE); nothing in this adapter reads it.
PNL_INCOMPLETE = "pnl_incomplete"

T = TypeVar("T")

# Contract prefixes used by exchanges for low-priced coins (1000PEPE = 1000 PEPE, 1MBABYDOGE = 1000000 BABYDOGE).
_MULTIPLIER_PREFIXES = (1000, 10000, 100000, 1000000)
_PREFIX_RE = re.compile(r"^(1000000|100000|10000|1000)([A-Z0-9]+)$")
_PREFIX_1M_RE = re.compile(r"^1M([A-Z][A-Z0-9]*)$")     # only before a letter: "1MBABYDOGE", not "1M0..."
_STABLE_QUOTES = ("USDT", "USDC")
_FAMILIES = ("binance", "bybit", "okx", "bitget")
_CLIENT_ID_MAX = {"binance": 36, "bybit": 36, "okx": 32, "bitget": 40, "generic": 32}
# Exchanges whose attached SL/TP on an entry become ORDER-sized child orders (not position-level).
_ORDER_SIZED_ATTACH = ("bybit", "okx")
# "Nothing to change" answers to set_leverage / set_margin_mode.
_ALREADY_SET_RE = re.compile(
    r"not modified|no need to change|already|is the same|unchanged|110043|110026|-4046", re.IGNORECASE
)
_CANCELED = ("canceled", "cancelled", "expired", "rejected")
# set_leverage refused because the value is too high for the symbol or for the account (bybit 110013 / 10001...).
# binance: -4028 invalid, -4209 / -4208 above the symbol's maximum, -4202 / -4203 / -4205 / -4206 / -4403 account
# limits (verification level, account age, country). All of them are answered the same way: use less.
_LEVERAGE_INVALID_RE = re.compile(
    r"-4028|-4209|-4208|-4202|-4203|-4205|-4206|-4403|leverage .*not valid|max(imum)? leverage"
    r"|leverage.*(exceed|too (high|large)|invalid|out of range|can only|greater than)|more than \d+x leverage"
    r"|110013|10001", re.IGNORECASE)
# The same refusal for reasons of the ACCOUNT (not of this coin): the limit then holds for every symbol.
_LEVERAGE_ACCOUNT_RE = re.compile(r"-4202|-4203|-4205|-4206|-4403")
# The limit named in the refusal: "The max leverage of Symbol is 20x", "leverage can only up to 10x in your
# region", "More than 20x leverage is available ...", "a maximum leverage of 20", "cannot exceed 20 when ...".
_LEVERAGE_HINT_RE = re.compile(
    r"(?<![\d.\-])(\d{1,3})\s*x\b|(?:leverage of|leverage is|limit is|exceed|up to|over)\s+(\d{1,3})(?![\d.])",
    re.IGNORECASE)
_LEVERAGE_STEPS = (125, 100, 75, 50, 40, 25, 20, 15, 10, 8, 5, 4, 3, 2, 1)
_LEVERAGE_INFO_TTL_MS = 6 * 3600 * 1000     # leverage brackets / limits learned from refusals are re-read after this
_CLOCK_RESYNC_MS = 30 * 60 * 1000           # the offset to the exchange clock is measured again after this
_LOST_ORDER_MISSES = 3                      # "not found" this many passes in a row: the order never reached the venue
_LOST_ORDER_MIN_AGE_MS = 60_000             # ... and at least this long after it was sent (it may still execute late;
                                            # meanwhile the engine sends no new partial close for it)
_SHRUNK_NOTE_TTL_MS = 2 * 60 * 1000         # a size cut for margin by size_position is "the same order" this long
_HEALTH_TIMEOUT_SEC = 20.0
_TP_PLACE_ATTEMPTS = 3
# An entry / close request that ended without a clear answer (see _unclear) is looked up by its client id up to
# _LOOKUP_ATTEMPTS times, ``retry_delay`` (1 s) apart, and never longer than _LOOKUP_WINDOW_SEC in all: an order whose
# answer was lost can take a moment to show up (binance -1007 "execution status unknown"). An entry runs inside the
# engine's deadline for a leg (orchestrator.LEG_DEADLINE_SEC = 30 s); the lost request itself may already have used
# ccxt's whole request timeout (10 s by default) and the reads before it a few seconds more, and an entry that is
# found still needs its stop and targets (about 1-3 s): 8 s of looking keeps the whole leg inside the deadline.
_LOOKUP_ATTEMPTS = 6
_LOOKUP_WINDOW_SEC = 8.0
# pos.error is a " | "-joined list of notes; these markers find the notes that clear themselves.
_ERR_SEP = " | "
_STALE_MARK = "eski stop emri"
_UNCONFIRMED_MARK = "sonucu okunamadı"
_CRITICAL_MARK = "KRİTİK"
_SYNC_MARK = "durum güncellenemedi"
_UNSURE_MARK = "doğrulanamıyor"


# --------------------------------------------------------------------------- #
# Pure helpers (shared with the paper broker)
# --------------------------------------------------------------------------- #
def num(value: Any) -> float | None:
    """ccxt numbers may be str/float/None/''; return a finite float or None."""
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def precision_to_step(value: Any, precision_mode: int) -> float | None:
    """Convert a ccxt precision value to a step/tick size. None if not representable."""
    v = num(value)
    if v is None:
        return None
    if precision_mode == TICK_SIZE:
        return v if v > 0 else None
    if precision_mode == DECIMAL_PLACES:
        return float(Decimal(1).scaleb(-round(v)))
    if precision_mode == SIGNIFICANT_DIGITS:
        return None  # step depends on magnitude; not supported
    return None


def quote_from_ticker(ticker: dict[str, Any] | None) -> Quote | None:
    """Quote from a ccxt ticker with bid/ask/last fallbacks; None if it carries no price."""
    if not ticker:
        return None
    bid, ask = num(ticker.get("bid")), num(ticker.get("ask"))
    last = num(ticker.get("last")) or num(ticker.get("close"))
    bid = bid if bid and bid > 0 else None
    ask = ask if ask and ask > 0 else None
    if last is None or last <= 0:
        last = (bid + ask) / 2 if bid and ask else (bid or ask)
    if last is None or last <= 0:
        return None
    return Quote(bid=bid or last, ask=ask or last, last=last)


def _is_active(market: dict[str, Any]) -> bool:
    return market.get("active") is not False


def _market_matches(market: dict[str, Any], market_type: str) -> bool:
    if not _is_active(market):
        return False
    if market_type == "swap":
        return bool(market.get("swap")) and market.get("linear") is not False
    return bool(market.get("spot"))


def _lookup_symbol_map(symbol_map: dict[str, str], keys: Iterable[str | None]) -> str | None:
    lowered = {k.strip().lower(): v for k, v in symbol_map.items()}
    for key in keys:
        if key and key.strip().lower() in lowered:
            return lowered[key.strip().lower()]
    return None


def _split_prefix(base: str) -> tuple[float, str] | None:
    """(contract multiplier, coin) of a prefixed name: 1000PEPE -> (1000, PEPE), 1MBABYDOGE -> (1000000, BABYDOGE)."""
    m = _PREFIX_1M_RE.match(base)
    if m:
        return 1_000_000.0, m.group(1)
    m = _PREFIX_RE.match(base)
    if m:
        return float(m.group(1)), m.group(2)
    return None


def _base_variants(base: str) -> list[tuple[str, float]]:
    """(market base, price multiplier) candidates: PEPE -> 1000PEPE (x1000), 1000PEPE -> PEPE (x0.001),
    BABYDOGE -> 1MBABYDOGE (x1000000), 1000000BABYDOGE -> 1MBABYDOGE (x1). The name as written comes first."""
    split = _split_prefix(base)
    own, coin = split if split else (1.0, base)
    names = [(coin, 1.0)] if split else []
    names += [(f"{p}{coin}", float(p)) for p in _MULTIPLIER_PREFIXES]
    names.append((f"1M{coin}", 1_000_000.0))
    return [(base, 1.0)] + [(name, mult / own) for name, mult in names if name != base]


def find_market(
    markets: dict[str, dict[str, Any]],
    *,
    base_asset: str,
    quote_asset: str | None,
    default_quote: str,
    market_type: str,
    symbol_map: dict[str, str] | None = None,
    raw_symbol: str | None = None,
) -> tuple[dict[str, Any], float] | None:
    """Find the market for a signal asset. Returns (market, price_multiplier) or None.

    price_multiplier = exchange price / signal price (1000 for 1000PEPE when the signal says PEPE).
    """
    base = base_asset.strip().upper()
    mapped = _lookup_symbol_map(symbol_map or {}, [raw_symbol, base])
    if mapped:
        market = markets.get(mapped)
        if market is None:  # mapped to an exchange id like "ETHUSDT"
            candidates = [m for m in markets.values() if m.get("id") == mapped]
            preferred = [m for m in candidates if _market_matches(m, market_type)]
            market = (preferred or candidates or [None])[0]
        if market is None or not _is_active(market):
            logger.warning("symbol_map target %s not found or inactive", mapped)
            return None
        return market, 1.0

    quotes: list[str] = []
    q = (quote_asset or "").strip().upper()
    if q in _STABLE_QUOTES:
        quotes.append(q)
    if default_quote.upper() not in quotes:
        quotes.append(default_quote.upper())

    for variant, multiplier in _base_variants(base):
        for quote in quotes:
            symbol = f"{variant}/{quote}:{quote}" if market_type == "swap" else f"{variant}/{quote}"
            market = markets.get(symbol)
            if market is not None and _market_matches(market, market_type):
                return market, multiplier
    return None


def symbol_info_from_market(
    market: dict[str, Any],
    precision_mode: int,
    *,
    base_asset: str,
    price_multiplier: float = 1.0,
) -> SymbolInfo | None:
    """SymbolInfo from a ccxt market; None when the precision cannot be expressed as a step."""
    precision = market.get("precision") or {}
    limits = market.get("limits") or {}
    qty_step = precision_to_step(precision.get("amount"), precision_mode)
    price_tick = precision_to_step(precision.get("price"), precision_mode)
    if qty_step is None or price_tick is None:
        logger.warning("market %s: unsupported precision %s (mode %s)", market.get("symbol"), precision, precision_mode)
        return None
    amount_limits = limits.get("amount") or {}
    max_candidates = [x for x in (num(amount_limits.get("max")), num((limits.get("market") or {}).get("max"))) if x]
    lev_max = num((limits.get("leverage") or {}).get("max"))
    return SymbolInfo(
        broker_symbol=market["symbol"],
        base_asset=base_asset.upper(),
        quote_asset=str(market.get("quote") or ""),
        qty_step=qty_step,
        min_qty=num(amount_limits.get("min")) or 0.0,
        max_qty=min(max_candidates) if max_candidates else None,
        min_notional=num((limits.get("cost") or {}).get("min")),
        price_tick=price_tick,
        contract_size=num(market.get("contractSize")) or 1.0,
        max_leverage=lev_max if lev_max and lev_max >= 1 else None,
        extra={
            "price_multiplier": price_multiplier,
            "market_id": market.get("id"),
            "market_base": market.get("base"),
            "market_type": market.get("type"),
            "settle": market.get("settle"),
            "taker_fee": num(market.get("taker")),
        },
    )


def split_quantities(total: float, fractions: list[float], step: float, min_qty: float) -> list[float]:
    """Split ``total`` over TP levels by fractions (normalized). The last level gets the
    remainder so the sum equals ``total``. Levels below the minimum are skipped (0) and their
    size moves to the next level (the last one's to the previous level)."""
    n = len(fractions)
    if n == 0:
        return []
    weights = [max(0.0, f) for f in fractions]
    s = sum(weights)
    if s <= 0:
        weights, s = [1.0] * n, float(n)
    d_total = Decimal(str(total))
    raw = [floor_to_step(total * w / s, step) for w in weights[:-1]]
    rest = float(d_total - sum((Decimal(str(x)) for x in raw), Decimal(0)))
    qtys = raw + [floor_to_step(max(rest, 0.0), step)]

    minimum = max(min_qty, step)
    out = [0.0] * n
    carry = Decimal(0)
    for i, q in enumerate(qtys):
        dq = Decimal(str(q)) + carry
        if i < n - 1 and float(dq) < minimum * (1 - 1e-9):
            carry = dq
            continue
        out[i] = float(dq)
        carry = Decimal(0)
    if 0 < out[-1] < minimum * (1 - 1e-9):
        for j in range(n - 2, -1, -1):
            if out[j] > 0:
                out[j] = float(Decimal(str(out[j])) + Decimal(str(out[-1])))
                out[-1] = 0.0
                break
    return out


def _dec_sub(a: float, b: float) -> float:
    return float(Decimal(str(a)) - Decimal(str(b)))


def _short(err: BaseException, limit: int = 180) -> str:
    text = str(err).strip().replace("\n", " ")
    return (text[:limit] + "…") if len(text) > limit else (text or type(err).__name__)


def _fmt(x: float | None) -> str:
    if x is None:
        return "-"
    return format(Decimal(f"{x:.10g}").normalize(), "f")


def _unclear(err: BaseException | None) -> bool:
    """True when an order request that failed with ``err`` may still have reached the venue and been executed: no
    clear answer came back. That is every ccxt NetworkError (timeout, broken connection, HTTP 5xx; binance -1007 "send
    status unknown; execution status unknown"), a plain OperationFailed (binance -1000 / -1001 / -1006 / -1008: unknown
    error, internal error, "execution status unknown", server busy) and an answer that an order with our client id
    exists already (DuplicateOrderId). Not the two NetworkErrors that are the venue's clear refusal, given before any
    order is processed (ccxt classes them as network trouble because they can be retried): a request stamped outside
    the venue's time window (InvalidNonce: binance -1021) and a rate limit or ban answered with HTTP 418 / 429
    (DDoSProtection: ccxt 4.5.92's binance raises it for those two statuses, whatever the code in the body, e.g.
    -1003 / -1015). A RateLimitExceeded (ccxt's class for binance -1003 / -1015 when they come with another HTTP status;
    NOT a DDoSProtection subclass) stays unclear on purpose: it is looked up by client id like a lost answer, the
    cautious side (never sent twice). The engine's no_answer() agrees (orchestrator._REFUSAL_TYPES)."""
    if err is None or isinstance(err, (cx.InvalidNonce, cx.DDoSProtection)):
        return False
    return isinstance(err, (cx.OperationFailed, cx.DuplicateOrderId))


def _why(err: BaseException | None, limit: int = 180) -> str:
    """Short reason for an owner text: an unclear answer (see _unclear) says that the answer was not received."""
    text = _short(err or Exception(), limit)
    return f"yanıt alınamadı ({text})" if _unclear(err) else text


def _venue_code(err: BaseException) -> str | None:
    """The exchange's own error code in a ccxt error text ('binanceusdm {"code":-2015,"msg":...}' -> "-2015")."""
    m = re.search(r'"code"\s*:\s*"?(-?\d+)', str(err))
    return m.group(1) if m else None


def _join_error(old: str | None, msg: str) -> str:
    """Add ``msg`` to an existing error text (once)."""
    if not old:
        return msg
    return old if msg in old.split(_ERR_SEP) else old + _ERR_SEP + msg


def _drop_error(old: str | None, marker: str) -> str | None:
    """Remove the notes containing ``marker``; None when nothing is left."""
    if not old:
        return old
    return _ERR_SEP.join(p for p in old.split(_ERR_SEP) if marker not in p) or None


# --------------------------------------------------------------------------- #
# Order state
# --------------------------------------------------------------------------- #
@dataclass
class OrderState:
    status: str                      # "open" | "filled" | "canceled"
    filled: float | None = None
    average: float | None = None
    amount: float | None = None
    linked_ids: list[str] = field(default_factory=list)  # actual orders created by a trigger order
    raw: dict[str, Any] = field(default_factory=dict)


def parse_order_state(order: dict[str, Any], *, trigger: bool) -> OrderState:
    status = str(order.get("status") or "open").lower()
    filled = num(order.get("filled"))
    amount = num(order.get("amount"))
    average = num(order.get("average"))
    info = order.get("info") if isinstance(order.get("info"), dict) else {}
    oid = str(order.get("id"))
    linked = [str(info[k]) for k in ("actualOrderId", "ordId", "executeOrderId")
              if info.get(k) not in (None, "", 0, "0") and str(info[k]) != oid]
    if average is None:
        for key in ("actualPrice", "actualPx", "priceAvg", "avgPrice", "avgPx"):
            average = num(info.get(key))
            if average:
                break
        if not average:
            average = None
    if status == "closed":
        state = "filled"
        if trigger and not filled:
            filled = num(info.get("actualSz")) or amount  # triggered market order closes its size
    elif status in _CANCELED:
        state = "canceled"
    else:
        state = "open"
    return OrderState(state, filled, average, amount, linked, order)


# --------------------------------------------------------------------------- #
# Adapter
# --------------------------------------------------------------------------- #
class CcxtBroker(Broker):
    kind = "ccxt"

    def __init__(
        self,
        cfg: CcxtBrokerConfig,
        *,
        exchange: Any | None = None,
        sleep: Callable[[float], Awaitable[Any]] | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        super().__init__(cfg)
        self.cfg: CcxtBrokerConfig = cfg
        self.exchange: Any = exchange          # injectable for tests
        self.risk_config: RiskConfig = RiskConfig()
        self._sleep = sleep or asyncio.sleep
        self._now_ms = clock_ms or (lambda: int(time.time() * 1000))
        self._locks: dict[str, asyncio.Lock] = {}
        self._known_order_ids: set[str] = set()
        self._margin_mode_set: set[str] = set()
        self._leverage_tiers: dict[str, Any] | None = None   # ccxt fetch_leverage_tiers() result
        self._leverage_tiers_ms = 0                          # when it was read
        self._set_leverage: dict[str, int] = {}              # leverage really accepted by the venue, per symbol
        # limits learned from refused set_leverage calls: symbol (or "*" = whole account) -> (leverage, when)
        self._leverage_caps: dict[str, tuple[int, int]] = {}
        # sizes that size_position cut down for margin: symbol -> (quantity, when); read by open_position
        self._margin_shrunk: dict[str, tuple[float, int]] = {}
        self._clock_synced_ms = 0
        self.fill_poll_attempts = 6
        self.fill_poll_delay = 0.5
        self.retry_delay = 1.0

    # ------------------------------------------------------------------ lifecycle
    def _build_exchange(self) -> Any:
        klass = getattr(ccxt_async, self.cfg.exchange, None)
        if klass is None:
            raise BrokerError(f"{self.name}: bilinmeyen ccxt borsası '{self.cfg.exchange}'.")
        options: dict[str, Any] = {"defaultType": self.cfg.market_type}
        if self.cfg.exchange.lower().startswith("binance"):
            # Binance refuses signed requests whose timestamp is off (-1021): do not trust the machine's clock,
            # measure the offset to the exchange (see _sync_clock). An explicit value in the config wins.
            options["adjustForTimeDifference"] = True
        options.update(self.cfg.options)
        params: dict[str, Any] = {"enableRateLimit": True, "options": options}
        for key, value in (("apiKey", self.cfg.api_key), ("secret", self.cfg.secret), ("password", self.cfg.password)):
            if value:
                params[key] = value
        return klass(params)

    async def connect(self) -> None:
        if self.exchange is None:
            self.exchange = self._build_exchange()
        if self.cfg.sandbox:
            try:
                self.exchange.set_sandbox_mode(True)
            except cx.BaseError as e:
                raise BrokerError(f"{self.name}: test ağı (sandbox) desteklenmiyor: {_short(e)}") from e
        if getattr(self.cfg, "demo", False):
            enable = getattr(self.exchange, "enable_demo_trading", None)
            if enable is None:
                raise BrokerError(f"{self.name}: bu borsada demo işlem (demo: true) desteklenmiyor")
            try:
                enable(True)
            except cx.BaseError as e:
                raise BrokerError(f"{self.name}: demo işlem açılamadı: {_short(e)}") from e
            logger.info("%s: demo trading enabled (orders go to the exchange's demo environment)", self.name)
        # ccxt measures the clock offset only inside load_markets, AFTER its first signed request (the currency
        # list): with a clock that is off that request already fails. So measure it first.
        await self._sync_clock(force=True)
        await self._ccxt("piyasa bilgileri yüklenemedi", self.exchange.load_markets())
        if self.cfg.api_key and self.cfg.secret:
            # Catch wrong/expired keys at startup instead of on the first signal.
            await self._ccxt("API anahtarı doğrulanamadı (yetki/IP kısıtı?)", self.exchange.fetch_balance())
        logger.info("%s: connected to %s (%d markets)", self.name, self.exchange.id, len(self.exchange.markets or {}))

    async def close(self) -> None:
        if self.exchange is not None and hasattr(self.exchange, "close"):
            try:
                await self.exchange.close()
            except Exception:
                logger.exception("%s: error while closing exchange", self.name)

    # ------------------------------------------------------------------ helpers
    @property
    def _family(self) -> str:
        ex_id = str(getattr(self.exchange, "id", self.cfg.exchange)).lower()
        return next((f for f in _FAMILIES if ex_id.startswith(f)), "generic")

    @property
    def _hedged(self) -> bool:
        return self.cfg.position_mode == "hedge"

    def _risk(self) -> RiskConfig:
        return getattr(self, "risk_config", None) or RiskConfig()

    def _lock(self, symbol: str) -> asyncio.Lock:
        return self._locks.setdefault(symbol, asyncio.Lock())

    async def _sync_clock(self, *, force: bool = False) -> bool:
        """binance: (re)measure the offset between this machine's clock and the exchange; signed requests are
        stamped with it. Done at connect, every ``_CLOCK_RESYNC_MS`` and after a "timestamp outside recvWindow"
        answer (a clock that drifts, or that Windows corrects, makes an old offset wrong). True = measured now."""
        ex = self.exchange
        if self._family != "binance" or not (getattr(ex, "options", None) or {}).get("adjustForTimeDifference"):
            return False
        now = self._now_ms()
        if not force and now - self._clock_synced_ms < _CLOCK_RESYNC_MS:
            return False
        try:
            await ex.load_time_difference()
        except Exception as e:      # best effort: the old offset stays
            logger.warning("%s: could not measure the clock offset to the exchange: %s", self.name, e)
            return False
        self._clock_synced_ms = now
        logger.info("%s: clock offset to the exchange: %s ms", self.name, ex.options.get("timeDifference"))
        return True

    async def _ccxt(self, what: str, awaitable: Awaitable[T]) -> T:
        try:
            return await awaitable
        except BrokerError:
            raise
        except cx.InsufficientFunds as e:
            raise BrokerError(f"{self.name}: {what}: yetersiz bakiye ({_short(e)})") from e
        except cx.BaseError as e:
            raise BrokerError(f"{self.name}: {what}: {_short(e)}") from e

    def _register(self, pos: BrokerPosition) -> None:
        d = pos.data
        for key in ("entry_order_id", "sl_order_id"):
            if d.get(key):
                self._known_order_ids.add(str(d[key]))
        for tp in d.get("tp_order_ids") or []:
            if tp.get("id"):
                self._known_order_ids.add(str(tp["id"]))
        for oid in d.get("other_order_ids") or []:
            self._known_order_ids.add(str(oid))

    def _remember(self, d: dict[str, Any], order_id: str | None) -> None:
        if order_id:
            self._known_order_ids.add(str(order_id))
            others = d.setdefault("other_order_ids", [])
            if order_id not in others:
                others.append(str(order_id))

    def _market_rules(self, pos: BrokerPosition) -> tuple[float, float, float, float]:
        """(qty_step, min_qty, price_tick, contract_size) from loaded markets, else from data."""
        d = pos.data
        market = (getattr(self.exchange, "markets", None) or {}).get(pos.broker_symbol)
        if market:
            info = symbol_info_from_market(market, self.exchange.precisionMode, base_asset=d.get("base", "?"))
            if info:
                return info.qty_step, info.min_qty, info.price_tick, info.contract_size
        return (float(d.get("qty_step", 0) or 0), float(d.get("min_qty", 0) or 0),
                float(d.get("price_tick", 0) or 0), float(d.get("contract_size", 1) or 1))

    def _client_id(self, tag: str, suffix: str) -> str:
        limit = _CLIENT_ID_MAX[self._family]
        clean_tag = re.sub(r"[^A-Za-z0-9]", "", tag)
        clean_suffix = re.sub(r"[^A-Za-z0-9]", "", suffix)
        return clean_tag[: max(1, limit - len(clean_suffix))] + clean_suffix

    @staticmethod
    def _order_side(side: Side) -> str:
        return "buy" if side is Side.LONG else "sell"

    @staticmethod
    def _close_side(side: Side) -> str:
        return "sell" if side is Side.LONG else "buy"

    def _position_params(self) -> dict[str, Any]:
        return {"hedged": True} if self._hedged else {}

    def _margin_params(self) -> dict[str, Any]:
        # okx: tdMode is per order; bitget: marginMode is required per order (ccxt default "cross").
        return {"marginMode": self.cfg.margin_mode} if self._family in ("okx", "bitget") else {}

    def _sl_params(self, price: float) -> dict[str, Any]:
        p: dict[str, Any] = {"stopLossPrice": price, "reduceOnly": True, **self._position_params(), **self._margin_params()}
        if self._family == "bitget":
            p.update({"planType": "loss_plan", "triggerType": "fill_price"})
        return p

    def _tp_params(self, price: float) -> dict[str, Any]:
        p: dict[str, Any] = {"takeProfitPrice": price, "reduceOnly": True, **self._position_params(), **self._margin_params()}
        if self._family == "bitget":
            p.update({"planType": "profit_plan", "triggerType": "fill_price"})
        return p

    def _close_params(self, client_id: str | None = None) -> dict[str, Any]:
        p: dict[str, Any] = {"reduceOnly": True, **self._position_params(), **self._margin_params()}
        if client_id:
            p["clientOrderId"] = client_id
        return p

    def _attach_params(self, stop_loss: float, take_profits: list[TakeProfit]) -> dict[str, Any] | None:
        """Attached SL (+ single TP) for a limit entry, only where children are order-sized."""
        if not (getattr(self.exchange, "has", {}) or {}).get("createOrderWithTakeProfitAndStopLoss"):
            return None
        if self._family not in _ORDER_SIZED_ATTACH or len(take_profits) > 1:
            return None
        p: dict[str, Any] = {"stopLoss": {"triggerPrice": stop_loss}}
        if take_profits:
            p["takeProfit"] = {"triggerPrice": take_profits[0].price}
        if self._family == "bybit":
            p["tpslMode"] = "Partial"   # "Full" would be a position-level SL for every trade on the symbol
        return p

    @staticmethod
    def _done(pos: BrokerPosition) -> bool:
        """No further protection work: closed/failed, or only leftover cancels remain."""
        return pos.status is not PositionStatus.OPEN or bool(pos.data.get("closing"))

    def _require_real(self, pos: BrokerPosition) -> None:
        if pos.dry_run or pos.data.get("simulated"):
            raise BrokerError(f"{self.name}: simüle (dry-run) pozisyon gerçek hesapta yönetilemez.")

    def _require_swap(self) -> None:
        if self.cfg.market_type != "swap":
            raise BrokerError(
                f"{self.name}: spot işlemler desteklenmiyor (spotta stop ve kâr al emirleri aynı bakiyeyi "
                f"kilitler). market_type: swap kullanın."
            )

    # ------------------------------------------------------------------ account
    async def _balance_figures(self) -> tuple[float | None, float | None]:
        await self._sync_clock()        # only when the last measurement is old
        try:
            bal = await self._ccxt("bakiye alınamadı", self.exchange.fetch_balance())
        except BrokerError as e:
            # binance -1021 (timestamp outside the window): our clock offset is stale; measure it and ask once more.
            if not isinstance(e.__cause__, cx.InvalidNonce) or not await self._sync_clock(force=True):
                raise
            bal = await self._ccxt("bakiye alınamadı", self.exchange.fetch_balance())
        ccy = self.cfg.quote_currency.upper()
        acct = bal.get(ccy) or {}
        equity, free = num(acct.get("total")), num(acct.get("free"))
        if self._family == "bybit":
            b_eq, b_free = _bybit_coin_figures(bal.get("info"), ccy)
            equity = b_eq if b_eq is not None else equity
            free = free if free is not None else b_free
        return equity, free

    async def get_equity(self) -> float:
        equity, free = await self._balance_figures()
        value = (equity if equity is not None else free) if self._risk().use_equity else (free if free is not None else equity)
        if value is None:
            raise BrokerError(f"{self.name}: {self.cfg.quote_currency} bakiyesi bulunamadı.")
        return value

    async def get_free_margin(self) -> float:
        equity, free = await self._balance_figures()
        if free is None:
            if equity is None:
                raise BrokerError(f"{self.name}: {self.cfg.quote_currency} bakiyesi bulunamadı.")
            logger.warning("%s: free balance unknown, using equity %.2f", self.name, equity)
            return equity
        return free

    async def account_currency(self) -> str:
        return self.cfg.quote_currency.upper()

    # ------------------------------------------------------------------ market data
    async def resolve_symbol(
        self,
        base_asset: str,
        quote_asset: str | None = None,
        market_type: MarketType | None = None,
        raw_symbol: str | None = None,
    ) -> SymbolInfo | None:
        if not getattr(self.exchange, "markets", None):
            await self._ccxt("piyasa bilgileri yüklenemedi", self.exchange.load_markets())
        has_mapping = _lookup_symbol_map(self.cfg.symbol_map, [raw_symbol, base_asset]) is not None
        if market_type in (MarketType.FOREX, MarketType.CFD) and not has_mapping:
            return None  # forex/CFD signals only via an explicit symbol_map entry
        found = find_market(
            self.exchange.markets,
            base_asset=base_asset,
            quote_asset=quote_asset,
            default_quote=self.cfg.quote_currency,
            market_type=self.cfg.market_type,
            symbol_map=self.cfg.symbol_map,
            raw_symbol=raw_symbol,
        )
        if found is None:
            return None
        market, multiplier = found
        return symbol_info_from_market(market, self.exchange.precisionMode, base_asset=base_asset, price_multiplier=multiplier)

    async def get_quote(self, symbol: SymbolInfo) -> Quote:
        ticker = await self._ccxt("fiyat alınamadı", self.exchange.fetch_ticker(symbol.broker_symbol))
        quote = quote_from_ticker(ticker)
        if ticker and (num(ticker.get("bid")) is None or num(ticker.get("ask")) is None):
            # e.g. binance futures 24h tickers carry no bid/ask
            try:
                book = await self.exchange.fetch_order_book(symbol.broker_symbol, 5)
                bid = num(book["bids"][0][0]) if book.get("bids") else None
                ask = num(book["asks"][0][0]) if book.get("asks") else None
                last = quote.last if quote else (bid + ask) / 2 if bid and ask else None
                if bid and ask and last:
                    quote = Quote(bid=bid, ask=ask, last=last)
            except (cx.BaseError, KeyError, IndexError, TypeError) as e:
                logger.debug("%s: order book fallback failed: %s", self.name, e)
        if quote is None:
            raise BrokerError(f"{self.name}: {symbol.broker_symbol} için fiyat alınamadı.")
        return quote

    async def fetch_ohlcv_1m(self, broker_symbol: str, since_ms: int, limit: int = 500) -> list[list[float]]:
        """Read-only helper for simulations (dry-run): 1m candles [ts, o, h, l, c, v]."""
        return await self._ccxt("mum verisi alınamadı", self.exchange.fetch_ohlcv(broker_symbol, "1m", since_ms, limit))

    # ------------------------------------------------------------------ sizing
    def _taker_fee(self, symbol: SymbolInfo) -> float:
        fee = num(symbol.extra.get("taker_fee"))
        if fee is None:
            market = (getattr(self.exchange, "markets", None) or {}).get(symbol.broker_symbol) or {}
            fee = num(market.get("taker"))
        return fee if fee is not None and fee >= 0 else self._risk().fee_rate

    async def size_position(
        self,
        symbol: SymbolInfo,
        side: Side,
        entry_price: float,
        stop_loss: float,
        risk_amount: float,
    ) -> SizingResult:
        self._require_swap()
        if (side is Side.LONG and stop_loss >= entry_price) or (side is Side.SHORT and stop_loss <= entry_price):
            raise SizingError(f"Stop ({_fmt(stop_loss)}) {side.value} işlem için girişin ({_fmt(entry_price)}) yanlış tarafında.")
        risk = self._risk()
        result = compute_linear_size(
            risk_amount=risk_amount,
            entry=entry_price,
            stop=stop_loss,
            contract_size=symbol.contract_size,
            qty_step=symbol.qty_step,
            min_qty=symbol.min_qty,
            max_qty=symbol.max_qty,
            min_notional=symbol.min_notional,
            fee_rate=self._taker_fee(symbol),
            include_fees=risk.include_fees_in_risk,
            allow_min_qty_round_up=risk.allow_min_qty_round_up,
            rounding=str(getattr(self.config, "lot_rounding", "down") or "down"),
        )
        free = await self.get_free_margin()
        frac = abs(entry_price - stop_loss) / entry_price
        ladder = await self._leverage_ladder(symbol, frac)

        def pick(notional: float) -> tuple[int, float]:
            return choose_leverage(
                notional=notional, free_margin=free, stop_distance_frac=frac, preferred=self.cfg.leverage,
                max_leverage=self.cfg.max_leverage, venue_max_leverage=self._ladder_leverage(ladder, notional),
                max_margin_usage_pct=risk.max_margin_usage_pct, liquidation_cap=self.cfg.margin_mode == "isolated",
            )

        self._margin_shrunk.pop(symbol.broker_symbol, None)
        try:
            leverage, margin = pick(result.notional)
        except SizingError as err:
            # The margin does not fit at the highest leverage a position of this size may use: open the largest
            # size that fits (less than the target risk, never more) instead of skipping the trade.
            result = self._shrink_to_margin(symbol, result, entry_price, free, ladder, err)
            leverage, margin = pick(result.notional)
            # open_position marks the position it opens with this size ("size_capped"): the engine then tells
            # the owner about the smaller risk even when execution reports are switched off.
            self._margin_shrunk[symbol.broker_symbol] = (result.quantity, self._now_ms())
        return result.model_copy(update={"leverage": float(leverage), "margin_required": margin})

    def _loss_per_unit(self, symbol: SymbolInfo, entry: float, stop: float) -> float:
        """Money lost at the stop per 1 quantity (the formula of compute_linear_size)."""
        loss = abs(entry - stop) * symbol.contract_size
        if self._risk().include_fees_in_risk:
            loss += self._taker_fee(symbol) * (entry + stop) * symbol.contract_size
        return loss

    def _shrink_to_margin(self, symbol: SymbolInfo, result: SizingResult, entry_price: float, free: float,
                          ladder: list[tuple[int, float | None]], err: SizingError) -> SizingResult:
        """``result`` cut down to the largest quantity whose margin fits ``max_margin_usage_pct`` of the free
        balance, with a warning naming the planned size, the new size and the risk it carries. Raises
        SizingError (nothing is opened) when not even the smallest order fits."""
        allowed = free * self._risk().max_margin_usage_pct / 100.0
        unit = symbol.contract_size * entry_price
        if not (math.isfinite(allowed) and allowed > 0 and unit > 0 and result.quantity > 0):
            raise err
        value, lev = self._largest_fit(ladder, allowed)
        qty = min(result.quantity, floor_to_step(value / unit * 0.999, symbol.qty_step))
        if qty <= 0 or qty < symbol.min_qty or (symbol.min_notional and qty * unit < symbol.min_notional * (1 - 1e-9)):
            smallest = max(symbol.min_qty, symbol.qty_step)
            if symbol.min_notional:
                smallest = max(smallest, symbol.min_notional / unit)
            raise SizingError(
                f"Yetersiz teminat: en küçük emir (~{smallest * unit:.2f} {symbol.quote_asset}) için bile "
                f"{self._ladder_leverage(ladder, smallest * unit)}x kaldıraçla ~"
                f"{smallest * unit / self._ladder_leverage(ladder, smallest * unit):.2f} teminat gerekiyor; "
                f"kullanılabilir {allowed:.2f} (serbest bakiye {free:.2f}). İşlem açılmadı."
            ) from err
        ratio = qty / result.quantity
        loss = result.est_loss_at_sl * ratio
        note = (f"Teminat yetmediği için pozisyon küçültüldü: planlanan {_fmt(result.quantity)} yerine {_fmt(qty)} "
                f"açılıyor; stopta kayıp ~{loss:.2f} (hedef {result.risk_amount:.2f}). Serbest bakiye {free:.2f}, "
                f"bu büyüklükte kullanılabilen en yüksek kaldıraç {lev}x.")
        return result.model_copy(update={
            "quantity": qty, "notional": qty * unit, "est_loss_at_sl": loss,
            "est_fees_at_sl": result.est_fees_at_sl * ratio, "warnings": [*result.warnings, note],
        })

    @staticmethod
    def _ladder_leverage(ladder: list[tuple[int, float | None]], notional: float) -> int:
        """Highest leverage a position worth ``notional`` may use."""
        for lev, top in ladder:
            if top is None or notional <= top:
                return lev
        return ladder[-1][0]

    @staticmethod
    def _largest_fit(ladder: list[tuple[int, float | None]], allowed_margin: float) -> tuple[float, int]:
        """(largest position value whose margin fits ``allowed_margin``, the leverage it needs)."""
        best, best_lev = 0.0, ladder[-1][0]
        for lev, top in ladder:
            value = allowed_margin * lev if top is None else min(allowed_margin * lev, top)
            if value > best:
                best, best_lev = value, lev
        return best, best_lev

    async def _leverage_ladder(self, symbol: SymbolInfo, stop_frac: float) -> list[tuple[int, float | None]]:
        """[(usable leverage, largest position value the venue allows at it; None = no limit)], highest leverage
        first, never empty. Limits: config, liquidation beyond the stop (isolated only), the venue's maximum
        (binance: the account's leverage brackets, which allow less for larger positions) and what refused
        set_leverage calls taught us."""
        cap = max(1, int(self.cfg.max_leverage))
        if self.cfg.margin_mode == "isolated":   # cross: liquidation depends on the whole account
            cap = min(cap, max(1, math.floor(1 / (stop_frac * 1.5 + 0.005))))
        if symbol.max_leverage and symbol.max_leverage >= 1:
            cap = min(cap, int(symbol.max_leverage))
        learned = self._learned_cap(symbol.broker_symbol)
        if learned:
            cap = min(cap, learned)
        tiers = await self._tiers(symbol.broker_symbol)
        return [(max(1, min(cap, int(lev))), top) for lev, top in tiers] or [(cap, None)]

    def _learned_cap(self, symbol: str) -> int | None:
        """Leverage limit learned from refused set_leverage calls (for the symbol or the whole account)."""
        now = self._now_ms()
        caps = [lev for lev, at in (self._leverage_caps.get(key, (0, 0)) for key in (symbol, "*"))
                if lev >= 1 and now - at < _LEVERAGE_INFO_TTL_MS]
        return min(caps) if caps else None

    async def _tiers(self, symbol: str) -> list[tuple[float, float | None]]:
        """The symbol's leverage brackets as (maximum leverage, largest position value) pairs, highest leverage
        first; [] = unknown. ccxt's binance markets carry no leverage limit, so it is read from the account's
        leverage brackets (one request for all symbols, repeated every few hours)."""
        ex = self.exchange
        if self._family != "binance" or not (getattr(ex, "has", {}) or {}).get("fetchLeverageTiers"):
            return []
        now = self._now_ms()
        if self._leverage_tiers is None or now - self._leverage_tiers_ms >= _LEVERAGE_INFO_TTL_MS:
            try:
                self._leverage_tiers = await ex.fetch_leverage_tiers() or {}
                self._leverage_tiers_ms = now
            except Exception as e:   # best effort: without it the maximum is found by trial (see _prepare_symbol)
                logger.warning("%s: leverage brackets unavailable (%s); the symbol maximum is found by trial",
                               self.name, e)
                if self._leverage_tiers is None:
                    return []
        out: list[tuple[float, float | None]] = []
        for tier in self._leverage_tiers.get(symbol) or []:
            lev = num(tier.get("maxLeverage"))
            if lev and lev >= 1:
                out.append((lev, num(tier.get("maxNotional")) or None))
        return sorted(out, key=lambda t: -t[0])

    async def _resolve_leverage(self, req: OpenRequest, qty: float, ref_price: float) -> int:
        frac = abs(ref_price - req.stop_loss) / ref_price
        notional = qty * req.symbol.contract_size * ref_price
        cap = self._ladder_leverage(await self._leverage_ladder(req.symbol, frac), notional)
        if req.leverage:
            lev = math.ceil(req.leverage - 1e-9)
        else:
            lev, _ = choose_leverage(
                notional=notional,
                free_margin=await self.get_free_margin(),
                stop_distance_frac=frac,
                preferred=self.cfg.leverage,
                max_leverage=self.cfg.max_leverage,
                venue_max_leverage=cap,
                max_margin_usage_pct=self._risk().max_margin_usage_pct,
                liquidation_cap=self.cfg.margin_mode == "isolated",
            )
        return max(1, min(lev, cap))

    async def _prepare_symbol(self, symbol: str, leverage: int, side: Side,
                              isolated_safe_leverage: int | None = None) -> list[str]:
        """Margin mode (best effort) + leverage (must succeed). Returns warnings.

        ``isolated_safe_leverage``: the highest leverage whose liquidation stays beyond the stop if the
        position ends up ISOLATED. Used when cross margin was asked for but the exchange refused to switch
        the symbol (open orders / positions, multi-asset mode...): the position then trades in the symbol's
        existing mode, so the leverage chosen for cross could liquidate it before the stop."""
        warnings: list[str] = []
        fam, mode = self._family, self.cfg.margin_mode
        mode_unsure = False
        key = symbol   # once per symbol and session (bybit UTA: the call is account-wide)
        if (fam != "okx" and key not in self._margin_mode_set
                and (getattr(self.exchange, "has", {}) or {}).get("setMarginMode", True)):
            params = {"leverage": leverage} if fam == "bybit" else {}
            try:
                await self.exchange.set_margin_mode(mode, symbol, params)
                self._margin_mode_set.add(key)
            except cx.NoChange:
                self._margin_mode_set.add(key)
            except cx.BaseError as e:
                if _ALREADY_SET_RE.search(str(e)):
                    self._margin_mode_set.add(key)
                else:
                    # Not a stop-safety issue: liquidation stays beyond the stop in both modes.
                    logger.warning("%s: set_margin_mode(%s, %s) failed: %s", self.name, mode, symbol, e)
                    warnings.append(f"Marjin modu '{mode}' ayarlanamadı: {_short(e, 120)}")
                    mode_unsure = True
                    if mode == "cross" and isolated_safe_leverage is not None and leverage > isolated_safe_leverage:
                        warnings.append(
                            f"Coin izole kalmış olabilir: kaldıraç {leverage}x yerine {isolated_safe_leverage}x "
                            "kullanıldı (tasfiye fiyatı stoptan önce gelmesin diye)."
                        )
                        leverage = isolated_safe_leverage
        lev_params: dict[str, Any] = {}
        if fam == "okx":
            lev_params["marginMode"] = mode
            if self._hedged and mode == "isolated":
                lev_params["posSide"] = side.value
        elif fam == "bitget" and self._hedged and mode == "isolated":
            lev_params["holdSide"] = side.value
        # The maximum for this symbol and this account is not always known up front (a new or restricted
        # account may be limited to 20x whatever the coin allows). Every "too high" answer is treated alike:
        # jump to the limit named in the message when there is one, else to the next common level.
        lev: int | None = leverage
        last_error: Exception | None = None
        account_limit: int | None = None
        network_retry = True
        while lev is not None and lev >= 1:
            try:
                await self.exchange.set_leverage(lev, symbol, lev_params)
            except cx.NoChange:
                pass
            except cx.NetworkError as e:             # no answer (timeout, rate limit...): not a refusal
                last_error = e
                if network_retry:
                    network_retry = False            # setting a leverage twice is harmless
                    if isinstance(e, cx.InvalidNonce):
                        await self._sync_clock(force=True)
                    await self._sleep(self.retry_delay)
                    continue
                break
            except cx.BaseError as e:
                text = str(e)
                if not _ALREADY_SET_RE.search(text):
                    last_error = e
                    if _LEVERAGE_INVALID_RE.search(text):
                        hints = [int(h) for pair in _LEVERAGE_HINT_RE.findall(text) for h in pair if h and 1 <= int(h) < lev]
                        lev = max(hints) if hints else next((x for x in _LEVERAGE_STEPS if x < lev), None)
                        if _LEVERAGE_ACCOUNT_RE.search(text):
                            account_limit = lev
                        continue
                    break                            # any other refusal: see what the symbol has right now
            now = self._now_ms()
            if lev != leverage:
                warnings.append(f"Kaldıraç {leverage}x yerine {lev}x (borsanın bu coin / hesap için izin verdiği en yüksek).")
                self._leverage_caps[symbol] = (lev, now)         # later trades are sized with it from the start
                if account_limit is not None:
                    self._leverage_caps["*"] = (max(lev, account_limit), now)
            self._set_leverage[symbol] = lev
            return warnings
        # No leverage could be set. The symbol still HAS a leverage on the exchange: trade with that one when
        # it is known and safe (cross: any; isolated: not above the wanted one, whose liquidation is beyond the
        # stop) instead of giving the trade up. The margin check after this call sizes the position to it.
        current = await self._current_leverage(symbol)
        if current is not None and (current <= leverage or (mode == "cross" and not mode_unsure)):
            logger.warning("%s: set_leverage on %s refused (%s); using the symbol's current %sx",
                           self.name, symbol, last_error, current)
            warnings.append(f"Kaldıraç ayarlanamadı ({_short(last_error or Exception(), 100)}); borsadaki mevcut "
                            f"kaldıraç {current}x ile devam edildi.")
            self._set_leverage[symbol] = current
            return warnings
        raise BrokerError(f"{self.name}: kaldıraç ayarlanamadı: {_short(last_error or Exception())}")

    async def _current_leverage(self, symbol: str) -> int | None:
        """The leverage the symbol has on the venue right now (binance: symbol configuration). None = unknown."""
        ex = self.exchange
        if self._family != "binance" or not (getattr(ex, "has", {}) or {}).get("fetchLeverages"):
            return None
        try:
            info = (await ex.fetch_leverages([symbol]) or {}).get(symbol) or {}
        except Exception as e:      # read-only helper: a failure only means "unknown"
            logger.warning("%s: current leverage of %s unreadable: %s", self.name, symbol, e)
            return None
        lev = num(info.get("longLeverage")) or num(info.get("shortLeverage"))
        return int(lev) if lev and lev >= 1 else None

    # ------------------------------------------------------------------ order lookups
    async def _fetch_order(self, order_id: str, symbol: str, kind: str, since: int | None = None) -> dict[str, Any]:
        """Fetch one of our orders. ``kind``: entry | close | sl | tp (sl/tp are trigger orders)."""
        trigger = kind in ("sl", "tp")
        ex, fam = self.exchange, self._family
        if fam == "bybit":
            # fetchOrder is limited on UTA and fetchClosedOrder only matches "Filled": use
            # open orders, then the history (filled AND cancelled) filtered by orderId.
            p = {"trigger": True} if trigger else {}
            try:
                return await ex.fetch_open_order(order_id, symbol, p)
            except cx.OrderNotFound:
                pass
            for extra in ([p, {}] if trigger else [p]):
                history = await ex.fetch_canceled_and_closed_orders(symbol, since, None, {"orderId": order_id, **extra})
                for o in history or []:
                    if str(o.get("id")) == str(order_id):
                        return o
            raise cx.OrderNotFound(f"bybit order {order_id} not found")
        if fam == "bitget" and trigger:
            plan = {"planType": "profit_loss"}
            for o in await ex.fetch_open_orders(symbol, None, None, plan):
                if str(o.get("id")) == str(order_id):
                    return o
            for o in await ex.fetch_canceled_and_closed_orders(symbol, since, None, plan):
                if str(o.get("id")) == str(order_id):
                    return o
            raise cx.OrderNotFound(f"bitget plan order {order_id} not found")
        if fam == "binance" and trigger:
            try:
                return await ex.fetch_order(order_id, symbol, {"trigger": True})
            except cx.OrderNotFound as missing:
                # Binance has answered -2013 for conditional orders that are live or just completed: look the id
                # up in the conditional-order history (GET allAlgoOrders from this algoId on) before believing it.
                fetch_history = getattr(ex, "fetch_orders", None)
                if fetch_history is None:
                    raise
                try:
                    history = await fetch_history(symbol, None, 10, {"trigger": True, "algoId": order_id})
                except cx.OrderNotFound:
                    raise missing from None
                except cx.OperationFailed:
                    raise                       # no answer: unknown, the caller asks again on the next pass
                except cx.BaseError as e:
                    logger.warning("%s: conditional order history of %s unreadable: %s", self.name, symbol, e)
                    raise missing from None
                for o in history or []:
                    if str(o.get("id")) == str(order_id):
                        return o
                raise
        if fam in ("binance", "okx") or not trigger:
            return await ex.fetch_order(order_id, symbol, {"trigger": True} if trigger else {})
        try:
            return await ex.fetch_order(order_id, symbol, {"trigger": True})
        except cx.OrderNotFound:
            raise
        except cx.BaseError:
            return await ex.fetch_order(order_id, symbol, {})

    async def _order_state(self, order_id: str, symbol: str, kind: str, since: int | None = None) -> OrderState:
        order = await self._fetch_order(order_id, symbol, kind, since)
        state = parse_order_state(order, trigger=kind in ("sl", "tp"))
        if state.status == "filled" and kind in ("sl", "tp") and self._family == "binance" and state.linked_ids:
            # A triggered binance algo order reads "closed" whatever became of its market child. Only the child
            # tells what really executed: it may have been rejected, or have ended early with part of the size.
            try:
                child = parse_order_state(await self.exchange.fetch_order(state.linked_ids[0], symbol, {}), trigger=False)
            except cx.OperationFailed:
                raise                           # no answer: book nothing on a guess, read it again next pass
            except cx.BaseError as e:           # not readable at all: the algo order's own data has to do
                logger.warning("%s: could not read triggered child %s of %s: %s", self.name, state.linked_ids[0],
                               order_id, e)
                return state
            if child.status == "open":          # the market child is still executing: read it again next time
                return OrderState("open", child.filled, child.average, state.amount, state.linked_ids, order)
            if child.status == "canceled":
                if not child.filled:
                    return OrderState("canceled", 0.0, None, state.amount, state.linked_ids, order)
                state.filled = child.filled     # ended early: only what it executed is closed
                state.average = child.average or state.average
            elif child.status == "filled":
                state.filled = child.filled or state.filled
                state.average = child.average or state.average
        return state

    def _cancel_param_sets(self, kind: str) -> list[dict[str, Any]]:
        trigger = kind in ("sl", "tp")
        fam = self._family
        if not trigger or fam == "bybit":
            return [{}]
        if fam == "bitget":
            return [{"trigger": True, "planType": "loss_plan" if kind == "sl" else "profit_plan"}]
        if fam in ("binance", "okx"):
            return [{"trigger": True}]
        return [{"trigger": True}, {}]

    async def _cancel(self, order_id: str | None, symbol: str, kind: str) -> bool:
        """Cancel one of our orders. True = cancelled, False = it no longer exists. Raises BrokerError."""
        if not order_id:
            return False
        last: Exception | None = None
        for params in self._cancel_param_sets(kind):
            try:
                await self.exchange.cancel_order(order_id, symbol, params)
                return True
            except cx.OrderNotFound:
                # binance has answered "unknown order" for conditional orders that were still live: believe it
                # only when the order is not among the open conditional orders either (else: retried later).
                if kind in ("sl", "tp") and await self._is_live_trigger(symbol, str(order_id)) is not False:
                    raise BrokerError(f"{self.name}: emir iptal edilemedi ({order_id}): borsa emri tanımadı ama "
                                      f"emir hâlâ açık görünüyor ya da açık emirler okunamadı") from None
                return False
            except cx.BaseError as e:
                last = e
        if _unclear(last):
            # (sending the cancel again is harmless: one for an order that is gone is answered "unknown order")
            raise BrokerError(f"{self.name}: emir iptal isteğine yanıt alınamadı ({order_id}): {_short(last)}; emir "
                              f"iptal edilmiş de olabilir, hâlâ açık da.") from last
        raise BrokerError(f"{self.name}: emir iptal edilemedi ({order_id}): {_short(last or Exception())}")

    async def _lookup_client_id(self, symbol: str, client_id: str, since: int) -> dict[str, Any] | None:
        """One look-up of an order of ours by its client id: the order, or None when the venue answered that it has
        none. Raises the ccxt error when a read got no answer."""
        ex, fam = self.exchange, self._family
        if fam in ("binance", "okx", "bitget"):
            try:
                return await ex.fetch_order("", symbol, {"clientOrderId": client_id})
            except cx.OrderNotFound:
                pass
        lists = [await ex.fetch_open_orders(symbol)]
        try:
            lists.append(await ex.fetch_closed_orders(symbol, since))
        except cx.BaseError:
            pass
        for orders in lists:
            for o in orders or []:
                if o.get("clientOrderId") == client_id:
                    return o
        return None

    async def _find_by_client_id(self, symbol: str, client_id: str, since: int, *,
                                 attempts: int = _LOOKUP_ATTEMPTS) -> tuple[dict[str, Any] | None, bool]:
        """Look for an order of ours whose request failed without a clear answer (see _unclear): it may exist all the
        same. Asks up to ``attempts`` times, ``retry_delay`` apart and never longer than _LOOKUP_WINDOW_SEC in all
        (each read is cut off when the window ends). Returns (the order, True) as soon as it is found; (None, True)
        when the venue answered that it has no such order (it can still show up later: the answer was lost); (None,
        False) when no read got an answer at all: the bot could not look."""
        loop = asyncio.get_running_loop()
        end = loop.time() + _LOOKUP_WINDOW_SEC
        looked = False
        for attempt in range(max(1, attempts)):
            if attempt:
                if loop.time() + self.retry_delay >= end:
                    break
                await self._sleep(self.retry_delay)
            left = end - loop.time()
            if left <= 0:
                break
            try:
                found = await asyncio.wait_for(self._lookup_client_id(symbol, client_id, since), left)
            except Exception as e:      # (ccxt's errors, the window's timeout, an answer ccxt could not parse)
                logger.warning("%s: lookup by client id %s got no answer: %r", self.name, client_id, e)
                continue
            looked = True
            if found is not None:
                return found, True
        return None, looked

    # ------------------------------------------------------------------ trading
    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        self._require_swap()
        info, side = req.symbol, req.plan.side
        symbol = info.broker_symbol
        qty = floor_to_step(req.quantity, info.qty_step)
        if qty <= 0 or qty < info.min_qty:
            raise BrokerError(f"{self.name}: miktar ({_fmt(qty)}) minimum emir miktarının ({_fmt(info.min_qty)}) altında.")
        sl = round_to_tick(req.stop_loss, info.price_tick)
        tps = [TakeProfit(price=round_to_tick(tp.price, info.price_tick), close_fraction=tp.close_fraction)
               for tp in req.take_profits]
        long = side is Side.LONG

        quote: Quote | None = None
        limit_price: float | None = None
        if req.entry_type is EntryType.MARKET:
            quote = await self.get_quote(info)
            if (long and sl >= quote.bid) or (not long and sl <= quote.ask):
                raise BrokerError(
                    f"{self.name}: stop ({_fmt(sl)}) mevcut fiyatın ({_fmt(quote.last)}) yanlış tarafında; işlem açılmadı."
                )
            ref_price = quote.entry_price_for(side)
        else:
            if not req.entry_price:
                raise BrokerError(f"{self.name}: limit emri için giriş fiyatı yok.")
            limit_price = round_to_tick(req.entry_price, info.price_tick)
            if (long and sl >= limit_price) or (not long and sl <= limit_price):
                raise BrokerError(f"{self.name}: stop ({_fmt(sl)}) limit girişin ({_fmt(limit_price)}) yanlış tarafında.")
            ref_price = limit_price

        now = self._now_ms()
        data: dict[str, Any] = {
            "exchange": getattr(self.exchange, "id", self.cfg.exchange),
            "base": info.base_asset,
            "client_tag": req.client_tag,
            "entry_type": req.entry_type.value,
            "entry_price": limit_price,
            "qty_step": info.qty_step,
            "min_qty": info.min_qty,
            "price_tick": info.price_tick,
            "contract_size": info.contract_size,
            "price_multiplier": info.extra.get("price_multiplier", 1.0),
            "margin_mode": self.cfg.margin_mode,
            "hedged": self._hedged,
            "sl_order_id": None,
            "sl_price": sl,
            "protected_qty": 0.0,
            "tp_order_ids": [],
            "tp_levels": [tp.model_dump() for tp in tps],
            "closed_qty": 0.0,
            "exits": [],
            "attached": False,
            "entry_done": False,
            "opened_at": now,
            "entry_created_ms": now,
            "warnings": [],
            "close_seq": 0,
        }
        pos = BrokerPosition(
            broker=self.name, broker_symbol=symbol, side=side, status=PositionStatus.PENDING,
            quantity=qty, stop_loss=sl, take_profits=tps, risk_amount=None, data=data,
        )
        data = pos.data   # pydantic copied the dict
        shrunk = self._margin_shrunk.pop(symbol, None)
        if shrunk and abs(shrunk[0] - qty) <= info.qty_step / 2 and 0 <= now - shrunk[1] < _SHRUNK_NOTE_TTL_MS:
            data["size_capped"] = True      # sized below the target risk because of margin (see size_position)

        leverage = await self._resolve_leverage(req, qty, ref_price)
        data["leverage"] = leverage
        client_id = self._client_id(req.client_tag, "e")
        data["entry_client_id"] = client_id
        params: dict[str, Any] = {"clientOrderId": client_id, **self._position_params(), **self._margin_params()}
        if req.entry_type is EntryType.LIMIT:
            attach = self._attach_params(sl, tps)
            if attach:
                params.update(attach)
                data.update(attached=True, attached_sl_price=sl,
                            attached_tp_price=tps[0].price if tps else None)

        planned = qty
        async with self._lock(symbol):
            if not self._hedged:
                await self._refuse_netting(pos, info)       # before anything is sent or changed on the venue
            frac = abs(ref_price - req.stop_loss) / ref_price
            safe = max(1, math.floor(1 / (frac * 1.5 + 0.005)))
            data["warnings"] += await self._prepare_symbol(symbol, leverage, side, isolated_safe_leverage=safe)
            actual = self._set_leverage.get(symbol, leverage)
            data["leverage"] = actual                       # what the venue accepted, not what was asked for
            why = None
            # lower than the size was planned with (req.leverage: what size_position checked the margin at)
            if actual < max(leverage, math.ceil((req.leverage or 0) - 1e-9)):
                qty = await self._fit_to_leverage(pos, info, qty, ref_price, actual)
                why = f"Kaldıraç {actual}x ile teminat yetmediği için" if qty < planned else None
            entry, sent = await self._send_entry(pos, req, qty, ref_price, limit_price, params, now)
            if sent < qty:
                qty, why = sent, "Borsa teminatı yetersiz bulduğu için"
        if qty < planned:
            # Never larger than planned; the owner is told the planned size, the opened size and its risk.
            pos.quantity = qty
            data["size_capped"] = True
            loss = qty * self._loss_per_unit(info, ref_price, req.stop_loss)
            data["warnings"].append(f"{why} pozisyon küçültüldü: planlanan {_fmt(planned)} yerine {_fmt(qty)} "
                                    f"açıldı; stopta kayıp ~{loss:.2f}.")
        data["entry_order_id"] = str(entry.get("id"))
        self._known_order_ids.add(data["entry_order_id"])
        logger.info("%s: entry %s %s %s qty=%s id=%s", self.name, req.entry_type.value, side.value, symbol, qty, data["entry_order_id"])

        if req.entry_type is EntryType.MARKET:
            return await self._after_market_entry(pos, entry, quote)
        # Limit: one immediate check (a marketable limit fills at once and must be protected now).
        try:
            await self._sync_entry(pos)
        except (BrokerError, cx.BaseError) as e:
            logger.warning("%s: initial limit fill check failed: %s", self.name, e)
        return pos

    async def _refuse_netting(self, pos: BrokerPosition, info: SymbolInfo) -> None:
        """One-way mode: the exchange nets an order against an opposite position (or a resting opposite entry)
        on the same symbol: the old trade shrinks instead of a new one opening and both are booked wrongly.
        Raises BrokerError before anything is sent. The engine's no_hedge check normally skips such a signal
        earlier; this also sees what the bot did not open. A read that fails does not stop the entry: the
        owner gets a warning instead."""
        symbol, side = pos.broker_symbol, pos.side
        step = info.qty_step
        try:
            held = await self._venue_position_size(symbol, side)
        except Exception as e:      # this check runs before every entry: nothing that breaks in it may cost the trade
            logger.warning("%s: positions of %s unreadable before the entry: %r", self.name, symbol, e)
            held = None
        if held is not None and held < -step / 2:
            raise BrokerError(
                f"{self.name}: {symbol} için borsada ters yönde açık pozisyon var ({side.opposite.value.upper()} "
                f"{_fmt(-held)}). Tek yönlü modda yeni {side.value.upper()} emri yeni işlem açmaz, o pozisyonu "
                f"kapatırdı; işlem açılmadı."
            )
        if held is None and (getattr(self.exchange, "has", {}) or {}).get("fetchPositions", True):
            pos.data["warnings"].append("Borsadaki açık pozisyonlar okunamadı; ters yönde pozisyon kontrolü yapılamadı.")
        try:
            orders = await self.exchange.fetch_open_orders(symbol)
            opposite = self._close_side(side)
            resting = [o for o in orders or []
                       if o.get("side") == opposite and not o.get("reduceOnly") and not num(o.get("triggerPrice"))]
            amount = sum(max(0.0, (num(o.get("amount")) or 0.0) - (num(o.get("filled")) or 0.0)) for o in resting)
        except Exception as e:      # as above: also an error that is not the exchange's
            logger.warning("%s: open orders of %s unreadable before the entry: %r", self.name, symbol, e)
            pos.data["warnings"].append("Borsadaki bekleyen emirler okunamadı; ters yönde emir kontrolü yapılamadı.")
            return
        if amount - max(held or 0.0, 0.0) > step / 2:      # more than would only close what we hold in our direction
            raise BrokerError(
                f"{self.name}: {symbol} için borsada ters yönde bekleyen giriş emri var ({side.opposite.value.upper()} "
                f"{_fmt(amount)}). Tek yönlü modda iki emir birbirini kapatırdı; işlem açılmadı."
            )

    async def _fit_to_leverage(self, pos: BrokerPosition, info: SymbolInfo, qty: float, ref_price: float,
                               leverage: int) -> float:
        """The venue accepted a lower leverage than the size was planned with. When its margin no longer fits
        ``max_margin_usage_pct`` of the free balance: the largest quantity that does (never larger), or
        BrokerError before any order is sent when not even the smallest order fits."""
        try:
            free = await self.get_free_margin()
        except BrokerError as e:
            # Without the balance the margin cannot be checked here. The exchange checks it when the order arrives
            # (_send_entry then tries a smaller size): an unreadable balance must not cost the trade.
            logger.warning("%s: free balance unreadable after the leverage change (%s); the exchange decides",
                           self.name, e)
            return qty
        allowed = free * self._risk().max_margin_usage_pct / 100.0
        unit = info.contract_size * ref_price
        if qty * unit / leverage <= allowed:
            return qty
        new_qty = min(qty, floor_to_step(allowed * leverage / unit * 0.999, info.qty_step))
        if (new_qty <= 0 or new_qty < info.min_qty
                or (info.min_notional and new_qty * unit < info.min_notional * (1 - 1e-9))):
            raise BrokerError(
                f"{self.name}: yetersiz teminat: borsa {pos.broker_symbol} için en fazla {leverage}x kaldıraca izin "
                f"verdi; {_fmt(qty)} için {qty * unit / leverage:.2f} teminat gerekiyor, kullanılabilir {allowed:.2f} "
                f"(serbest bakiye {free:.2f}) en küçük emre bile yetmiyor. İşlem açılmadı."
            )
        return new_qty

    async def _send_entry(
        self, pos: BrokerPosition, req: OpenRequest, qty: float, ref_price: float, limit_price: float | None,
        params: dict[str, Any], since: int,
    ) -> tuple[dict[str, Any], float]:
        """Send the entry order; returns (order, quantity sent). The exchange's own margin requirement is a
        little above notional / leverage (fees, mark price): on "margin is insufficient" the order was refused
        as a whole, so it is sent again smaller (3/4, then 1/2 of the size) instead of losing the trade."""
        info, d = req.symbol, pos.data
        unit = info.contract_size * ref_price
        refused: BrokerError | None = None
        for attempt, share in enumerate((1.0, 0.75, 0.5)):
            q = qty if not attempt else floor_to_step(qty * share, info.qty_step)
            if attempt and (q <= 0 or q < info.min_qty
                            or (info.min_notional and q * unit < info.min_notional * (1 - 1e-9))):
                break
            cid = d["entry_client_id"] if not attempt else self._client_id(req.client_tag, f"e{attempt + 1}")
            try:
                entry = await self._create_entry(pos.broker_symbol, req.entry_type, self._order_side(pos.side), q,
                                                 limit_price, {**params, "clientOrderId": cid}, cid, since)
            except BrokerError as e:
                if not isinstance(e.__cause__, cx.InsufficientFunds):
                    raise
                refused = e
                logger.warning("%s: entry of %s %s refused for margin: %s", self.name, _fmt(q), pos.broker_symbol, e)
                continue
            d["entry_client_id"] = cid
            return entry, q
        assert refused is not None
        raise refused

    async def _create_entry(
        self, symbol: str, entry_type: EntryType, side: str, qty: float, price: float | None,
        params: dict[str, Any], client_id: str, since: int,
    ) -> dict[str, Any]:
        """Send the entry order; it is never sent twice. When the request ends without a clear answer (see _unclear)
        the order may exist: it is looked up by its client id for a short while (_find_by_client_id) and adopted when
        found; otherwise OrderOutcomeUnknownError says that whether it exists is not known (the engine then reports
        "açılıp açılmadığı bilinmiyor", never "açılmadı")."""
        try:
            return await self.exchange.create_order(symbol, entry_type.value, side, qty, price, params)
        except cx.InsufficientFunds as e:
            raise BrokerError(f"{self.name}: giriş emri reddedildi: yetersiz bakiye ({_short(e)})") from e
        except (cx.OperationFailed, cx.DuplicateOrderId) as e:
            unclear = _unclear(e)
            # (a clear refusal among these - binance -1021, a rate limit - is looked up once, as before)
            found, looked = await self._find_by_client_id(symbol, client_id, since - 60_000,
                                                          attempts=_LOOKUP_ATTEMPTS if unclear else 1)
            if found:
                logger.warning("%s: entry request failed (%s) but order %s exists; adopting it", self.name, e, found.get("id"))
                return found
            if not unclear:
                raise BrokerError(f"{self.name}: giriş emri reddedildi: {_short(e)}") from e
            where = (f"bu etiketle ({client_id}) borsada henüz görünmüyor, ama yanıt kaybolduğu için kesin değil"
                     if looked else f"borsaya ulaşılamadığı için bu etiketle ({client_id}) aranamadı")
            if entry_type is EntryType.MARKET:
                check = (f"Borsada {symbol} pozisyonuna elle bakın; emir gerçekleşmişse bot o pozisyonu takip ETMİYOR "
                         f"ve stopu YOK: stop koyun ya da kapatın.")
            else:
                check = (f"Borsada {symbol} bekleyen emirlerine ve pozisyonuna elle bakın; emir borsadaysa bot onu "
                         f"takip ETMİYOR ve dolarsa stopu OLMAYACAK: iptal edin ya da stop koyun.")
            logger.error("%s: entry %s on %s sent, its answer lost (%s) and not found by its client id (looked: %s)",
                         self.name, client_id, symbol, e, looked)
            raise OrderOutcomeUnknownError(
                f"{self.name}: giriş emri gönderildi ama yanıtı alınamadı ({_short(e)}); emir {OUTCOME_UNKNOWN_MARK} "
                f"{where}. Aynı emir ikinci kez GÖNDERİLMEDİ. {check}"
            ) from e
        except cx.BaseError as e:
            hint = ""
            if "-4061" in str(e):       # binance: the ACCOUNT is in the other position mode than position_mode says
                hint = (" Hesabın pozisyon modu ayarla uyuşmuyor: Binance'te Tercihler > Pozisyon Modu'nu "
                        + ("çift yönlü (hedge)" if self._hedged else "tek yönlü") + " yapın.")
            raise BrokerError(f"{self.name}: giriş emri reddedildi: {_short(e)}{hint}") from e

    async def _await_fill(self, order: dict[str, Any], symbol: str, kind: str) -> OrderState | None:
        """Poll a market order until it is filled/cancelled. None = state could not be read."""
        state = parse_order_state(order, trigger=False)
        known = order.get("status") is not None
        for _ in range(self.fill_poll_attempts):
            if known and (state.status != "open" or (state.filled and state.amount and state.filled >= state.amount)):
                return state
            await self._sleep(self.fill_poll_delay)
            try:
                state = await self._order_state(str(order["id"]), symbol, kind)
                known = True
            except cx.BaseError as e:
                logger.warning("%s: fill check of %s failed: %s", self.name, order.get("id"), e)
        return state if known else None

    async def _after_market_entry(self, pos: BrokerPosition, entry: dict[str, Any], quote: Quote | None) -> BrokerPosition:
        d, symbol = pos.data, pos.broker_symbol
        state = await self._await_fill(entry, symbol, "entry")
        if state is not None and state.status == "open":
            # Market order still not complete: stop further fills so the protected size is final.
            await self._cancel_quiet(str(entry["id"]), symbol, "entry")
            try:
                state = await self._order_state(str(entry["id"]), symbol, "entry")
            except cx.BaseError:
                pass
        if state is None or state.status == "open" or (state.status == "filled" and not state.filled):
            filled, avg = pos.quantity, (state.average if state else None)
            d["fill_assumed"] = True   # sync re-reads the entry and corrects the size
            logger.warning("%s: entry fill unknown, protecting the full requested size", self.name)
        else:
            filled, avg = state.filled or 0.0, state.average
            d["entry_done"] = True
        if filled <= 0:
            pos.status = PositionStatus.FAILED
            pos.error = f"{self.name}: piyasa emri gerçekleşmedi (durum: {state.status if state else '?'})."
            return pos
        pos.filled_quantity = filled
        pos.remaining_quantity = filled
        pos.avg_entry_price = avg or (quote.entry_price_for(pos.side) if quote else None)
        pos.status = PositionStatus.OPEN
        d["opened_at"] = self._now_ms()
        return await self._protect_new_fill(pos, emergency_status=PositionStatus.FAILED)

    async def _place_sl(self, pos: BrokerPosition, qty: float, price: float) -> tuple[str | None, Exception | None]:
        return await self._place_trigger(pos, "sl", qty, price, attempts=2)

    async def _place_trigger(self, pos: BrokerPosition, kind: str, qty: float, price: float, *,
                             attempts: int) -> tuple[str | None, Exception | None]:
        """Place one reduce-only stop ("sl") or take-profit ("tp") order. Returns (order id, None) or (None, error).

        binance: every request carries a client id of our own. When a request fails, the order is looked up by
        that id: if it exists the reply was only lost, and it is adopted instead of sending a second one. When
        the outcome stays unknown (no clear answer from the venue), the id is kept in ``lost_orders`` and settled
        by the following syncs (_resolve_lost_orders), so no order of ours can stay behind untracked."""
        d = pos.data
        base = self._sl_params(price) if kind == "sl" else self._tp_params(price)
        last: Exception | None = None
        for attempt in range(attempts):
            if attempt:
                await self._sleep(self.retry_delay)
            params, cid = base, None
            if self._family == "binance":
                d["prot_seq"] = int(d.get("prot_seq", 0)) + 1
                cid = self._client_id(d.get("client_tag", "ct"), f"{kind[0]}{d['prot_seq']}")
                params = {**base, "clientOrderId": cid}
            try:
                order = await self.exchange.create_order(
                    pos.broker_symbol, "market", self._close_side(pos.side), qty, None, params
                )
                oid = str(order.get("id"))
                self._known_order_ids.add(oid)
                return oid, None
            except cx.BaseError as e:
                last = e
                logger.error("%s: %s order attempt %d failed: %s", self.name, kind.upper(), attempt + 1, e)
                oid, clear = await self._find_trigger(pos, cid, qty, price)
                if oid:
                    return oid, None
                if cid and (isinstance(e, cx.OperationFailed) or not clear):
                    d.setdefault("lost_orders", []).append({"cid": cid, "kind": kind, "misses": 0})
        return None, last

    async def _find_trigger(self, pos: BrokerPosition, cid: str | None, qty: float, price: float) -> tuple[str | None, bool]:
        """After a failed create request: look the conditional order up by OUR client id. Returns (its id when
        it exists as the order we asked for -- live or already triggered --, did the venue give a clear answer)."""
        if not cid:
            return None, True
        try:
            found = await self.exchange.fetch_order("", pos.broker_symbol, {"trigger": True, "clientOrderId": cid})
        except cx.OrderNotFound:
            return None, True
        except cx.BaseError as e:
            logger.warning("%s: could not look up conditional order %s: %s", self.name, cid, e)
            return None, False
        if parse_order_state(found, trigger=True).status == "canceled":
            return None, True                   # refused / expired: nothing of ours is live
        step, _, tick, _ = self._market_rules(pos)
        amount, trigger = num(found.get("amount")), num(found.get("triggerPrice"))
        if (amount is None or trigger is None or abs(amount - qty) > max(step / 2, 1e-12)
                or abs(trigger - price) > max(tick / 2, 1e-12)):
            return None, False                  # something with our id that is not what we asked for
        oid = str(found.get("id"))
        logger.warning("%s: conditional order %s (client id %s) exists although its request failed; adopting it",
                       self.name, oid, cid)
        self._known_order_ids.add(oid)
        return oid, True

    async def _resolve_lost_orders(self, pos: BrokerPosition) -> bool:
        """Settle the protective orders whose create request ended without a clear answer (``lost_orders``).
        One that exists after all becomes our stop when the position has none (same size and trigger), else it
        is cancelled: a second reduce-only order must not stay behind. One the venue does not know for
        ``_LOST_ORDER_MISSES`` passes never arrived. True = nothing is left to settle."""
        d, symbol = pos.data, pos.broker_symbol
        keep: list[dict[str, Any]] = []
        for lo in list(d.get("lost_orders") or []):
            try:
                found = await self.exchange.fetch_order("", symbol, {"trigger": True, "clientOrderId": lo["cid"]})
            except cx.OrderNotFound:
                lo["misses"] = int(lo.get("misses", 0)) + 1
                if lo["misses"] < _LOST_ORDER_MISSES:
                    keep.append(lo)
                continue
            except cx.BaseError as e:
                logger.warning("%s: could not look up conditional order %s: %s", self.name, lo["cid"], e)
                keep.append(lo)
                continue
            oid = str(found.get("id"))
            state = parse_order_state(found, trigger=True)
            self._remember(d, oid)
            if state.status != "open":
                if state.status == "filled":    # it triggered: its trades belong to this position
                    d.setdefault("linked_order_ids", []).extend(state.linked_ids)
                continue
            step, _, tick, _ = self._market_rules(pos)
            amount, trigger = num(found.get("amount")), num(found.get("triggerPrice"))
            unprotected = (pos.status is PositionStatus.OPEN and not d.get("closing") and pos.remaining_quantity > 0
                           and (d.get("unprotected") or not d.get("sl_order_id")))
            if (lo.get("kind") == "sl" and unprotected and amount is not None and trigger is not None
                    and abs(amount - pos.remaining_quantity) <= max(step / 2, 1e-12)
                    and abs(trigger - float(d["sl_price"])) <= max(tick / 2, 1e-12)):
                logger.warning("%s: stop %s (client id %s) was placed after all; adopting it", self.name, oid, lo["cid"])
                d.update(sl_order_id=oid, protected_qty=pos.remaining_quantity, unprotected=False, sl_cancelled=False)
                pos.stop_loss = float(d["sl_price"])
                pos.error = _drop_error(pos.error, _CRITICAL_MARK)
                continue
            try:
                await self._cancel(oid, symbol, lo.get("kind") or "sl")
            except BrokerError as e:
                logger.error("%s: %s", self.name, e)
                keep.append(lo)
        d["lost_orders"] = keep
        return not keep

    async def _exit_fill(self, order: dict[str, Any], symbol: str, qty: float) -> tuple[float | None, float | None]:
        """(executed size, average price) of a reduce-only market exit. Executed None = unknown.

        Venues end a market order early (slippage / price-band protection, thin book): bybit
        'PartiallyFilledCanceled' is reported as 'closed', binance/okx expire the rest. So the
        executed size is the order's ``filled``, never the requested size."""
        oid = str(order.get("id"))
        state = await self._await_fill(order, symbol, "close")
        if (state is not None and state.status == "open"
                and not (state.filled and state.amount and state.filled >= state.amount - 1e-12)):
            # still working after polling: stop it so the executed size becomes final
            await self._cancel_quiet(oid, symbol, "close")
            try:
                state = await self._order_state(oid, symbol, "close")
            except cx.BaseError as e:
                logger.warning("%s: exit order %s state unknown after cancel: %s", self.name, oid, e)
        if state is None:
            return None, None
        if state.status == "open":
            if state.filled and state.amount and state.filled >= state.amount - 1e-12:
                return min(state.filled, qty), state.average
            return None, state.average
        if state.status == "filled":
            executed = state.filled if state.filled else qty
        else:
            executed = state.filled or 0.0          # cancelled / expired / rejected: only what executed
        return min(executed, qty), state.average

    async def _book_exit_order(self, pos: BrokerPosition, order: dict[str, Any], qty: float, kind: str) -> float | None:
        """Book what a reduce-only market exit really executed. None = result unknown; the order is
        then kept in ``unconfirmed_closes`` and settled by the next sync (the stop stays as it is)."""
        d = pos.data
        oid = str(order.get("id"))
        self._remember(d, oid)
        executed, avg = await self._exit_fill(order, pos.broker_symbol, qty)
        if executed is None:
            size = await self._venue_position_size(pos.broker_symbol, pos.side)
            if size is not None and size < self._market_rules(pos)[0] / 2:
                executed = qty                      # nothing in our direction is left on the venue
            else:
                logger.error("%s: result of exit order %s on %s unknown; re-checked on next sync",
                             self.name, oid, pos.broker_symbol)
                d.setdefault("unconfirmed_closes", []).append({"order_id": oid, "qty": qty, "kind": kind})
                return None
        executed = min(executed, pos.remaining_quantity)
        if executed > 0:
            self._record_exit(pos, oid, executed, avg, kind)
        return executed

    async def _emergency_close(self, pos: BrokerPosition, qty: float) -> tuple[bool, float | None]:
        """Reduce-only market close. Returns (sent, executed size; None = unknown). A request without a clear answer
        (see _unclear) counts as sent: the order is looked up by its client id once and booked when found, else kept
        in ``unconfirmed_closes`` by that id (the next sync looks it up first, before anything is sent again)."""
        d = pos.data
        if qty <= 0:
            return True, 0.0
        d["close_seq"] = int(d.get("close_seq", 0)) + 1
        cid = self._client_id(d.get("client_tag", "ct"), f"x{d['close_seq']}")
        sent_ms = self._now_ms()
        try:
            order = await self.exchange.create_order(
                pos.broker_symbol, "market", self._close_side(pos.side), qty, None, self._close_params(cid)
            )
        except cx.BaseError as e:
            if not _unclear(e):
                logger.critical("%s: EMERGENCY CLOSE FAILED for %s: %s", self.name, pos.broker_symbol, e)
                return False, None
            found, _looked = await self._find_by_client_id(pos.broker_symbol, cid, sent_ms - 60_000, attempts=1)
            if found is None:
                logger.critical("%s: EMERGENCY CLOSE of %s sent but its answer was lost (%s); looked up on the next "
                                "sync", self.name, pos.broker_symbol, e)
                d.setdefault("unconfirmed_closes", []).append(
                    {"order_id": None, "client_id": cid, "qty": qty, "kind": "emergency", "since": sent_ms,
                     "misses": 0})
                return True, None
            logger.warning("%s: emergency close %s exists although its answer was lost; booking it", self.name, cid)
            order = found
        executed = await self._book_exit_order(pos, order, qty, "emergency")
        if pos.remaining_quantity > 0:
            logger.critical("%s: EMERGENCY CLOSE of %s incomplete (executed %s of %s)",
                            self.name, pos.broker_symbol, executed, qty)
        return True, executed

    def _unprotected_error(self, pos: BrokerPosition, why: str, sent: bool, executed: float | None,
                           requested: float) -> str:
        left = _fmt(pos.remaining_quantity)
        if not sent:
            detail = "pozisyon kapatılamadı"
        elif executed is None:
            detail = f"acil kapatma emrinin sonucu okunamadı ({left} açık olabilir)"
        elif executed > 0:
            detail = f"pozisyon yalnızca kısmen kapatılabildi ({_fmt(executed)}/{_fmt(requested)}); {left} STOPSUZ açık"
        else:
            detail = f"acil kapatma emri gerçekleşmedi; {left} STOPSUZ açık"
        return (f"{_CRITICAL_MARK} {self.name}: stop emri yerleştirilemedi ve {detail}! "
                f"{pos.broker_symbol} pozisyonunu borsada hemen kontrol edin. ({why})")

    async def _protect_new_fill(self, pos: BrokerPosition, *, emergency_status: PositionStatus) -> BrokerPosition:
        """Place (or resize) SL for the open size, then TPs. Rule 1 on SL failure."""
        d = pos.data
        qty = pos.remaining_quantity
        old_sl = d.get("sl_order_id")
        new_id, err = await self._place_sl(pos, qty, float(d["sl_price"]))
        if new_id is None:
            entry_ok = await self._stop_entry(pos)
            requested = pos.remaining_quantity
            sent, executed = await self._emergency_close(pos, requested)
            why = _why(err)
            if pos.remaining_quantity > 0:
                # Part (or all) of it is still open without a stop: keep OPEN so every sync retries.
                d["unprotected"] = True
                pos.error = self._unprotected_error(pos, why, sent, executed, requested)
                return pos
            d["unprotected"] = False
            leftovers_ok = await self._cancel_protection(pos, include_sl=bool(old_sl))
            pos.realized_pnl = await self._realized_pnl(pos)
            pos.error = f"{self.name}: stop emri yerleştirilemedi ({why}); pozisyon piyasa fiyatından kapatıldı."
            if leftovers_ok and entry_ok:
                pos.status = emergency_status
            else:
                # A live entry / reduce-only order must not be forgotten: keep syncing until it is gone.
                d.update(closing=True, closed_reason="emergency", final_status=emergency_status.value)
                pos.error += " Bazı eski emirler iptal edilemedi; tekrar denenecek, borsayı kontrol edin."
            return pos
        old_qty = float(d.get("protected_qty") or 0.0)
        d.update(sl_order_id=new_id, protected_qty=qty, unprotected=False)
        pos.stop_loss = float(d["sl_price"])
        pos.error = _drop_error(pos.error, _CRITICAL_MARK)
        await self._retire_old_sl(pos, old_sl, old_qty)
        if self._done(pos):
            return pos
        await self._cleanup_attached(pos)
        levels = [TakeProfit(**lv) for lv in self._unfilled_levels(pos)]
        await self._replace_take_profits(pos, levels, pos.remaining_quantity)
        return pos

    def _unfilled_levels(self, pos: BrokerPosition) -> list[dict[str, Any]]:
        """TP levels not hit yet (from tp_order_ids, else the planned levels)."""
        orders = pos.data.get("tp_order_ids") or []
        if not orders:
            return list(pos.data.get("tp_levels") or [])
        return [{"price": o["price"], "close_fraction": o["fraction"]} for o in orders if o.get("status") != "filled"]

    async def _replace_take_profits(self, pos: BrokerPosition, levels: list[TakeProfit], total_qty: float) -> None:
        """Cancel our open TP orders, then place ``levels`` for ``total_qty`` (fractions normalized)."""
        d = pos.data
        kept: list[dict[str, Any]] = []
        for tp in d.get("tp_order_ids") or []:
            if tp.get("status") == "filled":
                kept.append(tp)
                continue
            if tp.get("status") == "open" and tp.get("id"):
                try:
                    cancelled = await self._cancel(tp["id"], pos.broker_symbol, "tp")
                except BrokerError as e:
                    # still live: keep tracking it and leave its size out of the new orders
                    logger.error("%s: %s", self.name, e)
                    kept.append(tp)
                    total_qty = max(0.0, _dec_sub(total_qty, float(tp["qty"])))
                    pos.error = (f"{self.name}: eski kâr al emrinin ({tp['id']}) iptal isteğine yanıt alınamadı; hâlâ "
                                 f"açık olabilir, tekrar denenecek." if _unclear(e.__cause__) else
                                 f"{self.name}: eski kâr al emri iptal edilemedi ({tp['id']}); tekrar denenecek.")
                    continue
                self._remember(d, tp["id"])
                if not cancelled:
                    executed = await self._check_vanished_tp(pos, tp)
                    if tp.get("status") == "filled":
                        kept.append(tp)
                        total_qty = max(0.0, _dec_sub(total_qty, executed))
        d["tp_order_ids"] = kept
        step, min_qty, _, _ = self._market_rules(pos)
        qtys = split_quantities(total_qty, [lv.close_fraction for lv in levels], step, min_qty)
        errors: list[str] = []
        for lv, q in zip(levels, qtys):
            entry: dict[str, Any] = {"id": None, "price": lv.price, "qty": q, "fraction": lv.close_fraction,
                                     "status": "skipped" if q <= 0 else "failed", "attempts": 0}
            d["tp_order_ids"].append(entry)
            if q > 0:
                err = await self._place_tp(pos, entry)
                if err:
                    errors.append(err)
        skipped = [lv.price for lv, q in zip(levels, qtys) if q <= 0]
        if skipped:
            d["warnings"].append(f"Minimum miktar nedeniyle birleştirilen TP seviyeleri: {', '.join(_fmt(p) for p in skipped)}")
        if errors:
            pos.error = f"{self.name}: kâr al emri yerleştirilemedi (stop aktif): {'; '.join(errors)}"

    async def _place_tp(self, pos: BrokerPosition, entry: dict[str, Any]) -> str | None:
        entry["attempts"] = int(entry.get("attempts", 0)) + 1
        oid, err = await self._place_trigger(pos, "tp", entry["qty"], float(entry["price"]), attempts=1)
        if oid is None:
            entry["status"] = "failed"
            return f"TP {_fmt(entry['price'])}: {_why(err, 120)}"
        entry.update(id=oid, status="open")
        return None

    def _rearm_tp_rest(self, pos: BrokerPosition, tp: dict[str, Any], executed: float, rest: float,
                       min_qty: float) -> None:
        """A TP order ended with only part of its size executed: the rest is still open. Queue a TP at
        the same price for it (placed by the retry in sync) and tell the owner."""
        note = (f"{self.name}: TP {_fmt(tp['price'])} kısmen gerçekleşti ({_fmt(executed)}/{_fmt(float(tp['qty']))}); "
                f"kalan {_fmt(rest)} açık, stop onu koruyor")
        if rest >= min_qty:
            planned = float(tp["qty"]) or 1.0
            pos.data["tp_order_ids"].append({
                "id": None, "price": tp["price"], "qty": rest, "status": "failed", "attempts": 0,
                "fraction": float(tp.get("fraction") or 0.0) * rest / planned, "rest_of": tp.get("id"),
            })
            note += "; kalan için kâr al emri yeniden kuruluyor."
        else:
            note += " (TP emri için minimum miktarın altında)."
        pos.error = _join_error(pos.error, note)

    async def _cancel_quiet(self, order_id: str | None, symbol: str, kind: str) -> bool:
        """Cancel, logging instead of raising. True = cancelled now."""
        try:
            return await self._cancel(order_id, symbol, kind)
        except BrokerError as e:
            logger.error("%s: %s", self.name, e)
            return False

    async def _stop_entry(self, pos: BrokerPosition) -> bool:
        """Cancel a still-working entry and pick up its last fills. False = it may still be live
        (the caller must not forget it: a later fill would be an unprotected position)."""
        d = pos.data
        if d.get("entry_done") or not d.get("entry_order_id"):
            return True
        try:
            await self._cancel(d["entry_order_id"], pos.broker_symbol, "entry")
        except BrokerError as e:
            logger.error("%s: %s", self.name, e)
            return False
        state = await self._refresh_entry_fill(pos)
        if state is None or state.status == "open":
            return False
        d["entry_done"] = True
        return True

    async def _cancel_protection(self, pos: BrokerPosition, *, include_sl: bool = True) -> bool:
        """Cancel our SL / open TPs / stale stops (+ attached children). False if some order may still be live."""
        d = pos.data
        ok = True
        if include_sl and d.get("sl_order_id") and not d.get("sl_cancelled"):
            try:
                await self._cancel(d["sl_order_id"], pos.broker_symbol, "sl")
                d["sl_cancelled"] = True
            except BrokerError as e:
                logger.error("%s: %s", self.name, e)
                ok = False
        for tp in d.get("tp_order_ids") or []:
            if tp.get("status") == "open" and tp.get("id"):
                try:
                    await self._cancel(tp["id"], pos.broker_symbol, "tp")
                    tp["status"] = "canceled"
                except BrokerError as e:
                    logger.error("%s: %s", self.name, e)
                    ok = False
        for so in list(d.get("stale_orders") or []):
            try:
                await self._cancel(so["id"], pos.broker_symbol, so.get("kind") or "sl")
            except BrokerError as e:
                logger.error("%s: stale order still live: %s", self.name, e)
                ok = False
                continue
            d["stale_orders"] = [s for s in d["stale_orders"] if s["id"] != so["id"]]
        if not d.get("stale_orders"):
            pos.error = _drop_error(pos.error, _STALE_MARK)
        if d.get("lost_orders") and not await self._resolve_lost_orders(pos):
            ok = False                      # an order of ours may exist that we could not confirm yet
        return await self._cleanup_attached(pos) and ok

    async def _cleanup_attached(self, pos: BrokerPosition) -> bool:
        """Cancel the venue-created children of an attached SL/TP once our own orders exist.
        False while some child may still be live."""
        d = pos.data
        if not d.get("attached") or d.get("attached_cleaned") or pos.filled_quantity <= 0:
            return True  # children appear per fill, so this re-runs until the entry is complete
        targets = [p for p in (d.get("attached_sl_price"), d.get("attached_tp_price")) if p]
        _, _, tick, _ = self._market_rules(pos)
        tol = max(tick / 2, 1e-12)
        close_side = self._close_side(pos.side)
        created = int(d.get("entry_created_ms") or 0) - 5_000
        try:
            orders = await self._list_open_protective(pos.broker_symbol)
        except cx.BaseError as e:
            logger.warning("%s: cannot list orders to clean attached SL/TP: %s", self.name, e)
            return False
        ok = True
        for o in orders:
            oid = str(o.get("id"))
            if oid in self._known_order_ids or o.get("side") != close_side:
                continue
            ts = o.get("timestamp")
            if ts and ts < created:
                continue
            amount = num(o.get("amount"))
            if amount and amount > pos.filled_quantity * 1.001:
                continue
            prices = [num(o.get(k)) for k in ("triggerPrice", "stopLossPrice", "takeProfitPrice", "stopPrice")]
            if any(p is not None and abs(p - t) <= tol for p in prices for t in targets):
                if await self._cancel_quiet(oid, pos.broker_symbol, "sl"):
                    logger.info("%s: cancelled attached child order %s", self.name, oid)
                else:
                    ok = False
        d["attached_cleaned"] = ok and bool(d.get("entry_done"))
        return ok

    async def _list_open_protective(self, symbol: str) -> list[dict[str, Any]]:
        ex, fam = self.exchange, self._family
        if fam == "okx":
            lists = [await ex.fetch_open_orders(symbol, None, None, {"trigger": True, "ordType": "conditional,oco"})]
        else:
            lists = [await ex.fetch_open_orders(symbol)]
            try:
                lists.append(await ex.fetch_open_orders(symbol, None, None, {"trigger": True}))
            except cx.BaseError:
                pass
        seen: dict[str, dict[str, Any]] = {}
        for orders in lists:
            for o in orders or []:
                seen.setdefault(str(o.get("id")), o)
        return list(seen.values())

    def _record_exit(self, pos: BrokerPosition, order_id: str | None, qty: float, price: float | None, kind: str) -> None:
        d = pos.data
        d.setdefault("exits", []).append({"order_id": order_id, "qty": qty, "price": price, "kind": kind})
        d["closed_qty"] = float(Decimal(str(d.get("closed_qty", 0.0))) + Decimal(str(qty)))
        step = self._market_rules(pos)[0]
        remaining = float(Decimal(str(pos.remaining_quantity)) - Decimal(str(qty)))
        pos.remaining_quantity = 0.0 if remaining <= step / 2 else remaining

    async def _resize_sl(self, pos: BrokerPosition) -> None:
        """New SL for the remaining size first, then cancel the old one."""
        d = pos.data
        if pos.remaining_quantity <= 0 or abs(float(d.get("protected_qty", 0)) - pos.remaining_quantity) < 1e-12:
            return
        new_id, err = await self._place_sl(pos, pos.remaining_quantity, float(d["sl_price"]))
        if new_id is None:
            if d.get("sl_order_id") and not d.get("unprotected"):   # unprotected: that id is a stop that is gone
                pos.error = f"{self.name}: stop miktarı güncellenemedi (eski stop aktif): {_why(err)}"
            else:
                d["unprotected"] = True     # nothing protects the open size: next sync retries / closes
                pos.error = (f"{_CRITICAL_MARK} {self.name}: {pos.broker_symbol} için stop emri yerleştirilemedi; "
                             f"{_fmt(pos.remaining_quantity)} STOPSUZ açık. ({_why(err)})")
            return
        old = d.get("sl_order_id")
        old_qty = float(d.get("protected_qty") or 0.0)
        d.update(sl_order_id=new_id, protected_qty=pos.remaining_quantity, unprotected=False)
        await self._retire_old_sl(pos, old, old_qty)

    async def _retire_old_sl(self, pos: BrokerPosition, old_id: str | None, old_qty: float) -> None:
        """Cancel a replaced stop. One that may still be live is kept in ``stale_orders`` and retried on
        every sync (and before the trade may close): a forgotten reduce-only stop could later close
        another position on the same symbol."""
        if not old_id:
            return
        self._remember(pos.data, old_id)
        cancel_failed = False
        try:
            if await self._cancel(old_id, pos.broker_symbol, "sl"):
                return
        except BrokerError as e:
            logger.error("%s: %s", self.name, e)
            cancel_failed = True
        state = await self._check_old_sl_filled(pos, old_id, old_qty)
        # "missing" after a cancel that FAILED proves nothing (binance has answered "unknown order" for orders
        # that were still listed as open): it stays tracked until a cancel or a lookup gives a clear answer.
        if state is None or state.status == "open" or (cancel_failed and state.status == "missing"):
            self._add_stale(pos, old_id, "sl", old_qty)

    def _add_stale(self, pos: BrokerPosition, order_id: str, kind: str, qty: float) -> None:
        d = pos.data
        stale = d.setdefault("stale_orders", [])
        if not any(str(s["id"]) == str(order_id) for s in stale):
            stale.append({"id": str(order_id), "kind": kind, "qty": qty})
        pos.error = _join_error(pos.error, f"{self.name}: {_STALE_MARK} ({order_id}) iptal edilemedi, borsada "
                                           f"hâlâ aktif olabilir; her kontrolde tekrar denenecek.")

    async def _retry_stale_orders(self, pos: BrokerPosition) -> None:
        d = pos.data
        for so in list(d.get("stale_orders") or []):
            if self._done(pos):
                break                     # finalizing: _cancel_protection takes the rest
            d["stale_orders"] = [s for s in d["stale_orders"] if s["id"] != so["id"]]
            await self._retire_old_sl(pos, so["id"], float(so.get("qty") or 0.0))
        if not d.get("stale_orders"):
            pos.error = _drop_error(pos.error, _STALE_MARK)

    async def _check_old_sl_filled(self, pos: BrokerPosition, old_id: str, old_qty: float) -> OrderState | None:
        """A replaced SL could not be cancelled: read it and book its fill if it filled.
        Returns its state (status 'missing' if the venue does not know it), None if unreadable."""
        try:
            state = await self._order_state(old_id, pos.broker_symbol, "sl", pos.data.get("opened_at"))
        except cx.OrderNotFound:
            return OrderState("missing")
        except cx.BaseError as e:
            logger.warning("%s: replaced stop %s unreadable: %s", self.name, old_id, e)
            return None
        booked = any(str(e.get("order_id")) == str(old_id) for e in pos.data.get("exits") or [])
        if state.status != "filled" or booked or self._done(pos) or pos.remaining_quantity <= 0:
            return state
        executed = min(state.filled or old_qty or pos.remaining_quantity, pos.remaining_quantity)
        self._record_exit(pos, old_id, executed, state.average, "sl")
        if pos.remaining_quantity <= 0:
            await self._finalize_closed(pos, "stop_replaced")   # also cancels the new SL
        else:
            await self._resize_sl(pos)
        return state

    async def _check_vanished_tp(self, pos: BrokerPosition, tp: dict[str, Any]) -> float:
        """A TP we wanted to cancel is gone. Returns the size it executed (0 if none / unknown)."""
        try:
            state = await self._order_state(tp["id"], pos.broker_symbol, "tp", pos.data.get("opened_at"))
        except cx.BaseError:
            return 0.0
        if state.status == "filled":
            tp["status"] = "filled"
            executed = min(state.filled or float(tp["qty"]), float(tp["qty"]), pos.remaining_quantity)
            tp["filled_qty"] = executed
            self._record_exit(pos, tp["id"], executed, state.average or tp["price"], "tp")
            return executed
        if state.status == "canceled":
            tp["status"] = "canceled"
        return 0.0

    async def _finalize_closed(self, pos: BrokerPosition, reason: str) -> None:
        """The trade is over (remaining 0): stop the entry, cancel what is left, mark CLOSED.

        Stays OPEN with ``closing`` while some order may still be live, and closes any size the
        entry filled meanwhile instead of zeroing it."""
        d = pos.data
        d["closed_reason"] = reason
        entry_ok = await self._stop_entry(pos)
        if pos.remaining_quantity > 0:
            # The entry filled more while the trade was ending: that size is open and must go too.
            requested = pos.remaining_quantity
            sent, executed = await self._emergency_close(pos, requested)
            if pos.remaining_quantity > 0:
                d.update(closing=False, unprotected=True)
                what = ("kapatma emri gönderildi ama sonucu okunamadı" if sent and executed is None
                        else f"kapatılamadı ({_fmt(executed or 0.0)} kapatıldı)")
                pos.error = (f"{_CRITICAL_MARK} {self.name}: işlem kapanırken giriş emri {_fmt(requested)} daha doldu "
                             f"ve {what}; {_fmt(pos.remaining_quantity)} "
                             f"STOPSUZ açık olabilir. {pos.broker_symbol} pozisyonunu borsada hemen kontrol edin.")
                return
        if not (await self._cancel_protection(pos, include_sl=reason != "stop") and entry_ok):
            # Keep it OPEN (size 0) so the next sync retries: a forgotten reduce-only order could
            # later shrink another trade on the same symbol, a live entry could open a new one.
            d["closing"] = True
            pos.error = f"{self.name}: pozisyon kapandı ama kalan emirler iptal edilemedi; tekrar denenecek."
            return
        d["closing"] = False
        d["unprotected"] = False
        pos.status = PositionStatus(d.pop("final_status", None) or PositionStatus.CLOSED.value)
        d["closed_at"] = self._now_ms()
        pos.realized_pnl = await self._realized_pnl(pos)
        logger.info("%s: %s %s closed (%s), pnl=%s", self.name, pos.broker_symbol, pos.side.value, reason, pos.realized_pnl)

    def _note_uncovered_exits(self, pos: BrokerPosition, exit_trades: list[Any]) -> None:
        """Mark the result as incomplete (PNL_INCOMPLETE) when the trades it is summed from hold less than the
        exits booked for the position (the list was cut, is behind, or an exit's order is not among ours)."""
        try:
            covered = sum(num(t.get("amount")) or 0.0 for t in exit_trades)
            booked = sum(num(e.get("qty")) or 0.0 for e in pos.data.get("exits") or [])
            if covered < booked * (1.0 - 1e-6) - 1e-12:
                pos.data[PNL_INCOMPLETE] = True
        except Exception:           # (a note for a text: never in the way of closing the position's record)
            logger.exception("%s: could not compare the trades of %s with its booked exits", self.name, pos.broker_symbol)
            pos.data[PNL_INCOMPLETE] = True

    async def _realized_pnl(self, pos: BrokerPosition) -> float | None:
        d = pos.data
        d.pop(PNL_INCOMPLETE, None)
        cs = float(d.get("contract_size", 1.0) or 1.0)
        direction = 1.0 if pos.side is Side.LONG else -1.0
        entry = pos.avg_entry_price
        ids = {str(x) for x in [d.get("entry_order_id"), d.get("sl_order_id"), *(d.get("other_order_ids") or [])] if x}
        ids |= {str(t["id"]) for t in d.get("tp_order_ids") or [] if t.get("id")}
        ids |= {str(e["order_id"]) for e in d.get("exits") or [] if e.get("order_id")}
        ids |= {str(x) for x in d.get("linked_order_ids") or []}
        ccy = self.cfg.quote_currency.upper()
        try:
            trades = await self.exchange.fetch_my_trades(pos.broker_symbol, int(d.get("opened_at", 0)) - 60_000)
            ours = [t for t in trades or [] if str(t.get("order")) in ids]
            close_side = self._close_side(pos.side)
            exits = [t for t in ours if t.get("side") == close_side]
            entries = [t for t in ours if t.get("side") != close_side]
            if exits:
                if entries:
                    q = sum(num(t.get("amount")) or 0 for t in entries)
                    if q > 0:
                        entry = sum((num(t.get("price")) or 0) * (num(t.get("amount")) or 0) for t in entries) / q
                if entry:
                    gross = sum(((num(t.get("price")) or 0) - entry) * (num(t.get("amount")) or 0) * cs * direction
                                for t in exits)
                    fees = 0.0
                    for t in ours:
                        for fee in (t.get("fees") or ([t["fee"]] if t.get("fee") else [])):
                            if fee and str(fee.get("currency", "")).upper() == ccy:
                                fees += num(fee.get("cost")) or 0.0
                    self._note_uncovered_exits(pos, exits)
                    return gross - fees
        except cx.BaseError as e:
            logger.warning("%s: fetch_my_trades failed, estimating pnl: %s", self.name, e)
        if not entry:
            return None
        if not d.get("exits"):
            return None                     # closed outside our orders (flat on the venue): PnL unknown, not 0
        fee_rate = self._risk().fee_rate
        total = 0.0
        for ex in d.get("exits") or []:
            price = num(ex.get("price"))
            qty = num(ex.get("qty")) or 0.0
            if price is None:
                if qty > 0:
                    d[PNL_INCOMPLETE] = True        # (this exit is not in the estimate)
                continue
            total += (price - entry) * qty * cs * direction - fee_rate * (price + entry) * qty * cs
        return total

    # ------------------------------------------------------------------ management
    async def update_stop_loss(self, pos: BrokerPosition, new_stop_loss: float) -> BrokerPosition:
        self._require_real(pos)
        caller = pos
        pos = pos.model_copy(deep=True)
        self._register(pos)
        d = pos.data
        _, _, tick, _ = self._market_rules(pos)
        price = round_to_tick(new_stop_loss, tick)
        if pos.status is PositionStatus.PENDING and pos.filled_quantity <= 0:
            d["sl_price"] = price            # placed at fill time (an attached SL keeps the old level until then)
            pos.stop_loss = price
            return pos
        if pos.status is not PositionStatus.OPEN:
            raise BrokerError(f"{self.name}: pozisyon açık değil ({pos.status.value}); stop taşınamadı.")
        new_id, err = await self._place_sl(pos, pos.remaining_quantity, price)
        if new_id is None:
            # A stop request whose answer was lost is in ``lost_orders`` (with the client-id sequence): it must not
            # be lost with this error, or a stop of ours could stay on the venue untracked (see _hand_back).
            self._hand_back(caller, pos)
            has_stop = bool(d.get("sl_order_id")) and not d.get("unprotected")
            if _unclear(err):
                old = "eski stop yerinde ve aktif" if has_stop else "pozisyonun çalışan bir stopu YOK"
                raise BrokerError(f"{self.name}: yeni stop ({_fmt(price)}) emrine yanıt alınamadı ({_short(err)}); "
                                  f"{old}. Yeni stop emri borsaya ulaştıysa bot sonraki kontrolde onu bulup iptal "
                                  f"edecek.") from err
            old = "eski stop aktif" if has_stop else "pozisyonun çalışan bir stopu YOK"
            raise BrokerError(f"{self.name}: yeni stop ({_fmt(price)}) yerleştirilemedi, {old}: "
                              f"{_short(err or Exception())}")
        old = d.get("sl_order_id")
        old_qty = float(d.get("protected_qty") or 0.0)
        d.update(sl_order_id=new_id, sl_price=price, protected_qty=pos.remaining_quantity, unprotected=False)
        pos.stop_loss = price
        pos.error = _drop_error(pos.error, _CRITICAL_MARK)
        await self._retire_old_sl(pos, old, old_qty)
        return pos

    async def update_take_profits(self, pos: BrokerPosition, take_profits: list[TakeProfit]) -> BrokerPosition:
        self._require_real(pos)
        pos = pos.model_copy(deep=True)
        self._register(pos)
        d = pos.data
        _, _, tick, _ = self._market_rules(pos)
        levels = [TakeProfit(price=round_to_tick(tp.price, tick), close_fraction=tp.close_fraction) for tp in take_profits]
        pos.take_profits = levels
        if pos.status is PositionStatus.PENDING and pos.filled_quantity <= 0:
            d["tp_levels"] = [lv.model_dump() for lv in levels]
            d["tp_order_ids"] = []
            return pos
        if pos.status is not PositionStatus.OPEN:
            raise BrokerError(f"{self.name}: pozisyon açık değil ({pos.status.value}); kâr al güncellenemedi.")
        # A level that already filled is done: re-placing it would close another slice of the
        # remaining size there (the trader's message usually still lists it).
        filled = [float(t["price"]) for t in d.get("tp_order_ids") or [] if t.get("status") == "filled"]
        tol = tick / 2 if tick > 0 else 1e-9
        dropped = [lv.price for lv in levels if any(abs(lv.price - p) <= tol for p in filled)]
        if dropped:
            levels = [lv for lv in levels if lv.price not in dropped]
            if not levels:
                raise BrokerError(
                    f"{self.name}: yeni kâr al seviyeleri ({', '.join(_fmt(p) for p in dropped)}) zaten gerçekleşti; "
                    f"kalan miktar için yeni seviye yok, kâr al değiştirilmedi."
                )
            d["warnings"].append(f"Zaten gerçekleşmiş TP seviyeleri yeniden kurulmadı: {', '.join(_fmt(p) for p in dropped)}")
        d["tp_levels"] = [lv.model_dump() for lv in levels]
        before = pos.remaining_quantity
        await self._replace_take_profits(pos, levels, pos.remaining_quantity)
        if pos.remaining_quantity <= 0:
            await self._finalize_closed(pos, "take_profit")
        elif pos.remaining_quantity != before:
            await self._resize_sl(pos)
        return pos

    async def close_position(self, pos: BrokerPosition, fraction: float = 1.0) -> BrokerPosition:
        """See Broker.close_position. A close request without a clear answer (see _unclear) is never sent again by
        the adapter: it is looked up by its client id (_find_by_client_id) and booked like any close when it exists
        (size, P/L, stop and targets re-sized). When it is not found, or cannot be looked for, it is kept in
        ``unconfirmed_closes`` by its client id and OrderOutcomeUnknownError is raised ("gerçekleşip gerçekleşmediği
        bilinmiyor"); the next syncs look it up first and book it once it shows up (_settle_unconfirmed). That
        record and the advanced ``close_seq`` (so no later close reuses the lost order's client id) are handed back
        into ``pos`` itself before the error is raised (see _hand_back): the caller stores it when it books the
        failed close."""
        self._require_real(pos)
        work = pos.model_copy(deep=True)
        try:
            return await self._close_position(work, fraction)
        except OrderOutcomeUnknownError:
            self._hand_back(pos, work)
            raise

    async def _close_position(self, pos: BrokerPosition, fraction: float) -> BrokerPosition:
        """close_position on the adapter's own copy ``pos``."""
        self._register(pos)
        d = pos.data
        if pos.status is PositionStatus.PENDING and pos.filled_quantity <= 0:
            return await self.cancel_entry(pos)
        if pos.status is not PositionStatus.OPEN:
            return pos
        if d.get("closing"):                 # already flat; only leftover orders remain
            await self._finalize_closed(pos, d.get("closed_reason") or "manual_close")
            return pos
        step, min_qty, _, _ = self._market_rules(pos)
        if d.get("unconfirmed_closes"):      # an earlier exit's result first (it may have closed it)
            before = pos.remaining_quantity
            await self._settle_unconfirmed(pos, count_misses=False)
            if self._done(pos):
                return pos
            if fraction < 0.999 and before - pos.remaining_quantity > step / 2:
                # An earlier close of ours turned out to have gone through just now. The fraction asked for was
                # worked out from the size before it: closing it now could close the same part twice. Nothing new
                # is sent; the caller sees the size that is left.
                pos.error = _join_error(pos.error, f"{self.name}: önceki kapatma emri borsada gerçekleşmiş "
                                                   f"({_fmt(_dec_sub(before, pos.remaining_quantity))} kapandı, kalan "
                                                   f"{_fmt(pos.remaining_quantity)}); yeni kapatma emri gönderilmedi.")
                return pos
        full = fraction >= 0.999
        if full:
            await self._stop_entry(pos)      # if it may still be live, _finalize_closed retries it
        target = min(pos.remaining_quantity, pos.filled_quantity * max(0.0, fraction))
        qty = pos.remaining_quantity if full else floor_to_step(target, step)
        if not full and pos.remaining_quantity - qty < max(min_qty, step):
            full, qty = True, pos.remaining_quantity
        if qty <= 0 or (not full and qty < min_qty):
            raise BrokerError(f"{self.name}: kapatılacak miktar ({_fmt(qty)}) minimum emir miktarının altında.")
        d["close_seq"] = int(d.get("close_seq", 0)) + 1
        cid = self._client_id(d.get("client_tag", "ct"), f"c{d['close_seq']}")
        order = await self._send_close(pos, qty, cid)
        executed = await self._book_exit_order(pos, order, qty, "close")
        if executed is None:
            pos.error = _join_error(pos.error, f"{self.name}: kapatma emri gönderildi ama {_UNCONFIRMED_MARK}; stop "
                                               f"yerinde bırakıldı, sonraki kontrolde doğrulanacak.")
            return pos
        if pos.remaining_quantity <= 0:
            await self._finalize_closed(pos, "manual_close")
            return pos
        # Only what really executed is closed (the venue may end a market order early); the rest
        # stays OPEN with its stop re-sized to it.
        if executed > 0:
            await self._resize_sl(pos)
            levels = [TakeProfit(**lv) for lv in self._unfilled_levels(pos)]
            if levels and not self._done(pos):
                await self._replace_take_profits(pos, levels, pos.remaining_quantity)
        if _dec_sub(qty, executed) > step / 2:
            left = _fmt(pos.remaining_quantity)
            what = (f"kısmen gerçekleşti ({_fmt(executed)}/{_fmt(qty)}); {left} hâlâ açık, stop kalan miktarı koruyor"
                    if executed > 0 else f"gerçekleşmedi (borsa emri iptal etti); {left} hâlâ açık, stop yerinde")
            pos.error = _join_error(pos.error, f"{self.name}: kapatma emri {what}. Kapatmayı tekrar deneyin veya "
                                               f"borsayı kontrol edin.")
        return pos

    async def _send_close(self, pos: BrokerPosition, qty: float, cid: str) -> dict[str, Any]:
        """Send the reduce-only market close of ``qty`` tagged ``cid`` and return the order. A clear refusal raises
        BrokerError ("reddedildi"). A request without a clear answer is looked up by its client id: the order is
        returned when it exists; otherwise it is kept in ``unconfirmed_closes`` and OrderOutcomeUnknownError raised."""
        sent_ms = self._now_ms()
        try:
            return await self.exchange.create_order(pos.broker_symbol, "market", self._close_side(pos.side), qty, None,
                                                    self._close_params(cid))
        except cx.InsufficientFunds as e:
            raise BrokerError(f"{self.name}: kapatma emri reddedildi: yetersiz bakiye ({_short(e)})") from e
        except cx.BaseError as e:
            if not _unclear(e):
                raise BrokerError(f"{self.name}: kapatma emri reddedildi: {_short(e)}") from e
            found, looked = await self._find_by_client_id(pos.broker_symbol, cid, sent_ms - 60_000)
            if found is not None:
                logger.warning("%s: close request failed (%s) but order %s (client id %s) exists; booking it",
                               self.name, e, found.get("id"), cid)
                return found
            where = (f"bu etiketle ({cid}) borsada henüz görünmüyor" if looked
                     else f"borsaya ulaşılamadığı için bu etiketle ({cid}) aranamadı")
            pos.data.setdefault("unconfirmed_closes", []).append(
                {"order_id": None, "client_id": cid, "qty": qty, "kind": "close", "since": sent_ms, "misses": 0})
            pos.error = _join_error(pos.error, f"{self.name}: {_fmt(qty)} kapatma emri gönderildi ama "
                                               f"{_UNCONFIRMED_MARK} ({where}); stop yerinde bırakıldı, sonraki "
                                               f"kontrollerde aranacak.")
            logger.error("%s: close %s of %s on %s sent, its answer lost (%s) and not found (looked: %s)",
                         self.name, cid, _fmt(qty), pos.broker_symbol, e, looked)
            raise OrderOutcomeUnknownError(
                f"{self.name}: kapatma emri ({_fmt(qty)} {pos.broker_symbol}) gönderildi ama yanıtı alınamadı "
                f"({_short(e)}); emir {OUTCOME_UNKNOWN_MARK} {where}, gerçekleşip gerçekleşmediği bilinmiyor. Bot "
                f"sonraki kontrollerde bu emri borsada arayacak ve bulursa kayda geçirecek."
            ) from e

    @staticmethod
    def _hand_back(caller: BrokerPosition, pos: BrokerPosition) -> None:
        """Before an error is raised after an order request went out without a clear answer: the caller's own
        position object takes this call's state (``pos``, the adapter's working copy). Every caller of the engine and
        the monitor stores the object it passed in when it books the failed change, so the record of such an order
        (``unconfirmed_closes`` / ``lost_orders``) and the advanced client-id sequence are not lost with the error:
        the next sync looks the order up and books (or cancels) it, and no later order reuses its client id. Other
        failures leave the caller's object as it was."""
        state = pos.model_copy(deep=True)
        for name in type(pos).model_fields:
            setattr(caller, name, getattr(state, name))

    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        self._require_real(pos)
        work = pos.model_copy(deep=True)
        try:
            return await self._cancel_entry(work)
        except OrderOutcomeUnknownError:
            self._hand_back(pos, work)      # (the close of a fill found now went out without a clear answer)
            raise

    async def _cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        """cancel_entry on the adapter's own copy ``pos``."""
        self._register(pos)
        d = pos.data
        if pos.status not in (PositionStatus.PENDING, PositionStatus.OPEN):
            return pos
        if not d.get("entry_done"):
            await self._cancel(d.get("entry_order_id"), pos.broker_symbol, "entry")
            await self._refresh_entry_fill(pos)
            d["entry_done"] = True
        if pos.filled_quantity <= 0:
            pos.status = PositionStatus.CANCELLED
            pos.remaining_quantity = 0.0
            return pos
        if pos.status is PositionStatus.PENDING:
            # Filled between our last sync and the cancel: it was never protected by our own orders.
            pos.status = PositionStatus.OPEN
        return await self.close_position(pos, 1.0)

    async def cancel_entry_remainder(self, pos: BrokerPosition) -> BrokerPosition:
        """Cancel only the unfilled rest of a partly filled limit entry (pending-entry expiry).
        The filled size stays open; fills that arrived before the cancel get their stop now."""
        self._require_real(pos)
        pos = pos.model_copy(deep=True)
        self._register(pos)
        d = pos.data
        if pos.status is not PositionStatus.OPEN or d.get("entry_done") or d.get("closing"):
            return pos
        prev = pos.filled_quantity
        await self._cancel(d.get("entry_order_id"), pos.broker_symbol, "entry")   # BrokerError if it fails
        state = await self._refresh_entry_fill(pos)
        if state is not None and state.status != "open":
            d["entry_done"] = True
        if pos.filled_quantity > prev:
            await self._protect_new_fill(pos, emergency_status=PositionStatus.CLOSED)
        return pos

    async def _refresh_entry_fill(self, pos: BrokerPosition) -> OrderState | None:
        """Re-read the entry order and apply any NEW fills to filled/remaining/avg."""
        d = pos.data
        try:
            state = await self._order_state(str(d["entry_order_id"]), pos.broker_symbol, "entry")
        except cx.BaseError as e:
            logger.warning("%s: entry state unknown: %s", self.name, e)
            if isinstance(e, cx.InvalidNonce):
                await self._sync_clock(force=True)      # -1021: our offset to the exchange clock is stale
            return None
        actual = state.filled or 0.0
        if state.status == "filled" and not state.filled:
            actual = state.amount or pos.quantity
        state.filled = actual
        if actual > pos.filled_quantity and not d.get("fill_assumed"):
            added = float(Decimal(str(actual)) - Decimal(str(pos.filled_quantity)))
            pos.filled_quantity = actual
            pos.remaining_quantity = float(Decimal(str(pos.remaining_quantity)) + Decimal(str(added)))
            pos.avg_entry_price = state.average or pos.avg_entry_price or d.get("entry_price")
        return state

    # ------------------------------------------------------------------ sync
    async def sync_position(self, pos: BrokerPosition) -> BrokerPosition:
        if pos.status not in (PositionStatus.PENDING, PositionStatus.OPEN):
            return pos
        if pos.dry_run or pos.data.get("simulated"):
            logger.warning("%s: simulated position %s not synced on the real account", self.name, pos.broker_symbol)
            return pos
        before = pos
        pos = pos.model_copy(deep=True)
        self._register(pos)
        try:
            if not pos.data.get("entry_done") or pos.data.get("fill_assumed"):
                await self._sync_entry(pos)
            if pos.status is PositionStatus.OPEN:
                await self._sync_open(pos)
        except cx.OperationFailed as e:
            # No clear answer (network, timeout, "server busy / internal error" = binance -1000/-1001/-1008...):
            # transient. The copy is returned, so orders placed and fills booked earlier in this pass are kept.
            logger.warning("%s: sync of %s interrupted by a transient error: %s", self.name, pos.broker_symbol, e)
            if isinstance(e, cx.InvalidNonce):
                await self._sync_clock(force=True)      # -1021: our offset to the exchange clock is stale
            return pos
        except Exception as e:
            if pos.model_dump() == before.model_dump():
                if isinstance(e, cx.BaseError):
                    raise BrokerError(f"{self.name}: {_SYNC_MARK}: {_short(e)}") from e
                raise
            # Something was placed or booked before the error. It must reach the database, or the next pass
            # would do it a second time (a second stop, a fill booked twice): keep the new state and report
            # the error through pos.error instead of raising.
            logger.error("%s: sync of %s failed after changes were made (kept): %r", self.name, pos.broker_symbol, e)
            text = _short(e).removeprefix(f"{self.name}: ")
            pos.error = _join_error(_drop_error(pos.error, _SYNC_MARK), f"{self.name}: {_SYNC_MARK}: {text}")
            return pos
        if pos.error and _SYNC_MARK in pos.error:
            pos.error = _drop_error(pos.error, _SYNC_MARK)
        return pos

    async def _sync_entry(self, pos: BrokerPosition) -> None:
        d = pos.data
        prev = pos.filled_quantity
        state = await self._refresh_entry_fill(pos)
        if state is None:
            return
        if d.get("fill_assumed"):
            if state.status == "open":
                return                     # still unknown: the assumed (full) size stays protected
            d["fill_assumed"] = False
            d["entry_done"] = True
            actual = state.filled or 0.0
            pos.avg_entry_price = state.average or pos.avg_entry_price
            if abs(actual - prev) <= 1e-12:
                return
            pos.filled_quantity = actual
            pos.remaining_quantity = max(0.0, float(Decimal(str(actual)) - Decimal(str(d.get("closed_qty", 0.0)))))
            if actual <= 0:
                pos.error = f"{self.name}: piyasa emri gerçekleşmedi; koruma emirleri iptal edildi."
                if await self._cancel_protection(pos):
                    pos.status = PositionStatus.FAILED
                else:   # keep syncing until the leftover reduce-only orders are gone
                    d.update(closing=True, closed_reason="no_fill", final_status=PositionStatus.FAILED.value)
                return
            await self._resize_sl(pos)
            await self._replace_take_profits(pos, [TakeProfit(**lv) for lv in self._unfilled_levels(pos)],
                                             pos.remaining_quantity)
            return
        if state.status != "open":
            d["entry_done"] = True
        if pos.filled_quantity > prev:
            if prev <= 0:
                d["opened_at"] = self._now_ms()
            pos.status = PositionStatus.OPEN
            if d.get("closing"):
                return                  # the trade is ending: _finalize_closed closes this new size
            await self._protect_new_fill(pos, emergency_status=PositionStatus.FAILED)
        elif pos.filled_quantity <= 0 and state.status == "canceled":
            pos.status = PositionStatus.CANCELLED
            pos.error = pos.error or f"{self.name}: limit giriş emri borsada iptal edildi / süresi doldu."

    async def _try_state(self, order_id: str, symbol: str, kind: str, since: int | None) -> OrderState | None:
        """Order state, OrderState('missing') if the venue does not know it, None on network trouble."""
        try:
            return await self._order_state(order_id, symbol, kind, since)
        except cx.OrderNotFound:
            return OrderState("missing")
        except cx.OperationFailed as e:     # network trouble, or the venue is busy (binance -1000/-1001/-1008)
            logger.warning("%s: could not read order %s: %s", self.name, order_id, e)
            if isinstance(e, cx.InvalidNonce):
                await self._sync_clock(force=True)      # -1021: our offset to the exchange clock is stale
            return None

    async def _settle_unconfirmed(self, pos: BrokerPosition, *, count_misses: bool = True) -> None:
        """Book exit orders whose result could not be read when they were sent. One whose request got no answer at
        all is kept by its client id (``order_id`` None): it is looked up by that id and, once found, settled like
        the others. When the venue has answered on _LOST_ORDER_MISSES syncs (``count_misses``: this call is a sync;
        a sync whose look-up got no answer does not count) that it has no order with that id, and at least
        _LOST_ORDER_MIN_AGE_MS have passed since it was sent, the request never reached it: nothing was closed by it
        (its client id is remembered in ``dropped_closes``, see _flat_note). One found and booked by a sync leaves a
        note for the owner in ``pos.error``."""
        d, symbol = pos.data, pos.broker_symbol
        keep: list[dict[str, Any]] = []
        kinds: set[str] = set()
        found_late: list[tuple[str, float]] = []      # (client id, size closed): lost closes a sync found executed
        for u in list(d.get("unconfirmed_closes") or []):
            late = False
            if not u.get("order_id"):
                since = int(u.get("since") or d.get("opened_at") or 0) - 60_000
                found, looked = await self._find_by_client_id(symbol, str(u.get("client_id")), since, attempts=1)
                if found is None:
                    if looked and count_misses:
                        u = {**u, "misses": int(u.get("misses") or 0) + 1}
                    if (int(u.get("misses") or 0) >= _LOST_ORDER_MISSES
                            and self._now_ms() - int(u.get("since") or 0) >= _LOST_ORDER_MIN_AGE_MS):
                        logger.warning("%s: exit order %s (client id) never reached the venue; nothing closed by it",
                                       self.name, u.get("client_id"))
                        d["dropped_closes"] = [*(d.get("dropped_closes") or []), str(u.get("client_id"))][-5:]
                        continue
                    keep.append(u)
                    continue
                u = {**u, "order_id": str(found.get("id"))}
                self._remember(d, u["order_id"])
                logger.warning("%s: exit order %s (client id %s) whose answer was lost exists; settling it", self.name,
                               u["order_id"], u.get("client_id"))
                st: OrderState | None = parse_order_state(found, trigger=False)
                late = count_misses
            else:
                st = await self._try_state(str(u["order_id"]), symbol, "close", d.get("opened_at"))
            if st is None:
                keep.append(u)
                continue
            if st.status == "open":
                await self._cancel_quiet(str(u["order_id"]), symbol, "close")   # make its size final
                keep.append(u)
                continue
            if st.status == "missing":
                logger.warning("%s: exit order %s unknown to the venue; assuming nothing executed",
                               self.name, u["order_id"])
                continue
            executed = (st.filled or float(u["qty"])) if st.status == "filled" else (st.filled or 0.0)
            executed = min(executed, float(u["qty"]), pos.remaining_quantity)
            if executed > 0:
                self._record_exit(pos, str(u["order_id"]), executed, st.average, u.get("kind") or "close")
                kinds.add(u.get("kind") or "close")
                if late:
                    found_late.append((str(u.get("client_id")), executed))
        d["unconfirmed_closes"] = keep
        if not keep:
            pos.error = _drop_error(pos.error, _UNCONFIRMED_MARK)
        if not kinds:
            return
        if pos.remaining_quantity <= 0:
            emergency = "emergency" in kinds
            await self._finalize_closed(pos, "emergency" if emergency else "manual_close")
            if emergency and pos.status is PositionStatus.CLOSED:
                pos.error = f"{self.name}: stop emri yerleştirilemediği için pozisyon piyasa fiyatından kapatıldı."
        elif not d.get("unprotected") and d.get("sl_order_id"):
            await self._resize_sl(pos)
            levels = [TakeProfit(**lv) for lv in self._unfilled_levels(pos)]
            if levels and not self._done(pos):
                await self._replace_take_profits(pos, levels, pos.remaining_quantity)
        for cid, executed in found_late:
            # Booked by this sync, not by a call that reports it: the owner is told here (once: the monitor sends
            # a changed pos.error; the note is not written again by a later sync).
            pos.error = _join_error(pos.error, f"{self.name}: yanıtı alınamayan önceki kapatma emri ({cid}) borsada "
                                               f"gerçekleşmiş bulundu: {_fmt(executed)} kapandı, kalan "
                                               f"{_fmt(pos.remaining_quantity)}.")

    async def _sync_open(self, pos: BrokerPosition) -> None:
        d, symbol = pos.data, pos.broker_symbol
        since = d.get("opened_at")
        if d.get("unconfirmed_closes") and not d.get("closing"):
            await self._settle_unconfirmed(pos)
            if self._done(pos):
                return
        if d.get("stale_orders") and not d.get("closing"):
            await self._retry_stale_orders(pos)
            if self._done(pos):
                return
        if d.get("lost_orders") and not d.get("closing"):
            had_stop = bool(d.get("sl_order_id")) and not d.get("unprotected")
            await self._resolve_lost_orders(pos)
            if not had_stop and d.get("sl_order_id") and not d.get("unprotected") and not d.get("tp_order_ids"):
                # A stop that had arrived after all was adopted: the take profits that follow a stop are missing.
                await self._replace_take_profits(pos, [TakeProfit(**lv) for lv in self._unfilled_levels(pos)],
                                                 pos.remaining_quantity)
        if d.get("closing"):
            await self._finalize_closed(pos, d.get("closed_reason") or "closing")
            if pos.status is PositionStatus.CLOSED and d.get("closed_reason") != "emergency":
                pos.error = self._flat_note(pos) if d.get("closed_outside") else None
            return

        if d.get("unprotected") or not d.get("sl_order_id"):
            if await self._gone_on_venue(pos):
                return
            await self._protect_new_fill(pos, emergency_status=PositionStatus.CLOSED)
            if self._done(pos) or d.get("unprotected"):
                return

        sl = await self._try_state(str(d["sl_order_id"]), symbol, "sl", since)
        if sl is None:
            return
        unsure = False
        if sl.status == "missing":
            live = await self._is_live_trigger(symbol, str(d["sl_order_id"]))
            if live is not False:
                # An "order does not exist" answer is not proof (binance has returned it for live conditional
                # orders): the stop is still listed as open, or the list could not be read. Nothing is re-placed;
                # it is asked again next pass, and if nothing can be read for several passes the owner is told.
                # The rest of the pass still runs (take profits, position gone on the venue): a stop that stays
                # unreadable by its id must not freeze the trade.
                logger.warning("%s: stop %s on %s not found by id but not confirmed gone; re-checking", self.name,
                               d["sl_order_id"], symbol)
                unsure = True
                d["sl_unsure"] = int(d.get("sl_unsure", 0)) + 1
                if live is None and d["sl_unsure"] >= 3:
                    pos.error = _join_error(pos.error, f"{self.name}: stop emri ({d['sl_order_id']}) borsada "
                                                       f"{_UNSURE_MARK}: emir numarasıyla bulunamadı ve açık "
                                                       f"emirler okunamıyor. {symbol} stopunu borsada kontrol edin.")
                sl = OrderState("open")
        if not unsure and d.pop("sl_unsure", None):
            pos.error = _drop_error(pos.error, _UNSURE_MARK)
        if sl.status == "filled":
            sl_id = str(d["sl_order_id"])
            d.setdefault("linked_order_ids", []).extend(sl.linked_ids)
            # Book what the stop really executed: a triggered market order can end early.
            executed = min(sl.filled or pos.remaining_quantity, pos.remaining_quantity)
            self._record_exit(pos, sl_id, executed, sl.average or pos.stop_loss, "sl")
            self._remember(d, sl_id)
            d.update(sl_order_id=None, protected_qty=0.0)     # consumed: never cancel / re-book it
            if pos.remaining_quantity <= 0:
                await self._finalize_closed(pos, "stop")
                return
            left = pos.remaining_quantity
            logger.error("%s: SL %s on %s filled only %s; %s still open", self.name, sl_id, symbol, executed, left)
            await self._protect_new_fill(pos, emergency_status=PositionStatus.CLOSED)
            if not self._done(pos) and not d.get("unprotected"):
                pos.error = _join_error(pos.error, f"{self.name}: stop emri kısmen gerçekleşti ({_fmt(executed)}); "
                                                   f"kalan {_fmt(left)} için yeni stop konuldu.")
            return
        if sl.status in ("canceled", "missing"):
            if await self._gone_on_venue(pos):
                return                      # nothing left to protect: closed after two flat reads
            logger.error("%s: our SL %s on %s is %s; re-placing", self.name, d["sl_order_id"], symbol, sl.status)
            # The old id stays in place: _protect_new_fill places the new stop first and then cancels the old one
            # once more (should the exchange have hidden a live order, it must not stay behind as a second stop).
            await self._protect_new_fill(pos, emergency_status=PositionStatus.CLOSED)
            if self._done(pos) or d.get("unprotected"):
                return

        changed = False
        step, min_qty, _, _ = self._market_rules(pos)
        for tp in d.get("tp_order_ids") or []:      # entries appended below (re-armed rest) are visited too
            if tp.get("status") == "open" and tp.get("id"):
                st = await self._try_state(str(tp["id"]), symbol, "tp", since)
                if st is not None and st.status == "filled":
                    tp["status"] = "filled"
                    d.setdefault("linked_order_ids", []).extend(st.linked_ids)
                    planned = float(tp["qty"])
                    executed = min(st.filled or planned, planned, pos.remaining_quantity)
                    tp["filled_qty"] = executed
                    self._record_exit(pos, tp["id"], executed, st.average or tp["price"], "tp")
                    changed = True
                    rest = min(_dec_sub(planned, executed), pos.remaining_quantity)
                    if rest > step / 2:
                        self._rearm_tp_rest(pos, tp, executed, rest, min_qty)
                elif st is not None and st.status == "canceled":
                    logger.warning("%s: TP %s vanished; re-placing", self.name, tp["id"])
                    self._remember(d, tp["id"])
                    tp.update(status="failed", id=None, attempts=0)
            retry = tp.get("status") == "failed" and int(tp.get("attempts", 0)) < _TP_PLACE_ATTEMPTS and tp.get("qty")
            if retry and await self._place_tp(pos, tp) is None and pos.error and "kâr al" in pos.error:
                pos.error = _drop_error(pos.error, "kâr al")

        tp_orders = d.get("tp_order_ids") or []
        all_tp_filled = (bool(tp_orders) and all(t.get("status") in ("filled", "skipped") for t in tp_orders)
                         and any(t.get("status") == "filled" for t in tp_orders))
        if pos.remaining_quantity <= 0:
            await self._finalize_closed(pos, "take_profit")
            return
        if all_tp_filled:
            # Every TP order is done but size is left (fills ended early): never cancel its stop.
            pos.error = _join_error(pos.error, f"{self.name}: tüm kâr al emirleri gerçekleşti ama "
                                               f"{_fmt(pos.remaining_quantity)} açık kaldı; yalnızca stop ile "
                                               f"korunuyor, borsayı kontrol edin.")
        if changed:
            await self._resize_sl(pos)
            if self._done(pos):
                return

        await self._gone_on_venue(pos)

    async def _gone_on_venue(self, pos: BrokerPosition) -> bool:
        """True when the venue holds nothing in our direction (the caller must not place protection for it).
        After two consecutive flat reads (position APIs can lag behind fills) the trade is closed.
        ``pos.data[FLAT_SEEN]`` counts those flat reads (0 after a read that shows the position): part of this
        adapter's contract, read by the monitor before it retries a close (see FLAT_SEEN)."""
        d = pos.data
        size = await self._venue_position_size(pos.broker_symbol, pos.side)
        step = self._market_rules(pos)[0]
        if size is None or size >= step / 2:
            d[FLAT_SEEN] = 0
            return False
        d[FLAT_SEEN] = int(d.get(FLAT_SEEN, 0)) + 1
        if d[FLAT_SEEN] >= 2:
            outside = pos.remaining_quantity  # gone on the venue (closed there by hand, liquidated, netted...)
            pos.remaining_quantity = 0.0
            d["unprotected"] = False
            if outside > 0:
                d["closed_outside"] = outside
            await self._finalize_closed(pos, "flat_on_exchange")
            pos.error = self._flat_note(pos) if not pos.error or _CRITICAL_MARK in pos.error else pos.error
        return True

    def _flat_note(self, pos: BrokerPosition) -> str:
        d = pos.data
        note = f"{self.name}: pozisyon borsada kapalı bulundu; kalan emirler iptal edildi."
        outside = float(d.get("closed_outside") or 0.0)
        dropped = [str(c) for c in d.get("dropped_closes") or []]
        if outside > 0 and dropped:
            # A close of ours whose answer was lost and that was not found (so given up) may have executed late.
            note += (f" {_fmt(outside)} botun kayda geçirdiği emirler dışında kapandı; bu, yanıtı alınamayan ve "
                     f"borsada bulunamayan önceki kapatma emrinin ({', '.join(dropped)}) sonradan gerçekleşmesi de "
                     f"olabilir. " + ("Kâr/zarar bilinmiyor." if pos.realized_pnl is None else
                                      "Kâr/zarar yalnızca botun kayda geçirdiği emirleri içerir."))
        elif outside > 0:                     # no order of ours closed that size: its result is not known to us
            note += (" Kapanış botun emirleriyle olmadığı için kâr/zarar bilinmiyor." if pos.realized_pnl is None else
                     f" {_fmt(outside)} botun emirleri dışında kapandı; kâr/zarar yalnızca botun kendi "
                     f"emirlerini içerir.")
        return note

    async def _is_live_trigger(self, symbol: str, order_id: str) -> bool | None:
        """Is our conditional order in the venue's list of open conditional orders? None = could not tell."""
        if self._family != "binance":
            return False                    # other venues: keep the old behaviour (treat as gone)
        try:
            orders = await self.exchange.fetch_open_orders(symbol, None, None, {"trigger": True})
        except cx.BaseError as e:
            logger.warning("%s: open conditional orders unreadable: %s", self.name, e)
            return None
        return any(str(o.get("id")) == str(order_id) for o in orders or [])

    # ------------------------------------------------------------------ health / self-test (read-only)
    async def health_problem(self) -> str | None:
        """None when one cheap signed read (the balance) answers within ``_HEALTH_TIMEOUT_SEC`` seconds, else a
        short Turkish reason (API key refused, IP not allowed, clock difference, no connection...). Read-only;
        never raises (a real cancellation of the calling task still passes through)."""
        if self.exchange is None:
            return "borsa bağlantısı kurulmamış"
        try:
            return await asyncio.wait_for(self._health_read(), _HEALTH_TIMEOUT_SEC)
        except asyncio.TimeoutError:
            return f"borsa {int(_HEALTH_TIMEOUT_SEC)} saniye içinde yanıt vermedi (bağlantı yok ya da çok yavaş)"
        except Exception as e:
            return f"beklenmeyen hata: {_short(e, 120)}"

    async def _health_read(self) -> str | None:
        await self._sync_clock()            # keeps the clock offset fresh while the bot runs (only when it is old)
        for attempt in (0, 1):
            try:
                await self.exchange.fetch_balance()
                return None
            except cx.InvalidNonce as e:    # -1021: measure the offset again and ask once more
                if attempt or not await self._sync_clock(force=True):
                    return self._health_text(e)
            except cx.BaseError as e:
                return self._health_text(e)
        return None

    @staticmethod
    def _health_text(e: Exception) -> str:
        """Plain Turkish reason for a failed signed read (binance codes where they are known)."""
        code = _venue_code(e)
        if isinstance(e, cx.InvalidNonce) or code == "-1021":
            return "bu bilgisayarın saati borsa saatiyle uyuşmuyor (Windows saat eşitlemesini kontrol edin)"
        if code in ("-1005", "-1011"):
            return "bu bilgisayarın IP adresi API anahtarında izinli değil"
        if code == "-2015":     # ccxt reports it as a rate limit once a signed call has worked; the code decides
            return ("API anahtarı reddedildi: anahtar silinmiş / geçersiz, bu bilgisayarın IP adresi izinli değil ya "
                    "da vadeli işlem yetkisi yok")
        if code == "-1022":
            return "API gizli anahtarı (secret) yanlış: imza reddedildi"
        if isinstance(e, cx.AuthenticationError):     # also PermissionDenied / AccountSuspended
            return f"API anahtarı reddedildi (yetki ya da IP kısıtı): {_short(e, 100)}"
        if isinstance(e, (cx.RateLimitExceeded, cx.DDoSProtection)):
            return "borsa çok fazla istek nedeniyle geçici olarak engelledi"
        if isinstance(e, cx.ExchangeNotAvailable):    # also OnMaintenance
            return "borsa bakımda ya da ulaşılamıyor"
        if isinstance(e, cx.RequestTimeout):
            return "borsa zamanında yanıt vermedi (bağlantı çok yavaş)"
        if isinstance(e, cx.NetworkError):
            return f"borsaya bağlanılamıyor (internet / ağ sorunu): {_short(e, 100)}"
        return f"borsa hata verdi: {_short(e, 120)}"

    async def preflight_report(self, bases: list[str]) -> list[str]:
        """Self-test lines for the owner, in plain Turkish, each starting with TAMAM or DİKKAT: the clock, the
        account's position mode and multi-assets mode, for every base the contract with its maximum leverage and
        minimum order, and what is already on the account (positions, orders, conditional orders), so that the
        owner knows about things the bot did not create.

        Strictly read-only: GET requests only, no leverage / margin-mode change, no order. A check that cannot
        be read becomes a DİKKAT line; the method does not raise."""
        ex = self.exchange
        if ex is None:
            return ["DİKKAT Borsa bağlantısı kurulmamış; kontroller yapılamadı."]
        lines: list[str] = []

        async def section(what: str, check: Callable[[], Awaitable[list[str]]]) -> None:
            try:
                lines.extend(await check())
            except Exception as e:      # one unreadable check must not hide the others
                lines.append(f"DİKKAT {what} okunamadı: {_short(e, 140)}")

        has = getattr(ex, "has", {}) or {}
        binance = self._family == "binance"
        # the signed reads below need a fresh clock offset (a GET; nothing changes on the venue)
        await self._sync_clock(force=True)
        if has.get("fetchTime"):
            await section("Saat farkı", self._pf_clock)
        if binance:
            await section("Pozisyon modu", self._pf_position_mode)
            await section("Çoklu varlık modu", self._pf_multi_assets)
        for base in bases:
            await section(f"{base} sözleşme bilgisi", lambda base=base: self._pf_symbol(base))
        if binance and self._leverage_tiers:
            # The bracket table is per symbol; limits of the ACCOUNT only show when a leverage is set (not done
            # here: this report changes nothing). Said once, so that "125x" is not read as a promise.
            lines.append("TAMAM Not: kaldıraç tablosu hesaba özel sınırları (yeni açılmış hesap, kimlik doğrulama, "
                         "ülke) göstermez. Borsa daha azına izin verirse bot ilk işlemde kaldıracı kendisi düşürür; "
                         "bu, işlem raporundaki uyarıda yazar.")
        if has.get("fetchPositions", True):
            await section("Açık pozisyonlar", self._pf_positions)
        if binance:
            await section("Bekleyen emirler", self._pf_orders)
        return lines

    async def _pf_clock(self) -> list[str]:
        ex = self.exchange
        t0 = time.time() * 1000
        server = await ex.fetch_time()
        t1 = time.time() * 1000
        if not server:
            return ["DİKKAT Saat farkı okunamadı: borsa saat bilgisini vermedi."]
        ahead = (t0 + t1) / 2 - float(server)
        where = "ileride" if ahead >= 0 else "geride"
        adjusted = bool((getattr(ex, "options", None) or {}).get("adjustForTimeDifference"))
        if abs(ahead) < 1000:
            return [f"TAMAM Saat farkı: bu bilgisayarın saati borsadan {abs(ahead):.0f} ms {where}"
                    + (" (bot bu farkı kendisi düzeltiyor)." if adjusted else ".")]
        return [f"DİKKAT Saat farkı büyük: bu bilgisayarın saati borsadan {abs(ahead) / 1000:.1f} saniye {where}. "
                + ("Bot farkı kendisi düzeltiyor; yine de Windows saat eşitlemesini açın." if adjusted else
                   "Borsa istekleri reddedebilir; Windows saat eşitlemesini açın.")]

    async def _pf_position_mode(self) -> list[str]:
        mode = await self.exchange.fetch_position_mode()
        hedged = bool((mode or {}).get("hedged"))
        name = "çift yönlü (hedge)" if hedged else "tek yönlü"
        if hedged == self._hedged:
            return [f"TAMAM Pozisyon modu: {name} (ayarla uyumlu)."]
        wanted = "çift yönlü (hedge)" if self._hedged else "tek yönlü"
        return [f"DİKKAT Hesabın pozisyon modu {name}, ayarda ise {wanted} yazıyor: bu hâlde borsa giriş emirlerini "
                f"reddeder. Binance'te Tercihler > Pozisyon Modu'nu {wanted} yapın."]

    async def _pf_multi_assets(self) -> list[str]:
        answer = await self.exchange.fapiPrivateGetMultiAssetsMargin()
        if str((answer or {}).get("multiAssetsMargin")).lower() == "true":
            return ["DİKKAT Çoklu varlık modu açık: bot yalnızca USDT bakiyesine göre hesap yapar ve izole marjin "
                    "kullanılamaz. Binance'te kapatmanız önerilir."]
        return ["TAMAM Çoklu varlık modu kapalı."]

    async def _pf_symbol(self, base: str) -> list[str]:
        sym = await self.resolve_symbol(base)
        if sym is None:
            return [f"DİKKAT {base}: borsada {self.cfg.quote_currency.upper()} vadeli sözleşmesi bulunamadı; "
                    f"bu coinin sinyalleri bu hesapta açılmaz."]
        market = (getattr(self.exchange, "markets", None) or {}).get(sym.broker_symbol) or {}
        quote = sym.quote_asset or self.cfg.quote_currency.upper()
        text = f"{base}: {sym.broker_symbol} sözleşmesi"
        mult = float(sym.extra.get("price_multiplier") or 1.0)
        if mult != 1.0:
            text += f" (fiyatlar {_fmt(mult)} ile çarpılır)"
        text += f"; en küçük emir {_fmt(sym.min_qty)} {market.get('base') or base}"
        if sym.min_notional:
            text += f" ve {_fmt(sym.min_notional)} {quote}"
        problems: list[str] = []
        tiers = await self._tiers(sym.broker_symbol)
        venue_max = int(tiers[0][0]) if tiers else (int(sym.max_leverage) if sym.max_leverage else None)
        learned = self._learned_cap(sym.broker_symbol)
        if venue_max is not None and learned:
            venue_max = min(venue_max, learned)
        if venue_max is None:
            problems.append("en yüksek kaldıraç okunamadı (ilk işlemde deneyerek bulunur)")
        else:
            text += f"; borsanın kaldıraç tablosunda en yüksek {venue_max}x"
            if tiers and tiers[0][1]:
                text += f" ({_fmt(tiers[0][1])} {quote} büyüklüğe kadar)"
            wanted = min(self.cfg.leverage, self.cfg.max_leverage)
            if venue_max < wanted:      # not a fault, but the owner should know why such a coin needs more margin
                text += f", ayardaki {wanted}x yerine bu kullanılır (aynı işlem daha çok teminat ister)"
        types = (market.get("info") or {}).get("orderTypes") if isinstance(market.get("info"), dict) else None
        if types and not {"MARKET", "LIMIT", "STOP_MARKET", "TAKE_PROFIT_MARKET"} <= set(types):
            problems.append("stop / kâr al emir tipleri bu sözleşmede yok")
        if problems:
            return [f"DİKKAT {text}. Sorun: {'; '.join(problems)}."]
        return [f"TAMAM {text}."]

    async def _pf_positions(self) -> list[str]:
        positions = [p for p in await self.exchange.fetch_positions() or [] if abs(num(p.get("contracts")) or 0.0) > 0]
        if not positions:
            return ["TAMAM Hesapta açık pozisyon yok."]
        out = [f"DİKKAT Hesapta {len(positions)} açık pozisyon var (botun açtıkları dahil; tek yönlü modda aynı "
               f"coindeki bot işlemleriyle birleşir):"]
        for p in positions[:10]:
            lev = num(p.get("leverage"))
            out.append(f"DİKKAT   {p.get('symbol')} {str(p.get('side') or '?').upper()} {_fmt(num(p.get('contracts')))}"
                       f", giriş {_fmt(num(p.get('entryPrice')))}"
                       + (f", kaldıraç {_fmt(lev)}x" if lev else "")
                       + (f", {p.get('marginMode')}" if p.get("marginMode") else ""))
        if len(positions) > 10:
            out.append(f"DİKKAT   ... ve {len(positions) - 10} pozisyon daha.")
        return out

    async def _pf_orders(self) -> list[str]:
        ex = self.exchange
        options = ex.options
        key, missing = "warnOnFetchOpenOrdersWithoutSymbol", object()
        old = options.get(key, missing)
        options[key] = False        # a client-side reminder of ccxt (the request is heavier), not an exchange setting
        try:
            plain = await ex.fetch_open_orders()
            conditional = await ex.fetch_open_orders(None, None, None, {"trigger": True})
        finally:
            if old is missing:
                options.pop(key, None)
            else:
                options[key] = old
        if not plain and not conditional:
            return ["TAMAM Hesapta bekleyen emir yok (normal ve koşullu)."]
        out = [f"DİKKAT Hesapta bekleyen emir var: {len(plain)} normal, {len(conditional)} koşullu (stop / kâr al). "
               f"Botun açık işlemlerine ait değillerse borsada iptal edin:"]
        sides = {"buy": "ALIŞ", "sell": "SATIŞ"}
        for o in [*plain, *conditional][:12]:
            price, trigger = num(o.get("price")), num(o.get("triggerPrice"))
            kind = "koşullu emir" if trigger else ("limit emri" if o.get("type") == "limit" else f"{o.get('type')} emri")
            out.append(f"DİKKAT   {o.get('symbol')} {sides.get(str(o.get('side')), '?')} {kind}, miktar "
                       f"{_fmt(num(o.get('amount')))}"
                       + (f", fiyat {_fmt(price)}" if price else "")
                       + (f", tetik fiyatı {_fmt(trigger)}" if trigger else "")
                       + (", yalnızca pozisyon azaltır" if o.get("reduceOnly") else "")
                       + f" (emir no {o.get('id')})")
        if len(plain) + len(conditional) > 12:
            out.append(f"DİKKAT   ... ve {len(plain) + len(conditional) - 12} emir daha.")
        return out

    async def venue_position_size(self, symbol: str, side: Side) -> float | None:
        """Public contract (the monitor's close retry, ``Monitor._shared_venue_doubt``): the position size the venue
        holds on ``symbol`` in ``side``'s direction, in contracts (one-way mode: the signed net, positive = that
        side; hedge mode: that side's leg). None = unknown (the venue cannot tell, or the read failed). One
        ``fetch_positions`` call; nothing is changed."""
        return await self._venue_position_size(symbol, side)

    async def _venue_position_size(self, symbol: str, side: Side) -> float | None:
        """Size on the venue in our direction (one-way: signed net, positive = ours). None = unknown."""
        if not (getattr(self.exchange, "has", {}) or {}).get("fetchPositions", True):
            return None
        try:
            positions = await self.exchange.fetch_positions([symbol])
        except cx.BaseError as e:
            logger.debug("%s: fetch_positions failed: %s", self.name, e)
            return None
        total = 0.0
        for p in positions or []:
            if p.get("symbol") != symbol:
                continue
            contracts = abs(num(p.get("contracts")) or 0.0)
            pside = p.get("side")
            if self._hedged:
                total += contracts if pside == side.value else 0.0
            elif pside in ("long", "short"):
                total += contracts if pside == side.value else -contracts
        return total


def _bybit_coin_figures(info: Any, ccy: str) -> tuple[float | None, float | None]:
    """Bybit balance 'total' is walletBalance (no unrealized PnL); read the coin's equity instead."""
    try:
        accounts = (info or {}).get("result", {}).get("list") or []
    except AttributeError:
        return None, None
    equity = free = None
    for acct in accounts:
        for coin in acct.get("coin") or []:
            if str(coin.get("coin", "")).upper() == ccy:
                equity = num(coin.get("equity"))
                free = num(coin.get("availableToWithdraw"))
        if free is None:
            free = num(acct.get("totalAvailableBalance"))
    return equity, free
