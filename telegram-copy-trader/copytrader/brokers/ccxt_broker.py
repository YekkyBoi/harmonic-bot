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
from .base import Broker, BrokerError

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Contract prefixes used by exchanges for low-priced coins (1000PEPE = 1000 PEPE).
_MULTIPLIER_PREFIXES = (1000, 10000, 100000, 1000000)
_PREFIX_RE = re.compile(r"^(1000000|100000|10000|1000)([A-Z0-9]+)$")
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
_TP_PLACE_ATTEMPTS = 3
# pos.error is a " | "-joined list of notes; these markers find the notes that clear themselves.
_ERR_SEP = " | "
_STALE_MARK = "eski stop emri"
_UNCONFIRMED_MARK = "sonucu okunamadı"
_CRITICAL_MARK = "KRİTİK"


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


def _base_variants(base: str) -> list[tuple[str, float]]:
    """(market base, price multiplier) candidates: PEPE -> 1000PEPE (x1000), 1000PEPE -> PEPE (x0.001)."""
    out = [(base, 1.0)]
    out += [(f"{p}{base}", float(p)) for p in _MULTIPLIER_PREFIXES]
    m = _PREFIX_RE.match(base)
    if m:
        out.append((m.group(2), 1.0 / float(m.group(1))))
    return out


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
        self.fill_poll_attempts = 6
        self.fill_poll_delay = 0.5
        self.retry_delay = 1.0

    # ------------------------------------------------------------------ lifecycle
    def _build_exchange(self) -> Any:
        klass = getattr(ccxt_async, self.cfg.exchange, None)
        if klass is None:
            raise BrokerError(f"{self.name}: bilinmeyen ccxt borsası '{self.cfg.exchange}'.")
        params: dict[str, Any] = {
            "enableRateLimit": True,
            "options": {"defaultType": self.cfg.market_type, **self.cfg.options},
        }
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
        )
        free = await self.get_free_margin()
        leverage, margin = choose_leverage(
            notional=result.notional,
            free_margin=free,
            stop_distance_frac=abs(entry_price - stop_loss) / entry_price,
            preferred=self.cfg.leverage,
            max_leverage=self.cfg.max_leverage,
            venue_max_leverage=symbol.max_leverage,
            max_margin_usage_pct=risk.max_margin_usage_pct,
        )
        return result.model_copy(update={"leverage": float(leverage), "margin_required": margin})

    async def _resolve_leverage(self, req: OpenRequest, qty: float, ref_price: float) -> int:
        frac = abs(ref_price - req.stop_loss) / ref_price
        liq_cap = max(1, math.floor(1 / (frac * 1.5 + 0.005)))
        caps = [liq_cap, max(1, self.cfg.max_leverage)]
        if req.symbol.max_leverage:
            caps.append(max(1, int(req.symbol.max_leverage)))
        if req.leverage:
            lev = math.ceil(req.leverage - 1e-9)
        else:
            notional = qty * req.symbol.contract_size * ref_price
            lev, _ = choose_leverage(
                notional=notional,
                free_margin=await self.get_free_margin(),
                stop_distance_frac=frac,
                preferred=self.cfg.leverage,
                max_leverage=self.cfg.max_leverage,
                venue_max_leverage=req.symbol.max_leverage,
                max_margin_usage_pct=self._risk().max_margin_usage_pct,
            )
        return max(1, min([lev, *caps]))

    async def _prepare_symbol(self, symbol: str, leverage: int, side: Side) -> list[str]:
        """Margin mode (best effort) + leverage (must succeed). Returns warnings."""
        warnings: list[str] = []
        fam, mode = self._family, self.cfg.margin_mode
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
        lev_params: dict[str, Any] = {}
        if fam == "okx":
            lev_params["marginMode"] = mode
            if self._hedged and mode == "isolated":
                lev_params["posSide"] = side.value
        elif fam == "bitget" and self._hedged and mode == "isolated":
            lev_params["holdSide"] = side.value
        try:
            await self.exchange.set_leverage(leverage, symbol, lev_params)
        except cx.NoChange:
            pass
        except cx.BaseError as e:
            if not _ALREADY_SET_RE.search(str(e)):
                raise BrokerError(f"{self.name}: kaldıraç {leverage}x ayarlanamadı: {_short(e)}") from e
        return warnings

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
            # A triggered binance algo order is "closed" even if its market child was rejected.
            try:
                child = parse_order_state(await self.exchange.fetch_order(state.linked_ids[0], symbol, {}), trigger=False)
                if child.status == "canceled" and not child.filled:
                    return OrderState("canceled", 0.0, None, state.amount, state.linked_ids, order)
                if child.status == "filled":
                    state.filled = child.filled or state.filled
                    state.average = child.average or state.average
            except cx.BaseError as e:
                logger.debug("%s: could not fetch triggered child %s: %s", self.name, state.linked_ids[0], e)
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
                return False
            except cx.BaseError as e:
                last = e
        raise BrokerError(f"{self.name}: emir iptal edilemedi ({order_id}): {_short(last or Exception())}")

    async def _find_by_client_id(self, symbol: str, client_id: str, since: int) -> dict[str, Any] | None:
        """Look for an order we may have placed although the request failed (timeout / duplicate)."""
        ex, fam = self.exchange, self._family
        for attempt in range(3):
            if attempt:
                await self._sleep(self.retry_delay)
            try:
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
            except cx.NetworkError as e:
                logger.warning("%s: lookup by client id %s failed: %s", self.name, client_id, e)
        return None

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

        async with self._lock(symbol):
            data["warnings"] += await self._prepare_symbol(symbol, leverage, side)
            entry = await self._create_entry(symbol, req.entry_type, self._order_side(side), qty, limit_price, params, client_id, now)
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

    async def _create_entry(
        self, symbol: str, entry_type: EntryType, side: str, qty: float, price: float | None,
        params: dict[str, Any], client_id: str, since: int,
    ) -> dict[str, Any]:
        try:
            return await self.exchange.create_order(symbol, entry_type.value, side, qty, price, params)
        except (cx.NetworkError, cx.DuplicateOrderId) as e:
            found = await self._find_by_client_id(symbol, client_id, since - 60_000)
            if found:
                logger.warning("%s: entry request failed (%s) but order %s exists; adopting it", self.name, e, found.get("id"))
                return found
            raise BrokerError(
                f"{self.name}: giriş emri gönderilemedi ({_short(e)}). Bu etiketle ({client_id}) emir bulunamadı; "
                f"yine de borsayı kontrol edin."
            ) from e
        except cx.InsufficientFunds as e:
            raise BrokerError(f"{self.name}: giriş emri reddedildi: yetersiz bakiye ({_short(e)})") from e
        except cx.BaseError as e:
            raise BrokerError(f"{self.name}: giriş emri reddedildi: {_short(e)}") from e

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
        last: Exception | None = None
        for attempt in range(2):
            if attempt:
                await self._sleep(self.retry_delay)
            try:
                order = await self.exchange.create_order(
                    pos.broker_symbol, "market", self._close_side(pos.side), qty, None, self._sl_params(price)
                )
                oid = str(order.get("id"))
                self._known_order_ids.add(oid)
                return oid, None
            except cx.BaseError as e:
                last = e
                logger.error("%s: SL order attempt %d failed: %s", self.name, attempt + 1, e)
        return None, last

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
        """Reduce-only market close. Returns (sent, executed size; None = unknown)."""
        d = pos.data
        if qty <= 0:
            return True, 0.0
        d["close_seq"] = int(d.get("close_seq", 0)) + 1
        cid = self._client_id(d.get("client_tag", "ct"), f"x{d['close_seq']}")
        try:
            order = await self.exchange.create_order(
                pos.broker_symbol, "market", self._close_side(pos.side), qty, None, self._close_params(cid)
            )
        except cx.BaseError as e:
            logger.critical("%s: EMERGENCY CLOSE FAILED for %s: %s", self.name, pos.broker_symbol, e)
            return False, None
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
            why = _short(err or Exception())
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
                    pos.error = f"{self.name}: eski kâr al emri iptal edilemedi ({tp['id']}); tekrar denenecek."
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
        try:
            order = await self.exchange.create_order(
                pos.broker_symbol, "market", self._close_side(pos.side), entry["qty"], None, self._tp_params(entry["price"])
            )
        except cx.BaseError as e:
            entry["status"] = "failed"
            logger.error("%s: TP %s order failed: %s", self.name, entry["price"], e)
            return f"TP {_fmt(entry['price'])}: {_short(e, 120)}"
        entry.update(id=str(order.get("id")), status="open")
        self._known_order_ids.add(entry["id"])
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
            if d.get("sl_order_id"):
                pos.error = f"{self.name}: stop miktarı güncellenemedi (eski stop aktif): {_short(err or Exception())}"
            else:
                d["unprotected"] = True     # nothing protects the open size: next sync retries / closes
                pos.error = (f"{_CRITICAL_MARK} {self.name}: {pos.broker_symbol} için stop emri yerleştirilemedi; "
                             f"{_fmt(pos.remaining_quantity)} STOPSUZ açık. ({_short(err or Exception())})")
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
        try:
            if await self._cancel(old_id, pos.broker_symbol, "sl"):
                return
        except BrokerError as e:
            logger.error("%s: %s", self.name, e)
        state = await self._check_old_sl_filled(pos, old_id, old_qty)
        if state is None or state.status == "open":
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
                pos.error = (f"{_CRITICAL_MARK} {self.name}: işlem kapanırken giriş emri {_fmt(requested)} daha doldu "
                             f"ve kapatılamadı ({_fmt(executed or 0.0)} kapatıldı); {_fmt(pos.remaining_quantity)} "
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

    async def _realized_pnl(self, pos: BrokerPosition) -> float | None:
        d = pos.data
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
                    return gross - fees
        except cx.BaseError as e:
            logger.warning("%s: fetch_my_trades failed, estimating pnl: %s", self.name, e)
        if not entry:
            return None
        fee_rate = self._risk().fee_rate
        total = 0.0
        for ex in d.get("exits") or []:
            price = num(ex.get("price"))
            qty = num(ex.get("qty")) or 0.0
            if price is None:
                continue
            total += (price - entry) * qty * cs * direction - fee_rate * (price + entry) * qty * cs
        return total

    # ------------------------------------------------------------------ management
    async def update_stop_loss(self, pos: BrokerPosition, new_stop_loss: float) -> BrokerPosition:
        self._require_real(pos)
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
            raise BrokerError(f"{self.name}: yeni stop ({_fmt(price)}) yerleştirilemedi, eski stop aktif: {_short(err or Exception())}")
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
        self._require_real(pos)
        pos = pos.model_copy(deep=True)
        self._register(pos)
        d = pos.data
        if pos.status is PositionStatus.PENDING and pos.filled_quantity <= 0:
            return await self.cancel_entry(pos)
        if pos.status is not PositionStatus.OPEN:
            return pos
        if d.get("closing"):                 # already flat; only leftover orders remain
            await self._finalize_closed(pos, d.get("closed_reason") or "manual_close")
            return pos
        if d.get("unconfirmed_closes"):      # an earlier exit's result first (it may have closed it)
            await self._settle_unconfirmed(pos)
            if self._done(pos):
                return pos
        step, min_qty, _, _ = self._market_rules(pos)
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
        order = await self._ccxt(
            "kapatma emri reddedildi",
            self.exchange.create_order(pos.broker_symbol, "market", self._close_side(pos.side), qty, None, self._close_params(cid)),
        )
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

    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        self._require_real(pos)
        pos = pos.model_copy(deep=True)
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
        pos = pos.model_copy(deep=True)
        self._register(pos)
        try:
            if not pos.data.get("entry_done") or pos.data.get("fill_assumed"):
                await self._sync_entry(pos)
            if pos.status is PositionStatus.OPEN:
                await self._sync_open(pos)
        except cx.NetworkError as e:
            logger.warning("%s: sync of %s interrupted by a network error: %s", self.name, pos.broker_symbol, e)
        except cx.BaseError as e:
            raise BrokerError(f"{self.name}: durum güncellenemedi: {_short(e)}") from e
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
        except cx.NetworkError as e:
            logger.warning("%s: could not read order %s: %s", self.name, order_id, e)
            return None

    async def _settle_unconfirmed(self, pos: BrokerPosition) -> None:
        """Book exit orders whose result could not be read when they were sent."""
        d, symbol = pos.data, pos.broker_symbol
        keep: list[dict[str, Any]] = []
        kinds: set[str] = set()
        for u in list(d.get("unconfirmed_closes") or []):
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
        if d.get("closing"):
            await self._finalize_closed(pos, d.get("closed_reason") or "closing")
            if pos.status is PositionStatus.CLOSED and d.get("closed_reason") != "emergency":
                pos.error = None
            return

        if d.get("unprotected") or not d.get("sl_order_id"):
            await self._protect_new_fill(pos, emergency_status=PositionStatus.CLOSED)
            if self._done(pos) or d.get("unprotected"):
                return

        sl = await self._try_state(str(d["sl_order_id"]), symbol, "sl", since)
        if sl is None:
            return
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
            logger.error("%s: our SL %s on %s is %s; re-placing", self.name, d["sl_order_id"], symbol, sl.status)
            self._remember(d, d["sl_order_id"])
            d["sl_order_id"] = None
            d["protected_qty"] = 0.0
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

        size = await self._venue_position_size(symbol, pos.side)
        step = self._market_rules(pos)[0]
        if size is not None and size < step / 2:
            d["flat_seen"] = int(d.get("flat_seen", 0)) + 1
            if d["flat_seen"] >= 2:   # two consecutive reads: position APIs can lag behind fills
                pos.remaining_quantity = 0.0      # gone on the venue (closed there by hand)
                await self._finalize_closed(pos, "flat_on_exchange")
                pos.error = pos.error or f"{self.name}: pozisyon borsada kapalı bulundu; kalan emirler iptal edildi."
        else:
            d["flat_seen"] = 0

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
