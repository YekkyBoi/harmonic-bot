"""Test doubles for the engine: parser, broker, notifier, price oracle."""

from __future__ import annotations

import math
from collections.abc import Callable
from typing import Any

from copytrader.brokers.base import Broker, BrokerError
from copytrader.config import PaperBrokerConfig
from copytrader.models import (
    BrokerPosition,
    EntryType,
    MarketType,
    OpenRequest,
    ParseContext,
    ParseResult,
    PendingConfirmation,
    PositionStatus,
    Quote,
    Severity,
    Side,
    SizingResult,
    StoredMessage,
    SymbolInfo,
    TakeProfit,
)

ScriptItem = ParseResult | Exception | Callable[[list[StoredMessage], ParseContext], ParseResult]


class FakeParser:
    """Returns scripted results in order; once the script is used up the last item repeats.
    An item may be a ParseResult, an exception to raise, or a callable(messages, context)."""

    def __init__(self, *results: ScriptItem) -> None:
        self.script: list[ScriptItem] = list(results)
        self.last: ScriptItem | None = None
        self.calls: list[tuple[list[StoredMessage], ParseContext]] = []

    def push(self, *results: ScriptItem) -> None:
        self.script.extend(results)

    async def parse(self, messages: list[StoredMessage], context: ParseContext) -> ParseResult:
        self.calls.append((list(messages), context))
        if self.script:
            self.last = self.script.pop(0)
        item = self.last
        if item is None:
            raise AssertionError("FakeParser: no scripted result")
        if isinstance(item, Exception):
            raise item
        if callable(item) and not isinstance(item, ParseResult):
            return item(messages, context)
        return item


class FakeBroker(Broker):
    """In-memory broker. Scripted equity/quotes, records every call, optional failures.

    ``fail``: method name -> exception to raise (e.g. {"open_position": BrokerError("x")}).
    ``unsupported``: base assets this broker cannot resolve.
    ``sync_fn``: called by sync_position (default: return the position unchanged).
    """

    kind = "fake"

    def __init__(
        self,
        name: str = "fake",
        *,
        equity: float = 10_000.0,
        quote: Quote | None = None,
        currency: str = "USDT",
        qty_step: float = 0.001,
        allowed_assets: list[str] | None = None,
        risk_multiplier: float = 1.0,
        unsupported: set[str] | None = None,
        fail: dict[str, Exception] | None = None,
        sync_fn: Callable[[BrokerPosition], BrokerPosition] | None = None,
    ) -> None:
        cfg = PaperBrokerConfig(type="paper", name=name, allowed_assets=allowed_assets or [],
                                risk_multiplier=risk_multiplier)
        super().__init__(cfg)
        self.equity = equity
        self.quote = quote or Quote(bid=2232.80, ask=2232.90, last=2232.85)
        self.quotes: dict[str, Quote] = {}
        self.currency = currency
        self.qty_step = qty_step
        self.unsupported = unsupported or set()
        self.fail = fail or {}
        self.sync_fn = sync_fn
        self.calls: list[tuple[str, Any]] = []
        self.connected = False
        self.closed = False
        self.sized: list[dict[str, Any]] = []
        self.opened: list[OpenRequest] = []

    def _maybe_fail(self, method: str) -> None:
        exc = self.fail.get(method)
        if exc is not None:
            raise exc

    def calls_of(self, method: str) -> list[Any]:
        return [args for name, args in self.calls if name == method]

    # ---- lifecycle
    async def connect(self) -> None:
        self.calls.append(("connect", None))
        self._maybe_fail("connect")
        self.connected = True

    async def close(self) -> None:
        self.calls.append(("close", None))
        self.closed = True

    # ---- account
    async def get_equity(self) -> float:
        self._maybe_fail("get_equity")
        return self.equity

    async def get_free_margin(self) -> float:
        return self.equity

    async def account_currency(self) -> str:
        return self.currency

    # ---- market data
    async def resolve_symbol(
        self,
        base_asset: str,
        quote_asset: str | None = None,
        market_type: MarketType | None = None,
        raw_symbol: str | None = None,
    ) -> SymbolInfo | None:
        self.calls.append(("resolve_symbol", (base_asset, quote_asset, market_type, raw_symbol)))
        self._maybe_fail("resolve_symbol")
        if base_asset.upper() in self.unsupported:
            return None
        return SymbolInfo(broker_symbol=f"{base_asset.upper()}/USDT:USDT", base_asset=base_asset.upper(),
                          quote_asset="USDT", qty_step=self.qty_step, min_qty=self.qty_step, price_tick=0.01)

    async def get_quote(self, symbol: SymbolInfo) -> Quote:
        self._maybe_fail("get_quote")
        return self.quotes.get(symbol.base_asset, self.quote)

    # ---- sizing
    async def size_position(
        self, symbol: SymbolInfo, side: Side, entry_price: float, stop_loss: float, risk_amount: float
    ) -> SizingResult:
        self.sized.append({"symbol": symbol.broker_symbol, "side": side, "entry": entry_price,
                           "stop": stop_loss, "risk_amount": risk_amount})
        self._maybe_fail("size_position")
        per_unit = abs(entry_price - stop_loss)
        qty = math.floor(risk_amount / per_unit / self.qty_step + 1e-9) * self.qty_step
        qty = round(qty, 10)
        return SizingResult(quantity=qty, risk_amount=risk_amount, est_loss_at_sl=qty * per_unit,
                            notional=qty * entry_price, leverage=5.0)

    # ---- trading
    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        self.calls.append(("open_position", req))
        self.opened.append(req)
        self._maybe_fail("open_position")
        market = req.entry_type is EntryType.MARKET
        q = self.quotes.get(req.symbol.base_asset, self.quote)
        return BrokerPosition(
            broker=self.name,
            broker_symbol=req.symbol.broker_symbol,
            side=req.plan.side,
            status=PositionStatus.OPEN if market else PositionStatus.PENDING,
            quantity=req.quantity,
            filled_quantity=req.quantity if market else 0.0,
            remaining_quantity=req.quantity if market else 0.0,
            avg_entry_price=q.entry_price_for(req.plan.side) if market else None,
            stop_loss=req.stop_loss,
            take_profits=list(req.take_profits),
            data={"client_tag": req.client_tag, "contract_size": 1.0},
        )

    async def update_stop_loss(self, pos: BrokerPosition, new_stop_loss: float) -> BrokerPosition:
        self.calls.append(("update_stop_loss", (pos, new_stop_loss)))
        self._maybe_fail("update_stop_loss")
        return pos.model_copy(update={"stop_loss": new_stop_loss})

    async def update_take_profits(self, pos: BrokerPosition, take_profits: list[TakeProfit]) -> BrokerPosition:
        self.calls.append(("update_take_profits", (pos, take_profits)))
        self._maybe_fail("update_take_profits")
        return pos.model_copy(update={"take_profits": list(take_profits)})

    async def close_position(self, pos: BrokerPosition, fraction: float = 1.0) -> BrokerPosition:
        self.calls.append(("close_position", (pos, fraction)))
        self._maybe_fail("close_position")
        qty = min(pos.remaining_quantity, pos.filled_quantity * fraction)
        remaining = round(pos.remaining_quantity - qty, 10)
        status = PositionStatus.CLOSED if remaining <= 0 else pos.status
        return pos.model_copy(update={"remaining_quantity": max(0.0, remaining), "status": status,
                                      "realized_pnl": 0.0 if status is PositionStatus.CLOSED else pos.realized_pnl})

    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        self.calls.append(("cancel_entry", pos))
        self._maybe_fail("cancel_entry")
        return pos.model_copy(update={"status": PositionStatus.CANCELLED})

    async def sync_position(self, pos: BrokerPosition) -> BrokerPosition:
        self.calls.append(("sync_position", pos))
        self._maybe_fail("sync_position")
        return self.sync_fn(pos) if self.sync_fn else pos


class FakeNotifier:
    """Records everything; ask_confirmation returns incrementing bot message ids."""

    def __init__(self) -> None:
        self.confirmations: list[PendingConfirmation] = []
        self.closed: list[tuple[PendingConfirmation, str]] = []
        self.notifications: list[tuple[str, Severity]] = []
        self.engine: Any = None
        self._next_id = 1000
        self.fail_ask = False

    async def start(self, engine: Any) -> None:
        self.engine = engine

    async def stop(self) -> None:
        return None

    async def ask_confirmation(self, conf: PendingConfirmation) -> int | None:
        if self.fail_ask:
            return None
        self.confirmations.append(conf.model_copy(deep=True))
        self._next_id += 1
        return self._next_id

    async def close_confirmation(self, conf: PendingConfirmation, result_text: str) -> None:
        self.closed.append((conf.model_copy(deep=True), result_text))

    async def notify(self, text: str, severity: Severity = Severity.INFO) -> None:
        self.notifications.append((text, severity))

    # helpers
    def texts(self, severity: Severity | None = None) -> list[str]:
        return [t for t, s in self.notifications if severity is None or s is severity]

    def closed_text(self, conf_id: str) -> str | None:
        for conf, text in self.closed:
            if conf.id == conf_id:
                return text
        return None


class FakePriceOracle:
    """price() returns ``prices[base]`` (or ``default``); ``None`` = unknown."""

    def __init__(self, prices: dict[str, float | None] | None = None, default: float | None = None) -> None:
        self.prices: dict[str, float | None] = dict(prices or {})
        self.default = default
        self.calls: list[tuple[Any, ...]] = []

    async def price(
        self,
        base_asset: str | None,
        quote_asset: str | None = None,
        market_type: MarketType | None = None,
        raw_symbol: str | None = None,
        *,
        fresh: bool = False,
    ) -> float | None:
        self.calls.append((base_asset, quote_asset, market_type, raw_symbol, fresh))
        if base_asset is None:
            return None
        return self.prices.get(base_asset.upper(), self.default)

    async def close(self) -> None:
        return None


__all__ = ["FakeParser", "FakeBroker", "FakeNotifier", "FakePriceOracle", "BrokerError"]
