"""CcxtBroker against an in-memory fake ccxt exchange (no network)."""

from __future__ import annotations

import copy
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest
from ccxt.base import errors as cx
from ccxt.base.decimal_to_precision import DECIMAL_PLACES, SIGNIFICANT_DIGITS, TICK_SIZE

from copytrader.brokers.base import BrokerError
from copytrader.brokers.ccxt_broker import (
    CcxtBroker,
    find_market,
    parse_order_state,
    precision_to_step,
    split_quantities,
    symbol_info_from_market,
)
from copytrader.config import CcxtBrokerConfig, RiskConfig
from copytrader.models import (
    BrokerPosition,
    EntryType,
    MarketType,
    OpenRequest,
    PositionStatus,
    Side,
    TakeProfit,
    TradePlan,
)

SYM = "ETH/USDT:USDT"


def _market(symbol: str, base: str, *, amount: Any = 0.001, price: Any = 0.01, min_qty: float = 0.001,
            min_cost: float | None = 5.0, contract_size: float = 1.0, active: bool = True) -> dict[str, Any]:
    return {
        "id": symbol.replace("/", "").split(":")[0],
        "symbol": symbol,
        "base": base,
        "quote": "USDT",
        "settle": "USDT",
        "type": "swap",
        "swap": True,
        "spot": False,
        "linear": True,
        "active": active,
        "contractSize": contract_size,
        "taker": 0.0005,
        "precision": {"amount": amount, "price": price},
        "limits": {"amount": {"min": min_qty, "max": 10000.0}, "cost": {"min": min_cost},
                   "leverage": {"max": 100.0}, "market": {"max": 5000.0}},
    }


class FakeExchange:
    """Records every call; orders live in memory; ``fill`` simulates executions."""

    def __init__(self, ex_id: str = "binanceusdm", *, precision_mode: int = TICK_SIZE) -> None:
        self.id = ex_id
        self.precisionMode = precision_mode
        self.has = {"fetchPositions": True, "setMarginMode": True,
                    "createOrderWithTakeProfitAndStopLoss": ex_id.startswith(("bybit", "okx", "bitget"))}
        self.markets: dict[str, dict[str, Any]] = {
            SYM: _market(SYM, "ETH"),
            "1000PEPE/USDT:USDT": _market("1000PEPE/USDT:USDT", "1000PEPE", amount=1, price=0.0000001, min_qty=1),
            "BTC/USDT:USDT": _market("BTC/USDT:USDT", "BTC", active=False),
        }
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.orders: dict[str, dict[str, Any]] = {}
        self.trades: list[dict[str, Any]] = []
        self.net: dict[str, float] = {}
        self.ticker = {"bid": 2232.80, "ask": 2232.85, "last": 2232.85}
        self.balance: dict[str, Any] = {"USDT": {"total": 10_000.0, "free": 9_000.0}, "info": {}}
        self.failures: list[tuple[Callable[[str, tuple[Any, ...]], bool], Exception]] = []
        self.minimal_create_response = False
        self._seq = 0
        self.now = 1_700_000_000_000

    # -- test helpers -----------------------------------------------------------
    def fail_when(self, predicate: Callable[[str, tuple[Any, ...]], bool], exc: Exception, times: int = 1) -> None:
        for _ in range(times):
            self.failures.append((predicate, exc))

    def _check(self, method: str, args: tuple[Any, ...]) -> None:
        self.calls.append((method, args))
        for i, (pred, exc) in enumerate(self.failures):
            if pred(method, args):
                del self.failures[i]
                raise exc

    def names(self) -> list[str]:
        return [c[0] for c in self.calls]

    def created(self) -> list[dict[str, Any]]:
        return [self.orders[c[1][-1]] for c in self.calls if c[0] == "create_order:ok"]

    def fill(self, order_id: str, amount: float | None = None, price: float | None = None) -> None:
        o = self.orders[order_id]
        qty = amount if amount is not None else o["amount"] - o["filled"]
        px = price or o.get("triggerPrice") or o.get("price") or self.ticker["last"]
        o["filled"] = round(o["filled"] + qty, 10)
        o["average"] = px
        o["status"] = "closed" if o["filled"] >= o["amount"] - 1e-12 else "open"
        sign = 1 if o["side"] == "buy" else -1
        self.net[o["symbol"]] = round(self.net.get(o["symbol"], 0.0) + sign * qty, 10)
        self.trades.append({"order": order_id, "side": o["side"], "amount": qty, "price": px,
                            "fee": {"cost": 0.1, "currency": "USDT"}})

    def by_param(self, key: str) -> list[dict[str, Any]]:
        return [o for o in self.orders.values() if key in o["params"]]

    # -- ccxt surface -----------------------------------------------------------------
    def set_sandbox_mode(self, enabled: bool) -> None:
        self.calls.append(("set_sandbox_mode", (enabled,)))

    async def load_markets(self, reload: bool = False) -> dict[str, Any]:
        self._check("load_markets", ())
        return self.markets

    async def close(self) -> None:
        self.calls.append(("close", ()))

    async def fetch_balance(self, params: dict | None = None) -> dict[str, Any]:
        self._check("fetch_balance", ())
        return copy.deepcopy(self.balance)

    async def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        self._check("fetch_ticker", (symbol,))
        return dict(self.ticker)

    async def fetch_order_book(self, symbol: str, limit: int | None = None) -> dict[str, Any]:
        self._check("fetch_order_book", (symbol,))
        return {"bids": [[self.ticker["bid"], 1]], "asks": [[self.ticker["ask"], 1]]}

    async def set_margin_mode(self, mode: str, symbol: str, params: dict | None = None) -> dict:
        self._check("set_margin_mode", (mode, symbol, params or {}))
        return {}

    async def set_leverage(self, leverage: int, symbol: str, params: dict | None = None) -> dict:
        self._check("set_leverage", (leverage, symbol, params or {}))
        return {}

    async def create_order(self, symbol: str, type: str, side: str, amount: float,
                           price: float | None = None, params: dict | None = None) -> dict[str, Any]:
        params = dict(params or {})
        self._check("create_order", (symbol, type, side, amount, price, params))
        self._seq += 1
        oid = str(self._seq)
        trigger = params.get("stopLossPrice") or params.get("takeProfitPrice") or params.get("triggerPrice")
        order = {
            "id": oid, "clientOrderId": params.get("clientOrderId"), "symbol": symbol, "type": type,
            "side": side, "amount": amount, "price": price, "filled": 0.0, "average": None, "status": "open",
            "reduceOnly": params.get("reduceOnly"), "triggerPrice": trigger,
            "stopLossPrice": params.get("stopLossPrice"), "takeProfitPrice": params.get("takeProfitPrice"),
            "timestamp": self.now, "info": {}, "params": params,
        }
        self.orders[oid] = order
        self.calls.append(("create_order:ok", (symbol, type, side, amount, price, params, oid)))
        if type == "market" and not trigger:
            self.fill(oid, price=self.ticker["ask"] if side == "buy" else self.ticker["bid"])
        if self.minimal_create_response:
            return {"id": oid, "status": None, "filled": None, "info": {}}
        return copy.deepcopy(order)

    def _get(self, order_id: str) -> dict[str, Any]:
        if order_id not in self.orders:
            raise cx.OrderNotFound(order_id)
        return copy.deepcopy(self.orders[order_id])

    async def fetch_order(self, order_id: str, symbol: str | None = None, params: dict | None = None) -> dict:
        params = params or {}
        self._check("fetch_order", (order_id, symbol, params))
        if params.get("clientOrderId"):
            for o in self.orders.values():
                if o["clientOrderId"] == params["clientOrderId"]:
                    return copy.deepcopy(o)
            raise cx.OrderNotFound(params["clientOrderId"])
        return self._get(order_id)

    async def fetch_open_order(self, order_id: str, symbol: str | None = None, params: dict | None = None) -> dict:
        self._check("fetch_open_order", (order_id, symbol, params or {}))
        o = self._get(order_id)
        if o["status"] != "open":
            raise cx.OrderNotFound(order_id)
        return o

    async def fetch_closed_order(self, order_id: str, symbol: str | None = None, params: dict | None = None) -> dict:
        self._check("fetch_closed_order", (order_id, symbol, params or {}))
        o = self._get(order_id)
        if o["status"] == "open":
            raise cx.OrderNotFound(order_id)
        return o

    async def fetch_open_orders(self, symbol: str | None = None, since: int | None = None,
                                limit: int | None = None, params: dict | None = None) -> list[dict]:
        self._check("fetch_open_orders", (symbol, params or {}))
        return [copy.deepcopy(o) for o in self.orders.values() if o["status"] == "open" and o["symbol"] == symbol]

    async def fetch_closed_orders(self, symbol: str | None = None, since: int | None = None,
                                  limit: int | None = None, params: dict | None = None) -> list[dict]:
        self._check("fetch_closed_orders", (symbol, params or {}))
        return [copy.deepcopy(o) for o in self.orders.values() if o["status"] != "open" and o["symbol"] == symbol]

    async def fetch_canceled_and_closed_orders(self, symbol: str | None = None, since: int | None = None,
                                               limit: int | None = None, params: dict | None = None) -> list[dict]:
        self._check("fetch_canceled_and_closed_orders", (symbol, params or {}))
        return [copy.deepcopy(o) for o in self.orders.values() if o["status"] != "open" and o["symbol"] == symbol]

    async def cancel_order(self, order_id: str, symbol: str | None = None, params: dict | None = None) -> dict:
        self._check("cancel_order", (order_id, symbol, params or {}))
        o = self.orders.get(order_id)
        if o is None or o["status"] != "open":
            raise cx.OrderNotFound(order_id)
        o["status"] = "canceled"
        return copy.deepcopy(o)

    async def fetch_positions(self, symbols: list[str] | None = None, params: dict | None = None) -> list[dict]:
        self._check("fetch_positions", (tuple(symbols or ()),))
        out = []
        for sym, net in self.net.items():
            if (not symbols or sym in symbols) and abs(net) > 1e-12:
                out.append({"symbol": sym, "contracts": abs(net), "side": "long" if net > 0 else "short"})
        return out

    async def fetch_my_trades(self, symbol: str | None = None, since: int | None = None,
                              limit: int | None = None, params: dict | None = None) -> list[dict]:
        self._check("fetch_my_trades", (symbol, since))
        return copy.deepcopy(self.trades)


# --------------------------------------------------------------------------- #
# fixtures / helpers
# --------------------------------------------------------------------------- #
def make_broker(ex_id: str = "binanceusdm", **cfg_kw: Any) -> tuple[CcxtBroker, FakeExchange]:
    fake = FakeExchange(ex_id)
    cfg = CcxtBrokerConfig(type="ccxt", name=f"test-{ex_id}", exchange=ex_id, **cfg_kw)

    async def no_sleep(_: float) -> None:
        return None

    broker = CcxtBroker(cfg, exchange=fake, sleep=no_sleep, clock_ms=lambda: fake.now)
    broker.set_risk_config(RiskConfig())
    return broker, fake


def make_plan(side: Side = Side.LONG, *, entry: float = 2232.18, sl: float = 2205.11,
              tps: list[TakeProfit] | None = None, entry_type: EntryType = EntryType.MARKET) -> TradePlan:
    return TradePlan(
        plan_id="p1", base_asset="ETH", quote_asset="USDT", market_type=MarketType.PERP, side=side,
        entry_type=entry_type, entry_price=entry, stop_loss=sl,
        take_profits=tps if tps is not None else [TakeProfit(price=2310.60, close_fraction=1.0)],
        risk_r=1.0, risk_pct=2.0, source_chat_id=-100, source_message_ids=[1],
        created_at=datetime(2026, 11, 7, tzinfo=UTC),
    )


async def open_market(broker: CcxtBroker, *, qty: float = 6.828, tps: list[TakeProfit] | None = None,
                      side: Side = Side.LONG, sl: float = 2205.11) -> BrokerPosition:
    info = await broker.resolve_symbol("ETH", "USDT", MarketType.PERP, "ETHUSDT.P")
    assert info is not None
    plan = make_plan(side, sl=sl, tps=tps)
    req = OpenRequest(plan=plan, symbol=info, quantity=qty, entry_type=EntryType.MARKET,
                      stop_loss=plan.stop_loss, take_profits=plan.take_profits, leverage=10, client_tag="ct12ab34cd")
    return await broker.open_position(req)


async def open_limit(broker: CcxtBroker, *, qty: float = 2.0, limit: float = 2220.0,
                     tps: list[TakeProfit] | None = None) -> BrokerPosition:
    info = await broker.resolve_symbol("ETH", "USDT")
    assert info is not None
    plan = make_plan(entry=limit, tps=tps, entry_type=EntryType.LIMIT)
    req = OpenRequest(plan=plan, symbol=info, quantity=qty, entry_type=EntryType.LIMIT, entry_price=limit,
                      stop_loss=plan.stop_loss, take_profits=plan.take_profits, leverage=10, client_tag="ct12ab34cd")
    return await broker.open_position(req)


TWO_TPS = [TakeProfit(price=2260.0, close_fraction=0.5), TakeProfit(price=2310.60, close_fraction=0.5)]


def open_orders_of(fake: FakeExchange, key: str) -> list[dict[str, Any]]:
    return [o for o in fake.by_param(key) if o["status"] == "open"]


# --------------------------------------------------------------------------- #
# pure helpers
# --------------------------------------------------------------------------- #
def test_precision_modes() -> None:
    assert precision_to_step(0.001, TICK_SIZE) == 0.001
    assert precision_to_step(3, DECIMAL_PLACES) == 0.001
    assert precision_to_step(0, DECIMAL_PLACES) == 1.0
    assert precision_to_step(4, SIGNIFICANT_DIGITS) is None
    market = _market(SYM, "ETH", amount=3, price=2)
    info = symbol_info_from_market(market, DECIMAL_PLACES, base_asset="ETH")
    assert info is not None and info.qty_step == 0.001 and info.price_tick == 0.01
    assert info.max_qty == 5000.0  # min(amount.max, market-order max)
    assert symbol_info_from_market(market, SIGNIFICANT_DIGITS, base_asset="ETH") is None


def test_split_quantities_sum_and_merge() -> None:
    assert split_quantities(6.828, [0.5, 0.5], 0.001, 0.001) == [3.414, 3.414]
    q = split_quantities(1.0, [1 / 3, 1 / 3, 1 / 3], 0.001, 0.001)
    assert q == [0.333, 0.333, 0.334] and abs(sum(q) - 1.0) < 1e-12
    # first level below minimum -> merged into the next
    assert split_quantities(0.03, [0.2, 0.8], 0.01, 0.01) == [0.0, 0.03]
    # last level below minimum -> merged into the previous one
    assert split_quantities(0.05, [0.9, 0.1], 0.01, 0.02) == [0.05, 0.0]


def test_find_market_1000x_and_symbol_map() -> None:
    fake = FakeExchange()
    found = find_market(fake.markets, base_asset="PEPE", quote_asset=None, default_quote="USDT", market_type="swap")
    assert found is not None and found[0]["symbol"] == "1000PEPE/USDT:USDT" and found[1] == 1000.0
    assert find_market(fake.markets, base_asset="BTC", quote_asset="USDT", default_quote="USDT", market_type="swap") is None
    mapped = find_market(fake.markets, base_asset="XYZ", quote_asset=None, default_quote="USDT", market_type="swap",
                         symbol_map={"xyzusdt.p": "ETHUSDT"}, raw_symbol="XYZUSDT.P")
    assert mapped is not None and mapped[0]["symbol"] == SYM


def test_parse_order_state_trigger_closed_without_filled() -> None:
    st = parse_order_state({"id": "9", "status": "closed", "filled": 0, "amount": 2.0,
                            "info": {"actualOrderId": "77"}}, trigger=True)
    assert st.status == "filled" and st.filled == 2.0 and st.linked_ids == ["77"]


# --------------------------------------------------------------------------- #
# symbols / account / sizing
# --------------------------------------------------------------------------- #
async def test_resolve_symbol_pepe_multiplier_and_inactive() -> None:
    broker, _ = make_broker()
    info = await broker.resolve_symbol("PEPE", "USDT", MarketType.PERP, "PEPEUSDT.P")
    assert info is not None
    assert info.broker_symbol == "1000PEPE/USDT:USDT"
    assert info.extra["price_multiplier"] == 1000.0
    assert info.qty_step == 1 and info.base_asset == "PEPE"
    assert await broker.resolve_symbol("BTC") is None        # inactive market
    assert await broker.resolve_symbol("XAU", "USD", MarketType.CFD) is None


async def test_symbol_map_case_insensitive() -> None:
    broker, _ = make_broker(symbol_map={"ethusdt.p": "ETH/USDT:USDT"})
    info = await broker.resolve_symbol("WHATEVER", None, None, "ETHUSDT.P")
    assert info is not None and info.broker_symbol == SYM


async def test_equity_and_bybit_coin_equity() -> None:
    broker, _ = make_broker()
    assert await broker.get_equity() == 10_000.0
    assert await broker.get_free_margin() == 9_000.0
    broker.set_risk_config(RiskConfig(use_equity=False))
    assert await broker.get_equity() == 9_000.0

    bybit, bfake = make_broker("bybit")
    bfake.balance = {"USDT": {"total": 10_000.0, "free": None},
                     "info": {"result": {"list": [{"accountType": "UNIFIED", "totalAvailableBalance": "8000",
                                                   "coin": [{"coin": "USDT", "equity": "10250.5"}]}]}}}
    assert await bybit.get_equity() == 10250.5           # incl. unrealized PnL
    assert await bybit.get_free_margin() == 8000.0


async def test_size_position_eth_1r() -> None:
    broker, _ = make_broker()
    info = await broker.resolve_symbol("ETH")
    assert info is not None
    risk = 10_000 * 0.02
    res = await broker.size_position(info, Side.LONG, 2232.18, 2205.11, risk)
    loss_per_unit = (2232.18 - 2205.11) + 0.0005 * (2232.18 + 2205.11)
    assert res.quantity == pytest.approx(int(risk / loss_per_unit * 1000) / 1000)
    assert res.est_loss_at_sl <= risk
    assert res.leverage == 10.0
    assert res.margin_required == pytest.approx(res.notional / 10)
    with pytest.raises(BrokerError):
        await broker.size_position(info, Side.LONG, 2232.18, 2240.0, risk)


# --------------------------------------------------------------------------- #
# market entry
# --------------------------------------------------------------------------- #
async def test_market_long_happy_path_order_sequence() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)

    assert pos.status is PositionStatus.OPEN
    assert pos.filled_quantity == pos.remaining_quantity == 6.828
    assert pos.avg_entry_price == 2232.85
    seq = [c for c in fake.names() if c in ("set_margin_mode", "set_leverage", "create_order:ok")]
    assert seq[:3] == ["set_margin_mode", "set_leverage", "create_order:ok"]
    margin_call = next(c for c in fake.calls if c[0] == "set_margin_mode")
    assert margin_call[1][:2] == ("isolated", SYM)
    lev_call = next(c for c in fake.calls if c[0] == "set_leverage")
    assert lev_call[1][0] == 10

    entry, sl, tp1, tp2 = fake.created()
    assert (entry["type"], entry["side"], entry["amount"]) == ("market", "buy", 6.828)
    assert entry["clientOrderId"] == "ct12ab34cde"
    assert sl["side"] == "sell" and sl["amount"] == 6.828
    assert sl["params"]["stopLossPrice"] == 2205.11 and sl["params"]["reduceOnly"] is True
    assert "takeProfitPrice" not in sl["params"]
    assert [tp1["params"]["takeProfitPrice"], tp2["params"]["takeProfitPrice"]] == [2260.0, 2310.6]
    assert all(t["params"]["reduceOnly"] is True and t["side"] == "sell" for t in (tp1, tp2))
    assert tp1["amount"] + tp2["amount"] == pytest.approx(6.828)
    assert tp1["amount"] == 3.414

    d = pos.data
    assert d["entry_order_id"] == entry["id"] and d["sl_order_id"] == sl["id"]
    assert [t["id"] for t in d["tp_order_ids"]] == [tp1["id"], tp2["id"]]
    assert d["protected_qty"] == 6.828 and d["client_tag"] == "ct12ab34cd" and d["opened_at"] == fake.now


async def test_market_entry_polls_when_response_is_minimal() -> None:
    broker, fake = make_broker("bybit")
    fake.minimal_create_response = True
    pos = await open_market(broker)
    assert pos.status is PositionStatus.OPEN and pos.filled_quantity == 6.828
    assert "fetch_canceled_and_closed_orders" in fake.names()     # bybit: open + history lookups


async def test_stop_on_wrong_side_of_market_refuses() -> None:
    broker, fake = make_broker()
    fake.ticker = {"bid": 2200.0, "ask": 2200.1, "last": 2200.0}
    with pytest.raises(BrokerError):
        await open_market(broker)
    assert "create_order" not in fake.names()


async def test_sl_failure_retry_then_emergency_close_failed() -> None:
    broker, fake = make_broker()
    is_sl = lambda m, a: m == "create_order" and "stopLossPrice" in a[5]
    fake.fail_when(is_sl, cx.InvalidOrder("Order would immediately trigger"), times=2)
    pos = await open_market(broker, tps=TWO_TPS)

    assert pos.status is PositionStatus.FAILED
    assert "stop" in (pos.error or "") and "kapatıldı" in (pos.error or "")
    sl_attempts = [c for c in fake.calls if c[0] == "create_order" and "stopLossPrice" in c[1][5]]
    assert len(sl_attempts) == 2
    close = fake.created()[-1]
    assert close["type"] == "market" and close["side"] == "sell" and close["params"]["reduceOnly"] is True
    assert close["amount"] == 6.828
    assert not fake.by_param("takeProfitPrice")              # no TPs for a closed trade
    assert fake.net[SYM] == 0
    assert pos.remaining_quantity == 0 and pos.realized_pnl is not None


async def test_sl_and_emergency_failure_keeps_position_flagged() -> None:
    broker, fake = make_broker()
    fake.fail_when(lambda m, a: m == "create_order" and a[3] and a[5].get("reduceOnly"), cx.ExchangeError("down"), times=3)
    pos = await open_market(broker, tps=[])
    assert pos.status is PositionStatus.OPEN
    assert pos.data["unprotected"] is True and "KRİTİK" in (pos.error or "")
    # next sync retries the stop
    pos = await broker.sync_position(pos)
    assert pos.data["unprotected"] is False and pos.data["sl_order_id"]
    assert open_orders_of(fake, "stopLossPrice")[0]["amount"] == 6.828


async def test_tp_failure_keeps_position_open_with_error() -> None:
    broker, fake = make_broker()
    fake.fail_when(lambda m, a: m == "create_order" and "takeProfitPrice" in a[5], cx.ExchangeError("tp rejected"))
    pos = await open_market(broker)
    assert pos.status is PositionStatus.OPEN and pos.data["sl_order_id"]
    assert "kâr al" in (pos.error or "")
    assert pos.data["tp_order_ids"][0]["status"] == "failed"
    pos = await broker.sync_position(pos)                  # retried by sync
    assert pos.data["tp_order_ids"][0]["status"] == "open" and pos.error is None


async def test_entry_timeout_adopts_existing_order_by_client_id() -> None:
    broker, fake = make_broker()
    original = fake.create_order

    async def flaky(symbol, type, side, amount, price=None, params=None):
        order = await original(symbol, type, side, amount, price, params)
        if not (params or {}).get("reduceOnly") and not fake.__dict__.get("_timed_out"):
            fake.__dict__["_timed_out"] = True
            raise cx.RequestTimeout("timeout")
        return order

    fake.create_order = flaky  # type: ignore[method-assign]
    pos = await open_market(broker)
    entries = [o for o in fake.orders.values() if o["clientOrderId"] == "ct12ab34cde"]
    assert len(entries) == 1 and pos.status is PositionStatus.OPEN
    assert pos.data["entry_order_id"] == entries[0]["id"]


# --------------------------------------------------------------------------- #
# limit entry
# --------------------------------------------------------------------------- #
async def test_limit_pending_then_sync_places_sl_tp() -> None:
    broker, fake = make_broker()
    pos = await open_limit(broker, tps=TWO_TPS)
    assert pos.status is PositionStatus.PENDING
    assert pos.data["attached"] is False                     # binance: no attached SL/TP
    assert not fake.by_param("stopLossPrice")

    fake.fill(pos.data["entry_order_id"], price=2220.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.filled_quantity == 2.0 and pos.avg_entry_price == 2220.0
    sl = open_orders_of(fake, "stopLossPrice")
    assert len(sl) == 1 and sl[0]["amount"] == 2.0
    tps = open_orders_of(fake, "takeProfitPrice")
    assert sorted(t["amount"] for t in tps) == [1.0, 1.0]


async def test_limit_partial_fills_resize_protection_new_before_cancel() -> None:
    broker, fake = make_broker()
    pos = await open_limit(broker, tps=TWO_TPS)
    entry_id = pos.data["entry_order_id"]
    fake.fill(entry_id, amount=1.0, price=2220.0)
    pos = await broker.sync_position(pos)
    first_sl = pos.data["sl_order_id"]
    assert fake.orders[first_sl]["amount"] == 1.0 and pos.status is PositionStatus.OPEN

    fake.fill(entry_id, amount=1.0, price=2220.0)
    mark = len(fake.calls)
    pos = await broker.sync_position(pos)
    new_sl = pos.data["sl_order_id"]
    assert new_sl != first_sl and fake.orders[new_sl]["amount"] == 2.0
    later = fake.calls[mark:]
    created_at = next(i for i, c in enumerate(later) if c[0] == "create_order:ok" and c[1][-1] == new_sl)
    cancelled_at = next(i for i, c in enumerate(later) if c[0] == "cancel_order" and c[1][0] == first_sl)
    assert created_at < cancelled_at
    assert sorted(t["amount"] for t in open_orders_of(fake, "takeProfitPrice")) == [1.0, 1.0]
    assert pos.data["entry_done"] is True


async def test_limit_attached_on_bybit_then_children_cleaned() -> None:
    broker, fake = make_broker("bybit")
    pos = await open_limit(broker)                            # exactly one TP -> attached
    entry = fake.orders[pos.data["entry_order_id"]]
    assert entry["params"]["stopLoss"] == {"triggerPrice": 2205.11}
    assert entry["params"]["takeProfit"] == {"triggerPrice": 2310.6}
    assert entry["params"]["tpslMode"] == "Partial"
    assert pos.data["attached"] is True

    # venue creates order-sized children on fill
    fake.fill(entry["id"], price=2220.0)
    fake._seq += 1
    child = {"id": str(fake._seq), "clientOrderId": None, "symbol": SYM, "type": "market", "side": "sell",
             "amount": 2.0, "price": None, "filled": 0.0, "average": None, "status": "open", "reduceOnly": True,
             "triggerPrice": 2205.11, "stopLossPrice": None, "takeProfitPrice": None,
             "timestamp": fake.now + 10, "info": {}, "params": {}}
    fake.orders[child["id"]] = child
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN
    assert fake.orders[child["id"]]["status"] == "canceled"
    assert fake.orders[pos.data["sl_order_id"]]["status"] == "open"
    assert pos.data["attached_cleaned"] is True


async def test_cancel_entry_pending_and_partially_filled() -> None:
    broker, fake = make_broker()
    pos = await open_limit(broker)
    pos = await broker.cancel_entry(pos)
    assert pos.status is PositionStatus.CANCELLED

    broker, fake = make_broker()
    pos = await open_limit(broker)
    fake.fill(pos.data["entry_order_id"], amount=0.5, price=2220.0)   # filled before we noticed
    pos = await broker.cancel_entry(pos)
    assert pos.status is PositionStatus.CLOSED
    close = fake.created()[-1]
    assert close["params"]["reduceOnly"] is True and close["amount"] == 0.5
    assert fake.net[SYM] == 0


# --------------------------------------------------------------------------- #
# sync: TP / SL fills, flat detection
# --------------------------------------------------------------------------- #
async def test_tp_fill_reduces_remaining_and_resizes_sl() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    old_sl = pos.data["sl_order_id"]
    tp1 = pos.data["tp_order_ids"][0]["id"]
    fake.fill(tp1, price=2260.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN
    assert pos.remaining_quantity == pytest.approx(3.414)
    new_sl = pos.data["sl_order_id"]
    assert new_sl != old_sl and fake.orders[new_sl]["amount"] == pytest.approx(3.414)
    assert fake.orders[old_sl]["status"] == "canceled"
    assert pos.data["tp_order_ids"][0]["status"] == "filled"

    fake.fill(pos.data["tp_order_ids"][1]["id"], price=2310.6)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED
    assert fake.orders[new_sl]["status"] == "canceled"
    # (2260-2232.85)*3.414 + (2310.6-2232.85)*3.414 - 3 fees of 0.1
    assert pos.realized_pnl == pytest.approx((2260 - 2232.85) * 3.414 + (2310.6 - 2232.85) * 3.414 - 0.3)


async def test_sl_fill_closes_and_cancels_leftovers() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    fake.fill(pos.data["sl_order_id"], price=2205.11)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.remaining_quantity == 0
    assert all(fake.orders[t["id"]]["status"] == "canceled" for t in pos.data["tp_order_ids"])
    assert pos.realized_pnl == pytest.approx((2205.11 - 2232.85) * 6.828 - 0.2)
    assert pos.data["closed_reason"] == "stop"


async def test_sl_detected_by_order_id_even_if_symbol_shared() -> None:
    """One-way mode: another trade keeps the net position open, our SL fill still closes OUR trade."""
    broker, fake = make_broker()
    pos = await open_market(broker, tps=[])
    fake.net[SYM] += 5.0          # someone else's long on the same symbol
    fake.fill(pos.data["sl_order_id"], price=2205.11)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED


async def test_vanished_sl_is_replaced() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=[])
    fake.orders[pos.data["sl_order_id"]]["status"] = "canceled"     # cancelled on the exchange UI
    old = pos.data["sl_order_id"]
    pos = await broker.sync_position(pos)
    assert pos.data["sl_order_id"] != old
    assert fake.orders[pos.data["sl_order_id"]]["status"] == "open"


async def test_flat_on_exchange_needs_two_reads() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    fake.net[SYM] = 0.0                    # closed manually on the exchange
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["flat_seen"] == 1
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED
    assert fake.orders[pos.data["sl_order_id"]]["status"] == "canceled"
    assert all(fake.orders[t["id"]]["status"] == "canceled" for t in pos.data["tp_order_ids"])


# --------------------------------------------------------------------------- #
# management
# --------------------------------------------------------------------------- #
async def test_update_stop_loss_places_new_before_cancelling_old() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    old = pos.data["sl_order_id"]
    mark = len(fake.calls)
    pos = await broker.update_stop_loss(pos, 2232.853)        # breakeven, rounded to tick
    later = fake.calls[mark:]
    i_new = next(i for i, c in enumerate(later) if c[0] == "create_order:ok")
    i_cancel = next(i for i, c in enumerate(later) if c[0] == "cancel_order")
    assert i_new < i_cancel and later[i_cancel][1][0] == old
    new = fake.orders[pos.data["sl_order_id"]]
    assert new["params"]["stopLossPrice"] == 2232.85 and new["amount"] == 6.828
    assert pos.stop_loss == 2232.85 and fake.orders[old]["status"] == "canceled"


async def test_update_stop_loss_failure_keeps_old() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=[])
    old = pos.data["sl_order_id"]
    fake.fail_when(lambda m, a: m == "create_order" and "stopLossPrice" in a[5], cx.InvalidOrder("bad"), times=2)
    with pytest.raises(BrokerError):
        await broker.update_stop_loss(pos, 2220.0)
    assert fake.orders[old]["status"] == "open"


async def test_update_take_profits_replaces_orders() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    old_ids = [t["id"] for t in pos.data["tp_order_ids"]]
    pos = await broker.update_take_profits(pos, [TakeProfit(price=2400.0, close_fraction=1.0)])
    assert all(fake.orders[i]["status"] == "canceled" for i in old_ids)
    tps = open_orders_of(fake, "takeProfitPrice")
    assert len(tps) == 1 and tps[0]["amount"] == 6.828 and tps[0]["params"]["takeProfitPrice"] == 2400.0


async def test_close_half_resizes_sl_and_tps() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    old_sl = pos.data["sl_order_id"]
    pos = await broker.close_position(pos, 0.5)
    close = next(o for o in fake.created() if o["clientOrderId"] == "ct12ab34cdc1")
    assert close["amount"] == 3.414 and close["params"]["reduceOnly"] is True and close["side"] == "sell"
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(3.414)
    assert fake.orders[pos.data["sl_order_id"]]["amount"] == pytest.approx(3.414)
    assert fake.orders[old_sl]["status"] == "canceled"
    tps = open_orders_of(fake, "takeProfitPrice")
    assert sum(t["amount"] for t in tps) == pytest.approx(3.414)


async def test_close_full_cancels_everything() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    pos = await broker.close_position(pos, 1.0)
    assert pos.status is PositionStatus.CLOSED and fake.net[SYM] == 0
    assert not [o for o in fake.orders.values() if o["status"] == "open"]
    assert pos.realized_pnl is not None


async def test_hedge_mode_params() -> None:
    broker, fake = make_broker(position_mode="hedge")
    await open_market(broker)
    entry, sl, tp = fake.created()
    assert entry["params"]["hedged"] is True and sl["params"]["hedged"] is True and tp["params"]["hedged"] is True


# --------------------------------------------------------------------------- #
# per-exchange trigger order handling
# --------------------------------------------------------------------------- #
async def test_binance_trigger_fetch_and_cancel_params() -> None:
    broker, fake = make_broker("binanceusdm")
    pos = await open_market(broker, tps=[])
    await broker.sync_position(pos)
    sl_fetch = [c for c in fake.calls if c[0] == "fetch_order" and c[1][0] == pos.data["sl_order_id"]]
    assert sl_fetch and sl_fetch[0][1][2] == {"trigger": True}
    await broker._cancel(pos.data["sl_order_id"], SYM, "sl")
    assert fake.calls[-1] == ("cancel_order", (pos.data["sl_order_id"], SYM, {"trigger": True}))


async def test_bybit_trigger_lookup_open_then_closed() -> None:
    broker, fake = make_broker("bybit")
    pos = await open_market(broker, tps=[])
    sl_id = pos.data["sl_order_id"]
    fake.fill(sl_id, price=2205.11)
    pos = await broker.sync_position(pos)
    opens = [c for c in fake.calls if c[0] == "fetch_open_order" and c[1][0] == sl_id]
    assert opens and opens[0][1][2] == {"trigger": True}
    history = [c for c in fake.calls if c[0] == "fetch_canceled_and_closed_orders"]
    assert history and history[0][1][1] == {"orderId": sl_id, "trigger": True}
    assert pos.status is PositionStatus.CLOSED
    assert broker._cancel_param_sets("sl") == [{}]           # linear bybit cancel needs no flag


async def test_okx_params_and_trigger_fetch() -> None:
    broker, fake = make_broker("okx", margin_mode="cross")
    pos = await open_market(broker)
    entry, sl, _tp = fake.created()
    assert entry["params"]["marginMode"] == "cross" and sl["params"]["marginMode"] == "cross"
    assert "set_margin_mode" not in fake.names()            # okx: tdMode per order
    assert next(c for c in fake.calls if c[0] == "set_leverage")[1][2] == {"marginMode": "cross"}
    await broker.sync_position(pos)
    assert ("fetch_order", (pos.data["sl_order_id"], SYM, {"trigger": True})) in fake.calls
    assert len(entry["clientOrderId"]) <= 32 and entry["clientOrderId"].isalnum()


async def test_bitget_plan_types_and_plan_lookup() -> None:
    broker, fake = make_broker("bitget")
    pos = await open_market(broker, tps=TWO_TPS)
    sl = fake.orders[pos.data["sl_order_id"]]
    assert sl["params"]["planType"] == "loss_plan" and sl["params"]["triggerType"] == "fill_price"
    tp = fake.orders[pos.data["tp_order_ids"][0]["id"]]
    assert tp["params"]["planType"] == "profit_plan"
    assert sl["params"]["marginMode"] == "isolated"
    await broker.sync_position(pos)
    plan_lists = [c for c in fake.calls if c[0] == "fetch_open_orders" and c[1][1] == {"planType": "profit_loss"}]
    assert plan_lists
    assert not [c for c in fake.calls if c[0] == "fetch_order" and c[1][0] == pos.data["sl_order_id"]]
    await broker._cancel(pos.data["sl_order_id"], SYM, "sl")
    assert fake.calls[-1][1][2] == {"trigger": True, "planType": "loss_plan"}
    await broker._cancel(pos.data["tp_order_ids"][0]["id"], SYM, "tp")
    assert fake.calls[-1][1][2] == {"trigger": True, "planType": "profit_plan"}


async def test_margin_mode_failure_is_warning_but_leverage_failure_refuses() -> None:
    broker, fake = make_broker()
    fake.fail_when(lambda m, a: m == "set_margin_mode", cx.ExchangeError("position exists"))
    pos = await open_market(broker, tps=[])
    assert pos.status is PositionStatus.OPEN and pos.data["warnings"]

    broker, fake = make_broker()
    fake.fail_when(lambda m, a: m == "set_margin_mode", cx.MarginModeAlreadySet("No need to change margin type."))
    fake.fail_when(lambda m, a: m == "set_leverage", cx.BadRequest("leverage not allowed"))
    with pytest.raises(BrokerError):
        await open_market(broker, tps=[])
    assert "create_order" not in fake.names()


async def test_simulated_positions_are_refused() -> None:
    broker, fake = make_broker()
    pos = BrokerPosition(broker="x", broker_symbol=SYM, side=Side.LONG, status=PositionStatus.OPEN,
                         quantity=1, dry_run=True, data={"simulated": True})
    assert await broker.sync_position(pos) is pos
    with pytest.raises(BrokerError):
        await broker.close_position(pos)
    assert not fake.calls


# --------------------------------------------------------------------------- #
# failure handling around leftovers / unknown fills
# --------------------------------------------------------------------------- #
async def test_leftover_cancel_failure_is_retried_before_closing() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    fake.fill(pos.data["sl_order_id"], price=2205.11)
    tp2 = pos.data["tp_order_ids"][1]["id"]
    fake.fail_when(lambda m, a: m == "cancel_order" and a[0] == tp2, cx.ExchangeNotAvailable("busy"))
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["closing"] is True and pos.remaining_quantity == 0
    assert fake.orders[tp2]["status"] == "open"
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.error is None
    assert fake.orders[tp2]["status"] == "canceled"


async def test_tp_replace_keeps_uncancellable_tp_out_of_new_size() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    tp1 = pos.data["tp_order_ids"][0]["id"]
    fake.fail_when(lambda m, a: m == "cancel_order" and a[0] == tp1, cx.ExchangeError("try later"), times=2)
    pos = await broker.update_take_profits(pos, [TakeProfit(price=2400.0, close_fraction=1.0)])
    live = open_orders_of(fake, "takeProfitPrice")
    assert sum(o["amount"] for o in live) == pytest.approx(6.828)        # never more than the position
    assert {o["id"] for o in live} >= {tp1}
    assert "iptal edilemedi" in (pos.error or "")


async def test_unknown_market_fill_is_protected_then_corrected() -> None:
    broker, fake = make_broker()
    fake.minimal_create_response = True
    fake.fail_when(lambda m, a: m == "fetch_order", cx.NetworkError("flaky"), times=6)
    pos = await open_market(broker, tps=[])
    assert pos.status is PositionStatus.OPEN and pos.data["fill_assumed"] is True
    assert fake.orders[pos.data["sl_order_id"]]["amount"] == 6.828
    entry = fake.orders[pos.data["entry_order_id"]]
    entry["filled"], entry["average"] = 6.0, 2233.0                    # venue actually filled less
    pos = await broker.sync_position(pos)
    assert pos.data["fill_assumed"] is False and pos.filled_quantity == 6.0
    assert pos.remaining_quantity == 6.0 and pos.avg_entry_price == 2233.0
    assert fake.orders[pos.data["sl_order_id"]]["amount"] == 6.0


async def test_bybit_limit_cancelled_on_exchange_becomes_cancelled() -> None:
    broker, fake = make_broker("bybit")
    pos = await open_limit(broker, tps=TWO_TPS)       # two TPs -> nothing attached
    assert pos.data["attached"] is False
    fake.orders[pos.data["entry_order_id"]]["status"] = "canceled"
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CANCELLED


async def test_cross_margin_uses_max_leverage_without_liquidation_cap() -> None:
    from copytrader.risk.sizing import choose_leverage

    # 1% stop: isolated would cap at floor(1 / (0.015 + 0.005)) = 50x; cross may use 125x
    iso, _ = choose_leverage(notional=1000, free_margin=50, stop_distance_frac=0.01, preferred=125,
                             max_leverage=125)
    cross, margin = choose_leverage(notional=1000, free_margin=50, stop_distance_frac=0.01, preferred=125,
                                    max_leverage=125, liquidation_cap=False)
    assert iso == 50 and cross == 125 and margin == 8.0


async def test_refused_leverage_steps_down_to_symbol_max() -> None:
    broker, fake = make_broker(margin_mode="cross", leverage=125, max_leverage=125)
    await broker.connect()
    for lev in (125, 100, 75):
        fake.fail_when(lambda m, a, lev=lev: m == "set_leverage" and a[0] == lev,
                       cx.BadRequest('binanceusdm {"code":-4028,"msg":"Leverage %d is not valid"}' % lev))
    warnings = await broker._prepare_symbol(SYM, 125, Side.LONG)
    levs = [c[1][0] for c in fake.calls if c[0] == "set_leverage"]
    assert levs == [125, 100, 75, 50]
    assert any("50x" in w for w in warnings)


async def test_other_leverage_errors_still_refuse() -> None:
    broker, fake = make_broker()
    await broker.connect()
    fake.fail_when(lambda m, a: m == "set_leverage", cx.AuthenticationError("invalid api key"))
    with pytest.raises(BrokerError):
        await broker._prepare_symbol(SYM, 10, Side.LONG)
