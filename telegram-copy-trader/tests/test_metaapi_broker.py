"""MetaApiBroker against a fake RPC connection (signatures checked against the installed SDK)."""

from __future__ import annotations

import inspect
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from metaapi_cloud_sdk.clients.metaapi.trade_exception import TradeException
from metaapi_cloud_sdk.metaapi.metatrader_account import MetatraderAccount
from metaapi_cloud_sdk.metaapi.rpc_metaapi_connection_instance import RpcMetaApiConnectionInstance

from copytrader.brokers.base import BrokerError
from copytrader.brokers.metaapi_broker import MetaApiBroker
from copytrader.config import MetaApiBrokerConfig, RiskConfig
from copytrader.models import EntryType, MarketType, OpenRequest, PositionStatus, Side, TakeProfit, TradePlan
from copytrader.risk.sizing import SizingError

BID, ASK = 2232.75, 2232.95
T0 = datetime(2026, 11, 7, 12, 0, tzinfo=UTC)


def trade_exc(code: int, scode: str, msg: str = "rejected") -> TradeException:
    return TradeException(msg, code, scode)


class FakeConn:
    """In-memory MetaApi RPC connection (hedging MT5 account)."""

    def __init__(self, *, margin_mode: str = "ACCOUNT_MARGIN_MODE_RETAIL_HEDGING") -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.info: dict[str, Any] = {
            "platform": "mt5", "broker": "Demo", "currency": "USD", "balance": 10_000.0, "equity": 10_000.0,
            "freeMargin": 9_000.0, "tradeAllowed": True, "marginMode": margin_mode,
        }
        self.symbols = ["ETCUSD", "ETHBTC", "ETHUSD", "BTCUSD"]
        self.specs: dict[str, dict[str, Any]] = {
            s: {
                "symbol": s, "tickSize": 0.01, "point": 0.01, "minVolume": 0.01, "maxVolume": 100.0,
                "volumeStep": 0.01, "contractSize": 1.0, "digits": 2, "profitCurrency": "USD",
                "tradeMode": "SYMBOL_TRADE_MODE_FULL", "stopsLevel": 0,
                "allowedExpirationModes": ["SYMBOL_EXPIRATION_GTC", "SYMBOL_EXPIRATION_SPECIFIED"],
                "allowedOrderTypes": ["SYMBOL_ORDER_MARKET", "SYMBOL_ORDER_LIMIT", "SYMBOL_ORDER_STOP"],
            }
            for s in self.symbols
        }
        self.prices: dict[str, dict[str, Any]] = {
            "ETHUSD": {"symbol": "ETHUSD", "bid": BID, "ask": ASK, "profitTickValue": 0.01, "lossTickValue": 0.01}
        }
        self.positions: dict[str, dict[str, Any]] = {}
        self.orders: dict[str, dict[str, Any]] = {}
        self.history_orders: dict[str, dict[str, Any]] = {}
        self.deals: list[dict[str, Any]] = []
        self.fail: dict[str, list[BaseException]] = {}
        self.commission_per_lot = 0.0
        self.margin_per_lot = 100.0
        self._next = 70000

    # -- helpers -------------------------------------------------------------
    def _rec(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.calls.append((name, args, kwargs))
        queue = self.fail.get(name)
        if queue:
            raise queue.pop(0)

    def _id(self) -> str:
        self._next += 1
        return str(self._next)

    def called(self, name: str) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
        return [(a, k) for n, a, k in self.calls if n == name]

    def _deal(self, p: dict[str, Any], entry: str, volume: float, price: float, reason: str, profit: float) -> None:
        buy = (p["type"] == "POSITION_TYPE_BUY") == (entry == "DEAL_ENTRY_IN")
        self.deals.append({
            "id": self._id(), "type": "DEAL_TYPE_BUY" if buy else "DEAL_TYPE_SELL", "entryType": entry,
            "symbol": p["symbol"], "volume": volume, "price": price, "profit": profit, "swap": 0.0,
            "commission": -self.commission_per_lot * volume, "positionId": p["id"], "reason": reason,
            "time": T0 + timedelta(seconds=len(self.deals)), "clientId": p.get("clientId"),
        })

    def _open(self, pid: str, symbol: str, buy: bool, volume: float, price: float, sl: Any, tp: Any,
              options: dict[str, Any] | None) -> None:
        opts = options or {}
        p = {"id": pid, "type": "POSITION_TYPE_BUY" if buy else "POSITION_TYPE_SELL", "symbol": symbol,
             "magic": opts.get("magic", 0), "openPrice": price, "volume": volume, "stopLoss": sl, "takeProfit": tp,
             "clientId": opts.get("clientId"), "comment": opts.get("comment"), "realizedProfit": 0.0}
        self.positions[pid] = p
        self._deal(p, "DEAL_ENTRY_IN", volume, price, "DEAL_REASON_EXPERT", 0.0)

    def _close(self, pid: str, volume: float, reason: str, price: float | None = None) -> None:
        p = self.positions[pid]
        buy = p["type"] == "POSITION_TYPE_BUY"
        px = price if price is not None else (self.prices[p["symbol"]]["bid" if buy else "ask"])
        profit = (px - p["openPrice"]) * volume * (1 if buy else -1)
        self._deal(p, "DEAL_ENTRY_OUT", volume, px, reason, profit)
        p["realizedProfit"] += profit
        p["volume"] = round(p["volume"] - volume, 8)
        if p["volume"] <= 1e-9:
            del self.positions[pid]

    def hit(self, pid: str, reason: str, price: float) -> None:
        self._close(pid, self.positions[pid]["volume"], reason, price)

    def fill(self, oid: str) -> None:
        o = self.orders.pop(oid)
        self._open(oid, o["symbol"], o["type"].startswith("ORDER_TYPE_BUY"), o["volume"], o["openPrice"],
                   o.get("stopLoss"), o.get("takeProfit"), {"magic": o["magic"], "clientId": o.get("clientId")})
        self.history_orders[oid] = {**o, "state": "ORDER_STATE_FILLED", "positionId": oid}

    def _response(self, **ids: Any) -> dict[str, Any]:
        return {"numericCode": 10009, "stringCode": "TRADE_RETCODE_DONE", "message": "Request completed", **ids}

    # -- connection API ----------------------------------------------------------
    async def connect(self) -> None:
        self._rec("connect")

    async def wait_synchronized(self, timeout_in_seconds: float = 300) -> None:
        self._rec("wait_synchronized")

    async def close(self) -> None:
        self._rec("close")

    async def get_account_information(self, options: Any = None) -> dict[str, Any]:
        self._rec("get_account_information")
        return dict(self.info)

    async def get_symbols(self) -> list[str]:
        self._rec("get_symbols")
        return list(self.symbols)

    async def get_symbol_specification(self, symbol: str) -> dict[str, Any]:
        self._rec("get_symbol_specification", symbol)
        return dict(self.specs[symbol])

    async def get_symbol_price(self, symbol: str, keep_subscription: bool = False) -> dict[str, Any]:
        self._rec("get_symbol_price", symbol)
        return dict(self.prices[symbol])

    async def calculate_margin(self, order: dict[str, Any]) -> dict[str, Any]:
        self._rec("calculate_margin", order)
        return {"margin": order["volume"] * self.margin_per_lot}

    async def _market(self, name: str, buy: bool, symbol: str, volume: float, stop_loss: Any, take_profit: Any,
                      options: Any) -> dict[str, Any]:
        self._rec(name, symbol, volume, stop_loss=stop_loss, take_profit=take_profit, options=options)
        pid = self._id()
        px = self.prices[symbol]["ask" if buy else "bid"]
        self._open(pid, symbol, buy, volume, px, stop_loss, take_profit, options)
        return self._response(orderId=pid, positionId=pid)

    async def create_market_buy_order(self, symbol: str, volume: float, stop_loss: Any = None,
                                      take_profit: Any = None, options: Any = None) -> dict[str, Any]:
        return await self._market("create_market_buy_order", True, symbol, volume, stop_loss, take_profit, options)

    async def create_market_sell_order(self, symbol: str, volume: float, stop_loss: Any = None,
                                       take_profit: Any = None, options: Any = None) -> dict[str, Any]:
        return await self._market("create_market_sell_order", False, symbol, volume, stop_loss, take_profit, options)

    async def _pending(self, name: str, otype: str, symbol: str, volume: float, open_price: float, stop_loss: Any,
                       take_profit: Any, options: Any) -> dict[str, Any]:
        self._rec(name, symbol, volume, open_price, stop_loss=stop_loss, take_profit=take_profit, options=options)
        oid = self._id()
        opts = options or {}
        self.orders[oid] = {"id": oid, "type": otype, "symbol": symbol, "volume": volume, "currentVolume": volume,
                            "openPrice": open_price, "stopLoss": stop_loss, "takeProfit": take_profit,
                            "magic": opts.get("magic", 0), "clientId": opts.get("clientId"), "state": "ORDER_STATE_PLACED"}
        return {"numericCode": 10008, "stringCode": "TRADE_RETCODE_PLACED", "message": "placed", "orderId": oid}

    async def create_limit_buy_order(self, symbol: str, volume: float, open_price: float, stop_loss: Any = None,
                                     take_profit: Any = None, options: Any = None) -> dict[str, Any]:
        return await self._pending("create_limit_buy_order", "ORDER_TYPE_BUY_LIMIT", symbol, volume, open_price,
                                   stop_loss, take_profit, options)

    async def create_limit_sell_order(self, symbol: str, volume: float, open_price: float, stop_loss: Any = None,
                                      take_profit: Any = None, options: Any = None) -> dict[str, Any]:
        return await self._pending("create_limit_sell_order", "ORDER_TYPE_SELL_LIMIT", symbol, volume, open_price,
                                   stop_loss, take_profit, options)

    async def create_stop_buy_order(self, symbol: str, volume: float, open_price: float, stop_loss: Any = None,
                                    take_profit: Any = None, options: Any = None) -> dict[str, Any]:
        return await self._pending("create_stop_buy_order", "ORDER_TYPE_BUY_STOP", symbol, volume, open_price,
                                   stop_loss, take_profit, options)

    async def create_stop_sell_order(self, symbol: str, volume: float, open_price: float, stop_loss: Any = None,
                                     take_profit: Any = None, options: Any = None) -> dict[str, Any]:
        return await self._pending("create_stop_sell_order", "ORDER_TYPE_SELL_STOP", symbol, volume, open_price,
                                   stop_loss, take_profit, options)

    async def modify_position(self, position_id: str, stop_loss: Any = None, take_profit: Any = None,
                              trailing_stop_loss: Any = None, stop_price_base: Any = None) -> dict[str, Any]:
        self._rec("modify_position", position_id, stop_loss=stop_loss, take_profit=take_profit)
        p = self.positions[position_id]
        p["stopLoss"], p["takeProfit"] = stop_loss, take_profit
        return self._response(positionId=position_id)

    async def modify_order(self, order_id: str, open_price: float, stop_loss: Any = None, take_profit: Any = None,
                           options: Any = None) -> dict[str, Any]:
        self._rec("modify_order", order_id, open_price, stop_loss=stop_loss, take_profit=take_profit)
        o = self.orders[order_id]
        o["openPrice"], o["stopLoss"], o["takeProfit"] = open_price, stop_loss, take_profit
        return self._response(orderId=order_id)

    async def close_position(self, position_id: str, options: Any = None) -> dict[str, Any]:
        self._rec("close_position", position_id, options)
        self._close(position_id, self.positions[position_id]["volume"], "DEAL_REASON_EXPERT")
        return self._response(positionId=position_id)

    async def close_position_partially(self, position_id: str, volume: float, options: Any = None) -> dict[str, Any]:
        self._rec("close_position_partially", position_id, volume, options)
        self._close(position_id, volume, "DEAL_REASON_EXPERT")
        return self._response(positionId=position_id)

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        self._rec("cancel_order", order_id)
        o = self.orders.pop(order_id)
        self.history_orders[order_id] = {**o, "state": "ORDER_STATE_CANCELED"}
        return self._response(orderId=order_id)

    async def get_positions(self, options: Any = None) -> list[dict[str, Any]]:
        self._rec("get_positions")
        return [dict(p) for p in self.positions.values()]

    async def get_orders(self, options: Any = None) -> list[dict[str, Any]]:
        self._rec("get_orders")
        return [dict(o) for o in self.orders.values()]

    async def get_history_orders_by_ticket(self, ticket: str) -> dict[str, Any]:
        self._rec("get_history_orders_by_ticket", ticket)
        h = self.history_orders.get(ticket)
        return {"historyOrders": [h] if h else [], "synchronizing": False}

    async def get_deals_by_position(self, position_id: str) -> dict[str, Any]:
        self._rec("get_deals_by_position", position_id)
        return {"deals": [d for d in self.deals if d["positionId"] == position_id], "synchronizing": False}


# --------------------------------------------------------------------------- #
# builders
# --------------------------------------------------------------------------- #
def make_cfg(**kw: Any) -> MetaApiBrokerConfig:
    return MetaApiBrokerConfig(type="metaapi", name="metaapi-demo", token="tok", account_id="acc-1", **kw)


def make_plan(side: Side = Side.LONG, entry_type: EntryType = EntryType.MARKET, entry: float = 2232.18,
              sl: float = 2205.11, tps: tuple[tuple[float, float], ...] = ((2310.60, 1.0),)) -> TradePlan:
    return TradePlan(
        plan_id="p1", base_asset="ETH", quote_asset="USDT", market_type=MarketType.PERP, side=side,
        entry_type=entry_type, entry_price=entry, stop_loss=sl,
        take_profits=[TakeProfit(price=p, close_fraction=f) for p, f in tps], risk_r=1.0, risk_pct=2.0,
        source_chat_id=-100123, source_message_ids=[42], created_at=T0,
    )


def make_req(sym: Any, quantity: float, plan: TradePlan, tag: str = "ct12ab34cd") -> OpenRequest:
    return OpenRequest(
        plan=plan, symbol=sym, quantity=quantity, entry_type=plan.entry_type,
        entry_price=plan.entry_price if plan.entry_type is EntryType.LIMIT else None,
        stop_loss=plan.stop_loss, take_profits=plan.take_profits, client_tag=tag,
    )


THREE_TPS = ((2260.0, 1 / 3), (2290.0, 1 / 3), (2310.6, 1 / 3))


async def connected(conn: FakeConn, cfg: MetaApiBrokerConfig | None = None, **risk: Any) -> MetaApiBroker:
    b = MetaApiBroker(cfg or make_cfg(), connection=conn)
    b.retry_delay_sec = 0
    b.set_risk_config(RiskConfig(**{"include_fees_in_risk": False, **risk}))
    await b.connect()
    return b


@pytest.fixture
def conn() -> FakeConn:
    return FakeConn()


@pytest.fixture
async def broker(conn: FakeConn) -> Any:
    b = await connected(conn)
    yield b
    await b.close()


# --------------------------------------------------------------------------- #
# SDK contract guard
# --------------------------------------------------------------------------- #
USED_METHODS = [
    "connect", "wait_synchronized", "close", "get_account_information", "get_symbols", "get_symbol_specification",
    "get_symbol_price", "calculate_margin", "create_market_buy_order", "create_market_sell_order",
    "create_limit_buy_order", "create_limit_sell_order", "create_stop_buy_order", "create_stop_sell_order",
    "modify_position", "modify_order", "close_position", "close_position_partially", "cancel_order",
    "get_positions", "get_orders", "get_history_orders_by_ticket", "get_deals_by_position",
]


@pytest.mark.parametrize("name", USED_METHODS)
def test_fake_matches_installed_sdk_signature(name: str) -> None:
    real = list(inspect.signature(getattr(RpcMetaApiConnectionInstance, name)).parameters)
    fake = list(inspect.signature(getattr(FakeConn, name)).parameters)
    assert fake[: len(real)] == real


def test_account_api_methods_exist() -> None:
    for name in ("deploy", "wait_deployed", "wait_connected", "get_rpc_connection", "state"):
        assert hasattr(MetatraderAccount, name)


# --------------------------------------------------------------------------- #
# lifecycle / symbols
# --------------------------------------------------------------------------- #
class FakeAccount:
    def __init__(self, conn: FakeConn) -> None:
        self.state = "UNDEPLOYED"
        self.conn = conn
        self.log: list[str] = []

    async def deploy(self) -> None:
        self.log.append("deploy")
        self.state = "DEPLOYING"

    async def wait_deployed(self) -> None:
        self.log.append("wait_deployed")

    async def wait_connected(self) -> None:
        self.log.append("wait_connected")

    def get_rpc_connection(self) -> FakeConn:
        self.log.append("get_rpc_connection")
        return self.conn


async def test_connect_deploys_and_synchronizes(conn: FakeConn) -> None:
    account = FakeAccount(conn)
    seen: dict[str, Any] = {}

    class FakeAccountApi:
        async def get_account(self, account_id: str) -> FakeAccount:
            seen["account_id"] = account_id
            return account

    class FakeApi:
        def __init__(self, token: str, opts: dict[str, Any]) -> None:
            seen["token"], seen["opts"] = token, opts
            self.metatrader_account_api = FakeAccountApi()
            self.closed = False

        def close(self) -> None:
            self.closed = True

    b = MetaApiBroker(make_cfg(region="london"), api_factory=FakeApi)
    await b.connect()
    assert seen == {"token": "tok", "opts": {"region": "london"}, "account_id": "acc-1"}
    assert account.log == ["deploy", "wait_deployed", "wait_connected", "get_rpc_connection"]
    assert [n for n, *_ in conn.calls[:3]] == ["connect", "wait_synchronized", "get_account_information"]
    assert await b.get_equity() == 10_000.0 and await b.get_free_margin() == 9_000.0
    await b.close()
    assert b._api is None and conn.called("close")


async def test_connect_refuses_investor_password(conn: FakeConn) -> None:
    conn.info["tradeAllowed"] = False
    with pytest.raises(BrokerError, match="işlem izni yok"):
        await connected(conn)


async def test_resolve_symbol(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH", "USDT", raw_symbol="ETHUSDT.P")
    assert sym is not None and sym.broker_symbol == "ETHUSD"
    assert (sym.qty_step, sym.min_qty, sym.max_qty, sym.price_tick, sym.contract_size) == (0.01, 0.01, 100.0, 0.01, 1.0)
    assert await broker.resolve_symbol("DOGE") is None


async def test_resolve_symbol_with_suffix(conn: FakeConn) -> None:
    template = conn.specs["ETHUSD"]
    conn.symbols = ["ETHBTC.r", "ETHUSD.r", "ETCUSD.r"]
    conn.specs = {s: {**template, "symbol": s} for s in conn.symbols}
    b = await connected(conn)
    sym = await b.resolve_symbol("ETH", "USDT")
    assert sym is not None and sym.broker_symbol == "ETHUSD.r"


# --------------------------------------------------------------------------- #
# sizing
# --------------------------------------------------------------------------- #
async def test_loss_per_lot_from_loss_tick_value(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    assert sym is not None
    res = await broker.size_position(sym, Side.LONG, 2232.18, 2205.11, 200.0)
    # 27.07 / tickSize 0.01 * lossTickValue 0.01 = 27.07 per lot -> 7.38 lots
    assert res.quantity == 7.38 and res.est_loss_at_sl == pytest.approx(7.38 * 27.07)
    assert res.margin_required == pytest.approx(738.0)

    conn.prices["ETHUSD"]["lossTickValue"] = 0.02     # e.g. a different account currency
    res = await broker.size_position(sym, Side.LONG, 2232.18, 2205.11, 200.0)
    assert res.quantity == 3.69


async def test_loss_per_lot_fallback_and_refusal(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    assert sym is not None
    del conn.prices["ETHUSD"]["lossTickValue"]
    res = await broker.size_position(sym, Side.SHORT, 2205.11, 2232.18, 200.0)   # contractSize * distance
    assert res.quantity == 7.38
    broker._specs["ETHUSD"]["profitCurrency"] = "JPY"
    with pytest.raises(BrokerError, match="tick değeri"):
        await broker.size_position(sym, Side.SHORT, 2205.11, 2232.18, 200.0)


async def test_size_insufficient_margin(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    conn.margin_per_lot = 5_000.0
    with pytest.raises(SizingError, match="Yetersiz teminat"):
        await broker.size_position(sym, Side.LONG, 2232.18, 2205.11, 200.0)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# opening
# --------------------------------------------------------------------------- #
async def test_open_market_one_leg_per_tp(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS)))
    calls = conn.called("create_market_buy_order")
    assert [(a[1], k["stop_loss"], k["take_profit"], k["options"]["clientId"]) for a, k in calls] == [
        (0.03, 2205.11, 2260.0, "ct12ab34cd-1"),
        (0.03, 2205.11, 2290.0, "ct12ab34cd-2"),
        (0.04, 2205.11, 2310.6, "ct12ab34cd-3"),
    ]
    for _, k in calls:
        opts = k["options"]
        assert opts["magic"] == 770077 and opts["slippage"] == 20
        assert len(opts["clientId"]) + len(opts.get("comment", "")) <= 26
    assert pos.status is PositionStatus.OPEN and pos.error is None
    assert pos.filled_quantity == 0.10 and pos.avg_entry_price == pytest.approx(ASK)   # filled in by the post-open refresh
    assert [leg["ticket"] for leg in pos.data["legs"]] == list(conn.positions)


async def test_trade_exception_maps_to_broker_error(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    conn.fail["create_market_buy_order"] = [trade_exc(10019, "TRADE_RETCODE_NO_MONEY", "No money")]
    with pytest.raises(BrokerError, match="yetersiz bakiye") as ei:
        await broker.open_position(make_req(sym, 0.05, make_plan()))
    assert "10019" in str(ei.value)
    assert conn.positions == {}


async def test_invalid_stops_then_attach_fails_closes_leg(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    conn.fail["create_market_buy_order"] = [trade_exc(10016, "TRADE_RETCODE_INVALID_STOPS")]
    conn.fail["modify_position"] = [trade_exc(10016, "TRADE_RETCODE_INVALID_STOPS")] * 2
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    buys = conn.called("create_market_buy_order")
    assert [(k["stop_loss"], k["take_profit"]) for _, k in buys] == [(2205.11, 2310.6), (None, None)]
    assert len(conn.called("modify_position")) == 2 and len(conn.called("close_position")) == 1
    assert conn.positions == {} and pos.status is PositionStatus.FAILED
    assert pos.error and "Stop konulamadı" in pos.error


async def test_invalid_stops_then_attach_succeeds(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    conn.fail["create_market_buy_order"] = [trade_exc(10016, "TRADE_RETCODE_INVALID_STOPS")]
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    (p,) = conn.positions.values()
    assert (p["stopLoss"], p["takeProfit"]) == (2205.11, 2310.6)
    assert pos.status is PositionStatus.OPEN and pos.error is None


@pytest.mark.parametrize(
    "side, entry, sl, tp_price, method",
    [
        (Side.LONG, 2200.0, 2180.0, 2300.0, "create_limit_buy_order"),
        (Side.SHORT, 2250.0, 2270.0, 2200.0, "create_limit_sell_order"),
    ],
)
async def test_pending_limit_stays_limit(conn: FakeConn, broker: MetaApiBroker, side: Side, entry: float, sl: float,
                                     tp_price: float, method: str) -> None:
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(side=side, entry_type=EntryType.LIMIT, entry=entry, sl=sl, tps=((tp_price, 1.0),))
    before = datetime.now(UTC)
    pos = await broker.open_position(make_req(sym, 0.05, plan))
    ((args, kw),) = conn.called(method)
    assert args == ("ETHUSD", 0.05, entry) and (kw["stop_loss"], kw["take_profit"]) == (sl, tp_price)
    exp = kw["options"]["expiration"]
    assert exp["type"] == "ORDER_TIME_SPECIFIED"
    assert before + timedelta(minutes=1439) < exp["time"] < before + timedelta(minutes=1441)
    assert pos.status is PositionStatus.PENDING


# --------------------------------------------------------------------------- #
# management / sync
# --------------------------------------------------------------------------- #
async def test_update_stop_loss_keeps_tp(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS)))
    new = await broker.update_stop_loss(pos, 2220.0)
    mods = conn.called("modify_position")
    assert [(a[0], k["stop_loss"], k["take_profit"]) for a, k in mods] == [
        (leg["ticket"], 2220.0, leg["tp"]) for leg in pos.data["legs"]
    ]
    assert new.stop_loss == 2220.0
    assert {p["takeProfit"] for p in conn.positions.values()} == {2260.0, 2290.0, 2310.6}


async def test_update_stop_loss_pending_uses_modify_order(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    pos = await broker.open_position(make_req(sym, 0.05, plan))
    await broker.update_stop_loss(pos, 2190.0)
    ((args, kw),) = conn.called("modify_order")
    assert args[1] == 2200.0 and (kw["stop_loss"], kw["take_profit"]) == (2190.0, 2300.0)


async def test_partial_close_nearest_tp_first(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    tps = ((2310.6, 1 / 3), (2260.0, 1 / 3), (2290.0, 1 / 3))   # 0.03@2310.6, 0.03@2260, 0.04@2290
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=tps)))
    by_tp = {leg["tp"]: leg["ticket"] for leg in pos.data["legs"]}
    new = await broker.close_position(pos, 0.5)
    assert [a[0] for a, _ in conn.called("close_position")] == [by_tp[2260.0]]
    assert [(a[0], a[1]) for a, _ in conn.called("close_position_partially")] == [(by_tp[2290.0], 0.02)]
    assert new.status is PositionStatus.OPEN and new.remaining_quantity == 0.05
    assert new.realized_pnl == pytest.approx((BID - ASK) * 0.05)


async def test_sync_closed_with_realized_pnl(conn: FakeConn, broker: MetaApiBroker) -> None:
    conn.commission_per_lot = 3.0
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 2.0, make_plan(tps=((2260.0, 0.5), (2310.6, 0.5)))))
    t1, t2 = (leg["ticket"] for leg in pos.data["legs"])
    conn.hit(t1, "DEAL_REASON_TP", 2260.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == 1.0
    conn.hit(t2, "DEAL_REASON_SL", 2205.11)
    pos = await broker.sync_position(pos)
    expected = (2260.0 - ASK) + (2205.11 - ASK) - 3.0 * 4
    assert pos.status is PositionStatus.CLOSED and pos.realized_pnl == pytest.approx(expected)
    assert pos.data["exit_reasons"] == ["tp", "sl"]


async def test_sync_pending_fill_and_cancel(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    pos = await broker.open_position(make_req(sym, 0.05, plan))
    conn.fill(pos.data["legs"][0]["order_ticket"])
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.avg_entry_price == pytest.approx(2200.0)

    pos2 = await broker.open_position(make_req(sym, 0.05, plan, tag="ctsecond00"))
    pos2 = await broker.cancel_entry(pos2)
    assert pos2.status is PositionStatus.CANCELLED and len(conn.called("cancel_order")) == 1
