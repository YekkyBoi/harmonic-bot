"""Mt5Broker against an in-memory fake of the MetaTrader5 package (hedging account by default)."""

from __future__ import annotations

import fnmatch
from collections.abc import Callable
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from copytrader.brokers import mt5_broker
from copytrader.brokers.base import BrokerError
from copytrader.brokers.mt5_broker import MT5, Mt5Broker, rank_symbol, split_tp_legs
from copytrader.config import Mt5BrokerConfig, RiskConfig
from copytrader.models import (
    EntryType,
    MarketType,
    OpenRequest,
    PositionStatus,
    Side,
    TakeProfit,
    TradePlan,
)
from copytrader.risk.sizing import SizingError

DONE, PLACED, INVALID_STOPS, INVALID_FILL = 10009, 10008, 10016, 10030
BID, ASK = 2232.75, 2232.95
SERVER_TIME = 1_700_000_000


def make_symbol(name: str, **kw: Any) -> SimpleNamespace:
    base = dict(
        name=name, visible=True, trade_mode=MT5.SYMBOL_TRADE_MODE_FULL, volume_step=0.01, volume_min=0.01,
        volume_max=100.0, trade_tick_size=0.01, point=0.01, trade_contract_size=1.0, digits=2,
        filling_mode=MT5.SYMBOL_FILLING_FOK | MT5.SYMBOL_FILLING_IOC, expiration_mode=15, order_mode=127,
        trade_stops_level=0, currency_base=name[:3], currency_profit="USD", currency_margin=name[:3],
    )
    base.update(kw)
    return SimpleNamespace(**base)


class FakeMT5:
    """Just enough of MetaTrader5 to exercise the adapter: orders, positions, deals, history."""

    def __init__(self, *, margin_mode: int = MT5.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.sent: list[dict[str, Any]] = []
        self.hooks: list[Callable[[dict[str, Any]], int | None]] = []
        self.account = SimpleNamespace(
            login=5001, equity=10_000.0, balance=10_000.0, margin_free=9_000.0, currency="USD",
            trade_allowed=True, trade_expert=True, margin_mode=margin_mode,
        )
        self.symbols: dict[str, SimpleNamespace] = {"ETHUSD": make_symbol("ETHUSD")}
        self.ticks: dict[str, SimpleNamespace] = {
            "ETHUSD": SimpleNamespace(bid=BID, ask=ASK, last=0.0, time=SERVER_TIME)
        }
        self.positions: dict[int, SimpleNamespace] = {}
        self.orders: dict[int, SimpleNamespace] = {}
        self.history_orders: dict[int, SimpleNamespace] = {}
        self.deals: list[SimpleNamespace] = []
        self.accepted_fillings = {MT5.ORDER_FILLING_FOK, MT5.ORDER_FILLING_IOC, MT5.ORDER_FILLING_RETURN}
        self.reject_stops_on_deal = False
        self.sltp_fail = 0
        self.commission_per_lot = 0.0
        self.margin_per_lot = 100.0
        self.fail_once: dict[str, tuple[int, str]] = {}
        self.last = (MT5.RES_S_OK, "Success")
        self.init_count = 0
        self._next = 1000

    # -- helpers ------------------------------------------------------------
    def _id(self) -> int:
        self._next += 1
        return self._next

    def _log(self, name: str, *args: Any, **kwargs: Any) -> bool:
        self.calls.append((name, args, kwargs))
        if name in self.fail_once:
            self.last = self.fail_once.pop(name)
            return True
        self.last = (MT5.RES_S_OK, "Success")
        return False

    def _result(self, retcode: int, **kw: Any) -> SimpleNamespace:
        base = dict(retcode=retcode, order=0, deal=0, volume=0.0, price=0.0, comment="", request_id=1)
        base.update(kw)
        return SimpleNamespace(**base)

    def _deal(self, p: SimpleNamespace, entry: int, volume: float, price: float, reason: int, profit: float) -> None:
        buy = (p.type == MT5.ORDER_TYPE_BUY) == (entry == MT5.DEAL_ENTRY_IN)
        self.deals.append(SimpleNamespace(
            ticket=self._id(), order=p.ticket, time_msc=self._next, type=0 if buy else 1, entry=entry,
            magic=p.magic, position_id=p.ticket, reason=reason, volume=volume, price=price,
            commission=-self.commission_per_lot * volume, swap=0.0, profit=profit, fee=0.0, symbol=p.symbol,
            comment=p.comment,
        ))

    def _open(self, ticket: int, symbol: str, otype: int, volume: float, price: float, sl: float, tp: float,
              magic: int, comment: str) -> SimpleNamespace:
        p = SimpleNamespace(ticket=ticket, identifier=ticket, symbol=symbol, type=otype, volume=volume,
                            price_open=price, sl=sl, tp=tp, magic=magic, comment=comment, profit=0.0, swap=0.0)
        self.positions[ticket] = p
        self._deal(p, MT5.DEAL_ENTRY_IN, volume, price, 3, 0.0)
        return p

    def _close(self, p: SimpleNamespace, volume: float, price: float, reason: int) -> None:
        cs = self.symbols[p.symbol].trade_contract_size
        sign = 1 if p.type == MT5.ORDER_TYPE_BUY else -1
        self._deal(p, MT5.DEAL_ENTRY_OUT, volume, price, reason, (price - p.price_open) * volume * cs * sign)
        p.volume = round(p.volume - volume, 8)
        if p.volume <= 1e-9:
            del self.positions[p.ticket]

    def fill(self, ticket: int, price: float | None = None) -> None:
        o = self.orders.pop(ticket)
        otype = MT5.ORDER_TYPE_BUY if o.type in (MT5.ORDER_TYPE_BUY_LIMIT, MT5.ORDER_TYPE_BUY_STOP) else MT5.ORDER_TYPE_SELL
        self._open(ticket, o.symbol, otype, o.volume_current, price or o.price_open, o.sl, o.tp, o.magic, o.comment)
        o.state = MT5.ORDER_STATE_FILLED
        o.position_id = ticket
        self.history_orders[ticket] = o

    def hit(self, ticket: int, reason: int, price: float) -> None:
        p = self.positions[ticket]
        self._close(p, p.volume, price, reason)

    # -- MetaTrader5 API -------------------------------------------------------
    def initialize(self, *args: Any, **kwargs: Any) -> bool:
        self._log("initialize", *args, **kwargs)
        self.init_count += 1
        return True

    def shutdown(self) -> None:
        self._log("shutdown")

    def last_error(self) -> tuple[int, str]:
        return self.last

    def account_info(self) -> Any:
        return None if self._log("account_info") else self.account

    def terminal_info(self) -> Any:
        self._log("terminal_info")
        return SimpleNamespace(trade_allowed=True)

    def symbol_info(self, name: str) -> Any:
        self._log("symbol_info", name)
        return self.symbols.get(name)

    def symbol_info_tick(self, name: str) -> Any:
        self._log("symbol_info_tick", name)
        return self.ticks.get(name)

    def symbol_select(self, name: str, enable: bool) -> bool:
        self._log("symbol_select", name, enable)
        s = self.symbols.get(name)
        if s is not None:
            s.visible = True
        return s is not None

    def symbols_get(self, group: str = "*") -> tuple[Any, ...]:
        self._log("symbols_get", group=group)
        return tuple(s for n, s in self.symbols.items() if fnmatch.fnmatch(n.upper(), group.upper()))

    def order_calc_profit(self, action: int, symbol: str, volume: float, price_open: float, price_close: float) -> float:
        self._log("order_calc_profit", action, symbol, volume, price_open, price_close)
        sign = 1 if action == MT5.ORDER_TYPE_BUY else -1
        return (price_close - price_open) * volume * self.symbols[symbol].trade_contract_size * sign

    def order_calc_margin(self, action: int, symbol: str, volume: float, price: float) -> float:
        self._log("order_calc_margin", action, symbol, volume, price)
        return volume * self.margin_per_lot

    def positions_get(self, ticket: int | None = None, symbol: str | None = None) -> Any:
        if self._log("positions_get", ticket=ticket, symbol=symbol):
            return None
        return tuple(p for p in self.positions.values()
                     if (ticket is None or p.ticket == ticket) and (symbol is None or p.symbol == symbol))

    def orders_get(self, ticket: int | None = None, symbol: str | None = None) -> Any:
        self._log("orders_get", ticket=ticket, symbol=symbol)
        return tuple(o for o in self.orders.values()
                     if (ticket is None or o.ticket == ticket) and (symbol is None or o.symbol == symbol))

    def history_orders_get(self, ticket: int | None = None) -> Any:
        self._log("history_orders_get", ticket=ticket)
        h = self.history_orders.get(ticket)  # type: ignore[arg-type]
        return (h,) if h else ()

    def history_deals_get(self, ticket: int | None = None, position: int | None = None) -> Any:
        self._log("history_deals_get", ticket=ticket, position=position)
        return tuple(d for d in self.deals
                     if (ticket is None or d.order == ticket) and (position is None or d.position_id == position))

    def order_send(self, request: dict[str, Any]) -> Any:
        self._log("order_send", request)
        self.sent.append(dict(request))
        for hook in self.hooks:
            rc = hook(request)
            if rc is not None:
                return self._result(rc, comment="hook")
        action = request["action"]
        if action in (MT5.TRADE_ACTION_DEAL, MT5.TRADE_ACTION_PENDING) and request.get("type_filling") not in self.accepted_fillings:
            return self._result(INVALID_FILL, comment="Unsupported filling mode")
        if action == MT5.TRADE_ACTION_DEAL and "position" in request:
            p = self.positions.get(request["position"])
            if p is None:
                return self._result(10036)
            vol = request["volume"]
            self._close(p, vol, request["price"], 3)
            return self._result(DONE, order=self._id(), volume=vol, price=request["price"])
        if action == MT5.TRADE_ACTION_DEAL:
            if self.reject_stops_on_deal and (request.get("sl") or request.get("tp")):
                return self._result(INVALID_STOPS, comment="Invalid stops")
            t = self._id()
            self._open(t, request["symbol"], request["type"], request["volume"], request["price"],
                       request.get("sl", 0.0), request.get("tp", 0.0), request.get("magic", 0), request.get("comment", ""))
            return self._result(DONE, order=t, deal=self.deals[-1].ticket, volume=request["volume"], price=request["price"])
        if action == MT5.TRADE_ACTION_SLTP:
            if self.sltp_fail > 0:
                self.sltp_fail -= 1
                return self._result(INVALID_STOPS, comment="Invalid stops")
            p = self.positions[request["position"]]
            p.sl, p.tp = request["sl"], request["tp"]
            return self._result(DONE)
        if action == MT5.TRADE_ACTION_PENDING:
            t = self._id()
            self.orders[t] = SimpleNamespace(
                ticket=t, symbol=request["symbol"], type=request["type"], volume_initial=request["volume"],
                volume_current=request["volume"], price_open=request["price"], sl=request.get("sl", 0.0),
                tp=request.get("tp", 0.0), magic=request.get("magic", 0), comment=request.get("comment", ""),
                type_time=request.get("type_time", 0), time_expiration=request.get("expiration", 0), state=1,
                position_id=0,
            )
            return self._result(PLACED, order=t, volume=request["volume"], price=request["price"])
        if action == MT5.TRADE_ACTION_MODIFY:
            o = self.orders[request["order"]]
            o.price_open, o.sl, o.tp = request["price"], request["sl"], request["tp"]
            return self._result(DONE, order=o.ticket)
        if action == MT5.TRADE_ACTION_REMOVE:
            o = self.orders.pop(request["order"], None)
            if o is None:
                return self._result(10013)
            o.state = MT5.ORDER_STATE_CANCELED
            self.history_orders[o.ticket] = o
            return self._result(DONE, order=o.ticket)
        return self._result(10013)

    # -- assertions helpers -------------------------------------------------------
    def sent_actions(self, action: int) -> list[dict[str, Any]]:
        return [r for r in self.sent if r["action"] == action]


# --------------------------------------------------------------------------- #
# fixtures / builders
# --------------------------------------------------------------------------- #
def make_cfg(**kw: Any) -> Mt5BrokerConfig:
    return Mt5BrokerConfig(type="mt5", name="mt5demo", login=5001, password="pw", server="Demo-Server", **kw)


def make_plan(
    side: Side = Side.LONG,
    entry_type: EntryType = EntryType.MARKET,
    entry: float = 2232.18,
    sl: float = 2205.11,
    tps: tuple[tuple[float, float], ...] = ((2310.60, 1.0),),
) -> TradePlan:
    return TradePlan(
        plan_id="p1", base_asset="ETH", quote_asset="USDT", market_type=MarketType.PERP, side=side,
        entry_type=entry_type, entry_price=entry, stop_loss=sl,
        take_profits=[TakeProfit(price=p, close_fraction=f) for p, f in tps],
        risk_r=1.0, risk_pct=2.0, source_chat_id=-100123, source_message_ids=[42],
        created_at=datetime(2026, 11, 7, tzinfo=UTC),
    )


def make_req(sym: Any, quantity: float, plan: TradePlan, tag: str = "ct12ab34cd") -> OpenRequest:
    return OpenRequest(
        plan=plan, symbol=sym, quantity=quantity, entry_type=plan.entry_type,
        entry_price=plan.entry_price if plan.entry_type is EntryType.LIMIT else None,
        stop_loss=plan.stop_loss, take_profits=plan.take_profits, client_tag=tag,
    )


THREE_TPS = ((2260.0, 1 / 3), (2290.0, 1 / 3), (2310.6, 1 / 3))


@pytest.fixture(autouse=True)
def _server_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fake ticks carry SERVER_TIME; make the broker's "now" match (server = UTC+2) and skip real sleeps."""
    monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: SERVER_TIME - 7200.0))
    monkeypatch.setattr(Mt5Broker, "tick_wait_sec", 0.0)


@pytest.fixture
def fake() -> FakeMT5:
    return FakeMT5()


async def connected(fake: FakeMT5, cfg: Mt5BrokerConfig | None = None, **risk: Any) -> Mt5Broker:
    b = Mt5Broker(cfg or make_cfg(), mt5_module=fake)
    b.retry_delay_sec = 0
    b.set_risk_config(RiskConfig(**{"include_fees_in_risk": False, **risk}))
    await b.connect()
    return b


@pytest.fixture
async def broker(fake: FakeMT5) -> Any:
    b = await connected(fake)
    yield b
    await b.close()


# --------------------------------------------------------------------------- #
# pure helpers
# --------------------------------------------------------------------------- #
def tp(*pairs: tuple[float, float]) -> list[TakeProfit]:
    return [TakeProfit(price=p, close_fraction=f) for p, f in pairs]


def test_split_equal_thirds_remainder_to_last() -> None:
    legs = split_tp_legs(0.10, tp((1, 1 / 3), (2, 1 / 3), (3, 1 / 3)), 0.01, 0.01)
    assert [(l.volume, l.tp, l.tp_indexes) for l in legs] == [(0.03, 1, [0]), (0.03, 2, [1]), (0.04, 3, [2])]


def test_split_merges_tiny_share_into_next_tp() -> None:
    legs = split_tp_legs(0.15, tp((1, 0.5), (2, 0.5)), 0.01, 0.1)   # 0.07 < min 0.1 -> merged into TP2
    assert [(l.volume, l.tp, l.tp_indexes) for l in legs] == [(0.15, 2, [0, 1])]


def test_split_merges_tiny_last_share_into_previous() -> None:
    legs = split_tp_legs(0.5, tp((1, 0.9), (2, 0.1)), 0.01, 0.1)
    assert [(l.volume, l.tp, l.tp_indexes) for l in legs] == [(0.5, 1, [0, 1])]


def test_split_without_tp_is_single_leg() -> None:
    legs = split_tp_legs(0.25, [], 0.01, 0.01)
    assert [(l.volume, l.tp) for l in legs] == [(0.25, None)]


def test_rank_symbol_prefers_plain_suffix() -> None:
    cands = ["ETHUSD", "ETHUSDT"]
    assert rank_symbol("ETHUSD.m", cands) < rank_symbol("ETHUSDm", cands)  # type: ignore[operator]
    assert rank_symbol("ETCUSD", cands) is None
    assert rank_symbol("ETHBTC", cands) is None


# --------------------------------------------------------------------------- #
# lifecycle / symbols
# --------------------------------------------------------------------------- #
async def test_connect_requires_windows_without_injected_module(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mt5_broker.sys, "platform", "linux")
    with pytest.raises(BrokerError, match="Windows"):
        await Mt5Broker(make_cfg()).connect()


async def test_connect_rejects_other_login(fake: FakeMT5) -> None:
    fake.account.login = 999
    with pytest.raises(BrokerError, match="farklı bir hesaba"):
        await connected(fake)
    assert ("shutdown", (), {}) in fake.calls


async def test_connect_passes_credentials(fake: FakeMT5) -> None:
    b = await connected(fake, make_cfg(terminal_path=r"C:\MT5\terminal64.exe"))
    _, args, kwargs = next(c for c in fake.calls if c[0] == "initialize")
    assert args == (r"C:\MT5\terminal64.exe",)
    assert kwargs["login"] == 5001 and kwargs["password"] == "pw" and kwargs["server"] == "Demo-Server"
    assert await b.get_equity() == 10_000.0 and await b.account_currency() == "USD"
    await b.close()


async def test_ipc_error_reinitializes_once(fake: FakeMT5, broker: Mt5Broker) -> None:
    fake.fail_once["account_info"] = (-10004, "No IPC connection")
    before = fake.init_count
    assert await broker.get_free_margin() == 9_000.0
    assert fake.init_count == before + 1


async def test_resolve_symbol_direct_candidate(broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH", "USDT", MarketType.PERP, "ETHUSDT.P")
    assert sym is not None and sym.broker_symbol == "ETHUSD"
    assert (sym.qty_step, sym.min_qty, sym.max_qty, sym.price_tick, sym.contract_size) == (0.01, 0.01, 100.0, 0.01, 1.0)
    assert sym.quote_asset == "USD" and sym.extra["digits"] == 2


async def test_resolve_symbol_group_search_with_suffix(fake: FakeMT5, broker: Mt5Broker) -> None:
    fake.symbols = {
        "ETCUSD.m": make_symbol("ETCUSD.m"),
        "ETHBTC.m": make_symbol("ETHBTC.m"),
        "ETHUSD.m": make_symbol("ETHUSD.m", visible=False),
    }
    sym = await broker.resolve_symbol("ETH", "USDT")
    assert sym is not None and sym.broker_symbol == "ETHUSD.m"
    assert ("symbol_select", ("ETHUSD.m", True), {}) in fake.calls


async def test_resolve_symbol_map_and_missing(fake: FakeMT5) -> None:
    fake.symbols["XETH.pro"] = make_symbol("XETH.pro")
    b = await connected(fake, make_cfg(symbol_map={"ethusdt.p": "XETH.pro"}))
    sym = await b.resolve_symbol("ETH", "USDT", raw_symbol="ETHUSDT.P")
    assert sym is not None and sym.broker_symbol == "XETH.pro"
    assert await b.resolve_symbol("DOGE", "USDT") is None
    await b.close()


# --------------------------------------------------------------------------- #
# sizing
# --------------------------------------------------------------------------- #
async def test_size_position_uses_order_calc_profit(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH", "USDT")
    assert sym is not None
    res = await broker.size_position(sym, Side.LONG, 2232.18, 2205.11, 200.0)
    # loss per lot = |order_calc_profit(BUY, 1 lot, 2232.18 -> 2205.11)| = 27.07 -> 200 / 27.07 = 7.388 -> 7.38
    assert res.quantity == 7.38
    assert res.est_loss_at_sl == pytest.approx(7.38 * 27.07)
    assert res.margin_required == pytest.approx(738.0)
    assert ("order_calc_profit", (MT5.ORDER_TYPE_BUY, "ETHUSD", 1.0, 2232.18, 2205.11), {}) in fake.calls

    # a 0.1-ETH contract: 10x the lots for the same money at risk
    fake.symbols["ETHUSD"].trade_contract_size = 0.1
    res = await broker.size_position(sym, Side.LONG, 2232.18, 2205.11, 200.0)
    assert res.quantity == 73.88


async def test_size_position_fee_buffer(fake: FakeMT5) -> None:
    # The crypto-style percentage fee is NOT applied on MT brokers (it would undersize forex badly).
    b = await connected(fake, include_fees_in_risk=True, fee_rate=0.0006)
    sym = await b.resolve_symbol("ETH")
    assert sym is not None
    res = await b.size_position(sym, Side.LONG, 2232.18, 2205.11, 200.0)
    assert res.quantity == 7.38
    await b.close()

    # An explicit commission per lot is added to the loss per lot.
    b = await connected(fake, include_fees_in_risk=True, fee_rate=0.0006)
    b.config = b.config.model_copy(update={"commission_per_lot": 2.0})
    res = await b.size_position(sym, Side.LONG, 2232.18, 2205.11, 200.0)
    assert res.quantity == 6.87 and res.est_loss_at_sl == pytest.approx(6.87 * 29.07)  # 200 / 29.07 = 6.879
    await b.close()


async def test_size_position_insufficient_margin(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    assert sym is not None
    fake.margin_per_lot = 2_000.0   # 7.38 lots -> 14760 > 9000 * 90%
    with pytest.raises(SizingError, match="Yetersiz teminat"):
        await broker.size_position(sym, Side.LONG, 2232.18, 2205.11, 200.0)


# --------------------------------------------------------------------------- #
# opening
# --------------------------------------------------------------------------- #
async def test_open_market_one_leg_per_tp(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS)))
    deals = fake.sent_actions(MT5.TRADE_ACTION_DEAL)
    assert [(d["volume"], d["tp"], d["sl"], d["comment"]) for d in deals] == [
        (0.03, 2260.0, 2205.11, "ct12ab34cd-1"),
        (0.03, 2290.0, 2205.11, "ct12ab34cd-2"),
        (0.04, 2310.6, 2205.11, "ct12ab34cd-3"),
    ]
    assert all(d["type"] == MT5.ORDER_TYPE_BUY and d["price"] == ASK and d["magic"] == 770077 for d in deals)
    assert all(d["deviation"] == 20 and d["type_time"] == MT5.ORDER_TIME_GTC for d in deals)
    assert pos.status is PositionStatus.OPEN and pos.error is None
    legs = pos.data["legs"]
    assert [l["ticket"] for l in legs] == sorted(fake.positions)
    assert pos.filled_quantity == 0.10 and pos.remaining_quantity == 0.10
    assert pos.avg_entry_price == pytest.approx(ASK)
    assert pos.stop_loss == 2205.11 and [t.price for t in pos.take_profits] == [2260.0, 2290.0, 2310.6]


async def test_filling_mode_fallback_and_cache(fake: FakeMT5, broker: Mt5Broker) -> None:
    fake.symbols["ETHUSD"].filling_mode = MT5.SYMBOL_FILLING_FOK   # claims FOK...
    fake.accepted_fillings = {MT5.ORDER_FILLING_IOC}              # ...but only IOC works
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.04, make_plan(tps=((2260.0, 0.5), (2290.0, 0.5)))))
    fillings = [d["type_filling"] for d in fake.sent_actions(MT5.TRADE_ACTION_DEAL)]
    assert fillings == [MT5.ORDER_FILLING_FOK, MT5.ORDER_FILLING_RETURN, MT5.ORDER_FILLING_IOC, MT5.ORDER_FILLING_IOC]
    assert pos.status is PositionStatus.OPEN and len(fake.positions) == 2


async def test_stop_rejected_then_attach_fails_closes_leg(fake: FakeMT5, broker: Mt5Broker) -> None:
    fake.reject_stops_on_deal = True
    fake.sltp_fail = 2
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    kinds = [(r["action"], r.get("sl"), "position" in r) for r in fake.sent]
    assert kinds == [
        (MT5.TRADE_ACTION_DEAL, 2205.11, False),   # rejected: invalid stops
        (MT5.TRADE_ACTION_DEAL, 0.0, False),       # naked entry
        (MT5.TRADE_ACTION_SLTP, 2205.11, True),    # attach SL: fails
        (MT5.TRADE_ACTION_SLTP, 2205.11, True),    # retry: fails
        (MT5.TRADE_ACTION_DEAL, None, True),       # rule 1: close the leg
    ]
    assert fake.sent[-1]["type"] == MT5.ORDER_TYPE_SELL and fake.sent[-1]["price"] == BID
    assert fake.positions == {}
    assert pos.status is PositionStatus.FAILED
    assert pos.error and "Stop konulamadı" in pos.error


async def test_stop_rejected_then_attach_retry_succeeds(fake: FakeMT5, broker: Mt5Broker) -> None:
    fake.reject_stops_on_deal = True
    fake.sltp_fail = 1
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    assert pos.status is PositionStatus.OPEN and pos.error is None
    (p,) = fake.positions.values()
    assert (p.sl, p.tp) == (2205.11, 2310.6)


async def test_market_rejects_tp_already_passed(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    with pytest.raises(BrokerError, match="TP"):
        await broker.open_position(make_req(sym, 0.05, make_plan(tps=((2230.0, 1.0),))))
    assert fake.sent == []


async def test_retry_with_same_tag_does_not_double(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    req = make_req(sym, 0.10, make_plan(tps=THREE_TPS))
    await broker.open_position(req)
    again = await broker.open_position(req)
    assert len(fake.sent_actions(MT5.TRADE_ACTION_DEAL)) == 3
    assert len(again.data["legs"]) == 3 and again.error and "etiket" in again.error


@pytest.mark.parametrize(
    "side, entry, sl, tp_price, expected",
    [
        (Side.LONG, 2200.0, 2180.0, 2300.0, MT5.ORDER_TYPE_BUY_LIMIT),
        (Side.SHORT, 2250.0, 2270.0, 2200.0, MT5.ORDER_TYPE_SELL_LIMIT),
    ],
)
async def test_pending_limit_stays_limit(
    fake: FakeMT5, broker: Mt5Broker, side: Side, entry: float, sl: float, tp_price: float, expected: int
) -> None:
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(side=side, entry_type=EntryType.LIMIT, entry=entry, sl=sl, tps=((tp_price, 1.0),))
    pos = await broker.open_position(make_req(sym, 0.05, plan))
    (req,) = fake.sent_actions(MT5.TRADE_ACTION_PENDING)
    assert req["type"] == expected and req["price"] == entry and req["sl"] == sl and req["tp"] == tp_price
    assert req["type_time"] == MT5.ORDER_TIME_SPECIFIED and req["expiration"] == SERVER_TIME + 1440 * 60
    assert pos.status is PositionStatus.PENDING and pos.filled_quantity == 0


async def test_pending_gtc_when_specified_expiry_unsupported(fake: FakeMT5, broker: Mt5Broker) -> None:
    fake.symbols["ETHUSD"].expiration_mode = 1   # GTC only
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    await broker.open_position(make_req(sym, 0.05, plan))
    (req,) = fake.sent_actions(MT5.TRADE_ACTION_PENDING)
    assert req["type_time"] == MT5.ORDER_TIME_GTC and "expiration" not in req


# --------------------------------------------------------------------------- #
# management
# --------------------------------------------------------------------------- #
async def test_update_stop_loss_keeps_each_legs_tp(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS)))
    fake.sent.clear()
    new = await broker.update_stop_loss(pos, 2220.0)
    sltp = fake.sent_actions(MT5.TRADE_ACTION_SLTP)
    assert [(r["position"], r["sl"], r["tp"]) for r in sltp] == [
        (leg["ticket"], 2220.0, leg["tp"]) for leg in pos.data["legs"]
    ]
    assert [r["tp"] for r in sltp] == [2260.0, 2290.0, 2310.6]
    assert new.stop_loss == 2220.0 and all(p.sl == 2220.0 for p in fake.positions.values())
    assert pos.stop_loss == 2205.11   # input not mutated


async def test_update_stop_loss_refuses_stop_through_price(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    fake.sent.clear()
    with pytest.raises(BrokerError, match="mevcut fiyatın"):
        await broker.update_stop_loss(pos, 2240.0)   # above bid for a long
    assert fake.sent == []


async def test_update_stop_loss_on_pending_uses_modify(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    pos = await broker.open_position(make_req(sym, 0.05, plan))
    await broker.update_stop_loss(pos, 2190.0)
    (req,) = fake.sent_actions(MT5.TRADE_ACTION_MODIFY)
    assert (req["price"], req["sl"], req["tp"], req["type_time"]) == (2200.0, 2190.0, 2300.0, MT5.ORDER_TIME_SPECIFIED)


async def test_update_take_profits_remaps_legs(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS)))
    fake.sent.clear()
    new = await broker.update_take_profits(pos, tp((2270.0, 0.5), (2320.0, 0.5)))
    assert [r["tp"] for r in fake.sent_actions(MT5.TRADE_ACTION_SLTP)] == [2270.0, 2320.0, 2320.0]
    assert all(r["sl"] == 2205.11 for r in fake.sent)
    assert new.error and "TP sayısı" in new.error


async def test_partial_close_nearest_tp_first(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    tps = ((2310.6, 1 / 3), (2260.0, 1 / 3), (2290.0, 1 / 3))   # legs: 0.03@2310.6, 0.03@2260, 0.04@2290
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=tps)))
    by_tp = {leg["tp"]: leg["ticket"] for leg in pos.data["legs"]}
    fake.sent.clear()
    new = await broker.close_position(pos, 0.5)   # 0.05 lots
    closes = fake.sent_actions(MT5.TRADE_ACTION_DEAL)
    assert [(r["position"], r["volume"]) for r in closes] == [(by_tp[2260.0], 0.03), (by_tp[2290.0], 0.02)]
    assert all(r["type"] == MT5.ORDER_TYPE_SELL and r["price"] == BID for r in closes)
    assert new.status is PositionStatus.OPEN and new.remaining_quantity == 0.05
    assert new.realized_pnl == pytest.approx((BID - ASK) * 0.05)
    assert by_tp[2260.0] not in fake.positions and fake.positions[by_tp[2290.0]].volume == pytest.approx(0.02)


async def test_full_close(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS)))
    new = await broker.close_position(pos, 1.0)
    assert fake.positions == {} and new.status is PositionStatus.CLOSED and new.remaining_quantity == 0


async def test_sync_tp_then_sl_closed_with_realized_pnl(fake: FakeMT5, broker: Mt5Broker) -> None:
    fake.commission_per_lot = 7.0
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 2.0, make_plan(tps=((2260.0, 0.5), (2310.6, 0.5)))))
    t1, t2 = (leg["ticket"] for leg in pos.data["legs"])

    fake.hit(t1, MT5.DEAL_REASON_TP, 2260.0)
    pos = await broker.sync_position(pos)
    tp_pnl = (2260.0 - ASK) * 1.0 - 7.0 * 2   # in + out commission
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == 1.0
    assert pos.realized_pnl == pytest.approx(tp_pnl)

    fake.hit(t2, MT5.DEAL_REASON_SL, 2205.11)
    pos = await broker.sync_position(pos)
    sl_pnl = (2205.11 - ASK) * 1.0 - 7.0 * 2
    assert pos.status is PositionStatus.CLOSED and pos.remaining_quantity == 0
    assert pos.realized_pnl == pytest.approx(tp_pnl + sl_pnl)
    assert pos.data["exit_reasons"] == ["tp", "sl"]


async def test_sync_detects_pending_fill(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2260.0, 0.5), (2300.0, 0.5)))
    pos = await broker.open_position(make_req(sym, 0.10, plan))
    assert pos.status is PositionStatus.PENDING
    for leg in pos.data["legs"]:
        fake.fill(leg["order_ticket"])
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN
    assert pos.filled_quantity == 0.10 and pos.avg_entry_price == pytest.approx(2200.0)
    assert {p.sl for p in fake.positions.values()} == {2180.0}


async def test_cancel_entry_removes_pending(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    pos = await broker.open_position(make_req(sym, 0.05, plan))
    new = await broker.cancel_entry(pos)
    assert len(fake.sent_actions(MT5.TRADE_ACTION_REMOVE)) == 1 and fake.orders == {}
    assert new.status is PositionStatus.CANCELLED


async def test_cancel_entry_after_fill_closes(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    pos = await broker.open_position(make_req(sym, 0.05, plan))
    fake.fill(pos.data["legs"][0]["order_ticket"])
    new = await broker.cancel_entry(pos)
    assert fake.positions == {} and new.status is PositionStatus.CLOSED


async def test_sync_reattaches_missing_stop(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    (p,) = fake.positions.values()
    p.sl = 0.0   # someone removed the stop in the terminal
    await broker.sync_position(pos)
    assert p.sl == 2205.11


# --------------------------------------------------------------------------- #
# netting accounts
# --------------------------------------------------------------------------- #
async def test_netting_single_leg_with_emulated_tp() -> None:
    fake = FakeMT5(margin_mode=0)
    b = await connected(fake)
    sym = await b.resolve_symbol("ETH")
    pos = await b.open_position(make_req(sym, 0.10, make_plan(tps=((2260.0, 0.5), (2310.6, 0.5)))))
    (deal,) = fake.sent_actions(MT5.TRADE_ACTION_DEAL)
    assert deal["volume"] == 0.10 and deal["tp"] == 2310.6
    assert [e["price"] for e in pos.data["emulated_tps"]] == [2260.0]

    fake.ticks["ETHUSD"] = SimpleNamespace(bid=2261.0, ask=2261.2, last=0.0, time=SERVER_TIME)
    fake.sent.clear()
    pos = await b.sync_position(pos)
    (close,) = fake.sent_actions(MT5.TRADE_ACTION_DEAL)
    assert close["volume"] == 0.05 and close["type"] == MT5.ORDER_TYPE_SELL
    assert pos.remaining_quantity == 0.05 and pos.data["emulated_tps"][0]["done"] is True

    with pytest.raises(BrokerError, match="Netting"):
        await b.open_position(make_req(sym, 0.10, make_plan(), tag="ctother000"))
    await b.close()


# --------------------------------------------------------------------------- #
# FX pre-flight fixes
# --------------------------------------------------------------------------- #
async def test_marketable_long_limit_opens_at_market_not_buy_stop(fake: FakeMT5, broker: Mt5Broker) -> None:
    # ask 2232.95 <= limit 2240: used to become a BUY_STOP (type 4); now a market buy with the same SL/TP
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2240.0, sl=2205.0, tps=((2300.0, 1.0),))
    pos = await broker.open_position(make_req(sym, 0.05, plan))
    assert fake.sent_actions(MT5.TRADE_ACTION_PENDING) == []
    (deal,) = fake.sent_actions(MT5.TRADE_ACTION_DEAL)
    assert deal["type"] == MT5.ORDER_TYPE_BUY and deal["sl"] == 2205.0 and deal["tp"] == 2300.0
    assert pos.status is PositionStatus.OPEN
    assert any("piyasa emriyle" in w for w in pos.data["warnings"])


async def test_marketable_short_limit_opens_at_market_not_sell_stop(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(side=Side.SHORT, entry_type=EntryType.LIMIT, entry=2220.0, sl=2260.0, tps=((2150.0, 1.0),))
    pos = await broker.open_position(make_req(sym, 0.05, plan))
    assert fake.sent_actions(MT5.TRADE_ACTION_PENDING) == []
    (deal,) = fake.sent_actions(MT5.TRADE_ACTION_DEAL)
    assert deal["type"] == MT5.ORDER_TYPE_SELL and deal["sl"] == 2260.0
    assert pos.status is PositionStatus.OPEN and pos.data["warnings"]


async def test_marketable_limit_is_resized_from_the_real_fill(fake: FakeMT5, broker: Mt5Broker) -> None:
    # sized for entry 2240 / stop 2200 (40 pts) but fills at ask 2232.95 (32.95 pts): lots grow so 1R stays 1R
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2240.0, sl=2200.0, tps=((2300.0, 1.0),))
    pos = await broker.open_position(make_req(sym, 1.00, plan))
    (deal,) = fake.sent_actions(MT5.TRADE_ACTION_DEAL)
    assert deal["volume"] == 1.21           # floor(1.0 * 40 / 32.95, 0.01)
    assert pos.quantity == 1.21


async def test_non_marketable_limit_stays_pending_limit(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    pos = await broker.open_position(make_req(sym, 0.05, plan))
    (req,) = fake.sent_actions(MT5.TRADE_ACTION_PENDING)
    assert req["type"] == MT5.ORDER_TYPE_BUY_LIMIT and fake.sent_actions(MT5.TRADE_ACTION_DEAL) == []
    assert pos.data["warnings"] == [] and pos.status is PositionStatus.PENDING


def test_10027_hint_is_actionable() -> None:
    text = mt5_broker.retcode_text(10027)
    assert "Algo Trading" in text and "yeşil" in text and "Uzman Danışmanlar" in text


async def test_algo_trading_off_at_connect_is_a_warning_not_a_failure(fake: FakeMT5) -> None:
    fake.terminal_info = lambda: SimpleNamespace(trade_allowed=False)   # type: ignore[method-assign]
    b = await connected(fake)
    assert b._connected and any("Algo Trading" in w and "yeşil" in w for w in b.startup_warnings)
    # the first order then fails with the actionable text while the button is still off
    fake.hooks.append(lambda r: 10027)
    sym = await b.resolve_symbol("ETH")
    with pytest.raises(BrokerError, match="yeşil"):
        await b.open_position(make_req(sym, 0.05, make_plan()))
    await b.close()


async def test_order_send_is_never_auto_retried_and_lost_reply_adopts_by_tag(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    real_send = fake.order_send
    state = {"n": 0}

    def flaky(request: dict[str, Any]) -> Any:
        state["n"] += 1
        res = real_send(request)          # the order IS recorded by the "server" ...
        if state["n"] == 1:
            fake.last = (-10001, "IPC send failed")
            return None                   # ... but the reply is lost
        return res

    fake.order_send = flaky   # type: ignore[method-assign]
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    assert state["n"] == 1                                   # not sent a second time
    assert len(fake.positions) == 1                          # no duplicate on the venue
    (leg,) = pos.data["legs"]
    assert leg["state"] == "open" and leg["sl"] == 2205.11   # adopted by its tag, stop verified
    assert any("tekrar gönderilmedi" in w for w in pos.data["warnings"])
    assert fake.init_count >= 2                              # terminal link re-initialized


async def test_lost_reply_without_order_on_venue_raises_clear_error(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    state = {"n": 0}

    def lost(request: dict[str, Any]) -> Any:
        state["n"] += 1
        fake.last = (-10001, "IPC send failed")
        return None

    fake.order_send = lost   # type: ignore[method-assign]
    with pytest.raises(BrokerError, match="yanıtı alınamadı"):
        await broker.open_position(make_req(sym, 0.05, make_plan()))
    assert state["n"] == 1 and fake.positions == {}


async def test_pending_order_tries_return_filling_first(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    await broker.open_position(make_req(sym, 0.05, plan))
    (req,) = fake.sent_actions(MT5.TRADE_ACTION_PENDING)
    assert req["type_filling"] == MT5.ORDER_FILLING_RETURN


async def test_pending_filling_falls_back_after_10030(fake: FakeMT5, broker: Mt5Broker) -> None:
    fake.accepted_fillings = {MT5.ORDER_FILLING_IOC}
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    await broker.open_position(make_req(sym, 0.05, plan))
    fillings = [r["type_filling"] for r in fake.sent_actions(MT5.TRADE_ACTION_PENDING)]
    assert fillings[0] == MT5.ORDER_FILLING_RETURN and fillings[-1] == MT5.ORDER_FILLING_IOC


async def test_market_order_keeps_fok_ioc_order(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    await broker.open_position(make_req(sym, 0.05, make_plan()))
    (deal,) = fake.sent_actions(MT5.TRADE_ACTION_DEAL)
    assert deal["type_filling"] == MT5.ORDER_FILLING_FOK


async def test_pending_order_ticket_zero_is_an_error(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    real_send = fake.order_send

    def zero_ticket(request: dict[str, Any]) -> Any:
        real_send(request)
        return fake._result(PLACED, order=0)

    fake.order_send = zero_ticket   # type: ignore[method-assign]
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    with pytest.raises(BrokerError, match="emir numarası"):
        await broker.open_position(make_req(sym, 0.05, plan))


async def test_forex_does_not_probe_usdt_candidate(fake: FakeMT5) -> None:
    fake.symbols["EURUSDT"] = make_symbol("EURUSDT")
    fake.symbols["EURUSD.a"] = make_symbol("EURUSD.a")
    b = await connected(fake)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)
    assert sym is not None and sym.broker_symbol == "EURUSD.a"
    assert not any(c[0] == "symbol_info" and c[1] == ("EURUSDT",) for c in fake.calls)
    await b.close()


async def test_missing_gold_symbol_gives_symbol_map_hint(fake: FakeMT5, broker: Mt5Broker) -> None:
    with pytest.raises(BrokerError, match=r"symbol_map: \{XAU: GOLD\}"):
        await broker.resolve_symbol("XAU", "USD", MarketType.CFD)


async def test_tick_retried_after_symbol_select(fake: FakeMT5) -> None:
    fake.symbols["ETHUSD"].visible = False
    b = await connected(fake)
    real = fake.symbol_info_tick
    seen = {"n": 0}

    def late(name: str) -> Any:
        seen["n"] += 1
        return None if seen["n"] < 3 else real(name)

    fake.symbol_info_tick = late   # type: ignore[method-assign]
    sym = await b.resolve_symbol("ETH")
    assert sym is not None and seen["n"] == 3
    await b.close()


async def test_weekend_stale_tick_pending_has_no_expiration(
    fake: FakeMT5, broker: Mt5Broker, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Saturday: the last tick is from Friday evening (hours old) -> GTC instead of a past expiration
    monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: SERVER_TIME - 7200.0 + 13 * 3600))
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    await broker.open_position(make_req(sym, 0.05, plan))
    (req,) = fake.sent_actions(MT5.TRADE_ACTION_PENDING)
    assert req["type_time"] == MT5.ORDER_TIME_GTC and "expiration" not in req


async def test_fresh_tick_pending_keeps_expiration(fake: FakeMT5, broker: Mt5Broker) -> None:
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    await broker.open_position(make_req(sym, 0.05, plan))
    (req,) = fake.sent_actions(MT5.TRADE_ACTION_PENDING)
    assert req["type_time"] == MT5.ORDER_TIME_SPECIFIED
