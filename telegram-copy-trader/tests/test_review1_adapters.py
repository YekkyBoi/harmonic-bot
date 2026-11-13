"""Review 1 of the live release 62e0719, adapter lane (brokers/*.py). Each test failed on 62e0719.

A1  BROKERLAR-1: a pending MetaTrader order that fills while the bot reads the account is never booked closed or
    cancelled while its position is open (both MetaTrader adapters share MetaTraderLegBroker)
A2  BROKERLAR-2: realized PnL after the bot closes a leg in two or more steps equals the venue's deals
A3  BROKERLAR-3: the filled part of a partly filled pending order is tracked and managed, never left untracked
A4  ILETISIM-6: MetaApi refuses new signals only after 2 failed health checks in a row (or at once when the SDK
    itself reports the link down), and says only what is known
A5  YONETIM-2 (a) / ILETISIM-1: while MetaApi reads time out (or the SDK reports the link down) reads give up after
    about 3 s and management calls fail fast with a clear text; trade requests in flight are never cut short
A6  ILETISIM-2: the manual broker hands its instruction to the notifier without waiting for Telegram
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from copytrader.brokers import manual_broker as mbk
from copytrader.brokers.manual_broker import ManualBroker
from copytrader.config import ManualBrokerConfig, RiskConfig
from copytrader.models import EntryType, MarketType, OpenRequest, Severity, Side, TakeProfit, TradePlan

from test_paper_broker import FakePrices

NOW = datetime(2026, 11, 12, 12, 0, tzinfo=UTC)


# --------------------------------------------------------------------------- #
# A6  manual broker: the instruction is handed over, never awaited inside the leg
# --------------------------------------------------------------------------- #
class SilentNotifier:
    """The owner's bot whose Telegram link is silent: notify() returns only when ``answer`` is set (the real
    BotNotifier waits up to SEND_TIMEOUT_SEC = 20 s, then queues). Records the order the texts were handed in."""

    def __init__(self) -> None:
        self.answer = asyncio.Event()
        self.started: list[str] = []
        self.delivered: list[str] = []

    async def notify(self, text: str, severity: Severity = Severity.INFO) -> None:
        self.started.append(text)
        await self.answer.wait()
        self.delivered.append(text)


def manual(tmp_path: Path, notifier: Any) -> ManualBroker:
    broker = ManualBroker(ManualBrokerConfig(type="manual", name="breakout", risk_usd=50.0), str(tmp_path),
                          price_source=FakePrices())
    broker.set_risk_config(RiskConfig())
    if notifier is not None:
        broker.set_notifier(notifier)
    return broker


def eth_short() -> TradePlan:
    return TradePlan(plan_id="m1", base_asset="ETH", side=Side.SHORT, entry_type=EntryType.MARKET, entry_price=2239.20,
                     stop_loss=2250.60, take_profits=[TakeProfit(price=2205.11)], risk_r=1.0, risk_pct=2.0,
                     source_chat_id=1, created_at=NOW)


async def manual_req(broker: ManualBroker) -> OpenRequest:
    info = await broker.resolve_symbol("ETH", "USDT", MarketType.PERP, "ETHUSDT.P")
    plan = eth_short()
    return OpenRequest(plan=plan, symbol=info, quantity=2.0, entry_type=EntryType.MARKET, stop_loss=2250.60,
                       take_profits=plan.take_profits, client_tag="ctm1")


async def fast(coro: Any, limit: float = 0.5) -> Any:
    """The call must return well within ``limit`` (62e0719 waited for Telegram: up to 20 s in production)."""
    t0 = time.monotonic()
    result = await asyncio.wait_for(coro, limit * 4)
    assert time.monotonic() - t0 < limit, f"took {time.monotonic() - t0:.2f}s"
    return result


async def test_a6_a_silent_bot_link_never_holds_up_the_manual_leg(tmp_path: Path) -> None:
    notifier = SilentNotifier()
    broker = manual(tmp_path, notifier)
    await broker.connect()
    pos = await fast(broker.open_position(await manual_req(broker)))
    assert pos.status.value == "open" and pos.data["manual_qty"] == 2.0
    pos = await fast(broker.update_stop_loss(pos, 2240.0))
    pos = await fast(broker.update_take_profits(pos, [TakeProfit(price=2200.0)]))
    await fast(broker.close_position(pos, 1.0))
    # every instruction was handed to the notifier at once, in the order given; none is delivered yet
    assert len(notifier.started) == 4 and notifier.delivered == []
    assert "ETH — ETH SHORT" in notifier.started[0] and "STOPU 2240 YAP" in notifier.started[1]  # (final: the instruction names the unit and the USD value; 'KONTRAT' was typed as USD on 2026-11-12)
    assert "TP'Yİ 2200 YAP" in notifier.started[2] and "TAMAMINI KAPAT" in notifier.started[3]
    notifier.answer.set()                       # Telegram answers again: the notifier delivers them, in order
    await fast(broker.close())
    assert notifier.delivered == notifier.started


async def test_a6_cancel_instruction_is_not_awaited_either(tmp_path: Path) -> None:
    notifier = SilentNotifier()
    broker = manual(tmp_path, notifier)
    await broker.connect()
    req = await manual_req(broker)
    plan = req.plan.model_copy(update={"entry_type": EntryType.LIMIT, "entry_price": 2245.0})
    req = req.model_copy(update={"plan": plan, "entry_type": EntryType.LIMIT, "entry_price": 2245.0})
    pos = await fast(broker.open_position(req))
    await fast(broker.cancel_entry(pos))
    assert "LİMİT emir" in notifier.started[0] and "BEKLEYEN LİMİT EMRİ İPTAL ET" in notifier.started[1]
    notifier.answer.set()
    await broker.close()


async def test_a6_close_waits_for_hand_overs_but_never_for_ever(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mbk, "HANDOFF_WAIT_SEC", 0.2)
    notifier = SilentNotifier()
    broker = manual(tmp_path, notifier)
    await broker.connect()
    await fast(broker.open_position(await manual_req(broker)))
    t0 = time.monotonic()
    await broker.close()                        # Telegram never answers: close() gives up after HANDOFF_WAIT_SEC
    assert 0.15 < time.monotonic() - t0 < 1.0
    notifier.answer.set()
    await asyncio.sleep(0)


async def test_a6_a_failing_notifier_does_not_break_the_leg(tmp_path: Path) -> None:
    class Broken:
        async def notify(self, text: str, severity: Severity = Severity.INFO) -> None:
            raise RuntimeError("telegram is gone")

    broker = manual(tmp_path, Broken())
    await broker.connect()
    pos = await fast(broker.open_position(await manual_req(broker)))
    assert pos.status.value == "open" and not pos.data.get("warnings")
    await broker.close()


async def test_a6_without_a_notifier_the_open_says_the_instruction_was_not_sent(tmp_path: Path) -> None:
    broker = manual(tmp_path, None)
    await broker.connect()
    pos = await broker.open_position(await manual_req(broker))
    assert any("Elle işlem talimatı gönderilemedi" in w for w in pos.data["warnings"])
    await broker.close()


# --------------------------------------------------------------------------- #
# MetaTrader venues that change state at an exact point of the adapter's reads
# --------------------------------------------------------------------------- #
import test_metaapi_broker as tma  # noqa: E402
import test_mt5_broker as tmt  # noqa: E402

from copytrader.brokers.base import BrokerError  # noqa: E402
from copytrader.brokers.metaapi_broker import MetaApiBroker  # noqa: E402
from copytrader.brokers.mt5_broker import _MAX_MISSES, MT5, Mt5Broker  # noqa: E402
from copytrader.models import PositionStatus  # noqa: E402


@pytest.fixture(autouse=True)
def _server_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: tmt.SERVER_TIME - 7200.0))
    monkeypatch.setattr(Mt5Broker, "tick_wait_sec", 0.0)


class RacyTerminal(tmt.FakeMT5):
    """``arm(ticket, "between")``: the order fills after positions_get answered and before orders_get answers.
    ``arm(ticket, "history_first")``: the terminal shows the order in its history as FILLED before the deal and
    the position exist (``complete()`` brings them)."""

    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.armed: tuple[int, str] | None = None
        self.deferred: Any = None

    def arm(self, ticket: int, mode: str) -> None:
        self.armed = (ticket, mode)

    def orders_get(self, ticket: int | None = None, symbol: str | None = None) -> Any:
        if self.armed is not None and ticket is None:
            oid, mode = self.armed
            self.armed = None
            if mode == "between":
                self.fill(oid)
            else:
                o = self.orders.pop(oid)
                o.state, o.position_id = MT5.ORDER_STATE_FILLED, oid
                self.history_orders[oid] = o
                self.deferred = o
        return super().orders_get(ticket=ticket, symbol=symbol)

    def complete(self) -> None:
        o, self.deferred = self.deferred, None
        buy = o.type in (MT5.ORDER_TYPE_BUY_LIMIT, MT5.ORDER_TYPE_BUY_STOP)
        self._open(o.ticket, o.symbol, MT5.ORDER_TYPE_BUY if buy else MT5.ORDER_TYPE_SELL, o.volume_current,
                   o.price_open, o.sl, o.tp, o.magic, o.comment)

    def venue(self) -> list[tuple[int, float, float, float]]:
        return [(p.ticket, p.volume, p.sl, p.tp) for p in self.positions.values()]


class RacyConn(tma.FakeConn):
    """``armed = order id``: the order fills between the getPositions and the getOrders round trips."""

    def __init__(self) -> None:
        super().__init__()
        self.armed: str | None = None

    async def get_orders(self, options: Any = None) -> list[dict[str, Any]]:
        if self.armed is not None:
            oid, self.armed = self.armed, None
            self.fill(oid)
        return await super().get_orders(options)


async def mt5(term: tmt.FakeMT5) -> Mt5Broker:
    return await tmt.connected(term)


async def resting_buy_limit(b: Any, mod: Any, lots: float = 0.10) -> Any:
    sym = await b.resolve_symbol("ETH")
    plan = mod.make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    pos = await b.open_position(mod.make_req(sym, lots, plan))
    assert pos.status is PositionStatus.PENDING
    return pos


# --------------------------------------------------------------------------- #
# A1  a pending order that fills while the bot reads the account
# --------------------------------------------------------------------------- #
async def test_a1_mt5_fill_between_the_two_reads_is_an_open_managed_position() -> None:
    term = RacyTerminal()
    b = await mt5(term)
    pos = await resting_buy_limit(b, tmt)
    term.arm(pos.data["legs"][0]["order_ticket"], "between")
    pos = await b.sync_position(pos)
    # 62e0719: status closed, reason manual, realized 0.0, while the venue holds (1001, 0.1, 2180.0, 2300.0)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(0.10)
    assert term.venue() == [(1001, 0.1, 2180.0, 2300.0)] and pos.data["legs"][0]["ticket"] == 1001
    assert pos.data["unprotected"] is False
    pos = await b.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and "unconfirmed" not in pos.data["legs"][0]
    moved = await b.update_stop_loss(pos, 2200.0)               # the trader's "stop to entry" reaches the venue
    assert term.venue() == [(1001, 0.1, 2200.0, 2300.0)] and moved.stop_loss == 2200.0
    closed = await b.close_position(moved, 1.0)                 # ... and so does "kapattık"
    assert closed.status is PositionStatus.CLOSED and term.venue() == []
    assert closed.realized_pnl == pytest.approx(sum(d.profit for d in term.deals))


async def test_a1_mt5_history_before_the_position_is_open_until_the_position_shows() -> None:
    term = RacyTerminal()
    b = await mt5(term)
    pos = await resting_buy_limit(b, tmt)
    term.arm(pos.data["legs"][0]["order_ticket"], "history_first")
    pos = await b.sync_position(pos)
    # 62e0719: status cancelled (closed/unknown leg), and never looked at again
    (leg,) = pos.data["legs"]
    assert pos.status is PositionStatus.OPEN and leg["unconfirmed"] and leg["open_volume"] == pytest.approx(0.10)
    assert leg["sl"] == 2180.0                                  # the order's stop: no repair of an unseen position
    assert term.sent_actions(MT5.TRADE_ACTION_SLTP) == [] and term.sent_actions(MT5.TRADE_ACTION_DEAL) == []
    term.complete()
    pos = await b.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and "unconfirmed" not in pos.data["legs"][0]
    assert pos.data["legs"][0]["sl"] == 2180.0


async def test_a1_mt5_a_fill_that_never_shows_is_closed_only_after_the_miss_rule() -> None:
    term = RacyTerminal()
    b = await mt5(term)
    pos = await resting_buy_limit(b, tmt)
    term.arm(pos.data["legs"][0]["order_ticket"], "history_first")
    states = []
    for _ in range(_MAX_MISSES + 1):
        pos = await b.sync_position(pos)
        states.append(pos.status.value)
    assert states == ["open"] * _MAX_MISSES + ["closed"]
    assert pos.data["legs"][0]["reason"] == "unknown"


async def test_a1_mt5_filled_and_stopped_out_between_two_passes_is_closed_with_its_pnl() -> None:
    term = RacyTerminal()
    b = await mt5(term)
    pos = await resting_buy_limit(b, tmt)
    oid = pos.data["legs"][0]["order_ticket"]
    term.fill(oid)
    term.hit(oid, MT5.DEAL_REASON_SL, 2180.0)
    pos = await b.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["legs"][0]["reason"] == "sl"
    assert pos.realized_pnl == pytest.approx((2180.0 - 2200.0) * 0.10)


async def test_a1_mt5_cancelled_order_is_still_cancelled() -> None:
    term = RacyTerminal()
    b = await mt5(term)
    pos = await resting_buy_limit(b, tmt)
    o = term.orders.pop(pos.data["legs"][0]["order_ticket"])
    o.state = MT5.ORDER_STATE_CANCELED
    term.history_orders[o.ticket] = o
    pos = await b.sync_position(pos)
    assert pos.status is PositionStatus.CANCELLED


async def test_a1_metaapi_fill_between_the_two_round_trips_is_an_open_managed_position() -> None:
    conn = RacyConn()
    b = await tma.connected(conn)
    pos = await resting_buy_limit(b, tma)
    conn.armed = pos.data["legs"][0]["order_ticket"]
    pos = await b.sync_position(pos)
    # 62e0719: status closed [('closed', 'manual', 0.0, 0.0)] while the venue holds position 70001
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(0.10)
    moved = await b.update_stop_loss(pos, 2200.0)
    assert conn.positions["70001"]["stopLoss"] == 2200.0 and moved.stop_loss == 2200.0
    closed = await b.close_position(moved, 1.0)
    assert closed.status is PositionStatus.CLOSED and conn.positions == {}


async def test_a1_an_open_leg_whose_last_closing_deal_lags_is_not_booked_closed_on_the_partial_deals() -> None:
    """_refresh_open, the same rule: a position gone from the venue is booked closed only when its deals cover
    every lot; while the newest closing deal is missing from the history it counts as a miss."""

    class LaggingDeals(tmt.FakeMT5):
        hide: Any = None

        def history_deals_get(self, ticket: Any = None, position: Any = None) -> Any:
            out = super().history_deals_get(ticket=ticket, position=position)
            return tuple(d for d in out if d.ticket != self.hide)

    term = LaggingDeals()
    b = await mt5(term)
    sym = await b.resolve_symbol("ETH")
    pos = await b.open_position(tmt.make_req(sym, 0.10, tmt.make_plan()))
    ticket = pos.data["legs"][0]["ticket"]
    term.ticks["ETHUSD"].bid, term.ticks["ETHUSD"].ask = 2250.0, 2250.2
    pos = await b.close_position(pos, 0.5)                      # the bot closes half
    term.hit(ticket, MT5.DEAL_REASON_SL, 2205.11)               # the venue stop takes the rest ...
    term.hide = term.deals[-1].ticket                           # ... and that deal is not in the history yet
    pos = await b.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["legs"][0]["misses"] == 1   # 62e0719: closed, 'manual'
    term.hide = None
    pos = await b.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["legs"][0]["reason"] == "sl"
    assert pos.realized_pnl == pytest.approx(sum(d.profit for d in term.deals))


# --------------------------------------------------------------------------- #
# A2  realized PnL after the bot closes a leg in two or more steps
# --------------------------------------------------------------------------- #
from types import SimpleNamespace  # noqa: E402

ENTRY, STOP, TARGET = 1.10872, 1.11005, 1.10654             # the "Eu short" of 2026-11-12


def eurusd_terminal(commission: float = 0.0, cls: type = tmt.FakeMT5) -> Any:
    f = cls(strict=True)
    f.symbols = {"EURUSD": tmt.make_symbol(
        "EURUSD", digits=5, point=1e-5, trade_tick_size=1e-5, trade_contract_size=100_000.0,
        currency_base="EUR", currency_profit="USD", currency_margin="EUR")}
    f.ticks = {}
    f.commission_per_lot = commission
    px(f, ENTRY, ENTRY + 0.00002)
    return f


def px(f: Any, bid: float, ask: float) -> None:
    f.ticks["EURUSD"] = SimpleNamespace(bid=bid, ask=ask, last=0.0, time=tmt.SERVER_TIME)


def venue_realized(f: Any, ticket: int) -> float:
    return sum(d.profit + d.commission + d.swap + d.fee for d in f.deals if d.position_id == ticket)


async def eu_short(f: Any, lots: float = 0.75) -> tuple[Mt5Broker, Any]:
    b = await tmt.connected(f)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX, "EURUSD")
    plan = TradePlan(plan_id="p1", base_asset="EUR", quote_asset="USD", market_type=MarketType.FOREX,
                     side=Side.SHORT, entry_type=EntryType.MARKET, entry_price=ENTRY, stop_loss=STOP,
                     take_profits=[TakeProfit(price=TARGET)], risk_r=1.0, risk_pct=1.0, source_chat_id=-100123,
                     source_message_ids=[1], created_at=NOW)
    req = OpenRequest(plan=plan, symbol=sym, quantity=lots, entry_type=EntryType.MARKET, entry_price=None,
                      stop_loss=STOP, take_profits=plan.take_profits, client_tag="ctr1a2")
    return b, await b.open_position(req)


async def test_a2_mt5_half_then_the_rest_closed_by_the_bot() -> None:
    f = eurusd_terminal()
    b, pos = await eu_short(f)
    ticket = pos.data["legs"][0]["ticket"]
    px(f, 1.10748, 1.10750)                                 # 12.2 pips in profit
    half = await b.close_position(pos, 0.5)                 # "yarısını kapattık"
    assert half.realized_pnl == pytest.approx(venue_realized(f, ticket), abs=0.01)
    px(f, 1.10948, 1.10950)                                 # 7.8 pips against now
    rest = await b.close_position(half, 1.0)                # "kapattık"
    assert rest.status is PositionStatus.CLOSED
    # 62e0719: 45.14 (the first half only) while the venue's deals sum to 15.50, for good
    assert rest.realized_pnl == pytest.approx(venue_realized(f, ticket), abs=0.01)
    assert round(venue_realized(f, ticket), 2) == 15.5
    for _ in range(3):
        rest = await b.sync_position(rest)
    assert rest.realized_pnl == pytest.approx(15.5, abs=0.01)


async def test_a2_mt5_two_partials_then_the_rest_with_commission() -> None:
    f = eurusd_terminal(commission=7.0)
    b, pos = await eu_short(f)
    ticket = pos.data["legs"][0]["ticket"]
    px(f, 1.10748, 1.10750)
    p1 = await b.close_position(pos, 0.5)                   # 0.37 lots
    assert p1.realized_pnl == pytest.approx(venue_realized(f, ticket), abs=0.01)
    px(f, 1.10698, 1.10700)
    p2 = await b.sync_position(await b.close_position(p1, 0.25))   # 0.18 lots more
    # 62e0719: 37.30 against the venue's 67.00
    assert p2.realized_pnl == pytest.approx(venue_realized(f, ticket), abs=0.01)
    px(f, 1.10898, 1.10900)
    p3 = await b.sync_position(await b.close_position(p2, 1.0))
    assert p3.status is PositionStatus.CLOSED
    assert p3.realized_pnl == pytest.approx(venue_realized(f, ticket), abs=0.01)


async def test_a2_mt5_control_partial_then_the_venue_stop() -> None:
    f = eurusd_terminal()
    b, pos = await eu_short(f)
    ticket = pos.data["legs"][0]["ticket"]
    px(f, 1.10748, 1.10750)
    half = await b.close_position(pos, 0.5)
    f.hit(ticket, MT5.DEAL_REASON_SL, STOP)
    after = await b.sync_position(half)
    assert after.status is PositionStatus.CLOSED and after.data["legs"][0]["reason"] == "sl"
    assert after.realized_pnl == pytest.approx(venue_realized(f, ticket), abs=0.01)


class LaggingHistory(tmt.FakeMT5):
    """The newest closing deal is missing from the FIRST history query after it was made."""

    hide: Any = None

    def _close(self, p: Any, volume: float, price: float, reason: int) -> None:
        super()._close(p, volume, price, reason)
        self.hide = self.deals[-1].ticket

    def history_deals_get(self, ticket: Any = None, position: Any = None) -> Any:
        out = super().history_deals_get(ticket=ticket, position=position)
        if self.hide is not None:
            out, self.hide = tuple(d for d in out if d.ticket != self.hide), None
        return out


async def test_a2_mt5_a_lagging_deal_history_does_not_freeze_a_partial_number() -> None:
    f = eurusd_terminal(cls=LaggingHistory)
    b, pos = await eu_short(f)
    ticket = pos.data["legs"][0]["ticket"]
    px(f, 1.10748, 1.10750)
    half = await b.sync_position(await b.close_position(pos, 0.5))
    px(f, 1.10948, 1.10950)
    rest = await b.close_position(half, 1.0)
    for _ in range(3):
        rest = await b.sync_position(rest)
    assert rest.realized_pnl == pytest.approx(venue_realized(f, ticket), abs=0.01)
    assert round(venue_realized(f, ticket), 2) == 15.5


@pytest.mark.parametrize("commission", [0.0, 3.0])
async def test_a2_metaapi_half_then_the_rest_and_two_partials(commission: float) -> None:
    conn = tma.FakeConn()
    conn.commission_per_lot = commission
    b = await tma.connected(conn)
    sym = await b.resolve_symbol("ETH")
    pos = await b.open_position(tma.make_req(sym, 1.0, tma.make_plan()))
    pid = pos.data["legs"][0]["ticket"]

    def venue() -> float:
        return sum(d["profit"] + d["commission"] + d["swap"] for d in conn.deals if d["positionId"] == pid)

    conn.prices["ETHUSD"].update(bid=2242.95, ask=2243.15)
    p1 = await b.close_position(pos, 0.5)
    # While the leg is partly open its number is the position's own realizedProfit (this fake leaves commission
    # out of it, unlike the deals): compared only without commission. The final number comes from the deals.
    assert commission or p1.realized_pnl == pytest.approx(venue(), abs=0.01)
    conn.prices["ETHUSD"].update(bid=2236.95, ask=2237.15)
    p2 = await b.sync_position(await b.close_position(p1, 0.25))
    assert commission or p2.realized_pnl == pytest.approx(venue(), abs=0.01)
    conn.prices["ETHUSD"].update(bid=2226.95, ask=2227.15)
    p3 = await b.sync_position(await b.close_position(p2, 1.0))
    # 62e0719: the number read after the first close stands for good
    assert p3.status is PositionStatus.CLOSED and p3.realized_pnl == pytest.approx(venue(), abs=0.01)
    await b.close()


# --------------------------------------------------------------------------- #
# A4 / A5  MetaApi: the friend's link in trouble
# --------------------------------------------------------------------------- #
from copytrader.brokers import metaapi_broker as mb  # noqa: E402
from copytrader.config import MetaApiBrokerConfig  # noqa: E402


class TroubledConn(tma.FakeConn):
    """FakeConn whose requests can hang (``hang``: names, "*" = every request) or answer late (``slow``: name ->
    seconds), and whose SDK state ``synchronized`` can be set (None: the SDK object is not there)."""

    def __init__(self) -> None:
        super().__init__()
        self.hang: set[str] = set()
        self.slow: dict[str, float] = {}
        self.synchronized: bool | None = None
        self.started: list[str] = []                # every request made (also those that never get an answer)
        self.done: list[str] = []                   # requests that got their answer
        self._never = asyncio.Event()

    @property
    def _meta_api_connection(self) -> Any:
        if self.synchronized is None:
            raise AttributeError("_meta_api_connection")
        return SimpleNamespace(is_synchronized=lambda: self.synchronized)

    def __getattribute__(self, name: str) -> Any:
        attr = object.__getattribute__(self, name)
        if name.startswith("_") or name in ("connect", "close") or not asyncio.iscoroutinefunction(attr):
            return attr                             # the connection's own life cycle is not a request
        hang, slow, done, started = (object.__getattribute__(self, n) for n in ("hang", "slow", "done", "started"))
        never = object.__getattribute__(self, "_never")

        async def request(*args: Any, **kwargs: Any) -> Any:
            started.append(name)
            if name in hang or "*" in hang:
                await never.wait()
            if name in slow:
                await asyncio.sleep(slow[name])
            result = await attr(*args, **kwargs)
            done.append(name)
            return result

        return request


@pytest.fixture
def short_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Production 30 s / 3 s / 120 s, scaled down; the asserted bounds keep the same order."""
    monkeypatch.setattr(mb, "READ_TIMEOUT_SEC", 1.0)
    monkeypatch.setattr(mb, "DEGRADED_READ_TIMEOUT_SEC", 0.1, raising=False)     # new in Review 1
    monkeypatch.setattr(mb, "RPC_TIMEOUT_SEC", 2.0)
    monkeypatch.setattr(mb, "HEALTH_TIMEOUT_SEC", 0.2)
    monkeypatch.setattr(mb, "HEALTH_SYNC_GRACE_SEC", 0.05)


async def tickmill(conn: TroubledConn) -> MetaApiBroker:
    b = MetaApiBroker(MetaApiBrokerConfig(type="metaapi", name="tickmill", token="t", account_id="a"),
                      connection=conn)
    b.retry_delay_sec = 0
    b.set_risk_config(RiskConfig(include_fees_in_risk=False))
    await b.connect()
    return b


async def timed(coro: Any) -> tuple[float, Any]:
    t0 = time.monotonic()
    try:
        result: Any = await coro
    except BrokerError as exc:
        result = exc
    return time.monotonic() - t0, result


async def test_a5_after_a_read_got_no_answer_the_next_reads_give_up_after_the_short_limit(short_limits: None) -> None:
    conn = TroubledConn()
    b = await tickmill(conn)
    sym = await b.resolve_symbol("ETH")
    pos = await b.open_position(tma.make_req(sym, 0.05, tma.make_plan()))
    conn.hang = {"*"}                                       # MetaApi stops answering
    first, err = await timed(b.sync_position(pos))          # nothing known yet: the full read limit
    assert isinstance(err, BrokerError) and "1 sn içinde yanıt gelmedi" in str(err) and first >= 0.95
    for call in (b.sync_position(pos), b.get_quote(sym), b.get_equity(), b._deal_summary("1")):
        took, err = await timed(call)                       # 62e0719: the full read limit, every time
        assert isinstance(err, BrokerError) and "0.1 sn içinde yanıt gelmedi" in str(err), err
        assert took < 0.5, took
    conn.hang = set()                                       # answers again, at once: the first answer ends it
    assert (await b.sync_position(pos)).status is PositionStatus.OPEN and b._stale is None
    conn.hang = {"*"}
    await timed(b.sync_position(pos))
    conn.hang = set()
    conn.slow = {"get_positions": 0.3}                      # answers again, but later than the short limit
    took, err = await timed(b.sync_position(pos))
    assert isinstance(err, BrokerError) and took < 0.5      # nothing answered yet: still the short limit
    assert await b.health_problem() is None                 # the heartbeat's check (its own limit) is answered
    assert b._stale is None
    took, synced = await timed(b.sync_position(pos))        # the full read limit again
    assert synced.status is PositionStatus.OPEN and took >= 0.3
    await b.close()


async def test_a5_the_sdk_reporting_the_link_down_shortens_reads_at_once(short_limits: None) -> None:
    conn = TroubledConn()
    b = await tickmill(conn)
    sym = await b.resolve_symbol("ETH")
    conn.synchronized = False
    conn.hang = {"get_symbol_price"}
    took, err = await timed(b.get_quote(sym))
    assert isinstance(err, BrokerError) and "0.1 sn içinde yanıt gelmedi (MetaApi bağlantısı kopuk görünüyor)" in str(err)
    assert took < 0.5
    await b.close()


@pytest.mark.parametrize("call", ["stop", "tp", "close", "partial", "cancel"])
async def test_a5_management_while_the_account_does_not_answer_fails_fast_and_sends_nothing(
        short_limits: None, call: str) -> None:
    conn = TroubledConn()
    b = await tickmill(conn)
    sym = await b.resolve_symbol("ETH")
    pos = await b.open_position(tma.make_req(sym, 0.10, tma.make_plan()))
    pend = await b.open_position(tma.make_req(sym, 0.05, tma.make_plan(entry_type=EntryType.LIMIT, entry=2200.0,
                                                                       sl=2180.0), tag="ctpend0001"))
    conn.hang = {"*"}
    await timed(b.get_quote(sym))                           # a read got no answer (e.g. the engine's live quote)
    sent_before = len(conn.started)
    run = {
        "stop": lambda: b.update_stop_loss(pos, 2232.18),
        "tp": lambda: b.update_take_profits(pos, [TakeProfit(price=2300.0)]),
        "close": lambda: b.close_position(pos, 1.0),
        "partial": lambda: b.close_position(pos, 0.5),
        "cancel": lambda: b.cancel_entry(pend),
    }[call]
    took, err = await timed(run())
    # 62e0719: each of these waited for its own reads (30 s each in production) and could then wait for its trade
    # request (120 s); here: refused at once, the reason named, nothing sent to the account
    assert took < 0.05, took
    assert isinstance(err, BrokerError)
    text = str(err)
    # the first read of the outage waited the full read limit (1 s here, 30 s live): it is the reason named
    assert text.startswith("tickmill hesabı son isteğe yanıt vermedi (MetaApi ETHUSD fiyatı: 1 sn içinde yanıt gelmedi)")
    assert {"stop": "Stop değiştirilmedi.", "tp": "TP'ler değiştirilmedi.", "close": "Pozisyon kapatılmadı.",
            "partial": "Kısmi kapatma yapılmadı.", "cancel": "Bekleyen giriş iptal edilmedi."}[call] in text
    assert "tickmill hesabındaki pozisyonu MetaTrader'dan kendiniz kontrol edin" in text
    assert conn.started[sent_before:] == []
    await asyncio.sleep(0.15)                               # a moment later: one short look at the account first
    took, err = await timed(run())
    assert isinstance(err, BrokerError) and "bağlantı kontrolü: 0.1 sn içinde yanıt gelmedi" in str(err)
    assert 0.09 < took < 0.5
    assert conn.started[sent_before:] == ["get_account_information"]         # and no trade request
    await b.close()


async def test_a5_management_goes_ahead_when_the_account_answers_again(short_limits: None) -> None:
    conn = TroubledConn()
    b = await tickmill(conn)
    sym = await b.resolve_symbol("ETH")
    pos = await b.open_position(tma.make_req(sym, 0.10, tma.make_plan()))
    conn.hang = {"*"}
    await timed(b.get_quote(sym))
    conn.hang = set()                                       # back, but nothing has read since
    await asyncio.sleep(0.15)
    moved = await b.update_stop_loss(pos, 2232.18)          # the short look succeeds: the stop is moved
    assert conn.positions[pos.data["legs"][0]["ticket"]]["stopLoss"] == 2232.18 and moved.stop_loss == 2232.18
    assert b._stale is None
    await b.close()


async def test_a5_a_trade_request_in_flight_is_never_cut_short(short_limits: None) -> None:
    conn = TroubledConn()
    b = await tickmill(conn)
    sym = await b.resolve_symbol("ETH")
    pos = await b.open_position(tma.make_req(sym, 0.10, tma.make_plan()))
    conn.slow = {"modify_position": 0.6}                    # the answer is on its way (longer than the short limit)
    moving = asyncio.ensure_future(b.update_stop_loss(pos, 2232.18))
    await asyncio.sleep(0.05)
    conn.hang = {"get_symbol_price"}
    await timed(b.get_quote(sym))                           # meanwhile a read gets no answer: the link is degraded
    assert b._degraded()
    moved = await moving                                    # the request in flight still gets its answer
    assert moved.stop_loss == 2232.18 and "modify_position" in conn.done
    await b.close()


# --------------------------------------------------------------------------- #
# A4  refusing NEW signals on tickmill: only on what is known
# --------------------------------------------------------------------------- #
async def own_tickmill(conn: TroubledConn) -> MetaApiBroker:
    b = await tickmill(conn)
    b._owns_conn = True                     # the self-healing parts, as for the connection connect() builds
    return b


async def test_a4_one_late_health_check_does_not_refuse_the_next_signal(short_limits: None) -> None:
    """ILETISIM-6: one check answered late (MetaApi busy for a moment), the link works right after."""
    conn = TroubledConn()
    b = await own_tickmill(conn)
    conn.slow = {"get_account_information": 0.3}            # later than HEALTH_TIMEOUT_SEC (0.2 here, 20 s live)
    problem = await b.health_problem()
    assert problem is not None and "yanıt gelmedi" in problem
    conn.slow = {}
    # 62e0719: "MetaApi şu an yanıt vermiyor (...); bu hesapta yeni işlem açılmadı. Bot bağlantıyı kendisi yeniliyor."
    sym = await b.resolve_symbol("ETH")
    assert sym is not None and await b.get_equity() == 10_000.0
    opened = await b.open_position(tma.make_req(sym, 0.05, tma.make_plan()))
    assert opened.status is PositionStatus.OPEN
    await b.close()


async def test_a4_two_failed_checks_in_a_row_refuse_and_the_text_says_what_is_known(short_limits: None) -> None:
    conn = TroubledConn()
    b = await own_tickmill(conn)
    sym = await b.resolve_symbol("ETH")
    conn.hang = {"*"}
    await b.health_problem()
    await b.health_problem()
    before = len(conn.started)
    took, err = await timed(b.resolve_symbol("BTC"))
    assert took < 0.02 and isinstance(err, BrokerError)
    text = str(err)
    assert text.startswith("MetaApi bağlantısı sorunlu: son 2 bağlantı kontrolü yanıt almadı (MetaApi bağlantı kontrolü:")
    assert "bu hesapta yeni işlem açılmadı" in text and "kendisi yeniliyor" not in text
    assert "Bağlantı kontrolü üst üste 3 kez yanıtsız kalırsa bot bağlantıyı sıfırdan kurar." in text
    for call in (b.get_equity(), b.open_position(tma.make_req(sym, 0.05, tma.make_plan(), tag="ctnew00002"))):
        took, err = await timed(call)
        assert took < 0.02 and isinstance(err, BrokerError) and "yeni işlem açılmadı" in str(err)
    assert conn.started[before:] == []
    await b.close()


async def test_a4_the_sdk_reporting_the_link_down_refuses_after_one_check(short_limits: None) -> None:
    conn = TroubledConn()
    b = await own_tickmill(conn)
    conn.synchronized = False
    assert await b.health_problem() == mb._DISCONNECTED_TR
    took, err = await timed(b.get_equity())
    assert took < 0.02 and isinstance(err, BrokerError)
    assert "MetaApi bu hesabın bağlantısının koptuğunu bildiriyor" in str(err) and "yeni işlem açılmadı" in str(err)
    conn.synchronized = True                                # the SDK reconnected by itself
    assert await b.get_equity() == 10_000.0
    await b.close()


async def test_a4_an_answer_after_the_failed_checks_lets_the_next_signal_try(short_limits: None) -> None:
    conn = TroubledConn()
    b = await own_tickmill(conn)
    sym = await b.resolve_symbol("ETH")
    pos = await b.open_position(tma.make_req(sym, 0.05, tma.make_plan()))
    conn.hang = {"*"}
    await b.health_problem()
    await b.health_problem()
    assert isinstance((await timed(b.get_equity()))[1], BrokerError)
    conn.hang = set()
    assert (await b.sync_position(pos)).status is PositionStatus.OPEN       # e.g. the monitor's next pass
    assert await b.get_equity() == 10_000.0                 # answered after the failed checks: no refusal
    await b.close()


# --------------------------------------------------------------------------- #
# A3  a partly filled pending order: the filled part is never left untracked
# --------------------------------------------------------------------------- #
def part_fill_mt5(term: tmt.FakeMT5, oid: int, lots: float) -> None:
    """The venue fills ``lots`` of the resting order; the rest keeps resting under the same ticket."""
    o = term.orders[oid]
    term._open(oid, o.symbol, MT5.ORDER_TYPE_BUY, lots, o.price_open, o.sl, o.tp, o.magic, o.comment)
    o.volume_current = round(o.volume_current - lots, 8)


@pytest.mark.parametrize("how", ["close", "cancel"])
@pytest.mark.parametrize("synced", [True, False])
async def test_a3_mt5_close_or_cancel_of_a_partly_filled_order_closes_the_filled_part(how: str, synced: bool) -> None:
    term = tmt.FakeMT5()
    b = await mt5(term)
    pos = await resting_buy_limit(b, tmt)
    oid = pos.data["legs"][0]["order_ticket"]
    part_fill_mt5(term, oid, 0.04)
    if synced:                                  # the monitor saw the partial fill (else: the close itself does)
        pos = await b.sync_position(pos)
        assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(0.04)
    term.ticks["ETHUSD"].bid, term.ticks["ETHUSD"].ask = 2210.0, 2210.2
    done = await (b.close_position(pos, 1.0) if how == "close" else b.cancel_entry(pos))
    # 62e0719: status cancelled, error None, and the venue still holds (1001, 0.04) with nobody managing it
    assert term.positions == {} and term.orders == {}
    assert done.status is PositionStatus.CLOSED and done.filled_quantity == pytest.approx(0.04)
    assert done.realized_pnl == pytest.approx(sum(d.profit for d in term.deals)) and done.error is None
    # the rest was removed BEFORE the filled part was closed: it cannot fill after the close
    removes = [i for i, r in enumerate(term.sent) if r["action"] == MT5.TRADE_ACTION_REMOVE]
    closes = [i for i, r in enumerate(term.sent) if r["action"] == MT5.TRADE_ACTION_DEAL and "position" in r]
    assert len(removes) == 1 and len(closes) == 1 and removes[0] < closes[0]


async def test_a3_mt5_a_part_that_fills_while_the_order_is_removed_is_closed_and_told() -> None:
    term = tmt.FakeMT5()
    b = await mt5(term)
    pos = await resting_buy_limit(b, tmt)
    oid = pos.data["legs"][0]["order_ticket"]

    def fill_part_first(request: dict[str, Any]) -> None:       # 0.04 fills just before the removal arrives
        if request["action"] == MT5.TRADE_ACTION_REMOVE and oid in term.orders:
            part_fill_mt5(term, oid, 0.04)
        return None

    term.hooks.append(fill_part_first)
    done = await b.cancel_entry(pos)
    assert term.positions == {} and term.orders == {}
    assert done.status is PositionStatus.CLOSED and done.filled_quantity == pytest.approx(0.04)
    assert done.error == "Bekleyen emrin 0.04 lotluk kısmı dolmuştu; o kısım piyasa fiyatından kapatıldı."


async def test_a3_mt5_a_filled_part_that_cannot_be_closed_stays_an_open_managed_leg() -> None:
    term = tmt.FakeMT5()
    b = await mt5(term)
    pos = await resting_buy_limit(b, tmt)
    oid = pos.data["legs"][0]["order_ticket"]
    part_fill_mt5(term, oid, 0.04)
    term.hooks.append(lambda r: 10018 if r["action"] == MT5.TRADE_ACTION_DEAL and "position" in r else None)  # closed
    done = await b.close_position(pos, 1.0)
    assert term.orders == {} and [(t, p.volume) for t, p in term.positions.items()] == [(oid, 0.04)]
    assert done.status is PositionStatus.OPEN and done.remaining_quantity == pytest.approx(0.04)
    assert done.error is not None and done.error.startswith("Bazı bacaklar kapatılamadı: #1001")
    term.hooks.clear()
    moved = await b.update_stop_loss(done, 2200.0)              # it is managed: the stop move reaches it
    assert term.positions[oid].sl == 2200.0 and moved.stop_loss == 2200.0


async def test_a3_mt5_while_the_rest_rests_the_filled_part_is_managed() -> None:
    term = tmt.FakeMT5()
    b = await mt5(term)
    pos = await resting_buy_limit(b, tmt)
    oid = pos.data["legs"][0]["order_ticket"]
    part_fill_mt5(term, oid, 0.04)
    pos = await b.sync_position(pos)
    # 62e0719: "pending", filled 0: the engine skipped a stop move ("giriş henüz dolmadı") and saw no position
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(0.04)
    (leg,) = pos.data["legs"]
    assert (leg["rest_order"], leg["rest_volume"]) == (oid, pytest.approx(0.06))
    term.ticks["ETHUSD"].bid, term.ticks["ETHUSD"].ask = 2230.0, 2230.2
    pos = await b.update_stop_loss(pos, 2190.0)                 # both parts get the new stop
    assert term.positions[oid].sl == 2190.0 and term.orders[oid].sl == 2190.0 and pos.error is None
    pos = await b.update_take_profits(pos, [TakeProfit(price=2290.0)])
    assert term.positions[oid].tp == 2290.0 and term.orders[oid].tp == 2290.0 and pos.error is None
    pos = await b.update_stop_loss(pos, 2200.0)                 # stop to entry: the rest cannot rest with it
    assert term.positions[oid].sl == 2200.0 and term.orders == {}
    assert pos.error == "Emrin dolmamış kalan kısmı (0.06 lot) yeni stopla bekleyemeyeceği için iptal edildi."
    assert "rest_order" not in pos.data["legs"][0] and pos.stop_loss == 2200.0


async def test_a3_mt5_the_rest_filling_later_joins_the_tracked_position() -> None:
    term = tmt.FakeMT5()
    b = await mt5(term)
    pos = await resting_buy_limit(b, tmt)
    oid = pos.data["legs"][0]["order_ticket"]
    part_fill_mt5(term, oid, 0.04)
    pos = await b.sync_position(pos)
    o = term.orders.pop(oid)                                    # the rest fills into the same position
    term.positions[oid].volume = 0.10
    o.state, o.position_id = MT5.ORDER_STATE_FILLED, oid
    term.history_orders[oid] = o
    pos = await b.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(0.10)
    assert "rest_order" not in pos.data["legs"][0] and pos.filled_quantity == pytest.approx(0.10)


async def test_a3_mt5_a_rest_left_after_the_filled_part_closed_is_cancelled_and_told() -> None:
    term = tmt.FakeMT5()
    b = await mt5(term)
    pos = await resting_buy_limit(b, tmt)
    oid = pos.data["legs"][0]["order_ticket"]
    part_fill_mt5(term, oid, 0.04)
    pos = await b.sync_position(pos)
    term.hit(oid, MT5.DEAL_REASON_TP, 2300.0)                   # the filled part takes its target
    pos = await b.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["legs"][0]["reason"] == "tp"
    assert term.orders == {} and pos.error == "Emrin dolmuş kısmı kapandı; dolmamış kalan kısmı (0.06 lot) iptal edildi."
    assert pos.realized_pnl == pytest.approx((2300.0 - 2200.0) * 0.04)


async def test_a3_mt5_rest_cancelled_after_the_filled_part_closed_books_the_filled_part() -> None:
    term = tmt.FakeMT5()
    b = await mt5(term)
    pos = await resting_buy_limit(b, tmt)
    oid = pos.data["legs"][0]["order_ticket"]
    part_fill_mt5(term, oid, 0.04)
    term.hit(oid, MT5.DEAL_REASON_SL, 2180.0)                   # the filled part is stopped out ...
    o = term.orders.pop(oid)                                    # ... and the rest expires on the venue
    o.state = MT5.ORDER_STATE_EXPIRED
    term.history_orders[oid] = o
    pos = await b.sync_position(pos)
    # 62e0719: "cancelled" (expired), its loss never booked
    assert pos.status is PositionStatus.CLOSED and pos.data["legs"][0]["reason"] == "sl"
    assert pos.realized_pnl == pytest.approx((2180.0 - 2200.0) * 0.04)


async def test_a3_metaapi_close_of_a_partly_filled_order_closes_the_filled_part() -> None:
    conn = tma.FakeConn()
    b = await tma.connected(conn)
    pos = await resting_buy_limit(b, tma)
    oid = pos.data["legs"][0]["order_ticket"]
    o = conn.orders[oid]
    conn._open(oid, o["symbol"], True, 0.04, o["openPrice"], o.get("stopLoss"), o.get("takeProfit"),
               {"magic": o["magic"], "clientId": o.get("clientId")})
    o["currentVolume"] = 0.06
    pos = await b.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["legs"][0]["rest_order"] == oid
    pos = await b.update_stop_loss(pos, 2190.0)
    assert conn.positions[oid]["stopLoss"] == 2190.0 and conn.orders[oid]["stopLoss"] == 2190.0
    done = await b.close_position(pos, 1.0)
    assert conn.positions == {} and conn.orders == {}
    assert done.status is PositionStatus.CLOSED and done.filled_quantity == pytest.approx(0.04) and done.error is None
    await b.close()


async def test_a3_mt5_cancel_of_an_unfilled_order_is_still_a_plain_cancel() -> None:
    term = tmt.FakeMT5()
    b = await mt5(term)
    pos = await resting_buy_limit(b, tmt)
    done = await b.cancel_entry(pos)
    assert done.status is PositionStatus.CANCELLED and done.error is None and term.orders == {}


async def test_a5_a_symbol_read_without_answer_is_not_reported_as_symbol_not_found(short_limits: None) -> None:
    """YONETIM-2 (incidental): a timed-out read of a symbol_map symbol was told as "sembolü ... bulunamadı"."""
    conn = TroubledConn()
    b = MetaApiBroker(MetaApiBrokerConfig(type="metaapi", name="tickmill", token="t", account_id="a",
                                          symbol_map={"ETH": "ETHUSD"}), connection=conn)
    b.set_risk_config(RiskConfig())
    await b.connect()
    conn.hang = {"get_symbol_specification"}
    took, err = await timed(b.resolve_symbol("ETH"))
    assert isinstance(err, BrokerError) and "bulunamadı" not in str(err)
    assert str(err).startswith("'ETHUSD' sembol bilgisi tickmill hesabından okunamadı (MetaApi ETHUSD sembol bilgisi:")
    conn.hang = set()
    conn.specs.pop("ETHUSD")
    conn.fail["get_symbol_specification"] = [KeyError("ETHUSD")]  # an answer: the symbol is not there
    took, err = await timed(b.resolve_symbol("ETH"))
    assert isinstance(err, BrokerError) and "symbol_map'teki 'ETHUSD' sembolü tickmill hesabında bulunamadı" in str(err)
    await b.close()


async def test_a3_mt5_the_monitor_expires_only_the_resting_rest_of_a_partly_filled_entry() -> None:
    """The engine's existing rule for a partly filled limit entry (monitor._expire_remainder): after
    pending_entry_expiry_min only the rest is cancelled; the filled part stays open with its stop."""
    from copytrader.engine.monitor import entry_remainder_resting

    term = tmt.FakeMT5()
    b = await mt5(term)
    pos = await resting_buy_limit(b, tmt)
    oid = pos.data["legs"][0]["order_ticket"]
    assert not entry_remainder_resting(pos)
    part_fill_mt5(term, oid, 0.04)
    pos = await b.sync_position(pos)
    assert entry_remainder_resting(pos)                         # 62e0719: "pending", the monitor saw no fill
    done = await b.cancel_entry_remainder(pos)
    assert term.orders == {} and [(t, p.volume, p.sl) for t, p in term.positions.items()] == [(oid, 0.04, 2180.0)]
    assert done.status is PositionStatus.OPEN and not entry_remainder_resting(done) and done.error is None
