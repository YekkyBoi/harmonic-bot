"""The closing headline never announces a result in R from a realized_pnl the ADAPTER itself knows is partial.

The real CcxtBroker (on the fake Binance REST transport) and the real Mt5Broker (on the fake terminal), then the
text the monitor / the engine would send at that moment (monitor.closing_summary_text). Review of the first
version (cases A, B, C): a third taken at TP1 and the rest gone without a known result read "🟢 hooopp +0.1R".
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import ccxt
import pytest

from test_ccxt_binance_real import _connected, _open, _plan
from test_mt5_broker import ASK, MT5, FakeMT5, _server_clock, connected, make_plan, make_req  # noqa: F401 (autouse)

from copytrader.brokers.ccxt_broker import PNL_INCOMPLETE
from copytrader.engine.monitor import INITIAL_RISK, closing_summary_text, position_result_r, rollup_status
from copytrader.engine.orchestrator import initial_risk_basis
from copytrader.models import BrokerPosition, PositionStatus, Side, SizingResult, TradeRecord, TradeStatus

NOW = datetime(2026, 11, 14, tzinfo=UTC)
BTC_MONEY = "🏁 #1 BTC LONG (giriş ?) kapandı. Gerçekleşen K/Z: binance "
ETH_MONEY = "🏁 #1 ETH LONG (giriş 2232.18) kapandı. Gerçekleşen K/Z: mt5demo "


def record(plan: Any) -> TradeRecord:
    return TradeRecord(id=1, plan=plan, status=TradeStatus.OPEN, current_stop_loss=plan.stop_loss, created_at=NOW,
                       updated_at=NOW)


def notice(plan: Any, pos: BrokerPosition) -> str:
    """What Monitor._rollup (or the engine's flow that ends the trade) sends for the trade now."""
    status = rollup_status([pos])
    assert status is TradeStatus.CLOSED
    return closing_summary_text(record(plan), [pos], status)


def booked(pos: BrokerPosition, size: SizingResult, entry: float, stop: float) -> None:
    """What Engine._open_on_broker writes on the position it opened."""
    pos.risk_amount = size.risk_amount
    basis = initial_risk_basis(size, 1.0, "USDT", "USDT", entry, entry, stop)
    assert basis is not None
    pos.data[INITIAL_RISK] = basis


# --------------------------------------------------------------------------- #
# binance (ccxt)
# --------------------------------------------------------------------------- #
async def binance_after_tp1() -> tuple[Any, Any, Any, BrokerPosition]:
    """BTC long, three targets; TP1 (a third) has filled through the bot's own order."""
    broker, fake = await _connected()
    plan = _plan("BTC", Side.LONG, 73300.0, [(74500.0, 1 / 3), (76000.0, 1 / 3), (77000.0, 1 / 3)])
    size, pos = await _open(broker, plan, risk=10.0)
    booked(pos, size, float(pos.avg_entry_price or 0.0), 73300.0)
    fake.price["BTCUSDT"] = (74510.0, 74510.1)
    fake.trigger(pos.data["tp_order_ids"][0]["id"], price=74505.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(0.008)
    return broker, fake, plan, pos


async def test_a_the_rest_closed_outside_the_bots_orders_gives_no_headline() -> None:
    broker, fake, plan, pos = await binance_after_tp1()
    left = pos.remaining_quantity
    fake.pos["BTCUSDT"]["amt"] = 0.0            # the rest is gone on the exchange, not by an order of the bot
    pos = await broker.sync_position(pos)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_outside"] == pytest.approx(left)
    assert "kâr/zarar yalnızca botun kendi emirlerini içerir" in (pos.error or "")
    assert pos.realized_pnl is not None and pos.realized_pnl > 0        # the TP1 third only
    assert position_result_r(pos, 1.0) is None
    assert notice(plan, pos) == BTC_MONEY + f"{pos.realized_pnl:+.2f}"   # the money line as before, no "hooopp"
    await broker.close()


async def test_a_control_the_rest_closed_by_the_bots_own_stop_has_its_headline() -> None:
    broker, fake, plan, pos = await binance_after_tp1()
    fake.price["BTCUSDT"] = (73290.0, 73290.1)
    fake.trigger(pos.data["sl_order_id"], price=73300.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and not pos.data.get("closed_outside")
    assert PNL_INCOMPLETE not in pos.data
    # a third at +0.7R of the stop distance, two thirds at the stop: about -0.5R with the fees
    headline, money = notice(plan, pos).splitlines()
    assert headline == "🔴 amk -0.5R yedik" and money == BTC_MONEY + f"{pos.realized_pnl:+.2f}"
    await broker.close()


async def test_the_exchanges_trade_list_not_covering_the_bots_exits_gives_no_headline() -> None:
    """ccxt_broker._realized_pnl sums the exit trades the exchange lists: when they hold less than the exits the
    adapter booked (here the TP1 fill is missing from the list), the sum is a part of the result."""
    broker, fake, plan, pos = await binance_after_tp1()
    tp1 = [t for t in fake.trades if t["side"] == "SELL"]
    assert len(tp1) == 1
    fake.trades.remove(tp1[0])
    fake.price["BTCUSDT"] = (73290.0, 73290.1)
    fake.trigger(pos.data["sl_order_id"], price=73300.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data[PNL_INCOMPLETE] is True
    assert position_result_r(pos, 1.0) is None
    assert notice(plan, pos) == BTC_MONEY + f"{pos.realized_pnl:+.2f}"
    await broker.close()


async def test_an_exit_without_a_price_left_out_of_the_estimate_gives_no_headline() -> None:
    """The trade list cannot be read: the result is estimated from the booked exits, and an exit whose price is
    not known is not in that estimate."""
    broker, fake, plan, pos = await binance_after_tp1()
    pos.data["exits"][0]["price"] = None
    fake.fail(lambda m, p, q: p == "/fapi/v1/userTrades", ccxt.NetworkError("binanceusdm GET userTrades"), times=100)
    fake.price["BTCUSDT"] = (73290.0, 73290.1)
    fake.trigger(pos.data["sl_order_id"], price=73300.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data[PNL_INCOMPLETE] is True
    assert pos.realized_pnl is not None and position_result_r(pos, 1.0) is None
    assert notice(plan, pos) == BTC_MONEY + f"{pos.realized_pnl:+.2f}"
    await broker.close()


async def test_an_estimate_from_every_booked_exit_is_a_complete_result() -> None:
    broker, fake, plan, pos = await binance_after_tp1()
    fake.fail(lambda m, p, q: p == "/fapi/v1/userTrades", ccxt.NetworkError("binanceusdm GET userTrades"), times=100)
    fake.price["BTCUSDT"] = (73290.0, 73290.1)
    fake.trigger(pos.data["sl_order_id"], price=73300.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and PNL_INCOMPLETE not in pos.data
    assert notice(plan, pos).splitlines()[0] == "🔴 amk -0.6R yedik"       # (the estimate assumes higher fees)
    await broker.close()


# --------------------------------------------------------------------------- #
# MetaTrader (fx / tickmill): a leg whose closing deals are not in the terminal's history yet
# --------------------------------------------------------------------------- #
def hide_out_deals(fake: FakeMT5, tickets: set[int]) -> dict[str, bool]:
    """The terminal's deal history does not show the closing deals of ``tickets`` yet (state["hidden"])."""
    state = {"hidden": True}
    real = fake.history_deals_get

    def lagging(ticket: int | None = None, position: int | None = None) -> Any:
        deals = real(ticket=ticket, position=position)
        if not state["hidden"]:
            return deals
        return tuple(d for d in deals if not (d.position_id in tickets and d.entry == MT5.DEAL_ENTRY_OUT))

    fake.history_deals_get = lagging                        # type: ignore[method-assign]
    return state


async def mt5_after_tp1(fake: FakeMT5) -> tuple[Any, Any, BrokerPosition, list[int]]:
    """ETH long 0.9 lot in three legs; the TP1 leg (a third) has closed with a known result."""
    broker = await connected(fake)
    sym = await broker.resolve_symbol("ETH")
    plan = make_plan(tps=((2245.0, 1 / 3), (2290.0, 1 / 3), (2310.6, 1 / 3)))
    at_stop = 0.9 * (ASK - 2205.11)                         # what 0.9 lot loses at the stop
    pos = await broker.open_position(make_req(sym, 0.9, plan))
    booked(pos, SizingResult(quantity=0.9, risk_amount=30.0, est_loss_at_sl=at_stop, notional=0.9 * ASK), ASK, 2205.11)
    tickets = [leg["ticket"] for leg in pos.data["legs"]]
    fake.hit(tickets[0], MT5.DEAL_REASON_TP, 2245.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.realized_pnl == pytest.approx((2245.0 - ASK) * 0.3)
    return broker, plan, pos, tickets


async def test_b_a_stop_out_whose_deals_are_not_in_the_history_yet_gives_no_headline() -> None:
    fake = FakeMT5()
    broker, plan, pos, tickets = await mt5_after_tp1(fake)
    state = hide_out_deals(fake, set(tickets[1:]))
    fake.hit(tickets[1], MT5.DEAL_REASON_SL, 2205.11)
    fake.hit(tickets[2], MT5.DEAL_REASON_SL, 2205.11)
    for _ in range(3):                                      # "vanished without history": closed after 3 misses
        pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED
    assert [leg.get("realized") is None for leg in pos.data["legs"]] == [False, True, True]
    assert position_result_r(pos, 1.0) is None
    assert notice(plan, pos) == ETH_MONEY + f"{pos.realized_pnl:+.2f}"     # (the TP1 leg only: no "hooopp +0.1R")
    state["hidden"] = False                                 # the history catches up: the result is complete
    later = await broker.sync_position(pos)
    assert notice(plan, later).splitlines() == ["🔴 amk -0.5R yedik", ETH_MONEY + f"{later.realized_pnl:+.2f}"]
    await broker.close()


async def test_c_a_close_by_the_bot_whose_deals_are_not_in_the_history_yet_gives_no_headline() -> None:
    fake = FakeMT5()
    broker, plan, pos, tickets = await mt5_after_tp1(fake)
    state = hide_out_deals(fake, set(tickets[1:]))
    tick = fake.ticks["ETHUSD"]
    fake.ticks["ETHUSD"] = SimpleNamespace(bid=2210.0, ask=2210.5, last=0.0, time=tick.time)
    pos = await broker.close_position(pos, 1.0)             # "kapattık" below the entry
    assert pos.status is PositionStatus.CLOSED
    assert position_result_r(pos, 1.0) is None
    assert notice(plan, pos) == ETH_MONEY + f"{pos.realized_pnl:+.2f}"
    state["hidden"] = False
    later = await broker.sync_position(pos)
    assert notice(plan, later).splitlines() == ["🔴 amk -0.4R yedik", ETH_MONEY + f"{later.realized_pnl:+.2f}"]
    await broker.close()


async def test_c_control_a_close_whose_deals_are_in_the_history_has_its_headline() -> None:
    fake = FakeMT5()
    broker, plan, pos, _tickets = await mt5_after_tp1(fake)
    tick = fake.ticks["ETHUSD"]
    fake.ticks["ETHUSD"] = SimpleNamespace(bid=2210.0, ask=2210.5, last=0.0, time=tick.time)
    pos = await broker.close_position(pos, 1.0)
    assert pos.status is PositionStatus.CLOSED
    assert notice(plan, pos).splitlines() == ["🔴 amk -0.4R yedik", ETH_MONEY + f"{pos.realized_pnl:+.2f}"]
    await broker.close()
