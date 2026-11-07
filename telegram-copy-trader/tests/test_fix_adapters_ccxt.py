"""Regression tests for the CCXT adapter audit fixes (M0 partial exits, M3 stale stops, M5 MOVE_TP re-arm)."""

from __future__ import annotations

import copy
from typing import Any

import pytest
from ccxt.base import errors as cx

from copytrader.brokers.base import BrokerError
from copytrader.models import PositionStatus, TakeProfit
from test_ccxt_broker import SYM, TWO_TPS, FakeExchange, make_broker, open_limit, open_market, open_orders_of


def _live_sls(fake: FakeExchange) -> list[dict[str, Any]]:
    return open_orders_of(fake, "stopLossPrice")


def _live_tps(fake: FakeExchange) -> list[dict[str, Any]]:
    return open_orders_of(fake, "takeProfitPrice")


def _partial_market_closes(fake: FakeExchange, fill: float, status: str = "expired") -> None:
    """Every plain reduce-only market order (close / emergency close) executes only ``fill`` and then ends."""
    original = fake.create_order

    async def create_order(symbol: str, type: str, side: str, amount: float,
                           price: float | None = None, params: dict | None = None) -> dict[str, Any]:
        params = dict(params or {})
        is_close = (type == "market" and params.get("reduceOnly")
                    and not (params.get("stopLossPrice") or params.get("takeProfitPrice")))
        if not is_close:
            return await original(symbol, type, side, amount, price, params)
        fake._check("create_order", (symbol, type, side, amount, price, params))
        fake._seq += 1
        oid = str(fake._seq)
        order = {"id": oid, "clientOrderId": params.get("clientOrderId"), "symbol": symbol, "type": type,
                 "side": side, "amount": amount, "price": None, "filled": 0.0, "average": None, "status": "open",
                 "reduceOnly": True, "triggerPrice": None, "stopLossPrice": None, "takeProfitPrice": None,
                 "timestamp": fake.now, "info": {}, "params": params}
        fake.orders[oid] = order
        fake.calls.append(("create_order:ok", (symbol, type, side, amount, price, params, oid)))
        fake.fill(oid, amount=min(fill, amount), price=fake.ticker["bid"] if side == "sell" else fake.ticker["ask"])
        order["status"] = status
        return copy.deepcopy(order)

    fake.create_order = create_order  # type: ignore[method-assign]


# --------------------------------------------------------------------------- #
# M0: partial exits must never be booked as complete
# --------------------------------------------------------------------------- #
async def test_full_close_partly_filled_keeps_rest_open_and_protected() -> None:
    broker, fake = make_broker("binanceusdm")
    pos = await open_market(broker, qty=2.0, tps=TWO_TPS)
    _partial_market_closes(fake, 0.8)                       # venue price band expires the rest
    pos = await broker.close_position(pos, 1.0)

    assert fake.net[SYM] == pytest.approx(1.2)
    assert pos.status is PositionStatus.OPEN
    assert pos.remaining_quantity == pytest.approx(1.2)
    sls = _live_sls(fake)
    assert len(sls) == 1 and sls[0]["amount"] == pytest.approx(1.2)      # stop re-sized to what is left
    assert sum(t["amount"] for t in _live_tps(fake)) == pytest.approx(1.2)
    assert pos.error and "kısmen" in pos.error
    # the next sync still watches it (it is not CLOSED)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(1.2)


async def test_partial_close_that_fills_nothing_changes_nothing() -> None:
    broker, fake = make_broker("binanceusdm")
    pos = await open_market(broker, qty=2.0, tps=TWO_TPS)
    sl_before = pos.data["sl_order_id"]
    _partial_market_closes(fake, 0.0)
    pos = await broker.close_position(pos, 1.0)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(2.0)
    assert pos.data["sl_order_id"] == sl_before and fake.orders[sl_before]["status"] == "open"
    assert pos.error and "gerçekleşmedi" in pos.error and "hâlâ açık" in pos.error


async def test_last_tp_partly_filled_is_not_booked_as_full() -> None:
    broker, fake = make_broker("bybit")
    pos = await open_market(broker, qty=2.0, tps=TWO_TPS)
    tp1, tp2 = (t["id"] for t in pos.data["tp_order_ids"])
    fake.fill(tp1)
    pos = await broker.sync_position(pos)
    # TP2 triggers, its market child fills 0.8 of 1.0, rest cancelled (bybit PartiallyFilledCanceled -> "closed")
    fake.fill(tp2, amount=0.8)
    fake.orders[tp2]["status"] = "closed"
    pos = await broker.sync_position(pos)

    assert fake.net[SYM] == pytest.approx(0.2)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(0.2)
    assert [s["amount"] for s in _live_sls(fake)] == [pytest.approx(0.2)]
    rearmed = _live_tps(fake)                                # the unfilled 0.2 waits at the same TP price again
    assert [(t["params"]["takeProfitPrice"], t["amount"]) for t in rearmed] == [(2310.6, pytest.approx(0.2))]

    fake.fill(rearmed[0]["id"])
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and fake.net[SYM] == pytest.approx(0.0)
    assert not [o for o in fake.orders.values() if o["status"] == "open"]


async def test_tp_partly_filled_without_rearm_flags_error() -> None:
    broker, fake = make_broker("binanceusdm")
    pos = await open_market(broker, qty=2.0, tps=[TakeProfit(price=2310.6, close_fraction=1.0)])
    tp = pos.data["tp_order_ids"][0]["id"]
    fake.fill(tp, amount=1.5)
    fake.orders[tp]["status"] = "closed"
    fake.fail_when(lambda m, a: m == "create_order" and "takeProfitPrice" in a[5],
                   cx.InvalidOrder("Order would immediately trigger"), times=5)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(0.5)
    assert [s["amount"] for s in _live_sls(fake)] == [pytest.approx(0.5)]
    assert pos.error and "kısmen" in pos.error


async def test_stop_partly_filled_re_protects_the_rest() -> None:
    broker, fake = make_broker("bybit")
    pos = await open_market(broker, qty=2.0, tps=TWO_TPS)
    sl = pos.data["sl_order_id"]
    fake.fill(sl, amount=1.5, price=2205.11)
    fake.orders[sl]["status"] = "closed"
    pos = await broker.sync_position(pos)

    assert fake.net[SYM] == pytest.approx(0.5)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(0.5)
    live = _live_sls(fake)
    assert len(live) == 1 and live[0]["id"] != sl and live[0]["amount"] == pytest.approx(0.5)
    assert pos.data["sl_order_id"] == live[0]["id"]
    assert pos.error and "kısmen" in pos.error


async def test_stop_partly_filled_and_unplaceable_closes_the_rest() -> None:
    broker, fake = make_broker("bybit")
    pos = await open_market(broker, qty=2.0, tps=TWO_TPS)
    sl = pos.data["sl_order_id"]
    fake.fill(sl, amount=1.5, price=2205.11)
    fake.orders[sl]["status"] = "closed"
    fake.fail_when(lambda m, a: m == "create_order" and "stopLossPrice" in a[5],
                   cx.InvalidOrder("would immediately trigger"), times=2)
    pos = await broker.sync_position(pos)
    assert fake.net[SYM] == pytest.approx(0.0)
    assert pos.status is PositionStatus.CLOSED and pos.remaining_quantity == 0
    assert not [o for o in fake.orders.values() if o["status"] == "open"]


async def test_partial_emergency_close_is_flagged_unprotected_and_retried() -> None:
    broker, fake = make_broker("binanceusdm")
    fake.fail_when(lambda m, a: m == "create_order" and "stopLossPrice" in a[5], cx.ExchangeError("down"), times=2)
    _partial_market_closes(fake, 0.8)
    pos = await open_market(broker, qty=2.0, tps=TWO_TPS)

    assert fake.net[SYM] == pytest.approx(1.2)
    assert pos.status is PositionStatus.OPEN                   # not FAILED/CLOSED while 1.2 is still open
    assert pos.remaining_quantity == pytest.approx(1.2)
    assert pos.data["unprotected"] is True
    assert pos.error and "KRİTİK" in pos.error and "kısmen" in pos.error

    pos = await broker.sync_position(pos)                      # stop works again
    assert pos.data["unprotected"] is False
    assert [s["amount"] for s in _live_sls(fake)] == [pytest.approx(1.2)]
    assert not (pos.error and "KRİTİK" in pos.error)


async def test_close_with_unknown_result_is_confirmed_on_next_sync() -> None:
    broker, fake = make_broker("binanceusdm")
    pos = await open_market(broker, qty=2.0, tps=TWO_TPS)
    fake.net[SYM] += 5.0                                       # another trade shares the one-way position
    fake.minimal_create_response = True
    fake.fail_when(lambda m, a: m == "fetch_order" and a[0] == str(fake._seq), cx.RequestTimeout("timeout"), times=7)
    pos = await broker.close_position(pos, 1.0)

    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(2.0)
    assert fake.orders[pos.data["sl_order_id"]]["status"] == "open"      # stop kept until we know
    assert pos.data["unconfirmed_closes"] and pos.error and "okunamadı" in pos.error

    fake.failures.clear()
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.remaining_quantity == 0
    assert not pos.data["unconfirmed_closes"] and pos.error is None
    assert not [o for o in fake.orders.values() if o["status"] == "open"]


async def test_close_with_unknown_result_but_flat_venue_is_closed() -> None:
    broker, fake = make_broker("binanceusdm")
    pos = await open_market(broker, qty=2.0, tps=TWO_TPS)
    fake.minimal_create_response = True
    fake.fail_when(lambda m, a: m == "fetch_order" and a[0] == str(fake._seq), cx.RequestTimeout("timeout"), times=7)
    pos = await broker.close_position(pos, 1.0)
    assert pos.status is PositionStatus.CLOSED and fake.net[SYM] == pytest.approx(0.0)


async def test_entry_still_filling_while_closing_is_closed_too() -> None:
    broker, fake = make_broker("binanceusdm")
    pos = await open_limit(broker, qty=2.0, tps=TWO_TPS)
    entry = pos.data["entry_order_id"]
    fake.fill(entry, amount=1.0, price=2220.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(1.0)

    fake.fill(pos.data["sl_order_id"], price=2205.11)          # stopped out, entry rest still resting
    fake.fail_when(lambda m, a: m == "cancel_order" and a[0] == entry, cx.ExchangeNotAvailable("busy"))
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["closing"] is True   # entry may still fill
    assert fake.orders[entry]["status"] == "open"

    fake.fill(entry, amount=0.5, price=2210.0)                 # it does fill a bit more
    pos = await broker.sync_position(pos)
    assert fake.net[SYM] == pytest.approx(0.0)                # the late fill was closed as well
    assert pos.status is PositionStatus.CLOSED
    assert fake.orders[entry]["status"] == "canceled"
    assert not [o for o in fake.orders.values() if o["status"] == "open"]


# --------------------------------------------------------------------------- #
# M3: a replaced stop that could not be cancelled is retried
# --------------------------------------------------------------------------- #
async def test_stale_stop_after_move_sl_is_retried_on_sync() -> None:
    broker, fake = make_broker("binanceusdm")
    pos = await open_market(broker, qty=2.0, tps=TWO_TPS)
    old = pos.data["sl_order_id"]
    fake.fail_when(lambda m, a: m == "cancel_order" and a[0] == old, cx.RequestTimeout("timeout"))
    pos = await broker.update_stop_loss(pos, 2232.85)
    assert fake.orders[old]["status"] == "open"
    assert [s["id"] for s in pos.data["stale_orders"]] == [old]
    assert pos.error and "eski stop" in pos.error

    pos = await broker.sync_position(pos)
    assert fake.orders[old]["status"] == "canceled"
    assert pos.data["stale_orders"] == [] and pos.error is None
    assert [s["id"] for s in _live_sls(fake)] == [pos.data["sl_order_id"]]


async def test_stale_stop_blocks_close_until_cancelled() -> None:
    broker, fake = make_broker("binanceusdm")
    pos = await open_market(broker, qty=2.0, tps=TWO_TPS)
    old = pos.data["sl_order_id"]
    fake.fail_when(lambda m, a: m == "cancel_order" and a[0] == old, cx.RequestTimeout("timeout"), times=50)
    pos = await broker.update_stop_loss(pos, 2232.85)
    fake.fill(pos.data["sl_order_id"])                         # new stop fills: trade is flat
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.data["closing"] is True and pos.remaining_quantity == 0
    assert fake.orders[old]["status"] == "open"

    fake.failures.clear()
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_reason"] == "stop"
    assert fake.orders[old]["status"] == "canceled"
    assert not [o for o in fake.orders.values() if o["status"] == "open"]


async def test_stale_stop_that_fills_is_booked_once() -> None:
    broker, fake = make_broker("binanceusdm")
    pos = await open_market(broker, qty=2.0, tps=TWO_TPS)
    old = pos.data["sl_order_id"]
    fake.fail_when(lambda m, a: m == "cancel_order" and a[0] == old, cx.RequestTimeout("timeout"))
    fake.fill(pos.data["tp_order_ids"][0]["id"])               # TP1 -> SL re-sized to 1.0, old cancel fails
    pos = await broker.sync_position(pos)
    assert pos.remaining_quantity == pytest.approx(1.0)
    assert [s["id"] for s in pos.data["stale_orders"]] == [old]

    fake.fill(old, amount=1.0)                                 # the stale stop fires first (reduce-only: 1.0)
    fake.orders[old]["status"] = "closed"
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.remaining_quantity == 0
    assert pos.data["closed_qty"] == pytest.approx(2.0)
    assert [e["order_id"] for e in pos.data["exits"]].count(old) == 1
    assert not [o for o in fake.orders.values() if o["status"] == "open"]


# --------------------------------------------------------------------------- #
# M5: MOVE_TP must not re-arm a TP level that already filled
# --------------------------------------------------------------------------- #
async def test_move_tp_after_tp1_does_not_rearm_tp1() -> None:
    broker, fake = make_broker("binanceusdm")
    pos = await open_market(broker, qty=2.0, tps=TWO_TPS)
    fake.fill(pos.data["tp_order_ids"][0]["id"])
    pos = await broker.sync_position(pos)
    pos = await broker.update_take_profits(pos, [TakeProfit(price=2260.0, close_fraction=0.5),
                                                 TakeProfit(price=2400.0, close_fraction=0.5)])
    live = [(t["params"]["takeProfitPrice"], t["amount"]) for t in _live_tps(fake)]
    assert live == [(2400.0, pytest.approx(1.0))]
    assert any("2260" in w for w in pos.data["warnings"])


async def test_move_tp_listing_only_filled_levels_is_refused() -> None:
    broker, fake = make_broker("binanceusdm")
    pos = await open_market(broker, qty=2.0, tps=TWO_TPS)
    fake.fill(pos.data["tp_order_ids"][0]["id"])
    pos = await broker.sync_position(pos)
    tp2 = pos.data["tp_order_ids"][1]["id"]
    with pytest.raises(BrokerError):
        await broker.update_take_profits(pos, [TakeProfit(price=2260.0, close_fraction=1.0)])
    assert fake.orders[tp2]["status"] == "open"                 # nothing was touched


# --------------------------------------------------------------------------- #
# engine hook for pending_entry_expiry_min on a partly filled limit entry (M6, adapter side)
# --------------------------------------------------------------------------- #
async def test_cancel_entry_remainder_keeps_filled_part_protected() -> None:
    broker, fake = make_broker("binanceusdm")
    pos = await open_limit(broker, qty=2.0, tps=TWO_TPS)
    entry = pos.data["entry_order_id"]
    fake.fill(entry, amount=1.0, price=2220.0)
    pos = await broker.sync_position(pos)
    fake.fill(entry, amount=0.3, price=2220.0)                  # fills just before the cancel
    pos = await broker.cancel_entry_remainder(pos)
    assert fake.orders[entry]["status"] == "canceled" and pos.data["entry_done"] is True
    assert pos.status is PositionStatus.OPEN and pos.filled_quantity == pytest.approx(1.3)
    assert [s["amount"] for s in _live_sls(fake)] == [pytest.approx(1.3)]
    assert sum(t["amount"] for t in _live_tps(fake)) == pytest.approx(1.3)
