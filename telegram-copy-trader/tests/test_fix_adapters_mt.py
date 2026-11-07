"""Regression tests for the MetaTrader leg adapter audit fixes (M5 netting TPs, unprotected flag, partial close)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from copytrader.brokers.base import BrokerError
from copytrader.brokers.mt5_broker import MT5
from copytrader.models import PositionStatus
from test_mt5_broker import SERVER_TIME, FakeMT5, connected, make_plan, make_req, tp


def _tick(fake: FakeMT5, bid: float) -> None:
    fake.ticks["ETHUSD"] = SimpleNamespace(bid=bid, ask=bid + 0.2, last=0.0, time=SERVER_TIME)


async def test_netting_move_tp_after_emulated_tp1_does_not_rearm_it() -> None:
    fake = FakeMT5(margin_mode=0)
    b = await connected(fake)
    sym = await b.resolve_symbol("ETH")
    pos = await b.open_position(make_req(sym, 0.10, make_plan(tps=((2260.0, 0.5), (2310.6, 0.5)))))
    _tick(fake, 2261.0)
    pos = await b.sync_position(pos)                      # emulated TP1 partial-closes 0.05
    assert pos.remaining_quantity == pytest.approx(0.05)

    _tick(fake, 2240.0)
    pos = await b.update_take_profits(pos, tp((2260.0, 0.5), (2400.0, 0.5)))
    assert [(e["price"], e["done"]) for e in pos.data["emulated_tps"]] == [(2260.0, True)]
    (p,) = fake.positions.values()
    assert p.tp == 2400.0

    _tick(fake, 2261.0)
    fake.sent.clear()
    pos = await b.sync_position(pos)
    assert not fake.sent_actions(MT5.TRADE_ACTION_DEAL)   # 2260 is not hit a second time
    assert pos.remaining_quantity == pytest.approx(0.05)

    with pytest.raises(BrokerError):
        await b.update_take_profits(pos, tp((2260.0, 1.0)))
    await b.close()


async def test_naked_leg_sets_unprotected_until_stop_is_back() -> None:
    fake = FakeMT5()
    b = await connected(fake)
    fake.reject_stops_on_deal = True
    fake.sltp_fail = 4                                     # attach fails at open and in the post-open check
    reject_close = lambda r: 10013 if r["action"] == MT5.TRADE_ACTION_DEAL and "position" in r else None  # noqa: E731
    fake.hooks.append(reject_close)
    sym = await b.resolve_symbol("ETH")
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    assert pos.status is PositionStatus.OPEN
    assert pos.data["unprotected"] is True and "KRİTİK" in (pos.error or "")

    fake.hooks.clear()
    pos = await b.sync_position(pos)                      # the stop can be attached again
    assert pos.data["unprotected"] is False
    assert not any(leg.get("naked") for leg in pos.data["legs"])
    assert pos.error is None
    (p,) = fake.positions.values()
    assert p.sl == 2205.11
    await b.close()


async def test_full_close_partly_executed_reports_the_rest() -> None:
    fake = FakeMT5()
    b = await connected(fake)
    sym = await b.resolve_symbol("ETH")
    pos = await b.open_position(make_req(sym, 0.10, make_plan()))
    original = fake.order_send

    def half_close(request: dict[str, Any]) -> Any:
        if request["action"] == MT5.TRADE_ACTION_DEAL and "position" in request:
            request = dict(request, volume=round(request["volume"] / 2, 2))
        return original(request)

    fake.order_send = half_close  # type: ignore[method-assign]
    pos = await b.close_position(pos, 1.0)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(0.05)
    assert pos.error and "hâlâ açık" in pos.error
    await b.close()
