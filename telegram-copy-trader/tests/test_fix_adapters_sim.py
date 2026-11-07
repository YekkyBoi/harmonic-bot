"""Regression tests for the paper / dry-run simulator audit fix M5 (MOVE_TP after a TP filled)."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from copytrader.brokers.base import BrokerError
from copytrader.brokers.dry_run import DryRunBroker
from copytrader.models import PositionStatus, Quote, TakeProfit
from test_dry_run import T0, FakeInner, _request
from test_paper_broker import make, open_pos

TWO = [TakeProfit(price=2260.0, close_fraction=0.5), TakeProfit(price=2310.6, close_fraction=0.5)]


def _open_tps(pos) -> list[tuple[float, float]]:
    return [(t["price"], t["qty"]) for t in pos.data["tps"] if t["status"] == "open"]


async def test_paper_move_tp_after_tp1_does_not_rearm_tp1(tmp_path: Path) -> None:
    broker, prices, _ = make(tmp_path)
    pos = await open_pos(broker, qty=2.0, tps=TWO)
    prices.ticker = {"bid": 2261.0, "ask": 2261.05, "last": 2261.0}       # TP1 hit
    pos = await broker.sync_position(pos)
    assert pos.remaining_quantity == pytest.approx(1.0)

    prices.ticker = {"bid": 2240.0, "ask": 2240.05, "last": 2240.0}       # pulls back
    pos = await broker.update_take_profits(pos, [TakeProfit(price=2260.0, close_fraction=0.5),
                                                 TakeProfit(price=2400.0, close_fraction=0.5)])
    assert _open_tps(pos) == [(2400.0, pytest.approx(1.0))]

    prices.ticker = {"bid": 2261.0, "ask": 2261.05, "last": 2261.0}       # revisits the old TP1
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(1.0)


async def test_paper_move_tp_with_only_filled_levels_is_refused(tmp_path: Path) -> None:
    broker, prices, _ = make(tmp_path)
    pos = await open_pos(broker, qty=2.0, tps=TWO)
    prices.ticker = {"bid": 2261.0, "ask": 2261.05, "last": 2261.0}
    pos = await broker.sync_position(pos)
    with pytest.raises(BrokerError):
        await broker.update_take_profits(pos, [TakeProfit(price=2260.0, close_fraction=1.0)])


async def test_dry_run_move_tp_after_tp1_does_not_rearm_tp1(tmp_path: Path) -> None:
    inner = FakeInner()
    clock = SimpleNamespace(now=T0)
    dry = DryRunBroker(inner, str(tmp_path), clock_ms=lambda: clock.now)
    info = await dry.resolve_symbol("ETH")
    assert info is not None
    pos = await dry.open_position(_request(info, tps=TWO))
    inner.quote = Quote(bid=2261.0, ask=2261.05, last=2261.0)
    pos = await dry.sync_position(pos)
    assert pos.remaining_quantity == pytest.approx(1.0)

    inner.quote = Quote(bid=2240.0, ask=2240.05, last=2240.0)
    pos = await dry.update_take_profits(pos, [TakeProfit(price=2260.0, close_fraction=0.5),
                                              TakeProfit(price=2400.0, close_fraction=0.5)])
    assert _open_tps(pos) == [(2400.0, pytest.approx(1.0))]
    inner.quote = Quote(bid=2261.0, ask=2261.05, last=2261.0)
    pos = await dry.sync_position(pos)
    assert pos.remaining_quantity == pytest.approx(1.0)
