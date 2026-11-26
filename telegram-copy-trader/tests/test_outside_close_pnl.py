"""A position closed by hand on Binance: the result of that close is read from the venue's own trade list
(closed_outside_pnl) so the owner's reports have it. realized_pnl keeps its meaning (the bot's own orders only)."""

from __future__ import annotations

import pytest

from copytrader.models import PositionStatus
from tests.test_ccxt_binance_real import BTC_LONG, _connected, _open, _plan


def _hand_close(fake, symbol: str, side: str, qty: float, price: float) -> None:
    order = {"symbol": symbol, "orderId": "HAND-1", "side": side, "origQty": str(qty), "executedQty": "0",
             "cumQuote": "0", "avgPrice": "0", "status": "NEW", "updateTime": fake.now}
    fake._fill(order, qty, price)


async def test_hand_close_result_is_read_from_the_venue_trades() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    qty, entry = pos.remaining_quantity, pos.avg_entry_price
    fake.now += 60_000
    _hand_close(fake, "BTCUSDT", "SELL", qty, 74500.0)
    pos = await broker.sync_position(pos)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_reason"] == "flat_on_exchange"
    assert pos.realized_pnl is None                              # unchanged: none of the bot's orders closed it
    want = (74500.0 - entry) * qty - 0.0005 * qty * 74500.0     # the hand close's own commission
    assert pos.data["closed_outside_pnl"] == pytest.approx(want, abs=0.05)
    assert pos.data["closed_outside_qty"] == pytest.approx(qty)
    assert "kâr/zararı borsa kayıtlarından hesaplandı: +" in pos.error
    await broker.close()


async def test_hand_close_trades_before_the_open_are_not_counted() -> None:
    broker, fake = await _connected()
    fake.now += 60_000
    _hand_close(fake, "BTCUSDT", "SELL", 0.001, 70000.0)        # an older trade on the symbol, before our entry
    _hand_close(fake, "BTCUSDT", "BUY", 0.001, 70000.0)
    fake.now += 600_000
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.pos["BTCUSDT"]["amt"] = 0.0                            # gone without a trade we can see
    pos = await broker.sync_position(pos)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED
    assert "closed_outside_pnl" not in pos.data
    await broker.close()


async def test_only_the_size_closed_outside_is_counted() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    qty, entry = pos.remaining_quantity, pos.avg_entry_price
    fake.now += 60_000
    _hand_close(fake, "BTCUSDT", "SELL", qty, 74500.0)
    _hand_close(fake, "BTCUSDT", "SELL", 0.5, 74400.0)          # a later, unrelated short opened by hand
    pos = await broker.sync_position(pos)
    pos = await broker.sync_position(pos)
    want = (74500.0 - entry) * qty - 0.0005 * qty * 74500.0
    assert pos.data["closed_outside_pnl"] == pytest.approx(want, abs=0.05)
    await broker.close()

