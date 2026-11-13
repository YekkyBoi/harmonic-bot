"""Review 3 (coordinator): the monitor keeps what an adapter handed back when a stale limit entry's cancel raised.

The Binance adapter hands its working state back into the caller's position object before it raises after an
order request went out without a clear answer (CcxtBroker._hand_back: the record of the lost order in
``unconfirmed_closes`` and the advanced client-id sequence). Monitor._expire_pending (pending_entry_expiry_min)
only logged such a failure and dropped that state: the next sync could not look the lost order up, and the owner
was not told. Now the handed-back position is stored and the adapter's note is sent once.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from copytrader.brokers.base import BrokerError, OrderOutcomeUnknownError
from copytrader.models import BrokerPosition, PositionStatus, Severity, TradeStatus

from test_preflight2_monitor import Venue, add_trade, db, make_monitor, no_task_left_behind, pending  # noqa: F401

LOST_NOTE = ("binance: 0.004 kapatma emri gönderildi ama sonucu okunamadı (borsaya ulaşılamadığı için bu etiketle "
             "(ctab12cd34ef56c1) aranamadı); stop yerinde bırakıldı, sonraki kontrollerde aranacak.")


def stale_binance_entry() -> BrokerPosition:
    old_ms = int((datetime.now(timezone.utc) - timedelta(hours=2)).timestamp() * 1000)
    return pending("binance", opened_at=old_ms)


async def test_handed_back_state_of_a_failed_stale_entry_cancel_is_stored_and_told_once(db) -> None:  # noqa: F811
    binance = Venue("binance", "ccxt")
    trade_id = await add_trade(db, stale_binance_entry(), status=TradeStatus.PENDING)
    monitor, notifier = make_monitor(db, [binance], interval=3600.0)
    monitor.config.risk.pending_entry_expiry_min = 1
    calls: list[int] = []

    async def cancel_entry(pos: BrokerPosition) -> BrokerPosition:
        # What the adapter does when the close of a fill found during the cancel loses its answer: its working
        # state goes into the caller's object (here: the lost close and the bumped sequence), then it raises.
        calls.append(1)
        pos.data["unconfirmed_closes"] = [{"order_id": None, "client_id": "ctab12cd34ef56c1", "qty": 0.004,
                                           "kind": "close", "since": 1, "misses": 0}]
        pos.data["close_seq"] = 1
        pos.filled_quantity = pos.remaining_quantity = 0.004
        pos.error = LOST_NOTE
        raise OrderOutcomeUnknownError("binance: kapatma emri (0.004 BTC/USDT:USDT) gönderildi ama yanıtı alınamadı; "
                                       "emir durumu belirsiz: gerçekleşip gerçekleşmediği bilinmiyor.")

    binance.cancel_entry = cancel_entry  # type: ignore[method-assign]
    await monitor.run_once()
    (stored,) = await db.broker_positions(trade_id)
    assert calls == [1]
    assert stored.data["unconfirmed_closes"][0]["client_id"] == "ctab12cd34ef56c1"
    assert stored.data["close_seq"] == 1 and stored.error == LOST_NOTE
    told = notifier.texts(Severity.ERROR)
    assert len(told) == 1 and LOST_NOTE in told[0], told
    await monitor.run_once()                       # the same failure again: stored again, not told again
    assert len(calls) == 2 and len(notifier.texts(Severity.ERROR)) == 1


async def test_a_failed_stale_entry_cancel_that_changed_nothing_stays_only_logged(db) -> None:  # noqa: F811
    binance = Venue("binance", "ccxt")
    trade_id = await add_trade(db, stale_binance_entry(), status=TradeStatus.PENDING)
    monitor, notifier = make_monitor(db, [binance], interval=3600.0)
    monitor.config.risk.pending_entry_expiry_min = 1

    async def cancel_entry(pos: BrokerPosition) -> BrokerPosition:
        raise BrokerError("binance: emir iptal edilemedi (e-1): -2011 Unknown order sent.")

    binance.cancel_entry = cancel_entry  # type: ignore[method-assign]
    await monitor.run_once()
    (stored,) = await db.broker_positions(trade_id)
    assert stored.status is PositionStatus.PENDING and "unconfirmed_closes" not in stored.data
    assert notifier.notifications == []            # as before: the next pass tries the cancel again
