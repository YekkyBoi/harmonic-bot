"""Startup drops unconsumed messages older than the catch-up age (never merged into a later signal)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from copytrader.engine.orchestrator import Engine
from copytrader.models import Severity, StoredMessage
from copytrader.storage.db import Database

from fakes import FakeBroker, FakeNotifier, FakeParser, FakePriceOracle
from test_engine import CHAT, make_config


async def test_stale_unconsumed_messages_are_consumed_on_start() -> None:
    db = Database(":memory:")
    await db.open()
    now = datetime.now(timezone.utc)
    await db.save_message(StoredMessage(chat_id=CHAT, message_id=1, sender_id=7, date=now - timedelta(minutes=4),
                                        text="eth 1R long"))
    await db.save_message(StoredMessage(chat_id=CHAT, message_id=2, sender_id=7, date=now - timedelta(seconds=10),
                                        text="btc short"))
    notifier = FakeNotifier()
    engine = Engine(make_config(), db, FakeParser(), [FakeBroker("b")], notifier,
                    price_oracle=FakePriceOracle({"ETH": 2232.0}))
    await engine.start(monitor=False)
    try:
        old = await db.get_message(CHAT, 1)
        fresh = await db.get_message(CHAT, 2)
        assert old is not None and old.consumed
        assert fresh is not None and not fresh.consumed       # young enough for the listener's catch-up
        assert any("eski mesaj" in t for t in notifier.texts(Severity.WARNING))
    finally:
        await engine.stop()
        await db.close()
