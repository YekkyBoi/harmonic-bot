"""Pre-flight batch 3: Telegram listener, storage and app wiring (tasks L1-L10).

Findings of the telegram, engine, parser and ops audits: messages that arrive while the listener is
away, the batch window, failed screenshot downloads, unchanged edits after a restart, anonymous
admins, deleted / content-less / forwarded messages, unknown topics, Ctrl+C during shutdown and
secrets in the log. Everything runs against fakes: they prove the bot's logic, not Telegram or a broker.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from copytrader import app as app_mod
from copytrader.app import build_app, serve
from copytrader.config import AppConfig
from copytrader.engine.orchestrator import Engine
from copytrader.models import (
    ActionType,
    EntryType,
    MarketType,
    ParsedAction,
    ParseResult,
    Quote,
    Severity,
    Side,
    StoredMessage,
    TakeProfit,
)
from copytrader.storage.db import Database
from copytrader.telegram import listener as listener_mod
from copytrader.telegram.listener import TelegramListener

from fakes import FakeBroker, FakeNotifier, FakeParser, FakePriceOracle
from test_listener import FakeDB, FakeMessage, Recorder
from test_topics_listener import ForumClient, in_topic

CHAT = -1003782294463
TRADER, OTHER = 111, 222
CRYPTO, SOHBET, FX = 3, 4, 5
TITLES = {CRYPTO: "Kasım Ayı Kripto İşlemleri", FX: "Kasım Ayı Fx İşlemleri", SOHBET: "Sohbet", 1: "General"}
BROKERS = ("binance", "breakout", "fx", "tickmill")
# The bot has run before: the owner was already told which topics of the forum are followed (L7).
TOPICS_STATE = f"topics_known:{CHAT}"
KNOWN_TOPICS = {str(topic): title for topic, title in TITLES.items() if topic != 1}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def prod_like_config(tmp_path: Path, **telegram: Any) -> AppConfig:
    """The owner's routes and broker names (the brokers themselves are fakes in every test)."""
    return AppConfig.model_validate({
        "dry_run": True,
        "data_dir": str(tmp_path / "data"),
        "monitor_interval_sec": 3600.0,
        "telegram": {
            "api_id": 1, "api_hash": "hash", "session_string": "fake-session", "source_chats": [CHAT],
            "debounce_sec": 0.05,
            "topics": [
                {"name": "kripto", "title_contains": ["Kripto İşlemleri"], "kind": "crypto",
                 "brokers": ["binance", "breakout", "tickmill"]},
                {"name": "fx", "title_contains": ["Fx İşlemleri"], "kind": "fx", "brokers": ["fx", "tickmill"]},
            ],
            **telegram,
        },
        "notifier": {"bot_token": "123456789:" + "A" * 35, "owner_user_id": 42},
        "llm": {"prefilter": False},
        "brokers": [{"type": "paper", "name": name} for name in BROKERS],
    })


def msg(mid: int, text: str = "", *, topic: int | None = CRYPTO, sender: int | None = TRADER, age: float = 0.0,
        reply_to: int | None = None, **kw: Any) -> FakeMessage:
    header = None if topic is None else in_topic(topic, reply_to)
    return FakeMessage(id=mid, message=text, chat_id=CHAT, sender_id=sender, reply_to=header,
                       date=utcnow() - timedelta(seconds=age), **kw)


def eth_long(ids: list[int]) -> ParsedAction:
    return ParsedAction(
        action=ActionType.OPEN, symbol="ETHUSDT.P", base_asset="ETH", quote_asset="USDT",
        market_type=MarketType.PERP, side=Side.LONG, entry_type=EntryType.MARKET, entry_price=2239.2,
        stop_loss=2215.0, take_profits=[TakeProfit(price=2290.0)], risk_r=1.0, used_message_ids=ids,
        confidence=0.95, summary_tr="ETH LONG piyasa, SL 2215, TP 2290, risk 1R",
    )


def none_action(ids: list[int]) -> ParsedAction:
    return ParsedAction(action=ActionType.NONE, used_message_ids=ids, confidence=0.99, summary_tr="işlem mesajı değil")


class TgClient(ForumClient):
    """Read-only Telegram fake that can also be start()ed. Any call the listener is not allowed to
    make is recorded in ``forbidden`` (and fails)."""

    def __init__(self, history: list[Any] | None = None, titles: dict[int, str] | None = None) -> None:
        super().__init__(TITLES if titles is None else titles, history=history)
        self.channel = SimpleNamespace(id=3782294463, title="KLAN", forum=True)
        self.handlers: list[tuple[Any, Any]] = []
        self.connected_at: float | None = None
        self.history_calls = 0
        self.limits: list[int | None] = []  # the limit of every history request
        self.fail_history = 0              # the next N history requests fail
        self.hang_history = False          # ... or never answer
        self.forbidden: list[str] = []
        self._gone = asyncio.Event()

    async def connect(self) -> None:
        self.connected_at = time.monotonic()

    async def is_user_authorized(self) -> bool:
        return True

    async def get_me(self) -> Any:
        return SimpleNamespace(id=999, bot=False)

    async def get_entity(self, target: Any) -> Any:
        return self.channel

    def add_event_handler(self, callback: Any, builder: Any) -> None:
        self.handlers.append((callback, builder))

    def remove_event_handler(self, callback: Any) -> None:
        pass

    def iter_messages(self, entity: Any, min_id: int = 0, limit: int | None = None) -> Any:
        self.history_calls += 1
        self.limits.append(limit)
        inner = super().iter_messages(entity, min_id=min_id, limit=limit)

        async def gen() -> Any:
            if self.hang_history:
                await asyncio.Event().wait()
            if self.fail_history:
                self.fail_history -= 1
                raise ConnectionError("Cannot send requests while disconnected")
            async for m in inner:
                yield m
        return gen()

    async def run_until_disconnected(self) -> None:
        await self._gone.wait()

    async def disconnect(self) -> None:
        self._gone.set()

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__"):
            raise AttributeError(name)
        self.forbidden.append(name)
        raise AssertionError(f"listener must not call client.{name}")


def patch_peer_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """start() resolves the source chat with telethon.utils.get_peer_id: the fake channel maps to CHAT."""
    monkeypatch.setattr(listener_mod.utils, "get_peer_id", lambda entity: CHAT)


def make_listener(tmp_path: Path, cfg: AppConfig, db: Any, on_ready: Any, on_edit: Any,
                  notifier: FakeNotifier | None = None, client: Any = None) -> TelegramListener:
    """A listener that is 'connected' to the forum without start() (handlers are called directly)."""
    if isinstance(db, FakeDB):
        db.state.setdefault(TOPICS_STATE, dict(KNOWN_TOPICS))
    listener = TelegramListener(cfg.telegram, db, str(tmp_path), on_ready, on_edit)
    listener._client = client or TgClient()
    listener._entities = {CHAT: SimpleNamespace(forum=True)}
    listener._forums = {CHAT}
    if notifier is not None and hasattr(listener, "set_notifier"):
        listener.set_notifier(notifier)
    return listener


async def live(listener: TelegramListener, message: Any) -> None:
    """Deliver a message the way Telethon's update stream does."""
    await listener._on_new_message_event(SimpleNamespace(message=message))
    await asyncio.sleep(0)


async def wait_until(cond: Any, timeout: float = 3.0, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for: {what}")
        await asyncio.sleep(0.02)


def fast_notices(monkeypatch: pytest.MonkeyPatch, throttle: float | None = None) -> None:
    monkeypatch.setattr(listener_mod, "SKIP_NOTICE_COLLECT_SEC", 0.01, raising=False)
    if throttle is not None:
        monkeypatch.setattr(listener_mod, "SKIP_NOTICE_THROTTLE_SEC", throttle, raising=False)


@pytest.fixture
async def sqldb():
    database = Database(":memory:")
    await database.open()
    yield database
    await database.close()


class SlowParser(FakeParser):
    """FakeParser whose calls take ``delays[i]`` seconds (the stream lock is held meanwhile)."""

    def __init__(self, *results: Any, delays: tuple[float, ...] = ()) -> None:
        super().__init__(*results)
        self.delays = list(delays)

    async def parse(self, messages: list[StoredMessage], context: Any) -> ParseResult:
        delay = self.delays.pop(0) if self.delays else 0.0
        self.calls.append((list(messages), context))
        await asyncio.sleep(delay)
        item = self.script.pop(0) if self.script else self.last
        self.last = item
        if callable(item) and not isinstance(item, ParseResult):
            return item(messages, context)
        return item  # type: ignore[return-value]


async def make_engine(cfg: AppConfig, db: Database, parser: FakeParser) -> tuple[Engine, FakeNotifier, dict[str, FakeBroker]]:
    quote = Quote(bid=2239.1, ask=2239.3, last=2239.2)
    brokers = [FakeBroker(name, quote=quote, qty_step=0.01) for name in BROKERS]
    notifier = FakeNotifier()
    engine = Engine(cfg, db, parser, brokers, notifier, price_oracle=FakePriceOracle({"ETH": 2239.2}))
    await db.set_state(TOPICS_STATE, dict(KNOWN_TOPICS))
    await engine.start(monitor=False)
    notifier.notifications.clear()
    return engine, notifier, {b.name: b for b in brokers}


def read_all(messages: list[StoredMessage], ctx: Any) -> ParseResult:
    return ParseResult(actions=[eth_long([m.message_id for m in messages])], model="fake")


# =========================================================================== #
# L1 (a): the listener's own catch-up runs periodically
# =========================================================================== #
async def started(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, client: TgClient, *, db: Any = None,
                  seed_topics: bool = True, **telegram: Any) -> tuple[TelegramListener, Any, Recorder, FakeNotifier]:
    patch_peer_id(monkeypatch)
    cfg = prod_like_config(tmp_path, **telegram)
    db = db or FakeDB()
    if isinstance(db, FakeDB):
        db.state.setdefault(f"last_msg_id:{CHAT}", 100)
        if seed_topics:
            db.state.setdefault(TOPICS_STATE, dict(KNOWN_TOPICS))
    rec = Recorder(db if isinstance(db, FakeDB) else None)
    notifier = FakeNotifier()
    listener = TelegramListener(cfg.telegram, db, str(tmp_path), rec.on_ready, rec.on_edit)
    listener._client = client
    if hasattr(listener, "set_notifier"):
        listener.set_notifier(notifier)
    await listener.start()
    return listener, db, rec, notifier


async def test_l1a_periodic_catch_up_fetches_a_message_the_update_stream_never_delivered(tmp_path, monkeypatch):
    monkeypatch.setattr(listener_mod, "CATCHUP_POLL_SEC", 0.05, raising=False)
    client = TgClient([])
    listener, db, rec, notifier = await started(tmp_path, monkeypatch, client)
    try:
        await asyncio.sleep(0.2)
        assert rec.calls == []
        # Written during a short drop that Telethon repaired by itself: no update is ever delivered.
        client.history.append(msg(101, "eth 1R long piyasadan SL 2215 TP 2290", age=5))
        await wait_until(lambda: rec.calls == [(CHAT, TRADER, CRYPTO)], 2, "the missed message to be passed on")
        assert db.messages[(CHAT, 101)].consumed is False
        # one read-only history request per round, nothing else
        before, t0 = client.history_calls, time.monotonic()
        await wait_until(lambda: client.history_calls - before >= 4, 5, "four more rounds")
        rounds, elapsed = client.history_calls - before, time.monotonic() - t0
        assert rounds <= elapsed / 0.05 + 2, (rounds, elapsed)      # never more than one request per round
        assert client.forbidden == []
        assert rec.calls == [(CHAT, TRADER, CRYPTO)]                  # the same message is never handled twice
        assert notifier.notifications == []
    finally:
        await listener.stop()
    after_stop = client.history_calls
    await asyncio.sleep(0.2)
    assert client.history_calls == after_stop                         # stop() ends the rounds


async def test_l1a_periodic_round_asks_for_few_messages_and_fetches_all_when_many_were_missed(tmp_path, monkeypatch):
    """Telethon's history request returns the newest ``limit`` messages whatever ``min_id`` is (it filters
    locally), so a round asks for 20; when all 20 are new, the full fetch follows and nothing is lost."""
    monkeypatch.setattr(listener_mod, "CATCHUP_POLL_SEC", 0.05, raising=False)
    client = TgClient([])
    listener, db, rec, notifier = await started(tmp_path, monkeypatch, client)
    try:
        await asyncio.sleep(0.2)
        assert client.limits[0] == listener_mod.CATCHUP_FETCH_LIMIT            # the catch-up at start: everything
        assert len(client.limits) > 1 and set(client.limits[1:]) == {listener_mod.POLL_FETCH_LIMIT} == {20}
        burst = [msg(200 + i, f"mesaj {i}", sender=TRADER if i % 2 else OTHER, age=3) for i in range(30)]
        client.history.extend(burst)                                            # 30 messages between two rounds
        await wait_until(lambda: len(db.messages) == 30, 3, "all thirty messages")
        assert sorted(mid for _, mid in db.messages) == list(range(200, 230))
        assert listener_mod.CATCHUP_FETCH_LIMIT in client.limits[1:]            # the full fetch was made once
        assert db.state[f"last_msg_id:{CHAT}"] == 229
    finally:
        await listener.stop()


async def test_l1a_failed_round_is_retried_and_the_owner_learns_how_long_telegram_was_unreachable(
        tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(listener_mod, "CATCHUP_POLL_SEC", 0.05, raising=False)
    client = TgClient([])
    listener, db, rec, notifier = await started(tmp_path, monkeypatch, client)
    try:
        client.fail_history = 3                                      # the connection is down for three rounds
        client.history.append(msg(101, "eth kapattık", topic=CRYPTO, age=400))
        client.history.append(msg(102, "EUR LONG 1R Limit Emir", topic=FX, age=20))
        with caplog.at_level(logging.WARNING):
            await wait_until(lambda: len(notifier.notifications) == 1, 3, "the 'not processed' notice")
        text, severity = notifier.notifications[0]
        assert severity is Severity.WARNING
        assert "Telegram bağlantısı yaklaşık" in text and "kesikti" in text
        assert "1 mesaj" in text and "İŞLENMEDİ" in text
        assert TITLES[CRYPTO] in text and "eth kapattık" in text and "7 dakika önce" in text
        assert db.messages[(CHAT, 101)].consumed is True              # context only, never executed
        await wait_until(lambda: rec.calls == [(CHAT, TRADER, FX)], 2, "the young message to be passed on")
        # one log line for the outage, not one per failed round
        assert sum("Periodic catch-up failed" in r.getMessage() for r in caplog.records) == 1
    finally:
        await listener.stop()


async def test_l1a_history_request_that_never_answers_does_not_block_the_listener(tmp_path, monkeypatch):
    monkeypatch.setattr(listener_mod, "CATCHUP_POLL_SEC", 0.05, raising=False)
    monkeypatch.setattr(listener_mod, "CATCHUP_REQUEST_TIMEOUT_SEC", 0.1, raising=False)
    client = TgClient([])
    listener, db, rec, notifier = await started(tmp_path, monkeypatch, client)
    try:
        client.hang_history = True                                   # Telethon is reconnecting: requests queue up
        await asyncio.sleep(0.4)
        client.hang_history = False
        client.history.append(msg(101, "eth 1R long", age=5))
        await wait_until(lambda: rec.calls == [(CHAT, TRADER, CRYPTO)], 2, "the round after the hang")
    finally:
        await asyncio.wait_for(listener.stop(), 2)


# =========================================================================== #
# L1 (b): one age rule for live and catch-up messages
# =========================================================================== #
async def test_l1b_late_live_message_is_not_executed_and_the_owner_is_told(tmp_path, monkeypatch, sqldb):
    """Telegram finding 4: 200 s old -> was opened at market; 360 s old -> was dropped silently."""
    fast_notices(monkeypatch)
    cfg = prod_like_config(tmp_path)
    assert cfg.telegram.catchup_max_age_sec == 120 and cfg.telegram.context_window_sec == 300
    parser = FakeParser(read_all)
    engine, notifier, brokers = await make_engine(cfg, sqldb, parser)
    listener = make_listener(tmp_path, cfg, sqldb, engine.on_messages_ready, engine.on_message_edited, notifier)
    try:
        await live(listener, msg(302, "eth 1R long piyasadan SL 2215 TP 2290", age=200))
        await live(listener, msg(301, "EUR kapattık", topic=FX, age=360))
        await wait_until(lambda: len(notifier.notifications) == 2, 2, "one notice per topic")
        assert parser.calls == [] and await sqldb.active_trades() == []
        assert all(b.opened == [] for b in brokers.values())
        for mid in (301, 302):
            row = await sqldb.get_message(CHAT, mid)
            assert row is not None and row.consumed is True           # context only: never merged into a later signal
        by_topic = {TITLES[CRYPTO] in t: t for t, _ in notifier.notifications}
        crypto, fx = by_topic[True], by_topic[False]
        assert "1 mesaj bota geç ulaştı" in crypto and "120 saniyeden eski olduğu için İŞLENMEDİ" in crypto
        assert "3 dakika önce: eth 1R long piyasadan SL 2215 TP 2290" in crypto
        assert TITLES[FX] in fx and "6 dakika önce: EUR kapattık" in fx
        assert "sunucunun saati" in crypto                            # hint for a VPS clock that runs fast
        assert all(s is Severity.WARNING for _, s in notifier.notifications)

        # a young live message is still copied to every broker of its route
        await live(listener, msg(303, "eth 1R long piyasadan SL 2215 TP 2290", age=60))
        await wait_until(lambda: brokers["tickmill"].opened, 3, "the young signal to open")
        assert {n: len(b.opened) for n, b in brokers.items()} == {"binance": 1, "breakout": 1, "fx": 0, "tickmill": 1}
        assert [m.message_id for m in parser.calls[0][0]] == [303]
        assert len(notifier.texts(Severity.WARNING)) == 2
    finally:
        await listener.stop()
        await engine.stop()


async def test_l1b_edit_of_a_message_the_bot_never_saw_follows_the_same_rule(tmp_path, monkeypatch):
    fast_notices(monkeypatch)
    cfg = prod_like_config(tmp_path)
    db, notifier = FakeDB(), FakeNotifier()
    rec = Recorder(db)
    listener = make_listener(tmp_path, cfg, db, rec.on_ready, rec.on_edit, notifier)
    try:
        await listener.handle_edit(msg(401, "eth 1R long", age=30))          # young: handled as a new message
        await listener.handle_edit(msg(402, "eski sinyal (düzeltildi)", age=5 * 3600))
        await asyncio.sleep(0.3)
        assert rec.calls == [(CHAT, TRADER, CRYPTO)]
        assert db.messages[(CHAT, 402)].consumed is True
        assert notifier.notifications == []                           # an old edited message is not a missed one
    finally:
        await listener.stop()


# =========================================================================== #
# L1 (c): listen while the brokers connect, hold the batches until the engine is ready
# =========================================================================== #
class SlowConnectBroker(FakeBroker):
    delay = 0.0

    async def connect(self) -> None:
        await asyncio.sleep(self.delay)
        await super().connect()


async def test_l1c_restart_judges_a_message_by_the_time_the_bot_was_away_not_by_the_broker_connect(
        tmp_path, monkeypatch):
    """Telegram finding 3 through the real app.serve(): one broker (MetaApi) connects slowly."""
    patch_peer_id(monkeypatch)
    cfg = prod_like_config(tmp_path, catchup_max_age_sec=4)
    db = Database(tmp_path / "copytrader.db")
    await db.open()
    await db.set_state(f"last_msg_id:{CHAT}", 100)                    # the bot had been running before
    await db.set_state(TOPICS_STATE, dict(KNOWN_TOPICS))
    await db.set_state("listener_alive_at", (utcnow() - timedelta(minutes=10)).isoformat())
    # read as "incomplete" before the bot stopped: still waiting, far too old now
    await db.save_message(StoredMessage(chat_id=CHAT, message_id=90, sender_id=TRADER, text="btc 1R short",
                                        date=utcnow() - timedelta(minutes=11), topic_id=CRYPTO,
                                        topic_title=TITLES[CRYPTO]))
    client = TgClient([
        msg(101, "kapattık", topic=FX, age=400),                                  # written while the bot was down
        msg(102, "eth 1R long piyasadan SL 2215 TP 2290", topic=CRYPTO, age=1.0),  # written 1 s before the restart
    ])
    parser, notifier = FakeParser(read_all), FakeNotifier()
    brokers: dict[str, SlowConnectBroker] = {}

    def broker_factory(bcfg: Any) -> SlowConnectBroker:
        b = SlowConnectBroker(bcfg.name, quote=Quote(bid=2239.1, ask=2239.3, last=2239.2), qty_step=0.01)
        b.delay = 3.5 if bcfg.name == "tickmill" else 0.0             # e.g. MetaApi taking long to connect
        brokers[bcfg.name] = b
        return b

    def listener_factory(tg: Any, database: Any, data_dir: str, on_ready: Any, on_edit: Any) -> TelegramListener:
        listener = TelegramListener(tg, database, data_dir, on_ready, on_edit)
        listener._client = client
        return listener

    app = build_app(cfg, db=db, parser=parser, notifier=notifier, broker_factory=broker_factory,
                    listener_factory=listener_factory)
    app.engine.prices = FakePriceOracle({"ETH": 2239.2})
    stop = asyncio.Event()
    t0 = time.monotonic()
    task = asyncio.ensure_future(serve(app, stop))
    try:
        await wait_until(lambda: brokers["tickmill"].opened or task.done(), 8, "the signal to open on tickmill")
        assert not task.done()
        opened_at = time.monotonic() - t0
        assert client.connected_at is not None and client.connected_at - t0 < 1.5   # listening while brokers connect
        assert opened_at > 3.4                                        # ... but nothing was passed on before they were ready
        assert [[m.message_id for m in batch] for batch, _ in parser.calls] == [[102]]
        assert {n: len(b.opened) for n, b in brokers.items()} == {"binance": 1, "breakout": 1, "fx": 0, "tickmill": 1}
        for mid in (90, 101):
            row = await db.get_message(CHAT, mid)
            assert row is not None and row.consumed is True
        # ONE notice for the restart: how long, how many, which
        restart = [t for t in notifier.texts() if "kapalıydı" in t]
        assert len(restart) == 1, notifier.texts()
        text = restart[0]
        assert "Bot yaklaşık 10 dakika kapalıydı" in text and "2 mesaj" in text and "İŞLENMEDİ" in text
        assert f"<b>{TITLES[FX]}</b> · 7 dakika önce: kapattık" in text
        assert f"<b>{TITLES[CRYPTO]}</b> · 11 dakika önce: btc 1R short" in text
        assert not any("eski mesaj" in t for t in notifier.texts())   # no second, count-only notice from the engine
    finally:
        stop.set()
        await asyncio.wait_for(task, 20)
        await db.close()


async def test_l1c_held_listener_stores_messages_but_passes_nothing_on_until_released(tmp_path):
    cfg = prod_like_config(tmp_path)
    db = FakeDB()
    rec = Recorder(db)
    listener = make_listener(tmp_path, cfg, db, rec.on_ready, rec.on_edit)
    try:
        listener.hold_batches()
        await live(listener, msg(601, "eth 1R long"))
        await asyncio.sleep(0.3)
        assert (CHAT, 601) in db.messages and rec.calls == []
        edited = msg(601, "eth 1R long (düzeltme)")
        await asyncio.wait_for(listener.handle_edit(edited), 1)       # never blocks while held
        await asyncio.sleep(0.2)
        assert rec.edits == [] and db.messages[(CHAT, 601)].text == "eth 1R long (düzeltme)"
        listener.release_batches()
        await wait_until(lambda: rec.calls == [(CHAT, TRADER, CRYPTO)] and len(rec.edits) == 1, 2, "release")
    finally:
        await listener.stop()


async def test_l1c_stop_while_held_does_not_wait_and_passes_nothing_on(tmp_path):
    cfg = prod_like_config(tmp_path)
    db = FakeDB()
    rec = Recorder(db)
    listener = make_listener(tmp_path, cfg, db, rec.on_ready, rec.on_edit)
    listener.hold_batches()
    await live(listener, msg(611, "eth 1R long"))
    await asyncio.sleep(0.2)
    t0 = time.monotonic()
    await listener.stop()
    assert time.monotonic() - t0 < 1.0
    await asyncio.sleep(0.2)
    assert rec.calls == []
    assert db.messages[(CHAT, 611)].consumed is False                  # judged by its age again after the restart


# =========================================================================== #
# L1 (d): the owner is told about every followed-topic message that is not processed because of its age
# =========================================================================== #
def test_l1d_notice_text_lists_at_most_five_messages_and_escapes_them():
    now = utcnow()
    items = [
        listener_mod.SkippedMessage(CHAT, 700 + i, CRYPTO, TITLES[CRYPTO], now - timedelta(seconds=200 + 60 * i),
                                    f"sinyal {i} <b>kalın</b>\nikinci satır", False)
        for i in range(7)
    ]
    items.append(listener_mod.SkippedMessage(CHAT, 799, FX, "Fx & <Yeni>", now - timedelta(hours=3), "", True))
    text = listener_mod.skipped_notice_tr(items, occasion="restart", away_sec=3 * 3600 + 300, max_age_sec=120, now=now)
    lines = text.splitlines()
    assert lines[0].startswith("Bot yaklaşık 3 saat 5 dakika kapalıydı.") and "8 mesaj" in lines[0]
    bullets = [line for line in lines if line.startswith("•")]
    assert len(bullets) == 5 and "… ve 3 mesaj daha." in lines
    assert "sinyal 0 &lt;b&gt;kalın&lt;/b&gt;" in text and "ikinci satır" not in text      # first line only, escaped
    assert "sinyal 6" not in text and "Fx &amp; &lt;Yeni&gt;" not in text                  # the five NEWEST are listed
    assert lines[-1] == "Grubu kontrol edin; gerekiyorsa işlemi elle yapın."
    unknown = listener_mod.skipped_notice_tr(items[:1], occasion="restart", away_sec=None, max_age_sec=120, now=now)
    assert unknown.startswith("Bot yeniden başladı.")
    image_only = listener_mod.skipped_notice_tr(items[-1:], occasion="reconnect", away_sec=95, max_age_sec=120, now=now)
    assert image_only.startswith("Telegram bağlantısı yaklaşık 2 dakika kesikti.")
    assert "<b>Fx &amp; &lt;Yeni&gt;</b> · 3 saat önce: (resim, metin yok)" in image_only


async def test_l1d_live_notices_are_throttled_per_topic_and_nothing_is_dropped(tmp_path, monkeypatch):
    fast_notices(monkeypatch, throttle=1.5)
    cfg = prod_like_config(tmp_path)
    db, notifier = FakeDB(), FakeNotifier()
    rec = Recorder(db)
    listener = make_listener(tmp_path, cfg, db, rec.on_ready, rec.on_edit, notifier)
    try:
        await live(listener, msg(801, "btc 1R long", age=300))
        await wait_until(lambda: len(notifier.notifications) == 1, 1, "first notice")
        await live(listener, msg(802, "sol 1R short", age=300))                 # same topic, inside the throttle time
        await live(listener, msg(803, "ada kapattık", age=300))
        await live(listener, msg(804, "EUR kapattık", topic=FX, age=300))       # another topic: its own notice
        await wait_until(lambda: len(notifier.notifications) == 2, 1, "notice of the other topic")
        assert TITLES[FX] in notifier.notifications[1][0]
        await asyncio.sleep(0.2)
        assert len(notifier.notifications) == 2                                 # crypto: still throttled
        await wait_until(lambda: len(notifier.notifications) == 3, 4, "the held notice")
        held = notifier.notifications[2][0]
        assert "2 mesaj" in held and "sol 1R short" in held and "ada kapattık" in held
        assert rec.calls == []
    finally:
        await listener.stop()


async def test_l1d_pending_notice_is_sent_when_the_listener_stops(tmp_path, monkeypatch):
    fast_notices(monkeypatch, throttle=600.0)
    cfg = prod_like_config(tmp_path)
    db, notifier = FakeDB(), FakeNotifier()
    rec = Recorder(db)
    listener = make_listener(tmp_path, cfg, db, rec.on_ready, rec.on_edit, notifier)
    await live(listener, msg(811, "btc 1R long", age=300))
    await wait_until(lambda: len(notifier.notifications) == 1, 1, "first notice")
    await live(listener, msg(812, "btc kapattık", age=300))                     # throttled for 10 minutes
    await asyncio.sleep(0.1)
    await listener.stop()
    assert len(notifier.notifications) == 2 and "btc kapattık" in notifier.notifications[1][0]


class DroppingClient(TgClient):
    """The connection is lost once (Telethon gave up), then it stays up until disconnect()."""

    def __init__(self, history: list[Any]) -> None:
        super().__init__(history)
        self.runs = 0
        self.connects = 0

    async def run_until_disconnected(self) -> None:
        self.runs += 1
        if self.runs == 1:
            raise ConnectionError("network down")
        await self._gone.wait()

    async def connect(self) -> None:
        self.connects += 1


async def test_l1d_reconnect_reports_what_was_skipped_and_for_how_long(tmp_path, monkeypatch):
    monkeypatch.setattr(listener_mod, "RECONNECT_BASE_DELAY_SEC", 0.01)
    cfg = prod_like_config(tmp_path)
    db, notifier = FakeDB(), FakeNotifier()
    db.state[f"last_msg_id:{CHAT}"] = 100
    rec = Recorder(db)
    client = DroppingClient([
        msg(101, "eth stop", age=500), msg(102, "selam", topic=SOHBET, age=300),
        msg(103, "EUR LONG 1R Limit Emir", topic=FX, age=10),
    ])
    listener = make_listener(tmp_path, cfg, db, rec.on_ready, rec.on_edit, notifier, client=client)
    runner = asyncio.ensure_future(listener.run_until_disconnected())
    try:
        await wait_until(lambda: len(notifier.notifications) == 1, 2, "the reconnect notice")
        text = notifier.notifications[0][0]
        assert text.startswith("Telegram bağlantısı yaklaşık") and "kesikti" in text
        assert "1 mesaj" in text and "8 dakika önce: eth stop" in text and "selam" not in text
        await wait_until(lambda: rec.calls == [(CHAT, TRADER, FX)], 2, "the young message")
        assert (CHAT, 102) not in db.messages                         # not a followed topic
    finally:
        await listener.stop()
        await asyncio.wait_for(runner, 2)


def test_l1_reconnect_attempts_are_at_most_30_seconds_apart():
    assert listener_mod.RECONNECT_MAX_DELAY_SEC <= 30.0
    assert getattr(listener_mod, "CATCHUP_POLL_SEC", None) == 30.0


# =========================================================================== #
# L2: the batch is defined at trigger time
# =========================================================================== #
async def test_l2_message_that_waited_behind_a_slow_batch_is_still_read(tmp_path, sqldb):
    """Engine finding 7 (a), scaled: window 1 s; the stream's first batch blocks for 1.6 s."""
    cfg = prod_like_config(tmp_path, context_window_sec=1)
    parser = SlowParser(read_all, lambda msgs, ctx: ParseResult(actions=[none_action([m.message_id for m in msgs])]),
                        delays=(1.6,))
    engine, notifier, brokers = await make_engine(cfg, sqldb, parser)
    listener = make_listener(tmp_path, cfg, sqldb, engine.on_messages_ready, engine.on_message_edited, notifier)
    try:
        await live(listener, msg(501, "eth 1R long piyasadan SL 2215 TP 2290"))
        await wait_until(lambda: len(parser.calls) == 1, 2, "first batch to reach the parser")
        await live(listener, msg(502, "eth stop"))                    # 'close it', while the first batch is busy
        await wait_until(lambda: len(parser.calls) == 2, 4, "the second batch to be read")
        assert [m.message_id for m in parser.calls[1][0]] == [502]
        age = (utcnow() - parser.calls[1][0][0].date).total_seconds()
        assert age > 1.0                                              # older than the window when it was finally read
        await wait_until(lambda: not listener._tasks, 2, "batches to finish")
        row = await sqldb.get_message(CHAT, 502)
        assert row is not None and row.consumed is True
        assert sqldb.pinned_messages(CHAT) == set()                   # nothing stays pinned after its batch
    finally:
        await listener.stop()
        await engine.stop()


async def test_l2_pinned_messages_survive_the_window_and_the_stale_sweep(sqldb):
    old = utcnow() - timedelta(seconds=900)
    for mid, sender, topic, consumed in ((1, TRADER, CRYPTO, False), (2, TRADER, FX, False), (3, OTHER, CRYPTO, False),
                                         (4, TRADER, CRYPTO, True), (5, TRADER, CRYPTO, False)):
        await sqldb.save_message(StoredMessage(chat_id=CHAT, message_id=mid, sender_id=sender, date=old, text="x",
                                               topic_id=topic, consumed=consumed))
    since = utcnow() - timedelta(seconds=300)

    async def ids(**kw: Any) -> list[int]:
        return [m.message_id for m in await sqldb.recent_messages(CHAT, since, **kw)]

    assert await ids() == []                                          # all outside the window
    sqldb.pin_messages(CHAT, [1, 2, 3, 4])
    assert await ids() == [1, 2, 3]                                   # pinned: returned; consumed (4): never
    assert await ids(sender_id=TRADER, topic_id=CRYPTO) == [1]        # the stream filters still apply
    assert await ids(only_unconsumed=False) == []                     # history queries are not affected
    assert await sqldb.recent_messages(CHAT + 1, since) == []         # pins are per chat
    assert await sqldb.consume_stale_messages(utcnow() - timedelta(seconds=120)) == 1     # only 5
    assert (await sqldb.get_message(CHAT, 5)).consumed and not (await sqldb.get_message(CHAT, 1)).consumed
    sqldb.unpin_messages(CHAT, [1, 2])
    assert sqldb.pinned_messages(CHAT) == {3, 4}
    assert await ids() == [3]
    sqldb.unpin_messages(CHAT, [3, 4, 99])
    assert sqldb.pinned_messages(CHAT) == set() and await ids() == []
    assert await sqldb.consume_stale_messages(utcnow() - timedelta(seconds=120)) == 3     # 1, 2, 3 now


async def test_l2_resumed_messages_are_pinned_until_their_batch_ran(tmp_path, sqldb):
    cfg = prod_like_config(tmp_path, debounce_sec=0.2)
    await sqldb.save_message(StoredMessage(chat_id=CHAT, message_id=21, sender_id=TRADER, date=utcnow(), text="eth long",
                                           topic_id=CRYPTO, topic_title=TITLES[CRYPTO]))
    seen: list[set[int]] = []

    async def on_ready(chat_id: int, sender_id: int | None, topic_id: int | None) -> None:
        seen.append(sqldb.pinned_messages(chat_id))

    listener = make_listener(tmp_path, cfg, sqldb, on_ready, Recorder().on_edit)
    try:
        await listener._resume_unprocessed()
        assert sqldb.pinned_messages(CHAT) == {21}
        await wait_until(lambda: seen == [{21}], 2, "the resumed batch")
        await asyncio.sleep(0.05)
        assert sqldb.pinned_messages(CHAT) == set()
    finally:
        await listener.stop()


# =========================================================================== #
# L3: a failed screenshot download is retried twice, then the owner is told
# =========================================================================== #
class FlakyImage(FakeMessage):
    """An image message whose first ``fail_times`` downloads fail."""

    fail_times = 0

    async def download_media(self, file: str) -> str:
        self.downloads += 1
        if self.downloads <= self.fail_times:
            raise ConnectionError("Cannot send requests while disconnected")
        Path(file).write_bytes(b"img")
        return file


def fast_downloads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(listener_mod, "MEDIA_RETRY_DELAYS_SEC", (0.01, 0.01), raising=False)


def l3_listener(tmp_path: Path) -> tuple[TelegramListener, FakeDB, Recorder, FakeNotifier]:
    cfg = prod_like_config(tmp_path)
    db, notifier = FakeDB(), FakeNotifier()
    rec = Recorder(db)
    return make_listener(tmp_path, cfg, db, rec.on_ready, rec.on_edit, notifier), db, rec, notifier


async def test_l3_failed_download_is_tried_three_times_then_the_owner_is_told_at_once(tmp_path, monkeypatch):
    fast_downloads(monkeypatch)
    listener, db, rec, notifier = l3_listener(tmp_path)
    try:
        bad = msg(510, "eth 1R long <ekran>", photo=SimpleNamespace(id=20), download_error=TimeoutError("download timed out"))
        await live(listener, bad)
        await wait_until(lambda: len(notifier.notifications) == 1, 2, "the missing-image notice")
        assert bad.downloads == 3                                     # one try + two more
        row = db.messages[(CHAT, 510)]
        assert row.media_type == "photo" and row.media_path is None   # the parser can see that an image is missing
        text, severity = notifier.notifications[0]
        assert severity is Severity.WARNING
        assert f"<b>{TITLES[CRYPTO]}</b> konusundaki bir mesajın resmi indirilemedi" in text
        assert "3 deneme" in text and "eth 1R long &lt;ekran&gt;" in text and "elle açın" in text
        await wait_until(lambda: rec.calls == [(CHAT, TRADER, CRYPTO)], 2, "the batch still runs")
    finally:
        await listener.stop()


async def test_l3_download_that_fails_once_is_recovered_without_a_notice(tmp_path, monkeypatch):
    fast_downloads(monkeypatch)
    listener, db, rec, notifier = l3_listener(tmp_path)
    try:
        flaky = FlakyImage(id=511, message="", chat_id=CHAT, sender_id=TRADER, reply_to=in_topic(CRYPTO),
                           photo=SimpleNamespace(id=21))
        flaky.fail_times = 1
        await live(listener, flaky)
        await wait_until(lambda: rec.calls == [(CHAT, TRADER, CRYPTO)], 2, "the batch")
        row = db.messages[(CHAT, 511)]
        assert flaky.downloads == 2 and row.media_path and Path(row.media_path).read_bytes() == b"img"
        assert notifier.notifications == []
    finally:
        await listener.stop()


async def test_l3_download_timeout_is_retried_and_reported(tmp_path, monkeypatch):
    fast_downloads(monkeypatch)
    monkeypatch.setattr(listener_mod, "MEDIA_DOWNLOAD_TIMEOUT_SEC", 0.05)
    listener, db, rec, notifier = l3_listener(tmp_path)
    try:
        slow = msg(512, "", photo=SimpleNamespace(id=22), download_delay=5.0)
        await live(listener, slow)
        await wait_until(lambda: len(notifier.notifications) == 1, 3, "the missing-image notice")
        text = notifier.notifications[0][0]
        assert "0.05 saniyede inmedi" in text and "(resim, metin yok)" in text
        assert db.messages[(CHAT, 512)].media_path is None
        await wait_until(lambda: rec.calls == [(CHAT, TRADER, CRYPTO)], 2, "the batch still runs")
    finally:
        await listener.stop()


async def test_l3_oversized_image_file_is_reported_and_context_only_messages_are_not(tmp_path, monkeypatch):
    fast_downloads(monkeypatch)
    listener, db, rec, notifier = l3_listener(tmp_path)
    try:
        big = msg(513, "btc 1R long", document=SimpleNamespace(id=7, mime_type="image/png", attributes=[],
                                                              size=25 * 1024 * 1024))
        await live(listener, big)
        await wait_until(lambda: len(notifier.notifications) == 1, 2, "the notice")
        assert "resim çok büyük: 25 MB" in notifier.notifications[0][0] and big.downloads == 0
        # an old message stored as context only: its image is not worth a notice
        old = msg(514, "", photo=SimpleNamespace(id=23), download_error=ConnectionError("x"), age=3600)
        await listener.handle_message(old, consumed=True)
        await asyncio.sleep(0.1)
        assert old.downloads == 3 and len(notifier.notifications) == 1
    finally:
        await listener.stop()


# =========================================================================== #
# L8: Ctrl+C during the graceful shutdown
# =========================================================================== #
REPO = str(Path(__file__).resolve().parents[1])

CTRLC_CHILD = r'''
"""Child process: the real copytrader.app.serve() with fake components, started like the CLI does
(asyncio.run + except KeyboardInterrupt). argv: repo root, mode ("posix" | "windows"), broker delay.
"windows": the loop refuses signal handlers exactly like the Windows ProactorEventLoop, so Ctrl+C takes the
path used on the owner's VPS (asyncio.run()'s own SIGINT handler)."""
import asyncio, faulthandler, sys, tempfile
from pathlib import Path

repo, mode, delay = sys.argv[1], sys.argv[2], float(sys.argv[3])
sys.path.insert(0, repo)
sys.path.insert(0, repo + "/tests")
faulthandler.dump_traceback_later(25, exit=True)        # a hang ends the child; the parent then fails

from copytrader.app import serve
from fakes import FakeNotifier, FakePriceOracle
from test_app_cli import _app
from test_engine import CHAT, SENDER, add_msg, eth_open, result
from test_fix_engine_shutdown import SlowBroker


def say(*a):
    print(*a, flush=True)


async def amain():
    loop = asyncio.get_running_loop()
    if mode == "windows":
        def no_signal_handlers(*a, **k):
            raise NotImplementedError
        loop.add_signal_handler = no_signal_handlers
    notifier = FakeNotifier()
    app, _ = _app(Path(tempfile.mkdtemp()), notifier)
    slow = SlowBroker("paper1", delay=delay)
    app.engine.configured_brokers = [slow]
    app.engine.brokers = [slow]
    app.engine.prices = FakePriceOracle({"ETH": 2232.85})
    app.parser.push(result(eth_open(used_message_ids=[501])))

    async def signal_arrives():
        await app.listener.started.wait()
        await add_msg(app.db, 501, "eth 1R long")
        flow = asyncio.ensure_future(app.engine.on_messages_ready(CHAT, SENDER))    # like the listener's batch task
        await slow.entry_filled.wait()
        say("ENTRY FILLED")
        await asyncio.gather(flow, return_exceptions=True)

    feeder = asyncio.ensure_future(signal_arrives())
    try:
        await serve(app)
    finally:
        say("BROKER:", ",".join(slow.events))
        say("DATABASE:", "closed" if app.db._conn is None else "open")
        feeder.cancel()


try:
    asyncio.run(amain())
    say("EXIT: returned")
except KeyboardInterrupt:
    say("EXIT: KeyboardInterrupt")                     # the CLI prints "Durduruldu." here and returns 130
'''


def run_ctrlc_child(tmp_path: Path, mode: str, presses: int, repo: str = REPO) -> tuple[list[str], int]:
    """Start the child, press Ctrl+C ``presses`` times (0.25 s apart) once its market entry is filled."""
    script = tmp_path / "ctrlc_child.py"
    script.write_text(CTRLC_CHILD, encoding="utf-8")
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    proc = subprocess.Popen([sys.executable, str(script), repo, mode, "1.0"], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, encoding="utf-8", env=env)
    lines: list[str] = []
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            lines.append(line.rstrip())
            if "ENTRY FILLED" in line:
                for _ in range(presses):
                    proc.send_signal(signal.SIGINT)
                    time.sleep(0.25)
        code = proc.wait(timeout=40)
    finally:
        if proc.poll() is None:
            proc.kill()
    return lines, code


@pytest.mark.skipif(os.name != "posix", reason="sends SIGINT to a child process")
@pytest.mark.parametrize(("mode", "ending"), [("posix", "EXIT: returned"), ("windows", "EXIT: KeyboardInterrupt")])
def test_l8_repeated_ctrl_c_never_aborts_an_order_flow_and_the_process_exits(tmp_path, mode, ending):
    """Engine finding 10: the entry is filled, the stop is not placed yet, the owner presses Ctrl+C three times."""
    lines, code = run_ctrlc_child(tmp_path, mode, presses=3)
    out = "\n".join(lines)
    assert "BROKER: entry filled,SL placed,close" in lines, out       # the stop was placed before the brokers closed
    assert "DATABASE: closed" in lines, out
    assert lines[-1] == ending and code == 0, out                     # the CLI's "Durduruldu." path still works
    assert out.count(app_mod.SHUTDOWN_NOTICE_TR) == 1, out            # one clear line on the first press
    assert out.count(app_mod.SHUTDOWN_REMINDER_TR) == 2, out          # the other two only remind
    assert "Traceback" not in out, out


@pytest.mark.skipif(os.name != "posix", reason="sends SIGINT to a child process")
def test_l8_single_ctrl_c_still_stops_gracefully_on_the_windows_path(tmp_path):
    lines, code = run_ctrlc_child(tmp_path, "windows", presses=1)
    out = "\n".join(lines)
    assert "BROKER: entry filled,SL placed,close" in lines and "DATABASE: closed" in lines, out
    assert lines[-1] == "EXIT: KeyboardInterrupt" and code == 0, out
    assert out.count(app_mod.SHUTDOWN_NOTICE_TR) == 1 and app_mod.SHUTDOWN_REMINDER_TR not in out, out


def test_l8_first_interrupt_line_is_the_agreed_text():
    assert app_mod.SHUTDOWN_NOTICE_TR == "Kapanıyor, lütfen bekleyin (en çok 2 dakika). Tekrar Ctrl+C YAPMAYIN."


class _FakeSignals:
    """Stands in for signal.signal / signal.getsignal: records what serve() installs for SIGINT."""

    def __init__(self, previous: Any) -> None:
        self.handler: Any = previous

    def getsignal(self, sig: int) -> Any:
        return self.handler

    def signal(self, sig: int, handler: Any) -> Any:
        old, self.handler = self.handler, handler
        return old


def windows_like(monkeypatch: pytest.MonkeyPatch, previous: Any) -> _FakeSignals:
    def no_signal_handlers(*a: Any, **k: Any) -> None:
        raise NotImplementedError

    monkeypatch.setattr(asyncio.get_running_loop(), "add_signal_handler", no_signal_handlers)
    fake = _FakeSignals(previous)
    monkeypatch.setattr(app_mod.signal, "getsignal", fake.getsignal)
    monkeypatch.setattr(app_mod.signal, "signal", fake.signal)
    return fake


async def test_l8_windows_handler_keeps_the_first_ctrl_c_and_swallows_the_next_ones(monkeypatch, capsys):
    reached: list[int] = []

    def runner_handler(signum: int, frame: Any) -> None:             # asyncio.run(): cancels the main task
        reached.append(signum)

    fake = windows_like(monkeypatch, runner_handler)
    guard = app_mod._Interrupts(asyncio.Event())
    guard.install()
    wrapper = fake.handler
    assert wrapper is not runner_handler
    for _ in range(3):
        wrapper(signal.SIGINT, None)
    await asyncio.sleep(0.01)
    assert reached == [signal.SIGINT]                                 # only the first press reaches asyncio.run()
    out = capsys.readouterr().out
    assert out.count(app_mod.SHUTDOWN_NOTICE_TR) == 1 and out.count(app_mod.SHUTDOWN_REMINDER_TR) == 2
    guard.restore()
    assert fake.handler is runner_handler                            # put back for asyncio.run()'s own cleanup


async def test_l8_hung_shutdown_can_still_be_interrupted_after_the_deadline(monkeypatch):
    reached: list[int] = []
    fake = windows_like(monkeypatch, lambda signum, frame: reached.append(signum))
    monkeypatch.setattr(app_mod, "HARD_INTERRUPT_AFTER_SEC", 0.0)
    guard = app_mod._Interrupts(asyncio.Event())
    guard.install()
    fake.handler(signal.SIGINT, None)
    fake.handler(signal.SIGINT, None)                                # the deadline has passed: not swallowed any more
    assert len(reached) == 2
    guard.restore()


async def test_l8_handler_that_is_not_asyncio_runs_is_left_as_it_was(monkeypatch):
    def plain(signum: int, frame: Any) -> None:                      # Python's default: raises at once
        raise KeyboardInterrupt

    fake = windows_like(monkeypatch, plain)
    guard = app_mod._Interrupts(asyncio.Event())
    guard.install()
    with pytest.raises(KeyboardInterrupt):
        fake.handler(signal.SIGINT, None)
    assert fake.handler is plain                                     # nothing is swallowed afterwards
    guard.restore()


async def test_l8_posix_signal_after_the_deadline_gets_its_default_effect(monkeypatch, capsys):
    raised: list[int] = []
    monkeypatch.setattr(app_mod.signal, "raise_signal", raised.append)
    monkeypatch.setattr(app_mod, "HARD_INTERRUPT_AFTER_SEC", 0.0)
    stop = asyncio.Event()
    guard = app_mod._Interrupts(stop)
    guard._loop_signals = [int(signal.SIGTERM)]                       # as if the loop handlers were installed
    guard._on_loop_signal(int(signal.SIGTERM))
    assert stop.is_set() and raised == []                             # the first one asks for the graceful stop
    guard._on_loop_signal(int(signal.SIGTERM))                        # the shutdown hangs: not swallowed for ever
    assert raised == [int(signal.SIGTERM)] and guard._loop_signals == []
    out = capsys.readouterr().out
    assert app_mod.SHUTDOWN_NOTICE_TR in out and app_mod.SHUTDOWN_FORCED_TR in out


class CancelledStopNotifier(FakeNotifier):
    async def stop(self) -> None:
        raise asyncio.CancelledError                                 # a shutdown step is interrupted hard


async def test_l8_database_is_closed_even_when_a_shutdown_step_is_interrupted(tmp_path):
    from test_app_cli import _app

    app, _brokers = _app(tmp_path, CancelledStopNotifier())
    stop = asyncio.Event()
    task = asyncio.ensure_future(serve(app, stop))
    await asyncio.wait_for(app.listener.started.wait(), 2)
    stop.set()
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert app.db._conn is None                                  # closed in the finally
    finally:
        await app.db.close()


async def test_l8_close_nowait_ends_the_database_worker_thread(tmp_path):
    db = Database(tmp_path / "x.db")
    await db.open()
    try:
        thread = db._conn._thread                                    # aiosqlite's worker (not a daemon thread)
        assert thread.is_alive()
        db.close_nowait()
        await asyncio.get_running_loop().run_in_executor(None, thread.join, 3)
        assert not thread.is_alive() and db._conn is None
    finally:
        await db.close()                                             # harmless afterwards
    again = Database(tmp_path / "x.db")
    await again.open()                                               # the file is not left locked
    await again.close()


# =========================================================================== #
# L9: secrets never reach the log; the reason of a failed MetaApi websocket connection does
# =========================================================================== #
JWT = "eyJhbGciOiJSUzUxMiJ9.eyJfaWQiOiJhYmMxMjMifQ.c2lnbmF0dXJlLXh5el8xMjM0NTY"
WS_URL = f"https://mt-client-api-v1.london.agiliumtrade.ai/ws/?auth-token={JWT}&clientId=0.4711&protocol=3"


@pytest.fixture
def clean_logging():
    names = ("engineio.client", "socketio.client")
    before = {n: (logging.getLogger(n).level, list(logging.getLogger(n).filters)) for n in names}
    yield
    root = logging.getLogger()
    for h in list(root.handlers):
        if getattr(h, app_mod._HANDLER_TAG, False):
            root.removeHandler(h)
            h.close()
    for n, (level, filters) in before.items():
        logging.getLogger(n).setLevel(level)
        logging.getLogger(n).filters[:] = filters


def log_text(path: Path) -> str:
    for h in logging.getLogger().handlers:
        h.flush()
    return path.read_text(encoding="utf-8")


def test_l9a_secrets_are_masked_in_every_record_whatever_logger_it_comes_from(tmp_path, clean_logging, capsys):
    session = "1" + "Bq7" * 117 + "="                               # shape of a Telethon StringSession
    path = app_mod.setup_logging("INFO", tmp_path, secrets=["Tr4de!pass", "hunter2-mt5"])
    assert path is not None
    logging.getLogger("metaapi_cloud_sdk.client").warning("Attempting polling connection to %s", WS_URL)
    logging.getLogger("some.library").info("login(password='pw-12345', server='FundingPips-SIM') token=abcdef123456")
    logging.getLogger("ccxt.base").error("headers={'X-MBX-APIKEY': '%s'} signature=%s", "k" * 64, "deadbeef01")
    logging.getLogger("copytrader.x").info("bot 123456789:%s, anthropic sk-ant-api03-AbCdEf123456, oturum %s",
                                           "A" * 35, session)
    logging.getLogger("copytrader.x").info("şifre Tr4de!pass ve hunter2-mt5 düz metin içinde")
    try:
        raise ConnectionError(f"Cannot connect to host {WS_URL}")
    except ConnectionError:
        logging.getLogger("engineio.client").exception("websocket error")
    text = log_text(path)
    console = capsys.readouterr().err
    for where in (text, console):
        for secret in (JWT, "pw-12345", "abcdef123456", "k" * 64, "deadbeef01", "A" * 35, "sk-ant-api03",
                       session, "Tr4de!pass", "hunter2-mt5"):
            assert secret not in where, secret
        assert "auth-token=***&clientId=0.4711" in where              # the rest of the line stays readable
        assert "password='***', server='FundingPips-SIM'" in where
        assert "Traceback" in where and "ConnectionError: Cannot connect to host" in where
    # ordinary trading text is not touched
    logging.getLogger("copytrader.x").info("ETH LONG piyasa SL 2215 TP 2290 max_tokens=16000 mesaj 501")
    assert "ETH LONG piyasa SL 2215 TP 2290 max_tokens=16000 mesaj 501" in log_text(path)


def test_l9a_config_secrets_finds_every_secret_of_the_config(tmp_path, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-env-key-000111")
    cfg = AppConfig.model_validate({
        "data_dir": str(tmp_path),
        "telegram": {"api_id": 1, "api_hash": "hash-0123456789", "session_string": "1SessionStringValue",
                     "source_chats": [CHAT]},
        "notifier": {"bot_token": "123456789:" + "B" * 35, "owner_user_id": 42},
        "llm": {"api_key": "sk-ant-cfg-key-222333"},
        "brokers": [
            {"type": "ccxt", "name": "binance", "exchange": "binanceusdm", "api_key": "binance-key-1", "secret": "binance-secret-1"},
            {"type": "mt5", "name": "fx", "login": 12345, "password": "mt5-password-1", "server": "FundingPips-SIM"},
            {"type": "metaapi", "name": "tickmill", "token": JWT, "account_id": "acc-1"},
        ],
    })
    found = app_mod.config_secrets(cfg)
    for secret in ("hash-0123456789", "1SessionStringValue", "123456789:" + "B" * 35, "sk-ant-cfg-key-222333",
                   "binance-key-1", "binance-secret-1", "mt5-password-1", JWT, "sk-ant-env-key-000111"):
        assert secret in found, secret
    assert "FundingPips-SIM" not in found and "binanceusdm" not in found and "acc-1" not in found
    masked = app_mod.mask_secrets("giriş mt5-password-1 ile yapıldı, anahtar binance-key-1", found)
    assert masked == "giriş *** ile yapıldı, anahtar ***"


def test_l9b_reason_of_a_failed_websocket_connection_reaches_the_log_once_a_minute(tmp_path, clean_logging, monkeypatch):
    path = app_mod.setup_logging("INFO", tmp_path)
    assert path is not None
    eio, sio = logging.getLogger("engineio.client"), logging.getLogger("socketio.client")
    cause = "Cannot connect to host localhost:8443 ssl:True [SSLCertVerificationError: CERTIFICATE_VERIFY_FAILED]"
    for _ in range(500):                                              # the SDK's connect loop
        eio.info("Attempting polling connection to " + WS_URL)
        eio.info("HTTP %s request to %s failed with error %s.", "GET", WS_URL, cause)
        sio.info("Connection failed, new attempt in {:.02f} seconds".format(1.0))
        sio.info('Emitting event "%s" [%s]', "request", "/")
    eio.warning("WebSocket upgrade failed: connection error")
    lines = log_text(path).splitlines()
    assert len(lines) == 3, lines
    assert "WARNING engineio.client: HTTP GET request to" in lines[0] and "CERTIFICATE_VERIFY_FAILED" in lines[0]
    assert "auth-token=***&clientId" in lines[0] and JWT not in "\n".join(lines)
    assert "WARNING socketio.client: Connection failed, new attempt in 1.00 seconds" in lines[1]
    assert "WebSocket upgrade failed" in lines[2]
    # a minute later the next line says how many were left out
    gate = next(f for f in eio.filters if isinstance(f, app_mod.ConnectFailureFilter))
    gate._last = {k: v - 61.0 for k, v in gate._last.items()}
    eio.info("HTTP %s request to %s failed with error %s.", "GET", WS_URL, cause)
    last = log_text(path).splitlines()[-1]
    assert "CERTIFICATE_VERIFY_FAILED" in last and "(+499 similar line(s) not logged)" in last
    # setup_logging stays idempotent: one gate per logger, other noisy loggers stay at WARNING
    app_mod.setup_logging("DEBUG", tmp_path)
    assert sum(isinstance(f, app_mod.ConnectFailureFilter) for f in eio.filters) == 1
    assert logging.getLogger("engineio").level >= logging.WARNING and logging.getLogger("telethon").level >= logging.WARNING


# =========================================================================== #
# L4: the "same photo" check of an edit works after a restart
# =========================================================================== #
async def test_l4_edit_that_changes_nothing_is_ignored_after_a_restart(tmp_path, sqldb):
    """Telegram finding 6: a new process took such an update for a new screenshot and re-read the signal."""
    cfg = prod_like_config(tmp_path)
    rec = Recorder()
    first = make_listener(tmp_path, cfg, sqldb, rec.on_ready, rec.on_edit)
    photo = msg(701, "eth 1R long", photo=SimpleNamespace(id=555))
    await live(first, photo)
    await wait_until(lambda: rec.calls == [(CHAT, TRADER, CRYPTO)], 2, "the signal")
    assert photo.downloads == 1
    await first.stop()

    rec2 = Recorder()                                                # the bot restarts: same database, same media folder
    second = make_listener(tmp_path, cfg, sqldb, rec2.on_ready, rec2.on_edit)
    try:
        same = msg(701, "eth 1R long", photo=SimpleNamespace(id=555))  # e.g. somebody reacted to the message
        await second.handle_edit(same)
        assert same.downloads == 0 and rec2.edits == []
        assert (await sqldb.message_meta(CHAT, 701))["media_id"] == 555
        replaced = msg(701, "eth 1R long", photo=SimpleNamespace(id=556))   # a really new screenshot is still seen
        await second.handle_edit(replaced)
        assert replaced.downloads == 1 and len(rec2.edits) == 1
        assert (await sqldb.message_meta(CHAT, 701))["media_id"] == 556
        again = msg(701, "eth 1R long", photo=SimpleNamespace(id=556))
        await second.handle_edit(again)
        assert again.downloads == 0 and len(rec2.edits) == 1
    finally:
        await second.stop()


async def test_l4_row_stored_without_a_photo_id_is_judged_by_its_text_and_file(tmp_path, sqldb):
    cfg = prod_like_config(tmp_path)
    rec = Recorder()
    first = make_listener(tmp_path, cfg, sqldb, rec.on_ready, rec.on_edit)
    await live(first, msg(702, "btc 1R short", photo=SimpleNamespace(id=600)))
    await wait_until(lambda: rec.calls, 2, "the signal")
    await first.stop()
    await sqldb.set_message_meta(CHAT, 702, media_id=None)           # as stored by the version before this one

    rec2 = Recorder()
    second = make_listener(tmp_path, cfg, sqldb, rec2.on_ready, rec2.on_edit)
    try:
        same_text = msg(702, "btc 1R short", photo=SimpleNamespace(id=600))
        await second.handle_edit(same_text)
        assert same_text.downloads == 0 and rec2.edits == []          # id unknown, file there, text the same: nothing
        new_text = msg(702, "btc 1R short (stop güncellendi)", photo=SimpleNamespace(id=600))
        await second.handle_edit(new_text)
        assert new_text.downloads == 1 and len(rec2.edits) == 1       # text changed: the image is fetched again to be sure
        row = await sqldb.get_message(CHAT, 702)
        Path(row.media_path).unlink()                                 # the stored file is gone
        third = TelegramListener(cfg.telegram, sqldb, str(tmp_path), rec2.on_ready, rec2.on_edit)
        third._entities, third._forums, third._client = second._entities, second._forums, second._client
        lost = msg(702, "btc 1R short (stop güncellendi)", photo=SimpleNamespace(id=600))
        await third.handle_edit(lost)
        assert lost.downloads == 1
        await third.stop()
    finally:
        await second.stop()


# =========================================================================== #
# L5: an anonymous admin's batch holds only messages without a sender
# =========================================================================== #
async def test_l5_anonymous_admin_does_not_make_the_engine_re_read_another_admins_signal(tmp_path, sqldb):
    """Telegram finding 10: batches were [[401], [401, 402]] and the owner got a 'duplicate' question."""
    cfg = prod_like_config(tmp_path)

    def reader(msgs: list[StoredMessage], ctx: Any) -> ParseResult:
        ids = [m.message_id for m in msgs]
        return ParseResult(actions=[eth_long(ids) if 401 in ids else none_action(ids)], model="fake")

    parser = SlowParser(reader, delays=(0.6,))
    engine, notifier, brokers = await make_engine(cfg, sqldb, parser)
    listener = make_listener(tmp_path, cfg, sqldb, engine.on_messages_ready, engine.on_message_edited, notifier)
    try:
        await live(listener, msg(401, "eth 1R long piyasadan SL 2215 TP 2290", sender=TRADER))
        await wait_until(lambda: len(parser.calls) == 1, 2, "the signal to reach the parser")
        await live(listener, msg(402, "Aktif", sender=None))          # anonymous admin, same topic, 401 still being read
        await wait_until(lambda: len(parser.calls) == 2, 3, "the anonymous admin's batch")
        await wait_until(lambda: not listener._tasks, 3, "batches to finish")
        assert [[m.message_id for m in batch] for batch, _ in parser.calls] == [[401], [402]]
        assert notifier.confirmations == [] and len(await sqldb.active_trades()) == 1
        assert len(brokers["binance"].opened) == 1
    finally:
        await listener.stop()
        await engine.stop()


async def test_l5_recent_messages_tells_no_sender_filter_from_sender_is_null(sqldb):
    from copytrader.storage.db import ANY_SENDER

    for mid, sender in ((1, TRADER), (2, None), (3, OTHER), (4, None)):
        await sqldb.save_message(StoredMessage(chat_id=CHAT, message_id=mid, sender_id=sender, date=utcnow(), text="x",
                                               topic_id=CRYPTO))
    since = utcnow() - timedelta(minutes=5)

    async def ids(*args: Any, **kw: Any) -> list[int]:
        return [m.message_id for m in await sqldb.recent_messages(CHAT, since, *args, **kw)]

    assert await ids() == [1, 2, 3, 4]                                # no filter (listener start-up, history)
    assert await ids(sender_id=ANY_SENDER) == [1, 2, 3, 4]
    assert await ids(sender_id=TRADER) == [1] and await ids(TRADER, True, 50, CRYPTO) == [1]
    assert await ids(sender_id=None) == [2, 4]                        # the anonymous stream: only its own messages
    assert await ids(None, True, 50, CRYPTO) == [2, 4]


# =========================================================================== #
# L6: deleted, content-less and forwarded messages
# =========================================================================== #
async def deleted(listener: TelegramListener, *ids: int) -> None:
    """Deliver a 'messages deleted' update the way Telethon does."""
    await listener._on_deleted_event(SimpleNamespace(chat_id=CHAT, deleted_ids=list(ids)))
    await asyncio.sleep(0.05)


async def test_l6a_signal_deleted_before_it_was_processed_is_never_executed(tmp_path, sqldb):
    cfg = prod_like_config(tmp_path, debounce_sec=1.0)
    parser = FakeParser(read_all)
    engine, notifier, brokers = await make_engine(cfg, sqldb, parser)
    listener = make_listener(tmp_path, cfg, sqldb, engine.on_messages_ready, engine.on_message_edited, notifier)
    try:
        await live(listener, msg(901, "eth 1R long piyasadan SL 2215 TP 2290"))
        await asyncio.sleep(0.1)
        await deleted(listener, 901, 55555)                           # posted by mistake, deleted within seconds
        await asyncio.sleep(1.4)                                      # the debounce has fired by now
        row = await sqldb.get_message(CHAT, 901)
        assert row is not None and row.consumed is True
        assert parser.calls == [] and await sqldb.active_trades() == []
        assert all(b.opened == [] for b in brokers.values())
        assert sqldb.pinned_messages(CHAT) == set()
        assert (await sqldb.message_meta(CHAT, 901))["deleted"] is True      # kept with the row (for the engine / parser)
        assert len(notifier.notifications) == 1
        text, severity = notifier.notifications[0]
        assert severity is Severity.INFO and "işlenmeden önce silindi" in text
        assert "eth 1R long piyasadan SL 2215 TP 2290" in text and TITLES[CRYPTO] in text
    finally:
        await listener.stop()
        await engine.stop()


async def test_l6a_deleted_signal_of_an_open_trade_is_reported_and_nothing_is_closed(tmp_path, sqldb):
    cfg = prod_like_config(tmp_path)
    parser = FakeParser(read_all)
    engine, notifier, brokers = await make_engine(cfg, sqldb, parser)
    listener = make_listener(tmp_path, cfg, sqldb, engine.on_messages_ready, engine.on_message_edited, notifier)
    try:
        await live(listener, msg(911, "eth 1R long piyasadan SL 2215 TP 2290"))
        await wait_until(lambda: brokers["tickmill"].opened, 3, "the trade")
        await wait_until(lambda: not listener._tasks, 3, "the batch to finish")
        notifier.notifications.clear()
        await deleted(listener, 911)
        await wait_until(lambda: notifier.notifications, 2, "the notice")
        assert len(notifier.notifications) == 1
        text, severity = notifier.notifications[0]
        assert severity is Severity.WARNING
        assert "#1 ETH LONG işleminin sinyal mesajı silindi" in text and "kendiliğinden kapatmaz" in text
        trades = await sqldb.active_trades()
        assert len(trades) == 1                                       # information only
        for broker in brokers.values():
            assert broker.calls_of("close_position") == [] and broker.calls_of("cancel_entry") == []
        await deleted(listener, 911)                                  # the same update twice: still nothing closed
        assert len(await sqldb.active_trades()) == 1
    finally:
        await listener.stop()
        await engine.stop()


async def test_l6a_message_deleted_while_its_image_downloads_is_stored_as_context_only(tmp_path):
    cfg = prod_like_config(tmp_path)
    db, notifier = FakeDB(), FakeNotifier()
    rec = Recorder(db)

    async def mark_consumed(chat_id: int, ids: list[int]) -> None:   # the listener tests' FakeDB has no such method
        for i in ids:
            db.messages[(chat_id, i)] = db.messages[(chat_id, i)].model_copy(update={"consumed": True})

    db.mark_consumed = mark_consumed                                  # type: ignore[attr-defined]
    listener = make_listener(tmp_path, cfg, db, rec.on_ready, rec.on_edit, notifier)
    try:
        slow = msg(921, "btc 1R long", photo=SimpleNamespace(id=30), download_delay=1.0)
        await live(listener, slow)
        await asyncio.sleep(0.1)
        await deleted(listener, 921)                                  # no row yet: remembered
        await wait_until(lambda: (CHAT, 921) in db.messages, 4, "the row")
        await asyncio.sleep(0.2)
        assert db.messages[(CHAT, 921)].consumed is True and rec.calls == []
        assert [t for t, _ in notifier.notifications if "işlenmeden önce silindi" in t]
    finally:
        await listener.stop()


async def test_l6a_start_registers_a_handler_for_deleted_messages_of_the_source_chats(tmp_path, monkeypatch):
    from telethon import events

    client = TgClient([])
    listener, db, rec, notifier = await started(tmp_path, monkeypatch, client)
    try:
        builders = [b for _, b in client.handlers if isinstance(b, events.MessageDeleted)]
        assert len(builders) == 1 and builders[0].chats == [CHAT]
    finally:
        await listener.stop()


async def test_l6b_message_without_text_or_image_is_context_only(tmp_path, sqldb):
    """Telegram finding 13: a sticker / voice note stayed 'waiting' and caused a false warning at the next start."""
    cfg = prod_like_config(tmp_path)
    rec = Recorder()
    listener = make_listener(tmp_path, cfg, sqldb, rec.on_ready, rec.on_edit)
    try:
        voice = msg(931, "", document=SimpleNamespace(id=9, mime_type="audio/ogg", attributes=[], size=10))
        await live(listener, voice)
        await live(listener, msg(932, ""))                            # e.g. a poll or a contact
        await asyncio.sleep(0.3)
        for mid in (931, 932):
            row = await sqldb.get_message(CHAT, mid)
            assert row is not None and row.consumed is True
        assert rec.calls == []
        assert await sqldb.consume_stale_messages(utcnow() + timedelta(hours=1)) == 0   # nothing left for the sweep
    finally:
        await listener.stop()


async def test_l6c_forwarded_flag_is_stored_with_the_row(tmp_path, sqldb):
    cfg = prod_like_config(tmp_path)
    rec = Recorder()
    listener = make_listener(tmp_path, cfg, sqldb, rec.on_ready, rec.on_edit)
    try:
        forwarded = msg(941, "btc 1R long (geçen haftanın sinyali)")
        forwarded.fwd_from = SimpleNamespace(from_name="Kadir", date=utcnow() - timedelta(days=7))
        await live(listener, forwarded)
        await live(listener, msg(942, "eth 1R long"))
        await asyncio.sleep(0.2)
        assert (await sqldb.message_meta(CHAT, 941)) == {"media_id": None, "forwarded": True, "deleted": False}
        assert (await sqldb.message_meta(CHAT, 942)) == {"media_id": None, "forwarded": False, "deleted": False}
        assert await sqldb.message_meta(CHAT, 999) is None
        with pytest.raises(ValueError):
            await sqldb.set_message_meta(CHAT, 941, consumed=False)   # only the two extras can be set this way
    finally:
        await listener.stop()


# =========================================================================== #
# L7: topics that match no route
# =========================================================================== #
def service(msg_id: int, action: Any, reply_to: Any = None) -> Any:
    from telethon.tl import types

    return types.MessageService(id=msg_id, peer_id=types.PeerChannel(int(str(CHAT)[4:])), date=utcnow(),
                                action=action, reply_to=reply_to, from_id=types.PeerUser(TRADER))


async def test_l7_first_listing_says_once_which_topics_are_followed_and_which_are_not(tmp_path, monkeypatch):
    db = FakeDB()
    listener, db, rec, notifier = await started(tmp_path, monkeypatch, TgClient([]), db=db, seed_topics=False)
    await asyncio.sleep(0.05)
    await listener.stop()
    assert len(notifier.notifications) == 1
    text, severity = notifier.notifications[0]
    assert severity is Severity.INFO
    assert f"Takip edilen konular: '{TITLES[CRYPTO]}' (kripto), '{TITLES[FX]}' (fx)." in text
    assert "Hiçbir rotaya uymayan konular (bu konulardaki mesajlar kopyalanmayacak): 'Sohbet'." in text
    assert "General" not in text
    # the next start repeats nothing
    listener2, _, _, notifier2 = await started(tmp_path, monkeypatch, TgClient([]), db=db, seed_topics=False)
    await asyncio.sleep(0.05)
    await listener2.stop()
    assert notifier2.notifications == []


async def test_l7_new_or_renamed_topic_that_matches_no_route_is_reported_once(tmp_path, monkeypatch):
    from telethon.tl import types

    client = TgClient([])
    listener, db, rec, notifier = await started(tmp_path, monkeypatch, client)
    try:
        assert notifier.notifications == []
        # 1 December: the new month's topic is created under a name no route knows
        await listener.handle_message(service(950, types.MessageActionTopicCreate(title="Aralık Kripto <Sinyal>", icon_color=0)))
        await wait_until(lambda: len(notifier.notifications) == 1, 2, "the notice")
        text, severity = notifier.notifications[0]
        assert severity is Severity.WARNING
        assert text.startswith("Yeni konu 'Aralık Kripto &lt;Sinyal&gt;' hiçbir rotaya uymuyor; "
                               "bu konudaki mesajlar kopyalanmayacak")
        await live(listener, msg(951, "eth 1R long", topic=950))
        await live(listener, msg(952, "btc 1R long", topic=950))
        await asyncio.sleep(0.2)
        assert (CHAT, 951) not in db.messages and rec.calls == [] and len(notifier.notifications) == 1   # once

        # a topic first seen through a message (one read-only lookup)
        client.titles[77] = "Çekiliş"
        await live(listener, msg(953, "katılın", topic=77))
        await live(listener, msg(954, "son gün", topic=77))
        await wait_until(lambda: len(notifier.notifications) == 2, 2, "the notice")
        await asyncio.sleep(0.1)
        assert len(notifier.notifications) == 2 and "Yeni konu 'Çekiliş' hiçbir rotaya uymuyor" in notifier.notifications[1][0]

        # the followed crypto topic is renamed to something no route matches
        await listener.handle_message(service(955, types.MessageActionTopicEdit(title="Kripto Odası"), in_topic(CRYPTO)))
        await wait_until(lambda: len(notifier.notifications) == 3, 2, "the rename notice")
        text, severity = notifier.notifications[2]
        assert severity is Severity.WARNING
        assert f"Takip edilen '{TITLES[CRYPTO]}' konusunun adı 'Kripto Odası' olarak değişti" in text
        assert "artık hiçbir rotaya uymuyor" in text

        # a new topic that DOES match is followed, and the owner hears that too
        await listener.handle_message(service(960, types.MessageActionTopicCreate(title="Aralık Ayı Fx İşlemleri", icon_color=0)))
        await wait_until(lambda: len(notifier.notifications) == 4, 2, "the 'followed' notice")
        assert notifier.notifications[3] == ("Yeni konu 'Aralık Ayı Fx İşlemleri' takip ediliyor (rota: fx).", Severity.INFO)
        await live(listener, msg(961, "EUR LONG 1R Limit Emir", topic=960))
        await wait_until(lambda: rec.calls == [(CHAT, TRADER, 960)], 2, "the new topic's signal")
        assert client.forbidden == []
    finally:
        await listener.stop()


async def test_l7_route_without_a_matching_topic_is_reported_at_start(tmp_path, monkeypatch):
    client = TgClient([], titles={CRYPTO: TITLES[CRYPTO], SOHBET: "Sohbet", 1: "General"})
    listener, db, rec, notifier = await started(tmp_path, monkeypatch, client)
    await asyncio.sleep(0.05)
    await listener.stop()
    texts = notifier.texts(Severity.WARNING)
    assert len(texts) == 1 and texts[0].startswith("'fx' rotasına uyan bir konu şu anda grupta yok (aranan: 'Fx İşlemleri').")


# =========================================================================== #
# L10: what happens today when a screenshot arrives alone and its text later than the debounce
# =========================================================================== #
async def test_l10_photo_alone_then_text_after_the_debounce_costs_two_model_readings(tmp_path, sqldb):
    """Scaled 1:10 from production (debounce 4 s, text 6 s after the photo): debounce 0.4 s, text 0.6 s later.
    The photo is read alone first (incomplete), then photo + text are read again and the order goes out."""
    cfg = prod_like_config(tmp_path, debounce_sec=0.4)
    incomplete = ParsedAction(action=ActionType.INCOMPLETE, used_message_ids=[1001], confidence=0.9,
                              summary_tr="grafik var, yön / risk yok")
    parser = FakeParser(ParseResult(actions=[incomplete], model="fake"), read_all)
    engine, notifier, brokers = await make_engine(cfg, sqldb, parser)
    listener = make_listener(tmp_path, cfg, sqldb, engine.on_messages_ready, engine.on_message_edited, notifier)
    t0 = time.monotonic()
    stamps: list[float] = []
    real_parse = parser.parse

    async def timed_parse(messages: list[StoredMessage], context: Any) -> ParseResult:
        stamps.append(time.monotonic() - t0)
        return await real_parse(messages, context)

    parser.parse = timed_parse                                        # type: ignore[method-assign]
    try:
        await live(listener, msg(1001, "", photo=SimpleNamespace(id=41)))
        await asyncio.sleep(0.6)
        await live(listener, msg(1002, "eth 1R long"))
        await wait_until(lambda: brokers["binance"].opened, 3, "the order")
        assert [[m.message_id for m in batch] for batch, _ in parser.calls] == [[1001], [1001, 1002]]
        assert stamps[0] >= 0.39 and stamps[1] >= 0.99                # debounce after the photo; debounce after the text
        assert len(await sqldb.active_trades()) == 1
    finally:
        await listener.stop()
        await engine.stop()


# =========================================================================== #
# storage: the live database upgrades by itself and can still be opened by the previous version
# =========================================================================== #
BASE_SCHEMA = """
CREATE TABLE schema_version (version INTEGER NOT NULL);
INSERT INTO schema_version (version) VALUES (2);
CREATE TABLE messages (
    chat_id              INTEGER NOT NULL,
    message_id           INTEGER NOT NULL,
    sender_id            INTEGER,
    sender_name          TEXT,
    date                 TEXT    NOT NULL,
    text                 TEXT    NOT NULL DEFAULT '',
    media_path           TEXT,
    media_type           TEXT,
    reply_to_message_id  INTEGER,
    grouped_id           INTEGER,
    edited               INTEGER NOT NULL DEFAULT 0,
    consumed             INTEGER NOT NULL DEFAULT 0,
    topic_id             INTEGER,
    topic_title          TEXT,
    PRIMARY KEY (chat_id, message_id)
);
CREATE INDEX ix_messages_chat_date ON messages (chat_id, date);
"""
# The previous version's insert (it names its 14 columns and reads rows by column name).
BASE_INSERT = """INSERT INTO messages (chat_id, message_id, sender_id, sender_name, date, text, media_path, media_type,
    reply_to_message_id, grouped_id, topic_id, topic_title, edited, consumed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""


async def test_db_made_by_the_previous_version_upgrades_itself_and_stays_readable_by_it(tmp_path):
    from copytrader.storage import db as dbmod

    path = tmp_path / "copytrader.db"
    raw = sqlite3.connect(path)
    raw.executescript(BASE_SCHEMA)
    raw.execute(BASE_INSERT, (CHAT, 101, TRADER, "Kadir", "2026-11-12T09:00:01.000000+00:00", "eth 1R long",
                              "/data/media/x_101.jpg", "photo", None, None, CRYPTO, TITLES[CRYPTO], 0, 1))
    raw.commit()
    raw.close()

    db = Database(path)
    await db.open()                                                   # the new program starts on the live file
    try:
        row = await db.get_message(CHAT, 101)
        assert row is not None and row.text == "eth 1R long" and row.consumed and row.media_type == "photo"
        assert await db.message_meta(CHAT, 101) == {"media_id": None, "forwarded": False, "deleted": False}
        await db.set_message_meta(CHAT, 101, media_id=2**62 + 5, forwarded=True)  # Telegram photo ids are 64-bit
        assert await db.message_meta(CHAT, 101) == {"media_id": 2**62 + 5, "forwarded": True, "deleted": False}
        await db.save_message(StoredMessage(chat_id=CHAT, message_id=101, sender_id=TRADER, text="eth 1R long (edit)",
                                            date=utcnow(), media_path="/data/media/x_101.jpg", media_type="photo"))
        assert (await db.message_meta(CHAT, 101))["media_id"] == 2**62 + 5                # an edit keeps the extras
        assert (await db.message_meta(CHAT, 101))["forwarded"] is True
    finally:
        await db.close()
    await db.open()                                                   # a second start changes nothing
    await db.close()

    raw = sqlite3.connect(path)
    try:
        columns = [r[1] for r in raw.execute("PRAGMA table_info(messages)")]
        assert columns[-3:] == ["media_id", "forwarded", "deleted"] and len(columns) == 17
        # The schema version is NOT raised: the previous program refuses a newer version, and a rollback
        # of the code must not lock the owner out of the database.
        assert raw.execute("SELECT version FROM schema_version").fetchall() == [(2,)] and dbmod.SCHEMA_VERSION == 2
        # ... and the previous program's own statements still work on the upgraded file
        raw.execute(BASE_INSERT, (CHAT, 102, None, None, "2026-11-12T09:00:02.000000+00:00", "kapattık", None, None,
                                  None, None, FX, TITLES[FX], 0, 0))
        raw.commit()
        assert raw.execute("SELECT forwarded, deleted, media_id FROM messages WHERE message_id = 102").fetchone() \
            == (0, 0, None)
        assert raw.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        raw.close()
    db = Database(path)
    await db.open()
    assert [m.message_id for m in await db.recent_messages(CHAT, datetime(2026, 11, 1, tzinfo=timezone.utc),
                                                           only_unconsumed=False)] == [101, 102]
    await db.close()


async def test_db_fills_message_flags_into_the_model_once_it_has_such_fields(sqldb, monkeypatch):
    """StoredMessage has no 'forwarded' / 'deleted' field yet (models.py); when it gets them, rows carry them."""
    from copytrader.storage import db as dbmod

    class FlaggedMessage(StoredMessage):
        forwarded: bool = False
        deleted: bool = False

    await sqldb.save_message(StoredMessage(chat_id=CHAT, message_id=7, sender_id=TRADER, date=utcnow(), text="x"))
    await sqldb.set_message_meta(CHAT, 7, forwarded=True)
    plain = await sqldb.get_message(CHAT, 7)
    assert type(plain) is StoredMessage and dbmod._OPTIONAL_MESSAGE_FIELDS == ()
    monkeypatch.setattr(dbmod, "StoredMessage", FlaggedMessage)
    monkeypatch.setattr(dbmod, "_OPTIONAL_MESSAGE_FIELDS", ("forwarded", "deleted"))
    flagged = await sqldb.get_message(CHAT, 7)
    assert isinstance(flagged, FlaggedMessage) and flagged.forwarded is True and flagged.deleted is False


# =========================================================================== #
# real Telethon objects / remaining log-only paths of the listener
# =========================================================================== #
async def test_l6a_real_telethon_delete_update_of_the_supergroup_reaches_the_handler(tmp_path):
    from telethon import events
    from telethon.tl import types

    cfg = prod_like_config(tmp_path)
    db, notifier = FakeDB(), FakeNotifier()
    rec = Recorder(db)

    async def mark_consumed(chat_id: int, ids: list[int]) -> None:
        for i in ids:
            db.messages[(chat_id, i)] = db.messages[(chat_id, i)].model_copy(update={"consumed": True})

    db.mark_consumed = mark_consumed                                  # type: ignore[attr-defined]
    listener = make_listener(tmp_path, cfg, db, rec.on_ready, rec.on_edit, notifier)
    try:
        listener.hold_batches()                                       # e.g. still waiting for its batch
        await live(listener, msg(971, "sol 1R short"))
        builder = events.MessageDeleted(chats=[CHAT])
        await builder.resolve(None)
        update = types.UpdateDeleteChannelMessages(channel_id=int(str(CHAT)[4:]), messages=[971], pts=10, pts_count=1)
        event = events.MessageDeleted.build(update, None, 999)
        assert builder.filter(event)                                  # the supergroup's own update passes the filter
        elsewhere = events.MessageDeleted.build(
            types.UpdateDeleteChannelMessages(channel_id=1234567890, messages=[971], pts=1, pts_count=1), None, 999)
        assert not builder.filter(elsewhere)                          # same id in another channel does not
        await listener._on_deleted_event(event)
        await asyncio.sleep(0.1)
        assert db.messages[(CHAT, 971)].consumed is True
        listener.release_batches()
        await asyncio.sleep(0.2)
        assert [t for t, _ in notifier.notifications if "işlenmeden önce silindi" in t and "sol 1R short" in t]
    finally:
        await listener.stop()


async def test_engine_error_escaping_a_batch_and_a_failed_save_reach_the_owner(tmp_path):
    """Engine finding 8 (listener side): these two paths wrote a log line only."""
    cfg = prod_like_config(tmp_path)
    db, notifier = FakeDB(), FakeNotifier()
    rec = Recorder(db, fail=True)                                     # on_ready raises "engine exploded"
    listener = make_listener(tmp_path, cfg, db, rec.on_ready, rec.on_edit, notifier)
    try:
        await live(listener, msg(981, "eth 1R long"))
        await wait_until(lambda: notifier.notifications, 2, "the error notice")
        text, severity = notifier.notifications[0]
        assert severity is Severity.ERROR and "beklenmeyen bir hata" in text and "engine exploded" in text

        async def broken_save(message: StoredMessage) -> None:
            raise RuntimeError("database is locked")

        db.save_message = broken_save                                 # type: ignore[method-assign]
        await live(listener, msg(982, "btc 1R long", topic=FX))
        await wait_until(lambda: len(notifier.notifications) >= 2, 2, "the save notice")
        assert any("kaydedilemedi" in t and "database is locked" in t for t, _ in notifier.notifications)
    finally:
        await listener.stop()
