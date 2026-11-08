import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from telethon.tl import types

from copytrader.config import TelegramConfig
from copytrader.models import StoredMessage
from copytrader.telegram.listener import (
    TelegramListener,
    image_info,
    normalize_chat_ref,
    should_process_catchup,
)

CHAT = -1001234567890
TRADER = 111
OTHER = 222


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class FakeMessage:
    id: int
    message: str = ""
    chat_id: int = CHAT
    sender_id: int | None = TRADER
    date: datetime = field(default_factory=utcnow)
    sender: Any = field(default_factory=lambda: SimpleNamespace(first_name="Ali", last_name="Trader", username="ali"))
    photo: Any = None
    document: Any = None
    reply_to: Any = None
    grouped_id: int | None = None
    out: bool = False
    download_delay: float = 0.0
    download_error: Exception | None = None
    downloads: int = 0

    async def download_media(self, file: str) -> str:
        await asyncio.sleep(self.download_delay)
        self.downloads += 1
        if self.download_error:
            raise self.download_error
        Path(file).write_bytes(b"img")
        return file


class FakeDB:
    def __init__(self) -> None:
        self.messages: dict[tuple[int, int], StoredMessage] = {}
        self.state: dict[str, Any] = {}

    async def save_message(self, msg: StoredMessage) -> None:
        old = self.messages.get((msg.chat_id, msg.message_id))
        if old is not None:                       # contract: an edit keeps `consumed`
            msg = msg.model_copy(update={"consumed": old.consumed})
        self.messages[(msg.chat_id, msg.message_id)] = msg

    async def get_message(self, chat_id: int, message_id: int) -> StoredMessage | None:
        return self.messages.get((chat_id, message_id))

    async def recent_messages(self, chat_id, since, sender_id=None, only_unconsumed=True, limit=50, topic_id=None):
        out = [
            m for m in self.messages.values()
            if m.chat_id == chat_id and m.date >= since and (not only_unconsumed or not m.consumed)
            and (sender_id is None or m.sender_id == sender_id)
            and (topic_id is None or m.topic_id == topic_id)
        ]
        return sorted(out, key=lambda m: m.message_id)[:limit]

    async def get_state(self, key: str, default: Any = None) -> Any:
        return self.state.get(key, default)

    async def set_state(self, key: str, value: Any) -> None:
        self.state[key] = value


class ReadOnlyFakeClient:
    """Only read methods exist; anything else (send, forward, read-ack...) fails the test."""

    def __init__(self, history: list[FakeMessage]) -> None:
        self.history = history

    async def get_messages(self, entity, limit=1):
        return sorted(self.history, key=lambda m: m.id, reverse=True)[:limit]

    def iter_messages(self, entity, min_id=0, limit=None):
        async def gen():
            for m in sorted(self.history, key=lambda m: m.id, reverse=True)[:limit]:
                if m.id > min_id:
                    yield m
        return gen()

    def __getattr__(self, name: str):
        raise AssertionError(f"listener must not call client.{name}")


class Recorder:
    def __init__(self, db: FakeDB | None = None, fail: bool = False) -> None:
        self.ready: list[tuple[int, int | None]] = []                 # (chat_id, sender_id)
        self.calls: list[tuple[int, int | None, int | None]] = []     # (chat_id, sender_id, topic_id)
        self.edits: list[StoredMessage] = []
        self.snapshots: list[list[int]] = []
        self.db = db
        self.fail = fail

    async def on_ready(self, chat_id: int, sender_id: int | None, topic_id: int | None) -> None:
        self.ready.append((chat_id, sender_id))
        self.calls.append((chat_id, sender_id, topic_id))
        if self.db is not None:
            self.snapshots.append(sorted(k[1] for k in self.db.messages))
        if self.fail:
            raise RuntimeError("engine exploded")

    async def on_edit(self, msg: StoredMessage) -> None:
        self.edits.append(msg)


def make_listener(tmp_path, db=None, rec=None, **cfg) -> tuple[TelegramListener, FakeDB, Recorder]:
    db = db or FakeDB()
    rec = rec or Recorder(db)
    config = TelegramConfig(
        api_id=1, api_hash="x", source_chats=[CHAT], debounce_sec=cfg.pop("debounce_sec", 0.05), **cfg
    )
    listener = TelegramListener(config, db, str(tmp_path), rec.on_ready, rec.on_edit)
    return listener, db, rec


async def settle(seconds: float = 0.25) -> None:
    await asyncio.sleep(seconds)


# --------------------------------------------------------------------------- #
async def test_message_conversion_with_photo_and_reply(tmp_path):
    listener, db, rec = make_listener(tmp_path)
    msg = FakeMessage(
        id=42, message="eth 1R long", photo=SimpleNamespace(id=555), grouped_id=99,
        reply_to=SimpleNamespace(reply_to_msg_id=40, reply_to_peer_id=None, forum_topic=False, reply_to_top_id=None),
        date=datetime(2026, 11, 7, 10, 0),  # naive -> treated as UTC
    )
    await listener.handle_message(msg)
    stored = db.messages[(CHAT, 42)]
    assert stored.text == "eth 1R long"
    assert stored.sender_id == TRADER and stored.sender_name == "Ali Trader"
    assert stored.date.tzinfo is not None and stored.date.utcoffset() == timedelta(0)
    assert stored.reply_to_message_id == 40
    assert stored.grouped_id == 99
    assert stored.media_type == "photo"
    assert stored.media_path == str(tmp_path / "media" / f"{abs(CHAT)}_42.jpg")
    assert Path(stored.media_path).read_bytes() == b"img"
    assert not stored.edited and not stored.consumed
    assert stored.topic_id is None and stored.topic_title is None      # not a forum
    assert db.state[f"last_msg_id:{CHAT}"] == 42
    await settle()
    assert rec.calls == [(CHAT, TRADER, None)]


async def test_image_document_and_sticker():
    png = FakeMessage(id=1, document=SimpleNamespace(id=7, mime_type="image/png", attributes=[], size=10))
    assert image_info(png)[:2] == ("image_document", ".png")
    sticker_attr = types.DocumentAttributeSticker(alt="🚀", stickerset=types.InputStickerSetEmpty())
    sticker = FakeMessage(id=2, document=SimpleNamespace(id=8, mime_type="image/webp", attributes=[sticker_attr]))
    assert image_info(sticker) is None
    video = FakeMessage(id=3, document=SimpleNamespace(id=9, mime_type="video/mp4", attributes=[]))
    assert image_info(video) is None


async def test_debounce_coalesces_burst_per_sender(tmp_path):
    listener, db, rec = make_listener(tmp_path, debounce_sec=0.1)
    for i in range(3):
        await listener.handle_message(FakeMessage(id=10 + i, message=f"part {i}"))
        await asyncio.sleep(0.03)
    await listener.handle_message(FakeMessage(id=20, message="hi", sender_id=OTHER))
    await settle(0.4)
    assert sorted(rec.ready) == sorted([(CHAT, TRADER), (CHAT, OTHER)])
    assert len(db.messages) == 4


async def test_debounce_waits_for_slow_image_download(tmp_path):
    listener, db, rec = make_listener(tmp_path, debounce_sec=0.05)
    photo = FakeMessage(id=50, photo=SimpleNamespace(id=1), download_delay=0.3)
    text = FakeMessage(id=51, message="eth 1R long")
    listener._spawn(listener.handle_message(photo))
    listener._spawn(listener.handle_message(text))
    await settle(0.6)
    assert rec.ready == [(CHAT, TRADER)]
    assert rec.snapshots == [[50, 51]]          # both stored before the engine was called


async def test_duplicate_delivery_is_ignored(tmp_path):
    listener, db, rec = make_listener(tmp_path)
    msg = FakeMessage(id=5, message="btc short")
    await listener.handle_message(msg)
    await listener.handle_message(msg)           # e.g. live event + catch-up
    await settle()
    assert rec.ready == [(CHAT, TRADER)]


async def test_sender_filter(tmp_path):
    listener, db, rec = make_listener(tmp_path, allowed_sender_ids=[TRADER])
    await listener.handle_message(FakeMessage(id=1, message="eth long", sender_id=OTHER))
    await settle()
    assert db.messages == {} and rec.ready == []
    assert db.state[f"last_msg_id:{CHAT}"] == 1   # still counted as seen
    await listener.handle_message(FakeMessage(id=2, message="eth long", sender_id=TRADER))
    await settle()
    assert list(db.messages) == [(CHAT, 2)] and rec.ready == [(CHAT, TRADER)]


async def test_own_outgoing_messages_and_other_chats_ignored(tmp_path):
    listener, db, rec = make_listener(tmp_path)
    listener._entities = {CHAT: object()}
    await listener.handle_message(FakeMessage(id=1, message="my own text", out=True))
    await listener.handle_message(FakeMessage(id=2, message="other chat", chat_id=-100999))
    await settle()
    assert db.messages == {} and rec.ready == []


async def test_download_error_still_stores_message(tmp_path, caplog):
    listener, db, rec = make_listener(tmp_path)
    msg = FakeMessage(id=3, message="chart", photo=SimpleNamespace(id=1), download_error=OSError("boom"))
    with caplog.at_level(logging.ERROR):
        await listener.handle_message(msg)
    stored = db.messages[(CHAT, 3)]
    assert stored.media_path is None and stored.media_type == "photo" and stored.text == "chart"
    assert "boom" in caplog.text
    await settle()
    assert rec.ready == [(CHAT, TRADER)]


async def test_on_ready_exception_is_logged_not_raised(tmp_path, caplog):
    rec = Recorder(fail=True)
    listener, db, _ = make_listener(tmp_path, rec=rec)
    with caplog.at_level(logging.ERROR):
        await listener.handle_message(FakeMessage(id=1, message="eth long"))
        await settle()
    assert rec.ready == [(CHAT, TRADER)]
    assert "on_ready failed" in caplog.text


async def test_content_less_message_does_not_trigger(tmp_path):
    listener, db, rec = make_listener(tmp_path)
    await listener.handle_message(FakeMessage(id=1, message=""))
    await settle()
    assert rec.ready == [] and (CHAT, 1) in db.messages


async def test_stop_cancels_pending_debounce(tmp_path):
    listener, db, rec = make_listener(tmp_path, debounce_sec=0.2)
    await listener.handle_message(FakeMessage(id=1, message="eth long"))
    await listener.stop()
    await settle(0.3)
    assert rec.ready == []


async def test_edit_updates_and_calls_on_edit(tmp_path):
    listener, db, rec = make_listener(tmp_path)
    await listener.handle_message(FakeMessage(id=7, message="eth long sl 2200"))
    await settle()
    db.messages[(CHAT, 7)] = db.messages[(CHAT, 7)].model_copy(update={"consumed": True})
    await listener.handle_edit(FakeMessage(id=7, message="eth long sl 2205"))
    stored = db.messages[(CHAT, 7)]
    assert stored.text == "eth long sl 2205" and stored.edited and stored.consumed
    assert [m.text for m in rec.edits] == ["eth long sl 2205"]
    # no-op edit (e.g. reactions changed): ignored
    await listener.handle_edit(FakeMessage(id=7, message="eth long sl 2205"))
    assert len(rec.edits) == 1


async def test_edit_redownloads_changed_media_only(tmp_path):
    listener, db, rec = make_listener(tmp_path)
    first = FakeMessage(id=8, message="chart", photo=SimpleNamespace(id=1))
    await listener.handle_message(first)
    same = FakeMessage(id=8, message="chart v2", photo=SimpleNamespace(id=1))
    await listener.handle_edit(same)
    assert same.downloads == 0 and db.messages[(CHAT, 8)].media_path is not None
    changed = FakeMessage(id=8, message="chart v2", photo=SimpleNamespace(id=2))
    await listener.handle_edit(changed)
    assert changed.downloads == 1 and len(rec.edits) == 2


async def test_edit_of_unknown_old_message_is_context_only(tmp_path):
    listener, db, rec = make_listener(tmp_path, catchup_max_age_sec=120)
    old = FakeMessage(id=9, message="old signal edited", date=utcnow() - timedelta(hours=5))
    await listener.handle_edit(old)
    await settle()
    stored = db.messages[(CHAT, 9)]
    assert stored.consumed and stored.edited
    assert rec.edits == [] and rec.ready == []


def test_should_process_catchup_rule():
    now = datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)
    assert should_process_catchup(now - timedelta(seconds=30), now, 120)
    assert should_process_catchup(now - timedelta(seconds=120), now, 120)
    assert not should_process_catchup(now - timedelta(seconds=121), now, 120)
    assert should_process_catchup(now + timedelta(seconds=5), now, 120)       # clock skew
    assert not should_process_catchup(datetime(2026, 11, 7, 11, 0), now, 120)  # naive = UTC


async def test_catch_up_executes_only_recent_messages(tmp_path):
    listener, db, rec = make_listener(tmp_path, catchup_max_age_sec=120)
    db.state[f"last_msg_id:{CHAT}"] = 100
    history = [
        FakeMessage(id=99, message="already processed", date=utcnow() - timedelta(hours=2)),
        FakeMessage(id=101, message="old missed", date=utcnow() - timedelta(minutes=30)),
        FakeMessage(id=102, message="eth 1R long", date=utcnow() - timedelta(seconds=20)),
    ]
    listener._client = ReadOnlyFakeClient(history)
    listener._entities = {CHAT: object()}
    await listener._catch_up()
    await settle()
    assert (CHAT, 99) not in db.messages
    assert db.messages[(CHAT, 101)].consumed is True
    assert db.messages[(CHAT, 102)].consumed is False
    assert rec.ready == [(CHAT, TRADER)]
    assert db.state[f"last_msg_id:{CHAT}"] == 102


async def test_catch_up_without_state_only_remembers_position(tmp_path):
    listener, db, rec = make_listener(tmp_path)
    listener._client = ReadOnlyFakeClient([FakeMessage(id=500, message="eth long")])
    listener._entities = {CHAT: object()}
    await listener._catch_up()
    await settle()
    assert db.messages == {} and rec.ready == []
    assert db.state[f"last_msg_id:{CHAT}"] == 500


async def test_catch_up_only_old_messages_never_trigger(tmp_path):
    listener, db, rec = make_listener(tmp_path, catchup_max_age_sec=120)
    db.state[f"last_msg_id:{CHAT}"] = 1
    listener._client = ReadOnlyFakeClient([FakeMessage(id=2, message="x", date=utcnow() - timedelta(days=1))])
    listener._entities = {CHAT: object()}
    await listener._catch_up()
    await settle()
    assert db.messages[(CHAT, 2)].consumed and rec.ready == []


async def test_resume_unprocessed_recent_messages(tmp_path):
    listener, db, rec = make_listener(tmp_path, catchup_max_age_sec=120)
    listener._entities = {CHAT: object()}
    await db.save_message(StoredMessage(chat_id=CHAT, message_id=1, sender_id=TRADER, date=utcnow(), text="eth long"))
    await db.save_message(StoredMessage(chat_id=CHAT, message_id=2, sender_id=OTHER, date=utcnow(), text="x",
                                        consumed=True))
    await listener._resume_unprocessed()
    await settle()
    assert rec.ready == [(CHAT, TRADER)]


def test_reply_to_forum_topic_is_not_a_reply(tmp_path):
    from copytrader.telegram.listener import _reply_to_id

    topic = SimpleNamespace(reply_to_msg_id=5, reply_to_peer_id=None, forum_topic=True, reply_to_top_id=None)
    assert _reply_to_id(FakeMessage(id=1, reply_to=topic)) is None
    real = SimpleNamespace(reply_to_msg_id=8, reply_to_peer_id=None, forum_topic=True, reply_to_top_id=5)
    assert _reply_to_id(FakeMessage(id=2, reply_to=real)) == 8


@pytest.mark.parametrize(
    ("ref", "expected"),
    [
        (-1001234567890, -1001234567890),
        ("-1001234567890", -1001234567890),
        ("https://t.me/c/1234567890/55", -1001234567890),
        ("t.me/c/1234567890", -1001234567890),
        ("https://t.me/signalgroup/123", "@signalgroup"),
        ("@signalgroup", "@signalgroup"),
        ("https://t.me/+AbCdEf123", "https://t.me/+AbCdEf123"),
        ("https://t.me/joinchat/AbCdEf", "https://t.me/joinchat/AbCdEf"),
    ],
)
def test_normalize_chat_ref(ref, expected):
    assert normalize_chat_ref(ref) == expected


class FlakyClient:
    """run_until_disconnected fails once, then blocks until disconnect()."""

    def __init__(self, authorized: bool = True) -> None:
        self.runs = 0
        self.connects = 0
        self.authorized = authorized
        self._gone = asyncio.Event()

    async def run_until_disconnected(self):
        self.runs += 1
        if self.runs == 1:
            raise ConnectionError("network down")
        await self._gone.wait()

    async def connect(self):
        self.connects += 1

    async def is_user_authorized(self):
        return self.authorized

    async def disconnect(self):
        self._gone.set()

    async def get_messages(self, entity, limit=1):
        return []


async def test_run_until_disconnected_reconnects_and_stops(tmp_path, monkeypatch):
    from copytrader.telegram import listener as listener_mod

    monkeypatch.setattr(listener_mod, "RECONNECT_BASE_DELAY_SEC", 0.01)
    listener, db, rec = make_listener(tmp_path)
    client = FlakyClient()
    listener._client = client
    listener._entities = {CHAT: object()}
    runner = asyncio.ensure_future(listener.run_until_disconnected())
    await settle(0.1)
    assert client.runs == 2 and client.connects == 1 and not runner.done()
    await listener.stop()
    await asyncio.wait_for(runner, 1)


async def test_run_until_disconnected_raises_when_session_revoked(tmp_path, monkeypatch):
    from copytrader.telegram import listener as listener_mod

    monkeypatch.setattr(listener_mod, "RECONNECT_BASE_DELAY_SEC", 0.01)
    listener, db, rec = make_listener(tmp_path)
    listener._client = FlakyClient(authorized=False)
    with pytest.raises(RuntimeError, match="login"):
        await asyncio.wait_for(listener.run_until_disconnected(), 1)


async def test_start_refuses_unauthorized_session(tmp_path):
    listener, db, rec = make_listener(tmp_path)
    listener._client = FlakyClient(authorized=False)
    with pytest.raises(RuntimeError, match="python -m copytrader login"):
        await listener.start()
