"""Forum topics in the Telegram listener ("KLAN KASIM": only the crypto and FX topics are followed)."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

from telethon import events, functions
from telethon.tl import types

from copytrader.config import TopicRoute
from copytrader.models import StoredMessage
from copytrader.telegram import listener as listener_mod
from copytrader.telegram.listener import _reply_to_id, topic_id_of
from test_listener import CHAT, OTHER, TRADER, FakeMessage, make_listener, settle

KADIR, ANIL = TRADER, OTHER
CHANNEL_ID = int(str(CHAT)[4:])            # -1001234567890 -> 1234567890

CRYPTO, SOHBET, FX, ANALIZ, PNL, ACIKLAMA, DUYURU = 3, 4, 5, 6, 8, 9, 10
TITLES = {
    CRYPTO: "💰 Kasım Ayı Kripto İşlemleri",
    SOHBET: "Sohbet",
    FX: "💱 Kasım Ayı Fx İşlemleri",
    ANALIZ: "Analiz",
    PNL: "PnL ve Sertifika",
    ACIKLAMA: "İşlem Açıklamaları",
    DUYURU: "Duyuru",
    1: "General",
}
ROUTES = [
    TopicRoute(name="kripto", title_contains=["Kripto İşlemleri"], brokers=["binance"], kind="crypto"),
    TopicRoute(name="fx", title_contains=["FX İŞLEMLERİ"], brokers=["mt5"], kind="fx"),
]
READ_REQUESTS = (functions.messages.GetForumTopicsByIDRequest, functions.messages.GetForumTopicsRequest)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def forum_topic(topic_id: int, title: str) -> types.ForumTopic:
    return types.ForumTopic(
        id=topic_id, date=utcnow(), peer=types.PeerChannel(CHANNEL_ID), title=title, icon_color=0,
        top_message=topic_id, read_inbox_max_id=0, read_outbox_max_id=0, unread_count=0,
        unread_mentions_count=0, unread_reactions_count=0, unread_poll_votes_count=0,
        from_id=types.PeerUser(KADIR), notify_settings=types.PeerNotifySettings(),
    )


def in_topic(topic: int, reply_to: int | None = None) -> types.MessageReplyHeader:
    """Real layer-229 header: top-level message in a topic, or a reply inside it."""
    if reply_to is None:
        return types.MessageReplyHeader(forum_topic=True, reply_to_msg_id=topic)
    return types.MessageReplyHeader(forum_topic=True, reply_to_msg_id=reply_to, reply_to_top_id=topic)


def service(msg_id: int, action: Any, reply_to: Any = None, date: datetime | None = None) -> types.MessageService:
    return types.MessageService(id=msg_id, peer_id=types.PeerChannel(CHANNEL_ID), date=date or utcnow(),
                                action=action, reply_to=reply_to, from_id=types.PeerUser(KADIR))


class ForumClient:
    """Read-only fake: forum-topic requests and history only; anything else fails the test."""

    def __init__(self, titles: dict[int, str] | None = None, history: list[Any] | None = None) -> None:
        self.titles = dict(TITLES if titles is None else titles)
        self.history = history or []
        self.requests: list[Any] = []
        self.fail = 0                      # the next N requests raise
        self.delay = 0.0

    async def __call__(self, request: Any) -> Any:
        assert isinstance(request, READ_REQUESTS), f"listener must not send {type(request).__name__}"
        self.requests.append(request)
        await asyncio.sleep(self.delay)
        if self.fail:
            self.fail -= 1
            raise ConnectionError("telegram down")
        ids = request.topics if isinstance(request, functions.messages.GetForumTopicsByIDRequest) else list(self.titles)
        topics = [forum_topic(t, self.titles[t]) for t in ids if t in self.titles]
        return types.messages.ForumTopics(count=len(topics), topics=topics, messages=[], chats=[], users=[], pts=1)

    async def get_messages(self, entity, limit=1):
        return sorted(self.history, key=lambda m: m.id, reverse=True)[:limit]

    def iter_messages(self, entity, min_id=0, limit=None):
        async def gen():
            for m in sorted(self.history, key=lambda m: m.id, reverse=True)[:limit]:
                if m.id > min_id:
                    yield m
        return gen()

    async def disconnect(self):
        pass

    def __getattr__(self, name: str):
        raise AssertionError(f"listener must not call client.{name}")


def forum_listener(tmp_path, *, routes=ROUTES, client: ForumClient | None = None, forum: bool = True, **cfg):
    listener, db, rec = make_listener(tmp_path, topics=routes, **cfg)
    listener._client = client or ForumClient()
    listener._entities = {CHAT: SimpleNamespace(forum=forum)}
    listener._forums = {CHAT} if forum else set()
    return listener, db, rec, listener._client


def fast_retries(monkeypatch, delay: float = 0.05) -> None:
    monkeypatch.setattr(listener_mod, "TOPIC_RETRY_DELAYS_SEC", (delay,))


# --------------------------------------------------------------------------- #
# Topic id / reply id from real Telethon TL objects
# --------------------------------------------------------------------------- #
def _msg(reply_to: Any) -> types.Message:
    return types.Message(id=200, peer_id=types.PeerChannel(CHANNEL_ID), date=utcnow(), message="x",
                         from_id=types.PeerUser(KADIR), reply_to=reply_to)


def test_topic_and_reply_ids_follow_telegram_header_rules():
    top_level = _msg(in_topic(CRYPTO))
    assert (topic_id_of(top_level), _reply_to_id(top_level)) == (CRYPTO, None)       # not a real reply
    reply = _msg(in_topic(CRYPTO, reply_to=150))
    assert (topic_id_of(reply), _reply_to_id(reply)) == (CRYPTO, 150)
    general = _msg(None)
    assert (topic_id_of(general), _reply_to_id(general)) == (None, None)
    general_reply = _msg(types.MessageReplyHeader(reply_to_msg_id=50))
    assert (topic_id_of(general_reply), _reply_to_id(general_reply)) == (None, 50)
    other_chat = _msg(types.MessageReplyHeader(forum_topic=True, reply_to_peer_id=types.PeerChannel(42),
                                               reply_to_msg_id=9))
    assert (topic_id_of(other_chat), _reply_to_id(other_chat)) == (None, None)       # 9 is not a topic id
    other_chat_in_topic = _msg(types.MessageReplyHeader(forum_topic=True, reply_to_peer_id=types.PeerChannel(42),
                                                        reply_to_msg_id=9, reply_to_top_id=FX))
    assert topic_id_of(other_chat_in_topic) == FX
    assert topic_id_of(_msg(types.MessageReplyHeader(forum_topic=True, reply_to_msg_id=1))) is None  # General = None
    root = service(77, types.MessageActionTopicCreate(title="Aralık Ayı Kripto İşlemleri", icon_color=0))
    assert topic_id_of(root) == 77
    story = _msg(types.MessageReplyStoryHeader(peer=types.PeerUser(1), story_id=3))
    assert topic_id_of(story) is None


# --------------------------------------------------------------------------- #
# Filtering
# --------------------------------------------------------------------------- #
async def test_only_routed_topics_are_stored_and_passed_on(tmp_path):
    listener, db, rec, client = forum_listener(tmp_path)
    chart = FakeMessage(id=101, photo=SimpleNamespace(id=1), reply_to=in_topic(CRYPTO))
    sizing = FakeMessage(id=102, message="Eth short\n1R risk\n100 risk için 8.7 lot", reply_to=in_topic(CRYPTO))
    fx = FakeMessage(id=103, message="EUR LONG 1R Limit Emir", photo=SimpleNamespace(id=2), reply_to=in_topic(FX))
    chatter_photo = FakeMessage(id=104, message="bakın", photo=SimpleNamespace(id=3), reply_to=in_topic(SOHBET))
    others = [
        FakeMessage(id=105, message="BTC short olabilir", reply_to=in_topic(ANALIZ)),
        FakeMessage(id=106, message="+3R", reply_to=in_topic(PNL)),
        FakeMessage(id=107, message="ETH işlemi açıklaması", reply_to=in_topic(ACIKLAMA)),
        FakeMessage(id=108, message="Yarın canlı yayın", reply_to=in_topic(DUYURU)),
        FakeMessage(id=109, message="selam", sender_id=ANIL),                   # General topic
    ]
    for m in [chart, sizing, fx, chatter_photo, *others]:
        await listener.handle_message(m)
    await settle()

    assert sorted(db.messages) == [(CHAT, 101), (CHAT, 102), (CHAT, 103)]
    s = db.messages[(CHAT, 102)]
    assert (s.topic_id, s.topic_title, s.reply_to_message_id) == (CRYPTO, TITLES[CRYPTO], None)
    assert db.messages[(CHAT, 103)].topic_id == FX and db.messages[(CHAT, 103)].topic_title == TITLES[FX]
    assert chart.downloads == 1 and fx.downloads == 1
    assert chatter_photo.downloads == 0                                  # ignored topics are never downloaded
    assert sorted(rec.calls) == [(CHAT, KADIR, CRYPTO), (CHAT, KADIR, FX)]
    assert db.state[f"last_msg_id:{CHAT}"] == 109                        # ignored messages still count as seen
    assert all(isinstance(r, READ_REQUESTS) for r in client.requests)


async def test_reply_inside_topic_keeps_reply_and_topic(tmp_path):
    listener, db, rec, client = forum_listener(tmp_path)
    await listener.handle_message(FakeMessage(id=120, message="EUR LONG 1R Limit Emir", reply_to=in_topic(FX)))
    await listener.handle_message(FakeMessage(id=121, message="Aktif", sender_id=ANIL, reply_to=in_topic(FX, 120)))
    await listener.handle_message(FakeMessage(id=122, message="2.8 r", reply_to=in_topic(FX)))
    await settle()
    assert db.messages[(CHAT, 121)].reply_to_message_id == 120 and db.messages[(CHAT, 121)].topic_id == FX
    assert db.messages[(CHAT, 122)].reply_to_message_id is None and db.messages[(CHAT, 122)].topic_id == FX
    assert sorted(rec.calls) == [(CHAT, KADIR, FX), (CHAT, ANIL, FX)]


async def test_debounce_is_per_topic(tmp_path):
    listener, db, rec, client = forum_listener(tmp_path, debounce_sec=0.1)
    await listener._load_topics()
    for i, topic in enumerate([CRYPTO, CRYPTO, FX, CRYPTO]):
        await listener.handle_message(FakeMessage(id=130 + i, message=f"part {i}", reply_to=in_topic(topic)))
        await asyncio.sleep(0.02)
    await settle(0.4)
    assert sorted(rec.calls) == [(CHAT, KADIR, CRYPTO), (CHAT, KADIR, FX)]   # one batch per (topic, sender)


async def test_route_text_is_turkish_case_insensitive_and_partial(tmp_path):
    titles = {**TITLES, 40: "ARALIK AYI KRİPTO İŞLEMLERİ"}
    listener, db, rec, client = forum_listener(tmp_path, client=ForumClient(titles))
    await listener.handle_message(FakeMessage(id=140, message="sol long", reply_to=in_topic(40)))
    assert db.messages[(CHAT, 140)].topic_title == "ARALIK AYI KRİPTO İŞLEMLERİ"


# --------------------------------------------------------------------------- #
# Title cache: start-up list, cache miss, service messages
# --------------------------------------------------------------------------- #
async def test_new_monthly_topic_is_fetched_on_cache_miss_then_cached(tmp_path):
    listener, db, rec, client = forum_listener(tmp_path)
    await listener._load_topics()
    assert [type(r) for r in client.requests] == [functions.messages.GetForumTopicsRequest]
    client.titles[200] = "💰 Aralık Ayı Kripto İşlemleri"                  # created while we were not looking
    await listener.handle_message(FakeMessage(id=201, message="Btc long", reply_to=in_topic(200)))
    await listener.handle_message(FakeMessage(id=202, message="1R risk", reply_to=in_topic(200)))
    await listener.handle_message(FakeMessage(id=203, message="sohbet", reply_to=in_topic(SOHBET)))
    by_id = [r for r in client.requests if isinstance(r, functions.messages.GetForumTopicsByIDRequest)]
    assert [r.topics for r in by_id] == [[200]]                           # once; Sohbet was listed at start
    assert db.messages[(CHAT, 201)].topic_title == "💰 Aralık Ayı Kripto İşlemleri"
    assert (CHAT, 202) in db.messages and (CHAT, 203) not in db.messages


async def test_topic_create_and_edit_service_messages_update_cache_without_api(tmp_path):
    listener, db, rec, client = forum_listener(tmp_path)
    await listener._load_topics()
    client.requests.clear()
    created = service(300, types.MessageActionTopicCreate(title="💱 Aralık Ayı Fx İşlemleri", icon_color=0))
    await listener._on_raw_update(types.UpdateNewChannelMessage(message=created, pts=1, pts_count=1))
    await settle(0.05)
    await listener.handle_message(FakeMessage(id=301, message="GBP SHORT 1R", reply_to=in_topic(300)))
    assert db.messages[(CHAT, 301)].topic_title == "💱 Aralık Ayı Fx İşlemleri"

    renamed = service(302, types.MessageActionTopicEdit(title="Ocak Ayı Kripto İşlemleri"), reply_to=in_topic(SOHBET))
    await listener.handle_message(renamed)
    archived = service(303, types.MessageActionTopicEdit(title="Arşiv"), reply_to=in_topic(FX))
    await listener.handle_message(archived)
    closed = service(304, types.MessageActionTopicEdit(closed=True), reply_to=in_topic(CRYPTO))
    await listener.handle_message(closed)
    await listener.handle_message(FakeMessage(id=305, message="Eth long", reply_to=in_topic(SOHBET)))
    await listener.handle_message(FakeMessage(id=306, message="EUR long", reply_to=in_topic(FX)))
    await listener.handle_message(FakeMessage(id=307, message="Sol long", reply_to=in_topic(CRYPTO)))
    assert (CHAT, 305) in db.messages and (CHAT, 306) not in db.messages and (CHAT, 307) in db.messages
    assert client.requests == []
    assert not any(k[1] in (300, 302, 303, 304) for k in db.messages)    # service messages are never stored

    # A rename without a topic header cannot be attributed: titles are fetched again when needed.
    await listener.handle_message(service(308, types.MessageActionTopicEdit(title="Genel Sohbet")))
    await listener.handle_message(FakeMessage(id=309, message="Btc short", reply_to=in_topic(CRYPTO)))
    assert [r.topics for r in client.requests] == [[CRYPTO]] and (CHAT, 309) in db.messages


async def test_raw_handler_ignores_other_updates_and_chats(tmp_path):
    listener, db, rec, client = forum_listener(tmp_path)
    other_chat = types.MessageService(id=5, peer_id=types.PeerChannel(999), date=utcnow(),
                                      action=types.MessageActionTopicCreate(title="Kripto İşlemleri", icon_color=0))
    await listener._on_raw_update(types.UpdateNewChannelMessage(message=other_chat, pts=1, pts_count=1))
    pin = service(6, types.MessageActionPinMessage())
    await listener._on_raw_update(types.UpdateNewChannelMessage(message=pin, pts=1, pts_count=1))
    await listener._on_raw_update(types.UpdateChannelTooLong(channel_id=CHANNEL_ID))
    await settle(0.05)
    assert listener._topic_titles == {} and listener._tasks == set()


# --------------------------------------------------------------------------- #
# Telegram errors: retry, give up, late messages, shutdown
# --------------------------------------------------------------------------- #
async def test_api_errors_are_retried_and_concurrent_messages_share_the_lookup(tmp_path, monkeypatch, caplog):
    fast_retries(monkeypatch)
    listener, db, rec, client = forum_listener(tmp_path)
    client.fail = 2
    with caplog.at_level(logging.WARNING):
        listener._spawn(listener.handle_message(FakeMessage(id=401, photo=SimpleNamespace(id=1), reply_to=in_topic(CRYPTO))))
        listener._spawn(listener.handle_message(FakeMessage(id=402, message="Eth short", reply_to=in_topic(CRYPTO))))
        await settle(0.6)
    assert sorted(db.messages) == [(CHAT, 401), (CHAT, 402)]
    assert rec.calls == [(CHAT, KADIR, CRYPTO)]
    assert len(client.requests) == 3                                     # 2 failures + 1 success, not per message
    assert "Could not fetch forum topic" in caplog.text


async def test_gives_up_after_deadline_but_a_later_delivery_succeeds(tmp_path, monkeypatch, caplog):
    fast_retries(monkeypatch)
    monkeypatch.setattr(listener_mod, "TOPIC_GIVE_UP_SEC", 0.2)
    listener, db, rec, client = forum_listener(tmp_path)
    client.fail = 10_000
    msg = FakeMessage(id=410, message="Eth short", reply_to=in_topic(CRYPTO))
    with caplog.at_level(logging.ERROR):
        await listener.handle_message(msg)
    assert db.messages == {} and "NOT stored" in caplog.text
    assert db.state[f"last_msg_id:{CHAT}"] == 410
    client.fail = 0
    await asyncio.sleep(0.06)                                            # backoff of the last failure
    await listener.handle_message(msg)                                   # e.g. catch-up after a reconnect
    await settle()
    assert (CHAT, 410) in db.messages and rec.calls == [(CHAT, KADIR, CRYPTO)]


async def test_title_found_late_for_an_old_message_is_context_only(tmp_path, monkeypatch):
    fast_retries(monkeypatch, 0.1)
    monkeypatch.setattr(listener_mod, "TOPIC_LATE_SEC", 0.05)
    listener, db, rec, client = forum_listener(tmp_path, catchup_max_age_sec=120)
    client.fail = 1
    old = FakeMessage(id=420, message="Eth short", date=utcnow() - timedelta(minutes=5), reply_to=in_topic(CRYPTO))
    young = FakeMessage(id=421, message="EUR LONG", reply_to=in_topic(FX))
    listener._spawn(listener.handle_message(old))
    listener._spawn(listener.handle_message(young))
    await settle(0.5)
    assert db.messages[(CHAT, 420)].consumed is True                     # waited and older than 120 s
    assert db.messages[(CHAT, 421)].consumed is False
    assert rec.calls == [(CHAT, KADIR, FX)]


async def test_stop_while_waiting_for_a_title_keeps_last_msg_id_below_it(tmp_path, monkeypatch):
    fast_retries(monkeypatch, 30.0)
    listener, db, rec, client = forum_listener(tmp_path)
    listener._set_title(CHAT, SOHBET, "Sohbet")
    client.fail = 10_000
    listener._spawn(listener.handle_message(FakeMessage(id=500, message="Eth short", reply_to=in_topic(CRYPTO))))
    await settle(0.05)
    await listener.handle_message(FakeMessage(id=501, message="selam", reply_to=in_topic(SOHBET)))
    assert db.state[f"last_msg_id:{CHAT}"] == 499                        # 500 is still waiting
    started = time.monotonic()
    await listener.stop()
    assert time.monotonic() - started < 2                                # the 30 s backoff did not delay stop
    assert db.messages == {} and (CHAT, 500) in listener._held_back and (CHAT, 500) not in listener._seen
    await listener._persist_last_id(CHAT)
    assert db.state[f"last_msg_id:{CHAT}"] == 499                        # catch-up after restart refetches 500


# --------------------------------------------------------------------------- #
# Non-forum chats / no routes
# --------------------------------------------------------------------------- #
async def test_topics_configured_but_chat_is_not_a_forum(tmp_path, caplog):
    listener, db, rec, client = forum_listener(tmp_path, forum=False)
    with caplog.at_level(logging.WARNING):
        await listener._load_topics()
        await listener.handle_message(FakeMessage(id=600, message="eth long"))
        await listener.handle_message(FakeMessage(id=601, message="btc long", sender_id=ANIL))
        await settle()
    assert caplog.text.count("is not a forum") == 1
    assert db.messages[(CHAT, 600)].topic_id is None and db.messages[(CHAT, 600)].topic_title is None
    assert sorted(rec.calls) == [(CHAT, KADIR, None), (CHAT, ANIL, None)]
    assert client.requests == []


async def test_forum_detected_from_message_headers(tmp_path, caplog):
    listener, db, rec, client = forum_listener(tmp_path, forum=False)
    with caplog.at_level(logging.WARNING):
        await listener.handle_message(FakeMessage(id=610, message="selam", reply_to=in_topic(SOHBET)))
        await listener.handle_message(FakeMessage(id=611, message="Eth short", reply_to=in_topic(CRYPTO)))
    assert list(db.messages) == [(CHAT, 611)]
    assert "is a forum" in caplog.text


async def test_forum_without_routes_follows_every_topic_with_topic_info(tmp_path, monkeypatch):
    fast_retries(monkeypatch)
    listener, db, rec, client = forum_listener(tmp_path, routes=[])
    await listener.handle_message(FakeMessage(id=620, message="selam", reply_to=in_topic(SOHBET)))
    await listener.handle_message(FakeMessage(id=621, message="genel", sender_id=ANIL))
    client.fail = 1
    await listener.handle_message(FakeMessage(id=622, message="analiz", reply_to=in_topic(ANALIZ)))   # API error
    await settle()
    assert (db.messages[(CHAT, 620)].topic_id, db.messages[(CHAT, 620)].topic_title) == (SOHBET, "Sohbet")
    assert (db.messages[(CHAT, 621)].topic_id, db.messages[(CHAT, 621)].topic_title) == (None, "General")
    assert (db.messages[(CHAT, 622)].topic_id, db.messages[(CHAT, 622)].topic_title) == (ANALIZ, None)  # no waiting
    assert sorted(rec.calls, key=str) == sorted([(CHAT, KADIR, SOHBET), (CHAT, ANIL, None), (CHAT, KADIR, ANALIZ)],
                                                key=str)


# --------------------------------------------------------------------------- #
# Edits
# --------------------------------------------------------------------------- #
async def test_edits_follow_the_topic_rules(tmp_path):
    listener, db, rec, client = forum_listener(tmp_path, catchup_max_age_sec=120)
    await listener._load_topics()
    await listener.handle_message(FakeMessage(id=700, message="Eth short", reply_to=in_topic(CRYPTO)))
    await listener.handle_message(FakeMessage(id=701, message="sohbet", reply_to=in_topic(SOHBET)))
    await settle()
    await listener.handle_edit(FakeMessage(id=700, message="Eth short\n1R risk", reply_to=in_topic(CRYPTO)))
    await listener.handle_edit(FakeMessage(id=701, message="sohbet (düzenlendi)", reply_to=in_topic(SOHBET)))
    assert [(m.message_id, m.topic_id, m.topic_title) for m in rec.edits] == [(700, CRYPTO, TITLES[CRYPTO])]
    assert (CHAT, 701) not in db.messages

    # Row stored before topic support (no title): checked like a new message.
    await db.save_message(StoredMessage(chat_id=CHAT, message_id=650, sender_id=KADIR, date=utcnow(), text="x",
                                        consumed=True))
    await listener.handle_edit(FakeMessage(id=650, message="y", reply_to=in_topic(ANALIZ)))
    assert db.messages[(CHAT, 650)].text == "x" and len(rec.edits) == 1

    # Edits of old messages we never saw: context only, and only for followed topics.
    old = utcnow() - timedelta(hours=5)
    await listener.handle_edit(FakeMessage(id=640, message="EUR long", date=old, reply_to=in_topic(FX)))
    await listener.handle_edit(FakeMessage(id=641, message="analiz", date=old, reply_to=in_topic(ANALIZ)))
    assert db.messages[(CHAT, 640)].consumed and db.messages[(CHAT, 640)].topic_id == FX
    assert (CHAT, 641) not in db.messages


async def test_edit_during_topic_lookup_is_stored_instead(tmp_path):
    listener, db, rec, client = forum_listener(tmp_path)
    client.delay = 0.2
    listener._spawn(listener.handle_message(FakeMessage(id=710, message="Eth short sl 2250", reply_to=in_topic(CRYPTO))))
    await asyncio.sleep(0.05)
    await listener.handle_edit(FakeMessage(id=710, message="Eth short sl 2250.60", reply_to=in_topic(CRYPTO)))
    await settle(0.5)
    stored = db.messages[(CHAT, 710)]
    assert stored.text == "Eth short sl 2250.60" and stored.edited and stored.topic_id == CRYPTO
    assert rec.calls == [(CHAT, KADIR, CRYPTO)] and rec.edits == []


# --------------------------------------------------------------------------- #
# Catch-up / resume
# --------------------------------------------------------------------------- #
async def test_catch_up_filters_topics_with_a_single_title_request(tmp_path):
    history = [
        service(101, types.MessageActionTopicCreate(title="💰 Aralık Ayı Kripto İşlemleri", icon_color=0)),
        FakeMessage(id=102, message="Btc long 1R", reply_to=in_topic(101)),
        FakeMessage(id=103, message="selam", reply_to=in_topic(SOHBET)),
        FakeMessage(id=104, message="EUR LONG 1R Limit Emir", reply_to=in_topic(FX)),
        FakeMessage(id=105, message="genel", sender_id=ANIL),
        FakeMessage(id=106, message="kapattık", reply_to=in_topic(FX), date=utcnow() - timedelta(hours=1)),
    ]
    listener, db, rec, client = forum_listener(tmp_path, client=ForumClient(history=history), catchup_max_age_sec=120)
    db.state[f"last_msg_id:{CHAT}"] = 100
    await listener._catch_up()
    await settle()
    assert sorted(db.messages) == [(CHAT, 102), (CHAT, 104), (CHAT, 106)]
    assert db.messages[(CHAT, 102)].topic_title == "💰 Aralık Ayı Kripto İşlemleri"
    assert db.messages[(CHAT, 106)].consumed is True
    assert sorted(rec.calls) == [(CHAT, KADIR, FX), (CHAT, KADIR, 101)]
    assert [sorted(r.topics) for r in client.requests] == [[1, SOHBET, FX]]
    assert db.state[f"last_msg_id:{CHAT}"] == 106


async def test_catch_up_does_not_wait_for_unknown_titles(tmp_path, monkeypatch):
    fast_retries(monkeypatch, 0.2)
    history = [FakeMessage(id=110, message="Eth short", reply_to=in_topic(CRYPTO)),
               FakeMessage(id=111, message="1R risk", reply_to=in_topic(CRYPTO))]
    listener, db, rec, client = forum_listener(tmp_path, client=ForumClient(history=history))
    client.fail = 2                                                      # prefetch + first lookup fail
    db.state[f"last_msg_id:{CHAT}"] = 100
    started = time.monotonic()
    await listener._catch_up()
    assert time.monotonic() - started < 0.15 and db.messages == {}      # continued in the background
    await settle(0.8)
    assert sorted(db.messages) == [(CHAT, 110), (CHAT, 111)] and rec.calls == [(CHAT, KADIR, CRYPTO)]


async def test_resume_unprocessed_per_topic_skips_unrouted_rows(tmp_path):
    listener, db, rec, client = forum_listener(tmp_path, catchup_max_age_sec=120)
    rows = [
        StoredMessage(chat_id=CHAT, message_id=1, sender_id=KADIR, date=utcnow(), text="Eth short",
                      topic_id=CRYPTO, topic_title=TITLES[CRYPTO]),
        StoredMessage(chat_id=CHAT, message_id=2, sender_id=KADIR, date=utcnow(), text="EUR LONG",
                      topic_id=FX, topic_title=TITLES[FX]),
        StoredMessage(chat_id=CHAT, message_id=3, sender_id=ANIL, date=utcnow(), text="selam"),  # pre-topic row
    ]
    for row in rows:
        await db.save_message(row)
    await listener._resume_unprocessed()
    await settle()
    assert sorted(rec.calls) == [(CHAT, KADIR, CRYPTO), (CHAT, KADIR, FX)]


# --------------------------------------------------------------------------- #
# start(): real Channel entity, handlers, one topic listing
# --------------------------------------------------------------------------- #
class StartClient(ForumClient):
    def __init__(self) -> None:
        super().__init__()
        self.handlers: list[tuple[Any, Any]] = []
        self.channel = types.Channel(id=CHANNEL_ID, title="KLAN KASIM", photo=types.ChatPhotoEmpty(), date=None,
                                     megagroup=True, forum=True, access_hash=1)

    async def connect(self):
        pass

    async def is_user_authorized(self):
        return True

    async def get_me(self):
        return SimpleNamespace(id=999, bot=False)

    async def get_entity(self, target):
        assert target == CHAT
        return self.channel

    def add_event_handler(self, callback, builder):
        self.handlers.append((callback, builder))

    def remove_event_handler(self, callback):
        pass


async def test_start_lists_topics_once_and_registers_service_handler(tmp_path, caplog):
    routes = [*ROUTES, TopicRoute(name="hisse", title_contains=["Hisse İşlemleri"])]
    listener, db, rec = make_listener(tmp_path, topics=routes)
    client = StartClient()
    listener._client = client
    with caplog.at_level(logging.INFO):
        await listener.start()
    assert listener._forums == {CHAT}
    assert [type(r) for r in client.requests] == [functions.messages.GetForumTopicsRequest]
    assert client.requests[0].limit == listener_mod.TOPIC_LIST_LIMIT
    raw = [b for _, b in client.handlers if isinstance(b, events.Raw)]
    assert len(raw) == 1 and raw[0].types is types.UpdateNewChannelMessage
    assert "followed (route 'kripto')" in caplog.text and "followed (route 'fx')" in caplog.text
    assert "'Sohbet': ignored" in caplog.text
    assert "matches telegram.topics 'hisse'" in caplog.text
    await listener.handle_message(FakeMessage(id=800, message="Eth short", reply_to=in_topic(CRYPTO)))
    await listener.handle_message(FakeMessage(id=801, message="selam", reply_to=in_topic(SOHBET)))
    assert list(db.messages) == [(CHAT, 800)] and len(client.requests) == 1
    await listener.stop()


async def test_real_telethon_message_in_fx_topic(tmp_path):
    listener, db, rec, client = forum_listener(tmp_path)
    msg = types.Message(id=900, peer_id=types.PeerChannel(CHANNEL_ID), date=utcnow(), message="EUR LONG 1R Limit Emir",
                        from_id=types.PeerUser(KADIR), reply_to=in_topic(FX))
    await listener.handle_message(msg)
    await settle()
    stored = db.messages[(CHAT, 900)]
    assert (stored.topic_id, stored.topic_title, stored.reply_to_message_id) == (FX, TITLES[FX], None)
    assert rec.calls == [(CHAT, KADIR, FX)]

