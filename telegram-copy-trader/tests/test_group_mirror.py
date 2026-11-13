"""The whole source group, copied to the owner's friend's two groups (``/grup``, ``/bagla``).

  /grup ac · /grup kapat · /grup   the owner (only he) switches the copy and sees its state; stored in the database;
                                   OFF by default. /grup konular <ad, ad> limits it to some topics (hepsi: all).
  /bagla@<bot> ana|sohbet          written IN one of the friend's groups by the friend or the owner: that group gets the
                                   copies of that role. "sohbet" topics -> the sohbet group, every other topic -> the
                                   ana group; an unbound role is skipped and counted, never sent elsewhere. Bindings
                                   belong to the friend (/arkadas sil or a new friend drops them).
  the copy                         a header line (topic, author), the text, and its photo / file (downloaded by the
                                   userbot, uploaded by the bot; at most 5 MB; only with a native AES). A medium that
                                   cannot be copied: the text and one generic line. Edits: a "düzenledi" reply.
  never in the way of trading      the listener's handling is the same with the copy on, off or absent; copies wait
                                   in a bounded queue of their own; media moves only while the trading path is idle
                                   (each pause bounded, then the transfer is given up; shutdown aborts it); right before
                                   every request: still on, still the bound group; a failure is told to the owner at
                                   most once an hour (a throttle of its own).

REAL TelegramListener (the read-only forum fakes of tests/test_topics_listener.py) and REAL BotNotifier (the per-chat
fake bot of tests/test_friend_mirror.py, plus send_file); only Telegram is fake. The tests named r*/i* are the
reproductions of the two reviews (correctness: grprev/correctness, isolation: grprev/isolation), now regression tests.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from telethon import errors, functions
from telethon.tl import types
from test_friend_mirror import Client as FriendClient
from test_listener import FakeDB, FakeMessage, Recorder
from test_notifier import FakeBotClient, FakeEngine, MsgEvent
from test_topics_listener import (
    ACIKLAMA,
    CRYPTO,
    DUYURU,
    FX,
    PNL,
    SOHBET,
    TITLES,
    ForumClient,
    forum_listener,
    in_topic,
)

from copytrader.config import NotifierConfig, TelegramConfig
from copytrader.telegram import group_mirror as gm
from copytrader.telegram import listener as listener_mod
from copytrader.telegram import notifier as notifier_mod
from copytrader.telegram.group_mirror import GroupMirror
from copytrader.telegram.notifier import BotNotifier

from test_listener import CHAT, OTHER, TRADER

OWNER, FRIEND, STRANGER, NEW_FRIEND = 777, 555, 666, 4444
ANA_G, SOHBET_G = -1009000000001, -1009000000002       # the friend's two groups
BOT_NAME = "kopya_bot"
BOT_ID = 4242424
KADIR = SimpleNamespace(first_name="Kadir", last_name=None, username="kadir")
ANIL = SimpleNamespace(first_name="Anıl", last_name="K.", username="anil")
HINT = ("Yalnızca bazı konular için: <code>/grup konular &lt;ad, ad&gt;</code> (hepsi için: "
        "<code>/grup konular hepsi</code>)")
BIND_HELP_BOTH = (
    "🔗 Grup kopyası açık. Kopyaların gideceği gruplarınızı bağlamak için botu gruba ekleyin ve o grupta aşağıdaki "
    "satırı gönderin (kopyalayıp yapıştırabilirsiniz):\n"
    "• Ana grup (sohbet dışındaki tüm konular: sinyaller, PnL, General...): <code>/bagla@kopya_bot ana</code>\n"
    "• Sohbet grubu (adında “sohbet” geçen konu): <code>/bagla@kopya_bot sohbet</code>\n"
    "Bir grup bağlanana kadar o konuların mesajları kopyalanmaz.")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Msg(FakeMessage):
    """A source-group message whose download reports progress (as Telethon's does) and can be watched."""

    payload: bytes = b"img"
    media: Any = None
    started_at: float | None = None          # monotonic: a download of it began
    finished_at: float | None = None
    between_chunks: Any = None               # coroutine function run after the first of two chunks
    sender: Any = field(default_factory=lambda: KADIR)
    files: list[str] = field(default_factory=list)

    async def download_media(self, file: str, progress_callback: Any = None) -> str:
        self.started_at = time.monotonic()
        self.files.append(file)
        await asyncio.sleep(self.download_delay)
        self.downloads += 1
        if self.download_error:
            raise self.download_error
        half = len(self.payload) // 2
        if self.between_chunks is not None:
            Path(file).write_bytes(self.payload[:half])
            await self.between_chunks()
            if progress_callback is not None:
                await progress_callback(half, len(self.payload))
        Path(file).write_bytes(self.payload)
        if progress_callback is not None:
            await progress_callback(len(self.payload), len(self.payload))
        self.finished_at = time.monotonic()
        return file


@dataclass
class GroupEvent:
    """A message the bot gets in a group (Telethon NewMessage event)."""

    raw_text: str
    sender_id: int | None = FRIEND
    chat_id: int = ANA_G
    title: str = "Ana Grup"
    is_private: bool = False
    is_group: bool = True
    out: bool = False
    id: int = 7
    reply_to_msg_id: int | None = None

    @property
    def chat(self) -> Any:
        return SimpleNamespace(title=self.title)


class Bot(FriendClient):
    """The notifier bot's Telegram, per chat, with uploads (``send_file``, ``parts`` progress steps each)."""

    def __init__(self) -> None:
        super().__init__()
        self.files: list[Any] = []
        self.gate: asyncio.Event | None = None       # set: the sends to the groups wait for it
        self.send_delay = 0.0
        self.parts = 1
        self.part_delay = 0.0
        self.part_times: list[float] = []
        self.chats_reply: Any = None                 # answer of GetChatsRequest (a migrated group)
        self.requests: list[Any] = []

    async def __call__(self, request: Any) -> Any:
        self.requests.append(request)
        assert isinstance(request, functions.messages.GetChatsRequest), request
        return self.chats_reply

    async def _wait(self, entity: Any) -> None:
        if self.gate is not None and entity in (ANA_G, SOHBET_G):
            await self.gate.wait()
        if self.send_delay:
            await asyncio.sleep(self.send_delay)

    async def send_message(self, entity: Any, message: Any, **kw: Any) -> Any:
        await self._wait(entity)
        if not isinstance(entity, int):                          # an input peer (a migrated group)
            return await FakeBotClient.send_message(self, entity, message, **kw)
        return await super().send_message(entity, message, **kw)

    async def send_file(self, entity: Any, file: Any, **kw: Any) -> Any:
        await self._wait(entity)
        if entity in self.hang_for:
            await asyncio.sleep(3600)
        if entity in self.fail_for:
            raise self.fail_for[entity]
        if self.fail_once_for.get(entity):
            raise self.fail_once_for[entity].pop(0)
        data = Path(file).read_bytes()
        progress = kw.get("progress_callback")
        for i in range(1, self.parts + 1):
            await asyncio.sleep(self.part_delay)
            self.part_times.append(time.monotonic())
            if progress is not None:
                await progress(len(data) * i // self.parts, len(data))
        self.next_id += 1
        record = SimpleNamespace(entity=entity, text=kw.get("caption"), file=str(file), data=data, id=self.next_id,
                                 at=time.monotonic(), **kw)
        self.sent.append(record)
        self.files.append(record)
        return SimpleNamespace(id=self.next_id)


class TimedRecorder(Recorder):
    def __init__(self, db: Any = None, work: float = 0.0) -> None:
        super().__init__(db)
        self.work = work
        self.times: list[tuple[float, float]] = []      # (start, end) of each on_ready, monotonic
        self.edit_times: list[tuple[float, float]] = []

    async def on_ready(self, chat_id: int, sender_id: int | None, topic_id: int | None) -> None:
        start = time.monotonic()
        await super().on_ready(chat_id, sender_id, topic_id)
        await asyncio.sleep(self.work)                  # the engine reads the signal, places orders...
        self.times.append((start, time.monotonic()))

    async def on_edit(self, msg: Any) -> None:
        start = time.monotonic()
        await super().on_edit(msg)
        await asyncio.sleep(self.work)                  # the engine moves a stop...
        self.edit_times.append((start, time.monotonic()))


@pytest.fixture(autouse=True)
def fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(notifier_mod, "GROUP_GAP_SEC", 0.0)
    monkeypatch.setattr(notifier_mod, "GROUP_POLL_SEC", 0.01)
    monkeypatch.setattr(gm, "IDLE_POLL_SEC", 0.01)


class World:
    """Listener + notifier + mirror as the app wires them (``mirror``: False = no mirror at all, as in 8eef65b); both
    groups bound by the friend unless ``bind`` is False."""

    def __init__(self, tmp_path: Path, *, on: bool = True, friend: bool = True, mirror: bool = True,
                 bind: bool = True, work: float = 0.0, db: FakeDB | None = None, engine: Any = None,
                 **cfg: Any) -> None:
        self.tmp_path, self.on, self.with_friend, self.with_mirror = tmp_path, on, friend, mirror
        self.bind, self.work, self.given_db, self.engine, self.cfg = bind, work, db, engine, cfg

    async def up(self) -> "World":
        db = self.given_db or FakeDB()
        rec = TimedRecorder(db, self.work)
        self.listener, self.db, self.rec, self.tg = forum_listener(self.tmp_path, db=db, rec=rec, **self.cfg)
        self.bot = Bot()
        self.n = BotNotifier(NotifierConfig(bot_token="123:abc", owner_user_id=OWNER),
                             TelegramConfig(api_id=1, api_hash="x", source_chats=[CHAT]), str(self.tmp_path))
        self.n._client = self.bot
        self.n._engine = FakeEngine()
        self.n._username = BOT_NAME
        self.n._me_id = BOT_ID

        async def set_friend_chat(chat: int | None) -> None:     # the engine's /arkadas storage
            self.n.set_friend(chat)

        self.n._engine.set_friend_chat = set_friend_chat
        self.listener.set_notifier(self.n)
        if self.with_friend:
            self.n.set_friend(FRIEND)
        self.mirror: GroupMirror | None = None
        if self.with_mirror:
            self.mirror = GroupMirror(self.db, self.n, self.listener, str(self.tmp_path), engine=self.engine)
            self.listener.set_group_mirror(self.mirror)
            self.n.set_group_mirror(self.mirror)
            await self.mirror.load()
            if self.on:
                await self.mirror.set_enabled(True)
            if self.bind:
                await self.mirror.bind("ana", ANA_G, "Ana Grup", FRIEND)
                await self.mirror.bind("sohbet", SOHBET_G, "Sohbet Grubu", FRIEND)
        await self.listener._load_topics()            # (as start() does: the titles are known)
        await self.settle()
        self.bot.sent.clear()
        self.bot.files.clear()
        return self

    async def settle(self, timeout: float = 5.0) -> None:
        loop = asyncio.get_running_loop()
        end = loop.time() + timeout
        while True:
            busy = [t for t in (*self.listener._tasks, *self.listener._timers.values(), *self.n._tasks)
                    if not t.done()]
            for task in (getattr(self.mirror, "_worker", None), self.n._friend_pump):
                if task is not None and not task.done():
                    busy.append(task)
            if not busy:
                return
            assert loop.time() < end, busy
            await asyncio.sleep(0.01)

    async def deliver(self, *messages: Any) -> None:
        for m in messages:
            await self.listener._on_new_message_event(SimpleNamespace(message=m))

    async def edit(self, message: Any) -> None:
        await self.listener._on_edited_event(SimpleNamespace(message=message))

    async def owner_says(self, text: str, **kw: Any) -> None:
        await self.n.handle_message(MsgEvent(text, **{"sender_id": OWNER, **kw}))
        await self.settle()

    async def in_group(self, text: str, **kw: Any) -> None:
        await self.n.handle_message(GroupEvent(text, **kw))
        await self.settle()

    def texts(self, chat: int) -> list[str]:
        return [m.text for m in self.bot.to(chat)]

    def ana(self) -> list[str]:
        return self.texts(ANA_G)

    def sohbet(self) -> list[str]:
        return self.texts(SOHBET_G)

    def friend_texts(self) -> list[str]:
        return self.texts(FRIEND)

    def owner_texts(self) -> list[str]:
        return self.texts(OWNER)


async def world(tmp_path: Path, **kw: Any) -> World:
    return await World(tmp_path, **kw).up()


def body(text: str) -> str:
    return text.split("\n", 1)[1] if "\n" in text else ""


# =========================================================================== #
# Off by default, owner control
# =========================================================================== #
async def test_g1_off_by_default_and_nothing_without_a_friend(tmp_path: Path) -> None:
    w = await world(tmp_path, on=False, bind=False)
    assert w.mirror is not None and not w.mirror.enabled
    assert "group_mirror" not in w.db.state                               # nothing stored until the owner says
    await w.deliver(Msg(id=10, message="selam", reply_to=in_topic(SOHBET), sender=ANIL, sender_id=OTHER),
                    Msg(id=11, photo=SimpleNamespace(id=1), reply_to=in_topic(PNL)),
                    Msg(id=12, message="Eth short\n1R risk", reply_to=in_topic(CRYPTO)))
    await w.settle()
    assert w.bot.sent == [] and w.mirror.waiting == 0 and w.mirror._worker is None
    assert w.rec.calls == [(CHAT, TRADER, CRYPTO)]

    w2 = await world(tmp_path / "b", friend=False)                        # switched on, bound, but no friend set
    await w2.deliver(Msg(id=10, message="selam", reply_to=in_topic(SOHBET)))
    await w2.settle()
    assert w2.bot.sent == [] and w2.mirror._worker is None


async def test_g2_grup_commands_are_the_owners_stored_and_survive_a_restart(tmp_path: Path) -> None:
    w = await world(tmp_path, on=False, bind=False)
    await w.owner_says("/grup")
    state = w.owner_texts()[-1].split("\n")
    assert state[0] == "👥 Grup kopyası kapalı."
    assert state[1] == "Ana grup (sohbet dışındaki tüm konular: sinyaller, PnL, General...): bağlı değil"
    assert state[2] == "Sohbet grubu (adında “sohbet” geçen konu): bağlı değil"
    assert state[3].startswith("Bilinen konular: 'General', '💰 Kasım Ayı Kripto İşlemleri', '💱 Kasım Ayı Fx "
                               "İşlemleri', 'Analiz', 'PnL ve Sertifika', 'İşlem Açıklamaları', 'Duyuru' → ana grup; "
                               "'Sohbet' → sohbet grubu.")
    assert state[-2] == (f"Arkadaşınız (<code>{FRIEND}</code>) gruplarını, botu ekledikten sonra o grupta "
                         "<code>/bagla@kopya_bot ana</code> ya da <code>/bagla@kopya_bot sohbet</code> yazarak bağlar.")
    assert state[-1] == "Açmak için: <code>/grup ac</code> · " + HINT

    await w.owner_says("/grup aç")
    assert w.owner_texts()[-1] == (
        "✅ Grup kopyası açıldı: kaynak gruptaki her yeni mesaj (tüm konular, resimler dahil) arkadaşınızın bağladığı "
        "gruplara gönderilecek; size gelmez. Kapatmak için: <code>/grup kapat</code>\n"
        "Arkadaşınızın iki grubu da henüz bağlı değil; nasıl bağlayacağı ona yazıldı. Bağlanana kadar o konuların "
        "mesajları kopyalanmaz.")
    assert w.db.state["group_mirror"] == {"on": True, "topics": [], "targets": {}, "friend": None}
    assert w.friend_texts() == ["👥 Bot sahibi grup kopyasını açtı: kaynak gruptaki yeni mesajlar bağladığınız "
                                "gruplara gelecek.", BIND_HELP_BOTH]
    await w.in_group("/bagla@kopya_bot ana")
    await w.owner_says("/grup ac")                                       # again: only the sohbet group is missing
    assert w.owner_texts()[-1].endswith("Arkadaşınızın sohbet grubu henüz bağlı değil; nasıl bağlayacağı ona "
                                        "yazıldı. Bağlanana kadar o konuların mesajları kopyalanmaz.")
    assert w.friend_texts()[-1] == (
        "🔗 Grup kopyası açık. Kopyaların gideceği gruplarınızı bağlamak için botu gruba ekleyin ve o grupta aşağıdaki "
        "satırı gönderin (kopyalayıp yapıştırabilirsiniz):\n"
        "• Sohbet grubu (adında “sohbet” geçen konu): <code>/bagla@kopya_bot sohbet</code>\n"
        "Bir grup bağlanana kadar o konuların mesajları kopyalanmaz.")
    await w.owner_says("/grup")
    state = w.owner_texts()[-1].split("\n")
    assert state[0] == "👥 Grup kopyası açık."
    assert state[1] == "Ana grup (sohbet dışındaki tüm konular: sinyaller, PnL, General...): <b>Ana Grup</b> (bağlı)"
    assert state[-1] == "Kapatmak için: <code>/grup kapat</code> · " + HINT
    await w.owner_says("/grup konular PnL,  sohbet")
    assert w.owner_texts()[-1] == (
        "✅ Grup kopyası bundan sonra yalnızca adında 'PnL', 'sohbet' geçen konuların mesajlarını gönderecek. Tüm "
        "konular için: <code>/grup konular hepsi</code>")
    assert w.db.state["group_mirror"]["topics"] == ["PnL", "sohbet"]
    await w.owner_says("/grup konular")
    assert "Yalnızca adında 'PnL', 'sohbet' geçen konular kopyalanıyor." in w.owner_texts()[-1].split("\n")
    await w.owner_says("/grup konular HEPSİ")
    assert w.owner_texts()[-1] == "✅ Grup kopyası bundan sonra tüm konuların mesajlarını gönderecek."
    await w.owner_says("/grup belki")
    assert w.owner_texts()[-1] == (
        "Kullanım: <code>/grup</code> (durum), <code>/grup ac</code>, <code>/grup kapat</code>, "
        "<code>/grup konular &lt;ad, ad&gt;</code> ya da <code>/grup konular hepsi</code>")

    # only the owner, only in his private chat with the bot
    friend_before, owner_before = len(w.bot.to(FRIEND)), len(w.bot.to(OWNER))
    await w.n.handle_message(MsgEvent("/grup kapat", sender_id=FRIEND))
    await w.n.handle_message(MsgEvent("/grup kapat", sender_id=STRANGER))
    await w.n.handle_message(MsgEvent("/grup kapat", sender_id=OWNER, is_private=False))
    await w.settle()
    assert w.mirror.enabled and w.db.state["group_mirror"]["on"] is True
    assert w.friend_texts()[friend_before:] == ["Bu bot komutlarını yalnızca sahibi kullanabilir."]
    assert len(w.bot.to(OWNER)) == owner_before and w.bot.to(STRANGER) == []

    await w.owner_says("/grup KAPAT")
    assert w.owner_texts()[-1] == ("✅ Grup kopyası kapatıldı; kaynak gruptaki mesajlar artık arkadaşınızın gruplarına "
                                   "gönderilmiyor. Size gelen bildirimlerin kopyası ona gitmeye devam eder.")
    assert w.friend_texts()[-1] == ("👥 Bot sahibi grup kopyasını kapattı; kaynak gruptaki mesajlar artık gruplarınıza "
                                    "gelmeyecek.")
    stored = w.db.state["group_mirror"]
    assert stored["on"] is False and stored["targets"]["ana"] == {"chat": ANA_G, "title": "Ana Grup", "by": FRIEND}
    assert stored["friend"] == FRIEND
    assert all(m.entity in (OWNER, FRIEND, ANA_G) for m in w.bot.sent)      # (ANA_G: the /bagla answer)

    again = GroupMirror(w.db, w.n, w.listener, str(tmp_path))             # a restart reads the stored setting
    await again.load()
    assert not again.enabled and again.targets["ana"].chat == ANA_G and "sohbet" not in again.targets


async def test_g2_without_a_friend_and_when_the_setting_cannot_be_stored(tmp_path: Path) -> None:
    w = await world(tmp_path, on=False, friend=False, bind=False)
    await w.owner_says("/grup ac")
    assert w.owner_texts()[-1].endswith(
        "Kapatmak için: <code>/grup kapat</code>\nArkadaş sohbeti ayarlı değil: kopyalar <code>/arkadas "
        "&lt;sohbet_id&gt;</code> ile arkadaşınızı ekleyip o gruplarını bağlayınca başlar.")
    await w.owner_says("/grup")
    assert ("Arkadaş sohbeti ayarlı değil: kopyalar <code>/arkadas &lt;sohbet_id&gt;</code> ile arkadaşınızı ekleyip "
            "o gruplarını bağlayınca başlar.") in w.owner_texts()[-1].split("\n")

    async def broken(key: str, value: Any) -> None:
        raise OSError("disk dolu")

    w.db.set_state = broken                                              # type: ignore[method-assign]
    await w.owner_says("/grup kapat")
    assert w.owner_texts()[-1] == "❌ Ayar kaydedilemedi (<code>disk dolu</code>); grup kopyası değişmedi."
    assert w.mirror.enabled


async def test_r6_a_setting_that_cannot_be_read_refuses_changes_and_is_read_again_on_grup(tmp_path: Path) -> None:
    db = FakeDB()
    db.state["group_mirror"] = {"on": False, "topics": ["PnL"], "targets": {}, "friend": FRIEND}
    real = db.get_state
    failing = True

    async def get_state(key: str, default: Any = None) -> Any:
        if key == "group_mirror" and failing:
            raise OSError("database is locked")
        return await real(key, default)

    db.get_state = get_state                                             # type: ignore[method-assign]
    w = await world(tmp_path, db=db, on=False, bind=False)
    assert not w.mirror.loaded and not w.mirror.active
    await w.owner_says("/grup ac")
    assert w.owner_texts()[-1] == ("❌ Grup kopyasının kayıtlı ayarı şu an okunamıyor; bu yüzden kopyalama kapalı ve "
                                   "değişiklik yapılamıyor. Lütfen biraz sonra <code>/grup</code> ile tekrar deneyin.")
    assert db.state["group_mirror"] == {"on": False, "topics": ["PnL"], "targets": {}, "friend": FRIEND}   # kept
    await w.in_group("/bagla@kopya_bot ana")
    assert w.ana() == ["❌ Şu an bağlanamadı; lütfen biraz sonra tekrar deneyin."]
    failing = False                                                      # the database answers again
    await w.owner_says("/grup ac")
    assert w.owner_texts()[-1].startswith("✅ Grup kopyası açıldı: kaynak gruptaki her yeni mesaj (yalnızca adında "
                                          "'PnL' geçen konular, resimler dahil)")
    assert db.state["group_mirror"]["topics"] == ["PnL"] and db.state["group_mirror"]["on"] is True


@pytest.mark.parametrize("raw", [None, "on", ["on"], {"on": "true"}, {"on": 1}, {"on": True, "topics": "PnL"},
                                 {"on": True, "topics": [None, 5, " ", "PnL"]}, {"topics": ["x"]},
                                 {"on": True, "targets": {"ana": {"chat": "x"}, "sohbet": {"chat": 5}}}])
async def test_r6_malformed_stored_values_never_crash(tmp_path: Path, raw: Any) -> None:
    w = await world(tmp_path, on=False, bind=False)
    w.db.state["group_mirror"] = raw
    m = GroupMirror(w.db, w.n, w.listener, str(tmp_path))
    assert await m.load()
    assert m.enabled is (isinstance(raw, dict) and raw.get("on") is True)
    assert all(isinstance(t, str) and t.strip() for t in m.topics) and m.targets == {}


# =========================================================================== #
# Binding the friend's groups (/bagla)
# =========================================================================== #
async def test_b1_the_friend_binds_his_groups_with_bagla_at_the_bot(tmp_path: Path) -> None:
    w = await world(tmp_path, bind=False)
    await w.in_group("/bagla@kopya_bot ana", chat_id=ANA_G, title="Kadir Kopya")
    assert w.ana() == ["✅ Bu grup bağlandı: kaynak grubun sohbet dışındaki tüm konuları buraya kopyalanacak."]
    assert w.bot.to(ANA_G)[0].reply_to == 7
    assert w.owner_texts() == [
        "🔗 <b>Kadir Kopya</b> grubu grup kopyasının ana grup olarak bağlandı (sohbet dışındaki tüm konular: sinyaller, "
        "PnL, General...; bağlayan: arkadaşınız)."]
    await w.in_group("/BAGLA@Kopya_Bot", chat_id=SOHBET_G, title="Kadir Sohbet")      # no role: from the title
    assert w.sohbet() == ["✅ Bu grup bağlandı: kaynak grubun sohbet konusu buraya kopyalanacak."]
    await w.in_group("/bagla", chat_id=-1009000000003, title="Yedek", sender_id=OWNER)  # the owner too, bare command
    assert w.texts(-1009000000003) == ["✅ Bu grup bağlandı: kaynak grubun sohbet dışındaki tüm konuları buraya "
                                       "kopyalanacak."]
    assert w.owner_texts()[-1].endswith("(sohbet dışındaki tüm konular: sinyaller, PnL, General...; bağlayan: siz).")
    targets = w.db.state["group_mirror"]["targets"]
    assert targets == {"ana": {"chat": -1009000000003, "title": "Yedek", "by": OWNER},
                       "sohbet": {"chat": SOHBET_G, "title": "Kadir Sohbet", "by": FRIEND}}
    await w.in_group("/bagla@kopya_bot herkes", chat_id=SOHBET_G)
    assert w.sohbet()[-1] == "Kullanım: /bagla@kopya_bot ana ya da /bagla@kopya_bot sohbet"
    assert w.friend_texts() == []                                        # nothing in his private chat for this
    await w.owner_says("/bagla ana")                                     # in the private chat: where it belongs
    assert w.owner_texts()[-1] == ("ℹ️ <code>/bagla</code> arkadaşınızın grubunda yazılır: botu o gruba ekleyin, sonra o "
                                   "grupta <code>/bagla@kopya_bot ana</code> ya da <code>/bagla@kopya_bot sohbet</code> "
                                   "gönderin.")

    w2 = await world(tmp_path / "off", bind=False, on=False)
    await w2.in_group("/bagla@kopya_bot sohbet", chat_id=SOHBET_G)
    assert w2.sohbet() == ["✅ Bu grup bağlandı: kaynak grubun sohbet konusu buraya kopyalanacak. (Grup kopyası şu an "
                           "kapalı.)"]
    assert w2.owner_texts()[-1].endswith("bağlayan: arkadaşınız). Grup kopyası şu an kapalı; açmak için: "
                                         "<code>/grup ac</code>")


async def test_b2_nobody_else_binds_and_every_other_group_message_is_handled_as_before(tmp_path: Path) -> None:
    w = await world(tmp_path, bind=False)
    for event in (GroupEvent("/bagla@kopya_bot ana", sender_id=STRANGER),           # a member of his group
                  GroupEvent("/bagla ana", sender_id=None),                           # an anonymous admin
                  GroupEvent("/bagla@kopya_bot ana", sender_id=OWNER, is_group=False)):   # a channel
        await w.n.handle_message(event)
    await w.settle()
    assert w.bot.sent == [] and w.mirror.targets == {}                   # silently ignored: no reply anywhere
    # not a /bagla for this bot: nothing is answered in a group, by anybody (as the owner's own were at 8eef65b)
    await w.n.handle_message(GroupEvent("/bagla@baska_bot ana", sender_id=OWNER))
    await w.n.handle_message(GroupEvent("/durum@kopya_bot", sender_id=OWNER))
    await w.n.handle_message(GroupEvent("merhaba", sender_id=STRANGER))
    await w.settle()
    assert w.bot.sent == [] and w.n._engine.calls == []
    for text in ("/bagla@baska_bot ana", "güzel sinyal, teşekkürler", "/durum"):     # the friend in a group (N6)
        await w.n.handle_message(GroupEvent(text, sender_id=FRIEND, reply_to_msg_id=5))
    await w.settle()
    assert w.bot.sent == [] and w.mirror.targets == {}           # nothing answered, not even in his private chat

    plain = await world(tmp_path / "absent", mirror=False, bind=False)   # no mirror at all: /bagla is ignored
    await plain.n.handle_message(GroupEvent("/bagla@kopya_bot ana", sender_id=OWNER))
    await plain.settle()
    assert plain.bot.sent == []


async def test_b3_a_new_friend_drops_the_bindings_queue_and_edit_memory_and_gets_the_instructions(
    tmp_path: Path,
) -> None:
    w = await world(tmp_path)
    await w.deliver(Msg(id=2401, message="ilk", reply_to=in_topic(SOHBET)))
    await w.settle()
    (first,) = w.bot.to(SOHBET_G)
    held = asyncio.get_running_loop().create_future()
    w.mirror.offer(Msg(id=2402, message="bekleyen", reply_to=in_topic(SOHBET)), handled=held)
    await asyncio.sleep(0.02)
    w.mirror.offer(Msg(id=2403, message="sırada", reply_to=in_topic(SOHBET)))
    await w.n.handle_message(MsgEvent(f"/arkadas {NEW_FRIEND}", sender_id=OWNER))
    await w.n.drain(5)
    held.set_result(None)
    await w.settle()
    assert w.owner_texts()[-1] == (
        f"✅ Arkadaşınızın sohbeti kaydedildi (<code>{NEW_FRIEND}</code>); deneme mesajı ulaştı. Bundan sonra size gelen "
        "her bildirim ona da gider; bağlantı yokken atlanan tickmill işlemleri <code>/sonradan</code> ayarına göre ya "
        "kendiliğinden açılır ya da ona sorulur.\n"
        "Eski arkadaşınızın grup kopyası için bağladığı gruplar ve bekleyen kopyalar kaldırıldı.\n"
        "Grup kopyası açık: arkadaşınıza gruplarını nasıl bağlayacağı yazıldı.")
    assert w.texts(NEW_FRIEND)[-1] == BIND_HELP_BOTH
    await w.edit(Msg(id=2401, message="ilk (düzeltildi)", reply_to=in_topic(SOHBET)))
    await w.settle()
    assert w.sohbet() == ["💬 <b>Sohbet</b> · Kadir\nilk"]              # nothing else: no queue, no edit, no binding
    assert w.mirror.targets == {} and w.db.state["group_mirror"]["targets"] == {}
    assert w.db.state["group_mirror"]["friend"] == NEW_FRIEND
    await w.in_group("/bagla@kopya_bot sohbet", chat_id=SOHBET_G, sender_id=FRIEND)    # the old friend: no more
    assert w.mirror.targets == {}

    await w.owner_says("/arkadas sil")
    assert w.owner_texts()[-1] == ("✅ Arkadaş sohbeti kaldırıldı; bildirimler artık yalnızca size gelir (<code>/sonradan "
                                   "sor</code> seçiliyse tickmill sorularını siz cevaplarsınız).")              # (nothing left to drop)
    w2 = await world(tmp_path / "sil")
    await w2.owner_says("/arkadas sil")
    assert w2.owner_texts()[-1] == ("✅ Arkadaş sohbeti kaldırıldı; bildirimler artık yalnızca size gelir (<code>/sonradan "
                                    "sor</code> seçiliyse tickmill sorularını siz cevaplarsınız).\nGrup kopyası için "
                                    "bağlanan gruplar ve bekleyen kopyalar da kaldırıldı.")


async def test_b3_bindings_of_another_friend_are_dropped_at_start(tmp_path: Path) -> None:
    db = FakeDB()
    db.state["group_mirror"] = {"on": True, "topics": [], "friend": 9999,       # (the friend was changed by 8eef65b)
                                "targets": {"ana": {"chat": ANA_G, "title": "Eski", "by": 9999}}}
    w = await world(tmp_path, db=db, bind=False)
    assert w.mirror.targets == {} and db.state["group_mirror"]["targets"] == {}
    assert db.state["group_mirror"]["friend"] == FRIEND and w.mirror.enabled


async def test_b4_a_group_upgraded_to_a_supergroup_is_followed_and_the_copy_sent_once(tmp_path: Path) -> None:
    w = await world(tmp_path, bind=False)
    basic, channel_id = -4711, 2000000123
    await w.in_group("/bagla@kopya_bot ana", chat_id=basic, title="Kadir Kopya")
    w.bot.sent.clear()
    w.bot.fail_once_for[basic] = [errors.ChatIdInvalidError(request=None)]
    w.bot.chats_reply = SimpleNamespace(chats=[SimpleNamespace(id=4711, migrated_to=types.InputChannel(channel_id, 99))])
    await w.deliver(Msg(id=501, message="Eth short 1R", reply_to=in_topic(CRYPTO)))
    await w.settle()
    new_id = -1000000000000 - channel_id
    (copy,) = w.bot.sent
    assert isinstance(copy.entity, types.InputPeerChannel) and copy.entity.channel_id == channel_id
    assert copy.text == f"💬 <b>{TITLES[CRYPTO]}</b> · Kadir\nEth short 1R"
    assert w.db.state["group_mirror"]["targets"]["ana"]["chat"] == new_id and w.mirror.failed == 0
    assert w.bot.requests[0].id == [4711]


async def test_b5_a_group_the_bot_was_removed_from_is_marked_broken_and_the_owner_told(tmp_path: Path) -> None:
    w = await world(tmp_path)
    w.bot.fail_for[ANA_G] = errors.ChatWriteForbiddenError(request=None)
    await w.deliver(Msg(id=601, message="bir", reply_to=in_topic(CRYPTO)), Msg(id=602, message="iki", reply_to=in_topic(PNL)),
                    Msg(id=603, message="sohbet", reply_to=in_topic(SOHBET)))
    await w.settle()
    assert w.owner_texts() == [
        "⚠️ Bot arkadaşınızın ana grubuna (<b>Ana Grup</b>) artık yazamıyor (<code>You can't write in this chat (caused "
        "by NoneType)</code>): bot gruptan çıkarılmış ya da grup silinmiş olabilir. Bu grubun kopyaları durdu; "
        "arkadaşınız botu gruba yeniden ekleyip orada <code>/bagla@kopya_bot ana</code> yazmalı."]
    assert w.mirror.failed == 1 and w.mirror.skipped["ana"] == 1 and w.sohbet() == ["💬 <b>Sohbet</b> · Kadir\nsohbet"]
    assert w.db.state["group_mirror"]["targets"]["ana"]["broken"]
    await w.owner_says("/grup")
    assert ("Ana grup (sohbet dışındaki tüm konular: sinyaller, PnL, General...): <b>Ana Grup</b> (bağlantı koptu: bot "
            "oraya yazamıyor; <code>/bagla@kopya_bot ana</code> ile yeniden bağlanmalı)") in w.owner_texts()[-1].split("\n")
    assert ("Bot başladığından beri ana grup bağlı olmadığı için atlanan: 1; gönderilemeyen: 1 mesaj."
            in w.owner_texts()[-1].split("\n"))
    del w.bot.fail_for[ANA_G]
    await w.in_group("/bagla@kopya_bot ana")                             # bound again
    await w.deliver(Msg(id=604, message="üç", reply_to=in_topic(CRYPTO)))
    await w.settle()
    assert w.ana()[-1] == f"💬 <b>{TITLES[CRYPTO]}</b> · Kadir\nüç"


async def test_b6_an_unbound_role_is_skipped_counted_and_never_sent_anywhere_else(tmp_path: Path) -> None:
    w = await world(tmp_path, bind=False)
    await w.mirror.bind("ana", ANA_G, "Ana Grup", FRIEND)
    await w.deliver(Msg(id=701, message="selam", reply_to=in_topic(SOHBET)), Msg(id=702, message="+3R", reply_to=in_topic(PNL)))
    await w.settle()
    assert w.ana() == ["💬 <b>PnL ve Sertifika</b> · Kadir\n+3R"]
    assert {m.entity for m in w.bot.sent} == {ANA_G} and w.mirror.skipped == {"ana": 0, "sohbet": 1}
    await w.owner_says("/grup")
    assert "Bot başladığından beri sohbet grubu bağlı olmadığı için atlanan: 1 mesaj." in w.owner_texts()[-1].split("\n")


# =========================================================================== #
# What is copied, and where to
# =========================================================================== #
async def test_g3_every_topic_goes_to_its_group_and_nowhere_else(tmp_path: Path) -> None:
    w = await world(tmp_path)
    chat = Msg(id=101, message="selam <b>herkes</b> & hoş geldiniz", reply_to=in_topic(SOHBET), sender=ANIL,
               sender_id=OTHER)
    pnl = Msg(id=102, photo=SimpleNamespace(id=7), payload=b"pnl-certificate", reply_to=in_topic(PNL), sender=ANIL,
              sender_id=OTHER)
    signal = Msg(id=103, message="Eth short\n1R risk", photo=SimpleNamespace(id=8), payload=b"chart",
                 reply_to=in_topic(CRYPTO))
    fx = Msg(id=104, message="EUR LONG 1R Limit Emir", reply_to=in_topic(FX))
    general = Msg(id=105, message="duyuru yarın", sender=ANIL, sender_id=OTHER)
    reply = Msg(id=106, message="Aktif", reply_to=in_topic(FX, 104), sender=ANIL, sender_id=OTHER)
    explained = Msg(id=107, message="ETH işleminin açıklaması", reply_to=in_topic(ACIKLAMA))
    own = Msg(id=108, message="sahibin kendi mesajı", out=True, reply_to=in_topic(DUYURU))
    await w.deliver(chat, pnl, signal, fx, general, reply, explained, own)
    await w.settle()

    assert {m.entity for m in w.bot.sent} == {ANA_G, SOHBET_G}          # every bot send went to one of his groups
    assert w.sohbet() == ["💬 <b>Sohbet</b> · Anıl K.\nselam &lt;b&gt;herkes&lt;/b&gt; &amp; hoş geldiniz"]
    assert w.ana() == [
        "💬 <b>PnL ve Sertifika</b> · Anıl K.",
        f"💬 <b>{TITLES[CRYPTO]}</b> · Kadir\nEth short\n1R risk",
        f"💬 <b>{TITLES[FX]}</b> · Kadir\nEUR LONG 1R Limit Emir",
        "💬 <b>General</b> · Anıl K.\nduyuru yarın",
        f"💬 <b>{TITLES[FX]}</b> · Anıl K.\nAktif",
        "💬 <b>İşlem Açıklamaları</b> · Kadir\nETH işleminin açıklaması",
        "💬 <b>Duyuru</b> · Kadir\nsahibin kendi mesajı",
    ]
    files = {m.text: m for m in w.bot.files}
    assert files["💬 <b>PnL ve Sertifika</b> · Anıl K."].data == b"pnl-certificate"
    copied_signal = files[f"💬 <b>{TITLES[CRYPTO]}</b> · Kadir\nEth short\n1R risk"]
    assert copied_signal.data == b"chart" and copied_signal.force_document is False and copied_signal.attributes is None
    # the signal's picture: the listener's own download is uploaded, never downloaded a second time
    assert signal.downloads == 1 and copied_signal.file == w.db.messages[(CHAT, 103)].media_path
    assert Path(copied_signal.file).is_file()                            # (the engine's file stays where it is)
    assert pnl.downloads == 1 and (CHAT, 102) not in w.db.messages       # the copy's own download; topic still ignored
    assert not (tmp_path / "group_copy" / f"{abs(CHAT)}_102").exists()   # (the download is removed after the upload)
    assert sorted(w.rec.calls) == sorted([(CHAT, OTHER, FX), (CHAT, TRADER, CRYPTO), (CHAT, TRADER, FX)])
    assert all(m.reply_to is None for m in w.bot.sent) and w.mirror.failed == 0
    assert w.db.state["group_mirror_last"] == {str(CHAT): 108}


def scenario(date: datetime) -> tuple[list[Any], list[Any]]:
    """(new messages, edits): photos and text in the signal topics and elsewhere, a reply, an edit."""
    messages = [
        Msg(id=201, photo=SimpleNamespace(id=1), payload=b"chart", reply_to=in_topic(CRYPTO)),
        Msg(id=202, message="Eth short\n1R risk\n100 risk için 8.7 lot", reply_to=in_topic(CRYPTO)),
        Msg(id=203, message="bakın", photo=SimpleNamespace(id=3), reply_to=in_topic(SOHBET), sender=ANIL,
            sender_id=OTHER),
        Msg(id=204, message="EUR LONG 1R Limit Emir", photo=SimpleNamespace(id=2), payload=b"fx",
            reply_to=in_topic(FX)),
        Msg(id=205, message="+3R", photo=SimpleNamespace(id=4), reply_to=in_topic(PNL)),
        Msg(id=206, message="Aktif", reply_to=in_topic(FX, 204), sender=ANIL, sender_id=OTHER),
        Msg(id=207, message="selam", sender=ANIL, sender_id=OTHER),
    ]
    edits = [Msg(id=202, message="Eth short\n1R risk\n100 risk için 9 lot", reply_to=in_topic(CRYPTO)),
             Msg(id=204, message="EUR LONG 1R Limit Emir\nSL 1.0790", photo=SimpleNamespace(id=2), payload=b"fx",
                 reply_to=in_topic(FX))]
    for m in (*messages, *edits):
        m.date = date                                           # (the same messages in every run)
    return messages, edits


@pytest.mark.parametrize("variant", ["on", "allowed_on"])
async def test_g4_the_signal_handling_is_the_same_with_the_copy_absent_off_or_on(tmp_path: Path, variant: str) -> None:
    cfg: dict[str, Any] = {"debounce_sec": 0.1}
    if variant == "allowed_on":
        cfg["allowed_sender_ids"] = [TRADER]                             # only the trader's messages are signals
    runs: dict[str, Any] = {}
    date = utcnow()
    for name, kw in (("absent", {"mirror": False}), ("off", {"on": False}), ("on", {})):
        w = await world(tmp_path / name, work=0.05, **cfg, **kw)
        messages, edits = scenario(date)
        t0 = time.monotonic()
        await w.deliver(*messages)
        await asyncio.sleep(0.02)
        await w.edit(edits[0])                                  # (its copy still waits: it shows the new text)
        await w.settle()
        await asyncio.sleep(0.15)
        await w.settle()
        await w.edit(edits[1])                                  # (copied already: a "düzenledi" copy)
        await w.settle()
        base = str(tmp_path / name)

        def dump(m: Any, base: str = base) -> dict[str, Any]:
            d = m.model_dump()
            d["media_path"] = d["media_path"] and d["media_path"].replace(base, "<tmp>")
            return d

        runs[name] = {
            "rows": {k: dump(v) for k, v in w.db.messages.items()},
            "state": {k: v for k, v in w.db.state.items() if not k.startswith("group_mirror")},
            "calls": list(w.rec.calls),
            "snapshots": list(w.rec.snapshots),
            "edits": [dump(e) for e in w.rec.edits],
            "owner": w.owner_texts(),
            "signal_downloads": {m.id: m.downloads for m in messages
                                 if (CHAT, m.id) in w.db.messages and m.photo is not None},
            "ready_at": sorted(start - t0 for start, _ in w.rec.times),
            "groups": w.ana() + w.sohbet(),
        }
    absent, off, on = runs["absent"], runs["off"], runs["on"]
    for key in ("rows", "state", "calls", "snapshots", "edits", "owner", "signal_downloads"):
        assert absent[key] == off[key] == on[key], key
    assert on["signal_downloads"] == {201: 1, 204: 1}                    # the copy never downloads a signal again
    for a, b in zip(absent["ready_at"], on["ready_at"], strict=True):     # the engine is called at the same moments
        assert abs(a - b) < 0.08, (absent["ready_at"], on["ready_at"])
    assert absent["groups"] == off["groups"] == []
    assert len([t for t in on["groups"] if t.startswith("💬")]) == 7    # every message of every topic
    assert f"💬 <b>{TITLES[CRYPTO]}</b> · Kadir\nEth short\n1R risk\n100 risk için 9 lot" in on["groups"]
    assert on["groups"][-2] == f"✏️ <b>{TITLES[FX]}</b> · Kadir (düzenledi)\nEUR LONG 1R Limit Emir\nSL 1.0790"


# =========================================================================== #
# Never in the way of trading (isolation review)
# =========================================================================== #
async def test_g5_media_waits_until_the_signal_is_handled_and_the_engine_is_done(tmp_path: Path) -> None:
    w = await world(tmp_path, work=0.3)
    signal = Msg(id=302, message="BTC long 1R", photo=SimpleNamespace(id=2), payload=b"chart",
                 reply_to=in_topic(CRYPTO), download_delay=0.3)
    chat_photo = Msg(id=301, photo=SimpleNamespace(id=1), reply_to=in_topic(SOHBET), sender=ANIL, sender_id=OTHER)
    w.listener._spawn(w.listener.handle_message(signal, live=True))     # a signal is being handled ...
    await asyncio.sleep(0.01)
    await w.deliver(chat_photo)                                          # ... when a chat photo comes in
    await w.settle()
    ((_, ready_end),) = w.rec.times
    assert chat_photo.started_at is not None and chat_photo.started_at >= ready_end   # not while stored / in engine
    assert w.bot.files[0].at >= ready_end and w.bot.files[0].data == b"img"
    signal2 = Msg(id=303, message="ETH long 1R", photo=SimpleNamespace(id=3), payload=b"chart2",
                  reply_to=in_topic(CRYPTO), download_delay=0.1)
    await w.deliver(signal2)
    await w.settle()
    (_, ready2_end) = w.rec.times[-1]
    (upload,) = [m for m in w.bot.files if m.data == b"chart2"]
    assert upload.at >= ready2_end and signal2.downloads == 1


async def test_g5_a_download_pauses_between_its_chunks_while_a_signal_comes_in(tmp_path: Path) -> None:
    w = await world(tmp_path, work=0.3)
    signal = Msg(id=402, message="SOL short 1R", photo=SimpleNamespace(id=2), reply_to=in_topic(CRYPTO),
                 download_delay=0.2)

    async def signal_arrives() -> None:
        await w.deliver(signal)
        await asyncio.sleep(0.01)                                # (the next chunk is on its way meanwhile)

    big = Msg(id=401, document=SimpleNamespace(id=9, mime_type="application/pdf", size=2000, attributes=[
        types.DocumentAttributeFilename(file_name="../Sertifika Kasım.pdf")]), payload=b"x" * 2000,
        reply_to=in_topic(PNL), between_chunks=signal_arrives)
    await w.deliver(big)
    await w.settle()
    (_, ready_end), = w.rec.times
    assert big.finished_at is not None and big.finished_at >= ready_end    # the second chunk came after the engine
    (upload,) = [m for m in w.bot.files if m.text.startswith("💬 <b>PnL")]
    assert upload.force_document is True and upload.data == b"x" * 2000
    assert upload.attributes == [types.DocumentAttributeFilename("Sertifika Kasım.pdf")]   # its own name, as an attribute
    assert Path(big.files[0]).name == "media.pdf"                         # ... the download a fixed safe name


async def test_g5_an_owner_tap_being_handled_also_holds_the_media(tmp_path: Path) -> None:
    w = await world(tmp_path)
    gate = asyncio.Event()

    async def tap() -> None:
        await gate.wait()                                    # (an approval placing orders)

    w.n._spawn(tap())
    photo = Msg(id=501, photo=SimpleNamespace(id=1), reply_to=in_topic(SOHBET))
    text = Msg(id=502, message="yazı", reply_to=in_topic(SOHBET))
    await w.deliver(photo, text)
    await asyncio.sleep(0.2)
    assert photo.downloads == 0 and w.sohbet() == []
    released = time.monotonic()
    gate.set()
    await w.settle()
    assert photo.started_at >= released
    assert w.sohbet() == ["💬 <b>Sohbet</b> · Kadir", "💬 <b>Sohbet</b> · Kadir\nyazı"]


async def test_i3_an_edited_signal_in_the_engine_counts_as_busy(tmp_path: Path) -> None:
    w = await world(tmp_path, work=0.3)
    sig = Msg(id=900, message="Eth long\n1R risk\nGiriş 2232 SL 2205", reply_to=in_topic(CRYPTO))
    await w.deliver(sig)
    await w.settle()
    samples: list[bool] = []
    real = w.rec.on_edit

    async def on_edit(msg: Any) -> None:
        samples.append(w.mirror._busy())
        await real(msg)
        samples.append(w.mirror._busy())

    w.listener._on_edit = on_edit
    photo = Msg(id=901, photo=SimpleNamespace(id=1), reply_to=in_topic(SOHBET))
    await w.edit(replace(sig, message="Eth long\n1R risk\nGiriş 2232 SL 2215 (SL güncellendi)"))
    await asyncio.sleep(0.01)
    await w.deliver(photo)
    await w.settle()
    ((_, edit_end),) = w.rec.edit_times
    assert samples == [True, True] and photo.started_at >= edit_end


async def test_i3_an_order_flow_or_a_monitor_sync_of_the_engine_counts_as_busy(tmp_path: Path) -> None:
    gate = asyncio.Event()
    flow = asyncio.ensure_future(gate.wait())
    engine = SimpleNamespace(_critical={flow}, monitor=SimpleNamespace(_running={}))
    w = await world(tmp_path, engine=engine)
    photo = Msg(id=950, photo=SimpleNamespace(id=1), reply_to=in_topic(SOHBET))
    await w.deliver(photo)
    await asyncio.sleep(0.15)
    assert photo.downloads == 0
    sync = asyncio.ensure_future(asyncio.sleep(0.2))
    engine.monitor._running[1] = sync
    gate.set()
    await asyncio.sleep(0.05)
    assert photo.downloads == 0                                  # the monitor's sync now
    await w.settle()
    assert photo.downloads == 1 and photo.started_at is not None and sync.done()


async def test_i2_each_upload_attempt_gets_a_fresh_pause_with_its_own_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(gm, "PAUSE_MAX_SEC", 0.3)
    w = await world(tmp_path, work=0.2)
    w.n._group_hold_until = time.monotonic() + 0.6               # a group FloodWait first, longer than one pause limit
    w.bot.parts, w.bot.part_delay = 6, 0.05
    doc = Msg(id=960, message="rapor", document=SimpleNamespace(id=9, mime_type="application/pdf", size=10,
                                                                attributes=[]), payload=b"r" * 600,
              reply_to=in_topic(SOHBET))
    await w.deliver(doc)
    while not w.bot.part_times:
        await asyncio.sleep(0.01)
    await w.deliver(Msg(id=961, message="BTC long 1R", reply_to=in_topic(CRYPTO)))   # a signal during the upload
    await w.settle()
    ((start, end),) = w.rec.times
    inside = [t for t in w.bot.part_times if start + 0.06 < t < end]
    assert inside == []                                          # no part STARTED while the signal was in the engine
    assert [m.data for m in w.bot.files] == [b"r" * 600]


async def test_i2_a_pause_past_its_limit_gives_up_the_transfer_and_sends_the_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(gm, "PAUSE_MAX_SEC", 0.2)
    monkeypatch.setattr(gm, "LAST_WRITE_WAIT_SEC", 0.05)
    w = await world(tmp_path)
    gate = asyncio.Event()
    w.n._spawn(gate.wait())                                      # an owner tap that takes long
    w.mirror._fetch = lambda m, media: _fake_fetch(tmp_path)     # type: ignore[method-assign]  (file already here)
    await w.deliver(Msg(id=970, message="grafik", photo=SimpleNamespace(id=1), reply_to=in_topic(SOHBET)))
    await asyncio.sleep(0.5)
    gate.set()
    await w.settle()
    assert w.sohbet() == ["💬 <b>Sohbet</b> · Kadir\ngrafik\n📎 Fotoğraf kopyalanmadı."] and w.bot.files == []


async def _fake_fetch(tmp_path: Path) -> tuple[str, Path | None]:
    path = tmp_path / "ready.jpg"
    path.write_bytes(b"x")
    return str(path), None


async def test_i4_shutdown_aborts_a_transfer_on_its_way(tmp_path: Path) -> None:
    w = await world(tmp_path)
    w.bot.parts, w.bot.part_delay = 20, 0.05
    await w.deliver(Msg(id=980, message="büyük", photo=SimpleNamespace(id=1), payload=b"p" * 2000,
                        reply_to=in_topic(SOHBET)))
    while len(w.bot.part_times) < 2:
        await asyncio.sleep(0.01)
    await w.mirror.stop()
    stopped = time.monotonic()
    await asyncio.sleep(0.3)
    assert [t for t in w.bot.part_times if t > stopped + 0.06] == []      # the next part raised: no more transfer
    assert w.bot.files == [] and w.sohbet() == []                        # (and nothing sent at shutdown)


async def test_i1_media_moves_only_with_a_native_aes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    assert gm.native_aes() is True                                       # (this machine: libssl / cryptg)
    caplog.set_level(logging.WARNING, logger="copytrader.telegram.group_mirror")
    monkeypatch.setattr(gm, "native_aes", lambda: False)                 # Windows without cryptg
    w = await world(tmp_path)
    photos = [Msg(id=990 + i, message=f"resim {i}", photo=SimpleNamespace(id=i), reply_to=in_topic(SOHBET))
              for i in range(2)]
    await w.deliver(*photos)
    await w.settle()
    assert w.sohbet() == [f"💬 <b>Sohbet</b> · Kadir\nresim {i}\n📎 Fotoğraf kopyalanmadı." for i in range(2)]
    assert all(p.downloads == 0 for p in photos) and w.bot.files == []
    assert len([r for r in caplog.records if "no native AES" in r.getMessage()]) == 1    # logged once
    await w.owner_says("/grup")
    assert ("Resimler ve dosyalar kopyalanmıyor (yalnızca metinleri): bu bilgisayarda hızlı şifreleme paketi (cryptg) "
            "kurulu değil.") in w.owner_texts()[-1].split("\n")
    await w.owner_says("/grup ac")
    assert w.owner_texts()[-1].startswith(
        "✅ Grup kopyası açıldı: kaynak gruptaki her yeni mesaj (tüm konular, yalnızca metinler: bu bilgisayarda hızlı "
        "şifreleme paketi (cryptg) kurulu olmadığı için resimler ve dosyalar kopyalanmaz) arkadaşınızın bağladığı "
        "gruplara gönderilecek")


def test_i1_cryptg_is_an_optional_update_step_not_a_requirement() -> None:
    root = Path(__file__).resolve().parents[1]
    lines = root.joinpath("requirements.txt").read_text(encoding="utf-8").splitlines()
    assert not any("cryptg" in line.split("#", 1)[0] for line in lines)     # (pip never runs for it when stopped)
    assert 'CRYPTG = "cryptg==0.6.1"' in root.joinpath("guncelle.py").read_text(encoding="utf-8")


# =========================================================================== #
# Media that cannot be copied (generic notes: the groups may have other members)
# =========================================================================== #
async def test_g6_media_that_cannot_be_copied_leaves_the_text_and_one_generic_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(gm, "DOWNLOAD_TIMEOUT_SEC", 0.2)
    caplog.set_level(logging.WARNING, logger="copytrader.telegram.group_mirror")
    w = await world(tmp_path)
    poll_media = type("MessageMediaPoll", (), {})()
    web = type("MessageMediaWebPage", (), {})()
    sticker = SimpleNamespace(id=5, mime_type="image/webp", size=10, attributes=[
        types.DocumentAttributeSticker(alt="🚀", stickerset=types.InputStickerSetEmpty())])
    video = SimpleNamespace(id=6, mime_type="video/mp4", size=6 * 1024 * 1024, attributes=[])
    disk = OSError(28, "No space left on device", r"C:\Users\SOMEUSER\copytrader\data\group_copy\1_2\media.jpg")
    msgs = [
        Msg(id=601, message="büyük video", document=video, reply_to=in_topic(SOHBET)),
        Msg(id=602, message="bozuk", photo=SimpleNamespace(id=1), download_error=disk, reply_to=in_topic(SOHBET)),
        Msg(id=603, message="yavaş", photo=SimpleNamespace(id=2), download_delay=1.0, reply_to=in_topic(SOHBET)),
        Msg(id=604, document=sticker, reply_to=in_topic(SOHBET)),
        Msg(id=605, message="hangisi?", media=poll_media, reply_to=in_topic(SOHBET)),
        Msg(id=606, message="https://example.com bakın", media=web, photo=SimpleNamespace(id=3),
            reply_to=in_topic(SOHBET)),
    ]
    await w.deliver(*msgs)
    await w.settle()
    h = "💬 <b>Sohbet</b> · Kadir"
    assert w.sohbet() == [
        f"{h}\nbüyük video\n📎 Video kopyalanmadı (dosya çok büyük; en çok 5 MB).",
        f"{h}\nbozuk\n📎 Fotoğraf kopyalanmadı.",
        f"{h}\nyavaş\n📎 Fotoğraf kopyalanmadı.",
        f"{h}\n📎 Bu mesajdaki çıkartma kopyalanmadı.",
        f"{h}\nhangisi?\n📎 Bu mesajdaki anket kopyalanmadı.",
        f"{h}\nhttps://example.com bakın",                     # a link preview is not a picture to copy
    ]
    assert not any("SOMEUSER" in m.text for m in w.bot.sent)            # the local path stays in the log
    assert any("SOMEUSER" in r.getMessage() for r in caplog.records)
    assert msgs[0].downloads == 0 and msgs[5].downloads == 0 and w.bot.files == [] and w.bot.to(OWNER) == []
    assert not (tmp_path / "group_copy").exists() or not any((tmp_path / "group_copy").iterdir())


async def test_g6_size_found_during_the_download_refused_upload_long_caption_and_device_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(gm, "MAX_FILE_BYTES", 1024 * 1024)
    w = await world(tmp_path)
    h = "💬 <b>Sohbet</b> · Kadir"
    await w.deliver(Msg(id=701, message="ağır", photo=SimpleNamespace(id=1), payload=b"x" * (1024 * 1024 + 10),
                        reply_to=in_topic(SOHBET)))
    await w.settle()
    assert w.sohbet() == [f"{h}\nağır\n📎 Fotoğraf kopyalanmadı (dosya çok büyük; en çok 1 MB)."]

    w.bot.fail_once_for[SOHBET_G] = [errors.PhotoInvalidDimensionsError(request=None)]    # refused as a photo ...
    await w.deliver(Msg(id=702, message="garip resim", photo=SimpleNamespace(id=2), payload=b"odd",
                        reply_to=in_topic(SOHBET)))
    await w.settle()
    (as_file,) = [m for m in w.bot.files if m.data == b"odd"]                            # ... it goes as a file
    assert as_file.force_document is True and as_file.text == f"{h}\ngarip resim"
    w.bot.fail_once_for[SOHBET_G] = [errors.PhotoInvalidDimensionsError(request=None),
                                     errors.MediaEmptyError(request=None)]               # refused both ways
    await w.deliver(Msg(id=704, message="olmadı", photo=SimpleNamespace(id=4), reply_to=in_topic(SOHBET)))
    await w.settle()
    assert w.sohbet()[-1] == f"{h}\nolmadı\n📎 Fotoğraf kopyalanmadı." and w.owner_texts() == []

    long_text = "uzun açıklama " * 100                          # > 1024 characters: no room in a caption
    await w.deliver(Msg(id=703, message=long_text, photo=SimpleNamespace(id=3), payload=b"pic",
                        reply_to=in_topic(SOHBET)))
    await w.settle()
    text_copy, file_copy = w.bot.to(SOHBET_G)[-2:]
    assert text_copy.text == f"{h}\n{long_text}" and file_copy.text == h and file_copy.data == b"pic"

    con = Msg(id=705, document=SimpleNamespace(id=8, mime_type="application/pdf", size=5, attributes=[
        types.DocumentAttributeFilename(file_name="CON.pdf")]), payload=b"pdf", reply_to=in_topic(SOHBET))
    await w.deliver(con)
    await w.settle()
    assert Path(con.files[0]).name == "media.pdf"                       # never a device name on disk
    assert w.bot.files[-1].attributes == [types.DocumentAttributeFilename("CON.pdf")]


# =========================================================================== #
# Queue, failures, FloodWait, no answer (friend's rules)
# =========================================================================== #
async def test_g7_the_queue_is_bounded_and_drops_the_oldest_with_a_log_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(gm, "QUEUE_MAX", 3)
    w = await world(tmp_path)
    held = asyncio.get_running_loop().create_future()          # the first copy waits for "its handling"
    msgs = [Msg(id=800 + i, message=f"mesaj {i}", reply_to=in_topic(SOHBET)) for i in range(6)]
    caplog.set_level(logging.WARNING, logger="copytrader.telegram.group_mirror")
    w.mirror.offer(msgs[0], handled=held)
    await asyncio.sleep(0.05)                                   # (the worker has taken it and waits)
    for m in msgs[1:]:
        w.mirror.offer(m)
    assert w.mirror.waiting == 3 and w.mirror.dropped == 2
    drops = [r.getMessage() for r in caplog.records if "queue full" in r.getMessage()]
    assert len(drops) == 2 and f"{CHAT}/801" in drops[0] and f"{CHAT}/802" in drops[1]
    held.set_result(None)
    await w.settle()
    assert [body(t) for t in w.sohbet()] == ["mesaj 0", "mesaj 3", "mesaj 4", "mesaj 5"]
    await w.owner_says("/grup")
    assert ("Bot başladığından beri sıra dolduğu ya da bot kapandığı için atlanan: 2 mesaj."
            in w.owner_texts()[-1].split("\n"))


async def test_r7_shutdown_drops_are_counted(tmp_path: Path) -> None:
    w = await world(tmp_path)
    held = asyncio.get_running_loop().create_future()
    w.mirror.offer(Msg(id=2701, message="a", reply_to=in_topic(SOHBET)), handled=held)
    await asyncio.sleep(0.05)
    for i in range(5):
        w.mirror.offer(Msg(id=2702 + i, message=f"b{i}", reply_to=in_topic(SOHBET)))
    await w.mirror.stop()
    held.set_result(None)
    assert w.mirror.dropped == 6 and w.bot.sent == []           # the 5 waiting and the one the worker was on


async def test_g8_r3_failures_are_told_once_an_hour_with_a_throttle_of_their_own(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(notifier_mod, "SEND_TIMEOUT_SEC", 0.2)
    w = await world(tmp_path)
    w.bot.hang_for.add(ANA_G)                                   # Telegram does not answer for that group
    caplog.set_level(logging.ERROR, logger="copytrader.telegram.group_mirror")
    await w.deliver(Msg(id=901, message="bir", reply_to=in_topic(CRYPTO)), Msg(id=902, message="iki", reply_to=in_topic(PNL)))
    await w.settle()
    assert w.owner_texts() == ["⚠️ Gruptan bir mesaj arkadaşınızın ana grubuna (<b>Ana Grup</b>) kopyalanamadı: "
                               "Telegram yanıt vermedi. (Bu uyarı en fazla saatte bir gönderilir.)"]
    assert w.mirror.failed == 2 and len([r for r in caplog.records if "to the ana group failed" in r.getMessage()]) == 2
    # later the friend blocks the bot: the owner hears about his notifications all the same (reviewer r3)
    w.bot.fail_for[FRIEND] = errors.UserIsBlockedError(request=None)
    await w.n.notify("tickmill: ETH long açıldı")
    await w.settle()
    assert any(t.startswith(f"⚠️ Arkadaşınıza (<code>{FRIEND}</code>) mesaj iletilemedi") for t in w.owner_texts())


async def test_g9_flood_wait_is_waited_for_each_copy_goes_once_and_the_friends_queue_waits_it_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(notifier_mod, "FLOOD_MARGIN_SEC", 0.0)
    w = await world(tmp_path)
    w.bot.fail_once_for[SOHBET_G] = [errors.FloodWaitError(request=None, capture=1)]
    t0 = time.monotonic()
    await w.deliver(Msg(id=1001, message="bir", reply_to=in_topic(SOHBET)),
                    Msg(id=1002, message="iki", reply_to=in_topic(SOHBET)))
    await asyncio.sleep(0.2)
    await w.n.notify("bildirim")                                # the friend's own copy during the group FloodWait
    await w.settle()
    assert time.monotonic() - t0 >= 1.0
    assert [body(t) for t in w.sohbet()] == ["bir", "iki"]     # each once, in order
    friend_copy = [m for m in w.bot.to(FRIEND) if m.text == "ℹ️ bildirim"]
    assert len(friend_copy) == 1 and w.mirror.failed == 0
    assert w.bot.to(OWNER)[0].text == "ℹ️ bildirim"

    w.bot.fail_once_for[SOHBET_G] = [errors.FloodWaitError(request=None, capture=10_000)]   # far too long: given up
    await w.deliver(Msg(id=1003, message="üç", reply_to=in_topic(SOHBET)))
    await w.settle()
    assert w.mirror.failed == 1 and [body(t) for t in w.sohbet()] == ["bir", "iki"]
    assert w.owner_texts()[-1].startswith("⚠️ Gruptan bir mesaj arkadaşınızın sohbet grubuna (<b>Sohbet Grubu</b>) "
                                          "kopyalanamadı: <code>A wait of 10000 seconds is required")
    assert w.n._group_hold_until > time.monotonic() + 9000     # ... and the next copies wait for it


async def test_g9_a_flood_wait_of_the_friends_own_message_is_waited_out_not_dropped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(notifier_mod, "FLOOD_MARGIN_SEC", 0.0)
    w = await world(tmp_path, on=False, bind=False)
    w.bot.fail_once_for[FRIEND] = [errors.FloodWaitError(request=None, capture=1)]
    await w.n.notify("tickmill bağlandı")
    await w.settle()
    assert w.friend_texts() == ["ℹ️ tickmill bağlandı"] and w.owner_texts() == ["ℹ️ tickmill bağlandı"]


async def test_g10_a_send_without_an_answer_is_never_cancelled_nor_sent_twice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(notifier_mod, "SEND_TIMEOUT_SEC", 0.2)
    w = await world(tmp_path)
    w.bot.gate = asyncio.Event()                                # Telegram does not answer for the groups
    await w.deliver(Msg(id=1101, message="bir", reply_to=in_topic(SOHBET)),
                    Msg(id=1102, message="iki", reply_to=in_topic(SOHBET)))
    await w.settle()
    assert w.sohbet() == [] and w.mirror.failed == 2
    pending = [t for t in w.n._send_tasks if not t.done()]
    assert len(pending) == 2 and not any(t.cancelled() for t in pending)     # still with Telethon, never cancelled
    assert w.owner_texts() == ["⚠️ Gruptan bir mesaj arkadaşınızın sohbet grubuna (<b>Sohbet Grubu</b>) kopyalanamadı: "
                               "Telegram yanıt vermedi. (Bu uyarı en fazla saatte bir gönderilir.)"]
    w.bot.gate.set()                                            # the connection is back: Telethon delivers them
    await asyncio.sleep(0.05)
    assert [body(t) for t in w.sohbet()] == ["bir", "iki"]     # once each, never sent again


async def test_g11_the_friends_own_messages_go_first(tmp_path: Path) -> None:
    w = await world(tmp_path)
    w.bot.send_delay = 0.05
    await w.n.notify("tickmill bağlandı")
    await w.deliver(Msg(id=1201, message="grup mesajı", reply_to=in_topic(SOHBET)))
    await w.n.notify("ikinci bildirim")
    await w.settle()
    order = [m.text for m in w.bot.sent if m.entity in (FRIEND, SOHBET_G)]
    assert order == ["ℹ️ tickmill bağlandı", "ℹ️ ikinci bildirim", "💬 <b>Sohbet</b> · Kadir\ngrup mesajı"]


# =========================================================================== #
# Right before every request: still on, still the bound group (correctness review r1, r2)
# =========================================================================== #
@pytest.mark.parametrize("change", ["sil", "replace", "kapat", "rebind"])
async def test_r1_r2_a_copy_waiting_for_its_turn_is_not_sent_once_the_owner_changed_something(
    tmp_path: Path, change: str
) -> None:
    w = await world(tmp_path)
    w.n._group_hold_until = time.monotonic() + 1.0              # an earlier copy's FloodWait still runs
    await w.deliver(Msg(id=2001, message="eski hedef görmemeli", reply_to=in_topic(SOHBET)))
    await asyncio.sleep(0.3)
    assert w.sohbet() == []
    if change == "sil":
        await w.owner_says("/arkadas sil")
    elif change == "replace":
        await w.owner_says(f"/arkadas {NEW_FRIEND}")
    elif change == "kapat":
        await w.owner_says("/grup kapat")
    else:
        await w.in_group("/bagla@kopya_bot sohbet", chat_id=-1009000000077, title="Yeni Sohbet")
    await w.settle()
    assert not any("eski hedef" in (m.text or "") for m in w.bot.sent)  # nothing went out, to any chat
    if change == "kapat":
        assert w.friend_texts()[-1].startswith("👥 Bot sahibi grup kopyasını kapattı")


async def test_r1_the_second_part_of_a_long_copy_is_not_sent_after_the_friend_was_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(notifier_mod, "GROUP_GAP_SEC", 0.5)
    w = await world(tmp_path)
    long_text = "ilk parca " * 450 + "IKINCI_PARCA_SONU"           # > 4000 chars: two messages
    await w.deliver(Msg(id=2101, message=long_text, reply_to=in_topic(SOHBET)))
    for _ in range(200):
        if w.sohbet():
            break
        await asyncio.sleep(0.01)
    assert len(w.sohbet()) == 1
    await w.owner_says("/arkadas sil")
    await w.settle()
    assert len(w.sohbet()) == 1 and not any("IKINCI_PARCA_SONU" in (m.text or "") for m in w.bot.sent)


# =========================================================================== #
# Edits, topic filter, catch-up, restart
# =========================================================================== #
async def test_g12_edits(tmp_path: Path) -> None:
    w = await world(tmp_path)
    await w.deliver(Msg(id=1301, message="ilk hali", reply_to=in_topic(SOHBET), sender=ANIL, sender_id=OTHER))
    await w.settle()
    (first,) = w.bot.to(SOHBET_G)
    await w.edit(Msg(id=1301, message="ilk hali", reply_to=in_topic(SOHBET), sender=ANIL, sender_id=OTHER))  # reaction
    await w.edit(Msg(id=1301, message="düzeltilmiş hali", reply_to=in_topic(SOHBET), sender=ANIL, sender_id=OTHER))
    await w.settle()
    assert w.sohbet()[1:] == ["✏️ <b>Sohbet</b> · Anıl K. (düzenledi)\ndüzeltilmiş hali"]
    assert w.bot.to(SOHBET_G)[1].reply_to == first.id

    held = asyncio.get_running_loop().create_future()           # edited while its copy still waits: one copy, new text
    w.mirror.offer(Msg(id=1302, message="taslak", reply_to=in_topic(SOHBET)), handled=held)
    await asyncio.sleep(0.02)
    w.mirror.offer(Msg(id=1303, message="sonraki", reply_to=in_topic(SOHBET)))
    w.mirror.offer(Msg(id=1303, message="sonraki (düzeltildi)", reply_to=in_topic(SOHBET)), edited=True)
    w.mirror.offer(Msg(id=1302, message="taslak (bekliyorken)", reply_to=in_topic(SOHBET)), edited=True)
    held.set_result(None)
    await w.settle()
    assert w.sohbet()[2:] == ["💬 <b>Sohbet</b> · Kadir\ntaslak (bekliyorken)",
                              "💬 <b>Sohbet</b> · Kadir\nsonraki (düzeltildi)"]

    await w.edit(Msg(id=999, message="hiç kopyalanmamış", reply_to=in_topic(SOHBET)))      # never copied: nothing
    await w.in_group("/bagla@kopya_bot sohbet", chat_id=-1009000000088, title="Yeni")      # rebound: no old edits
    await w.edit(Msg(id=1301, message="yine düzeltildi", reply_to=in_topic(SOHBET), sender=ANIL, sender_id=OTHER))
    await w.settle()
    assert len(w.sohbet()) == 4 and [m for m in w.bot.to(-1009000000088) if "düzeltildi" in m.text] == []


async def test_g13_topics_can_be_limited(tmp_path: Path) -> None:
    w = await world(tmp_path)
    await w.owner_says("/grup konular pnl, SOHBET")
    w.bot.sent.clear()
    await w.deliver(Msg(id=1401, message="selam", reply_to=in_topic(SOHBET)),
                    Msg(id=1402, message="+3R", reply_to=in_topic(PNL)),
                    Msg(id=1403, message="Eth short 1R", reply_to=in_topic(CRYPTO)),
                    Msg(id=1404, message="genel", sender_id=OTHER))
    await w.settle()
    assert w.sohbet() == ["💬 <b>Sohbet</b> · Kadir\nselam"] and w.ana() == ["💬 <b>PnL ve Sertifika</b> · Kadir\n+3R"]
    assert w.rec.calls == [(CHAT, TRADER, CRYPTO)]               # the signal is handled as always


async def test_g14_catch_up_finds_are_copied_once_and_old_ones_not(tmp_path: Path) -> None:
    w = await world(tmp_path)
    now = utcnow()
    live = Msg(id=1501, message="canlı geldi", reply_to=in_topic(SOHBET))
    missed = Msg(id=1502, message="kaçırılan", reply_to=in_topic(SOHBET), date=now - timedelta(minutes=5))
    old = Msg(id=1503, message="çok eski", reply_to=in_topic(SOHBET), date=now - timedelta(hours=2))
    w.tg.history = [live, missed, old]
    await w.deliver(live)
    await w.settle()
    w.db.state[f"last_msg_id:{CHAT}"] = 1500                    # the catch-up finds the live one again, too
    await w.listener._catch_up_chat(CHAT, w.listener._entities[CHAT])
    await w.settle()
    assert w.sohbet() == ["💬 <b>Sohbet</b> · Kadir\ncanlı geldi",
                          "💬 <b>Sohbet</b> · Kadir · <i>5 dakika önce yazıldı</i>\nkaçırılan"]


async def test_r8_a_restart_never_copies_a_message_twice(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gm, "HANDLED_WAIT_SEC", 0.2)
    monkeypatch.setattr(gm, "PAUSE_MAX_SEC", 0.2)
    monkeypatch.setattr(gm, "LAST_WRITE_WAIT_SEC", 0.1)
    w = await world(tmp_path / "a")
    slow = Msg(id=3000, message="BTC long 1R", photo=SimpleNamespace(id=1), payload=b"c", reply_to=in_topic(CRYPTO),
               download_delay=30.0)                               # the listener is still downloading its picture
    chat = Msg(id=3001, message="sohbet mesaji", reply_to=in_topic(SOHBET))
    await w.deliver(slow, chat)
    for _ in range(300):
        if any("sohbet mesaji" in t for t in w.sohbet()):
            break
        await asyncio.sleep(0.01)
    assert any("sohbet mesaji" in t for t in w.sohbet())
    assert w.db.state[f"last_msg_id:{CHAT}"] < 3001              # the listener will fetch it again after a restart
    # the process dies here (power loss / kill); a new process starts on the same database
    w2 = await world(tmp_path / "b", db=w.db, bind=False)
    w2.tg.history = [slow, chat]
    slow.download_delay = 0.0
    await w2.listener._catch_up_chat(CHAT, w2.listener._entities[CHAT])
    await w2.settle()
    assert w2.bot.sent == [] and w2.mirror.targets["sohbet"].chat == SOHBET_G      # (bindings kept: same friend)
    for t in list(w.listener._tasks):
        t.cancel()


# =========================================================================== #
# Wiring
# =========================================================================== #
async def test_g15_the_app_wires_loads_and_stops_the_copy(tmp_path: Path) -> None:
    from test_app_cli import FakeListener, write_config

    from copytrader.app import build_app, serve
    from copytrader.config import load_config
    from copytrader.storage.db import Database

    from fakes import FakeBroker, FakeNotifier, FakeParser

    path = str(tmp_path / "bot.db")
    db = Database(path)
    await db.open()
    await db.set_state("group_mirror", {"on": True, "topics": ["PnL"], "friend": FRIEND,
                                        "targets": {"ana": {"chat": ANA_G, "title": "Ana", "by": FRIEND}}})
    await db.close()
    order: list[str] = []

    class Listener(FakeListener):
        def set_group_mirror(self, mirror: Any) -> None:
            self.mirror = mirror

        async def stop(self) -> None:
            order.append("listener.stop")
            await super().stop()

    class Notifier(FakeNotifier):
        friend_chat_id = FRIEND

        def set_group_mirror(self, mirror: Any) -> None:
            self.mirror = mirror

    app = build_app(load_config(write_config(tmp_path)), db=Database(path), parser=FakeParser(), notifier=Notifier(),
                    broker_factory=lambda cfg: FakeBroker(cfg.name), listener_factory=Listener)
    assert isinstance(app.mirror, GroupMirror) and app.listener.mirror is app.mirror and app.notifier.mirror is app.mirror
    assert app.mirror._engine is app.engine
    real_stop = app.mirror.stop

    async def stop() -> None:
        order.append("mirror.stop")
        await real_stop()

    app.mirror.stop = stop                                       # type: ignore[method-assign]
    stop_event = asyncio.Event()
    task = asyncio.ensure_future(serve(app, stop_event))
    await asyncio.wait_for(app.listener.started.wait(), 5)
    assert app.mirror.enabled and app.mirror.topics == ["PnL"] and app.mirror.active
    assert app.mirror.targets["ana"].chat == ANA_G
    stop_event.set()
    await asyncio.wait_for(task, 10)
    assert order == ["mirror.stop", "listener.stop"] and not app.mirror.active

    (tmp_path / "p").mkdir()
    plain = build_app(load_config(write_config(tmp_path / "p")), db=Database(":memory:"), parser=FakeParser(),
                      notifier=FakeNotifier(), broker_factory=lambda cfg: FakeBroker(cfg.name),
                      listener_factory=FakeListener)
    assert plain.mirror is None                                  # parts without the copy: wired as before


async def test_g16_the_listener_never_talks_to_the_group_and_offer_never_raises(tmp_path: Path) -> None:
    w = await world(tmp_path)

    class Broken:
        chat_id = CHAT

        @property
        def id(self) -> int:
            raise RuntimeError("bozuk mesaj")

    w.mirror.offer(Broken())                                    # swallowed (logged), the listener never sees it
    photo = Msg(id=1600, message="resim", photo=SimpleNamespace(id=5), reply_to=in_topic(SOHBET))
    await w.deliver(photo)                                       # (its download goes through the message, never the
    await w.settle()                                             #  userbot client: ForumClient fails on any other call)
    assert w.mirror.failed == 0 and photo.downloads == 1 and len(w.bot.files) == 1
    w.listener.set_group_mirror(SimpleNamespace(offer=lambda *a, **k: 1 / 0))
    m = Msg(id=1601, message="Eth short 1R", reply_to=in_topic(CRYPTO))
    await w.deliver(m)
    await w.settle()
    assert w.rec.calls == [(CHAT, TRADER, CRYPTO)] and (CHAT, 1601) in w.db.messages
    assert isinstance(w.tg, ForumClient) and w.tg.requests == [r for r in w.tg.requests if
                                                               type(r).__name__.startswith("GetForumTopics")]
    assert listener_mod.TelegramListener.trading_busy(w.listener) is False


# =========================================================================== #
# Round-2 correctness review (grprev/correctness/test_repro_r2.py), now regression tests
# =========================================================================== #
async def test_n2_no_copy_reaches_the_old_friends_groups_while_the_engine_finishes_arkadas(tmp_path: Path) -> None:
    w = await world(tmp_path)

    async def set_friend_chat(chat: int | None) -> None:
        # as orchestrator.set_friend_chat: the notifier gets the new friend first, then the engine still awaits
        # (its log, the old friend's open catch-up questions re-sent to the owner) before /arkadas goes on
        w.n.set_friend(chat)
        await asyncio.sleep(0.5)

    w.n._engine.set_friend_chat = set_friend_chat
    w.n._group_hold_until = time.monotonic() + 0.4              # the copy's turn comes inside that window
    await w.deliver(Msg(id=2201, message="eski arkadaşın grubuna gitmemeli", reply_to=in_topic(SOHBET)))
    await asyncio.sleep(0.2)
    mark = len(w.bot.sent)
    await w.n.handle_message(MsgEvent(f"/arkadas {NEW_FRIEND}", sender_id=OWNER))
    await w.n.drain(5)
    await w.settle()
    assert w.n.friend_chat_id == NEW_FRIEND and w.mirror.targets == {}
    assert not any(m.entity in (ANA_G, SOHBET_G) for m in w.bot.sent[mark:])
    # a copy offered inside the window is not routed to the old groups either
    w2 = await world(tmp_path / "b")
    w2.n.set_friend(NEW_FRIEND)                                  # (the engine's first step, the rest still running)
    await w2.deliver(Msg(id=2202, message="pencerede", reply_to=in_topic(SOHBET)))
    await w2.settle()
    assert w2.bot.to(SOHBET_G) == [] and w2.mirror.skipped["sohbet"] == 1


async def test_n1_the_source_group_is_never_a_target_and_the_bots_own_posts_never_echo(tmp_path: Path) -> None:
    w = await world(tmp_path, bind=False)
    assert CHAT in w.listener.chat_ids
    await w.in_group("/bagla@kopya_bot ana", chat_id=CHAT, title="KLAN KASIM", sender_id=OWNER)
    assert w.mirror.targets == {} and w.bot.to(CHAT) == []        # refused, and nothing said in the source group
    assert w.owner_texts() == ["❌ <b>KLAN KASIM</b> kaynak grubun kendisi; grup kopyası oraya bağlanamaz. Arkadaşınızın "
                               "kendi gruplarından birinde yazılmalı."]
    with pytest.raises(ValueError):
        await w.mirror.bind("ana", CHAT, "KLAN KASIM", OWNER)
    await w.mirror.bind("ana", ANA_G, "Ana Grup", FRIEND)
    w.bot.sent.clear()
    echo = Msg(id=2602, message="💬 General · kopya_bot", sender_id=BOT_ID,
               sender=SimpleNamespace(first_name="kopya_bot", last_name=None, username=BOT_NAME))
    await w.deliver(echo)                                        # the bot's own post, if it ever is in a source chat
    await w.settle()
    assert w.bot.sent == [] and w.mirror.waiting == 0
    # a target that is a source chat (stored by hand / earlier) is never sent to
    w.mirror._targets["ana"] = gm.Target("ana", CHAT, "KLAN KASIM", OWNER, since=0.0)
    await w.deliver(Msg(id=2603, message="Eth short 1R", reply_to=in_topic(CRYPTO)))
    await w.settle()
    assert w.bot.to(CHAT) == [] and w.mirror.skipped["ana"] == 1


async def test_n4_a_group_flood_wait_never_delays_the_friends_own_messages_nor_his_question(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    w = await world(tmp_path)
    w.n._group_hold_until = time.monotonic() + 3600             # a group got a long FloodWait (group copies held)
    t0 = time.monotonic()
    for i in range(4):
        await w.n.notify(f"bildirim {i}")
    await w.settle(10)
    assert w.friend_texts() == [f"ℹ️ bildirim {i}" for i in range(4)] and time.monotonic() - t0 < 0.5
    from test_friend_mirror import _open_conf

    conf = _open_conf("q1")
    conf.kind, conf.catch_up = "catch_up", {"decider": FRIEND, "broker": "tickmill"}
    await w.n.ask_confirmation(conf)                             # the tickmill question the friend answers
    await w.settle(10)
    assert [m for m in w.bot.to(FRIEND) if m.buttons] and time.monotonic() - t0 < 1.0


async def test_n3_a_rebind_while_a_copy_is_on_its_way_never_sends_its_edit_to_the_new_group(tmp_path: Path) -> None:
    w = await world(tmp_path)
    w.bot.gate = asyncio.Event()                                 # the copy to SOHBET_G is on its way (no answer yet)
    await w.deliver(Msg(id=2501, message="ilk hali", reply_to=in_topic(SOHBET)))
    await asyncio.sleep(0.2)
    other = -1009000000088
    await w.n.handle_message(GroupEvent("/bagla@kopya_bot sohbet", chat_id=other, title="Diğer Sohbet"))
    await asyncio.sleep(0.05)
    w.bot.gate.set()                                             # Telegram answers: the copy reached SOHBET_G
    await w.settle()
    assert [m.text for m in w.bot.to(SOHBET_G)] == ["💬 <b>Sohbet</b> · Kadir\nilk hali"]
    mark = len(w.bot.sent)
    await w.edit(Msg(id=2501, message="ikinci hali", reply_to=in_topic(SOHBET)))
    await w.settle()
    assert [m for m in w.bot.sent[mark:] if "ikinci hali" in (m.text or "")] == []


async def test_n5_the_copy_on_its_way_at_shutdown_is_counted_and_never_copied_twice(tmp_path: Path) -> None:
    w = await world(tmp_path)
    w.n._group_hold_until = time.monotonic() + 30               # its turn has not come yet
    await w.deliver(Msg(id=2701, message="yolda", reply_to=in_topic(SOHBET)))
    await asyncio.sleep(0.2)
    for i in range(3):
        w.mirror.offer(Msg(id=2702 + i, message=f"sırada {i}", reply_to=in_topic(SOHBET)))
    await w.mirror.stop()
    assert w.mirror.dropped == 4 and w.mirror.failed == 0 and w.sohbet() == []
    assert w.db.state["group_mirror_last"] == {str(CHAT): 2701}  # marked before sending: never copied twice
    w2 = await world(tmp_path / "b", db=w.db, bind=False)
    w2.mirror.offer(Msg(id=2701, message="yolda", reply_to=in_topic(SOHBET)), catchup=True)
    await w2.settle()
    assert w2.bot.sent == []


# =========================================================================== #
# Round-2 isolation review
# =========================================================================== #
async def test_n2i_the_last_copied_id_is_stored_before_the_send_but_not_while_trading_is_busy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    w = await world(tmp_path)
    writes: list[tuple[float, Any]] = []
    real = w.db.set_state

    async def set_state(key: str, value: Any) -> None:
        if key == "group_mirror_last":
            writes.append((time.monotonic(), dict(value)))
        await real(key, value)

    w.db.set_state = set_state                                   # type: ignore[method-assign]
    gate = asyncio.Event()
    w.n._spawn(gate.wait())                                      # an owner tap placing orders
    await w.deliver(Msg(id=3102, message="meşgulken", reply_to=in_topic(SOHBET)))
    await asyncio.sleep(0.3)
    assert writes == [] and w.sohbet() == []                     # neither the write nor the copy while busy
    released = time.monotonic()
    gate.set()
    await w.settle()
    assert [v for _, v in writes] == [{str(CHAT): 3102}] and writes[0][0] >= released
    assert w.bot.to(SOHBET_G)[0].text.endswith("meşgulken")      # (stored first, then sent)
    monkeypatch.setattr(gm, "LAST_WRITE_WAIT_SEC", 0.1)          # a long busy spell: written after the limit
    gate2 = asyncio.Event()
    w.n._spawn(gate2.wait())
    await w.deliver(Msg(id=3103, message="uzun meşgul", reply_to=in_topic(SOHBET)))
    await asyncio.sleep(0.4)
    assert [v for _, v in writes][-1] == {str(CHAT): 3103} and w.sohbet()[-1].endswith("uzun meşgul")
    gate2.set()
    await w.settle()


async def test_n3i_an_open_question_goes_before_an_ordinary_message_waiting_out_a_flood(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(notifier_mod, "FLOOD_MARGIN_SEC", 0.0)
    w = await world(tmp_path, on=False, bind=False)
    from test_friend_mirror import _open_conf

    w.bot.fail_once_for[FRIEND] = [errors.FloodWaitError(request=None, capture=2)]
    await w.n.notify("sıradan bildirim")                        # its send gets a FloodWait of 2 s ...
    conf = _open_conf("q2")
    conf.kind, conf.catch_up = "catch_up", {"decider": FRIEND, "broker": "tickmill"}
    await w.n.ask_confirmation(conf)                             # ... and the tickmill question comes behind it
    t0 = time.monotonic()
    await w.settle(10)
    sent = w.bot.to(FRIEND)
    assert sent[0].buttons                                       # the question first, at once
    assert [m.text for m in sent[1:]] == ["ℹ️ sıradan bildirim"]  # then the notice, sent once more (once)
    assert time.monotonic() - t0 < 1.0


async def test_r3_a_flood_wait_on_the_second_part_of_a_long_friend_message_never_repeats_the_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(notifier_mod, "FLOOD_MARGIN_SEC", 0.0)
    w = await world(tmp_path, on=False, bind=False)
    real = w.bot.send_message
    calls = {"n": 0}

    async def send_message(entity: Any, message: Any, **kw: Any) -> Any:
        if entity == FRIEND:
            calls["n"] += 1
            if calls["n"] == 2:                                   # the second part gets a FloodWait
                raise errors.FloodWaitError(request=None, capture=1)
        return await real(entity, message, **kw)

    w.bot.send_message = send_message                            # type: ignore[method-assign]
    await w.n.notify("BIRINCI_PARCA " + "x " * 2500 + " IKINCI_PARCA")
    await w.settle(10)
    texts = w.friend_texts()
    assert len([t for t in texts if "BIRINCI_PARCA" in t]) == 1 and len([t for t in texts if "IKINCI_PARCA" in t]) == 1
    assert len(texts) == 2
