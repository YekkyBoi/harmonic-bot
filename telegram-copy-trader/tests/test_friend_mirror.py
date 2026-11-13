"""The friend who owns the tickmill account (owner's choice: "Her şey"; the friend answers the tickmill catch-up
question).

  /arkadas <chat_id>   the owner stores the friend's chat in the database; the bot sends the friend a test message and
                       says whether it arrived (a bot cannot message a user who never pressed Start: then nothing is
                       stored); /arkadas shows it, /arkadas sil removes it. Owner only.
  mirror               every message the owner gets also goes to the friend as read-only text; a message with buttons
                       for the owner goes without them, with one line saying that the owner decides. Never delays,
                       blocks or fails anything for the owner; a failing friend chat is logged and told to the owner at
                       most once an hour.
  catch-up question    goes to the friend WITH ✅ Evet / ❌ Hayır; the owner gets the same text without buttons and who
                       decides; the answer and the result reach both. The friend's button presses count for catch-up
                       questions only; his commands and messages are never executed.
  no friend            exactly as before.

REAL Engine and REAL BotNotifier; only Telegram is fake (the client of tests/test_notifier.py, per chat).
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from telethon import errors
from test_notifier import CbEvent, FakeBotClient, MsgEvent

from copytrader.brokers.base import BrokerUnavailableError
from copytrader.config import AppConfig, NotifierConfig, TelegramConfig
from copytrader.engine.orchestrator import Engine
from copytrader.models import (
    ActionType,
    EntryType,
    ParsedAction,
    PendingConfirmation,
    Severity,
    Side,
    TakeProfit,
    TradePlan,
)
from copytrader.storage.db import Database
from copytrader.telegram import notifier as notifier_mod
from copytrader.telegram.notifier import BotNotifier

from fakes import FakeBroker, FakePriceOracle

OWNER, FRIEND = 777, 555
CHAT = -1001234567890


class Client(FakeBotClient):
    """Telegram, per chat: ``fail_for`` raises for that chat, ``hang_for`` never answers it."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_for: dict[int, Exception] = {}
        self.fail_once_for: dict[int, list[Exception]] = {}
        self.hang_for: set[int] = set()

    async def send_message(self, entity: Any, message: Any, **kw: Any) -> Any:
        if entity in self.hang_for:
            await asyncio.sleep(3600)
        if entity in self.fail_for:
            raise self.fail_for[entity]
        if self.fail_once_for.get(entity):
            raise self.fail_once_for[entity].pop(0)
        return await super().send_message(entity, message, **kw)

    async def edit_message(self, entity: Any, message: Any, text: Any, **kw: Any) -> Any:
        if entity in self.hang_for:
            await asyncio.sleep(3600)
        if entity in self.fail_for:
            raise self.fail_for[entity]
        return await super().edit_message(entity, message, text, **kw)

    def disconnect(self) -> None:
        return None

    def to(self, chat: int) -> list[Any]:
        return [m for m in self.sent if m.entity == chat]

    def edits_of(self, chat: int) -> list[Any]:
        return [e for e in self.edits if e.entity == chat]


def _cfg() -> AppConfig:
    c = AppConfig(telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[CHAT]),
                  notifier=NotifierConfig(bot_token="123:abc", owner_user_id=OWNER))
    c.monitor_interval_sec = 3600.0
    return c


def _plan(names: list[str]) -> TradePlan:
    return TradePlan(plan_id="p1", base_asset="ETH", side=Side.LONG, entry_type=EntryType.MARKET, entry_price=2232.85,
                     stop_loss=2200.0, take_profits=[TakeProfit(price=2300.0, close_fraction=1.0)], risk_r=1.0,
                     risk_pct=2.0, source_chat_id=CHAT, source_message_ids=[1], created_at=datetime.now(UTC),
                     broker_names=names)


def _open_conf(conf_id: str = "c1") -> PendingConfirmation:
    now = datetime.now(UTC)
    action = ParsedAction(action=ActionType.OPEN, base_asset="ETH", quote_asset="USDT", side=Side.LONG,
                          entry_price=2232.18, stop_loss=2205.11, take_profits=[TakeProfit(price=2310.6)], risk_r=1.0,
                          confidence=0.7)
    plan = TradePlan(plan_id="p", base_asset="ETH", quote_asset="USDT", side=Side.LONG, entry_type=EntryType.MARKET,
                     entry_price=2232.18, stop_loss=2205.11, take_profits=[TakeProfit(price=2310.6)], risk_r=1.0,
                     risk_pct=2.0, source_chat_id=CHAT, source_message_ids=[5], created_at=now)
    return PendingConfirmation(id=conf_id, kind="open", action=action, plan=plan, source_chat_id=CHAT,
                               source_message_ids=[5], created_at=now, expires_at=now + timedelta(minutes=10))


class World:
    """Engine + notifier on one database; tickmill is an optional FakeBroker."""

    def __init__(self, tmp_path: Path, path: str | None = None) -> None:
        self.tmp_path = tmp_path
        self.path = path or str(tmp_path / "bot.db")

    async def up(self, *, tickmill_down: bool = False) -> "World":
        self.db = Database(self.path)
        await self.db.open()
        self.client = Client()
        self.n = BotNotifier(NotifierConfig(bot_token="123:abc", owner_user_id=OWNER),
                             TelegramConfig(api_id=1, api_hash="x", source_chats=[CHAT]), str(self.tmp_path))
        self.n._client = self.client
        fail = {"resolve_symbol": BrokerUnavailableError("bağlantı yok (test)")} if tickmill_down else None
        self.tm = FakeBroker("tickmill", optional=True, fail=fail)
        self.engine = Engine(_cfg(), self.db, object(), [FakeBroker("binance"), self.tm], self.n,
                             price_oracle=FakePriceOracle({"ETH": 2232.85}))
        self.n._engine = self.engine
        await self.engine.start(monitor=False)
        await self.settle()
        self.client.sent.clear()
        self.client.edits.clear()
        return self

    async def down(self) -> None:
        await self.engine.stop()
        await self.n.stop()
        await self.db.close()

    async def settle(self, timeout: float = 5.0, *, friend: bool = True) -> None:
        """``friend=False``: the friend's chat is not waited for (a question to him is being retried)."""
        loop = asyncio.get_running_loop()
        end = loop.time() + timeout
        while True:
            busy = [t for t in (*self.engine._tasks, *self.engine._critical, *self.n._tasks)
                    if not t.done() and not t.get_name().startswith(("expire-", "retry-"))]
            pump = getattr(self.n, "_friend_pump", None)
            if friend and pump is not None and not pump.done():
                busy.append(pump)
            if not busy:
                return
            assert loop.time() < end, busy
            await asyncio.sleep(0.01)

    async def owner_says(self, text: str) -> None:
        await self.n.handle_message(MsgEvent(text, sender_id=OWNER))
        await self.settle()

    async def ask_catch_up(self, *, friend: bool = True) -> PendingConfirmation:
        """tickmill misses a signal (not connected), comes back: the catch-up question (the newest one)."""
        before = {c.id for c in await self.engine._catch_up_pending()}
        self.tm.fail = {"resolve_symbol": BrokerUnavailableError("bağlantı yok (test)")}
        await self.engine.execute_open(_plan(["binance", "tickmill"]))
        self.tm.fail = {}
        self.engine.monitor.heartbeat.on_healthy("tickmill")
        await self.settle(friend=friend)
        (conf,) = [c for c in await self.engine._catch_up_pending() if c.id not in before]
        return conf


@pytest.fixture
async def world(tmp_path: Path):
    w = await World(tmp_path).up()
    yield w
    await w.down()


def buttons(msg: Any) -> list[tuple[str, bytes]]:
    return [(b.text, b.type.data) for row in (msg.buttons or []) for b in row]


# =========================================================================== #
async def test_f1_without_a_friend_everything_goes_to_the_owner_as_before(world: World) -> None:
    await world.n.notify("bir bildirim", Severity.WARNING)
    conf = await world.ask_catch_up()
    assert {m.entity for m in world.client.sent} == {OWNER}
    (question,) = [m for m in world.client.to(OWNER) if "açılsın mı?" in m.text]
    assert buttons(question) == [("✅ Evet", f"ok:{conf.id}".encode()), ("❌ Hayır", f"no:{conf.id}".encode())]
    assert question.text.startswith("❓ <b>#1 ETH LONG tickmill hesabında da açılsın mı?</b>\n"
                                    "✅ Evet = tickmill hesabında aç · ❌ Hayır = açma\n")
    await world.n.handle_callback(CbEvent(f"ok:{conf.id}".encode(), sender_id=OWNER, message_id=question.id))
    await world.settle()
    assert len(world.tm.opened) == 1
    assert await world.db.get_state("friend_chat_id") is None


async def test_f2_arkadas_stores_the_chat_after_a_test_message_reaches_it(tmp_path: Path) -> None:
    world = await World(tmp_path).up()
    try:
        await world.owner_says("/arkadas")
        assert world.client.to(OWNER)[-1].text == (
            "👤 Arkadaş sohbeti ayarlı değil. Eklemek için: <code>/arkadas &lt;sohbet_id&gt;</code> (arkadaşınız önce "
            "botu Telegram'da açıp Start'a basmalı).")
        await world.owner_says(f"/arkadas {FRIEND}")
        (hello,) = world.client.to(FRIEND)
        assert hello.text.startswith("👋 Merhaba! Bu sohbet, kopya-trade botunun bildirimlerini almak için bot sahibi "
                                     "tarafından eklendi.")
        assert world.client.to(OWNER)[-1].text == (
            f"✅ Arkadaşınızın sohbeti kaydedildi (<code>{FRIEND}</code>); deneme mesajı ulaştı. Bundan sonra size "
            "gelen her bildirim ona da gider; tickmill 'sonradan açılsın mı?' sorularını o cevaplar.")
        assert await world.db.get_state("friend_chat_id") == FRIEND
        await world.owner_says("/arkadas")
        assert world.client.to(OWNER)[-1].text.startswith(f"👤 Arkadaşınızın sohbeti: <code>{FRIEND}</code>.")
        assert len(world.client.to(FRIEND)) == 1             # the owner's /arkadas replies are not mirrored
    finally:
        await world.down()

    again = await World(tmp_path, world.path).up()           # a restart keeps it
    try:
        await again.n.notify("yeniden başladı")
        await again.settle()
        assert [m.text for m in again.client.to(FRIEND)] == ["ℹ️ yeniden başladı"]
        await again.owner_says("/arkadas sil")
        assert again.client.to(OWNER)[-1].text == ("✅ Arkadaş sohbeti kaldırıldı; bildirimler artık yalnızca size "
                                                   "gelir, tickmill sorularını siz cevaplarsınız.")
        assert again.client.to(FRIEND)[-1].text == ("ℹ️ Bot sahibi bu sohbeti bildirim listesinden çıkardı; buraya "
                                                    "artık mesaj gelmeyecek.")
        assert await again.db.get_state("friend_chat_id") is None
        sent = len(again.client.to(FRIEND))
        await again.n.notify("sonra")
        await again.settle()
        assert len(again.client.to(FRIEND)) == sent
    finally:
        await again.down()


async def test_f3_arkadas_refuses_wrong_ids_and_explains_start(world: World) -> None:
    await world.owner_says("/arkadas abc")
    assert world.client.to(OWNER)[-1].text == (
        "Kullanım: <code>/arkadas &lt;sohbet_id&gt;</code> (arkadaşınızın kullanıcı kimliği: pozitif bir sayı; grup "
        "ya da kanal olmaz), <code>/arkadas</code> ya da <code>/arkadas sil</code>")
    await world.owner_says(f"/arkadas {OWNER}")
    assert world.client.to(OWNER)[-1].text == "Bu sizin kendi kimliğiniz; arkadaşınızın sohbet kimliğini yazın."
    world.client.fail_for[FRIEND] = errors.UserIsBlockedError(request=None)
    await world.owner_says(f"/arkadas {FRIEND}")
    assert world.client.to(OWNER)[-1].text == (
        f"❌ Deneme mesajı <code>{FRIEND}</code> sohbetine ulaştırılamadı; kaydedilmedi. Bir bot, kendisine hiç "
        "yazmamış (botu Telegram'da açıp Start'a basmamış) ya da botu engellemiş bir kullanıcıya mesaj gönderemez. "
        f"Arkadaşınız botu açıp Start'a bassın, sonra <code>/arkadas {FRIEND}</code> komutunu tekrar gönderin.")
    assert await world.db.get_state("friend_chat_id") is None


async def test_f4_every_owner_message_is_mirrored_and_buttons_stay_with_the_owner(world: World) -> None:
    await world.owner_says(f"/arkadas {FRIEND}")
    world.client.sent.clear()
    await world.n.notify("tickmill bağlandı", Severity.INFO)
    await world.settle()
    assert [m.text for m in world.client.to(OWNER)] == ["ℹ️ tickmill bağlandı"]
    assert [m.text for m in world.client.to(FRIEND)] == ["ℹ️ tickmill bağlandı"]

    conf = _open_conf()
    conf.bot_message_id = await world.n.ask_confirmation(conf)
    await world.settle()
    (asked,) = [m for m in world.client.to(OWNER) if m.buttons]
    assert [t for t, _ in buttons(asked)] == ["✅ Onayla", "✏️ Düzelt", "❌ İptal"]
    mirrored = world.client.to(FRIEND)[-1]
    assert mirrored.buttons is None
    assert mirrored.text == asked.text + "\n\n👤 Bu mesajdaki seçimi bot sahibi yapar (düğmeler yalnızca onda)."
    await world.n.close_confirmation(conf, "✅ İşlem açıldı")
    await world.settle()
    assert world.client.edits_of(OWNER)[-1].text.endswith("<b>Sonuç:</b> ✅ İşlem açıldı")
    outcome = world.client.to(FRIEND)[-1]
    assert outcome.text.endswith("<b>Sonuç:</b> ✅ İşlem açıldı") and "ETH" in outcome.text and outcome.buttons is None
    assert world.client.edits_of(FRIEND) == []

    await world.owner_says("/yardim")                        # replies to the owner's own commands too ("Her şey")
    assert world.client.to(FRIEND)[-1].text == world.client.to(OWNER)[-1].text
    assert "/arkadas" in world.client.to(OWNER)[-1].text


async def test_f5_the_friend_answers_the_tickmill_question_and_both_see_the_result(world: World) -> None:
    await world.owner_says(f"/arkadas {FRIEND}")
    world.client.sent.clear()
    conf = await world.ask_catch_up()
    (question,) = [m for m in world.client.to(FRIEND) if m.buttons]
    assert buttons(question) == [("✅ Evet", f"ok:{conf.id}".encode()), ("❌ Hayır", f"no:{conf.id}".encode())]
    assert question.text.startswith("❓ <b>#1 ETH LONG tickmill hesabında da açılsın mı?</b>\n"
                                    "✅ Evet = tickmill hesabında aç · ❌ Hayır = açma\n")
    (copy,) = [m for m in world.client.to(OWNER) if "açılsın mı?" in m.text]
    assert copy.buttons is None and "✅ Evet = tickmill hesabında aç" not in copy.text
    assert ("👤 Bu soruyu tickmill hesabının sahibi (arkadaşınız) cevaplayacak; düğmeler onda. Size bilgi için "
            "gönderildi.") in copy.text
    assert world.tm.opened == []

    await world.n.handle_callback(CbEvent(f"ok:{conf.id}".encode(), sender_id=FRIEND, message_id=question.id))
    await world.settle()
    assert len(world.tm.opened) == 1                         # the friend's Evet opened it
    answer = ("✅ Evet (yanıtlayan: tickmill hesabının sahibi): tickmill hesabına emir gönderiliyor (işlemin şu anki "
              "stop ve hedefleriyle); sonuç ayrıca bildirilecek.")
    owner_edit = [e for e in world.client.edits_of(OWNER) if e.msg_id == copy.id][-1]
    assert owner_edit.text.endswith(f"<b>Sonuç:</b> {answer}") and owner_edit.buttons is None
    friend_edit = [e for e in world.client.edits_of(FRIEND) if e.msg_id == question.id][-1]
    assert friend_edit.text.endswith(f"<b>Sonuç:</b> {answer}") and friend_edit.buttons is None
    for chat in (OWNER, FRIEND):                             # the open's own result reaches both
        assert any("tickmill hesabında sonradan açıldı (tickmill hesabının sahibinin onayıyla)" in m.text
                   for m in world.client.to(chat)), chat
    # once only
    await world.n.handle_callback(CbEvent(f"ok:{conf.id}".encode(), sender_id=FRIEND, message_id=question.id))
    await world.settle()
    assert len(world.tm.opened) == 1


async def test_f6_the_friend_can_answer_nothing_else_and_run_no_command(world: World) -> None:
    await world.owner_says(f"/arkadas {FRIEND}")
    conf = _open_conf("c9")
    await world.db.save_confirmation(conf)
    world.client.sent.clear()
    for data in (b"fix:c9", b"closeall:yes"):                # not his buttons at all
        ev = CbEvent(data, sender_id=FRIEND)
        await world.n.handle_callback(ev)
        assert ev.answers == [("Bu düğme yalnızca bot sahibi içindir.", True)], (data, ev.answers)
    for data in (b"ok:c9", b"no:c9"):                         # an owner's question (not a catch-up one): the engine says no
        await world.n.handle_callback(CbEvent(data, sender_id=FRIEND))
        await world.settle()
    assert [m.text for m in world.client.to(FRIEND)] == ["Bu soruyu yalnızca bot sahibi cevaplayabilir."] * 2
    assert (await world.db.get_confirmation("c9")).status.value == "pending"
    world.client.sent.clear()
    for text in ("/durum", "/hepsini_kapat", "/arkadas sil", "merhaba"):
        await world.n.handle_message(MsgEvent(text, sender_id=FRIEND))
        await world.settle()
    assert world.client.to(OWNER) == []                      # nothing ran for the owner
    assert {m.text for m in world.client.to(FRIEND)} == {"Bu bot komutlarını yalnızca sahibi kullanabilir."}
    assert await world.db.get_state("friend_chat_id") == FRIEND


async def test_f7_a_dead_friend_chat_never_holds_the_owner_and_is_told_once_an_hour(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(notifier_mod, "SEND_TIMEOUT_SEC", 0.2)
    await world.owner_says(f"/arkadas {FRIEND}")
    world.client.sent.clear()
    world.client.hang_for.add(FRIEND)                        # Telegram never answers for the friend's chat
    t0 = time.monotonic()
    for i in range(3):
        await world.n.notify(f"bildirim {i}")
    assert time.monotonic() - t0 < 0.5
    assert [m.text for m in world.client.to(OWNER)] == ["ℹ️ bildirim 0", "ℹ️ bildirim 1", "ℹ️ bildirim 2"]
    await world.settle()
    alerts = [m.text for m in world.client.to(OWNER) if "Arkadaşınıza" in m.text]
    assert alerts == [f"⚠️ Arkadaşınıza (<code>{FRIEND}</code>) mesaj iletilemedi: Telegram yanıt vermedi. (Bu uyarı "
                      "en fazla saatte bir gönderilir.)"]
    world.client.hang_for.clear()
    world.client.fail_for[FRIEND] = errors.UserIsBlockedError(request=None)
    await world.n.notify("bir tane daha")
    await world.settle()
    assert len([m for m in world.client.to(OWNER) if "Arkadaşınıza" in m.text]) == 1      # not again within the hour
    assert world.client.to(FRIEND) == []


async def test_f8_a_second_press_of_the_friend_bothers_nobody_but_him(world: World) -> None:
    await world.owner_says(f"/arkadas {FRIEND}")
    conf = await world.ask_catch_up()
    (question,) = [m for m in world.client.to(FRIEND) if m.buttons]
    await world.n.handle_callback(CbEvent(f"no:{conf.id}".encode(), sender_id=FRIEND, message_id=question.id))
    await world.settle()
    assert world.tm.opened == []
    answer = "❌ Hayır (yanıtlayan: tickmill hesabının sahibi): tickmill hesabında işlem açılmadı."
    assert [e for e in world.client.edits_of(OWNER)][-1].text.endswith(f"<b>Sonuç:</b> {answer}")
    owner_count = len(world.client.to(OWNER))
    await world.n.handle_callback(CbEvent(f"ok:{conf.id}".encode(), sender_id=FRIEND, message_id=question.id))
    await world.settle()
    assert len(world.client.to(OWNER)) == owner_count and world.tm.opened == []
    assert world.client.to(FRIEND)[-1].text == "Bu onay artık geçerli değil (iptal edildi)."


# =========================================================================== #
# Review round 1 (referee)
# =========================================================================== #
def fail_question_sends(world: World, failures: list[Any]) -> None:
    """The next sends of a message WITH buttons to the friend fail, one per item ("slow": answered only after
    SEND_TIMEOUT_SEC - it must never be sent a second time)."""
    real = world.client.send_message

    async def send(entity: Any, message: Any, **kw: Any) -> Any:
        if entity == FRIEND and kw.get("buttons") and failures:
            failure = failures.pop(0)
            if failure == "slow":
                await asyncio.sleep(0.5)
            else:
                raise failure
        return await real(entity, message, **kw)

    world.client.send_message = send                     # type: ignore[method-assign]


def _with_buttons(world: World, chat: int, conf_id: str) -> list[Any]:
    return [m for m in world.client.to(chat) if m.buttons and f"ok:{conf_id}".encode() in [d for _, d in buttons(m)]]


@pytest.mark.parametrize("failure", ["flood", "disconnected", "no_answer"])
async def test_r1_the_friends_question_is_never_dropped_while_it_is_open(
    world: World, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    monkeypatch.setattr(notifier_mod, "RETRY_BASE_SEC", 0.05)
    monkeypatch.setattr(notifier_mod, "FLOOD_MARGIN_SEC", 0.0)
    monkeypatch.setattr(notifier_mod, "SEND_TIMEOUT_SEC", 0.2)
    await world.owner_says(f"/arkadas {FRIEND}")
    failures: list[Any] = {"flood": [errors.FloodWaitError(request=None, capture=1)],
                           "disconnected": [ConnectionError("Cannot send requests while disconnected")] * 3,
                           "no_answer": ["slow"]}[failure]
    fail_question_sends(world, failures)
    conf = await world.ask_catch_up()
    await world.settle()
    (question,) = _with_buttons(world, FRIEND, conf.id)   # delivered, once
    assert _with_buttons(world, OWNER, conf.id) == []     # the owner still has no buttons (the friend decides)
    await world.n.handle_callback(CbEvent(f"ok:{conf.id}".encode(), sender_id=FRIEND, message_id=question.id))
    await world.settle()
    assert len(world.tm.opened) == 1


async def test_r1_a_question_that_never_reached_the_friend_says_so_when_it_ends(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(notifier_mod, "RETRY_BASE_SEC", 0.05)
    await world.owner_says(f"/arkadas {FRIEND}")
    world.client.fail_for[FRIEND] = errors.UserIsBlockedError(request=None)
    conf = await world.ask_catch_up(friend=False)
    await asyncio.sleep(0.2)                              # retried meanwhile, never dropped
    assert world.n._friend_queue and world.n._friend_queue[0].conf_id == conf.id
    world.engine._clock = lambda: datetime.now(UTC) + timedelta(minutes=6)
    await world.engine._expire(conf.id)
    await world.settle()
    note = "Soru tickmill hesabının sahibine (arkadaşınıza) ulaştırılamadı; bu yüzden yanıt gelmedi."
    (closed,) = [e for e in world.client.edits_of(OWNER) if "<b>Sonuç:</b>" in e.text]
    assert closed.text.endswith(f"<b>Sonuç:</b> ⌛ Süre doldu; tickmill hesabında işlem açılmadı.\n{note}")
    assert not world.n._friend_queue                                       # the pump stopped retrying
    assert [m.text[:10] for m in world.client.to(FRIEND)] == ["👋 Merhaba!"]   # (only the /arkadas test message)


async def test_r2_only_a_users_own_id_is_accepted(world: World) -> None:
    await world.owner_says("/arkadas -100777888")
    assert world.client.to(OWNER)[-1].text == (
        "Kullanım: <code>/arkadas &lt;sohbet_id&gt;</code> (arkadaşınızın kullanıcı kimliği: pozitif bir sayı; grup "
        "ya da kanal olmaz), <code>/arkadas</code> ya da <code>/arkadas sil</code>")
    assert world.client.to(-100777888) == [] and await world.db.get_state("friend_chat_id") is None


@pytest.mark.parametrize("change", ["sil", "other"])
async def test_r3_a_change_of_friend_gives_the_open_questions_to_the_owner(world: World, change: str) -> None:
    other = 999
    await world.owner_says(f"/arkadas {FRIEND}")
    conf = await world.ask_catch_up()
    (old_question,) = _with_buttons(world, FRIEND, conf.id)
    await world.owner_says("/arkadas sil" if change == "sil" else f"/arkadas {other}")
    (owner_question,) = _with_buttons(world, OWNER, conf.id)
    assert owner_question.text.startswith("♻️ Arkadaş ayarı değişti; bu soruyu artık siz cevaplıyorsunuz.\n❓ <b>#1 ETH")
    assert buttons(owner_question) == [("✅ Evet", f"ok:{conf.id}".encode()), ("❌ Hayır", f"no:{conf.id}".encode())]
    assert _with_buttons(world, other, conf.id) == []
    edited = [e for e in world.client.edits_of(FRIEND) if e.msg_id == old_question.id][-1]
    assert edited.buttons is None and edited.text.endswith("Bu soru artık size ait değil; bot sahibi cevaplayacak.")
    ev = CbEvent(f"ok:{conf.id}".encode(), sender_id=FRIEND, message_id=old_question.id)
    await world.n.handle_callback(ev)                     # the old friend's tap
    await world.settle()
    assert ev.answers == [("Bu soru artık size ait değil.", True)] and world.tm.opened == []
    await world.n.handle_callback(CbEvent(f"ok:{conf.id}".encode(), sender_id=OWNER, message_id=owner_question.id))
    await world.settle()
    assert len(world.tm.opened) == 1
    closed = [e for e in world.client.edits_of(OWNER) if e.msg_id == owner_question.id][-1]
    assert closed.buttons is None and "<b>Sonuç:</b> ✅ Evet" in closed.text


async def test_r4_who_approved_is_named_in_both_chats(world: World) -> None:
    await world.owner_says(f"/arkadas {FRIEND}")
    conf = await world.ask_catch_up()
    (question,) = _with_buttons(world, FRIEND, conf.id)
    await world.n.handle_callback(CbEvent(f"ok:{conf.id}".encode(), sender_id=FRIEND, message_id=question.id))
    await world.settle()
    title = "✅ <b>#1 ETH LONG</b> — tickmill hesabında sonradan açıldı (tickmill hesabının sahibinin onayıyla)"
    for chat in (OWNER, FRIEND):
        assert any(title in m.text for m in world.client.to(chat)), chat
    assert not any("onayınızla" in m.text for m in world.client.sent)


async def test_r4_the_owners_own_approval_is_named_when_a_friend_reads_along(world: World) -> None:
    conf = await world.ask_catch_up()                     # asked to the owner (no friend yet)
    await world.owner_says(f"/arkadas {FRIEND}")
    owner_q = [m for m in world.client.to(OWNER) if m.buttons and "açılsın mı?" in m.text][-1]
    await world.n.handle_callback(CbEvent(f"ok:{conf.id}".encode(), sender_id=OWNER, message_id=owner_q.id))
    await world.settle()
    title = "✅ <b>#1 ETH LONG</b> — tickmill hesabında sonradan açıldı (bot sahibinin onayıyla)"
    for chat in (OWNER, FRIEND):
        assert any(title in m.text for m in world.client.to(chat)), chat
    assert any("✅ Evet (yanıtlayan: bot sahibi): tickmill hesabına emir" in m.text for m in world.client.to(FRIEND))


async def test_r5_a_friend_press_on_a_question_he_was_never_given_is_refused(world: World) -> None:
    conf = await world.ask_catch_up()                     # the owner has the buttons
    await world.owner_says(f"/arkadas {FRIEND}")
    world.client.sent.clear()
    await world.n.handle_callback(CbEvent(f"ok:{conf.id}".encode(), sender_id=FRIEND, message_id=12345))
    await world.settle()
    assert world.tm.opened == []
    assert [m.text for m in world.client.to(FRIEND)] == ["Bu soruyu yalnızca bot sahibi cevaplayabilir."]


async def test_r6_a_question_that_reaches_the_friend_late_says_so_or_is_not_sent_any_more(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(notifier_mod, "FLOOD_MARGIN_SEC", 0.0)
    monkeypatch.setattr(notifier_mod, "LATE_MARK_SEC", 0.5)
    await world.owner_says(f"/arkadas {FRIEND}")
    fail_question_sends(world, [errors.FloodWaitError(request=None, capture=1)])
    conf = await world.ask_catch_up()
    await world.settle()
    (question,) = _with_buttons(world, FRIEND, conf.id)
    assert question.text.startswith("🕒 <i>Bu soru size 1 sn gecikmeyle ulaştı; yanıt için kalan süre aşağıda "
                                    "yazıyor.</i>\n❓ <b>#1 ETH LONG tickmill hesabında da açılsın mı?</b>")
    monkeypatch.setattr(notifier_mod, "QUESTION_MIN_LEFT_SEC", 10_000.0)     # too late to ask at all now ...
    fail_question_sends(world, [errors.FloodWaitError(request=None, capture=1)])     # ... once it is late
    second = await world.ask_catch_up()
    await world.settle()
    assert _with_buttons(world, FRIEND, second.id) == []


async def test_r7_the_friend_is_known_before_the_notifier_starts(tmp_path: Path) -> None:
    from test_app_cli import FakeListener, write_config

    from copytrader.app import build_app, serve
    from copytrader.config import load_config

    from fakes import FakeNotifier, FakeParser

    path = str(tmp_path / "bot.db")
    db = Database(path)
    await db.open()
    await db.set_state("friend_chat_id", FRIEND)
    await db.close()
    order: list[str] = []

    class Recording(FakeNotifier):
        def set_friend(self, chat_id: int | None) -> None:
            order.append(f"set_friend {chat_id}")

        async def start(self, engine: Any) -> None:
            order.append("start")
            await super().start(engine)

    app = build_app(load_config(write_config(tmp_path)), db=Database(path), parser=FakeParser(), notifier=Recording(),
                    broker_factory=lambda cfg: FakeBroker(cfg.name), listener_factory=FakeListener)
    stop = asyncio.Event()
    task = asyncio.ensure_future(serve(app, stop))
    await asyncio.wait_for(app.listener.started.wait(), 5)
    stop.set()
    await asyncio.wait_for(task, 10)
    assert order[:2] == [f"set_friend {FRIEND}", "start"]


async def test_r8_a_reply_to_the_owners_copy_is_no_correction(world: World) -> None:
    await world.owner_says(f"/arkadas {FRIEND}")
    await world.ask_catch_up()
    (copy,) = [m for m in world.client.to(OWNER) if "hesabında da açılsın mı?" in m.text]
    world.client.sent.clear()
    await world.n.handle_message(MsgEvent("evet aç", sender_id=OWNER, reply_to_msg_id=copy.id))
    await world.settle()
    assert [m.text for m in world.client.to(OWNER)] == [
        "Bu soruyu tickmill hesabının sahibi (arkadaşınız) cevaplıyor; bu soruda düzeltme yok."]
    assert world.tm.opened == []
