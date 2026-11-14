"""The report card at the owner's bot (copytrader/telegram/notifier.py) and in the app (copytrader/app.py):

  /karne, /karne hepsi   owner only; answered at once, the card follows from the service's own task
  send_owner_document    the card as a FILE for the owner alone: one bounded request, only with a native AES, paused
                         while the trading path is busy, never cancelled nor sent twice, never mirrored to the friend
  Pillow                 installed or not, Telethon never uses it in the bot's process
  app                    the service is built, started after the engine and stopped first

REAL BotNotifier and REAL KarneService; only Telegram (and the drawing process) are stand-ins.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from telethon import errors
from telethon.tl import types
from test_group_mirror import Bot
from test_karne_service import Clock, Drawer
from test_notifier import MsgEvent

from copytrader import app as app_mod
from copytrader.app import real_account_names, serve
from copytrader.config import AppConfig, NotifierConfig, TelegramConfig
from copytrader.karne.service import STATE_KEY, KarneService
from copytrader.storage.db import Database
from copytrader.telegram import group_mirror as gm
from copytrader.telegram import notifier as notifier_mod
from copytrader.telegram.notifier import BotNotifier

from fakes import FakeBroker, FakeNotifier
from test_app_cli import _app

OWNER, FRIEND, STRANGER = 777, 555, 666
NOW = datetime(2026, 11, 14, 13, 0, tzinfo=UTC)
PNG = b"\x89PNG\r\n\x1a\n" + b"k" * 30_000


class Engine:
    """What the notifier needs of the engine for a command."""

    async def status_text(self) -> str:
        return "durum"


def notifier(tmp_path: Path, *, friend: bool = True) -> tuple[BotNotifier, Bot]:
    n = BotNotifier(NotifierConfig(bot_token="123:abc", owner_user_id=OWNER),
                    TelegramConfig(api_id=1, api_hash="x", source_chats=[-100]), str(tmp_path))
    bot = Bot()
    n._client = bot
    n._engine = Engine()          # type: ignore[assignment]
    if friend:
        n.set_friend(FRIEND)
    return n, bot


@pytest.fixture
async def world(tmp_path: Path):
    n, bot = notifier(tmp_path)
    clock = Clock(NOW)
    db = Database(tmp_path / "copytrader.db", clock=clock)
    await db.open()
    drawer = Drawer()
    service = KarneService(db, n, tmp_path, accounts=["binance"], busy=lambda: False, clock=clock, renderer=drawer,
                           pillow=lambda: True)
    n.set_karne(service)
    yield n, bot, service, drawer, db
    await service.stop()
    await n.drain(2)
    for task in list(n._send_tasks) + [t for t in (n._pump_task, n._friend_pump) if t is not None]:
        task.cancel()
    await db.close()


async def card_out(service: KarneService) -> None:
    for _ in range(300):
        if not service._jobs:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("the card did not go out")


async def friend_idle(n: BotNotifier) -> None:
    for _ in range(100):
        if not n._friend_queue:
            break
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.02)


# --------------------------------------------------------------------------- #
# /karne
# --------------------------------------------------------------------------- #
async def test_the_owner_writes_karne_and_gets_the_card_as_a_file(world: Any) -> None:
    n, bot, service, drawer, _db = world
    await n.handle_message(MsgEvent("/karne"))
    assert [m.text for m in bot.to(OWNER)] == ["⏳ Karne hazırlanıyor; birkaç saniye içinde gelecek."]
    assert not n.trading_busy()                    # the command's handler is over: the card is the service's job now
    await card_out(service)
    (file,) = bot.files
    assert file.entity == OWNER and file.data == PNG[:8] + b"\x00" * 20_000 and file.force_document is True
    assert file.parse_mode == "html" and file.text.startswith("📊 <b>Ara karne · 8–14 Kasım 2026</b>")
    (attribute,) = file.attributes
    assert isinstance(attribute, types.DocumentAttributeFilename) and attribute.file_name == "karne_ara_2026-11-14.png"
    assert len(drawer.cards) == 1 and drawer.cards[0]["sample"] is False
    # the friend's mirror gets neither the answer nor the card
    await friend_idle(n)
    assert bot.to(FRIEND) == []


async def test_karne_hepsi_and_the_bot_name_in_the_command(world: Any) -> None:
    n, bot, service, drawer, _db = world
    await n.handle_message(MsgEvent("/karne@kopya_bot hepsi"))
    assert bot.to(OWNER)[-1].text == "⏳ Tüm işlemlerin karnesi hazırlanıyor; birkaç saniye içinde gelecek."
    await card_out(service)
    assert drawer.cards[0]["subtitle"].startswith("TÜM İŞLEMLER") and bot.files[0].attributes[0].file_name == (
        "karne_tum_2026-11-14.png")
    await n.handle_message(MsgEvent("/KARNE"))                                   # within the minute: refused
    assert bot.to(OWNER)[-1].text == "⏳ Az önce bir karne istendi; 60 sn sonra tekrar deneyin."
    await n.handle_message(MsgEvent("/karne dün"))
    assert bot.to(OWNER)[-1].text.startswith("Kullanım: <code>/karne</code>")
    assert len(drawer.cards) == 1 and len(bot.files) == 1


async def test_only_the_owner_can_ask_for_a_card(world: Any) -> None:
    n, bot, service, drawer, _db = world
    await n.handle_message(MsgEvent("/karne", sender_id=FRIEND))                 # the friend: never executed
    await n.handle_message(MsgEvent("/karne hepsi", sender_id=FRIEND))
    await n.handle_message(MsgEvent("/karne", sender_id=STRANGER))               # anyone else: ignored, silently
    await n.handle_message(MsgEvent("/karne", sender_id=OWNER, is_private=False))   # in a group: nothing at all
    await n.handle_message(MsgEvent("/karne", sender_id=OWNER, out=True))
    await friend_idle(n)
    await asyncio.sleep(0.05)
    assert drawer.cards == [] and bot.files == [] and not service._jobs
    assert bot.to(OWNER) == [] and bot.to(STRANGER) == []
    assert [m.text for m in bot.to(FRIEND)] == ["Bu bot komutlarını yalnızca sahibi kullanabilir."]


async def test_many_karne_commands_draw_one_card(world: Any) -> None:
    n, bot, service, drawer, _db = world
    drawer.gate = asyncio.Event()
    for _ in range(25):
        await n.handle_message(MsgEvent("/karne"))
        await n.handle_message(MsgEvent("/karne hepsi"))
    answers = [m.text for m in bot.to(OWNER)]
    assert answers[0].startswith("⏳ Karne hazırlanıyor") and len(answers) == 50
    assert set(answers[1:]) == {"⏳ Bir karne zaten hazırlanıyor; o gelince tekrar isteyebilirsiniz."}
    drawer.gate.set()
    await card_out(service)
    assert len(drawer.cards) == 1 and drawer.most == 1 and len(bot.files) == 1


async def test_karne_without_the_service_and_in_the_help(tmp_path: Path) -> None:
    n, bot = notifier(tmp_path, friend=False)
    await n.handle_message(MsgEvent("/karne"))
    assert bot.to(OWNER)[-1].text == "ℹ️ Karne bu kurulumda kullanılamıyor."
    await n.handle_message(MsgEvent("/yardim"))
    assert ("/karne — bu haftanın karnesi, şu ana kadar (haftalık karne her pazar 22:00'de, İngiltere saatiyle, "
            "kendiliğinden gelir); /karne hepsi — bugüne kadarki tüm işlemler") in bot.to(OWNER)[-1].text


async def test_the_text_card_goes_to_the_owner_alone_and_as_it_is(world: Any) -> None:
    n, bot, _service, _drawer, _db = world
    await n.tell_owner("📊 <b>BABÜR'S TRADE PERFORMANCE</b>\nToplam: +1.0R")
    assert [m.text for m in bot.to(OWNER)] == ["📊 <b>BABÜR'S TRADE PERFORMANCE</b>\nToplam: +1.0R"]   # no icon added
    await friend_idle(n)
    assert bot.to(FRIEND) == []


# --------------------------------------------------------------------------- #
# the file for the owner
# --------------------------------------------------------------------------- #
@pytest.fixture
def card(tmp_path: Path) -> str:
    path = tmp_path / "karne_test.png"
    path.write_bytes(PNG)
    return str(path)


@pytest.fixture(autouse=True)
def fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(notifier_mod, "OWNER_FILE_POLL_SEC", 0.01)


async def test_the_file_is_one_document_request_to_the_owner(tmp_path: Path, card: str) -> None:
    n, bot = notifier(tmp_path)
    assert await n.send_owner_document(card, caption="<b>Karne</b>", name="karne_2026-11-15.png") is None
    (file,) = bot.files
    assert (file.entity, file.data, file.text, file.force_document, file.parse_mode) == (
        OWNER, PNG, "<b>Karne</b>", True, "html")
    assert file.attributes[0].file_name == "karne_2026-11-15.png"
    await friend_idle(n)
    assert bot.to(FRIEND) == [] and len(bot.sent) == 1


async def test_without_a_native_aes_no_file_is_moved(tmp_path: Path, card: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Telethon's pure-Python AES would hold the event loop for seconds per MB: then the card is text."""
    n, bot = notifier(tmp_path)
    monkeypatch.setattr(gm, "native_aes", lambda: False)
    reason = await n.send_owner_document(card, caption="x", name="k.png")
    assert reason is not None and "cryptg" in reason and bot.files == []


async def test_the_file_never_overtakes_waiting_notices_and_is_not_tried_on_a_dead_link(tmp_path: Path, card: str) -> None:
    n, bot = notifier(tmp_path, friend=False)
    n._mark_down("test")
    assert "bağlantısı şu an yok" in (await n.send_owner_document(card) or "") and bot.files == []
    n._mark_up()
    bot.hang_for.add(OWNER)
    notice = asyncio.ensure_future(n.notify("bekleyen bildirim"))
    await asyncio.sleep(0.05)
    n._queue.append(notifier_mod._Outgoing(text="sırada"))
    assert "bekleyen mesajlar var" in (await n.send_owner_document(card) or "") and bot.files == []
    notice.cancel()
    n._shut = True
    assert await n.send_owner_document(card) == "bildirim botu çalışmıyor"
    n._client = None
    assert await n.send_owner_document(card) == "bildirim botu çalışmıyor"


async def test_the_upload_waits_for_an_idle_trading_path_before_it_and_between_its_parts(tmp_path: Path,
                                                                                        card: str) -> None:
    n, bot = notifier(tmp_path, friend=False)
    bot.parts = 3
    busy_until = time.monotonic() + 0.3
    looks: list[float] = []

    def busy() -> bool:
        looks.append(time.monotonic())
        return time.monotonic() < busy_until

    started = time.monotonic()
    assert await n.send_owner_document(card, busy=busy) is None
    assert bot.part_times and bot.part_times[0] - started >= 0.3                 # not a byte moved while busy
    assert len(bot.files) == 1 and len(looks) > 10

    # busy again in the middle of the upload: it pauses between two parts and goes on afterwards
    bot.files.clear()
    bot.part_times.clear()
    bot.part_delay = 0.05
    state = {"busy_from": 0.0, "busy_to": 0.0}

    def busy_mid() -> bool:
        if len(bot.part_times) == 1 and not state["busy_from"]:
            state["busy_from"], state["busy_to"] = time.monotonic(), time.monotonic() + 0.4
        return state["busy_from"] > 0 and time.monotonic() < state["busy_to"]

    assert await n.send_owner_document(card, busy=busy_mid) is None
    assert len(bot.part_times) == 3 and bot.part_times[1] - bot.part_times[0] >= 0.4
    assert len(bot.files) == 1


async def test_a_trading_path_that_stays_busy_gives_the_upload_up(tmp_path: Path, card: str,
                                                                 monkeypatch: pytest.MonkeyPatch) -> None:
    n, bot = notifier(tmp_path, friend=False)
    monkeypatch.setattr(notifier_mod, "OWNER_FILE_PAUSE_SEC", 0.2)
    started = time.monotonic()
    reason = await n.send_owner_document(card, busy=lambda: True)
    assert reason == "işlem trafiği 0.2 sn boyunca yoğundu" and 0.2 <= time.monotonic() - started < 2
    assert bot.files == []

    def broken() -> bool:
        raise RuntimeError("bakılamadı")

    assert await n.send_owner_document(card, busy=broken) == "işlem trafiği 0.2 sn boyunca yoğundu"     # = busy
    assert bot.files == []


async def test_a_file_telegram_does_not_answer_is_left_alone_not_cancelled_not_sent_again(
        tmp_path: Path, card: str, monkeypatch: pytest.MonkeyPatch) -> None:
    n, bot = notifier(tmp_path, friend=False)
    monkeypatch.setattr(notifier_mod, "OWNER_FILE_TIMEOUT_SEC", 0.2)
    bot.hang_for.add(OWNER)
    started = time.monotonic()
    reason = await n.send_owner_document(card)
    assert reason == "Telegram 0.2 sn içinde yanıt vermedi; dosya sonradan da gelebilir"
    assert 0.2 <= time.monotonic() - started < 2
    (request,) = n._send_tasks                                                   # still Telethon's: it may yet arrive
    assert not request.done() and bot.files == []
    request.cancel()
    await asyncio.gather(request, return_exceptions=True)


@pytest.mark.parametrize("error, reason", [
    (errors.FloodWaitError(request=None, capture=17), "Telegram 17 sn beklememizi istiyor"),
    (errors.UserIsBlockedError(request=None), "bot size mesaj gönderemiyor (botu Telegram'da açıp Start'a basın)"),
    (ConnectionError("bağlantı koptu"), "bağlantı koptu"),
    (ValueError(), "ValueError"),
])
async def test_a_file_telegram_refuses_gives_the_reason_and_never_raises(tmp_path: Path, card: str,
                                                                        error: Exception, reason: str) -> None:
    n, bot = notifier(tmp_path, friend=False)
    bot.fail_for[OWNER] = error
    assert await n.send_owner_document(card) == reason
    assert bot.files == [] and not n._send_tasks


# --------------------------------------------------------------------------- #
# Pillow never works in the bot's process
# --------------------------------------------------------------------------- #
def test_telethon_does_not_use_pillow_in_the_bots_process(monkeypatch: pytest.MonkeyPatch) -> None:
    from telethon.client import uploads

    assert uploads.PIL is None                    # whether or not Pillow is installed (the notifier module was imported)
    monkeypatch.setattr(uploads, "PIL", object())                                # "Pillow got installed"
    notifier_mod.keep_pillow_out_of_telethon()
    assert uploads.PIL is None
    notifier_mod.keep_pillow_out_of_telethon()                                   # (again: nothing to do, no error)
    # a photo upload then leaves the file exactly as it is: nothing is decoded or re-encoded on the event loop
    assert uploads._resize_photo_if_needed("resim.png", True) == "resim.png"


def test_the_guard_says_so_when_this_telethon_has_nothing_to_switch_off(monkeypatch: pytest.MonkeyPatch,
                                                                       caplog: pytest.LogCaptureFixture) -> None:
    """(Isolation review, P1.) requirements say telethon>=1.40: a later Telethon that renames ``uploads.PIL`` would
    bring the photo resizing back without a word. With Pillow importable in the bot's process that is a WARNING."""
    from telethon.client import uploads

    monkeypatch.delattr(uploads, "PIL")
    monkeypatch.setattr(notifier_mod, "_pillow_importable", lambda: False)       # no Pillow for the bot: all is well
    with caplog.at_level("INFO"):
        notifier_mod.keep_pillow_out_of_telethon()
    assert caplog.records == [] and not hasattr(uploads, "PIL")
    monkeypatch.setattr(notifier_mod, "_pillow_importable", lambda: True)
    notifier_mod.keep_pillow_out_of_telethon()
    (record,) = caplog.records
    assert record.levelname == "WARNING" and "this Telethon has no 'uploads.PIL' to switch off" in record.getMessage()
    assert "karne_paketleri" in record.getMessage()


def test_whether_pillow_can_be_imported_in_the_bots_process_is_looked_up_not_tried(
        monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib.util
    import sys

    loaded = "PIL" in sys.modules
    assert notifier_mod._pillow_importable() is (loaded or importlib.util.find_spec("PIL") is not None)
    assert ("PIL" in sys.modules) is loaded


# --------------------------------------------------------------------------- #
# the app
# --------------------------------------------------------------------------- #
def test_the_accounts_row_names_the_accounts_that_trade_real_money() -> None:
    base = dict(telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[-100]),
                notifier=NotifierConfig(bot_token="123:abc", owner_user_id=OWNER))
    brokers = [
        {"name": "binance", "type": "ccxt", "exchange": "binance", "api_key": "k", "secret": "s"},
        {"name": "kagit", "type": "paper"},
        {"name": "elle", "type": "manual"},
        {"name": "kapali", "type": "ccxt", "exchange": "bybit", "api_key": "k", "secret": "s", "enabled": False},
    ]
    live = AppConfig(dry_run=False, brokers=brokers, **base)                     # type: ignore[arg-type]
    assert real_account_names(live) == ["binance"]              # no paper, no message-only account, none disabled
    assert real_account_names(AppConfig(dry_run=True, brokers=brokers, **base)) == []      # type: ignore[arg-type]


async def test_the_app_builds_the_service_for_the_real_notifier_only(tmp_path: Path) -> None:
    app, _brokers = _app(tmp_path, FakeNotifier())                                               # the stand-in notifier of other tests
    assert app.karne is None
    real = BotNotifier(app.config.notifier, app.config.telegram, str(tmp_path))
    built = app_mod.build_app(app.config, db=app.db, parser=app.parser, notifier=real,
                              broker_factory=lambda cfg: FakeBroker(cfg.name),
                              listener_factory=lambda *a, **kw: app.listener)
    assert isinstance(built.karne, KarneService) and real._karne is built.karne
    assert built.karne._folder == Path(app.config.data_dir) / "karne"
    assert built.karne._busy() is False                                          # the shared "trading path busy" look


async def test_a_service_that_cannot_be_built_never_stops_the_bot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                                 caplog: pytest.LogCaptureFixture) -> None:
    from copytrader.karne import service as service_mod

    def broken(*_a: Any, **_kw: Any) -> None:
        raise RuntimeError("karne kurulamadı")

    monkeypatch.setattr(service_mod, "KarneService", broken)
    app, _brokers = _app(tmp_path, FakeNotifier())
    real = BotNotifier(app.config.notifier, app.config.telegram, str(tmp_path))
    built = app_mod.build_app(app.config, db=app.db, parser=app.parser, notifier=real,
                              broker_factory=lambda cfg: FakeBroker(cfg.name),
                              listener_factory=lambda *a, **kw: app.listener)
    assert built.karne is None and real._karne is None and built.engine is not None
    assert "Report card (karne) could not be set up" in caplog.text


class Recorder:
    """Stands in for the service in serve(): when is it started and stopped, and what if it misbehaves."""

    def __init__(self, events: list[str], *, fail: bool = False, slow: float = 0.0) -> None:
        self.events, self.fail, self.slow = events, fail, slow

    def start(self) -> None:
        self.events.append("karne start")
        if self.fail:
            raise RuntimeError("zamanlayıcı başlamadı")

    async def stop(self) -> None:
        self.events.append("karne stop")
        await asyncio.sleep(self.slow)
        if self.fail:
            raise RuntimeError("zamanlayıcı durmadı")


@pytest.mark.parametrize("fail", [False, True])
async def test_serve_starts_the_schedule_after_the_engine_and_stops_it_first(tmp_path: Path, fail: bool) -> None:
    app, _brokers = _app(tmp_path, FakeNotifier())
    events: list[str] = []
    app.karne = Recorder(events, fail=fail)
    engine_start, listener_stop, engine_stop = app.engine.start, app.listener.stop, app.engine.stop

    async def start(*a: Any, **kw: Any) -> Any:
        result = await engine_start(*a, **kw)
        events.append("engine start")
        return result

    async def stop_listener(*a: Any, **kw: Any) -> Any:
        events.append("listener stop")
        return await listener_stop(*a, **kw)

    async def stop_engine(*a: Any, **kw: Any) -> Any:
        events.append("engine stop")
        return await engine_stop(*a, **kw)

    app.engine.start, app.listener.stop, app.engine.stop = start, stop_listener, stop_engine     # type: ignore[method-assign]
    stop = asyncio.Event()
    server = asyncio.ensure_future(serve(app, stop))
    await asyncio.wait_for(app.listener.started.wait(), 5)
    await asyncio.sleep(0.05)
    assert events == ["engine start", "karne start"]                             # (a failing start: the bot runs on)
    assert not server.done()
    stop.set()
    await asyncio.wait_for(server, 15)
    assert events == ["engine start", "karne start", "karne stop", "listener stop", "engine stop"]


async def test_the_real_service_in_serve_writes_its_first_record_and_stops_with_the_bot(tmp_path: Path) -> None:
    app, _brokers = _app(tmp_path, FakeNotifier())
    app.karne = KarneService(app.db, app.notifier, tmp_path, accounts=[])
    stop = asyncio.Event()
    server = asyncio.ensure_future(serve(app, stop))
    await asyncio.wait_for(app.listener.started.wait(), 5)
    for _ in range(200):
        if await app.db.get_state(STATE_KEY) is not None:
            break
        await asyncio.sleep(0.01)
    state = await app.db.get_state(STATE_KEY)
    assert state["note"] == "init" and state["handled"] == state["cutoff"]       # the first start: no card, no line
    assert datetime.fromisoformat(state["handled"]) <= datetime.now(UTC) < (
        datetime.fromisoformat(state["handled"]) + timedelta(days=7, hours=1))
    task = app.karne._task
    assert task is not None and not task.done()
    began = time.monotonic()
    stop.set()
    await asyncio.wait_for(server, 15)
    assert task.done() and time.monotonic() - began < 10
    assert not any("karne" in text.lower() for text in app.notifier.texts())
