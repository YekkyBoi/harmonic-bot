"""Review 1, glue lane of the owner's bot (copytrader/telegram/notifier.py, copytrader/interfaces.py).

G1  While Engine.start() is still connecting the owner's brokers, the engine answers an approval, a correction and
    close-all with STARTING_TEXT and leaves the confirmation pending. The owner's bot then only shows that text and
    keeps the question's buttons (as it does for SHUTDOWN_TEXT), so the owner can tap again a few seconds later.
    Before: every answer but SHUTDOWN_TEXT was taken as the outcome and the question lost its buttons.
G2  A notifier call that the engine or a broker awaits on a trading path (notify, ask_confirmation,
    close_confirmation; the engine holds its open lock and the topic's lock meanwhile) waits for Telegram at most
    CALLER_WAIT_SEC. A message that got no answer by then stays queued in its place with its request still running
    and is delivered later, nothing twice and nothing out of order; the link is taken for down only after
    SEND_TIMEOUT_SEC without an answer, as before. Before: the first message on a silent link held its caller for
    the whole SEND_TIMEOUT_SEC (20 s), so the owner's next signal waited up to 20 s.

The real BotNotifier runs on the Telethon double of tests/test_preflight3_notifier.py (``FakeClient``: requests
are answered at once, or not at all while its ``link`` is cleared). The end-to-end tests use the production replica
of tests/test_preflight_scenarios.py on the server's config (LIVE_YAML) with this real owner bot in place of
FakeNotifier, the shipped timing constants and the real wall clock.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

import pytest
from telethon import errors

import test_preflight3_notifier as p3
import test_preflight_scenarios as tps
from test_preflight3_notifier import CbEvent, FakeClient, MsgEvent, buttons_of, make_conf
from test_preflight_scenarios import EURUSD_SHORT, FX, KRIPTO, Prod, btc, fx_pair

from copytrader.brokers.ccxt_broker import CcxtBroker
from copytrader.brokers.manual_broker import ManualBroker
from copytrader.brokers.metaapi_broker import MetaApiBroker
from copytrader.brokers.mt5_broker import MT5, Mt5Broker
from copytrader.config import NotifierConfig, TelegramConfig
from copytrader.engine import orchestrator as orch
from copytrader.engine.orchestrator import Engine
from copytrader.engine.prices import PriceOracle
from copytrader.models import EntryType, PositionStatus, Severity
from copytrader.telegram import notifier as notifier_mod
from copytrader.telegram.formatting import CORRECTION_HINT, UNDELIVERED_QUESTION, format_confirmation
from copytrader.telegram.notifier import BotNotifier

from test_mt5_broker import SERVER_TIME

OWNER = 777
STARTING = "⏳ Bot başlıyor; birkaç saniye sonra tekrar deneyin."
# (taken from the engine: the copy in copytrader.interfaces is what this lane adds; 62e0719's notifier has none)
STARTING_TEXT = orch.STARTING_TEXT
GUARD = 30.0               # a call that takes longer than this hangs: fail instead of blocking the test run


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch) -> None:
    from copytrader.telegram import formatting as fmt
    monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: SERVER_TIME - 7200.0))
    monkeypatch.setattr(Mt5Broker, "tick_wait_sec", 0.0)
    monkeypatch.setattr(fmt, "RISK_PER_R_NOTE", fmt.RISK_PER_R_NOTE)


def owner_bot(data_dir: str) -> BotNotifier:
    """The real owner bot on the Telethon double (not started: no login; it sends through the double)."""
    n = BotNotifier(NotifierConfig(bot_token="123:abc", owner_user_id=OWNER),
                    TelegramConfig(api_id=1, api_hash="x", source_chats=[-100]), data_dir)
    n._client = FakeClient()
    return n


class OwnerBot(BotNotifier):
    """The real BotNotifier on the Telethon double, plus what the replica reads from FakeNotifier: ``notifications``
    (cleared after the start), ``texts()`` (what reached the owner) and ``handed`` (every notice in the order the
    engine and the brokers handed it over, as it would read on Telegram)."""

    def __init__(self, data_dir: str) -> None:
        super().__init__(NotifierConfig(bot_token="123:abc", owner_user_id=OWNER),
                         TelegramConfig(api_id=1, api_hash="x", source_chats=[-100]), data_dir)
        self.client = FakeClient()
        self._client = self.client
        self.notifications: list[tuple[str, Severity]] = []
        self.handed: list[str] = []

    async def notify(self, text: str, severity: Severity = Severity.INFO) -> None:
        self.notifications.append((text, severity))
        self.handed.append(f"{notifier_mod._PREFIX[severity]} {text}")
        await super().notify(text, severity)

    def texts(self, severity: Severity | None = None) -> list[str]:
        return [s.text for s in self.client.sent]


async def replica(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Prod, OwnerBot]:
    """tests/test_preflight_scenarios.Prod on the server's config (LIVE_YAML), with the real owner bot."""
    bot = OwnerBot(str(tmp_path / "bot"))
    monkeypatch.setattr(tps, "FakeNotifier", lambda: bot)
    p = await Prod.create(tmp_path, monkeypatch, replica="live")
    bot._engine = p.engine
    return p, bot


async def until(predicate: Any, timeout: float = GUARD) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached in time"
        await asyncio.sleep(0.01)


def edits_of(client: FakeClient, msg_id: int) -> list[Any]:
    return [e for e in client.edits if e.msg_id == msg_id]


# =========================================================================== #
# G1  STARTING_TEXT keeps the question usable
# =========================================================================== #
def test_g1_starting_text_is_defined_next_to_shutdown_text_and_is_the_engines() -> None:
    from copytrader import interfaces
    assert interfaces.STARTING_TEXT == STARTING
    assert orch.STARTING_TEXT == interfaces.STARTING_TEXT     # the engine refuses with the very text the bot knows
    assert notifier_mod.STARTING_TEXT is interfaces.STARTING_TEXT


class StartingEngine:
    """EngineControl double: while ``starting`` every owner action that would trade is refused with the engine's
    STARTING_TEXT (orchestrator.py handle_confirmation / request_correction / handle_correction / close_all)."""

    def __init__(self) -> None:
        self.starting = True
        self.calls: list[tuple[Any, ...]] = []

    async def handle_confirmation(self, conf_id: str, approved: bool) -> str:
        self.calls.append(("confirm", conf_id, approved))
        if approved and self.starting:
            return STARTING_TEXT
        return "✅ İşlem açıldı" if approved else "❌ İptal edildi"

    async def request_correction(self, conf_id: str) -> str:
        self.calls.append(("fix", conf_id))
        return STARTING_TEXT if self.starting else f"✏️ Onay #{conf_id} için düzeltmenizi bekliyorum."

    async def handle_correction(self, conf_id: str, correction_text: str) -> str:
        self.calls.append(("correction", conf_id, correction_text))
        return STARTING_TEXT if self.starting else "🔁 Yeniden soruldu"

    async def awaiting_correction_id(self) -> str | None:
        return None

    async def close_all(self) -> str:
        self.calls.append(("close_all",))
        return STARTING_TEXT if self.starting else "✅ Açık işlem yoktu; kapatılacak bir şey bulunmadı."


@pytest.fixture
async def bot(tmp_path: Path):
    n = owner_bot(str(tmp_path))
    engine = StartingEngine()
    n._engine = engine
    yield n, n._client, engine
    n._client.link.set()
    await asyncio.wait_for(n.stop(), GUARD)


async def test_g1_approval_during_start_keeps_the_buttons_and_works_after_the_start(bot) -> None:
    n, client, engine = bot
    conf = make_conf()
    msg_id = await n.ask_confirmation(conf)
    question = client.sent[-1]
    assert msg_id == question.id and buttons_of(question) == [b"ok:c1", b"fix:c1", b"no:c1"]
    await n.handle_callback(CbEvent(b"ok:c1", message_id=msg_id))
    assert edits_of(client, msg_id) == []                         # the question was not touched: buttons kept
    assert client.sent[-1].text == STARTING_TEXT                  # the owner reads why nothing happened
    assert "c1" not in n._closed                                  # and the bot does not take it for closed
    engine.starting = False                                       # a few seconds later the same button works
    await n.handle_callback(CbEvent(b"ok:c1", message_id=msg_id))
    (edit,) = edits_of(client, msg_id)
    assert "İşlem açıldı" in edit.text and edit.buttons is None
    assert engine.calls == [("confirm", "c1", True), ("confirm", "c1", True)]


async def test_g1_correction_button_during_start_shows_only_the_text(bot) -> None:
    n, client, engine = bot
    msg_id = await n.ask_confirmation(make_conf())
    await n.handle_callback(CbEvent(b"fix:c1", message_id=msg_id))
    assert client.sent[-1].text == STARTING_TEXT                  # no "write the correction" hint: nothing waits
    assert CORRECTION_HINT not in client.sent[-1].text and edits_of(client, msg_id) == []
    engine.starting = False
    await n.handle_callback(CbEvent(b"fix:c1", message_id=msg_id))
    assert CORRECTION_HINT in client.sent[-1].text


async def test_g1_correction_typed_as_a_reply_during_start_is_not_reported_as_received(bot) -> None:
    n, client, engine = bot
    msg_id = await n.ask_confirmation(make_conf())
    before = len(client.sent)
    await n.handle_message(MsgEvent("SL 2200 olacak", reply_to_msg_id=msg_id))
    assert [s.text for s in client.sent[before:]] == [STARTING_TEXT]  # not "⏳ Düzeltme alındı, ..." first
    assert engine.calls == [("fix", "c1")] and edits_of(client, msg_id) == []
    engine.starting = False
    await n.handle_message(MsgEvent("SL 2200 olacak", reply_to_msg_id=msg_id))
    assert engine.calls[-1] == ("correction", "c1", "SL 2200 olacak")


async def test_g1_close_all_during_start_gets_its_buttons_back_and_can_be_confirmed_again(bot) -> None:
    n, client, engine = bot
    await n.handle_message(MsgEvent("/hepsini_kapat"))
    prompt = client.sent[-1]
    assert buttons_of(prompt) == [b"closeall:yes", b"closeall:no"]
    await n.handle_callback(CbEvent(b"closeall:yes", message_id=prompt.id))
    assert client.sent[-1].text == STARTING_TEXT
    last = edits_of(client, prompt.id)[-1]                        # the question is shown again, with its buttons
    assert last.text == prompt.text and buttons_of(last) == [b"closeall:yes", b"closeall:no"]
    engine.starting = False
    await n.handle_callback(CbEvent(b"closeall:yes", message_id=prompt.id))
    assert engine.calls == [("close_all",), ("close_all",)]       # the second tap was carried out
    assert edits_of(client, prompt.id)[-1].text == "✅ Açık işlem yoktu; kapatılacak bir şey bulunmadı."
    assert edits_of(client, prompt.id)[-1].buttons is None


async def test_g1_owner_taps_onayla_while_the_restarted_engine_connects(tmp_path: Path,
                                                                       monkeypatch: pytest.MonkeyPatch) -> None:
    """The production flow end to end: a question was pending before a restart; the restarted engine is still
    connecting binance (2 s) when the owner taps "Onayla" on it. The real owner bot shows STARTING_TEXT and keeps
    the question's buttons; the same button opens the trade once the start is over."""
    p, bot = await replica(tmp_path, monkeypatch)
    client = bot.client
    await p.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0).model_copy(update={"confidence": 0.3}), KRIPTO,
                "btc long belki")
    (conf,) = await p.db.pending_confirmations()
    question = client.sent[-1]
    assert conf.bot_message_id == question.id and f"ok:{conf.id}".encode() in buttons_of(question)
    await p.engine.stop(grace_sec=1.0)                            # the bot stops; the question stays pending
    # The restarted bot: new adapters on the same venues and database (as in test_review1_engine_iso I5).
    by = {b.name: b for b in p.cfg.brokers}

    async def no_sleep(_seconds: float) -> None:
        return None

    binance = CcxtBroker(by["binance"], exchange=p.binance, sleep=no_sleep)
    original_connect = binance.connect

    async def slow_connect() -> None:
        await asyncio.sleep(2.0)
        await original_connect()

    binance.connect = slow_connect                                # type: ignore[method-assign]
    fx = Mt5Broker(by["fx"], mt5_module=p.terminal)
    fx.retry_delay_sec = 0
    tickmill = MetaApiBroker(by["tickmill"], connection=p.tickmill)
    tickmill.retry_delay_sec = 0
    adapters = [binance, ManualBroker(by["breakout"], p.cfg.data_dir, price_source=p.binance), fx, tickmill]
    holder: list[Engine] = []
    oracle = PriceOracle(lambda: holder[0].brokers, p.cfg.price_source_exchange, cache_ttl_sec=0.0,
                         public_exchange=p.binance)
    engine = Engine(p.cfg, p.db, p.parser, adapters, bot, price_oracle=oracle,  # type: ignore[arg-type]
                    reference_prices=p.ref)
    holder.append(engine)
    p.engine = engine
    bot._engine = engine
    try:
        starting = asyncio.ensure_future(engine.start(monitor=False))
        await asyncio.sleep(0.3)
        assert not starting.done()
        await bot.handle_callback(CbEvent(f"ok:{conf.id}".encode(), message_id=question.id))
        tapped = client.sent[-1].text
        kept = edits_of(client, question.id)
        await bot.handle_callback(CbEvent(f"fix:{conf.id}".encode(), message_id=question.id))
        fixed = client.sent[-1].text
        still = await p.db.get_confirmation(conf.id)
        assert not starting.done()                                # (all of it happened during the start)
        await starting
        await bot.handle_callback(CbEvent(f"ok:{conf.id}".encode(), message_id=question.id))
        approved = await p.db.get_confirmation(conf.id)
        (trade,) = await p.trades()
        positions = await p.positions(trade.id)
    finally:
        await p.close()
        client.link.set()
        await asyncio.wait_for(bot.stop(), GUARD)
    assert kept == []                                             # buttons kept during the start
    assert tapped == STARTING_TEXT
    assert fixed == STARTING_TEXT and CORRECTION_HINT not in fixed
    assert still is not None and still.status.value == "pending" and not still.awaiting_correction
    (closed,) = edits_of(client, question.id)                     # after the start: approved and opened
    assert "#1 BTC LONG açıldı" in closed.text and closed.buttons is None, closed.text
    assert approved is not None and approved.status.value == "approved"
    assert {b: pos.status for b, pos in positions.items()} == {
        "binance": PositionStatus.OPEN, "breakout": PositionStatus.OPEN, "tickmill": PositionStatus.OPEN}


# =========================================================================== #
# G2  a silent link holds a caller for CALLER_WAIT_SEC only
# =========================================================================== #
SEND = 1.0                 # SEND_TIMEOUT_SEC in the unit tests below (production: 20 s)
WAIT = 0.2                 # CALLER_WAIT_SEC in the unit tests below (production: 2.5 s)
AT_ONCE = 0.05             # "did not wait for Telegram at all"
FAST = {**p3.FAST, "SEND_TIMEOUT_SEC": SEND, "CALLER_WAIT_SEC": WAIT}
LONG_SEND = 5.0            # SEND_TIMEOUT_SEC where the link answers again "in time" (no outage on a busy machine)


def answer_in_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """The scenario needs Telegram to answer before SEND_TIMEOUT_SEC: give it room, the bound stays WAIT."""
    monkeypatch.setattr(notifier_mod, "SEND_TIMEOUT_SEC", LONG_SEND)


class OrderedClient(FakeClient):
    """The Telethon double, plus the order in which Telegram carried out sends and edits (``done``) and how many
    edit requests were made (``edit_calls``)."""

    def __init__(self) -> None:
        super().__init__()
        self.done: list[tuple[str, Any]] = []
        self.edit_calls = 0

    async def send_message(self, entity: Any, message: Any, **kw: Any) -> Any:
        result = await super().send_message(entity, message, **kw)
        self.done.append(("send", message))
        return result

    async def edit_message(self, entity: Any, message: Any, text: Any, **kw: Any) -> None:
        self.edit_calls += 1
        await super().edit_message(entity, message, text, **kw)
        self.done.append(("edit", message))


@pytest.fixture
async def g2bot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    for name, value in FAST.items():
        monkeypatch.setattr(notifier_mod, name, value, raising=False)    # (62e0719 has no CALLER_WAIT_SEC)
    n = owner_bot(str(tmp_path))
    client = n._client = OrderedClient()
    engine = StartingEngine()
    engine.starting = False
    n._engine = engine
    yield n, client, engine
    client.link.set()
    await asyncio.wait_for(n.stop(), GUARD)


async def timed(awaitable: Any) -> float:
    started = time.monotonic()
    await asyncio.wait_for(awaitable, GUARD)
    return time.monotonic() - started


async def test_g2_silent_link_holds_a_notice_for_the_short_bound_and_is_taken_for_down_after_the_send_timeout(
        g2bot) -> None:
    n, client, _ = g2bot
    client.link.clear()                           # Telegram stops answering; Telethon still says "connected"
    started = time.monotonic()
    took = await timed(n.notify("bir", Severity.ERROR))
    assert WAIT * 0.9 <= took < WAIT + 0.15, took # not the whole SEND_TIMEOUT_SEC (62e0719: SEND)
    (first,) = n._queue                           # kept in its place, its request still on its way
    assert first.text == "🚨 bir" and first.task is not None and not first.task.done()
    assert n._down_since is None                  # not taken for down after the short bound
    assert await timed(n.notify("iki", Severity.WARNING)) < AT_ONCE     # behind it, without waiting
    assert await timed(n.notify("üç")) < AT_ONCE
    await until(lambda: n._down_since is not None, SEND + 2)
    assert time.monotonic() - started >= SEND * 0.95                    # ...only after SEND_TIMEOUT_SEC
    assert abs(n._down_since - first.task_started) < 0.05              # counted from the unanswered request
    client.link.set()                                                   # Telegram answers again
    await until(lambda: not n._queue and len(client.texts) == 3)
    assert client.texts == ["🚨 bir", "⚠️ iki", "ℹ️ üç"]               # in order, each exactly once
    assert len(client.send_calls) == 3                                  # the first request was not made again


async def test_g2_slow_link_answered_after_the_bound_is_not_an_outage_and_nothing_goes_twice(
        g2bot, monkeypatch) -> None:
    n, client, _ = g2bot
    answer_in_time(monkeypatch)
    client.delay = 2.5 * WAIT                     # every request is answered, after 0.5 s (WAIT < 0.5 < SEND)
    took = await timed(n.notify("yavaş"))
    assert took < WAIT + 0.15, took               # 62e0719: waited for the answer (0.5 s)
    assert await timed(n.notify("sonraki")) < AT_ONCE
    await until(lambda: not n._queue and len(client.texts) == 2)
    await asyncio.sleep(0.1)
    assert client.texts == ["ℹ️ yavaş", "ℹ️ sonraki"] and len(client.send_calls) == 2
    assert n._down_since is None and n._outage_count == 0     # slow is not "down": no outage, no report


async def test_g2_question_that_goes_out_late_gets_its_message_id_so_its_buttons_work(g2bot, monkeypatch) -> None:
    n, client, engine = g2bot
    answer_in_time(monkeypatch)
    client.link.clear()
    conf = make_conf()
    started = time.monotonic()
    msg_id = await asyncio.wait_for(n.ask_confirmation(conf), GUARD)
    took = time.monotonic() - started
    assert msg_id is None and took < WAIT + 0.15, took         # 62e0719: SEND_TIMEOUT_SEC
    client.link.set()
    await until(lambda: not n._queue and len(client.sent) == 1)
    question = client.sent[0]
    assert question.text == format_confirmation(conf)          # asked as it was (on time: no late line)
    assert buttons_of(question) == [b"ok:c1", b"fix:c1", b"no:c1"]
    assert n._sent["c1"][0] == question.id and n._by_msg[question.id] == "c1"
    await n.handle_callback(CbEvent(b"ok:c1", message_id=question.id))
    assert engine.calls == [("confirm", "c1", True)]
    (edit,) = edits_of(client, question.id)                    # the outcome is shown in that very message
    assert "İşlem açıldı" in edit.text and edit.buttons is None


async def test_g2_question_that_goes_out_late_takes_replies_and_is_closed_in_place(g2bot) -> None:
    """Guard (passes on 62e0719 too, where it took a SEND_TIMEOUT_SEC wait): a question delivered after its
    ask_confirmation returned None is known by its message id, which now happens after CALLER_WAIT_SEC already."""
    n, client, engine = g2bot
    client.link.clear()
    conf = make_conf()
    conf.bot_message_id = await asyncio.wait_for(n.ask_confirmation(conf), GUARD)   # what the engine stores
    assert conf.bot_message_id is None
    client.link.set()
    await until(lambda: not n._queue and len(client.sent) == 1)
    question = client.sent[0]
    await n.handle_message(MsgEvent("SL 2200 olacak", reply_to_msg_id=question.id))
    assert engine.calls == [("fix", "c1"), ("correction", "c1", "SL 2200 olacak")]   # a reply to it = its correction
    await asyncio.wait_for(n.close_confirmation(conf, "⌛ Süre doldu, işlem açılmadı."), GUARD)
    (edit,) = edits_of(client, question.id)                    # closed in place, buttons removed
    assert "Süre doldu" in edit.text and edit.buttons is None
    assert not any(UNDELIVERED_QUESTION in t for t in client.texts)


async def test_g2_outcome_edit_on_a_silent_link_does_not_hold_the_caller_and_is_done_once_in_order(
        g2bot, monkeypatch) -> None:
    n, client, _ = g2bot
    answer_in_time(monkeypatch)
    conf = make_conf()
    conf.bot_message_id = q = await asyncio.wait_for(n.ask_confirmation(conf), GUARD)
    assert q == client.sent[0].id
    client.link.clear()
    took = await timed(n.close_confirmation(conf, "✅ İşlem açıldı"))
    assert took < WAIT + 0.15, took                            # 62e0719: waited SEND_TIMEOUT_SEC for the edit
    assert await timed(n.notify("rapor")) < AT_ONCE            # queued behind the edit
    assert n._down_since is None
    await asyncio.sleep(WAIT)
    client.link.set()                                          # Telegram answers before SEND_TIMEOUT_SEC
    await until(lambda: not n._queue and len(client.done) == 3)
    await asyncio.sleep(0.1)
    assert client.done == [("send", client.sent[0].text), ("edit", q), ("send", "ℹ️ rapor")]
    assert client.edit_calls == 1                              # the first request did it: not made again
    (edit,) = client.edits
    assert "İşlem açıldı" in edit.text and edit.buttons is None
    assert len(client.sent) == 2                               # no extra outcome message: the edit worked


async def test_g2_outcome_edit_without_an_answer_in_the_send_timeout_falls_back_as_before(g2bot) -> None:
    n, client, _ = g2bot
    conf = make_conf()
    conf.bot_message_id = q = await asyncio.wait_for(n.ask_confirmation(conf), GUARD)
    client.link.clear()
    started = time.monotonic()
    assert await timed(n.close_confirmation(conf, "✅ İşlem açıldı")) < WAIT + 0.15
    await until(lambda: n._down_since is not None, SEND + 2)
    assert time.monotonic() - started >= SEND * 0.95          # taken for down only after SEND_TIMEOUT_SEC
    client.link.set()
    await until(lambda: not n._queue and len(client.done) == 3)
    # As 62e0719 did after an edit that timed out: the edit is made again, then the outcome as a reply to it.
    assert client.done == [("send", client.sent[0].text), ("edit", q), ("send", "<b>Sonuç:</b> ✅ İşlem açıldı")]
    assert client.sent[1].reply_to == q
    (edit,) = client.edits                                     # the given-up request was not carried out as well
    assert "İşlem açıldı" in edit.text and edit.buttons is None


async def test_g2_a_flow_with_several_messages_waits_for_the_short_bound_once(g2bot, monkeypatch) -> None:
    """Report, outcome of the question and a warning, as the engine sends them for an approved signal: the caller
    waits once (for the first message); the rest waits in the queue, in that order."""
    n, client, _ = g2bot
    answer_in_time(monkeypatch)
    conf = make_conf()
    conf.bot_message_id = q = await asyncio.wait_for(n.ask_confirmation(conf), GUARD)
    client.link.clear()
    took = await timed(n.notify("rapor"))
    took += await timed(n.close_confirmation(conf, "✅ İşlem açıldı"))
    took += await timed(n.notify("uyarı", Severity.WARNING))
    assert took < WAIT + 0.2, took
    client.link.set()
    await until(lambda: not n._queue and len(client.done) == 4)
    await asyncio.sleep(0.1)
    assert client.done == [("send", client.sent[0].text), ("send", "ℹ️ rapor"), ("edit", q),
                           ("send", "⚠️ uyarı")]
    assert client.edit_calls == 1 and len(client.send_calls) == 3


async def test_g2_outcome_edit_done_only_long_after_also_reaches_the_owner_as_a_marked_message(
        g2bot, monkeypatch) -> None:
    """The outcome edit waited in the queue through an outage longer than LATE_MARK_SEC: the question is edited and
    the outcome also arrives as a late-marked reply to it, as 62e0719 sends an outcome after an outage."""
    n, client, _ = g2bot
    answer_in_time(monkeypatch)
    monkeypatch.setattr(notifier_mod, "LATE_MARK_SEC", 0.3)
    conf = make_conf()
    conf.bot_message_id = q = await asyncio.wait_for(n.ask_confirmation(conf), GUARD)
    client.link.clear()
    await timed(n.notify("rapor"))
    assert await timed(n.close_confirmation(conf, "✅ İşlem açıldı")) < AT_ONCE
    assert n._queue[-1].edit is not None
    await asyncio.sleep(0.5)                       # (longer than LATE_MARK_SEC here)
    client.link.set()
    await until(lambda: not n._queue and any(s.reply_to == q for s in client.sent))
    (edit,) = client.edits
    assert edit.msg_id == q and "İşlem açıldı" in edit.text and edit.buttons is None and client.edit_calls == 1
    (reply,) = [s for s in client.sent if s.reply_to == q]
    assert reply.text.startswith("🕒") and reply.text.endswith("<b>Sonuç:</b> ✅ İşlem açıldı")
    assert client.done.index(("edit", q)) < client.done.index(("send", reply.text))


async def test_g2_queued_edit_refused_with_a_flood_wait_is_made_after_the_wait(g2bot, monkeypatch) -> None:
    """Telegram answers the queued outcome edit with a flood wait: nothing is tried before the wait is over, then the
    question is edited and the outcome follows as a reply (as 62e0719 did with an outcome edit that failed)."""
    n, client, _ = g2bot
    answer_in_time(monkeypatch)
    conf = make_conf()
    conf.bot_message_id = q = await asyncio.wait_for(n.ask_confirmation(conf), GUARD)
    client.link.clear()
    await timed(n.notify("rapor"))                 # waits WAIT, then waits in the queue: the edit queues behind it
    flood: list[float] = []
    real_edit = client.edit_message

    async def edit_message(entity: Any, message: Any, text: Any, **kw: Any) -> None:
        if not flood or time.monotonic() < flood[0]:
            flood.append(time.monotonic() + 0.3)
            error = errors.FloodWaitError(request=None, capture=0)
            error.seconds = 0.3                        # Telegram: "wait this long"
            raise error
        await real_edit(entity, message, text, **kw)

    client.edit_message = edit_message             # type: ignore[method-assign]
    assert await timed(n.close_confirmation(conf, "✅ İşlem açıldı")) < AT_ONCE
    assert n._queue[-1].edit is not None           # the edit waits behind "rapor"
    client.link.set()
    await until(lambda: not n._queue and len(client.sent) == 3)
    assert len(flood) == 1                         # one refusal; the next try came after the wait
    (edit,) = client.edits
    assert edit.msg_id == q and "İşlem açıldı" in edit.text and edit.buttons is None
    assert client.texts[1:] == ["ℹ️ rapor", "<b>Sonuç:</b> ✅ İşlem açıldı"] and client.sent[2].reply_to == q


async def test_g2_queued_edit_that_fails_unexpectedly_does_not_block_the_queue(g2bot, monkeypatch, caplog) -> None:
    """Guard of the new queued edit: a bug in it never stops the delivery task; the outcome goes out as a reply."""
    n, client, _ = g2bot
    answer_in_time(monkeypatch)
    conf = make_conf()
    conf.bot_message_id = q = await asyncio.wait_for(n.ask_confirmation(conf), GUARD)
    client.link.clear()
    caplog.set_level(logging.ERROR, logger="copytrader.telegram.notifier")
    await timed(n.notify("rapor"))
    await timed(n.close_confirmation(conf, "✅ İşlem açıldı"))
    await timed(n.notify("sonra"))
    assert [it.edit is not None for it in n._queue] == [False, True, False]      # the edit waits in its place

    def broken(self: Any, task: Any, msg_id: int) -> bool:
        raise RuntimeError("unexpected")

    monkeypatch.setattr(BotNotifier, "_edit_answered", broken)
    client.link.set()
    await until(lambda: not n._queue and len(client.sent) == 4)
    assert client.texts[1:] == ["ℹ️ rapor", "<b>Sonuç:</b> ✅ İşlem açıldı", "ℹ️ sonra"]
    assert client.sent[2].reply_to == q and "A queued edit of bot message" in caplog.text


async def test_g2_outcome_edit_whose_caller_is_cancelled_is_still_done_once(g2bot, monkeypatch) -> None:
    n, client, _ = g2bot
    answer_in_time(monkeypatch)
    conf = make_conf()
    conf.bot_message_id = q = await asyncio.wait_for(n.ask_confirmation(conf), GUARD)
    client.link.clear()
    caller = asyncio.ensure_future(n.close_confirmation(conf, "✅ İşlem açıldı"))
    await asyncio.sleep(WAIT / 4)
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    client.link.set()
    await until(lambda: not n._queue and len(client.edits) == 1)
    await asyncio.sleep(0.1)
    assert client.edits[0].msg_id == q and client.edit_calls == 1 and len(client.sent) == 1


async def test_g2_healthy_link_sends_what_62e0719_sent(g2bot) -> None:
    """Guard (passes on 62e0719 too): on a link that answers, every call returns with its message delivered, the
    same requests in the same order, and no background work is started."""
    n, client, engine = g2bot
    await n.notify("bilgi")
    conf = make_conf()
    conf.bot_message_id = await n.ask_confirmation(conf)
    await n.notify("uyarı", Severity.WARNING)
    await n.close_confirmation(conf, "✅ İşlem açıldı")
    other = make_conf("c2")
    await n.close_confirmation(other, "⌛ Süre doldu, işlem açılmadı.")     # never asked: a message of its own
    await n.notify("hata", Severity.ERROR)
    assert [(s.id, s.text, s.reply_to) for s in client.sent] == [
        (101, "ℹ️ bilgi", None), (102, format_confirmation(conf), None), (103, "⚠️ uyarı", None),
        (104, f"{UNDELIVERED_QUESTION}\n\n{format_confirmation(other, with_expiry=False)}\n\n"
              "<b>Sonuç:</b> ⌛ Süre doldu, işlem açılmadı.", None),
        (105, "🚨 hata", None)]
    assert conf.bot_message_id == 102 and buttons_of(client.sent[1]) == [b"ok:c1", b"fix:c1", b"no:c1"]
    assert [(e.msg_id, e.text, e.buttons) for e in client.edits] == [
        (102, format_confirmation(conf, with_expiry=False) + "\n\n<b>Sonuç:</b> ✅ İşlem açıldı", None)]
    assert client.done == [("send", "ℹ️ bilgi"), ("send", client.sent[1].text), ("send", "⚠️ uyarı"),
                           ("edit", 102), ("send", client.sent[3].text), ("send", "🚨 hata")]
    assert not n._queue and n._pump_task is None and n._sup_task is None
    assert len(client.send_calls) == 5 and client.edit_calls == 1


async def test_g2_silent_owner_bot_link_holds_the_open_flow_for_the_short_bound_only(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Production constants and the server's config: the owner bot's link goes silent, the trader posts a BTC
    market signal (kripto: binance, breakout, tickmill) and 0.3 s later an EURUSD signal (fx: fx, tickmill). The
    first flow ends after about CALLER_WAIT_SEC (62e0719: SEND_TIMEOUT_SEC = 20 s, while it held the open lock), the
    fx order goes out right after it, and every notice reaches the owner once, in order, when the link answers."""
    assert notifier_mod.SEND_TIMEOUT_SEC == 20.0
    bound = getattr(notifier_mod, "CALLER_WAIT_SEC", None)      # (only printed; 62e0719 has none)
    limit = 3.5                  # "about 3 s": CALLER_WAIT_SEC = 2.5 s plus the order flows themselves
    p, bot = await replica(tmp_path, monkeypatch)
    client = bot.client
    stamps: dict[str, float] = {}
    t0 = time.monotonic()
    original_create = p.binance.create_order

    async def binance_create(symbol: str, type: str, side: str, amount: float, price: float | None = None,
                             params: dict | None = None) -> Any:
        params = dict(params or {})
        kind = "stop" if params.get("stopLossPrice") else ("tp" if params.get("takeProfitPrice") else "entry")
        stamps.setdefault(f"binance_{kind}", time.monotonic() - t0)
        return await original_create(symbol, type, side, amount, price, params)

    p.binance.create_order = binance_create                         # type: ignore[method-assign]

    def fx_hook(request: dict[str, Any]) -> None:
        if request.get("action") == MT5.TRADE_ACTION_DEAL and "position" not in request:
            stamps.setdefault("fx_entry", time.monotonic() - t0)
        return None

    p.terminal.hooks.append(fx_hook)

    async def flow(name: str, coro: Any) -> None:
        stamps[f"{name}_posted"] = time.monotonic() - t0
        await coro
        stamps[f"{name}_done"] = time.monotonic() - t0

    bot.handed.clear()
    client.sent.clear()
    client.send_calls.clear()
    try:
        client.link.clear()          # the owner bot's Telegram link goes silent (Telethon still says "connected")
        s1 = asyncio.ensure_future(flow("s1", p.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO,
                                                    "btc long piyasa")))
        await asyncio.sleep(0.3)
        s2 = asyncio.ensure_future(flow("s2", p.say(fx_pair(*EURUSD_SHORT), FX, "eurusd short")))
        await asyncio.wait_for(asyncio.gather(s1, s2), 60)
        silent_texts = list(client.texts)
        down = bot._down_since
        client.link.set()            # Telegram answers again (before SEND_TIMEOUT_SEC)
        await until(lambda: not bot._queue and len(client.sent) >= len(bot.handed))
        await asyncio.sleep(0.2)
        trades = await p.trades()
    finally:
        client.link.set()
        await p.close()
        await asyncio.wait_for(bot.stop(), GUARD)
    timeline = {k: round(v, 2) for k, v in sorted(stamps.items(), key=lambda kv: kv[1])}
    print(f"\nG2 silent owner-bot link, CALLER_WAIT_SEC={bound}: {timeline}")
    assert stamps["s1_done"] - stamps["s1_posted"] <= limit, timeline            # 62e0719: about 20 s
    assert stamps["fx_entry"] - stamps["s2_posted"] <= limit, timeline           # the next signal's order
    assert stamps["binance_stop"] - stamps["binance_entry"] < 1.0, timeline      # (stop with the market entry)
    assert silent_texts == [] and down is None                 # nothing reached the owner, nothing given up yet
    assert [t.status.value for t in trades] == ["open", "open"]
    assert len(bot.handed) >= 3, bot.handed                    # breakout's instruction and the two reports
    assert client.texts == bot.handed                          # every notice once, in the order handed over
    assert len(client.send_calls) == len(client.sent)          # no request was made twice
