"""Pre-flight batch 3: the owner bot must never block the engine and must not lose messages.

NO1  every notifier call is bounded; what cannot be sent is queued and delivered in order
NO2  the bot client keeps reconnecting; a supervisor reconnects a disconnected client and the owner
     is told how long the bot was unreachable and how many notices were late
NO3  a question that could not be sent is asked again; an expired, never delivered question names its trade
NO4  /durum also shows the bot's own connection state and the number of waiting notices
NO5  stop() delivers what is queued within the grace period and never hangs

The client here is a double that behaves like Telethon with ``connection_retries=None``: while the
connection is silent a request is not answered, and it is answered (once) when the connection is back.
"""

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from telethon import TelegramClient, errors

from copytrader.config import NotifierConfig, TelegramConfig
from copytrader.models import (
    ActionType,
    ConfirmationStatus,
    EntryType,
    ParsedAction,
    PendingConfirmation,
    Severity,
    Side,
    TakeProfit,
    TradePlan,
)
from copytrader.telegram import notifier as notifier_mod
from copytrader.telegram.notifier import BotNotifier

OWNER = 777
BOUND = 0.3            # SEND_TIMEOUT_SEC in these tests
AT_ONCE = 0.2          # "did not wait for Telegram" (clearly under the bound, with room for a busy machine)
GUARD = 3.0            # a call that takes longer than this hangs (the old behaviour): fail, do not block the suite


class FakeClient:
    """Telethon bot client double.

    ``link`` cleared = connected on paper but silent (network outage while Telethon keeps retrying):
    requests wait and are answered, in the order they were made, once ``link`` is set again.
    ``connected`` False = Telethon gave up / was disconnected: requests fail at once, ``start()`` reconnects.
    ``script`` = per send_message call: an exception to raise, "lost" (never answered) or None."""

    def __init__(self) -> None:
        self.sent: list[SimpleNamespace] = []
        self.edits: list[SimpleNamespace] = []
        self.next_id = 100
        self.link = asyncio.Event()
        self.link.set()
        self.connected = True
        self.can_connect = True
        self.script: list[Any] = []
        self.delay = 0.0                  # seconds every send takes (a slow line)
        self.send_calls: list[float] = []
        self.me_calls = 0
        self.starts: list[float] = []
        self.disconnects = 0
        self.start_hangs = False
        self.disconnect_hangs = False
        self.handlers: list[Any] = []
        self.parse_mode = None

    def is_connected(self) -> bool:
        return self.connected

    async def _answerable(self) -> None:
        if not self.connected:
            raise ConnectionError("Cannot send requests while disconnected")
        await self.link.wait()

    async def send_message(self, entity, message, **kw):
        self.send_calls.append(time.monotonic())
        step = self.script.pop(0) if self.script else None
        if isinstance(step, BaseException):
            raise step
        if step == "lost":
            await asyncio.Event().wait()
        await self._answerable()
        if self.delay:
            await asyncio.sleep(self.delay)
        self.next_id += 1
        self.sent.append(SimpleNamespace(entity=entity, text=message, id=self.next_id, **kw))
        return SimpleNamespace(id=self.next_id)

    async def edit_message(self, entity, message, text, **kw):
        await self._answerable()
        self.edits.append(SimpleNamespace(entity=entity, msg_id=message, text=text, **kw))

    async def get_messages(self, entity, ids=None):
        await self._answerable()
        return None

    async def get_me(self):
        self.me_calls += 1
        await self._answerable()
        return SimpleNamespace(bot=True, id=123, username="copybot")

    async def start(self, bot_token=None):
        self.starts.append(time.monotonic())
        if self.start_hangs:
            await asyncio.Event().wait()
        if not self.can_connect:
            raise ConnectionError("Connection to Telegram failed")
        self.connected = True
        self.link.set()

    async def disconnect(self):
        self.disconnects += 1
        if self.disconnect_hangs:
            await asyncio.Event().wait()
        self.connected = False

    def add_event_handler(self, callback, event=None):
        self.handlers.append(event)

    @property
    def texts(self) -> list[str]:
        return [s.text for s in self.sent]


class FakeEngine:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    async def handle_confirmation(self, conf_id: str, approved: bool) -> str:
        self.calls.append(("confirm", conf_id, approved))
        return "✅ İşlem açıldı" if approved else "❌ İptal edildi"

    async def request_correction(self, conf_id: str) -> str:
        return ""

    async def handle_correction(self, conf_id: str, correction_text: str) -> str:
        return ""

    async def awaiting_correction_id(self) -> str | None:
        return None

    async def status_text(self) -> str:
        return "durum: çalışıyor"

    async def close_all(self) -> str:
        self.calls.append(("close_all",))
        return "hepsi kapatıldı"


@dataclass
class MsgEvent:
    raw_text: str
    sender_id: int = OWNER
    is_private: bool = True
    out: bool = False
    reply_to_msg_id: int | None = None


@dataclass
class CbEvent:
    data: bytes
    sender_id: int = OWNER
    message_id: int = 101
    answers: list = field(default_factory=list)
    hang: bool = False

    async def answer(self, message=None, cache_time=0, *, url=None, alert=False):
        if self.hang:
            await asyncio.Event().wait()
        self.answers.append((message, alert))


FAST = {
    "SEND_TIMEOUT_SEC": BOUND, "RETRY_BASE_SEC": 0.02, "RETRY_MAX_SEC": 0.1, "FLOOD_MARGIN_SEC": 0.0,
    "STUCK_SEND_SEC": 0.1, "FLUSH_WAIT_SEC": 0.5, "SUPERVISE_EVERY_SEC": 0.02, "IDLE_RECHECK_SEC": 0.05,
    "RECONNECT_BASE_SEC": 0.02, "RECONNECT_MAX_SEC": 0.16, "RECONNECT_TRY_SEC": 0.3,
    "FORCE_RECONNECT_AFTER_SEC": 60.0, "START_TIMEOUT_SEC": 0.3, "DISCONNECT_TIMEOUT_SEC": 0.2,
    "STOP_GRACE_SEC": 1.0, "STOP_FLUSH_MIN_SEC": 0.2,
}


@pytest.fixture
async def setup(tmp_path, monkeypatch):
    for name, value in FAST.items():
        monkeypatch.setattr(notifier_mod, name, value, raising=False)
    n = BotNotifier(
        NotifierConfig(bot_token="123:abc", owner_user_id=OWNER),
        TelegramConfig(api_id=1, api_hash="x", source_chats=[-100]),
        str(tmp_path),
    )
    client, engine = FakeClient(), FakeEngine()
    n._client = client
    n._engine = engine
    yield n, client, engine
    client.disconnect_hangs = False
    client.link.set()
    await asyncio.wait_for(n.stop(), GUARD)


def make_conf(conf_id: str = "c1", minutes: float = 10, bot_message_id: int | None = None) -> PendingConfirmation:
    now = datetime.now(timezone.utc)
    action = ParsedAction(
        action=ActionType.OPEN, base_asset="ETH", quote_asset="USDT", side=Side.LONG, entry_price=2232.18,
        stop_loss=2205.11, take_profits=[TakeProfit(price=2310.6)], risk_r=1.0, confidence=0.7,
    )
    plan = TradePlan(
        plan_id="p", base_asset="ETH", quote_asset="USDT", side=Side.LONG, entry_type=EntryType.MARKET,
        entry_price=2232.18, stop_loss=2205.11, take_profits=[TakeProfit(price=2310.6)], risk_r=1.0,
        risk_pct=2.0, source_chat_id=-100, source_message_ids=[5], created_at=now,
    )
    return PendingConfirmation(
        id=conf_id, kind="open", action=action, plan=plan, source_chat_id=-100, source_message_ids=[5],
        created_at=now, expires_at=now + timedelta(minutes=minutes), bot_message_id=bot_message_id,
    )


async def timed(awaitable) -> float:
    """Seconds the call took; a call that hangs fails after GUARD instead of blocking the test run."""
    started = time.monotonic()
    await asyncio.wait_for(awaitable, GUARD)
    return time.monotonic() - started


async def guarded(awaitable) -> Any:
    """Await a notifier call; if it hangs (the old behaviour) fail after GUARD instead of blocking the run."""
    return await asyncio.wait_for(awaitable, GUARD)


async def until(predicate, timeout: float = GUARD) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached in time"
        await asyncio.sleep(0.005)


def buttons_of(sent) -> list[bytes]:
    return [b.type.data for row in (sent.buttons or []) for b in row]


# --------------------------------------------------------------------------- NO1
async def test_no1_notify_is_bounded_when_telegram_never_answers_and_nothing_is_lost_or_doubled(setup):
    n, client, _ = setup
    client.link.clear()                               # outage: Telethon keeps retrying, nothing is answered
    took = await timed(n.notify("birinci", Severity.ERROR))
    assert BOUND * 0.9 <= took < BOUND + 0.5          # waited the bound once, then returned
    assert n._backlog == ["🚨 birinci"]               # kept, not dropped
    # The connection is now known to be down: nobody waits any more.
    assert await timed(n.notify("ikinci", Severity.WARNING)) < AT_ONCE
    assert await timed(n.notify("üçüncü")) < AT_ONCE
    assert client.texts == [] and len(n._backlog) == 3
    client.link.set()                                 # network is back
    await until(lambda: not n._backlog)
    assert client.texts[:3] == ["🚨 birinci", "⚠️ ikinci", "ℹ️ üçüncü"]      # in order, each exactly once
    assert len(client.send_calls) == len(client.texts)                       # the first one was not sent twice


async def test_no1_normal_case_is_sent_directly_and_delivered_when_notify_returns(setup):
    n, client, _ = setup
    await n.notify("bilgi")
    await n.notify("hata", Severity.ERROR)
    assert client.texts == ["ℹ️ bilgi", "🚨 hata"]    # delivered when notify() returned, text unchanged
    assert list(n._backlog) == []                     # nothing waits, and no background work was started:
    assert getattr(n, "_pump_task", None) is None and getattr(n, "_sup_task", None) is None


@pytest.mark.parametrize(
    "error",
    [
        ConnectionError("Cannot send requests while disconnected"),
        asyncio.TimeoutError(),
        errors.RpcCallFailError(request=None),        # Telegram server error
        RuntimeError("boom"),                         # anything unexpected
    ],
    ids=["connection", "timeout", "server", "unknown"],
)
async def test_no1_send_that_raises_is_queued_and_delivered_later_in_order(setup, error):
    n, client, _ = setup
    client.script = [error]
    assert await timed(n.notify("işlem açılamadı", Severity.ERROR)) < BOUND
    await n.notify("sonraki")
    await until(lambda: len(client.texts) >= 2)
    assert client.texts[:2] == ["🚨 işlem açılamadı", "ℹ️ sonraki"]


async def test_no1_message_telegram_refuses_for_good_is_dropped_and_does_not_block_the_queue(setup, caplog):
    n, client, _ = setup
    client.script = [errors.MessageTooLongError(request=None)]
    with caplog.at_level(logging.ERROR):
        await n.notify("reddedilen")
    await n.notify("sonraki")
    assert client.texts == ["ℹ️ sonraki"] and list(n._backlog) == []
    assert "Sending to the owner failed" in caplog.text


async def test_no1_flood_wait_holds_the_queue_without_blocking_callers(setup):
    n, client, _ = setup
    flood = errors.FloodWaitError(request=None, capture=75)
    flood.seconds = 0.3                               # Telegram: "wait this long"
    client.script = [flood]
    started = time.monotonic()
    assert await timed(n.notify("bir", Severity.ERROR)) < AT_ONCE
    assert await timed(n.notify("iki", Severity.ERROR)) < AT_ONCE
    assert len(client.send_calls) == 1                # nothing is sent while Telegram asks us to wait
    await until(lambda: len(client.texts) == 2)
    assert client.texts == ["🚨 bir", "🚨 iki"]
    assert client.send_calls[1] - started >= 0.3      # the next attempt came after the wait, not before


async def test_no1_queue_is_bounded_oldest_info_goes_first_and_it_is_logged(setup, monkeypatch, caplog):
    n, client, _ = setup
    monkeypatch.setattr(notifier_mod, "QUEUE_MAX", 4, raising=False)
    client.connected, client.can_connect = False, False    # the client reports "disconnected"
    with caplog.at_level(logging.WARNING):
        for text, severity in [("i1", Severity.INFO), ("w1", Severity.WARNING), ("i2", Severity.INFO),
                               ("e1", Severity.ERROR)]:
            assert await timed(n.notify(text, severity)) < AT_ONCE
        assert n._backlog == ["ℹ️ i1", "⚠️ w1", "ℹ️ i2", "🚨 e1"]
        await n.notify("e2", Severity.ERROR)          # full: the oldest INFO goes
        assert n._backlog == ["⚠️ w1", "ℹ️ i2", "🚨 e1", "🚨 e2"]
        await n.notify("e3", Severity.ERROR)          # the next INFO
        assert n._backlog == ["⚠️ w1", "🚨 e1", "🚨 e2", "🚨 e3"]
        await n.notify("i3", Severity.INFO)           # only more important ones wait: the new INFO is not kept
        assert n._backlog == ["⚠️ w1", "🚨 e1", "🚨 e2", "🚨 e3"]
        await n.notify("e4", Severity.ERROR)          # no INFO left: the warning
        await n.notify("e5", Severity.ERROR)          # only errors left: the oldest
        assert n._backlog == ["🚨 e2", "🚨 e3", "🚨 e4", "🚨 e5"]
    for dropped in ("i1", "i2", "i3", "w1", "e1"):
        assert f"dropped the oldest least important one: {_PREFIX[dropped[0]]} {dropped}" in caplog.text
    client.can_connect = True                         # network is back: the supervisor reconnects
    await until(lambda: len(client.texts) >= 5)
    assert client.texts[:4] == ["🚨 e2", "🚨 e3", "🚨 e4", "🚨 e5"]
    assert "5 bildirim gönderilemeden silindi" in client.texts[4]             # the owner is told about the loss


_PREFIX = {"i": "ℹ️", "w": "⚠️", "e": "🚨"}


async def test_no1_edits_and_button_answers_are_bounded_and_the_outcome_is_not_lost(setup):
    n, client, engine = setup
    conf = make_conf()
    conf.bot_message_id = await guarded(n.ask_confirmation(conf))
    assert conf.bot_message_id == client.sent[-1].id
    client.link.clear()                               # outage after the question was delivered
    took = await timed(n.handle_callback(CbEvent(b"ok:c1", message_id=conf.bot_message_id, hang=True)))
    assert took < 2 * BOUND + 0.5                     # answer + edit: each bounded, never a hang
    assert engine.calls == [("confirm", "c1", True)]  # the approval itself was carried out
    assert await timed(n.close_confirmation(make_conf("c2", bot_message_id=55), "⌛ Süre doldu")) < AT_ONCE
    client.link.set()
    await until(lambda: len(client.texts) >= 3)
    assert "Sonuç:" in client.texts[1] and "İşlem açıldı" in client.texts[1]      # the result of the approval
    assert "Süre doldu" in client.texts[2] and client.sent[2].reply_to == 55
    # ...and the two messages the outcomes belong to were edited after all (their buttons are gone):
    assert [e.msg_id for e in client.edits] == [conf.bot_message_id, 55]
    assert "İşlem açıldı" in client.edits[0].text and client.edits[0].buttons is None


async def test_no1_request_that_is_never_answered_is_sent_again_once_the_connection_works(setup):
    n, client, _ = setup
    client.script = ["lost"]                          # this one request is never answered; the connection is fine
    assert await timed(n.notify("kayıp", Severity.ERROR)) < BOUND + 0.5
    await until(lambda: "🚨 kayıp" in client.texts)
    assert client.texts.count("🚨 kayıp") == 1 and len(client.send_calls) == 2


async def test_no1_long_text_on_a_slow_line_holds_the_caller_one_bound_only(setup):
    n, client, _ = setup
    client.delay = 0.8 * BOUND                        # every chunk is answered, but slowly
    long_text = "<b>Rapor</b>\n" + "\n".join(f"satır {i} " + "x" * 50 for i in range(160))
    took = await timed(n.notify(long_text))
    await until(lambda: len(client.sent) == 3 and not n._backlog)
    assert took < BOUND + 0.25                        # not 3 x 0.8 bounds: the rest went out in the background
    assert len(client.send_calls) == 3                # every chunk once, in order
    assert client.texts[0].startswith("ℹ️ Rapor") and client.texts[2].rstrip().endswith("x" * 50)
    assert "kesintisi görülmedi" in n.bot_status_text()    # slow is not "down"


async def test_no1_outage_is_counted_from_the_first_unanswered_request(setup):
    n, client, _ = setup
    client.link.clear()
    started = time.monotonic()
    await guarded(n.notify("ilk", Severity.ERROR))    # returns after the bound...
    await asyncio.sleep(BOUND)
    client.link.set()
    await until(lambda: client.texts == ["🚨 ilk"] and not n._backlog)
    lasted = n._last_outage[1]
    assert lasted >= 1.9 * BOUND and lasted <= time.monotonic() - started    # ...but the outage began before it


async def test_no1_requests_nobody_waits_for_are_taken_out_of_telethons_hands(setup):
    """Telethon keeps a request whose caller gave up and sends it after its next reconnect (seen with the
    real client: a second copy of a notice / an old edit). The notifier removes such requests."""
    n, client, _ = setup
    loop = asyncio.get_running_loop()

    def request_state(given_up: bool) -> SimpleNamespace:
        future = loop.create_future()
        if given_up:
            future.cancel()
        return SimpleNamespace(future=future)

    queued_kept, queued_gone, sent_kept, sent_gone = (request_state(g) for g in (False, True, False, True))
    client._sender = SimpleNamespace(                 # the two places Telethon 1.45 keeps requests in
        _send_queue=SimpleNamespace(_deque=deque([queued_gone, queued_kept])),
        _pending_state={11: sent_gone, 12: sent_kept},
    )
    n._forget_abandoned_requests()
    assert list(client._sender._send_queue._deque) == [queued_kept]
    assert client._sender._pending_state == {12: sent_kept}
    client._sender = object()                         # internals that look different: nothing happens, no error
    n._forget_abandoned_requests()
    del client._sender
    n._forget_abandoned_requests()


async def test_no1_one_message_that_cannot_be_processed_does_not_block_the_queue_for_ever(setup, monkeypatch, caplog):
    n, client, _ = setup
    client.connected, client.can_connect = False, False
    await n.notify("bozuk", Severity.ERROR)
    await n.notify("sağlam", Severity.ERROR)
    real_render = BotNotifier._render

    def render(self, item):
        if "bozuk" in item.text:
            raise RuntimeError("unexpected")          # a bug that hits this one message only
        return real_render(self, item)

    monkeypatch.setattr(BotNotifier, "_render", render, raising=False)
    client.can_connect = True
    with caplog.at_level(logging.ERROR):
        await until(lambda: len(client.texts) >= 2)
    assert client.texts[0] == "🚨 sağlam"             # the queue went on
    assert "1 bildirim gönderilemeden silindi" in client.texts[1]            # and the owner is told about the loss
    assert "could not be processed and is dropped: 🚨 bozuk" in caplog.text


async def test_no1_caller_cancelled_while_waiting_does_not_lose_the_notice(setup):
    n, client, _ = setup
    client.link.clear()
    caller = asyncio.ensure_future(n.notify("iptal edilen çağrı", Severity.ERROR))
    await asyncio.sleep(BOUND / 4)
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    client.link.set()
    await until(lambda: client.texts == ["🚨 iptal edilen çağrı"])


async def test_no1_owner_who_blocked_the_bot_is_not_hammered_and_gets_everything_when_they_write(setup):
    n, client, _ = setup
    blocked = errors.UserIsBlockedError(request=None)
    client.script = [blocked]
    await n.notify("bir", Severity.ERROR)
    await n.notify("iki", Severity.WARNING)
    await asyncio.sleep(0.2)                          # many back-off periods later:
    assert len(client.send_calls) == 1 and len(n._backlog) == 2      # no retry storm, nothing lost
    await n.handle_message(MsgEvent("/durum"))        # the owner writes: they can be reached again
    assert client.texts[:3] == ["🚨 bir", "⚠️ iki", "durum: çalışıyor"]


async def test_no1_start_is_bounded_when_telegram_is_unreachable(setup):
    n, client, engine = setup
    client.start_hangs = True                         # the client would keep trying to connect for ever
    with pytest.raises(ConnectionError, match="bağlanamadı"):
        await asyncio.wait_for(n.start(engine), GUARD)


# --------------------------------------------------------------------------- NO2
async def test_no2_bot_client_is_created_to_reconnect_for_ever(setup, monkeypatch):
    n, _, engine = setup
    created: list[dict[str, Any]] = []

    def fake_telegram_client(*args, **kwargs):
        created.append(kwargs)
        return FakeClient()

    monkeypatch.setattr(notifier_mod, "TelegramClient", fake_telegram_client)
    n._client = None
    await n.start(engine)
    assert len(created) == 1 and "connection_retries" in created[0]
    assert created[0]["connection_retries"] is None   # Telethon: None = retry for ever (default 5 = give up)
    assert n._client.texts == [notifier_mod.STARTUP_TEXT]


async def test_no2_real_telethon_client_is_built_with_endless_reconnect(tmp_path):
    n = BotNotifier(
        NotifierConfig(bot_token="123:abc", owner_user_id=OWNER),
        TelegramConfig(api_id=1, api_hash="x", source_chats=[-100]),
        str(tmp_path),
    )
    client = n._make_client()                         # the real Telethon class; nothing connects here
    try:
        assert isinstance(client, TelegramClient) and not client.is_connected()
        assert client._connection_retries is None     # Telethon's own name for "connection_retries"
        client.flood_sleep_threshold = 0              # the setting start() relies on exists
        assert client.flood_sleep_threshold == 0
    finally:
        client.session.close()


async def test_no2_supervisor_reconnects_with_backoff_delivers_in_order_and_reports_the_outage(setup, monkeypatch):
    n, client, engine = setup
    monkeypatch.setattr(notifier_mod, "LATE_MARK_SEC", 0.05, raising=False)
    monkeypatch.setattr(notifier_mod, "REPORT_MIN_DELAY_SEC", 0.05, raising=False)
    await n.start(engine)
    client.sent.clear()
    client.connected, client.can_connect = False, False    # Telethon gave up: the client is disconnected
    client.starts.clear()
    for text in ("bir", "iki", "üç"):
        assert await timed(n.notify(text, Severity.ERROR)) < AT_ONCE
    await until(lambda: len(client.starts) >= 5)      # the supervisor keeps trying to reconnect...
    gaps = [b - a for a, b in zip(client.starts, client.starts[1:])]
    assert gaps[3] > gaps[0] >= 0.02                  # ...with growing pauses (0.02 s doubling to 0.16 s here)
    assert client.texts == []
    client.can_connect = True                         # network is back
    await until(lambda: len(client.texts) >= 4)
    assert client.connected
    for sent, text in zip(client.texts[:3], ("bir", "iki", "üç")):   # in order, each marked with its real age
        assert sent.startswith("🕒") and "önce oluştu" in sent and sent.endswith(f"🚨 {text}")
    report = client.texts[3]
    assert "Telegram'a bağlanamadı" in report and "3 bildirim gecikti" in report


async def test_no2_owner_is_told_about_a_long_outage_even_when_nothing_was_delayed(setup, monkeypatch):
    n, client, engine = setup
    monkeypatch.setattr(notifier_mod, "REPORT_MIN_OUTAGE_SEC", 0.1, raising=False)
    await n.start(engine)
    client.sent.clear()
    client.connected, client.can_connect = False, False
    await asyncio.sleep(0.3)
    client.can_connect = True
    await until(lambda: len(client.texts) >= 1)
    assert "Telegram'a bağlanamadı" in client.texts[0] and "geciken bildirim olmadı" in client.texts[0]


async def test_no2_connected_but_silent_for_long_is_reconnected_from_scratch(setup, monkeypatch):
    n, client, _ = setup
    monkeypatch.setattr(notifier_mod, "FORCE_RECONNECT_AFTER_SEC", 0.3, raising=False)
    client.link.clear()                               # "connected" but nothing is answered, and it stays so
    await guarded(n.notify("sessiz bağlantı", Severity.ERROR))
    await until(lambda: client.disconnects >= 1 and len(client.starts) >= 1)   # dropped and built anew
    await until(lambda: "🚨 sessiz bağlantı" in client.texts)
    assert client.texts.count("🚨 sessiz bağlantı") == 1


async def test_no2_client_that_start_refused_is_never_reconnected_or_used(setup):
    n, client, engine = setup

    async def somebody_else():
        return SimpleNamespace(bot=False, id=999, username="me")      # bot.session of another account

    client.get_me = somebody_else
    with pytest.raises(RuntimeError, match="bot.session"):
        await n.start(engine)
    assert not client.connected and len(client.starts) == 1
    assert await timed(n.notify("Bot durdu", Severity.ERROR)) < AT_ONCE
    await asyncio.sleep(0.15)                         # the supervisor must not bring this client back
    assert len(client.starts) == 1 and client.texts == [] and list(n._backlog) == []


# --------------------------------------------------------------------------- NO3
async def test_no3_question_whose_send_got_no_answer_arrives_after_the_outage_and_can_be_answered(setup):
    n, client, engine = setup
    client.link.clear()
    conf = make_conf()
    started = time.monotonic()
    assert await asyncio.wait_for(n.ask_confirmation(conf), GUARD) is None    # not delivered: the engine goes on
    assert time.monotonic() - started < BOUND + 0.5
    client.link.set()
    await until(lambda: len(client.sent) == 1 and not n._backlog)
    question = client.sent[0]
    assert buttons_of(question) == [b"ok:c1", b"fix:c1", b"no:c1"] and "AÇAYIM MI" in question.text
    await guarded(n.handle_callback(CbEvent(b"ok:c1", message_id=question.id)))   # the owner can still approve it
    assert engine.calls == [("confirm", "c1", True)]
    assert client.edits[-1].msg_id == question.id and "İşlem açıldı" in client.edits[-1].text
    assert len(client.sent) == 1                      # asked once


async def test_no3_question_is_asked_again_after_reconnect_with_the_time_that_is_left(setup, monkeypatch):
    n, client, engine = setup
    monkeypatch.setattr(notifier_mod, "LATE_MARK_SEC", 0.05, raising=False)
    client.connected, client.can_connect = False, False
    conf = make_conf(minutes=4)
    assert await timed(n.ask_confirmation(conf)) < AT_ONCE
    assert client.texts == []
    await asyncio.sleep(0.1)
    client.can_connect = True
    await until(lambda: len(client.sent) >= 1 and not n._backlog)
    question = client.sent[0]
    assert buttons_of(question) == [b"ok:c1", b"fix:c1", b"no:c1"]
    assert question.text.startswith("🕒") and "gecikmeyle ulaştı" in question.text
    assert "4 dk içinde yanıt gelmezse" in question.text
    # The engine never got a message id (conf.bot_message_id stays None) and closes it later:
    conf.status = ConfirmationStatus.REJECTED
    await n.close_confirmation(conf, "❌ İptal edildi")
    assert client.edits[-1].msg_id == question.id and "İptal edildi" in client.edits[-1].text
    assert all(not buttons_of(s) for s in client.sent[1:])                    # no second question


async def test_no3_expired_question_that_never_went_out_names_the_trade(setup):
    n, client, _ = setup
    client.connected, client.can_connect = False, False
    conf = make_conf()
    assert await n.ask_confirmation(conf) is None
    conf.status = ConfirmationStatus.EXPIRED          # the engine's expiry timer fired during the outage
    await n.close_confirmation(conf, "⌛ Süre doldu, işlem açılmadı.")
    client.can_connect = True
    await until(lambda: len(client.sent) >= 1)
    await asyncio.sleep(0.1)
    told = client.sent[0]
    assert "ulaştırılamadı" in told.text
    assert "ETH" in told.text and "LONG" in told.text and "2205.11" in told.text      # which trade it was
    assert "Süre doldu, işlem açılmadı" in told.text
    assert all(not buttons_of(s) for s in client.sent)                        # the dead question is not asked


async def test_no3_reasked_question_that_never_went_out_is_not_mixed_up_with_its_older_version(setup):
    n, client, _ = setup
    first = make_conf()
    first.bot_message_id = await n.ask_confirmation(first)             # version 1 reached the owner
    client.connected, client.can_connect = False, False
    again = make_conf()                                                # corrected and asked again, same id
    assert await n.ask_confirmation(again) is None                     # ...but this one never goes out
    again.status = ConfirmationStatus.EXPIRED
    await n.close_confirmation(again, "⌛ Süre doldu, işlem açılmadı.")
    client.can_connect = True
    await until(lambda: any("ulaştırılamadı" in t for t in client.texts))
    await asyncio.sleep(0.1)
    told = next(t for t in client.texts if "ulaştırılamadı" in t)
    assert "ETH" in told and "Süre doldu, işlem açılmadı" in told
    assert client.edits == []                                          # version 1's message is not touched
    assert sum(1 for s in client.sent if buttons_of(s)) == 1           # and version 2 is not asked afterwards


async def test_no3_question_with_no_time_left_is_not_asked_any_more(setup, monkeypatch):
    n, client, _ = setup
    monkeypatch.setattr(notifier_mod, "LATE_MARK_SEC", 0.05, raising=False)
    client.connected, client.can_connect = False, False
    assert await n.ask_confirmation(make_conf(minutes=0.1)) is None           # 6 s left: under the minimum
    await n.notify("sonraki", Severity.ERROR)
    await asyncio.sleep(0.1)
    client.can_connect = True
    await until(lambda: len(client.sent) >= 1)
    await asyncio.sleep(0.1)
    assert all(not buttons_of(s) for s in client.sent) and client.texts[0].endswith("🚨 sonraki")


async def test_no3_question_closed_while_on_its_way_loses_its_buttons_when_it_arrives(setup):
    n, client, _ = setup
    client.link.clear()
    conf = make_conf()
    assert await asyncio.wait_for(n.ask_confirmation(conf), GUARD) is None
    conf.status = ConfirmationStatus.EXPIRED
    await guarded(n.close_confirmation(conf, "⌛ Süre doldu, işlem açılmadı."))
    client.link.set()                                 # Telethon now delivers the old request after all
    await until(lambda: len(client.edits) >= 1)
    arrived = next(s for s in client.sent if buttons_of(s))
    assert client.edits[0].msg_id == arrived.id and client.edits[0].buttons is None
    assert "Süre doldu, işlem açılmadı" in client.edits[0].text
    await until(lambda: any("ulaştırılamadı" in t for t in client.texts))     # and the owner is told which one


# --------------------------------------------------------------------------- NO4
async def test_no4_durum_shows_the_bots_own_connection_and_waiting_notices(setup, monkeypatch):
    n, client, engine = setup
    await n.handle_message(MsgEvent("/durum"))
    assert client.texts[0] == "durum: çalışıyor"      # the engine's status, unchanged
    own = client.texts[1]
    assert "Bildirim botu" in own and "Telegram bağlantısı: var" in own
    assert "bekleyen bildirim: yok" in own and "kesintisi görülmedi" in own

    await n.start(engine)
    client.connected, client.can_connect = False, False
    await n.notify("bekleyen", Severity.ERROR)
    down = n.bot_status_text()
    assert "Telegram bağlantısı: <b>YOK</b>" in down and "bekleyen bildirim: <b>1</b>" in down
    client.can_connect = True
    await until(lambda: "🚨 bekleyen" in client.texts)
    client.sent.clear()
    await n.handle_message(MsgEvent("/durum"))
    own = client.texts[-1]
    assert "Telegram bağlantısı: var" in own and "Son bağlantı kesintisi" in own and "1 kesinti" in own


# --------------------------------------------------------------------------- NO5
async def test_no5_stop_delivers_what_is_still_queued_then_disconnects(setup, monkeypatch):
    n, client, _ = setup
    monkeypatch.setattr(notifier_mod, "RETRY_BASE_SEC", 30.0, raising=False)   # the queue would wait this long
    client.script = [errors.RpcCallFailError(request=None), errors.RpcCallFailError(request=None)]
    await n.notify("bir", Severity.ERROR)
    await n.notify("iki", Severity.ERROR)
    await n.notify("🛑 Kopya-trade botu durduruldu.")
    await asyncio.sleep(0.05)
    assert client.texts == [] and len(n._backlog) == 3
    assert await timed(n.stop()) < 2.0                # at once, not after the 30 s back-off
    assert client.texts == ["🚨 bir", "🚨 iki", "ℹ️ 🛑 Kopya-trade botu durduruldu."]
    assert client.disconnects == 1 and not client.connected


async def test_no5_stop_never_hangs_and_logs_what_it_could_not_deliver(setup, caplog):
    n, client, _ = setup
    client.link.clear()                               # Telegram is silent and stays silent
    await guarded(n.notify("ulaşmayan", Severity.ERROR))
    await guarded(n.notify("ikinci ulaşmayan", Severity.ERROR))
    client.disconnect_hangs = True                    # even the disconnect gets no answer
    with caplog.at_level(logging.ERROR):
        took = await timed(n.stop())
    assert took < FAST["STOP_GRACE_SEC"] + FAST["DISCONNECT_TIMEOUT_SEC"] + 1.0
    assert "NOT delivered before shutdown" in caplog.text and "ulaşmayan" in caplog.text
    assert await timed(n.notify("kapandıktan sonra")) < AT_ONCE               # afterwards: no hang, no error
    assert client.texts == []


async def test_close_all_prompt_that_goes_out_late_still_works(setup):
    n, client, engine = setup
    client.connected, client.can_connect = False, False
    await n.handle_message(MsgEvent("/hepsini_kapat"))
    client.can_connect = True
    await until(lambda: any(buttons_of(s) for s in client.sent) and not n._backlog)
    prompt = next(s for s in client.sent if buttons_of(s))
    await n.handle_callback(CbEvent(b"closeall:yes", message_id=prompt.id))
    assert engine.calls == [("close_all",)]
