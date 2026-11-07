import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from telethon import errors
from telethon.extensions import html as tl_html

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
from copytrader.telegram.formatting import CORRECTION_HINT
from copytrader.telegram.notifier import BotNotifier

OWNER = 777
STRANGER = 666
NOW = datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)


class FakeBotClient:
    def __init__(self) -> None:
        self.sent: list[SimpleNamespace] = []
        self.edits: list[SimpleNamespace] = []
        self.next_id = 100
        self.fail_send: Exception | None = None
        self.fail_edit: Exception | None = None

    async def send_message(self, entity, message, **kw):
        if self.fail_send is not None:
            raise self.fail_send
        self.next_id += 1
        self.sent.append(SimpleNamespace(entity=entity, text=message, id=self.next_id, **kw))
        return SimpleNamespace(id=self.next_id)

    async def edit_message(self, entity, message, text, **kw):
        if self.fail_edit is not None:
            raise self.fail_edit
        self.edits.append(SimpleNamespace(entity=entity, msg_id=message, text=text, **kw))

    async def get_messages(self, entity, ids=None):
        return None


class FakeEngine:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.awaiting: str | None = None
        self.notifier: BotNotifier | None = None
        self.close_inside: PendingConfirmation | None = None
        self.gate: asyncio.Event | None = None

    async def handle_confirmation(self, conf_id: str, approved: bool) -> str:
        self.calls.append(("confirm", conf_id, approved))
        if self.gate is not None:
            await self.gate.wait()
        if self.close_inside is not None and self.notifier is not None:
            await self.notifier.close_confirmation(self.close_inside, "✅ İşlem açıldı")
        return "✅ İşlem açıldı" if approved else "❌ İptal edildi"

    async def request_correction(self, conf_id: str) -> str:
        self.calls.append(("fix", conf_id))
        self.awaiting = conf_id
        return ""

    async def handle_correction(self, conf_id: str, correction_text: str) -> str:
        self.calls.append(("correction", conf_id, correction_text))
        return "🔁 Yeniden soruldu"

    async def awaiting_correction_id(self) -> str | None:
        return self.awaiting

    async def status_text(self) -> str:
        self.calls.append(("status",))
        return "durum: çalışıyor"

    async def positions_text(self) -> str:
        self.calls.append(("positions",))
        return "pozisyon yok"

    async def balances_text(self) -> str:
        self.calls.append(("balances",))
        return "bakiye 1000"

    async def set_paused(self, paused: bool) -> str:
        self.calls.append(("paused", paused))
        return "durduruldu" if paused else "devam"

    async def close_all(self) -> str:
        self.calls.append(("close_all",))
        return "hepsi kapatıldı"

    async def test_parse(self, text: str) -> str:
        self.calls.append(("test", text))
        return "anlaşılan: <b>ETH LONG</b>"


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

    async def answer(self, message=None, cache_time=0, *, url=None, alert=False):
        self.answers.append((message, alert))


@pytest.fixture
def setup(tmp_path):
    n = BotNotifier(
        NotifierConfig(bot_token="123:abc", owner_user_id=OWNER),
        TelegramConfig(api_id=1, api_hash="x", source_chats=[-100]),
        str(tmp_path),
    )
    client, engine = FakeBotClient(), FakeEngine()
    engine.notifier = n
    n._client = client
    n._engine = engine
    return n, client, engine


def make_conf(plan: bool = True, conf_id: str = "c1") -> PendingConfirmation:
    action = ParsedAction(
        action=ActionType.OPEN, base_asset="ETH", quote_asset="USDT", side=Side.LONG, entry_price=2232.18,
        stop_loss=2205.11 if plan else None, take_profits=[TakeProfit(price=2310.6)], risk_r=1.0, confidence=0.7,
    )
    tp = TradePlan(
        plan_id="p", base_asset="ETH", quote_asset="USDT", side=Side.LONG, entry_type=EntryType.MARKET,
        entry_price=2232.18, stop_loss=2205.11, take_profits=[TakeProfit(price=2310.6)], risk_r=1.0,
        risk_pct=2.0, source_chat_id=-100, source_message_ids=[5], created_at=NOW,
    )
    return PendingConfirmation(
        id=conf_id, kind="open", action=action, plan=tp if plan else None, source_chat_id=-100,
        source_message_ids=[5], created_at=NOW, expires_at=NOW + timedelta(minutes=10),
    )


def button_data(sent) -> list[bytes]:
    return [b.type.data for row in sent.buttons for b in row]


# --------------------------------------------------------------------------- #
async def test_ask_confirmation_buttons(setup):
    n, client, _ = setup
    msg_id = await n.ask_confirmation(make_conf(plan=True))
    sent = client.sent[-1]
    assert msg_id == sent.id and sent.entity == OWNER and sent.parse_mode == "html"
    assert button_data(sent) == [b"ok:c1", b"fix:c1", b"no:c1"]
    assert [b.text for row in sent.buttons for b in row] == ["✅ Onayla", "✏️ Düzelt", "❌ İptal"]
    assert "Emin olamadım" in sent.text


async def test_ask_confirmation_without_plan_has_no_approve(setup):
    n, client, _ = setup
    await n.ask_confirmation(make_conf(plan=False, conf_id="c2"))
    assert button_data(client.sent[-1]) == [b"fix:c2", b"no:c2"]


async def test_approve_routes_to_engine_and_closes_message(setup):
    n, client, engine = setup
    msg_id = await n.ask_confirmation(make_conf())
    ev = CbEvent(b"ok:c1", message_id=msg_id)
    await n.handle_callback(ev)
    assert engine.calls == [("confirm", "c1", True)]
    assert ev.answers and "işleniyor" in ev.answers[0][0]
    edit = client.edits[-1]
    assert edit.msg_id == msg_id and edit.buttons is None
    assert "Sonuç:" in edit.text and "İşlem açıldı" in edit.text
    assert "⏳" not in edit.text                     # expiry note removed once closed


async def test_reject_routes_to_engine(setup):
    n, client, engine = setup
    await n.handle_callback(CbEvent(b"no:c1"))
    assert engine.calls == [("confirm", "c1", False)]
    assert "İptal edildi" in client.edits[-1].text


async def test_fix_requests_correction_and_sends_hint(setup):
    n, client, engine = setup
    await n.handle_callback(CbEvent(b"fix:c1"))
    assert engine.calls == [("fix", "c1")]
    assert CORRECTION_HINT in client.sent[-1].text


async def test_engine_closing_itself_is_not_duplicated(setup):
    n, client, engine = setup
    conf = make_conf()
    conf.bot_message_id = await n.ask_confirmation(conf)
    engine.close_inside = conf
    await n.handle_callback(CbEvent(b"ok:c1", message_id=conf.bot_message_id))
    assert len(client.edits) == 1                    # only the engine's close_confirmation
    assert len(client.sent) == 1                     # no duplicate outcome message


async def test_double_click_is_processed_once(setup):
    n, client, engine = setup
    engine.gate = asyncio.Event()
    first = asyncio.ensure_future(n.handle_callback(CbEvent(b"ok:c1")))
    await asyncio.sleep(0.01)
    second = CbEvent(b"ok:c1")
    await n.handle_callback(second)
    assert "Zaten" in second.answers[0][0]
    engine.gate.set()
    await first
    assert engine.calls == [("confirm", "c1", True)]


async def test_non_owner_is_ignored_and_logged_once(setup, caplog):
    n, client, engine = setup
    with caplog.at_level(logging.WARNING):
        ev = CbEvent(b"ok:c1", sender_id=STRANGER)
        await n.handle_callback(ev)
        await n.handle_callback(CbEvent(b"ok:c1", sender_id=STRANGER))
        await n.handle_message(MsgEvent("/hepsini_kapat", sender_id=STRANGER))
    assert engine.calls == [] and client.sent == [] and ev.answers == []
    assert caplog.text.count(str(STRANGER)) == 1


async def test_owner_message_in_group_is_ignored(setup):
    n, client, engine = setup
    await n.handle_message(MsgEvent("/durum", is_private=False))
    assert engine.calls == [] and client.sent == []


@pytest.mark.parametrize(
    ("text", "call"),
    [
        ("/durum", ("status",)),
        ("/status", ("status",)),
        ("/pozisyonlar", ("positions",)),
        ("/positions", ("positions",)),
        ("/bakiye", ("balances",)),
        ("/balance", ("balances",)),
        ("/durdur", ("paused", True)),
        ("/pause", ("paused", True)),
        ("/devam", ("paused", False)),
        ("/resume", ("paused", False)),
        ("/test eth 1R long", ("test", "eth 1R long")),
        ("/test@MyCopyBot btc short\nsl 70000", ("test", "btc short\nsl 70000")),
    ],
)
async def test_commands_route_to_engine(setup, text, call):
    n, client, engine = setup
    await n.handle_message(MsgEvent(text))
    assert engine.calls == [call]
    assert client.sent and client.sent[-1].entity == OWNER


@pytest.mark.parametrize("text", ["/start", "/yardim", "/yardım", "/help"])
async def test_help_commands(setup, text):
    n, client, engine = setup
    await n.handle_message(MsgEvent(text))
    assert "/hepsini_kapat" in client.sent[-1].text and engine.calls == []
    tl_html.parse(client.sent[-1].text)


async def test_unknown_command_and_empty_test(setup):
    n, client, engine = setup
    await n.handle_message(MsgEvent("/<b>x"))
    assert "Bilinmeyen komut" in client.sent[-1].text and "&lt;b&gt;" in client.sent[-1].text
    await n.handle_message(MsgEvent("/test"))
    assert "Kullanım" in client.sent[-1].text and engine.calls == []


async def test_close_all_asks_first_then_runs_once(setup):
    n, client, engine = setup
    await n.handle_message(MsgEvent("/hepsini_kapat"))
    prompt = client.sent[-1]
    assert button_data(prompt) == [b"closeall:yes", b"closeall:no"]
    assert engine.calls == []
    await n.handle_callback(CbEvent(b"closeall:yes", message_id=prompt.id))
    assert engine.calls == [("close_all",)]
    assert client.edits[-1].text == "hepsi kapatıldı"
    again = CbEvent(b"closeall:yes", message_id=prompt.id)
    await n.handle_callback(again)                    # second press: refused
    assert engine.calls == [("close_all",)] and again.answers[0][1] is True


async def test_close_all_no_and_stale_prompt(setup, monkeypatch):
    n, client, engine = setup
    await n.handle_message(MsgEvent("/closeall"))
    prompt = client.sent[-1]
    await n.handle_callback(CbEvent(b"closeall:no", message_id=prompt.id))
    assert engine.calls == [] and "Vazgeçildi" in client.edits[-1].text

    await n.handle_message(MsgEvent("/closeall"))
    prompt = client.sent[-1]
    real = notifier_mod.time.monotonic
    monkeypatch.setattr(notifier_mod.time, "monotonic", lambda: real() + notifier_mod.CLOSEALL_TTL_SEC + 1)
    await n.handle_callback(CbEvent(b"closeall:yes", message_id=prompt.id))
    assert engine.calls == []


async def test_plain_text_correction_flow(setup):
    n, client, engine = setup
    await n.handle_message(MsgEvent("merhaba"))
    assert engine.calls == [] and "/yardim" in client.sent[-1].text
    engine.awaiting = "c9"
    await n.handle_message(MsgEvent("SL 2200 olacak"))
    assert engine.calls == [("correction", "c9", "SL 2200 olacak")]
    assert client.sent[-1].text == "🔁 Yeniden soruldu"


async def test_reply_to_confirmation_is_a_correction(setup):
    n, client, engine = setup
    msg_id = await n.ask_confirmation(make_conf(conf_id="c5"))
    await n.handle_message(MsgEvent("short olacak", reply_to_msg_id=msg_id))
    assert engine.calls == [("fix", "c5"), ("correction", "c5", "short olacak")]


async def test_notify_prefixes_and_split(setup):
    n, client, _ = setup
    await n.notify("bilgi")
    await n.notify("uyarı", Severity.WARNING)
    await n.notify("hata", Severity.ERROR)
    assert [s.text for s in client.sent] == ["ℹ️ bilgi", "⚠️ uyarı", "🚨 hata"]
    client.sent.clear()
    long_text = "<b>Rapor</b>\n" + "\n".join(f"satır {i} " + "x" * 50 for i in range(200))
    await n.notify(long_text)
    assert len(client.sent) > 1
    for chunk in client.sent:
        assert len(chunk.text) <= notifier_mod.MAX_CHUNK
        assert chunk.formatting_entities is not None   # pre-parsed: markup never cut in half


async def test_notify_never_raises_and_backlogs_when_owner_unreachable(setup, caplog):
    n, client, _ = setup
    client.fail_send = errors.UserIsBlockedError(request=None)
    with caplog.at_level(logging.ERROR):
        await n.notify("önemli", Severity.ERROR)
        await n.notify("ikinci", Severity.ERROR)
    assert "press Start" in caplog.text and caplog.text.count("press Start") == 1
    client.fail_send = RuntimeError("network")
    await n.notify("x")                               # still no exception
    client.fail_send = None
    await n.handle_message(MsgEvent("/durum"))        # owner is back: backlog delivered first
    texts = [s.text for s in client.sent]
    assert "🚨 önemli" in texts and "🚨 ikinci" in texts
    assert texts.index("🚨 önemli") < texts.index("durum: çalışıyor")


async def test_close_confirmation_edit_and_fallback(setup):
    n, client, _ = setup
    conf = make_conf()
    conf.bot_message_id = await n.ask_confirmation(conf)
    conf.status = ConfirmationStatus.EXPIRED
    await n.close_confirmation(conf, "")
    edit = client.edits[-1]
    assert edit.msg_id == conf.bot_message_id and edit.buttons is None
    assert "Süresi doldu" in edit.text and "⏳" not in edit.text
    client.fail_edit = RuntimeError("message too old")
    await n.close_confirmation(conf, "⌛ Süre doldu")
    assert client.sent[-1].reply_to == conf.bot_message_id and "Süre doldu" in client.sent[-1].text


async def test_engine_error_is_reported_escaped(setup):
    n, client, engine = setup

    async def boom(conf_id, approved):
        raise RuntimeError("<b>broker</b> down")

    engine.handle_confirmation = boom
    await n.handle_callback(CbEvent(b"ok:c1"))
    assert "&lt;b&gt;broker&lt;/b&gt; down" in client.sent[-1].text
    assert "c1" not in n._inflight


async def test_spawned_handlers_are_tracked(setup):
    n, client, engine = setup
    await n._on_message_event(MsgEvent("/durum"))
    await n.drain(1)
    assert engine.calls == [("status",)]


async def test_reasked_confirmation_can_be_closed_again(setup):
    n, client, engine = setup
    first = await n.ask_confirmation(make_conf(conf_id="c7"))
    await n.handle_callback(CbEvent(b"no:c7", message_id=first))
    second = await n.ask_confirmation(make_conf(conf_id="c7"))
    await n.handle_callback(CbEvent(b"ok:c7", message_id=second))
    assert [e.msg_id for e in client.edits] == [first, second]


class StartableClient(FakeBotClient):
    def __init__(self, me) -> None:
        super().__init__()
        self.me = me
        self.handlers: list[Any] = []
        self.parse_mode = None
        self.disconnected = False

    async def start(self, bot_token=None):
        self.token = bot_token

    async def get_me(self):
        return self.me

    def add_event_handler(self, callback, event=None):
        self.handlers.append(event)

    async def disconnect(self):
        self.disconnected = True


async def test_start_logs_in_as_bot_and_greets_owner(setup):
    n, _, engine = setup
    client = StartableClient(SimpleNamespace(bot=True, id=123, username="copybot"))
    n._client = client
    await n.start(engine)
    assert client.token == "123:abc" and client.parse_mode == "html"
    assert {type(h).__name__ for h in client.handlers} == {"NewMessage", "CallbackQuery"}
    assert client.sent[0].entity == OWNER and "Kopya-trade botu çalışıyor" in client.sent[0].text
    await n.stop()
    assert client.disconnected


async def test_start_refuses_session_of_another_account(setup):
    n, _, engine = setup
    n._client = StartableClient(SimpleNamespace(bot=False, id=999, username="me"))
    with pytest.raises(RuntimeError, match="bot.session"):
        await n.start(engine)


async def test_start_survives_owner_never_pressed_start(setup, caplog):
    n, _, engine = setup
    client = StartableClient(SimpleNamespace(bot=True, id=123, username="copybot"))
    client.fail_send = ValueError("Could not find the input entity for PeerUser(user_id=777)")
    n._client = client
    with caplog.at_level(logging.ERROR):
        await n.start(engine)
    assert "press Start" in caplog.text
