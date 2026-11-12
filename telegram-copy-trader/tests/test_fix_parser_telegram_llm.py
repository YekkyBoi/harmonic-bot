"""Regression tests for audited parser defects F5, F11, F12, F14, F15, F16 (no network)."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import anthropic
import httpx2
import pytest

from copytrader.config import LLMConfig
from copytrader.errors import ParserError
from copytrader.models import ActionType, ParseContext, StoredMessage
from copytrader.parsing import llm_parser
from copytrader.parsing.llm_parser import WIRE_SCHEMA, ClaudeSignalParser

NOW = datetime(2026, 11, 7, 17, 7, 30, tzinfo=timezone.utc)
CHAT = -1001234567890


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def wire(**kw: Any) -> dict[str, Any]:
    base: dict[str, Any] = dict(
        action="none", symbol=None, base_asset=None, quote_asset=None, market_type=None, side=None,
        entry_type=None, entry_price=None, stop_loss=None, take_profits=[], risk_r=None,
        new_stop_loss=None, move_sl_to_entry=False, close_fraction=None, refers_to_message_id=None,
        target_trade_id=None, used_message_ids=[], evidence="", ambiguities=[], confidence=0.9,
        summary_tr="",
    )
    base.update(kw)
    return base


def msg(mid: int, text: str = "") -> StoredMessage:
    return StoredMessage(chat_id=CHAT, message_id=mid, sender_id=42, sender_name="T",
                         date=datetime(2026, 11, 7, 17, 7, mid % 60, tzinfo=timezone.utc), text=text)


def ctx() -> ParseContext:
    return ParseContext(now=NOW)


class Client:
    """Fake AsyncAnthropic: returns a canned response, raises, or hangs."""

    def __init__(self, response: Any = None, exc: BaseException | None = None, hang: float = 0.0) -> None:
        self.response, self.exc, self.hang, self.calls = response, exc, hang, 0
        self.beta = SimpleNamespace(messages=SimpleNamespace(create=self.create))

    async def create(self, **_: Any) -> Any:
        self.calls += 1
        if self.hang:
            await asyncio.sleep(self.hang)
        if self.exc is not None:
            raise self.exc
        return self.response


def response(payload: Any = None, stop_reason: str = "end_turn", stop_details: Any = None) -> Any:
    content = [] if payload is None else [SimpleNamespace(type="text", text=json.dumps(payload))]
    return SimpleNamespace(content=content, stop_reason=stop_reason, stop_details=stop_details,
                           model="claude-opus-5-5")


def parser_for(client: Client, **cfg: Any) -> ClaudeSignalParser:
    return ClaudeSignalParser(LLMConfig(**cfg), client=client)


@pytest.fixture
def no_credentials(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    import os

    for k in [k for k in os.environ if k.startswith("ANTHROPIC_") and k != "ANTHROPIC_BASE_URL"]:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))          # no ~/.config/anthropic profile either


def _req() -> httpx2.Request:
    return httpx2.Request("POST", "https://api.anthropic.com/v1/messages")


# --------------------------------------------------------------------------- #
# F5: bounded parse time, SDK retries capped
# --------------------------------------------------------------------------- #
def test_f5_client_uses_low_retries_and_split_timeouts() -> None:
    p = ClaudeSignalParser(LLMConfig(api_key="sk-test", timeout_sec=120))
    assert p._client.max_retries == llm_parser.SDK_MAX_RETRIES <= 1
    t = p._client.timeout
    assert t.connect == llm_parser.CONNECT_TIMEOUT_SEC and t.read == 120
    assert p._deadline_sec == 120


def test_f5_deadline_never_exceeds_timeout_or_caller_cap() -> None:
    p = ClaudeSignalParser(LLMConfig(api_key="sk-test", timeout_sec=120), deadline_sec=90)
    assert p._deadline_sec == 90
    p = ClaudeSignalParser(LLMConfig(api_key="sk-test", timeout_sec=60), deadline_sec=500)
    assert p._deadline_sec == 60


async def test_f5_overall_deadline_stops_a_stalled_call() -> None:
    client = Client(response(), hang=30.0)
    p = parser_for(client, timeout_sec=0.2)
    t0 = time.monotonic()
    with pytest.raises(ParserError) as ei:
        await p.parse([msg(1, "eth long")], ctx())
    assert time.monotonic() - t0 < 2.0
    assert ei.value.transient is True and "zamanında" in str(ei.value)


async def test_f5_deadline_covers_sdk_retries_through_real_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every HTTP attempt stalls: the whole parse still ends at the deadline (SDK retry included)."""
    attempts: list[float] = []

    async def handler(request: httpx2.Request) -> httpx2.Response:
        attempts.append(time.monotonic())
        await asyncio.sleep(30)
        return httpx2.Response(500)

    real = anthropic.AsyncAnthropic

    def factory(**kw: Any) -> Any:
        return real(**kw, http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)))

    monkeypatch.setattr(llm_parser.anthropic, "AsyncAnthropic", factory)
    p = ClaudeSignalParser(LLMConfig(api_key="sk-test", timeout_sec=0.3))
    t0 = time.monotonic()
    with pytest.raises(ParserError) as ei:
        await p.parse([msg(1, "eth long")], ctx())
    assert time.monotonic() - t0 < 2.0 and len(attempts) == 1
    assert ei.value.transient is True
    await p.aclose()


@pytest.mark.parametrize(
    ("code", "transient"),
    [(404, False), (413, False), (422, False), (408, True), (409, True), (500, True), (529, True)],
)
async def test_f5_status_errors_have_explicit_transient_flag(code: int, transient: bool) -> None:
    exc = anthropic.APIStatusError("boom", response=httpx2.Response(code, request=_req()), body=None)
    with pytest.raises(ParserError) as ei:
        await parser_for(Client(exc=exc)).parse([msg(1, "x")], ctx())
    assert ei.value.transient is transient


@pytest.mark.parametrize("stop_reason", ["refusal", "pause_turn"])       # max_tokens is retried (transient) now
async def test_f5_permanent_failures_are_explicitly_not_transient(stop_reason: str) -> None:
    with pytest.raises(ParserError) as ei:
        await parser_for(Client(response({"actions": [wire()]}, stop_reason=stop_reason))).parse(
            [msg(1, "x")], ctx())
    assert ei.value.transient is False


# --------------------------------------------------------------------------- #
# F11: actions without ids next to an INCOMPLETE that claims some
# --------------------------------------------------------------------------- #
async def test_f11_id_less_action_gets_only_unclaimed_ids_and_is_asked() -> None:
    payload = {"actions": [
        wire(action="move_sl", base_asset="BTC", side="long", move_sl_to_entry=True, used_message_ids=[],
             confidence=0.95, summary_tr="BTC stop girişe"),
        wire(action="incomplete", base_asset="ETH", side="long", stop_loss=2205.11,
             used_message_ids=[602], confidence=0.85),
    ]}
    sl, inc = (await parser_for(Client(response(payload))).parse([msg(601, "btc stop girişe"), msg(602)],
                                                                ctx())).actions
    assert sl.used_message_ids == [601]
    assert sl.ambiguities and sl.confidence <= 0.6           # the split was guessed: ask the owner
    assert inc.used_message_ids == [602] and inc.ambiguities == [] and inc.confidence == 0.85


async def test_f11_never_takes_incomplete_ids_when_others_can_be_shared() -> None:
    payload = {"actions": [
        wire(action="open", base_asset="SOL", side="long", stop_loss=1.0, used_message_ids=[601]),
        wire(action="close", base_asset="BTC", close_fraction=1.0, used_message_ids=[], confidence=0.95),
        wire(action="incomplete", base_asset="ETH", side="long", used_message_ids=[602]),
    ]}
    _o, close, _i = (await parser_for(Client(response(payload))).parse([msg(601, "a"), msg(602)],
                                                                      ctx())).actions
    assert close.used_message_ids == [601] and close.ambiguities and close.confidence <= 0.6


async def test_f11_unknown_ids_are_treated_like_missing_ids() -> None:
    payload = {"actions": [
        wire(action="close", base_asset="BTC", close_fraction=1.0, used_message_ids=[999], confidence=0.95),
        wire(action="incomplete", base_asset="ETH", side="long", used_message_ids=[602]),
    ]}
    close, _ = (await parser_for(Client(response(payload))).parse([msg(601, "a"), msg(602)], ctx())).actions
    assert close.used_message_ids == [601] and close.ambiguities


async def test_f11_sole_action_without_ids_still_gets_all_messages_unasked() -> None:
    payload = {"actions": [
        wire(action="move_sl", base_asset="BTC", move_sl_to_entry=True, used_message_ids=[], confidence=0.95),
        wire(action="none", used_message_ids=[]),
    ]}
    sl, none = (await parser_for(Client(response(payload))).parse([msg(1, "a"), msg(2, "b")], ctx())).actions
    assert sl.used_message_ids == [1, 2] and sl.ambiguities == [] and sl.confidence == 0.95
    assert none.used_message_ids == []


async def test_f11_none_leftover_goes_to_the_instruction() -> None:
    payload = {"actions": [
        wire(action="none", used_message_ids=[1]),
        wire(action="move_sl", base_asset="BTC", move_sl_to_entry=True, used_message_ids=[], confidence=0.95),
    ]}
    _, sl = (await parser_for(Client(response(payload))).parse([msg(1, "günaydın"), msg(2, "btc be")],
                                                              ctx())).actions
    assert sl.used_message_ids == [2] and sl.ambiguities == []


async def test_f11_engine_keeps_incomplete_and_asks_about_it(tmp_path) -> None:
    """Auditor's scenario end to end: the ETH setup is asked about when its watch expires."""
    from fakes import FakeBroker, FakeNotifier, FakePriceOracle

    from copytrader.config import AppConfig, NotifierConfig, TelegramConfig
    from copytrader.engine.orchestrator import Engine
    from copytrader.storage.db import Database

    payload = {"actions": [
        wire(action="move_sl", base_asset="BTC", side="long", move_sl_to_entry=True, used_message_ids=[],
             confidence=0.95, summary_tr="BTC stop girişe"),
        wire(action="incomplete", symbol="ETHUSDT.P", base_asset="ETH", side="long", entry_price=2232.18,
             stop_loss=2205.11, take_profits=[{"price": 2310.6, "close_fraction": 1.0}],
             used_message_ids=[602], confidence=0.85, summary_tr="ETH LONG hazırlanıyor"),
    ]}
    cfg = AppConfig(telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[CHAT], incomplete_timeout_sec=1),
                    notifier=NotifierConfig(bot_token="1:a", owner_user_id=7))
    cfg.monitor_interval_sec = 3600.0
    db = Database(":memory:")
    await db.open()
    notifier = FakeNotifier()
    engine = Engine(cfg, db, parser_for(Client(response(payload))), [FakeBroker("binance", equity=10_000.0)],
                    notifier, price_oracle=FakePriceOracle({"ETH": 2232.85, "BTC": 67000.0}))
    await engine.start(monitor=False)
    try:
        now = datetime.now(timezone.utc)
        await db.save_message(StoredMessage(chat_id=CHAT, message_id=601, sender_id=42, date=now,
                                            text="btc stop girişe"))
        await db.save_message(StoredMessage(chat_id=CHAT, message_id=602, sender_id=42, date=now,
                                            text="", media_path=str(tmp_path / "x.jpg"), media_type="photo"))
        await engine.on_messages_ready(CHAT, 42)
        assert not (await db.get_message(CHAT, 602)).consumed
        for _ in range(60):
            await asyncio.sleep(0.05)
            if any(c.action.summary_tr == "ETH LONG hazırlanıyor" for c in notifier.confirmations):
                break
        assert any(c.action.summary_tr == "ETH LONG hazırlanıyor" for c in notifier.confirmations)
    finally:
        await engine.stop()
        await db.close()


# --------------------------------------------------------------------------- #
# F12: empty actions list
# --------------------------------------------------------------------------- #
def test_f12_schema_requires_at_least_one_action() -> None:
    assert WIRE_SCHEMA["properties"]["actions"]["minItems"] == 1


async def test_f12_empty_actions_become_an_unsure_none() -> None:
    result = await parser_for(Client(response({"actions": []}))).parse(
        [msg(501, "BTC short 0.5R giriş 67250 stop 68100 tp 66000")], ctx())
    (a,) = result.actions
    assert a.action is ActionType.NONE and a.confidence == 0.0 and a.ambiguities
    assert a.summary_tr


# --------------------------------------------------------------------------- #
# F14: refusal because the fallback model was saturated
# --------------------------------------------------------------------------- #
async def test_f14_refusal_with_recommended_model_is_transient() -> None:
    det = SimpleNamespace(type="refusal", category=None, recommended_model="claude-opus-5")
    with pytest.raises(ParserError) as ei:
        await parser_for(Client(response(None, "refusal", det))).parse([msg(1, "eth long")], ctx())
    assert ei.value.transient is True and "meşgul" in str(ei.value)


async def test_f14_plain_refusal_stays_permanent() -> None:
    det = SimpleNamespace(type="refusal", category="general_harms", recommended_model=None)
    with pytest.raises(ParserError) as ei:
        await parser_for(Client(response(None, "refusal", det))).parse([msg(1, "eth long")], ctx())
    assert ei.value.transient is False and "reddetti" in str(ei.value)


# --------------------------------------------------------------------------- #
# F15: monthly spend limit
# --------------------------------------------------------------------------- #
def _spend_limit_error(body: dict[str, Any]) -> anthropic.APIStatusError:
    from anthropic._error_classes import make_status_error

    return make_status_error("limit", body=body, response=httpx2.Response(429, request=_req()))


@pytest.mark.parametrize(
    "body",
    [
        {"type": "error", "error": {"type": "rate_limit_error", "message": "monthly limit",
                                    "details": {"error_code": "enforced_spend_limit_reached"}}},
        {"type": "error", "error": {"type": "enforced_spend_limit_reached", "message": "monthly limit"}},
    ],
)
async def test_f15_spend_limit_is_permanent_with_turkish_hint(body: dict[str, Any]) -> None:
    exc = _spend_limit_error(body)
    assert isinstance(exc, anthropic.RateLimitError)
    with pytest.raises(ParserError) as ei:
        await parser_for(Client(exc=exc)).parse([msg(1, "x")], ctx())
    assert ei.value.transient is False
    assert "harcama limiti" in str(ei.value) and "Console" in str(ei.value)


async def test_f15_ordinary_rate_limit_stays_transient() -> None:
    exc = _spend_limit_error({"type": "error", "error": {"type": "rate_limit_error", "message": "slow down"}})
    with pytest.raises(ParserError) as ei:
        await parser_for(Client(exc=exc)).parse([msg(1, "x")], ctx())
    assert ei.value.transient is True and "istek limitine" in str(ei.value)


# --------------------------------------------------------------------------- #
# F16: missing API key
# --------------------------------------------------------------------------- #
@pytest.mark.usefixtures("no_credentials")
@pytest.mark.parametrize("key", [None, "", "   "])
def test_f16_missing_key_fails_at_construction_with_turkish_message(key: str | None) -> None:
    with pytest.raises(ParserError) as ei:
        ClaudeSignalParser(LLMConfig(api_key=key))
    assert "API anahtarı bulunamadı" in str(ei.value) and "ANTHROPIC_API_KEY" in str(ei.value)
    assert ei.value.transient is False


@pytest.mark.usefixtures("no_credentials")
def test_f16_broken_credentials_profile_is_a_parser_error(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("ANTHROPIC_CONFIG_DIR", str(tmp_path / "nowhere"))   # SDK raises CredentialsError
    with pytest.raises(ParserError) as ei:
        ClaudeSignalParser(LLMConfig())
    assert "API anahtarı bulunamadı" in str(ei.value)


@pytest.mark.usefixtures("no_credentials")
def test_f16_blank_config_key_falls_back_to_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-from-env")
    p = ClaudeSignalParser(LLMConfig(api_key="  "))
    assert p._client.api_key == "sk-from-env"


async def test_f16_sdk_auth_type_error_becomes_parser_error() -> None:
    exc = TypeError('"Could not resolve authentication method. Expected one of api_key, auth_token, '
                    'or credentials to be set."')
    with pytest.raises(ParserError) as ei:
        await parser_for(Client(exc=exc)).parse([msg(1, "x")], ctx())
    assert ei.value.transient is False and "API anahtarı" in str(ei.value)
    assert ei.value.__cause__ is exc


async def test_f16_other_type_errors_are_not_masked() -> None:
    with pytest.raises(TypeError):
        await parser_for(Client(exc=TypeError("unexpected keyword argument 'foo'"))).parse([msg(1, "x")], ctx())


# --------------------------------------------------------------------------- #
# F19: TradingView displaces overlapping price labels
# --------------------------------------------------------------------------- #
def test_f19_prompt_explains_label_stacking() -> None:
    from copytrader.parsing import prompts

    sp = prompts.SYSTEM_PROMPT
    assert "never overlap" in sp and "do not pair labels and edges by exact height" in sp
    assert "read the NUMBER" in sp
    assert "label level with the boundary" not in sp            # the old, wrong rule


def test_f19_worked_example_describes_the_displaced_entry_label() -> None:
    from copytrader.parsing import prompts

    sp = prompts.SYSTEM_PROMPT
    assert "exactly at the border" not in sp
    assert '"2.232,18" on green directly below the black label, pushed down' in sp
    evidence = prompts.WORKED_EXAMPLE_OUTPUT["actions"][0]["evidence"]
    assert "sınırındaki yeşil etiket" not in evidence and "kaydırılmış" in evidence
