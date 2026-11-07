"""Tests for the Claude signal parser (no network)."""

from __future__ import annotations

import base64
import copy
import inspect
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import anthropic
import httpx2
import pytest

from copytrader.config import LLMConfig
from copytrader.errors import ParserError
from copytrader.models import (
    ActionType,
    EntryType,
    MarketType,
    ParseContext,
    ParsedAction,
    Side,
    StoredMessage,
)
from copytrader.parsing import prompts
from copytrader.parsing.llm_parser import (
    FALLBACK_BETA,
    WIRE_SCHEMA,
    ClaudeSignalParser,
    _WireAction,
    _WireResult,
    _WireTakeProfit,
    sniff_image_media_type,
)

EXAMPLE_JPG = Path(__file__).resolve().parents[1] / "examples" / "eth_long_tradingview.jpg"
NOW = datetime(2026, 11, 7, 17, 7, 30, tzinfo=timezone.utc)
CHAT = -1001234567890
PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"\x00" * 32


# --------------------------------------------------------------------------- #
# Fakes / helpers
# --------------------------------------------------------------------------- #
class FakeMessages:
    def __init__(self, response: Any = None, exc: Exception | None = None) -> None:
        self.response = response
        self.exc = exc
        self.calls: list[dict[str, Any]] = []

    async def create(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if self.exc is not None:
            raise self.exc
        return self.response


class FakeClient:
    def __init__(self, response: Any = None, exc: Exception | None = None) -> None:
        self.messages = FakeMessages(response, exc)
        self.beta = SimpleNamespace(messages=self.messages)


def make_response(
    payload: Any, stop_reason: str = "end_turn", blocks: list[Any] | None = None
) -> SimpleNamespace:
    text = payload if isinstance(payload, str) else json.dumps(payload)
    content = blocks if blocks is not None else [SimpleNamespace(type="text", text=text)]
    return SimpleNamespace(content=content, stop_reason=stop_reason, model="claude-opus-5-5")


def wire_action(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "action": "none",
        "symbol": None,
        "base_asset": None,
        "quote_asset": None,
        "market_type": None,
        "side": None,
        "entry_type": None,
        "entry_price": None,
        "stop_loss": None,
        "take_profits": [],
        "risk_r": None,
        "new_stop_loss": None,
        "move_sl_to_entry": False,
        "close_fraction": None,
        "refers_to_message_id": None,
        "target_trade_id": None,
        "used_message_ids": [],
        "evidence": "",
        "ambiguities": [],
        "confidence": 0.9,
        "summary_tr": "İşlem talimatı yok",
    }
    base.update(overrides)
    return base


def msg(
    message_id: int,
    text: str = "",
    media_path: str | None = None,
    minute: int = 7,
    **kw: Any,
) -> StoredMessage:
    return StoredMessage(
        chat_id=CHAT,
        message_id=message_id,
        sender_id=42,
        sender_name="Ahmet",
        date=datetime(2026, 11, 7, 17, minute, 0, tzinfo=timezone.utc),
        text=text,
        media_path=media_path,
        media_type="photo" if media_path else None,
        **kw,
    )


def ctx(**kw: Any) -> ParseContext:
    return ParseContext(now=NOW, **kw)


def parser_with(response: Any = None, exc: Exception | None = None, **cfg: Any) -> tuple[ClaudeSignalParser, FakeClient]:
    client = FakeClient(response, exc)
    return ClaudeSignalParser(LLMConfig(**cfg), client=client), client


def user_content(client: FakeClient) -> list[dict[str, Any]]:
    return client.messages.calls[-1]["messages"][0]["content"]


def texts(content: list[dict[str, Any]]) -> str:
    return "\n".join(b["text"] for b in content if b["type"] == "text")


def images(content: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [b for b in content if b["type"] == "image"]


# --------------------------------------------------------------------------- #
# Request shape
# --------------------------------------------------------------------------- #
async def test_request_shape_with_fallbacks() -> None:
    parser, client = parser_with(make_response({"actions": [wire_action()]}), effort="xhigh", max_tokens=12000)
    await parser.parse([msg(1, "günaydın arkadaşlar")], ctx())

    call = client.messages.calls[0]
    assert call["model"] == "claude-opus-5-5"
    assert call["max_tokens"] == 12000
    assert call["system"] == [
        {"type": "text", "text": prompts.SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}
    ]
    assert call["thinking"] == {"type": "adaptive"}
    assert call["output_config"] == {
        "effort": "xhigh",
        "format": {"type": "json_schema", "schema": WIRE_SCHEMA},
    }
    assert call["betas"] == [FALLBACK_BETA] == ["server-side-fallback-2026-07-01"]
    assert call["fallbacks"] == "default"
    assert len(call["messages"]) == 1 and call["messages"][0]["role"] == "user"
    content = call["messages"][0]["content"]
    assert content[0]["text"].startswith("Current time (UTC): 2026-11-07T17:07:30Z")
    assert "[msg_id=1 | 2026-11-07T17:07:00Z | from=Ahmet (id 42)]\ngünaydın arkadaşlar" in texts(content)


async def test_request_without_fallbacks() -> None:
    parser, client = parser_with(make_response({"actions": [wire_action()]}), use_fallbacks=False)
    await parser.parse([msg(1, "selam")], ctx())
    call = client.messages.calls[0]
    assert "betas" not in call and "fallbacks" not in call


def test_request_kwargs_exist_on_installed_sdk() -> None:
    from anthropic.resources.beta.messages.messages import AsyncMessages

    params = inspect.signature(AsyncMessages.create).parameters
    parser = ClaudeSignalParser(LLMConfig(), client=FakeClient())
    for name in parser.build_request([{"type": "text", "text": "x"}]):
        assert name in params, name


async def test_system_prompt_is_stable_across_requests() -> None:
    parser, client = parser_with(make_response({"actions": [wire_action()]}))
    await parser.parse([msg(1, "a")], ctx())
    await parser.parse([msg(2, "b")], ParseContext(now=datetime(2027, 1, 1, tzinfo=timezone.utc)))
    s1, s2 = (c["system"] for c in client.messages.calls)
    assert s1 == s2
    assert "2232.18" in prompts.SYSTEM_PROMPT and "07:11" in prompts.SYSTEM_PROMPT


async def test_message_header_flags() -> None:
    parser, client = parser_with(make_response({"actions": [wire_action()]}))
    m = msg(5, "", reply_to_message_id=3, grouped_id=99, edited=True)
    await parser.parse([m], ctx())
    assert "[msg_id=5 | 2026-11-07T17:07:00Z | from=Ahmet (id 42) | reply_to=3 | album=99 | EDITED]\n(metin yok)" in texts(
        user_content(client)
    )


async def test_empty_messages_skip_api() -> None:
    parser, client = parser_with(make_response({"actions": []}))
    result = await parser.parse([], ctx())
    assert result.actions == [] and client.messages.calls == []


# --------------------------------------------------------------------------- #
# Images
# --------------------------------------------------------------------------- #
async def test_image_block_from_example_jpeg() -> None:
    parser, client = parser_with(make_response({"actions": [wire_action()]}))
    await parser.parse([msg(501, "eth 1R long", media_path=str(EXAMPLE_JPG))], ctx())
    content = user_content(client)
    imgs = images(content)
    assert len(imgs) == 1
    src = imgs[0]["source"]
    assert src["type"] == "base64" and src["media_type"] == "image/jpeg"
    assert base64.standard_b64decode(src["data"]) == EXAMPLE_JPG.read_bytes()
    # The image follows its message's text block.
    idx = content.index(imgs[0])
    assert content[idx - 1]["text"].startswith("[msg_id=501 ")


async def test_max_images_keeps_most_recent(tmp_path: Path) -> None:
    paths = []
    for i in range(3):
        p = tmp_path / f"img{i}.png"
        p.write_bytes(PNG_BYTES + bytes([i]))
        paths.append(p)
    parser, client = parser_with(make_response({"actions": [wire_action()]}), max_images=2)
    msgs = [msg(10 + i, f"m{i}", media_path=str(paths[i]), minute=i) for i in range(3)]
    await parser.parse(msgs, ctx())
    content = user_content(client)
    imgs = images(content)
    assert len(imgs) == 2
    attached = [base64.standard_b64decode(b["source"]["data"]) for b in imgs]
    assert attached == [paths[1].read_bytes(), paths[2].read_bytes()]
    assert all(b["source"]["media_type"] == "image/png" for b in imgs)
    first = next(b["text"] for b in content if b["type"] == "text" and b["text"].startswith("[msg_id=10 "))
    assert prompts.IMAGE_LIMIT_NOTE in first


async def test_missing_and_unsupported_images_are_noted(tmp_path: Path) -> None:
    bad = tmp_path / "x.bin"
    bad.write_bytes(b"not an image at all")
    parser, client = parser_with(make_response({"actions": [wire_action()]}))
    await parser.parse(
        [msg(1, "a", media_path=str(tmp_path / "missing.jpg")), msg(2, "b", media_path=str(bad))], ctx()
    )
    content = user_content(client)
    assert images(content) == []
    t = texts(content)
    assert "could not be attached (file missing or unreadable)" in t
    assert "could not be attached (unsupported image format)" in t


def test_sniff_media_types() -> None:
    assert sniff_image_media_type(EXAMPLE_JPG.read_bytes()) == "image/jpeg"
    assert sniff_image_media_type(PNG_BYTES) == "image/png"
    assert sniff_image_media_type(b"GIF89a....") == "image/gif"
    assert sniff_image_media_type(b"RIFF\x00\x00\x00\x00WEBPVP8 ") == "image/webp"
    assert sniff_image_media_type(b"%PDF-1.7") is None


# --------------------------------------------------------------------------- #
# Context blocks
# --------------------------------------------------------------------------- #
async def test_reply_to_open_trades_and_correction_context() -> None:
    previous = ParsedAction(
        action=ActionType.OPEN, base_asset="ETH", side=Side.LONG, entry_price=2232.18,
        stop_loss=2250.0, confidence=0.6, summary_tr="ETH LONG, SL 2250",
    )
    trades = [{"trade_id": 7, "asset": "ETH", "side": "long", "source_message_ids": [400]}]
    reply = msg(400, "", media_path=str(EXAMPLE_JPG), minute=1)
    parser, client = parser_with(make_response({"actions": [wire_action()]}))
    await parser.parse(
        [msg(501, "stop girişe", reply_to_message_id=400)],
        ctx(reply_to=reply, open_trades=trades, correction_text="SL 2205.11 olacak", previous_action=previous),
    )
    content = user_content(client)
    t = texts(content)
    assert prompts.REPLY_TO_HEADER + "\n[msg_id=400 " in t
    assert len(images(content)) == 1  # reply image attached (budget allows)
    assert prompts.OPEN_TRADES_HEADER + "\n" + json.dumps(trades, ensure_ascii=False, sort_keys=True) in t
    last = content[-1]["text"]
    assert last.startswith("=== OWNER CORRECTION ===")
    assert '"SL 2205.11 olacak"' in last
    assert '"stop_loss": 2250.0' in last  # previous understanding included
    assert "overrides your earlier reading" in last


async def test_reply_to_already_in_messages_is_not_duplicated() -> None:
    parser, client = parser_with(make_response({"actions": [wire_action()]}))
    m1 = msg(400, "", media_path=str(EXAMPLE_JPG))
    await parser.parse([m1, msg(401, "eth 1R long", reply_to_message_id=400)], ctx(reply_to=m1))
    content = user_content(client)
    assert len(images(content)) == 1
    assert prompts.REPLY_TO_ALREADY_INCLUDED.format(msg_id=400) in texts(content)
    assert prompts.OPEN_TRADES_NONE in texts(content)


# --------------------------------------------------------------------------- #
# Response mapping
# --------------------------------------------------------------------------- #
async def test_maps_worked_example_to_parsed_action() -> None:
    payload = copy.deepcopy(prompts.WORKED_EXAMPLE_OUTPUT)
    parser, _ = parser_with(make_response(payload))
    result = await parser.parse(
        [msg(501, "", media_path=str(EXAMPLE_JPG)), msg(502, "eth 1R long")], ctx()
    )
    assert result.model == "claude-opus-5-5"
    assert json.loads(result.raw_response or "") == payload
    (a,) = result.actions
    assert a.action is ActionType.OPEN
    assert (a.symbol, a.base_asset, a.quote_asset) == ("ETHUSDT.P", "ETH", "USDT")
    assert a.market_type is MarketType.PERP and a.side is Side.LONG and a.entry_type is EntryType.MARKET
    assert (a.entry_price, a.stop_loss) == (2232.18, 2205.11)
    assert [(tp.price, tp.close_fraction) for tp in a.take_profits] == [(2310.6, 1.0)]
    assert a.risk_r == 1.0 and a.confidence == 0.95
    assert a.used_message_ids == [501, 502]
    assert a.summary_tr == "ETH LONG, giriş 2232.18, SL 2205.11, TP 2310.60, risk 1R"
    assert "2232.85" in a.reasoning_brief
    assert a.ambiguities == []


async def test_multiple_actions_and_management_fields() -> None:
    trades = [
        {"trade_id": 7, "asset": "ETH", "side": "long", "source_message_ids": [400]},
        {"trade_id": 12, "asset": "SOL", "side": "short", "source_message_ids": [410]},
    ]
    payload = {
        "actions": [
            wire_action(action="move_sl", base_asset="eth", side="long", move_sl_to_entry=True,
                        target_trade_id=7, refers_to_message_id=400, used_message_ids=[600], confidence=0.95),
            wire_action(action="close", base_asset="SOL", side="short", close_fraction=0.5,
                        target_trade_id=12, used_message_ids=[601], confidence=0.92),
        ]
    }
    parser, _ = parser_with(make_response(payload))
    result = await parser.parse([msg(600, "eth stopu girişe çektim"), msg(601, "SOL yarısını kapattım")],
                                ctx(open_trades=trades))
    sl, close = result.actions
    assert sl.action is ActionType.MOVE_SL and sl.move_sl_to_entry and sl.target_trade_id == 7
    assert sl.refers_to_message_id == 400 and sl.base_asset == "ETH"
    assert close.action is ActionType.CLOSE and close.close_fraction == 0.5 and close.target_trade_id == 12


async def test_post_processing_assets_tps_and_confidence() -> None:
    payload = {
        "actions": [
            wire_action(action="open", base_asset="#eth", quote_asset="$usdt", side="long",
                        entry_price=2232.18, stop_loss=2205.11, confidence=1.7,
                        take_profits=[{"price": 0, "close_fraction": 1.0},
                                      {"price": -5, "close_fraction": 1.0},
                                      {"price": 2310.6, "close_fraction": 1.0}]),
            wire_action(base_asset="BINANCE:ETHUSDT.P", quote_asset="USDT", confidence=-0.2),
            wire_action(base_asset="BTC/USDT:USDT", quote_asset="USDT"),
        ]
    }
    parser, _ = parser_with(make_response(payload))
    a, b, c = (await parser.parse([msg(1, "x")], ctx())).actions
    assert (a.base_asset, a.quote_asset) == ("ETH", "USDT")
    assert [tp.price for tp in a.take_profits] == [2310.6]
    assert a.confidence == 1.0
    assert b.base_asset == "ETH" and b.confidence == 0.0
    assert c.base_asset == "BTC"


async def test_invalid_numbers_become_none_with_ambiguity() -> None:
    payload = {
        "actions": [
            wire_action(action="open", base_asset="ETH", side="long", entry_price=-1, stop_loss=0,
                        risk_r=0, confidence=0.99, summary_tr=""),
        ]
    }
    parser, _ = parser_with(make_response(payload))
    (a,) = (await parser.parse([msg(1, "x")], ctx())).actions
    assert a.entry_price is None and a.stop_loss is None and a.risk_r is None
    assert len(a.ambiguities) == 3
    assert a.confidence <= 0.6
    assert a.summary_tr.startswith("ETH LONG")  # fallback summary


async def test_tp_fraction_normalization() -> None:
    payload = {
        "actions": [
            wire_action(action="open", base_asset="BTC", side="short", stop_loss=68100, confidence=0.95,
                        take_profits=[{"price": 66000, "close_fraction": 30},
                                      {"price": 65000, "close_fraction": 30},
                                      {"price": 64000, "close_fraction": 40}]),
            wire_action(action="open", base_asset="BTC", side="short", stop_loss=68100, confidence=0.95,
                        take_profits=[{"price": 66000, "close_fraction": 0.5},
                                      {"price": 65000, "close_fraction": 0.2}]),
            wire_action(action="open", base_asset="BTC", side="short", stop_loss=68100, confidence=0.95,
                        take_profits=[{"price": 66000, "close_fraction": 0.5},
                                      {"price": 65000, "close_fraction": 0.5}]),
        ]
    }
    parser, _ = parser_with(make_response(payload))
    pct, bad, ok = (await parser.parse([msg(1, "x")], ctx())).actions
    assert [tp.close_fraction for tp in pct.take_profits] == pytest.approx([0.3, 0.3, 0.4])
    assert pct.ambiguities == [] and pct.confidence == 0.95
    assert [tp.close_fraction for tp in bad.take_profits] == [1.0, 1.0]
    assert bad.ambiguities and bad.confidence <= 0.6
    assert [tp.close_fraction for tp in ok.take_profits] == [0.5, 0.5]


async def test_close_fraction_percent_and_invalid() -> None:
    payload = {
        "actions": [
            wire_action(action="close", close_fraction=50, confidence=0.9),
            wire_action(action="close", close_fraction=-1, confidence=0.9),
        ]
    }
    parser, _ = parser_with(make_response(payload))
    pct, bad = (await parser.parse([msg(1, "x")], ctx())).actions
    assert pct.close_fraction == 0.5 and pct.ambiguities and pct.confidence <= 0.6
    assert bad.close_fraction is None and bad.ambiguities


# --------------------------------------------------------------------------- #
# Id sanitizing
# --------------------------------------------------------------------------- #
async def test_used_message_ids_sanitizing() -> None:
    trades = [{"trade_id": 7, "asset": "ETH", "side": "long", "source_message_ids": [300]}]
    payload = {
        "actions": [
            # unknown ids dropped, duplicates removed, reply_to id allowed
            wire_action(action="open", base_asset="ETH", side="long", stop_loss=1.0,
                        used_message_ids=[999, 11, 11, 400]),
            # empty ids next to an action that claims ids -> only the unclaimed ids, owner asked
            wire_action(action="move_sl", move_sl_to_entry=True, used_message_ids=[],
                        target_trade_id=7, refers_to_message_id=300, confidence=0.95),
            # only unknown ids -> same
            wire_action(action="incomplete", used_message_ids=[12345]),
            # NONE stays empty
            wire_action(action="none", used_message_ids=[]),
            # unknown trade / message references dropped
            wire_action(action="close", close_fraction=1.0, target_trade_id=99,
                        refers_to_message_id=777, confidence=0.95),
        ]
    }
    parser, _ = parser_with(make_response(payload))
    reply = msg(400, "", minute=1)
    result = await parser.parse([msg(10, "a"), msg(11, "b")], ctx(reply_to=reply, open_trades=trades))
    o, sl, inc, none, close = result.actions
    assert o.used_message_ids == [11, 400]
    assert sl.used_message_ids == [10] and sl.target_trade_id == 7 and sl.refers_to_message_id == 300
    assert sl.ambiguities and sl.confidence <= 0.6
    assert inc.used_message_ids == [10] and inc.ambiguities
    assert none.used_message_ids == []
    assert close.target_trade_id is None and close.refers_to_message_id is None
    assert close.ambiguities and close.confidence <= 0.6


async def test_messages_sorted_oldest_first() -> None:
    parser, client = parser_with(make_response({"actions": [wire_action()]}))
    await parser.parse([msg(2, "second", minute=9), msg(1, "first", minute=8)], ctx())
    t = texts(user_content(client))
    assert t.index("[msg_id=1 ") < t.index("[msg_id=2 ")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("stop_reason", ["refusal", "max_tokens", "pause_turn"])
async def test_bad_stop_reasons_raise(stop_reason: str) -> None:
    parser, _ = parser_with(make_response({"actions": [wire_action()]}, stop_reason=stop_reason))
    with pytest.raises(ParserError) as ei:
        await parser.parse([msg(1, "x")], ctx())
    assert "sinyal" in str(ei.value)


@pytest.mark.parametrize(
    "payload",
    [
        "not json",
        '{"actions": [',
        json.dumps({"actions": [{"action": "open"}]}),                       # missing fields
        json.dumps({"actions": [wire_action(action="buy")]}),                 # bad enum
        json.dumps({"actions": [wire_action(extra_field=1)]}),                # extra field
        json.dumps({"actions": [wire_action()]}).replace("0.9", "NaN"),       # non-finite
    ],
)
async def test_invalid_output_raises(payload: str) -> None:
    parser, _ = parser_with(make_response(payload))
    with pytest.raises(ParserError):
        await parser.parse([msg(1, "x")], ctx())


async def test_no_text_block_raises() -> None:
    parser, _ = parser_with(make_response("", blocks=[SimpleNamespace(type="thinking", thinking="")]))
    with pytest.raises(ParserError):
        await parser.parse([msg(1, "x")], ctx())


async def test_reads_text_after_fallback_block() -> None:
    good = json.dumps({"actions": [wire_action(confidence=0.42)]})
    blocks = [
        SimpleNamespace(type="thinking", thinking=""),
        SimpleNamespace(type="text", text='{"actions": [{"act'),   # partial output of the declining model
        SimpleNamespace(type="fallback"),
        SimpleNamespace(type="text", text=good),
    ]
    parser, _ = parser_with(make_response("", blocks=blocks))
    result = await parser.parse([msg(1, "x")], ctx())
    assert result.actions[0].confidence == 0.42 and result.raw_response == good


def _request() -> httpx2.Request:
    return httpx2.Request("POST", "https://api.anthropic.com/v1/messages")


def _status_error(cls: type[anthropic.APIStatusError], code: int) -> anthropic.APIStatusError:
    return cls("boom", response=httpx2.Response(code, request=_request()), body=None)


@pytest.mark.parametrize(
    "exc",
    [
        _status_error(anthropic.RateLimitError, 429),
        _status_error(anthropic.BadRequestError, 400),
        _status_error(anthropic.AuthenticationError, 401),
        _status_error(anthropic.InternalServerError, 500),
        anthropic.APIConnectionError(request=_request()),
        anthropic.APITimeoutError(request=_request()),
    ],
)
async def test_api_errors_become_parser_errors(exc: Exception) -> None:
    parser, _ = parser_with(exc=exc)
    with pytest.raises(ParserError) as ei:
        await parser.parse([msg(1, "x")], ctx())
    assert ei.value.__cause__ is exc
    assert "boom" not in str(ei.value)  # Turkish message, not the raw English error


# --------------------------------------------------------------------------- #
# Wire schema
# --------------------------------------------------------------------------- #
_UNSUPPORTED = {"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf",
                "minLength", "maxLength", "pattern", "oneOf", "maxItems", "uniqueItems"}


def _walk(schema: dict[str, Any], path: str = "$") -> int:
    """Check one schema node and its children; returns the number of nodes checked."""
    assert not (_UNSUPPORTED & schema.keys()), path
    if "anyOf" in schema:
        assert "type" not in schema, path
        return 1 + sum(_walk(v, f"{path}.anyOf[{i}]") for i, v in enumerate(schema["anyOf"]))
    assert schema["type"] in {"object", "array", "string", "number", "integer", "boolean", "null"}, path
    n = 1
    if schema["type"] == "object":
        assert schema.get("additionalProperties") is False, path
        assert schema["required"] == list(schema["properties"]), path
        n += sum(_walk(v, f"{path}.{k}") for k, v in schema["properties"].items())
    elif schema["type"] == "array":
        n += _walk(schema["items"], f"{path}[]")
    return n


def test_wire_schema_is_strict() -> None:
    json.dumps(WIRE_SCHEMA)  # serializable
    assert _walk(WIRE_SCHEMA) > 30
    action = WIRE_SCHEMA["properties"]["actions"]["items"]
    assert set(action["properties"]) == set(_WireAction.model_fields)
    tp = action["properties"]["take_profits"]["items"]
    assert set(tp["properties"]) == set(_WireTakeProfit.model_fields)
    assert action["properties"]["action"]["enum"] == [a.value for a in ActionType]
    assert action["properties"]["side"]["anyOf"][0]["enum"] == [s.value for s in Side]
    assert action["properties"]["entry_type"]["anyOf"][0]["enum"] == [e.value for e in EntryType]
    assert action["properties"]["market_type"]["anyOf"][0]["enum"] == [m.value for m in MarketType]
    # Wire-only field names map onto ParsedAction (evidence -> reasoning_brief).
    assert set(_WireAction.model_fields) - set(ParsedAction.model_fields) == {"evidence"}


def test_worked_example_matches_wire_schema() -> None:
    _WireResult.model_validate(prompts.WORKED_EXAMPLE_OUTPUT)
    assert json.dumps(prompts.WORKED_EXAMPLE_OUTPUT, ensure_ascii=False, indent=2) in prompts.SYSTEM_PROMPT


# --------------------------------------------------------------------------- #
# End to end through the real SDK (mock HTTP transport, no network)
# --------------------------------------------------------------------------- #
async def test_end_to_end_through_real_sdk() -> None:
    seen: dict[str, Any] = {}
    payload = copy.deepcopy(prompts.WORKED_EXAMPLE_OUTPUT)

    def handler(request: httpx2.Request) -> httpx2.Response:
        seen["path"] = request.url.path
        seen["beta"] = request.headers.get("anthropic-beta", "")
        seen["body"] = json.loads(request.content)
        return httpx2.Response(
            200,
            json={
                "id": "msg_test",
                "type": "message",
                "role": "assistant",
                "model": "claude-opus-5-5",
                "content": [
                    {"type": "thinking", "thinking": "", "signature": "sig"},
                    {"type": "text", "text": json.dumps(payload)},
                ],
                "stop_reason": "end_turn",
                "stop_sequence": None,
                "usage": {"input_tokens": 10, "output_tokens": 20,
                          "cache_read_input_tokens": 5000, "cache_creation_input_tokens": 0},
            },
        )

    client = anthropic.AsyncAnthropic(
        api_key="test-key",
        max_retries=0,
        http_client=anthropic.DefaultAsyncHttpxClient(transport=httpx2.MockTransport(handler)),
    )
    parser = ClaudeSignalParser(LLMConfig(), client=client)
    result = await parser.parse(
        [msg(501, "", media_path=str(EXAMPLE_JPG)), msg(502, "eth 1R long")], ctx()
    )
    await client.close()

    assert seen["path"] == "/v1/messages"
    assert "server-side-fallback-2026-07-01" in seen["beta"]
    body = seen["body"]
    assert body["fallbacks"] == "default"
    assert body["thinking"] == {"type": "adaptive"}
    assert body["output_config"]["format"]["schema"] == WIRE_SCHEMA
    assert body["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert any(b["type"] == "image" for b in body["messages"][0]["content"])
    (a,) = result.actions
    assert a.action is ActionType.OPEN and a.stop_loss == 2205.11 and a.risk_r == 1.0
