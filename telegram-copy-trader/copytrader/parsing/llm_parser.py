"""Claude-based signal parser (implements ``copytrader.interfaces.SignalParser``).

One ``beta.messages.create`` call per batch of messages: stable cached system
prompt, a user turn with the messages (text + images) and context, adaptive
thinking, and structured output constrained by ``WIRE_SCHEMA``. The wire format
is kept separate from ``models.ParsedAction`` and mapped/sanitized here.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import anthropic
from pydantic import BaseModel, ConfigDict, ValidationError

from ..config import LLMConfig
from ..errors import ParserError
from ..models import (
    ActionType,
    EntryType,
    MarketType,
    ParseContext,
    ParsedAction,
    ParseResult,
    Side,
    StoredMessage,
    TakeProfit,
)
from . import prompts

logger = logging.getLogger(__name__)

FALLBACK_BETA = "server-side-fallback-2026-07-01"   # pairs with fallbacks="default"
MAX_IMAGE_B64_BYTES = 5 * 1024 * 1024               # API per-image limit (base64 payload)
_LOW_CONFIDENCE_CAP = 0.6                           # cap when post-processing had to fix the output

# --------------------------------------------------------------------------- #
# Wire schema (structured output). Strict: every object lists all properties
# in "required" and sets additionalProperties=false; nullables via anyOf+null.
# --------------------------------------------------------------------------- #
_ACTION_VALUES = [a.value for a in ActionType]
_SIDE_VALUES = [s.value for s in Side]
_ENTRY_TYPE_VALUES = [e.value for e in EntryType]
_MARKET_TYPE_VALUES = [m.value for m in MarketType]


def _nullable(schema: dict[str, Any], description: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"anyOf": [schema, {"type": "null"}]}
    if description:
        out["description"] = description
    return out


def _described(schema: dict[str, Any], description: str) -> dict[str, Any]:
    return {**schema, "description": description}


_TAKE_PROFIT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "price": {"type": "number"},
        "close_fraction": _described(
            {"type": "number"}, "Fraction of the original size closed here; 1.0 unless the trader gave a split."
        ),
    },
    "required": ["price", "close_fraction"],
    "additionalProperties": False,
}

_ACTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "action": {"type": "string", "enum": _ACTION_VALUES},
        "symbol": _nullable({"type": "string"}, "Instrument as written/shown, e.g. ETHUSDT.P"),
        "base_asset": _nullable({"type": "string"}, "Normalized upper-case base asset, e.g. ETH, XAU"),
        "quote_asset": _nullable({"type": "string"}, "USDT, USD... only when stated or part of the symbol"),
        "market_type": _nullable({"type": "string", "enum": _MARKET_TYPE_VALUES}),
        "side": _nullable({"type": "string", "enum": _SIDE_VALUES}),
        "entry_type": _nullable({"type": "string", "enum": _ENTRY_TYPE_VALUES}),
        "entry_price": _nullable({"type": "number"}),
        "stop_loss": _nullable({"type": "number"}),
        "take_profits": {"type": "array", "items": _TAKE_PROFIT_SCHEMA},
        "risk_r": _nullable({"type": "number"}, "Size in R (1R, 0.5R); null if not stated"),
        "new_stop_loss": _nullable({"type": "number"}, "move_sl only"),
        "move_sl_to_entry": _described({"type": "boolean"}, "move_sl only: stop to break-even"),
        "close_fraction": _nullable({"type": "number"}, "close only: fraction of the open position"),
        "refers_to_message_id": _nullable({"type": "integer"}),
        "target_trade_id": _nullable({"type": "integer"}, "trade_id from OPEN TRADES"),
        "used_message_ids": {"type": "array", "items": {"type": "integer"}},
        "evidence": _described({"type": "string"}, "Turkish: where each value came from"),
        "ambiguities": _described(
            {"type": "array", "items": {"type": "string"}}, "Turkish, one short sentence each"
        ),
        "confidence": _described({"type": "number"}, "0..1"),
        "summary_tr": _described({"type": "string"}, "One Turkish line for the owner"),
    },
    "additionalProperties": False,
}
_ACTION_SCHEMA["required"] = list(_ACTION_SCHEMA["properties"])

WIRE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"actions": {"type": "array", "items": _ACTION_SCHEMA}},
    "required": ["actions"],
    "additionalProperties": False,
}


# Client-side mirror of WIRE_SCHEMA, used only to validate the response.
class _WireTakeProfit(BaseModel):
    model_config = ConfigDict(extra="forbid")

    price: float
    close_fraction: float


class _WireAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["open", "move_sl", "move_tp", "close", "cancel", "incomplete", "none"]
    symbol: str | None
    base_asset: str | None
    quote_asset: str | None
    market_type: Literal["perp", "spot", "forex", "cfd"] | None
    side: Literal["long", "short"] | None
    entry_type: Literal["market", "limit"] | None
    entry_price: float | None
    stop_loss: float | None
    take_profits: list[_WireTakeProfit]
    risk_r: float | None
    new_stop_loss: float | None
    move_sl_to_entry: bool
    close_fraction: float | None
    refers_to_message_id: int | None
    target_trade_id: int | None
    used_message_ids: list[int]
    evidence: str
    ambiguities: list[str]
    confidence: float
    summary_tr: str


class _WireResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actions: list[_WireAction]


# --------------------------------------------------------------------------- #
# Turkish error texts (shown to the owner)
# --------------------------------------------------------------------------- #
_ERR_REFUSAL = "Yapay zekâ bu mesajı işlemeyi reddetti; sinyali lütfen kendin kontrol et."
_ERR_MAX_TOKENS = "Yapay zekâ yanıtı yarıda kesildi (token limiti); sinyal okunamadı."
_ERR_STOP = "Yapay zekâ yanıtı beklenmedik şekilde bitti ({reason}); sinyal okunamadı."
_ERR_NO_TEXT = "Yapay zekâ boş yanıt döndürdü; sinyal okunamadı."
_ERR_BAD_JSON = "Yapay zekâ geçersiz bir yanıt döndürdü; sinyal okunamadı."
_ERR_TIMEOUT = "Yapay zekâ servisi zamanında yanıt vermedi; sinyal okunamadı."
_ERR_CONNECTION = "Yapay zekâ servisine bağlanılamadı; sinyal okunamadı."
_ERR_RATE_LIMIT = "Yapay zekâ servisi istek limitine takıldı; sinyal okunamadı."
_ERR_AUTH = "Anthropic API anahtarı geçersiz veya yetkisiz; ayarları kontrol et."
_ERR_BAD_REQUEST = "Yapay zekâ isteği reddetti (HTTP 400); sinyal okunamadı."
_ERR_STATUS = "Yapay zekâ servisi hata döndürdü (HTTP {code}); sinyal okunamadı."
_ERR_OTHER = "Yapay zekâ servisine erişilemedi (yapılandırma hatası); sinyal okunamadı."

_MANAGEMENT = {ActionType.MOVE_SL, ActionType.MOVE_TP, ActionType.CLOSE, ActionType.CANCEL}
_NEEDS_IDS = _MANAGEMENT | {ActionType.OPEN, ActionType.INCOMPLETE}


class ClaudeSignalParser:
    """Turns Telegram signal messages into ``ParsedAction``s with Claude."""

    def __init__(self, config: LLMConfig, client: Any | None = None) -> None:
        """``client``: an anthropic.AsyncAnthropic (injected in tests); created from config if None."""
        self._config = config
        self._owns_client = client is None
        # api_key=None lets the SDK resolve ANTHROPIC_API_KEY (or other env credentials).
        self._client: Any = client or anthropic.AsyncAnthropic(
            api_key=config.api_key, timeout=config.timeout_sec
        )

    async def aclose(self) -> None:
        """Close the HTTP client if this parser created it."""
        if self._owns_client:
            close = getattr(self._client, "close", None)
            if close is not None:
                await close()

    # ------------------------------------------------------------------ #
    async def parse(self, messages: list[StoredMessage], context: ParseContext) -> ParseResult:
        if not messages:
            return ParseResult(actions=[], model=None, raw_response=None)

        messages = sorted(messages, key=lambda m: (_as_utc(m.date), m.message_id))
        content = await self._build_content(messages, context)
        request = self.build_request(content)

        try:
            response = await self._client.beta.messages.create(**request)
        except anthropic.APITimeoutError as e:
            logger.warning("Claude request timed out: %s", e)
            raise ParserError(_ERR_TIMEOUT) from e
        except anthropic.APIConnectionError as e:
            logger.warning("Claude connection error: %s", e)
            raise ParserError(_ERR_CONNECTION) from e
        except anthropic.RateLimitError as e:
            logger.warning("Claude rate limited: %s", e)
            raise ParserError(_ERR_RATE_LIMIT) from e
        except (anthropic.AuthenticationError, anthropic.PermissionDeniedError) as e:
            logger.error("Claude auth error: %s", e)
            raise ParserError(_ERR_AUTH) from e
        except anthropic.BadRequestError as e:
            logger.error("Claude rejected the request (400): %s", e)
            raise ParserError(_ERR_BAD_REQUEST) from e
        except anthropic.APIStatusError as e:
            logger.warning("Claude API error %s: %s", e.status_code, e)
            raise ParserError(_ERR_STATUS.format(code=e.status_code)) from e
        except anthropic.AnthropicError as e:
            logger.error("Claude client error: %s", e)
            raise ParserError(_ERR_OTHER) from e

        return self._handle_response(response, messages, context)

    # ------------------------------------------------------------------ #
    def build_request(self, content: list[dict[str, Any]]) -> dict[str, Any]:
        """kwargs for ``client.beta.messages.create``. Stable parts first for prompt caching."""
        cfg = self._config
        request: dict[str, Any] = {
            "model": cfg.model,
            "max_tokens": cfg.max_tokens,
            "system": [
                {"type": "text", "text": prompts.SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}
            ],
            "messages": [{"role": "user", "content": content}],
            "thinking": {"type": "adaptive"},
            "output_config": {
                "effort": cfg.effort,
                "format": {"type": "json_schema", "schema": WIRE_SCHEMA},
            },
        }
        if cfg.use_fallbacks:
            request["betas"] = [FALLBACK_BETA]
            request["fallbacks"] = "default"
        return request

    # ------------------------------------------------------------------ #
    async def _build_content(
        self, messages: list[StoredMessage], context: ParseContext
    ) -> list[dict[str, Any]]:
        budget = max(0, self._config.max_images)
        msg_ids = {m.message_id for m in messages}

        # Pick the most recent images first; unreadable ones don't use budget.
        images: dict[int, dict[str, Any]] = {}
        notes: dict[int, str] = {}
        for m in reversed(messages):
            if not m.media_path:
                continue
            if len(images) >= budget:
                notes[m.message_id] = prompts.IMAGE_LIMIT_NOTE
                continue
            block, reason = await _load_image_block(m.media_path)
            if block is None:
                notes[m.message_id] = prompts.IMAGE_UNREADABLE_NOTE.format(reason=reason)
            else:
                images[m.message_id] = block

        content: list[dict[str, Any]] = [
            {"type": "text", "text": prompts.USER_INTRO.format(now=_iso(context.now))}
        ]
        for m in messages:
            content.append({"type": "text", "text": _render_message(m, notes.get(m.message_id))})
            if m.message_id in images:
                content.append(images[m.message_id])

        reply = context.reply_to
        if reply is not None:
            if reply.message_id in msg_ids:
                content.append(
                    {"type": "text", "text": prompts.REPLY_TO_ALREADY_INCLUDED.format(msg_id=reply.message_id)}
                )
            else:
                reply_image: dict[str, Any] | None = None
                note: str | None = None
                if reply.media_path:
                    if len(images) >= budget:
                        note = prompts.IMAGE_LIMIT_NOTE
                    else:
                        reply_image, reason = await _load_image_block(reply.media_path)
                        if reply_image is None:
                            note = prompts.IMAGE_UNREADABLE_NOTE.format(reason=reason)
                content.append(
                    {"type": "text", "text": prompts.REPLY_TO_HEADER + "\n" + _render_message(reply, note)}
                )
                if reply_image is not None:
                    content.append(reply_image)

        if context.open_trades:
            trades_json = json.dumps(context.open_trades, ensure_ascii=False, sort_keys=True, default=str)
            content.append({"type": "text", "text": prompts.OPEN_TRADES_HEADER + "\n" + trades_json})
        else:
            content.append({"type": "text", "text": prompts.OPEN_TRADES_NONE})

        if context.correction_text:
            previous = prompts.CORRECTION_NO_PREVIOUS
            if context.previous_action is not None:
                previous = json.dumps(
                    context.previous_action.model_dump(mode="json"), ensure_ascii=False, sort_keys=True
                )
            content.append(
                {
                    "type": "text",
                    "text": prompts.CORRECTION_TEMPLATE.format(
                        previous=previous, correction=context.correction_text.strip()
                    ),
                }
            )
        return content

    # ------------------------------------------------------------------ #
    def _handle_response(
        self, response: Any, messages: list[StoredMessage], context: ParseContext
    ) -> ParseResult:
        stop_reason = getattr(response, "stop_reason", None)
        model = getattr(response, "model", None)
        request_id = getattr(response, "_request_id", None)

        if stop_reason == "refusal":
            details = getattr(response, "stop_details", None)
            logger.warning(
                "Claude refused (model=%s, request_id=%s, category=%s)",
                model, request_id, getattr(details, "category", None),
            )
            raise ParserError(_ERR_REFUSAL)
        if stop_reason == "max_tokens":
            logger.warning("Claude hit max_tokens=%s (request_id=%s)", self._config.max_tokens, request_id)
            raise ParserError(_ERR_MAX_TOKENS)
        if stop_reason not in ("end_turn", "stop_sequence"):
            logger.warning("Unexpected stop_reason %r (request_id=%s)", stop_reason, request_id)
            raise ParserError(_ERR_STOP.format(reason=stop_reason))

        text = _final_text(getattr(response, "content", None) or [])
        if text is None:
            logger.warning("Claude response has no text block (request_id=%s)", request_id)
            raise ParserError(_ERR_NO_TEXT)

        try:
            data = json.loads(text, parse_constant=_reject_constant)
            wire = _WireResult.model_validate(data)
        except (ValueError, ValidationError) as e:   # JSONDecodeError is a ValueError
            logger.warning("Claude output does not match the schema: %s; raw=%.500s", e, text)
            raise ParserError(_ERR_BAD_JSON) from e

        usage = getattr(response, "usage", None)
        if usage is not None:
            logger.debug(
                "Claude usage: in=%s out=%s cache_read=%s cache_write=%s",
                getattr(usage, "input_tokens", None), getattr(usage, "output_tokens", None),
                getattr(usage, "cache_read_input_tokens", None),
                getattr(usage, "cache_creation_input_tokens", None),
            )

        actions = [_to_action(w, messages, context) for w in wire.actions]
        logger.info(
            "Parsed %d message(s) -> %s (model=%s)",
            len(messages), [f"{a.action.value}:{a.base_asset}:{a.confidence:.2f}" for a in actions], model,
        )
        return ParseResult(actions=actions, model=model, raw_response=text)


# --------------------------------------------------------------------------- #
# Content helpers
# --------------------------------------------------------------------------- #
def _as_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _iso(dt: datetime) -> str:
    return _as_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


def _render_message(m: StoredMessage, note: str | None = None) -> str:
    parts = [f"msg_id={m.message_id}", _iso(m.date)]
    sender = m.sender_name or "?"
    parts.append(f"from={sender} (id {m.sender_id})" if m.sender_id is not None else f"from={sender}")
    if m.reply_to_message_id is not None:
        parts.append(f"reply_to={m.reply_to_message_id}")
    if m.grouped_id is not None:
        parts.append(f"album={m.grouped_id}")
    if m.edited:
        parts.append("EDITED")
    text = m.text.strip() if m.text and m.text.strip() else prompts.NO_TEXT
    out = "[" + " | ".join(parts) + "]\n" + text
    if note:
        out += "\n" + note
    return out


def sniff_image_media_type(data: bytes) -> str | None:
    """Media type from magic bytes (jpeg/png/gif/webp), None if unsupported."""
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None


async def _load_image_block(path: str) -> tuple[dict[str, Any] | None, str]:
    """Base64 image block, or (None, reason) when the file is missing/unsupported/too large."""
    try:
        data = await asyncio.to_thread(Path(path).read_bytes)
    except OSError as e:
        logger.warning("Cannot read image %s: %s", path, e)
        return None, "file missing or unreadable"
    media_type = sniff_image_media_type(data)
    if media_type is None:
        logger.warning("Unsupported image format: %s", path)
        return None, "unsupported image format"
    b64 = base64.standard_b64encode(data).decode("ascii")
    if len(b64) > MAX_IMAGE_B64_BYTES:
        logger.warning("Image too large for the API (%d bytes base64): %s", len(b64), path)
        return None, "image too large"
    return {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": b64}}, ""


# --------------------------------------------------------------------------- #
# Response helpers
# --------------------------------------------------------------------------- #
def _final_text(blocks: list[Any]) -> str | None:
    """First text block after the last server-side fallback switch (if any)."""
    start = 0
    for i, b in enumerate(blocks):
        if _block_type(b) == "fallback":
            start = i + 1
    for b in blocks[start:]:
        if _block_type(b) == "text":
            text = b.get("text") if isinstance(b, dict) else getattr(b, "text", None)
            if isinstance(text, str) and text.strip():
                return text
    return None


def _block_type(block: Any) -> str | None:
    return block.get("type") if isinstance(block, dict) else getattr(block, "type", None)


def _reject_constant(name: str) -> float:
    raise ValueError(f"non-finite number {name} in model output")


def _clean_asset(raw: str | None) -> str | None:
    """Upper-case, strip '#$', '/QUOTE[:SETTLE]' part, 'EXCHANGE:' prefix and '.P' suffix."""
    if raw is None:
        return None
    s = raw.strip().upper()
    if "/" in s:                      # "BTC/USDT:USDT" -> "BTC"
        s = s.split("/", 1)[0]
    if ":" in s:                      # "BINANCE:ETHUSDT.P" -> "ETHUSDT.P"
        s = s.split(":", 1)[1]
    s = s.strip().strip("#$").strip().removesuffix(".P").strip()
    return s or None


def _positive(value: float | None) -> float | None:
    if value is None or not math.isfinite(value) or value <= 0:
        return None
    return value


def _fmt(x: float | None) -> str:
    return "?" if x is None else f"{x:g}"


def _to_action(w: _WireAction, messages: list[StoredMessage], context: ParseContext) -> ParsedAction:
    """Map one wire action to ParsedAction and sanitize it defensively."""
    action = ActionType(w.action)
    extra_issues: list[str] = []

    def price(name: str, value: float | None) -> float | None:
        out = _positive(value)
        if value is not None and out is None:
            extra_issues.append(f"Model geçersiz bir {name} değeri döndürdü ({value}).")
        return out

    entry_price = price("giriş", w.entry_price)
    stop_loss = price("stop", w.stop_loss)
    new_stop_loss = price("yeni stop", w.new_stop_loss)

    risk_r = w.risk_r
    if risk_r is not None and (not math.isfinite(risk_r) or risk_r <= 0):
        extra_issues.append(f"Model geçersiz bir risk (R) değeri döndürdü ({risk_r}).")
        risk_r = None

    take_profits = _clean_take_profits(w.take_profits, extra_issues)

    close_fraction = w.close_fraction
    if close_fraction is not None:
        if math.isfinite(close_fraction) and 0 < close_fraction <= 1:
            pass
        elif math.isfinite(close_fraction) and 1 < close_fraction <= 100:
            extra_issues.append(f"Kapatma oranı yüzde olarak yorumlandı (%{close_fraction:g}).")
            close_fraction = close_fraction / 100
        else:
            extra_issues.append(f"Model geçersiz bir kapatma oranı döndürdü ({close_fraction}).")
            close_fraction = None

    # Ids: only messages actually given (new ones + the replied-to message).
    provided = [m.message_id for m in messages]
    allowed_used = set(provided)
    if context.reply_to is not None:
        allowed_used.add(context.reply_to.message_id)
    used: list[int] = []
    for i in w.used_message_ids:
        if i in allowed_used and i not in used:
            used.append(i)
    dropped = [i for i in w.used_message_ids if i not in allowed_used]
    if dropped:
        logger.warning("Dropping unknown used_message_ids %s", dropped)
    if not used and action in _NEEDS_IDS:
        used = list(provided)

    known_refs = set(allowed_used)
    known_refs.update(m.reply_to_message_id for m in messages if m.reply_to_message_id is not None)
    trade_ids: set[int] = set()
    for t in context.open_trades:
        tid = t.get("trade_id")
        if isinstance(tid, int):
            trade_ids.add(tid)
        for sid in t.get("source_message_ids") or []:
            if isinstance(sid, int):
                known_refs.add(sid)

    refers_to = w.refers_to_message_id
    if refers_to is not None and refers_to not in known_refs:
        logger.warning("Dropping unknown refers_to_message_id %s", refers_to)
        refers_to = None

    target_trade_id = w.target_trade_id
    if target_trade_id is not None and target_trade_id not in trade_ids:
        logger.warning("Dropping unknown target_trade_id %s", target_trade_id)
        extra_issues.append(f"Model listede olmayan bir işleme (#{target_trade_id}) atıf yaptı.")
        target_trade_id = None

    confidence = w.confidence if math.isfinite(w.confidence) else 0.0
    confidence = min(1.0, max(0.0, confidence))
    ambiguities = [a.strip() for a in w.ambiguities if a and a.strip()]
    if extra_issues:
        ambiguities.extend(extra_issues)
        confidence = min(confidence, _LOW_CONFIDENCE_CAP)

    base_asset = _clean_asset(w.base_asset)
    quote_asset = _clean_asset(w.quote_asset)
    if (
        base_asset and quote_asset and base_asset.endswith(quote_asset)
        and len(base_asset) - len(quote_asset) >= 2
    ):
        base_asset = base_asset[: -len(quote_asset)]  # "ETHUSDT" + "USDT" -> "ETH"

    symbol = w.symbol.strip() if w.symbol and w.symbol.strip() else None

    parsed = ParsedAction(
        action=action,
        symbol=symbol,
        base_asset=base_asset,
        quote_asset=quote_asset,
        market_type=MarketType(w.market_type) if w.market_type else None,
        side=Side(w.side) if w.side else None,
        entry_type=EntryType(w.entry_type) if w.entry_type else None,
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profits=take_profits,
        risk_r=risk_r,
        new_stop_loss=new_stop_loss,
        move_sl_to_entry=w.move_sl_to_entry,
        close_fraction=close_fraction,
        refers_to_message_id=refers_to,
        target_trade_id=target_trade_id,
        used_message_ids=used,
        confidence=confidence,
        ambiguities=ambiguities,
        summary_tr=w.summary_tr.strip(),
        reasoning_brief=w.evidence.strip(),
    )
    if not parsed.summary_tr:
        parsed.summary_tr = _fallback_summary(parsed)
    return parsed


def _clean_take_profits(raw: list[_WireTakeProfit], issues: list[str]) -> list[TakeProfit]:
    tps = [tp for tp in raw if _positive(tp.price) is not None]
    if len(tps) != len(raw):
        logger.warning("Dropped %d take profit(s) with invalid price", len(raw) - len(tps))
    if not tps:
        return []
    fractions = [tp.close_fraction for tp in tps]
    finite = all(math.isfinite(f) for f in fractions)
    if finite and all(f == 1.0 for f in fractions):
        pass  # no split given: the app splits
    elif finite and all(0 < f <= 1 for f in fractions) and abs(sum(fractions) - 1.0) <= 0.01:
        pass  # explicit split
    elif finite and all(f > 0 for f in fractions) and abs(sum(fractions) - 100.0) <= 1.0:
        fractions = [f / 100 for f in fractions]  # percentages, e.g. 30/30/40
    else:
        issues.append(
            f"TP kapatma oranları tutarsız (toplam {sum(fractions):g}); eşit bölünecek varsayıldı."
        )
        fractions = [1.0] * len(tps)
    return [TakeProfit(price=tp.price, close_fraction=f) for tp, f in zip(tps, fractions)]


def _fallback_summary(a: ParsedAction) -> str:
    asset = a.base_asset or a.symbol or "?"
    side = a.side.value.upper() if a.side else ""
    if a.action is ActionType.OPEN:
        tps = " / ".join(_fmt(tp.price) for tp in a.take_profits) or "yok"
        risk = f"{_fmt(a.risk_r)}R" if a.risk_r is not None else "varsayılan"
        return f"{asset} {side}, giriş {_fmt(a.entry_price)}, SL {_fmt(a.stop_loss)}, TP {tps}, risk {risk}"
    if a.action is ActionType.MOVE_SL:
        target = "girişe" if a.move_sl_to_entry else _fmt(a.new_stop_loss)
        return f"{asset} işleminde stop {target} çekilecek"
    if a.action is ActionType.MOVE_TP:
        return f"{asset} işleminde TP: " + (" / ".join(_fmt(tp.price) for tp in a.take_profits) or "yok")
    if a.action is ActionType.CLOSE:
        return f"{asset} işleminin {_fmt(a.close_fraction)} oranı kapatılacak"
    if a.action is ActionType.CANCEL:
        return f"{asset} işlemi iptal edilecek"
    if a.action is ActionType.INCOMPLETE:
        return f"{asset} {side} işlemi hazırlanıyor, bilgiler eksik"
    return "İşlem talimatı yok"
