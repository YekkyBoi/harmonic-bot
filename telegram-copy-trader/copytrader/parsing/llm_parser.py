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
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import anthropic
from pydantic import BaseModel, ConfigDict, ValidationError

from ..config import LLMConfig, fold_tr
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
from .prefilter import is_bare_status
from .symbols import FIAT_CURRENCIES, normalize_symbol

logger = logging.getLogger(__name__)

FALLBACK_BETA = "server-side-fallback-2026-07-01"   # pairs with fallbacks="default"
MAX_IMAGE_B64_BYTES = 5 * 1024 * 1024               # API per-image limit (base64 payload)
_LOW_CONFIDENCE_CAP = 0.6                           # cap when post-processing had to fix the output
# The engine owns retries (it notifies the owner and re-reads the messages): the SDK retries a
# failed HTTP attempt at most once, and the whole create() call is bounded by one overall deadline.
SDK_MAX_RETRIES = 1
CONNECT_TIMEOUT_SEC = 10.0
_MIN_DEADLINE_SEC = 0.1

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
    # minItems 1 (supported by structured outputs): "no instruction" is one "none" action, never [].
    "properties": {"actions": {"type": "array", "items": _ACTION_SCHEMA, "minItems": 1}},
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
_ERR_REFUSAL_BUSY = (
    "Yapay zekâ bu mesajı reddetti ve yedek model şu an meşgul (geçici durum); sinyal okunamadı."
)
_ERR_MAX_TOKENS = "Yapay zekâ yanıtı yarıda kesildi (token limiti); sinyal okunamadı."
_ERR_STOP = "Yapay zekâ yanıtı beklenmedik şekilde bitti ({reason}); sinyal okunamadı."
_ERR_NO_TEXT = "Yapay zekâ boş yanıt döndürdü; sinyal okunamadı."
_ERR_BAD_JSON = "Yapay zekâ geçersiz bir yanıt döndürdü; sinyal okunamadı."
_ERR_BAD_BODY = "Yapay zekâ servisinden okunamayan bir yanıt geldi; sinyal okunamadı."
_ERR_TIMEOUT = "Yapay zekâ servisi zamanında yanıt vermedi; sinyal okunamadı."
_ERR_CONNECTION = "Yapay zekâ servisine bağlanılamadı; sinyal okunamadı."
_ERR_RATE_LIMIT = "Yapay zekâ servisi istek limitine takıldı; sinyal okunamadı."
_ERR_SPEND_LIMIT = (
    "Anthropic API aylık harcama limiti doldu; limit Claude Console'da (Settings → Limits) "
    "yükseltilene kadar sinyaller okunamaz."
)
_ERR_AUTH = "Anthropic API anahtarı geçersiz veya yetkisiz; ayarları kontrol et."
_ERR_NO_KEY = (
    "Anthropic API anahtarı bulunamadı: config.yaml'da llm.api_key ya da ANTHROPIC_API_KEY "
    "ortam değişkenini ayarla."
)
_ERR_BAD_REQUEST = "Yapay zekâ isteği reddetti (HTTP 400); sinyal okunamadı."
_ERR_STATUS = "Yapay zekâ servisi hata döndürdü (HTTP {code}); sinyal okunamadı."
_ERR_OTHER = "Yapay zekâ servisine erişilemedi (yapılandırma hatası); sinyal okunamadı."

# Ambiguities added by post-processing (shown to the owner; any ambiguity makes the engine ask).
_AMB_NO_ACTIONS = "Yapay zekâ bu mesajlar için hiçbir sonuç döndürmedi; işlem talimatı olup olmadığını kontrol et."
_AMB_IDS_GUESSED = (
    "Yapay zekâ bu talimatın hangi mesajdan geldiğini belirtmedi; aynı yanıtta başka talimatlar da var."
)
_AMB_IMAGE_MISSING = (
    "Mesajdaki grafik görseli okunamadı (indirilemedi ya da açılamadı); seviyeler görselden doğrulanamadı."
)
_AMB_BARE_STATUS = (
    "Mesajda yalnızca “{text}” yazıyor; ne olduğu (giriş mi doldu, hedef mi geldi, stop mu oldu) belli değil, "
    "bu yüzden sormadan uygulanmadı."
)
_SPEND_LIMIT_CODE = "enforced_spend_limit_reached"

_MANAGEMENT = {ActionType.MOVE_SL, ActionType.MOVE_TP, ActionType.CLOSE, ActionType.CANCEL}
_NEEDS_IDS = _MANAGEMENT | {ActionType.OPEN, ActionType.INCOMPLETE}


class ClaudeSignalParser:
    """Turns Telegram signal messages into ``ParsedAction``s with Claude."""

    def __init__(
        self, config: LLMConfig, client: Any | None = None, *, deadline_sec: float | None = None
    ) -> None:
        """``client``: an anthropic.AsyncAnthropic (injected in tests); created from config if None.

        One ``parse()`` API call never takes longer than ``config.timeout_sec`` in total (SDK retry
        included), or ``deadline_sec`` when that is smaller (callers pass something well below
        ``telegram.context_window_sec`` so the engine's retry still finds the messages).
        Raises ``ParserError`` when it has to build the client and no API credentials are configured."""
        self._config = config
        self._owns_client = client is None
        timeout = max(_MIN_DEADLINE_SEC, float(config.timeout_sec))
        self._deadline_sec = timeout if deadline_sec is None else max(_MIN_DEADLINE_SEC, min(timeout, deadline_sec))
        if client is None:
            # api_key=None lets the SDK resolve ANTHROPIC_API_KEY (or other env credentials).
            api_key = config.api_key.strip() if config.api_key and config.api_key.strip() else None
            try:
                client = anthropic.AsyncAnthropic(
                    api_key=api_key,
                    timeout=anthropic.Timeout(timeout, connect=min(CONNECT_TIMEOUT_SEC, timeout)),
                    max_retries=SDK_MAX_RETRIES,
                )
            except (anthropic.AnthropicError, ValueError) as e:   # e.g. a broken credentials profile
                logger.error("Cannot set up Anthropic credentials: %s", e)
                raise ParserError(_ERR_NO_KEY, transient=False) from e
            if not _has_credentials(client):
                logger.error("No Anthropic API credentials (llm.api_key / ANTHROPIC_API_KEY)")
                raise ParserError(_ERR_NO_KEY, transient=False)
        self._client: Any = client

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
        unseen: set[int] = set()        # ids of messages whose picture could not be given to the model
        content = await self._build_content(messages, context, unseen)
        request = self.build_request(content)

        try:
            response = await asyncio.wait_for(
                self._client.beta.messages.create(**request), self._deadline_sec
            )
        except TimeoutError as e:   # overall deadline (asyncio.TimeoutError is TimeoutError)
            logger.warning("Claude request exceeded the %.0fs deadline", self._deadline_sec)
            raise ParserError(_ERR_TIMEOUT, transient=True) from e
        except anthropic.APITimeoutError as e:
            logger.warning("Claude request timed out: %s", e)
            raise ParserError(_ERR_TIMEOUT, transient=True) from e
        except anthropic.APIConnectionError as e:
            logger.warning("Claude connection error: %s", e)
            raise ParserError(_ERR_CONNECTION, transient=True) from e
        except anthropic.RateLimitError as e:
            if _is_spend_limit(e):
                logger.error("Anthropic spend limit reached: %s", e)
                raise ParserError(_ERR_SPEND_LIMIT, transient=False) from e
            logger.warning("Claude rate limited: %s", e)
            raise ParserError(_ERR_RATE_LIMIT, transient=True) from e
        except (anthropic.AuthenticationError, anthropic.PermissionDeniedError) as e:
            logger.error("Claude auth error: %s", e)
            raise ParserError(_ERR_AUTH, transient=False) from e
        except anthropic.BadRequestError as e:
            logger.error("Claude rejected the request (400): %s", e)
            raise ParserError(_ERR_BAD_REQUEST, transient=False) from e
        except anthropic.APIStatusError as e:
            logger.warning("Claude API error %s: %s", e.status_code, e)
            transient = e.status_code >= 500 or e.status_code in (408, 409)
            raise ParserError(_ERR_STATUS.format(code=e.status_code), transient=transient) from e
        except (anthropic.APIResponseValidationError, json.JSONDecodeError) as e:
            # HTTP 200 whose body is not the API's JSON (empty or cut off, a proxy page): worth another try.
            logger.warning("Claude answered with an unreadable body: %s", e)
            raise ParserError(_ERR_BAD_BODY, transient=True) from e
        except anthropic.AnthropicError as e:
            logger.error("Claude client error: %s", e)
            raise ParserError(_ERR_OTHER, transient=False) from e
        except TypeError as e:
            # The SDK raises a bare TypeError when no API key / credentials could be resolved.
            if "authentication" not in str(e).lower():
                raise
            logger.error("No Anthropic API credentials: %s", e)
            raise ParserError(_ERR_NO_KEY, transient=False) from e

        return self._handle_response(response, messages, context, unseen)

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
        self, messages: list[StoredMessage], context: ParseContext, unseen: set[int] | None = None
    ) -> list[dict[str, Any]]:
        """The user turn. ``unseen`` (when given) receives the ids of the messages, the replied-to one
        included, whose picture exists but could not be attached (not downloaded, unreadable, too large)."""
        budget = max(0, self._config.max_images)
        msg_ids = {m.message_id for m in messages}
        unseen = unseen if unseen is not None else set()
        not_downloaded = prompts.IMAGE_UNREADABLE_NOTE.format(reason=prompts.IMAGE_NOT_DOWNLOADED_REASON)

        # Pick the most recent images first; unreadable ones don't use budget.
        images: dict[int, dict[str, Any]] = {}
        notes: dict[int, str] = {}
        for m in reversed(messages):
            if not m.media_path:
                if m.media_type:
                    # A photo / image document whose download failed: the model must know a picture exists.
                    notes[m.message_id] = not_downloaded
                    unseen.add(m.message_id)
                continue
            if len(images) >= budget:
                notes[m.message_id] = prompts.IMAGE_LIMIT_NOTE
                continue
            block, reason = await _load_image_block(m.media_path)
            if block is None:
                notes[m.message_id] = prompts.IMAGE_UNREADABLE_NOTE.format(reason=reason)
                unseen.add(m.message_id)
            else:
                images[m.message_id] = block

        content: list[dict[str, Any]] = [
            {"type": "text", "text": prompts.USER_INTRO.format(now=_iso(context.now))}
        ]
        topic = _topic_line(context)
        if topic is not None:
            content.append({"type": "text", "text": topic})
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
                            unseen.add(reply.message_id)
                elif reply.media_type:
                    note = not_downloaded
                    unseen.add(reply.message_id)
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
        self, response: Any, messages: list[StoredMessage], context: ParseContext,
        unseen: set[int] | None = None,
    ) -> ParseResult:
        """``unseen``: ids of the messages whose picture could not be given to the model (see _build_content).

        Failures worth another try (``ParserError.transient`` True; the engine's retry loop reads that flag):
        a body that is not a Messages API object, a reply cut by the token limit, a reply without text and
        one that is not valid JSON of the schema. A refusal (unless the fallback model was only busy) and an
        unknown stop reason are final."""
        if response is None or isinstance(response, (str, bytes)):
            # The SDK hands back the raw text when the body was not JSON (a proxy / gateway page).
            logger.warning("Claude answered with a body that is not a message: %.200r", response)
            raise ParserError(_ERR_BAD_BODY, transient=True)
        stop_reason = getattr(response, "stop_reason", None)
        model = getattr(response, "model", None)
        request_id = getattr(response, "_request_id", None)

        if stop_reason == "refusal":
            details = getattr(response, "stop_details", None)
            recommended = getattr(details, "recommended_model", None)
            logger.warning(
                "Claude refused (model=%s, request_id=%s, category=%s, recommended_model=%s)",
                model, request_id, getattr(details, "category", None), recommended,
            )
            if recommended:
                # The fallback model could not run (rate limited / overloaded): a later retry can succeed.
                raise ParserError(_ERR_REFUSAL_BUSY, transient=True)
            raise ParserError(_ERR_REFUSAL, transient=False)
        if stop_reason == "max_tokens":
            # The length of the model's own deliberation varies from call to call: another try can fit.
            logger.warning("Claude hit max_tokens=%s (request_id=%s)", self._config.max_tokens, request_id)
            raise ParserError(_ERR_MAX_TOKENS, transient=True)
        if stop_reason not in ("end_turn", "stop_sequence"):
            logger.warning("Unexpected stop_reason %r (request_id=%s)", stop_reason, request_id)
            raise ParserError(_ERR_STOP.format(reason=stop_reason), transient=False)

        text = _final_text(getattr(response, "content", None) or [])
        if text is None:
            logger.warning("Claude response has no text block (request_id=%s)", request_id)
            raise ParserError(_ERR_NO_TEXT, transient=True)

        try:
            data = json.loads(text, parse_constant=_reject_constant)
            wire = _WireResult.model_validate(data)
        except (ValueError, ValidationError) as e:   # JSONDecodeError is a ValueError
            logger.warning("Claude output does not match the schema: %s; raw=%.500s", e, text)
            raise ParserError(_ERR_BAD_JSON, transient=True) from e

        usage = getattr(response, "usage", None)
        if usage is not None:
            logger.debug(
                "Claude usage: in=%s out=%s cache_read=%s cache_write=%s",
                getattr(usage, "input_tokens", None), getattr(usage, "output_tokens", None),
                getattr(usage, "cache_read_input_tokens", None),
                getattr(usage, "cache_creation_input_tokens", None),
            )

        if wire.actions:
            actions = _fill_missing_ids(
                [_to_action(w, messages, context) for w in wire.actions], [m.message_id for m in messages]
            )
            actions = [_check_trader_sizing(_apply_topic_kind(a, context), messages, context) for a in actions]
            actions = [_flag_bare_status(_flag_unseen_image(a, unseen or set()), messages) for a in actions]
        else:
            # Schema-valid but against the rules ("no instruction" is one "none"): never consume the
            # messages silently; an unsure "none" makes the engine ask the owner.
            logger.warning("Claude returned no actions (request_id=%s)", request_id)
            actions = [ParsedAction(
                action=ActionType.NONE, confidence=0.0, ambiguities=[_AMB_NO_ACTIONS],
                summary_tr="Yapay zekâ sonuç döndürmedi",
            )]
        logger.info(
            "Parsed %d message(s) -> %s (model=%s)",
            len(messages), [f"{a.action.value}:{a.base_asset}:{a.confidence:.2f}" for a in actions], model,
        )
        return ParseResult(actions=actions, model=model, raw_response=text)


# --------------------------------------------------------------------------- #
# Client / error helpers
# --------------------------------------------------------------------------- #
def _has_credentials(client: Any) -> bool:
    """True if the SDK client resolved an API key, auth token, credential provider or auth header."""
    if any(getattr(client, name, None) for name in ("api_key", "auth_token", "credentials")):
        return True
    try:
        headers = dict(getattr(client, "default_headers", None) or {})
    except Exception:   # pragma: no cover - defensive; never block startup on introspection
        return True
    return any(str(k).lower() in ("authorization", "x-api-key") and isinstance(v, str) and v for k, v in headers.items())


def _is_spend_limit(err: BaseException) -> bool:
    """429 because the organization's monthly spend limit is reached (retrying cannot help)."""
    cls = getattr(anthropic, "EnforcedSpendLimitReachedError", None)
    if cls is not None and isinstance(err, cls):
        return True
    return _SPEND_LIMIT_CODE in (getattr(err, "error_code", None), getattr(err, "type", None))


# --------------------------------------------------------------------------- #
# Content helpers
# --------------------------------------------------------------------------- #
def _as_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _iso(dt: datetime) -> str:
    return _as_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


def _topic_line(context: ParseContext) -> str | None:
    """'Konu: <title> (tür: fx)' for a forum topic, None when the messages come from no topic."""
    title = " ".join((context.topic_title or "").split())
    if not title:
        return None
    if context.topic_kind:
        return prompts.TOPIC_LINE.format(title=title, kind=context.topic_kind)
    return prompts.TOPIC_LINE_NO_KIND.format(title=title)


def _render_message(m: StoredMessage, note: str | None = None) -> str:
    parts = [f"msg_id={m.message_id}", _iso(m.date)]
    sender = m.sender_name or "?"
    parts.append(f"from={sender} (id {m.sender_id})" if m.sender_id is not None else f"from={sender}")
    # In a forum topic every message "replies" to the topic's root message: that is not a reply.
    if m.reply_to_message_id is not None and m.reply_to_message_id != m.topic_id:
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
    # No usable ids: left empty here; _fill_missing_ids assigns them knowing the other actions.

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


# --------------------------------------------------------------------------- #
# Forum topic kind and the trader's own sizing note
# --------------------------------------------------------------------------- #
_OPENING = {ActionType.OPEN, ActionType.INCOMPLETE}
_FX_USD_QUOTED = frozenset({"EUR", "GBP", "AUD", "NZD"})   # "EUR" in an fx topic = EURUSD
_METALS = frozenset({"XAU", "XAG"})
_CRYPTO_MARKETS = {MarketType.PERP, MarketType.SPOT}
_FX_MARKETS = {MarketType.FOREX, MarketType.CFD}
# "100 risk için 8.7 lot", "100$ risk için 8,7 lot" (text folded with config.fold_tr).
_LOT_NOTE = re.compile(
    r"(?<![\d.,])(\d+(?:[.,]\d+)?)\s*(?:\$|usdt|usd|dolar|dolarlık)?\s*risk\s+[iı][çc][iı]n\s+"
    r"(\d+(?:[.,]\d+)?)\s*lot"
)
LOT_CHECK_TOLERANCE = 0.25            # |entry - stop| vs X / Y of "X risk için Y lot"
_AMB_TOPIC_KIND = "{kind} konusundaki sinyal {market} olarak okundu; enstrüman yanlış okunmuş olabilir."
_AMB_LOT = (
    "Trader'ın notu ({note}) stop mesafesinin ≈ {expected} olduğunu gösteriyor, okunan giriş ({entry}) ile stop "
    "({stop}) arası {actual} (%{dev} fark); seviyeler yanlış okunmuş olabilir."
)


def _apply_topic_kind(a: ParsedAction, context: ParseContext) -> ParsedAction:
    """Fill what the forum topic's kind makes certain and flag a contradiction (new signals only):
    fx topic: EUR/GBP/AUD/NZD and XAU/XAG without a quote -> the fiat quote their own symbol names ("EURGBP"),
    else USD; a missing market type -> forex for a currency, cfd for everything else (metals, indices, oil);
    crypto topic: USDT perpetual."""
    kind = context.topic_kind
    if a.action not in _OPENING or kind not in ("crypto", "fx"):
        return a
    update: dict[str, Any] = {}
    base = (a.base_asset or "").upper()
    if kind == "fx":
        if a.market_type in _CRYPTO_MARKETS:
            update["ambiguities"] = [*a.ambiguities, _AMB_TOPIC_KIND.format(kind="Forex", market="kripto")]
        else:
            s_base, s_quote, s_type = normalize_symbol(a.symbol) if a.symbol else (None, None, None)
            if base in _FX_USD_QUOTED | _METALS and a.quote_asset in (None, "USD"):
                # "EUR" alone is EURUSD; a pair spelled out in the action's own symbol keeps its quote.
                named = a.quote_asset is None and s_base == base and s_quote in FIAT_CURRENCIES and s_quote != base
                update["quote_asset"] = s_quote if named else "USD"
            asset = base or s_base
            # A perpetual written in the symbol ("ETHUSDT.P") is not relabelled.
            if a.market_type is None and asset and s_type is not MarketType.PERP:
                update["market_type"] = MarketType.FOREX if asset in FIAT_CURRENCIES else MarketType.CFD
    else:
        if a.market_type in _FX_MARKETS:
            update["ambiguities"] = [*a.ambiguities, _AMB_TOPIC_KIND.format(kind="Kripto", market="forex/CFD")]
        elif base:
            if a.market_type is None:
                update["market_type"] = MarketType.PERP
            if a.quote_asset is None:
                update["quote_asset"] = "USDT"
    if "ambiguities" in update:
        update["confidence"] = min(a.confidence, _LOW_CONFIDENCE_CAP)
    return a.model_copy(update=update) if update else a


def _number(raw: str) -> float | None:
    """'8.7' / '8,7' -> 8.7; a separator before exactly three digits ('1.000') is ambiguous -> None."""
    if re.search(r"[.,]\d{3}$", raw):
        return None
    try:
        value = float(raw.replace(",", "."))
    except ValueError:
        return None
    return value if math.isfinite(value) and value > 0 else None


def _check_trader_sizing(a: ParsedAction, messages: list[StoredMessage], context: ParseContext) -> ParsedAction:
    """"X risk için Y lot" is the trader's own size (never used for sizing). On a crypto signal Y lot = Y coins,
    so |entry - stop| should be about X / Y: far off (> LOT_CHECK_TOLERANCE) means a level was probably
    misread -> ambiguity + confidence cap (the owner is asked)."""
    if a.action not in _OPENING or a.entry_price is None or a.stop_loss is None:
        return a
    crypto = context.topic_kind == "crypto" or (context.topic_kind is None and a.market_type in _CRYPTO_MARKETS)
    if not crypto or a.market_type in _FX_MARKETS:
        return a
    used = set(a.used_message_ids)
    sources = [m for m in messages if not used or m.message_id in used]
    if context.reply_to is not None and context.reply_to.message_id in used:
        sources.append(context.reply_to)
    for m in sources:
        found = _LOT_NOTE.search(fold_tr(m.text or ""))
        if found is None:
            continue
        risk_amount, lots = _number(found.group(1)), _number(found.group(2))
        if risk_amount is None or lots is None:
            return a
        expected = risk_amount / lots
        actual = abs(a.entry_price - a.stop_loss)
        deviation = abs(actual - expected) / expected
        if deviation <= LOT_CHECK_TOLERANCE:
            return a
        logger.warning("Sizing note %r implies a stop distance of %.6g, the levels read give %.6g",
                       found.group(0), expected, actual)
        text = _AMB_LOT.format(note=found.group(0), expected=f"{expected:.6g}", entry=_fmt(a.entry_price),
                               stop=_fmt(a.stop_loss), actual=f"{actual:.6g}", dev=f"{deviation * 100:.0f}")
        return a.model_copy(update={"ambiguities": [*a.ambiguities, text],
                                    "confidence": min(a.confidence, _LOW_CONFIDENCE_CAP)})
    return a


_IMAGE_WORDS = ("görsel", "gorsel", "görüntü", "goruntu", "resim", "screenshot")   # in text folded with fold_tr


def _flag_unseen_image(a: ParsedAction, unseen: set[int]) -> ParsedAction:
    """A new trade ("open" / "incomplete") built on a message whose picture the model could not see: the levels
    cannot have been checked against the screenshot. The owner is told so in an ambiguity (any ambiguity makes
    the engine ask), unless the model already said it."""
    if a.action not in _OPENING or not unseen.intersection(a.used_message_ids):
        return a
    if any(word in fold_tr(text) for text in a.ambiguities for word in _IMAGE_WORDS):
        return a
    return a.model_copy(update={"ambiguities": [*a.ambiguities, _AMB_IMAGE_MISSING]})


def _flag_bare_status(a: ParsedAction, messages: list[StoredMessage]) -> ParsedAction:
    """A management action read from nothing but a bare status word or mark ("Geldi", "Tamamdır", a lone "✅";
    see ``prefilter.is_bare_status``): the text does not say what happened, so the reading is a guess. It gets
    an ambiguity (the engine then asks the owner instead of acting), unless the model already raised one."""
    if a.action not in _MANAGEMENT or a.ambiguities:
        return a
    used = [m for m in messages if m.message_id in a.used_message_ids] or list(messages)
    if not used or not all(is_bare_status(m) for m in used):
        return a
    text = " / ".join(" ".join(m.text.split()) for m in used)
    text = text if len(text) <= 40 else text[:39] + "…"
    return a.model_copy(update={"ambiguities": [_AMB_BARE_STATUS.format(text=text)]})


def _fill_missing_ids(actions: list[ParsedAction], provided: list[int]) -> list[ParsedAction]:
    """Give actions that need message ids but got none from the model the batch messages no OTHER
    action claims. When no other instruction claims any message it simply gets all of them (or
    those a "none" left over). When other instructions claim some, the split is a guess: it never
    takes messages an "incomplete" action keeps (if it can avoid it), and it gets an ambiguity
    plus a confidence cap so the owner is asked."""
    out: list[ParsedAction] = []
    for idx, a in enumerate(actions):
        if a.used_message_ids or a.action not in _NEEDS_IDS:
            out.append(a)
            continue
        others = [b for j, b in enumerate(actions) if j != idx]
        claimed = {i for b in others for i in b.used_message_ids}
        free = [i for i in provided if i not in claimed]
        update: dict[str, Any]
        if not any(b.used_message_ids for b in others if b.action is not ActionType.NONE):
            update = {"used_message_ids": free or list(provided)}
        else:
            held = {i for b in others if b.action is ActionType.INCOMPLETE for i in b.used_message_ids}
            ids = free or [i for i in provided if i not in held] or list(provided)
            logger.warning("%s action without used_message_ids next to other actions; assigned %s",
                           a.action.value, ids)
            update = {
                "used_message_ids": ids,
                "ambiguities": [*a.ambiguities, _AMB_IDS_GUESSED],
                "confidence": min(a.confidence, _LOW_CONFIDENCE_CAP),
            }
        out.append(a.model_copy(update=update))
    return out


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
