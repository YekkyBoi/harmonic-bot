"""Owner notification bot (Telethon client logged in as a BOT).

Talks to ``notifier.owner_user_id`` only; everybody else is ignored.
Texts are Telegram HTML. Text passed to :meth:`BotNotifier.notify` and text
returned by the engine is treated as HTML that our own code produced (use
``formatting.esc`` for anything untrusted inside it).
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import OrderedDict, deque
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

from telethon import Button, TelegramClient, errors, events, utils
from telethon.extensions import html as tl_html

from .. import __version__
from ..config import NotifierConfig, TelegramConfig
from ..interfaces import SHUTDOWN_TEXT, EngineControl
from ..models import ConfirmationStatus, PendingConfirmation, Severity
from .formatting import CORRECTION_HINT, esc, format_confirmation

log = logging.getLogger(__name__)

MAX_CHUNK = 4000                  # Telegram limit is 4096 chars after entity parsing
CLOSEALL_TTL_SEC = 120.0          # a "close all" yes-button older than this is refused
STOP_GRACE_SEC = 30.0
_BACKLOG_SIZE = 20
_CACHE_SIZE = 500

STARTUP_TEXT = "🤖 Kopya-trade botu çalışıyor\nKomutlar için /yardim yazın."
HELP_TEXT = (
    "🤖 <b>Kopya-trade botu</b>\n"
    "Kaynak gruptaki işlemleri otomatik kopyalar; emin olamadığında size sorar.\n\n"
    "/durum — genel durum\n"
    "/pozisyonlar — açık işlemler\n"
    "/bakiye — broker bakiyeleri\n"
    "/durdur — yeni işlem açmayı durdur\n"
    "/devam — yeni işlem açmaya devam et\n"
    "/hepsini_kapat — tüm pozisyonları kapat (onay ister)\n"
    "/test &lt;metin&gt; — bir mesajın nasıl anlaşılacağını dene (işlem açmaz)\n"
    "/yardim — bu mesaj\n\n"
    "Onay mesajlarında: ✅ Onayla · ✏️ Düzelt (doğrusunu yazarsınız) · ❌ İptal\n"
    "Bir onay mesajını yanıtlayarak da düzeltme yazabilirsiniz."
)
PLAIN_HINT = (
    "ℹ️ Şu an düzeltme beklenmiyor. Komutlar için /yardim yazın.\n"
    "Bir mesajın nasıl anlaşılacağını görmek için: <code>/test eth 1R long</code>"
)
_PREFIX = {Severity.INFO: "ℹ️", Severity.WARNING: "⚠️", Severity.ERROR: "🚨"}
_STATUS_OUTCOME = {
    ConfirmationStatus.APPROVED: "✅ Onaylandı",
    ConfirmationStatus.REJECTED: "❌ İptal edildi",
    ConfirmationStatus.EXPIRED: "⌛ Süresi doldu — işlem yapılmadı",
    ConfirmationStatus.SUPERSEDED: "♻️ Düzeltilmiş hali ayrıca gönderildi",
    ConfirmationStatus.PENDING: "⏳ Beklemede",
}
_OWNER_UNREACHABLE = (
    errors.UserIsBlockedError,
    errors.PeerIdInvalidError,
    errors.InputUserDeactivatedError,
)
_COMMANDS = {
    "start": "help", "yardim": "help", "yardım": "help", "help": "help",
    "durum": "status", "status": "status",
    "pozisyonlar": "positions", "positions": "positions",
    "bakiye": "balance", "balance": "balance",
    "durdur": "pause", "pause": "pause",
    "devam": "resume", "resume": "resume",
    "hepsini_kapat": "closeall", "closeall": "closeall",
    "test": "test",
}
_COMMAND_RE = re.compile(r"^/([^\s@]+)(?:@\S+)?\s*(.*)$", re.S)


def split_html(text: str, limit: int = MAX_CHUNK) -> list[str | tuple[str, list[Any]]]:
    """Short text -> [html]; long text -> [(plain, entities), ...] split without breaking markup."""
    plain, entities = tl_html.parse(text)
    if len(plain) <= limit:
        return [text]
    return [(chunk, ents) for chunk, ents in utils.split_text(plain, entities, limit=limit)]


def _outcome(text: str) -> str:
    return f"<b>Sonuç:</b> {text.strip()}" if text and text.strip() else "<b>Sonuç:</b> —"


class BotNotifier:
    def __init__(self, config: NotifierConfig, telegram_config: TelegramConfig, data_dir: str) -> None:
        """Telethon client logged in as the bot (api_id/api_hash from telegram_config, token from config)."""
        self._config = config
        self._tg = telegram_config
        self._data_dir = Path(data_dir)
        self._owner = config.owner_user_id
        self._client: Any = None
        self._engine: EngineControl | None = None
        self._ignored: set[int | None] = set()
        self._inflight: set[str] = set()
        self._closed: OrderedDict[str, str] = OrderedDict()               # conf id -> outcome shown
        self._sent: OrderedDict[str, tuple[int, str]] = OrderedDict()     # conf id -> (msg id, closed html)
        self._by_msg: OrderedDict[int, str] = OrderedDict()               # bot msg id -> conf id
        self._backlog: deque[str] = deque(maxlen=_BACKLOG_SIZE)
        self._closeall_prompt: tuple[int, float] | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._unreachable_logged = False

    # ---- lifecycle -------------------------------------------------------
    async def start(self, engine: EngineControl) -> None:
        self._engine = engine
        if self._client is None:
            self._data_dir.mkdir(parents=True, exist_ok=True)
            self._client = TelegramClient(
                str(self._data_dir / "bot.session"),
                self._tg.api_id,
                self._tg.api_hash,
                device_model="copytrader-bot",
                app_version=__version__,
            )
        client = self._client
        try:
            await client.start(bot_token=self._config.bot_token)
        except errors.AccessTokenInvalidError as exc:
            raise RuntimeError("Bot token geçersiz (notifier.bot_token). @BotFather'dan doğru token'ı alın.") from exc
        me = await client.get_me()
        token_id = self._config.bot_token.split(":", 1)[0]
        if not getattr(me, "bot", False) or str(getattr(me, "id", "")) != token_id:
            await client.disconnect()
            raise RuntimeError(
                f"{self._data_dir / 'bot.session'} başka bir hesaba ait (bot token ile eşleşmiyor). "
                "Bu dosyayı silip yeniden başlatın."
            )
        if me.id == self._owner:
            raise RuntimeError("notifier.owner_user_id botun kendi ID'si olamaz; kendi kullanıcı ID'nizi yazın.")
        client.parse_mode = "html"
        client.add_event_handler(self._on_message_event, events.NewMessage(incoming=True))
        client.add_event_handler(self._on_callback_event, events.CallbackQuery())
        log.info("Notifier bot @%s started; owner id %s", getattr(me, "username", None), self._owner)
        if await self._send(STARTUP_TEXT, backlog=False) is None:
            log.error(
                "The bot cannot message the owner (user id %s). Open the bot in Telegram and press Start, "
                "and check notifier.owner_user_id. Confirmations cannot be delivered until then.",
                self._owner,
            )

    async def stop(self) -> None:
        running = [t for t in self._tasks if not t.done()]
        if running:
            _, pending = await asyncio.wait(running, timeout=STOP_GRACE_SEC)
            for task in pending:
                task.cancel()
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:
                log.exception("Error while disconnecting the bot client")

    # ---- Notifier API ----------------------------------------------------
    async def notify(self, text: str, severity: Severity = Severity.INFO) -> None:
        try:
            await self._send(f"{_PREFIX.get(severity, 'ℹ️')} {text}", backlog=True)
        except Exception:
            log.exception("notify() failed")

    async def ask_confirmation(self, conf: PendingConfirmation) -> int | None:
        """Send the 'I understood X, is that right?' message with buttons. Returns bot message id."""
        try:
            text = format_confirmation(conf)
            row = []
            if conf.plan is not None:      # blocking issues -> only correction / cancel
                row.append(Button.inline("✅ Onayla", f"ok:{conf.id}".encode()))
            row.append(Button.inline("✏️ Düzelt", f"fix:{conf.id}".encode()))
            row.append(Button.inline("❌ İptal", f"no:{conf.id}".encode()))
        except Exception:
            log.exception("Could not build confirmation %s", conf.id)
            return None
        msg_id = await self._send(text, buttons=[row], backlog=False)
        if msg_id is not None:
            self._closed.pop(conf.id, None)            # re-asked (e.g. after a correction): open again
            self._remember(self._sent, conf.id, (msg_id, format_confirmation(conf, with_expiry=False)))
            self._remember(self._by_msg, msg_id, conf.id)
        return msg_id

    async def close_confirmation(self, conf: PendingConfirmation, result_text: str) -> None:
        """Edit the confirmation message: remove buttons, append the outcome."""
        try:
            outcome = _outcome(result_text or _STATUS_OUTCOME.get(conf.status, ""))
            self._remember(self._closed, conf.id, result_text or "")
            if conf.bot_message_id is None:
                await self._send(outcome)
                return
            body = format_confirmation(conf, with_expiry=False) + "\n\n" + outcome
            if not await self._edit(conf.bot_message_id, body):
                await self._send(outcome, reply_to=conf.bot_message_id)
        except Exception:
            log.exception("close_confirmation failed for %s", conf.id)

    # ---- Telethon handlers (thin; spawn tracked tasks) ----------------------
    async def _on_message_event(self, event: Any) -> None:
        self._spawn(self.handle_message(event))

    async def _on_callback_event(self, event: Any) -> None:
        self._spawn(self.handle_callback(event))

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        # Own tasks: a disconnect must not cancel an approval that is placing orders.
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)
        return task

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            log.error("Notifier task failed", exc_info=task.exception())

    async def drain(self, timeout: float | None = None) -> None:
        """Wait for running handler tasks (tests / shutdown)."""
        if self._tasks:
            await asyncio.wait(list(self._tasks), timeout=timeout)

    # ---- incoming messages (testable) ---------------------------------------
    def _is_owner(self, sender_id: int | None, kind: str) -> bool:
        if sender_id == self._owner:
            return True
        if sender_id not in self._ignored:
            self._ignored.add(sender_id)
            log.warning("Ignoring %s from non-owner user %s", kind, sender_id)
        return False

    async def handle_message(self, event: Any) -> None:
        if getattr(event, "out", False):
            return
        if not self._is_owner(getattr(event, "sender_id", None), "message"):
            return
        if not getattr(event, "is_private", True):
            return                       # owner typing in a group the bot is in: stay silent there
        await self._flush_backlog()
        text = (getattr(event, "raw_text", None) or "").strip()
        try:
            await self._handle_owner_text(text, getattr(event, "reply_to_msg_id", None))
        except Exception as exc:
            log.exception("Owner message handling failed")
            await self._send(f"🚨 Hata: <code>{esc(exc)}</code>")

    async def _handle_owner_text(self, text: str, reply_to: int | None) -> None:
        if not text:
            await self._send(PLAIN_HINT)
            return
        m = _COMMAND_RE.match(text)
        if m:
            await self._run_command(m.group(1).lower(), m.group(2).strip())
            return
        engine = self._engine
        if engine is None:
            await self._send("⏳ Sistem henüz hazır değil, biraz sonra tekrar deneyin.")
            return
        conf_id = self._by_msg.get(reply_to) if reply_to is not None else None
        if conf_id is not None and conf_id not in self._closed:
            await engine.request_correction(conf_id)     # reply to a confirmation = correction
        else:
            conf_id = await engine.awaiting_correction_id()
        if conf_id is None:
            await self._send(PLAIN_HINT)
            return
        await self._send("⏳ Düzeltme alındı, yeniden değerlendiriliyor…")
        reply = await engine.handle_correction(conf_id, text)
        if reply:
            await self._send(reply)

    async def _run_command(self, name: str, args: str) -> None:
        command = _COMMANDS.get(name)
        engine = self._engine
        if command is None:
            await self._send(f"❓ Bilinmeyen komut: /{esc(name)}\nKomutlar için /yardim yazın.")
            return
        if command == "help":
            await self._send(HELP_TEXT)
            return
        if engine is None:
            await self._send("⏳ Sistem henüz hazır değil, biraz sonra tekrar deneyin.")
            return
        if command == "status":
            reply = await engine.status_text()
        elif command == "positions":
            reply = await engine.positions_text()
        elif command == "balance":
            reply = await engine.balances_text()
        elif command == "pause":
            reply = await engine.set_paused(True)
        elif command == "resume":
            reply = await engine.set_paused(False)
        elif command == "closeall":
            await self._ask_close_all()
            return
        else:                            # test
            if not args:
                await self._send("Kullanım: <code>/test eth 1R long, giriş 2232, SL 2205, TP 2310</code>")
                return
            await self._send("⏳ Deneme amaçlı çözümleniyor (işlem açılmaz)…")
            reply = await engine.test_parse(args)
        await self._send(reply or "✔️")

    async def _ask_close_all(self) -> None:
        buttons = [[
            Button.inline("✅ Evet, hepsini kapat", b"closeall:yes"),
            Button.inline("❌ Vazgeç", b"closeall:no"),
        ]]
        msg_id = await self._send(
            "⚠️ <b>Tüm açık pozisyonlar kapatılsın ve bekleyen emirler iptal edilsin mi?</b>\n"
            f"Bu işlem geri alınamaz. ({int(CLOSEALL_TTL_SEC // 60)} dk içinde onaylayın.)",
            buttons=buttons,
        )
        if msg_id is not None:
            self._closeall_prompt = (msg_id, time.monotonic())

    # ---- button presses (testable) ------------------------------------------
    async def handle_callback(self, event: Any) -> None:
        if not self._is_owner(getattr(event, "sender_id", None), "button press"):
            return
        data = bytes(getattr(event, "data", None) or b"").decode("utf-8", "replace")
        if data.startswith("closeall:"):
            await self._handle_close_all(event, data.partition(":")[2])
            return
        kind, _, conf_id = data.partition(":")
        if kind not in ("ok", "no", "fix") or not conf_id:
            await self._answer(event, "Bilinmeyen buton")
            return
        engine = self._engine
        if engine is None:
            await self._answer(event, "Sistem henüz hazır değil", alert=True)
            return
        if conf_id in self._inflight:
            await self._answer(event, "⏳ Zaten işleniyor…")
            return
        self._inflight.add(conf_id)      # before any await: blocks double clicks
        try:
            if kind == "fix":
                await self._answer(event, "✏️ Doğrusunu yazın")
                reply = await engine.request_correction(conf_id)
                if reply == SHUTDOWN_TEXT:
                    await self._send(reply)
                    return
                await self._send(f"{reply.strip()}\n\n{CORRECTION_HINT}" if reply and reply.strip() else CORRECTION_HINT)
            else:
                approved = kind == "ok"
                await self._answer(event, "⏳ Onaylandı, işleniyor…" if approved else "❌ İptal ediliyor…")
                result = await engine.handle_confirmation(conf_id, approved)
                if result == SHUTDOWN_TEXT:
                    # Nothing was decided: the confirmation stays pending and is restored after the restart,
                    # so its buttons stay on the message (the owner can approve it again then).
                    await self._send(result)
                    return
                await self._after_decision(conf_id, getattr(event, "message_id", None), result)
        except Exception as exc:
            log.exception("Button %s for confirmation %s failed", kind, conf_id)
            await self._send(f"🚨 İşlem sırasında hata: <code>{esc(exc)}</code>")
        finally:
            self._inflight.discard(conf_id)

    async def _after_decision(self, conf_id: str, msg_id: int | None, result: str) -> None:
        shown = self._closed.get(conf_id)
        if shown is not None:            # engine already closed the message itself
            if result and result.strip() and result.strip() != shown.strip():
                await self._send(result)
            return
        self._remember(self._closed, conf_id, result or "")
        if msg_id is None:
            await self._send(_outcome(result))
            return
        base = await self._original_html(conf_id, msg_id)
        body = (base + "\n\n" if base else "") + _outcome(result)
        if not await self._edit(msg_id, body):
            await self._send(_outcome(result))

    async def _original_html(self, conf_id: str, msg_id: int) -> str:
        sent = self._sent.get(conf_id)
        if sent and sent[0] == msg_id:
            return sent[1]
        try:                             # after a restart: re-read the message
            msg = await self._client.get_messages(self._owner, ids=msg_id)
        except Exception as exc:
            log.debug("Could not fetch confirmation message %s: %s", msg_id, exc)
            return ""
        text = getattr(msg, "text", None) or ""
        return "\n".join(line for line in text.splitlines() if not line.startswith("⏳"))

    async def _handle_close_all(self, event: Any, choice: str) -> None:
        msg_id = getattr(event, "message_id", None)
        prompt = self._closeall_prompt
        valid = prompt is not None and prompt[0] == msg_id and time.monotonic() - prompt[1] <= CLOSEALL_TTL_SEC
        if valid:
            self._closeall_prompt = None  # one use only (before any await)
        if choice != "yes":
            await self._answer(event, "Vazgeçildi")
            if msg_id is not None:
                await self._edit(msg_id, "❌ Vazgeçildi, hiçbir pozisyon kapatılmadı.")
            return
        if not valid:
            await self._answer(event, "Bu onayın süresi doldu; /hepsini_kapat komutunu tekrar gönderin.", alert=True)
            if msg_id is not None:
                await self._edit(msg_id, "⌛ Süresi doldu, hiçbir pozisyon kapatılmadı.")
            return
        engine = self._engine
        if engine is None:
            await self._answer(event, "Sistem henüz hazır değil", alert=True)
            return
        await self._answer(event, "⏳ Kapatılıyor…")
        await self._edit(msg_id, "⏳ Tüm pozisyonlar kapatılıyor…")
        try:
            result = await engine.close_all()
        except Exception as exc:
            log.exception("close_all failed")
            result = f"🚨 Hepsini kapatma sırasında hata: <code>{esc(exc)}</code>"
        if not await self._edit(msg_id, result or "✔️ Tamamlandı."):
            await self._send(result or "✔️ Tamamlandı.")

    async def _answer(self, event: Any, text: str, *, alert: bool = False) -> None:
        try:
            await event.answer(text, alert=alert)
        except Exception as exc:
            log.debug("Callback answer failed: %s", exc)

    # ---- sending ------------------------------------------------------------
    async def _send(
        self,
        text: str,
        *,
        buttons: Any = None,
        reply_to: int | None = None,
        backlog: bool = False,
    ) -> int | None:
        """Send HTML to the owner. Never raises; returns the (last) message id or None."""
        if self._client is None:
            log.warning("Notifier not started; dropping message: %.200s", text)
            return None
        if not text or not text.strip():
            return None
        try:
            chunks = split_html(text)
        except Exception:
            log.exception("Could not parse outgoing HTML; sending as escaped text")
            chunks = split_html(esc(text))
        msg_id: int | None = None
        for i, chunk in enumerate(chunks):
            last = i == len(chunks) - 1
            kwargs: dict[str, Any] = {
                "buttons": buttons if last else None,
                "link_preview": False,
                "reply_to": reply_to if i == 0 else None,
            }
            try:
                if isinstance(chunk, str):
                    msg = await self._client.send_message(self._owner, chunk, parse_mode="html", **kwargs)
                else:
                    msg = await self._client.send_message(self._owner, chunk[0], formatting_entities=chunk[1], **kwargs)
            except Exception as exc:
                self._log_send_failure(exc)
                if backlog and self._is_unreachable(exc):
                    self._backlog.append(text)
                return None
            msg_id = getattr(msg, "id", None)
            self._unreachable_logged = False
        return msg_id

    async def _edit(self, msg_id: int, text: str) -> bool:
        """Replace a bot message (buttons removed). False if it could not be edited."""
        if self._client is None:
            return False
        try:
            chunk = split_html(text, limit=4096)[-1]   # too long: keep the tail (it holds the outcome)
            if isinstance(chunk, str):
                await self._client.edit_message(
                    self._owner, msg_id, chunk, parse_mode="html", buttons=None, link_preview=False
                )
            else:
                await self._client.edit_message(
                    self._owner, msg_id, chunk[0], formatting_entities=chunk[1], buttons=None, link_preview=False
                )
            return True
        except errors.MessageNotModifiedError:
            return True
        except Exception as exc:
            log.warning("Could not edit bot message %s: %s", msg_id, exc)
            return False

    async def _flush_backlog(self) -> None:
        if not self._backlog:
            return
        pending = list(self._backlog)
        self._backlog.clear()
        await self._send(f"📬 Ulaşamadığım {len(pending)} bildirim:", backlog=False)
        for text in pending:
            await self._send(text, backlog=True)

    @staticmethod
    def _is_unreachable(exc: Exception) -> bool:
        return isinstance(exc, _OWNER_UNREACHABLE) or (
            isinstance(exc, ValueError) and "input entity" in str(exc)
        )

    def _log_send_failure(self, exc: Exception) -> None:
        if self._is_unreachable(exc):
            if not self._unreachable_logged:
                self._unreachable_logged = True
                log.error(
                    "Cannot message the owner (user id %s): %s. Open the bot in Telegram and press Start "
                    "(and do not block it); check notifier.owner_user_id.",
                    self._owner, exc,
                )
        else:
            log.error("Sending to the owner failed: %s", exc)

    @staticmethod
    def _remember(cache: OrderedDict[Any, Any], key: Any, value: Any) -> None:
        cache[key] = value
        cache.move_to_end(key)
        while len(cache) > _CACHE_SIZE:
            cache.popitem(last=False)
