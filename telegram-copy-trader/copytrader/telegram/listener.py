"""Telegram user-account listener (Telethon userbot).

READ-ONLY by design: it only connects, resolves chats, reads messages and
downloads their images. It never sends, forwards, reacts to or marks
anything as read in the source chats.
"""

from __future__ import annotations

import asyncio
import getpass
import logging
import mimetypes
import re
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Coroutine
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from telethon import TelegramClient, errors, events, utils
from telethon.sessions import StringSession
from telethon.tl import types

from .. import __version__
from ..config import TelegramConfig
from ..interfaces import Storage
from ..models import StoredMessage

log = logging.getLogger(__name__)

OnReady = Callable[[int, int | None], Awaitable[None]]       # (chat_id, sender_id)
OnEdit = Callable[[StoredMessage], Awaitable[None]]

LOGIN_HINT_TR = (
    "Telegram kullanıcı oturumu yok veya geçersiz. Önce terminalde "
    "'python -m copytrader login' komutunu çalıştırıp giriş yapın "
    "(konteyner için çıkan oturum metnini TG_SESSION_STRING olarak ayarlayın)."
)


class TelegramAuthError(RuntimeError):
    """The user session is missing, revoked or banned: the owner must log in again."""


_AUTH_ERRORS: tuple[type[Exception], ...] = (
    errors.AuthKeyUnregisteredError,
    errors.SessionRevokedError,
    errors.AuthKeyDuplicatedError,
    errors.UserDeactivatedBanError,
    errors.UserDeactivatedError,
)

CATCHUP_FETCH_LIMIT = 200            # newest missed messages fetched per chat on (re)start
MEDIA_DOWNLOAD_TIMEOUT_SEC = 90.0
MAX_IMAGE_BYTES = 20 * 1024 * 1024
STOP_GRACE_SEC = 30.0                # let running callbacks (maybe placing orders) finish
RECONNECT_BASE_DELAY_SEC = 1.0
RECONNECT_MAX_DELAY_SEC = 300.0
_SEEN_LIMIT = 5000

DebounceKey = tuple[int, int | None]


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #
def should_process_catchup(msg_date: datetime, now: datetime, max_age_sec: float) -> bool:
    """Catch-up rule: a missed message is executed only if it is younger than ``max_age_sec``.

    Older ones are stored as context only (consumed=True)."""
    return (_as_utc(now) - _as_utc(msg_date)).total_seconds() <= max_age_sec


def normalize_chat_ref(ref: int | str) -> int | str:
    """Config chat reference -> something ``get_entity`` understands.

    ``-100123`` / ``"-100123"`` -> int, ``t.me/c/123/45`` -> ``-100123``,
    ``https://t.me/name/45`` -> ``"@name"``; invite links and @usernames pass through."""
    if isinstance(ref, int):
        return ref
    s = ref.strip()
    if re.fullmatch(r"-?\d+", s):
        return int(s)
    m = re.fullmatch(r"(?:https?://)?(?:www\.)?t(?:elegram)?\.(?:me|dog)/c/(\d+)(?:/\d+)*/?", s)
    if m:
        return int(f"-100{m.group(1)}")
    m = re.fullmatch(r"(?:https?://)?(?:www\.)?t(?:elegram)?\.(?:me|dog)/(?:s/)?([A-Za-z][A-Za-z0-9_]{3,})(?:/\d+)?/?", s)
    if m and m.group(1).lower() not in ("joinchat", "addlist"):
        return "@" + m.group(1)
    return s


def _as_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _display_name(entity: Any) -> str | None:
    if entity is None:
        return None
    first = getattr(entity, "first_name", None) or ""
    last = getattr(entity, "last_name", None) or ""
    name = f"{first} {last}".strip() or getattr(entity, "title", None) or getattr(entity, "username", None)
    return name or None


def _reply_to_id(message: Any) -> int | None:
    header = getattr(message, "reply_to", None)
    if header is None:
        return getattr(message, "reply_to_msg_id", None)
    if getattr(header, "reply_to_peer_id", None) is not None:
        return None                      # reply to a message in another chat
    if getattr(header, "forum_topic", False) and getattr(header, "reply_to_top_id", None) is None:
        return None                      # plain message in a forum topic, not a real reply
    return getattr(header, "reply_to_msg_id", None)


def _newer(current: Any, candidate: Any) -> Any:
    """The later of two versions of the same message (by edit_date; the candidate when unknown)."""
    if current is None:
        return candidate
    old, new = getattr(current, "edit_date", None), getattr(candidate, "edit_date", None)
    if isinstance(old, datetime) and isinstance(new, datetime) and _as_utc(new) < _as_utc(old):
        return current
    return candidate


def _is_sticker(doc: Any) -> bool:
    return any(isinstance(a, types.DocumentAttributeSticker) for a in getattr(doc, "attributes", None) or [])


def image_info(message: Any) -> tuple[str, str, int | None, int | None] | None:
    """(media_type, file extension, media id, size) for photos / image documents, else None."""
    photo = getattr(message, "photo", None)
    if photo is not None:
        return "photo", ".jpg", getattr(photo, "id", None), None
    doc = getattr(message, "document", None)
    if doc is not None:
        mime = (getattr(doc, "mime_type", None) or "").lower()
        if mime.startswith("image/") and not _is_sticker(doc):
            ext = mimetypes.guess_extension(mime) or ".img"
            return "image_document", ext, getattr(doc, "id", None), getattr(doc, "size", None)
    return None


def _session_file(session_path: str) -> str:
    path = Path(session_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def make_user_client(config: TelegramConfig, *, prefer_file: bool = False) -> TelegramClient:
    """Telethon client for the owner's user account (string session wins unless ``prefer_file``)."""
    if config.session_string and not prefer_file:
        try:
            session: Any = StringSession(config.session_string.strip())
        except Exception as exc:   # ValueError, binascii.Error, struct.error (truncated string)...
            raise TelegramAuthError(
                "TG_SESSION_STRING geçersiz (eksik veya bozuk kopyalanmış olabilir; tek satır ve "
                "boşluksuz olmalı). " + LOGIN_HINT_TR
            ) from exc
    else:
        session = _session_file(config.session_path)
    return TelegramClient(
        session,
        config.api_id,
        config.api_hash,
        device_model="copytrader",
        app_version=__version__,
    )


# --------------------------------------------------------------------------- #
# Listener
# --------------------------------------------------------------------------- #
class TelegramListener:
    def __init__(
        self,
        config: TelegramConfig,
        db: Storage,
        data_dir: str,
        on_ready: OnReady,
        on_edit: OnEdit,
    ) -> None:
        self._config = config
        self._db = db
        self._media_dir = Path(data_dir) / "media"
        self._on_ready = on_ready
        self._on_edit = on_edit
        self._client: TelegramClient | None = None
        self._self_id: int | None = None
        self._entities: dict[int, Any] = {}           # peer id -> entity
        self._allowed = set(config.allowed_sender_ids)
        self._timers: dict[DebounceKey, asyncio.Task[None]] = {}   # sleeping debounce timers
        self._inflight: dict[DebounceKey, int] = {}   # messages still being downloaded/saved
        self._tasks: set[asyncio.Task[Any]] = set()   # running handlers / callbacks
        self._seen: OrderedDict[tuple[int, int], None] = OrderedDict()
        # Messages being downloaded/stored right now -> newest edit that arrived meanwhile (or None).
        self._storing: dict[tuple[int, int], Any] = {}
        self._media_ids: OrderedDict[tuple[int, int], int | None] = OrderedDict()
        self._last_ids: dict[int, int] = {}
        self._stopping = False

    @property
    def chat_ids(self) -> set[int]:
        return set(self._entities)

    # ---- lifecycle -------------------------------------------------------
    async def start(self) -> None:
        """Connect with the saved user session (never prompts; raises if not logged in), resolve source chats."""
        self._stopping = False
        if self._client is None:
            self._client = make_user_client(self._config)
        client = self._client
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            raise TelegramAuthError(LOGIN_HINT_TR)
        me = await client.get_me()
        if getattr(me, "bot", False):
            await client.disconnect()
            raise RuntimeError("Dinleyici oturumu bir bot hesabına ait; kendi kullanıcı hesabınızla giriş yapın.")
        self._self_id = getattr(me, "id", None)

        self._entities = await self._resolve_chats()
        ids = list(self._entities)
        # Handlers first, then catch-up: nothing is missed; duplicates are filtered by _seen.
        client.remove_event_handler(self._on_new_message_event)
        client.remove_event_handler(self._on_edited_event)
        client.add_event_handler(self._on_new_message_event, events.NewMessage(chats=ids))
        client.add_event_handler(self._on_edited_event, events.MessageEdited(chats=ids))
        log.info("Listening to %d source chat(s): %s", len(ids), ids)
        await self._catch_up()
        await self._resume_unprocessed()

    async def run_until_disconnected(self) -> None:
        """Run until stop(); reconnects with backoff if Telethon gives up."""
        if self._client is None:
            raise RuntimeError("TelegramListener.start() must be called first")
        delay = RECONNECT_BASE_DELAY_SEC
        while not self._stopping:
            started = time.monotonic()
            try:
                await self._client.run_until_disconnected()
            except asyncio.CancelledError:
                raise
            except _AUTH_ERRORS as exc:
                raise TelegramAuthError(LOGIN_HINT_TR) from exc
            except Exception:
                log.exception("Telegram user connection failed")
            if self._stopping:
                break
            if time.monotonic() - started > 120:
                delay = RECONNECT_BASE_DELAY_SEC
            log.warning("Telegram user connection lost; reconnecting in %.0fs", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY_SEC)
            if self._stopping:
                break
            try:
                await self._client.connect()
                if not await self._client.is_user_authorized():
                    raise TelegramAuthError(LOGIN_HINT_TR)
                log.info("Telegram user connection restored")
                await self._catch_up()
            except TelegramAuthError:
                raise
            except _AUTH_ERRORS as exc:
                raise TelegramAuthError(LOGIN_HINT_TR) from exc
            except Exception as exc:
                log.warning("Reconnect failed: %s", exc)

    async def stop(self) -> None:
        self._stopping = True
        for task in list(self._timers.values()):
            task.cancel()
        self._timers.clear()
        running = [t for t in self._tasks if not t.done()]
        if running:
            _, pending = await asyncio.wait(running, timeout=STOP_GRACE_SEC)
            for task in pending:
                task.cancel()
        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception:
                log.exception("Error while disconnecting the user client")

    # ---- chat resolution ---------------------------------------------------
    async def _resolve_chats(self) -> dict[int, Any]:
        assert self._client is not None
        resolved: dict[int, Any] = {}
        dialogs_loaded = False
        for ref in self._config.source_chats:
            target = normalize_chat_ref(ref)
            try:
                try:
                    entity = await self._client.get_entity(target)
                except (ValueError, TypeError):
                    if dialogs_loaded:
                        raise
                    # Fresh StringSession has no entity cache: load dialogs once, then retry.
                    await self._client.get_dialogs()
                    dialogs_loaded = True
                    entity = await self._client.get_entity(target)
            except Exception as exc:
                raise RuntimeError(
                    f"Kaynak sohbet bulunamadı: {ref!r}. Hesabınızın o grupta üye olduğundan ve "
                    "ID/kullanıcı adının doğru olduğundan emin olun (sohbet listesini CLI ile görebilirsiniz)."
                ) from exc
            if isinstance(entity, list):
                raise RuntimeError(f"Kaynak sohbet tek bir sohbet olmalı: {ref!r}")
            peer_id = utils.get_peer_id(entity)
            resolved[peer_id] = entity
            log.info("Source chat %r -> %s (%s)", ref, peer_id, _display_name(entity))
        if not resolved:
            raise RuntimeError("telegram.source_chats boş; dinlenecek grubu ayarlayın.")
        return resolved

    # ---- Telethon handlers (thin; spawn tracked tasks) ----------------------
    async def _on_new_message_event(self, event: Any) -> None:
        self._spawn(self.handle_message(event.message))

    async def _on_edited_event(self, event: Any) -> None:
        self._spawn(self.handle_edit(event.message))

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        # Own tasks so Telethon's disconnect does not cancel work mid-way.
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)
        return task

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            log.error("Listener task failed", exc_info=task.exception())

    # ---- message processing (testable) --------------------------------------
    def _accepts(self, message: Any) -> bool:
        if isinstance(message, types.MessageService):
            return False
        chat_id = getattr(message, "chat_id", None)
        if chat_id is None or (self._entities and chat_id not in self._entities):
            return False
        if getattr(message, "out", False) and self._self_id not in self._allowed:
            return False                 # the owner's own messages are never signals
        sender_id = getattr(message, "sender_id", None)
        if self._allowed and sender_id not in self._allowed:
            log.debug("Ignoring message %s from non-allowed sender %s", message.id, sender_id)
            return False
        return True

    async def handle_message(self, message: Any, *, consumed: bool = False) -> None:
        """New message (live or catch-up): store it, then debounce ``on_ready``."""
        chat_id = getattr(message, "chat_id", None)
        if chat_id is None:
            return
        key = (chat_id, message.id)
        if key in self._seen:
            return
        self._remember(self._seen, key, None)
        self._note_last_id(chat_id, message.id)
        if not self._accepts(message):
            await self._persist_last_id(chat_id)
            return
        has_content = bool(getattr(message, "message", None)) or image_info(message) is not None
        trigger = has_content and not consumed
        dkey: DebounceKey = (chat_id, getattr(message, "sender_id", None))
        if trigger:
            self._debounce_hold(dkey)
        self._storing[key] = None
        late_edit: Any = None
        try:
            stored = await self._build_stored(message, consumed=consumed)
            # Edited while the image was downloading (nothing stored yet, so nothing was built from
            # it): store the newest version instead; the debounce is still held, so the engine only
            # ever sees the edited text.
            while (edit := self._storing.get(key)) is not None:
                self._storing[key] = None
                media_changed = self._media_changed(stored, image_info(edit))
                if not media_changed and (getattr(edit, "message", None) or "") == stored.text:
                    continue                                # e.g. only reactions changed
                stored = await self._build_stored(
                    edit,
                    consumed=consumed,
                    edited=True,
                    download=media_changed,
                    keep_media=(stored.media_path, stored.media_type),
                )
            await self._db.save_message(stored)
        except Exception:
            log.exception("Could not store message %s/%s", chat_id, message.id)
        finally:
            late_edit = self._storing.pop(key, None)
            if trigger:
                self._debounce_release(dkey)
        await self._persist_last_id(chat_id)
        if late_edit is not None:
            # Arrived while the row was being written: apply it as a normal edit now.
            await self.handle_edit(late_edit)

    async def handle_edit(self, message: Any) -> None:
        """Edited message: store the new version, then ``on_edit``."""
        if not self._accepts(message):
            return
        chat_id, msg_id = message.chat_id, message.id
        key = (chat_id, msg_id)
        existing = None if key in self._storing else await self._db.get_message(chat_id, msg_id)
        if existing is None and key in self._storing:
            # The original is still being downloaded/stored: handle_message stores this version
            # (or, if its row is already written, applies it as a normal edit right after).
            self._storing[key] = _newer(self._storing[key], message)
            log.debug("Edit of %s/%s arrived while storing; queued", chat_id, msg_id)
            return
        if existing is None:
            date = _as_utc(message.date)
            if should_process_catchup(date, datetime.now(timezone.utc), self._config.catchup_max_age_sec):
                await self.handle_message(message)          # never saw it: treat as new
            else:
                stored = await self._build_stored(message, consumed=True, edited=True)
                await self._db.save_message(stored)         # old message: context only
            return

        text = getattr(message, "message", None) or ""
        info = image_info(message)
        media_changed = self._media_changed(existing, info)
        if not media_changed and text == existing.text:
            log.debug("Edit of %s/%s changed nothing relevant (reactions?)", chat_id, msg_id)
            return
        stored = await self._build_stored(
            message,
            consumed=existing.consumed,
            edited=True,
            download=media_changed,
            keep_media=(existing.media_path, existing.media_type),
        )
        await self._db.save_message(stored)
        try:
            await self._on_edit(stored)
        except Exception:
            log.exception("on_edit failed for %s/%s", chat_id, msg_id)

    def _media_changed(self, existing: StoredMessage, info: tuple[str, str, int | None, int | None] | None) -> bool:
        if info is None:
            return existing.media_type is not None
        known = self._media_ids.get((existing.chat_id, existing.message_id))
        if not existing.media_path or not Path(existing.media_path).exists():
            return True
        return known is None or known != info[2]

    async def _build_stored(
        self,
        message: Any,
        *,
        consumed: bool = False,
        edited: bool = False,
        download: bool = True,
        keep_media: tuple[str | None, str | None] = (None, None),
    ) -> StoredMessage:
        chat_id, msg_id = message.chat_id, message.id
        media_path, media_type = keep_media
        info = image_info(message)
        if info is None:
            media_path, media_type = None, None
        elif download:
            media_type = info[0]
            media_path = await self._download(message, info)
            self._remember(self._media_ids, (chat_id, msg_id), info[2])
        return StoredMessage(
            chat_id=chat_id,
            message_id=msg_id,
            sender_id=getattr(message, "sender_id", None),
            sender_name=await self._sender_name(message),
            date=_as_utc(message.date),
            text=getattr(message, "message", None) or "",
            media_path=media_path,
            media_type=media_type,
            reply_to_message_id=_reply_to_id(message),
            grouped_id=getattr(message, "grouped_id", None),
            edited=edited,
            consumed=consumed,
        )

    async def _sender_name(self, message: Any) -> str | None:
        sender = getattr(message, "sender", None)
        if sender is None and hasattr(message, "get_sender"):
            try:
                sender = await message.get_sender()
            except Exception as exc:
                log.debug("get_sender failed: %s", exc)
        return _display_name(sender)

    async def _download(self, message: Any, info: tuple[str, str, int | None, int | None]) -> str | None:
        _, ext, _, size = info
        if size is not None and size > MAX_IMAGE_BYTES:
            log.warning("Image in %s/%s too large (%d bytes); not downloaded", message.chat_id, message.id, size)
            return None
        self._media_dir.mkdir(parents=True, exist_ok=True)
        target = self._media_dir / f"{abs(message.chat_id)}_{message.id}{ext}"
        try:
            result = await asyncio.wait_for(message.download_media(file=str(target)), MEDIA_DOWNLOAD_TIMEOUT_SEC)
        except Exception as exc:
            log.error("Could not download image of %s/%s: %s", message.chat_id, message.id, exc)
            return None
        if not result:
            log.error("Image of %s/%s could not be downloaded (empty result)", message.chat_id, message.id)
            return None
        return str(result)

    # ---- debounce -----------------------------------------------------------
    def _debounce_hold(self, key: DebounceKey) -> None:
        """A message for ``key`` arrived: stop the timer until it is stored."""
        timer = self._timers.pop(key, None)
        if timer is not None:
            timer.cancel()
        self._inflight[key] = self._inflight.get(key, 0) + 1

    def _debounce_release(self, key: DebounceKey) -> None:
        left = self._inflight.get(key, 1) - 1
        if left > 0:
            self._inflight[key] = left
            return
        self._inflight.pop(key, None)
        if self._stopping:
            return
        old = self._timers.pop(key, None)
        if old is not None:
            old.cancel()
        self._timers[key] = asyncio.ensure_future(self._fire_later(key))

    async def _fire_later(self, key: DebounceKey) -> None:
        await asyncio.sleep(self._config.debounce_sec)
        me = asyncio.current_task()
        if self._timers.get(key) is not me:
            return
        # Detach first: messages arriving while on_ready runs start a new timer
        # instead of cancelling a call that may be placing orders.
        del self._timers[key]
        assert me is not None
        self._tasks.add(me)
        try:
            await self._on_ready(key[0], key[1])
        except Exception:
            log.exception("on_ready failed for chat %s sender %s", key[0], key[1])
        finally:
            self._tasks.discard(me)

    # ---- catch-up / last id --------------------------------------------------
    def _note_last_id(self, chat_id: int, msg_id: int) -> None:
        if msg_id > self._last_ids.get(chat_id, 0):
            self._last_ids[chat_id] = msg_id

    async def _persist_last_id(self, chat_id: int) -> None:
        value = self._last_ids.get(chat_id)
        if value is None:
            return
        try:
            await self._db.set_state(f"last_msg_id:{chat_id}", value)
        except Exception:
            log.exception("Could not persist last message id for %s", chat_id)

    async def _catch_up(self) -> None:
        for chat_id, entity in self._entities.items():
            try:
                await self._catch_up_chat(chat_id, entity)
            except Exception:
                log.exception("Catch-up failed for chat %s", chat_id)

    async def _catch_up_chat(self, chat_id: int, entity: Any) -> None:
        assert self._client is not None
        stored = await self._db.get_state(f"last_msg_id:{chat_id}")
        if stored is None:
            # First run: only remember where we are, never execute history.
            latest = await self._client.get_messages(entity, limit=1)
            if latest:
                self._note_last_id(chat_id, latest[0].id)
                await self._persist_last_id(chat_id)
            return
        last = int(stored)
        self._note_last_id(chat_id, last)
        missed = [m async for m in self._client.iter_messages(entity, min_id=last, limit=CATCHUP_FETCH_LIMIT)]
        if not missed:
            return
        if len(missed) >= CATCHUP_FETCH_LIMIT:
            log.warning("Catch-up for %s hit the %d message limit; older missed messages skipped", chat_id, CATCHUP_FETCH_LIMIT)
        missed.sort(key=lambda m: m.id)
        now = datetime.now(timezone.utc)
        fresh = 0
        for msg in missed:
            young = should_process_catchup(msg.date, now, self._config.catchup_max_age_sec)
            fresh += young
            await self.handle_message(msg, consumed=not young)
        log.info("Catch-up %s: %d missed message(s), %d recent enough to process", chat_id, len(missed), fresh)

    async def _resume_unprocessed(self) -> None:
        """Re-trigger recent stored messages whose debounce never fired (crash / restart)."""
        since = datetime.now(timezone.utc) - timedelta(seconds=self._config.catchup_max_age_sec)
        for chat_id in self._entities:
            try:
                msgs = await self._db.recent_messages(chat_id, since, only_unconsumed=True)
            except Exception:
                log.exception("Could not read unprocessed messages of %s", chat_id)
                continue
            senders = {m.sender_id for m in msgs if not self._allowed or m.sender_id in self._allowed}
            for sender_id in senders:
                self._debounce_hold((chat_id, sender_id))
                self._debounce_release((chat_id, sender_id))
            if senders:
                log.info("Resuming %d unprocessed recent message(s) in %s", len(msgs), chat_id)

    @staticmethod
    def _remember(cache: OrderedDict[Any, Any], key: Any, value: Any) -> None:
        cache[key] = value
        cache.move_to_end(key)
        while len(cache) > _SEEN_LIMIT:
            cache.popitem(last=False)


# --------------------------------------------------------------------------- #
# CLI helpers
# --------------------------------------------------------------------------- #
async def _ask(prompt: str, secret: bool = False) -> str:
    reader = getpass.getpass if secret else input
    return (await asyncio.to_thread(reader, prompt)).strip()


async def interactive_login(config: TelegramConfig) -> str:
    """First-time login in a terminal (phone, code, 2FA). Saves the session file and returns a StringSession string."""

    async def phone() -> str:
        while True:
            value = await _ask("Telefon numarası (ülke koduyla, ör. +905551112233): ")
            if ":" not in value:     # Telethon would treat "id:hash" as a bot token
                return value
            print("Bu bir bot token'ı gibi görünüyor; kendi telefon numaranızı girin.")

    client = make_user_client(config, prefer_file=True)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            number = await phone()
            # Ask Telegram to send the login code by SMS (the in-app message may never arrive
            # on a new device), then sign in with the code the owner types.
            sent = await client.send_code_request(number, force_sms=True)
            print("Telegram kodu SMS ile gönderdi. Telefonunuza gelen 5 haneli kodu girin.")
            code = (await _ask("Gelen giriş kodu: ")).replace(" ", "")
            try:
                await client.sign_in(number, code, phone_code_hash=sent.phone_code_hash)
            except errors.SessionPasswordNeededError:
                password = await _ask("İki adımlı doğrulama şifresi: ", secret=True)
                await client.sign_in(password=password)
        me = await client.get_me()
        if getattr(me, "bot", False):
            raise RuntimeError("Bot hesabıyla giriş yapılamaz; kendi kullanıcı hesabınızı kullanın.")
        log.info("Logged in as %s (id %s)", _display_name(me), getattr(me, "id", None))
        return StringSession.save(client.session)
    finally:
        await client.disconnect()


def _dialog_kind(entity: Any) -> str:
    if isinstance(entity, types.User):
        return "bot" if entity.bot else "user"
    if isinstance(entity, (types.Chat, types.ChatForbidden)):
        return "group"
    if isinstance(entity, types.Channel):
        return "supergroup" if entity.megagroup or getattr(entity, "gigagroup", False) else "channel"
    if isinstance(entity, types.ChannelForbidden):
        return "supergroup" if entity.megagroup else "channel"
    return "group"


async def list_dialogs(config: TelegramConfig, limit: int = 200) -> list[tuple[int, str, str]]:
    """(chat_id, title, kind) for the account's chats - helps the owner find the source group id."""
    client = make_user_client(config)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise TelegramAuthError(LOGIN_HINT_TR)
        result: list[tuple[int, str, str]] = []
        async for dialog in client.iter_dialogs(limit=limit):
            result.append((dialog.id, dialog.name or "", _dialog_kind(dialog.entity)))
        return result
    finally:
        await client.disconnect()
