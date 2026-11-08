"""Telegram user-account listener (Telethon userbot).

READ-ONLY by design: it only connects, resolves chats, reads messages and
downloads their images. It never sends, forwards, reacts to or marks
anything as read in the source chats.

Forum groups (supergroups with topics)
--------------------------------------
Every message gets the id of the forum topic it was posted in (``topic_id_of``):

* top-level message in a topic: ``reply_to.forum_topic`` and ``reply_to_msg_id`` = topic id
  (NOT a reply to a real message, so ``reply_to_message_id`` is None);
* reply inside a topic: ``reply_to_top_id`` = topic id, ``reply_to_msg_id`` = the replied message;
* the topic's root service message (``MessageActionTopicCreate``): its own id;
* the "General" topic has no forum header: topic id None (its real id 1 is never stored).

Topic titles come from ``messages.getForumTopics`` / ``messages.getForumTopicsByID`` (read-only)
and are cached per chat; the cache is refreshed on a miss (a new monthly topic) and by topic
create/edit service messages. When ``telegram.topics`` is set and the chat is a forum, only
messages whose topic title matches a route (``config.match_topic``) are stored and passed on.
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
from typing import Any, NamedTuple

from telethon import TelegramClient, errors, events, functions, utils
from telethon.sessions import StringSession
from telethon.tl import types

from .. import __version__
from ..config import TelegramConfig, match_topic
from ..interfaces import Storage
from ..models import StoredMessage

log = logging.getLogger(__name__)

OnReady = Callable[[int, int | None, int | None], Awaitable[None]]   # (chat_id, sender_id, topic_id)
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

GENERAL_TOPIC_ID = 1                 # Telegram's id of a forum's "General" topic (stored as topic_id None)
GENERAL_TOPIC_TITLE = "General"      # used when Telegram does not return General's title
TOPIC_LIST_LIMIT = 100               # topics listed per forum at start (the rest are fetched by id on demand)
TOPIC_REQUEST_TIMEOUT_SEC = 20.0
TOPIC_RETRY_DELAYS_SEC = (2.0, 5.0, 15.0, 30.0, 60.0)   # after failed title lookups (last one repeats)
TOPIC_GIVE_UP_SEC = 600.0            # a message waits at most this long for its topic's title
TOPIC_UNKNOWN_TTL_SEC = 60.0         # topic Telegram did not return: treated as unknown this long, then asked again
TOPIC_LATE_SEC = 10.0                # title found after this long: the catch-up age rule applies to the message

_TOPIC_ACTIONS = (types.MessageActionTopicCreate, types.MessageActionTopicEdit)

DebounceKey = tuple[int, int | None, int | None]   # (chat_id, topic_id, sender_id)


class TopicLookupError(RuntimeError):
    """The title of a forum topic could not be fetched in time (or the listener is stopping)."""


class TopicRef(NamedTuple):
    follow: bool                     # store / pass on this message
    topic_id: int | None             # None = General topic / not a forum
    title: str | None


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


def _in_forum_topic(message: Any) -> bool:
    """The message carries a forum-topic reply header (the chat is a forum)."""
    return bool(getattr(getattr(message, "reply_to", None), "forum_topic", False))


def topic_id_of(message: Any) -> int | None:
    """Forum topic id of a message (Telethon 1.45 / layer 229 ``MessageReplyHeader``).

    ``forum_topic`` is set on every message of a topic except the General one:
    a reply has ``reply_to_top_id`` = topic id; a top-level message has ``reply_to_msg_id`` = topic id.
    The topic's root service message (``MessageActionTopicCreate``) has the topic's id as its own id.
    None = General topic (id 1 is normalized to None), a non-forum chat, or a reply to another chat
    without thread id (topic unknown)."""
    if isinstance(getattr(message, "action", None), types.MessageActionTopicCreate):
        topic: int | None = getattr(message, "id", None)
    else:
        header = getattr(message, "reply_to", None)
        if header is None or not getattr(header, "forum_topic", False):
            return None
        topic = getattr(header, "reply_to_top_id", None)
        if not topic:
            if getattr(header, "reply_to_peer_id", None) is not None:
                return None                  # reply_to_msg_id belongs to another chat
            topic = getattr(header, "reply_to_msg_id", None)
    if not topic or topic == GENERAL_TOPIC_ID:
        return None
    return int(topic)


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
        # Abandoned at shutdown before being stored: last_msg_id stays below them so catch-up refetches them.
        self._held_back: set[tuple[int, int]] = set()
        self._stopping = False
        self._stop_event = asyncio.Event()
        # ---- forum topics ----
        self._forums: set[int] = set()                                # chats known to be forums
        self._topic_titles: dict[tuple[int, int], str | None] = {}    # (chat, topic) -> title; None = deleted/unknown
        self._topic_unknown_until: dict[tuple[int, int], float] = {}  # expiry of a None ("not returned") entry
        self._topic_retry_at: dict[tuple[int, int], float] = {}       # after a failed lookup: not before (monotonic)
        self._topic_failures: dict[tuple[int, int], int] = {}
        self._topic_locks: dict[int, asyncio.Lock] = {}
        self._warned: set[str] = set()

    @property
    def chat_ids(self) -> set[int]:
        return set(self._entities)

    def _filtering(self, chat_id: int) -> bool:
        """Only configured topics are followed in this chat."""
        return bool(self._config.topics) and chat_id in self._forums

    # ---- lifecycle -------------------------------------------------------
    async def start(self) -> None:
        """Connect with the saved user session (never prompts; raises if not logged in), resolve source chats."""
        self._stopping = False
        self._stop_event.clear()
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
        self._forums = {cid for cid, entity in self._entities.items() if getattr(entity, "forum", False)}
        ids = list(self._entities)
        # Handlers first, then catch-up: nothing is missed; duplicates are filtered by _seen.
        client.remove_event_handler(self._on_new_message_event)
        client.remove_event_handler(self._on_edited_event)
        client.remove_event_handler(self._on_raw_update)
        client.add_event_handler(self._on_new_message_event, events.NewMessage(chats=ids))
        client.add_event_handler(self._on_edited_event, events.MessageEdited(chats=ids))
        # NewMessage drops service messages; topic create/edit ones keep the title cache fresh.
        client.add_event_handler(self._on_raw_update, events.Raw(types.UpdateNewChannelMessage))
        log.info("Listening to %d source chat(s): %s", len(ids), ids)
        await self._load_topics()
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
        self._stop_event.set()           # wakes messages waiting for a topic title
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

    async def _on_raw_update(self, update: Any) -> None:
        message = getattr(update, "message", None)
        if (
            isinstance(message, types.MessageService)
            and isinstance(getattr(message, "action", None), _TOPIC_ACTIONS)
            and getattr(message, "chat_id", None) in self._entities
        ):
            self._spawn(self.handle_message(message))

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
        """New message (live or catch-up): store it, then debounce ``on_ready``.

        Forum + ``telegram.topics``: messages of topics no route matches are neither stored nor
        downloaded. Topic create/edit service messages only update the title cache."""
        chat_id = getattr(message, "chat_id", None)
        if chat_id is None:
            return
        key = (chat_id, message.id)
        if key in self._seen:
            return
        self._remember(self._seen, key, None)
        self._held_back.discard(key)
        self._note_last_id(chat_id, message.id)
        if isinstance(message, types.MessageService):
            if not self._entities or chat_id in self._entities:
                self._apply_topic_action(message)
            await self._persist_last_id(chat_id)
            return
        if not self._accepts(message):
            await self._persist_last_id(chat_id)
            return
        self._storing[key] = None        # from here on, edits of this message are queued (see handle_edit)
        started = time.monotonic()
        try:
            topic = await self._topic_of(message)
        except TopicLookupError as exc:
            self._storing.pop(key, None)
            self._seen.pop(key, None)    # a later delivery (catch-up, edit) may try again
            if self._stopping:
                self._held_back.add(key)
                log.warning("Stopping: message %s/%s not stored (its forum topic is unknown yet)", chat_id, message.id)
                return
            log.error("Message %s/%s NOT stored: title of its forum topic unknown (%s)", chat_id, message.id, exc)
            await self._persist_last_id(chat_id)
            return
        except BaseException:
            self._storing.pop(key, None)
            self._seen.pop(key, None)
            self._held_back.add(key)
            raise
        if not topic.follow:
            self._storing.pop(key, None)
            await self._persist_last_id(chat_id)
            return
        waited = time.monotonic() - started
        if waited > TOPIC_LATE_SEC and not consumed and not should_process_catchup(
            message.date, datetime.now(timezone.utc), self._config.catchup_max_age_sec
        ):
            log.warning("Message %s/%s waited %.0fs for its forum topic; stored as context only", chat_id, message.id, waited)
            consumed = True
        has_content = bool(getattr(message, "message", None)) or image_info(message) is not None
        trigger = has_content and not consumed
        dkey: DebounceKey = (chat_id, topic.topic_id, getattr(message, "sender_id", None))
        if trigger:
            self._debounce_hold(dkey)
        late_edit: Any = None
        try:
            stored = await self._build_stored(message, consumed=consumed, topic=topic)
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
                    topic=topic,
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
        """Edited message: store the new version, then ``on_edit`` (only for followed topics)."""
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
                return
            topic = await self._edit_topic(None, message)
            if topic.follow:
                stored = await self._build_stored(message, consumed=True, edited=True, topic=topic)
                await self._db.save_message(stored)         # old message: context only
            return

        topic = await self._edit_topic(existing, message)
        if not topic.follow:
            log.debug("Edit of %s/%s ignored: topic %r is not followed", chat_id, msg_id, topic.title)
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
            topic=topic,
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
        topic: TopicRef | None = None,
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
            topic_id=topic.topic_id if topic is not None else None,
            topic_title=topic.title if topic is not None else None,
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

    # ---- forum topics ----------------------------------------------------------
    async def _topic_of(self, message: Any) -> TopicRef:
        """Topic of a new message and whether it is followed.

        Raises TopicLookupError when a filtered forum's topic title cannot be fetched in time."""
        chat_id = message.chat_id
        topic_id = topic_id_of(message)
        if _in_forum_topic(message):
            self._mark_forum(chat_id)
        if chat_id not in self._forums:
            if self._config.topics:
                self._warn_not_forum(chat_id)
            return TopicRef(True, None, None)
        filtering = bool(self._config.topics)
        # Without routes the title is informational only: one attempt, never wait for it.
        title = await self._topic_title(chat_id, topic_id or GENERAL_TOPIC_ID, patient=filtering)
        if title is None and topic_id is None:
            title = GENERAL_TOPIC_TITLE
        if filtering and match_topic(self._config.topics, title) is None:
            log.debug("Ignoring %s/%s: topic %s %r is not followed", chat_id, message.id, topic_id, title)
            return TopicRef(False, topic_id, title)
        return TopicRef(True, topic_id, title)

    async def _edit_topic(self, existing: StoredMessage | None, message: Any) -> TopicRef:
        """Topic of an edited message. A stored row keeps its topic (it was followed when stored);
        rows stored before topic support (no title) in a filtered forum are checked like new ones."""
        if existing is not None and not (self._filtering(existing.chat_id) and existing.topic_title is None):
            hit, cached = self._cached_title((existing.chat_id, existing.topic_id or GENERAL_TOPIC_ID))
            return TopicRef(True, existing.topic_id, cached if hit and cached else existing.topic_title)
        try:
            return await self._topic_of(message)
        except TopicLookupError as exc:
            log.error("Edit of %s/%s ignored: title of its forum topic unknown (%s)", message.chat_id, message.id, exc)
            return TopicRef(False, topic_id_of(message), None)

    async def _topic_title(self, chat_id: int, topic_id: int, *, patient: bool) -> str | None:
        """Cached title of a forum topic, fetched on a cache miss. None = deleted / not returned.

        API errors are remembered per topic (backoff), so concurrent messages do not hammer
        Telegram. ``patient``: wait and retry until TOPIC_GIVE_UP_SEC, then raise TopicLookupError
        (also when stopping); otherwise return None at once while the topic is in backoff."""
        key = (chat_id, topic_id)
        deadline = time.monotonic() + TOPIC_GIVE_UP_SEC
        while True:
            hit, title = self._cached_title(key)
            if hit:
                return title
            now = time.monotonic()
            wait = self._topic_retry_at.get(key, 0.0) - now
            if wait > 0:
                if not patient:
                    return None
                if self._stopping:
                    raise TopicLookupError("listener is stopping")
                if now >= deadline:
                    raise TopicLookupError(f"Telegram did not answer for {TOPIC_GIVE_UP_SEC:.0f}s")
                await self._sleep_unless_stopping(min(wait, deadline - now))
                continue
            async with self._topic_lock(chat_id):
                if self._cached_title(key)[0] or self._topic_retry_at.get(key, 0.0) > time.monotonic():
                    continue                 # fetched (or failed) by another message meanwhile
                try:
                    await self._fetch_topics(chat_id, [topic_id])
                except Exception as exc:
                    self._lookup_failed(key, exc)
                    continue
                if not self._cached_title(key)[0]:
                    if topic_id == GENERAL_TOPIC_ID:
                        self._set_title(chat_id, topic_id, GENERAL_TOPIC_TITLE)
                    else:
                        log.warning("Telegram did not return forum topic %s of %s; treated as unknown for %.0fs",
                                    topic_id, chat_id, TOPIC_UNKNOWN_TTL_SEC)
                        self._set_title(chat_id, topic_id, None, ttl=TOPIC_UNKNOWN_TTL_SEC)

    async def _fetch_topics(self, chat_id: int, ids: list[int] | None) -> None:
        """One read-only request filling the title cache: ``ids`` None lists the forum's topics."""
        entity = self._entities.get(chat_id)
        if self._client is None or entity is None:
            raise TopicLookupError(f"chat {chat_id} is not connected")
        request: Any
        if ids is None:
            request = functions.messages.GetForumTopicsRequest(
                peer=entity, offset_date=None, offset_id=0, offset_topic=0, limit=TOPIC_LIST_LIMIT
            )
        else:
            request = functions.messages.GetForumTopicsByIDRequest(peer=entity, topics=list(ids))
        result = await asyncio.wait_for(self._client(request), TOPIC_REQUEST_TIMEOUT_SEC)
        for topic in getattr(result, "topics", None) or []:
            if isinstance(topic, types.ForumTopic):
                self._set_title(chat_id, topic.id, topic.title)
            elif isinstance(topic, types.ForumTopicDeleted):
                self._set_title(chat_id, topic.id, None)

    def _lookup_failed(self, key: tuple[int, int], exc: BaseException) -> None:
        failures = self._topic_failures.get(key, 0) + 1
        self._topic_failures[key] = failures
        delay = TOPIC_RETRY_DELAYS_SEC[min(failures, len(TOPIC_RETRY_DELAYS_SEC)) - 1]
        flood = getattr(exc, "seconds", None)            # FloodWaitError
        if isinstance(flood, (int, float)):
            delay = max(delay, float(flood))
        self._topic_retry_at[key] = time.monotonic() + delay
        log.warning("Could not fetch forum topic %s of %s (attempt %d): %s; retrying in %.0fs",
                    key[1], key[0], failures, str(exc) or type(exc).__name__, delay)

    def _cached_title(self, key: tuple[int, int]) -> tuple[bool, str | None]:
        if key not in self._topic_titles:
            return False, None
        until = self._topic_unknown_until.get(key)
        if until is not None and time.monotonic() >= until:
            del self._topic_titles[key]
            del self._topic_unknown_until[key]
            return False, None
        return True, self._topic_titles[key]

    def _set_title(self, chat_id: int, topic_id: int, title: str | None, *, ttl: float | None = None) -> None:
        key = (chat_id, topic_id)
        changed = key not in self._topic_titles or self._topic_titles[key] != title
        self._topic_titles[key] = title
        if ttl is None:
            self._topic_unknown_until.pop(key, None)
        else:
            self._topic_unknown_until[key] = time.monotonic() + ttl
        self._topic_retry_at.pop(key, None)
        self._topic_failures.pop(key, None)
        if changed and title is not None:
            if self._config.topics:
                route = match_topic(self._config.topics, title)
                verdict = f"followed (route '{route.name}')" if route else "ignored"
                log.info("Forum %s topic %s %r: %s", chat_id, topic_id, title, verdict)
            else:
                log.info("Forum %s topic %s %r", chat_id, topic_id, title)

    def _forget_topics(self, chat_id: int) -> None:
        for key in [k for k in self._topic_titles if k[0] == chat_id]:
            self._topic_titles.pop(key, None)
            self._topic_unknown_until.pop(key, None)

    def _apply_topic_action(self, message: Any) -> None:
        """Topic create / edit service message: update the title cache (no API call)."""
        action = getattr(message, "action", None)
        if not isinstance(action, _TOPIC_ACTIONS):
            return
        chat_id = message.chat_id
        self._mark_forum(chat_id)
        if isinstance(action, types.MessageActionTopicCreate):
            self._set_title(chat_id, message.id, action.title)
        elif getattr(action, "title", None) is not None:         # renamed (not just closed / hidden)
            if _in_forum_topic(message):
                self._set_title(chat_id, topic_id_of(message) or GENERAL_TOPIC_ID, action.title)
            else:
                # No topic header (General, or unexpected): titles are fetched again when needed.
                log.info("Forum %s: a topic was renamed to %r; refreshing topic titles", chat_id, action.title)
                self._forget_topics(chat_id)

    async def _load_topics(self) -> None:
        """At start: list every forum's topics (warms the cache, logs which ones are followed)."""
        for chat_id in self._entities:
            if chat_id not in self._forums:
                if self._config.topics:
                    self._warn_not_forum(chat_id)
                continue
            try:
                async with self._topic_lock(chat_id):
                    await self._fetch_topics(chat_id, None)
            except Exception as exc:
                log.warning("Could not list forum topics of %s: %s (titles are fetched per topic later)", chat_id, exc)
                continue
            titles = [t for (cid, _), t in self._topic_titles.items() if cid == chat_id and t]
            for route in self._config.topics:
                if not any(match_topic([route], t) for t in titles):
                    log.warning("No topic of forum %s matches telegram.topics '%s' %s yet; it is followed once "
                                "such a topic exists", chat_id, route.name, route.title_contains)

    def _mark_forum(self, chat_id: int) -> None:
        if chat_id in self._forums:
            return
        self._forums.add(chat_id)
        if self._config.topics:
            log.warning("Chat %s is a forum (seen in message headers): from now on only telegram.topics are followed",
                        chat_id)

    def _warn_not_forum(self, chat_id: int) -> None:
        tag = f"not-forum:{chat_id}"
        if tag not in self._warned:
            self._warned.add(tag)
            log.error("telegram.topics is set but chat %s is not a forum (no topics): its messages are stored, "
                      "but NONE of them will be traded while telegram.topics is set. Remove telegram.topics "
                      "for this group or check the group id.", chat_id)

    def _topic_lock(self, chat_id: int) -> asyncio.Lock:
        lock = self._topic_locks.get(chat_id)
        if lock is None:
            lock = self._topic_locks[chat_id] = asyncio.Lock()
        return lock

    def _topic_known(self, message: Any) -> bool:
        """The message can be filtered without waiting for Telegram."""
        chat_id = message.chat_id
        if isinstance(message, types.MessageService) or not self._config.topics:
            return True
        if chat_id not in self._forums and not _in_forum_topic(message):
            return True
        return self._cached_title((chat_id, topic_id_of(message) or GENERAL_TOPIC_ID))[0]

    async def _sleep_unless_stopping(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=max(seconds, 0.0))
        except (asyncio.TimeoutError, TimeoutError):
            pass

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
        chat_id, topic_id, sender_id = key
        try:
            await self._on_ready(chat_id, sender_id, topic_id)
        except Exception:
            log.exception("on_ready failed for chat %s topic %s sender %s", chat_id, topic_id, sender_id)
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
        # Stay below messages still being stored (download, topic lookup) or abandoned at shutdown:
        # after a crash / restart, catch-up fetches them again.
        unfinished = [mid for cid, mid in (*self._storing, *self._held_back) if cid == chat_id]
        if unfinished:
            value = min(value, min(unfinished) - 1)
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
        await self._prefetch_topics(chat_id, missed)
        now = datetime.now(timezone.utc)
        fresh = 0
        for msg in missed:
            young = should_process_catchup(msg.date, now, self._config.catchup_max_age_sec)
            fresh += young
            if self._topic_known(msg):
                await self.handle_message(msg, consumed=not young)
            else:
                # Its topic's title is unknown (Telegram error): wait for it in the background,
                # without holding up the rest of the catch-up.
                self._spawn(self.handle_message(msg, consumed=not young))
        log.info("Catch-up %s: %d missed message(s), %d recent enough to process", chat_id, len(missed), fresh)

    async def _prefetch_topics(self, chat_id: int, messages: list[Any]) -> None:
        """One request for the titles of every uncached topic among missed messages (filtered forums)."""
        if not self._config.topics:
            return
        created = {m.id for m in messages if isinstance(getattr(m, "action", None), types.MessageActionTopicCreate)}
        wanted = {
            topic_id_of(m) or GENERAL_TOPIC_ID
            for m in messages
            if not isinstance(m, types.MessageService) and self._accepts(m)
            and (chat_id in self._forums or _in_forum_topic(m))
        }
        wanted = {t for t in wanted - created if not self._cached_title((chat_id, t))[0]}
        if not wanted:
            return
        try:
            async with self._topic_lock(chat_id):
                await self._fetch_topics(chat_id, sorted(wanted))
        except Exception as exc:
            log.warning("Could not fetch forum topics %s of %s during catch-up: %s", sorted(wanted), chat_id, exc)

    def _resume_followed(self, msg: StoredMessage) -> bool:
        """Stored rows of a filtered forum without a matching title (e.g. stored before topic support) stay put."""
        return not self._filtering(msg.chat_id) or match_topic(self._config.topics, msg.topic_title) is not None

    async def _resume_unprocessed(self) -> None:
        """Re-trigger recent stored messages whose debounce never fired (crash / restart)."""
        since = datetime.now(timezone.utc) - timedelta(seconds=self._config.catchup_max_age_sec)
        for chat_id in self._entities:
            try:
                msgs = await self._db.recent_messages(chat_id, since, only_unconsumed=True)
            except Exception:
                log.exception("Could not read unprocessed messages of %s", chat_id)
                continue
            keys: set[DebounceKey] = {
                (chat_id, m.topic_id, m.sender_id)
                for m in msgs
                if (not self._allowed or m.sender_id in self._allowed) and self._resume_followed(m)
            }
            for dkey in keys:
                self._debounce_hold(dkey)
                self._debounce_release(dkey)
            if keys:
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


# Countries whose national numbers start with a trunk "0" that must be dropped after the
# country code (+44 07765... -> +447765...). Italy (+39) is deliberately absent: its 0 stays.
_TRUNK_ZERO_CODES = ("44", "90", "49", "33", "31", "32", "41", "43", "61", "64", "81", "82",
                     "86", "91", "92", "353", "380")


def normalize_phone(raw: str) -> str:
    """'+44 0776 564-3200' / '0044...' -> '+447765643200'. Leaves anything unrecognised as typed."""
    s = re.sub(r"[\s\-().]", "", raw.strip())
    if s.startswith("00"):
        s = "+" + s[2:]
    if not s.startswith("+"):
        return s
    digits = s[1:]
    for cc in sorted(_TRUNK_ZERO_CODES, key=len, reverse=True):
        if digits.startswith(cc + "0") and len(digits) > len(cc) + 6:
            return "+" + cc + digits[len(cc) + 1:]
    return s


def _code_channel_tr(sent_type: Any) -> str:
    """Where Telegram says it sent the login code (Turkish, for the owner)."""
    name = type(sent_type).__name__
    labels = {
        "SentCodeTypeApp": "Telegram uygulamasındaki 'Telegram' sohbetine",
        "SentCodeTypeSms": "SMS ile telefonunuza",
        "SentCodeTypeFirebaseSms": "SMS ile telefonunuza",
        "SentCodeTypeFragmentSms": "SMS ile telefonunuza",
        "SentCodeTypeCall": "telefon araması ile",
        "SentCodeTypeFlashCall": "telefon araması ile",
        "SentCodeTypeMissedCall": "kaçan arama ile (son rakamlar)",
        "SentCodeTypeEmailCode": "e-posta ile",
    }
    return labels.get(name, name)


async def interactive_login(config: TelegramConfig) -> str:
    """First-time login in a terminal (phone, code, 2FA). Saves the session file and returns a StringSession string."""

    async def phone() -> str:
        while True:
            value = await _ask("Telefon numarası (ülke koduyla, ör. +905551112233): ")
            if ":" in value:     # Telethon would treat "id:hash" as a bot token
                print("Bu bir bot token'ı gibi görünüyor; kendi telefon numaranızı girin.")
                continue
            fixed = normalize_phone(value)
            if fixed != value:
                print(f"Numara uluslararası biçime çevrildi: {fixed}")
            return fixed

    client = make_user_client(config, prefer_file=True)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            number = await phone()
            # Ask Telegram to send the login code by SMS (the in-app message may never arrive
            # on a new device), then sign in with the code the owner types.
            # Telegram no longer lets third-party apps force SMS; the code goes where Telegram decides.
            sent = await client.send_code_request(number)
            print(f"Telegram kodu şuraya gönderdi: {_code_channel_tr(sent.type)}.")
            if sent.next_type is not None:
                print(f"Gelmezse sonraki seçenek: {_code_channel_tr(sent.next_type)}.")
            code = (await _ask("Gelen giriş kodu: ")).replace(" ", "")
            try:
                await client.sign_in(number, code, phone_code_hash=sent.phone_code_hash)
            except errors.SessionPasswordNeededError:
                await _sign_in_password(client)
        me = await client.get_me()
        if getattr(me, "bot", False):
            raise RuntimeError("Bot hesabıyla giriş yapılamaz; kendi kullanıcı hesabınızı kullanın.")
        log.info("Logged in as %s (id %s)", _display_name(me), getattr(me, "id", None))
        return StringSession.save(client.session)
    finally:
        await client.disconnect()


def render_qr_text(data: str, border: int = 2) -> str:
    """QR code as text for a dark terminal (PowerShell / Windows Terminal).

    Light modules are drawn with block characters and dark modules are left as the
    black background, two module rows per text line, so the code keeps the normal
    dark-on-light polarity and roughly square modules."""
    import segno  # pure Python dependency, only needed for QR login

    matrix = segno.make(data, error="l").matrix
    size = len(matrix)
    total = size + 2 * border

    def light(r: int, c: int) -> bool:
        r -= border
        c -= border
        if 0 <= r < size and 0 <= c < size:
            return not matrix[r][c]
        return True  # quiet zone

    lines: list[str] = []
    for r in range(0, total, 2):
        row = []
        for c in range(total):
            top = light(r, c)
            bottom = light(r + 1, c) if r + 1 < total else False
            row.append("█" if top and bottom else "▀" if top else "▄" if bottom else " ")
        lines.append("".join(row))
    return "\n".join(lines)


def _clear_screen() -> None:
    import os

    os.system("cls" if os.name == "nt" else "clear")


QR_HELP_TR = (
    "Telefonunuzda: Telegram → Ayarlar → Cihazlar → 'Masaüstü Cihaz Bağla' (Scan QR) → bu QR'ı okutun.\n"
    "ÖNEMLİ: QR'ı aynı telefonun ekranından okutamazsınız. Bu ekranı BAŞKA bir cihazda açın\n"
    "(bilgisayar, tablet ya da başka bir telefondan Uzak Masaüstü) ve kendi telefonunuzla okutun."
)


async def _sign_in_password(client: TelegramClient) -> None:
    """Ask for the 2FA password until it is right (typos are easy: nothing is shown while typing)."""
    while True:
        password = await _ask("İki adımlı doğrulama şifresi: ", secret=True)
        try:
            await client.sign_in(password=password)
            return
        except errors.PasswordHashInvalidError:
            print("Şifre yanlış, tekrar deneyin.")


async def _qr_accepted_during_recreate(client: TelegramClient, resp: Any) -> bool:
    """A scan accepted between two wait() calls shows up in recreate()'s response.

    Finish the login the same way QRLogin.wait() does (incl. the DC switch)."""
    if isinstance(resp, types.auth.LoginTokenMigrateTo):
        await client._switch_dc(resp.dc_id)
        resp = await client(functions.auth.ImportLoginTokenRequest(resp.token))
    if isinstance(resp, types.auth.LoginTokenSuccess):
        await client._on_login(resp.authorization.user)
        return True
    return False


async def interactive_qr_login(config: TelegramConfig, *, max_minutes: float = 10.0) -> str:
    """Login by scanning a QR code with the already logged-in Telegram app (no login code needed).

    Telegram does not reliably deliver login codes to third-party apps; QR login avoids that.
    The QR refreshes about every 30 seconds. Saves the session file and returns a StringSession string."""
    from pathlib import Path

    try:
        import segno
    except ImportError as exc:
        raise TelegramAuthError(
            "QR girişi için 'segno' paketi gerekli: 'pip install -r requirements.txt' çalıştırın "
            "(ya da kodla giriş: python -m copytrader login --code)."
        ) from exc

    client = make_user_client(config, prefer_file=True)
    png_path = Path(config.session_path).parent / "login_qr.png"
    try:
        await client.connect()
        if not await client.is_user_authorized():
            try:
                qr = await client.qr_login()
                deadline = time.monotonic() + max_minutes * 60
                while True:
                    if time.monotonic() > deadline:
                        raise TelegramAuthError(
                            f"QR {max_minutes:.0f} dakika içinde okutulmadı. Komutu tekrar çalıştırın."
                        )
                    _clear_screen()
                    print(render_qr_text(qr.url))
                    print()
                    print(QR_HELP_TR)
                    try:
                        png_path.parent.mkdir(parents=True, exist_ok=True)
                        segno.make(qr.url, error="l").save(str(png_path), scale=10, border=4)
                        print(f"Daha büyük görmek için resim dosyası: {png_path.resolve()}")
                    except OSError:
                        pass
                    # Do not trust the VPS clock against Telegram's (a clock running ahead would make
                    # wait() time out instantly and spin): tokens live about 30 seconds.
                    left = (qr.expires - datetime.now(timezone.utc)).total_seconds()
                    left = min(max(left, 10.0), 30.0)
                    print(f"Bu QR yaklaşık {left:.0f} saniye geçerli; süresi dolunca kendiliğinden yenilenir.")
                    try:
                        await qr.wait(timeout=left)
                        break
                    except (asyncio.TimeoutError, TimeoutError):
                        await qr.recreate()
                        if await _qr_accepted_during_recreate(client, getattr(qr, "_resp", None)):
                            break
            except errors.SessionPasswordNeededError:
                # Raised by qr_login()/wait()/recreate() once the scan is accepted on a 2FA account,
                # and by qr_login() on a rerun after an unfinished 2FA step.
                await _sign_in_password(client)
            try:
                png_path.unlink()
            except OSError:
                pass
        me = await client.get_me()
        if getattr(me, "bot", False):
            raise RuntimeError("Bot hesabıyla giriş yapılamaz; kendi kullanıcı hesabınızı kullanın.")
        log.info("Logged in via QR as %s (id %s)", _display_name(me), getattr(me, "id", None))
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
