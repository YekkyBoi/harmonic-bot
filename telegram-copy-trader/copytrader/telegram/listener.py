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
A new or renamed topic is reported to the owner once (followed, or matching no route).

Messages that arrive while the listener is away
-----------------------------------------------
* One age rule everywhere: a message older than ``catchup_max_age_sec`` when it reaches the bot
  (restart, reconnect, late delivery by the update stream) is stored as context only, and the
  owner gets ONE notice per occasion (how long the bot was away, how many messages, the first
  lines of up to five); for messages that simply arrive late the notices are throttled per topic.
* The listener's own catch-up runs at start, after a reconnect and every ``CATCHUP_POLL_SEC``
  (one read-only history request), so a message the update stream never delivered is fetched
  within seconds.
* At start the app lets the listener run while the brokers connect (``hold_batches`` /
  ``release_batches``): messages are stored and judged by their age at once, batches and edits
  are passed on when the engine is ready.
* A batch is defined when it is triggered: its messages are pinned in storage until ``on_ready``
  returns, so waiting cannot push them out of the engine's time window.

The owner is also told when an image cannot be downloaded (after three tries), when an unprocessed
message or the signal of an open trade is deleted in the group (nothing is ever closed
automatically), and when a message could not be stored or its batch failed.

The copy of the group for the owner's friend (``/grup``, group_mirror.py)
-------------------------------------------------------------------------
Every new message, edit and catch-up find of the source chats is offered to the GroupMirror AFTER the
listener's own handling of it has been started (live) or done (catch-up): the offer never waits and never
raises, and nothing of the handling above depends on it. The mirror reads the topic-title cache (it never
asks Telegram for a title), uploads the image the listener already downloaded for a followed topic, and
moves media only while :meth:`TelegramListener.trading_busy` is False.
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
from ..models import Severity, StoredMessage, TradeStatus
from .formatting import esc

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
MEDIA_DOWNLOAD_ATTEMPTS = 3          # a failed image download is tried again twice
MEDIA_RETRY_DELAYS_SEC = (1.0, 3.0)  # pauses before the second and the third try
MAX_IMAGE_BYTES = 20 * 1024 * 1024
STOP_GRACE_SEC = 30.0                # let running callbacks (maybe placing orders) finish
RECONNECT_BASE_DELAY_SEC = 1.0
RECONNECT_MAX_DELAY_SEC = 30.0       # a longer wait only adds to the time the bot is away after an outage
_SEEN_LIMIT = 5000
CATCHUP_POLL_SEC = 30.0              # the listener's own catch-up runs this often (one read-only history request)
CATCHUP_REQUEST_TIMEOUT_SEC = 20.0   # ... and gives up after this (tried again at the next round)
POLL_FETCH_LIMIT = 20                # newest messages asked for by a periodic round (Telethon filters by id locally,
                                     # so the request always returns this many); all new -> the full fetch follows
SKIP_NOTICE_THROTTLE_SEC = 600.0     # live paths: at most one "not processed" notice per topic in this time
SKIP_NOTICE_COLLECT_SEC = 2.0        # messages skipped together (album, gap recovery) share one notice
SKIP_NOTICE_MAX_LINES = 5            # messages listed in one notice (the rest is counted)
FIRST_LINE_CHARS = 120
STALE_SCAN_DAYS = 30                 # at start: unprocessed stored messages this recent are reported
NOTIFY_TIMEOUT_SEC = 60.0            # a notice that cannot be sent never blocks the listener longer
ALIVE_KEY = "listener_alive_at"      # storage state: when the listener was last running (ISO time, UTC)
TOPICS_KEY = "topics_known:{chat_id}"  # storage state: {topic id: title} the owner has been told about
DELETED_BATCH_LIMIT = 200            # deleted message ids looked at per event
NOTICE_REPEAT_SEC = 600.0            # the same kind of problem notice is not repeated within this
_ACTIVE_TRADE = (TradeStatus.OPENING, TradeStatus.PENDING, TradeStatus.OPEN)
TOPIC_HINT_TR = ("Sinyal konusu olması gereken bir konunun adı farklı yazıldıysa adını düzelttirin ya da "
                 "config.yaml içindeki konu adlarına ekleyin.")

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


class SkippedMessage(NamedTuple):
    """A message of a followed topic that was NOT processed because of its age."""

    chat_id: int
    message_id: int
    topic_id: int | None
    topic_title: str | None
    date: datetime
    text: str
    has_image: bool


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #
def should_process_catchup(msg_date: datetime, now: datetime, max_age_sec: float) -> bool:
    """Catch-up rule: a missed message is executed only if it is younger than ``max_age_sec``.

    Older ones are stored as context only (consumed=True)."""
    return (_as_utc(now) - _as_utc(msg_date)).total_seconds() <= max_age_sec


def duration_tr(seconds: float) -> str:
    """'45 saniye', '12 dakika', '3 saat 5 dakika', '2 gün 4 saat' (rounded; for owner notices)."""
    sec = max(0, int(round(seconds)))
    if sec < 90:
        return f"{sec} saniye"
    minutes = int(round(sec / 60))
    if minutes < 90:
        return f"{minutes} dakika"
    hours, minutes = divmod(minutes, 60)
    if hours < 48:
        return f"{hours} saat {minutes} dakika" if minutes else f"{hours} saat"
    days, hours = divmod(hours, 24)
    return f"{days} gün {hours} saat" if hours else f"{days} gün"


def first_line(text: str, has_image: bool = False, limit: int = FIRST_LINE_CHARS) -> str:
    """First non-empty line of a message, shortened (not escaped)."""
    for line in (text or "").splitlines():
        line = " ".join(line.split())
        if line:
            return line if len(line) <= limit else line[: limit - 1] + "…"
    return "(resim, metin yok)" if has_image else "(metin yok)"


def skipped_notice_tr(
    items: list[SkippedMessage],
    *,
    occasion: str,
    away_sec: float | None,
    max_age_sec: float,
    now: datetime,
) -> str:
    """Owner notice (Telegram HTML) about followed-topic messages that were not processed because
    of their age. ``occasion``: "restart", "reconnect" or "late" (delivered late while running)."""
    count = len(items)
    limit = f"{max_age_sec:g} saniyeden eski olduğu için İŞLENMEDİ"
    if occasion == "restart":
        head = (f"Bot yaklaşık {duration_tr(away_sec)} kapalıydı." if away_sec is not None
                else "Bot yeniden başladı.")
        head += f" Takip edilen konularda bot kapalıyken yazılan ya da işlenmeden kalan {count} mesaj, {limit}"
    elif occasion == "reconnect":
        head = (f"Telegram bağlantısı yaklaşık {duration_tr(away_sec)} kesikti." if away_sec is not None
                else "Telegram bağlantısı bir süre kesikti.")
        head += f" Takip edilen konularda bu sürede yazılan {count} mesaj, {limit}"
    else:
        head = f"Takip edilen konulardaki {count} mesaj bota geç ulaştı ve {limit}"
    lines = [head + " (bu mesajlardan işlem açılmadı, kapatılmadı, stop taşınmadı):"]
    shown = sorted(items, key=lambda m: (_as_utc(m.date), m.message_id))[-SKIP_NOTICE_MAX_LINES:]
    for item in shown:
        age = (_as_utc(now) - _as_utc(item.date)).total_seconds()
        topic = item.topic_title or "kaynak grup"
        lines.append(f"• <b>{esc(topic)}</b> · {duration_tr(age)} önce: {esc(first_line(item.text, item.has_image))}")
    if count > len(shown):
        lines.append(f"… ve {count - len(shown)} mesaj daha.")
    lines.append("Grubu kontrol edin; gerekiyorsa işlemi elle yapın.")
    if occasion == "late":
        lines.append("Mesajlar sık sık geç görünüyorsa sunucunun saati ileri olabilir; saati kontrol edin.")
    return "\n".join(lines)


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
        # ---- owner notices / start-up gate / batches defined at trigger time ----
        self._notifier: Any = None
        self._ready = asyncio.Event()                # cleared while batches are held (start-up)
        self._ready.set()
        self._batch: dict[DebounceKey, set[int]] = {}        # message ids that triggered a stream's next batch
        self._in_engine: set[tuple[int, int]] = set()        # ... and those whose on_ready call is running
        self._catchup_lock = asyncio.Lock()
        self._poll_task: asyncio.Task[None] | None = None
        self._poll_failing = False
        # ("restart" | "reconnect", last time the bot / the connection was known to be alive) until
        # the next complete catch-up has told the owner what was skipped meanwhile.
        self._outage: tuple[str, datetime | None] | None = None
        self._skip_pending: dict[tuple[int, int | None], list[SkippedMessage]] = {}
        self._skip_notified_at: dict[tuple[int, int | None], float] = {}
        self._skip_flush: dict[tuple[int, int | None], asyncio.Task[Any]] = {}
        self._download_failed: dict[tuple[int, int], str] = {}   # why an image could not be downloaded (Turkish)
        self._deleted: OrderedDict[tuple[int, int], None] = OrderedDict()   # deleted in the source chat
        self._topic_changes: list[tuple[int, int, str]] = []     # (chat, topic, title) not yet told to the owner
        self._topic_listing = False                              # start(): one notice for the whole listing
        self._topic_notice_lock = asyncio.Lock()
        self._noticed_at: dict[str, float] = {}                  # throttle of repeated problem notices
        self._mirror: Any = None                                 # the group copy for the owner's friend (/grup)
        self._editing = 0                                        # edits being stored / passed on (the group copy waits)

    @property
    def chat_ids(self) -> set[int]:
        return set(self._entities)

    # ---- wiring (called by the app before start()) --------------------------
    def set_notifier(self, notifier: Any) -> None:
        """The owner's notifier (``notify(text, severity)``). Without one, notices are only logged."""
        self._notifier = notifier

    def hold_batches(self) -> None:
        """Start-up: messages are still stored (and judged by their age at arrival), but no batch and
        no edit is passed on until :meth:`release_batches` (the engine is still connecting brokers)."""
        self._ready.clear()

    def release_batches(self) -> None:
        self._ready.set()

    # ---- the copy of the group for the owner's friend (/grup, group_mirror.py) ------------------------
    def set_group_mirror(self, mirror: Any) -> None:
        """The GroupMirror: offered every message of the source chats (it copies them only when switched on)."""
        self._mirror = mirror

    def _offer_mirror(self, message: Any, handled: Any = None, *, edited: bool = False, catchup: bool = False) -> None:
        mirror = self._mirror
        if mirror is None or self._stopping:
            return
        try:
            mirror.offer(message, handled=handled, edited=edited, catchup=catchup)
        except Exception:
            log.exception("Group copy: a message was not offered")

    def trading_busy(self) -> bool:
        """A source message is being stored (topic title, image download) or edited (its new image, the engine's
        on_edit), or a batch waits for / is in the engine."""
        return bool(self._storing or self._inflight or self._timers or self._in_engine or self._editing)

    def mirror_known_topics(self) -> list[str]:
        """The titles of the forum topics in the cache (every source chat), for /grup: which goes where."""
        titles: list[str] = []
        for (_chat, _topic), title in sorted(self._topic_titles.items(), key=lambda kv: (kv[0][0], kv[0][1])):
            if title and title not in titles:
                titles.append(title)
        return titles

    def mirror_topic(self, message: Any) -> tuple[str | None, str | None, int]:
        """(chat title, forum topic title or None for a chat without topics, number of source chats) of a message,
        from the cache only: never a request, never a change of the cache."""
        chat_id = getattr(message, "chat_id", None)
        chat_title = getattr(self._entities.get(chat_id), "title", None) if chat_id is not None else None
        topic: str | None = None
        if chat_id in self._forums or _in_forum_topic(message):
            topic_id = topic_id_of(message)
            key = (chat_id, topic_id or GENERAL_TOPIC_ID)
            title = self._topic_titles.get(key) if key not in self._topic_unknown_until else None
            topic = title or (GENERAL_TOPIC_TITLE if topic_id is None else f"Konu #{topic_id}")
        return chat_title, topic, max(1, len(self._entities))

    async def mirror_stored_media(self, chat_id: int, msg_id: int) -> str | None:
        """The image the listener downloaded for this message (a followed topic), if its file is there."""
        try:
            row = await self._db.get_message(chat_id, msg_id)
        except Exception as exc:
            log.debug("Group copy: stored message %s/%s not readable: %s", chat_id, msg_id, exc)
            return None
        path = getattr(row, "media_path", None) if row is not None else None
        return path if path and Path(path).is_file() else None

    async def _notify(self, text: str, severity: Severity = Severity.WARNING) -> None:
        if self._notifier is None:
            log.warning("Owner notice (no notifier attached): %s", text)
            return
        try:
            await asyncio.wait_for(self._notifier.notify(text, severity), NOTIFY_TIMEOUT_SEC)
        except Exception as exc:
            log.error("Owner notice could not be sent (%s): %s", str(exc) or type(exc).__name__, text)

    def _notify_soon(self, text: str, severity: Severity = Severity.WARNING) -> None:
        """Send a notice in its own task: message handling never waits for the notifier."""
        self._spawn(self._notify(text, severity))

    def _notify_problem(self, kind: str, text: str, severity: Severity = Severity.WARNING) -> None:
        """Like _notify_soon, but the same ``kind`` at most once in NOTICE_REPEAT_SEC."""
        now = time.monotonic()
        last = self._noticed_at.get(kind)
        if last is not None and now - last < NOTICE_REPEAT_SEC:
            return
        self._noticed_at[kind] = now
        self._notify_soon(text, severity)

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
        client.remove_event_handler(self._on_deleted_event)
        client.add_event_handler(self._on_new_message_event, events.NewMessage(chats=ids))
        client.add_event_handler(self._on_edited_event, events.MessageEdited(chats=ids))
        client.add_event_handler(self._on_deleted_event, events.MessageDeleted(chats=ids))
        # NewMessage drops service messages; topic create/edit ones keep the title cache fresh.
        client.add_event_handler(self._on_raw_update, events.Raw(types.UpdateNewChannelMessage))
        log.info("Listening to %d source chat(s): %s", len(ids), ids)
        self._outage = ("restart", await self._read_alive())
        await self._load_topics()
        skipped: list[SkippedMessage] = []
        complete = await self._catch_up(skipped)
        await self._resume_unprocessed(skipped)
        self._report_occasion(skipped, complete=complete)
        await self._heartbeat()
        if self._poll_task is None or self._poll_task.done():
            self._poll_task = asyncio.ensure_future(self._poll_loop())

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
            if self._outage is None:
                self._outage = ("reconnect", datetime.now(timezone.utc))
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
        self._stop_event.set()           # wakes messages waiting for a topic title, pending notices, the poll
        for task in list(self._timers.values()):
            task.cancel()
        self._timers.clear()
        self._ready.set()                # nothing waits at the start-up gate any more (the engine refuses new work)
        if self._poll_task is not None:
            self._poll_task.cancel()
            await asyncio.gather(self._poll_task, return_exceptions=True)
            self._poll_task = None
        running = [t for t in self._tasks if not t.done()]
        if running:
            _, pending = await asyncio.wait(running, timeout=STOP_GRACE_SEC)
            for task in pending:
                task.cancel()
        for dkey, ids in self._batch.items():
            self._unpin(dkey[0], ids)    # never fired: after the restart they are judged by their age again
        self._batch.clear()
        await self._heartbeat()          # "how long was the bot away" is measured from here
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
        task = self._spawn(self.handle_message(event.message, live=True))
        self._offer_mirror(event.message, task)

    async def _on_edited_event(self, event: Any) -> None:
        self._spawn(self.handle_edit(event.message))
        self._offer_mirror(event.message, edited=True)

    async def _on_deleted_event(self, event: Any) -> None:
        chat_id = getattr(event, "chat_id", None)
        ids = list(getattr(event, "deleted_ids", None) or [])
        if chat_id is not None and ids:
            self._spawn(self.handle_deleted(chat_id, ids))

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

    async def handle_message(
        self,
        message: Any,
        *,
        consumed: bool = False,
        live: bool = False,
        skipped: list[SkippedMessage] | None = None,
    ) -> None:
        """New message (live or catch-up): store it, then debounce ``on_ready``.

        Forum + ``telegram.topics``: messages of topics no route matches are neither stored nor
        downloaded. Topic create/edit service messages only update the title cache.

        ``live``: delivered by Telegram's update stream; the catch-up age rule applies to it too (a
        message that reaches the bot more than ``catchup_max_age_sec`` after it was written is stored
        as context only and the owner is told). ``consumed`` + ``skipped``: the catch-up judged the
        message too old; it is added to ``skipped`` (the caller sends one notice for all of them)."""
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
            self._notify_problem(
                f"topic-lookup:{chat_id}",
                "Telegram bir konunun adını vermediği için kaynak gruptaki bir mesaj kaydedilemedi ve "
                f"işlenmedi: {esc(first_line(getattr(message, 'message', None) or '', image_info(message) is not None))}\n"
                "Takip edilen bir konudaysa grubu kontrol edin; gerekiyorsa işlemi elle yapın.",
            )
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
        has_content = bool(getattr(message, "message", None)) or image_info(message) is not None
        too_old = consumed and skipped is not None           # judged by the catch-up
        if not consumed and (live or waited > TOPIC_LATE_SEC) and not should_process_catchup(
            message.date, datetime.now(timezone.utc), self._config.catchup_max_age_sec
        ):
            if waited > TOPIC_LATE_SEC:
                log.warning("Message %s/%s waited %.0fs for its forum topic; stored as context only",
                            chat_id, message.id, waited)
            else:
                log.warning("Message %s/%s reached the bot %.0fs after it was written; stored as context only",
                            chat_id, message.id, (datetime.now(timezone.utc) - _as_utc(message.date)).total_seconds())
            consumed = too_old = True
        if not has_content:
            consumed = True              # sticker, voice, video...: context only, never waits to be processed
        if too_old and has_content:
            item = SkippedMessage(chat_id, message.id, topic.topic_id, topic.title, _as_utc(message.date),
                                  getattr(message, "message", None) or "", image_info(message) is not None)
            if skipped is not None:
                skipped.append(item)
            else:
                self._report_skipped_live(item)
        trigger = has_content and not consumed
        dkey: DebounceKey = (chat_id, topic.topic_id, getattr(message, "sender_id", None))
        if trigger:
            self._debounce_hold(dkey, message.id)
        late_edit: Any = None
        try:
            stored = await self._build_stored(message, consumed=consumed, topic=topic, notify_download=not consumed)
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
                    notify_download=not consumed,
                )
            if key in self._deleted and not stored.consumed:
                # Deleted in the group while its image was downloading: stored as context, never executed.
                stored = stored.model_copy(update={"consumed": True})
                self._batch_discard(chat_id, message.id)
                self._deleted_notice(stored)
            await self._db.save_message(stored)
            await self._save_meta(message, stored, deleted=key in self._deleted)
        except Exception as exc:
            log.exception("Could not store message %s/%s", chat_id, message.id)
            self._notify_problem(
                f"store:{chat_id}",
                f"Kaynak gruptaki bir mesaj kaydedilemedi (<code>{esc(str(exc) or type(exc).__name__)}</code>); "
                "bu mesaj işlenmemiş olabilir. Grubu kontrol edin; gerekiyorsa işlemi elle yapın.",
            )
        finally:
            late_edit = self._storing.pop(key, None)
            if trigger:
                self._debounce_release(dkey)
        await self._persist_last_id(chat_id)
        if late_edit is not None:
            # Arrived while the row was being written: apply it as a normal edit now.
            await self.handle_edit(late_edit)

    async def handle_edit(self, message: Any) -> None:
        """Edited message: store the new version, then ``on_edit`` (only for followed topics). Counted in
        ``_editing`` while it runs (only read by :meth:`trading_busy`)."""
        self._editing += 1
        try:
            await self._handle_edit(message)
        finally:
            self._editing -= 1

    async def _handle_edit(self, message: Any) -> None:
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
                await self.handle_message(message, live=True)   # never saw it: treat as new
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
        known = await self._known_media_id(existing) if info is not None else None
        media_changed = self._media_changed(existing, info, known=known, same_text=text == existing.text)
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
        await self._save_meta(message, stored)
        if self._ready.is_set():
            await self._pass_edit(stored)
        else:
            # Start-up: passed on once the engine is ready (never waited for here: this may run
            # inside start()'s catch-up, which must finish before the batches are released).
            self._spawn(self._pass_edit(stored))

    async def _pass_edit(self, stored: StoredMessage) -> None:
        await self._ready.wait()
        self._editing += 1
        try:
            await self._on_edit(stored)
        except Exception:
            log.exception("on_edit failed for %s/%s", stored.chat_id, stored.message_id)
        finally:
            self._editing -= 1

    def _media_changed(
        self,
        existing: StoredMessage,
        info: tuple[str, str, int | None, int | None] | None,
        *,
        known: int | None = None,
        same_text: bool = False,
    ) -> bool:
        """The image of an edited message differs from the stored one. ``known``: the Telegram id of
        the stored image (default: what this process remembers). When that id is not known at all (a
        row stored by an older version) and the file is there, only a changed text speaks for a new
        image: an update that changes nothing (a reaction) must not look like a new screenshot."""
        if info is None:
            return existing.media_type is not None
        if known is None:
            known = self._media_ids.get((existing.chat_id, existing.message_id))
        if not existing.media_path or not Path(existing.media_path).exists():
            return True
        if known is None:
            return not same_text
        return known != info[2]

    async def _known_media_id(self, existing: StoredMessage) -> int | None:
        """Telegram id of the stored image: from memory, else from the row (survives a restart)."""
        key = (existing.chat_id, existing.message_id)
        known = self._media_ids.get(key)
        meta = getattr(self._db, "message_meta", None)
        if known is None and meta is not None:
            try:
                row = await meta(*key)
            except Exception as exc:
                log.debug("message_meta failed for %s/%s: %s", *key, exc)
                row = None
            known = row.get("media_id") if row else None
            if known is not None:
                self._remember(self._media_ids, key, known)
        return known

    async def _save_meta(self, message: Any, stored: StoredMessage, *, deleted: bool = False) -> None:
        """Extras kept with the row: the Telegram id of its image (the "same photo" check of an edit
        works after a restart), whether the message was forwarded into the chat, and whether it was
        deleted there while it was being stored."""
        save = getattr(self._db, "set_message_meta", None)
        if save is None:
            return
        fields: dict[str, Any] = {}
        info = image_info(message)
        if info is not None:
            fields["media_id"] = info[2] if stored.media_path else None
        if getattr(message, "fwd_from", None) is not None:
            fields["forwarded"] = True
        if deleted:
            fields["deleted"] = True
        if not fields:
            return
        try:
            await save(stored.chat_id, stored.message_id, **fields)
        except Exception:
            log.exception("Could not store the extras of message %s/%s", stored.chat_id, stored.message_id)

    # ---- deleted messages ---------------------------------------------------------------
    async def handle_deleted(self, chat_id: int, message_ids: list[int]) -> None:
        """Messages were deleted in a source chat. A stored message that was not processed yet is
        closed (it is never executed); when the deleted message is the signal of an open trade the
        owner is told. Nothing is ever closed or cancelled automatically."""
        if self._entities and chat_id not in self._entities:
            return
        for msg_id in message_ids[:DELETED_BATCH_LIMIT]:
            self._remember(self._deleted, (chat_id, msg_id), None)   # also seen by a message still being stored
            try:
                row = await self._db.get_message(chat_id, msg_id)
                if row is not None:
                    mark = getattr(self._db, "set_message_meta", None)
                    if mark is not None:
                        await mark(chat_id, msg_id, deleted=True)
                    await self._deleted_row(row)
            except Exception:
                log.exception("Could not handle the deletion of %s/%s", chat_id, msg_id)

    async def _deleted_row(self, row: StoredMessage) -> None:
        key = (row.chat_id, row.message_id)
        if not row.consumed:
            consume = getattr(self._db, "mark_consumed", None)
            if consume is not None:
                await consume(row.chat_id, [row.message_id])
            self._batch_discard(*key)
            log.info("Message %s/%s was deleted before it was processed; closed without action", *key)
            self._deleted_notice(row)
        find = getattr(self._db, "find_trade_by_message", None)
        trade = await find(*key) if find is not None else None
        if trade is not None and trade.status in _ACTIVE_TRADE:
            log.warning("Source message %s/%s of active trade #%s was deleted", *key, trade.id)
            self._notify_soon(
                f"<b>{esc(row.topic_title or 'kaynak grup')}</b> konusunda #{trade.id} "
                f"{esc(trade.plan.base_asset)} {esc(str(trade.plan.side.value).upper())} işleminin sinyal mesajı "
                f"silindi: {esc(first_line(row.text, row.media_type is not None))}\n"
                "Bot hiçbir şeyi kendiliğinden kapatmaz: işlem ve emirleri olduğu gibi duruyor. Trader "
                "vazgeçtiyse işlemi siz kapatın."
            )

    def _deleted_notice(self, row: StoredMessage) -> None:
        """An unprocessed message was deleted in the group."""
        if not (row.text or row.media_type):
            return
        title = esc(row.topic_title or "kaynak grup")
        line = esc(first_line(row.text, row.media_type is not None))
        if (row.chat_id, row.message_id) in self._in_engine:
            self._notify_soon(
                f"<b>{title}</b> konusundaki bir mesaj okunurken silindi: {line}\n"
                "Okuma durdurulamadı: bu mesajdan işlem açılmış olabilir. Bot hiçbir şeyi kendiliğinden "
                "kapatmaz; açık işlemleri kontrol edin."
            )
        else:
            self._notify_soon(
                f"<b>{title}</b> konusundaki bir mesaj işlenmeden önce silindi; bu mesajdan işlem "
                f"yapılmayacak: {line}",
                Severity.INFO,
            )

    def _batch_discard(self, chat_id: int, msg_id: int) -> None:
        """Take a message out of the batch it had triggered (it was deleted)."""
        for dkey, ids in self._batch.items():
            if dkey[0] == chat_id:
                ids.discard(msg_id)
        self._unpin(chat_id, [msg_id])

    async def _build_stored(
        self,
        message: Any,
        *,
        consumed: bool = False,
        edited: bool = False,
        download: bool = True,
        keep_media: tuple[str | None, str | None] = (None, None),
        topic: TopicRef | None = None,
        notify_download: bool = True,
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
            reason = self._download_failed.pop((chat_id, msg_id), None)
            if media_path is None and notify_download:
                # The message goes on without its image; the owner hears about it at once.
                title = (topic.title if topic is not None else None) or "kaynak grup"
                text = first_line(getattr(message, "message", None) or "", has_image=True)
                self._notify_soon(
                    f"<b>{esc(title)}</b> konusundaki bir mesajın resmi indirilemedi "
                    f"({esc(reason or 'bilinmeyen hata')}). Mesaj: {esc(text)}\n"
                    "Seviyeler resimdeyse sinyal eksik okunur ve işlem açılmayabilir; grubu kontrol edin, "
                    "gerekiyorsa işlemi elle açın."
                )
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
        """Download the image of a message: MEDIA_DOWNLOAD_ATTEMPTS tries (each limited to
        MEDIA_DOWNLOAD_TIMEOUT_SEC). None = no file; why (Turkish, for the owner) is left in
        ``self._download_failed``."""
        _, ext, _, size = info
        key = (message.chat_id, message.id)
        if size is not None and size > MAX_IMAGE_BYTES:
            log.warning("Image in %s/%s too large (%d bytes); not downloaded", message.chat_id, message.id, size)
            self._download_failed[key] = (f"resim çok büyük: {size / 1024 / 1024:.0f} MB, en çok "
                                          f"{MAX_IMAGE_BYTES / 1024 / 1024:.0f} MB indirilir")
            return None
        self._media_dir.mkdir(parents=True, exist_ok=True)
        target = self._media_dir / f"{abs(message.chat_id)}_{message.id}{ext}"
        reason = ""
        for attempt in range(1, MEDIA_DOWNLOAD_ATTEMPTS + 1):
            if attempt > 1:
                pause = MEDIA_RETRY_DELAYS_SEC[min(attempt - 2, len(MEDIA_RETRY_DELAYS_SEC) - 1)]
                await self._sleep_unless_stopping(pause)
            try:
                result = await asyncio.wait_for(message.download_media(file=str(target)), MEDIA_DOWNLOAD_TIMEOUT_SEC)
            except Exception as exc:
                timed_out = isinstance(exc, (asyncio.TimeoutError, TimeoutError))
                reason = (f"{MEDIA_DOWNLOAD_TIMEOUT_SEC:g} saniyede inmedi" if timed_out
                          else str(exc) or type(exc).__name__)
                log.error("Could not download image of %s/%s (attempt %d/%d): %s", message.chat_id, message.id,
                          attempt, MEDIA_DOWNLOAD_ATTEMPTS, str(exc) or type(exc).__name__)
                continue
            if result:
                if attempt > 1:
                    log.info("Image of %s/%s downloaded at attempt %d", message.chat_id, message.id, attempt)
                return str(result)
            reason = "Telegram boş yanıt verdi"
            log.error("Image of %s/%s could not be downloaded (empty result, attempt %d/%d)", message.chat_id,
                      message.id, attempt, MEDIA_DOWNLOAD_ATTEMPTS)
        self._download_failed[key] = f"{MEDIA_DOWNLOAD_ATTEMPTS} deneme; son hata: {reason}"
        return None

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
                if topic_id != GENERAL_TOPIC_ID:
                    self._topic_changes.append((chat_id, topic_id, title))
                    if not self._topic_listing:
                        self._spawn(self._announce_topics(chat_id))
            else:
                log.info("Forum %s topic %s %r", chat_id, topic_id, title)

    async def _announce_topics(self, chat_id: int, *, listing: bool = False) -> None:
        """Tell the owner, once per topic and title, about a new or renamed topic: whether it is
        followed, or that it matches no route and will not be copied (a misspelt signal topic must be
        seen on day one). What was told is kept in storage, so a restart does not repeat it; the very
        first listing of a forum is one notice for all its topics."""
        async with self._topic_notice_lock:
            changes = [c for c in self._topic_changes if c[0] == chat_id]
            self._topic_changes = [c for c in self._topic_changes if c[0] != chat_id]
            if not changes:
                return
            state_key = TOPICS_KEY.format(chat_id=chat_id)
            try:
                known = dict(await self._db.get_state(state_key) or {})
            except Exception as exc:
                log.warning("Could not read the known topics of %s: %s", chat_id, exc)
                return
            first_time = not known
            new: list[tuple[str, str | None]] = []           # (title, title before)
            for _, topic_id, title in changes:
                before = known.get(str(topic_id))
                if before != title:
                    new.append((title, before))
                    known[str(topic_id)] = title
            if not new:
                return
            try:
                await self._db.set_state(state_key, known)
            except Exception as exc:
                log.warning("Could not store the known topics of %s: %s", chat_id, exc)
        route_of = lambda title: match_topic(self._config.topics, title)   # noqa: E731
        if first_time and listing:
            followed = [f"'{esc(t)}' ({esc(route_of(t).name)})" for t, _ in new if route_of(t)]   # type: ignore[union-attr]
            ignored = [f"'{esc(t)}'" for t, _ in new if not route_of(t)]
            parts = []
            if followed:
                parts.append("Takip edilen konular: " + ", ".join(followed) + ".")
            if ignored:
                parts.append("Hiçbir rotaya uymayan konular (bu konulardaki mesajlar kopyalanmayacak): "
                             + ", ".join(ignored) + ". " + TOPIC_HINT_TR)
            self._notify_soon("\n".join(parts), Severity.INFO)
            return
        for title, before in new:
            route = route_of(title)
            was_followed = before is not None and route_of(before) is not None
            name = f"'{esc(title)}'"
            if route is not None:
                text = (f"Yeni konu {name} takip ediliyor (rota: {esc(route.name)})." if before is None else
                        f"'{esc(before)}' konusunun adı {name} olarak değişti; takip ediliyor (rota: {esc(route.name)}).")
                self._notify_soon(text, Severity.INFO)
            elif before is None:
                self._notify_soon(f"Yeni konu {name} hiçbir rotaya uymuyor; bu konudaki mesajlar kopyalanmayacak. "
                                  + TOPIC_HINT_TR)
            elif was_followed:
                self._notify_soon(f"Takip edilen '{esc(before)}' konusunun adı {name} olarak değişti ve artık hiçbir "
                                  "rotaya uymuyor; bu konudaki mesajlar kopyalanmayacak. " + TOPIC_HINT_TR)
            else:
                self._notify_soon(f"'{esc(before)}' konusunun adı {name} olarak değişti; hiçbir rotaya uymuyor, "
                                  "bu konudaki mesajlar kopyalanmayacak.", Severity.INFO)

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
            self._topic_listing = True
            try:
                async with self._topic_lock(chat_id):
                    await self._fetch_topics(chat_id, None)
            except Exception as exc:
                log.warning("Could not list forum topics of %s: %s (titles are fetched per topic later)", chat_id, exc)
                continue
            finally:
                self._topic_listing = False
            await self._announce_topics(chat_id, listing=True)
            titles = [t for (cid, _), t in self._topic_titles.items() if cid == chat_id and t]
            for route in self._config.topics:
                if not any(match_topic([route], t) for t in titles):
                    log.warning("No topic of forum %s matches telegram.topics '%s' %s yet; it is followed once "
                                "such a topic exists", chat_id, route.name, route.title_contains)
                    wanted = ", ".join(f"'{esc(t)}'" for t in route.title_contains)
                    self._notify_soon(
                        f"'{esc(route.name)}' rotasına uyan bir konu şu anda grupta yok (aranan: {wanted}). "
                        "Böyle bir konu açılınca kendiliğinden takip edilir. " + TOPIC_HINT_TR
                    )

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
            self._notify_soon(
                f"Kaynak grup ({chat_id}) konulara ayrılmış bir grup değil, ama config.yaml'da konular "
                "tanımlı: bu gruptan HİÇBİR mesaj kopyalanmayacak. Grup numarasını kontrol edin ya da "
                "config.yaml'daki konu tanımlarını kaldırın.",
                Severity.ERROR,
            )

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
    def _debounce_hold(self, key: DebounceKey, msg_id: int | None = None) -> None:
        """A message for ``key`` arrived: stop the timer until it is stored.

        ``msg_id`` joins the stream's next batch. The batch is defined here, at trigger time: its
        messages are pinned in storage until ``on_ready`` has returned, so however long the batch then
        waits (brokers still connecting at start, a slow broker call of the stream's previous batch),
        the engine's time window and its start-up sweep of old messages cannot drop them."""
        timer = self._timers.pop(key, None)
        if timer is not None:
            timer.cancel()
        self._inflight[key] = self._inflight.get(key, 0) + 1
        if msg_id is not None:
            self._batch.setdefault(key, set()).add(msg_id)
            self._pin(key[0], [msg_id])

    def _pin(self, chat_id: int, ids: Any) -> None:
        pin = getattr(self._db, "pin_messages", None)
        if pin is not None:
            pin(chat_id, list(ids))

    def _unpin(self, chat_id: int, ids: Any) -> None:
        unpin = getattr(self._db, "unpin_messages", None)
        if unpin is not None:
            unpin(chat_id, list(ids))

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
        await self._ready.wait()         # start-up: held until the engine is ready
        me = asyncio.current_task()
        if self._timers.get(key) is not me:
            return
        # Detach first: messages arriving while on_ready runs start a new timer
        # instead of cancelling a call that may be placing orders.
        del self._timers[key]
        chat_id, topic_id, sender_id = key
        batch = self._batch.pop(key, None)
        if batch is not None and not batch:
            log.info("Batch of chat %s topic %s sender %s: every message was deleted meanwhile; nothing passed on",
                     chat_id, topic_id, sender_id)
            return
        assert me is not None
        self._tasks.add(me)
        ids = batch or set()
        running = {(chat_id, i) for i in ids}
        self._in_engine |= running
        try:
            await self._on_ready(chat_id, sender_id, topic_id)
        except Exception as exc:
            log.exception("on_ready failed for chat %s topic %s sender %s", chat_id, topic_id, sender_id)
            self._notify_problem(
                f"on-ready:{chat_id}:{topic_id}",
                "Kaynak gruptan gelen mesajlar işlenirken beklenmeyen bir hata oluştu "
                f"(<code>{esc(str(exc) or type(exc).__name__)}</code>). Bu mesajlardan işlem yapılmamış "
                "olabilir; grubu ve açık işlemleri kontrol edin.",
                Severity.ERROR,
            )
        finally:
            self._in_engine -= running
            self._unpin(chat_id, ids)
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

    async def _catch_up(self, skipped: list[SkippedMessage] | None = None, *, quiet: bool = False) -> bool:
        """Fetch and handle the messages written since the last stored one (every source chat).

        Returns False when a chat could not be read (it is tried again at the next round). Messages
        that are too old to be processed are added to ``skipped`` when the caller reports them itself;
        otherwise the owner is told here, once for all of them. ``quiet``: the periodic round (a
        failure is logged once per outage, without a traceback)."""
        async with self._catchup_lock:
            own = skipped is None
            found: list[SkippedMessage] = [] if skipped is None else skipped
            complete = True
            for chat_id, entity in self._entities.items():
                try:
                    await self._catch_up_chat(chat_id, entity, found, quick=quiet)
                except Exception as exc:
                    complete = False
                    if not quiet:
                        log.exception("Catch-up failed for chat %s", chat_id)
                    elif not self._poll_failing:
                        log.warning("Periodic catch-up failed for chat %s: %s (tried again every %.0fs)",
                                    chat_id, str(exc) or type(exc).__name__, CATCHUP_POLL_SEC)
            if own:
                self._report_occasion(found, complete=complete)
            return complete

    async def _fetch_missed(self, entity: Any, last: int, limit: int = CATCHUP_FETCH_LIMIT) -> list[Any]:
        assert self._client is not None
        return [m async for m in self._client.iter_messages(entity, min_id=last, limit=limit)]

    async def _catch_up_chat(self, chat_id: int, entity: Any, skipped: list[SkippedMessage] | None = None,
                             *, quick: bool = False) -> None:
        """``quick`` (the periodic round): ask only for the newest POLL_FETCH_LIMIT messages; when all of
        them are new, more may have been missed and the full fetch is made."""
        assert self._client is not None
        stored = await self._db.get_state(f"last_msg_id:{chat_id}")
        if stored is None:
            # First run: only remember where we are, never execute history.
            latest = await asyncio.wait_for(self._client.get_messages(entity, limit=1), CATCHUP_REQUEST_TIMEOUT_SEC)
            if latest:
                self._note_last_id(chat_id, latest[0].id)
                await self._persist_last_id(chat_id)
            return
        last = int(stored)
        self._note_last_id(chat_id, last)
        # A request made while Telethon is reconnecting would wait in its queue: never wait for long.
        limit = POLL_FETCH_LIMIT if quick else CATCHUP_FETCH_LIMIT
        missed = await asyncio.wait_for(self._fetch_missed(entity, last, limit), CATCHUP_REQUEST_TIMEOUT_SEC)
        if quick and len(missed) >= limit:
            missed = await asyncio.wait_for(self._fetch_missed(entity, last), CATCHUP_REQUEST_TIMEOUT_SEC)
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
                await self.handle_message(msg, consumed=not young, skipped=None if young else skipped)
            else:
                # Its topic's title is unknown (Telegram error): wait for it in the background,
                # without holding up the rest of the catch-up (the age rule applies when the title
                # is known; a skipped one gets its own notice).
                self._spawn(self.handle_message(msg, live=True))
        log.info("Catch-up %s: %d missed message(s), %d recent enough to process", chat_id, len(missed), fresh)
        for msg in missed:
            self._offer_mirror(msg, catchup=True)       # (after their handling: the copy never comes first)

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

    async def _resume_unprocessed(self, skipped: list[SkippedMessage] | None = None) -> None:
        """Re-trigger recent stored messages whose debounce never fired (crash / restart).

        Older unprocessed ones (of followed topics) are closed here and added to ``skipped``, so the
        owner is told what they were instead of only how many (the engine's own start-up sweep then
        finds nothing left to report)."""
        now = datetime.now(timezone.utc)
        since = now - timedelta(seconds=self._config.catchup_max_age_sec)
        for chat_id in self._entities:
            try:
                msgs = await self._db.recent_messages(chat_id, since, only_unconsumed=True)
            except Exception:
                log.exception("Could not read unprocessed messages of %s", chat_id)
                continue
            batches: dict[DebounceKey, list[int]] = {}
            for m in msgs:
                if (not self._allowed or m.sender_id in self._allowed) and self._resume_followed(m):
                    batches.setdefault((chat_id, m.topic_id, m.sender_id), []).append(m.message_id)
            for dkey, ids in batches.items():
                self._debounce_hold(dkey)
                self._batch.setdefault(dkey, set()).update(ids)
                self._pin(chat_id, ids)
                self._debounce_release(dkey)
            if batches:
                log.info("Resuming %d unprocessed recent message(s) in %s", len(msgs), chat_id)
            if skipped is not None:
                await self._close_stale(chat_id, now, since, skipped)

    async def _close_stale(self, chat_id: int, now: datetime, since: datetime, skipped: list[SkippedMessage]) -> None:
        """Stored, unprocessed messages of followed topics that are too old now: context only."""
        consume = getattr(self._db, "mark_consumed", None)
        if consume is None:
            return
        try:
            rows = await self._db.recent_messages(chat_id, now - timedelta(days=STALE_SCAN_DAYS),
                                                  only_unconsumed=True, limit=200)
            stale = [m for m in rows if _as_utc(m.date) < since
                     and (not self._allowed or m.sender_id in self._allowed) and self._resume_followed(m)]
            if not stale:
                return
            await consume(chat_id, [m.message_id for m in stale])
        except Exception:
            log.exception("Could not close old unprocessed messages of %s", chat_id)
            return
        log.warning("Start: %d stored unprocessed message(s) of %s are too old; closed without action",
                    len(stale), chat_id)
        for m in stale:
            if m.text or m.media_type:
                skipped.append(SkippedMessage(chat_id, m.message_id, m.topic_id, m.topic_title, _as_utc(m.date),
                                              m.text, m.media_type is not None))

    # ---- "not processed because of its age" notices ----------------------------------
    def _report_occasion(self, skipped: list[SkippedMessage], *, complete: bool) -> None:
        """One notice for everything a catch-up skipped. After a restart / reconnect it says how long
        the bot was away; otherwise (messages that simply arrived late) it is throttled per topic."""
        outage = self._outage
        if complete:
            self._outage = None
        if not skipped:
            return
        if outage is None:
            for item in skipped:
                self._report_skipped_live(item)
            return
        occasion, since = outage
        now = datetime.now(timezone.utc)
        away = max(0.0, (now - since).total_seconds()) if since is not None else None
        self._notify_soon(skipped_notice_tr(list(skipped), occasion=occasion, away_sec=away,
                                            max_age_sec=self._config.catchup_max_age_sec, now=now))

    def _report_skipped_live(self, item: SkippedMessage) -> None:
        """A message that arrived too late while the bot was running: at most one notice per topic in
        SKIP_NOTICE_THROTTLE_SEC; later ones wait and are sent together when that time is over."""
        tkey = (item.chat_id, item.topic_id)
        self._skip_pending.setdefault(tkey, []).append(item)
        if tkey in self._skip_flush:
            return
        last = self._skip_notified_at.get(tkey)
        wait = 0.0 if last is None else last + SKIP_NOTICE_THROTTLE_SEC - time.monotonic()
        self._skip_flush[tkey] = self._spawn(self._flush_skipped(tkey, max(wait, SKIP_NOTICE_COLLECT_SEC)))

    async def _flush_skipped(self, tkey: tuple[int, int | None], delay: float) -> None:
        try:
            await self._sleep_unless_stopping(delay)       # returns at once when the listener stops
        finally:
            self._skip_flush.pop(tkey, None)
        items = self._skip_pending.pop(tkey, [])
        if not items:
            return
        self._skip_notified_at[tkey] = time.monotonic()
        await self._notify(skipped_notice_tr(items, occasion="late", away_sec=None,
                                             max_age_sec=self._config.catchup_max_age_sec,
                                             now=datetime.now(timezone.utc)))

    # ---- periodic catch-up / heartbeat ---------------------------------------------------
    async def _poll_loop(self) -> None:
        """The listener's own catch-up, every CATCHUP_POLL_SEC. Telethon asks Telegram for missed
        messages only when a later message shows a gap (or after 15 minutes of silence); this fetches
        a message that was missed during a short drop within seconds, whatever Telethon does."""
        while not self._stopping:
            await self._sleep_unless_stopping(CATCHUP_POLL_SEC)
            if self._stopping:
                break
            try:
                await self._poll_once()
            except Exception:
                log.exception("Periodic catch-up failed")

    async def _poll_once(self) -> None:
        await self._heartbeat()
        complete = await self._catch_up(quiet=True)
        if complete:
            if self._poll_failing:
                log.info("Periodic catch-up works again")
            self._poll_failing = False
            return
        self._poll_failing = True
        if self._outage is None:         # Telegram cannot be read: the connection is down or being repaired
            self._outage = ("reconnect", datetime.now(timezone.utc))

    async def _heartbeat(self) -> None:
        try:
            await self._db.set_state(ALIVE_KEY, datetime.now(timezone.utc).isoformat(timespec="seconds"))
        except Exception as exc:
            log.debug("Could not store the listener heartbeat: %s", exc)

    async def _read_alive(self) -> datetime | None:
        try:
            value = await self._db.get_state(ALIVE_KEY)
            return _as_utc(datetime.fromisoformat(value)) if isinstance(value, str) else None
        except Exception as exc:
            log.debug("Could not read the listener heartbeat: %s", exc)
            return None

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
