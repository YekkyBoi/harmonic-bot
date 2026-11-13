"""A copy of the whole source group for the owner's friend (``/grup``), sent to the friend's own two groups.

The owner switches it on with ``/grup ac`` (``/grup kapat``: off, ``/grup``: state, ``/grup konular <ad,ad>``: only
topics whose title contains one of these parts, ``/grup konular hepsi``: every topic again). The setting is kept in
the database (``group_mirror``) and is OFF until the owner turns it on; nothing is copied while no friend is set
(``/arkadas``).

Where the copies go: the friend made two groups and added the notifier bot to both. In each of them he (or the owner)
writes ``/bagla@<bot> ana`` or ``/bagla@<bot> sohbet``, which binds that group to that role (see
``BotNotifier._handle_bind``). A source topic whose title contains "sohbet" (case and Turkish letters ignored) goes to
the "sohbet" group; every other topic (General, the signal topics, PnL...) to the "ana" group. A role without a bound
group: the copy is skipped and counted (shown in ``/grup``); there is never a fallback to another chat. The friend's
private chat only gets the owner's notifications (as before), the on / off notices and the binding instructions.
The bindings belong to the friend: ``/arkadas sil`` and a new ``/arkadas`` clear them, the queue and the edit memory.

What a copy is: a header line (topic, author), then the text (plain, escaped). A photo or a document of at most
MAX_FILE_BYTES goes as a photo / file with that text as its caption. A medium that cannot be copied leaves the text
plus one generic line (the groups may have other members: the reason itself is only logged). An album is copied
picture by picture. An edit that changes the text is sent as a new message ("düzenledi", a reply to the earlier copy)
to the same group, while that group is still bound; an edit that arrives while the copy still waits only replaces its
text. Deletions are not copied. The highest message id copied per source chat is stored before the copy is sent, so a
restart never copies a message twice.

Never in the way of trading:

* The listener offers a message only after it has spawned its own handling of it; :meth:`GroupMirror.offer` never
  waits and never raises, and returns at once while the copy is off. The signal handling runs the same code on or off.
* Copies wait in a queue of their own (QUEUE_MAX; when it is full the oldest is dropped, with a log line). One worker
  task copies them one after the other, each after the listener's own handling of that message has finished.
* Media is moved only when Telethon encrypts natively (cryptg or libssl: pure-Python AES would stall the event loop for
  seconds per MB), and only while the trading path is idle: no source message being stored or edited, no batch
  waiting for or in the engine, no owner command or button press being handled, no order flow of the engine and no
  monitor sync running. That is checked before the download, between its chunks, right before an upload and between
  its parts; every pause has its own limit (PAUSE_MAX_SEC) and a pause that runs past it aborts the transfer (the text
  goes with a note), as does shutdown. A picture the listener already downloaded for a signal topic is uploaded from
  that file: it is never downloaded twice.
* Sending follows the notifier's rules (``BotNotifier.send_group_copy``): FloodWait waited for, Telegram's pace for a
  group, every wait bounded, a request without an answer never cancelled nor sent twice, and right before every
  single request a check that the copy is still on and the target is still the bound group (else nothing is sent).
  A failure is logged and told to the owner at most once an hour (a throttle of its own); a group the bot can no
  longer write to is marked broken (``/bagla`` again) and the owner is told.
"""

from __future__ import annotations

import asyncio
import logging
import mimetypes
import re
import shutil
import time
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NamedTuple

from telethon import utils
from telethon.tl import types

from ..config import fold_ascii, fold_tr
from .formatting import esc
from .listener import _display_name, duration_tr
from .notifier import TransferAborted

log = logging.getLogger(__name__)

STATE_KEY = "group_mirror"         # storage state: {"on", "topics", "targets": {role: {chat, title, by}}, "friend"}
LAST_KEY = "group_mirror_last"     # storage state: {source chat id: highest message id copied}
ROLES = ("ana", "sohbet")
SOHBET_PART = "sohbet"             # a topic whose title contains this goes to the "sohbet" group
QUEUE_MAX = 100                    # messages waiting to be copied (the oldest dropped first, with a log line)
MAX_FILE_BYTES = 5 * 1024 * 1024   # larger photos / files are not copied (the text goes with a note)
DOWNLOAD_TIMEOUT_SEC = 60.0        # one download through the userbot (one try), pauses included
HANDLED_WAIT_SEC = 120.0           # a copy waits at most this long for the listener's own handling of its message
LAST_WRITE_WAIT_SEC = 10.0         # the stored "last copied id" waits this long for an idle trading path
PAUSE_MAX_SEC = 60.0               # one pause of a transfer for the trading path; longer: the transfer is given up
IDLE_POLL_SEC = 0.2
AUTHOR_TIMEOUT_SEC = 5.0           # the author's name when Telegram did not send it with the message
MAX_AGE_SEC = 1800.0               # older when it reaches the bot (catch-up after a restart): not copied
LATE_MARK_SEC = 60.0               # a copy this much later than the message says when it was written
SEEN_LIMIT = 5000                  # messages remembered (no second copy; edits of copied messages)
TEMP_DIR = "group_copy"            # under data_dir: the downloads, deleted after the upload
STOP_WAIT_SEC = 5.0
_TOPIC_SPLIT = re.compile(r"[,;\n]+")
_BAD_NAME = re.compile(r'[\x00-\x1f<>:"/\\|?*]+')
_SAFE_EXT = re.compile(r"^\.[A-Za-z0-9]{1,8}$")

# media that is not a file (or not worth a file): named in a one-line note
_SKIPPED_MEDIA = {
    "MessageMediaPoll": "anket",
    "MessageMediaGeo": "konum",
    "MessageMediaGeoLive": "canlı konum",
    "MessageMediaVenue": "konum",
    "MessageMediaContact": "kişi kartı",
    "MessageMediaDice": "zar / emoji oyunu",
    "MessageMediaGame": "oyun",
    "MessageMediaInvoice": "ödeme",
    "MessageMediaStory": "hikâye",
    "MessageMediaGiveaway": "çekiliş",
    "MessageMediaGiveawayResults": "çekiliş sonucu",
    "MessageMediaPaidMedia": "ücretli içerik",
}
_NO_MEDIA = ("MessageMediaEmpty", "MessageMediaWebPage")   # a link preview: the link is in the text


def native_aes() -> bool:
    """Telethon encrypts with cryptg or libssl (not its pure-Python AES, which blocks the loop ~2.4 s per MB)."""
    try:
        from telethon.crypto import aes as tl_aes
        from telethon.crypto import libssl
    except Exception:      # noqa: BLE001
        return False
    if getattr(tl_aes, "cryptg", None) is not None:
        return True
    return callable(getattr(libssl, "encrypt_ige", None)) and callable(getattr(libssl, "decrypt_ige", None))


def role_of(topic_title: str | None) -> str:
    """'sohbet' for a topic whose title contains "sohbet" (any case, with or without Turkish letters), else 'ana'."""
    if topic_title and (SOHBET_PART in fold_tr(topic_title) or SOHBET_PART in fold_ascii(topic_title)):
        return "sohbet"
    return "ana"


class Media(NamedTuple):
    kind: str              # "photo", "document" or "skip" (only named in a note)
    label: str             # Turkish name: "Fotoğraf", "Video", ... ("skip": "çıkartma", "anket", ...)
    size: int | None       # bytes, when Telegram says
    name: str              # the original file name (sent as the document's name; never used on disk)
    ext: str               # a safe extension for the download's fixed name


@dataclass(eq=False)
class Target:
    """The friend's group bound to a role. A new object for every change (rebinding, migration): a copy on its way
    checks that its target is still THE bound one, by identity."""

    role: str
    chat: int
    title: str
    by: int | None
    broken: str | None = None       # the bot can no longer write there (why, for the log / owner); /bagla again
    since: float = field(default_factory=time.monotonic)

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"chat": self.chat, "title": self.title, "by": self.by}
        if self.broken:
            data["broken"] = self.broken
        return data


@dataclass(eq=False)
class _Item:
    message: Any
    handled: Any = None            # the listener's own handling of the message (a task), waited for first
    edit: bool = False
    folder: Path | None = None     # its download (removed once the copy is done or failed)
    offered: float = field(default_factory=time.monotonic)
    sent_to: Target | None = None  # the target it was really sent to (after a migration: the new one)
    finished: bool = False         # delivered, failed (counted) or given up on purpose

    @property
    def key(self) -> tuple[int, int]:
        return (self.message.chat_id, self.message.id)


@dataclass
class _Copied:
    text: str                      # the text the group has seen last
    msg_id: int                    # the copy in the group (edits reply to it)
    target: Target                 # where it went (an edit goes there only while it is still the bound group)


class _MediaFailed(Exception):
    """The medium of a message is not copied (``str(exc)``: why, for the log only)."""

    def __init__(self, why: str, *, too_large: bool = False, quiet: bool = False) -> None:
        super().__init__(why)
        self.too_large = too_large
        self.quiet = quiet             # (the reason was logged once already)


class NotLoaded(RuntimeError):
    """The stored setting could not be read: no change is made until it can (``ensure_loaded``)."""


def _photo_bytes(photo: Any) -> int | None:
    sizes = getattr(photo, "sizes", None) or []
    counts = []
    for size in sizes:
        try:
            counts.append(int(utils._photo_size_byte_count(size) or 0))
        except Exception:      # noqa: BLE001 - unknown size type: the download is limited all the same
            continue
    return max(counts) if counts and max(counts) > 0 else None


def _file_name(doc: Any, mime: str) -> str:
    name = ""
    for attr in getattr(doc, "attributes", None) or []:
        if isinstance(attr, types.DocumentAttributeFilename) and attr.file_name:
            name = attr.file_name
    name = _BAD_NAME.sub("_", Path(name.replace("\\", "/")).name).strip(" .")[:100]
    return name or "dosya" + (mimetypes.guess_extension(mime) or "")


def _safe_ext(name: str, mime: str) -> str:
    ext = Path(name).suffix.lower()
    if not _SAFE_EXT.match(ext):
        ext = (mimetypes.guess_extension(mime) or "").lower()
    return ext if _SAFE_EXT.match(ext) else ".bin"


def media_of(message: Any) -> Media | None:
    """What the message carries besides its text (None: nothing, or a link preview)."""
    media = getattr(message, "media", None)
    if media is not None and type(media).__name__ in _NO_MEDIA:
        return None          # (Telethon's .photo / .document would also return a link preview's picture)
    photo = getattr(message, "photo", None)
    if photo is not None:
        return Media("photo", "Fotoğraf", _photo_bytes(photo), "foto.jpg", ".jpg")
    doc = getattr(message, "document", None)
    if doc is not None:
        attrs = getattr(doc, "attributes", None) or []
        if any(isinstance(a, types.DocumentAttributeSticker) for a in attrs):
            return Media("skip", "çıkartma", None, "", "")
        mime = (getattr(doc, "mime_type", None) or "").lower()
        if mime.startswith("image/"):
            label = "Resim"
        elif any(isinstance(a, types.DocumentAttributeAnimated) for a in attrs):
            label = "GIF"
        elif any(isinstance(a, types.DocumentAttributeVideo) and getattr(a, "round_message", False) for a in attrs):
            label = "Görüntülü mesaj"
        elif mime.startswith("video/"):
            label = "Video"
        elif any(isinstance(a, types.DocumentAttributeAudio) and getattr(a, "voice", False) for a in attrs):
            label = "Ses kaydı"
        elif mime.startswith("audio/"):
            label = "Ses dosyası"
        else:
            label = "Dosya"
        size = getattr(doc, "size", None)
        name = _file_name(doc, mime)
        return Media("document", label, size if isinstance(size, int) else None, name, _safe_ext(name, mime))
    if media is None:
        return None
    return Media("skip", _SKIPPED_MEDIA.get(type(media).__name__, "ek"), None, "", "")


def _mb(size: float) -> str:
    return f"{size / 1024 / 1024:.1f}".rstrip("0").rstrip(".")


def parse_topics(text: str) -> list[str]:
    """'PnL, Sohbet' -> ['PnL', 'Sohbet'] (empty parts dropped, duplicates once)."""
    parts: list[str] = []
    for part in _TOPIC_SPLIT.split(text or ""):
        part = " ".join(part.split())
        if part and fold_tr(part) not in {fold_tr(p) for p in parts}:
            parts.append(part[:60])
    return parts[:20]


class GroupMirror:
    """See the module docstring. ``listener``: the TelegramListener (topic titles, the files it downloaded, whether its
    path is busy); ``notifier``: the BotNotifier (the friend, ``send_group_copy``); ``engine``: read only, to see
    whether an order flow or a monitor sync is running."""

    ROLES = ROLES
    role_of = staticmethod(role_of)

    def __init__(self, db: Any, notifier: Any, listener: Any, data_dir: str, engine: Any = None) -> None:
        self._db = db
        self._notifier = notifier
        self._listener = listener
        self._engine = engine
        self._dir = Path(data_dir) / TEMP_DIR
        self._loaded = False
        self._on = False
        self._topics: list[str] = []
        self._targets: dict[str, Target] = {}
        self._bound_friend: int | None = None              # the friend the bindings belong to
        self._last: dict[int, int] = {}                    # source chat -> highest message id copied (stored)
        self._boot_last: dict[int, int] = {}               # ... as stored when this process started
        self._queue: deque[_Item] = deque()
        self._queued: dict[tuple[int, int], _Item] = {}    # new messages still waiting (an edit replaces the text)
        self._offered: OrderedDict[tuple[int, int], None] = OrderedDict()    # never a second copy
        self._copied: OrderedDict[tuple[int, int], _Copied] = OrderedDict()
        self._worker: asyncio.Task[Any] | None = None
        self._current: _Item | None = None                 # the copy the worker is on
        self._stopping = False
        self._aes_logged = False
        self.dropped = 0               # since start: the queue was full, or shutdown
        self.failed = 0                # since start: a copy could not be sent
        self.skipped: dict[str, int] = dict.fromkeys(ROLES, 0)    # since start: no (working) group for that role

    # ---- state ------------------------------------------------------------------------------------
    @property
    def enabled(self) -> bool:
        return self._on

    @property
    def loaded(self) -> bool:
        return self._loaded

    @property
    def topics(self) -> list[str]:
        return list(self._topics)

    @property
    def targets(self) -> dict[str, Target]:
        return dict(self._targets)

    @property
    def waiting(self) -> int:
        return len(self._queue)

    @property
    def media_possible(self) -> bool:
        return native_aes()

    @property
    def active(self) -> bool:
        """Copies are made now: the setting was read, switched on, a friend is set, not stopping."""
        return (self._loaded and self._on and not self._stopping
                and getattr(self._notifier, "friend_chat_id", None) is not None)

    def _bindings_current(self) -> bool:
        """The bindings belong to the friend the notifier has NOW. (The engine hands a new friend to the notifier and
        only then finishes /arkadas, which drops the old bindings: in between, they are not used.)"""
        return self._bound_friend == getattr(self._notifier, "friend_chat_id", None)

    def _source_chats(self) -> set[int]:
        return set(getattr(self._listener, "chat_ids", None) or ())

    def is_source(self, chat: int) -> bool:
        """A chat the listener reads: never a target (the copies would loop back into the source group)."""
        return chat in self._source_chats()

    def unbound_roles(self) -> list[str]:
        """Roles without a working group (unbound, or the bot can no longer write there)."""
        return [r for r in ROLES if r not in self._targets or self._targets[r].broken]

    async def load(self) -> bool:
        """Read the owner's setting (start). A failure leaves the copy off and refuses changes until a later
        :meth:`ensure_loaded` succeeds. Bindings made for another friend than the current one (changed by an older
        version of the bot, for instance) are dropped. Removes downloads left by a crash."""
        shutil.rmtree(self._dir, ignore_errors=True)
        try:
            raw = await self._db.get_state(STATE_KEY)
        except Exception:      # noqa: BLE001
            log.exception("Group copy: could not read its setting; it stays off until it can be read")
            self._loaded = False
            return False
        self._apply(raw)
        try:
            last = await self._db.get_state(LAST_KEY)
        except Exception:      # noqa: BLE001
            log.exception("Group copy: could not read the last copied messages (a restart may copy some again)")
            last = None
        if isinstance(last, dict):
            for chat, msg_id in last.items():
                try:
                    self._last[int(chat)] = max(self._last.get(int(chat), 0), int(msg_id))
                except (TypeError, ValueError):
                    continue
        self._boot_last = dict(self._last)
        self._loaded = True
        friend = getattr(self._notifier, "friend_chat_id", None)
        if self._targets and self._bound_friend != friend:
            log.warning("Group copy: the bound groups belonged to another friend (%s); they are dropped",
                        self._bound_friend)
            self._targets.clear()
            self._bound_friend = friend
            try:
                await self._store()
            except Exception:      # noqa: BLE001
                log.exception("Group copy: could not store the dropped bindings")
        if self._on:
            log.info("Group copy for the owner's friend is on (%s; groups: %s)", ", ".join(self._topics) or "every topic",
                     ", ".join(f"{r}={t.chat}" for r, t in self._targets.items()) or "none bound")
        return True

    async def ensure_loaded(self) -> bool:
        return self._loaded or await self.load()

    def _apply(self, raw: Any) -> None:
        self._on, self._topics, self._targets, self._bound_friend = False, [], {}, None
        if not isinstance(raw, dict):
            return
        self._on = raw.get("on") is True
        topics = raw.get("topics")
        if isinstance(topics, list):
            self._topics = [t.strip() for t in topics if isinstance(t, str) and t.strip()]
        friend = raw.get("friend")
        self._bound_friend = friend if isinstance(friend, int) and not isinstance(friend, bool) else None
        targets = raw.get("targets")
        if isinstance(targets, dict):
            for role in ROLES:
                data = targets.get(role)
                chat = data.get("chat") if isinstance(data, dict) else None
                if isinstance(chat, int) and not isinstance(chat, bool) and chat < 0:
                    by = data.get("by")
                    self._targets[role] = Target(role, chat, str(data.get("title") or "grup")[:128],
                                                 by if isinstance(by, int) and not isinstance(by, bool) else None,
                                                 str(data["broken"])[:200] if data.get("broken") else None)

    async def _store(self) -> None:
        """Write the whole setting (raises: the caller says it was not stored)."""
        await self._db.set_state(STATE_KEY, {
            "on": self._on, "topics": list(self._topics),
            "targets": {r: t.to_dict() for r, t in self._targets.items()}, "friend": self._bound_friend,
        })

    def _need_loaded(self) -> None:
        if not self._loaded:
            raise NotLoaded("the group copy setting could not be read")

    async def set_enabled(self, on: bool) -> None:
        """Store and apply ``/grup ac`` / ``/grup kapat``. Raises when it cannot be stored (nothing changed)."""
        self._need_loaded()
        old = self._on
        self._on = on
        try:
            await self._store()
        except BaseException:
            self._on = old
            raise
        if not on:
            self._drop_waiting("the owner switched the group copy off", count=False)
        log.info("Group copy for the owner's friend switched %s", "on" if on else "off")

    @staticmethod
    def parse_topics(text: str) -> list[str]:
        return parse_topics(text)

    async def set_topics(self, topics: list[str]) -> None:
        """Store and apply ``/grup konular`` (empty: every topic). Raises when it cannot be stored."""
        self._need_loaded()
        old = self._topics
        self._topics = list(topics)
        try:
            await self._store()
        except BaseException:
            self._topics = old
            raise
        log.info("Group copy topics: %s", ", ".join(self._topics) or "every topic")

    async def bind(self, role: str, chat: int, title: str, by: int | None) -> Target:
        """``/bagla``: this group gets the copies of ``role`` from now on (a copy of the role that waited or was on its
        way goes nowhere; edits of earlier copies are no longer sent). Raises when it cannot be stored."""
        self._need_loaded()
        if role not in ROLES:
            raise ValueError(role)
        if self.is_source(int(chat)):
            raise ValueError("a source chat cannot be a target of the group copy")
        old_targets, old_friend = dict(self._targets), self._bound_friend
        target = Target(role, int(chat), (title or "grup")[:128], by)
        self._targets[role] = target
        self._bound_friend = getattr(self._notifier, "friend_chat_id", None)
        try:
            await self._store()
        except BaseException:
            self._targets, self._bound_friend = old_targets, old_friend
            raise
        self._forget_copies(role)
        log.info("Group copy: role %s bound to chat %s (%r) by %s", role, chat, title, by)
        return target

    async def friend_changed(self, new_friend: int | None) -> bool:
        """``/arkadas sil`` / a new ``/arkadas``: the bindings, the waiting copies and the edit memory belonged to the
        earlier friend and are dropped. True when there was something to drop. Never raises (a failed write is
        logged; the bindings are dropped in memory and again at the next start, see :meth:`load`)."""
        had = bool(self._targets) or bool(self._queue) or bool(self._copied)
        self._targets.clear()
        self._bound_friend = new_friend
        self._drop_waiting("the owner changed his friend", count=False)
        self._copied.clear()
        if self._loaded:
            try:
                await self._store()
            except Exception:      # noqa: BLE001
                log.exception("Group copy: could not store the dropped bindings")
        return had

    def _forget_copies(self, role: str) -> None:
        for key in [k for k, c in self._copied.items() if c.target.role == role]:
            del self._copied[key]

    async def _replace_target(self, old: Target, chat: int) -> Target | None:
        """The group was upgraded to a supergroup (new id): the binding follows it."""
        if self._targets.get(old.role) is not old:
            return None
        new = Target(old.role, chat, old.title, old.by, since=old.since)    # (the same group: what waits goes there)
        self._targets[old.role] = new
        for copied in self._copied.values():
            if copied.target is old:
                copied.target = new
        try:
            await self._store()
        except Exception:      # noqa: BLE001
            log.exception("Group copy: could not store the new id of a migrated group")
        log.warning("Group copy: the %s group %s became the supergroup %s; binding updated", old.role, old.chat, chat)
        return new

    async def _mark_broken(self, target: Target, exc: BaseException) -> None:
        if self._targets.get(target.role) is not target or target.broken:
            return
        target.broken = str(exc) or type(exc).__name__
        self._forget_copies(target.role)
        try:
            await self._store()
        except Exception:      # noqa: BLE001
            log.exception("Group copy: could not store the broken binding")
        log.error("Group copy: the bot can no longer write to the %s group %s (%s); it must be bound again",
                  target.role, target.chat, target.broken)
        report = getattr(self._notifier, "group_target_broken", None)
        if callable(report):
            try:
                await report(target, exc)
            except Exception:      # noqa: BLE001
                log.exception("Group copy: could not tell the owner about the broken group")

    def _wanted(self, title: str | None) -> bool:
        if not self._topics:
            return True
        if not title:
            return False
        key, plain = fold_tr(title), fold_ascii(title)
        return any(fold_tr(p) in key or fold_ascii(p) in plain for p in self._topics)

    def routes(self) -> list[tuple[str, str]]:
        """(topic title, role) for every topic the listener knows (its cache), as copies would be routed now."""
        known = getattr(self._listener, "mirror_known_topics", None)
        titles = known() if callable(known) else []
        return [(t, role_of(t)) for t in titles if self._wanted(t)]

    # ---- intake (called by the listener; never waits, never raises) ---------------------------------
    def offer(self, message: Any, *, handled: Any = None, edited: bool = False, catchup: bool = False) -> None:
        """A message of a source chat (a new one, an edit, or one found by the catch-up). Queued when the copy is on;
        ``handled``: the listener's own handling of it, which the copy waits for."""
        try:
            if not self.active or isinstance(message, types.MessageService):
                return
            chat_id, msg_id = getattr(message, "chat_id", None), getattr(message, "id", None)
            if chat_id is None or msg_id is None:
                return
            bot_id = getattr(self._notifier, "bot_user_id", None)
            if bot_id is not None and getattr(message, "sender_id", None) == bot_id:
                return                                 # the bot's own post: never copied (no echo, ever)
            key = (chat_id, msg_id)
            if edited:
                waiting = self._queued.get(key)
                if waiting is not None:
                    waiting.message = message          # not copied yet: the copy shows the newest text
                elif key in self._offered:             # (copied, or being copied: checked when its turn comes)
                    self._put(_Item(message, edit=True))
                return
            if key in self._offered or msg_id <= self._boot_last.get(chat_id, 0):
                return                                 # (copied, or skipped, already - in this or an earlier run)
            date = getattr(message, "date", None)
            if isinstance(date, datetime):
                age = (datetime.now(timezone.utc) - _utc(date)).total_seconds()
                if age > MAX_AGE_SEC:
                    log.debug("Group copy: %s/%s is %.0fs old; not copied", chat_id, msg_id, age)
                    return
            self._remember(self._offered, key, None)
            item = _Item(message, handled=handled)
            self._queued[key] = item
            self._put(item)
        except Exception:      # noqa: BLE001 - the listener's handling must never see this
            log.exception("Group copy: could not take a message")

    def _put(self, item: _Item) -> None:
        self._queue.append(item)
        while len(self._queue) > QUEUE_MAX:
            old = self._queue.popleft()
            if not old.edit:
                self._queued.pop(old.key, None)
            self.dropped += 1
            log.warning("Group copy queue full (%d): dropped the oldest message %s/%s", QUEUE_MAX, *old.key)
        if self._worker is None or self._worker.done():
            self._worker = asyncio.ensure_future(self._run())

    def _drop_waiting(self, why: str, *, count: bool) -> None:
        if self._queue:
            log.info("Group copy: %d waiting message(s) dropped (%s)", len(self._queue), why)
            if count:
                self.dropped += len(self._queue)
        self._queue.clear()
        self._queued.clear()

    async def stop(self) -> None:
        """Shutdown: nothing new is taken, what waits is dropped (logged, counted), the worker ends, and a transfer on
        its way aborts at its next chunk (its progress callback raises once stopping)."""
        self._stopping = True
        self._drop_waiting("shutdown", count=True)
        current = self._current
        if current is not None and not current.finished and not current.edit:
            self.dropped += 1          # (marked as copied before its send: it is never copied after the restart)
            log.warning("Group copy: the copy of %s/%s on its way is given up at shutdown (if Telegram already had "
                        "its request, it may still arrive)", *current.key)
        worker, self._worker = self._worker, None
        if worker is not None and not worker.done():
            worker.cancel()
            await asyncio.wait({worker}, timeout=STOP_WAIT_SEC)
        shutil.rmtree(self._dir, ignore_errors=True)

    # ---- the worker ----------------------------------------------------------------------------
    async def _run(self) -> None:
        while self._queue and not self._stopping:
            item = self._queue.popleft()
            self._current = item
            try:
                await self._copy(item)
                item.finished = True
            except asyncio.CancelledError:
                raise
            except Exception:      # noqa: BLE001 - one copy never stops the others
                item.finished = True
                log.exception("Group copy of %s/%s failed unexpectedly", *item.key)
            finally:
                if self._current is item:
                    self._current = None
                self._taken(item)
                if item.folder is not None:
                    shutil.rmtree(item.folder, ignore_errors=True)

    def _taken(self, item: _Item) -> None:
        """From now on an edit of this message is a copy of its own (until now it only replaced the text)."""
        if not item.edit and self._queued.get(item.key) is item:
            del self._queued[item.key]

    def _valid(self, target: Target) -> Callable[[], bool]:
        """Checked by the notifier right before every request of a copy: still on, still the bound group."""
        return lambda: (self.active and self._bindings_current() and self._targets.get(target.role) is target
                        and not target.broken and not self.is_source(target.chat))

    async def _copy(self, item: _Item) -> None:
        """Copy one message (its download, if any, is left in ``item.folder`` for the caller to remove)."""
        handled = item.handled
        if handled is not None and not handled.done():
            await asyncio.wait({handled}, timeout=HANDLED_WAIT_SEC)     # (never cancels it)
        self._taken(item)
        if not self.active:
            return
        message = item.message
        title, shown = self._topic(message)
        if not self._wanted(title):
            log.debug("Group copy: %s/%s skipped (topic %r is not in /grup konular)", *item.key, title)
            return
        if item.edit:
            await self._copy_edit(item, shown)
            return
        role = role_of(title)
        target = self._targets.get(role) if self._bindings_current() else None
        if target is None or target.broken or self.is_source(target.chat):
            self.skipped[role] += 1
            log.info("Group copy: %s/%s (topic %r) not copied: no working %s group is bound", *item.key, title, role)
            return
        if target.since > item.offered:
            log.info("Group copy: %s/%s not copied: its %s group was bound after it arrived", *item.key, role)
            return
        await self._mark_copied(*item.key)          # before sending: a crash never leads to a second copy
        author = await self._author(message)
        text = getattr(message, "message", None) or ""
        header = f"💬 <b>{esc(shown)}</b> · {esc(author)}"
        date = getattr(message, "date", None)
        if isinstance(date, datetime):
            age = (datetime.now(timezone.utc) - _utc(date)).total_seconds()
            if age >= LATE_MARK_SEC:
                header += f" · <i>{duration_tr(age)} önce yazıldı</i>"
        body = esc(text) if text.strip() else ""
        media = media_of(message)
        file: str | None = None
        note: str | None = None
        if media is not None and media.kind == "skip":
            note = f"📎 Bu mesajdaki {media.label} kopyalanmadı."
        elif media is not None:
            try:
                file, item.folder = await self._fetch(message, media)
            except (_MediaFailed, TransferAborted, OSError) as exc:
                log.log(logging.DEBUG if getattr(exc, "quiet", False) else logging.WARNING,
                        "Group copy: the %s of %s/%s is not copied: %s", media.label, *item.key, exc)
                big = isinstance(exc, _MediaFailed) and exc.too_large
                note = (f"📎 {media.label} kopyalanmadı (dosya çok büyük; en çok {_mb(MAX_FILE_BYTES)} MB)." if big
                        else f"📎 {media.label} kopyalanmadı.")
        msg_id = await self._send(item, target, header, body, file=file, media=media, note=note)
        if msg_id is not None:
            self._remember(self._copied, item.key, _Copied(text, msg_id, item.sent_to or target))

    async def _send(self, item: _Item, target: Target, header: str, body: str, *, file: str | None = None,
                    media: Media | None = None, note: str | None = None, reply_to: int | None = None) -> int | None:
        """Send one copy to ``target``; a group upgraded to a supergroup is followed (once); a group the bot can no
        longer write to is marked broken; any other failure is counted and told to the owner (throttled)."""
        n = self._notifier
        for attempt in (1, 2):
            try:
                msg_id = await n.send_group_copy(
                    target.chat, header, body, file=file, document=media is not None and media.kind == "document",
                    note=note, label=media.label if media is not None else "Dosya",
                    name=media.name if media is not None and media.kind == "document" else None, reply_to=reply_to,
                    progress=self._pauser if file is not None else None,
                    ready=self._ready_for_media if file is not None else None, valid=self._valid(target),
                )
                if msg_id is not None:
                    item.sent_to = target
                return msg_id
            except asyncio.CancelledError:
                raise
            except Exception as exc:      # noqa: BLE001
                gone = getattr(n, "group_chat_gone", None)
                if callable(gone) and gone(exc):
                    migrated = getattr(n, "group_migrated_to", None)
                    new_chat = await migrated(target.chat, exc) if attempt == 1 and callable(migrated) else None
                    if new_chat is not None and new_chat != target.chat:
                        new = await self._replace_target(target, new_chat)
                        if new is not None:
                            target, reply_to = new, None
                            continue                      # the same copy once more, to the supergroup
                    self.failed += 1
                    await self._mark_broken(target, exc)
                    return None
                self.failed += 1
                log.error("Group copy of %s/%s to the %s group failed: %s", *item.key, target.role,
                          str(exc) or type(exc).__name__)
                report = getattr(n, "group_copy_failed", None)
                if callable(report):
                    try:
                        await report(exc, target)
                    except Exception:     # noqa: BLE001
                        log.exception("Group copy: could not tell the owner")
                return None
        return None

    async def _copy_edit(self, item: _Item, shown: str) -> None:
        copied = self._copied.get(item.key)
        if (copied is None or not self._bindings_current() or self._targets.get(copied.target.role) is not copied.target
                or copied.target.broken):
            return                   # never copied, or its group changed since
        text = getattr(item.message, "message", None) or ""
        if text == copied.text:
            return                   # (a reaction, a new picture with the same text...)
        author = await self._author(item.message)
        header = f"✏️ <b>{esc(shown)}</b> · {esc(author)} (düzenledi)"
        body = esc(text) if text.strip() else "<i>(metin silindi)</i>"
        msg_id = await self._send(item, copied.target, header, body, reply_to=copied.msg_id)
        if msg_id is not None:
            copied.text = text

    async def _mark_copied(self, chat_id: int, msg_id: int) -> None:
        """Store the copy's id BEFORE it is sent (a restart or crash never copies it twice). The write waits for the
        trading path to be idle, at most LAST_WRITE_WAIT_SEC (the copy waits with it); only a busy spell longer than
        that gets one small write while busy."""
        if msg_id <= self._last.get(chat_id, 0):
            return
        self._last[chat_id] = msg_id
        deadline = time.monotonic() + LAST_WRITE_WAIT_SEC
        while self._busy() and not self._stopping and time.monotonic() < deadline:
            await asyncio.sleep(IDLE_POLL_SEC)
        try:
            await self._db.set_state(LAST_KEY, {str(c): m for c, m in self._last.items()})
        except Exception:      # noqa: BLE001
            log.exception("Group copy: could not store the last copied message (a restart may copy it again)")

    # ---- what the copy shows ----------------------------------------------------------------------
    def _topic(self, message: Any) -> tuple[str | None, str]:
        """(topic title for routing and /grup konular, name shown in the header)."""
        name = getattr(self._listener, "mirror_topic", None)
        chat_title, topic, chats = name(message) if callable(name) else (None, None, 1)
        title = topic if topic is not None else chat_title
        shown = title or "Kaynak grup"
        if chats > 1 and topic is not None and chat_title:
            shown = f"{chat_title} › {topic}"
        return title, shown

    @staticmethod
    async def _author(message: Any) -> str:
        sender = getattr(message, "sender", None)
        if sender is None and callable(getattr(message, "get_sender", None)):
            try:
                sender = await asyncio.wait_for(message.get_sender(), AUTHOR_TIMEOUT_SEC)
            except Exception:      # noqa: BLE001 - the name is only shown
                sender = None
        return _display_name(sender) or getattr(message, "post_author", None) or "bilinmeyen"

    # ---- media -------------------------------------------------------------------------------------
    def _busy(self) -> bool:
        """The trading path is working: the listener stores / edits a source message or a batch waits for or is in the
        engine; the notifier handles an owner command or button press; the engine runs an order flow or a monitor
        sync (read-only looks, never a lock)."""
        for part in (self._listener, self._notifier):
            check = getattr(part, "trading_busy", None)
            if callable(check):
                try:
                    if check():
                        return True
                except Exception:      # noqa: BLE001
                    return True
        engine = self._engine
        if engine is not None:
            try:
                if any(not t.done() for t in list(getattr(engine, "_critical", None) or ())):
                    return True
                running = getattr(getattr(engine, "monitor", None), "_running", None) or {}
                if any(not t.done() for t in list(running.values())):
                    return True
            except Exception:      # noqa: BLE001
                return True
        return False

    def _stopped(self) -> bool:
        return self._stopping or bool(getattr(self._notifier, "_shut", False))

    async def _pause(self) -> None:
        """One pause of a transfer while the trading path is busy, at most PAUSE_MAX_SEC (its own limit). Raises
        TransferAborted when stopping or when the pause runs past its limit."""
        deadline = time.monotonic() + PAUSE_MAX_SEC
        while True:
            if self._stopped():
                raise TransferAborted("the bot is stopping")
            if not self._busy():
                return
            if time.monotonic() >= deadline:
                raise TransferAborted(f"the trading path stayed busy for {PAUSE_MAX_SEC:g}s")
            await asyncio.sleep(IDLE_POLL_SEC)

    async def _ready_for_media(self) -> bool:
        """Right before an upload attempt: the trading path is idle (after one pause at most)."""
        try:
            await self._pause()
        except TransferAborted as exc:
            log.info("Group copy: upload not started: %s", exc)
            return False
        return True

    def _pauser(self, limit: int | None = None) -> Callable[..., Awaitable[None]]:
        """A fresh progress callback for ONE transfer attempt: between its chunks it pauses while the trading path is
        busy (each pause limited on its own), aborts on shutdown, and stops a download above ``limit`` bytes."""

        async def progress(done: float, total: float | None = None) -> None:
            if limit is not None and done > limit:
                raise _MediaFailed(f"larger than {_mb(limit)} MB", too_large=True)
            await self._pause()

        return progress

    async def _fetch(self, message: Any, media: Media) -> tuple[str, Path | None]:
        """The file of a message: the listener's own download of it (signal topics), else a download through the
        userbot to a fixed safe name (bounded in time and size, only while the trading path is idle and only with a
        native AES). (path, folder to remove)."""
        if not native_aes():
            if not self._aes_logged:
                self._aes_logged = True
                log.warning("Group copy: Telethon has no native AES (cryptg is not installed and libssl was not found): "
                            "no photos or files are copied, only their text (pip install cryptg)")
            raise _MediaFailed("no native AES in Telethon", quiet=True)
        if media.size is not None and media.size > MAX_FILE_BYTES:
            raise _MediaFailed(f"{_mb(media.size)} MB", too_large=True)
        await self._pause()
        stored = getattr(self._listener, "mirror_stored_media", None)
        if callable(stored):
            path = await stored(message.chat_id, message.id)
            try:
                size = Path(path).stat().st_size if path else 0
            except OSError:
                path, size = None, 0
            if path:
                if size > MAX_FILE_BYTES:
                    raise _MediaFailed(f"{_mb(size)} MB", too_large=True)
                return path, None
        folder = self._dir / f"{abs(message.chat_id)}_{message.id}"
        shutil.rmtree(folder, ignore_errors=True)
        folder.mkdir(parents=True, exist_ok=True)
        target = folder / f"media{media.ext}"           # a fixed safe name (never a name from the group)
        try:
            await self._pause()                          # (right before the first chunk is asked for)
            result = await asyncio.wait_for(
                message.download_media(file=str(target), progress_callback=self._pauser(MAX_FILE_BYTES)),
                DOWNLOAD_TIMEOUT_SEC)
        except (_MediaFailed, TransferAborted):
            shutil.rmtree(folder, ignore_errors=True)
            raise
        except (asyncio.TimeoutError, TimeoutError) as exc:
            shutil.rmtree(folder, ignore_errors=True)
            raise _MediaFailed(f"no download within {DOWNLOAD_TIMEOUT_SEC:g}s") from exc
        except Exception as exc:      # noqa: BLE001
            shutil.rmtree(folder, ignore_errors=True)
            raise _MediaFailed(f"download failed: {str(exc) or type(exc).__name__}") from exc
        if not result or not Path(str(result)).is_file():
            shutil.rmtree(folder, ignore_errors=True)
            raise _MediaFailed("Telegram returned no file")
        if Path(str(result)).stat().st_size > MAX_FILE_BYTES:
            shutil.rmtree(folder, ignore_errors=True)
            raise _MediaFailed(f"larger than {_mb(MAX_FILE_BYTES)} MB", too_large=True)
        return str(result), folder

    @staticmethod
    def _remember(cache: OrderedDict[Any, Any], key: Any, value: Any) -> None:
        cache[key] = value
        cache.move_to_end(key)
        while len(cache) > SEEN_LIMIT:
            cache.popitem(last=False)


def _utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
