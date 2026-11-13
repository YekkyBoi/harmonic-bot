"""Owner notification bot (Telethon client logged in as a BOT).

Talks to ``notifier.owner_user_id``; everybody else is ignored - except the owner's friend (the owner of the
optional tickmill account), when the owner has set his chat with ``/arkadas <id>``: every message the owner gets is
mirrored to the friend as read-only text (a message with buttons goes without them); when the owner chose
``/sonradan sor`` the friend answers the tickmill catch-up question ("... hesabında da açılsın mı?"), in the default
``/sonradan otomatik`` no question is asked (the bot opens it when the conditions hold, and both are told). Delivery
to the friend never delays, blocks or fails anything for the owner (a queue and a task of its own; see "the owner's
friend" below). The friend's commands and messages are never executed, and his button presses count for catch-up
questions only.
Texts are Telegram HTML. Text passed to :meth:`BotNotifier.notify` and text
returned by the engine is treated as HTML that our own code produced (use
``formatting.esc`` for anything untrusted inside it).

Delivery rules (Telegram trouble must never stop the engine, and nothing may get lost silently):

* No call into this class waits for Telegram longer than ``SEND_TIMEOUT_SEC``, and a message (notice, question,
  outcome of a question) keeps its caller waiting at most ``CALLER_WAIT_SEC``: the engine awaits them while it
  holds its locks, so a silent Telegram link must not hold up the next signal. A message that got no answer by
  then stays in the queue, in its place, with its request still running; the connection is taken for down only
  when ``SEND_TIMEOUT_SEC`` pass without an answer.
* A message that cannot be sent now is kept in a bounded in-memory queue and a background task
  delivers the queue in order, with back-off. While something is waiting, or the connection is known
  to be down, callers do not wait at all: the message is queued and the call returns.
* A send that got no answer in time is NOT cancelled: the request already sits in Telethon's own
  queue and Telethon sends it itself as soon as the connection is back. Cancelling it and sending the
  text again would deliver it twice, so the queue waits for that request instead (and only gives it
  up when the connection demonstrably works again and the answer still does not come).
* Where a request IS given up (an edit or a button reply that timed out, a request that stays
  unanswered although the connection works again), it is also taken out of Telethon's hands, or
  Telethon would send it after its next reconnect - an old edit or a second copy, maybe hours later.
* The client reconnects by itself for ever (``connection_retries=None``); a supervisor task covers
  the cases in which it ends up disconnected anyway and finds out when a silent connection works again.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import re
import struct
import time
from collections import OrderedDict, deque
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from telethon import Button, TelegramClient, errors, events, utils
from telethon.extensions import html as tl_html

from .. import __version__
from ..config import NotifierConfig, TelegramConfig
from ..interfaces import NO_CORRECTION_FRIEND_TEXT, SHUTDOWN_TEXT, STARTING_TEXT, EngineControl
from ..models import ConfirmationStatus, PendingConfirmation, Severity
from .formatting import (
    CATCH_UP_NO,
    CATCH_UP_YES,
    CORRECTION_HINT,
    UNDELIVERED_QUESTION,
    esc,
    format_bot_status,
    format_confirmation,
    format_delivery_report,
    late_notice_prefix,
    late_question_prefix,
)

log = logging.getLogger(__name__)

MAX_CHUNK = 4000                  # Telegram limit is 4096 chars after entity parsing
CLOSEALL_TTL_SEC = 120.0          # a "close all" yes-button older than this is refused
STOP_GRACE_SEC = 30.0
_CACHE_SIZE = 500

SEND_TIMEOUT_SEC = 20.0           # nobody waits longer than this for an answer from Telegram
# A message keeps its caller (the engine, a broker, a handler) waiting at most this long for Telegram (never longer
# than SEND_TIMEOUT_SEC); then it is left to the queue with its request still running (see the module docstring).
CALLER_WAIT_SEC = 2.5
QUEUE_MAX = 200                   # messages kept while they cannot be sent (least important, oldest dropped first)
RETRY_BASE_SEC = 2.0              # back-off of the delivery task after a failed attempt
RETRY_MAX_SEC = 60.0
MAX_UNKNOWN_FAILURES = 5          # an error of an unknown kind is retried this often, then the message is dropped
UNREACHABLE_RETRY_SEC = 600.0     # owner blocked the bot / never pressed Start: next try (or when they write)
FLOOD_MARGIN_SEC = 1.0            # added to the wait Telegram asks for (FloodWait) before the next try
STUCK_SEND_SEC = 20.0             # connection works again but an old request is still unanswered: send it again
FLUSH_WAIT_SEC = 5.0              # an owner message waits this long for the queue before it is answered
LATE_MARK_SEC = 60.0              # a message delivered this late says when it was really produced
REPORT_MIN_OUTAGE_SEC = 60.0      # a shorter connection loss that delayed nothing is not reported
REPORT_MIN_DELAY_SEC = 5.0        # a connection loss shorter than this is not worth a report at all
QUESTION_MIN_LEFT_SEC = 15.0      # a question with less time left than this is not asked any more
SUPERVISE_EVERY_SEC = 5.0         # how often the supervisor looks at the connection
RECONNECT_BASE_SEC = 2.0          # back-off between reconnect attempts of the supervisor
RECONNECT_MAX_SEC = 60.0
RECONNECT_TRY_SEC = 60.0          # one reconnect attempt (connect + login check)
FORCE_RECONNECT_AFTER_SEC = 300.0  # still no answer after this long: drop the connection and build a new one
START_TIMEOUT_SEC = 90.0          # first connection + login at start (then start() fails instead of hanging)
DISCONNECT_TIMEOUT_SEC = 10.0
STOP_FLUSH_MIN_SEC = 5.0          # the queue gets at least this long at shutdown
IDLE_RECHECK_SEC = 30.0           # the delivery task looks again this often while it waits for the connection
BOT_RETRY_DELAY_SEC = 5           # Telethon's pause between its own reconnect attempts (one log line each)

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
    "/arkadas &lt;sohbet_id&gt; — tickmill hesabının sahibine bildirimlerin kopyası (/arkadas: göster, "
    "/arkadas sil: kaldır)\n"
    "/sonradan otomatik | sor — bağlantı yokken atlanan tickmill işlemi: şartlar uygunsa sormadan aç ya da sor "
    "(/sonradan: göster)\n"
    "/yardim — bu mesaj\n\n"
    "Onay mesajlarında: ✅ Onayla · ✏️ Düzelt (doğrusunu yazarsınız) · ❌ İptal\n"
    "Bir onay mesajını yanıtlayarak da düzeltme yazabilirsiniz."
)
PLAIN_HINT = (
    "ℹ️ Şu an düzeltme beklenmiyor. Komutlar için /yardim yazın.\n"
    "Bir mesajın nasıl anlaşılacağını görmek için: <code>/test eth 1R long</code>"
)
START_FAILED_TEXT = (
    "Bildirim botu Telegram'a bağlanamadı ({seconds:.0f} sn içinde yanıt gelmedi). "
    "İnternet bağlantısını kontrol edip botu yeniden başlatın."
)
_PREFIX = {Severity.INFO: "ℹ️", Severity.WARNING: "⚠️", Severity.ERROR: "🚨"}
# ---- the owner's friend (``/arkadas``): read-only copies, and the tickmill catch-up question ----
FRIEND_QUEUE_MAX = 200            # messages waiting for the friend's chat (the oldest dropped first)
FRIEND_ALERT_EVERY_SEC = 3600.0   # the owner is told about a failing friend chat at most this often
FRIEND_REPLY_EVERY_SEC = 60.0     # the friend's own messages get the refusal line at most this often
FRIEND_OWNER_DECIDES = "👤 Bu mesajdaki seçimi bot sahibi yapar (düğmeler yalnızca onda)."
FRIEND_DECIDES = ("👤 Bu soruyu tickmill hesabının sahibi (arkadaşınız) cevaplayacak; düğmeler onda. Size bilgi için "
                  "gönderildi.")
FRIEND_REFUSED = "Bu bot komutlarını yalnızca sahibi kullanabilir."
FRIEND_BUTTON_REFUSED = "Bu düğme yalnızca bot sahibi içindir."
FRIEND_HELLO = (
    "👋 Merhaba! Bu sohbet, kopya-trade botunun bildirimlerini almak için bot sahibi tarafından eklendi. Bundan sonra "
    "bot sahibine giden her bildirim buraya da gelir. tickmill bağlı değilken gelen bir işlem, bağlantı gelince "
    "şartlar uygunsa bot sahibinin ayarına göre ya kendiliğinden açılır ya da size \"sonradan açılsın mı?\" diye "
    "sorulur (✅ Evet / ❌ Hayır). Bu sohbetten bot komutları çalıştırılamaz."
)
FRIEND_CATCH_UP_NOTE = ("bağlantı yokken atlanan tickmill işlemleri <code>/sonradan</code> ayarına göre ya kendiliğinden "
                        "açılır ya da ona sorulur.")
FRIEND_REMOVED = "ℹ️ Bot sahibi bu sohbeti bildirim listesinden çıkardı; buraya artık mesaj gelmeyecek."
FRIEND_NOT_YOURS = "Bu soru artık size ait değil; bot sahibi cevaplayacak."
FRIEND_NOT_YOURS_TAP = "Bu soru artık size ait değil."
OWNER_REASKED = "♻️ Arkadaş ayarı değişti; bu soruyu artık siz cevaplıyorsunuz."
_FRIEND_UNREACHABLE_TR = ("Bot, kendisine hiç yazmamış (Start'a basmamış) ya da botu engellemiş bir kullanıcıya mesaj "
                          "gönderemez.")
_USER_ID_RE = re.compile(r"^\d{1,20}$")       # a user's own id (a group or channel id is negative: refused)


def _with_prefix(text: str, severity: Severity) -> str:
    """The severity icon in front of a notice, unless the text already starts with that same icon."""
    icon = _PREFIX.get(severity, "ℹ️")
    bare = icon.replace("\ufe0f", "")
    if text.lstrip().replace("\ufe0f", "").startswith(bare):
        return text
    return f"{icon} {text}"
_IMPORTANCE = {Severity.INFO: 0, Severity.WARNING: 1, Severity.ERROR: 2}
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
# Telegram (or Telethon) refuses this very message: sending it again cannot help.
_PERMANENT = (errors.BadRequestError, errors.ForbiddenError, ValueError, TypeError, struct.error)
_COMMANDS = {
    "start": "help", "yardim": "help", "yardım": "help", "help": "help",
    "durum": "status", "status": "status",
    "pozisyonlar": "positions", "positions": "positions",
    "bakiye": "balance", "balance": "balance",
    "durdur": "pause", "pause": "pause",
    "devam": "resume", "resume": "resume",
    "hepsini_kapat": "closeall", "closeall": "closeall",
    "test": "test",
    "arkadas": "friend", "arkadaş": "friend",
    "sonradan": "catchup",
}
_COMMAND_RE = re.compile(r"^/([^\s@]+)(?:@\S+)?\s*(.*)$", re.S)
_SENT, _RETRY, _DROP = "sent", "retry", "drop"
# Engine answers that refuse an owner action for now (shutting down / still starting): nothing was decided, so the
# question keeps its buttons and stays usable; the owner only gets the text.
_NOT_NOW = (SHUTDOWN_TEXT, STARTING_TEXT)


def split_html(text: str, limit: int = MAX_CHUNK) -> list[str | tuple[str, list[Any]]]:
    """Short text -> [html]; long text -> [(plain, entities), ...] split without breaking markup."""
    plain, entities = tl_html.parse(text)
    if len(plain) <= limit:
        return [text]
    return [(chunk, ents) for chunk, ents in utils.split_text(plain, entities, limit=limit)]


def _outcome(text: str) -> str:
    return f"<b>Sonuç:</b> {text.strip()}" if text and text.strip() else "<b>Sonuç:</b> —"


def _utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _abandoned(state: Any) -> bool:
    """A Telethon request state whose caller stopped waiting (its future is cancelled)."""
    future = getattr(state, "future", None)
    return future is not None and future.cancelled()


@dataclass(eq=False)
class _FriendMessage:
    """One message for the owner's friend: a new message, or (``conf_id`` with ``closing``) the outcome of a question
    - the friend's own copy of it is edited when he got it with buttons, else it goes as a message."""

    text: str
    buttons: Any = None
    conf_id: str | None = None                       # a question (``buttons``) or its outcome (``closing``)
    closing: bool = False
    conf: PendingConfirmation | None = None          # the question itself (re-rendered when it goes out late)
    created: float = field(default_factory=time.time)
    task: asyncio.Task[Any] | None = None            # its send that got no answer in time (waited for, never resent)
    attempts: int = 0                                # sends of it started so far


@dataclass(eq=False)
class _Outgoing:
    """One message for the owner on its way out."""

    text: str
    importance: int = 1                              # 0 info, 1 warning / reply, 2 error / question / outcome
    created: float = field(default_factory=time.time)   # when the caller produced it
    buttons: Any = None
    reply_to: int | None = None
    on_sent: Callable[[int], None] | None = None     # called with the bot message id once it is delivered
    # (bot message id, html): when this message goes out late, that message is edited first. Used for
    # outcomes whose own edit could not be done: the old message loses its buttons after all.
    edit_first: tuple[int, str] | None = None
    conf: PendingConfirmation | None = None          # a question (asked late: re-rendered with the time left)
    closed_html: str = ""                            # the question without its expiry lines
    outcome: str = ""                                # set when the question is closed before it is delivered
    report: bool = False                             # the "messages were late" report itself
    chunks: list[Any] | None = None
    next_chunk: int = 0
    msg_id: int | None = None
    # A send that got no answer in time. It is kept (not cancelled): Telethon still holds the request
    # and delivers it itself once the connection is back.
    task: asyncio.Task[Any] | None = None
    task_started: float = 0.0
    # An edit of an earlier bot message (msg id, html; its buttons are removed) that the caller did not wait for:
    # the delivery task does it in its turn, or waits for its request when that is on its way already
    # (``edit_task``). Done: nothing else is sent. Not possible: the item goes on as an ordinary message (``text``,
    # with ``edit_first``), as when the caller's own edit fails.
    edit: tuple[int, str] | None = None
    edit_task: asyncio.Task[Any] | None = None
    edit_started: float = 0.0
    failures: int = 0
    queued: bool = False                             # went through the queue (not delivered at the first try)
    cancelled: bool = False                          # question closed meanwhile: do not ask it any more
    mirror: bool = True                              # a copy goes to the owner's friend (when he is set)
    decider: str | None = None                       # a question someone else answers: this line, no buttons


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
        self._closeall_prompt: tuple[int, float] | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._unreachable_logged = False
        # ---- delivery state (see the module docstring) ----
        self._queue: deque[_Outgoing] = deque()                # waiting messages, oldest first
        self._questions: dict[str, _Outgoing] = {}             # conf id -> question not delivered so far
        self._send_tasks: set[asyncio.Task[Any]] = set()       # requests handed to Telethon, not answered yet
        self._pump_task: asyncio.Task[Any] | None = None       # delivers the queue; ends when it is empty
        self._sup_task: asyncio.Task[Any] | None = None        # watches the connection
        self._probe_task: asyncio.Task[Any] | None = None      # "does Telegram answer again?" request
        self._wake = asyncio.Event()                           # wakes the delivery task
        self._sup_wake = asyncio.Event()                       # wakes the supervisor
        self._hold_until = 0.0                                 # monotonic: no new attempt before (flood wait, back-off)
        self._down_since: float | None = None                  # monotonic: Telegram stopped answering then
        self._up_since = 0.0                                   # monotonic: it answered again then
        self._next_forced = 0.0                                # monotonic: next forced reconnect at the earliest
        self._outage_total = 0.0                               # seconds without connection since the last report
        self._outage_count = 0                                 # since start (for /durum)
        self._last_outage: tuple[float, float] | None = None   # (monotonic end, seconds)
        self._late = 0                                         # messages delivered late since the last report
        self._late_oldest: float | None = None                 # creation time of the oldest of them
        self._dropped = 0                                      # thrown away (queue full) since the last report
        self._dropped_total = 0
        self._shut = False                                     # stop() has finished: nothing is sent any more
        # ---- the owner's friend (/arkadas) ----
        self._friend: int | None = None                        # his chat id (stored by the engine)
        self._friend_queue: deque[_FriendMessage] = deque()
        self._friend_pump: asyncio.Task[Any] | None = None     # delivers that queue, one message at a time
        self._friend_alert_at: float | None = None             # monotonic: the owner was told about a failure then
        self._friend_reply_at = float("-inf")                  # monotonic: the friend got the refusal line then
        # conf id -> (the friend's message id, its text) of a question he got with buttons
        self._friend_msgs: OrderedDict[str, tuple[int, str]] = OrderedDict()
        self._friend_routed: OrderedDict[str, None] = OrderedDict()    # conf ids of questions given to the friend
        self._former_friends: set[int] = set()                 # earlier friends: their taps get a short answer
        self._owner_reask: dict[str, int] = {}                 # conf id -> the owner's re-sent question (with buttons)
        self._friend_wake = asyncio.Event()                    # cuts a retry pause short (a question was closed...)

    # ---- lifecycle -------------------------------------------------------
    def _make_client(self) -> Any:
        """The bot's Telethon client. ``connection_retries=None``: after a network loss Telethon keeps
        trying to reconnect for ever (its default gives up after 5 tries and then stays disconnected:
        no alerts, no questions, no owner commands until a restart)."""
        return TelegramClient(
            str(self._data_dir / "bot.session"),
            self._tg.api_id,
            self._tg.api_hash,
            device_model="copytrader-bot",
            app_version=__version__,
            connection_retries=None,
            retry_delay=BOT_RETRY_DELAY_SEC,
        )

    async def start(self, engine: EngineControl) -> None:
        self._engine = engine
        if self._client is None:
            self._data_dir.mkdir(parents=True, exist_ok=True)
            self._client = self._make_client()
        client = self._client
        # Until the login is verified below nothing may be sent (or reconnected) through this client;
        # if start() fails it stays that way, as before: later notices are only logged.
        self._shut = True
        try:
            # Bounded: the client itself would keep trying to connect for ever.
            await asyncio.wait_for(client.start(bot_token=self._config.bot_token), START_TIMEOUT_SEC)
            me = await asyncio.wait_for(client.get_me(), SEND_TIMEOUT_SEC)
        except errors.AccessTokenInvalidError as exc:
            raise RuntimeError("Bot token geçersiz (notifier.bot_token). @BotFather'dan doğru token'ı alın.") from exc
        except asyncio.TimeoutError as exc:
            await self._disconnect_client()
            raise ConnectionError(START_FAILED_TEXT.format(seconds=START_TIMEOUT_SEC)) from exc
        token_id = self._config.bot_token.split(":", 1)[0]
        if not getattr(me, "bot", False) or str(getattr(me, "id", "")) != token_id:
            await self._disconnect_client()
            raise RuntimeError(
                f"{self._data_dir / 'bot.session'} başka bir hesaba ait (bot token ile eşleşmiyor). "
                "Bu dosyayı silip yeniden başlatın."
            )
        if me.id == self._owner:
            raise RuntimeError("notifier.owner_user_id botun kendi ID'si olamaz; kendi kullanıcı ID'nizi yazın.")
        self._shut = False
        client.parse_mode = "html"
        self._no_hidden_flood_sleep()
        client.add_event_handler(self._on_message_event, events.NewMessage(incoming=True))
        client.add_event_handler(self._on_callback_event, events.CallbackQuery())
        self._ensure_supervisor()
        log.info("Notifier bot @%s started; owner id %s", getattr(me, "username", None), self._owner)
        if await self._send(STARTUP_TEXT, backlog=False) is None:
            if self._unreachable_logged:
                log.error(
                    "The bot cannot message the owner (user id %s). Open the bot in Telegram and press Start, "
                    "and check notifier.owner_user_id. Confirmations cannot be delivered until then.",
                    self._owner,
                )
            else:
                log.warning("The start message could not be delivered yet; it is sent when Telegram answers")

    async def stop(self) -> None:
        """Let running handlers finish, deliver what is still queued (both within STOP_GRACE_SEC),
        then disconnect. Every wait is bounded: this never hangs."""
        deadline = time.monotonic() + STOP_GRACE_SEC
        running = [t for t in self._tasks if not t.done()]
        if running:
            _, pending = await asyncio.wait(running, timeout=STOP_GRACE_SEC)
            for task in pending:
                task.cancel()
        if self._queue and self._client is not None and not self._shut:
            flush_until = max(deadline, time.monotonic() + STOP_FLUSH_MIN_SEC)
            self._kick()
            pump = self._ensure_pump()
            while pump is not None and not pump.done() and self._queue:
                left = flush_until - time.monotonic()
                # A flood wait / unreachable owner that outlasts the grace period: waiting cannot help.
                if left <= 0 or self._hold_until - time.monotonic() > left:
                    break
                await asyncio.wait({pump}, timeout=min(left, 0.5))
        pump = self._friend_pump              # what the friend still gets (the "stopped" notice), within the grace
        self._friend_wake.set()
        if pump is not None and not pump.done():
            await asyncio.wait({pump}, timeout=max(0.0, min(STOP_FLUSH_MIN_SEC, deadline - time.monotonic())))
        self._shut = True
        self._wake.set()
        self._sup_wake.set()
        background = [
            t for t in (self._pump_task, self._sup_task, self._probe_task, self._friend_pump, *self._send_tasks)
            if t is not None and not t.done()
        ]
        for task in background:
            task.cancel()
        if background:
            await asyncio.wait(background, timeout=DISCONNECT_TIMEOUT_SEC)
        if self._queue:
            log.error(
                "%d message(s) for the owner were NOT delivered before shutdown: %s",
                len(self._queue), " | ".join(f"{item.text:.200}" for item in self._queue),
            )
        if self._client is not None:
            await self._disconnect_client()

    # ---- Notifier API ----------------------------------------------------
    async def notify(self, text: str, severity: Severity = Severity.INFO) -> None:
        """Tell the owner. Returns when the notice is delivered, or at the latest after CALLER_WAIT_SEC (it then
        stays queued, in order, and is delivered later); while notices are already waiting or the connection is
        down it is queued and this returns at once."""
        try:
            await self._send(_with_prefix(text, severity), backlog=True, importance=_IMPORTANCE.get(severity, 0))
        except Exception:
            log.exception("notify() failed")

    async def ask_confirmation(self, conf: PendingConfirmation) -> int | None:
        """Send the 'I understood X, is that right?' message with buttons. Returns bot message id.

        None: it was not delivered within CALLER_WAIT_SEC. It is then asked as soon as Telegram answers
        (its request may already be on its way), as long as it is still valid; once delivered, the bot
        knows its message id, so its buttons, replies to it and ``close_confirmation`` work as usual
        (see ``close_confirmation`` for a question that never went out)."""
        try:
            text = format_confirmation(conf)
            closed_html = format_confirmation(conf, with_expiry=False)
            row = []
            if conf.kind == "catch_up":    # yes / no: nothing to correct
                row.append(Button.inline(CATCH_UP_YES, f"ok:{conf.id}".encode()))
                row.append(Button.inline(CATCH_UP_NO, f"no:{conf.id}".encode()))
            else:
                if conf.plan is not None:      # blocking issues -> only correction / cancel
                    row.append(Button.inline("✅ Onayla", f"ok:{conf.id}".encode()))
                row.append(Button.inline("✏️ Düzelt", f"fix:{conf.id}".encode()))
                row.append(Button.inline("❌ İptal", f"no:{conf.id}".encode()))
        except Exception:
            log.exception("Could not build confirmation %s", conf.id)
            return None
        if self._client is None or self._shut:
            log.warning("Notifier not running (not started or stopped); dropping confirmation %s", conf.id)
            return None
        previous = self._questions.pop(conf.id, None)
        if previous is not None:           # asked again before the first version went out
            previous.cancelled = True
            previous.outcome = _outcome(_STATUS_OUTCOME[ConfirmationStatus.SUPERSEDED])
        self._closed.pop(conf.id, None)    # re-asked (e.g. after a correction): open again
        decider = (conf.catch_up or {}).get("decider") if conf.kind == "catch_up" else None
        if decider is not None and self._friend is not None and decider == self._friend:
            # The friend answers it (the engine wrote him into the question): he gets it with the buttons, the owner
            # the same text without them and who decides.
            self._remember(self._friend_routed, conf.id, None)
            self._friend_put(_FriendMessage(text=text, buttons=[row], conf_id=conf.id, conf=conf))
            item = _Outgoing(text=format_confirmation(conf, decider=FRIEND_DECIDES), importance=2, conf=conf,
                             closed_html=closed_html, mirror=False, decider=FRIEND_DECIDES)
        else:
            item = _Outgoing(text=text, importance=2, buttons=[row], conf=conf, closed_html=closed_html)
        self._questions[conf.id] = item    # until it is delivered (then _sent / _by_msg know it)
        try:
            return await self._submit(item)
        except Exception:
            log.exception("ask_confirmation failed for %s", conf.id)
            return None

    async def close_confirmation(self, conf: PendingConfirmation, result_text: str) -> None:
        """Edit the confirmation message: remove buttons, append the outcome.

        A question that never reached the owner gets a message of its own with the question in it,
        so that the owner knows which trade the outcome is about."""
        try:
            outcome = _outcome(result_text or _STATUS_OUTCOME.get(conf.status, ""))
            if conf.id in self._friend_routed and conf.id not in self._friend_msgs:
                outcome += f"\n{self._not_reached(conf)}"       # (never "he did not answer")
            self._remember(self._closed, conf.id, result_text or "")
            self._friend_wake.set()            # a retry of this question for the friend ends now
            waiting = self._questions.pop(conf.id, None)
            if waiting is not None:        # not delivered so far: never ask it now
                waiting.cancelled = True
                waiting.outcome = outcome
            msg_id = conf.bot_message_id
            if msg_id is None and waiting is None:
                # Delivered late, after ask_confirmation had returned None? (Not while this very
                # version still waits: then _sent can only know an older version of the question.)
                late = self._sent.get(conf.id)
                msg_id = late[0] if late else None
            closed_html = format_confirmation(conf, with_expiry=False)
            if self._friend is not None:   # (his copy of the question shows the outcome too)
                self._friend_put(_FriendMessage(text=f"{closed_html}\n\n{outcome}", conf_id=conf.id, closing=True))
            reasked = self._owner_reask.pop(conf.id, None)
            if reasked is not None and reasked != msg_id:  # the copy sent to the owner with buttons after a change
                await self._edit(reasked, f"{closed_html}\n\n{outcome}")
            if msg_id is None:
                await self._send(f"{UNDELIVERED_QUESTION}\n\n{closed_html}\n\n{outcome}", importance=2, mirror=False)
                return
            # The question's message shows the outcome (buttons removed); if that cannot be done, the outcome goes
            # out as a reply that edits the question first. The caller waits at most CALLER_WAIT_SEC.
            await self._edit_or_send(msg_id, closed_html + "\n\n" + outcome, outcome)
        except Exception:
            log.exception("close_confirmation failed for %s", conf.id)

    async def ask_owner(self, conf: PendingConfirmation) -> int | None:
        """A catch-up question the friend had, sent to the OWNER with its buttons (the friend was removed or changed:
        the owner answers it now). Returns the message id (None: queued)."""
        try:
            row = [Button.inline(CATCH_UP_YES, f"ok:{conf.id}".encode()),
                   Button.inline(CATCH_UP_NO, f"no:{conf.id}".encode())]
            text = f"{OWNER_REASKED}\n{format_confirmation(conf)}"
        except Exception:
            log.exception("Could not build confirmation %s", conf.id)
            return None

        def sent(msg_id: int, conf_id: str = conf.id) -> None:
            self._owner_reask[conf_id] = msg_id
            self._remember(self._by_msg, msg_id, conf_id)

        self._friend_routed.pop(conf.id, None)
        return await self._send(text, buttons=[row], importance=2, on_sent=sent, mirror=False)

    @staticmethod
    def _not_reached(conf: PendingConfirmation) -> str:
        broker = esc((conf.catch_up or {}).get("broker") or "?")
        return f"Soru {broker} hesabının sahibine (arkadaşınıza) ulaştırılamadı; bu yüzden yanıt gelmedi."

    def bot_status_text(self) -> str:
        """The bot's own delivery state (connection, waiting messages, last outage) for /durum."""
        now = time.monotonic()
        down_for = now - self._down_since if self._down_since is not None else None
        if down_for is None and self._client is not None and not self._client_connected():
            down_for = 0.0
        oldest = min((item.created for item in self._queue), default=None)
        last = self._last_outage
        return format_bot_status(
            down_for_sec=down_for,
            queued=len(self._queue),
            oldest_queued_age_sec=time.time() - oldest if oldest is not None else None,
            last_outage=(now - last[0], last[1]) if last is not None else None,
            outages=self._outage_count,
            dropped=self._dropped_total,
        )

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
        if self._is_friend(getattr(event, "sender_id", None)):
            self._friend_refuse()          # never executed, whatever it is
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
            await self._send(f"🚨 Hata: <code>{esc(exc)}</code>", importance=2)

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
            refused = await engine.request_correction(conf_id)     # reply to a confirmation = correction
            if refused in _NOT_NOW or refused == NO_CORRECTION_FRIEND_TEXT:
                # Shutting down / still starting: nothing was taken (no "correction received"); the question
                # stays as it is and the owner sends the correction again later.
                await self._send(refused)
                return
        else:
            conf_id = await engine.awaiting_correction_id()
        if conf_id is None:
            await self._send(PLAIN_HINT)
            return
        await self._send("⏳ Düzeltme alındı, yeniden değerlendiriliyor…")
        reply = await engine.handle_correction(conf_id, text)
        if reply:
            await self._send(reply, importance=2)

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
            await self._send(reply or "✔️")
            await self._send(self.bot_status_text())     # the bot's own connection / waiting messages
            return
        if command == "positions":
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
        elif command == "friend":
            await self._friend_command(args)
            return
        elif command == "catchup":
            reply = await engine.catch_up_command(args)
        else:                            # test
            if not args:
                await self._send("Kullanım: <code>/test eth 1R long, giriş 2232, SL 2205, TP 2310</code>")
                return
            await self._send("⏳ Deneme amaçlı çözümleniyor (işlem açılmaz)…")
            reply = await engine.test_parse(args)
        await self._send(reply or "✔️")

    @staticmethod
    def _close_all_prompt() -> tuple[str, list[list[Any]]]:
        """The /hepsini_kapat question (HTML) and its buttons."""
        return (
            "⚠️ <b>Tüm açık pozisyonlar kapatılsın ve bekleyen emirler iptal edilsin mi?</b>\n"
            f"Bu işlem geri alınamaz. ({int(CLOSEALL_TTL_SEC // 60)} dk içinde onaylayın.)",
            [[
                Button.inline("✅ Evet, hepsini kapat", b"closeall:yes"),
                Button.inline("❌ Vazgeç", b"closeall:no"),
            ]],
        )

    async def _ask_close_all(self) -> None:
        text, buttons = self._close_all_prompt()

        def prompt_sent(msg_id: int) -> None:    # also when it goes out late: its time starts at delivery
            self._closeall_prompt = (msg_id, time.monotonic())

        await self._send(text, buttons=buttons, on_sent=prompt_sent)

    # ---- button presses (testable) ------------------------------------------
    async def handle_callback(self, event: Any) -> None:
        if self._is_friend(getattr(event, "sender_id", None)):
            await self._handle_friend_callback(event)
            return
        if getattr(event, "sender_id", None) in self._former_friends and getattr(event, "sender_id", None) != self._owner:
            await self._answer(event, FRIEND_NOT_YOURS_TAP, alert=True)
            return
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
                if reply in _NOT_NOW:
                    await self._send(reply)          # (no "write the correction" hint: nothing waits for it)
                    return
                await self._send(f"{reply.strip()}\n\n{CORRECTION_HINT}" if reply and reply.strip() else CORRECTION_HINT)
            else:
                approved = kind == "ok"
                await self._answer(event, "⏳ Onaylandı, işleniyor…" if approved else "❌ İptal ediliyor…")
                result = await engine.handle_confirmation(conf_id, approved)
                if result in _NOT_NOW:
                    # Nothing was decided: the confirmation stays pending (shutting down: restored after the
                    # restart; starting: usable in a few seconds), so its buttons stay on the message and the
                    # owner can tap them again then.
                    await self._send(result)
                    return
                await self._after_decision(conf_id, getattr(event, "message_id", None), result)
        except Exception as exc:
            log.exception("Button %s for confirmation %s failed", kind, conf_id)
            await self._send(f"🚨 İşlem sırasında hata: <code>{esc(exc)}</code>", importance=2)
        finally:
            self._inflight.discard(conf_id)

    async def _after_decision(self, conf_id: str, msg_id: int | None, result: str) -> None:
        shown = self._closed.get(conf_id)
        if shown is not None:            # engine already closed the message itself
            if result and result.strip() and result.strip() != shown.strip():
                await self._send(result, importance=2)
            return
        self._remember(self._closed, conf_id, result or "")
        if msg_id is None:
            await self._send(_outcome(result), importance=2)
            return
        base = await self._original_html(conf_id, msg_id)
        body = (base + "\n\n" if base else "") + _outcome(result)
        self._mirror(body)
        if not await self._edit(msg_id, body):
            await self._send(_outcome(result), importance=2, edit_first=(msg_id, body), mirror=False)

    async def _original_html(self, conf_id: str, msg_id: int) -> str:
        sent = self._sent.get(conf_id)
        if sent and sent[0] == msg_id:
            return sent[1]
        if self._link_down():
            return ""
        try:                             # after a restart: re-read the message
            msg = await asyncio.wait_for(self._client.get_messages(self._owner, ids=msg_id), SEND_TIMEOUT_SEC)
        except Exception as exc:
            self._forget_abandoned_requests()
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
                self._mirror("❌ Vazgeçildi, hiçbir pozisyon kapatılmadı.")
                await self._edit(msg_id, "❌ Vazgeçildi, hiçbir pozisyon kapatılmadı.")
            return
        if not valid:
            await self._answer(event, "Bu onayın süresi doldu; /hepsini_kapat komutunu tekrar gönderin.", alert=True)
            if msg_id is not None:
                self._mirror("⌛ Süresi doldu, hiçbir pozisyon kapatılmadı.")
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
        if result == STARTING_TEXT:
            # Nothing was done (the owner's brokers are still being connected): the question gets its buttons back
            # and stays valid for the rest of its time, so "Evet" can be tapped again in a few seconds. (Shutting
            # down: as before the message shows the text; the question lives in memory only and could not be
            # confirmed after the restart.)
            if self._closeall_prompt is None:    # (unless a newer /hepsini_kapat question has been sent meanwhile)
                self._closeall_prompt = prompt
            text, buttons = self._close_all_prompt()
            await self._edit(msg_id, text, buttons=buttons)
            await self._send(result)
            return
        final = result or "✔️ Tamamlandı."
        self._mirror(final)
        if not await self._edit(msg_id, final):
            await self._send(final, importance=2, edit_first=(msg_id, final) if msg_id is not None else None,
                             mirror=False)

    async def _answer(self, event: Any, text: str, *, alert: bool = False) -> None:
        started = time.monotonic()
        try:
            await asyncio.wait_for(event.answer(text, alert=alert), SEND_TIMEOUT_SEC)
        except asyncio.TimeoutError:
            self._forget_abandoned_requests()
            self._mark_down(f"no answer to a button reply within {SEND_TIMEOUT_SEC:.0f}s", since=started)
        except Exception as exc:
            log.debug("Callback answer failed: %s", exc)

    # ---- the owner's friend (/arkadas) ---------------------------------------
    def set_friend(self, chat_id: int | None) -> None:
        """The friend's chat (the engine keeps it in its database and hands it over at start and on /arkadas). A
        change forgets what was waiting for the earlier friend; his later taps get a short answer."""
        new = int(chat_id) if chat_id else None
        old = self._friend
        if old is not None and old != new:
            self._former_friends.add(old)
        if new is not None:
            self._former_friends.discard(new)
        if new != old:
            self._friend_queue.clear()
            self._friend_msgs.clear()
            self._friend_routed.clear()
            self._friend_wake.set()
        self._friend = new

    @property
    def friend_chat_id(self) -> int | None:
        return self._friend

    def _is_friend(self, sender_id: int | None) -> bool:
        return self._friend is not None and sender_id is not None and sender_id == self._friend \
            and sender_id != self._owner

    def _mirror(self, text: str, *, buttons: bool = False) -> None:
        """A read-only copy of an owner message for the friend (a message with buttons: without them, and one line
        saying that the owner decides). Never waits, never raises."""
        if self._friend is None or not text or not text.strip():
            return
        self._friend_put(_FriendMessage(text=f"{text}\n\n{FRIEND_OWNER_DECIDES}" if buttons else text))

    def _friend_put(self, message: _FriendMessage) -> None:
        if self._friend is None or self._client is None or self._shut:
            return
        self._friend_queue.append(message)
        while len(self._friend_queue) > FRIEND_QUEUE_MAX:
            dropped = self._friend_queue.popleft()
            log.warning("Too many messages wait for the friend's chat (limit %d): dropped %.200s",
                        FRIEND_QUEUE_MAX, dropped.text)
        task = self._friend_pump
        if task is None or task.done():
            self._friend_pump = asyncio.ensure_future(self._friend_run())

    async def _friend_run(self) -> None:
        """Deliver the friend's queue in order, one message at a time, each bounded by SEND_TIMEOUT_SEC. A message that
        cannot be delivered is dropped (logged; the owner is told at most once per FRIEND_ALERT_EVERY_SEC) - except an
        open question with buttons (only he can answer it): it stays first and is tried again, after the wait Telegram
        asks for (FloodWait) or with back-off, until it is delivered or the question is closed."""
        delay = RETRY_BASE_SEC
        while self._friend_queue and not self._shut:
            message = self._friend_queue[0]
            chat = self._friend
            if chat is None:
                self._friend_queue.clear()
                return
            try:
                await self._friend_deliver(chat, message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:          # noqa: BLE001 - the friend's chat must never break anything
                await self._friend_failed(chat, exc)
                if self._friend_keep(message, chat):
                    seconds = getattr(exc, "seconds", None)
                    wait = (float(seconds) + FLOOD_MARGIN_SEC if isinstance(exc, errors.FloodError) and seconds
                            else delay)
                    delay = min(delay * 2.0, RETRY_MAX_SEC)
                    await self._friend_pause(wait)
                    continue              # (still first: tried again, unless it was closed meanwhile)
            delay = RETRY_BASE_SEC
            if self._friend_queue and self._friend_queue[0] is message:
                self._friend_queue.popleft()

    def _friend_keep(self, message: _FriendMessage, chat: int) -> bool:
        """An open question with buttons for this very friend: never dropped."""
        return (message.buttons is not None and message.conf_id is not None and not message.closing
                and message.conf_id not in self._closed and chat == self._friend and not self._shut
                and bool(self._friend_queue) and self._friend_queue[0] is message)

    async def _friend_pause(self, seconds: float) -> None:
        """Wait before a retry; a closed question, a change of friend or shutdown cut it short."""
        self._friend_wake.clear()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._friend_wake.wait(), max(0.0, seconds))

    async def _friend_deliver(self, chat: int, message: _FriendMessage) -> None:
        conf_id = message.conf_id
        if message.closing and conf_id is not None:
            sent = self._friend_msgs.pop(conf_id, None)
            if sent is not None and await self._friend_request(self._friend_edit(chat, sent[0], message.text)):
                return                        # his question now shows the outcome, buttons removed
            await self._friend_send(chat, message.text)
            return
        if message.buttons is not None and conf_id is not None:
            await self._friend_question(chat, message)
            return
        await self._friend_send(chat, message.text)

    async def _friend_question(self, chat: int, message: _FriendMessage) -> None:
        """A question the friend answers (with its buttons). Closed before it got to him: not sent (its outcome
        follows). Too little time left: not sent either. Going out late: says so, with the time really left. A send
        that got no answer in time is waited for again on the next try, never sent a second time."""
        conf_id = message.conf_id
        assert conf_id is not None
        task = message.task
        if task is None:
            if conf_id in self._closed:
                return
            text = message.text
            conf = message.conf
            if conf is not None:
                now = datetime.now(timezone.utc)
                age = time.time() - message.created
                late = message.attempts > 0 or age >= self._caller_wait()     # (not at its first chance)
                if late and (_utc(conf.expires_at) - now).total_seconds() < QUESTION_MIN_LEFT_SEC:
                    log.warning("Question %s is not sent to the friend any more: too little time left", conf_id)
                    return
                if age >= LATE_MARK_SEC:
                    text = late_question_prefix(age) + "\n" + format_confirmation(
                        conf.model_copy(update={"created_at": now}))
            chunk = self._chunks(text)[-1]
            if isinstance(chunk, str):
                request = self._client.send_message(chat, chunk, parse_mode="html", buttons=message.buttons,
                                                    link_preview=False)
            else:
                request = self._client.send_message(chat, chunk[0], formatting_entities=chunk[1],
                                                    buttons=message.buttons, link_preview=False)
            task = message.task = self._track(asyncio.ensure_future(request))
            message.text = text
            message.attempts += 1
        done, _ = await asyncio.wait({task}, timeout=SEND_TIMEOUT_SEC)
        if not done:
            raise asyncio.TimeoutError("Telegram yanıt vermedi")     # (the request stays with Telethon)
        message.task = None
        if task.cancelled():
            raise asyncio.TimeoutError("istek yarıda kesildi")
        exc = task.exception()
        if exc is not None:
            raise exc
        msg_id = getattr(task.result(), "id", None)
        if msg_id is not None:
            closed = format_confirmation(message.conf, with_expiry=False) if message.conf is not None else message.text
            if chat != self._friend:
                # The friend changed while this copy was on its way: it is not his question any more. Take its
                # buttons away in the chat it reached; never record it (an edit would go to the new friend's chat).
                await self._friend_request(self._friend_edit(chat, msg_id, f"{closed}\n\n{FRIEND_NOT_YOURS}"))
                return
            self._remember(self._friend_msgs, conf_id, (msg_id, closed))     # (its text once the buttons are gone)

    async def _friend_send(self, chat: int, text: str, *, buttons: Any = None) -> int | None:
        msg_id: int | None = None
        chunks = self._chunks(text)
        for i, chunk in enumerate(chunks):
            kwargs: dict[str, Any] = {"buttons": buttons if i == len(chunks) - 1 else None, "link_preview": False}
            if isinstance(chunk, str):
                request = self._client.send_message(chat, chunk, parse_mode="html", **kwargs)
            else:
                request = self._client.send_message(chat, chunk[0], formatting_entities=chunk[1], **kwargs)
            result = await self._friend_request(request, raise_errors=True)
            msg_id = getattr(result, "id", None)
        return msg_id

    def _friend_edit(self, chat: int, msg_id: int, text: str) -> Coroutine[Any, Any, Any]:
        chunk = split_html(text, limit=4096)[-1]
        if isinstance(chunk, str):
            return self._client.edit_message(chat, msg_id, chunk, parse_mode="html", buttons=None, link_preview=False)
        return self._client.edit_message(chat, msg_id, chunk[0], formatting_entities=chunk[1], buttons=None,
                                         link_preview=False)

    async def _friend_request(self, request: Coroutine[Any, Any, Any], *, raise_errors: bool = False) -> Any:
        """One request for the friend's chat, waited for at most SEND_TIMEOUT_SEC. A request without an answer by
        then is left to Telethon (not cancelled, as for the owner's messages) and counts as failed. Returns its
        result (an edit: True), or None / raises (``raise_errors``) when it failed."""
        task = self._track(asyncio.ensure_future(request))
        done, _ = await asyncio.wait({task}, timeout=SEND_TIMEOUT_SEC)
        if not done:
            if raise_errors:
                raise asyncio.TimeoutError("Telegram yanıt vermedi")
            return None
        exc = None if task.cancelled() else task.exception()
        if task.cancelled() or exc is not None:
            if isinstance(exc, errors.MessageNotModifiedError):
                return True
            if raise_errors:
                raise exc if exc is not None else asyncio.TimeoutError("istek yarıda kesildi")
            log.warning("A request for the friend's chat failed: %s", exc)
            return None
        return task.result() if raise_errors else True

    @staticmethod
    def _friend_why(exc: BaseException) -> str:
        if isinstance(exc, asyncio.TimeoutError):
            return "Telegram yanıt vermedi"
        return f"<code>{esc(str(exc) or type(exc).__name__)}</code>"

    async def _friend_failed(self, chat: int, exc: BaseException) -> None:
        log.error("A message for the friend's chat %s could not be delivered: %s", chat, exc)
        now = time.monotonic()
        if self._friend_alert_at is not None and now - self._friend_alert_at < FRIEND_ALERT_EVERY_SEC:
            return
        self._friend_alert_at = now
        why = (f" {_FRIEND_UNREACHABLE_TR} Size gelen mesajlar etkilenmez." if isinstance(exc, Exception)
               and self._is_unreachable(exc) else "")
        await self._send(f"⚠️ Arkadaşınıza (<code>{chat}</code>) mesaj iletilemedi: {self._friend_why(exc)}.{why} "
                         "(Bu uyarı en fazla saatte bir gönderilir.)", mirror=False)

    def _friend_refuse(self) -> None:
        """A message from the friend's chat: never executed; one refusal line now and then."""
        log.info("A message from the friend's chat was not executed (commands are the owner's)")
        now = time.monotonic()
        if now - self._friend_reply_at < FRIEND_REPLY_EVERY_SEC:
            return
        self._friend_reply_at = now
        self._friend_put(_FriendMessage(text=FRIEND_REFUSED))

    async def _handle_friend_callback(self, event: Any) -> None:
        """A button press of the friend: ✅ Evet / ❌ Hayır of a catch-up question only (the engine checks the kind);
        everything else is the owner's."""
        data = bytes(getattr(event, "data", None) or b"").decode("utf-8", "replace")
        kind, _, conf_id = data.partition(":")
        engine = self._engine
        if kind not in ("ok", "no") or not conf_id:
            await self._answer(event, FRIEND_BUTTON_REFUSED, alert=True)
            return
        if engine is None:
            await self._answer(event, "Sistem henüz hazır değil", alert=True)
            return
        if conf_id in self._inflight:
            await self._answer(event, "⏳ Zaten işleniyor…")
            return
        self._inflight.add(conf_id)
        try:
            approved = kind == "ok"
            closed_before = conf_id in self._closed
            await self._answer(event, "⏳ İşleniyor…")
            result = await engine.handle_confirmation(conf_id, approved, by_friend=True)
            shown = self._closed.get(conf_id)
            if closed_before or shown is None:
                # Nothing was decided now (not his question, answered / expired already, shutting down): to him only.
                self._friend_put(_FriendMessage(text=result or "—"))
            elif result and result.strip() and result.strip() != shown.strip():
                await self._send(result, importance=2)          # (mirrored to him as well)
        except Exception as exc:
            log.exception("The friend's button %s for confirmation %s failed", kind, conf_id)
            self._friend_put(_FriendMessage(text=f"🚨 İşlem sırasında hata: <code>{esc(exc)}</code>"))
        finally:
            self._inflight.discard(conf_id)

    async def _friend_dropped(self, old: int | None, questions: dict[str, tuple[int, str]]) -> None:
        """The friend ``old`` was removed / replaced: his question messages lose their buttons, and he is told."""
        if old is None:
            return
        for msg_id, text in questions.values():
            await self._friend_request(self._friend_edit(old, msg_id, f"{text}\n\n{FRIEND_NOT_YOURS}"))
        try:
            await self._friend_send(old, FRIEND_REMOVED)
        except Exception as exc:              # noqa: BLE001 - only a courtesy
            log.warning("Could not tell the friend's chat %s that it was removed: %s", old, exc)

    async def _friend_command(self, args: str) -> None:
        """/arkadas (show), /arkadas <chat id> (set, after a test message reached it), /arkadas sil (remove)."""
        engine = self._engine
        store = getattr(engine, "set_friend_chat", None)
        arg = args.strip()
        if not arg:
            if self._friend is None:
                await self._send("👤 Arkadaş sohbeti ayarlı değil. Eklemek için: <code>/arkadas &lt;sohbet_id&gt;</code> "
                                 "(arkadaşınız önce botu Telegram'da açıp Start'a basmalı).", mirror=False)
            else:
                await self._send(f"👤 Arkadaşınızın sohbeti: <code>{self._friend}</code>. Size gelen her bildirim ona "
                                 f"da gider; {FRIEND_CATCH_UP_NOTE} Kaldırmak için: <code>/arkadas sil</code>",
                                 mirror=False)
            return
        if store is None:
            await self._send("⏳ Sistem henüz hazır değil, biraz sonra tekrar deneyin.", mirror=False)
            return
        if arg.lower() in ("sil", "kaldır", "kaldir"):
            old, questions = self._friend, dict(self._friend_msgs)
            await store(None)                 # (the engine gives the owner the questions the friend had)
            await self._friend_dropped(old, questions)
            await self._send("✅ Arkadaş sohbeti kaldırıldı; bildirimler artık yalnızca size gelir (<code>/sonradan "
                             "sor</code> seçiliyse tickmill sorularını siz cevaplarsınız).", mirror=False)
            return
        if not _USER_ID_RE.match(arg) or int(arg) == 0:
            await self._send("Kullanım: <code>/arkadas &lt;sohbet_id&gt;</code> (arkadaşınızın kullanıcı kimliği: "
                             "pozitif bir sayı; grup ya da kanal olmaz), <code>/arkadas</code> ya da "
                             "<code>/arkadas sil</code>", mirror=False)
            return
        chat = int(arg)
        if chat == self._owner:
            await self._send("Bu sizin kendi kimliğiniz; arkadaşınızın sohbet kimliğini yazın.", mirror=False)
            return
        try:
            await self._friend_send(chat, FRIEND_HELLO)
        except Exception as exc:              # noqa: BLE001 - told to the owner
            reason = "" if self._is_unreachable(exc) else f" ({self._friend_why(exc)})"
            await self._send(f"❌ Deneme mesajı <code>{chat}</code> sohbetine ulaştırılamadı{reason}; kaydedilmedi. "
                             "Bir bot, kendisine hiç yazmamış (botu Telegram'da açıp Start'a basmamış) ya da botu "
                             "engellemiş bir kullanıcıya mesaj gönderemez. Arkadaşınız botu açıp Start'a bassın, sonra "
                             f"<code>/arkadas {chat}</code> komutunu tekrar gönderin.", mirror=False)
            return
        old, questions = self._friend, dict(self._friend_msgs)
        await store(chat)
        if old is not None and old != chat:
            await self._friend_dropped(old, questions)
        await self._send(f"✅ Arkadaşınızın sohbeti kaydedildi (<code>{chat}</code>); deneme mesajı ulaştı. Bundan sonra "
                         f"size gelen her bildirim ona da gider; {FRIEND_CATCH_UP_NOTE}", mirror=False)

    # ---- sending ------------------------------------------------------------
    async def _send(
        self,
        text: str,
        *,
        buttons: Any = None,
        reply_to: int | None = None,
        backlog: bool = False,
        importance: int = 1,
        on_sent: Callable[[int], None] | None = None,
        edit_first: tuple[int, str] | None = None,
        mirror: bool = True,
    ) -> int | None:
        """Send HTML to the owner. Never raises and never waits longer than SEND_TIMEOUT_SEC. ``mirror``: a copy goes
        to the owner's friend (when he is set; never for an outcome that edits an earlier message).

        Returns the (last) message id, or None when it could not be sent now. It is then queued and
        delivered later, in order, unless Telegram refused this very message (``on_sent`` gets the
        id at that point). ``backlog`` is kept for older callers: every message is queued now."""
        if self._client is None or self._shut:
            log.warning("Notifier not running (not started or stopped); dropping message: %.200s", text)
            return None
        if not text or not text.strip():
            return None
        item = _Outgoing(text=text, importance=importance, buttons=buttons, reply_to=reply_to, on_sent=on_sent,
                         edit_first=edit_first, mirror=mirror and edit_first is None)
        try:
            return await self._submit(item)
        except Exception:
            log.exception("Sending to the owner failed unexpectedly: %.200s", text)
            return None

    @staticmethod
    def _caller_wait() -> float:
        """How long a caller waits for Telegram before its message is left to the queue."""
        return min(CALLER_WAIT_SEC, SEND_TIMEOUT_SEC)

    async def _submit(self, item: _Outgoing) -> int | None:
        """Deliver now if nothing is in the way, else queue. The caller waits at most CALLER_WAIT_SEC for the
        whole message: a request without an answer by then stays with the item, which is queued in its place,
        and the delivery task gives it the rest of SEND_TIMEOUT_SEC before it takes the connection for down."""
        if item.mirror and self._friend is not None:
            self._mirror(item.text, buttons=item.buttons is not None)     # (never waits)
        if not self._client_connected():
            self._mark_down("the client is disconnected")
        if self._queue or self._down_since is not None:
            self._enqueue(item)          # keep the order; the caller does not wait
            return None
        try:
            outcome = await self._attempt(item, deadline=time.monotonic() + self._caller_wait())
        except asyncio.CancelledError:
            if item.task is not None:    # the caller was cancelled mid-send: the queue finishes the job
                self._enqueue(item)
            raise
        if outcome == _SENT:
            return item.msg_id
        if outcome == _RETRY:
            self._enqueue(item)
        else:
            self._forget_question(item)
        return None

    async def _attempt(self, item: _Outgoing, deadline: float | None = None) -> str:
        """Send what is left of ``item``. _SENT, _RETRY (keep it: it is or will be queued) or _DROP.
        Waits at most SEND_TIMEOUT_SEC per chunk, and not beyond ``deadline`` (a caller's whole message);
        a request without an answer stays with ``item.task``."""
        while True:
            task = item.task
            fresh = task is None
            timeout = SEND_TIMEOUT_SEC
            if deadline is not None:
                timeout = min(timeout, deadline - time.monotonic())
                if timeout <= 0:
                    return _RETRY        # a long text on a slow line: the queue sends the rest
            if task is None:
                if item.next_chunk == 0:
                    text = self._render(item)
                    if text is None:
                        return _DROP
                    item.chunks = self._chunks(text)
                    if item.edit_first is not None and item.queued:
                        # Going out late: now the message this outcome belongs to can be edited as well.
                        edited = await self._edit(*item.edit_first)
                        if not edited and self._down_since is not None:
                            return _RETRY            # no connection after all: try again later
                        item.edit_first = None
                task = item.task = self._track(asyncio.ensure_future(self._send_chunk(item)))
                item.task_started = time.monotonic()
            done, _ = await asyncio.wait({task}, timeout=timeout)
            if not done:
                if fresh and timeout >= SEND_TIMEOUT_SEC:
                    self._mark_down(f"no answer from Telegram within {timeout:.0f}s", since=item.task_started)
                return _RETRY            # (cut short by the deadline: the delivery task waits the rest)
            item.task = None
            if task.cancelled():
                return _RETRY
            exc = task.exception()
            if exc is not None:
                if not isinstance(exc, Exception):
                    raise exc
                return self._send_failed(item, exc)
            item.msg_id = getattr(task.result(), "id", None)
            item.next_chunk += 1
            self._round_trip_ok()
            if item.next_chunk >= len(item.chunks or ()):
                await self._delivered(item)
                return _SENT

    def _render(self, item: _Outgoing) -> str | None:
        """The text to send now; None: a question that must not be asked any more."""
        age = time.time() - item.created
        conf = item.conf
        if conf is not None:
            if item.cancelled:
                return None
            if not item.queued:
                return item.text
            now = datetime.now(timezone.utc)
            if (_utc(conf.expires_at) - now).total_seconds() < QUESTION_MIN_LEFT_SEC:
                return None              # too late to ask: close_confirmation() tells the owner about it
            if age < LATE_MARK_SEC:
                return item.text
            # "created now": the expiry line then shows the time that is really left.
            return late_question_prefix(age) + "\n" + format_confirmation(conf.model_copy(update={"created_at": now}),
                                                                         decider=item.decider)
        if item.queued and not item.report and age >= LATE_MARK_SEC:
            return late_notice_prefix(age) + "\n" + item.text
        return item.text

    @staticmethod
    def _chunks(text: str) -> list[Any]:
        try:
            return split_html(text)
        except Exception:
            log.exception("Could not parse outgoing HTML; sending as escaped text")
            return split_html(esc(text))

    async def _send_chunk(self, item: _Outgoing) -> Any:
        chunks = item.chunks or []
        i = item.next_chunk
        chunk = chunks[i]
        kwargs: dict[str, Any] = {
            "buttons": item.buttons if i == len(chunks) - 1 else None,
            "link_preview": False,
            "reply_to": item.reply_to if i == 0 else None,
        }
        if isinstance(chunk, str):
            return await self._client.send_message(self._owner, chunk, parse_mode="html", **kwargs)
        return await self._client.send_message(self._owner, chunk[0], formatting_entities=chunk[1], **kwargs)

    def _send_failed(self, item: _Outgoing, exc: Exception) -> str:
        self._log_send_failure(exc)
        seconds = getattr(exc, "seconds", None)
        if isinstance(exc, errors.FloodError) and seconds:
            self._hold(float(seconds) + FLOOD_MARGIN_SEC)   # Telegram asks us to wait: nothing is sent before
            return _RETRY
        if self._is_unreachable(exc):
            self._hold(UNREACHABLE_RETRY_SEC)         # until the owner writes to the bot, or this long
            return _RETRY
        if isinstance(exc, (ConnectionError, OSError, asyncio.TimeoutError)):
            self._mark_down(f"send failed: {type(exc).__name__}: {exc}")
            return _RETRY
        if isinstance(exc, _PERMANENT):
            return _DROP
        item.failures += 1
        if isinstance(exc, errors.RPCError) or item.failures < MAX_UNKNOWN_FAILURES:
            return _RETRY                             # server trouble / unknown: the delivery task backs off
        log.error("Giving up on a message to the owner after %d failed attempts: %.300s", item.failures, item.text)
        return _DROP

    async def _delivered(self, item: _Outgoing) -> None:
        self._unreachable_logged = False
        self._hold_until = 0.0
        if item.queued and not item.report:
            self._late += 1
            self._late_oldest = item.created if self._late_oldest is None else min(self._late_oldest, item.created)
        conf = item.conf
        if conf is not None:
            self._forget_question(item)
            if item.msg_id is not None and item.cancelled:
                # Closed while it was on its way: take the buttons off again and show the outcome.
                await self._edit(item.msg_id, f"{item.closed_html}\n\n{item.outcome or _outcome('')}")
            elif item.msg_id is not None:
                self._remember(self._sent, conf.id, (item.msg_id, item.closed_html))
                self._remember(self._by_msg, item.msg_id, conf.id)
        if item.on_sent is not None and item.msg_id is not None:
            try:
                item.on_sent(item.msg_id)
            except Exception:
                log.exception("on_sent callback failed")

    def _forget_question(self, item: _Outgoing) -> None:
        if item.conf is not None and self._questions.get(item.conf.id) is item:
            del self._questions[item.conf.id]

    def _track(self, task: asyncio.Task[Any]) -> asyncio.Task[Any]:
        self._send_tasks.add(task)
        task.add_done_callback(self._send_task_done)
        return task

    def _send_task_done(self, task: asyncio.Task[Any]) -> None:
        self._send_tasks.discard(task)
        if not task.cancelled():
            task.exception()             # looked at: a late failure of an abandoned request is not an error

    def _edit_request(self, msg_id: int, text: str, buttons: Any = None) -> Coroutine[Any, Any, Any]:
        """The Telethon request that replaces bot message ``msg_id`` by ``text`` (buttons removed, or ``buttons``)."""
        chunk = split_html(text, limit=4096)[-1]   # too long: keep the tail (it holds the outcome)
        if isinstance(chunk, str):
            return self._client.edit_message(
                self._owner, msg_id, chunk, parse_mode="html", buttons=buttons, link_preview=False
            )
        return self._client.edit_message(
            self._owner, msg_id, chunk[0], formatting_entities=chunk[1], buttons=buttons, link_preview=False
        )

    async def _edit(self, msg_id: int, text: str, *, buttons: Any = None) -> bool:
        """Replace a bot message (buttons removed, or ``buttons``). False if it could not be edited (bounded wait)."""
        if self._client is None:
            return False
        if self._link_down():
            return False                 # it would only time out; callers fall back to a (queued) message
        started = time.monotonic()
        try:
            await asyncio.wait_for(self._edit_request(msg_id, text, buttons), SEND_TIMEOUT_SEC)
            return True
        except errors.MessageNotModifiedError:
            return True
        except asyncio.TimeoutError:
            self._forget_abandoned_requests()    # or the edit would be applied after some later reconnect
            self._mark_down(f"no answer to an edit within {SEND_TIMEOUT_SEC:.0f}s", since=started)
            log.warning("Could not edit bot message %s: no answer from Telegram", msg_id)
            return False
        except Exception as exc:
            if isinstance(exc, (ConnectionError, OSError)):
                self._mark_down(f"edit failed: {type(exc).__name__}: {exc}")
            log.warning("Could not edit bot message %s: %s", msg_id, exc)
            return False

    async def _edit_or_send(self, msg_id: int, body: str, fallback: str) -> None:
        """``_edit(msg_id, body)``, and when that cannot be done ``fallback`` as a reply to that message which edits
        it first if it goes out late (as close_confirmation always did). The caller waits at most CALLER_WAIT_SEC:
        while messages are waiting the edit waits its turn in the queue, and an edit that got no answer in time is
        left to the delivery task with its request still running (see ``_Outgoing.edit``)."""
        if self._client is None or self._shut or self._link_down():
            # No edit is tried (it would only time out): the fallback is queued until Telegram answers, as before.
            await self._send(fallback, reply_to=msg_id, importance=2, edit_first=(msg_id, body), mirror=False)
            return
        item = _Outgoing(text=fallback, importance=2, reply_to=msg_id, edit_first=(msg_id, body), edit=(msg_id, body))
        if self._queue:
            self._enqueue(item)          # in order behind what is waiting; the caller does not wait
            return
        try:
            task = self._track(asyncio.ensure_future(self._edit_request(msg_id, body)))
        except Exception as exc:
            log.warning("Could not edit bot message %s: %s", msg_id, exc)
            await self._send(fallback, reply_to=msg_id, importance=2, edit_first=(msg_id, body))
            return
        started = time.monotonic()
        try:
            done, _ = await asyncio.wait({task}, timeout=self._caller_wait())
        except asyncio.CancelledError:
            item.edit_task, item.edit_started = task, started
            self._enqueue(item)          # the caller was cancelled meanwhile: the delivery task finishes the job
            raise
        if not done:
            item.edit_task, item.edit_started = task, started
            self._enqueue(item)          # no answer yet: the delivery task waits for it, in order
            return
        if not self._edit_answered(task, msg_id):
            await self._send(fallback, reply_to=msg_id, importance=2, edit_first=(msg_id, body))

    def _edit_answered(self, task: asyncio.Task[Any], msg_id: int) -> bool:
        """A finished edit request: True when the message shows the new text (also when it did so already)."""
        if task.cancelled():
            return False
        exc = task.exception()
        if exc is None or isinstance(exc, errors.MessageNotModifiedError):
            self._round_trip_ok()
            return True
        if isinstance(exc, (ConnectionError, OSError)):
            self._mark_down(f"edit failed: {type(exc).__name__}: {exc}")
        seconds = getattr(exc, "seconds", None)
        if isinstance(exc, errors.FloodError) and seconds:
            self._hold(float(seconds) + FLOOD_MARGIN_SEC)   # Telegram asks us to wait: the fallback goes after it
        log.warning("Could not edit bot message %s: %s", msg_id, exc)
        return False

    async def _queued_edit(self, item: _Outgoing) -> None:
        """One step of the delivery task for a queued edit (``_Outgoing.edit``): start it in its turn, or wait for
        its request (like for a message: SEND_TIMEOUT_SEC without an answer and the connection is taken for down);
        then the item is done, or goes on as its fallback message."""
        assert item.edit is not None
        task = item.edit_task
        if task is None:
            if self._link_down():
                if self._down_since is None:
                    self._mark_down("the client is disconnected")
                await self._pause(IDLE_RECHECK_SEC)    # the supervisor wakes us when Telegram answers again
                return
            wait = self._hold_until - time.monotonic()
            if wait > 0:
                await self._pause(wait)
                return
            try:
                item.edit_task = self._track(asyncio.ensure_future(self._edit_request(*item.edit)))
            except Exception as exc:
                log.warning("Could not edit bot message %s: %s", item.edit[0], exc)
                self._edit_fallback(item)
                return
            item.edit_started = time.monotonic()
            return
        if not task.done():
            left = item.edit_started + SEND_TIMEOUT_SEC - time.monotonic()
            if left > 0:
                await self._pause(left, task)
                return
            # No answer in time: given up as _edit() does (taken out of Telethon's hands, or it would be applied
            # after some later reconnect); the fallback edits the message again first once Telegram answers.
            task.cancel()
            item.edit_task = None
            self._forget_abandoned_requests()
            log.warning("Could not edit bot message %s: no answer from Telegram", item.edit[0])
            self._mark_down(f"no answer to an edit within {SEND_TIMEOUT_SEC:.0f}s", since=item.edit_started)
            self._edit_fallback(item)
            return
        item.edit_task = None
        if self._edit_answered(task, item.edit[0]):
            item.edit = None
            if time.time() - item.created < LATE_MARK_SEC:
                self._discard(item)      # edited: nothing else to send, as when the caller's own edit works
            else:
                # Edited only long after the outcome was known (an outage): the owner also gets the outcome as a
                # message of its own, marked as late, as after an edit that timed out; the edit itself is done.
                item.edit_first = None
        else:
            self._edit_fallback(item)

    def _edit_fallback(self, item: _Outgoing) -> None:
        """A queued edit that could not be done goes on as its message (one without a text is dropped)."""
        item.edit = None
        if not item.text.strip():
            self._discard(item)

    async def _flush_backlog(self) -> None:
        """The owner just wrote, so messages reach them: deliver what is waiting first (bounded wait)."""
        if not self._queue:
            return
        self._kick()
        pump = self._ensure_pump()
        if pump is not None and pump is not asyncio.current_task():
            await asyncio.wait({pump}, timeout=FLUSH_WAIT_SEC)

    @property
    def _backlog(self) -> list[str]:
        """Texts that are waiting to be delivered (older tools look at this name)."""
        return [item.text for item in self._queue]

    # ---- the queue ------------------------------------------------------------
    def _enqueue(self, item: _Outgoing) -> None:
        item.queued = True
        self._queue.append(item)
        # In the log as well: the queue lives in memory only (a crash before delivery loses it).
        log.info("Queued for the owner (%d waiting): %.200s", len(self._queue), item.text)
        while len(self._queue) > QUEUE_MAX:
            self._drop_one()
        self._ensure_pump()

    def _drop_one(self) -> None:
        """Queue full: throw away the least important message, the oldest of them first
        (INFO notices and plain replies, then warnings, then the rest), and write it to the log."""
        victim = min(self._queue, key=lambda it: (it.importance, it.created))
        self._queue.remove(victim)
        running = [t for t in (victim.task, victim.edit_task) if t is not None]
        for task in running:
            task.cancel()
        if running:
            self._forget_abandoned_requests()
        self._forget_question(victim)
        self._dropped += 1
        self._dropped_total += 1
        log.warning(
            "Too many messages are waiting for the owner (limit %d): dropped the oldest least important one: %.300s",
            QUEUE_MAX, victim.text,
        )

    def _discard(self, item: _Outgoing) -> None:
        with contextlib.suppress(ValueError):
            self._queue.remove(item)

    def _hold(self, seconds: float) -> None:
        self._hold_until = max(self._hold_until, time.monotonic() + seconds)

    def _kick(self) -> None:
        """Try the queue now (the owner wrote to the bot / shutdown): forget back-off and waits."""
        self._hold_until = 0.0
        self._wake.set()

    def _ensure_pump(self) -> asyncio.Task[Any] | None:
        if self._shut:
            return self._pump_task
        task = self._pump_task
        if task is None or task.done():
            task = self._pump_task = asyncio.ensure_future(self._pump())
            task.add_done_callback(self._pump_done)
        return task

    def _pump_done(self, task: asyncio.Task[Any]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            log.error("Notifier delivery task failed; restarting it", exc_info=exc)
            if not self._shut:
                asyncio.get_running_loop().call_later(RETRY_BASE_SEC, self._ensure_pump)
        elif self._queue and not self._shut:
            self._ensure_pump()

    async def _pump(self) -> None:
        """Deliver the queue in order, one message at a time. Ends when nothing is left
        (the next queued message starts it again)."""
        delay = RETRY_BASE_SEC
        while not self._shut:
            if not self._queue:
                report = self._take_report()
                if report is None:
                    return
                self._queue.append(report)
                continue
            item = self._queue[0]
            if item.edit is not None:    # an edit nobody waits for: done (or given up) in its turn
                try:
                    await self._queued_edit(item)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Never block the queue for ever: the item goes on as its message (the outcome as a reply).
                    log.exception("A queued edit of bot message %s failed; its message is sent instead", item.edit)
                    if item.edit_task is not None:
                        item.edit_task.cancel()
                        item.edit_task = None
                        self._forget_abandoned_requests()
                    self._edit_fallback(item)
                continue
            task = item.task
            if task is not None and not task.done():
                # Asked earlier, no answer so far: Telethon still holds the request and sends it once the
                # connection is back, so wait for that answer instead of sending the text a second time.
                now = time.monotonic()
                if self._down_since is not None:
                    await self._pause(IDLE_RECHECK_SEC, task)
                elif item.task_started >= self._up_since:
                    # The caller stopped waiting early (its time was up): the request gets its full time here.
                    left = item.task_started + SEND_TIMEOUT_SEC - now
                    if left > 0:
                        await self._pause(left, task)
                    else:
                        self._mark_down(f"no answer from Telegram within {SEND_TIMEOUT_SEC:.0f}s",
                                        since=item.task_started)
                elif now - self._up_since >= STUCK_SEND_SEC:
                    # The connection demonstrably works again and the old request is still unanswered:
                    # give it up and send the message again.
                    log.warning("A message to the owner got no answer although the connection works: sending it again")
                    task.cancel()
                    item.task = None
                    self._forget_abandoned_requests()    # or Telethon re-sends the old request later: a 2nd copy
                else:
                    await self._pause(STUCK_SEND_SEC - (now - self._up_since), task)
                continue
            if task is None:
                if item.cancelled:       # a question that was closed before it could be asked
                    self._discard(item)
                    continue
                if self._link_down():
                    if self._down_since is None:
                        self._mark_down("the client is disconnected")
                    await self._pause(IDLE_RECHECK_SEC)    # the supervisor wakes us when Telegram answers again
                    continue
                wait = self._hold_until - time.monotonic()
                if wait > 0:
                    await self._pause(wait)
                    continue
            try:
                outcome = await self._attempt(item)
            except asyncio.CancelledError:
                raise
            except Exception:
                # One message that cannot be handled must not block everything behind it for ever.
                log.exception("A message for the owner could not be processed and is dropped: %.300s", item.text)
                self._dropped += 1
                self._dropped_total += 1
                outcome = _DROP
            if outcome == _RETRY:
                if item.task is None and self._down_since is None and self._hold_until <= time.monotonic():
                    self._hold(delay)    # an error that may pass: back off
                    delay = min(delay * 2, RETRY_MAX_SEC)
                continue
            delay = RETRY_BASE_SEC
            self._discard(item)
            if outcome == _DROP:
                self._forget_question(item)

    async def _pause(self, timeout: float | None, task: asyncio.Task[Any] | None = None) -> None:
        """Sleep until woken (connection back, owner wrote, shutdown), ``task`` ends or ``timeout`` passes."""
        self._wake.clear()
        waiter = asyncio.ensure_future(self._wake.wait())
        try:
            await asyncio.wait(
                {waiter} if task is None else {waiter, task},
                timeout=None if timeout is None else max(timeout, 0.0),
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            waiter.cancel()

    def _take_report(self) -> _Outgoing | None:
        """After delayed messages went out: one message that says how long Telegram could not be
        reached and how many messages were late (or had to be dropped)."""
        if self._down_since is not None:
            return None                  # still down: _mark_up() starts the delivery task again
        late, dropped, outage = self._late, self._dropped, self._outage_total
        oldest_age = time.time() - self._late_oldest if self._late_oldest is not None else None
        noticeable = late and oldest_age is not None and oldest_age >= LATE_MARK_SEC
        self._late, self._late_oldest, self._dropped, self._outage_total = 0, None, 0, 0.0
        # Worth a message: something was thrown away; a connection loss that delayed something or was
        # long; messages that were clearly late for another reason (flood wait, owner unreachable).
        delayed_by_outage = late and outage >= REPORT_MIN_DELAY_SEC
        if not (dropped or delayed_by_outage or outage >= REPORT_MIN_OUTAGE_SEC or noticeable):
            return None
        text = format_delivery_report(
            outage_sec=outage if outage >= min(REPORT_MIN_DELAY_SEC, REPORT_MIN_OUTAGE_SEC) else None,
            late=late,
            oldest_age_sec=oldest_age if noticeable else None,
            dropped=dropped,
        )
        return _Outgoing(text=text, importance=1, report=True) if text else None

    def _forget_abandoned_requests(self) -> None:
        """Take requests nobody waits for any more (cancelled) out of Telethon's send queue and out of
        its list of unanswered requests; otherwise Telethon sends them after its next reconnect.

        Best effort: this looks into Telethon's sender (``_send_queue``, ``_pending_state``), so every
        step is guarded. If those names ever differ nothing happens here, and the worst case is the
        late duplicate / late edit described above."""
        try:
            sender = getattr(self._client, "_sender", None)
            waiting = getattr(getattr(sender, "_send_queue", None), "_deque", None)
            unanswered = getattr(sender, "_pending_state", None)
            if waiting is not None:
                for state in [st for st in list(waiting) if _abandoned(st)]:
                    waiting.remove(state)
            if isinstance(unanswered, dict):
                for key in [k for k, st in list(unanswered.items()) if _abandoned(st)]:
                    del unanswered[key]
        except Exception as exc:
            log.debug("Could not clean abandoned requests: %s", exc)

    # ---- connection state -------------------------------------------------------
    def _client_connected(self) -> bool:
        """False only when the client itself says it is disconnected (Telethon gave up / was stopped)."""
        check = getattr(self._client, "is_connected", None)
        if not callable(check):
            return True
        try:
            return bool(check())
        except Exception:
            return True

    def _link_down(self) -> bool:
        return self._down_since is not None or not self._client_connected()

    def _mark_down(self, reason: str, since: float | None = None) -> None:
        """Telegram does not answer: from now on messages are queued and nobody waits.
        ``since``: when the request that got no answer was made (the outage is counted from there)."""
        if self._shut:
            return
        if self._down_since is None:
            self._down_since = time.monotonic() if since is None else max(since, self._up_since)
            if self._probe_task is not None:     # an answer from before this outage proves nothing
                self._probe_task.cancel()
                self._probe_task = None
                self._forget_abandoned_requests()
            log.warning("Owner bot: connection problem (%s); messages are queued until Telegram answers again", reason)
        self._ensure_supervisor()
        self._sup_wake.set()

    def _mark_up(self) -> None:
        if self._down_since is None:
            return
        now = time.monotonic()
        lasted = now - self._down_since
        self._down_since = None
        self._up_since = now
        self._outage_total += lasted
        self._outage_count += 1
        self._last_outage = (now, lasted)
        log.warning("Owner bot: Telegram answers again after %.0fs; %d message(s) waiting", lasted, len(self._queue))
        self._wake.set()
        self._ensure_pump()              # delivers the queue, then the report about the outage

    def _round_trip_ok(self) -> None:
        if self._down_since is not None:
            self._mark_up()

    def _no_hidden_flood_sleep(self) -> None:
        # Telethon would sleep through a FloodWait of up to 60 s inside the call; here a flood wait is
        # handled by the queue (the caller returns, the message goes out when the wait is over).
        with contextlib.suppress(Exception):
            self._client.flood_sleep_threshold = 0

    def _ensure_supervisor(self) -> None:
        if self._shut:
            return
        if self._sup_task is None or self._sup_task.done():
            self._sup_task = asyncio.ensure_future(self._supervise())

    async def _supervise(self) -> None:
        """Watch the bot's connection: reconnect (with back-off) when the client is disconnected, and
        find out when a connection that stopped answering works again."""
        delay = RECONNECT_BASE_SEC
        while not self._shut:
            try:
                await asyncio.wait_for(self._sup_wake.wait(), SUPERVISE_EVERY_SEC)
            except asyncio.TimeoutError:
                pass
            self._sup_wake.clear()
            if self._shut or self._client is None:
                break
            try:
                if not self._client_connected():
                    self._mark_down("the client is disconnected")
                    if await self._reconnect():
                        delay = RECONNECT_BASE_SEC
                        self._mark_up()
                    else:
                        log.warning("Owner bot: next reconnect attempt in %.0fs", delay)
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, RECONNECT_MAX_SEC)
                        self._sup_wake.set()
                elif self._down_since is not None:
                    now = time.monotonic()
                    if await self._probe():
                        self._mark_up()
                    elif now - self._down_since >= FORCE_RECONNECT_AFTER_SEC and now >= self._next_forced:
                        # Connected on paper but silent for minutes: do not rely on Telethon's own
                        # reconnect any longer, build the connection anew.
                        self._next_forced = now + FORCE_RECONNECT_AFTER_SEC
                        log.warning("Owner bot: no answer from Telegram for %.0fs; reconnecting from scratch",
                                    now - self._down_since)
                        if await self._reconnect():
                            self._mark_up()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Notifier supervisor step failed (it keeps running)")

    async def _probe(self) -> bool:
        """Does Telegram answer again? One small request ("who am I"). At most one is on its way and,
        like a send, it is not cancelled when no answer comes: it is answered as soon as the
        connection is back, which is exactly what we want to learn."""
        get_me = getattr(self._client, "get_me", None)
        if get_me is None:
            return True
        task = self._probe_task
        if task is None:
            task = self._probe_task = self._track(asyncio.ensure_future(get_me()))
        done, _ = await asyncio.wait({task}, timeout=SEND_TIMEOUT_SEC)
        if not done:
            return False
        if self._probe_task is task:
            self._probe_task = None
        if task.cancelled():
            return False
        exc = task.exception()
        if exc is None or isinstance(exc, errors.RPCError):
            return True                  # any answer from Telegram means the connection works
        log.debug("Owner bot connection check failed: %s: %s", type(exc).__name__, exc)
        return False

    async def _reconnect(self) -> bool:
        """Full reconnect of the bot client (bounded). True when Telegram answers again."""
        client = self._client
        await self._disconnect_client()  # also stops Telethon's own loops; harmless when already down
        try:
            # connect + "who am I" (+ a new bot login if the session was lost)
            await asyncio.wait_for(client.start(bot_token=self._config.bot_token), RECONNECT_TRY_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            detail = str(exc) or f"no answer within {RECONNECT_TRY_SEC:.0f}s"
            log.warning("Owner bot: reconnect failed (%s: %s)", type(exc).__name__, detail)
            await self._disconnect_client()      # never leave a half-connected client behind
            return False
        self._no_hidden_flood_sleep()
        log.warning("Owner bot: reconnected to Telegram")
        return True

    async def _disconnect_client(self) -> None:
        client = self._client
        if client is None:
            return
        try:
            result = client.disconnect()
            if inspect.isawaitable(result):
                await asyncio.wait_for(result, DISCONNECT_TIMEOUT_SEC)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Error while disconnecting the bot client")

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
