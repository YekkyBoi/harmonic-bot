"""When a report card goes out: the weekly schedule, its stored cut-off, and the owner's ``/karne`` command.

* **Schedule.** One small task (started and stopped by ``app.serve`` like its other watch tasks) sleeps until the
  next Sunday 22:00 UK time (:mod:`.uktime`; in steps of at most MAX_SLEEP_SEC, so a changed system clock or a
  machine that slept is noticed) and then sends the card of the period from the previous card's cut-off to that
  moment.
* **Stored state** (``state`` table, key STATE_KEY): ``handled`` - the last Sunday 22:00 that was dealt with (card
  sent, or skipped) - and ``cutoff`` - the end of the period of the last card that was sent. It is written BEFORE
  the card's first message goes out, so a restart never sends a card twice (the price: a bot that dies in the moment
  between the two does not send that card; its trades are then missing from the weekly cards and are on
  ``/karne hepsi`` only).
* **A missed card** (the bot was not running at 22:00): sent at the next start when that is less than MAX_LATE_SEC
  (24 h) late, with a line that says so; later than that it is skipped with ONE line to the owner, ``cutoff`` stays
  where it was and the next card covers the skipped week as well (it says how many weeks it is about). Nothing is
  ever dropped silently.
* **The first start with this feature** only notes the last Sunday 22:00 as the cut-off: no card, no line (nothing
  was missed). The first card is the one of the following Sunday.
* **Each trade once, whole.** A trade that has ended while an optional account's open order is still unanswered
  changes when that order answers (the engine books the late position, closes it and gives the trade a new end
  time): on its card it would lack that account, and it would be on the next card again. ``pending`` (the engine's
  own look at its orders in flight; read only) keeps such a trade off the card; its id is stored (``carry``) and
  the next weekly card takes it (``waited`` counts the cards it was left off: after MAX_DEFERRALS of them it is
  listed anyway, with a line saying its late account's result may be missing). The ids of the trades the last cards listed are stored as well (``listed``, at
  most LISTED_MAX) and are never listed on a weekly card again, whatever moved their end time.
* **/karne** (the week so far) and **/karne hepsi** (every trade so far) send a card on demand and change nothing
  that is stored. One card is made at a time, whoever asked (the schedule or the owner), and a command is accepted
  at most once in COMMAND_GAP_SEC: the command can never start a second drawing process next to a running one, nor
  a chain of them. The command itself answers at once; the card follows from a task of its own (the notifier's
  command handler - which counts as "trading busy" while it runs - is never held).
* **Never in the way of trading**: database reads only, the drawing in another process (:mod:`.render`), the upload
  through the notifier's own bounded request (``BotNotifier.send_owner_document``: only with a native AES, paused
  while the trading path is busy, never cancelled nor sent twice), every failure a text card instead. Nothing here
  raises into the bot; :meth:`KarneService.stop` returns within STOP_WAIT_SEC.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import threading
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from ..config import fold_ascii
from ..engine.prices import stray_cancel
from ..models import Severity
from ..telegram.formatting import esc
from . import PILLOW
from .data import MODE_ALL, MODE_INTERIM, MODE_WEEK, Report, build_report, caption, render_data, summary_text
from .render import (
    FAILED,
    NO_PILLOW,
    RENDER_TIMEOUT_SEC,
    TIMEOUT,
    RenderResult,
    packages_dir,
    pillow_installed,
    remove_picture,
    render_card,
)
from .uktime import UTC, as_utc, last_slot, next_slot, previous_slot, stamp_tr, to_uk

logger = logging.getLogger(__name__)

# storage state: {"handled": iso, "cutoff": iso, "note": str, "at": iso, "carry": [trade ids], "listed": [trade ids]}
STATE_KEY = "karne"
LISTED_MAX = 500                  # ids of the trades of the last weekly cards that are remembered (newest first)
TEMP_DIR = "karne"                # under data_dir: the card's temporary files (removed after each card)
MAX_LATE_SEC = 24 * 3600.0        # a missed weekly card is still sent this long after its time
LATE_NOTE_SEC = 300.0             # a card sent this much after its time says that it is late
MAX_SLEEP_SEC = 300.0             # the schedule looks at the clock at least this often
MIN_SLEEP_SEC = 1.0
ERROR_RETRY_SEC = 300.0           # after a failed look / card (e.g. the database): the next try
COMMAND_GAP_SEC = 60.0            # /karne is accepted at most once in this time
STOP_WAIT_SEC = 5.0               # stop() waits this long for its tasks to end
STALE_FILE_SEC = 3600.0           # temporary files older than this are removed at start (left by a crash)

REPLY_STARTED = "⏳ Karne hazırlanıyor; birkaç saniye içinde gelecek."
REPLY_STARTED_ALL = "⏳ Tüm işlemlerin karnesi hazırlanıyor; birkaç saniye içinde gelecek."
REPLY_BUSY = "⏳ Bir karne zaten hazırlanıyor; o gelince tekrar isteyebilirsiniz."
REPLY_WAIT = "⏳ Az önce bir karne istendi; {seconds} sn sonra tekrar deneyin."
REPLY_STOPPING = "⏳ Bot kapanıyor; karne şu an hazırlanamıyor."
REPLY_USAGE = ("Kullanım: <code>/karne</code> (bu hafta, şu ana kadar) · <code>/karne hepsi</code> (bugüne kadarki "
               "tüm işlemler)")
NO_PILLOW_TR = ("ℹ️ Görsel karne için Pillow paketi kurulu değil; bu yüzden karne metin olarak gönderildi. Görsel "
                "için şunu çalıştırın (bot açık kalabilir; paket botun kendi Python ortamına değil, yalnızca karne "
                "çizimi için ayrı bir klasöre kurulur; PowerShell'e olduğu gibi yapıştırılabilir): "
                "<code>&amp; {python} -m pip install --upgrade --no-deps --only-binary=:all: --target {folder} "
                "{pillow}</code>")
RENDER_FAILED_TR = "⚠️ Karne görseli hazırlanamadı ({why}); bu yüzden karne metin olarak gönderildi."
NOT_SENT_TR = "⚠️ Karne görseli gönderilemedi ({why}); bu yüzden karne metin olarak gönderildi."
SKIPPED_TR = ("ℹ️ Haftalık karne gönderilmedi: bot {slot} (İngiltere saati) sırasında çalışmıyordu ve üzerinden 24 "
              "saatten fazla geçti. O haftanın işlemleri bir sonraki karnede yer alacak; şu ana kadarki durum için "
              "/karne yazabilirsiniz.")
FAILED_TR = "🚨 Karne hazırlanamadı: <code>{error}</code>"
REPLY_NO_ACCOUNT = "Size ait bir hesap yok; karne hazırlanamadı."
OWNER, FRIEND = "owner", "friend"         # whose card (the audiences)
_ALL_WORDS = ("hepsi", "hepsini", "tum", "tumu", "all")


def _parse(value: object) -> datetime | None:
    try:
        return as_utc(datetime.fromisoformat(str(value))) if value else None
    except ValueError:
        return None


def _ids(value: object) -> list[int]:
    """Trade ids as stored in the state (anything else: none)."""
    if not isinstance(value, (list, tuple)):
        return []
    return [item for item in value if isinstance(item, int) and not isinstance(item, bool)]


def _counts(value: object) -> dict[int, int]:
    """``{trade id: weekly cards it was left off}`` as stored (JSON keys are text); anything else: none."""
    if not isinstance(value, dict):
        return {}
    out: dict[int, int] = {}
    for key, count in value.items():
        try:
            if isinstance(count, int) and not isinstance(count, bool):
                out[int(key)] = count
        except (TypeError, ValueError):
            continue
    return out


def file_name(report: Report) -> str:
    """The name the owner sees on the file: "karne_2026-11-15.png" (the UK date the period ends on)."""
    kind = {MODE_ALL: "tum_", MODE_INTERIM: "ara_"}.get(report.mode, "")
    return f"karne_{kind}{to_uk(report.end).date().isoformat()}.png"


class KarneService:
    def __init__(
        self,
        db: Any,
        notifier: Any,
        data_dir: str | Path,
        *,
        accounts: Sequence[str] = (),
        friend_accounts: Sequence[str] = (),
        busy: Callable[[], bool] | None = None,
        pending: Callable[[int], bool] | None = None,
        clock: Callable[[], datetime] | None = None,
        sleep: Callable[[float], Awaitable[Any]] | None = None,
        renderer: Callable[..., Awaitable[RenderResult]] = render_card,
        pillow: Callable[[], bool] = pillow_installed,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        """``accounts``: the owner's configured accounts that trade real money, in the order they are shown (his card:
        only these, one row per account in a trade); ``friend_accounts``: the friend's (his own card, sent to his chat
        when the notifier has it; never on the owner's card); ``busy``: true
        while the trading path is working (the upload pauses); ``pending(trade_id)``: an open order of that trade is
        still in flight on some account (the trade waits for the next card). The rest replaces the outside world in
        tests."""
        self._db = db
        self._notifier = notifier
        self._folder = Path(data_dir) / TEMP_DIR
        self._accounts = list(accounts)
        self._friend_accounts = [a for a in friend_accounts if a not in self._accounts]
        self._busy = busy
        self._pending = pending
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleep = sleep or asyncio.sleep
        self._renderer = renderer
        self._pillow = pillow
        self._monotonic = monotonic
        self._task: asyncio.Task[None] | None = None
        self._jobs: set[asyncio.Task[None]] = set()
        self._lock = asyncio.Lock()                 # one card at a time (the schedule's or the owner's)
        # the friend's own /karne: a lock of its own (his card never holds back the owner's or the weekly one)
        self._friend_lock = asyncio.Lock()
        self._job_audience: dict[asyncio.Task[None], str] = {}
        self._handled: datetime | None = None       # the stored "handled", once read (no read every few minutes)
        self._last_command: dict[str, float] = {}   # monotonic time of the last accepted /karne, per audience
        self._stopping = False

    # ---- lifecycle ----------------------------------------------------------------------------------
    def start(self) -> None:
        """Start the schedule (idempotent). Returns at once."""
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        threading.Thread(target=self._purge_stale, name="karne-purge", daemon=True).start()
        self._task = asyncio.ensure_future(self._run())
        self._task.set_name("karne-schedule")

    async def stop(self) -> None:
        """No more cards; a card that is being made is given up (its drawing process is killed). Returns within
        STOP_WAIT_SEC."""
        self._stopping = True
        tasks = [t for t in (self._task, *self._jobs) if t is not None and not t.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=STOP_WAIT_SEC)
        self._task = None

    def _purge_stale(self) -> None:
        with contextlib.suppress(OSError):
            limit = time.time() - STALE_FILE_SEC
            for path in self._folder.glob("karne_*"):
                with contextlib.suppress(OSError):
                    if path.is_file() and path.stat().st_mtime < limit:
                        path.unlink()

    def _now(self) -> datetime:
        return as_utc(self._clock())

    # ---- the schedule -------------------------------------------------------------------------------
    async def _run(self) -> None:
        while not self._stopping:
            try:
                try:
                    delay = await self._tick()
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - the schedule never ends by itself, and never takes the bot along
                    logger.exception("Karne: the schedule's look at the clock failed; next try in %.0f s",
                                     ERROR_RETRY_SEC)
                    delay = ERROR_RETRY_SEC
                await self._sleep(max(MIN_SLEEP_SEC, min(delay, MAX_SLEEP_SEC)))
            except asyncio.CancelledError:
                # Only a cancellation that was ASKED for (stop()) ends the schedule. One that comes out of a
                # library call (nobody cancelled this task) is a failure of that call: without this the task
                # would end without a word and no weekly card would follow until the next restart.
                if self._stopping or not stray_cancel():
                    raise
                logger.error("Karne: a CancelledError nobody asked for came out of the schedule; it goes on",
                             exc_info=True)
                await asyncio.sleep(MIN_SLEEP_SEC)

    def _until_next(self) -> float:
        now = self._now()
        return (next_slot(now) - now).total_seconds()

    async def _tick(self) -> float:
        """One look at the clock; sends the weekly card when it is due. Returns the seconds until the next look is
        needed at the latest."""
        now = self._now()
        slot = last_slot(now)
        if self._handled is not None and self._handled >= slot:
            return self._until_next()
        state = await self._state()
        handled = _parse(state.get("handled"))
        if handled is None:
            # The first start with this feature: nothing was missed. The first card is next Sunday's, from here on.
            await self._save(slot, slot, "init", state)
            logger.info("Karne: first start; the first weekly card is due at %s UTC", next_slot(now).isoformat())
        elif handled >= slot:
            self._handled = handled
        else:
            late = (now - slot).total_seconds()
            if late > MAX_LATE_SEC:
                await self._save(slot, _parse(state.get("cutoff")), "skipped", state)    # first: the line goes out once
                logger.warning("Karne: the card of %s UTC was missed by %.1f h; skipped", slot.isoformat(), late / 3600)
                await self._tell(SKIPPED_TR.format(slot=stamp_tr(slot)))
            else:
                await self._weekly(slot, state, late)
        return self._until_next()

    async def _weekly(self, slot: datetime, state: dict[str, Any], late: float) -> None:
        async with self._lock:
            cutoff = _parse(state.get("cutoff"))
            start = cutoff if cutoff is not None and cutoff < slot else previous_slot(slot)
            carried, listed = _ids(state.get("carry")), _ids(state.get("listed"))
            waited = _counts(state.get("waited"))
            # One card per person, both of the same trades: the same period, and whether a trade still waits for an
            # order is looked at ONCE for both (an answer arriving between the two must not list a trade on one card
            # and leave it for later on the other).
            looked: dict[int, bool] = {}
            source = self._pending

            def pending(trade_id: int) -> bool:
                if trade_id not in looked:
                    looked[trade_id] = bool(source(trade_id)) if source is not None else False
                return looked[trade_id]

            cards: list[tuple[str, Report]] = []
            for audience in self._audiences():
                cards.append((audience, await build_report(
                    self._db, start, slot, mode=MODE_WEEK, accounts=self._accounts_of(audience),
                    late=late > LATE_NOTE_SEC, pending=pending if source is not None else None, carried=carried,
                    listed=listed, waited=waited, **self._scope(audience))))
            report = cards[0][1]
            shown = list(dict.fromkeys(row.trade_id for _audience, card in cards for row in card.rows))
            # The owner's picture is drawn first: a stop while it is drawn leaves nothing stored, and the card goes
            # out after the restart. (The friend's is drawn after the owner's card went out: it never holds that back;
            # a stop while it is drawn loses his picture of this week.)
            picture = await self._prepare(report)
            try:
                # Stored BEFORE the first message: whatever happens from here on, these cards are never sent again,
                # and neither is one of their trades (``listed``); the trades they left for later are remembered.
                await self._save(slot, slot, "sent", {
                    "carry": report.deferred,
                    "waited": {str(i): waited.get(i, 0) + 1 for i in report.deferred},
                    "listed": (shown + listed)[:LISTED_MAX]})
            except BaseException:
                remove_picture(picture.path if picture is not None else None)
                raise
            for index, (audience, card) in enumerate(cards):
                logger.info("Karne: weekly card (%s) %s .. %s UTC: %d row(s), total %.2fR", audience,
                            start.isoformat(), slot.isoformat(), len(card.rows), card.total_r)
                await self._send(card, picture if index == 0 else await self._prepare(card), audience)

    # ---- the owner's command ------------------------------------------------------------------------
    def _audiences(self) -> list[str]:
        """Who gets a card now: the owner always; the friend when he has an account and the notifier knows his chat."""
        has_friend = getattr(self._notifier, "has_friend", None)
        friend = bool(self._friend_accounts) and callable(has_friend) and bool(has_friend())
        return [OWNER, FRIEND] if friend else [OWNER]

    def _accounts_of(self, audience: str) -> list[str]:
        return self._friend_accounts if audience == FRIEND else self._accounts

    def _scope(self, audience: str) -> dict[str, Any]:
        """Whose rows: the friend's card only his accounts; the owner's every account but the friend's."""
        if audience == FRIEND:
            return {"only": set(self._friend_accounts)}
        return {"exclude": set(self._friend_accounts)}

    async def command(self, args: str = "", audience: str = OWNER) -> str:
        """``/karne`` / ``/karne hepsi`` of the owner (``audience`` FRIEND: of the friend, his own card): the answer
        for now (HTML); the card itself follows. Never raises."""
        if audience == FRIEND and not self._friend_accounts:
            return REPLY_NO_ACCOUNT
        word = fold_ascii(args or "")
        if word and word not in _ALL_WORDS:
            return REPLY_USAGE
        if self._stopping:
            return REPLY_STOPPING
        lock = self._friend_lock if audience == FRIEND else self._lock
        if lock.locked() or any(not job.done() and self._job_audience.get(job) == audience for job in self._jobs):
            return REPLY_BUSY
        now = self._monotonic()
        last = self._last_command.get(audience)
        if last is not None and now - last < COMMAND_GAP_SEC:
            return REPLY_WAIT.format(seconds=max(1, round(COMMAND_GAP_SEC - (now - last))))
        self._last_command[audience] = now
        mode = MODE_ALL if word else MODE_INTERIM
        job: asyncio.Task[None] = asyncio.ensure_future(self._on_demand(mode, audience))
        job.set_name(f"karne-{mode}-{audience}")
        self._jobs.add(job)
        self._job_audience[job] = audience
        job.add_done_callback(self._job_done)
        return REPLY_STARTED_ALL if mode == MODE_ALL else REPLY_STARTED

    def _job_done(self, job: asyncio.Task[None]) -> None:
        self._jobs.discard(job)
        self._job_audience.pop(job, None)
        if not job.cancelled() and job.exception() is not None:
            logger.error("Karne: an on-demand card failed", exc_info=job.exception())

    async def _on_demand(self, mode: str, audience: str = OWNER) -> None:
        try:
            async with self._friend_lock if audience == FRIEND else self._lock:
                now = self._now()
                start: datetime | None = None
                carried: list[int] = []
                listed: list[int] = []
                waited: dict[int, int] = {}
                if mode == MODE_INTERIM:            # what the next weekly card would say, so far
                    state = await self._state()
                    cutoff = _parse(state.get("cutoff"))
                    start = cutoff if cutoff is not None and cutoff < now else last_slot(now)
                    carried, listed = _ids(state.get("carry")), _ids(state.get("listed"))
                    waited = _counts(state.get("waited"))
                report = await build_report(self._db, start, now, mode=mode, accounts=self._accounts_of(audience),
                                            pending=self._pending, carried=carried, listed=listed,
                                            waited=waited if mode == MODE_INTERIM else None, **self._scope(audience))
                await self._send(report, await self._prepare(report), audience)
        except asyncio.CancelledError:
            if self._stopping or not stray_cancel():
                raise
            # (out of a library call, nobody cancelled this task: a failure like any other, told to the owner)
            logger.error("Karne: a CancelledError nobody asked for ended the card asked for with /karne",
                         exc_info=True)
            await self._tell(FAILED_TR.format(error="CancelledError"), audience)
        except Exception as exc:      # noqa: BLE001 - told to the owner, never raised into the bot
            logger.exception("Karne: the card asked for with /karne could not be made")
            await self._tell(FAILED_TR.format(error=esc(str(exc) or type(exc).__name__)), audience)

    # ---- making and sending one card ----------------------------------------------------------------
    async def _prepare(self, report: Report) -> RenderResult | None:
        """The card's picture, drawn by the other process. None: this notifier sends no files (the text card).
        Without Pillow no process is started at all."""
        if not callable(getattr(self._notifier, "send_owner_document", None)):
            return None
        try:
            if not self._pillow():
                return RenderResult(NO_PILLOW, detail="Pillow is not installed")
            return await self._renderer(render_data(report), self._folder)
        except asyncio.CancelledError:
            raise
        except Exception as exc:      # noqa: BLE001
            logger.exception("Karne: drawing failed unexpectedly")
            return RenderResult(FAILED, detail=repr(exc))

    async def _send(self, report: Report, picture: RenderResult | None, audience: str = OWNER) -> None:
        """The picture as a file; the text card when there is no picture or Telegram did not take it. ``audience``:
        whose card it is and to whom it goes (the owner's never to the friend, the friend's to his chat only)."""
        why = ""
        send_doc = getattr(self._notifier, "send_friend_document" if audience == FRIEND else "send_owner_document",
                           None)
        if picture is not None and picture.ok and not callable(send_doc):
            remove_picture(picture.path)
            picture = None
        if picture is not None and picture.ok:
            try:
                problem = await send_doc(
                    str(picture.path), caption=caption(report), name=file_name(report), busy=self._busy)
            except asyncio.CancelledError:
                raise
            except Exception as exc:      # noqa: BLE001
                logger.exception("Karne: sending the picture failed unexpectedly")
                problem = str(exc) or type(exc).__name__
            finally:
                remove_picture(picture.path)
            if problem is None:
                return
            logger.warning("Karne: the picture was not sent (%s); sending the text card", problem)
            why = NOT_SENT_TR.format(why=esc(problem))
        elif picture is not None and picture.status == NO_PILLOW:
            logger.warning("Karne: Pillow is not installed; sending the text card")
            why = NO_PILLOW_TR.format(python=esc(_quoted(sys.executable or "python")),
                                      folder=esc(_quoted(str(packages_dir()))), pillow=PILLOW)    # (& "..." : PowerShell)
        elif picture is not None:
            reason = (f"{RENDER_TIMEOUT_SEC:.0f} sn içinde bitmedi, çizim durduruldu" if picture.status == TIMEOUT
                      else f"çizim hatası: <code>{esc(picture.detail[:200])}</code>")
            why = RENDER_FAILED_TR.format(why=reason)
        if audience == FRIEND and why:
            # (why the picture did not go - install paths, the bot's own queue - is the owner's business)
            logger.warning("Karne: the friend's card went as text: %s", why)
            why = ""
        await self._tell(summary_text(report, why), audience)

    async def _tell(self, text: str, audience: str = OWNER) -> None:
        """A message for the owner alone (the friend's mirror gets no card); ``audience`` FRIEND: for the friend
        alone. Never raises."""
        try:
            if audience == FRIEND:
                tell_friend = getattr(self._notifier, "tell_friend", None)
                if callable(tell_friend):
                    await tell_friend(text)
                return
            tell = getattr(self._notifier, "tell_owner", None)
            if callable(tell):
                await tell(text)
            else:
                await self._notifier.notify(text, Severity.INFO)
        except asyncio.CancelledError:
            raise
        except Exception:      # noqa: BLE001
            logger.exception("Karne: could not tell the owner: %.200s", text)

    # ---- stored state -------------------------------------------------------------------------------
    async def _state(self) -> dict[str, Any]:
        state = await self._db.get_state(STATE_KEY, None)
        return state if isinstance(state, dict) else {}

    async def _save(self, handled: datetime, cutoff: datetime | None, note: str, trades: dict[str, Any]) -> None:
        """``trades``: where the trade ids to remember come from (``carry``, ``listed``): the state as it was (a
        card that was not sent changes neither), or the new lists of a card that is about to go out."""
        await self._db.set_state(STATE_KEY, {
            "handled": handled.isoformat(), "cutoff": cutoff.isoformat() if cutoff is not None else None,
            "note": note, "at": self._now().isoformat(),
            "carry": _ids(trades.get("carry")), "listed": _ids(trades.get("listed")),
            "waited": {str(i): n for i, n in _counts(trades.get("waited")).items() if i in _ids(trades.get("carry"))}})
        self._handled = handled


def _quoted(path: str) -> str:
    """In double quotes always: PowerShell runs a quoted program only after "&", and a path may have spaces."""
    return f'"{path}"'


__all__ = ["COMMAND_GAP_SEC", "FRIEND", "LISTED_MAX", "MAX_LATE_SEC", "OWNER", "STATE_KEY", "KarneService",
           "file_name"]
