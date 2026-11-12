"""Background position monitor.

Every ``monitor_interval_sec``: sync each PENDING/OPEN broker position with
its venue, cancel stale limit entries (and the unfilled rest of partially
filled ones) and roll the trade status up. Waiting virtual entries and
unfilled LIMIT entries resting on a ccxt exchange (their stop is placed when
the monitor notices the fill) are checked every ``VIRTUAL_POLL_SEC``.

One bad or slow position never stops or delays the others: every trade is
synced in a task of its own under its trade lock, the positions of a trade
at the same time, and the loop never waits for a sync beyond its own tick.
A broker still gets one sync at a time (:meth:`Monitor._turn`): a broker that
hangs holds up one trade at a time, never every trade that has a position on
it, and its adapter never sees two syncs of the monitor at once.
Only :meth:`Monitor.request_stop` and a real cancellation of its task end
:meth:`Monitor.run`; a ``CancelledError`` that a broker SDK raises on its own
is an ordinary sync error, and when the passes themselves keep failing (the
database cannot be read) the owner is told. Adapter errors (``pos.error``) are reported to the
owner when they change; a position without a working stop is reported at once
and again every ``UNPROTECTED_REALERT_PASSES`` passes until it is protected or
closed. A heartbeat (:mod:`.health`) tells the owner when a broker stops
answering.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

from ..brokers.base import Broker
from ..config import AppConfig
from ..interfaces import Notifier, Storage
from ..models import BrokerPosition, PositionStatus, Severity, TradeRecord, TradeStatus
from ..telegram.formatting import esc, fmt_price
from .health import BrokerHeartbeat, stray_cancel

logger = logging.getLogger(__name__)

ACTIVE_POSITION = frozenset({PositionStatus.PENDING, PositionStatus.OPEN})
ACTIVE_TRADE = frozenset({TradeStatus.OPENING, TradeStatus.PENDING, TradeStatus.OPEN})
SYNC_ERROR_ALERT_AFTER = 5
STUCK_OPENING_SEC = 300.0   # OPENING trade with no positions and no running execution -> FAILED
UNPROTECTED_REALERT_PASSES = 30   # repeat the "no stop" alert every this many passes (~5 min at 10 s)
ERROR_REALERT_PASSES = 30         # the same adapter error text is not re-sent within this many passes
ERROR_ALERTS_PER_WINDOW = 3       # and at most this many different ones per position in that window
_DEAD_LEG_STATES = frozenset({"closed", "cancelled", "failed"})
SYNC_TIMEOUT_SEC = 120.0          # upper bound of one venue sync call
# The fast pass: waiting virtual (reference-touch) entries and unfilled LIMIT entries on a ccxt exchange are
# checked this often. Per unfilled exchange entry that is one order lookup per check (binance: GET order).
VIRTUAL_POLL_SEC = 2.0
CANCEL_WAIT_SEC = 5.0             # a cancelled monitor waits at most this long for its cancelled syncs to end
PASS_ERROR_ALERT_AFTER = 5        # loop passes that failed in a row (e.g. the database cannot be read) before
#                                   the owner is told: once, and once more when a pass works again
# Per account one venue sync and one virtual entry at a time (see Monitor._turn). A position waits at most this
# long for another position of its account and is otherwise left to the next pass: a trade never keeps its lock
# while another trade's call to that account takes long or hangs.
TURN_WAIT_SEC = 2.0
STARVED_AFTER_INTERVALS = 2.0     # a position that has waited this many monitor intervals for its broker goes first


def is_virtual_pending(pos: BrokerPosition) -> bool:
    """A virtual entry waiting for its reference-exchange trigger: no order exists on the broker yet."""
    return pos.status is PositionStatus.PENDING and bool((pos.data or {}).get("virtual_trigger"))


def is_live_ccxt(broker: Broker | None) -> bool:
    """A real exchange account behind the ccxt adapter (told by the config type, no adapter import; the
    dry-run wrapper carries the same config but only simulates)."""
    if broker is None:
        return False
    cfg_type = getattr(getattr(broker, "config", None), "type", None)
    return cfg_type == "ccxt" and not str(getattr(broker, "kind", "") or "").startswith("dry-run")


def sync_error_text(exc: BaseException) -> str:
    """Why a venue call failed, for the owner and the log. Never empty."""
    if isinstance(exc, asyncio.CancelledError):       # raised by the broker SDK itself, see health.stray_cancel
        return "bağlantı kopuk (istek yarıda kaldı)"
    if isinstance(exc, asyncio.TimeoutError) and not str(exc):
        return f"{SYNC_TIMEOUT_SEC:g} sn içinde yanıt gelmedi"
    return str(exc) or type(exc).__name__


def utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def rollup_status(positions: Sequence[BrokerPosition]) -> TradeStatus:
    """Trade status implied by its broker positions."""
    statuses = {p.status for p in positions}
    if PositionStatus.OPEN in statuses:
        return TradeStatus.OPEN
    if PositionStatus.PENDING in statuses:
        return TradeStatus.PENDING
    if not positions or statuses <= {PositionStatus.FAILED}:
        return TradeStatus.FAILED
    ever_filled = any(p.status is PositionStatus.CLOSED or p.filled_quantity > 0 for p in positions)
    return TradeStatus.CLOSED if ever_filled else TradeStatus.CANCELLED


def protection_problem(pos: BrokerPosition) -> str | None:
    """Turkish reason when an OPEN position has no working stop (adapter flags), else None.

    ccxt: ``data["unprotected"]``; MetaTrader legs: ``data["legs"][i]["naked"]`` on a live leg."""
    if pos.status is not PositionStatus.OPEN:
        return None
    d = pos.data or {}
    if d.get("unprotected"):
        return "stop emri yok"
    naked = [leg for leg in d.get("legs") or []
             if isinstance(leg, dict) and leg.get("naked") and leg.get("state") not in _DEAD_LEG_STATES]
    if naked:
        tickets = ", ".join(f"#{leg.get('ticket')}" for leg in naked)
        return f"{len(naked)} bacak stopsuz ({tickets})"
    return None


def _parse_time(raw: object) -> datetime | None:
    try:
        if isinstance(raw, bool):
            return None
        if isinstance(raw, (int, float)) and raw > 0:
            seconds = raw / 1000.0 if raw > 1e11 else float(raw)
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        if isinstance(raw, str) and raw:
            return utc(datetime.fromisoformat(raw))
        if isinstance(raw, datetime):
            return utc(raw)
    except (ValueError, OverflowError, OSError):
        pass
    return None


def entry_placed_at(pos: BrokerPosition, fallback: datetime) -> datetime:
    """When the entry order was placed: "entry_created_ms" (kept by adapters that overwrite "opened_at"
    with the first fill time), else "opened_at", else ``fallback``."""
    for key in ("entry_created_ms", "opened_at"):
        found = _parse_time(pos.data.get(key))
        if found is not None:
            return found
    return utc(fallback)


def entry_remainder_resting(pos: BrokerPosition) -> bool:
    """A partly filled LIMIT entry whose unfilled rest may still be resting on the venue."""
    d = pos.data or {}
    return (
        pos.status is PositionStatus.OPEN
        and d.get("entry_done") is False
        and not d.get("fill_assumed")
        and str(d.get("entry_type") or "limit") != "market"
        and pos.filled_quantity < pos.quantity
    )


def position_opened_at(pos: BrokerPosition, fallback: datetime) -> datetime:
    """Placement time from adapter data ("opened_at": epoch ms/s or ISO), else ``fallback``."""
    found = _parse_time(pos.data.get("opened_at"))
    return found if found is not None else utc(fallback)


class Monitor:
    def __init__(
        self,
        config: AppConfig,
        db: Storage,
        brokers: Sequence[Broker] | Callable[[], Sequence[Broker]],
        notifier: Notifier,
        *,
        trade_lock: Callable[[int], asyncio.Lock] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """``brokers``: connected brokers (list or callable). ``trade_lock``: per-trade lock
        shared with the engine so a position is never written by two coroutines at once."""
        self.config = config
        self.db = db
        self._brokers = brokers
        self.notifier = notifier
        self._locks: dict[int, asyncio.Lock] = {}
        self._trade_lock = trade_lock or self._own_lock
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sync_errors: dict[tuple[int, str], int] = {}
        self._sync_counted_at: dict[tuple[int, str], float] = {}   # start of the last sync whose failure was counted
        self._synced_at: dict[tuple[int, str], float] = {}         # monotonic start of each position's last sync
        self._running: dict[int, asyncio.Task[None]] = {}          # trade id -> its sync task while it runs
        self._hook_locks: dict[str, asyncio.Lock] = {}             # broker -> one virtual-entry hook at a time
        self._hook_since: dict[str, float] = {}                    # broker -> monotonic start of its running hook
        self._sync_locks: dict[str, asyncio.Lock] = {}             # broker -> one venue sync at a time
        self._sync_since: dict[str, float] = {}                    # broker -> monotonic start of its running sync
        self._served_at: dict[tuple[int, str], float] = {}         # when each position last got its broker's turn
        #                                                            (before its first turn: when it was first due)
        self._pass_errors = 0                                      # loop passes that failed in a row
        self._pass_alerted = False                                 # the owner was told that the passes keep failing
        self._notes: set[asyncio.Task[None]] = set()               # notices of the loop itself that are being sent
        self._warned_missing: set[str] = set()
        self._unprotected: dict[tuple[int, str], int] = {}       # passes since the last "no stop" alert
        self._error_alerts: dict[tuple[int, str], list[tuple[int, str]]] = {}   # (pass no, text) of recent alerts
        self._remainder_warned: set[tuple[int, str]] = set()
        self._passes = 0
        self._stop = asyncio.Event()
        # Engine hook for virtual entries: (trade, position, now) -> new position. Called under the trade lock.
        self.virtual_hook: Callable[[TradeRecord, BrokerPosition, datetime], Awaitable[BrokerPosition]] | None = None
        # Started and stopped inside run(): tells the owner when a connected broker stops answering.
        self.heartbeat = BrokerHeartbeat(self._connected, notifier)

    def request_stop(self) -> None:
        """Let the position syncs that are running finish, start no new one, then leave :meth:`run`
        (graceful shutdown)."""
        self._stop.set()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def _own_lock(self, trade_id: int) -> asyncio.Lock:
        return self._locks.setdefault(trade_id, asyncio.Lock())

    def _connected(self) -> list[Broker]:
        src = self._brokers
        return list(src() if callable(src) else src)

    def _broker(self, name: str) -> Broker | None:
        for b in self._connected():
            if b.name == name:
                return b
        return None

    def _now(self) -> datetime:
        return utc(self._clock())

    # ---- schedule --------------------------------------------------------------
    def _periods(self) -> tuple[float, float, float]:
        """(monitor interval, fast interval, tolerance for a tick that comes a little early)."""
        interval = max(0.05, float(self.config.monitor_interval_sec))
        fast = min(interval, VIRTUAL_POLL_SEC)
        return interval, fast, min(fast / 2.0, interval / 10.0)

    def _fast_position(self, pos: BrokerPosition) -> bool:
        """Checked every VIRTUAL_POLL_SEC instead of every monitor interval: a waiting virtual entry, and an
        unfilled LIMIT entry resting on a ccxt exchange (it gets its stop only when the fill is noticed).
        Pending MetaTrader orders carry their stop and take profit at the broker; simulated ones can wait."""
        if pos.status is not PositionStatus.PENDING:
            return False
        if is_virtual_pending(pos):
            return True
        if pos.dry_run or (pos.data or {}).get("simulated"):
            return False
        return is_live_ccxt(self._broker(pos.broker))

    def _due(self, trade_id: int, pos: BrokerPosition, now: float) -> bool:
        """The loop's rule: a position is synced again when its own period has passed since its last sync
        STARTED (fast positions: the fast interval, all others: the monitor interval), and at once when this
        monitor has not synced it yet. An entry whose fill was noticed in a fast check is therefore looked at
        again a full interval later, exactly as when fills were only noticed in the full pass (adapters compare
        consecutive reads of the venue, e.g. two "flat" reads mean "closed there")."""
        last = self._synced_at.get((trade_id, pos.broker))
        if last is None:
            return True
        interval, fast, tolerance = self._periods()
        return now - last >= (fast if self._fast_position(pos) else interval) - tolerance

    def _wanted(self, mode: str, trade_id: int, pos: BrokerPosition, now: float) -> bool:
        if pos.status not in ACTIVE_POSITION:
            return False
        if mode == "virtual":
            return is_virtual_pending(pos)
        if mode == "fast":
            return self._fast_position(pos)
        if mode == "due":
            return self._due(trade_id, pos, now)
        return True

    # ---- loop ----------------------------------------------------------------
    async def run(self) -> None:
        """Run until :meth:`request_stop` or a real cancellation of this task. Nothing else ends the loop: a
        failing pass, and a CancelledError that a broker SDK raises on its own, are logged and the loop goes on.

        Every fast interval the positions that are due are synced, each trade in a task of its own. A tick waits
        for its tasks only until the next tick: a sync that is still running then is left alone and its trade
        is skipped until it has finished."""
        interval, fast, tolerance = self._periods()
        logger.info("Monitor started (every %.1fs, fast checks every %.1fs)", interval, fast)
        heartbeat: asyncio.Task[None] | None = None
        if not self._stop.is_set():
            heartbeat = asyncio.ensure_future(self.heartbeat.run())
            heartbeat.set_name("monitor-heartbeat")
        last_chores: float | None = None
        try:
            while not self._stop.is_set():
                tick = time.monotonic()
                failure: BaseException | None = None
                try:
                    chores = last_chores is None or tick - last_chores >= interval - tolerance
                    if chores:
                        last_chores = tick
                    tasks = await self._start_pass("due", chores=chores)
                    if tasks:       # normally all done well before the next tick
                        await asyncio.wait(tasks, timeout=max(0.0, tick + fast - time.monotonic()))
                except asyncio.CancelledError as exc:
                    if not stray_cancel():
                        raise
                    logger.error("Monitor pass ended by a CancelledError nobody requested (broker SDK); "
                                 "the loop goes on")
                    failure = exc
                except Exception as exc:
                    logger.exception("Monitor pass failed")
                    failure = exc
                self._pass_done(failure)
                pause = tick + fast - time.monotonic()
                if pause > 0:
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self._stop.wait(), pause)
                else:
                    await asyncio.sleep(0)
            await self._finish_running()
            await self._finish_notes()
        except asyncio.CancelledError:
            await self._cancel_tasks(list(self._running.values()))
            raise
        finally:
            await self._cancel_tasks([*self._notes, *([heartbeat] if heartbeat is not None else [])])
        logger.info("Monitor stopped")

    def _pass_done(self, failure: BaseException | None) -> None:
        """Book how a loop pass went. A pass fails when it cannot even find out what to sync (the database
        cannot be read...): the loop goes on whatever happens, but a monitor that is alive and syncs nothing
        must not go unnoticed. After PASS_ERROR_ALERT_AFTER failed passes in a row the owner is told once, and
        once more when a pass works again. Never raises, never waits (the notices are sent by their own task)."""
        if failure is None:
            self._pass_errors = 0
            if self._pass_alerted:
                self._pass_alerted = False
                logger.info("Monitor: the passes work again")
                self._tell_owner("✅ Pozisyon takibi yeniden çalışıyor.", Severity.INFO)
            return
        self._pass_errors += 1
        if self._pass_errors < PASS_ERROR_ALERT_AFTER or self._pass_alerted:
            return
        self._pass_alerted = True
        self._tell_owner(
            f"🚨 Pozisyon takibi çalışmıyor: son {self._pass_errors} denemede pozisyonlar kontrol edilemedi "
            f"(<code>{esc(sync_error_text(failure))}</code>). Açık pozisyonlarınızı ve stoplarını "
            "borsada/brokerda kontrol edin. Bot denemeye devam ediyor; düzelince haber verecek.",
            Severity.ERROR,
        )

    def _tell_owner(self, text: str, severity: Severity) -> None:
        """Send a notice of the loop itself in a task of its own: a slow Telegram never holds the loop up."""
        task = asyncio.ensure_future(self._notify_quietly(text, severity))
        task.set_name("monitor-note")
        self._notes.add(task)
        task.add_done_callback(self._notes.discard)

    async def _notify_quietly(self, text: str, severity: Severity) -> None:
        try:
            await self.notifier.notify(text, severity)
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            logger.error("Monitor: a notice to the owner was cut off: %s", text)
        except Exception:
            logger.exception("Monitor: could not tell the owner: %s", text)

    async def _finish_notes(self) -> None:
        """Graceful stop: give the notices that are being sent a moment to go out."""
        pending = [t for t in self._notes if not t.done()]
        if pending:
            await asyncio.wait(pending, timeout=CANCEL_WAIT_SEC)

    async def run_once(self) -> None:
        """One full pass over every active trade. Returns when all of its syncs are done. (A position whose
        broker is busy with another trade's sync for longer than TURN_WAIT_SEC is left to the next pass.)"""
        await self._finish(await self._start_pass("all"))

    async def run_virtual_once(self) -> None:
        """One fast pass: only trades with a waiting virtual entry or an unfilled exchange LIMIT entry (cheap;
        the venue-syncing pass over everything runs less often). Returns when all of its syncs are done."""
        await self._finish(await self._start_pass("fast", chores=False))

    async def _start_pass(self, mode: str, *, chores: bool = True) -> list[asyncio.Task[None]]:
        """Start (not await) one sync task for every trade that has work in this pass, is not locked and is not
        still being synced. ``mode``: "all" = every active position, "fast" = the fast ones, "due" = the loop:
        positions whose own period has passed. ``chores``: also count the pass and look at active trades
        without an active position (stuck openings, trades whose positions are all terminal)."""
        if chores:
            self._passes += 1
        now = time.monotonic()
        starved_after = STARVED_AFTER_INTERVALS * self._periods()[0]
        rank: dict[int, tuple[int, float]] = {}     # trade id -> (class, since when it waits for its broker)
        active: set[tuple[int, str]] = set()
        for trade_id, pos in await self.db.active_broker_positions():
            key = (trade_id, pos.broker)
            active.add(key)
            if self._wanted(mode, trade_id, pos, now):
                waiting = self._served_at.setdefault(key, now)      # its last turn; for a new one: first seen
                mine = (0 if now - waiting >= starved_after else 1 if self._fast_position(pos) else 2, waiting)
                rank[trade_id] = min(rank.get(trade_id, mine), mine)
        # A broker takes one sync at a time (see _turn), so the order matters. First whatever has not had its
        # broker's turn for STARVED_AFTER_INTERVALS intervals (a broker so slow that the fast checks alone keep
        # it busy must not leave its open positions unsynced), then the fill checks of resting entries (a stop
        # depends on them), then the rest; within each group whoever has waited longest - as fair as the round
        # of the sequential loop was.
        trade_ids = sorted(rank, key=lambda trade_id: rank[trade_id])
        if chores:
            busy = {trade_id for trade_id, _broker in active}
            for trade in await self.db.active_trades():   # also trades whose positions are all terminal
                if trade.id not in trade_ids and (mode == "all" or trade.id not in busy):
                    trade_ids.append(trade.id)
        for state in (self._synced_at, self._sync_counted_at, self._served_at):
            for key in [k for k in state if k not in active]:
                del state[key]
        for trade_id in [t for t, task in self._running.items() if task.done()]:
            del self._running[trade_id]         # (a task cancelled before it started never removed itself)
        started: list[asyncio.Task[None]] = []
        for trade_id in trade_ids:
            if self._stop.is_set():
                break           # shutting down: never start a new position sync
            running = self._running.get(trade_id)
            if running is not None and not running.done():
                continue        # its previous sync is still running (a slow broker): the others do not wait
            lock = self._trade_lock(trade_id)
            if lock.locked():
                continue        # being executed / managed right now; next pass
            task = asyncio.ensure_future(self._sync_task(trade_id, lock, mode))
            task.set_name(f"monitor-sync-{trade_id}")
            self._running[trade_id] = task
            started.append(task)
        return started

    async def _sync_task(self, trade_id: int, lock: asyncio.Lock, mode: str) -> None:
        """One trade's sync in a task of its own, under the trade lock. Never raises (a real cancellation
        excepted), so one bad trade never touches the others."""
        try:
            if lock.locked():
                return          # taken since the pass looked: being executed / managed right now; next pass
            async with lock:
                if not self._stop.is_set():     # shutting down: never start a new position sync
                    await self._sync_trade(trade_id, mode)
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            logger.error("Monitor: trade %s sync ended by a CancelledError nobody requested (broker SDK)", trade_id)
        except Exception:
            logger.exception("Monitor: trade %s sync failed", trade_id)
        finally:
            if self._running.get(trade_id) is asyncio.current_task():
                del self._running[trade_id]

    async def _finish(self, tasks: list[asyncio.Task[None]]) -> None:
        """Wait for a directly called pass. A cancelled caller takes its syncs with it (they used to run
        inside it)."""
        if not tasks:
            return
        try:
            await asyncio.wait(tasks)
        except asyncio.CancelledError:
            await self._cancel_tasks(tasks)
            raise

    async def _finish_running(self) -> None:
        """Graceful stop: the syncs that are running finish (one of them may be placing a stop)."""
        running = [t for t in self._running.values() if not t.done()]
        if running:
            logger.info("Monitor: stopping; waiting for %d running position sync(s)", len(running))
            await asyncio.wait(running)

    @staticmethod
    async def _cancel_tasks(tasks: Sequence[asyncio.Task[Any]]) -> None:
        """Cancel and wait (bounded) so that nothing is left running behind a monitor that has ended."""
        pending = [t for t in tasks if not t.done()]
        for task in pending:
            task.cancel()
        if pending:
            _done, left = await asyncio.wait(pending, timeout=CANCEL_WAIT_SEC)
            if left:
                logger.error("Monitor: %d task(s) did not end %.0fs after being cancelled: %s", len(left),
                             CANCEL_WAIT_SEC, sorted(t.get_name() for t in left))

    # ---- one trade -----------------------------------------------------------
    async def sync_trade(self, trade_id: int, *, virtual_only: bool = False) -> None:
        """Sync all active positions of one trade at the same time (``virtual_only``: just its waiting virtual
        entries), then roll the trade status up once. Caller holds the trade lock. A position whose broker is
        busy with another trade for longer than TURN_WAIT_SEC is not waited for: it keeps its stored state."""
        await self._sync_trade(trade_id, "virtual" if virtual_only else "all")

    async def _sync_trade(self, trade_id: int, mode: str) -> None:
        trade = await self.db.get_trade(trade_id)
        positions = await self.db.broker_positions(trade_id)   # fresh copies (the engine may have changed them)
        if trade is None:
            return
        now = self._now()
        if trade.status is TradeStatus.OPENING and not positions:
            if (now - utc(trade.created_at)).total_seconds() > STUCK_OPENING_SEC:
                await self.db.update_trade(trade_id, status=TradeStatus.FAILED, note="yarım kalan açılış")
                await self.notifier.notify(
                    f"#{trade_id} {esc(trade.plan.base_asset)} işlemi açılırken yarım kaldı. "
                    "Lütfen borsada/brokerda elle kontrol edin.",
                    Severity.WARNING,
                )
            return

        mono = time.monotonic()
        first = [i for i, pos in enumerate(positions) if self._wanted(mode, trade_id, pos, mono)]
        updated = await self._sync_positions(trade, positions, first, spaced=mode == "due")
        await self._rollup(trade, updated)

    async def _sync_positions(
        self, trade: TradeRecord, positions: list[BrokerPosition], first: list[int], *, spaced: bool
    ) -> list[BrokerPosition]:
        """Sync the positions ``first`` (indexes) at the same time - they are on different brokers - and return
        the trade's positions as they are afterwards.

        While one of them is slow (a hung call, a virtual entry sending its order) the trade's other positions
        do not wait for it: each is synced again when its own period has passed, as the next pass would do
        (e.g. an unfilled exchange entry keeps being checked and gets its stop). Nothing new is started once
        the slow ones are done or shutdown began, and this returns only when every call has ended: the caller
        holds the trade lock for all of them."""
        states = list(positions)
        if not first:
            return states
        if len(first) == 1 and sum(1 for pos in states if pos.status in ACTIVE_POSITION) == 1:
            states[first[0]], _ok = await self._sync_guarded(trade, states[first[0]], spaced)
            return states
        began = time.monotonic()
        tasks = {i: self._spawn_sync(trade, states[i], spaced) for i in first}
        slow = list(tasks.values())         # this call's own syncs: others are re-synced only while one of them runs
        failed: set[int] = set()            # ended unexpectedly: left to the next pass (it reads the database)
        try:
            while tasks:
                wait: float | None = None
                if not self._stop.is_set() and any(not t.done() for t in slow):
                    try:
                        wait = await self._resync_due(trade, states, tasks, failed, began)
                    except asyncio.CancelledError:
                        if not stray_cancel():
                            raise
                    except Exception:       # never a reason to cut the running syncs off
                        logger.exception("Monitor: trade #%s: planning the next checks failed", trade.id)
                await asyncio.wait(list(tasks.values()), timeout=None if wait is None else max(wait, 0.01),
                                   return_when=asyncio.FIRST_COMPLETED)
                for i in [i for i, task in tasks.items() if task.done()]:
                    task = tasks.pop(i)
                    if task.cancelled() or task.exception() is not None:     # _sync_guarded handles its own errors
                        logger.error("Monitor: sync task of %s trade #%s ended unexpectedly", states[i].broker,
                                     trade.id, exc_info=None if task.cancelled() else task.exception())
                        failed.add(i)
                        continue
                    states[i], ok = task.result()
                    if not ok:
                        failed.add(i)
        finally:
            if tasks:           # cancelled (or an unexpected error here): no sync may outlive the trade lock
                await self._cancel_tasks(list(tasks.values()))
        return states

    async def _resync_due(
        self, trade: TradeRecord, states: list[BrokerPosition],
        tasks: dict[int, asyncio.Task[tuple[BrokerPosition, bool]]], failed: set[int], began: float,
    ) -> float | None:
        """While a sync of the trade is slow: start the sync of every other active position whose own period has
        passed (not before the slow one has taken one fast interval), from what the database holds now.
        Returns the seconds until the next one is due (None: nothing to wait for)."""
        interval, fast, _tolerance = self._periods()
        wait: float | None = None
        for i, pos in enumerate(states):
            if i in tasks or i in failed or pos.status not in ACTIVE_POSITION:
                continue
            period = fast if self._fast_position(pos) else interval
            due_in = max(began + fast, self._synced_at.get((trade.id, pos.broker), began) + period) - time.monotonic()
            if due_in > 0:
                wait = due_in if wait is None else min(wait, due_in)
                continue
            try:
                states[i] = await self._stored(trade.id, pos)     # what the next pass would read
            except asyncio.CancelledError:
                if not stray_cancel():
                    raise
                failed.add(i)
                continue
            except Exception:
                logger.exception("Monitor: re-reading %s trade #%s failed", pos.broker, trade.id)
                failed.add(i)
                continue
            if states[i].status in ACTIVE_POSITION and not self._stop.is_set():
                tasks[i] = self._spawn_sync(trade, states[i], True)
        return wait

    def _spawn_sync(
        self, trade: TradeRecord, pos: BrokerPosition, spaced: bool
    ) -> asyncio.Task[tuple[BrokerPosition, bool]]:
        task = asyncio.ensure_future(self._sync_guarded(trade, pos, spaced))
        task.set_name(f"monitor-sync-{trade.id}-{pos.broker}")
        return task

    async def _sync_guarded(self, trade: TradeRecord, pos: BrokerPosition, spaced: bool) -> tuple[BrokerPosition, bool]:
        """One position's sync that never raises (a real cancellation excepted): the position as it is
        afterwards, and False when the sync ended unexpectedly (the result then is what the database holds)."""
        self._synced_at[(trade.id, pos.broker)] = time.monotonic()
        try:
            return await self._sync_position(trade, pos, self._now(), spaced=spaced), True
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            logger.error("Monitor: sync %s trade #%s ended by a CancelledError nobody requested (broker SDK)",
                         pos.broker, trade.id)
        except Exception:
            logger.exception("Monitor: sync %s trade #%s failed", pos.broker, trade.id)
        with contextlib.suppress(Exception):
            return await self._stored(trade.id, pos), False
        return pos, False

    @contextlib.asynccontextmanager
    async def _turn(
        self, locks: dict[str, asyncio.Lock], since: dict[str, float], key: tuple[int, str]
    ) -> AsyncIterator[bool]:
        """The turn of position ``key`` (trade id, broker) at its broker. As when the trades were synced one
        after another, a broker gets one venue sync at a time (``_sync_locks``: its adapter never sees two
        syncs of the monitor at once) and one virtual-entry hook at a time (``_hook_locks``: two entries of one
        account that fire together would both pass its hedge / margin / daily-loss checks).

        Yields False when it is not this position's turn within TURN_WAIT_SEC, and at once when the call that
        is running has been at it for that long already (the broker hangs, or an order is being sent): the
        position is then left to the next pass. A trade therefore never keeps its lock while ANOTHER trade's
        call to that broker is slow: one trade at a time is held up by a broker that does not answer, and the
        engine can manage all the others (their positions on the other brokers included) meanwhile."""
        lock = locks.setdefault(key[1], asyncio.Lock())
        started = since.get(key[1])
        if lock.locked() and started is not None and time.monotonic() - started >= TURN_WAIT_SEC:
            yield False
            return
        try:
            async with asyncio.timeout(TURN_WAIT_SEC):
                await lock.acquire()
        except TimeoutError:
            yield False
            return
        since[key[1]] = self._served_at[key] = time.monotonic()
        try:
            yield True
        finally:
            since.pop(key[1], None)
            lock.release()

    def _in_a_pass(self, trade_id: int) -> bool:
        """Is the trade being synced by a task that a pass started (and not by the engine through sync_trade)?
        Both hold the trade lock, so it is one or the other."""
        task = self._running.get(trade_id)
        return task is not None and not task.done()

    def _retry_soon(self, key: tuple[int, str], pos: BrokerPosition) -> None:
        """The position did not get its broker's turn: it is due again on the next fast pass (not a full
        period later), until it has had its turn."""
        interval, fast, _tolerance = self._periods()
        period = fast if self._fast_position(pos) else interval
        self._synced_at[key] = time.monotonic() - period + fast

    async def _stored(self, trade_id: int, pos: BrokerPosition) -> BrokerPosition:
        """The position as the database holds it now."""
        for stored in await self.db.broker_positions(trade_id):
            if stored.broker == pos.broker:
                return stored
        return pos

    async def _sync_position(
        self, trade: TradeRecord, pos: BrokerPosition, now: datetime, *, spaced: bool = False
    ) -> BrokerPosition:
        """``spaced``: called by the loop's schedule (see :meth:`_sync_failed`)."""
        if is_virtual_pending(pos):     # no venue order exists: the engine watches the reference price
            if self.virtual_hook is None:
                return pos
            try:
                async with self._turn(self._hook_locks, self._hook_since, (trade.id, pos.broker)) as mine:
                    if not mine or self._stop.is_set():
                        # The account is busy with another entry (next pass), or shutdown began while this one
                        # waited: an order flow is not started now.
                        return pos
                    return await self.virtual_hook(trade, pos, self._now())
            except asyncio.CancelledError as exc:
                if not stray_cancel():
                    raise
                logger.error("Monitor: virtual entry %s #%s failed: %s", pos.broker, trade.id, sync_error_text(exc))
            except Exception:
                logger.exception("Monitor: virtual entry %s #%s failed", pos.broker, trade.id)
            return await self._stored(trade.id, pos)     # the hook may have saved a newer state before it failed
        broker = self._broker(pos.broker)
        if broker is None:
            if pos.broker not in self._warned_missing:
                self._warned_missing.add(pos.broker)
                logger.warning("Monitor: broker %s is not connected; its positions are not synced", pos.broker)
            return pos
        key = (trade.id, pos.broker)
        before = pos.status
        failure: BaseException | None = None
        async with self._turn(self._sync_locks, self._sync_since, key) as mine:
            if not mine or (self._stop.is_set() and self._in_a_pass(trade.id)):
                # Another position of this broker is being synced and that takes long (or too many are queued):
                # not a failure of this one. It is tried again on the next fast pass. Or shutdown began before
                # this sync of a pass reached its broker: a call that has not started yet is not started now
                # (a sync the engine asks for itself, sync_trade(), still runs).
                logger.debug("Monitor: sync %s trade #%s left to the next pass (%s)", pos.broker, trade.id,
                             "shutting down" if mine else "the broker is busy with another position")
                self._retry_soon(key, pos)
                return pos
            try:
                new = await asyncio.wait_for(broker.sync_position(pos), SYNC_TIMEOUT_SEC)
            except asyncio.CancelledError as exc:
                if not stray_cancel():
                    raise               # this task really is being cancelled (shutdown)
                failure = exc
            except Exception as exc:
                failure = exc
        if failure is not None:
            return await self._sync_failed(trade, pos, failure, spaced)
        self._sync_errors.pop(key, None)
        self._sync_counted_at.pop(key, None)
        if new.model_dump() != pos.model_dump():
            await self.db.save_broker_position(trade.id, new)
            await self.db.log_event(
                "position_synced",
                {"trade_id": trade.id, "broker": pos.broker, "status": new.status.value,
                 "remaining": new.remaining_quantity, "realized_pnl": new.realized_pnl},
            )
        if new.status is PositionStatus.FAILED and before is not PositionStatus.FAILED:
            await self.notifier.notify(
                f"{esc(pos.broker)}: #{trade.id} {esc(trade.plan.base_asset)} pozisyonu hata durumunda: "
                f"<code>{esc(new.error or 'bilinmeyen hata')}</code>",
                Severity.ERROR,
            )
            if new.error:
                self._sent_error(key, new.error)
        elif new.error and new.error != pos.error and protection_problem(new) is None:
            await self._error_changed(trade, new)
        await self.check_protection(trade, new)

        if new.status is PositionStatus.PENDING:
            new = await self._expire_pending(broker, trade, new, now)
        elif entry_remainder_resting(new):
            new = await self._expire_remainder(broker, trade, new, now)
        return new

    async def _sync_failed(
        self, trade: TradeRecord, pos: BrokerPosition, exc: BaseException, spaced: bool
    ) -> BrokerPosition:
        """The venue call raised, timed out or was cut off by the broker SDK (a CancelledError nobody requested):
        logged and counted per (trade, broker); the owner is told once after SYNC_ERROR_ALERT_AFTER failures in
        a row. On the loop's schedule (``spaced``) at most one failure per monitor interval is counted, so a
        position that is checked every VIRTUAL_POLL_SEC does not bring the alert forward."""
        key = (trade.id, pos.broker)
        reason = sync_error_text(exc)
        started = self._synced_at.get(key, time.monotonic())      # when the failed sync began
        interval, _fast, tolerance = self._periods()
        last = self._sync_counted_at.get(key)
        if spaced and last is not None and started - last < interval - tolerance:
            logger.debug("Monitor: sync %s trade #%s failed again: %s", pos.broker, trade.id, reason)
        else:
            count = self._sync_errors.get(key, 0) + 1
            self._sync_errors[key] = count
            self._sync_counted_at[key] = started
            logger.warning("Monitor: sync %s trade #%s failed (%d in a row): %s", pos.broker, trade.id, count, reason)
            if count == SYNC_ERROR_ALERT_AFTER:
                await self.notifier.notify(
                    f"{esc(pos.broker)}: #{trade.id} {esc(trade.plan.base_asset)} pozisyonu {count} kez üst üste "
                    f"kontrol edilemedi: <code>{esc(reason)}</code>",
                    Severity.WARNING,
                )
        await self.check_protection(trade, pos)     # still without a stop? keep telling the owner
        return pos

    async def _error_changed(self, trade: TradeRecord, pos: BrokerPosition) -> None:
        """An adapter reported a new problem (TP not placed, SL not resized, emergency close...)."""
        key = (trade.id, pos.broker)
        error = pos.error or ""
        recent = [(n, t) for n, t in self._error_alerts.get(key, []) if self._passes - n < ERROR_REALERT_PASSES]
        if any(t == error for _n, t in recent) or len(recent) >= ERROR_ALERTS_PER_WINDOW:
            logger.warning("Monitor: %s #%s error (not re-sent): %s", pos.broker, trade.id, error)
            return
        self._sent_error(key, error)
        await self.notifier.notify(
            f"⚠️ {esc(pos.broker)}: #{trade.id} {esc(trade.plan.base_asset)} {trade.plan.side.value.upper()} "
            f"({_STATUS_TR.get(pos.status, pos.status.value)}): <code>{esc(error)}</code>",
            Severity.ERROR,
        )

    def _sent_error(self, key: tuple[int, str], error: str) -> None:
        recent = [(n, t) for n, t in self._error_alerts.get(key, []) if self._passes - n < ERROR_REALERT_PASSES]
        self._error_alerts[key] = [*recent, (self._passes, error)]

    def note_alerted(self, trade_id: int, pos: BrokerPosition) -> None:
        """The engine already told the owner about this position's problem (no duplicate next pass)."""
        key = (trade_id, pos.broker)
        if protection_problem(pos) is not None:
            self._unprotected[key] = 0
        if pos.error:
            self._sent_error(key, pos.error)

    async def check_protection(self, trade: TradeRecord, pos: BrokerPosition) -> None:
        """Alert (ERROR) while a filled position has no working stop: at once, then every
        UNPROTECTED_REALERT_PASSES passes. Independent of notify_on_execution."""
        key = (trade.id, pos.broker)
        problem = protection_problem(pos)
        if problem is None:
            if self._unprotected.pop(key, None) is not None and pos.status is PositionStatus.OPEN:
                await self.notifier.notify(
                    f"✅ {esc(pos.broker)}: #{trade.id} {esc(trade.plan.base_asset)} pozisyonu artık stop ile korunuyor"
                    + (f" (stop {fmt_price(pos.stop_loss)})." if pos.stop_loss is not None else "."),
                    Severity.INFO,
                )
            return
        passes = self._unprotected.get(key)
        if passes is not None and passes + 1 < UNPROTECTED_REALERT_PASSES:
            self._unprotected[key] = passes + 1
            return
        self._unprotected[key] = 0
        await self.notifier.notify(unprotected_alert_text(trade, pos, problem), Severity.ERROR)

    async def _expire_pending(
        self, broker: Broker, trade: TradeRecord, pos: BrokerPosition, now: datetime
    ) -> BrokerPosition:
        max_age = timedelta(minutes=self.config.risk.pending_entry_expiry_min)
        if self.config.risk.pending_entry_expiry_min <= 0 or now - position_opened_at(pos, trade.created_at) < max_age:
            return pos
        try:
            new = await broker.cancel_entry(pos)
        except asyncio.CancelledError as exc:
            if not stray_cancel():
                raise
            logger.warning("Monitor: cancelling stale entry %s #%s failed: %s", pos.broker, trade.id,
                           sync_error_text(exc))
            return pos
        except Exception as exc:
            logger.warning("Monitor: cancelling stale entry %s #%s failed: %s", pos.broker, trade.id, exc)
            return pos
        await self.db.save_broker_position(trade.id, new)
        await self.db.log_event("pending_expired", {"trade_id": trade.id, "broker": pos.broker})
        filled = new.status is not PositionStatus.CANCELLED
        await self.notifier.notify(
            f"{esc(pos.broker)}: #{trade.id} {esc(trade.plan.base_asset)} limit girişi "
            f"{self.config.risk.pending_entry_expiry_min} dk içinde dolmadı, iptal edildi"
            + (" (kısmi dolum kapatıldı)." if filled else "."),
            Severity.INFO,
        )
        return new

    async def _expire_remainder(
        self, broker: Broker, trade: TradeRecord, pos: BrokerPosition, now: datetime
    ) -> BrokerPosition:
        """pending_entry_expiry_min also applies to the unfilled REST of a partly filled limit entry:
        cancel only that rest; the filled part stays open and protected."""
        minutes = self.config.risk.pending_entry_expiry_min
        if minutes <= 0 or now - entry_placed_at(pos, trade.created_at) < timedelta(minutes=minutes):
            return pos
        key = (trade.id, pos.broker)
        cancel = getattr(broker, "cancel_entry_remainder", None)
        if cancel is None:
            if key not in self._remainder_warned:
                self._remainder_warned.add(key)
                logger.warning("Monitor: %s cannot cancel the rest of a partial entry (#%s)", pos.broker, trade.id)
                await self.notifier.notify(
                    f"{esc(pos.broker)}: #{trade.id} {esc(trade.plan.base_asset)} limit girişi kısmen doldu "
                    f"({fmt_price(pos.filled_quantity)}/{fmt_price(pos.quantity)}); dolmayan kısmı {minutes} dk "
                    "sonra hâlâ bekliyor ve bu hesapta otomatik iptal edilemiyor. Borsada elle iptal edin.",
                    Severity.WARNING,
                )
            return pos
        try:
            new = await cancel(pos)
        except asyncio.CancelledError as exc:
            if not stray_cancel():
                raise
            logger.warning("Monitor: cancelling the rest of entry %s #%s failed: %s", pos.broker, trade.id,
                           sync_error_text(exc))
            return pos
        except Exception as exc:
            logger.warning("Monitor: cancelling the rest of entry %s #%s failed: %s", pos.broker, trade.id, exc)
            return pos
        await self.db.save_broker_position(trade.id, new)
        if entry_remainder_resting(new):
            logger.warning("Monitor: rest of entry %s #%s not confirmed cancelled yet; next pass", pos.broker, trade.id)
            return new
        await self.db.log_event("entry_remainder_expired", {"trade_id": trade.id, "broker": pos.broker,
                                                            "filled": new.filled_quantity})
        await self.notifier.notify(
            f"{esc(pos.broker)}: #{trade.id} {esc(trade.plan.base_asset)} limit girişinin dolmayan kısmı "
            f"{minutes} dk içinde dolmadı, iptal edildi. Dolan {fmt_price(new.filled_quantity)} açık ve stoplu kalıyor.",
            Severity.INFO,
        )
        return new

    async def _rollup(self, trade: TradeRecord, positions: list[BrokerPosition]) -> None:
        if not positions:
            return
        status = rollup_status(positions)
        if status is trade.status:
            return
        await self.db.update_trade(trade.id, status=status)
        await self.db.log_event("trade_status", {"trade_id": trade.id, "from": trade.status.value, "to": status.value})
        logger.info("Trade #%s %s -> %s", trade.id, trade.status.value, status.value)
        if status in ACTIVE_TRADE or not self.config.notifier.notify_on_execution:
            return
        pnl = [(p.broker, p.realized_pnl) for p in positions if p.realized_pnl is not None]
        pnl_text = ", ".join(f"{esc(b)} {v:+.2f}" for b, v in pnl) or "bilinmiyor"
        word = "kapandı" if status is TradeStatus.CLOSED else (
            "iptal edildi" if status is TradeStatus.CANCELLED else "başarısız")
        entry = fmt_price(trade.plan.entry_price) if trade.plan.entry_price is not None else "?"
        topic = f" · 📁 {esc(trade.plan.route)}" if trade.plan.route else ""     # forum topic route
        await self.notifier.notify(
            f"🏁 #{trade.id} {esc(trade.plan.base_asset)} {trade.plan.side.value.upper()} (giriş {entry}) {word}. "
            f"Gerçekleşen K/Z: {pnl_text}{topic}",
            Severity.INFO,
        )


_STATUS_TR = {
    PositionStatus.PENDING: "bekleyen emir",
    PositionStatus.OPEN: "açık",
    PositionStatus.CLOSED: "kapalı",
    PositionStatus.CANCELLED: "iptal",
    PositionStatus.FAILED: "hata",
}


def unprotected_alert_text(trade: TradeRecord, pos: BrokerPosition, problem: str) -> str:
    detail = f"\n<code>{esc(pos.error)}</code>" if pos.error else ""
    return (
        f"🚨 <b>KRİTİK — STOPSUZ POZİSYON</b> {esc(pos.broker)}: #{trade.id} {esc(trade.plan.base_asset)} "
        f"{trade.plan.side.value.upper()} {fmt_price(pos.remaining_quantity)} {esc(pos.broker_symbol)} açık ve "
        f"stop koruması yok ({esc(problem)}).{detail}\n"
        "Borsada/brokerda hemen kontrol edin (gerekirse elle stop koyun veya kapatın). Bot her kontrolde "
        "stopu yeniden koymayı deniyor; korunana kadar bu uyarı tekrarlanır."
    )


__all__ = [
    "Monitor", "rollup_status", "position_opened_at", "entry_placed_at", "entry_remainder_resting",
    "protection_problem", "unprotected_alert_text", "is_virtual_pending", "VIRTUAL_POLL_SEC", "utc", "ACTIVE_POSITION", "ACTIVE_TRADE",
    "is_live_ccxt", "sync_error_text", "SYNC_TIMEOUT_SEC", "PASS_ERROR_ALERT_AFTER", "TURN_WAIT_SEC",
    "STARVED_AFTER_INTERVALS",
]
