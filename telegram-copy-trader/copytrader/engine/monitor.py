"""Background position monitor.

Every ``monitor_interval_sec``: sync each PENDING/OPEN broker position with
its venue, cancel stale limit entries (and the unfilled rest of partially
filled ones) and roll the trade status up. One bad position never stops the
loop. Adapter errors (``pos.error``) are reported to the owner when they
change; a position without a working stop is reported at once and again every
``UNPROTECTED_REALERT_PASSES`` passes until it is protected or closed.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timedelta, timezone


from ..brokers.base import Broker
from ..config import AppConfig
from ..interfaces import Notifier, Storage
from ..models import BrokerPosition, PositionStatus, Severity, TradeRecord, TradeStatus
from ..telegram.formatting import esc, fmt_price

logger = logging.getLogger(__name__)

ACTIVE_POSITION = frozenset({PositionStatus.PENDING, PositionStatus.OPEN})
ACTIVE_TRADE = frozenset({TradeStatus.OPENING, TradeStatus.PENDING, TradeStatus.OPEN})
SYNC_ERROR_ALERT_AFTER = 5
STUCK_OPENING_SEC = 300.0   # OPENING trade with no positions and no running execution -> FAILED
UNPROTECTED_REALERT_PASSES = 30   # repeat the "no stop" alert every this many passes (~5 min at 10 s)
ERROR_REALERT_PASSES = 30         # the same adapter error text is not re-sent within this many passes
ERROR_ALERTS_PER_WINDOW = 3       # and at most this many different ones per position in that window
_DEAD_LEG_STATES = frozenset({"closed", "cancelled", "failed"})
VIRTUAL_POLL_SEC = 2.0            # virtual (reference-touch) entries are checked this often


def is_virtual_pending(pos: BrokerPosition) -> bool:
    """A virtual entry waiting for its reference-exchange trigger: no order exists on the broker yet."""
    return pos.status is PositionStatus.PENDING and bool((pos.data or {}).get("virtual_trigger"))


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
        self._warned_missing: set[str] = set()
        self._unprotected: dict[tuple[int, str], int] = {}       # passes since the last "no stop" alert
        self._error_alerts: dict[tuple[int, str], list[tuple[int, str]]] = {}   # (pass no, text) of recent alerts
        self._remainder_warned: set[tuple[int, str]] = set()
        self._passes = 0
        self._stop = asyncio.Event()
        # Engine hook for virtual entries: (trade, position, now) -> new position. Called under the trade lock.
        self.virtual_hook: Callable[[TradeRecord, BrokerPosition, datetime], Awaitable[BrokerPosition]] | None = None

    def request_stop(self) -> None:
        """Finish the current position sync, then leave :meth:`run` (graceful shutdown)."""
        self._stop.set()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def _own_lock(self, trade_id: int) -> asyncio.Lock:
        return self._locks.setdefault(trade_id, asyncio.Lock())

    def _broker(self, name: str) -> Broker | None:
        src = self._brokers
        for b in src() if callable(src) else src:
            if b.name == name:
                return b
        return None

    def _now(self) -> datetime:
        return utc(self._clock())

    # ---- loop ----------------------------------------------------------------
    async def run(self) -> None:
        interval = max(0.05, float(self.config.monitor_interval_sec))
        logger.info("Monitor started (every %.1fs)", interval)
        fast = min(interval, VIRTUAL_POLL_SEC)
        last_full: float | None = None
        while not self._stop.is_set():
            try:
                now = time.monotonic()
                if last_full is None or now - last_full >= interval - 0.01:
                    last_full = now
                    await self.run_once()
                else:
                    await self.run_virtual_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Monitor pass failed")
            try:
                await asyncio.wait_for(self._stop.wait(), fast)
            except asyncio.TimeoutError:
                pass
        logger.info("Monitor stopped")

    async def run_once(self) -> None:
        self._passes += 1
        trade_ids: list[int] = []
        for trade_id, _pos in await self.db.active_broker_positions():
            if trade_id not in trade_ids:
                trade_ids.append(trade_id)
        for trade in await self.db.active_trades():   # also trades whose positions are all terminal
            if trade.id not in trade_ids:
                trade_ids.append(trade.id)
        for trade_id in trade_ids:
            if self._stop.is_set():
                return          # shutting down: never start a new position sync
            lock = self._trade_lock(trade_id)
            if lock.locked():
                continue        # being executed / managed right now; next pass
            async with lock:
                try:
                    await self.sync_trade(trade_id)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Monitor: trade %s sync failed", trade_id)

    async def run_virtual_once(self) -> None:
        """Fast pass: only trades with a waiting virtual entry (cheap; the venue-syncing pass runs less often)."""
        trade_ids: list[int] = []
        for trade_id, pos in await self.db.active_broker_positions():
            if is_virtual_pending(pos) and trade_id not in trade_ids:
                trade_ids.append(trade_id)
        for trade_id in trade_ids:
            if self._stop.is_set():
                return
            lock = self._trade_lock(trade_id)
            if lock.locked():
                continue
            async with lock:
                try:
                    await self.sync_trade(trade_id, virtual_only=True)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Monitor: virtual entry of trade %s failed", trade_id)

    # ---- one trade -----------------------------------------------------------
    async def sync_trade(self, trade_id: int, *, virtual_only: bool = False) -> None:
        """Sync all active positions of one trade (``virtual_only``: just its waiting virtual entries).
        Caller holds the trade lock."""
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

        updated: list[BrokerPosition] = []
        for pos in positions:
            if pos.status in ACTIVE_POSITION and (not virtual_only or is_virtual_pending(pos)):
                pos = await self._sync_position(trade, pos, now)
            updated.append(pos)
        await self._rollup(trade, updated)

    async def _sync_position(self, trade: TradeRecord, pos: BrokerPosition, now: datetime) -> BrokerPosition:
        if is_virtual_pending(pos):     # no venue order exists: the engine watches the reference price
            if self.virtual_hook is None:
                return pos
            try:
                return await self.virtual_hook(trade, pos, now)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Monitor: virtual entry %s #%s failed", pos.broker, trade.id)
                return pos
        broker = self._broker(pos.broker)
        if broker is None:
            if pos.broker not in self._warned_missing:
                self._warned_missing.add(pos.broker)
                logger.warning("Monitor: broker %s is not connected; its positions are not synced", pos.broker)
            return pos
        key = (trade.id, pos.broker)
        before = pos.status
        try:
            new = await broker.sync_position(pos)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            count = self._sync_errors.get(key, 0) + 1
            self._sync_errors[key] = count
            logger.warning("Monitor: sync %s trade #%s failed (%d in a row): %s", pos.broker, trade.id, count, exc)
            if count == SYNC_ERROR_ALERT_AFTER:
                await self.notifier.notify(
                    f"{esc(pos.broker)}: #{trade.id} {esc(trade.plan.base_asset)} pozisyonu {count} kez üst üste "
                    f"kontrol edilemedi: <code>{esc(exc)}</code>",
                    Severity.WARNING,
                )
            await self.check_protection(trade, pos)     # still without a stop? keep telling the owner
            return pos
        self._sync_errors.pop(key, None)
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
]
