"""Background position monitor.

Every ``monitor_interval_sec``: sync each PENDING/OPEN broker position with
its venue, cancel stale limit entries and roll the trade status up. One bad
position never stops the loop.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
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


def position_opened_at(pos: BrokerPosition, fallback: datetime) -> datetime:
    """Placement time from adapter data ("opened_at": epoch ms/s or ISO), else ``fallback``."""
    raw = pos.data.get("opened_at")
    try:
        if isinstance(raw, (int, float)) and raw > 0:
            seconds = raw / 1000.0 if raw > 1e11 else float(raw)
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        if isinstance(raw, str) and raw:
            return utc(datetime.fromisoformat(raw))
        if isinstance(raw, datetime):
            return utc(raw)
    except (ValueError, OverflowError, OSError):
        pass
    return utc(fallback)


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
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Monitor pass failed")
            await asyncio.sleep(interval)

    async def run_once(self) -> None:
        trade_ids: list[int] = []
        for trade_id, _pos in await self.db.active_broker_positions():
            if trade_id not in trade_ids:
                trade_ids.append(trade_id)
        for trade in await self.db.active_trades():   # also trades whose positions are all terminal
            if trade.id not in trade_ids:
                trade_ids.append(trade.id)
        for trade_id in trade_ids:
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

    # ---- one trade -----------------------------------------------------------
    async def sync_trade(self, trade_id: int) -> None:
        """Sync all active positions of one trade. Caller holds the trade lock."""
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
            if pos.status in ACTIVE_POSITION:
                pos = await self._sync_position(trade, pos, now)
            updated.append(pos)
        await self._rollup(trade, updated)

    async def _sync_position(self, trade: TradeRecord, pos: BrokerPosition, now: datetime) -> BrokerPosition:
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

        if new.status is PositionStatus.PENDING:
            new = await self._expire_pending(broker, trade, new, now)
        return new

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
        await self.notifier.notify(
            f"🏁 #{trade.id} {esc(trade.plan.base_asset)} {trade.plan.side.value.upper()} (giriş {entry}) {word}. "
            f"Gerçekleşen K/Z: {pnl_text}",
            Severity.INFO,
        )


__all__ = ["Monitor", "rollup_status", "position_opened_at", "utc", "ACTIVE_POSITION", "ACTIVE_TRADE"]
