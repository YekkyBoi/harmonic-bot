"""Portfolio-level safety guards. Pure functions."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ..models import TradePlan, TradeRecord, TradeStatus

_ACTIVE = frozenset({TradeStatus.OPENING, TradeStatus.PENDING, TradeStatus.OPEN})
_IGNORED_FOR_DUPLICATES = frozenset({TradeStatus.CANCELLED, TradeStatus.FAILED})


def _aware(dt: datetime) -> datetime:
    """Naive datetimes are taken as UTC."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _within_pct(a: float, b: float, tolerance_pct: float) -> bool:
    if b == 0:
        return a == b
    return abs(a - b) / abs(b) * 100.0 <= tolerance_pct


def find_duplicate(
    plan: TradePlan,
    recent_trades: list[TradeRecord],
    *,
    window_sec: int,
    now: datetime,
    entry_tolerance_pct: float = 0.5,
) -> TradeRecord | None:
    """A trade on the same asset and side, created within ``window_sec``, whose entry
    (or stop if entries are missing) is within ``entry_tolerance_pct`` - i.e. the
    signal was probably reposted. Cancelled/failed trades are ignored.

    Returns the most recent match. ``window_sec <= 0`` disables the check. A trade
    built from the same plan (same plan_id) is not its own duplicate.
    """
    if window_sec <= 0:
        return None
    now = _aware(now)
    asset = plan.base_asset.upper()
    matches: list[TradeRecord] = []
    for trade in recent_trades:
        other = trade.plan
        if trade.status in _IGNORED_FOR_DUPLICATES or other.plan_id == plan.plan_id:
            continue
        if other.base_asset.upper() != asset or other.side is not plan.side:
            continue
        age = (now - _aware(trade.created_at)).total_seconds()
        if age > window_sec:
            continue
        if plan.entry_price is not None and other.entry_price is not None:
            similar = _within_pct(plan.entry_price, other.entry_price, entry_tolerance_pct)
        else:
            similar = _within_pct(plan.stop_loss, other.stop_loss, entry_tolerance_pct)
        if similar:
            matches.append(trade)
    if not matches:
        return None
    return max(matches, key=lambda t: _aware(t.created_at))


def max_open_trades_reached(active_trades: list[TradeRecord], limit: int) -> bool:
    """True when the number of OPENING/PENDING/OPEN trades is at least ``limit``.
    limit <= 0 disables the guard."""
    if limit <= 0:
        return False
    return sum(1 for t in active_trades if t.status in _ACTIVE) >= limit


def daily_loss_exceeded(realized_pnl_today: float, reference_equity: float, limit_pct: float) -> bool:
    """True when today's realized loss is at least ``limit_pct`` % of ``reference_equity``.
    limit_pct <= 0 disables the guard. With a loss but no usable equity it fails safe (True)."""
    if limit_pct <= 0 or realized_pnl_today >= 0:
        return False
    if reference_equity <= 0:
        return True
    return -realized_pnl_today >= reference_equity * limit_pct / 100.0


def start_of_day(now: datetime, tz_name: str) -> datetime:
    """Midnight of ``now``'s day in ``tz_name`` (e.g. Europe/Istanbul), returned timezone-aware.
    A naive ``now`` is taken as UTC."""
    tz = ZoneInfo(tz_name)
    local = _aware(now).astimezone(tz)
    return datetime(local.year, local.month, local.day, tzinfo=tz)
