"""Portfolio-level safety guards. Pure functions."""

from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any, TypeGuard
from zoneinfo import ZoneInfo

from ..models import BrokerPosition, PositionStatus, Side, TradePlan, TradeRecord, TradeStatus

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


def _positive(x: Any) -> TypeGuard[float]:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and x > 0


def position_open_risk(pos: BrokerPosition) -> float:
    """Money (account currency) a PENDING or OPEN position still loses if its CURRENT stop is hit: never
    negative, 0 once the stop is at or beyond the entry (break-even or better).

    ``pos.risk_amount`` is the loss the position was sized for: ``risk_basis["quantity"]`` units moving from
    ``risk_basis["entry"]`` to ``risk_basis["stop"]`` (``pos.data["risk_basis"]``, written by the engine when it
    sends the order). When rounding made that size lose MORE than the target (a size raised to the minimum
    lot), the basis carries the real figure as ``risk_basis["loss"]`` and that one counts; a size rounded down
    is not credited (the target counts). That is scaled to what is at risk now:

        risk_amount x (entry -> current stop distance / basis distance) x (size still at risk / basis size)

    * entry: the average fill price of an OPEN position, the basis entry (the limit price) of a PENDING one;
    * size still at risk: ``quantity - (filled_quantity - remaining_quantity)``, i.e. what is open plus what
      may still fill (the whole order while PENDING).
    Without a basis (a position stored by an older version) the full ``risk_amount`` counts, reduced only by
    the share already closed. Without ``risk_amount`` (a waiting virtual entry: no order exists; it is checked
    again when it triggers) the result is 0.

    Not included: slippage or a gap beyond the stop, fees / commission / swap, a profit already locked in by
    a stop beyond the entry, and positions on the account that this bot did not open."""
    if pos.status not in (PositionStatus.PENDING, PositionStatus.OPEN) or not _positive(pos.risk_amount):
        return 0.0
    amount = float(pos.risk_amount or 0.0)
    basis = (pos.data or {}).get("risk_basis")
    basis = basis if isinstance(basis, dict) else {}
    b_loss = basis.get("loss")
    if _positive(b_loss):
        amount = max(amount, float(b_loss))
    b_entry, b_stop, b_qty = basis.get("entry"), basis.get("stop"), basis.get("quantity")
    entry: float | None = None
    if pos.status is PositionStatus.OPEN and _positive(pos.avg_entry_price):
        entry = float(pos.avg_entry_price or 0.0)
    elif _positive(b_entry):
        entry = float(b_entry)
    closed = max(0.0, pos.filled_quantity - pos.remaining_quantity)
    if _positive(b_qty):
        share = max(0.0, pos.quantity - closed) / float(b_qty)
    elif pos.filled_quantity > 0:
        share = min(1.0, max(0.0, pos.remaining_quantity / pos.filled_quantity))
    else:
        share = 1.0
    stop = pos.stop_loss
    if entry is None or not _positive(stop):
        return amount * share                     # cannot tell where the stop is: the planned risk counts
    distance = entry - float(stop or 0.0) if pos.side is Side.LONG else float(stop or 0.0) - entry
    if distance <= 0:
        return 0.0                                # stop at or beyond the entry: nothing left to lose
    if _positive(b_entry) and _positive(b_stop) and abs(float(b_entry) - float(b_stop)) > 0:
        return amount * distance / abs(float(b_entry) - float(b_stop)) * share
    return amount * share


def open_risk(positions: Iterable[BrokerPosition]) -> float:
    """Sum of :func:`position_open_risk` over ``positions``."""
    return sum(position_open_risk(p) for p in positions)


def daily_loss_with_open_risk(realized_pnl_today: float, open_risk_amount: float, new_risk_amount: float) -> float:
    """Worst-case loss of the day if everything still at risk is stopped out: today's realized loss (a
    realized profit counts 0, like in :func:`daily_loss_exceeded`) + the open risk + the new trade's risk."""
    return max(0.0, -realized_pnl_today) + max(0.0, open_risk_amount) + max(0.0, new_risk_amount)


def daily_loss_would_exceed(
    realized_pnl_today: float, open_risk_amount: float, new_risk_amount: float,
    reference_equity: float, limit_pct: float,
) -> bool:
    """True when :func:`daily_loss_with_open_risk` would be MORE than ``limit_pct`` % of
    ``reference_equity`` (exactly the limit is still allowed). limit_pct <= 0 disables the guard; without a
    usable equity it fails safe (True)."""
    if limit_pct <= 0:
        return False
    if not (math.isfinite(reference_equity) and reference_equity > 0):
        return True
    total = daily_loss_with_open_risk(realized_pnl_today, open_risk_amount, new_risk_amount)
    # The tiny factor keeps a total exactly on the limit (3 x 100 of 10 000 at 3%) from counting as "more".
    return total > reference_equity * limit_pct / 100.0 * (1.0 + 1e-9)


def start_of_day(now: datetime, tz_name: str) -> datetime:
    """Midnight of ``now``'s day in ``tz_name`` (e.g. Europe/Istanbul), returned timezone-aware.
    A naive ``now`` is taken as UTC."""
    tz = ZoneInfo(tz_name)
    local = _aware(now).astimezone(tz)
    return datetime(local.year, local.month, local.day, tzinfo=tz)
