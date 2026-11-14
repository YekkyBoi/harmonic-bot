"""What a report card says: read from the engine's own records (the ``trades`` and ``broker_positions`` tables), never
from anything else, and never a money amount.

Rules (one place; the picture, its caption and the text card are all made from the :class:`Report` built here, and
every sentence and mark below is the same on all three):

* **Which trades.** A trade belongs to the period in which it ENDED as a whole: its ``closed_at`` (set when the trade
  reaches CLOSED / CANCELLED / FAILED, i.e. when its last account is done) with ``start <= closed_at < end``. A trade
  that is still open on one account at the cut-off is not on the card; it is on the card of the period in which it
  ends, with the whole of its result. Only a trade in which at least one account really traded (a fill, a realized
  result or a closed position) is listed: an entry that never filled anywhere is not a trade result.
* **A trade whose end can still change.** An optional account's open order that has not answered yet when the trade
  ends is booked by the engine when it answers: the trade is opened again for a moment, its late position is closed
  and the trade gets a NEW ``closed_at``. Such a trade would be on its card without that account, and a second time
  on the next card. So: a trade with such an order still in flight (``pending``) is left out (``Report.deferred``)
  and carried to the next card whatever its ``closed_at`` says by then (``carried``); and a trade that was on the
  last cards is never listed again (``listed``). Each trade is on one weekly card, whole.
* **R of an account in a trade**: :func:`copytrader.engine.monitor.position_result_r`, the definition of the closing
  notice (realized result / the position's real initial risk stored at the open, times the plan's size in R). There
  is no second definition: no stored initial risk (a position opened by an older version, a size the engine limited),
  a result the adapter knows to be partial, a simulated account -> not known.
* **R of a trade**: the mean of its real accounts with a known R. None known: the trade is listed with "—" and is
  in no total, no rate, no best / worst; the card says how many such trades there are. Some known, some not: the
  row and the account's own figure are marked with "*" and the card says in how many trades an account is missing.
* **Break-even**: within +-CLOSING_FLAT_R (0.05R) of zero, as in the closing notice, and such a trade is printed
  "0.0R". Win rate = wins / (wins + losses). Total = the sum of the trades' R (not rounded first). An account's
  figure = the sum of ITS OWN known R over the period's trades (so the accounts do not add up to the total: that one
  sums the per-trade means).
* **The card of every trade so far** (``MODE_ALL``) only, and there only for trades opened BEFORE the version that
  stores a position's real initial risk (trades older than the first one that has such a position: after it, a
  position without that figure lacks it for a reason - a limited size - and stays unknown): an account without a
  known R gets an APPROXIMATE one where the old position stores enough for it:
  ``realized_pnl * plan.risk_r / position.risk_amount`` (the money result in units of the risk that account
  PLANNED: :func:`approximate_result_r`), with the same exclusions (simulated, partial result), not for a size the
  adapter marked as limited (its real risk was far from the planned one) and not when the value is implausible
  (beyond +-APPROX_MAX_R). A trade's R is then the mean of all its accounts' figures; a row or an account figure
  with an approximate part is marked "≈", and so is every figure that includes one. The weekly cards never use it.
* **Note**: how the trade ended, from what each account's adapter recorded (which take profits filled, a stop, a
  close) and from that account's OWN last stop, only where every real account of the trade says the same; otherwise
  empty (:func:`exit_note`).
* **Order and length**: a week's trades oldest first, every trade so far newest first; the date of a row is the UK
  date the trade ended on. The picture's table holds TABLE_ROWS rows: a longer period is handed over newest first,
  so that the rows it shows are the most recent ones and the older ones are the counted rest (they are in every
  figure all the same, as far as their R is known: the line that counts them says so).
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from ..engine.monitor import (
    CLOSING_FLAT_R,
    INITIAL_RISK,
    _result_incomplete,
    _simulated,
    position_result_r,
)
from ..models import BrokerPosition, PositionStatus, TradeRecord, TradeStatus, instrument_label
from ..telegram.formatting import esc
from . import TABLE_ROWS, TITLE
from .uktime import as_utc, date_range_tr, last_slot, stamp_tr, to_uk

logger = logging.getLogger(__name__)

MODE_WEEK, MODE_INTERIM, MODE_ALL = "week", "interim", "all"
TEXT_ROWS_MAX = 40                # trades listed in the text card (the newest; the others are counted)
APPROX_MAX_R = 20.0               # an approximate R beyond this is not believable (a tiny planned risk): unknown
LEVEL_TOL_R = 0.02                # two price levels are "the same" within this share of the trade's stop distance
_DAYS_TR = ("Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz")
_ENDED = (TradeStatus.CLOSED, TradeStatus.CANCELLED, TradeStatus.FAILED)
MINUS = "−"


# --------------------------------------------------------------------------- #
# Results
# --------------------------------------------------------------------------- #
def traded(pos: BrokerPosition) -> bool:
    """The account took part in the trade: something filled, a result was realized or the position was closed (the
    closing notice's own test for "this account traded")."""
    return pos.realized_pnl is not None or pos.status is PositionStatus.CLOSED or pos.filled_quantity > 0


def approximate_result_r(pos: BrokerPosition, risk_r: float) -> float | None:
    """For the card of every trade so far, and only for a position whose R is not known
    (:func:`position_result_r` is None): its realized result in units of the risk the account PLANNED for the trade
    (``risk_amount``, the target the size was computed for), times the plan's size in R. Not the trader's R (the
    real initial risk differs from the target by the lot rounding or a minimum lot), hence always shown with "≈".

    None: a simulated account; no realized result; a result the adapter knows to be partial; a size the adapter
    marked as limited (``data["size_capped"]``: it risked a fraction of the plan, the figure would overstate a win
    many times); figures that are not positive finite numbers; a value beyond +-APPROX_MAX_R (not believable: a
    planned risk of next to nothing)."""
    if _simulated(pos) or pos.realized_pnl is None or _result_incomplete(pos):
        return None
    if (pos.data or {}).get("size_capped"):
        return None
    try:
        pnl, size, risk = float(pos.realized_pnl), float(risk_r), float(pos.risk_amount)   # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(pnl) and math.isfinite(size) and size > 0 and math.isfinite(risk) and risk > 0):
        return None
    value = pnl * size / risk
    return value if math.isfinite(value) and abs(value) <= APPROX_MAX_R else None


def outcome_of(r: float | None) -> str:
    """"unknown" | "be" | "win" | "loss" (flat as in the closing notice: within +-CLOSING_FLAT_R of zero)."""
    if r is None:
        return "unknown"
    if abs(r) <= CLOSING_FLAT_R:
        return "be"
    return "win" if r > 0 else "loss"


# --------------------------------------------------------------------------- #
# The note column
# --------------------------------------------------------------------------- #
_TP_KINDS = ("tp", "take_profit")
_STOP_KINDS = ("sl", "stop")
_CLOSE_KINDS = ("close", "manual_close", "manual")


def _signal_scale(price: object, data: dict[str, Any]) -> float | None:
    """A price of the account's own venue in the signal's price scale (its stored offset and multiplier undone)."""
    from ..engine.orchestrator import price_multiplier, stored_offset, unshift_price     # (see _position_note)

    try:
        value = float(price)          # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    value = unshift_price(value, stored_offset(data)) / price_multiplier(data)
    return value if math.isfinite(value) else None


def _stop_word(trade: TradeRecord, pos: BrokerPosition) -> str | None:
    """What the stop that closed ``pos`` was, judged by the position's OWN last stop (the trade's stop is only the
    trader's wish: an account may have refused a move): "SL" when it is the signal's stop, "BE" when it is at the
    entry (the signal's, or this account's own fill), "stop" when it is elsewhere. None: the position has no stop
    on record. Two levels are the same within LEVEL_TOL_R of the trade's stop distance (tick rounding)."""
    data = pos.data or {}
    stop = _signal_scale(pos.stop_loss, data)
    if stop is None:
        return None
    plan = trade.plan
    entries = [e for e in (plan.entry_price, _signal_scale(pos.avg_entry_price, data)) if e is not None]
    distance = next((abs(e - plan.stop_loss) for e in entries if abs(e - plan.stop_loss) > 0), 0.0)
    tolerance = LEVEL_TOL_R * distance if distance > 0 else 1e-9 * max(1.0, abs(stop))
    if abs(stop - plan.stop_loss) <= tolerance:
        return "SL"
    if any(abs(stop - entry) <= tolerance for entry in entries):
        return "BE"
    return "stop"


def _position_note(trade: TradeRecord, pos: BrokerPosition) -> str | None:
    """How ``pos`` ended, from what its adapter recorded: the take-profit levels that filled ("TP1", "TP2"; "TP"
    when the price fits none of the trade's present levels), then "kapatıldı" (closed by an order of the bot or by
    hand), then the stop ("SL" / "BE" / "stop", :func:`_stop_word`) or "stop-out". Joined with " · ": a list, not an
    order of events. None: not recorded, or not recorded clearly (an exit of a kind this does not know, a part
    that ended outside the bot's orders, a stop that is not on record): nothing is guessed."""
    from ..engine.orchestrator import filled_tp_prices, tp_progress      # (imports this package's users: late)

    d = pos.data or {}
    if _result_incomplete(pos):
        return None
    kinds: set[str] = set()
    recorded = False
    legs = d.get("legs")
    if isinstance(legs, list):
        for leg in legs:
            if not isinstance(leg, dict):
                return None
            try:
                if float(leg.get("filled") or 0.0) <= 0:
                    continue                        # (a leg that never filled says nothing about the exit)
            except (TypeError, ValueError):
                return None
            reason = leg.get("reason")
            if leg.get("state") != "closed" or reason not in ("tp", "sl", "stop_out", "manual"):
                return None
            kinds.add({"tp": "tp", "sl": "stop", "stop_out": "stop_out", "manual": "closed"}[reason])
            recorded = True
    exits = d.get("exits")
    if isinstance(exits, list):
        for item in exits:
            kind = item.get("kind") if isinstance(item, dict) else None
            if kind in _TP_KINDS:
                kinds.add("tp")
            elif kind in _STOP_KINDS:
                kinds.add("stop")
            elif kind in _CLOSE_KINDS:
                kinds.add("closed")
            else:
                return None
            recorded = True
    if not recorded:
        return None
    words: list[str] = []
    hit, _share = tp_progress(trade, [pos])
    levels = [f"TP{i + 1}" for i, done in enumerate(hit) if done]
    if levels:
        words += levels
    elif "tp" in kinds or filled_tp_prices(pos):
        words.append("TP")
    if "closed" in kinds:
        words.append("kapatıldı")
    if "stop" in kinds:
        word = _stop_word(trade, pos)
        if word is None:
            return None
        words.append(word)
    if "stop_out" in kinds:
        words.append("stop-out")
    return " · ".join(words) if words else None


def exit_note(trade: TradeRecord, positions: Sequence[BrokerPosition]) -> str:
    """The note of a trade's row: how it ended, when every real account that traded it says the same thing (the
    simulated ones when no real account traded). Accounts that differ, or one whose end is not recorded clearly:
    empty. Never raises."""
    try:
        took_part = [p for p in positions if traded(p)]
        real = [p for p in took_part if not _simulated(p)]
        notes = {_position_note(trade, p) for p in (real or took_part)}
        if len(notes) != 1:
            return ""
        return next(iter(notes)) or ""
    except Exception:          # noqa: BLE001 - a note is never worth a card
        logger.exception("Karne: note of trade #%s could not be derived", getattr(trade, "id", "?"))
        return ""


# --------------------------------------------------------------------------- #
# The report
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Row:
    """One trade of the period. ``r`` None: not known (listed with "—"); ``approx``: an approximate part in it."""

    trade_id: int
    closed_at: datetime
    pair: str
    side: str                                       # "LONG" | "SHORT"
    r: float | None
    approx: bool = False
    note: str = ""
    # real account that traded it -> (its own R or None, approximate): what the accounts row sums
    accounts: dict[str, tuple[float | None, bool]] = field(default_factory=dict)

    @property
    def outcome(self) -> str:
        return outcome_of(self.r)

    @property
    def shown(self) -> float | None:
        """The figure as it is printed: a break-even trade reads 0.0R (the band of :func:`outcome_of`)."""
        return 0.0 if self.outcome == "be" else self.r

    @property
    def left_out(self) -> tuple[str, ...]:
        """The real accounts that traded it and have no R: they are not in ``r`` and not in their own sum."""
        return tuple(name for name, (value, _approx) in self.accounts.items() if value is None)

    @property
    def partial(self) -> bool:
        """It has a figure, but not from every account that traded it (marked "*")."""
        return self.r is not None and bool(self.left_out)


@dataclass(frozen=True)
class Account:
    name: str
    r: float | None                                 # None: it traded in the period, but no R of it is known
    approx: bool = False
    partial: bool = False                           # the sum leaves out a trade of this account (marked "*")


@dataclass
class Report:
    mode: str                                       # MODE_WEEK | MODE_INTERIM | MODE_ALL
    start: datetime | None                          # None: from the first trade (MODE_ALL)
    end: datetime
    rows: list[Row]                                 # oldest first (MODE_ALL: newest first)
    accounts: list[Account]
    late: bool = False                              # a weekly card sent after its time (the bot was down, or failed)
    deferred: list[int] = field(default_factory=list)   # trades left for a later card (a late order still in flight)
    unsettled: list[int] = field(default_factory=list)  # trades listed after MAX_DEFERRALS cards with the order unanswered

    # ---- figures (known trades only) ----
    @property
    def known(self) -> list[Row]:
        return [row for row in self.rows if row.r is not None]

    @property
    def unknown(self) -> int:
        return len(self.rows) - len(self.known)

    @property
    def approximate(self) -> int:
        """Trades with an approximate figure in them (their row, and with it their accounts' sums)."""
        return sum(1 for row in self.rows if row.approx)

    @property
    def partial(self) -> int:
        """Trades in which an account without a known R is missing from a figure the card shows: from the trade's
        own row, or from that account's marked sum."""
        marked = {a.name for a in self.accounts if a.partial}
        return sum(1 for row in self.rows if row.left_out and (row.r is not None or marked.intersection(row.left_out)))

    @property
    def total_r(self) -> float:
        return math.fsum(row.r for row in self.known if row.r is not None)

    @property
    def wins(self) -> int:
        return sum(1 for row in self.known if row.outcome == "win")

    @property
    def losses(self) -> int:
        return sum(1 for row in self.known if row.outcome == "loss")

    @property
    def flat(self) -> int:
        return sum(1 for row in self.known if row.outcome == "be")

    @property
    def win_rate(self) -> float | None:
        decided = self.wins + self.losses
        return 100.0 * self.wins / decided if decided else None

    def _in_time(self) -> list[Row]:
        return sorted(self.known, key=lambda row: (row.closed_at, row.trade_id))

    @property
    def best(self) -> Row | None:
        """The trade with the highest R; of several, the one that ended first (whatever order the rows are in)."""
        return max(self._in_time(), key=lambda row: row.r or 0.0, default=None)

    @property
    def worst(self) -> Row | None:
        return min(self._in_time(), key=lambda row: row.r or 0.0, default=None)

    @property
    def first(self) -> datetime:
        """Where the period really starts: ``start``, or the end of the first trade (MODE_ALL), or ``end``."""
        if self.start is not None:
            return self.start
        return min((row.closed_at for row in self.rows), default=self.end)

    @property
    def weeks(self) -> int:
        return max(1, round((self.end - self.first) / timedelta(days=7)))

    @property
    def long(self) -> bool:
        """It is about more than the one week: a weekly card that covers a skipped one as well, or a card of the
        "week so far" that starts before this week did (the last weekly card was skipped)."""
        if self.mode == MODE_WEEK:
            return self.weeks > 1
        return self.mode == MODE_INTERIM and self.start is not None and self.start < last_slot(self.end)


def trade_row(trade: TradeRecord, positions: Sequence[BrokerPosition], *, approximate: bool = False) -> Row | None:
    """The row of an ended trade, None when no account traded it. ``approximate``: a trade of an older version on
    the card of every trade so far (see the module text)."""
    if trade.closed_at is None or not any(traded(p) for p in positions):
        return None
    real = [p for p in positions if traded(p) and not _simulated(p)]
    risk_r = trade.plan.risk_r
    accounts: dict[str, tuple[float | None, bool]] = {}
    for pos in real:
        value, rough = position_result_r(pos, risk_r), False
        if value is None and approximate:
            value = approximate_result_r(pos, risk_r)
            rough = value is not None
        accounts[pos.broker] = (value, rough)
    figures = [value for value, _rough in accounts.values() if value is not None]
    r = math.fsum(figures) / len(figures) if figures else None
    return Row(trade_id=trade.id, closed_at=as_utc(trade.closed_at), pair=instrument_label(trade.plan),
               side=trade.plan.side.value.upper(), r=r, approx=any(rough for _value, rough in accounts.values()),
               note=exit_note(trade, positions), accounts=accounts)


def account_sums(rows: Sequence[Row], order: Sequence[str] = ()) -> list[Account]:
    """Per real account the sum of its own known R over ``rows``. ``order``: the configured real accounts, first
    and in that order (one without a trade in the period reads 0); accounts only the records know follow by name.
    An account that traded but has no known R at all: None ("—"); one with a sum that leaves a trade out: partial."""
    sums: dict[str, list[float]] = {}
    rough: set[str] = set()
    seen: set[str] = set()
    missing: set[str] = set()
    for row in rows:
        for name, (value, approx) in row.accounts.items():
            seen.add(name)
            if value is None:
                missing.add(name)
                continue
            sums.setdefault(name, []).append(value)
            if approx:
                rough.add(name)
    names = list(dict.fromkeys(order)) + sorted(seen - set(order))
    return [Account(name, math.fsum(sums[name]) if name in sums else (None if name in seen else 0.0), name in rough,
                    name in sums and name in missing)
            for name in names]


MAX_DEFERRALS = 2                 # a trade left off this many weekly cards is listed on the next one all the same
_pending_failure_logged = False


def _is_pending(pending: Callable[[int], bool] | None, trade_id: int) -> bool:
    """An open order of the trade is still in flight. When the look itself fails that is not knowable: the trade is
    taken as pending (it waits for the next card; after MAX_DEFERRALS cards it is listed with a warning line), and
    the failure is logged once."""
    global _pending_failure_logged
    if pending is None:
        return False
    try:
        return bool(pending(trade_id))
    except Exception:          # noqa: BLE001
        if not _pending_failure_logged:
            _pending_failure_logged = True
            logger.exception("Karne: could not ask whether trade #%s still has an open order in flight; "
                             "such trades are treated as still waiting (logged once)", trade_id)
        return True


async def build_report(db: Any, start: datetime | None, end: datetime, *, mode: str = MODE_WEEK,
                       accounts: Sequence[str] = (), late: bool = False,
                       pending: Callable[[int], bool] | None = None, carried: Sequence[int] = (),
                       listed: Sequence[int] = (), waited: Mapping[int, int] | None = None) -> Report:
    """The report of the trades that ended in ``[start, end)`` (``start`` None: every trade so far), read from
    ``db``. Read only.

    ``pending(trade_id)``: an open order of that trade is still in flight on some account (its answer will change
    the trade): such a trade is not listed, its id is in ``Report.deferred``. ``carried``: trades an earlier card
    deferred - listed now (when they have ended before ``end``), whatever their ``closed_at``. ``listed``: trades
    that were on the last cards - never listed again. (The last two: a period's cards only.) ``waited``: how many
    weekly cards each trade has been left off; one that has waited MAX_DEFERRALS cards is listed although its order
    is still unanswered (``Report.unsettled``: its late account's result may be missing)."""
    # Approximate figures: on the card of every trade only, and only for trades older than the first one that has a
    # position with a stored real initial risk (the version that stores it; None: no such trade yet).
    newer_from: int | None = None
    first_with = getattr(db, "first_trade_with_position_data", None)
    if mode == MODE_ALL and callable(first_with):
        newer_from = await first_with(INITIAL_RISK)
    trades = {trade.id: trade for trade in await db.closed_trades_between(start, end)}
    if mode != MODE_ALL:
        for trade_id in carried:
            if trade_id in trades:
                continue
            trade = await db.get_trade(trade_id)
            if (trade is not None and trade.status in _ENDED and trade.closed_at is not None
                    and as_utc(trade.closed_at) < as_utc(end)):
                trades[trade_id] = trade
        for trade_id in listed:
            trades.pop(trade_id, None)
    rows: list[Row] = []
    deferred: list[int] = []
    unsettled: list[int] = []
    for trade in trades.values():
        if _is_pending(pending, trade.id):
            if (waited or {}).get(trade.id, 0) >= MAX_DEFERRALS:
                unsettled.append(trade.id)
            else:
                deferred.append(trade.id)
                continue
        old = mode == MODE_ALL and (newer_from is None or trade.id < newer_from)
        row = trade_row(trade, await db.broker_positions(trade.id), approximate=old)
        if row is not None:
            rows.append(row)
    rows.sort(key=lambda row: (row.closed_at, row.trade_id), reverse=mode == MODE_ALL)
    return Report(mode=mode, start=as_utc(start) if start is not None else None, end=as_utc(end), rows=rows,
                  accounts=account_sums(rows, accounts), late=late, deferred=sorted(deferred),
                  unsettled=sorted(i for i in unsettled if any(row.trade_id == i for row in rows)))


# --------------------------------------------------------------------------- #
# Texts
# --------------------------------------------------------------------------- #
def fmt_r(value: float, approx: bool = False, partial: bool = False) -> str:
    """"+6.2R", "−1.0R", "0.0R" (one decimal, U+2212 for minus: as on the picture); "≈" in front when approximate,
    "*" behind when an account is missing from it."""
    shown = round(float(value), 1)
    body = f"{abs(shown):.1f}"
    sign = "+" if shown > 0 else MINUS if shown < 0 else ""
    return f"{'≈' if approx else ''}{sign}{body}R{'*' if partial else ''}"


def fmt_rate(rate: float | None, approx: bool = False) -> str:
    """"%63" (rounded half up, as on the picture), "—" when there is none."""
    if rate is None:
        return "—"
    return f"{'≈' if approx else ''}%{int(math.floor(rate + 0.5))}"


def _row_result(row: Row) -> str:
    return fmt_r(row.shown, row.approx, row.partial) if row.shown is not None else "—"


def _account_result(account: Account) -> str:
    return fmt_r(account.r, account.approx, account.partial) if account.r is not None else "—"


def heading(report: Report) -> str:
    """The subtitle of the card: what it is and the real dates it covers (UK dates)."""
    if report.mode == MODE_ALL:
        return f"TÜM İŞLEMLER · {date_range_tr(report.first, report.end)}"
    if report.mode == MODE_INTERIM:
        return f"ARA KARNE · {date_range_tr(report.first, report.end)}"
    kind = "HAFTALIK KARNE" if report.weeks == 1 else f"{report.weeks} HAFTALIK KARNE"
    return f"{kind} · {date_range_tr(report.first, report.end)}"


def period_line(report: Report) -> str:
    """The exact period, for the message that carries the card."""
    if report.mode == MODE_ALL:
        return f"Dönem: ilk işlemden {stamp_tr(report.end, year=True)} anına kadar (İngiltere saati)"
    return f"Dönem: {stamp_tr(report.first)} – {stamp_tr(report.end)} (İngiltere saati)"


def footnotes(report: Report) -> list[str]:
    """What the marks on the card mean and how many trades they concern: the SAME sentences under the picture's
    table, in its caption and in the text card (there with a full stop)."""
    notes = []
    if report.approximate:
        notes.append(f"≈ yaklaşık: {report.approximate} işlemde gerçek risk kayıtlı değil; R, hesabın planlanan "
                     "riskine göre hesaplandı")
    if report.partial:
        notes.append(f"* {report.partial} işlemde R’si bilinmeyen hesap, işlemin ortalamasına ve kendi toplamına "
                     "girmedi")
    if report.unknown:
        notes.append(f"— R’si bilinmeyen {report.unknown} işlem toplama ve kazanma oranına girmedi")
    return notes


def more_line(hidden: Sequence[Row]) -> str:
    """The line that counts the trades a list does not show. They are in every figure - as far as their R is
    known, and the line says so when it is not."""
    count = len(hidden)
    known = sum(1 for row in hidden if row.r is not None)
    if known == count:
        return f"… ve {count} işlem daha (toplama dahil)"
    if known == 0:
        return f"… ve {count} işlem daha (R’leri bilinmiyor: toplamda yok)"
    return f"… ve {count} işlem daha ({known} tanesi toplama dahil, {count - known} tanesinin R’si bilinmiyor)"


def _mode_lines(report: Report) -> list[str]:
    lines = []
    if report.mode == MODE_INTERIM:
        what = "Dönem" if report.long else "Hafta"
        lines.append(f"{what} sürüyor: yalnızca tamamen kapanmış işlemler sayıldı, açık işlemler dahil değil.")
        if report.long:
            lines.append("Önceki haftalık karne gönderilemediği için o haftanın işlemleri de bu karnede.")
    if report.mode == MODE_WEEK and report.long:
        lines.append(f"Önceki karne gönderilemediği için bu karne {report.weeks} haftayı kapsıyor.")
    if report.late:
        lines.append("Bu karne pazar 22:00'de gönderilemedi; şimdi gönderildi.")
    earlier = [row for row in report.rows if row.closed_at < report.first]
    if earlier:
        days = sorted({to_uk(row.closed_at).strftime("%d.%m") for row in earlier},
                      key=lambda day: (day[3:], day[:2]))
        shown = ", ".join(days[:3]) + (" …" if len(days) > 3 else "")
        lines.append(f"{len(earlier)} işlem önceki haftadan ({shown} tarihinde kapanmış) bu karneye alındı: bir "
                     "hesabın açılış emri geç yanıt verdi.")
    if report.unsettled:
        lines.append(f"{len(report.unsettled)} işlem {MAX_DEFERRALS} karne boyunca bekletildi ve yine de listelendi: "
                     "bir hesabın açılış emri hâlâ yanıtsız, o hesabın sonucu eksik olabilir.")
    if report.deferred:
        lines.append(f"Bir hesabın açılış emri hâlâ yanıt beklediği için {len(report.deferred)} işlem bu karneye "
                     "alınmadı; sonuçlanınca yer alacak.")
    lines += [f"{note}." for note in footnotes(report)]
    return lines


def _result_line(report: Report) -> str:
    rough = report.approximate > 0
    count = f"{len(report.known)} işlem"
    if report.known:
        count += f" ({report.wins} kazanç · {report.losses} kayıp)"
    return f"Toplam {fmt_r(report.total_r, rough)} · {count} · kazanma {fmt_rate(report.win_rate, rough)}"


def _heading_tr(report: Report) -> str:
    what = {MODE_ALL: "Tüm işlemler", MODE_INTERIM: "Ara karne"}.get(
        report.mode, "Haftalık karne" if report.weeks == 1 else f"{report.weeks} haftalık karne")
    return f"{what} · {date_range_tr(report.first, report.end)}"


def caption(report: Report) -> str:
    """The text under the picture (HTML, well below Telegram's caption limit)."""
    return "\n".join([f"📊 <b>{esc(_heading_tr(report))}</b>", period_line(report), _result_line(report),
                      *_mode_lines(report)])


def _row_line(row: Row) -> str:
    wall = to_uk(row.closed_at)
    note = f" · {esc(row.note)}" if row.note else ""
    return (f"{wall.strftime('%d.%m')} {_DAYS_TR[wall.weekday()]} · {esc(row.pair)} {row.side} · "
            f"<b>{_row_result(row)}</b>{note}")


def summary_text(report: Report, extra: str = "") -> str:
    """The card as plain text (HTML for Telegram): what goes out when the picture cannot be made or sent. ``extra``:
    one line that says why there is no picture."""
    rough = report.approximate > 0
    lines = [f"📊 <b>{esc(TITLE)}</b>", esc(_heading_tr(report)), period_line(report), ""]
    lines.append(f"<b>Toplam: {fmt_r(report.total_r, rough)}</b> ({len(report.known)} işlemin R toplamı)")
    if report.known:
        lines.append(f"{report.wins} kazanç · {report.losses} kayıp · kazanma oranı {fmt_rate(report.win_rate, rough)}")
        best, worst = report.best, report.worst
        if best is not None and worst is not None and best.shown is not None and worst.shown is not None:
            lines.append(f"En iyi: {fmt_r(best.shown, best.approx)} {esc(best.pair)} {best.side} · "
                         f"En kötü: {fmt_r(worst.shown, worst.approx)} {esc(worst.pair)} {worst.side}")
    if report.accounts:
        lines.append("Hesaplar: " + " · ".join(f"{esc(a.name)} {_account_result(a)}" for a in report.accounts))
    lines.append("")
    if not report.rows:
        lines.append("Henüz kapanmış işlem yok." if report.mode == MODE_ALL else "Bu dönemde kapanan işlem yok.")
    else:
        lines.append("<b>İşlemler</b>")
        in_time = sorted(report.rows, key=lambda row: (row.closed_at, row.trade_id))
        hidden, newest = in_time[:-TEXT_ROWS_MAX], in_time[-TEXT_ROWS_MAX:]
        if report.mode == MODE_ALL:
            newest.reverse()
        lines += [_row_line(row) for row in newest]
        if hidden:
            lines.append(more_line(hidden))
    notes = _mode_lines(report)
    if notes:
        lines += ["", *notes]
    if extra:
        lines += ["", extra]
    return "\n".join(lines)


def _stat(row: Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {"pair": row.pair, "side": row.side, "r": row.shown, "approx": row.approx}


def render_data(report: Report) -> dict[str, Any]:
    """The dictionary the drawing process gets (design.render): JSON only, no money amount, ``sample`` false (the
    "ÖRNEK VERİ" mark of the designer's sample data is never on a real card).

    ``trades`` holds only the rows the table DRAWS, however long the period is (every trade so far can be
    thousands): all of them up to TABLE_ROWS, else the TABLE_ROWS - 1 most recent, and ``hidden`` counts the rest.
    They are in the report's order (a week: oldest first, as the design shows it; every trade so far: newest first),
    except that a period too long for the table is always given newest first, so that the rows drawn are the most
    recent ones.

    The figures of the whole period (``aggregates``), best / worst, the footnotes and the line that counts the
    hidden rows are GIVEN to the picture: it shows the same figures, the same trades and the same sentences as the
    caption and the text card, and adds nothing up by itself."""
    rows = list(report.rows)
    if report.mode != MODE_ALL and len(rows) > TABLE_ROWS:
        rows.reverse()
    drawn = rows if len(rows) <= TABLE_ROWS else rows[:TABLE_ROWS - 1]
    hidden = rows[len(drawn):]
    best, worst = _stat(report.best), _stat(report.worst)
    data: dict[str, Any] = {
        "title": TITLE,
        "subtitle": heading(report),
        "footer": "",
        "sample": False,
        "trades": [{"date": to_uk(row.closed_at).date().isoformat(), "pair": row.pair, "side": row.side,
                    "r": row.r, "outcome": row.outcome, "note": row.note, "approx": row.approx,
                    "partial": row.partial}
                   for row in drawn],
        "hidden": len(hidden),
        "accounts": [{"name": a.name, "r": a.r, "approx": a.approx, "partial": a.partial} for a in report.accounts],
        "aggregates": {"wins": report.wins, "losses": report.losses, "be": report.flat, "unknown": report.unknown,
                       "approx": report.approximate, "partial": sum(1 for row in report.rows if row.partial),
                       "total_r": report.total_r, "win_rate": report.win_rate, "best": best, "worst": worst},
        "footnotes": footnotes(report),
    }
    if hidden:
        data["more_text"] = more_line(hidden)
    if report.mode == MODE_ALL:
        data.update(hero_label="TOPLAM SONUÇ", table_label="İŞLEMLER", empty_text="Henüz kapanmış işlem yok")
    elif report.mode == MODE_WEEK and report.long:
        data.update(hero_label="DÖNEM SONUCU", table_label="DÖNEMİN İŞLEMLERİ", empty_text="Bu dönemde işlem yok")
    elif report.mode == MODE_INTERIM and report.long:
        data.update(hero_label="DÖNEM SONUCU · ŞU ANA KADAR", table_label="DÖNEMİN İŞLEMLERİ",
                    empty_text="Bu dönemde kapanan işlem yok")
    elif report.mode == MODE_INTERIM:
        data.update(hero_label="HAFTALIK SONUÇ · ŞU ANA KADAR", empty_text="Bu hafta kapanan işlem yok")
    return data


__all__ = ["APPROX_MAX_R", "MODE_ALL", "MODE_INTERIM", "MODE_WEEK", "Account", "Report", "Row", "account_sums",
           "approximate_result_r", "build_report", "caption", "exit_note", "fmt_r", "fmt_rate", "footnotes",
           "heading", "more_line", "outcome_of", "period_line", "render_data", "summary_text", "trade_row", "traded"]
