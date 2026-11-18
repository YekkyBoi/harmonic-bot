"""The owner's trade card: what one picture of ONE trade on ONE of his accounts says (``/<hesap>kart``).

This module is pure: it reads nothing and draws nothing. The service (:mod:`.tradecard_service`) reads the bot's own
records (the trade, that account's position, the audit events) and the venue's candles, and hands them to
:func:`card_data`, which builds the small dictionary the drawing process (:mod:`.tradecard_design`, Pillow, another
process: :mod:`.render`) draws. Every figure comes from those records; nothing is guessed - what cannot be known
makes the card a short text answer instead (:class:`CardProblem`).

Rules (the owner's, one place):

* **Which trades** (the numbered list): the account's positions of THIS WEEK - the week of the weekly report card,
  from the last Sunday 22:00 UK time (:func:`week_start`) - that are open or pending now, or ended in the week, or
  whose trade started in the week; an entry that never filled and is no longer waiting is not a trade. Oldest first.
* **Which card**: decided when the number is picked, from a fresh read of that account's position: PENDING / OPEN
  -> the ENTRY card, anything else that traded -> the RESULT card.
* **Levels** in the account's own price scale (its stored price multiplier and offset, as the engine placed them):
  the entry = the position's fill price (pending: its limit price); the stop = the signal's ORIGINAL stop (the
  plan's ``stop_loss``, which a stop move never changes: moves appear only in the timeline); the target = the LAST of
  the plan's take profits (none: no target zone).
* **Result**: the close price = the volume-weighted price of the position's exits (MetaTrader: its closing deals;
  none on record: no card);
  the result in R = :func:`copytrader.engine.monitor.position_result_r` (the closing notice's own figure); without it
  (a simulated / hand-traded account, an older position) the price move, ``side x (close - entry) / (entry - stop)``,
  times the plan's size in R.
* **Candles**: the chart's bar size from the signal's picture (``plan.chart_timeframe_min``), 15 minutes when it was
  not read or the venue has no such bar size; BARS_BEFORE bars before the entry; the entry card runs on to the latest
  bar (the dotted line is the price now), the result card until the price touches the target or the stop after the
  close (+ AFTER_TOUCH_BARS) or RESULT_AFTER_BARS bars, whichever is later (as far as there are candles).
* **Times** in UK time (Europe/London, :mod:`.uktime`).
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from ..engine.monitor import _simulated, position_result_r
from ..models import BrokerPosition, EntryType, PositionStatus, Side, TradeRecord, instrument_label
from .uktime import UTC, as_utc, last_slot, stamp_tr, to_uk

DEFAULT_TIMEFRAME_MIN = 15        # the owner's: a signal whose chart's bar size was not read
BARS_BEFORE = 58                  # candles before the entry
MIN_BARS_BEFORE = 20              # ... at least (a long trade shortens what is shown before it)
ENTRY_BOX_BARS = 20               # the entry card's setup box: this long after the entry
ENTRY_ROOM_BARS = 28              # empty room after the entry on the entry card (the trade still to play out)
RESULT_AFTER_BARS = 24            # the result card: candles after the close at least ...
AFTER_TOUCH_BARS = 10             # ... or until the target / stop is touched after the close, plus these
TOUCH_SEARCH_BARS = 160           # a touch is looked for this far after the close
MAX_SLOTS = 240                   # candles one card holds (more: fewer candles before the entry)
END_ROOM_BARS = 6                 # a result card whose candles end at the close keeps this much room after it
FOOTER = "İngiltere saati"
MINUS = "−"
EVENT_KINDS = ("order_send", "entry_filled", "manage_done", "manage_late_done", "pozkapat", "close_all")
_TP_WORDS = frozenset({"tp", "take_profit"})
_STOP_WORDS = frozenset({"sl", "stop", "stop_out", "stop_replaced"})
_ACTIVE = (PositionStatus.PENDING, PositionStatus.OPEN)
_LABEL_STEPS_MIN = (15, 30, 60, 120, 180, 240, 360, 720, 1440, 2880, 4320, 10080, 20160, 43200)
_MAX_TIME_LABELS = 12


class CardProblem(Exception):
    """The card cannot be made; ``str()`` is the owner's short Turkish reason (HTML-safe)."""


# --------------------------------------------------------------------------- #
# The week and the list
# --------------------------------------------------------------------------- #
def week_start(now: datetime) -> datetime:
    """The start of the weekly report card's week: the last Sunday 22:00 UK time (UTC)."""
    return last_slot(now)


def traded(pos: BrokerPosition) -> bool:
    """Something filled on the account (a card has an entry to show)."""
    return pos.filled_quantity > 0 or (pos.avg_entry_price is not None and pos.status is not PositionStatus.PENDING)


def listed(rows: Iterable[tuple[TradeRecord, BrokerPosition, datetime | None]]
           ) -> list[tuple[TradeRecord, BrokerPosition, datetime | None]]:
    """The rows of the numbered list: active now, or traded (an entry that never filled and is over is no trade)."""
    return [row for row in rows if row[1].status in _ACTIVE or traded(row[1])]


def state_word(pos: BrokerPosition) -> str:
    if pos.status is PositionStatus.PENDING:
        return "bekliyor"
    if pos.status is PositionStatus.OPEN:
        return "açık"
    return "kapandı"


def list_label(trade: TradeRecord, pos: BrokerPosition) -> str:
    """"BTC LONG · #12 · 17.11 Pzt 07:55 · açık" (plain text; the caller escapes it)."""
    return (f"{instrument_label(trade.plan)} {trade.plan.side.value.upper()} · #{trade.id} · "
            f"{stamp_tr(trade.created_at)} · {state_word(pos)}")


# --------------------------------------------------------------------------- #
# Levels in the account's own price scale
# --------------------------------------------------------------------------- #
def venue_price(price: float | None, data: Mapping[str, Any] | None) -> float | None:
    """A signal-scale price on the account's venue: times its stored price multiplier, moved by its stored offset
    (exactly as the engine placed its levels at the open). None: not a positive finite number."""
    from ..engine.orchestrator import price_multiplier, shift_price, stored_offset     # (that module imports karne)

    try:
        value = float(price)          # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    data = dict(data or {})
    out = shift_price(value * price_multiplier(data), stored_offset(data))
    return out if math.isfinite(out) and out > 0 else None


def original_stop(trade: TradeRecord, pos: BrokerPosition) -> float | None:
    """The stop the trade was opened with, in the account's scale (never a moved one)."""
    return venue_price(trade.plan.stop_loss, pos.data)


def card_target(trade: TradeRecord, pos: BrokerPosition) -> float | None:
    """The LAST of the plan's take profits in the account's scale (None: the signal had none)."""
    if not trade.plan.take_profits:
        return None
    return venue_price(trade.plan.take_profits[-1].price, pos.data)


def entry_price(trade: TradeRecord, pos: BrokerPosition) -> float | None:
    """The fill price; a pending entry: its limit price (a MetaTrader leg's own, else the plan's in this scale)."""
    if pos.avg_entry_price is not None and pos.status is not PositionStatus.PENDING:
        try:
            value = float(pos.avg_entry_price)
            if math.isfinite(value) and value > 0:
                return value
        except (TypeError, ValueError):
            pass
    for leg in (pos.data or {}).get("legs") or []:
        if isinstance(leg, dict) and leg.get("entry"):
            try:
                value = float(leg["entry"])
            except (TypeError, ValueError):
                continue
            if math.isfinite(value) and value > 0:
                return value
    return venue_price(trade.plan.entry_price, pos.data)


def price_digits(pos: BrokerPosition, reference: float) -> int:
    """Decimals a price of this instrument is written with: from the venue's price tick when the position keeps it,
    else from the size of the price (BTC 1, ETH 2, EURUSD 5)."""
    data = pos.data or {}
    ticks = [data.get("price_tick"), (data.get("symbol_info") or {}).get("price_tick")
             if isinstance(data.get("symbol_info"), dict) else None]
    for tick in ticks:
        try:
            t = float(tick)           # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        if math.isfinite(t) and 1e-9 < t < 1e6:
            digits = 0
            while digits < 8 and abs(t * 10 ** digits - round(t * 10 ** digits)) > 1e-6:
                digits += 1
            if not (digits == 8 and t < 1e-7):
                return digits
    if not (math.isfinite(reference) and reference > 0):
        return 2
    return max(0, min(8, 5 - math.floor(math.log10(reference))))


def shown_symbol(broker_symbol: str) -> str:
    """The venue's symbol as the owner knows it: "BTC/USDT:USDT" -> "BTCUSDT", "US100.cash" as it is."""
    text = (broker_symbol or "").strip()
    if "/" in text:
        text = text.split(":", 1)[0].replace("/", "")
    return text


def timeframe_of(trade: TradeRecord, supported: Iterable[int]) -> int:
    """The bar size the card is drawn in: the signal picture's (``plan.chart_timeframe_min``) when the venue has it,
    else DEFAULT_TIMEFRAME_MIN."""
    tf = getattr(trade.plan, "chart_timeframe_min", None)
    return int(tf) if isinstance(tf, int) and tf in set(supported) else DEFAULT_TIMEFRAME_MIN


# --------------------------------------------------------------------------- #
# What the position's records say
# --------------------------------------------------------------------------- #
def _ms_time(value: object) -> datetime | None:
    """An adapter's time (epoch ms or s) as UTC; None: not one."""
    try:
        number = float(value)         # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    seconds = number / 1000.0 if number > 1e11 else number
    try:
        return datetime.fromtimestamp(seconds, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


@dataclass
class Deal:
    """One fill of the position on the venue (MetaTrader's deals; UTC)."""
    time: datetime
    price: float
    volume: float
    entry: bool                       # True: an opening deal
    reason: str = "manual"


@dataclass
class Records:
    """What happened to the position, as far as the records tell (UTC times)."""
    placed: datetime | None = None    # the entry order was sent
    filled: datetime | None = None    # the (first) fill
    closed: datetime | None = None    # the (last) exit
    exit_price: float | None = None   # volume-weighted price of the exits
    reason: str | None = None         # "tp" | "stop" | "close": how the position ended (its LAST exit)
    moves: list[tuple[datetime, str]] = field(default_factory=list)      # stop moves (timeline)
    close_by: str | None = None       # who closed it: "group" | "owner" | "all" | None


def _reason_word(raw: object) -> str:
    text = str(raw or "").lower()
    if text in _TP_WORDS:
        return "tp"
    if text in _STOP_WORDS:
        return "stop"
    return "close"


def _weighted(pairs: Sequence[tuple[float, float]]) -> float | None:
    total = sum(q for _p, q in pairs if q > 0)
    if total <= 0:
        return None
    value = sum(p * q for p, q in pairs if q > 0) / total
    return value if math.isfinite(value) and value > 0 else None


def _for_broker(outcomes: object, broker: str) -> bool:
    return isinstance(outcomes, list) and any(
        isinstance(o, dict) and o.get("broker") == broker and o.get("kind") == "ok" for o in outcomes)


def read_records(trade: TradeRecord, pos: BrokerPosition, closed_at: datetime | None,
                 events: Sequence[Mapping[str, Any]], deals: Sequence[Deal] | None = None) -> Records:
    """The position's story from the adapter's data (ccxt / paper: times and exits it keeps), the venue's deals
    (MetaTrader, ``deals``), the audit events of the trade (``events``: :data:`EVENT_KINDS`, oldest first) and the
    time the bot saw the position end (``closed_at``)."""
    rec = Records()
    data = pos.data or {}
    broker, tid = pos.broker, trade.id
    mine = [e for e in events if isinstance(e.get("payload"), dict)]
    for ev in mine:
        p, kind, ts = ev["payload"], ev.get("kind"), ev.get("ts")
        if not isinstance(ts, datetime):
            continue
        ts = as_utc(ts)
        if kind == "order_send" and p.get("trade_id") == tid and p.get("broker") == broker and rec.placed is None:
            rec.placed = ts
        elif kind == "entry_filled" and p.get("trade_id") == tid and p.get("broker") == broker and rec.filled is None:
            rec.filled = ts
        elif kind == "manage_done" and p.get("trade_id") == tid and _for_broker(p.get("outcomes"), broker):
            if p.get("action") == "move_sl":
                plan = p.get("plan") if isinstance(p.get("plan"), dict) else {}
                rec.moves.append((ts, "Stop girişe çekildi" if plan.get("move_sl_to_entry") else "Stop taşındı"))
            elif p.get("action") == "close" and rec.close_by is None:
                rec.close_by = "group"
        elif (kind == "manage_late_done" and p.get("trade_id") == tid and p.get("broker") == broker
              and p.get("kind") == "ok" and p.get("action") == "move_sl"):
            rec.moves.append((ts, "Stop taşındı"))
        elif kind == "pozkapat" and [broker, tid] in [list(x) for x in p.get("closes") or [] if isinstance(x, list)]:
            rec.close_by = "owner"
        elif kind == "close_all" and tid in (p.get("trades") or []):
            rec.close_by = "all"
    if deals:
        ins = [d for d in deals if d.entry]
        outs = [d for d in deals if not d.entry]
        if ins:
            rec.filled = min(d.time for d in ins)
        if outs and pos.status not in _ACTIVE:
            last = max(outs, key=lambda d: d.time)
            rec.closed = last.time
            rec.exit_price = _weighted([(d.price, d.volume) for d in outs])
            rec.reason = _reason_word(last.reason)
    else:
        opened = _ms_time(data.get("opened_at"))
        if opened is not None and traded(pos):
            rec.filled = opened
        exits = [x for x in data.get("exits") or [] if isinstance(x, dict)]
        if pos.status not in _ACTIVE:
            priced: list[tuple[float, float]] = []
            for x in exits:
                try:
                    price, qty = float(x.get("price")), float(x.get("qty") or 0.0)   # type: ignore[arg-type]
                except (TypeError, ValueError):
                    continue
                if math.isfinite(price) and price > 0 and math.isfinite(qty):
                    priced.append((price, qty))
            rec.exit_price = _weighted(priced)
            times = [t for t in (_ms_time(x.get("at")) for x in exits) if t is not None]
            rec.closed = _ms_time(data.get("closed_at")) or (max(times) if times else None)
            last_kind = exits[-1].get("kind") if exits else None
            legs = [leg for leg in data.get("legs") or [] if isinstance(leg, dict) and leg.get("reason")]
            raw = last_kind or data.get("closed_reason") or (legs[-1].get("reason") if legs else None)
            rec.reason = _reason_word(raw) if raw else None
    if pos.status not in _ACTIVE:
        if rec.closed is None and closed_at is not None:
            rec.closed = as_utc(closed_at)
        if rec.reason is None:
            rec.reason = "close"
    if rec.placed is None:
        rec.placed = as_utc(trade.created_at)       # (an older trade without the event: the trade's own start)
    if rec.filled is None and traded(pos) and trade.plan.entry_type is EntryType.MARKET:
        rec.filled = rec.placed
    rec.moves.sort(key=lambda m: m[0])
    if rec.closed is not None:
        rec.moves = [m for m in rec.moves if m[0] <= rec.closed]
    return rec


def result_r(trade: TradeRecord, pos: BrokerPosition, fill: float, stop: float, exit_price: float | None
             ) -> float | None:
    """The result in R: the closing notice's own figure (:func:`position_result_r`); a simulated (hand-traded)
    account or one without it: the price move in units of the original stop distance, times the plan's size in R."""
    if not _simulated(pos):
        value = position_result_r(pos, trade.plan.risk_r)
        if value is not None:
            return value
    risk = abs(fill - stop)
    if exit_price is None or not risk > 0:
        return None
    sign = 1.0 if trade.plan.side is Side.LONG else -1.0
    value = sign * (exit_price - fill) / risk * float(trade.plan.risk_r or 1.0)
    return value if math.isfinite(value) else None


# --------------------------------------------------------------------------- #
# The card's dictionary
# --------------------------------------------------------------------------- #
def fmt_num(value: float, digits: int) -> str:
    """Turkish number: "27.300,00"."""
    text = f"{value:,.{max(0, digits)}f}"
    return text.replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_r(value: float) -> str:
    """"+1.70R" / "−1.00R" (the card's hero and timeline)."""
    return f"{'+' if value >= 0 else MINUS}{abs(value):.2f}R"


def _duration(minutes: int) -> str:
    if minutes >= 1440:
        return f"{minutes // 1440} g {(minutes % 1440) // 60:02d} sa"
    return f"{minutes // 60} sa {minutes % 60:02d} dk"


def _bar_index(bars: Sequence[Sequence[float]], moment: datetime) -> int | None:
    """The bar the moment falls in: the last one that opens at or before it."""
    ts = as_utc(moment).timestamp()
    found = None
    for k, bar in enumerate(bars):
        if bar[0] <= ts:
            found = k
        else:
            break
    return found


def _time_labels(times: Sequence[float], tf_min: int) -> list[tuple[int, str]]:
    """Where the time axis is labelled (UK wall time): at the first candle of each step (a step that gives at most
    _MAX_TIME_LABELS labels), "17.11" at the first candle of a new day, else "09:00"."""
    if len(times) < 2:
        return []
    span = (times[-1] - times[0]) / 60.0 + tf_min
    step = next((s for s in _LABEL_STEPS_MIN if s >= tf_min and span / s <= _MAX_TIME_LABELS), _LABEL_STEPS_MIN[-1])
    out: list[tuple[int, str]] = []
    walls = [to_uk(datetime.fromtimestamp(t, tz=UTC)) for t in times]

    def bucket(w: datetime) -> int:
        minutes = (w - datetime(2000, 1, 3)).total_seconds() // 60      # (a Monday: weeks start on Mondays)
        return int(minutes // step)

    for k in range(1, len(walls)):
        if bucket(walls[k]) != bucket(walls[k - 1]):
            new_day = walls[k].date() != walls[k - 1].date()
            out.append((k, walls[k].strftime("%d.%m") if new_day else walls[k].strftime("%H:%M")))
    return out


def _first_bar(ie: int, last: int, before: int) -> int:
    """The first candle drawn: ``before`` candles before the entry, fewer (at least MIN_BARS_BEFORE) when the card
    would hold more than MAX_SLOTS candles."""
    first = max(0, ie - before)
    if last - first + 1 > MAX_SLOTS:
        first = max(0, min(max(first, last + 1 - MAX_SLOTS), ie - MIN_BARS_BEFORE))
    return first


def card_data(
    trade: TradeRecord, pos: BrokerPosition, account: str, rec: Records, bars: Sequence[Sequence[float]],
    tf_min: int, *, manual: bool = False, now: datetime | None = None,
) -> dict[str, Any]:
    """The card's dictionary for the drawing process (JSON; see tradecard_design.render). ``bars``: the venue's
    candles, UTC open time (s), open, high, low, close, oldest first, covering the trade (see the module text).
    Raises CardProblem with the owner's reason when a figure the card needs is not known."""
    kind = "entry" if pos.status in _ACTIVE else "result"
    pending = pos.status is PositionStatus.PENDING
    if kind == "result" and not traded(pos):
        raise CardProblem("bu işlemde bu hesapta giriş hiç dolmadı; gösterilecek bir işlem yok")
    long_ = trade.plan.side is Side.LONG
    sign = 1.0 if long_ else -1.0
    fill = entry_price(trade, pos)
    stop = original_stop(trade, pos)
    target = card_target(trade, pos)
    if fill is None:
        raise CardProblem("giriş fiyatı kayıtlarda yok")
    if stop is None or not sign * (fill - stop) > 0:
        raise CardProblem("işlemin ilk stopu bu hesabın fiyatında okunamadı")
    if target is not None and not sign * (target - fill) > 0:
        target = None                  # (a target on the wrong side of this fill is no target of this card)
    risk = abs(fill - stop)
    bars = [tuple(float(v) for v in bar[:5]) for bar in bars if len(bar) >= 5]
    bars = [b for b in bars if all(math.isfinite(v) for v in b) and min(b[1:5]) > 0]
    if len(bars) < 2:
        raise CardProblem("bu işlemin mumları okunamadı")
    bar_sec = int(tf_min) * 60
    anchor = rec.placed if pending else (rec.filled or rec.placed)
    if anchor is None:
        raise CardProblem("girişin zamanı kayıtlarda yok")
    ie = _bar_index(bars, anchor)
    if ie is None:
        raise CardProblem("girişin mumu okunamadı (mumlar girişten sonra başlıyor)")
    ix: int | None = None
    exit_price: float | None = None
    if kind == "result":
        exit_price = rec.exit_price         # (only a recorded one: never worked back from the result)
        r_out = result_r(trade, pos, fill, stop, exit_price)
        if exit_price is None or r_out is None:
            raise CardProblem("kapanış fiyatı kayıtlarda yok")
        if rec.closed is None:
            raise CardProblem("kapanışın zamanı kayıtlarda yok")
        ix = _bar_index(bars, rec.closed)
        if ix is None or ix < ie:
            ix = ie
    else:
        r_out = None

    # ---- which candles ----
    before = BARS_BEFORE
    if kind == "result":
        assert ix is not None
        i_lvl = ix
        if rec.reason not in ("tp", "stop"):
            # after an early close the chart runs on until the price reaches the target or the stop
            for j in range(ix + 1, min(len(bars), ix + 1 + TOUCH_SEARCH_BARS)):
                _t, _o, high, low, _c = bars[j]
                hit_tp = target is not None and (high >= target if long_ else low <= target)
                hit_sl = low <= stop if long_ else high >= stop
                if hit_tp or hit_sl:
                    i_lvl = j
                    break
        last = min(len(bars) - 1, max(ix + RESULT_AFTER_BARS, i_lvl + AFTER_TOUCH_BARS))
        first = _first_bar(ie, last, before)
        slots = max(last - first + 1, ix - first + 1 + END_ROOM_BARS)
    else:
        last = len(bars) - 1
        first = _first_bar(ie, last, before)
        slots = max((ie - first) + 1 + ENTRY_ROOM_BARS, (last - first) + 1 + 8)
    drawn = bars[first:last + 1]
    k_entry = ie - first
    k_exit = ix - first if ix is not None else None
    times = [b[0] for b in drawn] + [drawn[-1][0] + (n + 1) * bar_sec for n in range(slots - len(drawn))]

    digits = price_digits(pos, fill)
    p = lambda v: fmt_num(v, digits)  # noqa: E731
    r_tp = sign * (target - fill) / risk if target is not None else None
    wall_fill = to_uk(rec.filled) if rec.filled is not None else None
    wall_anchor = to_uk(anchor)
    entry_kind = "piyasa emri" if trade.plan.entry_type is EntryType.MARKET else "limit emir"
    stop_chip = ["STOP", p(stop), f"{MINUS}1R", "stop"]
    tp_chip = (["HEDEF", p(target), f"+{r_tp:.2f}R", "target"] if target is not None and r_tp is not None
               else ["HEDEF", "—", "yok", None])
    if kind == "result":
        assert rec.closed is not None and exit_price is not None and r_out is not None
        t_out = to_uk(rec.closed)
        if rec.reason == "tp":
            status = ["DURUM", "Hedef ✓", "kapandı", None]
        elif rec.reason == "stop":
            status = ["DURUM", "Stop", "kapandı", None]
        else:
            status = ["DURUM", "Kapatıldı", f"çıkış {p(exit_price)}", None]
        start = wall_fill or wall_anchor
        minutes = max(0, int((t_out - start).total_seconds() // 60))
        chips = [["GİRİŞ", p(fill), entry_kind, None], stop_chip, tp_chip, status,
                 ["SÜRE", _duration(minutes), f"{start:%H:%M} → {t_out:%H:%M}", None]]
    elif pending:
        chips = [["GİRİŞ", p(fill), entry_kind, None], stop_chip, tp_chip, ["DURUM", "Bekliyor", "emir girildi", None],
                 ["ZAMAN", f"{wall_anchor:%H:%M}", f"{wall_anchor:%d.%m.%Y}", None]]
    else:
        shown = wall_fill or wall_anchor
        chips = [["GİRİŞ", p(fill), entry_kind, None], stop_chip, tp_chip, ["DURUM", "Açık", "pozisyon açıldı", None],
                 ["ZAMAN", f"{shown:%H:%M}", f"{shown:%d.%m.%Y}", None]]

    # ---- the timeline (real events only) ----
    steps: list[list[str]] = []
    if trade.plan.entry_type is EntryType.LIMIT and rec.placed is not None:
        steps.append([f"{to_uk(rec.placed):%H:%M}", "Limit emir kondu"])
    if not pending:
        what = "Giriş · piyasa emri" if trade.plan.entry_type is EntryType.MARKET else "Giriş doldu"
        steps.append([f"{wall_fill:%H:%M}" if wall_fill is not None else "", what])
    for moment, text in rec.moves:
        steps.append([f"{to_uk(moment):%H:%M}", text])
    if kind == "result":
        assert rec.closed is not None and r_out is not None
        word = {"tp": "Hedef", "stop": "Stop"}.get(rec.reason or "", None) or {
            "group": "Grup kapattı", "owner": "Elle kapatıldı", "all": "Hepsi kapatıldı"}.get(rec.close_by or "",
                                                                                             "Kapatıldı")
        steps.append([f"{to_uk(rec.closed):%H:%M}", f"{word} · {fmt_r(r_out)}"])
    elif pending:
        steps.append(["", "Giriş bekleniyor"])
    elif not manual and pos.stop_loss is not None and not (pos.data or {}).get("unprotected"):
        steps.append(["", "Stop ve hedef brokerda" if pos.take_profits or target is not None else "Stop brokerda"])

    asset = instrument_label(trade.plan)
    return {
        "card": "trade",
        "kind": kind,
        "pending": pending,
        "no": trade.id,
        "asset": asset,
        "side": trade.plan.side.value,
        "account": account,
        "subtitle": (f"{shown_symbol(pos.broker_symbol)}  ·  {int(tf_min)} dakika  ·  "
                     f"{(wall_fill or wall_anchor):%d.%m.%Y}"),
        "hero": fmt_r(r_out) if r_out is not None else None,
        "chips": chips,
        "bars": [list(b) for b in drawn],
        "slots": int(slots),
        "bar_sec": bar_sec,
        "k_entry": k_entry,
        "k_exit": k_exit,
        "arrow_entry": not pending,
        "box_end": (k_exit if k_exit is not None else k_entry + ENTRY_BOX_BARS),
        "long": long_,
        "fill": fill,
        "stop": stop,
        "target": target,
        "exit": exit_price,
        # the box's profit side: the result card's reaches the close price - when the close made a profit; a loss
        # (or break-even) has no profit side, only the stop zone and the darker played-out part entry -> close
        "box_far": (exit_price if exit_price is not None and sign * (exit_price - fill) > 0 else None)
        if kind == "result" else target,
        "last_price": drawn[-1][4],
        "digits": digits,
        "time_labels": [[k, text] for k, text in _time_labels(times, tf_min)],
        "timeline": steps,
        "footer": FOOTER,
    }


def caption(trade: TradeRecord, account: str, kind: str) -> str:
    """The photo's caption (HTML-safe: only fixed words, the asset and the account)."""
    from ..telegram.formatting import esc

    what = "Giriş kartı" if kind == "entry" else "Sonuç kartı"
    return (f"🃏 <b>{esc(instrument_label(trade.plan))} {trade.plan.side.value.upper()}</b> · {esc(account)} · "
            f"#{trade.id} · {what}")


def window(anchor: datetime, end: datetime, tf_min: int, *, closed_market: bool) -> tuple[datetime, datetime]:
    """The candles to ask the venue for: enough before ``anchor`` (a market that closes for the weekend: at least
    four days before, so that BARS_BEFORE candles are there), up to ``end``."""
    before = timedelta(minutes=tf_min * (BARS_BEFORE + 4))
    if closed_market:
        before = max(before, timedelta(days=4))
    return as_utc(anchor) - before, as_utc(end)


def search_end(closed: datetime, tf_min: int, now: datetime) -> datetime:
    """The last candle a result card may need: TOUCH_SEARCH_BARS after the close, not after now."""
    return min(as_utc(now), as_utc(closed) + timedelta(minutes=tf_min * (TOUCH_SEARCH_BARS + 1)))


__all__ = ["BARS_BEFORE", "DEFAULT_TIMEFRAME_MIN", "EVENT_KINDS", "CardProblem", "Deal", "Records", "caption",
           "card_data", "card_target", "entry_price", "fmt_num", "fmt_r", "list_label", "listed", "original_stop",
           "price_digits", "read_records", "result_r", "search_end", "shown_symbol", "timeframe_of", "venue_price",
           "week_start", "window"]
