"""The engine: Telegram messages -> parsed actions -> validated plans -> orders.

Flow per (chat, sender, forum topic): the listener calls
:meth:`Engine.on_messages_ready` after a debounce; the engine parses the
unconsumed recent messages, and for each action either executes it on every
broker, asks the owner (anything uncertain), or ignores it. Owner decisions
arrive through the :class:`~copytrader.interfaces.EngineControl` methods.

Forum topics (``telegram.topics`` non-empty): only messages of a topic whose
title matches a :class:`~copytrader.config.TopicRoute` are read (others are
consumed unread). A new trade is opened only on its route's brokers
(``TradePlan.route`` / ``broker_names``), and management messages of a topic
only ever see and touch trades of the same route. With no routes configured
everything behaves as before (one stream per chat and sender).

Safety rules kept here:
* Source messages are marked consumed BEFORE orders are sent, so a crash or
  restart can never execute the same signal twice.
* Every position write for a trade happens under that trade's lock (shared
  with the monitor).
* Anything doubtful becomes a confirmation; unanswered confirmations expire
  without trading. The outcome of every confirmation is remembered per source
  message: a later action built on a rejected / expired / pending / already
  decided message is asked again, never executed automatically.
* Order flows (open / manage) run as shielded "critical" tasks: cancelling the
  caller never interrupts them, and shutdown waits for them before closing
  the brokers.
* Adapter problems (``pos.error``, a position without a stop) are always
  reported to the owner, whatever ``notify_on_execution`` says. So is a
  position whose size was limited (``risk.max_size_multiple``).
* No broker can abort or hold up a flow. Every broker call that fails, also with an
  ``asyncio.CancelledError`` nobody asked for (:func:`stray_cancel`), ends as that broker's own
  error outcome. When a new trade is opened, a broker that has not answered within
  ``LEG_DEADLINE_SEC`` is reported as "late": the flow finishes with the others (locks released)
  and the late leg is booked and reported when it answers. It is never cancelled (its order may
  already be at the broker), and until then that broker takes no second open.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import math
import secrets
import time
import uuid
from collections.abc import AsyncIterator, Callable, Coroutine
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, TypeVar

from ..brokers.base import Broker, BrokerError
from ..config import AppConfig, TopicRoute, effective_risk, match_topic, route_thresholds
from ..errors import ParserError
from ..interfaces import SHUTDOWN_TEXT, Notifier, SignalParser, Storage
from ..models import (
    ActionType,
    BrokerPosition,
    ConfirmationStatus,
    EntryType,
    ManagementPlan,
    MarketType,
    OpenRequest,
    ParseContext,
    ParsedAction,
    PendingConfirmation,
    PositionStatus,
    Quote,
    Severity,
    Side,
    StoredMessage,
    TakeProfit,
    TradePlan,
    TradeRecord,
    TradeStatus,
    ValidationIssue,
    instrument_label,
)
from ..parsing import prefilter, validator
from ..parsing.symbols import FIAT_CURRENCIES, canonical_asset, normalize_symbol
from ..risk import guards, sizing
from ..telegram import formatting as fmt
from ..telegram.formatting import esc, fmt_price
from .monitor import (
    ACTIVE_POSITION,
    ACTIVE_TRADE,
    Monitor,
    is_virtual_pending,
    protection_problem,
    rollup_status,
    unprotected_alert_text,
    utc,
)
from .prices import PriceOracle, ReferencePriceOracle, stray_cancel

logger = logging.getLogger(__name__)

T = TypeVar("T")
ConfKind = Literal["open", "manage", "incomplete"]
# (chat_id, sender_id) or, in a forum topic, (chat_id, sender_id, topic_id): one message stream each.
_Key = tuple[int, ...]


def stream_key(chat_id: int, sender_id: int | None, topic_id: int | None = None) -> _Key:
    """Key of a (chat, sender[, forum topic]) message stream; without a topic it is (chat, sender) as before."""
    return (chat_id, sender_id) if topic_id is None else (chat_id, sender_id, topic_id)  # type: ignore[return-value]


def _key_parts(key: _Key) -> tuple[int, int | None, int | None]:
    return key[0], key[1], (key[2] if len(key) > 2 else None)

MANAGEMENT_ACTIONS = frozenset({ActionType.MOVE_SL, ActionType.MOVE_TP, ActionType.CLOSE, ActionType.CANCEL})
# Arguments this version of validator.build_open_plan takes: optional ones are passed only when it knows them.
_OPEN_PLAN_PARAMS = frozenset(inspect.signature(validator.build_open_plan).parameters)
# Waits before the retries of a transient parse failure (Claude overloaded / unreachable): the last try starts 400 s
# after the first. A signal opened that late is still checked against the live price like any other.
TRANSIENT_PARSE_RETRY_DELAYS: tuple[float, ...] = (10.0, 30.0, 60.0, 120.0, 180.0)
# A "none" reading this unsure (or with ambiguities) is shown to the owner instead of being dropped.
NONE_ASK_BELOW_CONFIDENCE = 0.6
CONNECT_TIMEOUT_SEC = 180.0
READ_TIMEOUT_SEC = 20.0
OPTIONAL_RETRY_SEC = 300.0     # an optional broker that could not connect is retried this often
# A required broker that could not connect is retried sooner: after this, then after twice as long each time,
# until the wait is OPTIONAL_RETRY_SEC (30 s, 60 s, 120 s, 240 s, 300 s, 300 s ...).
REQUIRED_RETRY_FIRST_SEC = 30.0
EXCERPT_CHARS = 300
_PENDING_CODES = frozenset({"pending_confirmation", "duplicate_pending_signal"})
# A management message that refers to a trade in one of these states has nothing left to do: ignored, never asked.
_DONE_TRADE = frozenset({TradeStatus.CLOSED, TradeStatus.CANCELLED, TradeStatus.FAILED})
_DONE_TRADE_TR = {
    TradeStatus.CLOSED: "zaten kapanmış",
    TradeStatus.CANCELLED: "zaten iptal edilmiş",
    TradeStatus.FAILED: "hiçbir hesapta açılamamıştı",
}
SHUTDOWN_GRACE_SEC = 45.0          # shutdown waits this long for running order flows before closing brokers
LEG_DEADLINE_SEC = 30.0            # an open flow waits this long for each broker; a slower one is reported as "late"
BUSY_WAIT_SHARE = 0.9              # share of the deadline a new leg waits for the same broker's unanswered (late) leg
OPEN_LOCK_ALERT_SEC = 60.0         # the owner is told when one signal has blocked new trades for this long
MONITOR_RESTART_DELAY_SEC = 5.0    # a monitor task that ended on its own is started again after this
MONITOR_ALERT_THROTTLE_SEC = 1800.0  # ... and the owner is not told about it again within this
VIRTUAL_RETRY_MAX = 5              # retries of a triggered virtual entry that failed BEFORE any order was sent
VIRTUAL_REGISTER_PRICE_TIMEOUT_SEC = 5.0     # the market price read when a virtual entry is registered waits this long
# Failures that used to be in the log only (a background job of the engine that died, an order flow that failed
# after its caller was cancelled, a message whose topic is unknown): the owner is told, the same kind of
# failure at most once in this time (the log has every occurrence).
LOST_NOTICE_THROTTLE_SEC = 600.0
# Background jobs of the engine by the prefix of their task name: what a failure of that job means (Turkish).
_JOB_TR = (
    ("parse-retry-", "okunamayan bir sinyalin yeniden denenmesi"),
    ("incomplete-", "eksik gelen bir sinyalin devamının beklenmesi"),
    ("expire-", "bir onay sorusunun süresinin takibi"),
    ("retry-", "bağlanamayan bir hesaba yeniden bağlanma denemeleri"),
    ("monitor-restart", "pozisyon takibinin yeniden başlatılması"),
)
_FLOW_TR = (
    ("open-", "yeni işlem açma"),
    ("manage-", "açık işlemde değişiklik / kapatma"),
    ("late-", "geç yanıt veren brokerın sonucunun işlenmesi"),
)
_MINUTE_MS = 60_000
_LATE_KEY = "late_open_legs"       # storage state: legs that were waiting for their broker (restart notice)
# Broker outcomes of an open flow that mean "this broker of the route has not taken the trade".
_NOT_OPENED = frozenset({"skipped", "unsupported", "error", "filtered", "late"})
_CANCELLED_BEFORE_ORDER = "broker bağlantısı yanıt vermeden kesildi; emir gönderilmedi, bu hesapta işlem açılmadı"
_CANCELLED_AFTER_ORDER = ("emir gönderildi ama broker bağlantısı yanıt vermeden kesildi; pozisyon açılmış olabilir, "
                          "brokerda elle kontrol edin")
_CANCELLED_CONNECT = "bağlantı isteği yanıt gelmeden kesildi"
EDIT_REPROCESS_MAX_AGE_SEC = 900.0  # an edit turning a non-signal message into a signal is re-read this long
MSG_OUTCOME_MAX = 2000              # remembered confirmation outcomes per chat
MSG_OUTCOME_TTL_DAYS = 14
TP_MATCH_REL_TOL = 0.005            # a broker's filled TP price (tick-rounded) vs the trade's level

_CONF_STATUS_TR = {
    ConfirmationStatus.PENDING: "beklemede",
    ConfirmationStatus.APPROVED: "onaylandı",
    ConfirmationStatus.REJECTED: "iptal edildi",
    ConfirmationStatus.EXPIRED: "süresi doldu",
    ConfirmationStatus.SUPERSEDED: "yerine düzeltilmiş hali gönderildi",
}
_POS_STATUS_TR = {
    PositionStatus.PENDING: "bekleyen emir",
    PositionStatus.OPEN: "açık",
    PositionStatus.CLOSED: "kapalı",
    PositionStatus.CANCELLED: "iptal",
    PositionStatus.FAILED: "hata",
}
_ACTIVE_TRADE_TR = {
    TradeStatus.OPENING: "gönderiliyor",
    TradeStatus.PENDING: "bekleyen emir",
    TradeStatus.OPEN: "açık",
}


# --------------------------------------------------------------------------- #
# Small value types
# --------------------------------------------------------------------------- #
@dataclass
class Evaluation:
    """What the engine would do with one parsed action."""

    kind: ConfKind
    action: ParsedAction
    plan: TradePlan | ManagementPlan | None
    issues: list[ValidationIssue]
    market_price: float | None = None
    trade: TradeRecord | None = None
    ignored: str | None = None     # management for a trade that is already over: why it is ignored (Turkish)

    @property
    def automatic(self) -> bool:
        return self.plan is not None and not self.issues and self.kind != "incomplete"


@dataclass
class BrokerOutcome:
    broker: str
    # partial: applied, adapter reported a problem. late: the broker has not answered within LEG_DEADLINE_SEC
    # (its leg goes on in the background). filtered: the broker's allowed_assets does not list the asset.
    kind: Literal["ok", "partial", "unsupported", "skipped", "error", "late", "filtered"]
    text: str                                # HTML line for the owner
    position: BrokerPosition | None = None
    benign: bool = False                     # an intended skip (e.g. no-hedge rule): not an alarming failure
    sent: bool = False                       # an order request went to the broker (it may exist there, whatever came back)
    size_capped: bool = False                # larger than the planned size, but limited (risk.max_size_multiple)


@dataclass
class _OpenFlow:
    """Shared by the broker legs of one open flow. ``released``: the flow has finished and no longer holds
    the trade lock for its legs; a leg that is still running (a late one) takes the lock itself to write."""

    released: bool = False


@dataclass
class _LateLeg:
    """One broker's part of an open flow that had not answered by LEG_DEADLINE_SEC."""

    trade_id: int
    broker: str
    label: str                               # "#12 EUR LONG" (plain text)
    task: asyncio.Task[Any]                  # the leg itself: never cancelled
    announced: asyncio.Event = field(default_factory=asyncio.Event)   # the flow's own report has been sent
    close_on_arrival: str | None = None      # Turkish reason: close / cancel its position as soon as it is known
    missed: list[str] = field(default_factory=list)   # changes made to the trade meanwhile (not applied to this leg)


@dataclass
class _IncompleteWatch:
    chat_id: int
    action: ParsedAction
    message_ids: list[int]
    deadline: datetime
    task: asyncio.Task[None] | None = None
    route: TopicRoute | None = None


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def asset_of(action: ParsedAction) -> tuple[str | None, str | None, MarketType | None]:
    """Canonical (base, quote, market_type) of an action, like the validator derives them."""
    base = canonical_asset(action.base_asset) if action.base_asset and action.base_asset.strip() else None
    quote = action.quote_asset.strip().upper() if action.quote_asset and action.quote_asset.strip() else None
    market_type = action.market_type
    if action.symbol:
        s_base, s_quote, s_type = normalize_symbol(action.symbol)
        if base is None:
            base = s_base
        if s_base is not None and s_base == base:
            quote = quote or s_quote
            market_type = market_type or s_type
    return base, quote, market_type


def managed_asset(action: ParsedAction) -> tuple[str | None, str | None, MarketType | None]:
    """:func:`asset_of` for a management instruction. The open-trades context gives the model a trade's base
    asset and its quote separately (``TradeRecord.summary``), but the trader writes the pair as one word and
    the model may return that word as ``base_asset`` ("GBPUSD"): it is split into base and quote here. Only a
    name that normalises to a forex / CFD pair is split; every other name (a coin, "BTCUSDT") is left exactly
    as :func:`asset_of` gives it."""
    base, quote, market_type = asset_of(action)
    if base:
        p_base, p_quote, p_type = normalize_symbol(base)
        if p_base and p_quote and p_base != base and p_type in (MarketType.FOREX, MarketType.CFD):
            return p_base, quote or p_quote, market_type or p_type
    return base, quote, market_type


def merge_issues(*groups: list[ValidationIssue]) -> list[ValidationIssue]:
    """Concatenate, dropping exact duplicates (same code + text)."""
    seen: set[tuple[str, str]] = set()
    out: list[ValidationIssue] = []
    for group in groups:
        for issue in group:
            key = (issue.code, issue.message_tr)
            if key not in seen:
                seen.add(key)
                out.append(issue)
    return out


def _issue(code: str, text: str, blocking: bool = False) -> ValidationIssue:
    return ValidationIssue(code=code, message_tr=text, blocking=blocking)


def _same_price(a: float, b: float) -> bool:
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-12)


def _distance_pct(a: float, ref: float) -> float:
    return abs(a - ref) / ref * 100.0 if ref else math.inf


def near_entry(price: float, entry: float, stop: float | None, risk: Any) -> bool:
    """Is ``price`` close enough to ``entry`` to enter at market? The validator's rule for market entries
    (``validator._near_entry``; kept in step by a test): within ``risk.market_entry_tolerance_pct`` percent of
    the entry or, when ``risk.market_entry_tolerance_r`` is set and the stop is known, within that fraction of
    the entry-to-stop distance."""
    if _distance_pct(price, entry) <= risk.market_entry_tolerance_pct:
        return True
    tolerance_r = getattr(risk, "market_entry_tolerance_r", None)
    if tolerance_r is None or stop is None:
        return False
    return abs(price - entry) <= tolerance_r * abs(entry - stop) * (1.0 + 1e-9)


def _qty(x: float | None) -> str:
    return fmt_price(x) if x is not None else "—"


def _money(x: float) -> str:
    return f"{x:,.2f}".replace(",", " ")


def is_transient_parse_error(err: BaseException) -> bool:
    """Network / timeout / rate-limit / 5xx: worth retrying. Uses ``err.transient`` if present,
    else the chained cause (llm_parser raises ParserError ``from`` the SDK exception)."""
    flag = getattr(err, "transient", None)
    if isinstance(flag, bool):
        return flag
    cause = err.__cause__ or err.__context__
    if cause is None:
        return False
    try:
        import anthropic

        if isinstance(cause, (anthropic.APIConnectionError, anthropic.RateLimitError, anthropic.InternalServerError)):
            return True       # APITimeoutError is an APIConnectionError
        if isinstance(cause, anthropic.APIStatusError):
            return int(getattr(cause, "status_code", 0) or 0) >= 500
    except ImportError:  # pragma: no cover
        pass
    return isinstance(cause, (TimeoutError, asyncio.TimeoutError, ConnectionError))


def _excerpt(msgs: list[StoredMessage]) -> str:
    parts: list[str] = []
    for m in msgs:
        text = (m.text or "").strip()
        if m.media_path or m.media_type:
            text = ("[görsel] " + text).strip()
        if text:
            parts.append(text)
    joined = " | ".join(parts) or "(boş)"
    return joined if len(joined) <= EXCERPT_CHARS else joined[: EXCERPT_CHARS - 1] + "…"


def broker_risk_amount(broker: Broker, equity: float, plan: TradePlan) -> float:
    """Money to risk at the stop on ``broker``: a fixed ``risk_usd`` per R when the broker sets one,
    otherwise risk_pct of equity; both scaled by the broker's risk_multiplier."""
    multiplier = float(getattr(broker.config, "risk_multiplier", 1.0) or 1.0)
    fixed = getattr(broker.config, "risk_usd", None)
    if fixed:
        return float(fixed) * plan.risk_r * multiplier
    return sizing.risk_amount(equity, plan.risk_pct, multiplier)


DEFAULT_MAX_SIZE_MULTIPLE = 2.0        # RiskConfig.max_size_multiple when the config does not carry the field


def market_sizing_entry(
    side: Side, live_entry: float, stop: float, signal_entry: float | None,
    max_multiple: Any = DEFAULT_MAX_SIZE_MULTIPLE,
) -> tuple[float, bool]:
    """(entry price to SIZE a market order with, was the size limited?).

    A size is risk / |entry - stop|: sized on a live price that has moved most of the way from the signal's
    entry to the stop, the position would be many times the planned one. So it is never larger than
    ``max_multiple`` times the size implied by the signal's own entry-to-stop distance: when the live price is
    closer to the stop than that distance / ``max_multiple``, the size is computed as if the entry were exactly
    that far from the stop, on the entry side (the money really at risk is then below the target).
    Unchanged (the live price) without a signal entry and whenever the live price is not that close.
    A ``max_multiple`` that is not a positive number counts as the default, one below 1 as 1 (the size the
    signal's own distance implies always stands), like the MetaTrader adapters read it."""
    try:
        k = float(max_multiple)
    except (TypeError, ValueError):
        k = DEFAULT_MAX_SIZE_MULTIPLE
    k = max(1.0, k) if math.isfinite(k) and k > 0 else DEFAULT_MAX_SIZE_MULTIPLE
    if not signal_entry:
        return live_entry, False
    floor = abs(signal_entry - stop) / k
    # The tiny factor keeps a price exactly on the edge from counting as "closer" through float rounding.
    if not floor > 0 or abs(live_entry - stop) >= floor * (1.0 - 1e-9):
        return live_entry, False
    return (stop + floor if side is Side.LONG else stop - floor), True


def size_cap_warning(risk_amount: float, live_entry: float, sizing_entry: float, stop: float, currency: str) -> str:
    """Owner text for a size limited by :func:`market_sizing_entry`, with about what is really at risk."""
    real = risk_amount * abs(live_entry - stop) / abs(sizing_entry - stop)
    money = f"{_money(real)} {currency}".strip()
    return f"Fiyat stopa çok yakın olduğu için miktar sınırlandı; gerçek risk ~{money} (hedefin altında)."


def price_multiplier(extra: dict[str, Any] | None) -> float:
    """Exchange price / signal price (1000 when the signal says PEPE and the venue trades 1000PEPE)."""
    try:
        m = float((extra or {}).get("price_multiplier") or 1.0)
    except (TypeError, ValueError):
        return 1.0
    return m if math.isfinite(m) and m > 0 else 1.0


def shift_price(price: float, offset: dict[str, Any] | None) -> float:
    """Signal-scale-adjusted ``price`` moved onto an offset broker: additive ``+ value`` / ratio ``* value``."""
    if not offset:
        return price
    value = float(offset["value"])
    return price * value if offset.get("mode") == "ratio" else price + value


def unshift_price(price: float, offset: dict[str, Any] | None) -> float:
    """Inverse of :func:`shift_price` (broker price -> the signal's chart price)."""
    if not offset:
        return price
    value = float(offset["value"])
    if offset.get("mode") == "ratio":
        return price / value if value else price
    return price - value


def stored_offset(data: dict[str, Any] | None) -> dict[str, Any] | None:
    """The price offset kept on a broker position (``data["price_offset"]``) or None; never raises."""
    off = (data or {}).get("price_offset")
    if not isinstance(off, dict) or off.get("mode") not in ("additive", "ratio"):
        return None
    try:
        v = float(off["value"])
    except (KeyError, TypeError, ValueError):
        return None
    return {"mode": off["mode"], "value": v} if math.isfinite(v) else None


def offset_text(offset: dict[str, Any]) -> str:
    v = round(float(offset["value"]), 6)
    return f"fark {v:+} uygulandı" if offset["mode"] == "additive" else f"oran x{v} uygulandı"


def shifted_take_profits(tps: list[TakeProfit], offset: dict[str, Any] | None) -> list[TakeProfit]:
    if not offset:
        return tps
    return [TakeProfit(price=shift_price(tp.price, offset), close_fraction=tp.close_fraction) for tp in tps]


def scaled_take_profits(tps: list[TakeProfit], m: float) -> list[TakeProfit]:
    return [TakeProfit(price=tp.price * m, close_fraction=tp.close_fraction) for tp in tps]


def filled_tp_prices(pos: BrokerPosition) -> list[float]:
    """Take-profit prices (signal scale) that already filled on this broker position, from adapter data:
    ccxt ``tp_order_ids`` / paper ``tps`` (status "filled"), MetaTrader legs closed by their TP and
    emulated netting TPs marked done."""
    d = pos.data or {}
    raw: list[Any] = []
    for key in ("tp_order_ids", "tps"):
        for tp in d.get(key) or []:
            if isinstance(tp, dict) and tp.get("status") == "filled":
                raw.append(tp.get("price"))
    for leg in d.get("legs") or []:
        if isinstance(leg, dict) and leg.get("state") == "closed" and leg.get("reason") == "tp":
            raw.append(leg.get("tp"))
    for e in d.get("emulated_tps") or []:
        if isinstance(e, dict) and e.get("done"):
            raw.append(e.get("price"))
    mult = price_multiplier(d)
    offset = stored_offset(d)
    out: list[float] = []
    for p in raw:
        try:
            v = float(p)
        except (TypeError, ValueError):
            continue
        if math.isfinite(v) and v > 0:
            out.append(unshift_price(v, offset) / mult)
    return out


def _match_levels(levels: list[float], filled: list[float]) -> list[bool]:
    """Which of ``levels`` filled: each filled price marks its nearest level (within TP_MATCH_REL_TOL)."""
    hit = [False] * len(levels)
    for f in filled:
        best: int | None = None
        for i, lv in enumerate(levels):
            if lv > 0 and abs(lv - f) / lv <= TP_MATCH_REL_TOL and (best is None or abs(lv - f) < abs(levels[best] - f)):
                best = i
        if best is not None:
            hit[best] = True
    return hit


def tp_progress(trade: TradeRecord, positions: list[BrokerPosition]) -> tuple[list[bool], float | None]:
    """(hit flag per current TP level on any broker, share of the filled size still open or None)."""
    filled = [f for p in positions for f in filled_tp_prices(p)]
    hit = _match_levels([tp.price for tp in trade.current_take_profits], filled)
    shares = [p.remaining_quantity / p.filled_quantity for p in positions
              if p.status is PositionStatus.OPEN and p.filled_quantity > 0]
    return hit, (max(shares) if shares else None)


_REF_LABELS = {"binanceusdm": "Binance", "binance": "Binance", "bybit": "Bybit", "okx": "OKX", "bitget": "Bitget"}
VIRTUAL_NOTE_THROTTLE_SEC = 1800.0     # same reference-outage warning is not repeated within this


def ref_label(ref_id: str) -> str:
    return _REF_LABELS.get(ref_id.lower(), ref_id)


def pos_status_text(pos: BrokerPosition) -> str:
    """Turkish position status; a waiting virtual entry is "bekleyen (sanal tetik)"."""
    if is_virtual_pending(pos):
        return "bekleyen (sanal tetik)"
    return _POS_STATUS_TR.get(pos.status, pos.status.value)


def is_crypto_plan(plan: TradePlan, route_kind: str | None = None) -> bool:
    """Is ``plan`` a crypto plan, i.e. one the price offset and the virtual trigger apply to (its levels come
    from a chart of the reference exchange)? Decided by the kind of the plan's forum route (``route_kind``):
    "fx": never; "crypto": always, unless the plan itself says forex / CFD; no route or kind "any" (None):
    only an untyped or perpetual plan."""
    if route_kind == "fx":
        return False
    if route_kind == "crypto":
        return plan.market_type not in (MarketType.FOREX, MarketType.CFD)
    return plan.market_type in (None, MarketType.PERP)


def virtual_entry_wanted(broker: Broker, plan: TradePlan, route_kind: str | None = None) -> bool:
    """True when ``plan`` is a crypto LIMIT entry on a broker set to ``entry_trigger: reference_touch``.
    ``route_kind``: kind of the plan's forum route ("crypto" / "fx"; None: no route or kind "any"), see
    :func:`is_crypto_plan`."""
    cfg = broker.config
    return (
        getattr(cfg, "entry_trigger", "broker_limit") == "reference_touch"
        and bool(getattr(cfg, "reference_exchange", None))
        and plan.entry_type is EntryType.LIMIT
        and bool(plan.entry_price)
        and is_crypto_plan(plan, route_kind)
    )


class _VirtualEntryOps:
    """Stands in for a broker while managing a waiting virtual entry: no venue order exists, so stop /
    take-profit changes and cancellation only edit the stored position."""

    async def update_stop_loss(self, pos: BrokerPosition, sl: float) -> BrokerPosition:
        new = pos.model_copy(deep=True)
        new.stop_loss = sl
        return new

    async def update_take_profits(self, pos: BrokerPosition, tps: list[TakeProfit]) -> BrokerPosition:
        new = pos.model_copy(deep=True)
        new.take_profits = list(tps)
        return new

    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        new = pos.model_copy(deep=True)
        new.status = PositionStatus.CANCELLED
        new.data["virtual_state"] = "cancelled"
        return new


# Dollar quotes that name one and the same price for an instrument ("BTC/USD" is traded as BTC/USDT).
_DOLLAR_QUOTES = frozenset({"USD", "USDT", "USDC", "BUSD", "FDUSD"})


def _quote_key(quote: str | None) -> str | None:
    q = (quote or "").strip().upper()
    if not q:
        return None
    return "USD" if q in _DOLLAR_QUOTES else q


def same_pair(a: TradePlan, b: TradePlan) -> bool:
    """Do two plans name the same pair: the same base asset and the same quote (USDJPY and USDCAD share only
    the base). A plan without a quote matches any quote."""
    if a.base_asset.strip().upper() != b.base_asset.strip().upper():
        return False
    qa, qb = _quote_key(a.quote_asset), _quote_key(b.quote_asset)
    return qa is None or qb is None or qa == qb


def _price_problem(side: Side, quote: Quote, stop: float, tp1: float | None) -> str | None:
    """Live price already beyond the stop (or beyond TP1 when given)? Turkish text or None."""
    long = side is Side.LONG
    if (long and quote.bid <= stop) or (not long and quote.ask >= stop):
        return f"anlık fiyat ({fmt_price(quote.bid if long else quote.ask)}) stop seviyesini ({fmt_price(stop)}) geçmiş"
    if tp1 is not None and ((long and quote.ask >= tp1) or (not long and quote.bid <= tp1)):
        return f"anlık fiyat ({fmt_price(quote.ask if long else quote.bid)}) ilk hedefi ({fmt_price(tp1)}) geçmiş"
    return None


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #
class Engine:
    """Implements :class:`copytrader.interfaces.EngineControl`."""

    def __init__(
        self,
        config: AppConfig,
        db: Storage,
        parser: SignalParser,
        brokers: list[Broker],
        notifier: Notifier,
        price_oracle: PriceOracle | Any | None = None,
        *,
        reference_prices: ReferencePriceOracle | Any | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """``price_oracle``: anything with ``async price(base, quote, market_type, raw_symbol, *, fresh=False)``.
        ``reference_prices``: anything with ``async mid(exchange_id, base, *, fresh=False)`` (price-offset brokers).
        ``clock``: returns "now" (tests)."""
        self.config = config
        self.db = db
        self.parser = parser
        self.notifier = notifier
        self.configured_brokers: list[Broker] = list(brokers)
        self.brokers: list[Broker] = list(brokers)       # connected ones after start()
        self.failed_brokers: dict[str, str] = {}
        # Optional brokers (config ``optional: true``) that are not connected right now: name -> reason.
        # Their trades are skipped (with a short notice), never queued; they are retried in the background.
        self.optional_down: dict[str, str] = {}
        self.optional_retry_sec = OPTIONAL_RETRY_SEC
        self.required_retry_first_sec = REQUIRED_RETRY_FIRST_SEC
        self._retry_failed = False                 # start(): brokers that cannot connect are retried in the background
        self._retrying: set[str] = set()           # names of the brokers a background retry is running for
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._own_oracle = price_oracle is None
        self.prices: Any = price_oracle if price_oracle is not None else PriceOracle(
            lambda: self.brokers, config.price_source_exchange
        )
        self._own_reference = reference_prices is None
        self.reference_prices: Any = reference_prices if reference_prices is not None else ReferencePriceOracle()
        self.retry_delays: tuple[float, ...] = TRANSIENT_PARSE_RETRY_DELAYS
        self._sender_locks: dict[_Key, asyncio.Lock] = {}
        self._conf_locks: dict[str, asyncio.Lock] = {}
        self._trade_locks: dict[int, asyncio.Lock] = {}
        self._open_lock = asyncio.Lock()           # guards + execution of new trades are atomic
        self._expiry_tasks: dict[str, asyncio.Task[None]] = {}
        self._incomplete: dict[_Key, _IncompleteWatch] = {}
        self._retry_tasks: dict[_Key, asyncio.Task[None]] = {}
        # Transient parse failures per (chat, sender): message id -> failed attempts so far. A retry reloads
        # exactly these messages (however old they are by then); each message gets its own retry budget.
        self._failed_ids: dict[_Key, dict[int, int]] = {}
        self._parse_failures: dict[_Key, int] = {}      # most attempts of a pending message (0 / absent: none)
        self._tasks: set[asyncio.Task[Any]] = set()
        self._currency: dict[str, str] = {}
        self.monitor = Monitor(config, db, lambda: self.brokers, notifier,
                               trade_lock=self.trade_lock, clock=self._clock)
        self.monitor.virtual_hook = self._virtual_tick
        self._virtual_warned: dict[tuple[int, str], float] = {}   # (trade, broker) -> monotonic time of last outage warning
        self._monitor_task: asyncio.Task[None] | None = None
        self._accepting = True                     # False once shutdown began: no new work is started
        self._brokers_closed = False
        self._critical: set[asyncio.Task[Any]] = set()   # running order flows (never cancelled)
        self._outcome_lock = asyncio.Lock()
        self.shutdown_grace_sec = SHUTDOWN_GRACE_SEC
        # broker -> the open that is in flight on it right now (a signal's leg task, or the marker of a virtual
        # entry that is being sent): no second open is started on that account until it is done
        self._leg_running: dict[str, asyncio.Future[Any]] = {}
        self._late_legs: dict[tuple[int, str], _LateLeg] = {}     # (trade, broker) -> leg that missed the deadline
        self._late_state_lock = asyncio.Lock()
        self._virtual_seen: dict[tuple[int, str], int] = {}       # (trade, broker) -> ms of the last full look at it
        self._monitor_alert_at: float | None = None               # monotonic time of the last "monitor restarted" notice
        self._lost_noticed: dict[str, float] = {}                 # kind -> monotonic time of its last throttled notice

    # ------------------------------------------------------------------ utils
    def _now(self) -> datetime:
        return utc(self._clock())

    def trade_lock(self, trade_id: int) -> asyncio.Lock:
        return self._trade_locks.setdefault(trade_id, asyncio.Lock())

    def _sender_lock(self, key: _Key) -> asyncio.Lock:
        return self._sender_locks.setdefault(key, asyncio.Lock())

    def _conf_lock(self, conf_id: str) -> asyncio.Lock:
        return self._conf_locks.setdefault(conf_id, asyncio.Lock())

    def _spawn(self, coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task[Any]:
        task = asyncio.ensure_future(coro)
        task.set_name(name)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)
        return task

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            exc = task.exception()
            name = task.get_name()
            logger.error("Engine task %s failed", name, exc_info=exc)
            if self._accepting and name != "lost-notice":
                # A background job died: what it was looking after (the retry of an unread signal, a
                # question's expiry...) will not happen. Never only in the log.
                prefix, what = next(((p, w) for p, w in _JOB_TR if name.startswith(p)), (name, "arka plan işi"))
                self._spawn(self._notify_once(
                    f"job:{prefix}",
                    f"🚨 Botun arka plandaki bir işi beklenmeyen bir hatayla durdu: {what} "
                    f"(<code>{esc(str(exc) or type(exc).__name__)}</code>). Bir sinyal ya da onay işlenmemiş "
                    "olabilir; kaynak grubu ve açık işlemleri kontrol edin.",
                ), "lost-notice")

    async def _notify_once(self, kind: str, text: str, severity: Severity = Severity.ERROR) -> bool:
        """Owner notice for a failure that would otherwise be in the log only; the same ``kind`` at most
        once per LOST_NOTICE_THROTTLE_SEC. True if it was sent."""
        now = time.monotonic()
        last = self._lost_noticed.get(kind)
        if last is not None and now - last < LOST_NOTICE_THROTTLE_SEC:
            return False
        self._lost_noticed[kind] = now
        await self._notify(text, severity)
        return True

    async def _critical_section(self, coro: Coroutine[Any, Any, T], name: str) -> T:
        """Run an order flow so that cancelling the caller (shutdown, listener stop) never interrupts it
        half-way (e.g. after the entry filled but before the stop was placed). Shutdown waits for it."""
        if self._brokers_closed:
            coro.close()
            raise BrokerError("Bot kapanıyor; brokerlar kapatıldı, emir gönderilmedi.")
        task: asyncio.Task[T] = asyncio.ensure_future(coro)
        task.set_name(name)
        self._critical.add(task)
        task.add_done_callback(self._critical_done)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if not task.done():
                logger.warning("%s: caller cancelled; the order flow continues until it completes", name)
                task.add_done_callback(self._orphan_done)
            raise

    def _critical_done(self, task: asyncio.Task[Any]) -> None:
        self._critical.discard(task)
        if not task.cancelled():
            task.exception()       # retrieved here; the awaiting caller (if any) reports it

    def _orphan_done(self, task: asyncio.Task[Any]) -> None:
        if not task.cancelled() and task.exception() is not None:
            exc = task.exception()
            name = task.get_name()
            logger.error("Order flow %s failed after its caller was cancelled", name, exc_info=exc)
            # Nobody is waiting for this flow's result any more: without this the owner would never hear of it.
            # Counted as a running order flow, so that a shutdown waits until the notice is out.
            what = next((w for p, w in _FLOW_TR if name.startswith(p)), "emir")
            notice = asyncio.ensure_future(self._notify_once(
                f"flow:{what}",
                f"🚨 Bir emir işlemi ({what}) beklenmeyen bir hatayla yarıda kaldı "
                f"(<code>{esc(str(exc) or type(exc).__name__)}</code>). Emirler gönderilmiş olabilir; açık "
                "pozisyonları ve emirleri /pozisyonlar ile ve borsada/brokerda kontrol edin.",
            ))
            notice.set_name("lost-notice")
            self._critical.add(notice)
            notice.add_done_callback(self._critical_done)

    @property
    def accepting(self) -> bool:
        return self._accepting

    def _broker(self, name: str) -> Broker | None:
        return next((b for b in self.brokers if b.name == name), None)

    async def _notify(self, text: str, severity: Severity = Severity.INFO) -> None:
        try:
            await self.notifier.notify(text, severity)
        except Exception:
            logger.exception("notify failed")

    async def _log(self, kind: str, payload: dict[str, Any]) -> None:
        try:
            await self.db.log_event(kind, payload)
        except Exception:
            logger.exception("log_event %s failed", kind)

    async def _is_paused(self) -> bool:
        return bool(await self.db.get_state("paused", False))

    # ------------------------------------------------------------------ monitor task supervision
    def _start_monitor(self) -> None:
        task: asyncio.Task[None] = asyncio.ensure_future(self.monitor.run())
        task.set_name("monitor")
        task.add_done_callback(self._monitor_done)
        self._monitor_task = task

    def _monitor_done(self, task: asyncio.Task[None]) -> None:
        """The monitor task ended. While the engine is not stopping that is a fault (an exception or a stray
        cancellation got out of its loop): no position would be synced any more. It is started again."""
        exc = None if task.cancelled() else task.exception()       # always retrieved
        if task is not self._monitor_task or not self._accepting or self._brokers_closed or self.monitor.stopping:
            return
        why = "CancelledError" if task.cancelled() else (repr(exc) if exc is not None else "run() returned")
        logger.error("Monitor task ended although nobody stopped it (%s); restarting in %.0fs",
                     why, MONITOR_RESTART_DELAY_SEC, exc_info=exc)
        self._spawn(self._restart_monitor(why), "monitor-restart")

    async def _restart_monitor(self, why: str) -> None:
        await asyncio.sleep(MONITOR_RESTART_DELAY_SEC)      # never a tight loop if it keeps dying
        if not self._accepting or self._brokers_closed or self.monitor.stopping:
            return
        if self._monitor_task is not None and not self._monitor_task.done():
            return
        self._start_monitor()
        now = time.monotonic()
        if self._monitor_alert_at is not None and now - self._monitor_alert_at < MONITOR_ALERT_THROTTLE_SEC:
            return          # the owner was told a moment ago; every restart is in the log
        self._monitor_alert_at = now
        await self._notify(
            "🚨 Pozisyon takibi durmuştu, yeniden başlatıldı. Açık pozisyonlarınızı ve stoplarını /pozisyonlar ile "
            f"ve brokerda kontrol edin. (Sebep: <code>{esc(why)}</code>)",
            Severity.ERROR,
        )

    # ------------------------------------------------------------------ the open lock
    @contextlib.asynccontextmanager
    async def _holding_open_lock(self, what: str) -> AsyncIterator[None]:
        """``async with self._open_lock`` that tells the owner, once per hold, when it lasts longer than
        OPEN_LOCK_ALERT_SEC: every new signal waits behind it."""
        async with self._open_lock:
            alarm = asyncio.get_running_loop().call_later(OPEN_LOCK_ALERT_SEC, self._open_lock_slow, what)
            try:
                yield
            finally:
                alarm.cancel()

    def _open_lock_slow(self, what: str) -> None:
        logger.error("The open lock has been held for more than %.0fs by: %s", OPEN_LOCK_ALERT_SEC, what)
        self._spawn(self._notify(
            f"⚠️ Bir sinyalin işlenmesi {fmt.fmt_number(float(OPEN_LOCK_ALERT_SEC))} saniyeden uzun sürdü ve hâlâ "
            f"bitmedi ({esc(what)}). Bu sürede gelen yeni sinyaller sırada bekliyor; uzarsa botu ve brokerları "
            "kontrol edin.",
            Severity.WARNING,
        ), "open-lock-alert")

    # ------------------------------------------------------------------ forum topic routes
    def _route_of(self, msgs: list[StoredMessage]) -> tuple[TopicRoute | None, str | None]:
        """(matching TopicRoute, topic title) of a batch: the newest title that matches a route (a topic may
        be renamed meanwhile), else (None, the newest title). Always (None, ...) when no routes are set."""
        titles = [m.topic_title for m in reversed(msgs) if m.topic_title and m.topic_title.strip()]
        for title in titles:
            route = match_topic(self.config.telegram.topics, title)
            if route is not None:
                return route, title
        return None, (titles[0] if titles else None)

    def _route_named(self, name: str | None) -> TopicRoute | None:
        if not name:
            return None
        return next((r for r in self.config.telegram.topics if r.name == name), None)

    @staticmethod
    def _topic_kind(route: TopicRoute | None) -> str | None:
        return route.kind if route is not None and route.kind != "any" else None

    def _plan_kind(self, plan: TradePlan) -> str | None:
        """Kind of the plan's forum route: "crypto" / "fx"; None without a (known) route or for kind "any"."""
        return self._topic_kind(self._route_named(plan.route))

    @staticmethod
    def _in_scope(trade: TradeRecord, route: TopicRoute | None) -> bool:
        """Trades a message of ``route`` may see and manage: only that route's own (None = no routing, all)."""
        return route is None or trade.plan.route == route.name

    def _shares_broker(self, a: TradePlan, b: TradePlan) -> bool:
        """Could the two trades sit on the same broker account (empty broker_names = every broker)?"""
        every = {br.name for br in self.configured_brokers}
        return bool((set(a.broker_names) or every) & (set(b.broker_names) or every))

    def _same_instrument(self, a: TradePlan, b: TradePlan) -> bool:
        """Could the two plans be one instrument on a broker, as far as plans can tell? The same base asset;
        forex / CFD plans must also name the same pair (:func:`same_pair`: USDJPY is not USDCAD). A coin is one
        instrument whatever quote the signal names (crypto plans, :func:`is_crypto_plan`): as it always was."""
        if a.base_asset.strip().upper() != b.base_asset.strip().upper():
            return False
        if is_crypto_plan(a, self._plan_kind(a)) or is_crypto_plan(b, self._plan_kind(b)):
            return True
        return same_pair(a, b)

    def _is_pair_plan(self, plan: TradePlan) -> bool:
        """A forex / CFD trade: its instrument is the PAIR (GBPUSD is not GBPJPY). Never a coin: a perpetual
        or spot plan is one trade whatever quote is named (BTC, BTCUSDT and BTCUSD)."""
        if plan.market_type in (MarketType.FOREX, MarketType.CFD):
            return True
        if plan.market_type is not None:
            return False
        return self._plan_kind(plan) == "fx" or plan.base_asset.strip().upper() in FIAT_CURRENCIES

    def _other_pair(self, plan: TradePlan, quote: str | None) -> bool:
        """An instruction that names the quote currency ``quote`` cannot be about the trade ``plan`` when that
        trade is a forex / CFD pair with ANOTHER quote ("gbpjpy kapattık" is not about an open GBPUSD). False
        when either side names no quote (USD, USDT, USDC... count as one quote) and for every coin trade."""
        wanted, held = _quote_key(quote), _quote_key(plan.quote_asset)
        if wanted is None or held is None or wanted == held:
            return False
        return self._is_pair_plan(plan)

    def _all_no_hedge(self, plan: TradePlan) -> bool:
        names = set(plan.broker_names) or {b.name for b in self.configured_brokers}
        cfgs = [b.config for b in self.configured_brokers if b.name in names]
        return bool(cfgs) and all(getattr(c, "no_hedge", False) for c in cfgs)

    async def _hedge_conflict(
        self, broker: Broker, plan: TradePlan, trade: TradeRecord, broker_symbol: str | None = None
    ) -> str | None:
        """Turkish skip text when ``broker`` (no_hedge) already holds an active (pending or open) position on
        the same instrument in the opposite direction, else None. Same instrument: the same broker symbol
        (``broker_symbol`` is the plan's symbol on this broker; USDJPY is not USDCAD). When either side has no
        broker symbol, the two plans decide (:meth:`_same_instrument`)."""
        if not getattr(broker.config, "no_hedge", False):
            return None
        wanted = (broker_symbol or "").strip().upper()
        for trade_id, pos in await self.db.active_broker_positions():
            if pos.broker != broker.name or trade_id == trade.id or pos.side is not plan.side.opposite:
                continue
            held = (pos.broker_symbol or "").strip().upper()
            if wanted and held:
                if wanted != held:
                    continue
            else:
                other = await self.db.get_trade(trade_id)
                if other is None or not self._same_instrument(other.plan, plan):
                    continue
            state = ("bekleyen sanal tetikli giriş" if is_virtual_pending(pos)
                     else "bekleyen limit emri" if pos.status is PositionStatus.PENDING else "açık")
            return (f"{esc(plan.base_asset)}: {state} {pos.side.value.upper()} var; ters yönlü "
                    f"{plan.side.value.upper()} sinyali hedging yasağı nedeniyle açılmadı")
        return None

    def _plan_brokers(self, plan: TradePlan) -> list[Broker]:
        """Connected brokers the plan may trade on (its route's brokers; all when it names none)."""
        if not plan.broker_names:
            return list(self.brokers)
        return [b for b in self.brokers if b.name in plan.broker_names]

    async def _conf_route(
        self, conf: PendingConfirmation, msgs: list[StoredMessage]
    ) -> tuple[TopicRoute | None, str | None]:
        """Route of a confirmation: from its source messages' topic, else from its plan / managed trade."""
        route, title = self._route_of(msgs)
        if route is not None or not self.config.telegram.topics:
            return route, title
        if isinstance(conf.plan, TradePlan):
            route = self._route_named(conf.plan.route)
        else:
            trade_id = conf.plan.trade_id if isinstance(conf.plan, ManagementPlan) else conf.action.target_trade_id
            trade = await self.db.get_trade(trade_id) if trade_id is not None else None
            route = self._route_named(trade.plan.route) if trade is not None else None
        return route, title

    def _topic_unknown_issues(self, ev: Evaluation | None, route: TopicRoute | None) -> list[ValidationIssue]:
        """Topics are configured but a new trade's topic is unknown (its messages are gone): never automatic."""
        if ev is None or route is not None or not self.config.telegram.topics or not isinstance(ev.plan, TradePlan):
            return []
        return [_issue("topic_unknown",
                       "Sinyalin geldiği konu (topic) belirlenemedi; hangi brokerda açılacağı bilinmediği için "
                       "bu işlem açılamaz. Gerekirse işlemi brokerda elle açın.", blocking=True)]

    async def _parse_context(
        self, route: TopicRoute | None, title: str | None, reply_to: StoredMessage | None = None, *,
        now: datetime | None = None, **extra: Any,
    ) -> ParseContext:
        """Parser context; open trades and topic of ``route`` (None: every trade, no topic kind)."""
        return ParseContext(now=now or self._now(), open_trades=await self._open_trade_summaries(route),
                            reply_to=reply_to, topic_title=title, topic_kind=self._topic_kind(route), **extra)

    async def _open_trade_summaries(self, route: TopicRoute | None = None) -> list[dict[str, Any]]:
        """OPEN TRADES context: levels with their close fractions, which TPs already hit, share still open.
        ``route``: only the trades of that forum topic route (None: every active trade)."""
        out: list[dict[str, Any]] = []
        for t in await self.db.active_trades():
            if not self._in_scope(t, route):
                continue
            try:
                hit, open_fraction = tp_progress(t, await self.db.broker_positions(t.id))
            except Exception:
                logger.exception("TP progress of trade %s failed", t.id)
                hit, open_fraction = None, None   # type: ignore[assignment]
            out.append(t.summary(tp_hit=hit, open_fraction=open_fraction))
        return out

    async def _filled_tp_levels(self, trade: TradeRecord) -> list[float]:
        """Exact prices of the trade's current TP levels that already filled on some broker."""
        hit, _share = tp_progress(trade, await self.db.broker_positions(trade.id))
        return [tp.price for tp, h in zip(trade.current_take_profits, hit) if h]

    async def _price(self, base: str | None, quote: str | None, market_type: MarketType | None,
                     raw_symbol: str | None = None, *, fresh: bool = False) -> float | None:
        if not base:
            return None
        try:
            return await self.prices.price(base, quote, market_type, raw_symbol, fresh=fresh)
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            logger.error("Price oracle call for %s was cancelled without a cancel request", base)
            return None
        except Exception:
            logger.exception("Price oracle failed for %s", base)
            return None

    async def _reference_mid(self, ref_id: str, base: str) -> float | None:
        """Fresh mid of ``base`` on the reference exchange; None when it cannot be read (never raises)."""
        try:
            return await self.reference_prices.mid(ref_id, base, fresh=True)
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            logger.error("Reference price call for %s on %s was cancelled without a cancel request", base, ref_id)
        except Exception:
            logger.exception("Reference price failed for %s on %s", base, ref_id)
        return None

    async def _plan_price_within(self, plan: TradePlan, raw_symbol: str | None, timeout_sec: float) -> float | None:
        """The market price the plan was judged with (:meth:`_price`: normally still in the oracle's cache),
        giving up (None) after ``timeout_sec``: for a reading that is useful but must never hold an open flow
        up."""
        try:
            return await asyncio.wait_for(
                self._price(plan.base_asset, plan.quote_asset, plan.market_type, raw_symbol), timeout_sec)
        except TimeoutError:
            logger.warning("Price of %s not read within %.0fs", plan.base_asset, timeout_sec)
            return None

    async def _load_messages(self, chat_id: int, ids: list[int]) -> list[StoredMessage]:
        out: list[StoredMessage] = []
        for mid in dict.fromkeys(ids):
            m = await self.db.get_message(chat_id, mid)
            if m is not None:
                out.append(m)
        out.sort(key=lambda m: (utc(m.date), m.message_id))
        return out

    async def _reply_context(self, chat_id: int, msgs: list[StoredMessage]) -> StoredMessage | None:
        """The message replied to by the newest message that is a reply. In a forum topic every message
        "replies" to the topic's root (reply_to == topic_id): that is not a reply."""
        for m in reversed(msgs):
            if m.reply_to_message_id is not None and m.reply_to_message_id != m.topic_id:
                return await self.db.get_message(chat_id, m.reply_to_message_id)
        return None

    async def _worth_reading(self, msgs: list[StoredMessage], route: TopicRoute | None) -> bool:
        """Does this batch go to the model? Everything when ``llm.prefilter`` is off; otherwise what the
        prefilter lets through (pictures, trading words, numbers...) and, whatever its words, what the prefilter
        must never hide: a text that is a real Telegram reply (trades are managed by replies to their signal:
        "Başa baş", "Geçersiz"), and any text while a trade of the topic's route is active (None: of any
        topic). Chatter while no trade is active stays filtered."""
        if not self.config.llm.prefilter or any(prefilter.looks_relevant(m) for m in msgs):
            return True
        texts = [m for m in msgs if (m.text or "").strip()]
        if not texts:
            return False
        if any(m.reply_to_message_id is not None and m.reply_to_message_id != m.topic_id for m in texts):
            return True
        return any(self._in_scope(t, route) for t in await self.db.active_trades())

    async def _unstored_reply_id(self, chat_id: int, ids: list[int]) -> int | None:
        """Id of the message that the newest of the messages ``ids`` replies to, when that message is NOT in
        storage (posted before the bot started, or never kept): :meth:`_reply_context` gives None for it, like
        for a message that is no reply at all. None when the newest reply's target is stored, or when none of
        the messages is a real reply (in a forum topic a "reply" to the topic's root is not one)."""
        for m in reversed(await self._load_messages(chat_id, ids)):
            target = m.reply_to_message_id
            if target is not None and target != m.topic_id:
                return target if await self.db.get_message(chat_id, target) is None else None
        return None

    # ------------------------------------------------------------------ lifecycle
    async def start(self, *, monitor: bool = True) -> None:
        """Connect brokers, restore confirmations, start the monitor (``monitor=False``: tests)."""
        self._retry_failed = True
        await self.connect_brokers()
        for broker in self.configured_brokers:
            # Optional and required brokers alike: one that could not connect is retried in the background (a
            # broker that could not even be created has no object here and stays down until the next start).
            if broker.name in self.failed_brokers and broker.name not in self._retrying:
                self._retrying.add(broker.name)
                self._spawn(self._retry_optional(broker), f"retry-{broker.name}")
        await self._restore_confirmations()
        await self._reconcile_trades()
        await self._report_interrupted_legs()
        await self._drop_stale_messages()
        if monitor and (self._monitor_task is None or self._monitor_task.done()):
            self._start_monitor()
        await self._notify(await self._startup_text(), Severity.INFO)
        await self._warn_open_senders()
        logger.info("Engine started: %d/%d broker(s) connected, dry_run=%s",
                    len(self.brokers), len(self.configured_brokers), self.config.dry_run)

    async def _drop_stale_messages(self) -> None:
        """Unconsumed messages older than the restart catch-up age are never executed later
        (they could otherwise be merged into the next signal from the same sender)."""
        consume = getattr(self.db, "consume_stale_messages", None)
        if consume is None:
            return
        cutoff = self._now() - timedelta(seconds=self.config.telegram.catchup_max_age_sec)
        count = await consume(cutoff)
        if count:
            logger.warning("Startup: %d stale unprocessed message(s) older than %s marked consumed", count, cutoff)
            await self._notify(
                f"ℹ️ Yeniden başlatma: işlenmeyi bekleyen {count} eski mesaj "
                f"({self.config.telegram.catchup_max_age_sec} sn'den eski) işlem yapılmadan kapatıldı. "
                "Kaçırılmış bir sinyal olabilir; grubu kontrol edin.",
                Severity.WARNING,
            )

    async def _warn_open_senders(self) -> None:
        """Empty allowed_sender_ids: every member's trade in a group is copied. Log at every start,
        tell the owner once (per set of source chats)."""
        tg = self.config.telegram
        if tg.allowed_sender_ids:
            return
        logger.warning(
            "telegram.allowed_sender_ids is empty: trade messages from ANY sender in the source chats are copied. "
            "Set allowed_sender_ids to the trader's user id for groups where members can post."
        )
        marker = sorted(str(c) for c in tg.source_chats)
        try:
            if await self.db.get_state("open_senders_warned") == marker:
                return
            await self.db.set_state("open_senders_warned", marker)
        except Exception:
            logger.exception("open_senders_warned state")
        await self._notify(
            "⚠️ <b>telegram.allowed_sender_ids boş</b>: kaynak gruplarda yazan HERKESİN işlem mesajı "
            "kopyalanır (yalnızca yöneticilerin yazabildiği kanallarda sorun yok). Grup üyeleri de yazabiliyorsa "
            "sadece trader'ı kopyalamak için config.yaml'da <code>allowed_sender_ids: [trader_id]</code> ayarlayın. "
            "(Bu uyarı bir kez gönderilir.)",
            Severity.WARNING,
        )

    def _risk_per_r_text(self) -> str:
        """'binance 10 USD, breakout 50 USD' when brokers use fixed amounts, else 'bakiyenin %2'si'."""
        pct = f"bakiyenin {fmt.fmt_pct(self.config.risk.risk_per_r_pct, possessive=True)}"
        cfgs = [b for b in self.config.brokers if b.enabled]
        if not any(getattr(b, "risk_usd", None) for b in cfgs):
            return pct
        parts = []
        for b in cfgs:
            fixed = getattr(b, "risk_usd", None)
            parts.append(f"{b.name} {fixed:g} USD" if fixed else f"{b.name} {pct}")
        return ", ".join(parts)

    async def connect_brokers(self) -> None:
        """Connect every configured broker; failures are reported and excluded. No other side effects
        (the read-only CLI commands use this instead of start())."""
        if any(getattr(b, "risk_usd", None) for b in self.config.brokers if b.enabled):
            fmt.RISK_PER_R_NOTE = self._risk_per_r_text()
        results = await asyncio.gather(*(self._connect(b) for b in self.configured_brokers))
        self.brokers = [b for b, ok in zip(self.configured_brokers, results, strict=True) if ok]

    async def _try_connect(self, broker: Broker) -> None:
        """Connect one broker and read its currency. Raises on failure (the caller decides what that means)."""
        set_notifier = getattr(broker, "set_notifier", None)
        if set_notifier is not None:     # manual (message-only) brokers talk to the owner
            set_notifier(self.notifier)
        broker.set_risk_config(self.config.risk)
        await asyncio.wait_for(broker.connect(), CONNECT_TIMEOUT_SEC)
        broker.set_risk_config(self.config.risk)
        for warning in getattr(broker, "startup_warnings", None) or []:
            await self._notify(f"⚠️ {esc(broker.name)}: {esc(warning)}", Severity.WARNING)
        try:
            self._currency[broker.name] = await asyncio.wait_for(broker.account_currency(), READ_TIMEOUT_SEC)
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            self._currency[broker.name] = ""      # like any other failed read: connected, currency unknown
        except Exception:
            self._currency[broker.name] = ""

    @staticmethod
    def _is_optional(broker: Broker) -> bool:
        return bool(getattr(broker.config, "optional", False))

    async def _connect(self, broker: Broker) -> bool:
        try:
            await self._try_connect(broker)
            return True
        except (Exception, asyncio.CancelledError) as exc:
            cancelled = isinstance(exc, asyncio.CancelledError)
            if cancelled and not stray_cancel():
                raise
            reason = _CANCELLED_CONNECT if cancelled else (str(exc) or type(exc).__name__)
            self.failed_brokers[broker.name] = reason
            logger.exception("Broker %s failed to connect", broker.name)
            if self._is_optional(broker):
                self.optional_down[broker.name] = reason
                await self._notify(
                    f"⚠️ <b>{esc(broker.name)}</b> bağlanamadı (isteğe bağlı hesap). Bot ve diğer hesaplar "
                    f"normal çalışıyor; {esc(broker.name)} için gelen sinyaller atlanacak. Arka planda "
                    f"{fmt.fmt_number(self.optional_retry_sec / 60.0)} dakikada bir tekrar denenecek.\n"
                    f"Sebep: <code>{esc(reason)}</code>",
                    Severity.WARNING,
                )
            elif self._retry_failed:
                first = min(self.optional_retry_sec, self.required_retry_first_sec)
                await self._notify(
                    f"{esc(broker.name)} bağlanamadı: <code>{esc(reason)}</code>\n"
                    f"Bu hesap bağlanana kadar ona gelen sinyaller bu hesapta AÇILMAZ (her sinyalde ayrıca "
                    f"bildirilir). Arka planda yeniden denenecek (ilk deneme {fmt.fmt_number(first)} saniye sonra, "
                    f"en seyrek {fmt.fmt_number(self.optional_retry_sec / 60.0)} dakikada bir); bağlanınca haber "
                    "verilecek.",
                    Severity.ERROR,
                )
            else:
                await self._notify(f"{esc(broker.name)} bağlanamadı: <code>{esc(reason)}</code>", Severity.ERROR)
            await self._close_quietly(broker)
            return False

    async def _close_quietly(self, broker: Broker) -> None:
        """``broker.close()`` after a failed connect: nothing it raises matters."""
        try:
            await broker.close()
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            logger.debug("close of %s after a failed connect was cancelled from the inside", broker.name)
        except Exception:
            logger.debug("close after failed connect", exc_info=True)

    @staticmethod
    def _reopen(broker: Broker) -> None:
        """Make an adapter that was closed after a failed connect usable for the next attempt. ccxt refuses
        every request on an exchange object after its ``close()`` ("instance was closed by the user") until
        ``open()`` is called again, and the engine closes a broker after each failed connect: without this an
        exchange account could never be connected by a retry. A no-op for every adapter that keeps no ccxt
        exchange in ``exchange`` (looked for on the adapter and, in dry-run mode, on the adapter it wraps) and
        for an exchange that is not closed."""
        for holder in (broker, getattr(broker, "inner", None)):
            exchange = getattr(holder, "exchange", None)
            if exchange is None or not getattr(exchange, "closed_by_user", False):
                continue
            try:
                exchange.open()
            except Exception:
                logger.exception("%s: the closed exchange object could not be opened again", broker.name)

    async def _retry_optional(self, broker: Broker) -> None:
        """Keep trying to connect a broker that was down at startup (optional or required). Silent while it
        keeps failing (the owner was warned once, and every signal that skips it says so); one message when
        it comes up. An optional broker is tried every ``optional_retry_sec``; a required one sooner at first
        (``required_retry_first_sec``, doubled after each failure up to ``optional_retry_sec``)."""
        name = broker.name
        optional = self._is_optional(broker)
        kind = "Optional" if optional else "Required"
        delay = self.optional_retry_sec if optional else min(self.optional_retry_sec, self.required_retry_first_sec)
        try:
            while self._accepting and not self._brokers_closed:
                await asyncio.sleep(delay)
                delay = min(self.optional_retry_sec, delay * 2.0)
                if not self._accepting or self._brokers_closed:
                    return
                self._reopen(broker)
                try:
                    await self._try_connect(broker)
                except (Exception, asyncio.CancelledError) as exc:
                    cancelled = isinstance(exc, asyncio.CancelledError)
                    if cancelled and not stray_cancel():        # the engine is stopping
                        with contextlib.suppress(Exception):
                            await broker.close()
                        raise
                    reason = _CANCELLED_CONNECT if cancelled else (str(exc) or type(exc).__name__)
                    self.failed_brokers[name] = reason
                    if optional:
                        self.optional_down[name] = reason
                    logger.warning("%s broker %s still not connected: %s", kind, name, reason)
                    await self._close_quietly(broker)
                    continue
                if not self._accepting or self._brokers_closed:
                    with contextlib.suppress(Exception):
                        await broker.close()
                    return
                self.failed_brokers.pop(name, None)
                self.optional_down.pop(name, None)
                up = {b.name for b in self.brokers} | {name}
                self.brokers = [b for b in self.configured_brokers if b.name in up]
                logger.info("%s broker %s connected after retry", kind, name)
                await self._notify(f"✅ <b>{esc(name)}</b> bağlandı. Bundan sonraki sinyaller bu hesapta da açılacak "
                                   "(atlanan eski sinyaller açılmaz).", Severity.INFO)
                return
        finally:
            self._retrying.discard(name)

    async def _restore_confirmations(self) -> None:
        now = self._now()
        for conf in await self.db.pending_confirmations():
            if utc(conf.expires_at) <= now:
                await self._expire(conf.id)
            else:
                self._arm_expiry(conf)

    async def _reconcile_trades(self) -> None:
        """After a restart: fix trade statuses and flag trades whose opening was interrupted."""
        for trade in await self.db.active_trades():
            positions = await self.db.broker_positions(trade.id)
            if positions:
                status = rollup_status(positions)
                if status is not trade.status:
                    await self.db.update_trade(trade.id, status=status)
                if trade.status is TradeStatus.OPENING:   # interrupted while sending: other brokers unknown
                    saved = ", ".join(esc(p.broker) for p in positions)
                    await self._notify(
                        f"#{trade.id} {esc(trade.plan.base_asset)} işlemi gönderilirken sistem durdu. Kayıtlı "
                        f"pozisyonlar: {saved}. Diğer hesaplarda kayıtsız emir/pozisyon olabilir; elle kontrol edin.",
                        Severity.WARNING,
                    )
                continue
            await self.db.update_trade(trade.id, status=TradeStatus.FAILED,
                                       note="açılış yarıda kaldı (yeniden başlatma)")
            await self._notify(
                f"#{trade.id} {esc(trade.plan.base_asset)} {trade.plan.side.value.upper()} işlemi açılırken sistem "
                "durdu; kayıtlı pozisyon yok. Borsada/brokerda açık emir veya pozisyon olup olmadığını "
                "elle kontrol edin.",
                Severity.WARNING,
            )

    async def _late_state(self, *, add: list[_LateLeg] | None = None, remove: list[_LateLeg] | None = None) -> None:
        """Keep in storage which legs are waiting for their broker: after a restart nothing else would tell that
        an order may exist there without a record (the trade itself is no longer OPENING)."""
        async with self._late_state_lock:
            try:
                data = await self.db.get_state(_LATE_KEY, {}) or {}
                if not isinstance(data, dict):
                    data = {}
                for leg in add or []:
                    data[f"{leg.trade_id}:{leg.broker}"] = {"trade_id": leg.trade_id, "broker": leg.broker,
                                                            "label": leg.label, "since": self._now().isoformat()}
                for leg in remove or []:
                    data.pop(f"{leg.trade_id}:{leg.broker}", None)
                await self.db.set_state(_LATE_KEY, data)
            except Exception:
                logger.exception("Could not store the late open legs")

    async def _report_interrupted_legs(self) -> None:
        """After a restart: legs that were still waiting for their broker when the process stopped."""
        try:
            data = await self.db.get_state(_LATE_KEY, {}) or {}
        except Exception:
            logger.exception("Could not read the late open legs")
            return
        if not isinstance(data, dict) or not data:
            return
        for item in data.values():
            if not isinstance(item, dict):
                continue
            broker = esc(item.get("broker") or "?")
            await self._notify(
                f"⚠️ {esc(item.get('label') or '#' + str(item.get('trade_id')))}: bot durduğunda {broker} emrine "
                f"henüz yanıt gelmemişti. Emrin açılıp açılmadığı bilinmiyor; {broker} hesabında kayıtsız emir veya "
                "pozisyon olabilir, elle kontrol edin.",
                Severity.WARNING,
            )
        async with self._late_state_lock:
            try:
                await self.db.set_state(_LATE_KEY, {})
            except Exception:
                logger.exception("Could not clear the late open legs")

    async def _startup_text(self) -> str:
        mode = "🧪 DRY-RUN (gerçek emir gönderilmez)" if self.config.dry_run else "🔴 CANLI (gerçek emirler)"
        names = ", ".join(esc(b.name) for b in self.brokers) or "yok"
        lines = [
            "🚀 <b>Kopya-trade motoru başladı</b>",
            f"Mod: {mode}",
            f"Brokerlar: {names}",
        ]
        if self.failed_brokers:
            lines.append("❌ Bağlanamayan: " + ", ".join(
                esc(n) + (" (arka planda yeniden deneniyor)" if n in self._retrying else "")
                for n in self.failed_brokers))
        lines.append(f"Kaynak sohbet: {len(self.config.telegram.source_chats)}")
        if self.config.telegram.topics:
            lines.append(self._routes_text())
        lines.append(f"Risk: 1R = {esc(self._risk_per_r_text())}")
        pending = await self.db.pending_confirmations()
        if pending:
            lines.append(f"Bekleyen onay: {len(pending)}")
        if await self._is_paused():
            lines.append("⏸ Yeni işlem açma DURAKLATILMIŞ (/devam ile açın).")
        return "\n".join(lines)

    def _routes_text(self) -> str:
        """HTML: followed forum topics and their brokers."""
        connected = {b.name for b in self.brokers}

        def brokers_of(route: TopicRoute) -> str:
            if not route.brokers:
                return "tüm brokerlar"
            return ", ".join(esc(n) + ("" if n in connected else " ❌") for n in route.brokers)

        items = [f"{esc(r.name)} ('{esc(' / '.join(r.title_contains))}') → {brokers_of(r)}"
                 for r in self.config.telegram.topics]
        return "Takip edilen konular: " + "; ".join(items) + " (diğer konular yok sayılır)"

    def begin_shutdown(self) -> None:
        """Stop accepting new work (message batches, edits, approvals, corrections, /kapat); running
        order flows and the monitor's current pass are left to finish. Idempotent."""
        if self._accepting:
            logger.info("Engine: shutdown requested; no new work is accepted")
        self._accepting = False
        self.monitor.request_stop()

    def _busy(self) -> bool:
        if any(not t.done() for t in self._critical):
            return True
        if self._open_lock.locked() or any(lock.locked() for lock in self._trade_locks.values()):
            return True
        return self._monitor_task is not None and not self._monitor_task.done()

    async def drain(self, timeout: float) -> bool:
        """Wait until no order flow, trade lock or monitor pass is running. False on timeout."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, timeout)
        while self._busy():
            if loop.time() >= deadline:
                return False
            await asyncio.sleep(0.05)
        return True

    async def stop(self, *, grace_sec: float | None = None) -> None:
        """Graceful stop: no new work, wait (up to ``grace_sec``) for running order flows and the
        monitor's current pass, then cancel timers and close the brokers."""
        self.begin_shutdown()
        grace = self.shutdown_grace_sec if grace_sec is None else grace_sec
        if not await self.drain(grace):
            running = sorted(t.get_name() for t in self._critical if not t.done())
            logger.error("Shutdown: order flows still running after %.0fs: %s", grace, running)
            await self._notify(
                f"⚠️ Kapanırken süren emir işlemleri {fmt.fmt_number(float(grace))} sn içinde bitmedi "
                f"({esc(', '.join(running) or 'izleme')}). Bot yeniden başlayınca açık pozisyonları ve stopları "
                "borsada/brokerda kontrol edin.",
                Severity.WARNING,
            )
        self._brokers_closed = True       # set right after the drain check: no new order flow can start
        tasks: list[asyncio.Task[Any]] = []
        if self._monitor_task is not None:
            tasks.append(self._monitor_task)
            self._monitor_task = None
        tasks += list(self._tasks)
        self._expiry_tasks.clear()
        self._incomplete.clear()
        self._retry_tasks.clear()
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for broker in self.brokers:
            try:
                await broker.close()
            except asyncio.CancelledError:
                if not stray_cancel():
                    raise
                logger.error("Closing broker %s was cancelled from the inside (no cancel request)", broker.name)
            except Exception:
                logger.exception("Error closing broker %s", broker.name)
        if self._own_oracle and hasattr(self.prices, "close"):
            try:
                await self.prices.close()
            except Exception:
                logger.debug("oracle close failed", exc_info=True)
        if self._own_reference and hasattr(self.reference_prices, "close"):
            try:
                await self.reference_prices.close()
            except Exception:
                logger.debug("reference oracle close failed", exc_info=True)
        logger.info("Engine stopped")

    # ------------------------------------------------------------------ incoming messages
    async def on_messages_ready(self, chat_id: int, sender_id: int | None, topic_id: int | None = None) -> None:
        """A sender's messages in a chat (``topic_id``: forum topic, None = no topic / General) are ready."""
        if not self._accepting:
            # Left unconsumed: the listener re-triggers recent unprocessed messages after the restart.
            logger.info("Shutting down: batch of %s/%s (topic %s) not processed", chat_id, sender_id, topic_id)
            return
        key = stream_key(chat_id, sender_id, topic_id)
        async with self._sender_lock(key):
            await self._process_batch(key)

    async def _process_batch(self, key: _Key) -> None:
        chat_id, sender_id, topic_id = _key_parts(key)
        tg = self.config.telegram
        now = self._now()
        query: dict[str, Any] = dict(
            since=now - timedelta(seconds=tg.context_window_sec),
            sender_id=sender_id,
            only_unconsumed=True,
            limit=tg.max_context_messages,
        )
        if topic_id is not None:
            query["topic_id"] = topic_id        # only that forum topic's messages
        msgs = await self.db.recent_messages(chat_id, **query)
        msgs = await self._with_failed_messages(key, msgs)
        # Never mix topics in one batch (a General batch is not filtered by storage); the other topics'
        # messages stay unconsumed for their own batch. In a non-forum group every topic_id is None.
        msgs = [m for m in msgs if m.topic_id == topic_id]
        if not msgs:
            self._clear_parse_failures(key)
            return
        batch_ids = [m.message_id for m in msgs]
        route, title = self._route_of(msgs)
        if tg.topics and route is None:
            # Not one of the followed topics (chat, analysis, results...): consumed without reading.
            await self.db.mark_consumed(chat_id, batch_ids)
            self._clear_parse_failures(key, batch_ids)
            logger.info("Topic %r (id %s) is not followed: messages %s ignored", title, topic_id, batch_ids)
            await self._log("topic_ignored", {"chat_id": chat_id, "topic_id": topic_id, "topic_title": title,
                                              "message_ids": batch_ids})
            if title is None and topic_id is not None:
                # Not "another topic": the topic's title is unknown, so it may well be one of the followed ones.
                await self._notify_once(
                    "topic-unknown",
                    "⚠️ Kaynak gruptan gelen bir mesajın hangi konuya (topic) ait olduğu belirlenemedi; mesaj "
                    f"okunmadan kapatıldı (mesaj id: {esc(', '.join(str(i) for i in batch_ids))}). Takip edilen "
                    "konulardan birindeyse bu mesajdan işlem AÇILMADI; grubu kontrol edin.",
                    Severity.WARNING,
                )
            return
        if not await self._worth_reading(msgs, route):
            await self.db.mark_consumed(chat_id, batch_ids)
            self._clear_parse_failures(key, batch_ids)
            logger.debug("Prefilter: %s ignored", batch_ids)
            return

        reply_to = await self._reply_context(chat_id, msgs)
        ctx = await self._parse_context(route, title, reply_to, now=now)
        try:
            result = await self.parser.parse(msgs, ctx)
        except ParserError as err:
            await self._on_parse_error(key, msgs, err, transient=is_transient_parse_error(err))
            return
        except Exception as err:   # a bug, not an API hiccup: do not loop on it
            logger.exception("Parser crashed")
            await self._on_parse_error(key, msgs, err, transient=False)
            return
        self._clear_parse_failures(key, batch_ids)
        actions = self._fill_used_ids(result.actions, batch_ids)
        await self._log("parsed", {
            "chat_id": chat_id, "message_ids": batch_ids, "model": result.model,
            "actions": [a.model_dump(mode="json") for a in actions],
            **({"topic_id": topic_id, "route": route.name} if route is not None else {}),
        })

        incompletes = [a for a in actions if a.action is ActionType.INCOMPLETE]
        kept_for_incomplete = {i for a in incompletes for i in a.used_message_ids}
        decided = False
        for action in actions:
            if action.action is ActionType.INCOMPLETE:
                continue
            # A message an incomplete reading keeps stays unconsumed for it, whatever other action also
            # lists it: consuming it would make the incomplete watch expire silently (the owner never
            # asked). A later re-read of it is guarded: an OPEN by message_already_traded, a management
            # action by message_already_applied.
            ids = [i for i in action.used_message_ids if i not in kept_for_incomplete]
            try:
                if action.action is ActionType.NONE:
                    await self.db.mark_consumed(chat_id, ids)
                    await self._handle_none(action, chat_id)
                    continue
                # Decided: consume BEFORE sending orders (never execute a signal twice).
                await self.db.mark_consumed(chat_id, ids)
                decided = True
                if action.action is ActionType.OPEN:
                    await self._handle_open(action, chat_id, reply_to=reply_to, route=route)
                else:
                    await self._handle_management(action, chat_id, reply_to=reply_to, route=route)
            except Exception as exc:
                logger.exception("Handling %s failed", action.action.value)
                await self.db.mark_consumed(chat_id, ids)
                await self._notify(
                    f"Sinyal işlenirken hata ({esc(action.summary_tr or action.action.value)}): "
                    f"<code>{esc(exc)}</code>",
                    Severity.ERROR,
                )
        if incompletes:
            # One watch per (chat, sender); the newest incomplete reading replaces older ones.
            ids_all = list(dict.fromkeys(i for a in incompletes for i in a.used_message_ids))
            self._watch_incomplete(key, incompletes[-1].model_copy(update={"used_message_ids": ids_all}),
                                   route=route)
        elif decided:
            self._cancel_incomplete(key)
        # Messages the parser saw but did not keep for an incomplete signal are done.
        leftover = [i for i in batch_ids if i not in kept_for_incomplete]
        await self.db.mark_consumed(chat_id, leftover)

    @staticmethod
    def _fill_used_ids(actions: list[ParsedAction], batch_ids: list[int]) -> list[ParsedAction]:
        """Actions without used_message_ids get the batch messages no other action claimed
        (a NONE never takes messages another action uses)."""
        claimed = {i for a in actions for i in a.used_message_ids}
        free = [i for i in batch_ids if i not in claimed]
        out: list[ParsedAction] = []
        for a in actions:
            if not a.used_message_ids:
                ids = free if (free or a.action is ActionType.NONE) else batch_ids
                a = a.model_copy(update={"used_message_ids": list(ids)})
            out.append(a)
        return out

    @staticmethod
    def _none_is_unsure(action: ParsedAction) -> bool:
        return bool(action.ambiguities) or action.confidence < NONE_ASK_BELOW_CONFIDENCE

    async def _with_failed_messages(self, key: _Key, msgs: list[StoredMessage]) -> list[StoredMessage]:
        """Add the exact messages whose parse failed transiently (a retry is pending) to ``msgs``: the
        context window may have moved past them while the model was unreachable. Messages that another
        path consumed meanwhile are forgotten; messages that are gone from storage are given up with the
        final notice (never silently)."""
        failed = self._failed_ids.get(key)
        if not failed:
            return msgs
        chat_id = key[0]
        ids = list(failed)
        stored = await self._load_messages(chat_id, ids)
        found = {m.message_id for m in stored}
        gone = [i for i in ids if i not in found]
        handled = [m.message_id for m in stored if m.consumed]
        if handled:
            logger.info("Failed-parse messages %s were handled meanwhile; no retry for them", handled)
        if gone:
            attempts = max(failed.get(i, 0) for i in gone)
            await self._log("parse_failed", {"chat_id": chat_id, "message_ids": gone, "error": "messages gone",
                                             "transient": True})
            await self.db.mark_consumed(chat_id, gone)
            await self._remember_outcome(chat_id, gone, "parse_failed", None)
            await self._notify(
                f"Sinyal {attempts} denemede okunamadı ve mesaj artık bulunamıyor (id: "
                f"{esc(', '.join(str(i) for i in gone))}).\n"
                "Tekrar denenmeyecek: bu mesajdan işlem açılmadı ve sonradan otomatik açılmayacak. Gerekiyorsa "
                "kaynak grupta kontrol edip işlemi elle açın.",
                Severity.WARNING,
            )
        self._clear_parse_failures(key, gone + handled)
        known = {m.message_id for m in msgs}
        extra = [m for m in stored if not m.consumed and m.message_id not in known]
        if not extra:
            return msgs
        return sorted([*msgs, *extra], key=lambda m: (utc(m.date), m.message_id))

    def _clear_parse_failures(self, key: _Key, ids: list[int] | None = None) -> None:
        """Forget the transient-failure state of ``ids`` (all of the key's when None). When no failed
        message is left, the counter is reset and a pending retry is cancelled: the state never leaks
        into the next signal of this sender."""
        failed = self._failed_ids.get(key)
        if failed is not None and ids is not None:
            for i in ids:
                failed.pop(i, None)
        if ids is None or not failed:
            self._failed_ids.pop(key, None)
            self._parse_failures.pop(key, None)
            task = self._retry_tasks.pop(key, None)
            if task is not None and task is not asyncio.current_task():
                task.cancel()
        else:
            self._parse_failures[key] = max(failed.values())

    async def _on_parse_error(
        self, key: _Key, msgs: list[StoredMessage], err: BaseException, *, transient: bool
    ) -> None:
        chat_id = key[0]
        if not transient:
            self._clear_parse_failures(key)
            await self._give_up_parse(chat_id, msgs, err, transient=False, attempts=1)
            return
        failed = self._failed_ids.setdefault(key, {})
        for m in msgs:
            failed[m.message_id] = failed.get(m.message_id, 0) + 1
        budget = len(self.retry_delays)
        exhausted = [m for m in msgs if failed[m.message_id] > budget]
        waiting = [m for m in msgs if failed[m.message_id] <= budget]
        if exhausted:
            attempts = max(failed[m.message_id] for m in exhausted)
            self._clear_parse_failures(key, [m.message_id for m in exhausted])
            await self._give_up_parse(chat_id, exhausted, err, transient=True, attempts=attempts)
        if not waiting:
            return
        attempt = max(failed[m.message_id] for m in waiting)
        self._parse_failures[key] = max(failed.values())
        delay = self.retry_delays[attempt - 1]
        logger.warning("Transient parse error (%s); retry %d of %s in %.0fs", err, attempt,
                       [m.message_id for m in waiting], delay)
        first = [m for m in waiting if failed[m.message_id] == 1]
        if first:
            await self._notify(
                f"Sinyal okunamadı (geçici hata: {esc(err)}); tekrar denenecek.\n"
                f"Mesaj: <i>{esc(_excerpt(first))}</i>",
                Severity.WARNING,
            )
        old = self._retry_tasks.pop(key, None)
        if old is not None and old is not asyncio.current_task():
            old.cancel()
        self._retry_tasks[key] = self._spawn(self._retry_later(key, delay), f"parse-retry-{chat_id}")

    async def _give_up_parse(
        self, chat_id: int, msgs: list[StoredMessage], err: BaseException, *, transient: bool, attempts: int
    ) -> None:
        ids = [m.message_id for m in msgs]
        await self._log("parse_failed", {"chat_id": chat_id, "message_ids": ids,
                                         "error": str(err), "transient": transient})
        # Given up: consume, so the stale signal can never be picked up by a later batch and executed
        # silently; and remember it, so an action built on it later (a reply) is asked, never automatic.
        await self.db.mark_consumed(chat_id, ids)
        await self._remember_outcome(chat_id, ids, "parse_failed", None)
        if transient:
            await self._notify(
                f"Sinyal {attempts} denemede okunamadı (geçici hata: {esc(err) or esc(type(err).__name__)}).\n"
                f"Mesaj: <i>{esc(_excerpt(msgs))}</i>\n"
                "Tekrar denenmeyecek: bu mesajdan işlem açılmadı ve sonradan otomatik açılmayacak. Gerekiyorsa "
                "işlemi elle açın (sonraki bir mesaj bu sinyale dayanırsa önce size sorulur).",
                Severity.WARNING,
            )
            return
        await self._notify(
            f"Sinyal okunamadı: {esc(err) or esc(type(err).__name__)}\n"
            f"Mesaj: <i>{esc(_excerpt(msgs))}</i>\n"
            "İşlem açılmadı; gerekiyorsa elle kontrol edin.",
            Severity.WARNING,
        )

    async def _retry_later(self, key: _Key, delay: float) -> None:
        await asyncio.sleep(delay)
        if self._retry_tasks.get(key) is asyncio.current_task():
            del self._retry_tasks[key]
        await self.on_messages_ready(*key)

    async def _handle_none(self, action: ParsedAction, chat_id: int) -> None:
        if self._none_is_unsure(action):
            # "Probably not a trade" but the parser doubts it: let the owner decide.
            ev = Evaluation(
                kind="incomplete", action=action, plan=None,
                issues=merge_issues(
                    [_issue("unclear_message", "Bu mesajın bir işlem talimatı olup olmadığından emin olamadım.", True)],
                    validator.low_confidence_issues(action, self.config.llm),
                ),
            )
            await self._ask_eval(ev, chat_id)
            return
        if self.config.notifier.notify_on_ignored:
            await self._notify(f"💬 Yok sayıldı: {esc(action.summary_tr or 'işlem talimatı değil')}", Severity.INFO)

    # ------------------------------------------------------------------ incomplete signals
    def _watch_incomplete(self, key: _Key, action: ParsedAction, *, route: TopicRoute | None = None) -> None:
        self._cancel_incomplete(key)
        deadline = self._now() + timedelta(seconds=self.config.telegram.incomplete_timeout_sec)
        watch = _IncompleteWatch(chat_id=key[0], action=action, message_ids=list(action.used_message_ids),
                                 deadline=deadline, route=route)
        watch.task = self._spawn(self._incomplete_timeout(key, watch), f"incomplete-{key[0]}")
        self._incomplete[key] = watch

    def _cancel_incomplete(self, key: _Key) -> None:
        watch = self._incomplete.pop(key, None)
        if watch is not None and watch.task is not None and watch.task is not asyncio.current_task():
            watch.task.cancel()

    async def _incomplete_timeout(self, key: _Key, watch: _IncompleteWatch) -> None:
        await asyncio.sleep(max(0.0, (watch.deadline - self._now()).total_seconds()))
        async with self._sender_lock(key):
            if self._incomplete.get(key) is not watch:
                return
            del self._incomplete[key]
            msgs = await self._load_messages(watch.chat_id, watch.message_ids)
            if not any(not m.consumed for m in msgs):
                return      # a later parse completed (and consumed) them
            await self.db.mark_consumed(watch.chat_id, watch.message_ids)
            summary = watch.action.summary_tr or "eksik sinyal"
            if await self._is_paused():
                await self._notify(f"Duraklatıldı: eksik sinyal atlandı ({esc(summary)})", Severity.INFO)
                return
            secs = self.config.telegram.incomplete_timeout_sec
            reason = f"Sinyal {fmt.fmt_number(float(secs))} sn içinde tamamlanmadı; eksik bilgi var."
            ev = await self._evaluate_incomplete(watch.action, watch.chat_id, reason, route=watch.route)
            await self._ask_eval(ev, watch.chat_id)

    # ------------------------------------------------------------------ evaluation
    async def evaluate(
        self, action: ParsedAction, chat_id: int, *, reply_to: StoredMessage | None = None,
        exclude_conf: str | None = None, route: TopicRoute | None = None,
    ) -> Evaluation | None:
        """Validate one action without side effects. None for NONE actions.
        ``exclude_conf``: the confirmation being corrected / re-read (not a conflict with itself).
        ``route``: forum topic route of the messages (a new trade goes to its brokers; management only sees
        its trades); None = no routing."""
        if action.action is ActionType.OPEN:
            return await self._evaluate_open(action, chat_id, exclude_conf=exclude_conf, route=route)
        if action.action is ActionType.INCOMPLETE:
            return await self._evaluate_incomplete(
                action, chat_id, "Sinyal eksik: varlık, yön veya stop bilgisi tamamlanmadı.", route=route
            )
        if action.action in MANAGEMENT_ACTIONS:
            return await self._evaluate_management(action, chat_id, reply_to, exclude_conf=exclude_conf, route=route)
        return None

    def _open_plan_extras(self, route: TopicRoute | None) -> dict[str, Any]:
        """What the validator needs to know about the route besides its name and brokers: its kind (which
        entry-to-stop distance is plausible: a coin, a currency pair, an index) and whether a percentage of
        equity means anything for the plan (False when every broker it can be traded on risks a fixed amount
        per R: the texts then leave "(%2)" out). Only the arguments this validator version takes."""
        extras: dict[str, Any] = {}
        if "route_kind" in _OPEN_PLAN_PARAMS:
            extras["route_kind"] = route.kind if route is not None else None
        if "risk_pct_applies" in _OPEN_PLAN_PARAMS:
            names = set(route.brokers) if route is not None else set()
            configs = [b.config for b in self.configured_brokers if not names or b.name in names]
            extras["risk_pct_applies"] = not (configs and all(getattr(c, "risk_usd", None) for c in configs))
        return extras

    async def _evaluate_open(
        self, action: ParsedAction, chat_id: int, *, exclude_conf: str | None = None,
        route: TopicRoute | None = None,
    ) -> Evaluation:
        now = self._now()
        base, quote, market_type = asset_of(action)
        market = await self._price(base, quote, market_type, action.symbol)
        plan, issues = validator.build_open_plan(
            action, risk=effective_risk(self.config.risk, route), llm=self.config.llm, market_price=market,
            source_chat_id=chat_id, now=now,
            route=route.name if route is not None else None,
            broker_names=list(route.brokers) if route is not None else [],
            **self._open_plan_extras(route),
        )
        issues = merge_issues(issues, validator.low_confidence_issues(action, self.config.llm))
        if plan is not None:
            issues = merge_issues(issues, await self._portfolio_issues(plan, chat_id, exclude_conf=exclude_conf))
        return Evaluation("open", action, plan, issues, market)

    async def _evaluate_incomplete(
        self, action: ParsedAction, chat_id: int, reason: str, *, route: TopicRoute | None = None
    ) -> Evaluation:
        base, quote, market_type = asset_of(action)
        market = await self._price(base, quote, market_type, action.symbol)
        _plan, issues = validator.build_open_plan(
            action, risk=effective_risk(self.config.risk, route), llm=self.config.llm, market_price=market,
            source_chat_id=chat_id, now=self._now(),
            route=route.name if route is not None else None,
            broker_names=list(route.brokers) if route is not None else [],
            **self._open_plan_extras(route),
        )
        return Evaluation("incomplete", action, None, merge_issues([_issue("incomplete_signal", reason, True)], issues),
                          market)

    async def _portfolio_issues(
        self, plan: TradePlan, chat_id: int, *, exclude_conf: str | None = None
    ) -> list[ValidationIssue]:
        """Everything that makes a NEW trade doubtful beyond the signal itself: existing trades
        (duplicate / opposite / same message), pending confirmations of the same messages or a similar
        signal, and earlier confirmation outcomes of its messages."""
        return merge_issues(
            await self._trade_issues(plan, chat_id),
            await self._pending_issues(plan, chat_id, exclude_conf),
            await self._outcome_issues(chat_id, plan.source_message_ids, exclude_conf),
        )

    async def _trade_issues(self, plan: TradePlan, chat_id: int) -> list[ValidationIssue]:
        """Conflicts with trades that exist (re-checked at approval time). With forum routes a repost is
        looked for among the same route's trades only; an opposite trade counts when it is on the same
        instrument (:meth:`_same_instrument`: USDJPY long vs USDCAD short is no conflict) and is of the same
        route or may sit on one of the same broker accounts (one-way netting)."""
        now = self._now()
        risk = self.config.risk
        route = self._route_named(plan.route)
        out: list[ValidationIssue] = []
        recent = [t for t in await self.db.recent_trades(now - timedelta(seconds=max(0, risk.duplicate_window_sec)))
                  if self._in_scope(t, route)]
        dup = guards.find_duplicate(plan, recent, window_sec=risk.duplicate_window_sec, now=now,
                                    entry_tolerance_pct=route_thresholds(risk, route)[2])
        if dup is not None:
            out.append(_issue(
                "duplicate_signal",
                f"Benzer bir işlem zaten var (#{dup.id} {dup.plan.base_asset} {dup.plan.side.value.upper()}, "
                f"giriş {fmt_price(dup.plan.entry_price)}); sinyal tekrar paylaşılmış olabilir.",
            ))
        opposite = [t for t in await self.db.active_trades(plan.base_asset)
                    if t.plan.side is plan.side.opposite and self._same_instrument(t.plan, plan)
                    and (self._in_scope(t, route) or self._shares_broker(t.plan, plan))]
        if opposite and self._all_no_hedge(plan):
            opposite = []       # every target broker refuses hedging itself (skipped per broker, no question)
        if opposite:
            ids = ", ".join(f"#{t.id}" for t in opposite)
            out.append(_issue(
                "opposite_trade_open",
                f"{plan.base_asset} için ters yönde açık işlem var ({ids}); tek yönlü (one-way) hesaplarda "
                "iki pozisyon birbirini kapatır.",
            ))
        for mid in plan.source_message_ids:
            t = await self.db.find_trade_by_message(chat_id, mid)
            if t is not None and t.status not in (TradeStatus.FAILED, TradeStatus.CANCELLED):
                out.append(_issue("message_already_traded", f"Bu mesajdan zaten işlem açılmış (#{t.id})."))
                break
        return out

    async def _pending_issues(
        self, plan: TradePlan | None, chat_id: int, exclude_conf: str | None,
        ids: list[int] | None = None,
    ) -> list[ValidationIssue]:
        """A pending confirmation built on the same messages, or (``plan``) on a similar signal (a repost):
        executing now and approving that one later would open the same signal twice."""
        risk = self.config.risk
        ids = list(plan.source_message_ids) if ids is None and plan is not None else list(ids or [])
        out: list[ValidationIssue] = []
        similar: list[TradeRecord] = []
        by_plan: dict[str, PendingConfirmation] = {}
        for conf in await self.db.pending_confirmations():
            if conf.id == exclude_conf or conf.source_chat_id != chat_id:
                continue
            shared = sorted(set(ids) & set(conf.source_message_ids))
            if shared:
                out.append(_issue(
                    "pending_confirmation",
                    f"Bu mesaj için bekleyen bir onay var (#{conf.id}); ikisi de uygulanırsa aynı talimat iki kez "
                    "işlenir.",
                ))
                continue
            if plan is not None and isinstance(conf.plan, TradePlan):
                if plan.route is not None and conf.plan.route != plan.route:
                    continue    # another forum topic's signal is not a repost of this one
                by_plan[conf.plan.plan_id] = conf
                similar.append(TradeRecord(
                    id=0, plan=conf.plan, status=TradeStatus.OPENING, current_stop_loss=conf.plan.stop_loss,
                    current_take_profits=list(conf.plan.take_profits), created_at=conf.created_at,
                    updated_at=conf.created_at,
                ))
        if plan is not None and similar:
            dup = guards.find_duplicate(
                plan, similar, window_sec=risk.duplicate_window_sec, now=self._now(),
                entry_tolerance_pct=route_thresholds(risk, self._route_named(plan.route))[2],
            )
            if dup is not None:
                conf = by_plan[dup.plan.plan_id]
                out.append(_issue(
                    "duplicate_pending_signal",
                    f"Benzer bir sinyal için bekleyen bir onay var (#{conf.id} {dup.plan.base_asset} "
                    f"{dup.plan.side.value.upper()}, giriş {fmt_price(dup.plan.entry_price)}); sinyal tekrar "
                    "paylaşılmış olabilir.",
                ))
        return out

    # ------------------------------------------------------------------ confirmation outcomes per message
    @staticmethod
    def _outcome_key(chat_id: int) -> str:
        return f"msg_outcomes:{chat_id}"

    async def _remember_outcome(self, chat_id: int, ids: list[int], status: str, conf_id: str | None,
                                reason: str | None = None) -> None:
        """Remember how a confirmation (or a given-up parse) about these source messages ended.
        ``status``: a ConfirmationStatus value, "parse_failed", or "auto_refused" (approved by the owner,
        but the bot itself refused to execute it: ``reason``)."""
        ids = [int(i) for i in dict.fromkeys(ids) if i]
        if not ids:
            return
        now = self._now()
        record: dict[str, Any] = {"status": status, "conf": conf_id, "at": now.isoformat()}
        if reason:
            record["reason"] = reason
        async with self._outcome_lock:
            try:
                data = await self.db.get_state(self._outcome_key(chat_id), {}) or {}
                if not isinstance(data, dict):
                    data = {}
                for mid in ids:
                    data[str(mid)] = dict(record)
                await self.db.set_state(self._outcome_key(chat_id), self._prune_by_age(data, now))
            except Exception:
                logger.exception("Could not remember the outcome of messages %s", ids)

    @staticmethod
    def _prune_by_age(data: dict[str, Any], now: datetime) -> dict[str, Any]:
        """Entries newer than MSG_OUTCOME_TTL_DAYS, at most MSG_OUTCOME_MAX (newest kept). An entry is a
        record ``{"at": ...}`` or a list of them (its newest one counts)."""
        def at(v: Any) -> str:
            if isinstance(v, list):
                return max((str(x.get("at", "")) for x in v if isinstance(x, dict)), default="")
            return str(v.get("at", "")) if isinstance(v, dict) else ""

        cutoff = (now - timedelta(days=MSG_OUTCOME_TTL_DAYS)).isoformat()
        kept = sorted(((k, v) for k, v in data.items() if at(v) >= cutoff), key=lambda kv: at(kv[1]))
        return dict(kept[-MSG_OUTCOME_MAX:])

    async def _record_confirmation_outcome(
        self, conf: PendingConfirmation, outcome: tuple[str, str] | None = None
    ) -> None:
        """``outcome``: (status, reason) overriding ``conf.status`` (e.g. ("auto_refused", why))."""
        if conf.status in (ConfirmationStatus.PENDING, ConfirmationStatus.SUPERSEDED):
            return      # a superseding confirmation carries the same messages
        if conf.action.action is ActionType.NONE:
            return      # "is this a trade at all?" - a later explicit instruction is new information
        ids = list(conf.source_message_ids)
        if conf.kind == "manage":
            if conf.status is ConfirmationStatus.APPROVED:
                return  # applied: remembered per message by _remember_applied (like an automatic change)
            # The managed trade's own signal messages are not "decided" by a management question.
            trade_id = conf.plan.trade_id if isinstance(conf.plan, ManagementPlan) else conf.action.target_trade_id
            trade = await self.db.get_trade(trade_id) if trade_id is not None else None
            if trade is not None:
                ids = [i for i in ids if i not in trade.plan.source_message_ids]
        status, reason = outcome if outcome is not None else (conf.status.value, None)
        await self._remember_outcome(conf.source_chat_id, ids, status, conf.id, reason)

    # ------------------------------------------------------------------ management actions applied per message
    @staticmethod
    def _applied_key(chat_id: int) -> str:
        return f"msg_applied:{chat_id}"

    async def _remember_applied(self, plan: ManagementPlan, trade: TradeRecord) -> None:
        """Remember which management instruction each source message ran (automatically or approved), so a
        re-read of that message (an edit, or a message an incomplete reading kept unconsumed) is never
        applied a second time automatically. The trade's own signal messages are left out."""
        ids = [int(i) for i in dict.fromkeys(plan.source_message_ids)
               if i and i not in trade.plan.source_message_ids]
        if not ids:
            return
        now = self._now()
        record: dict[str, Any] = {
            "plan_id": plan.plan_id, "action": plan.action.value, "trade_id": trade.id,
            "asset": trade.plan.base_asset, "side": trade.plan.side.value,
            "new_stop_loss": plan.new_stop_loss, "move_sl_to_entry": plan.move_sl_to_entry,
            "take_profits": [[tp.price, tp.close_fraction] for tp in plan.take_profits],
            "close_fraction": plan.close_fraction, "at": now.isoformat(),
        }
        chat_id = plan.source_chat_id
        async with self._outcome_lock:
            try:
                data = await self.db.get_state(self._applied_key(chat_id), {}) or {}
                if not isinstance(data, dict):
                    data = {}
                for mid in ids:
                    old = data.get(str(mid))
                    records = [r for r in old if isinstance(r, dict) and r.get("plan_id") != plan.plan_id] \
                        if isinstance(old, list) else []
                    data[str(mid)] = [*records, dict(record)][-5:]
                await self.db.set_state(self._applied_key(chat_id), self._prune_by_age(data, now))
            except Exception:
                logger.exception("Could not remember the management applied for messages %s", ids)

    async def _applied_records(self, chat_id: int, ids: list[int]) -> list[dict[str, Any]]:
        """Management instructions already applied from any of ``ids`` (oldest first)."""
        if not ids:
            return []
        try:
            data = await self.db.get_state(self._applied_key(chat_id), {}) or {}
        except Exception:
            logger.exception("Could not read applied management records")
            return []
        if not isinstance(data, dict):
            return []
        out: list[dict[str, Any]] = []
        for mid in dict.fromkeys(ids):
            recs = data.get(str(mid))
            for r in recs if isinstance(recs, list) else []:
                if isinstance(r, dict) and all(r.get("plan_id") != o.get("plan_id") for o in out):
                    out.append(r)
        return sorted(out, key=lambda r: str(r.get("at", "")))

    @staticmethod
    def _applied_text(rec: dict[str, Any]) -> str:
        """Plain text (escape before use): what an applied management record did."""
        label = f"#{rec.get('trade_id')} {rec.get('asset', '?')} {str(rec.get('side', '')).upper()}".strip()
        kind = rec.get("action")
        if kind == ActionType.MOVE_SL.value:
            if rec.get("move_sl_to_entry"):
                what = "stop girişe çekildi"
            else:
                sl = rec.get("new_stop_loss")
                what = f"stop {fmt_price(float(sl)) if isinstance(sl, (int, float)) else '?'} yapıldı"
        elif kind == ActionType.MOVE_TP.value:
            prices = [fmt_price(float(tp[0])) for tp in rec.get("take_profits") or []
                      if isinstance(tp, (list, tuple)) and tp]
            what = "hedefler " + (" / ".join(prices) or "?") + " yapıldı"
        elif kind == ActionType.CLOSE.value:
            frac = rec.get("close_fraction")
            what = ("pozisyonun tamamı kapatıldı" if not isinstance(frac, (int, float)) or frac >= 0.999
                    else f"pozisyonun {fmt.fmt_pct(float(frac) * 100, possessive=True)} kapatıldı")
        else:
            what = "işlem iptal edildi"
        return f"{label}: {what}"

    @staticmethod
    def _same_as_applied(rec: dict[str, Any], action: ParsedAction, plan: TradePlan | ManagementPlan | None) -> bool:
        """Does ``action`` (with its evaluated ``plan``) say the same thing as the applied record?"""
        if action.action.value != rec.get("action"):
            return False
        mplan = plan if isinstance(plan, ManagementPlan) else None
        if mplan is not None:
            if mplan.trade_id != rec.get("trade_id"):
                return False
        elif action.target_trade_id is not None:
            if action.target_trade_id != rec.get("trade_id"):
                return False
        else:
            base, _q, _t = managed_asset(action)
            if base and base.upper() != str(rec.get("asset", "")).upper():
                return False
            if action.side is not None and action.side.value != rec.get("side"):
                return False
        if action.action is ActionType.MOVE_SL:
            if action.move_sl_to_entry or rec.get("move_sl_to_entry"):
                return bool(action.move_sl_to_entry) == bool(rec.get("move_sl_to_entry"))
            old = rec.get("new_stop_loss")
            return (action.new_stop_loss is not None and isinstance(old, (int, float))
                    and _same_price(float(action.new_stop_loss), float(old)))
        if action.action is ActionType.MOVE_TP:
            old_tps = [(float(tp[0]), float(tp[1])) for tp in rec.get("take_profits") or []
                       if isinstance(tp, (list, tuple)) and len(tp) == 2]
            if mplan is not None:
                new_tps = [(tp.price, tp.close_fraction) for tp in mplan.take_profits]
                return len(new_tps) == len(old_tps) and all(
                    _same_price(a[0], b[0]) and abs(a[1] - b[1]) <= 0.005 for a, b in zip(new_tps, old_tps))
            return bool(action.take_profits) and all(
                any(_same_price(tp.price, p) for p, _f in old_tps) for tp in action.take_profits)
        if action.action is ActionType.CLOSE:
            new_frac = action.close_fraction if action.close_fraction is not None else 1.0
            old_frac = rec.get("close_fraction")
            old_frac = float(old_frac) if isinstance(old_frac, (int, float)) else 1.0
            return abs(new_frac - old_frac) <= 1e-6
        return True     # CANCEL

    async def _applied_issues(
        self, chat_id: int, ids: list[int], action: ParsedAction, plan: ManagementPlan | None
    ) -> list[ValidationIssue]:
        """The SAME management instruction built again on a message that already ran it (e.g. a message an
        incomplete reading kept unconsumed is read again) is asked, never applied twice. A different
        instruction that only refers to such a message (a reply "kalanı da kapattım") stays automatic."""
        recs = [r for r in await self._applied_records(chat_id, ids) if self._same_as_applied(r, action, plan)]
        if not recs:
            return []
        done = "; ".join(self._applied_text(r) for r in recs)
        return [_issue("message_already_applied",
                       f"Bu mesajdaki talimat zaten uygulandı ({done}); aynı talimat ikinci kez uygulanmak "
                       "üzere olabilir.")]

    async def _outcome_issues(
        self, chat_id: int, ids: list[int], exclude_conf: str | None
    ) -> list[ValidationIssue]:
        """An action built on a message whose earlier confirmation was rejected / expired / approved (or
        whose parse was given up) is asked again, never executed automatically."""
        if not ids:
            return []
        try:
            data = await self.db.get_state(self._outcome_key(chat_id), {}) or {}
        except Exception:
            logger.exception("Could not read message outcomes")
            return []
        if not isinstance(data, dict):
            return []
        out: list[ValidationIssue] = []
        for mid in dict.fromkeys(ids):
            rec = data.get(str(mid))
            if not isinstance(rec, dict) or (exclude_conf is not None and rec.get("conf") == exclude_conf):
                continue
            conf = rec.get("conf")
            status = rec.get("status")
            ref = f" (onay #{esc(conf)})" if conf else ""
            text = {
                "rejected": f"Bu mesajdaki talimat için daha önce onay sorulmuş ve reddedilmişti{ref}.",
                "auto_refused": (f"Bu mesajdaki talimat için daha önce onay verilmiş ama bot uygulamamıştı "
                                 f"({rec.get('reason') or 'o sırada uygulanamadı'}){ref}."),
                "expired": f"Bu mesajdaki talimat için daha önce onay sorulmuş, yanıt gelmeden süresi dolmuştu{ref}.",
                "approved": f"Bu mesajdaki talimat daha önce onaylanıp uygulanmıştı{ref}; aynı talimat ikinci kez "
                            "uygulanmak üzere olabilir.",
                "parse_failed": "Bu mesaj daha önce okunamamış ve otomatik işlenmemek üzere bırakılmıştı.",
            }.get(str(status))
            if text:
                out.append(_issue("message_previously_decided", text))
        return out

    async def _resolve_trade(
        self, action: ParsedAction, chat_id: int, reply_to: StoredMessage | None,
        *, route: TopicRoute | None = None,
    ) -> tuple[TradeRecord | None, list[TradeRecord], list[ValidationIssue]]:
        """target_trade_id > refers_to message > replied-to message > the single active trade of the asset.
        ``route``: only that forum topic route's trades are considered (None: every trade).

        "Of the asset": the same base asset and, when the instruction names a quote currency (or a symbol that
        normalises to base + quote) and the trade is a forex / CFD pair, also the same quote
        (:meth:`_other_pair`). "gbpjpy kapattık" never picks an open GBPUSD: with no GBPJPY trade it is handled
        like an instruction for an asset without a trade. A trade found through an explicit reference (id,
        reply) that is another pair is asked about (asset_mismatch), never managed silently.

        A message the instruction explicitly refers to (Telegram reply, else refers_to_message_id) that
        belongs to a trade which is already over (closed / cancelled / failed) wins over a different
        target_trade_id: the instruction is about that finished trade (and is then ignored), never about
        another open one. A different ACTIVE trade behind the reply is a conflict: asked.

        A real Telegram reply to a message that is not in storage at all (a signal posted before the bot
        started; ``reply_to`` is None for it): neither the engine nor the model has seen what was replied to,
        so whatever active trade is found for it (by asset, as the only open trade, or named by the model) is a
        guess: asked (``reply_unlinked``), never applied on its own. Not when the instruction's own message
        belongs to that trade (an edited signal)."""
        active = [t for t in await self.db.active_trades() if self._in_scope(t, route)]
        base, quote, _type = managed_asset(action)
        extra: list[ValidationIssue] = []
        unstored_reply = (await self._unstored_reply_id(chat_id, action.used_message_ids)
                          if reply_to is None else None)

        async def by_message(mid: int, hops: int = 3) -> TradeRecord | None:
            found = await self.db.find_trade_by_message(chat_id, mid)
            if found is None and hops > 0:
                # A reply to a reply ("2R kapandı" -> "entry stop attık" / "Aktif" -> the signal): follow
                # the thread up to the signal message.
                parent = await self.db.get_message(chat_id, mid)
                up = parent.reply_to_message_id if parent is not None else None
                if parent is not None and up is not None and up != parent.topic_id:
                    return await by_message(up, hops - 1)
            return found if found is not None and self._in_scope(found, route) else None

        referenced: TradeRecord | None = None
        if reply_to is not None:
            referenced = await by_message(reply_to.message_id)
        if referenced is None and action.refers_to_message_id is not None:
            referenced = await by_message(action.refers_to_message_id)
        trade: TradeRecord | None = None
        if action.target_trade_id is not None:
            trade = await self.db.get_trade(action.target_trade_id)
            if trade is not None and not self._in_scope(trade, route):
                logger.warning("Trade #%s is not of route %s; not managed from this topic",
                               trade.id, route.name if route else None)
                trade = None
        if trade is None and action.refers_to_message_id is not None:
            trade = await by_message(action.refers_to_message_id)
        if trade is None and reply_to is not None:
            trade = await by_message(reply_to.message_id)
        if referenced is not None and trade is not None and referenced.id != trade.id:
            if referenced.status in _DONE_TRADE:
                trade = referenced
            elif reply_to is not None:
                extra.append(_issue(
                    "reply_trade_mismatch",
                    f"Mesaj #{referenced.id} {referenced.plan.base_asset} işleminin sinyaline yanıt, ama talimat "
                    f"#{trade.id} {trade.plan.base_asset} işlemi için okundu.",
                ))
        candidates = [t for t in active if base is None
                      or (t.plan.base_asset.upper() == base.upper() and not self._other_pair(t.plan, quote))]
        if trade is None:
            by_side = [t for t in candidates if action.side is None or t.plan.side is action.side]
            if base is not None and len(by_side) == 1:
                trade = by_side[0]
                if reply_to is not None:
                    # The message replies to something no trade was copied from (a signal we never took?):
                    # the only trade of that asset may be a DIFFERENT trade. Asked, never applied silently.
                    extra.append(_issue(
                        "reply_unlinked",
                        f"Mesaj #{reply_to.message_id} mesajına yanıt, ama o mesajdan kopyalanmış bir işlem yok; "
                        f"aynı varlıktaki #{trade.id} {trade.plan.base_asset} işlemi seçildi.",
                    ))
            elif base is None and len(active) == 1:
                trade = active[0]
                extra.append(_issue(
                    "trade_guessed",
                    f"Varlık belirtilmedi; tek açık işlem seçildi (#{trade.id} {trade.plan.base_asset}).",
                ))
            elif len(by_side) > 1:
                candidates = by_side
        if (trade is None and base is not None and not candidates
                and action.action in (ActionType.CLOSE, ActionType.CANCEL)):
            # "BTC kapandı" while no BTC copy is active: if this topic's latest copy of that asset is
            # already over, the message is about that one -> ignored like a reply to it, never asked.
            done = [t for t in await self.db.recent_trades(self._now() - timedelta(days=7))
                    if self._in_scope(t, route) and t.status in _DONE_TRADE
                    and t.plan.base_asset.upper() == base.upper() and not self._other_pair(t.plan, quote)
                    and (action.side is None or t.plan.side is action.side)]
            if done:
                trade = done[0]
        if trade is not None:
            if base is not None and trade.plan.base_asset.upper() != base.upper():
                extra.append(_issue(
                    "asset_mismatch",
                    f"Talimattaki varlık ({base}) ile eşleşen işlemin varlığı ({trade.plan.base_asset}) farklı.",
                ))
            elif base is not None and self._other_pair(trade.plan, quote):
                extra.append(_issue(
                    "asset_mismatch",
                    f"Talimattaki parite ({base}{(quote or '').upper()}) ile eşleşen işlemin paritesi "
                    f"({instrument_label(trade.plan).upper()}) farklı.",
                ))
            if action.side is not None and trade.plan.side is not action.side:
                extra.append(_issue(
                    "side_mismatch",
                    f"Talimattaki yön ({action.side.value}) ile işlemin yönü ({trade.plan.side.value}) farklı.",
                ))
            if (unstored_reply is not None and trade.status not in _DONE_TRADE
                    and not set(action.used_message_ids) & set(trade.plan.source_message_ids)):
                extra.append(_issue(
                    "reply_unlinked",
                    f"Bu mesaj, botun kaydında olmayan bir mesaja (#{unstored_reply}) yanıt olarak yazılmış; bot "
                    "başlamadan önce paylaşılmış ya da kopyalanmamış bir sinyalle ilgili olabilir. Yanıtlanan mesaj "
                    f"bilinmediği için #{trade.id} {trade.plan.base_asset} işlemi tahminle seçildi; bu mesaj o "
                    "işlemle ilgili değilse onaylamayın.",
                ))
        return trade, candidates, extra

    async def _evaluate_management(
        self, action: ParsedAction, chat_id: int, reply_to: StoredMessage | None, *,
        exclude_conf: str | None = None, route: TopicRoute | None = None,
    ) -> Evaluation:
        trade, candidates, extra = await self._resolve_trade(action, chat_id, reply_to, route=route)
        market = None
        filled: list[float] = []
        if trade is not None:
            market = await self._price(trade.plan.base_asset, trade.plan.quote_asset, trade.plan.market_type)
            if action.action is ActionType.MOVE_TP:
                try:
                    filled = await self._filled_tp_levels(trade)
                except Exception:
                    logger.exception("Filled TP levels of trade %s", trade.id)
        plan, issues = validator.build_management_plan(
            action, trade=trade, candidates=candidates, market_price=market,
            source_chat_id=chat_id, now=self._now(), filled_tp_prices=filled,
        )
        # Messages other than the managed trade's own signal: earlier questions about them still count.
        own = set(trade.plan.source_message_ids) if trade is not None else set()
        ids = [i for i in action.used_message_ids if i not in own]
        issues = merge_issues(
            extra, issues, validator.low_confidence_issues(action, self.config.llm),
            await self._pending_issues(None, chat_id, exclude_conf, ids),
            await self._outcome_issues(chat_id, ids, exclude_conf),
            await self._applied_issues(chat_id, ids, action, plan),
        )
        ignored: str | None = None
        if trade is not None and trade.status in _DONE_TRADE:
            # e.g. our stop already hit, then the trader replies "Stop" / "kapattık": nothing left to do.
            ignored = (f"#{trade.id} {trade.plan.base_asset} {trade.plan.side.value.upper()} işlemi "
                       f"{_DONE_TRADE_TR[trade.status]}")
        return Evaluation("manage", action, plan, issues, market, trade, ignored=ignored)

    # ------------------------------------------------------------------ open
    async def _handle_open(
        self, action: ParsedAction, chat_id: int, *, reply_to: StoredMessage | None = None,
        route: TopicRoute | None = None,
    ) -> None:
        async with self._holding_open_lock(action.summary_tr or "yeni sinyal"):
            ev = await self._evaluate_open(action, chat_id, route=route)
            summary = (ev.plan.summary_tr if isinstance(ev.plan, TradePlan) else "") or action.summary_tr or "?"
            if await self._is_paused():
                await self._notify(f"Duraklatıldı: sinyal atlandı ({esc(summary)})", Severity.INFO)
                await self._log("open_skipped_paused", {"chat_id": chat_id, "action": action.model_dump(mode="json")})
                return
            limit = self.config.risk.max_open_trades
            active = await self.db.active_trades()
            if guards.max_open_trades_reached(active, limit):
                await self._notify(
                    f"Açık işlem sınırına ({limit}) ulaşıldı; sinyal atlandı ({esc(summary)}).\n"
                    f"{self._limit_filled_by(active)}", Severity.WARNING
                )
                await self._log("open_skipped_max_open", {"chat_id": chat_id, "summary": summary,
                                                          "active": [t.id for t in active]})
                return
            if not ev.automatic:
                await self._ask_eval(ev, chat_id)
                return
            assert isinstance(ev.plan, TradePlan)
            await self.execute_open(ev.plan, raw_symbol=action.symbol)

    @staticmethod
    def _limit_filled_by(active: list[TradeRecord]) -> str:
        """HTML: the active trades that fill ``risk.max_open_trades`` (ids and instruments), so the owner can act.
        Every active trade of every topic counts, waiting entries included."""
        items = [
            f"#{t.id} {esc(instrument_label(t.plan).upper())} {t.plan.side.value.upper()} "
            f"({_ACTIVE_TRADE_TR.get(t.status, t.status.value)}" + (f" · {esc(t.plan.route)}" if t.plan.route else "") + ")"
            for t in sorted(active, key=lambda t: t.id) if t.status in ACTIVE_TRADE
        ]
        return ("Sınırı dolduran işlemler: " + ", ".join(items) + ". Yer açmak için bunlardan birini kapatın veya "
                "iptal edin (bekleyen girişler de sayılır); sınır config.yaml'daki risk.max_open_trades değeridir.")

    async def execute_open(self, plan: TradePlan, *, raw_symbol: str | None = None) -> str:
        """Open ``plan`` on every broker that can trade it (only ``plan.broker_names`` when it names any:
        a forum topic's brokers). Returns an HTML result text.
        Runs as a critical section: never interrupted half-way by a cancelled caller or shutdown.
        It returns at the latest LEG_DEADLINE_SEC after the orders went out: a broker that has not answered by
        then is reported as late and its result follows in a separate message (see :meth:`_late_leg_done`)."""
        return await self._critical_section(self._execute_open(plan, raw_symbol), f"open-{plan.plan_id}")

    async def _execute_open(self, plan: TradePlan, raw_symbol: str | None) -> str:
        if self.config.telegram.topics and not plan.route:
            # Topics route every trade to specific brokers; without a route we would open everywhere.
            text = (f"🚨 <b>{esc(plan.base_asset)} {plan.side.value.upper()}</b>: sinyalin konusu (topic) "
                    "belirlenemediği için işlem AÇILMADI (yanlış brokerda açılmasın diye).")
            await self._notify(text, Severity.ERROR)
            await self._log("open_refused_no_route", {"plan": plan.model_dump(mode="json")})
            return text
        trade = await self.db.create_trade(plan)
        if plan.broker_names and not self._plan_brokers(plan):
            async with self.trade_lock(trade.id):
                return await self._no_route_broker(trade)
        await self._optional_skip_notice(plan)
        label = f"#{trade.id} {esc(plan.base_asset)} {plan.side.value.upper()}"
        late: list[_LateLeg] = []
        async with self.trade_lock(trade.id):
            await self._log("open_start", {"trade_id": trade.id, "plan": plan.model_dump(mode="json")})
            routed = self._plan_brokers(plan)
            brokers = [b for b in routed if b.accepts_asset(plan.base_asset)]
            flow = _OpenFlow()
            answered, waiting = await self._run_open_legs(brokers, plan, trade, raw_symbol, flow)
            # In route order. A broker of the route whose allowed_assets does not list the asset is named too.
            outcomes: list[BrokerOutcome] = [
                answered[b.name] if b.name in answered else BrokerOutcome(
                    b.name, "filtered", f"{esc(b.name)}: {esc(plan.base_asset)} bu hesabın listesinde yok, atlandı",
                    benign=True)
                for b in routed
            ]
            positions = [o.position for o in outcomes if o.position is not None]
            status = rollup_status(positions)
            if waiting and status not in ACTIVE_TRADE:
                # Nothing is open yet but a broker has not answered: the trade is still being sent. Its late
                # leg decides (opened -> the trade is rolled up again; not opened -> FAILED then).
                status = TradeStatus.OPENING
            notes = "; ".join(f"{o.broker}: {o.kind}" for o in outcomes if o.kind != "ok") or None
            trade = await self.db.update_trade(trade.id, status=status, note=notes)
            late = [self._watch_late_leg(trade, plan, name, task, f"#{trade.id} {plan.base_asset} "
                                         f"{plan.side.value.upper()}") for name, task in waiting.items()]
            if late:
                await self._late_state(add=late)
            flow.released = True        # from here on a late leg takes the trade lock itself to write its position

        try:
            lines = [f"• {o.text}" for o in outcomes]
            if not outcomes or all(o.kind in ("unsupported", "filtered") for o in outcomes):
                head = f"⚠️ <b>{label}: hiçbir broker bu varlığı işlemiyor</b>; sinyal kopyalanamadı."
                named = "\n" + "\n".join(lines) if any(o.kind == "filtered" for o in outcomes) else ""
                await self._notify(f"{head}\n{esc(plan.summary_tr)}{named}", Severity.WARNING)
            elif status is TradeStatus.OPENING:
                head = f"⏳ <b>{label}: henüz hiçbir brokerda açılmadı</b>; yanıt vermeyen broker bekleniyor."
                await self._notify(head + "\n" + "\n".join(lines), Severity.WARNING)
            elif status is TradeStatus.FAILED and all(o.benign for o in outcomes):
                head = f"ℹ️ <b>{label} açılmadı</b> (hedging yasağı)"
                await self._notify(head + "\n" + "\n".join(lines), Severity.WARNING)
            elif status is TradeStatus.FAILED:
                head = f"🚨 <b>{label} hiçbir brokerda açılamadı</b>"
                await self._notify(head + "\n" + "\n".join(lines), Severity.ERROR)
            else:
                if status is TradeStatus.PENDING:
                    word = ("sanal tetik kuruldu" if any(is_virtual_pending(p) for p in positions)
                            else "limit emri yerleştirildi")
                else:
                    word = "açıldı"
                icon = "⚠️" if any(o.kind in ("partial", "error", "late") for o in outcomes) else "✅"
                head = f"{icon} <b>{label} {word}</b>"
                if self.config.dry_run:
                    head += " <i>(dry-run: gerçek emir yok)</i>"
                # A broker of the route that did not take the trade (or has not answered yet) is always reported,
                # whatever notify_on_execution says: "all opened" and "one was left out" must never look the same.
                if self.config.notifier.notify_on_execution or any(o.kind in _NOT_OPENED for o in outcomes):
                    await self._notify(f"{head}\n{fmt.format_plan(plan)}\n\n" + "\n".join(lines), Severity.INFO)
                elif any(o.size_capped for o in outcomes):
                    # Execution reports are off; a size the bot limited (less than 1R at risk) is told all the same.
                    dry = " <i>(dry-run: gerçek emir yok)</i>" if self.config.dry_run else ""
                    await self._notify(
                        f"⚠️ <b>{label} {word}</b>{dry}: miktar sınırlandı, gerçek risk hedefin altında\n"
                        + "\n".join(f"• {o.text}" for o in outcomes if o.size_capped), Severity.WARNING)
            await self._log("open_done", {
                "trade_id": trade.id, "status": status.value,
                "outcomes": [{"broker": o.broker, "kind": o.kind, "text": o.text} for o in outcomes],
            })
        finally:
            for leg in late:
                leg.announced.set()     # the late results may be reported now (always after this report)
        return "\n".join([head, *lines])

    # ------------------------------------------------------------------ broker legs of an open flow
    async def _run_open_legs(
        self, brokers: list[Broker], plan: TradePlan, trade: TradeRecord, raw_symbol: str | None, flow: _OpenFlow
    ) -> tuple[dict[str, BrokerOutcome], dict[str, asyncio.Task[BrokerOutcome]]]:
        """Send ``plan`` to every broker at once, each in its own task, and wait until all have answered, but
        never longer than LEG_DEADLINE_SEC. Returns (outcome per broker, still running leg task per broker):
        a broker that has not answered gets the "late" outcome and its task keeps running (never cancelled:
        its order may already be at the broker)."""
        if not brokers:
            return {}, {}
        tasks: dict[str, asyncio.Task[BrokerOutcome]] = {}
        for b in brokers:
            task = asyncio.ensure_future(self._open_leg(b, plan, trade, raw_symbol, flow))
            task.set_name(f"open-{plan.plan_id}-{b.name}")
            tasks[b.name] = task
        await asyncio.wait(tasks.values(), timeout=LEG_DEADLINE_SEC)
        outcomes: dict[str, BrokerOutcome] = {}
        waiting: dict[str, asyncio.Task[BrokerOutcome]] = {}
        for name, task in tasks.items():
            if task.done():
                outcomes[name] = self._leg_outcome(name, task)
            else:
                waiting[name] = task
                outcomes[name] = BrokerOutcome(
                    name, "late", f"⏳ {esc(name)}: yanıt bekleniyor; sonuç ayrıca bildirilecek")
        return outcomes, waiting

    async def _open_leg(
        self, broker: Broker, plan: TradePlan, trade: TradeRecord, raw_symbol: str | None, flow: _OpenFlow
    ) -> BrokerOutcome:
        """One broker's part of an open flow. While the broker still has an unanswered (late) leg of an earlier
        signal in flight, or a virtual entry of its account is being sent, no second open is started on it
        (hedge / daily-loss / margin checks would race): this leg waits for the earlier one for most of the
        deadline and otherwise gives up with an error outcome."""
        name = broker.name
        loop = asyncio.get_running_loop()
        give_up_at = loop.time() + LEG_DEADLINE_SEC * BUSY_WAIT_SHARE
        while True:     # (again after each wait: another open of this account may have started meanwhile)
            previous = self._leg_running.get(name)
            if previous is None or previous.done():
                break
            left = give_up_at - loop.time()
            if left > 0:
                await asyncio.wait({previous}, timeout=left)
            if not previous.done():
                text = f"{esc(name)} önceki emrine hâlâ yanıt vermedi; bu işlem {esc(name)}'da açılmadı"
                await self._notify(text, Severity.ERROR)
                return BrokerOutcome(name, "error", text)
        me = asyncio.current_task()
        if me is not None:
            self._leg_running[name] = me
        try:
            return await self._open_on_broker(broker, plan, trade, raw_symbol, flow=flow)
        finally:
            if self._leg_running.get(name) is me:
                del self._leg_running[name]

    @staticmethod
    def _leg_outcome(name: str, task: asyncio.Task[BrokerOutcome]) -> BrokerOutcome:
        """Outcome of a finished leg task. :meth:`_open_on_broker` turns every failure into an outcome itself;
        a leg task that died all the same (cancelled from outside, a bug) still becomes an error outcome."""
        if task.cancelled():
            logger.error("Open leg %s was cancelled", task.get_name())
            return BrokerOutcome(name, "error", f"{esc(name)}: işlem yarıda kesildi; emir gönderilmiş olabilir, "
                                 "brokerda elle kontrol edin", sent=True)
        exc = task.exception()
        if exc is not None:
            logger.error("Open leg %s failed", task.get_name(), exc_info=exc)
            return BrokerOutcome(name, "error", f"{esc(name)}: beklenmeyen hata <code>{esc(exc)}</code>")
        return task.result()

    def _watch_late_leg(
        self, trade: TradeRecord, plan: TradePlan, name: str, task: asyncio.Task[BrokerOutcome], label: str
    ) -> _LateLeg:
        """Register a leg that missed the deadline and start the background task that books it when it answers.
        That task counts as a running order flow: shutdown waits for it like for any critical section."""
        leg = _LateLeg(trade.id, name, label, task)
        self._late_legs[(trade.id, name)] = leg
        logger.warning("Open %s: %s has not answered within %.0fs; the flow finishes without it and its leg "
                       "continues in the background", plan.plan_id, name, LEG_DEADLINE_SEC)
        watcher = asyncio.ensure_future(self._late_leg_done(leg, plan))
        watcher.set_name(f"late-{plan.plan_id}-{name}")
        self._critical.add(watcher)
        watcher.add_done_callback(self._critical_done)
        watcher.add_done_callback(self._orphan_done)
        return leg

    async def _late_leg_done(self, leg: _LateLeg, plan: TradePlan) -> None:
        """Wait (in the background, without holding any lock) until a late leg has answered, then book its
        result under the trade lock and tell the owner: always, whatever notify_on_execution says."""
        name = leg.broker
        label = esc(leg.label)
        try:
            await asyncio.wait({leg.task})          # never cancelled: its order may already be at the broker
            await leg.announced.wait()
            outcome = self._leg_outcome(name, leg.task)
            async with self.trade_lock(leg.trade_id):
                text, severity = await self._book_late_leg(leg, plan, outcome)
        except Exception as exc:
            logger.exception("Late leg %s of trade #%s could not be booked", name, leg.trade_id)
            text, severity = (
                f"🚨 <b>{label}</b> — {esc(name)} geç yanıt verdi ama sonucu işlenirken hata oluştu: "
                f"<code>{esc(exc)}</code>\n{esc(name)} hesabında bu işlemin emrini / pozisyonunu elle kontrol edin.",
                Severity.ERROR,
            )
        finally:
            self._late_legs.pop((leg.trade_id, name), None)
        if not self._brokers_closed:
            await self._late_state(remove=[leg])
        # else: shutdown gave up waiting and closed the brokers under this leg; whatever it returned then says
        # nothing about its order, and this message may no longer reach the owner. The stored note stays, so
        # the restart tells the owner to check that account (see _report_interrupted_legs).
        await self._notify(text, severity)

    async def _book_late_leg(self, leg: _LateLeg, plan: TradePlan, outcome: BrokerOutcome) -> tuple[str, Severity]:
        """Trade lock held. Roll the trade up again with the late leg's position. If the trade has ended in the
        meantime (closed / cancelled, or the trader closed or cancelled it while this leg was in flight), the
        late position is closed / cancelled at once through the normal close path. A trade that is FAILED or
        still OPENING has not ended: no other broker opened it, so the late position is the trade.
        Returns (HTML message, severity)."""
        name = leg.broker
        trade = await self.db.get_trade(leg.trade_id)
        if trade is None:
            raise KeyError(f"trade {leg.trade_id} not found")
        positions = await self.db.broker_positions(trade.id)
        pos = next((p for p in positions if p.broker == name), None)     # the stored row (the monitor may have synced it)
        if pos is None and outcome.position is not None:                 # the leg always stores it; never lose it
            pos = outcome.position
            await self.db.save_broker_position(trade.id, pos)
            positions = await self.db.broker_positions(trade.id)
        lines = [f"• {outcome.text}"]
        severity = (Severity.INFO if outcome.kind == "ok"
                    else Severity.WARNING if outcome.kind in ("skipped", "unsupported") else Severity.ERROR)
        ended = leg.close_on_arrival
        if ended is None and trade.status in (TradeStatus.CLOSED, TradeStatus.CANCELLED):
            ended = f"işlem bu arada {'kapandı' if trade.status is TradeStatus.CLOSED else 'iptal edildi'}"
        active = pos is not None and pos.status in ACTIVE_POSITION
        if active and ended:
            assert pos is not None
            closing = ManagementPlan(
                plan_id=uuid.uuid4().hex[:10], action=ActionType.CLOSE, trade_id=trade.id, close_fraction=1.0,
                source_chat_id=plan.source_chat_id, summary_tr="geç açılan pozisyon kapatıldı", created_at=self._now(),
            )
            done = await self._manage_position(closing, trade, pos)
            positions = await self.db.broker_positions(trade.id)
            if done.kind in ("ok", "partial"):
                lines.append(f"• {esc(ended)}; geç açılan bu pozisyon hemen kapatıldı ({done.text})")
                severity = Severity.WARNING if done.kind == "ok" and severity is not Severity.ERROR else Severity.ERROR
            else:
                size = pos.remaining_quantity if pos.status is PositionStatus.OPEN else pos.quantity
                lines.append(
                    f"• 🚨 {esc(ended)} ama {esc(name)} hesabındaki {_qty(size)} {esc(pos.broker_symbol)} "
                    f"{pos.side.value.upper()} {'pozisyonu' if pos.status is PositionStatus.OPEN else 'emri'} "
                    f"KAPATILAMADI ({done.text}). {esc(name)} hesabında ELLE kapatın.")
                severity = Severity.ERROR
        elif active and leg.missed:
            lines.append("• ⚠️ Bu emir yanıt beklerken işlemde yapılan şu değişiklikler bu pozisyona UYGULANMADI: "
                         + esc("; ".join(leg.missed)) + f". {esc(name)} hesabında elle kontrol edin.")
            severity = Severity.ERROR
        status = rollup_status(positions)
        others = any(tid == trade.id and broker != name for tid, broker in self._late_legs)
        if others and status not in ACTIVE_TRADE and trade.status is TradeStatus.OPENING:
            status = TradeStatus.OPENING            # another late leg of this trade is still in flight
        parts = [p for p in (trade.note or "").split("; ") if p and p != f"{name}: late"]
        if outcome.kind != "ok":
            parts.append(f"{name}: {outcome.kind}")
        note = "; ".join(parts)
        if status is not trade.status or note != (trade.note or ""):
            await self.db.update_trade(trade.id, status=status, note=note)
        if status is TradeStatus.FAILED and trade.status is TradeStatus.OPENING:
            lines.append("• Bu işlem hiçbir brokerda açılamadı.")
        await self._log("open_late_done", {
            "trade_id": trade.id, "broker": name, "kind": outcome.kind, "status": status.value,
            "closed_on_arrival": bool(active and ended), "text": outcome.text,
        })
        opened = active and not ended and outcome.kind == "ok"
        icon = "✅" if opened else ("⚠️" if severity is not Severity.ERROR else "🚨")
        return f"{icon} <b>{esc(leg.label)}</b> — {esc(name)} geç yanıt verdi\n" + "\n".join(lines), severity

    async def _optional_skip_notice(self, plan: TradePlan) -> None:
        """Once per signal: tell the owner which brokers of the route are not connected and therefore skip this
        trade (optional ones: a short notice; the others: an error). The trade is never queued for them; the
        other brokers are not affected."""
        names = [n for n in (plan.broker_names or list(self.optional_down)) if n in self.optional_down]
        for name in names:
            await self._notify(
                f"ℹ️ {esc(name)} bağlı değil, bu işlem atlandı ({esc(plan.base_asset)} {plan.side.value.upper()}).",
                Severity.WARNING,
            )
        # A broker of the route that is NOT optional and could not be created or connected at startup: the
        # route's other brokers take the trade, and the owner is told that this one did not. One that could not
        # connect is retried in the background; one that could not even be created stays down until a restart.
        connected = {b.name for b in self.brokers}
        for name in plan.broker_names or list(self.failed_brokers):
            if name in self.failed_brokers and name not in self.optional_down and name not in connected:
                outlook = ("Arka planda yeniden bağlanmaya çalışılıyor; bağlanınca haber verilecek."
                           if name in self._retrying else "Bu hesap, bot yeniden başlatılana kadar bağlanmaz.")
                await self._notify(
                    f"🚨 {esc(name)} bağlı değil; bu işlem {esc(name)} hesabında AÇILMADI "
                    f"({esc(plan.base_asset)} {plan.side.value.upper()}). {outlook}",
                    Severity.ERROR,
                )

    async def _no_route_broker(self, trade: TradeRecord) -> str:
        """None of the route's brokers is connected: never trade the signal elsewhere. The trade is kept as
        FAILED (later "Stop" / "kapattık" replies to it are then ignored, not asked)."""
        plan = trade.plan
        if all(n in self.optional_down for n in plan.broker_names):
            # Only optional brokers are on this route and they are down: a short notice, not an alarm.
            await self.db.update_trade(trade.id, status=TradeStatus.FAILED, note="isteğe bağlı broker bağlı değil")
            text = (f"ℹ️ {esc(', '.join(plan.broker_names))} bağlı değil, bu işlem atlandı "
                    f"(#{trade.id} {esc(plan.base_asset)} {plan.side.value.upper()}).")
            await self._notify(text, Severity.WARNING)
            await self._log("open_skipped_optional_down", {"trade_id": trade.id, "route": plan.route,
                                                           "brokers": list(plan.broker_names)})
            return text
        names = ", ".join(esc(n) for n in plan.broker_names)
        why = "; ".join(f"{esc(n)}: {esc(self.failed_brokers[n])}" for n in plan.broker_names if n in self.failed_brokers)
        await self.db.update_trade(trade.id, status=TradeStatus.FAILED, note="konunun brokerı bağlı değil")
        text = (
            f"🚨 <b>#{trade.id} {esc(plan.base_asset)} {plan.side.value.upper()} açılamadı</b>: "
            f"'{esc(plan.route or '?')}' konusunun brokerı ({names}) bağlı değil. Sinyal başka bir hesapta "
            "AÇILMADI." + (f"\nBağlantı hatası: <code>{why}</code>" if why else "")
            + ("\nBağlanamayan hesaba arka planda yeniden bağlanmaya çalışılıyor; bağlanınca haber verilecek."
               if any(n in self._retrying for n in plan.broker_names) else "")
            + f"\n{esc(plan.summary_tr)}"
        )
        await self._notify(text, Severity.ERROR)
        await self._log("open_no_route_broker", {"trade_id": trade.id, "route": plan.route,
                                                 "brokers": list(plan.broker_names)})
        return text

    async def _compute_price_offset(
        self, broker: Broker, plan: TradePlan, quote: Quote, mult: float, *, ref_price: float | None = None
    ) -> tuple[dict[str, Any] | None, str | None]:
        """(offset, problem). Offset brokers only (``reference_exchange`` set, crypto plan: see
        :func:`is_crypto_plan`): compares the broker's mid with the reference exchange's mid (scaled like the
        signal) and returns the shift to apply.
        Fails closed: unavailable reference or a too big difference -> a problem text and no offset."""
        cfg = broker.config
        ref_id = getattr(cfg, "reference_exchange", None)
        if not ref_id or not is_crypto_plan(plan, self._plan_kind(plan)):
            return None, None
        ref = ref_price     # a virtual trigger passes the fresh reading it just triggered on
        if ref is None:
            ref = await self._reference_mid(ref_id, plan.base_asset)
        broker_mid = (quote.bid + quote.ask) / 2.0
        if ref is None or not (math.isfinite(ref) and ref > 0) or not (math.isfinite(broker_mid) and broker_mid > 0):
            return None, (f"referans fiyat ({esc(ref_id)}) alınamadı; fiyat farkı hesaplanamadığı için "
                          "işlem güvenlik gereği açılmadı")
        ref_mid = float(ref) * mult
        diff_pct = abs(broker_mid - ref_mid) / ref_mid * 100.0
        max_pct = float(getattr(cfg, "max_offset_pct", 1.0))
        if diff_pct > max_pct:
            return None, (f"broker fiyatı ({fmt_price(broker_mid)}) ile {esc(ref_id)} fiyatı ({fmt_price(ref_mid)}) "
                          f"arasındaki fark {fmt.fmt_pct(diff_pct)} (sınır {fmt.fmt_pct(max_pct)}); "
                          "yanlış sembol veya eski fiyat olabilir, işlem açılmadı")
        if getattr(cfg, "offset_mode", "additive") == "ratio":
            return {"mode": "ratio", "value": broker_mid / ref_mid}, None
        return {"mode": "additive", "value": broker_mid - ref_mid}, None

    async def _open_on_broker(
        self, broker: Broker, plan: TradePlan, trade: TradeRecord, raw_symbol: str | None,
        *, virtual: BrokerPosition | None = None, ref_price: float | None = None,
        flow: _OpenFlow | None = None, mute_errors: bool = False,
    ) -> BrokerOutcome:
        """``virtual``: the waiting virtual entry that just triggered (open it as a MARKET order with its stored
        stop / take-profits, the offset measured now from ``ref_price``).
        ``flow``: the open flow this leg belongs to (None: the caller holds the trade lock, e.g. the monitor
        for a virtual entry). ``mute_errors``: do not send the error notice here (the caller reports or retries).

        Always ends in a BrokerOutcome: a BrokerError, any other exception and a CancelledError that nobody
        asked for (:func:`stray_cancel`) are this broker's error outcome; only a real cancellation of the
        running task propagates. ``outcome.sent`` tells whether an order request had gone out by then."""
        name = broker.name
        risk = effective_risk(self.config.risk, self._route_named(plan.route))
        sent = False                # an order request went to the broker: it may exist there whatever came back
        try:
            sym = await broker.resolve_symbol(plan.base_asset, plan.quote_asset, plan.market_type, raw_symbol)
            if sym is None:
                return BrokerOutcome(name, "unsupported",
                                     f"{esc(name)}: {esc(plan.base_asset)} bu hesapta işlem görmüyor")
            conflict = await self._hedge_conflict(broker, plan, trade, sym.broker_symbol)
            if conflict:
                return BrokerOutcome(name, "skipped", f"{esc(name)}: {conflict}", benign=True)
            if virtual is None and virtual_entry_wanted(broker, plan, self._plan_kind(plan)):
                return await self._register_virtual(broker, plan, trade, sym, raw_symbol, flow)
            entry_type = EntryType.MARKET if virtual is not None else plan.entry_type
            equity = await broker.get_equity()
            ccy = self._currency.get(name, "")
            loss_limit = getattr(broker.config, "daily_loss_limit_pct", None)
            if loss_limit is None:
                loss_limit = risk.daily_loss_limit_pct
            if loss_limit > 0:
                start = guards.start_of_day(self._now(), self.config.timezone)
                realized = await self.db.realized_pnl_since(name, start)
                if guards.daily_loss_exceeded(realized, equity, loss_limit):
                    text = (f"{esc(name)}: günlük zarar sınırı ({fmt.fmt_pct(loss_limit)}) aşıldı "
                            f"(bugün {_money(realized)} {esc(ccy)}); yeni işlem açılmadı")
                    await self._notify(text, Severity.WARNING)
                    return BrokerOutcome(name, "skipped", text)
                if (getattr(broker.config, "daily_loss_include_open_risk", False)
                        and math.isfinite(equity) and equity > 0):
                    # Prop accounts: what is still at risk counts too. Realized loss of the day + the loss of this
                    # broker's open and pending positions at their current stops + this trade's own risk must
                    # stay within the limit (guards.position_open_risk says what "at risk" covers).
                    at_risk = guards.open_risk(p for tid, p in await self.db.active_broker_positions()
                                               if p.broker == name and tid != trade.id)
                    wanted = broker_risk_amount(broker, equity, plan)
                    if guards.daily_loss_would_exceed(realized, at_risk, wanted, equity, loss_limit):
                        total = guards.daily_loss_with_open_risk(realized, at_risk, wanted)
                        text = (
                            f"{esc(name)}: günlük zarar sınırı ({fmt.fmt_pct(loss_limit)} = "
                            f"{_money(equity * loss_limit / 100.0)} {esc(ccy)}) aşılırdı: bugün gerçekleşen zarar "
                            f"{_money(max(0.0, -realized))} + açık ve bekleyen pozisyonların stoptaki riski "
                            f"{_money(at_risk)} + bu işlemin riski {_money(wanted)} = {_money(total)} {esc(ccy)}; "
                            "yeni işlem açılmadı")
                        await self._notify(text, Severity.WARNING)
                        await self._log("open_skipped_open_risk", {
                            "trade_id": trade.id, "broker": name, "realized": realized, "open_risk": at_risk,
                            "new_risk": wanted, "equity": equity, "limit_pct": loss_limit})
                        return BrokerOutcome(name, "skipped", text)
            if not (math.isfinite(equity) and equity > 0):
                raise BrokerError(f"hesap değeri okunamadı veya sıfır ({equity})")
            quote = await broker.get_quote(sym)
            if not (quote.bid > 0 and quote.ask > 0):
                raise BrokerError(f"geçersiz fiyat (alış {quote.bid}, satış {quote.ask})")
            # Signal prices -> this venue's price scale (e.g. PEPE -> 1000PEPE contracts).
            mult = price_multiplier(sym.extra)
            stop = plan.stop_loss * mult
            tps = scaled_take_profits(plan.take_profits, mult)
            if virtual is not None:     # levels as moved by the trader while the entry was waiting
                stop = virtual.stop_loss if virtual.stop_loss is not None else stop
                tps = [TakeProfit(price=tp.price, close_fraction=tp.close_fraction) for tp in virtual.take_profits]
            signal_entry = plan.entry_price * mult if plan.entry_price else None
            offset, offset_problem = await self._compute_price_offset(broker, plan, quote, mult, ref_price=ref_price)
            if offset_problem:
                text = f"{esc(name)}: {offset_problem}"
                await self._notify(text, Severity.WARNING)
                return BrokerOutcome(name, "skipped", text)
            if offset:
                stop = shift_price(stop, offset)
                tps = shifted_take_profits(tps, offset)
                signal_entry = shift_price(signal_entry, offset) if signal_entry else None
                if stop <= 0 or any(tp.price <= 0 for tp in tps) or (signal_entry is not None and signal_entry <= 0):
                    text = f"{esc(name)}: kaydırılmış seviyeler geçersiz; bu hesapta işlem açılmadı"
                    await self._notify(text, Severity.WARNING)
                    return BrokerOutcome(name, "skipped", text)
            tp1 = tps[0].price if tps else None
            if entry_type is EntryType.MARKET:
                problem = _price_problem(plan.side, quote, stop, tp1)
                entry = quote.entry_price_for(plan.side)
                drift = _distance_pct(entry, signal_entry) if signal_entry else 0.0
                if problem is None and drift > risk.max_entry_distance_pct:
                    problem = (f"anlık fiyat ({fmt_price(entry)}) sinyal girişinden ({fmt_price(signal_entry)}) "
                               f"{fmt.fmt_pct(drift)} uzak")
            else:
                problem = _price_problem(plan.side, quote, stop, None)
                entry = float(signal_entry or 0.0)
                if problem is None and entry > 0:
                    # A limit that is already marketable is opened at the market by MT5 brokers: only within
                    # max_entry_distance_pct (beyond it the price was probably misread).
                    live = quote.entry_price_for(plan.side)
                    marketable = live <= entry if plan.side is Side.LONG else live >= entry
                    drift = _distance_pct(live, entry)
                    if marketable and drift > risk.max_entry_distance_pct:
                        problem = (f"anlık fiyat ({fmt_price(live)}) limit girişten ({fmt_price(entry)}) "
                                   f"{fmt.fmt_pct(drift)} uzak")
            if problem:
                text = f"{esc(name)}: {esc(problem)}; bu hesapta işlem açılmadı"
                await self._notify(text, Severity.WARNING)
                return BrokerOutcome(name, "skipped", text)

            amount = broker_risk_amount(broker, equity, plan)
            sizing_entry, size_capped = entry, False
            if entry_type is EntryType.MARKET:
                # A live price close to the stop would multiply the size: capped by the signal's own stop distance.
                sizing_entry, size_capped = market_sizing_entry(
                    plan.side, entry, stop, signal_entry,
                    getattr(risk, "max_size_multiple", DEFAULT_MAX_SIZE_MULTIPLE))
            size = await broker.size_position(sym, plan.side, sizing_entry, stop, amount)
            req = OpenRequest(
                plan=plan,
                symbol=sym,
                quantity=size.quantity,
                entry_type=entry_type,
                entry_price=signal_entry if entry_type is EntryType.LIMIT else None,
                stop_loss=stop,
                take_profits=tps,
                leverage=size.leverage,
                client_tag=f"ct{plan.plan_id}",
            )
            await self._log("order_send", {"trade_id": trade.id, "broker": name, "symbol": sym.broker_symbol,
                                           "quantity": size.quantity, "risk_amount": size.risk_amount,
                                           "entry_ref": entry, "leverage": size.leverage,
                                           **({"sizing_entry": sizing_entry} if size_capped else {})})
            sent = True
            pos = await broker.open_position(req)
            pos.risk_amount = size.risk_amount
            pos.data.setdefault("price_multiplier", mult)
            if offset:
                pos.data["price_offset"] = offset
            if getattr(broker.config, "daily_loss_include_open_risk", False):
                # What risk_amount was sized for (guards.position_open_risk scales it to the current stop). "loss":
                # what that size really loses there when it is MORE than the target (a size raised to the minimum
                # lot, risk.allow_min_qty_round_up); otherwise the target stands (rounding down is not credited).
                basis: dict[str, float] = {"entry": sizing_entry, "stop": stop, "quantity": size.quantity}
                if math.isfinite(size.est_loss_at_sl) and size.est_loss_at_sl > size.risk_amount:
                    basis["loss"] = size.est_loss_at_sl
                pos.data.setdefault("risk_basis", basis)
            if virtual is not None:
                pos.data["virtual_fired"] = {
                    "trigger_price": virtual.data.get("trigger_price"), "ref_price": ref_price,
                    "at": self._now().isoformat(),
                }
            await self._save_leg_position(trade.id, pos, flow)
            if pos.status is PositionStatus.FAILED:
                text = f"{esc(name)}: <code>{esc(pos.error or 'işlem açılamadı')}</code>"
                await self._notify(text, Severity.ERROR)
                self.monitor.note_alerted(trade.id, pos)
                return BrokerOutcome(name, "error", text, pos, sent=True)
            fill = f", giriş ~{fmt_price(pos.avg_entry_price)}" if pos.avg_entry_price else ""
            text = (f"{esc(name)}: {_qty(pos.quantity)} {esc(sym.broker_symbol)} "
                    f"({pos_status_text(pos)}), risk {_money(size.risk_amount)} {esc(ccy)}"
                    f"{fill}")
            if offset:
                text += f" ({offset_text(offset)})"
            warnings = [*size.warnings, *(str(w) for w in pos.data.get("warnings") or [])]
            if size_capped:
                warnings.insert(0, size_cap_warning(size.risk_amount, entry, sizing_entry, stop, ccy))
            if warnings:
                text += " ⚠️ " + esc("; ".join(warnings))
            # Larger than the size of the signal's own stop distance, but limited: by the engine (not when the
            # multiple is 1, where the planned size simply stands) or by the adapter. Always told to the owner.
            capped = bool(pos.data.get("size_capped")) or (
                size_capped and signal_entry is not None
                and abs(sizing_entry - stop) < abs(signal_entry - stop) * (1.0 - 1e-9))
            if await self._report_position_problem(trade, pos, None):
                text += f" 🚨 <code>{esc(pos.error or protection_problem(pos) or '')}</code>"
                return BrokerOutcome(name, "partial", text, pos, sent=True, size_capped=capped)
            return BrokerOutcome(name, "ok", text, pos, sent=True, size_capped=capped)
        except BrokerError as exc:
            text = f"{esc(name)}: <code>{esc(exc)}</code>"
            if not mute_errors:
                await self._notify(text, Severity.ERROR)
            return BrokerOutcome(name, "error", text, sent=sent)
        except asyncio.CancelledError:
            if not stray_cancel():
                raise           # this task itself is being cancelled
            # Nobody cancelled this task: a call into the broker's library was cancelled from the inside (e.g.
            # MetaApi while its account is disconnected). A failure of this broker only, never of the flow.
            logger.error("%s: a broker call was cancelled without a cancel request while opening %s "
                         "(order request sent: %s)", name, plan.base_asset, sent)
            text = f"{esc(name)}: {_CANCELLED_AFTER_ORDER if sent else _CANCELLED_BEFORE_ORDER}"
            if not mute_errors:
                await self._notify(text, Severity.ERROR)
            return BrokerOutcome(name, "error", text, sent=sent)
        except Exception as exc:
            logger.exception("%s: unexpected error opening %s", name, plan.base_asset)
            text = f"{esc(name)}: beklenmeyen hata <code>{esc(exc)}</code>"
            if not mute_errors:
                await self._notify(text, Severity.ERROR)
            return BrokerOutcome(name, "error", text, sent=sent)

    async def _save_leg_position(self, trade_id: int, pos: BrokerPosition, flow: _OpenFlow | None) -> None:
        """Store the position a leg just got from its broker (at once: a crash must never lose it). Every
        position write happens under the trade lock: the leg's flow (or the monitor, for a virtual entry)
        holds it; a late leg, whose flow has finished, takes it for the write."""
        if flow is not None and flow.released:
            async with self.trade_lock(trade_id):
                await self.db.save_broker_position(trade_id, pos)
        else:
            await self.db.save_broker_position(trade_id, pos)

    # ------------------------------------------------------------------ virtual (reference-touch) entries
    async def _register_virtual(
        self, broker: Broker, plan: TradePlan, trade: TradeRecord, sym: Any, raw_symbol: str | None,
        flow: _OpenFlow | None = None,
    ) -> BrokerOutcome:
        """``entry_trigger: reference_touch``: no order on the broker. A PENDING position carrying
        ``data["virtual_trigger"]`` is stored (it survives a restart); the monitor watches the reference price."""
        name = broker.name
        ref_id = str(broker.config.reference_exchange)
        mult = price_multiplier(sym.extra)
        entry = float(plan.entry_price or 0.0) * mult
        hours = float(getattr(broker.config, "virtual_trigger_max_hours", 48.0))
        # "The price ran to the first target without touching the entry" cancels a waiting entry. That rule
        # counts from the start when the market price is between the entry and the first target right now (as
        # it always did). A pullback limit starts with the price BEYOND its first target (the target lies
        # between the entry and today's price): there, and when the price is not known now, the rule starts
        # to count once the reference price has been seen between the two (see _virtual_check). The price is
        # the one the signal was just judged with (no reference reading: the offset is measured at the touch).
        tp_armed = True
        if plan.take_profits:
            tp1 = plan.take_profits[0].price
            price = await self._plan_price_within(plan, raw_symbol, VIRTUAL_REGISTER_PRICE_TIMEOUT_SEC)
            if price is None or not (math.isfinite(price) and price > 0):
                tp_armed = False
            else:
                tp_armed = not (price >= tp1 if plan.side is Side.LONG else price <= tp1)
        pos = BrokerPosition(
            broker=name, broker_symbol=sym.broker_symbol, side=plan.side, status=PositionStatus.PENDING,
            quantity=0.0, remaining_quantity=0.0,
            stop_loss=plan.stop_loss * mult, take_profits=scaled_take_profits(plan.take_profits, mult),
            dry_run=self.config.dry_run,
            data={
                "virtual_trigger": True, "virtual_state": "waiting", "trigger_price": entry,
                "reference_exchange": ref_id, "price_multiplier": mult, "raw_symbol": raw_symbol,
                "virtual_created_at": self._now().isoformat(), "virtual_max_hours": hours,
                "virtual_tp_armed": tp_armed,
            },
        )
        await self._save_leg_position(trade.id, pos, flow)
        text = (f"{esc(name)}: sanal tetik kuruldu: {esc(ref_label(ref_id))} fiyatı {fmt_price(plan.entry_price)} "
                f"seviyesine değince {esc(name)}'da açılacak (en çok {fmt.fmt_number(hours)} saat beklenir)")
        if not self.config.notifier.notify_on_execution:
            await self._notify(f"🧭 #{trade.id} {esc(plan.base_asset)} {plan.side.value.upper()}: {text}", Severity.INFO)
        await self._log("virtual_registered", {"trade_id": trade.id, "broker": name, "trigger": entry,
                                                "reference": ref_id})
        return BrokerOutcome(name, "ok", text, pos)

    async def _virtual_end(
        self, trade: TradeRecord, pos: BrokerPosition, status: PositionStatus, why: str, text: str,
        severity: Severity = Severity.WARNING,
    ) -> BrokerPosition:
        """Terminal state of a virtual entry (CANCELLED / FAILED): it can never trigger again."""
        new = pos.model_copy(deep=True)
        new.status = status
        new.data["virtual_state"] = "cancelled" if status is PositionStatus.CANCELLED else "failed"
        new.data["virtual_end"] = why
        if status is PositionStatus.FAILED:
            new.error = why
        await self.db.save_broker_position(trade.id, new)
        self._virtual_forget(trade.id, pos.broker)
        await self._log("virtual_ended", {"trade_id": trade.id, "broker": pos.broker, "status": status.value,
                                          "why": why})
        label = f"#{trade.id} {esc(trade.plan.base_asset)} {trade.plan.side.value.upper()}"
        await self._notify(f"🧭 {label} {esc(pos.broker)}: {text}", severity)
        return new

    def _virtual_forget(self, trade_id: int, broker: str) -> None:
        """Drop what is kept in memory about a virtual entry that has ended (fired, cancelled or failed)."""
        self._virtual_seen.pop((trade_id, broker), None)
        self._virtual_warned.pop((trade_id, broker), None)
        self._virtual_warned.pop((trade_id, f"{broker}:busy"), None)

    async def _virtual_tick(self, trade: TradeRecord, pos: BrokerPosition, now: datetime) -> BrokerPosition:
        """One look at a waiting virtual entry (monitor hook, trade lock held): expire / gap-cancel / trigger.
        Returns the (possibly new) position. A CancelledError that nobody asked for (a broker or exchange
        library, see :func:`stray_cancel`) never leaves this hook: the entry is looked at again next pass."""
        try:
            return await self._virtual_check(trade, pos, now)
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            logger.error("Virtual entry %s #%s: a call was cancelled without a cancel request; next pass",
                         pos.broker, trade.id)
            return pos

    async def _virtual_range(
        self, trade: TradeRecord, pos: BrokerPosition, ref_id: str, mult: float, px: float, now: datetime
    ) -> tuple[float, float, bool, list[tuple[int, float, float]]]:
        """(lowest, highest reference price since the previous look at this entry, in the entry's price scale;
        were the candles read?; the candles that count as (open ms, low, high) in that scale). The price is
        sampled only every couple of seconds, so a touch that reverses
        between two looks (a wick) and whatever happened while the reference was unreachable or the bot was
        down are taken from the reference exchange's 1-minute candles. Only minutes that began after the entry
        was registered count (the minute it was registered in also holds prices from before the signal).
        Without candles (not offered, or the request failed) the range is just the current price ``px``."""
        lo = hi = px
        counted: list[tuple[int, float, float]] = []
        fetch = getattr(self.reference_prices, "candles", None)
        if fetch is None:
            return lo, hi, True, counted
        try:
            created = utc(datetime.fromisoformat(str(pos.data.get("virtual_created_at"))))
        except (TypeError, ValueError):
            created = utc(trade.created_at)
        created_ms = int(created.timestamp() * 1000)
        seen = self._virtual_seen.get((trade.id, pos.broker)) or pos.data.get("virtual_seen_ms") or created_ms
        try:
            seen_ms = int(seen)
        except (TypeError, ValueError):
            seen_ms = created_ms
        first_minute = -(-created_ms // _MINUTE_MS) * _MINUTE_MS            # the first minute that began after it
        since = max(seen_ms // _MINUTE_MS * _MINUTE_MS, first_minute)
        now_ms = int(now.timestamp() * 1000)
        if since > now_ms:
            return lo, hi, True, counted
        try:
            rows = await fetch(ref_id, trade.plan.base_asset, since, now_ms)
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            rows = None
        except Exception:
            logger.exception("Reference candles failed for %s on %s", trade.plan.base_asset, ref_id)
            rows = None
        if rows is None:
            return lo, hi, False, counted
        for open_ms, low, high in rows:
            if open_ms >= since and low > 0 and high > 0:
                counted.append((int(open_ms), float(low) * mult, float(high) * mult))
                lo, hi = min(lo, float(low) * mult), max(hi, float(high) * mult)
        return lo, hi, True, counted

    async def _virtual_seen_now(self, trade: TradeRecord, pos: BrokerPosition, now: datetime) -> BrokerPosition:
        """Everything up to ``now`` has been looked at for this entry. Kept in memory; written to the stored
        position at most once a minute (so that after a restart the time the bot was down is checked too)."""
        now_ms = int(now.timestamp() * 1000)
        self._virtual_seen[(trade.id, pos.broker)] = now_ms
        try:
            stored = int(pos.data.get("virtual_seen_ms") or 0)
        except (TypeError, ValueError):
            stored = 0
        if stored == 0:
            try:
                stored = int(utc(datetime.fromisoformat(str(pos.data.get("virtual_created_at")))).timestamp() * 1000)
            except (TypeError, ValueError):
                stored = int(utc(trade.created_at).timestamp() * 1000)
        if now_ms - stored < _MINUTE_MS:
            return pos
        new = pos.model_copy(deep=True)
        new.data["virtual_seen_ms"] = now_ms
        await self.db.save_broker_position(trade.id, new)
        return new

    async def _virtual_check(self, trade: TradeRecord, pos: BrokerPosition, now: datetime) -> BrokerPosition:
        d = pos.data
        key = (trade.id, pos.broker)
        label = f"#{trade.id} {esc(trade.plan.base_asset)} {trade.plan.side.value.upper()}"
        if d.get("virtual_state") == "firing":
            # Saved just before the order was sent and never resolved: the process died in between. The order
            # may or may not exist on the broker -> never send it again.
            return await self._virtual_end(
                trade, pos, PositionStatus.FAILED, "tetikleme yarıda kaldı (yeniden başlatma)",
                "tetikleme sırasında sistem durdu; emrin gönderilip gönderilmediği bilinmiyor, tekrar "
                "GÖNDERİLMEDİ. Brokerda elle kontrol edin.", Severity.ERROR)
        try:
            created = utc(datetime.fromisoformat(str(d.get("virtual_created_at"))))
        except (TypeError, ValueError):
            created = utc(trade.created_at)
        max_hours = float(d.get("virtual_max_hours") or 48.0)
        if now - created >= timedelta(hours=max_hours):
            return await self._virtual_end(
                trade, pos, PositionStatus.CANCELLED, "süre doldu",
                f"sanal tetik {fmt.fmt_number(max_hours)} saatte tetiklenmedi, iptal edildi.", Severity.INFO)
        # The trade is over on the other accounts (a filled position there has closed and none is left): the
        # trader's trade has been played out, so this entry must not open later with the original stop.
        real = [p for p in await self.db.broker_positions(trade.id)
                if p.broker != pos.broker and not is_virtual_pending(p)]
        if (any(p.status is PositionStatus.CLOSED for p in real)
                and not any(p.status in ACTIVE_POSITION for p in real)):
            return await self._virtual_end(
                trade, pos, PositionStatus.CANCELLED, "diğer hesaplarda işlem kapandı",
                "işlemin diğer hesaplardaki pozisyonları kapandı; bekleyen sanal giriş iptal edildi, bu hesapta "
                "işlem AÇILMADI.", Severity.INFO)
        broker = self._broker(pos.broker)       # None: not connected right now (the levels are still watched)
        ref_id = str(d.get("reference_exchange") or "")
        if not ref_id:
            return pos
        ref = await self._reference_mid(ref_id, trade.plan.base_asset)
        if ref is None or not (math.isfinite(ref) and ref > 0):
            last = self._virtual_warned.get(key)
            if last is None or time.monotonic() - last >= VIRTUAL_NOTE_THROTTLE_SEC:
                self._virtual_warned[key] = time.monotonic()
                await self._notify(
                    f"⚠️ #{trade.id} {esc(trade.plan.base_asset)} {trade.plan.side.value.upper()} {esc(pos.broker)}: "
                    f"referans fiyat ({esc(ref_id)}) alınamıyor; sanal tetik beklemeye devam ediyor "
                    "(fiyat farkı ölçülemeden işlem AÇILMAZ).", Severity.WARNING)
            return pos          # not "seen": what happens meanwhile is read from the candles afterwards
        mult = price_multiplier(d)
        px = float(ref) * mult
        long = pos.side is Side.LONG
        trigger = float(d.get("trigger_price") or 0.0)
        stop = pos.stop_loss
        tp1 = pos.take_profits[0].price if pos.take_profits else None
        lo, hi, candles_read, candles = await self._virtual_range(trade, pos, ref_id, mult, px, now)
        worst = lo if long else hi                          # the extreme towards the stop (and the entry)
        gap = None
        if stop is not None and ((long and worst <= stop) or (not long and worst >= stop)):
            gap = (f"{esc(ref_label(ref_id))} fiyatı ({fmt_price(worst / mult)}) stop seviyesini "
                   f"({fmt_price(stop / mult)}) geçti")
        elif tp1 is not None:
            # "The price ran to the first target without touching the entry: the trade idea is played out."
            # That only counts once the price has been BETWEEN the entry and that target: a pullback limit
            # starts with the price beyond its first target (the target lies between the entry and today's
            # price) and has to come back through it first. ``virtual_tp_armed`` False (set when the entry is
            # registered and when its targets are moved): not seen between yet. Missing (an entry stored by
            # an earlier version): the rule counts, as it did there.
            # Candles: only minutes that began after ``virtual_tp_ms`` (the moment the rule was armed, or the
            # targets were moved) count for it; the minute of that moment also holds earlier prices.
            now_ms = int(now.timestamp() * 1000)
            try:
                tp_ms = int(d.get("virtual_tp_ms") or 0)
            except (TypeError, ValueError):
                tp_ms = 0
            tp_from = -(-tp_ms // _MINUTE_MS) * _MINUTE_MS
            lows = [px, *(low for open_ms, low, _high in candles if open_ms >= tp_from)]
            highs = [px, *(high for open_ms, _low, high in candles if open_ms >= tp_from)]
            # the extreme towards the entry / towards the targets, for this rule
            back, ahead = (min(lows), max(highs)) if long else (max(highs), min(lows))
            reached: float | None = None
            if d.get("virtual_tp_armed", True) is False:
                if (long and back < tp1) or (not long and back > tp1):
                    # Seen between the entry and the target: from now on the rule counts. In this very look
                    # only the price right now can have gone back to the target AFTER that.
                    armed = pos.model_copy(deep=True)
                    armed.data["virtual_tp_armed"] = True
                    armed.data["virtual_tp_ms"] = now_ms
                    await self.db.save_broker_position(trade.id, armed)
                    pos, d = armed, armed.data
                    reached = px
            else:
                reached = ahead
            if reached is not None and ((long and reached >= tp1) or (not long and reached <= tp1)):
                gap = (f"{esc(ref_label(ref_id))} fiyatı ({fmt_price(reached / mult)}) ilk hedefi "
                       f"({fmt_price(tp1 / mult)}) geçti")
        if gap:
            return await self._virtual_end(
                trade, pos, PositionStatus.CANCELLED, "fiyat stop/hedefi geçti",
                f"sanal tetik iptal edildi: {gap}; giriş tetiklenmeden fiyat uzaklaştı, işlem AÇILMADI.")
        at_level = trigger > 0 and ((long and px <= trigger) or (not long and px >= trigger))
        wicked = trigger > 0 and not at_level and ((long and worst <= trigger) or (not long and worst >= trigger))
        if not (at_level or wicked):
            return await self._virtual_seen_now(trade, pos, now) if candles_read else pos
        if broker is None:
            # Touched while the account is not connected: nothing can be opened now. The touch stays "unseen",
            # so once the account is back the rule below decides (still near the level: open; else no chase).
            return pos
        if wicked:
            # Only the candle shows the touch; the price has turned back. Enter only if it is still as close to
            # the level as a market entry may be (the validator's tolerance); never chase it.
            risk = effective_risk(self.config.risk, self._route_named(trade.plan.route))
            if not near_entry(px, trigger, stop, risk):
                if not d.get("virtual_missed"):
                    noted = pos.model_copy(deep=True)
                    noted.data["virtual_missed"] = {"at": now.isoformat(), "extreme": worst / mult, "price": px / mult}
                    await self.db.save_broker_position(trade.id, noted)
                    await self._log("virtual_missed", {"trade_id": trade.id, "broker": pos.broker,
                                                       "extreme": worst / mult, "price": px / mult})
                    await self._notify(
                        f"🧭 {label} {esc(pos.broker)}: tetik seviyesi görüldü ama fiyat uzaklaştı; "
                        f"{esc(pos.broker)}'da açılmadı. {esc(ref_label(ref_id))} fiyatı kısa süreliğine "
                        f"{fmt_price(worst / mult)} seviyesine geldi (tetik {fmt_price(trigger / mult)}), şimdi "
                        f"{fmt_price(px / mult)}. Fiyat seviyeye dönerse açılır; stop veya ilk hedef geçilirse "
                        "sanal giriş iptal edilir.", Severity.WARNING)
                    pos = noted
                return await self._virtual_seen_now(trade, pos, now) if candles_read else pos
        if await self._is_paused():
            return await self._virtual_end(
                trade, pos, PositionStatus.CANCELLED, "bot duraklatılmış",
                f"tetik seviyesine gelindi ({esc(ref_label(ref_id))} {fmt_price(px / mult)}) ama yeni işlem açma "
                "duraklatılmış (/durdur); sanal giriş iptal edildi, işlem AÇILMADI.")
        running = self._leg_running.get(pos.broker)
        if running is not None and not running.done():
            # Another signal's open is in flight on this account: no second open at the same time (its hedge /
            # daily-loss / margin checks would race, see _open_leg). The touch stays "unseen": the next pass
            # decides again (still at the level: open; only seen on the candle and gone: no chase). Normally
            # that is a matter of a second; the owner is told only when that other open is a late one.
            busy_key = (trade.id, f"{pos.broker}:busy")
            last = self._virtual_warned.get(busy_key)
            if (any(name == pos.broker for _tid, name in self._late_legs)
                    and (last is None or time.monotonic() - last >= VIRTUAL_NOTE_THROTTLE_SEC)):
                self._virtual_warned[busy_key] = time.monotonic()
                await self._notify(
                    f"🧭 {label} {esc(pos.broker)}: tetik seviyesine gelindi ama {esc(pos.broker)} önceki bir emrine "
                    f"hâlâ yanıt vermedi; yanıt gelene kadar {esc(pos.broker)} hesabına yeni emir gönderilmiyor. "
                    "Yanıt geldiğinde fiyat hâlâ seviyedeyse açılır.", Severity.WARNING)
            return pos

        # Touched: from here on at most one order may ever be sent (state saved BEFORE sending).
        fired = pos.model_copy(deep=True)
        fired.data["virtual_state"] = "firing"
        await self.db.save_broker_position(trade.id, fired)
        # While this order is on its way the account is busy for new signals too (their leg waits, _open_leg).
        marker: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._leg_running[pos.broker] = marker
        try:
            outcome = await self._open_on_broker(
                broker, trade.plan, trade, d.get("raw_symbol"), virtual=fired, ref_price=float(ref), mute_errors=True)
        finally:
            if self._leg_running.get(pos.broker) is marker:
                del self._leg_running[pos.broker]
            marker.set_result(None)
        seen = (f"{esc(ref_label(ref_id))} fiyatı kısa süreliğine {fmt_price(worst / mult)} seviyesine geldi, şimdi "
                f"{fmt_price(px / mult)}" if wicked else f"{esc(ref_label(ref_id))} fiyatı {fmt_price(px / mult)}")
        if outcome.position is None:
            if outcome.kind == "error" and not outcome.sent:
                # Nothing was sent to the broker (its symbol, balance or price could not be read): the entry is
                # not lost for that. It goes back to waiting and is tried again on the next passes.
                failures = int(d.get("virtual_retries") or 0) + 1
                if failures <= VIRTUAL_RETRY_MAX:
                    again = pos.model_copy(deep=True)
                    again.data["virtual_state"] = "waiting"
                    again.data["virtual_retries"] = failures
                    await self.db.save_broker_position(trade.id, again)
                    await self._log("virtual_retry", {"trade_id": trade.id, "broker": pos.broker,
                                                      "failures": failures, "text": outcome.text})
                    if failures == 1:
                        await self._notify(
                            f"🧭 {label}: tetiklendi ({seen}) ama {esc(pos.broker)} hesabına ulaşılamadı, emir "
                            f"gönderilmedi: {outcome.text}\nFiyat seviyede kaldıkça sonraki kontrollerde en çok "
                            f"{VIRTUAL_RETRY_MAX} kez daha denenecek.", Severity.WARNING)
                    return again        # the touch stays "unseen": the next pass looks at it again
            if outcome.kind == "error":
                await self._notify(outcome.text, Severity.ERROR)     # (muted in _open_on_broker)
            status = PositionStatus.CANCELLED if outcome.kind in ("skipped", "unsupported") else PositionStatus.FAILED
            return await self._virtual_end(
                trade, pos, status, outcome.kind,
                f"tetiklendi ({esc(ref_label(ref_id))} {fmt_price(px / mult)}) ama işlem açılmadı, sanal giriş "
                f"iptal edildi: {outcome.text}")
        new = outcome.position
        self._virtual_forget(trade.id, pos.broker)
        icon = "✅" if outcome.kind == "ok" else "⚠️"
        word = "açıldı" if new.status is not PositionStatus.FAILED else "açılamadı"
        await self._notify(
            f"{icon} <b>{label} {word}</b> — sanal tetik tetiklendi ({seen}, seviye {fmt_price(trigger / mult)})"
            f"\n• {outcome.text}",
            Severity.INFO if outcome.kind == "ok" else Severity.ERROR)
        await self._log("virtual_fired", {"trade_id": trade.id, "broker": pos.broker, "ref_price": ref,
                                          "status": new.status.value, "kind": outcome.kind})
        return new

    async def _report_position_problem(
        self, trade: TradeRecord, pos: BrokerPosition, previous_error: str | None
    ) -> bool:
        """Tell the owner (ERROR, always - independent of notify_on_execution) when an adapter returned a
        position without a working stop, or a new ``pos.error`` (TP not placed, SL not resized,
        emergency close...). True if something was reported."""
        problem = protection_problem(pos)
        if problem is not None:
            await self._notify(unprotected_alert_text(trade, pos, problem), Severity.ERROR)
            self.monitor.note_alerted(trade.id, pos)
            return True
        if pos.error and pos.error != previous_error:
            await self._notify(
                f"⚠️ {esc(pos.broker)}: #{trade.id} {esc(trade.plan.base_asset)} {trade.plan.side.value.upper()} "
                f"({_POS_STATUS_TR.get(pos.status, pos.status.value)}): <code>{esc(pos.error)}</code>",
                Severity.ERROR,
            )
            self.monitor.note_alerted(trade.id, pos)
            return True
        return False

    # ------------------------------------------------------------------ management
    async def _handle_management(
        self, action: ParsedAction, chat_id: int, *, reply_to: StoredMessage | None = None,
        route: TopicRoute | None = None,
    ) -> None:
        ev = await self._evaluate_management(action, chat_id, reply_to, route=route)
        if ev.ignored is None and ev.trade is not None and ev.trade.status in ACTIVE_TRADE:
            # The trader's "Stop" / "kapattık" often comes right after the stop hit: sync the trade with its
            # venues first, so a position that just closed is not "closed" again (or asked about).
            fresh = await self._refresh_trade(ev.trade)
            if fresh.status is not ev.trade.status:
                ev = await self._evaluate_management(action, chat_id, reply_to, route=route)
        if ev.ignored is not None:
            await self._ignore_management(ev, chat_id)
            return
        if not ev.automatic:
            await self._ask_eval(ev, chat_id)
            return
        assert isinstance(ev.plan, ManagementPlan)
        await self.execute_management(ev.plan)

    async def _refresh_trade(self, trade: TradeRecord) -> TradeRecord:
        """Sync one trade's positions now (like a monitor pass). Never raises; the stored trade after it."""
        try:
            async with self.trade_lock(trade.id):
                await self.monitor.sync_trade(trade.id)
            return await self.db.get_trade(trade.id) or trade
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            logger.error("Sync of trade #%s before managing it was cancelled without a cancel request", trade.id)
            return trade
        except Exception:
            logger.exception("Sync of trade #%s before managing it failed", trade.id)
            return trade

    async def _ignore_management(self, ev: Evaluation, chat_id: int) -> None:
        """A management instruction for a trade that is already over: nothing to do, the owner is not asked."""
        action = ev.action
        logger.info("Management %s for a finished trade ignored: %s (messages %s)",
                    action.action.value, ev.ignored, action.used_message_ids)
        await self._log("manage_ignored", {
            "chat_id": chat_id, "trade_id": ev.trade.id if ev.trade else None, "reason": ev.ignored,
            "action": action.model_dump(mode="json"),
        })
        if ev.trade is not None and self._ends_trade(action.action, action.close_fraction):
            # The trade counts as over, but one of its brokers has not answered the open yet: if that order
            # opens after all, it is closed at once (the trader's trade is over).
            self._close_late_legs(ev.trade.id, "trader işlemi kapattı / iptal etti")
        elif ev.trade is not None:
            # Any other change (stop, targets, a partial close) cannot reach an order that has not been answered
            # yet: the owner is told when that order answers.
            for leg in self._late_legs_of(ev.trade.id):
                leg.missed.append(action.summary_tr or action.action.value)
        if self.config.notifier.notify_on_ignored:
            await self._notify(
                f"💬 Yok sayıldı: {esc(action.summary_tr or action.action.value)} — {esc(ev.ignored or '')}; "
                "yapılacak bir şey yok.",
                Severity.INFO,
            )

    async def execute_management(self, plan: ManagementPlan, *, notify: bool = True) -> str:
        """Apply ``plan`` to every active broker position of the trade. Returns an HTML result text.
        Runs as a critical section (see :meth:`execute_open`)."""
        return await self._critical_section(self._execute_management(plan, notify),
                                            f"manage-{plan.trade_id}-{plan.plan_id}")

    async def _execute_management(self, plan: ManagementPlan, notify: bool) -> str:
        async with self.trade_lock(plan.trade_id):
            trade = await self.db.get_trade(plan.trade_id)
            if trade is None:
                return f"#{plan.trade_id} işlemi bulunamadı."
            if trade.status not in ACTIVE_TRADE:
                text = f"#{trade.id} işlemi aktif değil ({trade.status.value})."
                if self._ends_trade(plan.action, plan.close_fraction):
                    # Not active (any more), but a broker may still owe the answer to its open: closed on arrival.
                    marked = self._close_late_legs(trade.id, "trader işlemi kapattı / iptal etti")
                    text += self._late_close_note(marked)
                return text
            positions = await self.db.broker_positions(trade.id)
            active = [p for p in positions if p.status in ACTIVE_POSITION]
            outcomes: list[BrokerOutcome] = list(
                await asyncio.gather(*(self._manage_position(plan, trade, p) for p in active))
            ) if active else []
            changed = {o.position.broker: o.position for o in outcomes if o.position is not None}
            merged = [changed.get(p.broker, p) for p in positions]
            ok = any(o.kind in ("ok", "partial") for o in outcomes)
            update: dict[str, Any] = {}
            if ok and plan.action is ActionType.MOVE_SL:
                new_sl = plan.new_stop_loss if plan.new_stop_loss is not None else trade.plan.entry_price
                if new_sl is not None:
                    update["current_stop_loss"] = new_sl
            if ok and plan.action is ActionType.MOVE_TP:
                update["current_take_profits"] = list(plan.take_profits)
            if merged:
                status = rollup_status(merged)
                if status is not trade.status:
                    update["status"] = status
            if update:
                trade = await self.db.update_trade(trade.id, **update)
            # Brokers that have not answered this trade's open yet: there is no position to apply this to.
            if self._ends_trade(plan.action, plan.close_fraction):
                pending = self._close_late_legs(trade.id, "trader işlemi kapattı / iptal etti")
                late_lines = [f"• ⏳ {esc(n)}: açılış emri henüz yanıtlanmadı; açılırsa hemen kapatılacak"
                              for n in pending]
            else:
                pending = [leg.broker for leg in self._late_legs_of(trade.id)]
                for leg in self._late_legs_of(trade.id):
                    leg.missed.append(plan.summary_tr or plan.action.value)
                late_lines = [f"• ⏳ {esc(n)}: açılış emri henüz yanıtlanmadı; bu değişiklik ona uygulanmadı"
                              for n in pending]
        if ok:
            await self._remember_applied(plan, trade)

        label = f"#{trade.id} {esc(trade.plan.base_asset)} {trade.plan.side.value.upper()}"
        lines = ([f"• {o.text}" for o in outcomes] + late_lines) or ["• Aktif broker pozisyonu yok."]
        if not outcomes:
            head = f"⚠️ {label}: uygulanacak pozisyon yok"
        elif ok and all(o.kind not in ("error", "partial") for o in outcomes):
            # The plan summary already starts with the trade label and is written before the fact
            # ("#1 BTC SHORT: pozisyonun tamamı kapatılacak"): the outcome lines below say what was done.
            head = f"✅ {label}: uygulandı"
        elif ok:
            head = f"⚠️ {label}: kısmen uygulandı"
        else:
            head = f"🚨 {label}: uygulanamadı"
        if notify and ok and self.config.notifier.notify_on_execution:
            await self._notify(f"{head}\n{fmt.format_management(plan, trade)}\n\n" + "\n".join(lines), Severity.INFO)
        await self._log("manage_done", {
            "trade_id": trade.id, "action": plan.action.value, "plan": plan.model_dump(mode="json"),
            "outcomes": [{"broker": o.broker, "kind": o.kind, "text": o.text} for o in outcomes],
        })
        return "\n".join([head, *lines])

    @staticmethod
    def _ends_trade(kind: ActionType, close_fraction: float | None) -> bool:
        """A cancel, or a close of the whole position: after it the trader's trade is over."""
        return kind is ActionType.CANCEL or (
            kind is ActionType.CLOSE and (close_fraction is None or close_fraction >= 0.999))

    def _late_legs_of(self, trade_id: int) -> list[_LateLeg]:
        return [leg for (tid, _name), leg in self._late_legs.items() if tid == trade_id]

    @staticmethod
    def _late_close_note(brokers: list[str]) -> str:
        """HTML (leading space) for a result text: the trade has no position to close, but ``brokers`` have not
        answered its open yet and were marked by :meth:`_close_late_legs`. Empty without such brokers."""
        if not brokers:
            return ""
        return (f" ⏳ {', '.join(esc(n) for n in brokers)}: bu işlemin açılışına henüz yanıt vermedi; açılırsa "
                "hemen kapatılacak.")

    def _close_late_legs(self, trade_id: int | None, why: str) -> list[str]:
        """Mark the late legs of a trade (None: of every trade): whatever they still open is closed at once
        when they answer (see :meth:`_book_late_leg`). Returns "broker" names marked."""
        marked: list[str] = []
        for (tid, name), leg in self._late_legs.items():
            if trade_id is None or tid == trade_id:
                leg.close_on_arrival = leg.close_on_arrival or why
                marked.append(name)
        return marked

    async def _live_quote(self, broker: Broker, trade: TradeRecord) -> Quote | None:
        try:
            sym = await broker.resolve_symbol(trade.plan.base_asset, trade.plan.quote_asset, trade.plan.market_type)
            return await broker.get_quote(sym) if sym is not None else None
        except asyncio.CancelledError:
            if not stray_cancel():
                raise
            logger.warning("%s: live quote for %s was cancelled without a cancel request", broker.name,
                           trade.plan.base_asset)
            return None
        except Exception as exc:
            logger.debug("%s: no live quote for %s: %s", broker.name, trade.plan.base_asset, exc)
            return None

    async def _manage_position(self, plan: ManagementPlan, trade: TradeRecord, pos: BrokerPosition) -> BrokerOutcome:
        name = pos.broker
        virtual = is_virtual_pending(pos)
        broker: Any = _VirtualEntryOps() if virtual else self._broker(name)
        if broker is None:
            text = f"{esc(name)}: broker bağlı değil; değişiklik uygulanamadı"
            await self._notify(text, Severity.ERROR)
            return BrokerOutcome(name, "error", text)
        try:
            new: BrokerPosition
            mult = price_multiplier(pos.data)
            offset = stored_offset(pos.data)   # fixed when the position was opened, never recomputed
            if virtual and plan.action is ActionType.MOVE_SL and plan.move_sl_to_entry:
                # The trader is at break-even. If the price came back to the entry now, this entry would open with
                # the original stop while the trader's own trade ends flat: it is not opened any more.
                new = await broker.cancel_entry(pos)
                new.data["virtual_end"] = "stop girişe çekildi"
                text = (f"{esc(name)}: stop girişe çekildiği için bekleyen sanal giriş iptal edildi "
                        "(bu hesapta işlem açılmayacak)")
            elif plan.action is ActionType.MOVE_SL:
                signal_sl = (shift_price(plan.new_stop_loss * mult, offset)
                             if plan.new_stop_loss is not None else None)
                if plan.move_sl_to_entry:
                    if pos.status is PositionStatus.PENDING:
                        return BrokerOutcome(name, "skipped", f"{esc(name)}: giriş henüz dolmadı; stop değiştirilmedi")
                    sl = pos.avg_entry_price or signal_sl
                else:
                    sl = signal_sl
                if sl is None:
                    raise BrokerError("yeni stop seviyesi bilinmiyor")
                if pos.status is PositionStatus.OPEN:
                    q = await self._live_quote(broker, trade)
                    long = pos.side is Side.LONG
                    if q is not None and ((long and sl >= q.bid) or (not long and sl <= q.ask)):
                        raise BrokerError(
                            f"yeni stop ({fmt_price(sl)}) anlık fiyatın ({fmt_price(q.bid if long else q.ask)}) "
                            "yanlış tarafında; stop taşınmadı"
                        )
                new = await broker.update_stop_loss(pos, sl)
                text = f"{esc(name)}: stop {fmt_price(sl)}"
            elif plan.action is ActionType.MOVE_TP:
                # Never re-arm a level that already filled on THIS position (the rest is split over the others).
                done = _match_levels([tp.price for tp in plan.take_profits], filled_tp_prices(pos))
                levels = [tp for tp, hit in zip(plan.take_profits, done) if not hit]
                if not levels:
                    return BrokerOutcome(name, "skipped",
                                         f"{esc(name)}: hedeflerin hepsi bu hesapta zaten gerçekleşmiş; TP değişmedi")
                tps = shifted_take_profits(scaled_take_profits(levels, mult), offset)
                new = await broker.update_take_profits(pos, tps)
                if virtual:
                    # The first target of a waiting virtual entry changed: whether the price has been between
                    # the entry and it is found out again, from now on (see _virtual_check).
                    new.data["virtual_tp_armed"] = False
                    new.data["virtual_tp_ms"] = int(self._now().timestamp() * 1000)
                text = f"{esc(name)}: TP " + " / ".join(fmt_price(tp.price) for tp in tps)
                if len(levels) < len(plan.take_profits):
                    text += " (gerçekleşen hedef tekrar konmadı)"
            elif plan.action is ActionType.CLOSE:
                frac = plan.close_fraction if plan.close_fraction is not None else 1.0
                if pos.status is PositionStatus.PENDING:
                    if frac < 0.999:
                        return BrokerOutcome(name, "skipped", f"{esc(name)}: giriş dolmadı; kısmi kapatma yapılmadı")
                    new = await broker.cancel_entry(pos)
                    text = f"{esc(name)}: " + ("sanal tetik iptal edildi" if virtual else "bekleyen giriş iptal edildi")
                else:
                    # Signal fraction is of what is still open; the adapter wants a fraction of the filled size.
                    broker_frac = 1.0
                    if frac < 0.999 and pos.filled_quantity > 0 and pos.remaining_quantity > 0:
                        broker_frac = min(1.0, frac * pos.remaining_quantity / pos.filled_quantity)
                    elif frac < 0.999:
                        broker_frac = frac
                    new = await broker.close_position(pos, broker_frac)
                    text = f"{esc(name)}: {'tamamı' if frac >= 0.999 else fmt.fmt_pct(frac * 100)} kapatıldı"
            else:  # CANCEL
                if pos.status is PositionStatus.PENDING:
                    new = await broker.cancel_entry(pos)
                    text = f"{esc(name)}: " + ("sanal tetik iptal edildi" if virtual else "bekleyen giriş iptal edildi")
                else:
                    new = await broker.close_position(pos, 1.0)
                    text = f"{esc(name)}: pozisyon kapatıldı"
            await self.db.save_broker_position(trade.id, new)
            if virtual and new.status is PositionStatus.CANCELLED:
                self._virtual_forget(trade.id, name)
                if not self.config.notifier.notify_on_execution:
                    # Otherwise nothing would tell the owner that this account will not take the trade any more.
                    await self._notify(f"🧭 #{trade.id} {esc(trade.plan.base_asset)} "
                                       f"{trade.plan.side.value.upper()}: {text}", Severity.INFO)
            if new.realized_pnl is not None and new.status not in ACTIVE_POSITION:
                text += f" (K/Z {new.realized_pnl:+.2f})"
            if await self._report_position_problem(trade, new, pos.error):
                # Applied, but the adapter reported a problem (some legs failed, TP not placed, no stop...).
                text += f" 🚨 <code>{esc(new.error or protection_problem(new) or '')}</code>"
                return BrokerOutcome(name, "partial", text, new)
            return BrokerOutcome(name, "ok", text, new)
        except BrokerError as exc:
            text = f"{esc(name)}: <code>{esc(exc)}</code>"
            await self._notify(f"#{trade.id} {text}", Severity.ERROR)
            return BrokerOutcome(name, "error", text)
        except asyncio.CancelledError:
            if not stray_cancel():
                raise           # this task itself is being cancelled
            # A call into the broker's library was cancelled from the inside: this broker's failure only; the
            # other brokers of the trade are still managed.
            logger.error("%s: a broker call was cancelled without a cancel request while managing trade %s",
                         name, trade.id)
            text = (f"{esc(name)}: broker bağlantısı yanıt vermeden kesildi; değişikliğin uygulanıp uygulanmadığını "
                    "brokerda elle kontrol edin")
            await self._notify(f"#{trade.id} {text}", Severity.ERROR)
            return BrokerOutcome(name, "error", text)
        except Exception as exc:
            logger.exception("%s: unexpected error managing trade %s", name, trade.id)
            text = f"{esc(name)}: beklenmeyen hata <code>{esc(exc)}</code>"
            await self._notify(f"#{trade.id} {text}", Severity.ERROR)
            return BrokerOutcome(name, "error", text)

    # ------------------------------------------------------------------ confirmations
    async def _new_conf_id(self) -> str:
        while True:
            conf_id = secrets.token_hex(3)
            if conf_id not in self._conf_locks and await self.db.get_confirmation(conf_id) is None:
                return conf_id

    async def _ask_eval(self, ev: Evaluation, chat_id: int, *, conf_id: str | None = None) -> PendingConfirmation:
        return await self._ask(ev.kind, ev.action, ev.plan, ev.issues, ev.market_price, chat_id,
                               ev.action.used_message_ids, conf_id=conf_id)

    async def _ask(
        self,
        kind: ConfKind,
        action: ParsedAction,
        plan: TradePlan | ManagementPlan | None,
        issues: list[ValidationIssue],
        market_price: float | None,
        chat_id: int,
        source_ids: list[int],
        *,
        conf_id: str | None = None,
    ) -> PendingConfirmation:
        now = self._now()
        conf_id = conf_id or await self._new_conf_id()
        conf = PendingConfirmation(
            id=conf_id, kind=kind, action=action, plan=plan, issues=list(issues), market_price=market_price,
            source_chat_id=chat_id, source_message_ids=list(source_ids), created_at=now,
            expires_at=now + timedelta(seconds=self.config.notifier.confirm_timeout_sec),
        )
        async with self._conf_lock(conf_id):   # a very fast tap waits until the message id is stored
            await self.db.save_confirmation(conf)
            try:
                msg_id = await self.notifier.ask_confirmation(conf)
            except Exception:
                logger.exception("ask_confirmation failed for %s", conf_id)
                msg_id = None
            if msg_id is None:
                logger.error("Confirmation %s could not be delivered to the owner", conf_id)
            conf.bot_message_id = msg_id
            await self.db.save_confirmation(conf)
        self._arm_expiry(conf)
        await self._log("confirmation_asked", {
            "id": conf_id, "kind": kind, "chat_id": chat_id, "message_ids": list(source_ids),
            "issues": [i.code for i in issues], "has_plan": plan is not None,
        })
        logger.info("Asked owner (%s %s): %s", kind, conf_id, [i.code for i in issues])
        return conf

    def _arm_expiry(self, conf: PendingConfirmation) -> None:
        self._disarm_expiry(conf.id)
        task = self._spawn(self._expire_later(conf.id, utc(conf.expires_at)), f"expire-{conf.id}")
        self._expiry_tasks[conf.id] = task

    def _disarm_expiry(self, conf_id: str) -> None:
        task = self._expiry_tasks.pop(conf_id, None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    async def _expire_later(self, conf_id: str, at: datetime) -> None:
        await asyncio.sleep(max(0.0, (at - self._now()).total_seconds()))
        if self._expiry_tasks.get(conf_id) is asyncio.current_task():
            del self._expiry_tasks[conf_id]
        await self._expire(conf_id)

    async def _expire(self, conf_id: str) -> None:
        async with self._conf_lock(conf_id):
            conf = await self.db.get_confirmation(conf_id)
            if conf is None or conf.status is not ConfirmationStatus.PENDING:
                return
            if utc(conf.expires_at) > self._now() + timedelta(seconds=0.5):
                self._arm_expiry(conf)     # extended meanwhile (correction requested)
                return
            conf.status = ConfirmationStatus.EXPIRED
            conf.awaiting_correction = False
            await self.db.save_confirmation(conf)
            await self._record_confirmation_outcome(conf)
            text = ("⌛ Süre doldu, değişiklik uygulanmadı." if conf.kind == "manage"
                    else "⌛ Süre doldu, işlem açılmadı.")
            try:
                await self.notifier.close_confirmation(conf, text)
            except Exception:
                logger.exception("close_confirmation failed for %s", conf_id)
            await self._log("confirmation_expired", {"id": conf_id})

    async def _finish(self, conf: PendingConfirmation, status: ConfirmationStatus, text: str, *,
                      refused: str | None = None) -> None:
        """``refused``: the owner approved but the bot itself refused (paused, limit, price...): stored as
        REJECTED, remembered per message as "auto_refused" with this reason (not as the owner's no)."""
        conf.status = status
        conf.awaiting_correction = False
        await self.db.save_confirmation(conf)
        await self._record_confirmation_outcome(conf, ("auto_refused", refused) if refused else None)
        self._disarm_expiry(conf.id)
        try:
            await self.notifier.close_confirmation(conf, text)
        except Exception:
            logger.exception("close_confirmation failed for %s", conf.id)
        await self._log("confirmation_" + status.value, {"id": conf.id, "result": text})

    async def handle_confirmation(self, conf_id: str, approved: bool) -> str:
        if approved and not self._accepting:
            return SHUTDOWN_TEXT     # stays pending: restored (with its expiry) after the restart
        async with self._conf_lock(conf_id):
            conf = await self.db.get_confirmation(conf_id)
            if conf is None:
                return "Bu onay bulunamadı."
            if conf.status is not ConfirmationStatus.PENDING:
                return f"Bu onay artık geçerli değil ({_CONF_STATUS_TR.get(conf.status, conf.status.value)})."
            if utc(conf.expires_at) <= self._now():
                text = "⌛ Bu onayın süresi dolmuş; işlem yapılmadı."
                await self._finish(conf, ConfirmationStatus.EXPIRED, text)
                return text
            if not approved:
                text = "❌ İptal edildi; işlem yapılmadı."
                await self._finish(conf, ConfirmationStatus.REJECTED, text)
                return text
            if conf.plan is None:
                return "Önce düzeltme gerekli: ✏️ Düzelt'e basıp doğrusunu yazın."
            if isinstance(conf.plan, TradePlan):
                return await self._approve_open(conf, conf.plan)
            return await self._approve_management(conf, conf.plan)

    async def _approve_open(self, conf: PendingConfirmation, plan: TradePlan) -> str:
        if await self._is_paused():
            text = "⏸ Sistem duraklatılmış; işlem açılmadı. (/devam yazıp sinyali tekrar değerlendirin.)"
            await self._finish(conf, ConfirmationStatus.REJECTED, text, refused="sistem duraklatılmıştı")
            return text
        async with self._holding_open_lock(plan.summary_tr or "onaylanan sinyal"):
            if not self._accepting:
                return SHUTDOWN_TEXT
            limit = self.config.risk.max_open_trades
            active = await self.db.active_trades()
            if guards.max_open_trades_reached(active, limit):
                text = (f"⛔ Açık işlem sınırına ({limit}) ulaşıldı; işlem açılmadı.\n"
                        f"{self._limit_filled_by(active)}")
                await self._finish(conf, ConfirmationStatus.REJECTED, text,
                                   refused=f"açık işlem sınırı ({limit}) doluydu")
                return text
            # Re-check existing trades like the automatic path does: since the question was asked, the
            # same signal may have been executed (a repost, or a reply built on the same message).
            known = {(i.code, i.message_tr) for i in conf.issues}
            fresh = [i for i in await self._trade_issues(plan, conf.source_chat_id)
                     if (i.code, i.message_tr) not in known]
            if fresh:
                return await self._reask_open(conf, plan, fresh)
            problem = await self._approval_price_problem(plan, conf.action.symbol)
            if problem:
                text = f"⛔ {esc(problem)} — işlem açılmadı."
                await self._finish(conf, ConfirmationStatus.REJECTED, text, refused=problem[:1].lower() + problem[1:])
                return text
            # Mark first: a crash during execution must never leave it approvable again.
            conf.status = ConfirmationStatus.APPROVED
            conf.awaiting_correction = False
            await self.db.save_confirmation(conf)
            self._disarm_expiry(conf.id)
            try:
                result = await self.execute_open(plan, raw_symbol=conf.action.symbol)
            except Exception as exc:
                logger.exception("execute_open failed for confirmation %s", conf.id)
                result = f"🚨 İşlem açılırken hata: <code>{esc(exc)}</code>"
        await self._finish(conf, ConfirmationStatus.APPROVED, result)
        return result

    async def _reask_open(self, conf: PendingConfirmation, plan: TradePlan, fresh: list[ValidationIssue]) -> str:
        """Something new conflicts with an approved signal: do not execute; ask again showing it."""
        new_id = await self._new_conf_id()
        reasons = " ".join(i.message_tr for i in fresh)
        text = (f"♻️ Onay beklerken durum değişti: {esc(reasons)} İşlem açılmadı; yeni onay #{new_id} gönderildi "
                "(yine de açılsın istiyorsanız orada onaylayın).")
        await self._finish(conf, ConfirmationStatus.SUPERSEDED, text)
        market = await self._price(plan.base_asset, plan.quote_asset, plan.market_type, conf.action.symbol)
        # Questions about other pending confirmations are re-checked: the one it named may be resolved now.
        kept = [i for i in conf.issues if i.code not in _PENDING_CODES]
        pending = await self._pending_issues(plan, conf.source_chat_id, conf.id)
        ev = Evaluation("open", conf.action, plan, merge_issues(fresh, kept, pending),
                        market if market is not None else conf.market_price)
        await self._ask(ev.kind, ev.action, ev.plan, ev.issues, ev.market_price, conf.source_chat_id,
                        list(conf.source_message_ids), conf_id=new_id)
        await self._log("approval_reasked", {"id": conf.id, "new_id": new_id, "issues": [i.code for i in fresh]})
        return text

    async def _approval_price_problem(self, plan: TradePlan, raw_symbol: str | None) -> str | None:
        price = await self._price(plan.base_asset, plan.quote_asset, plan.market_type, raw_symbol, fresh=True)
        if price is None:
            return None      # each broker re-checks its own live quote before ordering
        q = Quote(bid=price, ask=price, last=price)
        tp1 = plan.take_profits[0].price if plan.take_profits else None
        if plan.entry_type is EntryType.LIMIT and plan.entry_price:
            # A resting limit (long: the entry is below the price, short: above it) is checked against its stop
            # only, like _open_on_broker does: its first target may well lie between the entry and today's price
            # (a pullback limit). A limit that is already marketable is filled at once: checked like a market order.
            resting = price > plan.entry_price if plan.side is Side.LONG else price < plan.entry_price
            if resting:
                tp1 = None
        problem = _price_problem(plan.side, q, plan.stop_loss, tp1)
        return problem[0].upper() + problem[1:] if problem else None

    async def _approve_management(self, conf: PendingConfirmation, plan: ManagementPlan) -> str:
        trade = await self.db.get_trade(plan.trade_id)
        if trade is None or trade.status not in ACTIVE_TRADE:
            text = f"⛔ #{plan.trade_id} işlemi artık aktif değil; değişiklik uygulanmadı."
            if trade is not None and self._ends_trade(plan.action, plan.close_fraction):
                text += self._late_close_note(self._close_late_legs(trade.id, "trader işlemi kapattı / iptal etti"))
            await self._finish(conf, ConfirmationStatus.REJECTED, text,
                               refused=f"#{plan.trade_id} işlemi artık aktif değildi")
            return text
        conf.status = ConfirmationStatus.APPROVED
        await self.db.save_confirmation(conf)
        self._disarm_expiry(conf.id)
        try:
            result = await self.execute_management(plan)
        except Exception as exc:
            logger.exception("execute_management failed for confirmation %s", conf.id)
            result = f"🚨 Uygulanırken hata: <code>{esc(exc)}</code>"
        await self._finish(conf, ConfirmationStatus.APPROVED, result)
        return result

    async def request_correction(self, conf_id: str) -> str:
        if not self._accepting:
            return SHUTDOWN_TEXT
        # Only one confirmation waits for a correction text at a time.
        for other in await self.db.pending_confirmations():
            if other.awaiting_correction and other.id != conf_id:
                async with self._conf_lock(other.id):
                    fresh = await self.db.get_confirmation(other.id)
                    if fresh is not None and fresh.awaiting_correction:
                        fresh.awaiting_correction = False
                        await self.db.save_confirmation(fresh)
        async with self._conf_lock(conf_id):
            conf = await self.db.get_confirmation(conf_id)
            if conf is None or conf.status is not ConfirmationStatus.PENDING:
                return "Bu onay artık geçerli değil."
            conf.awaiting_correction = True
            # Give the owner time to type.
            min_expiry = self._now() + timedelta(seconds=self.config.notifier.confirm_timeout_sec)
            if utc(conf.expires_at) < min_expiry:
                conf.expires_at = min_expiry
                self._arm_expiry(conf)
            await self.db.save_confirmation(conf)
        return f"✏️ Onay #{esc(conf_id)} için düzeltmenizi bekliyorum."

    async def awaiting_correction_id(self) -> str | None:
        found = [c for c in await self.db.pending_confirmations() if c.awaiting_correction]
        return found[-1].id if found else None

    async def handle_correction(self, conf_id: str, correction_text: str) -> str:
        text = (correction_text or "").strip()
        if not text:
            return "Düzeltme metni boş."
        if not self._accepting:
            return SHUTDOWN_TEXT
        async with self._conf_lock(conf_id):
            conf = await self.db.get_confirmation(conf_id)
            if conf is None or conf.status is not ConfirmationStatus.PENDING:
                status = _CONF_STATUS_TR.get(conf.status, conf.status.value) if conf else "bulunamadı"
                return f"Bu onay artık geçerli değil ({status})."
            msgs = await self._source_messages(conf)
            reply_to = await self._reply_context(conf.source_chat_id, msgs)
            route, title = await self._conf_route(conf, msgs)
            ctx = await self._parse_context(route, title, reply_to, correction_text=text,
                                            previous_action=conf.action)
            try:
                result = await self.parser.parse(msgs, ctx)
            except Exception as exc:
                logger.warning("Correction parse failed: %s", exc)
                return f"⚠️ Düzeltme okunamadı ({esc(exc)}). Lütfen tekrar yazın."
            actions = [a for a in result.actions if a.action is not ActionType.NONE]
            if not actions:
                out = "❌ Düzeltmeye göre işlem talimatı yok; işlem yapılmadı."
                await self._finish(conf, ConfirmationStatus.REJECTED, out)
                return out
            action = actions[0]
            if not action.used_message_ids:
                action = action.model_copy(update={"used_message_ids": list(conf.source_message_ids)})
            ev = await self.evaluate(action, conf.source_chat_id, reply_to=reply_to, exclude_conf=conf.id,
                                     route=route)
            assert ev is not None
            ev.issues = merge_issues(ev.issues, self._topic_unknown_issues(ev, route))
            if len(actions) > 1:
                ev.issues.append(_issue("multiple_actions",
                                        "Düzeltmede birden fazla talimat var; yalnızca ilki gösteriliyor."))
            new_id = await self._new_conf_id()
            await self._finish(conf, ConfirmationStatus.SUPERSEDED, f"♻️ Düzeltildi → yeni onay #{new_id}")
        new = await self._ask_eval(ev, conf.source_chat_id, conf_id=new_id)
        if new.plan is None:
            return f"♻️ Düzeltme işlendi ama hâlâ eksik bilgi var (onay #{new.id}). ✏️ Düzelt ile tekrar yazın."
        return f"♻️ Düzeltme işlendi; yeni onay gönderildi (#{new.id}). Doğruysa ✅ Onayla'ya basın."

    async def _source_messages(self, conf: PendingConfirmation) -> list[StoredMessage]:
        msgs = await self._load_messages(conf.source_chat_id, conf.source_message_ids)
        if not msgs:   # deleted / never stored: give the parser what we understood
            msgs = [StoredMessage(chat_id=conf.source_chat_id, message_id=0, date=utc(conf.created_at),
                                  text=conf.action.summary_tr or "", consumed=True)]
        return msgs

    # ------------------------------------------------------------------ edits
    async def on_message_edited(self, msg: StoredMessage) -> None:
        if not self._accepting:
            logger.info("Shutting down: edit of %s/%s not processed", msg.chat_id, msg.message_id)
            return
        try:
            # A batch of this sender may be parsing / executing this very message right now. Wait for it,
            # so the lookups below see the trade or confirmation it created and the edit is never lost.
            async with self._sender_lock(stream_key(msg.chat_id, msg.sender_id, msg.topic_id)):
                fresh = await self.db.get_message(msg.chat_id, msg.message_id)
            current = fresh or msg
            trade = await self.db.find_trade_by_message(msg.chat_id, msg.message_id)
            if trade is not None:
                if trade.status in ACTIVE_TRADE:
                    await self._edited_trade_signal(trade, current)
                return
            for conf in await self.db.pending_confirmations():
                if conf.source_chat_id == msg.chat_id and msg.message_id in conf.source_message_ids:
                    await self._edited_confirmation(conf.id)
                    return
            if fresh is not None and not fresh.consumed:
                await self.on_messages_ready(msg.chat_id, msg.sender_id, current.topic_id)
                return
            await self._edited_consumed_message(current)
        except Exception as exc:
            logger.exception("on_message_edited failed for %s/%s", msg.chat_id, msg.message_id)
            await self._notify(f"Düzenlenen mesaj işlenemedi: <code>{esc(exc)}</code>", Severity.ERROR)

    async def _edited_consumed_message(self, msg: StoredMessage) -> None:
        """An already handled message with no trade and no pending question was edited (e.g. read as
        "none" or dropped by the prefilter, then edited into a signal). Read the current version again;
        anything it now says is ASKED, never executed automatically."""
        age = (self._now() - utc(msg.date)).total_seconds()
        if age > EDIT_REPROCESS_MAX_AGE_SEC:
            logger.info("Edit of old message %s/%s (%.0fs) ignored", msg.chat_id, msg.message_id, age)
            return
        route, title = self._route_of([msg])
        if self.config.telegram.topics and route is None:
            logger.info("Edit of %s/%s in a topic that is not followed (%r) ignored", msg.chat_id, msg.message_id,
                        title)
            return
        if not await self._worth_reading([msg], route):
            logger.debug("Edit of %s/%s: prefilter says not relevant", msg.chat_id, msg.message_id)
            return
        reply_to = await self._reply_context(msg.chat_id, [msg])
        ctx = await self._parse_context(route, title, reply_to)
        try:
            result = await self.parser.parse([msg], ctx)
        except Exception as exc:
            await self._notify(
                f"Daha önce işlem talimatı sayılmayan bir mesaj düzenlendi ama okunamadı ({esc(exc)}).\n"
                f"Mesaj: <i>{esc(_excerpt([msg]))}</i>\nİşlem yapılmadı; gerekiyorsa elle kontrol edin.",
                Severity.WARNING,
            )
            return
        # A management instruction this message already ran (automatically or approved): the edit is
        # usually a typo fix. The same reading again is ignored; a different one is asked, saying clearly
        # that the original was already applied. Nothing is ever re-applied automatically.
        applied = await self._applied_records(msg.chat_id, [msg.message_id])
        applied_text = "; ".join(self._applied_text(r) for r in applied)
        actions = self._fill_used_ids([a for a in result.actions if a.action is not ActionType.NONE],
                                      [msg.message_id])
        if not actions:
            logger.info("Edit of %s/%s is still not a trading instruction", msg.chat_id, msg.message_id)
            if applied:
                await self._notify(
                    "✏️ Daha önce <b>uygulanmış</b> bir talimatın mesajı düzenlendi; yeni hali işlem "
                    f"talimatı içermiyor.\nUygulanan: {esc(applied_text)}\nMesaj: <i>{esc(_excerpt([msg]))}</i>\n"
                    "Uygulanan değişiklik geri alınmadı; gerekiyorsa elle kontrol edin.",
                    Severity.WARNING,
                )
            return
        edited = _issue("edited_after_read",
                        "Daha önce işlenmiş bir mesaj sonradan düzenlendi; yeni hali otomatik uygulanmaz, size "
                        "soruluyor.")
        await self._log("edited_reparsed", {"chat_id": msg.chat_id, "message_id": msg.message_id,
                                            "actions": [a.model_dump(mode="json") for a in actions],
                                            "applied": [r.get("plan_id") for r in applied]})
        for action in actions:
            if action.action is ActionType.OPEN and await self._is_paused():
                await self._notify(f"Duraklatıldı: düzenlenen sinyal atlandı ({esc(action.summary_tr or '?')})",
                                   Severity.INFO)
                continue
            ev = await self.evaluate(action, msg.chat_id, reply_to=reply_to, route=route)
            if ev is None:
                continue
            if ev.ignored is not None:
                logger.info("Edit of %s/%s: %s; ignored", msg.chat_id, msg.message_id, ev.ignored)
                continue
            if applied:
                same = [r for r in applied if self._same_as_applied(r, action, ev.plan)]
                if same:
                    logger.info("Edit of %s/%s reads the instruction already applied (%s) again; ignored",
                                msg.chat_id, msg.message_id, self._applied_text(same[0]))
                    await self._log("edited_applied_unchanged", {
                        "chat_id": msg.chat_id, "message_id": msg.message_id,
                        "plan_id": same[0].get("plan_id"), "action": action.model_dump(mode="json"),
                    })
                    continue
                plan_summary = ev.plan.summary_tr if isinstance(ev.plan, (TradePlan, ManagementPlan)) else ""
                new_text = plan_summary or action.summary_tr or action.action.value
                note = _issue(
                    "edited_after_applied",
                    f"Bu mesajdaki talimat ZATEN UYGULANDI: {applied_text}. Mesaj sonradan düzenlendi; yeni "
                    f"okuma: {new_text}. Onaylarsanız yeni okuma AYRICA uygulanır; ilk uygulanan geri alınmaz.",
                )
                ev.issues = merge_issues([note], [i for i in ev.issues if i.code != "message_already_applied"])
            else:
                ev.issues = merge_issues([edited], ev.issues)
            await self._ask_eval(ev, msg.chat_id)

    async def _edited_trade_signal(self, trade: TradeRecord, msg: StoredMessage) -> None:
        """Never auto-apply edits: build MOVE_SL / MOVE_TP proposals and ask."""
        chat_id = trade.plan.source_chat_id
        msgs = await self._load_messages(chat_id, trade.plan.source_message_ids)
        if all(m.message_id != msg.message_id for m in msgs):
            msgs.append(msg)
        reply_to = await self._reply_context(chat_id, msgs)
        route = self._route_named(trade.plan.route)
        title = self._route_of(msgs)[1]
        ctx = await self._parse_context(route, title, reply_to)
        try:
            result = await self.parser.parse(msgs, ctx)
        except Exception as exc:
            await self._notify(f"#{trade.id} sinyal mesajı düzenlendi ama okunamadı: {esc(exc)}. Elle kontrol edin.",
                               Severity.WARNING)
            return
        new_sl: float | None = None
        new_tps: list[TakeProfit] | None = None
        confidence = 1.0
        for a in result.actions:
            if a.action in (ActionType.OPEN, ActionType.INCOMPLETE):
                base, _q, _t = asset_of(a)
                if (a.side is not None and a.side is not trade.plan.side) or (base and base != trade.plan.base_asset):
                    await self._notify(
                        f"#{trade.id} sinyal mesajı düzenlendi: varlık/yön değişmiş görünüyor "
                        f"({esc(a.summary_tr)}). Açık işleme otomatik uygulanamaz; elle kontrol edin.",
                        Severity.WARNING,
                    )
                    continue
                if a.stop_loss is not None:
                    new_sl = a.stop_loss
                if a.take_profits:
                    new_tps = list(a.take_profits)
                confidence = min(confidence, a.confidence)
            elif a.action is ActionType.MOVE_SL and a.target_trade_id in (None, trade.id):
                new_sl = a.new_stop_loss if not a.move_sl_to_entry else trade.plan.entry_price
                confidence = min(confidence, a.confidence)
            elif a.action is ActionType.MOVE_TP and a.target_trade_id in (None, trade.id) and a.take_profits:
                new_tps = list(a.take_profits)
                confidence = min(confidence, a.confidence)

        edited = _issue("signal_edited", "Sinyal mesajı düzenlendi.")
        proposals: list[ParsedAction] = []
        common = dict(base_asset=trade.plan.base_asset, side=trade.plan.side, target_trade_id=trade.id,
                      refers_to_message_id=msg.message_id, used_message_ids=[msg.message_id], confidence=confidence)
        if new_sl is not None and not _same_price(new_sl, trade.current_stop_loss):
            proposals.append(ParsedAction(
                action=ActionType.MOVE_SL, new_stop_loss=new_sl,
                summary_tr=f"Sinyal düzenlendi: {trade.plan.base_asset} stop {fmt_price(trade.current_stop_loss)} → "
                           f"{fmt_price(new_sl)}",
                **common,
            ))
        old_prices = sorted({tp.price for tp in trade.current_take_profits})
        if new_tps is not None and sorted({tp.price for tp in new_tps}) != old_prices:
            proposals.append(ParsedAction(
                action=ActionType.MOVE_TP, take_profits=new_tps,
                summary_tr=f"Sinyal düzenlendi: {trade.plan.base_asset} hedefler → "
                           + " / ".join(fmt_price(tp.price) for tp in new_tps),
                **common,
            ))
        if not proposals:
            logger.info("Edit of %s/%s does not change trade #%s levels", msg.chat_id, msg.message_id, trade.id)
            return
        for action in proposals:
            ev = await self._evaluate_management(action, chat_id, None, route=route)
            ev.issues = merge_issues([edited], ev.issues)
            await self._ask_eval(ev, chat_id)

    async def _edited_confirmation(self, conf_id: str) -> None:
        async with self._conf_lock(conf_id):
            conf = await self.db.get_confirmation(conf_id)
            if conf is None or conf.status is not ConfirmationStatus.PENDING:
                return
            msgs = await self._source_messages(conf)
            reply_to = await self._reply_context(conf.source_chat_id, msgs)
            route, title = await self._conf_route(conf, msgs)
            ctx = await self._parse_context(route, title, reply_to)
            try:
                result = await self.parser.parse(msgs, ctx)
            except Exception as exc:
                await self._notify(f"Onay #{esc(conf_id)}: kaynak mesaj düzenlendi ama okunamadı ({esc(exc)}).",
                                   Severity.WARNING)
                return
            actions = [a for a in result.actions if a.action is not ActionType.NONE]
            if not actions:
                await self._finish(conf, ConfirmationStatus.SUPERSEDED,
                                   "♻️ Mesaj düzenlendi; artık işlem talimatı görünmüyor — işlem yapılmadı.")
                return
            action = actions[0]
            if not action.used_message_ids:
                action = action.model_copy(update={"used_message_ids": list(conf.source_message_ids)})
            ev = await self.evaluate(action, conf.source_chat_id, reply_to=reply_to, exclude_conf=conf.id,
                                     route=route)
            assert ev is not None
            ev.issues = merge_issues([_issue("signal_edited", "Sinyal mesajı düzenlendi.")], ev.issues,
                                     self._topic_unknown_issues(ev, route))
            new_id = await self._new_conf_id()
            await self._finish(conf, ConfirmationStatus.SUPERSEDED, f"♻️ Mesaj düzenlendi → yeni onay #{new_id}")
        await self._ask_eval(ev, conf.source_chat_id, conf_id=new_id)

    # ------------------------------------------------------------------ owner commands
    async def status_text(self) -> str:
        trades = await self.db.active_trades()
        pending = await self.db.pending_confirmations()
        mode = "🧪 DRY-RUN (gerçek emir gönderilmez)" if self.config.dry_run else "🔴 CANLI"
        paused = await self._is_paused()
        lines = [
            "📊 <b>Durum</b>",
            f"Mod: {mode}",
            "Yeni işlemler: " + ("⏸ duraklatıldı" if paused else "▶️ kopyalanıyor"),
        ]
        broker_bits = [f"✅ {esc(b.name)}" for b in self.brokers]
        broker_bits += [f"❌ {esc(n)}" + (" (yeniden deneniyor)" if n in self._retrying else "")
                        for n in self.failed_brokers]
        lines.append("Brokerlar: " + (", ".join(broker_bits) or "yok"))
        if self.config.telegram.topics:
            lines.append(self._routes_text())
        n_open = sum(1 for t in trades if t.status is TradeStatus.OPEN)
        n_pending = sum(1 for t in trades if t.status is TradeStatus.PENDING)
        n_virtual = sum(1 for _tid, p in await self.db.active_broker_positions() if is_virtual_pending(p))
        lines.append(f"Aktif işlem: {len(trades)} (açık {n_open}, bekleyen emir {n_pending})"
                     + (f" · sanal tetik bekleyen {n_virtual}" if n_virtual else ""))
        lines.append(f"Bekleyen onay: {len(pending)}")
        lines.append(f"Risk: 1R = {esc(self._risk_per_r_text())}")
        return "\n".join(lines)

    async def positions_text(self) -> str:
        trades = await self.db.active_trades()
        if not trades:
            return "📂 Açık işlem yok."
        lines = [f"📂 <b>Açık işlemler</b> ({len(trades)})"]
        for t in trades:
            lines.append("")
            lines.append(fmt.format_trade_line(t) + (f" · 📁 {esc(t.plan.route)}" if t.plan.route else ""))
            hit, _share = tp_progress(t, await self.db.broker_positions(t.id))
            tps = " / ".join(fmt_price(tp.price) + (" ✔" if h else "")
                             for tp, h in zip(t.current_take_profits, hit)) or "yok"
            price = await self._price(t.plan.base_asset, t.plan.quote_asset, t.plan.market_type)
            lines.append(f"🎯 TP: {tps}" + (f" · 💹 anlık {fmt_price(price)}" if price else ""))
            for p in await self.db.broker_positions(t.id):
                if is_virtual_pending(p):
                    ref_id = str(p.data.get("reference_exchange") or "")
                    trig = p.data.get("trigger_price")
                    mult = price_multiplier(p.data)
                    bits = [f"• {esc(p.broker)}: {pos_status_text(p)}",
                            f"{esc(ref_label(ref_id))} fiyatı {fmt_price(trig / mult if trig else None)} seviyesine değince"]
                else:
                    bits = [f"• {esc(p.broker)}: {_POS_STATUS_TR.get(p.status, p.status.value)}",
                            f"{_qty(p.remaining_quantity if p.status is PositionStatus.OPEN else p.quantity)}"]
                if p.avg_entry_price:
                    bits.append(f"giriş {fmt_price(p.avg_entry_price)}")
                if p.stop_loss is not None:
                    bits.append(f"stop {fmt_price(p.stop_loss)}")
                cs = p.data.get("contract_size")
                if (price and p.avg_entry_price and p.status is PositionStatus.OPEN
                        and isinstance(cs, (int, float)) and cs > 0):
                    direction = 1.0 if p.side is Side.LONG else -1.0
                    upnl = (price - p.avg_entry_price) * p.remaining_quantity * float(cs) * direction
                    bits.append(f"~K/Z {upnl:+.2f}")
                if p.dry_run:
                    bits.append("dry-run")
                lines.append(" · ".join(bits))
            for leg in self._late_legs_of(t.id):
                lines.append(f"• {esc(leg.broker)}: ⏳ açılış sürüyor, broker henüz yanıt vermedi")
        return "\n".join(lines)

    async def balances_text(self) -> str:
        lines = ["💰 <b>Bakiyeler</b>"]

        async def one(b: Broker) -> str:
            try:
                equity = await asyncio.wait_for(b.get_equity(), READ_TIMEOUT_SEC)
                free = await asyncio.wait_for(b.get_free_margin(), READ_TIMEOUT_SEC)
                ccy = self._currency.get(b.name) or await asyncio.wait_for(b.account_currency(), READ_TIMEOUT_SEC)
                tag = " <i>(dry-run)</i>" if b.kind.startswith("dry-run") else ""
                return f"• {esc(b.name)}: {_money(equity)} {esc(ccy)} (serbest {_money(free)}){tag}"
            except asyncio.CancelledError:
                if not stray_cancel():
                    raise
                return f"• {esc(b.name)}: okunamadı (broker bağlantısı yanıt vermiyor)"
            except Exception as exc:
                return f"• {esc(b.name)}: okunamadı <code>{esc(exc) or type(exc).__name__}</code>"

        lines += await asyncio.gather(*(one(b) for b in self.brokers))
        lines += [f"• {esc(n)}: ❌ bağlı değil" for n in self.failed_brokers]
        if len(lines) == 1:
            lines.append("Bağlı broker yok.")
        return "\n".join(lines)

    async def set_paused(self, paused: bool) -> str:
        await self.db.set_state("paused", bool(paused))
        await self._log("paused" if paused else "resumed", {})
        if paused:
            return "⏸ Yeni işlem açma durduruldu. Açık işlemler yönetilmeye devam eder."
        return "▶️ Yeni sinyaller tekrar kopyalanacak."

    async def close_all(self) -> str:
        if not self._accepting:
            return SHUTDOWN_TEXT
        trades = await self.db.active_trades()
        # Opens no broker has answered yet, also of trades that are not active (any more): closed on arrival.
        self._close_late_legs(None, "tüm işlemler kapatıldı (/kapat)")
        active_ids = {t.id for t in trades}
        orphans = [
            f"• ⏳ {esc(leg.label)} — {esc(leg.broker)}: açılışa henüz yanıt gelmedi; açılırsa hemen kapatılacak"
            for leg in self._late_legs.values() if leg.trade_id not in active_ids
        ]
        if not trades:
            return "\n".join(["Kapatılacak açık işlem yok.", *orphans])
        lines = ["🔒 <b>Tüm işlemler kapatılıyor</b>"]
        now = self._now()
        for t in trades:
            plan = ManagementPlan(
                plan_id=uuid.uuid4().hex[:10], action=ActionType.CLOSE, trade_id=t.id, close_fraction=1.0,
                source_chat_id=t.plan.source_chat_id, summary_tr="tamamı kapatıldı", created_at=now,
            )
            try:
                lines.append(await self.execute_management(plan, notify=False))
            except Exception as exc:
                logger.exception("close_all: trade %s", t.id)
                lines.append(f"🚨 #{t.id}: <code>{esc(exc)}</code>")
        lines += orphans
        await self._log("close_all", {"trades": [t.id for t in trades]})
        return "\n".join(lines)

    async def test_parse(self, text: str) -> str:
        now = self._now()
        msg = StoredMessage(chat_id=0, message_id=0, sender_id=None, date=now, text=text)
        ctx = ParseContext(now=now, open_trades=await self._open_trade_summaries())
        try:
            result = await self.parser.parse([msg], ctx)
        except Exception as exc:
            return f"⚠️ Okunamadı: {esc(exc)}"
        return await self.describe_actions(result.actions, chat_id=0)

    async def describe_actions(
        self, actions: list[ParsedAction], *, chat_id: int, route: TopicRoute | None = None
    ) -> str:
        """Owner-facing explanation of what would happen (no side effects). ``route``: as if the messages
        came from that forum topic (None: no topic)."""
        parts = ["🧪 <b>Deneme — işlem açılmaz</b>"]
        if route is not None:
            parts.append(f"📁 Konu: {esc(route.name)} → {esc(', '.join(route.brokers) or 'tüm brokerlar')}")
        elif self.config.telegram.topics:
            parts.append("📁 Konu bilgisi yok: tüm brokerlar ve tüm açık işlemler varsayıldı (gerçek mesajlarda "
                         "yalnızca takip edilen konular okunur).")
        if not actions:
            parts.append("Hiçbir talimat çıkmadı.")
        for action in actions:
            parts.append("")
            parts.append(fmt.format_action(action))
            if action.action is ActionType.NONE:
                unsure = self._none_is_unsure(action)
                parts.append("🤔 Emin olunamadığı için size sorulurdu." if unsure else "💬 Yok sayılırdı.")
                continue
            ev = await self.evaluate(action, chat_id, route=route)
            assert ev is not None
            if ev.ignored is not None:
                parts.append(f"💬 Yok sayılırdı: {esc(ev.ignored)}.")
                continue
            if isinstance(ev.plan, TradePlan):
                parts += ["", fmt.format_plan(ev.plan)]
            elif isinstance(ev.plan, ManagementPlan):
                parts += ["", fmt.format_management(ev.plan, ev.trade)]
            if ev.market_price is not None:
                parts.append(f"💹 Güncel fiyat: {fmt_price(ev.market_price)}")
            parts += fmt.format_issues(ev.issues)
            if ev.kind == "incomplete":
                parts.append("🧩 Devamı beklenirdi; gelmezse size sorulurdu.")
            elif ev.plan is None:
                parts.append("⛔ Bu haliyle uygulanamaz; size düzeltme sorulurdu.")
            elif ev.issues:
                parts.append("🤔 Size onay sorulurdu.")
            else:
                parts.append("✅ Otomatik uygulanırdı.")
        if await self._is_paused():
            parts.append("\n⏸ Not: yeni işlem açma şu an duraklatılmış.")
        return "\n".join(parts)

    async def preview_open(self, plan: TradePlan, *, raw_symbol: str | None = None) -> list[str]:
        """Per-broker sizing preview for a plan (read-only: never places orders). Plain text lines.
        Only the plan's route brokers (``plan.broker_names``) when it names any, like execute_open."""
        lines: list[str] = []
        risk = effective_risk(self.config.risk, self._route_named(plan.route))
        brokers = self._plan_brokers(plan)
        if plan.broker_names and not brokers:
            lines.append(f"'{plan.route or '?'}' konusunun brokerı ({', '.join(plan.broker_names)}) bağlı değil; "
                         "işlem başka hesapta açılmaz")
        for b in brokers:
            try:
                if not b.accepts_asset(plan.base_asset):
                    lines.append(f"{b.name}: bu varlığa izin verilmiyor (allowed_assets)")
                    continue
                sym = await b.resolve_symbol(plan.base_asset, plan.quote_asset, plan.market_type, raw_symbol)
                if sym is None:
                    lines.append(f"{b.name}: {plan.base_asset} bu hesapta yok")
                    continue
                equity = await b.get_equity()
                ccy = self._currency.get(b.name) or await b.account_currency()
                quote = await b.get_quote(sym)
                mult = price_multiplier(sym.extra)
                offset, offset_problem = await self._compute_price_offset(b, plan, quote, mult)
                if offset_problem:
                    lines.append(f"{b.name}: {offset_problem} (işlem açılmazdı)")
                    continue
                stop = shift_price(plan.stop_loss * mult, offset)
                signal_entry = shift_price(plan.entry_price * mult, offset) if plan.entry_price else None
                amount = broker_risk_amount(b, equity, plan)
                entry = float(signal_entry or 0.0)
                warnings: list[str] = []
                if plan.entry_type is EntryType.MARKET:      # sized like _open_on_broker does
                    live = quote.entry_price_for(plan.side)
                    entry, capped = market_sizing_entry(
                        plan.side, live, stop, signal_entry,
                        getattr(risk, "max_size_multiple", DEFAULT_MAX_SIZE_MULTIPLE))
                    if capped:
                        warnings.append(size_cap_warning(amount, live, entry, stop, ccy))
                size = await b.size_position(sym, plan.side, entry, stop, amount)
                warnings += size.warnings
                lev = f", kaldıraç {fmt.fmt_number(size.leverage)}x" if size.leverage else ""
                lines.append(
                    f"{b.name}: {sym.broker_symbol} miktar {_qty(size.quantity)}, risk {_money(amount)} {ccy} "
                    f"(stopta ~{_money(size.est_loss_at_sl)}), büyüklük {_money(size.notional)}{lev}, "
                    f"bakiye {_money(equity)} {ccy}"
                    + (f" ({offset_text(offset)})" if offset else "")
                    + (" ⚠️ " + "; ".join(warnings) if warnings else "")
                )
            except asyncio.CancelledError:
                if not stray_cancel():
                    raise
                lines.append(f"{b.name}: broker bağlantısı yanıt vermiyor")
            except Exception as exc:
                lines.append(f"{b.name}: {exc}")
        return lines


__all__ = ["Engine", "Evaluation", "BrokerOutcome", "asset_of", "managed_asset", "merge_issues",
           "is_transient_parse_error", "filled_tp_prices", "tp_progress", "stream_key", "stray_cancel", "near_entry",
           "LEG_DEADLINE_SEC"]
