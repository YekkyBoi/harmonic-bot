"""MetaTrader adapters: shared per-TP "leg" logic + the native MetaTrader5 terminal adapter.

MT4/MT5 positions carry exactly one SL and one TP. A signal with several take
profits is therefore opened as one position ("leg") per TP; all legs share the
stop loss. ``BrokerPosition.data["legs"]`` holds one dict per leg:

    ticket        position ticket (pending legs: the order ticket until filled)
    order_ticket  pending order ticket (None for market legs)
    state         "pending" | "open" | "closed" | "cancelled" | "failed"
    volume        planned lots
    filled        lots filled (original size, before partial closes)
    open_volume   lots still open
    tp            target take profit of this leg (None = no TP)
    tp_indexes    indexes into ``pos.take_profits`` this leg covers (merged tiny shares)
    entry / sl    fill (or pending) price / stop currently on the venue (None = none)
    realized      realized PnL incl. commission + swap, account currency (None = unknown)
    reason        exit reason: "tp" | "sl" | "stop_out" | "manual" | "unknown" | None
    unconfirmed   True while a filled leg has not been seen on the venue yet (its look-up failed right after the
                  fill, or a pending order's history said FILLED before its position could be read: tracked by
                  the position / order ticket, adopted by ticket or comment on a later refresh, closed only when
                  the deals show it fully closed or after _MAX_MISSES refreshes that find nothing)
    rest_order    a partly filled pending order: the leg is the filled part (an open position under the order's
                  ticket) and ``rest_order`` / ``rest_volume`` / ``rest_price`` the rest that still rests; stop and
                  TP moves go to both, a full close cancels the rest first, and a rest left when the filled part
                  has closed is cancelled

``MetaTraderLegBroker`` implements open/modify/close/sync on top of a small set
of venue primitives; ``Mt5Broker`` (here) and ``MetaApiBroker``
(metaapi_broker.py) only implement those primitives.

Netting accounts (one position per symbol) get a single leg: the venue TP is
the LAST take profit and the earlier ones are emulated in ``sync_position``
(partial close once price reaches them).
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import math
import sys
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable, Sequence
from concurrent.futures import Future as ThreadFuture
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, TypeVar

from ..config import Mt5BrokerConfig, RiskConfig
from ..models import (
    BrokerPosition,
    EntryType,
    MarketType,
    OpenRequest,
    PositionStatus,
    Quote,
    Side,
    SizingResult,
    SymbolInfo,
    TakeProfit,
)
from ..parsing.symbols import CFD_QUOTES, FIAT_CURRENCIES
from ..risk.sizing import SizingError, compute_lot_size, floor_to_step, round_to_tick
from .base import Broker, BrokerError, OrderOutcomeUnknownError

logger = logging.getLogger(__name__)

T = TypeVar("T")

_EPS = 1e-9
LIVE_STATES = ("pending", "open")
TERMINAL_STATES = ("closed", "cancelled", "failed")
_MAX_PNL_TRIES = 5          # re-query history this many syncs for a closed leg with unknown PnL
_MAX_MISSES = 3             # an open leg missing from the venue AND its history this many syncs -> closed
_VOL_TOL = 1e-6             # lots: sums of deal volumes vs. a leg's volume (lots have at most 8 decimals)
_DEFAULT_MAX_SIZE_MULTIPLE = 2.0    # RiskConfig.max_size_multiple when the risk config does not carry it


class MT5:
    """MetaTrader5 enum values (fixed by the MQL5 ABI; from MetaTrader5/__init__.py 5.0.x).

    Hardcoded so the adapter does not depend on which constants a (fake) module exports.
    """

    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1
    ORDER_TYPE_BUY_LIMIT = 2
    ORDER_TYPE_SELL_LIMIT = 3
    ORDER_TYPE_BUY_STOP = 4
    ORDER_TYPE_SELL_STOP = 5
    ORDER_STATE_CANCELED = 2
    ORDER_STATE_PARTIAL = 3
    ORDER_STATE_FILLED = 4
    ORDER_STATE_REJECTED = 5
    ORDER_STATE_EXPIRED = 6
    ORDER_FILLING_FOK = 0
    ORDER_FILLING_IOC = 1
    ORDER_FILLING_RETURN = 2
    ORDER_TIME_GTC = 0
    ORDER_TIME_SPECIFIED = 2
    DEAL_TYPE_BUY = 0
    DEAL_TYPE_SELL = 1
    DEAL_ENTRY_IN = 0
    DEAL_ENTRY_OUT = 1
    DEAL_ENTRY_INOUT = 2
    DEAL_ENTRY_OUT_BY = 3
    DEAL_REASON_SL = 4
    DEAL_REASON_TP = 5
    DEAL_REASON_SO = 6
    TRADE_ACTION_DEAL = 1
    TRADE_ACTION_PENDING = 5
    TRADE_ACTION_SLTP = 6
    TRADE_ACTION_MODIFY = 7
    TRADE_ACTION_REMOVE = 8
    SYMBOL_TRADE_MODE_DISABLED = 0
    SYMBOL_TRADE_MODE_LONGONLY = 1
    SYMBOL_TRADE_MODE_SHORTONLY = 2
    SYMBOL_TRADE_MODE_CLOSEONLY = 3
    SYMBOL_TRADE_MODE_FULL = 4
    ACCOUNT_MARGIN_MODE_RETAIL_HEDGING = 2
    # MQL5 bit flags not exported by the Python package
    SYMBOL_FILLING_FOK = 1
    SYMBOL_FILLING_IOC = 2
    SYMBOL_EXPIRATION_SPECIFIED = 4
    SYMBOL_ORDER_LIMIT = 2
    SYMBOL_ORDER_STOP = 4
    # retcodes
    TRADE_RETCODE_REQUOTE = 10004
    TRADE_RETCODE_PLACED = 10008
    TRADE_RETCODE_DONE = 10009
    TRADE_RETCODE_DONE_PARTIAL = 10010
    TRADE_RETCODE_TIMEOUT = 10012
    TRADE_RETCODE_INVALID_STOPS = 10016
    TRADE_RETCODE_PRICE_CHANGED = 10020
    TRADE_RETCODE_PRICE_OFF = 10021
    TRADE_RETCODE_INVALID_EXPIRATION = 10022
    TRADE_RETCODE_TOO_MANY_REQUESTS = 10024
    TRADE_RETCODE_NO_CHANGES = 10025
    TRADE_RETCODE_INVALID_FILL = 10030
    TRADE_RETCODE_CONNECTION = 10031
    # last_error() codes
    RES_S_OK = 1
    RES_E_NOT_FOUND = -4
    RES_E_AUTO_TRADING_DISABLED = -8


_STALE_TICK_SEC = 600           # a tick older than this = market closed (no expiration is set)
_EMPTY_TICK_WAITS = 10          # waits (of tick_wait_sec, 0.3 s) for a tick with a bid and an ask: about 3 s
_NEW_POSITION_POLLS = 10        # look-ups (one per tick_wait_sec) for the position of an accepted market order: ~3 s
_METAL_BASES = frozenset({"XAU", "XAG", "XPT", "XPD"})
_USD_LIKE_QUOTES = frozenset({"USD", "USDT", "USDC"})
_IPC_ERRORS = range(-10005, -9999)          # -10005..-10000: terminal IPC lost
_REQUOTE_CODES = (MT5.TRADE_RETCODE_REQUOTE, MT5.TRADE_RETCODE_PRICE_CHANGED, MT5.TRADE_RETCODE_PRICE_OFF)
# Refusals that usually pass a moment later (no connection to the trade server, request timed out, too many
# requests): ONE delayed retry.
_TRANSIENT_CODES = (MT5.TRADE_RETCODE_CONNECTION, MT5.TRADE_RETCODE_TIMEOUT, MT5.TRADE_RETCODE_TOO_MANY_REQUESTS)
_DEAL_OK = (MT5.TRADE_RETCODE_DONE, MT5.TRADE_RETCODE_DONE_PARTIAL)
_MODIFY_OK = (MT5.TRADE_RETCODE_DONE, MT5.TRADE_RETCODE_PLACED, MT5.TRADE_RETCODE_NO_CHANGES)

# Native terminal calls (Mt5Broker). The MetaTrader5 package waits for the terminal without any time limit.
CALL_TIMEOUT_SEC = 30.0         # one native call
INIT_TIMEOUT_SEC = 90.0         # initialize(): the package itself gives the terminal 60 s to start and log in
HEALTH_TIMEOUT_SEC = 10.0       # each call of health_problem() (the engine's heartbeat allows 30 s for the check)
LOST_REPLY_WAIT_SEC = 60.0      # an order_send ran out of time: wait this long for the terminal before the look-up

# Turkish hints for MT4/MT5 trade server return codes (MetaApi reports the same numeric codes).
RETCODE_TR: dict[int, str] = {
    10004: "yeniden fiyat (requote)",
    10006: "talep reddedildi",
    10007: "talep iptal edildi",
    10011: "talep işlenirken hata",
    10012: "zaman aşımı",
    10013: "geçersiz talep",
    10014: "geçersiz lot miktarı",
    10015: "geçersiz fiyat",
    10016: "geçersiz stop/TP seviyesi (fiyata çok yakın olabilir)",
    10017: "bu sembolde işlem kapalı",
    10018: "piyasa kapalı",
    10019: "yetersiz bakiye/teminat",
    10020: "fiyat değişti",
    10021: "fiyat teklifi yok",
    10022: "geçersiz emir süresi",
    10024: "çok sık istek",
    10026: "sunucu otomatik işlemi kapattı",
    10027: ("MT5'te üst araç çubuğundaki 'Algo Trading' düğmesi kapalı; yeşil yapın "
            "(Araçlar > Seçenekler > Uzman Danışmanlar > algoritmik ticarete izin ver)"),
    10028: "talep kilitli",
    10029: "emir/pozisyon donmuş (freeze level)",
    10030: "desteklenmeyen dolum tipi (filling)",
    10031: "işlem sunucusuyla bağlantı yok",
    10033: "bekleyen emir limiti doldu",
    10034: "sembol için lot limiti doldu",
    10036: "pozisyon zaten kapalı",
    10038: "kapatma lotu pozisyondan büyük",
    10040: "açık pozisyon limiti doldu",
    10042: "bu sembolde yalnızca alış açılabilir",
    10043: "bu sembolde yalnızca satış açılabilir",
    10044: "bu sembolde yalnızca pozisyon kapatılabilir",
    10045: "FIFO kuralı: önce eski pozisyon kapatılmalı",
}


def retcode_text(code: int | None, comment: str | None = None) -> str:
    hint = RETCODE_TR.get(int(code)) if code is not None else None
    parts = [p for p in (hint, comment) if p]
    return f"{' / '.join(parts) or 'bilinmeyen hata'} (kod {code})"


def _num(x: float | None) -> str:
    return "-" if x is None else f"{x:.10g}"


def _snap(volume: float, step: float) -> float:
    """Clean float noise from sums/differences of step multiples."""
    return max(0.0, floor_to_step(volume, step)) if step > 0 else max(0.0, volume)


# --------------------------------------------------------------------------- #
# Pure helpers (shared with metaapi_broker)
# --------------------------------------------------------------------------- #
@dataclass
class PlannedLeg:
    volume: float
    tp: float | None
    tp_indexes: list[int] = field(default_factory=list)


def split_tp_legs(
    total: float, take_profits: Sequence[TakeProfit], step: float, min_volume: float
) -> list[PlannedLeg]:
    """One leg per TP with volume = floor_to_step(total * fraction); remainder to the last.

    A share below ``min_volume`` is merged into the next TP's leg (the last one
    into the previous leg). Fractions are normalized; all zero -> equal split.
    """
    if not take_profits:
        return [PlannedLeg(total, None, [])]
    n = len(take_profits)
    fracs = [max(0.0, tp.close_fraction) for tp in take_profits]
    s = sum(fracs)
    fracs = [f / s for f in fracs] if s > 0 else [1.0 / n] * n

    shares: list[float] = []
    used = 0.0
    for f in fracs[:-1]:
        v = floor_to_step(total * f, step)
        shares.append(v)
        used += v
    shares.append(_snap(total - used, step))

    smallest = max(min_volume, step)
    legs: list[PlannedLeg] = []
    carry, idx = 0.0, []
    for i, v in enumerate(shares):
        carry = _snap(carry + v, step)
        idx.append(i)
        if i < n - 1 and carry + _EPS >= smallest:
            legs.append(PlannedLeg(carry, take_profits[i].price, idx))
            carry, idx = 0.0, []
    if carry + _EPS >= smallest or not legs:
        legs.append(PlannedLeg(carry, take_profits[-1].price, idx))
    else:
        legs[-1].volume = _snap(legs[-1].volume + carry, step)
        legs[-1].tp_indexes.extend(idx)
    return legs


def merged_tp_notes(
    planned: Sequence[PlannedLeg], take_profits: Sequence[TakeProfit], min_volume: float
) -> list[str]:
    """Turkish notes for the take profits that got no order of their own because their share was below the
    minimum lot (split_tp_legs merged it into another leg): which ones, and where that part closes instead."""
    notes: list[str] = []
    for leg in planned:
        if leg.tp is None or len(leg.tp_indexes) <= 1:
            continue
        own = next((i for i in reversed(leg.tp_indexes) if take_profits[i].price == leg.tp), None)
        missing = [f"TP{i + 1} ({_num(take_profits[i].price)})" for i in leg.tp_indexes if i != own]
        if own is None or not missing:
            continue
        if len(missing) == 1:
            names, share = f"{missing[0]} hedefine", "payı"
        else:
            names, share = ", ".join(missing[:-1]) + f" ve {missing[-1]} hedeflerine", "payları"
        notes.append(
            f"Lot küçük olduğu için {names} ayrı emir konulamadı ({share} minimum lotun "
            f"({_num(min_volume)}) altında kalıyor); pozisyonun bu kısmı da TP{own + 1} ({_num(leg.tp)}) "
            f"hedefinde kapanacak."
        )
    return notes


def symbol_candidates(base: str, quote: str | None, market_type: MarketType | None = None) -> list[str]:
    """Broker symbol names to look for, best first (an exact name, or the start of a suffixed one: rank_symbol)."""
    b = base.upper()
    q = (quote or "").upper()
    usd_like = not q or q in _USD_LIKE_QUOTES
    if not usd_like:
        # A pair the broker does not list is not tradable there: BASE+USD would be another instrument
        # (EURGBP is not EURUSD).
        quotes: tuple[str, ...] = (q,)
    elif market_type in (MarketType.FOREX, MarketType.CFD):
        # FX / CFD never trade against USDT: probing "EURUSDT" could hit an unrelated listing and beat "EURUSD.a".
        quotes = (q, "USD")
    else:
        quotes = (q, "USD", "USDT")
    out: list[str] = []
    for x in quotes:
        if x and b + x not in out:
            out.append(b + x)
    # Index / energy / stock CFDs are usually listed under the bare name ("NAS100", "US30.cash"): tried last
    # (see ``whole`` in rank_symbol). Not for a currency or a metal (always a pair: the quote is part of the
    # instrument), nor next to a quote that is not the currency the asset is priced in (a symbol called just
    # "BTC" is not BTC/EUR).
    if (market_type == MarketType.CFD and b not in FIAT_CURRENCIES and b not in _METAL_BASES
            and (usd_like or q == CFD_QUOTES.get(b))):
        out.append(b)
    return out


def rank_symbol(name: str, candidates: Sequence[str], whole: Sequence[str] = ()) -> tuple[int, int, int] | None:
    """Lower is better; None = not a match. Prefers no / non-letter suffix ("ETHUSD.m" over "ETHUSDm").

    A candidate listed in ``whole`` (the bare name of a CFD) matches only the whole name or one with a separated
    suffix ("US30", "US30.cash"); a longer name ("US30YR", "US30m") could be another instrument."""
    core = name.upper().lstrip("#._-!")
    best: tuple[int, int, int] | None = None
    for i, cand in enumerate(candidates):
        if core.startswith(cand):
            suffix = core[len(cand):]
            if suffix and suffix[0].isalnum() and cand in whole:
                continue
            r = (int(bool(suffix) and suffix[0].isalpha()), i, len(suffix))
            best = r if best is None or r < best else best
    return best


# Index / energy CFDs that brokers list under different names. Every name in a row is the SAME instrument; the
# order is the order in which the other names are tried once the signal's own name is not listed.
CFD_ALIASES: tuple[tuple[str, ...], ...] = (
    ("NAS100", "US100", "USTEC", "NDX100"),
    ("US30", "DJ30", "WS30"),
    ("SPX500", "US500", "SP500"),
    ("GER40", "DE40", "DAX40", "GER30"),
    ("UK100", "FTSE100"),
    ("JP225", "JPN225", "NIK225"),
    ("USOIL", "WTI", "XTIUSD", "USOUSD"),
    ("UKOIL", "BRENT", "XBRUSD", "UKOUSD"),
)


def alias_candidates(base: str, quote: str | None, market_type: MarketType | None = None) -> list[str]:
    """Other broker names of the same index / energy CFD, in table order; [] for every other asset.

    Only where the bare name itself is a candidate (see symbol_candidates): a CFD quoted in a USD-like currency
    or in the currency the instrument is priced in."""
    b = base.upper()
    row = next((r for r in CFD_ALIASES if b in r), None)
    if row is None or market_type != MarketType.CFD:
        return []
    q = (quote or "").upper()
    if q and q not in _USD_LIKE_QUOTES and q not in {CFD_QUOTES.get(n) for n in row}:
        return []
    return [n for n in row if n != b]


def alias_whole(alias: str) -> tuple[str, ...]:
    """``whole`` argument of rank_symbol for an alias: a bare name ("US100", "WTI") matches only the whole name
    or a separated suffix; a BASE+QUOTE style name ("XTIUSD") takes any broker suffix, like other pairs."""
    return () if alias.endswith("USD") else (alias,)


def mapped_symbol(
    symbol_map: dict[str, str], base: str, raw_symbol: str | None, quote: str | None = None
) -> str | None:
    """The ``symbol_map`` entry for a signal, if any. Keys are compared case-insensitively with, in this order:
    the symbol as the signal wrote it ("EURGBP", "ETHUSDT.P"), the pair BASE+QUOTE ("EURGBP") when ``quote`` is
    given, and the base asset alone ("XAU", "NAS100"). A base key covers EVERY pair of that base: right for a
    metal, an index or a coin, wrong for a currency (key a currency pair by the pair)."""
    lower = {k.lower(): v for k, v in symbol_map.items()}
    pair = base + quote if quote else None
    for key in (raw_symbol, pair, base):
        if key and key.lower() in lower:
            return lower[key.lower()]
    return None


def tag_matches(value: str | None, tag: str) -> bool:
    return bool(value) and (value == tag or value.startswith(tag + "-"))  # type: ignore[union-attr]


def closed_by_deals(s: dict[str, Any] | None) -> bool:
    """The deal summary of a position shows it fully closed: closing deals cover every lot it opened. A summary
    with an opening deal only (just filled), or with fewer closed lots than opened (partly closed, or the newest
    closing deal not in the history yet), is NOT closed."""
    if not s:
        return False
    out = float(s.get("out_volume") or 0.0)
    return out > _EPS and out + _VOL_TOL >= float(s.get("in_volume") or 0.0)


def leg_tag(tag: str, index: int, count: int, limit: int = 31) -> str:
    suffix = f"-{index + 1}" if count > 1 else ""
    return tag[: limit - len(suffix)] + suffix


def new_leg(
    *,
    ticket: Any,
    state: str,
    volume: float,
    filled: float = 0.0,
    entry: float | None = None,
    sl: float | None = None,
    tp: float | None = None,
    order_ticket: Any = None,
    tag: str = "",
) -> dict[str, Any]:
    return {
        "ticket": ticket,
        "order_ticket": order_ticket,
        "state": state,
        "volume": volume,
        "filled": filled,
        "open_volume": filled if state == "open" else 0.0,
        "tp": tp,
        "tp_indexes": [],
        "entry": entry,
        "sl": sl,
        "venue_tp": None,
        "realized": None,
        "reason": None,
        "tag": tag,
        "misses": 0,
        "pnl_tries": 0,
    }


def _tp_sort_key(side: Side) -> Callable[[dict[str, Any]], float]:
    """Nearest take profit first; legs without TP last."""
    def key(leg: dict[str, Any]) -> float:
        tp = leg.get("tp")
        if tp is None:
            return math.inf
        return tp if side is Side.LONG else -tp
    return key


# --------------------------------------------------------------------------- #
# Leg failures of a management call (stop / TP move, close, cancel)
# --------------------------------------------------------------------------- #
# A leg whose stop move got no answer (update_stop_loss): the stop it is to carry; sync_position sends it again when
# the account shows another one there (MetaTraderLegBroker._resend_wanted_stops).
SL_WANTED = "sl_wanted"
# Netting accounts: an emulated TP whose partial close got no answer: {"from": the open volume then, "at": epoch s}.
# It is done once the account shows the volume drop; after TP_LOST_WAIT_SEC without it, it did not execute.
TP_LOST = "lost"
TP_LOST_WAIT_SEC = 60.0


def answer_lost(exc: BaseException) -> bool:
    """A leg request that went out and whose answer was lost, so it may have been executed: an
    OrderOutcomeUnknownError (Mt5Broker: OrderReplyLostError) or a MetaApi error carrying ``no_answer`` (timeout,
    link cut while the request was under way)."""
    return isinstance(exc, OrderOutcomeUnknownError) or bool(getattr(exc, "no_answer", False))


class _LegErrors(list):  # type: ignore[type-arg]
    """The owner-facing error texts of the legs of one management call (a plain list of str, as before), with the
    leg exceptions behind them in ``failures`` (see _leg_failed), so that the call's own error can tell a lost answer
    from a refusal (_fold)."""

    def __init__(self) -> None:
        super().__init__()
        self.failures: list[BaseException] = []
        self.whole_closes: list[tuple[str, BaseException, dict[str, Any]]] = []   # (text, error, leg): leg closed in full


def _leg_failed(errors: list[str], text: str, exc: BaseException) -> None:
    """Record a failed leg request: ``text`` in ``errors`` and, when ``errors`` keeps them (_LegErrors), ``exc``."""
    errors.append(text)
    failures = getattr(errors, "failures", None)
    if failures is not None:
        failures.append(exc)


def _lost_answer(errors: list[str]) -> BaseException | None:
    """The first recorded leg failure whose answer was lost (see answer_lost), or None."""
    return next((e for e in getattr(errors, "failures", ()) if answer_lost(e)), None)


def _fold(failed: str, unknown: str, errors: list[str]) -> BrokerError:
    """The error of a management call none of whose leg requests worked: BrokerError(``failed`` + the leg texts).
    When one of those requests went out and its answer was lost (it may have worked): OrderOutcomeUnknownError
    (``unknown`` + the leg texts) whose cause is that leg's error, so that the engine and the monitor treat the
    outcome as unknown (base.outcome_unknown): never "could not be closed" or "refused", and the account is read
    before anything is sent again."""
    text = "; ".join(errors)
    lost = _lost_answer(errors)
    if lost is None:
        return BrokerError(failed + text)
    err = OrderOutcomeUnknownError(unknown + text)
    err.__cause__ = lost
    return err


def _some_failed(failed: str, unknown: str, errors: list[str]) -> str:
    """The note (pos.error) of a management call that worked on some legs only: ``unknown`` when one of the failed
    requests got no answer (it may have worked there too), else ``failed``; then the leg texts."""
    return (unknown if _lost_answer(errors) is not None else failed) + "; ".join(errors)


# --------------------------------------------------------------------------- #
# Shared leg-based broker
# --------------------------------------------------------------------------- #
class MetaTraderLegBroker(Broker, ABC):
    """Leg-per-TP trading logic for MT4/MT5-style venues. Subclasses implement the primitives."""

    retry_delay_sec: float = 0.5
    tag_limit: int = 31          # max chars of the order comment / client id carrying the tag

    def __init__(self, config: Any) -> None:
        super().__init__(config)
        self.risk_config: RiskConfig = RiskConfig()
        self._netting = False
        self._open_locks: dict[str, asyncio.Lock] = {}

    # ---- primitives --------------------------------------------------------
    @abstractmethod
    async def _quote(self, broker_symbol: str) -> Quote: ...

    @abstractmethod
    async def _loss_per_lot(self, symbol: SymbolInfo, side: Side, entry: float, stop: float) -> float:
        """Money (account currency) lost by 1 lot moving from entry to stop, excluding fees."""

    @abstractmethod
    async def _margin_for(self, symbol: SymbolInfo, side: Side, lots: float, entry: float) -> float | None: ...

    @abstractmethod
    async def _open_market_leg(
        self, symbol: SymbolInfo, side: Side, volume: float, sl: float, tp: float | None, tag: str
    ) -> dict[str, Any]:
        """Open one market leg. ``leg["sl"]`` must be None if the stop is NOT on the venue."""

    @abstractmethod
    async def _open_pending_leg(
        self, symbol: SymbolInfo, side: Side, volume: float, price: float, sl: float,
        tp: float | None, tag: str, quote: Quote,
    ) -> dict[str, Any]: ...

    @abstractmethod
    async def _modify_leg(self, symbol: str, side: Side, leg: dict[str, Any], sl: float, tp: float | None) -> None:
        """Set SL/TP of an open leg, or of a pending leg's order. Raise BrokerError on failure."""

    @abstractmethod
    async def _close_leg(self, symbol: str, side: Side, leg: dict[str, Any], volume: float | None) -> float:
        """Close ``volume`` lots (None = all) of an open leg at market. Returns the closed lots."""

    @abstractmethod
    async def _cancel_leg(self, symbol: str, leg: dict[str, Any]) -> None:
        """Delete a pending leg's order. Raise BrokerError if it could not be removed."""

    @abstractmethod
    async def _snapshot(self, symbol: str) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        """Open positions and pending orders of ``symbol`` keyed by str(ticket).

        Position dict: ticket, volume, entry, sl, tp, tag, magic, realized (closed part or None).
        Order dict: ticket, volume, price, sl, tp, tag, magic.
        """

    @abstractmethod
    async def _history_order(self, order_ticket: Any) -> dict[str, Any] | None:
        """{"state": "filled"|"cancelled"|"other", "position_id": ..., "reason": str} or None if unknown."""

    @abstractmethod
    async def _deal_summary(self, position_id: Any) -> dict[str, Any] | None:
        """From the position's deals: realized, in_volume, out_volume, entry, reason. None if no history."""

    async def _position_now(self, symbol: str, ticket: Any) -> dict[str, Any] | None:
        """One position read afresh (same dict as in _snapshot), None if the venue does not list it. Subclasses
        may read just that position; this default reads the whole snapshot again."""
        positions, _ = await self._snapshot(symbol)
        return positions.get(str(ticket))

    # ---- sizing ------------------------------------------------------------
    def _fee_per_lot(self, loss_per_lot: float, entry: float, stop: float) -> float:
        """MT brokers do not report commission up front: optional buffer.

        Only ``commission_per_lot`` from the broker config is used (round trip,
        account currency). The crypto-style ``risk.fee_rate`` on notional is NOT
        applied here: on forex/CFD it would massively undersize trades (a 0.06%
        fee on a 20-pip EURUSD stop is a ~66% buffer).
        """
        commission = getattr(self.config, "commission_per_lot", None)
        if commission is None or not self.risk_config.include_fees_in_risk:
            return 0.0
        return max(0.0, float(commission))

    async def size_position(
        self, symbol: SymbolInfo, side: Side, entry_price: float, stop_loss: float, risk_amount: float
    ) -> SizingResult:
        if entry_price <= 0 or stop_loss <= 0 or entry_price == stop_loss:
            raise SizingError("Pozisyon hesaplanamadı: giriş/stop fiyatı geçersiz.")
        rc = self.risk_config
        loss = await self._loss_per_lot(symbol, side, entry_price, stop_loss)
        if not math.isfinite(loss) or loss <= 0:
            raise SizingError(f"{symbol.broker_symbol}: stop mesafesinde lot başına zarar hesaplanamadı.")
        per_lot = loss + self._fee_per_lot(loss, entry_price, stop_loss)
        lots = compute_lot_size(
            risk_amount=risk_amount,
            loss_per_lot=per_lot,
            lot_step=symbol.qty_step,
            min_lot=symbol.min_qty,
            max_lot=symbol.max_qty,
            allow_min_qty_round_up=rc.allow_min_qty_round_up,
        )
        warnings: list[str] = []
        est = lots * per_lot
        if symbol.max_qty and risk_amount / per_lot > symbol.max_qty + _EPS:
            warnings.append(f"Lot brokerın azami lotuyla ({_num(symbol.max_qty)}) sınırlandı; risk hedefin altında.")
        if est > risk_amount * (1 + 1e-6):
            warnings.append(f"Lot minimum lota yükseltildi; stopta kayıp ~{est:.2f} (hedef {risk_amount:.2f}).")

        margin = await self._margin_for(symbol, side, lots, entry_price)
        free = await self.get_free_margin()
        allowed = free * rc.max_margin_usage_pct / 100.0
        currency = await self.account_currency()
        if margin is None:
            warnings.append("Teminat önceden hesaplanamadı; kontrolü broker emir sırasında yapacak.")
        elif margin > allowed:
            raise SizingError(
                f"Yetersiz teminat: {_num(lots)} lot {symbol.broker_symbol} için {margin:.2f} {currency} gerekiyor, "
                f"kullanılabilir {allowed:.2f} {currency} (serbest teminatın %{rc.max_margin_usage_pct:g}'i)."
            )
        return SizingResult(
            quantity=lots,
            risk_amount=risk_amount,
            est_loss_at_sl=est,
            notional=lots * symbol.contract_size * entry_price,
            leverage=None,
            margin_required=margin,
            warnings=warnings,
        )

    async def get_quote(self, symbol: SymbolInfo) -> Quote:
        return await self._quote(symbol.broker_symbol)

    # ---- open ----------------------------------------------------------------
    def _check_levels(
        self, side: Side, ref: float, sl: float, tps: Sequence[TakeProfit], quote: Quote,
        symbol: SymbolInfo, pending: bool,
    ) -> None:
        """Reject before sending anything: stops on the wrong side / closer than the broker's stops level."""
        sign = 1.0 if side is Side.LONG else -1.0
        level = float(symbol.extra.get("stops_level") or 0.0)
        base = ref if pending else quote.exit_price_for(side)
        what = "giriş fiyatının" if pending else "mevcut fiyatın"
        below_above = "altında" if side is Side.LONG else "üstünde"
        if sign * (base - sl) <= 0:
            raise BrokerError(f"Stop ({_num(sl)}) {what} ({_num(base)}) {below_above} olmalı; emir gönderilmedi.")
        if sign * (base - sl) < level - _EPS:
            raise BrokerError(
                f"Stop ({_num(sl)}) {what} ({_num(base)}) brokerın izin verdiği asgari mesafeden "
                f"({_num(level)}) daha yakın; emir gönderilmedi."
            )
        for tp in tps:
            if sign * (tp.price - base) <= 0:
                other = "üstünde" if side is Side.LONG else "altında"
                raise BrokerError(
                    f"TP ({_num(tp.price)}) {what} ({_num(base)}) {other} olmalı "
                    f"(fiyat TP'yi geçmiş olabilir); emir gönderilmedi."
                )
            if sign * (tp.price - base) < level - _EPS:
                raise BrokerError(f"TP ({_num(tp.price)}) fiyata asgari mesafeden ({_num(level)}) daha yakın.")
        if pending and level > 0 and abs(ref - quote.entry_price_for(side)) < level - _EPS:
            raise BrokerError(
                f"Bekleyen emir fiyatı ({_num(ref)}) piyasaya asgari mesafeden ({_num(level)}) daha yakın."
            )
        trade_mode = symbol.extra.get("trade_mode")
        if trade_mode == MT5.SYMBOL_TRADE_MODE_LONGONLY and side is Side.SHORT:
            raise BrokerError(f"{symbol.broker_symbol} bu brokerda yalnızca alış (long) açılabilir.")
        if trade_mode == MT5.SYMBOL_TRADE_MODE_SHORTONLY and side is Side.LONG:
            raise BrokerError(f"{symbol.broker_symbol} bu brokerda yalnızca satış (short) açılabilir.")

    def _plan(
        self, total: float, tps: list[TakeProfit], step: float, vmin: float
    ) -> tuple[list[PlannedLeg], list[dict[str, Any]]]:
        if not self._netting or len(tps) <= 1:
            return split_tp_legs(total, tps, step, vmin), []
        # Netting: one position per symbol. Venue TP = last TP; earlier ones emulated in sync.
        s = sum(max(0.0, t.close_fraction) for t in tps)
        fracs = [max(0.0, t.close_fraction) / s if s > 0 else 1.0 / len(tps) for t in tps]
        emulated = [
            {"index": i, "price": t.price, "fraction": fracs[i], "done": False}
            for i, t in enumerate(tps[:-1])
        ]
        return [PlannedLeg(total, tps[-1].price, list(range(len(tps))))], emulated

    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        # Serialize opens per symbol so the idempotency / netting exposure checks cannot race.
        lock = self._open_locks.setdefault(req.symbol.broker_symbol, asyncio.Lock())
        async with lock:
            return await self._open_position(req)

    async def _open_position(self, req: OpenRequest) -> BrokerPosition:
        sym = req.symbol
        side = req.plan.side
        step, vmin, tick = sym.qty_step, sym.min_qty, sym.price_tick
        total = floor_to_step(req.quantity, step)
        if total <= 0 or total + _EPS < vmin:
            raise BrokerError(f"Lot ({_num(req.quantity)}) brokerın minimum lotunun ({_num(vmin)}) altında.")
        sl = round_to_tick(req.stop_loss, tick)
        tps = [TakeProfit(price=round_to_tick(t.price, tick), close_fraction=t.close_fraction) for t in req.take_profits]
        pending = req.entry_type is EntryType.LIMIT
        quote = await self._quote(sym.broker_symbol)
        notes: list[str] = []
        size_capped = False
        if pending:
            if req.entry_price is None:
                raise BrokerError("Limit emir için giriş fiyatı yok.")
            ref = round_to_tick(req.entry_price, tick)
            # A limit that is already marketable (long: ask <= price; short: bid >= price) would be sent as a
            # BUY_STOP / SELL_STOP, a different trade. The signal means "get in at this price or better":
            # open it at the market instead (the engine still bounds the distance by max_entry_distance_pct).
            if (quote.ask <= ref) if side is Side.LONG else (quote.bid >= ref):
                fill = quote.entry_price_for(side)
                notes.append(
                    f"Limit giriş ({_num(ref)}) anlık fiyatta ({_num(fill)}) zaten gerçekleşebilir durumda "
                    f"olduğundan bekleyen emir yerine piyasa emriyle açıldı."
                )
                total, size_capped = await self._resize_for_fill(sym, side, total, ref, fill, sl, notes)
                pending = False
                ref = fill
        else:
            ref = quote.entry_price_for(side)
        self._check_levels(side, ref, sl, tps, quote, sym, pending)

        pos = BrokerPosition(
            broker=self.name,
            broker_symbol=sym.broker_symbol,
            side=side,
            status=PositionStatus.PENDING if pending else PositionStatus.OPEN,
            quantity=total,
            stop_loss=sl,
            take_profits=tps,
            data={
                "legs": [],
                "client_tag": req.client_tag,
                "netting": self._netting,
                "volume_step": step,
                "volume_min": vmin,
                "price_tick": tick,
                "emulated_tps": [],
                "warnings": notes,
            },
        )
        if size_capped:
            pos.data["size_capped"] = True      # larger than planned, but limited: the engine tells the owner

        # Rule 4: a retried request must not double the position.
        positions, orders = await self._snapshot(sym.broker_symbol)
        existing = self._legs_from_snapshot(positions, orders, req.client_tag)
        if existing:
            logger.warning("%s: orders tagged %s already exist, not re-sending", self.name, req.client_tag)
            pos.data["legs"] = existing
            self._aggregate(pos)
            pos.error = f"Bu işlem etiketiyle ({req.client_tag}) açılmış emir/pozisyon zaten var; tekrar gönderilmedi."
            return pos
        if self._netting and positions:
            raise BrokerError(
                f"Netting hesap: {sym.broker_symbol} için zaten açık pozisyon var; yeni işlem onunla birleşirdi. "
                f"İşlem bu brokerda atlandı."
            )

        planned, emulated = self._plan(total, tps, step, vmin)
        pos.data["emulated_tps"] = emulated
        if not emulated:
            # A small lot cannot be split over every take profit: say which ones got no order of their own.
            notes.extend(merged_tp_notes(planned, tps, vmin))
        legs: list[dict[str, Any]] = pos.data["legs"]
        errors: list[str] = []
        n = len(planned)
        for i, pl in enumerate(planned):
            tag = leg_tag(req.client_tag, i, n, self.tag_limit)
            label = f"{i + 1}/{n}. bacak" if n > 1 else "Emir"
            try:
                if pending:
                    leg = await self._open_pending_leg(sym, side, pl.volume, ref, sl, pl.tp, tag, quote)
                else:
                    leg = await self._open_market_leg(sym, side, pl.volume, sl, pl.tp, tag)
            except OrderReplyLostError as exc:
                # The reply was lost; the order may exist. Look it up by its tag instead of sending again.
                leg = await self._adopt_lost_leg(sym.broker_symbol, tag, pl.volume, sl, pl.tp)
                if leg is None:
                    # Not "açılamadı" (could not be opened): nobody knows whether this order exists.
                    errors.append(f"{label} durumu belirsiz: {exc}")
                    break
                notes.append(f"{label}: emir yanıtı kaybolmuştu, etiketle bulundu (#{leg['ticket']}); tekrar gönderilmedi.")
            except BrokerError as exc:
                errors.append(f"{label} açılamadı: {exc}")
                break
            leg["tp_indexes"] = pl.tp_indexes
            legs.append(leg)
            note = leg.pop("note", None)       # something the owner should know about how this leg was opened
            if note:
                notes.append(f"{label}: {note}")
            if leg["state"] == "open" and leg.get("sl") is None:
                err = await self._protect_or_close(sym.broker_symbol, side, leg, sl, pl.tp)
                if err:
                    errors.append(f"{label}: {err}")
                if leg["state"] == "failed":
                    break   # the stop is unplaceable; later legs would fail the same way

        if not any(leg["state"] in LIVE_STATES or leg["state"] == "failed" for leg in legs):
            raise BrokerError("; ".join(errors) or "Emir gönderilemedi.")

        if not pending and any(leg["state"] == "open" for leg in legs):
            # Verify on the venue that every filled leg really carries its stop (rule 1).
            try:
                await self._refresh_legs(pos, legs)
            except BrokerError as exc:
                logger.warning("%s: post-open refresh failed: %s", self.name, exc)
            for leg in legs:
                if leg["state"] == "open" and leg.get("sl") is None:
                    err = await self._protect_or_close(sym.broker_symbol, side, leg, sl, leg.get("tp"))
                    if err:
                        errors.append(err)
            for leg in legs:
                if leg["state"] == "open" and leg.get("unconfirmed"):
                    # Filled, but neither the look-up after the fill nor this refresh has seen it on the venue.
                    errors.append(
                        f"#{leg['ticket']}: emir gerçekleşti ama pozisyon henüz doğrulanamadı; sonraki "
                        f"kontrollerde doğrulanacak. MetaTrader'da '{leg['tag']}' etiketli pozisyona bakın."
                    )

        self._aggregate(pos)
        pos.error = "; ".join(errors) or None
        logger.info(
            "%s: opened %s %s %s lots in %d leg(s) status=%s%s",
            self.name, side.value, sym.broker_symbol, _num(total), len(legs), pos.status.value,
            f" error={pos.error}" if pos.error else "",
        )
        return pos

    async def _resize_for_fill(
        self, sym: SymbolInfo, side: Side, total: float, planned_entry: float, fill: float, sl: float,
        notes: list[str],
    ) -> tuple[float, bool]:
        """(lots, were they raised above the planned lots AND limited by ``risk.max_size_multiple``?). Lots
        re-sized from the price the order will really fill at, so that the loss at the stop stays the 1R the
        engine sized for the planned entry. Never raises: on any problem the original lots stand (always the
        smaller, safer size here, because a marketable limit fills at a better price).

        Never more than ``risk.max_size_multiple`` times the planned lots: a fill close to the stop would
        otherwise multiply them without a limit (0.48 -> 6.66 lots 1.5 pips from the stop)."""
        try:
            old = await self._loss_per_lot(sym, side, planned_entry, sl)
            new = await self._loss_per_lot(sym, side, fill, sl)
            fee = self._fee_per_lot(old, planned_entry, sl)
            if not (math.isfinite(old) and math.isfinite(new) and new > 0 and old > 0):
                return total, False
            lots = floor_to_step((total * (old + fee)) / (new + fee), sym.qty_step)
            if sym.max_qty:
                lots = min(lots, floor_to_step(sym.max_qty, sym.qty_step))
            multiple = self._max_size_multiple()
            cap = floor_to_step(total * multiple, sym.qty_step)
            cap_note: str | None = None
            if lots > cap + _EPS:
                lots = cap
                cap_note = (
                    f"Fiyat stopa yakın olduğu için lot, planlanan lotun {_num(multiple)} katıyla "
                    f"({_num(cap)}) sınırlandı; gerçek risk hedefin altında."
                )
            if lots <= total + _EPS:
                if cap_note:
                    notes.append(cap_note)
                return total, False             # the planned lots stand (multiple 1): nothing was raised
            margin = await self._margin_for(sym, side, lots, fill)
            allowed = await self.get_free_margin() * self.risk_config.max_margin_usage_pct / 100.0
            if margin is not None and margin > allowed:
                notes.append("Lot, piyasadan dolum fiyatına göre artırılamadı (teminat yetersiz); ilk hesaplanan lot kullanıldı.")
                return total, False
            resized = f"Lot dolum fiyatına göre {_num(total)} -> {_num(lots)} olarak yeniden hesaplandı"
            if cap_note:        # limited: the loss at the stop is below 1R, so the note must not promise 1R
                notes += [resized + ".", cap_note]
            else:
                notes.append(resized + " (risk 1R'de kalsın).")
            return lots, cap_note is not None
        except BrokerError as exc:
            logger.warning("%s: could not re-size for the market fill: %s", self.name, exc)
            return total, False

    def _max_size_multiple(self) -> float:
        """``risk.max_size_multiple``, at least 1 (the planned lots always stand). A missing or unusable value
        counts as the default; never raises."""
        try:
            k = float(getattr(self.risk_config, "max_size_multiple", _DEFAULT_MAX_SIZE_MULTIPLE))
        except (TypeError, ValueError):
            return _DEFAULT_MAX_SIZE_MULTIPLE
        return max(1.0, k) if math.isfinite(k) and k > 0 else _DEFAULT_MAX_SIZE_MULTIPLE

    async def _protect_or_close(
        self, symbol: str, side: Side, leg: dict[str, Any], sl: float, tp: float | None
    ) -> str | None:
        """Attach the stop to a filled leg (one retry); if impossible close the leg (rule 1)."""
        last = ""
        for attempt in range(2):
            try:
                await self._modify_leg(symbol, side, leg, sl, tp)
                leg["sl"] = sl
                leg.pop("naked", None)
                return None
            except BrokerError as exc:
                last = str(exc)
                logger.warning("%s: SL attach failed for leg %s (attempt %d): %s", self.name, leg["ticket"], attempt + 1, exc)
                if attempt == 0 and self.retry_delay_sec:
                    await asyncio.sleep(self.retry_delay_sec)
        try:
            await self._close_leg(symbol, side, leg, None)
        except BrokerError as exc:
            leg["naked"] = True
            logger.critical("%s: leg %s has NO stop loss and could not be closed: %s", self.name, leg["ticket"], exc)
            return (
                f"KRİTİK: {symbol} pozisyonu #{leg['ticket']} STOPSUZ açık ve kapatılamadı ({exc}). "
                f"Hemen elle kapatın!"
            )
        leg["state"] = "failed"
        leg["reason"] = "no_stop"
        return f"Stop konulamadı ({last}); pozisyon #{leg['ticket']} piyasa fiyatından kapatıldı."

    async def _adopt_lost_leg(
        self, symbol: str, tag: str, volume: float, sl: float, tp: float | None
    ) -> dict[str, Any] | None:
        """After a lost order_send reply: the leg with exactly this tag, if the order reached the venue."""
        try:
            positions, orders = await self._snapshot(symbol)
        except BrokerError as exc:
            logger.error("%s: cannot verify lost order_send reply for %s: %s", self.name, tag, exc)
            return None
        found = [leg for leg in self._legs_from_snapshot(positions, orders, tag) if leg["tag"] == tag]
        if not found:
            return None
        leg = found[0]
        leg["volume"] = float(volume) if not leg.get("volume") else leg["volume"]
        if leg["state"] == "pending":
            leg["order_ticket"] = leg["ticket"]
        return leg

    # ---- refresh / aggregate ---------------------------------------------------
    def _legs_from_snapshot(
        self, positions: dict[str, dict[str, Any]], orders: dict[str, dict[str, Any]], tag: str
    ) -> list[dict[str, Any]]:
        legs: list[dict[str, Any]] = []
        for p in positions.values():
            if tag_matches(p.get("tag"), tag):
                leg = new_leg(ticket=p["ticket"], state="open", volume=p["volume"], filled=p["volume"],
                              entry=p.get("entry"), sl=p.get("sl"), tp=p.get("tp"), tag=p["tag"])
                legs.append(leg)
        for o in orders.values():
            if tag_matches(o.get("tag"), tag):
                part = next((leg for leg in legs if str(leg["ticket"]) == str(o["ticket"])), None)
                if part is not None:        # partly filled: the position above is its filled part
                    part.update(rest_order=o["ticket"], rest_volume=o["volume"], rest_price=o.get("price"))
                    continue
                legs.append(new_leg(ticket=o["ticket"], order_ticket=o["ticket"], state="pending",
                                    volume=o["volume"], entry=o.get("price"), sl=o.get("sl"), tp=o.get("tp"),
                                    tag=o["tag"]))
        legs.sort(key=lambda leg: (len(leg["tag"]), leg["tag"]))   # "-2" before "-10"
        return legs

    @staticmethod
    def _apply_position(leg: dict[str, Any], p: dict[str, Any]) -> None:
        leg["state"] = "open"
        leg["ticket"] = p["ticket"]
        leg["open_volume"] = float(p["volume"])
        leg["filled"] = max(float(leg.get("filled") or 0.0), float(p["volume"]))
        leg["entry"] = p.get("entry") or leg.get("entry")
        leg["sl"] = p.get("sl") or None
        leg["venue_tp"] = p.get("tp") or None
        leg.pop("unconfirmed", None)    # seen on the venue
        if leg["sl"]:
            leg.pop("naked", None)      # the venue shows a stop on it again

    @staticmethod
    def _take_realized(leg: dict[str, Any], s: dict[str, Any]) -> bool:
        """Realized PnL of a leg from its deal summary, once the closing deals cover every lot closed so far
        (filled - open_volume). Until then (the newest closing deal is not in the history yet) realized stays
        None and a later refresh reads again; after _MAX_PNL_TRIES reads the last number read stands. True when
        it was taken."""
        closed = float(leg.get("filled") or 0.0) - float(leg.get("open_volume") or 0.0)
        if float(s.get("out_volume") or 0.0) + _VOL_TOL >= closed or leg.get("pnl_tries", 0) >= _MAX_PNL_TRIES:
            leg["realized"] = s.get("realized")
            return True
        leg["realized"] = None
        return False

    @staticmethod
    def _mark_closed_lots(leg: dict[str, Any], closed: float) -> None:
        """The bot closed ``closed`` lots of the leg: its realized PnL changed on the venue and must be read again
        (a number read after an earlier partial close would otherwise stand for good)."""
        if closed > _EPS:
            leg["realized"] = None
            leg["pnl_tries"] = 0

    @staticmethod
    def _close_from_summary(leg: dict[str, Any], s: dict[str, Any]) -> None:
        leg["state"] = "closed"
        leg["open_volume"] = 0.0
        leg["realized"] = s.get("realized")
        leg["reason"] = s.get("reason") or leg.get("reason") or "manual"
        leg["filled"] = max(float(leg.get("filled") or 0.0), float(s.get("in_volume") or 0.0))
        leg["entry"] = leg.get("entry") or s.get("entry")

    async def _refresh_legs(self, pos: BrokerPosition, legs: list[dict[str, Any]]) -> None:
        todo = [
            leg for leg in legs
            if leg["state"] in LIVE_STATES
            or (leg["state"] in ("closed", "failed") and leg.get("realized") is None
                and leg.get("filled", 0) > 0 and leg.get("pnl_tries", 0) < _MAX_PNL_TRIES)
        ]
        if not todo:
            return
        positions, orders = await self._snapshot(pos.broker_symbol)
        for leg in todo:
            if leg["state"] == "pending":
                await self._refresh_pending(leg, positions, orders, symbol=pos.broker_symbol)
            elif leg["state"] == "open":
                await self._refresh_open(leg, positions)
            else:
                leg["pnl_tries"] = leg.get("pnl_tries", 0) + 1
                s = await self._deal_summary(leg["ticket"])
                if s and s.get("out_volume", 0) > _EPS and self._take_realized(leg, s):
                    if leg["state"] == "closed" and leg.get("reason") in (None, "manual", "unknown"):
                        leg["reason"] = s.get("reason") or leg.get("reason")
            if leg.get("rest_order") is not None:
                o = orders.get(str(leg["rest_order"]))
                if o is None:
                    self._drop_rest(leg)    # filled into the position (it shows the lots), or cancelled / expired
                else:
                    leg["rest_volume"] = float(o.get("volume") or 0.0)
                    leg["rest_price"] = o.get("price") or leg.get("rest_price")

    async def _refresh_pending(
        self, leg: dict[str, Any], positions: dict[str, dict[str, Any]], orders: dict[str, dict[str, Any]],
        *, symbol: str | None = None,
    ) -> None:
        oid = leg["order_ticket"]
        o = orders.get(str(oid))
        if o is not None:
            leg["sl"] = o.get("sl") or None
            leg["venue_tp"] = o.get("tp") or None
            leg["entry"] = o.get("price") or leg.get("entry")
            p = positions.get(str(oid))
            if p is not None:
                # Partly filled: the filled part is a position under the order's ticket while the rest still rests.
                # The leg becomes that position (managed like any open leg); the resting rest goes with it.
                self._apply_position(leg, p)
                leg.update(rest_order=oid, rest_volume=float(o.get("volume") or 0.0), rest_price=o.get("price"))
                logger.warning("%s: pending order %s partly filled: %s lots open, %s lots still resting", self.name,
                               oid, _num(leg["open_volume"]), _num(leg["rest_volume"]))
            return
        pid: Any = oid
        hist = None
        if str(pid) not in positions:
            hist = await self._history_order(oid)
            if hist and hist.get("position_id"):
                pid = hist["position_id"]
        p = positions.get(str(pid))
        if p is not None:
            self._apply_position(leg, p)
            logger.info("%s: pending order %s filled -> position %s", self.name, oid, p["ticket"])
            return
        if hist is None:
            logger.warning("%s: pending order %s not found on venue nor in history yet", self.name, oid)
            return
        if hist["state"] == "filled":
            leg["ticket"] = pid
            # The snapshot can be older than the fill: positions are read before orders (an order that fills
            # between the two reads is in neither), and the terminal does not promise to show the position
            # before the order's history. One stale read must never book a live position closed or cancelled.
            p = await self._position_now(symbol, pid) if symbol else None
            if p is not None:
                self._apply_position(leg, p)
                logger.info("%s: pending order %s filled -> position %s (read again)", self.name, oid, p["ticket"])
                return
            s = await self._deal_summary(pid)
            if closed_by_deals(s):
                self._close_from_summary(leg, s)        # filled and closed again (stop / target) between two looks
                return
            self._filled_not_seen(leg, pid, s)
        elif hist["state"] == "cancelled":
            # A part may have filled before the rest was cancelled / expired (MetaTrader keeps a partly filled
            # order resting with the rest): that part is a position, never "cancelled".
            s = await self._deal_summary(pid)
            if s and float(s.get("in_volume") or 0.0) > _EPS:
                leg["ticket"] = pid
                if closed_by_deals(s):
                    self._close_from_summary(leg, s)
                    return
                p = await self._position_now(symbol, pid) if symbol else None
                if p is not None:
                    self._apply_position(leg, p)
                else:
                    self._filled_not_seen(leg, pid, s)
                logger.warning("%s: pending order %s was cancelled after a partial fill; tracking the filled part",
                               self.name, oid)
                return
            leg["state"] = "cancelled"
            leg["reason"] = hist.get("reason") or "cancelled"

    @staticmethod
    def _rest_view(leg: dict[str, Any]) -> dict[str, Any]:
        """The still resting rest of a partly filled order as a pending leg of its own (for _modify_leg /
        _cancel_leg)."""
        return {**leg, "state": "pending", "ticket": leg["rest_order"], "order_ticket": leg["rest_order"],
                "entry": leg.get("rest_price") or leg.get("entry")}

    @staticmethod
    def _drop_rest(leg: dict[str, Any]) -> None:
        for key in ("rest_order", "rest_volume", "rest_price"):
            leg.pop(key, None)

    async def _cancel_rest(self, symbol: str, leg: dict[str, Any], errors: list[str]) -> str | None:
        """Remove the resting rest of a partly filled order: "removed" (by this call), "gone" (the venue no longer
        lists it: it filled into the position meanwhile, or the venue cancelled it), or None (it still rests;
        the reason is in ``errors``)."""
        rest = leg["rest_order"]
        try:
            await self._cancel_leg(symbol, self._rest_view(leg))
        except BrokerError as exc:
            try:
                _, orders = await self._snapshot(symbol)
                resting = str(rest) in orders
            except BrokerError:
                resting = True              # cannot look: as far as is known it still rests
            if resting:
                what = "iptal isteğine yanıt alınamadı" if answer_lost(exc) else "iptal edilemedi"
                _leg_failed(errors, f"#{rest}: emrin dolmamış kalan kısmı {what} ({exc})", exc)
                return None
            self._drop_rest(leg)
            return "gone"
        self._drop_rest(leg)
        return "removed"

    def _filled_not_seen(self, leg: dict[str, Any], pid: Any, s: dict[str, Any] | None) -> None:
        """A pending order filled (history) but its position is not listed (yet): an open leg, tracked by the
        position ticket and the order comment until a refresh sees it (_refresh_open), closed only when the deals
        show it fully closed or after _MAX_MISSES refreshes that find nothing. Size from the opening deals, else
        the order's; its stop and target stay those of the order (they moved to the position with the fill), so
        the stop repair of sync_position does not act on a position it cannot see yet."""
        in_vol = float((s or {}).get("in_volume") or 0.0)
        out_vol = float((s or {}).get("out_volume") or 0.0)
        filled = in_vol if in_vol > _EPS else float(leg.get("volume") or 0.0)
        leg.update(state="open", ticket=pid, unconfirmed=True, misses=0)
        leg["filled"] = max(float(leg.get("filled") or 0.0), filled)
        leg["open_volume"] = max(0.0, filled - out_vol)
        if (s or {}).get("entry"):
            leg["entry"] = s["entry"]       # type: ignore[index]
        logger.warning("%s: pending order %s filled but position %s is not listed yet; tracking it as open",
                       self.name, leg.get("order_ticket"), pid)

    async def _refresh_open(self, leg: dict[str, Any], positions: dict[str, dict[str, Any]]) -> None:
        p = positions.get(str(leg["ticket"]))
        if p is None and leg.get("unconfirmed") and leg.get("tag"):
            # Tracked by its order ticket only (the look-up after the fill failed): the position carries our tag.
            p = next((x for x in positions.values() if x.get("tag") == leg["tag"]), None)
        if p is not None:
            prev = float(leg.get("open_volume") or 0.0)
            self._apply_position(leg, p)
            leg["misses"] = 0
            if leg["open_volume"] < prev - _EPS or (leg["open_volume"] < leg["filled"] - _EPS and leg["realized"] is None):
                if p.get("realized") is not None:
                    leg["realized"] = p["realized"]
                else:
                    leg["pnl_tries"] = leg.get("pnl_tries", 0) + 1
                    s = await self._deal_summary(leg["ticket"])
                    if s:
                        self._take_realized(leg, s)
            return
        s = await self._deal_summary(leg["ticket"])
        if closed_by_deals(s):
            self._close_from_summary(leg, s)
            return
        # Missing from positions, and its deals do not show it fully closed (none yet, or the newest closing
        # deal not in the history yet): do not conclude on one glitch.
        leg["misses"] = leg.get("misses", 0) + 1
        if leg["misses"] >= _MAX_MISSES:
            logger.warning("%s: position %s vanished without history; marking closed", self.name, leg["ticket"])
            leg.update(state="closed", open_volume=0.0, reason=leg.get("reason") or "unknown")

    def _aggregate(self, pos: BrokerPosition) -> None:
        legs: list[dict[str, Any]] = pos.data.get("legs", [])
        step = float(pos.data.get("volume_step") or 0.0)
        filled = _snap(sum(float(leg.get("filled") or 0.0) for leg in legs), step)
        open_vol = _snap(sum(float(leg.get("open_volume") or 0.0) for leg in legs if leg["state"] == "open"), step)
        weighted = [(float(leg["entry"]), float(leg["filled"])) for leg in legs if leg.get("entry") and leg.get("filled")]
        wsum = sum(w for _, w in weighted)
        if wsum > 0:
            pos.avg_entry_price = sum(px * w for px, w in weighted) / wsum
        pos.filled_quantity = filled
        pos.remaining_quantity = open_vol
        realized = [float(leg["realized"]) for leg in legs if leg.get("realized") is not None]
        pos.realized_pnl = sum(realized) if realized else None
        live_sls = {leg["sl"] for leg in legs if leg["state"] in LIVE_STATES and leg.get("sl")}
        if len(live_sls) == 1:
            pos.stop_loss = live_sls.pop()
        # A filled leg without a stop on the venue: the engine alerts on this flag until it is fixed.
        pos.data["unprotected"] = any(leg["state"] == "open" and not leg.get("sl") for leg in legs)
        states = {leg["state"] for leg in legs}
        if "open" in states:
            pos.status = PositionStatus.OPEN
        elif "pending" in states:
            pos.status = PositionStatus.OPEN if filled > 0 else PositionStatus.PENDING
        elif not legs or states == {"failed"}:
            pos.status = PositionStatus.FAILED
        elif filled > 0:
            pos.status = PositionStatus.CLOSED
        else:
            pos.status = PositionStatus.CANCELLED
        reasons = [leg["reason"] for leg in legs if leg.get("reason")]
        pos.data["exit_reasons"] = reasons
        if any(leg.get("rest_order") is not None and leg["state"] in LIVE_STATES for leg in legs):
            pos.data["entry_done"] = False      # the rest of a partly filled entry still rests (monitor: its expiry)
        else:
            pos.data.pop("entry_done", None)

    # ---- management ----------------------------------------------------------
    def _copy(self, pos: BrokerPosition) -> tuple[BrokerPosition, list[dict[str, Any]]]:
        p = pos.model_copy(deep=True)
        legs = p.data.setdefault("legs", [])
        return p, legs

    async def update_stop_loss(self, pos: BrokerPosition, new_stop_loss: float) -> BrokerPosition:
        p, legs = self._copy(pos)
        await self._refresh_legs(p, legs)
        live = [leg for leg in legs if leg["state"] in LIVE_STATES]
        if not live:
            raise BrokerError("Stop güncellenemedi: açık ya da bekleyen pozisyon yok.")
        sl = round_to_tick(new_stop_loss, float(p.data.get("price_tick") or 0.0))
        sign = 1.0 if p.side is Side.LONG else -1.0
        side_word = "altında" if p.side is Side.LONG else "üstünde"
        if any(leg["state"] == "open" for leg in live):
            px = (await self._quote(p.broker_symbol)).exit_price_for(p.side)
            if sign * (px - sl) <= 0:
                raise BrokerError(
                    f"Yeni stop ({_num(sl)}) mevcut fiyatın ({_num(px)}) {side_word} olmalı; stop hemen "
                    f"tetiklenirdi. Stop değiştirilmedi."
                )
        for leg in live:
            if leg["state"] == "pending" and leg.get("entry") and sign * (float(leg["entry"]) - sl) <= 0:
                raise BrokerError(f"Yeni stop ({_num(sl)}) bekleyen giriş fiyatının ({_num(leg['entry'])}) {side_word} olmalı.")

        errors, changed = _LegErrors(), 0
        notes: list[str] = []
        for leg in live:
            leg.pop(SL_WANTED, None)        # (a newer stop move replaces an older one still being checked)
            try:
                await self._modify_leg(p.broker_symbol, p.side, leg, sl, leg.get("tp"))
                leg["sl"] = sl
                changed += 1
            except BrokerError as exc:
                _leg_failed(errors, f"#{leg['ticket']}: {exc}", exc)
                if answer_lost(exc) and leg["state"] == "open":
                    # It may or may not have been set: the next sync reads the leg's stop and sends it again when
                    # the venue still shows another one (_resend_wanted_stops).
                    leg[SL_WANTED] = sl
        if not changed:
            raise _fold("Stop güncellenemedi: ", "Stop değişikliğinin sonucu alınamadı: ", errors)
        for leg in live:
            if leg.get("rest_order") is None or leg["state"] != "open":
                continue
            # The rest of a partly filled order gets the same stop; one at or beyond its price is impossible
            # there (stop to entry, a trailed stop): that rest is cancelled instead.
            rest, lots = self._rest_view(leg), _num(float(leg.get("rest_volume") or 0.0))
            if rest.get("entry") and sign * (float(rest["entry"]) - sl) <= 0:
                if await self._cancel_rest(p.broker_symbol, leg, errors) == "removed":
                    notes.append(f"Emrin dolmamış kalan kısmı ({lots} lot) yeni stopla bekleyemeyeceği için iptal edildi.")
                continue
            try:
                await self._modify_leg(p.broker_symbol, p.side, rest, sl, leg.get("tp"))
            except BrokerError as exc:
                _leg_failed(errors, f"#{leg['rest_order']} (emrin dolmamış kalan kısmı): {exc}", exc)
        self._aggregate(p)
        if not errors:
            p.stop_loss = sl
        p.error = " ".join(([_some_failed("Stop bazı bacaklarda güncellenemedi: ",
                                          "Stop bazı bacaklarda güncellenemedi ya da sonucu alınamadı: ", errors)]
                            if errors else []) + notes) or None
        return p

    async def update_take_profits(self, pos: BrokerPosition, take_profits: list[TakeProfit]) -> BrokerPosition:
        if not take_profits:
            raise BrokerError("Yeni TP listesi boş; TP'ler değiştirilmedi.")
        p, legs = self._copy(pos)
        tick = float(p.data.get("price_tick") or 0.0)
        tps = [TakeProfit(price=round_to_tick(t.price, tick), close_fraction=t.close_fraction) for t in take_profits]
        await self._refresh_legs(p, legs)
        live = [leg for leg in legs if leg["state"] in LIVE_STATES]
        if not live:
            raise BrokerError("TP güncellenemedi: açık ya da bekleyen pozisyon yok.")

        warning = None
        targets: list[tuple[dict[str, Any], float]] = []
        if p.data.get("netting"):
            # An emulated TP that already partial-closed stays done: re-arming it would close another
            # slice of the remaining volume there (the trader's message usually still lists it).
            done_px = [float(e["price"]) for e in p.data.get("emulated_tps") or [] if e.get("done")]
            tol = tick / 2 if tick > 0 else 1e-9
            hit = [any(abs(t.price - px) <= tol for px in done_px) for t in tps]
            if all(hit):
                raise BrokerError("Yeni TP seviyelerinin hepsi zaten gerçekleşti; TP'ler değiştirilmedi.")
            last = max(i for i, h in enumerate(hit) if not h)     # venue TP: the last level not hit yet
            targets = [(live[0], tps[last].price)]
            s = sum(max(0.0, t.close_fraction) for t in tps)
            # A level whose partial close got no answer and is still being checked keeps that mark (TP_LOST): it is
            # not sent again before the account shows whether it went through.
            lost = [e for e in p.data.get("emulated_tps") or [] if isinstance(e.get(TP_LOST), dict) and not e.get("done")]
            p.data["emulated_tps"] = [
                {"index": i, "price": t.price, "fraction": (max(0.0, t.close_fraction) / s) if s > 0 else 1 / len(tps),
                 "done": hit[i]}
                for i, t in enumerate(tps) if i != last
            ]
            for e in p.data["emulated_tps"]:
                was = next((x for x in lost if abs(float(x["price"]) - e["price"]) <= tol), None)
                if was is not None and not e["done"]:
                    e[TP_LOST] = dict(was[TP_LOST])
            if any(hit):
                logger.info("%s: filled emulated TP level(s) kept done: %s", self.name,
                            [t.price for t, h in zip(tps, hit) if h])
        elif len(tps) == len(legs):
            targets = [(leg, tps[i].price) for i, leg in enumerate(legs) if leg["state"] in LIVE_STATES]
        elif len(tps) == len(live):
            targets = [(leg, tps[i].price) for i, leg in enumerate(live)]
        else:
            targets = [(leg, tps[min(i, len(tps) - 1)].price) for i, leg in enumerate(live)]
            warning = (
                f"TP sayısı ({len(tps)}) açık bacak sayısından ({len(live)}) farklı; bacaklar sırayla eşlendi, "
                f"fazla bacaklara son TP verildi."
            )

        sign = 1.0 if p.side is Side.LONG else -1.0
        px = None
        if any(leg["state"] == "open" for leg, _ in targets):
            px = (await self._quote(p.broker_symbol)).exit_price_for(p.side)
        for leg, tp in targets:
            base = px if leg["state"] == "open" else leg.get("entry")
            if base is not None and sign * (tp - float(base)) <= 0:
                raise BrokerError(f"Yeni TP ({_num(tp)}) fiyatın ({_num(base)}) yanlış tarafında; TP'ler değiştirilmedi.")

        errors, changed = _LegErrors(), 0
        for leg, tp in targets:
            keep_sl = leg.get("sl") or p.stop_loss
            if not keep_sl:   # never send sl=0 (would remove the stop)
                errors.append(f"#{leg['ticket']}: mevcut stop bilinmiyor, TP değiştirilmedi")
                continue
            try:
                await self._modify_leg(p.broker_symbol, p.side, leg, float(keep_sl), tp)
                leg["tp"] = tp
                changed += 1
            except BrokerError as exc:
                _leg_failed(errors, f"#{leg['ticket']}: {exc}", exc)
                continue
            if leg.get("rest_order") is not None and leg["state"] == "open":
                try:                        # the rest of a partly filled order gets the same target
                    await self._modify_leg(p.broker_symbol, p.side, self._rest_view(leg), float(keep_sl), tp)
                except BrokerError as exc:
                    _leg_failed(errors, f"#{leg['rest_order']} (emrin dolmamış kalan kısmı): {exc}", exc)
        if not changed:
            raise _fold("TP güncellenemedi: ", "TP değişikliğinin sonucu alınamadı: ", errors)
        p.take_profits = tps
        self._aggregate(p)
        msgs = ([warning] if warning else []) + ([_some_failed(
            "TP bazı bacaklarda güncellenemedi: ", "TP bazı bacaklarda güncellenemedi ya da sonucu alınamadı: ",
            errors)] if errors else [])
        p.error = " ".join(msgs) or None
        return p

    async def close_position(self, pos: BrokerPosition, fraction: float = 1.0) -> BrokerPosition:
        if not 0 < fraction <= 1 + 1e-9:
            raise BrokerError(f"Geçersiz kapatma oranı: {fraction}")
        p, legs = self._copy(pos)
        await self._refresh_legs(p, legs)
        step = float(p.data.get("volume_step") or 0.0)
        vmin = float(p.data.get("volume_min") or 0.0)
        open_legs = sorted((leg for leg in legs if leg["state"] == "open"), key=_tp_sort_key(p.side))
        errors = _LegErrors()
        notes: list[str] = []           # what the owner must know although nothing failed (a filled part closed)
        closed_any = False

        if fraction >= 1 - 1e-9:
            rests = [leg for leg in open_legs if leg.get("rest_order") is not None]
            if rests:
                # The rest of a partly filled order is cancelled first: it must not fill after the close. A part
                # that filled meanwhile shows in the position, read again before it is closed.
                for leg in rests:
                    closed_any |= await self._cancel_rest(p.broker_symbol, leg, errors) is not None
                try:
                    await self._refresh_legs(p, legs)
                except BrokerError as exc:
                    logger.warning("%s: refresh before close failed: %s", self.name, exc)
                open_legs = sorted((leg for leg in legs if leg["state"] == "open"), key=_tp_sort_key(p.side))
            for leg in open_legs:
                try:
                    await self._close_leg(p.broker_symbol, p.side, leg, None)
                    closed_any = True
                except BrokerError as exc:
                    _leg_failed(errors, f"#{leg['ticket']}: {exc}", exc)
                    errors.whole_closes.append((f"#{leg['ticket']}: {exc}", exc, leg))
            for leg in legs:
                if leg["state"] == "pending":
                    closed_any |= await self._cancel_or_close(p, leg, errors, notes)
        else:
            if not open_legs:
                raise BrokerError("Açık pozisyon yok; kısmi kapatma yapılamadı.")
            filled = _snap(sum(float(leg.get("filled") or 0.0) for leg in legs), step)
            open_total = _snap(sum(float(leg["open_volume"]) for leg in open_legs), step)
            target = min(floor_to_step(filled * fraction, step), open_total)
            if target <= 0 or target + _EPS < vmin:
                raise BrokerError(
                    f"Kapatılacak miktar ({_num(target)}) minimum lotun ({_num(vmin)}) altında; işlem yapılmadı."
                )
            closed_lots = 0.0
            lost_leg: tuple[dict[str, Any], BaseException] | None = None
            for leg in open_legs:  # nearest TP first
                if target <= _EPS:
                    break
                ov = float(leg["open_volume"])
                vol: float | None = None   # None = whole leg
                if target + _EPS < ov:
                    vol = floor_to_step(target, step)
                    if ov - vol + _EPS < vmin:    # would leave less than the minimum lot
                        vol = floor_to_step(ov - vmin, step)
                    if vol + _EPS < max(vmin, step):
                        continue
                try:
                    done = await self._close_leg(p.broker_symbol, p.side, leg, vol)
                    target = _snap(target - done, step)
                    closed_lots = _snap(closed_lots + done, step)
                    closed_any = True
                except BrokerError as exc:
                    _leg_failed(errors, f"#{leg['ticket']}: {exc}", exc)
                    if answer_lost(exc):
                        # That close may have executed: no further leg is closed while its outcome is unknown
                        # (closing the next leg too would close about twice what was asked).
                        lost_leg = (leg, exc)
                        break
            if lost_leg is not None and closed_any:
                # Some legs closed, then one close's answer was lost: how much of the partial close is done is not
                # known. Never reported as done: an unknown outcome (the engine owes the change and reads the
                # account before anything is sent again), with what is known handed back in ``position``.
                try:
                    await self._refresh_legs(p, legs)
                except BrokerError as exc:
                    logger.warning("%s: refresh after close failed: %s", self.name, exc)
                self._aggregate(p)
                leg, exc = lost_leg
                err = OrderOutcomeUnknownError(
                    f"Kısmi kapatmanın bir kısmı yapıldı ({_num(closed_lots)} lot kapatıldı), ardından "
                    f"#{leg['ticket']} için gönderilen kapatma emrinin sonucu alınamadı: {exc} "
                    f"Kısmi kapatmanın tamamlanıp tamamlanmadığı bilinmiyor.")
                err.__cause__ = exc
                err.position = p  # type: ignore[attr-defined]  # (what is known after the legs that did close)
                raise err

        if not closed_any and errors:
            whole = fraction >= 1 - 1e-9
            raise _fold("Pozisyon kapatılamadı: ", "Kapatma emrinin sonucu alınamadı: " if whole
                        else "Kısmi kapatma emrinin sonucu alınamadı: ", errors)
        try:
            await self._refresh_legs(p, legs)   # pick up realized PnL
            for text, exc, leg in errors.whole_closes:
                if answer_lost(exc) and leg["state"] in TERMINAL_STATES and text in errors:
                    # Its answer was lost, but the account shows the leg closed now: that close went through.
                    errors.remove(text)
                    errors.failures.remove(exc)
                    logger.info("%s: close of leg %s had no answer but the account shows it closed", self.name,
                                leg.get("ticket"))
        except BrokerError as exc:
            logger.warning("%s: refresh after close failed: %s", self.name, exc)
        self._aggregate(p)
        if fraction >= 1 - 1e-9 and not errors and not notes and p.remaining_quantity > _EPS:
            # A close deal can execute less than asked: the rest stays open (its stop stays on the venue).
            errors.append(f"{_num(p.remaining_quantity)} lot hâlâ açık (kapatma emri kısmen gerçekleşti; stop yerinde)")
        p.error = " ".join(([_some_failed("Bazı bacaklar kapatılamadı: ",
                                          "Bazı bacaklar kapatılamadı ya da kapatma sonucu alınamadı: ", errors)]
                            if errors else []) + notes) or None
        return p

    async def _cancel_or_close(
        self, p: BrokerPosition, leg: dict[str, Any], errors: list[str], notes: list[str] | None = None
    ) -> bool:
        try:
            await self._cancel_leg(p.broker_symbol, leg)
        except BrokerError as exc:
            try:
                await self._refresh_legs(p, [leg])   # maybe it just filled
            except BrokerError as exc_read:
                if not answer_lost(exc):
                    raise
                # The removal went out, its answer was lost, and the account cannot be read now: whether the order
                # is still there is not known (never "could not be cancelled").
                raise OrderOutcomeUnknownError(
                    f"#{leg['order_ticket']}: {exc} Ardından hesap da okunamadı ({exc_read}); emrin iptal edilip "
                    f"edilmediği bilinmiyor.") from exc
            if leg["state"] == "open":
                try:
                    await self._close_leg(p.broker_symbol, p.side, leg, None)
                    return True
                except BrokerError as exc2:
                    _leg_failed(errors, f"#{leg['ticket']}: {exc2}", exc2)
                    return False
            if leg["state"] in TERMINAL_STATES:
                return True
            _leg_failed(errors, f"#{leg['order_ticket']}: {exc}", exc)
            return False
        await self._after_cancel(p, leg, errors if notes is None else notes)
        return True

    async def _after_cancel(self, p: BrokerPosition, leg: dict[str, Any], notes: list[str]) -> None:
        """The pending order is removed. A part of it may have filled before (MetaTrader keeps a partly filled
        order resting with the rest, under the same ticket): the leg is booked cancelled only when the account
        shows no filled part. A filled part is closed at market (the trader closed or cancelled the trade); when
        that fails it stays an open leg the bot keeps managing. ``notes`` gets what the owner must know."""
        tag = leg.get("tag") or ""
        try:
            await self._refresh_legs(p, [leg])          # the order is gone now: a filled part shows as a position
            self._drop_rest(leg)                        # (a read that still lists the removed order: no rest left)
        except BrokerError as exc:
            leg["state"], leg["reason"] = "cancelled", "cancelled"
            notes.append(f"Bekleyen emir #{leg['order_ticket']} iptal edildi ama emrin dolmuş bir kısmı olup "
                         f"olmadığı okunamadı ({exc}); MetaTrader'da '{tag}' etiketli pozisyon var mı bakın.")
            return
        if leg["state"] == "pending":                   # not in the venue's history yet: the removal stands
            leg["state"], leg["reason"] = "cancelled", "cancelled"
            return
        if leg["state"] != "open":
            return                                      # cancelled; or filled and closed again (result booked)
        lots = _num(float(leg.get("open_volume") or 0.0))
        if leg.get("unconfirmed"):
            notes.append(f"Bekleyen emrin {lots} lotluk kısmı dolmuş ama pozisyonu henüz görünmüyor; açık pozisyon "
                         f"olarak takip ediliyor. MetaTrader'da '{tag}' etiketli pozisyona bakın.")
            return
        try:
            await self._close_leg(p.broker_symbol, p.side, leg, None)
        except BrokerError as exc:
            stop = ", stopu yerinde" if leg.get("sl") else ""
            what = "kapatma emrine yanıt alınamadı" if answer_lost(exc) else "kapatılamadı"
            notes.append(f"Bekleyen emrin {lots} lotluk kısmı dolmuştu ve {what} ({exc}); o kısım açık "
                         f"pozisyon olarak takip ediliyor{stop}.")
            return
        notes.append(f"Bekleyen emrin {lots} lotluk kısmı dolmuştu; o kısım piyasa fiyatından kapatıldı.")

    async def cancel_entry_remainder(self, pos: BrokerPosition) -> BrokerPosition:
        """Cancel only the still resting rest of a partly filled entry (the monitor does this once
        pending_entry_expiry_min has passed); the filled part stays open with its stop."""
        p, legs = self._copy(pos)
        await self._refresh_legs(p, legs)
        errors: list[str] = []
        for leg in legs:
            if leg.get("rest_order") is not None and leg["state"] in LIVE_STATES:
                await self._cancel_rest(p.broker_symbol, leg, errors)
        self._aggregate(p)
        if errors:
            p.error = "; ".join(errors)
        return p

    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        p, legs = self._copy(pos)
        await self._refresh_legs(p, legs)
        if any(leg["state"] == "open" for leg in legs):
            return await self.close_position(p, 1.0)
        errors = _LegErrors()
        notes: list[str] = []
        changed = False
        for leg in legs:
            if leg["state"] == "pending":
                changed |= await self._cancel_or_close(p, leg, errors, notes)
        if not changed and errors:
            raise _fold("Bekleyen emir iptal edilemedi: ", "Bekleyen emrin iptal isteğinin sonucu alınamadı: ", errors)
        try:
            await self._refresh_legs(p, legs)
        except BrokerError as exc:
            logger.warning("%s: refresh after cancel failed: %s", self.name, exc)
        self._aggregate(p)
        p.error = " ".join(([_some_failed("Bazı emirler iptal edilemedi: ",
                                          "Bazı emirler iptal edilemedi ya da iptal sonucu alınamadı: ", errors)]
                            if errors else []) + notes) or None
        return p

    async def sync_position(self, pos: BrokerPosition) -> BrokerPosition:
        p, legs = self._copy(pos)
        if p.status in (PositionStatus.CLOSED, PositionStatus.CANCELLED, PositionStatus.FAILED) and not any(
            leg["state"] in TERMINAL_STATES and leg.get("realized") is None and leg.get("filled", 0) > 0
            and leg.get("pnl_tries", 0) < _MAX_PNL_TRIES for leg in legs
        ):
            return p
        await self._refresh_legs(p, legs)
        errors: list[str] = []
        for leg in legs:
            if leg.get("rest_order") is not None and leg["state"] not in LIVE_STATES:
                # The filled part of a partly filled order has closed (stop / target) while the rest still rests:
                # it would open a new position after the trade ended. Cancelled.
                lots = _num(float(leg.get("rest_volume") or 0.0))
                if await self._cancel_rest(p.broker_symbol, leg, errors) == "removed":
                    errors.append(f"Emrin dolmuş kısmı kapandı; dolmamış kalan kısmı ({lots} lot) iptal edildi.")
        # Rule 1: a filled leg must always carry a stop.
        for leg in legs:
            if leg["state"] == "open" and leg.get("sl") is None and p.stop_loss:
                err = await self._protect_or_close(p.broker_symbol, p.side, leg, float(p.stop_loss), leg.get("tp"))
                if err:
                    errors.append(err)
        await self._resend_wanted_stops(p, legs, errors)
        if p.data.get("emulated_tps"):
            await self._run_emulated_tps(p, legs, errors)
        self._aggregate(p)
        if p.data.get("unprotected") and not any("KRİTİK" in e for e in errors):
            errors.append(f"KRİTİK: {p.broker_symbol} pozisyonunda stop yok ve eklenemedi; hemen kontrol edin!")
        if errors:
            p.error = "; ".join(errors)
        elif p.error and "KRİTİK" in p.error:
            p.error = None              # every filled leg carries its stop again
        return p

    async def _resend_wanted_stops(self, p: BrokerPosition, legs: list[dict[str, Any]], errors: list[str]) -> None:
        """A stop move whose answer was lost on a leg (SL_WANTED, set by update_stop_loss): the account has just been
        read. When the leg shows that stop it went through; otherwise it is sent again (the leg kept its old stop).
        A refusal now is reported and not tried again; another lost answer is looked at at the next sync."""
        tick = float(p.data.get("price_tick") or 0.0)
        tol = tick / 2 if tick > 0 else 1e-9
        for leg in legs:
            want = leg.get(SL_WANTED)
            if want is None:
                continue
            if leg["state"] != "open":
                leg.pop(SL_WANTED, None)
                continue
            if leg.get("sl") is not None and abs(float(leg["sl"]) - float(want)) <= tol:
                leg.pop(SL_WANTED, None)
                logger.info("%s: stop move of leg %s had no answer; the account shows it set", self.name,
                            leg.get("ticket"))
                continue
            try:
                await self._modify_leg(p.broker_symbol, p.side, leg, float(want), leg.get("tp"))
            except BrokerError as exc:
                if answer_lost(exc):
                    errors.append(f"#{leg['ticket']}: stop {_num(float(want))} yeniden gönderildi, yine yanıt "
                                  f"alınamadı ({exc}); bot sonraki kontrolde yeniden bakacak.")
                    continue
                leg.pop(SL_WANTED, None)
                errors.append(f"#{leg['ticket']}: stop {_num(float(want))} bu bacakta yeniden gönderildi ama "
                              f"uygulanamadı ({exc}); bu bacağın stopu {_num(leg.get('sl'))} olarak kaldı.")
                continue
            leg["sl"] = float(want)
            leg.pop(SL_WANTED, None)
            logger.info("%s: stop %s sent again to leg %s (its earlier answer was lost) and set", self.name, want,
                        leg.get("ticket"))

    async def _run_emulated_tps(self, p: BrokerPosition, legs: list[dict[str, Any]], errors: list[str]) -> None:
        """Netting accounts: partial-close at intermediate TPs the venue cannot hold."""
        todo = [e for e in p.data.get("emulated_tps", []) if not e.get("done")]
        leg = next((x for x in legs if x["state"] == "open"), None)
        if not todo or leg is None:
            return
        if self._settle_lost_tps(todo, leg, errors):
            return          # (one TP's partial close is still unknown: nothing else is sent meanwhile)
        todo = [e for e in todo if not e.get("done")]
        px = (await self._quote(p.broker_symbol)).exit_price_for(p.side)
        sign = 1.0 if p.side is Side.LONG else -1.0
        step = float(p.data.get("volume_step") or 0.0)
        vmin = float(p.data.get("volume_min") or 0.0)
        for e in sorted(todo, key=lambda e: sign * e["price"]):
            if sign * (px - e["price"]) < 0 or leg["state"] != "open":
                continue
            ov = float(leg["open_volume"])
            vol = min(floor_to_step(float(leg["filled"]) * e["fraction"], step), ov)
            if ov - vol + _EPS < vmin:
                vol = floor_to_step(ov - vmin, step)
            if vol + _EPS < max(vmin, step):
                e["done"] = True
                errors.append(f"TP{e['index'] + 1} ({_num(e['price'])}) için kısmi kapatma lotu minimumun altında; atlandı.")
                continue
            try:
                await self._close_leg(p.broker_symbol, p.side, leg, vol)
                e["done"] = True
                logger.info("%s: emulated TP%d hit at %s, closed %s", self.name, e["index"] + 1, px, vol)
            except BrokerError as exc:
                if answer_lost(exc):
                    # It may have been executed: not sent again while the account does not show whether it was
                    # (a second one would close another slice). Done once the open volume shows the drop
                    # (_settle_lost_tps); nothing else is sent meanwhile.
                    e[TP_LOST] = {"from": ov, "at": self._wall_clock()}
                    errors.append(f"TP{e['index'] + 1} ({_num(e['price'])}) kısmi kapatma emrine yanıt alınamadı "
                                  f"({exc}); tekrar gönderilmedi, bot hesabı okuyup gerçekleşip gerçekleşmediğine "
                                  f"bakacak.")
                    break
                errors.append(f"TP{e['index'] + 1} kısmi kapatma başarısız: {exc}")
        try:
            await self._refresh_legs(p, legs)
        except BrokerError as exc:
            # The TPs done above must reach the database with this sync (else the next one would close them again).
            logger.warning("%s: refresh after the emulated take profits failed: %s", self.name, exc)
            return
        lost = [e for e in todo if e.get(TP_LOST) and not e.get("done")]
        if lost and leg["state"] == "open":
            self._settle_lost_tps(lost, leg, errors, quiet=True)

    def _wall_clock(self) -> float:
        """Seconds since the epoch (stored with the position: it must hold across a restart)."""
        return time.time()

    def _settle_lost_tps(self, todo: list[dict[str, Any]], leg: dict[str, Any], errors: list[str], *,
                         quiet: bool = False) -> bool:
        """Emulated TPs whose partial close got no answer (TP_LOST): done once the leg's open volume has dropped
        below what it was when that close was sent; after TP_LOST_WAIT_SEC without the drop it did not execute and
        it may be sent again. True while one is still unknown (nothing else is sent meanwhile)."""
        waiting = False
        for e in todo:
            lost = e.get(TP_LOST)
            if not isinstance(lost, dict) or e.get("done"):
                continue
            if float(leg["open_volume"]) + _EPS < float(lost.get("from") or 0.0):
                e["done"] = True
                e.pop(TP_LOST, None)
                logger.info("%s: emulated TP%d had no answer; the account shows it executed", self.name,
                            e["index"] + 1)
            elif self._wall_clock() - float(lost.get("at") or 0.0) >= TP_LOST_WAIT_SEC:
                e.pop(TP_LOST, None)
                if not quiet:
                    errors.append(f"TP{e['index'] + 1} ({_num(e['price'])}) kısmi kapatma emri hesapta "
                                  f"gerçekleşmemiş görünüyor; fiyat hâlâ hedefteyse yeniden gönderilecek.")
            else:
                waiting = True
        return waiting


# --------------------------------------------------------------------------- #
# Native MetaTrader5 terminal
# --------------------------------------------------------------------------- #
_ACTIVE_LOGINS: dict[int, int] = {}     # id(MetaTrader5 module) -> login: the package drives ONE terminal per process


def _last_error(mt5: Any) -> tuple[int, str]:
    try:
        code, msg = mt5.last_error()
        return int(code), str(msg)
    except Exception:  # noqa: BLE001 - diagnostics only
        return -1, "unknown"


class OrderReplyLostError(OrderOutcomeUnknownError):
    """The reply to an order request was lost (IPC loss, or the terminal did not answer in time): the order may or
    may not exist on the venue. Never re-sent; the caller looks it up by its tag. (An OrderOutcomeUnknownError, and
    so still a BrokerError.)"""


_LOOK_AT_TERMINAL_TR = (
    "MT5 penceresine bakın: donmuşsa ya da bir uyarı penceresi açık kalmışsa kapatın, gerekirse terminali "
    "yeniden başlatın."
)
_NOT_CONNECTED_TR = (
    "MT5 terminali işlem sunucusuna bağlı değil (terminalin sağ alt köşesindeki bağlantı göstergesine bakın; "
    "internet ya da hesap girişi kopmuş olabilir). Bağlantı gelene kadar emir gönderilemez"
)
_ALGO_OFF_TR = (
    "MT5'te üst araç çubuğundaki 'Algo Trading' düğmesi KAPALI. Emir gönderilebilmesi için "
    "yeşil yapın (Araçlar > Seçenekler > Uzman Danışmanlar > algoritmik ticarete izin ver)"
)
# The option's exact label differs between terminal builds / languages ("Disable automated trading via external
# Python API", newer builds say "algorithmic"): the text names the tab and the words that are always in it.
_PYTHON_API_BOX_TR = (
    "içinde 'Python API' geçen devre dışı bırakma kutusu (İngilizce arayüzde: 'Disable automated trading via "
    "external Python API' ya da 'Disable algorithmic trading via external Python API')"
)
_PYTHON_API_OFF_TR = (
    "MT5'te Python üzerinden işlem KAPALI. Emir gönderilebilmesi için Araçlar > Seçenekler > Uzman Danışmanlar "
    f"sekmesinde, {_PYTHON_API_BOX_TR} işaretli olmamalı: işareti KALDIRIP Tamam'a basın"
)
# order_send was refused for "auto-trading disabled" but terminal_info() does not show which switch it is.
_CHECK_SWITCHES_TR = (
    "MT5'te şu iki ayara bakın: (1) üst araç çubuğundaki 'Algo Trading' düğmesi yeşil olmalı; (2) Araçlar > "
    f"Seçenekler > Uzman Danışmanlar sekmesinde, {_PYTHON_API_BOX_TR} işaretli OLMAMALI"
)


# The same three problems in a few words each, for health_problem(): the engine's heartbeat puts that text into
# one alert line and cuts it at 300 characters, so all three together (plus "MT5 terminali: ") must stay below.
_NOT_CONNECTED_SHORT_TR = "işlem sunucusuna bağlı değil (sağ alt köşedeki bağlantı göstergesine bakın)"
_ALGO_OFF_SHORT_TR = "'Algo Trading' düğmesi kapalı (üst araç çubuğunda tıklayıp yeşil yapın)"
_PYTHON_API_OFF_SHORT_TR = (
    "Python ile işlem kapalı (Araçlar > Seçenekler > Uzman Danışmanlar: 'Python API' geçen kutunun işaretini "
    "kaldırın)"
)


def trade_switches_off(info: Any, *, short: bool = False) -> list[str]:
    """The terminal switches that refuse orders from this program, each as a Turkish text saying where to
    click: the Algo Trading button (trade_allowed) and trading through the Python API (tradeapi_disabled).
    ``short``: the few-word forms of health_problem()."""
    out: list[str] = []
    if not getattr(info, "trade_allowed", True):
        out.append(_ALGO_OFF_SHORT_TR if short else _ALGO_OFF_TR)
    if getattr(info, "tradeapi_disabled", False):
        out.append(_PYTHON_API_OFF_SHORT_TR if short else _PYTHON_API_OFF_TR)
    return out


def terminal_problems(info: Any, *, short: bool = False) -> list[str]:
    """Everything in terminal_info() that stops orders: no connection to the trade server, then the switches."""
    not_connected = _NOT_CONNECTED_SHORT_TR if short else _NOT_CONNECTED_TR
    return ([] if getattr(info, "connected", True) else [not_connected]) + trade_switches_off(info, short=short)


class TerminalTimeoutError(BrokerError):
    """The terminal did not answer a native call in time (or is still stuck in an earlier one).

    ``started``: the call was handed to the terminal and may still complete there; False: it never left the
    queue (or was refused by the circuit breaker), so nothing was sent."""

    def __init__(self, reason: str, *, started: bool) -> None:
        super().__init__(f"{reason}. {_LOOK_AT_TERMINAL_TR}")
        self.reason = reason
        self.started = started


class _NativeCall:
    """One call handed to the worker thread. Created, read and changed on the event-loop thread only; the worker
    thread touches nothing but ``future`` (a thread-safe concurrent.futures.Future), sets ``relink`` (a
    threading.Event) when it starts logging in to the terminal again, and appends the time at which it started
    the call to ``began`` (one append, never changed afterwards; the event-loop thread only reads it)."""

    __slots__ = ("future", "what", "submitted", "began", "limit", "timed_out", "relink")

    def __init__(
        self, future: ThreadFuture[Any], what: str, limit: float, relink: threading.Event | None,
        began: list[float],
    ) -> None:
        self.future = future
        self.what = what
        self.submitted = time.monotonic()
        self.began = began              # [time the worker thread started the call]; empty while it is queued
        self.limit = limit              # seconds inside the terminal after which the call counts as stuck
        self.timed_out = False          # a caller ran out of time waiting for it
        self.relink = relink

    def running_for(self) -> float:
        """Seconds the terminal has been busy with this call (0 while it still waits behind another call)."""
        return time.monotonic() - self.began[0] if self.began else 0.0

    def relinking(self) -> bool:
        """The call is (or includes) a log-in to the terminal: initialize(), which has its own, longer limit."""
        return self.what == "initialize" or (self.relink is not None and self.relink.is_set())

    def stuck_after(self) -> float:
        extra = INIT_TIMEOUT_SEC if self.relink is not None and self.relink.is_set() else 0.0
        return self.limit + extra


def _err_text(err: tuple[int, str] | None) -> str:
    return f"{err[1]} (kod {err[0]})" if err else "bilinmeyen hata"


def _f(x: Any) -> float:
    try:
        return float(x or 0.0)
    except (TypeError, ValueError):
        return 0.0


class Mt5Broker(MetaTraderLegBroker):
    """MetaTrader 5 via the official ``MetaTrader5`` package (Windows, terminal installed).

    The package is blocking and not thread-safe, so every call runs on one
    dedicated worker thread. ``mt5_module`` injects a fake for tests.

    The package has no timeout of its own: a terminal that stops answering would block the worker thread, and
    with it every later call, for ever. ``_run`` therefore waits at most CALL_TIMEOUT_SEC for a call. A call
    that ran out of time cannot be interrupted; while it is still inside the terminal the circuit breaker
    (``_stuck_call``) fails every later call at once instead of queueing it, and closes by itself as soon as
    that call returns. All of this bookkeeping (``_calls``) lives on the event-loop thread; the worker thread
    only runs the native function and completes its future.
    """

    kind = "mt5"
    tag_limit = 29                       # the MetaTrader5 package refuses a comment longer than 29 characters
    tick_wait_sec: float = 0.3
    requote_delay_sec: float = 0.4       # pause before a requote is retried with a fresh price
    transient_retry_sec: float = 1.5     # pause before the single retry after a transient refusal

    def __init__(self, config: Mt5BrokerConfig, *, mt5_module: Any | None = None) -> None:
        super().__init__(config)
        self.startup_warnings: list[str] = []
        self.cfg = config
        self._mt5: Any | None = mt5_module
        self._executor: ThreadPoolExecutor | None = None
        self._calls: deque[_NativeCall] = deque()      # handed to the worker thread and not finished, oldest first
        self._connected = False
        self._filling_cache: dict[str, int] = {}

    # ---- plumbing ------------------------------------------------------------
    def _initialize(self, mt5: Any) -> bool:
        kwargs: dict[str, Any] = {
            "login": int(self.cfg.login), "password": self.cfg.password, "server": self.cfg.server, "timeout": 60_000,
        }
        if self.cfg.terminal_path:
            return bool(mt5.initialize(self.cfg.terminal_path, **kwargs))
        return bool(mt5.initialize(**kwargs))

    def _oldest_call(self) -> _NativeCall | None:
        """The call the worker thread is busy with: the oldest one that has not finished (one thread, FIFO)."""
        while self._calls and self._calls[0].future.done():
            self._calls.popleft()
        return self._calls[0] if self._calls else None

    def _stuck_call(self) -> _NativeCall | None:
        """Circuit breaker: the call the worker thread is stuck in, i.e. one that ran out of time (its caller
        gave up, or its caller is gone and the terminal has been busy with it for longer than its limit) and
        has still not returned. None again as soon as it returns. The time a call spent waiting behind another
        call (e.g. a re-login) does not count: only the time inside the terminal does."""
        call = self._oldest_call()
        if call is not None and (call.timed_out or call.running_for() >= call.stuck_after()):
            return call
        return None

    async def _wait_terminal(self, max_wait: float) -> bool:
        """Wait, at most ``max_wait`` seconds, until no timed-out call is stuck in the worker thread any more.
        True = the terminal answers again."""
        deadline = time.monotonic() + max_wait
        while self._stuck_call() is not None:
            left = deadline - time.monotonic()
            if left <= 0:
                return False
            await asyncio.sleep(min(0.2, left))
        return True

    def _log_late_result(self, call: _NativeCall, future: ThreadFuture[Any]) -> None:
        """Done-callback of a call that had timed out (runs on the worker thread: logging only)."""
        waited = time.monotonic() - call.submitted
        try:
            result = future.result()
        except BaseException as exc:  # noqa: BLE001 - diagnostics only
            logger.warning("%s: MT5 %s that timed out ended after %.0f s with %r", self.name, call.what, waited, exc)
            return
        # Only an order result is written out: it is the only trace of what happened to that order.
        detail = f": {str(result)[:500]}" if call.what == "order_send" else ""
        logger.warning("%s: MT5 %s that timed out returned after %.0f s; the terminal answers again%s",
                       self.name, call.what, waited, detail)

    async def _run(
        self, fn: Callable[[Any], T], *, timeout: float | None = None, what: str = "call",
        relink: threading.Event | None = None,
    ) -> T:
        """Run ``fn(mt5)`` on the worker thread and wait at most ``timeout`` (default CALL_TIMEOUT_SEC) seconds.

        Raises TerminalTimeoutError when the time is up, or at once while an earlier call is still stuck.
        ``relink``: set by ``fn`` when it starts logging in to the terminal again (see _call). Logging in has
        its own, longer limit (INIT_TIMEOUT_SEC) that does not count against a call with the default limit,
        whether the call does it itself or waits in the queue behind it."""
        if self._mt5 is None or self._executor is None:
            raise BrokerError(f"{self.name}: MT5 bağlantısı yok.")
        limit = CALL_TIMEOUT_SEC if timeout is None else float(timeout)
        stuck = self._stuck_call()
        if stuck is not None:
            # Circuit breaker: do not queue behind a call that the terminal never answered.
            waited = time.monotonic() - stuck.submitted
            raise TerminalTimeoutError(
                f"MetaTrader terminali {waited:.0f} sn'dir yanıt vermiyor (önceki istek hâlâ bekliyor)",
                started=False)
        began: list[float] = []

        def work(mt5: Any) -> T:
            began.append(time.monotonic())      # worker thread: the terminal is busy with this call from now on
            return fn(mt5)

        future = self._executor.submit(work, self._mt5)
        stuck_after = max(limit, INIT_TIMEOUT_SEC if what == "initialize" else CALL_TIMEOUT_SEC)
        call = _NativeCall(future, what, stuck_after, relink, began)
        self._calls.append(call)
        answer = asyncio.wrap_future(future)
        deadline = call.submitted + limit
        patient = timeout is None       # a caller with its own limit (the health check) never waits longer
        try:
            while True:
                try:
                    # shield: running out of time must not cancel a call that may still be given more of it
                    return await asyncio.wait_for(asyncio.shield(answer), max(0.0, deadline - time.monotonic()))
                except asyncio.TimeoutError:
                    if future.done() and not future.cancelled():
                        return future.result()          # answered at the very last moment
                    busy = self._oldest_call()
                    if patient and busy is not None and busy.relinking():
                        # The worker thread is logging in to the terminal again: wait for that as well, once.
                        patient = False
                        deadline = time.monotonic() + INIT_TIMEOUT_SEC + (0.0 if busy is call else limit)
                        continue
                    break
        except asyncio.CancelledError:
            answer.cancel()                 # the caller was cancelled: drop the call if it is still queued
            raise
        waited = time.monotonic() - call.submitted
        started = not future.cancel()       # cancel() succeeds only while it is still queued: it never runs
        answer.cancel()
        blocker = call if started else self._oldest_call()
        if blocker is not None and (timeout is None or limit >= blocker.limit):
            blocker.timed_out = True        # later calls fail at once until it returns
        if started:
            future.add_done_callback(lambda f, c=call: self._log_late_result(c, f))
        logger.error("%s: MT5 %s got no answer within %.3g s (%s)", self.name, what, waited,
                     "still running in the terminal" if started else "never started: the terminal is busy")
        secs = f"{limit:g}" if waited < limit + 1.0 else f"{waited:.0f}"
        raise TerminalTimeoutError(f"MetaTrader terminali {secs} sn içinde yanıt vermedi", started=started)

    async def _call(
        self, name: str, *args: Any, call_timeout: float | None = None, **kwargs: Any
    ) -> tuple[Any, tuple[int, str] | None]:
        relink = threading.Event()

        def job(mt5: Any) -> tuple[Any, tuple[int, str] | None]:
            func = getattr(mt5, name)

            def invoke() -> Any:
                # Never pass an EMPTY keyword dict: the MetaTrader5 package's order_send / order_check refuse a
                # positional request together with ANY keyword dict, even an empty one, and answer
                # (-2, "Unnamed arguments not allowed") without sending anything. ``f(*args, **{})`` hands a
                # C function an empty dict, not "no keywords".
                return func(*args, **kwargs) if kwargs else func(*args)

            res = invoke()
            if res is not None:
                return res, None
            err = _last_error(mt5)
            if err[0] in _IPC_ERRORS:
                relink.set()            # logging in again may take much longer than a call: tell the callers
            if err[0] in _IPC_ERRORS and self._initialize(mt5):   # terminal link dropped: re-login once
                logger.warning("%s: MT5 IPC error %s, re-initialized", self.name, err)
                if name == "order_send":
                    # Never re-send: the first request may have reached the server and only the reply was lost,
                    # a second send would duplicate the order. The caller looks the order up by its tag.
                    return None, err
                res = invoke()
                if res is not None:
                    return res, None
                err = _last_error(mt5)
            return None, err
        return await self._run(job, timeout=call_timeout, what=name, relink=relink)

    async def _get(self, name: str, *args: Any, what: str, **kwargs: Any) -> Any:
        res, err = await self._call(name, *args, **kwargs)
        if res is None:
            raise BrokerError(f"MT5 {what} alınamadı: {_err_text(err)}")
        return res

    async def _get_list(self, name: str, *args: Any, what: str, **kwargs: Any) -> tuple[Any, ...]:
        res, err = await self._call(name, *args, **kwargs)
        if res is None:
            if err and err[0] in (MT5.RES_S_OK, MT5.RES_E_NOT_FOUND):
                return ()
            raise BrokerError(f"MT5 {what} alınamadı: {_err_text(err)}")
        return tuple(res)

    @staticmethod
    def _lost_reply_text(reason: str, request: dict[str, Any]) -> str:
        """Owner-facing text of a lost order reply. It is shown only when the order could not be found by its
        comment either (an entry), or for a request that is not looked up at all (close / change / cancel)."""
        entry = request.get("action") == MT5.TRADE_ACTION_PENDING or (
            request.get("action") == MT5.TRADE_ACTION_DEAL and "position" not in request
        )
        text = (
            f"MT5 emir yanıtı alınamadı ({reason}). Emir sunucuya ulaşmış olabilir; aynı emir "
            f"ikinci kez GÖNDERİLMEDİ. Gerçekleşip gerçekleşmediği doğrulanamadı: MT5'te "
            f"{request.get('symbol') or 'ilgili'} emir/pozisyonlarına elle bakın"
        )
        if entry:   # nothing tracks an entry that went through after all
            text += "; emir gerçekleşmişse bot o pozisyonu takip ETMİYOR, kendiniz yönetin ya da kapatın"
        return text + "."

    async def _trading_off_text(self) -> str:
        """order_send refused with last_error -8 (auto-trading disabled): which switch it is, where to click."""
        problems: list[str] = []
        try:
            info, _ = await self._call("terminal_info")
            problems = trade_switches_off(info) if info is not None else []
        except BrokerError as exc:
            logger.warning("%s: terminal_info failed after a refused order: %s", self.name, exc)
        if not problems:        # the terminal does not say which switch it is: tell the owner to check both
            problems = [_CHECK_SWITCHES_TR]
        return ("MT5 terminalinde otomatik işlem kapalı olduğu için emir sunucuya GÖNDERİLMEDİ. "
                + ". ".join(problems) + ".")

    async def _send(self, request: dict[str, Any]) -> Any:
        try:
            res, err = await self._call("order_send", dict(request))
        except TerminalTimeoutError as exc:
            if not exc.started:
                raise TerminalTimeoutError(f"{exc.reason}; emir gönderilmedi", started=False) from exc
            # Handed to the terminal, no answer in time: it may still be executed there. A lost reply: never
            # re-sent, looked up by its comment once the terminal answers again.
            raise OrderReplyLostError(self._lost_reply_text(exc.reason, request)) from exc
        if res is None:
            code = err[0] if err else None
            if code in _IPC_ERRORS:
                # The link to the terminal broke while the request was under way: it may have gone through.
                raise OrderReplyLostError(self._lost_reply_text(_err_text(err), request))
            if code == MT5.RES_E_AUTO_TRADING_DISABLED:
                raise BrokerError(await self._trading_off_text())
            # Refused by the package or the terminal before anything was sent (e.g. an invalid argument).
            raise BrokerError(f"MT5 emir gönderilemedi: {_err_text(err)}")
        logger.info(
            "%s: order_send action=%s type=%s vol=%s price=%s sl=%s tp=%s -> retcode=%s order=%s %s",
            self.name, request.get("action"), request.get("type"), request.get("volume"), request.get("price"),
            request.get("sl"), request.get("tp"), res.retcode, getattr(res, "order", None), getattr(res, "comment", ""),
        )
        return res

    def _require(self) -> None:
        if not self._connected:
            raise BrokerError(f"{self.name}: MT5 bağlı değil (connect çağrılmadı).")

    # ---- lifecycle -------------------------------------------------------------
    async def connect(self) -> None:
        if self._connected:
            return
        if self._mt5 is None:
            if sys.platform != "win32":
                raise BrokerError(
                    "MetaTrader5 Python paketi yalnızca Windows'ta çalışır. Linux/Docker için "
                    "'metaapi' broker tipini kullanın."
                )
            try:
                self._mt5 = importlib.import_module("MetaTrader5")
            except ImportError as exc:
                raise BrokerError("MetaTrader5 paketi kurulu değil: pip install MetaTrader5") from exc
        key = id(self._mt5)
        other = _ACTIVE_LOGINS.get(key)
        if other is not None and other != int(self.cfg.login):
            raise BrokerError(
                f"Aynı programda yalnızca bir MT5 hesabı kullanılabilir (zaten bağlı: {other}). "
                f"İkinci hesap için ayrı bir kopya çalıştırın ya da 'metaapi' kullanın."
            )
        if self._oldest_call() is not None:
            # A call of the previous connection is still inside the terminal: the package is not thread-safe,
            # so no second worker thread may be started next to it.
            raise BrokerError(
                "MetaTrader terminali önceki isteğe hâlâ yanıt vermedi; yeniden bağlanılamıyor. "
                + _LOOK_AT_TERMINAL_TR
            )
        self.startup_warnings = []
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"mt5-{self.name}")
        try:
            ok, err = await self._run(
                lambda m: (self._initialize(m), _last_error(m)), timeout=INIT_TIMEOUT_SEC, what="initialize"
            )
            if not ok:
                raise BrokerError(
                    f"MT5 terminaline bağlanılamadı: {_err_text(err)}. Terminal kurulu mu, "
                    f"login/şifre/sunucu doğru mu?"
                )
            acc = await self._get("account_info", what="hesap bilgisi")
            if int(acc.login) != int(self.cfg.login):
                raise BrokerError(f"MT5 terminali farklı bir hesaba bağlı ({acc.login}, beklenen {self.cfg.login}).")
            if not getattr(acc, "trade_allowed", True):
                raise BrokerError("Bu MT5 hesabında işlem izni yok (yatırımcı şifresi mi girildi?).")
            if not getattr(acc, "trade_expert", True):
                raise BrokerError("MT5 hesabında otomatik (EA) işlem kapalı.")
            if hasattr(self._mt5, "terminal_info"):
                ti, _ = await self._call("terminal_info")
                for problem in terminal_problems(ti) if ti is not None else []:
                    # Not fatal: each of them can be put right in the terminal without restarting the bot.
                    # Until then an order fails with an actionable text of its own (10027 / -8 / 10031).
                    warning = f"{problem}; programı yeniden başlatmanız gerekmez."
                    self.startup_warnings.append(warning)
                    logger.warning("%s: %s", self.name, warning)
            self._netting = int(getattr(acc, "margin_mode", MT5.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING)) != (
                MT5.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING
            )
        except BaseException:
            await self._shutdown_terminal()
            raise
        _ACTIVE_LOGINS[key] = int(self.cfg.login)
        self._connected = True
        logger.info(
            "%s: MT5 connected login=%s server=%s currency=%s netting=%s",
            self.name, acc.login, self.cfg.server, getattr(acc, "currency", "?"), self._netting,
        )

    async def _shutdown_terminal(self) -> None:
        if self._executor is None:
            return
        try:
            await self._run(lambda m: m.shutdown())
        except Exception as exc:  # noqa: BLE001 - best effort on shutdown
            logger.warning("%s: MT5 shutdown failed: %s", self.name, exc)
        self._executor.shutdown(wait=False)
        self._executor = None

    async def close(self) -> None:
        if self._mt5 is not None and _ACTIVE_LOGINS.get(id(self._mt5)) == int(self.cfg.login):
            _ACTIVE_LOGINS.pop(id(self._mt5), None)
        self._connected = False
        await self._shutdown_terminal()

    async def health_problem(self) -> str | None:
        """Heartbeat check: None when the terminal answers, is logged in to our account, is connected to the
        trade server and lets this program trade; otherwise a short Turkish text naming the problem and what to
        do about it. Bounded (two short calls, circuit breaker); never raises a BrokerError."""
        if not self._connected:
            return "MT5 bağlı değil."
        limit = min(HEALTH_TIMEOUT_SEC, CALL_TIMEOUT_SEC)
        try:
            acc, err = await self._call("account_info", call_timeout=limit)
            if acc is None:
                return (f"MT5 hesap bilgisi alınamadı: {_err_text(err)}. Terminal açık mı ve hesaba giriş "
                        f"yapılmış mı, bakın.")
            if int(acc.login) != int(self.cfg.login):
                return (f"MT5 terminali farklı bir hesaba bağlı ({acc.login}, beklenen {self.cfg.login}). "
                        f"Terminalde doğru hesaba giriş yapın.")
            if not getattr(acc, "trade_allowed", True):
                return "Bu MT5 hesabında işlem izni yok (yatırımcı şifresiyle mi giriş yapıldı?)."
            if not getattr(acc, "trade_expert", True):
                return "MT5 hesabında otomatik (EA) işlem kapalı; brokerınıza sorun."
            if not hasattr(self._mt5, "terminal_info"):
                return None
            info, err = await self._call("terminal_info", call_timeout=limit)
        except BrokerError as exc:
            return str(exc)
        if info is None:
            return f"MT5 terminal bilgisi alınamadı: {_err_text(err)}."
        problems = terminal_problems(info, short=True)
        return ("MT5 terminali: " + "; ".join(problems) + ".") if problems else None

    # ---- account -----------------------------------------------------------------
    async def _account(self) -> Any:
        self._require()
        return await self._get("account_info", what="hesap bilgisi")

    async def get_equity(self) -> float:
        acc = await self._account()
        return float(acc.equity if self.risk_config.use_equity else acc.margin_free)

    async def get_free_margin(self) -> float:
        return float((await self._account()).margin_free)

    async def account_currency(self) -> str:
        return str((await self._account()).currency)

    # ---- market data ---------------------------------------------------------------
    @staticmethod
    def _tradable(info: Any) -> bool:
        return int(getattr(info, "trade_mode", MT5.SYMBOL_TRADE_MODE_FULL)) not in (
            MT5.SYMBOL_TRADE_MODE_DISABLED, MT5.SYMBOL_TRADE_MODE_CLOSEONLY,
        )

    async def resolve_symbol(
        self,
        base_asset: str,
        quote_asset: str | None = None,
        market_type: MarketType | None = None,
        raw_symbol: str | None = None,
    ) -> SymbolInfo | None:
        self._require()
        base = base_asset.upper()
        # A currency without a quote is traded against USD (symbol_candidates): the same pair is the symbol_map
        # key that is looked up, and the one the "not found" hint names.
        pair_quote = quote_asset or ("USD" if base in FIAT_CURRENCIES else None)
        mapped = mapped_symbol(self.cfg.symbol_map, base_asset, raw_symbol, pair_quote)
        if mapped:
            info, _ = await self._call("symbol_info", mapped)
            if info is None:
                raise BrokerError(f"symbol_map'teki '{mapped}' sembolü {self.name} brokerında bulunamadı.")
            return await self._symbol_info_model(info, base_asset, quote_asset)

        candidates = symbol_candidates(base_asset, quote_asset, market_type)
        info = await self._find_symbol(candidates, (base,), base)
        # The signal's own name first; then the other names of the same index / energy CFD, in table order.
        aliases = alias_candidates(base_asset, quote_asset, market_type) if info is None else []
        for alias in aliases:
            info = await self._find_symbol([alias], alias_whole(alias), alias)
            if info is not None:
                logger.info("%s: %s is listed as %s on this broker", self.name, base, info.name)
                break
        if info is None:
            logger.info("%s: no MT5 symbol for %s/%s", self.name, base_asset, quote_asset)
            if market_type in (MarketType.FOREX, MarketType.CFD) or base in _METAL_BASES:
                raise BrokerError(self._no_symbol_text(base, pair_quote, candidates, aliases))
            return None
        return await self._symbol_info_model(info, base_asset, quote_asset)

    async def _find_symbol(self, candidates: Sequence[str], whole: Sequence[str], pattern: str) -> Any | None:
        """A tradable symbol named exactly like one of ``candidates``; else the best-ranked variant with a broker
        prefix / suffix among the symbols whose name contains ``pattern`` (see rank_symbol)."""
        for name in candidates:
            info, _ = await self._call("symbol_info", name)
            if info is not None and self._tradable(info):
                return info
        group, _ = await self._call("symbols_get", group=f"*{pattern}*")
        scored = []
        for info in group or ():
            rank = rank_symbol(str(info.name), candidates, whole=whole)
            if rank is not None and self._tradable(info):
                full = int(getattr(info, "trade_mode", MT5.SYMBOL_TRADE_MODE_FULL)) == MT5.SYMBOL_TRADE_MODE_FULL
                scored.append(((rank, not full, not getattr(info, "visible", True)), info))
        if not scored:
            return None
        scored.sort(key=lambda x: x[0])
        return scored[0][1]

    def _no_symbol_text(self, base: str, quote: str | None, candidates: Sequence[str], aliases: Sequence[str]) -> str:
        """Owner-facing error: what was tried and the symbol_map entry that fixes it.

        symbol_map keys are matched against the signal's own spelling, the pair BASE+QUOTE or the base alone (see
        mapped_symbol). A base key would remap every pair of a currency, so the hint names the pair wherever the
        quote is part of the instrument, and the base for a metal / index / commodity."""
        q = (quote or "").upper()
        if base in FIAT_CURRENCIES:
            # A currency pair. Never suggest a key of the base currency alone: it would send EVERY pair of that
            # currency (EURUSD too) to the mapped symbol.
            pair = base + (q or "USD")
            return (
                f"{self.name}: {pair} paritesi MT5'te bulunamadı (denenen: {', '.join(candidates)}). Bu hesapta "
                f"bu parite olmayabilir. Broker pariteye başka bir ad veriyorsa MT5'te Piyasa Gözlemi (Market "
                f"Watch) penceresindeki tam adı bulup config.yaml'da bu brokerın ayarına parite adıyla ekleyin, "
                f"örn. symbol_map: {{{pair}: MT5_SEMBOL_ADI}} (MT5_SEMBOL_ADI yerine oradaki tam sembol adını "
                f"yazın; {base} diye eklemeyin: tüm {base} paritelerini değiştirir)."
            )
        own_quote = not q or q in _USD_LIKE_QUOTES or q == CFD_QUOTES.get(base)
        key = base if own_quote else base + q
        example = "GOLD" if key == "XAU" else "MT5_SEMBOL_ADI"
        also = f" Aynı enstrümanın diğer adları da denendi: {', '.join(aliases)}." if aliases else ""
        return (
            f"{self.name}: '{base}' için MT5'te sembol bulunamadı (denenen: {', '.join(candidates)}).{also} "
            f"Broker bu enstrümana başka bir ad veriyor olabilir (ör. altın için GOLD): MT5'te Piyasa Gözlemi "
            f"(Market Watch) penceresindeki tam adı bulup config.yaml'da bu brokerın ayarına ekleyin, örn. "
            f"symbol_map: {{{key}: {example}}} ({example} yerine oradaki tam sembol adını yazın)."
        )

    async def _symbol_info_model(self, info: Any, base: str, quote: str | None) -> SymbolInfo:
        name = str(info.name)
        if not getattr(info, "visible", True):
            ok, _ = await self._call("symbol_select", name, True)
            if not ok:
                logger.warning("%s: symbol_select(%s) failed", self.name, name)
            info = await self._get("symbol_info", name, what=f"{name} sembol bilgisi")
            # A freshly selected symbol has no quote for a moment (no tick, or an all-zero one): wait for the
            # first tick that carries a price (best effort).
            try:
                await self._tick(name, attempts=5, priced=True)
            except BrokerError as exc:
                logger.warning("%s: no tick for %s right after symbol_select: %s", self.name, name, exc)
        point = _f(info.point)
        tick = _f(getattr(info, "trade_tick_size", 0.0)) or point
        logger.info("%s: %s resolved to %s", self.name, base, name)
        return SymbolInfo(
            broker_symbol=name,
            base_asset=base.upper(),
            quote_asset=str(getattr(info, "currency_profit", "") or quote or "USD").upper(),
            qty_step=_f(info.volume_step),
            min_qty=_f(info.volume_min),
            max_qty=_f(info.volume_max) or None,
            price_tick=tick,
            contract_size=_f(getattr(info, "trade_contract_size", 1.0)) or 1.0,
            extra={
                "digits": int(getattr(info, "digits", 0)),
                "point": point,
                "stops_level": int(getattr(info, "trade_stops_level", 0) or 0) * point,
                "filling_mode": int(getattr(info, "filling_mode", 0) or 0),
                "expiration_mode": int(getattr(info, "expiration_mode", 0) or 0),
                "order_mode": int(getattr(info, "order_mode", 0) or 0),
                "trade_mode": int(getattr(info, "trade_mode", MT5.SYMBOL_TRADE_MODE_FULL)),
                "currency_margin": str(getattr(info, "currency_margin", "") or ""),
            },
        )

    @staticmethod
    def _now_epoch() -> float:
        return time.time()

    def _tick_is_stale(self, tick_time: int) -> bool:
        """True when the tick is clearly old (market closed, e.g. the weekend).

        MT5 gives no trade-server clock and tick.time is in server time (a whole-hour offset from UTC,
        typically UTC+2/+3). So the difference to our UTC clock must be a whole-hour offset +/- 10 minutes
        inside [-4h, +1h]; anything else is an old tick. (A stale tick that looks fresh by chance only costs a
        10022 retry without expiration, which is handled.)"""
        d = self._now_epoch() - tick_time
        if not -4 * 3600 - _STALE_TICK_SEC <= d <= 3600 + _STALE_TICK_SEC:
            return True
        off = (d + 1800) % 3600 - 1800        # distance to the nearest whole hour
        return abs(off) > _STALE_TICK_SEC

    async def _tick(self, broker_symbol: str, attempts: int = 3, *, priced: bool = False) -> Any:
        """Latest tick; a symbol that was just added to Market Watch answers None for a moment, so retry.

        ``priced``: such a symbol can also answer with an all-zero tick for a moment. A tick without a bid or
        an ask then counts as "no tick yet" as well: it is polled again every ``tick_wait_sec`` until about 3 s
        have been waited in total, then the "no price" error is raised (a market that is really closed ends
        there, a few seconds later). A tick with a price returns at once; None answers keep their own, shorter
        budget (``attempts``)."""
        missing = waits = 0
        while True:
            res, err = await self._call("symbol_info_tick", broker_symbol)
            if res is None:
                missing += 1
                if missing >= max(1, attempts):
                    raise BrokerError(f"MT5 {broker_symbol} fiyatı alınamadı: {_err_text(err)}")
            elif priced and (_f(getattr(res, "bid", 0.0)) <= 0 or _f(getattr(res, "ask", 0.0)) <= 0):
                if waits >= _EMPTY_TICK_WAITS:
                    raise BrokerError(f"{broker_symbol} için fiyat yok (piyasa kapalı olabilir).")
            else:
                return res
            waits += 1
            await asyncio.sleep(self.tick_wait_sec)

    async def _quote(self, broker_symbol: str) -> Quote:
        self._require()
        t = await self._tick(broker_symbol, priced=True)     # bid and ask are both > 0, or it raised
        bid, ask = _f(t.bid), _f(t.ask)
        return Quote(bid=bid, ask=ask, last=_f(getattr(t, "last", 0.0)) or (bid + ask) / 2)

    # ---- sizing primitives -------------------------------------------------------------
    async def _loss_per_lot(self, symbol: SymbolInfo, side: Side, entry: float, stop: float) -> float:
        self._require()
        otype = MT5.ORDER_TYPE_BUY if side is Side.LONG else MT5.ORDER_TYPE_SELL
        profit = await self._get(
            "order_calc_profit", otype, symbol.broker_symbol, 1.0, float(entry), float(stop), what="stop zararı hesabı"
        )
        return abs(float(profit))

    async def _margin_for(self, symbol: SymbolInfo, side: Side, lots: float, entry: float) -> float | None:
        otype = MT5.ORDER_TYPE_BUY if side is Side.LONG else MT5.ORDER_TYPE_SELL
        margin, err = await self._call("order_calc_margin", otype, symbol.broker_symbol, float(lots), float(entry))
        if margin is None:
            logger.warning("%s: order_calc_margin failed: %s", self.name, err)
            return None
        return float(margin)

    # ---- order primitives -------------------------------------------------------------
    def _fillings(self, broker_symbol: str, filling_flags: int, pending: bool = False) -> list[int]:
        """Filling modes to try, best first: cached > configured > symbol flags > the rest.

        Pending orders (``pending``) start with RETURN (the mode a resting order normally uses; FOK/IOC on a
        pending order is rejected or meaningless on many servers) unless the owner pinned a mode in the config."""
        order: list[int] = []
        if pending and self.cfg.filling == "auto":
            order.append(MT5.ORDER_FILLING_RETURN)
        cached = self._filling_cache.get(broker_symbol + (":pending" if pending else ""))
        if cached is not None:
            order.append(cached)
        configured = {"fok": MT5.ORDER_FILLING_FOK, "ioc": MT5.ORDER_FILLING_IOC, "return": MT5.ORDER_FILLING_RETURN}
        if self.cfg.filling in configured:
            order.append(configured[self.cfg.filling])
        if filling_flags & MT5.SYMBOL_FILLING_FOK:
            order.append(MT5.ORDER_FILLING_FOK)
        if filling_flags & MT5.SYMBOL_FILLING_IOC:
            order.append(MT5.ORDER_FILLING_IOC)
        order += [MT5.ORDER_FILLING_RETURN, MT5.ORDER_FILLING_FOK, MT5.ORDER_FILLING_IOC]
        return list(dict.fromkeys(order))

    async def _send_filling(self, request: dict[str, Any], broker_symbol: str, flags: int) -> Any:
        res = None
        pending = request.get("action") == MT5.TRADE_ACTION_PENDING
        cache_key = broker_symbol + (":pending" if pending else "")
        for filling in self._fillings(broker_symbol, flags, pending):
            request["type_filling"] = filling
            res = await self._send(request)
            if res.retcode != MT5.TRADE_RETCODE_INVALID_FILL:
                if res.retcode in _DEAL_OK or res.retcode == MT5.TRADE_RETCODE_PLACED:
                    self._filling_cache[cache_key] = filling
                return res
            logger.info("%s: filling %s rejected for %s, trying next", self.name, filling, broker_symbol)
        return res

    async def _retry_transient(self, res: Any, request: dict[str, Any], broker_symbol: str) -> bool:
        """After a transient refusal (no connection / timeout / too many requests): pause, then True when the same
        request may be sent ONCE more. The caller must not ask again for the retried request.

        10012 ("request cancelled by timeout") is the one refusal after which the server may have executed the
        request anyway: a close is not repeated (a second partial close could close another slice), an entry
        only when no order or position carries its comment; otherwise it is handled like a lost reply."""
        code = int(res.retcode)
        if code not in _TRANSIENT_CODES:
            return False
        action = request.get("action")
        timeout = code == MT5.TRADE_RETCODE_TIMEOUT
        if timeout and action == MT5.TRADE_ACTION_DEAL and "position" in request:
            return False
        logger.warning("%s: %s; retrying once in %.1f s", self.name,
                       retcode_text(code, getattr(res, "comment", "")), self.transient_retry_sec)
        await asyncio.sleep(self.transient_retry_sec)
        if timeout and action in (MT5.TRADE_ACTION_DEAL, MT5.TRADE_ACTION_PENDING):
            tag = str(request.get("comment") or "")
            try:
                positions, orders = await self._snapshot(broker_symbol)
                exists = not tag or any(x["tag"] == tag for x in (*positions.values(), *orders.values()))
            except BrokerError as exc:
                logger.warning("%s: cannot check for order %s after a timeout answer: %s", self.name, tag, exc)
                exists = True       # unknown: never send it twice
            if exists:
                raise OrderReplyLostError(
                    self._lost_reply_text(retcode_text(code, getattr(res, "comment", "")), request))
        return True

    async def _send_market(self, request: dict[str, Any], broker_symbol: str, flags: int, buy: bool) -> Any:
        """DEAL at a fresh price. A requote is retried (3 attempts in all, a short pause in between); a transient
        refusal gets ONE more attempt after a longer pause. A lost reply raises and is never sent again."""
        requotes, retried = 0, False
        while True:
            # Never build a request from a tick without a price (a symbol that is not quoting yet, a closed
            # market): wait a moment for one, else "no price" is raised and nothing is sent.
            t = await self._tick(broker_symbol, priced=True)
            request["price"] = _f(t.ask if buy else t.bid)
            res = await self._send_filling(request, broker_symbol, flags)
            if res.retcode in _REQUOTE_CODES and requotes < 2:
                requotes += 1
                await asyncio.sleep(self.requote_delay_sec)
                continue
            if not retried and await self._retry_transient(res, request, broker_symbol):
                retried = True
                continue
            return res

    async def _symbol_flags(self, broker_symbol: str) -> Any:
        return await self._get("symbol_info", broker_symbol, what=f"{broker_symbol} sembol bilgisi")

    async def _open_market_leg(
        self, symbol: SymbolInfo, side: Side, volume: float, sl: float, tp: float | None, tag: str
    ) -> dict[str, Any]:
        self._require()
        name = symbol.broker_symbol
        info = await self._symbol_flags(name)
        flags = int(getattr(info, "filling_mode", 0) or 0)
        buy = side is Side.LONG
        request: dict[str, Any] = {
            "action": MT5.TRADE_ACTION_DEAL,
            "symbol": name,
            "volume": float(volume),
            "type": MT5.ORDER_TYPE_BUY if buy else MT5.ORDER_TYPE_SELL,
            "sl": float(sl),
            "tp": float(tp or 0.0),
            "deviation": int(self.cfg.deviation_points),
            "magic": int(self.cfg.magic),
            "comment": tag,
            "type_time": MT5.ORDER_TIME_GTC,
        }
        res = await self._send_market(request, name, flags, buy)
        naked = False
        if res.retcode == MT5.TRADE_RETCODE_INVALID_STOPS:
            # Some execution modes refuse stops on the entry: open without, attach right after (rule 1).
            logger.warning("%s: stops rejected on entry, opening without and attaching", self.name)
            request.update(sl=0.0, tp=0.0)
            res = await self._send_market(request, name, flags, buy)
            naked = True
        order = int(getattr(res, "order", 0) or 0)
        if res.retcode == MT5.TRADE_RETCODE_PLACED:
            return await self._adopt_placed_deal(name, order, tag, float(volume), sl, tp, naked)
        if res.retcode not in _DEAL_OK:
            raise BrokerError(f"{name} emri reddedildi: {retcode_text(res.retcode, getattr(res, 'comment', ''))}")
        filled = _f(getattr(res, "volume", 0.0)) or float(volume)
        entry = _f(getattr(res, "price", 0.0)) or request["price"]
        leg = new_leg(ticket=order, state="open", volume=float(volume), filled=filled, entry=entry,
                      sl=None if naked else float(sl), tp=tp, tag=tag)
        # The order IS filled (10009 / 10010). A problem while looking its position up must not turn that into
        # "could not open": the position would stay on the account without anybody managing it.
        if order > 0:
            try:
                leg["ticket"] = await self._position_ticket(order)
            except BrokerError as exc:
                self._track_by_order(leg, order, f"emir gerçekleşti ama pozisyonu sorgulanamadı ({exc})")
        else:   # no order ticket in the result: find our position by its tag
            p, problem = await self._await_new_position(name, 0, tag)
            if p is None:
                why = f" ({problem})" if problem else ""
                if naked:
                    raise BrokerError(
                        f"KRİTİK: {name} emri STOPSUZ gerçekleşti ama pozisyon numarası alınamadı{why}. MT5'te "
                        f"'{tag}' etiketli pozisyonu hemen bulup stop koyun ya da kapatın!"
                    )
                raise BrokerError(
                    f"{name} emri gerçekleşti ama pozisyon numarası alınamadı{why}; MT5'te '{tag}' etiketli "
                    f"pozisyonu kontrol edin."
                )
            leg["ticket"] = p["ticket"]
        return leg

    @staticmethod
    def _track_by_order(leg: dict[str, Any], order: int, what: str) -> None:
        """A filled leg whose position could not be looked up: keep it, tracked by the order ticket (a new
        position carries the ticket of the order that opened it), until a refresh sees it on the venue."""
        leg["ticket"] = order
        leg["unconfirmed"] = True
        leg["note"] = f"{what}; pozisyon emir numarasıyla (#{order}) takip ediliyor, sonraki kontrolde doğrulanacak"

    async def _await_new_position(self, name: str, order: int, tag: str) -> tuple[dict[str, Any] | None, str | None]:
        """Look for the position a market order opened, for a few seconds: by the order ticket (a new position
        carries it), else by the order comment. Returns (position, None), or (None, the last look-up error or
        None when the look-ups worked and found nothing)."""
        problem: str | None = None
        for attempt in range(_NEW_POSITION_POLLS):
            if attempt:
                await asyncio.sleep(self.tick_wait_sec)
            try:
                positions, _ = await self._snapshot(name)
            except BrokerError as exc:
                problem = str(exc)
                continue
            problem = None
            p = positions.get(str(order)) if order > 0 else None
            p = p or next((x for x in positions.values() if tag and x["tag"] == tag), None)
            if p is not None:
                return p, None
        return None, problem

    async def _adopt_placed_deal(
        self, name: str, order: int, tag: str, volume: float, sl: float, tp: float | None, naked: bool
    ) -> dict[str, Any]:
        """Retcode 10008 on a market order: accepted, executed a moment later. Adopt the position once it shows
        up (by the order ticket or the comment); a failure is reported only if nothing appears."""
        p, problem = await self._await_new_position(name, order, tag)
        if p is not None:
            # Size, price and stop as the venue shows them (no stop there -> the caller attaches it).
            return new_leg(ticket=p["ticket"], state="open", volume=volume, filled=p["volume"] or volume,
                           entry=p.get("entry"), sl=p.get("sl"), tp=tp, tag=tag)
        hist: dict[str, Any] | None = None
        if order > 0:
            try:
                hist = await self._history_order(order)
            except BrokerError as exc:
                problem = problem or str(exc)
        if hist and hist["state"] == "cancelled":
            reason = {"rejected": "reddedildi", "expired": "süresi doldu"}.get(str(hist.get("reason")), "iptal edildi")
            raise BrokerError(f"{name} emri sunucuda kabul edildi ama gerçekleşmedi ({reason}).")
        leg = new_leg(ticket=order, state="open", volume=volume, filled=volume,
                      sl=None if naked else float(sl), tp=tp, tag=tag)
        if hist and hist["state"] == "filled":
            # Executed according to the order history although no position is listed (any more).
            self._track_by_order(leg, int(hist.get("position_id") or order), "emir gerçekleşti ama pozisyonu görünmüyor")
            return leg
        if problem is not None and order > 0:
            # Accepted, and the look-ups themselves failed: nothing says it was not executed.
            self._track_by_order(leg, order, f"emir sunucuda kabul edildi ama pozisyonu sorgulanamadı ({problem})")
            return leg
        waited = (_NEW_POSITION_POLLS - 1) * self.tick_wait_sec
        raise BrokerError(
            f"{name} emri sunucuda kabul edildi (kod 10008) ama {waited:.0f} sn içinde pozisyon oluşmadı. Emir "
            f"sonradan gerçekleşebilir: MT5'te '{tag}' etiketli emir/pozisyon var mı kontrol edin."
        )

    async def _adopt_lost_leg(
        self, symbol: str, tag: str, volume: float, sl: float, tp: float | None
    ) -> dict[str, Any] | None:
        # An order_send that ran out of time may still be inside the terminal. The order can only be looked up
        # once that call has returned (until then every call fails at once): wait for it, bounded.
        if not await self._wait_terminal(LOST_REPLY_WAIT_SEC):
            logger.error("%s: terminal still not answering %.0f s after order %s timed out; cannot look it up",
                         self.name, LOST_REPLY_WAIT_SEC, tag)
            return None
        return await super()._adopt_lost_leg(symbol, tag, volume, sl, tp)

    async def _position_ticket(self, order_ticket: int) -> int:
        """A new position's ticket normally equals its opening order's ticket; confirm via the deal."""
        ps = await self._get_list("positions_get", ticket=order_ticket, what="pozisyon")
        if ps:
            return int(ps[0].ticket)
        deals = await self._get_list("history_deals_get", ticket=order_ticket, what="işlem geçmişi")
        for d in deals:
            if getattr(d, "position_id", 0):
                return int(d.position_id)
        return order_ticket

    async def _open_pending_leg(
        self, symbol: SymbolInfo, side: Side, volume: float, price: float, sl: float,
        tp: float | None, tag: str, quote: Quote,
    ) -> dict[str, Any]:
        self._require()
        name = symbol.broker_symbol
        info = await self._symbol_flags(name)
        if side is Side.LONG:
            limit = price < quote.ask
            otype = MT5.ORDER_TYPE_BUY_LIMIT if limit else MT5.ORDER_TYPE_BUY_STOP
        else:
            limit = price > quote.bid
            otype = MT5.ORDER_TYPE_SELL_LIMIT if limit else MT5.ORDER_TYPE_SELL_STOP
        order_mode = int(getattr(info, "order_mode", 0) or 0)
        need = MT5.SYMBOL_ORDER_LIMIT if limit else MT5.SYMBOL_ORDER_STOP
        if order_mode and not order_mode & need:
            raise BrokerError(f"{name} için {'limit' if limit else 'stop'} emir türüne broker izin vermiyor.")
        request: dict[str, Any] = {
            "action": MT5.TRADE_ACTION_PENDING,
            "symbol": name,
            "volume": float(volume),
            "type": otype,
            "price": float(price),
            "sl": float(sl),
            "tp": float(tp or 0.0),
            "deviation": int(self.cfg.deviation_points),
            "magic": int(self.cfg.magic),
            "comment": tag,
            "type_time": MT5.ORDER_TIME_GTC,
        }
        minutes = int(self.risk_config.pending_entry_expiry_min or 0)
        exp_flags = int(getattr(info, "expiration_mode", 0) or 0)
        if minutes > 0 and exp_flags & MT5.SYMBOL_EXPIRATION_SPECIFIED:
            # Never from a tick without a price: its time is not a server time (raises "no price", nothing sent).
            tick = await self._tick(name, priced=True)
            server_now = int(getattr(tick, "time", 0) or 0)   # expiration is in trade-server time, like tick.time
            # Weekend / closed market: tick.time is the last (Friday) tick, so "now + N min" would already be
            # in the past. Stale tick -> GTC; the engine cancels stale pending entries itself.
            if server_now > 0 and not self._tick_is_stale(server_now):
                request.update(type_time=MT5.ORDER_TIME_SPECIFIED, expiration=server_now + minutes * 60)
        flags = int(getattr(info, "filling_mode", 0) or 0)
        res = await self._send_filling(request, name, flags)
        if await self._retry_transient(res, request, name):
            res = await self._send_filling(request, name, flags)
        if res.retcode == MT5.TRADE_RETCODE_INVALID_EXPIRATION and request["type_time"] == MT5.ORDER_TIME_SPECIFIED:
            request["type_time"] = MT5.ORDER_TIME_GTC
            request.pop("expiration", None)
            res = await self._send_filling(request, name, flags)
        if res.retcode not in (MT5.TRADE_RETCODE_PLACED, MT5.TRADE_RETCODE_DONE):
            raise BrokerError(f"{name} bekleyen emri reddedildi: {retcode_text(res.retcode, getattr(res, 'comment', ''))}")
        if int(getattr(res, "order", 0) or 0) <= 0:
            raise BrokerError(
                f"{name} bekleyen emri kabul edildi görünüyor ama emir numarası alınamadı; MT5'te "
                f"'{tag}' etiketli emri kontrol edin."
            )
        leg = new_leg(ticket=int(res.order), order_ticket=int(res.order), state="pending", volume=float(volume),
                      entry=float(price), sl=float(sl), tp=tp, tag=tag)
        leg["order_type"] = otype
        return leg

    async def _modify_leg(self, symbol: str, side: Side, leg: dict[str, Any], sl: float, tp: float | None) -> None:
        self._require()
        if leg["state"] == "open":
            request: dict[str, Any] = {
                "action": MT5.TRADE_ACTION_SLTP, "symbol": symbol, "position": int(leg["ticket"]),
                "sl": float(sl), "tp": float(tp or 0.0), "magic": int(self.cfg.magic),
            }
        elif leg["state"] == "pending":
            orders = await self._get_list("orders_get", ticket=int(leg["order_ticket"]), what="bekleyen emir")
            if not orders:
                raise BrokerError(f"Bekleyen emir #{leg['order_ticket']} bulunamadı (dolmuş ya da iptal olmuş olabilir).")
            o = orders[0]
            request = {
                "action": MT5.TRADE_ACTION_MODIFY, "symbol": symbol, "order": int(leg["order_ticket"]),
                "price": _f(o.price_open), "sl": float(sl), "tp": float(tp or 0.0),
                "type_time": int(getattr(o, "type_time", MT5.ORDER_TIME_GTC)),
                "expiration": int(getattr(o, "time_expiration", 0) or 0),
            }
        else:
            raise BrokerError(f"#{leg['ticket']} artık açık değil.")
        res = await self._send(request)
        if await self._retry_transient(res, request, symbol):
            res = await self._send(request)
        if res.retcode not in _MODIFY_OK:
            raise BrokerError(f"SL/TP değiştirilemedi: {retcode_text(res.retcode, getattr(res, 'comment', ''))}")

    async def _close_leg(self, symbol: str, side: Side, leg: dict[str, Any], volume: float | None) -> float:
        self._require()
        if leg["state"] != "open":
            return 0.0
        ov = float(leg["open_volume"] or leg["filled"])
        vol = ov if volume is None else min(float(volume), ov)
        info = await self._symbol_flags(symbol)
        closing_buy = side is Side.SHORT   # close a long by selling, a short by buying
        request: dict[str, Any] = {
            "action": MT5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "position": int(leg["ticket"]),
            "volume": float(vol),
            "type": MT5.ORDER_TYPE_BUY if closing_buy else MT5.ORDER_TYPE_SELL,
            "deviation": int(self.cfg.deviation_points),
            "magic": int(self.cfg.magic),
            "comment": str(leg.get("tag") or "")[: self.tag_limit],
            "type_time": MT5.ORDER_TIME_GTC,
        }
        res = await self._send_market(request, symbol, int(getattr(info, "filling_mode", 0) or 0), closing_buy)
        if res.retcode == MT5.TRADE_RETCODE_PLACED:
            # Accepted, executed a moment later (like an entry answered with 10008): wait for the venue to show it.
            closed = await self._await_closed(symbol, int(leg["ticket"]), ov, vol)
        elif res.retcode == MT5.TRADE_RETCODE_TIMEOUT:
            # "Request cancelled by timeout": the server may have executed it anyway (so _retry_transient never
            # repeats a close for it). Its outcome is unknown: never "could not be closed".
            raise OrderReplyLostError(
                self._lost_reply_text(retcode_text(res.retcode, getattr(res, "comment", "")), request))
        elif res.retcode not in _DEAL_OK:
            raise BrokerError(f"#{leg['ticket']} kapatılamadı: {retcode_text(res.retcode, getattr(res, 'comment', ''))}")
        else:
            closed = _f(getattr(res, "volume", 0.0)) or vol
        step = float(info.volume_step or 0.0)
        leg["open_volume"] = _snap(ov - closed, step)
        self._mark_closed_lots(leg, closed)
        if leg["open_volume"] <= _EPS:
            leg["state"] = "closed"
            leg["reason"] = "manual"
        return closed

    async def _await_closed(self, symbol: str, ticket: int, open_volume: float, volume: float) -> float:
        """A closing deal answered with 10008: the lots the venue shows as closed once the position has shrunk by
        ``volume`` (or is gone), looked for during a few seconds. Raises when it still looks unchanged."""
        problem: str | None = None
        for attempt in range(_NEW_POSITION_POLLS):
            if attempt:
                await asyncio.sleep(self.tick_wait_sec)
            try:
                positions, _ = await self._snapshot(symbol)
            except BrokerError as exc:
                problem = str(exc)
                continue
            problem = None
            p = positions.get(str(ticket))
            if p is None:
                return open_volume
            if p["volume"] <= open_volume - volume + _EPS:
                return open_volume - p["volume"]
        why = f"pozisyon sorgulanamadı ({problem})" if problem else "pozisyon hâlâ açık görünüyor"
        raise BrokerError(
            f"#{ticket} için kapatma emri sunucuda kabul edildi (kod 10008) ama {why}. Emir sonradan "
            f"gerçekleşebilir; MT5'te kontrol edin."
        )

    async def _cancel_leg(self, symbol: str, leg: dict[str, Any]) -> None:
        self._require()
        request = {"action": MT5.TRADE_ACTION_REMOVE, "order": int(leg["order_ticket"])}
        res = await self._send(request)
        if await self._retry_transient(res, request, symbol):
            res = await self._send(request)
        if res.retcode not in (MT5.TRADE_RETCODE_DONE, MT5.TRADE_RETCODE_PLACED):
            raise BrokerError(f"Emir #{leg['order_ticket']} iptal edilemedi: {retcode_text(res.retcode, getattr(res, 'comment', ''))}")

    # ---- state primitives ---------------------------------------------------------------
    def _position_dict(self, p: Any) -> dict[str, Any]:
        magic = int(getattr(p, "magic", 0) or 0)
        return {
            "ticket": int(p.ticket), "volume": _f(p.volume), "entry": _f(p.price_open) or None,
            "sl": _f(p.sl) or None, "tp": _f(p.tp) or None,
            # Only our own orders count for tag matching / idempotency.
            "tag": str(getattr(p, "comment", "") or "") if magic == int(self.cfg.magic) else "",
            "magic": magic, "realized": None,
        }

    async def _position_now(self, symbol: str, ticket: Any) -> dict[str, Any] | None:
        self._require()
        ps = await self._get_list("positions_get", ticket=int(ticket), what="pozisyon")
        return next((self._position_dict(p) for p in ps if int(p.ticket) == int(ticket)), None)

    async def _snapshot(self, symbol: str) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        self._require()
        ps = await self._get_list("positions_get", symbol=symbol, what="pozisyonlar")
        os_ = await self._get_list("orders_get", symbol=symbol, what="bekleyen emirler")
        positions = {str(p.ticket): self._position_dict(p) for p in ps}
        orders = {
            str(o.ticket): {
                "ticket": int(o.ticket), "volume": _f(getattr(o, "volume_current", 0.0) or getattr(o, "volume_initial", 0.0)),
                "price": _f(o.price_open) or None, "sl": _f(o.sl) or None, "tp": _f(o.tp) or None,
                "tag": str(getattr(o, "comment", "") or ""), "magic": int(getattr(o, "magic", 0) or 0),
            }
            for o in os_
            if int(getattr(o, "type", 0)) >= MT5.ORDER_TYPE_BUY_LIMIT
        }
        # Only our own orders count for tag matching / idempotency.
        for v in orders.values():
            if v["magic"] != int(self.cfg.magic):
                v["tag"] = ""
        return positions, orders

    async def _history_order(self, order_ticket: Any) -> dict[str, Any] | None:
        hist = await self._get_list("history_orders_get", ticket=int(order_ticket), what="emir geçmişi")
        if not hist:
            return None
        h = hist[0]
        state = int(getattr(h, "state", -1))
        if state in (MT5.ORDER_STATE_FILLED, MT5.ORDER_STATE_PARTIAL):
            kind = "filled"
        elif state in (MT5.ORDER_STATE_CANCELED, MT5.ORDER_STATE_EXPIRED, MT5.ORDER_STATE_REJECTED):
            kind = "cancelled"
        else:
            kind = "other"
        reason = {MT5.ORDER_STATE_EXPIRED: "expired", MT5.ORDER_STATE_REJECTED: "rejected"}.get(state, "cancelled")
        return {"state": kind, "position_id": int(getattr(h, "position_id", 0) or 0) or None, "reason": reason}

    async def _deal_summary(self, position_id: Any) -> dict[str, Any] | None:
        deals = await self._get_list("history_deals_get", position=int(position_id), what="işlem geçmişi")
        if not deals:
            return None
        trades = [d for d in deals if int(d.type) in (MT5.DEAL_TYPE_BUY, MT5.DEAL_TYPE_SELL)]
        realized = sum(_f(d.profit) + _f(d.commission) + _f(d.swap) + _f(getattr(d, "fee", 0.0)) for d in deals)
        ins = [d for d in trades if int(d.entry) == MT5.DEAL_ENTRY_IN]
        outs = [d for d in trades if int(d.entry) in (MT5.DEAL_ENTRY_OUT, MT5.DEAL_ENTRY_OUT_BY, MT5.DEAL_ENTRY_INOUT)]
        in_vol = sum(_f(d.volume) for d in ins)
        reason = None
        if outs:
            last = max(outs, key=lambda d: (int(getattr(d, "time_msc", 0) or 0), int(getattr(d, "ticket", 0) or 0)))
            reason = {MT5.DEAL_REASON_SL: "sl", MT5.DEAL_REASON_TP: "tp", MT5.DEAL_REASON_SO: "stop_out"}.get(
                int(getattr(last, "reason", -1)), "manual"
            )
        return {
            "realized": realized,
            "in_volume": in_vol,
            "out_volume": sum(_f(d.volume) for d in outs),
            "entry": (sum(_f(d.price) * _f(d.volume) for d in ins) / in_vol) if in_vol else None,
            "reason": reason,
        }
