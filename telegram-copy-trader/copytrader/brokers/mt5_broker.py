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
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
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
from ..risk.sizing import SizingError, compute_lot_size, floor_to_step, round_to_tick
from .base import Broker, BrokerError

logger = logging.getLogger(__name__)

T = TypeVar("T")

_EPS = 1e-9
LIVE_STATES = ("pending", "open")
TERMINAL_STATES = ("closed", "cancelled", "failed")
_MAX_PNL_TRIES = 5          # re-query history this many syncs for a closed leg with unknown PnL
_MAX_MISSES = 3             # an open leg missing from the venue AND its history this many syncs -> closed


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
    TRADE_RETCODE_INVALID_STOPS = 10016
    TRADE_RETCODE_PRICE_CHANGED = 10020
    TRADE_RETCODE_PRICE_OFF = 10021
    TRADE_RETCODE_INVALID_EXPIRATION = 10022
    TRADE_RETCODE_NO_CHANGES = 10025
    TRADE_RETCODE_INVALID_FILL = 10030
    # last_error() codes
    RES_S_OK = 1
    RES_E_NOT_FOUND = -4


_IPC_ERRORS = range(-10005, -9999)          # -10005..-10000: terminal IPC lost
_REQUOTE_CODES = (MT5.TRADE_RETCODE_REQUOTE, MT5.TRADE_RETCODE_PRICE_CHANGED, MT5.TRADE_RETCODE_PRICE_OFF)
_DEAL_OK = (MT5.TRADE_RETCODE_DONE, MT5.TRADE_RETCODE_DONE_PARTIAL)
_MODIFY_OK = (MT5.TRADE_RETCODE_DONE, MT5.TRADE_RETCODE_PLACED, MT5.TRADE_RETCODE_NO_CHANGES)

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
    10027: "terminalde 'Algo Trading' kapalı",
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


def symbol_candidates(base: str, quote: str | None) -> list[str]:
    out: list[str] = []
    for q in (quote, "USD", "USDT"):
        if q:
            name = base.upper() + q.upper()
            if name not in out:
                out.append(name)
    return out


def rank_symbol(name: str, candidates: Sequence[str]) -> tuple[int, int, int] | None:
    """Lower is better; None = not a match. Prefers no / non-letter suffix ("ETHUSD.m" over "ETHUSDm")."""
    core = name.upper().lstrip("#._-!")
    best: tuple[int, int, int] | None = None
    for i, cand in enumerate(candidates):
        if core.startswith(cand):
            suffix = core[len(cand):]
            r = (int(bool(suffix) and suffix[0].isalpha()), i, len(suffix))
            best = r if best is None or r < best else best
    return best


def mapped_symbol(symbol_map: dict[str, str], base: str, raw_symbol: str | None) -> str | None:
    lower = {k.lower(): v for k, v in symbol_map.items()}
    for key in (raw_symbol, base):
        if key and key.lower() in lower:
            return lower[key.lower()]
    return None


def tag_matches(value: str | None, tag: str) -> bool:
    return bool(value) and (value == tag or value.startswith(tag + "-"))  # type: ignore[union-attr]


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
        if pending:
            if req.entry_price is None:
                raise BrokerError("Limit emir için giriş fiyatı yok.")
            ref = round_to_tick(req.entry_price, tick)
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
            },
        )

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
            except BrokerError as exc:
                errors.append(f"{label} açılamadı: {exc}")
                break
            leg["tp_indexes"] = pl.tp_indexes
            legs.append(leg)
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

        self._aggregate(pos)
        pos.error = "; ".join(errors) or None
        logger.info(
            "%s: opened %s %s %s lots in %d leg(s) status=%s%s",
            self.name, side.value, sym.broker_symbol, _num(total), len(legs), pos.status.value,
            f" error={pos.error}" if pos.error else "",
        )
        return pos

    async def _protect_or_close(
        self, symbol: str, side: Side, leg: dict[str, Any], sl: float, tp: float | None
    ) -> str | None:
        """Attach the stop to a filled leg (one retry); if impossible close the leg (rule 1)."""
        last = ""
        for attempt in range(2):
            try:
                await self._modify_leg(symbol, side, leg, sl, tp)
                leg["sl"] = sl
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
                await self._refresh_pending(leg, positions, orders)
            elif leg["state"] == "open":
                await self._refresh_open(leg, positions)
            else:
                leg["pnl_tries"] = leg.get("pnl_tries", 0) + 1
                s = await self._deal_summary(leg["ticket"])
                if s and s.get("out_volume", 0) > _EPS:
                    leg["realized"] = s.get("realized")
                    if leg["state"] == "closed" and leg.get("reason") in (None, "manual", "unknown"):
                        leg["reason"] = s.get("reason") or leg.get("reason")

    async def _refresh_pending(
        self, leg: dict[str, Any], positions: dict[str, dict[str, Any]], orders: dict[str, dict[str, Any]]
    ) -> None:
        oid = leg["order_ticket"]
        o = orders.get(str(oid))
        if o is not None:
            leg["sl"] = o.get("sl") or None
            leg["venue_tp"] = o.get("tp") or None
            leg["entry"] = o.get("price") or leg.get("entry")
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
            s = await self._deal_summary(pid)
            if s:
                self._close_from_summary(leg, s)
            else:
                leg.update(state="closed", open_volume=0.0, reason="unknown")
        elif hist["state"] == "cancelled":
            leg["state"] = "cancelled"
            leg["reason"] = hist.get("reason") or "cancelled"

    async def _refresh_open(self, leg: dict[str, Any], positions: dict[str, dict[str, Any]]) -> None:
        p = positions.get(str(leg["ticket"]))
        if p is not None:
            prev = float(leg.get("open_volume") or 0.0)
            self._apply_position(leg, p)
            leg["misses"] = 0
            if leg["open_volume"] < prev - _EPS or (leg["open_volume"] < leg["filled"] - _EPS and leg["realized"] is None):
                if p.get("realized") is not None:
                    leg["realized"] = p["realized"]
                else:
                    s = await self._deal_summary(leg["ticket"])
                    if s:
                        leg["realized"] = s.get("realized")
            return
        s = await self._deal_summary(leg["ticket"])
        if s and s.get("out_volume", 0) > _EPS:
            self._close_from_summary(leg, s)
            return
        # Missing from positions and history: do not conclude on one glitch.
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

        errors, changed = [], 0
        for leg in live:
            try:
                await self._modify_leg(p.broker_symbol, p.side, leg, sl, leg.get("tp"))
                leg["sl"] = sl
                changed += 1
            except BrokerError as exc:
                errors.append(f"#{leg['ticket']}: {exc}")
        if not changed:
            raise BrokerError("Stop güncellenemedi: " + "; ".join(errors))
        self._aggregate(p)
        if not errors:
            p.stop_loss = sl
        p.error = ("Stop bazı bacaklarda güncellenemedi: " + "; ".join(errors)) if errors else None
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
            targets = [(live[0], tps[-1].price)]
            s = sum(max(0.0, t.close_fraction) for t in tps)
            p.data["emulated_tps"] = [
                {"index": i, "price": t.price, "fraction": (max(0.0, t.close_fraction) / s) if s > 0 else 1 / len(tps),
                 "done": False}
                for i, t in enumerate(tps[:-1])
            ]
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

        errors, changed = [], 0
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
                errors.append(f"#{leg['ticket']}: {exc}")
        if not changed:
            raise BrokerError("TP güncellenemedi: " + "; ".join(errors))
        p.take_profits = tps
        self._aggregate(p)
        msgs = ([warning] if warning else []) + (["TP bazı bacaklarda güncellenemedi: " + "; ".join(errors)] if errors else [])
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
        errors: list[str] = []
        closed_any = False

        if fraction >= 1 - 1e-9:
            for leg in open_legs:
                try:
                    await self._close_leg(p.broker_symbol, p.side, leg, None)
                    closed_any = True
                except BrokerError as exc:
                    errors.append(f"#{leg['ticket']}: {exc}")
            for leg in legs:
                if leg["state"] == "pending":
                    closed_any |= await self._cancel_or_close(p, leg, errors)
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
                    closed_any = True
                except BrokerError as exc:
                    errors.append(f"#{leg['ticket']}: {exc}")

        if not closed_any and errors:
            raise BrokerError("Pozisyon kapatılamadı: " + "; ".join(errors))
        try:
            await self._refresh_legs(p, legs)   # pick up realized PnL
        except BrokerError as exc:
            logger.warning("%s: refresh after close failed: %s", self.name, exc)
        self._aggregate(p)
        p.error = ("Bazı bacaklar kapatılamadı: " + "; ".join(errors)) if errors else None
        return p

    async def _cancel_or_close(self, p: BrokerPosition, leg: dict[str, Any], errors: list[str]) -> bool:
        try:
            await self._cancel_leg(p.broker_symbol, leg)
            leg["state"] = "cancelled"
            leg["reason"] = "cancelled"
            return True
        except BrokerError as exc:
            await self._refresh_legs(p, [leg])   # maybe it just filled
            if leg["state"] == "open":
                try:
                    await self._close_leg(p.broker_symbol, p.side, leg, None)
                    return True
                except BrokerError as exc2:
                    errors.append(f"#{leg['ticket']}: {exc2}")
                    return False
            if leg["state"] in TERMINAL_STATES:
                return True
            errors.append(f"#{leg['order_ticket']}: {exc}")
            return False

    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        p, legs = self._copy(pos)
        await self._refresh_legs(p, legs)
        if any(leg["state"] == "open" for leg in legs):
            return await self.close_position(p, 1.0)
        errors: list[str] = []
        changed = False
        for leg in legs:
            if leg["state"] == "pending":
                changed |= await self._cancel_or_close(p, leg, errors)
        if not changed and errors:
            raise BrokerError("Bekleyen emir iptal edilemedi: " + "; ".join(errors))
        try:
            await self._refresh_legs(p, legs)
        except BrokerError as exc:
            logger.warning("%s: refresh after cancel failed: %s", self.name, exc)
        self._aggregate(p)
        p.error = ("Bazı emirler iptal edilemedi: " + "; ".join(errors)) if errors else None
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
        # Rule 1: a filled leg must always carry a stop.
        for leg in legs:
            if leg["state"] == "open" and leg.get("sl") is None and p.stop_loss:
                err = await self._protect_or_close(p.broker_symbol, p.side, leg, float(p.stop_loss), leg.get("tp"))
                if err:
                    errors.append(err)
        if p.data.get("emulated_tps"):
            await self._run_emulated_tps(p, legs, errors)
        self._aggregate(p)
        if errors:
            p.error = "; ".join(errors)
        return p

    async def _run_emulated_tps(self, p: BrokerPosition, legs: list[dict[str, Any]], errors: list[str]) -> None:
        """Netting accounts: partial-close at intermediate TPs the venue cannot hold."""
        todo = [e for e in p.data.get("emulated_tps", []) if not e.get("done")]
        leg = next((x for x in legs if x["state"] == "open"), None)
        if not todo or leg is None:
            return
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
                errors.append(f"TP{e['index'] + 1} kısmi kapatma başarısız: {exc}")
        await self._refresh_legs(p, legs)


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
    """

    kind = "mt5"

    def __init__(self, config: Mt5BrokerConfig, *, mt5_module: Any | None = None) -> None:
        super().__init__(config)
        self.cfg = config
        self._mt5: Any | None = mt5_module
        self._executor: ThreadPoolExecutor | None = None
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

    async def _run(self, fn: Callable[[Any], T]) -> T:
        if self._mt5 is None or self._executor is None:
            raise BrokerError(f"{self.name}: MT5 bağlantısı yok.")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, fn, self._mt5)

    async def _call(self, name: str, *args: Any, **kwargs: Any) -> tuple[Any, tuple[int, str] | None]:
        def job(mt5: Any) -> tuple[Any, tuple[int, str] | None]:
            func = getattr(mt5, name)
            res = func(*args, **kwargs)
            if res is not None:
                return res, None
            err = _last_error(mt5)
            if err[0] in _IPC_ERRORS and self._initialize(mt5):   # terminal link dropped: re-login once
                logger.warning("%s: MT5 IPC error %s, re-initialized", self.name, err)
                res = func(*args, **kwargs)
                if res is not None:
                    return res, None
                err = _last_error(mt5)
            return None, err
        return await self._run(job)

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

    async def _send(self, request: dict[str, Any]) -> Any:
        res, err = await self._call("order_send", dict(request))
        if res is None:
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
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"mt5-{self.name}")
        try:
            ok, err = await self._run(lambda m: (self._initialize(m), _last_error(m)))
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
                if ti is not None and not getattr(ti, "trade_allowed", True):
                    raise BrokerError("MT5 terminalinde 'Algo Trading' düğmesi kapalı; açıp programı yeniden başlatın.")
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
        mapped = mapped_symbol(self.cfg.symbol_map, base_asset, raw_symbol)
        if mapped:
            info, _ = await self._call("symbol_info", mapped)
            if info is None:
                raise BrokerError(f"symbol_map'teki '{mapped}' sembolü {self.name} brokerında bulunamadı.")
            return await self._symbol_info_model(info, base_asset, quote_asset)

        candidates = symbol_candidates(base_asset, quote_asset)
        for name in candidates:
            info, _ = await self._call("symbol_info", name)
            if info is not None and self._tradable(info):
                return await self._symbol_info_model(info, base_asset, quote_asset)

        group, _ = await self._call("symbols_get", group=f"*{base_asset.upper()}*")
        scored = []
        for info in group or ():
            rank = rank_symbol(str(info.name), candidates)
            if rank is not None and self._tradable(info):
                full = int(getattr(info, "trade_mode", MT5.SYMBOL_TRADE_MODE_FULL)) == MT5.SYMBOL_TRADE_MODE_FULL
                scored.append(((rank, not full, not getattr(info, "visible", True)), info))
        if not scored:
            logger.info("%s: no MT5 symbol for %s/%s", self.name, base_asset, quote_asset)
            return None
        scored.sort(key=lambda x: x[0])
        return await self._symbol_info_model(scored[0][1], base_asset, quote_asset)

    async def _symbol_info_model(self, info: Any, base: str, quote: str | None) -> SymbolInfo:
        name = str(info.name)
        if not getattr(info, "visible", True):
            ok, _ = await self._call("symbol_select", name, True)
            if not ok:
                logger.warning("%s: symbol_select(%s) failed", self.name, name)
            info = await self._get("symbol_info", name, what=f"{name} sembol bilgisi")
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

    async def _tick(self, broker_symbol: str) -> Any:
        return await self._get("symbol_info_tick", broker_symbol, what=f"{broker_symbol} fiyatı")

    async def _quote(self, broker_symbol: str) -> Quote:
        self._require()
        t = await self._tick(broker_symbol)
        bid, ask = _f(t.bid), _f(t.ask)
        if bid <= 0 or ask <= 0:
            raise BrokerError(f"{broker_symbol} için fiyat yok (piyasa kapalı olabilir).")
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
    def _fillings(self, broker_symbol: str, filling_flags: int) -> list[int]:
        """Filling modes to try, best first: cached > configured > symbol flags > the rest."""
        order: list[int] = []
        cached = self._filling_cache.get(broker_symbol)
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
        for filling in self._fillings(broker_symbol, flags):
            request["type_filling"] = filling
            res = await self._send(request)
            if res.retcode != MT5.TRADE_RETCODE_INVALID_FILL:
                if res.retcode in _DEAL_OK or res.retcode == MT5.TRADE_RETCODE_PLACED:
                    self._filling_cache[broker_symbol] = filling
                return res
            logger.info("%s: filling %s rejected for %s, trying next", self.name, filling, broker_symbol)
        return res

    async def _send_market(self, request: dict[str, Any], broker_symbol: str, flags: int, buy: bool) -> Any:
        """DEAL with a fresh price; retries requotes."""
        res = None
        for _ in range(3):
            t = await self._tick(broker_symbol)
            request["price"] = _f(t.ask if buy else t.bid)
            res = await self._send_filling(request, broker_symbol, flags)
            if res.retcode not in _REQUOTE_CODES:
                return res
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
        if res.retcode not in _DEAL_OK:
            raise BrokerError(f"{name} emri reddedildi: {retcode_text(res.retcode, getattr(res, 'comment', ''))}")
        if int(getattr(res, "order", 0) or 0) > 0:
            ticket = await self._position_ticket(int(res.order))
        else:   # no order ticket in the result: find our position by its tag
            positions, _ = await self._snapshot(name)
            found = [p["ticket"] for p in positions.values() if p["tag"] == tag]
            if not found:
                raise BrokerError(f"{name} emri gerçekleşti ama pozisyon numarası alınamadı; '{tag}' etiketli pozisyonu kontrol edin.")
            ticket = found[0]
        filled = _f(getattr(res, "volume", 0.0)) or float(volume)
        entry = _f(getattr(res, "price", 0.0)) or request["price"]
        return new_leg(ticket=ticket, state="open", volume=float(volume), filled=filled, entry=entry,
                       sl=None if naked else float(sl), tp=tp, tag=tag)

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
            tick = await self._tick(name)
            server_now = int(getattr(tick, "time", 0) or 0)   # expiration is in trade-server time, like tick.time
            if server_now > 0:
                request.update(type_time=MT5.ORDER_TIME_SPECIFIED, expiration=server_now + minutes * 60)
        flags = int(getattr(info, "filling_mode", 0) or 0)
        res = await self._send_filling(request, name, flags)
        if res.retcode == MT5.TRADE_RETCODE_INVALID_EXPIRATION and request["type_time"] == MT5.ORDER_TIME_SPECIFIED:
            request["type_time"] = MT5.ORDER_TIME_GTC
            request.pop("expiration", None)
            res = await self._send_filling(request, name, flags)
        if res.retcode not in (MT5.TRADE_RETCODE_PLACED, MT5.TRADE_RETCODE_DONE):
            raise BrokerError(f"{name} bekleyen emri reddedildi: {retcode_text(res.retcode, getattr(res, 'comment', ''))}")
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
            "comment": str(leg.get("tag") or "")[:31],
            "type_time": MT5.ORDER_TIME_GTC,
        }
        res = await self._send_market(request, symbol, int(getattr(info, "filling_mode", 0) or 0), closing_buy)
        if res.retcode not in _DEAL_OK:
            raise BrokerError(f"#{leg['ticket']} kapatılamadı: {retcode_text(res.retcode, getattr(res, 'comment', ''))}")
        closed = _f(getattr(res, "volume", 0.0)) or vol
        step = float(info.volume_step or 0.0)
        leg["open_volume"] = _snap(ov - closed, step)
        if leg["open_volume"] <= _EPS:
            leg["state"] = "closed"
            leg["reason"] = "manual"
        return closed

    async def _cancel_leg(self, symbol: str, leg: dict[str, Any]) -> None:
        self._require()
        res = await self._send({"action": MT5.TRADE_ACTION_REMOVE, "order": int(leg["order_ticket"])})
        if res.retcode not in (MT5.TRADE_RETCODE_DONE, MT5.TRADE_RETCODE_PLACED):
            raise BrokerError(f"Emir #{leg['order_ticket']} iptal edilemedi: {retcode_text(res.retcode, getattr(res, 'comment', ''))}")

    # ---- state primitives ---------------------------------------------------------------
    async def _snapshot(self, symbol: str) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        self._require()
        ps = await self._get_list("positions_get", symbol=symbol, what="pozisyonlar")
        os_ = await self._get_list("orders_get", symbol=symbol, what="bekleyen emirler")
        positions = {
            str(p.ticket): {
                "ticket": int(p.ticket), "volume": _f(p.volume), "entry": _f(p.price_open) or None,
                "sl": _f(p.sl) or None, "tp": _f(p.tp) or None, "tag": str(getattr(p, "comment", "") or ""),
                "magic": int(getattr(p, "magic", 0) or 0), "realized": None,
            }
            for p in ps
        }
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
        for d in (positions, orders):
            for v in d.values():
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
