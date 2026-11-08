"""Deterministic validation of parsed actions.

The LLM reads the messages; this module decides whether what it read is
safe to execute automatically. ANY returned issue means "ask the owner".
A *blocking* issue means the trade cannot be executed even if approved
(the owner must send a correction first) and the returned plan is None.

Issue codes are snake_case English; ``message_tr`` is shown to the owner.
"""

from __future__ import annotations

import logging
import math
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Literal

from ..config import LLMConfig, RiskConfig
from ..models import (
    ActionType,
    EntryType,
    ManagementPlan,
    ParsedAction,
    Side,
    TakeProfit,
    TradePlan,
    TradeRecord,
    TradeStatus,
    ValidationIssue,
)
from .symbols import canonical_asset, normalize_symbol

logger = logging.getLogger(__name__)

_MANAGEMENT_ACTIONS = frozenset({ActionType.MOVE_SL, ActionType.MOVE_TP, ActionType.CLOSE, ActionType.CANCEL})
_ACTIVE = frozenset({TradeStatus.OPENING, TradeStatus.PENDING, TradeStatus.OPEN})
_SPLIT_SUM_TOLERANCE = 0.01


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _fmt(x: float) -> str:
    """Price text for messages: 2232.18, 2240, 0.00001234 (no exponent)."""
    if not math.isfinite(x):
        return str(x)
    return format(Decimal(f"{x:.10g}").normalize(), "f")


def _pct(x: float) -> str:
    return format(Decimal(f"{x:.4g}").normalize(), "f")


def _positive(x: float | None) -> bool:
    return x is not None and math.isfinite(x) and x > 0


def _loss_side(side: Side, price: float, ref: float) -> bool:
    """``price`` is where a stop belongs relative to ``ref`` (long: below, short: above)."""
    return price < ref if side is Side.LONG else price > ref


def _profit_side(side: Side, price: float, ref: float) -> bool:
    """``price`` is where a take profit belongs relative to ``ref`` (long: above, short: below)."""
    return price > ref if side is Side.LONG else price < ref


def _distance_pct(a: float, ref: float) -> float:
    return abs(a - ref) / ref * 100.0


class _Issues:
    def __init__(self) -> None:
        self.items: list[ValidationIssue] = []

    def block(self, code: str, message: str) -> None:
        self.items.append(ValidationIssue(code=code, message_tr=message, blocking=True))

    def warn(self, code: str, message: str) -> None:
        self.items.append(ValidationIssue(code=code, message_tr=message, blocking=False))

    @property
    def blocking(self) -> bool:
        return any(i.blocking for i in self.items)


def _sanitize_market(market_price: float | None) -> float | None:
    if market_price is None:
        return None
    if not _positive(market_price):
        logger.warning("Ignoring invalid market price %r", market_price)
        return None
    return market_price


def _normalize_take_profits(
    tps: list[TakeProfit], side: Side, split: Literal["equal", "last_only"] = "equal"
) -> tuple[list[TakeProfit], bool]:
    """Sort nearest-first (long: ascending, short: descending) and set close fractions.

    An explicit split from the signal (fractions in (0, 1] summing to ~1) is kept and
    rescaled to sum exactly 1.0. Otherwise ``split`` applies: "equal" = 1/n each,
    "last_only" = only the farthest level is kept, closing everything.
    Returns (take_profits, split_was_invalid).
    """
    if not tps:
        return [], False
    fractions = [tp.close_fraction for tp in tps]
    explicit = any(f != 1.0 for f in fractions)
    valid_explicit = (
        explicit
        and all(math.isfinite(f) and 0 < f <= 1 for f in fractions)
        and abs(sum(fractions) - 1.0) <= _SPLIT_SUM_TOLERANCE + 1e-9  # 0.33 * 3 is 0.99000...01 off
    )

    merged: dict[float, float] = {}  # duplicate prices collapse into one level
    for tp in tps:
        merged[tp.price] = merged.get(tp.price, 0.0) + (tp.close_fraction if valid_explicit else 0.0)
    prices = sorted(merged, reverse=side is Side.SHORT)

    if len(prices) == 1:
        out = [1.0]
    elif valid_explicit:
        total = sum(merged.values())
        out = [merged[p] / total for p in prices]
    elif split == "last_only":
        return [TakeProfit(price=prices[-1], close_fraction=1.0)], explicit and not valid_explicit
    else:
        out = [1.0 / len(prices)] * len(prices)
    out[-1] = 1.0 - sum(out[:-1])  # exact sum of 1.0 despite float division
    return [TakeProfit(price=p, close_fraction=f) for p, f in zip(prices, out)], explicit and not valid_explicit


def _same_level(a: float, b: float) -> bool:
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-12)


def _is_filled(price: float, filled: list[float]) -> bool:
    return any(_same_level(price, f) for f in filled)


def _uniform(fractions: list[float]) -> bool:
    return not fractions or max(fractions) - min(fractions) <= 1e-6


def _split_close(a: float, b: float) -> bool:
    return abs(a - b) <= _SPLIT_SUM_TOLERANCE + 1e-9


def _carried_fractions(prices: list[float], open_current: list[TakeProfit], side: Side) -> dict[float, float]:
    """The trade's current fraction for each new price: an unchanged price keeps its own, a replaced level
    takes the fraction of the level it replaces (paired nearest-first, only when the counts match)."""
    old = {tp.price: tp.close_fraction for tp in open_current
           if math.isfinite(tp.close_fraction) and tp.close_fraction > 0}
    carried: dict[float, float] = {}
    for p in prices:
        for o, f in old.items():
            if _same_level(p, o):
                carried[p] = f
    unmatched_new = [p for p in prices if p not in carried]
    unmatched_old = sorted((o for o in old if not any(_same_level(o, p) for p in prices)),
                           reverse=side is Side.SHORT)
    if len(unmatched_new) == len(unmatched_old):
        for p, o in zip(unmatched_new, unmatched_old):
            carried[p] = old[o]
    return carried


def _move_tp_levels(
    incoming: list[TakeProfit], trade: TradeRecord, filled: list[float], side: Side
) -> tuple[list[TakeProfit], list[TakeProfit], ValidationIssue | None]:
    """New TP list for MOVE_TP that keeps the trader's split. Returns (levels, filled_levels, issue).

    Meaning of ``close_fraction`` in a MOVE_TP (prompt rule, same scale as the OPEN TRADES context): the
    level's share of the ORIGINAL position size; 1.0 means "not stated". The model is told to copy the
    fractions of unchanged levels, give a moved level the fraction of the level it replaces and leave out
    levels that already hit, so the stated fractions of the open levels add up to the share still open
    (1 minus the hit levels' shares), not to 1. The result is always renormalized over the levels still
    open, so either scale keeps the trader's split:

    * Levels already filled (``filled``: exact prices of the trade's current levels that hit) are never
      re-armed: they stay in the list with their recorded fraction (the engine skips them on every broker
      position where they filled), whether or not the model copied them.
    * Every open level stated: kept as weights when they add up to 1 (shares of what is open, or of the
      original with nothing hit yet), to the share still open (original shares, hit levels left out), or
      to 1 together with the copied hit levels. Any other sum is inconsistent: asked (``tp_split_invalid``),
      proposed proportionally.
    * Nothing stated (all 1.0): the trade's current fractions are carried over (unchanged price keeps its
      own, a replaced level takes the one it replaces, paired nearest-first). When the number of open
      levels changed and the old split was not equal it falls back to an equal split and asks
      (``tp_split_changed``).
    * Some stated, some 1.0: the stated levels keep their share of the original size and the unstated ones
      share what is left (by their carried fractions when known). Nothing left, or several unstated levels
      without carried fractions and an unequal old split: asked.
    * A fraction outside (0, 1] is invalid: treated as not stated, and asked.
    """
    current = list(trade.current_take_profits)
    filled_levels = [tp for tp in current if _is_filled(tp.price, filled)]
    open_current = [tp for tp in current if not _is_filled(tp.price, filled)]
    stated: dict[float, float] = {}
    seen: list[float] = []
    invalid = False
    hit_copied = 0.0                       # fractions the model copied for levels that already hit
    for tp in incoming:
        f = tp.close_fraction
        ok = math.isfinite(f) and 0 < f <= 1
        invalid = invalid or not ok
        if _is_filled(tp.price, filled):
            hit_copied += f if ok and f < 1.0 else 0.0
            continue
        if tp.price not in seen:
            seen.append(tp.price)
        if ok and f < 1.0:
            stated[tp.price] = stated.get(tp.price, 0.0) + f
    prices = sorted(seen, reverse=side is Side.SHORT)
    if not prices:
        return [], filled_levels, None

    done = sum(tp.close_fraction for tp in filled_levels if math.isfinite(tp.close_fraction) and tp.close_fraction > 0)
    share = 1.0 - done if 0 <= done < 1.0 else 1.0
    unstated = [p for p in prices if p not in stated]
    carried = _carried_fractions(prices, open_current, side)
    old_equal = _uniform([tp.close_fraction for tp in open_current])
    issue: ValidationIssue | None = None
    weights: dict[float, float] = {}

    def from_current() -> dict[float, float]:
        nonlocal issue
        if all(p in carried for p in prices):
            return {p: carried[p] for p in prices}
        if len(prices) > 1 and not old_equal:
            issue = ValidationIssue(
                code="tp_split_changed",
                message_tr="Hedef sayısı değişti; işlemin TP kapatma oranları korunamadı, açık hedefler eşit "
                           "bölündü.",
            )
        return {}

    if not stated:
        weights = from_current()
    elif not unstated:
        total = sum(stated.values())
        weights = dict(stated)
        consistent = _split_close(total, 1.0) or _split_close(total, share) or _split_close(total + hit_copied, 1.0)
        if len(prices) > 1 and not consistent:      # a single open level takes everything open anyway
            issue = ValidationIssue(
                code="tp_split_invalid",
                message_tr=(f"TP kapatma oranlarının toplamı tutmuyor (%{_pct(total * 100)}; açık kalan pozisyon "
                            f"%{_pct(share * 100)}); oranlar açık hedeflere orantılı bölündü."),
            )
    else:
        rest = share - sum(stated.values())
        if rest > _SPLIT_SUM_TOLERANCE:
            weights = dict(stated)
            if len(unstated) == 1:
                weights[unstated[0]] = rest
            elif all(p in carried for p in unstated):
                carried_total = sum(carried[p] for p in unstated)
                for p in unstated:
                    weights[p] = rest * carried[p] / carried_total
            elif old_equal:
                for p in unstated:
                    weights[p] = rest / len(unstated)
            else:
                weights = {}
        if not weights:
            weights = from_current()
            if issue is None:
                used = "işlemin mevcut oranları kullanıldı." if weights else "açık hedefler eşit bölündü."
                issue = ValidationIssue(
                    code="tp_split_invalid",
                    message_tr="TP kapatma oranları tutarsız (bazı hedeflerin oranı yok, kalan pay tutmuyor); "
                               + used,
                )
    if invalid and issue is None:
        issue = ValidationIssue(code="tp_split_invalid",
                                message_tr="TP kapatma oranlarından biri geçersiz (0 ile 1 arasında değil); "
                                           "o oran yok sayıldı.")
    if not weights or any(not (math.isfinite(weights.get(p, 0.0)) and weights.get(p, 0.0) > 0) for p in prices):
        weights = {p: 1.0 for p in prices}
    total_w = sum(weights[p] for p in prices)
    levels = [TakeProfit(price=p, close_fraction=share * weights[p] / total_w) for p in prices]
    combined = sorted(filled_levels + levels, key=lambda tp: tp.price, reverse=side is Side.SHORT)
    total = sum(tp.close_fraction for tp in combined)
    out = [TakeProfit(price=tp.price, close_fraction=tp.close_fraction / total) for tp in combined]
    out[-1] = TakeProfit(price=out[-1].price, close_fraction=1.0 - sum(tp.close_fraction for tp in out[:-1]))
    return out, filled_levels, issue


def _side_word(side: Side) -> str:
    return "Long" if side is Side.LONG else "Short"


# --------------------------------------------------------------------------- #
# Open
# --------------------------------------------------------------------------- #
def build_open_plan(
    action: ParsedAction,
    *,
    risk: RiskConfig,
    llm: LLMConfig,
    market_price: float | None,
    source_chat_id: int,
    now: datetime,
    plan_id: str | None = None,
    route: str | None = None,
    broker_names: list[str] | None = None,
) -> tuple[TradePlan | None, list[ValidationIssue]]:
    """Validate an OPEN action and turn it into a TradePlan.

    ``route`` / ``broker_names``: the forum topic route the signal came from (TopicRoute.name) and the
    brokers it may be traded on (empty = every enabled broker); copied onto the plan unchanged.

    Blocking issues (plan is None): missing asset / side / stop loss; stop on the
    wrong side of entry (long: SL >= entry; short: SL <= entry); market price
    already beyond the stop; a take profit on the wrong side of entry;
    non-positive prices.
    Non-blocking issues (plan returned, but ask first): confidence below
    llm.min_confidence; parser ambiguities; risk_r missing and
    risk.ask_if_r_missing; risk_r > risk.max_r_per_trade; entry farther than
    risk.max_entry_distance_pct from market; market price already beyond TP1;
    no take profit given; market_price unknown.
    Decisions: entry type from risk.entry_mode ("auto": market if the market is
    within market_entry_tolerance_pct of the entry or no entry price given,
    otherwise limit at the entry; an explicit entry_type from the signal wins),
    risk_r defaults to risk.default_r, risk_pct = risk_r * risk.risk_per_r_pct,
    take profits sorted nearest-first with close_fraction set per
    risk.multi_tp_split (fractions sum to 1.0) unless the signal gave a split.

    Additional rules: the reference price for side checks is the entry if given,
    else the market price (neither known -> blocking ``price_unknown``); market
    beyond TP1 is blocking for a MARKET entry (the TP would be behind the fill);
    a LIMIT entry needs a price; a marketable limit (long above / short below the
    market), an explicit market entry that drifted beyond the tolerance and a
    symbol/asset mismatch are non-blocking. ``auto`` with an unknown market price
    chooses LIMIT at the entry (never chase blindly).
    """
    issues = _Issues()
    market = _sanitize_market(market_price)

    if action.action not in (ActionType.OPEN, ActionType.INCOMPLETE):
        issues.block("unsupported_action", f"Bu talimat yeni işlem açma talimatı değil ({action.action.value}).")

    # ---- asset ---------------------------------------------------------------
    base = canonical_asset(action.base_asset) if action.base_asset and action.base_asset.strip() else None
    quote = action.quote_asset.strip().upper() if action.quote_asset and action.quote_asset.strip() else None
    market_type = action.market_type
    if action.symbol:
        sym_base, sym_quote, sym_type = normalize_symbol(action.symbol)
        if base is None:
            base = sym_base
        if sym_base is not None and sym_base == base:
            quote = quote or sym_quote
            market_type = market_type or sym_type
        elif sym_base is not None:
            issues.warn(
                "symbol_asset_mismatch",
                f"Sembol ({action.symbol}) ile varlık ({base}) uyuşmuyor.",
            )
    if not base:
        issues.block("missing_asset", "İşlem yapılacak varlık (sembol) anlaşılamadı.")

    # ---- side / prices ---------------------------------------------------------
    side = action.side
    if side is None:
        issues.block("missing_side", "İşlem yönü (long/short) anlaşılamadı.")

    entry = action.entry_price
    stop = action.stop_loss
    if entry is not None and not _positive(entry):
        issues.block("invalid_price", f"Geçersiz giriş fiyatı ({entry}).")
        entry = None
    if stop is None:
        issues.block("missing_stop_loss", "Stop seviyesi bulunamadı — stopsuz işlem açılmaz.")
    elif not _positive(stop):
        issues.block("invalid_price", f"Geçersiz stop fiyatı ({stop}).")
        stop = None
    raw_tps: list[TakeProfit] = []
    for tp in action.take_profits:
        if _positive(tp.price):
            raw_tps.append(tp)
        else:
            issues.block("invalid_price", f"Geçersiz hedef (TP) fiyatı ({tp.price}).")

    ref = entry if entry is not None else market
    ref_word = "giriş" if entry is not None else "piyasa"
    if ref is None and action.entry_price is None:
        issues.block("price_unknown", "Giriş fiyatı verilmedi ve piyasa fiyatı alınamadı — fiyat bilinmiyor.")

    if side is not None and ref is not None:
        if stop is not None and not _loss_side(side, stop, ref):
            where = "altında" if side is Side.LONG else "üstünde"
            issues.block(
                "sl_wrong_side",
                f"{_side_word(side)} işlemde stop {'girişin' if entry is not None else 'piyasa fiyatının'} "
                f"{where} olmalı ({ref_word} {_fmt(ref)}, stop {_fmt(stop)}).",
            )
        for tp in raw_tps:
            if not _profit_side(side, tp.price, ref):
                where = "üstünde" if side is Side.LONG else "altında"
                issues.block(
                    "tp_wrong_side",
                    f"{_side_word(side)} işlemde hedef {'girişin' if entry is not None else 'piyasa fiyatının'} "
                    f"{where} olmalı ({ref_word} {_fmt(ref)}, TP {_fmt(tp.price)}).",
                )
    if side is not None and entry is not None and market is not None and stop is not None:
        if not _loss_side(side, stop, market):
            where = "altına inmiş" if side is Side.LONG else "üstüne çıkmış"
            issues.block(
                "market_beyond_stop",
                f"Piyasa fiyatı ({_fmt(market)}) zaten stop seviyesinin ({_fmt(stop)}) {where} — işlem geçersiz.",
            )

    # ---- entry type ------------------------------------------------------------
    explicit_type = action.entry_type
    if explicit_type is not None:
        entry_type = explicit_type
    elif risk.entry_mode == "market":
        entry_type = EntryType.MARKET
    elif risk.entry_mode == "limit":
        if entry is None:
            entry_type = EntryType.MARKET
            issues.warn("limit_without_entry", "Giriş fiyatı verilmedi; limit yerine piyasa emri kullanılacak.")
        else:
            entry_type = EntryType.LIMIT
    elif entry is None:
        entry_type = EntryType.MARKET
    elif market is None:
        entry_type = EntryType.LIMIT
    else:
        within = _distance_pct(market, entry) <= risk.market_entry_tolerance_pct
        entry_type = EntryType.MARKET if within else EntryType.LIMIT

    if entry_type is EntryType.LIMIT and entry is None and action.entry_price is None:
        issues.block("missing_limit_price", "Limit emir için giriş fiyatı yok.")

    # ---- market-relative checks ------------------------------------------------
    if market is None:
        if entry is not None:
            issues.warn(
                "market_price_unknown",
                "Piyasa fiyatı alınamadı; giriş ve stop piyasaya göre kontrol edilemedi.",
            )
    elif entry is not None and side is not None:
        dist = _distance_pct(market, entry)
        if dist > risk.max_entry_distance_pct:
            issues.warn(
                "entry_far_from_market",
                f"Giriş ({_fmt(entry)}) piyasa fiyatından ({_fmt(market)}) %{_pct(dist)} uzak — "
                f"fiyat yanlış okunmuş olabilir.",
            )
        if explicit_type is EntryType.MARKET and dist > risk.market_entry_tolerance_pct:
            issues.warn(
                "market_far_from_entry",
                f"Sinyal piyasa emri diyor ama anlık fiyat ({_fmt(market)}) girişten ({_fmt(entry)}) "
                f"%{_pct(dist)} uzak.",
            )
        if entry_type is EntryType.LIMIT and _profit_side(side, entry, market):
            where = "üstünde" if side is Side.LONG else "altında"
            issues.warn(
                "limit_entry_marketable",
                f"{_side_word(side)} limit giriş ({_fmt(entry)}) piyasa fiyatının ({_fmt(market)}) {where} — "
                f"limit emir hemen anlık fiyattan dolar (kırılım/stop emri desteklenmiyor).",
            )
    if entry is not None and market is not None and side is not None and raw_tps:
        tp1 = min(raw_tps, key=lambda t: t.price) if side is Side.LONG else max(raw_tps, key=lambda t: t.price)
        if not _profit_side(side, tp1.price, market):
            where = "üstüne çıkmış" if side is Side.LONG else "altına inmiş"
            text = f"Piyasa fiyatı ({_fmt(market)}) ilk hedefin ({_fmt(tp1.price)}) {where}."
            if entry_type is EntryType.MARKET:
                issues.block("market_beyond_tp", text + " Piyasa emriyle girilemez.")
            else:
                issues.warn("market_beyond_tp", text)

    if not action.take_profits:
        issues.warn("no_take_profit", "Hedef (TP) verilmedi — işlem hedefsiz açılacak.")

    # ---- risk --------------------------------------------------------------------
    risk_r = action.risk_r
    if risk_r is None:
        risk_r = risk.default_r
        if risk.ask_if_r_missing:
            issues.warn("risk_r_missing", f"Risk (R) belirtilmedi; varsayılan {_fmt(risk_r)}R kullanılacak.")
    if not _positive(risk_r):
        issues.block("invalid_risk", f"Geçersiz risk değeri ({risk_r}R).")
    elif risk_r > risk.max_r_per_trade:
        issues.warn(
            "risk_r_too_high",
            f"Sinyal riski {_fmt(risk_r)}R (%{_pct(risk_r * risk.risk_per_r_pct)}) — izin verilen en fazla "
            f"{_fmt(risk.max_r_per_trade)}R. Onaylanırsa {_fmt(risk_r)}R ile açılır.",
        )

    # ---- take profits --------------------------------------------------------------
    take_profits: list[TakeProfit] = []
    if side is not None:
        take_profits, split_invalid = _normalize_take_profits(raw_tps, side, risk.multi_tp_split)
        if split_invalid:
            issues.warn("tp_split_invalid", "TP kapatma oranları geçersiz; varsayılan bölüşüm kullanıldı.")

    issues.items.extend(low_confidence_issues(action, llm))

    if issues.blocking:
        return None, issues.items
    assert base is not None and side is not None and stop is not None and risk_r is not None

    plan_entry = entry if entry_type is EntryType.LIMIT else (entry if entry is not None else market)
    plan = TradePlan(
        plan_id=plan_id or uuid.uuid4().hex[:10],
        base_asset=base,
        quote_asset=quote,
        market_type=market_type,
        side=side,
        entry_type=entry_type,
        entry_price=plan_entry,
        stop_loss=stop,
        take_profits=take_profits,
        risk_r=risk_r,
        risk_pct=risk_r * risk.risk_per_r_pct,
        source_chat_id=source_chat_id,
        source_message_ids=list(action.used_message_ids),
        created_at=now,
        route=route,
        broker_names=list(broker_names or []),
    )
    plan.summary_tr = _open_summary(plan, entry_is_market_price=entry is None)
    return plan, issues.items


def _open_summary(plan: TradePlan, *, entry_is_market_price: bool) -> str:
    kind = "piyasa" if plan.entry_type is EntryType.MARKET else "limit"
    if plan.entry_price is None:
        entry = "giriş ?"
    elif entry_is_market_price:
        entry = f"giriş ~{_fmt(plan.entry_price)} (anlık fiyat)"
    else:
        entry = f"giriş {_fmt(plan.entry_price)}"
    tps = " / ".join(_fmt(tp.price) for tp in plan.take_profits) or "yok"
    out = (
        f"{plan.base_asset} {plan.side.value.upper()} ({kind}), {entry}, SL {_fmt(plan.stop_loss)}, "
        f"TP {tps}, risk {_fmt(plan.risk_r)}R (%{_pct(plan.risk_pct)})"
    )
    if plan.route:
        out += f" · konu {plan.route} → {', '.join(plan.broker_names) or 'tüm brokerlar'}"
    return out


# --------------------------------------------------------------------------- #
# Management
# --------------------------------------------------------------------------- #
def _trade_label(trade: TradeRecord) -> str:
    return f"#{trade.id} {trade.plan.base_asset} {trade.plan.side.value.upper()}"


def build_management_plan(
    action: ParsedAction,
    *,
    trade: TradeRecord | None,
    candidates: list[TradeRecord],
    market_price: float | None,
    source_chat_id: int,
    now: datetime,
    plan_id: str | None = None,
    filled_tp_prices: list[float] | None = None,
) -> tuple[ManagementPlan | None, list[ValidationIssue]]:
    """Validate MOVE_SL / MOVE_TP / CLOSE / CANCEL against the matched trade.

    ``filled_tp_prices``: prices of the trade's current take profits that already filled on a broker
    (MOVE_TP keeps them as done and never re-arms them; see ``_move_tp_levels``).

    ``trade`` is the resolved target (None if it could not be resolved);
    ``candidates`` are the active trades that could match (used in messages).
    Blocking: no trade ("trade_not_found"), several candidates and no trade
    ("ambiguous_trade"), MOVE_SL without a price and not to-entry, new SL on the
    wrong side of the market (long: new SL >= market), MOVE_TP without prices,
    close_fraction outside (0, 1].
    Non-blocking: confidence below threshold / parser ambiguities are NOT
    checked here (the engine applies llm.min_confidence uniformly).

    Additional rules: trade not active -> blocking; for a PENDING trade the new
    SL / TPs are also checked against the entry; TPs on the wrong side of the
    market or of the current stop -> blocking; market price unknown, a stop
    moved farther away (more risk), CLOSE without a fraction (-> 1.0) and CANCEL
    of an already filled trade -> non-blocking.
    """
    issues = _Issues()
    if action.action not in _MANAGEMENT_ACTIONS:
        issues.block("unsupported_action", f"Bu talimat bir işlem yönetimi talimatı değil ({action.action.value}).")
        return None, issues.items
    if trade is None:
        if len(candidates) > 1:
            listed = ", ".join(_trade_label(t) for t in candidates)
            issues.block("ambiguous_trade", f"Birden fazla işlem eşleşiyor ({listed}); hangisi olduğu belli değil.")
        else:
            issues.block("trade_not_found", "Bu talimatın ait olduğu açık işlem bulunamadı.")
        return None, issues.items

    label = _trade_label(trade)
    if trade.status not in _ACTIVE:
        issues.block("trade_not_active", f"{label} işlemi aktif değil (durum: {trade.status.value}).")

    market = _sanitize_market(market_price)
    side = trade.plan.side
    pending = trade.status is TradeStatus.PENDING
    plan_entry = trade.plan.entry_price
    new_sl: float | None = None
    take_profits: list[TakeProfit] = []
    close_fraction: float | None = None
    summary = label

    if action.action is ActionType.MOVE_SL:
        if action.move_sl_to_entry:
            new_sl = plan_entry
            if new_sl is None:
                issues.block("entry_unknown", f"{label} işleminin giriş fiyatı bilinmiyor; stop girişe çekilemez.")
        elif action.new_stop_loss is not None:
            if _positive(action.new_stop_loss):
                new_sl = action.new_stop_loss
            else:
                issues.block("invalid_price", f"Geçersiz yeni stop fiyatı ({action.new_stop_loss}).")
        else:
            issues.block("missing_new_stop", "Yeni stop seviyesi belirtilmedi.")
        if new_sl is not None:
            below = "altında" if side is Side.LONG else "üstünde"
            if market is None:
                issues.warn("market_price_unknown", "Piyasa fiyatı alınamadı; yeni stop piyasaya göre kontrol edilemedi.")
            elif not _loss_side(side, new_sl, market):
                issues.block(
                    "sl_wrong_side",
                    f"{_side_word(side)} işlemde yeni stop piyasa fiyatının {below} olmalı "
                    f"(piyasa {_fmt(market)}, yeni stop {_fmt(new_sl)}).",
                )
            if pending and plan_entry is not None and not _loss_side(side, new_sl, plan_entry):
                issues.block(
                    "sl_wrong_side",
                    f"Bekleyen {_side_word(side).lower()} işlemde stop girişin {below} olmalı "
                    f"(giriş {_fmt(plan_entry)}, yeni stop {_fmt(new_sl)}).",
                )
            if _loss_side(side, new_sl, trade.current_stop_loss):
                issues.warn(
                    "sl_widened",
                    f"Yeni stop ({_fmt(new_sl)}) mevcut stoptan ({_fmt(trade.current_stop_loss)}) daha uzak — risk artar.",
                )
            to_entry = " (giriş)" if action.move_sl_to_entry else ""
            summary = f"{label}: stop {_fmt(new_sl)}{to_entry} seviyesine çekilecek"

    elif action.action is ActionType.MOVE_TP:
        valid = [tp for tp in action.take_profits if _positive(tp.price)]
        if len(valid) != len(action.take_profits):
            issues.block("invalid_price", "Geçersiz hedef (TP) fiyatı.")
        if not action.take_profits:
            issues.block("missing_take_profits", "Yeni hedef (TP) seviyeleri belirtilmedi.")
        filled = list(filled_tp_prices or [])
        take_profits, _done, split_issue = _move_tp_levels(valid, trade, filled, side)
        if split_issue is not None:
            issues.warn(split_issue.code, split_issue.message_tr)
        if valid and not any(not _is_filled(tp.price, filled) for tp in take_profits):
            issues.block("tp_all_filled", "Yeni hedeflerin hepsi zaten gerçekleşmiş seviyeler; değiştirilecek açık "
                                          "hedef yok.")
        above = "üstünde" if side is Side.LONG else "altında"
        refs: list[tuple[str, float]] = [("mevcut stop", trade.current_stop_loss)]
        if market is not None:
            refs.append(("piyasa", market))
        if pending and plan_entry is not None:
            refs.append(("giriş", plan_entry))
        for tp in take_profits:
            if _is_filled(tp.price, filled):
                continue      # already done: not re-placed, so its side vs the market does not matter
            for name, ref in refs:
                if not _profit_side(side, tp.price, ref):
                    issues.block(
                        "tp_wrong_side",
                        f"{_side_word(side)} işlemde hedef {name} fiyatının {above} olmalı "
                        f"({name} {_fmt(ref)}, TP {_fmt(tp.price)}).",
                    )
                    break
        if market is None and take_profits:
            issues.warn("market_price_unknown", "Piyasa fiyatı alınamadı; yeni hedefler piyasaya göre kontrol edilemedi.")
        summary = f"{label}: yeni hedefler " + (" / ".join(
            _fmt(tp.price) + (" (gerçekleşti)" if _is_filled(tp.price, filled) else "") for tp in take_profits
        ) or "yok")

    elif action.action is ActionType.CLOSE:
        close_fraction = action.close_fraction
        if close_fraction is None:
            close_fraction = 1.0
            issues.warn("close_fraction_missing", "Kapatma oranı belirtilmedi; pozisyonun tamamı kapatılacak.")
        elif not (math.isfinite(close_fraction) and 0 < close_fraction <= 1):
            issues.block("invalid_close_fraction", f"Kapatma oranı geçersiz ({close_fraction}).")
        if close_fraction is not None and math.isfinite(close_fraction) and close_fraction >= 1:
            summary = f"{label}: pozisyonun tamamı kapatılacak"
        else:
            summary = f"{label}: pozisyon kısmen kapatılacak (%{_pct((close_fraction or 0) * 100)})"

    else:  # CANCEL
        if trade.status is TradeStatus.OPEN:
            issues.warn(
                "cancel_filled_trade",
                f"{label} girişi zaten dolmuş; iptal talimatı pozisyonun kapatılması anlamına gelir.",
            )
            summary = f"{label}: işlem iptal edilecek (açık pozisyon kapatılır)"
        else:
            summary = f"{label}: bekleyen giriş iptal edilecek"

    if issues.blocking:
        return None, issues.items
    return (
        ManagementPlan(
            plan_id=plan_id or uuid.uuid4().hex[:10],
            action=action.action,
            trade_id=trade.id,
            new_stop_loss=new_sl,
            move_sl_to_entry=action.move_sl_to_entry if action.action is ActionType.MOVE_SL else False,
            take_profits=take_profits,
            close_fraction=close_fraction,
            source_chat_id=source_chat_id,
            source_message_ids=list(action.used_message_ids),
            summary_tr=summary,
            created_at=now,
        ),
        issues.items,
    )


def low_confidence_issues(action: ParsedAction, llm: LLMConfig) -> list[ValidationIssue]:
    """Issues for confidence < llm.min_confidence and for every parser ambiguity (non-blocking)."""
    issues = _Issues()
    if action.confidence < llm.min_confidence:
        issues.warn("low_confidence", f"Güven düşük (%{round(action.confidence * 100)}).")
    for text in action.ambiguities:
        if text and text.strip():
            issues.warn("ambiguity", text.strip())
    return issues.items
