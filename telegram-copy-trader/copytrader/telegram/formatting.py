"""Turkish, HTML-safe texts for the owner bot (pure functions).

Everything returned here is Telegram HTML: untrusted text (trader messages,
LLM output, symbols) is escaped with :func:`esc`; the markup is ours.
"""

from __future__ import annotations

import html
import math
from collections.abc import Iterable
from decimal import Decimal, InvalidOperation

from ..models import (
    ActionType,
    EntryType,
    ManagementPlan,
    MarketType,
    ParsedAction,
    PendingConfirmation,
    Side,
    TakeProfit,
    TradePlan,
    TradeRecord,
    TradeStatus,
    ValidationIssue,
)

HEADER_UNSURE = "🤔 <b>Emin olamadım — şunu anladım, doğru mu?</b>"
HEADER_INCOMPLETE = "🧩 <b>Eksik sinyal — tamamlayabilir misin?</b>"
CORRECTION_HINT = (
    "✏️ Doğrusunu yazın, ör: <code>SL 2200, TP 2320</code> veya <code>short olacak</code>"
)

_MARKET_TR = {
    MarketType.PERP: "vadeli (perp)",
    MarketType.SPOT: "spot",
    MarketType.FOREX: "forex",
    MarketType.CFD: "CFD",
}
_STATUS_TR = {
    TradeStatus.OPENING: "açılıyor",
    TradeStatus.PENDING: "bekleyen emir",
    TradeStatus.OPEN: "açık",
    TradeStatus.CLOSED: "kapalı",
    TradeStatus.CANCELLED: "iptal",
    TradeStatus.FAILED: "başarısız",
}
_MISSING = "❓ <i>yok</i>"


# --------------------------------------------------------------------------- #
# Small helpers (public: the engine may reuse them)
# --------------------------------------------------------------------------- #
def esc(text: object) -> str:
    """Escape untrusted text for Telegram HTML."""
    return html.escape("" if text is None else str(text), quote=False)


def fmt_price(price: float | None) -> str:
    """2310.6 -> '2310.6', 1.2e-05 -> '0.000012' (no float noise, no exponent)."""
    if price is None:
        return "—"
    if isinstance(price, float) and not math.isfinite(price):
        return esc(price)
    try:
        d = Decimal(repr(float(price))).normalize()
    except (InvalidOperation, ValueError, TypeError):
        return esc(price)
    text = format(d, "f")
    return "0" if text in ("-0", "") else text


def fmt_number(value: float) -> str:
    """Short number for R / percentages: 1.0 -> '1', 0.5 -> '0.5', 33.333 -> '33.3'."""
    if not math.isfinite(value):
        return str(value)
    rounded = round(value, 1) if abs(value) >= 10 else round(value, 2)
    text = f"{rounded:f}".rstrip("0").rstrip(".")
    return "0" if text in ("-0", "") else text


_DIGIT_SUFFIX = {"1": "i", "2": "si", "3": "ü", "4": "ü", "5": "i", "6": "sı", "7": "si", "8": "i", "9": "u"}
_TENS_SUFFIX = {"1": "u", "2": "si", "3": "u", "4": "ı", "5": "si", "6": "ı", "7": "i", "8": "i", "9": "ı"}


def tr_possessive(number_text: str) -> str:
    """Turkish possessive suffix for a written number: '2' -> "'si", '10' -> "'u", '0.5' -> "'i"."""
    digits = number_text.strip().lstrip("%")
    if not digits or not digits[-1].isdigit():
        return ""
    if digits[-1] != "0":
        return "'" + _DIGIT_SUFFIX[digits[-1]]
    integer = digits.split(".")[0].split(",")[0].lstrip("-")
    stripped = integer.rstrip("0")
    zeros = len(integer) - len(stripped)
    if not stripped:
        return "'ı"                      # sıfır
    if zeros == 1:
        return "'" + _TENS_SUFFIX[stripped[-1]]
    if zeros == 2:
        return "'ü"                      # yüz
    return "'i"                          # bin, milyon (good enough)


def fmt_pct(value_pct: float, *, possessive: bool = False) -> str:
    """'%2' or, with possessive, "%2'si"."""
    num = fmt_number(value_pct)
    return f"%{num}{tr_possessive(num) if possessive else ''}"


def fmt_side(side: Side | None) -> str:
    if side is Side.LONG:
        return "📈 <b>LONG</b> (alış)"
    if side is Side.SHORT:
        return "📉 <b>SHORT</b> (satış)"
    return _MISSING


def fmt_entry_type(entry_type: EntryType | None) -> str:
    if entry_type is EntryType.MARKET:
        return "piyasa"
    if entry_type is EntryType.LIMIT:
        return "limit"
    return "belirtilmedi"


def _asset_label(base: str | None, quote: str | None, symbol: str | None = None) -> str:
    if base:
        label = esc(base.upper()) + (f"/{esc(quote.upper())}" if quote else "")
        if symbol and symbol.upper() not in (base.upper(), f"{base}/{quote}".upper()):
            label += f" <i>({esc(symbol)})</i>"
        return label
    if symbol:
        return esc(symbol)
    return _MISSING


def _sl_distance(entry: float | None, stop: float | None) -> str:
    if not entry or stop is None:
        return ""
    return f" <i>(girişten {fmt_pct(abs(entry - stop) / entry * 100)})</i>"


def _rr(entry: float | None, stop: float | None, tp: float) -> str:
    if entry is None or stop is None or entry == stop:
        return ""
    return f" ≈{fmt_number(abs(tp - entry) / abs(entry - stop))}R"


def _tp_lines(tps: Iterable[TakeProfit], entry: float | None, stop: float | None) -> list[str]:
    tps = list(tps)
    if not tps:
        return ["🎯 TP: ❓ <i>yok</i>"]
    lines = []
    for i, tp in enumerate(tps, 1):
        share = fmt_pct(tp.close_fraction * 100, possessive=True)
        lines.append(f"🎯 TP{i}: <b>{fmt_price(tp.price)}</b>{_rr(entry, stop, tp.price)} — pozisyonun {share}")
    return lines


def _risk_line(risk_r: float | None, risk_pct: float | None) -> str:
    if risk_r is None:
        return "⚖️ Risk: <i>belirtilmedi (varsayılan R kullanılır)</i>"
    text = f"⚖️ Risk: <b>{fmt_number(risk_r)}R</b>"
    if risk_pct is not None:
        text += f" = bakiyenin {fmt_pct(risk_pct, possessive=True)}"
    return text


def _summary_line(summary: str | None) -> list[str]:
    return [f"📝 <i>{esc(summary.strip())}</i>"] if summary and summary.strip() else []


def _truncate(text: str, limit: int) -> str:
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


# --------------------------------------------------------------------------- #
# Public formatters
# --------------------------------------------------------------------------- #
def format_plan(plan: TradePlan) -> str:
    """A fully specified new trade."""
    market = f" · {_MARKET_TR[plan.market_type]}" if plan.market_type in _MARKET_TR else ""
    lines = [f"<b>{_asset_label(plan.base_asset, plan.quote_asset)}</b> {fmt_side(plan.side)}{market}"]
    if plan.entry_type is EntryType.MARKET:
        ref = f" <i>(sinyaldeki giriş: {fmt_price(plan.entry_price)})</i>" if plan.entry_price else ""
        lines.append(f"➡️ Giriş: <b>piyasa</b> fiyatından{ref}")
    else:
        lines.append(f"➡️ Giriş: <b>limit</b> @ <b>{fmt_price(plan.entry_price)}</b>")
    lines.append(f"🛑 Stop: <b>{fmt_price(plan.stop_loss)}</b>{_sl_distance(plan.entry_price, plan.stop_loss)}")
    lines += _tp_lines(plan.take_profits, plan.entry_price, plan.stop_loss)
    lines.append(_risk_line(plan.risk_r, plan.risk_pct))
    lines += _summary_line(plan.summary_tr)
    return "\n".join(lines)


def format_action(action: ParsedAction) -> str:
    """What the parser understood (fields may be missing)."""
    a = action.action
    asset = _asset_label(action.base_asset, action.quote_asset, action.symbol)
    lines: list[str] = []
    if a in (ActionType.OPEN, ActionType.INCOMPLETE):
        title = "🆕 <b>Yeni işlem</b>" if a is ActionType.OPEN else "🧩 <b>Eksik sinyal</b>"
        market = f" · {_MARKET_TR[action.market_type]}" if action.market_type in _MARKET_TR else ""
        lines.append(f"{title}: <b>{asset}</b> {fmt_side(action.side)}{market}")
        entry = fmt_price(action.entry_price) if action.entry_price is not None else None
        if action.entry_type is EntryType.LIMIT:
            lines.append(f"➡️ Giriş: <b>limit</b> @ <b>{entry or '❓'}</b>")
        elif action.entry_type is EntryType.MARKET:
            lines.append("➡️ Giriş: <b>piyasa</b>" + (f" <i>(sinyaldeki giriş: {entry})</i>" if entry else ""))
        else:
            lines.append(f"➡️ Giriş: <b>{entry}</b> <i>(tür belirtilmedi)</i>" if entry else f"➡️ Giriş: {_MISSING}")
        stop = f"<b>{fmt_price(action.stop_loss)}</b>" if action.stop_loss is not None else _MISSING
        lines.append(f"🛑 Stop: {stop}{_sl_distance(action.entry_price, action.stop_loss)}")
        lines += _tp_lines(action.take_profits, action.entry_price, action.stop_loss)
        lines.append(_risk_line(action.risk_r, None))
    elif a is ActionType.MOVE_SL:
        if action.move_sl_to_entry:
            target = "girişe (başabaş)"
        elif action.new_stop_loss is not None:
            target = f"<b>{fmt_price(action.new_stop_loss)}</b>"
        else:
            target = _MISSING
        lines.append(f"🔧 <b>Stop taşı</b> ({asset}): yeni stop {target}")
    elif a is ActionType.MOVE_TP:
        lines.append(f"🎯 <b>TP güncelle</b> ({asset})")
        lines += _tp_lines(action.take_profits, None, None)
    elif a is ActionType.CLOSE:
        lines.append(f"🔒 <b>Kapat</b> ({asset}): {_close_amount(action.close_fraction)}")
    elif a is ActionType.CANCEL:
        lines.append(f"🚫 <b>İptal</b> ({asset}): bekleyen giriş emri / sinyal iptal")
    else:
        lines.append("💬 İşlem talimatı değil")
    if a not in (ActionType.OPEN, ActionType.INCOMPLETE, ActionType.NONE):
        ref = []
        if action.target_trade_id is not None:
            ref.append(f"işlem #{action.target_trade_id}")
        if action.refers_to_message_id is not None:
            ref.append(f"mesaj #{action.refers_to_message_id}")
        lines.append("🔗 Hedef: " + (", ".join(ref) if ref else "❓ <i>belirsiz</i>"))
    lines += _summary_line(action.summary_tr)
    return "\n".join(lines)


def _close_amount(fraction: float | None) -> str:
    if fraction is None or fraction >= 0.999:
        return "<b>tamamı</b>"
    return f"<b>{fmt_pct(fraction * 100)}</b>"


def format_trade_line(trade: TradeRecord) -> str:
    """One line describing an existing trade."""
    p = trade.plan
    side = "LONG" if p.side is Side.LONG else "SHORT"
    entry = f" · giriş {fmt_price(p.entry_price)}" if p.entry_price is not None else ""
    return (
        f"#{trade.id} {esc(p.base_asset.upper())} {side}{entry} · stop {fmt_price(trade.current_stop_loss)}"
        f" · {_STATUS_TR.get(trade.status, esc(trade.status.value))}"
    )


def format_management(plan: ManagementPlan, trade: TradeRecord | None) -> str:
    """An instruction for an existing trade."""
    target = format_trade_line(trade) if trade else f"#{plan.trade_id}"
    lines = [f"🔗 İşlem: {target}"]
    if plan.action is ActionType.MOVE_SL:
        current = f"{fmt_price(trade.current_stop_loss)} → " if trade else ""
        if plan.move_sl_to_entry:
            entry = trade.plan.entry_price if trade else None
            new = "girişe (başabaş)" + (f" <b>{fmt_price(entry)}</b>" if entry is not None else "")
        elif plan.new_stop_loss is not None:
            new = f"<b>{fmt_price(plan.new_stop_loss)}</b>"
        else:
            new = _MISSING
        lines.insert(0, "🔧 <b>Stop taşı</b>")
        lines.append(f"🛑 Stop: {current}{new}")
    elif plan.action is ActionType.MOVE_TP:
        lines.insert(0, "🎯 <b>TP güncelle</b>")
        if trade and trade.current_take_profits:
            old = ", ".join(fmt_price(tp.price) for tp in trade.current_take_profits)
            lines.append(f"Eski TP: {old}")
        entry = trade.plan.entry_price if trade else None
        stop = trade.current_stop_loss if trade else None
        lines += _tp_lines(plan.take_profits, entry, stop)
    elif plan.action is ActionType.CLOSE:
        lines.insert(0, f"🔒 <b>Kapat</b>: {_close_amount(plan.close_fraction)}")
    elif plan.action is ActionType.CANCEL:
        lines.insert(0, "🚫 <b>Bekleyen giriş emri iptal</b>")
    else:
        lines.insert(0, f"⚙️ <b>{esc(plan.action.value)}</b>")
    lines += _summary_line(plan.summary_tr)
    return "\n".join(lines)


def format_issues(issues: Iterable[ValidationIssue], ambiguities: Iterable[str] = ()) -> list[str]:
    lines = [f"{'⛔' if i.blocking else '⚠️'} {esc(i.message_tr)}" for i in issues]
    lines += [f"❔ {esc(a)}" for a in ambiguities if a and a.strip()]
    return lines


def expiry_minutes(conf: PendingConfirmation) -> int:
    seconds = (conf.expires_at - conf.created_at).total_seconds()
    return max(1, round(seconds / 60))


def format_confirmation(conf: PendingConfirmation, *, with_expiry: bool = True) -> str:
    """The 'I understood X, is that right?' message (``with_expiry=False`` once it is closed)."""
    action = conf.action
    parts = [HEADER_INCOMPLETE if conf.kind == "incomplete" else HEADER_UNSURE, ""]
    if isinstance(conf.plan, TradePlan):
        parts.append(format_plan(conf.plan))
    elif isinstance(conf.plan, ManagementPlan):
        parts.append(format_management(conf.plan, None))
    else:
        parts.append(format_action(action))
    parts.append("")
    if conf.market_price is not None:
        parts.append(f"💹 Güncel fiyat: <b>{fmt_price(conf.market_price)}</b>")
    parts.append(f"📊 Güven: {fmt_pct(max(0.0, min(1.0, action.confidence)) * 100)}")

    # The validator already turns parser ambiguities into issues; show each text once.
    extra = [a for a in action.ambiguities if a and a.strip() and not any(a.strip() in i.message_tr for i in conf.issues)]
    issue_lines = format_issues(conf.issues, extra)
    if issue_lines:
        parts.append("")
        parts.append("<b>Dikkat:</b>")
        parts += issue_lines
    if action.reasoning_brief and action.reasoning_brief.strip():
        parts.append(f"🔎 <i>Okunan: {esc(_truncate(action.reasoning_brief, 400))}</i>")

    ids = conf.source_message_ids or action.used_message_ids
    if ids:
        parts.append("📨 Kaynak mesaj: " + ", ".join(f"#{i}" for i in ids))
    if with_expiry:
        if conf.plan is None:
            parts.append("✏️ Onaylamak için önce <b>Düzelt</b> ile eksik bilgiyi yazın.")
        outcome = "bu değişiklik uygulanmaz" if conf.kind == "manage" else "işlem açılmaz"
        parts.append(f"⏳ {expiry_minutes(conf)} dk içinde yanıt gelmezse {outcome}.")
    return "\n".join(parts)
