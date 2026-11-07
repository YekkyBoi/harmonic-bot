"""Position sizing math. Pure functions, no I/O.

All step / tick arithmetic goes through ``Decimal`` built from the float's
shortest repr, so 0.1 + 0.2 floors to 0.3 and never to 0.2.
"""

from __future__ import annotations

import logging
import math
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal

from ..brokers.base import BrokerError
from ..models import SizingResult

logger = logging.getLogger(__name__)

# A quotient this close to an integer is treated as that integer (absorbs float noise
# like 2.9999999999999996 from risk / loss_per_unit). Relative to the quotient.
_STEP_EPS = Decimal("1e-9")
# Round-up to the minimum order is refused above this multiple of the target risk.
_MAX_ROUND_UP_RISK_MULT = 2.0


class SizingError(BrokerError):
    """Trade cannot be sized within the rules (too small, not enough margin...)."""


def _d(x: float) -> Decimal:
    return Decimal(str(x))


def _fmt(x: float) -> str:
    """Plain decimal text for Turkish messages: 0.0004, 2232.18, 20."""
    if not math.isfinite(x):
        return str(x)
    return format(Decimal(f"{x:.10g}").normalize(), "f")


def _quantize_steps(value: float, step: float, rounding: str) -> float:
    q = _d(value) / _d(step)
    nearest = q.to_integral_value(rounding=ROUND_HALF_UP)
    if abs(q - nearest) <= _STEP_EPS * max(Decimal(1), abs(q)):
        q = nearest
    else:
        q = q.to_integral_value(rounding=rounding)
    return float(q * _d(step))


def risk_amount(equity: float, risk_pct: float, multiplier: float = 1.0) -> float:
    """Money to lose at the stop: equity * risk_pct / 100 * multiplier."""
    return equity * risk_pct / 100.0 * multiplier


def floor_to_step(value: float, step: float) -> float:
    """Round DOWN to a multiple of ``step`` using Decimal (no float drift). step<=0 -> value."""
    if step <= 0 or not math.isfinite(value) or not math.isfinite(step):
        return value
    return _quantize_steps(value, step, ROUND_FLOOR)


def ceil_to_step(value: float, step: float) -> float:
    """Round UP to a multiple of ``step`` (Decimal-safe). step<=0 -> value."""
    if step <= 0 or not math.isfinite(value) or not math.isfinite(step):
        return value
    return _quantize_steps(value, step, ROUND_CEILING)


def round_to_tick(price: float, tick: float) -> float:
    """Round to the NEAREST multiple of ``tick`` (Decimal-safe). tick<=0 -> price."""
    if tick <= 0 or not math.isfinite(price) or not math.isfinite(tick):
        return price
    q = (_d(price) / _d(tick)).to_integral_value(rounding=ROUND_HALF_UP)
    return float(q * _d(tick))


def _require_positive(**values: float | None) -> None:
    for name, value in values.items():
        if value is None or not math.isfinite(value) or value <= 0:
            raise SizingError(f"Pozisyon hesaplanamadı: geçersiz değer ({name}={value}).")


def _require_non_negative(**values: float | None) -> None:
    for name, value in values.items():
        if value is not None and (not math.isfinite(value) or value < 0):
            raise SizingError(f"Pozisyon hesaplanamadı: geçersiz değer ({name}={value}).")


def compute_linear_size(
    *,
    risk_amount: float,
    entry: float,
    stop: float,
    contract_size: float = 1.0,
    qty_step: float,
    min_qty: float,
    max_qty: float | None = None,
    min_notional: float | None = None,
    fee_rate: float = 0.0,
    include_fees: bool = True,
    allow_min_qty_round_up: bool = False,
) -> SizingResult:
    """Size for linear instruments (USDT-margined perps, spot, paper).

    loss_per_unit = |entry - stop| * contract_size
                    + (fee_rate * (entry + stop) * contract_size if include_fees)
    quantity = floor_to_step(risk_amount / loss_per_unit, qty_step), capped at max_qty (warning).
    If quantity < min_qty or notional < min_notional: with allow_min_qty_round_up
    use the smallest valid size (warning, est_loss_at_sl shows the larger risk)
    but still raise if that would lose more than 2x risk_amount; otherwise raise
    SizingError with a Turkish message. Raises SizingError if entry == stop or
    any input is non-positive. leverage/margin_required are left None.

    ``min_qty`` / ``min_notional`` / ``fee_rate`` may be 0 (= no minimum / no fee);
    ``qty_step`` must be > 0.
    """
    _require_positive(
        risk_amount=risk_amount, entry=entry, stop=stop, contract_size=contract_size, qty_step=qty_step
    )
    _require_non_negative(min_qty=min_qty, min_notional=min_notional, fee_rate=fee_rate)
    if max_qty is not None:
        _require_positive(max_qty=max_qty)
    if entry == stop:
        raise SizingError("Pozisyon hesaplanamadı: giriş ve stop aynı fiyatta.")

    loss_per_unit = abs(entry - stop) * contract_size
    if include_fees:
        loss_per_unit += fee_rate * (entry + stop) * contract_size

    warnings: list[str] = []
    quantity = floor_to_step(risk_amount / loss_per_unit, qty_step)
    if max_qty is not None and quantity > max_qty:
        capped = floor_to_step(max_qty, qty_step)
        warnings.append(
            f"Miktar borsanın azami emir miktarıyla ({_fmt(max_qty)}) sınırlandı "
            f"(hesaplanan {_fmt(quantity)}); risk hedefin altında kalır."
        )
        quantity = capped

    unit_notional = contract_size * entry
    smallest = ceil_to_step(max(min_qty, qty_step), qty_step)
    if min_notional:
        smallest = max(smallest, ceil_to_step(min_notional / unit_notional, qty_step))

    below_qty = quantity <= 0 or quantity < min_qty
    below_notional = bool(min_notional) and quantity * unit_notional < min_notional * (1 - 1e-9)
    if below_qty or below_notional:
        if not allow_min_qty_round_up:
            if below_qty:
                raise SizingError(
                    f"Risk tutarına ({risk_amount:.2f}) göre hesaplanan miktar ({_fmt(quantity)}) "
                    f"borsanın minimum emir miktarının ({_fmt(max(min_qty, qty_step))}) altında."
                )
            raise SizingError(
                f"Risk tutarına ({risk_amount:.2f}) göre hesaplanan işlem büyüklüğü "
                f"({quantity * unit_notional:.2f}) borsanın minimum emir tutarının "
                f"({_fmt(min_notional or 0.0)}) altında."
            )
        if max_qty is not None and smallest > max_qty:
            raise SizingError(
                f"Minimum geçerli miktar ({_fmt(smallest)}) azami emir miktarını ({_fmt(max_qty)}) aşıyor."
            )
        rounded_loss = smallest * loss_per_unit
        if rounded_loss > _MAX_ROUND_UP_RISK_MULT * risk_amount:
            raise SizingError(
                f"Minimum emir miktarıyla ({_fmt(smallest)}) stopta kayıp {rounded_loss:.2f} olur; "
                f"bu, hedef riskin ({risk_amount:.2f}) 2 katından fazla."
            )
        warnings.append(
            f"Miktar minimum emir büyüklüğüne yükseltildi ({_fmt(quantity)} → {_fmt(smallest)}); "
            f"stopta kayıp ~{rounded_loss:.2f} (hedef {risk_amount:.2f})."
        )
        quantity = smallest

    return SizingResult(
        quantity=quantity,
        risk_amount=risk_amount,
        est_loss_at_sl=quantity * loss_per_unit,
        notional=quantity * unit_notional,
        leverage=None,
        margin_required=None,
        warnings=warnings,
    )


def compute_lot_size(
    *,
    risk_amount: float,
    loss_per_lot: float,
    lot_step: float,
    min_lot: float,
    max_lot: float | None = None,
    allow_min_qty_round_up: bool = False,
) -> float:
    """Lots for MT4/MT5-style instruments where the broker reports the money lost per 1 lot at the stop.

    Same rounding / minimum rules as compute_linear_size. Returns the lot size.
    ``loss_per_lot`` should already include commission if the caller wants fees in the risk.
    """
    _require_positive(risk_amount=risk_amount, loss_per_lot=loss_per_lot, lot_step=lot_step)
    _require_non_negative(min_lot=min_lot)
    if max_lot is not None:
        _require_positive(max_lot=max_lot)

    lots = floor_to_step(risk_amount / loss_per_lot, lot_step)
    if max_lot is not None and lots > max_lot:
        logger.warning("Lot size %s capped at broker max %s", lots, max_lot)
        lots = floor_to_step(max_lot, lot_step)

    if lots <= 0 or lots < min_lot:
        smallest = ceil_to_step(max(min_lot, lot_step), lot_step)
        if not allow_min_qty_round_up:
            raise SizingError(
                f"Risk tutarına ({risk_amount:.2f}) göre hesaplanan lot ({_fmt(lots)}) "
                f"brokerın minimum lotunun ({_fmt(max(min_lot, lot_step))}) altında."
            )
        if max_lot is not None and smallest > max_lot:
            raise SizingError(f"Minimum geçerli lot ({_fmt(smallest)}) azami lotu ({_fmt(max_lot)}) aşıyor.")
        rounded_loss = smallest * loss_per_lot
        if rounded_loss > _MAX_ROUND_UP_RISK_MULT * risk_amount:
            raise SizingError(
                f"Minimum lotla ({_fmt(smallest)}) stopta kayıp {rounded_loss:.2f} olur; "
                f"bu, hedef riskin ({risk_amount:.2f}) 2 katından fazla."
            )
        logger.warning(
            "Lot size %s raised to minimum %s (loss at SL %.2f vs target %.2f)",
            lots, smallest, rounded_loss, risk_amount,
        )
        lots = smallest
    return lots


def choose_leverage(
    *,
    notional: float,
    free_margin: float,
    stop_distance_frac: float,
    preferred: int,
    max_leverage: int,
    venue_max_leverage: float | None = None,
    max_margin_usage_pct: float = 90.0,
    maintenance_margin_rate: float = 0.005,
) -> tuple[int, float]:
    """Pick leverage for an isolated/cross futures position. Returns (leverage, margin_required).

    * Liquidation must stay beyond the stop with a safety buffer:
      leverage <= floor(1 / (stop_distance_frac * 1.5 + maintenance_margin_rate)).
    * Start from ``preferred`` (lowered to the liquidation-safe cap if needed).
    * If margin (notional / leverage) > free_margin * max_margin_usage_pct/100,
      raise leverage to the smallest integer that fits.
    * Never exceed min(max_leverage, venue_max_leverage, liquidation-safe cap);
      if it still does not fit raise SizingError (Turkish message).

    The caps are floored at 1x (a fully collateralized position).
    """
    _require_positive(notional=notional, stop_distance_frac=stop_distance_frac)
    _require_non_negative(maintenance_margin_rate=maintenance_margin_rate)
    if not math.isfinite(free_margin) or not math.isfinite(max_margin_usage_pct):
        raise SizingError("Kaldıraç seçilemedi: serbest bakiye bilinmiyor.")

    liq_cap = int((Decimal(1) / (_d(stop_distance_frac) * Decimal("1.5") + _d(maintenance_margin_rate)))
                  .to_integral_value(rounding=ROUND_FLOOR))
    caps = [max(1, liq_cap), max(1, int(max_leverage))]
    if venue_max_leverage is not None and math.isfinite(venue_max_leverage) and venue_max_leverage >= 1:
        caps.append(int(math.floor(venue_max_leverage)))
    cap = min(caps)

    leverage = min(max(1, int(preferred)), cap)
    allowed_margin = free_margin * max_margin_usage_pct / 100.0
    if allowed_margin <= 0:
        raise SizingError(f"Yetersiz teminat: kullanılabilir bakiye yok (serbest bakiye {free_margin:.2f}).")

    if notional / leverage > allowed_margin:
        needed = int((_d(notional) / _d(allowed_margin)).to_integral_value(rounding=ROUND_CEILING))
        if needed > cap:
            raise SizingError(
                f"Yetersiz teminat: {notional:.2f} büyüklüğündeki pozisyon için en az {needed}x kaldıraç "
                f"gerekiyor, izin verilen en yüksek kaldıraç {cap}x (serbest bakiye {free_margin:.2f})."
            )
        leverage = max(leverage, needed)
    return leverage, notional / leverage
