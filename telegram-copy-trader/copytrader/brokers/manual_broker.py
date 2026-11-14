"""Message-only "broker": the owner trades by hand.

For every trade routed here the owner gets a Telegram message with entry, stop, take profit and the
quantity (in bold) for a fixed money risk per R. Later instructions on the same trade (stop to
entry, close, cancel) are forwarded as messages too. The position is tracked virtually with real
prices (paper simulator), so a trade that already hit its stop or target is not messaged again.

The messages are handed to the owner's notifier WITHOUT waiting for Telegram: the notifier queues a message it
cannot deliver now and sends it later, in order. A silent Telegram link therefore never holds up this leg, and so
never the legs of the other brokers of the same trade (the engine waits for every leg of a trade)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..config import ManualBrokerConfig, RiskConfig
from ..models import (
    BrokerPosition,
    EntryType,
    OpenRequest,
    PositionStatus,
    Severity,
    Side,
    SizingResult,
    SymbolInfo,
    TakeProfit,
)
from ..risk.sizing import compute_linear_size, floor_to_step
from ..telegram.formatting import esc, fmt_price
from .base import BrokerError, contract_multiplier
from .paper_broker import PaperBroker

logger = logging.getLogger(__name__)

HANDOFF_WAIT_SEC = 25.0     # close(): bound for instructions still being handed to the notifier (it waits <= 2.5 s)


def _fmt_qty(qty: float) -> str:
    text = f"{qty:.8f}".rstrip("0").rstrip(".")
    return text or "0"


def _bundle(asset: str) -> tuple[float, str] | None:
    """A bundled contract name ("1000PEPE", "1000000MOG", "1MBABYDOGE"): (coins per unit, the single coin); None
    otherwise. The same prefixes as the rest of the bot (base.contract_multiplier)."""
    return contract_multiplier(asset)


# The Turkish possessive ending after a number, by how its last word is said: "%50'si" (elli), "%25'i" (beş),
# "%40'ı" (kırk), "%100'ü" (yüz).
_ONES_END = {1: "i", 2: "si", 3: "ü", 4: "ü", 5: "i", 6: "sı", 7: "si", 8: "i", 9: "u"}
_TENS_END = {1: "u", 2: "si", 3: "u", 4: "ı", 5: "si", 6: "ı", 7: "i", 8: "i", 9: "ı"}


def _pct_of(pct: float) -> str:
    """'%50'si' for 50.0; 'yaklaşık %49'u' when the share is not a whole number of percent."""
    n = int(round(pct))
    if n % 10:
        end = _ONES_END[n % 10]
    elif n % 100 == 0:
        end = "ü" if n else "ı"            # yüz / sıfır
    else:
        end = _TENS_END[(n // 10) % 10]
    return ("" if abs(pct - n) < 0.05 else "yaklaşık ") + f"%{n}'{end}"


class ManualBroker(PaperBroker):
    kind = "manual"

    def __init__(self, cfg: ManualBrokerConfig, data_dir: str, **kw: Any) -> None:
        super().__init__(cfg, data_dir, **kw)   # same fields as PaperBrokerConfig for prices + tracking
        self.cfg = cfg
        self._notifier: Any = None
        self._handoffs: set[asyncio.Task[None]] = set()     # messages being handed to the notifier

    def set_notifier(self, notifier: Any) -> None:
        self._notifier = notifier

    async def _send(self, text: str) -> bool:
        """Hand one instruction to the notifier and return at once: True when it was handed over, False when
        there is no notifier (then it is only logged). The notifier delivers it, or queues it and delivers it
        later, in the order the instructions were given (each hand-over starts before this returns)."""
        if self._notifier is None:
            logger.warning("%s: no notifier set; manual trade message not sent:\n%s", self.name, text)
            return False
        task = asyncio.ensure_future(self._hand_over(self._notifier, text))
        self._handoffs.add(task)
        task.add_done_callback(self._handoffs.discard)
        await asyncio.sleep(0)      # the hand-over runs up to its first wait now: the message is queued or on its way
        return True

    async def _hand_over(self, notifier: Any, text: str) -> None:
        try:
            await notifier.notify(text, Severity.INFO)
        except Exception:           # a notice must never break the broker; the notifier logs its own trouble
            logger.exception("%s: manual trade message could not be handed to the notifier:\n%s", self.name, text)

    async def close(self) -> None:
        # Instructions still being handed over finish first (the notifier waits at most CALLER_WAIT_SEC for
        # Telegram, then queues): the notifier is stopped only after the brokers, and flushes its queue then.
        pending = [t for t in self._handoffs if not t.done()]
        if pending:
            await asyncio.wait(pending, timeout=HANDOFF_WAIT_SEC)
        await super().close()

    # A fixed money amount per R instead of a % of equity: the engine computes
    # risk = equity * risk_pct / 100, so report the equity that makes 1R == risk_usd.
    async def get_equity(self) -> float:
        rc = getattr(self, "risk_config", None) or RiskConfig()
        return (self.cfg.risk_usd or 50.0) * 100.0 / rc.risk_per_r_pct

    async def get_free_margin(self) -> float:
        return await self.get_equity() * 1000.0     # never a margin limit for hand trades

    async def size_position(
        self, symbol: SymbolInfo, side: Side, entry_price: float, stop_loss: float, risk_amount: float
    ) -> SizingResult:
        if (side is Side.LONG and stop_loss >= entry_price) or (side is Side.SHORT and stop_loss <= entry_price):
            raise BrokerError(f"Stop {side.value} işlem için girişin yanlış tarafında.")
        result = compute_linear_size(
            risk_amount=risk_amount, entry=entry_price, stop=stop_loss, contract_size=symbol.contract_size,
            qty_step=symbol.qty_step, min_qty=0.0, max_qty=None, min_notional=None,
            fee_rate=0.0, include_fees=False, allow_min_qty_round_up=False,
            rounding=str(getattr(self.config, "lot_rounding", "down") or "down"),
        )
        return result

    def _coins(self, pos_or_req_qty: float, symbol_extra: dict[str, Any], contract_size: float) -> float:
        mult = float(symbol_extra.get("price_multiplier") or 1.0)
        coins = pos_or_req_qty * contract_size * mult
        if self.cfg.qty_decimals is not None:
            coins = floor_to_step(coins, 10.0 ** -self.cfg.qty_decimals)
        return coins

    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        pos = await super().open_position(req)
        plan = req.plan
        coins = self._coins(req.quantity, req.symbol.extra, req.symbol.contract_size)
        pos.data["manual_asset"] = plan.base_asset
        pos.data["manual_qty"] = coins
        price = pos.avg_entry_price or req.entry_price or 0.0      # venue units, like req.stop_loss
        risk = req.quantity * abs(price - req.stop_loss) * req.symbol.contract_size
        # The value of the position in USD next to its quantity: a platform whose amount field takes USD gets the
        # right size too (2026-11-12: "11627 KONTRAT" typed as 11627 USD bought 67716 ADA, 5.8x the plan).
        notional = req.quantity * req.symbol.contract_size * price
        side = "LONG 📈" if plan.side is Side.LONG else "SHORT 📉"
        if req.entry_type is EntryType.LIMIT:
            entry = f"{fmt_price(plan.entry_price)} (LİMİT emir)"
        else:
            entry = f"PİYASA (sinyal girişi {fmt_price(plan.entry_price)})" if plan.entry_price else "PİYASA"
        tps = " / ".join(fmt_price(tp.price) for tp in plan.take_profits) or "yok"
        qty = f"{_fmt_qty(coins)} {esc(plan.base_asset)}"
        amount = f"Miktar: <b>{qty}</b> (adet)"
        bundle = _bundle(plan.base_asset)
        if bundle is not None:      # "1000PEPE": a platform may count single PEPE; give that count as well
            factor, single = bundle
            amount += f" = {_fmt_qty(coins * factor)} {esc(single)}"
        if notional > 0:
            amount += (f" ≈ <b>{notional:.2f} USD</b> tutarında; tutar USD olarak giriliyorsa "
                       f"{notional:.2f} USD girin")
        text = (
            f"<b>🔢 {qty} — {esc(plan.base_asset)} {side}</b>\n"
            f"✋ Elle işlem ({esc(self.name)})\n"
            f"{amount}\n"
            f"Giriş: {entry}\n"
            f"Stop: {fmt_price(plan.stop_loss)}\n"
            f"TP: {tps}\n"
            f"Risk: ~{risk:.2f} USD ({plan.risk_r:g}R, 1R = {(self.cfg.risk_usd or 50.0):g} USD)"
        )
        if not await self._send(text):
            pos.data.setdefault("warnings", []).append(
                "Elle işlem talimatı gönderilemedi (bildirim botu bağlı değil); bu işlemi kendiniz açmanız gerekir.")
        return pos

    def _label(self, pos: BrokerPosition) -> str:
        return esc(str(pos.data.get("manual_asset") or pos.broker_symbol.split("/")[0]))

    def _amount(self, pos: BrokerPosition, qty: float) -> str:
        """``qty`` (the record's units) in the units of the open instruction: coins of the trade's asset, with the
        single-coin count of a bundled contract ("120 1000PEPE = 120000 PEPE")."""
        info = pos.data.get("symbol_info") or {}
        coins = self._coins(qty, info.get("extra") or {}, float(info.get("contract_size") or 1.0))
        asset = str(pos.data.get("manual_asset") or pos.broker_symbol.split("/")[0])
        text = f"{_fmt_qty(coins)} {esc(asset)}"
        bundle = _bundle(asset)
        if bundle is not None:
            factor, single = bundle
            text += f" = {_fmt_qty(coins * factor)} {esc(single)}"
        return text

    def _usd(self, pos: BrokerPosition, qty: float, price: float | None) -> float | None:
        """The USD value of ``qty`` (the record's units) at ``price`` (the venue's price), as the open instruction
        works it out (quantity x contract size x price); None when no price is known."""
        if price is None or not price > 0:
            return None
        info = pos.data.get("symbol_info") or {}
        return qty * float(info.get("contract_size") or 1.0) * price

    @staticmethod
    def _exit_price(before: BrokerPosition, after: BrokerPosition) -> float | None:
        """The price at which the record closed in this call (the current quote), None when it closed nothing."""
        exits = list(after.data.get("exits") or [])[len(before.data.get("exits") or []):]
        try:
            return float(exits[-1]["price"]) if exits else None
        except (KeyError, TypeError, ValueError):
            return None

    def _close_text(self, before: BrokerPosition, after: BrokerPosition) -> str:
        """The close instruction: the amount to close and what must stay open, in the units of the open instruction
        (the owner types it by hand; "%25'ini" of an earlier size was read as a share of what is open), each with its
        USD value at the current price, as the open instruction gives it: a platform whose amount field takes USD
        gets the right amount too (2026-11-12: "11627 KONTRAT" typed as 11627 USD). No percent: the amounts are the
        instruction (a percent of the rounded amount, "yaklaşık %49'u", differed from the bot's own "%50")."""
        label, tail = self._label(before), f"\n✋ Elle işlem ({esc(self.name)})"
        if before.status is PositionStatus.PENDING:         # (the record cancelled the unfilled entry)
            return f"<b>🚫 {label}: BEKLEYEN LİMİT EMRİ İPTAL ET</b>" + tail
        held = before.remaining_quantity
        left = after.remaining_quantity if after.status is PositionStatus.OPEN else 0.0
        if held <= 0:
            return f"<b>❌ {label}: POZİSYONUN TAMAMINI KAPAT</b>" + tail
        price = self._exit_price(before, after)
        no_price = "şu anki fiyat alınamadı, USD karşılığı verilemiyor"
        if left <= 0:
            usd = self._usd(before, held, price)
            worth = f"≈ {usd:.2f} USD, şu anki fiyatla" if usd is not None else no_price
            return f"<b>❌ {label}: POZİSYONUN TAMAMINI KAPAT ({self._amount(before, held)})</b> ({worth})" + tail
        closed = max(0.0, held - left)
        usd_closed, usd_left = self._usd(before, closed, price), self._usd(before, left, price)
        if usd_closed is None or usd_left is None:
            return (f"<b>❌ {label}: {self._amount(before, closed)} KAPAT</b> ({no_price}; kapattıktan sonra "
                    f"{self._amount(before, left)} açık kalmalı)" + tail)
        return (f"<b>❌ {label}: {self._amount(before, closed)} KAPAT</b> (≈ <b>{usd_closed:.2f} USD</b> tutarında, şu "
                f"anki fiyatla; tutar USD olarak giriliyorsa {usd_closed:.2f} USD girin; kapattıktan sonra "
                f"{self._amount(before, left)} ≈ {usd_left:.2f} USD açık kalmalı)" + tail)

    async def update_stop_loss(self, pos: BrokerPosition, new_stop_loss: float) -> BrokerPosition:
        new = await super().update_stop_loss(pos, new_stop_loss)
        await self._send(f"<b>✏️ {self._label(pos)}: STOPU {fmt_price(new_stop_loss)} YAP</b>\n✋ Elle işlem ({esc(self.name)})")
        return new

    async def update_take_profits(self, pos: BrokerPosition, take_profits: list[TakeProfit]) -> BrokerPosition:
        new = await super().update_take_profits(pos, take_profits)
        tps = " / ".join(fmt_price(tp.price) for tp in take_profits)
        await self._send(f"<b>🎯 {self._label(pos)}: TP'Yİ {tps} YAP</b>\n✋ Elle işlem ({esc(self.name)})")
        return new

    async def close_position(self, pos: BrokerPosition, fraction: float = 1.0) -> BrokerPosition:
        new = await super().close_position(pos, fraction)
        await self._send(self._close_text(pos, new))
        return new

    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        new = await super().cancel_entry(pos)
        if pos.status is PositionStatus.OPEN:   # the entry had filled: the record closed the position
            await self._send(self._close_text(pos, new))
            return new
        await self._send(f"<b>🚫 {self._label(pos)}: BEKLEYEN LİMİT EMRİ İPTAL ET</b>\n✋ Elle işlem ({esc(self.name)})")
        return new


__all__ = ["ManualBroker"]
