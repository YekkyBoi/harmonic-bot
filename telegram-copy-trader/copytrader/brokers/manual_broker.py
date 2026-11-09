"""Message-only "broker": the owner trades by hand.

For every trade routed here the owner gets a Telegram message with entry, stop, take profit and the
quantity (in bold) for a fixed money risk per R. Later instructions on the same trade (stop to
entry, close, cancel) are forwarded as messages too. The position is tracked virtually with real
prices (paper simulator), so a trade that already hit its stop or target is not messaged again."""

from __future__ import annotations

import logging
from typing import Any

from ..config import ManualBrokerConfig, RiskConfig
from ..models import BrokerPosition, EntryType, OpenRequest, Severity, Side, SizingResult, SymbolInfo, TakeProfit
from ..risk.sizing import compute_linear_size, floor_to_step
from ..telegram.formatting import esc, fmt_price
from .base import BrokerError
from .paper_broker import PaperBroker

logger = logging.getLogger(__name__)


def _fmt_qty(qty: float) -> str:
    text = f"{qty:.8f}".rstrip("0").rstrip(".")
    return text or "0"


class ManualBroker(PaperBroker):
    kind = "manual"

    def __init__(self, cfg: ManualBrokerConfig, data_dir: str, **kw: Any) -> None:
        super().__init__(cfg, data_dir, **kw)   # same fields as PaperBrokerConfig for prices + tracking
        self.cfg = cfg
        self._notifier: Any = None

    def set_notifier(self, notifier: Any) -> None:
        self._notifier = notifier

    async def _send(self, text: str) -> None:
        if self._notifier is None:
            logger.warning("%s: no notifier set; manual trade message not sent:\n%s", self.name, text)
            return
        await self._notifier.notify(text, Severity.INFO)

    # A fixed money amount per R instead of a % of equity: the engine computes
    # risk = equity * risk_pct / 100, so report the equity that makes 1R == risk_usd.
    async def get_equity(self) -> float:
        rc = getattr(self, "risk_config", None) or RiskConfig()
        return self.cfg.risk_usd * 100.0 / rc.risk_per_r_pct

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
        risk = req.quantity * abs((pos.avg_entry_price or req.entry_price or 0.0) - req.stop_loss) \
            * req.symbol.contract_size
        side = "LONG 📈" if plan.side is Side.LONG else "SHORT 📉"
        if req.entry_type is EntryType.LIMIT:
            entry = f"{fmt_price(plan.entry_price)} (LİMİT emir)"
        else:
            entry = f"PİYASA (sinyal girişi {fmt_price(plan.entry_price)})" if plan.entry_price else "PİYASA"
        tps = " / ".join(fmt_price(tp.price) for tp in plan.take_profits) or "yok"
        text = (
            f"<b>🔢 {_fmt_qty(coins)} KONTRAT — {esc(plan.base_asset)} {side}</b>\n"
            f"✋ Elle işlem ({esc(self.name)})\n"
            f"Giriş: {entry}\n"
            f"Stop: {fmt_price(plan.stop_loss)}\n"
            f"TP: {tps}\n"
            f"Risk: ~{risk:.2f} USD ({plan.risk_r:g}R, 1R = {self.cfg.risk_usd:g} USD)"
        )
        await self._send(text)
        return pos

    def _label(self, pos: BrokerPosition) -> str:
        return esc(str(pos.data.get("manual_asset") or pos.broker_symbol.split("/")[0]))

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
        part = "TAMAMINI" if fraction >= 0.999 else f"%{fraction * 100:.0f}'ini"
        await self._send(f"<b>❌ {self._label(pos)}: POZİSYONUN {part} KAPAT</b>\n✋ Elle işlem ({esc(self.name)})")
        return new

    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        new = await super().cancel_entry(pos)
        await self._send(f"<b>🚫 {self._label(pos)}: BEKLEYEN LİMİT EMRİ İPTAL ET</b>\n✋ Elle işlem ({esc(self.name)})")
        return new


__all__ = ["ManualBroker"]
