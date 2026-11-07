"""Dry-run wrapper: reads come from the real adapter, writes are simulated.

Equity, quotes, symbol resolution and sizing use the real account, so the dry run shows
exactly what would be traded. Orders never reach the venue: open / SL / TP / close /
cancel / sync go through the paper simulator, priced with ``inner.get_quote`` (and the
inner adapter's 1m candles when it offers ``fetch_ohlcv_1m``).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..config import RiskConfig
from ..models import BrokerPosition, MarketType, OpenRequest, Quote, Side, SizingResult, SymbolInfo, TakeProfit
from .base import Broker, BrokerError
from .ccxt_broker import num
from .paper_broker import PaperSimulator, safe_file_part

logger = logging.getLogger(__name__)


class DryRunBroker(Broker):
    def __init__(self, inner: Broker, data_dir: str, *, clock_ms: Callable[[], int] | None = None) -> None:
        super().__init__(inner.config)
        self.inner = inner
        self.kind = f"dry-run:{inner.kind}"
        self.risk_config: RiskConfig = getattr(inner, "risk_config", None) or RiskConfig()
        self.sim = PaperSimulator(
            name=inner.name,
            state_path=Path(data_dir) / f"dryrun_{safe_file_part(inner.name)}.json",
            starting_balance=0.0,              # equity comes from the real account; only PnL is tracked
            quote_fn=inner.get_quote,
            fee_rate_fn=self._fee_rate,
            candles_fn=getattr(inner, "fetch_ohlcv_1m", None),
            dry_run=True,
            clock_ms=clock_ms,
        )

    def _fee_rate(self, info: SymbolInfo) -> float:
        fee = num(info.extra.get("taker_fee"))
        return fee if fee is not None and fee >= 0 else self.risk_config.fee_rate

    # ---- lifecycle (the real adapter must be connected for reads) -------------
    async def connect(self) -> None:
        await self.inner.connect()
        self.sim.load()
        logger.info("%s: DRY RUN - orders are simulated, nothing is sent to %s", self.name, self.inner.kind)

    async def close(self) -> None:
        await self.inner.close()

    def set_risk_config(self, risk_config: Any) -> None:
        self.risk_config = risk_config
        self.inner.set_risk_config(risk_config)

    def accepts_asset(self, base_asset: str) -> bool:
        return self.inner.accepts_asset(base_asset)

    # ---- reads: real ------------------------------------------------------------
    async def get_equity(self) -> float:
        return await self.inner.get_equity()

    async def get_free_margin(self) -> float:
        return await self.inner.get_free_margin()

    async def account_currency(self) -> str:
        return await self.inner.account_currency()

    async def resolve_symbol(
        self,
        base_asset: str,
        quote_asset: str | None = None,
        market_type: MarketType | None = None,
        raw_symbol: str | None = None,
    ) -> SymbolInfo | None:
        return await self.inner.resolve_symbol(base_asset, quote_asset, market_type, raw_symbol)

    async def get_quote(self, symbol: SymbolInfo) -> Quote:
        return await self.inner.get_quote(symbol)

    async def size_position(
        self, symbol: SymbolInfo, side: Side, entry_price: float, stop_loss: float, risk_amount: float
    ) -> SizingResult:
        return await self.inner.size_position(symbol, side, entry_price, stop_loss, risk_amount)

    # ---- writes: simulated ----------------------------------------------------------
    def _require_simulated(self, pos: BrokerPosition) -> None:
        if not pos.data.get("simulated"):
            raise BrokerError(
                f"{self.name}: bu pozisyon gerçek hesapta açılmış; dry-run modunda yönetilemez (borsadan yönetin)."
            )

    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        pos = await self.sim.open(req)
        logger.info("%s: [dry-run] %s %s %s qty=%s", self.name, req.entry_type.value, req.plan.side.value,
                    req.symbol.broker_symbol, pos.quantity)
        return pos

    async def update_stop_loss(self, pos: BrokerPosition, new_stop_loss: float) -> BrokerPosition:
        self._require_simulated(pos)
        return await self.sim.update_stop_loss(pos, new_stop_loss)

    async def update_take_profits(self, pos: BrokerPosition, take_profits: list[TakeProfit]) -> BrokerPosition:
        self._require_simulated(pos)
        return await self.sim.update_take_profits(pos, take_profits)

    async def close_position(self, pos: BrokerPosition, fraction: float = 1.0) -> BrokerPosition:
        self._require_simulated(pos)
        return await self.sim.close(pos, fraction)

    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        self._require_simulated(pos)
        return await self.sim.cancel_entry(pos)

    async def sync_position(self, pos: BrokerPosition) -> BrokerPosition:
        if not pos.data.get("simulated"):
            logger.warning("%s: real position %s not synced in dry-run mode", self.name, pos.broker_symbol)
            return pos
        return await self.sim.sync(pos)
