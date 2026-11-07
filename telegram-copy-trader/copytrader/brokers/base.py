"""Broker adapter contract.

One adapter instance = one account (one exchange account, one MT5 login...).
All methods are async. Adapters must be safe to call concurrently for
DIFFERENT trades; the engine never calls two write methods for the SAME
position at the same time.

Money-safety rules every adapter must follow
---------------------------------------------
1. Never leave a filled position without a stop loss. If the protective stop
   cannot be placed after an entry fills (after a retry), close the position
   at market and return status FAILED with ``error`` explaining why.
2. Quantities are rounded DOWN to the instrument's step (never risk more
   than requested). Prices are rounded to the instrument's tick.
3. Stop loss and take profit orders must be reduce-only / attached to the
   position so they can never open a new position in the other direction.
4. Use ``OpenRequest.client_tag`` to tag orders (client order id / comment)
   so that a retried request can be detected instead of doubling a position.
5. Raise ``BrokerError`` (with a short, human readable message) for any
   failure the engine should report; never swallow errors silently.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ..models import (
    BrokerPosition,
    MarketType,
    OpenRequest,
    Quote,
    Side,
    SizingResult,
    SymbolInfo,
    TakeProfit,
)


class BrokerError(Exception):
    """A broker operation failed. ``str(err)`` is shown to the owner."""


class Broker(ABC):
    #: "ccxt" | "mt5" | "metaapi" | "paper"
    kind: str = "base"

    def __init__(self, config: Any) -> None:
        self.config = config
        self.name: str = config.name

    # ---- lifecycle -------------------------------------------------------
    @abstractmethod
    async def connect(self) -> None:
        """Log in / load markets. Called once at startup."""

    @abstractmethod
    async def close(self) -> None:
        """Release connections."""

    # ---- account ---------------------------------------------------------
    @abstractmethod
    async def get_equity(self) -> float:
        """Account value used for sizing, in ``account_currency()``.

        Equity (incl. unrealized PnL) or free balance depending on
        ``RiskConfig.use_equity`` (passed to the adapter by the engine via
        ``set_risk_config``)."""

    @abstractmethod
    async def get_free_margin(self) -> float:
        """Balance available for new margin, in ``account_currency()``."""

    @abstractmethod
    async def account_currency(self) -> str:
        ...

    # ---- market data -----------------------------------------------------
    @abstractmethod
    async def resolve_symbol(
        self,
        base_asset: str,
        quote_asset: str | None = None,
        market_type: MarketType | None = None,
        raw_symbol: str | None = None,
    ) -> SymbolInfo | None:
        """Map a signal's asset to this broker's instrument, or None if not tradable here.

        ``config.symbol_map`` wins (keys: base asset like "ETH" or raw symbol
        like "ETHUSDT.P", case-insensitive)."""

    @abstractmethod
    async def get_quote(self, symbol: SymbolInfo) -> Quote:
        ...

    # ---- sizing ------------------------------------------------------------
    @abstractmethod
    async def size_position(
        self,
        symbol: SymbolInfo,
        side: Side,
        entry_price: float,
        stop_loss: float,
        risk_amount: float,
    ) -> SizingResult:
        """Quantity such that hitting ``stop_loss`` from ``entry_price`` loses ~``risk_amount``.

        Must account for contract size / tick value and (if configured) fees,
        pick leverage for futures (liquidation must be beyond the stop), and
        raise ``BrokerError`` if the resulting size is below the minimum
        order size (unless ``allow_min_qty_round_up``) or margin is insufficient."""

    # ---- trading -----------------------------------------------------------
    @abstractmethod
    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        """Place the entry and its protective SL + TP orders.

        MARKET: returns status OPEN with SL/TP live.
        LIMIT: returns status PENDING; SL/TP are attached to the entry when the
        venue supports it, otherwise ``sync_position`` places them after fill."""

    @abstractmethod
    async def update_stop_loss(self, pos: BrokerPosition, new_stop_loss: float) -> BrokerPosition:
        ...

    @abstractmethod
    async def update_take_profits(self, pos: BrokerPosition, take_profits: list[TakeProfit]) -> BrokerPosition:
        ...

    @abstractmethod
    async def close_position(self, pos: BrokerPosition, fraction: float = 1.0) -> BrokerPosition:
        """Close ``fraction`` of the ORIGINAL filled size (capped at what is still open).

        Closing 100% also cancels the remaining SL/TP orders."""

    @abstractmethod
    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        """Cancel an unfilled entry (status PENDING -> CANCELLED) and its attached orders.

        If the entry already (partially) filled, behave like ``close_position(pos, 1.0)``."""

    @abstractmethod
    async def sync_position(self, pos: BrokerPosition) -> BrokerPosition:
        """Refresh state from the venue. Called periodically by the monitor.

        Responsibilities: detect limit fills (and place SL/TP if they were not
        attached), detect SL/TP hits and partial TP fills, cancel orphaned
        reduce-only orders once the position is flat (OCO emulation), fill in
        ``realized_pnl`` when known, and mark CLOSED/CANCELLED."""

    # ---- optional ------------------------------------------------------------
    def set_risk_config(self, risk_config: Any) -> None:
        """Engine hands the global RiskConfig to adapters at startup."""
        self.risk_config = risk_config

    def accepts_asset(self, base_asset: str) -> bool:
        allowed = [a.upper() for a in getattr(self.config, "allowed_assets", [])]
        return not allowed or base_asset.upper() in allowed
