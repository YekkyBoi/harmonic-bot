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

import re
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


class BrokerUnavailableError(BrokerError):
    """The adapter refused a call itself, BEFORE sending any request of it, because the account is not connected or
    its link is known to be down (MetaApi: not connected, rebuilding its connection, or health checks found the link
    dead). Never raised once a request of that call went out. The engine may offer the owner to open a signal that
    was skipped this way on that account once it is healthy again (Engine catch-up)."""


class OrderOutcomeUnknownError(BrokerError):
    """An order request was SENT and its answer was lost (no answer in time, the link broke under it): the order may
    or may not exist at the broker. It is never sent again; the owner must be told that its outcome is unknown, never
    that it "could not be opened"."""


# The words with which the MetaTrader adapters report, inside a plain BrokerError, an order leg whose answer was lost
# after looking it up by its tag ("Emir durumu belirsiz: ..." / "2/3. bacak durumu belirsiz: ...":
# MetaTraderLegBroker._open_position, MetaApiBroker._unsure_not_failed). tests/test_review2_repair.py pins that both
# adapters still say so.
OUTCOME_UNKNOWN_MARK = "durumu belirsiz:"


def outcome_unknown(exc: BaseException | None) -> bool:
    """True when ``exc`` (the failure of a broker call that sent an order) says that the order request went out and
    its answer was lost: an :class:`OrderOutcomeUnknownError`, raised directly or wrapped (its ``__cause__`` /
    ``__context__``), or a BrokerError carrying OUTCOME_UNKNOWN_MARK."""
    seen: set[int] = set()
    cur = exc
    while cur is not None and id(cur) not in seen:
        if isinstance(cur, OrderOutcomeUnknownError):
            return True
        seen.add(id(cur))
        cur = cur.__cause__ or cur.__context__
    return isinstance(exc, BrokerError) and OUTCOME_UNKNOWN_MARK in str(exc)


# Contract multiplier an exchange puts before a low-priced coin: "1000PEPE" = 1000 PEPE, "1MBABYDOGE" =
# 1000000 BABYDOGE. The same prefixes as ccxt_broker._PREFIX_RE plus the "1M" spelling, and only before a
# letter, so a coin whose real name starts with a digit ("1INCH") is left alone.
_MULTIPLIER_PREFIX_RE = re.compile(r"^(?:1000000|100000|10000|1000|1M)(?=[A-Z])")


def _asset_key(asset: str) -> str:
    """Upper-case asset name without a contract multiplier prefix: "1000pepe" -> "PEPE", "1INCH" -> "1INCH"."""
    return _MULTIPLIER_PREFIX_RE.sub("", asset.upper(), count=1)


def contract_multiplier(asset: str) -> tuple[float, str] | None:
    """(coins per contract unit, the single coin) for a bundled contract name, by the same prefixes as
    :func:`_asset_key`: "1000PEPE" -> (1000.0, "PEPE"), "1MBABYDOGE" -> (1000000.0, "BABYDOGE"). None for any
    other name ("1INCH", "2Z", "ADA")."""
    name = (asset or "").upper()
    found = _MULTIPLIER_PREFIX_RE.match(name)
    if found is None:
        return None
    prefix = found.group(0)
    return (1_000_000.0 if prefix == "1M" else float(prefix)), name[len(prefix):]


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
        """``config.allowed_assets`` (empty = everything). "PEPE" and the "1000PEPE" contract are the same asset."""
        allowed = {_asset_key(a) for a in getattr(self.config, "allowed_assets", [])}
        return not allowed or _asset_key(base_asset) in allowed
