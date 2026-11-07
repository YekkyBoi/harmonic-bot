"""Shared data models.

Every module talks to every other module through these types. Keep them
stable: the parser, validator, brokers, storage, Telegram layer and engine
all depend on them.

Conventions
-----------
* Prices are plain floats in the instrument's quote currency.
* "R" is the risk unit. 1R = ``risk.risk_per_r_pct`` percent of account
  equity (default 2%). A "1R long" loses exactly 1R if the stop loss is hit.
* ``TakeProfit.close_fraction`` is a fraction of the ORIGINAL position size.
  The fractions of one trade's take profits must add up to 1.0.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class Side(str, Enum):
    LONG = "long"
    SHORT = "short"

    @property
    def opposite(self) -> "Side":
        return Side.SHORT if self is Side.LONG else Side.LONG


class ActionType(str, Enum):
    OPEN = "open"              # open a new trade
    MOVE_SL = "move_sl"        # move stop loss (incl. "SL to breakeven")
    MOVE_TP = "move_tp"        # replace take profit levels
    CLOSE = "close"            # close a running trade fully or partially
    CANCEL = "cancel"          # cancel a not-yet-filled (limit) entry / call off a signal
    INCOMPLETE = "incomplete"  # looks like a trade being posted but info is still missing
    NONE = "none"              # chatter, analysis, results, anything that is not an instruction


class EntryType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class MarketType(str, Enum):
    PERP = "perp"      # crypto perpetual futures (e.g. ETHUSDT.P)
    SPOT = "spot"
    FOREX = "forex"
    CFD = "cfd"        # indices, metals, stocks via CFD brokers


class TradeStatus(str, Enum):
    OPENING = "opening"        # being sent to brokers
    PENDING = "pending"        # limit entry placed, not filled anywhere yet
    OPEN = "open"              # filled on at least one broker
    CLOSED = "closed"
    CANCELLED = "cancelled"
    FAILED = "failed"          # failed on every broker


class PositionStatus(str, Enum):
    PENDING = "pending"        # entry order resting, not filled
    OPEN = "open"
    CLOSED = "closed"
    CANCELLED = "cancelled"
    FAILED = "failed"


class ConfirmationStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    SUPERSEDED = "superseded"  # replaced by a corrected / re-parsed version


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


# --------------------------------------------------------------------------- #
# Telegram messages
# --------------------------------------------------------------------------- #
class StoredMessage(BaseModel):
    """A message from a watched source chat, as persisted in the database."""

    chat_id: int
    message_id: int
    sender_id: int | None = None
    sender_name: str | None = None
    date: datetime
    text: str = ""
    media_path: str | None = None       # local path of a downloaded image, if any
    media_type: str | None = None       # "photo" | "image_document" | None
    reply_to_message_id: int | None = None
    grouped_id: int | None = None       # Telegram album id
    edited: bool = False
    consumed: bool = False              # already turned into an action / decision


# --------------------------------------------------------------------------- #
# Parsing (LLM output)
# --------------------------------------------------------------------------- #
class TakeProfit(BaseModel):
    price: float
    close_fraction: float = 1.0


class ParsedAction(BaseModel):
    """One trading instruction understood from one or more Telegram messages."""

    action: ActionType
    symbol: str | None = None             # as written: "ETHUSDT.P", "ETH", "XAUUSD", "BTC/USDT"
    base_asset: str | None = None         # normalized base: "ETH", "BTC", "XAU", "EUR"
    quote_asset: str | None = None        # "USDT", "USD", ... None if not stated
    market_type: MarketType | None = None
    side: Side | None = None
    entry_type: EntryType | None = None   # None = not stated (engine decides via risk.entry_mode)
    entry_price: float | None = None
    stop_loss: float | None = None
    take_profits: list[TakeProfit] = Field(default_factory=list)
    risk_r: float | None = None           # "1R" -> 1.0, "0.5R" -> 0.5, None = not stated
    new_stop_loss: float | None = None    # MOVE_SL: explicit new stop price
    move_sl_to_entry: bool = False        # MOVE_SL: "SL to BE / girişe çek"
    close_fraction: float | None = None   # CLOSE: fraction of what is STILL OPEN (1.0 = all, 0.5 = half of the rest).
                                          # The engine converts it; Broker.close_position takes a fraction of the original fill.
    refers_to_message_id: int | None = None  # management: the original signal message, if known
    target_trade_id: int | None = None    # management: id of the open trade (from ParseContext.open_trades), if clear
    used_message_ids: list[int] = Field(default_factory=list)  # messages this action was built from
    confidence: float = 0.0               # 0..1, parser's own confidence
    ambiguities: list[str] = Field(default_factory=list)  # Turkish, one short sentence each
    summary_tr: str = ""                  # one-line Turkish summary of what was understood
    reasoning_brief: str = ""             # where each number came from (image label / text)


class ParseResult(BaseModel):
    actions: list[ParsedAction] = Field(default_factory=list)
    model: str | None = None
    raw_response: str | None = None       # for audit / debugging


class ParseContext(BaseModel):
    """Extra context handed to the parser alongside the messages."""

    now: datetime
    open_trades: list[dict[str, Any]] = Field(default_factory=list)  # compact summaries (see TradeRecord.summary())
    reply_to: StoredMessage | None = None
    correction_text: str | None = None    # user's correction from the confirmation dialog
    previous_action: ParsedAction | None = None  # what we understood before the correction


# --------------------------------------------------------------------------- #
# Validation / plans
# --------------------------------------------------------------------------- #
class ValidationIssue(BaseModel):
    code: str                 # machine code, e.g. "missing_stop_loss", "sl_wrong_side"
    message_tr: str           # Turkish explanation shown to the user
    blocking: bool = False    # True: cannot execute as-is, user must correct (e.g. no stop loss)


class TradePlan(BaseModel):
    """A fully specified NEW trade, ready to be executed on every broker."""

    plan_id: str
    base_asset: str
    quote_asset: str | None = None
    market_type: MarketType | None = None
    side: Side
    entry_type: EntryType
    entry_price: float | None = None      # signal entry (limit price for LIMIT, reference for MARKET)
    stop_loss: float
    take_profits: list[TakeProfit] = Field(default_factory=list)
    risk_r: float
    risk_pct: float                       # percent of equity lost at SL (= risk_r * risk_per_r_pct)
    source_chat_id: int
    source_message_ids: list[int] = Field(default_factory=list)
    summary_tr: str = ""
    created_at: datetime


class ManagementPlan(BaseModel):
    """An instruction for an EXISTING trade."""

    plan_id: str
    action: ActionType                    # MOVE_SL | MOVE_TP | CLOSE | CANCEL
    trade_id: int
    new_stop_loss: float | None = None
    move_sl_to_entry: bool = False
    take_profits: list[TakeProfit] = Field(default_factory=list)
    close_fraction: float | None = None
    source_chat_id: int
    source_message_ids: list[int] = Field(default_factory=list)
    summary_tr: str = ""
    created_at: datetime


class PendingConfirmation(BaseModel):
    """Something the bot was unsure about and asked the owner to confirm."""

    id: str                               # short id used in button callback data
    kind: Literal["open", "manage", "incomplete"]
    action: ParsedAction                  # what was understood
    plan: TradePlan | ManagementPlan | None = None  # executable on approval; None => needs correction
    issues: list[ValidationIssue] = Field(default_factory=list)
    market_price: float | None = None
    source_chat_id: int
    source_message_ids: list[int] = Field(default_factory=list)
    created_at: datetime
    expires_at: datetime
    status: ConfirmationStatus = ConfirmationStatus.PENDING
    bot_message_id: int | None = None     # id of the bot message carrying the buttons
    awaiting_correction: bool = False     # owner pressed "Düzelt" and we wait for their text


# --------------------------------------------------------------------------- #
# Trades & broker state
# --------------------------------------------------------------------------- #
class TradeRecord(BaseModel):
    id: int
    plan: TradePlan
    status: TradeStatus
    current_stop_loss: float
    current_take_profits: list[TakeProfit] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None = None
    note: str | None = None

    def summary(self) -> dict[str, Any]:
        """Compact dict for the LLM parser context and status messages."""
        return {
            "trade_id": self.id,
            "asset": self.plan.base_asset,
            "side": self.plan.side.value,
            "entry": self.plan.entry_price,
            "stop_loss": self.current_stop_loss,
            "take_profits": [tp.price for tp in self.current_take_profits],
            "status": self.status.value,
            "source_message_ids": self.plan.source_message_ids,
            "opened_at": self.created_at.isoformat(),
        }


class Quote(BaseModel):
    bid: float
    ask: float
    last: float

    def entry_price_for(self, side: Side) -> float:
        """Price a market order would roughly fill at when OPENING ``side``."""
        return self.ask if side is Side.LONG else self.bid

    def exit_price_for(self, side: Side) -> float:
        """Price a market order would roughly fill at when CLOSING a ``side`` position."""
        return self.bid if side is Side.LONG else self.ask


class SymbolInfo(BaseModel):
    broker_symbol: str                    # symbol as the broker/exchange wants it ("ETH/USDT:USDT", "ETHUSD")
    base_asset: str
    quote_asset: str
    qty_step: float                       # quantity increment (coins / contracts / lots)
    min_qty: float
    max_qty: float | None = None
    min_notional: float | None = None
    price_tick: float
    contract_size: float = 1.0            # units of base per 1 qty (ccxt contractSize, MT5 trade_contract_size)
    max_leverage: float | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class SizingResult(BaseModel):
    quantity: float                       # broker units, already rounded DOWN to qty_step
    risk_amount: float                    # target money at risk (account currency)
    est_loss_at_sl: float                 # loss at SL after rounding (incl. fees if configured)
    notional: float                       # quantity * contract_size * entry
    leverage: float | None = None         # leverage to set (futures only)
    margin_required: float | None = None
    warnings: list[str] = Field(default_factory=list)


class OpenRequest(BaseModel):
    plan: TradePlan
    symbol: SymbolInfo
    quantity: float
    entry_type: EntryType
    entry_price: float | None = None      # LIMIT price (None for MARKET)
    stop_loss: float
    take_profits: list[TakeProfit] = Field(default_factory=list)
    leverage: float | None = None
    client_tag: str                       # idempotency / order label, e.g. "ct12ab34cd"


class BrokerPosition(BaseModel):
    """State of one trade on one broker. ``data`` is adapter-private (order ids, tickets...)."""

    broker: str
    broker_symbol: str
    side: Side
    status: PositionStatus
    quantity: float                       # intended size
    filled_quantity: float = 0.0
    remaining_quantity: float = 0.0       # still open (after partial closes / TP hits)
    avg_entry_price: float | None = None
    stop_loss: float | None = None
    take_profits: list[TakeProfit] = Field(default_factory=list)
    realized_pnl: float | None = None
    risk_amount: float | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None
    dry_run: bool = False
