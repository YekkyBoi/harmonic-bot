"""Cross-module interfaces (Protocols).

Concrete implementations:
  Storage        -> copytrader.storage.db.Database
  SignalParser   -> copytrader.parsing.llm_parser.ClaudeSignalParser
  Notifier       -> copytrader.telegram.notifier.BotNotifier
  EngineControl  -> copytrader.engine.orchestrator.Engine
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from .models import (
    BrokerPosition,
    ConfirmationStatus,
    ParseContext,
    ParseResult,
    PendingConfirmation,
    Severity,
    StoredMessage,
    TradePlan,
    TradeRecord,
    TradeStatus,
)

# Returned by the engine for an owner action refused because the bot is shutting down. Nothing was decided:
# the confirmation stays pending (restored after the restart), so the notifier keeps its buttons.
SHUTDOWN_TEXT = "⏳ Bot kapanıyor; şu an işlem yapılamıyor. Bot yeniden başlayınca tekrar deneyin."
# Returned by the engine for an owner action that would trade (approval, correction, close-all) while it is still
# connecting the owner's brokers at start-up. Nothing was decided: the confirmation stays pending, so the notifier
# keeps its buttons (as with SHUTDOWN_TEXT) and the owner taps again a few seconds later.
STARTING_TEXT = "⏳ Bot başlıyor; birkaç saniye sonra tekrar deneyin."
# The engine's answer to a correction of a catch-up question (it has none: yes / no only); the second one while the
# owner's friend answers it (the owner's copy has no buttons). Nothing waits for a correction text then.
NO_CORRECTION_TEXT = "Bu soruda düzeltme yok: ✅ Evet ya da ❌ Hayır'a basın."
NO_CORRECTION_FRIEND_TEXT = "Bu soruyu tickmill hesabının sahibi (arkadaşınız) cevaplıyor; bu soruda düzeltme yok."


@runtime_checkable
class Storage(Protocol):
    """Async persistence. SQLite file at ``<data_dir>/copytrader.db``."""

    async def open(self) -> None: ...
    async def close(self) -> None: ...

    # -- source messages ---------------------------------------------------
    async def save_message(self, msg: StoredMessage) -> None:
        """Upsert by (chat_id, message_id). On edit: update text/media, set edited=True, keep consumed."""

    async def get_message(self, chat_id: int, message_id: int) -> StoredMessage | None: ...

    async def recent_messages(
        self,
        chat_id: int,
        since: datetime,
        sender_id: int | None = None,
        only_unconsumed: bool = True,
        limit: int = 50,
        topic_id: int | None = None,
    ) -> list[StoredMessage]:
        """Oldest first. ``topic_id`` not None: only messages of that forum topic (None = every topic)."""

    async def mark_consumed(self, chat_id: int, message_ids: list[int]) -> None: ...

    # -- trades --------------------------------------------------------------
    async def create_trade(self, plan: TradePlan, status: TradeStatus = TradeStatus.OPENING) -> TradeRecord: ...
    async def get_trade(self, trade_id: int) -> TradeRecord | None: ...
    async def find_trade_by_message(self, chat_id: int, message_id: int) -> TradeRecord | None:
        """Trade whose plan.source_message_ids contains message_id (most recent first)."""

    async def active_trades(self, base_asset: str | None = None) -> list[TradeRecord]:
        """Trades with status OPENING / PENDING / OPEN, newest first."""

    async def recent_trades(self, since: datetime) -> list[TradeRecord]: ...
    async def update_trade(
        self,
        trade_id: int,
        *,
        status: TradeStatus | None = None,
        current_stop_loss: float | None = None,
        current_take_profits: list[Any] | None = None,
        note: str | None = None,
    ) -> TradeRecord: ...

    # -- broker positions ----------------------------------------------------
    async def save_broker_position(self, trade_id: int, pos: BrokerPosition) -> None:
        """Upsert by (trade_id, pos.broker)."""

    async def broker_positions(self, trade_id: int) -> list[BrokerPosition]: ...
    async def active_broker_positions(self) -> list[tuple[int, BrokerPosition]]:
        """(trade_id, position) for positions with status PENDING or OPEN."""

    async def realized_pnl_since(self, broker: str, since: datetime) -> float:
        """Sum of realized_pnl of positions on ``broker`` closed since ``since``."""

    # -- confirmations ---------------------------------------------------------
    async def save_confirmation(self, conf: PendingConfirmation) -> None:
        """Upsert by id."""

    async def get_confirmation(self, conf_id: str) -> PendingConfirmation | None: ...
    async def pending_confirmations(self) -> list[PendingConfirmation]: ...
    async def set_confirmation_status(self, conf_id: str, status: ConfirmationStatus) -> None: ...

    # -- misc --------------------------------------------------------------------
    async def get_state(self, key: str, default: Any = None) -> Any: ...
    async def set_state(self, key: str, value: Any) -> None:
        """JSON-serializable values."""

    async def log_event(self, kind: str, payload: dict[str, Any]) -> None:
        """Append-only audit log."""


class SignalParser(Protocol):
    async def parse(self, messages: list[StoredMessage], context: ParseContext) -> ParseResult:
        """Turn source messages (oldest first, images included) into actions."""


class Notifier(Protocol):
    """Talks to the OWNER (never to the source group); a notifier that has ``set_friend(chat_id | None)`` also mirrors
    everything to the owner's friend and lets him answer the catch-up questions (the engine calls it, see
    Engine.set_friend_chat)."""

    async def start(self, engine: "EngineControl") -> None: ...
    async def stop(self) -> None: ...
    async def ask_confirmation(self, conf: PendingConfirmation) -> int | None:
        """Send the 'I understood X, is that right?' message with buttons. Returns bot message id."""

    async def close_confirmation(self, conf: PendingConfirmation, result_text: str) -> None:
        """Edit the confirmation message: remove buttons, append the outcome."""

    async def notify(self, text: str, severity: Severity = Severity.INFO) -> None: ...


class EngineControl(Protocol):
    """What the notifier bot may ask the engine to do. All return Turkish text for the owner."""

    async def handle_confirmation(self, conf_id: str, approved: bool) -> str:
        """The owner's ✅ / ❌. (Engine also takes ``by_friend=True``: a press from the owner's friend, accepted for a
        catch-up question only.)"""
    async def request_correction(self, conf_id: str) -> str:
        """Owner pressed 'Düzelt': mark the confirmation as awaiting a correction text."""

    async def handle_correction(self, conf_id: str, correction_text: str) -> str:
        """Owner typed a correction ('SL 2200 olacak'). Re-parse and ask again (or execute if now clear)."""

    async def awaiting_correction_id(self) -> str | None:
        """Id of the confirmation currently waiting for a correction text, if any."""

    async def status_text(self) -> str: ...
    async def positions_text(self) -> str: ...
    async def balances_text(self) -> str: ...
    async def set_paused(self, paused: bool) -> str: ...
    async def close_all(self) -> str: ...
    async def test_parse(self, text: str) -> str:
        """Dry parse of free text (no execution) - lets the owner see how a message would be understood."""
