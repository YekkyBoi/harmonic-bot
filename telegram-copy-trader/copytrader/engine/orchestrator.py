"""The engine (STUB - see copytrader.interfaces.EngineControl)."""

from __future__ import annotations

from ..brokers.base import Broker
from ..config import AppConfig
from ..interfaces import Notifier, SignalParser, Storage


class Engine:
    def __init__(
        self,
        config: AppConfig,
        db: Storage,
        parser: SignalParser,
        brokers: list[Broker],
        notifier: Notifier,
        price_oracle: "object | None" = None,
    ) -> None:
        raise NotImplementedError

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def on_messages_ready(self, chat_id: int, sender_id: int | None) -> None: ...
    async def on_message_edited(self, msg) -> None: ...
