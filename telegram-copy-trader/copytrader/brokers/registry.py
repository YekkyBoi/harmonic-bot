"""Broker factory (STUB - signature is the contract)."""

from __future__ import annotations

from typing import Any

from .base import Broker


def create_broker(cfg: Any, *, dry_run: bool, app_config: Any) -> Broker:
    """Build the adapter for one BrokerConfig.

    type "ccxt" -> CcxtBroker, "mt5" -> Mt5Broker, "metaapi" -> MetaApiBroker,
    "paper" -> PaperBroker. When ``dry_run`` is True every non-paper adapter is
    wrapped in DryRunBroker (reads are real, writes are simulated). Imports of
    optional SDKs (MetaTrader5, metaapi_cloud_sdk) happen lazily inside this
    function so a missing optional package only breaks that broker type.
    """
    raise NotImplementedError
