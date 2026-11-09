"""Broker factory."""

from __future__ import annotations

import logging
from typing import Any

from .base import Broker, BrokerError

logger = logging.getLogger(__name__)


def create_broker(cfg: Any, *, dry_run: bool, app_config: Any) -> Broker:
    """Build the adapter for one BrokerConfig.

    type "ccxt" -> CcxtBroker, "mt5" -> Mt5Broker, "metaapi" -> MetaApiBroker,
    "paper" -> PaperBroker. When ``dry_run`` is True every non-paper adapter is
    wrapped in DryRunBroker (reads are real, writes are simulated). Imports of
    optional SDKs (MetaTrader5, metaapi_cloud_sdk) happen lazily inside this
    function so a missing optional package only breaks that broker type.
    """
    data_dir = str(getattr(app_config, "data_dir", None) or "data")
    kind = getattr(cfg, "type", None)

    if kind == "paper":
        from .paper_broker import PaperBroker

        return PaperBroker(cfg, data_dir)   # already simulated, never wrapped

    if kind == "manual":
        from .manual_broker import ManualBroker

        return ManualBroker(cfg, data_dir)  # sends messages only, never places orders

    broker: Broker
    try:
        if kind == "ccxt":
            from .ccxt_broker import CcxtBroker

            broker = CcxtBroker(cfg)
        elif kind == "mt5":
            from .mt5_broker import Mt5Broker

            broker = Mt5Broker(cfg)
        elif kind == "metaapi":
            from .metaapi_broker import MetaApiBroker

            broker = MetaApiBroker(cfg)
        else:
            raise BrokerError(f"Bilinmeyen broker türü: {kind!r} ({getattr(cfg, 'name', '?')}).")
    except ImportError as e:
        raise BrokerError(
            f"{getattr(cfg, 'name', kind)}: '{kind}' bağlantısı için gerekli paket yüklü değil ({e.name or e})."
        ) from e

    if dry_run:
        from .dry_run import DryRunBroker

        logger.info("%s: wrapped in dry-run mode", broker.name)
        return DryRunBroker(broker, data_dir)
    return broker
