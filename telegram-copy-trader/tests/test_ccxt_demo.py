"""Binance demo trading: `demo: true` switches ccxt to the exchange's demo environment."""

from __future__ import annotations

import ccxt.async_support as ccxt_async
import pytest

from copytrader.brokers.base import BrokerError
from copytrader.brokers.ccxt_broker import CcxtBroker
from copytrader.config import CcxtBrokerConfig


def _cfg(**kw) -> CcxtBrokerConfig:
    return CcxtBrokerConfig(type="ccxt", name="binance", exchange="binanceusdm", **kw)


async def test_demo_switches_binance_to_demo_urls(monkeypatch) -> None:
    ex = ccxt_async.binanceusdm()
    loaded = []

    async def fake_load_markets(*a, **k):
        loaded.append(ex.urls["api"]["fapiPublic"])
        return {}

    monkeypatch.setattr(ex, "load_markets", fake_load_markets)
    broker = CcxtBroker(_cfg(demo=True), exchange=ex)
    await broker.connect()
    assert loaded == ["https://demo-fapi.binance.com/fapi/v1"]
    assert ex.options.get("enableDemoTrading") is True
    await ex.close()


async def test_demo_unsupported_exchange_is_a_clear_error() -> None:
    class NoDemo:
        id = "nodemo"
        markets: dict = {}

        async def load_markets(self):
            return {}

    broker = CcxtBroker(_cfg(demo=True), exchange=NoDemo())
    with pytest.raises(BrokerError, match="demo"):
        await broker.connect()


def test_demo_and_sandbox_are_exclusive() -> None:
    with pytest.raises(Exception, match="demo"):
        _cfg(demo=True, sandbox=True)
