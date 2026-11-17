"""The owner's FundingPips account "fx" (type mt5, native, NOT optional) also on the crypto topic, configured like the
friend's tickmill: ``reference_exchange: binanceusdm``, additive offset, ``entry_trigger: reference_touch``.

  limit   no order at the chart (Binance) level: a virtual entry waits for Binance to touch it, then ONE market order
          on fx, stop / target shifted by fx - Binance measured at that moment, sized for 1R (100 USD) from there.
  market  opened at once, stop / target shifted by the offset.
  fx      the forex topic is untouched by the setting (no offset, a plain broker limit).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from copytrader.config import AppConfig
from copytrader.engine.monitor import is_virtual_pending
from copytrader.models import EntryType, PositionStatus, Side

from test_preflight_scenarios import (  # noqa: F401  (_isolate: autouse)
    FX,
    KRIPTO,
    Prod,
    _isolate,
    btc,
    fx_pair,
)


def _fx_on_crypto(cfg: AppConfig) -> None:
    fx = next(b for b in cfg.brokers if b.name == "fx")
    fx.reference_exchange = "binanceusdm"
    fx.offset_mode = "additive"
    fx.max_offset_pct = 1.0
    fx.entry_trigger = "reference_touch"
    fx.virtual_trigger_max_hours = 24
    kripto = next(r for r in cfg.telegram.topics if r.name == "kripto")
    kripto.brokers = [*kripto.brokers, "fx"]


@pytest.fixture
async def prod(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    p = await Prod.create(tmp_path, monkeypatch, edit=_fx_on_crypto)
    assert next(b for b in p.cfg.brokers if b.name == "fx").optional is False
    yield p
    await p.close()


async def test_crypto_limit_waits_on_fx_for_the_binance_touch_then_opens_once_at_market_shifted(prod: Prod) -> None:
    await prod.say(btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0), KRIPTO, "btc 1R long limit emir")
    assert prod.asked() == []
    (trade,) = await prod.trades()
    pos = await prod.positions(trade.id)
    # fx: no order at the chart level; a virtual entry waits for Binance (like tickmill)
    assert prod.fx_orders() == []
    assert is_virtual_pending(pos["fx"]) and pos["fx"].broker_symbol == "BTCUSD"
    assert pos["fx"].data["reference_exchange"] == "binanceusdm" and pos["fx"].data["trigger_price"] == 73000.0
    assert is_virtual_pending(pos["tickmill"])                              # the friend's account: as before

    await prod.engine.monitor.run_virtual_once()                            # Binance not there yet
    assert prod.fx_orders() == []

    # Binance touches 73000; fx quotes 50 below Binance (offset -50): stop 72250, target 74950
    prod.binance.set_price("BTC", 72989.0, 72991.0)
    prod.terminal.set_price("BTCUSD", 72939.0, 72941.0)
    prod.tickmill.set_price("BTCUSD", 72939.0, 72941.0)
    await prod.engine.monitor.run_virtual_once()
    ((kind, symbol, lots, _price, sl, tp),) = prod.fx_orders()
    assert (kind, symbol, sl, tp) == ("BUY", "BTCUSD", 72250.0, 74950.0)
    # 1R = 100 USD from the live entry (72941) to the shifted stop (72250): 691 USD per BTC -> 0.14 BTC
    assert lots == pytest.approx(0.14)
    fired = (await prod.positions(trade.id))["fx"]
    assert fired.status is PositionStatus.OPEN and fired.data["price_offset"]["value"] == pytest.approx(-50.0)

    await prod.engine.monitor.run_virtual_once()                            # never a second order
    await prod.engine.monitor.run_once()
    await prod.engine.monitor.run_virtual_once()
    assert len(prod.fx_orders()) == 1


async def test_crypto_market_signal_opens_fx_at_once_with_stop_and_target_shifted(prod: Prod) -> None:
    prod.binance.set_price("BTC", 73989.0, 73991.0)
    prod.terminal.set_price("BTCUSD", 73939.0, 73941.0)
    prod.tickmill.set_price("BTCUSD", 73939.0, 73941.0)
    await prod.say(btc(EntryType.MARKET, 73990.0, 73300.0, 75500.0), KRIPTO, "btc 1R long")
    assert prod.asked() == []
    ((kind, symbol, _lots, _price, sl, tp),) = prod.fx_orders()
    assert (kind, symbol, sl, tp) == ("BUY", "BTCUSD", 73250.0, 75450.0)
    (trade,) = await prod.trades()
    assert (await prod.positions(trade.id))["fx"].status is PositionStatus.OPEN


async def test_the_forex_topic_on_fx_is_unchanged(prod: Prod) -> None:
    await prod.say(fx_pair("EURUSD", Side.LONG, 1.1610, 1.1590, 1.1650, entry_type=EntryType.LIMIT), FX,
                   "EUR LONG 1R Limit Emir")
    (order,) = prod.fx_orders()
    assert order[0] == "BUY_LIMIT" and order[3:] == (1.1610, 1.1590, 1.1650)
    assert prod.ref.calls == []                                             # no Binance offset on the forex topic
