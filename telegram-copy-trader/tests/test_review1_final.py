"""Final fixes after the live day of 2026-11-12 (coordinator).

1. The manual (breakout) instruction said "11627 KONTRAT — ADA LONG". On the Breakout platform the number was
   typed into a USD amount field: 11627 USD bought 67716 ADA at 0.1717, 5.8x the planned size (risk 270.86 USD
   at the stop instead of 50). The instruction now names the unit and the value in USD.
2. A WARNING notice whose text already starts with the warning icon got a second one ("⚠️ ⚠️ #4 ... kısmen açıldı").
3. STARTING_TEXT has one definition (interfaces.py), shared by the engine and the notifier.
"""

from __future__ import annotations

import re
from pathlib import Path

from copytrader.models import EntryType, MarketType, OpenRequest, Severity, Side, TakeProfit
from copytrader.telegram.notifier import _with_prefix

from copytrader.brokers.manual_broker import ManualBroker
from copytrader.config import ManualBrokerConfig, RiskConfig

from fakes import FakeNotifier
from test_manual_broker import eth_short, make
from test_paper_broker import FakePrices


def with_ada(tmp_path: Path) -> tuple[ManualBroker, FakeNotifier]:
    prices = FakePrices()
    prices.markets["ADA/USDT:USDT"] = {
        "id": "ADAUSDT", "symbol": "ADA/USDT:USDT", "base": "ADA", "quote": "USDT", "settle": "USDT",
        "type": "swap", "swap": True, "spot": False, "linear": True, "active": True, "contractSize": 1.0,
        "precision": {"amount": 1, "price": 0.0001}, "limits": {"amount": {"min": 1}, "cost": {"min": 5.0}}}
    prices.ticker = {"bid": 0.1735, "ask": 0.1736, "last": 0.1736}     # above the limit: it rests, as live
    broker = ManualBroker(ManualBrokerConfig(type="manual", name="breakout", risk_usd=50.0), str(tmp_path),
                          price_source=prices)
    broker.set_risk_config(RiskConfig())
    notifier = FakeNotifier()
    broker.set_notifier(notifier)
    return broker, notifier


async def test_manual_instruction_names_the_unit_and_the_usd_value(tmp_path: Path) -> None:
    broker, notifier = make(tmp_path)
    await broker.connect()
    info = await broker.resolve_symbol("ETH", "USDT", MarketType.PERP, "ETHUSDT.P")
    plan = eth_short(entry_type=EntryType.LIMIT, entry_price=2239.20)
    pos = await broker.open_position(OpenRequest(
        plan=plan, symbol=info, quantity=4.386, entry_type=EntryType.LIMIT, entry_price=2239.20,
        stop_loss=2250.60, take_profits=plan.take_profits, client_tag="cfin1"))
    [(text, severity)] = notifier.notifications
    assert severity is Severity.INFO
    assert "KONTRAT" not in text
    lines = text.split("\n")
    assert lines[0] == "<b>🔢 4.386 ETH — ETH SHORT 📉</b>"
    # the value in USD of exactly that quantity at the entry, so that a USD amount field gets the same size
    usd = 4.386 * 2239.20
    assert lines[2] == (f"Miktar: <b>4.386 ETH</b> (adet) ≈ <b>{usd:.2f} USD</b> tutarında; "
                        f"tutar USD olarak giriliyorsa {usd:.2f} USD girin")
    assert "Giriş: 2239.2 (LİMİT emir)" in text and "Stop: 2250.6" in text and "Risk: ~50.00 USD" in text
    assert pos.data["manual_qty"] == 4.386


async def test_the_live_ada_numbers(tmp_path: Path) -> None:
    """2026-11-12 18:40 UTC: Ada Long limit 0.1720, stop 0.1677, 1R = 50 USD -> 11627 ADA worth about 2000 USD."""
    broker, notifier = with_ada(tmp_path)
    await broker.connect()
    info = await broker.resolve_symbol("ADA", "USDT", MarketType.PERP, "ADAUSDT.P")
    assert info is not None
    size = await broker.size_position(info, Side.LONG, 0.172, 0.1677, 50.0)
    assert size.quantity == 11627.0                       # the quantity the bot gave on 2026-11-12
    plan = eth_short(base_asset="ADA", side=Side.LONG, entry_type=EntryType.LIMIT, entry_price=0.172,
                     stop_loss=0.1677, take_profits=[TakeProfit(price=0.1835)])
    await broker.open_position(OpenRequest(
        plan=plan, symbol=info, quantity=11627.0, entry_type=EntryType.LIMIT, entry_price=0.172,
        stop_loss=0.1677, take_profits=plan.take_profits, client_tag="cfin2"))
    [(text, _)] = notifier.notifications
    assert text.split("\n")[0] == "<b>🔢 11627 ADA — ADA LONG 📈</b>"
    assert "Miktar: <b>11627 ADA</b> (adet) ≈ <b>1999.84 USD</b> tutarında" in text
    assert "tutar USD olarak giriliyorsa 1999.84 USD girin" in text
    assert re.search(r"Risk: ~50\.00 USD", text)


def test_a_notice_never_gets_its_severity_icon_twice() -> None:
    assert _with_prefix("⚠️ <b>#4 NZD LONG kısmen açıldı</b>", Severity.WARNING) == "⚠️ <b>#4 NZD LONG kısmen açıldı</b>"
    assert _with_prefix("⚠ <b>x</b>", Severity.WARNING) == "⚠ <b>x</b>"           # without the variation selector
    assert _with_prefix("🚨 <b>#1 hiçbir brokerda açılamadı</b>", Severity.ERROR).count("🚨") == 1
    # other icons keep the severity icon in front, as before
    assert _with_prefix("✅ <b>#7 ADA LONG</b>", Severity.INFO) == "ℹ️ ✅ <b>#7 ADA LONG</b>"
    assert _with_prefix("<b>#7</b>", Severity.WARNING) == "⚠️ <b>#7</b>"
    assert _with_prefix("ℹ️ bilgi", Severity.WARNING) == "⚠️ ℹ️ bilgi"


def test_starting_text_has_one_definition() -> None:
    import copytrader.engine.orchestrator as orchestrator
    import copytrader.interfaces as interfaces
    import copytrader.telegram.notifier as notifier
    assert orchestrator.STARTING_TEXT is interfaces.STARTING_TEXT
    assert getattr(notifier, "STARTING_TEXT", interfaces.STARTING_TEXT) is interfaces.STARTING_TEXT
    src = Path(orchestrator.__file__).read_text(encoding="utf-8")
    assert "STARTING_TEXT = " not in src


# --------------------------------------------------------------------------- #
# 4. A resting limit that fills is reported to the owner (2026-11-12: the ADA limit filled on Binance and the owner
#    heard nothing; the trader's "Aktif" was the only sign).
# --------------------------------------------------------------------------- #
import pytest

from copytrader.models import PositionStatus, TradeStatus

from test_preflight_scenarios import KRIPTO, btc, prod  # noqa: F401  (fixture)


def _fill_notices(texts: list[str]) -> list[str]:
    return [t for t in texts if "limit giriş" in t and "doldu" in t]


@pytest.mark.parametrize("prod", ["live"], indirect=True)
async def test_binance_limit_fill_is_told_once_with_its_stop(prod) -> None:
    await prod.say(btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0), KRIPTO, "btc 1R long limit emir")
    (trade,) = await prod.trades()
    (entry,) = prod.binance.created()
    assert entry["status"] == "open" and _fill_notices(prod.texts()) == []
    await prod.engine.monitor.run_once()                       # still resting: nothing to tell
    assert _fill_notices(prod.texts()) == []

    prod.binance.set_price("BTC", 72999.0, 73001.0)
    prod.binance.fill(entry["id"])
    await prod.engine.monitor.run_once()
    pos = (await prod.positions(trade.id))["binance"]
    assert pos.status is PositionStatus.OPEN and pos.stop_loss == 72300.0
    (notice,) = _fill_notices(prod.texts())
    assert notice == ("✅ #1 BTC LONG binance: limit girişi doldu — 0.012 BTC/USDT:USDT @ 73000; "
                      "stop 72300 hesapta kurulu.")
    await prod.engine.monitor.run_once()                       # never twice
    await prod.engine.monitor.run_once()
    assert len(_fill_notices(prod.texts())) == 1
    # the manual broker (breakout) is the owner's own: no fill notice for it
    assert not any("breakout: limit girişi" in t for t in prod.texts())


@pytest.mark.parametrize("prod", ["live"], indirect=True)
async def test_partial_fill_is_told_then_the_rest(prod) -> None:
    await prod.say(btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0), KRIPTO, "btc 1R long limit emir")
    (trade,) = await prod.trades()
    (entry,) = prod.binance.created()
    prod.binance.set_price("BTC", 72999.0, 73001.0)
    prod.binance.fill(entry["id"], amount=0.005)
    await prod.engine.monitor.run_once()
    notices = _fill_notices(prod.texts())
    assert len(notices) == 1 and notices[0].startswith("⏳ #1 BTC LONG binance: limit girişi kısmen doldu — 0.005/0.012")
    assert "dolmayan kısmı bekliyor" in notices[0]
    prod.binance.fill(entry["id"])                              # the rest
    await prod.engine.monitor.run_once()
    await prod.engine.monitor.run_once()
    notices = _fill_notices(prod.texts())
    assert len(notices) == 2
    assert notices[1].startswith("✅ #1 BTC LONG binance: limit girişinin kalan kısmı da doldu — toplam 0.012")


@pytest.mark.parametrize("prod", ["prod"], indirect=True)
async def test_no_fill_notice_without_notify_on_execution(prod) -> None:
    assert prod.cfg.notifier.notify_on_execution is False
    await prod.say(btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0), KRIPTO, "btc 1R long limit emir")
    (entry,) = prod.binance.created()
    prod.binance.set_price("BTC", 72999.0, 73001.0)
    prod.binance.fill(entry["id"])
    await prod.engine.monitor.run_once()
    assert _fill_notices(prod.texts()) == []


def test_bundled_contract_names_also_give_the_single_coin_count() -> None:
    from copytrader.brokers.manual_broker import _bundle
    assert _bundle("1000PEPE") == (1000.0, "PEPE")
    assert _bundle("1MBABYDOGE") == (1_000_000.0, "BABYDOGE")
    assert _bundle("1000000MOG") == (1_000_000.0, "MOG")
    assert _bundle("ADA") is None and _bundle("BTC") is None and _bundle("1INCH") is None and _bundle("") is None
    # Only the prefixes the rest of the bot knows (base.contract_multiplier): a coin whose real name starts with a
    # digit is not a bundle ("2Z" is the DoubleZero coin, not 2 x "Z").
    assert _bundle("2Z") is None and _bundle("10000LADYS") == (10_000.0, "LADYS")


@pytest.mark.parametrize("prod", ["live"], indirect=True)
async def test_1000pepe_instruction_names_both_units_and_the_usd_value(prod) -> None:
    from test_preflight_scenarios import signal
    from copytrader.models import MarketType
    pepe = signal("1000PEPEUSDT.P", "1000PEPE", "USDT", MarketType.PERP, Side.LONG, 0.012, 0.0117, 0.0129,
                  entry_type=EntryType.MARKET)
    await prod.say(pepe, KRIPTO, "1000pepe 1R long")
    (note,) = [t for t in prod.texts() if "Elle işlem (breakout)" in t]
    assert "Miktar: <b>166666 1000PEPE</b> (adet) = 166666000 PEPE ≈ <b>" in note
    usd = float(note.split("≈ <b>")[1].split(" USD")[0])
    assert 1990.0 < usd < 2010.0
