"""Manual (message-only) broker: fixed money risk per R, bold quantity message, follow-up messages,
and end-to-end routing from a crypto topic next to a real exchange broker."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from copytrader.brokers.manual_broker import ManualBroker
from copytrader.brokers.registry import create_broker
from copytrader.config import AppConfig, ManualBrokerConfig, NotifierConfig, RiskConfig, TelegramConfig
from copytrader.models import EntryType, MarketType, OpenRequest, Severity, Side, TakeProfit, TradePlan

from fakes import FakeNotifier
from test_paper_broker import FakePrices

NOW = datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc)


def make(tmp_path: Path, **kw) -> tuple[ManualBroker, FakeNotifier]:
    cfg = ManualBrokerConfig(type="manual", name="breakout", risk_usd=50.0, **kw)
    broker = ManualBroker(cfg, str(tmp_path), price_source=FakePrices())
    broker.set_risk_config(RiskConfig())
    notifier = FakeNotifier()
    broker.set_notifier(notifier)
    return broker, notifier


def eth_short(**kw) -> TradePlan:
    data = dict(plan_id="m1", base_asset="ETH", side=Side.SHORT, entry_type=EntryType.MARKET, entry_price=2239.20,
                stop_loss=2250.60, take_profits=[TakeProfit(price=2205.11)], risk_r=1.0, risk_pct=2.0,
                source_chat_id=1, created_at=NOW)
    data.update(kw)
    return TradePlan(**data)


async def test_one_r_is_fixed_50_usd(tmp_path: Path) -> None:
    broker, _ = make(tmp_path)
    await broker.connect()
    equity = await broker.get_equity()
    assert equity * 2.0 / 100 == 50.0                     # engine: risk = equity * risk_pct / 100


async def test_open_sends_bold_quantity_with_levels(tmp_path: Path) -> None:
    broker, notifier = make(tmp_path)
    await broker.connect()
    info = await broker.resolve_symbol("ETH", "USDT", MarketType.PERP, "ETHUSDT.P")
    assert info is not None
    entry = 2232.80                                         # live bid for a short (FakePrices)
    size = await broker.size_position(info, Side.SHORT, entry, 2250.60, 50.0)
    assert abs(size.quantity * (2250.60 - entry) - 50.0) < 0.05   # risk == 50 USD, no fee buffer
    plan = eth_short()
    req = OpenRequest(plan=plan, symbol=info, quantity=size.quantity, entry_type=EntryType.MARKET,
                      stop_loss=2250.60, take_profits=plan.take_profits, client_tag="ctm1")
    pos = await broker.open_position(req)
    [(text, severity)] = [(t, s) for t, s in notifier.notifications]
    assert severity is Severity.INFO
    first_line = text.split("\n")[0]
    assert first_line.startswith("<b>🔢 ") and "KONTRAT — ETH SHORT" in first_line and first_line.endswith("</b>")
    assert "2250.6" in text and "2205.11" in text and "2239.2" in text and "50 USD" in text
    assert pos.data["manual_qty"] == size.quantity


async def test_follow_up_messages(tmp_path: Path) -> None:
    broker, notifier = make(tmp_path)
    await broker.connect()
    info = await broker.resolve_symbol("ETH", "USDT", MarketType.PERP, "ETHUSDT.P")
    plan = eth_short()
    pos = await broker.open_position(OpenRequest(plan=plan, symbol=info, quantity=2.0, entry_type=EntryType.MARKET,
                                                 stop_loss=2250.60, take_profits=plan.take_profits, client_tag="c"))
    pos = await broker.update_stop_loss(pos, 2240.00)
    await broker.close_position(pos, 1.0)
    texts = [t for t, _ in notifier.notifications]
    assert "STOPU 2240 YAP" in texts[1] and "ETH" in texts[1]
    assert "TAMAMINI KAPAT" in texts[2]


async def test_qty_decimals_rounds_down(tmp_path: Path) -> None:
    broker, notifier = make(tmp_path, qty_decimals=1)
    await broker.connect()
    info = await broker.resolve_symbol("ETH", "USDT", MarketType.PERP, "ETHUSDT.P")
    plan = eth_short()
    pos = await broker.open_position(OpenRequest(plan=plan, symbol=info, quantity=2.876, entry_type=EntryType.MARKET,
                                                 stop_loss=2250.60, take_profits=plan.take_profits, client_tag="c"))
    assert pos.data["manual_qty"] == 2.8
    assert "2.8 KONTRAT" in notifier.notifications[0][0]


def test_registry_builds_manual_and_never_wraps_in_dry_run(tmp_path: Path) -> None:
    cfg = ManualBrokerConfig(type="manual", name="breakout")
    app = AppConfig(telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[-1]),
                    notifier=NotifierConfig(bot_token="1:x", owner_user_id=1), data_dir=str(tmp_path),
                    brokers=[cfg])
    broker = create_broker(cfg, dry_run=True, app_config=app)
    assert isinstance(broker, ManualBroker)
