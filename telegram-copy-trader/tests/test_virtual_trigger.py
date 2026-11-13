"""entry_trigger: reference_touch - a LIMIT entry on an offset broker is not placed as a broker order; the bot
waits for the reference exchange to touch the chart level, measures the offset right then and opens a MARKET
order with the shifted SL/TPs."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from copytrader.config import AppConfig, NotifierConfig, TelegramConfig
from copytrader.engine.monitor import is_virtual_pending
from copytrader.engine.orchestrator import Engine
from copytrader.models import (
    ActionType,
    EntryType,
    ManagementPlan,
    MarketType,
    ParsedAction,
    PositionStatus,
    Quote,
    Severity,
    Side,
    TakeProfit,
    TradeStatus,
)
from copytrader.storage.db import Database

from test_engine import CHAT, SENDER, action, add_msg, now, result
from fakes import FakeBroker, FakeNotifier, FakeParser, FakePriceOracle


class FakeReference:
    def __init__(self, prices: dict[str, float | None]) -> None:
        self.prices = prices
        self.calls = 0

    async def mid(self, exchange_id: str, base_asset: str, *, fresh: bool = False) -> float | None:
        self.calls += 1
        return self.prices.get(base_asset.upper())


def btc(side: Side = Side.SHORT, **kw: Any) -> ParsedAction:
    short = side is Side.SHORT
    data: dict[str, Any] = dict(
        action=ActionType.OPEN, symbol="BTCUSDT.P", base_asset="BTC", quote_asset="USDT",
        market_type=MarketType.PERP, side=side, entry_type=EntryType.LIMIT,
        entry_price=73200.0 if short else 72000.0, stop_loss=73700.0 if short else 71500.0,
        take_profits=[TakeProfit(price=72000.0 if short else 74000.0)], risk_r=1.0,
        used_message_ids=[501, 502], confidence=0.95, summary_tr="BTC limit", )
    data.update(kw)
    return ParsedAction(**data)


class Env:
    def __init__(self, cfg: AppConfig, db: Database, parser: FakeParser, broker: FakeBroker,
                 notifier: FakeNotifier, ref: FakeReference, engine: Engine) -> None:
        self.cfg, self.db, self.parser, self.broker = cfg, db, parser, broker
        self.notifier, self.ref, self.engine = notifier, ref, engine

    async def signal(self) -> None:
        await add_msg(self.db, 501, "btc limit")
        await add_msg(self.db, 502, "1R")
        await self.engine.on_messages_ready(CHAT, SENDER)

    async def tick(self) -> None:
        await self.engine.monitor.run_virtual_once()


def make_engine(db: Database, broker: FakeBroker, parser: FakeParser, notifier: FakeNotifier,
                ref: FakeReference, cfg: AppConfig, clock: Any = None) -> Engine:
    return Engine(cfg, db, parser, [broker], notifier,
                  price_oracle=FakePriceOracle({"BTC": 73200.0}), reference_prices=ref, clock=clock)


async def make_env(db: Database, *, trigger: str = "reference_touch", ref_price: float | None = 72900.0,
                   no_hedge: bool = False, clock: Any = None, max_hours: float = 48.0) -> Env:
    cfg = AppConfig(
        telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[CHAT]),
        notifier=NotifierConfig(bot_token="123:abc", owner_user_id=42),
    )
    cfg.monitor_interval_sec = 3600.0
    cfg.notifier.notify_on_execution = True
    broker = FakeBroker("tickmill", equity=10_000.0, quote=Quote(bid=73099.9, ask=73100.1, last=73100.0))
    broker.config.reference_exchange = "binanceusdm"
    broker.config.entry_trigger = trigger            # type: ignore[assignment]
    broker.config.virtual_trigger_max_hours = max_hours
    broker.config.no_hedge = no_hedge
    parser, notifier = FakeParser(), FakeNotifier()
    ref = FakeReference({"BTC": ref_price})
    engine = make_engine(db, broker, parser, notifier, ref, cfg, clock)
    await engine.start(monitor=False)
    notifier.notifications.clear()
    return Env(cfg, db, parser, broker, notifier, ref, engine)


@pytest.fixture
async def db():
    database = Database(":memory:")
    await database.open()
    yield database
    await database.close()


async def only_position(env: Env):
    trades = await env.db.active_trades()
    if not trades:
        # terminal trade: find it through all positions
        row = await env.db.get_trade(1)
        return row, (await env.db.broker_positions(1))[0]
    return trades[0], (await env.db.broker_positions(trades[0].id))[0]


# --------------------------------------------------------------------------- #
# Registration / triggering
# --------------------------------------------------------------------------- #
async def test_registers_virtual_entry_without_broker_order(db: Database) -> None:
    env = await make_env(db)
    env.parser.push(result(btc()))
    await env.signal()
    assert env.broker.opened == [] and env.broker.calls_of("open_position") == []
    trade, pos = await only_position(env)
    assert trade.status is TradeStatus.PENDING
    assert is_virtual_pending(pos) and pos.data["trigger_price"] == 73200.0
    assert pos.stop_loss == 73700.0
    infos = env.notifier.texts(Severity.INFO)
    assert any("Sanal tetik kuruldu".lower() in t.lower() and "73200" in t and "tickmill" in t for t in infos)
    assert "bekleyen (sanal tetik)" in await env.engine.positions_text()
    assert "sanal tetik" in await env.engine.status_text()


async def test_short_no_order_before_touch_then_fires_with_offset_measured_at_trigger(db: Database) -> None:
    env = await make_env(db, ref_price=72900.0)
    env.parser.push(result(btc()))
    await env.signal()
    for price in (72900.0, 73100.0, 73199.9):
        env.ref.prices["BTC"] = price
        await env.tick()
    assert env.broker.opened == []
    # touch: reference 73200 while the broker trades at 73350 -> offset +150 (it was +200 at signal time)
    env.ref.prices["BTC"] = 73200.0
    env.broker.quote = Quote(bid=73349.9, ask=73350.1, last=73350.0)
    env.notifier.notifications.clear()
    await env.tick()
    assert len(env.broker.opened) == 1
    req = env.broker.opened[0]
    assert req.entry_type is EntryType.MARKET and req.entry_price is None
    assert req.stop_loss == pytest.approx(73850.0)
    assert [tp.price for tp in req.take_profits] == [pytest.approx(72150.0)]
    sized = env.broker.sized[0]
    assert sized["entry"] == pytest.approx(73349.9) and sized["stop"] == pytest.approx(73850.0)
    trade, pos = await only_position(env)
    assert trade.status is TradeStatus.OPEN and pos.status is PositionStatus.OPEN
    assert pos.data["price_offset"] == {"mode": "additive", "value": pytest.approx(150.0)}
    assert not is_virtual_pending(pos)
    text = "\n".join(env.notifier.texts())
    assert "tetiklendi" in text and "açıldı" in text and "fark +150.0 uygulandı" in text
    # never fires twice
    await env.tick()
    await env.engine.monitor.run_once()
    assert len(env.broker.opened) == 1


async def test_long_fires_when_reference_falls_to_entry(db: Database) -> None:
    env = await make_env(db, ref_price=72500.0)
    env.broker.quote = Quote(bid=72199.9, ask=72200.1, last=72200.0)
    env.parser.push(result(btc(Side.LONG)))
    await env.signal()
    await env.tick()
    assert env.broker.opened == []
    env.ref.prices["BTC"] = 72000.0
    env.broker.quote = Quote(bid=72099.9, ask=72100.1, last=72100.0)      # offset +100 now
    await env.tick()
    req = env.broker.opened[0]
    assert req.entry_type is EntryType.MARKET
    assert req.stop_loss == pytest.approx(71600.0)
    assert [tp.price for tp in req.take_profits] == [pytest.approx(74100.0)]
    _, pos = await only_position(env)
    assert pos.data["price_offset"]["value"] == pytest.approx(100.0)


async def test_restart_keeps_virtual_entry_and_does_not_fire_twice(db: Database) -> None:
    env = await make_env(db)
    env.parser.push(result(btc()))
    await env.signal()
    # "restart": a brand new engine on the same database
    broker2 = FakeBroker("tickmill", equity=10_000.0, quote=Quote(bid=73349.9, ask=73350.1, last=73350.0))
    broker2.config.reference_exchange = "binanceusdm"
    broker2.config.entry_trigger = "reference_touch"         # type: ignore[assignment]
    notifier2, ref2 = FakeNotifier(), FakeReference({"BTC": 72500.0})
    engine2 = make_engine(db, broker2, FakeParser(), notifier2, ref2, env.cfg)
    await engine2.start(monitor=False)
    await engine2.monitor.run_virtual_once()
    assert broker2.opened == []
    ref2.prices["BTC"] = 73250.0
    await engine2.monitor.run_virtual_once()
    assert len(broker2.opened) == 1
    # a second restart after it fired: nothing more
    engine3 = make_engine(db, broker2, FakeParser(), FakeNotifier(), ref2, env.cfg)
    await engine3.start(monitor=False)
    await engine3.monitor.run_virtual_once()
    await engine3.monitor.run_once()
    assert len(broker2.opened) == 1


async def test_interrupted_firing_is_never_resent(db: Database) -> None:
    env = await make_env(db)
    env.parser.push(result(btc()))
    await env.signal()
    trade, pos = await only_position(env)
    pos.data["virtual_state"] = "firing"                     # the process died after saving this
    await db.save_broker_position(trade.id, pos)
    env.ref.prices["BTC"] = 73300.0
    await env.tick()
    assert env.broker.opened == []
    _, pos = await only_position(env)
    assert pos.status is PositionStatus.FAILED
    assert any("GÖNDERİLMEDİ" in t for t in env.notifier.texts(Severity.ERROR))


# --------------------------------------------------------------------------- #
# Cancel / expiry / gap
# --------------------------------------------------------------------------- #
async def test_cancel_message_cancels_virtual_entry(db: Database) -> None:
    env = await make_env(db)
    env.parser.push(result(btc()))
    await env.signal()
    env.parser.push(result(action(ActionType.CANCEL, base_asset="BTC", side=Side.SHORT, used_message_ids=[600])))
    await add_msg(env.db, 600, "iptal", reply_to=502)
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.broker.calls_of("cancel_entry") == []         # no venue order to cancel
    trade = await env.db.get_trade(1)
    assert trade.status is TradeStatus.CANCELLED
    assert (await env.db.broker_positions(1))[0].status is PositionStatus.CANCELLED
    env.ref.prices["BTC"] = 73300.0
    await env.tick()
    assert env.broker.opened == []


async def test_closure_report_before_trigger_cancels(db: Database) -> None:
    env = await make_env(db)
    env.parser.push(result(btc()))
    await env.signal()
    plan = ManagementPlan(plan_id="p1", action=ActionType.CLOSE, trade_id=1, close_fraction=1.0,
                          source_chat_id=CHAT, summary_tr="kapat", created_at=now())
    out = await env.engine.execute_management(plan)
    assert "sanal tetik iptal edildi" in out
    assert (await env.db.get_trade(1)).status is TradeStatus.CANCELLED
    env.ref.prices["BTC"] = 73300.0
    await env.tick()
    assert env.broker.opened == []


async def test_move_sl_and_tp_update_stored_levels_then_fire_uses_them(db: Database) -> None:
    env = await make_env(db)
    env.parser.push(result(btc()))
    await env.signal()
    env.parser.push(result(action(ActionType.MOVE_SL, base_asset="BTC", new_stop_loss=73600.0,
                                  used_message_ids=[600])))
    await add_msg(env.db, 600, "stop 73600", reply_to=502)
    await env.engine.on_messages_ready(CHAT, SENDER)
    plan = ManagementPlan(plan_id="p2", action=ActionType.MOVE_TP, trade_id=1,
                          take_profits=[TakeProfit(price=71800.0)], source_chat_id=CHAT,
                          summary_tr="tp", created_at=now())
    await env.engine.execute_management(plan)
    assert env.broker.calls_of("update_stop_loss") == [] and env.broker.calls_of("update_take_profits") == []
    pos = (await env.db.broker_positions(1))[0]
    assert is_virtual_pending(pos) and pos.stop_loss == 73600.0 and pos.take_profits[0].price == 71800.0
    env.ref.prices["BTC"] = 73200.0
    env.broker.quote = Quote(bid=73349.9, ask=73350.1, last=73350.0)
    await env.tick()
    req = env.broker.opened[0]
    assert req.stop_loss == pytest.approx(73750.0) and req.take_profits[0].price == pytest.approx(71950.0)


async def test_expires_after_max_hours(db: Database) -> None:
    clock = {"t": now()}
    env = await make_env(db, clock=lambda: clock["t"], max_hours=48.0)
    # Review 1 (engine_mgmt, M3): a virtual entry never waits longer than risk.pending_entry_expiry_min (default
    # 1440 min = 24 h, which would end it first); with a longer limit max_hours is what ends it, as tested here.
    env.cfg.risk.pending_entry_expiry_min = 72 * 60
    env.parser.push(result(btc()))
    await env.signal()
    clock["t"] += timedelta(hours=47)
    await env.tick()
    assert (await env.db.broker_positions(1))[0].status is PositionStatus.PENDING
    clock["t"] += timedelta(hours=2)
    env.ref.prices["BTC"] = 73300.0                          # touch after expiry must not open
    await env.tick()
    assert env.broker.opened == []
    assert (await env.db.broker_positions(1))[0].status is PositionStatus.CANCELLED
    assert (await env.db.get_trade(1)).status is TradeStatus.CANCELLED
    assert any("tetiklenmedi" in t for t in env.notifier.texts())


async def test_gap_past_stop_cancels_with_warning(db: Database) -> None:
    env = await make_env(db)
    env.parser.push(result(btc()))
    await env.signal()
    env.ref.prices["BTC"] = 73750.0                          # jumped over the entry AND the stop
    await env.tick()
    assert env.broker.opened == []
    assert (await env.db.broker_positions(1))[0].status is PositionStatus.CANCELLED
    warns = env.notifier.texts(Severity.WARNING)
    assert warns and "stop seviyesini" in warns[0] and "AÇILMADI" in warns[0]


async def test_price_past_tp1_before_trigger_cancels(db: Database) -> None:
    env = await make_env(db)
    env.parser.push(result(btc()))
    await env.signal()
    env.ref.prices["BTC"] = 71900.0                          # fell through TP1 without ever touching the entry
    await env.tick()
    assert env.broker.opened == []
    assert (await env.db.broker_positions(1))[0].status is PositionStatus.CANCELLED
    assert any("ilk hedefi" in t for t in env.notifier.texts(Severity.WARNING))


# --------------------------------------------------------------------------- #
# Fail closed / guards
# --------------------------------------------------------------------------- #
async def test_reference_outage_keeps_waiting_and_warns_once(db: Database) -> None:
    env = await make_env(db)
    env.parser.push(result(btc()))
    await env.signal()
    env.ref.prices["BTC"] = None
    for _ in range(5):
        await env.tick()
    warns = env.notifier.texts(Severity.WARNING)
    assert len(warns) == 1 and "referans fiyat" in warns[0]
    assert env.broker.opened == []
    assert (await env.db.broker_positions(1))[0].status is PositionStatus.PENDING
    env.ref.prices["BTC"] = 73250.0                          # recovers and touches: opens, shifted +100
    env.broker.quote = Quote(bid=73349.9, ask=73350.1, last=73350.0)
    await env.tick()
    assert len(env.broker.opened) == 1 and env.broker.opened[0].stop_loss == pytest.approx(73800.0)


async def test_offset_over_max_pct_at_trigger_cancels_not_opens(db: Database) -> None:
    env = await make_env(db)
    env.broker.config.max_offset_pct = 0.5
    env.parser.push(result(btc()))
    await env.signal()
    env.ref.prices["BTC"] = 73200.0
    env.broker.quote = Quote(bid=74999.9, ask=75000.1, last=75000.0)       # 2.4% away
    await env.tick()
    assert env.broker.opened == []
    assert (await env.db.broker_positions(1))[0].status is PositionStatus.CANCELLED
    assert any("fark" in t for t in env.notifier.texts(Severity.WARNING))


async def test_no_hedge_blocks_opposite_signal_while_virtual_waits_and_at_trigger(db: Database) -> None:
    env = await make_env(db, no_hedge=True)
    env.parser.push(result(btc()))
    await env.signal()
    # an opposite LONG limit signal while the short virtual entry waits -> refused by the hedge rule
    env.parser.push(result(btc(Side.LONG, used_message_ids=[510, 511])))
    await add_msg(env.db, 510, "btc long")
    await add_msg(env.db, 511, "1R")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert len(await env.db.active_trades()) == 1
    assert any("hedging" in t for t in env.notifier.texts())
    assert (await env.db.broker_positions(1))[0].status is PositionStatus.PENDING


async def test_no_hedge_at_trigger_when_opposite_position_appeared_meanwhile(db: Database) -> None:
    env = await make_env(db, no_hedge=True)
    env.parser.push(result(btc()))
    await env.signal()
    # an opposite OPEN position on the same broker/asset appears while the entry waits
    from copytrader.models import BrokerPosition
    plan = (await db.get_trade(1)).plan
    t2 = await db.create_trade(plan.model_copy(update={"side": Side.LONG, "plan_id": "other"}))
    await db.save_broker_position(t2.id, BrokerPosition(
        broker="tickmill", broker_symbol="BTC/USDT:USDT", side=Side.LONG, status=PositionStatus.OPEN,
        quantity=1, filled_quantity=1, remaining_quantity=1, avg_entry_price=73000.0))
    await db.update_trade(t2.id, status=TradeStatus.OPEN)
    env.ref.prices["BTC"] = 73300.0
    await env.tick()
    assert env.broker.opened == []
    assert (await env.db.broker_positions(1))[0].status is PositionStatus.CANCELLED


async def test_daily_loss_limit_respected_at_trigger(db: Database) -> None:
    env = await make_env(db)
    env.parser.push(result(btc()))
    await env.signal()
    env.broker.config.daily_loss_limit_pct = 1.0
    # book a -500 realized loss today on this broker via another closed trade
    from copytrader.models import BrokerPosition
    t2 = await db.create_trade((await db.get_trade(1)).plan)
    await db.save_broker_position(t2.id, BrokerPosition(
        broker="tickmill", broker_symbol="BTC/USDT:USDT", side=Side.SHORT, status=PositionStatus.CLOSED,
        quantity=1, filled_quantity=1, remaining_quantity=0, realized_pnl=-500.0))
    await db.update_trade(t2.id, status=TradeStatus.CLOSED)
    env.ref.prices["BTC"] = 73300.0
    await env.tick()
    assert env.broker.opened == []
    assert (await env.db.broker_positions(1))[0].status is PositionStatus.CANCELLED
    assert any("günlük zarar" in t for t in env.notifier.texts())


# --------------------------------------------------------------------------- #
# Unchanged behaviour
# --------------------------------------------------------------------------- #
async def test_default_broker_limit_places_a_real_limit_order(db: Database) -> None:
    env = await make_env(db, trigger="broker_limit", ref_price=73200.0)
    env.broker.quote = Quote(bid=73399.9, ask=73400.1, last=73400.0)
    env.parser.push(result(btc()))
    await env.signal()
    req = env.broker.opened[0]
    assert req.entry_type is EntryType.LIMIT and req.entry_price == pytest.approx(73400.0)
    _, pos = await only_position(env)
    assert not pos.data.get("virtual_trigger")
    await env.tick()
    assert len(env.broker.opened) == 1


async def test_market_entry_opens_at_once_with_offset(db: Database) -> None:
    env = await make_env(db, ref_price=73200.0)
    env.broker.quote = Quote(bid=73399.9, ask=73400.1, last=73400.0)
    env.parser.push(result(btc(entry_type=EntryType.MARKET)))
    await env.signal()
    req = env.broker.opened[0]
    assert req.entry_type is EntryType.MARKET and req.stop_loss == pytest.approx(73900.0)
    _, pos = await only_position(env)
    assert not pos.data.get("virtual_trigger") and pos.data["price_offset"]["value"] == pytest.approx(200.0)


async def test_config_requires_reference_exchange_and_defaults_off() -> None:
    from pydantic import ValidationError
    from copytrader.config import PaperBrokerConfig
    assert PaperBrokerConfig(type="paper", name="x").entry_trigger == "broker_limit"
    with pytest.raises(ValidationError):
        PaperBrokerConfig(type="paper", name="x", entry_trigger="reference_touch")
    ok = PaperBrokerConfig(type="paper", name="x", entry_trigger="reference_touch", reference_exchange="binanceusdm")
    assert ok.virtual_trigger_max_hours == 48.0
