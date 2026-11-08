"""Engine end-to-end tests with fake parser / brokers / notifier and a real in-memory Database."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from copytrader.brokers.base import BrokerError
from copytrader.config import AppConfig, NotifierConfig, TelegramConfig
from copytrader.engine.orchestrator import Engine, is_transient_parse_error
from copytrader.engine.prices import PriceOracle
from copytrader.errors import ParserError
from copytrader.models import (
    ActionType,
    BrokerPosition,
    ConfirmationStatus,
    EntryType,
    ManagementPlan,
    MarketType,
    ParsedAction,
    ParseResult,
    PendingConfirmation,
    PositionStatus,
    Quote,
    Severity,
    Side,
    StoredMessage,
    TakeProfit,
    TradePlan,
    TradeStatus,
)
from copytrader.storage.db import Database

from fakes import FakeBroker, FakeNotifier, FakeParser, FakePriceOracle

CHAT = -1001234567890
SENDER = 777
IMAGE = str(Path(__file__).resolve().parents[1] / "examples" / "eth_long_tradingview.jpg")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def now() -> datetime:
    return datetime.now(timezone.utc)


def make_config() -> AppConfig:
    cfg = AppConfig(
        telegram=TelegramConfig(api_id=1, api_hash="hash", source_chats=[CHAT]),
        notifier=NotifierConfig(bot_token="123:abc", owner_user_id=42),
    )
    cfg.monitor_interval_sec = 3600.0
    return cfg


def eth_open(**kw: object) -> ParsedAction:
    """The owner's example: TradingView long tool on ETHUSDT.P + 'eth 1R long'."""
    data: dict[str, object] = dict(
        action=ActionType.OPEN, symbol="ETHUSDT.P", base_asset="ETH", quote_asset="USDT",
        market_type=MarketType.PERP, side=Side.LONG, entry_type=EntryType.MARKET,
        entry_price=2232.18, stop_loss=2205.11, take_profits=[TakeProfit(price=2310.6, close_fraction=1.0)],
        risk_r=1.0, used_message_ids=[501, 502], confidence=0.95,
        summary_tr="ETH LONG, giriş 2232.18, SL 2205.11, TP 2310.60, risk 1R",
        reasoning_brief="TP 2310.60 gri etiket, giriş 2232.18 yeşil, SL 2205.11 kırmızı; 2232.85 anlık fiyat.",
    )
    data.update(kw)
    return ParsedAction(**data)  # type: ignore[arg-type]


def eth_plan(**kw: object) -> TradePlan:
    data: dict[str, object] = dict(
        plan_id="old", base_asset="ETH", side=Side.LONG, entry_type=EntryType.MARKET, entry_price=2000.0,
        stop_loss=1950.0, risk_r=1.0, risk_pct=2.0, source_chat_id=CHAT, source_message_ids=[100], created_at=now(),
    )
    data.update(kw)
    return TradePlan(**data)  # type: ignore[arg-type]


def action(kind: ActionType, **kw: object) -> ParsedAction:
    data: dict[str, object] = dict(action=kind, confidence=0.95)
    data.update(kw)
    return ParsedAction(**data)  # type: ignore[arg-type]


def result(*actions: ParsedAction) -> ParseResult:
    return ParseResult(actions=list(actions), model="fake")


async def add_msg(db: Database, mid: int, text: str = "", *, media: str | None = None,
                  reply_to: int | None = None, sender: int = SENDER) -> StoredMessage:
    msg = StoredMessage(chat_id=CHAT, message_id=mid, sender_id=sender, sender_name="Trader", date=now(),
                        text=text, media_path=media, media_type="photo" if media else None,
                        reply_to_message_id=reply_to)
    await db.save_message(msg)
    return msg


@dataclass
class Env:
    cfg: AppConfig
    db: Database
    parser: FakeParser
    a: FakeBroker
    b: FakeBroker
    notifier: FakeNotifier
    oracle: FakePriceOracle
    engine: Engine

    @property
    def brokers(self) -> list[FakeBroker]:
        return [self.a, self.b]

    async def signal(self) -> None:
        """Owner's ETH example: screenshot (501) + 'eth 1R long' (502)."""
        await add_msg(self.db, 501, media=IMAGE)
        await add_msg(self.db, 502, "eth 1R long")
        await self.engine.on_messages_ready(CHAT, SENDER)

    def opens(self) -> int:
        return sum(len(b.opened) for b in self.brokers)


@pytest.fixture
async def db():
    database = Database(":memory:")
    await database.open()
    yield database
    await database.close()


@pytest.fixture
async def env(db: Database):
    cfg = make_config()
    parser = FakeParser()
    a = FakeBroker("binance", equity=10_000.0)
    b = FakeBroker("bybit", equity=5_000.0)
    notifier = FakeNotifier()
    oracle = FakePriceOracle({"ETH": 2232.85})
    engine = Engine(cfg, db, parser, [a, b], notifier, price_oracle=oracle)
    await engine.start(monitor=False)
    notifier.notifications.clear()      # startup summary
    yield Env(cfg, db, parser, a, b, notifier, oracle, engine)
    await engine.stop()


async def wait_for(predicate, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


# --------------------------------------------------------------------------- #
# The owner's example, fully automatic
# --------------------------------------------------------------------------- #
async def test_eth_example_executes_on_both_brokers_with_2pct_risk(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()

    assert env.notifier.confirmations == []
    assert env.notifier.texts(Severity.ERROR) == [] and env.notifier.texts(Severity.WARNING) == []
    for broker in env.brokers:
        assert len(broker.opened) == 1
        req = broker.opened[0]
        assert req.entry_type is EntryType.MARKET and req.entry_price is None
        assert req.stop_loss == 2205.11
        assert [(tp.price, tp.close_fraction) for tp in req.take_profits] == [(2310.6, 1.0)]
        assert req.plan.side is Side.LONG and req.plan.base_asset == "ETH"
        assert req.client_tag.startswith("ct")
        sized = broker.sized[0]
        assert sized["risk_amount"] == pytest.approx(0.02 * broker.equity)    # 1R = 2% of equity
        assert sized["entry"] == 2232.90 and sized["stop"] == 2205.11          # live ask for a long
    # parser saw both messages together, no open trades yet
    msgs, ctx = env.parser.calls[0]
    assert [m.message_id for m in msgs] == [501, 502] and ctx.open_trades == []

    for mid in (501, 502):
        stored = await env.db.get_message(CHAT, mid)
        assert stored is not None and stored.consumed
    trades = await env.db.active_trades()
    assert len(trades) == 1 and trades[0].status is TradeStatus.OPEN
    assert trades[0].plan.risk_pct == 2.0 and trades[0].current_stop_loss == 2205.11
    positions = await env.db.broker_positions(trades[0].id)
    assert sorted(p.broker for p in positions) == ["binance", "bybit"]
    assert {p.broker: p.risk_amount for p in positions} == {"binance": pytest.approx(200.0), "bybit": pytest.approx(100.0)}


async def test_notify_on_execution_reports_plan(env: Env) -> None:
    env.cfg.notifier.notify_on_execution = True
    env.parser.push(result(eth_open()))
    await env.signal()
    infos = env.notifier.texts(Severity.INFO)
    assert len(infos) == 1 and "açıldı" in infos[0] and "2205.11" in infos[0] and "binance" in infos[0]


# --------------------------------------------------------------------------- #
# Confirmations
# --------------------------------------------------------------------------- #
async def test_low_confidence_asks_then_approve_executes(env: Env) -> None:
    env.parser.push(result(eth_open(confidence=0.7)))
    await env.signal()
    assert env.opens() == 0
    assert len(env.notifier.confirmations) == 1
    conf = env.notifier.confirmations[0]
    assert conf.kind == "open" and isinstance(conf.plan, TradePlan)
    assert "low_confidence" in [i.code for i in conf.issues]
    stored = await env.db.get_confirmation(conf.id)
    assert stored is not None and stored.bot_message_id is not None
    assert (await env.db.get_message(CHAT, 501)).consumed  # type: ignore[union-attr]

    text = await env.engine.handle_confirmation(conf.id, True)
    assert "açıldı" in text
    assert env.opens() == 2
    assert (await env.db.get_confirmation(conf.id)).status is ConfirmationStatus.APPROVED  # type: ignore[union-attr]
    assert env.notifier.closed_text(conf.id) == text
    again = await env.engine.handle_confirmation(conf.id, True)
    assert "artık geçerli değil" in again and env.opens() == 2


async def test_concurrent_double_tap_executes_once(env: Env) -> None:
    env.parser.push(result(eth_open(confidence=0.7)))
    await env.signal()
    conf_id = env.notifier.confirmations[0].id
    r1, r2 = await asyncio.gather(env.engine.handle_confirmation(conf_id, True),
                                  env.engine.handle_confirmation(conf_id, True))
    assert env.opens() == 2
    assert sum("artık geçerli değil" in r for r in (r1, r2)) == 1


async def test_reject_does_nothing(env: Env) -> None:
    env.parser.push(result(eth_open(confidence=0.7)))
    await env.signal()
    conf = env.notifier.confirmations[0]
    text = await env.engine.handle_confirmation(conf.id, False)
    assert "İptal" in text and env.opens() == 0
    assert (await env.db.get_confirmation(conf.id)).status is ConfirmationStatus.REJECTED  # type: ignore[union-attr]
    assert await env.db.active_trades() == []


async def test_expiry_does_nothing(env: Env) -> None:
    env.cfg.notifier.confirm_timeout_sec = 0.2  # type: ignore[assignment]
    env.parser.push(result(eth_open(confidence=0.7)))
    await env.signal()
    conf = env.notifier.confirmations[0]
    await wait_for(lambda: env.notifier.closed_text(conf.id) is not None)
    assert "Süre doldu" in (env.notifier.closed_text(conf.id) or "")
    assert (await env.db.get_confirmation(conf.id)).status is ConfirmationStatus.EXPIRED  # type: ignore[union-attr]
    assert "artık geçerli değil" in await env.engine.handle_confirmation(conf.id, True)
    assert env.opens() == 0


async def test_blocking_no_stop_then_correction_then_approve(env: Env) -> None:
    env.parser.push(result(eth_open(stop_loss=None)))
    await env.signal()
    conf = env.notifier.confirmations[0]
    assert conf.plan is None
    assert any(i.code == "missing_stop_loss" and i.blocking for i in conf.issues)
    assert "düzeltme" in (await env.engine.handle_confirmation(conf.id, True)).lower()
    assert env.opens() == 0

    await env.engine.request_correction(conf.id)
    assert await env.engine.awaiting_correction_id() == conf.id
    env.parser.push(result(eth_open(confidence=0.97)))
    reply = await env.engine.handle_correction(conf.id, "SL 2205.11")
    assert "yeni onay" in reply
    _msgs, ctx = env.parser.calls[-1]
    assert ctx.correction_text == "SL 2205.11" and ctx.previous_action is not None
    assert ctx.previous_action.stop_loss is None
    assert [m.message_id for m in _msgs] == [501, 502]

    assert (await env.db.get_confirmation(conf.id)).status is ConfirmationStatus.SUPERSEDED  # type: ignore[union-attr]
    assert "Düzeltildi" in (env.notifier.closed_text(conf.id) or "")
    assert len(env.notifier.confirmations) == 2
    new = env.notifier.confirmations[1]
    assert isinstance(new.plan, TradePlan) and new.plan.stop_loss == 2205.11
    assert new.issues == []          # clear now, but still one more tap
    assert env.opens() == 0
    assert await env.engine.awaiting_correction_id() is None

    await env.engine.handle_confirmation(new.id, True)
    assert env.opens() == 2
    assert env.a.opened[0].stop_loss == 2205.11


async def test_correction_saying_not_a_trade_rejects(env: Env) -> None:
    env.parser.push(result(eth_open(confidence=0.5)))
    await env.signal()
    conf = env.notifier.confirmations[0]
    env.parser.push(result(action(ActionType.NONE, used_message_ids=[501, 502])))
    reply = await env.engine.handle_correction(conf.id, "bu işlem değil")
    assert "işlem talimatı yok" in reply
    assert (await env.db.get_confirmation(conf.id)).status is ConfirmationStatus.REJECTED  # type: ignore[union-attr]
    assert len(env.notifier.confirmations) == 1 and env.opens() == 0


async def test_restore_pending_confirmations_on_start(db: Database) -> None:
    t = now()
    old = PendingConfirmation(id="aaa111", kind="open", action=eth_open(), source_chat_id=CHAT,
                              created_at=t - timedelta(hours=1), expires_at=t - timedelta(minutes=50))
    live = PendingConfirmation(id="bbb222", kind="open", action=eth_open(), source_chat_id=CHAT,
                               created_at=t, expires_at=t + timedelta(seconds=0.3))
    await db.save_confirmation(old)
    await db.save_confirmation(live)
    notifier = FakeNotifier()
    engine = Engine(make_config(), db, FakeParser(), [FakeBroker("x")], notifier, price_oracle=FakePriceOracle())
    await engine.start(monitor=False)
    try:
        assert (await db.get_confirmation("aaa111")).status is ConfirmationStatus.EXPIRED  # type: ignore[union-attr]
        assert (await db.get_confirmation("bbb222")).status is ConfirmationStatus.PENDING  # type: ignore[union-attr]
        await wait_for(lambda: notifier.closed_text("bbb222") is not None)
        assert (await db.get_confirmation("bbb222")).status is ConfirmationStatus.EXPIRED  # type: ignore[union-attr]
    finally:
        await engine.stop()


# --------------------------------------------------------------------------- #
# Incomplete signals
# --------------------------------------------------------------------------- #
def eth_incomplete(**kw: object) -> ParsedAction:
    data: dict[str, object] = dict(base_asset="ETH", side=Side.LONG, entry_price=2232.18,
                                   take_profits=[TakeProfit(price=2310.6)], used_message_ids=[501],
                                   confidence=0.8, summary_tr="ETH long, stop yok")
    data.update(kw)
    return action(ActionType.INCOMPLETE, **data)


async def test_incomplete_times_out_into_incomplete_confirmation(env: Env) -> None:
    env.cfg.telegram.incomplete_timeout_sec = 0.2  # type: ignore[assignment]
    env.parser.push(result(eth_incomplete()))
    await add_msg(env.db, 501, media=IMAGE)
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.notifier.confirmations == []
    assert not (await env.db.get_message(CHAT, 501)).consumed  # type: ignore[union-attr]

    await wait_for(lambda: len(env.notifier.confirmations) == 1)
    conf = env.notifier.confirmations[0]
    assert conf.kind == "incomplete" and conf.plan is None
    codes = [i.code for i in conf.issues]
    assert "incomplete_signal" in codes and "missing_stop_loss" in codes
    assert (await env.db.get_message(CHAT, 501)).consumed  # type: ignore[union-attr]
    assert env.opens() == 0


async def test_incomplete_then_completing_message_executes(env: Env) -> None:
    env.cfg.telegram.incomplete_timeout_sec = 0.3  # type: ignore[assignment]
    env.parser.push(result(eth_incomplete()), result(eth_open()))
    await add_msg(env.db, 501, media=IMAGE)
    await env.engine.on_messages_ready(CHAT, SENDER)
    await add_msg(env.db, 502, "eth 1R long")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert [m.message_id for m in env.parser.calls[1][0]] == [501, 502]
    assert env.opens() == 2
    await asyncio.sleep(0.5)
    assert env.notifier.confirmations == []


# --------------------------------------------------------------------------- #
# Management
# --------------------------------------------------------------------------- #
def move_up(env: Env, price: float) -> None:
    for b in env.brokers:
        b.quote = Quote(bid=price, ask=price + 0.1, last=price)
    env.oracle.prices["ETH"] = price


async def test_move_sl_to_breakeven_via_reply(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    move_up(env, 2260.0)
    env.parser.push(result(action(ActionType.MOVE_SL, base_asset="ETH", move_sl_to_entry=True,
                                  used_message_ids=[600], summary_tr="ETH stop girişe")))
    await add_msg(env.db, 600, "stopu girişe çekiyorum", reply_to=502)
    await env.engine.on_messages_ready(CHAT, SENDER)

    _msgs, ctx = env.parser.calls[-1]
    assert ctx.reply_to is not None and ctx.reply_to.message_id == 502
    assert len(ctx.open_trades) == 1
    assert env.notifier.confirmations == []
    for b in env.brokers:
        calls = b.calls_of("update_stop_loss")
        assert len(calls) == 1 and calls[0][1] == 2232.90     # each position's own fill price
    trade = (await env.db.active_trades())[0]
    assert trade.current_stop_loss == 2232.18
    assert all(p.stop_loss == 2232.90 for p in await env.db.broker_positions(trade.id))
    assert (await env.db.get_message(CHAT, 600)).consumed  # type: ignore[union-attr]


async def test_ambiguous_management_asks(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    first = (await env.db.active_trades())[0]
    second_plan = first.plan.model_copy(update={"plan_id": "second", "entry_price": 2100.0, "stop_loss": 2050.0,
                                                "source_message_ids": [400]})
    await env.db.create_trade(second_plan, status=TradeStatus.OPEN)
    move_up(env, 2260.0)
    env.parser.push(result(action(ActionType.MOVE_SL, base_asset="ETH", move_sl_to_entry=True,
                                  used_message_ids=[600], confidence=0.5,
                                  ambiguities=["İki açık ETH işlemi var; hangisi?"])))
    await add_msg(env.db, 600, "eth stop girişe")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert len(env.notifier.confirmations) == 1
    conf = env.notifier.confirmations[0]
    assert conf.kind == "manage" and conf.plan is None
    assert "ambiguous_trade" in [i.code for i in conf.issues]
    assert all(not b.calls_of("update_stop_loss") for b in env.brokers)


async def test_partial_close_fraction_is_of_remaining_size(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    trade = (await env.db.active_trades())[0]
    for pos in await env.db.broker_positions(trade.id):   # TP1 already took half
        await env.db.save_broker_position(trade.id, pos.model_copy(update={"remaining_quantity": pos.filled_quantity / 2}))
    env.parser.push(result(action(ActionType.CLOSE, base_asset="ETH", close_fraction=0.5, target_trade_id=trade.id,
                                  used_message_ids=[601])))
    await add_msg(env.db, 601, "eth yarısını kapattım")
    await env.engine.on_messages_ready(CHAT, SENDER)
    for b in env.brokers:
        (_pos, fraction), = b.calls_of("close_position")
        assert fraction == pytest.approx(0.25)


async def test_close_all_closes_every_position(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    text = await env.engine.close_all()
    assert "kapatıldı" in text
    assert all(len(b.calls_of("close_position")) == 1 for b in env.brokers)
    assert await env.db.active_trades() == []


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #
async def test_paused_skips_signal(env: Env) -> None:
    await env.engine.set_paused(True)
    env.parser.push(result(eth_open()))
    await env.signal()
    assert env.opens() == 0 and env.notifier.confirmations == []
    assert any("Duraklatıldı" in t for t in env.notifier.texts(Severity.INFO))
    assert (await env.db.get_message(CHAT, 502)).consumed  # type: ignore[union-attr]
    await env.engine.set_paused(False)
    assert "kopyalanıyor" in await env.engine.status_text()


async def test_duplicate_signal_asks(env: Env) -> None:
    env.parser.push(result(eth_open()), result(eth_open(used_message_ids=[503, 504])))
    await env.signal()
    await add_msg(env.db, 503, media=IMAGE)
    await add_msg(env.db, 504, "eth 1R long")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.opens() == 2
    assert len(env.notifier.confirmations) == 1
    assert "duplicate_signal" in [i.code for i in env.notifier.confirmations[0].issues]


async def test_max_open_trades_skips(env: Env) -> None:
    env.cfg.risk.max_open_trades = 1
    env.parser.push(result(eth_open()), result(eth_open(base_asset="BTC", symbol="BTCUSDT.P", entry_price=None,
                                                        stop_loss=60000.0, take_profits=[], used_message_ids=[503])))
    await env.signal()
    await add_msg(env.db, 503, "btc long")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.opens() == 2
    assert any("sınır" in t for t in env.notifier.texts(Severity.WARNING))


async def test_daily_loss_guard_skips_that_broker(env: Env) -> None:
    old = await env.db.create_trade(eth_plan(), status=TradeStatus.CLOSED)
    await env.db.save_broker_position(old.id, BrokerPosition(
        broker="binance", broker_symbol="ETH/USDT:USDT", side=Side.LONG, status=PositionStatus.CLOSED,
        quantity=1.0, filled_quantity=1.0, realized_pnl=-1_100.0))   # > 10% of 10 000
    env.parser.push(result(eth_open()))
    await env.signal()
    assert env.a.opened == [] and len(env.b.opened) == 1
    assert any("günlük zarar" in t and "binance" in t for t in env.notifier.texts(Severity.WARNING))
    trade = (await env.db.active_trades())[0]
    assert trade.status is TradeStatus.OPEN


async def test_market_beyond_stop_on_approval_aborts(env: Env) -> None:
    env.parser.push(result(eth_open(confidence=0.7)))
    await env.signal()
    conf = env.notifier.confirmations[0]
    env.oracle.prices["ETH"] = 2200.0                  # fell through the 2205.11 stop meanwhile
    text = await env.engine.handle_confirmation(conf.id, True)
    assert "stop" in text and "açılmadı" in text
    assert env.opens() == 0
    assert (await env.db.get_confirmation(conf.id)).status is ConfirmationStatus.REJECTED  # type: ignore[union-attr]


async def test_broker_live_price_beyond_stop_skips_that_broker(env: Env) -> None:
    env.b.quote = Quote(bid=2204.0, ask=2204.2, last=2204.1)
    env.parser.push(result(eth_open()))
    await env.signal()
    assert len(env.a.opened) == 1 and env.b.opened == []
    assert any("bybit" in t and "stop" in t for t in env.notifier.texts(Severity.WARNING))


# --------------------------------------------------------------------------- #
# Broker failures
# --------------------------------------------------------------------------- #
async def test_partial_broker_failure_reports_error_and_trade_opens(env: Env) -> None:
    env.b.fail["open_position"] = BrokerError("Yetersiz teminat")
    env.parser.push(result(eth_open()))
    await env.signal()
    errors = env.notifier.texts(Severity.ERROR)
    assert len(errors) == 1 and "bybit" in errors[0] and "Yetersiz teminat" in errors[0]
    trade = (await env.db.active_trades())[0]
    assert trade.status is TradeStatus.OPEN
    assert [p.broker for p in await env.db.broker_positions(trade.id)] == ["binance"]


async def test_all_brokers_fail_marks_trade_failed(env: Env) -> None:
    for b in env.brokers:
        b.fail["open_position"] = BrokerError("borsa kapalı")
    env.parser.push(result(eth_open()))
    await env.signal()
    trades = await env.db.recent_trades(now() - timedelta(minutes=1))
    assert len(trades) == 1 and trades[0].status is TradeStatus.FAILED
    assert any("hiçbir brokerda açılamadı" in t for t in env.notifier.texts(Severity.ERROR))


async def test_no_broker_lists_asset_warns(env: Env) -> None:
    for b in env.brokers:
        b.unsupported.add("ETH")
    env.parser.push(result(eth_open()))
    await env.signal()
    assert any("hiçbir broker" in t for t in env.notifier.texts(Severity.WARNING))
    assert env.notifier.texts(Severity.ERROR) == []


async def test_broker_connect_failure_is_excluded(db: Database) -> None:
    good, bad = FakeBroker("good"), FakeBroker("bad", fail={"connect": BrokerError("API anahtarı hatalı")})
    notifier = FakeNotifier()
    parser = FakeParser(result(eth_open()))
    engine = Engine(make_config(), db, parser, [good, bad], notifier, price_oracle=FakePriceOracle({"ETH": 2232.85}))
    await engine.start(monitor=False)
    try:
        assert [b.name for b in engine.brokers] == ["good"]
        assert any("bad bağlanamadı" in t for t in notifier.texts(Severity.ERROR))
        assert any("DRY-RUN" in t and "good" in t for t in notifier.texts(Severity.INFO))
        await add_msg(db, 501, media=IMAGE)
        await add_msg(db, 502, "eth 1R long")
        await engine.on_messages_ready(CHAT, SENDER)
        assert len(good.opened) == 1 and bad.opened == []
    finally:
        await engine.stop()
    assert good.closed


# --------------------------------------------------------------------------- #
# Edits
# --------------------------------------------------------------------------- #
async def test_edit_of_executed_signal_asks_manage_confirmation(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    edited = StoredMessage(chat_id=CHAT, message_id=502, sender_id=SENDER, date=now(),
                           text="eth 1R long sl 2210", edited=True)
    await env.db.save_message(edited)
    env.parser.push(result(eth_open(stop_loss=2210.0, confidence=0.8)))
    await env.engine.on_message_edited(edited)
    assert len(env.notifier.confirmations) == 1
    conf = env.notifier.confirmations[0]
    assert conf.kind == "manage" and isinstance(conf.plan, ManagementPlan)
    assert conf.plan.action is ActionType.MOVE_SL and conf.plan.new_stop_loss == 2210.0
    assert "signal_edited" in [i.code for i in conf.issues]
    assert all(not b.calls_of("update_stop_loss") for b in env.brokers)   # never auto-applied

    await env.engine.handle_confirmation(conf.id, True)
    assert all(b.calls_of("update_stop_loss")[0][1] == 2210.0 for b in env.brokers)
    assert (await env.db.active_trades())[0].current_stop_loss == 2210.0


async def test_edit_of_pending_confirmation_supersedes_it(env: Env) -> None:
    env.parser.push(result(eth_open(confidence=0.7)))
    await env.signal()
    first = env.notifier.confirmations[0]
    edited = StoredMessage(chat_id=CHAT, message_id=502, sender_id=SENDER, date=now(), text="eth 0.5R long",
                           edited=True)
    await env.db.save_message(edited)
    env.parser.push(result(eth_open(risk_r=0.5)))
    await env.engine.on_message_edited(edited)
    assert (await env.db.get_confirmation(first.id)).status is ConfirmationStatus.SUPERSEDED  # type: ignore[union-attr]
    second = env.notifier.confirmations[1]
    assert isinstance(second.plan, TradePlan) and second.plan.risk_pct == 1.0
    assert "signal_edited" in [i.code for i in second.issues]


async def test_edit_of_unconsumed_message_reprocesses(env: Env) -> None:
    env.cfg.telegram.incomplete_timeout_sec = 60  # type: ignore[assignment]
    env.parser.push(result(eth_incomplete(used_message_ids=[501])), result(eth_open(used_message_ids=[501])))
    msg = await add_msg(env.db, 501, "eth long")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.opens() == 0
    edited = msg.model_copy(update={"text": "eth 1R long sl 2205.11 tp 2310.6", "edited": True})
    await env.db.save_message(edited)
    await env.engine.on_message_edited(edited)
    assert env.opens() == 2


# --------------------------------------------------------------------------- #
# Monitor
# --------------------------------------------------------------------------- #
async def test_monitor_rolls_trade_to_closed(env: Env) -> None:
    env.cfg.notifier.notify_on_execution = True
    env.parser.push(result(eth_open()))
    await env.signal()
    env.notifier.notifications.clear()

    def stopped_out(pos: BrokerPosition) -> BrokerPosition:
        return pos.model_copy(update={"status": PositionStatus.CLOSED, "remaining_quantity": 0.0,
                                      "realized_pnl": -(pos.risk_amount or 0.0)})

    for b in env.brokers:
        b.sync_fn = stopped_out
    await env.engine.monitor.run_once()
    trades = await env.db.recent_trades(now() - timedelta(minutes=1))
    assert trades[0].status is TradeStatus.CLOSED
    infos = env.notifier.texts(Severity.INFO)
    assert len(infos) == 1 and "kapandı" in infos[0] and "-200.00" in infos[0]
    assert await env.db.active_broker_positions() == []


async def test_monitor_survives_sync_errors_and_warns_once(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    env.a.fail["sync_position"] = RuntimeError("timeout")
    for _ in range(7):
        await env.engine.monitor.run_once()
    warnings = env.notifier.texts(Severity.WARNING)
    assert len(warnings) == 1 and "binance" in warnings[0]
    assert len(env.b.calls_of("sync_position")) == 7


async def test_monitor_cancels_stale_pending_entry(env: Env) -> None:
    env.cfg.risk.pending_entry_expiry_min = 1
    env.parser.push(result(eth_open(entry_type=EntryType.LIMIT, entry_price=2220.0)))
    await env.signal()
    trade = (await env.db.active_trades())[0]
    assert trade.status is TradeStatus.PENDING
    old_ms = int((now() - timedelta(minutes=5)).timestamp() * 1000)
    for pos in await env.db.broker_positions(trade.id):
        await env.db.save_broker_position(trade.id, pos.model_copy(update={"data": {**pos.data, "opened_at": old_ms}}))
    await env.engine.monitor.run_once()
    assert all(len(b.calls_of("cancel_entry")) == 1 for b in env.brokers)
    assert (await env.db.get_trade(trade.id)).status is TradeStatus.CANCELLED  # type: ignore[union-attr]


# --------------------------------------------------------------------------- #
# Parser failures / prefilter / none
# --------------------------------------------------------------------------- #
def transient_error() -> ParserError:
    err = ParserError("Yapay zekâ servisine bağlanılamadı.")
    err.__cause__ = TimeoutError("read timeout")
    return err


async def test_transient_parser_error_retries_then_executes(env: Env) -> None:
    env.engine.retry_delays = (0.05,)
    env.parser.push(transient_error(), result(eth_open()))
    await env.signal()
    assert env.opens() == 0 and not (await env.db.get_message(CHAT, 501)).consumed  # type: ignore[union-attr]
    await wait_for(lambda: env.opens() == 2)
    assert len(env.notifier.texts(Severity.WARNING)) == 1


async def test_non_transient_parser_error_consumes_and_warns(env: Env) -> None:
    env.parser.push(ParserError("Model yanıtı okunamadı."))
    await env.signal()
    warnings = env.notifier.texts(Severity.WARNING)
    assert len(warnings) == 1 and "eth 1R long" in warnings[0]
    assert (await env.db.get_message(CHAT, 502)).consumed  # type: ignore[union-attr]


def test_transient_detection() -> None:
    assert is_transient_parse_error(transient_error())
    assert not is_transient_parse_error(ParserError("bad json"))
    flagged = ParserError("x")
    flagged.transient = True  # type: ignore[attr-defined]
    assert is_transient_parse_error(flagged)


async def test_prefilter_skips_chatter_without_llm(env: Env) -> None:
    await add_msg(env.db, 700, "günaydın")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.parser.calls == []
    assert (await env.db.get_message(CHAT, 700)).consumed  # type: ignore[union-attr]


async def test_none_action_is_consumed_silently(env: Env) -> None:
    env.parser.push(result(action(ActionType.NONE, confidence=0.98, used_message_ids=[])))
    await add_msg(env.db, 701, "ETH 2300'ü kırarsa uçar")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.notifier.notifications == [] and env.notifier.confirmations == []
    assert (await env.db.get_message(CHAT, 701)).consumed  # type: ignore[union-attr]


async def test_unsure_none_asks_owner(env: Env) -> None:
    env.parser.push(result(action(ActionType.NONE, confidence=0.4, used_message_ids=[702],
                                  ambiguities=["Bu bir işlem talimatı olabilir."])))
    await add_msg(env.db, 702, "eth burdan güzel")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert len(env.notifier.confirmations) == 1 and env.notifier.confirmations[0].plan is None


# --------------------------------------------------------------------------- #
# Owner commands
# --------------------------------------------------------------------------- #
async def test_test_parse_never_executes(env: Env) -> None:
    env.parser.push(result(eth_open(used_message_ids=[0])))
    text = await env.engine.test_parse("eth 1R long giriş 2232.18 sl 2205.11 tp 2310.6")
    assert "Otomatik uygulanırdı" in text and "2205.11" in text
    assert env.opens() == 0 and await env.db.active_trades() == []
    assert env.parser.calls[0][0][0].message_id == 0 and env.parser.calls[0][0][0].chat_id == 0


async def test_status_positions_balances(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    status = await env.engine.status_text()
    assert "DRY-RUN" in status and "Aktif işlem: 1" in status
    positions = await env.engine.positions_text()
    assert "binance" in positions and "ETH" in positions and "2232.9" in positions
    balances = await env.engine.balances_text()
    assert "10 000.00 USDT" in balances and "5 000.00 USDT" in balances


# --------------------------------------------------------------------------- #
# Price oracle
# --------------------------------------------------------------------------- #
class _PublicExchange:
    def __init__(self, ticker: dict | None = None, fail: bool = False) -> None:
        self.ticker = ticker or {"bid": 100.0, "ask": 102.0}
        self.fail = fail
        self.calls: list[str] = []
        self.closed = False

    async def fetch_ticker(self, symbol: str) -> dict:
        self.calls.append(symbol)
        if self.fail:
            raise RuntimeError("down")
        return self.ticker

    async def close(self) -> None:
        self.closed = True


async def test_price_oracle_prefers_broker_mid_and_caches() -> None:
    broker = FakeBroker("x", quote=Quote(bid=10.0, ask=12.0, last=11.0))
    public = _PublicExchange()
    oracle = PriceOracle([broker], public_exchange=public)
    assert await oracle.price("ETH") == 11.0
    assert await oracle.price("ETH") == 11.0
    assert len(broker.calls_of("resolve_symbol")) == 1        # cached
    await oracle.price("ETH", fresh=True)
    assert len(broker.calls_of("resolve_symbol")) == 2
    assert public.calls == []


async def test_price_oracle_falls_back_to_public_and_never_raises() -> None:
    broker = FakeBroker("x", unsupported={"SOL", "XAU"})
    public = _PublicExchange()
    oracle = PriceOracle([broker], public_exchange=public)
    assert await oracle.price("SOL") == 101.0
    assert public.calls == ["SOL/USDT:USDT"]
    assert await oracle.price("XAU", market_type=MarketType.CFD) is None   # never a crypto proxy for CFDs
    failing = PriceOracle([FakeBroker("y", fail={"resolve_symbol": RuntimeError("x")})],
                          public_exchange=_PublicExchange(fail=True))
    assert await failing.price("BTC") is None
    assert await failing.price(None) is None


async def test_restart_flags_interrupted_openings(db: Database) -> None:
    stuck = await db.create_trade(eth_plan(plan_id="stuck"))                 # OPENING, nothing saved
    half = await db.create_trade(eth_plan(plan_id="half", source_message_ids=[101]))
    await db.save_broker_position(half.id, BrokerPosition(
        broker="binance", broker_symbol="ETH/USDT:USDT", side=Side.LONG, status=PositionStatus.OPEN,
        quantity=1.0, filled_quantity=1.0, remaining_quantity=1.0))
    notifier = FakeNotifier()
    engine = Engine(make_config(), db, FakeParser(), [FakeBroker("binance")], notifier, price_oracle=FakePriceOracle())
    await engine.start(monitor=False)
    try:
        assert (await db.get_trade(stuck.id)).status is TradeStatus.FAILED  # type: ignore[union-attr]
        assert (await db.get_trade(half.id)).status is TradeStatus.OPEN  # type: ignore[union-attr]
        # (the one-time "allowed_sender_ids is empty" startup warning is not about the restart)
        warnings = [w for w in notifier.texts(Severity.WARNING) if "allowed_sender_ids" not in w]
        assert len(warnings) == 2 and all("elle kontrol" in w for w in warnings)
    finally:
        await engine.stop()


async def test_incomplete_watch_survives_unrelated_open_in_same_batch(env: Env) -> None:
    env.cfg.telegram.incomplete_timeout_sec = 0.2  # type: ignore[assignment]
    btc = eth_incomplete(base_asset="BTC", used_message_ids=[503], summary_tr="BTC long, stop yok")
    env.parser.push(result(btc, eth_open()))
    await add_msg(env.db, 501, media=IMAGE)
    await add_msg(env.db, 502, "eth 1R long")
    await add_msg(env.db, 503, "btc long")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.opens() == 2
    assert not (await env.db.get_message(CHAT, 503)).consumed  # type: ignore[union-attr]
    await wait_for(lambda: len(env.notifier.confirmations) == 1)
    assert env.notifier.confirmations[0].kind == "incomplete"
    assert env.notifier.confirmations[0].action.base_asset == "BTC"


# --------------------------------------------------------------------------- #
# Replies to a trade's signal (also without forum topics)
# --------------------------------------------------------------------------- #
async def test_close_reply_to_an_already_closed_trade_is_ignored(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    trade = (await env.db.active_trades())[0]
    for pos in await env.db.broker_positions(trade.id):
        await env.db.save_broker_position(trade.id, pos.model_copy(update={"status": PositionStatus.CLOSED,
                                                                           "remaining_quantity": 0.0}))
    await env.db.update_trade(trade.id, status=TradeStatus.CLOSED)
    env.parser.push(result(action(ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=502,
                                  used_message_ids=[600], ambiguities=["Açık işlem bulunamadı."])))
    await add_msg(env.db, 600, "Stop", reply_to=502)
    await env.engine.on_messages_ready(CHAT, SENDER, None)          # topic_id=None: same stream as before
    assert env.notifier.confirmations == [] and env.notifier.notifications == []
    assert all(b.calls_of("close_position") == [] for b in env.brokers)
    assert (await env.db.get_message(CHAT, 600)).consumed  # type: ignore[union-attr]


async def test_reply_to_one_trade_read_as_another_open_trade_is_asked(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    first = (await env.db.active_trades())[0]
    second = await env.db.create_trade(first.plan.model_copy(update={"plan_id": "second", "source_message_ids": [400]}),
                                       status=TradeStatus.OPEN)
    env.parser.push(result(action(ActionType.CLOSE, close_fraction=1.0, target_trade_id=second.id,
                                  base_asset="ETH", used_message_ids=[600])))
    await add_msg(env.db, 600, "kapattım", reply_to=502)            # 502 is the FIRST trade's signal
    await env.engine.on_messages_ready(CHAT, SENDER)
    (conf,) = env.notifier.confirmations
    assert "reply_trade_mismatch" in [i.code for i in conf.issues]
    assert all(b.calls_of("close_position") == [] for b in env.brokers)
