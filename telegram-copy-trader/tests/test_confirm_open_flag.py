"""confirm_open: false (the friend's tickmill account): a signal the bot would ask about ("... AÇAYIM MI?") is opened
on that account at once, as read; the question stays for the route's other accounts. Evet adds them to the same
trade (tickmill never twice), Hayır / expiry leaves tickmill's position as a normal trade. Real in-memory Database."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from copytrader.config import AppConfig, NotifierConfig, PaperBrokerConfig, TelegramConfig, TopicRoute
from copytrader.engine.monitor import is_virtual_pending
from copytrader.engine.orchestrator import Engine
from copytrader.models import (
    ActionType,
    ConfirmationStatus,
    EntryType,
    MarketType,
    ParsedAction,
    ParseResult,
    PositionStatus,
    Quote,
    Side,
    StoredMessage,
    TakeProfit,
    TradeStatus,
)
from copytrader.storage.db import Database

from fakes import FakeBroker, FakeNotifier, FakeParser, FakePriceOracle

CHAT = -1001111111111
KADIR = 900
CRYPTO, FX = 10, 20
TITLES = {CRYPTO: "💰 Kasım Ayı Kripto İşlemleri", FX: "💱 Kasım Ayı Fx İşlemleri"}
ROUTES = [
    TopicRoute(name="kripto", title_contains=["Kripto İşlemleri"], brokers=["binance", "tickmill"], kind="crypto"),
    TopicRoute(name="fx", title_contains=["Fx İşlemleri"], brokers=["fx", "tickmill"], kind="fx"),
]


def now() -> datetime:
    return datetime.now(timezone.utc)


def result(*actions: ParsedAction) -> ParseResult:
    return ParseResult(actions=list(actions), model="fake")


def eth_short(**kw: Any) -> ParsedAction:
    data: dict[str, Any] = dict(
        action=ActionType.OPEN, symbol="ETHUSDT.P", base_asset="ETH", quote_asset="USDT",
        market_type=MarketType.PERP, side=Side.SHORT, entry_type=EntryType.MARKET, entry_price=2239.2,
        stop_loss=2250.6, take_profits=[TakeProfit(price=2205.11)], risk_r=1.0, used_message_ids=[610, 611],
        confidence=0.5, summary_tr="ETH SHORT, giriş 2239.20, SL 2250.60, TP 2205.11, risk 1R",
    )
    data.update(kw)
    return ParsedAction(**data)


def eur_long(**kw: Any) -> ParsedAction:
    data: dict[str, Any] = dict(
        action=ActionType.OPEN, symbol="EURUSD", base_asset="EUR", quote_asset="USD", market_type=MarketType.FOREX,
        side=Side.LONG, entry_type=EntryType.LIMIT, entry_price=1.0840, stop_loss=1.0800,
        take_profits=[TakeProfit(price=1.0950)], risk_r=1.0, used_message_ids=[700, 701], confidence=0.5,
        summary_tr="EURUSD LONG limit 1.0840, SL 1.0800, TP 1.0950, risk 1R",
    )
    data.update(kw)
    return ParsedAction(**data)


class FakeReference:
    def __init__(self, prices: dict[str, float | None]) -> None:
        self.prices = prices

    async def mid(self, exchange_id: str, base_asset: str, *, fresh: bool = False) -> float | None:
        return self.prices.get(base_asset.upper())


class FriendNotifier(FakeNotifier):
    def __init__(self) -> None:
        super().__init__()
        self.friend: list[str] = []

    async def friend_note(self, text: str) -> None:
        self.friend.append(text)


class Env:
    def __init__(self, db: Database, parser: FakeParser, binance: FakeBroker, fx: FakeBroker, tm: FakeBroker,
                 notifier: FriendNotifier, ref: FakeReference, engine: Engine) -> None:
        self.db, self.parser, self.engine = db, parser, engine
        self.binance, self.fx, self.tm, self.notifier, self.ref = binance, fx, tm, notifier, ref

    async def msg(self, mid: int, text: str = "", *, topic: int, reply_to: int | None = None) -> None:
        await self.db.save_message(StoredMessage(
            chat_id=CHAT, message_id=mid, sender_id=KADIR, sender_name="Kadir", date=now(), text=text,
            reply_to_message_id=reply_to if reply_to is not None else topic, topic_id=topic,
            topic_title=TITLES[topic]))

    async def crypto_signal(self, action: ParsedAction | None = None) -> None:
        self.parser.push(result(action or eth_short()))
        await self.msg(610, "chart", topic=CRYPTO)
        await self.msg(611, "Eth short 1R", topic=CRYPTO)
        await self.engine.on_messages_ready(CHAT, KADIR, CRYPTO)

    async def fx_signal(self, action: ParsedAction | None = None) -> None:
        self.parser.push(result(action or eur_long()))
        await self.msg(700, "chart", topic=FX)
        await self.msg(701, "EUR LONG 1R Limit Emir", topic=FX)
        await self.engine.on_messages_ready(CHAT, KADIR, FX)

    async def positions(self, trade_id: int) -> dict[str, Any]:
        return {p.broker: p for p in await self.db.broker_positions(trade_id)}


async def make_env(db: Database, *, flag: bool = True, virtual: bool = False) -> Env:
    cfg = AppConfig(
        telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[CHAT], topics=list(ROUTES)),
        notifier=NotifierConfig(bot_token="123:abc", owner_user_id=42),
        brokers=[PaperBrokerConfig(type="paper", name=n) for n in ("binance", "fx", "tickmill")],
    )
    cfg.monitor_interval_sec = 3600.0
    parser = FakeParser()
    binance = FakeBroker("binance", quote=Quote(bid=2239.1, ask=2239.3, last=2239.2))
    fx = FakeBroker("fx", qty_step=0.01)
    fx.quotes["EUR"] = Quote(bid=1.0850, ask=1.0851, last=1.08505)
    tm = FakeBroker("tickmill", qty_step=0.01, optional=True, quote=Quote(bid=2239.1, ask=2239.3, last=2239.2))
    tm.quotes["EUR"] = Quote(bid=1.0850, ask=1.0851, last=1.08505)
    tm.config.risk_usd = 5.0
    if flag:
        tm.config.confirm_open = False
    ref = FakeReference({"ETH": 2239.2})
    if virtual:
        tm.config.reference_exchange = "binanceusdm"
        tm.config.entry_trigger = "reference_touch"     # type: ignore[assignment]
    notifier = FriendNotifier()
    oracle = FakePriceOracle({"ETH": 2239.2, "EUR": 1.08505})
    engine = Engine(cfg, db, parser, [binance, fx, tm], notifier, price_oracle=oracle, reference_prices=ref)
    await engine.start(monitor=False)
    notifier.notifications.clear()
    return Env(db, parser, binance, fx, tm, notifier, ref, engine)


@pytest.fixture
async def db():
    database = Database(":memory:")
    await database.open()
    yield database
    await database.close()


def test_config_accepts_the_flag() -> None:
    assert PaperBrokerConfig(type="paper", name="x").confirm_open is True
    assert PaperBrokerConfig(type="paper", name="x", confirm_open=False).confirm_open is False


async def test_question_asked_and_tickmill_opened_at_once(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        assert conf.kind == "open" and conf.early is not None
        assert env.binance.opened == []
        (req,) = env.tm.opened
        assert req.entry_type is EntryType.MARKET and req.stop_loss == 2250.6     # as read
        trade = await db.find_trade_by_message(CHAT, 611)
        assert trade is not None and trade.status is TradeStatus.OPEN and conf.early["trade_id"] == trade.id
        assert set(await env.positions(trade.id)) == {"tickmill"}
        # the owner is told, with the short line (also when execution reports are off)
        report = next(t for t in env.notifier.texts() if "tickmill" in t and "açıldı" in t)
        assert "onay beklenmedi" in report and f"#{conf.id}" in report
        # the friend's feed sees his account's open as for any open
        assert any("tickmill" in t for t in env.notifier.friend)
        # the question names it and says the expiry concerns the other accounts
        from copytrader.telegram.formatting import format_confirmation
        text = format_confirmation(conf)
        assert "AÇAYIM MI" in text and "onay beklenmeden" in text and "binance" in text
        assert "diğer hesaplarda işlem açılmaz" in text
    finally:
        await env.engine.stop()


async def test_yes_opens_others_into_same_trade_tickmill_not_twice(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        out = await env.engine.handle_confirmation(conf.id, True)
        assert len(env.tm.opened) == 1 and len(env.binance.opened) == 1
        trades = await db.recent_trades(now() - timedelta(hours=1))
        assert len(trades) == 1
        (trade,) = trades
        assert set(await env.positions(trade.id)) == {"binance", "tickmill"}
        assert trade.status is TradeStatus.OPEN
        assert "tekrar gönderilmedi" in out
        stored = await db.get_confirmation(conf.id)
        assert stored is not None and stored.status is ConfirmationStatus.APPROVED
        # a later stop move reaches both
        env.parser.push(result(ParsedAction(action=ActionType.MOVE_SL, new_stop_loss=2245.0, refers_to_message_id=611,
                                            used_message_ids=[612], confidence=0.95, summary_tr="stop 2245")))
        await env.msg(612, "stop 2245", topic=CRYPTO, reply_to=611)
        await env.engine.on_messages_ready(CHAT, KADIR, CRYPTO)
        assert env.tm.calls_of("update_stop_loss") and env.binance.calls_of("update_stop_loss")
    finally:
        await env.engine.stop()


async def test_no_leaves_tickmill_and_others_not_opened_then_managed(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        out = await env.engine.handle_confirmation(conf.id, False)
        assert "diğer hesaplarda işlem açılmadı" in out and "açık kalır" in out
        assert env.binance.opened == []
        trade = await db.find_trade_by_message(CHAT, 611)
        assert trade is not None and trade.status is TradeStatus.OPEN
        # the trader closes it: tickmill's position is closed like any trade's
        env.parser.push(result(ParsedAction(action=ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=611,
                                            base_asset="ETH", side=Side.SHORT, used_message_ids=[613],
                                            confidence=0.95, summary_tr="kapat")))
        await env.msg(613, "kapattık", topic=CRYPTO, reply_to=611)
        await env.engine.on_messages_ready(CHAT, KADIR, CRYPTO)
        assert env.tm.calls_of("close_position") and env.binance.calls_of("close_position") == []
        assert (await env.positions(trade.id))["tickmill"].status is PositionStatus.CLOSED
    finally:
        await env.engine.stop()


async def test_expiry_leaves_tickmill(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        stored = await db.get_confirmation(conf.id)
        assert stored is not None
        stored.expires_at = now() - timedelta(seconds=5)
        await db.save_confirmation(stored)
        await env.engine._expire(conf.id)
        text = env.notifier.closed_text(conf.id)
        assert text is not None and "Süre doldu" in text and "açık kalır" in text
        assert env.binance.opened == [] and len(env.tm.opened) == 1
    finally:
        await env.engine.stop()


async def test_flag_absent_everything_waits(db: Database) -> None:
    env = await make_env(db, flag=False)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        assert conf.early is None and env.tm.opened == [] and env.binance.opened == []
        assert await db.find_trade_by_message(CHAT, 611) is None
        await env.engine.handle_confirmation(conf.id, True)
        assert len(env.tm.opened) == 1 and len(env.binance.opened) == 1
    finally:
        await env.engine.stop()


async def test_untradeable_plan_not_opened_on_tickmill(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal(eth_short(stop_loss=None))
        (conf,) = env.notifier.confirmations
        assert conf.plan is None and conf.early is None
        assert env.tm.opened == [] and await db.find_trade_by_message(CHAT, 611) is None
    finally:
        await env.engine.stop()


async def test_doubt_about_existing_trade_not_opened_on_tickmill(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal(eth_short(confidence=0.95))           # opened everywhere, no question
        assert len(env.tm.opened) == 1 and len(env.binance.opened) == 1
        env.parser.push(result(eth_short(used_message_ids=[620])))     # a repost: duplicate question
        await env.msg(620, "Eth short 1R", topic=CRYPTO)
        await env.engine.on_messages_ready(CHAT, KADIR, CRYPTO)
        (conf,) = env.notifier.confirmations
        assert any(i.code == "duplicate_signal" for i in conf.issues) and conf.early is None
        assert len(env.tm.opened) == 1
    finally:
        await env.engine.stop()


async def test_crypto_virtual_trigger_fires_while_binance_waits(db: Database) -> None:
    """The kripto route's tickmill reference is the public reference exchange (not the binance leg): its virtual
    trigger is set and fires while binance still waits for the owner's answer."""
    env = await make_env(db, virtual=True)
    try:
        env.ref.prices["ETH"] = 2230.0
        await env.crypto_signal(eth_short(entry_type=EntryType.LIMIT, entry_price=2245.0))
        (conf,) = env.notifier.confirmations
        trade = await db.find_trade_by_message(CHAT, 611)
        assert trade is not None and trade.status is TradeStatus.PENDING
        pos = (await env.positions(trade.id))["tickmill"]
        assert is_virtual_pending(pos) and env.tm.opened == [] and env.binance.opened == []
        assert "sanal tetiği kuruldu" in (conf.early or {}).get("line", "")
        env.ref.prices["ETH"] = 2245.5                                  # touched (short: price >= entry)
        await env.engine.monitor.run_virtual_once()
        (req,) = env.tm.opened
        assert req.entry_type is EntryType.MARKET and env.binance.opened == []
        assert (await env.positions(trade.id))["tickmill"].status is PositionStatus.OPEN
        await env.engine.handle_confirmation(conf.id, True)
        assert len(env.tm.opened) == 1 and len(env.binance.opened) == 1
    finally:
        await env.engine.stop()


async def test_fx_route_limit_rests_on_tickmill_while_fx_waits(db: Database) -> None:
    """The fx route has no reference trigger in this version: tickmill takes the fx LIMIT signal as a resting
    broker limit order at once (no FundingPips fill is waited for), while fx waits for the owner's answer."""
    env = await make_env(db, virtual=True)
    try:
        await env.fx_signal()
        (conf,) = env.notifier.confirmations
        (req,) = env.tm.opened
        assert req.entry_type is EntryType.LIMIT and req.entry_price == pytest.approx(1.0840)
        assert env.fx.opened == []
        trade = await db.find_trade_by_message(CHAT, 701)
        assert trade is not None and trade.status is TradeStatus.PENDING
        pos = (await env.positions(trade.id))["tickmill"]
        assert pos.status is PositionStatus.PENDING and not is_virtual_pending(pos)
        await env.engine.handle_confirmation(conf.id, True)
        assert len(env.tm.opened) == 1 and len(env.fx.opened) == 1
        assert set(await env.positions(trade.id)) == {"fx", "tickmill"}
    finally:
        await env.engine.stop()


async def test_yes_after_tickmill_trade_closed_does_not_open_others(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        trade = await db.find_trade_by_message(CHAT, 611)
        assert trade is not None
        await db.update_trade(trade.id, status=TradeStatus.CLOSED)
        out = await env.engine.handle_confirmation(conf.id, True)
        assert "onay beklerken kapandı" in out and env.binance.opened == [] and len(env.tm.opened) == 1
    finally:
        await env.engine.stop()


async def test_correction_carries_the_early_trade(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        await env.engine.request_correction(conf.id)
        env.parser.push(result(eth_short(confidence=0.95)))
        await env.engine.handle_correction(conf.id, "doğru, aç")
        new = env.notifier.confirmations[-1]
        assert new.id != conf.id and new.early == conf.early
        assert not any(i.code == "message_already_traded" for i in new.issues)
        await env.engine.handle_confirmation(new.id, True)
        assert len(env.tm.opened) == 1 and len(env.binance.opened) == 1
        assert len(await db.recent_trades(now() - timedelta(hours=1))) == 1
    finally:
        await env.engine.stop()


async def test_stop_moved_while_asking_then_yes_opens_others_with_the_trades_stop(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        env.parser.push(result(ParsedAction(action=ActionType.MOVE_SL, new_stop_loss=2245.0, refers_to_message_id=611,
                                            used_message_ids=[612], confidence=0.95, summary_tr="stop 2245")))
        await env.msg(612, "stop 2245", topic=CRYPTO, reply_to=611)
        await env.engine.on_messages_ready(CHAT, KADIR, CRYPTO)
        assert env.tm.calls_of("update_stop_loss") and env.binance.opened == []
        await env.engine.handle_confirmation(conf.id, True)
        (req,) = env.binance.opened
        assert req.stop_loss == 2245.0 and len(env.tm.opened) == 1
    finally:
        await env.engine.stop()


async def test_slow_tickmill_is_late_then_yes_never_sends_it_twice(db: Database, monkeypatch: Any) -> None:
    import asyncio

    from copytrader.engine import orchestrator

    monkeypatch.setattr(orchestrator, "OPTIONAL_LEG_GRACE_SEC", 0.05)
    env = await make_env(db)
    gate = asyncio.Event()
    real_open = env.tm.open_position

    async def slow_open(req: Any) -> Any:
        await gate.wait()
        return await real_open(req)

    env.tm.open_position = slow_open      # type: ignore[method-assign]
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        assert conf.early is not None and conf.early["done"] == ["tickmill"]
        assert "yanıtı bekleniyor" in conf.early["line"]
        trade = await db.find_trade_by_message(CHAT, 611)
        assert trade is not None and trade.status is TradeStatus.OPENING
        gate.set()
        for _ in range(100):
            await asyncio.sleep(0.01)
            if "tickmill" in await env.positions(trade.id):
                break
        await env.engine.handle_confirmation(conf.id, True)
        assert len(env.tm.opened) == 1 and len(env.binance.opened) == 1
        assert set(await env.positions(trade.id)) == {"binance", "tickmill"}
        after = await db.get_trade(trade.id)
        assert after is not None and after.status is TradeStatus.OPEN
    finally:
        gate.set()
        await env.engine.stop()
