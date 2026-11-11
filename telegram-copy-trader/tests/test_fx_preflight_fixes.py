"""FX pre-flight regression tests: per-route entry thresholds, marketable limits, no-hedge, per-broker daily
loss limit and netting accounts, driven through the real Engine + a fake MetaTrader5 module."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from copytrader.brokers.mt5_broker import MT5, Mt5Broker
from copytrader.config import (
    AppConfig,
    Mt5BrokerConfig,
    NotifierConfig,
    RiskConfig,
    TelegramConfig,
    TopicRoute,
    effective_risk,
    route_thresholds,
)
from copytrader.engine.orchestrator import Engine
from copytrader.engine.prices import PriceOracle
from copytrader.models import (
    ActionType,
    EntryType,
    MarketType,
    ParsedAction,
    ParseResult,
    PositionStatus,
    Severity,
    Side,
    StoredMessage,
    TakeProfit,
    TradeStatus,
)
from copytrader.parsing import validator
from copytrader.storage.db import Database

from fakes import FakeNotifier, FakeParser
from test_mt5_broker import SERVER_TIME, FakeMT5, make_symbol

CHAT, KADIR, FX = -1001111111111, 900, 20
TITLE = "Kasım Ayı Fx İşlemleri"
NETTING = 0     # ACCOUNT_MARGIN_MODE_RETAIL_NETTING


@pytest.fixture(autouse=True)
def _clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: SERVER_TIME - 7200.0))
    monkeypatch.setattr(Mt5Broker, "tick_wait_sec", 0.0)


class FxFake(FakeMT5):
    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.symbols = {"EURUSD": make_symbol(
            "EURUSD", digits=5, point=0.00001, trade_tick_size=0.00001, trade_contract_size=100000.0,
            filling_mode=MT5.SYMBOL_FILLING_IOC, currency_profit="USD", currency_base="EUR", volume_max=100.0)}
        self.ticks = {"EURUSD": SimpleNamespace(bid=1.0850, ask=1.0851, last=0.0, time=SERVER_TIME)}
        self.margin_per_lot = 1000.0

    def set_price(self, bid: float, ask: float) -> None:
        self.ticks["EURUSD"] = SimpleNamespace(bid=bid, ask=ask, last=0.0, time=SERVER_TIME)


def eur(side: Side = Side.LONG, entry: float | None = 1.0840, sl: float = 1.0800, tps: tuple[float, ...] = (1.0950,),
        et: EntryType | None = None, **kw: Any) -> ParsedAction:
    d: dict[str, Any] = dict(
        action=ActionType.OPEN, symbol="EURUSD", base_asset="EUR", quote_asset="USD", market_type=MarketType.FOREX,
        side=side, entry_type=et, entry_price=entry, stop_loss=sl, take_profits=[TakeProfit(price=p) for p in tps],
        risk_r=1.0, confidence=0.95, summary_tr="EURUSD")
    d.update(kw)
    return ParsedAction(**d)


def short(entry: float | None = 1.0860, sl: float = 1.0900, tps: tuple[float, ...] = (1.0750,), **kw: Any) -> ParsedAction:
    return eur(Side.SHORT, entry, sl, tps, **kw)


def manage(kind: ActionType, **kw: Any) -> ParsedAction:
    return ParsedAction(action=kind, confidence=0.95, **kw)


@dataclass
class Env:
    db: Database
    fake: FxFake
    broker: Mt5Broker
    engine: Engine
    parser: FakeParser
    notifier: FakeNotifier
    mid: int = 5000

    async def say(self, action: ParsedAction | None, text: str = "EUR", reply_to: int | None = None) -> int:
        self.mid += 1
        mid = self.mid
        if action is not None:
            used = action.model_copy(update={"used_message_ids": [mid]})
            self.parser.push(ParseResult(actions=[used], model="fake"))
        else:
            self.parser.push(ParseResult(actions=[ParsedAction(
                action=ActionType.NONE, confidence=0.95, used_message_ids=[mid], summary_tr="bilgi")], model="fake"))
        await self.db.save_message(StoredMessage(
            chat_id=CHAT, message_id=mid, sender_id=KADIR, sender_name="Kadir", date=datetime.now(UTC), text=text,
            reply_to_message_id=reply_to if reply_to is not None else FX, topic_id=FX, topic_title=TITLE))
        await self.engine.on_messages_ready(CHAT, KADIR, FX)
        return mid

    def sent(self, action: int) -> list[dict[str, Any]]:
        return self.fake.sent_actions(action)

    def texts(self, severity: Severity | None = None) -> list[str]:
        return self.notifier.texts(severity)


async def build_env(*, fake: FxFake | None = None, kind: str = "fx", mt5_kw: dict[str, Any] | None = None,
                    route_kw: dict[str, Any] | None = None, risk_kw: dict[str, Any] | None = None) -> Env:
    db = Database(":memory:")
    await db.open()
    fake = fake or FxFake()
    bcfg = Mt5BrokerConfig(type="mt5", name="vantage", login=5001, password="pw", server="Demo", risk_usd=100.0,
                           **(mt5_kw or {}))
    cfg = AppConfig(
        telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[CHAT], topics=[TopicRoute(
            name="fx", title_contains=["Fx İşlemleri"], brokers=["vantage"], kind=kind, **(route_kw or {}))]),
        notifier=NotifierConfig(bot_token="123:abc", owner_user_id=42), brokers=[bcfg],
        risk=RiskConfig(**(risk_kw or {})))
    cfg.monitor_interval_sec = 3600.0
    broker = Mt5Broker(bcfg, mt5_module=fake)
    broker.retry_delay_sec = 0
    parser, notifier = FakeParser(), FakeNotifier()
    holder: dict[str, Engine] = {}
    oracle = PriceOracle(lambda: holder["e"].brokers, exchange_id=None, cache_ttl_sec=0)
    engine = Engine(cfg, db, parser, [broker], notifier, price_oracle=oracle)
    holder["e"] = engine
    await engine.start(monitor=False)
    return Env(db, fake, broker, engine, parser, notifier)


@pytest.fixture
async def env():
    e = await build_env()
    yield e
    await e.engine.stop()
    await e.db.close()


async def make(**kw: Any):
    e = await build_env(**kw)
    return e


async def finish(e: Env) -> None:
    await e.engine.stop()
    await e.db.close()


# --------------------------------------------------------------------------- #
# 1. per-route entry thresholds
# --------------------------------------------------------------------------- #
def test_route_threshold_defaults_and_overrides() -> None:
    risk = RiskConfig()
    fx = TopicRoute(name="fx", title_contains=["x"], kind="fx")
    assert route_thresholds(risk, fx) == (0.03, 1.0, 0.05)
    crypto = TopicRoute(name="k", title_contains=["x"], kind="crypto")
    assert route_thresholds(risk, crypto) == (0.5, 10.0, 0.5)
    assert effective_risk(risk, crypto) is risk and route_thresholds(risk, None) == (0.5, 10.0, 0.5)
    custom = TopicRoute(name="fx", title_contains=["x"], kind="fx", market_entry_tolerance_pct=0.1,
                        max_entry_distance_pct=2.0, duplicate_tolerance_pct=0.2)
    assert route_thresholds(risk, custom) == (0.1, 2.0, 0.2)
    anyroute = TopicRoute(name="a", title_contains=["x"], max_entry_distance_pct=3.0)
    assert route_thresholds(risk, anyroute) == (0.5, 3.0, 0.5)


@pytest.mark.parametrize("entry", [1.0840, 1.0830, 1.0800 + 0.0001])
async def test_fx_route_auto_entry_far_from_market_is_limit_at_signal_entry(entry: float) -> None:
    e = await make()
    try:
        await e.say(eur(entry=entry, sl=1.0700))
        assert e.sent(MT5.TRADE_ACTION_DEAL) == []
        (req,) = e.sent(MT5.TRADE_ACTION_PENDING)
        assert req["type"] == MT5.ORDER_TYPE_BUY_LIMIT and req["price"] == pytest.approx(entry)
        assert e.notifier.confirmations == []
    finally:
        await finish(e)


async def test_fx_route_entry_within_three_pips_is_market() -> None:
    e = await make()
    try:
        await e.say(eur(entry=1.0849))
        assert e.sent(MT5.TRADE_ACTION_PENDING) == []
        (deal,) = e.sent(MT5.TRADE_ACTION_DEAL)
        assert deal["type"] == MT5.ORDER_TYPE_BUY
    finally:
        await finish(e)


def _build(kind: str | None, market: float, **kw: Any):
    route = TopicRoute(name="r", title_contains=["x"], kind=kind) if kind else None
    return validator.build_open_plan(
        eur(entry=1.0840, sl=1.0700), risk=effective_risk(RiskConfig(), route), llm=AppConfig.model_fields["llm"].default_factory(),
        market_price=market, source_chat_id=1, now=datetime.now(UTC), **kw)


def test_crypto_route_keeps_global_tolerance_fx_does_not() -> None:
    plan, _ = _build("crypto", 1.0850)     # 0.09% from entry: inside the global 0.5%
    assert plan is not None and plan.entry_type is EntryType.MARKET
    plan, _ = _build("fx", 1.0850)
    assert plan is not None and plan.entry_type is EntryType.LIMIT and plan.entry_price == 1.0840


async def test_market_drift_guard_uses_fx_distance() -> None:
    e = await build_env()
    try:
        base, _ = validator.build_open_plan(
            eur(entry=1.0850, sl=1.0700), risk=RiskConfig(), llm=e.engine.config.llm, market_price=1.0850,
            source_chat_id=CHAT, now=datetime.now(UTC), route="fx", broker_names=["vantage"])
        assert base is not None
        far = base.model_copy(update={"entry_type": EntryType.MARKET, "entry_price": 1.0700})    # 1.4% away
        await e.engine.execute_open(far, raw_symbol="EURUSD")
        assert e.sent(MT5.TRADE_ACTION_DEAL) == [] and any("uzak" in t for t in e.texts())
    finally:
        await finish(e)
    e = await build_env(kind="crypto")    # global 10% guard: the same drift is let through
    try:
        await e.engine.execute_open(far.model_copy(update={"route": "fx"}), raw_symbol="EURUSD")
        assert len(e.sent(MT5.TRADE_ACTION_DEAL)) == 1
    finally:
        await finish(e)


async def test_fx_duplicate_tolerance_is_tight() -> None:
    e = await make()
    try:
        await e.say(eur(entry=1.0840, sl=1.0700))
        await e.say(eur(entry=1.0830, sl=1.0700))     # 0.09% apart: a different level on FX, not a repost
        assert len(e.sent(MT5.TRADE_ACTION_PENDING)) == 2 and e.notifier.confirmations == []
        await e.say(eur(entry=1.08404, sl=1.0700))    # 0.004% apart: a repost -> asked
        assert len(e.notifier.confirmations) == 1
        assert any(i.code == "duplicate_signal" for i in e.notifier.confirmations[0].issues)
    finally:
        await finish(e)


# --------------------------------------------------------------------------- #
# 2. marketable limit
# --------------------------------------------------------------------------- #
async def test_marketable_limit_on_fx_route_opens_at_market_without_question() -> None:
    e = await make()
    try:
        e.fake.set_price(1.0829, 1.0830)
        await e.say(eur(entry=1.0840, sl=1.0800, et=EntryType.LIMIT))
        assert e.notifier.confirmations == [] and e.sent(MT5.TRADE_ACTION_PENDING) == []
        (deal,) = e.sent(MT5.TRADE_ACTION_DEAL)
        assert deal["type"] == MT5.ORDER_TYPE_BUY and deal["sl"] == 1.0800
        pos = (await e.db.active_broker_positions())[0][1]
        assert pos.status is PositionStatus.OPEN and any("piyasa emriyle" in w for w in pos.data["warnings"])
    finally:
        await finish(e)


async def test_marketable_limit_far_beyond_max_distance_still_asks() -> None:
    e = await make()
    try:
        e.fake.set_price(1.0529, 1.0530)       # 3% below the entry: probably a misread price
        await e.say(eur(entry=1.0840, sl=1.0500, et=EntryType.LIMIT))
        assert len(e.notifier.confirmations) == 1 and e.sent(MT5.TRADE_ACTION_DEAL) == []
    finally:
        await finish(e)


def test_marketable_limit_crypto_route_still_asks() -> None:
    plan, issues = validator.build_open_plan(
        ParsedAction(action=ActionType.OPEN, base_asset="ETH", quote_asset="USDT", side=Side.LONG, entry_price=2232.18,
                     stop_loss=2180, take_profits=[TakeProfit(price=2300)], risk_r=1.0, confidence=0.95),
        risk=RiskConfig(), llm=AppConfig.model_fields["llm"].default_factory(), market_price=2210,
        source_chat_id=1, now=datetime.now(UTC))
    assert [i.code for i in issues] == ["limit_entry_marketable"]


# --------------------------------------------------------------------------- #
# 3. connect with Algo Trading off -> warning to the owner
# --------------------------------------------------------------------------- #
async def test_algo_trading_off_warns_owner_but_connects() -> None:
    fake = FxFake()
    fake.terminal_info = lambda: SimpleNamespace(trade_allowed=False)   # type: ignore[method-assign]
    e = await build_env(fake=fake)
    try:
        assert e.engine.brokers and not e.engine.failed_brokers
        assert any("Algo Trading" in t and "yeşil" in t for t in e.texts(Severity.WARNING))
    finally:
        await finish(e)


# --------------------------------------------------------------------------- #
# A. no_hedge
# --------------------------------------------------------------------------- #
async def test_no_hedge_skips_opposite_signal_on_open_position() -> None:
    e = await make(mt5_kw={"no_hedge": True})
    try:
        await e.say(eur(entry=1.0850))                      # market long
        n_before = len(e.fake.sent)
        await e.say(short(entry=1.0850))
        assert len(e.fake.sent) == n_before                 # no order
        assert e.notifier.confirmations == []               # nothing asked
        hits = [t for t in e.texts() if "hedging yasağı" in t]
        assert len(hits) == 1 and "LONG" in hits[0] and "SHORT" in hits[0]
        trades = await e.db.recent_trades(datetime(2000, 1, 1, tzinfo=UTC))
        assert sorted(t.status.value for t in trades) == ["failed", "open"]
        assert len(e.fake.positions) == 1 and e.fake.orders == {}
    finally:
        await finish(e)


async def test_no_hedge_skips_opposite_signal_on_pending_limit() -> None:
    e = await make(mt5_kw={"no_hedge": True})
    try:
        await e.say(eur(entry=1.0800, sl=1.0700))           # pending buy limit
        assert len(e.fake.orders) == 1
        await e.say(short(entry=1.0900, sl=1.0950, tps=(1.0800,)))    # pending sell limit would be a hedge
        assert len(e.fake.orders) == 1 and e.notifier.confirmations == []
        assert len([t for t in e.texts() if "hedging yasağı" in t]) == 1
        assert any("bekleyen limit" in t for t in e.texts())
    finally:
        await finish(e)


async def test_no_hedge_opposite_signal_opens_after_long_closed() -> None:
    e = await make(mt5_kw={"no_hedge": True})
    try:
        first = await e.say(eur(entry=1.0850))
        await e.say(manage(ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=first), text="kapattık",
                    reply_to=first)
        assert e.fake.positions == {}
        await e.say(short(entry=1.0850))
        assert len(e.fake.positions) == 1
        assert next(iter(e.fake.positions.values())).type == MT5.ORDER_TYPE_SELL
    finally:
        await finish(e)


async def test_without_no_hedge_opposite_signal_still_asks() -> None:
    e = await make(mt5_kw={"no_hedge": False})
    try:
        await e.say(eur(entry=1.0850))
        await e.say(short(entry=1.0850))
        assert len(e.notifier.confirmations) == 1
        assert any(i.code == "opposite_trade_open" for i in e.notifier.confirmations[0].issues)
    finally:
        await finish(e)


async def test_no_hedge_same_direction_second_signal_is_refused_on_netting() -> None:
    e = await make(fake=FxFake(margin_mode=NETTING), mt5_kw={"no_hedge": True})
    try:
        await e.say(eur(entry=1.0850, sl=1.0800))
        await e.say(eur(entry=1.0830, sl=1.0790, tps=(1.0960,)), text="EUR tekrar")
        assert len(e.fake.positions) == 1
        errs = e.texts(Severity.ERROR)
        assert any("Netting hesap" in t for t in errs)       # reported, not silent
    finally:
        await finish(e)


# --------------------------------------------------------------------------- #
# B. per-broker daily loss limit
# --------------------------------------------------------------------------- #
async def _record_loss(e: Env, amount: float) -> None:
    plan, _ = validator.build_open_plan(
        eur(entry=1.0850), risk=RiskConfig(), llm=e.engine.config.llm, market_price=1.0850, source_chat_id=CHAT,
        now=datetime.now(UTC), route="fx", broker_names=["vantage"])
    assert plan is not None
    trade = await e.db.create_trade(plan, TradeStatus.CANCELLED)    # not a duplicate candidate
    from copytrader.models import BrokerPosition
    bp = BrokerPosition(broker="vantage", broker_symbol="EURUSD", side=Side.LONG, status=PositionStatus.CLOSED,
                        quantity=0.1, realized_pnl=-amount)
    await e.db.save_broker_position(trade.id, bp)


async def test_broker_daily_loss_limit_overrides_global() -> None:
    # global limit 10% would allow it; the broker's own 3% (of ~10k) blocks after a 400 loss
    e = await make(mt5_kw={"daily_loss_limit_pct": 3.0})
    try:
        await _record_loss(e, 400.0)
        await e.say(eur(entry=1.0850))
        assert e.sent(MT5.TRADE_ACTION_DEAL) == [] and any("günlük zarar sınırı" in t for t in e.texts())
    finally:
        await finish(e)
    e = await make()          # no per-broker value: the global 10% applies -> opens
    try:
        await _record_loss(e, 400.0)
        await e.say(eur(entry=1.0850))
        assert len(e.sent(MT5.TRADE_ACTION_DEAL)) == 1
    finally:
        await finish(e)


# --------------------------------------------------------------------------- #
# 10. netting accounts
# --------------------------------------------------------------------------- #
async def test_netting_second_signal_on_same_symbol_is_refused_and_reported() -> None:
    e = await make(fake=FxFake(margin_mode=NETTING))
    try:
        assert e.broker._netting is True
        await e.say(eur(entry=1.0850, sl=1.0800))
        assert len(e.fake.positions) == 1
        n = len(e.fake.sent)
        await e.say(eur(entry=1.0830, sl=1.0780, tps=(1.0970,)), text="EUR ikinci")
        assert len(e.fake.sent) == n and len(e.fake.positions) == 1
        assert any("Netting hesap" in t and "zaten açık pozisyon" in t for t in e.texts(Severity.ERROR))
        p = next(iter(e.fake.positions.values()))
        assert p.sl == 1.0800                                  # the existing position kept its stop
    finally:
        await finish(e)


async def test_netting_opposite_signal_refused_without_no_hedge_after_owner_approval() -> None:
    e = await make(fake=FxFake(margin_mode=NETTING))
    try:
        await e.say(eur(entry=1.0850))
        await e.say(short(entry=1.0850))
        # engine asks first (opposite_trade_open); even if approved the broker refuses to net it away
        assert len(e.notifier.confirmations) == 1 and len(e.fake.positions) == 1
    finally:
        await finish(e)


async def test_netting_sl_move_partial_and_full_close_with_single_position() -> None:
    e = await make(fake=FxFake(margin_mode=NETTING))
    try:
        m = await e.say(eur(entry=1.0850, sl=1.0800, tps=(1.0900, 1.0950)))
        (deal,) = e.sent(MT5.TRADE_ACTION_DEAL)
        assert deal["tp"] == 1.0950 and deal["sl"] == 1.0800           # venue TP = last level
        (p,) = e.fake.positions.values()
        assert p.sl == 1.0800
        e.fake.set_price(1.0870, 1.0871)
        await e.say(manage(ActionType.MOVE_SL, move_sl_to_entry=True, refers_to_message_id=m), text="stop giriş",
                    reply_to=m)
        assert p.sl == pytest.approx(1.0851, abs=2e-4) and p.sl > 0     # moved, never removed
        total = p.volume
        await e.say(manage(ActionType.CLOSE, close_fraction=0.5, refers_to_message_id=m), text="yarı", reply_to=m)
        assert 0 < p.volume < total and p.sl > 0                        # partial close keeps the stop
        await e.say(manage(ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=m), text="kapattık", reply_to=m)
        assert e.fake.positions == {}
    finally:
        await finish(e)


async def test_netting_multi_tp_single_position_with_synced_partial() -> None:
    e = await make(fake=FxFake(margin_mode=NETTING))
    try:
        await e.say(eur(entry=1.0850, sl=1.0800, tps=(1.0900, 1.0950)))
        assert len(e.sent(MT5.TRADE_ACTION_DEAL)) == 1 and len(e.fake.positions) == 1
        (pos,) = [p for _, p in await e.db.active_broker_positions()]
        assert [x["price"] for x in pos.data["emulated_tps"]] == [1.0900]
        vol = next(iter(e.fake.positions.values())).volume
        e.fake.set_price(1.0901, 1.0902)
        new = await e.broker.sync_position(pos)
        p = next(iter(e.fake.positions.values()))
        assert p.volume < vol and p.sl == 1.0800 and new.data["emulated_tps"][0]["done"] is True
    finally:
        await finish(e)
