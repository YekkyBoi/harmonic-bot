"""Forum topics: only the configured topics are read, each topic's trades go to its own brokers, and a
topic's management messages only ever touch that topic's trades. Real in-memory Database."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from copytrader.brokers.base import BrokerError
from copytrader.config import AppConfig, NotifierConfig, PaperBrokerConfig, TelegramConfig, TopicRoute
from copytrader.engine.orchestrator import Engine
from copytrader.models import (
    ActionType,
    EntryType,
    MarketType,
    ParseContext,
    ParsedAction,
    ParseResult,
    PositionStatus,
    Quote,
    Severity,
    Side,
    StoredMessage,
    TakeProfit,
    TradeStatus,
)
from copytrader.storage.db import Database

from fakes import FakeBroker, FakeNotifier, FakeParser, FakePriceOracle

CHAT = -1001111111111
KADIR = 900
CRYPTO, FX, SOHBET = 10, 20, 30
TITLES = {CRYPTO: "💰 Kasım Ayı Kripto İşlemleri", FX: "💱 Kasım Ayı Fx İşlemleri", SOHBET: "Sohbet"}
ROUTES = [
    TopicRoute(name="kripto", title_contains=["Kripto İşlemleri"], brokers=["binance"], kind="crypto"),
    TopicRoute(name="fx", title_contains=["Fx İşlemleri"], brokers=["mt5"], kind="fx"),
]


def now() -> datetime:
    return datetime.now(timezone.utc)


def make_config(*, topics: bool = True) -> AppConfig:
    cfg = AppConfig(
        telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[CHAT], topics=list(ROUTES) if topics else []),
        notifier=NotifierConfig(bot_token="123:abc", owner_user_id=42),
        brokers=[PaperBrokerConfig(type="paper", name="binance"), PaperBrokerConfig(type="paper", name="mt5")],
    )
    cfg.monitor_interval_sec = 3600.0
    return cfg


def result(*actions: ParsedAction) -> ParseResult:
    return ParseResult(actions=list(actions), model="fake")


def eth_short(**kw: Any) -> ParsedAction:
    data: dict[str, Any] = dict(
        action=ActionType.OPEN, symbol="ETHUSDT.P", base_asset="ETH", quote_asset="USDT",
        market_type=MarketType.PERP, side=Side.SHORT, entry_type=EntryType.MARKET, entry_price=2239.2,
        stop_loss=2250.6, take_profits=[TakeProfit(price=2205.11)], risk_r=1.0, used_message_ids=[610, 611],
        confidence=0.95, summary_tr="ETH SHORT, giriş 2239.20, SL 2250.60, TP 2205.11, risk 1R",
    )
    data.update(kw)
    return ParsedAction(**data)


def eur_long(**kw: Any) -> ParsedAction:
    data: dict[str, Any] = dict(
        action=ActionType.OPEN, symbol="EURUSD", base_asset="EUR", quote_asset="USD", market_type=MarketType.FOREX,
        side=Side.LONG, entry_type=EntryType.LIMIT, entry_price=1.0840, stop_loss=1.0800,
        take_profits=[TakeProfit(price=1.0950)], risk_r=1.0, used_message_ids=[700, 701], confidence=0.95,
        summary_tr="EURUSD LONG limit 1.0840, SL 1.0800, TP 1.0950, risk 1R",
    )
    data.update(kw)
    return ParsedAction(**data)


def manage(kind: ActionType, **kw: Any) -> ParsedAction:
    data: dict[str, Any] = dict(action=kind, confidence=0.95)
    data.update(kw)
    return ParsedAction(**data)


class Env:
    def __init__(self, cfg: AppConfig, db: Database, parser: FakeParser, binance: FakeBroker, mt5: FakeBroker,
                 notifier: FakeNotifier, oracle: FakePriceOracle, engine: Engine) -> None:
        self.cfg, self.db, self.parser, self.engine = cfg, db, parser, engine
        self.binance, self.mt5, self.notifier, self.oracle = binance, mt5, notifier, oracle

    async def msg(self, mid: int, text: str = "", *, topic: int | None, reply_to: int | None = None,
                  photo: bool = False, sender: int = KADIR) -> StoredMessage:
        m = StoredMessage(
            chat_id=CHAT, message_id=mid, sender_id=sender, sender_name="Kadir", date=now(), text=text,
            media_path="/nonexistent/chart.jpg" if photo else None, media_type="photo" if photo else None,
            reply_to_message_id=reply_to if reply_to is not None else topic,   # forum: replies default to the root
            topic_id=topic, topic_title=TITLES.get(topic) if topic is not None else None,
        )
        await self.db.save_message(m)
        return m

    async def ready(self, topic: int | None, sender: int = KADIR) -> None:
        await self.engine.on_messages_ready(CHAT, sender, topic)

    async def crypto_signal(self) -> int:
        self.parser.push(result(eth_short()))
        await self.msg(610, topic=CRYPTO, photo=True)
        await self.msg(611, "Eth short\n1R risk\n100 risk için 8.7 lot", topic=CRYPTO)
        await self.ready(CRYPTO)
        trade = await self.db.find_trade_by_message(CHAT, 611)
        assert trade is not None
        return trade.id

    async def fx_signal(self) -> int:
        self.parser.push(result(eur_long()))
        await self.msg(700, topic=FX, photo=True)
        await self.msg(701, "EUR LONG 1R Limit Emir", topic=FX)
        await self.ready(FX)
        trade = await self.db.find_trade_by_message(CHAT, 701)
        assert trade is not None
        return trade.id


async def _env(db: Database, *, topics: bool = True, mt5_fails: bool = False) -> Env:
    cfg = make_config(topics=topics)
    parser = FakeParser()
    binance = FakeBroker("binance", quote=Quote(bid=2239.1, ask=2239.3, last=2239.2))
    mt5 = FakeBroker("mt5", qty_step=0.01, fail={"connect": BrokerError("terminal kapalı")} if mt5_fails else None)
    mt5.quotes["EUR"] = Quote(bid=1.0850, ask=1.0851, last=1.08505)
    notifier = FakeNotifier()
    oracle = FakePriceOracle({"ETH": 2239.2, "EUR": 1.08505})
    engine = Engine(cfg, db, parser, [binance, mt5], notifier, price_oracle=oracle)
    await engine.start(monitor=False)
    notifier.notifications.clear()
    return Env(cfg, db, parser, binance, mt5, notifier, oracle, engine)


@pytest.fixture
async def db():
    database = Database(":memory:")
    await database.open()
    yield database
    await database.close()


@pytest.fixture
async def env(db: Database):
    e = await _env(db)
    yield e
    await e.engine.stop()


# --------------------------------------------------------------------------- #
# Topic filtering and routing of new trades
# --------------------------------------------------------------------------- #
async def test_crypto_topic_signal_opens_only_on_binance(env: Env) -> None:
    trade_id = await env.crypto_signal()

    assert len(env.binance.opened) == 1 and env.mt5.opened == []
    assert env.notifier.confirmations == [] and env.notifier.texts(Severity.ERROR) == []
    trade = await env.db.get_trade(trade_id)
    assert trade is not None and trade.status is TradeStatus.OPEN
    assert trade.plan.route == "kripto" and trade.plan.broker_names == ["binance"]
    assert "konu kripto → binance" in trade.plan.summary_tr
    msgs, ctx = env.parser.calls[0]
    assert [m.message_id for m in msgs] == [610, 611]
    assert ctx.topic_title == TITLES[CRYPTO] and ctx.topic_kind == "crypto"
    assert ctx.reply_to is None             # "reply" to the topic root is not a reply


async def test_fx_topic_signal_opens_only_on_mt5(env: Env) -> None:
    trade_id = await env.fx_signal()

    assert env.binance.opened == [] and len(env.mt5.opened) == 1
    req = env.mt5.opened[0]
    assert req.entry_type is EntryType.LIMIT and req.entry_price == pytest.approx(1.0840)
    trade = await env.db.get_trade(trade_id)
    assert trade is not None and trade.status is TradeStatus.PENDING
    assert trade.plan.route == "fx" and trade.plan.broker_names == ["mt5"]
    _msgs, ctx = env.parser.calls[0]
    assert ctx.topic_title == TITLES[FX] and ctx.topic_kind == "fx"


async def test_other_topics_are_consumed_without_reading(env: Env) -> None:
    await env.msg(800, "ETH 2300'ü kırarsa long düşünürüm", topic=SOHBET)
    await env.ready(SOHBET)
    await env.msg(801, "BTC long 1R stop 60000", topic=None)        # General
    await env.ready(None)

    assert env.parser.calls == []
    assert (await env.db.get_message(CHAT, 800)).consumed            # type: ignore[union-attr]
    assert (await env.db.get_message(CHAT, 801)).consumed            # type: ignore[union-attr]
    assert env.binance.opened == [] and env.mt5.opened == [] and env.notifier.notifications == []
    events = await env.db.events("topic_ignored")
    assert sorted(str(e["payload"]["topic_title"]) for e in events) == ["None", "Sohbet"]


async def test_topic_title_matching_survives_the_monthly_rename(env: Env) -> None:
    env.parser.push(result(eth_short(used_message_ids=[620])))
    m = StoredMessage(chat_id=CHAT, message_id=620, sender_id=KADIR, date=now(), text="Eth short 1R",
                      topic_id=11, topic_title="💰 ARALIK AYI KRİPTO İŞLEMLERİ")
    await env.db.save_message(m)
    await env.ready(11)
    assert len(env.binance.opened) == 1 and env.mt5.opened == []


async def test_general_batch_never_takes_another_topics_messages(env: Env) -> None:
    await env.msg(610, topic=CRYPTO, photo=True)
    await env.ready(None)                       # e.g. a General message of the same sender triggered it
    assert env.parser.calls == []
    assert not (await env.db.get_message(CHAT, 610)).consumed       # type: ignore[union-attr]

    env.parser.push(result(eth_short(used_message_ids=[610, 611])))
    await env.msg(611, "Eth short 1R", topic=CRYPTO)
    await env.ready(CRYPTO)
    assert [m.message_id for m in env.parser.calls[0][0]] == [610, 611]
    assert len(env.binance.opened) == 1


async def test_same_sender_in_two_topics_gives_two_separate_batches(env: Env) -> None:
    env.parser.push(result(eth_short(used_message_ids=[611])), result(eur_long(used_message_ids=[701])))
    await env.msg(611, "Eth short 1R", topic=CRYPTO)
    await env.msg(701, "EUR LONG 1R Limit Emir", topic=FX)
    await env.ready(CRYPTO)
    await env.ready(FX)
    assert [[m.message_id for m in msgs] for msgs, _ctx in env.parser.calls] == [[611], [701]]
    assert len(env.binance.opened) == 1 and len(env.mt5.opened) == 1
    assert env.binance.opened[0].plan.base_asset == "ETH" and env.mt5.opened[0].plan.base_asset == "EUR"


async def test_route_broker_not_connected_errors_and_trades_nowhere_else(db: Database) -> None:
    env = await _env(db, mt5_fails=True)
    try:
        trade_id = await env.fx_signal()
        assert env.binance.opened == [] and env.mt5.opened == []
        errors = env.notifier.texts(Severity.ERROR)
        assert len(errors) == 1 and "bağlı değil" in errors[0] and "mt5" in errors[0] and "AÇILMADI" in errors[0]
        trade = await env.db.get_trade(trade_id)
        assert trade is not None and trade.status is TradeStatus.FAILED

        # A later "Stop" reply to it has nothing to do: ignored, never asked.
        env.notifier.notifications.clear()
        env.parser.push(result(manage(ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=701,
                                      base_asset="EUR", used_message_ids=[702])))
        await env.msg(702, "Stop", topic=FX, reply_to=701)
        await env.ready(FX)
        assert env.notifier.confirmations == [] and env.notifier.notifications == []
    finally:
        await env.engine.stop()


async def test_preview_uses_the_route_brokers(env: Env) -> None:
    ev = await env.engine.evaluate(eur_long(), CHAT, route=ROUTES[1])
    assert ev is not None and ev.plan is not None
    lines = await env.engine.preview_open(ev.plan)   # type: ignore[arg-type]
    assert len(lines) == 1 and lines[0].startswith("mt5:")
    env.engine.brokers = [env.binance]                # mt5 gone
    lines = await env.engine.preview_open(ev.plan)   # type: ignore[arg-type]
    assert lines == ["'fx' konusunun brokerı (mt5) bağlı değil; işlem başka hesapta açılmaz"]


# --------------------------------------------------------------------------- #
# Management is scoped to the topic's own trades
# --------------------------------------------------------------------------- #
async def test_fx_stop_reply_never_touches_the_crypto_trade(env: Env) -> None:
    crypto_id = await env.crypto_signal()
    fx_id = await env.fx_signal()

    env.parser.push(result(manage(ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=701,
                                  base_asset="EUR", side=Side.LONG, used_message_ids=[702])))
    await env.msg(702, "Stop", topic=FX, reply_to=701)
    await env.ready(FX)

    _msgs, ctx = env.parser.calls[-1]
    assert [t["trade_id"] for t in ctx.open_trades] == [fx_id]      # the crypto trade is not even shown
    assert ctx.reply_to is not None and ctx.reply_to.message_id == 701
    assert env.notifier.confirmations == []
    assert len(env.mt5.calls_of("cancel_entry")) == 1               # pending limit -> cancelled
    assert env.binance.calls_of("close_position") == [] and env.binance.calls_of("cancel_entry") == []
    assert (await env.db.get_trade(crypto_id)).status is TradeStatus.OPEN      # type: ignore[union-attr]
    assert (await env.db.get_trade(fx_id)).status is TradeStatus.CANCELLED     # type: ignore[union-attr]


async def test_a_wrong_target_id_from_another_topic_is_never_used(env: Env) -> None:
    crypto_id = await env.crypto_signal()
    fx_id = await env.fx_signal()
    env.parser.push(result(manage(ActionType.CLOSE, close_fraction=1.0, target_trade_id=crypto_id,
                                  used_message_ids=[703])))
    await env.msg(703, "kapattık", topic=FX)
    await env.ready(FX)
    assert env.binance.calls_of("close_position") == []
    (conf,) = env.notifier.confirmations                            # the only FX trade is a guess: asked
    assert conf.kind == "manage" and conf.plan is not None and conf.plan.trade_id == fx_id  # type: ignore[union-attr]
    assert "trade_guessed" in [i.code for i in conf.issues]


async def test_duplicate_and_opposite_checks_are_per_route(env: Env) -> None:
    await env.crypto_signal()                                       # ETH SHORT on binance (route kripto)
    eth_long_fx = eth_short(side=Side.LONG, stop_loss=2200.0, take_profits=[], used_message_ids=[705])
    ev = await env.engine.evaluate(eth_long_fx, CHAT, route=ROUTES[1])
    assert ev is not None and ev.plan is not None
    codes = [i.code for i in ev.issues]
    assert "opposite_trade_open" not in codes                       # other route, other broker account
    ev = await env.engine.evaluate(eth_long_fx, CHAT)               # no route: may be on binance too
    assert ev is not None and "opposite_trade_open" in [i.code for i in ev.issues]
    repost = await env.engine.evaluate(eth_short(used_message_ids=[706]), CHAT, route=ROUTES[0])
    assert repost is not None and "duplicate_signal" in [i.code for i in repost.issues]
    other = await env.engine.evaluate(eth_short(used_message_ids=[707]), CHAT, route=ROUTES[1])
    assert other is not None and "duplicate_signal" not in [i.code for i in other.issues]


# --------------------------------------------------------------------------- #
# Management of a trade that is already over: ignored silently
# --------------------------------------------------------------------------- #
async def _close_trade(env: Env, trade_id: int) -> None:
    for pos in await env.db.broker_positions(trade_id):
        await env.db.save_broker_position(trade_id, pos.model_copy(update={
            "status": PositionStatus.CLOSED, "remaining_quantity": 0.0, "realized_pnl": -200.0}))
    await env.db.update_trade(trade_id, status=TradeStatus.CLOSED)


async def test_stop_reply_to_a_closed_trade_is_ignored_without_asking(env: Env) -> None:
    trade_id = await env.crypto_signal()
    await _close_trade(env, trade_id)                               # our stop already hit
    env.parser.push(result(manage(ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=611,
                                  base_asset="ETH", side=Side.SHORT, used_message_ids=[612], confidence=0.8,
                                  ambiguities=["Açık işlemlerde bu sinyal yok."])))
    await env.msg(612, "Stop", topic=CRYPTO, reply_to=611)
    await env.ready(CRYPTO)

    assert env.notifier.confirmations == [] and env.notifier.notifications == []
    assert env.binance.calls_of("close_position") == []
    assert (await env.db.get_message(CHAT, 612)).consumed            # type: ignore[union-attr]
    (event,) = await env.db.events("manage_ignored")
    assert event["payload"]["trade_id"] == trade_id and "kapanmış" in event["payload"]["reason"]


async def test_ignored_management_is_reported_when_notify_on_ignored(env: Env) -> None:
    env.cfg.notifier.notify_on_ignored = True
    trade_id = await env.crypto_signal()
    await _close_trade(env, trade_id)
    env.parser.push(result(manage(ActionType.MOVE_SL, move_sl_to_entry=True, refers_to_message_id=611,
                                  used_message_ids=[613], summary_tr="ETH stop girişe")))
    await env.msg(613, "Stop olmayanlar varsa entry stop atsın", topic=CRYPTO, reply_to=611)
    await env.ready(CRYPTO)
    assert env.notifier.confirmations == []
    (text,) = env.notifier.texts(Severity.INFO)
    assert "Yok sayıldı" in text and f"#{trade_id}" in text and "kapanmış" in text


async def test_stop_reply_to_an_open_trade_closes_it_fully(env: Env) -> None:
    trade_id = await env.crypto_signal()
    env.parser.push(result(manage(ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=611,
                                  target_trade_id=trade_id, base_asset="ETH", used_message_ids=[612])))
    await env.msg(612, "0.2R kar ile manuel kapattık.", topic=CRYPTO, reply_to=611)
    await env.ready(CRYPTO)
    assert env.notifier.confirmations == []
    (_pos, fraction), = env.binance.calls_of("close_position")
    assert fraction == 1.0
    assert (await env.db.get_trade(trade_id)).status is TradeStatus.CLOSED    # type: ignore[union-attr]


async def test_stop_that_hit_since_the_last_monitor_pass_is_not_closed_again(env: Env) -> None:
    trade_id = await env.crypto_signal()
    env.binance.sync_fn = lambda pos: pos.model_copy(update={
        "status": PositionStatus.CLOSED, "remaining_quantity": 0.0, "realized_pnl": -200.0})
    env.parser.push(result(manage(ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=611,
                                  target_trade_id=trade_id, used_message_ids=[612])))
    await env.msg(612, "Stop", topic=CRYPTO, reply_to=611)
    await env.ready(CRYPTO)
    assert len(env.binance.calls_of("sync_position")) == 1
    assert env.binance.calls_of("close_position") == []
    assert env.notifier.confirmations == []
    assert (await env.db.get_trade(trade_id)).status is TradeStatus.CLOSED    # type: ignore[union-attr]


async def test_reply_to_a_finished_trade_wins_over_another_open_trade(env: Env) -> None:
    old_id = await env.crypto_signal()
    await _close_trade(env, old_id)
    env.parser.push(result(eth_short(used_message_ids=[620], entry_price=2300.0, stop_loss=2320.0,
                                     take_profits=[TakeProfit(price=2250.0)])))
    env.binance.quote = Quote(bid=2299.9, ask=2300.1, last=2300.0)
    env.oracle.prices["ETH"] = 2300.0
    await env.msg(620, "Eth short 1R tekrar", topic=CRYPTO)
    await env.ready(CRYPTO)
    new = await env.db.find_trade_by_message(CHAT, 620)
    assert new is not None and new.status is TradeStatus.OPEN
    # The model links the late "Stop" reply about the OLD signal to the open trade: the reply wins.
    env.parser.push(result(manage(ActionType.CLOSE, close_fraction=1.0, target_trade_id=new.id,
                                  base_asset="ETH", used_message_ids=[621])))
    await env.msg(621, "Stop", topic=CRYPTO, reply_to=611)
    await env.ready(CRYPTO)
    assert env.binance.calls_of("close_position") == [] and env.notifier.confirmations == []
    assert (await env.db.get_trade(new.id)).status is TradeStatus.OPEN        # type: ignore[union-attr]


async def test_aktif_reply_is_information_only(env: Env) -> None:
    await env.fx_signal()
    env.parser.push(result(manage(ActionType.NONE, used_message_ids=[702], summary_tr="Limit emir doldu (bilgi)")))
    await env.msg(702, "Aktif", topic=FX, reply_to=701)
    await env.ready(FX)
    assert len(env.parser.calls) == 2                               # passed the prefilter
    assert env.notifier.confirmations == [] and env.notifier.notifications == []
    assert env.mt5.calls_of("cancel_entry") == [] and env.mt5.calls_of("close_position") == []


# --------------------------------------------------------------------------- #
# Confirmations, corrections and edits keep the topic
# --------------------------------------------------------------------------- #
async def test_correction_keeps_the_topic_route_and_approval_trades_only_there(env: Env) -> None:
    env.parser.push(result(eur_long(confidence=0.6, ambiguities=["Stop etiketi okunamadı."])))
    await env.msg(700, topic=FX, photo=True)
    await env.msg(701, "EUR LONG 1R Limit Emir", topic=FX)
    await env.ready(FX)
    (conf,) = env.notifier.confirmations
    assert conf.plan is not None and conf.plan.route == "fx"          # type: ignore[union-attr]

    seen: list[ParseContext] = []

    def corrected(msgs: list[StoredMessage], ctx: ParseContext) -> ParseResult:
        seen.append(ctx)
        return result(eur_long(stop_loss=1.0790))

    env.parser.push(corrected)
    await env.engine.handle_correction(conf.id, "SL 1.0790")
    assert seen[0].topic_title == TITLES[FX] and seen[0].topic_kind == "fx"
    new = env.notifier.confirmations[-1]
    assert new.id != conf.id and new.plan is not None
    assert new.plan.route == "fx" and new.plan.broker_names == ["mt5"]     # type: ignore[union-attr]
    await env.engine.handle_confirmation(new.id, True)
    assert env.binance.opened == [] and len(env.mt5.opened) == 1
    assert env.mt5.opened[0].stop_loss == pytest.approx(1.0790)


async def test_edit_in_a_topic_that_is_not_followed_is_ignored(env: Env) -> None:
    m = await env.msg(800, "selam", topic=SOHBET)
    await env.ready(SOHBET)
    edited = m.model_copy(update={"text": "BTC long 1R stop 60000", "edited": True})
    await env.db.save_message(edited)
    await env.engine.on_message_edited(edited)
    assert env.parser.calls == [] and env.notifier.confirmations == []


async def test_status_lists_the_followed_topics(env: Env) -> None:
    text = await env.engine.status_text()
    assert "Takip edilen konular" in text and "kripto" in text and "binance" in text and "mt5" in text


# --------------------------------------------------------------------------- #
# No topics configured: behaviour as before (a topic id only narrows the batch)
# --------------------------------------------------------------------------- #
async def test_without_routes_every_topic_is_read_and_traded_everywhere(db: Database) -> None:
    env = await _env(db, topics=False)
    try:
        env.parser.push(result(eth_short(used_message_ids=[800])))
        await env.msg(800, "Eth short 1R", topic=SOHBET)
        await env.msg(801, "EUR LONG", topic=FX)
        await env.ready(SOHBET)
        msgs, ctx = env.parser.calls[0]
        assert [m.message_id for m in msgs] == [800]               # still one topic per batch
        assert ctx.topic_title == "Sohbet" and ctx.topic_kind is None
        assert len(env.binance.opened) == 1 and len(env.mt5.opened) == 1
        trade = await env.db.find_trade_by_message(CHAT, 800)
        assert trade is not None and trade.plan.route is None and trade.plan.broker_names == []
    finally:
        await env.engine.stop()
