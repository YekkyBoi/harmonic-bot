"""Follow-up of release 0b4bbe0 (the owner's two requests).

A  "kopukken gelen yeni sinyal bağlantı gelince şartlar uygunsa açılsın; Telegram'dan bana sorsun girilsin mi diye":
   a new signal whose leg on the optional broker (tickmill, the friend's account) was skipped BEFORE anything was sent
   because the broker was not connected, or its MetaApi link was known down, is remembered (in the database). When
   that broker is healthy again (it connected, or it passed a heartbeat check) the owner is asked ONCE with ✅ Evet /
   ❌ Hayır - if the trade is still open on his own brokers (a market entry or a filled limit), the signal is at most
   15 minutes old, tickmill's price is within the bot's normal market-entry tolerance and neither the stop nor a
   target was reached. "Evet" checks everything again and opens it on tickmill with the trade's CURRENT stop and
   targets; "Hayır" or no answer opens nothing. Otherwise the owner is told once why it is not caught up. A refusal
   by the broker (or any other reason) is never caught up; a healthy tickmill changes nothing.
B  ``python -m copytrader stop``: the running bot shuts down as after Ctrl+C, the command waits until it has.

End to end on the production replica of tests/test_preflight_scenarios.py (live config): REAL Engine, validator and
the four adapters on the fake venues. The link-down cases use a MetaApi adapter that made its connection itself (as
in production) on the fake SDK of tests/test_preflight4_metaapi_reconnect.py, so its own health checks and the
heartbeat decide.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from test_notifier import CbEvent, FakeBotClient, FakeEngine
from test_preflight4_metaapi_reconnect import World
from test_preflight_scenarios import (  # noqa: F401  (_isolate: autouse)
    CHAT,
    EURUSD_LONG,
    FX,
    KRIPTO,
    Prod,
    _isolate,
    btc,
    fx_pair,
    set_eurusd,
)

import copytrader.__main__ as cli
import copytrader.brokers.metaapi_broker as mb
from copytrader import app as app_mod
from copytrader.brokers.base import BrokerError, BrokerUnavailableError
from copytrader.brokers.metaapi_broker import MetaApiBroker
from copytrader.config import AppConfig, NotifierConfig, TelegramConfig
from copytrader.engine.orchestrator import Engine
from copytrader.models import (
    ActionType,
    ConfirmationStatus,
    EntryType,
    ParsedAction,
    PendingConfirmation,
    PositionStatus,
    Severity,
    Side,
    TakeProfit,
    TradePlan,
    TradeStatus,
)
from copytrader.storage.db import Database
from copytrader.telegram.formatting import format_confirmation
from copytrader.telegram.notifier import BotNotifier

from fakes import FakeBroker, FakeNotifier, FakePriceOracle

MISSED = "missed_optional_legs"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
@pytest.fixture
async def live(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    p = await Prod.create(tmp_path, monkeypatch, replica="live")
    yield p
    await p.close()


async def settle(engine: Engine, timeout: float = 5.0) -> None:
    """Wait until the engine's background work (catch-up checks, order flows, late legs) is done."""
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while True:
        busy = [t for t in (*engine._tasks, *engine._critical)
                if not t.done() and not t.get_name().startswith(("expire-", "retry-"))]
        if not busy:
            return
        assert loop.time() < end, [t.get_name() for t in busy]
        await asyncio.sleep(0.01)


async def owned_tickmill(p: Prod, monkeypatch: pytest.MonkeyPatch) -> tuple[MetaApiBroker, World]:
    """Replace the replica's tickmill adapter by one that made its MetaApi connection itself (production): its own
    health checks keep the link state and refuse a new signal's calls while it is known down."""
    monkeypatch.setattr(mb, "HEALTH_TIMEOUT_SEC", 0.05)
    monkeypatch.setattr(mb, "HEALTH_SYNC_GRACE_SEC", 0.02)
    world = World()
    world.venue = p.tickmill
    old = p.brokers["tickmill"]
    new = MetaApiBroker(old.config, api_factory=world.factory)
    new.retry_delay_sec = 0
    await old.close()
    await p.engine._try_connect(new)
    eng = p.engine
    eng.configured_brokers = [new if b is old else b for b in eng.configured_brokers]
    eng.brokers = [new if b is old else b for b in eng.brokers]
    p.brokers["tickmill"] = new
    return new, world


async def link_down(tm: MetaApiBroker, world: World) -> None:
    """The MetaApi link stops answering and two health checks in a row find it (as the heartbeat would)."""
    world.line.dead = True
    assert await tm.health_problem() is not None
    assert await tm.health_problem() is not None


def tickmill_down(p: Prod) -> None:
    """tickmill is not connected (as after a failed connect at start: it is retried in the background)."""
    eng = p.engine
    eng.brokers = [b for b in eng.brokers if b.name != "tickmill"]
    eng.failed_brokers["tickmill"] = eng.optional_down["tickmill"] = "MetaApi bağlanamadı (test)"


async def reconnect(p: Prod) -> None:
    """The engine's background retry connects tickmill again (Engine._retry_optional)."""
    eng = p.engine
    eng.optional_retry_sec = 0.01
    eng._retrying.add("tickmill")
    await eng._retry_optional(p.brokers["tickmill"])
    await settle(eng)


def questions(p: Prod) -> list[PendingConfirmation]:
    return [c for c in p.notifier.confirmations if c.kind == "catch_up"]


def stop_at(trade_id: int, base: str, quote: str, level: float) -> ParsedAction:
    return ParsedAction(action=ActionType.MOVE_SL, new_stop_loss=level, base_asset=base, quote_asset=quote,
                        side=Side.LONG, target_trade_id=trade_id, confidence=0.95, summary_tr=f"{base} stop {level}")


# =========================================================================== #
# A1  link known down -> remembered -> heartbeat healthy -> one question -> Evet -> opened on tickmill
# =========================================================================== #
async def test_a1_link_known_down_signal_is_asked_once_when_back_and_opened_on_evet(
    live: Prod, monkeypatch: pytest.MonkeyPatch
) -> None:
    tm, world = await owned_tickmill(live, monkeypatch)
    await link_down(tm, world)
    await live.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")

    (trade,) = await live.trades()
    assert trade.status is TradeStatus.OPEN and sorted(await live.positions(trade.id)) == ["fx"]
    assert live.tickmill_orders() == []
    # at the signal: exactly what the released version says (nothing new, no question)
    refusal = ("tickmill: <code>MetaApi bağlantısı sorunlu: son 2 bağlantı kontrolü yanıt almadı (MetaApi bağlantı "
               "kontrolü: 0.1 sn içinde yanıt gelmedi); bu hesapta yeni işlem açılmadı.")
    assert any(t.startswith(refusal) for t in live.texts(Severity.ERROR))
    (report,) = live.reports()
    assert report.startswith("⚠️ <b>#1 EUR LONG açıldı</b>") and "• fx: 0.48 EURUSD (açık)" in report
    assert live.notifier.confirmations == []
    stored = await live.db.get_state(MISSED)
    assert list(stored) == ["1:tickmill"] and stored["1:tickmill"]["trade_id"] == 1     # kept in the database

    world.line.dead = False                     # the link works again: the heartbeat's check finds it healthy
    live.notifier.notifications.clear()
    await live.engine.monitor.heartbeat.check_once()
    await settle(live.engine)

    (conf,) = questions(live)
    text = format_confirmation(conf)
    assert text.startswith("❓ <b>#1 EUR LONG tickmill hesabında da açılsın mı?</b>\n"
                           "✅ Evet = tickmill hesabında aç · ❌ Hayır = açma\n")
    for line in ("Bu sinyal geldiğinde tickmill bağlı değildi (bağlantısı yoktu ya da kopuktu); işlem orada açılmadı. "
                 "İşlem fx hesabında açık. tickmill şimdi yeniden bağlı.",
                 "• Sinyal fiyatı: 1.162 (", "• Şimdiki fiyat (tickmill): 1.16205", "• Stop: 1.16", "• Hedef: 1.166",
                 "• tickmill hesabında risk: 45.00 USD (stop çalışırsa yaklaşık kayıp)",
                 "⏳ 5 dk içinde yanıt gelmezse tickmill hesabında işlem açılmaz."):
        assert line in text, (line, text)
    assert (conf.expires_at - conf.created_at) == timedelta(minutes=5)
    assert await live.db.get_state(MISSED) == {}          # dropped before it was asked: never asked twice
    await live.engine.monitor.heartbeat.check_once()      # healthy again: no second question
    await settle(live.engine)
    assert len(questions(live)) == 1 and live.tickmill_orders() == []

    result = await live.engine.handle_confirmation(conf.id, True)
    assert result == ("✅ Evet: tickmill hesabına emir gönderiliyor (işlemin şu anki stop ve hedefleriyle); sonuç "
                      "ayrıca bildirilecek.")
    await settle(live.engine)
    # one market order, sized as a normal open (45 USD / 20.5 pips), with the trade's stop and target
    assert live.tickmill_orders() == [("market_buy", "EURUSD", 0.21, 1.16, 1.166)]
    pos = await live.positions(trade.id)
    assert pos["tickmill"].status is PositionStatus.OPEN and pos["tickmill"].stop_loss == 1.16
    (done,) = [t for t in live.texts() if "sonradan" in t]
    assert done == ("✅ <b>#1 EUR LONG</b> — tickmill hesabında sonradan açıldı (onayınızla)\n"
                    "• tickmill: 0.21 EURUSD (açık), risk 45.00 USD, giriş ~1.16205")
    assert (await live.db.get_trade(trade.id)).status is TradeStatus.OPEN
    # never twice
    assert (await live.engine.handle_confirmation(conf.id, True)).startswith("Bu onay artık geçerli değil")
    await live.engine.monitor.heartbeat.check_once()
    await settle(live.engine)
    assert len(live.tickmill_orders()) == 1 and len(questions(live)) == 1


# =========================================================================== #
# A2  not connected -> skipped as before -> connected by the retry -> question with the moved stop -> Evet
# =========================================================================== #
async def test_a2_not_connected_crypto_signal_is_opened_with_the_current_stop_and_the_signals_size(live: Prod) -> None:
    tickmill_down(live)
    await live.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO, "btc 1R long")
    assert "ℹ️ tickmill bağlı değil, bu işlem atlandı (BTC LONG)." in live.texts(Severity.WARNING)   # as before
    (trade,) = await live.trades()
    assert sorted(await live.positions(trade.id)) == ["binance", "breakout"] and live.notifier.confirmations == []
    # the trader moves the stop while tickmill is away (binance / breakout get it)
    await live.say(stop_at(trade.id, "BTC", "USDT", 73500.0), KRIPTO, "stop 73500")
    assert (await live.db.get_trade(trade.id)).current_stop_loss == 73500.0

    live.notifier.notifications.clear()
    await reconnect(live)
    assert live.texts()[0] == ("✅ <b>tickmill</b> bağlandı. Bundan sonraki sinyaller bu hesapta da açılacak (atlanan "
                               "sinyaller kendiliğinden açılmaz; bağlantı yokken atlananlar için ayrıca bilgi "
                               "verilecek).")
    (conf,) = questions(live)
    text = format_confirmation(conf)
    for line in ("❓ <b>#1 BTC LONG tickmill hesabında da açılsın mı?</b>",
                 "İşlem binance ve breakout hesaplarında açık.",
                 "• Sinyal fiyatı: 74000 (",
                 "• Şimdiki fiyat: 73951 (tickmill fiyatı; sinyal grafiğinde ~74001)",
                 "• Stop: 73500 (tickmill fiyatıyla 73450) — sinyalde 73300 idi, sonradan taşındı",
                 "• Hedef: 75500 (tickmill fiyatıyla 75450)",
                 "• tickmill hesabında risk: 45.00 USD (stop çalışırsa yaklaşık kayıp); miktar sinyalin ilk stopuna "
                 "göre hesaplanır, şimdiki stopta kayıp bundan az olur"):
        assert line in text, (line, text)

    assert (await live.engine.handle_confirmation(conf.id, True)).startswith("✅ Evet: tickmill hesabına emir")
    await settle(live.engine)
    # the CURRENT stop (shifted by tickmill's -50), the size of the signal's own stop: 45 / (73951 - 73250) = 0.06 lot
    # (the size of a normal open, test_a of test_preflight_scenarios), never the size of the closer stop (0.09)
    assert live.tickmill_orders() == [("market_buy", "BTCUSD", 0.06, 73450.0, 75450.0)]
    (done,) = [t for t in live.texts() if "sonradan açıldı" in t]
    assert done.startswith("✅ <b>#1 BTC LONG</b> — tickmill hesabında sonradan açıldı (onayınızla)\n"
                           "• tickmill: 0.06 BTCUSD (açık), risk 45.00 USD, giriş ~73951 (fark -50.0 uygulandı)")
    assert "miktar sinyalin ilk stopuna göre hesaplandı" in done


# =========================================================================== #
# A3  Hayır / no answer: nothing on tickmill
# =========================================================================== #
async def test_a3_hayir_and_expiry_open_nothing(live: Prod) -> None:
    tickmill_down(live)
    await live.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
    await live.say(fx_pair("GBPUSD", Side.LONG, 1.3150, 1.3130, 1.3190), FX, "GBP LONG 1R")
    await reconnect(live)
    first, second = questions(live)
    assert "#1 EUR LONG" in format_confirmation(first) and "#2 GBP LONG" in format_confirmation(second)

    assert await live.engine.handle_confirmation(first.id, False) == "❌ Hayır: tickmill hesabında işlem açılmadı."
    live.engine._clock = lambda: datetime.now(UTC) + timedelta(minutes=6)       # 5 minutes passed
    await live.engine._expire(second.id)
    assert live.notifier.closed_text(second.id) == "⌛ Süre doldu; tickmill hesabında işlem açılmadı."
    assert (await live.engine._get_confirmation(second.id)).status is ConfirmationStatus.EXPIRED
    await settle(live.engine)
    assert live.tickmill_orders() == []
    assert (await live.engine.handle_confirmation(second.id, True)).startswith("Bu onay artık geçerli değil")
    assert live.tickmill_orders() == []


# =========================================================================== #
# A4  conditions that do not hold when it is back: told once, never asked
# =========================================================================== #
async def test_a4_pending_limit_and_a_price_that_ran_away_are_told_once_and_never_asked(live: Prod) -> None:
    tickmill_down(live)
    await live.say(fx_pair("EURUSD", Side.LONG, 1.1580, 1.1560, 1.1660, entry_type=EntryType.LIMIT), FX, "EUR limit")
    await live.say(fx_pair("GBPUSD", Side.LONG, 1.3150, 1.3130, 1.3190), FX, "GBP LONG 1R")
    one, two = await live.trades()
    assert one.status is TradeStatus.PENDING and two.status is TradeStatus.OPEN
    live.tickmill.set_price("GBPUSD", 1.31695, 1.31705)                         # 0.16 % above the signal now

    live.notifier.notifications.clear()
    await reconnect(live)
    assert questions(live) == [] and live.tickmill_orders() == []
    (told,) = [t for t in live.texts() if "AÇILMAYACAK" in t]
    assert told == (
        "ℹ️ <b>tickmill</b> yeniden bağlı. Bağlantı yokken atlanan şu işlemler tickmill hesabında sonradan AÇILMAYACAK:\n"
        "• <b>#1 EUR LONG</b>: işlem diğer hesaplarda bekleyen limit emri olarak duruyor (giriş henüz dolmadı); bekleyen "
        "bir işlem sonradan eklenmez\n"
        "• <b>#2 GBP LONG</b>: anlık fiyat (1.31705) sinyal fiyatından (1.315) %0.16 uzak; botun piyasa emriyle giriş "
        "sınırı sinyal fiyatından en fazla %0.03 ya da stop mesafesinin 0.25 katı")
    assert await live.db.get_state(MISSED) == {}
    await live.engine.monitor.heartbeat.check_once()                            # told once
    await settle(live.engine)
    assert sum("AÇILMAYACAK" in t for t in live.texts()) == 1 and questions(live) == []


async def test_a4_too_old_signal_is_told_and_a_late_question_never_outlives_15_minutes(
    live: Prod, monkeypatch: pytest.MonkeyPatch
) -> None:
    import test_preflight_scenarios as tps

    shift = {"by": timedelta(0)}

    class Shifted(datetime):        # the trader's messages (and the bot's clock) of a few minutes ago
        @classmethod
        def now(cls, tz: Any = None) -> Any:
            return datetime.now(tz) + shift["by"]

    monkeypatch.setattr(tps, "datetime", Shifted)
    live.engine._clock = lambda: datetime.now(UTC) + shift["by"]
    tickmill_down(live)
    shift["by"] = -timedelta(minutes=16)
    await live.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
    shift["by"] = -timedelta(minutes=4)
    await live.say(fx_pair("GBPUSD", Side.LONG, 1.3150, 1.3130, 1.3190), FX, "GBP LONG 1R")
    shift["by"] = -timedelta(minutes=12)
    await live.say(fx_pair("AUDUSD", Side.LONG, 0.6520, 0.6500, 0.6560), FX, "AUD LONG 1R")
    shift["by"] = timedelta(0)
    assert [t.status for t in await live.trades()] == [TradeStatus.OPEN] * 3
    live.notifier.notifications.clear()
    await reconnect(live)

    (told,) = [t for t in live.texts() if "AÇILMAYACAK" in t]
    assert told == ("ℹ️ <b>tickmill</b> yeniden bağlı. Bağlantı yokken atlanan şu işlem tickmill hesabında sonradan "
                    "AÇILMAYACAK:\n• <b>#1 EUR LONG</b>: sinyalin üzerinden 16 dk geçti; sonradan açma yalnızca sinyalden "
                    "sonraki 15 dk içinde yapılır")
    aud, gbp = questions(live)                  # (oldest miss first: #3 was 12 minutes ago, #2 4 minutes ago)
    assert "#2 GBP LONG" in format_confirmation(gbp) and gbp.expires_at - gbp.created_at == timedelta(minutes=5)
    # 12 minutes old: the question ends when the signal is 15 minutes old
    assert "#3 AUD LONG" in format_confirmation(aud)
    assert timedelta(minutes=2, seconds=50) < aud.expires_at - aud.created_at <= timedelta(minutes=3)
    assert "⏳ 3 dk içinde yanıt gelmezse tickmill hesabında işlem açılmaz." in format_confirmation(aud)
    assert "• Sinyal fiyatı: 0.652 (12 dk önce)" in format_confirmation(aud)
    # answered after the 15 minutes (the question had expired meanwhile): never opened
    live.engine._clock = lambda: datetime.now(UTC) + timedelta(minutes=4)
    assert (await live.engine.handle_confirmation(aud.id, True)).startswith("⌛ Bu onayın süresi dolmuş")
    assert (await live.engine.handle_confirmation(gbp.id, True)).startswith("✅ Evet")
    await settle(live.engine)
    assert [o[:2] for o in live.tickmill_orders()] == [("market_buy", "GBPUSD")]


# =========================================================================== #
# A5  Evet checks everything again
# =========================================================================== #
async def test_a5_evet_after_the_price_ran_away_opens_nothing_and_says_why(live: Prod) -> None:
    tickmill_down(live)
    await live.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
    await reconnect(live)
    (conf,) = questions(live)
    live.tickmill.set_price("EURUSD", 1.16395, 1.16405)                         # 20 pips above the signal now
    result = await live.engine.handle_confirmation(conf.id, True)
    assert result == ("⛔ tickmill hesabında işlem açılmadı: anlık fiyat (1.16405) sinyal fiyatından (1.162) %0.18 uzak; "
                      "botun piyasa emriyle giriş sınırı sinyal fiyatından en fazla %0.03 ya da stop mesafesinin 0.25 "
                      "katı.")
    await settle(live.engine)
    assert live.tickmill_orders() == []
    assert (await live.engine._get_confirmation(conf.id)).status is ConfirmationStatus.REJECTED


async def test_a5_evet_after_the_trade_was_closed_opens_nothing(live: Prod) -> None:
    tickmill_down(live)
    await live.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
    await reconnect(live)
    (conf,) = questions(live)
    (trade,) = await live.trades()
    await live.say(ParsedAction(action=ActionType.CLOSE, close_fraction=1.0, base_asset="EUR", quote_asset="USD",
                                side=Side.LONG, target_trade_id=trade.id, confidence=0.95, summary_tr="EUR kapat"),
                   FX, "kapattık")
    assert (await live.db.get_trade(trade.id)).status is TradeStatus.CLOSED
    result = await live.engine.handle_confirmation(conf.id, True)
    assert result == "⛔ tickmill hesabında işlem açılmadı: işlem zaten kapanmış."
    await settle(live.engine)
    assert live.tickmill_orders() == []


# =========================================================================== #
# A6  never for anything but "not connected / link known down"; a healthy tickmill changes nothing
# =========================================================================== #
async def test_a6_a_refusal_by_the_broker_and_an_asset_it_does_not_list_are_never_caught_up(live: Prod) -> None:
    from test_metaapi_broker import trade_exc

    live.tickmill.fail["create_market_buy_order"] = [trade_exc(10019, "TRADE_RETCODE_NO_MONEY", "No money")]
    await live.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
    assert any("tickmill" in t and "No money" in t for t in live.texts())
    assert await live.db.get_state(MISSED, {}) == {}
    tickmill_down(live)
    await live.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0).model_copy(
        update={"symbol": "SOLUSDT.P", "base_asset": "SOL", "entry_price": 180.0, "stop_loss": 175.0,
                "take_profits": [TakeProfit(price=190.0)], "summary_tr": "SOL LONG"}), KRIPTO, "sol long")
    assert await live.db.get_state(MISSED, {}) == {}                    # tickmill does not list SOL
    await reconnect(live)
    await live.engine.monitor.heartbeat.check_once()
    await settle(live.engine)
    assert questions(live) == [] and not any("AÇILMAYACAK" in t for t in live.texts())


async def test_a6_a_signal_while_tickmill_is_connected_is_exactly_as_before(live: Prod) -> None:
    """The scenario (c) of test_preflight_scenarios on the live replica, then a heartbeat round: the same orders, the
    same one report, nothing remembered, no question."""
    set_eurusd(live, 1.16235, 1.16245)
    await live.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
    assert live.fx_orders() == [("BUY", "EURUSD", 0.40, 1.16245, 1.16, 1.166)]
    assert live.tickmill_orders() == [("market_buy", "EURUSD", 0.18, 1.16, 1.166)]
    texts = list(live.texts())
    (report,) = live.reports()
    assert report.startswith("✅ <b>#1 EUR LONG açıldı</b>") and len(texts) == 1
    assert await live.db.get_state(MISSED, {}) == {}
    await live.engine.monitor.heartbeat.check_once()
    await settle(live.engine)
    assert live.texts() == texts and live.notifier.confirmations == []


# =========================================================================== #
# A7  the miss survives a restart (generic optional broker)
# =========================================================================== #
def _cfg() -> AppConfig:
    c = AppConfig(telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[-1001234567890]),
                  notifier=NotifierConfig(bot_token="123:abc", owner_user_id=42))
    c.monitor_interval_sec = 3600.0
    return c


def _plan(names: list[str]) -> TradePlan:
    return TradePlan(plan_id="p1", base_asset="ETH", side=Side.LONG, entry_type=EntryType.MARKET, entry_price=2232.85,
                     stop_loss=2200.0, take_profits=[TakeProfit(price=2300.0, close_fraction=1.0)], risk_r=1.0,
                     risk_pct=2.0, source_chat_id=-1001234567890, source_message_ids=[1],
                     created_at=datetime.now(UTC), broker_names=names)


async def test_a7_a_remembered_miss_is_asked_after_a_restart(tmp_path: Path) -> None:
    path = str(tmp_path / "bot.db")
    db = Database(path)
    await db.open()
    tm = FakeBroker("tickmill", optional=True,
                    fail={"resolve_symbol": BrokerUnavailableError("MetaApi bağlantısı yok (test)")})
    notifier = FakeNotifier()
    engine = Engine(_cfg(), db, object(), [FakeBroker("binance"), tm], notifier,
                    price_oracle=FakePriceOracle({"ETH": 2232.85}))
    await engine.start(monitor=False)
    await engine.execute_open(_plan(["binance", "tickmill"]))
    assert tm.opened == [] and list(await db.get_state(MISSED)) == ["1:tickmill"]
    await engine.stop()
    await db.close()

    db = Database(path)                                 # the restart: a healthy tickmill
    await db.open()
    tm2 = FakeBroker("tickmill", optional=True)
    notifier = FakeNotifier()
    engine = Engine(_cfg(), db, object(), [FakeBroker("binance"), tm2], notifier,
                    price_oracle=FakePriceOracle({"ETH": 2232.85}))
    try:
        await engine.start(monitor=False)
        await settle(engine)
        (conf,) = [c for c in notifier.confirmations if c.kind == "catch_up"]
        assert "#1 ETH LONG tickmill hesabında da açılsın mı?" in format_confirmation(conf)
        assert await engine.handle_confirmation(conf.id, True)
        await settle(engine)
        (req,) = tm2.opened
        assert req.entry_type is EntryType.MARKET and req.stop_loss == 2200.0
        assert [tp.price for tp in req.take_profits] == [2300.0]
    finally:
        await engine.stop()
        await db.close()


async def test_a7_a_plain_broker_error_is_not_remembered(tmp_path: Path) -> None:
    db = Database(":memory:")
    await db.open()
    tm = FakeBroker("tickmill", optional=True, fail={"resolve_symbol": BrokerError("sembol listesi okunamadı")})
    engine = Engine(_cfg(), db, object(), [FakeBroker("binance"), tm], FakeNotifier(),
                    price_oracle=FakePriceOracle({"ETH": 2232.85}))
    try:
        await engine.start(monitor=False)
        await engine.execute_open(_plan(["binance", "tickmill"]))
        assert await db.get_state(MISSED, {}) == {}
    finally:
        await engine.stop()
        await db.close()


async def test_a7_a_late_answer_that_says_not_connected_is_remembered_too(tmp_path: Path,
                                                                           monkeypatch: pytest.MonkeyPatch) -> None:
    """tickmill answers after the owner's brokers' report (its 3 s grace) that it could not even try: remembered."""
    from copytrader.engine import orchestrator as orch

    monkeypatch.setattr(orch, "OPTIONAL_LEG_GRACE_SEC", 0.05)

    class Slow(FakeBroker):
        async def resolve_symbol(self, *args: Any, **kwargs: Any) -> Any:
            await asyncio.sleep(0.3)
            raise BrokerUnavailableError("MetaApi bağlantısı şu an yenileniyor (test)")

    db = Database(":memory:")
    await db.open()
    engine = Engine(_cfg(), db, object(), [FakeBroker("binance"), Slow("tickmill", optional=True)], FakeNotifier(),
                    price_oracle=FakePriceOracle({"ETH": 2232.85}))
    try:
        await engine.start(monitor=False)
        await engine.execute_open(_plan(["binance", "tickmill"]))
        assert await db.get_state(MISSED, {}) == {}             # still waiting for tickmill
        await settle(engine)
        assert list(await db.get_state(MISSED)) == ["1:tickmill"]
    finally:
        await engine.stop()
        await db.close()


async def test_a7_less_than_a_minute_left_to_answer_is_told_not_asked(tmp_path: Path) -> None:
    db = Database(":memory:")
    await db.open()
    tm = FakeBroker("tickmill", optional=True, fail={"resolve_symbol": BrokerUnavailableError("bağlantı yok (test)")})
    notifier = FakeNotifier()
    engine = Engine(_cfg(), db, object(), [FakeBroker("binance"), tm], notifier,
                    price_oracle=FakePriceOracle({"ETH": 2232.85}))
    try:
        await engine.start(monitor=False)
        plan = _plan(["binance", "tickmill"]).model_copy(
            update={"created_at": datetime.now(UTC) - timedelta(minutes=14, seconds=30)})
        await engine.execute_open(plan)
        tm.fail.clear()                                     # healthy again: the heartbeat's hook
        engine.monitor.heartbeat.on_healthy("tickmill")
        await settle(engine)
        assert notifier.confirmations == [] and tm.opened == []
        (told,) = [t for t in notifier.texts() if "AÇILMAYACAK" in t]
        assert told.endswith("• <b>#1 ETH LONG</b>: sinyalin üzerinden 14 dk geçti; 15 dk sınırına kadar yanıt için "
                             "yeterli süre kalmadı")
    finally:
        await engine.stop()
        await db.close()


# =========================================================================== #
# A8  the question's buttons (the real notifier)
# =========================================================================== #
def _catch_up_conf() -> PendingConfirmation:
    now = datetime(2026, 11, 13, 12, 0, tzinfo=UTC)
    action = ParsedAction(action=ActionType.OPEN, base_asset="EUR", side=Side.LONG, confidence=1.0)
    return PendingConfirmation(
        id="k1", kind="catch_up", action=action, source_chat_id=-100, created_at=now,
        expires_at=now + timedelta(minutes=5),
        catch_up={"trade_id": 1, "broker": "tickmill", "question": "#1 EUR LONG tickmill hesabında da açılsın mı?",
                  "details": "• Stop: 1.16"})


async def test_a8_the_question_has_evet_and_hayir_buttons_and_no_correction(tmp_path: Path) -> None:
    n = BotNotifier(NotifierConfig(bot_token="123:abc", owner_user_id=777),
                    TelegramConfig(api_id=1, api_hash="x", source_chats=[-100]), str(tmp_path))
    client, engine = FakeBotClient(), FakeEngine()
    n._client, n._engine = client, engine
    await n.ask_confirmation(_catch_up_conf())
    sent = client.sent[-1]
    assert [b.text for row in sent.buttons for b in row] == ["✅ Evet", "❌ Hayır"]
    assert [b.type.data for row in sent.buttons for b in row] == [b"ok:k1", b"no:k1"]
    assert sent.text == ("❓ <b>#1 EUR LONG tickmill hesabında da açılsın mı?</b>\n"
                         "✅ Evet = tickmill hesabında aç · ❌ Hayır = açma\n\n"
                         "• Stop: 1.16\n"
                         "⏳ 5 dk içinde yanıt gelmezse tickmill hesabında işlem açılmaz.")
    await n.handle_callback(CbEvent(b"ok:k1", sender_id=777))
    assert engine.calls == [("confirm", "k1", True)]

    db = Database(":memory:")
    await db.open()
    eng = Engine(_cfg(), db, object(), [FakeBroker("binance")], FakeNotifier(), price_oracle=FakePriceOracle({}))
    try:
        await eng.start(monitor=False)
        await eng._save_confirmation(_catch_up_conf().model_copy(update={
            "created_at": datetime.now(UTC), "expires_at": datetime.now(UTC) + timedelta(minutes=5)}))
        assert await eng.request_correction("k1") == "Bu soruda düzeltme yok: ✅ Evet ya da ❌ Hayır'a basın."
    finally:
        await eng.stop()
        await db.close()


# =========================================================================== #
# A9  the approved open never holds the owner's brokers; a change meanwhile reaches it
# =========================================================================== #
async def test_a9_a_slow_catch_up_open_never_holds_the_owners_brokers(live: Prod, monkeypatch: pytest.MonkeyPatch) -> None:
    tm, world = await owned_tickmill(live, monkeypatch)
    await link_down(tm, world)
    await live.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
    world.line.dead = False
    await live.engine.monitor.heartbeat.check_once()
    await settle(live.engine)
    (conf,) = questions(live)
    (trade,) = await live.trades()

    world.line.order_gate = asyncio.Event()             # tickmill's order is on its way and not answered yet
    t0 = time.monotonic()
    assert (await live.engine.handle_confirmation(conf.id, True)).startswith("✅ Evet")
    await asyncio.sleep(0.05)
    assert world.line.orders_waiting == 1
    # the trader moves the stop meanwhile: fx gets it at once, tickmill when its order is answered
    await asyncio.wait_for(live.say(stop_at(trade.id, "EUR", "USD", 1.1610), FX, "stop 1.1610"), 2.0)
    assert time.monotonic() - t0 < 2.0
    assert (await live.positions(trade.id))["fx"].stop_loss == 1.161
    world.line.order_gate.set()
    await settle(live.engine)
    assert live.tickmill_orders() == [("market_buy", "EURUSD", 0.21, 1.16, 1.166)]
    pos = (await live.positions(trade.id))["tickmill"]
    assert pos.status is PositionStatus.OPEN and pos.stop_loss == 1.161     # the change reached it on arrival


# =========================================================================== #
# B  python -m copytrader stop
# =========================================================================== #
REPO = Path(__file__).resolve().parents[1]


def _paper_config(tmp_path: Path) -> Path:
    import yaml

    data = {"dry_run": True, "data_dir": str(tmp_path / "data"),
            "telegram": {"api_id": 1, "api_hash": "h", "session_string": "fake-session", "source_chats": [-1001]},
            "notifier": {"bot_token": "123456789:" + "A" * 35, "owner_user_id": 42},
            "brokers": [{"type": "paper", "name": "sanal", "starting_balance": 1000}]}
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
    return path


def test_b1_stop_without_a_running_bot_exits_0_and_leaves_no_request(tmp_path: Path,
                                                                    capsys: pytest.CaptureFixture[str]) -> None:
    cfg = _paper_config(tmp_path)
    (tmp_path / "data").mkdir()
    app_mod.write_stop_request(tmp_path / "data", 999999)              # an old one: nobody would read it
    assert cli.main(["stop", "--config", str(cfg)]) == cli.EXIT_OK
    assert "Çalışan bir bot bulunamadı" in capsys.readouterr().out
    assert not (tmp_path / "data" / app_mod.STOP_REQUEST_FILE).exists()


def test_b2_stop_shuts_a_running_bot_down_and_waits_until_it_has_ended(tmp_path: Path,
                                                                       monkeypatch: pytest.MonkeyPatch,
                                                                       capsys: pytest.CaptureFixture[str]) -> None:
    """Another process runs the REAL ``copytrader run`` command (lock, old-request clean-up, "Durduruldu.") with the
    REAL request watch inside a stand-in for the app (no Telegram); ``stop`` asks it and returns 0 once it has
    ended and released bot.lock."""
    cfg = _paper_config(tmp_path)
    data = tmp_path / "data"
    data.mkdir()
    app_mod.write_stop_request(data, os.getpid())       # a request left from before: must not stop the new bot
    code = (
        "import asyncio, sys, time; sys.path.insert(0, sys.argv[1]); import copytrader.__main__ as cli; "
        "import copytrader.app as app_mod\n"
        "async def run(config_path):\n"
        "    stop = asyncio.Event()\n"
        "    watch = asyncio.ensure_future(app_mod.watch_stop_request(app_mod.stop_request_path(sys.argv[3]), "
        "stop.set, poll_sec=0.1))\n"
        "    print('calisiyor', flush=True)\n"
        "    await stop.wait()\n"
        "    await asyncio.sleep(0.5)          # the orderly shutdown takes a moment\n"
        "    return True\n"
        "app_mod.run = run\n"
        "sys.exit(cli.main(['run', '--config', sys.argv[2]]))\n")
    proc = subprocess.Popen([sys.executable, "-c", code, str(REPO), str(cfg), str(data)], stdout=subprocess.PIPE,
                            text=True, env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
    try:
        assert proc.stdout is not None and proc.stdout.readline().strip() == "calisiyor"
        time.sleep(0.3)
        assert proc.poll() is None                      # the old request did not stop it
        assert cli.lock_holder(data)["pid"] == proc.pid
        monkeypatch.setattr(cli, "STOP_CHECK_SEC", 0.05)
        t0 = time.monotonic()
        assert cli.main(["stop", "--config", str(cfg)]) == cli.EXIT_OK
        assert time.monotonic() - t0 < 10
        assert proc.poll() == 0                         # really ended
        out = capsys.readouterr().out
        assert "Durdurma isteği gönderildi" in out and out.rstrip().endswith("Bot durduruldu.")
        assert proc.stdout.read().strip().endswith("Durduruldu.")
        assert cli.lock_holder(data) is None and not (data / "bot.lock").exists()
        assert not (data / app_mod.STOP_REQUEST_FILE).exists()
    finally:
        proc.kill()
        proc.wait()


def test_b3_a_bot_that_does_not_take_the_request_in_time_is_left_running(tmp_path: Path,
                                                                         monkeypatch: pytest.MonkeyPatch,
                                                                         capsys: pytest.CaptureFixture[str]) -> None:
    cfg = _paper_config(tmp_path)
    data = tmp_path / "data"
    code = ("import sys, time; sys.path.insert(0, sys.argv[1]); import copytrader.__main__ as cli; "
            "cli.acquire_lock(sys.argv[2]); print('kilit', flush=True); time.sleep(300)")
    proc = subprocess.Popen([sys.executable, "-c", code, str(REPO), str(data)], stdout=subprocess.PIPE, text=True,
                            env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
    try:
        assert proc.stdout is not None and proc.stdout.readline().strip() == "kilit"
        monkeypatch.setattr(cli, "STOP_WAIT_SEC", 0.3)
        monkeypatch.setattr(cli, "STOP_CHECK_SEC", 0.05)
        assert cli.main(["stop", "--config", str(cfg)]) == cli.EXIT_PROBLEM
        assert "durdurma isteğini almadı; istek geri alındı, bot çalışmaya devam ediyor" in capsys.readouterr().out
        assert proc.poll() is None and not (data / app_mod.STOP_REQUEST_FILE).exists()   # never killed
    finally:
        proc.kill()
        proc.wait()


async def test_b4_serve_shuts_down_in_order_on_a_request_for_its_own_process_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from test_app_cli import _app

    monkeypatch.setattr(app_mod, "STOP_POLL_SEC", 0.05)
    notifier = FakeNotifier()
    app, brokers = _app(tmp_path, notifier)
    data = tmp_path / "data"
    stop_file = app_mod.stop_request_path(data)
    task = asyncio.ensure_future(app_mod.serve(app, stop_file=stop_file))
    await asyncio.wait_for(app.listener.started.wait(), 2)
    app_mod.write_stop_request(data, os.getpid() + 1)                  # another process: removed, ignored
    await asyncio.sleep(0.3)
    assert not task.done() and not stop_file.exists()
    app_mod.write_stop_request(data, os.getpid())
    assert await asyncio.wait_for(task, 5) is True
    # the same orderly shutdown as after Ctrl+C (test_app_cli.test_serve_starts_everything_and_shuts_down_in_order)
    assert app.listener.stopped and brokers[0].closed and app.db._conn is None
    assert any("durduruldu" in t for t in notifier.texts(Severity.INFO))
    assert not stop_file.exists()


def test_b5_run_prints_durduruldu_after_a_stop_request(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                      capsys: pytest.CaptureFixture[str]) -> None:
    cfg = _paper_config(tmp_path)

    async def fake_run(config_path: Any = "config.yaml") -> bool:
        return True

    monkeypatch.setattr(app_mod, "run", fake_run)
    assert cli.main(["run", "--config", str(cfg)]) == cli.EXIT_OK
    assert capsys.readouterr().out.rstrip().endswith("Durduruldu.")
    assert not (tmp_path / "data" / "bot.lock").exists()


def test_b6_the_runbook_uses_options_that_exist() -> None:
    doc = (REPO / "docs" / "SUNUCU_CLAUDE.md").read_text(encoding="utf-8")
    for line in ("[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12",
                 '-OutFile "$env:USERPROFILE\\Downloads\\harmonic-bot-guncel.zip"',    # (review: Path.home() on Windows)
                 ".\\venv\\Scripts\\python.exe -m copytrader stop",
                 ".\\venv\\Scripts\\python.exe guncelle.py --baslatma --bekci-yok",
                 "Start-Process powershell -WorkingDirectory (Get-Location).Path -ArgumentList '-NoExit', '-NoProfile', "
                 "'-Command', "
                 "'.\\venv\\Scripts\\python.exe -m copytrader run'",
                 "data\\logs\\copytrader.log", "`.env` dosyasını **asla** okumayın", "Botu **asla zorla kapatmayın**"):
        assert line in doc, line
    import importlib.util

    spec = importlib.util.spec_from_file_location("guncelle_for_test", REPO / "guncelle.py")
    assert spec is not None and spec.loader is not None
    g = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(g)
    opts = g._secenekler(["--baslatma", "--bekci-yok"])
    assert opts.baslatma is True and opts.bekci_yok is True
    assert g.ZIP_KALIBI == "harmonic-bot*.zip" and "stop" in cli._COMMANDS


# =========================================================================== #
# Review round 1 (referee): rollback safety, fx venue state at "Evet", owner brokers, small points
# =========================================================================== #
OLD_KINDS = {"open", "manage", "incomplete"}          # what PendingConfirmation.kind of 0b4bbe0 accepts


def _table_kinds(path: str) -> set[str]:
    """The kinds of every confirmation row in the database file (what an older version's loader validates)."""
    import sqlite3

    with sqlite3.connect(path) as conn:
        return {json.loads(raw)["kind"] for (raw,) in conn.execute("SELECT json FROM confirmations")}


async def _asked_engine(path: str) -> tuple[Engine, Database, FakeNotifier, PendingConfirmation]:
    db = Database(path)
    await db.open()
    tm = FakeBroker("tickmill", optional=True, fail={"resolve_symbol": BrokerUnavailableError("bağlantı yok (test)")})
    notifier = FakeNotifier()
    engine = Engine(_cfg(), db, object(), [FakeBroker("binance"), tm], notifier,
                    price_oracle=FakePriceOracle({"ETH": 2232.85}))
    await engine.start(monitor=False)
    await engine.execute_open(_plan(["binance", "tickmill"]))
    tm.fail.clear()
    engine.monitor.heartbeat.on_healthy("tickmill")
    await settle(engine)
    (conf,) = [c for c in notifier.confirmations if c.kind == "catch_up"]
    return engine, db, notifier, conf


async def _restarted(path: str) -> tuple[Engine, Database, FakeNotifier, FakeBroker]:
    db = Database(path)
    await db.open()
    tm = FakeBroker("tickmill", optional=True)
    notifier = FakeNotifier()
    engine = Engine(_cfg(), db, object(), [FakeBroker("binance"), tm], notifier,
                    price_oracle=FakePriceOracle({"ETH": 2232.85}))
    await engine.start(monitor=False)
    await settle(engine)
    return engine, db, notifier, tm


async def test_r1_a_question_open_at_a_clean_stop_is_closed_and_an_older_version_can_read_the_database(
    tmp_path: Path,
) -> None:
    path = str(tmp_path / "bot.db")
    engine, db, notifier, conf = await _asked_engine(path)
    assert _table_kinds(path) <= OLD_KINDS                  # the question is not in the confirmations table
    await engine.stop()
    assert notifier.closed_text(conf.id) == "⌛ Bot kapandı; tickmill hesabında işlem açılmadı."
    await db.close()
    assert _table_kinds(path) <= OLD_KINDS

    engine, db, notifier, tm = await _restarted(path)      # the next start: nothing to restore, nothing asked again
    try:
        assert notifier.confirmations == []
        assert (await engine.handle_confirmation(conf.id, True)).startswith("Bu onay artık geçerli değil")
        assert tm.opened == []
    finally:
        await engine.stop()
        await db.close()


async def test_r1_after_a_crash_an_older_version_can_read_the_database_and_this_one_restores_the_question(
    tmp_path: Path,
) -> None:
    path = str(tmp_path / "bot.db")
    engine, db, notifier, conf = await _asked_engine(path)
    for task in list(engine._tasks):                        # the process dies: no stop(), nothing closed
        task.cancel()
    await asyncio.gather(*engine._tasks, return_exceptions=True)
    await db.close()
    assert _table_kinds(path) <= OLD_KINDS                  # (0b4bbe0's Engine.start validates every pending row)

    engine, db, notifier, tm = await _restarted(path)
    try:
        assert notifier.confirmations == [] and conf.id in engine._expiry_tasks     # restored with its expiry
        assert (await engine.handle_confirmation(conf.id, True)).startswith("✅ Evet: tickmill hesabına emir")
        await settle(engine)
        assert len(tm.opened) == 1
    finally:
        await engine.stop()
        await db.close()


async def test_r2_evet_after_fx_stopped_out_at_the_venue_before_the_monitor_saw_it_opens_nothing(live: Prod) -> None:
    from copytrader.brokers.mt5_broker import MT5

    tickmill_down(live)
    await live.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
    await reconnect(live)
    (conf,) = questions(live)
    (trade,) = await live.trades()
    ticket = int((await live.positions(trade.id))["fx"].data["legs"][0]["ticket"])
    live.terminal.hit(ticket, MT5.DEAL_REASON_SL, 1.1600)  # the stop filled at fx; the monitor has not looked yet
    result = await live.engine.handle_confirmation(conf.id, True)
    await settle(live.engine)
    assert live.tickmill_orders() == []
    assert result == "⛔ tickmill hesabında işlem açılmadı: işlem zaten kapanmış."
    assert (await live.db.get_trade(trade.id)).status is TradeStatus.CLOSED


async def test_r3_an_owner_brokers_late_not_connected_answer_is_never_remembered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from copytrader.engine import orchestrator as orch

    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.1)

    class Slow(FakeBroker):                                  # the OWNER's account (not optional), answers late
        async def resolve_symbol(self, *args: Any, **kwargs: Any) -> Any:
            await asyncio.sleep(0.3)
            raise BrokerUnavailableError("MetaApi bağlantısı şu an yenileniyor (test)")

    db = Database(":memory:")
    await db.open()
    engine = Engine(_cfg(), db, object(), [FakeBroker("binance"), Slow("ownmeta")], FakeNotifier(),
                    price_oracle=FakePriceOracle({"ETH": 2232.85}))
    try:
        await engine.start(monitor=False)
        await engine.execute_open(_plan(["binance", "ownmeta"]))
        await settle(engine)
        assert await db.get_state(MISSED, {}) == {}
    finally:
        await engine.stop()
        await db.close()


async def test_r4_a_late_miss_keeps_the_signals_symbol_and_old_misses_are_dropped_at_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from copytrader.engine import orchestrator as orch

    monkeypatch.setattr(orch, "OPTIONAL_LEG_GRACE_SEC", 0.05)

    class Slow(FakeBroker):
        async def resolve_symbol(self, *args: Any, **kwargs: Any) -> Any:
            await asyncio.sleep(0.3)
            raise BrokerUnavailableError("MetaApi bağlantısı şu an yenileniyor (test)")

    path = str(tmp_path / "bot.db")
    db = Database(path)
    await db.open()
    old = (datetime.now(UTC) - timedelta(days=2)).isoformat()
    await db.set_state(MISSED, {"9:tickmill": {"trade_id": 9, "broker": "tickmill", "label": "#9 ETH LONG",
                                               "signal_at": old, "missed_at": old, "why": "x", "raw_symbol": None}})
    engine = Engine(_cfg(), db, object(), [FakeBroker("binance"), Slow("tickmill", optional=True)], FakeNotifier(),
                    price_oracle=FakePriceOracle({"ETH": 2232.85}))
    try:
        await engine.start(monitor=False)
        assert await db.get_state(MISSED, {}) == {}            # older than a day: dropped at start
        await engine.execute_open(_plan(["binance", "tickmill"]), raw_symbol="ETHUSDT.P")
        await settle(engine)
        stored = await db.get_state(MISSED)
        assert list(stored) == ["1:tickmill"] and stored["1:tickmill"]["raw_symbol"] == "ETHUSDT.P"
    finally:
        await engine.stop()
        await db.close()


async def test_r5_an_optional_broker_that_connects_after_the_start_says_the_missed_signals_are_looked_at(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from copytrader.engine import orchestrator as orch

    monkeypatch.setattr(orch, "OPTIONAL_CONNECT_GRACE_SEC", 0.05)
    gate = asyncio.Event()

    class SlowConnect(FakeBroker):
        async def connect(self) -> None:
            await gate.wait()
            await super().connect()

    db = Database(":memory:")
    await db.open()
    tm = SlowConnect("tickmill", optional=True)
    notifier = FakeNotifier()
    engine = Engine(_cfg(), db, object(), [FakeBroker("binance"), tm], notifier,
                    price_oracle=FakePriceOracle({"ETH": 2232.85}))
    try:
        await engine.start(monitor=False)
        assert "tickmill" in engine.optional_down
        await engine.execute_open(_plan(["binance", "tickmill"]))
        assert list(await db.get_state(MISSED)) == ["1:tickmill"]
        gate.set()
        await settle(engine)
        assert ("✅ <b>tickmill</b> bağlandı (bot başladıktan sonra). Bundan sonraki sinyaller bu hesapta da açılacak; "
                "bağlanana kadar gelen sinyaller bu hesapta açılmadı (kendiliğinden açılmaz; bağlantı yokken "
                "atlananlar için ayrıca bilgi verilecek).") in notifier.texts()
        assert [c.kind for c in notifier.confirmations] == ["catch_up"]
    finally:
        await engine.stop()
        await db.close()


async def test_r1_a_stop_while_the_question_is_still_being_sent_never_hangs_and_closes_it(tmp_path: Path) -> None:
    class Hang(FakeNotifier):                              # Telegram does not answer while the question goes out
        hang = False

        async def ask_confirmation(self, conf: PendingConfirmation) -> int | None:
            if self.hang:
                await asyncio.sleep(3600)
            return await super().ask_confirmation(conf)

    path = str(tmp_path / "bot.db")
    db = Database(path)
    await db.open()
    tm = FakeBroker("tickmill", optional=True, fail={"resolve_symbol": BrokerUnavailableError("bağlantı yok (test)")})
    notifier = Hang()
    engine = Engine(_cfg(), db, object(), [FakeBroker("binance"), tm], notifier,
                    price_oracle=FakePriceOracle({"ETH": 2232.85}))
    await engine.start(monitor=False)
    await engine.execute_open(_plan(["binance", "tickmill"]))
    tm.fail.clear()
    notifier.hang = True
    engine.monitor.heartbeat.on_healthy("tickmill")
    await asyncio.sleep(0.1)
    await asyncio.wait_for(engine.stop(grace_sec=0.2), 10)
    ((closed, text),) = notifier.closed
    assert closed.kind == "catch_up" and text == "⌛ Bot kapandı; tickmill hesabında işlem açılmadı."
    await db.close()
    assert _table_kinds(path) <= OLD_KINDS


# =========================================================================== #
# Review round 2: a sync slot that is busy is not a fresh read; a failed read of the question store writes nothing
# =========================================================================== #
async def test_r6_evet_while_fx_is_busy_with_another_sync_opens_nothing(live: Prod) -> None:
    from copytrader.brokers.mt5_broker import MT5

    tickmill_down(live)
    await live.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
    await reconnect(live)
    (conf,) = questions(live)
    (trade,) = await live.trades()
    ticket = int((await live.positions(trade.id))["fx"].data["legs"][0]["ticket"])
    live.terminal.hit(ticket, MT5.DEAL_REASON_SL, 1.1600)  # the stop filled at fx ...
    monitor = live.engine.monitor
    lock = monitor._sync_locks.setdefault("fx", asyncio.Lock())
    await lock.acquire()                                   # ... and fx is busy with another trade's sync (for 3 s)
    monitor._sync_since["fx"] = time.monotonic() - 3.0
    try:
        result = await live.engine.handle_confirmation(conf.id, True)
    finally:
        lock.release()
        monitor._sync_since.pop("fx", None)
    await settle(live.engine)
    assert result == "⛔ tickmill hesabında işlem açılmadı: fx hesabında pozisyonun durumu şu an okunamadı."
    assert live.tickmill_orders() == []


async def test_r7_a_failed_read_of_the_question_store_never_wipes_the_other_questions(live: Prod) -> None:
    tickmill_down(live)
    await live.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
    await live.say(fx_pair("GBPUSD", Side.LONG, 1.3150, 1.3130, 1.3190), FX, "GBP LONG 1R")
    await reconnect(live)
    eur, gbp = questions(live)
    real = live.db.get_state
    reads = {"n": 0}

    async def flaky(key: str, default: Any = None) -> Any:
        if key == "catch_up_questions":
            reads["n"] += 1
            if reads["n"] == 2:                            # the read of the save that marks #1 answered
                raise RuntimeError("database is locked")
        return await real(key, default)

    live.db.get_state = flaky                              # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="database is locked"):
        await live.engine.handle_confirmation(eur.id, False)
    stored = await real("catch_up_questions", {})
    assert {k: v["status"] for k, v in stored.items()} == {eur.id: "pending", gbp.id: "pending"}
    assert (await live.engine.handle_confirmation(gbp.id, True)).startswith("✅ Evet")
    await settle(live.engine)
    assert [o[:2] for o in live.tickmill_orders()] == [("market_buy", "GBPUSD")]
