"""Catch-up mode (owner and friend's decision): a signal the optional tickmill account missed while it was not
connected is opened there AUTOMATICALLY once it is back, when the same conditions hold as for today's "Evet"
(mode "otomatik", the default); mode "sor" keeps the Evet / Hayır question. The mode is kept in the database and set
by the owner with /sonradan otomatik | /sonradan sor (/sonradan shows it).

End to end on the production replica (live config) of tests/test_preflight_scenarios.py, as tests/test_followup_ask_stop.py.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from test_followup_ask_stop import _cfg, questions, reconnect, settle, stop_at, tickmill_down
from test_friend_mirror import FRIEND, OWNER, World, _plan
from test_notifier import MsgEvent
from test_preflight_scenarios import (  # noqa: F401  (_isolate: autouse)
    EURUSD_LONG,
    FX,
    KRIPTO,
    Prod,
    _isolate,
    btc,
    fx_pair,
)

from copytrader.brokers.base import BrokerUnavailableError
from copytrader.brokers.mt5_broker import MT5
from copytrader.engine import orchestrator as orch
from copytrader.engine.orchestrator import Engine
from copytrader.models import ActionType, EntryType, ManagementPlan, PositionStatus, Side, TradeStatus
from copytrader.storage.db import Database

from fakes import FakeBroker, FakeNotifier, FakePriceOracle

AUTO = "🔁 <b>#{n} {label}</b> tickmill hesabında sonradan açılıyor (bağlantı yokken atlanmıştı; şartlar uygun):\n"


@pytest.fixture
async def live(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    p = await Prod.create(tmp_path, monkeypatch, replica="live")
    yield p
    await p.close()


def told(p: Prod) -> list[str]:
    return [t for t in p.texts() if "AÇILMAYACAK" in t]


# =========================================================================== #
async def test_auto_is_the_default_and_opens_once_with_the_current_levels(live: Prod) -> None:
    assert await live.engine.catch_up_mode() == "otomatik"
    tickmill_down(live)
    await live.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO, "btc 1R long")
    (trade,) = await live.trades()
    await live.say(stop_at(trade.id, "BTC", "USDT", 73500.0), KRIPTO, "stop 73500")    # moved while tickmill is away
    live.notifier.notifications.clear()
    await reconnect(live)

    assert live.texts()[0] == ("✅ <b>tickmill</b> bağlandı. Bundan sonraki sinyaller bu hesapta da açılacak (bağlantı "
                               "yokken atlananlar şartlar uygunsa şimdi sonradan açılacak, uygun değilse nedeni "
                               "bildirilecek).")
    assert questions(live) == [] and live.notifier.confirmations == []      # never asked
    # the current stop (shifted by tickmill's -50), the signal's own stop for the size (as the Evet path)
    assert live.tickmill_orders() == [("market_buy", "BTCUSD", 0.06, 73450.0, 75450.0)]
    texts = live.texts()
    (auto,) = [i for i, t in enumerate(texts) if t.startswith(AUTO.format(n=1, label="BTC LONG"))]
    for line in ("• Sinyal fiyatı: 74000 (", "• Şimdiki fiyat: 73951 (tickmill fiyatı; sinyal grafiğinde ~74001)",
                 "• Stop: 73500 (tickmill fiyatıyla 73450) — sinyalde 73300 idi, sonradan taşındı",
                 "• Hedef: 75500 (tickmill fiyatıyla 75450)",
                 ("Piyasa emriyle, işlemin şu anki stop ve hedefleriyle, normal miktarla açılıyor; sonuç ayrıca "
                  "bildirilecek.")):
        assert line in texts[auto], (line, texts[auto])
    (done,) = [i for i, t in enumerate(texts) if "sonradan açıldı" in t]
    assert texts[done].startswith("✅ <b>#1 BTC LONG</b> — tickmill hesabında sonradan açıldı (otomatik; şartlar "
                                  "uygundu)\n• tickmill: 0.06 BTCUSD (açık)")
    assert auto < done                                                      # the notice first, then the result
    await live.engine.monitor.heartbeat.check_once()                        # never twice
    await reconnect(live)
    assert len(live.tickmill_orders()) == 1 and await live.db.get_state("missed_optional_legs") == {}


REASONS = {      # the reason in the "not caught up" notice
    "pending_limit": ("işlem diğer hesaplarda bekleyen limit emri olarak duruyor (giriş henüz dolmadı); bekleyen bir "
                      "işlem sonradan eklenmez"),
    "price_ran_away": ("anlık fiyat (1.16405) sinyal fiyatından (1.162) %0.18 uzak; botun piyasa emriyle giriş sınırı "
                       "sinyal fiyatından en fazla %0.03 ya da stop mesafesinin 0.25 katı"),
    "too_old": "sinyalin üzerinden 16 dk geçti; sonradan açma yalnızca sinyalden sonraki 15 dk içinde yapılır",
    "paused": "yeni işlem açma duraklatılmış (/devam ile açılır)",
    "closed": "işlem zaten kapanmış",
    "stopped_at_venue": "işlem zaten kapanmış",       # seen by the fresh venue read right before the open
    "partly_closed": "fx hesabında işlemin bir kısmı kapatılmış",
    "already_recorded": "bu işlemin tickmill hesabında zaten bir kaydı var",
}


@pytest.mark.parametrize("name", list(REASONS))
async def test_auto_opens_nothing_when_a_condition_fails_and_says_why(live: Prod, name: str) -> None:
    reason = REASONS[name]
    tickmill_down(live)
    if name == "pending_limit":
        await live.say(fx_pair("EURUSD", Side.LONG, 1.1580, 1.1560, 1.1660, entry_type=EntryType.LIMIT), FX, "limit")
    else:
        await live.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
    (trade,) = await live.trades()
    if name == "price_ran_away":
        live.tickmill.set_price("EURUSD", 1.16395, 1.16405)
    elif name == "too_old":
        live.engine._clock = lambda: datetime.now(UTC) + timedelta(minutes=16)
    elif name == "paused":
        await live.engine.set_paused(True)
    elif name == "closed":
        await live.engine.execute_management(_close_plan(trade.id))
        assert (await live.db.get_trade(trade.id)).status is TradeStatus.CLOSED
    elif name == "stopped_at_venue":                    # filled at fx; the monitor has not looked yet
        ticket = int((await live.positions(trade.id))["fx"].data["legs"][0]["ticket"])
        live.terminal.hit(ticket, MT5.DEAL_REASON_SL, 1.1600)
    elif name == "partly_closed":
        await live.engine.execute_management(_close_plan(trade.id, 0.5))
    elif name == "already_recorded":                    # (the double-open guard)
        fx = (await live.positions(trade.id))["fx"]
        await live.db.save_broker_position(trade.id, fx.model_copy(update={"broker": "tickmill"}))
    live.notifier.notifications.clear()
    await reconnect(live)

    assert live.tickmill_orders() == [] and live.notifier.confirmations == []
    assert not any(t.startswith("🔁") for t in live.texts())
    assert told(live) == [
        ("ℹ️ <b>tickmill</b> yeniden bağlı. Bağlantı yokken atlanan şu işlem tickmill hesabında sonradan AÇILMAYACAK:\n"
         f"• <b>#1 EUR LONG</b>: {reason}")]


def _close_plan(trade_id: int, fraction: float = 1.0) -> Any:
    return ManagementPlan(plan_id="m1", action=ActionType.CLOSE, trade_id=trade_id, close_fraction=fraction,
                          source_chat_id=-1003782294463, summary_tr="kapat", created_at=datetime.now(UTC))


async def test_sor_keeps_the_question(live: Prod) -> None:
    assert await live.engine.catch_up_command("sor") == (
        "✅ Bağlantı yokken atlanan tickmill işlemleri artık: <b>sor</b> — bağlantı gelince şartlar uygunsa Evet / "
        "Hayır diye sorulur (arkadaş ayarlıysa ona).")
    tickmill_down(live)
    await live.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
    await reconnect(live)
    (conf,) = questions(live)
    assert live.tickmill_orders() == [] and not any(t.startswith("🔁") for t in live.texts())
    assert (await live.engine.handle_confirmation(conf.id, True)).startswith("✅ Evet: tickmill hesabına emir")
    await settle(live.engine)
    assert live.tickmill_orders() == [("market_buy", "EURUSD", 0.21, 1.16, 1.166)]


async def test_the_mode_is_kept_across_a_restart_and_only_the_owner_sets_it(tmp_path: Path) -> None:
    world = await World(tmp_path).up()
    try:
        await world.owner_says("/sonradan")
        assert world.client.to(OWNER)[-1].text == (
            "🔁 Bağlantı yokken atlanan tickmill işlemleri: <b>otomatik</b> — bağlantı gelince şartlar uygunsa "
            "sormadan açılır. Değiştirmek için: <code>/sonradan otomatik</code> ya da <code>/sonradan sor</code>")
        await world.owner_says("/sonradan nedir")
        assert world.client.to(OWNER)[-1].text == ("Kullanım: <code>/sonradan otomatik</code>, "
                                                   "<code>/sonradan sor</code> ya da <code>/sonradan</code>")
        await world.owner_says(f"/arkadas {FRIEND}")
        await world.n.handle_message(MsgEvent("/sonradan sor", sender_id=FRIEND))   # the friend cannot change it
        await world.settle()
        assert await world.engine.catch_up_mode() == "otomatik"
        await world.owner_says("/sonradan sor")
        assert world.client.to(OWNER)[-1].text.startswith("✅ Bağlantı yokken atlanan tickmill işlemleri artık: "
                                                          "<b>sor</b>")
    finally:
        await world.down()
    again = await World(tmp_path, world.path).up()          # a restart keeps it
    try:
        assert await again.engine.catch_up_mode() == "sor"
        conf = await again.ask_catch_up()                   # asked, not opened
        assert conf.kind == "catch_up" and again.tm.opened == []
        await again.owner_says("/yardim")
        assert "/sonradan otomatik" in again.client.to(OWNER)[-1].text
    finally:
        await again.down()


async def test_auto_tells_the_friend_too_and_never_asks_him(tmp_path: Path) -> None:
    world = await World(tmp_path).up()
    try:
        await world.owner_says(f"/arkadas {FRIEND}")
        world.client.sent.clear()
        world.tm.fail = {"resolve_symbol": BrokerUnavailableError("bağlantı yok (test)")}
        await world.engine.execute_open(_plan(["binance", "tickmill"]))
        world.tm.fail = {}
        world.engine.monitor.heartbeat.on_healthy("tickmill")
        await world.settle()
        assert len(world.tm.opened) == 1 and await world.engine._catch_up_pending() == []
        for chat in (OWNER, FRIEND):
            texts = [m.text for m in world.client.to(chat)]
            assert sum(AUTO.format(n=1, label="ETH LONG") in t for t in texts) == 1, (chat, texts)
            assert any("tickmill hesabında sonradan açıldı (otomatik; şartlar uygundu)" in t for t in texts), chat
            assert not any(m.buttons for m in world.client.to(chat))
    finally:
        await world.down()



async def test_a_broker_that_connects_after_the_start_says_the_missed_signals_are_opened(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        await engine.execute_open(_plan(["binance", "tickmill"]))
        gate.set()
        await settle(engine)
        assert ("✅ <b>tickmill</b> bağlandı (bot başladıktan sonra). Bundan sonraki sinyaller bu hesapta da açılacak; "
                "bağlanana kadar gelen sinyaller bu hesapta açılmadı (şartlar uygunsa şimdi sonradan açılacak, uygun "
                "değilse nedeni bildirilecek).") in notifier.texts()
        assert notifier.confirmations == [] and len(tm.opened) == 1
        assert any(t.startswith(AUTO.format(n=1, label="ETH LONG")) for t in notifier.texts())
    finally:
        await engine.stop()
        await db.close()


# =========================================================================== #
# review round: double opens, storage faults, stops (World: real Engine + BotNotifier, tickmill a FakeBroker)
# =========================================================================== #
UNAV = BrokerUnavailableError("bağlantı yok (test)")
NOT_CAUGHT_UP = ("ℹ️ <b>tickmill</b> yeniden bağlı. Bağlantı yokken atlanan şu işlem tickmill hesabında sonradan "
                 "AÇILMAYACAK:\n• <b>#1 ETH LONG</b>: ")
KEPT = ("ℹ️ <b>tickmill</b> yeniden bağlı. Bağlantı yokken atlanan şu işlem tickmill hesabında şimdi açılmadı:\n"
        "• <b>#1 ETH LONG</b>: ")
MAY_OPEN = "\nBot sinyalden sonraki 15 dk içinde yeniden başlarsa şartlar uygunsa bir kez açılabilir."


async def _miss(world: World) -> None:
    world.tm.fail = {"resolve_symbol": UNAV}
    await world.engine.execute_open(_plan(["binance", "tickmill"]))
    world.tm.fail = {}
    assert list(await world.db.get_state("missed_optional_legs")) == ["1:tickmill"]


def _gate_open(tm: FakeBroker) -> asyncio.Event:
    """tickmill's open waits for the returned event (the order is on its way, unanswered); ``tm.entered`` counts."""
    gate = asyncio.Event()
    orig = tm.open_position
    tm.entered = 0  # type: ignore[attr-defined]

    async def slow(req: Any) -> Any:
        tm.entered += 1  # type: ignore[attr-defined]
        await gate.wait()
        return await orig(req)

    tm.open_position = slow  # type: ignore[method-assign]
    return gate


def _hold_sync(world: World) -> tuple[asyncio.Event, asyncio.Event]:
    """The owner's-broker sync before the open waits for ``hold``; ``entered`` is set once it is waiting."""
    hold, entered = asyncio.Event(), asyncio.Event()
    orig = world.engine.monitor.sync_trade

    async def slow_sync(trade_id: int, **kw: Any) -> Any:
        entered.set()
        await hold.wait()
        return await orig(trade_id, **kw)

    world.engine.monitor.sync_trade = slow_sync  # type: ignore[method-assign]
    return hold, entered


async def _until(cond: Any, timeout: float = 3.0) -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not cond():
        assert loop.time() < end
        await asyncio.sleep(0.01)


async def _stop(world: World, grace: float = 0.2) -> None:
    await world.engine.stop(grace_sec=grace)
    await world.n.stop()
    await world.db.close()


def _owner(world: World) -> list[str]:
    return [m.text for m in world.client.to(OWNER)]


async def test_r10_a_miss_that_could_not_be_dropped_from_storage_is_not_opened(tmp_path: Path) -> None:
    """The removal of the miss is not written (one failed write): a restart while the order is unanswered would
    open it again, so it is not opened now; the next start (the miss still stored) opens it once, with every check."""
    world = await World(tmp_path).up()
    await world.owner_says(f"/arkadas {FRIEND}")
    gate = _gate_open(world.tm)
    orig = world.db.set_state
    fails = []

    async def flaky(key: str, value: Any) -> Any:
        if key == "missed_optional_legs" and not fails:
            fails.append(key)
            raise RuntimeError("disk I/O error (test)")
        return await orig(key, value)

    try:
        await _miss(world)
        world.client.sent.clear()
        world.db.set_state = flaky  # type: ignore[method-assign]
        world.engine.monitor.heartbeat.on_healthy("tickmill")
        await world.settle()
        assert fails and world.tm.entered == 0                                       # nothing sent
        assert _owner(world) == [KEPT + "kayıt silinemedi; iki kez açılmaması için açılmadı" + MAY_OPEN]
        assert [m.text for m in world.client.to(FRIEND)] == _owner(world)            # (never "AÇILMAYACAK")
        assert list(await world.db.get_state("missed_optional_legs")) == ["1:tickmill"]
    finally:
        world.db.set_state = orig  # type: ignore[method-assign]
        gate.set()
        await _stop(world)
    again = await World(tmp_path, world.path).up()
    try:
        again.engine.monitor.heartbeat.on_healthy("tickmill")
        await again.settle()
        assert len(again.tm.opened) == 1 and await again.db.get_state("missed_optional_legs") == {}
    finally:
        await again.down()


async def test_r5_an_unreadable_mode_opens_nothing_and_the_next_check_retries(tmp_path: Path) -> None:
    world = await World(tmp_path).up()
    orig = world.db.get_state

    async def flaky(key: str, default: Any = None) -> Any:
        if key == "catch_up_mode":
            raise RuntimeError("database is locked")
        return await orig(key, default)

    try:
        await world.owner_says("/sonradan sor")
        await _miss(world)
        world.client.sent.clear()
        world.db.get_state = flaky  # type: ignore[method-assign]
        world.engine.monitor.heartbeat.on_healthy("tickmill")
        await world.settle()
        assert world.tm.opened == [] and await world.engine._catch_up_pending() == []
        assert _owner(world) == []                                                   # nothing told: kept
        assert list(await orig("missed_optional_legs")) == ["1:tickmill"]
        await world.owner_says("/sonradan")
        assert _owner(world)[-1] == ("⚠️ Ayar şu an okunamadı (<code>database is locked</code>); biraz sonra "
                                     "tekrar deneyin.")
        world.db.get_state = orig  # type: ignore[method-assign]
        world.engine.monitor.heartbeat.on_healthy("tickmill")                        # the next pass
        await world.settle()
        assert [c.kind for c in await world.engine._catch_up_pending()] == ["catch_up"] and world.tm.opened == []
    finally:
        world.db.get_state = orig  # type: ignore[method-assign]
        await world.down()


async def test_r4_no_order_once_the_stop_began_during_the_sync_before_the_open(tmp_path: Path) -> None:
    world = await World(tmp_path).up()
    hold, entered = _hold_sync(world)
    try:
        await _miss(world)
        world.client.sent.clear()
        world.engine.monitor.heartbeat.on_healthy("tickmill")
        await asyncio.wait_for(entered.wait(), 3)
        world.engine.begin_shutdown()
        hold.set()
        await world.settle()
        assert world.tm.opened == [] and not any("🔁" in t for t in _owner(world))
        assert _owner(world) == [NOT_CAUGHT_UP + "bot kapanıyor"]
    finally:
        await world.down()


async def test_r3_a_stop_during_the_sync_before_the_open_is_told(tmp_path: Path) -> None:
    world = await World(tmp_path).up()
    _hold, entered = _hold_sync(world)                  # (never released: the stop cancels the catch-up)
    try:
        await _miss(world)
        world.client.sent.clear()
        world.engine.monitor.heartbeat.on_healthy("tickmill")
        await asyncio.wait_for(entered.wait(), 3)
    finally:
        await _stop(world)
    assert world.tm.opened == []
    assert [t for t in _owner(world) if "AÇILMAYACAK" in t] == [NOT_CAUGHT_UP + "bot kapandığı için emir gönderilmedi"]
    again = await World(tmp_path, world.path).up()
    try:
        again.engine.monitor.heartbeat.on_healthy("tickmill")
        await again.settle()
        assert again.tm.opened == [] and _owner(again) == []                         # told once, before the stop
    finally:
        await again.down()


async def test_r5b_a_stop_that_began_before_the_miss_was_dropped_keeps_it_for_the_next_start(tmp_path: Path) -> None:
    world = await World(tmp_path).up()
    orig = world.engine._catch_up_problem

    async def then_stop(*a: Any, **kw: Any) -> Any:
        result = await orig(*a, **kw)
        world.engine.begin_shutdown()                   # (a deploy: the stop arrives while the miss is looked at)
        return result

    try:
        await _miss(world)
        world.client.sent.clear()
        world.engine._catch_up_problem = then_stop  # type: ignore[method-assign]
        world.engine.monitor.heartbeat.on_healthy("tickmill")
        await world.settle()
        assert world.tm.opened == [] and _owner(world) == []
        assert list(await world.db.get_state("missed_optional_legs")) == ["1:tickmill"]
    finally:
        await _stop(world)
    again = await World(tmp_path, world.path).up()
    try:
        again.engine.monitor.heartbeat.on_healthy("tickmill")
        await again.settle()
        assert len(again.tm.opened) == 1                                             # with every check, once
    finally:
        await again.down()


async def test_r1_a_flapping_link_and_a_monitor_pass_during_the_open_never_open_twice(tmp_path: Path) -> None:
    world = await World(tmp_path).up()
    try:
        await _miss(world)
        gate = _gate_open(world.tm)
        world.engine.monitor.heartbeat.on_healthy("tickmill")
        await _until(lambda: world.tm.entered == 1)
        world.engine.optional_down["tickmill"] = "kopuk (test)"
        world.engine._broker_healthy("tickmill")
        world.engine.optional_down.pop("tickmill", None)
        for _ in range(3):
            world.engine.monitor.heartbeat.on_healthy("tickmill")
            world.engine._start_catch_up_check("tickmill")
            await asyncio.sleep(0.02)
        async with world.engine.trade_lock(1):
            await world.engine.monitor.sync_trade(1)
        gate.set()
        await world.settle()
        for _ in range(3):
            world.engine.monitor.heartbeat.on_healthy("tickmill")
            await world.settle()
        assert world.tm.entered == 1 and len(world.tm.opened) == 1
    finally:
        await world.down()


async def test_r2_a_restart_while_the_open_is_unanswered_never_opens_again(tmp_path: Path) -> None:
    world = await World(tmp_path).up()
    gate = _gate_open(world.tm)
    try:
        await _miss(world)
        world.engine.monitor.heartbeat.on_healthy("tickmill")
        await _until(lambda: world.tm.entered == 1)
    finally:
        await _stop(world)
    again = await World(tmp_path, world.path).up()
    try:
        again.engine.monitor.heartbeat.on_healthy("tickmill")
        await again.settle()
        assert again.tm.opened == [] and await again.db.get_state("missed_optional_legs") == {}
    finally:
        gate.set()
        await again.down()


async def test_r6_the_notice_comes_before_a_failed_result_in_both_chats(tmp_path: Path) -> None:
    world = await World(tmp_path).up()
    try:
        await world.owner_says(f"/arkadas {FRIEND}")
        await _miss(world)
        world.client.sent.clear()
        orig = world.tm.open_position

        async def failed(req: Any) -> Any:
            pos = await orig(req)
            return pos.model_copy(update={"status": PositionStatus.FAILED, "error": "stop yerleştirilemedi (test)"})

        world.tm.open_position = failed  # type: ignore[method-assign]
        world.engine.monitor.heartbeat.on_healthy("tickmill")
        await world.settle()
        for chat in (OWNER, FRIEND):
            texts = [m.text for m in world.client.to(chat)]
            (auto,) = [i for i, t in enumerate(texts) if AUTO.format(n=1, label="ETH LONG") in t]
            (error,) = [i for i, t in enumerate(texts) if t.startswith("🚨 tickmill: <code>stop yerleştirilemedi")]
            (done,) = [i for i, t in enumerate(texts) if "sonradan açılamadı (otomatik; şartlar uygundu)" in t]
            assert auto < error < done, (chat, texts)
    finally:
        await world.down()


async def test_r7_the_friend_texts_hold_in_both_modes_and_turkish_capitals_work(tmp_path: Path) -> None:
    world = await World(tmp_path).up()
    try:
        await world.owner_says(f"/arkadas {FRIEND}")
        assert world.client.to(FRIEND)[0].text == (
            "👋 Merhaba! Bu sohbet, kopya-trade botunun bildirimlerini almak için bot sahibi tarafından eklendi. Bundan "
            "sonra bot sahibine giden her bildirim buraya da gelir. tickmill bağlı değilken gelen bir işlem, bağlantı "
            "gelince şartlar uygunsa bot sahibinin ayarına göre ya kendiliğinden açılır ya da size \"sonradan açılsın "
            "mı?\" diye sorulur (✅ Evet / ❌ Hayır). Bu sohbetten bot komutları çalıştırılamaz.")
        await world.owner_says("/arkadas")
        assert _owner(world)[-1] == (
            f"👤 Arkadaşınızın sohbeti: <code>{FRIEND}</code>. Size gelen her bildirim ona da gider; bağlantı yokken "
            "atlanan tickmill işlemleri <code>/sonradan</code> ayarına göre ya kendiliğinden açılır ya da ona sorulur. "
            "Kaldırmak için: <code>/arkadas sil</code>")
        await world.owner_says("/sonradan SOR")
        assert await world.engine.catch_up_mode() == "sor"
        await world.owner_says("/sonradan OTOMATİK")
        assert _owner(world)[-1] == ("✅ Bağlantı yokken atlanan tickmill işlemleri artık: <b>otomatik</b> — bağlantı "
                                     "gelince şartlar uygunsa sormadan açılır.")
        assert await world.engine.catch_up_mode() == "otomatik"
    finally:
        await world.down()


async def test_r8_an_unanswered_open_never_holds_the_owners_new_signal_or_management(tmp_path: Path) -> None:
    import time

    world = await World(tmp_path).up()
    gate = _gate_open(world.tm)
    try:
        await _miss(world)
        world.engine.monitor.heartbeat.on_healthy("tickmill")
        await _until(lambda: world.tm.entered == 1)
        binance = world.engine._broker("binance")
        before = len(binance.opened)
        t0 = time.monotonic()
        await world.engine.execute_open(_plan(["binance", "tickmill"]).model_copy(update={"plan_id": "p2"}))
        assert time.monotonic() - t0 < 1.0 and len(binance.opened) == before + 1
        t0 = time.monotonic()
        await world.engine.execute_management(ManagementPlan(
            plan_id="m1", action=ActionType.MOVE_SL, trade_id=1, new_stop_loss=2210.0, source_chat_id=-1,
            summary_tr="stop 2210", created_at=datetime.now(UTC)))
        assert time.monotonic() - t0 < 1.0 and binance.calls_of("update_stop_loss")
    finally:
        gate.set()
        await world.down()


async def test_r9_a_question_left_open_then_otomatik_opens_only_on_evet(tmp_path: Path) -> None:
    world = await World(tmp_path).up()
    try:
        await world.owner_says("/sonradan sor")
        conf = await world.ask_catch_up()
        await world.owner_says("/sonradan otomatik")
        for _ in range(3):
            world.engine.monitor.heartbeat.on_healthy("tickmill")
            await world.settle()
        assert world.tm.opened == []
        await world.engine.handle_confirmation(conf.id, True)
        await world.settle()
        world.engine.monitor.heartbeat.on_healthy("tickmill")
        await world.settle()
        assert len(world.tm.opened) == 1
    finally:
        await world.down()


async def test_n1_a_refused_miss_that_could_not_be_dropped_is_not_told_as_never(tmp_path: Path) -> None:
    """Paused, and the removal of the miss is not written: a restart within 15 minutes looks at it again (and could
    open it once /devam was given), so it is not listed as "AÇILMAYACAK"."""
    world = await World(tmp_path).up()
    orig = world.db.set_state

    async def flaky(key: str, value: Any) -> Any:
        if key == "missed_optional_legs":
            raise RuntimeError("disk I/O error (test)")
        return await orig(key, value)

    try:
        await _miss(world)
        await world.engine.set_paused(True)
        world.client.sent.clear()
        world.db.set_state = flaky  # type: ignore[method-assign]
        world.engine.monitor.heartbeat.on_healthy("tickmill")
        await world.settle()
        assert world.tm.opened == []
        assert _owner(world) == [KEPT + "yeni işlem açma duraklatılmış (/devam ile açılır); kayıt da silinemedi"
                                 + MAY_OPEN]
    finally:
        world.db.set_state = orig  # type: ignore[method-assign]
        await world.down()


def _mode_unreadable_once(db: Any) -> None:
    orig = db.get_state
    reads: list[str] = []

    async def once(key: str, default: Any = None) -> Any:
        if key == "catch_up_mode":
            reads.append(key)
            if len(reads) == 1:
                raise RuntimeError("database is locked")
        return await orig(key, default)

    db.get_state = once


async def test_n2_the_connect_notice_is_true_in_both_modes_when_the_mode_cannot_be_read(live: Prod) -> None:
    """The first read of the mode (for the connect notice) fails, the next one (the catch-up check) works: the
    notice says neither "kendiliğinden açılmaz" nor that it will be opened."""
    tickmill_down(live)
    await live.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
    _mode_unreadable_once(live.db)
    live.notifier.notifications.clear()
    await reconnect(live)
    texts = live.texts()
    assert texts[0] == ("✅ <b>tickmill</b> bağlandı. Bundan sonraki sinyaller bu hesapta da açılacak (bağlantı yokken "
                        "atlananlar için ayrıca bilgi verilecek).")
    assert any(t.startswith(AUTO.format(n=1, label="EUR LONG")) for t in texts)
    assert live.tickmill_orders() == [("market_buy", "EURUSD", 0.21, 1.16, 1.166)]


async def test_n2_the_after_start_connect_notice_too(monkeypatch: pytest.MonkeyPatch) -> None:
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
        await engine.execute_open(_plan(["binance", "tickmill"]))
        _mode_unreadable_once(db)
        gate.set()
        await settle(engine)
        assert ("✅ <b>tickmill</b> bağlandı (bot başladıktan sonra). Bundan sonraki sinyaller bu hesapta da açılacak; "
                "bağlanana kadar gelen sinyaller bu hesapta açılmadı (bağlantı yokken atlananlar için ayrıca bilgi "
                "verilecek).") in notifier.texts()
        assert len(tm.opened) == 1
    finally:
        await engine.stop()
        await db.close()
