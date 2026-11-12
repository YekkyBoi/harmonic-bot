"""Failure isolation: an ``optional`` broker that cannot connect / fails at runtime never hurts the others."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from copytrader.brokers.base import BrokerError
from copytrader.config import AppConfig, NotifierConfig, TelegramConfig, load_config
from copytrader.engine.monitor import Monitor
from copytrader.engine.orchestrator import Engine
from copytrader.models import (
    BrokerPosition, EntryType, PositionStatus, Severity, Side, TakeProfit, TradePlan, TradeStatus,
)
from copytrader.storage.db import Database

from fakes import FakeBroker, FakeNotifier, FakePriceOracle

CHAT = -1001234567890


def cfg() -> AppConfig:
    c = AppConfig(telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[CHAT]),
                  notifier=NotifierConfig(bot_token="123:abc", owner_user_id=42))
    c.monitor_interval_sec = 3600.0
    return c


def plan(names: list[str], **kw: object) -> TradePlan:
    data: dict[str, object] = dict(
        plan_id="p1", base_asset="ETH", side=Side.LONG, entry_type=EntryType.MARKET, entry_price=2232.0,
        stop_loss=2200.0, take_profits=[TakeProfit(price=2300.0, close_fraction=1.0)], risk_r=1.0, risk_pct=2.0,
        source_chat_id=CHAT, source_message_ids=[1], created_at=datetime.now(timezone.utc), broker_names=names,
    )
    data.update(kw)
    return TradePlan(**data)  # type: ignore[arg-type]


@pytest.fixture
async def db():
    d = Database(":memory:")
    await d.open()
    yield d
    await d.close()


async def make(db, brokers, retry_sec: float = 0.05):
    notifier = FakeNotifier()
    engine = Engine(cfg(), db, object(), brokers, notifier, price_oracle=FakePriceOracle({"ETH": 2232.85}))
    engine.optional_retry_sec = retry_sec
    await engine.start(monitor=False)
    return engine, notifier


async def wait_for(pred, timeout: float = 2.0) -> None:
    end = asyncio.get_running_loop().time() + timeout
    while not pred():
        assert asyncio.get_running_loop().time() < end, "condition not met in time"
        await asyncio.sleep(0.01)


def warnings(n: FakeNotifier) -> list[str]:
    return n.texts(Severity.WARNING)


async def test_default_is_not_optional() -> None:
    assert FakeBroker("x").config.optional is False
    assert FakeBroker("x", optional=True).config.optional is True


async def test_optional_connect_failure_does_not_stop_bot_and_warns_once(db) -> None:
    binance = FakeBroker("binance")
    tm = FakeBroker("tickmill", optional=True, fail={"connect": BrokerError("token geçersiz")})
    engine, notifier = await make(db, [binance, tm], retry_sec=3600)
    try:
        assert [b.name for b in engine.brokers] == ["binance"]
        assert "tickmill" in engine.optional_down and "tickmill" in engine.failed_brokers
        warns = [t for t in warnings(notifier) if "tickmill" in t and "bağlanamadı" in t]
        assert len(warns) == 1 and "token geçersiz" in warns[0] and "isteğe bağlı" in warns[0]
        assert not notifier.texts(Severity.ERROR)          # a warning, not an alarm
    finally:
        await engine.stop()


async def test_required_broker_failure_is_an_alarm_and_is_retried_in_the_background(db) -> None:
    tm = FakeBroker("tickmill", fail={"connect": BrokerError("x")})
    engine, notifier = await make(db, [FakeBroker("binance"), tm], retry_sec=3600)
    try:
        assert any("tickmill bağlanamadı" in t for t in notifier.texts(Severity.ERROR))
        assert engine.optional_down == {}                    # an alarm, not the optional broker's short warning
        # ops finding 4: a required broker is retried in the background too (it used to stay down until a restart)
        assert [t.get_name() for t in engine._tasks] == ["retry-tickmill"]
    finally:
        await engine.stop()


async def test_signal_skipped_for_down_optional_others_trade(db) -> None:
    binance = FakeBroker("binance")
    tm = FakeBroker("tickmill", optional=True, fail={"connect": BrokerError("down")})
    engine, notifier = await make(db, [binance, tm], retry_sec=3600)
    try:
        notifier.notifications.clear()
        await engine.execute_open(plan(["binance", "tickmill"]))
        assert len(binance.opened) == 1 and tm.opened == []
        skips = [t for t in warnings(notifier) if "tickmill bağlı değil, bu işlem atlandı" in t]
        assert len(skips) == 1                      # once per signal
        assert not notifier.texts(Severity.ERROR)
        await engine.execute_open(plan(["binance", "tickmill"], plan_id="p2"))
        assert len([t for t in warnings(notifier) if "tickmill bağlı değil, bu işlem atlandı" in t]) == 2
    finally:
        await engine.stop()


async def test_route_with_only_down_optional_is_soft_and_not_queued(db) -> None:
    tm = FakeBroker("tickmill", optional=True, fail={"connect": BrokerError("down")})
    engine, notifier = await make(db, [FakeBroker("binance"), tm], retry_sec=3600)
    try:
        notifier.notifications.clear()
        text = await engine.execute_open(plan(["tickmill"]))
        assert "tickmill bağlı değil, bu işlem atlandı" in text
        assert not notifier.texts(Severity.ERROR)
        (trade,) = await db.active_trades() if await db.active_trades() else (None,)
        assert trade is None                         # FAILED, not queued / active
    finally:
        await engine.stop()


async def test_required_down_route_still_errors(db) -> None:
    tm = FakeBroker("tickmill", fail={"connect": BrokerError("down")})
    engine, notifier = await make(db, [FakeBroker("binance"), tm])
    try:
        notifier.notifications.clear()
        await engine.execute_open(plan(["tickmill"]))
        assert any("AÇILMADI" in t for t in notifier.texts(Severity.ERROR))
    finally:
        await engine.stop()


async def test_background_retry_recovers_silently_then_announces(db) -> None:
    binance = FakeBroker("binance")
    tm = FakeBroker("tickmill", optional=True, fail={"connect": BrokerError("down")})
    engine, notifier = await make(db, [binance, tm], retry_sec=0.03)
    try:
        await asyncio.sleep(0.15)                    # several failed retries
        assert [t for t in warnings(notifier) if "bağlanamadı" in t].__len__() == 1   # warned only once
        assert len(tm.calls_of("connect")) > 1
        del tm.fail["connect"]
        await wait_for(lambda: any("bağlandı" in t and "tickmill" in t for t in notifier.texts(Severity.INFO)))
        assert [b.name for b in engine.brokers] == ["binance", "tickmill"]
        assert engine.optional_down == {} and "tickmill" not in engine.failed_brokers
        notifier.notifications.clear()
        await engine.execute_open(plan(["binance", "tickmill"]))
        assert len(tm.opened) == 1 and len(binance.opened) == 1
    finally:
        await engine.stop()


async def test_stop_cancels_retry_task(db) -> None:
    tm = FakeBroker("tickmill", optional=True, fail={"connect": BrokerError("down")})
    engine, _ = await make(db, [tm], retry_sec=3600)
    assert any(t.get_name() == "retry-tickmill" for t in engine._tasks)
    await engine.stop()
    assert not any(not t.done() for t in engine._tasks)


async def test_runtime_error_of_optional_broker_is_isolated_open(db) -> None:
    binance = FakeBroker("binance")
    tm = FakeBroker("tickmill", optional=True, fail={"get_quote": RuntimeError("boom")})
    engine, notifier = await make(db, [binance, tm])
    try:
        await engine.execute_open(plan(["binance", "tickmill"]))
        assert len(binance.opened) == 1 and tm.opened == []
        trades = await db.active_trades()
        assert len(trades) == 1 and trades[0].status is TradeStatus.OPEN
    finally:
        await engine.stop()


async def test_monitor_isolates_raising_and_hanging_sync(db, monkeypatch: pytest.MonkeyPatch) -> None:
    from copytrader.engine import monitor as mon

    monkeypatch.setattr(mon, "SYNC_TIMEOUT_SEC", 0.05)
    binance = FakeBroker("binance")
    bad = FakeBroker("tickmill", optional=True)
    hang = FakeBroker("hang", optional=True)
    seen: list[str] = []

    async def boom(pos: BrokerPosition) -> BrokerPosition:
        raise RuntimeError("sdk exploded")

    async def never(pos: BrokerPosition) -> BrokerPosition:
        await asyncio.sleep(3600)
        return pos

    async def ok(pos: BrokerPosition) -> BrokerPosition:
        seen.append(pos.broker)
        return pos

    bad.sync_position = boom  # type: ignore[method-assign]
    hang.sync_position = never  # type: ignore[method-assign]
    binance.sync_position = ok  # type: ignore[method-assign]
    engine, notifier = await make(db, [bad, hang, binance])
    try:
        await engine.execute_open(plan(["binance", "tickmill", "hang"]))
        await asyncio.wait_for(engine.monitor.run_once(), 5)       # must neither raise nor hang
        assert seen == ["binance"]
    finally:
        await engine.stop()


def test_optional_in_yaml(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in {"TG_API_ID": "1", "TG_API_HASH": "h", "TG_BOT_TOKEN": "1:a", "TG_OWNER_ID": "4",
                 "ANTHROPIC_API_KEY": "k"}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.chdir(tmp_path)
    f = tmp_path / "c.yaml"
    f.write_text("""
telegram:
  api_id: ${TG_API_ID}
  api_hash: ${TG_API_HASH}
  source_chats: [-1001]
notifier:
  bot_token: ${TG_BOT_TOKEN}
  owner_user_id: ${TG_OWNER_ID}
llm:
  api_key: ${ANTHROPIC_API_KEY}
brokers:
  - {name: a, type: paper}
  - {name: b, type: paper, optional: true}
""")
    c = load_config(f)
    assert [b.optional for b in c.brokers] == [False, True]


async def test_build_app_optional_creation_failure(tmp_path) -> None:
    from copytrader.app import build_app, serve

    c = cfg()
    c.data_dir = str(tmp_path)
    c.brokers = [c_b for c_b in load_brokers()]

    def factory(bc):
        if bc.name == "tickmill":
            raise BrokerError("tickmill: paket yüklü değil")
        return FakeBroker(bc.name)

    notifier = FakeNotifier()
    app = build_app(c, db=Database(":memory:"), parser=object(), notifier=notifier, broker_factory=factory,
                    listener_factory=lambda *a, **k: _Listener())
    assert "tickmill" in app.engine.optional_down
    stop = asyncio.Event()
    task = asyncio.ensure_future(serve(app, stop))
    await asyncio.sleep(0.3)
    stop.set()
    await asyncio.wait_for(task, 5)
    assert any("İsteğe bağlı broker oluşturulamadı" in t for t in notifier.texts(Severity.WARNING))
    assert not any("oluşturulamadı" in t for t in notifier.texts(Severity.ERROR))


def load_brokers():
    from copytrader.config import PaperBrokerConfig

    return [PaperBrokerConfig(type="paper", name="binance"),
            PaperBrokerConfig(type="paper", name="tickmill", optional=True)]


class _Listener:
    def __init__(self) -> None:
        self._stop = asyncio.Event()

    async def start(self) -> None: ...
    async def run_until_disconnected(self) -> None:
        await self._stop.wait()

    async def stop(self) -> None:
        self._stop.set()
