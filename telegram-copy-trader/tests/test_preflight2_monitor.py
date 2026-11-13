"""Pre-flight batch 2, the position monitor (tasks D1-D4).

D1  The loop survives a CancelledError nobody requested (a broker SDK raises it); only request_stop() and a real
    cancellation end Monitor.run(). The "could not be checked" alert never shows an empty reason. The owner is
    told when the passes themselves keep failing.
D2  One slow position never delays the others: every trade is synced in its own task, the positions of a trade
    concurrently, and a pass does not wait for a sync that is still running. A broker still gets one sync (and
    one virtual entry) at a time, with a bounded wait: a broker that hangs holds up one trade, not every trade
    that has a position on it, so the engine can manage the others (their Binance legs) meanwhile.
D3  An unfilled exchange LIMIT entry of a ccxt broker is checked in the fast pass, so its stop is placed about
    VIRTUAL_POLL_SEC after the fill instead of up to monitor_interval_sec.
D4  Broker heartbeat: the owner is told once when a broker stops answering and once when it is back.

The brokers here are fakes: they prove the monitor's logic, not the behaviour of a real exchange or terminal.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from copytrader.brokers.base import BrokerError
from copytrader.brokers.dry_run import DryRunBroker
from copytrader.config import (
    AppConfig, CcxtBrokerConfig, ManualBrokerConfig, MetaApiBrokerConfig, Mt5BrokerConfig, NotifierConfig,
    PaperBrokerConfig, TelegramConfig,
)
from copytrader.engine import monitor as mon
from copytrader.engine.monitor import Monitor
from copytrader.engine.orchestrator import Engine
from copytrader.models import (
    ActionType, BrokerPosition, EntryType, ManagementPlan, PositionStatus, Quote, Severity, Side, TakeProfit,
    TradePlan, TradeRecord, TradeStatus,
)
from copytrader.storage.db import Database

from fakes import FakeBroker, FakeNotifier, FakePriceOracle

CHAT = -1001234567890


# --------------------------------------------------------------------------- #
# fakes
# --------------------------------------------------------------------------- #
def broker_config(kind: str, name: str) -> Any:
    """The config a real adapter of this kind carries (the monitor decides by ``config.type``)."""
    if kind == "ccxt":
        return CcxtBrokerConfig(type="ccxt", name=name, exchange="binanceusdm")
    if kind == "metaapi":
        return MetaApiBrokerConfig(type="metaapi", name=name, optional=True)
    if kind == "mt5":
        return Mt5BrokerConfig(type="mt5", name=name, login=1, password="x", server="s")
    if kind == "manual":
        return ManualBrokerConfig(type="manual", name=name)
    return PaperBrokerConfig(type="paper", name=name)


def poisoned() -> asyncio.Future[Any]:
    """What metaapi-cloud-sdk 29.1.1 awaits in every call while the account is disconnected: ONE shared future
    that an earlier caller's timeout cancelled. Awaiting it raises CancelledError at once although nobody
    cancelled the awaiting task (asyncio.current_task().cancelling() == 0)."""
    fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
    fut.cancel()
    return fut


class Venue(FakeBroker):
    """FakeBroker with the config type / kind of a real adapter (FakeBroker itself is a "paper" account) and
    switchable trouble in sync_position."""

    def __init__(self, name: str, kind: str = "ccxt", **kw: Any) -> None:
        super().__init__(name, **kw)
        self.config = broker_config(kind, name)
        self.kind = kind
        self.hang = False             # sync_position never returns (a hung RPC / terminal call)
        self.stray = False            # sync_position raises CancelledError the way the MetaApi SDK does
        self.delay = 0.0              # seconds one sync takes
        self.entered = 0              # sync calls started
        self.finished = 0             # sync calls that returned
        self.cancelled = 0            # sync calls ended by a REAL cancellation
        self.stamps: list[float] = [] # monotonic start time of every sync call
        self.seen: list[PositionStatus] = []     # status of the position of every sync call, in call order
        self.running = 0              # sync calls in flight right now
        self.most = 0                 # the most sync calls that were ever in flight at the same time
        self.release = asyncio.Event()

    async def sync_position(self, pos: BrokerPosition) -> BrokerPosition:
        self.entered += 1
        self.stamps.append(time.monotonic())
        self.seen.append(pos.status)
        self.running += 1
        self.most = max(self.most, self.running)
        try:
            if self.stray:
                await poisoned()
            if self.hang:
                await self.release.wait()
            if self.delay:
                await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                self.cancelled += 1
            raise
        finally:
            self.running -= 1
        self.finished += 1
        return await super().sync_position(pos)


class BinanceLike(Venue):
    """A ccxt broker the way Binance behaves for a LIMIT entry: the order rests WITHOUT a stop; the stop is
    placed by sync_position when it notices the fill."""

    def __init__(self, name: str = "binance") -> None:
        super().__init__(name, "ccxt")
        self.filled_at: float | None = None
        self.stop_placed_at: float | None = None
        self.stops_placed = 0

    def fill(self) -> None:
        """The exchange fills our resting limit order now."""
        self.filled_at = time.monotonic()

    async def sync_position(self, pos: BrokerPosition) -> BrokerPosition:
        pos = await super().sync_position(pos)
        if pos.status is PositionStatus.PENDING and self.filled_at is not None:
            self.stop_placed_at = time.monotonic()
            self.stops_placed += 1
            return pos.model_copy(update={
                "status": PositionStatus.OPEN, "filled_quantity": pos.quantity, "remaining_quantity": pos.quantity,
                "avg_entry_price": 73000.0, "data": {**pos.data, "sl_order_id": "sl-1", "entry_done": True}})
        return pos


def app_config(interval: float = 0.05) -> AppConfig:
    c = AppConfig(telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[CHAT]),
                  notifier=NotifierConfig(bot_token="123:abc", owner_user_id=42))
    c.dry_run = False
    c.monitor_interval_sec = interval
    return c


def position(broker: str, status: PositionStatus = PositionStatus.OPEN, **data: Any) -> BrokerPosition:
    is_open = status is PositionStatus.OPEN
    return BrokerPosition(
        broker=broker, broker_symbol="BTC/USDT:USDT", side=Side.LONG, status=status, quantity=0.012,
        filled_quantity=0.012 if is_open else 0.0, remaining_quantity=0.012 if is_open else 0.0,
        avg_entry_price=73000.0 if is_open else None, stop_loss=72300.0,
        take_profits=[TakeProfit(price=75000.0, close_fraction=1.0)], risk_amount=10.0,
        data={"contract_size": 1.0, **data},
    )


def pending(broker: str, **data: Any) -> BrokerPosition:
    return position(broker, PositionStatus.PENDING, entry_order_id="e-1", entry_type="limit", **data)


def virtual(broker: str) -> BrokerPosition:
    """A waiting virtual (reference-touch) entry as the engine stores it: no order exists on the broker."""
    return position(broker, PositionStatus.PENDING, virtual_trigger=True, virtual_state="waiting",
                    trigger_price=73000.0, reference_exchange="binanceusdm")


_SEQ = iter(range(1, 10_000))


async def add_trade(db: Database, *positions: BrokerPosition, status: TradeStatus = TradeStatus.OPEN) -> int:
    n = next(_SEQ)
    plan = TradePlan(
        plan_id=f"p{n}", base_asset="BTC", quote_asset="USDT", side=Side.LONG, entry_type=EntryType.LIMIT,
        entry_price=73000.0, stop_loss=72300.0, take_profits=[TakeProfit(price=75000.0, close_fraction=1.0)],
        risk_r=1.0, risk_pct=2.0, source_chat_id=CHAT, source_message_ids=[n],
        created_at=datetime.now(timezone.utc),
    )
    trade = await db.create_trade(plan, status)
    for pos in positions:
        await db.save_broker_position(trade.id, pos)
    return trade.id


def make_monitor(db: Database, brokers: list[Any], interval: float = 0.05) -> tuple[Monitor, FakeNotifier]:
    notifier = FakeNotifier()
    return Monitor(app_config(interval), db, brokers, notifier), notifier


async def wait_until(pred: Callable[[], bool], timeout: float = 5.0, *, task: asyncio.Future[Any] | None = None,
                     what: str = "condition") -> None:
    end = time.monotonic() + timeout
    while not pred():
        assert task is None or not task.done(), f"the monitor task ended while waiting for: {what}"
        assert time.monotonic() < end, f"not met within {timeout:g}s: {what}"
        await asyncio.sleep(0.005)


@contextlib.asynccontextmanager
async def looping(monitor: Monitor) -> AsyncIterator[asyncio.Task[None]]:
    """Monitor.run() in the background; cancelled at the end of the block."""
    task = asyncio.ensure_future(monitor.run())
    try:
        yield task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.fixture
async def db() -> AsyncIterator[Database]:
    d = Database(":memory:")
    await d.open()
    yield d
    await d.close()


@pytest.fixture(autouse=True)
async def no_task_left_behind() -> AsyncIterator[None]:
    """Whatever the monitor starts (sync tasks, the heartbeat) must be gone when it has ended."""
    yield
    await asyncio.sleep(0)
    left = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    assert left == [], f"tasks left running: {[t.get_name() for t in left]}"


@pytest.fixture
def health() -> Any:
    from copytrader.engine import health as module       # new in this batch

    return module


def status_of(trade: TradeRecord | None) -> TradeStatus:
    assert trade is not None
    return trade.status


# =========================================================================== #
# D1  the monitor never dies silently
# =========================================================================== #
async def test_d1_stray_cancelled_error_does_not_end_the_loop_and_the_owner_is_told_once(db: Database) -> None:
    tickmill, binance = Venue("tickmill", "metaapi"), Venue("binance", "ccxt")
    await add_trade(db, position("tickmill"))                 # synced first: the base loop died right here
    await add_trade(db, position("binance"))
    monitor, notifier = make_monitor(db, [binance, tickmill])
    tickmill.stray = True                                     # MetaApi account disconnected
    async with looping(monitor) as task:
        await wait_until(lambda: tickmill.entered >= mon.SYNC_ERROR_ALERT_AFTER + 3, task=task,
                         what="tickmill syncs keep being tried")
        await wait_until(lambda: binance.finished >= 5, task=task, what="binance keeps being synced")
        assert not task.done()
        # MetaApi is back: the position is synced again and the counter starts over
        tickmill.stray = False
        before = tickmill.finished
        await wait_until(lambda: tickmill.finished > before, task=task, what="tickmill synced after reconnect")
    assert tickmill.cancelled == 0 and binance.cancelled == 0          # the stray error cancelled nothing
    warnings = notifier.texts(Severity.WARNING)
    assert len(warnings) == 1, warnings
    assert "tickmill" in warnings[0] and "#1 BTC" in warnings[0] and "5 kez üst üste kontrol edilemedi" in warnings[0]
    assert "<code></code>" not in warnings[0] and "bağlantı kopuk" in warnings[0]
    assert notifier.texts(Severity.ERROR) == []


async def test_d1_stray_cancelled_error_in_a_direct_pass_is_an_ordinary_counted_sync_error(db: Database) -> None:
    tickmill, binance = Venue("tickmill", "metaapi"), Venue("binance", "ccxt")
    await add_trade(db, position("tickmill"), position("binance"))     # both legs of ONE trade
    monitor, notifier = make_monitor(db, [binance, tickmill], interval=3600.0)
    tickmill.stray = True
    for _ in range(7):
        await monitor.run_once()                              # base: CancelledError came out of run_once()
    assert tickmill.entered == 7 and binance.finished == 7
    assert len(notifier.texts(Severity.WARNING)) == 1         # at the 5th failure in a row, not again
    tickmill.stray = False
    await monitor.run_once()
    tickmill.stray = True
    for _ in range(4):
        await monitor.run_once()
    assert len(notifier.texts(Severity.WARNING)) == 1         # a success in between started the count again
    await monitor.run_once()
    assert len(notifier.texts(Severity.WARNING)) == 2


async def test_d1_stray_cancelled_error_from_the_virtual_hook_and_from_cancel_entry(db: Database) -> None:
    tickmill, binance = Venue("tickmill", "metaapi"), Venue("binance", "ccxt")
    old_ms = int((datetime.now(timezone.utc) - timedelta(hours=2)).timestamp() * 1000)
    virtual_trade = await add_trade(db, virtual("tickmill"), status=TradeStatus.PENDING)
    stale_trade = await add_trade(db, pending("tickmill", opened_at=old_ms), status=TradeStatus.PENDING)
    await add_trade(db, position("binance"))
    monitor, notifier = make_monitor(db, [binance, tickmill], interval=3600.0)
    monitor.config.risk.pending_entry_expiry_min = 1          # the stale limit entry is cancelled by the monitor
    hook_calls: list[int] = []

    async def hook(trade: TradeRecord, pos: BrokerPosition, now: datetime) -> BrokerPosition:
        hook_calls.append(trade.id)
        await poisoned()
        return pos

    async def cancel_entry(pos: BrokerPosition) -> BrokerPosition:
        tickmill.calls.append(("cancel_entry", pos))
        await poisoned()
        return pos

    monitor.virtual_hook = hook
    tickmill.cancel_entry = cancel_entry  # type: ignore[method-assign]
    for _ in range(2):
        await monitor.run_once()                              # base: CancelledError out of the first pass
        await monitor.run_virtual_once()
    assert hook_calls == [virtual_trade] * 4
    assert len(tickmill.calls_of("cancel_entry")) == 2 and binance.finished == 2
    assert [p.status for p in await db.broker_positions(stale_trade)] == [PositionStatus.PENDING]
    assert status_of(await db.get_trade(virtual_trade)) is TradeStatus.PENDING
    assert notifier.notifications == []


@pytest.mark.parametrize("failure", ["exception", "stray-cancel"])
async def test_d1_a_failing_pass_never_ends_the_loop(db: Database, failure: str) -> None:
    binance = Venue("binance", "ccxt")
    await add_trade(db, position("binance"))
    monitor, _notifier = make_monitor(db, [binance])
    real = db.active_broker_positions
    broken = {"on": True, "calls": 0}

    async def flaky() -> list[tuple[int, BrokerPosition]]:
        if broken["on"]:
            broken["calls"] += 1
            if failure == "exception":
                raise RuntimeError("database is locked")
            await poisoned()
        return await real()

    db.active_broker_positions = flaky  # type: ignore[method-assign]
    async with looping(monitor) as task:
        await wait_until(lambda: broken["calls"] >= 3, task=task, what="passes keep being tried")
        assert binance.entered == 0 and not task.done()
        broken["on"] = False
        await wait_until(lambda: binance.finished >= 2, task=task, what="syncs resume when the cause is gone")


async def test_d1_owner_is_told_when_the_passes_keep_failing_and_once_when_they_work_again(db: Database) -> None:
    """The loop survives a pass that cannot even read the database, but a monitor that is alive and syncs
    nothing must not go unnoticed either."""
    binance = Venue("binance", "ccxt")
    await add_trade(db, position("binance"))
    monitor, notifier = make_monitor(db, [binance])
    real = db.active_broker_positions
    broken = {"left": 10 ** 9, "calls": 0}

    async def flaky() -> list[tuple[int, BrokerPosition]]:
        if broken["left"] > 0:
            broken["left"] -= 1
            broken["calls"] += 1
            raise RuntimeError("database is locked")
        return await real()

    db.active_broker_positions = flaky  # type: ignore[method-assign]
    async with looping(monitor) as task:
        await wait_until(lambda: len(notifier.notifications) == 1, task=task, what="the owner is told")
        await wait_until(lambda: broken["calls"] >= mon.PASS_ERROR_ALERT_AFTER + 5, task=task, what="more failures")
        text, severity = notifier.notifications[0]
        assert severity is Severity.ERROR and "Pozisyon takibi çalışmıyor" in text
        assert "<code>database is locked</code>" in text and "5 denemede" in text       # at the 5th failure in a row
        assert len(notifier.notifications) == 1 and binance.entered == 0      # told once per outage
        broken["left"] = 0                                    # the database answers again
        await wait_until(lambda: len(notifier.notifications) == 2, task=task, what="recovery is reported")
        assert notifier.notifications[1] == ("✅ Pozisyon takibi yeniden çalışıyor.", Severity.INFO)
        await wait_until(lambda: binance.finished >= 2, task=task, what="syncs resumed")
        # a hiccup shorter than the threshold is not reported
        calls, synced = broken["calls"], binance.finished
        broken["left"] = mon.PASS_ERROR_ALERT_AFTER - 1
        await wait_until(lambda: broken["calls"] == calls + mon.PASS_ERROR_ALERT_AFTER - 1
                         and binance.finished >= synced + 2, task=task, what="hiccup over")
    assert len(notifier.notifications) == 2


async def test_d1_a_notifier_that_hangs_never_holds_the_loop_up(db: Database, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mon, "CANCEL_WAIT_SEC", 0.2)          # production: 5 s
    binance = Venue("binance", "ccxt")
    await add_trade(db, position("binance"))
    monitor, notifier = make_monitor(db, [binance])
    real = db.active_broker_positions
    broken = {"on": True, "calls": 0}
    sending: list[str] = []

    async def flaky() -> list[tuple[int, BrokerPosition]]:
        if broken["on"]:
            broken["calls"] += 1
            raise RuntimeError("disk I/O error")
        return await real()

    async def telegram_hangs(text: str, severity: Severity = Severity.INFO) -> None:
        sending.append(text)
        await asyncio.Event().wait()

    db.active_broker_positions = flaky  # type: ignore[method-assign]
    notifier.notify = telegram_hangs  # type: ignore[method-assign]
    task = asyncio.ensure_future(monitor.run())
    await wait_until(lambda: len(sending) == 1, task=task, what="the alert is being sent")
    calls = broken["calls"]
    await wait_until(lambda: broken["calls"] >= calls + 5, task=task, what="passes go on while Telegram hangs")
    broken["on"] = False
    await wait_until(lambda: binance.finished >= 3, task=task, what="positions are synced while Telegram hangs")
    assert len(sending) == 2                                  # the recovery notice hangs too; nobody waits for it
    t0 = time.monotonic()
    monitor.request_stop()
    await asyncio.wait_for(task, 3)                           # the hanging notices are given up on at the end
    assert time.monotonic() - t0 < 1.5 and task.exception() is None


async def test_d1_a_real_cancellation_ends_run_at_once_and_takes_the_running_syncs_with_it(db: Database) -> None:
    tickmill, binance = Venue("tickmill", "metaapi"), Venue("binance", "ccxt")
    await add_trade(db, position("binance"))
    await add_trade(db, position("tickmill"))
    monitor, _notifier = make_monitor(db, [binance, tickmill])
    tickmill.hang = True
    task = asyncio.ensure_future(monitor.run())
    await wait_until(lambda: tickmill.entered == 1 and binance.finished >= 1, task=task, what="syncs running")
    t0 = time.monotonic()
    task.cancel()
    await asyncio.wait({task}, timeout=2)
    assert task.done() and task.cancelled() and time.monotonic() - t0 < 1.0
    assert tickmill.cancelled == 1                            # the hanging call was cancelled, not left behind
    assert not monitor.stopping                               # a cancellation is not a stop request
    # the engine's supervisor starts run() again: everything is synced again
    tickmill.hang = False
    synced = (binance.finished, tickmill.finished)
    async with looping(monitor) as again:
        await wait_until(lambda: binance.finished > synced[0] and tickmill.finished > synced[1], task=again,
                         what="both brokers synced by the restarted loop")


async def test_d1_request_stop_ends_run_promptly_and_run_can_be_started_again(db: Database) -> None:
    binance = Venue("binance", "ccxt")
    await add_trade(db, position("binance"))
    monitor, _notifier = make_monitor(db, [binance], interval=3600.0)     # sleeping between passes
    task = asyncio.ensure_future(monitor.run())
    await wait_until(lambda: binance.finished == 1, task=task, what="first pass")
    t0 = time.monotonic()
    monitor.request_stop()
    await asyncio.wait_for(task, 2)
    assert time.monotonic() - t0 < 0.5 and task.exception() is None and monitor.stopping
    again = asyncio.ensure_future(monitor.run())              # stop requested: returns at once, syncs nothing
    await asyncio.wait_for(again, 2)
    assert binance.finished == 1


async def test_d1_timeout_alert_names_the_reason(db: Database, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mon, "SYNC_TIMEOUT_SEC", 0.02)        # production: 120 s
    tickmill = Venue("tickmill", "metaapi")
    await add_trade(db, position("tickmill"))
    monitor, notifier = make_monitor(db, [tickmill], interval=3600.0)
    tickmill.hang = True
    for _ in range(mon.SYNC_ERROR_ALERT_AFTER):
        await monitor.run_once()
    (warning,) = notifier.texts(Severity.WARNING)
    assert "<code></code>" not in warning                      # base: an empty reason
    assert "0.02 sn içinde yanıt gelmedi" in warning and "tickmill" in warning
    # an exception without a message is named by its type
    tickmill.hang = False
    await monitor.run_once()
    tickmill.fail["sync_position"] = KeyError()
    for _ in range(mon.SYNC_ERROR_ALERT_AFTER):
        await monitor.run_once()
    assert "<code>KeyError</code>" in notifier.texts(Severity.WARNING)[-1]


# =========================================================================== #
# D2  one slow position never delays the others
# =========================================================================== #
async def test_d2_a_hanging_sync_does_not_delay_the_other_trades(db: Database) -> None:
    tickmill, binance, fx = Venue("tickmill", "metaapi"), Venue("binance", "ccxt"), Venue("fx", "mt5")
    await add_trade(db, position("tickmill"))                 # first in every pass
    await add_trade(db, position("binance"))
    await add_trade(db, position("fx"))
    monitor, notifier = make_monitor(db, [binance, fx, tickmill])
    tickmill.hang = True                                      # up to SYNC_TIMEOUT_SEC (120 s) in production
    async with looping(monitor) as task:
        await wait_until(lambda: binance.finished >= 6 and fx.finished >= 6, task=task,
                         what="binance and fx are synced pass after pass while tickmill hangs")
        assert tickmill.entered == 1 and tickmill.finished == 0      # the next passes skipped the busy trade
        gaps = [b - a for a, b in zip(binance.stamps, binance.stamps[1:])]
        assert max(gaps) < 0.5, gaps                          # base: nothing for 120 s
    assert notifier.notifications == []


async def test_d2_a_hanging_leg_does_not_delay_the_other_legs_of_the_same_trade(db: Database) -> None:
    tickmill, binance, fx = Venue("tickmill", "metaapi"), Venue("binance", "ccxt"), Venue("fx", "mt5")
    trade = await add_trade(db, position("tickmill"), position("binance"), position("fx"))
    monitor, _notifier = make_monitor(db, [binance, fx, tickmill])
    tickmill.hang = True
    async with looping(monitor) as task:
        await wait_until(lambda: binance.finished >= 5 and fx.finished >= 5, task=task,
                         what="the trade's own binance and fx legs stay on their schedule")
        assert tickmill.entered == 1                          # never a second call for the leg that is in flight
        # binance closes its leg meanwhile: booked at once, not when the tickmill call ends
        binance.sync_fn = lambda p: p.model_copy(update={"status": PositionStatus.CLOSED, "remaining_quantity": 0.0,
                                                          "realized_pnl": 12.5})

        async def binance_closed() -> bool:
            return {p.broker: p.status for p in await db.broker_positions(trade)}["binance"] is PositionStatus.CLOSED

        end = time.monotonic() + 3
        while not await binance_closed():
            assert time.monotonic() < end, "binance leg not booked while tickmill hangs"
            await asyncio.sleep(0.01)
        closed_syncs = binance.entered
        await asyncio.sleep(0.2)
        assert binance.entered == closed_syncs                # a closed leg is not synced any more
        # Review 1 (engine_iso, I2): tickmill is optional: its sync runs under its own position lock, never under
        # the trade lock (which used to stay held for the whole hung call)
        assert not monitor._trade_lock(trade).locked() and monitor.position_lock(trade, "tickmill").locked()
        tickmill.hang = False
        tickmill.release.set()                                # the hung call returns
        await wait_until(lambda: tickmill.finished >= 2, task=task, what="tickmill synced again afterwards")
    assert status_of(await db.get_trade(trade)) is TradeStatus.OPEN


async def test_d2_positions_of_one_trade_are_synced_concurrently_and_rolled_up_once(db: Database) -> None:
    tickmill, binance = Venue("tickmill", "metaapi"), Venue("binance", "ccxt")
    trade = await add_trade(db, position("tickmill"), position("binance"))
    monitor, notifier = make_monitor(db, [binance, tickmill], interval=3600.0)
    monitor.config.notifier.notify_on_execution = True
    tickmill.delay = 0.3
    closed = lambda p: p.model_copy(update={"status": PositionStatus.CLOSED, "remaining_quantity": 0.0,  # noqa: E731
                                            "realized_pnl": 5.0})
    tickmill.sync_fn = binance.sync_fn = closed
    t0 = time.monotonic()
    async with monitor._trade_lock(trade):                    # the way the engine calls it before managing a trade
        optional = await monitor.sync_trade(trade)
    # Review 1 (engine_iso, I2): the optional tickmill's sync goes on outside the trade lock; the engine waits for
    # it (bounded) after releasing the lock
    await asyncio.wait(optional)
    assert binance.stamps[0] - t0 < 0.25                      # base: binance waited for tickmill (>= 0.3 s)
    assert tickmill.entered == binance.entered == 1
    assert status_of(await db.get_trade(trade)) is TradeStatus.CLOSED
    reports = [t for t in notifier.texts() if "🏁" in t]
    assert len(reports) == 1 and "tickmill +5.00" in reports[0] and "binance +5.00" in reports[0]


async def test_d2_direct_passes_return_only_when_everything_is_done(db: Database) -> None:
    brokers = [Venue(f"b{i}", "ccxt") for i in range(4)]
    trades = [await add_trade(db, position(b.name)) for b in brokers]
    hooked: list[int] = []
    for b in brokers:
        b.delay = 0.3
        b.sync_fn = lambda p: p.model_copy(update={"data": {**p.data, "seen": True}})
    v1 = await add_trade(db, virtual("b0"), status=TradeStatus.PENDING)
    v2 = await add_trade(db, virtual("b1"), status=TradeStatus.PENDING)
    monitor, _notifier = make_monitor(db, brokers, interval=3600.0)

    async def hook(trade: TradeRecord, pos: BrokerPosition, now: datetime) -> BrokerPosition:
        await asyncio.sleep(0.3)
        hooked.append(trade.id)
        return pos

    monitor.virtual_hook = hook
    t0 = time.monotonic()
    await monitor.run_virtual_once()
    fast_took = time.monotonic() - t0
    assert sorted(hooked) == [v1, v2] and all(b.entered == 0 for b in brokers)
    t0 = time.monotonic()
    await monitor.run_once()
    took = time.monotonic() - t0
    assert all(b.entered == b.finished == 1 for b in brokers) and sorted(hooked) == [v1, v1, v2, v2]
    for trade in trades:                                      # saved before run_once() returned
        assert all(p.data.get("seen") for p in await db.broker_positions(trade))
    assert monitor._running == {}                             # nothing is left running behind the caller
    assert took < 0.8 and fast_took < 0.5, (took, fast_took)  # base: one after another (1.8 s and 0.6 s)


async def test_d2_a_locked_trade_is_skipped_and_error_counters_stay_per_trade_and_broker(db: Database) -> None:
    binance = Venue("binance", "ccxt")
    first = await add_trade(db, position("binance"))
    second = await add_trade(db, position("binance"))
    monitor, notifier = make_monitor(db, [binance], interval=3600.0)
    binance.fail["sync_position"] = BrokerError("binance: durum güncellenemedi: 503")
    async with monitor._trade_lock(second):                   # the engine is managing the second trade
        for _ in range(4):
            await monitor.run_once()
    assert len(binance.calls_of("sync_position")) == 4 and notifier.notifications == []
    await monitor.run_once()                                  # 5th failure of trade 1, 1st of trade 2
    (warning,) = notifier.texts(Severity.WARNING)
    assert f"#{first} BTC" in warning and "503" in warning
    for _ in range(4):
        await monitor.run_once()
    warnings = notifier.texts(Severity.WARNING)
    assert len(warnings) == 2 and f"#{second} BTC" in warnings[1]


async def test_d2_request_stop_lets_a_running_sync_finish_and_starts_nothing_new(db: Database) -> None:
    binance, fx = Venue("binance", "ccxt"), Venue("fx", "mt5")
    trade = await add_trade(db, position("binance"))
    await add_trade(db, position("fx"))
    monitor, _notifier = make_monitor(db, [binance, fx])
    binance.delay = 0.3                                       # e.g. placing the stop of a limit entry that filled
    binance.sync_fn = lambda p: p.model_copy(update={"data": {**p.data, "sl_order_id": "sl-9"}})
    task = asyncio.ensure_future(monitor.run())
    await wait_until(lambda: binance.entered == 1, task=task, what="binance sync started")
    fx_syncs = fx.entered
    monitor.request_stop()
    await asyncio.wait_for(task, 3)
    assert task.exception() is None
    assert binance.finished == 1 and binance.cancelled == 0   # it finished; it was not cut off
    assert (await db.broker_positions(trade))[0].data.get("sl_order_id") == "sl-9"      # and its result is saved
    assert binance.entered == 1 and fx.entered == fx_syncs              # nothing new after the stop request


async def test_d2_request_stop_does_not_start_a_sync_that_was_still_waiting_for_its_broker(db: Database) -> None:
    binance = Venue("binance", "ccxt")
    await add_trade(db, position("binance"))
    second = await add_trade(db, position("binance"))
    monitor, _notifier = make_monitor(db, [binance])
    binance.delay = 0.3
    task = asyncio.ensure_future(monitor.run())
    await wait_until(lambda: binance.entered == 1, task=task, what="first sync in flight; the second waits its turn")
    monitor.request_stop()
    await asyncio.wait_for(task, 3)
    assert task.exception() is None
    assert binance.finished == 1 and binance.entered == 1 and binance.cancelled == 0      # as the sequential loop
    async with monitor._trade_lock(second):                   # the engine itself may still sync a trade it manages
        await monitor.sync_trade(second)
    assert binance.finished == 2


async def test_d2_shutdown_during_a_hanging_sync_waits_then_a_cancellation_ends_it(db: Database) -> None:
    tickmill, binance = Venue("tickmill", "metaapi"), Venue("binance", "ccxt")
    await add_trade(db, position("binance"))
    await add_trade(db, position("tickmill"))
    monitor, _notifier = make_monitor(db, [binance, tickmill])
    tickmill.hang = True
    task = asyncio.ensure_future(monitor.run())
    await wait_until(lambda: tickmill.entered == 1 and binance.finished >= 1, task=task, what="running")
    monitor.request_stop()
    await asyncio.sleep(0.3)
    assert not task.done()                                    # the engine's drain waits for it (as before)
    synced = binance.entered
    await asyncio.sleep(0.2)
    assert binance.entered == synced                          # but no pass starts any more
    task.cancel()                                             # the engine gives up after its grace period
    await asyncio.wait({task}, timeout=2)
    assert task.cancelled() and tickmill.cancelled == 1


def engine_plan(names: list[str]) -> TradePlan:
    return TradePlan(
        plan_id="e1", base_asset="ETH", side=Side.LONG, entry_type=EntryType.MARKET, entry_price=2232.0,
        stop_loss=2200.0, take_profits=[TakeProfit(price=2300.0, close_fraction=1.0)], risk_r=1.0, risk_pct=2.0,
        source_chat_id=CHAT, source_message_ids=[1], created_at=datetime.now(timezone.utc), broker_names=names,
    )


async def make_engine(db: Database, brokers: list[Any]) -> tuple[Engine, FakeNotifier]:
    notifier = FakeNotifier()
    engine = Engine(app_config(0.05), db, object(), brokers, notifier,  # type: ignore[arg-type]
                    price_oracle=FakePriceOracle({"ETH": 2232.85}))
    await engine.start(monitor=True)
    return engine, notifier


async def test_d2_engine_stop_waits_for_a_running_sync_before_it_closes_the_brokers(db: Database) -> None:
    binance = Venue("binance", "ccxt")
    events: list[str] = []
    engine, _notifier = await make_engine(db, [binance])
    await engine.execute_open(engine_plan(["binance"]))
    real_close = binance.close

    async def close() -> None:
        events.append("broker closed")
        await real_close()

    def slow_sync(pos: BrokerPosition) -> BrokerPosition:
        events.append("sync finished")
        return pos

    binance.close = close  # type: ignore[method-assign]
    binance.sync_fn = slow_sync
    binance.delay = 0.3
    await wait_until(lambda: binance.entered > binance.finished, what="a slow sync is in flight")
    events.clear()
    await engine.stop()                                       # default grace period (45 s)
    assert events == ["sync finished", "broker closed"] and binance.cancelled == 0


async def test_d2_engine_stop_gives_up_on_a_hanging_sync_after_the_grace_period(db: Database) -> None:
    tickmill, binance = Venue("tickmill", "metaapi"), Venue("binance", "ccxt")
    engine, notifier = await make_engine(db, [binance, tickmill])
    await engine.execute_open(engine_plan(["binance", "tickmill"]))
    tickmill.hang = True
    entered = tickmill.entered
    await wait_until(lambda: tickmill.entered > entered, what="the hanging sync started")
    t0 = time.monotonic()
    await engine.stop(grace_sec=0.3)
    assert 0.25 < time.monotonic() - t0 < 2.5
    assert any("sn içinde bitmedi" in t for t in notifier.texts(Severity.WARNING))
    assert tickmill.cancelled == 1 and tickmill.closed and binance.closed


async def test_d2_a_database_error_between_checks_never_cuts_a_running_order_flow(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(mon, "VIRTUAL_POLL_SEC", 0.1)
    binance, tickmill = BinanceLike(), Venue("tickmill", "metaapi")
    trade = await add_trade(db, pending("binance"), virtual("tickmill"), status=TradeStatus.PENDING)
    monitor, _notifier = make_monitor(db, [binance, tickmill], interval=10.0)
    order = {"sent": 0, "confirmed": 0}
    real = db.broker_positions

    async def hook(tr: TradeRecord, pos: BrokerPosition, now: datetime) -> BrokerPosition:
        order["sent"] += 1
        await asyncio.sleep(0.5)                              # the virtual entry is sending its market order
        order["confirmed"] += 1
        return pos

    async def broken(trade_id: int) -> list[BrokerPosition]:
        if order["sent"] and not order["confirmed"]:
            raise RuntimeError("database is locked")
        return await real(trade_id)

    monitor.virtual_hook = hook
    db.broker_positions = broken  # type: ignore[method-assign]
    await monitor.run_virtual_once()                          # must neither raise nor cancel the order flow
    assert order == {"sent": 1, "confirmed": 1}
    assert binance.entered == 1                               # the re-check was skipped, not forced
    assert not monitor._trade_lock(trade).locked()


async def test_d2_loop_chores_stuck_opening_and_trades_whose_positions_are_all_terminal(db: Database) -> None:
    binance = Venue("binance", "ccxt")
    closed = position("binance", PositionStatus.CLOSED)
    closed.realized_pnl = 4.0
    finished = await add_trade(db, closed, status=TradeStatus.OPEN)        # not rolled up yet
    stuck = await add_trade(db, status=TradeStatus.OPENING)                # opening interrupted, no position
    notifier = FakeNotifier()
    later = lambda: datetime.now(timezone.utc) + timedelta(seconds=mon.STUCK_OPENING_SEC + 60)   # noqa: E731
    monitor = Monitor(app_config(0.05), db, [binance], notifier, clock=later)
    async with looping(monitor) as task:
        async def settled() -> bool:
            return (status_of(await db.get_trade(finished)) is TradeStatus.CLOSED
                    and status_of(await db.get_trade(stuck)) is TradeStatus.FAILED)

        end = time.monotonic() + 3
        while not await settled():
            assert not task.done() and time.monotonic() < end, "chores not done by the loop"
            await asyncio.sleep(0.01)
    assert binance.entered == 0
    assert [t for t in notifier.texts(Severity.WARNING) if "yarım kaldı" in t and f"#{stuck} BTC" in t]


async def test_d2_a_trade_that_was_locked_when_due_is_synced_at_the_next_tick(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(mon, "VIRTUAL_POLL_SEC", 0.1)
    binance = Venue("binance", "ccxt")
    trade = await add_trade(db, position("binance"))
    monitor, _notifier = make_monitor(db, [binance], interval=10.0)
    lock = monitor._trade_lock(trade)
    await lock.acquire()                                      # the engine is opening / managing the trade
    async with looping(monitor) as task:
        await asyncio.sleep(0.35)
        assert binance.entered == 0                           # skipped, never queued behind the engine
        lock.release()
        t0 = time.monotonic()
        await wait_until(lambda: binance.entered == 1, timeout=2, task=task, what="synced after the lock is free")
        assert time.monotonic() - t0 < 0.5                    # base: at the next full pass (10 s later)
        await asyncio.sleep(0.4)
        assert binance.entered == 1                           # and then again one monitor interval later


async def test_d2_virtual_entries_of_one_broker_never_fire_at_the_same_time(db: Database) -> None:
    """Trades run in tasks of their own now, but two virtual entries of ONE account must not send their orders
    together (both would pass the account's hedge / margin / daily-loss checks). Other accounts do not wait."""
    tickmill, other = Venue("tickmill", "metaapi"), Venue("other", "metaapi")
    first = await add_trade(db, virtual("tickmill"), status=TradeStatus.PENDING)
    second = await add_trade(db, virtual("tickmill"), status=TradeStatus.PENDING)
    third = await add_trade(db, virtual("other"), status=TradeStatus.PENDING)
    monitor, _notifier = make_monitor(db, [tickmill, other], interval=3600.0)
    running: dict[str, int] = {"tickmill": 0, "other": 0}
    most: dict[str, int] = {"tickmill": 0, "other": 0, "all": 0}
    done: list[int] = []

    async def hook(trade: TradeRecord, pos: BrokerPosition, now: datetime) -> BrokerPosition:
        running[pos.broker] += 1
        most[pos.broker] = max(most[pos.broker], running[pos.broker])
        most["all"] = max(most["all"], sum(running.values()))
        await asyncio.sleep(0.2)                              # sending the market order
        running[pos.broker] -= 1
        done.append(trade.id)
        return pos

    monitor.virtual_hook = hook
    t0 = time.monotonic()
    await monitor.run_virtual_once()
    took = time.monotonic() - t0
    assert sorted(done) == [first, second, third]
    assert most["tickmill"] == 1                              # one after the other on the same account
    assert most["all"] == 2 and took < 1.0                    # but not behind another account's entry


async def test_d2_a_broker_that_hangs_keeps_one_trade_busy_and_not_every_trade_that_has_a_leg_on_it(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every Kripto trade has a Binance and a tickmill leg. While a tickmill call hangs, its trade's lock is
    held (the call may be placing an order). That must be ONE trade, as it was with the sequential loop: if
    the syncs of all trades hung at once, no trade could be managed (its Binance leg closed) meanwhile."""
    monkeypatch.setattr(mon, "TURN_WAIT_SEC", 0.2)            # production: 2 s
    tickmill, binance = Venue("tickmill", "metaapi"), Venue("binance", "ccxt")
    trades = [await add_trade(db, position("binance"), position("tickmill")) for _ in range(3)]
    monitor, notifier = make_monitor(db, [binance, tickmill])
    tickmill.hang = True                                      # MetaApi does not answer (up to its timeout)

    async def engine_gets_the_lock(trade_id: int) -> float:
        t0 = time.monotonic()
        async with asyncio.timeout(2.0):
            async with monitor._trade_lock(trade_id):
                return time.monotonic() - t0

    async with looping(monitor) as task:
        await wait_until(lambda: tickmill.entered == 1, task=task, what="a tickmill sync hangs")
        await asyncio.sleep(0.2 + 0.3)                        # longer than the bounded wait for the account
        assert tickmill.entered == 1 and tickmill.most == 1   # nobody queues behind a call that does not answer
        samples: list[set[int]] = []
        for _ in range(12):                                   # (the other trades are locked for moments only)
            samples.append({t for t in trades if monitor._trade_lock(t).locked()})
            await asyncio.sleep(0.013)
        stuck = set.intersection(*samples)                    # locked the whole time
        # Review 1 (engine_iso, I2): tickmill is optional, its syncs never hold a trade lock (it was one trade)
        assert stuck == set(), samples                        # before: all three
        for trade in trades:
            if trade not in stuck:
                assert await engine_gets_the_lock(trade) < 0.3
        synced = binance.finished
        await wait_until(lambda: binance.finished >= synced + 9, task=task,
                         what="the Binance legs of all three trades stay on their schedule")
        assert tickmill.entered == 1
        tickmill.hang = False
        tickmill.release.set()                                # MetaApi answers again
        await wait_until(lambda: tickmill.finished >= 6, task=task, what="every tickmill leg is synced again")
    assert tickmill.most == 1 and notifier.notifications == []


async def test_d2_engine_closes_a_trade_at_once_while_a_broker_hangs_in_the_sync_of_another_trade(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End to end with the real Engine: ETH and BTC are open on binance + tickmill. MetaApi stops answering in
    the middle of a position sync. The trader closes the other trade: its Binance close goes out at once."""
    monkeypatch.setattr(mon, "TURN_WAIT_SEC", 0.2)            # production: 2 s
    tickmill, binance = Venue("tickmill", "metaapi"), Venue("binance", "ccxt")
    for venue in (tickmill, binance):
        venue.quotes["BTC"] = Quote(bid=72999.9, ask=73000.1, last=73000.0)
    engine, _notifier = await make_engine(db, [binance, tickmill])
    try:
        await engine.execute_open(engine_plan(["binance", "tickmill"]))
        await engine.execute_open(TradePlan(
            plan_id="b1", base_asset="BTC", side=Side.LONG, entry_type=EntryType.MARKET, entry_price=73000.0,
            stop_loss=72300.0, take_profits=[TakeProfit(price=75000.0, close_fraction=1.0)], risk_r=1.0,
            risk_pct=2.0, source_chat_id=CHAT, source_message_ids=[2], created_at=datetime.now(timezone.utc),
            broker_names=["binance", "tickmill"]))
        trades = [t.id for t in await db.active_trades()]
        assert len(trades) == 2
        tickmill.hang = True                                  # MetaApi stops answering
        hung = tickmill.entered
        await wait_until(lambda: tickmill.entered == hung + 1, what="a tickmill sync hangs")
        await asyncio.sleep(0.2 + 0.3)
        samples: list[set[int]] = []
        for _ in range(12):
            samples.append({t for t in trades if engine.trade_lock(t).locked()})
            await asyncio.sleep(0.013)
        # Review 1 (engine_iso, I2): tickmill is optional, so the call that hangs holds no trade lock (it held one);
        # the trade whose tickmill position it is busy with is told by the position lock
        assert set.intersection(*samples) == set(), samples
        (stuck,) = {t for t in trades if engine.monitor.position_busy_sec(t, "tickmill") > 0}
        (free,) = set(trades) - {stuck}
        close = ManagementPlan(plan_id="m1", action=ActionType.CLOSE, trade_id=free, close_fraction=1.0,
                               source_chat_id=CHAT, summary_tr="kapat", created_at=datetime.now(timezone.utc))
        t0 = time.monotonic()
        await asyncio.wait_for(engine.execute_management(close), 3)      # before: not until MetaApi answered
        assert time.monotonic() - t0 < 1.0
        assert len(binance.calls_of("close_position")) == 1 and tickmill.entered == hung + 1
        assert status_of(await db.get_trade(free)) is TradeStatus.CLOSED
        assert status_of(await db.get_trade(stuck)) is TradeStatus.OPEN
    finally:
        tickmill.hang = False
        tickmill.release.set()
        await engine.stop()


async def test_d2_venue_syncs_of_one_account_run_one_at_a_time_and_accounts_do_not_wait_for_each_other(
    db: Database,
) -> None:
    """As with the sequential loop an adapter never sees two monitor syncs at once; different brokers overlap."""
    binance, fx = Venue("binance", "ccxt"), Venue("fx", "mt5")
    for _ in range(3):
        await add_trade(db, position("binance"), position("fx"))
    monitor, _notifier = make_monitor(db, [binance, fx], interval=3600.0)
    binance.delay = fx.delay = 0.1
    await monitor.run_once()
    assert binance.finished == fx.finished == 3               # everything is done when the pass returns
    assert binance.most == 1 and fx.most == 1                 # before: 3 calls into one adapter at the same time
    assert binance.stamps[0] < fx.stamps[-1] and fx.stamps[0] < binance.stamps[-1]      # the brokers overlap
    for venue in (binance, fx):                               # one after the other: 0.1 s apart
        assert all(b - a >= 0.09 for a, b in zip(venue.stamps, venue.stamps[1:])), venue.stamps


async def test_d2_unfilled_exchange_entries_get_their_brokers_turn_before_the_open_positions(db: Database) -> None:
    """One sync at a time per account: the fill check of a resting LIMIT entry (its stop depends on it) must
    not queue behind the routine syncs of that account's open positions."""
    binance = Venue("binance", "ccxt")
    for _ in range(4):
        await add_trade(db, position("binance"))
    await add_trade(db, pending("binance"), status=TradeStatus.PENDING)       # the newest trade
    monitor, _notifier = make_monitor(db, [binance])
    binance.delay = 0.02
    async with looping(monitor) as task:
        await wait_until(lambda: binance.finished >= 5, task=task, what="first pass")
    assert binance.seen[0] is PositionStatus.PENDING, binance.seen[:5]
    assert sorted(s.value for s in binance.seen[:5]) == ["open"] * 4 + ["pending"] and binance.most == 1


async def test_d2_a_broker_kept_busy_by_the_fast_checks_still_syncs_its_open_positions(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A very slow exchange: the fill checks of three resting entries alone take longer than a fast interval.
    They go first, but not for ever: a position that has not had its broker's turn for two monitor intervals
    is served before them."""
    monkeypatch.setattr(mon, "VIRTUAL_POLL_SEC", 0.1)         # production: 2 s
    monkeypatch.setattr(mon, "TURN_WAIT_SEC", 0.1)            # production: 2 s (equal to the fast interval)
    binance = Venue("binance", "ccxt")
    for _ in range(3):
        await add_trade(db, pending("binance"), status=TradeStatus.PENDING)
    await add_trade(db, position("binance"))
    monitor, notifier = make_monitor(db, [binance], interval=0.5)
    binance.delay = 0.12                                      # one check takes longer than a fast interval
    async with looping(monitor) as task:
        await wait_until(lambda: binance.seen.count(PositionStatus.OPEN) >= 2, timeout=8, task=task,
                         what="the open position is synced although the fast checks keep the broker busy")
        assert binance.seen.count(PositionStatus.PENDING) >= 6          # and the fill checks went on meanwhile
    assert binance.most == 1 and notifier.notifications == []


async def test_d2_a_slow_virtual_order_never_keeps_the_lock_of_another_trade(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One virtual entry at a time per account - but a trade must not sit on its own lock while it waits for
    ANOTHER trade's order on that account (MetaApi: seconds, up to minutes when it hangs): the engine could
    not manage that trade meanwhile, e.g. close its Binance leg."""
    monkeypatch.setattr(mon, "TURN_WAIT_SEC", 0.2)            # production: 2 s
    tickmill, binance = Venue("tickmill", "metaapi"), Venue("binance", "ccxt")
    first = await add_trade(db, virtual("tickmill"), status=TradeStatus.PENDING)
    second = await add_trade(db, position("binance"), virtual("tickmill"))
    monitor, _notifier = make_monitor(db, [binance, tickmill])
    order_done = asyncio.Event()
    calls: list[int] = []
    running = {"now": 0, "most": 0}

    async def hook(trade: TradeRecord, pos: BrokerPosition, now: datetime) -> BrokerPosition:
        calls.append(trade.id)
        running["now"] += 1
        running["most"] = max(running["most"], running["now"])
        try:
            if trade.id == first:
                await order_done.wait()                       # the first trade's order is being sent
        finally:
            running["now"] -= 1
        return pos

    async def engine_gets_the_lock() -> float:
        t0 = time.monotonic()
        async with asyncio.timeout(2.0):                      # base + unbounded wait: never while the order runs
            async with monitor._trade_lock(second):
                return time.monotonic() - t0

    monitor.virtual_hook = hook
    async with looping(monitor) as task:
        await wait_until(lambda: calls == [first], task=task, what="the first trade's order is in flight")
        assert await engine_gets_the_lock() < 0.2 + 0.4       # at the latest after one bounded wait
        await asyncio.sleep(0.3)                              # the order has been running for longer than the wait
        for _ in range(5):
            assert await engine_gets_the_lock() < 0.3         # from then on nobody queues behind it
            await asyncio.sleep(0.02)
        synced = binance.finished
        await wait_until(lambda: binance.finished >= synced + 5, task=task,
                         what="the second trade's Binance leg stays on its schedule")
        assert calls == [first]                               # and never a second order flow on the account
        order_done.set()
        await wait_until(lambda: second in calls, task=task, what="second entry looked at once the account is free")
    assert running["most"] == 1


class Reference:
    """Reference-exchange prices for the engine's virtual entries (what ReferencePriceOracle.mid answers)."""

    def __init__(self, prices: dict[str, float]) -> None:
        self.prices = prices

    async def mid(self, exchange_id: str, base_asset: str, *, fresh: bool = False) -> float | None:
        return self.prices.get(base_asset.upper())


def limit_plan(base: str, entry: float, stop: float, target: float) -> TradePlan:
    return TradePlan(
        plan_id=f"L{base}", base_asset=base, quote_asset="USDT", side=Side.LONG, entry_type=EntryType.LIMIT,
        entry_price=entry, stop_loss=stop, take_profits=[TakeProfit(price=target, close_fraction=1.0)], risk_r=1.0,
        risk_pct=2.0, source_chat_id=CHAT, source_message_ids=[1], created_at=datetime.now(timezone.utc),
        broker_names=["binance", "tickmill"],
    )


async def test_d2_engine_closes_one_trade_while_the_virtual_order_of_another_hangs_at_the_broker(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End to end with the real Engine. Two LIMIT signals wait: BTC and ETH, each with a resting Binance entry
    and a virtual entry on tickmill. BTC's level is touched and its tickmill market order does not come back
    (MetaApi). The trader closes ETH meanwhile: that must not wait for tickmill."""
    monkeypatch.setattr(mon, "TURN_WAIT_SEC", 0.2)            # production: 2 s
    quotes = {"BTC": Quote(bid=73299.9, ask=73300.1, last=73300.0), "ETH": Quote(bid=2259.9, ask=2260.1, last=2260.0)}
    binance, tickmill = FakeBroker("binance"), FakeBroker("tickmill")
    binance.quotes.update(quotes)
    tickmill.quotes.update(quotes)
    tickmill.config.reference_exchange = "binanceusdm"
    tickmill.config.entry_trigger = "reference_touch"         # type: ignore[assignment]
    reference = Reference({"BTC": 73300.0, "ETH": 2260.0})
    notifier = FakeNotifier()
    engine = Engine(app_config(0.05), db, object(), [binance, tickmill], notifier,  # type: ignore[arg-type]
                    price_oracle=FakePriceOracle({"BTC": 73300.0, "ETH": 2260.0}), reference_prices=reference)
    await engine.start(monitor=True)
    order_sent, order_back = asyncio.Event(), asyncio.Event()
    real_open = tickmill.open_position

    async def open_position(req: Any) -> BrokerPosition:
        order_sent.set()
        await order_back.wait()                               # the market order is on its way: no answer yet
        return await real_open(req)

    try:
        await engine.execute_open(limit_plan("BTC", 73000.0, 72300.0, 75000.0))
        await engine.execute_open(limit_plan("ETH", 2232.0, 2200.0, 2300.0))
        ids = {t.plan.base_asset: t.id for t in await db.active_trades()}
        btc, eth = ids["BTC"], ids["ETH"]
        assert {p.broker: mon.is_virtual_pending(p) for p in await db.broker_positions(eth)} == {
            "binance": False, "tickmill": True}
        tickmill.open_position = open_position  # type: ignore[method-assign]
        tickmill.quotes["BTC"] = Quote(bid=72989.9, ask=72990.1, last=72990.0)
        reference.prices["BTC"] = 72990.0                     # Binance touches the BTC level: the entry fires
        await asyncio.wait_for(order_sent.wait(), 3)
        await asyncio.sleep(0.1)
        close = ManagementPlan(plan_id="m1", action=ActionType.CLOSE, trade_id=eth, close_fraction=1.0,
                               source_chat_id=CHAT, summary_tr="eth kapat", created_at=datetime.now(timezone.utc))
        t0 = time.monotonic()
        await asyncio.wait_for(engine.execute_management(close), 3)      # before: until tickmill answered
        assert time.monotonic() - t0 < 1.0
        assert not order_back.is_set() and engine.trade_lock(btc).locked()         # BTC's order is still out
        assert {p.broker: p.status for p in await db.broker_positions(eth)} == {
            "binance": PositionStatus.CANCELLED, "tickmill": PositionStatus.CANCELLED}
        assert status_of(await db.get_trade(eth)) is TradeStatus.CANCELLED
        assert [p.broker_symbol for p in binance.calls_of("cancel_entry")] == ["ETH/USDT:USDT"]
        order_back.set()                                      # tickmill answers at last: BTC is booked as usual
        await wait_until(lambda: len(tickmill.opened) == 1, what="the BTC order is booked")

        async def btc_open_on_tickmill() -> bool:
            return {p.broker: p.status for p in await db.broker_positions(btc)}["tickmill"] is PositionStatus.OPEN

        end = time.monotonic() + 3
        while not await btc_open_on_tickmill():
            assert time.monotonic() < end, "BTC position not booked after tickmill answered"
            await asyncio.sleep(0.01)
    finally:
        order_back.set()
        await engine.stop()


async def test_d2_a_hanging_venue_sync_does_not_stop_the_virtual_poll_of_the_same_trade(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(mon, "VIRTUAL_POLL_SEC", 0.1)         # production: 2 s
    binance, tickmill = Venue("binance", "ccxt"), Venue("tickmill", "metaapi")
    trade = await add_trade(db, position("binance"), virtual("tickmill"))       # binance is synced first
    monitor, _notifier = make_monitor(db, [binance, tickmill], interval=10.0)
    hooked: list[float] = []

    async def hook(tr: TradeRecord, pos: BrokerPosition, now: datetime) -> BrokerPosition:
        hooked.append(time.monotonic())
        return pos

    monitor.virtual_hook = hook
    binance.hang = True                                       # up to SYNC_TIMEOUT_SEC (120 s) in production
    async with looping(monitor) as task:
        await wait_until(lambda: len(hooked) >= 8, timeout=3, task=task,
                         what="the trigger level keeps being watched (base: not once while the sync hangs)")
        gaps = [b - a for a, b in zip(hooked, hooked[1:])]
        assert max(gaps) < 0.45, gaps                         # every fast interval (0.1 s) plus slack
        assert binance.entered == 1 and monitor._trade_lock(trade).locked()


# =========================================================================== #
# D3  unfilled exchange LIMIT entries are checked in the fast pass
# =========================================================================== #
async def test_d3_fast_pass_checks_pending_ccxt_limit_entries_and_nothing_else(db: Database, tmp_path: Path) -> None:
    binance, bybit = Venue("binance", "ccxt"), Venue("bybit", "ccxt")
    tickmill, fx = Venue("tickmill", "metaapi"), Venue("fx", "mt5")
    breakout, paper = Venue("breakout", "manual"), FakeBroker("paper")
    dry = DryRunBroker(Venue("dry", "ccxt"), str(tmp_path))            # dry-run mode: orders are simulated
    dry_syncs: list[str] = []
    dry_sync = dry.sync_position

    async def counted(pos: BrokerPosition) -> BrokerPosition:
        dry_syncs.append(pos.broker)
        return await dry_sync(pos)

    dry.sync_position = counted  # type: ignore[method-assign]
    brokers = [binance, bybit, tickmill, fx, breakout, paper, dry]
    await add_trade(db, pending("binance"), pending("breakout"), virtual("tickmill"), status=TradeStatus.PENDING)
    await add_trade(db, pending("tickmill"), pending("fx"), status=TradeStatus.PENDING)   # MetaTrader pending orders
    await add_trade(db, position("bybit"))                                              # an OPEN ccxt position
    simulated = pending("bybit", simulated=True)                                        # left over from a dry run
    simulated.dry_run = True
    await add_trade(db, simulated, status=TradeStatus.PENDING)
    await add_trade(db, pending("paper"), status=TradeStatus.PENDING)
    await add_trade(db, pending("dry"), status=TradeStatus.PENDING)
    monitor, _notifier = make_monitor(db, brokers, interval=3600.0)
    hooked: list[str] = []

    async def hook(trade: TradeRecord, pos: BrokerPosition, now: datetime) -> BrokerPosition:
        hooked.append(pos.broker)
        return pos

    monitor.virtual_hook = hook
    await monitor.run_virtual_once()
    assert binance.entered == 1                               # base: 0 (it waited for the next full pass)
    assert hooked == ["tickmill"]
    assert [b.entered for b in (bybit, tickmill, fx, breakout)] == [0, 0, 0, 0]
    assert paper.calls_of("sync_position") == [] and dry_syncs == []
    await monitor.run_once()                                  # the full pass still covers everything
    assert [b.entered for b in (binance, bybit, tickmill, fx, breakout)] == [2, 2, 1, 1, 1]
    assert len(paper.calls_of("sync_position")) == 1 and dry_syncs == ["dry"]


async def test_d3_stop_is_placed_within_the_fast_interval_even_while_another_broker_hangs(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(mon, "VIRTUAL_POLL_SEC", 0.2)         # production: 2 s (only speeds the test up)
    binance, tickmill = BinanceLike(), Venue("tickmill", "metaapi")
    await add_trade(db, position("tickmill"))
    trade = await add_trade(db, pending("binance"), status=TradeStatus.PENDING)
    monitor, _notifier = make_monitor(db, [binance, tickmill], interval=10.0)   # production interval
    tickmill.hang = True
    async with looping(monitor) as task:
        await wait_until(lambda: binance.entered >= 1 and tickmill.entered == 1, task=task, what="first pass")
        await asyncio.sleep(0.05)
        binance.fill()                                        # just after a pass: the worst case
        await wait_until(lambda: binance.stop_placed_at is not None, timeout=3, task=task,
                         what="the stop of the filled limit entry is placed (base: at the next full pass, 10 s)")
        assert binance.stop_placed_at is not None and binance.filled_at is not None
        waited = binance.stop_placed_at - binance.filled_at
        assert waited < 0.6, waited                           # one fast interval (0.2 s) plus slack
        await asyncio.sleep(0.5)
        assert binance.stops_placed == 1
    stored = (await db.broker_positions(trade))[0]
    assert stored.status is PositionStatus.OPEN and stored.data["sl_order_id"] == "sl-1"
    assert status_of(await db.get_trade(trade)) is TradeStatus.OPEN


async def test_d3_stop_is_placed_while_the_same_trades_virtual_entry_is_still_sending_its_order(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(mon, "VIRTUAL_POLL_SEC", 0.2)
    binance, tickmill = BinanceLike(), Venue("tickmill", "metaapi")
    trade = await add_trade(db, pending("binance"), virtual("tickmill"), status=TradeStatus.PENDING)
    monitor, _notifier = make_monitor(db, [binance, tickmill], interval=10.0)
    order = {"started": 0.0, "done": 0.0, "calls": 0}
    touched = asyncio.Event()

    async def hook(tr: TradeRecord, pos: BrokerPosition, now: datetime) -> BrokerPosition:
        if not touched.is_set():
            return pos
        order["calls"] += 1
        order["started"] = time.monotonic()
        await asyncio.sleep(1.2)                              # a slow MetaApi market order (about 14 RPCs)
        order["done"] = time.monotonic()
        new = position("tickmill")
        await db.save_broker_position(tr.id, new)
        return new

    monitor.virtual_hook = hook
    async with looping(monitor) as task:
        await wait_until(lambda: binance.entered >= 1, task=task, what="first pass")
        touched.set()                                         # Binance touches the level: the virtual entry fires
        await wait_until(lambda: order["calls"] == 1, task=task, what="tickmill order being sent")
        await asyncio.sleep(0.1)
        binance.fill()                                        # ... and our Binance limit fills a moment later
        await wait_until(lambda: binance.stop_placed_at is not None, timeout=3, task=task, what="binance stop")
        assert binance.stop_placed_at is not None and binance.filled_at is not None
        assert binance.stop_placed_at - binance.filled_at < 0.6
        assert order["done"] == 0.0                           # the tickmill order was still in flight
        await wait_until(lambda: order["done"] > 0, task=task, what="tickmill order done")
        await asyncio.sleep(0.4)
    assert order["calls"] == 1 and binance.stops_placed == 1  # never a second order, never a second stop
    assert {p.broker: p.status for p in await db.broker_positions(trade)} == {
        "binance": PositionStatus.OPEN, "tickmill": PositionStatus.OPEN}


async def test_d3_after_a_fill_noticed_in_the_fast_pass_the_next_sync_waits_a_full_interval(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ccxt adapter treats two consecutive "flat" reads of the exchange position as "closed there"; the
    reads must stay a full monitor interval apart, as they were when fills were only noticed in the full pass."""
    monkeypatch.setattr(mon, "VIRTUAL_POLL_SEC", 0.1)
    binance = BinanceLike()
    await add_trade(db, pending("binance"), status=TradeStatus.PENDING)
    monitor, _notifier = make_monitor(db, [binance], interval=0.8)
    async with looping(monitor) as task:
        await wait_until(lambda: binance.entered >= 5, task=task, what="pending entry polled in the fast passes")
        binance.fill()
        await wait_until(lambda: binance.stop_placed_at is not None, timeout=2, task=task, what="fill noticed")
        noticed = binance.entered
        await wait_until(lambda: binance.entered > noticed, timeout=3, task=task, what="first sync after the fill")
        assert binance.stamps[noticed] - binance.stamps[noticed - 1] >= 0.7, binance.stamps[noticed - 3:]


async def test_d3_real_ccxt_adapter_places_the_stop_in_the_fast_pass_with_one_request_per_poll(db: Database) -> None:
    """The real CcxtBroker against the in-memory exchange of the adapter tests (binanceusdm rules)."""
    from test_ccxt_broker import make_broker, open_limit, open_orders_of

    broker, exchange = make_broker("binanceusdm")
    pos = await open_limit(broker)                            # resting LIMIT entry, nothing attached on binance
    assert pos.status is PositionStatus.PENDING and not exchange.by_param("stopLossPrice")
    trade = await add_trade(db, pos, status=TradeStatus.PENDING)
    notifier = FakeNotifier()
    exchange_time = lambda: datetime.fromtimestamp(exchange.now / 1000.0, tz=timezone.utc)   # noqa: E731
    monitor = Monitor(app_config(3600.0), db, [broker], notifier, clock=exchange_time)

    mark = len(exchange.calls)
    for _ in range(3):
        await monitor.run_virtual_once()                      # not filled yet
    polls = [name for name, _args in exchange.calls[mark:]]
    assert polls == ["fetch_order"] * 3                       # the extra load: ONE order lookup per fast pass
    assert not exchange.by_param("stopLossPrice")

    exchange.fill(pos.data["entry_order_id"], price=2220.0)   # the exchange fills the limit
    await monitor.run_virtual_once()
    (stop,) = open_orders_of(exchange, "stopLossPrice")       # base: none until the next full pass
    assert stop["amount"] == pos.quantity and stop["params"]["stopLossPrice"] == 2205.11
    stored = (await db.broker_positions(trade))[0]
    assert stored.status is PositionStatus.OPEN and stored.data["sl_order_id"] == stop["id"]
    assert status_of(await db.get_trade(trade)) is TradeStatus.OPEN

    mark = len(exchange.calls)
    await monitor.run_virtual_once()                          # an OPEN position is not in the fast pass
    assert exchange.calls[mark:] == []
    assert notifier.texts(Severity.ERROR) == []


async def test_d3_fast_polls_do_not_bring_the_sync_error_alert_forward(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(mon, "VIRTUAL_POLL_SEC", 0.05)
    binance = Venue("binance", "ccxt")
    await add_trade(db, pending("binance"), status=TradeStatus.PENDING)
    monitor, notifier = make_monitor(db, [binance], interval=10.0)
    binance.fail["sync_position"] = BrokerError("binance: durum güncellenemedi: 503")
    async with looping(monitor) as task:
        await wait_until(lambda: binance.entered >= 12, task=task, what="12 failed polls")
    assert notifier.notifications == []                       # one counted failure per 10 s interval, not twelve
    for _ in range(mon.SYNC_ERROR_ALERT_AFTER - 1):
        await monitor.run_once()                              # full passes always count
    assert len(notifier.texts(Severity.WARNING)) == 1


async def test_d3_open_positions_and_metatrader_orders_keep_the_monitor_interval(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only waiting virtual entries and unfilled ccxt entries get the fast interval: the request load of every
    other position is what it was."""
    monkeypatch.setattr(mon, "VIRTUAL_POLL_SEC", 0.05)
    binance, bybit = Venue("binance", "ccxt"), Venue("bybit", "ccxt")
    tickmill, fx = Venue("tickmill", "metaapi"), Venue("fx", "mt5")
    await add_trade(db, position("binance"), position("tickmill"))
    await add_trade(db, pending("fx"), pending("tickmill"), status=TradeStatus.PENDING)
    await add_trade(db, pending("bybit"), virtual("tickmill"), status=TradeStatus.PENDING)
    monitor, _notifier = make_monitor(db, [binance, bybit, tickmill, fx], interval=0.5)
    hooked: list[float] = []

    async def hook(trade: TradeRecord, pos: BrokerPosition, now: datetime) -> BrokerPosition:
        hooked.append(time.monotonic())
        return pos

    monitor.virtual_hook = hook
    async with looping(monitor) as task:
        await wait_until(lambda: len(hooked) >= 20, task=task, what="20 fast checks")
        took = hooked[-1] - hooked[0]
    full_passes = took / 0.5 + 1
    assert 16 <= bybit.entered <= len(hooked) + 2             # the unfilled exchange entry: every fast check
    for venue, per_pass in ((binance, 1), (fx, 1), (tickmill, 2)):
        assert venue.entered <= per_pass * (full_passes + 1), (venue.name, venue.entered, full_passes)
    slow_gaps = [b - a for a, b in zip(binance.stamps, binance.stamps[1:])]
    assert all(gap >= 0.4 for gap in slow_gaps), slow_gaps


# =========================================================================== #
# D4  broker heartbeat
# =========================================================================== #
class Account(Venue):
    """get_equity / health_problem under test control."""

    def __init__(self, name: str, kind: str = "ccxt", *, with_health: bool = False) -> None:
        super().__init__(name, kind)
        self.problem: Any = None      # None healthy | str | Exception | "hang" | "stray"
        self.equity_calls = 0
        self.health_calls = 0
        if with_health:
            self.health_problem = self._health_problem      # only some adapters have this coroutine

    async def _answer(self) -> str | None:
        if self.problem == "hang":
            await asyncio.Event().wait()
        if self.problem == "stray":
            await poisoned()
        if isinstance(self.problem, Exception):
            raise self.problem
        return self.problem

    async def get_equity(self) -> float:
        self.equity_calls += 1
        await self._answer()
        return self.equity

    async def _health_problem(self) -> str | None:
        self.health_calls += 1
        return await self._answer()


async def test_d4_owner_is_told_once_after_three_problems_and_once_when_the_broker_is_back(
    db: Database, health: Any
) -> None:
    tickmill, binance = Account("tickmill", "metaapi"), Account("binance", "ccxt")
    monitor, notifier = make_monitor(db, [binance, tickmill])
    beat = monitor.heartbeat
    tickmill.problem = BrokerError("MetaApi hesap bilgisi: 120 sn içinde yanıt gelmedi.")
    assert health.HEARTBEAT_SEC == 60.0 and health.HEARTBEAT_ALERT_AFTER == 3
    for _ in range(2):
        await beat.check_once()
    assert notifier.notifications == []
    await beat.check_once()                                   # third problem in a row
    assert notifier.notifications == [(
        "🔌 <b>tickmill</b> yanıt vermiyor: MetaApi hesap bilgisi: 120 sn içinde yanıt gelmedi. "
        "Yeni sinyaller bu hesapta açılamayabilir.", Severity.WARNING)]
    assert beat.problems() == {"tickmill": "MetaApi hesap bilgisi: 120 sn içinde yanıt gelmedi"}
    for _ in range(5):
        await beat.check_once()
    assert len(notifier.notifications) == 1                   # told once per outage
    tickmill.problem = None
    await beat.check_once()
    assert notifier.notifications[1:] == [("✅ <b>tickmill</b> tekrar yanıt veriyor.", Severity.INFO)]
    await beat.check_once()
    assert len(notifier.notifications) == 2 and beat.problems() == {}
    # one or two bad answers followed by a good one are not an outage
    for _ in range(3):
        tickmill.problem = BrokerError("geçici")
        await beat.check_once()
        await beat.check_once()
        tickmill.problem = None
        await beat.check_once()
    assert len(notifier.notifications) == 2
    assert binance.equity_calls == tickmill.equity_calls == 19


async def test_d4_health_problem_is_used_when_the_adapter_has_it(db: Database, health: Any) -> None:
    fx, binance = Account("fx", "mt5", with_health=True), Account("binance", "ccxt")
    monitor, notifier = make_monitor(db, [binance, fx])
    fx.problem = "MT5 terminalinde 'Algo Trading' kapalı; araç çubuğundaki düğmeye basın."
    for _ in range(3):
        await monitor.heartbeat.check_once()
    assert fx.health_calls == 3 and fx.equity_calls == 0      # its own check, not the balance read
    assert notifier.notifications == [(
        "🔌 <b>fx</b> hesabında sorun var: MT5 terminalinde 'Algo Trading' kapalı; araç çubuğundaki düğmeye basın. "
        "Yeni sinyaller bu hesapta açılamayabilir.", Severity.WARNING)]
    fx.problem = None
    await monitor.heartbeat.check_once()
    assert notifier.notifications[1:] == [("✅ <b>fx</b> hesabındaki sorun giderildi.", Severity.INFO)]


async def test_d4_timeouts_exceptions_and_stray_cancellations_count_and_brokers_are_checked_concurrently(
    db: Database, health: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(health, "HEARTBEAT_READ_TIMEOUT_SEC", 0.2)     # production: 20 s
    monkeypatch.setattr(health, "HEARTBEAT_CHECK_TIMEOUT_SEC", 0.3)    # production: 30 s
    hung, stray, down, slow_health, fine = (
        Account("fx", "mt5"), Account("tickmill", "metaapi"), Account("binance", "ccxt"),
        Account("fx2", "mt5", with_health=True), Account("okx", "ccxt"))
    hung.problem, stray.problem, slow_health.problem = "hang", "stray", "hang"
    down.problem = RuntimeError("")                           # an exception without a message
    monitor, notifier = make_monitor(db, [hung, stray, down, slow_health, fine])
    t0 = time.monotonic()
    for _ in range(3):
        await monitor.heartbeat.check_once()
    took = time.monotonic() - t0
    assert took < 3 * 0.3 + 0.6, took                         # each round = the slowest check, not their sum
    texts = {t.split("</b>")[0].split("<b>")[1]: t for t in notifier.texts(Severity.WARNING)}
    assert set(texts) == {"fx", "tickmill", "binance", "fx2"} and len(notifier.notifications) == 4
    assert "yanıt vermiyor: 0.2 sn içinde yanıt gelmedi." in texts["fx"]
    assert "yanıt vermiyor: 0.3 sn içinde yanıt gelmedi." in texts["fx2"]
    assert "yanıt vermiyor: bağlantı kopuk" in texts["tickmill"]
    assert "yanıt vermiyor: RuntimeError." in texts["binance"]
    assert fine.equity_calls == 3


async def test_d4_manual_paper_and_dry_run_brokers_are_left_alone(db: Database, health: Any, tmp_path: Path) -> None:
    breakout = Account("breakout", "manual")
    paper = FakeBroker("paper", fail={"get_equity": BrokerError("fiyat kaynağı yok")})
    inner = Account("binance", "ccxt")
    inner.problem = BrokerError("API anahtarı reddedildi")
    dry = DryRunBroker(inner, str(tmp_path))
    monitor, notifier = make_monitor(db, [breakout, paper, dry])
    breakout.problem = BrokerError("x")
    for _ in range(5):
        await monitor.heartbeat.check_once()
    assert notifier.notifications == []
    assert breakout.equity_calls == 0 and inner.equity_calls == 0
    assert [health.BrokerHeartbeat.watched(b) for b in (breakout, paper, dry, inner)] == [False, False, False, True]


async def test_d4_healthy_brokers_cause_no_message_and_a_failing_notifier_is_retried(db: Database, health: Any) -> None:
    binance, fx = Account("binance", "ccxt"), Account("fx", "mt5", with_health=True)
    monitor, notifier = make_monitor(db, [binance, fx])
    for _ in range(10):
        await monitor.heartbeat.check_once()
    assert notifier.notifications == [] and binance.equity_calls == 10 and fx.health_calls == 10

    sent = notifier.notify

    async def telegram_down(text: str, severity: Severity = Severity.INFO) -> None:
        raise ConnectionError("telegram unreachable")

    binance.problem = BrokerError("API anahtarı reddedildi")
    notifier.notify = telegram_down  # type: ignore[method-assign]
    for _ in range(4):
        await monitor.heartbeat.check_once()                  # must not raise
    notifier.notify = sent  # type: ignore[method-assign]
    await monitor.heartbeat.check_once()
    assert len(notifier.texts(Severity.WARNING)) == 1 and "API anahtarı reddedildi" in notifier.notifications[0][0]


async def test_d4_heartbeat_lives_inside_run_and_never_delays_the_position_sync(
    db: Database, health: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(health, "HEARTBEAT_SEC", 0.05)                 # production: 60 s
    tickmill, binance = Account("tickmill", "metaapi"), Account("binance", "ccxt")
    await add_trade(db, position("binance"))
    monitor, notifier = make_monitor(db, [binance, tickmill])
    tickmill.problem = "hang"                                 # its check blocks for the whole timeout (30 s)
    task = asyncio.ensure_future(monitor.run())
    await wait_until(lambda: tickmill.equity_calls == 1, task=task, what="heartbeat started by run()")
    before = binance.finished
    await wait_until(lambda: binance.finished >= before + 6, task=task, what="positions synced meanwhile")
    gaps = [b - a for a, b in zip(binance.stamps, binance.stamps[1:])]
    assert max(gaps) < 0.5, gaps
    monitor.request_stop()                                    # a check in flight never holds the shutdown up
    await asyncio.wait_for(task, 2)
    assert task.exception() is None and notifier.notifications == []
    calls = tickmill.equity_calls
    await asyncio.sleep(0.2)
    assert tickmill.equity_calls == calls                     # the heartbeat ended with run()


async def test_d4_alert_and_recovery_through_the_running_monitor(
    db: Database, health: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(health, "HEARTBEAT_SEC", 0.03)
    tickmill, binance = Account("tickmill", "metaapi"), Account("binance", "ccxt")
    monitor, notifier = make_monitor(db, [binance, tickmill])
    async with looping(monitor) as task:
        await wait_until(lambda: binance.equity_calls >= 3, task=task, what="healthy rounds")
        assert notifier.notifications == []
        tickmill.problem = "stray"                            # MetaApi disconnected: its calls raise CancelledError
        await wait_until(lambda: len(notifier.notifications) == 1, task=task, what="outage reported")
        await asyncio.sleep(0.2)
        assert len(notifier.notifications) == 1 and not task.done()
        tickmill.problem = None
        await wait_until(lambda: len(notifier.notifications) == 2, task=task, what="recovery reported")
    assert [s for _t, s in notifier.notifications] == [Severity.WARNING, Severity.INFO]
    assert "tickmill" in notifier.notifications[0][0] and "tekrar yanıt veriyor" in notifier.notifications[1][0]


async def test_d4_heartbeat_with_the_real_adapters_own_health_checks(db: Database, health: Any) -> None:
    """The real CcxtBroker, Mt5Broker and MetaApiBroker code on the rigs of their own tests (in-memory exchange,
    fake MetaTrader5 module, fake MetaApi connection): what each adapter's health_problem() says reaches the owner."""
    import ccxt

    from test_ccxt_broker import make_broker
    from test_mt5_broker import FakeMT5
    from test_mt5_broker import connected as mt5_connected

    binance, exchange = make_broker("binanceusdm")
    await binance.connect()
    terminal = FakeMT5()
    fx = await mt5_connected(terminal)
    brokers: list[Any] = [binance, fx]
    try:                                                      # (the MetaApi SDK is an optional dependency)
        from test_metaapi_broker import FakeConn
        from test_metaapi_broker import connected as metaapi_connected
    except ImportError:
        pass
    else:
        brokers.append(await metaapi_connected(FakeConn()))
    monitor, notifier = make_monitor(db, brokers)
    beat = monitor.heartbeat
    try:
        assert all(beat.watched(b) for b in brokers)
        for _ in range(4):
            await beat.check_once()
        assert notifier.notifications == [] and beat.problems() == {}      # healthy accounts: silence

        async def refused(params: Any = None) -> Any:
            raise ccxt.AuthenticationError(
                'binanceusdm {"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}')

        balance = exchange.fetch_balance
        exchange.fetch_balance = refused                      # the API key was deleted / its IP is not allowed
        terminal.terminal.trade_allowed = False               # "Algo Trading" was switched off in the terminal
        for _ in range(2):
            await beat.check_once()
        assert notifier.notifications == []
        await beat.check_once()                               # third problem in a row
        warnings = notifier.texts(Severity.WARNING)
        assert len(warnings) == 2 and len(notifier.notifications) == 2
        by_name = {w.split("</b>")[0].split("<b>")[1]: w for w in warnings}
        assert set(by_name) == {binance.name, fx.name}        # nothing about the healthy MetaApi account
        assert "API anahtarı" in by_name[binance.name] and "Algo Trading" in by_name[fx.name]
        assert all(w.endswith("Yeni sinyaller bu hesapta açılamayabilir.") for w in warnings)
        exchange.fetch_balance = balance
        terminal.terminal.trade_allowed = True
        await beat.check_once()
        recovered = notifier.texts(Severity.INFO)
        assert len(recovered) == 2 and all(text.startswith("✅") for text in recovered)
        assert beat.problems() == {}
    finally:
        for broker in brokers:
            await broker.close()
