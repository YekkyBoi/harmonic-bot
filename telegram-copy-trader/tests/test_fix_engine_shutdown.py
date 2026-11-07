"""Regression tests (F1): shutdown never interrupts an order flow half-way (entry filled, stop not placed)."""

from __future__ import annotations

import asyncio
from pathlib import Path

from copytrader.app import serve
from copytrader.errors import ParserError
from copytrader.models import BrokerPosition, ConfirmationStatus, OpenRequest, PositionStatus, TradeStatus

from fakes import FakeBroker, FakeNotifier, FakePriceOracle
from test_app_cli import _app
from test_engine import CHAT, SENDER, Env, add_msg, db, env, eth_open, result  # noqa: F401  (fixtures)


class SlowBroker(FakeBroker):
    """Like CcxtBroker: the market entry fills, then it polls the fill and places the stop. After close()
    every request fails (ccxt raises ExchangeClosedByUser), so a stop placed after close is impossible."""

    def __init__(self, *a, delay: float = 0.3, **kw) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*a, **kw)
        self.delay = delay
        self.events: list[str] = []
        self.entry_filled = asyncio.Event()

    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        if self.closed:
            raise RuntimeError("instance was closed by the user")
        pos = await super().open_position(req)
        self.events.append("entry filled")
        self.entry_filled.set()
        await asyncio.sleep(self.delay)          # fill poll before the stop
        if self.closed:
            self.events.append("SL FAILED: closed")
            return pos.model_copy(update={"data": {**pos.data, "unprotected": True}, "error": "KRİTİK stop yok"})
        self.events.append("SL placed")
        return pos

    async def close(self) -> None:
        self.events.append("close")
        await super().close()


async def test_engine_stop_waits_for_a_running_open_even_if_its_caller_is_cancelled(env: Env) -> None:
    slow = SlowBroker("binance")
    env.engine.brokers = [slow]
    env.parser.push(result(eth_open()))
    await add_msg(env.db, 501, "eth 1R long 2232.18 sl 2205.11 tp 2310.6")
    batch = asyncio.ensure_future(env.engine.on_messages_ready(CHAT, SENDER))   # e.g. a parse-retry task
    await asyncio.wait_for(slow.entry_filled.wait(), 2)
    batch.cancel()                                       # what Engine.stop / listener.stop used to do
    await env.engine.stop()
    assert slow.events == ["entry filled", "SL placed", "close"]
    trade = (await env.db.recent_trades(env.engine._now().replace(year=2000)))[0]
    assert trade.status is TradeStatus.OPEN
    positions = await env.db.broker_positions(trade.id)
    assert len(positions) == 1 and positions[0].status is PositionStatus.OPEN


async def test_no_new_work_after_shutdown_began(env: Env) -> None:
    env.parser.push(result(eth_open(confidence=0.7)))
    await env.signal()
    conf = env.notifier.confirmations[0]
    env.engine.begin_shutdown()
    text = await env.engine.handle_confirmation(conf.id, True)
    assert "kapanıyor" in text and env.opens() == 0
    assert (await env.db.get_confirmation(conf.id)).status is ConfirmationStatus.PENDING  # type: ignore[union-attr]
    assert "kapanıyor" in await env.engine.close_all()
    # a new batch is left unconsumed (the listener resumes it after the restart)
    await add_msg(env.db, 900, "btc long 66000 stop 65000")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert len(env.parser.calls) == 1
    assert not (await env.db.get_message(CHAT, 900)).consumed  # type: ignore[union-attr]


async def test_retry_task_inside_execute_open_is_not_cancelled_by_stop(env: Env) -> None:
    slow = SlowBroker("binance")
    env.engine.brokers = [slow]
    env.engine.retry_delays = (0.01,)
    err = ParserError("Yapay zekâ servisine bağlanılamadı.")
    err.transient = True  # type: ignore[attr-defined]
    env.parser.push(err, result(eth_open()))
    await add_msg(env.db, 501, "eth 1R long 2232.18 sl 2205.11 tp 2310.6")
    await env.engine.on_messages_ready(CHAT, SENDER)
    await asyncio.wait_for(slow.entry_filled.wait(), 2)          # the retry task is in execute_open now
    await env.engine.stop()
    assert slow.events == ["entry filled", "SL placed", "close"]
    trades = await env.db.active_trades()
    assert len(trades) == 1 and trades[0].status is TradeStatus.OPEN
    assert len(await env.db.broker_positions(trades[0].id)) == 1


async def test_stop_gives_up_after_the_grace_period_and_warns(env: Env) -> None:
    slow = SlowBroker("binance", delay=2.0)
    env.engine.brokers = [slow]
    env.parser.push(result(eth_open()))
    await add_msg(env.db, 501, "eth 1R long 2232.18 sl 2205.11 tp 2310.6")
    batch = asyncio.ensure_future(env.engine.on_messages_ready(CHAT, SENDER))
    await asyncio.wait_for(slow.entry_filled.wait(), 2)
    await env.engine.stop(grace_sec=0.1)
    assert any("sn içinde bitmedi" in t for t in env.notifier.texts())
    batch.cancel()
    await asyncio.gather(batch, return_exceptions=True)


class BotLikeNotifier(FakeNotifier):
    """Owner taps run in the notifier's own tasks (like BotNotifier.handle_callback)."""

    def __init__(self) -> None:
        super().__init__()
        self.tasks: set[asyncio.Task[object]] = set()

    def tap(self, coro) -> asyncio.Task[object]:  # type: ignore[no-untyped-def]
        task = asyncio.ensure_future(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    async def stop(self) -> None:
        if self.tasks:
            await asyncio.wait(list(self.tasks), timeout=5)


async def test_serve_shutdown_lets_an_approval_place_its_stop_before_closing_brokers(tmp_path: Path) -> None:
    notifier = BotLikeNotifier()
    app, _brokers = _app(tmp_path, notifier)
    slow = SlowBroker("paper1")
    app.engine.configured_brokers = [slow]
    app.engine.brokers = [slow]
    app.engine.prices = FakePriceOracle({"ETH": 2232.85})
    app.parser.push(result(eth_open(used_message_ids=[501], confidence=0.8, ambiguities=["SL bulanık"])))
    stop = asyncio.Event()
    server = asyncio.ensure_future(serve(app, stop))
    await asyncio.wait_for(app.listener.started.wait(), 2)
    await add_msg(app.db, 501, "eth 1R long")
    await app.engine.on_messages_ready(CHAT, SENDER)
    conf = notifier.confirmations[-1]
    tap = notifier.tap(app.engine.handle_confirmation(conf.id, True))    # owner taps ✅ Onayla
    await asyncio.wait_for(slow.entry_filled.wait(), 2)
    stop.set()                                                            # SIGTERM right after
    await asyncio.wait_for(server, 10)
    assert slow.events == ["entry filled", "SL placed", "close"]
    assert "açıldı" in tap.result()  # type: ignore[operator]


async def test_trade_record_written_before_close(env: Env) -> None:
    """Sanity: nothing is left in OPENING after a graceful stop with an order in flight."""
    slow = SlowBroker("binance", delay=0.1)
    env.engine.brokers = [slow]
    env.parser.push(result(eth_open()))
    await add_msg(env.db, 501, "eth 1R long 2232.18 sl 2205.11 tp 2310.6")
    task = asyncio.ensure_future(env.engine.on_messages_ready(CHAT, SENDER))
    await asyncio.wait_for(slow.entry_filled.wait(), 2)
    await env.engine.stop()
    await asyncio.gather(task, return_exceptions=True)
    assert all(t.status is not TradeStatus.OPENING for t in await env.db.active_trades())
