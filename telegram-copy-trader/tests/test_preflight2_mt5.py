"""Pre-flight batch 2, native MetaTrader 5 adapter (and the leg logic it shares with MetaApi).

B1  terminal permissions: Python-API trading / server connection are read at connect; order_send -> None is a
    lost reply only for IPC failures, -8 and other codes say that nothing was sent;
B2  a terminal call that never returns: time limit, circuit breaker, an order_send that timed out is a lost reply;
B3  retcode 10008 on a market order is "accepted": the position is looked up and adopted;
B4  a failed look-up right after a filled order keeps the leg (tracked by the order ticket);
B5  transient refusals: pause between requote attempts, one delayed retry for 10031 / 10012 / 10024;
B6  no request is built from a tick without a price;
B7  index / energy CFD aliases, symbol_map pair keys, a correct "symbol not found" hint;
B8  take profits that got no order of their own (lot too small) are named in the position's warnings;
B9  the stricter fake MetaTrader5 module (and the production failure it now shows: an order must reach the
    package without a keyword dict);
B10 health_problem() for the heartbeat.

Everything here runs against fakes: it proves the adapter's logic, not what a real terminal or server does.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from copytrader.brokers import mt5_broker
from copytrader.brokers.base import BrokerError
from copytrader.brokers.mt5_broker import (
    MT5,
    Mt5Broker,
    OrderReplyLostError,
    mapped_symbol,
    rank_symbol,
    split_tp_legs,
    symbol_candidates,
)
from copytrader.models import EntryType, MarketType, PositionStatus, Side, TakeProfit, TradePlan

from test_metaapi_broker import FakeConn
from test_metaapi_broker import connected as metaapi_connected
from test_mt5_broker import ASK, BID, SERVER_TIME, THREE_TPS, FakeMT5, make_cfg, make_plan, make_req, make_symbol
from test_mt5_broker import connected as mt5_connected

DONE, PLACED = 10009, 10008
DEAL, PENDING, SLTP = MT5.TRADE_ACTION_DEAL, MT5.TRADE_ACTION_PENDING, MT5.TRADE_ACTION_SLTP
GUARD = 3.0                 # no await of these tests may take longer (an adapter that hangs fails, not the run)
HANG_LIMIT = 15.0           # a "hung" fake call gives up by itself after this long
# real pauses, read before the fixture below zeroes them (None on a tree without them)
REAL_REQUOTE_DELAY = getattr(Mt5Broker, "requote_delay_sec", None)
REAL_TRANSIENT_DELAY = getattr(Mt5Broker, "transient_retry_sec", None)


@pytest.fixture(autouse=True)
def _fast(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fake ticks carry SERVER_TIME; make the broker's "now" match and skip the real waits."""
    monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: SERVER_TIME - 7200.0))
    monkeypatch.setattr(Mt5Broker, "tick_wait_sec", 0.0)
    monkeypatch.setattr(Mt5Broker, "requote_delay_sec", 0.0, raising=False)
    monkeypatch.setattr(Mt5Broker, "transient_retry_sec", 0.0, raising=False)


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """The adapter's pauses with their real lengths, recorded instead of slept."""
    slept: list[float] = []
    real_sleep = asyncio.sleep

    async def fake_sleep(delay: float, *args: Any, **kwargs: Any) -> None:
        slept.append(delay)
        await real_sleep(0)

    monkeypatch.setattr(Mt5Broker, "requote_delay_sec", REAL_REQUOTE_DELAY, raising=False)
    monkeypatch.setattr(Mt5Broker, "transient_retry_sec", REAL_TRANSIENT_DELAY, raising=False)
    monkeypatch.setattr(mt5_broker.asyncio, "sleep", fake_sleep)
    return slept


@pytest.fixture
def short_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    """0.4 s instead of 30 s per native call, 0.4 s instead of 60 s of waiting for a lost order."""
    monkeypatch.setattr(mt5_broker, "CALL_TIMEOUT_SEC", 0.4, raising=False)
    monkeypatch.setattr(mt5_broker, "LOST_REPLY_WAIT_SEC", 0.4, raising=False)


# --------------------------------------------------------------------------- #
# fakes / builders
# --------------------------------------------------------------------------- #
class HungTerminal(FakeMT5):
    """A terminal that stops answering. Calls named in ``hang`` never return; calls named in ``hang_reply``
    are carried out but their answer never returns. ``let_go()`` makes the terminal answer again."""

    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.hang: set[str] = set()
        self.hang_reply: set[str] = set()
        self.release = threading.Event()
        self.inside = threading.Event()         # a call is stuck inside the terminal right now
        self.threads: set[str] = set()

    def _block(self) -> None:
        self.inside.set()
        self.release.wait(HANG_LIMIT)

    def _log(self, name: str, *args: Any, **kwargs: Any) -> bool:
        self.threads.add(threading.current_thread().name)
        if name in self.hang:
            self._block()
        return super()._log(name, *args, **kwargs)

    def order_send(self, request: dict[str, Any]) -> Any:
        res = super().order_send(request)
        if "order_send" in self.hang_reply:
            self._block()
        return res

    def let_go(self) -> None:
        self.hang.clear()
        self.hang_reply.clear()
        self.release.set()


async def until(done: Callable[[], Any], timeout: float = 5.0) -> None:
    end = time.monotonic() + timeout
    while not done():
        assert time.monotonic() < end, "condition not reached in time"
        await asyncio.sleep(0.01)


async def settle(b: Mt5Broker, fake: HungTerminal) -> None:
    """End of a hang test: the terminal answers again, the worker thread gets free, the broker is closed."""
    fake.let_go()
    busy = getattr(b, "_oldest_call", None)
    if busy is not None:
        await until(lambda: busy() is None)
    await asyncio.wait_for(b.close(), GUARD)


def tick(bid: float, ask: float) -> SimpleNamespace:
    return SimpleNamespace(bid=bid, ask=ask, last=0.0, time=SERVER_TIME)


def empty_tick() -> SimpleNamespace:
    return SimpleNamespace(bid=0.0, ask=0.0, last=0.0, time=0)


def index_symbol(name: str, **kw: Any) -> SimpleNamespace:
    base: dict[str, Any] = dict(digits=1, point=0.1, trade_tick_size=0.1, currency_base="USD",
                                currency_profit="USD", currency_margin="USD")
    base.update(kw)
    return make_symbol(name, **base)


def fx_symbol(name: str, **kw: Any) -> SimpleNamespace:
    base: dict[str, Any] = dict(
        digits=5, point=0.00001, trade_tick_size=0.00001, trade_contract_size=100000.0,
        filling_mode=MT5.SYMBOL_FILLING_IOC, currency_base=name[:3], currency_profit=name[3:6],
        currency_margin=name[:3],
    )
    base.update(kw)
    return make_symbol(name, **base)


def terminal(*symbols: SimpleNamespace, **kw: Any) -> FakeMT5:
    fake = FakeMT5(**kw)
    fake.symbols = {s.name: s for s in symbols}
    fake.ticks = {}
    return fake


def entries(fake: FakeMT5) -> list[dict[str, Any]]:
    """Market entry requests that reached the terminal (closing deals carry a position ticket)."""
    return [r for r in fake.sent_actions(DEAL) if "position" not in r]


def closes(fake: FakeMT5) -> list[dict[str, Any]]:
    return [r for r in fake.sent_actions(DEAL) if "position" in r]


def answer(fake: FakeMT5, codes: list[int], *, only: Callable[[dict[str, Any]], bool] = lambda r: True) -> None:
    """The server refuses the next matching requests with ``codes`` (one each), then works normally."""
    left = list(codes)
    fake.hooks.append(lambda r: left.pop(0) if left and only(r) else None)


def is_entry(r: dict[str, Any]) -> bool:
    return r["action"] == DEAL and "position" not in r


def is_close(r: dict[str, Any]) -> bool:
    return r["action"] == DEAL and "position" in r


async def eth(fake: FakeMT5, **cfg: Any) -> tuple[Mt5Broker, Any]:
    b = await mt5_connected(fake, make_cfg(**cfg) if cfg else None)
    sym = await b.resolve_symbol("ETH")
    assert sym is not None
    return b, sym


LIMIT_PLAN = dict(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))


# --------------------------------------------------------------------------- #
# B1 - terminal permission checks
# --------------------------------------------------------------------------- #
async def test_b1_connect_warns_when_python_api_trading_is_disabled() -> None:
    fake = FakeMT5()
    fake.terminal.tradeapi_disabled = True
    b = await mt5_connected(fake)
    assert b._connected                                          # a warning, not a failure
    (w,) = b.startup_warnings
    assert "Python" in w and "Araçlar > Seçenekler > Uzman Danışmanlar" in w
    assert "Disable automated trading via external Python API" in w and "KALDIRIP" in w
    assert "yeniden başlatmanız gerekmez" in w
    await b.close()


async def test_b1_connect_warns_when_the_terminal_has_no_server_connection() -> None:
    fake = FakeMT5()
    fake.terminal.connected = False
    b = await mt5_connected(fake)
    (w,) = b.startup_warnings
    assert "işlem sunucusuna bağlı değil" in w and "sağ alt" in w
    await b.close()


async def test_b1_connect_reports_every_terminal_problem_and_nothing_for_a_healthy_terminal() -> None:
    fake = FakeMT5()
    fake.terminal.connected = False
    fake.terminal.trade_allowed = False
    fake.terminal.tradeapi_disabled = True
    b = await mt5_connected(fake)
    assert len(b.startup_warnings) == 3
    assert "işlem sunucusuna" in b.startup_warnings[0]
    assert "Algo Trading" in b.startup_warnings[1] and "yeşil" in b.startup_warnings[1]      # the existing text
    assert "Python" in b.startup_warnings[2]
    await b.close()

    healthy = await mt5_connected(FakeMT5())
    assert healthy.startup_warnings == []
    await healthy.close()


async def test_b1_reconnect_does_not_repeat_old_warnings() -> None:
    fake = FakeMT5()
    fake.terminal.tradeapi_disabled = True
    b = await mt5_connected(fake)
    await b.close()
    fake.terminal.tradeapi_disabled = False                      # the owner unticked the box
    await b.connect()
    assert b.startup_warnings == []
    await b.close()


async def test_b1_order_refused_for_disabled_python_api_is_a_clear_error_not_a_lost_reply() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    fake.terminal.tradeapi_disabled = True                       # order_send -> None, last_error -8
    inits = fake.init_count
    with pytest.raises(BrokerError) as ei:
        await b.open_position(make_req(sym, 0.05, make_plan()))
    text = str(ei.value)
    assert "GÖNDERİLMEDİ" in text and "Python" in text and "Uzman Danışmanlar" in text
    assert "ulaşmış olabilir" not in text and "yanıtı alınamadı" not in text     # nothing may exist: do not say so
    assert fake.sent == [] and fake.positions == {}
    assert fake.init_count == inits                              # no re-login either
    # no look-up for a "lost" order: the only position query is the idempotency check before the order
    assert sum(1 for c in fake.calls if c[0] == "positions_get") == 1
    await b.close()


async def test_b1_minus_8_names_the_switch_that_is_off() -> None:
    fake = FakeMT5()
    b, _sym = await eth(fake)
    request = {"action": DEAL, "symbol": "ETHUSD", "volume": 0.05, "type": 0, "price": ASK, "comment": "x"}
    fake.terminal.tradeapi_disabled = True
    with pytest.raises(BrokerError) as ei:
        await b._send(request)
    assert not isinstance(ei.value, OrderReplyLostError)
    assert "Python üzerinden işlem KAPALI" in str(ei.value) and "Algo Trading" not in str(ei.value)

    # -8 while terminal_info() shows nothing wrong: the owner is told to check both switches (not that both are off)
    fake.terminal.tradeapi_disabled = False

    def refused(request: dict[str, Any]) -> Any:
        fake.last = (-8, "Auto-Trading using Python API disabled in Terminal")
        return None

    fake.order_send = refused   # type: ignore[method-assign]
    with pytest.raises(BrokerError) as ei:
        await b._send(request)
    text = str(ei.value)
    assert "şu iki ayara bakın" in text and "Algo Trading" in text and "Python API" in text
    assert "KAPALI" not in text
    await b.close()


@pytest.mark.parametrize("err", [
    (-2, 'Invalid "comment" argument'),
    (-1, "Terminal: Call failed"),
    (-6, "Terminal: Authorization failed"),
])
async def test_b1_other_refusals_say_the_order_could_not_be_sent(err: tuple[int, str]) -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    sends = []

    def refused(request: dict[str, Any]) -> Any:
        sends.append(request)
        fake.last = err
        return None

    fake.order_send = refused   # type: ignore[method-assign]
    with pytest.raises(BrokerError) as ei:
        await b._send({"action": DEAL, "symbol": "ETHUSD", "volume": 0.05, "type": 0, "price": ASK})
    assert not isinstance(ei.value, OrderReplyLostError)
    assert str(ei.value) == f"MT5 emir gönderilemedi: {err[1]} (kod {err[0]})"
    with pytest.raises(BrokerError) as ei2:
        await b.open_position(make_req(sym, 0.05, make_plan()))
    assert "emir gönderilemedi" in str(ei2.value) and "ulaşmış olabilir" not in str(ei2.value)
    assert len(sends) == 2                                       # one per attempt above, never repeated
    await b.close()


@pytest.mark.parametrize("code", [-10005, -10004, -10003, -10002, -10001, -10000])
async def test_b1_ipc_failures_are_still_lost_replies(code: int) -> None:
    fake = FakeMT5()
    b, _sym = await eth(fake)
    sends = []

    def lost(request: dict[str, Any]) -> Any:
        sends.append(request)
        fake.last = (code, "IPC failure")
        return None

    fake.order_send = lost   # type: ignore[method-assign]
    with pytest.raises(OrderReplyLostError, match="ulaşmış olabilir") as ei:
        await b._send({"action": DEAL, "symbol": "ETHUSD", "volume": 0.05, "type": 0, "price": ASK})
    assert "takip ETMİYOR" in str(ei.value)                      # an entry: it would be on nobody's books
    with pytest.raises(OrderReplyLostError, match="doğrulanamadı") as ei:
        await b._send({"action": DEAL, "symbol": "ETHUSD", "volume": 0.05, "type": 1, "price": BID, "position": 7})
    assert "takip ETMİYOR" not in str(ei.value)                  # a close: the position is still tracked
    assert len(sends) == 2                                       # one per request, never re-sent
    await b.close()


# --------------------------------------------------------------------------- #
# B2 - a terminal call that never returns
# --------------------------------------------------------------------------- #
async def test_b2_a_hung_call_ends_in_a_turkish_timeout_error(short_limits: None) -> None:
    fake = HungTerminal()
    b = await mt5_connected(fake)
    try:
        fake.hang = {"account_info"}
        t0 = time.monotonic()
        with pytest.raises(BrokerError, match=r"MetaTrader terminali 0\.4 sn içinde yanıt vermedi") as ei:
            await asyncio.wait_for(b.get_equity(), GUARD)
        assert 0.35 <= time.monotonic() - t0 < 2.0
        assert ei.value.started is True                          # it is still inside the terminal
        assert "MT5 penceresine bakın" in str(ei.value)          # tells the owner what to do
    finally:
        await settle(b, fake)


def test_b2_the_real_limits() -> None:
    assert mt5_broker.CALL_TIMEOUT_SEC == 30.0
    assert mt5_broker.INIT_TIMEOUT_SEC > 60.0                    # initialize() itself waits up to 60 s
    assert mt5_broker.HEALTH_TIMEOUT_SEC * 2 < 30.0              # two calls inside the heartbeat's 30 s


async def test_b2_later_calls_fail_at_once_and_the_breaker_closes_by_itself(short_limits: None) -> None:
    fake = HungTerminal()
    b = await mt5_connected(fake)
    try:
        fake.hang = {"account_info"}
        with pytest.raises(BrokerError):
            await asyncio.wait_for(b.get_equity(), GUARD)        # runs out of time; still stuck in the terminal
        seen = len(fake.calls)
        for call in (b.account_currency, b.get_free_margin, lambda: b.resolve_symbol("ETH")):
            t0 = time.monotonic()
            with pytest.raises(BrokerError, match="yanıt vermiyor") as ei:
                await asyncio.wait_for(call(), GUARD)
            assert time.monotonic() - t0 < 0.2                   # at once: not after another time limit
            assert ei.value.started is False
        assert len(fake.calls) == seen                           # nothing was queued behind the stuck call
        fake.let_go()                                            # the terminal answers again ...
        await until(lambda: b._stuck_call() is None)             # ... and the breaker closes by itself
        assert await b.get_free_margin() == 9_000.0
        assert fake.threads == {"mt5-mt5demo_0"}                 # still one worker thread for every native call
    finally:
        await settle(b, fake)


async def test_b2_a_call_queued_behind_the_stuck_one_never_reaches_the_terminal(short_limits: None) -> None:
    fake = HungTerminal()
    b = await mt5_connected(fake)
    try:
        fake.hang = {"account_info"}
        first = asyncio.ensure_future(b.get_equity())
        await until(fake.inside.is_set)
        with pytest.raises(BrokerError, match="yanıt vermedi") as queued:
            await asyncio.wait_for(b._get_list("positions_get", symbol="ETHUSD", what="pozisyonlar"), GUARD)
        assert queued.value.started is False
        with pytest.raises(BrokerError) as running:
            await asyncio.wait_for(first, GUARD)
        assert running.value.started is True
        fake.let_go()
        await until(lambda: b._oldest_call() is None)
        await asyncio.sleep(0.05)
        assert not any(c[0] == "positions_get" for c in fake.calls)      # dropped from the queue, never run
    finally:
        await settle(b, fake)


async def test_b2_order_send_that_timed_out_is_adopted_once_the_terminal_answers_again(
    short_limits: None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(mt5_broker, "LOST_REPLY_WAIT_SEC", 5.0, raising=False)
    caplog.set_level(logging.WARNING, logger="copytrader.brokers.mt5_broker")
    fake = HungTerminal()
    b, sym = await eth(fake)
    try:
        fake.hang_reply = {"order_send"}                         # the server fills the order, no answer comes back
        task = asyncio.ensure_future(b.open_position(make_req(sym, 0.05, make_plan())))
        await until(fake.inside.is_set)
        await asyncio.sleep(0.8)                                 # the 0.4 s limit is over
        assert not task.done()                                   # waiting for the terminal, not failing blindly
        assert len(entries(fake)) == 1
        fake.let_go()
        pos = await asyncio.wait_for(task, GUARD)
        assert len(entries(fake)) == 1 and len(fake.positions) == 1      # never sent a second time
        (leg,) = pos.data["legs"]
        assert leg["state"] == "open" and leg["ticket"] in fake.positions and leg["sl"] == 2205.11
        assert pos.status is PositionStatus.OPEN and pos.error is None
        assert any("tekrar gönderilmedi" in w for w in pos.data["warnings"])
        # the late answer is written to the log: the only trace of what the terminal did with the order
        await until(lambda: any("order_send that timed out returned" in r.getMessage() for r in caplog.records))
        late = next(r.getMessage() for r in caplog.records if "order_send that timed out returned" in r.getMessage())
        assert "retcode=10009" in late
    finally:
        await settle(b, fake)


async def test_b2_order_send_that_timed_out_and_cannot_be_confirmed_says_the_order_may_exist(
    short_limits: None,
) -> None:
    fake = HungTerminal()
    b, sym = await eth(fake)
    try:
        fake.hang_reply = {"order_send"}
        t0 = time.monotonic()
        with pytest.raises(BrokerError) as ei:
            await asyncio.wait_for(b.open_position(make_req(sym, 0.05, make_plan())), GUARD)
        assert 0.7 <= time.monotonic() - t0 < 2.5                # 0.4 s for the call + 0.4 s for the terminal
        text = str(ei.value)
        assert "0.4 sn içinde yanıt vermedi" in text
        assert "Emir sunucuya ulaşmış olabilir" in text and "ikinci kez GÖNDERİLMEDİ" in text
        assert "MT5'te ETHUSD emir/pozisyonlarına elle bakın" in text      # what the owner has to do now ...
        assert "takip ETMİYOR" in text                                      # ... and why it matters
        assert text.startswith("Emir durumu belirsiz:") and "açılamadı" not in text     # not "could not be opened"
        assert len(entries(fake)) == 1                           # sent once, never again
    finally:
        await settle(b, fake)


async def test_b2_no_order_is_sent_while_the_terminal_is_stuck(short_limits: None) -> None:
    fake = HungTerminal()
    b, sym = await eth(fake)
    try:
        fake.hang = {"account_info"}
        with pytest.raises(BrokerError):
            await asyncio.wait_for(b.get_equity(), GUARD)
        request = {"action": DEAL, "symbol": "ETHUSD", "volume": 0.05, "type": 0, "price": ASK, "comment": "x"}
        with pytest.raises(BrokerError, match="emir gönderilmedi") as ei:
            await asyncio.wait_for(b._send(request), GUARD)
        assert not isinstance(ei.value, OrderReplyLostError)     # it never left: nothing to look for
        with pytest.raises(BrokerError, match="yanıt vermiyor"):
            await asyncio.wait_for(b.open_position(make_req(sym, 0.05, make_plan())), GUARD)
        assert fake.sent == []
    finally:
        await settle(b, fake)


async def test_b2_a_call_abandoned_by_its_caller_opens_the_breaker_once_it_is_over_the_limit(
    short_limits: None,
) -> None:
    fake = HungTerminal()
    b = await mt5_connected(fake)
    try:
        fake.hang = {"account_info"}
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(b.get_equity(), 0.05)         # e.g. the price oracle's own, shorter timeout
        assert b._stuck_call() is None                           # only 0.05 s old: a slow terminal may still answer
        await asyncio.sleep(0.5)                                 # ... but it never does
        t0 = time.monotonic()
        with pytest.raises(BrokerError, match="yanıt vermiyor"):
            await asyncio.wait_for(b.get_free_margin(), GUARD)
        assert time.monotonic() - t0 < 0.2
    finally:
        await settle(b, fake)


async def test_b2_close_does_not_wait_for_a_stuck_call(short_limits: None) -> None:
    fake = HungTerminal()
    b = await mt5_connected(fake)
    try:
        fake.hang = {"account_info"}
        with pytest.raises(BrokerError):
            await asyncio.wait_for(b.get_equity(), GUARD)
        t0 = time.monotonic()
        await asyncio.wait_for(b.close(), GUARD)
        assert time.monotonic() - t0 < 1.0 and b._executor is None
    finally:
        fake.let_go()


async def test_b2_initialize_keeps_its_own_longer_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mt5_broker, "CALL_TIMEOUT_SEC", 0.1, raising=False)
    fake = FakeMT5()
    real = fake.initialize

    def slow(*args: Any, **kwargs: Any) -> bool:
        time.sleep(0.5)                                          # the terminal takes a while to start and log in
        return real(*args, **kwargs)

    fake.initialize = slow   # type: ignore[method-assign]
    b = await mt5_connected(fake)
    assert b._connected
    await b.close()


async def test_b2_initialize_that_never_returns_fails_connect_and_no_second_thread_is_started(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mt5_broker, "CALL_TIMEOUT_SEC", 0.1, raising=False)
    monkeypatch.setattr(mt5_broker, "INIT_TIMEOUT_SEC", 0.3, raising=False)
    fake = HungTerminal()
    fake.hang = {"initialize"}
    b = Mt5Broker(make_cfg(), mt5_module=fake)
    try:
        with pytest.raises(BrokerError, match=r"0\.3 sn içinde yanıt vermedi"):
            await asyncio.wait_for(b.connect(), GUARD)
        assert not b._connected
        # the package is not thread-safe: while the first initialize() is still running, no new worker thread
        with pytest.raises(BrokerError, match="önceki isteğe hâlâ yanıt vermedi"):
            await asyncio.wait_for(b.connect(), GUARD)
        assert fake.init_count == 0
        fake.let_go()
        await until(lambda: b._oldest_call() is None)
        await asyncio.wait_for(b.connect(), GUARD)
        assert b._connected
    finally:
        await settle(b, fake)


def dropped_link(fake: FakeMT5, login_sec: float) -> None:
    """The link to the terminal drops: the next account_info answers an IPC error, and logging in again
    (initialize) takes ``login_sec`` seconds."""
    real = fake.initialize

    def slow(*args: Any, **kwargs: Any) -> bool:
        time.sleep(login_sec)
        return real(*args, **kwargs)

    fake.initialize = slow   # type: ignore[method-assign]
    fake.fail_once["account_info"] = (-10004, "IPC timeout")


@pytest.fixture
def login_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    """0.3 s per native call, 2 s for logging in to the terminal."""
    monkeypatch.setattr(mt5_broker, "CALL_TIMEOUT_SEC", 0.3, raising=False)
    monkeypatch.setattr(mt5_broker, "INIT_TIMEOUT_SEC", 2.0, raising=False)


async def test_b2_logging_in_again_after_a_dropped_link_does_not_count_against_the_call(login_limits: None) -> None:
    fake = FakeMT5()
    b = await mt5_connected(fake)
    dropped_link(fake, 0.8)                                      # longer than a call may take, a normal log-in
    t0 = time.monotonic()
    assert await asyncio.wait_for(b.get_equity(), GUARD) == 10_000.0     # re-login, then the call is repeated
    assert 0.75 <= time.monotonic() - t0 < 2.0 and fake.init_count == 2
    assert b._stuck_call() is None
    await b.close()


async def test_b2_calls_queued_behind_a_re_login_wait_for_it_too(login_limits: None) -> None:
    fake = FakeMT5()
    b = await mt5_connected(fake)
    dropped_link(fake, 0.8)
    first = asyncio.ensure_future(b.get_equity())
    await until(lambda: fake.calls[-1][0] == "account_info")      # the worker thread is logging in again
    t0 = time.monotonic()
    queued = await asyncio.wait_for(
        asyncio.gather(b.get_free_margin(), b.account_currency(), return_exceptions=True), GUARD)
    assert queued == [9_000.0, "USD"] and time.monotonic() - t0 >= 0.5      # waited, not failed after 0.3 s
    assert await asyncio.wait_for(first, GUARD) == 10_000.0
    assert b._stuck_call() is None and len(b._calls) == 0
    await b.close()


async def test_b2_a_call_that_waited_for_the_re_login_does_not_trip_the_breaker_while_it_runs(
    login_limits: None,
) -> None:
    """The breaker measures how long the terminal has been busy with a call, not how long the call waited."""
    fake = FakeMT5()
    b = await mt5_connected(fake)
    real_positions = fake.positions_get
    running, finish = threading.Event(), threading.Event()

    def held(ticket: int | None = None, symbol: str | None = None) -> Any:
        running.set()
        finish.wait(HANG_LIMIT)                                  # an ordinary call that the test lets finish
        return real_positions(ticket=ticket, symbol=symbol)

    fake.positions_get = held   # type: ignore[method-assign]
    dropped_link(fake, 0.8)
    first = asyncio.ensure_future(b.get_equity())
    await until(lambda: fake.calls[-1][0] == "account_info")
    second = asyncio.ensure_future(b._get_list("positions_get", symbol="ETHUSD", what="pozisyonlar"))
    try:
        await until(running.is_set)                              # 0.8 s after it was queued, it has just started
        assert b._stuck_call() is None                           # used to count the 0.8 s in the queue as "stuck"
        third = asyncio.ensure_future(b.get_free_margin())       # a new call is queued behind it, not refused
        await asyncio.sleep(0.05)
        assert not third.done()
    finally:
        finish.set()
    assert await asyncio.wait_for(third, GUARD) == 9_000.0
    assert await asyncio.wait_for(first, GUARD) == 10_000.0 and await asyncio.wait_for(second, GUARD) == ()
    await b.close()


async def test_b2_a_re_login_that_never_returns_ends_in_time_and_opens_the_breaker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mt5_broker, "CALL_TIMEOUT_SEC", 0.3, raising=False)
    monkeypatch.setattr(mt5_broker, "INIT_TIMEOUT_SEC", 0.5, raising=False)
    fake = HungTerminal()
    b = await mt5_connected(fake)
    try:
        fake.hang = {"initialize"}
        fake.fail_once["account_info"] = (-10004, "IPC timeout")
        t0 = time.monotonic()
        with pytest.raises(BrokerError, match="yanıt vermedi") as ei:
            await asyncio.wait_for(b.get_equity(), GUARD)
        assert 0.75 <= time.monotonic() - t0 < 2.0               # the call's 0.3 s + the log-in's 0.5 s, not more
        assert ei.value.started is True
        t0 = time.monotonic()
        with pytest.raises(BrokerError, match="yanıt vermiyor"):
            await asyncio.wait_for(b.get_free_margin(), GUARD)   # breaker open: at once
        assert time.monotonic() - t0 < 0.2
        fake.let_go()
        await until(lambda: b._stuck_call() is None)
        assert await b.get_free_margin() == 9_000.0
    finally:
        await settle(b, fake)


async def test_b2_health_check_does_not_wait_for_a_re_login(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mt5_broker, "CALL_TIMEOUT_SEC", 5.0, raising=False)
    monkeypatch.setattr(mt5_broker, "INIT_TIMEOUT_SEC", 5.0, raising=False)
    monkeypatch.setattr(mt5_broker, "HEALTH_TIMEOUT_SEC", 0.3, raising=False)
    fake = FakeMT5()
    b = await mt5_connected(fake)
    dropped_link(fake, 0.8)
    first = asyncio.ensure_future(b.get_equity())
    await until(lambda: fake.calls[-1][0] == "account_info")
    t0 = time.monotonic()
    text = await asyncio.wait_for(b.health_problem(), GUARD)     # its own, short limit: no extra patience
    assert text is not None and "0.3 sn içinde yanıt vermedi" in text and time.monotonic() - t0 < 0.7
    assert b._stuck_call() is None                               # ... and that alone does not open the breaker
    assert await asyncio.wait_for(first, GUARD) == 10_000.0
    await b.close()


async def test_b2_order_whose_reply_was_lost_with_the_link_is_adopted_after_a_slow_re_login(
    login_limits: None,
) -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    real_send, real_init = fake.order_send, fake.initialize

    def lost(request: dict[str, Any]) -> Any:
        real_send(request)                                       # the server fills it ...
        fake.last = (-10002, "IPC recv failed")                  # ... but the answer never reaches the package
        return None

    def slow_login(*args: Any, **kwargs: Any) -> bool:
        time.sleep(0.8)
        return real_init(*args, **kwargs)

    fake.order_send = lost   # type: ignore[method-assign]
    fake.initialize = slow_login   # type: ignore[method-assign]
    pos = await asyncio.wait_for(b.open_position(make_req(sym, 0.05, make_plan())), GUARD)
    assert len(entries(fake)) == 1 and len(fake.positions) == 1  # never sent a second time
    (leg,) = pos.data["legs"]
    assert leg["state"] == "open" and leg["ticket"] in fake.positions and pos.error is None
    assert any("tekrar gönderilmedi" in w for w in pos.data["warnings"])
    await b.close()


async def test_b2_twenty_calls_behind_a_hung_one_all_end_in_time_and_none_runs_later(short_limits: None) -> None:
    fake = HungTerminal()
    b = await mt5_connected(fake)
    try:
        fake.hang = {"account_info"}
        first = asyncio.ensure_future(b.get_equity())
        await until(fake.inside.is_set)
        queued = [asyncio.ensure_future(b._get_list("positions_get", symbol="ETHUSD", what="pozisyonlar"))
                  for _ in range(20)]
        t0 = time.monotonic()
        results = await asyncio.wait_for(asyncio.gather(first, *queued, return_exceptions=True), GUARD)
        assert time.monotonic() - t0 < 1.5                            # one time limit for all of them, not 21
        assert all(isinstance(r, BrokerError) for r in results)
        assert results[0].started is True and not any(r.started for r in results[1:])
        fake.let_go()
        await until(lambda: b._oldest_call() is None)
        await asyncio.sleep(0.05)
        assert not any(c[0] == "positions_get" for c in fake.calls)   # none of the 20 was run afterwards
        assert len(b._calls) == 0
        assert await b.get_equity() == 10_000.0
    finally:
        await settle(b, fake)


async def test_b2_slow_but_answering_terminal_does_not_latch_the_breaker(short_limits: None) -> None:
    """Congestion, not a hang: calls that waited too long give up, the terminal keeps answering, and the breaker
    is closed again as soon as the worker thread is free (no restart, no manual reset)."""
    fake = FakeMT5()
    real = fake.account_info

    def slow() -> Any:
        time.sleep(0.15)
        return real()

    fake.account_info = slow   # type: ignore[method-assign]
    b = await mt5_connected(fake)
    before = sum(1 for c in fake.calls if c[0] == "account_info")
    results = await asyncio.wait_for(
        asyncio.gather(*(b.get_equity() for _ in range(8)), return_exceptions=True), GUARD)
    ok = [r for r in results if r == 10_000.0]
    failed = [r for r in results if isinstance(r, BrokerError)]
    assert ok and failed and len(ok) + len(failed) == 8               # 8 x 0.15 s do not fit into 0.4 s
    await until(lambda: b._oldest_call() is None)
    ran = sum(1 for c in fake.calls if c[0] == "account_info") - before
    assert ran == len(ok) + sum(1 for r in failed if r.started)       # a call that gave up in the queue never ran
    assert b._stuck_call() is None and await b.get_equity() == 10_000.0
    await b.close()


async def test_b2_close_that_timed_out_is_reported_as_unknown_and_never_repeated(short_limits: None) -> None:
    fake = HungTerminal()
    b, sym = await eth(fake)
    try:
        pos = await b.open_position(make_req(sym, 0.05, make_plan()))
        fake.hang_reply = {"order_send"}                              # the close goes through, no answer returns
        with pytest.raises(BrokerError) as ei:
            await asyncio.wait_for(b.close_position(pos, 1.0), GUARD)
        text = str(ei.value)
        assert "yanıtı alınamadı" in text and "Emir sunucuya ulaşmış olabilir" in text
        assert "takip ETMİYOR" not in text                            # the position stays on the bot's books
        assert len(closes(fake)) == 1
        fake.let_go()
        await until(lambda: b._stuck_call() is None)
        synced = await b.sync_position(pos)                           # the next sync sees what really happened
        assert synced.status is PositionStatus.CLOSED and len(closes(fake)) == 1
    finally:
        await settle(b, fake)


async def test_b2_many_concurrent_calls_all_answer_and_leave_no_bookkeeping_behind() -> None:
    fake = FakeMT5()
    b = await mt5_connected(fake)
    results = await asyncio.gather(*(b.get_equity() for _ in range(200)))
    assert results == [10_000.0] * 200
    assert b._oldest_call() is None and len(b._calls) == 0
    await b.close()


# --------------------------------------------------------------------------- #
# B3 - retcode 10008 on a market order
# --------------------------------------------------------------------------- #
def placed_answers(fake: FakeMT5, *, hidden_lookups: int = 0, dropped: bool = False, history: int | None = None) -> None:
    """The server answers market entries with 10008 (placed). The position is hidden from the first
    ``hidden_lookups`` position queries, or never created (``dropped``; ``history`` = the order's final state)."""
    real_send, real_positions = fake.order_send, fake.positions_get
    state = {"hidden": 0}

    def send(request: dict[str, Any]) -> Any:
        res = real_send(request)
        if not is_entry(request) or res is None or res.retcode != DONE:
            return res
        state["hidden"] = hidden_lookups
        if dropped:
            p = fake.positions.pop(res.order)
            fake.deals[:] = [d for d in fake.deals if d.position_id != p.ticket]
            if history is not None:
                fake.history_orders[res.order] = SimpleNamespace(ticket=res.order, state=history, position_id=0)
        return fake._result(PLACED, order=res.order)             # no deal, volume or price in a "placed" answer

    def positions_get(ticket: int | None = None, symbol: str | None = None) -> Any:
        if state["hidden"] > 0:
            state["hidden"] -= 1
            fake._log("positions_get", ticket=ticket, symbol=symbol)
            return ()
        return real_positions(ticket=ticket, symbol=symbol)

    fake.order_send = send   # type: ignore[method-assign]
    fake.positions_get = positions_get   # type: ignore[method-assign]


@pytest.mark.parametrize("hidden_lookups", [0, 4])
async def test_b3_placed_market_order_is_adopted_when_its_position_shows_up(hidden_lookups: int) -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    placed_answers(fake, hidden_lookups=hidden_lookups)
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))     # used to raise "... reddedildi (kod 10008)"
    assert len(entries(fake)) == 1                                    # not sent again
    (p,) = fake.positions.values()
    (leg,) = pos.data["legs"]
    assert leg["state"] == "open" and leg["ticket"] == p.ticket
    assert (leg["filled"], leg["entry"], leg["sl"]) == (0.05, ASK, 2205.11)      # as the venue shows them
    assert pos.status is PositionStatus.OPEN and pos.error is None and pos.filled_quantity == 0.05
    await b.close()


async def test_b3_placed_order_without_stop_on_the_venue_gets_its_stop_attached() -> None:
    fake = FakeMT5()
    fake.reject_stops_on_deal = True                                  # stops refused on the entry -> sent without
    b, sym = await eth(fake)
    placed_answers(fake)
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    (p,) = fake.positions.values()
    assert (p.sl, p.tp) == (2205.11, 2310.6)                          # attached right after (rule 1)
    assert pos.status is PositionStatus.OPEN and pos.error is None
    await b.close()


async def test_b3_placed_order_that_never_shows_up_is_reported_as_a_failure_that_may_still_fill() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    placed_answers(fake, dropped=True)
    with pytest.raises(BrokerError) as ei:
        await b.open_position(make_req(sym, 0.05, make_plan()))
    text = str(ei.value)
    assert "kabul edildi (kod 10008)" in text and "pozisyon oluşmadı" in text
    assert "sonradan gerçekleşebilir" in text and "ct12ab34cd" in text      # may still appear: where to look
    assert "reddedildi" not in text
    assert len(entries(fake)) == 1
    await b.close()


async def test_b3_placed_order_that_the_server_then_rejected_says_so() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    placed_answers(fake, dropped=True, history=MT5.ORDER_STATE_REJECTED)
    with pytest.raises(BrokerError, match=r"kabul edildi ama gerçekleşmedi \(reddedildi\)"):
        await b.open_position(make_req(sym, 0.05, make_plan()))
    assert len(entries(fake)) == 1
    await b.close()


def placed_closes(fake: FakeMT5, *, hidden_lookups: int = 0, executed: bool = True) -> None:
    """The server answers closing deals with 10008. The close shows on the venue after ``hidden_lookups``
    position queries (the fake carries it out only then), or never (``executed`` False)."""
    real_send, real_positions = fake.order_send, fake.positions_get
    state: dict[str, Any] = {"hidden": 0, "todo": None}

    def send(request: dict[str, Any]) -> Any:
        if not is_close(request):
            return real_send(request)
        fake._log("order_send", request)
        fake.sent.append(dict(request))
        state.update(hidden=hidden_lookups, todo=dict(request) if executed else None)
        return fake._result(PLACED, order=fake._id())

    def positions_get(ticket: int | None = None, symbol: str | None = None) -> Any:
        if state["hidden"] > 0:
            state["hidden"] -= 1
        elif state["todo"] is not None:
            request, state["todo"] = state["todo"], None
            fake._close(fake.positions[request["position"]], request["volume"], request["price"], 3)
        return real_positions(ticket=ticket, symbol=symbol)

    fake.order_send = send   # type: ignore[method-assign]
    fake.positions_get = positions_get   # type: ignore[method-assign]


@pytest.mark.parametrize("hidden_lookups", [0, 3])
async def test_b3_placed_close_counts_once_the_venue_shows_it(hidden_lookups: int) -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    pos = await b.open_position(make_req(sym, 0.10, make_plan()))
    placed_closes(fake, hidden_lookups=hidden_lookups)
    half = await b.close_position(pos, 0.5)                           # used to raise "kapatılamadı ... (kod 10008)"
    assert len(closes(fake)) == 1 and half.remaining_quantity == 0.05 and half.error is None
    (p,) = fake.positions.values()
    assert p.volume == pytest.approx(0.05)
    done = await b.close_position(half, 1.0)
    assert len(closes(fake)) == 2 and fake.positions == {} and done.status is PositionStatus.CLOSED
    await b.close()


async def test_b3_placed_close_that_never_shows_is_an_error_that_says_it_was_accepted() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    pos = await b.open_position(make_req(sym, 0.10, make_plan()))
    placed_closes(fake, executed=False)
    with pytest.raises(BrokerError) as ei:
        await b.close_position(pos, 1.0)
    assert "kabul edildi (kod 10008)" in str(ei.value) and "hâlâ açık görünüyor" in str(ei.value)
    assert len(closes(fake)) == 1                                     # not sent again
    await b.close()


# --------------------------------------------------------------------------- #
# B4 - a failed look-up right after a filled order
# --------------------------------------------------------------------------- #
def failing_lookups(fake: FakeMT5, *, by_ticket: int = 0, by_symbol: int = 0) -> None:
    """After the first filled entry, the next position queries fail with (-1, 'Terminal: Call failed')."""
    real_send, real_positions = fake.order_send, fake.positions_get
    left = {"ticket": 0, "symbol": 0}

    def send(request: dict[str, Any]) -> Any:
        res = real_send(request)
        if is_entry(request) and res is not None and res.retcode == DONE:
            left.update(ticket=by_ticket, symbol=by_symbol)
        return res

    def positions_get(ticket: int | None = None, symbol: str | None = None) -> Any:
        kind = "ticket" if ticket is not None else "symbol"
        if left[kind] > 0:
            left[kind] -= 1
            fake._log("positions_get", ticket=ticket, symbol=symbol)
            fake.last = (-1, "Terminal: Call failed")
            return None
        return real_positions(ticket=ticket, symbol=symbol)

    fake.order_send = send   # type: ignore[method-assign]
    fake.positions_get = positions_get   # type: ignore[method-assign]


async def test_b4_failed_lookup_after_a_fill_keeps_the_leg_and_the_refresh_confirms_it() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    failing_lookups(fake, by_ticket=1)
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))     # used to raise "... pozisyon alınamadı"
    (p,) = fake.positions.values()
    (leg,) = pos.data["legs"]
    assert leg["state"] == "open" and leg["ticket"] == p.ticket and leg["sl"] == 2205.11
    assert "unconfirmed" not in leg                                   # seen on the venue by the post-open refresh
    assert pos.status is PositionStatus.OPEN and pos.error is None
    (note,) = [w for w in pos.data["warnings"] if "sorgulanamadı" in w]
    assert "Terminal: Call failed" in note and f"#{p.ticket}" in note
    await b.close()


async def test_b4_leg_that_cannot_be_confirmed_yet_is_reported_and_confirmed_by_a_later_sync() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    failing_lookups(fake, by_ticket=1, by_symbol=1)                   # the post-open refresh fails as well
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    (p,) = fake.positions.values()
    (leg,) = pos.data["legs"]
    assert pos.status is PositionStatus.OPEN and leg["ticket"] == p.ticket and leg["unconfirmed"] is True
    assert pos.error and "henüz doğrulanamadı" in pos.error and "ct12ab34cd" in pos.error      # the owner is told
    assert any("sorgulanamadı" in w for w in pos.data["warnings"])

    synced = await b.sync_position(pos)                               # the terminal answers again
    (leg,) = synced.data["legs"]
    assert "unconfirmed" not in leg and leg["sl"] == 2205.11 and synced.status is PositionStatus.OPEN
    await b.close()


async def test_b4_unconfirmed_leg_is_found_by_its_comment_when_the_position_has_another_ticket() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    failing_lookups(fake, by_ticket=1, by_symbol=1)
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    (old,) = list(fake.positions)
    p = fake.positions.pop(old)
    p.ticket = p.identifier = 987654                                  # the position is listed under another number
    fake.positions[987654] = p
    synced = await b.sync_position(pos)
    (leg,) = synced.data["legs"]
    assert leg["ticket"] == 987654 and leg["state"] == "open" and "unconfirmed" not in leg
    await b.close()


async def test_b4_unconfirmed_leg_that_never_appears_is_closed_after_three_syncs() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    failing_lookups(fake, by_ticket=1, by_symbol=1)
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    fake.positions.clear()
    fake.deals.clear()
    for _ in range(3):
        pos = await b.sync_position(pos)
    assert pos.data["legs"][0]["state"] == "closed" and pos.data["legs"][0]["reason"] == "unknown"
    await b.close()


@pytest.mark.parametrize("naked", [False, True])
async def test_b4_fill_without_any_ticket_and_without_comment_is_still_an_error_that_tells_what_to_do(
    naked: bool,
) -> None:
    fake = FakeMT5()
    fake.reject_stops_on_deal = naked
    b, sym = await eth(fake)
    real_send = fake.order_send

    def send(request: dict[str, Any]) -> Any:
        res = real_send(request)
        if is_entry(request) and res is not None and res.retcode == DONE:
            res.order = 0                                             # no order ticket in the answer ...
            for p in fake.positions.values():
                p.comment = ""                                        # ... and the server does not keep comments
        return res

    fake.order_send = send   # type: ignore[method-assign]
    with pytest.raises(BrokerError) as ei:
        await b.open_position(make_req(sym, 0.05, make_plan()))
    text = str(ei.value)
    assert "gerçekleşti ama pozisyon numarası alınamadı" in text and "ct12ab34cd" in text
    assert ("KRİTİK" in text and "STOPSUZ" in text) is naked          # no stop on it: said loudly
    await b.close()


# --------------------------------------------------------------------------- #
# B5 - transient refusals
# --------------------------------------------------------------------------- #
async def test_b5_requotes_are_retried_with_a_pause(sleeps: list[float]) -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    answer(fake, [10004, 10020], only=is_entry)
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    assert len(entries(fake)) == 3 and pos.status is PositionStatus.OPEN
    assert sleeps == [REAL_REQUOTE_DELAY, REAL_REQUOTE_DELAY] and 0.3 <= REAL_REQUOTE_DELAY <= 0.5
    await b.close()


async def test_b5_three_requotes_still_give_up_after_three_attempts() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    answer(fake, [10004, 10021, 10020], only=is_entry)
    with pytest.raises(BrokerError, match="fiyat değişti"):
        await b.open_position(make_req(sym, 0.05, make_plan()))
    assert len(entries(fake)) == 3 and fake.positions == {}
    await b.close()


@pytest.mark.parametrize("code", [10031, 10024])
async def test_b5_one_delayed_retry_after_a_transient_refusal(code: int, sleeps: list[float]) -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    answer(fake, [code], only=is_entry)
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))     # used to fail after the first answer
    assert len(entries(fake)) == 2 and len(fake.positions) == 1 and pos.status is PositionStatus.OPEN
    assert sleeps == [REAL_TRANSIENT_DELAY] and 1.0 <= REAL_TRANSIENT_DELAY <= 2.0
    await b.close()


async def test_b5_a_second_transient_refusal_is_final() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    answer(fake, [10031, 10031, 10031], only=is_entry)
    with pytest.raises(BrokerError, match="işlem sunucusuyla bağlantı yok"):
        await b.open_position(make_req(sym, 0.05, make_plan()))
    assert len(entries(fake)) == 2                                    # one retry, not more
    await b.close()


async def test_b5_timeout_answer_on_an_entry_is_retried_only_when_no_order_exists() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    answer(fake, [10012], only=is_entry)                              # refused, nothing on the venue
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    assert len(entries(fake)) == 2 and len(fake.positions) == 1 and pos.status is PositionStatus.OPEN
    await b.close()


async def test_b5_timeout_answer_on_an_entry_that_was_executed_is_adopted_not_sent_again() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    real_send = fake.order_send
    state = {"n": 0}

    def send(request: dict[str, Any]) -> Any:
        res = real_send(request)                                      # the server DID execute it ...
        state["n"] += 1
        if is_entry(request) and state["n"] == 1:
            return fake._result(10012, comment="timeout")             # ... but answers "cancelled by timeout"
        return res

    fake.order_send = send   # type: ignore[method-assign]
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    assert len(entries(fake)) == 1 and len(fake.positions) == 1       # no duplicate position
    (leg,) = pos.data["legs"]
    assert leg["state"] == "open" and leg["ticket"] in fake.positions and pos.status is PositionStatus.OPEN
    assert any("tekrar gönderilmedi" in w for w in pos.data["warnings"])
    await b.close()


async def test_b5_close_is_retried_after_no_connection_but_not_after_a_timeout_answer() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    answer(fake, [10012], only=is_close)                              # the close may have gone through
    with pytest.raises(BrokerError, match="zaman aşımı"):
        await b.close_position(pos, 1.0)
    assert len(closes(fake)) == 1                                     # not repeated: could close another slice

    answer(fake, [10031], only=is_close)
    closed = await b.close_position(pos, 1.0)
    assert len(closes(fake)) == 3 and fake.positions == {} and closed.status is PositionStatus.CLOSED
    await b.close()


async def test_b5_stop_move_and_cancel_are_retried_once() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    answer(fake, [10031], only=lambda r: r["action"] == SLTP)
    moved = await b.update_stop_loss(pos, 2220.0)
    assert len(fake.sent_actions(SLTP)) == 2 and moved.stop_loss == 2220.0 and moved.error is None

    pending = await b.open_position(make_req(sym, 0.05, make_plan(**LIMIT_PLAN), tag="ctpending01"))
    answer(fake, [10024], only=lambda r: r["action"] == MT5.TRADE_ACTION_REMOVE)
    cancelled = await b.cancel_entry(pending)
    assert len(fake.sent_actions(MT5.TRADE_ACTION_REMOVE)) == 2 and cancelled.status is PositionStatus.CANCELLED
    await b.close()


async def test_b5_pending_order_gets_one_delayed_retry(sleeps: list[float]) -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    answer(fake, [10031], only=lambda r: r["action"] == PENDING)
    pos = await b.open_position(make_req(sym, 0.05, make_plan(**LIMIT_PLAN)))
    assert len(fake.sent_actions(PENDING)) == 2 and len(fake.orders) == 1 and pos.status is PositionStatus.PENDING
    assert sleeps == [REAL_TRANSIENT_DELAY]
    await b.close()


async def test_b5_a_lost_reply_is_never_retried() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    sends = []

    def lost(request: dict[str, Any]) -> Any:
        sends.append(request)
        fake.last = (-10002, "IPC recv failed")
        return None

    fake.order_send = lost   # type: ignore[method-assign]
    with pytest.raises(BrokerError, match="ulaşmış olabilir"):
        await b.open_position(make_req(sym, 0.05, make_plan()))
    assert len(sends) == 1
    await b.close()


# --------------------------------------------------------------------------- #
# B6 - no request from a tick without a price
# --------------------------------------------------------------------------- #
def empty_ticks_after(fake: FakeMT5, good_polls: int) -> None:
    """The first ``good_polls`` tick requests carry a price, every later one is an all-zero tick."""
    real = fake.symbol_info_tick
    seen = {"n": 0}

    def polled(name: str) -> Any:
        seen["n"] += 1
        if seen["n"] <= good_polls:
            return real(name)
        fake._log("symbol_info_tick", name)
        return empty_tick()

    fake.symbol_info_tick = polled   # type: ignore[method-assign]


async def test_b6_market_order_is_not_built_from_an_empty_tick() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    empty_ticks_after(fake, 1)                                        # the quote is fine, the order's own tick is not
    with pytest.raises(BrokerError, match=r"ETHUSD için fiyat yok"):
        await b.open_position(make_req(sym, 0.05, make_plan()))       # used to send a DEAL with price 0.0
    assert fake.sent == [] and fake.positions == {}
    await b.close()


async def test_b6_close_is_not_built_from_an_empty_tick() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    fake.sent.clear()
    empty_ticks_after(fake, 0)
    with pytest.raises(BrokerError, match="fiyat yok"):
        await b.close_position(pos, 1.0)
    assert fake.sent == [] and len(fake.positions) == 1               # still open, its stop still on the venue
    await b.close()


async def test_b6_pending_order_is_not_built_from_an_empty_tick() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    empty_ticks_after(fake, 1)
    with pytest.raises(BrokerError, match=r"ETHUSD için fiyat yok"):
        await b.open_position(make_req(sym, 0.05, make_plan(**LIMIT_PLAN)))   # used to place it without expiration
    assert fake.sent == [] and fake.orders == {}
    await b.close()


# --------------------------------------------------------------------------- #
# B7 - index / energy aliases, symbol_map keys, the "not found" hint
# --------------------------------------------------------------------------- #
CFD = MarketType.CFD


@pytest.mark.parametrize(("base", "quote", "market_type", "expected"), [
    ("NAS100", "USD", CFD, ["US100", "USTEC", "NDX100"]),
    ("US100", None, CFD, ["NAS100", "USTEC", "NDX100"]),             # the signal's own name is never repeated
    ("US30", "USD", CFD, ["DJ30", "WS30"]),
    ("US500", "USD", CFD, ["SPX500", "SP500"]),                      # "SPX500" reaches the broker as US500
    ("SPX500", "USD", CFD, ["US500", "SP500"]),
    ("GER40", "EUR", CFD, ["DE40", "DAX40", "GER30"]),               # quoted in its own currency
    ("GER40", None, CFD, ["DE40", "DAX40", "GER30"]),
    ("UK100", "GBP", CFD, ["FTSE100"]),
    ("JP225", "JPY", CFD, ["JPN225", "NIK225"]),
    ("USOIL", "USD", CFD, ["WTI", "XTIUSD", "USOUSD"]),
    ("UKOIL", "USD", CFD, ["BRENT", "XBRUSD", "UKOUSD"]),
    ("nas100", "usdt", CFD, ["US100", "USTEC", "NDX100"]),
    # nothing for other assets, other market types, or a quote that is not the instrument's own
    ("XAU", "USD", CFD, []),
    ("BTC", "USD", CFD, []),
    ("EUR", "USD", MarketType.FOREX, []),
    ("NAS100", "USD", MarketType.FOREX, []),
    ("NAS100", "USD", None, []),
    ("GER40", "JPY", CFD, []),
    ("US30", "EUR", CFD, []),
])
def test_b7_alias_candidates(base: str, quote: str | None, market_type: MarketType | None, expected: list[str]) -> None:
    assert mt5_broker.alias_candidates(base, quote, market_type) == expected


def test_b7_the_alias_table_is_the_one_from_the_task_and_the_own_name_candidates_are_unchanged() -> None:
    assert mt5_broker.CFD_ALIASES == (
        ("NAS100", "US100", "USTEC", "NDX100"),
        ("US30", "DJ30", "WS30"),
        ("SPX500", "US500", "SP500"),
        ("GER40", "DE40", "DAX40", "GER30"),
        ("UK100", "FTSE100"),
        ("JP225", "JPN225", "NIK225"),
        ("USOIL", "WTI", "XTIUSD", "USOUSD"),
        ("UKOIL", "BRENT", "XBRUSD", "UKOUSD"),
    )
    names = [n for row in mt5_broker.CFD_ALIASES for n in row]
    assert len(names) == len(set(names))                             # a name belongs to one instrument only
    assert symbol_candidates("NAS100", "USD", CFD) == ["NAS100USD", "NAS100"]
    assert symbol_candidates("GER40", "EUR", CFD) == ["GER40EUR", "GER40"]


@pytest.mark.parametrize(("base", "quote", "listed", "expected"), [
    ("NAS100", "USD", ["USTEC"], "USTEC"),
    ("NAS100", "USD", ["US100.cash"], "US100.cash"),                 # separated broker suffix
    ("NAS100", "USD", ["#NDX100"], "#NDX100"),
    ("US30", "USD", ["DJ30"], "DJ30"),
    ("US30", "USD", ["WS30.r"], "WS30.r"),
    ("US500", "USD", ["SPX500"], "SPX500"),
    ("US500", "USD", ["SP500-ECN"], "SP500-ECN"),
    ("GER40", "EUR", ["DE40"], "DE40"),
    ("GER40", "EUR", ["GER30"], "GER30"),
    ("UK100", "GBP", ["FTSE100"], "FTSE100"),
    ("JP225", "JPY", ["JPN225"], "JPN225"),
    ("USOIL", "USD", ["WTI"], "WTI"),
    ("USOIL", "USD", ["XTIUSD"], "XTIUSD"),
    ("USOIL", "USD", ["XTIUSDm"], "XTIUSDm"),                        # BASE+QUOTE style name: any suffix, like pairs
    ("UKOIL", "USD", ["XBRUSD.r"], "XBRUSD.r"),
    ("UKOIL", "USD", ["BRENT"], "BRENT"),
    # order: the signal's own name (plain or suffixed) first, then the aliases in table order
    ("NAS100", "USD", ["US100", "NAS100"], "NAS100"),
    ("NAS100", "USD", ["US100", "NAS100.cash"], "NAS100.cash"),
    ("NAS100", "USD", ["NDX100", "USTEC", "US100"], "US100"),
    ("NAS100", "USD", ["NDX100", "USTEC.cash"], "USTEC.cash"),
    ("US30", "USD", ["WS30", "DJ30"], "DJ30"),
])
async def test_b7_native_resolves_an_index_or_oil_cfd_under_its_other_names(
    base: str, quote: str, listed: list[str], expected: str
) -> None:
    fake = terminal(fx_symbol("EURUSD"), *(index_symbol(n) for n in listed))
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol(base, quote, CFD, base)             # used to raise "sembol bulunamadı"
    assert sym is not None and sym.broker_symbol == expected and sym.base_asset == base
    await b.close()


async def test_b7_aliases_match_exact_names_only() -> None:
    """No partial matches: a longer name could be another instrument (a bond, a future, another index)."""
    fake = terminal(index_symbol("US30YR"), index_symbol("DJ30FT"), index_symbol("WS300"), index_symbol("US1000"),
                    index_symbol("USTECH"), index_symbol("WTIX"), index_symbol("SOLANA"))
    b = await mt5_connected(fake)
    with pytest.raises(BrokerError) as ei:
        await b.resolve_symbol("US30", "USD", CFD)
    text = str(ei.value)
    assert "'US30' için MT5'te sembol bulunamadı (denenen: US30USD, US30)" in text
    assert "Aynı enstrümanın diğer adları da denendi: DJ30, WS30." in text       # everything that was tried
    assert "symbol_map: {US30: MT5_SEMBOL_ADI}" in text and "Market Watch" in text
    for base in ("NAS100", "USOIL"):
        with pytest.raises(BrokerError, match="sembol bulunamadı"):
            await b.resolve_symbol(base, "USD", CFD)
    with pytest.raises(BrokerError, match=r"denenen: SOLUSD, SOL\)\. Broker"):   # no aliases: none are mentioned
        await b.resolve_symbol("SOL", "USD", CFD)
    await b.close()


async def test_b7_an_alias_that_cannot_be_traded_is_skipped() -> None:
    fake = terminal(index_symbol("US100", trade_mode=MT5.SYMBOL_TRADE_MODE_DISABLED), index_symbol("USTEC"))
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("NAS100", "USD", CFD)
    assert sym is not None and sym.broker_symbol == "USTEC"
    await b.close()


async def test_b7_an_index_under_another_name_opens_like_any_other_symbol() -> None:
    fake = terminal(index_symbol("USTEC"))
    fake.ticks = {"USTEC": tick(21000.0, 21001.0)}
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("NAS100", "USD", CFD, "NAS100")
    plan = TradePlan(
        plan_id="p1", base_asset="NAS100", quote_asset="USD", market_type=CFD, side=Side.LONG,
        entry_type=EntryType.MARKET, entry_price=21001.0, stop_loss=20900.0,
        take_profits=[TakeProfit(price=21300.0, close_fraction=1.0)], risk_r=1.0, risk_pct=1.0,
        source_chat_id=-100123, source_message_ids=[42], created_at=datetime(2026, 11, 12, tzinfo=UTC),
    )
    pos = await b.open_position(make_req(sym, 0.10, plan))
    (deal,) = entries(fake)
    assert (deal["symbol"], deal["volume"], deal["price"], deal["sl"]) == ("USTEC", 0.10, 21001.0, 20900.0)
    assert pos.status is PositionStatus.OPEN and pos.broker_symbol == "USTEC"
    await b.close()


def test_b7_symbol_map_keys_the_signal_spelling_then_the_pair_then_the_base() -> None:
    # what the keys are compared with (case-insensitive), in this order
    assert mapped_symbol({"eurgbp": "A", "EUR": "B"}, "EUR", "EURGBP") == "A"           # the signal's own spelling
    assert mapped_symbol({"EURGBP": "A", "EUR": "B"}, "EUR", None, "GBP") == "A"        # the pair BASE+QUOTE (new)
    assert mapped_symbol({"EURGBP": "A"}, "EUR", "eur/gbp", "GBP") == "A"               # however the signal wrote it
    assert mapped_symbol({"EURGBP": "A"}, "EUR", "EURUSD", "USD") is None               # another pair is left alone
    assert mapped_symbol({"XAU": "GOLD"}, "XAU", "XAUUSD", "USD") == "GOLD"             # the base alone
    # a base key covers EVERY pair of that base: the old hint "symbol_map: {EUR: ...}" would have done this
    assert mapped_symbol({"EUR": "EURUSD.x"}, "EUR", "EURGBP", "GBP") == "EURUSD.x"
    # a caller that passes no quote gets the old behaviour: the signal's spelling, then the base
    assert mapped_symbol({"EURGBP": "A"}, "EUR", None) is None
    assert mapped_symbol({"ETH": "ETHUSD"}, "ETH", "ETHUSDT.P") == "ETHUSD"


async def test_b7_not_found_hint_names_a_key_that_works_for_a_currency_pair() -> None:
    fake = terminal(fx_symbol("EURUSD"), fx_symbol("EGBP.pro"))
    b = await mt5_connected(fake)
    with pytest.raises(BrokerError) as ei:
        await b.resolve_symbol("EUR", "GBP", MarketType.FOREX)
    text = str(ei.value)
    assert "symbol_map: {EURGBP: MT5_SEMBOL_ADI}" in text            # used to be "{EUR: GOLD}"
    assert "{EUR:" not in text and "MT5_SEMBOL_ADI yerine" in text and "Piyasa Gözlemi" in text
    # one text for a currency pair (the wording the previous batch's tests pin): the pair is named, and why
    # the bare currency must not be the key
    assert text.startswith("mt5demo: EURGBP paritesi MT5'te bulunamadı (denenen: EURGBP). ")
    assert "EUR diye eklemeyin: tüm EUR paritelerini değiştirir" in text and "GOLD" not in text
    await b.close()

    # following the hint works for that pair, with or without the signal's own spelling, and only for it
    b = await mt5_connected(fake, make_cfg(symbol_map={"EURGBP": "EGBP.pro"}))
    for raw in (None, "EURGBP", "eur/gbp"):
        sym = await b.resolve_symbol("EUR", "GBP", MarketType.FOREX, raw)
        assert sym is not None and sym.broker_symbol == "EGBP.pro"
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX, "EURUSD")
    assert sym is not None and sym.broker_symbol == "EURUSD"
    await b.close()


@pytest.mark.parametrize(("base", "quote", "market_type", "hint"), [
    ("XAU", "USD", CFD, "symbol_map: {XAU: GOLD}"),                  # unchanged
    ("NAS100", "USD", CFD, "symbol_map: {NAS100: MT5_SEMBOL_ADI}"),
    ("GER40", "EUR", CFD, "symbol_map: {GER40: MT5_SEMBOL_ADI}"),    # priced in EUR: the base is the instrument
    ("EUR", "USD", MarketType.FOREX, "symbol_map: {EURUSD: MT5_SEMBOL_ADI}"),
    ("EUR", None, MarketType.FOREX, "symbol_map: {EURUSD: MT5_SEMBOL_ADI}"),
    ("USD", "JPY", MarketType.FOREX, "symbol_map: {USDJPY: MT5_SEMBOL_ADI}"),
    ("XAU", "EUR", CFD, "symbol_map: {XAUEUR: MT5_SEMBOL_ADI}"),
])
async def test_b7_not_found_hint_key(base: str, quote: str | None, market_type: MarketType, hint: str) -> None:
    b = await mt5_connected(terminal(fx_symbol("CADCHF")))
    with pytest.raises(BrokerError) as ei:
        await b.resolve_symbol(base, quote, market_type)
    assert hint in str(ei.value)
    await b.close()

    # the key of the hint is one that symbol_map really looks up
    key = hint.split("{")[1].split(":")[0]
    b = await mt5_connected(terminal(fx_symbol("CADCHF"), fx_symbol("ANYNAME")), make_cfg(symbol_map={key: "ANYNAME"}))
    sym = await b.resolve_symbol(base, quote, market_type)
    assert sym is not None and sym.broker_symbol == "ANYNAME"
    await b.close()


def test_b7_bare_alias_names_use_the_whole_name_rule_of_the_previous_batch() -> None:
    whole = mt5_broker.alias_whole
    assert whole("DJ30") == ("DJ30",) and whole("WTI") == ("WTI",) and whole("XTIUSD") == ()
    assert rank_symbol("DJ30", ["DJ30"], whole("DJ30")) == (0, 0, 0)
    assert rank_symbol("DJ30.cash", ["DJ30"], whole("DJ30")) == (0, 0, 5)
    assert rank_symbol("DJ30m", ["DJ30"], whole("DJ30")) is None             # glued letters: symbol_map
    assert rank_symbol("DJ300", ["DJ30"], whole("DJ30")) is None
    assert rank_symbol("XTIUSDm", ["XTIUSD"], whole("XTIUSD")) == (1, 0, 1)


# --------------------------------------------------------------------------- #
# B8 - take profits that got no order of their own
# --------------------------------------------------------------------------- #
def tps(*pairs: tuple[float, float]) -> list[TakeProfit]:
    return [TakeProfit(price=p, close_fraction=f) for p, f in pairs]


def test_b8_notes_name_the_take_profits_without_an_order_and_where_their_part_closes() -> None:
    three = tps((74989.0, 1 / 3), (75989.0, 1 / 3), (77989.0, 1 / 3))
    legs = split_tp_legs(0.02, three, 0.01, 0.01)
    assert [(l.volume, l.tp, l.tp_indexes) for l in legs] == [(0.02, 77989.0, [0, 1, 2])]      # the rule is kept
    (note,) = mt5_broker.merged_tp_notes(legs, three, 0.01)
    assert "TP1 (74989) ve TP2 (75989) hedeflerine ayrı emir konulamadı" in note
    assert "payları minimum lotun (0.01) altında" in note and "TP3 (77989) hedefinde kapanacak" in note

    four = tps((1.0, 0.25), (2.0, 0.25), (3.0, 0.25), (4.0, 0.25))
    legs = split_tp_legs(0.03, four, 0.01, 0.01)                     # 0.03 lot / 4 TPs -> everything on TP4
    (note,) = mt5_broker.merged_tp_notes(legs, four, 0.01)
    assert "TP1 (1), TP2 (2) ve TP3 (3) hedeflerine" in note and "TP4 (4) hedefinde" in note

    # the LAST share is the small one: it joins the previous leg and closes earlier than the signal says
    two = tps((1.0, 0.9), (2.0, 0.1))
    legs = split_tp_legs(0.5, two, 0.01, 0.1)
    assert [(l.volume, l.tp, l.tp_indexes) for l in legs] == [(0.5, 1.0, [0, 1])]
    (note,) = mt5_broker.merged_tp_notes(legs, two, 0.1)
    assert "TP2 (2) hedefine ayrı emir konulamadı (payı minimum lotun (0.1) altında" in note
    assert "TP1 (1) hedefinde kapanacak" in note


def test_b8_no_note_when_every_take_profit_has_its_own_order() -> None:
    three = tps((1.0, 1 / 3), (2.0, 1 / 3), (3.0, 1 / 3))
    assert mt5_broker.merged_tp_notes(split_tp_legs(0.10, three, 0.01, 0.01), three, 0.01) == []
    assert mt5_broker.merged_tp_notes(split_tp_legs(0.25, [], 0.01, 0.01), [], 0.01) == []
    one = tps((1.0, 1.0))
    assert mt5_broker.merged_tp_notes(split_tp_legs(0.01, one, 0.01, 0.01), one, 0.01) == []


async def test_b8_native_small_lot_with_three_take_profits_warns_the_owner() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    pos = await b.open_position(make_req(sym, 0.02, make_plan(tps=THREE_TPS)))
    (deal,) = entries(fake)
    assert (deal["volume"], deal["tp"]) == (0.02, 2310.6)             # as before: the whole lot on TP3
    (note,) = [w for w in pos.data["warnings"] if "ayrı emir konulamadı" in w]      # used to be silent
    assert "TP1 (2260)" in note and "TP2 (2290)" in note and "TP3 (2310.6)" in note
    assert pos.status is PositionStatus.OPEN and pos.error is None    # a warning for the open report, not an error

    pending = await b.open_position(make_req(sym, 0.01, make_plan(
        entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2260.0, 0.5), (2300.0, 0.5))), tag="ctpending02"))
    (order,) = fake.sent_actions(PENDING)
    assert (order["volume"], order["tp"]) == (0.01, 2300.0)
    assert any("TP1 (2260) hedefine" in w and "TP2 (2300) hedefinde" in w for w in pending.data["warnings"])
    await b.close()


async def test_b8_no_warning_for_a_lot_that_splits_or_a_netting_account() -> None:
    fake = FakeMT5()
    b, sym = await eth(fake)
    pos = await b.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS)))
    assert len(entries(fake)) == 3 and pos.data["warnings"] == []
    await b.close()

    netting = FakeMT5(margin_mode=0)                                  # one position: earlier TPs are emulated
    b, sym = await eth(netting)
    pos = await b.open_position(make_req(sym, 0.02, make_plan(tps=THREE_TPS)))
    assert len(pos.data["emulated_tps"]) == 2 and pos.data["warnings"] == []
    await b.close()


async def test_b8_metaapi_shares_the_warning() -> None:
    conn = FakeConn()
    b = await metaapi_connected(conn)
    sym = await b.resolve_symbol("ETH")
    pos = await b.open_position(make_req(sym, 0.02, make_plan(tps=THREE_TPS)))
    ((args, kwargs),) = conn.called("create_market_buy_order")
    assert args == ("ETHUSD", 0.02) and kwargs["take_profit"] == 2310.6
    (note,) = [w for w in pos.data["warnings"] if "ayrı emir konulamadı" in w]
    assert "TP1 (2260)" in note and "TP2 (2290)" in note and "TP3 (2310.6)" in note
    await b.close()


# --------------------------------------------------------------------------- #
# B9 - the stricter fake MetaTrader5 module
# --------------------------------------------------------------------------- #
async def test_b9_strict_fake_answers_a_zero_tick_right_after_symbol_select() -> None:
    fake = FakeMT5(strict=True)
    fake.symbols["ETHUSD"].visible = False
    assert fake.symbol_select("ETHUSD", True)
    first, second, third = (fake.symbol_info_tick("ETHUSD") for _ in range(3))
    assert (first.bid, first.ask, first.time) == (0.0, 0.0, 0) and (second.bid, second.ask) == (0.0, 0.0)
    assert (third.bid, third.ask) == (BID, ASK)
    assert fake.symbol_select("ETHUSD", True)                         # already visible: no new cold phase
    assert fake.symbol_info_tick("ETHUSD").bid == BID

    lenient = FakeMT5()                                               # the default is unchanged
    lenient.symbols["ETHUSD"].visible = False
    lenient.symbol_select("ETHUSD", True)
    assert lenient.symbol_info_tick("ETHUSD").bid == BID


async def test_b9_adapter_copes_with_the_zero_ticks_of_the_strict_fake() -> None:
    fake = FakeMT5(strict=True)
    fake.symbols["ETHUSD"].visible = False
    fake.cold_ticks = 4
    b, sym = await eth(fake)
    quote = await b.get_quote(sym)
    assert (quote.bid, quote.ask) == (BID, ASK)
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    assert pos.status is PositionStatus.OPEN and entries(fake)[0]["price"] == ASK
    await b.close()


def test_b9_terminal_info_has_the_fields_of_the_real_package() -> None:
    info = FakeMT5().terminal_info()
    assert (info.connected, info.trade_allowed, info.tradeapi_disabled) == (True, True, False)


@pytest.mark.parametrize(("change", "message"), [
    ({"volume": 1}, 'Invalid "volume" argument'),                     # an int where the package wants a float
    ({"price": 2233}, 'Invalid "price" argument'),
    ({"sl": 0}, 'Invalid "sl" argument'),
    ({"tp": "2310.6"}, 'Invalid "tp" argument'),
    ({"type": 0.0}, 'Invalid "type" argument'),                       # a float where it wants an int
    ({"magic": "770077"}, 'Invalid "magic" argument'),
    ({"deviation": 20.0}, 'Invalid "deviation" argument'),
    ({"comment": "x" * 30}, 'Invalid "comment" argument'),            # more than 29 characters
    ({"comment": None}, 'Invalid "comment" argument'),
    ({"symbol": "S" * 30}, 'Invalid "symbol" argument'),
])
def test_b9_strict_fake_refuses_what_the_real_package_refuses(change: dict[str, Any], message: str) -> None:
    good = {"action": DEAL, "symbol": "ETHUSD", "volume": 0.05, "type": 0, "price": ASK, "sl": 2205.11,
            "tp": 2310.6, "deviation": 20, "magic": 770077, "comment": "x" * 29, "type_time": 0, "type_filling": 0}
    strict = FakeMT5(strict=True)
    assert strict.order_send(dict(good)).retcode == DONE
    assert strict.order_send({**good, **change}) is None and strict.last_error() == (-2, message)
    assert len(strict.sent) == 1                                      # the refused request never reached the terminal
    assert strict.order_send([good]) is None and strict.last_error() == (-2, "Invalid arguments")   # type: ignore[arg-type]

    lenient = FakeMT5()                                               # the default still accepts anything
    assert lenient.order_send({**good, **change}) is not None


GOOD_REQUEST = {"action": DEAL, "symbol": "ETHUSD", "volume": 0.05, "type": 0, "price": ASK, "sl": 2205.11,
                "tp": 2310.6, "deviation": 20, "magic": 770077, "comment": "ct12ab34cd", "type_time": 0,
                "type_filling": 0}


def test_b9_strict_order_send_refuses_a_request_that_comes_with_any_keyword_dict() -> None:
    """What broke every order of the first live signal: order_send(request, **{}) is refused by the package."""
    strict = FakeMT5(strict=True)
    assert type(strict.order_send).__name__ == "builtin_function_or_method"     # a C function sees the difference
    empty: dict[str, Any] = {}
    assert strict.order_send(dict(GOOD_REQUEST), **empty) is None
    assert strict.last_error() == (-2, "Unnamed arguments not allowed")
    assert strict.sent == [] and strict.positions == {}              # nothing reached the terminal
    assert strict.order_send(dict(GOOD_REQUEST)).retcode == DONE     # the documented form
    assert len(strict.sent) == 1 and strict.last_error()[0] == MT5.RES_S_OK
    assert strict.order_send() is None and strict.last_error() == (-2, "Invalid arguments")

    lenient = FakeMT5()                                              # a Python function cannot tell the two apart
    assert lenient.order_send(dict(GOOD_REQUEST), **empty).retcode == DONE


async def test_b9_orders_reach_the_package_without_a_keyword_dict() -> None:
    """The production path: a market order, a stop move and a close on the native adapter. All of them failed
    with "MT5 emir yanıtı alınamadı (Unnamed arguments not allowed (kod -2))" on the first live signal."""
    fake = FakeMT5(strict=True)
    b, sym = await eth(fake)
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    assert pos.status is PositionStatus.OPEN and pos.error is None and len(fake.positions) == 1
    pos = await b.update_stop_loss(pos, 2220.0)
    assert pos.error is None and pos.stop_loss == 2220.0
    pos = await b.close_position(pos, 1.0)
    assert pos.status is PositionStatus.CLOSED and fake.positions == {}
    # every order_send arrived as one positional request, none was refused for the way it was called
    sends = [c for c in fake.calls if c[0] == "order_send"]
    assert len(sends) == 3 and all(len(c[1]) == 1 and c[2] == {} for c in sends)
    assert [r["action"] for r in fake.sent] == [DEAL, SLTP, DEAL]
    await b.close()


async def test_b9_a_refusal_for_the_way_the_package_was_called_says_nothing_was_sent() -> None:
    """If the package ever refuses the call itself again, the owner must not be told the order may exist."""
    fake = FakeMT5(strict=True)
    b, sym = await eth(fake)
    strict_send = fake.order_send
    fake.order_send = lambda request: strict_send(request, **{})   # type: ignore[method-assign]
    with pytest.raises(BrokerError) as ei:
        await b.open_position(make_req(sym, 0.05, make_plan()))
    text = str(ei.value)
    assert "MT5 emir gönderilemedi: Unnamed arguments not allowed (kod -2)" in text
    assert "ulaşmış olabilir" not in text and "yanıtı alınamadı" not in text
    assert fake.sent == [] and fake.init_count == 1
    await b.close()


async def test_b9_every_request_of_the_adapter_passes_the_strict_fake() -> None:
    fake = FakeMT5(strict=True)
    b, sym = await eth(fake)
    pos = await b.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS)))
    pos = await b.update_stop_loss(pos, 2220.0)
    pos = await b.update_take_profits(pos, tps((2270.0, 0.5), (2290.0, 0.25), (2320.0, 0.25)))
    pos = await b.close_position(pos, 0.5)
    pos = await b.close_position(pos, 1.0)
    assert pos.status is PositionStatus.CLOSED and pos.error is None
    pending = await b.open_position(make_req(sym, 0.05, make_plan(**LIMIT_PLAN), tag="ctpending03"))
    pending = await b.update_stop_loss(pending, 2190.0)
    pending = await b.cancel_entry(pending)
    assert pending.status is PositionStatus.CANCELLED and pending.error is None
    kinds = {r["action"] for r in fake.sent}
    assert kinds == {DEAL, SLTP, PENDING, MT5.TRADE_ACTION_MODIFY, MT5.TRADE_ACTION_REMOVE}
    assert not any(c[0] == "order_send" and fake.request_problem(c[1][0]) for c in fake.calls)
    await b.close()


async def test_b9_a_long_client_tag_is_cut_to_what_the_package_accepts() -> None:
    fake = FakeMT5(strict=True)
    b, sym = await eth(fake)
    tag = "ct" + "0123456789" * 4                                     # 42 characters
    pos = await b.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS), tag=tag))     # 31 were refused
    comments = [r["comment"] for r in entries(fake)]
    assert len(comments) == 3 and all(len(c) <= 29 for c in comments) and len(set(comments)) == 3
    closed = await b.close_position(pos, 1.0)
    assert closed.status is PositionStatus.CLOSED and all(len(r["comment"]) <= 29 for r in closes(fake))
    await b.close()


async def test_b9_strict_fake_honours_the_terminal_switches() -> None:
    fake = FakeMT5(strict=True)
    b, sym = await eth(fake)
    fake.terminal.trade_allowed = False                               # Algo Trading button off -> retcode 10027
    with pytest.raises(BrokerError, match="yeşil"):
        await b.open_position(make_req(sym, 0.05, make_plan()))
    fake.terminal.trade_allowed = True
    fake.terminal.tradeapi_disabled = True                            # -> None, last_error -8
    assert fake.order_send({"action": DEAL, "symbol": "ETHUSD", "volume": 0.05, "type": 0, "price": ASK}) is None
    assert fake.last_error() == (-8, "Auto-Trading using Python API disabled in Terminal")
    await b.close()


# --------------------------------------------------------------------------- #
# B10 - health_problem() for the heartbeat
# --------------------------------------------------------------------------- #
async def test_b10_healthy_terminal_has_no_problem() -> None:
    fake = FakeMT5()
    b = await mt5_connected(fake)
    fake.calls.clear()
    assert await b.health_problem() is None
    assert [c[0] for c in fake.calls] == ["account_info", "terminal_info"]      # two cheap reads, nothing else
    await b.close()


@pytest.mark.parametrize(("flag", "value", "expected"), [
    ("connected", False,
     "MT5 terminali: işlem sunucusuna bağlı değil (sağ alt köşedeki bağlantı göstergesine bakın)."),
    ("trade_allowed", False,
     "MT5 terminali: 'Algo Trading' düğmesi kapalı (üst araç çubuğunda tıklayıp yeşil yapın)."),
    ("tradeapi_disabled", True,
     "MT5 terminali: Python ile işlem kapalı (Araçlar > Seçenekler > Uzman Danışmanlar: 'Python API' geçen "
     "kutunun işaretini kaldırın)."),
])
async def test_b10_names_the_problem_and_what_to_click(flag: str, value: bool, expected: str) -> None:
    fake = FakeMT5()
    b = await mt5_connected(fake)
    setattr(fake.terminal, flag, value)                               # changed while the bot is running
    assert await b.health_problem() == expected
    setattr(fake.terminal, flag, not value)
    assert await b.health_problem() is None                           # put right: healthy again
    await b.close()


async def test_b10_reports_every_problem_at_once_and_still_fits_into_the_heartbeat_alert() -> None:
    """The heartbeat (engine/health.py) cuts the text at 300 characters: with all three problems at once the
    last instruction must still be complete (the long startup texts would lose it)."""
    fake = FakeMT5()
    b = await mt5_connected(fake)
    fake.terminal.connected = False
    fake.terminal.trade_allowed = False
    fake.terminal.tradeapi_disabled = True
    text = await b.health_problem()
    assert text is not None and len(text) <= 285
    assert text.startswith("MT5 terminali: işlem sunucusuna bağlı değil (")
    assert "; 'Algo Trading' düğmesi kapalı (" in text
    assert text.endswith("; Python ile işlem kapalı (Araçlar > Seçenekler > Uzman Danışmanlar: 'Python API' "
                         "geçen kutunun işaretini kaldırın).")
    from copytrader.engine import health                              # the real limit, when that module is there
    assert len(text) < getattr(health, "REASON_MAX_CHARS", 300)
    # the startup warnings keep their long, step-by-step form (each is a message of its own, never cut)
    await b.close()
    await b.connect()
    assert len(b.startup_warnings) == 3 and all(len(w) > 150 for w in b.startup_warnings)
    await b.close()


async def test_b10_account_problems() -> None:
    fake = FakeMT5()
    b = await mt5_connected(fake)
    fake.fail_once["account_info"] = (-1, "Terminal: Call failed")
    text = await b.health_problem()
    assert text is not None and "hesap bilgisi alınamadı" in text and "Terminal: Call failed" in text

    fake.account.login = 4242                                         # someone logged in to another account
    text = await b.health_problem()
    assert text is not None and "farklı bir hesaba bağlı (4242, beklenen 5001)" in text
    fake.account.login = 5001
    assert await b.health_problem() is None
    await b.close()
    assert await b.health_problem() == "MT5 bağlı değil."


async def test_b10_hung_terminal_gives_a_text_within_the_health_limit_and_does_not_open_the_breaker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mt5_broker, "HEALTH_TIMEOUT_SEC", 0.3, raising=False)    # CALL_TIMEOUT_SEC stays 30 s
    fake = HungTerminal()
    b = await mt5_connected(fake)
    try:
        fake.hang = {"account_info"}
        t0 = time.monotonic()
        text = await asyncio.wait_for(b.health_problem(), GUARD)      # returns a text, never raises or hangs
        assert 0.25 <= time.monotonic() - t0 < 2.0
        assert text is not None and "MetaTrader terminali 0.3 sn içinde yanıt vermedi" in text
        # a health check gives up early; that alone must not make orders fail while the terminal is only slow
        assert b._stuck_call() is None
    finally:
        await settle(b, fake)


async def test_b10_respects_the_call_limit_and_the_circuit_breaker(short_limits: None) -> None:
    fake = HungTerminal()
    b = await mt5_connected(fake)
    try:
        fake.hang = {"terminal_info"}
        t0 = time.monotonic()
        text = await asyncio.wait_for(b.health_problem(), GUARD)      # limited by CALL_TIMEOUT_SEC (0.4 s here)
        assert text is not None and "0.4 sn içinde yanıt vermedi" in text and time.monotonic() - t0 < 2.0
        t0 = time.monotonic()
        text = await asyncio.wait_for(b.health_problem(), GUARD)      # breaker open: answered at once
        assert text is not None and "yanıt vermiyor" in text and time.monotonic() - t0 < 0.2
        fake.let_go()
        await until(lambda: b._stuck_call() is None)
        assert await b.health_problem() is None                       # the terminal answers again
    finally:
        await settle(b, fake)
