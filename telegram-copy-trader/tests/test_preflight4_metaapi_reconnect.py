"""Pre-flight batch 4, MetaApi adapter: the connection heals itself.

Production incident (2026-11-12): after about seven idle hours the first MetaApi request of a live signal was not
answered for 120 s and the trade was not opened on the optional Tickmill account. Whatever the cause was, the
adapter must notice within minutes that MetaApi does not answer, rebuild the connection from scratch by itself,
tell the owner, and never make a new signal wait for a dead connection.

P   ReconnectPolicy (pure): 3 failed health checks in a row, then back-off 1, 2, 5, 10, 10 ... minutes; the owner
    hears "down" once, "the reconnect did not help" once per outage and "recovered" once
R   reconnect(): the whole SDK client is dropped and connect() runs again; never twice at the same time, never
    while an order request is in flight; a failure leaves a clean "not connected" adapter and the next one works
H   health_problem() drives it (the heartbeat calls it every minute): a healthy connection is never touched
F   fail fast: while the latest check failed a NEW signal's calls are refused at once with a clear Turkish text
D   _drop_client(): nothing of a dropped REAL SDK client keeps running (no network: local objects only)

The venue is the fake RPC connection of test_metaapi_broker.py behind a fake MetaApi object (api_factory).
"""

from __future__ import annotations

import asyncio
import inspect
import itertools
import time
from types import SimpleNamespace
from typing import Any

import pytest

from copytrader.brokers import metaapi_broker as mb
from copytrader.brokers.base import BrokerError
from copytrader.brokers.metaapi_broker import MetaApiBroker
from copytrader.config import RiskConfig
from copytrader.models import PositionStatus, Severity

from fakes import FakeNotifier
from test_metaapi_broker import FakeConn, make_cfg, make_plan, make_req

TRADES = ("create_market_buy_order", "create_market_sell_order", "create_limit_buy_order", "create_limit_sell_order",
          "create_stop_buy_order", "create_stop_sell_order", "modify_position", "modify_order", "close_position",
          "close_position_partially", "cancel_order")
MIN = 60.0


# --------------------------------------------------------------------------- #
# fakes
# --------------------------------------------------------------------------- #
class Inner:
    """Stands for the SDK's RpcMetaApiConnection: the flags and the state method the adapter looks at."""

    def __init__(self) -> None:
        self._opened = False
        self._closed = False
        self.synchronized = True

    def is_synchronized(self) -> bool:
        return self.synchronized


class Line:
    """One RPC connection of a fake SDK client. The account behind it (positions, prices ...) is the shared
    venue; the line itself can be dead (requests are never answered), closed, or slow to synchronize."""

    def __init__(self, venue: FakeConn, number: int) -> None:
        self._meta_api_connection = Inner()
        self.venue = venue
        self.number = number
        self.dead = False                               # every request hangs
        self.closed = False
        self.sync_gate: asyncio.Event | None = None     # wait_synchronized() waits for this
        self.sync_error: BaseException | None = None
        self.order_gate: asyncio.Event | None = None    # trade requests wait here (the answer is on its way)
        self.orders_waiting = 0
        self.seen: list[str] = []

    async def connect(self) -> None:
        self.seen.append("connect")

    async def wait_synchronized(self, timeout_in_seconds: float = 300) -> None:
        self.seen.append("wait_synchronized")
        if self.sync_gate is not None:
            await self.sync_gate.wait()
        if self.sync_error is not None:
            raise self.sync_error

    async def close(self) -> None:
        self.seen.append("close")
        self.closed = True

    def __getattr__(self, name: str) -> Any:
        fn = getattr(self.venue, name)          # AttributeError for anything the venue does not have
        if not callable(fn):
            return fn

        async def request(*args: Any, **kwargs: Any) -> Any:
            if self.closed:
                raise Exception("This connection has been closed, please create a new connection")
            self.seen.append(name)
            if self.dead:
                await asyncio.sleep(3600)
            if name in TRADES and self.order_gate is not None:
                self.orders_waiting += 1
                try:
                    await self.order_gate.wait()
                finally:
                    self.orders_waiting -= 1
            return await fn(*args, **kwargs)

        return request


class World:
    """A fake MetaApi service: every MetaApi(token, opts) object the adapter creates, its account and the
    connection it hands out."""

    def __init__(self) -> None:
        self.venue = FakeConn()
        self.apis: list[Any] = []
        self.lines: list[Line] = []
        self.account_state = "DEPLOYED"
        self.account_log: list[str] = []
        self.get_account_error: BaseException | None = None
        self.prepare: Any = None                # called with every new Line before the adapter sees it

    def factory(self, token: str, opts: dict[str, Any]) -> Any:
        world = self

        class Account:
            def __init__(self) -> None:
                self.state = world.account_state

            async def deploy(self) -> None:
                world.account_log.append("deploy")
                self.state = "DEPLOYING"

            async def wait_deployed(self) -> None:
                world.account_log.append("wait_deployed")

            async def wait_connected(self) -> None:
                world.account_log.append("wait_connected")

            def get_rpc_connection(self) -> Line:
                line = Line(world.venue, len(world.lines) + 1)
                if world.prepare is not None:
                    world.prepare(line)
                world.lines.append(line)
                return line

        class Accounts:
            async def get_account(self, _id: str) -> Account:
                world.account_log.append("get_account")
                if world.get_account_error is not None:
                    raise world.get_account_error
                return Account()

        api = SimpleNamespace(metatrader_account_api=Accounts(), closed=False)
        api.close = lambda: setattr(api, "closed", True)
        self.apis.append(api)
        return api

    @property
    def line(self) -> Line:
        return self.lines[-1]


def policy(*args: Any) -> Any:
    return mb.ReconnectPolicy(*args)


@pytest.fixture
def world() -> World:
    return World()


@pytest.fixture
async def broker(world: World, monkeypatch: pytest.MonkeyPatch) -> Any:
    """A connected adapter that made its connection itself (as in production), with short health timeouts."""
    monkeypatch.setattr(mb, "HEALTH_TIMEOUT_SEC", 0.05)
    monkeypatch.setattr(mb, "HEALTH_SYNC_GRACE_SEC", 0.02)
    monkeypatch.setattr(mb, "LOST_REPLY_DELAY_SEC", 0.0)
    b = MetaApiBroker(make_cfg(), api_factory=world.factory)
    b.retry_delay_sec = 0
    b.set_risk_config(RiskConfig(include_fees_in_risk=False))
    await b.connect()
    yield b
    await b.close()


async def until(cond: Any, what: str = "condition") -> None:
    """Wait (at most 2 s) until ``cond()`` is true."""
    deadline = time.monotonic() + 2.0
    while not cond():
        assert time.monotonic() < deadline, f"timed out waiting for: {what}"
        await asyncio.sleep(0.001)


async def heal_done(b: MetaApiBroker) -> None:
    task = b._heal_task
    if task is not None:
        await asyncio.wait({task}, timeout=5)
        assert task.done()


async def sent(notifier: FakeNotifier) -> list[str]:
    await asyncio.sleep(0.01)           # notices are sent by their own task
    return notifier.texts()


async def raises_fast(coro: Any) -> str:
    """The call must end in a BrokerError at once (it is given a second so that a wrong tree fails, not hangs)."""
    t0 = time.monotonic()
    with pytest.raises(BrokerError) as err:
        await asyncio.wait_for(coro, 1.0)
    assert time.monotonic() - t0 < 0.02
    return str(err.value)


# --------------------------------------------------------------------------- #
# P  the policy
# --------------------------------------------------------------------------- #
def test_p_first_attempt_comes_with_the_third_failed_check_in_a_row() -> None:
    p = policy()
    assert p.check(False, 0) == [] and p.check(False, 60) == []
    assert p.check(True, 120) == []                             # one healthy check: the count starts again
    assert p.check(False, 180) == [] and p.check(False, 240) == []
    assert p.check(False, 300) == ["down", "reconnect"]


def test_p_backoff_between_attempts_is_1_2_5_10_10_minutes() -> None:
    """Heartbeat every 60 s; every attempt fails at once. The pause counts from the END of an attempt and is
    looked at by the next failed check."""
    p = policy()
    started: list[float] = []
    now = 0.0
    while len(started) < 7:
        if "reconnect" in p.check(False, now):
            p.attempt_started()
            started.append(now)
            p.attempt_finished(False, now)
        now += 60.0
    assert started[0] == 2 * MIN                                # third check: t = 0, 60, 120
    assert [(b - a) / MIN for a, b in itertools.pairwise(started)] == [1, 2, 5, 10, 10, 10]
    assert [p.pause_after(n) / MIN for n in (1, 2, 3, 4, 5, 9)] == [1, 2, 5, 10, 10, 10]


def test_p_a_long_attempt_is_followed_by_its_pause_not_by_the_next_attempt() -> None:
    p = policy()
    for t in (0, 60, 120):
        events = p.check(False, t)
    assert events == ["down", "reconnect"] and p.attempt_started() == 1
    assert p.check(False, 180) == [] and p.check(False, 240) == []      # still running: nothing new
    assert p.attempt_finished(False, 300) == ["reconnect_failed"]       # it took three minutes
    assert p.check(False, 301) == []                                    # one minute of rest first
    assert p.check(False, 361) == ["reconnect"]


def test_p_a_check_a_moment_before_the_pause_is_over_still_starts_the_attempt() -> None:
    """Production: attempt 1 runs into its 180 s limit and ends 0.2 s after a heartbeat check; the check one
    minute later is 0.2 s early. Without the slack every pause after such an attempt lasted a heartbeat longer."""
    p = policy()
    for t in (0.0, 60.0, 120.0):
        p.check(False, t)
    p.attempt_started()
    assert p.attempt_finished(False, 300.2) == ["reconnect_failed"]
    assert p.check(False, 300.3) == []                          # not at once
    assert p.check(False, 354.0) == []                          # 6 s early: no
    assert p.check(False, 360.0) == ["reconnect"]               # 0.2 s early: yes
    assert policy(3, (60.0,), 0.0).slack == 0.0 and p.slack == mb.RECONNECT_SLACK_SEC == 5.0


def test_p_owner_hears_down_once_failed_once_and_recovered_once_per_outage() -> None:
    p = policy()
    heard: list[str] = []
    now = 0.0
    for _ in range(40):                                         # forty minutes of outage, every attempt fails
        events = p.check(False, now)
        heard += [e for e in events if e != "reconnect"]
        if "reconnect" in events:
            p.attempt_started()
            heard += p.attempt_finished(False, now + 5)
        now += 60.0
    assert p.attempts >= 5
    assert heard == ["down", "reconnect_failed"]
    assert p.check(True, now) == ["recovered"]
    assert p.check(True, now + 60) == []                        # only once
    assert (p.problems, p.attempts, p.down, p.retry_at) == (0, 0, False, None)
    heard = []
    for t in (0, 60, 120):                                      # the next outage is told again
        heard += p.check(False, now + 120 + t)
    assert heard == ["down", "reconnect"]


def test_p_an_attempt_that_connected_but_did_not_help_counts_as_failed() -> None:
    p = policy()
    for t in (0, 60, 120):
        p.check(False, t)
    p.attempt_started()
    assert p.attempt_finished(True, 130) == []                  # connected: the next check decides
    assert p.check(False, 180) == ["reconnect_failed"]          # ... it still does not answer; 50 s < 1 min
    assert p.check(False, 240) == ["reconnect"]
    p.attempt_started()
    assert p.attempt_finished(True, 250) == []
    assert p.check(False, 300) == []                            # told once; 2 minutes of rest
    assert p.check(True, 360) == ["recovered"]


def test_p_an_attempt_that_helped_is_never_reported_as_failed() -> None:
    p = policy()
    for t in (0, 60, 120):
        p.check(False, t)
    p.attempt_started()
    assert p.attempt_finished(True, 130) == []
    assert p.check(True, 180) == ["recovered"]


def test_p_an_attempt_that_could_not_start_is_asked_for_again_and_costs_no_pause() -> None:
    p = policy()
    for t in (0, 60, 120):
        events = p.check(False, t)
    assert "reconnect" in events                                # ... an order was in flight: not started
    assert p.check(False, 180) == ["reconnect"] and p.attempts == 0
    assert p.attempt_started() == 1


def test_p_trouble_that_ends_before_the_third_check_is_silent() -> None:
    p = policy()
    assert p.check(False, 0) == [] and p.check(False, 60) == [] and p.check(True, 120) == []
    assert (p.problems, p.down) == (0, False)


def test_p_defaults_are_three_checks_and_1_2_5_10_minutes() -> None:
    assert mb.RECONNECT_AFTER_PROBLEMS == 3
    assert [x / MIN for x in mb.RECONNECT_BACKOFF_SEC] == [1, 2, 5, 10]
    assert mb.RECONNECT_TIMEOUT_SEC == 180.0
    p = policy()
    assert (p.after, p.backoff) == (3, (60.0, 120.0, 300.0, 600.0))


# --------------------------------------------------------------------------- #
# R  reconnect()
# --------------------------------------------------------------------------- #
async def test_r_reconnect_drops_the_whole_client_and_connects_from_scratch(world: World,
                                                                            broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    old_api, old_line = world.apis[0], world.lines[0]

    await broker.reconnect()

    assert old_api.closed and old_line.closed                   # nothing of the old client is kept
    assert len(world.apis) == 2 and len(world.lines) == 2
    assert broker._api is world.apis[1] and broker._conn is world.lines[1] and not world.apis[1].closed
    assert world.lines[1].seen[:3] == ["connect", "wait_synchronized", "get_account_information"]
    assert broker._connected and not broker._reconnecting and broker._half_open is None
    assert (await broker.sync_position(pos)).status is PositionStatus.OPEN      # the account is the same
    again = await broker.open_position(make_req(sym, 0.05, make_plan(), tag="ctafter0001"))
    assert again.status is PositionStatus.OPEN and len(world.venue.positions) == 2
    assert "close" not in world.lines[1].seen


async def test_r_reconnect_deploys_the_account_if_it_is_not_deployed(world: World, broker: MetaApiBroker) -> None:
    world.account_log.clear()
    world.account_state = "UNDEPLOYED"                          # MetaApi undeployed it in the meantime
    await broker.reconnect()
    assert world.account_log == ["get_account", "deploy", "wait_deployed", "wait_connected"]
    assert await broker.get_equity() == 10_000.0


async def test_r_never_while_an_order_request_is_in_flight(world: World, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    line = world.line
    line.order_gate = asyncio.Event()                           # the order is at the broker, its answer is not here
    opening = asyncio.ensure_future(broker.open_position(make_req(sym, 0.05, make_plan())))
    await until(lambda: line.orders_waiting, "the order request to reach the broker")
    assert broker.busy and broker._orders_in_flight == 1 and broker._flows == 1

    text = await raises_fast(broker.reconnect())
    assert "emir isteği sürerken" in text.lower()
    assert not broker._reconnecting and broker._connected and broker._conn is line
    assert len(world.apis) == 1 and not world.apis[0].closed and not line.closed

    line.order_gate.set()
    pos = await opening
    assert pos.status is PositionStatus.OPEN and pos.error is None and pos.data["legs"][0]["sl"] == 2205.11
    assert not broker.busy
    await broker.reconnect()                                    # now it may
    assert len(world.apis) == 2 and world.apis[0].closed


@pytest.mark.parametrize("flow", ["close_position", "update_stop_loss", "update_take_profits", "cancel_entry"])
async def test_r_never_between_two_requests_of_a_management_flow(world: World, broker: MetaApiBroker,
                                                                 flow: str) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    line = world.line
    line.order_gate = asyncio.Event()
    calls = {
        "close_position": lambda: broker.close_position(pos, 1.0),
        "update_stop_loss": lambda: broker.update_stop_loss(pos, 2210.0),
        "update_take_profits": lambda: broker.update_take_profits(pos, make_plan(tps=((2300.0, 1.0),)).take_profits),
        "cancel_entry": lambda: broker.cancel_entry(pos),
    }
    running = asyncio.ensure_future(calls[flow]())
    await until(lambda: line.orders_waiting, "the order request to reach the broker")
    assert broker._flows >= 1               # cancel_entry of a filled entry runs close_position inside itself
    assert "emir isteği sürerken" in (await raises_fast(broker.reconnect())).lower()
    line.order_gate.set()
    await running
    assert not broker.busy and len(world.apis) == 1


async def test_r_never_twice_at_the_same_time(world: World, broker: MetaApiBroker) -> None:
    gate = asyncio.Event()
    world.prepare = lambda line: setattr(line, "sync_gate", gate)       # the new connection takes its time
    first = asyncio.ensure_future(broker.reconnect())
    await until(lambda: len(world.lines) >= 2, "the reconnect to make its new connection")
    assert broker._reconnecting

    assert "zaten yenileniyor" in await raises_fast(broker.reconnect())
    assert len(world.apis) == 2                                 # the second call created nothing

    gate.set()
    await first
    assert len(world.apis) == 2 and broker._connected and not broker._reconnecting


async def test_r_calls_made_while_it_reconnects_fail_at_once_with_a_clear_text(world: World,
                                                                               broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    gate = asyncio.Event()
    world.prepare = lambda line: setattr(line, "sync_gate", gate)
    running = asyncio.ensure_future(broker.reconnect())
    await until(lambda: len(world.lines) >= 2, "the reconnect to make its new connection")
    for call in (broker.resolve_symbol("ETH"), broker.get_equity(), broker.get_quote(sym), broker.sync_position(pos),
                 broker.close_position(pos, 1.0), broker.open_position(make_req(sym, 0.05, make_plan(), tag="ctx"))):
        text = await raises_fast(call)
        assert text.startswith("MetaApi bağlantısı şu an yenileniyor") and "bu işlem yapılamadı" in text
    assert world.lines[1].seen == ["connect", "wait_synchronized"]      # nothing reached the half-made connection
    gate.set()
    await running
    assert await broker.get_equity() == 10_000.0


async def test_r_a_failed_reconnect_leaves_a_clean_adapter_and_the_next_one_works(world: World,
                                                                                  broker: MetaApiBroker) -> None:
    world.prepare = lambda line: setattr(line, "sync_error", Exception("Timed out waiting for MetaApi"))
    with pytest.raises(BrokerError, match="Timed out waiting for MetaApi"):
        await broker.reconnect()

    assert (broker._api, broker._conn, broker._half_open) == (None, None, None)
    assert not broker._connected and not broker._reconnecting and not broker.busy
    assert [a.closed for a in world.apis] == [True, True]       # also the client of the failed attempt
    assert [line.closed for line in world.lines] == [True, True]    # ... and its half-open connection
    text = await raises_fast(broker.get_equity())
    assert "MetaApi bağlantısı yok" in text and "tekrar deniyor" in text

    world.prepare = None                                        # MetaApi is back
    await broker.reconnect()
    assert len(world.apis) == 3 and broker._api is world.apis[2] and await broker.get_equity() == 10_000.0


async def test_r_a_failed_reconnect_before_any_connection_was_made_is_clean_too(world: World,
                                                                                broker: MetaApiBroker) -> None:
    world.get_account_error = Exception("getaddrinfo failed")   # MetaApi's REST API cannot be reached
    with pytest.raises(BrokerError, match="getaddrinfo failed"):
        await broker.reconnect()
    assert (broker._api, broker._conn, broker._half_open, broker._connected) == (None, None, None, False)
    assert [a.closed for a in world.apis] == [True, True] and len(world.lines) == 1
    world.get_account_error = None
    await broker.reconnect()
    assert await broker.get_equity() == 10_000.0


async def test_r_an_attempt_is_bounded(world: World, broker: MetaApiBroker, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mb, "RECONNECT_TIMEOUT_SEC", 0.05)
    world.prepare = lambda line: setattr(line, "sync_gate", asyncio.Event())    # never synchronizes
    t0 = time.monotonic()
    with pytest.raises(BrokerError, match="yeniden bağlanma 0 sn içinde tamamlanamadı"):
        await asyncio.wait_for(broker.reconnect(), 3.0)         # (a tree without the limit fails here, not hangs)
    assert time.monotonic() - t0 < 1.0
    assert (broker._api, broker._conn, broker._half_open, broker._reconnecting) == (None, None, None, False)
    assert world.apis[1].closed and world.lines[1].closed


async def test_r_a_connection_handed_in_from_outside_is_never_rebuilt() -> None:
    conn = FakeConn()
    b = MetaApiBroker(make_cfg(), connection=conn)
    await b.connect()
    with pytest.raises(BrokerError, match="dışarıdan verildi"):
        await b.reconnect()
    assert await b.get_equity() == 10_000.0 and not conn.called("close")
    await b.close()


# --------------------------------------------------------------------------- #
# H  health_problem() drives the healing
# --------------------------------------------------------------------------- #
async def test_h_three_failed_checks_rebuild_the_connection_and_it_works_again(world: World,
                                                                               broker: MetaApiBroker) -> None:
    notifier = FakeNotifier()
    broker.set_notifier(notifier)
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    world.line.dead = True                                      # MetaApi stops answering on this connection

    first, second = await broker.health_problem(), await broker.health_problem()
    assert first == second and "yanıt gelmedi" in first and "deneme" not in first
    assert len(world.apis) == 1 and broker._heal_task is None   # two failed checks: nothing yet
    third = await broker.health_problem()                       # never waits for the reconnect itself
    assert "yanıt gelmedi" in third and "Bot bağlantıyı kendisi sıfırdan kuruyor (1. deneme)" in third
    await heal_done(broker)

    assert len(world.apis) == 2 and world.apis[0].closed and world.lines[0].closed
    assert broker._conn is world.lines[1] and broker._connected
    assert await broker.get_equity() == 10_000.0                # usable at once, not only after the next check
    assert await broker.health_problem() is None                # the heartbeat reports the recovery
    assert broker._policy.problems == 0 and broker._policy.attempts == 0
    assert (await broker.sync_position(pos)).status is PositionStatus.OPEN
    opened = await broker.open_position(make_req(sym, 0.05, make_plan(), tag="cthealed001"))
    assert opened.status is PositionStatus.OPEN
    assert await sent(notifier) == []                           # "down" / "recovered" are the heartbeat's notices


async def test_h_a_healthy_connection_is_never_torn_down(world: World, broker: MetaApiBroker) -> None:
    line = world.line
    before = len(line.seen)
    for _ in range(20):
        assert await broker.health_problem() is None
    assert len(world.apis) == 1 and len(world.lines) == 1 and not world.apis[0].closed and not line.closed
    assert line.seen[before:] == ["get_account_information"] * 20       # one cheap read per check, nothing else
    assert broker._heal_task is None and broker._policy.problems == 0 and broker._unhealthy is None


async def test_h_one_or_two_failed_checks_do_nothing(world: World, broker: MetaApiBroker) -> None:
    line = world.line
    for _ in range(3):                                          # two bad checks, then it answers again: three times
        line.dead = True
        assert await broker.health_problem() is not None and await broker.health_problem() is not None
        line.dead = False
        assert await broker.health_problem() is None
    assert len(world.apis) == 1 and broker._heal_task is None and not line.closed


async def test_h_attempts_that_fail_back_off_and_the_owner_is_told_once(world: World, broker: MetaApiBroker) -> None:
    notifier = FakeNotifier()
    broker.set_notifier(notifier)
    broker._policy = policy(3, (0.2, 0.6), 0.0)                      # instead of 1, 2, 5, 10 minutes
    world.line.dead = True
    world.prepare = lambda line: setattr(line, "sync_error", Exception("Timed out waiting for MetaApi"))

    for _ in range(3):
        await broker.health_problem()
    await heal_done(broker)                                     # attempt 1 failed
    assert len(world.apis) == 2 and broker._policy.attempts == 1
    notices = await sent(notifier)
    assert len(notices) == 1 and notifier.notifications[0][1] is Severity.WARNING
    assert "sıfırdan kurulamadı" in notices[0] and "Timed out waiting for MetaApi" in notices[0]
    assert "<b>metaapi-demo</b>" in notices[0] and "diğer hesaplar etkilenmez" in notices[0]

    text = await broker.health_problem()                        # not connected now; too early for attempt 2
    assert text is not None and "MetaApi bağlı değil" in text and len(world.apis) == 2
    await asyncio.sleep(0.25)
    await broker.health_problem()
    await heal_done(broker)                                     # attempt 2 failed
    assert len(world.apis) == 3 and broker._policy.attempts == 2
    await asyncio.sleep(0.25)
    await broker.health_problem()                               # the pause after attempt 2 is longer
    assert len(world.apis) == 3
    assert [a.closed for a in world.apis] == [True] * 3 and [ln.closed for ln in world.lines] == [True] * 3
    assert (broker._api, broker._conn, broker._half_open) == (None, None, None)

    world.prepare = None                                        # MetaApi answers again
    await asyncio.sleep(0.4)
    await broker.health_problem()
    await heal_done(broker)                                     # attempt 3 connects
    assert len(world.apis) == 4 and broker._connected
    assert await broker.health_problem() is None
    assert await broker.get_equity() == 10_000.0
    assert len(await sent(notifier)) == 1                       # one notice for the whole outage


async def test_h_a_reconnect_that_connected_but_did_not_help_is_told_once(world: World, broker: MetaApiBroker) -> None:
    notifier = FakeNotifier()
    broker.set_notifier(notifier)
    broker._policy = policy(3, (0.2,), 0.0)
    world.line.dead = True
    for _ in range(3):
        await broker.health_problem()
    await heal_done(broker)
    assert broker._connected and len(world.apis) == 2 and await sent(notifier) == []
    world.lines[1].dead = True                                  # the fresh connection does not answer either

    await broker.health_problem()
    notices = await sent(notifier)
    assert len(notices) == 1 and "sıfırdan kuruldu ama hesap hâlâ yanıt vermiyor" in notices[0]
    assert len(world.apis) == 2                                 # the pause after attempt 1 is not over
    await asyncio.sleep(0.25)
    await broker.health_problem()
    await heal_done(broker)
    world.lines[2].dead = True
    await broker.health_problem()
    assert len(world.apis) == 3 and len(await sent(notifier)) == 1      # not once per attempt


async def test_h_healing_waits_for_an_order_request_in_flight(world: World, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    line = world.line
    line.order_gate = asyncio.Event()
    opening = asyncio.ensure_future(broker.open_position(make_req(sym, 0.05, make_plan())))
    await until(lambda: line.orders_waiting, "the order request to reach the broker")
    world.venue.fail["get_account_information"] = [Exception("no answer")] * 4     # the health read fails
    for _ in range(3):
        assert await broker.health_problem() is not None
    assert broker._heal_task is None and not broker._reconnecting       # put off: the order is still in flight
    assert len(world.apis) == 1 and not line.closed and broker._policy.attempts == 0

    line.order_gate.set()
    pos = await opening
    assert pos.status is PositionStatus.OPEN and pos.error is None
    await broker.health_problem()                               # the next failed check starts it
    await heal_done(broker)
    assert len(world.apis) == 2 and world.apis[0].closed and broker._connected
    assert (await broker.sync_position(pos)).status is PositionStatus.OPEN


async def test_h_trading_switched_off_on_the_account_is_reported_but_nothing_is_rebuilt(world: World,
                                                                                        broker: MetaApiBroker) -> None:
    world.venue.info["tradeAllowed"] = False                    # MetaApi answers: nothing a new connection could fix
    for _ in range(6):
        assert await broker.health_problem() == "MetaApi hesabında işlem izni kapalı görünüyor."
    assert len(world.apis) == 1 and broker._heal_task is None and broker._unhealthy is None


async def test_h_close_stops_a_running_reconnect_and_nothing_starts_again(world: World, broker: MetaApiBroker) -> None:
    world.line.dead = True
    world.prepare = lambda line: setattr(line, "sync_gate", asyncio.Event())    # the new connection hangs
    for _ in range(3):
        await broker.health_problem()
    await until(lambda: len(world.lines) >= 2, "the reconnect to make its new connection")
    task = broker._heal_task
    assert task is not None and not task.done() and broker._reconnecting

    await broker.close()

    assert task.done() and not broker._reconnecting and broker._heal_task is None
    assert [a.closed for a in world.apis] == [True, True] and [ln.closed for ln in world.lines] == [True, True]
    assert (broker._api, broker._conn, broker._half_open, broker._connected) == (None, None, None, False)
    for _ in range(5):                                          # a closed adapter does not bring itself back
        assert await broker.health_problem() == "MetaApi bağlı değil."
    await asyncio.sleep(0.05)
    assert len(world.apis) == 2 and broker._heal_task is None
    assert "connect çağrılmadı" in await raises_fast(broker.get_equity())


async def test_h_a_check_cut_by_the_callers_time_limit_counts_but_starts_nothing(world: World, broker: MetaApiBroker,
                                                                               monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mb, "HEALTH_TIMEOUT_SEC", 5.0)
    world.line.dead = True
    for _ in range(3):
        with pytest.raises(asyncio.TimeoutError):               # the heartbeat's own limit (30 s there)
            await asyncio.wait_for(broker.health_problem(), 0.02)
    assert broker._policy.problems == 3 and broker._policy.down
    assert broker._heal_task is None and len(world.apis) == 1   # never from inside a cancelled call
    monkeypatch.setattr(mb, "HEALTH_TIMEOUT_SEC", 0.05)
    await broker.health_problem()                               # the next ordinary check does
    await heal_done(broker)
    assert len(world.apis) == 2 and broker._connected


async def test_h_the_engine_hands_over_its_notifier(world: World) -> None:
    """Engine._try_connect: ``set_notifier = getattr(broker, "set_notifier", None)`` then ``set_notifier(notifier)``."""
    b = MetaApiBroker(make_cfg(), api_factory=world.factory)
    notifier = FakeNotifier()
    set_notifier = getattr(b, "set_notifier", None)
    assert callable(set_notifier)
    set_notifier(notifier)
    assert b._notifier is notifier


async def test_h_the_token_never_reaches_the_owner_notice(world: World, broker: MetaApiBroker) -> None:
    notifier = FakeNotifier()
    broker.set_notifier(notifier)
    world.line.dead = True
    world.get_account_error = Exception("Cannot connect to https://x/ws?auth-token=SECRET123&clientId=0.4 <refused>")
    for _ in range(3):
        await broker.health_problem()
    await heal_done(broker)
    (notice,) = await sent(notifier)
    assert "SECRET123" not in notice and "auth-token=***" in notice and "&lt;refused&gt;" in notice


async def test_h_with_the_real_heartbeat_the_owner_hears_down_once_and_recovered_once(world: World,
                                                                                    broker: MetaApiBroker) -> None:
    from copytrader.engine.health import BrokerHeartbeat

    notifier = FakeNotifier()
    broker.set_notifier(notifier)
    heartbeat = BrokerHeartbeat(lambda: [broker], notifier)
    await heartbeat.check_once()
    assert notifier.notifications == []
    world.line.dead = True
    for _ in range(3):
        await heartbeat.check_once()
    await heal_done(broker)
    assert len(world.apis) == 2 and broker._connected
    await heartbeat.check_once()
    await heartbeat.check_once()
    texts = await sent(notifier)
    assert len(texts) == 2
    assert "metaapi-demo" in texts[0] and "yanıt gelmedi" in texts[0] and "sıfırdan kuruyor (1. deneme)" in texts[0]
    assert texts[1].startswith("✅") and "metaapi-demo" in texts[1]
    assert [s for _, s in notifier.notifications] == [Severity.WARNING, Severity.INFO]


# --------------------------------------------------------------------------- #
# F  fail fast
# --------------------------------------------------------------------------- #
async def test_f_after_a_failed_check_a_new_signal_is_refused_at_once(world: World, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    line = world.line
    line.dead = True
    assert await broker.health_problem() is not None            # one failed check is enough
    before = len(line.seen)

    for call in (broker.resolve_symbol("BTC"), broker.get_equity(), broker.get_free_margin(),
                 broker.account_currency(), broker.open_position(make_req(sym, 0.05, make_plan(), tag="ctnew00001"))):
        text = await raises_fast(call)
        assert text.startswith("MetaApi şu an yanıt vermiyor (") and "yanıt gelmedi" in text
        assert "bu hesapta yeni işlem açılmadı" in text and "Bot bağlantıyı kendisi yeniliyor" in text
    assert line.seen[before:] == []                             # nothing was sent to the dead connection

    task = asyncio.ensure_future(broker.sync_position(pos))     # an EXISTING position is still looked after
    await asyncio.sleep(0.02)
    assert line.seen[before:] == ["get_positions"] and not task.done()
    task.cancel()
    await asyncio.wait({task})

    line.dead = False                                           # it answers again: the next check lifts the refusal
    assert await broker.health_problem() is None
    assert await broker.get_equity() == 10_000.0
    assert (await broker.resolve_symbol("BTC")) is not None


async def test_f_link_reported_back_by_the_sdk_lets_a_new_signal_try(world: World, broker: MetaApiBroker) -> None:
    inner = world.line._meta_api_connection
    inner.synchronized = False                                  # the SDK itself says: no terminal connected
    assert await broker.health_problem() == "MetaApi bağlantısı kopuk; yeniden bağlanması bekleniyor."
    assert "yanıt vermiyor" in await raises_fast(broker.get_equity())
    inner.synchronized = True                                   # the SDK reconnected by itself, before the next check
    assert await broker.get_equity() == 10_000.0


async def test_f_the_refusal_expires_when_no_health_check_follows(world: World, broker: MetaApiBroker,
                                                                 monkeypatch: pytest.MonkeyPatch) -> None:
    """Without a heartbeat (monitor not running, a command line tool) nothing would ever lift the refusal."""
    assert mb.UNHEALTHY_TTL_SEC == 180.0
    monkeypatch.setattr(mb, "UNHEALTHY_TTL_SEC", 0.05)
    line = world.line
    line.dead = True
    assert await broker.health_problem() is not None
    assert "yanıt vermiyor" in await raises_fast(broker.get_equity())
    line.dead = False                                           # it answers again, but nobody checks
    await asyncio.sleep(0.06)
    assert await broker.get_equity() == 10_000.0


async def test_f_not_connected_after_a_failed_reconnect_is_said_at_once(world: World, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    world.get_account_error = Exception("getaddrinfo failed")
    with pytest.raises(BrokerError):
        await broker.reconnect()
    for call in (broker.resolve_symbol("ETH"), broker.get_equity(), broker.sync_position(pos),
                 broker.close_position(pos, 1.0), broker.open_position(make_req(sym, 0.05, make_plan(), tag="cty"))):
        assert "MetaApi bağlantısı yok (yeniden bağlanma denemesi sonuç vermedi" in await raises_fast(call)


async def test_f_before_the_first_connect_the_old_text_stays(world: World) -> None:
    b = MetaApiBroker(make_cfg(), api_factory=world.factory)
    assert "MetaApi bağlı değil (connect çağrılmadı)" in await raises_fast(b.get_equity())
    assert await b.health_problem() == "MetaApi bağlı değil."
    assert world.apis == []


# --------------------------------------------------------------------------- #
# D  nothing of a dropped REAL SDK client keeps running
# --------------------------------------------------------------------------- #
class _Eio:
    def __init__(self) -> None:
        self.state = "connected"
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self.ping_loop_event = asyncio.Event()
        self.http = SimpleNamespace(closed=False)
        self.http.close = self._close_http
        self.read_loop_task = asyncio.ensure_future(asyncio.sleep(3600))
        self.write_loop_task = asyncio.ensure_future(asyncio.sleep(3600))
        self.ping_loop_task = asyncio.ensure_future(asyncio.sleep(3600))

    async def _close_http(self) -> None:
        self.http.closed = True


class _Socket:
    """What _close_socket() touches of a socket.io client. ``polite=False``: the peer never answers the goodbye."""

    def __init__(self, polite: bool = True) -> None:
        self.eio = _Eio()
        self.polite = polite
        self.disconnects = 0

    async def disconnect(self) -> None:
        self.disconnects += 1
        if not self.polite:
            await asyncio.sleep(3600)

    async def connect(self, *args: Any, **kwargs: Any) -> None:
        raise AssertionError("a dropped client opened a socket")


def _sdk_tasks() -> list[str]:
    out = []
    for task in asyncio.all_tasks():
        code = getattr(task.get_coro(), "cr_code", None)
        name = getattr(code, "co_filename", "") or ""
        if not task.done() and ("metaapi_cloud_sdk" in name or "socketio" in name or "engineio" in name):
            out.append(getattr(task.get_coro(), "__qualname__", "?"))
    return sorted(out)


@pytest.mark.parametrize("polite", [True, False])
async def test_d_nothing_of_a_dropped_real_sdk_client_keeps_running(monkeypatch: pytest.MonkeyPatch,
                                                                    polite: bool) -> None:
    """A REAL MetaApi object of the installed SDK in the state of a client whose server stopped answering: two
    sockets with their interval jobs, a status timer, a subscribe loop that keeps retrying, a latency probe that
    cannot connect, a call waiting for "a terminal is connected" and a request without an answer. No network:
    the sockets are stand-ins and the probe's own socket never gets further than "connecting"."""
    pytest.importorskip("metaapi_cloud_sdk")
    from metaapi_cloud_sdk import MetaApi
    from metaapi_cloud_sdk.clients.metaapi import latency_service as sdk_latency
    from metaapi_cloud_sdk.clients.metaapi.synchronization_throttler import SynchronizationThrottler
    from metaapi_cloud_sdk.clients.timeout_exception import TimeoutException

    class StillConnecting(sdk_latency.socketio.AsyncClient):    # type: ignore[name-defined,misc]
        async def connect(self, *_a: Any, **_k: Any) -> None:
            await asyncio.sleep(3600)

    monkeypatch.setattr(sdk_latency, "socketio", SimpleNamespace(AsyncClient=StillConnecting))
    monkeypatch.setattr(mb, "SOCKET_CLOSE_SEC", 0.05, raising=False)
    assert _sdk_tasks() == []
    loop = asyncio.get_running_loop()
    api = MetaApi("token", {})
    ws = api._metaapi_websocket_client
    ws.set_url("http://127.0.0.1:1")                            # never contacted; keeps the SDK from asking for one
    acc, region = "acc-1", "new-york"
    ws._accounts_by_replica_id[acc] = acc
    ws._account_replicas[acc] = {region: acc}
    ws._regions_by_accounts[acc] = {"region": region, "connections": 1, "lastUsed": time.time()}
    sockets = [_Socket(polite), _Socket(polite)]
    unanswered = loop.create_future()
    instances = []
    for number, sock in enumerate(sockets):
        throttler = SynchronizationThrottler(ws, 0, number, region, {})
        throttler.start()
        inst = {"id": 0, "connected": True, "resolved": True, "sessionId": "s", "isReconnecting": False,
                "requestResolves": {"r1": unanswered} if number == 0 else {}, "socket": sock,
                "synchronizationThrottler": throttler, "subscribeLock": None, "instanceNumber": number}
        ws._socket_instances.setdefault(region, {})[number] = [inst]
        ws._socket_instances_by_accounts[number] = {acc: 0}
        instances.append(inst)

    async def no_answer(*_a: Any, **_k: Any) -> None:
        raise TimeoutException("no answer")

    ws._make_request = no_answer
    ws.ensure_subscribe(acc, 0)                                 # the SDK's own "subscribe until authenticated" loop
    timer = asyncio.ensure_future(asyncio.sleep(60))
    ws._status_timers[f"{acc}:{region}:0:host"] = timer
    probe = asyncio.ensure_future(ws._latency_service.on_connected(f"{acc}:{region}:0:host"))
    waiting = asyncio.ensure_future(ws._latency_service.wait_connected_instance("other-account"))
    await asyncio.sleep(0.05)
    assert f"{acc}:0" in ws._subscription_manager._subscriptions and not probe.done() and not waiting.done()
    assert len(_sdk_tasks()) >= 8                               # the client is alive and busy

    b = MetaApiBroker(make_cfg(), api_factory=lambda token, opts: api)
    b._api = api
    t0 = time.monotonic()
    await b._teardown()                                         # what close() and reconnect() do
    assert time.monotonic() - t0 < 1.0
    await asyncio.sleep(0.05)

    assert _sdk_tasks() == []                                   # not one task of the client is left
    for inst, sock in zip(instances, sockets, strict=True):
        assert inst["connected"] is False and sock.disconnects == 1
        assert sock.eio.state == "disconnected" and sock.eio.http.closed
        assert all(t.done() for t in (sock.eio.read_loop_task, sock.eio.write_loop_task, sock.eio.ping_loop_task))
        throttler = inst["synchronizationThrottler"]
        throttler.start()                                       # the SDK's disconnect handler would do this
        assert throttler._remove_old_sync_ids_interval is None and throttler._process_queue_interval is None
        with pytest.raises(ConnectionError):
            await sock.connect("http://x")
    assert timer.cancelled() and probe.cancelled()
    assert isinstance(unanswered.exception(), ConnectionError)
    assert isinstance(waiting.exception(), ConnectionError)
    assert ws._subscription_manager._subscriptions == {}
    ws.ensure_subscribe(acc, 0)                                 # nothing of it can start again
    with pytest.raises(ConnectionError):
        await ws.connect(0, region)
    await asyncio.sleep(0.05)
    assert ws._subscription_manager._subscriptions == {} and _sdk_tasks() == []
    assert b._api is None


def test_d_the_private_sdk_parts_the_clean_up_relies_on_exist() -> None:
    """If a new SDK version renames one of these, _drop_client() silently does less: this test says so."""
    pytest.importorskip("metaapi_cloud_sdk")
    import engineio
    import socketio
    from metaapi_cloud_sdk.clients.metaapi.latency_service import LatencyService
    from metaapi_cloud_sdk.clients.metaapi.metaapi_websocket_client import MetaApiWebsocketClient
    from metaapi_cloud_sdk.clients.metaapi.subscription_manager import SubscriptionManager
    from metaapi_cloud_sdk.clients.metaapi.synchronization_throttler import SynchronizationThrottler
    from metaapi_cloud_sdk.metaapi.metaapi import MetaApi

    ws_init = inspect.getsource(MetaApiWebsocketClient.__init__)
    for name in ("_socket_instances", "_subscription_manager", "_latency_service", "_status_timers"):
        assert f"self.{name} = " in ws_init, name
    connect = inspect.getsource(MetaApiWebsocketClient.connect)
    for key in ("'connected'", "'requestResolves'", "'socket'", "'synchronizationThrottler'"):
        assert key in connect, key
    assert "instance['connected']" in inspect.getsource(MetaApiWebsocketClient._reconnect)
    manager = inspect.getsource(SubscriptionManager)
    for name in ("self._subscriptions = ", "self._awaiting_resubscribe = ", "def cancel_subscribe",
                 "async def schedule_subscribe"):
        assert name in manager, name
    assert "self._wait_connect_promises" in inspect.getsource(LatencyService.__init__)
    assert inspect.iscoroutinefunction(LatencyService.on_connected)
    assert LatencyService.on_connected.__qualname__ == "LatencyService.on_connected"
    for name in ("start", "stop"):
        assert callable(getattr(SynchronizationThrottler, name))
    assert "_metaapi_websocket_client" in inspect.getsource(MetaApi.__init__)
    assert "self.eio = " in inspect.getsource(socketio.AsyncClient.__init__) or hasattr(socketio.AsyncClient, "eio")
    eio = inspect.getsource(engineio.AsyncClient) + inspect.getsource(engineio.Client.__init__)
    for name in ("self.state", "self.queue", "self.ping_loop_event", "self.http", "self.read_loop_task",
                 "self.write_loop_task", "self.ping_loop_task"):
        assert name in eio, name
