"""MetaApi (tickmill) link: a dropped link comes back as fast as is safe.

Production (2026-11-12/13, owner's server log): both MetaApi websockets ended with the engine.io keep-alive abort
("PONG response has not been received") 6, 10.5 and 32 minutes after a start; the SDK's own socket reconnect then
made one attempt and nothing more for hours. The adapter rebuilt the connection itself only after three failed
60 s heartbeat checks: 3-4 minutes without tickmill per drop.

W   link watch: when the SDK itself reports the link down (its sockets ended, or "not synchronized") and does not
    heal it within LINK_CONFIRM_SEC, the connection is rebuilt at once, without waiting for heartbeat checks; an
    ambiguous slow answer keeps the old rule (three failed checks); never two rebuilds at once, never under an
    order request in flight, bounded back-off while MetaApi is really down
N   owner notices: one per outage and one when it is back; a flapping link does not spam
K   engine.io 3 keep-alive: one PONG that is late (or lost) no longer kills both sockets; a dead link is still
    given up one ping interval later at the latest; a stale ping loop of an earlier connection stops

The venue is the World of test_preflight4_metaapi_reconnect.py; its MetaApi objects get a fake websocket client
here (the socket list the SDK keeps) so that "the sockets ended" can be shown without a network.
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from typing import Any

import pytest

from copytrader.brokers import metaapi_broker as mb
from copytrader.brokers.metaapi_broker import MetaApiBroker
from copytrader.config import RiskConfig
from copytrader.models import PositionStatus, Severity

from fakes import FakeNotifier
from test_metaapi_broker import make_cfg, make_plan, make_req
from test_preflight4_metaapi_reconnect import World, heal_done, sent, until


# --------------------------------------------------------------------------- #
# fakes
# --------------------------------------------------------------------------- #
class Sock:
    """One socket.io client as the SDK keeps it (instance['socket']): only what the adapter looks at."""

    def __init__(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False


class SocketWorld(World):
    """World whose MetaApi objects carry the SDK's socket list: {region: {instance number: [instance, ...]}}."""

    def factory(self, token: str, opts: dict[str, Any]) -> Any:
        api = super().factory(token, opts)
        api.socks = [Sock(), Sock()]                # london-a, london-b
        api._metaapi_websocket_client = SimpleNamespace(_socket_instances={"london": {
            0: [{"connected": True, "socket": api.socks[0]}],
            1: [{"connected": True, "socket": api.socks[1]}],
        }})
        return api

    def drop_sockets(self) -> None:
        for sock in self.apis[-1].socks:
            sock.connected = False


@pytest.fixture
def world() -> SocketWorld:
    return SocketWorld()


@pytest.fixture
def fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mb, "HEALTH_TIMEOUT_SEC", 0.05)
    monkeypatch.setattr(mb, "HEALTH_SYNC_GRACE_SEC", 0.02)
    monkeypatch.setattr(mb, "LOST_REPLY_DELAY_SEC", 0.0)
    # the fake MetaApi objects carry a websocket client: connect() must not wrap the real SDK's modules for them
    monkeypatch.setattr(mb, "_pause_socket_retries", lambda: None)
    # raising=False: on a tree without the link watch the tests run (and fail on what they check)
    monkeypatch.setattr(mb, "LINK_WATCH_SEC", 0.01, raising=False)
    monkeypatch.setattr(mb, "LINK_CONFIRM_SEC", 0.05, raising=False)
    monkeypatch.setattr(mb, "LINK_SETTLE_SEC", 0.1, raising=False)


@pytest.fixture
async def broker(world: SocketWorld, fast: None) -> Any:
    b = MetaApiBroker(make_cfg(), api_factory=world.factory)
    b.retry_delay_sec = 0
    b.set_risk_config(RiskConfig(include_fees_in_risk=False))
    b.set_notifier(FakeNotifier())
    await b.connect()
    yield b
    await b.close()


def rebuilds(world: World) -> int:
    return len(world.apis) - 1


async def back(world: World, b: MetaApiBroker, n: int, what: str = "the rebuild") -> None:
    await until(lambda: rebuilds(world) >= n and b._connected and not b._reconnecting, what)
    await heal_done(b)


# --------------------------------------------------------------------------- #
# W  the link watch
# --------------------------------------------------------------------------- #
def test_w_defaults() -> None:
    assert mb.LINK_WATCH_SEC == 2.0 and mb.LINK_CONFIRM_SEC == 20.0 and mb.LINK_SETTLE_SEC == 90.0
    assert mb.LINK_NOTICE_QUIET_SEC == 1800.0 and mb.PING_MISSES_ALLOWED == 1
    from copytrader.engine import health

    # the adapter knows when the heartbeat tells the owner itself (no second notice for the same outage)
    assert mb.HEARTBEAT_TELLS_AFTER == health.HEARTBEAT_ALERT_AFTER


def test_w_policy_link_down_is_an_outage_at_once_with_the_same_backoff() -> None:
    p = mb.ReconnectPolicy(3, (60.0, 120.0), 0.0)
    assert p.link_down(0.0) == ["down", "reconnect"]            # no three checks needed
    p.attempt_started()
    assert p.link_down(1.0) == []                               # one attempt at a time
    assert p.attempt_finished(False, 10.0) == ["reconnect_failed"]
    assert p.link_down(30.0) == []                              # the pause after attempt 1 (60 s) is not over
    assert p.link_down(70.0) == ["reconnect"]
    p.attempt_started()
    p.attempt_finished(False, 80.0)
    assert p.link_down(150.0) == [] and p.link_down(200.0) == ["reconnect"]     # 120 s after attempt 2
    assert p.check(True, 210.0) == ["recovered"] and (p.down, p.attempts, p.retry_at) == (False, 0, None)


async def test_w_sockets_ended_rebuild_without_any_health_check(world: SocketWorld, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    t0 = time.monotonic()
    world.drop_sockets()                                        # both sockets ended (the PONG abort) ...
    world.line.dead = True                                      # ... and nothing is answered any more
    await back(world, broker, 1)
    took = time.monotonic() - t0
    assert rebuilds(world) == 1 and world.apis[0].closed and world.lines[0].closed
    assert took < 1.0                                           # LINK_CONFIRM_SEC + a few ticks, not 3 heartbeats
    assert broker._policy.problems == 0                         # no heartbeat check was needed
    assert await broker.get_equity() == 10_000.0
    assert (await broker.sync_position(pos)).status is PositionStatus.OPEN
    opened = await broker.open_position(make_req(sym, 0.05, make_plan(), tag="ctlink00001"))
    assert opened.status is PositionStatus.OPEN


async def test_w_not_synchronized_on_live_sockets_keeps_the_three_check_rule(world: SocketWorld,
                                                                             broker: MetaApiBroker) -> None:
    """Changed on purpose (verifier problem 3): this test used to expect a fast rebuild here. "Not synchronized"
    while the sockets are up means the terminal lost the broker (MetaApi's 'disconnected' packet): a new MetaApi
    client does not bring the broker back, and tearing a working client down only refuses every call (stop moves
    too) meanwhile. Only ended sockets are rebuilt fast; this case keeps the three-check rule."""
    world.line._meta_api_connection.synchronized = False
    await asyncio.sleep(0.3)                                    # LINK_CONFIRM_SEC (0.05) six times over
    assert rebuilds(world) == 0 and broker._heal_task is None and broker._policy.down is False
    for _ in range(2):
        assert await broker.health_problem() is not None
    assert rebuilds(world) == 0 and broker._heal_task is None
    await broker.health_problem()                               # the third failed check, as before
    await back(world, broker, 1)
    assert rebuilds(world) == 1
    assert await broker.health_problem() is None


async def test_w_a_drop_the_sdk_heals_itself_within_the_confirmation_is_left_alone(world: SocketWorld,
                                                                                  broker: MetaApiBroker) -> None:
    for _ in range(5):
        world.drop_sockets()
        await asyncio.sleep(0.02)                               # the SDK's own reconnect: back in time
        for sock in world.apis[-1].socks:
            sock.connected = True
        await asyncio.sleep(0.04)
    inner = world.line._meta_api_connection
    inner.synchronized = False
    await asyncio.sleep(0.02)
    inner.synchronized = True
    await asyncio.sleep(0.1)
    assert rebuilds(world) == 0 and broker._heal_task is None and broker._policy.down is False


async def test_w_one_socket_of_two_down_is_not_an_outage(world: SocketWorld, broker: MetaApiBroker) -> None:
    world.apis[-1].socks[0].connected = False                   # the other instance still carries the account
    await asyncio.sleep(0.2)
    assert rebuilds(world) == 0
    assert await broker.get_equity() == 10_000.0


async def test_w_an_ambiguous_slow_answer_keeps_the_three_check_rule(world: SocketWorld,
                                                                     broker: MetaApiBroker) -> None:
    world.line.dead = True                                      # the SDK says all is well, requests hang
    await asyncio.sleep(0.2)
    assert rebuilds(world) == 0                                 # the watch alone does nothing
    for _ in range(2):
        assert await broker.health_problem() is not None
    assert rebuilds(world) == 0 and broker._heal_task is None
    await broker.health_problem()                               # the third failed check, as before
    await back(world, broker, 1)
    assert rebuilds(world) == 1


async def test_w_never_under_an_order_request_in_flight(world: SocketWorld, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    line = world.line
    line.order_gate = asyncio.Event()
    opening = asyncio.ensure_future(broker.open_position(make_req(sym, 0.05, make_plan())))
    await until(lambda: line.orders_waiting, "the order request to reach the broker")
    world.drop_sockets()
    await asyncio.sleep(0.3)                                    # confirmed long ago, but an order is in flight
    assert rebuilds(world) == 0 and not broker._reconnecting and not line.closed
    line.order_gate.set()
    pos = await opening
    assert pos.status is PositionStatus.OPEN and pos.error is None
    await back(world, broker, 1, "the rebuild after the order")
    assert (await broker.sync_position(pos)).status is PositionStatus.OPEN


async def test_w_never_two_rebuilds_at_once(world: SocketWorld, broker: MetaApiBroker) -> None:
    gate = asyncio.Event()
    world.prepare = lambda line: setattr(line, "sync_gate", gate)       # the new connection takes its time
    world.drop_sockets()
    await until(lambda: rebuilds(world) == 1 and broker._reconnecting, "the rebuild to start")
    for _ in range(4):                                          # heartbeat checks meanwhile
        assert await broker.health_problem() is not None
    await asyncio.sleep(0.2)                                    # and the watch keeps looking
    assert rebuilds(world) == 1
    gate.set()
    await back(world, broker, 1)
    assert rebuilds(world) == 1 and await broker.get_equity() == 10_000.0


async def test_w_backoff_while_metaapi_is_really_down(world: SocketWorld, broker: MetaApiBroker) -> None:
    broker._policy = mb.ReconnectPolicy(3, (0.15, 0.3), 0.0)
    world.prepare = lambda line: setattr(line, "sync_error", Exception("Timed out waiting for MetaApi"))
    started: list[float] = []
    real = broker._heal

    async def heal(attempt: int) -> None:
        started.append(time.monotonic())
        await real(attempt)

    broker._heal = heal  # type: ignore[method-assign]
    world.drop_sockets()
    await asyncio.sleep(1.2)
    assert 3 <= len(started) <= 5                               # not one per watch tick (120 ticks)
    gaps = [b - a for a, b in zip(started, started[1:])]
    assert gaps[0] >= 0.15 and all(g >= 0.3 for g in gaps[1:])
    assert (broker._api, broker._conn, broker._connected) == (None, None, False)
    world.prepare = None                                        # MetaApi is back
    await until(lambda: broker._connected, "the next attempt to connect")
    assert await broker.get_equity() == 10_000.0


async def test_w_a_client_whose_sockets_all_ended_is_not_waited_for(world: SocketWorld, broker: MetaApiBroker) -> None:
    """The SDK's close() only schedules an unsubscribe; through ended sockets it can never be sent, so waiting
    CLOSE_WAIT_SEC (10 s) for it made 10 of the 11 s of a rebuild in the real-SDK rig."""
    inner = world.line._meta_api_connection
    inner._opened = True                                        # the SDK's close never finishes
    world.drop_sockets()
    t0 = time.monotonic()
    await back(world, broker, 1)
    assert time.monotonic() - t0 < 1.0 and mb.CLOSE_WAIT_SEC == 10.0


async def test_w_close_stops_the_watch(world: SocketWorld, broker: MetaApiBroker) -> None:
    task = broker._watch_task
    assert task is not None and not task.done()
    await broker.close()
    assert task.done() and broker._watch_task is None
    world.drop_sockets()
    await asyncio.sleep(0.15)
    assert rebuilds(world) == 0


async def test_w_no_watch_for_a_connection_handed_in_from_outside() -> None:
    from test_metaapi_broker import FakeConn

    b = MetaApiBroker(make_cfg(), connection=FakeConn())
    await b.connect()
    assert b._watch_task is None
    await b.close()


async def test_w_reads_give_up_early_while_the_sockets_are_down(world: SocketWorld, broker: MetaApiBroker,
                                                                monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mb, "LINK_CONFIRM_SEC", 60.0, raising=False)     # no rebuild in this test
    monkeypatch.setattr(mb, "DEGRADED_READ_TIMEOUT_SEC", 0.05)
    world.drop_sockets()
    world.line.dead = True
    t0 = time.monotonic()
    with pytest.raises(mb.BrokerError):
        await broker.get_equity()
    assert time.monotonic() - t0 < 0.5                          # not READ_TIMEOUT_SEC (30 s)


# --------------------------------------------------------------------------- #
# N  owner notices
# --------------------------------------------------------------------------- #
async def test_n_a_short_drop_is_told_once_after_it_is_over(world: SocketWorld, broker: MetaApiBroker) -> None:
    notifier = broker._notifier
    world.drop_sockets()
    await back(world, broker, 1)
    # updated with the settle rule (verifier problem 1): the outage ends only once the account answered a request
    # after the rebuild; before that no "✅" is sent
    await asyncio.sleep(0.2)
    assert not notifier.notifications and broker._policy.down is True
    assert await broker.get_equity() == 10_000.0                # an answered request
    await until(lambda: notifier.notifications, "the notice")
    await asyncio.sleep(0.2)
    (text,) = await sent(notifier)
    assert notifier.notifications[0][1] is Severity.INFO
    assert text.startswith("✅ <b>metaapi-demo</b>: MetaApi bağlantısı koptu (websocket kapandı)")
    assert "bot bağlantıyı yeniden kurdu" in text and "kesinti yaklaşık 1 sn sürdü" in text
    # updated wording (verifier problem 2): not "no order could be sent"
    assert "Kesinti sırasında gelen yeni sinyaller bu hesapta açılmadı; diğer hesaplar etkilenmedi." in text
    assert "gönderilemez" not in text
    assert broker._policy.down is False                         # the outage is over


async def test_n_with_the_real_heartbeat_there_is_no_second_notice(world: SocketWorld, broker: MetaApiBroker) -> None:
    from copytrader.engine.health import BrokerHeartbeat

    notifier = broker._notifier
    heartbeat = BrokerHeartbeat(lambda: [broker], notifier)
    world.drop_sockets()
    world.line.dead = True
    await heartbeat.check_once()                                # one failed check during the drop
    await back(world, broker, 1)
    await heartbeat.check_once()                                # healthy: it ends the outage (updated: the
    await until(lambda: notifier.notifications, "the notice")   # settle rule alone needs an answered request)
    await heartbeat.check_once()
    await heartbeat.check_once()
    texts = await sent(notifier)
    assert len(texts) == 1 and "bot bağlantıyı yeniden kurdu" in texts[0]


async def test_n_a_flapping_link_does_not_spam(world: SocketWorld, broker: MetaApiBroker,
                                               monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mb, "LINK_NOTICE_QUIET_SEC", 1.0, raising=False)
    notifier = broker._notifier
    for n in (1, 2, 3):                                         # three drops in a row, each healed by a rebuild
        world.drop_sockets()
        await back(world, broker, n)
        assert await broker.get_equity() == 10_000.0            # updated: the account answers after the rebuild
        await until(lambda: not broker._policy.down, "the outage to be over")
    assert len(await sent(notifier)) == 1                       # only the first one is told ...
    await asyncio.sleep(1.0)
    world.drop_sockets()                                        # ... the next one after the quiet time, with the count
    await back(world, broker, 4)
    assert await broker.get_equity() == 10_000.0
    await until(lambda: len(notifier.notifications) == 2, "the second notice")
    assert "Son bildirimden beri 2 kısa kopma daha oldu; bot hepsini kendisi giderdi." in notifier.texts()[1]


async def test_n_a_failed_rebuild_is_told_and_so_is_the_recovery(world: SocketWorld, broker: MetaApiBroker) -> None:
    """The adapter promised "Hesap yanıt verince ayrıca haber verilir": with the fast path the heartbeat may never
    reach its own alert, so the adapter keeps the promise itself."""
    broker._policy = mb.ReconnectPolicy(3, (0.15,), 0.0)
    notifier = broker._notifier
    world.prepare = lambda line: setattr(line, "sync_error", Exception("Timed out waiting for MetaApi"))
    world.drop_sockets()
    await until(lambda: len(notifier.notifications) == 1, "the failure notice")
    assert "sıfırdan kurulamadı" in notifier.texts()[0]
    world.prepare = None
    await until(lambda: broker._connected and not broker._reconnecting, "the next attempt to connect")
    await asyncio.sleep(0.2)                                    # updated: connected is not "the account answers"
    assert len(notifier.notifications) == 1 and broker._policy.down is True
    assert await broker.get_equity() == 10_000.0                # an answered request
    await until(lambda: len(notifier.notifications) == 2, "the recovery notice")
    text, severity = notifier.notifications[1]
    assert severity is Severity.INFO
    assert text.startswith("✅ <b>metaapi-demo</b>: MetaApi bağlantısı yeniden kuruldu, hesap yanıt veriyor")
    assert "Yeni sinyaller bu hesapta yine açılır." in text
    await asyncio.sleep(0.2)
    assert len(notifier.notifications) == 2


async def test_n_a_rebuilt_link_whose_account_does_not_answer_is_never_declared_back(
        world: SocketWorld, broker: MetaApiBroker) -> None:
    """Verifier problem 1 (real-SDK rig 'deafafter'): the rebuild connects, but the account answers nothing. The
    old settle rule ended the outage after LINK_SETTLE_SEC anyway: a false '✅ ... hesap yanıt veriyor' to the
    owner, and the self-healing back-off and the fail-fast flag reset while the account was still dead."""
    from copytrader.engine.health import BrokerHeartbeat

    notifier = broker._notifier
    heartbeat = BrokerHeartbeat(lambda: [broker], notifier)
    world.drop_sockets()
    await back(world, broker, 1)
    world.line.dead = True                                      # the new link is up, the account does not answer
    for _ in range(3):
        await heartbeat.check_once()                            # the heartbeat every minute: all fail
        await asyncio.sleep(0.15)                               # LINK_SETTLE_SEC (0.1) passes in between
    texts = await sent(notifier)
    assert not any(t.startswith("✅") for t in texts), texts
    assert broker._policy.down is True and broker._policy.attempts == 1 and broker._policy.problems == 3
    assert broker._unhealthy is not None
    # the 3-check rule still applies: the heartbeat tells the owner itself
    assert any("🔌" in t and "metaapi-demo" in t for t in texts), texts
    # and new signals are refused at once meanwhile (the fail-fast flag was not reset)
    sym_err = None
    try:
        await asyncio.wait_for(broker.resolve_symbol("ETH"), 1.0)
    except mb.BrokerError as exc:
        sym_err = str(exc)
    assert sym_err is not None and "bu hesapta yeni işlem açılmadı" in sym_err
    world.line.dead = False                                     # the account answers again
    await heartbeat.check_once()
    texts = await sent(notifier)
    assert [t for t in texts if t.startswith("✅")] == [texts[-1]] and broker._policy.down is False


async def test_n_an_order_waiting_during_the_drop_is_not_called_not_opened(world: SocketWorld,
                                                                         broker: MetaApiBroker) -> None:
    """Verifier problem 2 (rig 'inflight'): an order request was out when the link died; its answer never came,
    so it may have executed. The short-drop notice must not say that nothing was opened on this account."""
    sym = await broker.resolve_symbol("ETH")
    line = world.line
    line.order_gate = asyncio.Event()
    opening = asyncio.ensure_future(broker.open_position(make_req(sym, 0.05, make_plan())))
    await until(lambda: line.orders_waiting, "the order request to reach the broker")
    world.drop_sockets()
    await asyncio.sleep(0.2)                                    # confirmed; the rebuild waits for the order
    line.order_gate.set()
    await opening
    await back(world, broker, 1, "the rebuild after the order")
    assert await broker.get_equity() == 10_000.0
    await until(lambda: broker._notifier.notifications, "the notice")
    (text,) = await sent(broker._notifier)
    assert "bot bağlantıyı yeniden kurdu" in text
    assert "bu hesaba gönderilmiş bir emir yanıt bekliyordu; o emir açılmış olabilir" in text
    assert "yeni sinyaller bu hesapta açılmadı" not in text and "gönderilemez" not in text
    # the next drop with no order out gets the plain sentence again
    broker._short_told_at = float("-inf")
    broker._notifier.notifications.clear()
    world.drop_sockets()
    await back(world, broker, 2)
    assert await broker.get_equity() == 10_000.0
    await until(lambda: broker._notifier.notifications, "the second notice")
    assert "Kesinti sırasında gelen yeni sinyaller bu hesapta açılmadı" in broker._notifier.texts()[0]


async def test_n_not_synchronized_gets_no_notice_crediting_a_rebuild(world: SocketWorld,
                                                                     broker: MetaApiBroker) -> None:
    """Verifier problem 3: the terminal lost the broker. Left to the three-check rule, the heartbeat tells
    "down" and later "✅" itself; the adapter never says that its rebuild fixed it."""
    from copytrader.engine.health import BrokerHeartbeat

    notifier = broker._notifier
    heartbeat = BrokerHeartbeat(lambda: [broker], notifier)
    world.line._meta_api_connection.synchronized = False
    await asyncio.sleep(0.2)
    for _ in range(3):
        await heartbeat.check_once()
    await heal_done(broker)
    await heartbeat.check_once()
    await asyncio.sleep(0.2)
    texts = await sent(notifier)
    assert len(texts) == 2 and texts[1].startswith("✅"), texts
    assert not any("yeniden kurdu" in t or "kesinti yaklaşık" in t for t in texts), texts


async def test_n_a_check_that_spans_a_rebuild_is_not_its_verdict(world: SocketWorld, broker: MetaApiBroker,
                                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    """Found while updating the tests: a heartbeat check that began on the dropped link and ended after the link
    watch rebuilt it got its answer from the OLD connection ('This connection has been closed') and was booked
    as the first check of the NEW one: a false '🔌 ... sıfırdan kuruldu ama hesap hâlâ yanıt vermiyor' to the
    owner, and new signals refused on a link that works."""
    monkeypatch.setattr(mb, "HEALTH_SYNC_GRACE_SEC", 0.5)
    monkeypatch.setattr(mb, "HEALTH_TIMEOUT_SEC", 0.6)
    notifier = broker._notifier
    world.drop_sockets()
    checking = asyncio.ensure_future(broker.health_problem())  # waits for the sockets (the SDK's own reconnect)
    await back(world, broker, 1)                                # ... while the watch rebuilds the link
    problem = await checking
    assert problem is not None and problem.startswith(mb._RECONNECTING_TR.rstrip("."))
    texts = await sent(notifier)
    assert not any("hâlâ yanıt vermiyor" in t for t in texts), texts
    assert broker._unhealthy is None and broker._policy.failed_told is False
    sym = await broker.resolve_symbol("ETH")                    # a new signal is not refused
    assert (await broker.open_position(make_req(sym, 0.05, make_plan()))).status is PositionStatus.OPEN
    assert await broker.health_problem() is None                # the next check ends the outage
    await until(lambda: notifier.notifications, "the notice")
    assert notifier.texts()[0].startswith("✅") and len(notifier.texts()) == 1
    # the order went over the rebuilt link (after the outage the notice names): the plain sentence
    assert "Kesinti sırasında gelen yeni sinyaller bu hesapta açılmadı" in notifier.texts()[0]


async def test_n_the_slow_rule_keeps_the_heartbeats_own_notices(world: SocketWorld, broker: MetaApiBroker) -> None:
    from copytrader.engine.health import BrokerHeartbeat

    notifier = broker._notifier
    heartbeat = BrokerHeartbeat(lambda: [broker], notifier)
    world.line.dead = True                                      # ambiguous: the SDK says all is well
    for _ in range(3):
        await heartbeat.check_once()
    await heal_done(broker)
    await heartbeat.check_once()
    await asyncio.sleep(0.2)
    texts = await sent(notifier)
    assert len(texts) == 2 and "sıfırdan kuruyor (1. deneme)" in texts[0] and texts[1].startswith("✅")


# --------------------------------------------------------------------------- #
# K  engine.io 3 keep-alive
# --------------------------------------------------------------------------- #
class Wire:
    """Stands for the websocket: records PINGs and answers them (or not) as told."""

    def __init__(self, eio: Any, answer: Any) -> None:
        self.eio = eio
        self.answer = answer            # answer(n) -> True: the PONG of the n-th PING arrives
        self.pings = 0
        self.closed = False

    async def send_packet(self, pkt: Any) -> None:
        self.pings += 1
        if self.answer(self.pings):
            asyncio.get_running_loop().call_later(0.002, setattr, self.eio, "pong_received", True)

    async def close(self) -> None:
        self.closed = True


def engineio_client(answer: Any, interval: float = 0.05) -> tuple[Any, Wire]:
    import engineio

    eio = engineio.AsyncClient()
    assert mb._patient_keepalive(eio) is True
    wire = Wire(eio, answer)
    eio._send_packet = wire.send_packet
    eio.ws = wire
    eio.queue = asyncio.Queue()
    eio.ping_interval = interval
    eio.state = "connected"
    return eio, wire


async def test_k_one_late_pong_does_not_abort_the_socket() -> None:
    eio, wire = engineio_client(lambda n: n != 3)               # the 3rd PING is never answered
    eio.ping_loop_task = asyncio.ensure_future(eio._ping_loop())
    await asyncio.sleep(0.42)
    assert not eio.ping_loop_task.done() and not wire.closed and eio.queue.empty() and wire.pings >= 7
    eio.state = "disconnected"
    eio.ping_loop_event.set()
    await asyncio.wait_for(eio.ping_loop_task, 1)


async def test_k_a_dead_link_is_given_up_one_interval_later_at_the_latest() -> None:
    eio, wire = engineio_client(lambda n: n <= 2)               # the line dies after the 2nd PONG
    t0 = time.monotonic()
    eio.ping_loop_task = asyncio.ensure_future(eio._ping_loop())
    await asyncio.wait_for(eio.ping_loop_task, 1)
    took = time.monotonic() - t0
    # PINGs at 0, 1, 2, 3 intervals; the 3rd went unanswered (tolerated at 3), the 4th too: abort at 4 intervals
    # (the library alone: at 3)
    assert wire.closed and eio.queue.get_nowait() is None and wire.pings == 4
    assert 0.19 <= took < 0.3


async def test_k_a_stale_ping_loop_of_an_earlier_connection_stops() -> None:
    """python-engineio 3.x: a disconnect(abort=True) leaves the ping loop running; when the same client connects
    again a second loop starts and both share pong_received: spurious "PONG response has not been received"."""
    eio, wire = engineio_client(lambda n: True)
    old = asyncio.ensure_future(eio._ping_loop())
    eio.ping_loop_task = old
    await asyncio.sleep(0.01)
    new = asyncio.ensure_future(eio._ping_loop())               # the next connection's loop
    eio.ping_loop_task = new
    await asyncio.wait_for(old, 1)                              # the old one ends at its next tick
    assert not new.done() and not wire.closed
    eio.state = "disconnected"
    eio.ping_loop_event.set()
    await asyncio.wait_for(new, 1)


def test_k_the_sdk_socket_clients_get_the_patient_keepalive() -> None:
    import socketio

    client = mb._pausing_socketio(socketio).AsyncClient(reconnection=False)
    assert getattr(client.eio._ping_loop, "copytrader_patient", False) is True
    assert mb._patient_keepalive(SimpleNamespace()) is False    # anything else is left alone


async def test_k_a_socket_the_peer_closed_is_not_reported_as_a_missing_pong() -> None:
    """python-engineio 3.14.2: when the read loop ends (the server closed the socket: 'Unexpected error decoding
    packet' / 'Read loop: WebSocket connection was closed') it sets ping_loop_event; the library's ping loop then
    goes round once more with a PING out and logs "PONG response has not been received, aborting" for a socket
    that was in fact closed by the peer (shown with the real SDK in the link rig). The patient loop just ends."""
    import logging

    eio, wire = engineio_client(lambda n: True, interval=5.0)
    seen: list[str] = []

    class Catch(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            seen.append(record.getMessage())

    handler = Catch()
    eio.logger.addHandler(handler)
    mb.logger.addHandler(handler)
    try:
        eio.ping_loop_task = asyncio.ensure_future(eio._ping_loop())
        await asyncio.sleep(0.01)
        eio.ping_loop_event.set()                               # the read loop ends: "Waiting for ping loop task"
        await asyncio.wait_for(eio.ping_loop_task, 1)
    finally:
        eio.logger.removeHandler(handler)
        mb.logger.removeHandler(handler)
    assert not any("PONG response" in m or "keep-alive" in m for m in seen), seen
    assert not wire.closed and eio.queue.empty()                # the read loop does its own clean-up
