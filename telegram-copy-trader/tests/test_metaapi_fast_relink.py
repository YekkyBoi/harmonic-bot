"""MetaApi (tickmill) link: rebuilt the moment the SDK's sockets end, and the rebuild itself without idle waits.

Production (2026-11-13, owner's server log): every 10-15 minutes MetaApi closed both websockets from its side
('Unexpected error receiving packet: "WebSocket read returned None", aborting' -> 'Engine.IO connection dropped');
the SDK's own socket reconnect never brought them back, and the link watch rebuilt the connection only after
LINK_CONFIRM_SEC = 20 s: 23-25 s without tickmill per drop (once 86 s). The owner wants it back at once.

D   detection: the drop is seen when engine.io reports it, although socket.io still calls the socket connected
    while the SDK's own reconnect handler runs; the rebuild starts at once (production defaults)
P   put off under an order request in flight, then started the moment that request ends
S   the rebuild: no one-second polling for "synchronized", no account REST calls that only repeat the one just
    made, no second request for MetaApi's server names
G   guards: one socket of two is not an outage, a socket the SDK brought back is up again, many drop events are no
    storm (the back-off holds)

Real-SDK rig (scratchpad fastrelink/frig.py): abort -> first answered request 24.0 s -> 0.1 s (no latency) and
24.7 s -> 0.7 s (40 ms round trips, 200 ms per HTTPS call, 250 ms per socket connect).
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
from copytrader.models import PositionStatus

from fakes import FakeNotifier
from test_metaapi_broker import FakeConn, make_cfg, make_plan, make_req
from test_metaapi_link import SocketWorld, back, broker, fast, rebuilds, world  # noqa: F401 - fixtures
from test_preflight4_metaapi_reconnect import heal_done, sent, until


async def within(cond: Any, limit: float, what: str) -> float:
    """Seconds until ``cond()`` is true; fails after ``limit``."""
    t0 = time.monotonic()
    while not cond():
        assert time.monotonic() - t0 < limit, f"timed out after {limit} s waiting for: {what}"
        await asyncio.sleep(0.002)
    return time.monotonic() - t0


# --------------------------------------------------------------------------- #
# D  detection
# --------------------------------------------------------------------------- #
def test_d_production_defaults() -> None:
    assert mb.LINK_CONFIRM_SEC == 0.0 and mb.LINK_WATCH_SEC == 0.5 and mb.SYNC_POLL_SEC == 0.05
    # unchanged guards
    assert mb.LINK_SETTLE_SEC == 90.0 and mb.LINK_NOTICE_QUIET_SEC == 1800.0
    assert mb.RECONNECT_BACKOFF_SEC == (60.0, 120.0, 300.0, 600.0)


@pytest.fixture
async def prod_broker(world: SocketWorld, monkeypatch: pytest.MonkeyPatch) -> Any:
    """As ``broker``, but the link watch runs with its production timing (LINK_WATCH_SEC, LINK_CONFIRM_SEC)."""
    monkeypatch.setattr(mb, "HEALTH_TIMEOUT_SEC", 0.05)
    monkeypatch.setattr(mb, "HEALTH_SYNC_GRACE_SEC", 0.02)
    monkeypatch.setattr(mb, "LOST_REPLY_DELAY_SEC", 0.0)
    monkeypatch.setattr(mb, "_pause_socket_retries", lambda: None)
    monkeypatch.setattr(mb, "LINK_SETTLE_SEC", 0.1, raising=False)
    b = MetaApiBroker(make_cfg(), api_factory=world.factory)
    b.retry_delay_sec = 0
    b.set_risk_config(RiskConfig(include_fees_in_risk=False))
    b.set_notifier(FakeNotifier())
    await b.connect()
    yield b
    await b.close()


async def test_d_with_production_timing_a_drop_is_rebuilt_at_once(world: SocketWorld,
                                                                  prod_broker: MetaApiBroker) -> None:
    """Before: LINK_CONFIRM_SEC = 20 s and a look every 2 s (production: 23-25 s per drop). Now the first look
    that sees every socket ended rebuilds; with no drop event at all (this fake socket sends none) that look comes
    within LINK_WATCH_SEC."""
    b = prod_broker
    world.drop_sockets()
    world.line.dead = True
    took = await within(lambda: rebuilds(world) >= 1 and b._connected and not b._reconnecting, 3.0, "the rebuild")
    await heal_done(b)
    assert took < mb.LINK_WATCH_SEC + 0.5, took
    assert b._policy.problems == 0                              # no heartbeat check was needed
    assert await b.get_equity() == 10_000.0                     # the account answers: the outage is over
    await until(lambda: b._notifier.notifications, "the notice")
    (text,) = await sent(b._notifier)
    assert "bot bağlantıyı yeniden kurdu, kesinti yaklaşık 1 sn sürdü" in text    # one notice, the true length


class DropWorld(SocketWorld):
    """SocketWorld whose sockets are the SDK's socket.io client class exactly as the adapter wraps it
    (mb._pausing_socketio): no network, only what engine.io and socket.io do when a connection ends."""

    def factory(self, token: str, opts: dict[str, Any]) -> Any:
        import socketio

        api = super().factory(token, opts)
        cls = mb._pausing_socketio(socketio).AsyncClient
        api.socks = []
        for number in (0, 1):
            sock = cls(reconnection=False)
            sock.connected = True                           # as after its connect()
            api.socks.append(sock)
            api._metaapi_websocket_client._socket_instances["london"][number] = [{"connected": True,
                                                                                  "socket": sock}]
        return api


@pytest.fixture
def drop_world() -> DropWorld:
    return DropWorld()


@pytest.fixture
async def drop_broker(drop_world: DropWorld, fast: None) -> Any:  # noqa: F811 - the fixture of test_metaapi_link
    b = MetaApiBroker(make_cfg(), api_factory=drop_world.factory)
    b.retry_delay_sec = 0
    b.set_risk_config(RiskConfig(include_fees_in_risk=False))
    b.set_notifier(FakeNotifier())
    await b.connect()
    yield b
    await b.close()


def test_d_the_socketio_parts_the_drop_mark_relies_on_exist() -> None:
    """If a new python-socketio / SDK version changes these, the drop is seen only by the LINK_WATCH_SEC look (and
    later: socket.connected) again: this test says so."""
    import inspect

    import socketio
    from metaapi_cloud_sdk.clients.metaapi.metaapi_websocket_client import MetaApiWebsocketClient

    client = mb._pausing_socketio(socketio).AsyncClient(reconnection=False)
    handler = client.eio.handlers["disconnect"]                 # what engine.io calls when its connection ends
    assert handler.__func__ is type(client)._handle_eio_disconnect and handler.__self__ is client
    assert inspect.iscoroutinefunction(socketio.AsyncClient._handle_eio_disconnect)
    # the SDK's 'disconnect' handler runs its socket reconnect loop, which disconnects only after a sleep
    connect = inspect.getsource(MetaApiWebsocketClient.connect)
    assert "@socket_instance.on('disconnect')" in connect and "await self._reconnect(" in connect
    reconnect = inspect.getsource(MetaApiWebsocketClient._reconnect)
    assert reconnect.index("await asyncio.sleep(") < reconnect.index("await instance['socket'].disconnect()")


def engineio_drop(sock: Any, sdk_handler: Any) -> asyncio.Task[None]:
    """What engine.io's read loop does when the server closed the socket: it calls socket.io's
    _handle_eio_disconnect, which runs the SDK's 'disconnect' handler (its socket reconnect loop) inside."""
    sock.on("disconnect", sdk_handler)
    return asyncio.ensure_future(sock._handle_eio_disconnect())


async def test_d_the_drop_is_seen_while_the_sdk_handler_still_holds_the_socket(
        drop_world: DropWorld, drop_broker: MetaApiBroker, monkeypatch: pytest.MonkeyPatch) -> None:
    """Shown in the real-SDK rig: socket.io keeps socket.connected True until the SDK's handler returns, and that
    handler is the SDK's reconnect loop (it sleeps a second, then disconnects; in production it never brought the
    sockets back). Before: the drop was seen only when the handler got that far, on the next 2 s look. Now the
    socket says so when engine.io ends it, and the watch looks at once (no polling here: one look a minute)."""
    b = drop_broker
    monkeypatch.setattr(mb, "LINK_WATCH_SEC", 60.0)
    monkeypatch.setattr(mb, "LINK_CONFIRM_SEC", 0.0)
    await asyncio.sleep(0.05)                                   # the watch now waits a minute between looks
    socks = drop_world.apis[-1].socks
    held = asyncio.Event()

    async def sdk_reconnect_loop() -> None:                     # never gets anywhere (production)
        await held.wait()

    first = engineio_drop(socks[0], sdk_reconnect_loop)
    await asyncio.sleep(0.05)
    assert socks[0].connected is True                           # socket.io still says so
    assert b._sockets_down() is False and rebuilds(drop_world) == 0     # one of two: not an outage
    t0 = time.monotonic()
    second = engineio_drop(socks[1], sdk_reconnect_loop)
    await within(lambda: rebuilds(drop_world) >= 1, 1.0, "the rebuild")
    assert time.monotonic() - t0 < 0.3
    await back(drop_world, b, 1)
    assert await b.get_equity() == 10_000.0
    held.set()
    await asyncio.wait({first, second}, timeout=2)


# --------------------------------------------------------------------------- #
# P  put off, then at once
# --------------------------------------------------------------------------- #
async def test_p_a_rebuild_put_off_by_an_order_starts_the_moment_the_order_is_done(
        world: SocketWorld, broker: MetaApiBroker, monkeypatch: pytest.MonkeyPatch) -> None:
    """Before: the watch asked again on its next look (2 s later in production). Now the end of the request (and of
    its flow) wakes it."""
    monkeypatch.setattr(mb, "LINK_WATCH_SEC", 60.0)
    monkeypatch.setattr(mb, "LINK_CONFIRM_SEC", 0.0)
    await asyncio.sleep(0.05)
    sym = await broker.resolve_symbol("ETH")
    line = world.line
    line.order_gate = asyncio.Event()
    opening = asyncio.ensure_future(broker.open_position(make_req(sym, 0.05, make_plan())))
    await until(lambda: line.orders_waiting, "the order request to reach the broker")
    world.drop_sockets()
    loop = asyncio.get_running_loop()
    broker._look_at_link(loop.time())                          # the watch sees the drop ...
    broker._look_at_link(loop.time())
    assert rebuilds(world) == 0 and not broker._reconnecting and broker._put_off_told   # ... and puts it off
    line.order_gate.set()
    pos = await opening
    assert pos.status is PositionStatus.OPEN and pos.error is None
    await within(lambda: rebuilds(world) >= 1, 0.5, "the rebuild right after the order")
    await back(world, broker, 1)
    assert (await broker.sync_position(pos)).status is PositionStatus.OPEN


# --------------------------------------------------------------------------- #
# S  the rebuild itself
# --------------------------------------------------------------------------- #
async def test_s_synchronized_is_not_waited_for_in_whole_seconds(world: SocketWorld, broker: MetaApiBroker) -> None:
    """SDK 29.x RpcMetaApiConnection.wait_synchronized looks at the flag once a second: in the real-SDK rig the
    subscription was answered 0.01 s after the socket connected and the rebuild still took 1.00 s."""
    def prepare(line: Any) -> None:
        inner = line._meta_api_connection
        inner.synchronized = False
        asyncio.get_running_loop().call_later(0.03, setattr, inner, "synchronized", True)     # MetaApi answers

        async def sdk_wait_synchronized(timeout_in_seconds: float = 300) -> None:
            line.seen.append("wait_synchronized")
            while not inner.is_synchronized():                  # rpc_metaapi_connection.py: sleep(1) steps
                await asyncio.sleep(1)
        line.wait_synchronized = sdk_wait_synchronized

    world.prepare = prepare
    t0 = time.monotonic()
    await broker.reconnect()
    assert time.monotonic() - t0 < 0.4
    assert world.line.seen[:3] == ["connect", "wait_synchronized", "get_account_information"]   # the SDK call too


async def test_s_the_request_after_synchronized_does_not_wait_for_the_latency_probe() -> None:
    """Real-SDK rig (40 ms / 100 ms round trips, 1 drop in 4): the SDK handles MetaApi's 'authenticated' packet by
    starting LatencyService.on_connected as a task (its first step marks the instance connected, then it measures
    the region with one more socket) and then setting "synchronized". Seen in that same turn of the event loop,
    the SDK's waitSynchronized request found no connected instance and waited for the whole measurement (+0.6 s)."""
    inner = SimpleNamespace(synchronized=False)
    inner.is_synchronized = lambda: inner.synchronized
    marked = {"connected": False}

    async def latency_on_connected() -> None:
        marked["connected"] = True                              # _connected_instances_cache[instance] = True
        await asyncio.sleep(0.6)                                # _refresh_latency(region): one more socket

    seen: list[bool] = []

    async def wait_synchronized(timeout_in_seconds: float = 300) -> None:
        seen.append(marked["connected"])                        # rpc_request: a connected instance, or it waits

    conn = SimpleNamespace(_meta_api_connection=inner, wait_synchronized=wait_synchronized)
    asyncio.ensure_future(latency_on_connected())               # the 'authenticated' packet, handled in this turn
    inner.synchronized = True
    await mb._wait_synchronized(conn)
    assert seen == [True]


async def test_s_a_terminal_that_never_synchronizes_still_ends_the_attempt(world: SocketWorld,
                                                                           broker: MetaApiBroker) -> None:
    """Guard: the faster look changes nothing for a terminal that never synchronizes: the caller's limit
    (RECONNECT_TIMEOUT_SEC, the engine's connect limit) ends the attempt and nothing of it is left."""
    world.prepare = lambda line: setattr(line._meta_api_connection, "synchronized", False)
    t0 = time.monotonic()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(broker.reconnect(), 0.3)
    assert time.monotonic() - t0 < 0.6
    assert not broker._connected and not broker._reconnecting and broker._api is None and broker._conn is None


def _real_account_api(states: list[str], rest: list[str]) -> Any:
    """The SDK's own MetatraderAccountApi of a real MetaApi object, its REST client answered locally:
    connectionStatus of the successive reads in ``states``; every REST call is logged in ``rest``."""
    from metaapi_cloud_sdk import MetaApi

    api = MetaApi("token", {})
    dto = {"_id": "acc-1", "state": "DEPLOYED", "magic": 0, "region": "london", "reliability": "regular",
           "type": "cloud-g2", "name": "tickmill", "login": "123456", "server": "Tickmill-Live", "version": 5,
           "platform": "mt5", "application": "MetaApi", "accountReplicas": []}

    async def get_account(_id: str) -> dict[str, Any]:
        rest.append("get_account")
        status = states[min(len(rest), len(states)) - 1]
        return {**dto, "connectionStatus": status}

    api.metatrader_account_api._metatrader_account_client.get_account = get_account
    return api


@pytest.mark.parametrize("states, calls", [
    (["CONNECTED"], 1),                         # the account just read says it all: one REST call (before: 3)
    (["DISCONNECTED", "DISCONNECTED", "CONNECTED"], 3),   # not connected yet: wait_connected() reads until it is
])
async def test_s_the_account_is_read_once_when_that_answer_already_says_it_is_ready(
        monkeypatch: pytest.MonkeyPatch, states: list[str], calls: int) -> None:
    """SDK 29.x: wait_deployed() and wait_connected() each read the account again first (a new HTTPS connection
    per call) although get_account() has just read it. Real SDK account objects here; only the REST answers and
    the RPC connection are stand-ins."""
    pytest.importorskip("metaapi_cloud_sdk")
    from metaapi_cloud_sdk.metaapi.metatrader_account import MetatraderAccount

    monkeypatch.setattr(mb, "_pause_socket_retries", lambda: None)
    rest: list[str] = []
    conn = FakeConn()
    monkeypatch.setattr(MetatraderAccount, "get_rpc_connection", lambda self: conn)
    monkeypatch.setattr(MetatraderAccount, "_delay", lambda self, ms: asyncio.sleep(0), raising=False)
    api = _real_account_api(states, rest)
    b = MetaApiBroker(make_cfg(), api_factory=lambda token, opts: api)
    try:
        await asyncio.wait_for(b.connect(), 5)
        assert b._connected and len(rest) == calls, rest
    finally:
        await b.close()


def test_s_broker_connected_reads_what_wait_connected_waits_for() -> None:
    pytest.importorskip("metaapi_cloud_sdk")
    import inspect

    from metaapi_cloud_sdk.metaapi.metatrader_account import MetatraderAccount

    source = inspect.getsource(MetatraderAccount.wait_connected)
    assert "'CONNECTED' in [self.connection_status]" in source and "self.replicas" in source
    assert mb._broker_connected(SimpleNamespace(connection_status="DISCONNECTED", replicas=[
        SimpleNamespace(connection_status="CONNECTED")])) is True
    assert mb._broker_connected(SimpleNamespace(connection_status="DISCONNECTED_FROM_BROKER", replicas=[])) is False
    assert mb._broker_connected(SimpleNamespace(state="DEPLOYED")) is False      # cannot tell: SDK call as before


async def test_s_a_new_client_reuses_the_server_names_the_dropped_one_knew(monkeypatch: pytest.MonkeyPatch) -> None:
    """Real SDK objects: a new MetaApi object asks MetaApi for its server names (DomainClient: a REST call over a
    new HTTPS connection) before its first socket can connect. After a drop it gets what the dropped client
    read, with its time: the SDK's own 10-minute rule still decides when to read them again."""
    pytest.importorskip("metaapi_cloud_sdk")
    from metaapi_cloud_sdk import MetaApi

    b = MetaApiBroker(make_cfg())
    old, new, stale = MetaApi("token", {}), MetaApi("token", {}), MetaApi("token", {})
    asked: list[str] = []

    async def request(opts: dict[str, Any], type: str = "", *_a: Any, **_k: Any) -> Any:
        asked.append(type)
        return {"domain": "agiliumtrade.ai", "hostname": "mt-client-api-v1"}

    try:
        for api in (old, new, stale):
            monkeypatch.setattr(api._metaapi_websocket_client._domain_client._http_client, "request", request)
        assert (await old._metaapi_websocket_client.get_url_settings(0, "london"))["url"] == \
            "https://mt-client-api-v1.london-a.agiliumtrade.ai"
        assert asked == ["_update_domain"]                      # the first client asks once
        b._remember_domain(old)
        b._reuse_domain(new)
        assert (await new._metaapi_websocket_client.get_url_settings(1, "london"))["url"] == \
            "https://mt-client-api-v1.london-b.agiliumtrade.ai"
        assert asked == ["_update_domain"]                      # ... the new one does not ask again
        b._domain_cache = {**b._domain_cache, "lastUpdated": time.time() - 11 * 60}
        b._reuse_domain(stale)
        await stale._metaapi_websocket_client.get_url_settings(0, "london")
        assert asked == ["_update_domain", "_update_domain"]    # older than 10 minutes: read again (the SDK's rule)
    finally:
        for api in (old, new, stale):
            b._api = api
            await b._teardown()


async def test_s_the_rebuild_hands_the_server_names_over(world: SocketWorld, broker: MetaApiBroker) -> None:
    """The wiring: _teardown() keeps what the dropped client knew, connect() gives it to the new one."""
    first = world.apis[-1]
    first._metaapi_websocket_client._domain_client = SimpleNamespace(_url_cache={
        "domain": "agiliumtrade.ai", "hostname": "mt-client-api-v1", "requestPromise": None,
        "lastUpdated": time.time()})
    real = world.factory

    def factory(token: str, opts: dict[str, Any]) -> Any:
        api = real(token, opts)
        api._metaapi_websocket_client._domain_client = SimpleNamespace(_url_cache={
            "domain": None, "hostname": None, "requestPromise": None, "lastUpdated": 0})
        return api

    broker._api_factory = factory
    world.drop_sockets()
    await back(world, broker, 1)
    cache = world.apis[-1]._metaapi_websocket_client._domain_client._url_cache
    assert cache["domain"] == "agiliumtrade.ai" and cache["hostname"] == "mt-client-api-v1"
    assert cache["requestPromise"] is None


# --------------------------------------------------------------------------- #
# G  guards
# --------------------------------------------------------------------------- #
async def test_g_a_socket_the_sdk_brought_back_is_up_again(drop_world: DropWorld, drop_broker: MetaApiBroker,
                                                           monkeypatch: pytest.MonkeyPatch) -> None:
    """A healthy link is never rebuilt: socket A ends and the SDK's own reconnect brings it back; later socket B
    ends alone. Neither moment has every socket ended."""
    import socketio

    async def connects(self: Any, *_a: Any, **_k: Any) -> None:
        self.connected = True

    monkeypatch.setattr(socketio.AsyncClient, "connect", connects)
    monkeypatch.setattr(mb, "LINK_CONFIRM_SEC", 0.0)
    b = drop_broker
    socks = drop_world.apis[-1].socks

    held = asyncio.Event()

    async def sdk_reconnect_works() -> None:                    # metaapi_websocket_client.py _reconnect: it
        await asyncio.sleep(0.02)                               # disconnects, connects again and then waits on
        await socks[0].disconnect()                             # the new connection (socket.wait()) inside the
        await socks[0].connect("http://127.0.0.1:1")            # 'disconnect' handler
        await held.wait()

    async def held_handler() -> None:                           # this one never gets anywhere
        await held.wait()

    first = engineio_drop(socks[0], sdk_reconnect_works)
    await asyncio.sleep(0.1)
    assert socks[0].connected is True and socks[0].copytrader_dropped is False and b._sockets_down() is False
    other = engineio_drop(socks[1], held_handler)
    await asyncio.sleep(0.2)
    assert socks[1].copytrader_dropped is True and b._sockets_down() is False
    assert rebuilds(drop_world) == 0 and b._heal_task is None and b._policy.down is False
    held.set()
    await asyncio.wait({first, other}, timeout=2)


async def test_g_a_failed_socket_connect_keeps_it_ended(monkeypatch: pytest.MonkeyPatch) -> None:
    import socketio

    async def refused(self: Any, *_a: Any, **_k: Any) -> None:
        raise socketio.exceptions.ConnectionError("Connection refused by the server")

    monkeypatch.setattr(socketio.AsyncClient, "connect", refused)
    monkeypatch.setattr(mb, "SOCKET_RETRY_PAUSE_SEC", 0.0)
    sock = mb._pausing_socketio(socketio).AsyncClient(reconnection=False)
    assert sock.copytrader_dropped is False
    await sock._handle_eio_disconnect()
    assert sock.copytrader_dropped is True
    with pytest.raises(socketio.exceptions.ConnectionError):
        await sock.connect("http://127.0.0.1:1")
    assert sock.copytrader_dropped is True


async def test_g_drop_events_are_no_storm_while_metaapi_is_down(world: SocketWorld, broker: MetaApiBroker,
                                                               monkeypatch: pytest.MonkeyPatch) -> None:
    """Every drop event wakes the watch; while MetaApi stays down the attempts still follow the back-off."""
    monkeypatch.setattr(mb, "LINK_CONFIRM_SEC", 0.0)
    broker._policy = mb.ReconnectPolicy(3, (0.15, 0.3), 0.0)
    world.prepare = lambda line: setattr(line, "sync_error", Exception("Timed out waiting for MetaApi"))
    started: list[float] = []
    real = broker._heal

    async def heal(attempt: int) -> None:
        started.append(time.monotonic())
        await real(attempt)

    broker._heal = heal  # type: ignore[method-assign]
    world.drop_sockets()
    t0 = time.monotonic()
    while time.monotonic() - t0 < 1.2:
        mb._socket_dropped()                                    # a socket ends every 5 ms
        await asyncio.sleep(0.005)
    assert 3 <= len(started) <= 5, started
    gaps = [later - earlier for earlier, later in zip(started, started[1:])]
    assert gaps[0] >= 0.15 and all(g >= 0.3 for g in gaps[1:])
    world.prepare = None
    await until(lambda: broker._connected, "the next attempt to connect")
    assert await broker.get_equity() == 10_000.0


async def test_g_close_stops_listening_for_drop_events(world: SocketWorld, broker: MetaApiBroker) -> None:
    assert broker in mb._LINK_WATCHERS
    await broker.close()
    assert broker not in mb._LINK_WATCHERS
    mb._socket_dropped()                                        # nothing to wake, nothing breaks
    world.drop_sockets()
    await asyncio.sleep(0.1)
    assert rebuilds(world) == 0
