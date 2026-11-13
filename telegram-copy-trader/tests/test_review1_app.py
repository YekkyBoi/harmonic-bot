"""Review 1 of the live release 62e0719, lane "app" (copytrader/app.py).

P1  diagnostics for the MetaApi websocket drops (cause unknown: our process stalling, the network or MetaApi):
    (a) serve() runs an event-loop watchdog: "Olay döngüsü <n> sn gecikti" when the loop was held > 5 s;
    (b) the engine.io lines that report the LOSS of an established websocket ("PONG response has not been
        received", "Exiting ping task", ...) pass the ConnectFailureFilter; the packet lines stay out.
P2  (YONETIM-7) the first live fx signal (test_incident_day_signals.py) on the live config replica as well
    (the scenarios a-g run on both replicas in test_preflight_scenarios.py).
P3  Windows Ctrl+C: asyncio.run() cancels the main task, so serve()'s shutdown used to run with
    Task.cancelling() == 1 and a broker SDK's stray CancelledError (stray_cancel) passed for a real one.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

import copytrader.app as app_mod
from copytrader.models import Severity
from copytrader.telegram import formatting as fmt

from fakes import FakeBroker, FakeNotifier
from test_app_cli import _app
from test_incident_day_signals import (
    test_todays_eurusd_short_opens_on_both_accounts_with_the_traders_own_lot as incident_day_eurusd_short,
)
from test_preflight_scenarios import REPLICAS, Prod

REPO = str(Path(__file__).resolve().parents[1])
JWT = "eyJhbGciOiJSUzUxMiJ9.eyJfaWQiOiJhYmMxMjMifQ.c2lnbmF0dXJlLXh5el8xMjM0NTY"
WS_URL = f"https://mt-client-api-v1.london-a.agiliumtrade.ai/ws/?auth-token={JWT}&clientId=0.4711&protocol=3"
STALL = re.compile(r"Olay döngüsü (\d+\.\d) sn gecikti")


def stall_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records
            if r.name == "copytrader.app" and r.levelno == logging.WARNING and "Olay döngüsü" in r.getMessage()]


# =========================================================================== #
# P1 (a): the event-loop watchdog
# =========================================================================== #
async def test_p1a_a_loop_blocked_for_6_s_gives_exactly_one_warning_with_the_delay(caplog) -> None:
    """The production settings (1 s tick, warn above 5 s, at most one line a minute)."""
    caplog.set_level(logging.WARNING, logger="copytrader.app")
    watch = asyncio.ensure_future(app_mod.watch_event_loop())
    try:
        await asyncio.sleep(0.5)            # the watchdog is half-way through its 1 s sleep
        time.sleep(6.0)                     # something holds the event loop for 6 s
        await asyncio.sleep(2.5)            # it wakes up ~5.5 s late, then ticks on time twice
    finally:
        watch.cancel()
        await asyncio.gather(watch, return_exceptions=True)
    lines = stall_lines(caplog)
    assert len(lines) == 1, lines
    match = STALL.fullmatch(lines[0])
    assert match is not None, lines[0]
    assert 5.0 < float(match.group(1)) < 6.5, lines[0]          # the measured delay


async def test_p1a_a_short_hold_is_not_reported() -> None:
    logged: list[str] = []
    handler = logging.Handler()
    handler.emit = lambda record: logged.append(record.getMessage())       # type: ignore[method-assign]
    logging.getLogger("copytrader.app").addHandler(handler)
    watch = asyncio.ensure_future(app_mod.watch_event_loop(tick_sec=0.05, warn_after_sec=0.5, every_sec=60.0))
    try:
        await asyncio.sleep(0.1)
        time.sleep(0.2)                     # below the threshold
        await asyncio.sleep(0.2)
    finally:
        watch.cancel()
        await asyncio.gather(watch, return_exceptions=True)
        logging.getLogger("copytrader.app").removeHandler(handler)
    assert [m for m in logged if "Olay döngüsü" in m] == []


async def test_p1a_at_most_one_line_per_window_and_the_stalls_in_between_are_counted(caplog) -> None:
    """Scaled down: 0.05 s tick, warn above 0.25 s, one line per 2 s (production: 1 s, 5 s, 60 s)."""
    caplog.set_level(logging.WARNING, logger="copytrader.app")
    watch = asyncio.ensure_future(app_mod.watch_event_loop(tick_sec=0.05, warn_after_sec=0.25, every_sec=2.0))
    try:
        await asyncio.sleep(0.1)
        time.sleep(0.4)                     # stall 1: reported at once
        await asyncio.sleep(0.05)
        time.sleep(0.4)                     # stalls 2 and 3: inside the window, only counted
        await asyncio.sleep(0.05)
        time.sleep(0.4)
        await asyncio.sleep(0.05)
        first = stall_lines(caplog)
        assert len(first) == 1 and STALL.fullmatch(first[0]), first
        await asyncio.sleep(2.1)            # the window is over: ONE line with the count and the longest delay
        lines = stall_lines(caplog)
        assert len(lines) == 2, lines
        summary = re.fullmatch(r"Olay döngüsü son uyarıdan beri (\d+) kez daha 0\.25 sn'den uzun gecikti "
                               r"\(en uzunu (\d+\.\d) sn\)", lines[1])
        assert summary is not None, lines[1]
        assert int(summary.group(1)) >= 2 and float(summary.group(2)) >= 0.3, lines[1]
        time.sleep(0.4)                     # a stall right after that line: counted again, not logged
        await asyncio.sleep(0.1)
        assert len(stall_lines(caplog)) == 2
    finally:
        watch.cancel()
        await asyncio.gather(watch, return_exceptions=True)


async def test_p1a_serve_runs_the_watchdog_while_the_bot_runs_and_stops_it(tmp_path, monkeypatch, caplog) -> None:
    # raising=False: on a version without the watchdog this test fails on the missing warning, not on setattr
    monkeypatch.setattr(app_mod, "LOOP_WATCH_TICK_SEC", 0.05, raising=False)
    monkeypatch.setattr(app_mod, "LOOP_STALL_WARN_SEC", 0.3, raising=False)
    caplog.set_level(logging.WARNING, logger="copytrader.app")
    app, _brokers = _app(tmp_path, FakeNotifier())
    stop = asyncio.Event()
    task = asyncio.ensure_future(app_mod.serve(app, stop))
    await asyncio.wait_for(app.listener.started.wait(), 2)
    await asyncio.sleep(0.1)
    time.sleep(0.6)                         # the loop is held while the bot runs
    await asyncio.sleep(0.2)
    lines = stall_lines(caplog)
    assert len(lines) == 1 and STALL.fullmatch(lines[0]), lines
    stop.set()
    await asyncio.wait_for(task, 5)
    await asyncio.sleep(0)
    assert [t for t in asyncio.all_tasks() if t.get_name() == "copytrader-loop-watchdog" and not t.done()] == []


# =========================================================================== #
# P1 (b): engine.io's "connection lost" lines reach copytrader.log, the packet lines do not
# =========================================================================== #
@pytest.fixture
def clean_logging():
    names = ("engineio.client", "socketio.client")
    before = {n: (logging.getLogger(n).level, list(logging.getLogger(n).filters)) for n in names}
    root_level = logging.getLogger().level
    yield
    root = logging.getLogger()
    for h in list(root.handlers):
        if getattr(h, app_mod._HANDLER_TAG, False):
            root.removeHandler(h)
            h.close()
    root.setLevel(root_level)
    for n, (level, filters) in before.items():
        logging.getLogger(n).setLevel(level)
        logging.getLogger(n).filters[:] = filters


def log_lines(path: Path) -> list[str]:
    for h in logging.getLogger().handlers:
        h.flush()
    return path.read_text(encoding="utf-8").splitlines()


class _FakeWs:
    """aiohttp's websocket as engine.io uses it: close(), and a receive() that fails like a dead connection."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def receive(self) -> Any:
        raise ConnectionResetError(f"Cannot write to closing transport ({WS_URL})")


def engineio_client(monkeypatch: pytest.MonkeyPatch) -> Any:
    """A REAL engineio.AsyncClient (the version the MetaApi SDK pins), in the state of an open websocket."""
    engineio = pytest.importorskip("engineio")
    # Client.__init__ installs engine.io's own SIGINT handler once per process: not in this test run
    monkeypatch.setattr(engineio.client, "original_signal_handler", signal.getsignal(signal.SIGINT))
    client = engineio.AsyncClient()
    client.state, client.ws, client.ping_interval = "connected", _FakeWs(), 0.01
    client.queue = client.create_queue()
    return client


async def test_p1b_the_drop_of_two_sockets_reaches_the_log_and_the_packet_lines_do_not(
        tmp_path, clean_logging, monkeypatch) -> None:
    from engineio import packet

    path = app_mod.setup_logging("INFO", tmp_path)
    assert path is not None
    sio = logging.getLogger("socketio.client")
    sockets = [engineio_client(monkeypatch), engineio_client(monkeypatch)]       # london-a, london-b
    for _ in range(200):                    # the hundreds of harmless lines of a working connection
        for client in sockets:
            await client._send_packet(packet.Packet(packet.PING))
            await client._receive_packet(packet.Packet(packet.PONG))
            await client._receive_packet(packet.Packet(
                packet.MESSAGE, data='2["synchronization",{"type":"prices","accountId":"acc","host":"ps-mpa-1"}]'))
        sio.info('Received event "%s" [%s]', "synchronization", "/")
        sio.info('Emitting event "%s" [%s]', "request", "/")
    await sockets[0]._receive_packet(packet.Packet(            # packet text never counts as a connection line
        packet.MESSAGE, data='2["processingError",{"message":"PONG response has not been received"}]'))
    assert log_lines(path) == []

    # The drop: no PONG for the last PING on BOTH sockets (the real library code of engine.io's ping loop) ...
    for client in sockets:
        await client._ping_loop()
    # ... and the read loop of the first one sees the dead connection (the real library code as well)
    first = sockets[0]
    first.write_loop_task = asyncio.ensure_future(asyncio.sleep(0))
    first.ping_loop_task = asyncio.ensure_future(asyncio.sleep(0))
    await first._read_loop_websocket()
    sio.info("Engine.IO connection dropped")

    lines = log_lines(path)
    text = "\n".join(lines)
    assert [line.split(" ", 2)[2] for line in lines] == [
        "WARNING engineio.client: PONG response has not been received, aborting",
        "WARNING engineio.client: Exiting ping task",
        "WARNING engineio.client: PONG response has not been received, aborting",
        "WARNING engineio.client: Exiting ping task",
        'WARNING engineio.client: Unexpected error receiving packet: "Cannot write to closing transport '
        '(https://mt-client-api-v1.london-a.agiliumtrade.ai/ws/?auth-token=***&clientId=0.4711&protocol=3)", '
        "aborting",
        "WARNING socketio.client: Engine.IO connection dropped",
    ], text
    assert JWT not in text                                       # masked like every other line
    for quiet in ("Sending packet", "Received packet", "Received event", "Emitting event", "Waiting for",
                  "Exiting read loop task"):
        assert quiet not in text, quiet
    assert all(client.ws.closed for client in sockets)          # engine.io itself did abort both sockets


async def test_p1b_a_flood_of_connection_lost_lines_is_capped_and_counted(tmp_path, clean_logging) -> None:
    path = app_mod.setup_logging("INFO", tmp_path)
    assert path is not None
    eio = logging.getLogger("engineio.client")
    for _ in range(500):
        eio.info("PONG response has not been received, aborting")
        eio.info("Read loop: WebSocket connection was closed, aborting")
        eio.info("Write loop: WebSocket connection was closed, aborting")
        eio.info('Unexpected error decoding packet: "%s", aborting', "bad")
    lines = log_lines(path)
    burst = app_mod.CONNECTION_LOST_BURST
    assert len(lines) == 4 * burst, lines                        # per kind and minute
    assert sum("PONG response has not been received" in line for line in lines) == burst
    gate = next(f for f in eio.filters if isinstance(f, app_mod.ConnectFailureFilter))
    gate._last = {k: v - 61.0 for k, v in gate._last.items()}  # a minute later
    eio.info("PONG response has not been received, aborting")
    assert log_lines(path)[-1].endswith(
        f"PONG response has not been received, aborting (+{500 - burst} similar line(s) not logged)")


def test_p1b_the_engineio_lines_the_filter_relies_on_exist_in_the_installed_library() -> None:
    """If an engine.io version rewords them, the filter silently lets nothing through: this test says so."""
    engineio = pytest.importorskip("engineio")
    source = inspect.getsource(engineio.asyncio_client)
    for text in ("PONG response has not been received, aborting", "Exiting ping task",
                 'Unexpected error receiving packet: "%s", aborting',
                 'Unexpected error decoding packet: "%s", aborting',
                 "Read loop: WebSocket connection was closed, aborting",
                 "Write loop: WebSocket connection was closed, "):
        assert text in source, text
        assert app_mod._CONNECTION_LOST.search(text.replace("%s", "x")), text


# =========================================================================== #
# P2 (YONETIM-7): the first live fx signal on both config replicas
# =========================================================================== #
@pytest.mark.parametrize("replica", REPLICAS)
async def test_p2_the_first_live_fx_signal_opens_alike_on_both_replicas_and_the_live_one_reports_it(
        tmp_path, monkeypatch, replica) -> None:
    monkeypatch.setattr(fmt, "RISK_PER_R_NOTE", fmt.RISK_PER_R_NOTE)     # Engine.connect_brokers() sets it
    prod = await Prod.create(tmp_path, monkeypatch, replica=replica)
    try:
        await incident_day_eurusd_short(prod)        # every assertion of test_incident_day_signals.py
        # the live config (notify_on_execution) also tells the owner what each account did
        prod.assert_reported("✅ <b>#1 EUR SHORT açıldı</b>", "• fx: 0.75 EURUSD (açık), risk 100.00 USD",
                             "• tickmill: 0.33 EURUSD (açık), risk 45.00 USD")
        assert prod.texts(Severity.WARNING) == [] and prod.texts(Severity.ERROR) == []
    finally:
        await prod.close()


# =========================================================================== #
# P3: Ctrl+C on Windows (asyncio.run() cancels the main task) and a broker SDK's stray CancelledError
# =========================================================================== #
class StrayCloseBroker(FakeBroker):
    """close() fails the way metaapi-cloud-sdk can: it awaits a future that somebody else cancels, so a
    CancelledError comes up in the calling task although nobody asked that task to stop."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.cancelling_seen: list[int] = []

    async def close(self) -> None:
        self.calls.append(("close", None))
        task = asyncio.current_task()
        assert task is not None
        self.cancelling_seen.append(task.cancelling())
        fut = asyncio.get_running_loop().create_future()
        asyncio.get_running_loop().call_soon(fut.cancel)
        await fut


class RecordingNotifier(FakeNotifier):
    def __init__(self) -> None:
        super().__init__()
        self.stopped = False

    async def stop(self) -> None:
        self.stopped = True


def closed_names(brokers: list[FakeBroker]) -> list[str]:
    return [b.name for b in brokers if any(name == "close" for name, _ in b.calls)]


async def test_p3_a_cancelled_serve_still_runs_the_whole_shutdown_when_an_sdk_close_strays(tmp_path) -> None:
    notifier = RecordingNotifier()
    app, _ = _app(tmp_path, notifier)
    tickmill = StrayCloseBroker("tickmill")
    brokers = [FakeBroker("binance"), tickmill, FakeBroker("after")]
    app.engine.configured_brokers = list(brokers)
    app.engine.brokers = list(brokers)
    aclosed: list[bool] = []

    async def aclose() -> None:
        aclosed.append(True)
    app.parser.aclose = aclose                                   # type: ignore[attr-defined]
    task = asyncio.ensure_future(app_mod.serve(app))
    await asyncio.wait_for(app.listener.started.wait(), 2)
    await asyncio.sleep(0.05)
    task.cancel()                       # what asyncio.run()'s SIGINT handler does on the first Ctrl+C on Windows
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 5)  # serve() still ends cancelled: the CLI's "Durduruldu." path is unchanged
    assert tickmill.cancelling_seen == [0]                       # the shutdown no longer counts the Ctrl+C
    assert closed_names(brokers) == ["binance", "tickmill", "after"]
    assert "🛑 Kopya-trade botu durduruldu." in notifier.texts(Severity.INFO)
    assert notifier.stopped and aclosed == [True]                # the owner's queue is sent, the parser closed
    assert app.db._conn is None


class HangingCloseBroker(FakeBroker):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.entered = asyncio.Event()

    async def close(self) -> None:
        self.calls.append(("close", None))
        self.entered.set()
        await asyncio.Event().wait()


async def test_p3_a_second_cancellation_during_the_shutdown_still_goes_on(tmp_path) -> None:
    """The withdrawn request does not make the shutdown uncancellable (asyncio.run()'s final clean-up after a
    hard Ctrl+C cancels the main task again)."""
    app, _ = _app(tmp_path, RecordingNotifier())
    hanging = HangingCloseBroker("tickmill")
    app.engine.configured_brokers = [hanging]
    app.engine.brokers = [hanging]
    task = asyncio.ensure_future(app_mod.serve(app))
    await asyncio.wait_for(app.listener.started.wait(), 2)
    task.cancel()
    await asyncio.wait_for(hanging.entered.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert app.db._conn is None                                  # closed in the finally all the same


P3_CHILD = r'''
"""Child: the real serve() of <repo> under the real asyncio.run(), with a loop that refuses signal handlers
exactly like the Windows ProactorEventLoop, so Ctrl+C reaches asyncio.Runner's own SIGINT handler (it cancels the
main task). tickmill is the REAL MetaApi adapter; the SDK connection's close() strays (CancelledError)."""
import asyncio, faulthandler, sys, tempfile
from pathlib import Path

repo = sys.argv[1]
sys.path.insert(0, repo)
sys.path.insert(0, repo + "/tests")
faulthandler.dump_traceback_later(25, exit=True)

from copytrader.app import serve
from copytrader.brokers.metaapi_broker import MetaApiBroker
from copytrader.config import MetaApiBrokerConfig
from fakes import FakeBroker, FakeNotifier
from test_app_cli import _app
from test_metaapi_broker import FakeConn


def say(*a):
    print(*a, flush=True)


class StrayConn(FakeConn):
    async def close(self):
        self._rec("close")
        say("SDK CLOSE SEES CANCELLING", asyncio.current_task().cancelling())
        fut = asyncio.get_running_loop().create_future()
        asyncio.get_running_loop().call_soon(fut.cancel)
        await fut


class Notifier(FakeNotifier):
    stopped = False

    async def stop(self):
        self.stopped = True


async def amain():
    loop = asyncio.get_running_loop()

    def no_signal_handlers(*a, **k):
        raise NotImplementedError
    loop.add_signal_handler = no_signal_handlers
    notifier = Notifier()
    app, _ = _app(Path(tempfile.mkdtemp()), notifier)
    conn = StrayConn()
    tickmill = MetaApiBroker(MetaApiBrokerConfig(type="metaapi", name="tickmill", token="tok", account_id="acc-1"),
                             connection=conn)
    after = FakeBroker("after")
    app.engine.configured_brokers = [tickmill, after]
    app.engine.brokers = [tickmill, after]

    async def ready():
        await app.listener.started.wait()
        await asyncio.sleep(0.2)
        say("READY")
    feeder = asyncio.ensure_future(ready())
    try:
        await serve(app)
    finally:
        say("SDK CLOSE CALLED:", "close" in [c[0] for c in conn.calls])
        say("AFTER CLOSED:", after.closed)
        say("STOPPED NOTICE:", "🛑 Kopya-trade botu durduruldu." in notifier.texts())
        say("NOTIFIER STOPPED:", notifier.stopped)
        say("DATABASE:", "closed" if app.db._conn is None else "open")
        feeder.cancel()


try:
    asyncio.run(amain())
    say("EXIT: returned")
except KeyboardInterrupt:
    say("EXIT: KeyboardInterrupt")                     # the CLI prints "Durduruldu." here and returns 130
'''


@pytest.mark.skipif(os.name != "posix", reason="sends SIGINT to a child process")
def test_p3_windows_ctrl_c_with_a_stray_metaapi_close_still_shuts_down_completely(tmp_path) -> None:
    script = tmp_path / "p3_child.py"
    script.write_text(P3_CHILD, encoding="utf-8")
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    proc = subprocess.Popen([sys.executable, str(script), REPO], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, encoding="utf-8", env=env)
    lines: list[str] = []
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            lines.append(line.rstrip())
            if line.strip() == "READY":
                proc.send_signal(signal.SIGINT)              # ONE Ctrl+C
        code = proc.wait(timeout=40)
    finally:
        if proc.poll() is None:
            proc.kill()
    out = "\n".join(lines)
    assert "SDK CLOSE SEES CANCELLING 0" in lines, out             # before the fix: 1
    assert "tickmill: MetaApi connection close was interrupted by the SDK" in out, out
    for expected in ("SDK CLOSE CALLED: True", "AFTER CLOSED: True", "STOPPED NOTICE: True", "NOTIFIER STOPPED: True",
                     "DATABASE: closed"):
        assert expected in lines, out
    assert lines[-1] == "EXIT: KeyboardInterrupt" and code == 0, out
    assert "Traceback" not in out, out
