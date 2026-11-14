"""Broker type "mt5_bridge": a second MetaTrader 5 terminal on the same Windows computer, through a helper process.

Why a process of its own
------------------------
The MetaTrader5 Python package talks to ONE terminal per process (module-global state), and the bot process already
uses it for the owner's own account (type "mt5"). This adapter therefore starts ``python -m
copytrader.brokers.mt5_helper`` (same Python, same folder), which runs the very same native adapter
(``mt5_broker.Mt5Broker``) bound to ``terminal_path``, and forwards every Broker call to it over the helper's
stdin / stdout. There is no second order engine: legs, tags, lost-reply adoption, sync, stops, partial closes and the
rest are the native adapter's own code, running in the helper.

Safety rules of the link (the owner's other accounts are never blocked or slowed down by it)
-------------------------------------------------------------------------------------------
* Every call has a time limit (READ_TIMEOUT_SEC, WRITE_TIMEOUT_SEC, ...). The pipe is written by a thread of its
  own, so a helper that stopped reading can never block the bot's event loop.
* The helper is watched: it answers a ping every PING_SEC; one that ended (crash, killed) or that has not answered
  for HANG_SEC is killed and started again with a back-off (RESTART_BACKOFF_SEC), and the owner is told once that
  the link is down and once that it is back, with how long it lasted.
* While the link is down every call is refused at once with BrokerUnavailableError (nothing was sent: the engine may
  open a skipped signal there later, see Engine catch-up).
* A call whose request had gone to the helper when the helper ended or the time ran out: a pure read fails; an order
  request (open, stop / target move, close, cancel) is an OrderOutcomeUnknownError (it may have been executed), never
  "could not be done". An OPEN whose answer was lost is looked up by its tag once the helper is back
  (``adopt_tagged``: found -> adopted, never sent twice; not found -> outcome unknown).
* Shutdown is bounded (CLOSE_*_SEC). The helper ends by itself as soon as the bot's end of its stdin closes (the bot
  exited, crashed or was killed); on Windows it is also put into a kill-on-close job object, and started without a
  console window.
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import html
import itertools
import logging
import os
import subprocess
import sys
import threading
import time
import weakref
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..models import (BrokerPosition, MarketType, OpenRequest, PositionStatus, Quote, Severity, Side, SizingResult,
                      SymbolInfo, TakeProfit)
from ..risk.sizing import SizingError
from . import mt5_helper as wire
from .base import OUTCOME_UNKNOWN_MARK, Broker, BrokerError, BrokerUnavailableError, OrderOutcomeUnknownError
from .mt5_broker import OrderReplyLostError, TerminalGuard, TerminalTimeoutError

logger = logging.getLogger(__name__)

READY_TIMEOUT_SEC = 30.0          # helper started -> "ready" (Python start + imports)
CONNECT_TIMEOUT_SEC = 170.0       # the helper's connect(): initialize() alone may take 90 s (engine allows 180)
READ_TIMEOUT_SEC = 90.0           # account / price / symbol / sizing reads (each native call inside: 30 s)
HEALTH_TIMEOUT_SEC = 20.0         # health_problem() (the engine's heartbeat bounds the whole check to 30 s)
SYNC_TIMEOUT_SEC = 110.0          # sync_position() (the monitor bounds it to 120 s)
WRITE_TIMEOUT_SEC = 300.0         # stop / target moves, closes, cancels
OPEN_TIMEOUT_SEC = 600.0          # open_position(): several legs, each with its own lost-reply wait inside
PING_SEC = 5.0                    # the helper is pinged this often ...
# ... and counts as hung when no pong came for this long. The MetaTrader5 package never releases Python's GIL inside
# a call (its _core.pyd imports neither PyEval_SaveThread nor PyEval_RestoreThread): while the terminal does not
# answer, the WHOLE helper stands still - its own time limits (mt5_broker.CALL_TIMEOUT_SEC, 30 s) cannot fire, so
# this watchdog is the limit that does. Longer than one ordinary native call may take, shorter than a re-login.
HANG_SEC = 45.0
ADOPT_WAIT_SEC = 90.0             # an open whose answer was lost: wait this long for the helper to be back, then look
RESTART_BACKOFF_SEC = (1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0)
STABLE_SEC = 300.0                # a helper that stayed up this long resets the back-off; a drop sooner after the
                                  # last "back" notice is the same unsteady link (no new pair of notices) ...
FLAP_QUIET_SEC = 60.0             # ... unless it lasted this long: then its end is told, with how long it lasted
CLOSE_RPC_SEC = 8.0               # close(): the helper's own close (terminal shutdown) ...
CLOSE_EXIT_SEC = 3.0              # ... then this long for the process to end by itself ...
CLOSE_KILL_SEC = 2.0              # ... then it is killed
CREATE_NO_WINDOW = 0x08000000     # Windows process creation flag: no console window for the helper

_WRITES = frozenset({"open_position", "update_stop_loss", "update_take_profits", "close_position", "cancel_entry",
                     "cancel_entry_remainder"})
_LIVE: "weakref.WeakSet[_Helper]" = weakref.WeakSet()       # every helper process that may still run
_WRITE_TR = {"open_position": "açılış", "update_stop_loss": "stop taşıma", "update_take_profits": "hedef değişikliği",
             "close_position": "kapatma", "cancel_entry": "bekleyen emir iptali",
             "cancel_entry_remainder": "kalan emir iptali"}


def _took_tr(seconds: float) -> str:
    """How long an outage lasted, as the owner reads it: "25 sn", "4 dakika"."""
    return f"{max(1, round(seconds))} sn" if seconds < 90 else f"{max(2, round(seconds / 60))} dakika"


def _every_tr(seconds: float) -> str:
    """"once every ...", as the owner reads it: "1 dakikada bir", "30 saniyede bir"."""
    return f"{seconds / 60.0:g} dakikada bir" if seconds >= 60 else f"{seconds:g} saniyede bir"


# --------------------------------------------------------------------------- #
# Windows: a job object that kills the helper when the bot ends in any way
# --------------------------------------------------------------------------- #
_JOB: Any = None


def _windows_job() -> Any:  # pragma: no cover - Windows only
    """One job object (kept for the bot's lifetime) with KILL_ON_JOB_CLOSE: when the bot process ends - also when it
    is killed - Windows closes the handle and ends every helper in it. SILENT_BREAKAWAY_OK keeps the programs a helper
    starts (MetaTrader5.initialize() may start the terminal itself) OUT of the job: the terminal survives the bot."""
    global _JOB
    if _JOB is not None:
        return _JOB
    import ctypes
    from ctypes import wintypes

    class _Basic(ctypes.Structure):
        _fields_ = [("PerProcessUserTimeLimit", ctypes.c_int64), ("PerJobUserTimeLimit", ctypes.c_int64),
                    ("LimitFlags", wintypes.DWORD), ("MinimumWorkingSetSize", ctypes.c_size_t),
                    ("MaximumWorkingSetSize", ctypes.c_size_t), ("ActiveProcessLimit", wintypes.DWORD),
                    ("Affinity", ctypes.c_size_t), ("PriorityClass", wintypes.DWORD),
                    ("SchedulingClass", wintypes.DWORD)]

    class _Io(ctypes.Structure):
        _fields_ = [(n, ctypes.c_uint64) for n in ("ReadOperationCount", "WriteOperationCount",
                                                    "OtherOperationCount", "ReadTransferCount",
                                                    "WriteTransferCount", "OtherTransferCount")]

    class _Extended(ctypes.Structure):
        _fields_ = [("BasicLimitInformation", _Basic), ("IoInfo", _Io), ("ProcessMemoryLimit", ctypes.c_size_t),
                    ("JobMemoryLimit", ctypes.c_size_t), ("PeakProcessMemoryUsed", ctypes.c_size_t),
                    ("PeakJobMemoryUsed", ctypes.c_size_t)]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel32.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        raise OSError(ctypes.get_last_error(), "CreateJobObjectW")
    info = _Extended()
    info.BasicLimitInformation.LimitFlags = 0x2000 | 0x1000    # KILL_ON_JOB_CLOSE | SILENT_BREAKAWAY_OK
    if not kernel32.SetInformationJobObject(job, 9, ctypes.byref(info), ctypes.sizeof(info)):
        raise OSError(ctypes.get_last_error(), "SetInformationJobObject")
    _JOB = (kernel32, job)
    return _JOB


def _put_in_job(proc: subprocess.Popen[bytes]) -> None:  # pragma: no cover - Windows only
    try:
        kernel32, job = _windows_job()
        if not kernel32.AssignProcessToJobObject(job, int(proc._handle)):   # type: ignore[attr-defined]
            import ctypes
            raise OSError(ctypes.get_last_error(), "AssignProcessToJobObject")
    except Exception as exc:  # noqa: BLE001 - stdin's end still ends the helper
        logger.warning("MT5 helper %s could not be put into the kill-on-close job: %s", proc.pid, exc)


@atexit.register
def _kill_all() -> None:
    """Last resort at interpreter exit: no helper is left behind."""
    for helper in list(_LIVE):
        with contextlib.suppress(Exception):
            helper.kill()


def helper_command() -> tuple[list[str], dict[str, str], str]:
    """(argv, environment, working folder) of a helper: the bot's own Python, the bot's own copytrader package."""
    package_root = str(Path(__file__).resolve().parents[2])
    python = sys.executable or "python"
    exe = Path(python)
    if exe.name.lower() == "pythonw.exe" and (exe.parent / "python.exe").is_file():
        python = str(exe.parent / "python.exe")     # same venv; the console variant (no window: CREATE_NO_WINDOW)
    env = dict(os.environ)
    env["PYTHONPATH"] = package_root + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    return [python, "-m", "copytrader.brokers.mt5_helper"], env, package_root


# --------------------------------------------------------------------------- #
# One helper process
# --------------------------------------------------------------------------- #
class _HelperLost(Exception):
    """The helper ended (or was killed) while a request was pending. ``written``: the request had been written to
    the helper's stdin (it may have been executed)."""

    def __init__(self, reason: str, written: bool) -> None:
        super().__init__(reason)
        self.reason = reason
        self.written = written


class _Pending:
    __slots__ = ("future", "method", "written", "abandoned", "sent_at", "args")

    def __init__(self, future: asyncio.Future[Any], method: str, args: tuple[Any, ...] = ()) -> None:
        self.future = future
        self.method = method
        self.args = args
        self.written = False         # set by the writer thread once the frame is in the pipe
        self.abandoned = False       # the caller gave up (time limit / cancelled): a late answer is only logged
        self.sent_at = time.monotonic()


class _Helper:
    """A running helper: the process, a writer thread (the only one that writes its stdin), a reader thread (its
    stdout), a stderr thread (into the bot's log) and a waiter thread (its exit). Everything that touches the
    pending calls runs on the event loop."""

    def __init__(self, name: str, loop: asyncio.AbstractEventLoop, on_lost: Callable[["_Helper", str], None],
                 command: Callable[[], tuple[list[str], dict[str, str], str]] = helper_command) -> None:
        self.name = name
        self.loop = loop
        self.on_lost = on_lost
        self.on_late: Callable[[_Pending, Any], None] | None = None     # a write's success after its caller gave up
        self.command = command
        self.proc: subprocess.Popen[bytes] | None = None
        self.pending: dict[int, _Pending] = {}
        self.ids = itertools.count(1)
        self.ready: asyncio.Future[Any] = loop.create_future()
        self.ready.add_done_callback(_retrieve)
        self.last_pong = time.monotonic()
        self.lost: str | None = None
        self.started_at = time.monotonic()
        self._queue: list[tuple[Any, _Pending | None]] = []
        self._cond = threading.Condition()
        self._stop_writer = False     # the writer sends what is queued, then closes the helper's stdin
        self.retired = False          # the bot ends this helper itself: its end is not a lost link

    @property
    def pid(self) -> int | None:
        return self.proc.pid if self.proc is not None else None

    def alive(self) -> bool:
        return self.proc is not None and self.lost is None and self.proc.poll() is None

    def start(self, hello: dict[str, Any]) -> None:
        argv, env, cwd = self.command()
        flags = CREATE_NO_WINDOW if sys.platform == "win32" else 0
        self.proc = subprocess.Popen(                                  # noqa: S603 - our own Python and module
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd, env=env,
            close_fds=True, creationflags=flags)
        _LIVE.add(self)
        if sys.platform == "win32":                                    # pragma: no cover - Windows only
            _put_in_job(self.proc)
        logger.info("%s: MT5 helper process started (pid %s)", self.name, self.proc.pid)
        self._post(("hello", hello), None)
        for target, label in ((self._writer, "to"), (self._reader, "from"), (self._stderr, "log"),
                              (self._waiter, "wait")):
            threading.Thread(target=target, name=f"mt5-helper-{label}-{self.name}", daemon=True).start()

    # ---- threads ------------------------------------------------------------
    def _post(self, msg: Any, pending: _Pending | None) -> None:
        with self._cond:
            self._queue.append((msg, pending))
            self._cond.notify()

    def _writer(self) -> None:
        assert self.proc is not None and self.proc.stdin is not None
        stream = self.proc.stdin
        while True:
            with self._cond:
                while not self._queue and not self._stop_writer:
                    self._cond.wait()
                if not self._queue:
                    break
                msg, pending = self._queue.pop(0)
            try:
                frame = wire.encode_frame(msg)
            except Exception as exc:  # noqa: BLE001 - only this call fails, never the writer
                logger.error("%s: a request to the MT5 helper could not be encoded: %r", self.name, exc)
                if pending is not None:
                    with contextlib.suppress(RuntimeError):
                        self.loop.call_soon_threadsafe(
                            _fail, pending, BrokerError(f"istek MT5 yardımcı sürecine gönderilemedi ({exc}); "
                                                        "hiçbir şey gönderilmedi"))
                continue
            if pending is not None:
                # BEFORE the write: once any byte may have reached the helper, the request may be executed there
                # (it may run it and end before this thread runs again): its loss is then "outcome unknown".
                pending.written = True
            try:
                stream.write(frame)
                stream.flush()
            except Exception:  # noqa: BLE001 - the pipe is gone: the reader / waiter report the end
                break
        with contextlib.suppress(Exception):
            stream.close()

    def _reader(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        why = "yardımcı sürecin çıkışı kapandı"
        try:
            while True:
                msg = wire.read_frame(self.proc.stdout)
                if msg is None:
                    break
                self.loop.call_soon_threadsafe(self._on_message, msg)
        except Exception as exc:  # noqa: BLE001
            why = f"yardımcı sürecin yanıtı okunamadı ({exc})"
        else:
            try:                                        # its end: say how it ended when it ends right away
                why = f"yardımcı süreç kapandı (çıkış kodu {self.proc.wait(timeout=2.0)})"
            except subprocess.TimeoutExpired:
                pass
        with contextlib.suppress(RuntimeError):         # the loop may be closed at interpreter exit
            self.loop.call_soon_threadsafe(self._gone, why)

    def _stderr(self) -> None:
        assert self.proc is not None and self.proc.stderr is not None
        child = logging.getLogger(f"{__name__}.helper.{self.name}")
        for raw in iter(self.proc.stderr.readline, b""):
            line = raw.decode("utf-8", "replace").rstrip()
            if not line:
                continue
            level = logging.INFO
            for name, value in (("CRITICAL", logging.CRITICAL), ("ERROR", logging.ERROR),
                                ("WARNING", logging.WARNING), ("DEBUG", logging.DEBUG)):
                if line.startswith(name + " "):
                    level = value
                    break
            child.log(level, "%s", line)

    def _waiter(self) -> None:
        assert self.proc is not None
        code = self.proc.wait()
        with contextlib.suppress(RuntimeError):
            self.loop.call_soon_threadsafe(self._gone, f"yardımcı süreç kapandı (çıkış kodu {code})")

    # ---- event loop side ------------------------------------------------------
    def _on_message(self, msg: Any) -> None:
        kind = msg[0] if isinstance(msg, tuple) and msg else None
        if kind == "pong":
            self.last_pong = time.monotonic()
            return
        if kind == "ready":
            self.last_pong = time.monotonic()
            if not self.ready.done():
                self.ready.set_result(msg[1])
            return
        if kind == "refused":
            if not self.ready.done():
                self.ready.set_exception(BrokerError(str(msg[1])))
            return
        if kind != "result":
            logger.warning("%s: unexpected message from the MT5 helper: %r", self.name, kind)
            return
        _, rid, ok, value = msg
        self.last_pong = time.monotonic()           # an answer proves life as well as a pong
        pending = self.pending.pop(rid, None)
        if pending is None:
            return
        if pending.abandoned or pending.future.done():
            logger.warning("%s: late answer of %s from the MT5 helper after %.0f s (the caller had given up): %s",
                           self.name, pending.method, time.monotonic() - pending.sent_at,
                           str(value if ok else value.get("text"))[:500])
            if ok and pending.method in _WRITES and self.on_late is not None:
                try:
                    self.on_late(pending, value)
                except Exception:  # noqa: BLE001
                    logger.exception("%s: telling the owner about a late answer failed", self.name)
            return
        if ok:
            pending.future.set_result(value)
        else:
            pending.future.set_exception(error_from_wire(value))
            pending.future.add_done_callback(_retrieve)

    def _gone(self, why: str) -> None:
        if self.lost is not None:
            return
        self.lost = why
        if not self.ready.done():
            self.ready.set_exception(BrokerUnavailableError(f"MT5 yardımcı süreci başlatılamadı ({why})."))
        for pending in list(self.pending.values()):
            _fail(pending, _HelperLost(why, pending.written))
        self.pending.clear()
        self.kill()
        if not self.retired:
            self.on_lost(self, why)

    def request(self, method: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> _Pending:
        rid = next(self.ids)
        pending = _Pending(self.loop.create_future(), method, args)
        self.pending[rid] = pending
        self._post(("call", rid, method, args, kwargs), pending)
        return pending

    def post(self, msg: Any) -> None:
        if self.alive():
            self._post(msg, None)

    def ping(self) -> None:
        self.post(("ping", next(self.ids)))

    def silent_for(self) -> float:
        return time.monotonic() - self.last_pong

    def kill(self) -> None:
        """Ends the process at once (no-op when it has ended). Never blocks."""
        with self._cond:
            self._stop_writer = True
            self._queue.clear()
            self._cond.notify_all()
        proc = self.proc
        if proc is not None and proc.poll() is None:
            with contextlib.suppress(Exception):
                proc.kill()

    async def stop(self) -> None:
        """Bounded, polite end: "exit" (the helper closes its terminal link), then kill."""
        proc = self.proc
        self.retired = True
        self._post(("exit",), None)
        with self._cond:
            self._stop_writer = True
            self._cond.notify_all()
        if proc is None:
            return
        loop = asyncio.get_running_loop()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(loop.run_in_executor(None, proc.wait), CLOSE_EXIT_SEC)
        if proc.poll() is None:
            with contextlib.suppress(Exception):
                proc.kill()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(loop.run_in_executor(None, proc.wait), CLOSE_KILL_SEC)
        self.lost = self.lost or "kapatıldı"
        for pending in list(self.pending.values()):
            _fail(pending, _HelperLost("kapatıldı", pending.written))
        self.pending.clear()


def _retrieve(future: asyncio.Future[Any]) -> None:
    """Done-callback: an exception nobody waits for any more is not reported as "never retrieved"."""
    if not future.cancelled():
        future.exception()


def _fail(pending: _Pending, exc: BaseException) -> None:
    if pending.future.done():
        return
    if pending.abandoned:              # its caller gave up already (time limit, cancelled): nobody to tell
        pending.future.cancel()
        return
    pending.future.set_exception(exc)


def error_from_wire(data: dict[str, Any]) -> BaseException:
    """The adapter's exception, rebuilt from :func:`mt5_helper.error_to_wire` (the classes the engine tells apart)."""
    types = list(data.get("types") or [])
    text = str(data.get("text") or "")
    attrs = dict(data.get("attrs") or {})
    exc: BaseException
    if "TerminalTimeoutError" in types:
        exc = TerminalTimeoutError.__new__(TerminalTimeoutError)
        BrokerError.__init__(exc, text)
        exc.reason = str(attrs.get("reason") or text)               # type: ignore[attr-defined]
        exc.started = bool(attrs.get("started"))                     # type: ignore[attr-defined]
    elif "OrderReplyLostError" in types:
        exc = OrderReplyLostError(text)
    elif "OrderOutcomeUnknownError" in types:
        exc = OrderOutcomeUnknownError(text)
    elif "BrokerUnavailableError" in types:
        exc = BrokerUnavailableError(text)
    elif "SizingError" in types:
        exc = SizingError(text)
    elif "CancelledError" in types:
        exc = BrokerError(f"MT5 yardımcı sürecinde istek yarıda kaldı ({text})")
    else:
        exc = BrokerError(text or "MT5 yardımcı sürecinde bilinmeyen hata")
    if attrs.get("no_answer"):
        exc.no_answer = True                                         # type: ignore[attr-defined]
    return exc


# --------------------------------------------------------------------------- #
# The adapter
# --------------------------------------------------------------------------- #
class Mt5BridgeBroker(Broker):
    """Broker of type "mt5_bridge" (see the module text). ``command`` replaces the helper's start command in tests."""

    kind = "mt5_bridge"

    def __init__(self, config: Any, *, pin_dir: str | None = None, guard: TerminalGuard | None = None,
                 command: Callable[[], tuple[list[str], dict[str, str], str]] = helper_command) -> None:
        super().__init__(config)
        self.cfg = config
        self.startup_warnings: list[str] = []
        self.risk_config: Any = None
        self._pin_dir = pin_dir
        self._guard = guard or TerminalGuard()
        self._command = command
        self._helper: _Helper | None = None
        self._up = False                    # the helper is running and connected to its terminal
        self._wanted = False                # between a successful connect() and close(): keep it up
        self._notifier: Any = None
        self._watch: asyncio.Task[None] | None = None
        self._restart: asyncio.Task[None] | None = None
        self._notices: set[asyncio.Task[None]] = set()
        self._outage_since: float | None = None
        self._outage_why = ""
        self._outage_told = False
        self._restart_failed_told = False
        self._order_in_outage = False
        self._attempts = 0
        self.outages = 0
        self._back_at: float | None = None     # the last "back" notice (monotonic)
        self._flapping = False                 # the present outage began soon after that notice
        self._quiet_drops = 0                  # short drops not told yet

    # ---- engine hooks -----------------------------------------------------------
    def set_notifier(self, notifier: Any) -> None:
        self._notifier = notifier

    def set_risk_config(self, risk_config: Any) -> None:
        self.risk_config = risk_config
        if self._helper is not None:
            self._helper.post(("risk", risk_config))

    @property
    def link_outage_announced(self) -> bool:
        """For the engine's heartbeat: the helper is down and the owner was told so by this adapter (its own
        "back" notice follows): the heartbeat does not send a second alert for the same outage."""
        return self._outage_since is not None and self._outage_told

    # ---- lifecycle ------------------------------------------------------------------
    def _hello(self) -> dict[str, Any]:
        return {
            # (the MetaApi keys kept in the block for the way back are not needed there: never handed over)
            "version": wire.PROTOCOL, "config": self.cfg.model_dump(mode="python", exclude={"token", "account_id",
                                                                                         "region"}),
            "pin_dir": self._pin_dir,
            "guard": {"folders": dict(self._guard.folders), "companies": dict(self._guard.companies),
                      "markers": dict(self._guard.markers)},
            "risk": self.risk_config, "log_level": logging.getLevelName(logging.getLogger("copytrader").getEffectiveLevel()),
        }

    async def _start_helper(self) -> _Helper:
        """A new helper, connected to its terminal. Raises BrokerError (the helper is ended then)."""
        loop = asyncio.get_running_loop()
        helper = _Helper(self.name, loop, self._helper_lost, self._command)
        helper.on_late = self._late_answer
        self._helper = helper
        try:
            try:
                await loop.run_in_executor(None, helper.start, self._hello())
            except OSError as exc:
                raise BrokerError(f"MT5 yardımcı süreci başlatılamadı ({exc}).") from exc
            try:
                await asyncio.wait_for(asyncio.shield(helper.ready), READY_TIMEOUT_SEC)
            except asyncio.TimeoutError as exc:
                raise BrokerError(f"MT5 yardımcı süreci {READY_TIMEOUT_SEC:g} sn içinde hazır olmadı.") from exc
            await self._call_on(helper, "connect", (), {}, CONNECT_TIMEOUT_SEC)
            try:
                warnings = await self._call_on(helper, "startup_warnings", (), {}, READ_TIMEOUT_SEC)
            except BrokerError:
                warnings = []
            self.startup_warnings = [str(w) for w in warnings or []]
        except BaseException:
            helper.retired = True
            helper.kill()
            if self._helper is helper:
                self._helper = None
            raise
        return helper

    async def connect(self) -> None:
        if self._up:
            return
        await self._start_helper()
        self._up = True
        self._wanted = True
        self._attempts = 0
        if self._watch is None or self._watch.done():
            self._watch = asyncio.ensure_future(self._watch_loop())

    async def close(self) -> None:
        self._wanted = False
        self._up = False
        for task in (self._watch, self._restart):
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(BaseException):
                    await asyncio.wait({task}, timeout=CLOSE_KILL_SEC)
        self._watch = self._restart = None
        helper, self._helper = self._helper, None
        if helper is None:
            return
        helper.retired = True
        if helper.alive():
            with contextlib.suppress(BaseException):
                await self._call_on(helper, "close", (), {}, CLOSE_RPC_SEC)
        await helper.stop()
        for notice in list(self._notices):
            notice.cancel()

    # ---- watching and restarting ----------------------------------------------------
    async def _watch_loop(self) -> None:
        while self._wanted:
            await asyncio.sleep(PING_SEC)
            helper = self._helper
            if not self._up or helper is None:
                continue
            if not helper.alive():
                helper._gone(helper.lost or "yardımcı süreç kapandı")
                continue
            if helper.silent_for() > HANG_SEC:
                logger.error("%s: MT5 helper (pid %s) has not answered for %.0f s; it is killed and started again",
                             self.name, helper.pid, helper.silent_for())
                helper._gone(f"yardımcı süreç {helper.silent_for():.0f} sn'dir yanıt vermiyor")
                continue
            if helper.started_at and time.monotonic() - helper.started_at > STABLE_SEC:
                self._attempts = 0
            helper.ping()

    def _helper_lost(self, helper: _Helper, why: str) -> None:
        """A helper ended or was declared hung (called on the event loop, never blocks)."""
        if helper is not self._helper or not self._wanted:
            return
        self._up = False
        self._helper = None
        if self._outage_since is None:
            now = time.monotonic()
            self._outage_since = now
            self._outage_why = why
            self._restart_failed_told = False
            self.outages += 1
            logger.error("%s: MT5 link lost: %s", self.name, why)
            if self._back_at is not None and now - self._back_at < STABLE_SEC:
                # A drop again soon after the last "back" notice: the same unsteady link, no down notice now
                # (counted into the next one). Nobody has been told: the heartbeat's own alert stays armed for an
                # outage that lasts, and _back tells one that took FLAP_QUIET_SEC or more.
                self._flapping = True
                self._quiet_drops += 1
                self._outage_told = False
            else:
                self._flapping = False
                self._outage_told = False
                self._tell_down(why)
        if self._restart is None or self._restart.done():
            self._restart = asyncio.ensure_future(self._restart_loop())

    async def _restart_loop(self) -> None:
        while self._wanted and not self._up:
            delay = RESTART_BACKOFF_SEC[min(self._attempts, len(RESTART_BACKOFF_SEC) - 1)]
            self._attempts += 1
            await asyncio.sleep(delay)
            if not self._wanted:
                return
            try:
                await self._start_helper()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - tried again after the next pause
                reason = str(exc) or type(exc).__name__
                logger.warning("%s: MT5 helper restart %d failed: %s", self.name, self._attempts, reason)
                self._tell_restart_failed(reason)
                continue
            self._up = True
            self._back()
            return

    # ---- owner notices ------------------------------------------------------------------
    def _send_notice(self, text: str, severity: Severity) -> None:
        notifier = self._notifier
        if notifier is None:
            return

        async def send() -> None:
            try:
                await notifier.notify(text, severity)
            except Exception:  # noqa: BLE001 - a notice must never break the adapter
                logger.exception("%s: could not send a notice to the owner", self.name)

        task = asyncio.ensure_future(send())
        self._notices.add(task)
        task.add_done_callback(self._notices.discard)

    def _tell_down(self, why: str) -> None:
        name = html.escape(self.name, quote=False)
        self._outage_told = self._notifier is not None
        self._send_notice(
            f"🔌 <b>{name}</b>: MT5 bağlantısı koptu ({html.escape(why, quote=False)}). Bot bağlantıyı kendisi yeniden "
            "kuruyor; bu sırada bu hesapta yeni işlem açılmaz, diğer hesaplar etkilenmez. Bağlantı gelince ayrıca "
            f"haber verilir.{self._drops_tr()}", Severity.WARNING)

    def _tell_restart_failed(self, reason: str) -> None:
        if self._restart_failed_told:
            return
        self._restart_failed_told = True
        name = html.escape(self.name, quote=False)
        self._send_notice(
            f"🔌 <b>{name}</b>: MT5 bağlantısı yeniden kurulamadı: <code>{html.escape(reason, quote=False)}</code>. "
            f"Bot denemeyi sürdürüyor (en seyrek {_every_tr(RESTART_BACKOFF_SEC[-1])}). Bu sırada bu hesapta yeni "
            "işlem açılmaz; diğer hesaplar etkilenmez (bu uyarı tekrarlanmaz).", Severity.WARNING)

    def _back(self) -> None:
        since, self._outage_since = self._outage_since, None
        order_waited, self._order_in_outage = self._order_in_outage, False
        flapping, self._flapping = self._flapping, False
        if since is None:
            return
        now = time.monotonic()
        took = _took_tr(now - since)
        logger.info("%s: MT5 link back after %s", self.name, took)
        if flapping and now - since < FLAP_QUIET_SEC and not order_waited and not self._restart_failed_told:
            return                          # a short drop of an unsteady link: counted, told with the next notice
        self._back_at = now
        name = html.escape(self.name, quote=False)
        if flapping and self._quiet_drops:
            self._quiet_drops -= 1          # this outage's own drop was counted as a quiet one: it is told here
        more = self._drops_tr()
        if order_waited:
            during = ("Kesinti sırasında bu hesaba gönderilmiş bir emir yanıt bekliyordu; o emir gerçekleşmiş olabilir, "
                      "bu hesaptaki işlemleri kontrol edin.")
        else:
            during = "Kesinti sırasında gelen yeni sinyaller bu hesapta o an açılmadı (atlanan olduysa ayrıca bildirilir)."
        self._send_notice(
            f"✅ <b>{name}</b>: MT5 bağlantısı yeniden kuruldu, kesinti yaklaşık {took} sürdü. {during} Diğer "
            f"hesaplar etkilenmedi; yeni sinyaller bu hesapta yine açılır.{more}", Severity.INFO)

    def _drops_tr(self) -> str:
        """The short drops since the last notice, for the next one (and the count starts again)."""
        drops, self._quiet_drops = self._quiet_drops, 0
        return (f" Son bildirimden beri {drops} kısa kopma daha oldu; bot hepsini kendisi giderdi." if drops else "")

    def _late_answer(self, pending: _Pending, value: Any) -> None:
        """A write's SUCCESSFUL answer arrived after its caller had given up (its time limit; the owner was told
        "durumu belirsiz"). It is told to the owner once, with what the account now holds. Not adopted here: the
        engine has already closed that leg as "outcome unknown" and owns the trade's records (written under the
        trade's lock); a stop / target / close change shows up in the position's next sync anyway, but an OPEN that
        went through is a position the bot does not track, so the owner must know at once."""
        name = html.escape(self.name, quote=False)
        what = _WRITE_TR.get(pending.method, pending.method)
        if pending.method == "open_position" and isinstance(value, BrokerPosition):
            req = pending.args[0] if pending.args else None
            tag = html.escape(str(getattr(req, "client_tag", "") or ""), quote=False)
            late = f"⚠️ <b>{name}</b>: zaman sınırı aşıldıktan sonra MT5'ten gecikmiş bir yanıt geldi: '{tag}' etiketli"
            symbol = html.escape(value.broker_symbol, quote=False)
            if value.status in (PositionStatus.FAILED, PositionStatus.CANCELLED):
                why = html.escape(str(value.error or "neden bildirilmedi"), quote=False)
                text = (f"{late} emir AÇILMADI (durum: {value.status.value}; <code>{why}</code>). Bu hesapta bu "
                        "işlem için pozisyon yok; yine de MT5'te kontrol edin.")
            elif value.status is PositionStatus.CLOSED:
                why = html.escape(str(value.error or "neden bildirilmedi"), quote=False)
                text = (f"{late} emir açıldı ama pozisyon hemen KAPANDI (durum: closed; <code>{why}</code>). Bu "
                        "hesapta bu işlemden açık pozisyon kalmamış olmalı; yine de MT5'te kontrol edin.")
            elif value.status is PositionStatus.PENDING:
                text = (f"{late} bekleyen emir {symbol} için YERLEŞTİRİLDİ ({value.quantity:g} lot). Bot bu emri "
                        "kaydetmedi ve takip ETMİYOR: dolarsa pozisyonu da izlemez. MT5'te kontrol edin, "
                        "gerekirse emri iptal edin.")
            else:
                text = (f"{late} emir AÇILDI ({value.filled_quantity or value.quantity:g} lot {symbol}, durum: "
                        f"{value.status.value}). Bot bu pozisyonu kaydetmedi ve takip ETMİYOR: MT5'te kontrol edin, "
                        "kendiniz yönetin ya da kapatın.")
        else:
            pos = pending.args[0] if pending.args else None
            symbol = html.escape(str(getattr(pos, "broker_symbol", "") or ""), quote=False)
            text = (f"⚠️ <b>{name}</b>: zaman sınırı aşıldıktan sonra MT5'ten gecikmiş bir yanıt geldi: {symbol} "
                    f"için {what} isteği GERÇEKLEŞTİ. Bot bunu pozisyonun bir sonraki kontrolünde hesaptan okur; "
                    "MT5'te kontrol edin.")
        self._send_notice(text, Severity.WARNING)

    # ---- calls ------------------------------------------------------------------------
    def _down_text(self) -> str:
        if self._outage_since is not None:
            return (f"MT5 bağlantısı şu an yeniden kuruluyor ({self._outage_why}); bu hesapta işlem "
                    "yapılmadı.")
        return "MT5 bağlı değil."

    async def _call_on(self, helper: _Helper, method: str, args: tuple[Any, ...], kwargs: dict[str, Any],
                       timeout: float) -> Any:
        if not helper.alive():
            raise BrokerUnavailableError(self._down_text())
        pending = helper.request(method, args, kwargs)
        try:
            return await asyncio.wait_for(asyncio.shield(pending.future), timeout)
        except asyncio.TimeoutError:
            pending.abandoned = True
            if method in _WRITES:
                self._order_in_outage = self._order_in_outage or self._outage_since is not None
                raise OrderOutcomeUnknownError(
                    f"Emir {OUTCOME_UNKNOWN_MARK} MT5 yardımcı süreci {timeout:g} sn içinde yanıt "
                    "vermedi. İstek gönderilmiş olabilir; aynı istek ikinci kez gönderilmedi. MT5'te bu hesaptaki "
                    "emir ve pozisyonlara bakın.") from None
            raise BrokerError(f"MT5 yardımcı süreci {timeout:g} sn içinde yanıt vermedi.") from None
        except asyncio.CancelledError:
            pending.abandoned = True
            raise
        except _HelperLost as lost:
            if method in _WRITES and lost.written:
                self._order_in_outage = True
                raise OrderOutcomeUnknownError(
                    f"Emir {OUTCOME_UNKNOWN_MARK} istek MT5 yardımcı sürecine gönderildikten sonra "
                    f"bağlantı koptu ({lost.reason}). İstek gerçekleşmiş olabilir; aynı istek ikinci kez "
                    "gönderilmedi.") from lost
            raise BrokerUnavailableError(f"MT5 bağlantısı koptu ({lost.reason}); bu istek "
                                         "tamamlanmadı.") from lost

    async def _rpc(self, method: str, *args: Any, timeout: float | None = None, **kwargs: Any) -> Any:
        helper = self._helper
        if not self._up or helper is None:
            raise BrokerUnavailableError(self._down_text())
        return await self._call_on(helper, method, args, kwargs, READ_TIMEOUT_SEC if timeout is None else timeout)

    async def health_problem(self) -> str | None:
        if not self._up or self._helper is None:
            return self._down_text()
        try:
            answer = await self._rpc("health_problem", timeout=HEALTH_TIMEOUT_SEC)
        except BrokerError as exc:
            return str(exc)
        return str(answer) if answer else None

    # ---- account / market data -------------------------------------------------------------
    async def get_equity(self) -> float:
        return float(await self._rpc("get_equity"))

    async def get_free_margin(self) -> float:
        return float(await self._rpc("get_free_margin"))

    async def account_currency(self) -> str:
        return str(await self._rpc("account_currency"))

    async def resolve_symbol(self, base_asset: str, quote_asset: str | None = None,
                             market_type: MarketType | None = None, raw_symbol: str | None = None) -> SymbolInfo | None:
        return await self._rpc("resolve_symbol", base_asset, quote_asset, market_type, raw_symbol)  # type: ignore[no-any-return]

    async def get_quote(self, symbol: SymbolInfo) -> Quote:
        return await self._rpc("get_quote", symbol)  # type: ignore[no-any-return]

    async def size_position(self, symbol: SymbolInfo, side: Side, entry_price: float, stop_loss: float,
                            risk_amount: float) -> SizingResult:
        return await self._rpc("size_position", symbol, side, entry_price, stop_loss, risk_amount)  # type: ignore[no-any-return]

    # ---- trading --------------------------------------------------------------------------
    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        try:
            return await self._rpc("open_position", req, timeout=OPEN_TIMEOUT_SEC)  # type: ignore[no-any-return]
        except OrderOutcomeUnknownError as exc:
            if not isinstance(exc.__cause__, _HelperLost):
                raise
            lost = exc
        # The helper ended while the open ran: look the order up by its tag once the helper is back. Never re-sent.
        logger.error("%s: MT5 helper lost during open %s; looking it up by its tag", self.name, req.client_tag)
        deadline = time.monotonic() + ADOPT_WAIT_SEC
        while not self._up and self._wanted and time.monotonic() < deadline:
            await asyncio.sleep(0.2)
        if not self._up:
            raise lost
        try:
            pos = await self._rpc("adopt_tagged", req)
        except BrokerError as exc:
            raise OrderOutcomeUnknownError(
                f"Emir {OUTCOME_UNKNOWN_MARK} bağlantı açılış sırasında koptu ve '{req.client_tag}' "
                f"etiketli emir hesapta aranamadı ({exc}). Emir gerçekleşmiş olabilir; ikinci kez gönderilmedi. MT5'te "
                "bu hesaba bakın.") from lost
        if pos is None:
            raise OrderOutcomeUnknownError(
                f"Emir {OUTCOME_UNKNOWN_MARK} bağlantı açılış sırasında koptu; hesapta '{req.client_tag}' "
                "etiketli emir ya da pozisyon şu an görünmüyor, yine de gönderilmiş olabilir. Aynı emir ikinci kez "
                "gönderilmedi; MT5'te bu hesaba bakın.") from lost
        pos.data.setdefault("warnings", []).append(
            f"MT5 bağlantısı açılış sırasında koptu; emir yeniden bağlanınca '{req.client_tag}' etiketiyle hesapta "
            "bulundu ve takibe alındı (tekrar gönderilmedi).")
        return pos  # type: ignore[no-any-return]

    async def update_stop_loss(self, pos: BrokerPosition, new_stop_loss: float) -> BrokerPosition:
        return await self._rpc("update_stop_loss", pos, new_stop_loss, timeout=WRITE_TIMEOUT_SEC)  # type: ignore[no-any-return]

    async def update_take_profits(self, pos: BrokerPosition, take_profits: list[TakeProfit]) -> BrokerPosition:
        return await self._rpc("update_take_profits", pos, take_profits, timeout=WRITE_TIMEOUT_SEC)  # type: ignore[no-any-return]

    async def close_position(self, pos: BrokerPosition, fraction: float = 1.0) -> BrokerPosition:
        return await self._rpc("close_position", pos, fraction, timeout=WRITE_TIMEOUT_SEC)  # type: ignore[no-any-return]

    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        return await self._rpc("cancel_entry", pos, timeout=WRITE_TIMEOUT_SEC)  # type: ignore[no-any-return]

    async def cancel_entry_remainder(self, pos: BrokerPosition) -> BrokerPosition:
        return await self._rpc("cancel_entry_remainder", pos, timeout=WRITE_TIMEOUT_SEC)  # type: ignore[no-any-return]

    async def sync_position(self, pos: BrokerPosition) -> BrokerPosition:
        return await self._rpc("sync_position", pos, timeout=SYNC_TIMEOUT_SEC)  # type: ignore[no-any-return]


__all__ = ["Mt5BridgeBroker", "error_from_wire", "helper_command"]
