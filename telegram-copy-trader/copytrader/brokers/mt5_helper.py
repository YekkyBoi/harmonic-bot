"""The helper process of a broker of type "mt5_bridge" (see mt5_bridge.py): ``python -m copytrader.brokers.mt5_helper``.

The MetaTrader5 package talks to ONE terminal per process and the bot process already uses it for the owner's own
account (type "mt5"). A second terminal is therefore driven from this separate process, which runs the very same
native adapter (``Mt5Broker``) bound to that terminal's path, and answers the bot over its stdin / stdout.

Protocol (both directions): frames of a 4-byte big-endian length + a pickle (the bot and this process are the same
code from the same folder, started with the same Python). The bot sends ``("hello", {...})`` first, then
``("call", id, method, args, kwargs)``, ``("risk", RiskConfig)``, ``("ping", id)`` and ``("exit",)``; this process
answers ``("ready", {...})``, ``("result", id, ok, value | error)`` and ``("pong", id)``. An error goes over as a
plain dict (type name, text, a few attributes) and is rebuilt on the other side.

stdout carries the protocol only: file descriptor 1 is pointed at stderr right at the start, so nothing printed (by
Python or by a native library) can break a frame. Logging goes to stderr, which the bot copies into its own log.
When stdin ends (the bot exited, crashed or was killed) this process ends at once: it never outlives the bot.

Windows: no thread may sit in a blocking read of stdin. While a synchronous read is pending on the pipe, any other
use of the same handle waits for it - and a library that sets up its own C runtime when it is loaded (numpy, which
the MetaTrader5 package imports) asks Windows about the standard handles and so hung for ever (live, 2026-11-14:
"MT5 yardımcı süreci 170 sn içinde yanıt vermedi"; reproduced on the server with tools/teshis_mt5*.py). So the
reader waits for data by polling the pipe (PeekNamedPipe) and only then reads what is there, and the MetaTrader5
package is loaded before the reader thread starts.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import logging
import os
import pickle
import struct
import sys
import threading
import time
from typing import Any, BinaryIO

PROTOCOL = 1
_HEADER = struct.Struct(">I")
MAX_FRAME = 64 * 1024 * 1024
EXIT_CLOSE_SEC = 5.0              # "exit": how long the adapter's close() may take before the process just ends
# Tests only: "module:callable" returning a fake MetaTrader5 module (the real package is imported otherwise), and
# a folder put in front of sys.path for it.
STDIN_POLL_SEC = 0.005            # Windows: how often the reader looks whether the bot has written something
FAKE_ENV = "COPYTRADER_MT5_HELPER_FAKE"
FAKE_PATH_ENV = "COPYTRADER_MT5_HELPER_FAKE_PATH"

# The adapter's methods the bot may call (nothing else is reachable through the pipe).
METHODS = frozenset({
    "connect", "close", "get_equity", "get_free_margin", "account_currency", "resolve_symbol", "get_quote",
    "size_position", "open_position", "update_stop_loss", "update_take_profits", "close_position", "cancel_entry",
    "cancel_entry_remainder", "sync_position", "health_problem", "adopt_tagged", "startup_warnings",
})

logger = logging.getLogger("copytrader.mt5_helper")


def encode_frame(obj: Any) -> bytes:
    data = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    return _HEADER.pack(len(data)) + data


def write_frame(stream: BinaryIO, obj: Any) -> None:
    stream.write(encode_frame(obj))
    stream.flush()


def read_frame(stream: BinaryIO, wait: Any = None) -> Any:
    """The next message, or None at the end of the stream (also for a frame cut off by the end). ``wait``: called
    before every read; it returns once the stream has data (True) or has ended (False), see :func:`pipe_waiter`."""
    head = _read_exact(stream, _HEADER.size, wait)
    if head is None:
        return None
    (size,) = _HEADER.unpack(head)
    if size > MAX_FRAME:
        raise ValueError(f"frame too large: {size}")
    body = _read_exact(stream, size, wait)
    if body is None:
        return None
    return pickle.loads(body)      # noqa: S301 - our own parent / child process over a private pipe


def _read_exact(stream: BinaryIO, size: int, wait: Any = None) -> bytes | None:
    parts: list[bytes] = []
    left = size
    while left > 0:
        if wait is not None and not wait():
            return None
        chunk = stream.read(left)
        if not chunk:
            return None
        parts.append(chunk)
        left -= len(chunk)
    return b"".join(parts)


def pipe_waiter(fd: int, *, peek: Any = None, sleep: Any = time.sleep) -> Any:
    """A ``wait`` for :func:`read_frame` that never leaves a blocking read pending on ``fd`` (see the module text):
    it polls the pipe until it has data (True) or its other end is closed (False). None where that is not needed or
    not possible: not Windows, or ``fd`` is not a pipe (the reads then block as they always did).
    ``peek(handle) -> bytes available, or None when the pipe is broken`` and ``sleep`` are injectable for tests."""
    if peek is None:
        if sys.platform != "win32":
            return None
        peek = _windows_peek(fd)
        if peek is None:
            return None

    def wait() -> bool:
        while True:
            available = peek()
            if available is None:
                return False
            if available:
                return True
            sleep(STDIN_POLL_SEC)

    return wait


def _windows_peek(fd: int) -> Any:
    """``peek()`` for a pipe on Windows (PeekNamedPipe: bytes available, None when the pipe is broken); None if
    ``fd`` is not a pipe or the call is not available."""
    try:
        import ctypes
        import msvcrt
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)          # type: ignore[attr-defined]
        kernel32.PeekNamedPipe.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p,
                                           ctypes.POINTER(wintypes.DWORD), ctypes.c_void_p]
        kernel32.PeekNamedPipe.restype = wintypes.BOOL
        kernel32.GetFileType.argtypes = [wintypes.HANDLE]
        kernel32.GetFileType.restype = wintypes.DWORD
        handle = msvcrt.get_osfhandle(fd)                                  # type: ignore[attr-defined]
        if kernel32.GetFileType(handle) != 3:                              # FILE_TYPE_PIPE
            return None
    except Exception:  # noqa: BLE001 - the blocking read is the fallback
        logger.warning("stdin cannot be polled; reading it the blocking way", exc_info=True)
        return None

    def peek() -> int | None:
        available = wintypes.DWORD(0)
        if not kernel32.PeekNamedPipe(handle, None, 0, None, ctypes.byref(available), None):
            return None                      # broken pipe (the bot is gone) or any other failure: the stream ended
        return int(available.value)

    return peek


def preload_mt5() -> None:
    """Load the MetaTrader5 package (and numpy with it) now: before the reader thread exists and before anything
    else could be waiting on a standard handle. A missing package is reported later, by Mt5Broker.connect()."""
    if os.environ.get(FAKE_ENV) or sys.platform != "win32":
        return
    try:
        importlib.import_module("MetaTrader5")
    except Exception:  # noqa: BLE001 - connect() tells the owner what is wrong
        logger.warning("MetaTrader5 package could not be loaded at the start", exc_info=True)


def error_to_wire(exc: BaseException) -> dict[str, Any]:
    """An exception of the adapter as plain data: its class names (most specific first), text and attributes."""
    names = [k.__name__ for k in type(exc).__mro__ if k not in (BaseException, object)]
    attrs = {}
    for key in ("started", "reason", "no_answer", "code"):
        value = getattr(exc, key, None)
        if isinstance(value, (bool, int, float, str)) or value is None:
            attrs[key] = value
    return {"types": names, "text": str(exc), "attrs": attrs}


def _no_inherit(*fds: int) -> None:
    """Keep the protocol pipes out of programs this process starts (MetaTrader5.initialize() may start the
    terminal): a terminal holding them would keep them open after this process ended."""
    for fd in fds:
        with contextlib.suppress(OSError, ValueError):
            os.set_inheritable(fd, False)


def _load_mt5() -> Any | None:
    spec = os.environ.get(FAKE_ENV)
    if not spec:
        return None                                  # Mt5Broker.connect() imports the real package
    extra = os.environ.get(FAKE_PATH_ENV)
    if extra and extra not in sys.path:
        sys.path.insert(0, extra)
    module_name, _, attr = spec.partition(":")
    factory = getattr(importlib.import_module(module_name), attr or "make")
    return factory()


class _Server:
    def __init__(self, broker: Any, out: BinaryIO) -> None:
        self.broker = broker
        self.out = out
        self.lock = threading.Lock()
        self.tasks: set[asyncio.Task[Any]] = set()

    def send(self, msg: Any) -> None:
        with self.lock:
            write_frame(self.out, msg)

    async def call(self, rid: int, method: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        try:
            if method not in METHODS:
                raise ValueError(f"unknown method {method!r}")
            if method == "startup_warnings":
                value: Any = list(getattr(self.broker, "startup_warnings", None) or [])
            else:
                value = await getattr(self.broker, method)(*args, **kwargs)
            reply = ("result", rid, True, value)
        except asyncio.CancelledError:
            reply = ("result", rid, False, {"types": ["CancelledError"], "text": "iptal edildi", "attrs": {}})
        except BaseException as exc:  # noqa: BLE001 - every failure goes back to the bot
            if not isinstance(exc, Exception):
                logger.exception("%s raised %r", method, exc)
            reply = ("result", rid, False, error_to_wire(exc))
        try:
            self.send(reply)
        except Exception:  # noqa: BLE001 - unpicklable result: report it as an error instead
            logger.exception("reply of %s could not be sent", method)
            with contextlib.suppress(Exception):
                self.send(("result", rid, False, {"types": ["BrokerError"],
                                                   "text": f"yardımcı süreç {method} yanıtını gönderemedi",
                                                   "attrs": {}}))

    async def serve(self, inbox: asyncio.Queue[Any]) -> None:
        while True:
            msg = await inbox.get()
            if msg is None:
                return
            kind = msg[0]
            if kind == "ping":
                self.send(("pong", msg[1]))
            elif kind == "call":
                _, rid, method, args, kwargs = msg
                task = asyncio.ensure_future(self.call(rid, method, tuple(args), dict(kwargs)))
                self.tasks.add(task)
                task.add_done_callback(self.tasks.discard)
            elif kind == "risk":
                with contextlib.suppress(Exception):
                    self.broker.set_risk_config(msg[1])
            elif kind == "exit":
                with contextlib.suppress(BaseException):
                    await asyncio.wait_for(self.broker.close(), EXIT_CLOSE_SEC)
                return


async def _run(hello: dict[str, Any], stdin: BinaryIO, out: BinaryIO) -> None:
    from ..config import Mt5BridgeBrokerConfig
    from .mt5_broker import Mt5Broker, TerminalGuard

    preload_mt5()
    cfg = Mt5BridgeBrokerConfig.model_validate(hello["config"])
    guard = TerminalGuard(**(hello.get("guard") or {}))
    broker = Mt5Broker(cfg, mt5_module=_load_mt5(), pin_dir=hello.get("pin_dir"), guard=guard)  # type: ignore[arg-type]
    if hello.get("risk") is not None:
        broker.set_risk_config(hello["risk"])
    server = _Server(broker, out)
    loop = asyncio.get_running_loop()
    inbox: asyncio.Queue[Any] = asyncio.Queue()

    wait = pipe_waiter(stdin.fileno())

    def reader() -> None:
        try:
            while True:
                msg = read_frame(stdin, wait)
                if msg is None:
                    break
                loop.call_soon_threadsafe(inbox.put_nowait, msg)
        except Exception:  # noqa: BLE001
            logger.exception("reading from the bot failed")
        # stdin ended: the bot is gone (or closed the pipe). Never outlive it, whatever the loop is doing.
        logging.shutdown()
        os._exit(0)

    threading.Thread(target=reader, name="from-bot", daemon=True).start()
    server.send(("ready", {"pid": os.getpid(), "version": PROTOCOL, "python": sys.version.split()[0]}))
    await server.serve(inbox)


def main() -> int:
    stdin = os.fdopen(os.dup(0), "rb", buffering=0)     # (also under pythonw.exe, whose sys.stdin may be None)
    out_fd = os.dup(1)                    # the protocol's own copy of stdout ...
    os.dup2(2, 1)                         # ... and fd 1 is stderr from now on: nothing printed reaches the pipe
    if sys.stderr is None:
        sys.stderr = os.fdopen(2, "w", encoding="utf-8", errors="replace", buffering=1)
    sys.stdout = sys.stderr
    _no_inherit(0, 1, 2, out_fd)
    out = os.fdopen(out_fd, "wb", buffering=0)
    hello = read_frame(stdin, pipe_waiter(stdin.fileno()))
    if not isinstance(hello, tuple) or hello[0] != "hello":
        return 2
    info = hello[1]
    logging.basicConfig(stream=sys.stderr, level=str(info.get("log_level") or "INFO").upper(),
                        format="%(levelname)s %(name)s: %(message)s")
    if int(info.get("version") or 0) != PROTOCOL:
        write_frame(out, ("refused", f"protokol sürümü uyuşmuyor ({info.get('version')} != {PROTOCOL})"))
        return 2
    try:
        asyncio.run(_run(info, stdin, out))
    except Exception as exc:  # noqa: BLE001
        logger.exception("helper failed")
        with contextlib.suppress(Exception):
            write_frame(out, ("refused", f"yardımcı süreç başlatılamadı: {exc}"))
        return 1
    logging.shutdown()
    os._exit(0)                           # the reader thread may still block on stdin


if __name__ == "__main__":
    sys.exit(main())
