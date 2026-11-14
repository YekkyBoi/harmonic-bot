"""Live, 2026-11-14 (Windows): the helper's reader thread sat in a blocking read of stdin while the MetaTrader5 package
(numpy) was loaded; numpy's start-up asks Windows about the standard handles, which waits for the pending read, so
the helper hung and the bot said "MT5 yardımcı süreci 170 sn içinde yanıt vermedi". Reproduced on the server with
tools/teshis_mt5.py (B, D hang; A, C fine) and tools/teshis_mt5_2.py (G: numpy hangs; E: a polling reader is fine).
The reader now polls the pipe and reads only what is there, and the package is loaded before the reader starts."""
from __future__ import annotations

import inspect
import io
import struct
import sys

import pytest

from copytrader.brokers import mt5_helper as helper


class Stream:
    """A raw pipe end: read(n) returns what is available (at most n) and FAILS the test if nothing is available,
    i.e. if the caller would have been left in a blocking read."""

    def __init__(self) -> None:
        self.data = b""
        self.reads = 0

    def read(self, size: int) -> bytes:
        assert self.data, "read() while the pipe is empty: this would be a blocking read"
        self.reads += 1
        chunk, self.data = self.data[:size], self.data[size:]
        return chunk


def frame(obj: object) -> bytes:
    return helper.encode_frame(obj)


def test_the_reader_never_reads_an_empty_pipe_and_gets_frames_that_arrive_in_pieces() -> None:
    stream = Stream()
    payload = frame(("call", 7, "get_equity", (), {}))
    arrivals = [payload[:2], payload[2:5], b"", payload[5:9], payload[9:], b""]   # (b"": nothing new this look)
    sleeps: list[float] = []

    def peek() -> int | None:
        if not stream.data and arrivals:
            stream.data += arrivals.pop(0)
        return len(stream.data)

    wait = helper.pipe_waiter(0, peek=peek, sleep=sleeps.append)
    assert helper.read_frame(stream, wait) == ("call", 7, "get_equity", (), {})   # type: ignore[arg-type]
    assert stream.reads >= 4 and sleeps and set(sleeps) == {helper.STDIN_POLL_SEC}


def test_a_broken_pipe_is_the_end_of_the_stream_also_in_the_middle_of_a_frame() -> None:
    stream = Stream()
    payload = frame(("ping", 1))
    states: list[int | None] = [len(payload[:3]), None]
    stream.data = payload[:3]

    def peek() -> int | None:
        return states.pop(0) if states else None

    wait = helper.pipe_waiter(0, peek=peek, sleep=lambda _s: None)
    assert helper.read_frame(stream, wait) is None                               # type: ignore[arg-type]
    assert helper.pipe_waiter(0, peek=lambda: None)() is False                   # the bot is gone: no read at all


def test_without_a_waiter_frames_are_read_as_before() -> None:
    data = frame(("hello", {"version": 1})) + frame(("exit",))
    stream = io.BytesIO(data)
    assert helper.read_frame(stream) == ("hello", {"version": 1})
    assert helper.read_frame(stream) == ("exit",)
    assert helper.read_frame(stream) is None
    assert helper.read_frame(io.BytesIO(struct.pack(">I", 10) + b"abc")) is None   # cut off by the end


@pytest.mark.skipif(sys.platform == "win32", reason="the other platforms' branch")
def test_only_windows_polls() -> None:
    assert helper.pipe_waiter(0) is None            # select()-able pipes: the blocking read harms nothing there


def test_the_package_is_loaded_before_the_reader_thread_and_every_stdin_read_goes_through_the_waiter() -> None:
    run = inspect.getsource(helper._run)
    assert 0 <= run.index("preload_mt5()") < run.index("threading.Thread(target=reader")
    assert "read_frame(stdin, wait)" in run and "read_frame(stdin)" not in run
    main = inspect.getsource(helper.main)
    assert "read_frame(stdin, pipe_waiter(stdin.fileno()))" in main and "read_frame(stdin)" not in main


def test_preload_does_nothing_for_the_test_fake_and_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    loaded: list[str] = []
    monkeypatch.setattr(helper.importlib, "import_module", lambda name: loaded.append(name))
    monkeypatch.setenv(helper.FAKE_ENV, "x:y")
    monkeypatch.setattr(helper.sys, "platform", "win32")
    helper.preload_mt5()
    assert loaded == []
    monkeypatch.delenv(helper.FAKE_ENV)
    helper.preload_mt5()
    assert loaded == ["MetaTrader5"]

    def boom(name: str) -> None:
        raise ImportError(name)

    monkeypatch.setattr(helper.importlib, "import_module", boom)
    helper.preload_mt5()                             # (reported later, by connect())
    monkeypatch.setattr(helper.sys, "platform", "linux")
    monkeypatch.setattr(helper.importlib, "import_module", lambda name: loaded.append("again"))
    helper.preload_mt5()
    assert loaded == ["MetaTrader5"]
