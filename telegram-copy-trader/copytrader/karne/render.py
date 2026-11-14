"""Drawing a card WITHOUT holding the bot: the picture is made by another process.

Drawing is pure CPU work in Pillow: about 3 s and 170 MB for a 1080 px card, 9-12 s and 550-600 MB for a 2160 px one
(the designer's measurement; tests/test_karne_render.py prints this machine's). In the bot's own process that would
stop the event loop - signals, orders, the monitor, the MetaTrader helper's keep-alive - for that long, and a thread
would not help (the drawing code holds the interpreter lock most of the time). So:

* :func:`render_card` writes the card's data to a JSON file, starts ``python -m copytrader.karne`` with the bot's own
  Python (:func:`render_command`; no console window on Windows, below normal priority there and ``nice`` elsewhere:
  on a small server the drawing gives way to the bot), and then only LOOKS at the process every POLL_SEC between
  ``asyncio.sleep`` calls. Starting the process and every file operation run in a worker thread; nothing in here
  waits for the child in a way that holds the loop.
* After ``timeout`` seconds the process is killed (it also ends itself a little later, should the bot be gone). When
  the caller is cancelled (shutdown) it is killed too. A killed process leaves no half picture: it writes to a
  temporary name and renames at the end.
* Scale: 1 (1080 px wide), on purpose - a third of the time and under a third of the memory of 2x on a small VPS
  that also runs the MetaTrader terminals, a file under 1 MB (short upload on the bot's connection), and it is the
  size of the samples the owner chose from. ``copytrader.karne.OUT_SCALE`` is the one place to change it.
* Pillow is NOT installed into the bot's own Python environment: guncelle.py puts it into a folder of its own next
  to the package (:func:`packages_dir`, ``<bot folder>/karne_paketleri``), and only the drawing process gets that
  folder on its PYTHONPATH (:func:`render_command`). The bot's process cannot import it - also after a way back to
  a version that knows nothing of it - so nothing in the bot (Telethon's photo upload) ever starts using it.
  (A Pillow that IS in the bot's environment, e.g. in a Docker image, is used as well: see
  ``telegram.notifier.keep_pillow_out_of_telethon`` for what that needs.)
* Every outcome is a :class:`RenderResult`; this never raises (except the caller's own cancellation). No picture:
  the caller sends the card as text.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import json
import logging
import os
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import OUT_SCALE

logger = logging.getLogger(__name__)

RENDER_TIMEOUT_SEC = 90.0         # one card (about 3 s on a development machine; a busy small server is slower)
POLL_SEC = 0.25                   # how often the bot looks whether the drawing process has ended
CHILD_GRACE_SEC = 10.0            # the process ends itself this long after the bot's limit (if the bot is gone)
MIN_PNG_BYTES = 10_000            # a real card is hundreds of kB: less is not a card
MAX_PNG_BYTES = 8 * 1024 * 1024   # more is not sent (the 1x card is about 0.6 MB)
STDERR_TAIL = 400                 # characters of the process's error text kept for the log / the owner
STDERR_READ = 4000                # bytes read from the END of the process's error file (never the whole file)
PACKAGES_DIR = "karne_paketleri"  # in the bot folder: where guncelle.py installs Pillow (pip --target); same name there
CREATE_NO_WINDOW = 0x08000000             # Windows process creation flags: no console window ...
BELOW_NORMAL_PRIORITY_CLASS = 0x00004000  # ... and a priority below the bot's own
EXIT_NO_PILLOW = 3                # (copytrader.karne.__main__.EXIT_NO_PILLOW; tests keep the two equal)
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"

OK, NO_PILLOW, TIMEOUT, FAILED = "ok", "no_pillow", "timeout", "failed"


@dataclass(frozen=True)
class RenderResult:
    status: str                   # OK | NO_PILLOW | TIMEOUT | FAILED
    path: Path | None = None      # the PNG (OK only); the caller removes it when it is done with it
    detail: str = ""              # why not (English, for the log)
    seconds: float = 0.0          # how long the process ran

    @property
    def ok(self) -> bool:
        return self.status == OK and self.path is not None


def packages_dir() -> Path:
    """The folder next to the package that holds what only the drawing process may import (Pillow)."""
    return Path(__file__).resolve().parents[2] / PACKAGES_DIR


def pillow_installed() -> bool:
    """Could the drawing process import Pillow? A look at the files, never an import: it is in the drawing process's
    own folder (:func:`packages_dir`), or in the bot's Python environment. The drawing process has the last word
    (its exit code EXIT_NO_PILLOW); this look only saves starting a process that could not draw."""
    try:
        if (packages_dir() / "PIL" / "__init__.py").is_file():
            return True
        return importlib.util.find_spec("PIL") is not None
    except Exception:          # noqa: BLE001 - e.g. a module of that name without a spec: let the process decide
        return True


def render_command() -> tuple[list[str], dict[str, str], str]:
    """(argv without the card's arguments, environment, working folder) of the drawing process: the bot's own Python
    and the bot's own copytrader package (as for the MetaTrader helper), plus - for this process only - the folder
    Pillow is installed into (:func:`packages_dir`)."""
    package_root = str(Path(__file__).resolve().parents[2])
    python = sys.executable or "python"
    exe = Path(python)
    if exe.name.lower() == "pythonw.exe" and (exe.parent / "python.exe").is_file():
        python = str(exe.parent / "python.exe")     # same venv; the console variant (no window: CREATE_NO_WINDOW)
    env = dict(os.environ)
    paths = [package_root, str(packages_dir()), *([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])]
    env["PYTHONPATH"] = os.pathsep.join(paths)
    env.setdefault("PYTHONIOENCODING", "utf-8")
    return [python, "-m", "copytrader.karne"], env, package_root


class _Child:
    """The drawing process and its files; :meth:`start` runs in a worker thread."""

    def __init__(self, folder: Path, command: Callable[[], tuple[list[str], dict[str, str], str]]) -> None:
        folder = Path(os.path.abspath(folder))      # (the process runs in another working folder than the bot)
        token = uuid.uuid4().hex[:12]
        self.data = folder / f"karne_{token}.json"
        self.out = folder / f"karne_{token}.png"
        self.err = folder / f"karne_{token}.err"
        self.command = command
        self.proc: subprocess.Popen[bytes] | None = None
        self.abandoned = False
        self._lock = threading.Lock()
        self._starting = threading.Event()          # start() was entered ...
        self._started = threading.Event()           # ... and has returned (with or without a process)

    def start(self, data: dict[str, Any], scale: int, limit: float) -> None:
        self._starting.set()
        try:
            self._start(data, scale, limit)
        finally:
            self._started.set()

    def _start(self, data: dict[str, Any], scale: int, limit: float) -> None:
        self.data.parent.mkdir(parents=True, exist_ok=True)
        self.data.write_text(json.dumps(data, ensure_ascii=False, allow_nan=False), encoding="utf-8")
        argv, env, cwd = self.command()
        flags = (CREATE_NO_WINDOW | BELOW_NORMAL_PRIORITY_CLASS) if sys.platform == "win32" else 0
        with open(self.err, "wb") as err:           # (a file, not a pipe: nothing to drain, nothing can fill up)
            proc = subprocess.Popen(                # noqa: S603 - our own Python and module
                [*argv, str(self.data), str(self.out), "--scale", str(scale), "--limit", f"{limit:.0f}"],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=err, cwd=cwd, env=env,
                close_fds=True, creationflags=flags)
        with self._lock:
            self.proc = proc
            abandoned = self.abandoned
        if abandoned:                               # the caller was cancelled while this thread was starting it
            self.kill()

    def kill(self) -> None:
        """End the process now (never waits on the event loop: a daemon thread collects it)."""
        with self._lock:
            self.abandoned = True
            proc = self.proc
        if proc is None or proc.poll() is not None:
            return
        with contextlib.suppress(Exception):
            proc.kill()
        threading.Thread(target=_collect, args=(proc,), name="karne-collect", daemon=True).start()

    def error_text(self) -> str:
        """The end of what the process wrote to stderr (at most STDERR_READ bytes are read, however much it wrote)."""
        try:
            with open(self.err, "rb") as handle:
                handle.seek(0, os.SEEK_END)
                handle.seek(max(0, handle.tell() - STDERR_READ))
                text = handle.read(STDERR_READ).decode("utf-8", "replace").strip()
        except OSError:
            return ""
        return " | ".join(line.strip() for line in text.splitlines() if line.strip())[-STDERR_TAIL:]

    def picture_problem(self) -> str | None:
        """None when ``out`` is a whole PNG of a believable size."""
        try:
            size = self.out.stat().st_size
            with open(self.out, "rb") as handle:
                head = handle.read(len(_PNG_SIGNATURE))
        except OSError as exc:
            return f"no picture was written ({exc})"
        if head != _PNG_SIGNATURE:
            return "the file written is not a PNG"
        if not MIN_PNG_BYTES <= size <= MAX_PNG_BYTES:
            return f"the picture has an unbelievable size ({size} bytes)"
        return None

    def clean(self, *, picture: bool) -> None:
        for path in (self.data, self.err, Path(f"{self.out}.part.png"), *((self.out,) if picture else ())):
            with contextlib.suppress(OSError):
                path.unlink()


def _collect(proc: subprocess.Popen[bytes]) -> None:
    with contextlib.suppress(Exception):
        proc.wait(timeout=30)


async def render_card(
    data: dict[str, Any],
    folder: str | os.PathLike[str],
    *,
    scale: int = OUT_SCALE,
    timeout: float = RENDER_TIMEOUT_SEC,
    command: Callable[[], tuple[list[str], dict[str, str], str]] = render_command,
    poll_sec: float = POLL_SEC,
) -> RenderResult:
    """Draw the card ``data`` (copytrader.karne.data.render_data) into a PNG in ``folder``. See the module text.
    Returns within about ``timeout`` seconds whatever the drawing process does."""
    loop = asyncio.get_running_loop()
    child = _Child(Path(folder), command)
    started = time.monotonic()
    keep_picture = False
    try:
        try:
            await asyncio.to_thread(child.start, data, int(scale), float(timeout) + CHILD_GRACE_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as exc:          # noqa: BLE001 - e.g. the Python executable or the data folder is gone
            return RenderResult(FAILED, detail=f"the drawing process could not be started: {exc!r}")
        proc = child.proc
        assert proc is not None
        deadline = loop.time() + max(0.0, float(timeout))
        while (code := proc.poll()) is None:
            if loop.time() >= deadline:
                child.kill()
                logger.error("Karne: the drawing process (pid %s) did not finish within %.0f s; killed",
                             proc.pid, timeout)
                return RenderResult(TIMEOUT, detail=f"no picture within {timeout:.0f} s",
                                    seconds=time.monotonic() - started)
            await asyncio.sleep(poll_sec)
        seconds = time.monotonic() - started
        if code == EXIT_NO_PILLOW:
            return RenderResult(NO_PILLOW, detail=await asyncio.to_thread(child.error_text), seconds=seconds)
        if code != 0:
            detail = await asyncio.to_thread(child.error_text)
            logger.error("Karne: the drawing process ended with code %s: %s", code, detail)
            head = f"exit code {code}: "         # (the END of what it said is what tells why)
            return RenderResult(FAILED, detail=head + detail[-(STDERR_TAIL - len(head)):], seconds=seconds)
        problem = await asyncio.to_thread(child.picture_problem)
        if problem is not None:
            logger.error("Karne: %s", problem)
            return RenderResult(FAILED, detail=problem, seconds=seconds)
        keep_picture = True
        return RenderResult(OK, path=child.out, seconds=seconds)
    finally:
        child.kill()                      # (no-op when it has ended; a cancelled caller never leaves it running)
        _clean_later(child, picture=not keep_picture)


def _clean_later(child: _Child, *, picture: bool) -> None:
    """Remove the card's temporary files in a daemon thread (also when the caller was cancelled: nothing to await)."""

    def clean() -> None:
        # (a caller cancelled while the worker thread was still starting the process: wait until that thread is
        # through - it kills what it started - so that nothing it creates is left behind)
        if child._starting.is_set() or child._starting.wait(1.0):
            child._started.wait(60.0)
        proc = child.proc
        if proc is not None:
            _collect(proc)                # (Windows cannot remove a file the process still holds)
        child.clean(picture=picture)

    threading.Thread(target=clean, name="karne-clean", daemon=True).start()


def remove_picture(path: Path | None) -> None:
    """Remove a sent (or unsendable) picture; never raises, never waits."""
    if path is None:
        return

    def remove() -> None:
        with contextlib.suppress(OSError):
            Path(path).unlink()

    threading.Thread(target=remove, name="karne-remove", daemon=True).start()


__all__ = ["FAILED", "NO_PILLOW", "OK", "PACKAGES_DIR", "TIMEOUT", "RenderResult", "packages_dir",
           "pillow_installed", "remove_picture", "render_card", "render_command", "RENDER_TIMEOUT_SEC"]
