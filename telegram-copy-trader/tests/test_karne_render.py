"""The report card's picture is drawn by ANOTHER process (copytrader/karne/render.py + __main__.py + design.py).

Part 1 needs no Pillow: the bot's side with stand-in drawing processes (one that hangs, one that crashes, one that
burns CPU...), the real process without Pillow, and the event loop's lag measured while a process draws.
Part 2 needs Pillow (skipped cleanly without it; Pillow is optional and is NOT installed into the bot's test
environments: put it on PYTHONPATH from a folder of its own): the real picture, compared pixel for pixel with the
designer's reference render of the design the owner approved (tests/karne_final_positive.png).
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from copytrader import karne
from copytrader.karne import __main__ as child_main
from copytrader.karne import render
from copytrader.karne.render import FAILED, NO_PILLOW, OK, TIMEOUT, RenderResult, render_card, render_command

HERE = Path(__file__).resolve().parent
PACKAGE = HERE.parent / "copytrader" / "karne"
REFERENCE = HERE / "karne_final_positive.png"
HAS_PILLOW = importlib.util.find_spec("PIL") is not None
needs_pillow = pytest.mark.skipif(not HAS_PILLOW, reason="Pillow is not installed (optional: the card's picture)")
FAKE_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 20_000        # passes the bot's look at the file (not a real picture)
CARD = {"title": "BABÜR'S TRADE PERFORMANCE", "subtitle": "HAFTALIK KARNE · 8–15 Kasım 2026", "footer": "",
        "sample": False, "accounts": [{"name": "binance", "r": 1.0, "approx": False}],
        "trades": [{"date": "2026-11-09", "pair": "ETH", "side": "LONG", "r": 1.0, "outcome": "win", "note": "TP1",
                    "approx": False}]}


def stand_in(tmp_path: Path, body: str) -> Any:
    """A drawing process that runs ``body`` instead (argv as for the real one: data, out, --scale N, --limit S);
    it writes its pid next to its script first."""
    script = tmp_path / "stand_in.py"
    script.write_text("import os, sys, time\nfrom pathlib import Path\n"
                      "Path(__file__).with_suffix('.pid').write_text(str(os.getpid()))\n"
                      "data, out = sys.argv[1], sys.argv[2]\n" + textwrap.dedent(body), encoding="utf-8")
    return lambda: ([sys.executable, "-B", str(script)], dict(os.environ), str(tmp_path))


def pid_of(tmp_path: Path) -> int:
    return int((tmp_path / "stand_in.pid").read_text())


async def gone(pid: int, within: float = 5.0) -> bool:
    """The process ``pid`` has ended and was collected (no zombie left)."""
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            return False
        await asyncio.sleep(0.05)
    return False


async def files_left(folder: Path, expected: list[str] | None = None, within: float = 5.0) -> list[str]:
    """What is in the card's folder once the cleaning thread is done (it is given ``within`` seconds to get to
    ``expected``, by default nothing)."""
    expected = expected or []
    deadline = time.monotonic() + within
    while True:
        names = sorted(p.name for p in folder.glob("*")) if folder.is_dir() else []
        if names == expected or time.monotonic() >= deadline:
            return names
        await asyncio.sleep(0.05)


class Lag:
    """How late the event loop wakes a 10 ms sleeper while something else is going on."""

    def __init__(self) -> None:
        self.worst, self.ticks, self._task = 0.0, 0, None

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            due = loop.time() + 0.01
            await asyncio.sleep(0.01)
            self.worst = max(self.worst, loop.time() - due)
            self.ticks += 1

    async def __aenter__(self) -> "Lag":
        self._task = asyncio.ensure_future(self._run())
        await asyncio.sleep(0)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)


# =========================================================================== #
# Part 1: the bot's side (no Pillow needed)
# =========================================================================== #
async def test_a_drawing_process_that_hangs_is_killed_at_the_time_limit(tmp_path: Path) -> None:
    command = stand_in(tmp_path, "time.sleep(600)")
    started = time.monotonic()
    async with Lag() as lag:
        result = await render_card(CARD, tmp_path / "karne", timeout=1.0, command=command, poll_sec=0.05)
    took = time.monotonic() - started
    assert result.status == TIMEOUT and result.path is None and not result.ok
    assert 0.9 < took < 3.0, took                                  # back at the limit, not when the process ends
    assert await gone(pid_of(tmp_path))                            # killed and collected
    assert await files_left(tmp_path / "karne") == []              # no data file, no half picture left
    assert lag.ticks > 50 and lag.worst < 0.25, (lag.ticks, lag.worst)


async def test_a_drawing_process_that_crashes_is_a_failure_with_its_reason(tmp_path: Path) -> None:
    command = stand_in(tmp_path, "raise RuntimeError('font dosyası bozuk')")
    result = await render_card(CARD, tmp_path / "karne", command=command, poll_sec=0.05)
    assert result.status == FAILED and result.path is None
    assert "exit code 1" in result.detail and "RuntimeError: font dosyası bozuk" in result.detail
    assert await files_left(tmp_path / "karne") == []


@pytest.mark.parametrize("body, problem", [
    ("pass", "no picture was written"),
    ("Path(out).write_bytes(b'x' * 50_000)", "not a PNG"),
    ("Path(out).write_bytes(b'\\x89PNG\\r\\n\\x1a\\n' + b'x' * 100)", "unbelievable size"),
    ("Path(out).write_bytes(b'\\x89PNG\\r\\n\\x1a\\n' + b'x' * 9_000_000)", "unbelievable size"),
])
async def test_a_process_that_ends_well_without_a_believable_picture_is_a_failure(tmp_path: Path, body: str,
                                                                                 problem: str) -> None:
    result = await render_card(CARD, tmp_path / "karne", command=stand_in(tmp_path, body), poll_sec=0.05)
    assert result.status == FAILED and problem in result.detail and result.path is None
    assert await files_left(tmp_path / "karne") == []


async def test_a_process_that_cannot_be_started_is_a_failure_not_an_exception(tmp_path: Path) -> None:
    result = await render_card(CARD, tmp_path / "karne", poll_sec=0.05,
                               command=lambda: ([str(tmp_path / "no-such-python"), "-m", "x"], dict(os.environ),
                                                str(tmp_path)))
    assert result.status == FAILED and "could not be started" in result.detail
    assert await files_left(tmp_path / "karne") == []


async def test_the_real_process_without_pillow_says_so(tmp_path: Path) -> None:
    """The bot's own command, with a PIL that cannot be imported in front of whatever this machine has."""
    blocker = tmp_path / "no_pillow" / "PIL"
    blocker.mkdir(parents=True)
    (blocker / "__init__.py").write_text("raise ImportError('No module named PIL (hidden for this test)')\n",
                                         encoding="utf-8")

    def command() -> tuple[list[str], dict[str, str], str]:
        argv, env, cwd = render_command()
        env["PYTHONPATH"] = str(blocker.parent) + os.pathsep + env["PYTHONPATH"]
        return argv, env, cwd

    result = await render_card(CARD, tmp_path / "karne", command=command, poll_sec=0.05)
    assert result.status == NO_PILLOW and result.path is None and not result.ok
    assert "Pillow is not installed" in result.detail
    assert await files_left(tmp_path / "karne") == []


async def test_cancelling_the_caller_kills_the_drawing_process(tmp_path: Path) -> None:
    command = stand_in(tmp_path, "time.sleep(600)")
    task = asyncio.ensure_future(render_card(CARD, tmp_path / "karne", timeout=60.0, command=command, poll_sec=0.05))
    deadline = time.monotonic() + 10.0
    while not (tmp_path / "stand_in.pid").exists() and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    pid = pid_of(tmp_path)
    task.cancel()                                                  # the bot is shutting down
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await gone(pid)
    assert await files_left(tmp_path / "karne") == []


async def test_the_event_loop_keeps_its_pace_while_a_process_draws(tmp_path: Path) -> None:
    """A process that does what drawing does to a machine - 1.5 s of pure CPU - while the loop serves a 10 ms timer:
    the bot's signals, orders and keep-alives are that timer."""
    command = stand_in(tmp_path, f"""
        end = time.perf_counter() + 1.5
        n = 0
        while time.perf_counter() < end:
            n += sum(i * i for i in range(2000))
        Path(out).write_bytes({FAKE_PNG!r})
        """)
    async with Lag() as lag:
        result = await render_card(CARD, tmp_path / "karne", command=command)
    assert result.status == OK and result.ok and result.path is not None and result.seconds >= 1.5
    assert result.path.read_bytes() == FAKE_PNG
    assert lag.ticks > 100 and lag.worst < 0.25, (lag.ticks, lag.worst)
    # the picture only is left (it is the caller's now), until the caller has sent it
    assert await files_left(tmp_path / "karne", [result.path.name]) == [result.path.name]
    render.remove_picture(result.path)
    render.remove_picture(None)
    assert await files_left(tmp_path / "karne") == []


async def test_the_card_reaches_the_process_as_json_with_scale_and_limit(tmp_path: Path) -> None:
    command = stand_in(tmp_path, f"""
        import json
        card = json.load(open(data, encoding="utf-8"))
        Path(__file__).with_suffix(".seen").write_text(json.dumps([card, sys.argv[3:]], ensure_ascii=False),
                                                       encoding="utf-8")
        Path(out).write_bytes({FAKE_PNG!r})
        """)
    result = await render_card(CARD, tmp_path / "karne", command=command, poll_sec=0.05, timeout=40.0)
    assert result.ok
    card, args = json.loads((tmp_path / "stand_in.seen").read_text(encoding="utf-8"))
    assert card == CARD and args == ["--scale", "1", "--limit", "50"]          # (its own limit: the bot's + 10 s)
    assert karne.OUT_SCALE == 1


async def test_a_data_folder_given_relative_to_the_bots_working_folder_still_works(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``data_dir`` may be relative ("data"); the drawing process runs in another working folder than the bot."""
    work = tmp_path / "bot"
    work.mkdir()
    monkeypatch.chdir(work)
    other = tmp_path / "elsewhere"
    other.mkdir()
    made = stand_in(tmp_path, f"Path(out).write_bytes({FAKE_PNG!r})")
    result = await render_card(CARD, Path("data") / "karne", poll_sec=0.05,
                               command=lambda: (made()[0], dict(os.environ), str(other)))
    assert result.ok and result.path is not None and result.path.is_absolute()
    assert result.path.parent == work / "data" / "karne" and result.path.read_bytes() == FAKE_PNG
    assert not (other / "data").exists()


def test_the_process_is_started_without_a_window_and_below_the_bots_priority_on_windows(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    class FakePopen:
        pid = 4242

        def __init__(self, argv: list[str], **kw: Any) -> None:
            seen.update(argv=argv, **kw)

        def poll(self) -> int:
            return 0

    monkeypatch.setattr(render.subprocess, "Popen", FakePopen)
    monkeypatch.setattr(render.sys, "platform", "win32")
    child = render._Child(tmp_path / "karne", render_command)
    child.start(CARD, 1, 100.0)
    assert seen["creationflags"] == 0x08000000 | 0x00004000                    # CREATE_NO_WINDOW | BELOW_NORMAL
    assert seen["stdin"] == subprocess.DEVNULL and seen["stdout"] == subprocess.DEVNULL       # no pipe to fill up
    assert seen["argv"][1:3] == ["-m", "copytrader.karne"] and seen["argv"][-4:] == ["--scale", "1", "--limit", "100"]
    paths = seen["env"]["PYTHONPATH"].split(os.pathsep)
    assert paths[0] == str(HERE.parent)                                         # the bot's own package ...
    assert paths[1] == str(HERE.parent / "karne_paketleri") == str(render.packages_dir())   # ... and ITS Pillow folder
    assert seen["cwd"] == str(HERE.parent)
    monkeypatch.setattr(render.sys, "platform", "linux")
    child.start(CARD, 1, 100.0)
    assert seen["creationflags"] == 0


def test_the_bots_own_python_draws_never_the_windowless_one(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    scripts = tmp_path / "venv" / "Scripts"
    scripts.mkdir(parents=True)
    (scripts / "pythonw.exe").write_bytes(b"MZ")
    (scripts / "python.exe").write_bytes(b"MZ")
    monkeypatch.setattr(render.sys, "executable", str(scripts / "pythonw.exe"))
    assert render_command()[0] == [str(scripts / "python.exe"), "-m", "copytrader.karne"]
    monkeypatch.setattr(render.sys, "executable", str(scripts / "python.exe"))
    assert render_command()[0][0] == str(scripts / "python.exe")


def run_child(*args: str, env: dict[str, str] | None = None, timeout: float = 60.0) -> subprocess.CompletedProcess[str]:
    argv, base, cwd = render_command()
    return subprocess.run([*argv, *args], cwd=cwd, env={**base, **(env or {})}, capture_output=True, text=True,
                          timeout=timeout, encoding="utf-8", errors="replace")


def test_the_drawing_process_refuses_what_is_not_a_card(tmp_path: Path) -> None:
    out = tmp_path / "card.png"
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    result = run_child(str(bad), str(out))
    assert result.returncode == child_main.EXIT_INPUT == 2 and "cannot read the card data" in result.stderr
    assert run_child(str(tmp_path / "missing.json"), str(out)).returncode == 2
    listed = tmp_path / "list.json"
    listed.write_text("[1, 2]", encoding="utf-8")
    assert run_child(str(listed), str(out)).returncode == 2
    good = tmp_path / "card.json"
    good.write_text(json.dumps(CARD), encoding="utf-8")
    assert run_child(str(good), str(out), "--scale", "9").returncode == 2      # only 1 or 2: never a huge canvas
    assert not out.exists() and not list(tmp_path.glob("*.part.png"))


def test_the_drawing_process_ends_itself_when_its_own_limit_runs_out(tmp_path: Path) -> None:
    argv, env, cwd = render_command()
    started = time.monotonic()
    result = subprocess.run([argv[0], "-B", "-c", "import time\nfrom copytrader.karne.__main__ import _own_limit\n"
                             "_own_limit(1.0)\ntime.sleep(60)\n"], cwd=cwd, env=env, timeout=30)
    assert result.returncode == child_main.EXIT_TIMEOUT == 4 and time.monotonic() - started < 10


def test_the_look_whether_pillow_is_there_imports_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    before = "PIL" in sys.modules
    assert render.pillow_installed() is HAS_PILLOW
    assert ("PIL" in sys.modules) is before
    monkeypatch.setattr(render.importlib.util, "find_spec", lambda name: None)
    assert render.pillow_installed() is False

    def broken(name: str) -> None:
        raise ValueError(f"{name}.__spec__ is None")

    monkeypatch.setattr(render.importlib.util, "find_spec", broken)
    assert render.pillow_installed() is True                     # not knowable: the drawing process decides


def test_pillow_is_looked_for_in_the_drawing_process_own_folder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """(Isolation review, P1.) Pillow is installed into ``<bot folder>/karne_paketleri``, a folder the bot's own
    process never imports from; only the drawing process gets it on its path."""
    monkeypatch.setattr(render.importlib.util, "find_spec", lambda name: None)     # none in the bot's environment
    monkeypatch.setattr(render, "packages_dir", lambda: tmp_path / "karne_paketleri")
    assert render.pillow_installed() is False
    (tmp_path / "karne_paketleri" / "PIL").mkdir(parents=True)
    assert render.pillow_installed() is False                                      # (a folder is not a package yet)
    (tmp_path / "karne_paketleri" / "PIL" / "__init__.py").write_text("", encoding="utf-8")
    assert render.pillow_installed() is True
    argv, env, _cwd = render_command()
    assert env["PYTHONPATH"].split(os.pathsep)[:2] == [str(HERE.parent), str(tmp_path / "karne_paketleri")]
    assert str(tmp_path / "karne_paketleri") not in sys.path and argv[1:] == ["-m", "copytrader.karne"]


def bot_folder(tmp_path: Path, pillow_from: Path | None) -> Path:
    """A bot folder as on the server: the package, and ``karne_paketleri`` holding Pillow (``pillow_from``: the
    folder a real Pillow is in; None: a stand-in package that only proves where it would be imported from)."""
    import shutil

    bot = tmp_path / "bot"
    shutil.copytree(HERE.parent / "copytrader", bot / "copytrader", ignore=shutil.ignore_patterns("__pycache__"))
    target = bot / "karne_paketleri"
    target.mkdir()
    if pillow_from is None:
        (target / "PIL").mkdir()
        (target / "PIL" / "__init__.py").write_text("__version__ = 'stand-in'\n", encoding="utf-8")
    else:
        for name in ("PIL", "pillow.libs"):
            if (pillow_from / name).is_dir():
                shutil.copytree(pillow_from / name, target / name, ignore=shutil.ignore_patterns("__pycache__"))
    return bot


def env_without_pillow(bot: Path) -> dict[str, str]:
    """The environment of the bot's own process on the server: its folder on the path, no Pillow of this test run."""
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    return {**env, "PYTHONPATH": str(bot), "PYTHONDONTWRITEBYTECODE": "1"}


def pillow_free_interpreter() -> bool:
    """This interpreter itself (its site-packages) has no Pillow; one on PYTHONPATH can be left out for a test."""
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    probe = subprocess.run([sys.executable, "-B", "-c", "import importlib.util as u, sys\n"
                            "sys.exit(0 if u.find_spec('PIL') is None else 1)"], env=env, timeout=60)
    return probe.returncode == 0


def test_the_bots_own_process_cannot_import_the_pillow_in_that_folder(tmp_path: Path) -> None:
    if not pillow_free_interpreter():
        pytest.skip("this interpreter has a Pillow of its own in site-packages")
    bot = bot_folder(tmp_path, None)
    code = ("import importlib.util, os, sys\n"
            "import copytrader.app, copytrader.telegram.notifier, copytrader.karne.service\n"
            "from copytrader.karne import render\n"
            "assert importlib.util.find_spec('PIL') is None and 'PIL' not in sys.modules      # not for the bot\n"
            "assert render.pillow_installed()                                               # ... but it is there\n"
            "argv, env, cwd = render.render_command()\n"
            "assert env['PYTHONPATH'].split(os.pathsep)[1] == str(render.packages_dir())\n"
            "import subprocess\n"
            "seen = subprocess.run([argv[0], '-B', '-c', 'import PIL; print(PIL.__file__)'], env=env, cwd=cwd,\n"
            "                      capture_output=True, text=True)\n"
            "print(seen.stdout.strip())\n")
    result = subprocess.run([sys.executable, "-B", "-c", code], cwd=bot, env=env_without_pillow(bot),
                            capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    assert Path(result.stdout.strip()) == bot / "karne_paketleri" / "PIL" / "__init__.py"   # the drawing process: yes


def test_the_error_text_of_a_process_is_read_from_the_end_of_its_file_only(tmp_path: Path) -> None:
    """(Isolation review.) A process that wrote 200 MB to stderr raised the bot's memory by 200 MB."""
    import tracemalloc

    child = render._Child(tmp_path, render_command)
    with open(child.err, "wb") as handle:
        for _ in range(64):
            handle.write(b"x" * (1024 * 1024 - 1) + b"\n")                         # 64 MB of noise
        handle.write("Traceback...\nOSError: yazı tipi açılamadı\n".encode())
    tracemalloc.start()
    text = child.error_text()
    _now, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert text.endswith("OSError: yazı tipi açılamadı") and len(text) <= render.STDERR_TAIL
    assert peak < 1024 * 1024, peak                                                 # not the file's 64 MB
    child.err.unlink()
    assert child.error_text() == ""


async def test_a_process_that_floods_stderr_and_fails_gives_only_its_last_words(tmp_path: Path) -> None:
    command = stand_in(tmp_path, """
        sys.stderr.write("gürültü\\n" * 400000)
        sys.stderr.write("RuntimeError: son söz\\n")
        sys.exit(1)
        """)
    result = await render_card(CARD, tmp_path / "karne", command=command, poll_sec=0.05)
    assert result.status == FAILED and result.detail.endswith("RuntimeError: son söz")
    assert len(result.detail) <= render.STDERR_TAIL and await files_left(tmp_path / "karne") == []


async def test_a_caller_cancelled_while_the_process_is_still_being_started_leaves_nothing_behind(tmp_path: Path) -> None:
    """(Isolation review.) Cancelled while the worker thread was starting the process: its error file stayed."""
    made = stand_in(tmp_path, "time.sleep(600)")

    def slow_command() -> tuple[list[str], dict[str, str], str]:
        time.sleep(0.5)                                            # (in the worker thread: the start takes a while)
        return made()

    task = asyncio.ensure_future(render_card(CARD, tmp_path / "karne", timeout=60.0, command=slow_command))
    await asyncio.sleep(0.1)                                       # the thread is in the middle of starting it
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await files_left(tmp_path / "karne", within=10.0) == []     # no .json, no .err
    # what the thread started after all was killed at once (usually before it could even write its pid)
    await asyncio.sleep(0.7)
    if (tmp_path / "stand_in.pid").exists():
        assert await gone(pid_of(tmp_path))
    assert not [t for t in threading.enumerate() if t.name == "karne-clean" and t.is_alive()]


def test_the_parts_agree_on_their_constants() -> None:
    assert render.EXIT_NO_PILLOW == child_main.EXIT_NO_PILLOW == 3
    updater = (HERE.parent / "guncelle.py").read_text(encoding="utf-8")
    assert f'PILLOW = "{karne.PILLOW}"' in updater and karne.PILLOW == "pillow==13.0.0"
    requirements = [line.split("#", 1)[0].strip().lower() for line in
                    (HERE.parent / "requirements.txt").read_text(encoding="utf-8").splitlines()]
    assert not any(line.startswith("pillow") for line in requirements)          # optional: never a requirement
    assert RenderResult(OK).ok is False                                          # (no picture, not ok)


def test_the_bot_itself_never_imports_the_drawing_code_or_uses_pillow() -> None:
    """In the bot's process: the karne service is importable without Pillow, never loads the drawing module, and
    Telethon's own use of Pillow (its photo resizing, on the event loop) is switched off whether or not Pillow is
    installed."""
    argv, env, cwd = render_command()
    code = ("import sys\nimport copytrader.app, copytrader.karne.service, copytrader.karne.data\n"
            "import copytrader.telegram.notifier\nfrom telethon.client import uploads\n"
            "assert 'copytrader.karne.design' not in sys.modules\n"
            "assert uploads.PIL is None, uploads.PIL\n"
            "assert uploads._resize_photo_if_needed('x.png', True) == 'x.png'\n"
            "print('PIL loaded:', 'PIL' in sys.modules)\n")
    result = subprocess.run([argv[0], "-B", "-c", code], cwd=cwd, env=env, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == f"PIL loaded: {HAS_PILLOW}"                  # (Telethon imports it when it is there)


def test_the_shipped_fonts_and_their_licences() -> None:
    fonts = sorted(p.name for p in (PACKAGE / "fonts").iterdir())
    assert fonts == ["CormorantGaramond_600SemiBold.ttf", "CormorantGaramond_600SemiBold_Italic.ttf",
                     "Inter_500Medium.ttf", "Montserrat_400Regular.ttf", "Montserrat_500Medium.ttf",
                     "Montserrat_600SemiBold.ttf", "OFL-CormorantGaramond.txt", "OFL-Inter.txt", "OFL-Montserrat.txt"]
    for name in fonts:
        data = (PACKAGE / "fonts" / name).read_bytes()
        if name.endswith(".ttf"):
            assert data[:4] == b"\x00\x01\x00\x00" and len(data) > 100_000, name         # a TrueType file, whole
        else:
            assert b"SIL OPEN FONT LICENSE Version 1.1" in data, name


# =========================================================================== #
# Part 2: the real picture (needs Pillow)
# =========================================================================== #
def pixels(path: Path) -> Any:
    from PIL import Image

    with Image.open(path) as image:
        return image.convert("RGB").copy()


def difference_box(a: Any, b: Any) -> tuple[int, int, int, int] | None:
    from PIL import ImageChops

    return ImageChops.difference(a, b).getbbox()


@pytest.fixture(scope="module")
def design() -> Any:
    from copytrader.karne import design as module

    return module


@needs_pillow
async def test_the_bot_draws_the_design_the_owner_approved_pixel_for_pixel(tmp_path: Path, design: Any) -> None:
    """The designer's sample data, drawn by the bot's own drawing process with the bot's one preset, equals the
    designer's reference render of the final design (preset 25) in every pixel."""
    assert karne.PRESET == design.FINAL_PRESET == 25 and karne.TABLE_ROWS == design.MAX_ROWS == 20
    started = time.monotonic()
    async with Lag() as lag:
        result = await render_card(design.sample_data(), tmp_path / "karne")
    took = time.monotonic() - started
    assert result.ok and result.path is not None, result
    drawn, reference = pixels(result.path), pixels(REFERENCE)
    assert drawn.size == reference.size == (1080, 1728)
    assert difference_box(drawn, reference) is None
    size = result.path.stat().st_size
    assert 200_000 < size < 2_000_000
    print(f"\nkarne: drawn in {took:.1f} s by the other process; picture {size / 1024:.0f} kB; "
          f"worst event-loop lag meanwhile {lag.worst * 1000:.0f} ms over {lag.ticks} ticks")
    assert lag.ticks > 50 and lag.worst < 0.25, (lag.ticks, lag.worst)          # the loop never waited for it
    assert await files_left(tmp_path / "karne", [result.path.name]) == [result.path.name]


@needs_pillow
def test_a_real_card_never_shows_the_sample_mark(tmp_path: Path, design: Any) -> None:
    """"ÖRNEK VERİ" and the background chart's price labels are drawn for the designer's sample data only
    (``sample``): the bot's data says false, and then the mark's place at the top right is plain background and the
    chart has its faint lines but no numbers; the figures of the card are the same."""
    sample, real = design.sample_data(), dict(design.sample_data(), sample=False)
    design.render(sample, karne.PRESET, str(tmp_path / "sample.png"))
    design.render(real, karne.PRESET, str(tmp_path / "real.png"))
    box = difference_box(pixels(tmp_path / "sample.png"), pixels(tmp_path / "real.png"))
    assert box is not None and box[1] > 40 and box[3] < 900, box                # only the mark and the chart, up top
    missing = dict(real)
    del missing["sample"]                                                        # no key at all: no mark either
    design.render(missing, karne.PRESET, str(tmp_path / "missing.png"))
    assert difference_box(pixels(tmp_path / "real.png"), pixels(tmp_path / "missing.png")) is None


@needs_pillow
def test_the_one_design_loads_only_the_fonts_that_are_shipped(tmp_path: Path, design: Any,
                                                              monkeypatch: pytest.MonkeyPatch) -> None:
    used: set[str] = set()
    plain = design._font.__wrapped__

    def spy(name: str, px: int) -> Any:
        used.add(name)
        return plain(name, px)

    monkeypatch.setattr(design, "_font", spy)
    cards = [design.sample_data(), design.sample_negative(), design.sample_data(25), dict(design.sample_data(), title="KARNE")]
    empty = design.sample_data()
    empty.update(trades=[], accounts=[])
    rough = design.sample_data(12)
    for i, trade in enumerate(rough["trades"]):
        trade["approx"] = i % 2 == 0
        if i == 3:
            trade["r"] = None
    rough["accounts"][0]["approx"], rough["accounts"][1]["r"] = True, None
    for i, card in enumerate([*cards, empty, rough]):
        design.render(card, karne.PRESET, str(tmp_path / f"{i}.png"))
    shipped = {p.stem for p in (PACKAGE / "fonts").glob("*.ttf")}
    assert used == shipped, (used ^ shipped)


@needs_pillow
def test_the_marks_the_bot_adds_exist_in_the_fonts(design: Any) -> None:
    from PIL import Image, ImageDraw, ImageFont

    def drawn(font: Any, text: str) -> bytes:
        image = Image.new("L", (80, 80), 0)
        ImageDraw.Draw(image).text((10, 10), text, font=font, fill=255)
        return image.tobytes()

    for path in sorted((PACKAGE / "fonts").glob("Montserrat_*.ttf")):
        font = ImageFont.truetype(str(path), 40, layout_engine=ImageFont.Layout.BASIC)
        unknown = drawn(font, "\U000F1234")                                      # what a missing glyph looks like
        assert unknown == drawn(font, "\U000F4321")
        for mark in "≈—−…’·%+RİŞÖÜÇĞışöüçğ":
            assert drawn(font, mark) != unknown, (path.name, mark)


def drawn_strings(design: Any, card: dict[str, Any], out: Path, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The chart's price labels: the strings drawn in the chart's own small Inter size (10 px)."""
    seen: list[str] = []
    plain = design.Canvas.text

    def spy(self: Any, x: float, y: float, s: str, *a: Any, **kw: Any) -> Any:
        if a and str(a[0]).startswith("Inter") and a[1] == 10:
            seen.append(s)
        return plain(self, x, y, s, *a, **kw)

    monkeypatch.setattr(design.Canvas, "text", spy)
    design.render(card, karne.PRESET, str(out))
    monkeypatch.setattr(design.Canvas, "text", plain)
    return seen


@needs_pillow
def test_no_real_card_draws_a_made_up_price_label(tmp_path: Path, design: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """(Review, P9.) The chart behind the card is a seeded random series: its axis numbers (125.00 ... 150.00) are
    not data. The designer's sample render keeps them; a card of real results has the faint price lines and no
    number on them - whatever the mode, the length or the sign of the result."""
    labels = drawn_strings(design, design.sample_data(), tmp_path / "s.png", monkeypatch)
    assert {"125.00", "150.00"} <= set(labels)                                  # the designer's sample keeps them
    cards = [dict(design.sample_data(), sample=False), dict(design.sample_negative(), sample=False),
             dict(design.sample_data(25), sample=False), dict(design.sample_data(3), sample=False)]
    empty = dict(design.sample_data(), sample=False, trades=[], accounts=[])
    unknown = dict(design.sample_data(), sample=False)
    unknown["trades"] = [dict(t, r=None) for t in unknown["trades"]]
    for i, card in enumerate([*cards, empty, unknown]):
        assert drawn_strings(design, card, tmp_path / f"{i}.png", monkeypatch) == [], i
    # the bot's own data for a real week
    import datetime as dt

    from copytrader.karne.data import MODE_WEEK, Report, Row, render_data

    end = dt.datetime(2026, 11, 15, 22, 0, tzinfo=dt.timezone.utc)
    row = Row(1, end, "BTC", "LONG", 1.0, False, "TP1", {"binance": (1.0, False)})
    data = render_data(Report(MODE_WEEK, end - dt.timedelta(days=7), end, [row], []))
    assert data["sample"] is False
    assert drawn_strings(design, data, tmp_path / "real.png", monkeypatch) == []


@needs_pillow
def test_a_trade_without_a_known_r_and_an_approximate_one_on_the_picture(tmp_path: Path, design: Any) -> None:
    card = dict(design.sample_data(), sample=False)
    card["trades"] = [dict(t) for t in card["trades"]]
    exact = design.summarize(card)
    assert (exact["unknown"], exact["approx"], exact["n"]) == (0, 0, 9)
    card["trades"][1].update(r=None, outcome="unknown")                          # EURUSD -1.0: not known now
    card["trades"][3]["approx"] = True                                           # XAUUSD +3.0: approximate
    card["accounts"] = [{"name": "binance", "r": 5.2, "approx": True}, {"name": "fx", "r": None},
                        {"name": "tickmill", "r": 2.0}]
    summed = design.summarize(card)
    assert (summed["unknown"], summed["approx"], summed["n"]) == (1, 1, 8)
    assert (summed["wins"], summed["losses"], summed["be"]) == (5, 2, 1)
    assert summed["total_r"] == pytest.approx(6.2 + 1.0) and summed["win_rate"] == pytest.approx(100 * 5 / 7)
    assert summed["best"]["pair"] == "XAUUSD" and summed["best"]["approx"] is True
    assert design._footnotes(summed) == [
        "≈ yaklaşık: 1 işlemde gerçek risk kayıtlı değil; R, hesabın planlanan riskine göre hesaplandı",
        "— R’si bilinmeyen 1 işlem toplama ve kazanma oranına girmedi"]
    assert design._footnotes(exact) == []
    plain = design.render(dict(design.sample_data(), sample=False), karne.PRESET, str(tmp_path / "plain.png"))
    marked = design.render(card, karne.PRESET, str(tmp_path / "marked.png"))
    # two more lines under the table: 11 rows instead of 9 (above 10 rows the design's rows are 46 px, not 54)
    assert marked == (1080, plain[1] + 11 * 46 - 9 * 54)
    labelled = dict(card, hero_label="TOPLAM SONUÇ", table_label="İŞLEMLER", empty_text="Henüz kapanmış işlem yok")
    assert design.render(labelled, karne.PRESET, str(tmp_path / "labelled.png")) == marked
    assert difference_box(pixels(tmp_path / "marked.png"), pixels(tmp_path / "labelled.png")) is not None


@needs_pillow
def test_a_long_period_and_a_bot_without_real_accounts_on_the_picture(tmp_path: Path, design: Any) -> None:
    card = dict(design.sample_data(25), sample=False)
    shown, hidden = design._visible(design.summarize(card)["trades"])
    assert (len(shown), hidden) == (19, 6) and shown == design.summarize(card)["trades"][:19]   # the FIRST rows given
    with_accounts = design.render(card, karne.PRESET, str(tmp_path / "accounts.png"))
    without = design.render(dict(card, accounts=[]), karne.PRESET, str(tmp_path / "none.png"))
    # no "HESAPLAR" section at all: its label (22), its cards (108) and the gap below (46) are not there
    assert without == (1080, with_accounts[1] - 22 - 108 - 46)


@needs_pillow
def test_the_text_card_and_the_picture_write_the_figures_the_same_way(design: Any) -> None:
    from copytrader.karne.data import fmt_r, fmt_rate

    for value in (6.2, -1.0, 0.0, 0.04, -0.04, 0.05, 0.15, 12.349, -0.96, 103.25, -0.05):
        assert fmt_r(value) == design.fmt_r(value), value
    for rate in (0.0, 62.5, 33.3333, 66.6667, 99.5, 100.0):
        assert fmt_rate(rate) == design.fmt_pct(rate), rate


@needs_pillow
async def test_the_real_process_draws_a_real_card_from_the_bots_data(tmp_path: Path) -> None:
    result = await render_card(CARD, tmp_path / "karne")
    assert result.ok and result.path is not None
    image = pixels(result.path)
    assert image.size[0] == 1080 and 1000 < image.size[1] < 2400
    assert result.path.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


@needs_pillow
def test_the_bot_draws_from_its_own_pillow_folder_and_its_process_still_cannot_import_it(tmp_path: Path) -> None:
    """(Isolation review, P1.) A bot folder as on the server - Pillow only in ``karne_paketleri`` - draws the card
    the owner approved, while the bot's own process has no Pillow to import (and so nothing Telethon could use)."""
    if not pillow_free_interpreter():
        pytest.skip("this interpreter has a Pillow of its own in site-packages")
    import PIL

    bot = bot_folder(tmp_path, Path(PIL.__file__).resolve().parent.parent)
    out = tmp_path / "out"
    code = ("import asyncio, importlib.util, json, sys\n"
            "import copytrader.app, copytrader.telegram.notifier\n"
            "from telethon.client import uploads\n"
            "from copytrader.karne import render\n"
            "assert importlib.util.find_spec('PIL') is None and uploads.PIL is None\n"
            "assert render.pillow_installed()\n"
            f"card = json.load(open({str(tmp_path / 'card.json')!r}, encoding='utf-8'))\n"
            f"result = asyncio.run(render.render_card(card, {str(out)!r}))\n"
            "assert result.ok, result\n"
            "assert 'PIL' not in sys.modules\n"
            "print(result.path)\n")
    from copytrader.karne import design

    (tmp_path / "card.json").write_text(json.dumps(design.sample_data(), ensure_ascii=False), encoding="utf-8")
    result = subprocess.run([sys.executable, "-B", "-c", code], cwd=bot, env=env_without_pillow(bot),
                            capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stderr
    assert difference_box(pixels(Path(result.stdout.strip())), pixels(REFERENCE)) is None


@needs_pillow
def test_the_picture_shows_the_given_figures_of_the_whole_period_with_only_the_drawn_rows(tmp_path: Path,
                                                                                         design: Any) -> None:
    """(Isolation review, P2.) Every trade so far can be thousands of rows; the picture draws 19. The bot hands over
    those rows and the period's figures: the card is, pixel for pixel, the one drawn from all the rows."""
    from datetime import UTC, datetime, timedelta

    from copytrader.karne.data import MODE_ALL, MODE_WEEK, Account, Report, Row, render_data

    end = datetime(2026, 11, 15, 22, 0, tzinfo=UTC)
    values = [1.2, -1.0, None, 0.03, 2.9, -1.0, 2.9, 0.5] * 40                  # 320 trades
    rows = [Row(trade_id=i + 1, closed_at=end - timedelta(hours=i + 1), pair=f"C{i % 7}USD", side="LONG" if i % 3 else "SHORT",
                r=value, approx=(value is not None and i % 5 == 0), note="TP1" if value and value > 0 else "",
                accounts={"binance": (value, False), **({"fx": (None, False)} if i % 11 == 0 else {})})
            for i, value in enumerate(values)]
    accounts = [Account("binance", 12.5, True, True), Account("fx", None)]
    report = Report(MODE_ALL, None, end, rows, accounts)
    data = render_data(report)
    assert len(data["trades"]) == 19 and data["hidden"] == 301 and len(json.dumps(data)) < 8000
    summed = design.summarize(data)
    known = [value for value in values if value is not None]
    assert summed["n"] == len(known) == report.wins + report.losses + report.flat
    assert (summed["wins"], summed["losses"], summed["be"]) == (report.wins, report.losses, report.flat)
    assert summed["total_r"] == pytest.approx(sum(known)) and summed["win_rate"] == pytest.approx(report.win_rate)
    assert (summed["unknown"], summed["approx"]) == (report.unknown, report.approximate)
    assert summed["best"]["pair"] == report.best.pair and summed["worst"]["pair"] == report.worst.pair
    # the same card from ALL the rows (the picture adding up by itself; the given sentences kept)
    full = dict(data, trades=[{"date": (end - timedelta(hours=i + 1)).date().isoformat(), "pair": row.pair,
                               "side": row.side, "r": row.r, "outcome": row.outcome, "note": row.note,
                               "approx": row.approx, "partial": row.partial} for i, row in enumerate(rows)])
    del full["aggregates"], full["hidden"]
    full["best"], full["worst"] = data["aggregates"]["best"], data["aggregates"]["worst"]
    size = design.render(data, karne.PRESET, str(tmp_path / "given.png"))
    assert design.render(full, karne.PRESET, str(tmp_path / "full.png")) == size
    assert difference_box(pixels(tmp_path / "given.png"), pixels(tmp_path / "full.png")) is None
    # a week that fits the table: nothing hidden, the same figures
    week = Report(MODE_WEEK, end - timedelta(days=7), end, rows[:20], accounts)
    assert render_data(week)["hidden"] == 0 and len(render_data(week)["trades"]) == 20


@needs_pillow
def test_the_marks_on_the_picture_a_missing_account_and_a_break_even_trade(tmp_path: Path, design: Any) -> None:
    card = dict(design.sample_data(), sample=False)
    card["trades"] = [dict(t) for t in card["trades"]]
    plain = design.render(card, karne.PRESET, str(tmp_path / "plain.png"))
    # P7: a break-even trade at +0.05R is printed exactly like one at 0.0R
    flat = dict(card, trades=[dict(t) for t in card["trades"]])
    flat["trades"][5].update(r=0.05, outcome="be")
    flat["total_r"] = None
    design.render(dict(flat, aggregates={**{k: design.summarize(card)[k] for k in (
        "wins", "losses", "be", "unknown", "approx", "partial", "total_r", "win_rate", "best", "worst")}}),
        karne.PRESET, str(tmp_path / "flat.png"))
    assert difference_box(pixels(tmp_path / "plain.png"), pixels(tmp_path / "flat.png")) is None
    # P2: "*" behind a row and an account figure that leave an account out; one footnote line more
    marked = dict(card, trades=[dict(t) for t in card["trades"]], accounts=[dict(a) for a in card["accounts"]])
    marked["trades"][2]["partial"] = True
    marked["accounts"][1]["partial"] = True
    assert design.summarize(marked)["partial"] == 1
    assert design._footnotes(design.summarize(marked)) == [
        "* 1 işlemde R’si bilinmeyen hesap, işlemin ortalamasına ve kendi toplamına girmedi"]
    size = design.render(marked, karne.PRESET, str(tmp_path / "marked.png"))
    assert size == (1080, plain[1] + 54)
    # next to the same card without the two marks (the same line under the table): only fx's figure (the second
    # account card) and the end of the third row differ
    note = design._footnotes(design.summarize(marked))
    unmarked = dict(card, footnotes=note, aggregates={"partial": 1, **{k: design.summarize(card)[k] for k in (
        "wins", "losses", "be", "unknown", "approx", "total_r", "win_rate", "best", "worst")}})
    assert design.render(unmarked, karne.PRESET, str(tmp_path / "unmarked.png")) == size
    box = difference_box(pixels(tmp_path / "unmarked.png"), pixels(tmp_path / "marked.png"))
    assert box is not None and 380 < box[0] < 700 and 950 < box[1] and box[2] <= 1000 and box[3] < 1340, box
    # given footnotes and the line for the hidden rows are drawn as given
    given = dict(marked, footnotes=["* özel dipnot"], more_text="… ve 7 işlem daha (5 tanesi toplama dahil)", hidden=7)
    assert design._footnotes(design.summarize(given)) == ["* özel dipnot"]
    # (9 rows + the counting line + one footnote = 11 rows: above 10 the design's rows are 46 px, not 54)
    assert design.render(given, karne.PRESET, str(tmp_path / "given.png")) == (1080, plain[1] + 11 * 46 - 9 * 54)
