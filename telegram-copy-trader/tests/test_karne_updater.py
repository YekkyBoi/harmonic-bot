"""The report card and the updater (guncelle.py).

* The new package folder ``copytrader/karne/`` with its BINARY font files reaches the server whole: through the
  downloaded zip and the documented line, into the backup, and back out of it on the way back.
* Pillow is optional (step 5c, like cryptg): looked for, installed with ONE bounded pip run of the pinned version
  when missing, a warning when that does not work; never in requirements.txt, never a reason to stop or undo an update.
* Pillow goes into a folder of its own in the bot folder (``karne_paketleri``, ``pip install --target``), never into
  the bot's Python environment: the bot's own process cannot import it - also not after a way back to a version
  that has no guard against Telethon using it. The updater never touches that folder; a way back needs no uninstall.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import socket
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from test_preflight2_tools import (  # noqa: F401  (g, _frozen_new_tree: fixtures)
    Runner,
    _frozen_new_tree,
    _keep_environment,
    g,
    new_tree,
    old_bot,
    package_files,
    paste,
    release_zip,
)

from copytrader import karne

FONTS = ["CormorantGaramond_600SemiBold.ttf", "CormorantGaramond_600SemiBold_Italic.ttf", "Inter_500Medium.ttf",
         "Montserrat_400Regular.ttf", "Montserrat_500Medium.ttf", "Montserrat_600SemiBold.ttf",
         "OFL-CormorantGaramond.txt", "OFL-Inter.txt", "OFL-Montserrat.txt"]
KARNE_FILES = ["__init__.py", "__main__.py", "data.py", "design.py", "render.py", "service.py", "tradecard.py",
               "tradecard_design.py", "tradecard_service.py", "uktime.py", *[f"fonts/{name}" for name in FONTS]]
PILLOW_CHECK = ("-c import os, sys; k = os.path.abspath('karne_paketleri'); sys.path.insert(0, k); import PIL; "
                "from PIL import Image, ImageDraw, ImageFilter, ImageFont, features; "
                "raise SystemExit(0 if features.check('freetype2') and "
                "os.path.abspath(PIL.__file__).startswith(k + os.sep) else 1)")
PILLOW_PIP = ("-m pip install --disable-pip-version-check --upgrade --no-deps --target karne_paketleri "
              "pillow==13.0.0 --only-binary=:all: --timeout 10 --retries 1")
CRYPTG_PIP = ("-m pip install --disable-pip-version-check cryptg==0.6.1 --only-binary=:all: --timeout 10 "
              "--retries 1")
HERE = Path(__file__).resolve().parent


def digest(folder: Path) -> dict[str, str]:
    return {path.relative_to(folder).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(folder.rglob("*")) if path.is_file() and "__pycache__" not in path.parts}


def run_update(g: Any, bot: Path, runner: Runner, *options: str, tree: Path | None = None,  # noqa: F811
               extra: Any = "both") -> tuple[int, list[tuple[Path, str]]]:
    started: list[tuple[Path, str]] = []

    def start(hedef: Path, python: str) -> int:
        started.append((hedef, python))
        return 130

    code = g.main(["--hedef", str(bot), *options], calistir=runner, surecler=lambda: [], baslat=start,
                  girdi=lambda prompt: "", yeni_agac=tree or new_tree(),
                  ek_paket=g.ek_paketler if extra == "both" else extra)
    return code, started


# --------------------------------------------------------------------------- #
# the files
# --------------------------------------------------------------------------- #
def test_the_release_ships_the_karne_package_with_its_fonts() -> None:
    shipped = digest(new_tree() / "copytrader" / "karne")
    assert sorted(shipped) == sorted(KARNE_FILES)
    source = digest(HERE.parent / "copytrader" / "karne")
    assert shipped == source                                          # (the test tree is a faithful copy)
    for name in FONTS:
        data = (new_tree() / "copytrader" / "karne" / "fonts" / name).read_bytes()
        assert (data[:4] == b"\x00\x01\x00\x00" and b"\x00" in data[100:200]) if name.endswith(".ttf") else (
            b"SIL OPEN FONT LICENSE" in data), name


def test_an_update_installs_the_new_folder_and_its_binary_fonts_byte_for_byte(g: Any, tmp_path: Path) -> None:  # noqa: F811
    bot = old_bot(tmp_path)                                            # an installation from before the report card
    assert not (bot / "copytrader" / "karne").exists()
    code, started = run_update(g, bot, Runner(), extra=None)
    assert code == 130 and len(started) == 1
    assert digest(bot / "copytrader" / "karne") == digest(new_tree() / "copytrader" / "karne")
    assert sorted(digest(bot / "copytrader" / "karne")) == sorted(KARNE_FILES)
    assert package_files(bot / "copytrader") == package_files(new_tree() / "copytrader")
    # the backup of the old version has no such folder, and the way back removes it again (no stale module survives)
    (backup,) = (bot / "yedek").iterdir()
    assert not (backup / "copytrader" / "karne").exists()
    code, _ = run_update(g, bot, Runner(), "--geri-al", extra=None)
    assert code == 130 and not (bot / "copytrader" / "karne").exists()


def test_the_backup_keeps_the_fonts_and_the_way_back_puts_them_back_unchanged(g: Any, tmp_path: Path) -> None:  # noqa: F811
    bot = old_bot(tmp_path)
    run_update(g, bot, Runner(), "--baslatma", extra=None)             # this version is installed now
    installed = digest(bot / "copytrader" / "karne")
    # a later release: one font replaced (other bytes, another size), one removed, a new binary file in a new folder
    later = tmp_path / "sonraki" / "telegram-copy-trader"
    shutil.copytree(new_tree(), later)
    fonts = later / "copytrader" / "karne" / "fonts"
    (fonts / "Inter_500Medium.ttf").write_bytes(b"\x00\x01\x00\x00" + bytes(range(256)) * 40)
    (fonts / "Montserrat_400Regular.ttf").unlink()
    (later / "copytrader" / "karne" / "yeni").mkdir()
    (later / "copytrader" / "karne" / "yeni" / "logo.bin").write_bytes(bytes(reversed(range(256))) * 9)
    (later / "copytrader" / "__init__.py").write_text("__version__ = 'sonraki'\n", encoding="utf-8")
    time.sleep(1.1)                                                     # (backup folders are named by the second)
    code, _ = run_update(g, bot, Runner(), "--baslatma", tree=later, extra=None)
    assert code == 0 and digest(bot / "copytrader" / "karne") == digest(later / "copytrader" / "karne")
    backups = sorted((bot / "yedek").iterdir())
    assert digest(backups[-1] / "copytrader" / "karne") == installed    # the backup of step 3: every font, whole
    time.sleep(1.1)
    code, _ = run_update(g, bot, Runner(), "--geri-al", "--baslatma", extra=None)
    assert code == 0
    assert digest(bot / "copytrader" / "karne") == installed            # replaced font restored, removed one back,
    assert not (bot / "copytrader" / "karne" / "yeni").exists()         # the later release's folder gone


def test_an_update_that_fails_puts_the_fonts_of_the_old_version_back(g: Any, tmp_path: Path) -> None:  # noqa: F811
    bot = old_bot(tmp_path)
    run_update(g, bot, Runner(), "--baslatma", extra=None)
    installed = digest(bot / "copytrader")
    later = tmp_path / "sonraki" / "telegram-copy-trader"
    shutil.copytree(new_tree(), later)
    (later / "copytrader" / "karne" / "fonts" / "Inter_500Medium.ttf").write_bytes(b"\x00\x01\x00\x00bozuk")
    (later / "copytrader" / "__init__.py").write_text("__version__ = 'sonraki'\n", encoding="utf-8")
    time.sleep(1.1)
    runner = Runner({"-m copytrader migrate": [(3, ["config okunamadı"])]})          # step 6 fails
    code, started = run_update(g, bot, runner, tree=later, extra=None)
    assert code == 130 and len(started) == 1                             # the OLD version is started
    assert digest(bot / "copytrader") == installed


def test_the_downloaded_zip_and_the_documented_line_bring_the_fonts_and_only_warn_about_pillow(tmp_path: Path) -> None:
    """The owner's real path: the zip in Downloads, the one line of the guide. The fonts arrive byte for byte; Pillow
    cannot be installed here (no package index): one warning, every step still TAMAM, nothing rolled back."""
    home = tmp_path / "home"
    downloads = home / "Downloads"
    top = "harmonic-bot-claude-telegram-crypto-automation-o6xo7l"
    bot = old_bot(downloads / top)
    release_zip(downloads / f"{top} (1).zip")
    result = paste(home, bot, "--kontrol-yok", "--baslatma")
    out = result.stdout
    assert result.returncode == 0, out + result.stderr
    steps = [line for line in out.splitlines() if re.match(r"\d\. ", line)]
    assert [s[0] for s in steps] == list("123456789") and all(s.endswith(": TAMAM") for s in steps), out
    assert digest(bot / "copytrader" / "karne") == digest(new_tree() / "copytrader" / "karne")
    assert "   Haftalık karne görseli için Pillow paketi kuruluyor (en çok 120 saniye)..." in out
    assert ("   UYARI: karne görseli paketi (Pillow) kurulamadı; güncelleme devam ediyor. Haftalık karne ve /karne "
            "resim yerine metin olarak gelir. Sonra elle kurmak için (botun klasöründe, PowerShell'e olduğu gibi "
            f'yapıştırılabilir): & "{sys.executable}" -m pip install --upgrade --no-deps --only-binary=:all: '
            '--target "karne_paketleri" pillow==13.0.0') in out
    assert out.index("5. ") < out.index("Pillow paketi kuruluyor") < out.index("6. ")
    assert "GERİ YÜKLENDİ" not in out and "GÜNCELLEME YAPILMADI" not in out
    assert not (home / "pip-target").exists() or not any((home / "pip-target").iterdir())     # nothing was installed
    assert not (bot / "karne_paketleri").exists() or not any((bot / "karne_paketleri").iterdir())
    # the installed bot can say what it draws without Pillow: exit code 3 from its own drawing process
    import subprocess

    card = tmp_path / "card.json"
    card.write_text("{}", encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    drawn = subprocess.run([sys.executable, "-B", "-m", "copytrader.karne", str(card), str(tmp_path / "card.png")],
                           cwd=bot, env=env, capture_output=True, text=True, timeout=120)
    try:
        import PIL  # noqa: F401
        in_site_packages = "site-packages" in (PIL.__file__ or "")
    except ImportError:
        in_site_packages = False
    if not in_site_packages:                                              # (a Pillow of this interpreter's own would draw)
        assert drawn.returncode == 3 and "Pillow is not installed" in drawn.stderr


# --------------------------------------------------------------------------- #
# step 5c: Pillow, optional
# --------------------------------------------------------------------------- #
def test_pillow_is_installed_when_missing_after_cryptg_and_before_the_config_is_migrated(
        g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:  # noqa: F811
    bot = old_bot(tmp_path)
    runner = Runner({"-c import cryptg": [(0, [])], PILLOW_CHECK: [(1, ["ModuleNotFoundError: No module named 'PIL'"])],
                     PILLOW_PIP: [(0, ["Successfully installed pillow-13.0.0"])]})
    code, started = run_update(g, bot, runner)
    out = capsys.readouterr().out
    assert runner.calls[1:4] == ["-c import cryptg", PILLOW_CHECK, PILLOW_PIP]       # right after step 5
    assert runner.calls[4] == "-m copytrader migrate"
    assert "   Karne görseli paketi (Pillow) kuruldu." in out and "UYARI" not in out
    assert code == 130 and len(started) == 1
    assert g.PILLOW == karne.PILLOW == "pillow==13.0.0"


def test_pillow_that_cannot_be_installed_is_one_warning_never_a_rollback(
        g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:  # noqa: F811
    bot = old_bot(tmp_path)
    runner = Runner({"-c import cryptg": [(0, [])], PILLOW_CHECK: [(1, [])],
                     PILLOW_PIP: [(1, ["ERROR: Could not find a version that satisfies the requirement pillow==13.0.0",
                                       "ERROR: No matching distribution found for pillow==13.0.0"])]})
    code, started = run_update(g, bot, runner)
    out = capsys.readouterr().out
    assert "   | ERROR: No matching distribution found for pillow==13.0.0" in out
    assert "UYARI: karne görseli paketi (Pillow) kurulamadı; güncelleme devam ediyor." in out
    assert code == 130 and len(started) == 1 and "ESKİ SÜRÜM GERİ YÜKLENDİ" not in out
    assert (bot / "copytrader" / "karne" / "design.py").is_file()                     # the new version stays
    steps = [line for line in out.splitlines() if re.match(r"\d\. ", line)]
    assert all(line.endswith(": TAMAM") or line.startswith("9. ") for line in steps), out
    assert runner.calls.count(PILLOW_PIP) == 1                                        # one run, no second try


def test_pillow_that_is_there_already_runs_no_pip_whatever_its_version(
        g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:  # noqa: F811
    bot = old_bot(tmp_path)
    runner = Runner({"-c import cryptg": [(0, [])], PILLOW_CHECK: [(0, [])]})
    code, _ = run_update(g, bot, runner)
    assert code == 130 and PILLOW_PIP not in runner.calls and runner.calls.count(PILLOW_CHECK) == 1
    assert "Pillow" not in capsys.readouterr().out                                    # (said in the log file only)
    log = next(bot.glob("guncelleme_*.log")).read_text(encoding="utf-8")
    assert "karne görseli paketi (Pillow) zaten kurulu" in log
    runner = Runner()                                                                 # the way back never touches it
    code, _ = run_update(g, bot, runner, "--geri-al")
    assert code == 130 and not any("PIL" in c or "pillow" in c for c in runner.calls)


def test_each_optional_package_is_tried_on_its_own(g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str],  # noqa: F811
                                                    monkeypatch: pytest.MonkeyPatch) -> None:
    # cryptg cannot be installed: Pillow is still tried (and the other way round)
    bot = old_bot(tmp_path / "a")
    runner = Runner({"-c import cryptg": [(1, [])], CRYPTG_PIP: [(1, ["ERROR: no wheel"])],
                     PILLOW_CHECK: [(1, [])], PILLOW_PIP: [(0, [])]})
    code, _ = run_update(g, bot, runner)
    out = capsys.readouterr().out
    assert code == 130 and runner.calls[1:5] == ["-c import cryptg", CRYPTG_PIP, PILLOW_CHECK, PILLOW_PIP]
    assert "UYARI: hızlı şifreleme paketi (cryptg) kurulamadı" in out and "Karne görseli paketi (Pillow) kuruldu." in out

    # a step that raises: said in one line, the next one still runs, the update goes on
    def broken(_komut: Any, _gunluk: Any) -> bool:
        raise OSError("pip bulunamadı")

    monkeypatch.setattr(g, "cryptg_adimi", broken)
    bot = old_bot(tmp_path / "b")
    runner = Runner({PILLOW_CHECK: [(0, [])]})
    code, started = run_update(g, bot, runner)
    out = capsys.readouterr().out
    assert code == 130 and len(started) == 1 and runner.calls[1] == PILLOW_CHECK
    assert "UYARI: isteğe bağlı paket adımı başarısız (pip bulunamadı); güncelleme devam ediyor." in out


def test_the_real_runner_gets_both_optional_steps_and_requirements_do_not_name_pillow(g: Any) -> None:  # noqa: F811
    import inspect

    source = inspect.getsource(g.main)
    assert "ek_paket = ek_paketler if calistir is komut_calistir else None" in source
    assert inspect.getsource(g.ek_paketler).count("cryptg_adimi, pillow_adimi") == 1
    lines = g.gereksinimler(HERE.parent / "requirements.txt")
    assert lines is not None and not any("pillow" in line or "pil" == line for line in lines)
    # so step 5 (the unbounded pip of requirements.txt) does not run because of this release
    before = [line for line in lines]
    assert g.gereksinimler(HERE.parent / "requirements.txt") == before


def test_pillow_with_a_hanging_package_index_is_cut_off_in_time(g: Any, tmp_path: Path,  # noqa: F811
                                                                monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("pip", reason="this interpreter has no pip (the server's venv has; guncelle uses it)")
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(8)                                     # accepts connections and never answers
    port = server.getsockname()[1]
    monkeypatch.setattr(g, "PILLOW_SURESI", 6.0)
    for name in [k for k in os.environ if k.upper().startswith("PIP_")]:
        monkeypatch.delenv(name)                         # hermetic: no pip config, no other index
    monkeypatch.setenv("PIP_CONFIG_FILE", os.devnull)
    monkeypatch.setenv("PIP_INDEX_URL", f"http://127.0.0.1:{port}/simple")
    monkeypatch.setenv("PIP_NO_CACHE_DIR", "1")
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.setenv(name, "127.0.0.1,localhost")
    lines: list[str] = []
    log = SimpleNamespace(yaz=lambda text="": lines.append(text), dosyaya=lambda text: lines.append(text))

    def komut(*parts: str, sure: float = 900.0, ekranda: bool = True) -> tuple[int, list[str]]:
        # (as in the updater: the command runs IN the bot folder, here tmp_path; the step's own --target is used)
        if parts[0] == "-c":
            return 1, ["ModuleNotFoundError"]            # (not installed: whatever this machine has)
        out: list[str] = []
        return g.komut_calistir([sys.executable, *parts], tmp_path, sure, out.append), out   # the REAL pip

    started = time.monotonic()
    try:
        ok = g.pillow_adimi(komut, log)
    finally:
        server.close()
    took = time.monotonic() - started
    assert ok is False and 5.5 < took < 30, took             # stopped by the bound (pip itself would wait on)
    assert "   | 6 saniye içinde bitmedi; durduruldu" in lines
    target = tmp_path / "karne_paketleri"
    assert not target.exists() or not any(target.iterdir())      # nothing half-installed is left in the folder
    assert any("UYARI: karne görseli paketi (Pillow) kurulamadı" in line for line in lines)


def fake_pillow(folder: Path, marker: bytes = b"PIL") -> dict[str, str]:
    """A stand-in for what ``pip install --target`` leaves in the folder: a package with a binary file in it."""
    (folder / "PIL").mkdir(parents=True)
    (folder / "PIL" / "__init__.py").write_text("__version__ = '13.0.0'\n", encoding="utf-8")
    (folder / "PIL" / "_imaging.pyd").write_bytes(marker + bytes(range(256)) * 20)
    (folder / "pillow-13.0.0.dist-info").mkdir()
    (folder / "pillow-13.0.0.dist-info" / "METADATA").write_text("Name: pillow\nVersion: 13.0.0\n", encoding="utf-8")
    return digest(folder)


def test_the_pillow_check_asks_for_pillow_in_its_own_folder_only(tmp_path: Path) -> None:
    """The command the updater uses to ask "is a usable Pillow there?": yes only for one that is IN the bot folder's
    karne_paketleri (the command runs in the bot folder), whatever the interpreter itself may have."""
    import importlib.util
    import subprocess

    check = PILLOW_CHECK[len("-c "):]

    def run(folder: Path, **env: str) -> int:
        return subprocess.run([sys.executable, "-B", "-c", check], cwd=folder, env={**os.environ, **env},
                              capture_output=True, timeout=60).returncode

    empty = tmp_path / "empty"
    empty.mkdir()
    blocker = tmp_path / "blocker" / "PIL"
    blocker.mkdir(parents=True)
    (blocker / "__init__.py").write_text("raise ImportError('hidden')\n", encoding="utf-8")
    assert run(empty, PYTHONPATH=str(blocker.parent)) == 1                  # no Pillow anywhere
    if importlib.util.find_spec("PIL") is None:
        return
    import PIL

    assert run(empty) == 1                    # this interpreter has one, but not in the bot's own folder: "missing"
    bot = tmp_path / "bot"
    (bot / "karne_paketleri").mkdir(parents=True)
    source = Path(PIL.__file__).resolve().parent
    shutil.copytree(source, bot / "karne_paketleri" / "PIL")
    for extra in source.parent.glob("pillow.libs"):
        shutil.copytree(extra, bot / "karne_paketleri" / extra.name)
    assert run(bot) == 0                      # the same Pillow in the folder: there


def test_the_folder_pillow_lives_in_survives_updates_and_ways_back_untouched(g: Any, tmp_path: Path) -> None:  # noqa: F811
    """(Isolation review, P1.) The way back to the version before the report card must leave no Pillow the old bot
    could import: Pillow is never in the bot's environment, and its folder is none of the updater's business -
    not backed up, not replaced, not removed; a way back needs no uninstall (no pip command at all)."""
    bot = old_bot(tmp_path)
    pillow = fake_pillow(bot / "karne_paketleri")
    assert g.PILLOW_KLASORU == "karne_paketleri"
    runner = Runner({"-c import cryptg": [(0, [])], PILLOW_CHECK: [(0, [])]})       # (it is there: no pip)
    code, _ = run_update(g, bot, runner, "--baslatma")
    assert code == 0 and digest(bot / "karne_paketleri") == pillow
    (backup,) = (bot / "yedek").iterdir()
    assert not (backup / "karne_paketleri").exists()                               # not part of a backup
    # a later release
    later = tmp_path / "sonraki" / "telegram-copy-trader"
    shutil.copytree(new_tree(), later)
    (later / "copytrader" / "__init__.py").write_text("__version__ = 'sonraki'\n", encoding="utf-8")
    time.sleep(1.1)
    code, _ = run_update(g, bot, Runner({"-c import cryptg": [(0, [])], PILLOW_CHECK: [(0, [])]}), "--baslatma",
                         tree=later)
    assert code == 0 and digest(bot / "karne_paketleri") == pillow
    # an update that fails and is rolled back
    time.sleep(1.1)
    failing = Runner({"-c import cryptg": [(0, [])], PILLOW_CHECK: [(0, [])],
                      "-m copytrader migrate": [(3, ["config okunamadı"])]})
    code, _ = run_update(g, bot, failing, "--baslatma")
    assert code == 1 and digest(bot / "karne_paketleri") == pillow
    # the way back, twice: down to the version before the report card
    for _ in range(2):
        time.sleep(1.1)
        back = Runner()
        code, _ = run_update(g, bot, back, "--geri-al", "--baslatma")
        assert code == 0 and digest(bot / "karne_paketleri") == pillow
        assert not any("pip" in call for call in back.calls)                       # no uninstall, nothing to undo
    assert not (bot / "copytrader" / "karne").exists()                             # the old version is installed
    # ... and that old version's Python cannot import the Pillow that is still on the disk
    import subprocess

    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    seen = subprocess.run([sys.executable, "-B", "-c", "import PIL, sys; sys.stdout.write(PIL.__file__ or '')"],
                          cwd=bot, env=env, capture_output=True, text=True, timeout=60)
    assert "karne_paketleri" not in seen.stdout


def test_a_release_never_ships_the_pillow_folder() -> None:
    assert not (new_tree() / "karne_paketleri").exists()
    ignored = (HERE.parent / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert "karne_paketleri/" in ignored
    assert "karne_paketleri/" in (HERE.parent / ".dockerignore").read_text(encoding="utf-8").splitlines()
    from copytrader.karne import render

    assert render.PACKAGES_DIR == "karne_paketleri"                                # the bot looks where it is put
    assert f'PILLOW_KLASORU = "{render.PACKAGES_DIR}"' in (HERE.parent / "guncelle.py").read_text(encoding="utf-8")
