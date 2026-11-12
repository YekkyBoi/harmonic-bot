#!/usr/bin/env python
"""Bot güncelleme betiği (the updater). Owner steps: docs/GUNCELLEME.md.

It runs from the NEW, freshly extracted tree while the current directory (or ``--hedef``) is the OLD bot
folder; the one line of docs/GUNCELLEME.md extracts the downloaded zip and starts it with runpy.

Standard library only, and it never imports ``copytrader`` (with the bot folder as current directory that
would be the OLD package): everything is done with files and with subprocesses of the same Python
(``python -m pip``, ``python -m copytrader migrate | check | selftest | run``).

Steps (one numbered line each, ending in TAMAM or HATA; everything is also written to
``guncelleme_<timestamp>.log`` in the bot folder):

1. the folders are what they should be            6. config.yaml is migrated (``copytrader migrate``)
2. the bot is not running                         7. ``copytrader check``; if it fails the OLD version is restored
3. backup into ``yedek/<timestamp>/``             8. ``copytrader selftest`` (problems are listed, never a rollback)
4. the new code is installed                      9. the bot is started in the same window
5. pip, only when requirements.txt changed

Between the steps, without a number and never a reason to fail or undo the update: after step 3 the MetaApi
token an older version wrote into ``data/logs/copytrader.log*`` is masked in place, and after step 8 the watchman
(bekci.py) is registered as a Windows scheduled task (every 5 minutes, as SYSTEM) and asked for one test message.
While the update runs ``<data_dir>/guncelleme.lock`` tells the watchman that the bot is down on purpose.

A failure in steps 3 to 7 puts the old code and config back from the backup of step 3 and the OLD version is
started. Step 7 counts as failed when the new version's check reports something the INSTALLED version's check
(run once before anything is changed, output in the log file only) did not report: a problem that was there
before the update is no reason to lose the update, it is listed and the new version stays.
Never touched: ``.env``, ``data/``, ``venv/``, session files, ``*.yedek*`` and any file the new version does
not ship. Running it twice is harmless.

Options: ``--hedef <folder>`` (bot folder, default: current directory), ``--baslatma`` (do not start the
bot), ``--kontrol-yok`` (skip steps 7 and 8; for tests), ``--bekci-yok`` (do not register the watchman).

``--bekci-kur`` only registers the watchman (no update, no zip; the bot may keep running): for a window that was
not opened as administrator at update time, or after the bot folder was moved.

``--geri-al`` is the way back after an update that went through: it puts back, from ``yedek/``, the code that
was installed before the installed one together with the config.yaml of that time (see ``son_farkli_yedek``),
runs that version's ``check`` and starts the bot. The present state is backed up first
(``yedek/<timestamp>-geri``); the way forward again is the update line. It needs no zip and nothing of the
new tree.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import runpy
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import zipfile
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PAKET = "copytrader"
# Top-level files / folders of the new version that replace the installed ones (everything else stays).
UST_DOSYALAR = ("requirements.txt", "config.example.yaml", "config.tickmill.example.yaml", "guncelle.py",
                "bekci.py", "README.md", "pyproject.toml")
UST_KLASORLER = ("docs",)
YENI_GEREKLI = (f"{PAKET}/__init__.py", f"{PAKET}/__main__.py", f"{PAKET}/migrate.py", f"{PAKET}/selftest.py",
                "requirements.txt", "guncelle.py")
ZIP_KALIBI = "harmonic-bot*.zip"
KILIT_DOSYASI = "bot.lock"
GERI_EKI = "-geri"              # name suffix of the backups that --geri-al itself makes (never a way back)
ACILIS_PAYI = 300.0             # seconds; see BOOT_SLACK_SEC in copytrader/__main__.py
ONAY = "EVET"
# The watchman (bekci.py; the same names and times are in that file)
GOREV_ADI = "CopytraderBekci"
BEKCI_DOSYASI = "bekci.py"
BEKCI_DURUMU, BEKCI_DENEME = "bekci.json", "bekci.dene"
GUNCELLEME_KILIDI = "guncelleme.lock"   # in the data folder; younger than KILIT_OMRU: "an update is running"
KILIT_OMRU = 1800.0                     # seconds (GUNCELLEME_SURESI of bekci.py)
BASLANGIC_PAYI = 120.0                  # seconds the update lock still counts after the bot was started
BEKCI_BEKLEME = 30.0                    # seconds to wait for the test message of the freshly registered task
GOREV_TR_SINIRI = 261                   # longest /TR value schtasks accepts
KONTROL_SURESI = 900.0          # seconds for check / selftest / migrate
PIP_SURESI = 1800.0
BEKLEME = 15.0                  # seconds before the single retry of a failed check
CIKIS_TAMAM, CIKIS_GERI_ALINDI, CIKIS_REDDEDILDI, CIKIS_KESILDI = 0, 1, 2, 130

# "python -m copytrader run" (also the console script "copytrader run"), however it is quoted / spaced.
BOT_KOMUTU = re.compile(r"""(?:-m\s+copytrader(?:\.__main__)?|[\\/]copytrader(?:\.exe)?["']?)\s+run(?:\s|$)""",
                        re.IGNORECASE)
DURDUR = ('Botu kendi sekmesinde Ctrl+C ile durdurun, "Durduruldu." yazısını bekleyin, sonra aynı satırı '
          "tekrar yapıştırın. (Sekme yanıt vermiyorsa o sekmeyi kapatın.)")

Calistirici = Callable[[list[str], Path, float, Callable[[str], None]], int]
OTOMATIK: Any = object()        # default of ``main(gorev=...)``: decided by the system this runs on


class Hata(Exception):
    """A step failed; ``str(err)`` is the Turkish reason shown to the owner."""


# --------------------------------------------------------------------------- #
# Output: console + log file
# --------------------------------------------------------------------------- #
class Gunluk:
    """Prints every line and appends it to the log file (lines written before the file is opened are kept)."""

    def __init__(self) -> None:
        self.yol: Path | None = None
        self._dosya: Any = None
        self._bekleyen: list[str] = []

    def ac(self, yol: Path) -> None:
        try:
            self._dosya = open(yol, "a", encoding="utf-8")
            self.yol = yol
            for satir in self._bekleyen:
                self._dosya.write(satir + "\n")
            self._dosya.flush()
        except OSError:
            self._dosya = None          # no log file is no reason to stop an update
        self._bekleyen = []

    def yaz(self, metin: str = "") -> None:
        try:
            print(metin, flush=True)
        except (OSError, ValueError):
            pass
        if self._dosya is None:
            self._bekleyen.append(metin)
            return
        try:
            self._dosya.write(metin + "\n")
            self._dosya.flush()
        except (OSError, ValueError):
            pass

    def dosyaya(self, metin: str) -> None:
        """Only into the log file (long tool output that would bury the steps on the screen)."""
        if self._dosya is None:
            self._bekleyen.append(metin)
            return
        try:
            self._dosya.write(metin + "\n")
            self._dosya.flush()
        except (OSError, ValueError):
            pass

    def adim(self, no: int, metin: str, tamam: bool = True) -> None:
        self.yaz(f"{no}. {metin}: {'TAMAM' if tamam else 'HATA'}")

    def kapat(self) -> None:
        if self._dosya is not None:
            try:
                self._dosya.close()
            except OSError:
                pass
            self._dosya = None


# --------------------------------------------------------------------------- #
# Is the bot running? Process list (any version) and lock file (versions that write one)
# --------------------------------------------------------------------------- #
Surec = tuple[int, str, str]         # (pid, program name, command line)


def powershell_cikti_coz(metin: str) -> list[Surec]:
    """Lines "<pid>|<name>|<command line>" (the PowerShell query below)."""
    sonuc: list[Surec] = []
    for satir in metin.splitlines():
        m = re.match(r"\s*(\d+)\|([^|]*)\|(.*)$", satir)
        if m:
            sonuc.append((int(m.group(1)), m.group(2).strip(), m.group(3).strip()))
    return sonuc


def wmic_cikti_coz(metin: str) -> list[Surec]:
    """``wmic process get CommandLine,Name,ProcessId /format:csv``: "Node,CommandLine,Name,ProcessId" rows;
    the command line may contain commas itself, the last two fields never do."""
    sonuc: list[Surec] = []
    for satir in metin.splitlines():
        parcalar = satir.strip().split(",")
        if len(parcalar) < 4 or not parcalar[-1].strip().isdigit():
            continue
        sonuc.append((int(parcalar[-1]), parcalar[-2].strip(), ",".join(parcalar[1:-2]).strip()))
    return sonuc


def surecler_windows(calis: Callable[..., Any] = subprocess.run) -> list[Surec] | None:
    """Every process: PowerShell (Get-CimInstance Win32_Process), then wmic. None: neither worked."""
    kok = os.environ.get("SystemRoot") or r"C:\Windows"
    powershell = os.path.join(kok, "System32", "WindowsPowerShell", "v1.0", "powershell.exe")
    betik = ("Get-CimInstance Win32_Process | ForEach-Object "
             "{ '{0}|{1}|{2}' -f $_.ProcessId, $_.Name, $_.CommandLine }")
    denemeler = (
        ([powershell if os.path.isfile(powershell) else "powershell", "-NoProfile", "-NonInteractive",
          "-Command", betik], powershell_cikti_coz),
        (["wmic", "process", "get", "CommandLine,Name,ProcessId", "/format:csv"], wmic_cikti_coz),
    )
    for komut, coz in denemeler:
        try:
            sonuc = calis(komut, capture_output=True, timeout=90, stdin=subprocess.DEVNULL)
            metin = (sonuc.stdout or b"")
            if isinstance(metin, bytes):
                metin = metin.decode("utf-8", "replace").replace("\x00", "")
            liste = coz(metin)
            if liste:
                return liste
        except Exception:      # noqa: BLE001 - try the next way
            continue
    return None


def surecler_proc(kok: str = "/proc") -> list[Surec] | None:
    """Linux: /proc/<pid>/cmdline (arguments separated by NUL; the first one is the program)."""
    try:
        adlar = os.listdir(kok)
    except OSError:
        return None
    sonuc: list[Surec] = []
    for ad in adlar:
        if not ad.isdigit():
            continue
        try:
            ham = Path(kok, ad, "cmdline").read_bytes()
        except OSError:
            continue
        parcalar = [p.decode("utf-8", "replace") for p in ham.split(b"\x00") if p]
        if parcalar:
            sonuc.append((int(ad), os.path.basename(parcalar[0]), " ".join(parcalar)))
    return sonuc


def surecler_ps(calis: Callable[..., Any] = subprocess.run) -> list[Surec] | None:
    """Other Unix systems: ``ps``."""
    try:
        sonuc = calis(["ps", "-axo", "pid=,command="], capture_output=True, timeout=60, stdin=subprocess.DEVNULL)
        satirlar = (sonuc.stdout or b"").decode("utf-8", "replace").splitlines()
    except Exception:          # noqa: BLE001
        return None
    liste: list[Surec] = []
    for satir in satirlar:
        pid, _, komut = satir.strip().partition(" ")
        if pid.isdigit() and komut.strip():
            liste.append((int(pid), os.path.basename(komut.split()[0]), komut.strip()))
    return liste or None


def surec_listesi() -> list[Surec] | None:
    if os.name == "nt":
        return surecler_windows()
    if os.path.isdir("/proc/self"):
        return surecler_proc()
    return surecler_ps()


def calisan_botlar(surecler: list[Surec], kendi: int | None = None) -> list[Surec]:
    """Python processes (other than this one) whose command line is the bot's ``run`` command. A shell or a
    launcher that merely carries the same text in its command line is not the bot."""
    kendi = os.getpid() if kendi is None else kendi
    return [(pid, ad, komut) for pid, ad, komut in surecler
            if pid != kendi and "python" in ad.lower() and BOT_KOMUTU.search(komut)]


def _surec_durumu_windows(pid: int) -> tuple[bool | None, str | None]:   # pragma: no cover - Windows only
    import ctypes
    from ctypes import wintypes

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)        # type: ignore[attr-defined]
    k32.OpenProcess.restype = wintypes.HANDLE
    k32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    k32.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
    k32.GetProcessTimes.argtypes = (wintypes.HANDLE,) + (ctypes.POINTER(wintypes.FILETIME),) * 4
    k32.CloseHandle.argtypes = (wintypes.HANDLE,)
    tutamac = k32.OpenProcess(0x1000, False, pid)               # PROCESS_QUERY_LIMITED_INFORMATION
    if not tutamac:
        return ctypes.get_last_error() == 5, None               # type: ignore[attr-defined]  # 5: access denied
    try:
        kod = wintypes.DWORD()
        yasiyor = bool(k32.GetExitCodeProcess(tutamac, ctypes.byref(kod))) and kod.value == 259  # STILL_ACTIVE
        dogum, t1, t2, t3 = (wintypes.FILETIME() for _ in range(4))
        kimlik = None
        if k32.GetProcessTimes(tutamac, ctypes.byref(dogum), ctypes.byref(t1), ctypes.byref(t2), ctypes.byref(t3)):
            kimlik = str((dogum.dwHighDateTime << 32) | dogum.dwLowDateTime)
        return yasiyor, kimlik
    finally:
        k32.CloseHandle(tutamac)


def surec_durumu(pid: int) -> tuple[bool | None, str | None]:
    """(alive, identity = creation time where the system tells it). Same rules as the bot's own lock check
    (copytrader/__main__.py ``_process_state``); never sends a signal."""
    if pid <= 0:
        return False, None
    try:
        if os.name == "nt":
            return _surec_durumu_windows(pid)     # os.kill(pid, 0) would send Ctrl+C there
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False, None
        except PermissionError:
            pass
        try:
            alanlar = Path(f"/proc/{pid}/stat").read_bytes().rsplit(b")", 1)[-1].split()
        except OSError:
            return True, None
        if alanlar and alanlar[0] == b"Z":
            return False, None
        return True, (alanlar[19].decode() if len(alanlar) > 19 else None)
    except Exception:          # noqa: BLE001
        return None, None


def calisma_suresi() -> float | None:
    """Seconds since the system started; None when it cannot be told."""
    try:
        if os.name == "nt":                       # pragma: no cover - Windows only
            import ctypes

            sayac = ctypes.windll.kernel32.GetTickCount64       # type: ignore[attr-defined]
            sayac.restype = ctypes.c_ulonglong
            return float(sayac()) / 1000.0
        return float(Path("/proc/uptime").read_text(encoding="ascii").split()[0])
    except Exception:          # noqa: BLE001
        return None


def acilistan_once_yazilmis(baslangic: Any) -> bool:
    """The lock is from before the last system start (same rule as ``_written_before_boot`` of the bot)."""
    sure = calisma_suresi()
    if sure is None:
        return False
    try:
        zaman = datetime.fromisoformat(str(baslangic))
    except (ValueError, TypeError):
        return False
    if zaman.tzinfo is None:
        return False
    return zaman < datetime.now(timezone.utc) - timedelta(seconds=sure + ACILIS_PAYI)


def veri_klasoru(hedef: Path) -> Path:
    """``data_dir`` of the bot's config.yaml (default "data"), relative to the bot folder."""
    deger = "data"
    try:
        metin = (hedef / "config.yaml").read_text(encoding="utf-8", errors="replace")
        m = re.search(r"(?m)^data_dir[ \t]*:[ \t]*([^#\r\n]+)", metin)
        if m:
            bulunan = m.group(1).strip().strip("'\"")
            if bulunan and "${" not in bulunan:
                deger = bulunan
    except OSError:
        pass
    yol = Path(deger).expanduser()
    return yol if yol.is_absolute() else hedef / yol


def kilit_sahibi(veri: Path) -> dict[str, Any] | None:
    """Content of ``bot.lock`` when a LIVE process wrote it (see ``lock_holder`` in copytrader/__main__.py)."""
    try:
        bilgi = json.loads((veri / KILIT_DOSYASI).read_text(encoding="utf-8"))
        pid = int(bilgi["pid"])
    except (OSError, ValueError, KeyError, TypeError):
        return None
    if pid == os.getpid():
        return None
    yasiyor, kimlik = surec_durumu(pid)
    if not yasiyor:
        return None
    if kimlik and bilgi.get("token") and str(bilgi["token"]) != kimlik:
        return None
    if acilistan_once_yazilmis(bilgi.get("started")):
        return None
    return dict(bilgi)


def bot_calisiyor_mu(hedef: Path, surecler: Callable[[], list[Surec] | None]) -> tuple[str | None, bool]:
    """(what shows a running bot or None, could the process list be read?)."""
    kilit = kilit_sahibi(veri_klasoru(hedef))
    if kilit is not None:
        return f"bot çalışıyor (PID {kilit.get('pid')}, kilit dosyası)", True
    liste = surecler()
    if liste is None:
        return None, False
    botlar = calisan_botlar(liste)
    if botlar:
        return "bot çalışıyor (PID " + ", ".join(str(bot[0]) for bot in botlar) + ")", True
    return None, True


# --------------------------------------------------------------------------- #
# Files
# --------------------------------------------------------------------------- #
def _tekrarla(islem: Callable[[], Any], kez: int = 6, bekle: float = 0.5) -> Any:
    """Windows: a virus scanner / indexer can hold a file for a moment. Retry a few times."""
    for deneme in range(kez):
        try:
            return islem()
        except OSError:
            if deneme == kez - 1:
                raise
            time.sleep(bekle)
    return None


def _yazilabilir_yap(islev: Callable[..., Any], yol: str, *_bilgi: Any) -> None:
    os.chmod(yol, stat.S_IWRITE | stat.S_IREAD)
    islev(yol)


def _sil(yol: Path) -> None:
    """Remove a file or a whole folder (also read-only files); nothing when it is not there."""
    if yol.is_dir() and not yol.is_symlink():
        if sys.version_info >= (3, 12):
            _tekrarla(lambda: shutil.rmtree(yol, onexc=_yazilabilir_yap))
        else:                                                            # pragma: no cover
            _tekrarla(lambda: shutil.rmtree(yol, onerror=_yazilabilir_yap))
    elif yol.exists() or yol.is_symlink():
        _tekrarla(yol.unlink)


_YOKSAY = shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo")


def _dosyalar(kok: Path) -> dict[str, int]:
    """Relative path -> size of every file below ``kok`` (compiled caches left out)."""
    sonuc: dict[str, int] = {}
    for yol in kok.rglob("*"):
        if yol.is_file() and "__pycache__" not in yol.parts and yol.suffix not in (".pyc", ".pyo"):
            sonuc[yol.relative_to(kok).as_posix()] = yol.stat().st_size
    return sonuc


def yedekle(hedef: Path, yedek: Path) -> None:
    """Step 3: the package, config.yaml and the top-level files / folders an update overwrites."""
    yedek.mkdir(parents=True)
    shutil.copytree(hedef / PAKET, yedek / PAKET, ignore=_YOKSAY)
    for ad in ("config.yaml", *UST_DOSYALAR):
        if (hedef / ad).is_file():
            shutil.copy2(hedef / ad, yedek / ad)
    for klasor in UST_KLASORLER:
        if (hedef / klasor).is_dir():
            shutil.copytree(hedef / klasor, yedek / klasor, ignore=_YOKSAY)
    if _dosyalar(hedef / PAKET) != _dosyalar(yedek / PAKET):
        raise Hata("yedek, kurulu sürümle aynı çıkmadı (kopyalama eksik kaldı)")
    if (hedef / "config.yaml").read_bytes() != (yedek / "config.yaml").read_bytes():
        raise Hata("config.yaml yedeği aynı çıkmadı")


def kur(hedef: Path, yeni: Path) -> None:
    """Step 4: swap the whole package (no stale module survives), then copy the top-level files."""
    eski, gecici = hedef / f"{PAKET}.eski", hedef / f"{PAKET}.yeni"
    for artik in (eski, gecici):                 # left behind by an interrupted run
        _sil(artik)
    shutil.copytree(yeni / PAKET, gecici, ignore=_YOKSAY)
    if _dosyalar(yeni / PAKET) != _dosyalar(gecici):
        _sil(gecici)
        raise Hata("yeni sürüm kopyalanırken eksik kaldı")
    try:
        _tekrarla(lambda: os.rename(hedef / PAKET, eski))
    except OSError as exc:
        _sil(gecici)
        raise Hata(f"eski {PAKET} klasörü yerinden alınamadı (bir program içindeki bir dosyayı açık tutuyor "
                   f"olabilir): {exc}") from exc
    try:
        _tekrarla(lambda: os.rename(gecici, hedef / PAKET))
    except OSError as exc:
        _tekrarla(lambda: os.rename(eski, hedef / PAKET))       # the old package goes back to its place
        raise Hata(f"yeni {PAKET} klasörü yerine konamadı: {exc}") from exc
    try:
        _sil(eski)
    except OSError:
        pass                                     # harmless leftover; the next run removes it
    for ad in UST_DOSYALAR:
        if (yeni / ad).is_file():
            shutil.copy2(yeni / ad, hedef / ad)
    for klasor in UST_KLASORLER:
        if (yeni / klasor).is_dir():
            shutil.copytree(yeni / klasor, hedef / klasor, ignore=_YOKSAY, dirs_exist_ok=True)


def geri_yukle(hedef: Path, yedek: Path) -> None:
    """Put code and config back from the backup of step 3 (files the backup does not have are left alone)."""
    for artik in (hedef / f"{PAKET}.yeni",):
        try:
            _sil(artik)
        except OSError:
            pass
    if not (hedef / PAKET).is_dir() and (hedef / f"{PAKET}.eski").is_dir():
        os.rename(hedef / f"{PAKET}.eski", hedef / PAKET)
    try:
        _sil(hedef / PAKET)
    except OSError:
        pass                                     # whatever could not be removed is overwritten below
    shutil.copytree(yedek / PAKET, hedef / PAKET, dirs_exist_ok=True)
    for ad in ("config.yaml", *UST_DOSYALAR):
        if (yedek / ad).is_file():
            shutil.copy2(yedek / ad, hedef / ad)
    for klasor in UST_KLASORLER:
        if (yedek / klasor).is_dir():
            shutil.copytree(yedek / klasor, hedef / klasor, dirs_exist_ok=True)
    try:
        _sil(hedef / f"{PAKET}.eski")
    except OSError:
        pass


def _icerik(kok: Path) -> dict[str, str]:
    """Relative path -> content hash of every file below ``kok`` (compiled caches left out)."""
    sonuc: dict[str, str] = {}
    for yol in kok.rglob("*"):
        if yol.is_file() and "__pycache__" not in yol.parts and yol.suffix not in (".pyc", ".pyo"):
            sonuc[yol.relative_to(kok).as_posix()] = hashlib.sha256(yol.read_bytes()).hexdigest()
    return sonuc


def son_farkli_yedek(hedef: Path) -> Path | None:
    """The backup ``--geri-al`` goes back to: the code that was installed before the installed one.

    The backups the UPDATES made are the history, newest first (the folders are named by their time). Where
    the installed code itself is in that history (the newest backup holding exactly it: after a way back, or
    after the same update was run twice), only older backups count; from there the first one with different
    code is the answer. So going back repeatedly walks towards older versions and never swings forward again.
    The backups ``--geri-al`` itself makes are not part of the history. None: nothing older is there."""
    try:
        adaylar = sorted((yol for yol in (hedef / "yedek").iterdir()
                          if GERI_EKI not in yol.name
                          and (yol / PAKET / "__main__.py").is_file() and (yol / "config.yaml").is_file()),
                         key=lambda yol: yol.name, reverse=True)
    except OSError:
        return None
    kurulu = _icerik(hedef / PAKET)
    icerikler = [(aday, _icerik(aday / PAKET)) for aday in adaylar]
    konum = next((sira for sira, (_aday, icerik) in enumerate(icerikler) if icerik == kurulu), -1)
    for aday, icerik in icerikler[konum + 1:]:
        if icerik != kurulu:
            return aday
    return None


def yedek_tarihi(yedek: Path) -> str:
    """'12.11.2026 11:54' from a backup folder name ('20261112-115456'); the name itself when it is not one."""
    try:
        return datetime.strptime(yedek.name[:15], "%Y%m%d-%H%M%S").strftime("%d.%m.%Y %H:%M")
    except ValueError:
        return yedek.name


def gereksinimler(yol: Path) -> list[str] | None:
    """Requirement lines without comments, blanks and order (None: no such file)."""
    try:
        satirlar = yol.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    return sorted(s for s in ("".join(satir.split("#", 1)[0].split()).lower() for satir in satirlar) if s)


def en_yeni_zip(klasor: Path) -> Path | None:
    """Newest downloaded zip of the repository in ``klasor`` (the browser's download folder)."""
    try:
        adaylar = sorted(klasor.glob(ZIP_KALIBI), key=lambda yol: yol.stat().st_mtime)
    except OSError:
        return None
    return adaylar[-1] if adaylar else None


def zipten_ac(zip_yolu: Path) -> Path | None:
    """Extract into a fresh temp folder; the bot tree inside (the folder holding guncelle.py) or None."""
    kok = Path(tempfile.mkdtemp(prefix="bot-guncelleme-"))
    with zipfile.ZipFile(zip_yolu) as arsiv:
        arsiv.extractall(kok)
    bulunan = sorted(kok.glob("*/telegram-copy-trader/guncelle.py"))
    return bulunan[0].parent if bulunan else None


# --------------------------------------------------------------------------- #
# Old log files: the MetaApi token an older version wrote into them
# --------------------------------------------------------------------------- #
_GIZLI = (
    # auth-token=<value> (URL query), "auth-token": "<value>" (header / dict text)
    (re.compile(rb"""(auth-token["']?\s*[=:]\s*["']?)[^&\s"'<>,;)}\]]+""", re.IGNORECASE), rb"\1***"),
    (re.compile(rb"""([?&]token=)[^&\s"'<>]+""", re.IGNORECASE), rb"\1***"),            # ...?token=<value> in a URL
    (re.compile(rb"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"), b"***"),   # the token itself (a JWT)
)


def gunlukleri_temizle(veri: Path) -> tuple[int, int, int, str | None]:
    """Mask account tokens in ``<veri>/logs/copytrader.log*`` in place (the value becomes ``***``; every other
    byte stays as it is). Returns (files read, files rewritten, lines masked, reason of a failure or None);
    never raises: a log file that cannot be rewritten is no reason to stop an update."""
    dosya_sayisi = satir_sayisi = 0
    neden: str | None = None
    try:
        dosyalar = sorted(yol for yol in (veri / "logs").glob("copytrader.log*")
                          if yol.is_file() and not yol.name.endswith(".gizle"))
    except OSError as exc:
        return 0, 0, 0, str(exc)
    for yol in dosyalar:
        gecici = yol.with_name(yol.name + ".gizle")
        try:
            degisen = 0
            zamanlar = yol.stat()
            with open(yol, "rb") as kaynak, open(gecici, "wb") as cikti:
                for satir in kaynak:
                    yeni = satir
                    for kalip, yerine in _GIZLI:
                        yeni = kalip.sub(yerine, yeni)
                    degisen += yeni != satir
                    cikti.write(yeni)
            if degisen:
                _tekrarla(lambda: os.replace(gecici, yol), kez=3)
                try:
                    os.utime(yol, (zamanlar.st_atime, zamanlar.st_mtime))     # the file keeps its date
                except OSError:
                    pass
                dosya_sayisi += 1
                satir_sayisi += degisen
        except Exception as exc:      # noqa: BLE001 - see the docstring
            neden = neden or f"{yol.name}: {exc}"
        finally:
            try:
                gecici.unlink()
            except OSError:
                pass
    return len(dosyalar), dosya_sayisi, satir_sayisi, neden


def gunluk_temizligi(veri: Path, gunluk: "Gunluk") -> None:
    """Mask the old log files and say in one line what was done."""
    okunan, _degisen, satir, neden = gunlukleri_temizle(veri)
    if satir:
        gunluk.yaz(f"   Eski kayıt dosyalarında (copytrader.log) {satir} satırdaki hesap anahtarı (token) gizlendi.")
    elif okunan and not neden:
        gunluk.yaz("   Eski kayıt dosyalarında (copytrader.log) gizlenecek hesap anahtarı bulunmadı.")
    if neden:
        gunluk.yaz(f"   Eski kayıt dosyaları temizlenemedi (isteğe bağlı): {neden}")


# --------------------------------------------------------------------------- #
# The watchman (bekci.py): update lock, scheduled task, test message
# --------------------------------------------------------------------------- #
def guncelleme_kilidi_yaz(veri: Path) -> None:
    """Tell the watchman that the bot is down on purpose (nothing where there is no data folder)."""
    try:
        if veri.is_dir():
            (veri / GUNCELLEME_KILIDI).write_text("guncelleme\n", encoding="ascii")
    except OSError:
        pass


def guncelleme_kilidi_kaldir(veri: Path, bot_basliyor: bool = False) -> None:
    """The update is over. ``bot_basliyor``: the bot is started next and needs a moment until its own lock file
    is there, so the update lock is left to run out BASLANGIC_PAYI seconds from now instead of being removed."""
    yol = veri / GUNCELLEME_KILIDI
    if bot_basliyor:
        try:
            eski = time.time() - (KILIT_OMRU - BASLANGIC_PAYI)
            os.utime(yol, (eski, eski))
            return
        except OSError:
            pass
    try:
        yol.unlink()
    except OSError:
        pass


def kilit_yazan_surum(hedef: Path) -> bool:
    """Does the installed bot version write bot.lock? (bekci.py asks the same; without it the watchman is silent.)"""
    try:
        return f'"{KILIT_DOSYASI}"' in (hedef / PAKET / "__main__.py").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False


def _schtasks() -> str:
    yol = os.path.join(os.environ.get("SystemRoot") or r"C:\Windows", "System32", "schtasks.exe")
    return yol if os.path.isfile(yol) else "schtasks"


def _tirnakli(yol: str) -> str:
    """A path inside the /TR value: in double quotes only when it holds a blank (the value is limited in length)."""
    return f'"{yol}"' if any(k.isspace() for k in yol) else yol


def bekci_komutu(python: str, bekci: str) -> list[str]:
    """The schtasks command that registers bekci.py: every 5 minutes, as SYSTEM (so also after a restart of
    the server, before anyone logs on), replacing an earlier registration of the same name."""
    return [_schtasks(), "/Create", "/TN", GOREV_ADI, "/SC", "MINUTE", "/MO", "5", "/RU", "SYSTEM", "/RL", "HIGHEST",
            "/F", "/TR", f"{_tirnakli(python)} {_tirnakli(bekci)}"]


def bekci_python(hedef: Path, varsayilan: str) -> str:
    """The Python the scheduled task runs: the bot's own (venv), else the one this update runs with."""
    for ad in ("venv", ".venv"):
        yol = hedef / ad / "Scripts" / "python.exe"
        if yol.is_file():
            return str(yol)
    return varsayilan


def _cikti(sonuc: Any) -> str:
    """What a finished command said (its error text first), as one short line."""
    parcalar = []
    for ham in (getattr(sonuc, "stderr", None), getattr(sonuc, "stdout", None)):
        if isinstance(ham, bytes):
            for kod in ("oem", "utf-8"):
                try:
                    ham = ham.decode(kod, "replace")
                    break
                except LookupError:               # "oem" exists on Windows only
                    continue
        metin = " ".join(str(ham or "").replace("\x00", "").split())
        if metin:
            parcalar.append(metin)
    return " | ".join(parcalar)[:300]


def yonetici_mi() -> bool | None:
    """Was this window opened with administrator rights? None: cannot be told (or not Windows)."""
    try:
        import ctypes

        return bool(ctypes.windll.shell32.IsUserAnAdmin())      # type: ignore[attr-defined]
    except Exception:          # noqa: BLE001
        return None


def bekci_kur(hedef: Path, python: str, calis: Callable[..., Any],
              yonetici: Callable[[], bool | None] = yonetici_mi) -> str | None:
    """Register (or refresh) the watchman's scheduled task. None: done; else the Turkish reason why not."""
    bekci = hedef / BEKCI_DOSYASI
    if not bekci.is_file():
        return f"{BEKCI_DOSYASI} bot klasöründe yok"
    komut = bekci_komutu(bekci_python(hedef, python), str(bekci))
    if len(komut[-1]) > GOREV_TR_SINIRI:
        return (f"bot klasörünün yolu çok uzun (görev satırı {len(komut[-1])} karakter tutuyor, Windows en fazla "
                f"{GOREV_TR_SINIRI} karaktere izin veriyor)")
    try:
        sonuc = calis(komut, capture_output=True, timeout=120, stdin=subprocess.DEVNULL)
    except Exception as exc:   # noqa: BLE001
        return f"schtasks çalıştırılamadı ({exc})"
    kod = getattr(sonuc, "returncode", None)
    if kod != 0:
        neden = _cikti(sonuc) or f"schtasks çıkış kodu {kod}"
        if yonetici() is False:
            neden = ('bu pencere yönetici yetkisiyle açılmamış (PowerShell\'i "Yönetici olarak çalıştır" ile açıp '
                     f"güncelleme satırını yeniden yapıştırabilirsiniz); Windows'un yanıtı: {neden}")
        return neden
    return None


def bekci_dene(veri: Path, calis: Callable[..., Any], bekle: float | None = None) -> tuple[bool | None, str]:
    """Run the registered task once, now, and wait for the result of the test message it is asked for.

    (True, ""): the message went out; (False, reason): the task ran but its test failed (it could not send, or
    it cannot look at processes there); (None, reason): no answer within ``bekle`` seconds (the request stays on
    file: the next scheduled pass answers it)."""
    bekle = BEKCI_BEKLEME if bekle is None else bekle
    istek = f"{os.getpid()}-{int(time.time())}"
    try:
        (veri / BEKCI_DENEME).write_text(istek, encoding="ascii")
    except OSError as exc:
        return None, f"deneme isteği yazılamadı ({exc})"
    try:
        sonuc = calis([_schtasks(), "/Run", "/TN", GOREV_ADI], capture_output=True, timeout=60, stdin=subprocess.DEVNULL)
    except Exception as exc:   # noqa: BLE001
        return None, f"görev başlatılamadı ({exc})"
    if getattr(sonuc, "returncode", None) != 0:
        return None, _cikti(sonuc) or "görev başlatılamadı"
    son = time.monotonic() + bekle
    while True:
        try:
            deneme = json.loads((veri / BEKCI_DURUMU).read_text(encoding="utf-8")).get("deneme") or {}
        except (OSError, ValueError, AttributeError):
            deneme = {}
        if isinstance(deneme, dict) and deneme.get("istek") == istek:
            cevap = str(deneme.get("sonuc") or "")
            return (True, "") if cevap == "tamam" else (False, cevap or "sebep yazılmamış")
        if time.monotonic() >= son:
            return None, f"{bekle:.0f} saniye içinde yanıt gelmedi"
        time.sleep(0.5)


def varsayilan_gorev(sistem: str | None = None) -> Callable[..., Any] | None:
    """What runs schtasks: the real thing on Windows; None elsewhere (no scheduled task, nothing is said)."""
    return subprocess.run if (os.name if sistem is None else sistem) == "nt" else None


def bekci_adimi(hedef: Path, python: str, gorev: Callable[..., Any] | None, atla: bool, gunluk: "Gunluk") -> bool:
    """Register the watchman and say in one line how that went; True: registered and its test message went
    out. ``gorev`` runs schtasks (None: this is not Windows, nothing is done or said). Never raises: the
    watchman is optional, the update is not."""
    if gorev is None:
        return False
    if atla:
        gunluk.yaz("   Bekçi kurulmadı (--bekci-yok).")
        return False
    try:
        neden = bekci_kur(hedef, python, gorev)
        if neden is not None:
            gunluk.yaz(f"   Bekçi kurulamadı (isteğe bağlı): {neden}")
            return False
        oldu, neden = bekci_dene(veri_klasoru(hedef), gorev)
    except Exception as exc:   # noqa: BLE001
        gunluk.yaz(f"   Bekçi kurulamadı (isteğe bağlı): {exc}")
        return False
    if oldu:
        gunluk.yaz("   Bekçi kuruldu: bot çalışmıyorsa birkaç dakika içinde Telegram'dan haber verir. Deneme mesajı "
                   "gönderildi; Telegram'ınıza bakın.")
        return True
    if oldu is False:
        gunluk.yaz(f"   Bekçi kuruldu, ama denemesi başarısız oldu: {neden}. Bu haliyle haber veremez; ekran "
                   "görüntüsünü gönderin.")
    else:
        gunluk.yaz(f"   Bekçi kuruldu, ama denemesi doğrulanamadı ({neden}). Birkaç dakika içinde Telegram'a "
                   '"Bekçi kuruldu" mesajı gelmezse bekçi çalışmıyordur; ekran görüntüsünü gönderin.')
    return False


ESKI_SURUM_NOTU = ("   Not: bu eski sürüm, çalışıp çalışmadığını bekçiye bildirmez; bu sürüm kuruluyken bekçi "
                   "\"Bot çalışmıyor\" uyarısı göndermez.")


def yalniz_bekci(hedef: Path, python: str, gorev: Callable[..., Any] | None, gunluk: "Gunluk") -> int:
    """``--bekci-kur``: register the watchman, nothing else (the bot may be running)."""
    gunluk.yaz(f"=== BEKÇİ KURULUMU ===  {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    gunluk.yaz(f"Bot klasörü: {hedef}")
    if gorev is None:
        gunluk.yaz("   Bekçi yalnızca Windows'ta kurulur; bu bilgisayarda yapılacak bir şey yok.")
        return CIKIS_REDDEDILDI
    if not ((hedef / PAKET / "__main__.py").is_file() and (hedef / "config.yaml").is_file()):
        gunluk.yaz("   burası botun klasörü değil; komutu botun klasöründeyken çalıştırın. Hiçbir şey yapılmadı.")
        return CIKIS_REDDEDILDI
    tamam = bekci_adimi(hedef, python, gorev, False, gunluk)
    if not kilit_yazan_surum(hedef):
        gunluk.yaz(ESKI_SURUM_NOTU)
    return CIKIS_TAMAM if tamam else CIKIS_GERI_ALINDI


# --------------------------------------------------------------------------- #
# Subprocesses
# --------------------------------------------------------------------------- #
def komut_calistir(komut: list[str], klasor: Path, sure: float, satir: Callable[[str], None]) -> int:
    """Run ``komut`` in ``klasor``; every output line goes to ``satir`` as it appears. Exit code; -1 when
    it could not be started, -2 when it did not finish within ``sure`` seconds (it is stopped then)."""
    ortam = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
    try:
        surec = subprocess.Popen(komut, cwd=str(klasor), env=ortam, stdin=subprocess.DEVNULL,
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except OSError as exc:
        satir(f"komut başlatılamadı: {exc}")
        return -1

    def oku() -> None:
        assert surec.stdout is not None
        for ham in iter(surec.stdout.readline, b""):
            satir(ham.decode("utf-8", "replace").rstrip("\r\n"))

    okuyucu = threading.Thread(target=oku, daemon=True)
    okuyucu.start()
    try:
        kod = surec.wait(timeout=sure)
    except subprocess.TimeoutExpired:
        surec.kill()
        surec.wait()
        okuyucu.join(5)
        satir(f"{sure:.0f} saniye içinde bitmedi; durduruldu")
        return -2
    except KeyboardInterrupt:
        try:
            surec.wait(timeout=30)      # it received the same Ctrl+C: let it finish by itself
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            surec.kill()
        raise
    okuyucu.join(10)
    return int(kod)


def botu_baslat(hedef: Path, python: str, komut: list[str] | None = None) -> int:
    """Step 9: the bot as a child in the same window; returns its exit code.

    Ctrl+C reaches the bot directly (same console): it shuts down by itself. This process only waits and never
    kills it: that would cut the bot's orderly shutdown short."""
    komut = komut or [python, "-m", PAKET, "run"]
    try:
        surec = subprocess.Popen(komut, cwd=str(hedef))
    except OSError as exc:
        print(f"Bot başlatılamadı: {exc}", flush=True)
        return 1
    onceki: Any = None
    try:
        onceki = signal.signal(signal.SIGINT, signal.SIG_IGN)      # after the child exists: it is not inherited
    except (ValueError, OSError):
        pass
    try:
        while True:
            try:
                return int(surec.wait())
            except KeyboardInterrupt:
                continue
    finally:
        if onceki is not None:
            try:
                signal.signal(signal.SIGINT, onceki)
            except (ValueError, OSError, TypeError):
                pass


Ozet = tuple[set[str], int | None]      # (problem keys, the count the report itself states; None: no summary line)
# A line of the logging module ("WARNING copytrader.brokers.x: ..."): never one of the report's own lines.
_GUNLUK_SATIRI = re.compile(r"(?:DEBUG|INFO|WARNING|ERROR|CRITICAL) \S+: ")


def kontrol_ozeti(satirlar: list[str]) -> Ozet:
    """What a ``copytrader check`` output reports as problems, as comparable keys (a failing broker by its
    name, a failing setting by its label), and the number of its closing line ("3 sorun bulundu." / "Sorun
    yok"). No closing line means the check did not run to its end."""
    sorunlar: set[str] = set()
    sayi: int | None = None
    for satir in satirlar:
        metin = satir.strip()
        if _GUNLUK_SATIRI.match(metin):
            continue
        bitis = re.fullmatch(r"(\d+) sorun bulundu\.", metin)
        if bitis:
            sayi = int(bitis.group(1))
        elif metin.startswith("Sorun yok"):
            sayi = 0
        elif metin.startswith("\u274c"):                                   # the red cross of a failing broker
            sorunlar.add("hesap: " + metin.lstrip("\u274c \ufe0f").split(":", 1)[0].strip())
        elif "Etkin broker yok" in metin:
            sorunlar.add("ayar: etkin hesap yok")
        elif ":" in metin and not metin.startswith(("\u2705", "\u26a0")) and (
                "YOK" in metin.split(":", 1)[1] or "geçersiz" in metin.split(":", 1)[1]):
            sorunlar.add("ayar: " + metin.split(":", 1)[0].strip())
    return sorunlar, sayi


def geri_alinmali(onceki_kod: int | None, onceki: Ozet, yeni_kod: int, yeni: Ozet) -> bool:
    """Does the new version's check result call for going back to the old version?

    Yes whenever it failed, except when it ran to its end and only reports problems (exit code 1) that the
    installed version's own check reported as well: those were there before the update, so going back would
    not cure them. Anything that cannot be compared with certainty (no earlier result, a report without its
    closing line, a problem this function cannot name) counts as a failure."""
    if yeni_kod == 0:
        return False
    if onceki_kod != 1 or yeni_kod != 1:
        return True
    (eski_sorunlar, eski_sayi), (sorunlar, sayi) = onceki, yeni
    if eski_sayi is None or sayi is None or not sorunlar or sayi != len(sorunlar):
        return True
    return bool(sorunlar - eski_sorunlar)


# --------------------------------------------------------------------------- #
# The update
# --------------------------------------------------------------------------- #
def _secenekler(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="guncelle.py", description="Kopya-trade botunu yeni sürüme günceller.")
    p.add_argument("--hedef", default=None, metavar="KLASOR", help="bot klasörü (varsayılan: bulunduğunuz klasör)")
    p.add_argument("--baslatma", action="store_true", help="güncellemeden sonra botu başlatma")
    p.add_argument("--kontrol-yok", action="store_true", dest="kontrol_yok",
                   help="7. ve 8. adımı (check ve selftest) atla (testler için)")
    p.add_argument("--bekci-yok", action="store_true", dest="bekci_yok",
                   help="bekçiyi (bot çalışmıyorsa Telegram'dan haber veren zamanlanmış görev) kurma")
    p.add_argument("--bekci-kur", action="store_true", dest="bekci_kur",
                   help="güncelleme yapma: yalnızca bekçiyi kur (bot çalışırken de yapılabilir)")
    p.add_argument("--geri-al", action="store_true", dest="geri_al",
                   help="güncelleme yapma: son güncellemeden önceki sürümü ve config.yaml'ı yedekten geri koy")
    return p.parse_args(argv)


def _ayni_klasor(a: Path, b: Path) -> bool:
    try:
        return os.path.samefile(a, b)
    except OSError:
        return os.path.normcase(os.path.abspath(a)) == os.path.normcase(os.path.abspath(b))


def _zipten_devral(hedef: Path, argv: list[str] | None, gunluk: Gunluk) -> int:
    """This file is the INSTALLED copy: find the newest downloaded zip and hand over to its guncelle.py."""
    indirilenler = Path.home() / "Downloads"
    zip_yolu = en_yeni_zip(indirilenler)
    if zip_yolu is None:
        gunluk.yaz(f"Yeni sürümün zip dosyası bulunamadı: {indirilenler} klasöründe {ZIP_KALIBI} yok. "
                   "Önce docs/GUNCELLEME.md dosyasındaki 1. adımı yapın (zip dosyasını indirin).")
        return CIKIS_REDDEDILDI
    try:
        yeni = zipten_ac(zip_yolu)
    except (OSError, zipfile.BadZipFile) as exc:
        gunluk.yaz(f"{zip_yolu.name} açılamadı ({exc}). Dosyayı yeniden indirip tekrar deneyin.")
        return CIKIS_REDDEDILDI
    if yeni is None or _ayni_klasor(yeni, hedef):
        gunluk.yaz(f"{zip_yolu.name} eski bir sürüm (içinde guncelle.py yok). Yeni zip dosyasını indirin.")
        return CIKIS_REDDEDILDI
    gunluk.yaz(f"Yeni sürüm: {zip_yolu.name}")
    secenek = [a for a in (sys.argv[1:] if argv is None else argv)]
    if "--hedef" not in secenek and not any(a.startswith("--hedef=") for a in secenek):
        secenek += ["--hedef", str(hedef)]
    eski_argv = sys.argv
    sys.argv = [str(yeni / "guncelle.py"), *secenek]
    try:
        runpy.run_path(str(yeni / "guncelle.py"), run_name="__main__")
    except SystemExit as cikis:
        return int(cikis.code) if isinstance(cikis.code, int) else (0 if cikis.code is None else 1)
    finally:
        sys.argv = eski_argv
    return CIKIS_TAMAM


def _calisan_bot_bulgusu(hedef: Path, surecler: Callable[[], list[Surec] | None], girdi: Callable[[str], str],
                         gunluk: Gunluk) -> str | None:
    """What shows that the bot is running (None: it is stopped). When the process list cannot be read the
    owner is asked; anything but the typed word counts as "not confirmed"."""
    bulgu, okunabildi = bot_calisiyor_mu(hedef, surecler)
    if bulgu is None and not okunabildi:
        gunluk.yaz("   Botun çalışıp çalışmadığı bu bilgisayarda otomatik anlaşılamadı.")
        try:
            yanit = girdi(f"   Bot KAPALIYSA devam etmek için {ONAY} yazıp Enter'a basın: ")
        except EOFError:
            yanit = ""
        if yanit.strip() != ONAY:
            bulgu = "botun kapalı olduğu doğrulanamadı"
    return bulgu


def _bot_calisiyor_yaz(hedef: Path, bulgu: str, gunluk: Gunluk, ne: str) -> None:
    gunluk.yaz(f"   {ne} yapılmadı: {bulgu}.")
    gunluk.yaz(f"   {DURDUR}")
    if "kilit dosyası" in bulgu:      # the way out when the lock is wrong
        gunluk.yaz(f"   Botun hiçbir pencerede çalışmadığından eminseniz {veri_klasoru(hedef) / KILIT_DOSYASI} "
                   "dosyasını silip satırı tekrar yapıştırın.")


def _bos_yedek_yolu(hedef: Path, zaman: str) -> Path:
    yedek, sayac = hedef / "yedek" / zaman, 1
    while yedek.exists():
        sayac += 1
        yedek = hedef / "yedek" / f"{zaman}-{sayac}"
    return yedek


def _geri_al(hedef: Path, secenek: argparse.Namespace, gunluk: Gunluk, python: str, zaman: str,
             calistir: Calistirici, surecler: Callable[[], list[Surec] | None],
             baslat: Callable[[Path, str], int], girdi: Callable[[str], str]) -> int:
    """``--geri-al``: see the module docstring."""
    gunluk.yaz(f"=== ESKİ SÜRÜME DÖNÜŞ ===  {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    gunluk.yaz(f"Bot klasörü: {hedef}")
    gunluk.yaz()
    if not (hedef / PAKET).is_dir() and (hedef / f"{PAKET}.eski").is_dir():
        try:                                     # an update was cut between the two renames of its step 4
            os.rename(hedef / f"{PAKET}.eski", hedef / PAKET)
        except OSError:
            pass
    eksik = [ad for ad, var in ((f"{PAKET} klasörü", (hedef / PAKET / "__main__.py").is_file()),
                                ("config.yaml", (hedef / "config.yaml").is_file()),
                                (".env", (hedef / ".env").is_file())) if not var]
    if eksik:
        gunluk.adim(1, "Klasör kontrol edildi", False)
        gunluk.yaz(f"   burası botun klasörü değil ({', '.join(eksik)} yok). Hiçbir şey değiştirilmedi.")
        return CIKIS_REDDEDILDI
    gunluk.ac(hedef / f"guncelleme_{zaman}.log")
    try:
        secilen = son_farkli_yedek(hedef)
    except OSError as exc:
        secilen = None
        gunluk.yaz(f"   yedekler okunamadı: {exc}")
    if secilen is None:
        gunluk.adim(1, "Geri dönülecek yedek arandı", False)
        gunluk.yaz("   yedek klasöründe, kurulu sürümden farklı bir yedek yok. Hiçbir şey değiştirilmedi.")
        gunluk.kapat()
        return CIKIS_REDDEDILDI
    gunluk.adim(1, f"Geri dönülecek yedek bulundu ({Path('yedek') / secilen.name})")
    gunluk.yaz(f"   Bu, {yedek_tarihi(secilen)} tarihinde yapılan güncellemeden hemen önceki sürümdür.")

    bulgu = _calisan_bot_bulgusu(hedef, surecler, girdi, gunluk)
    if bulgu is not None:
        gunluk.adim(2, "Bot kapalı mı diye bakıldı", False)
        _bot_calisiyor_yaz(hedef, bulgu, gunluk, "Geri dönüş")
        gunluk.kapat()
        return CIKIS_REDDEDILDI
    gunluk.adim(2, "Bot kapalı mı diye bakıldı")

    simdiki = _bos_yedek_yolu(hedef, zaman + GERI_EKI)
    try:
        yedekle(hedef, simdiki)
    except (Hata, OSError, shutil.Error) as exc:
        gunluk.adim(3, "Şimdiki sürüm yedeklenemedi", False)
        gunluk.yaz(f"   {exc}. Hiçbir şey değiştirilmedi.")
        gunluk.kapat()
        return CIKIS_REDDEDILDI
    gunluk.adim(3, f"Şimdiki sürüm yedeklendi ({Path('yedek') / simdiki.name})")

    kesildi = False
    try:
        try:
            geri_yukle(hedef, secilen)
        except KeyboardInterrupt:
            kesildi = True
            geri_yukle(hedef, secilen)           # never leave a half-restored package: finish it, then stop
    except (OSError, shutil.Error) as exc:
        gunluk.adim(4, "Eski sürüm geri konulamadı", False)
        try:
            geri_yukle(hedef, simdiki)
            gunluk.yaz(f"   {exc}. Şimdiki sürüm yerinde bırakıldı; hiçbir şey değişmedi.")
            gunluk.yaz(f"   Bot başlatılmadı. Başlatmak için: {python} -m {PAKET} run")
        except (OSError, shutil.Error) as exc2:
            gunluk.yaz(f"   {exc}; şimdiki sürüm de yerine konamadı ({exc2}). Yedekler: {secilen} ve {simdiki}. "
                       "Botu başlatmadan önce bu ekranın görüntüsünü gönderin.")
        gunluk.kapat()
        return CIKIS_GERI_ALINDI
    gunluk.adim(4, "Eski sürüm ve o günkü config.yaml geri konuldu")
    if kesildi:
        gunluk.yaz("Durduruldu (Ctrl+C). Eski sürüm kurulu; bot başlatılmadı.")
        gunluk.kapat()
        return CIKIS_KESILDI
    gunluk.yaz("   Not: sonradan config.yaml'da yaptığınız değişiklikler bu dosyada yoktur; şimdiki hali "
               f"{Path('yedek') / simdiki.name} klasöründe duruyor.")
    gunluk.yaz("   Daha eski bir sürüme dönmek için aynı komutu bir kez daha çalıştırın; yeni sürüme dönmek için "
               "güncelleme satırını yeniden yapıştırın.")
    if (hedef / BEKCI_DOSYASI).is_file() and not kilit_yazan_surum(hedef):
        gunluk.yaz(ESKI_SURUM_NOTU)

    if secenek.kontrol_yok:
        gunluk.adim(5, "Ayar ve bağlantı kontrolü atlandı (--kontrol-yok)")
    else:
        gunluk.yaz("   Ayarlar ve hesap bağlantıları kontrol ediliyor (1-3 dakika sürebilir)...")
        try:
            kod = calistir([python, "-m", PAKET, "check"], hedef, KONTROL_SURESI, lambda metin: gunluk.yaz(f"   | {metin}"))
        except KeyboardInterrupt:
            gunluk.yaz("Durduruldu (Ctrl+C). Eski sürüm kurulu; bot başlatılmadı.")
            gunluk.kapat()
            return CIKIS_KESILDI
        gunluk.adim(5, "Ayarlar ve hesap bağlantıları kontrol edildi", kod == 0)
        if kod != 0:
            gunluk.yaz("   Kontrol sorun bildirdi (yukarıdaki satırlar). Sürüm yeniden değiştirilmedi.")

    if secenek.baslatma:
        gunluk.adim(6, "Bot başlatılmadı (ESKİ sürüm kurulu)")
        gunluk.yaz(f"   Başlatmak için: {python} -m {PAKET} run")
        gunluk.kapat()
        return CIKIS_TAMAM
    gunluk.adim(6, "Bot başlatılıyor (ESKİ sürüm). Durdurmak için bu pencerede Ctrl+C")
    gunluk.yaz()
    gunluk.kapat()
    return baslat(hedef, python)


def main(
    argv: list[str] | None = None,
    *,
    calistir: Calistirici = komut_calistir,
    surecler: Callable[[], list[Surec] | None] = surec_listesi,
    baslat: Callable[[Path, str], int] = botu_baslat,
    girdi: Callable[[str], str] = input,
    yeni_agac: Path | None = None,
    gorev: Any = OTOMATIK,
) -> int:
    """Run the update. The keyword arguments replace the outside world in tests: ``calistir`` runs commands,
    ``surecler`` lists processes, ``baslat`` starts the bot, ``girdi`` asks the owner, ``yeni_agac`` is the
    new tree (default: the folder of this file), ``gorev`` runs schtasks for the watchman (default: the real
    one on Windows, None = nothing to register elsewhere)."""
    if gorev is OTOMATIK:
        gorev = varsayilan_gorev()
    for akis in (sys.stdout, sys.stderr):
        try:
            akis.reconfigure(errors="replace")       # legacy code pages: never crash on Turkish letters
        except (AttributeError, ValueError):
            pass
    secenek = _secenekler(argv)
    gunluk = Gunluk()
    hedef = Path(secenek.hedef).expanduser() if secenek.hedef else Path.cwd()
    hedef = Path(os.path.abspath(hedef))
    yeni = Path(os.path.abspath(yeni_agac if yeni_agac is not None else Path(__file__).parent))
    python = sys.executable
    zaman = datetime.now().strftime("%Y%m%d-%H%M%S")

    if secenek.bekci_kur:
        return yalniz_bekci(hedef, python, gorev, gunluk)
    if secenek.geri_al:
        veri = veri_klasoru(hedef)

        def baslat_kilitli(klasor: Path, yorumlayici: str) -> int:
            guncelleme_kilidi_kaldir(veri, bot_basliyor=True)
            return baslat(klasor, yorumlayici)

        guncelleme_kilidi_yaz(veri)               # the watchman stays silent while the bot is down on purpose
        try:
            return _geri_al(hedef, secenek, gunluk, python, zaman, calistir, surecler, baslat_kilitli, girdi)
        finally:
            guncelleme_kilidi_kaldir(veri)
    if _ayni_klasor(yeni, hedef):
        return _zipten_devral(hedef, argv, gunluk)

    gunluk.yaz(f"=== BOT GÜNCELLEME ===  {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    gunluk.yaz(f"Bot klasörü: {hedef}")
    gunluk.yaz(f"Yeni sürüm : {yeni}")
    gunluk.yaz()

    def komut(*parcalar: str, sure: float = KONTROL_SURESI, ekranda: bool = True) -> tuple[int, list[str]]:
        satirlar: list[str] = []

        def satir(metin: str) -> None:
            satirlar.append(metin)
            (gunluk.yaz if ekranda else gunluk.dosyaya)(f"   | {metin}")

        return calistir([python, *parcalar], hedef, sure, satir), satirlar

    # ---- 1. folders ------------------------------------------------------
    if not (hedef / PAKET).is_dir() and (hedef / f"{PAKET}.eski").is_dir():
        try:                                     # an earlier run was cut between the two renames of step 4
            os.rename(hedef / f"{PAKET}.eski", hedef / PAKET)
        except OSError:
            pass
    eksik = [ad for ad, var in ((f"{PAKET} klasörü", (hedef / PAKET / "__main__.py").is_file()),
                                ("config.yaml", (hedef / "config.yaml").is_file()),
                                (".env", (hedef / ".env").is_file())) if not var]
    yeni_eksik = [ad for ad in YENI_GEREKLI if not (yeni / ad).is_file()]
    sorun = None
    if eksik:
        sorun = (f"burası botun klasörü değil ({', '.join(eksik)} yok). Satırı botu çalıştırdığınız sekmede, "
                 "botun klasöründeyken yapıştırın")
    elif yeni_eksik:
        sorun = f"indirilen yeni sürüm eksik ({', '.join(yeni_eksik)} yok). Zip dosyasını yeniden indirin"
    elif sys.version_info < (3, 11):
        sorun = f"bu sürüm Python 3.11 veya yenisini ister (burada {sys.version.split()[0]})"
    if sorun:
        gunluk.adim(1, "Klasörler kontrol edildi", False)
        gunluk.yaz(f"   {sorun}.")
        gunluk.yaz("   Hiçbir şey değiştirilmedi.")
        return CIKIS_REDDEDILDI
    gunluk.ac(hedef / f"guncelleme_{zaman}.log")
    gunluk.adim(1, "Klasörler kontrol edildi")
    for ad in ("venv", ".venv"):
        if (hedef / ad).is_dir() and not _ayni_klasor(Path(sys.prefix), hedef / ad):
            gunluk.yaz(f"   Uyarı: bu komut botun kendi Python'u ({ad} klasörü) ile çalışmıyor: {python}")
            break

    # ---- 2. the bot must be stopped ---------------------------------------
    bulgu = _calisan_bot_bulgusu(hedef, surecler, girdi, gunluk)
    if bulgu is not None:
        gunluk.adim(2, "Bot kapalı mı diye bakıldı", False)
        _bot_calisiyor_yaz(hedef, bulgu, gunluk, "Güncelleme")
        gunluk.kapat()
        return CIKIS_REDDEDILDI
    gunluk.adim(2, "Bot kapalı mı diye bakıldı")
    veri = veri_klasoru(hedef)
    guncelleme_kilidi_yaz(veri)                   # the watchman stays silent while the bot is down on purpose

    # The installed version's own check, before anything is changed: step 7 compares against it.
    onceki_kod: int | None = None
    onceki: Ozet = (set(), None)
    if not secenek.kontrol_yok:
        gunluk.yaz("   Güncellemeden önceki durum kaydediliyor (1-3 dakika sürebilir)...")
        try:
            onceki_kod, satirlar = komut("-m", PAKET, "check", ekranda=False)
        except KeyboardInterrupt:
            guncelleme_kilidi_kaldir(veri)
            gunluk.yaz("Durduruldu (Ctrl+C); hiçbir şey değiştirilmedi, bot başlatılmadı.")
            gunluk.yaz("Botu başlatmak için aynı satırı tekrar yapıştırın.")
            gunluk.kapat()
            return CIKIS_KESILDI
        onceki = kontrol_ozeti(satirlar)
        gunluk.yaz("   Güncellemeden önce: " + (
            "kontrol sorunsuzdu." if onceki_kod == 0 else
            f"kontrol şu sorunları bildiriyordu: {', '.join(sorted(onceki[0])) or 'okunamadı'}."
            if onceki_kod == 1 else f"kontrol çalıştırılamadı (çıkış kodu {onceki_kod})."))

    # ---- 3. to 6.: any failure puts the old installation back ---------------
    yedek = _bos_yedek_yolu(hedef, zaman)
    yedek_var = False
    dogrulandi = False                        # step 7 passed (or was skipped on request): the new version stays
    eski_surum: str | None = None             # set: the OLD version was restored, with the reason
    adim = 3
    try:
        try:
            yedekle(hedef, yedek)
            yedek_var = True
            gunluk.adim(3, f"Eski sürüm yedeklendi ({Path('yedek') / yedek.name})")
            gunluk_temizligi(veri, gunluk)        # never raises; a problem is one line on the screen

            adim = 4
            kur(hedef, yeni)
            gunluk.adim(4, "Yeni sürüm kuruldu")

            adim = 5
            if gereksinimler(yedek / "requirements.txt") == gereksinimler(hedef / "requirements.txt"):
                gunluk.adim(5, "Gerekli paketler aynı, kurulum gerekmedi")
            else:
                gunluk.yaz("   Gerekli paketler değişmiş; kuruluyor (birkaç dakika sürebilir)...")
                kod, satirlar = komut("-m", "pip", "install", "--disable-pip-version-check", "-r",
                                      "requirements.txt", sure=PIP_SURESI, ekranda=False)
                for metin in [m for m in satirlar if m.strip()][-(3 if kod == 0 else 15):]:
                    gunluk.yaz(f"   | {metin}")           # the short result; the whole output is in the log file
                if kod != 0:
                    raise Hata(f"gerekli paketler kurulamadı (pip çıkış kodu {kod})")
                gunluk.adim(5, "Gerekli paketler kuruldu")

            adim = 6
            kod, satirlar = komut("-m", PAKET, "migrate")
            if kod != 0:
                raise Hata(f"ayar dosyası (config.yaml) güncellenemedi (çıkış kodu {kod})")
            gunluk.adim(6, "Ayar dosyası (config.yaml) güncel hale getirildi")
        except Hata as hata:
            eski_surum = str(hata)
        except (OSError, shutil.Error) as exc:
            eski_surum = f"dosya işlemi başarısız: {exc}"
        if eski_surum is not None:
            gunluk.adim(adim, {3: "Eski sürüm yedeklenemedi", 4: "Yeni sürüm kurulamadı",
                               5: "Gerekli paketler kurulamadı", 6: "Ayar dosyası güncellenemedi"}[adim], False)

        # ---- 7. check: the new version must work here, else the old one comes back ------
        if eski_surum is None and secenek.kontrol_yok:
            dogrulandi = True
            gunluk.adim(7, "Ayar ve bağlantı kontrolü atlandı (--kontrol-yok)")
            gunluk.adim(8, "Sağlık kontrolü atlandı (--kontrol-yok)")
        elif eski_surum is None:
            adim = 7
            gunluk.yaz("   Ayarlar ve hesap bağlantıları kontrol ediliyor (1-3 dakika sürebilir)...")
            kod, satirlar = komut("-m", PAKET, "check")
            if kod == 1 and geri_alinmali(onceki_kod, onceki, kod, kontrol_ozeti(satirlar)):
                gunluk.yaz("   Kontrol sorun bildirdi; geçici olabilir, 15 saniye sonra bir kez daha deneniyor...")
                time.sleep(BEKLEME)
                kod, satirlar = komut("-m", PAKET, "check")
            ozet = kontrol_ozeti(satirlar)
            sorunlar = ozet[0]
            if kod == 0:
                dogrulandi = True
                gunluk.adim(7, "Ayarlar ve hesap bağlantıları kontrol edildi")
            elif geri_alinmali(onceki_kod, onceki, kod, ozet):
                gunluk.adim(7, "Ayarlar ve hesap bağlantıları kontrol edildi", False)
                yeni_sorun = ", ".join(sorted(sorunlar - onceki[0]))
                eski_surum = (f"yeni sürümün kontrolü (check) başarısız oldu (çıkış kodu {kod})"
                              + (f"; güncellemeden önce olmayan sorun: {yeni_sorun}" if yeni_sorun else "")
                              + "; ayrıntısı yukarıdaki satırlarda")
            else:
                dogrulandi = True
                gunluk.adim(7, "Ayarlar ve hesap bağlantıları kontrol edildi", False)
                gunluk.yaz(f"   Kontrol sorun bildirdi ({', '.join(sorted(sorunlar))}), ama aynı sorun güncellemeden "
                           "önce de vardı: güncelleme bunu çözmez, geri almak da çözmez.")
                gunluk.yaz("   Bu yüzden güncelleme GERİ ALINMADI; yeni sürüm kurulu kalıyor. Sorunu ayrıca giderin.")

        if eski_surum is not None:
            if yedek_var:
                try:
                    geri_yukle(hedef, yedek)
                    geri = "ESKİ SÜRÜM GERİ YÜKLENDİ (kod ve config.yaml güncellemeden önceki haline döndü)."
                except (OSError, shutil.Error) as exc:
                    geri = (f"ESKİ SÜRÜM GERİ YÜKLENEMEDİ ({exc}). Yedek burada duruyor: {yedek}. Botu "
                            "başlatmadan önce bu ekranın görüntüsünü gönderin.")
                    secenek.baslatma = True
            else:
                geri = "Kurulu sürüme dokunulmadı."
            gunluk.yaz()
            gunluk.yaz("!!! GÜNCELLEME YAPILMADI !!!")
            gunluk.yaz(f"!!! Sebep: {eski_surum}.")
            gunluk.yaz(f"!!! {geri}")
            gunluk.yaz("!!! Sorunu giderdikten sonra aynı satırı tekrar yapıştırabilirsiniz. Bu ekranın "
                       "görüntüsünü" + (f" ya da {gunluk.yol.name} dosyasını" if gunluk.yol else "") + " gönderin.")
            gunluk.yaz()
        elif not secenek.kontrol_yok:
            # ---- 8. selftest: problems are listed for the owner, never a rollback ----------
            gunluk.yaz("   Sağlık kontrolü yapılıyor: hesaplar, semboller, fiyat ve miktar (1-3 dakika sürebilir)...")
            kod, satirlar = komut("-m", PAKET, "selftest")
            if kod == 0:
                gunluk.adim(8, "Sağlık kontrolü yapıldı, sorun yok")
            else:
                gunluk.adim(8, "Sağlık kontrolü sorun buldu" if kod == 1
                            else f"Sağlık kontrolü çalıştırılamadı (çıkış kodu {kod})", False)
                gunluk.yaz("   Sorunlar yukarıda SONUÇ bölümünde yazıyor. Güncelleme geri ALINMADI; "
                           + ("yeni sürüm kurulu kalıyor" if secenek.baslatma else "bot yeni sürümle başlatılıyor")
                           + ". Ekran görüntüsünü gönderin.")
    except KeyboardInterrupt:
        gunluk.yaz()
        if yedek_var and adim >= 4 and eski_surum is None and not dogrulandi:
            try:
                geri_yukle(hedef, yedek)
                gunluk.yaz("Güncelleme yarıda kesildi (Ctrl+C). ESKİ SÜRÜM GERİ YÜKLENDİ; bot başlatılmadı.")
            except (OSError, shutil.Error) as exc:
                gunluk.yaz(f"Güncelleme yarıda kesildi ve eski sürüm geri yüklenemedi ({exc}). Yedek: {yedek}")
        elif dogrulandi:
            gunluk.yaz("Durduruldu (Ctrl+C). Yeni sürüm kurulu; bot başlatılmadı.")
        else:
            gunluk.yaz("Güncelleme yarıda kesildi (Ctrl+C); kurulu sürüm değişmedi, bot başlatılmadı.")
        gunluk.yaz("Botu başlatmak için aynı satırı tekrar yapıştırın.")
        gunluk.kapat()
        guncelleme_kilidi_kaldir(veri)
        return CIKIS_KESILDI
    except Exception as exc:      # noqa: BLE001 - a bug here must not leave a half-installed bot
        gunluk.yaz(f"Beklenmeyen hata: {exc}")
        gunluk.yaz(traceback.format_exc())
        if yedek_var:
            try:
                geri_yukle(hedef, yedek)
                gunluk.yaz("!!! GÜNCELLEME YAPILMADI; ESKİ SÜRÜM GERİ YÜKLENDİ. Bu ekranın görüntüsünü gönderin.")
            except (OSError, shutil.Error) as exc2:
                gunluk.yaz(f"!!! Eski sürüm geri yüklenemedi ({exc2}). Yedek: {yedek}")
                secenek.baslatma = True
        eski_surum = eski_surum or f"beklenmeyen hata: {exc}"

    # ---- the watchman: optional, never a reason to fail or undo the update ------
    if eski_surum is None:
        try:
            bekci_adimi(hedef, python, gorev, secenek.bekci_yok, gunluk)
        except KeyboardInterrupt:
            guncelleme_kilidi_kaldir(veri)
            gunluk.yaz()
            gunluk.yaz("Durduruldu (Ctrl+C). Yeni sürüm kurulu; bot başlatılmadı.")
            gunluk.yaz("Botu başlatmak için aynı satırı tekrar yapıştırın.")
            gunluk.kapat()
            return CIKIS_KESILDI

    # ---- 9. start the bot -----------------------------------------------------
    surum = "ESKİ sürüm" if eski_surum is not None else "Yeni sürüm"
    if secenek.baslatma:
        guncelleme_kilidi_kaldir(veri)
        gunluk.adim(9, f"Bot başlatılmadı ({surum} kurulu)")
        gunluk.yaz(f"   Başlatmak için: {python} -m {PAKET} run")
        gunluk.kapat()
        return CIKIS_TAMAM if eski_surum is None else CIKIS_GERI_ALINDI
    gunluk.adim(9, f"Bot başlatılıyor ({surum}). Durdurmak için bu pencerede Ctrl+C")
    gunluk.yaz()
    gunluk.kapat()
    guncelleme_kilidi_kaldir(veri, bot_basliyor=True)      # the bot's own lock file needs a moment
    try:
        return baslat(hedef, python)
    finally:
        guncelleme_kilidi_kaldir(veri)            # the bot has ended: from here on the watchman reports it


if __name__ == "__main__":
    sys.exit(main())
