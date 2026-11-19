#!/usr/bin/env python
"""Taşıma betiği: moves the bot to a NEW Windows server. Owner steps: docs/TASIMA.md.

Standard library only, so that ``kur`` runs with a plain system Python (3.11 or newer) from any folder, before
the bot has a Python environment of its own. Nothing of ``copytrader`` is imported into this process; what needs
PyYAML (reading the broker list of config.yaml, editing one key of it) runs as a subprocess of the bot's own
Python environment. The pieces guncelle.py already has (is the bot running, running commands, the optional
packages, the watchman) are taken from the guncelle.py of the bot folder itself, so both do them the same way.

Commands
--------
``python tasi.py paketle`` (OLD server, in the bot folder; the bot must be stopped):
    writes ``copytrader-tasima-YYYYMMDD-HHMM.zip`` into the bot folder: the whole bot folder (config.yaml,
    .env, data\\ with the database and the Telegram session, docs, ...) without venv\\, __pycache__,
    karne_paketleri\\ (reinstalled on the new server), yedek\\ (``--yedekli`` takes it along), log files older
    than 7 days, the bot's run-time marks (bot.lock, stop.request, guncelleme.lock, bekci.*) and the packages
    themselves. ``tasima_manifest.json`` inside lists every file with its sha256, the MetaTrader 5 terminals
    the accounts need (name, terminal path, company, server; never a login or a password), the packages of the
    bot's Python environment and the watchman's settings. The package is read back and checked before it gets
    its final name.
``python tasi.py kontrol <zip>``: checks a package (every file against the manifest) without installing.
``python tasi.py kur <zip>`` (NEW server): the steps below, each ending in TAMAM or SORUN, and a summary.

    1. the package is checked                     7. the MetaTrader 5 terminals are where config.yaml /
    2. the bot folder is chosen                      data\\mt5_terminal_<hesap>.json say (else looked for)
    3. the files are extracted and checked        8. ``copytrader check`` (a failure: the bot is not started)
    4. the Python environment (venv) is made      9. ``copytrader selftest`` (problems are listed only)
    5. the packages are installed                10. the watchman (bekçi) is registered (optional)
    6. the packages are tried (import)           11. the bot is started in a window of its own

    A step that fails stops there (except 9 and 10) and says what to do; running the same command again
    afterwards goes on from where it is: a folder that this package was extracted into is not extracted again.

Options of ``kur``: ``--hedef <folder>`` (default: the old server's path when its parent folder exists here,
else C:\\copytrader), ``--uzerine-yaz`` (a folder that is in the way is renamed to ``<name>.eski-<time>``, never
deleted), ``--evet`` (answer the questions with yes: the target folder, a terminal found at another path),
``--baslatma`` (do not start the bot), ``--bekci-yok`` (do not register the watchman), ``--kontrol-yok``
(skip 8 and 9; for tests).

Exit codes (as guncelle.py): 0 TAMAM; 1 SORUN: something needs doing by hand (the screen says what), the bot was
not started; 2 nothing was done (the bot is running, the package is damaged, the folder is in the way, wrong
command); 3 (kur only) installed and started (not started with --baslatma), but a step says SORUN (selftest, an
optional account that did not connect, the watchman): the owner should read those lines; 130 stopped with Ctrl+C.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import uuid
import zipfile
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath
from types import ModuleType
from typing import Any

PAKET = "copytrader"
MANIFEST = "tasima_manifest.json"
ZIP_ON_EKI = "copytrader-tasima-"
KURULUM_IZI = "tasima_kurulum.json"            # in the new bot folder: which package was extracted into it
BICIM = 1                                      # version of the manifest layout
CIKIS_TAMAM, CIKIS_SORUN, CIKIS_REDDEDILDI, CIKIS_KESILDI = 0, 1, 2, 130
CIKIS_UYARILI = 3       # kur: installed (and started unless --baslatma) but some step says SORUN (selftest, an
                        # optional account that did not connect, the watchman): tell the owner
MOVED_FILE_ADI = "TASINDI.txt"   # copytrader/__main__.py MOVED_FILE: a bot version that knows the moved mark
ONAY = "EVET"
GUNLUK_OMRU_GUN = 7                            # log files older than this stay behind
PARCA = 1024 * 1024
# Folders of the bot folder that never travel (venv: made anew; karne_paketleri: binary wheels, installed anew)
HARIC_UST = ("venv", ".venv", "karne_paketleri", f"{PAKET}.eski", f"{PAKET}.yeni", ".pytest_cache")
YEDEK = "yedek"
# Run-time marks in the data folder: they describe THIS server's processes, never the new one's
# The mark paketle leaves on the OLD server: there ``copytrader run`` (and guncelle.py) refuse to start the bot.
TASINDI = "TASINDI.txt"
TASINMAYAN_VERI = ("bot.lock", "stop.request", "guncelleme.lock", "bekci.dene", "bekci.json", TASINDI)
GEREKLI = ("config.yaml", ".env", f"{PAKET}/__main__.py", "requirements.txt", "guncelle.py", "bekci.py")
TERMINAL_EXE = "terminal64.exe"
PIN_ON_EKI = "mt5_terminal_"
MT_TURLERI = ("mt5", "mt5_bridge")
# Optional packages requirements.txt names only in comments; installed when an account needs them
MT5_PAKETI, METAAPI_PAKETI = "MetaTrader5", "metaapi-cloud-sdk"
YARDIMCI_SURESI = 300.0
VENV_SURESI = 600.0
PIP_SURESI = 1800.0
KONTROL_SURESI = 900.0
BASLAMA_BEKLEME = 180.0                        # seconds to wait for the started bot's bot.lock
GUNCELLEME_KILIDI = "guncelleme.lock"
KILIT_OMRU, BASLANGIC_PAYI = 1800.0, 120.0     # as guncelle.py
DURDURMA = r".\venv\Scripts\python.exe -m copytrader stop"
GIZLI_UYARI = ("Bu dosyada hesap şifreleri ve anahtarları (.env), Telegram oturumu ve veritabanı var. Yalnızca "
               "doğrudan yeni sunucuya kopyalayın (Uzak Masaüstü'nde kopyala-yapıştır). Hiçbir yere yüklemeyin, "
               "e-postayla ya da sohbette (Telegram, WhatsApp, Claude dahil) göndermeyin. Taşıma bitince bu "
               "dosyayı iki sunucudan da silin.")

# Run with the bot's own Python (PyYAML): the broker list of config.yaml, ONLY the fields below. The values are
# the raw text of the file (``${VAR}`` stays as it is: nothing of .env is read). Logins / passwords never leave.
AYAR_BETIGI = r"""
import json, sys
import yaml
with open(sys.argv[1], encoding="utf-8-sig") as f:
    raw = yaml.safe_load(f.read()) or {}
def d(v):
    return v if v is None or isinstance(v, (str, int, float, bool)) else str(v)
tg = raw.get("telegram") if isinstance(raw.get("telegram"), dict) else {}
out = {"data_dir": d(raw.get("data_dir")), "session_path": d(tg.get("session_path")), "brokers": []}
for b in raw.get("brokers") or []:
    if isinstance(b, dict):
        out["brokers"].append({k: d(b.get(k)) for k in
                               ("name", "type", "enabled", "terminal_path", "broker_company", "server")})
out["python"] = sys.version.split()[0]
print("TASI_AYAR " + json.dumps(out, ensure_ascii=True))
"""
# Run with the bot's own Python in the bot folder: one key of one broker block (copytrader.migrate)
DUZENLE_BETIGI = r"""
import sys
from copytrader.migrate import MigrationError, set_broker_value
try:
    yedek = set_broker_value(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
except MigrationError as exc:
    print("TASI_HATA " + str(exc))
    raise SystemExit(2)
print("TASI_YEDEK " + yedek.name)
"""

Calistirici = Callable[[list[str], Path, float, Callable[[str], None]], int]
Yardimci = Callable[[list[str], Path, float], tuple[int, str]]
OTOMATIK: Any = object()


class Hata(Exception):
    """A step failed; ``str(err)`` is the Turkish reason shown to the owner."""


# --------------------------------------------------------------------------- #
# Output: console + log file, and the summary
# --------------------------------------------------------------------------- #
class Gunluk:
    """Prints every line and appends it to the log file (lines written before the file is opened are kept).
    The same interface as guncelle.Gunluk, so that its steps can write here."""

    def __init__(self) -> None:
        self.yol: Path | None = None
        self._dosya: Any = None
        self._bekleyen: list[str] = []
        self.ozet: list[tuple[str, bool, str]] = []

    def ac(self, yol: Path) -> None:
        try:
            self._dosya = open(yol, "a", encoding="utf-8")
            self.yol = yol
            for satir in self._bekleyen:
                self._dosya.write(satir + "\n")
            self._dosya.flush()
        except OSError:
            self._dosya = None
        self._bekleyen = []

    def _dosyaya_yaz(self, metin: str) -> None:
        if self._dosya is None:
            self._bekleyen.append(metin)
            return
        try:
            self._dosya.write(metin + "\n")
            self._dosya.flush()
        except (OSError, ValueError):
            pass

    def yaz(self, metin: str = "") -> None:
        try:
            print(metin, flush=True)
        except (OSError, ValueError):
            pass
        self._dosyaya_yaz(metin)

    def dosyaya(self, metin: str) -> None:
        self._dosyaya_yaz(metin)

    def adim(self, no: int, metin: str, tamam: bool = True, ne_yapmali: str = "") -> None:
        self.yaz(f"{no}. {metin}: {'TAMAM' if tamam else 'SORUN'}")
        if ne_yapmali:
            for satir in ne_yapmali.splitlines():
                self.yaz(f"   {satir}")
        self.ozet.append((f"{no}. {metin}", tamam, ne_yapmali))

    def ozeti_yaz(self) -> None:
        if not self.ozet:
            return
        self.yaz()
        self.yaz("=== ÖZET ===")
        for metin, tamam, ne in self.ozet:
            self.yaz(f"{metin}: {'TAMAM' if tamam else 'SORUN'}")
            if not tamam and ne:
                satirlar = ne.splitlines()
                self.yaz(f"   Ne yapmalı: {satirlar[0]}")
                for satir in satirlar[1:]:
                    self.yaz(f"   {satir}")

    def kapat(self) -> None:
        if self._dosya is not None:
            try:
                self._dosya.close()
            except OSError:
                pass
            self._dosya = None


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def guncelle_yukle(klasor: Path) -> ModuleType:
    """guncelle.py of ``klasor`` as a module (standard library only, never imports copytrader)."""
    yol = klasor / "guncelle.py"
    if not yol.is_file():
        raise Hata(f"{yol} yok")
    spec = importlib.util.spec_from_file_location("guncelle_tasima", yol)
    if spec is None or spec.loader is None:
        raise Hata(f"{yol} yüklenemedi")
    modul = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modul)
    return modul


def sha256_dosya(yol: Path) -> str:
    h = hashlib.sha256()
    with open(yol, "rb") as f:
        for parca in iter(lambda: f.read(PARCA), b""):
            h.update(parca)
    return h.hexdigest()


def yer_tutucu(deger: Any) -> bool:
    return isinstance(deger, str) and "${" in deger


def bot_klasoru_mu(klasor: Path) -> bool:
    return (klasor / PAKET / "__main__.py").is_file() and (klasor / "config.yaml").is_file()


def venv_python(hedef: Path) -> Path:
    return hedef / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def bot_python(hedef: Path) -> str:
    """The Python of the bot folder (venv or .venv), else the one this runs with."""
    for ad in ("venv", ".venv"):
        for alt in ("Scripts/python.exe", "bin/python"):
            yol = hedef / ad / alt
            if yol.is_file():
                return str(yol)
    return sys.executable


def yardimci_calistir(komut: list[str], klasor: Path, sure: float) -> tuple[int, str]:
    """A short helper run: (exit code, its standard output). Its error output is never shown (it could quote
    a line of config.yaml)."""
    ortam = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUTF8="1")
    try:
        sonuc = subprocess.run(komut, cwd=str(klasor), env=ortam, capture_output=True, timeout=sure,
                               stdin=subprocess.DEVNULL)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return -1, type(exc).__name__
    return sonuc.returncode, (sonuc.stdout or b"").decode("utf-8", "replace")


def isaretli_satir(cikti: str, isaret: str) -> str | None:
    for satir in cikti.splitlines():
        if satir.startswith(isaret + " "):
            return satir[len(isaret) + 1:].strip()
    return None


def ayarlari_oku(python: str, hedef: Path, yardimci: Yardimci) -> dict[str, Any]:
    """data_dir, telegram.session_path and the brokers (name, type, enabled, terminal_path, broker_company,
    server) of config.yaml, read with the bot's own Python. Raises Hata."""
    kod, cikti = yardimci([python, "-c", AYAR_BETIGI, "config.yaml"], hedef, YARDIMCI_SURESI)
    satir = isaretli_satir(cikti, "TASI_AYAR")
    if kod != 0 or satir is None:
        raise Hata(f"config.yaml okunamadı (çıkış kodu {kod}); dosyanın biçimi bozuk olabilir")
    try:
        ayar = json.loads(satir)
    except ValueError as exc:
        raise Hata("config.yaml okunamadı") from exc
    if not isinstance(ayar, dict):
        raise Hata("config.yaml okunamadı")
    return ayar


def klasor_icinde(kok: Path, deger: Any, varsayilan: str) -> tuple[Path, str | None]:
    """(absolute path, path relative to ``kok`` or None when it is outside) of a config path value."""
    metin = str(deger).strip().strip("'\"") if isinstance(deger, str) and deger.strip() else varsayilan
    if "${" in metin:
        metin = varsayilan
    yol = Path(metin).expanduser()
    if not yol.is_absolute():
        yol = kok / yol
    yol = Path(os.path.abspath(yol))
    try:
        return yol, yol.relative_to(Path(os.path.abspath(kok))).as_posix()
    except ValueError:
        return yol, None


def pinleri_oku(veri: Path) -> dict[str, dict[str, str]]:
    """The terminal pins of the bot (data\\mt5_terminal_<hesap>.json): broker -> folder, path, company, server.
    The account number in them is never taken."""
    sonuc: dict[str, dict[str, str]] = {}
    try:
        dosyalar = sorted(veri.glob(f"{PIN_ON_EKI}*.json"))
    except OSError:
        return sonuc
    for dosya in dosyalar:
        try:
            veri_ = json.loads(dosya.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(veri_, dict) and veri_.get("broker"):
            sonuc[str(veri_["broker"])] = {"dosya": dosya.name, **{k: str(veri_.get(k) or "") for k in
                                                                     ("folder", "path", "company", "server")}}
    return sonuc


def _exe_in(klasor: str) -> str:
    ayrac = "\\" if ("\\" in klasor or ":" in klasor[:3]) else "/"
    return klasor.rstrip("\\/") + ayrac + TERMINAL_EXE


def terminal_listesi(ayar: dict[str, Any], pinler: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    """The MetaTrader 5 terminals the ENABLED mt5 / mt5_bridge accounts need: the account's terminal_path, else
    the terminal it was pinned to (data\\mt5_terminal_<hesap>.json). No login, no password."""
    sonuc: list[dict[str, Any]] = []
    for b in ayar.get("brokers") or []:
        if not isinstance(b, dict) or b.get("type") not in MT_TURLERI or b.get("enabled") is False:
            continue
        ad = str(b.get("name") or "")
        pin = pinler.get(ad) or {}
        yol, kaynak = b.get("terminal_path"), None
        if isinstance(yol, str) and yol.strip():
            kaynak = "config"
            yol = yol.strip().strip('"')
        elif pin.get("path") or pin.get("folder"):
            kaynak, yol = "pin", pin.get("path") or _exe_in(pin["folder"])
        else:
            yol = None
        sirket = b.get("broker_company") if isinstance(b.get("broker_company"), str) else None
        sunucu = b.get("server") if isinstance(b.get("server"), str) and not yer_tutucu(b.get("server")) else None
        sonuc.append({
            "ad": ad, "tur": b.get("type"), "terminal_path": yol, "kaynak": kaynak,
            "broker_company": sirket, "server": sunucu or (pin.get("server") or None),
            "terminal_sirketi": pin.get("company") or None, "pin_dosyasi": pin.get("dosya"),
        })
    return sonuc


def terminal_etiketi(t: dict[str, Any]) -> str:
    """What the owner knows the terminal by: its company (Tickmill, BEM...), else the server's first word."""
    for aday in (t.get("broker_company"), t.get("server"), t.get("terminal_sirketi")):
        if isinstance(aday, str) and aday.strip() and "metaquotes" not in aday.casefold():
            return aday.split("-")[0].strip()
    return "MetaTrader 5"


# --------------------------------------------------------------------------- #
# The package: which files, the manifest, the check
# --------------------------------------------------------------------------- #
def gunluk_dosyasi_mi(ad: str) -> bool:
    return ad.endswith(".log") or bool(re.search(r"\.log\.[\w.-]+$", ad))


def paket_dosyalari(hedef: Path, veri_goreli: str | None, yedekli: bool, haric: set[Path],
                    simdi: float | None = None) -> tuple[list[tuple[Path, str]], int]:
    """(file, name in the zip) of everything that travels, and the number of old log files left behind."""
    simdi = time.time() if simdi is None else simdi
    sinir = simdi - GUNLUK_OMRU_GUN * 86400
    haric_ust = set(HARIC_UST) | (set() if yedekli else {YEDEK})
    veri_onek = (veri_goreli.rstrip("/") + "/") if veri_goreli and veri_goreli != "." else ""
    sonuc: list[tuple[Path, str]] = []
    eski_gunluk = 0
    haric_anahtar = {os.path.normcase(os.path.abspath(y)) for y in haric}
    veri_bag = hedef / veri_goreli if veri_onek else None
    # The data folder itself may be a link (symlink / junction) to another drive: it is followed (only it)
    veri_bagli = veri_bag is not None and (veri_bag.is_symlink() or _junction_mi(veri_bag))
    yuruyusler = [(hedef, PurePosixPath("."))]
    if veri_bagli:
        yuruyusler.append((veri_bag, PurePosixPath(veri_goreli)))
    for disk_kok, arc_kok in yuruyusler:
        for kok, klasorler, dosyalar in os.walk(disk_kok):
            kok_yol = Path(kok)
            goreli_kok = PurePosixPath(arc_kok, kok_yol.relative_to(disk_kok).as_posix())
            klasorler[:] = sorted(k for k in klasorler if k != "__pycache__"
                                  and not (goreli_kok == PurePosixPath(".") and k in haric_ust)
                                  and not (kok_yol / k).is_symlink() and not _junction_mi(kok_yol / k))
            for ad in sorted(dosyalar):
                yol = kok_yol / ad
                arc = (goreli_kok / ad).as_posix()
                if yol.is_symlink() or ad.endswith((".pyc", ".pyo")):
                    continue
                if os.path.normcase(os.path.abspath(yol)) in haric_anahtar:
                    continue
                if goreli_kok == PurePosixPath(".") and ad.startswith(ZIP_ON_EKI) and (ad.endswith(".zip")
                                                                              or ad.endswith(".zip.part")):
                    continue
                if veri_onek and arc.startswith(veri_onek) and arc[len(veri_onek):] in TASINMAYAN_VERI:
                    continue
                if gunluk_dosyasi_mi(ad):
                    try:
                        if yol.stat().st_mtime < sinir:
                            eski_gunluk += 1
                            continue
                    except OSError:
                        continue
                sonuc.append((yol, arc))
    return sonuc, eski_gunluk


def _junction_mi(yol: Path) -> bool:
    """A Windows junction (not a symlink to Python before 3.12's is_junction)."""
    try:
        return bool(getattr(yol, "is_junction", lambda: False)())
    except OSError:
        return False


def guvensiz_ad(ad: str) -> bool:
    """A name in the zip that could land outside the target folder."""
    if not ad or ad.startswith(("/", "\\")) or "\\" in ad or ":" in ad:
        return True
    return any(parca in ("..", "") for parca in ad.rstrip("/").split("/"))


def paketi_dogrula(zip_yolu: Path) -> tuple[dict[str, Any] | None, list[str]]:
    """(manifest, problems) of a package: every file is read and its sha256 compared with the manifest; files
    the manifest does not list, missing ones and unsafe names are problems."""
    sorunlar: list[str] = []
    try:
        arsiv = zipfile.ZipFile(zip_yolu)
    except (OSError, zipfile.BadZipFile) as exc:
        return None, [f"dosya açılamadı ({type(exc).__name__}: {exc}); kopyalama yarım kalmış olabilir"]
    with arsiv:
        try:
            manifest = json.loads(arsiv.read(MANIFEST).decode("utf-8"))
        except KeyError:
            return None, [f"pakette {MANIFEST} yok: bu dosya bir taşıma paketi değil"]
        except (ValueError, zipfile.BadZipFile, OSError) as exc:
            return None, [f"{MANIFEST} okunamadı ({type(exc).__name__})"]
        if not isinstance(manifest, dict) or not isinstance(manifest.get("dosyalar"), list):
            return None, [f"{MANIFEST} beklenen biçimde değil"]
        if int(manifest.get("bicim") or 0) > BICIM:
            sorunlar.append("paket bu betikten daha yeni bir sürümle yapılmış; paketin içindeki tasi.py'yi kullanın")
        liste = {str(d.get("yol")): d for d in manifest["dosyalar"] if isinstance(d, dict)}
        uyeler = {bilgi.filename: bilgi for bilgi in arsiv.infolist() if not bilgi.is_dir()}
        for ad in uyeler:
            if guvensiz_ad(ad):
                sorunlar.append(f"güvensiz dosya adı: {ad}")
            elif ad != MANIFEST and ad not in liste:
                sorunlar.append(f"listede olmayan dosya: {ad}")
        for ad, kayit in liste.items():
            if guvensiz_ad(ad):
                sorunlar.append(f"güvensiz dosya adı: {ad}")
                continue
            if ad not in uyeler:
                sorunlar.append(f"eksik dosya: {ad}")
                continue
            h = hashlib.sha256()
            try:
                with arsiv.open(uyeler[ad]) as f:
                    for parca in iter(lambda: f.read(PARCA), b""):
                        h.update(parca)
            except (OSError, zipfile.BadZipFile, EOFError) as exc:
                sorunlar.append(f"okunamadı: {ad} ({type(exc).__name__})")
                continue
            if h.hexdigest() != kayit.get("sha256"):
                sorunlar.append(f"içeriği değişmiş: {ad}")
        for ad in GEREKLI:
            if ad not in liste:
                sorunlar.append(f"pakette {ad} yok")
    return manifest, sorunlar


def manifest_ozeti(manifest: dict[str, Any], gunluk: Gunluk) -> None:
    dosyalar = manifest.get("dosyalar") or []
    boyut = sum(int(d.get("boyut") or 0) for d in dosyalar if isinstance(d, dict))
    gunluk.yaz(f"   Paket: {len(dosyalar)} dosya, {boyut / 1024 / 1024:.1f} MB; hazırlandığı an: "
               f"{manifest.get('olusturma_yerel', '?')}; eski klasör: {manifest.get('kaynak_klasor', '?')}")
    oturum = manifest.get("telegram_oturumu") or {}
    gunluk.yaz(f"   Telegram oturum dosyası: {'pakette' if oturum.get('var') else 'pakette YOK'}")
    terminaller = manifest.get("mt5_terminalleri") or []
    if terminaller:
        gunluk.yaz("   Gereken MetaTrader 5 terminalleri:")
        for t in terminaller:
            gunluk.yaz(f"   - {t.get('ad')} ({terminal_etiketi(t)}): {t.get('terminal_path') or 'yolu kayıtlı değil'}")


# --------------------------------------------------------------------------- #
# paketle (old server)
# --------------------------------------------------------------------------- #
def _git_surumu(hedef: Path) -> str | None:
    if not (hedef / ".git").exists():
        return None
    kod, cikti = yardimci_calistir(["git", "rev-parse", "HEAD"], hedef, 30)
    return (cikti.strip() or None) if kod == 0 else None


def _kod_parmak_izi(hedef: Path) -> str:
    h = hashlib.sha256()
    for yol in sorted((hedef / PAKET).rglob("*.py")):
        if "__pycache__" in yol.parts:
            continue
        h.update(yol.relative_to(hedef).as_posix().encode() + b"\0" + sha256_dosya(yol).encode() + b"\n")
    return h.hexdigest()


def _bekci_kurulu(g: ModuleType, gorev: Callable[..., Any] | None) -> bool | None:
    if gorev is None:
        return None
    try:
        sonuc = gorev([g._schtasks(), "/Query", "/TN", g.GOREV_ADI], capture_output=True, timeout=60,
                      stdin=subprocess.DEVNULL)
        return getattr(sonuc, "returncode", 1) == 0
    except Exception:          # noqa: BLE001
        return None


def paketle(hedef: Path, secenek: argparse.Namespace, gunluk: Gunluk, surecler: Any, girdi: Callable[[str], str],
            yardimci: Yardimci, gorev: Any, simdi: datetime | None = None) -> int:
    simdi = simdi or datetime.now()
    gunluk.yaz(f"=== TAŞIMA PAKETİ ===  {simdi.strftime('%d.%m.%Y %H:%M')}")
    gunluk.yaz(f"Bot klasörü: {hedef}")
    gunluk.yaz()
    eksik = [ad for ad in ("config.yaml", ".env", f"{PAKET}/__main__.py", "guncelle.py") if not (hedef / ad).is_file()]
    if eksik:
        gunluk.adim(1, "Bot klasörü kontrol edildi", False,
                    f"burası botun klasörü değil ({', '.join(eksik)} yok). Komutu botun klasöründe çalıştırın.")
        return CIKIS_REDDEDILDI
    try:
        bilir = MOVED_FILE_ADI in (hedef / PAKET / "__main__.py").read_text(encoding="utf-8", errors="replace")
    except OSError:
        bilir = False
    if not bilir:
        gunluk.adim(1, "Bot klasörü kontrol edildi", False,
                    "kurulu bot sürümü eski: taşındıktan sonra bu sunucuda kendiliğinden başlamayı engelleyemez. "
                    "Önce güncelleyin (docs/GUNCELLEME.md), sonra bu komutu tekrar çalıştırın. Hiçbir şey yapılmadı.")
        return CIKIS_REDDEDILDI
    try:
        g = guncelle_yukle(hedef)
    except Exception as exc:   # noqa: BLE001
        gunluk.adim(1, "Bot klasörü kontrol edildi", False, f"guncelle.py yüklenemedi ({exc}).")
        return CIKIS_REDDEDILDI
    gunluk.adim(1, "Bot klasörü kontrol edildi")

    # ---- 2. the bot must be stopped (never stopped from here) -------------------------------------------
    veri = g.veri_klasoru(hedef)
    bulgu = g._calisan_bot_bulgusu(hedef, surecler, girdi, gunluk)
    if bulgu is None:
        try:
            kilit = veri / GUNCELLEME_KILIDI
            if kilit.is_file() and time.time() - kilit.stat().st_mtime < KILIT_OMRU:
                bulgu = "şu anda bir güncelleme sürüyor (guncelleme.lock)"
        except OSError:
            pass
    if bulgu is not None:
        gunluk.adim(2, "Bot kapalı mı diye bakıldı", False,
                    f"Paket yapılmadı: {bulgu}.\nÖnce botu durdurun (botun klasöründe): {DURDURMA}\n"
                    '"Bot durduruldu." yazınca bu komutu tekrar çalıştırın. Botu zorla kapatmayın.')
        return CIKIS_REDDEDILDI
    gunluk.adim(2, "Bot kapalı mı diye bakıldı")

    # ---- 3. what the new server needs to know ------------------------------------------------------------
    python = bot_python(hedef)
    try:
        ayar = ayarlari_oku(python, hedef, yardimci)
    except Hata as exc:
        gunluk.adim(3, "Ayarlar okundu", False, f"{exc}. Hiçbir şey yapılmadı.")
        return CIKIS_REDDEDILDI
    veri_yol, veri_goreli = klasor_icinde(hedef, ayar.get("data_dir"), "data")
    oturum_yol, oturum_goreli = klasor_icinde(hedef, ayar.get("session_path"), "data/telegram_user")
    disarida = [ad for ad, goreli in (("data_dir", veri_goreli), ("telegram.session_path", oturum_goreli))
                if goreli is None]
    if disarida:
        gunluk.adim(3, "Ayarlar okundu", False,
                    f"config.yaml'daki {', '.join(disarida)} bot klasörünün dışını gösteriyor; o dosyalar pakete "
                    "girmezdi. Bu durumda taşıma elle yapılmalı; ekran görüntüsünü gönderin.")
        return CIKIS_REDDEDILDI
    pinler = pinleri_oku(veri_yol)
    terminaller = terminal_listesi(ayar, pinler)
    metaapi = [str(b.get("name")) for b in ayar.get("brokers") or []
               if isinstance(b, dict) and b.get("type") == "metaapi" and b.get("enabled") is not False]
    kod, freeze = yardimci([python, "-m", "pip", "freeze", "--disable-pip-version-check"], hedef, 300)
    pip_paketleri = [s.strip() for s in freeze.splitlines() if s.strip() and not s.startswith("#")] if kod == 0 else []
    oturum_dosyasi = Path(str(oturum_yol) + ".session")
    gunluk.adim(3, "Ayarlar okundu")
    gunluk.yaz(f"   MetaTrader 5 terminali gereken hesaplar: {len(terminaller)}"
               + (" (" + ", ".join(f"{t['ad']}: {terminal_etiketi(t)}" for t in terminaller) + ")" if terminaller else ""))
    if not oturum_dosyasi.is_file():
        gunluk.yaz("   Not: Telegram oturum dosyası bulunamadı (oturum .env'de yazılı olabilir); paket yine yapılıyor.")

    # ---- 4. the zip ----------------------------------------------------------------------------------------
    ad = f"{ZIP_ON_EKI}{simdi.strftime('%Y%m%d-%H%M')}.zip"
    zip_yolu = hedef / ad
    sayac = 1
    while zip_yolu.exists():
        sayac += 1
        zip_yolu = hedef / f"{ZIP_ON_EKI}{simdi.strftime('%Y%m%d-%H%M')}-{sayac}.zip"
    gecici = zip_yolu.with_name(zip_yolu.name + ".part")
    haric = {gunluk.yol} if gunluk.yol else set()
    dosyalar, eski_gunluk = paket_dosyalari(hedef, veri_goreli, secenek.yedekli, haric)
    paketlenen = {arc for _yol, arc in dosyalar}
    veri_on = "" if veri_goreli in (None, ".") else veri_goreli.rstrip("/") + "/"
    oturum_arc = (oturum_goreli or "") + ".session"
    eksik_veri = [ad for ad, gerekli in ((f"{veri_on}copytrader.db", True),
                                         (oturum_arc, oturum_dosyasi.is_file())) if gerekli and ad not in paketlenen]
    if eksik_veri:
        gunluk.adim(4, "Paket yazıldı", False,
                    f"pakete girmeyen gerekli dosya: {', '.join(eksik_veri)} (veritabanı ya da Telegram oturumu). "
                    "Veri klasörü başka bir yere bağlantı olabilir ya da okunamıyor. Hiçbir şey yapılmadı; ekran "
                    "görüntüsünü gönderin.")
        return CIKIS_SORUN
    tasi_kaydi: str | None = None
    kayitlar: list[dict[str, Any]] = []
    try:
        with zipfile.ZipFile(gecici, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as arsiv:
            for yol, arc in dosyalar:
                bilgi = zipfile.ZipInfo.from_file(yol, arc, strict_timestamps=False)
                bilgi.compress_type = zipfile.ZIP_DEFLATED
                h = hashlib.sha256()
                boyut = 0
                with open(yol, "rb") as kaynak, arsiv.open(bilgi, "w", force_zip64=True) as hedef_dosya:
                    for parca in iter(lambda: kaynak.read(PARCA), b""):
                        h.update(parca)
                        boyut += len(parca)
                        hedef_dosya.write(parca)
                kayitlar.append({"yol": arc, "boyut": boyut, "sha256": h.hexdigest()})
                if arc == "tasi.py":
                    tasi_kaydi = h.hexdigest()
            manifest = {
                "bicim": BICIM,
                "paket_kimligi": uuid.uuid4().hex,
                "olusturma_yerel": simdi.strftime("%d.%m.%Y %H:%M"),
                "olusturma_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "kaynak_klasor": str(hedef),
                "isletim_sistemi": platform.platform(),
                "tasi_python": sys.version.split()[0],
                "bot_python": ayar.get("python"),
                "surum": {"git": _git_surumu(hedef), "kod_parmak_izi": _kod_parmak_izi(hedef),
                          "son_guncelleme_gunlugu": max((p.name for p in hedef.glob("guncelleme_*.log")), default=None)},
                "tasi_py_sha256": tasi_kaydi,
                "data_dir": veri_goreli,
                "telegram_oturumu": {"yol": oturum_arc, "var": oturum_arc in paketlenen},
                "mt5_terminalleri": terminaller,
                "metaapi_hesaplari": metaapi,
                "pip_paketleri": pip_paketleri,
                "bekci": {"gorev_adi": g.GOREV_ADI, "aralik_dakika": 5, "kullanici": "SYSTEM",
                          "bekci_py": (hedef / g.BEKCI_DOSYASI).is_file(), "bu_sunucuda_kurulu": _bekci_kurulu(g, gorev)},
                "haric": {"yedek": not secenek.yedekli, "eski_gunluk_sayisi": eski_gunluk,
                          "klasorler": list(HARIC_UST) + ([] if secenek.yedekli else [YEDEK]),
                          "veri": list(TASINMAYAN_VERI)},
                "dosyalar": kayitlar,
            }
            arsiv.writestr(MANIFEST, json.dumps(manifest, ensure_ascii=False, indent=1))
    except (OSError, ValueError, zipfile.LargeZipFile) as exc:
        try:
            gecici.unlink()
        except OSError:
            pass
        gunluk.adim(4, "Paket yazıldı", False, f"paket yazılamadı ({exc}). Diskte yer olduğuna bakıp tekrar deneyin.")
        return CIKIS_SORUN
    gunluk.adim(4, f"Paket yazıldı ({len(kayitlar)} dosya; {eski_gunluk} eski kayıt dosyası alınmadı"
                   + ("" if secenek.yedekli else "; yedek klasörü alınmadı") + ")")

    # ---- 5. read it back ---------------------------------------------------------------------------------
    _manifest, sorunlar = paketi_dogrula(gecici)
    if sorunlar:
        gunluk.adim(5, "Paket doğrulandı", False, "; ".join(sorunlar[:5]) + ". Paket silindi; tekrar deneyin.")
        try:
            gecici.unlink()
        except OSError:
            pass
        return CIKIS_SORUN
    try:
        os.replace(gecici, zip_yolu)
    except OSError as exc:
        gunluk.adim(5, "Paket doğrulandı", False, f"dosyanın adı verilemedi ({exc}).")
        return CIKIS_SORUN
    gunluk.adim(5, "Paket doğrulandı (her dosya yeniden okunup karşılaştırıldı)")
    try:
        veri_yol.mkdir(parents=True, exist_ok=True)
        (veri_yol / TASINDI).write_text(
            f"Bu bot {simdi.strftime('%d.%m.%Y %H:%M')} tarihinde başka bir sunucuya taşındı (paket: {zip_yolu.name}).\n"
            "Bu dosya durdukça bot bu sunucuda başlatılmaz: iki sunucuda birden çalışırsa her işlem iki kez açılır.\n"
            "Bu sunucuyu gerçekten yeniden kullanmak istiyorsanız (ve yeni sunucudaki bot kapalıysa) bu dosyayı "
            "silin.\n", encoding="utf-8")
        gunluk.yaz(f"   Bu sunucuda bot artık başlatılmaz ({Path(veri_goreli or 'data') / TASINDI} dosyası yazıldı).")
    except OSError as exc:
        gunluk.yaz(f"   UYARI: {TASINDI} yazılamadı ({exc}); bu sunucuda botu kendiniz başlatmayın.")
    gunluk.yaz()
    gunluk.yaz(f"Paket hazır: {zip_yolu}")
    gunluk.yaz(f"Boyutu: {zip_yolu.stat().st_size / 1024 / 1024:.1f} MB")
    gunluk.yaz()
    gunluk.yaz(f"!!! DİKKAT: {GIZLI_UYARI}")
    gunluk.yaz()
    gunluk.yaz("Yeni sunucuya iki dosyayı kopyalayın: bu paket ve aynı klasördeki tasi.py.")
    gunluk.yaz("Bu sunucuda botu bir daha BAŞLATMAYIN: iki bot aynı hesaplarda her işlemi iki kez açar.")
    return CIKIS_TAMAM


# --------------------------------------------------------------------------- #
# kur (new server)
# --------------------------------------------------------------------------- #
def program_files_kokleri(ortam: Any = None) -> list[Path]:
    """The Program Files folders (as copytrader.migrate.program_files_roots)."""
    ortam = os.environ if ortam is None else ortam
    bulunan = [str(ortam[n]) for n in ("ProgramW6432", "ProgramFiles", "ProgramFiles(x86)") if ortam.get(n)]
    if os.name == "nt":
        bulunan += [r"C:\Program Files", r"C:\Program Files (x86)"]
    sonuc: list[Path] = []
    for ad in bulunan:
        if all(os.path.normcase(os.path.normpath(str(y))) != os.path.normcase(os.path.normpath(ad)) for y in sonuc):
            sonuc.append(Path(ad))
    return sonuc


def terminal_anahtari(yol: str | None) -> str:
    """The folder of a terminal64.exe, case-folded (copytrader.config.terminal_key)."""
    metin = str(yol or "").strip().strip('"').replace("/", "\\").rstrip("\\")
    if metin.lower().endswith(".exe"):
        metin = metin.rsplit("\\", 1)[0] if "\\" in metin else ""
    return metin.rstrip("\\").casefold()


def _klasor_adi(yol: str) -> str:
    saf = PureWindowsPath(yol) if ("\\" in yol or ":" in yol[:3]) else PurePosixPath(yol)
    return saf.parent.name if saf.name.casefold() == TERMINAL_EXE else saf.name


def terminal_ara(t: dict[str, Any], kokler: list[Path]) -> list[Path]:
    """terminal64.exe files in Program Files that may be account ``t``'s terminal at another path: a folder with
    the old folder's name, or one whose name holds its company / server / account name (directly in it or one
    folder below, as copytrader.migrate.find_tickmill_terminals)."""
    kelimeler: list[str] = []
    for aday, en_az in ((t.get("broker_company"), 3), (t.get("server"), 5), (t.get("terminal_sirketi"), 5),
                        (t.get("ad"), 5)):
        if isinstance(aday, str):
            kelime = aday.split("-")[0].strip().casefold()
            if len(kelime) >= en_az and "metaquotes" not in kelime and kelime not in kelimeler:
                kelimeler.append(kelime)
    eski_ad = _klasor_adi(t["terminal_path"]).casefold() if t.get("terminal_path") else ""
    sonuc: list[Path] = []
    gorulen: set[str] = set()

    def ekle(exe: Path) -> None:
        try:
            var = exe.is_file()
        except OSError:
            var = False
        anahtar = os.path.normcase(str(exe))
        if var and anahtar not in gorulen:
            gorulen.add(anahtar)
            sonuc.append(exe)

    for kok in kokler:
        try:
            altlar = sorted(kok.iterdir())
        except OSError:
            continue
        for alt in altlar:
            try:
                if not alt.is_dir():
                    continue
                ad = alt.name.casefold()
                if (eski_ad and ad == eski_ad) or any(k in ad for k in kelimeler):
                    ekle(alt / TERMINAL_EXE)
                    for ic in sorted(alt.iterdir()):
                        if ic.is_dir():
                            ekle(ic / TERMINAL_EXE)
            except OSError:
                continue
    return sonuc


def pin_guncelle(veri: Path, pin_dosyasi: str, yeni: Path, zaman: str) -> Path:
    """Point an account's terminal pin at the terminal's new folder (folder and path only; company, server and
    account stay, so the bot's checks stay as they are). A backup is written first. Returns the backup."""
    yol = veri / pin_dosyasi
    ham = yol.read_bytes()
    kayit = json.loads(ham.decode("utf-8"))
    yedek = yol.with_name(f"{yol.name}.yedek-{zaman}")
    yedek.write_bytes(ham)
    kayit["folder"] = str(yeni.parent)
    kayit["path"] = str(yeni)
    gecici = yol.with_name(yol.name + ".tmp")
    gecici.write_text(json.dumps(kayit, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(gecici, yol)
    return yedek


def _sor(girdi: Callable[[str], str], soru: str) -> str:
    try:
        return girdi(soru)
    except EOFError:
        return ""


def _bos_mu(klasor: Path) -> bool:
    try:
        return not any(klasor.iterdir())
    except OSError:
        return False


def kurulum_izi(hedef: Path) -> str | None:
    try:
        return str(json.loads((hedef / KURULUM_IZI).read_text(encoding="utf-8")).get("paket_kimligi") or "") or None
    except (OSError, ValueError, AttributeError):
        return None


def varsayilan_hedef(manifest: dict[str, Any]) -> Path:
    eski = str(manifest.get("kaynak_klasor") or "")
    if eski:
        yol = Path(eski)
        if yol.is_absolute() and yol.parent.is_dir():
            if not yol.exists() or _bos_mu(yol) or kurulum_izi(yol) == manifest.get("paket_kimligi"):
                return yol
    return Path(r"C:\copytrader") if os.name == "nt" else Path.home() / "copytrader"


def disari_ac(zip_yolu: Path, manifest: dict[str, Any], hedef: Path, iz: str = "") -> None:
    """Extract every listed file into a fresh folder beside ``hedef``, check each sha256, write ``iz`` (the
    install mark, KURULUM_IZI) into it, then give the folder its name: a folder with the name is always complete
    and marked, so a later run goes on from it. Raises Hata (the half-extracted folder is removed)."""
    gecici = hedef.with_name(hedef.name + ".tasima-gecici")
    if gecici.exists():
        shutil.rmtree(gecici)
    gecici.mkdir(parents=True)
    try:
        with zipfile.ZipFile(zip_yolu) as arsiv:
            for kayit in manifest["dosyalar"]:
                ad = str(kayit["yol"])
                if guvensiz_ad(ad):
                    raise Hata(f"güvensiz dosya adı: {ad}")
                bilgi = arsiv.getinfo(ad)
                cikti = gecici.joinpath(*ad.split("/"))
                cikti.parent.mkdir(parents=True, exist_ok=True)
                h = hashlib.sha256()
                with arsiv.open(bilgi) as kaynak, open(cikti, "wb") as yaz:
                    for parca in iter(lambda: kaynak.read(PARCA), b""):
                        h.update(parca)
                        yaz.write(parca)
                if h.hexdigest() != kayit.get("sha256"):
                    raise Hata(f"açılan dosya pakettekinden farklı: {ad}")
                try:
                    zaman = time.mktime(bilgi.date_time + (0, 0, -1))
                    os.utime(cikti, (zaman, zaman))
                except (OverflowError, ValueError, OSError):
                    pass
        if iz:
            (gecici / KURULUM_IZI).write_text(iz, encoding="utf-8")
        if hedef.exists():
            hedef.rmdir()                     # only an empty folder gets here
        _yeniden_adlandir(gecici, hedef)
    except BaseException:
        shutil.rmtree(gecici, ignore_errors=True)
        raise


def yolda_olani_kaldir(hedef: Path, zaman: str) -> Path:
    """``--uzerine-yaz``: rename the folder that is in the way (never delete it)."""
    yeni = hedef.with_name(f"{hedef.name}.eski-{zaman}")
    sayac = 1
    while yeni.exists():
        sayac += 1
        yeni = hedef.with_name(f"{hedef.name}.eski-{zaman}-{sayac}")
    _yeniden_adlandir(hedef, yeni)
    return yeni


def _yeniden_adlandir(eski: Path, yeni: Path, kez: int = 6, bekle: float = 0.5) -> None:
    """os.rename with a few retries (Windows: a virus scanner can hold a file for a moment; as guncelle.py)."""
    for deneme in range(kez):
        try:
            os.rename(eski, yeni)
            return
        except OSError:
            if deneme == kez - 1:
                raise
            time.sleep(bekle)


def _ps_tirnak(metin: str) -> str:
    return "'" + metin.replace("'", "''") + "'"


def ayri_pencerede_baslat(hedef: Path, python: str) -> None:
    """Start the bot in a window of its own (docs/SUNUCU_CLAUDE.md step 4): a PowerShell window that stays open
    after the bot ends, so its last lines can be read. Returns at once."""
    if os.name == "nt":                       # pragma: no cover - Windows only
        kok = os.environ.get("SystemRoot") or r"C:\Windows"
        ps = os.path.join(kok, "System32", "WindowsPowerShell", "v1.0", "powershell.exe")
        komut = f"& {_ps_tirnak(python)} -m {PAKET} run"
        subprocess.Popen([ps if os.path.isfile(ps) else "powershell", "-NoExit", "-NoProfile", "-Command", komut],
                         cwd=str(hedef), creationflags=getattr(subprocess, "CREATE_NEW_CONSOLE", 0x10))
        return
    subprocess.Popen([python, "-m", PAKET, "run"], cwd=str(hedef), stdin=subprocess.DEVNULL,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)


def _paket_surumu(pip_paketleri: list[str], ad: str) -> str | None:
    for satir in pip_paketleri:
        m = re.match(r"\s*([A-Za-z0-9_.-]+)\s*==\s*([^\s;]+)", satir)
        if m and m.group(1).replace("_", "-").casefold() == ad.replace("_", "-").casefold():
            return m.group(2)
    return None


_NUMARALAR = (
    (re.compile(r"(?i)\b(login\s*[=:]\s*)\d+"), r"\1***"),                       # login=12345678
    (re.compile(r"(farklı bir hesaba bağlı \()[^,)]+(, beklenen )[^)]+"), r"\1***\2***"),
    (re.compile(r"(Sahip kullanıcı ID:\s*)\S+"), r"\1***"),                          # the owner's Telegram id
    (re.compile(r"(?<![\d.,])\d{6,}(?![\d.,]*\d)"), "***"),                          # any other long number
)


def numaralari_gizle(metin: str) -> str:
    """A line of check / selftest output without account numbers and the owner's Telegram id (balances stay)."""
    for kalip, yerine in _NUMARALAR:
        metin = kalip.sub(yerine, metin)
    return metin


def bagli_olmayan_istege_bagli(satirlar: list[str]) -> list[str]:
    """Optional accounts ``copytrader check`` could not connect (it still exits 0 for them)."""
    adlar: list[str] = []
    for satir in satirlar:
        metin = satir.strip()
        m = re.match(r"\u26a0\ufe0f?\s+([^:\s]+): ", metin)
        if m and "isteğe bağlı hesap" in metin and m.group(1) not in adlar:
            adlar.append(m.group(1))
        m = re.search(r"Sorun yok \(isteğe bağlı (.+) bağlı değil\)", metin)
        if m:
            for ad in m.group(1).split(","):
                if ad.strip() and ad.strip() not in adlar:
                    adlar.append(ad.strip())
    return adlar


def kur(zip_yolu: Path, secenek: argparse.Namespace, gunluk: Gunluk, *, calistir: Calistirici | None,
        surecler: Any, girdi: Callable[[str], str], yardimci: Yardimci, gorev: Any, ek_paket: Any,
        baslat: Callable[[Path, str], None], python_bul: Callable[[Path], str] | None,
        baslama_bekleme: float) -> int:
    zaman = datetime.now().strftime("%Y%m%d-%H%M%S")
    gunluk.yaz(f"=== YENİ SUNUCUYA KURULUM ===  {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    gunluk.yaz(f"Paket: {zip_yolu}")
    gunluk.yaz()

    # ---- 1. the package -----------------------------------------------------------------------------------
    if sys.version_info < (3, 11):
        gunluk.adim(1, "Paket kontrol edildi", False,
                    f"bu betik Python 3.11 veya yenisini ister (burada {sys.version.split()[0]}). Python 3.12 kurun.")
        return CIKIS_REDDEDILDI
    gunluk.yaz("   Paket kontrol ediliyor (büyükse birkaç dakika sürebilir)...")
    manifest, sorunlar = paketi_dogrula(zip_yolu)
    if manifest is None or sorunlar:
        gunluk.adim(1, "Paket kontrol edildi", False,
                    "; ".join(sorunlar[:5]) + ".\nPaketi eski sunucudan yeniden kopyalayın. Hiçbir şey yapılmadı.")
        return CIKIS_REDDEDILDI
    gunluk.adim(1, "Paket kontrol edildi (her dosya listeyle karşılaştırıldı)")
    manifest_ozeti(manifest, gunluk)
    try:
        bu = sha256_dosya(Path(__file__))
        if manifest.get("tasi_py_sha256") and bu != manifest["tasi_py_sha256"]:
            gunluk.yaz("   Not: çalıştırdığınız tasi.py paketin içindekinden farklı; paketle birlikte kopyalanan "
                       "tasi.py'yi kullanmanız önerilir.")
    except OSError:
        pass

    # ---- 2. the bot folder ---------------------------------------------------------------------------------
    kimlik = str(manifest.get("paket_kimligi") or "")
    if secenek.hedef:
        hedef = Path(os.path.abspath(Path(secenek.hedef).expanduser()))
    else:
        oneri = varsayilan_hedef(manifest)
        if secenek.evet:
            yanit = ""
        else:
            yanit = _sor(girdi, f"   Bot şu klasöre kurulacak: {oneri}\n   Uygunsa Enter'a basın; başka bir klasör "
                                "için tam yolunu yazıp Enter'a basın: ").strip().strip('"')
        hedef = Path(os.path.abspath(Path(yanit).expanduser())) if yanit else oneri
    devam = hedef.is_dir() and kurulum_izi(hedef) == kimlik
    if hedef.exists() and not devam and not (hedef.is_dir() and _bos_mu(hedef)):
        if not secenek.uzerine_yaz:
            ne = ("içinde başka bir bot kurulu (data klasörü var)" if (hedef / "data").exists()
                  else "klasör boş değil")
            gunluk.adim(2, f"Bot klasörü seçildi ({hedef})", False,
                        f"{ne}. Başka bir klasör seçin (--hedef \"C:\\...\") ya da bu klasörü kenara almak için "
                        "komutun sonuna --uzerine-yaz ekleyin (klasör silinmez, adı değiştirilir). Hiçbir şey yapılmadı.")
            return CIKIS_REDDEDILDI
        if (hedef / "data" / "bot.lock").exists():
            calisiyor = None
            try:
                calisiyor = guncelle_yukle(hedef).kilit_sahibi(hedef / "data")
            except Exception:      # noqa: BLE001 - cannot be told: refuse
                calisiyor = {"pid": "?"}
            if calisiyor is not None:
                gunluk.adim(2, f"Bot klasörü seçildi ({hedef})", False,
                            "bu klasörde bir bot çalışıyor; önce onu durdurun. Hiçbir şey yapılmadı.")
                return CIKIS_REDDEDILDI
        try:
            kenar = yolda_olani_kaldir(hedef, zaman)
        except OSError as exc:
            gunluk.adim(2, f"Bot klasörü seçildi ({hedef})", False,
                        f"klasör kenara alınamadı ({exc}); içindeki bir dosya açık olabilir. Hiçbir şey yapılmadı.")
            return CIKIS_REDDEDILDI
        gunluk.yaz(f"   Yoldaki klasörün adı değiştirildi: {kenar}")
    if not hedef.parent.is_dir():
        try:
            hedef.parent.mkdir(parents=True)
        except OSError as exc:
            gunluk.adim(2, f"Bot klasörü seçildi ({hedef})", False, f"üst klasör oluşturulamadı ({exc}).")
            return CIKIS_REDDEDILDI
    gunluk.adim(2, f"Bot klasörü seçildi ({hedef})")

    # ---- 3. extract --------------------------------------------------------------------------------------
    if devam:
        gunluk.adim(3, "Dosyalar bu klasöre daha önce açılmış; yeniden açılmadı (veriniz korunur)")
    else:
        gunluk.yaz("   Dosyalar açılıyor ve tek tek kontrol ediliyor...")
        try:
            disari_ac(zip_yolu, manifest, hedef, json.dumps({
                "paket_kimligi": kimlik, "paket": zip_yolu.name,
                "acildi": datetime.now(timezone.utc).isoformat(timespec="seconds")}, indent=1))
        except (Hata, OSError, KeyError, zipfile.BadZipFile) as exc:
            gunluk.adim(3, "Dosyalar açıldı", False, f"{exc}. Yarım açılan dosyalar silindi, bot klasörü "
                                                       "oluşturulmadı; paketi yeniden kopyalayıp tekrar deneyin.")
            return CIKIS_SORUN
        gunluk.adim(3, f"Dosyalar açıldı ve kontrol edildi ({len(manifest['dosyalar'])} dosya)")
    gunluk.ac(hedef / f"tasima_{zaman}.log")
    try:
        g = guncelle_yukle(hedef)
    except Exception as exc:   # noqa: BLE001
        gunluk.adim(4, "Python ortamı hazırlandı", False, f"guncelle.py yüklenemedi ({exc}).")
        return CIKIS_SORUN
    calistir = calistir or g.komut_calistir
    surecler = surecler or g.surec_listesi
    if ek_paket is OTOMATIK:
        ek_paket = g.ek_paketler if calistir is g.komut_calistir else None
    veri = g.veri_klasoru(hedef)

    # the bot must not be running (also: no other copy of it on this computer)
    bulgu = g._calisan_bot_bulgusu(hedef, surecler, girdi, gunluk)
    if bulgu is not None:
        gunluk.adim(4, "Bot kapalı mı diye bakıldı", False,
                    f"{bulgu}. Kurulum durduruldu: bot zaten çalışıyorsa ikinci bir bot başlatılmamalı. "
                    f"Durdurmak için bot klasöründe: {DURDURMA}")
        return CIKIS_REDDEDILDI

    # ---- 4. venv -------------------------------------------------------------------------------------------
    python = python_bul(hedef) if python_bul else str(venv_python(hedef))

    def ekrana(metin: str) -> None:
        gunluk.yaz(f"   | {metin}")

    def komut(*parcalar: str, sure: float = KONTROL_SURESI, ekranda: bool = True) -> tuple[int, list[str]]:
        satirlar: list[str] = []

        def satir(metin: str) -> None:
            metin = numaralari_gizle(metin)
            satirlar.append(metin)
            (gunluk.yaz if ekranda else gunluk.dosyaya)(f"   | {metin}")

        return calistir([python, *parcalar], hedef, sure, satir), satirlar

    def son_satirlar(satirlar: list[str], kac: int) -> None:
        for metin in [m for m in satirlar if m.strip()][-kac:]:
            gunluk.yaz(f"   | {metin}")

    if python_bul is None and Path(python).is_file() and calistir([python, "-c", "import sys"], hedef, 120,
                                                                   gunluk.dosyaya) == 0:
        gunluk.adim(4, "Python ortamı (venv) zaten var")
    else:
        satirlar: list[str] = []
        kod = calistir([sys.executable, "-m", "venv", "--clear", str(hedef / "venv")], hedef, VENV_SURESI,
                       satirlar.append)
        if kod != 0:
            son_satirlar(satirlar, 10)
            gunluk.adim(4, "Python ortamı (venv) oluşturuldu", False,
                        f"venv oluşturulamadı (çıkış kodu {kod}). Python 3.12'yi python.org'dan yeniden kurun, "
                        "sonra aynı komutu tekrar çalıştırın.")
            return CIKIS_SORUN
        gunluk.adim(4, f"Python ortamı (venv) oluşturuldu (Python {sys.version.split()[0]})")

    # ---- 5. packages --------------------------------------------------------------------------------------
    gunluk.yaz("   Gerekli paketler kuruluyor (birkaç dakika sürebilir)...")
    kod, satirlar = komut("-m", "pip", "install", "--disable-pip-version-check", "-r", "requirements.txt",
                          sure=PIP_SURESI, ekranda=False)
    if kod != 0:
        son_satirlar(satirlar, 15)
        gunluk.adim(5, "Gerekli paketler kuruldu", False,
                    f"pip çıkış kodu {kod}. İnternet bağlantısına bakın, sonra aynı komutu tekrar çalıştırın.")
        return CIKIS_SORUN
    try:
        ayar = ayarlari_oku(python, hedef, yardimci)
    except Hata as exc:
        gunluk.adim(5, "Gerekli paketler kuruldu", False, f"{exc}.")
        return CIKIS_SORUN
    turler = {str(b.get("type")) for b in ayar.get("brokers") or []
              if isinstance(b, dict) and b.get("enabled") is not False}
    eski_paketler = [str(s) for s in manifest.get("pip_paketleri") or []]
    gerekli_import = ["copytrader.__main__", "copytrader.app"]
    for paket, modul, gerekli in ((MT5_PAKETI, "MetaTrader5", bool(turler & set(MT_TURLERI))),
                                  (METAAPI_PAKETI, "metaapi_cloud_sdk", "metaapi" in turler)):
        surum = _paket_surumu(eski_paketler, paket)
        if not gerekli and surum is None:
            continue
        adaylar = ([f"{paket}=={surum}"] if surum else []) + [paket]
        kuruldu = False
        for aday in adaylar:
            kod, satirlar = komut("-m", "pip", "install", "--disable-pip-version-check", aday,
                                  sure=PIP_SURESI, ekranda=False)
            if kod == 0:
                kuruldu = True
                break
        if kuruldu:
            gerekli_import.append(modul)
        elif gerekli:
            son_satirlar(satirlar, 10)
            gunluk.adim(5, "Gerekli paketler kuruldu", False,
                        f"{paket} kurulamadı (hesaplarınız için gerekli). İnternet bağlantısına bakın, sonra aynı "
                        "komutu tekrar çalıştırın.")
            return CIKIS_SORUN
        else:
            gunluk.yaz(f"   Uyarı: eski sunucuda kurulu olan {paket} kurulamadı (etkin bir hesap kullanmıyor).")
    gunluk.adim(5, "Gerekli paketler kuruldu")
    if ek_paket is not None:
        try:
            ek_paket(komut, gunluk)
        except Exception as exc:  # noqa: BLE001
            gunluk.yaz(f"   UYARI: isteğe bağlı paket adımı başarısız ({exc}); kurulum devam ediyor.")

    # ---- 6. imports ---------------------------------------------------------------------------------------
    kod, satirlar = komut("-c", "import " + ", ".join(gerekli_import), sure=300, ekranda=False)
    if kod != 0:
        son_satirlar(satirlar, 5)
        gunluk.adim(6, "Paketler denendi", False,
                    "bot programı yüklenemedi. Ekran görüntüsünü gönderin.")
        return CIKIS_SORUN
    gunluk.adim(6, "Paketler denendi (bot programı yükleniyor)")

    # ---- 7. MetaTrader 5 terminals --------------------------------------------------------------------------
    pinler = pinleri_oku(veri)
    terminaller = terminal_listesi(ayar, pinler)
    kokler = program_files_kokleri()
    eksikler: list[str] = []
    notlar: list[str] = []
    kullanilan = {terminal_anahtari(t["terminal_path"]) for t in terminaller
                  if t.get("terminal_path") and not yer_tutucu(t["terminal_path"]) and Path(t["terminal_path"]).is_file()}
    for t in terminaller:
        ad, etiket, yol = t["ad"], terminal_etiketi(t), t.get("terminal_path")
        if yol and yer_tutucu(yol):
            notlar.append(f"{ad} ({etiket}): terminal yolu .env dosyasında yazılı; bu adımda denetlenemedi "
                          "(8. adım bağlantıyı dener)")
            continue
        if not yol:
            notlar.append(f"{ad} ({etiket}): terminal yolu kayıtlı değil; bot bilgisayardaki MetaTrader 5'e "
                          "bağlanır (8. adım bağlantıyı dener)")
            continue
        if Path(yol).is_file():
            gunluk.yaz(f"   {ad} ({etiket}): terminal yerinde ({yol})")
            continue
        adaylar = [a for a in terminal_ara(t, kokler) if terminal_anahtari(str(a)) not in kullanilan]
        if len(adaylar) == 1:
            yeni = adaylar[0]
            soru = (f"   {ad} ({etiket}): terminal eski yerinde yok ({yol}), şurada bulundu: {yeni}\n"
                    f"   {'config.yaml' if t['kaynak'] == 'config' else 'Terminal kaydı (' + str(t['pin_dosyasi']) + ')'}"
                    f" bu yola çevrilsin mi? Çevrilsin diye {ONAY} yazıp Enter'a basın: ")
            if secenek.evet or _sor(girdi, soru).strip() == ONAY:
                try:
                    if t["kaynak"] == "config":
                        kod, cikti = yardimci([python, "-c", DUZENLE_BETIGI, "config.yaml", ad, "terminal_path",
                                               str(yeni)], hedef, YARDIMCI_SURESI)
                        yedek = isaretli_satir(cikti, "TASI_YEDEK")
                        if kod != 0 or not yedek:
                            raise Hata(isaretli_satir(cikti, "TASI_HATA") or f"çıkış kodu {kod}")
                    else:
                        yedek = pin_guncelle(veri, str(t["pin_dosyasi"]), yeni, zaman).name
                except (Hata, OSError, ValueError) as exc:
                    eksikler.append(f"{ad} ({etiket}): terminal {yeni} yolunda bulundu ama ayar değiştirilemedi "
                                    f"({exc}). config.yaml'da {ad} hesabının terminal_path satırını bu yolla "
                                    "elle değiştirin.")
                    continue
                kullanilan.add(terminal_anahtari(str(yeni)))
                gunluk.yaz(f"   {ad} ({etiket}): terminal yolu {yeni} olarak değiştirildi (eski hali: {yedek})")
                continue
            eksikler.append(f"{ad} ({etiket}): terminal {yeni} yolunda; ayar değiştirilmedi (onay verilmedi).")
            continue
        if len(adaylar) > 1:
            eksikler.append(f"{ad} ({etiket}): terminal eski yerinde yok ({yol}); birden fazla aday var ("
                            + ", ".join(str(a) for a in adaylar) + f"). config.yaml'da {ad} hesabının terminal_path "
                            "satırına doğru olanı yazın.")
            continue
        eksikler.append(f"{ad} ({etiket}): MetaTrader 5 terminali bulunamadı ({yol}). {etiket} MT5 terminalini "
                        "kurun, açın ve hesabınızla giriş yapın.")
    for metin in notlar:
        gunluk.yaz(f"   Not: {metin}")
    if eksikler:
        gunluk.adim(7, "MetaTrader 5 terminalleri", False, "\n".join(eksikler) + "\n"
                    "Bunları yaptıktan sonra aynı komutu tekrar çalıştırın. Bot başlatılmadı.")
        return CIKIS_SORUN
    gunluk.adim(7, f"MetaTrader 5 terminalleri ({len(terminaller)} hesap)")

    # ---- 8./9. check and selftest (the bot is stopped: it has not run on this server yet) ---------------------
    g.guncelleme_kilidi_yaz(veri)               # the watchman (if any) stays silent meanwhile
    try:
        if secenek.kontrol_yok:
            gunluk.adim(8, "Ayar ve bağlantı kontrolü atlandı (--kontrol-yok)")
            gunluk.adim(9, "Sağlık kontrolü atlandı (--kontrol-yok)")
        else:
            gunluk.yaz("   Ayarlar ve hesap bağlantıları kontrol ediliyor (1-3 dakika sürebilir)...")
            kod, satirlar = komut("-m", PAKET, "check")
            if kod != 0:
                gunluk.adim(8, "Ayarlar ve hesap bağlantıları kontrol edildi", False,
                            "Yukarıdaki ❌ satırlarındaki sorunu giderin (MetaTrader terminalleri açık ve hesaba giriş "
                            "yapılmış olmalı), sonra aynı komutu tekrar çalıştırın. Bot başlatılmadı.")
                g.guncelleme_kilidi_kaldir(veri)
                return CIKIS_SORUN
            bagli_degil = bagli_olmayan_istege_bagli(satirlar)
            if bagli_degil:
                gunluk.adim(8, "Ayarlar ve hesap bağlantıları kontrol edildi", False,
                            f"Bağlanamayan isteğe bağlı hesap: {', '.join(bagli_degil)}. O hesabın MetaTrader "
                            "terminalini açıp hesaba giriş yapın (Algo Trading yeşil olsun); bot bu hesap olmadan "
                            "başlatılıyor ve bağlanmayı kendisi yeniden dener.")
            else:
                gunluk.adim(8, "Ayarlar ve hesap bağlantıları kontrol edildi")
            gunluk.yaz("   Sağlık kontrolü yapılıyor: hesaplar, semboller, fiyat ve miktar (1-3 dakika sürebilir)...")
            kod, satirlar = komut("-m", PAKET, "selftest")
            if kod == 0:
                gunluk.adim(9, "Sağlık kontrolü yapıldı, sorun yok")
            else:
                gunluk.adim(9, "Sağlık kontrolü", False,
                            "Sorunlar yukarıda SONUÇ bölümünde yazıyor; bot yine başlatılıyor. Ekran görüntüsünü "
                            "gönderin.")

        # ---- 10. the watchman --------------------------------------------------------------------------------
        if gorev is None:
            gunluk.yaz("   Bekçi yalnızca Windows'ta kurulur.")
        elif secenek.bekci_yok:
            gunluk.adim(10, "Bekçi kurulmadı (--bekci-yok)")
        else:
            tamam = g.bekci_adimi(hedef, python, gorev, False, gunluk)
            gunluk.adim(10, "Bekçi kuruldu" if tamam else "Bekçi", tamam,
                        "" if tamam else "İsteğe bağlı: PowerShell'i \"Yönetici olarak çalıştır\" ile açıp bot "
                        r"klasöründe .\venv\Scripts\python.exe guncelle.py --bekci-kur çalıştırın.")

        # ---- 11. start ---------------------------------------------------------------------------------------
        if secenek.baslatma:
            g.guncelleme_kilidi_kaldir(veri)
            gunluk.adim(11, "Bot başlatılmadı (--baslatma)")
            gunluk.yaz(f"   Başlatmak için bot klasöründe: {python} -m {PAKET} run")
            return CIKIS_UYARILI if any(not tamam for _m, tamam, _n in gunluk.ozet) else CIKIS_TAMAM
        bulgu, _ = g.bot_calisiyor_mu(hedef, surecler)
        if bulgu is not None:
            g.guncelleme_kilidi_kaldir(veri)
            gunluk.adim(11, "Bot başlatılmadı", False, f"{bulgu}; ikinci bir bot başlatılmadı.")
            return CIKIS_REDDEDILDI
        g.guncelleme_kilidi_kaldir(veri, bot_basliyor=True)
        try:
            baslat(hedef, python)
        except OSError as exc:
            gunluk.adim(11, "Bot başlatıldı", False, f"başlatılamadı ({exc}). Bot klasöründe elle başlatın: "
                                                     f"{python} -m {PAKET} run")
            return CIKIS_SORUN
        gunluk.yaz("   Bot ayrı bir pencerede başlatıldı; çalıştığı doğrulanıyor...")
        son = time.monotonic() + baslama_bekleme
        sahip = None
        while True:
            sahip = g.kilit_sahibi(veri)
            if sahip is not None or time.monotonic() >= son:
                break
            time.sleep(1.0)
        if sahip is None:
            gunluk.adim(11, "Bot başlatıldı", False,
                        "bot çalışıyor görünmüyor. Açılan pencerede yazanların ekran görüntüsünü gönderin.")
            return CIKIS_SORUN
        gunluk.adim(11, f"Bot başlatıldı ve çalışıyor (PID {sahip.get('pid')})")
        gunluk.yaz("   Açılan bot penceresini KAPATMAYIN. Telegram'a \"🚀 Kopya-trade motoru başladı\" mesajı gelmeli.")
        return CIKIS_UYARILI if any(not tamam for _m, tamam, _n in gunluk.ozet) else CIKIS_TAMAM
    except BaseException:
        g.guncelleme_kilidi_kaldir(veri)
        raise


# --------------------------------------------------------------------------- #
# Command line
# --------------------------------------------------------------------------- #
def _secenekler(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="tasi.py", description="Kopya-trade botunu yeni bir sunucuya taşır.")
    alt = p.add_subparsers(dest="komut", required=True, metavar="KOMUT")
    pk = alt.add_parser("paketle", help="ESKİ sunucuda: botu (ayarlar, veri, oturum) tek bir zip dosyasına koyar")
    pk.add_argument("--hedef", default=None, metavar="KLASOR", help="bot klasörü (varsayılan: bulunduğunuz klasör)")
    pk.add_argument("--yedekli", action="store_true", help="yedek klasörünü de pakete koy")
    kn = alt.add_parser("kontrol", help="bir paketi kurmadan kontrol eder")
    kn.add_argument("zip", metavar="ZIP")
    kr = alt.add_parser("kur", help="YENİ sunucuda: paketi açar, kurar, kontrol eder ve botu başlatır")
    kr.add_argument("zip", metavar="ZIP")
    kr.add_argument("--hedef", default=None, metavar="KLASOR", help="botun kurulacağı klasör")
    kr.add_argument("--uzerine-yaz", action="store_true", dest="uzerine_yaz",
                    help="hedef klasör doluysa adını değiştirip kenara al (silinmez)")
    kr.add_argument("--evet", action="store_true", help="soruları evet diye yanıtla")
    kr.add_argument("--baslatma", action="store_true", help="botu başlatma")
    kr.add_argument("--bekci-yok", action="store_true", dest="bekci_yok", help="bekçiyi kurma")
    kr.add_argument("--kontrol-yok", action="store_true", dest="kontrol_yok",
                    help="check ve selftest adımlarını atla (testler için)")
    return p.parse_args(argv)


def _varsayilan_gorev() -> Callable[..., Any] | None:
    return subprocess.run if os.name == "nt" else None


def main(
    argv: list[str] | None = None,
    *,
    calistir: Calistirici | None = None,
    surecler: Callable[[], Any] | None = None,
    girdi: Callable[[str], str] = input,
    yardimci: Yardimci = yardimci_calistir,
    gorev: Any = OTOMATIK,
    ek_paket: Any = OTOMATIK,
    baslat: Callable[[Path, str], None] = ayri_pencerede_baslat,
    python_bul: Callable[[Path], str] | None = None,
    baslama_bekleme: float = BASLAMA_BEKLEME,
    simdi: datetime | None = None,
) -> int:
    """The keyword arguments replace the outside world in tests (as in guncelle.main): ``calistir`` runs the long
    commands (default: guncelle.komut_calistir), ``surecler`` lists processes (default: guncelle.surec_listesi),
    ``yardimci`` runs the short helper commands, ``gorev`` runs schtasks, ``baslat`` starts the bot,
    ``python_bul`` names the bot's Python instead of venv\\Scripts\\python.exe (no venv is made then)."""
    for akis in (sys.stdout, sys.stderr):
        try:
            akis.reconfigure(errors="replace")
        except (AttributeError, ValueError):
            pass
    secenek = _secenekler(argv)
    if gorev is OTOMATIK:
        gorev = _varsayilan_gorev()
    gunluk = Gunluk()
    try:
        if secenek.komut == "kontrol":
            zip_yolu = Path(secenek.zip).expanduser()
            gunluk.yaz(f"=== PAKET KONTROLÜ ===  {zip_yolu}")
            manifest, sorunlar = paketi_dogrula(zip_yolu)
            if manifest is not None:
                manifest_ozeti(manifest, gunluk)
            if manifest is None or sorunlar:
                for metin in sorunlar[:20]:
                    gunluk.yaz(f"   - {metin}")
                gunluk.yaz("SONUÇ: SORUN (paket bozuk ya da değiştirilmiş; eski sunucudan yeniden kopyalayın)")
                return CIKIS_SORUN
            gunluk.yaz("SONUÇ: TAMAM (her dosya listeyle aynı)")
            return CIKIS_TAMAM
        if secenek.komut == "paketle":
            hedef = Path(os.path.abspath(Path(secenek.hedef).expanduser() if secenek.hedef else Path.cwd()))
            if not secenek.hedef and not bot_klasoru_mu(hedef) and bot_klasoru_mu(Path(__file__).resolve().parent):
                hedef = Path(__file__).resolve().parent
            zaman = (simdi or datetime.now()).strftime("%Y%m%d-%H%M%S")
            if bot_klasoru_mu(hedef):
                gunluk.ac(hedef / f"tasima_paketle_{zaman}.log")
            sur = surecler
            if sur is None:
                try:
                    sur = guncelle_yukle(hedef).surec_listesi
                except Exception:  # noqa: BLE001 - paketle says itself that the folder is wrong
                    sur = lambda: None   # noqa: E731
            kod = paketle(hedef, secenek, gunluk, sur, girdi, yardimci, gorev, simdi)
            gunluk.ozeti_yaz()
            return kod
        zip_yolu = Path(os.path.abspath(Path(secenek.zip).expanduser()))
        if not zip_yolu.is_file():
            gunluk.yaz(f"Paket bulunamadı: {zip_yolu}. Dosyanın tam yolunu yazın (ör. "
                       r"C:\Users\Administrator\Desktop\copytrader-tasima-....zip).")
            return CIKIS_REDDEDILDI

        kod = kur(zip_yolu, secenek, gunluk, calistir=calistir, surecler=surecler, girdi=girdi,
                  yardimci=yardimci, gorev=gorev, ek_paket=ek_paket, baslat=baslat, python_bul=python_bul,
                  baslama_bekleme=baslama_bekleme)
        gunluk.ozeti_yaz()
        return kod
    except KeyboardInterrupt:
        gunluk.yaz()
        gunluk.yaz("Durduruldu (Ctrl+C). Bot başlatılmadı. Aynı komutu tekrar çalıştırabilirsiniz.")
        return CIKIS_KESILDI
    finally:
        gunluk.kapat()


if __name__ == "__main__":
    sys.exit(main())
