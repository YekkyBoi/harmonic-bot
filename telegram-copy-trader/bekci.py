#!/usr/bin/env python
"""Bekçi (the watchman): tells the owner on Telegram when the bot is NOT running. Owner notes: docs/GUNCELLEME.md.

The updater (guncelle.py) registers this file as a Windows scheduled task that runs every 5 minutes as SYSTEM,
so it also runs after a restart of the server before anyone logs on. Every run is one short pass:

* a live bot process holds ``<data_dir>/bot.lock``: nothing to say (after an outage that was reported: one
  "the bot is back" message);
* nobody holds it (no file, or a file left behind by a closed window, a crash or a restart of the server):
  ONE message, then a reminder at most every 30 minutes while the bot stays down;
* an update is running (``<data_dir>/guncelleme.lock`` younger than 30 minutes), the installed bot version does
  not write a lock file, or the answer cannot be told for certain: silence.

Standard library only and nothing of ``copytrader`` is imported: the pass never opens the bot's database, its
Telegram session or a broker link. The bot token and the owner's chat id are read from the bot folder's
config.yaml / .env and are never printed or written anywhere. Any error ends the pass quietly (the next one is
five minutes away); what was said is remembered in ``<data_dir>/bekci.json``, and a pass that cannot remember
says nothing, so the same message can never repeat every five minutes.

``python bekci.py --dene`` sends one test message now and prints whether that worked.
"""

from __future__ import annotations

import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PAKET = "copytrader"
KILIT_DOSYASI = "bot.lock"                # written by ``python -m copytrader run`` (copytrader/__main__.py)
GUNCELLEME_KILIDI = "guncelleme.lock"     # written by guncelle.py while an update is running
DURUM_DOSYASI = "bekci.json"
DENEME_ISTEGI = "bekci.dene"              # guncelle.py: "send one test message at your next pass"
HATIRLATMA_ARASI = 30 * 60.0              # seconds between two reminders while the bot stays down
GUNCELLEME_SURESI = 30 * 60.0             # an update lock younger than this means: an update is running
ACILIS_PAYI = 300.0                       # seconds; see BOOT_SLACK_SEC in copytrader/__main__.py
ZAMAN_ASIMI = 20.0                        # seconds for one Telegram request
TELEGRAM_API = "https://api.telegram.org"

CALISIYOR, KAPALI, BILINMIYOR, GUNCELLEME, ESKI_SURUM = "calisiyor", "kapali", "bilinmiyor", "guncelleme", "eski-surum"

ILK_UYARI = ("⚠️ Bot çalışmıyor (sunucu yeniden başlamış ya da pencere kapanmış olabilir). "
             "Sunucuya bağlanıp botu başlatın.")
HATIRLATMA = "⚠️ Bot hâlâ çalışmıyor (ilk uyarıdan bu yana {sure} geçti). Sunucuya bağlanıp botu başlatın."
GERI_GELDI = "✅ Bot tekrar çalışıyor."
DENEME_METNI = ("\U0001f6e1 Bekçi kuruldu ve çalışıyor: bot kapanırsa (sunucu yeniden başlarsa, pencere kapanırsa) "
                "birkaç dakika içinde buradan haber verilir.")
AYAR_YOK = "bot_token ya da owner_user_id okunamadı (config.yaml / .env)"
SUREC_OKUNAMIYOR = "bu sistemde çalışan programların durumu okunamıyor; botun çalışıp çalışmadığı anlaşılamaz"

_ORTAM = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_BOT_TOKEN = re.compile(r"\d{5,}:[A-Za-z0-9_-]{30,}")
_AYAR = re.compile(r"""(?<![\w-])(bot_token|owner_user_id)\s*:\s*("[^"]*"|'[^']*'|\$\{[^}]*\}|[^\s,}#]+)?""")


# --------------------------------------------------------------------------- #
# Settings: the bot folder's config.yaml and .env (read as text; no YAML library needed)
# --------------------------------------------------------------------------- #
def ortam_dosyasi(yol: Path) -> dict[str, str]:
    """KEY=VALUE lines of a .env file (the rules of the bot's own loader, copytrader/config.py)."""
    sonuc: dict[str, str] = {}
    try:
        satirlar = yol.read_text(encoding="utf-8-sig", errors="replace").splitlines()
    except OSError:
        return sonuc
    for ham in satirlar:
        satir = ham.strip()
        if not satir or satir.startswith("#") or "=" not in satir:
            continue
        ad, _, deger = satir.partition("=")
        ad = ad.strip()
        if ad.startswith("export "):
            ad = ad[len("export "):].strip()
        deger = deger.strip()
        if len(deger) >= 2 and deger[0] == deger[-1] and deger[0] in "'\"":
            deger = deger[1:-1]
        sonuc.setdefault(ad, deger)
    return sonuc


def _deger(ham: str | None, ortam: dict[str, str]) -> str:
    """A plain config value: quotes removed, ``${VAR}`` / ``${VAR:-default}`` filled in from ``ortam``."""
    metin = (ham or "").strip()
    if len(metin) >= 2 and metin[0] == metin[-1] and metin[0] in "'\"":
        metin = metin[1:-1]
    return _ORTAM.sub(lambda m: ortam.get(m.group(1)) or (m.group(2) or ""), metin).strip()


def bildirim_ayarlari(bot: Path) -> tuple[str | None, int | None]:
    """(bot token, owner chat id) of the ``notifier:`` section of config.yaml; None where it cannot be read.

    Values written as ``${NAME}`` come from the process environment or the folder's .env, like in the bot.
    When the section cannot be read at all, the names of .env.example (TG_BOT_TOKEN, TG_OWNER_ID) are tried."""
    ortam = ortam_dosyasi(bot / ".env")
    ortam.update({ad: deger for ad, deger in os.environ.items() if deger})
    bulunan: dict[str, str] = {}
    try:
        satirlar = (bot / "config.yaml").read_text(encoding="utf-8-sig", errors="replace").splitlines()
    except OSError:
        satirlar = []
    icinde = False
    for satir in satirlar:
        if not satir.strip() or satir.lstrip().startswith("#"):
            continue
        if not satir[0].isspace():                           # a top-level key starts or ends the section
            icinde = re.match(r"notifier\s*:", satir) is not None
            if not icinde:
                continue
            satir = satir.split(":", 1)[1]                   # "notifier: {bot_token: ..., owner_user_id: ...}"
        if icinde:
            for ad, ham in _AYAR.findall(re.split(r"\s#", satir, maxsplit=1)[0]):      # without a trailing comment
                bulunan.setdefault(ad, _deger(ham, ortam))
    token = bulunan.get("bot_token") or ortam.get("TG_BOT_TOKEN", "")
    kime = bulunan.get("owner_user_id") or ortam.get("TG_OWNER_ID", "")
    try:
        sayi: int | None = int(kime)
    except ValueError:
        sayi = None
    return (token if _BOT_TOKEN.fullmatch(token) else None), (sayi if sayi and sayi > 0 else None)


def veri_klasoru(bot: Path) -> Path:
    """``data_dir`` of config.yaml (default "data"), relative to the bot folder (as guncelle.py reads it)."""
    deger = "data"
    try:
        metin = (bot / "config.yaml").read_text(encoding="utf-8-sig", errors="replace")
        m = re.search(r"(?m)^data_dir[ \t]*:[ \t]*([^#\r\n]+)", metin)
        if m:
            bulunan = m.group(1).strip().strip("'\"")
            if bulunan and "${" not in bulunan:
                deger = bulunan
    except OSError:
        pass
    yol = Path(deger).expanduser()
    return yol if yol.is_absolute() else bot / yol


def kilit_yazan_surum(bot: Path) -> bool:
    """Does the INSTALLED bot version write ``bot.lock``? After ``guncelle.py --geri-al`` an older version may
    be installed that does not: its running bot cannot be told from a stopped one, so nothing is said."""
    try:
        return f'"{KILIT_DOSYASI}"' in (bot / PAKET / "__main__.py").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False


# --------------------------------------------------------------------------- #
# Is the bot running? (the rules of ``lock_holder`` in copytrader/__main__.py; never sends a signal)
# --------------------------------------------------------------------------- #
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
    """(alive, identity = creation time where the system tells it); alive None: it cannot be told."""
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
    except Exception:          # noqa: BLE001 - cannot tell
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
    """The lock's start time lies before the last system start (same rule as the bot's ``_written_before_boot``)."""
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


def bot_durumu(
    veri: Path,
    surec: Callable[[int], tuple[bool | None, str | None]] = surec_durumu,
    acilis: Callable[[Any], bool] = acilistan_once_yazilmis,
    mutlak: bool | None = None,
) -> str:
    """CALISIYOR when a live bot process wrote ``<veri>/bot.lock``, KAPALI when nobody holds it, BILINMIYOR
    when that cannot be told (then nothing is said).

    ``mutlak`` (default: on Windows): the creation time is a point in time, so a process with the lock's pid
    AND the lock's creation time is the bot itself and nothing else is asked; a clock that was corrected
    while the bot runs must not turn a running bot into an alarm. Elsewhere (Linux counts the creation time
    from the system start, so it can repeat after a restart) and where the creation time is not known, a lock
    from before the last system start counts as left behind, exactly as in the bot's own check."""
    if mutlak is None:
        mutlak = os.name == "nt"
    try:
        ham = (veri / KILIT_DOSYASI).read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return KAPALI
    except OSError:
        return BILINMIYOR            # held by another program this very moment, or not readable: no guessing
    try:
        bilgi = json.loads(ham)
        pid = int(bilgi["pid"])
    except (ValueError, KeyError, TypeError):
        return KAPALI                # empty or damaged: not the lock of a running bot
    if pid == os.getpid():
        return KAPALI                # this pass has that pid, so its writer is gone
    yasiyor, kimlik = surec(pid)
    if yasiyor is None:
        return BILINMIYOR
    if not yasiyor:
        return KAPALI
    yazilan = bilgi.get("token")
    if kimlik and yazilan:
        if str(yazilan) != kimlik:
            return KAPALI            # the pid belongs to another program now
        if mutlak:
            return CALISIYOR
    return KAPALI if acilis(bilgi.get("started")) else CALISIYOR


def guncelleme_suruyor(veri: Path, an: float) -> bool:
    """guncelle.py is at work: its lock file is younger than GUNCELLEME_SURESI (a forgotten one stops counting)."""
    try:
        yas = an - (veri / GUNCELLEME_KILIDI).stat().st_mtime
    except OSError:
        return False
    return -GUNCELLEME_SURESI < yas < GUNCELLEME_SURESI


# --------------------------------------------------------------------------- #
# What to say (pure) and what was said (bekci.json)
# --------------------------------------------------------------------------- #
def sure_metni(saniye: float) -> str:
    """'35 dakika', '2 saat 5 dakika', '3 gün 4 saat'."""
    dakika = max(1, int(saniye // 60))
    if dakika < 90:
        return f"{dakika} dakika"
    saat, dakika = divmod(dakika, 60)
    if saat < 48:
        return f"{saat} saat" + (f" {dakika} dakika" if dakika else "")
    gun, saat = divmod(saat, 24)
    return f"{gun} gün" + (f" {saat} saat" if saat else "")


def _sayi(deger: Any) -> float | None:
    return float(deger) if isinstance(deger, (int, float)) and not isinstance(deger, bool) else None


def karar(durum: dict[str, Any], gorulen: str, an: float) -> tuple[str | None, dict[str, Any]]:
    """(message to send or None, the state to remember once it is sent) for what this pass saw.

    ``durum``: what the earlier passes left (``ilk_uyari`` / ``son_uyari``: when the first / the last message
    about the present outage went out). Anything but CALISIYOR and KAPALI changes nothing."""
    yeni = dict(durum, son_durum=gorulen)
    ilk, son = _sayi(durum.get("ilk_uyari")), _sayi(durum.get("son_uyari"))
    if gorulen == CALISIYOR:
        yeni.update(ilk_uyari=None, son_uyari=None, uyari_sayisi=0)
        return (GERI_GELDI if ilk is not None else None), yeni
    if gorulen != KAPALI:
        return None, yeni
    if ilk is None or son is None:
        yeni.update(ilk_uyari=an, son_uyari=an, uyari_sayisi=1)
        return ILK_UYARI, yeni
    if an < son:                                   # the clock was set back: the 30 minutes count from now
        yeni["son_uyari"] = an
        return None, yeni
    if an - son >= HATIRLATMA_ARASI:
        try:
            sayi = int(durum.get("uyari_sayisi") or 1) + 1
        except (ValueError, TypeError):
            sayi = 2
        yeni.update(son_uyari=an, uyari_sayisi=sayi)
        return HATIRLATMA.format(sure=sure_metni(max(0.0, an - ilk))), yeni
    return None, yeni


def durum_oku(veri: Path) -> dict[str, Any]:
    try:
        durum = json.loads((veri / DURUM_DOSYASI).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return durum if isinstance(durum, dict) else {}


def durum_yaz(veri: Path, durum: dict[str, Any]) -> bool:
    """True only when the state is on file and reads back the same."""
    yol = veri / DURUM_DOSYASI
    gecici = veri / (DURUM_DOSYASI + ".tmp")
    try:
        gecici.write_text(json.dumps(durum, ensure_ascii=False), encoding="utf-8")
        os.replace(gecici, yol)
    except OSError:
        try:
            gecici.unlink()
        except OSError:
            pass
        return False
    return durum_oku(veri) == json.loads(json.dumps(durum))


def deneme_yap(bot: Path, gonder: Callable[[str, int, str], None] | None = None) -> str | None:
    """The test the updater (or ``--dene``) asks for: can a process be looked at here (this one), and does a
    message reach the owner? None: both; else the Turkish reason."""
    if surec_durumu(os.getpid())[0] is not True:
        return SUREC_OKUNAMIYOR
    return mesaj_gonder(bot, DENEME_METNI, gonder)


def deneme_istegi_al(veri: Path) -> str | None:
    """The updater's request for one test message (its id), taken off the disk; None: no request, or one that
    cannot be removed (it would be answered again at every pass)."""
    yol = veri / DENEME_ISTEGI
    try:
        istek = yol.read_text(encoding="utf-8", errors="replace").strip()[:80]
        yol.unlink()
    except OSError:
        return None
    return istek


# --------------------------------------------------------------------------- #
# Telegram (Bot API over HTTPS; the token never leaves this module)
# --------------------------------------------------------------------------- #
def gizle(metin: str, token: str | None = None) -> str:
    if token:
        metin = metin.replace(token, "***")
    return _BOT_TOKEN.sub("***", metin)


def _ssl_baglami() -> ssl.SSLContext:
    """The system's certificates plus certifi's bundle when that package is there (a Windows server knows only
    the certificate authorities it has met before; the bot itself needed the same help)."""
    baglam = ssl.create_default_context()
    try:
        import certifi

        baglam.load_verify_locations(certifi.where())
    except Exception:          # noqa: BLE001 - no certifi: the system's certificates alone
        pass
    return baglam


def telegram_gonder(token: str, kime: int, metin: str) -> None:
    """``sendMessage`` of the Bot API. Raises with a reason that never holds the token."""
    govde = urllib.parse.urlencode({"chat_id": str(kime), "text": metin}).encode("utf-8")
    istek = urllib.request.Request(f"{TELEGRAM_API}/bot{token}/sendMessage", data=govde, method="POST")
    try:
        with urllib.request.urlopen(istek, timeout=ZAMAN_ASIMI, context=_ssl_baglami()) as yanit:
            ham = yanit.read(65536)
    except urllib.error.HTTPError as exc:
        try:
            aciklama = str(json.loads(exc.read(65536)).get("description") or "")
        except Exception:      # noqa: BLE001
            aciklama = ""
        raise RuntimeError(gizle(f"Telegram mesajı kabul etmedi (HTTP {exc.code}{': ' + aciklama if aciklama else ''})",
                                 token)) from None
    except Exception as exc:   # noqa: BLE001 - no network, no name resolution, certificate, timeout...
        neden = getattr(exc, "reason", None) or exc
        raise RuntimeError(gizle(f"Telegram'a ulaşılamadı ({neden})", token)) from None
    try:
        tamam = json.loads(ham).get("ok") is True
    except Exception:          # noqa: BLE001
        tamam = False
    if not tamam:
        raise RuntimeError("Telegram'ın yanıtı anlaşılamadı")


def mesaj_gonder(bot: Path, metin: str, gonder: Callable[[str, int, str], None] | None = None) -> str | None:
    """Send ``metin`` to the owner. None: sent; else the Turkish reason (without secrets)."""
    token, kime = bildirim_ayarlari(bot)
    if token is None or kime is None:
        return AYAR_YOK
    try:
        (gonder or telegram_gonder)(token, kime, metin)
    except Exception as exc:   # noqa: BLE001
        return gizle(str(exc) or type(exc).__name__, token)[:300]
    return None


# --------------------------------------------------------------------------- #
# One pass
# --------------------------------------------------------------------------- #
def bak(
    bot: Path,
    *,
    simdi: Callable[[], float] = time.time,
    gonder: Callable[[str, int, str], None] | None = None,
    durum_sor: Callable[[Path], str] = bot_durumu,
) -> str:
    """One pass over the bot folder ``bot``; returns what it saw / did (for the tests). The keyword arguments
    replace the clock, the Telegram request and the look at the lock file."""
    veri = veri_klasoru(bot)
    an = float(simdi())
    kayit = dict(durum_oku(veri), son_bakis=an)        # what stays on file when nothing new is said
    istek = deneme_istegi_al(veri)
    if istek is not None:
        hata = deneme_yap(bot, gonder)
        kayit["deneme"] = {"istek": istek, "sonuc": hata or "tamam", "zaman": an}
    if guncelleme_suruyor(veri, an):
        gorulen = GUNCELLEME
    elif not kilit_yazan_surum(bot):
        gorulen = ESKI_SURUM
    else:
        gorulen = durum_sor(veri)
    mesaj, yeni = karar(kayit, gorulen, an)
    if mesaj is None:
        durum_yaz(veri, yeni)
        return gorulen
    if not durum_yaz(veri, dict(yeni, son_hata=None)):
        return "durum-yazilamadi"                      # cannot remember what is said: say nothing
    hata = mesaj_gonder(bot, mesaj, gonder)
    if hata is None:
        return "gonderildi"
    durum_yaz(veri, dict(kayit, son_durum=gorulen, son_hata=hata))     # as before: the next pass tries again
    return "gonderilemedi"


def main(argv: list[str] | None = None) -> int:
    secenek = sys.argv[1:] if argv is None else argv
    bot = Path(__file__).resolve().parent
    try:
        if "--dene" in secenek:
            for akis in (sys.stdout, sys.stderr):
                try:
                    akis.reconfigure(errors="replace")       # legacy code pages: never crash on Turkish letters
                except (AttributeError, ValueError):
                    pass
            hata = deneme_yap(bot)
            print("Deneme mesajı Telegram'a gönderildi." if hata is None else f"Deneme başarısız: {hata}")
            return 0 if hata is None else 1
        bak(bot)
    except Exception:          # noqa: BLE001 - quiet by design: the next pass is five minutes away
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
