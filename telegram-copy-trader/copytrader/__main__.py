"""Command line: ``python -m copytrader <komut> [--config config.yaml]``.

login      - first Telegram login (prints the session string)
chats      - list chats to find the source group id
run        - run the bot (holds <data_dir>/bot.lock while it runs)
stop       - ask the running bot to shut down as on Ctrl+C (writes <data_dir>/stop.request) and wait for it
check      - check config + broker connections (never trades)
parse      - show how a message would be understood (never trades)
migrate    - bring config.yaml up to date for this version (writes a backup first)
selftest   - read-only health report: accounts, symbols, prices, sizes, who may post in the source group
             (never trades, never posts)
canli-test - the ONLY command that trades: smallest real round trip on one broker, after a typed "EVET"

``stop`` waits (at most STOP_WAIT_SEC) until the bot has released bot.lock and its process has ended; exit code 0
only then (or when no bot was running).

login, chats, selftest and canli-test refuse to start while the bot is running (they would open the same
Telegram session / broker links); check and parse stay usable.
"""

from __future__ import annotations

import os as _os

# Windows' certificate store may lack the root CA some services use (MetaApi's websocket failed with
# CERTIFICATE_VERIFY_FAILED). Point OpenSSL at certifi's bundle before any SSL context is created; on Windows the
# system store is still loaded as well, so this only adds trusted roots.
if not _os.environ.get("SSL_CERT_FILE"):
    try:
        import certifi as _certifi

        _os.environ["SSL_CERT_FILE"] = _certifi.where()
    except ImportError:  # pragma: no cover - certifi ships with httpx/requests
        pass

import argparse
import asyncio
import html
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .brokers.base import Broker
from .config import AppConfig, TelegramConfig, load_config
from .engine.prices import PriceOracle
from .errors import ConfigError
from .models import ParseContext, Severity, StoredMessage, TradePlan

EXIT_OK, EXIT_PROBLEM, EXIT_CONFIG, EXIT_INTERRUPTED = 0, 1, 2, 130
_TAG = re.compile(r"<[^>]+>")
_BOT_TOKEN = re.compile(r"^\d{5,}:[A-Za-z0-9_-]{30,}$")


def html_to_text(text: str) -> str:
    """Telegram HTML -> terminal text."""
    return html.unescape(_TAG.sub("", text))


class PrintNotifier:
    """Notifier for CLI commands: prints to the terminal, never asks anything."""

    async def start(self, engine: Any) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def ask_confirmation(self, conf: Any) -> int | None:
        return None

    async def close_confirmation(self, conf: Any, result_text: str) -> None:
        return None

    async def notify(self, text: str, severity: Severity = Severity.INFO) -> None:
        label = {Severity.INFO: "BİLGİ", Severity.WARNING: "UYARI", Severity.ERROR: "HATA"}[severity]
        print(f"[{label}] {html_to_text(text)}")


# --------------------------------------------------------------------------- #
# Factories (patched in tests)
# --------------------------------------------------------------------------- #
def _make_parser(config: AppConfig) -> Any:
    from .app import make_parser

    return make_parser(config)      # same deadline as ``run``; no API key -> ConfigError (exit 2)


def _make_oracle(config: AppConfig, brokers: Any) -> Any:
    return PriceOracle(brokers, config.price_source_exchange)


def _make_brokers(config: AppConfig) -> tuple[list[Broker], list[str]]:
    """Brokers for read-only commands: always dry-run wrapped, so no order can ever be sent."""
    from .brokers.base import BrokerError
    from .brokers.registry import create_broker

    out: list[Broker] = []
    errors: list[str] = []
    for cfg in config.enabled_brokers:
        try:
            out.append(create_broker(cfg, dry_run=True, app_config=config))
        except BrokerError as exc:
            errors.append(f"{cfg.name}: {exc}")
    return out, errors


def _make_user_client(telegram_config: Any) -> Any:
    """The owner's Telegram user client exactly as the listener opens it (``selftest`` reads the group's
    posting rights with it; never used while the bot runs)."""
    from .telegram.listener import make_user_client

    return make_user_client(telegram_config)


def _make_live_broker(config: AppConfig, cfg: Any) -> Broker:
    """The REAL adapter of one broker, whatever ``dry_run`` says: its orders reach the venue. Only
    ``canli-test`` uses it."""
    from .brokers.registry import create_broker

    return create_broker(cfg, dry_run=False, app_config=config)


# --------------------------------------------------------------------------- #
# "Is the bot running?": <data_dir>/bot.lock, written by ``run``
# --------------------------------------------------------------------------- #
LOCK_FILE = "bot.lock"
BOOT_SLACK_SEC = 300.0      # a lock is "from before the last system start" only when it is older by this much
STOP_HINT = ('Önce çalışan botu kendi penceresinde Ctrl+C ile durdurun ve "Durduruldu." yazısını bekleyin '
             "(pencere yanıt vermiyorsa o pencereyi kapatın).")


def lock_hint(data_dir: str | os.PathLike[str]) -> str:
    """The way out when the lock is wrong (the check that reads other processes cannot be made perfect)."""
    return (f"Botun hiçbir pencerede çalışmadığından eminseniz {Path(data_dir) / LOCK_FILE} dosyasını silip "
            "tekrar deneyin.")


class BotRunningError(Exception):
    """A live process holds the bot lock. ``str(err)`` is Turkish."""


def _process_state_windows(pid: int) -> tuple[bool | None, str | None]:   # pragma: no cover - Windows only
    import ctypes
    from ctypes import wintypes

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)        # type: ignore[attr-defined]
    k32.OpenProcess.restype = wintypes.HANDLE
    k32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    k32.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
    k32.GetProcessTimes.argtypes = (wintypes.HANDLE,) + (ctypes.POINTER(wintypes.FILETIME),) * 4
    k32.CloseHandle.argtypes = (wintypes.HANDLE,)
    handle = k32.OpenProcess(0x1000, False, pid)                # PROCESS_QUERY_LIMITED_INFORMATION
    if not handle:
        # 5 = access denied: the process exists but is not ours to open; anything else: no such process
        return ctypes.get_last_error() == 5, None               # type: ignore[attr-defined]
    try:
        code = wintypes.DWORD()
        alive = bool(k32.GetExitCodeProcess(handle, ctypes.byref(code))) and code.value == 259   # STILL_ACTIVE
        created, t_exit, t_kernel, t_user = (wintypes.FILETIME() for _ in range(4))
        token = None
        if k32.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(t_exit), ctypes.byref(t_kernel),
                               ctypes.byref(t_user)):
            token = str((created.dwHighDateTime << 32) | created.dwLowDateTime)
        return alive, token
    finally:
        k32.CloseHandle(handle)


def _process_state(pid: int) -> tuple[bool | None, str | None]:
    """(alive, identity) of process ``pid``. ``alive`` None: cannot be told. The identity is the process's
    creation time where the system gives it (Windows, Linux): a pid that was given to ANOTHER process since
    (after a crash or a reboot) is then not mistaken for the bot. Never sends a signal."""
    if pid <= 0:
        return False, None
    try:
        if os.name == "nt":
            return _process_state_windows(pid)    # os.kill(pid, 0) would send Ctrl+C there
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False, None
        except PermissionError:
            pass                                  # it exists, it is just not ours
        try:
            # /proc/<pid>/stat: "pid (name) state ... starttime ..."; the name may contain spaces and brackets
            fields = Path(f"/proc/{pid}/stat").read_bytes().rsplit(b")", 1)[-1].split()
        except OSError:
            return True, None                     # no /proc (macOS...): alive, identity unknown
        if fields and fields[0] == b"Z":
            return False, None                    # exited, only not collected by its parent yet
        return True, (fields[19].decode() if len(fields) > 19 else None)
    except Exception:                             # noqa: BLE001 - a broken check must not stop a command
        return None, None


def _uptime_seconds() -> float | None:
    """Seconds since the system started; None when it cannot be told."""
    try:
        if os.name == "nt":                       # pragma: no cover - Windows only
            import ctypes

            ticks = ctypes.windll.kernel32.GetTickCount64       # type: ignore[attr-defined]
            ticks.restype = ctypes.c_ulonglong
            return float(ticks()) / 1000.0
        return float(Path("/proc/uptime").read_text(encoding="ascii").split()[0])
    except Exception:                             # noqa: BLE001 - unknown: the rule below is simply not applied
        return None


def _written_before_boot(started: Any) -> bool:
    """True when the lock's start time lies before the last system start: the process that wrote it cannot be
    alive, whoever owns its pid now (after a restart of the server the pid may belong to a process that cannot
    be inspected). Unknown uptime or an unreadable time: False."""
    uptime = _uptime_seconds()
    if uptime is None:
        return False
    try:
        began = datetime.fromisoformat(str(started))
    except (ValueError, TypeError):
        return False
    if began.tzinfo is None:
        return False
    return began < datetime.now(timezone.utc) - timedelta(seconds=uptime + BOOT_SLACK_SEC)


def lock_holder(data_dir: str | os.PathLike[str]) -> dict[str, Any] | None:
    """Content of ``<data_dir>/bot.lock`` when a LIVE bot process wrote it, else None (no file, unreadable,
    our own pid, process gone, the pid now belongs to another process, or the file is from before the last
    system start: a stale file never blocks)."""
    try:
        info = json.loads((Path(data_dir).expanduser() / LOCK_FILE).read_text(encoding="utf-8"))
        pid = int(info["pid"])
    except (OSError, ValueError, KeyError, TypeError):
        return None
    if pid == os.getpid():
        return None
    alive, token = _process_state(pid)
    if not alive:
        return None
    if token and info.get("token") and str(info["token"]) != token:
        return None
    if _written_before_boot(info.get("started")):
        return None
    return dict(info)


def running_text(info: dict[str, Any]) -> str:
    since = ""
    try:
        started = datetime.fromisoformat(str(info.get("started"))).astimezone()
        since = f", başlangıç {started.strftime('%d.%m.%Y %H:%M')}"
    except (ValueError, TypeError):
        pass
    return f"Bot şu anda çalışıyor (PID {info.get('pid')}{since})."


def acquire_lock(data_dir: str | os.PathLike[str]) -> Path:
    """Write the lock for this process. Raises :class:`BotRunningError` when a live bot holds it; a file left
    behind by a crash is replaced. OSError: the file could not be written (the caller decides)."""
    directory = Path(data_dir).expanduser()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / LOCK_FILE
    payload = json.dumps({
        "pid": os.getpid(), "started": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "token": _process_state(os.getpid())[1],
    })
    for _attempt in range(3):
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            holder = lock_holder(directory)
            if holder is not None:
                raise BotRunningError(f"{running_text(holder)} Aynı klasörde ikinci bir bot başlatılamaz. "
                                      f"{STOP_HINT} {lock_hint(directory)}") from None
            try:
                path.unlink()                     # stale: left by a crash / kill
            except FileNotFoundError:
                pass
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        return path
    raise OSError(f"{path} başka bir işlem tarafından sürekli yeniden yazılıyor")


def release_lock(path: Path) -> None:
    """Remove the lock if it is still ours (never another process's)."""
    try:
        if int(json.loads(path.read_text(encoding="utf-8")).get("pid", -1)) == os.getpid():
            path.unlink()
    except (OSError, ValueError, TypeError, AttributeError):
        pass


def _data_dir(config_path: str) -> str:
    """``data_dir`` of the config; also when the rest of the file does not load yet (login / chats)."""
    try:
        return str(load_config(config_path).data_dir)
    except Exception:
        try:
            import yaml

            raw = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
            value = raw.get("data_dir") if isinstance(raw, dict) else None
        except Exception:
            value = None
        return value if isinstance(value, str) and value and "${" not in value else "data"


# --------------------------------------------------------------------------- #
# Config helpers
# --------------------------------------------------------------------------- #
def _load_telegram_only(path: str) -> TelegramConfig:
    """Only the ``telegram`` section (login/chats work before the rest is filled in)."""
    import yaml

    from . import config as config_mod

    p = Path(path)
    config_mod._load_dotenv(p.parent / ".env")
    config_mod._load_dotenv(Path.cwd() / ".env")
    raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    return TelegramConfig.model_validate(config_mod._expand_env(raw.get("telegram") or {}))


def _telegram_config(path: str) -> TelegramConfig:
    try:
        return load_config(path).telegram
    except FileNotFoundError:
        raise
    except Exception:
        return _load_telegram_only(path)


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #
async def cmd_login(args: argparse.Namespace) -> int:
    from .telegram.listener import interactive_login, interactive_qr_login

    tg = _telegram_config(args.config)
    session = await (interactive_login(tg) if args.code else interactive_qr_login(tg))
    print()
    print("✅ Giriş başarılı. Oturum dosyası kaydedildi:", f"{tg.session_path}.session")
    print()
    print("⚠️  UYARI: Aşağıdaki oturum metni Telegram hesabınıza TAM erişim verir.")
    print("    Kimseyle paylaşmayın, ekran görüntüsü almayın. Sunucu/Docker için yalnızca")
    print("    .env dosyasında TG_SESSION_STRING olarak saklayın.")
    print()
    print(session)
    return EXIT_OK


async def cmd_chats(args: argparse.Namespace) -> int:
    from .telegram.listener import list_dialogs

    tg = _telegram_config(args.config)
    rows = await list_dialogs(tg, limit=args.limit)
    print(f"{'SOHBET ID':>16}  {'TÜR':<10}  AD")
    print("-" * 60)
    for chat_id, title, kind in rows:
        print(f"{chat_id:>16}  {kind:<10}  {title}")
    print()
    print("Sinyal grubunun ID'sini config.yaml içinde telegram.source_chats listesine yazın.")
    return EXIT_OK


async def cmd_run(args: argparse.Namespace) -> int:
    from . import app as app_mod

    data_dir = load_config(args.config).data_dir
    lock: Path | None = None
    try:
        lock = acquire_lock(data_dir)              # a second bot on the same data is refused (BotRunningError)
    except OSError as exc:
        # The lock is a guard, not a requirement: a file problem here must not keep the bot from starting.
        # (The watchman, bekci.py, reads the same file: without it a running bot looks stopped to it.)
        print(f"Uyarı: kilit dosyası yazılamadı ({exc}); bot yine de başlatılıyor. Bekçi kuruluysa botu kapalı "
              'sanıp "Bot çalışmıyor" uyarısı gönderebilir.')
    # A stop request still there is from before this bot (no bot held the lock): it must never stop this one.
    if app_mod.clear_stop_request(data_dir):
        logging.getLogger(__name__).warning("An old stop request file was removed at start")
    try:
        requested = await app_mod.run(args.config)
    finally:
        if lock is not None:
            release_lock(lock)
    if requested is True:
        print("\nDurduruldu.")                   # (stopped by `copytrader stop`: the same end as after Ctrl+C)
    return EXIT_OK


STOP_WAIT_SEC = 240.0       # ``stop`` waits this long for the bot to end
STOP_CHECK_SEC = 1.0        # ... looking this often
STOP_PROGRESS_SEC = 15.0    # ... and says that it still waits this often


def _bot_ended(data_dir: str, pid: int, token: Any) -> bool:
    """The bot process ``pid`` has released the lock and is gone (a pid that belongs to another process now counts
    as gone)."""
    if lock_holder(data_dir) is not None:
        return False
    alive, now_token = _process_state(pid)
    if alive is None:
        return True                               # cannot be told: the released lock decides
    return not alive or bool(token and now_token and str(token) != now_token)


async def cmd_stop(args: argparse.Namespace) -> int:
    from . import app as app_mod

    data_dir = _data_dir(args.config)
    holder = lock_holder(data_dir)
    if holder is None:
        app_mod.clear_stop_request(data_dir)      # nobody would read it: never left for a later bot
        print("Çalışan bir bot bulunamadı (bot zaten kapalı); durdurulacak bir şey yok.")
        return EXIT_OK
    pid = int(holder["pid"])
    path = app_mod.write_stop_request(data_dir, pid)
    print(f"{running_text(holder)} Durdurma isteği gönderildi; bot Ctrl+C'deki gibi kendi kapanışını yapıyor "
          f"(süren emir işlemlerini bitirip bağlantıları kapatır). En çok {STOP_WAIT_SEC / 60:g} dakika beklenir.",
          flush=True)
    loop = asyncio.get_running_loop()
    began = loop.time()
    say_at = began + STOP_PROGRESS_SEC
    taken = False
    try:
        while True:
            if not taken and not path.exists():
                taken = True
                print("Bot isteği aldı, kapanıyor...", flush=True)
            if _bot_ended(data_dir, pid, holder.get("token")):
                app_mod.clear_stop_request(data_dir)
                print("Bot durduruldu.", flush=True)
                return EXIT_OK
            now = loop.time()
            if now - began >= STOP_WAIT_SEC:
                break
            if now >= say_at:
                say_at += STOP_PROGRESS_SEC
                print(f"Bot hâlâ kapanıyor ({round(now - began)} sn)...", flush=True)
            await asyncio.sleep(STOP_CHECK_SEC)
    except BaseException:
        if not taken and app_mod.clear_stop_request(data_dir):
            print("Bekleme yarıda kesildi; bot isteği henüz almamıştı, istek geri alındı (bot çalışmaya devam ediyor).",
                  flush=True)
        raise
    minutes = f"{STOP_WAIT_SEC / 60:g}"
    if not taken and app_mod.clear_stop_request(data_dir):
        print(f"Bot {minutes} dakika içinde durdurma isteğini almadı; istek geri alındı, bot çalışmaya devam ediyor. "
              "Botun penceresine ve data/logs/copytrader.log dosyasına bakın. Botu zorla kapatmayın.")
    else:
        print(f"Bot kapanmaya başladı ama {minutes} dakika içinde bitmedi. Botun penceresine ve "
              "data/logs/copytrader.log dosyasına bakın; botu zorla kapatmayın.")
    return EXIT_PROBLEM


async def cmd_check(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    problems = 0
    print(f"Yapılandırma: {args.config}")
    print("Mod:", "DRY-RUN (gerçek emir gönderilmez)" if config.dry_run else "CANLI — GERÇEK EMİRLER GÖNDERİLİR")
    print(f"Risk: 1R = bakiyenin %{config.risk.risk_per_r_pct:g}'si · kaynak sohbet: {config.telegram.source_chats}")

    api_key = config.llm.api_key or os.environ.get("ANTHROPIC_API_KEY")
    print("Anthropic API anahtarı:", "ayarlı" if api_key and len(api_key) > 20 else "YOK / geçersiz görünüyor")
    problems += not (api_key and len(api_key) > 20)
    token_ok = bool(_BOT_TOKEN.match(config.notifier.bot_token.strip()))
    print("Bot token:", "ayarlı" if token_ok else "YOK / geçersiz görünüyor (@BotFather'dan alın)")
    problems += not token_ok
    owner_ok = config.notifier.owner_user_id > 0
    print("Sahip kullanıcı ID:", config.notifier.owner_user_id if owner_ok else "geçersiz")
    problems += not owner_ok
    session_ok = bool(config.telegram.session_string) or Path(f"{config.telegram.session_path}.session").is_file()
    print("Telegram kullanıcı oturumu:", "var" if session_ok else "YOK ('python -m copytrader login' çalıştırın)")
    problems += not session_ok

    # An optional broker (a friend's account) that cannot connect is shown but is not a problem of the check:
    # the bot runs without it, so it must not make the check (and an update that relies on it) fail.
    optional = {b.name for b in config.enabled_brokers if getattr(b, "optional", False)}
    optional_down: list[str] = []
    optional_note = "isteğe bağlı hesap: bot bu hesap olmadan çalışır, bu hesabın sinyalleri atlanır"
    brokers, errors = _make_brokers(config)
    for err in errors:
        if err.split(":", 1)[0] in optional:
            print(f"⚠️  {err} ({optional_note})")
            optional_down.append(err.split(":", 1)[0])
            continue
        print(f"❌ {err}")
        problems += 1
    if not brokers and not errors:
        print("⚠️  Etkin broker yok.")
        problems += 1
    for broker in brokers:
        try:
            broker.set_risk_config(config.risk)
            await asyncio.wait_for(broker.connect(), 120)
            equity = await broker.get_equity()
            free = await broker.get_free_margin()
            ccy = await broker.account_currency()
            print(f"✅ {broker.name} ({broker.kind}): hesap değeri {equity:,.2f} {ccy}, serbest {free:,.2f} {ccy}")
        except Exception as exc:
            reason = str(exc) or type(exc).__name__
            if broker.name in optional:
                print(f"⚠️  {broker.name}: bağlanılamadı: {reason} ({optional_note})")
                optional_down.append(broker.name)
            else:
                print(f"❌ {broker.name}: bağlanılamadı: {reason}")
                problems += 1
        finally:
            try:
                await broker.close()
            except Exception:
                pass
    print()
    if problems == 0 and optional_down:
        print(f"Sorun yok (isteğe bağlı {', '.join(optional_down)} bağlı değil).")
    else:
        print("Sorun yok." if problems == 0 else f"{problems} sorun bulundu.")
    return EXIT_OK if problems == 0 else EXIT_PROBLEM


async def cmd_parse(args: argparse.Namespace) -> int:
    from .engine.orchestrator import Engine
    from .storage.db import Database

    config = load_config(args.config)
    now = datetime.now(timezone.utc)
    msgs: list[StoredMessage] = []
    for i, image in enumerate(args.image or [], start=1):
        if not Path(image).is_file():
            print(f"Görsel bulunamadı: {image}")
            return EXIT_CONFIG
        msgs.append(StoredMessage(chat_id=0, message_id=i, date=now, media_path=str(image), media_type="photo"))
    if args.text or not msgs:
        msgs.append(StoredMessage(chat_id=0, message_id=len(msgs) + 1, date=now, text=args.text or ""))

    parser = _make_parser(config)       # before anything is opened: no API key -> ConfigError (exit 2)
    db_path = Path(config.data_dir) / "copytrader.db"
    db = Database(db_path if db_path.is_file() else ":memory:")   # real DB only to see open trades
    await db.open()
    brokers, errors = _make_brokers(config)
    for err in errors:
        print(f"[HATA] {err}")
    oracle = _make_oracle(config, lambda: engine.brokers)   # late-bound: brokers connected below
    engine = Engine(config, db, parser, brokers, PrintNotifier(), price_oracle=oracle)
    try:
        await engine.connect_brokers()       # read-only: no confirmations / trades touched
        ctx = ParseContext(now=now, open_trades=[t.summary() for t in await db.active_trades()])
        print("⏳ Okunuyor…")
        result = await parser.parse(msgs, ctx)
        print(html_to_text(await engine.describe_actions(result.actions, chat_id=0)))
        for action in result.actions:
            ev = await engine.evaluate(action, 0)
            if ev is not None and isinstance(ev.plan, TradePlan):
                print()
                print("📐 Broker bazında boyut önizlemesi (emir gönderilmez):")
                for line in await engine.preview_open(ev.plan, raw_symbol=action.symbol):
                    print("  •", line)
        if action_reasoning := [a.reasoning_brief for a in result.actions if a.reasoning_brief]:
            print()
            print("🔎 Okunan:", " | ".join(action_reasoning))
    finally:
        await engine.stop()
        close = getattr(oracle, "close", None)
        if close is not None:
            await close()
        aclose = getattr(parser, "aclose", None)
        if aclose is not None:
            await aclose()
        await db.close()
    return EXIT_OK


async def cmd_migrate(args: argparse.Namespace) -> int:
    from .migrate import run_cli

    return run_cli(args.config)


async def cmd_selftest(args: argparse.Namespace) -> int:
    from . import selftest

    config = load_config(args.config)
    logging.getLogger().setLevel(logging.CRITICAL)      # the report itself names every failure and its reason
    brokers, errors = _make_brokers(config)
    return await selftest.run_selftest(config, brokers, errors, config_path=args.config, assets=args.varlik,
                                       oracle_factory=_make_oracle, telegram_client=_make_user_client)


async def cmd_canli_test(args: argparse.Namespace) -> int:
    from . import livetest

    config = load_config(args.config)
    return await livetest.run_live_test(config, args.broker, symbol=args.sembol, hold_sec=args.bekle,
                                        broker_factory=_make_live_broker)


# --------------------------------------------------------------------------- #
# Argument parsing
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", default="config.yaml", help="yapılandırma dosyası (varsayılan: config.yaml)")

    p = argparse.ArgumentParser(
        prog="copytrader",
        description="Telegram grubundaki işlemleri kripto borsalarınıza ve MT4/MT5 brokerlarınıza kopyalar.",
    )
    sub = p.add_subparsers(dest="command", required=True, metavar="KOMUT")
    login = sub.add_parser("login", parents=[common],
                           help="Telegram kullanıcı hesabınızla ilk girişi yapar (varsayılan: QR kod ile)")
    login.add_argument("--code", action="store_true",
                       help="QR yerine telefon numarası + Telegram'dan gelen kod ile giriş")
    chats = sub.add_parser("chats", parents=[common], help="sohbetleri listeler (kaynak grubun ID'sini bulmak için)")
    chats.add_argument("--limit", type=int, default=200, help="en fazla kaç sohbet listelensin (varsayılan: 200)")
    sub.add_parser("run", parents=[common], help="botu çalıştırır")
    sub.add_parser("stop", parents=[common],
                   help="çalışan botu Ctrl+C'deki gibi durdurur ve kapanmasını bekler (en çok 4 dakika)")
    sub.add_parser("check", parents=[common], help="ayarları ve broker bağlantılarını kontrol eder (işlem açmaz)")
    parse = sub.add_parser("parse", parents=[common], help="bir mesajın nasıl anlaşılacağını gösterir (işlem açmaz)")
    parse.add_argument("--text", default="", help='mesaj metni, ör. "eth 1R long"')
    parse.add_argument("--image", action="append", metavar="DOSYA", help="ekran görüntüsü (birden fazla verilebilir)")
    sub.add_parser("migrate", parents=[common],
                   help="config.yaml dosyasını bu sürüme göre günceller (önce yedeğini alır)")
    selftest = sub.add_parser("selftest", parents=[common],
                              help="sağlık kontrolü: hesaplar, semboller, fiyat ve miktar; sinyal grubuna kimlerin "
                                   "yazabildiği (işlem açmaz, mesaj göndermez)")
    selftest.add_argument("--varlik", default="", metavar="LİSTE",
                          help="denenecek varlıklar, virgülle (ör. BTC,ETH,EURUSD); boş = hazır liste")
    live = sub.add_parser("canli-test", parents=[common],
                          help="TEK hesapta en küçük GERÇEK işlemi açıp kapatır (önce onay ister)")
    live.add_argument("--broker", required=True, metavar="AD", help="config.yaml'daki broker adı (name)")
    live.add_argument("--sembol", default=None, metavar="SEMBOL",
                      help="ör. EURUSD veya BTC (boş = hesabın konusuna göre seçilir)")
    live.add_argument("--bekle", type=float, default=0.0, metavar="SANİYE",
                      help="kapatmadan önce pozisyonu bu kadar saniye açık tut (varsayılan 0)")
    return p


_COMMANDS = {"login": cmd_login, "chats": cmd_chats, "run": cmd_run, "stop": cmd_stop, "check": cmd_check,
             "parse": cmd_parse, "migrate": cmd_migrate, "selftest": cmd_selftest, "canli-test": cmd_canli_test}
# These would open the bot's Telegram session / broker links a second time: refused while the bot runs.
_NEEDS_STOPPED_BOT = frozenset({"login", "chats", "selftest", "canli-test"})


def main(argv: list[str] | None = None) -> int:
    # Redirected output (file, Tee-Object, PowerShell ISE) uses a legacy code page without
    # emoji / block characters: replace them instead of crashing.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="replace")
        except (AttributeError, ValueError):
            pass
    args = build_parser().parse_args(argv)
    if args.command != "run":   # run sets up console + file logging itself
        logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    if args.command in _NEEDS_STOPPED_BOT:
        data_dir = _data_dir(args.config)
        holder = lock_holder(data_dir)
        if holder is not None:
            print(f"{running_text(holder)} '{args.command}' komutu bot çalışırken kullanılamaz (aynı Telegram "
                  f"oturumunu / hesap bağlantılarını açar). {STOP_HINT} {lock_hint(data_dir)}")
            return EXIT_PROBLEM
    try:
        return asyncio.run(_COMMANDS[args.command](args))
    except KeyboardInterrupt:
        print("\nDurduruldu.")
        return EXIT_INTERRUPTED
    except BotRunningError as exc:
        print(str(exc))
        return EXIT_CONFIG
    except FileNotFoundError as exc:
        print(f"Dosya bulunamadı: {exc.filename or exc}. (Örnek: config.example.yaml dosyasını config.yaml "
              "olarak kopyalayıp doldurun.)")
        return EXIT_CONFIG
    except ConfigError as exc:
        print(f"Yapılandırma hatası ({args.config}): {exc}")
        return EXIT_CONFIG
    except Exception as exc:
        from pydantic import ValidationError

        if isinstance(exc, ValidationError):
            print(f"Yapılandırma hatalı ({args.config}):\n{exc}")
            return EXIT_CONFIG
        if type(exc).__name__ == "TelegramAuthError" or (isinstance(exc, ValueError) and "environment" in str(exc)):
            print(f"Hata: {exc}")
            return EXIT_CONFIG
        logging.getLogger(__name__).exception("Command %s failed", args.command)
        print(f"Hata: {exc}")
        return EXIT_PROBLEM


if __name__ == "__main__":
    sys.exit(main())
