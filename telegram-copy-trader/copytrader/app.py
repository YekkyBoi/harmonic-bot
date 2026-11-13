"""Application wiring: build every component from the config and run until stopped."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import signal
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from .brokers.base import Broker, BrokerError
from .config import AppConfig, load_config
from .engine.orchestrator import Engine
from .errors import ConfigError, ParserError
from .models import Severity

logger = logging.getLogger(__name__)

_HANDLER_TAG = "_copytrader_handler"
_NOISY_LOGGERS = ("telethon", "aiosqlite", "asyncio", "httpx", "httpcore", "anthropic", "ccxt", "urllib3", "metaapi",
                  "metaapi_cloud_sdk", "engineio", "socketio")
LOG_FILE_MAX_BYTES = 10 * 1024 * 1024
LOG_FILE_BACKUPS = 5
MIN_PARSE_DEADLINE_SEC = 60.0      # a slow (image + high effort) reading must still fit

# ---- secrets never reach the console or the log file (defence in depth) ----
SECRET_MASK = "***"
_SECRET_NAME = (r"(?:auth[-_]?token|access[-_]?token|bot[-_]?token|token|api[-_]?key|apikey|api[-_]?secret|"
                r"api[-_]?hash|secret|password|passwd|passphrase|session[-_]?string|signature|authorization)")
_SECRET_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    # name=value, name: value, "name": "value" (URL query, HTTP header, dict / JSON text)
    (re.compile(rf"""(?i)(\b{_SECRET_NAME}["']?\s*[=:]\s*(?:bearer\s+)?["']?)[^\s"'&,;<>)}}\]]+"""), r"\1" + SECRET_MASK),
    (re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{30,}"), SECRET_MASK),                                 # Telegram bot token
    (re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"), SECRET_MASK),    # JWT (MetaApi token)
    (re.compile(r"\bsk-ant-[A-Za-z0-9_-]{10,}"), SECRET_MASK),                                   # Anthropic API key
    (re.compile(r"\b1[A-Za-z0-9_-]{250,}={0,2}"), SECRET_MASK),                                  # Telethon session string
)
_SECRET_FIELD = re.compile(r"(?i)(secret|password|passphrase|token|api_key|api_hash|session_string)$")
MIN_SECRET_CHARS = 6               # shorter config values are not masked literally (they would hide ordinary text)

# ---- the reason of a failed MetaApi websocket connection (engineio / socketio log it at INFO) ----
_CONNECT_FAILURE_LOGGERS = ("engineio.client", "socketio.client")
_CONNECT_FAILURE = re.compile(r"failed with error|[Cc]onnection failed|[Cc]onnection refused|was rejected|"
                              r"attempts reached|upgrade failed|connection dropped|Unexpected status code")
CONNECT_FAILURE_LOG_EVERY_SEC = 60.0   # at most one such line per kind in this time (a failing connect loops fast)
# ... and the moment an ESTABLISHED websocket was lost (engineio logs these at INFO, each once per socket and drop):
# the keep-alive answer did not come, the socket's loops ended. Anchored at the start of the message, so the text of
# a received packet can never match.
_CONNECTION_LOST = re.compile(r"^(?:PONG response has not been received|Exiting ping task|"
                              r"(?:Read|Write) loop: WebSocket connection was closed|"
                              r"Unexpected error (?:receiving|decoding) packet)")
CONNECTION_LOST_BURST = 4              # lines per kind in that time: a drop takes down both main sockets at once
_FILTER_TAG = "_copytrader_filter"

# ---- event-loop stall watchdog (diagnostics): while the loop is held, no websocket keep-alive, timer or order flow
# of the bot can run ----
LOOP_WATCH_TICK_SEC = 1.0          # the watchdog sleeps this long in a loop ...
LOOP_STALL_WARN_SEC = 5.0          # ... and logs a WARNING when it wakes up more than this late
LOOP_STALL_LOG_EVERY_SEC = 60.0    # at most one such line in this time; the stalls in between are counted


def mask_secrets(text: str, secrets: Iterable[str] = ()) -> str:
    """``text`` with tokens, API keys, passwords and Telegram session strings replaced by ``***``.

    ``secrets``: exact values to hide as well (the loaded config's own secrets)."""
    for value in secrets:
        if value and value in text:
            text = text.replace(value, SECRET_MASK)
    for pattern, replacement in _SECRET_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def config_secrets(config: Any) -> list[str]:
    """Every secret value of a loaded config (keys, tokens, passwords, session string), longest first."""
    found: set[str] = set()

    def walk(node: Any) -> None:
        fields = getattr(type(node), "model_fields", None)
        if isinstance(fields, dict):
            for name in fields:
                value = getattr(node, name, None)
                if isinstance(value, str):
                    if _SECRET_FIELD.search(name):
                        found.add(value.strip())
                else:
                    walk(value)
        elif isinstance(node, dict):
            for value in node.values():
                walk(value)
        elif isinstance(node, (list, tuple)):
            for value in node:
                walk(value)

    walk(config)
    for env in ("ANTHROPIC_API_KEY", "TG_SESSION_STRING"):
        found.add((os.environ.get(env) or "").strip())
    return sorted((v for v in found if len(v) >= MIN_SECRET_CHARS), key=len, reverse=True)


class SecretMaskFilter(logging.Filter):
    """On every handler: masks secrets in the message, the exception text and the stack of any record,
    whatever logger it comes from (third-party libraries included)."""

    def __init__(self, secrets: Iterable[str] = ()) -> None:
        super().__init__()
        self._secrets = [s for s in secrets if isinstance(s, str) and len(s) >= MIN_SECRET_CHARS]

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            return True                  # logging reports the formatting error itself
        masked = mask_secrets(message, self._secrets)
        if masked != message:
            record.msg, record.args = masked, ()
        try:
            if record.exc_info and not record.exc_text:
                record.exc_text = logging.Formatter().formatException(record.exc_info)
            if record.exc_text:
                record.exc_text = mask_secrets(record.exc_text, self._secrets)
            if record.stack_info:
                record.stack_info = mask_secrets(record.stack_info, self._secrets)
        except Exception:                # never lose a record because its traceback could not be rendered here
            pass
        return True


class ConnectFailureFilter(logging.Filter):
    """On the 'engineio.client' / 'socketio.client' loggers (the MetaApi websocket): these libraries log
    WHY a connection failed at INFO, among hundreds of harmless lines. Records that report a failed
    connection pass as WARNING (at most one per kind in ``every_sec``, with the number left out), and so
    do those that report the loss of an established one ("PONG response has not been received", "Exiting
    ping task", ...: up to CONNECTION_LOST_BURST per kind in ``every_sec``, so the drop's exact time and
    every socket it took down are in the log); everything else below WARNING stays quiet. Secrets are
    masked by the handlers' SecretMaskFilter."""

    def __init__(self, every_sec: float = CONNECT_FAILURE_LOG_EVERY_SEC) -> None:
        super().__init__()
        self._every = every_sec
        self._last: dict[str, float] = {}       # kind -> start of its present window
        self._passed: dict[str, int] = {}       # kind -> lines let through in that window
        self._hidden: dict[str, int] = {}

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.WARNING:
            return True
        try:
            message = record.getMessage()
        except Exception:
            return False
        lost = _CONNECTION_LOST.search(message) is not None
        if not lost and not _CONNECT_FAILURE.search(message):
            return False
        kind = f"{record.name}:{str(record.msg)[:40]}"
        now = time.monotonic()
        last = self._last.get(kind)
        if last is not None and now - last < self._every:
            if self._passed.get(kind, 1) >= (CONNECTION_LOST_BURST if lost else 1):
                self._hidden[kind] = self._hidden.get(kind, 0) + 1
                return False
            self._passed[kind] = self._passed.get(kind, 1) + 1
        else:
            self._last[kind] = now
            self._passed[kind] = 1
        hidden = self._hidden.pop(kind, 0)
        record.levelno, record.levelname = logging.WARNING, "WARNING"
        if hidden:
            record.msg, record.args = f"{message} (+{hidden} similar line(s) not logged)", ()
        return True


def setup_logging(level: str = "INFO", data_dir: str | os.PathLike[str] | None = None,
                  secrets: Iterable[str] = ()) -> Path | None:
    """Console + rotating file (``<data_dir>/logs/copytrader.log``). Idempotent. Returns the log file path.

    ``secrets``: exact values that must never be written (see :func:`config_secrets`); token-, key- and
    password-shaped text is masked in any case."""
    lvl = logging.getLevelName(str(level).upper())
    if not isinstance(lvl, int):
        lvl = logging.INFO
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, _HANDLER_TAG, False):
            root.removeHandler(handler)
            handler.close()
    mask = SecretMaskFilter(list(secrets))
    formatter = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.addFilter(mask)
    setattr(console, _HANDLER_TAG, True)
    root.addHandler(console)
    log_path: Path | None = None
    if data_dir is not None:
        log_dir = Path(data_dir).expanduser() / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / "copytrader.log"
        file_handler = RotatingFileHandler(log_path, maxBytes=LOG_FILE_MAX_BYTES, backupCount=LOG_FILE_BACKUPS,
                                           encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.addFilter(mask)
        setattr(file_handler, _HANDLER_TAG, True)
        root.addHandler(file_handler)
    root.setLevel(lvl)
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(max(lvl, logging.WARNING))
    for name in _CONNECT_FAILURE_LOGGERS:
        noisy = logging.getLogger(name)
        for old in list(noisy.filters):
            if getattr(old, _FILTER_TAG, False):
                noisy.removeFilter(old)
        gate = ConnectFailureFilter()
        setattr(gate, _FILTER_TAG, True)
        noisy.addFilter(gate)
        noisy.setLevel(logging.INFO)     # the filter decides: failed-connection lines only
    return log_path


async def watch_event_loop(*, tick_sec: float = LOOP_WATCH_TICK_SEC, warn_after_sec: float = LOOP_STALL_WARN_SEC,
                           every_sec: float = LOOP_STALL_LOG_EVERY_SEC) -> None:
    """Runs until cancelled (serve() starts it): sleeps ``tick_sec`` in a loop and logs the WARNING
    "Olay döngüsü <n> sn gecikti" when it wakes up more than ``warn_after_sec`` late, i.e. when the event loop
    was held (a blocking call, a starved process, a paused machine) and nothing of the bot could run, the
    MetaApi websockets' keep-alive answers included. At most one line per ``every_sec``: a stall in between is
    counted and the count (with the longest delay) is added to the next line, at the latest when that time is
    over. Costs one timer per ``tick_sec``."""
    loop = asyncio.get_running_loop()
    last_line: float | None = None
    hidden, worst = 0, 0.0
    while True:
        due = loop.time() + tick_sec
        await asyncio.sleep(tick_sec)
        now = loop.time()
        late = now - due
        stalled = late > warn_after_sec
        window_open = last_line is None or now - last_line >= every_sec
        if stalled and not window_open:
            hidden, worst = hidden + 1, max(worst, late)
            continue
        if stalled:
            more = f" (son uyarıdan beri {hidden} gecikme daha oldu, en uzunu {worst:.1f} sn)" if hidden else ""
            logger.warning("Olay döngüsü %.1f sn gecikti%s", late, more)
        elif hidden and window_open:
            logger.warning("Olay döngüsü son uyarıdan beri %d kez daha %g sn'den uzun gecikti (en uzunu %.1f sn)",
                           hidden, warn_after_sec, worst)
        else:
            continue
        last_line, hidden, worst = now, 0, 0.0


def parser_deadline_sec(config: AppConfig) -> float:
    """Overall limit of one parse() call: ``llm.timeout_sec``, but at most a third of
    ``telegram.context_window_sec`` (so a stalled API leaves room for the engine's retries and does not
    hold the sender's later messages for long), never below MIN_PARSE_DEADLINE_SEC (unless timeout_sec is)."""
    window_share = max(MIN_PARSE_DEADLINE_SEC, float(config.telegram.context_window_sec) / 3.0)
    return min(float(config.llm.timeout_sec), window_share)


def make_parser(config: AppConfig) -> Any:
    """The Claude parser for ``config``. A setup problem (no API key, broken credentials) raises
    :class:`ConfigError` (the CLI exits with the config-error code, so systemd does not restart-loop)."""
    from .parsing.llm_parser import ClaudeSignalParser

    try:
        return ClaudeSignalParser(config.llm, deadline_sec=parser_deadline_sec(config))
    except ParserError as exc:
        raise ConfigError(str(exc)) from exc


@dataclass
class App:
    config: AppConfig
    db: Any
    parser: Any
    brokers: list[Broker]
    notifier: Any
    engine: Engine
    listener: Any
    broker_errors: list[str] = field(default_factory=list)


def build_app(
    config: AppConfig,
    *,
    db: Any | None = None,
    parser: Any | None = None,
    notifier: Any | None = None,
    broker_factory: Callable[[Any], Broker] | None = None,
    listener_factory: Callable[..., Any] | None = None,
) -> App:
    """Construct (but do not start) every component. Injectable parts are for tests."""
    data_dir = Path(config.data_dir).expanduser()
    data_dir.mkdir(parents=True, exist_ok=True)
    if db is None:
        from .storage.db import Database

        db = Database(data_dir / "copytrader.db")
    if parser is None:
        parser = make_parser(config)
    if broker_factory is None:
        from .brokers.registry import create_broker

        def broker_factory(cfg: Any) -> Broker:
            return create_broker(cfg, dry_run=config.dry_run, app_config=config)

    brokers: list[Broker] = []
    errors: list[str] = []
    optional_errors: dict[str, str] = {}
    for cfg in config.enabled_brokers:
        try:
            brokers.append(broker_factory(cfg))
        except BrokerError as exc:
            errors.append(f"{cfg.name}: {exc}")
            if getattr(cfg, "optional", False):
                optional_errors[cfg.name] = str(exc)
            logger.error("Broker %s could not be created: %s", cfg.name, exc)
    if notifier is None:
        from .telegram.notifier import BotNotifier

        notifier = BotNotifier(config.notifier, config.telegram, str(data_dir))
    engine = Engine(config, db, parser, brokers, notifier)
    for err in errors:
        engine.failed_brokers[err.split(":", 1)[0]] = err.split(":", 1)[-1].strip()
    engine.optional_down.update(optional_errors)     # optional + could not even be created: skipped, no retry
    if listener_factory is None:
        from .telegram.listener import TelegramListener

        listener_factory = TelegramListener
    listener = listener_factory(
        config.telegram, db, str(data_dir), on_ready=engine.on_messages_ready, on_edit=engine.on_message_edited
    )
    set_notifier = getattr(listener, "set_notifier", None)
    if callable(set_notifier):
        set_notifier(notifier)           # the listener tells the owner about skipped messages, lost images...
    return App(config, db, parser, brokers, notifier, engine, listener, errors)


SHUTDOWN_NOTICE_TR = "Kapanıyor, lütfen bekleyin (en çok 2 dakika). Tekrar Ctrl+C YAPMAYIN."
SHUTDOWN_REMINDER_TR = "Kapanış sürüyor (açık emir işlemleri bitiriliyor). Lütfen bekleyin, tekrar Ctrl+C yapmayın."
SHUTDOWN_FORCED_TR = ("Kapanış 3 dakikadır bitmedi; bot şimdi zorla durduruluyor. Yeniden başlatınca açık "
                      "pozisyonları ve stopları kontrol edin.")
HARD_INTERRUPT_AFTER_SEC = 180.0   # a shutdown still running this long after the first Ctrl+C may be interrupted hard
# ``python -m copytrader stop`` writes this file into data_dir; the running bot (``run``) looks for it every
# STOP_POLL_SEC and then shuts down exactly as after the first Ctrl+C. Only a request that names the bot's own process
# id counts; ``run`` removes a file left from before it started (it can never stop a later bot).
STOP_REQUEST_FILE = "stop.request"
STOP_POLL_SEC = 2.0
STOP_REQUEST_TR = "Durdurma isteği alındı (python -m copytrader stop). " + SHUTDOWN_NOTICE_TR


def stop_request_path(data_dir: str | os.PathLike[str]) -> Path:
    return Path(data_dir).expanduser() / STOP_REQUEST_FILE


def write_stop_request(data_dir: str | os.PathLike[str], pid: int) -> Path:
    """Ask the bot process ``pid`` to stop (written at once and whole: a temporary file renamed over it)."""
    path = stop_request_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps({"pid": int(pid), "by": os.getpid(),
                               "requested": time.strftime("%Y-%m-%dT%H:%M:%S%z")}), encoding="utf-8")
    os.replace(tmp, path)
    return path


def clear_stop_request(data_dir: str | os.PathLike[str]) -> bool:
    """Remove a stop request (True if there was one). Never raises."""
    try:
        stop_request_path(data_dir).unlink()
        return True
    except FileNotFoundError:
        return False
    except OSError as exc:
        logger.warning("Could not remove the stop request file: %s", exc)
        return False


def read_stop_request(path: Path) -> dict[str, Any] | None:
    """The request in ``path`` ({"pid": ..., ...}), None when there is none or it cannot be read right now (the next
    poll reads it again); a file whose content is not a request counts as one for no process (``{"pid": None}``)."""
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError:
        return None         # (e.g. a Windows sharing violation while it is being written: the next poll reads it)
    try:
        info = json.loads(raw)
        return info if isinstance(info, dict) else {"pid": None}
    except ValueError:
        return {"pid": None}


async def watch_stop_request(path: Path, on_request: Callable[[], None], *, poll_sec: float = STOP_POLL_SEC) -> None:
    """Look for a stop request every ``poll_sec`` until one for THIS process comes: remove it, call ``on_request`` and
    end. A request for another process (left by an earlier bot) is removed and never counts. Runs until cancelled;
    nothing it meets ends it otherwise."""
    me = os.getpid()
    while True:
        await asyncio.sleep(poll_sec)
        try:
            info = read_stop_request(path)
            if info is None:
                continue
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            try:
                pid = int(info.get("pid"))         # type: ignore[arg-type]
            except (TypeError, ValueError):
                pid = None
            if pid != me:
                logger.warning("Stop request file for another process (pid %s, this bot is %s) removed; ignored",
                               info.get("pid"), me)
                continue
            logger.warning("Stop requested (%s by `python -m copytrader stop`, pid %s): shutting down", path.name,
                           info.get("by"))
            on_request()
            return
        except Exception:                       # noqa: BLE001 - the watch must never end the bot
            logger.exception("Stop request watch failed; it goes on")


class _Interrupts:
    """Ctrl+C / SIGTERM while serve() runs. The FIRST one asks for the graceful shutdown; every further
    one during that shutdown is swallowed (it used to abort an order flow between its entry and its
    stop). Only when the shutdown is still running HARD_INTERRUPT_AFTER_SEC later does another Ctrl+C
    work as before.

    * Where the loop has signal handlers (Linux, macOS): they set ``stop``.
    * Where it has none (Windows): asyncio.run()'s own SIGINT handler cancels the main task on the first
      Ctrl+C (serve() then shuts down gracefully inside that cancelled task and the CLI prints
      "Durduruldu.") and raises KeyboardInterrupt inside the loop on the second. That handler is wrapped:
      its first reaction is kept, the later ones are swallowed."""

    def __init__(self, stop: asyncio.Event) -> None:
        self.stop = stop
        self.count = 0
        self._first_at = 0.0
        self._loop = asyncio.get_running_loop()
        self._loop_signals: list[int] = []
        self._previous: Any = None
        self._wrapped = False
        self.requested = False           # the shutdown was asked for by a stop request (request_stop)

    def install(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                self._loop.add_signal_handler(sig, self._on_loop_signal, int(sig))
                self._loop_signals.append(int(sig))
            except (NotImplementedError, RuntimeError, ValueError):
                pass  # Windows: Ctrl+C arrives as KeyboardInterrupt / task cancellation
        if int(signal.SIGINT) not in self._loop_signals:
            self._wrap_sigint()

    def restore(self) -> None:
        self._remove_loop_handlers()
        self._unwrap_sigint()

    def request_stop(self) -> None:
        """A stop request from outside (``python -m copytrader stop``): the same as the first Ctrl+C on a system with
        loop signal handlers - the graceful shutdown starts; a Ctrl+C during it is swallowed like a second one."""
        if self._again():
            return                       # a Ctrl+C came first: that shutdown is running already
        self.requested = True
        self._say(STOP_REQUEST_TR)
        self.stop.set()

    # ---- loop signal handlers (POSIX) ----
    def _on_loop_signal(self, signum: int) -> None:
        if self._again():
            logger.warning("Signal %s received again during the shutdown: ignored", signum)
            self._say(SHUTDOWN_REMINDER_TR)
            return
        if self.count > 1:               # the shutdown hangs: this signal gets its default (hard) effect
            logger.error("Signal %s: shutdown still running after %.0fs; interrupting hard",
                         signum, HARD_INTERRUPT_AFTER_SEC)
            self._say(SHUTDOWN_FORCED_TR)
            self._remove_loop_handlers()
            signal.raise_signal(signum)
            return
        logger.warning("Signal %s received: shutting down", signum)
        self._say(SHUTDOWN_NOTICE_TR)
        self.stop.set()

    def _remove_loop_handlers(self) -> None:
        for sig in self._loop_signals:
            with contextlib.suppress(Exception):
                self._loop.remove_signal_handler(sig)
        self._loop_signals = []

    # ---- plain SIGINT handler (Windows) ----
    def _wrap_sigint(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        previous = signal.getsignal(signal.SIGINT)
        if not callable(previous):
            return

        def on_sigint(signum: int, frame: Any) -> None:
            if self._again():
                self._loop.call_soon_threadsafe(self._say, SHUTDOWN_REMINDER_TR)
                return
            if self.count == 1:
                self._loop.call_soon_threadsafe(self._say, SHUTDOWN_NOTICE_TR)
            else:                        # the shutdown hangs: this Ctrl+C works as it did before
                self._say(SHUTDOWN_FORCED_TR)
            try:
                previous(signum, frame)  # asyncio.run(): cancels the main task -> graceful shutdown in serve()
            except BaseException:
                self._unwrap_sigint()    # not asyncio.run()'s handler (or its hard interrupt): as before
                raise

        try:
            signal.signal(signal.SIGINT, on_sigint)
        except (ValueError, OSError):
            return
        self._previous, self._wrapped = previous, True

    def _unwrap_sigint(self) -> None:
        if self._wrapped:
            self._wrapped = False
            with contextlib.suppress(ValueError, OSError, TypeError):
                signal.signal(signal.SIGINT, self._previous)

    # ---- shared ----
    def _again(self) -> bool:
        """Count this interrupt. True = a repeated one that must be swallowed."""
        self.count += 1
        now = time.monotonic()
        if self.count == 1:
            self._first_at = now
            return False
        return now - self._first_at < HARD_INTERRUPT_AFTER_SEC

    @staticmethod
    def _say(text: str) -> None:
        with contextlib.suppress(Exception):
            print(text, flush=True)


async def serve(app: App, stop: asyncio.Event | None = None, *, stop_file: Path | None = None) -> bool:
    """Start everything, run until the listener ends or ``stop`` is set, then shut down in order. ``stop_file``: a
    stop request written there (:data:`STOP_REQUEST_FILE`) shuts down like the first Ctrl+C. Returns True when the
    shutdown came from such a request."""
    stop = stop or asyncio.Event()
    interrupts = _Interrupts(stop)
    interrupts.install()
    watchdog = asyncio.get_running_loop().create_task(
        watch_event_loop(tick_sec=LOOP_WATCH_TICK_SEC, warn_after_sec=LOOP_STALL_WARN_SEC,
                         every_sec=LOOP_STALL_LOG_EVERY_SEC), name="copytrader-loop-watchdog")
    stop_watch = (asyncio.get_running_loop().create_task(
        watch_stop_request(stop_file, interrupts.request_stop, poll_sec=STOP_POLL_SEC), name="copytrader-stop-request")
        if stop_file is not None else None)
    listen_task: asyncio.Task[Any] | None = None
    stop_task: asyncio.Task[Any] | None = None
    early_start: asyncio.Task[Any] | None = None
    failure: BaseException | None = None
    try:
        await app.db.open()
        await app.notifier.start(app.engine)
        for err in app.broker_errors:
            if err.split(":", 1)[0] in app.engine.optional_down:
                await _safe(app.notifier.notify(
                    f"⚠️ İsteğe bağlı broker oluşturulamadı: {_esc(err)}\nBot ve diğer hesaplar normal çalışıyor; "
                    "bu hesap için sinyaller atlanacak.", Severity.WARNING))
            else:
                await app.notifier.notify(f"Broker oluşturulamadı: {_esc(err)}", Severity.ERROR)
        hold = getattr(app.listener, "hold_batches", None)
        release = getattr(app.listener, "release_batches", None)
        if callable(hold) and callable(release):
            # Listen and catch up WHILE the brokers connect (that can take minutes): the age of a message
            # is then judged against the time the bot was really away, not against the end of a slow
            # broker connect. The listener stores the messages and holds its batches and edits until the
            # engine is ready.
            hold()
            early_start = asyncio.ensure_future(app.listener.start())
            await app.engine.start()
            await early_start
            release()
        else:
            await app.engine.start()
            await app.listener.start()
        logger.info("Copy trader running (%s)", "DRY-RUN" if app.config.dry_run else "LIVE")
        listen_task = asyncio.ensure_future(app.listener.run_until_disconnected())
        stop_task = asyncio.ensure_future(stop.wait())
        done, _ = await asyncio.wait({listen_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if listen_task in done and not listen_task.cancelled() and listen_task.exception() is not None:
            failure = listen_task.exception()
            assert failure is not None
            await _safe(app.notifier.notify(f"Telegram dinleyicisi durdu: <code>{_esc(failure)}</code>",
                                            Severity.ERROR))
    except BaseException as exc:
        failure = exc
        if isinstance(exc, asyncio.CancelledError):
            _withdraw_cancel_requests()
        if not isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)):
            logger.exception("Startup/run failed")
            await _safe(app.notifier.notify(f"Bot durdu: <code>{_esc(exc)}</code>", Severity.ERROR))
    finally:
        try:
            for task in (stop_watch, stop_task, listen_task, early_start):
                if task is not None and not task.done():
                    task.cancel()
            if early_start is not None:
                await asyncio.gather(early_start, return_exceptions=True)
            # 1. No new work anywhere: new batches, edits, approvals and /kapat are refused from now on
            #    (an owner tap gets "bot kapanıyor"; the confirmation stays pending for after the restart).
            begin_shutdown = getattr(app.engine, "begin_shutdown", None)
            if begin_shutdown is not None:
                with contextlib.suppress(Exception):
                    begin_shutdown()
            # 2. Stop the Telegram listener (waits for its running handlers).
            await _safe(app.listener.stop())
            if listen_task is not None:
                await asyncio.gather(listen_task, return_exceptions=True)
            # 3. The engine waits for running order flows (entry -> stop -> TPs, also those started by an owner
            #    tap in a notifier task) and the monitor's current pass, THEN closes the brokers. The notifier
            #    stays connected until after this, so errors from those flows still reach the owner.
            await _safe(app.engine.stop())            # drain, timers, monitor, brokers
            await _safe(app.notifier.notify("🛑 Kopya-trade botu durduruldu.", Severity.INFO))
            await _safe(app.notifier.stop())
            aclose = getattr(app.parser, "aclose", None)
            if aclose is not None:
                await _safe(aclose())
        finally:
            # Always, also when a step above was interrupted: an open database keeps a worker thread
            # alive and the process would never exit.
            try:
                await _close_db(app.db)
            finally:
                watchdog.cancel()
                interrupts.restore()
                logger.info("Shutdown complete")
    if failure is not None:
        raise failure
    return interrupts.requested


def _withdraw_cancel_requests() -> None:
    """serve() was cancelled and answers by shutting down: on Windows that is how the first Ctrl+C arrives
    (asyncio.run() cancels the main task). The shutdown runs in this same task, and code in it tells a real
    cancellation from a library's stray CancelledError by ``Task.cancelling()`` (engine.prices.stray_cancel and
    its users, the MetaApi adapter's close): with the request still counted, an SDK's stray CancelledError
    would pass for a new cancellation and end the shutdown half-way (no "durduruldu" notice, the notifier's
    queue not sent, brokers after that one not closed). The request is therefore withdrawn here, as when the
    shutdown starts from the stop event; serve() still re-raises the CancelledError at the end, and a new
    cancellation during the shutdown is counted again and goes on."""
    task = asyncio.current_task()
    cancelling = getattr(task, "cancelling", None)
    uncancel = getattr(task, "uncancel", None)
    if cancelling is None or uncancel is None:
        return
    withdrawn = 0
    while cancelling() > 0:
        uncancel()
        withdrawn += 1
    if withdrawn:
        logger.info("Cancelled (Ctrl+C on Windows or a cancelled main task): shutting down")


async def _close_db(db: Any) -> None:
    try:
        await db.close()
    except BaseException as exc:         # also a second cancellation: at least stop its worker thread
        logger.error("Closing the database failed: %s", str(exc) or type(exc).__name__)
        close_nowait = getattr(db, "close_nowait", None)
        if close_nowait is not None:
            with contextlib.suppress(Exception):
                close_nowait()
        if not isinstance(exc, Exception):
            raise


async def _safe(awaitable: Any) -> None:
    try:
        await awaitable
    except Exception:
        logger.exception("Shutdown step failed")


def _esc(x: object) -> str:
    from .telegram.formatting import esc

    return esc(x)


async def run(config_path: str | os.PathLike[str] = "config.yaml") -> bool:
    """Entry point of ``python -m copytrader run``. True: it was stopped by ``python -m copytrader stop``."""
    config = load_config(config_path)
    log_path = setup_logging(config.log_level, config.data_dir, secrets=config_secrets(config))
    logger.info("Config %s loaded; mode %s; log file %s", config_path,
                "DRY-RUN" if config.dry_run else "LIVE", log_path)
    if not config.dry_run:
        logger.warning("LIVE MODE: real orders will be sent to %s", [b.name for b in config.enabled_brokers])
    return await serve(build_app(config), stop_file=stop_request_path(config.data_dir))


__all__ = ["App", "STOP_REQUEST_FILE", "build_app", "clear_stop_request", "config_secrets", "make_parser",
           "mask_secrets", "parser_deadline_sec", "read_stop_request", "run", "serve", "setup_logging",
           "stop_request_path", "watch_event_loop", "watch_stop_request", "write_stop_request"]
