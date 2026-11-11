"""Application wiring: build every component from the config and run until stopped."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from collections.abc import Callable
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
_NOISY_LOGGERS = ("telethon", "aiosqlite", "asyncio", "httpx", "httpcore", "anthropic", "ccxt", "urllib3", "metaapi")
LOG_FILE_MAX_BYTES = 10 * 1024 * 1024
LOG_FILE_BACKUPS = 5
MIN_PARSE_DEADLINE_SEC = 60.0      # a slow (image + high effort) reading must still fit


def setup_logging(level: str = "INFO", data_dir: str | os.PathLike[str] | None = None) -> Path | None:
    """Console + rotating file (``<data_dir>/logs/copytrader.log``). Idempotent. Returns the log file path."""
    lvl = logging.getLevelName(str(level).upper())
    if not isinstance(lvl, int):
        lvl = logging.INFO
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, _HANDLER_TAG, False):
            root.removeHandler(handler)
            handler.close()
    formatter = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(formatter)
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
        setattr(file_handler, _HANDLER_TAG, True)
        root.addHandler(file_handler)
    root.setLevel(lvl)
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(max(lvl, logging.WARNING))
    return log_path


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
    return App(config, db, parser, brokers, notifier, engine, listener, errors)


def _install_signal_handlers(stop: asyncio.Event) -> list[int]:
    """SIGINT/SIGTERM -> graceful stop (first signal only; a second Ctrl+C interrupts hard)."""
    loop = asyncio.get_running_loop()
    installed: list[int] = []

    def handler(signum: int) -> None:
        logger.warning("Signal %s received: shutting down", signum)
        stop.set()
        for s in installed:
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(s)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handler, int(sig))
            installed.append(int(sig))
        except (NotImplementedError, RuntimeError, ValueError):
            pass  # Windows: Ctrl+C arrives as KeyboardInterrupt / task cancellation
    return installed


async def serve(app: App, stop: asyncio.Event | None = None) -> None:
    """Start everything, run until the listener ends or ``stop`` is set, then shut down in order."""
    stop = stop or asyncio.Event()
    installed = _install_signal_handlers(stop)
    listen_task: asyncio.Task[Any] | None = None
    stop_task: asyncio.Task[Any] | None = None
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
        if not isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)):
            logger.exception("Startup/run failed")
            await _safe(app.notifier.notify(f"Bot durdu: <code>{_esc(exc)}</code>", Severity.ERROR))
    finally:
        loop = asyncio.get_running_loop()
        for s in installed:
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(s)
        for task in (stop_task, listen_task):
            if task is not None and not task.done():
                task.cancel()
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
        await _safe(app.db.close())
        logger.info("Shutdown complete")
    if failure is not None:
        raise failure


async def _safe(awaitable: Any) -> None:
    try:
        await awaitable
    except Exception:
        logger.exception("Shutdown step failed")


def _esc(x: object) -> str:
    from .telegram.formatting import esc

    return esc(x)


async def run(config_path: str | os.PathLike[str] = "config.yaml") -> None:
    """Entry point of ``python -m copytrader run``."""
    config = load_config(config_path)
    log_path = setup_logging(config.log_level, config.data_dir)
    logger.info("Config %s loaded; mode %s; log file %s", config_path,
                "DRY-RUN" if config.dry_run else "LIVE", log_path)
    if not config.dry_run:
        logger.warning("LIVE MODE: real orders will be sent to %s", [b.name for b in config.enabled_brokers])
    await serve(build_app(config))


__all__ = ["App", "build_app", "make_parser", "parser_deadline_sec", "run", "serve", "setup_logging"]
