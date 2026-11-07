"""App wiring and CLI tests (no network: fakes for Telegram, Claude and brokers)."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import pytest
import yaml

import copytrader.__main__ as cli
from copytrader.app import _HANDLER_TAG, build_app, serve, setup_logging
from copytrader.config import AppConfig, load_config
from copytrader.models import ActionType, EntryType, MarketType, ParsedAction, ParseResult, Severity, Side, TakeProfit
from copytrader.storage.db import Database

from fakes import FakeBroker, FakeNotifier, FakeParser, FakePriceOracle

IMAGE = str(Path(__file__).resolve().parents[1] / "examples" / "eth_long_tradingview.jpg")
GOOD_TOKEN = "123456789:" + "A" * 35


def write_config(tmp_path: Path, **overrides: Any) -> Path:
    data: dict[str, Any] = {
        "dry_run": True,
        "data_dir": str(tmp_path / "data"),
        "telegram": {"api_id": 1, "api_hash": "hash", "session_string": "fake-session",
                     "source_chats": [-1001234567890]},
        "notifier": {"bot_token": GOOD_TOKEN, "owner_user_id": 42},
        "brokers": [{"type": "paper", "name": "paper1", "starting_balance": 10000}],
    }
    data.update(overrides)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def eth_result(ids: list[int]) -> ParseResult:
    return ParseResult(actions=[ParsedAction(
        action=ActionType.OPEN, symbol="ETHUSDT.P", base_asset="ETH", quote_asset="USDT", market_type=MarketType.PERP,
        side=Side.LONG, entry_type=EntryType.MARKET, entry_price=2232.18, stop_loss=2205.11,
        take_profits=[TakeProfit(price=2310.6)], risk_r=1.0, used_message_ids=ids, confidence=0.95,
        summary_tr="ETH LONG, giriş 2232.18, SL 2205.11, TP 2310.60, risk 1R",
    )])


@pytest.fixture
def clean_logging():
    yield
    root = logging.getLogger()
    for h in list(root.handlers):
        if getattr(h, _HANDLER_TAG, False):
            root.removeHandler(h)
            h.close()


# --------------------------------------------------------------------------- #
# argparse
# --------------------------------------------------------------------------- #
def test_cli_commands_and_default_config() -> None:
    p = cli.build_parser()
    for cmd in ("login", "chats", "run", "check"):
        args = p.parse_args([cmd])
        assert args.command == cmd and args.config == "config.yaml"
    args = p.parse_args(["parse", "--text", "eth 1R long", "--image", "a.jpg", "--image", "b.jpg", "--config", "x.yaml"])
    assert args.text == "eth 1R long" and args.image == ["a.jpg", "b.jpg"] and args.config == "x.yaml"
    assert p.parse_args(["chats", "--limit", "5"]).limit == 5
    with pytest.raises(SystemExit):
        p.parse_args([])


def test_missing_config_file(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["check", "--config", str(tmp_path / "nope.yaml")]) == cli.EXIT_CONFIG
    assert "bulunamadı" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# logging
# --------------------------------------------------------------------------- #
def test_setup_logging_console_and_rotating_file(tmp_path: Path, clean_logging: None) -> None:
    path = setup_logging("DEBUG", tmp_path)
    setup_logging("DEBUG", tmp_path)     # idempotent
    assert path == tmp_path / "logs" / "copytrader.log"
    tagged = [h for h in logging.getLogger().handlers if getattr(h, _HANDLER_TAG, False)]
    assert len(tagged) == 2
    logging.getLogger("copytrader.test").info("merhaba günlük")
    for h in tagged:
        h.flush()
    assert "merhaba günlük" in path.read_text(encoding="utf-8")
    assert logging.getLogger("telethon").level >= logging.WARNING


# --------------------------------------------------------------------------- #
# check
# --------------------------------------------------------------------------- #
def test_check_reports_paper_equity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                    capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-" + "x" * 40)
    code = cli.main(["check", "--config", str(write_config(tmp_path))])
    out = capsys.readouterr().out
    assert code == cli.EXIT_OK, out
    assert "DRY-RUN" in out and "paper1" in out and "10,000.00 USDT" in out and "Sorun yok" in out


def test_check_flags_missing_key_and_bad_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                               capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    path = write_config(tmp_path, notifier={"bot_token": "abc", "owner_user_id": 42})
    assert cli.main(["check", "--config", str(path)]) == cli.EXIT_PROBLEM
    out = capsys.readouterr().out
    assert out.count("YOK") >= 2 and "2 sorun" in out


# --------------------------------------------------------------------------- #
# parse
# --------------------------------------------------------------------------- #
def test_parse_command_previews_without_trading(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                capsys: pytest.CaptureFixture[str]) -> None:
    parser = FakeParser(eth_result([1, 2]))
    broker = FakeBroker("demo", equity=10_000.0)
    monkeypatch.setattr(cli, "_make_parser", lambda config: parser)
    monkeypatch.setattr(cli, "_make_oracle", lambda config, brokers: FakePriceOracle({"ETH": 2232.85}))
    monkeypatch.setattr(cli, "_make_brokers", lambda config: ([broker], []))
    path = write_config(tmp_path)
    code = cli.main(["parse", "--config", str(path), "--text", "eth 1R long", "--image", IMAGE])
    out = capsys.readouterr().out
    assert code == cli.EXIT_OK, out
    msgs, _ctx = parser.calls[0]
    assert [m.message_id for m in msgs] == [1, 2] and msgs[0].media_path == IMAGE and msgs[1].text == "eth 1R long"
    assert "Otomatik uygulanırdı" in out and "2205.11" in out and "<b>" not in out
    assert "demo:" in out and "risk 200.00 USDT" in out
    assert broker.opened == [] and broker.connected and broker.closed
    assert not (tmp_path / "data" / "copytrader.db").exists()      # read-only: no DB created


def test_parse_command_rejects_missing_image(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code = cli.main(["parse", "--config", str(write_config(tmp_path)), "--image", str(tmp_path / "x.jpg")])
    assert code == cli.EXIT_CONFIG and "bulunamadı" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# app wiring
# --------------------------------------------------------------------------- #
class FakeListener:
    instances: list["FakeListener"] = []

    def __init__(self, config: Any, db: Any, data_dir: str, *, on_ready: Any, on_edit: Any,
                 fail: BaseException | None = None) -> None:
        self.config, self.db, self.data_dir = config, db, data_dir
        self.on_ready, self.on_edit = on_ready, on_edit
        self.fail = fail
        self.started = asyncio.Event()
        self._stop = asyncio.Event()
        self.stopped = False
        FakeListener.instances.append(self)

    async def start(self) -> None:
        self.started.set()

    async def run_until_disconnected(self) -> None:
        if self.fail is not None:
            raise self.fail
        await self._stop.wait()

    async def stop(self) -> None:
        self.stopped = True
        self._stop.set()


def _app(tmp_path: Path, notifier: FakeNotifier, listener_kw: dict[str, Any] | None = None):
    config = load_config(write_config(tmp_path))
    brokers: list[FakeBroker] = []

    def broker_factory(cfg: Any) -> FakeBroker:
        brokers.append(FakeBroker(cfg.name))
        return brokers[-1]

    app = build_app(
        config, db=Database(":memory:"), parser=FakeParser(), notifier=notifier, broker_factory=broker_factory,
        listener_factory=lambda *a, **kw: FakeListener(*a, **kw, **(listener_kw or {})),
    )
    return app, brokers


async def test_serve_starts_everything_and_shuts_down_in_order(tmp_path: Path) -> None:
    notifier = FakeNotifier()
    app, brokers = _app(tmp_path, notifier)
    listener: FakeListener = app.listener
    assert listener.on_ready == app.engine.on_messages_ready and listener.on_edit == app.engine.on_message_edited
    stop = asyncio.Event()
    task = asyncio.ensure_future(serve(app, stop))
    await asyncio.wait_for(listener.started.wait(), 2)
    assert notifier.engine is app.engine
    assert [b.name for b in app.engine.brokers] == ["paper1"] and brokers[0].connected
    assert any("Kopya-trade motoru başladı" in t for t in notifier.texts(Severity.INFO))
    stop.set()
    await asyncio.wait_for(task, 5)
    assert listener.stopped and brokers[0].closed
    assert any("durduruldu" in t for t in notifier.texts(Severity.INFO))
    assert app.db._conn is None        # closed


async def test_serve_reports_listener_failure(tmp_path: Path) -> None:
    notifier = FakeNotifier()
    app, brokers = _app(tmp_path, notifier, {"fail": RuntimeError("oturum iptal edildi")})
    with pytest.raises(RuntimeError, match="oturum iptal"):
        await asyncio.wait_for(serve(app), 5)
    assert any("oturum iptal edildi" in t for t in notifier.texts(Severity.ERROR))
    assert brokers[0].closed and app.listener.stopped


def test_build_app_with_real_components(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-" + "x" * 40)
    config: AppConfig = load_config(write_config(tmp_path))
    app = build_app(config)
    from copytrader.brokers.paper_broker import PaperBroker
    from copytrader.parsing.llm_parser import ClaudeSignalParser
    from copytrader.storage.db import Database as Db
    from copytrader.telegram.listener import TelegramListener
    from copytrader.telegram.notifier import BotNotifier

    assert isinstance(app.db, Db) and app.db.path == str(Path(config.data_dir) / "copytrader.db")
    assert isinstance(app.parser, ClaudeSignalParser)
    assert isinstance(app.notifier, BotNotifier) and isinstance(app.listener, TelegramListener)
    assert [type(b) for b in app.brokers] == [PaperBroker]
    assert app.engine.brokers == app.brokers and app.broker_errors == []
