"""Regression tests: config.timezone is validated at load (F17); empty allowed_sender_ids is flagged (F8)."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

import copytrader.__main__ as cli
from copytrader.config import load_config
from copytrader.engine.orchestrator import Engine
from copytrader.models import Severity
from copytrader.storage.db import Database

from fakes import FakeBroker, FakeNotifier, FakeParser, FakePriceOracle
from test_app_cli import write_config
from test_engine import SENDER, db, make_config  # noqa: F401  (fixture)


def test_invalid_timezone_is_rejected_at_load(tmp_path: Path) -> None:
    with pytest.raises(ValidationError) as info:
        load_config(write_config(tmp_path, timezone="Istanbul"))
    text = str(info.value)
    assert "geçersiz saat dilimi" in text and "Europe/Istanbul" in text


def test_valid_timezones_load(tmp_path: Path) -> None:
    assert load_config(write_config(tmp_path, timezone="Europe/Istanbul")).timezone == "Europe/Istanbul"
    assert load_config(write_config(tmp_path, timezone="UTC")).timezone == "UTC"


def test_check_command_reports_the_bad_timezone(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["check", "--config", str(write_config(tmp_path, timezone="Istanbul"))]) == cli.EXIT_CONFIG
    assert "geçersiz saat dilimi" in capsys.readouterr().out


async def start_engine(database: Database, allowed: list[int]) -> FakeNotifier:
    cfg = make_config()
    cfg.telegram.allowed_sender_ids = allowed
    notifier = FakeNotifier()
    engine = Engine(cfg, database, FakeParser(), [FakeBroker("binance")], notifier, price_oracle=FakePriceOracle())
    await engine.start(monitor=False)
    await engine.stop()
    return notifier


def open_sender_warnings(notifier: FakeNotifier) -> list[str]:
    return [t for t in notifier.texts(Severity.WARNING) if "allowed_sender_ids" in t]


async def test_empty_allowed_senders_warns_the_owner_once(db: Database, caplog: pytest.LogCaptureFixture) -> None:
    first = await start_engine(db, [])
    warnings = open_sender_warnings(first)
    assert len(warnings) == 1 and "HERKESİN" in warnings[0]
    second = await start_engine(db, [])                 # restart: logged again, not sent again
    assert open_sender_warnings(second) == []
    assert "allowed_sender_ids is empty" in caplog.text


async def test_allowed_senders_set_means_no_warning(db: Database) -> None:
    assert open_sender_warnings(await start_engine(db, [SENDER])) == []
