"""Owner tooling of the pre-flight batch (no network, no real venue, no real Telegram):

T1  guncelle.py                the updater (and the one line of docs/GUNCELLEME.md that starts it)
T2  copytrader/migrate.py      ``python -m copytrader migrate``
T3  copytrader/selftest.py     ``python -m copytrader selftest`` (read-only health report)
T4  copytrader/livetest.py     ``python -m copytrader canli-test`` (the only command that trades; fakes / paper here)
T5  bot.lock                   ``run`` holds it; chats / login / selftest / canli-test refuse while the bot runs

The new modules are imported inside the tests, so that every test fails on its own where a module is missing.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import textwrap
import time
import urllib.parse
import zipfile
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

import copytrader.__main__ as cli
from copytrader.brokers.base import BrokerError
from copytrader.config import AppConfig, _BrokerBase, load_config
from copytrader.models import (
    BrokerPosition,
    EntryType,
    MarketType,
    PositionStatus,
    Quote,
    Side,
    SizingResult,
    SymbolInfo,
    TradePlan,
)

from fakes import FakeBroker, FakePriceOracle

REPO = Path(__file__).resolve().parents[1]
POSIX_PROC = os.name == "posix" and Path("/proc/self/stat").is_file()

# Reduced copy of the owner's production config.yaml (placeholders as in production; dummy values in ENV).
PROD_YAML = """\
# KLAN - canlı ayarlar (yorum satırı korunmalı)
dry_run: false
data_dir: data
timezone: Europe/Istanbul
telegram:
  api_id: ${TG_API_ID}
  api_hash: ${TG_API_HASH}
  source_chats: [-1003782294463]
  topics:
    - name: kripto
      title_contains: ["Kripto İşlemleri"]
      brokers: [binance, breakout, tickmill]
      kind: crypto
    - name: fx
      title_contains: ["Fx İşlemleri"]
      brokers: [fx, tickmill]
      kind: fx
notifier:
  bot_token: ${TG_BOT_TOKEN}
  owner_user_id: ${TG_OWNER_ID}
llm:
  api_key: ${ANTHROPIC_API_KEY}
brokers:
  - name: binance
    type: ccxt
    exchange: binanceusdm
    api_key: ${BINANCE_API_KEY}
    secret: ${BINANCE_API_SECRET}
    risk_usd: 10
    margin_mode: cross
    leverage: 125
    max_leverage: 125
  - name: breakout
    type: manual
    risk_usd: 50
    allowed_assets: [BTC, ETH, SOL, XRP, DOGE]
  - name: fx
    type: mt5
    login: ${MT5_LOGIN}
    password: ${MT5_PASSWORD}
    server: ${MT5_SERVER}
    risk_usd: 100
    no_hedge: true
    daily_loss_limit_pct: 3
  - name: tickmill
    type: metaapi
    enabled: true
    optional: true
    token: ${METAAPI_TOKEN:-}
    account_id: ${METAAPI_ACCOUNT_ID:-}
    risk_usd: 45
    reference_exchange: binanceusdm
    offset_mode: additive
    max_offset_pct: 1.0
    entry_trigger: reference_touch
    virtual_trigger_max_hours: 48
    symbol_map:
      BTC: BTCUSD
      ETH: ETHUSD
    allowed_assets: [EUR, GBP, USD, AUD, NZD, BTC, ETH, XAU, XAG]
    no_hedge: true
    daily_loss_limit_pct: 0
"""
ENV = {
    "TG_API_ID": "123456", "TG_API_HASH": "hash-SECRET-1", "TG_BOT_TOKEN": "123456789:" + "A" * 35,
    "TG_OWNER_ID": "42", "ANTHROPIC_API_KEY": "sk-ant-SECRET-" + "x" * 30, "BINANCE_API_KEY": "binance-KEY-SECRET",
    "BINANCE_API_SECRET": "binance-SECRET-2", "MT5_LOGIN": "5550001", "MT5_PASSWORD": "mt5-SECRET-3",
    "MT5_SERVER": "FundingPips-SIM", "METAAPI_TOKEN": "metaapi-SECRET-4", "METAAPI_ACCOUNT_ID": "acc-1",
}
SECRETS = [v for v in ENV.values() if "SECRET" in v]
KNOWS_OPEN_RISK = "daily_loss_include_open_risk" in _BrokerBase.model_fields
# Settings the production replica needs: binance x2, notifier x2, risk x2, the kripto topic (+ fx open risk).
N_CHANGES = 8 if KNOWS_OPEN_RISK else 7
PLACEHOLDER = re.compile(r"\$\{[^}]*\}")


@pytest.fixture(autouse=True)
def _keep_environment():
    """load_config() copies .env values into os.environ: undo that after every test."""
    saved = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(saved)


def write_env(folder: Path) -> None:
    (folder / ".env").write_text("".join(f"{k}={v}\n" for k, v in ENV.items()), encoding="utf-8")


def prod_file(tmp_path: Path, text: str = PROD_YAML, raw: bytes | None = None) -> Path:
    write_env(tmp_path)
    path = tmp_path / "config.yaml"
    path.write_bytes(raw if raw is not None else text.encode("utf-8"))
    return path


def is_subsequence(part: list[str], whole: list[str]) -> bool:
    it = iter(whole)
    return all(any(line == other for other in it) for line in part)


def broker_of(cfg: AppConfig, name: str) -> Any:
    return next(b for b in cfg.brokers if b.name == name)


# =========================================================================== #
# T2  migrate
# =========================================================================== #
def migrate_mod() -> Any:
    return importlib.import_module("copytrader.migrate")


def test_migrate_prod_replica_changes_only_the_wanted_keys(tmp_path: Path) -> None:
    m = migrate_mod()
    path = prod_file(tmp_path)
    notes: list[str] = []
    lines = m.migrate_config(path, notes=notes, now=datetime(2026, 11, 12, 6, 0, 0))

    assert lines[0] == "binance: günlük zarar freni kapatıldı (daily_loss_limit_pct: 0)"
    assert lines[1] == "binance: aynı coinde açık işlem varken ters yönlü sinyal atlanacak (no_hedge: true)"
    joined = "\n".join(lines)
    for key in ("notify_on_execution: true", "notify_on_ignored: true", "max_open_trades: 25",
                "allow_min_qty_round_up: true", "explicit_market_drift: follow"):
        assert key in joined
    assert ("daily_loss_include_open_risk: true" in joined) is KNOWS_OPEN_RISK
    assert len(lines) == N_CHANGES

    new_text = path.read_text(encoding="utf-8")
    # the file is the old text plus new lines: every old line is still there, in order, comments included
    assert is_subsequence(PROD_YAML.splitlines(), new_text.splitlines())
    # (a multiset difference: "    daily_loss_limit_pct: 0" and "    no_hedge: true" are also lines of the old file)
    added = list((Counter(new_text.splitlines()) - Counter(PROD_YAML.splitlines())).elements())
    assert sorted(a.strip() for a in added) == sorted(
        ["explicit_market_drift: follow", "notify_on_execution: true", "notify_on_ignored: true",
         "daily_loss_limit_pct: 0", "no_hedge: true", "risk:", "max_open_trades: 25", "allow_min_qty_round_up: true"]
        + (["daily_loss_include_open_risk: true"] if KNOWS_OPEN_RISK else []))
    # the two binance settings sit at the end of the binance block, in the block's own indentation
    assert "    max_leverage: 125\n    daily_loss_limit_pct: 0\n    no_hedge: true\n  - name: breakout\n" in new_text
    # placeholders byte for byte, no secret written
    assert PLACEHOLDER.findall(new_text) == PLACEHOLDER.findall(PROD_YAML)
    assert "${METAAPI_TOKEN:-}" in new_text and "${METAAPI_ACCOUNT_ID:-}" in new_text
    assert not any(secret in new_text for secret in SECRETS)
    # backup first, byte for byte
    backup = tmp_path / "config.yaml.yedek-20261112-060000"
    assert backup.read_bytes() == PROD_YAML.encode("utf-8")
    assert any("config.yaml.yedek-20261112-060000" in n for n in notes)

    cfg = load_config(path)                       # the app's real loader
    assert broker_of(cfg, "binance").daily_loss_limit_pct == 0 and broker_of(cfg, "binance").no_hedge is True
    assert broker_of(cfg, "breakout").no_hedge is False                   # only binance gets it
    assert broker_of(cfg, "fx").daily_loss_limit_pct == 3 and broker_of(cfg, "tickmill").daily_loss_limit_pct == 0
    assert cfg.notifier.notify_on_execution is True and cfg.notifier.notify_on_ignored is True
    assert cfg.risk.max_open_trades == 25 and cfg.risk.allow_min_qty_round_up is True
    assert cfg.risk.risk_per_r_pct == 2.0         # the new risk section carries nothing else
    kripto, fx = cfg.telegram.topics
    assert kripto.explicit_market_drift == "follow" and fx.explicit_market_drift is None
    if KNOWS_OPEN_RISK:
        assert broker_of(cfg, "fx").daily_loss_include_open_risk is True
        assert broker_of(cfg, "tickmill").daily_loss_include_open_risk is False


def test_migrate_second_run_changes_nothing(tmp_path: Path) -> None:
    m = migrate_mod()
    path = prod_file(tmp_path)
    assert m.migrate_config(path)
    after_first = path.read_bytes()
    files = sorted(p.name for p in tmp_path.iterdir())
    assert m.migrate_config(path) == []
    assert path.read_bytes() == after_first
    assert sorted(p.name for p in tmp_path.iterdir()) == files          # no second backup either


def test_migrate_existing_keys_with_other_values_are_changed_in_place(tmp_path: Path) -> None:
    m = migrate_mod()
    text = PROD_YAML.replace("    max_leverage: 125\n", "    max_leverage: 125\n    daily_loss_limit_pct: 10   # eski değer\n"
                                                        "    no_hedge: false\n")
    text = text.replace("  owner_user_id: ${TG_OWNER_ID}\n",
                        "  owner_user_id: ${TG_OWNER_ID}\n  notify_on_execution: false    # her işlemi bildir\n"
                        "  notify_on_ignored: no\n")
    text = text.replace("      kind: crypto\n", "      kind: crypto\n      explicit_market_drift: ask\n")
    text += "risk:\n  risk_per_r_pct: 2.0\n  max_open_trades: 10 # eski\n  allow_min_qty_round_up: false\n"
    path = prod_file(tmp_path, text)
    lines = m.migrate_config(path)
    assert len(lines) == N_CHANGES
    assert "10 → 25" in "\n".join(lines)
    new = path.read_text(encoding="utf-8")
    assert "    daily_loss_limit_pct: 0   # eski değer\n    no_hedge: true\n  - name: breakout\n" in new   # value replaced, comment kept
    assert "  notify_on_execution: true    # her işlemi bildir\n" in new and "  notify_on_ignored: true\n" in new
    assert "  max_open_trades: 25 # eski\n" in new and "  allow_min_qty_round_up: true\n" in new
    assert "      explicit_market_drift: follow\n" in new
    assert len(new.splitlines()) == len(text.splitlines()) + (1 if KNOWS_OPEN_RISK else 0)   # nothing else added
    assert new.count("\nrisk:") == 1                                      # the existing section was used, no second one


def test_migrate_leaves_alone_what_is_already_as_wanted(tmp_path: Path) -> None:
    m = migrate_mod()
    text = PROD_YAML.replace("    max_leverage: 125\n", "    max_leverage: 125\n    daily_loss_limit_pct: 0.0\n    no_hedge: yes\n")
    text = text.replace("  owner_user_id: ${TG_OWNER_ID}\n",
                        "  owner_user_id: ${TG_OWNER_ID}\n  notify_on_execution: true\n  notify_on_ignored: true\n")
    text = text.replace("      kind: crypto\n", "      kind: crypto\n      explicit_market_drift: follow\n")
    text = text.replace("    daily_loss_limit_pct: 3\n", "    daily_loss_limit_pct: 3\n    daily_loss_include_open_risk: true\n"
                        if KNOWS_OPEN_RISK else "    daily_loss_limit_pct: 3\n")
    text += "risk:\n  max_open_trades: 40\n  allow_min_qty_round_up: true\n"
    path = prod_file(tmp_path, text)
    assert m.migrate_config(path) == []
    assert path.read_text(encoding="utf-8") == text
    assert load_config(path).risk.max_open_trades == 40                  # a higher value is not lowered


def test_migrate_config_without_fx_and_binance_brokers(tmp_path: Path) -> None:
    m = migrate_mod()
    data = yaml.safe_load(PROD_YAML)
    data["brokers"] = [b for b in data["brokers"] if b["name"] not in ("fx", "binance")]
    data["telegram"]["topics"] = [{"name": "kripto", "title_contains": ["Kripto"], "brokers": ["breakout", "tickmill"],
                                   "kind": "crypto"}]
    path = prod_file(tmp_path, yaml.safe_dump(data, sort_keys=False, allow_unicode=True))
    notes: list[str] = []
    lines = m.migrate_config(path, notes=notes)
    assert len(lines) == 5 and not any(line.startswith(("binance", "fx")) for line in lines)
    assert any("'fx' adlı broker yok" in n for n in notes) and any("'binance' adlı broker yok" in n for n in notes)
    cfg = load_config(path)
    assert cfg.notifier.notify_on_execution and cfg.risk.max_open_trades == 25
    assert cfg.telegram.topics[0].explicit_market_drift == "follow"
    assert "daily_loss_include_open_risk" not in path.read_text(encoding="utf-8")


def test_migrate_skips_a_key_the_installed_config_model_does_not_know(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    m = migrate_mod()
    real = m._knows
    monkeypatch.setattr(m, "_knows", lambda model, key: key != "daily_loss_include_open_risk" and real(model, key))
    path = prod_file(tmp_path)
    notes: list[str] = []
    lines = m.migrate_config(path, notes=notes)
    assert len(lines) == 7 and "daily_loss_include_open_risk" not in path.read_text(encoding="utf-8")
    assert any("daily_loss_include_open_risk" in n and "tanımıyor" in n for n in notes)
    load_config(path)                                                    # still a valid file


@pytest.mark.parametrize("variant", ["crlf", "bom", "no_final_newline", "indent4"])
def test_migrate_keeps_line_endings_bom_and_indentation(tmp_path: Path, variant: str) -> None:
    m = migrate_mod()
    raw = {
        "crlf": PROD_YAML.replace("\n", "\r\n").encode("utf-8"),
        "bom": b"\xef\xbb\xbf" + PROD_YAML.encode("utf-8"),
        "no_final_newline": PROD_YAML.rstrip("\n").encode("utf-8"),
        "indent4": PROD_YAML.replace("\n  ", "\n    ").encode("utf-8"),
    }[variant]
    path = prod_file(tmp_path, raw=raw)
    assert len(m.migrate_config(path)) == N_CHANGES
    out = path.read_bytes()
    if variant == "crlf":
        assert b"\n" not in out.replace(b"\r\n", b"")                    # every line break is still CRLF
        assert b"    daily_loss_limit_pct: 0\r\n" in out
    if variant == "bom":
        assert out.startswith(b"\xef\xbb\xbf")
    if variant == "indent4":
        assert b"\n    notify_on_execution: true\n" in out and b"\nrisk:\n    max_open_trades: 25\n" in out
    assert is_subsequence(raw.decode("utf-8").splitlines(), out.decode("utf-8").splitlines())
    assert load_config(path).risk.max_open_trades == 25
    assert m.migrate_config(path) == []


def test_migrate_inline_sections_and_empty_values(tmp_path: Path) -> None:
    m = migrate_mod()
    text = PROD_YAML.replace("notifier:\n  bot_token: ${TG_BOT_TOKEN}\n  owner_user_id: ${TG_OWNER_ID}\n",
                             "notifier: {bot_token: '${TG_BOT_TOKEN}', owner_user_id: '${TG_OWNER_ID}'}\n")
    text = text.replace("    max_leverage: 125\n", "    max_leverage: 125\n    daily_loss_limit_pct:    # boş\n")
    text += "risk: {}\n"
    assert text.count("notifier: {") == 1
    path = prod_file(tmp_path, text)
    notes: list[str] = []
    assert len(m.migrate_config(path, notes=notes)) == N_CHANGES
    new = path.read_text(encoding="utf-8")
    assert ("notifier: {bot_token: '${TG_BOT_TOKEN}', owner_user_id: '${TG_OWNER_ID}', notify_on_execution: true, "
            "notify_on_ignored: true}\n") in new
    assert "    daily_loss_limit_pct: 0    # boş\n" in new
    assert "risk: {max_open_trades: 25, allow_min_qty_round_up: true}\n" in new
    assert not any("korunamadı" in n for n in notes)                     # still the text edit, not the rewrite
    assert load_config(path).notifier.notify_on_ignored is True


def test_migrate_rewrites_through_pyyaml_when_the_text_edit_does_not_verify(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    m = migrate_mod()
    # a text edit that silently damages another value: the verification must refuse it
    monkeypatch.setattr(m, "_edit_text", lambda text, changes: text.replace("risk_usd: 100", "risk_usd: 1000"))
    path = prod_file(tmp_path)
    notes: list[str] = []
    lines = m.migrate_config(path, notes=notes, now=datetime(2026, 11, 12, 6, 0, 0))
    assert len(lines) == N_CHANGES
    assert any("korunamadı" in n for n in notes)                         # the owner is told the comments are gone
    new = path.read_text(encoding="utf-8")
    assert "config.yaml.yedek-20261112-060000" in new.splitlines()[1]
    old, parsed = yaml.safe_load(PROD_YAML), yaml.safe_load(new)
    assert parsed["brokers"][2]["risk_usd"] == 100                       # the damaged edit was thrown away
    assert list(parsed)[:len(old)] == list(old)                          # key order kept, new section last
    assert [list(b) [:len(o)] for b, o in zip(parsed["brokers"], old["brokers"])] == [list(o) for o in old["brokers"]]
    assert sorted(PLACEHOLDER.findall(new)) == sorted(PLACEHOLDER.findall(PROD_YAML))
    assert parsed["brokers"][3]["token"] == "${METAAPI_TOKEN:-}" and not any(s in new for s in SECRETS)
    assert load_config(path).risk.max_open_trades == 25
    assert (tmp_path / "config.yaml.yedek-20261112-060000").read_bytes() == PROD_YAML.encode("utf-8")


def test_migrate_verification_catches_unplanned_differences() -> None:
    m = migrate_mod()
    data = yaml.safe_load(PROD_YAML)
    cfg = AppConfig.model_validate(yaml.safe_load(PROD_YAML.replace("${TG_API_ID}", "1").replace("${TG_OWNER_ID}", "1")
                                                  .replace("${MT5_LOGIN}", "1")))
    changes, _notes = m.plan_changes(data, cfg)
    good = m._edit_text(PROD_YAML, changes)
    assert m._verify(PROD_YAML, good, changes) is None
    assert m._verify(PROD_YAML, good.replace("risk_usd: 45", "risk_usd: 46"), changes)          # another value
    assert m._verify(PROD_YAML, good.replace("leverage: 125\n", "leverage: 125.0\n", 1), changes)  # a type
    assert m._verify(PROD_YAML, good.replace("${METAAPI_TOKEN:-}", "${METAAPI_TOKEN}"), changes)  # a placeholder
    assert m._verify(PROD_YAML, good.replace("    daily_loss_limit_pct: 0\n", "", 1), changes)   # a missing change
    swapped = good.replace("    risk_usd: 10\n    margin_mode: cross\n", "    margin_mode: cross\n    risk_usd: 10\n")
    assert swapped != good and m._verify(PROD_YAML, swapped, changes)                           # key order


def test_migrate_puts_the_old_file_back_when_the_new_one_does_not_load(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    m = migrate_mod()
    path = prod_file(tmp_path)
    real, calls = m.load_config, []

    def loader(p: Any) -> AppConfig:
        calls.append(p)
        if len(calls) > 1:
            raise ValueError("ortam değişkeni X ayarlanmamış")
        return real(p)

    monkeypatch.setattr(m, "load_config", loader)
    with pytest.raises(m.MigrationError, match="eski hali geri konuldu"):
        m.migrate_config(path, now=datetime(2026, 11, 12, 6, 0, 0))
    assert path.read_bytes() == PROD_YAML.encode("utf-8")
    assert (tmp_path / "config.yaml.yedek-20261112-060000").read_bytes() == PROD_YAML.encode("utf-8")
    assert not (tmp_path / "config.yaml.tmp").exists()


def test_migrate_refuses_a_file_the_app_cannot_load_and_never_quotes_values(tmp_path: Path) -> None:
    m = migrate_mod()
    bad = PROD_YAML.replace("    risk_usd: 10\n", "    risk_usd: 10\n    api_keyy: literal-SECRET-9\n")
    path = prod_file(tmp_path, bad)
    with pytest.raises(m.MigrationError) as err:
        m.migrate_config(path)
    assert "hiçbir şey değiştirilmedi" in str(err.value) and "SECRET" not in str(err.value)
    assert path.read_text(encoding="utf-8") == bad
    assert [p.name for p in tmp_path.iterdir() if "yedek" in p.name] == []


def test_migrate_does_not_touch_a_setting_that_comes_from_the_environment(tmp_path: Path) -> None:
    m = migrate_mod()
    text = PROD_YAML.replace("  owner_user_id: ${TG_OWNER_ID}\n",
                             "  owner_user_id: ${TG_OWNER_ID}\n  notify_on_execution: ${BILDIR:-false}\n")
    path = prod_file(tmp_path, text)
    notes: list[str] = []
    lines = m.migrate_config(path, notes=notes)
    assert "notify_on_execution: ${BILDIR:-false}\n" in path.read_text(encoding="utf-8")
    assert not any("notify_on_execution" in line for line in lines)
    assert any("notify_on_execution" in n and "dokunulmadı" in n for n in notes)


def test_migrate_command(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = prod_file(tmp_path)
    assert cli.main(["migrate", "--config", str(path)]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "• binance: günlük zarar freni kapatıldı (daily_loss_limit_pct: 0)" in out
    assert "• binance: aynı coinde açık işlem varken ters yönlü sinyal atlanacak (no_hedge: true)" in out
    assert f"{N_CHANGES} ayar güncellendi." in out and "eski dosyanın yedeği: config.yaml.yedek-" in out
    assert not any(secret in out for secret in SECRETS)
    assert cli.main(["migrate", "--config", str(path)]) == cli.EXIT_OK
    assert "zaten güncel" in capsys.readouterr().out
    path.write_text("telegram: [", encoding="utf-8")
    assert cli.main(["migrate", "--config", str(path)]) == cli.EXIT_CONFIG
    assert "güncellenemedi" in capsys.readouterr().out
    assert cli.main(["migrate", "--config", str(tmp_path / "yok.yaml")]) == cli.EXIT_CONFIG


# =========================================================================== #
# T5  bot.lock
# =========================================================================== #
def paper_config(tmp_path: Path, **extra: Any) -> Path:
    data: dict[str, Any] = {
        "dry_run": True, "data_dir": str(tmp_path / "data"),
        "telegram": {"api_id": 1, "api_hash": "h", "session_string": "fake-session", "source_chats": [-1001]},
        "notifier": {"bot_token": "123456789:" + "A" * 35, "owner_user_id": 42},
        "brokers": [{"type": "paper", "name": "sanal", "starting_balance": 1000}],
    }
    data.update(extra)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
    return path


@pytest.fixture
def sleeper():
    """A live process that is not us (stands in for a running bot)."""
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    yield proc
    proc.kill()
    proc.wait()


def write_lock(data_dir: Path, pid: int, token: str | None = "same", started: str | None = None) -> Path:
    """What a bot with this pid would have written (``started``: now, as a bot that is running did)."""
    data_dir.mkdir(parents=True, exist_ok=True)
    if token == "same":
        token = cli._process_state(pid)[1]
    path = data_dir / "bot.lock"
    started = started or datetime.now(UTC).isoformat(timespec="seconds")
    path.write_text(json.dumps({"pid": pid, "started": started, "token": token}), encoding="utf-8")
    return path


def test_run_holds_the_lock_while_the_bot_runs_and_removes_it_at_exit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import copytrader.app as app_mod

    cfg = paper_config(tmp_path)
    lock = tmp_path / "data" / "bot.lock"
    seen: dict[str, Any] = {}

    async def fake_run(config_path: Any = "config.yaml") -> None:
        seen.update(json.loads(lock.read_text(encoding="utf-8")))
        seen["holder_from_inside"] = cli.lock_holder(tmp_path / "data")

    monkeypatch.setattr(app_mod, "run", fake_run)
    assert cli.main(["run", "--config", str(cfg)]) == cli.EXIT_OK
    assert seen["pid"] == os.getpid() and seen["started"] and seen["holder_from_inside"] is None
    assert not lock.exists()


def test_run_removes_the_lock_when_the_bot_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                capsys: pytest.CaptureFixture[str]) -> None:
    import copytrader.app as app_mod

    cfg = paper_config(tmp_path)

    async def fake_run(config_path: Any = "config.yaml") -> None:
        raise RuntimeError("oturum iptal edildi")

    monkeypatch.setattr(app_mod, "run", fake_run)
    assert cli.main(["run", "--config", str(cfg)]) == cli.EXIT_PROBLEM
    assert not (tmp_path / "data" / "bot.lock").exists()


def test_a_second_run_is_refused_while_a_live_process_holds_the_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                                      capsys: pytest.CaptureFixture[str], sleeper: Any) -> None:
    import copytrader.app as app_mod

    cfg = paper_config(tmp_path)
    lock = write_lock(tmp_path / "data", sleeper.pid)
    before = lock.read_text(encoding="utf-8")
    started: list[int] = []

    async def fake_run(config_path: Any = "config.yaml") -> None:
        started.append(1)

    monkeypatch.setattr(app_mod, "run", fake_run)
    assert cli.main(["run", "--config", str(cfg)]) == cli.EXIT_CONFIG
    out = capsys.readouterr().out
    assert f"Bot şu anda çalışıyor (PID {sleeper.pid}" in out and "ikinci bir bot başlatılamaz" in out
    assert f"{tmp_path / 'data' / 'bot.lock'} dosyasını silip tekrar deneyin" in out      # the way out, if it is wrong
    assert started == [] and lock.read_text(encoding="utf-8") == before        # the other bot's lock is not ours to touch

    sleeper.kill()
    sleeper.wait()                                                             # crash: the file stays behind
    assert cli.main(["run", "--config", str(cfg)]) == cli.EXIT_OK              # stale lock: the bot starts
    assert started == [1] and not lock.exists()


def test_stale_lock_files_never_block(tmp_path: Path, sleeper: Any) -> None:
    data = tmp_path / "data"
    assert cli.lock_holder(data) is None                                       # no file
    write_lock(data, sleeper.pid)
    assert cli.lock_holder(data)["pid"] == sleeper.pid                         # live
    write_lock(data, os.getpid())
    assert cli.lock_holder(data) is None                                       # our own pid (same container pid 1)
    (data / "bot.lock").write_text("yarım kalmış", encoding="utf-8")
    assert cli.lock_holder(data) is None                                       # unreadable
    if POSIX_PROC:
        write_lock(data, sleeper.pid, token="12345")
        assert cli.lock_holder(data) is None                                   # the pid now belongs to another process
    sleeper.kill()
    sleeper.wait()
    write_lock(data, sleeper.pid, token=None)
    assert cli.lock_holder(data) is None                                       # process gone
    path = cli.acquire_lock(data)
    assert json.loads(path.read_text(encoding="utf-8"))["pid"] == os.getpid()
    cli.release_lock(path)
    assert not path.exists()


@pytest.mark.skipif(not POSIX_PROC, reason="needs /proc")
def test_process_state() -> None:
    alive, token = cli._process_state(os.getpid())
    assert alive is True and token and token.isdigit()
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    try:
        deadline = time.time() + 10
        while time.time() < deadline:                    # exited, not yet collected: a zombie is not a running bot
            if Path(f"/proc/{child.pid}/stat").read_text().rsplit(")", 1)[-1].split()[0] == "Z":
                break
            time.sleep(0.02)
        assert cli._process_state(child.pid) == (False, None)
    finally:
        child.wait()
    assert cli._process_state(child.pid) == (False, None)
    assert cli._process_state(0) == (False, None) and cli._process_state(-5) == (False, None)


@pytest.mark.parametrize("argv", [["chats"], ["login"], ["login", "--code"], ["selftest"],
                                  ["canli-test", "--broker", "sanal"]])
def test_commands_that_open_the_bots_links_refuse_while_the_bot_runs(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], sleeper: Any,
        argv: list[str]) -> None:
    cfg = paper_config(tmp_path)
    write_lock(tmp_path / "data", sleeper.pid)

    def must_not_run(*a: Any, **k: Any) -> Any:
        raise AssertionError("the command went on although the bot is running")

    for name in ("cmd_chats", "cmd_login", "cmd_selftest", "cmd_canli_test"):
        monkeypatch.setitem(cli._COMMANDS, name.removeprefix("cmd_").replace("_", "-"), must_not_run)
    assert cli.main([*argv, "--config", str(cfg)]) == cli.EXIT_PROBLEM
    out = capsys.readouterr().out
    assert "Bot şu anda çalışıyor" in out and "Ctrl+C" in out and argv[0] in out
    assert f"{tmp_path / 'data' / 'bot.lock'} dosyasını silip tekrar deneyin" in out


def test_a_lock_from_before_the_last_system_start_never_blocks(g: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                               sleeper: Any) -> None:
    """The server restarted while the bot ran: the file stays, and its pid may now belong to a process that
    cannot be inspected (alive, identity unknown). Such a lock must not keep the bot from starting."""
    data = tmp_path / "data"
    two_hours_ago = (datetime.now(UTC) - timedelta(hours=2)).isoformat(timespec="seconds")
    write_lock(data, sleeper.pid, token=None, started=two_hours_ago)
    for module, name in ((cli, "_uptime_seconds"), (g, "calisma_suresi")):
        holder = cli.lock_holder if module is cli else g.kilit_sahibi
        monkeypatch.setattr(module, name, lambda: 3 * 3600.0)             # system up for 3 h: written after the start
        assert holder(data)["pid"] == sleeper.pid
        monkeypatch.setattr(module, name, lambda: 3600.0)                 # system up for 1 h: written before it
        assert holder(data) is None
        monkeypatch.setattr(module, name, lambda: 2 * 3600.0 - 60.0)      # within the tolerance: not called stale
        assert holder(data)["pid"] == sleeper.pid
        monkeypatch.setattr(module, name, lambda: None)                   # uptime unknown: the rule is not applied
        assert holder(data)["pid"] == sleeper.pid
        monkeypatch.setattr(module, name, lambda: 60.0)
        for unreadable in ("x", "2026-11-12T03:00:00", None):             # no usable time in the file: not applied
            write_lock(data, sleeper.pid, token=None, started=unreadable or "yok")
            assert holder(data)["pid"] == sleeper.pid
        write_lock(data, sleeper.pid, token=None, started=two_hours_ago)
    monkeypatch.undo()
    if POSIX_PROC:
        assert cli._uptime_seconds() > 0 and g.calisma_suresi() > 0       # the real reading on this system
        assert cli._written_before_boot("2000-01-01T00:00:00+00:00") is True
        assert cli._written_before_boot(datetime.now(UTC).isoformat()) is False


def test_run_starts_even_when_the_lock_file_cannot_be_written(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                              capsys: pytest.CaptureFixture[str]) -> None:
    """The lock is a guard: a file problem with it must never be the reason the bot does not start."""
    import copytrader.app as app_mod

    cfg = paper_config(tmp_path)
    started: list[int] = []

    async def fake_run(config_path: Any = "config.yaml") -> None:
        started.append(1)

    def no_lock(data_dir: Any) -> Path:
        raise PermissionError(13, "erişim engellendi")

    monkeypatch.setattr(app_mod, "run", fake_run)
    monkeypatch.setattr(cli, "acquire_lock", no_lock)
    assert cli.main(["run", "--config", str(cfg)]) == cli.EXIT_OK
    assert started == [1] and "Uyarı: kilit dosyası yazılamadı" in capsys.readouterr().out


def test_the_same_commands_start_when_the_lock_is_stale(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sleeper: Any) -> None:
    cfg = paper_config(tmp_path)
    write_lock(tmp_path / "data", sleeper.pid)
    sleeper.kill()
    sleeper.wait()
    ran: list[str] = []

    async def fake(args: Any) -> int:
        ran.append(args.command)
        return 0

    for name in ("chats", "login", "selftest", "canli-test"):
        monkeypatch.setitem(cli._COMMANDS, name, fake)
    for argv in (["chats"], ["login"], ["selftest"], ["canli-test", "--broker", "sanal"]):
        assert cli.main([*argv, "--config", str(cfg)]) == 0
    assert ran == ["chats", "login", "selftest", "canli-test"]


def test_check_stays_usable_while_the_bot_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                               capsys: pytest.CaptureFixture[str], sleeper: Any) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-" + "x" * 40)
    cfg = paper_config(tmp_path)
    write_lock(tmp_path / "data", sleeper.pid)
    assert cli.main(["check", "--config", str(cfg)]) == cli.EXIT_OK
    assert "Sorun yok." in capsys.readouterr().out


def test_lock_folder_is_found_even_when_the_config_does_not_load_yet(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text("data_dir: veri\ntelegram: {api_id: 1}\n", encoding="utf-8")       # incomplete: login / chats
    assert cli._data_dir(str(path)) == "veri"
    path.write_text("telegram: [", encoding="utf-8")
    assert cli._data_dir(str(path)) == "data"
    assert cli._data_dir(str(tmp_path / "yok.yaml")) == "data"


# ---- check: an optional broker that is down is shown, but is not a failure of the check ------------------
@pytest.mark.parametrize("optional", [True, False])
def test_check_optional_broker_down(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                    capsys: pytest.CaptureFixture[str], optional: bool) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-" + "x" * 40)
    cfg = paper_config(tmp_path, brokers=[{"type": "paper", "name": "kendi"},
                                          {"type": "paper", "name": "arkadas", "optional": optional}])
    own = FakeBroker("kendi", equity=500.0)
    friend = FakeBroker("arkadas", fail={"connect": BrokerError("MetaApi: hesap bağlı değil")})
    monkeypatch.setattr(cli, "_make_brokers", lambda config: ([own, friend], []))
    code = cli.main(["check", "--config", str(cfg)])
    out = capsys.readouterr().out
    assert "✅ kendi" in out and "arkadas: bağlanılamadı: MetaApi: hesap bağlı değil" in out
    if optional:
        assert code == cli.EXIT_OK and "isteğe bağlı hesap" in out and "Sorun yok (isteğe bağlı arkadas bağlı değil)." in out
        assert "❌" not in out
    else:
        assert code == cli.EXIT_PROBLEM and "❌ arkadas" in out and "1 sorun bulundu." in out
    # a broker that cannot even be created
    monkeypatch.setattr(cli, "_make_brokers", lambda config: ([own], ["arkadas: gerekli paket yüklü değil"]))
    code = cli.main(["check", "--config", str(cfg)])
    out = capsys.readouterr().out
    assert code == (cli.EXIT_OK if optional else cli.EXIT_PROBLEM) and "arkadas: gerekli paket yüklü değil" in out


# =========================================================================== #
# T3  selftest
# =========================================================================== #
PRICES = {"BTC": 103250.0, "ETH": 3500.0, "SOL": 180.0, "EUR": 1.0850, "GBP": 1.2700, "USD": 155.20, "XAU": 4010.0}
MT_NAMES = {"BTC": "BTCUSD", "ETH": "ETHUSD", "EUR": "EURUSD", "GBP": "GBPUSD", "USD": "USDJPY", "XAU": "XAUUSD"}
MT_CONTRACT = {"EUR": 100000.0, "GBP": 100000.0, "USD": 1000.0, "XAU": 100.0}     # USDJPY: 1 lot ~ 1000 USD per JPY


class Venue(FakeBroker):
    """FakeBroker that carries a REAL broker config and names instruments like the venue does.
    ``mt``: MetaTrader-like names and lots; ``shift``: price offset against the chart exchange, per asset
    (a CFD broker's BTCUSD / ETHUSD quote a little away from the Binance perpetual)."""

    def __init__(self, config: Any, *, mt: bool = False, shift: dict[str, float] | None = None,
                 missing: tuple[str, ...] = (), **kw: Any) -> None:
        super().__init__(config.name, **kw)
        self.config = config
        self.mt, self.shift, self.missing = mt, dict(shift or {}), set(missing)
        self.writes: list[str] = []

    async def resolve_symbol(self, base_asset: str, quote_asset: str | None = None,
                             market_type: MarketType | None = None, raw_symbol: str | None = None) -> SymbolInfo | None:
        base = base_asset.upper()
        if self.mt:
            name = (getattr(self.config, "symbol_map", {}) or {}).get(base) or MT_NAMES.get(base)
            if name is None or base in self.missing:
                raise BrokerError(f"{self.name}: '{base}' için MT5'te sembol bulunamadı (symbol_map ekleyin)")
            return SymbolInfo(broker_symbol=name, base_asset=base, quote_asset="USD", qty_step=0.01, min_qty=0.01,
                              price_tick=0.00001, contract_size=MT_CONTRACT.get(base, 1.0))
        if market_type in (MarketType.FOREX, MarketType.CFD) or base in self.missing:
            return None
        return SymbolInfo(broker_symbol=f"{base}/USDT:USDT", base_asset=base, quote_asset="USDT", qty_step=0.001,
                          min_qty=0.001, price_tick=0.01)

    async def get_quote(self, symbol: SymbolInfo) -> Quote:
        self._maybe_fail("get_quote")
        price = PRICES[symbol.base_asset] + self.shift.get(symbol.base_asset, 0.0)
        return Quote(bid=price * 0.9999, ask=price * 1.0001, last=price)

    async def size_position(self, symbol: SymbolInfo, side: Side, entry_price: float, stop_loss: float,
                            risk_amount: float) -> SizingResult:
        self._maybe_fail("size_position")
        per_unit = abs(entry_price - stop_loss) * symbol.contract_size
        qty = int(risk_amount / per_unit / symbol.qty_step + 1e-9) * symbol.qty_step
        if qty < symbol.min_qty:
            raise BrokerError(f"miktar minimum emir miktarının ({symbol.min_qty}) altında")
        return SizingResult(quantity=round(qty, 8), risk_amount=risk_amount, est_loss_at_sl=qty * per_unit,
                            notional=qty * symbol.contract_size * entry_price)

    async def open_position(self, req: Any) -> Any:
        self.writes.append("open_position")
        return await super().open_position(req)

    async def update_stop_loss(self, pos: Any, new_stop_loss: float) -> Any:
        self.writes.append("update_stop_loss")
        return await super().update_stop_loss(pos, new_stop_loss)

    async def close_position(self, pos: Any, fraction: float = 1.0) -> Any:
        self.writes.append("close_position")
        return await super().close_position(pos, fraction)

    async def cancel_entry(self, pos: Any) -> Any:
        self.writes.append("cancel_entry")
        return await super().cancel_entry(pos)


class ChartOracle:
    """The engine's price source: chart price of the base asset (what the signal's levels are written in)."""

    async def price(self, base_asset: str | None, quote_asset: str | None = None, market_type: Any = None,
                    raw_symbol: str | None = None, *, fresh: bool = False) -> float | None:
        return PRICES.get((base_asset or "").upper())

    async def close(self) -> None:
        return None


class Reference:
    async def mid(self, exchange_id: str, base_asset: str, *, fresh: bool = False) -> float | None:
        return PRICES.get(base_asset.upper())

    async def close(self) -> None:
        return None


def prod_config() -> AppConfig:
    text = PROD_YAML
    for key, value in {**ENV, "METAAPI_TOKEN:-": ENV["METAAPI_TOKEN"], "METAAPI_ACCOUNT_ID:-": "acc-1"}.items():
        text = text.replace("${" + key + "}", value)
    return AppConfig.model_validate(yaml.safe_load(text))


def prod_venues(cfg: AppConfig, **kw: Any) -> dict[str, Venue]:
    return {
        "binance": Venue(broker_of(cfg, "binance"), equity=46.90),
        "breakout": Venue(broker_of(cfg, "breakout"), equity=2500.0),
        "fx": Venue(broker_of(cfg, "fx"), mt=True, equity=10_000.0, currency="USD"),
        # +150 on BTC (0.15 %) and +5 on ETH (0.14 %): inside tickmill's max_offset_pct of 1 %
        "tickmill": Venue(broker_of(cfg, "tickmill"), mt=True, shift={"BTC": 150.0, "ETH": 5.0}, equity=108.0,
                          currency="USD"),
    }


async def run_selftest(cfg: AppConfig, venues: dict[str, Venue], errors: list[str] | None = None, **kw: Any) -> tuple[int, str]:
    selftest = importlib.import_module("copytrader.selftest")
    lines: list[str] = []
    code = await selftest.run_selftest(cfg, list(venues.values()), errors or [], out=lines.append,
                                       oracle_factory=lambda config, brokers: ChartOracle(),
                                       reference_prices=Reference(), **kw)
    return code, "\n".join(lines)


def line_of(text: str, *parts: str) -> str:
    found = [ln for ln in text.splitlines() if all(p in ln for p in parts)]
    assert len(found) == 1, (parts, found)
    return found[0]


async def test_selftest_prices_and_sizes_every_routed_instrument_and_sends_nothing() -> None:
    cfg = prod_config()
    venues = prod_venues(cfg)
    code, text = await run_selftest(cfg, venues)
    assert code == 0, text
    assert "hiçbir emir gönderilmez" in text and "SONUÇ: SORUN YOK" in text and "<" not in text
    # accounts
    assert "[TAMAM] binance (fake): hesap değeri 46.90 USDT" in text
    assert "[TAMAM] tickmill (fake, isteğe bağlı): hesap değeri 108.00 USD" in text
    # kripto route: symbol, price and size per broker; the size line is the engine's own preview
    row = line_of(text, "[TAMAM] binance", "BTC/USDT:USDT")
    assert "fiyat 103260.325" in row and "miktar 0.009" in row and "risk 10.00 USDT" in row
    assert "miktar" in line_of(text, "[TAMAM] breakout", "ETH/USDT:USDT")
    shifted = line_of(text, "[TAMAM] tickmill", "BTCUSD")                  # offset broker: levels moved by +150
    assert "risk 45.00 USD" in shifted and "fark +150.0 uygulandı" in shifted
    assert "allowed_assets" in line_of(text, "tickmill", "SOL")            # excluded by config: reported, no problem
    # fx route
    assert "risk 100.00 USD" in line_of(text, "[TAMAM] fx", "EURUSD")
    for asset in ("GBPUSD", "USDJPY", "XAUUSD"):
        assert "miktar" in line_of(text, "[TAMAM] tickmill", asset)
    assert text.count("[TAMAM]") == 4 + 8 + 8 and "[SORUN]" not in text
    for venue in venues.values():                                           # read-only: no order call at all
        assert venue.writes == [] and venue.opened == []
        assert venue.connected and venue.closed
    assert "1R (işlem başına risk): binance 10 USD, breakout 50 USD, fx 100 USD, tickmill 45 USD" in text
    assert "Günlük zarar freni: binance %10, breakout %10, fx %3, tickmill kapalı" in text


async def test_selftest_names_the_exact_problems_and_exits_1() -> None:
    cfg = prod_config()
    venues = prod_venues(cfg)
    venues["tickmill"].fail["connect"] = BrokerError("MetaApi: token geçersiz")
    venues["fx"].missing = {"XAU"}
    venues["binance"].fail["size_position"] = BrokerError("binance: yetersiz teminat (gerekli 12.00, serbest 3.10)")
    code, text = await run_selftest(cfg, venues)
    assert code == 1
    assert "[SORUN] tickmill (isteğe bağlı): bağlanılamadı: MetaApi: token geçersiz" in text
    assert "symbol_map ekleyin" in line_of(text, "[SORUN] fx", "XAU", "MT5'te sembol bulunamadı")
    assert "yetersiz teminat (gerekli 12.00, serbest 3.10)" in line_of(text, "[SORUN] binance", "BTC/USDT:USDT")
    assert "[TAMAM] breakout" in text and "[TAMAM] fx" in text              # the others are still checked
    summary = text[text.index("SONUÇ:"):]
    # 1 account that is down (listed once, not once per instrument) + 3 coins on binance + XAUUSD on fx
    assert summary.splitlines()[0] == "SONUÇ: 5 SORUN VAR"
    assert "1. tickmill: bağlanılamadı: MetaApi: token geçersiz" in summary
    assert "kripto / BTC / binance: BTC/USDT:USDT: binance: yetersiz teminat" in summary
    assert "fx / XAUUSD / fx:" in summary and "tickmill" not in summary.split("\n", 2)[2]
    assert text.count("tickmill: hesap bağlı değil") == 2 + 4               # BTC, ETH + the four fx instruments


async def test_selftest_a_broker_that_cannot_be_created_or_priced() -> None:
    cfg = prod_config()
    venues = prod_venues(cfg)
    del venues["fx"]                                                        # e.g. the MetaTrader5 package is missing
    venues["breakout"].fail["get_quote"] = BrokerError("fiyat kaynağı yanıt vermedi")
    code, text = await run_selftest(cfg, venues, errors=["fx: 'mt5' bağlantısı için gerekli paket yüklü değil"])
    assert code == 1
    assert "[SORUN] fx: bağlanılamadı: 'mt5' bağlantısı için gerekli paket yüklü değil" in text
    assert "fiyat kaynağı yanıt vermedi" in line_of(text, "[SORUN] breakout", "BTC/USDT:USDT")


async def test_selftest_account_extras_health_warnings_and_the_adapters_own_report(monkeypatch: pytest.MonkeyPatch) -> None:
    selftest = importlib.import_module("copytrader.selftest")
    cfg = prod_config()
    venues = prod_venues(cfg)
    fx, binance, tickmill = venues["fx"], venues["binance"], venues["tickmill"]
    fx.kind = "mt5"

    async def unhealthy() -> str | None:
        return "MT5'te Python API ile işlem kapalı (Araçlar > Seçenekler > Uzman Danışmanlar)"

    async def healthy() -> str | None:
        return None

    seen: list[list[str]] = []

    async def report(bases: list[str]) -> list[str]:
        seen.append(list(bases))
        return ["TAMAM Çoklu varlık modu kapalı.", "DİKKAT Hesapta 1 açık pozisyon var:", "DİKKAT   BTC/USDT:USDT LONG 0.002"]

    fx.health_problem = unhealthy                         # type: ignore[attr-defined]
    # what the native MT5 adapter noticed while connecting: the same finding as its health check reports
    fx.startup_warnings = ["MT5'te Python API ile işlem kapalı (Araçlar > Seçenekler > Uzman Danışmanlar); "
                           "programı yeniden başlatmanız gerekmez."]                 # type: ignore[attr-defined]
    binance.startup_warnings = ["MT5'te 'Algo Trading' düğmesi KAPALI."]     # type: ignore[attr-defined]  # no health check
    tickmill.health_problem = healthy                     # type: ignore[attr-defined]
    binance.preflight_report = report                     # type: ignore[attr-defined]
    monkeypatch.setattr(selftest, "_package_version", lambda name: "5.0.5260" if name == "MetaTrader5" else None)
    code, text = await run_selftest(cfg, venues)
    assert code == 1
    assert "[SORUN] bağlantı kontrolü: MT5'te Python API ile işlem kapalı" in text
    assert text.count("Python API ile işlem kapalı") == 2                   # once under HESAPLAR, once under SONUÇ
    assert "[SORUN] MT5'te 'Algo Trading' düğmesi KAPALI." in text           # an adapter without a health check
    assert "MetaTrader5 paketi: 5.0.5260" in text
    assert text.count("bağlantı kontrolü: TAMAM") == 1                      # tickmill; brokers without the check: nothing
    assert seen == [["BTC", "ETH", "SOL"]]                                  # the coins this account is routed
    assert "TAMAM Çoklu varlık modu kapalı." in text and "[UYARI] Hesapta 1 açık pozisyon var:" in text
    summary = text[text.index("UYARILAR"):]
    assert "1. binance: Hesapta 1 açık pozisyon var:" in summary            # a warning, not a problem
    assert "SONUÇ: 2 SORUN VAR" in summary and "fx: MT5'te Python API ile işlem kapalı" in summary


async def test_selftest_binance_checks_are_plain_get_requests_in_the_installed_ccxt() -> None:
    """The real ccxt binanceusdm class with only its HTTP layer replaced: the position mode and the API key
    restrictions are read with GET requests (nothing can change on the account)."""
    import ccxt.async_support as ccxt_async

    from copytrader.config import CcxtBrokerConfig

    selftest = importlib.import_module("copytrader.selftest")

    class Holder:
        kind = "ccxt"

        def __init__(self, exchange: Any, config: Any) -> None:
            self.exchange, self.config, self.name = exchange, config, config.name

    async def lines_for(answers: dict[str, Any], **cfg_kw: Any) -> tuple[list[str], Any, list[tuple[str, str, str, bool]]]:
        ex = ccxt_async.binanceusdm({"apiKey": "k" * 16, "secret": "s" * 16})
        log: list[tuple[str, str, str, bool]] = []

        async def fetch(url: str, method: str = "GET", headers: Any = None, body: Any = None) -> Any:
            parts = urllib.parse.urlsplit(url)
            log.append((method, parts.netloc, parts.path, bool(body)))
            return answers[parts.path]

        ex.fetch = fetch
        report = selftest._Report(lambda text: None)
        cfg = CcxtBrokerConfig(type="ccxt", name="binance", exchange="binanceusdm", **cfg_kw)
        try:
            return await selftest._ccxt_lines(Holder(ex, cfg), report, has_report=False), report, log
        finally:
            await ex.close()

    good = {"/fapi/v1/positionSide/dual": {"dualSidePosition": False},
            "/sapi/v1/account/apiRestrictions": {"ipRestrict": True, "enableWithdrawals": False, "enableFutures": True}}
    lines, report, log = await lines_for(good)
    assert log == [("GET", "fapi.binance.com", "/fapi/v1/positionSide/dual", False),
                   ("GET", "api.binance.com", "/sapi/v1/account/apiRestrictions", False)]
    assert lines == ["pozisyon modu: tek yönlü (ayarla aynı)",
                     "API anahtarı: çekim izni kapalı, IP kısıtı var, vadeli işlem izni açık"]
    assert report.problems == [] and report.warnings == []

    bad = {"/fapi/v1/positionSide/dual": {"dualSidePosition": True},
           "/sapi/v1/account/apiRestrictions": {"ipRestrict": False, "enableWithdrawals": True, "enableFutures": False}}
    lines, report, log = await lines_for(bad)
    assert all(method == "GET" and not body for method, _host, _path, body in log)
    assert len(report.problems) == 2 and "pozisyon modu hedge" in report.problems[0] and "Futures" in report.problems[1]
    assert len(report.warnings) == 2 and "para çekme izni AÇIK" in report.warnings[0] and "IP kısıtı yok" in report.warnings[1]

    lines, report, log = await lines_for(good, demo=True)                    # no such endpoint on demo / testnet
    assert [path for _m, _h, path, _b in log] == ["/fapi/v1/positionSide/dual"]
    assert "kontrol edilmedi" in lines[-1]


def test_selftest_probe_instruments() -> None:
    selftest = importlib.import_module("copytrader.selftest")
    assert selftest.PROBE_ASSETS == {"crypto": ("BTC", "ETH", "SOL"), "fx": ("EURUSD", "GBPUSD", "USDJPY", "XAUUSD")}
    assert selftest.asset_spec("BTC", "crypto") == ("BTC", "USDT", MarketType.PERP, 1.0)
    assert selftest.asset_spec("EURUSD", "fx") == ("EUR", "USD", MarketType.FOREX, 0.3)
    assert selftest.asset_spec("XAUUSD", "fx") == ("XAU", "USD", MarketType.CFD, 0.3)
    assert selftest.asset_spec("", "fx") is None
    assert selftest.parse_assets(" btc, eurusd ,,") == ["BTC", "EURUSD"]
    assert selftest.assets_for("crypto", []) == ["BTC", "ETH", "SOL"]
    assert selftest.assets_for("crypto", ["BTC", "EURUSD", "DOGE"]) == ["BTC", "DOGE"]
    assert selftest.assets_for("fx", ["BTC", "EURUSD", "XAUUSD"]) == ["EURUSD", "XAUUSD"]
    assert selftest.assets_for("any", [], {"ccxt"}) == ["BTC", "ETH", "SOL"]
    assert selftest.assets_for("any", [], {"metaapi"}) == ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD"]
    assert len(selftest.assets_for("any", [], {"ccxt", "mt5"})) == 7


async def test_selftest_override_list_and_a_config_without_topics(tmp_path: Path) -> None:
    cfg = prod_config()
    venues = prod_venues(cfg)
    code, text = await run_selftest(cfg, venues, assets="ETH,GBPUSD")
    assert code == 0 and "BTC" not in text.split("KONU", 1)[1] and "EURUSD" not in text
    assert "miktar" in line_of(text, "[TAMAM] binance", "ETH/USDT:USDT") and "miktar" in line_of(text, "[TAMAM] fx", "GBPUSD")

    plain = load_config(paper_config(tmp_path, brokers=[{"type": "paper", "name": "tek"}, {"type": "paper", "name": "iki"}]))
    one, two = Venue(plain.brokers[0], equity=1000.0), Venue(plain.brokers[1], equity=1000.0, missing=("SOL",))
    code, text = await run_selftest(plain, {"tek": one, "iki": two})
    assert code == 0 and "Tüm mesajlar (konu ayarı yok) -> tek, iki" in text and "EURUSD" not in text
    assert "bu hesapta yok" in line_of(text, "iki", "SOL")                   # another broker trades it: no problem
    one.missing = {"SOL"}
    code, text = await run_selftest(plain, {"tek": one, "iki": two})
    assert code == 1 and "tüm mesajlar / SOL: bu varlığı işleyebilen hesap yok" in text


def test_selftest_command_uses_the_dry_run_factory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                   capsys: pytest.CaptureFixture[str]) -> None:
    cfg_path = paper_config(tmp_path)
    venue = Venue(load_config(cfg_path).brokers[0], equity=1000.0)
    monkeypatch.setattr(cli, "_make_brokers", lambda config: ([venue], []))
    monkeypatch.setattr(cli, "_make_oracle", lambda config, brokers: FakePriceOracle({"ETH": 3500.0}))
    assert cli.main(["selftest", "--config", str(cfg_path), "--varlik", "ETH"]) == cli.EXIT_OK
    out = capsys.readouterr().out
    assert "BOT SAĞLIK KONTROLÜ" in out and "[TAMAM] sanal: ETH/USDT:USDT" in out and "SONUÇ: SORUN YOK" in out
    assert venue.writes == [] and not (tmp_path / "data" / "copytrader.db").exists()
    venue2 = Venue(load_config(cfg_path).brokers[0], equity=1000.0, fail={"connect": BrokerError("kapalı")})
    monkeypatch.setattr(cli, "_make_brokers", lambda config: ([venue2], []))
    assert cli.main(["selftest", "--config", str(cfg_path), "--varlik", "ETH"]) == cli.EXIT_PROBLEM
    assert "Traceback" not in capsys.readouterr().out


# =========================================================================== #
# T4  canli-test (paper broker with a fake price source, and failing fakes)
# =========================================================================== #
class FakePrices:
    """Public price source of the paper broker (shape of a ccxt exchange)."""

    id = "binanceusdm"

    def __init__(self) -> None:
        from ccxt.base.decimal_to_precision import TICK_SIZE

        self.precisionMode = TICK_SIZE
        self.markets = {"ETH/USDT:USDT": {
            "id": "ETHUSDT", "symbol": "ETH/USDT:USDT", "base": "ETH", "quote": "USDT", "settle": "USDT", "type": "swap",
            "swap": True, "spot": False, "linear": True, "active": True, "contractSize": 1.0, "taker": 0.0005,
            "precision": {"amount": 0.001, "price": 0.01},
            "limits": {"amount": {"min": 0.001, "max": 10000.0}, "cost": {"min": 5.0}, "leverage": {"max": 100}}}}
        self.ticker = {"bid": 2232.80, "ask": 2232.85, "last": 2232.85}

    async def load_markets(self) -> dict[str, Any]:
        return self.markets

    async def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        return dict(self.ticker)

    async def fetch_ohlcv(self, *a: Any, **k: Any) -> list[Any]:
        return []

    async def close(self) -> None:
        return None


def live_config(tmp_path: Path) -> AppConfig:
    return load_config(paper_config(
        tmp_path, brokers=[{"type": "paper", "name": "sanal", "starting_balance": 1000}, {"type": "manual", "name": "elle"}],
        telegram={"api_id": 1, "api_hash": "h", "session_string": "s", "source_chats": [-1001],
                  "topics": [{"name": "kripto", "title_contains": ["Kripto"], "brokers": ["sanal", "elle"], "kind": "crypto"}]}))


class Recorder:
    """Wraps a broker and records the order calls (what reached the venue)."""

    def __init__(self, inner: Any, fail: dict[str, list[BaseException]] | None = None) -> None:
        self.inner, self.calls, self.fail = inner, [], fail or {}
        self.last: BrokerPosition | None = None

    def __getattr__(self, name: str) -> Any:
        target = getattr(self.inner, name)
        if name not in ("open_position", "update_stop_loss", "close_position", "cancel_entry", "sync_position"):
            return target

        async def call(*args: Any, **kwargs: Any) -> Any:
            self.calls.append(name)
            if self.fail.get(name):
                raise self.fail[name].pop(0)
            result = await target(*args, **kwargs)
            self.last = result
            return result

        return call


async def live(tmp_path: Path, name: str = "sanal", *, answer: str = "EVET", fail: dict[str, list[BaseException]] | None = None,
               **kw: Any) -> tuple[int, str, Recorder | None, list[float]]:
    from copytrader.brokers.paper_broker import PaperBroker

    livetest = importlib.import_module("copytrader.livetest")
    cfg = live_config(tmp_path)
    box: list[Recorder] = []
    lines: list[str] = []
    slept: list[float] = []
    prompts: list[str] = []

    def factory(config: Any, broker_cfg: Any) -> Any:
        box.append(Recorder(PaperBroker(broker_cfg, str(tmp_path / "data"), price_source=FakePrices()), fail))
        return box[-1]

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    def confirm(prompt: str) -> str:
        prompts.append(prompt)
        return answer

    code = await livetest.run_live_test(cfg, name, broker_factory=factory, confirm=confirm, out=lines.append,
                                        sleep=sleep, **kw)
    return code, "\n".join(lines + prompts), (box[0] if box else None), slept


def paper_positions(tmp_path: Path) -> dict[str, Any]:
    state = tmp_path / "data" / "paper_sanal.json"
    return json.loads(state.read_text(encoding="utf-8"))["positions"] if state.is_file() else {}


async def test_live_test_round_trip_on_the_paper_broker(tmp_path: Path) -> None:
    code, text, broker, slept = await live(tmp_path, symbol="ETH", hold_sec=3)
    assert code == 0, text
    assert broker is not None and broker.calls == ["open_position", "sync_position", "update_stop_loss", "sync_position",
                                                   "sync_position", "close_position", "sync_position"]
    assert slept == [3, 1.0]
    # shown before anything is sent: broker, symbol, smallest size (min 0.001, but 5 USDT minimum order value), worst case
    before = text[:text.index("1. Emir gönderiliyor")]
    assert "SANAL HESAP TESTİ" in before and "sanal (paper), hesap değeri 1,000.00 USDT" in before
    assert "Sembol : ETH/USDT:USDT" in before and "Miktar : 0.003" in before
    assert "Stop   : 2210.47   Kâr al: 2255.18" in before                  # about 1 % each side
    assert "En kötü durumda kayıp (stop çalışırsa): yaklaşık 0.07 USDT" in before
    for step in ("TAMAM: pozisyon açıldı, miktar 0.003, giriş 2232.85", "TAMAM: stop 2210.47 ve kâr al 2255.18 brokerda",
                 "3. Stop taşınıyor: 2210.47 -> 2214.94", "TAMAM: yeni stop 2214.94 brokerda",
                 "TAMAM: pozisyon kapandı", "SONUÇ: TEST BAŞARILI"):
        assert step in text
    assert "brokerın bildirdiği sonuç -0.01 USDT" in text and "hesap değeri 1,000.00 -> 999.99 USDT" in text
    assert broker.last is not None and broker.last.status is PositionStatus.CLOSED and paper_positions(tmp_path) == {}
    assert "EVET" in text.splitlines()[-1]


@pytest.mark.parametrize("answer", ["evet", "", "E", "yes"])
async def test_live_test_sends_nothing_without_the_typed_word(tmp_path: Path, answer: str) -> None:
    code, text, broker, _ = await live(tmp_path, symbol="ETH", answer=answer)
    assert code == 0 and "İptal edildi; hiçbir emir gönderilmedi." in text
    assert broker is not None and broker.calls == [] and paper_positions(tmp_path) == {}


async def live_with_prices(tmp_path: Path, confirm: Any, prices: FakePrices) -> tuple[int, str, Recorder]:
    from copytrader.brokers.paper_broker import PaperBroker

    livetest = importlib.import_module("copytrader.livetest")
    cfg = live_config(tmp_path)
    box: list[Recorder] = []
    lines: list[str] = []

    def factory(config: Any, broker_cfg: Any) -> Any:
        box.append(Recorder(PaperBroker(broker_cfg, str(tmp_path / "data"), price_source=prices)))
        return box[-1]

    async def sleep(seconds: float) -> None:
        return None

    code = await livetest.run_live_test(cfg, "sanal", symbol="ETH", broker_factory=factory, confirm=confirm,
                                        out=lines.append, sleep=sleep)
    return code, "\n".join(lines), box[0]


async def test_live_test_keeps_the_event_loop_running_while_the_owner_reads(tmp_path: Path) -> None:
    """A websocket broker (MetaApi) must be able to answer its keep-alive while the question is on the screen:
    the answer is read in a helper thread. Here the 'keyboard' only answers after the loop has done other work."""
    import threading

    loop_worked = threading.Event()

    async def other_work() -> None:
        for _ in range(5):
            await asyncio.sleep(0.01)
        loop_worked.set()

    def confirm(prompt: str) -> str:
        assert threading.current_thread() is not threading.main_thread()
        return "EVET" if loop_worked.wait(10) else "olmadı"       # blocks like input() does

    task = asyncio.ensure_future(other_work())
    code, text, broker = await live_with_prices(tmp_path, confirm, FakePrices())
    await task
    assert code == 0 and "SONUÇ: TEST BAŞARILI" in text and broker.calls[0] == "open_position"


async def test_live_test_no_keyboard_or_a_failing_prompt_sends_nothing(tmp_path: Path) -> None:
    def no_keyboard(prompt: str) -> str:
        raise EOFError                                            # stdin closed / redirected

    code, text, broker = await live_with_prices(tmp_path, no_keyboard, FakePrices())
    assert code == 0 and "İptal edildi; hiçbir emir gönderilmedi." in text and broker.calls == []

    def broken(prompt: str) -> str:
        raise OSError("konsol okunamadı")

    for leftover in (tmp_path / "data").glob("paper_*"):
        leftover.unlink()
    code, text, broker = await live_with_prices(tmp_path, broken, FakePrices())
    assert code == 1 and "HATA: konsol okunamadı. Hiçbir emir gönderilmedi." in text and broker.calls == []


async def test_live_test_sends_nothing_when_the_price_moved_while_the_owner_read(tmp_path: Path) -> None:
    prices = FakePrices()

    def slow_owner(prompt: str) -> str:
        prices.ticker = {"bid": 2244.00, "ask": 2244.05, "last": 2244.05}      # +0.5 %: half of the stop distance
        return "EVET"

    code, text, broker = await live_with_prices(tmp_path, slow_owner, prices)
    assert code == 1 and broker.calls == [] and paper_positions(tmp_path) == {}
    assert "Siz onaylarken fiyat değişti (gösterilen 2232.85, şimdi 2244.05)" in text
    assert "Hiçbir emir gönderilmedi; komutu yeniden çalıştırın." in text

    def quick_owner(prompt: str) -> str:
        prices.ticker = {"bid": 2245.00, "ask": 2245.05, "last": 2245.05}      # a few ticks more: still the same market
        return "EVET"

    code, text, broker = await live_with_prices(tmp_path, quick_owner, prices)
    assert code == 0 and "SONUÇ: TEST BAŞARILI" in text
    assert "Stop   : 2221.56" in text and "stop 2221.56 ve kâr al 2266.49 brokerda" in text    # exactly what was shown


async def test_live_test_refuses_manual_and_unknown_brokers(tmp_path: Path) -> None:
    code, text, broker, _ = await live(tmp_path, "elle")
    assert code == 2 and "elle işlem hesabı" in text and broker is None        # the adapter is not even created
    code, text, broker, _ = await live(tmp_path, "yok")
    assert code == 2 and "Tanımlı brokerlar: sanal, elle" in text and broker is None


async def test_live_test_default_symbol_comes_from_the_brokers_route(tmp_path: Path) -> None:
    livetest = importlib.import_module("copytrader.livetest")
    cfg = prod_config()
    assert livetest.default_symbol(cfg, broker_of(cfg, "binance")) == "BTC"
    assert livetest.default_symbol(cfg, broker_of(cfg, "fx")) == "EURUSD"
    assert livetest.default_symbol(cfg, broker_of(cfg, "tickmill")) == "BTC"   # its first route is kripto
    plain = load_config(paper_config(tmp_path))
    assert livetest.default_symbol(plain, plain.brokers[0]) == "BTC"
    code, text, broker, _ = await live(tmp_path, symbol="DOGE")                # not on this venue
    assert code == 1 and "bulunamadı" in text and "Hiçbir emir gönderilmedi" in text and broker.calls == []


async def test_live_test_a_failing_step_is_reported_and_the_position_is_still_closed(tmp_path: Path) -> None:
    code, text, broker, _ = await live(tmp_path, symbol="ETH",
                                       fail={"update_stop_loss": [BrokerError("stop fiyata çok yakın (10016)")]})
    assert code == 1
    assert "HATA: stop taşınamadı: stop fiyata çok yakın (10016)" in text
    assert "TAMAM: pozisyon kapandı" in text and "SONUÇ: TEST BAŞARISIZ (1 sorun)" in text
    assert "Hesapta bu testten açık pozisyon kalmadı" in text and "DİKKAT" not in text
    assert paper_positions(tmp_path) == {} and broker.last.status is PositionStatus.CLOSED


async def test_live_test_error_in_the_middle_closes_the_position(tmp_path: Path) -> None:
    code, text, broker, _ = await live(tmp_path, symbol="ETH", fail={"sync_position": [RuntimeError("bağlantı koptu")]})
    assert code == 1 and "HATA: bağlantı koptu" in text
    assert "Pozisyon kapatıldı; hesapta bu testten açık bir şey kalmadı" in text and "AÇIK KALMIŞ" not in text
    assert broker.calls[:3] == ["open_position", "sync_position", "close_position"] and paper_positions(tmp_path) == {}


async def test_live_test_shouts_when_the_position_may_still_be_open(tmp_path: Path) -> None:
    # 1. the order call itself fails: nobody knows whether the venue took it
    code, text, broker, _ = await live(tmp_path, symbol="ETH", fail={"open_position": [BrokerError("yanıt alınamadı")]})
    assert code == 1
    assert "DİKKAT: sanal hesabında ETH/USDT:USDT LONG 0.003 pozisyonu AÇIK KALMIŞ OLABİLİR" in text
    assert "emrin brokera ulaşıp ulaşmadığı bilinmiyor" in text and "ELLE KAPATIN" in text

    # 2. the position is open and cannot be closed
    for leftover in (tmp_path / "data").glob("paper_*"):
        leftover.unlink()
    code, text, broker, _ = await live(tmp_path, symbol="ETH",
                                       fail={"close_position": [BrokerError("piyasa kapalı"), BrokerError("piyasa kapalı")]})
    assert code == 1
    assert "HATA: pozisyon kapatılamadı: piyasa kapalı" in text
    assert "DİKKAT: sanal hesabında ETH/USDT:USDT LONG 0.003 pozisyonu AÇIK KALMIŞ OLABİLİR" in text
    assert "SONUÇ: TEST BAŞARILI" not in text and len(paper_positions(tmp_path)) == 1     # it really is still open


async def test_live_test_checks_what_the_adapter_recorded_about_the_venue() -> None:
    livetest = importlib.import_module("copytrader.livetest")

    def pos(**data: Any) -> BrokerPosition:
        return BrokerPosition(broker="b", broker_symbol="S", side=Side.LONG, status=PositionStatus.OPEN, quantity=1,
                              stop_loss=90.0, take_profits=[{"price": 110.0}], data=data)

    ok = livetest._position_problem
    assert ok(pos(), 90.0, 110.0, 0.01) is None
    assert "istenen" in ok(pos(), 91.0, 110.0, 0.01) and "kâr al" in ok(pos(), 90.0, 111.0, 0.01)
    assert ok(pos(unprotected=True), 90.0, 110.0, 0.01) == "stop emri yok"
    failed = pos()
    failed.error = "kâr al emri yerleştirilemedi"
    assert ok(failed, 90.0, 110.0, 0.01) == "kâr al emri yerleştirilemedi"
    # MetaTrader legs: the take profit read back from the terminal
    assert ok(pos(legs=[{"state": "open", "ticket": 7, "venue_tp": 110.0, "sl": 90.0}]), 90.0, 110.0, 0.01) is None
    assert "#7" in ok(pos(legs=[{"state": "open", "ticket": 7, "venue_tp": None, "sl": 90.0}]), 90.0, 110.0, 0.01)
    # ccxt: the live stop / take-profit orders
    assert ok(pos(sl_order_id="1", tp_order_ids=[{"id": "2", "status": "open"}]), 90.0, 110.0, 0.01) is None
    assert "kâr al emri" in ok(pos(sl_order_id="1", tp_order_ids=[{"id": None, "status": "failed"}]), 90.0, 110.0, 0.01)
    assert "stop emri" in ok(pos(sl_order_id=None, tp_order_ids=[{"id": "2", "status": "open"}]), 90.0, 110.0, 0.01)


@pytest.mark.skipif(os.name != "posix", reason="sends itself SIGINT")
async def test_live_test_ctrl_c_while_holding_closes_the_position_first(tmp_path: Path) -> None:
    from copytrader.brokers.paper_broker import PaperBroker

    livetest = importlib.import_module("copytrader.livetest")
    cfg = live_config(tmp_path)
    lines: list[str] = []
    box: list[Recorder] = []

    def factory(config: Any, broker_cfg: Any) -> Any:
        box.append(Recorder(PaperBroker(broker_cfg, str(tmp_path / "data"), price_source=FakePrices())))
        return box[-1]

    async def sleep(seconds: float) -> None:
        if seconds == 30:
            os.kill(os.getpid(), signal.SIGINT)              # the owner presses Ctrl+C during the hold
            os.kill(os.getpid(), signal.SIGINT)              # ... twice
        await asyncio.sleep(seconds if seconds == 30 else 0)

    previous = signal.getsignal(signal.SIGINT)
    started = time.monotonic()
    code = await livetest.run_live_test(cfg, "sanal", symbol="ETH", hold_sec=30, broker_factory=factory,
                                        confirm=lambda prompt: "EVET", out=lines.append, sleep=sleep)
    text = "\n".join(lines)
    assert time.monotonic() - started < 10                    # the 30 s hold ended at once
    assert code == 1 and "Ctrl+C alındı: önce pozisyon kapatılacak" in text and text.count("Ctrl+C alındı") == 1
    assert "TAMAM: pozisyon kapandı" in text and "test Ctrl+C ile yarıda kesildi" in text
    assert box[0].last.status is PositionStatus.CLOSED and paper_positions(tmp_path) == {}
    assert signal.getsignal(signal.SIGINT) is previous        # the handler is put back


async def test_live_test_refuses_a_symbol_the_bot_itself_holds(tmp_path: Path) -> None:
    from copytrader.storage.db import Database

    cfg = live_config(tmp_path)
    db = Database(Path(cfg.data_dir) / "copytrader.db")
    await db.open()
    plan = TradePlan(plan_id="p1", base_asset="ETH", side=Side.LONG, entry_type=EntryType.MARKET, stop_loss=2000.0,
                     risk_r=1.0, risk_pct=2.0, source_chat_id=1, created_at=datetime(2026, 11, 12, tzinfo=UTC))
    trade = await db.create_trade(plan)
    await db.save_broker_position(trade.id, BrokerPosition(
        broker="sanal", broker_symbol="ETH/USDT:USDT", side=Side.LONG, status=PositionStatus.OPEN, quantity=1.0,
        filled_quantity=1.0, remaining_quantity=1.0))
    await db.close()
    code, text, broker, _ = await live(tmp_path, symbol="ETH")
    assert code == 2 and f"bota ait açık işlem var (#{trade.id})" in text and broker.calls == []


def test_live_test_sizes_and_distances() -> None:
    livetest = importlib.import_module("copytrader.livetest")

    def sym(**kw: Any) -> SymbolInfo:
        base = dict(broker_symbol="X", base_asset="X", quote_asset="USD", qty_step=0.001, min_qty=0.001, price_tick=0.01)
        return SymbolInfo(**{**base, **kw})

    assert livetest.minimal_quantity(sym(), 100.0) == 0.001
    assert livetest.minimal_quantity(sym(min_notional=5.0), 2232.85) == 0.003          # 5 USDT * 1.1 / 2232.85, rounded up
    assert livetest.minimal_quantity(sym(min_notional=100.0), 103250.0) == 0.002
    assert livetest.minimal_quantity(sym(qty_step=0.01, min_qty=0.01, contract_size=100000.0), 1.085) == 0.01
    q = Quote(bid=1.08500, ask=1.08510, last=1.08505)
    fx = sym(price_tick=0.00001, extra={"stops_level": 0.0003})
    assert livetest.stop_distance(MarketType.FOREX, "USD", q, fx) == pytest.approx(0.0050)       # 50 pips
    assert livetest.stop_distance(MarketType.FOREX, "JPY", Quote(bid=155.20, ask=155.21, last=155.2), fx) == pytest.approx(0.50)
    assert livetest.stop_distance(MarketType.CFD, "USD", Quote(bid=4010.0, ask=4010.4, last=4010.2), fx) == pytest.approx(20.051)
    assert livetest.stop_distance(MarketType.PERP, "USDT", Quote(bid=100.0, ask=100.0, last=100.0), sym()) == pytest.approx(1.0)
    wide = Quote(bid=100.0, ask=101.0, last=100.5)                                     # never inside the spread
    assert livetest.stop_distance(MarketType.PERP, "USDT", wide, sym()) == pytest.approx(5.0)


async def test_live_test_worst_case_loss_is_in_the_account_currency() -> None:
    livetest = importlib.import_module("copytrader.livetest")
    usdjpy = SymbolInfo(broker_symbol="USDJPY", base_asset="USD", quote_asset="JPY", qty_step=0.01, min_qty=0.01,
                        price_tick=0.001, contract_size=100000.0)

    class Mt(FakeBroker):
        async def size_position(self, symbol: Any, side: Any, entry_price: float, stop_loss: float, risk_amount: float) -> Any:
            per_lot = abs(entry_price - stop_loss) * symbol.contract_size / entry_price      # JPY -> USD
            qty = int(risk_amount / per_lot / 0.01 + 1e-9) * 0.01
            if qty < 0.01:
                raise BrokerError("minimumun altında")
            return SizingResult(quantity=qty, risk_amount=risk_amount, est_loss_at_sl=qty * per_lot, notional=0.0)

    loss, in_account = await livetest.loss_at_stop(Mt("fx"), usdjpy, 155.21, 154.71, 0.01)
    assert in_account and loss == pytest.approx(0.01 * 0.50 * 100000.0 / 155.21, rel=1e-6)   # 3.22 USD, not 500 JPY
    loss, in_account = await livetest.loss_at_stop(FakeBroker("x", fail={"size_position": BrokerError("yok")}),
                                                   usdjpy, 155.21, 154.71, 0.01)
    assert not in_account and loss == pytest.approx(500.0)                              # plain difference, flagged


def test_canli_test_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from copytrader.brokers.paper_broker import PaperBroker

    cfg_path = paper_config(tmp_path)
    made: list[Any] = []

    def factory(config: Any, broker_cfg: Any) -> Any:
        made.append(broker_cfg.name)
        return PaperBroker(broker_cfg, str(tmp_path / "data"), price_source=FakePrices())

    monkeypatch.setattr(cli, "_make_live_broker", factory)
    monkeypatch.setattr("builtins.input", lambda prompt="": "EVET")
    assert cli.main(["canli-test", "--config", str(cfg_path), "--broker", "sanal", "--sembol", "ETH"]) == 0
    out = capsys.readouterr().out
    assert made == ["sanal"] and "SONUÇ: TEST BAŞARILI" in out
    with pytest.raises(SystemExit):
        cli.main(["canli-test", "--config", str(cfg_path)])                # --broker is required: never a default account
    args = cli.build_parser().parse_args(["canli-test", "--broker", "fx"])
    assert args.sembol is None and args.bekle == 0.0


def test_the_live_factory_builds_the_real_adapter_even_in_dry_run(tmp_path: Path) -> None:
    from copytrader.brokers.ccxt_broker import CcxtBroker
    from copytrader.brokers.dry_run import DryRunBroker

    data = yaml.safe_load(paper_config(tmp_path).read_text(encoding="utf-8"))
    data["brokers"] = [{"type": "ccxt", "name": "b", "exchange": "binanceusdm"}]
    cfg = AppConfig.model_validate(data)
    assert cfg.dry_run is True
    assert isinstance(cli._make_live_broker(cfg, cfg.brokers[0]), CcxtBroker)
    brokers, errors = cli._make_brokers(cfg)                                # check / parse / selftest: never real orders
    assert errors == [] and isinstance(brokers[0], DryRunBroker)


# =========================================================================== #
# T1  guncelle.py
# =========================================================================== #
@pytest.fixture(scope="module")
def g() -> Any:
    """guncelle.py loaded from the package root (no bytecode written next to it)."""
    saved, sys.dont_write_bytecode = sys.dont_write_bytecode, True
    try:
        spec = importlib.util.spec_from_file_location("guncelle_under_test", REPO / "guncelle.py")
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = saved
    module.BEKLEME = 0.0
    return module


_NEW_TREE: list[Path] = []


@pytest.fixture(scope="module", autouse=True)
def _frozen_new_tree(tmp_path_factory: pytest.TempPathFactory):
    """'The new version' of the updater tests: a copy of this source tree taken once. (The tests install it and
    compare the result with it; the live tree may be edited while they run, a copy cannot change under them.)"""
    dest = tmp_path_factory.mktemp("yeni-surum") / "telegram-copy-trader"
    try:
        shutil.copytree(REPO, dest, ignore=shutil.ignore_patterns(
            "__pycache__", "*.pyc", "*.pyo", ".git", ".pytest_cache", ".mypy_cache", ".ruff_cache", "data", "venv",
            ".venv", "yedek", ".env", "config.yaml", "*.session", "*.log"))
    except shutil.Error:
        pass        # a file that vanished while it was being copied (an editor's temporary file): not part of a release
    for needed in ("copytrader/__main__.py", "copytrader/migrate.py", "guncelle.py", "requirements.txt",
                   "docs/GUNCELLEME.md"):
        assert (dest / needed).is_file(), needed
    _NEW_TREE.append(dest)
    yield
    _NEW_TREE.clear()


def new_tree() -> Path:
    return _NEW_TREE[0]


OLD_MAIN = "import sys\nprint('ESKI SURUM', sys.argv[1:])\n"


def old_bot(folder: Path, config: str = PROD_YAML) -> Path:
    """A bot folder as it is on the server: OLD package, the owner's own files, data and venv."""
    bot = folder / "telegram-copy-trader"
    (bot / "copytrader" / "brokers").mkdir(parents=True)
    (bot / "copytrader" / "__init__.py").write_text("__version__ = 'eski'\n", encoding="utf-8")
    (bot / "copytrader" / "__main__.py").write_text(OLD_MAIN, encoding="utf-8")
    (bot / "copytrader" / "eski_modul.py").write_text("# yeni sürümde olmayan modül\n", encoding="utf-8")
    (bot / "copytrader" / "brokers" / "eski_broker.py").write_text("# eski\n", encoding="utf-8")
    (bot / "copytrader" / "__pycache__").mkdir()
    (bot / "copytrader" / "__pycache__" / "eski_modul.cpython-312.pyc").write_bytes(b"\x00pyc")
    (bot / "config.yaml").write_text(config, encoding="utf-8")
    write_env(bot)
    (bot / "requirements.txt").write_text((new_tree() / "requirements.txt").read_text(encoding="utf-8"), encoding="utf-8")
    (bot / "README.md").write_text("eski beni oku\n", encoding="utf-8")
    (bot / "docs").mkdir()
    (bot / "docs" / "ESKI.md").write_text("eski belge\n", encoding="utf-8")
    (bot / "data" / "logs").mkdir(parents=True)
    (bot / "data" / "copytrader.db").write_bytes(b"veritabani")
    (bot / "data" / "telegram_user.session").write_bytes(b"oturum")
    (bot / "data" / "logs" / "copytrader.log").write_text("kayıt\n", encoding="utf-8")
    (bot / "venv" / "Scripts").mkdir(parents=True)
    (bot / "venv" / "Scripts" / "python.exe").write_bytes(b"MZ")
    (bot / "extra.session").write_bytes(b"baska oturum")
    (bot / "config.yaml.yedek1").write_text("# sahibin yedeği\n", encoding="utf-8")
    (bot / "baslat.ps1").write_text("# sahibin betiği\n", encoding="utf-8")
    return bot


OWNER_FILES = [".env", "data/copytrader.db", "data/telegram_user.session", "data/logs/copytrader.log",
               "venv/Scripts/python.exe", "extra.session", "config.yaml.yedek1", "baslat.ps1", "docs/ESKI.md"]


def snapshot(folder: Path, skip: tuple[str, ...] = ("yedek/",)) -> dict[str, bytes]:
    out = {}
    for path in sorted(folder.rglob("*")):
        rel = path.relative_to(folder).as_posix()
        if path.is_file() and "__pycache__" not in rel and not rel.startswith(skip):
            out[rel] = path.read_bytes()
    return out


def package_files(folder: Path) -> dict[str, bytes]:
    return {k: v for k, v in snapshot(folder).items() if not k.endswith((".pyc", ".pyo"))}


CHECK_OK = ["Yapılandırma: config.yaml", "Anthropic API anahtarı: ayarlı", "✅ binance (ccxt): hesap değeri 46.90 USDT",
            "", "Sorun yok."]


def check_bad(*brokers: str) -> list[str]:
    return ["Anthropic API anahtarı: ayarlı", *[f"❌ {b}: bağlanılamadı: zaman aşımı" for b in brokers], "",
            f"{len(brokers)} sorun bulundu."]


class Runner:
    """Stands in for the subprocesses of the updater. ``script``: command -> list of (exit code, output lines),
    one entry per call (the last one repeats). ``migrate`` runs the real migration on the folder's config."""

    def __init__(self, script: dict[str, list[tuple[int, list[str]]]] | None = None) -> None:
        self.script = script or {}
        self.calls: list[str] = []

    def __call__(self, komut: list[str], klasor: Path, sure: float, satir: Any) -> int:
        key = " ".join(komut[1:])
        self.calls.append(key)
        if key in self.script:
            n = self.calls.count(key)
            code, lines = self.script[key][min(n, len(self.script[key])) - 1]
            for line in lines:
                satir(line)
            return code
        if key == "-m copytrader migrate":
            return importlib.import_module("copytrader.migrate").run_cli(str(klasor / "config.yaml"), out=satir)
        if key == "-m copytrader check":
            for line in CHECK_OK:
                satir(line)
            return 0
        if key == "-m copytrader selftest":
            satir("SONUÇ: SORUN YOK.")
            return 0
        raise AssertionError(f"unexpected command: {komut}")


def update(g: Any, bot: Path, *options: str, runner: Runner | None = None, processes: Any = lambda: [],
           answer: str = "", capsys: Any = None) -> tuple[int, Runner, list[tuple[Path, str]]]:
    runner = runner or Runner()
    started: list[tuple[Path, str]] = []

    def start(hedef: Path, python: str) -> int:
        started.append((hedef, python))
        return 130                                   # the bot ran until Ctrl+C

    code = g.main(["--hedef", str(bot), *options], calistir=runner, surecler=processes, baslat=start,
                  girdi=lambda prompt: answer, yeni_agac=new_tree())
    return code, runner, started


def test_update_installs_migrates_checks_and_starts_the_bot(g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bot = old_bot(tmp_path)
    before = snapshot(bot)
    code, runner, started = update(g, bot)
    out = capsys.readouterr().out
    assert code == 130 and started == [(bot, sys.executable)]             # the bot's own exit code comes back
    assert runner.calls == ["-m copytrader check", "-m copytrader migrate", "-m copytrader check", "-m copytrader selftest"]
    steps = [ln for ln in out.splitlines() if re.match(r"\d\. ", ln)]
    assert [s[0] for s in steps] == list("123456789") and all(s.endswith(": TAMAM") for s in steps), out
    assert "binance: günlük zarar freni kapatıldı" in out and "Güncellemeden önce: kontrol sorunsuzdu." in out

    after = snapshot(bot)
    # the package is exactly the new one: no stale module, nothing missing
    new_package = {f"copytrader/{k}": v for k, v in package_files(new_tree() / "copytrader").items()}
    assert {k: v for k, v in after.items() if k.startswith("copytrader/")} == new_package
    assert "copytrader/eski_modul.py" not in after and not (bot / "copytrader" / "__pycache__" / "eski_modul.cpython-312.pyc").exists()
    for name in g.UST_DOSYALAR:
        assert after[name] == (new_tree() / name).read_bytes()
    assert after["docs/GUNCELLEME.md"] == (new_tree() / "docs" / "GUNCELLEME.md").read_bytes()
    for name in OWNER_FILES:                                              # never touched
        assert after[name] == before[name], name
    assert load_config(bot / "config.yaml").risk.max_open_trades == 25    # migrated with the real migration
    # backup of everything that was overwritten
    (backup,) = list((bot / "yedek").iterdir())
    assert (backup / "config.yaml").read_bytes() == before["config.yaml"]
    assert {f"copytrader/{k}": v for k, v in package_files(backup / "copytrader").items()} == {
        k: v for k, v in before.items() if k.startswith("copytrader/")}
    assert (backup / "README.md").read_text(encoding="utf-8") == "eski beni oku\n" and not (backup / ".env").exists()
    assert not (backup / "data").exists() and not (backup / "venv").exists()
    # everything on the screen is in the log file too
    (log,) = list(bot.glob("guncelleme_*.log"))
    logged = log.read_text(encoding="utf-8")
    assert all(step in logged for step in steps) and "| Sorun yok." in logged
    assert not (bot / "copytrader.eski").exists() and not (bot / "copytrader.yeni").exists()


def test_update_twice_is_harmless(g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bot = old_bot(tmp_path)
    assert update(g, bot)[0] == 130
    first = snapshot(bot, skip=("yedek/", "guncelleme_"))
    time.sleep(1.1)                                                       # the backup folders are named by the second
    assert update(g, bot)[0] == 130
    assert snapshot(bot, skip=("yedek/", "guncelleme_")) == first         # same code, same config, no second config backup
    assert len(list((bot / "yedek").iterdir())) == 2
    assert "zaten güncel" in capsys.readouterr().out


def test_update_refuses_while_the_bot_runs(g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str], sleeper: Any) -> None:
    bot = old_bot(tmp_path)
    before = snapshot(bot)
    python_bot = [(4321, "python.exe", r"C:\bot\venv\Scripts\python.exe -m copytrader run")]
    code, runner, started = update(g, bot, processes=lambda: python_bot)
    out = capsys.readouterr().out
    assert code == 2 and runner.calls == [] and started == []
    assert "2. Bot kapalı mı diye bakıldı: HATA" in out and "PID 4321" in out
    assert 'Ctrl+C ile durdurun, "Durduruldu." yazısını bekleyin, sonra aynı satırı tekrar yapıştırın' in out
    assert "bot.lock" not in out                                          # seen in the process list, not in a lock file
    assert {k: v for k, v in snapshot(bot).items() if not k.startswith("guncelleme_")} == before

    # a bot of a version that writes the lock file, not visible in the process list
    (bot / "data" / "bot.lock").write_text(
        json.dumps({"pid": sleeper.pid, "started": "x", "token": g.surec_durumu(sleeper.pid)[1]}), encoding="utf-8")
    code, runner, started = update(g, bot)
    out = capsys.readouterr().out
    assert code == 2 and runner.calls == [] and f"PID {sleeper.pid}, kilit dosyası" in out
    assert f"{bot / 'data' / 'bot.lock'} dosyasını silip satırı tekrar yapıştırın" in out     # the way out, if it is wrong
    sleeper.kill()
    sleeper.wait()
    assert update(g, bot)[0] == 130                                       # stale lock: the update goes on


def test_update_asks_when_the_process_list_cannot_be_read(g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bot = old_bot(tmp_path)
    code, runner, started = update(g, bot, processes=lambda: None, answer="hayır")
    assert code == 2 and runner.calls == [] and "doğrulanamadı" in capsys.readouterr().out
    code, runner, started = update(g, bot, processes=lambda: None, answer="EVET")
    assert code == 130 and len(started) == 1


@pytest.mark.parametrize("failing", ["migrate", "pip"])
def test_update_failure_before_the_check_restores_the_old_installation(
        g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str], failing: str) -> None:
    bot = old_bot(tmp_path)
    if failing == "pip":
        (bot / "requirements.txt").write_text("telethon>=1.0\n", encoding="utf-8")       # differs from the new one
    before = snapshot(bot)
    pip = "-m pip install --disable-pip-version-check -r requirements.txt"
    script = {"-m copytrader migrate": [(2, ["Ayar dosyası güncellenemedi: ..."])]} if failing == "migrate" else {
        pip: [(1, ["ERROR: Could not find a version that satisfies the requirement x"])]}
    code, runner, started = update(g, bot, runner=Runner(script))
    out = capsys.readouterr().out
    assert (pip in runner.calls) is (failing == "pip")                    # pip only when requirements.txt changed
    assert "-m copytrader selftest" not in runner.calls and runner.calls.count("-m copytrader check") == 1
    assert f"{6 if failing == 'migrate' else 5}. " in out and ": HATA" in out
    assert "GÜNCELLEME YAPILMADI" in out and "ESKİ SÜRÜM GERİ YÜKLENDİ" in out
    assert "9. Bot başlatılıyor (ESKİ sürüm)" in out and code == 130 and len(started) == 1
    after = snapshot(bot)
    for name, content in before.items():                                  # code and config are what they were
        assert after[name] == content, name
    assert sorted(set(after) - set(before) - {"guncelle.py", "bekci.py", "config.example.yaml",
                                              "config.tickmill.example.yaml", "pyproject.toml", "docs/GUNCELLEME.md",
                                              "docs/ARKADAS_METAAPI.md", "docs/OWNER_TICKMILL_EKLE.md",
                                              "docs/SUNUCU_CLAUDE.md"}
                  ) == sorted(p.name for p in bot.glob("guncelleme_*.log"))     # nothing else: no lock file left either


def test_update_pip_runs_only_when_requirements_changed(g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bot = old_bot(tmp_path)
    (bot / "requirements.txt").write_text("telethon>=1.0\n", encoding="utf-8")
    pip = "-m pip install --disable-pip-version-check -r requirements.txt"
    noise = [f"Requirement already satisfied: paket{i}" for i in range(40)] + ["Successfully installed pydantic-2.14.3"]
    code, runner, _ = update(g, bot, runner=Runner({pip: [(0, noise)]}))
    out = capsys.readouterr().out
    assert code == 130 and runner.calls[1] == pip and "5. Gerekli paketler kuruldu: TAMAM" in out
    assert "Successfully installed pydantic-2.14.3" in out and "paket3\n" not in out      # a short result on the screen
    assert "paket3" in next(bot.glob("guncelleme_*.log")).read_text(encoding="utf-8")       # everything in the log
    assert g.gereksinimler(bot / "requirements.txt") == g.gereksinimler(new_tree() / "requirements.txt")
    (tmp_path / "a.txt").write_text("B>=1  # yorum\n\nA\n", encoding="utf-8")
    (tmp_path / "b.txt").write_text("a\r\nb >= 1\r\n", encoding="utf-8")
    assert g.gereksinimler(tmp_path / "a.txt") == g.gereksinimler(tmp_path / "b.txt") == ["a", "b>=1"]
    assert g.gereksinimler(tmp_path / "yok.txt") is None


def test_update_check_failure_brings_the_old_version_back(g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bot = old_bot(tmp_path)
    before = snapshot(bot)
    # fine before the update, the new version cannot connect binance (also on the second try)
    runner = Runner({"-m copytrader check": [(0, CHECK_OK), (1, check_bad("binance")), (1, check_bad("binance"))]})
    code, runner, started = update(g, bot, runner=runner)
    out = capsys.readouterr().out
    assert runner.calls == ["-m copytrader check", "-m copytrader migrate", "-m copytrader check", "-m copytrader check"]
    assert "7. Ayarlar ve hesap bağlantıları kontrol edildi: HATA" in out and "bir kez daha deneniyor" in out
    assert "GÜNCELLEME YAPILMADI" in out and "güncellemeden önce olmayan sorun: hesap: binance" in out
    assert "ESKİ SÜRÜM GERİ YÜKLENDİ" in out and "9. Bot başlatılıyor (ESKİ sürüm)" in out
    assert code == 130 and len(started) == 1
    after = snapshot(bot)
    assert all(after[name] == content for name, content in before.items())
    assert after["config.yaml"] == PROD_YAML.encode("utf-8") and "copytrader/eski_modul.py" in after
    assert "copytrader/migrate.py" not in after                            # no new module left in the old package


def test_update_check_passes_on_the_second_try(g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bot = old_bot(tmp_path)
    runner = Runner({"-m copytrader check": [(0, CHECK_OK), (1, check_bad("binance")), (0, CHECK_OK)]})
    code, runner, started = update(g, bot, runner=runner)
    out = capsys.readouterr().out
    assert "7. Ayarlar ve hesap bağlantıları kontrol edildi: TAMAM" in out and "GÜNCELLEME YAPILMADI" not in out
    assert runner.calls[-1] == "-m copytrader selftest" and (bot / "copytrader" / "migrate.py").is_file()


def test_update_keeps_the_new_version_when_the_same_problem_existed_before(g: Any, tmp_path: Path,
                                                                           capsys: pytest.CaptureFixture[str]) -> None:
    bot = old_bot(tmp_path)
    # before: fx and the friend's optional account are down; after: fx still down (the optional one no longer counts)
    runner = Runner({"-m copytrader check": [(1, check_bad("fx", "tickmill")), (1, check_bad("fx"))]})
    code, runner, started = update(g, bot, runner=runner)
    out = capsys.readouterr().out
    assert runner.calls.count("-m copytrader check") == 2                 # no pointless second try
    assert "Güncellemeden önce: kontrol şu sorunları bildiriyordu: hesap: fx, hesap: tickmill." in out
    assert "7. Ayarlar ve hesap bağlantıları kontrol edildi: HATA" in out and "GERİ ALINMADI" in out
    assert "GÜNCELLEME YAPILMADI" not in out and "9. Bot başlatılıyor (Yeni sürüm)" in out
    assert (bot / "copytrader" / "migrate.py").is_file() and runner.calls[-1] == "-m copytrader selftest"


def test_update_check_result_comparison(g: Any) -> None:
    ok, fx, both = g.kontrol_ozeti(CHECK_OK), g.kontrol_ozeti(check_bad("fx")), g.kontrol_ozeti(check_bad("fx", "tickmill"))
    assert ok == (set(), 0) and fx == ({"hesap: fx"}, 1) and both == ({"hesap: fx", "hesap: tickmill"}, 2)
    assert g.geri_alinmali(0, ok, 0, ok) is False
    assert g.geri_alinmali(0, ok, 1, fx) is True                          # fine before, not fine now
    assert g.geri_alinmali(1, both, 1, fx) is False                       # nothing new
    assert g.geri_alinmali(1, fx, 1, both) is True                        # something new
    assert g.geri_alinmali(1, both, 2, (set(), None)) is True             # config error / crash of the new version
    assert g.geri_alinmali(None, (set(), None), 1, fx) is True            # no earlier result to compare with
    crashed = g.kontrol_ozeti(["Anthropic API anahtarı: ayarlı", "Hata: invalid syntax"])
    assert crashed == (set(), None) and g.geri_alinmali(1, both, 1, crashed) is True      # did not run to its end
    half = g.kontrol_ozeti(["Bot token: YOK / geçersiz görünüyor (@BotFather'dan alın)", "Traceback (most recent call last):"])
    assert g.geri_alinmali(1, (half[0], 1), 1, half) is True              # no closing line: never "the same as before"
    unnamed = (fx[0], 2)                                                  # the report counts more than can be named
    assert g.geri_alinmali(1, both, 1, unnamed) is True
    settings = g.kontrol_ozeti(["Anthropic API anahtarı: YOK / geçersiz görünüyor", "Sahip kullanıcı ID: geçersiz",
                                "Telegram kullanıcı oturumu: YOK ('python -m copytrader login' çalıştırın)",
                                "⚠️  Etkin broker yok.", "⚠️  tickmill: bağlanılamadı: x (isteğe bağlı hesap: ...)",
                                "", "4 sorun bulundu."])
    assert settings == ({"ayar: Anthropic API anahtarı", "ayar: Sahip kullanıcı ID", "ayar: Telegram kullanıcı oturumu",
                         "ayar: etkin hesap yok"}, 4)
    # a line of the logging module in between is not a line of the report, whatever words it carries
    noisy = g.kontrol_ozeti(["WARNING copytrader.brokers.metaapi_broker: tickmill: token geçersiz, hesap YOK",
                             "ERROR copytrader.brokers.ccxt_broker: binance: API anahtarı geçersiz", *check_bad("fx")])
    assert noisy == fx and g.geri_alinmali(1, both, 1, noisy) is False


def test_update_selftest_problems_never_roll_back(g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bot = old_bot(tmp_path)
    runner = Runner({"-m copytrader selftest": [(1, ["SONUÇ: 2 SORUN VAR", "  1. fx / XAUUSD / fx: sembol bulunamadı"])]})
    code, runner, started = update(g, bot, runner=runner)
    out = capsys.readouterr().out
    assert "8. Sağlık kontrolü sorun buldu: HATA" in out and "fx / XAUUSD / fx: sembol bulunamadı" in out
    assert "Güncelleme geri ALINMADI" in out and "9. Bot başlatılıyor (Yeni sürüm)" in out and len(started) == 1
    assert (bot / "copytrader" / "selftest.py").is_file()


def test_update_options(g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bot = old_bot(tmp_path)
    code, runner, started = update(g, bot, "--baslatma", "--kontrol-yok")
    out = capsys.readouterr().out
    assert code == 0 and started == [] and runner.calls == ["-m copytrader migrate"]
    assert "7. Ayar ve bağlantı kontrolü atlandı (--kontrol-yok): TAMAM" in out and "9. Bot başlatılmadı (Yeni sürüm kurulu): TAMAM" in out
    runner = Runner({"-m copytrader migrate": [(2, [])]})
    code, runner, started = update(g, bot, "--baslatma", "--kontrol-yok", runner=runner)
    assert code == 1 and started == []                                    # rolled back and not started: exit code 1


def test_update_refuses_a_folder_that_is_not_the_bot_or_an_incomplete_download(g: Any, tmp_path: Path,
                                                                                capsys: pytest.CaptureFixture[str]) -> None:
    empty = tmp_path / "bos"
    empty.mkdir()
    code, runner, started = update(g, empty)
    out = capsys.readouterr().out
    assert code == 2 and runner.calls == [] and started == [] and list(empty.iterdir()) == []
    assert "1. Klasörler kontrol edildi: HATA" in out and "burası botun klasörü değil" in out

    bot = old_bot(tmp_path)
    (bot / ".env").unlink()
    assert update(g, bot)[0] == 2 and ".env" in capsys.readouterr().out
    write_env(bot)
    broken_new = tmp_path / "yeni"
    shutil.copytree(new_tree() / "copytrader", broken_new / "copytrader", ignore=shutil.ignore_patterns("__pycache__"))
    (broken_new / "copytrader" / "selftest.py").unlink()
    for name in ("requirements.txt", "guncelle.py"):
        shutil.copy2(new_tree() / name, broken_new / name)
    before = snapshot(bot)
    code = g.main(["--hedef", str(bot)], calistir=Runner(), surecler=lambda: [], baslat=lambda h, p: 0, yeni_agac=broken_new)
    assert code == 2 and "copytrader/selftest.py yok" in capsys.readouterr().out and snapshot(bot) == before


def test_update_install_failure_leaves_the_old_package_in_place(g: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                                capsys: pytest.CaptureFixture[str]) -> None:
    bot = old_bot(tmp_path)
    before = snapshot(bot)
    real = os.rename
    monkeypatch.setattr(g, "_tekrarla", lambda islem, kez=6, bekle=0.5: islem())      # no waiting in the test

    def rename(src: Any, dst: Any) -> None:
        if str(src).endswith("copytrader.yeni"):
            raise PermissionError(13, "dosya başka bir program tarafından kullanılıyor")
        real(src, dst)

    monkeypatch.setattr(g.os, "rename", rename)
    code, runner, started = update(g, bot)
    out = capsys.readouterr().out
    assert "4. Yeni sürüm kurulamadı: HATA" in out and "ESKİ SÜRÜM GERİ YÜKLENDİ" in out and len(started) == 1
    after = snapshot(bot)
    assert all(after[name] == content for name, content in before.items())
    assert not (bot / "copytrader.eski").exists() and not (bot / "copytrader.yeni").exists()

    # an earlier run was cut between the two renames: the package is put back before anything else
    monkeypatch.setattr(g.os, "rename", real)
    real(bot / "copytrader", bot / "copytrader.eski")
    assert update(g, bot)[0] == 130 and (bot / "copytrader" / "migrate.py").is_file()


def test_update_interrupted_before_the_check_passed_restores(g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bot = old_bot(tmp_path)
    before = snapshot(bot)

    class Interrupting(Runner):
        def __call__(self, komut: list[str], klasor: Path, sure: float, satir: Any) -> int:
            if " ".join(komut[1:]) == "-m copytrader check" and self.calls:          # the new version's check
                raise KeyboardInterrupt
            return super().__call__(komut, klasor, sure, satir)

    code, runner, started = update(g, bot, runner=Interrupting())
    out = capsys.readouterr().out
    assert code == 130 and started == [] and "ESKİ SÜRÜM GERİ YÜKLENDİ; bot başlatılmadı" in out
    after = snapshot(bot)
    assert all(after[name] == content for name, content in before.items())

    class InterruptingSelftest(Runner):
        def __call__(self, komut: list[str], klasor: Path, sure: float, satir: Any) -> int:
            if " ".join(komut[1:]) == "-m copytrader selftest":
                raise KeyboardInterrupt
            return super().__call__(komut, klasor, sure, satir)

    code, runner, started = update(g, bot, runner=InterruptingSelftest())
    assert code == 130 and started == [] and "Yeni sürüm kurulu; bot başlatılmadı" in capsys.readouterr().out
    assert (bot / "copytrader" / "migrate.py").is_file()                   # the check had passed: the new version stays


# ---- the way back (--geri-al) -------------------------------------------------------------------------------
# (No waiting between the runs here: backups made within the same second get "-2", "-3"... and keep their order.)
def package_of(files: dict[str, bytes]) -> dict[str, bytes]:
    return {k: v for k, v in files.items() if k.startswith("copytrader/")}


def test_way_back_restores_the_code_and_config_from_before_the_update(g: Any, tmp_path: Path,
                                                                      capsys: pytest.CaptureFixture[str]) -> None:
    bot = old_bot(tmp_path)
    before = snapshot(bot)
    assert update(g, bot)[0] == 130                                       # old -> new
    with open(bot / "config.yaml", "a", encoding="utf-8") as fh:
        fh.write("# sahibin güncellemeden sonra eklediği satır\n")
    capsys.readouterr()
    code, runner, started = update(g, bot, "--geri-al")
    out = capsys.readouterr().out
    assert code == 130 and started == [(bot, sys.executable)] and runner.calls == ["-m copytrader check"]
    assert "ESKİ SÜRÜME DÖNÜŞ" in out and "6. Bot başlatılıyor (ESKİ sürüm)" in out
    steps = [ln for ln in out.splitlines() if re.match(r"\d\. ", ln)]
    assert [s[0] for s in steps] == list("123456") and all(s.endswith(": TAMAM") for s in steps), out

    after = snapshot(bot)
    assert package_of(after) == package_of(before) and after["config.yaml"] == before["config.yaml"]
    assert "copytrader/eski_modul.py" in after and "copytrader/migrate.py" not in after
    for name in OWNER_FILES:
        assert after[name] == before[name], name
    assert after["guncelle.py"] == (new_tree() / "guncelle.py").read_bytes()      # stays: the way forward again
    # what was replaced is kept, the owner's later edit included
    first, second = sorted(path.name for path in (bot / "yedek").iterdir())
    assert second.endswith("-geri") and f"Bu, {g.yedek_tarihi(bot / 'yedek' / first)} tarihinde yapılan güncellemeden" in out
    assert "sahibin güncellemeden sonra eklediği satır" in (bot / "yedek" / second / "config.yaml").read_text(encoding="utf-8")
    assert (bot / "yedek" / second / "copytrader" / "migrate.py").is_file()
    logged = "".join(path.read_text(encoding="utf-8") for path in sorted(bot.glob("guncelleme_*.log")))
    assert "=== BOT GÜNCELLEME ===" in logged and "=== ESKİ SÜRÜME DÖNÜŞ ===" in logged       # both runs are on file

    assert update(g, bot)[0] == 130 and (bot / "copytrader" / "migrate.py").is_file()   # and forward again
    assert update(g, bot, "--geri-al", "--baslatma", "--kontrol-yok")[0] == 0             # ... and back once more
    assert package_of(snapshot(bot)) == package_of(before)
    assert update(g, bot, "--geri-al", "--baslatma", "--kontrol-yok")[0] == 2             # nothing older than that
    assert package_of(snapshot(bot)) == package_of(before)


def test_way_back_skips_backups_that_equal_the_installed_code(g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bot = old_bot(tmp_path)
    before = snapshot(bot)
    assert update(g, bot)[0] == 130
    assert update(g, bot)[0] == 130                                       # the same update again: its backup IS the new code
    first, second = sorted(path.name for path in (bot / "yedek").iterdir())
    assert g.son_farkli_yedek(bot).name == first
    code, runner, started = update(g, bot, "--geri-al", "--baslatma", "--kontrol-yok")
    out = capsys.readouterr().out
    assert code == 0 and started == [] and runner.calls == [] and f"yedek/{first}" in out.replace("\\", "/")
    assert "5. Ayar ve bağlantı kontrolü atlandı (--kontrol-yok): TAMAM" in out and "6. Bot başlatılmadı (ESKİ sürüm kurulu): TAMAM" in out
    after = snapshot(bot)
    assert package_of(after) == package_of(before) and after["config.yaml"] == before["config.yaml"]
    # The backup the way back made of the new code is not a way back itself: a second --geri-al must not swing
    # forward to it. Nothing older is left here, so it refuses; the way forward is the update line.
    assert g.son_farkli_yedek(bot) is None
    assert update(g, bot, "--geri-al", "--baslatma", "--kontrol-yok")[0] == 2
    assert package_of(snapshot(bot)) == package_of(before)


def test_way_back_twice_reaches_the_version_before_the_previous_update(g: Any, tmp_path: Path,
                                                                       capsys: pytest.CaptureFixture[str]) -> None:
    """Two updates with two different new versions: the first --geri-al undoes the second update, the second
    --geri-al the first one (it does not swing forward again)."""
    bot = old_bot(tmp_path)
    old = package_of(snapshot(bot))
    newer = tmp_path / "ikinci-surum"
    shutil.copytree(new_tree(), newer, ignore=shutil.ignore_patterns("tests", "__pycache__"))
    with open(newer / "copytrader" / "__init__.py", "a", encoding="utf-8") as fh:
        fh.write("# ikinci yeni sürüm\n")

    assert update(g, bot)[0] == 130                                       # old -> first new version
    first_new = package_of(snapshot(bot))
    code = g.main(["--hedef", str(bot), "--kontrol-yok", "--baslatma"], calistir=Runner(), surecler=lambda: [],
                  baslat=lambda h, p: 0, girdi=lambda prompt: "", yeni_agac=newer)       # -> second new version
    assert code == 0 and package_of(snapshot(bot)) != first_new
    assert update(g, bot, "--geri-al", "--baslatma", "--kontrol-yok")[0] == 0
    assert package_of(snapshot(bot)) == first_new
    assert update(g, bot, "--geri-al", "--baslatma", "--kontrol-yok")[0] == 0
    assert package_of(snapshot(bot)) == old
    assert g.yedek_tarihi(Path("yedek/20261112-115456")) == "12.11.2026 11:54" and g.yedek_tarihi(Path("x")) == "x"


def test_way_back_refusals_change_nothing(g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def files(folder: Path) -> dict[str, bytes]:
        return {k: v for k, v in snapshot(folder).items() if not k.startswith("guncelleme_")}

    empty = tmp_path / "bos"
    empty.mkdir()
    assert update(g, empty, "--geri-al")[0] == 2 and "burası botun klasörü değil" in capsys.readouterr().out
    assert list(empty.iterdir()) == []

    bot = old_bot(tmp_path)
    before = files(bot)
    code, runner, started = update(g, bot, "--geri-al")                   # never updated: nothing to go back to
    assert code == 2 and started == [] and runner.calls == []
    assert "kurulu sürümden farklı bir yedek yok" in capsys.readouterr().out and files(bot) == before

    assert update(g, bot)[0] == 130
    installed = files(bot)
    capsys.readouterr()
    running = [(4321, "python.exe", r"C:\bot\venv\Scripts\python.exe -m copytrader run")]
    code, runner, started = update(g, bot, "--geri-al", processes=lambda: running)
    out = capsys.readouterr().out
    assert code == 2 and started == [] and runner.calls == [] and "Geri dönüş yapılmadı: bot çalışıyor (PID 4321)" in out
    assert files(bot) == installed and len(list((bot / "yedek").iterdir())) == 1

    code, runner, started = update(g, bot, "--geri-al", processes=lambda: None, answer="hayır")   # cannot tell, not confirmed
    assert code == 2 and files(bot) == installed

    # the check of the restored version reports a problem: it is shown, nothing is switched again, the bot starts
    code, runner, started = update(g, bot, "--geri-al", runner=Runner({"-m copytrader check": [(1, check_bad("fx"))]}))
    out = capsys.readouterr().out
    assert code == 130 and len(started) == 1 and "5. Ayarlar ve hesap bağlantıları kontrol edildi: HATA" in out
    assert "Sürüm yeniden değiştirilmedi" in out and "copytrader/eski_modul.py" in snapshot(bot)


def test_way_back_failure_or_ctrl_c_never_leaves_half_a_package(g: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                               capsys: pytest.CaptureFixture[str]) -> None:
    bot = old_bot(tmp_path)
    old = package_of(snapshot(bot))
    assert update(g, bot)[0] == 130
    new = package_of(snapshot(bot))
    real = g.geri_yukle
    calls: list[str] = []

    # 1. the old package cannot be put back (a file is held by another program): the present version stays whole
    def failing(hedef: Path, yedek: Path) -> None:
        calls.append(yedek.name)
        if len(calls) == 1:
            shutil.rmtree(hedef / "copytrader" / "brokers")              # ... after part of the work was done
            raise PermissionError(13, "dosya kullanımda")
        real(hedef, yedek)

    monkeypatch.setattr(g, "geri_yukle", failing)
    code, runner, started = update(g, bot, "--geri-al")
    out = capsys.readouterr().out
    assert code == 1 and started == [] and "4. Eski sürüm geri konulamadı: HATA" in out
    assert "Şimdiki sürüm yerinde bırakıldı" in out and "Bot başlatılmadı. Başlatmak için:" in out
    assert package_of(snapshot(bot)) == new and calls[1].endswith("-geri")

    # 2. Ctrl+C in the middle of the copy: the copy is finished first, then it stops (the bot is not started)
    calls.clear()

    def interrupted(hedef: Path, yedek: Path) -> None:
        calls.append(yedek.name)
        if len(calls) == 1:
            shutil.rmtree(hedef / "copytrader" / "brokers")
            raise KeyboardInterrupt
        real(hedef, yedek)

    monkeypatch.setattr(g, "geri_yukle", interrupted)
    code, runner, started = update(g, bot, "--geri-al")
    out = capsys.readouterr().out
    assert code == 130 and started == [] and runner.calls == [] and len(calls) == 2
    assert "Durduruldu (Ctrl+C). Eski sürüm kurulu; bot başlatılmadı." in out
    assert package_of(snapshot(bot)) == old


# ---- is the bot running? (pure logic; the Windows commands themselves cannot run here) ---------------------
def test_updater_recognises_the_bot_process_only(g: Any) -> None:
    rows = [
        (10, "python.exe", r"C:\Users\Administrator\Downloads\bot\venv\Scripts\python.exe -m copytrader run"),
        (11, "python.exe", r'"C:\Program Files\Python312\python.exe"  -m  copytrader  run --config config.yaml'),
        (12, "python3.12", "/opt/bot/.venv/bin/python -m copytrader run"),
        (13, "python.exe", r'"C:\bot\venv\Scripts\python.exe" "C:\bot\venv\Scripts\copytrader.exe" run'),
        (14, "python", "python -m copytrader.__main__ run"),
        (20, "python.exe", r"C:\bot\venv\Scripts\python.exe -m copytrader check"),
        (21, "python.exe", r"C:\bot\venv\Scripts\python.exe -m copytrader runner"),
        (22, "powershell.exe", "powershell -Command python -m copytrader run"),          # a shell carrying the text
        (23, "notepad.exe", r"notepad C:\bot\copytrader\run.txt"),
        (24, "python.exe", r"python.exe -c import glob,os,runpy;runpy.run_path('guncelle.py')"),
        (25, "python.exe", ""),
    ]
    assert [pid for pid, _name, _cmd in g.calisan_botlar(rows, kendi=0)] == [10, 11, 12, 13, 14]
    assert [pid for pid, _name, _cmd in g.calisan_botlar(rows, kendi=10)] == [11, 12, 13, 14]     # never itself


def test_updater_reads_the_windows_process_list_formats(g: Any) -> None:
    powershell = ("4|System|\r\n"
                  "5120|python.exe|\"C:\\bot\\venv\\Scripts\\python.exe\" -m copytrader run\r\n"
                  "6000|powershell.exe|powershell.exe -NoProfile -Command Get-CimInstance Win32_Process | x\r\n"
                  "bozuk satır\r\n")
    assert g.powershell_cikti_coz(powershell) == [
        (4, "System", ""), (5120, "python.exe", '"C:\\bot\\venv\\Scripts\\python.exe" -m copytrader run'),
        (6000, "powershell.exe", "powershell.exe -NoProfile -Command Get-CimInstance Win32_Process | x")]
    wmic = ("\r\nNode,CommandLine,Name,ProcessId\r\n"
            "VPS,,System,4\r\n"
            "VPS,\"C:\\bot\\venv\\Scripts\\python.exe\" -m copytrader run,python.exe,5120\r\n"
            "VPS,cmd /c echo a,b,c,cmd.exe,77\r\n")
    assert g.wmic_cikti_coz(wmic) == [(4, "System", ""),
                                      (5120, "python.exe", '"C:\\bot\\venv\\Scripts\\python.exe" -m copytrader run'),
                                      (77, "cmd.exe", "cmd /c echo a,b,c")]

    class Result:
        def __init__(self, stdout: bytes) -> None:
            self.stdout = stdout

    used: list[str] = []

    def both_work(komut: list[str], **kw: Any) -> Any:
        used.append(Path(komut[0]).name.lower())
        return Result(powershell.encode("utf-8"))

    assert g.calisan_botlar(g.surecler_windows(both_work) or [], kendi=0)[0][0] == 5120 and len(used) == 1
    assert "powershell" in used[0]

    def powershell_fails(komut: list[str], **kw: Any) -> Any:
        used.append(Path(komut[0]).name.lower())
        if "powershell" in used[-1]:
            raise FileNotFoundError("powershell")
        return Result(wmic.encode("cp1254"))

    used.clear()
    assert g.calisan_botlar(g.surecler_windows(powershell_fails) or [], kendi=0)[0][0] == 5120
    assert used[-1] == "wmic" and len(used) == 2

    def nothing_works(komut: list[str], **kw: Any) -> Any:
        raise OSError("yok")

    assert g.surecler_windows(nothing_works) is None                       # the owner is asked then
    assert g.surecler_windows(lambda komut, **kw: Result(b"")) is None     # empty answers are not "no bot"


def test_updater_reads_linux_proc_and_finds_the_data_folder(g: Any, tmp_path: Path) -> None:
    proc = tmp_path / "proc"
    for pid, argv in (("100", [b"/opt/bot/venv/bin/python", b"-m", b"copytrader", b"run"]),
                      ("101", [b"bash", b"-c", b"python -m copytrader run"]), ("102", [])):
        (proc / pid).mkdir(parents=True)
        (proc / pid / "cmdline").write_bytes(b"\x00".join(argv) + (b"\x00" if argv else b""))
    (proc / "self").mkdir()
    rows = g.surecler_proc(str(proc))
    assert sorted(rows) == [(100, "python", "/opt/bot/venv/bin/python -m copytrader run"),
                            (101, "bash", "bash -c python -m copytrader run")]
    assert [pid for pid, *_ in g.calisan_botlar(rows, kendi=0)] == [100]
    assert g.surecler_proc(str(tmp_path / "yok")) is None

    bot = tmp_path / "bot"
    bot.mkdir()
    assert g.veri_klasoru(bot) == bot / "data"                             # no config: the default
    (bot / "config.yaml").write_text("dry_run: false\ndata_dir: 'veri'   # yorum\n", encoding="utf-8")
    assert g.veri_klasoru(bot) == bot / "veri"
    (bot / "config.yaml").write_text("data_dir: ${VERI}\n", encoding="utf-8")
    assert g.veri_klasoru(bot) == bot / "data"
    (bot / "config.yaml").write_text(f"data_dir: {tmp_path / 'mutlak'}\n", encoding="utf-8")
    assert g.veri_klasoru(bot) == tmp_path / "mutlak"
    assert g.kilit_sahibi(bot / "data") is None


def test_updater_and_bot_agree_on_the_lock(g: Any, tmp_path: Path, sleeper: Any) -> None:
    data = tmp_path / "data"
    assert g.surec_durumu(os.getpid()) == cli._process_state(os.getpid())
    assert g.surec_durumu(sleeper.pid) == cli._process_state(sleeper.pid)
    write_lock(data, sleeper.pid)
    assert g.kilit_sahibi(data) == cli.lock_holder(data) and g.kilit_sahibi(data)["pid"] == sleeper.pid
    lock = cli.acquire_lock(tmp_path / "kendi")                            # what a running bot writes
    assert g.kilit_sahibi(tmp_path / "kendi") is None                      # ... is our own pid here
    cli.release_lock(lock)
    sleeper.kill()
    sleeper.wait()
    assert g.kilit_sahibi(data) is None and cli.lock_holder(data) is None


# ---- real subprocesses -------------------------------------------------------------------------------------
def test_updater_runs_a_command_and_shows_its_output_live(g: Any, tmp_path: Path) -> None:
    lines: list[str] = []
    code = g.komut_calistir([sys.executable, "-c", "import os, sys; print('ilk satır: ğüşiöç'); "
                             "print(os.getcwd()); print(os.environ['PYTHONIOENCODING']); sys.exit(3)"],
                            tmp_path, 60, lines.append)
    assert code == 3 and lines == ["ilk satır: ğüşiöç", str(tmp_path), "utf-8"]
    lines.clear()
    assert g.komut_calistir([sys.executable, "-c", "import time; print('başladı', flush=True); time.sleep(60)"],
                            tmp_path, 1.0, lines.append) == -2
    assert lines[0] == "başladı" and "bitmedi" in lines[-1]
    assert g.komut_calistir([str(tmp_path / "olmayan-program")], tmp_path, 5, lines.append) == -1


@pytest.mark.skipif(os.name != "posix", reason="process groups and SIGINT")
def test_updater_waits_for_the_bot_and_never_kills_it_on_ctrl_c(g: Any, tmp_path: Path) -> None:
    """Ctrl+C in the window reaches the updater and the bot alike. The bot must be left to shut down by itself
    (here: 1.5 s of 'closing positions'), and its exit code comes back."""
    marker = tmp_path / "kapandi.txt"
    bot = textwrap.dedent(f"""
        import pathlib, signal, sys, time
        def stop(*_):
            time.sleep(1.5)                                   # orderly shutdown takes a while
            pathlib.Path({str(marker)!r}).write_text("temiz kapanış")
            print("Durduruldu.")
            sys.exit(130)
        signal.signal(signal.SIGINT, stop)
        pathlib.Path({str(tmp_path / 'hazir.txt')!r}).write_text("x")
        time.sleep(60)
    """)
    harness = textwrap.dedent(f"""
        import importlib.util, sys
        sys.dont_write_bytecode = True
        spec = importlib.util.spec_from_file_location("guncelle", {str(REPO / 'guncelle.py')!r})
        g = importlib.util.module_from_spec(spec); spec.loader.exec_module(g)
        from pathlib import Path
        sys.exit(g.botu_baslat(Path({str(tmp_path)!r}), sys.executable, [sys.executable, "-c", {bot!r}]))
    """)
    proc = subprocess.Popen([sys.executable, "-c", harness], start_new_session=True, stdout=subprocess.PIPE, text=True)
    deadline = time.time() + 20
    while not (tmp_path / "hazir.txt").exists():
        assert time.time() < deadline and proc.poll() is None
        time.sleep(0.05)
    os.killpg(proc.pid, signal.SIGINT)                        # what the console does on Ctrl+C
    time.sleep(0.3)
    os.killpg(proc.pid, signal.SIGINT)                        # an impatient second press reaches the updater too
    out, _ = proc.communicate(timeout=30)
    assert proc.returncode == 130 and "Durduruldu." in out
    assert marker.read_text() == "temiz kapanış"              # the bot finished its own shutdown


# ---- the documented line, end to end -----------------------------------------------------------------------
def documented_line() -> str:
    doc = (REPO / "docs" / "GUNCELLEME.md").read_text(encoding="utf-8")
    found = re.findall(r'^\.\\venv\\Scripts\\python\.exe -c "([^"\n]+)"$', doc, flags=re.MULTILINE)
    assert len(found) == 1, found
    return found[0]


def test_the_documented_line_is_safe_to_paste_into_powershell() -> None:
    code = documented_line()
    assert not set(code) & set('$`"') and code.isascii() and "\\" not in code and len(code) < 400
    compile(code, "<satır>", "exec")
    doc = (REPO / "docs" / "GUNCELLEME.md").read_text(encoding="utf-8")
    for step in ("## 1. Yeni sürümü indirin", "## 2. Botu durdurun", "## 3. Aşağıdaki satırı"):
        assert step in doc
    assert "Durduruldu." in doc and "Ctrl+C" in doc
    assert "\n.\\venv\\Scripts\\python.exe guncelle.py --geri-al\n" in doc      # the way back, as the updater knows it


def test_the_guide_lists_every_setting_the_update_changes() -> None:
    """docs/GUNCELLEME.md tells the owner what the update does to config.yaml: no setting may be missing there."""
    m = migrate_mod()
    cfg = prod_config()
    changes, _notes = m.plan_changes(yaml.safe_load(PROD_YAML), cfg)
    assert len(changes) == N_CHANGES
    doc = (REPO / "docs" / "GUNCELLEME.md").read_text(encoding="utf-8")
    table = doc[doc.index("## Güncelleme `config.yaml` dosyasında neleri değiştirir"):doc.index("## Bir adım HATA verirse")]
    for change in changes:
        row = next((ln for ln in table.splitlines() if f"`{change.key}`" in ln), None)
        assert row is not None, change.key
        shown = "true" if change.value is True else str(change.value)
        assert f"| `{shown}` |" in row, (change.key, row)


def release_zip(dest: Path, top: str = "harmonic-bot-claude-telegram-crypto-automation-o6xo7l") -> None:
    """What the browser downloads: <top>/telegram-copy-trader/... of this source tree."""
    skip_dirs = {"__pycache__", "data", "venv", ".venv", "yedek", ".pytest_cache", ".git"}
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(f"{top}/app.py", "# deponun kökündeki başka bir dosya\n")
        for root, dirs, files in os.walk(new_tree()):
            dirs[:] = [d for d in dirs if d not in skip_dirs]
            for name in files:
                path = Path(root) / name
                if name in (".env", "config.yaml") or name.endswith((".pyc", ".session", ".log")):
                    continue
                archive.write(path, f"{top}/telegram-copy-trader/{path.relative_to(new_tree()).as_posix()}")


def paste(home: Path, bot: Path, *options: str, code: str | None = None, script: str | None = None) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ, HOME=str(home), USERPROFILE=str(home), TMPDIR=str(home / "tmp"), TEMP=str(home / "tmp"),
               TMP=str(home / "tmp"), PYTHONDONTWRITEBYTECODE="1", PYTHONIOENCODING="utf-8")
    env.pop("PYTHONPATH", None)
    for name in [k for k in env if k.upper().startswith("PIP_")]:
        env.pop(name)            # the optional cryptg step: offline here, never into this interpreter
    env.update(PIP_CONFIG_FILE=os.devnull, PIP_NO_INDEX="1", PIP_TARGET=str(home / "pip-target"))
    (home / "tmp").mkdir(exist_ok=True)
    command = [sys.executable, script, *options] if script else [sys.executable, "-c", code or documented_line(), *options]
    return subprocess.run(command, cwd=bot, env=env, capture_output=True, text=True, encoding="utf-8", timeout=300)


def test_the_documented_line_updates_a_server_like_folder(tmp_path: Path) -> None:
    """The server layout: the bot runs from the folder the first zip was extracted to, inside Downloads; an old
    zip lies next to the new one. The pasted line (its Python part, run with this interpreter) must pick the
    newest zip, extract it elsewhere, install it, and migrate the config with the NEW code."""
    home = tmp_path / "home"
    downloads = home / "Downloads"
    top = "harmonic-bot-claude-telegram-crypto-automation-o6xo7l"
    bot = old_bot(downloads / top)
    old_zip = downloads / f"{top}.zip"
    with zipfile.ZipFile(old_zip, "w") as archive:                         # an older download without the updater
        archive.writestr(f"{top}/telegram-copy-trader/copytrader/__main__.py", OLD_MAIN)
    past = time.time() - 5 * 86400
    os.utime(old_zip, (past, past))
    before = snapshot(bot)

    # 1. no new zip yet: the old one is recognised as old, nothing happens
    result = paste(home, bot, "--kontrol-yok", "--baslatma")
    assert result.returncode == 1 and "Bu zip eski: yenisini indirin" in result.stderr and snapshot(bot) == before

    # 2. the new download ("... (1).zip", because the old one is still there)
    release_zip(downloads / f"{top} (1).zip")
    result = paste(home, bot, "--kontrol-yok", "--baslatma")
    out = result.stdout
    assert result.returncode == 0, out + result.stderr
    steps = [ln for ln in out.splitlines() if re.match(r"\d\. ", ln)]
    assert [s[0] for s in steps] == list("123456789") and all(s.endswith(": TAMAM") for s in steps), out
    assert "binance: günlük zarar freni kapatıldı (daily_loss_limit_pct: 0)" in out           # real `copytrader migrate`
    after = snapshot(bot)
    assert {k: v for k, v in after.items() if k.startswith("copytrader/")} == {
        f"copytrader/{k}": v for k, v in package_files(new_tree() / "copytrader").items()}
    assert after["guncelle.py"] == (new_tree() / "guncelle.py").read_bytes()
    for name in OWNER_FILES:
        assert after[name] == before[name], name
    assert not (bot / "tests").exists() and not (bot / "app.py").exists()   # only what an update is meant to bring
    assert load_config(bot / "config.yaml").notifier.notify_on_execution is True
    assert sorted(p.name for p in downloads.iterdir()) == sorted([top, f"{top}.zip", f"{top} (1).zip"])   # extracted elsewhere
    assert len(list(bot.glob("guncelleme_*.log"))) == 1 and len(list((bot / "yedek").iterdir())) == 1

    # 3. the installed copy, run directly (the short form of the docs), does the same again; harmless
    config_after = (bot / "config.yaml").read_bytes()
    time.sleep(1.1)
    result = paste(home, bot, "--kontrol-yok", "--baslatma", script="guncelle.py")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Yeni sürüm: " + f"{top} (1).zip" in result.stdout and "zaten güncel" in result.stdout
    assert (bot / "config.yaml").read_bytes() == config_after and len(list((bot / "yedek").iterdir())) == 2

    # 4. the way back: the installed copy alone, no zip needed; code and config are what they were at the start
    for archive_file in downloads.glob("*.zip"):
        archive_file.unlink()
    time.sleep(1.1)
    result = paste(home, bot, "--geri-al", "--kontrol-yok", "--baslatma", script="guncelle.py")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "ESKİ SÜRÜME DÖNÜŞ" in result.stdout and "4. Eski sürüm ve o günkü config.yaml geri konuldu: TAMAM" in result.stdout
    back = snapshot(bot)
    assert {k: v for k, v in back.items() if k.startswith("copytrader/")} == {
        k: v for k, v in before.items() if k.startswith("copytrader/")}
    assert back["config.yaml"] == before["config.yaml"]
    for name in OWNER_FILES:
        assert back[name] == before[name], name


def test_the_documented_line_without_a_download_says_so(tmp_path: Path) -> None:
    home = tmp_path / "home"
    bot = old_bot(home / "Downloads" / "x")
    before = snapshot(bot)
    result = paste(home, bot)
    assert result.returncode == 1 and "Zip bulunamadi: once 1. adimdaki dosyayi indirin" in result.stderr
    assert snapshot(bot) == before


# ---- step 5b, optional: cryptg (the group copy's media); never a reason to stop or roll back an update ----------
CRYPTG_PIP = ("-m pip install --disable-pip-version-check cryptg==0.6.1 --only-binary=:all: --timeout 10 "
              "--retries 1")


def update_with_cryptg(g: Any, bot: Path, runner: Runner, *options: str) -> tuple[int, list[tuple[Path, str]]]:
    started: list[tuple[Path, str]] = []

    def start(hedef: Path, python: str) -> int:
        started.append((hedef, python))
        return 130

    code = g.main(["--hedef", str(bot), *options], calistir=runner, surecler=lambda: [], baslat=start,
                  girdi=lambda prompt: "", yeni_agac=new_tree(), ek_paket=g.cryptg_adimi)
    return code, started


def test_update_cryptg_is_installed_when_missing_and_a_failure_is_only_a_warning(
    g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bot = old_bot(tmp_path)
    runner = Runner({"-c import cryptg": [(1, ["ModuleNotFoundError: No module named 'cryptg'"])],
                     CRYPTG_PIP: [(1, ["ERROR: Could not find a version that satisfies the requirement cryptg==0.6.1"])]})
    code, started = update_with_cryptg(g, bot, runner)
    out = capsys.readouterr().out
    assert runner.calls[1:3] == ["-c import cryptg", CRYPTG_PIP]          # right after step 5, before migrate
    assert "UYARI: hızlı şifreleme paketi (cryptg) kurulamadı; güncelleme devam ediyor." in out
    assert code == 130 and len(started) == 1 and "ESKİ SÜRÜM GERİ YÜKLENDİ" not in out
    assert (bot / "copytrader" / "migrate.py").is_file()                  # the new version stays

    bot2 = old_bot(tmp_path / "b")
    runner = Runner({"-c import cryptg": [(1, [])], CRYPTG_PIP: [(0, ["Successfully installed cryptg-0.6.1"])]})
    code, _ = update_with_cryptg(g, bot2, runner)
    assert code == 130 and "Hızlı şifreleme paketi (cryptg) kuruldu." in capsys.readouterr().out


def test_update_cryptg_already_there_runs_no_pip_and_the_way_back_never_touches_it(
    g: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bot = old_bot(tmp_path)
    runner = Runner({"-c import cryptg": [(0, [])]})
    code, _ = update_with_cryptg(g, bot, runner)
    assert code == 130 and CRYPTG_PIP not in runner.calls and runner.calls.count("-c import cryptg") == 1
    runner = Runner()                                                     # (any cryptg command would fail the test)
    code, _ = update_with_cryptg(g, bot, runner, "--geri-al")
    assert code == 130 and not any("cryptg" in c for c in runner.calls)


def test_update_cryptg_with_a_hanging_package_index_is_cut_off_in_time(
    g: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import socket

    pytest.importorskip("pip", reason="this interpreter has no pip (the server's venv has; guncelle uses it)")
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(8)                                     # accepts connections and never answers
    port = server.getsockname()[1]
    monkeypatch.setattr(g, "CRYPTG_SURESI", 6.0)
    for name in [k for k in os.environ if k.upper().startswith("PIP_")]:
        monkeypatch.delenv(name)                         # hermetic: no pip config, no other index
    monkeypatch.setenv("PIP_CONFIG_FILE", os.devnull)
    monkeypatch.setenv("PIP_INDEX_URL", f"http://127.0.0.1:{port}/simple")
    monkeypatch.setenv("PIP_NO_CACHE_DIR", "1")
    monkeypatch.setenv("PIP_TARGET", str(tmp_path / "target"))      # never into this interpreter
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.setenv(name, "127.0.0.1,localhost")
    lines: list[str] = []
    log = SimpleNamespace(yaz=lambda text="": lines.append(text), dosyaya=lambda text: lines.append(text))

    def komut(*parts: str, sure: float = 900.0, ekranda: bool = True) -> tuple[int, list[str]]:
        if parts == ("-c", "import cryptg"):
            return 1, ["ModuleNotFoundError"]            # (not installed: whatever this machine has)
        out: list[str] = []
        return g.komut_calistir([sys.executable, *parts], tmp_path, sure, out.append), out   # the REAL pip

    started = time.monotonic()
    try:
        ok = g.cryptg_adimi(komut, log)
    finally:
        server.close()
    took = time.monotonic() - started
    assert ok is False and 5.5 < took < 30, took             # stopped by the bound (pip itself would wait on)
    assert "   | 6 saniye içinde bitmedi; durduruldu" in lines
    assert not (tmp_path / "target").exists() or not any((tmp_path / "target").iterdir())
    assert any("UYARI: hızlı şifreleme paketi (cryptg) kurulamadı" in line for line in lines)
