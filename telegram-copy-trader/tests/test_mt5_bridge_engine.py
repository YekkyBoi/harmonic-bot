"""The engine with the friend's tickmill account on type "mt5_bridge" (a REAL helper process on the fake terminal of
tests/mt5_fake_terminal.py) next to the owner's accounts (the fake venues of test_preflight2_engine): the optional
account trades, its outage never touches the owner's fx leg, a signal it missed is remembered for the catch-up, and
its open position is managed again once the helper is back."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from copytrader.brokers import mt5_bridge, mt5_helper
from copytrader.brokers.mt5_bridge import Mt5BridgeBroker
from copytrader.config import Mt5BridgeBrokerConfig
from copytrader.engine.orchestrator import Engine
from copytrader.models import ActionType, ManagementPlan, PositionStatus, Severity
from copytrader.storage.db import Database

from fakes import FakeNotifier, FakeParser
from mt5_fake_terminal import TERMINAL_COMPANY_ENV, TERMINAL_CONTROL_ENV, TERMINAL_FOLDER_ENV, TERMINAL_STATE_ENV
from test_preflight2_engine import (CHAT, FX, FX_QUOTES, PRICES, Env, PairOracle, Reference, Venue, fx_pair,
                                    prod_config, q)

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals drive the outage")
FOLDER = r"C:\Program Files\Tickmill MT5 Terminal"


@pytest.fixture
async def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setenv(mt5_helper.FAKE_ENV, "mt5_fake_terminal:make")
    monkeypatch.setenv(mt5_helper.FAKE_PATH_ENV, str(Path(__file__).resolve().parent))
    monkeypatch.setenv(TERMINAL_STATE_ENV, str(tmp_path / "terminal.pkl"))
    monkeypatch.setenv(TERMINAL_CONTROL_ENV, str(tmp_path / "control.json"))
    monkeypatch.setenv(TERMINAL_FOLDER_ENV, FOLDER)
    monkeypatch.setenv(TERMINAL_COMPANY_ENV, "Tickmill Ltd")
    monkeypatch.setattr(mt5_bridge, "PING_SEC", 0.1)
    monkeypatch.setattr(mt5_bridge, "RESTART_BACKOFF_SEC", (1.0,))       # a window in which a signal arrives
    cfg = prod_config()
    old = next(b for b in cfg.brokers if b.name == "tickmill")
    keep = old.model_dump(exclude={"type", "token", "account_id", "region", "magic", "slippage_points",
                                   "commission_per_lot", "terminal_path", "broker_company", "deviation_points",
                                   "filling"})
    bridge_cfg = Mt5BridgeBrokerConfig(**keep, type="mt5_bridge", terminal_path=FOLDER + r"\terminal64.exe",
                                       broker_company="Tickmill")
    cfg.brokers = [bridge_cfg if b.name == "tickmill" else b for b in cfg.brokers]
    by = {b.name: b for b in cfg.brokers}
    venues = {
        "binance": Venue(by["binance"], equity=46.9, currency="USDT"),
        "breakout": Venue(by["breakout"], equity=1_000_000.0, currency="USDT"),
        "fx": Venue(by["fx"], equity=10_000.0, currency="USD", qty_step=0.01),
    }
    venues["fx"].quotes.update(FX_QUOTES)
    for name in ("binance", "breakout"):
        venues[name].quotes.update({"BTC/USDT:USDT": q(73999.0, 74001.0)})
    tickmill = Mt5BridgeBroker(bridge_cfg, pin_dir=str(tmp_path))
    db = Database(":memory:")
    await db.open()
    notifier, parser = FakeNotifier(), FakeParser()
    engine = Engine(cfg, db, parser, [*venues.values(), tickmill], notifier, price_oracle=PairOracle(PRICES),
                    reference_prices=Reference({"BTC": 74000.0}))
    await engine.start(monitor=False)
    assert [b.name for b in engine.brokers] == ["binance", "breakout", "fx", "tickmill"]
    notifier.notifications.clear()
    e = Env(cfg, db, {**venues, "tickmill": tickmill}, parser, notifier, Reference({}), PairOracle(PRICES), engine)
    e.term = tmp_path                                                    # type: ignore[attr-defined]
    yield e
    await engine.stop(grace_sec=2.0)
    await db.close()


async def test_the_friends_account_trades_misses_during_an_outage_and_is_managed_after_it(env: Any) -> None:
    tickmill: Mt5BridgeBroker = env.brokers["tickmill"]
    await env.say(fx_pair("EURUSD"), FX, "EURUSD LONG 1R", timeout=15.0)
    (first,) = await env.trades()
    pos = await env.position(first.id, "tickmill")
    assert pos.status is PositionStatus.OPEN and pos.filled_quantity > 0
    assert len(env.fx.opened) == 1

    # the helper dies: the owner's fx leg of the next signal is not held up, tickmill is skipped and remembered
    assert tickmill._helper is not None and tickmill._helper.pid is not None
    os.kill(tickmill._helper.pid, signal.SIGKILL)
    await env.until(lambda: not tickmill._up, "the bridge sees the helper gone", timeout=5.0)
    started = asyncio.get_running_loop().time()
    await env.say(fx_pair("GBPUSD"), FX, "GBPUSD LONG 1R", timeout=5.0)
    assert asyncio.get_running_loop().time() - started < 3.0
    assert len(env.fx.opened) == 2
    second = (await env.trades())[1]
    assert "tickmill" not in {p.broker for p in await env.db.broker_positions(second.id)} or \
        (await env.position(second.id, "tickmill")).status is not PositionStatus.OPEN
    assert "tickmill" in env.engine._missed_brokers                     # for the catch-up once it is healthy
    assert any(t.startswith("🔌 <b>tickmill</b>: MT5 bağlantısı koptu") for t in env.texts())

    # back: the open position of the first trade is managed through the new helper
    await env.until(lambda: tickmill._up, "the helper is back", timeout=10.0)
    await env.until(lambda: any(t.startswith("✅ <b>tickmill</b>: MT5 bağlantısı yeniden kuruldu") for t in env.texts()),
                    "the back notice", timeout=5.0)
    env.notifier.notifications.clear()
    await env.engine.execute_management(ManagementPlan(
        plan_id="sl", action=ActionType.MOVE_SL, trade_id=first.id, new_stop_loss=1.1610, source_chat_id=CHAT,
        summary_tr="sl 1.1610", created_at=datetime.now(UTC)))
    moved = await env.position(first.id, "tickmill")
    assert moved.stop_loss == pytest.approx(1.1610) and moved.data["legs"][0]["sl"] == pytest.approx(1.1610)
    assert not env.texts(Severity.ERROR), env.texts()
