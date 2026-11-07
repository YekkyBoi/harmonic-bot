"""Integration fixes: 1000x contract price scaling and config env handling."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from copytrader.config import load_config
from copytrader.engine.orchestrator import Engine
from copytrader.models import (
    ActionType,
    EntryType,
    ManagementPlan,
    MarketType,
    Quote,
    Side,
    SymbolInfo,
    TakeProfit,
    TradePlan,
)
from copytrader.storage.db import Database

from fakes import FakeBroker, FakeNotifier, FakeParser, FakePriceOracle
from test_engine import make_config

NOW = datetime(2026, 11, 7, 17, 0, tzinfo=timezone.utc)


class ThousandXBroker(FakeBroker):
    """Venue that lists PEPE only as the 1000PEPE contract (prices 1000x the signal's)."""

    async def resolve_symbol(self, base_asset, quote_asset=None, market_type=None, raw_symbol=None):
        self.calls.append(("resolve_symbol", (base_asset, quote_asset, market_type, raw_symbol)))
        return SymbolInfo(broker_symbol="1000PEPE/USDT:USDT", base_asset=base_asset.upper(), quote_asset="USDT",
                          qty_step=1.0, min_qty=1.0, price_tick=0.000001,
                          extra={"price_multiplier": 1000.0, "market_base": "1000PEPE"})


async def test_1000x_contract_prices_are_scaled_for_open_and_manage() -> None:
    db = Database(":memory:")
    await db.open()
    broker = ThousandXBroker("binance", equity=10_000.0, qty_step=1.0,
                             quote=Quote(bid=0.010000, ask=0.010010, last=0.010005))
    engine = Engine(make_config(), db, FakeParser(), [broker], FakeNotifier(),
                    price_oracle=FakePriceOracle({"PEPE": 0.00001}))
    await engine.start(monitor=False)
    try:
        plan = TradePlan(
            plan_id="pepe1", base_asset="PEPE", quote_asset="USDT", market_type=MarketType.PERP,
            side=Side.LONG, entry_type=EntryType.MARKET, entry_price=0.00001000, stop_loss=0.00000950,
            take_profits=[TakeProfit(price=0.00001100, close_fraction=1.0)], risk_r=1.0, risk_pct=2.0,
            source_chat_id=1, source_message_ids=[1], created_at=NOW,
        )
        await engine.execute_open(plan)
        req = broker.opened[0]
        assert req.stop_loss == pytest.approx(0.0095)
        assert req.take_profits[0].price == pytest.approx(0.011)
        sized = broker.sized[0]
        assert sized["entry"] == pytest.approx(0.010010) and sized["stop"] == pytest.approx(0.0095)
        # risk stays 2% of equity: qty * |entry - stop| ~= 200
        assert req.quantity * (0.010010 - 0.0095) == pytest.approx(200.0, rel=0.01)

        trade = (await db.active_trades())[0]
        positions = await db.broker_positions(trade.id)
        assert positions[0].data["price_multiplier"] == 1000.0

        await engine.execute_management(ManagementPlan(
            plan_id="m1", action=ActionType.MOVE_SL, trade_id=trade.id, new_stop_loss=0.00000990,
            source_chat_id=1, created_at=NOW,
        ))
        (_, new_sl), = broker.calls_of("update_stop_loss")
        assert new_sl == pytest.approx(0.0099)
    finally:
        await engine.stop()
        await db.close()


def test_env_placeholders_keep_secrets_verbatim(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CT_SECRET", "#0123yes")
    monkeypatch.setenv("CT_API_ID", "12345")
    monkeypatch.setenv("CT_PASSWORD", "000123")
    monkeypatch.delenv("CT_EMPTY", raising=False)
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        "telegram:\n"
        "  api_id: ${CT_API_ID}\n"
        "  api_hash: abc\n"
        "  source_chats: [-1001]\n"
        "notifier:\n"
        "  bot_token: '1:x'\n"
        "  owner_user_id: 5\n"
        "llm:\n"
        "  api_key: ${CT_EMPTY:-}\n"
        "brokers:\n"
        "  - name: bn\n"
        "    type: ccxt\n"
        "    exchange: binanceusdm\n"
        "    api_key: ${CT_PASSWORD}\n"
        "    secret: ${CT_SECRET}\n",
        encoding="utf-8",
    )
    cfg = load_config(cfg_file)
    assert cfg.telegram.api_id == 12345
    assert cfg.brokers[0].secret == "#0123yes"
    assert cfg.brokers[0].api_key == "000123"
    assert cfg.llm.api_key is None


def test_spot_market_type_is_rejected_at_load(tmp_path: Path) -> None:
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        "telegram: {api_id: 1, api_hash: a, source_chats: [-1]}\n"
        "notifier: {bot_token: 't', owner_user_id: 1}\n"
        "brokers:\n  - {name: s, type: ccxt, exchange: binance, market_type: spot}\n",
        encoding="utf-8",
    )
    with pytest.raises(Exception):
        load_config(cfg_file)
