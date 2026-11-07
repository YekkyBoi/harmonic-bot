"""PaperBroker with an injected price source (no network)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from ccxt.base.decimal_to_precision import TICK_SIZE

from copytrader.brokers.base import BrokerError
from copytrader.brokers.paper_broker import PaperBroker
from copytrader.config import PaperBrokerConfig, RiskConfig
from copytrader.models import (
    BrokerPosition,
    EntryType,
    MarketType,
    OpenRequest,
    PositionStatus,
    Side,
    TakeProfit,
    TradePlan,
)

SYM = "ETH/USDT:USDT"
T0 = 1_700_000_040_000          # 40 s into a minute
MIN = 60_000


class FakePrices:
    def __init__(self) -> None:
        self.id = "binanceusdm"
        self.precisionMode = TICK_SIZE
        self.markets = {
            SYM: {"id": "ETHUSDT", "symbol": SYM, "base": "ETH", "quote": "USDT", "settle": "USDT", "type": "swap",
                  "swap": True, "spot": False, "linear": True, "active": True, "contractSize": 1.0, "taker": 0.0005,
                  "precision": {"amount": 0.001, "price": 0.01},
                  "limits": {"amount": {"min": 0.001, "max": 10000.0}, "cost": {"min": 5.0}, "leverage": {"max": 100}}},
            "1000PEPE/USDT:USDT": {"id": "1000PEPEUSDT", "symbol": "1000PEPE/USDT:USDT", "base": "1000PEPE",
                                   "quote": "USDT", "settle": "USDT", "type": "swap", "swap": True, "linear": True,
                                   "active": True, "contractSize": 1.0, "precision": {"amount": 1, "price": 1e-7},
                                   "limits": {"amount": {"min": 1}, "cost": {"min": 5.0}}},
        }
        self.ticker = {"bid": 2232.80, "ask": 2232.85, "last": 2232.85}
        self.candles: list[list[float]] = []
        self.loaded = 0
        self.closed = False

    async def load_markets(self) -> dict[str, Any]:
        self.loaded += 1
        return self.markets

    async def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        return dict(self.ticker)

    async def fetch_ohlcv(self, symbol: str, timeframe: str = "1m", since: int | None = None,
                          limit: int | None = None) -> list[list[float]]:
        return [c for c in self.candles if since is None or c[0] >= since]

    async def close(self) -> None:
        self.closed = True


class Clock:
    def __init__(self, now: int = T0) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now


def make(tmp_path: Path, prices: FakePrices | None = None, clock: Clock | None = None,
         name: str = "paper1") -> tuple[PaperBroker, FakePrices, Clock]:
    prices = prices or FakePrices()
    clock = clock or Clock()
    cfg = PaperBrokerConfig(type="paper", name=name, starting_balance=10_000.0, fee_rate=0.0005)
    broker = PaperBroker(cfg, str(tmp_path), price_source=prices, clock_ms=clock)
    broker.set_risk_config(RiskConfig())
    return broker, prices, clock


def plan(side: Side = Side.LONG, sl: float = 2205.11, tps: list[TakeProfit] | None = None,
         entry_type: EntryType = EntryType.MARKET, entry: float = 2232.18) -> TradePlan:
    return TradePlan(
        plan_id="p", base_asset="ETH", side=side, entry_type=entry_type, entry_price=entry, stop_loss=sl,
        take_profits=tps if tps is not None else [TakeProfit(price=2310.60)], risk_r=1.0, risk_pct=2.0,
        source_chat_id=1, created_at=datetime(2026, 11, 7, tzinfo=UTC),
    )


async def open_pos(broker: PaperBroker, *, qty: float = 2.0, entry_type: EntryType = EntryType.MARKET,
                   limit: float | None = None, tps: list[TakeProfit] | None = None,
                   side: Side = Side.LONG, sl: float = 2205.11) -> BrokerPosition:
    info = await broker.resolve_symbol("ETH", "USDT", MarketType.PERP, "ETHUSDT.P")
    assert info is not None
    p = plan(side, sl, tps, entry_type, limit or 2232.18)
    req = OpenRequest(plan=p, symbol=info, quantity=qty, entry_type=entry_type, entry_price=limit,
                      stop_loss=sl, take_profits=p.take_profits, leverage=None, client_tag="ctpaper1")
    return await broker.open_position(req)


def candle(ts: int, o: float, h: float, low: float, c: float) -> list[float]:
    return [ts, o, h, low, c, 1.0]


# --------------------------------------------------------------------------- #
async def test_resolve_and_size(tmp_path: Path) -> None:
    broker, prices, _ = make(tmp_path)
    await broker.connect()
    info = await broker.resolve_symbol("PEPE")
    assert info is not None and info.broker_symbol == "1000PEPE/USDT:USDT" and info.extra["price_multiplier"] == 1000
    eth = await broker.resolve_symbol("ETH")
    assert eth is not None
    res = await broker.size_position(eth, Side.LONG, 2232.18, 2205.11, 200.0)
    assert res.est_loss_at_sl <= 200.0 and res.quantity > 6.8
    assert res.leverage is not None and res.leverage >= 1
    assert prices.loaded == 1                                  # markets loaded lazily, once


async def test_market_fill_fee_and_equity(tmp_path: Path) -> None:
    broker, prices, _ = make(tmp_path)
    await broker.connect()
    pos = await open_pos(broker)
    assert pos.status is PositionStatus.OPEN and pos.dry_run is False
    assert pos.avg_entry_price == 2232.85                     # buy at ask
    fee = 0.0005 * 2232.85 * 2.0
    assert await broker.get_free_margin() == pytest.approx(10_000.0 - fee + min(0.0, (2232.80 - 2232.85) * 2))
    prices.ticker = {"bid": 2250.0, "ask": 2250.05, "last": 2250.0}
    assert await broker.get_equity() == pytest.approx(10_000.0 - fee + (2250.0 - 2232.85) * 2.0)


async def test_stop_wrong_side_refused(tmp_path: Path) -> None:
    broker, prices, _ = make(tmp_path)
    prices.ticker = {"bid": 2200.0, "ask": 2200.05, "last": 2200.0}
    with pytest.raises(BrokerError):
        await open_pos(broker)


async def test_limit_fills_when_candle_crosses(tmp_path: Path) -> None:
    broker, prices, clock = make(tmp_path)
    pos = await open_pos(broker, entry_type=EntryType.LIMIT, limit=2220.0)
    assert pos.status is PositionStatus.PENDING
    start = (T0 // MIN + 1) * MIN
    prices.candles = [candle(start, 2232, 2235, 2225, 2226), candle(start + MIN, 2226, 2227, 2218, 2221)]
    clock.now = start + 2 * MIN + 5_000
    prices.ticker = {"bid": 2221.0, "ask": 2221.05, "last": 2221.0}
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.avg_entry_price == 2220.0
    assert pos.data["opened_at"] == start + MIN


async def test_marketable_limit_fills_immediately(tmp_path: Path) -> None:
    broker, _, _ = make(tmp_path)
    pos = await open_pos(broker, entry_type=EntryType.LIMIT, limit=2240.0)
    assert pos.status is PositionStatus.OPEN and pos.avg_entry_price == 2232.85


async def test_sl_first_when_sl_and_tp_in_same_candle(tmp_path: Path) -> None:
    broker, prices, clock = make(tmp_path)
    pos = await open_pos(broker)
    start = (T0 // MIN + 1) * MIN
    prices.candles = [candle(start, 2232, 2320, 2200, 2300)]      # touches both 2205.11 and 2310.60
    clock.now = start + MIN + 1
    prices.ticker = {"bid": 2300.0, "ask": 2300.05, "last": 2300.0}
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_reason"] == "stop"
    expected = (2205.11 - 2232.85) * 2 - 0.0005 * 2232.85 * 2 - 0.0005 * 2205.11 * 2
    assert pos.realized_pnl == pytest.approx(expected)
    assert broker.sim.balance == pytest.approx(10_000.0 + expected)


async def test_gap_through_stop_fills_at_open(tmp_path: Path) -> None:
    broker, prices, clock = make(tmp_path)
    pos = await open_pos(broker, tps=[])
    start = (T0 // MIN + 1) * MIN
    prices.candles = [candle(start, 2190, 2195, 2180, 2185)]
    clock.now = start + MIN + 1
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["exits"][0]["price"] == 2190


async def test_partial_tps_and_pnl(tmp_path: Path) -> None:
    broker, prices, clock = make(tmp_path)
    tps = [TakeProfit(price=2260.0, close_fraction=0.5), TakeProfit(price=2310.6, close_fraction=0.5)]
    pos = await open_pos(broker, tps=tps)
    start = (T0 // MIN + 1) * MIN
    prices.candles = [candle(start, 2233, 2262, 2230, 2255)]
    clock.now = start + MIN + 1
    prices.ticker = {"bid": 2255.0, "ask": 2255.05, "last": 2255.0}
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(1.0)
    assert [t["status"] for t in pos.data["tps"]] == ["filled", "open"]

    prices.candles.append(candle(start + MIN, 2255, 2312, 2250, 2311))
    clock.now = start + 2 * MIN + 1
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED
    gross = (2260.0 - 2232.85) * 1.0 + (2310.6 - 2232.85) * 1.0
    fees = 0.0005 * (2232.85 * 2 + 2260.0 + 2310.6)
    assert pos.realized_pnl == pytest.approx(gross - fees)
    assert broker.sim.balance == pytest.approx(10_000.0 + gross - fees)


async def test_quote_fallback_without_candles(tmp_path: Path) -> None:
    broker, prices, _ = make(tmp_path)
    pos = await open_pos(broker)
    prices.ticker = {"bid": 2311.0, "ask": 2311.05, "last": 2311.0}   # no candles at all
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_reason"] == "take_profit"


async def test_in_progress_candle_is_not_used(tmp_path: Path) -> None:
    broker, prices, clock = make(tmp_path)
    pos = await open_pos(broker)
    start = (T0 // MIN + 1) * MIN
    prices.candles = [candle(start, 2232, 2235, 2200, 2230)]          # still forming
    clock.now = start + 30_000
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN


async def test_update_sl_tp_and_close_half(tmp_path: Path) -> None:
    broker, _, _ = make(tmp_path)
    pos = await open_pos(broker)
    with pytest.raises(BrokerError):
        await broker.update_stop_loss(pos, 2240.0)                    # above current bid
    pos = await broker.update_stop_loss(pos, 2220.004)
    assert pos.stop_loss == 2220.0
    pos = await broker.update_take_profits(pos, [TakeProfit(price=2300.0, close_fraction=0.5),
                                                 TakeProfit(price=2350.0, close_fraction=0.5)])
    assert [t["qty"] for t in pos.data["tps"]] == [1.0, 1.0]
    pos = await broker.close_position(pos, 0.5)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(1.0)
    assert sum(t["qty"] for t in pos.data["tps"] if t["status"] == "open") == pytest.approx(1.0)
    pos = await broker.close_position(pos, 1.0)
    assert pos.status is PositionStatus.CLOSED


async def test_cancel_pending(tmp_path: Path) -> None:
    broker, _, _ = make(tmp_path)
    pos = await open_pos(broker, entry_type=EntryType.LIMIT, limit=2200.0, sl=2190.0)
    pos = await broker.cancel_entry(pos)
    assert pos.status is PositionStatus.CANCELLED


async def test_state_persisted(tmp_path: Path) -> None:
    broker, prices, clock = make(tmp_path)
    await broker.connect()
    pos = await open_pos(broker)
    fee = 0.0005 * 2232.85 * 2.0
    path = tmp_path / "paper_paper1.json"
    state = json.loads(path.read_text())
    assert state["balance"] == pytest.approx(10_000.0 - fee)
    assert pos.data["paper_id"] in state["positions"]

    again, _, _ = make(tmp_path, prices=prices, clock=clock)
    await again.connect()
    assert again.sim.balance == pytest.approx(10_000.0 - fee)
    prices.ticker = {"bid": 2242.85, "ask": 2242.9, "last": 2242.85}
    assert await again.get_equity() == pytest.approx(10_000.0 - fee + 20.0)   # open position survives restart
    pos = await again.close_position(pos, 1.0)
    assert json.loads(path.read_text())["positions"] == {}
