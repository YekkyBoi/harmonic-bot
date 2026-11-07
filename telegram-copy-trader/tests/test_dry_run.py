"""DryRunBroker never touches the venue for writes; registry wiring."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from copytrader.brokers.base import Broker, BrokerError
from copytrader.brokers.ccxt_broker import CcxtBroker
from copytrader.brokers.dry_run import DryRunBroker
from copytrader.brokers.paper_broker import PaperBroker
from copytrader.brokers.registry import create_broker
from copytrader.config import CcxtBrokerConfig, Mt5BrokerConfig, PaperBrokerConfig, RiskConfig
from copytrader.models import (
    BrokerPosition,
    EntryType,
    MarketType,
    OpenRequest,
    PositionStatus,
    Quote,
    Side,
    SizingResult,
    SymbolInfo,
    TakeProfit,
    TradePlan,
)

MIN = 60_000
T0 = 1_700_000_040_000


class FakeInner(Broker):
    """Real-adapter stand-in: reads answer, writes explode."""

    kind = "ccxt"

    def __init__(self) -> None:
        super().__init__(SimpleNamespace(name="binance-main", allowed_assets=["ETH"]))
        self.quote = Quote(bid=2232.80, ask=2232.85, last=2232.85)
        self.reads: list[str] = []
        self.candles: list[list[float]] = []
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    async def get_equity(self) -> float:
        self.reads.append("equity")
        return 12_345.0

    async def get_free_margin(self) -> float:
        self.reads.append("free")
        return 10_000.0

    async def account_currency(self) -> str:
        return "USDT"

    async def resolve_symbol(self, base_asset: str, quote_asset: str | None = None,
                             market_type: MarketType | None = None, raw_symbol: str | None = None) -> SymbolInfo | None:
        self.reads.append("resolve")
        return SymbolInfo(broker_symbol="ETH/USDT:USDT", base_asset="ETH", quote_asset="USDT", qty_step=0.001,
                          min_qty=0.001, price_tick=0.01, extra={"taker_fee": 0.0004})

    async def get_quote(self, symbol: SymbolInfo) -> Quote:
        self.reads.append("quote")
        return self.quote

    async def fetch_ohlcv_1m(self, broker_symbol: str, since_ms: int) -> list[list[float]]:
        self.reads.append("candles")
        return [c for c in self.candles if c[0] >= since_ms]

    async def size_position(self, symbol: SymbolInfo, side: Side, entry_price: float, stop_loss: float,
                            risk_amount: float) -> SizingResult:
        self.reads.append("size")
        return SizingResult(quantity=7.0, risk_amount=risk_amount, est_loss_at_sl=190.0, notional=15_000.0, leverage=10)

    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        raise AssertionError("inner.open_position must not be called in dry-run")

    async def update_stop_loss(self, pos: BrokerPosition, new_stop_loss: float) -> BrokerPosition:
        raise AssertionError("inner.update_stop_loss must not be called in dry-run")

    async def update_take_profits(self, pos: BrokerPosition, take_profits: list[TakeProfit]) -> BrokerPosition:
        raise AssertionError("inner.update_take_profits must not be called in dry-run")

    async def close_position(self, pos: BrokerPosition, fraction: float = 1.0) -> BrokerPosition:
        raise AssertionError("inner.close_position must not be called in dry-run")

    async def cancel_entry(self, pos: BrokerPosition) -> BrokerPosition:
        raise AssertionError("inner.cancel_entry must not be called in dry-run")

    async def sync_position(self, pos: BrokerPosition) -> BrokerPosition:
        raise AssertionError("inner.sync_position must not be called in dry-run")


def _request(info: SymbolInfo, *, entry_type: EntryType = EntryType.MARKET, limit: float | None = None,
             tps: list[TakeProfit] | None = None) -> OpenRequest:
    tps = tps if tps is not None else [TakeProfit(price=2260.0, close_fraction=0.5),
                                       TakeProfit(price=2310.6, close_fraction=0.5)]
    plan = TradePlan(plan_id="p", base_asset="ETH", side=Side.LONG, entry_type=entry_type, entry_price=limit or 2232.18,
                     stop_loss=2205.11, take_profits=tps, risk_r=1.0, risk_pct=2.0, source_chat_id=1,
                     created_at=datetime(2026, 11, 7, tzinfo=UTC))
    return OpenRequest(plan=plan, symbol=info, quantity=2.0, entry_type=entry_type, entry_price=limit,
                       stop_loss=2205.11, take_profits=tps, leverage=10, client_tag="ctdry1")


async def test_reads_delegate_and_writes_are_simulated(tmp_path: Path) -> None:
    inner = FakeInner()
    clock = SimpleNamespace(now=T0)
    dry = DryRunBroker(inner, str(tmp_path), clock_ms=lambda: clock.now)
    await dry.connect()
    assert inner.connected and dry.kind == "dry-run:ccxt" and dry.name == "binance-main"
    assert await dry.get_equity() == 12_345.0 and await dry.get_free_margin() == 10_000.0
    assert await dry.account_currency() == "USDT"
    assert dry.accepts_asset("eth") and not dry.accepts_asset("BTC")
    info = await dry.resolve_symbol("ETH")
    assert info is not None
    sizing = await dry.size_position(info, Side.LONG, 2232.18, 2205.11, 246.9)
    assert sizing.quantity == 7.0

    pos = await dry.open_position(_request(info))
    assert pos.dry_run is True and pos.status is PositionStatus.OPEN and pos.broker == "binance-main"
    assert pos.avg_entry_price == 2232.85                   # filled at the REAL quote
    assert pos.data["fees"] == pytest.approx(0.0004 * 2232.85 * 2.0)   # venue taker fee from SymbolInfo

    pos = await dry.update_stop_loss(pos, 2232.0)
    pos = await dry.update_take_profits(pos, [TakeProfit(price=2300.0)])
    start = (T0 // MIN + 1) * MIN
    inner.candles = [[start, 2233.0, 2240.0, 2232.5, 2238.0, 1.0]]
    clock.now = start + MIN + 1
    pos = await dry.sync_position(pos)
    assert "candles" in inner.reads and pos.status is PositionStatus.OPEN
    pos = await dry.close_position(pos, 0.5)
    assert pos.remaining_quantity == pytest.approx(1.0)
    pos = await dry.cancel_entry(pos)                       # open -> behaves like a full close
    assert pos.status is PositionStatus.CLOSED
    assert (tmp_path / "dryrun_binance-main.json").is_file()


async def test_dry_run_limit_and_stop(tmp_path: Path) -> None:
    inner = FakeInner()
    clock = SimpleNamespace(now=T0)
    dry = DryRunBroker(inner, str(tmp_path), clock_ms=lambda: clock.now)
    info = await dry.resolve_symbol("ETH")
    assert info is not None
    pos = await dry.open_position(_request(info, entry_type=EntryType.LIMIT, limit=2220.0))
    assert pos.status is PositionStatus.PENDING
    inner.quote = Quote(bid=2219.0, ask=2219.05, last=2219.0)
    pos = await dry.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.avg_entry_price == 2220.0
    inner.quote = Quote(bid=2204.0, ask=2204.05, last=2204.0)
    pos = await dry.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED and pos.data["closed_reason"] == "stop"
    assert pos.realized_pnl is not None and pos.realized_pnl < 0


async def test_dry_run_refuses_real_positions(tmp_path: Path) -> None:
    dry = DryRunBroker(FakeInner(), str(tmp_path))
    real = BrokerPosition(broker="binance-main", broker_symbol="ETH/USDT:USDT", side=Side.LONG,
                          status=PositionStatus.OPEN, quantity=1.0, data={"sl_order_id": "1"})
    assert await dry.sync_position(real) is real
    with pytest.raises(BrokerError):
        await dry.close_position(real)


async def test_risk_config_reaches_inner(tmp_path: Path) -> None:
    inner = FakeInner()
    dry = DryRunBroker(inner, str(tmp_path))
    rc = RiskConfig(fee_rate=0.001)
    dry.set_risk_config(rc)
    assert inner.risk_config is rc and dry.risk_config is rc


# --------------------------------------------------------------------------- #
# registry
# --------------------------------------------------------------------------- #
def _app(tmp_path: Path) -> Any:
    return SimpleNamespace(data_dir=str(tmp_path))


def test_registry_ccxt_dry_run_wraps(tmp_path: Path) -> None:
    cfg = CcxtBrokerConfig(type="ccxt", name="bn", exchange="binanceusdm")
    wrapped = create_broker(cfg, dry_run=True, app_config=_app(tmp_path))
    assert isinstance(wrapped, DryRunBroker) and isinstance(wrapped.inner, CcxtBroker)
    assert wrapped.kind == "dry-run:ccxt"
    real = create_broker(cfg, dry_run=False, app_config=_app(tmp_path))
    assert isinstance(real, CcxtBroker)


def test_registry_paper_is_never_wrapped(tmp_path: Path) -> None:
    cfg = PaperBrokerConfig(type="paper", name="pp")
    broker = create_broker(cfg, dry_run=True, app_config=_app(tmp_path))
    assert isinstance(broker, PaperBroker)
    assert broker.sim.state_path == tmp_path / "paper_pp.json"


def test_registry_missing_optional_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "copytrader.brokers.mt5_broker", None)   # import -> ImportError
    cfg = Mt5BrokerConfig(type="mt5", name="mt5", login=1, password="x", server="s")
    with pytest.raises(BrokerError):
        create_broker(cfg, dry_run=False, app_config=_app(tmp_path))
