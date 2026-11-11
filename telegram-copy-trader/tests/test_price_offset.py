"""Price-offset brokers: a broker whose crypto CFD price differs from the exchange the signal charts come
from (``reference_exchange``) gets the signal levels shifted by the live price difference."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from copytrader.config import AppConfig, NotifierConfig, TelegramConfig, load_config
from copytrader.engine.orchestrator import Engine, filled_tp_prices, shift_price, unshift_price
from copytrader.engine.prices import ReferencePriceOracle
from copytrader.models import (
    ActionType,
    BrokerPosition,
    EntryType,
    MarketType,
    PositionStatus,
    ParsedAction,
    Quote,
    Severity,
    Side,
    TakeProfit,
)
from copytrader.storage.db import Database

from test_engine import CHAT, SENDER, action, add_msg, result
from fakes import FakeBroker, FakeNotifier, FakeParser, FakePriceOracle

ROOT = Path(__file__).resolve().parents[1]


class FakeReference:
    """mid(exchange_id, base) -> scripted value (None = outage)."""

    def __init__(self, prices: dict[str, float | None]) -> None:
        self.prices = prices
        self.calls: list[tuple[str, str]] = []

    async def mid(self, exchange_id: str, base_asset: str, *, fresh: bool = False) -> float | None:
        self.calls.append((exchange_id, base_asset))
        return self.prices.get(base_asset.upper())


def btc_short(**kw: Any):
    data: dict[str, Any] = dict(
        action=ActionType.OPEN, symbol="BTCUSDT.P", base_asset="BTC", quote_asset="USDT",
        market_type=MarketType.PERP, side=Side.SHORT, entry_type=EntryType.LIMIT, entry_price=73200.0,
        stop_loss=73700.0, take_profits=[TakeProfit(price=72000.0)], risk_r=1.0, used_message_ids=[501, 502],
        confidence=0.95, summary_tr="BTC SHORT limit 73200, SL 73700, TP 72000, risk 1R",
    )
    data.update(kw)
    return ParsedAction(**data)


def eur_long(**kw: Any):
    data: dict[str, Any] = dict(
        action=ActionType.OPEN, symbol="EURUSD", base_asset="EUR", quote_asset="USD", market_type=MarketType.FOREX,
        side=Side.LONG, entry_type=EntryType.LIMIT, entry_price=1.0840, stop_loss=1.0800,
        take_profits=[TakeProfit(price=1.0950)], risk_r=1.0, used_message_ids=[501, 502], confidence=0.95,
        summary_tr="EURUSD LONG limit 1.0840",
    )
    data.update(kw)
    return btc_short(**data)


class Env:
    def __init__(self, cfg: AppConfig, db: Database, parser: FakeParser, broker: FakeBroker,
                 notifier: FakeNotifier, ref: FakeReference, engine: Engine) -> None:
        self.cfg, self.db, self.parser, self.broker = cfg, db, parser, broker
        self.notifier, self.ref, self.engine = notifier, ref, engine

    async def signal(self) -> None:
        await add_msg(self.db, 501, "btc short")
        await add_msg(self.db, 502, "1R")
        await self.engine.on_messages_ready(CHAT, SENDER)


async def make_env(db: Database, *, reference: str | None = "binanceusdm", mode: str = "additive",
                   ref_price: float | None = 73200.0, max_pct: float = 1.0) -> Env:
    cfg = AppConfig(
        telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[CHAT]),
        notifier=NotifierConfig(bot_token="123:abc", owner_user_id=42),
    )
    cfg.monitor_interval_sec = 3600.0
    cfg.notifier.notify_on_execution = True
    broker = FakeBroker("tickmill", equity=10_000.0, quote=Quote(bid=73399.9, ask=73400.1, last=73400.0))
    broker.quotes["EUR"] = Quote(bid=1.0850, ask=1.0851, last=1.08505)
    broker.config.reference_exchange = reference
    broker.config.offset_mode = mode          # type: ignore[assignment]
    broker.config.max_offset_pct = max_pct
    parser, notifier = FakeParser(), FakeNotifier()
    ref = FakeReference({"BTC": ref_price, "EUR": 1.0})
    engine = Engine(cfg, db, parser, [broker], notifier,
                    price_oracle=FakePriceOracle({"BTC": 73200.0, "EUR": 1.08505}), reference_prices=ref)
    await engine.start(monitor=False)
    notifier.notifications.clear()
    return Env(cfg, db, parser, broker, notifier, ref, engine)


@pytest.fixture
async def db():
    database = Database(":memory:")
    await database.open()
    yield database
    await database.close()


# --------------------------------------------------------------------------- #
# Opening
# --------------------------------------------------------------------------- #
async def test_additive_shift_applied_to_entry_sl_tps_and_sizing(db: Database) -> None:
    env = await make_env(db)
    env.parser.push(result(btc_short()))
    await env.signal()
    req = env.broker.opened[0]
    assert req.entry_price == pytest.approx(73400.0)
    assert req.stop_loss == pytest.approx(73900.0)
    assert [tp.price for tp in req.take_profits] == [pytest.approx(72200.0)]
    sized = env.broker.sized[0]
    assert sized["entry"] == pytest.approx(73400.0) and sized["stop"] == pytest.approx(73900.0)
    # 2% of 10000 = 200 at risk; qty * |entry - stop| (shifted) equals the risk
    assert req.quantity * abs(sized["entry"] - sized["stop"]) == pytest.approx(sized["risk_amount"], rel=1e-3)
    trade = (await env.db.active_trades())[0]
    assert trade.plan.entry_price == 73200.0 and trade.plan.stop_loss == 73700.0   # original signal kept
    assert trade.plan.take_profits[0].price == 72000.0
    pos = (await env.db.broker_positions(trade.id))[0]
    assert pos.data["price_offset"]["mode"] == "additive"
    assert pos.data["price_offset"]["value"] == pytest.approx(200.0)
    infos = env.notifier.texts(Severity.INFO)
    assert infos and "fark +200.0 uygulandı" in infos[0]
    assert env.ref.calls == [("binanceusdm", "BTC")]


async def test_ratio_shift(db: Database) -> None:
    env = await make_env(db, mode="ratio")
    env.parser.push(result(btc_short()))
    await env.signal()
    f = 73400.0 / 73200.0
    req = env.broker.opened[0]
    assert req.entry_price == pytest.approx(73200.0 * f) == pytest.approx(73400.0)
    assert req.stop_loss == pytest.approx(73700.0 * f)
    assert req.take_profits[0].price == pytest.approx(72000.0 * f)
    trade = (await env.db.active_trades())[0]
    pos = (await env.db.broker_positions(trade.id))[0]
    assert pos.data["price_offset"] == {"mode": "ratio", "value": pytest.approx(f)}
    assert "oran x" in env.notifier.texts(Severity.INFO)[0]


async def test_market_entry_drift_guard_uses_shifted_entry(db: Database) -> None:
    env = await make_env(db)
    env.parser.push(result(btc_short(entry_type=EntryType.MARKET)))
    await env.signal()
    # shifted signal entry 73400 vs live bid 73399.9: no drift problem (unshifted it would be 0.27% off, still ok);
    # the shifted levels are what is sized
    assert env.broker.opened and env.broker.opened[0].stop_loss == pytest.approx(73900.0)
    assert env.broker.sized[0]["stop"] == pytest.approx(73900.0)


async def test_reference_outage_fails_closed_with_warning(db: Database) -> None:
    env = await make_env(db, ref_price=None)
    env.parser.push(result(btc_short()))
    await env.signal()
    assert env.broker.opened == [] and env.broker.sized == []
    warns = env.notifier.texts(Severity.WARNING)
    assert len(warns) == 1 and "referans fiyat" in warns[0] and "tickmill" in warns[0]
    assert not env.notifier.texts(Severity.INFO)


async def test_offset_above_max_pct_skips(db: Database) -> None:
    env = await make_env(db, ref_price=70000.0)            # broker 73400 is 4.9% away
    env.parser.push(result(btc_short()))
    await env.signal()
    assert env.broker.opened == []
    warns = env.notifier.texts(Severity.WARNING)
    assert len(warns) == 1 and "fark" in warns[0] and "işlem açılmadı" in warns[0]


async def test_offset_within_custom_limit_trades(db: Database) -> None:
    env = await make_env(db, ref_price=72000.0, max_pct=2.0)    # 1.94% away but limit 2%
    env.engine.prices.prices["BTC"] = 72000.0
    env.parser.push(result(btc_short(entry_price=72000.0, stop_loss=72500.0, take_profits=[TakeProfit(price=71000.0)])))
    await env.signal()
    assert env.broker.opened and env.broker.opened[0].entry_price == pytest.approx(73400.0)


async def test_fx_plan_is_never_shifted(db: Database) -> None:
    env = await make_env(db)
    env.parser.push(result(eur_long()))
    await env.signal()
    assert env.ref.calls == []
    req = env.broker.opened[0]
    assert req.entry_price == 1.0840 and req.stop_loss == 1.0800
    trade = (await env.db.active_trades())[0]
    assert "price_offset" not in (await env.db.broker_positions(trade.id))[0].data


async def test_default_config_is_unaffected(db: Database) -> None:
    env = await make_env(db, reference=None)
    env.parser.push(result(btc_short()))
    await env.signal()
    assert env.ref.calls == []
    req = env.broker.opened[0]
    assert req.entry_price == 73200.0 and req.stop_loss == 73700.0 and req.take_profits[0].price == 72000.0
    trade = (await env.db.active_trades())[0]
    assert "price_offset" not in (await env.db.broker_positions(trade.id))[0].data


# --------------------------------------------------------------------------- #
# Management
# --------------------------------------------------------------------------- #
async def test_move_sl_and_move_tp_use_the_stored_offset(db: Database) -> None:
    env = await make_env(db)
    env.parser.push(result(btc_short(entry_type=EntryType.MARKET)))
    await env.signal()
    assert env.broker.opened
    env.engine.prices.prices["BTC"] = 73000.0
    env.ref.prices["BTC"] = 73000.0                       # the live difference changed: must NOT be recomputed
    env.broker.quote = Quote(bid=72999.9, ask=73000.1, last=73000.0)
    env.parser.push(result(action(ActionType.MOVE_SL, base_asset="BTC", new_stop_loss=73100.0,
                                  used_message_ids=[600])))
    await add_msg(env.db, 600, "stop 73100", reply_to=502)
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.broker.calls_of("update_stop_loss")[-1][1] == pytest.approx(73300.0)
    trade = (await env.db.active_trades())[0]
    env.parser.push(result(action(ActionType.MOVE_TP, base_asset="BTC", side=Side.SHORT, target_trade_id=trade.id,
                                  take_profits=[TakeProfit(price=71500.0)], used_message_ids=[601])))
    await add_msg(env.db, 601, "tp 71500", reply_to=502)
    await env.engine.on_messages_ready(CHAT, SENDER)
    tps = env.broker.calls_of("update_take_profits")[-1][1]
    assert [tp.price for tp in tps] == [pytest.approx(71700.0)]
    assert env.ref.calls == [("binanceusdm", "BTC")]      # only the opening looked at the reference


async def test_broker_without_reference_manages_unshifted(db: Database) -> None:
    env = await make_env(db, reference=None)
    env.parser.push(result(btc_short(entry_type=EntryType.MARKET)))
    await env.signal()
    assert env.broker.opened
    env.engine.prices.prices["BTC"] = 73000.0
    env.broker.quote = Quote(bid=72999.9, ask=73000.1, last=73000.0)
    env.parser.push(result(action(ActionType.MOVE_SL, base_asset="BTC", new_stop_loss=73100.0,
                                  used_message_ids=[600])))
    await add_msg(env.db, 600, "stop 73100", reply_to=502)
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.broker.calls_of("update_stop_loss")[-1][1] == 73100.0


# --------------------------------------------------------------------------- #
# Preview
# --------------------------------------------------------------------------- #
async def test_preview_shows_shifted_sizing_and_outage(db: Database) -> None:
    env = await make_env(db)
    ev = await env.engine.evaluate(btc_short(), CHAT)
    assert ev is not None and ev.plan is not None
    lines = await env.engine.preview_open(ev.plan)       # type: ignore[arg-type]
    assert len(lines) == 1 and "fark +200.0 uygulandı" in lines[0]
    assert env.broker.sized[-1]["entry"] == pytest.approx(73400.0)
    assert env.broker.sized[-1]["stop"] == pytest.approx(73900.0)
    env.ref.prices["BTC"] = None
    lines = await env.engine.preview_open(ev.plan)       # type: ignore[arg-type]
    assert len(lines) == 1 and "referans fiyat" in lines[0] and "açılmazdı" in lines[0]
    assert env.broker.opened == []


# --------------------------------------------------------------------------- #
# Helpers, reference oracle, config
# --------------------------------------------------------------------------- #
def test_shift_helpers_roundtrip() -> None:
    for off in ({"mode": "additive", "value": 200.0}, {"mode": "ratio", "value": 1.0027}):
        assert unshift_price(shift_price(73200.0, off), off) == pytest.approx(73200.0)
    assert shift_price(5.0, None) == 5.0


def test_filled_tp_prices_are_unshifted() -> None:
    pos = BrokerPosition(broker="t", broker_symbol="BTCUSD", side=Side.SHORT, status=PositionStatus.OPEN,
                         quantity=1, filled_quantity=1, remaining_quantity=1,
                         data={"price_offset": {"mode": "additive", "value": 200.0},
                               "tps": [{"price": 72200.0, "status": "filled"}]})
    assert filled_tp_prices(pos) == [pytest.approx(72000.0)]


class _Ex:
    def __init__(self, tickers: dict[str, dict] | None = None, fail: bool = False) -> None:
        self.tickers, self.fail, self.calls, self.closed = tickers or {}, fail, [], False

    async def fetch_ticker(self, symbol: str) -> dict:
        self.calls.append(symbol)
        if self.fail or symbol not in self.tickers:
            raise RuntimeError("nope")
        return self.tickers[symbol]

    async def close(self) -> None:
        self.closed = True


async def test_reference_oracle_mid_cache_1000_prefix_and_never_raises() -> None:
    ex = _Ex({"BTC/USDT:USDT": {"bid": 73199.0, "ask": 73201.0}, "1000PEPE/USDT:USDT": {"bid": 9.0, "ask": 11.0}})
    oracle = ReferencePriceOracle(exchanges={"binanceusdm": ex})
    assert await oracle.mid("binanceusdm", "btc") == 73200.0
    assert await oracle.mid("binanceusdm", "BTC") == 73200.0
    assert ex.calls == ["BTC/USDT:USDT"]                       # cached
    assert await oracle.mid("binanceusdm", "BTC", fresh=True) == 73200.0
    assert len(ex.calls) == 2
    assert await oracle.mid("binanceusdm", "PEPE") == pytest.approx(0.01)
    assert await oracle.mid("binanceusdm", "NOPE") is None
    assert await ReferencePriceOracle(exchanges={"x": _Ex(fail=True)}).mid("x", "BTC") is None
    assert await ReferencePriceOracle().mid("no_such_exchange_id", "BTC") is None
    assert await oracle.mid("", "BTC") is None
    await oracle.close()
    assert ex.closed


def test_config_defaults_and_validation() -> None:
    from copytrader.config import CcxtBrokerConfig, Mt5BrokerConfig
    m = Mt5BrokerConfig(type="mt5", name="m", login=1, password="p", server="s")
    assert m.reference_exchange is None and m.offset_mode == "additive" and m.max_offset_pct == 1.0
    c = CcxtBrokerConfig(type="ccxt", name="c", exchange="bybit", reference_exchange="binanceusdm",
                         offset_mode="ratio", max_offset_pct=0.5)
    assert c.offset_mode == "ratio" and c.max_offset_pct == 0.5
    with pytest.raises(ValueError):
        Mt5BrokerConfig(type="mt5", name="m", login=1, password="p", server="s", offset_mode="bogus")  # type: ignore[arg-type]


def test_tickmill_example_loads(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for k, v in {"TG_API_ID": "123", "TG_API_HASH": "h", "TG_BOT_TOKEN": "1:abc", "TG_OWNER_ID": "42",
                 "ANTHROPIC_API_KEY": "sk-test", "MT5_LOGIN": "5000", "MT5_PASSWORD": "pw",
                 "MT5_SERVER": "Tickmill-Demo"}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.chdir(tmp_path)          # no stray .env
    cfg = load_config(ROOT / "config.tickmill.example.yaml")
    (b,) = cfg.brokers
    assert b.name == "tickmill" and b.type == "mt5" and b.risk_usd == 10
    assert b.reference_exchange == "binanceusdm" and b.offset_mode == "additive" and b.max_offset_pct == 1.0
    assert b.symbol_map == {"BTC": "BTCUSD", "ETH": "ETHUSD"} and b.no_hedge and b.daily_loss_limit_pct == 3
    assert [t.name for t in cfg.telegram.topics] == ["kripto", "fx"]
    assert all(t.brokers == ["tickmill"] for t in cfg.telegram.topics)
    assert cfg.dry_run is True
