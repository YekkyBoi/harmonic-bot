"""Pre-flight fixes, batch 2, engine (orchestrator / prices / guards / models / config), on a replica of the
owner's production setup (routes "kripto" / "fx"; brokers binance / breakout / fx / tickmill) with fake venues:

C1. no broker can abort a flow: every failure, also a CancelledError nobody asked for, is that broker's own
    error outcome; the monitor task is supervised;
C2. a slow broker does not hold up the open flow: per-leg deadline, late legs booked and reported afterwards;
C3. management messages are matched by PAIR for forex / CFD trades (GBPJPY is not GBPUSD);
C4. the daily-loss guard can include what is still at risk (``daily_loss_include_open_risk``);
C5. every routed broker that did not take a trade is named;
C6. virtual (reference-touch) entries: wicks, retries, cancellation rules, pause, gap check on candles;
C7. the max_open_trades notice names the trades that fill the limit.

The fakes prove the bot's own logic; no real broker, exchange or Telegram is involved. Names added by this
batch are reached through their modules (``orch.managed_asset``, ``prices_mod.stray_cancel`` ...), so that
against the sources before the batch every test fails on its own instead of the whole file at import.
"""

from __future__ import annotations

import asyncio
import itertools
from datetime import UTC, datetime
from typing import Any

import pytest
import yaml

from copytrader import models
from copytrader.brokers.base import BrokerError
from copytrader.config import AppConfig, PaperBrokerConfig, RiskConfig
from copytrader.engine import orchestrator as orch
from copytrader.engine import prices as prices_mod
from copytrader.engine.monitor import is_virtual_pending
from copytrader.engine.orchestrator import BrokerOutcome, Engine
from copytrader.engine.prices import PriceOracle, ReferencePriceOracle
from copytrader.models import (
    ActionType,
    BrokerPosition,
    EntryType,
    ManagementPlan,
    MarketType,
    OpenRequest,
    ParsedAction,
    ParseResult,
    PositionStatus,
    Quote,
    Severity,
    Side,
    SizingResult,
    StoredMessage,
    SymbolInfo,
    TakeProfit,
    TradePlan,
    TradeRecord,
    TradeStatus,
)
from copytrader.parsing import validator
from copytrader.risk import guards
from copytrader.storage.db import Database
from copytrader.telegram import formatting as fmt

from fakes import FakeBroker, FakeNotifier, FakeParser

CHAT, SENDER = -1003782294463, 900
KRIPTO, FX = 10, 20
TITLES = {KRIPTO: "Kasım Ayı Kripto İşlemleri", FX: "Kasım Ayı Fx İşlemleri"}
MINUTE = 60_000

# Replica of the owner's production config.yaml (dummy secrets; the real "breakout" coin list is longer).
PROD_YAML = """
dry_run: false
timezone: Europe/Istanbul
telegram:
  api_id: 1
  api_hash: h
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
  bot_token: "1:abc"
  owner_user_id: 42
llm:
  api_key: sk-test
brokers:
  - name: binance
    type: ccxt
    exchange: binanceusdm
    api_key: k
    secret: s
    risk_usd: 10
    margin_mode: cross
    leverage: 125
    max_leverage: 125
  - name: breakout
    type: manual
    risk_usd: 50
    allowed_assets: [BTC, ETH, SOL, XRP, DOGE, ADA, AVAX, LINK, BNB, LTC, SUI, TON, DOT, NEAR, APT, ARB, OP,
                     PEPE, WIF, HYPE]
  - name: fx
    type: mt5
    login: 1
    password: p
    server: s
    risk_usd: 100
    no_hedge: true
    daily_loss_limit_pct: 3
  - name: tickmill
    type: metaapi
    enabled: true
    optional: true
    token: t
    account_id: a
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


def prod_config() -> AppConfig:
    cfg = AppConfig.model_validate(yaml.safe_load(PROD_YAML))
    cfg.monitor_interval_sec = 3600.0
    return cfg


def q(bid: float, ask: float) -> Quote:
    return Quote(bid=bid, ask=ask, last=(bid + ask) / 2.0)


async def cancelled_elsewhere() -> None:
    """What a broker SDK does while its account is disconnected (MetaApi's shared "wait connected" future, cancelled
    by an earlier timeout): awaiting it raises CancelledError although nobody cancelled the awaiting task."""
    fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    fut.cancel()
    await fut


# --------------------------------------------------------------------------- #
# Doubles
# --------------------------------------------------------------------------- #
class Reference:
    """reference_prices double: ``mid`` = scripted last price (None: unavailable); ``candles`` = scripted
    1-minute candles (open ms, low, high) per base (no entry: candles cannot be read)."""

    def __init__(self, prices: dict[str, float | None] | None = None) -> None:
        self.prices: dict[str, float | None] = dict(prices or {})
        self.rows: dict[str, list[tuple[int, float, float]]] = {}
        self.calls: list[tuple[str, str]] = []
        self.candle_calls: list[tuple[str, int, int | None]] = []
        self.stray = False

    async def mid(self, exchange_id: str, base_asset: str, *, fresh: bool = False) -> float | None:
        self.calls.append((exchange_id, base_asset.upper()))
        if self.stray:
            await cancelled_elsewhere()
        return self.prices.get(base_asset.upper())

    async def candles(self, exchange_id: str, base_asset: str, since_ms: int,
                      until_ms: int | None = None) -> list[tuple[int, float, float]] | None:
        self.candle_calls.append((base_asset.upper(), since_ms, until_ms))
        rows = self.rows.get(base_asset.upper())
        return None if rows is None else [r for r in rows if r[0] >= since_ms]


class PairOracle:
    """price_oracle double keyed by pair ("USDJPY") or, failing that, by base ("BTC")."""

    def __init__(self, prices: dict[str, float]) -> None:
        self.prices = dict(prices)

    async def price(self, base_asset: str | None, quote_asset: str | None = None,
                    market_type: MarketType | None = None, raw_symbol: str | None = None, *,
                    fresh: bool = False) -> float | None:
        if not base_asset:
            return None
        base = base_asset.upper()
        return self.prices.get(base + (quote_asset or "").upper(), self.prices.get(base))


class Venue(FakeBroker):
    """FakeBroker that carries a REAL broker config and names instruments like the venue does (MetaTrader:
    "USDJPY" / "BTCUSD" with symbol_map first; everything else "BTC/USDT:USDT"). Quotes by broker symbol.

    ``hold(method)``: calls of that method wait until the returned event is set (a slow / hung broker). For
    open_position the order is already at the broker and only the answer is held back.
    ``stray``: methods that raise a CancelledError nobody asked for (for open_position after the order went out).
    """

    def __init__(self, config: Any, **kw: Any) -> None:
        super().__init__(config.name, **kw)
        self.config = config
        self.gates: dict[str, asyncio.Event] = {}
        self.stray: set[str] = set()
        self.waiting = asyncio.Event()          # a call is waiting at a gate
        self.running = 0                        # open_position calls in flight
        self.most_running = 0

    def hold(self, method: str) -> asyncio.Event:
        self.gates[method] = asyncio.Event()
        return self.gates[method]

    async def _at(self, method: str) -> None:
        if method in self.stray:
            await cancelled_elsewhere()
        gate = self.gates.get(method)
        if gate is not None and not gate.is_set():
            self.waiting.set()
            await gate.wait()

    async def resolve_symbol(self, base_asset: str, quote_asset: str | None = None,
                             market_type: MarketType | None = None, raw_symbol: str | None = None) -> SymbolInfo | None:
        self.calls.append(("resolve_symbol", (base_asset, quote_asset, market_type, raw_symbol)))
        await self._at("resolve_symbol")
        self._maybe_fail("resolve_symbol")
        base = base_asset.upper()
        if base in self.unsupported:
            return None
        if getattr(self.config, "type", "") in ("mt5", "metaapi"):
            quote = (quote_asset or "USD").upper()
            quote = "USD" if quote in ("USDT", "USDC") else quote
            name = {k.upper(): v for k, v in self.config.symbol_map.items()}.get(base) or f"{base}{quote}"
        elif market_type in (MarketType.FOREX, MarketType.CFD):
            return None                                          # a crypto venue lists no forex / CFD
        else:
            quote, name = "USDT", f"{base}/USDT:USDT"
        return SymbolInfo(broker_symbol=name, base_asset=base, quote_asset=quote, qty_step=self.qty_step,
                          min_qty=self.qty_step, price_tick=0.00001)

    def _quote(self, symbol: SymbolInfo) -> Quote:
        return self.quotes.get(symbol.broker_symbol) or self.quotes.get(symbol.base_asset, self.quote)

    async def get_equity(self) -> float:
        await self._at("get_equity")
        return await super().get_equity()

    async def get_free_margin(self) -> float:
        await self._at("get_free_margin")
        return await super().get_free_margin()

    async def get_quote(self, symbol: SymbolInfo) -> Quote:
        await self._at("get_quote")
        self._maybe_fail("get_quote")
        return self._quote(symbol)

    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        self.running += 1
        self.most_running = max(self.most_running, self.running)
        try:
            pos = await super().open_position(req)               # the order is at the broker now
            if pos.avg_entry_price is not None:
                pos.avg_entry_price = self._quote(req.symbol).entry_price_for(req.plan.side)
            await self._at("open_position")                      # ... its answer may be slow or lost
            return pos
        finally:
            self.running -= 1

    async def close_position(self, pos: BrokerPosition, fraction: float = 1.0) -> BrokerPosition:
        await self._at("close_position")
        return await super().close_position(pos, fraction)

    async def update_stop_loss(self, pos: BrokerPosition, new_stop_loss: float) -> BrokerPosition:
        await self._at("update_stop_loss")
        return await super().update_stop_loss(pos, new_stop_loss)

    async def sync_position(self, pos: BrokerPosition) -> BrokerPosition:
        await self._at("sync_position")
        return await super().sync_position(pos)


FX_QUOTES = {"EURUSD": q(1.16195, 1.16205), "USDJPY": q(154.195, 154.205), "USDCAD": q(1.40195, 1.40205),
             "GBPUSD": q(1.31495, 1.31505), "GBPJPY": q(202.490, 202.510), "XAUUSD": q(4099.90, 4100.10),
             "AUDUSD": q(0.65495, 0.65505), "NZDUSD": q(0.57495, 0.57505)}
PRICES = {"EURUSD": 1.1620, "USDJPY": 154.20, "USDCAD": 1.4020, "GBPUSD": 1.3150, "GBPJPY": 202.50,
          "XAUUSD": 4100.0, "AUDUSD": 0.6550, "NZDUSD": 0.5750, "EUR": 1.1620, "BTC": 74000.0, "ETH": 2232.0,
          "SOL": 150.0}


class Env:
    def __init__(self, cfg: AppConfig, db: Database, brokers: dict[str, Venue], parser: FakeParser,
                 notifier: FakeNotifier, ref: Reference, oracle: PairOracle, engine: Engine) -> None:
        self.cfg, self.db, self.brokers, self.parser = cfg, db, brokers, parser
        self.notifier, self.ref, self.oracle, self.engine = notifier, ref, oracle, engine
        self.mid = 5000
        self.clock: Any = None                               # the engine's clock when the test controls the time

    def __getattr__(self, name: str) -> Venue:              # env.fx, env.tickmill, env.binance, env.breakout
        try:
            return self.__dict__["brokers"][name]
        except KeyError:
            raise AttributeError(name) from None

    async def post(self, action: ParsedAction, topic: int, text: str = "LONG 1R", *,
                   reply_to: int | None = None) -> int:
        """The trader posts a message in ``topic`` (the fake parser will read it as ``action``); not processed yet."""
        self.mid += 1
        mid = self.mid
        self.parser.push(ParseResult(actions=[action.model_copy(update={"used_message_ids": [mid]})], model="fake"))
        date = self.clock() if self.clock is not None else datetime.now(UTC)
        await self.db.save_message(StoredMessage(
            chat_id=CHAT, message_id=mid, sender_id=SENDER, sender_name="Hoca", date=date, text=text,
            reply_to_message_id=reply_to if reply_to is not None else topic, topic_id=topic,
            topic_title=TITLES[topic]))
        return mid

    async def say(self, action: ParsedAction, topic: int, text: str = "LONG 1R", *,
                  reply_to: int | None = None, timeout: float = 3.0) -> int:
        """Post a message and let the engine process it (returns when the engine is done with the batch). The
        engine must be done within ``timeout``: a batch that is still waiting for a broker then fails the test."""
        mid = await self.post(action, topic, text, reply_to=reply_to)
        await asyncio.wait_for(self.engine.on_messages_ready(CHAT, SENDER, topic), timeout)
        return mid

    async def trades(self) -> list[TradeRecord]:
        return sorted(await self.db.recent_trades(datetime(2000, 1, 1, tzinfo=UTC)), key=lambda t: t.id)

    async def trade(self, trade_id: int) -> TradeRecord:
        trade = await self.db.get_trade(trade_id)
        assert trade is not None
        return trade

    async def position(self, trade_id: int, broker: str) -> BrokerPosition:
        return next(p for p in await self.db.broker_positions(trade_id) if p.broker == broker)

    async def brokers_of(self, trade_id: int) -> dict[str, str]:
        return {p.broker: ("virtual" if is_virtual_pending(p) else p.status.value)
                for p in await self.db.broker_positions(trade_id)}

    def texts(self, severity: Severity | None = None) -> list[str]:
        return self.notifier.texts(severity)

    async def until(self, predicate: Any, what: str = "condition", timeout: float = 3.0) -> None:
        end = asyncio.get_running_loop().time() + timeout
        while not predicate():
            assert asyncio.get_running_loop().time() < end, f"not in time: {what}"
            await asyncio.sleep(0.005)


async def prod_env(db: Database, *, clock: Any = None, notify_on_execution: bool = False) -> Env:
    """The production replica: real config objects on fake venues (fx: 10,000 USD prop account)."""
    cfg = prod_config()
    cfg.notifier.notify_on_execution = notify_on_execution
    by = {b.name: b for b in cfg.brokers}
    brokers = {
        "binance": Venue(by["binance"], equity=46.9, currency="USDT"),
        "breakout": Venue(by["breakout"], equity=1_000_000.0, currency="USDT"),
        "fx": Venue(by["fx"], equity=10_000.0, currency="USD", qty_step=0.01),
        "tickmill": Venue(by["tickmill"], equity=108.0, currency="USD", qty_step=0.01),
    }
    for name in ("binance", "breakout"):
        brokers[name].quotes.update({"BTC/USDT:USDT": q(73999.0, 74001.0), "ETH/USDT:USDT": q(2231.9, 2232.1),
                                     "SOL/USDT:USDT": q(149.99, 150.01)})
    brokers["fx"].quotes.update(FX_QUOTES)
    brokers["tickmill"].quotes.update(FX_QUOTES)
    brokers["tickmill"].quotes["BTCUSD"] = q(73949.0, 73951.0)       # CFD price: 50 USD below Binance
    brokers["tickmill"].quotes["ETHUSD"] = q(2229.9, 2230.1)
    parser, notifier = FakeParser(), FakeNotifier()
    ref, oracle = Reference({"BTC": 74000.0, "ETH": 2232.0}), PairOracle(PRICES)
    engine = Engine(cfg, db, parser, list(brokers.values()), notifier, price_oracle=oracle, reference_prices=ref,
                    clock=clock)
    await engine.start(monitor=False)
    assert [b.name for b in engine.brokers] == ["binance", "breakout", "fx", "tickmill"]
    notifier.notifications.clear()
    env = Env(cfg, db, brokers, parser, notifier, ref, oracle, engine)
    env.clock = clock
    return env


@pytest.fixture(autouse=True)
def _restore_formatting_global(monkeypatch: pytest.MonkeyPatch) -> None:
    # Engine.connect_brokers() sets this module global when a broker uses risk_usd: other test files must not see it.
    monkeypatch.setattr(fmt, "RISK_PER_R_NOTE", fmt.RISK_PER_R_NOTE)


@pytest.fixture
async def db():
    database = Database(":memory:")
    await database.open()
    yield database
    await database.close()


@pytest.fixture
async def env(db: Database):
    e = await prod_env(db)
    yield e
    for broker in e.brokers.values():            # never leave a held call behind
        for gate in broker.gates.values():
            gate.set()
    await e.engine.stop(grace_sec=2.0)


# --------------------------------------------------------------------------- #
# Signals
# --------------------------------------------------------------------------- #
def signal(base: str, quote: str | None, side: Side, entry: float | None, sl: float, tp: float, *,
           symbol: str | None = None, market_type: MarketType | None = None,
           entry_type: EntryType | None = EntryType.MARKET, **kw: Any) -> ParsedAction:
    data: dict[str, Any] = dict(
        action=ActionType.OPEN, symbol=symbol, base_asset=base, quote_asset=quote, market_type=market_type,
        side=side, entry_type=entry_type, entry_price=entry, stop_loss=sl, take_profits=[TakeProfit(price=tp)],
        risk_r=1.0, confidence=0.95, summary_tr=f"{base}{quote or ''} {side.value.upper()}")
    data.update(kw)
    return ParsedAction(**data)


def btc(entry_type: EntryType = EntryType.MARKET, side: Side = Side.LONG) -> ParsedAction:
    """BTCUSDT.P: market long 74000 / SL 73300 / TP 75500, or limit long 73000 / SL 72300 / TP 75000."""
    if entry_type is EntryType.MARKET:
        if side is Side.SHORT:
            return signal("BTC", "USDT", Side.SHORT, 74000.0, 74700.0, 72500.0, symbol="BTCUSDT.P",
                          market_type=MarketType.PERP)
        return signal("BTC", "USDT", Side.LONG, 74000.0, 73300.0, 75500.0, symbol="BTCUSDT.P",
                      market_type=MarketType.PERP)
    return signal("BTC", "USDT", Side.LONG, 73000.0, 72300.0, 75000.0, symbol="BTCUSDT.P",
                  market_type=MarketType.PERP, entry_type=EntryType.LIMIT)


def eth() -> ParsedAction:
    return signal("ETH", "USDT", Side.LONG, 2232.0, 2200.0, 2300.0, symbol="ETHUSDT.P", market_type=MarketType.PERP)


def sol() -> ParsedAction:
    return signal("SOL", "USDT", Side.LONG, 150.0, 147.0, 156.0, symbol="SOLUSDT.P", market_type=MarketType.PERP)


def fx_pair(pair: str, side: Side = Side.LONG, **kw: Any) -> ParsedAction:
    """A forex market signal at the pair's current price, 20 pips of stop (200 for gold)."""
    price = PRICES[pair]
    step = {"USDJPY": 0.20, "GBPJPY": 0.20, "XAUUSD": 15.0}.get(pair, 0.0020)
    sign = 1 if side is Side.LONG else -1
    return signal(pair[:3], pair[3:], side, price, round(price - sign * step, 5), round(price + sign * 2 * step, 5),
                  symbol=pair, market_type=MarketType.CFD if pair.startswith("XAU") else MarketType.FOREX, **kw)


def manage(kind: ActionType, **kw: Any) -> ParsedAction:
    data: dict[str, Any] = dict(action=kind, confidence=0.95, summary_tr=kind.value)
    data.update(kw)
    return ParsedAction(**data)


_ids = itertools.count(1)


def plan_of(base: str, quote: str | None, side: Side = Side.LONG, *, route: str | None = None,
            brokers: list[str] | None = None, entry: float | None = 100.0, sl: float = 90.0,
            market_type: MarketType | None = None) -> TradePlan:
    n = next(_ids)
    return TradePlan(plan_id=f"q{n}", base_asset=base, quote_asset=quote, market_type=market_type, side=side,
                     entry_type=EntryType.MARKET, entry_price=entry, stop_loss=sl, risk_r=1.0, risk_pct=2.0,
                     source_chat_id=CHAT, source_message_ids=[80_000 + n], created_at=datetime.now(UTC),
                     route=route, broker_names=list(brokers or []))


# =========================================================================== #
# C1 - no broker may abort a flow
# =========================================================================== #
async def test_c1_stray_cancel_tells_a_library_cancellation_from_a_real_one() -> None:
    seen: dict[str, bool] = {}

    async def library_call() -> None:
        try:
            await cancelled_elsewhere()
        except asyncio.CancelledError:
            seen["library"] = prices_mod.stray_cancel()

    await asyncio.ensure_future(library_call())
    assert seen["library"] is True                      # nobody cancelled that task

    async def cancelled_task() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            seen["task"] = prices_mod.stray_cancel()
            raise

    task = asyncio.ensure_future(cancelled_task())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert seen["task"] is False                        # a real cancellation: must be re-raised

    async def slow() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            seen["timeout"] = prices_mod.stray_cancel()
            raise

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(slow(), 0.01)
    assert seen["timeout"] is False                     # the timeout of an enclosing wait_for is a real one too

    async def library_call_under_a_timeout() -> None:
        try:
            await asyncio.wait_for(cancelled_elsewhere(), 5)
        except asyncio.CancelledError:
            seen["inside"] = prices_mod.stray_cancel()

    await library_call_under_a_timeout()
    assert seen["inside"] is True
    assert orch.stray_cancel is prices_mod.stray_cancel   # one helper, used by the engine and the price oracles


async def test_c1_stray_cancellation_before_the_order_is_that_brokers_error_only(env: Env) -> None:
    env.tickmill.stray.add("get_equity")                # MetaApi disconnected: its calls raise CancelledError at once

    await env.say(btc(), KRIPTO, "btc 1R long")         # must not raise, must not leave the trade half open

    (trade,) = await env.trades()
    assert trade.status is TradeStatus.OPEN and trade.note == "tickmill: error"
    assert await env.brokers_of(trade.id) == {"binance": "open", "breakout": "open"}
    assert len(env.binance.opened) == 1 and len(env.breakout.opened) == 1 and env.tickmill.opened == []
    assert env.texts(Severity.ERROR) == [
        "tickmill: broker bağlantısı yanıt vermeden kesildi; emir gönderilmedi, bu hesapta işlem açılmadı"]
    (report,) = [t for t in env.texts(Severity.INFO) if "#1 BTC LONG açıldı" in t]
    assert "• binance: " in report and "• tickmill: broker bağlantısı yanıt vermeden kesildi" in report
    assert not env.engine._open_lock.locked() and not env.engine.trade_lock(trade.id).locked()
    assert (await env.db.get_message(CHAT, env.mid)).consumed          # type: ignore[union-attr]


async def test_c1_stray_cancellation_after_the_order_went_out_says_the_position_may_exist(env: Env) -> None:
    env.tickmill.stray.add("open_position")             # the order reaches the broker, its answer never comes

    await env.say(btc(), KRIPTO, "btc 1R long")

    (trade,) = await env.trades()
    assert len(env.tickmill.opened) == 1
    assert trade.status is TradeStatus.OPEN
    assert await env.brokers_of(trade.id) == {"binance": "open", "breakout": "open"}
    assert env.texts(Severity.ERROR) == [
        "tickmill: emir gönderildi ama broker bağlantısı yanıt vermeden kesildi; pozisyon açılmış olabilir, "
        "brokerda elle kontrol edin"]


async def test_c1_every_leg_ends_in_an_outcome_and_a_real_cancellation_still_propagates(env: Env) -> None:
    plan = plan_of("BTC", "USDT", route="kripto", brokers=["tickmill"], entry=74000.0, sl=73300.0,
                   market_type=MarketType.PERP)
    trade = await env.db.create_trade(plan)
    for method in ("resolve_symbol", "get_equity", "get_quote"):
        env.tickmill.stray = {method}
        out = await env.engine._open_on_broker(env.tickmill, plan, trade, None)
        assert isinstance(out, BrokerOutcome) and (out.kind, out.sent, out.position) == ("error", False, None)
    env.tickmill.stray = {"open_position"}
    out = await env.engine._open_on_broker(env.tickmill, plan, trade, None)
    assert (out.kind, out.sent, out.position) == ("error", True, None)

    env.tickmill.stray = set()
    gate = env.tickmill.hold("get_equity")
    task = asyncio.ensure_future(env.engine._open_on_broker(env.tickmill, plan, trade, None))
    await asyncio.wait_for(env.tickmill.waiting.wait(), 2)
    task.cancel()                                       # the task itself is cancelled: cancelling() > 0
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()
    gate.set()


async def test_c1_a_leg_task_that_died_anyway_is_still_an_error_outcome() -> None:
    async def boom() -> BrokerOutcome:
        raise RuntimeError("bug")

    async def sleeper() -> BrokerOutcome:
        await asyncio.sleep(10)
        raise AssertionError

    failed = asyncio.ensure_future(boom())
    cancelled = asyncio.ensure_future(sleeper())
    await asyncio.sleep(0)
    cancelled.cancel()
    await asyncio.wait({failed, cancelled})
    out = Engine._leg_outcome("fx", failed)
    assert out.kind == "error" and "beklenmeyen hata" in out.text and "bug" in out.text
    out = Engine._leg_outcome("fx", cancelled)
    assert out.kind == "error" and out.sent is True and "brokerda elle kontrol edin" in out.text


async def test_c1_stray_cancellation_while_managing_leaves_the_other_brokers_managed(env: Env) -> None:
    await env.say(btc(), KRIPTO, "btc 1R long")
    (trade,) = await env.trades()
    assert await env.brokers_of(trade.id) == {"binance": "open", "breakout": "open", "tickmill": "open"}
    env.notifier.notifications.clear()
    env.tickmill.stray.update({"sync_position", "close_position"})      # disconnected from now on

    await env.say(manage(ActionType.CLOSE, base_asset="BTC", close_fraction=1.0), KRIPTO, "btc kapattık")

    assert await env.brokers_of(trade.id) == {"binance": "closed", "breakout": "closed", "tickmill": "open"}
    assert len(env.binance.calls_of("close_position")) == 1
    assert [t for t in env.texts(Severity.ERROR) if "değişikliğin" in t] == [
        f"#{trade.id} tickmill: broker bağlantısı yanıt vermeden kesildi; değişikliğin uygulanıp uygulanmadığını "
        "brokerda elle kontrol edin"]
    assert (await env.trade(trade.id)).status is TradeStatus.OPEN       # the tickmill leg is still there
    assert (await env.db.get_message(CHAT, env.mid)).consumed           # type: ignore[union-attr]
    assert not env.engine.trade_lock(trade.id).locked()


async def test_c1_balances_answer_although_one_broker_raises_a_stray_cancellation(env: Env) -> None:
    env.tickmill.stray.add("get_equity")
    text = await env.engine.balances_text()             # /bakiye
    assert "• binance: 46.90 USDT" in text and "• fx: 10 000.00 USD" in text
    assert "• tickmill: okunamadı (broker bağlantısı yanıt vermiyor)" in text


async def test_c1_stray_cancellation_of_the_currency_read_is_not_a_failed_connect(db: Database) -> None:
    class Sdk(FakeBroker):
        async def account_currency(self) -> str:
            await cancelled_elsewhere()
            return "USD"

    broker, notifier = Sdk("tickmill", optional=True), FakeNotifier()
    engine = Engine(prod_config(), db, FakeParser(), [broker], notifier)
    assert await engine._connect(broker) is True        # connected; only the currency is unknown
    assert engine._currency == {"tickmill": ""} and engine.failed_brokers == {} and engine.optional_down == {}
    assert not broker.closed and notifier.texts() == []


async def test_c1_price_oracles_treat_a_stray_cancellation_as_no_price() -> None:
    class Sdk(FakeBroker):
        async def resolve_symbol(self, *a: Any, **k: Any) -> SymbolInfo | None:
            await cancelled_elsewhere()
            return None

    good = FakeBroker("binance", quote=q(99.0, 101.0))
    assert await PriceOracle([Sdk("tickmill"), good], exchange_id=None).price("BTC") == 100.0   # the next broker answers
    assert await PriceOracle([Sdk("tickmill")], exchange_id=None).price("BTC") is None

    class Exchange:
        async def fetch_ticker(self, symbol: str) -> dict[str, Any]:
            await cancelled_elsewhere()
            return {}

        async def fetch_ohlcv(self, *a: Any) -> list[list[float]]:
            await cancelled_elsewhere()
            return []

    ref = ReferencePriceOracle(exchanges={"binanceusdm": Exchange()})
    assert await ref.mid("binanceusdm", "BTC") is None
    assert await ref.candles("binanceusdm", "BTC", 0) is None

    class Slow:
        async def fetch_ticker(self, symbol: str) -> dict[str, Any]:
            await asyncio.sleep(10)
            return {}

    task = asyncio.ensure_future(ReferencePriceOracle(exchanges={"x": Slow()}).mid("x", "BTC"))
    await asyncio.sleep(0.01)
    task.cancel()                                       # a real cancellation is not swallowed
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_c1_engine_price_reads_and_the_virtual_hook_absorb_a_stray_cancellation(env: Env) -> None:
    class Bad:
        async def price(self, *a: Any, **k: Any) -> float | None:
            await cancelled_elsewhere()
            return None

    good, env.engine.prices = env.engine.prices, Bad()
    assert await env.engine._price("BTC", "USDT", MarketType.PERP) is None
    env.engine.prices = good

    await env.say(btc(EntryType.LIMIT), KRIPTO, "btc 1R long limit emir")
    (trade,) = await env.trades()
    pos = await env.position(trade.id, "tickmill")
    assert is_virtual_pending(pos)
    env.ref.stray = True                                # the reference exchange's client raises it
    assert await env.engine._reference_mid("binanceusdm", "BTC") is None
    assert is_virtual_pending(await env.engine._virtual_tick(trade, pos, env.engine._now()))   # the monitor's hook
    env.ref.stray = False

    async def broken(*a: Any, **k: Any) -> BrokerPosition:
        await cancelled_elsewhere()
        raise AssertionError

    env.engine._virtual_check = broken                  # type: ignore[method-assign]
    assert await env.engine._virtual_tick(trade, pos, env.engine._now()) is pos      # anywhere else in the hook
    assert is_virtual_pending(await env.position(trade.id, "tickmill"))


async def test_c1_unreadable_reference_skips_only_the_offset_broker(env: Env) -> None:
    env.ref.stray = True
    await env.say(btc(), KRIPTO, "btc 1R long")
    (trade,) = await env.trades()
    assert await env.brokers_of(trade.id) == {"binance": "open", "breakout": "open"}
    assert any("tickmill: referans fiyat (binanceusdm) alınamadı" in t for t in env.texts(Severity.WARNING))


async def test_c1_monitor_task_that_dies_is_restarted_and_the_owner_is_told_once(
        db: Database, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "MONITOR_RESTART_DELAY_SEC", 0.02, raising=False)
    e = await prod_env(db)
    real_run = e.engine.monitor.run
    starts: list[int] = []

    async def run() -> None:
        starts.append(len(starts) + 1)
        if starts[-1] == 1:
            await cancelled_elsewhere()                 # e.g. a broker SDK's CancelledError got out of the loop
        if starts[-1] == 2:
            raise RuntimeError("monitor bug")
        await real_run()

    e.engine.monitor.run = run                          # type: ignore[method-assign]
    await e.engine.start()                              # starts the (supervised) monitor task
    e.notifier.notifications.clear()
    await e.until(lambda: len(starts) == 3, "third start")
    await asyncio.sleep(0.1)
    assert len(starts) == 3 and e.engine._monitor_task is not None and not e.engine._monitor_task.done()
    told = [t for t in e.texts(Severity.ERROR) if "Pozisyon takibi durmuştu, yeniden başlatıldı" in t]
    assert len(told) == 1                               # the second restart within the throttle is only logged
    await e.engine.stop()
    await asyncio.sleep(0.1)
    assert len(starts) == 3                             # stopping the engine does not restart it


# =========================================================================== #
# C2 - a slow broker does not hold up the open flow
# =========================================================================== #
def late_reports(env: Env) -> list[str]:
    return [t for t in env.texts() if "geç yanıt verdi" in t]


async def test_c2_all_brokers_fast_leaves_no_trace_of_the_late_machinery(env: Env) -> None:
    await env.say(btc(), KRIPTO, "btc 1R long")
    (trade,) = await env.trades()
    assert trade.status is TradeStatus.OPEN and trade.note is None
    assert await env.brokers_of(trade.id) == {"binance": "open", "breakout": "open", "tickmill": "open"}
    assert env.texts() == []                             # notify_on_execution is off: nothing to say, as before
    assert env.engine._late_legs == {} and env.engine._leg_running == {}
    assert await env.db.get_state("late_open_legs") is None
    assert "risk_basis" not in (await env.position(trade.id, "fx" if False else "tickmill")).data


async def test_c2_slow_broker_the_report_comes_at_the_deadline_and_its_result_later(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.3, raising=False)
    answer = env.tickmill.hold("open_position")          # the order went out, the answer does not come
    loop = asyncio.get_running_loop()
    started = loop.time()

    await env.say(btc(), KRIPTO, "btc 1R long")

    assert 0.3 <= loop.time() - started < 1.5            # not until tickmill answers: until the deadline
    (trade,) = await env.trades()
    assert trade.status is TradeStatus.OPEN and trade.note == "tickmill: late"
    assert await env.brokers_of(trade.id) == {"binance": "open", "breakout": "open"}
    (report,) = [t for t in env.texts(Severity.INFO) if "#1 BTC LONG açıldı" in t]
    assert report.startswith("⚠️")
    assert "• ⏳ tickmill: yanıt bekleniyor; sonuç ayrıca bildirilecek" in report
    assert "• binance: " in report and "• breakout: " in report
    assert not env.engine._open_lock.locked() and not env.engine.trade_lock(trade.id).locked()
    assert late_reports(env) == []
    # (worded so that it is true whether or not the order itself has already gone out)
    assert "• tickmill: ⏳ açılış sürüyor, broker henüz yanıt vermedi" in await env.engine.positions_text()

    # The next signal opens on the fast brokers; tickmill is busy and gets no second concurrent order.
    await env.say(eth(), KRIPTO, "eth 1R long")
    second = (await env.trades())[1]
    assert await env.brokers_of(second.id) == {"binance": "open", "breakout": "open"}
    assert len(env.binance.opened) == 2 and len(env.tickmill.opened) == 1 and env.tickmill.most_running == 1
    assert "tickmill önceki emrine hâlâ yanıt vermedi; bu işlem tickmill'da açılmadı" in env.texts(Severity.ERROR)

    # tickmill answers: booked under the trade lock, rolled up and reported (notify_on_execution is off).
    answer.set()
    await env.until(lambda: late_reports(env) != [], "the late report")
    (late,) = late_reports(env)
    assert late.startswith("✅ <b>#1 BTC LONG</b> — tickmill geç yanıt verdi\n• tickmill: ") and "BTCUSD" in late
    assert await env.brokers_of(trade.id) == {"binance": "open", "breakout": "open", "tickmill": "open"}
    after = await env.trade(trade.id)
    assert after.status is TradeStatus.OPEN and not after.note
    await env.until(lambda: not env.engine._busy(), "the watcher to end")
    assert env.engine._late_legs == {} and await env.db.get_state("late_open_legs") == {}


async def test_c2_next_signal_waits_for_the_busy_broker_and_opens_after_it(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.4, raising=False)
    answer = env.tickmill.hold("open_position")
    await env.say(btc(), KRIPTO, "btc 1R long")          # tickmill is late
    second = asyncio.ensure_future(env.say(eth(), KRIPTO, "eth 1R long"))
    await asyncio.sleep(0.1)                             # the new leg is waiting for the busy broker
    assert len(env.tickmill.opened) == 1 and len(env.binance.opened) == 2
    answer.set()                                         # the first order is answered within the wait
    await second
    assert len(env.tickmill.opened) == 2 and env.tickmill.most_running == 1     # one after the other
    assert (await env.brokers_of(2))["tickmill"] == "open"
    assert not any("önceki emrine" in t for t in env.texts())


async def test_c2_late_leg_that_answers_after_the_trade_ended_is_closed_at_once(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    answer = env.tickmill.hold("open_position")
    await env.say(btc(), KRIPTO, "btc 1R long")
    (trade,) = await env.trades()

    def stopped_out(pos: BrokerPosition) -> BrokerPosition:
        return pos.model_copy(update={"status": PositionStatus.CLOSED, "remaining_quantity": 0.0,
                                      "realized_pnl": -10.0})

    env.binance.sync_fn = env.breakout.sync_fn = stopped_out      # both open legs hit their stop meanwhile
    await env.engine.monitor.run_once()
    assert (await env.trade(trade.id)).status is TradeStatus.CLOSED

    answer.set()
    await env.until(lambda: late_reports(env) != [], "the late report")
    (late,) = late_reports(env)
    assert late.startswith("⚠️ <b>#1 BTC LONG</b> — tickmill geç yanıt verdi")
    assert "işlem bu arada kapandı; geç açılan bu pozisyon hemen kapatıldı" in late
    assert len(env.tickmill.calls_of("close_position")) == 1
    assert await env.brokers_of(trade.id) == {"binance": "closed", "breakout": "closed", "tickmill": "closed"}
    assert (await env.trade(trade.id)).status is TradeStatus.CLOSED


async def test_c2_late_leg_that_cannot_be_closed_is_reported_with_broker_symbol_and_size(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    answer = env.tickmill.hold("open_position")
    await env.say(btc(), KRIPTO, "btc 1R long")
    (trade,) = await env.trades()
    await env.say(manage(ActionType.CLOSE, base_asset="BTC", close_fraction=1.0), KRIPTO, "btc kapattık")
    assert (await env.trade(trade.id)).status is TradeStatus.CLOSED          # the trader closed it meanwhile
    env.tickmill.fail["close_position"] = BrokerError("piyasa kapalı")

    answer.set()
    await env.until(lambda: late_reports(env) != [], "the late report")
    (late,) = late_reports(env)
    size = orch._qty(env.tickmill.opened[0].quantity)
    assert late.startswith("🚨") and env.notifier.notifications[-1][1] is Severity.ERROR
    assert f"tickmill hesabındaki {size} BTCUSD LONG pozisyonu KAPATILAMADI" in late
    assert "piyasa kapalı" in late and "tickmill hesabında ELLE kapatın" in late
    # It is on record and the trade is active again: monitored, and the next instruction can manage it.
    assert (await env.brokers_of(trade.id))["tickmill"] == "open"
    assert (await env.trade(trade.id)).status is TradeStatus.OPEN


async def test_c2_trade_stays_opening_while_only_the_late_broker_can_still_open_it(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    env.fx.fail["get_quote"] = BrokerError("EURUSD için fiyat yok (piyasa kapalı olabilir).")
    answer = env.tickmill.hold("open_position")

    await env.say(fx_pair("EURUSD"), FX, "EURUSD LONG 1R")

    (trade,) = await env.trades()
    assert trade.status is TradeStatus.OPENING and trade.note == "fx: error; tickmill: late"
    (report,) = [t for t in env.texts(Severity.WARNING) if "henüz hiçbir brokerda açılmadı" in t]
    assert "• fx: " in report and "• ⏳ tickmill: yanıt bekleniyor; sonuç ayrıca bildirilecek" in report
    assert not env.engine._open_lock.locked() and not env.engine.trade_lock(trade.id).locked()
    assert not any("hiçbir brokerda açılamadı" in t for t in env.texts())

    # Even if a monitor pass has meanwhile marked the trade FAILED (no stored position is active), the late
    # position is not thrown away: FAILED only means that no OTHER broker opened it.
    await env.db.update_trade(trade.id, status=TradeStatus.FAILED)
    answer.set()
    await env.until(lambda: late_reports(env) != [], "the late report")
    (late,) = late_reports(env)
    assert late.startswith("✅ <b>#1 EUR LONG</b> — tickmill geç yanıt verdi")
    assert await env.brokers_of(trade.id) == {"tickmill": "open"}
    after = await env.trade(trade.id)
    assert after.status is TradeStatus.OPEN and after.note == "fx: error"
    assert env.tickmill.calls_of("close_position") == []


async def test_c2_late_leg_that_does_not_open_either_ends_the_trade_as_failed(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    env.fx.fail["get_quote"] = BrokerError("EURUSD için fiyat yok (piyasa kapalı olabilir).")
    slow = env.tickmill.hold("get_quote")
    await env.say(fx_pair("EURUSD"), FX, "EURUSD LONG 1R")
    (trade,) = await env.trades()
    assert trade.status is TradeStatus.OPENING

    env.tickmill.fail["get_quote"] = BrokerError("MetaApi fiyat: 120 sn içinde yanıt gelmedi.")
    slow.set()
    await env.until(lambda: late_reports(env) != [], "the late report")
    (late,) = late_reports(env)
    assert late.startswith("🚨") and "120 sn içinde yanıt gelmedi" in late
    assert "• Bu işlem hiçbir brokerda açılamadı." in late
    after = await env.trade(trade.id)
    assert after.status is TradeStatus.FAILED and after.note == "fx: error; tickmill: error"


async def test_c2_close_instruction_while_the_only_leg_is_in_flight_closes_it_on_arrival(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    env.fx.fail["get_quote"] = BrokerError("EURUSD için fiyat yok (piyasa kapalı olabilir).")
    answer = env.tickmill.hold("open_position")
    await env.say(fx_pair("EURUSD"), FX, "EURUSD LONG 1R")
    (trade,) = await env.trades()
    assert trade.status is TradeStatus.OPENING

    # The trader calls the trade off while tickmill has not answered: nothing to close yet, but remembered.
    result = await env.engine.execute_management(ManagementPlan(
        plan_id="m1", action=ActionType.CLOSE, trade_id=trade.id, close_fraction=1.0, source_chat_id=CHAT,
        summary_tr="kapat", created_at=datetime.now(UTC)))
    assert "• ⏳ tickmill: açılış emri henüz yanıtlanmadı; açılırsa hemen kapatılacak" in result

    answer.set()
    await env.until(lambda: late_reports(env) != [], "the late report")
    (late,) = late_reports(env)
    assert "trader işlemi kapattı / iptal etti; geç açılan bu pozisyon hemen kapatıldı" in late
    assert len(env.tickmill.calls_of("close_position")) == 1
    assert await env.brokers_of(trade.id) == {"tickmill": "closed"}
    assert (await env.trade(trade.id)).status is TradeStatus.CLOSED


async def test_c2_ignored_close_and_close_all_also_mark_the_late_leg(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    env.fx.fail["get_quote"] = BrokerError("EURUSD için fiyat yok (piyasa kapalı olabilir).")
    answer = env.tickmill.hold("open_position")
    await env.say(fx_pair("EURUSD"), FX, "EURUSD LONG 1R")
    (trade,) = await env.trades()
    await env.db.update_trade(trade.id, status=TradeStatus.FAILED)       # a monitor pass saw no active position

    # "eurusd kapattık" is about a trade that counts as over: ignored, but the leg in flight is marked.
    await env.say(manage(ActionType.CLOSE, symbol="EURUSD", base_asset="EUR", quote_asset="USD",
                         close_fraction=1.0), FX, "eurusd kapattık")
    (leg,) = env.engine._late_legs.values()
    assert leg.close_on_arrival == "trader işlemi kapattı / iptal etti"
    leg.close_on_arrival = None
    # /kapat while no trade is active: the order that has not been answered yet is still reached, and named.
    text = await env.engine.close_all()
    assert text.splitlines() == [
        "Kapatılacak açık işlem yok.",
        "• ⏳ #1 EUR LONG — tickmill: açılışa henüz yanıt gelmedi; açılırsa hemen kapatılacak"]
    assert leg.close_on_arrival == "tüm işlemler kapatıldı (/kapat)"
    leg.close_on_arrival = None
    await env.db.update_trade(trade.id, status=TradeStatus.OPENING)
    text = await env.engine.close_all()                                    # /kapat with the trade still active
    assert leg.close_on_arrival == "tüm işlemler kapatıldı (/kapat)"
    assert "• ⏳ tickmill: açılış emri henüz yanıtlanmadı; açılırsa hemen kapatılacak" in text
    assert "açılışa henüz yanıt gelmedi" not in text                      # named once, with its trade

    answer.set()
    await env.until(lambda: late_reports(env) != [], "the late report")
    assert len(env.tickmill.calls_of("close_position")) == 1
    assert await env.brokers_of(trade.id) == {"tickmill": "closed"}


async def test_c2_close_all_without_anything_in_flight_answers_as_before(env: Env) -> None:
    assert await env.engine.close_all() == "Kapatılacak açık işlem yok."


async def test_c2_ignored_change_of_a_trade_whose_only_leg_is_in_flight_is_reported_when_it_answers(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    env.fx.fail["get_quote"] = BrokerError("EURUSD için fiyat yok (piyasa kapalı olabilir).")
    answer = env.tickmill.hold("open_position")
    signal_id = await env.say(fx_pair("EURUSD"), FX, "EURUSD LONG 1R")
    (trade,) = await env.trades()
    await env.db.update_trade(trade.id, status=TradeStatus.FAILED)       # a monitor pass saw no active position

    # A reply "stop 1.1610" to the signal of a trade that counts as not opened: ignored, but remembered for the
    # leg in flight.
    await env.say(manage(ActionType.MOVE_SL, symbol="EURUSD", base_asset="EUR", quote_asset="USD",
                         new_stop_loss=1.1610, summary_tr="stop 1.1610"), FX, "stop 1.1610", reply_to=signal_id)
    assert env.notifier.confirmations == []
    (leg,) = env.engine._late_legs.values()
    assert leg.missed == ["stop 1.1610"] and leg.close_on_arrival is None

    answer.set()
    await env.until(lambda: late_reports(env) != [], "the late report")
    (late,) = late_reports(env)
    assert "değişiklikler bu pozisyona UYGULANMADI: stop 1.1610" in late
    assert env.notifier.notifications[-1][1] is Severity.ERROR
    assert await env.brokers_of(trade.id) == {"tickmill": "open"}        # kept: nobody closed the trade


async def test_c2_close_that_finds_the_trade_no_longer_active_still_marks_the_leg_in_flight(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    env.fx.fail["get_quote"] = BrokerError("EURUSD için fiyat yok (piyasa kapalı olabilir).")
    answer = env.tickmill.hold("open_position")
    await env.say(fx_pair("EURUSD"), FX, "EURUSD LONG 1R")
    (trade,) = await env.trades()
    # An unsure "eurusd kapattık" is asked about while the trade still counts as being sent ...
    await env.say(closure("EURUSD", "EUR", "USD", confidence=0.5), FX, "eurusd kapattık")
    (conf,) = env.notifier.confirmations
    # ... and approved after a monitor pass has marked the trade as not opened (no position anywhere yet).
    await env.db.update_trade(trade.id, status=TradeStatus.FAILED)
    text = await env.engine.handle_confirmation(conf.id, True)
    note = " ⏳ tickmill: bu işlemin açılışına henüz yanıt vermedi; açılırsa hemen kapatılacak."
    assert text == f"⛔ #{trade.id} işlemi artık aktif değil; değişiklik uygulanmadı.{note}"
    (leg,) = env.engine._late_legs.values()
    assert leg.close_on_arrival == "trader işlemi kapattı / iptal etti"
    # The same through execute_management itself (a plan that reaches it after the trade stopped being active).
    leg.close_on_arrival = None
    result = await env.engine.execute_management(ManagementPlan(
        plan_id="m2", action=ActionType.CANCEL, trade_id=trade.id, source_chat_id=CHAT, summary_tr="iptal",
        created_at=datetime.now(UTC)))
    assert result == f"#{trade.id} işlemi aktif değil (failed).{note}" and leg.close_on_arrival is not None
    # A change that does not end the trade marks nothing.
    leg.close_on_arrival = None
    result = await env.engine.execute_management(ManagementPlan(
        plan_id="m3", action=ActionType.MOVE_SL, trade_id=trade.id, new_stop_loss=1.161, source_chat_id=CHAT,
        summary_tr="stop", created_at=datetime.now(UTC)))
    assert result == f"#{trade.id} işlemi aktif değil (failed)." and leg.close_on_arrival is None

    leg.close_on_arrival = "trader işlemi kapattı / iptal etti"
    answer.set()
    await env.until(lambda: late_reports(env) != [], "the late report")
    assert len(env.tickmill.calls_of("close_position")) == 1
    assert await env.brokers_of(trade.id) == {"tickmill": "closed"}


async def test_c2_late_leg_that_registers_a_virtual_entry_writes_it_under_the_trade_lock(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    slow = env.tickmill.hold("resolve_symbol")           # the account does not even answer the symbol lookup
    await env.say(btc(EntryType.LIMIT), KRIPTO, "btc 1R long limit emir")
    (trade,) = await env.trades()
    assert trade.status is TradeStatus.PENDING and trade.note == "tickmill: late"
    assert await env.brokers_of(trade.id) == {"binance": "pending", "breakout": "pending"}
    async with env.engine.trade_lock(trade.id):          # e.g. the monitor is syncing this trade right now
        slow.set()
        await asyncio.sleep(0.1)
        assert "tickmill" not in await env.brokers_of(trade.id)
    await env.until(lambda: late_reports(env) != [], "the late report")
    assert (await env.brokers_of(trade.id))["tickmill"] == "virtual"
    (late,) = late_reports(env)
    assert late.startswith("✅ <b>#1 BTC LONG</b> — tickmill geç yanıt verdi\n• tickmill: sanal tetik kuruldu")
    assert env.tickmill.opened == []


async def test_c2_virtual_entry_is_not_sent_while_its_account_owes_the_answer_to_another_open(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    await env.say(btc(EntryType.LIMIT), KRIPTO, "btc 1R long limit emir")     # tickmill: a waiting virtual entry
    (first,) = await env.trades()
    assert (await env.brokers_of(first.id))["tickmill"] == "virtual"
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    answer = env.tickmill.hold("open_position")
    await env.say(eth(), KRIPTO, "eth 1R long")                               # tickmill's ETH order: no answer yet
    assert len(env.tickmill.opened) == 1 and env.engine._leg_running.get("tickmill") is not None
    env.notifier.notifications.clear()

    env.ref.prices["BTC"] = 72995.0                                           # the BTC level is touched meanwhile
    env.tickmill.quotes["BTCUSD"] = q(72944.0, 72946.0)
    for _ in range(2):                    # (a look that sent the order would wait for the held answer: timeout)
        await asyncio.wait_for(env.engine.monitor.run_virtual_once(), 3)
    assert len(env.tickmill.opened) == 1 and env.tickmill.most_running == 1   # no second open at the same time
    pos = await env.position(first.id, "tickmill")
    assert is_virtual_pending(pos) and pos.data["virtual_state"] == "waiting"
    (told,) = env.texts()                                                     # once, not at every look
    assert told.startswith("🧭 #1 BTC LONG tickmill: tetik seviyesine gelindi ama tickmill önceki bir emrine hâlâ "
                           "yanıt vermedi")
    assert env.notifier.notifications[0][1] is Severity.WARNING

    answer.set()                                                              # the ETH order is answered
    await env.until(lambda: late_reports(env) != [], "the late report")
    await env.until(lambda: env.engine._leg_running == {}, "the leg to end")
    await env.engine.monitor.run_virtual_once()                               # still at the level: now it opens
    assert len(env.tickmill.opened) == 2 and env.tickmill.most_running == 1
    assert (await env.position(first.id, "tickmill")).status is PositionStatus.OPEN
    assert (first.id, "tickmill:busy") not in env.engine._virtual_warned


async def test_c2_virtual_entry_waits_quietly_for_an_open_that_is_only_just_in_flight(env: Env) -> None:
    await env.say(btc(EntryType.LIMIT), KRIPTO, "btc 1R long limit emir")     # tickmill: a waiting virtual entry
    (first,) = await env.trades()
    answer = env.tickmill.hold("open_position")
    second = asyncio.ensure_future(env.say(eth(), KRIPTO, "eth 1R long"))     # deadline 30 s: far from "late"
    await asyncio.wait_for(env.tickmill.waiting.wait(), 2)
    env.notifier.notifications.clear()
    env.ref.prices["BTC"] = 72995.0
    env.tickmill.quotes["BTCUSD"] = q(72944.0, 72946.0)
    await asyncio.wait_for(env.engine.monitor.run_virtual_once(), 3)
    assert len(env.tickmill.opened) == 1 and env.texts() == []               # waits for the next look, no alarm
    answer.set()
    await second
    await env.engine.monitor.run_virtual_once()
    assert len(env.tickmill.opened) == 2 and env.tickmill.most_running == 1
    assert (await env.position(first.id, "tickmill")).status is PositionStatus.OPEN


async def test_c2_new_signal_waits_for_a_virtual_entry_that_is_being_sent_on_the_same_account(env: Env) -> None:
    await env.say(btc(EntryType.LIMIT), KRIPTO, "btc 1R long limit emir")
    (first,) = await env.trades()
    answer = env.tickmill.hold("open_position")
    env.ref.prices["BTC"] = 72995.0                                           # touched: the order goes out ...
    env.tickmill.quotes["BTCUSD"] = q(72944.0, 72946.0)
    firing = asyncio.ensure_future(env.engine.monitor.run_virtual_once())
    await asyncio.wait_for(env.tickmill.waiting.wait(), 2)                    # ... and is not answered yet
    assert len(env.tickmill.opened) == 1

    second = asyncio.ensure_future(env.say(eth(), KRIPTO, "eth 1R long"))     # a new signal for the same account
    await asyncio.sleep(0.1)
    assert len(env.binance.opened) == 2                                       # the owner's account: at once
    assert len(env.tickmill.opened) == 1                                      # tickmill: not a second open yet
    answer.set()
    await asyncio.wait_for(firing, 3)
    await second
    assert len(env.tickmill.opened) == 2 and env.tickmill.most_running == 1   # one after the other
    assert (await env.position(first.id, "tickmill")).status is PositionStatus.OPEN
    assert (await env.brokers_of(2))["tickmill"] == "open"
    assert env.engine._leg_running == {} and not any("önceki emrine" in t for t in env.texts())


async def test_c2_change_made_while_the_leg_was_in_flight_is_reported_as_not_applied(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    answer = env.tickmill.hold("open_position")
    await env.say(btc(), KRIPTO, "btc 1R long")
    (trade,) = await env.trades()
    await env.say(manage(ActionType.MOVE_SL, base_asset="BTC", new_stop_loss=73500.0, summary_tr="stop 73500"),
                  KRIPTO, "stop 73500")
    assert (await env.position(trade.id, "binance")).stop_loss == 73500.0

    answer.set()
    await env.until(lambda: late_reports(env) != [], "the late report")
    (late,) = late_reports(env)
    assert "değişiklikler bu pozisyona UYGULANMADI" in late and "tickmill hesabında elle kontrol edin" in late
    assert env.notifier.notifications[-1][1] is Severity.ERROR
    pos = await env.position(trade.id, "tickmill")
    assert pos.status is PositionStatus.OPEN and pos.stop_loss == pytest.approx(73250.0)   # 73300 shifted by -50


async def test_c2_late_leg_writes_its_position_under_the_trade_lock(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    answer = env.tickmill.hold("open_position")
    await env.say(btc(), KRIPTO, "btc 1R long")
    (trade,) = await env.trades()
    async with env.engine.trade_lock(trade.id):          # e.g. the monitor is syncing this trade right now
        answer.set()
        await asyncio.sleep(0.1)
        assert "tickmill" not in await env.brokers_of(trade.id)
    await env.until(lambda: late_reports(env) != [], "the late report")
    assert (await env.brokers_of(trade.id))["tickmill"] == "open"


async def test_c2_shutdown_waits_for_a_late_leg(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    answer = env.tickmill.hold("open_position")
    await env.say(btc(), KRIPTO, "btc 1R long")
    (trade,) = await env.trades()

    stopping = asyncio.ensure_future(env.engine.stop(grace_sec=5.0))
    await asyncio.sleep(0.15)
    assert not stopping.done() and not env.tickmill.closed and not env.binance.closed    # waiting for the leg
    answer.set()
    await asyncio.wait_for(stopping, 3)
    assert env.tickmill.closed and env.binance.closed
    assert (await env.brokers_of(trade.id))["tickmill"] == "open"          # booked before the brokers closed
    assert len(late_reports(env)) == 1
    assert not any("sn içinde bitmedi" in t for t in env.texts())


async def test_c2_shutdown_gives_up_on_a_late_leg_after_the_grace_period_and_says_so(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    answer = env.tickmill.hold("open_position")
    await env.say(btc(), KRIPTO, "btc 1R long")
    await env.engine.stop(grace_sec=0.1)
    (warning,) = [t for t in env.texts(Severity.WARNING) if "sn içinde bitmedi" in t]
    assert "late-" in warning and "-tickmill" in warning
    answer.set()
    await env.until(lambda: late_reports(env) != [], "the late report")
    # The brokers were closed under that leg: the note about it stays in storage for the restart notice.
    assert list(await env.db.get_state("late_open_legs")) == ["1:tickmill"]


async def test_c2_after_a_restart_the_owner_is_told_which_broker_had_not_answered(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    env.tickmill.hold("open_position")
    await env.say(btc(), KRIPTO, "btc 1R long")
    (trade,) = await env.trades()
    assert trade.status is TradeStatus.OPEN              # nothing else would show that an order is unanswered
    stored = await env.db.get_state("late_open_legs")
    assert list(stored) == ["1:tickmill"] and stored["1:tickmill"]["label"] == "#1 BTC LONG"

    # The process dies here (no clean stop). A new one starts on the same database.
    notifier = FakeNotifier()
    restarted = Engine(env.cfg, env.db, FakeParser(), [], notifier, price_oracle=env.oracle, reference_prices=env.ref)
    await restarted.start(monitor=False)
    assert [t for t in notifier.texts(Severity.WARNING) if "henüz yanıt gelmemişti" in t] == [
        "⚠️ #1 BTC LONG: bot durduğunda tickmill emrine henüz yanıt gelmemişti. Emrin açılıp açılmadığı bilinmiyor; "
        "tickmill hesabında kayıtsız emir veya pozisyon olabilir, elle kontrol edin."]
    assert await env.db.get_state("late_open_legs") == {}                  # told once
    await restarted.stop(grace_sec=1.0)


async def test_c2_owner_is_told_once_when_new_trades_have_been_blocked_too_long(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orch, "OPEN_LOCK_ALERT_SEC", 0.1, raising=False)
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.3, raising=False)
    slow = env.tickmill.hold("get_equity")               # the lock is held for the 0.3 s deadline: longer than 0.1 s
    await env.say(btc(), KRIPTO, "btc 1R long")
    await asyncio.sleep(0.15)
    alerts = [t for t in env.texts(Severity.WARNING) if "saniyeden uzun sürdü" in t]
    assert len(alerts) == 1 and "BTCUSDT LONG" in alerts[0] and "yeni sinyaller sırada bekliyor" in alerts[0]

    slow.set()
    await env.until(lambda: late_reports(env) != [], "the late report")
    env.notifier.notifications.clear()
    await env.say(fx_pair("GBPUSD"), FX, "GBPUSD LONG 1R")     # everybody answers at once
    await asyncio.sleep(0.15)
    assert not any("saniyeden uzun sürdü" in t for t in env.texts())


# =========================================================================== #
# C3 - management messages are matched by PAIR for forex / CFD trades
# =========================================================================== #
def closure(symbol: str | None, base: str, quote: str | None = None, **kw: Any) -> ParsedAction:
    """ "<pair> kapattık" as the model reads a closure report that is not a reply."""
    return manage(ActionType.CLOSE, symbol=symbol, base_asset=base, quote_asset=quote, close_fraction=1.0,
                  summary_tr=f"{symbol or base} kapatıldı", **kw)


def closes(env: Env) -> dict[str, int]:
    return {name: len(b.calls_of("close_position")) for name, b in env.brokers.items() if b.calls_of("close_position")}


async def test_c3_gbpjpy_close_never_closes_the_only_open_gbpusd(env: Env) -> None:
    await env.say(fx_pair("GBPUSD"), FX, "GBPUSD LONG 1R")
    (trade,) = await env.trades()

    await env.say(closure("GBPJPY", "GBP", "JPY", market_type=MarketType.FOREX), FX, "gbpjpy kapattık")

    assert closes(env) == {}
    assert await env.brokers_of(trade.id) == {"fx": "open", "tickmill": "open"}
    (conf,) = env.notifier.confirmations                 # like an instruction for an asset without a trade
    assert conf.plan is None and [i.code for i in conf.issues] == ["trade_not_found"]


async def test_c3_usdcad_close_never_closes_the_only_open_usdjpy(env: Env) -> None:
    await env.say(fx_pair("USDJPY"), FX, "USDJPY LONG 1R")
    (trade,) = await env.trades()

    await env.say(closure("USDCAD", "USD"), FX, "USDCAD kapattık")       # the quote comes from the symbol

    assert closes(env) == {}
    assert await env.brokers_of(trade.id) == {"fx": "open", "tickmill": "open"}
    (conf,) = env.notifier.confirmations
    assert conf.plan is None and [i.code for i in conf.issues] == ["trade_not_found"]


async def test_c3_with_both_pairs_open_only_the_named_one_is_managed(env: Env) -> None:
    await env.say(fx_pair("GBPUSD"), FX, "GBPUSD LONG 1R")
    await env.say(fx_pair("GBPJPY"), FX, "GBPJPY LONG 1R")
    usd, jpy = await env.trades()

    await env.say(closure("GBPJPY", "GBP", "JPY"), FX, "gbpjpy kapattık")

    assert await env.brokers_of(jpy.id) == {"fx": "closed", "tickmill": "closed"}
    assert await env.brokers_of(usd.id) == {"fx": "open", "tickmill": "open"}
    assert env.notifier.confirmations == []


async def test_c3_instruction_without_a_quote_still_finds_the_only_trade_of_that_base(env: Env) -> None:
    await env.say(fx_pair("GBPUSD"), FX, "GBPUSD LONG 1R")
    (trade,) = await env.trades()
    await env.say(closure(None, "GBP"), FX, "gbp kapattık")
    assert await env.brokers_of(trade.id) == {"fx": "closed", "tickmill": "closed"}
    assert env.notifier.confirmations == []


async def test_c3_explicit_reference_to_a_trade_of_another_pair_is_asked_not_applied(env: Env) -> None:
    await env.say(fx_pair("GBPUSD"), FX, "GBPUSD LONG 1R")
    (trade,) = await env.trades()

    await env.say(closure("GBPJPY", "GBP", "JPY", target_trade_id=trade.id), FX, "gbpjpy kapattık")

    assert closes(env) == {}
    (conf,) = env.notifier.confirmations
    (issue,) = [i for i in conf.issues if i.code == "asset_mismatch"]
    assert "GBPJPY" in issue.message_tr and "GBPUSD" in issue.message_tr


async def test_c3_pair_written_into_base_asset_is_read_as_base_and_quote(env: Env) -> None:
    assert orch.managed_asset(closure(None, "GBPUSD")) == ("GBP", "USD", MarketType.FOREX)
    assert orch.managed_asset(closure(None, "xauusd")) == ("XAU", "USD", MarketType.CFD)
    assert orch.managed_asset(closure("GBPJPY", "GBPJPY")) == ("GBP", "JPY", MarketType.FOREX)
    assert orch.managed_asset(closure(None, "BTCUSDT")) == ("BTCUSDT", None, None)      # a coin name: as before
    assert orch.managed_asset(closure("BTCUSDT.P", "BTC")) == ("BTC", "USDT", MarketType.PERP)
    assert orch.managed_asset(closure(None, "NAS100")) == ("NAS100", None, None)

    await env.say(fx_pair("GBPUSD"), FX, "GBPUSD LONG 1R")
    (trade,) = await env.trades()
    # The model copies the "asset" of OPEN TRADES ("GBPUSD") into base_asset: another pair is still refused...
    await env.say(closure(None, "GBPJPY"), FX, "gbpjpy kapattık")
    assert closes(env) == {} and len(env.notifier.confirmations) == 1
    # ... and the trade's own pair is managed without a question.
    await env.say(closure(None, "GBPUSD", target_trade_id=trade.id), FX, "gbpusd kapattık")
    assert await env.brokers_of(trade.id) == {"fx": "closed", "tickmill": "closed"}
    assert len(env.notifier.confirmations) == 1


async def test_c3_closure_report_for_another_pair_is_not_taken_for_the_finished_trade(env: Env) -> None:
    await env.say(fx_pair("GBPUSD"), FX, "GBPUSD LONG 1R")
    await env.say(closure("GBPUSD", "GBP", "USD"), FX, "gbpusd kapattık")
    (trade,) = await env.trades()
    assert trade.status is TradeStatus.CLOSED

    await env.say(closure("GBPUSD", "GBP", "USD"), FX, "gbpusd kapandı")      # about the finished trade: ignored
    assert env.notifier.confirmations == []
    await env.say(closure("GBPJPY", "GBP", "JPY"), FX, "gbpjpy kapattık")     # another pair: not "already closed"
    (conf,) = env.notifier.confirmations
    assert [i.code for i in conf.issues] == ["trade_not_found"]


@pytest.mark.parametrize(("symbol", "quote"), [
    (None, None), ("BTCUSDT", "USDT"), ("BTCUSD", "USD"), ("BTC/USDC", "USDC"), ("BTCEUR", "EUR")])
async def test_c3_a_coin_is_still_matched_by_its_base_whatever_quote_is_named(
        env: Env, symbol: str | None, quote: str | None) -> None:
    await env.say(btc(), KRIPTO, "btc 1R long")
    (trade,) = await env.trades()
    await env.say(closure(symbol, "BTC", quote), KRIPTO, "btc kapattık")
    assert await env.brokers_of(trade.id) == {"binance": "closed", "breakout": "closed", "tickmill": "closed"}
    assert env.notifier.confirmations == []


async def test_c3_owner_labels_show_the_pair_and_the_parser_context_gives_base_plus_quote(env: Env) -> None:
    # (Was "..._summary_and_open_trades_context_show_the_pair_...": the first version of this batch put the pair
    # into summary()["asset"]. The previous batch added a separate "quote" key instead and its prompt explains
    # exactly that to the model ("asset USD with quote JPY is USDJPY"); the coordinator kept that design. So
    # "asset" stays the base for every instrument, "quote" is added when the plan names one, and the pair as one
    # word (instrument_label) is only for texts shown to the owner.)
    def label(base: str, quote: str | None, market_type: MarketType | None) -> str:
        return models.instrument_label(plan_of(base, quote, market_type=market_type))

    assert label("GBP", "USD", MarketType.FOREX) == "GBPUSD" and label("USD", "JPY", None) == "USDJPY"
    assert label("XAU", "USD", MarketType.CFD) == "XAUUSD" and label("EUR", None, MarketType.FOREX) == "EUR"
    assert label("NAS100", "USD", MarketType.CFD) == "NAS100"            # an index is not a pair
    for coin in (("BTC", "USDT", MarketType.PERP), ("BTC", "USD", None), ("BTC", "USDT", MarketType.SPOT),
                 ("ETH", None, None), ("1000PEPE", "USDT", MarketType.PERP)):
        assert label(*coin) == coin[0]                                    # crypto: the base alone

    await env.say(fx_pair("GBPUSD"), FX, "GBPUSD LONG 1R")
    await env.say(fx_pair("XAUUSD"), FX, "XAUUSD LONG 1R")
    await env.say(btc(), KRIPTO, "btc 1R long")
    gbp, xau, coin_trade = await env.trades()
    # What the model gets: base and quote separately, for forex / CFD and for coins alike.
    assert (gbp.summary()["asset"], gbp.summary()["quote"]) == ("GBP", "USD")
    assert (xau.summary()["asset"], xau.summary()["quote"]) == ("XAU", "USD")
    summary = coin_trade.summary(tp_hit=[False], open_fraction=1.0)
    assert (summary["asset"], summary["quote"], summary["side"]) == ("BTC", "USDT", "long")
    assert sorted(summary) == ["asset", "entry", "open_fraction", "opened_at", "quote", "side", "source_message_ids",
                               "status", "stop_loss", "take_profits", "trade_id"]
    no_quote = TradeRecord(id=9, plan=plan_of("ETH", None), status=TradeStatus.OPEN, current_stop_loss=90.0,
                           created_at=datetime.now(UTC), updated_at=datetime.now(UTC))
    assert "quote" not in no_quote.summary() and no_quote.summary()["asset"] == "ETH"
    fx_route = next(r for r in env.cfg.telegram.topics if r.name == "fx")
    kripto_route = next(r for r in env.cfg.telegram.topics if r.name == "kripto")
    ctx = await env.engine._parse_context(fx_route, TITLES[FX])
    assert sorted((t["asset"], t["quote"]) for t in ctx.open_trades) == [("GBP", "USD"), ("XAU", "USD")]
    ctx = await env.engine._parse_context(kripto_route, TITLES[KRIPTO])
    assert [(t["asset"], t["quote"]) for t in ctx.open_trades] == [("BTC", "USDT")]
    # What the owner reads: the pair (here in the list of the trades that fill max_open_trades).
    listed = Engine._limit_filled_by([gbp, xau, coin_trade])
    assert "#1 GBPUSD LONG (açık · fx), #2 XAUUSD LONG (açık · fx), #3 BTC LONG (açık · kripto)" in listed


# =========================================================================== #
# C4 - daily-loss guard including what is still at risk
# =========================================================================== #
def held(*, side: Side = Side.LONG, status: PositionStatus = PositionStatus.OPEN, quantity: float = 1.0,
         filled: float | None = None, remaining: float | None = None, entry: float | None = 100.0,
         stop: float | None = 90.0, risk: float | None = 100.0,
         basis: tuple[float, float, float] | None = (100.0, 90.0, 1.0), loss: float | None = None,
         **data: Any) -> BrokerPosition:
    """A position sized to lose ``risk`` when ``basis`` = (entry, stop, quantity) is stopped out (``loss``: what
    the rounded size really loses there, stored by the engine only when it is more than ``risk``)."""
    is_open = status is PositionStatus.OPEN
    filled = (quantity if is_open else 0.0) if filled is None else filled
    if basis is not None:
        data["risk_basis"] = {"entry": basis[0], "stop": basis[1], "quantity": basis[2]}
        if loss is not None:
            data["risk_basis"]["loss"] = loss
    return BrokerPosition(
        broker="fx", broker_symbol="EURUSD", side=side, status=status, quantity=quantity, filled_quantity=filled,
        remaining_quantity=filled if remaining is None else remaining,
        avg_entry_price=entry if is_open else None, stop_loss=stop, risk_amount=risk, data=data)


@pytest.mark.parametrize(("pos", "expected"), [
    (held(), 100.0),                                               # at the stop it was sized for
    (held(stop=95.0), 50.0),                                       # stop moved half-way
    (held(stop=100.0), 0.0),                                       # stop at the entry: nothing left to lose
    (held(stop=104.0), 0.0),                                       # ... or beyond it: never negative
    (held(stop=80.0), 200.0),                                      # stop moved away: more at risk
    (held(remaining=0.5), 50.0),                                   # half closed
    (held(entry=101.0), 110.0),                                    # filled worse than the sizing price
    (held(entry=93.0), 30.0),                                      # size was capped: sized on 10, really 3 from the stop
    (held(quantity=2.0), 200.0),                                   # the adapter opened twice the planned size
    (held(quantity=1.0, filled=0.4), 100.0),                       # partly filled limit: the rest may still fill
    (held(status=PositionStatus.PENDING), 100.0),                  # resting limit order: no fill price yet
    (held(status=PositionStatus.PENDING, stop=95.0), 50.0),
    (held(side=Side.SHORT, entry=100.0, stop=110.0, basis=(100.0, 110.0, 1.0)), 100.0),
    (held(side=Side.SHORT, entry=100.0, stop=100.0, basis=(100.0, 110.0, 1.0)), 0.0),
    (held(side=Side.SHORT, entry=100.0, stop=95.0, basis=(100.0, 110.0, 1.0)), 0.0),
    (held(basis=None), 100.0),                                     # stored by an older version: the planned risk
    (held(basis=None, remaining=0.25), 25.0),
    (held(basis=None, stop=100.0), 0.0),
    (held(stop=None), 100.0),                                      # no stop known: the planned risk counts
    (held(risk=None), 0.0),                                        # a waiting virtual entry: checked when it triggers
    (held(loss=150.0), 150.0),                                     # raised to the minimum lot: really loses 150
    (held(loss=150.0, stop=95.0), 75.0),
    (held(loss=150.0, stop=100.0), 0.0),
    (held(loss=80.0), 100.0),                                      # never below the target it was sized for
    (held(status=PositionStatus.CLOSED), 0.0),
    (held(status=PositionStatus.FAILED), 0.0),
])
def test_c4_position_open_risk(pos: BrokerPosition, expected: float) -> None:
    assert guards.position_open_risk(pos) == pytest.approx(expected)


def test_c4_daily_loss_would_exceed_counts_realized_loss_open_risk_and_the_new_trade() -> None:
    exceed = guards.daily_loss_would_exceed
    assert exceed(0.0, 200.0, 100.0, 10_000.0, 3.0) is False        # 300 of 300: exactly the limit is allowed
    assert exceed(0.0, 200.01, 100.0, 10_000.0, 3.0) is True
    assert exceed(-100.0, 100.0, 100.0, 9_900.0, 3.0) is True       # 300 > 297
    assert exceed(-100.0, 97.0, 100.0, 9_900.0, 3.0) is False
    assert exceed(+500.0, 200.0, 100.0, 10_000.0, 3.0) is False     # a realized profit is not credited
    assert exceed(+500.0, 201.0, 100.0, 10_000.0, 3.0) is True
    assert exceed(0.0, 0.0, 100.0, 0.0, 3.0) is True                # no usable equity: fail safe
    assert exceed(-900.0, 900.0, 900.0, 10_000.0, 0.0) is False     # limit 0 = off
    assert guards.daily_loss_with_open_risk(-100.0, 97.0, 100.0) == pytest.approx(297.0)
    assert guards.open_risk([held(), held(stop=95.0), held(risk=None)]) == pytest.approx(150.0)


def test_c4_config_option_has_the_agreed_name_and_is_off_by_default() -> None:
    assert all(b.daily_loss_include_open_risk is False for b in prod_config().brokers)
    data = yaml.safe_load(PROD_YAML)
    data["brokers"][2]["daily_loss_include_open_risk"] = True
    cfg = AppConfig.model_validate(data)
    assert [b.daily_loss_include_open_risk for b in cfg.brokers] == [False, False, True, False]
    assert PaperBrokerConfig(type="paper", name="x").daily_loss_include_open_risk is False


async def test_c4_open_risk_counts_towards_the_daily_limit_when_the_option_is_on(env: Env) -> None:
    env.fx.config.daily_loss_include_open_risk = True    # FundingPips: 3% of 10 000 = 300 USD, 1R = 100 USD
    limit_order = signal("EUR", "USD", Side.LONG, 1.1600, 1.1580, 1.1640, symbol="EURUSD",
                         market_type=MarketType.FOREX, entry_type=EntryType.LIMIT)
    await env.say(limit_order, FX, "EURUSD LONG 1R limit")          # 0 +   0 + 100: resting order
    await env.say(fx_pair("GBPUSD"), FX, "GBPUSD LONG 1R")          # 0 + 100 + 100
    await env.say(fx_pair("USDJPY"), FX, "USDJPY LONG 1R")          # 0 + 200 + 100 = 300: exactly the limit
    first, second, third = await env.trades()
    assert len(env.fx.opened) == 3
    assert (await env.position(first.id, "fx")).status is PositionStatus.PENDING
    for trade in (first, second, third):
        assert "risk_basis" in (await env.position(trade.id, "fx")).data
    assert "risk_basis" not in (await env.position(second.id, "tickmill")).data      # option off there
    env.notifier.notifications.clear()

    await env.say(fx_pair("AUDUSD"), FX, "AUDUSD LONG 1R")          # 0 + 300 + 100 = 400 > 300
    assert len(env.fx.opened) == 3 and len(env.tickmill.opened) == 4     # tickmill (limit 0 = off) still opens
    assert [t for t in env.texts(Severity.WARNING) if "günlük zarar" in t] == [
        "fx: günlük zarar sınırı (%3 = 300.00 USD) aşılırdı: bugün gerçekleşen zarar 0.00 + açık ve bekleyen "
        "pozisyonların stoptaki riski 300.00 + bu işlemin riski 100.00 = 400.00 USD; yeni işlem açılmadı"]

    # GBPUSD goes to break-even (stop at the entry): it has nothing left to lose -> 0 + 200 + 100 = 300.
    env.fx.quotes["GBPUSD"] = env.tickmill.quotes["GBPUSD"] = q(1.3190, 1.3191)
    await env.engine.execute_management(ManagementPlan(
        plan_id="be", action=ActionType.MOVE_SL, trade_id=second.id, move_sl_to_entry=True,
        new_stop_loss=second.plan.entry_price, source_chat_id=CHAT, summary_tr="be", created_at=datetime.now(UTC)))
    assert guards.position_open_risk(await env.position(second.id, "fx")) == 0.0
    await env.say(fx_pair("NZDUSD"), FX, "NZDUSD LONG 1R")
    assert len(env.fx.opened) == 4

    # USDJPY is stopped out: -100 realized, equity 9 900 (limit 297). 100 + 200 (EURUSD order, NZDUSD) + 100 = 400.
    pos = await env.position(third.id, "fx")
    await env.db.save_broker_position(third.id, pos.model_copy(update={
        "status": PositionStatus.CLOSED, "remaining_quantity": 0.0, "realized_pnl": -100.0}))
    env.fx.equity = 9_900.0
    env.notifier.notifications.clear()
    await env.say(fx_pair("XAUUSD"), FX, "XAUUSD LONG 1R")
    assert len(env.fx.opened) == 4
    assert [t for t in env.texts(Severity.WARNING) if "günlük zarar" in t] == [
        "fx: günlük zarar sınırı (%3 = 297.00 USD) aşılırdı: bugün gerçekleşen zarar 100.00 + açık ve bekleyen "
        "pozisyonların stoptaki riski 200.00 + bu işlemin riski 100.00 = 400.00 USD; yeni işlem açılmadı"]


async def test_c4_size_raised_to_the_minimum_lot_counts_with_what_it_really_loses(env: Env) -> None:
    env.fx.config.daily_loss_include_open_risk = True
    real_size = env.fx.size_position

    async def rounded_up(symbol: SymbolInfo, side: Side, entry: float, stop: float, amount: float) -> SizingResult:
        size = await real_size(symbol, side, entry, stop, amount)
        # risk.allow_min_qty_round_up: the smallest lot loses 1.5 x the 100 USD target at the stop
        return size.model_copy(update={"est_loss_at_sl": amount * 1.5})

    env.fx.size_position = rounded_up                    # type: ignore[method-assign]
    await env.say(fx_pair("EURUSD"), FX, "EURUSD LONG 1R")          # 0 +   0 + 100
    await env.say(fx_pair("GBPUSD"), FX, "GBPUSD LONG 1R")          # 0 + 150 + 100
    first, second = await env.trades()
    pos = await env.position(first.id, "fx")
    assert pos.risk_amount == 100.0 and pos.data["risk_basis"]["loss"] == 150.0
    assert guards.position_open_risk(pos) == pytest.approx(150.0)
    assert len(env.fx.opened) == 2
    env.notifier.notifications.clear()
    await env.say(fx_pair("USDJPY"), FX, "USDJPY LONG 1R")          # 0 + 300 + 100 = 400 > 300
    assert len(env.fx.opened) == 2
    (told,) = [t for t in env.texts(Severity.WARNING) if "günlük zarar" in t]
    assert "açık ve bekleyen pozisyonların stoptaki riski 300.00 + bu işlemin riski 100.00 = 400.00 USD" in told
    # A size that was only rounded DOWN carries no "loss": the target counts (see test_c4_open_risk_counts_...).
    assert "loss" not in (await env.position(first.id, "tickmill")).data.get("risk_basis", {})


async def test_c4_with_the_option_off_only_the_realized_loss_counts_as_before(env: Env) -> None:
    for pair in ("EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD"):
        await env.say(fx_pair(pair), FX, f"{pair} LONG 1R")
    assert len(env.fx.opened) == 5 and env.texts() == []           # 500 USD of open risk at a 300 USD limit
    for trade in await env.trades():
        assert "risk_basis" not in (await env.position(trade.id, "fx")).data


# =========================================================================== #
# C5 - every routed broker that did not take the trade is named
# =========================================================================== #
async def test_c5_broker_left_out_by_allowed_assets_is_named_and_the_report_is_sent(env: Env) -> None:
    await env.say(sol(), KRIPTO, "sol 1R long")          # tickmill's allowed_assets does not list SOL

    (trade,) = await env.trades()
    assert await env.brokers_of(trade.id) == {"binance": "open", "breakout": "open"}
    assert trade.status is TradeStatus.OPEN and trade.note == "tickmill: filtered"
    (report,) = env.texts()                              # sent although notify_on_execution is off
    assert report.startswith("✅ <b>#1 SOL LONG açıldı</b>")
    assert report.splitlines()[-1] == "• tickmill: SOL bu hesabın listesinde yok, atlandı"
    assert env.notifier.notifications[0][1] is Severity.INFO


async def test_c5_all_routed_brokers_opened_stays_silent_when_notify_on_execution_is_off(env: Env) -> None:
    await env.say(btc(), KRIPTO, "btc 1R long")
    await env.say(fx_pair("EURUSD"), FX, "EURUSD LONG 1R")
    assert len(await env.trades()) == 2 and env.texts() == []


@pytest.mark.parametrize("why", ["unsupported", "hedge", "error"])
async def test_c5_a_routed_broker_that_did_not_open_is_reported_whatever_notify_on_execution_says(
        env: Env, why: str) -> None:
    if why == "unsupported":
        env.tickmill.unsupported.add("EUR")              # the symbol is not on that account
        line = "• tickmill: EUR bu hesapta işlem görmüyor"
    elif why == "hedge":
        other = await env.db.create_trade(plan_of("EUR", "USD", Side.SHORT, route="fx", brokers=["tickmill"],
                                                  entry=1.17, sl=1.18, market_type=MarketType.FOREX))
        await env.db.save_broker_position(other.id, BrokerPosition(
            broker="tickmill", broker_symbol="EURUSD", side=Side.SHORT, status=PositionStatus.OPEN, quantity=0.1,
            filled_quantity=0.1, remaining_quantity=0.1, avg_entry_price=1.17, stop_loss=1.18))
        await env.db.update_trade(other.id, status=TradeStatus.OPEN)
        line = "• tickmill: EUR: açık SHORT var; ters yönlü LONG sinyali hedging yasağı nedeniyle açılmadı"
    else:
        env.tickmill.fail["get_quote"] = BrokerError("MetaApi fiyat: 120 sn içinde yanıt gelmedi.")
        line = "• tickmill: <code>MetaApi fiyat: 120 sn içinde yanıt gelmedi.</code>"

    await env.say(fx_pair("EURUSD"), FX, "EURUSD LONG 1R")

    trade = (await env.trades())[-1]
    assert await env.brokers_of(trade.id) == {"fx": "open"}
    (report,) = [t for t in env.texts(Severity.INFO) if f"#{trade.id} EUR LONG açıldı" in t]
    assert line in report.splitlines() and any(x.startswith("• fx: ") for x in report.splitlines())


async def test_c5_routed_broker_that_is_not_connected_and_not_optional_is_named_for_every_signal(env: Env) -> None:
    # What Engine._connect leaves behind when the owner's own account cannot connect at startup (never retried).
    env.engine.brokers = [b for b in env.engine.brokers if b.name != "fx"]
    env.engine.failed_brokers["fx"] = "MT5 terminali açık değil"

    await env.say(fx_pair("EURUSD"), FX, "EURUSD LONG 1R")
    await env.say(btc(), KRIPTO, "btc 1R long")          # fx is not on that route: no word about it there

    eur, coin = await env.trades()
    assert await env.brokers_of(eur.id) == {"tickmill": "open"} and env.fx.opened == []
    assert env.texts(Severity.ERROR) == [
        "🚨 fx bağlı değil; bu işlem fx hesabında AÇILMADI (EUR LONG). Bu hesap, bot yeniden başlatılana kadar "
        "bağlanmaz."]
    assert (await env.brokers_of(coin.id)).keys() == {"binance", "breakout", "tickmill"}

    # An optional broker that is down keeps its own short notice and gets no second one.
    env.notifier.notifications.clear()
    env.engine.brokers = [b for b in env.engine.brokers if b.name != "tickmill"]
    env.engine.failed_brokers["tickmill"] = env.engine.optional_down["tickmill"] = "MetaApi bağlanamadı"
    await env.say(eth(), KRIPTO, "eth 1R long")
    assert env.texts(Severity.ERROR) == []
    assert env.texts(Severity.WARNING) == ["ℹ️ tickmill bağlı değil, bu işlem atlandı (ETH LONG)."]


async def test_c5_route_whose_brokers_all_leave_the_asset_out_names_each_of_them(env: Env) -> None:
    plan = plan_of("XYZ", "USDT", route="kripto", brokers=["breakout", "tickmill"], market_type=MarketType.PERP)
    text = await env.engine.execute_open(plan)
    (notice,) = env.texts(Severity.WARNING)
    assert "hiçbir broker bu varlığı işlemiyor" in notice
    for name in ("breakout", "tickmill"):
        assert f"• {name}: XYZ bu hesabın listesinde yok, atlandı" in notice.splitlines()
        assert f"• {name}: XYZ bu hesabın listesinde yok, atlandı" in text.splitlines()
    (trade,) = await env.trades()
    assert trade.status is TradeStatus.FAILED


# =========================================================================== #
# C7 - the max_open_trades notice names the trades that fill the limit
# =========================================================================== #
async def test_c7_max_open_trades_notice_names_the_trades_that_fill_the_limit(env: Env) -> None:
    env.cfg.risk.max_open_trades = 2
    await env.say(btc(EntryType.LIMIT), KRIPTO, "btc 1R long limit emir")     # a waiting entry counts too
    await env.say(fx_pair("EURUSD"), FX, "EURUSD LONG 1R")
    env.notifier.notifications.clear()

    await env.say(fx_pair("GBPUSD"), FX, "GBPUSD LONG 1R")                    # the third: both topics count

    assert len(await env.trades()) == 2
    (notice,) = env.texts(Severity.WARNING)
    assert notice.startswith("Açık işlem sınırına (2) ulaşıldı; sinyal atlandı (")
    assert "Sınırı dolduran işlemler: #1 BTC LONG (bekleyen emir · kripto), #2 EURUSD LONG (açık · fx)." in notice
    assert "risk.max_open_trades" in notice

    # the same list when the limit stops an approved signal
    env.cfg.risk.max_open_trades = 10
    await env.say(fx_pair("GBPUSD", confidence=0.5), FX, "GBPUSD LONG 1R")    # unsure: asked
    (conf,) = env.notifier.confirmations
    env.cfg.risk.max_open_trades = 2
    answer = await env.engine.handle_confirmation(conf.id, True)
    assert answer.startswith("⛔ Açık işlem sınırına (2) ulaşıldı; işlem açılmadı.")
    assert "#1 BTC LONG (bekleyen emir · kripto), #2 EURUSD LONG (açık · fx)" in answer


# =========================================================================== #
# C6 - virtual (reference-touch) entries
# =========================================================================== #
class Clock:
    """The engine's (and the database's) clock: 12 November 2026, moved by the test."""

    def __init__(self) -> None:
        self.t = datetime(2026, 11, 12, 9, 0, 40, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.t

    def at(self, hour: int, minute: int, second: int = 0) -> None:
        self.t = self.t.replace(hour=hour, minute=minute, second=second)


def minute_ms(hour: int, minute: int) -> int:
    return int(datetime(2026, 11, 12, hour, minute, tzinfo=UTC).timestamp() * 1000)


@pytest.fixture
async def timed():
    """The production replica with a clock the test moves; the BTC limit signal below arrives at 09:00:40."""
    clock = Clock()
    database = Database(":memory:", clock=clock)
    await database.open()
    e = await prod_env(database, clock=clock)
    yield e
    await e.engine.stop(grace_sec=2.0)
    await database.close()


async def waiting_entry(e: Env) -> TradeRecord:
    """BTC LONG limit 73000 / SL 72300 / TP 75000 while Binance trades at 74000: Binance and breakout get limit
    orders, tickmill a virtual entry (no order) that waits for Binance to touch 73000."""
    e.ref.rows["BTC"] = []
    await e.say(btc(EntryType.LIMIT), KRIPTO, "btc 1R long limit emir")
    (trade,) = await e.trades()
    pos = await e.position(trade.id, "tickmill")
    assert is_virtual_pending(pos) and pos.data["trigger_price"] == 73000.0 and e.tickmill.opened == []
    assert await e.brokers_of(trade.id) == {"binance": "pending", "breakout": "pending", "tickmill": "virtual"}
    e.notifier.notifications.clear()
    return trade


def at_price(e: Env, price: float | None) -> None:
    """Binance trades at ``price`` now (None: Binance cannot be reached); the tickmill CFD 50 USD below it."""
    e.ref.prices["BTC"] = price
    if price is not None:
        e.tickmill.quotes["BTCUSD"] = q(price - 51.0, price - 49.0)


async def look(e: Env) -> None:
    """One fast monitor pass over the waiting virtual entries (every ~2 s in production)."""
    await e.engine.monitor.run_virtual_once()


async def test_c6a_touch_between_two_looks_is_read_from_the_candle_and_opens_while_the_price_is_near(
        timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)
    # The minute the signal arrived in also holds a low from BEFORE the signal: that one must never count.
    e.ref.rows["BTC"] = [(minute_ms(9, 0), 72900.0, 74100.0)]
    e.clock.at(9, 0, 42)
    at_price(e, 73900.0)
    await look(e)
    assert e.tickmill.opened == [] and e.texts() == []

    # 09:01: a wick to 72990 (the resting Binance limit fills there); at the next look the price is back at 73200.
    e.ref.rows["BTC"] += [(minute_ms(9, 1), 72990.0, 73950.0), (minute_ms(9, 2), 73150.0, 73250.0)]
    e.clock.at(9, 2, 10)
    at_price(e, 73200.0)                                 # 0.27% above the level: within the 0.5% market tolerance
    await look(e)

    (req,) = e.tickmill.opened
    assert req.entry_type is EntryType.MARKET and req.stop_loss == pytest.approx(72250.0)    # 72300 shifted by -50
    (told,) = e.texts()
    assert "sanal tetik tetiklendi" in told and "seviye 73000" in told
    assert "Binance fiyatı kısa süreliğine 72990 seviyesine geldi, şimdi 73200" in told
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.OPEN
    await look(e)
    assert len(e.tickmill.opened) == 1                   # never twice


async def test_c6a_touch_seen_only_on_the_candle_is_not_chased_once_the_price_has_left(timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)
    e.ref.rows["BTC"] = [(minute_ms(9, 1), 72990.0, 73950.0), (minute_ms(9, 2), 73400.0, 73700.0)]
    e.clock.at(9, 2, 10)
    at_price(e, 73600.0)                                 # 0.82% above the level: too far to enter at market
    await look(e)
    e.clock.at(9, 2, 12)
    await look(e)

    assert e.tickmill.opened == []
    (told,) = e.texts()                                  # the owner is told once
    assert told.startswith("🧭 #1 BTC LONG tickmill: tetik seviyesi görüldü ama fiyat uzaklaştı; "
                           "tickmill'da açılmadı.")
    assert "72990" in told and "73600" in told and e.notifier.notifications[0][1] is Severity.WARNING
    pos = await e.position(trade.id, "tickmill")
    assert is_virtual_pending(pos) and pos.data["virtual_missed"]["extreme"] == 72990.0

    # It keeps waiting for a return to the level ...
    e.ref.rows["BTC"].append((minute_ms(9, 30), 72995.0, 73050.0))
    e.clock.at(9, 30, 5)
    at_price(e, 72995.0)
    await look(e)
    assert len(e.tickmill.opened) == 1


async def test_c6a_after_a_missed_touch_the_gap_rules_still_cancel_the_entry(timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)
    e.ref.rows["BTC"] = [(minute_ms(9, 1), 72990.0, 73950.0), (minute_ms(9, 2), 73400.0, 73700.0)]
    e.clock.at(9, 2, 10)
    at_price(e, 73600.0)
    await look(e)                                        # missed, not chased
    e.ref.rows["BTC"].append((minute_ms(9, 50), 74800.0, 75010.0))       # ... then the first target is reached
    e.clock.at(9, 50, 30)
    at_price(e, 74900.0)
    await look(e)
    assert e.tickmill.opened == []
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.CANCELLED
    assert any("ilk hedefi" in t and "75010" in t for t in e.texts(Severity.WARNING))


async def test_c6b_failure_before_any_order_is_retried_on_the_next_looks(timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)
    e.tickmill.fail["get_equity"] = BrokerError("MetaApi hesap bilgisi: 120 sn içinde yanıt gelmedi.")
    e.clock.at(9, 5, 0)
    at_price(e, 72995.0)                                 # touched
    for _ in range(3):
        await look(e)

    pos = await e.position(trade.id, "tickmill")
    assert is_virtual_pending(pos) and pos.data["virtual_state"] == "waiting" and pos.data["virtual_retries"] == 3
    assert e.tickmill.opened == [] and e.texts(Severity.ERROR) == []
    (told,) = e.texts()                                  # told once, not at every retry
    assert "tetiklendi" in told and "emir gönderilmedi" in told and "120 sn içinde yanıt gelmedi" in told
    assert "en çok 5 kez daha denenecek" in told

    e.tickmill.fail.clear()                              # the account answers again
    await look(e)
    assert len(e.tickmill.opened) == 1
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.OPEN


async def test_c6b_gives_up_with_the_usual_messages_after_five_retries(timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)
    e.tickmill.fail["get_equity"] = BrokerError("MetaApi hesap bilgisi: 120 sn içinde yanıt gelmedi.")
    e.clock.at(9, 5, 0)
    at_price(e, 72995.0)
    for failures in range(1, 6):                         # the first attempt and four retries
        await look(e)
        pos = await e.position(trade.id, "tickmill")
        assert is_virtual_pending(pos) and pos.data["virtual_retries"] == failures
    assert e.texts(Severity.ERROR) == []

    await look(e)                                        # the fifth retry fails too
    pos = await e.position(trade.id, "tickmill")
    assert pos.status is PositionStatus.FAILED and e.tickmill.opened == []
    assert e.texts(Severity.ERROR) == ["tickmill: <code>MetaApi hesap bilgisi: 120 sn içinde yanıt gelmedi.</code>"]
    assert "tetiklendi (Binance 72995) ama işlem açılmadı, sanal giriş iptal edildi" in e.texts(Severity.WARNING)[-1]
    count = len(e.texts())
    await look(e)
    assert len(e.texts()) == count


async def test_c6b_failure_after_the_order_went_out_is_never_retried(timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)
    e.tickmill.fail["open_position"] = BrokerError("emir reddedildi")
    e.clock.at(9, 5, 0)
    at_price(e, 72995.0)
    await look(e)
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.FAILED
    assert len(e.tickmill.opened) == 1                   # one order request went out ...
    await look(e)
    await look(e)
    assert len(e.tickmill.opened) == 1                   # ... and never a second one
    assert e.texts(Severity.ERROR) == ["tickmill: <code>emir reddedildi</code>"]


async def test_c6c_entry_is_cancelled_once_the_trade_is_over_on_the_other_accounts(timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)

    def played_out(pos: BrokerPosition) -> BrokerPosition:
        return pos.model_copy(update={"status": PositionStatus.CLOSED, "filled_quantity": pos.quantity,
                                      "remaining_quantity": 0.0, "avg_entry_price": 73000.0, "realized_pnl": 20.0})

    e.binance.sync_fn = e.breakout.sync_fn = played_out  # a wick filled both limit orders; they then hit their TP
    e.clock.at(9, 40, 0)
    at_price(e, 74900.0)
    await e.engine.monitor.run_once()
    await look(e)

    pos = await e.position(trade.id, "tickmill")
    assert pos.status is PositionStatus.CANCELLED and pos.data["virtual_end"] == "diğer hesaplarda işlem kapandı"
    (told,) = [t for t in e.texts() if "sanal giriş" in t]
    assert "işlemin diğer hesaplardaki pozisyonları kapandı; bekleyen sanal giriş iptal edildi" in told
    assert (await e.trade(trade.id)).status is TradeStatus.CLOSED
    e.clock.at(10, 0, 0)
    at_price(e, 72995.0)                                 # back at the entry later: nothing opens
    await look(e)
    assert e.tickmill.opened == []


async def test_c6c_entry_keeps_waiting_while_no_other_account_ever_filled(timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)
    for name in ("binance", "breakout"):                 # their limit orders were only cancelled (never filled)
        pos = await e.position(trade.id, name)
        await e.db.save_broker_position(trade.id, pos.model_copy(update={"status": PositionStatus.CANCELLED}))
    e.clock.at(9, 40, 0)
    at_price(e, 73900.0)
    await look(e)
    assert is_virtual_pending(await e.position(trade.id, "tickmill")) and e.texts() == []


async def test_c6c_stop_to_entry_cancels_the_waiting_entry(timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)

    def filled(pos: BrokerPosition) -> BrokerPosition:
        return pos.model_copy(update={"status": PositionStatus.OPEN, "filled_quantity": pos.quantity,
                                      "remaining_quantity": pos.quantity, "avg_entry_price": 73000.0})

    e.binance.sync_fn = e.breakout.sync_fn = filled      # filled on a wick the virtual entry did not act on
    await e.engine.monitor.run_once()
    e.binance.sync_fn = e.breakout.sync_fn = None
    e.notifier.notifications.clear()

    await e.say(manage(ActionType.MOVE_SL, base_asset="BTC", move_sl_to_entry=True, summary_tr="entry stop attık"),
                KRIPTO, "1R kâr var entry stop attık")

    by = {p.broker: p for p in await e.db.broker_positions(trade.id)}
    assert by["binance"].stop_loss == 73000.0 and by["breakout"].stop_loss == 73000.0
    assert by["tickmill"].status is PositionStatus.CANCELLED
    assert by["tickmill"].data["virtual_end"] == "stop girişe çekildi"
    assert e.texts() == ["🧭 #1 BTC LONG: tickmill: stop girişe çekildiği için bekleyen sanal giriş iptal edildi "
                         "(bu hesapta işlem açılmayacak)"]
    e.clock.at(9, 30, 0)
    at_price(e, 72995.0)                                 # back at the entry: the trader is out flat; nothing opens
    await look(e)
    assert e.tickmill.opened == []


async def test_c6c_closing_the_trade_cancels_the_waiting_entry_and_the_owner_is_told(timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)
    await e.say(closure("BTCUSDT.P", "BTC", "USDT"), KRIPTO, "btc kapattık")
    assert await e.brokers_of(trade.id) == {"binance": "cancelled", "breakout": "cancelled", "tickmill": "cancelled"}
    assert e.texts() == ["🧭 #1 BTC LONG: tickmill: sanal tetik iptal edildi"]
    at_price(e, 72995.0)
    await look(e)
    assert e.tickmill.opened == []


async def test_c6d_touch_while_paused_cancels_the_entry_instead_of_opening_it(timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)
    await e.engine.set_paused(True)                      # /durdur
    e.clock.at(9, 3, 0)
    at_price(e, 73900.0)
    await look(e)
    assert is_virtual_pending(await e.position(trade.id, "tickmill")) and e.texts() == []   # not touched: left alone

    e.clock.at(9, 5, 0)
    at_price(e, 72995.0)
    await look(e)
    assert e.tickmill.opened == []
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.CANCELLED
    (told,) = e.texts()
    assert "yeni işlem açma duraklatılmış (/durdur); sanal giriş iptal edildi, işlem AÇILMADI" in told
    assert await e.brokers_of(trade.id) == {"binance": "pending", "breakout": "pending", "tickmill": "cancelled"}


async def test_c6e_stop_crossed_while_the_reference_was_unreachable_is_read_from_the_candles(timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)
    e.clock.at(9, 1, 30)
    at_price(e, 73800.0)
    await look(e)
    at_price(e, None)                                    # Binance cannot be reached from 09:02 to 09:06
    for minute in (2, 3, 4, 5):
        e.clock.at(9, minute, 30)
        await look(e)
    assert len([t for t in e.texts(Severity.WARNING) if "referans fiyat" in t]) == 1
    assert is_virtual_pending(await e.position(trade.id, "tickmill"))

    # Meanwhile the price fell through the entry AND the stop (72300) and came back above the entry.
    e.ref.rows["BTC"] = [(minute_ms(9, 1), 73700.0, 73900.0), (minute_ms(9, 3), 72200.0, 73750.0),
                         (minute_ms(9, 6), 73050.0, 73150.0)]
    e.clock.at(9, 6, 30)
    at_price(e, 73100.0)
    await look(e)

    assert e.tickmill.opened == []
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.CANCELLED
    assert any("stop seviyesini" in t and "72200" in t and "AÇILMADI" in t for t in e.texts(Severity.WARNING))
    e.clock.at(9, 7, 0)
    at_price(e, 72990.0)                                 # a touch afterwards must not open the dead trade
    await look(e)
    assert e.tickmill.opened == []


async def test_c6e_levels_are_watched_while_the_account_is_not_connected(timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)
    e.engine.brokers = [b for b in e.engine.brokers if b.name != "tickmill"]     # the optional account is down
    e.clock.at(9, 5, 0)
    at_price(e, 72250.0)                                 # through the entry and the stop
    await look(e)
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.CANCELLED
    assert any("stop seviyesini" in t for t in e.texts(Severity.WARNING))


async def test_c6e_touch_while_the_account_was_not_connected_follows_the_no_chase_rule(timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)
    connected = e.engine.brokers
    e.engine.brokers = [b for b in connected if b.name != "tickmill"]
    e.ref.rows["BTC"] = [(minute_ms(9, 5), 72980.0, 73300.0)]
    e.clock.at(9, 5, 10)
    at_price(e, 72990.0)                                 # touched, but nothing can be opened there now
    await look(e)
    assert e.tickmill.opened == [] and is_virtual_pending(await e.position(trade.id, "tickmill"))

    e.engine.brokers = connected                         # back four minutes later
    e.ref.rows["BTC"].append((minute_ms(9, 9), 73150.0, 73260.0))
    e.clock.at(9, 9, 20)
    at_price(e, 73250.0)                                 # 0.34% above the level: still near enough
    await look(e)
    assert len(e.tickmill.opened) == 1


async def test_c6_the_last_look_is_kept_in_the_stored_position_once_a_minute(timed: Env) -> None:
    e = timed
    trade = await waiting_entry(e)
    e.clock.at(9, 0, 50)
    at_price(e, 73900.0)
    await look(e)
    assert "virtual_seen_ms" not in (await e.position(trade.id, "tickmill")).data      # no write every 2 seconds
    e.clock.at(9, 1, 45)
    await look(e)
    assert (await e.position(trade.id, "tickmill")).data["virtual_seen_ms"] == minute_ms(9, 1) + 45_000
    # After a restart (nothing in memory) the candles are read from the stored look on: the downtime is covered.
    e.engine._virtual_seen.clear()
    e.ref.candle_calls.clear()
    e.clock.at(9, 20, 0)
    await look(e)
    assert e.ref.candle_calls == [("BTC", minute_ms(9, 1), minute_ms(9, 20))]


async def test_c6_reference_candles_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    class Klines:
        def __init__(self) -> None:
            self.rows: dict[str, list[list[float]]] = {}
            self.calls: list[tuple[str, str, int, int]] = []

        async def fetch_ohlcv(self, symbol: str, timeframe: str = "1m", since: int | None = None,
                              limit: int | None = None) -> list[list[float]]:
            self.calls.append((symbol, timeframe, int(since or 0), int(limit or 0)))
            if symbol not in self.rows:
                raise ValueError(f"does not have market symbol {symbol}")
            return [r for r in self.rows[symbol] if r[0] >= (since or 0)][:limit]

    m0 = minute_ms(9, 0)
    ex = Klines()
    ex.rows["BTC/USDT:USDT"] = [[m0 - MINUTE, 1, 110.0, 90.0, 1, 1], [m0, 1, 105.0, 95.0, 1, 1],
                                [m0 + MINUTE, 1, 104.0, 96.0, 1, 1]]
    ex.rows["1000PEPE/USDT:USDT"] = [[m0, 1, 0.012, 0.010, 1, 1]]
    ref = ReferencePriceOracle(exchanges={"binanceusdm": ex})

    rows = await ref.candles("binanceusdm", "btc", m0, m0 + 90_000)
    assert rows == [(m0, 95.0, 105.0), (m0 + MINUTE, 96.0, 104.0)]       # (open ms, low, high) from ``since`` on
    assert ex.calls[-1] == ("BTC/USDT:USDT", "1m", m0, 3)                # no more candles than the window needs

    rows = await ref.candles("binanceusdm", "PEPE", m0, m0)               # trades as 1000PEPE: signal price scale
    assert rows == [(m0, pytest.approx(0.00001), pytest.approx(0.000012))]
    seen = len(ex.calls)
    await ref.candles("binanceusdm", "PEPE", m0, m0)
    assert [c[0] for c in ex.calls[seen:]] == ["1000PEPE/USDT:USDT"]      # the working symbol is remembered

    assert await ref.candles("binanceusdm", "NOPE", m0) is None           # cannot be read: None, never an exception
    assert await ref.candles("nosuchexchange", "BTC", m0) is None
    assert await ref.candles("binanceusdm", "BTC", m0 + 10 * MINUTE, m0 + 10 * MINUTE) == []   # no candle yet

    monkeypatch.setattr(prices_mod, "CANDLE_PAGE", 3, raising=False)      # a long window is read in pages
    ex.rows["ETH/USDT:USDT"] = [[m0 + i * MINUTE, 1, 10.0 + i, 5.0 + i, 1, 1] for i in range(7)]
    rows = await ref.candles("binanceusdm", "ETH", m0, m0 + 7 * MINUTE)
    assert [r[0] for r in rows] == [m0 + i * MINUTE for i in range(7)]


def test_c6_near_entry_is_the_validators_rule_for_market_entries() -> None:
    reference = getattr(validator, "_near_entry", None)
    crypto, fx = RiskConfig(), RiskConfig(market_entry_tolerance_pct=0.03, market_entry_tolerance_r=0.25)
    cases = [(73200.0, 73000.0, 72300.0), (73365.0, 73000.0, 72300.0), (73366.0, 73000.0, 72300.0),
             (72700.0, 73000.0, 72300.0), (1.16250, 1.16200, 1.16000), (1.16251, 1.16200, 1.16000),
             (1.16230, 1.16200, None), (1.16240, 1.16200, None)]
    for risk in (crypto, fx):
        for price, entry, stop in cases:
            got = orch.near_entry(price, entry, stop, risk)
            if reference is not None:
                assert got == reference(price, entry, stop, risk), (price, entry, stop)
    assert orch.near_entry(73365.0, 73000.0, 72300.0, crypto) is True       # exactly 0.5%
    assert orch.near_entry(73366.0, 73000.0, 72300.0, crypto) is False
    assert orch.near_entry(1.16250, 1.16200, 1.16000, fx) is True           # a quarter of the 20 pip stop
    assert orch.near_entry(1.16251, 1.16200, 1.16000, fx) is False
