"""Pre-flight fixes, batch 3, engine (orchestrator / config / prices), on a replica of the owner's production
setup (routes "kripto" / "fx"; brokers binance / breakout / fx / tickmill) with fake venues:

E1.  pullback limit entries (the first target lies between the entry and today's price): the approval re-check
     and the virtual (reference-touch) entry no longer refuse / cancel them;
E2.  a management reply to a message the bot never stored is asked, never applied to "the only trade of that asset";
E3.  a required broker that cannot connect at start-up is retried in the background; every signal names it;
E4.  a relative data_dir / Telegram session path is resolved against the folder of the config file;
E5.  real replies, and any text while the topic's route has an active trade, bypass the prefilter;
E6.  a Claude outage is retried for about six and a half minutes (10, 30, 60, 120, 180 s);
E7.  topic titles typed without Turkish letters match the routes;
E8.  the public price lookup knows the 10000 / 100000 / 1000000 / 1M contract prefixes;
E9.  stop-distance bounds in RiskConfig (read by the validator);
E10. throttled owner notices where a signal used to disappear with a log line only.

The fakes prove the bot's own logic; no real broker, exchange or Telegram is involved. Everything the tests need
is defined in this file (the production replica is embedded).
"""

from __future__ import annotations

import asyncio
import inspect
import sqlite3
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import ccxt
import pytest
import yaml

from copytrader.brokers.base import BrokerError
from copytrader.app import build_app
from copytrader.config import AppConfig, RiskConfig, TopicRoute, fold_tr, load_config, match_topic
from copytrader.brokers import ccxt_broker
from copytrader.engine import orchestrator as orch
from copytrader.engine import prices as prices_mod
from copytrader.errors import ParserError
from copytrader.engine.monitor import is_virtual_pending
from copytrader.engine.orchestrator import Engine
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
    StoredMessage,
    SymbolInfo,
    TakeProfit,
    TradePlan,
    TradeRecord,
    TradeStatus,
)
from copytrader.parsing import prefilter, validator
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


# --------------------------------------------------------------------------- #
# Doubles
# --------------------------------------------------------------------------- #
class Reference:
    """reference_prices double: ``mid`` = scripted last price (None: unavailable); ``candles`` = scripted
    1-minute candles (open ms, low, high) per base (no entry: candles cannot be read)."""

    def __init__(self, prices: dict[str, float | None] | None = None) -> None:
        self.prices: dict[str, float | None] = dict(prices or {})
        self.rows: dict[str, list[tuple[int, float, float]]] = {}

    async def mid(self, exchange_id: str, base_asset: str, *, fresh: bool = False) -> float | None:
        return self.prices.get(base_asset.upper())

    async def candles(self, exchange_id: str, base_asset: str, since_ms: int,
                      until_ms: int | None = None) -> list[tuple[int, float, float]] | None:
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
    "EURUSD" / "BTCUSD" with symbol_map first; everything else "BTC/USDT:USDT"). Quotes by broker symbol."""

    def __init__(self, config: Any, **kw: Any) -> None:
        super().__init__(config.name, **kw)
        self.config = config

    async def resolve_symbol(self, base_asset: str, quote_asset: str | None = None,
                             market_type: MarketType | None = None, raw_symbol: str | None = None) -> SymbolInfo | None:
        self.calls.append(("resolve_symbol", (base_asset, quote_asset, market_type, raw_symbol)))
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

    async def get_quote(self, symbol: SymbolInfo) -> Quote:
        self._maybe_fail("get_quote")
        return self._quote(symbol)

    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        pos = await super().open_position(req)
        if pos.avg_entry_price is not None:
            pos.avg_entry_price = self._quote(req.symbol).entry_price_for(req.plan.side)
        return pos


FX_QUOTES = {"EURUSD": q(1.16195, 1.16205), "GBPUSD": q(1.31495, 1.31505), "XAUUSD": q(4099.90, 4100.10)}
PRICES = {"EURUSD": 1.1620, "GBPUSD": 1.3150, "XAUUSD": 4100.0, "EUR": 1.1620, "BTC": 74000.0, "ETH": 2232.0}


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

    async def post(self, action: ParsedAction | None, topic: int, text: str = "LONG 1R", *,
                   reply_to: int | None = None) -> int:
        """The trader posts a message in ``topic`` (the fake parser will read it as ``action``; None: nothing
        is scripted for it); not processed yet. ``reply_to``: a real Telegram reply to that message id."""
        self.mid += 1
        mid = self.mid
        if action is not None:
            self.parser.push(ParseResult(actions=[action.model_copy(update={"used_message_ids": [mid]})],
                                         model="fake"))
        date = self.clock() if self.clock is not None else datetime.now(UTC)
        await self.db.save_message(StoredMessage(
            chat_id=CHAT, message_id=mid, sender_id=SENDER, sender_name="Hoca", date=date, text=text,
            reply_to_message_id=reply_to if reply_to is not None else topic, topic_id=topic,
            topic_title=TITLES[topic]))
        return mid

    async def say(self, action: ParsedAction | None, topic: int, text: str = "LONG 1R", *,
                  reply_to: int | None = None, timeout: float = 3.0) -> int:
        """Post a message and let the engine process it (returns when the engine is done with the batch)."""
        mid = await self.post(action, topic, text, reply_to=reply_to)
        await asyncio.wait_for(self.engine.on_messages_ready(CHAT, SENDER, topic), timeout)
        return mid

    async def approve_if_asked(self) -> str | None:
        """Tap "Onayla" on the newest question that is still open (None: nothing was asked)."""
        pending = await self.db.pending_confirmations()
        if not pending:
            return None
        return await self.engine.handle_confirmation(pending[-1].id, True)

    async def trades(self) -> list[TradeRecord]:
        return sorted(await self.db.recent_trades(datetime(2000, 1, 1, tzinfo=UTC)), key=lambda t: t.id)

    async def position(self, trade_id: int, broker: str) -> BrokerPosition:
        return next(p for p in await self.db.broker_positions(trade_id) if p.broker == broker)

    async def brokers_of(self, trade_id: int) -> dict[str, str]:
        return {p.broker: ("virtual" if is_virtual_pending(p) else p.status.value)
                for p in await self.db.broker_positions(trade_id)}

    def texts(self, severity: Severity | None = None) -> list[str]:
        return self.notifier.texts(severity)

    def questions(self) -> list[list[str]]:
        """Issue codes of every question the owner was asked, oldest first."""
        return [[i.code for i in c.issues] for c in self.notifier.confirmations]

    def closes(self) -> dict[str, int]:
        return {name: len(b.calls_of("close_position")) for name, b in self.brokers.items()}

    async def until(self, predicate: Any, what: str = "condition", timeout: float = 3.0) -> None:
        end = asyncio.get_running_loop().time() + timeout
        while not predicate():
            assert asyncio.get_running_loop().time() < end, f"not in time: {what}"
            await asyncio.sleep(0.005)


def prod_brokers(cfg: AppConfig) -> dict[str, Venue]:
    by = {b.name: b for b in cfg.brokers}
    brokers = {
        "binance": Venue(by["binance"], equity=46.9, currency="USDT"),
        "breakout": Venue(by["breakout"], equity=1_000_000.0, currency="USDT"),
        "fx": Venue(by["fx"], equity=10_000.0, currency="USD", qty_step=0.01),
        "tickmill": Venue(by["tickmill"], equity=108.0, currency="USD", qty_step=0.01),
    }
    for name in ("binance", "breakout"):
        brokers[name].quotes.update({"BTC/USDT:USDT": q(73999.0, 74001.0), "ETH/USDT:USDT": q(2231.9, 2232.1)})
    brokers["fx"].quotes.update(FX_QUOTES)
    brokers["tickmill"].quotes.update(FX_QUOTES)
    brokers["tickmill"].quotes["BTCUSD"] = q(73949.0, 73951.0)       # CFD price: 50 USD below Binance
    brokers["tickmill"].quotes["ETHUSD"] = q(2229.9, 2230.1)
    return brokers


async def prod_env(db: Database, *, clock: Any = None, start: bool = True,
                   brokers: dict[str, Venue] | None = None) -> Env:
    """The production replica: real config objects on fake venues (fx: 10,000 USD prop account)."""
    cfg = prod_config()
    brokers = brokers if brokers is not None else prod_brokers(cfg)
    parser, notifier = FakeParser(), FakeNotifier()
    ref, oracle = Reference({"BTC": 74000.0, "ETH": 2232.0}), PairOracle(PRICES)
    engine = Engine(cfg, db, parser, list(brokers.values()), notifier, price_oracle=oracle, reference_prices=ref,
                    clock=clock)
    env = Env(cfg, db, brokers, parser, notifier, ref, oracle, engine)
    env.clock = clock
    if start:
        await engine.start(monitor=False)
        assert [b.name for b in engine.brokers] == ["binance", "breakout", "fx", "tickmill"]
        notifier.notifications.clear()
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
    await e.engine.stop(grace_sec=2.0)


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
    """The production replica with a clock the test moves; signals arrive at 09:00:40."""
    clock = Clock()
    database = Database(":memory:", clock=clock)
    await database.open()
    e = await prod_env(database, clock=clock)
    yield e
    await e.engine.stop(grace_sec=2.0)
    await database.close()


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


def btc_market(**kw: Any) -> ParsedAction:
    """BTCUSDT.P at 74000: market long, SL 73300, TP 75500."""
    return signal("BTC", "USDT", Side.LONG, 74000.0, 73300.0, 75500.0, symbol="BTCUSDT.P",
                  market_type=MarketType.PERP, **kw)


def btc_pullback(**kw: Any) -> ParsedAction:
    """The auditor's case. BTC trades at 74000: "btc 1R long limit emir", entry 72500, SL 72100, TP 73300: the
    first target lies between the entry and today's price."""
    return signal("BTC", "USDT", Side.LONG, 72500.0, 72100.0, 73300.0, symbol="BTCUSDT.P",
                  market_type=MarketType.PERP, entry_type=EntryType.LIMIT, **kw)


def btc_limit(**kw: Any) -> ParsedAction:
    """An ordinary limit: BTC at 74000, limit long 73000, SL 72300, TP 75000 (the price is between entry and target)."""
    return signal("BTC", "USDT", Side.LONG, 73000.0, 72300.0, 75000.0, symbol="BTCUSDT.P",
                  market_type=MarketType.PERP, entry_type=EntryType.LIMIT, **kw)


def eurusd(**kw: Any) -> ParsedAction:
    """EURUSD at 1.1620: market long, 20 pips of stop."""
    return signal("EUR", "USD", Side.LONG, 1.1620, 1.1600, 1.1660, symbol="EURUSD", market_type=MarketType.FOREX, **kw)


def manage(kind: ActionType, **kw: Any) -> ParsedAction:
    data: dict[str, Any] = dict(action=kind, confidence=0.95, summary_tr=kind.value)
    data.update(kw)
    return ParsedAction(**data)


def at_price(e: Env, price: float | None) -> None:
    """Binance trades at ``price`` now (None: Binance cannot be reached); the tickmill CFD 50 USD below it."""
    e.ref.prices["BTC"] = price
    if price is not None:
        e.oracle.prices["BTC"] = price
        for name in ("binance", "breakout"):
            e.brokers[name].quotes["BTC/USDT:USDT"] = q(price - 1.0, price + 1.0)
        e.tickmill.quotes["BTCUSD"] = q(price - 51.0, price - 49.0)


async def look(e: Env) -> None:
    """One fast monitor pass over the waiting virtual entries (every ~2 s in production)."""
    await e.engine.monitor.run_virtual_once()


# =========================================================================== #
# E1 - pullback limit entries
# =========================================================================== #
async def pullback_entry(e: Env) -> TradeRecord:
    """The pullback limit is posted while Binance trades at 74000 and is opened (approved first when this
    version of the validator still asks about it): Binance and breakout hold limit orders at 72500, tickmill a
    virtual entry that waits for Binance to touch 72500."""
    e.ref.rows["BTC"] = []
    await e.say(btc_pullback(), KRIPTO, "btc 1R long limit emir")
    answer = await e.approve_if_asked()
    assert answer is None or not answer.startswith("⛔"), answer
    (trade,) = await e.trades()
    assert await e.brokers_of(trade.id) == {"binance": "pending", "breakout": "pending", "tickmill": "virtual"}
    e.notifier.notifications.clear()
    return trade


async def test_e1_pullback_limit_is_opened_on_every_broker_of_its_route(timed: Env) -> None:
    e = timed
    trade = await pullback_entry(e)
    assert trade.status is TradeStatus.PENDING
    for name in ("binance", "breakout"):
        (req,) = e.brokers[name].opened
        assert req.entry_type is EntryType.LIMIT and req.entry_price == 72500.0 and req.stop_loss == 72100.0
    assert e.tickmill.opened == []                               # no order yet: the entry waits for the touch
    pos = await e.position(trade.id, "tickmill")
    assert pos.data["trigger_price"] == 72500.0 and pos.data["virtual_tp_armed"] is False

    # The monitor looks at the waiting entry every couple of seconds: the price is still above the first target.
    for second in (42, 44, 46):
        e.clock.at(9, 0, second)
        await look(e)
    assert is_virtual_pending(await e.position(trade.id, "tickmill"))
    assert e.texts() == []                                       # nothing was cancelled, nothing to tell


async def test_e1_approval_of_a_pullback_limit_is_not_refused_for_its_first_target(timed: Env) -> None:
    e = timed
    # A doubtful reading (low confidence) is asked about on every version of the validator.
    await e.say(btc_pullback(confidence=0.5), KRIPTO, "btc 1R long limit emir")
    assert len(e.notifier.confirmations) == 1 and await e.trades() == []
    answer = await e.approve_if_asked()
    assert answer is not None and "ilk hedefi" not in answer and not answer.startswith("⛔"), answer
    (trade,) = await e.trades()
    assert await e.brokers_of(trade.id) == {"binance": "pending", "breakout": "pending", "tickmill": "virtual"}


async def test_e1_approval_still_refuses_a_market_plan_whose_first_target_the_price_has_passed(timed: Env) -> None:
    e = timed
    await e.say(btc_market(confidence=0.5), KRIPTO, "btc 1R long")
    at_price(e, 75600.0)                                         # while the question waits the target (75500) is reached
    answer = await e.approve_if_asked()
    assert answer == "⛔ Anlık fiyat (75600) ilk hedefi (75500) geçmiş — işlem açılmadı."
    assert await e.trades() == [] and all(b.opened == [] for b in e.brokers.values())


async def test_e1_approval_still_refuses_a_limit_whose_stop_the_price_has_passed(timed: Env) -> None:
    e = timed
    await e.say(btc_pullback(confidence=0.5), KRIPTO, "btc 1R long limit emir")
    at_price(e, 72050.0)                                         # fell through the entry and the stop (72100)
    answer = await e.approve_if_asked()
    assert answer == "⛔ Anlık fiyat (72050) stop seviyesini (72100) geçmiş — işlem açılmadı."
    assert await e.trades() == []


def _plan(entry_type: EntryType, entry: float, sl: float, tp: float, side: Side = Side.LONG) -> TradePlan:
    return TradePlan(plan_id="e1", base_asset="BTC", quote_asset="USDT", market_type=MarketType.PERP, side=side,
                     entry_type=entry_type, entry_price=entry, stop_loss=sl, take_profits=[TakeProfit(price=tp)],
                     risk_r=1.0, risk_pct=2.0, source_chat_id=CHAT, source_message_ids=[1],
                     created_at=datetime.now(UTC), route="kripto", broker_names=["binance", "breakout", "tickmill"])


@pytest.mark.parametrize(("plan", "price", "problem"), [
    # resting limits (the price has not reached the entry): only the stop counts
    (_plan(EntryType.LIMIT, 72500.0, 72100.0, 73300.0), 74000.0, None),
    (_plan(EntryType.LIMIT, 72500.0, 72100.0, 73300.0), 73300.0, None),
    (_plan(EntryType.LIMIT, 75500.0, 75900.0, 74700.0, Side.SHORT), 74000.0, None),
    (_plan(EntryType.LIMIT, 72500.0, 72100.0, 73300.0), 72100.0, "stop seviyesini"),
    (_plan(EntryType.LIMIT, 75500.0, 75900.0, 74700.0, Side.SHORT), 75900.0, "stop seviyesini"),
    # market plans: stop and first target, as before
    (_plan(EntryType.MARKET, 74000.0, 73300.0, 75500.0), 74000.0, None),
    (_plan(EntryType.MARKET, 74000.0, 73300.0, 75500.0), 75500.0, "ilk hedefi"),
    (_plan(EntryType.MARKET, 74000.0, 74700.0, 72500.0, Side.SHORT), 72500.0, "ilk hedefi"),
    # a limit that is already marketable is filled at once like a market order: the first target still counts
    (_plan(EntryType.LIMIT, 74500.0, 73300.0, 74200.0), 74300.0, "ilk hedefi"),
    (_plan(EntryType.LIMIT, 73500.0, 74700.0, 73800.0, Side.SHORT), 73700.0, "ilk hedefi"),
])
async def test_e1_approval_price_check_per_entry_type(env: Env, plan: TradePlan, price: float,
                                                      problem: str | None) -> None:
    env.oracle.prices["BTC"] = price
    found = await env.engine._approval_price_problem(plan, "BTCUSDT.P")
    assert (found is None) if problem is None else (found is not None and problem in found), found


async def test_e1_virtual_entry_of_a_pullback_limit_opens_when_the_price_comes_back_to_its_entry(timed: Env) -> None:
    e = timed
    trade = await pullback_entry(e)
    for second, price in ((42, 73900.0), (44, 73350.0)):          # still above the first target (73300)
        e.clock.at(9, 0, second)
        at_price(e, price)
        await look(e)
    assert (await e.position(trade.id, "tickmill")).data["virtual_tp_armed"] is False
    e.clock.at(9, 20, 0)
    at_price(e, 73100.0)                                         # back through the target, towards the entry
    await look(e)
    pos = await e.position(trade.id, "tickmill")
    assert is_virtual_pending(pos) and pos.data["virtual_tp_armed"] is True and e.texts() == []

    e.clock.at(9, 40, 0)
    at_price(e, 72500.0)                                         # the entry is touched
    await look(e)
    (req,) = e.tickmill.opened
    assert req.entry_type is EntryType.MARKET
    assert req.stop_loss == pytest.approx(72050.0) and req.take_profits[0].price == pytest.approx(73250.0)  # -50
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.OPEN
    (told,) = e.texts()
    assert "sanal tetik tetiklendi" in told and "seviye 72500" in told


async def test_e1_virtual_entry_is_cancelled_when_the_price_returns_to_the_target_after_coming_back(timed: Env) -> None:
    e = timed
    trade = await pullback_entry(e)
    e.clock.at(9, 5, 0)
    at_price(e, 73000.0)                                         # between the entry (72500) and the target (73300)
    await look(e)
    assert is_virtual_pending(await e.position(trade.id, "tickmill")) and e.texts() == []
    e.clock.at(9, 30, 0)
    at_price(e, 73320.0)                                         # ... and up to the target without touching the entry
    await look(e)
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.CANCELLED
    (told,) = e.texts(Severity.WARNING)
    assert "sanal tetik iptal edildi: Binance fiyatı (73320) ilk hedefi (73300) geçti" in told
    assert e.tickmill.opened == []


@pytest.mark.parametrize("ending", ["touches the entry", "falls back to the target"])
async def test_e1_short_pullback_entry_follows_the_same_rule_mirrored(timed: Env, ending: str) -> None:
    e = timed
    e.ref.rows["BTC"] = []
    # BTC at 74000: limit short 75500, SL 75900, TP 74700 (the target lies between today's price and the entry).
    short = signal("BTC", "USDT", Side.SHORT, 75500.0, 75900.0, 74700.0, symbol="BTCUSDT.P",
                   market_type=MarketType.PERP, entry_type=EntryType.LIMIT)
    await e.say(short, KRIPTO, "btc 1R short limit emir")
    await e.approve_if_asked()
    (trade,) = await e.trades()
    assert (await e.position(trade.id, "tickmill")).data["virtual_tp_armed"] is False
    e.notifier.notifications.clear()
    e.clock.at(9, 0, 42)
    await look(e)                                                # 74000: still below the target, nothing happens
    e.clock.at(9, 10, 0)
    at_price(e, 75000.0)                                         # up through the target, towards the entry
    await look(e)
    pos = await e.position(trade.id, "tickmill")
    assert is_virtual_pending(pos) and pos.data["virtual_tp_armed"] is True and e.texts() == []

    e.clock.at(9, 30, 0)
    if ending == "touches the entry":
        at_price(e, 75500.0)
        await look(e)
        (req,) = e.tickmill.opened
        assert req.entry_type is EntryType.MARKET and req.plan.side is Side.SHORT
        assert req.stop_loss == pytest.approx(75850.0)           # 75900 shifted by -50
    else:
        at_price(e, 74650.0)                                     # back down to the target without touching 75500
        await look(e)
        assert (await e.position(trade.id, "tickmill")).status is PositionStatus.CANCELLED
        assert any("ilk hedefi (74700) geçti" in t for t in e.texts(Severity.WARNING)) and e.tickmill.opened == []


async def test_e1_prices_of_the_minute_before_the_price_came_back_never_cancel_the_entry(timed: Env) -> None:
    e = timed
    trade = await pullback_entry(e)
    # 09:07 opens above the target (73450) and falls to 73250; the bot looks while the price is at 73260.
    e.ref.rows["BTC"] = [(minute_ms(9, 6), 73400.0, 73500.0), (minute_ms(9, 7), 73250.0, 73450.0)]
    e.clock.at(9, 7, 30)
    at_price(e, 73260.0)
    await look(e)
    e.clock.at(9, 7, 32)                                         # the high of that minute is from BEFORE it came back
    await look(e)
    e.ref.rows["BTC"].append((minute_ms(9, 8), 73240.0, 73290.0))
    e.clock.at(9, 8, 30)
    at_price(e, 73270.0)
    await look(e)
    assert is_virtual_pending(await e.position(trade.id, "tickmill")) and e.texts() == []

    e.ref.rows["BTC"].append((minute_ms(9, 9), 73270.0, 73310.0))    # a later minute does reach the target
    e.clock.at(9, 10, 5)
    at_price(e, 73280.0)
    await look(e)
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.CANCELLED
    assert any("ilk hedefi (73300) geçti" in t and "73310" in t for t in e.texts(Severity.WARNING))


async def test_e1_dip_below_the_target_seen_only_on_a_candle_counts_as_coming_back(timed: Env) -> None:
    e = timed
    trade = await pullback_entry(e)
    e.ref.rows["BTC"] = [(minute_ms(9, 1), 73250.0, 73900.0), (minute_ms(9, 2), 73350.0, 73420.0)]
    e.clock.at(9, 2, 30)
    at_price(e, 73400.0)                 # dipped to 73250 between two looks and is above the target again
    await look(e)
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.CANCELLED
    assert any("ilk hedefi (73300) geçti" in t for t in e.texts(Severity.WARNING))


async def test_e1_stop_rule_of_a_pullback_entry_is_unchanged(timed: Env) -> None:
    e = timed
    trade = await pullback_entry(e)
    e.ref.rows["BTC"] = [(minute_ms(9, 1), 72050.0, 73900.0)]    # straight through the entry and the stop
    e.clock.at(9, 2, 5)
    at_price(e, 72300.0)
    await look(e)
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.CANCELLED
    (told,) = e.texts(Severity.WARNING)
    assert "stop seviyesini (72100) geçti" in told and e.tickmill.opened == []


async def test_e1_ordinary_limit_keeps_the_first_target_rule(timed: Env) -> None:
    e = timed
    e.ref.rows["BTC"] = []
    await e.say(btc_limit(), KRIPTO, "btc 1R long limit emir")
    (trade,) = await e.trades()
    e.notifier.notifications.clear()
    e.clock.at(9, 0, 42)
    await look(e)                                                # 74000: between the entry (73000) and the target
    assert (await e.position(trade.id, "tickmill")).data["virtual_tp_armed"] is True
    e.ref.rows["BTC"] = [(minute_ms(9, 50), 74800.0, 75010.0)]   # the target (75000) is reached between two looks
    e.clock.at(9, 51, 30)
    at_price(e, 74900.0)
    await look(e)
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.CANCELLED
    assert any("ilk hedefi (75000) geçti" in t and "75010" in t for t in e.texts(Severity.WARNING))


async def test_e1_ordinary_limit_whose_first_look_comes_late_is_still_cancelled_at_its_target(timed: Env) -> None:
    e = timed
    await e.say(btc_limit(), KRIPTO, "btc 1R long limit emir")   # registered at 74000: between entry and target
    (trade,) = await e.trades()
    assert (await e.position(trade.id, "tickmill")).data["virtual_tp_armed"] is True
    e.notifier.notifications.clear()
    e.clock.at(9, 30, 0)
    at_price(e, 75050.0)                 # the very first look finds the price beyond the target (no candles here)
    await look(e)
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.CANCELLED


@pytest.mark.parametrize("pullback", [True, False])
async def test_e1_price_unknown_when_the_entry_is_registered_is_decided_at_the_first_reading(
        timed: Env, pullback: bool) -> None:
    e = timed
    del e.oracle.prices["BTC"]                                   # no market price at this moment
    e.ref.rows["BTC"] = []
    await e.say(btc_pullback() if pullback else btc_limit(), KRIPTO, "btc 1R long limit emir")
    await e.approve_if_asked()                                   # (asked: the price could not be checked)
    (trade,) = await e.trades()
    assert (await e.position(trade.id, "tickmill")).data["virtual_tp_armed"] is False
    e.notifier.notifications.clear()
    e.clock.at(9, 0, 50)
    at_price(e, 74000.0)
    await look(e)
    pos = await e.position(trade.id, "tickmill")
    # 74000 is beyond the pullback's target (73300): it keeps waiting. For the ordinary limit (target 75000)
    # it is between entry and target: the rule counts from here on.
    assert is_virtual_pending(pos) and pos.data["virtual_tp_armed"] is (not pullback) and e.texts() == []


async def test_e1_price_source_that_does_not_answer_never_holds_the_registration_up(
        timed: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    e = timed
    monkeypatch.setattr(orch, "VIRTUAL_REGISTER_PRICE_TIMEOUT_SEC", 0.05)
    never = asyncio.Event()
    hanging = {"now": False}                                     # the source answers until the entry is registered
    real_price, real_register = e.oracle.price, e.engine._register_virtual

    async def price(*args: Any, **kw: Any) -> float | None:
        if hanging["now"]:
            await never.wait()
        return await real_price(*args, **kw)

    async def register(*args: Any, **kw: Any) -> Any:
        hanging["now"] = True
        try:
            return await real_register(*args, **kw)
        finally:
            hanging["now"] = False

    e.oracle.price = price                                       # type: ignore[method-assign]
    e.engine._register_virtual = register                        # type: ignore[method-assign]
    await e.say(btc_pullback(), KRIPTO, "btc 1R long limit emir", timeout=2.0)
    await asyncio.wait_for(e.approve_if_asked(), 2.0)
    (trade,) = await e.trades()
    assert await e.brokers_of(trade.id) == {"binance": "pending", "breakout": "pending", "tickmill": "virtual"}
    assert (await e.position(trade.id, "tickmill")).data["virtual_tp_armed"] is False


async def test_e1_registering_a_virtual_entry_reads_no_reference_price(timed: Env) -> None:
    e = timed
    asked: list[str] = []
    real = e.ref.mid

    async def mid(exchange_id: str, base_asset: str, *, fresh: bool = False) -> float | None:
        asked.append(base_asset)
        return await real(exchange_id, base_asset, fresh=fresh)

    e.ref.mid = mid                                              # type: ignore[method-assign]
    await pullback_entry(e)
    assert asked == []                                           # the offset is measured at the touch, as before


async def test_e1_entry_stored_by_the_previous_version_keeps_the_rule_it_was_registered_with(timed: Env) -> None:
    e = timed
    e.ref.rows["BTC"] = []
    await e.say(btc_limit(), KRIPTO, "btc 1R long limit emir")
    (trade,) = await e.trades()
    old = await e.position(trade.id, "tickmill")
    old.data.pop("virtual_tp_armed")                             # what the version before this fix stored
    await e.db.save_broker_position(trade.id, old)
    e.notifier.notifications.clear()
    e.clock.at(9, 30, 0)
    at_price(e, 75050.0)
    await look(e)
    assert (await e.position(trade.id, "tickmill")).status is PositionStatus.CANCELLED


async def test_e1_moving_the_target_of_a_waiting_entry_below_the_price_does_not_cancel_it(timed: Env) -> None:
    e = timed
    e.ref.rows["BTC"] = []
    await e.say(btc_limit(), KRIPTO, "btc 1R long limit emir")
    (trade,) = await e.trades()
    e.clock.at(9, 0, 42)
    await look(e)                                                # armed: 74000 is between 73000 and 75000
    e.clock.at(9, 5, 0)
    # (Applied directly: today's validator refuses a new target on the far side of the market price.)
    await e.engine.execute_management(ManagementPlan(
        plan_id="tp1", action=ActionType.MOVE_TP, trade_id=trade.id, take_profits=[TakeProfit(price=73600.0)],
        source_chat_id=CHAT, summary_tr="btc tp 73600", created_at=e.clock()))
    pos = await e.position(trade.id, "tickmill")
    assert pos.take_profits[0].price == 73600.0 and pos.data["virtual_tp_armed"] is False
    e.notifier.notifications.clear()
    e.clock.at(9, 5, 2)
    await look(e)                                                # 74000 is above the new target: a pullback entry now
    assert is_virtual_pending(await e.position(trade.id, "tickmill")) and e.texts() == []
    e.clock.at(9, 30, 0)
    at_price(e, 73000.0)
    await look(e)
    assert len(e.tickmill.opened) == 1


# =========================================================================== #
# E3 - a required broker that cannot connect at start-up
# =========================================================================== #
MT5_DOWN = "MT5 terminaline bağlanılamadı: IPC timeout (kod -10005). Terminal kurulu mu, login/şifre/sunucu doğru mu?"


async def started_without(db: Database, *down: str, retry_sec: float = 3600.0, first_sec: float = 3600.0) -> Env:
    """The production replica started while the brokers ``down`` cannot connect."""
    e = await prod_env(db, start=False)
    for name in down:
        e.brokers[name].fail["connect"] = BrokerError(MT5_DOWN)
    e.engine.optional_retry_sec = retry_sec
    e.engine.required_retry_first_sec = first_sec
    await e.engine.start(monitor=False)
    return e


def connects(broker: Venue) -> int:
    return len(broker.calls_of("connect"))


async def test_e3_required_broker_that_cannot_connect_at_start_is_retried_and_announced_when_back(db: Database) -> None:
    e = await started_without(db, "fx", retry_sec=0.03, first_sec=0.03)
    try:
        assert [b.name for b in e.engine.brokers] == ["binance", "breakout", "tickmill"]
        assert "fx" in e.engine.failed_brokers and "fx" not in e.engine.optional_down
        (alarm,) = e.texts(Severity.ERROR)
        assert alarm.startswith(f"fx bağlanamadı: <code>{MT5_DOWN}</code>")
        assert "Arka planda yeniden denenecek" in alarm and "bağlanınca haber verilecek" in alarm
        assert any("❌ Bağlanamayan: fx (arka planda yeniden deneniyor)" in t for t in e.texts(Severity.INFO))
        assert "✅ tickmill, ❌ fx (yeniden deneniyor)" in await e.engine.status_text()          # /durum

        await e.until(lambda: connects(e.fx) >= 4, "more connect attempts")
        assert len(e.texts(Severity.ERROR)) == 1                 # silent while it keeps failing
        assert [b.name for b in e.engine.brokers] == ["binance", "breakout", "tickmill"]

        del e.fx.fail["connect"]                                 # the terminal is ready now
        await e.until(lambda: any("bağlandı" in t for t in e.texts(Severity.INFO)), "the reconnect notice")
        assert "✅ <b>fx</b> bağlandı. Bundan sonraki sinyaller bu hesapta da açılacak" in e.texts(Severity.INFO)[-1]
        assert [b.name for b in e.engine.brokers] == ["binance", "breakout", "fx", "tickmill"]   # configured order
        assert "fx" not in e.engine.failed_brokers and "✅ fx" in await e.engine.status_text()
        tries = connects(e.fx)
        await asyncio.sleep(0.15)
        assert connects(e.fx) == tries                           # connected: no further attempts

        e.notifier.notifications.clear()
        await e.say(eurusd(), FX, "EURUSD LONG 1R")
        (trade,) = await e.trades()
        assert await e.brokers_of(trade.id) == {"fx": "open", "tickmill": "open"}
        assert e.texts(Severity.ERROR) == []
    finally:
        await e.engine.stop(grace_sec=2.0)


async def test_e3_every_signal_names_the_required_broker_that_is_not_connected(db: Database) -> None:
    e = await started_without(db, "fx")
    try:
        e.notifier.notifications.clear()
        await e.say(eurusd(), FX, "EURUSD LONG 1R")
        (eur,) = await e.trades()
        assert await e.brokers_of(eur.id) == {"tickmill": "open"} and e.fx.opened == []
        assert e.texts(Severity.ERROR) == [
            "🚨 fx bağlı değil; bu işlem fx hesabında AÇILMADI (EUR LONG). Arka planda yeniden bağlanmaya "
            "çalışılıyor; bağlanınca haber verilecek."]

        e.notifier.notifications.clear()
        await e.say(btc_market(), KRIPTO, "btc 1R long")          # fx is not on that route: no word about it there
        assert e.texts(Severity.ERROR) == []
        await e.say(signal("GBP", "USD", Side.LONG, 1.3150, 1.3130, 1.3190, symbol="GBPUSD",
                           market_type=MarketType.FOREX), FX, "GBPUSD LONG 1R")
        assert len(e.texts(Severity.ERROR)) == 1 and "fx bağlı değil" in e.texts(Severity.ERROR)[0]   # every signal
    finally:
        await e.engine.stop(grace_sec=2.0)


async def test_e3_route_whose_brokers_are_all_down_says_that_they_are_being_retried(db: Database) -> None:
    e = await started_without(db, "fx", "tickmill")
    try:
        e.notifier.notifications.clear()
        await e.say(eurusd(), FX, "EURUSD LONG 1R")
        (alarm,) = e.texts(Severity.ERROR)
        assert "'fx' konusunun brokerı (fx, tickmill) bağlı değil. Sinyal başka bir hesapta AÇILMADI." in alarm
        assert "arka planda yeniden bağlanmaya çalışılıyor" in alarm
        (trade,) = await e.trades()
        assert trade.status is TradeStatus.FAILED
    finally:
        await e.engine.stop(grace_sec=2.0)


async def never_connecting(db: Database, *, optional_sec: float, first_sec: float) -> tuple[Env, dict[str, list[float]]]:
    """Started while fx (required) and tickmill (optional) cannot connect; the time of every connect attempt."""
    e = await prod_env(db, start=False)
    loop = asyncio.get_running_loop()
    stamps: dict[str, list[float]] = {"fx": [], "tickmill": []}
    for name in stamps:
        async def connect(seen: list[float] = stamps[name]) -> None:
            seen.append(loop.time())
            raise BrokerError(MT5_DOWN)

        e.brokers[name].connect = connect                        # type: ignore[method-assign]
    e.engine.optional_retry_sec, e.engine.required_retry_first_sec = optional_sec, first_sec
    await e.engine.start(monitor=False)
    return e, stamps


async def test_e3_waits_between_the_retries_of_a_required_broker_double_up_to_the_optional_period(db: Database) -> None:
    e, stamps = await never_connecting(db, optional_sec=0.2, first_sec=0.05)
    try:
        await e.until(lambda: len(stamps["fx"]) >= 6, "five retries of fx", timeout=5.0)
        gaps = [b - a for a, b in zip(stamps["fx"], stamps["fx"][1:])]
        # 0.05, 0.1, then 0.2 (the optional period) for good; a wait is never shorter than asked for
        for gap, least in zip(gaps, (0.05, 0.1, 0.2, 0.2, 0.2)):
            assert gap >= least * 0.95, (gaps, least)
        optional = [b - a for a, b in zip(stamps["tickmill"], stamps["tickmill"][1:])]
        assert optional and all(gap >= 0.2 * 0.95 for gap in optional)       # unchanged: the fixed period
    finally:
        await e.engine.stop(grace_sec=2.0)


async def test_e3_required_broker_is_retried_sooner_than_an_optional_one(db: Database) -> None:
    e, stamps = await never_connecting(db, optional_sec=3.0, first_sec=0.05)
    try:
        await e.until(lambda: len(stamps["fx"]) >= 4, "three retries of fx", timeout=3.0)     # after about 0.35 s
        assert len(stamps["tickmill"]) == 1          # the optional broker's first retry is not due before 3 s
    finally:
        await e.engine.stop(grace_sec=2.0)


async def test_e3_stop_ends_the_retry_of_a_required_broker(db: Database) -> None:
    e = await started_without(db, "fx", retry_sec=0.02, first_sec=0.02)
    assert any(t.get_name() == "retry-fx" for t in e.engine._tasks)
    await e.engine.stop(grace_sec=2.0)
    assert not any(not t.done() for t in e.engine._tasks)
    tries = connects(e.fx)
    await asyncio.sleep(0.1)
    assert connects(e.fx) == tries


async def test_e3_broker_that_could_not_even_be_created_is_not_retried_and_the_notice_says_so(db: Database) -> None:
    cfg = prod_config()
    brokers = {name: b for name, b in prod_brokers(cfg).items() if name != "fx"}
    e = await prod_env(db, start=False, brokers=brokers)
    e.engine.failed_brokers["fx"] = "MetaTrader5 paketi kurulu değil"       # what app.build_app records
    await e.engine.start(monitor=False)
    try:
        assert not any(t.get_name() == "retry-fx" for t in e.engine._tasks)
        e.notifier.notifications.clear()
        await e.say(eurusd(), FX, "EURUSD LONG 1R")
        assert e.texts(Severity.ERROR) == [
            "🚨 fx bağlı değil; bu işlem fx hesabında AÇILMADI (EUR LONG). Bu hesap, bot yeniden başlatılana kadar "
            "bağlanmaz."]
    finally:
        await e.engine.stop(grace_sec=2.0)


async def test_e3_optional_broker_keeps_its_own_short_notices(db: Database) -> None:
    e = await started_without(db, "tickmill")
    try:
        (warning,) = [t for t in e.texts(Severity.WARNING) if "bağlanamadı" in t]
        assert "isteğe bağlı hesap" in warning and e.texts(Severity.ERROR) == []
        e.notifier.notifications.clear()
        await e.say(eurusd(), FX, "EURUSD LONG 1R")
        assert e.texts(Severity.WARNING) == ["ℹ️ tickmill bağlı değil, bu işlem atlandı (EUR LONG)."]
        assert e.texts(Severity.ERROR) == []
    finally:
        await e.engine.stop(grace_sec=2.0)


@pytest.mark.parametrize("wrapped", [False, True])
async def test_e3_exchange_object_closed_after_the_failed_connect_is_opened_again_for_the_retry(
        db: Database, wrapped: bool) -> None:
    """The real ccxt class, no network: after ``close()`` it refuses every request ("instance was closed by the
    user") until ``open()`` is called. The engine closes a broker after a failed connect, so its retry has to
    open the adapter's exchange object again (measured with the real CcxtBroker in the batch notes)."""
    import ccxt.async_support as ccxt_async

    exchange = ccxt_async.binanceusdm()
    if not hasattr(exchange, "closed_by_user"):
        pytest.skip("this ccxt version has no closed_by_user")
    e = await prod_env(db, start=False)
    binance = e.binance
    if wrapped:
        binance.inner = SimpleNamespace(exchange=exchange)       # dry-run mode: DryRunBroker.inner is the CcxtBroker
    else:
        binance.exchange = exchange                  # like CcxtBroker.exchange
    attempts: list[str] = []

    async def connect() -> None:
        try:
            exchange.open(True)                      # what ccxt does before every request
        except Exception as exc:
            attempts.append(type(exc).__name__)
            raise BrokerError(f"binance: piyasa bilgileri yüklenemedi: {exc}") from exc
        if not attempts:
            attempts.append("NetworkError")
            raise BrokerError("binance: piyasa bilgileri yüklenemedi: connection reset by peer")
        attempts.append("ok")

    async def close() -> None:
        await exchange.close()

    binance.connect, binance.close = connect, close  # type: ignore[method-assign]
    e.engine.optional_retry_sec = e.engine.required_retry_first_sec = 0.02
    await e.engine.start(monitor=False)
    try:
        assert "binance" in e.engine.failed_brokers and exchange.closed_by_user is True
        await e.until(lambda: "binance" not in e.engine.failed_brokers, "binance to connect on a retry")
        assert attempts == ["NetworkError", "ok"]    # never "ExchangeClosedByUser"
        assert [b.name for b in e.engine.brokers] == ["binance", "breakout", "fx", "tickmill"]
    finally:
        await e.engine.stop(grace_sec=2.0)


# =========================================================================== #
# E2 - a management reply to a message the bot never stored
# =========================================================================== #
UNKNOWN = 4000          # yesterday's signal: posted before the bot started, never stored


async def open_btc(e: Env) -> tuple[TradeRecord, int]:
    """Today's copied trade: BTC LONG at market on binance, breakout and tickmill. (trade, id of its signal message)"""
    signal_id = await e.say(btc_market(), KRIPTO, "btc 1R long")
    (trade,) = await e.trades()
    assert await e.brokers_of(trade.id) == {"binance": "open", "breakout": "open", "tickmill": "open"}
    e.notifier.notifications.clear()
    return trade, signal_id


def btc_stopped(**kw: Any) -> ParsedAction:
    return manage(ActionType.CLOSE, base_asset="BTC", close_fraction=1.0, summary_tr="BTC kapatılacak", **kw)


@pytest.mark.parametrize("how", ["asset only", "refers to the replied message", "the model names the trade"])
async def test_e2_reply_to_a_signal_the_bot_never_stored_is_asked_not_applied_to_todays_trade(env: Env, how: str) -> None:
    trade, _ = await open_btc(env)
    action = {"asset only": btc_stopped(),
              "refers to the replied message": btc_stopped(refers_to_message_id=UNKNOWN),
              "the model names the trade": btc_stopped(target_trade_id=trade.id)}[how]

    await env.say(action, KRIPTO, "btc stop oldu", reply_to=UNKNOWN)

    assert env.closes() == {"binance": 0, "breakout": 0, "fx": 0, "tickmill": 0}      # nothing was closed
    assert (await env.db.get_trade(trade.id)).status is TradeStatus.OPEN              # type: ignore[union-attr]
    (conf,) = env.notifier.confirmations                                              # the owner decides
    (issue,) = [i for i in conf.issues if i.code == "reply_unlinked"]
    assert f"botun kaydında olmayan bir mesaja (#{UNKNOWN}) yanıt" in issue.message_tr
    assert f"#{trade.id} BTC işlemi tahminle seçildi" in issue.message_tr and not issue.blocking

    answer = await env.approve_if_asked()                                             # "yes, it is about #1"
    assert answer is not None and "uygulandı" in answer
    assert env.closes() == {"binance": 1, "breakout": 1, "fx": 0, "tickmill": 1}


async def test_e2_stop_alone_as_a_reply_to_an_unknown_message_names_both_doubts(env: Env) -> None:
    trade, _ = await open_btc(env)
    await env.say(manage(ActionType.CLOSE, close_fraction=1.0), KRIPTO, "Stop", reply_to=UNKNOWN)
    assert sum(env.closes().values()) == 0
    (codes,) = env.questions()
    assert "trade_guessed" in codes and "reply_unlinked" in codes


async def test_e2_reply_to_the_trades_own_signal_is_applied_as_before(env: Env) -> None:
    trade, signal_id = await open_btc(env)
    await env.say(btc_stopped(), KRIPTO, "btc stop oldu", reply_to=signal_id)
    assert env.closes() == {"binance": 1, "breakout": 1, "fx": 0, "tickmill": 1} and env.questions() == []


async def test_e2_reply_to_an_update_about_the_trade_is_applied_as_before(env: Env) -> None:
    trade, signal_id = await open_btc(env)
    update = await env.say(manage(ActionType.NONE, confidence=0.99, summary_tr="bilgi"), KRIPTO, "1R kârdayız",
                           reply_to=signal_id)
    await env.say(btc_stopped(), KRIPTO, "kapattık", reply_to=update)     # reply -> update -> the signal
    assert env.closes() == {"binance": 1, "breakout": 1, "fx": 0, "tickmill": 1} and env.questions() == []


async def test_e2_message_that_is_no_reply_is_applied_as_before(env: Env) -> None:
    trade, _ = await open_btc(env)
    await env.say(btc_stopped(), KRIPTO, "btc stop oldu")                 # top-level message of the topic
    assert env.closes() == {"binance": 1, "breakout": 1, "fx": 0, "tickmill": 1} and env.questions() == []


async def test_e2_reply_to_a_stored_message_without_a_trade_is_asked_as_before(env: Env) -> None:
    trade, _ = await open_btc(env)
    other = await env.say(manage(ActionType.NONE, confidence=0.99, summary_tr="analiz"), KRIPTO, "btc analiz")
    await env.say(btc_stopped(), KRIPTO, "btc stop oldu", reply_to=other)
    assert sum(env.closes().values()) == 0
    (codes,) = env.questions()
    assert codes == ["reply_unlinked"]
    assert f"Mesaj #{other} mesajına yanıt, ama o mesajdan kopyalanmış bir işlem yok" in \
        env.notifier.confirmations[0].issues[0].message_tr


async def test_e2_reply_to_an_unknown_message_without_any_trade_of_that_asset_is_handled_as_before(env: Env) -> None:
    await env.say(btc_stopped(), KRIPTO, "btc stop oldu", reply_to=UNKNOWN)
    (conf,) = env.notifier.confirmations
    assert conf.plan is None and [i.code for i in conf.issues] == ["trade_not_found"]


async def test_e2_reply_to_an_unknown_message_after_our_trade_ended_is_ignored_as_before(env: Env) -> None:
    trade, signal_id = await open_btc(env)
    await env.say(btc_stopped(), KRIPTO, "btc stop oldu", reply_to=signal_id)        # our copy is closed
    env.notifier.confirmations.clear()
    await env.say(btc_stopped(), KRIPTO, "btc stop oldu", reply_to=UNKNOWN)
    assert env.questions() == [] and env.closes() == {"binance": 1, "breakout": 1, "fx": 0, "tickmill": 1}


async def test_e2_edited_signal_that_itself_replies_to_an_unknown_message_gets_no_reply_doubt(env: Env) -> None:
    signal_id = await env.say(btc_market(), KRIPTO, "btc 1R long", reply_to=UNKNOWN)
    (trade,) = await env.trades()
    env.parser.push(ParseResult(actions=[btc_market(stop_loss=73200.0, used_message_ids=[signal_id])], model="fake"))
    edited = await env.db.get_message(CHAT, signal_id)
    assert edited is not None
    await env.engine.on_message_edited(edited.model_copy(update={"text": "btc 1R long (stop 73200)", "edited": True}))
    (codes,) = env.questions()
    assert "signal_edited" in codes and "reply_unlinked" not in codes


# =========================================================================== #
# E5 - what the prefilter must never hide
# =========================================================================== #
CHATTER = "tamamdır hocam"          # everyday talk: the prefilter drops it (checked by the fixture ``chatter``)


def parsed(e: Env) -> list[list[str]]:
    """Texts of the batches that reached the model, in order."""
    return [[m.text for m in msgs] for msgs, _ctx in e.parser.calls]


async def consumed(e: Env, message_id: int) -> bool:
    stored = await e.db.get_message(CHAT, message_id)
    assert stored is not None
    return stored.consumed


@pytest.fixture
def chatter() -> str:
    """A text the prefilter drops: these tests are about what reaches the model all the same."""
    probe = StoredMessage(chat_id=CHAT, message_id=1, date=datetime.now(UTC), text=CHATTER)
    assert prefilter.looks_relevant(probe) is False
    return CHATTER


async def test_e5_chatter_while_no_trade_is_active_stays_filtered(env: Env, chatter: str) -> None:
    mid = await env.say(None, KRIPTO, CHATTER)
    assert parsed(env) == [] and await consumed(env, mid)


async def test_e5_real_reply_reaches_the_model_whatever_its_words(env: Env, chatter: str) -> None:
    # No trade is active: a reply is still never dropped (it may be about a signal that waits for approval).
    mid = await env.say(manage(ActionType.NONE, confidence=0.99, summary_tr="sohbet"), KRIPTO, CHATTER, reply_to=UNKNOWN)
    assert parsed(env) == [[CHATTER]] and await consumed(env, mid)


async def test_e5_reply_with_an_unknown_word_manages_the_trade(env: Env, chatter: str) -> None:
    trade, signal_id = await open_btc(env)
    at_price(env, 74600.0)                                        # in profit: the stop can go to the entry
    # What matters is that the model gets to read it; the fake model understands it as "stop to entry".
    await env.say(manage(ActionType.MOVE_SL, base_asset="BTC", move_sl_to_entry=True, target_trade_id=trade.id),
                  KRIPTO, CHATTER, reply_to=signal_id)
    assert parsed(env)[-1] == [CHATTER] and env.questions() == []
    assert len(env.binance.calls_of("update_stop_loss")) == 1


async def test_e5_any_text_while_the_route_has_an_active_trade_reaches_the_model(env: Env, chatter: str) -> None:
    await open_btc(env)                                           # a kripto trade is active; nothing on the fx route
    before = len(env.parser.calls)
    fx_mid = await env.say(None, FX, CHATTER)                     # the fx topic has no active trade: still filtered
    assert len(env.parser.calls) == before and await consumed(env, fx_mid)

    await env.say(manage(ActionType.NONE, confidence=0.99, summary_tr="sohbet"), KRIPTO, CHATTER)
    assert parsed(env)[-1] == [CHATTER]

    empty = await env.say(None, KRIPTO, "   ")                    # nothing to read: not sent to the model
    assert len(env.parser.calls) == before + 1 and await consumed(env, empty)


async def test_e5_edit_of_a_reply_that_was_not_a_trade_message_is_read_again(env: Env, chatter: str) -> None:
    mid = await env.say(manage(ActionType.NONE, confidence=0.99, summary_tr="sohbet"), KRIPTO, CHATTER, reply_to=UNKNOWN)
    calls = len(env.parser.calls)
    env.parser.push(ParseResult(actions=[manage(ActionType.NONE, confidence=0.99, summary_tr="sohbet")], model="fake"))
    stored = await env.db.get_message(CHAT, mid)
    assert stored is not None
    edited = stored.model_copy(update={"text": CHATTER + " tamam", "edited": True})
    assert prefilter.looks_relevant(edited) is False
    await env.db.save_message(edited)
    await env.engine.on_message_edited(edited)
    assert len(env.parser.calls) == calls + 1


# =========================================================================== #
# E6 - a Claude outage of a few minutes does not drop the signal
# =========================================================================== #
def test_e6_transient_parse_errors_are_retried_for_several_minutes() -> None:
    assert orch.TRANSIENT_PARSE_RETRY_DELAYS == (10.0, 30.0, 60.0, 120.0, 180.0)
    assert sum(orch.TRANSIENT_PARSE_RETRY_DELAYS) == 400.0       # the last try starts 6 min 40 s after the first


async def test_e6_signal_is_opened_when_claude_answers_again_on_the_sixth_attempt(env: Env) -> None:
    assert env.engine.retry_delays == orch.TRANSIENT_PARSE_RETRY_DELAYS
    env.engine.retry_delays = tuple(0.0 for _ in env.engine.retry_delays)      # the same retries without the waits
    env.parser.push(*[ParserError("Claude API şu an yanıt vermiyor (529 overloaded)", transient=True)] * 5)
    await env.say(btc_market(), KRIPTO, "btc 1R long")           # the model's reading comes on the 6th attempt
    await env.until(lambda: len(env.tickmill.opened) == 1, "the signal to be opened")
    assert len(env.parser.calls) == 6
    (trade,) = await env.trades()
    assert await env.brokers_of(trade.id) == {"binance": "open", "breakout": "open", "tickmill": "open"}
    warnings = env.texts(Severity.WARNING)
    assert len(warnings) == 1 and "tekrar denenecek" in warnings[0]          # told once, never "given up"


async def test_e6_an_outage_longer_than_all_retries_still_ends_with_the_final_notice(env: Env) -> None:
    env.engine.retry_delays = tuple(0.0 for _ in env.engine.retry_delays)
    env.parser.push(ParserError("Claude API şu an yanıt vermiyor (529 overloaded)", transient=True))   # for ever
    await env.say(None, KRIPTO, "btc 1R long")
    await env.until(lambda: any("denemede okunamadı" in t for t in env.texts(Severity.WARNING)), "the final notice")
    assert len(env.parser.calls) == 6 and await env.trades() == []
    assert "Sinyal 6 denemede okunamadı" in env.texts(Severity.WARNING)[-1]


# =========================================================================== #
# E7 - topic titles typed without Turkish letters
# =========================================================================== #
@pytest.mark.parametrize(("title", "route"), [
    ("Kasım Ayı Kripto İşlemleri", "kripto"),                    # as today
    ("ARALIK AYI KRİPTO İŞLEMLERİ", "kripto"),
    ("Aralık Ayı Fx İşlemleri", "fx"),
    ("Aralik Ayi Kripto Islemleri", "kripto"),                   # a keyboard without Turkish letters
    ("KASIM AYI KRIPTO ISLEMLERI", "kripto"),
    ("Kasim Ayi Kripto Islemleri", "kripto"),
    ("Kasım Ayı Fx Islemleri", "fx"),
    ("ocak ayi fx islemleri", "fx"),
    (unicodedata.normalize("NFD", "Kasım Ayı Kripto İşlemleri"), "kripto"),      # decomposed letters
    (unicodedata.normalize("NFD", "ARALIK AYI FX İŞLEMLERİ"), "fx"),
    ("Kripto  İşlemleri (Aralık)", "kripto"),               # spacing
    ("Kripto Analizleri", None),                                 # other topics stay unfollowed
    ("Fx Sohbet", None),
    ("Kripto İşlemler", None),
    ("Genel", None),
    ("", None),
    (None, None),
])
def test_e7_topic_title_matches_its_route_with_and_without_turkish_letters(title: str | None, route: str | None) -> None:
    found = match_topic(prod_config().telegram.topics, title)
    assert (found.name if found is not None else None) == route


def test_e7_route_written_without_turkish_letters_matches_the_turkish_title() -> None:
    routes = [TopicRoute(name="kripto", title_contains=["Kripto Islemleri"]),
              TopicRoute(name="fx", title_contains=["FX ISLEMLERI"])]
    assert match_topic(routes, "Kasım Ayı Kripto İşlemleri").name == "kripto"        # type: ignore[union-attr]
    assert match_topic(routes, "Kasım Ayı Fx İşlemleri").name == "fx"                # type: ignore[union-attr]
    assert match_topic(routes, "Kasım Ayı Kripto Sohbet") is None


def test_e7_the_turkish_key_itself_is_unchanged() -> None:
    assert fold_tr("KASIM AYI KRİPTO İŞLEMLERİ") == "kasım ayı kripto işlemleri"     # the parser uses it too
    assert fold_tr("ISLEMLERI") == "ıslemlerı"


async def test_e7_signal_in_a_topic_typed_without_turkish_letters_is_followed(env: Env) -> None:
    env.parser.push(ParseResult(actions=[btc_market(used_message_ids=[7001])], model="fake"))
    await env.db.save_message(StoredMessage(
        chat_id=CHAT, message_id=7001, sender_id=SENDER, date=datetime.now(UTC), text="btc 1R long",
        reply_to_message_id=None, topic_id=30, topic_title="Aralik Ayi Kripto Islemleri"))
    await env.engine.on_messages_ready(CHAT, SENDER, 30)
    (trade,) = await env.trades()
    assert trade.plan.route == "kripto" and set(await env.brokers_of(trade.id)) == {"binance", "breakout", "tickmill"}


# =========================================================================== #
# E4 - data_dir and the Telegram session path follow the config file
# =========================================================================== #
def write_config(folder: Path, extra: str = "") -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "config.yaml"
    path.write_text(PROD_YAML + extra, encoding="utf-8")
    return path


def test_e4_relative_paths_follow_the_config_file_not_the_folder_the_bot_was_started_from(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bot, elsewhere = tmp_path / "bot", tmp_path / "elsewhere"
    config_path = write_config(bot)
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)                     # e.g. a scheduled task without "Start in"

    cfg = load_config(config_path)                   # --config C:\bot\config.yaml

    assert Path(cfg.data_dir) == bot / "data"
    assert Path(cfg.telegram.session_path) == bot / "data" / "telegram_user"
    app = build_app(cfg, parser=FakeParser(), notifier=FakeNotifier(), broker_factory=lambda c: FakeBroker(c.name),
                    listener_factory=lambda *a, **k: object())
    assert Path(app.db.path) == bot / "data" / "copytrader.db"
    assert list(elsewhere.iterdir()) == []           # nothing is created where the bot was started from


@pytest.mark.parametrize("argument", ["config.yaml", "./config.yaml", None])
def test_e4_the_owners_layout_started_from_the_bot_folder_is_exactly_as_before(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, argument: str | None) -> None:
    write_config(tmp_path)
    monkeypatch.chdir(tmp_path)
    cfg = load_config() if argument is None else load_config(argument)
    assert cfg.data_dir == "data" and cfg.telegram.session_path == "data/telegram_user"      # the very same strings


def test_e4_absolute_and_home_paths_stay_as_written_and_other_relative_ones_follow_the_config(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    absolute = tmp_path / "var" / "bot-data"
    cfg = load_config(write_config(tmp_path / "a", f"data_dir: {absolute.as_posix()}\n"))
    assert cfg.data_dir == absolute.as_posix()
    assert Path(cfg.telegram.session_path) == tmp_path / "a" / "data" / "telegram_user"

    cfg = load_config(write_config(tmp_path / "b", 'data_dir: "~/bot-data"\n'))
    assert cfg.data_dir == "~/bot-data"              # expanded by its users, as before

    custom = PROD_YAML.replace("  api_hash: h\n", "  api_hash: h\n  session_path: oturum/kullanici\n")
    (tmp_path / "c").mkdir()
    (tmp_path / "c" / "config.yaml").write_text(custom + "data_dir: veri\n", encoding="utf-8")
    cfg = load_config(tmp_path / "c" / "config.yaml")
    assert Path(cfg.data_dir) == tmp_path / "c" / "veri"
    assert Path(cfg.telegram.session_path) == tmp_path / "c" / "oturum" / "kullanici"


# =========================================================================== #
# E9 - stop-distance bounds in the risk config (read by the validator)
# =========================================================================== #
STOP_BOUNDS = {"min_stop_distance_pct_crypto": 0.15, "max_stop_distance_pct_crypto": 25.0,
               "min_stop_distance_pct_fx": 0.03, "max_stop_distance_pct_fx": 3.0,
               "min_stop_distance_pct_cfd": 0.05, "max_stop_distance_pct_cfd": 8.0}


def test_e9_risk_config_carries_the_stop_distance_bounds_with_the_validators_defaults() -> None:
    risk = RiskConfig()
    assert {name: getattr(risk, name) for name in STOP_BOUNDS} == STOP_BOUNDS
    defaults = getattr(validator, "STOP_DISTANCE_PCT_DEFAULTS", None)
    if defaults is not None:                         # the validator of this tree: the same numbers
        assert {f"{edge}_stop_distance_pct_{cls}": value
                for cls, pair in defaults.items() for edge, value in zip(("min", "max"), pair)} == STOP_BOUNDS


def test_e9_bounds_can_be_set_in_config_yaml_and_zero_switches_one_off(tmp_path: Path) -> None:
    cfg = load_config(write_config(tmp_path, "risk:\n  max_stop_distance_pct_crypto: 0\n  min_stop_distance_pct_fx: 0.05\n"))
    assert cfg.risk.max_stop_distance_pct_crypto == 0 and cfg.risk.min_stop_distance_pct_fx == 0.05
    with pytest.raises(ValueError):
        RiskConfig(min_stop_distance_pct_cfd=-1)


async def test_e9_the_validator_asks_about_a_stop_outside_the_configured_bounds(env: Env) -> None:
    if not hasattr(validator, "STOP_DISTANCE_PCT_DEFAULTS"):
        pytest.skip("this validator has no stop-distance check")
    wide = signal("BTC", "USDT", Side.LONG, 74000.0, 51800.0, 80000.0, symbol="BTCUSDT.P",
                  market_type=MarketType.PERP)                    # stop 30% away
    await env.say(wide, KRIPTO, "btc 1R long")
    assert env.questions() == [["stop_too_wide"]] and await env.trades() == []
    await env.engine.handle_confirmation(env.notifier.confirmations[0].id, False)        # "no"

    env.notifier.confirmations.clear()
    env.cfg.risk.max_stop_distance_pct_crypto = 0                 # the owner switches that bound off
    await env.say(wide, KRIPTO, "btc 1R long")
    assert env.questions() == [] and len(await env.trades()) == 1


# =========================================================================== #
# E8 - contract prefixes in the public price lookup
# =========================================================================== #
class PublicExchange:
    """ccxt-like public exchange: ``tickers`` / ``ohlcv`` by symbol; an unknown symbol raises like ccxt does
    (BadSymbol), ``down`` makes every request fail like an unreachable exchange."""

    def __init__(self, tickers: dict[str, float], ohlcv: dict[str, list[list[float]]] | None = None) -> None:
        self.tickers, self.ohlcv = dict(tickers), dict(ohlcv or {})
        self.asked: list[str] = []
        self.down: Exception | None = None

    async def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        self.asked.append(symbol)
        if self.down is not None:
            raise self.down
        if symbol not in self.tickers:
            raise ccxt.BadSymbol(f"binanceusdm does not have market symbol {symbol}")
        price = self.tickers[symbol]
        return {"symbol": symbol, "bid": price, "ask": price, "last": price}

    async def fetch_ohlcv(self, symbol: str, timeframe: str = "1m", since: int | None = None,
                          limit: int | None = None) -> list[list[float]]:
        self.asked.append(symbol)
        if self.down is not None:
            raise self.down
        if symbol not in self.ohlcv:
            raise ccxt.BadSymbol(f"binanceusdm does not have market symbol {symbol}")
        return [row for row in self.ohlcv[symbol] if since is None or row[0] >= since]


@pytest.mark.parametrize(("base", "names"), [
    ("BTC", [("BTC", 1.0), ("1000BTC", 1e3), ("10000BTC", 1e4), ("100000BTC", 1e5), ("1000000BTC", 1e6),
             ("1MBTC", 1e6)]),
    ("1000PEPE", [("1000PEPE", 1.0), ("PEPE", 1e-3), ("10000PEPE", 10.0), ("100000PEPE", 100.0),
                  ("1000000PEPE", 1e3), ("1MPEPE", 1e3)]),
    ("1MBABYDOGE", [("1MBABYDOGE", 1.0), ("BABYDOGE", 1e-6), ("1000BABYDOGE", 1e-3), ("10000BABYDOGE", 1e-2),
                    ("100000BABYDOGE", 1e-1), ("1000000BABYDOGE", 1.0)]),
    ("1000000mog", [("1000000MOG", 1.0), ("MOG", 1e-6), ("1000MOG", 1e-3), ("10000MOG", 1e-2), ("100000MOG", 1e-1),
                    ("1MMOG", 1.0)]),
])
def test_e8_contract_names_follow_the_rule_of_the_exchange_adapter(base: str, names: list[tuple[str, float]]) -> None:
    found = prices_mod.contract_names(base)
    assert [n for n, _ in found] == [n for n, _ in names]
    assert [m for _, m in found] == pytest.approx([m for _, m in names])
    adapter_rule = getattr(ccxt_broker, "_base_variants", None)
    if adapter_rule is not None:                     # the same candidates, in the same order, as the adapter
        theirs = adapter_rule(base.upper())
        assert [n for n, _ in theirs] == [n for n, _ in found]
        assert [m for _, m in theirs] == pytest.approx([m for _, m in found])


@pytest.mark.parametrize(("listed", "price", "asked_for", "expected"), [
    ("BTC/USDT:USDT", 74000.0, "BTC", 74000.0),
    ("1000PEPE/USDT:USDT", 0.0120, "PEPE", 0.0000120),                # as before
    ("10000LADYS/USDT:USDT", 0.0005, "LADYS", 0.00000005),
    ("100000MOG/USDT:USDT", 0.09, "MOG", 0.0000009),
    ("1000000MOG/USDT:USDT", 0.9, "MOG", 0.0000009),
    ("1MBABYDOGE/USDT:USDT", 1.3, "BABYDOGE", 0.0000013),
    ("1MBABYDOGE/USDT:USDT", 1.3, "1000000BABYDOGE", 1.3),             # the reading names the long prefix
    ("1MBABYDOGE/USDT:USDT", 1.3, "1MBABYDOGE", 1.3),
    ("1000PEPE/USDT:USDT", 0.0120, "1000PEPE", 0.0120),               # the reading names the contract itself
])
async def test_e8_public_price_is_given_in_the_signals_scale_for_every_contract_prefix(
        listed: str, price: float, asked_for: str, expected: float) -> None:
    exchange = PublicExchange({listed: price})
    assert await prices_mod.fetch_public_price(exchange, asked_for, 1.0) == pytest.approx(expected)
    oracle = PriceOracle([], public_exchange=exchange)
    assert await oracle.price(asked_for) == pytest.approx(expected)
    reference = ReferencePriceOracle(exchanges={"binanceusdm": exchange})
    assert await reference.mid("binanceusdm", asked_for) == pytest.approx(expected)


async def test_e8_unknown_coin_is_none_and_an_unreachable_exchange_is_asked_only_once() -> None:
    exchange = PublicExchange({"BTC/USDT:USDT": 74000.0})
    assert await prices_mod.fetch_public_price(exchange, "NOSUCHCOIN", 1.0) is None
    assert len(exchange.asked) == 6                  # every contract name was tried

    for failure in (ccxt.RequestTimeout("binanceusdm GET https://fapi.binance.com/... timed out"),
                    ccxt.NetworkError("connection reset"), TimeoutError()):
        exchange.asked.clear()
        exchange.down = failure
        assert await prices_mod.fetch_public_price(exchange, "BTC", 1.0) is None
        assert exchange.asked == ["BTC/USDT:USDT"]   # not once more per prefix: they would fail the same way


async def test_e8_reference_candles_know_the_contract_prefixes_too() -> None:
    minute = 1_794_000_000_000 // MINUTE * MINUTE
    exchange = PublicExchange({}, {"1MBABYDOGE/USDT:USDT": [[minute, 1.30, 1.36, 1.28, 1.31, 10.0]]})
    reference = ReferencePriceOracle(exchanges={"binanceusdm": exchange})
    rows = await reference.candles("binanceusdm", "BABYDOGE", minute, minute + 30_000)
    assert rows == [(minute, pytest.approx(1.28e-6), pytest.approx(1.36e-6))]
    exchange.asked.clear()
    await reference.candles("binanceusdm", "BABYDOGE", minute, minute + 30_000)
    assert exchange.asked == ["1MBABYDOGE/USDT:USDT"]             # the name that answered is remembered

    exchange.down = ccxt.NetworkError("connection reset")
    other = ReferencePriceOracle(exchanges={"binanceusdm": exchange})
    exchange.asked.clear()
    assert await other.candles("binanceusdm", "BTC", minute, minute + 30_000) is None
    assert exchange.asked == ["BTC/USDT:USDT"]


# =========================================================================== #
# E10 - a signal or its outcome never disappears with a log line only
# =========================================================================== #
async def test_e10_background_job_of_the_engine_that_dies_is_reported_once_in_ten_minutes(env: Env) -> None:
    env.engine.retry_delays = (0.0, 0.0)
    env.parser.push(ParserError("Claude API şu an yanıt vermiyor (529 overloaded)", transient=True))
    real = env.db.recent_messages
    calls = {"n": 0}

    async def locked_on_the_retry(*args: Any, **kw: Any) -> Any:
        calls["n"] += 1
        if calls["n"] >= 2:                          # the retry of the signal cannot read the database
            raise sqlite3.OperationalError("database is locked")
        return await real(*args, **kw)

    env.db.recent_messages = locked_on_the_retry     # type: ignore[method-assign]
    await env.say(None, KRIPTO, "btc 1R long")       # first reading fails: a retry is scheduled
    await env.until(lambda: env.texts(Severity.ERROR) != [], "the owner to be told")
    (alarm,) = env.texts(Severity.ERROR)
    assert "okunamayan bir sinyalin yeniden denenmesi" in alarm and "database is locked" in alarm
    assert "grubu ve açık işlemleri kontrol edin" in alarm

    # The same kind of failure again a moment later: in the log, not a second message ...
    env.engine._spawn(locked_on_the_retry(), "parse-retry-again")
    await asyncio.sleep(0.05)
    assert len(env.texts(Severity.ERROR)) == 1
    # ... but again once ten minutes have passed.
    for kind in env.engine._lost_noticed:
        env.engine._lost_noticed[kind] -= orch.LOST_NOTICE_THROTTLE_SEC + 1
    env.engine._spawn(locked_on_the_retry(), "parse-retry-later")
    await env.until(lambda: len(env.texts(Severity.ERROR)) == 2, "the second notice")
    assert orch.LOST_NOTICE_THROTTLE_SEC == 600.0


async def test_e10_order_flow_that_fails_after_its_caller_was_cancelled_is_reported(env: Env) -> None:
    reached, release = asyncio.Event(), asyncio.Event()

    async def update_trade(*args: Any, **kw: Any) -> Any:
        reached.set()
        await release.wait()
        raise sqlite3.OperationalError("database is locked")

    env.db.update_trade = update_trade               # type: ignore[method-assign]
    plan = _plan(EntryType.MARKET, 74000.0, 73300.0, 75500.0)
    caller = asyncio.ensure_future(env.engine.execute_open(plan, raw_symbol="BTCUSDT.P"))
    await asyncio.wait_for(reached.wait(), 2.0)      # the orders are out; the flow is about to book the trade
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    assert env.texts(Severity.ERROR) == []
    release.set()                                    # ... and then it fails, with nobody waiting for its result
    await env.until(lambda: env.texts(Severity.ERROR) != [], "the owner to be told")
    (alarm,) = env.texts(Severity.ERROR)
    assert alarm.startswith("🚨 Bir emir işlemi (yeni işlem açma) beklenmeyen bir hatayla yarıda kaldı")
    assert "database is locked" in alarm and "borsada/brokerda kontrol edin" in alarm
    del env.db.update_trade                          # type: ignore[attr-defined]


async def test_e10_message_whose_topic_is_unknown_is_reported_not_dropped_in_silence(env: Env) -> None:
    async def arrives(message_id: int, topic_id: int, title: str | None) -> None:
        await env.db.save_message(StoredMessage(
            chat_id=CHAT, message_id=message_id, sender_id=SENDER, date=datetime.now(UTC), text="btc 1R long",
            reply_to_message_id=None, topic_id=topic_id, topic_title=title))
        await env.engine.on_messages_ready(CHAT, SENDER, topic_id)

    await arrives(8001, 77, None)                    # the title of topic 77 was never found out
    assert (await env.db.get_message(CHAT, 8001)).consumed and env.parser.calls == []     # type: ignore[union-attr]
    (warning,) = env.texts(Severity.WARNING)
    assert "hangi konuya (topic) ait olduğu belirlenemedi" in warning and "8001" in warning

    await arrives(8002, 77, None)                    # again within ten minutes: one notice is enough
    assert len(env.texts(Severity.WARNING)) == 1
    await arrives(8003, 78, "Sohbet")                # a topic that is simply not followed: no notice, as before
    assert len(env.texts(Severity.WARNING)) == 1 and env.parser.calls == []


# =========================================================================== #
# E11 - the engine tells the validator the route's kind and whether a percentage of equity applies
#       (asked for by the parser batch: its validator takes ``route_kind`` / ``risk_pct_applies``)
# =========================================================================== #
VALIDATOR_PARAMS = set(inspect.signature(validator.build_open_plan).parameters)
needs_route_kind = pytest.mark.skipif("route_kind" not in VALIDATOR_PARAMS, reason="validator without route_kind")
needs_risk_pct_applies = pytest.mark.skipif("risk_pct_applies" not in VALIDATOR_PARAMS,
                                            reason="validator without risk_pct_applies")


@needs_route_kind
async def test_e11_stop_distance_of_an_index_in_the_fx_topic_is_judged_as_an_index_not_as_a_coin(env: Env) -> None:
    # FRA40 (a ticker the symbol table does not know) with a stop 0.08% away: plausible for an index
    # (0.05% .. 8%), far too tight for a coin (0.15% .. 25%).
    index = signal("FRA40", None, Side.LONG, 8000.0, 7993.6, 8030.0, symbol="FRA40", market_type=None)

    async def price(*args: Any, **kw: Any) -> float | None:
        return 8000.0

    env.oracle.price = price                         # type: ignore[method-assign]
    route = env.engine._route_named("fx")
    ev = await env.engine.evaluate(index, CHAT, route=route)
    assert ev is not None and [i.code for i in ev.issues] == [] and ev.automatic
    without = await env.engine.evaluate(index, CHAT, route=None)          # no topic: an unknown ticker counts as a coin
    assert without is not None and "stop_too_tight" in [i.code for i in without.issues]


@needs_risk_pct_applies
async def test_e11_summary_shows_no_percentage_of_equity_when_every_broker_of_the_route_risks_a_fixed_amount(
        env: Env) -> None:
    route = env.engine._route_named("kripto")
    ev = await env.engine.evaluate(btc_market(), CHAT, route=route)
    assert ev is not None and isinstance(ev.plan, TradePlan)
    assert ev.plan.summary_tr == ("BTC LONG (piyasa), giriş 74000, SL 73300, TP 75500, risk 1R · konu kripto → "
                                  "binance, breakout, tickmill")
    too_much = await env.engine.evaluate(btc_market(risk_r=3.0), CHAT, route=route)
    assert too_much is not None
    assert [i.message_tr for i in too_much.issues if i.code == "risk_r_too_high"] == [
        "Sinyal riski 3R — izin verilen en fazla 2R. Onaylanırsa 3R ile açılır."]

    env.binance.config.risk_usd = None               # one broker of the route sizes by a percentage of its equity
    ev = await env.engine.evaluate(btc_market(), CHAT, route=route)
    assert ev is not None and isinstance(ev.plan, TradePlan) and "risk 1R (%2) · konu kripto" in ev.plan.summary_tr


async def test_e11_engine_passes_the_validator_only_arguments_it_knows(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}
    real = validator.build_open_plan

    def older_validator(action: Any, *, risk: Any, llm: Any, market_price: Any, source_chat_id: int, now: Any,
                        route: str | None = None, broker_names: list[str] | None = None) -> Any:
        seen["called"] = True                        # the signature before the parser batch: no extra arguments
        return real(action, risk=risk, llm=llm, market_price=market_price, source_chat_id=source_chat_id, now=now,
                    route=route, broker_names=broker_names)

    monkeypatch.setattr(validator, "build_open_plan", older_validator)
    monkeypatch.setattr(orch, "_OPEN_PLAN_PARAMS", frozenset(inspect.signature(older_validator).parameters))
    await env.say(btc_market(), KRIPTO, "btc 1R long")
    (trade,) = await env.trades()
    assert seen == {"called": True} and trade.status is TradeStatus.OPEN
