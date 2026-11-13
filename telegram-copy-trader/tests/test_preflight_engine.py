"""Pre-flight fixes in the engine and in the parser's topic normalisation, on a replica of the owner's
production setup (routes "kripto" / "fx"; brokers binance / breakout / fx / tickmill) with fake brokers:

1. the price offset and the virtual trigger apply to crypto plans only, decided by the ROUTE kind;
2. fx-topic normalisation in the parser (a market type for every base, the pair named by the symbol);
3. no_hedge and the opposite-trade question compare the INSTRUMENT, not only the base currency;
4. a market order is never sized larger than K times the size implied by the signal's own stop distance.
"""

from __future__ import annotations

import itertools
from datetime import UTC, datetime
from typing import Any

import pytest
import yaml

from copytrader.config import AppConfig, NotifierConfig, RiskConfig, TelegramConfig, TopicRoute
from copytrader.engine import orchestrator as orch
from copytrader.engine.monitor import is_virtual_pending
from copytrader.engine.orchestrator import Engine, virtual_entry_wanted
from copytrader.models import (
    ActionType,
    BrokerPosition,
    EntryType,
    MarketType,
    OpenRequest,
    ParseContext,
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
from copytrader.parsing.llm_parser import ClaudeSignalParser, _apply_topic_kind
from copytrader.storage.db import Database
from copytrader.telegram import formatting as fmt

from fakes import FakeBroker, FakeNotifier, FakeParser
from test_llm_parser import make_response, msg, parser_with, wire_action

CHAT, SENDER = -1003782294463, 900
KRIPTO, FX = 10, 20
TITLES = {KRIPTO: "Kasım Ayı Kripto İşlemleri", FX: "Kasım Ayı Fx İşlemleri"}
NOW = datetime(2026, 11, 12, 9, 0, tzinfo=UTC)
PIP = 0.0001

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
    """reference_prices double: mid(exchange_id, base) -> scripted value (None = not listed there)."""

    def __init__(self, prices: dict[str, float | None] | None = None) -> None:
        self.prices: dict[str, float | None] = dict(prices or {})
        self.calls: list[tuple[str, str]] = []

    async def mid(self, exchange_id: str, base_asset: str, *, fresh: bool = False) -> float | None:
        self.calls.append((exchange_id, base_asset.upper()))
        return self.prices.get(base_asset.upper())


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


class VenueFake(FakeBroker):
    """FakeBroker that carries a REAL broker config (no_hedge, reference_exchange, entry_trigger, risk_usd,
    allowed_assets, symbol_map) and names instruments like the venue does: MetaTrader "USDJPY" / "BTCUSD"
    (symbol_map first), everything else "BTC/USDT:USDT". Quotes are looked up by that broker symbol."""

    def __init__(self, config: Any, **kw: Any) -> None:
        super().__init__(config.name, **kw)
        self.config = config

    async def resolve_symbol(self, base_asset: str, quote_asset: str | None = None,
                             market_type: MarketType | None = None, raw_symbol: str | None = None) -> SymbolInfo | None:
        self.calls.append(("resolve_symbol", (base_asset, quote_asset, market_type, raw_symbol)))
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
        return self._quote(symbol)

    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        pos = await super().open_position(req)
        if pos.avg_entry_price is not None:
            pos.avg_entry_price = self._quote(req.symbol).entry_price_for(req.plan.side)
        return pos


FX_QUOTES = {"EURUSD": q(1.16195, 1.16205), "USDJPY": q(154.195, 154.205), "USDCAD": q(1.40195, 1.40205),
             "EURJPY": q(178.890, 178.910), "GBPUSD": q(1.31495, 1.31505), "GBPJPY": q(202.490, 202.510),
             "XAUUSD": q(4099.90, 4100.10)}
PRICES = {"EURUSD": 1.1620, "USDJPY": 154.20, "USDCAD": 1.4020, "EURJPY": 178.90, "GBPUSD": 1.3150,
          "GBPJPY": 202.50, "XAUUSD": 4100.0, "EUR": 1.1620, "BTC": 74000.0, "ETH": 2232.0}


class Env:
    def __init__(self, cfg: AppConfig, db: Database, brokers: dict[str, VenueFake], parser: FakeParser,
                 notifier: FakeNotifier, ref: Reference, oracle: PairOracle, engine: Engine) -> None:
        self.cfg, self.db, self.brokers, self.parser = cfg, db, brokers, parser
        self.notifier, self.ref, self.oracle, self.engine = notifier, ref, oracle, engine
        self.mid = 5000

    def __getattr__(self, name: str) -> VenueFake:          # env.fx, env.tickmill, env.binance, env.breakout
        try:
            return self.__dict__["brokers"][name]
        except KeyError:
            raise AttributeError(name) from None

    def route(self, name: str) -> TopicRoute:
        return next(r for r in self.cfg.telegram.topics if r.name == name)

    async def say(self, action: ParsedAction, topic: int, text: str = "LONG 1R") -> int:
        """The trader posts a message in ``topic``; the (fake) parser reads it as ``action``."""
        self.mid += 1
        mid = self.mid
        self.parser.push(ParseResult(actions=[action.model_copy(update={"used_message_ids": [mid]})], model="fake"))
        await self.db.save_message(StoredMessage(
            chat_id=CHAT, message_id=mid, sender_id=SENDER, sender_name="Hoca", date=datetime.now(UTC), text=text,
            reply_to_message_id=topic, topic_id=topic, topic_title=TITLES[topic]))
        await self.engine.on_messages_ready(CHAT, SENDER, topic)
        return mid

    async def plan(self, action: ParsedAction, route: str | None) -> TradePlan:
        """The plan the engine builds for ``action`` (the owner may still be asked about it)."""
        ev = await self.engine.evaluate(action.model_copy(update={"used_message_ids": [self.mid + 1]}), CHAT,
                                        route=self.route(route) if route else None)
        assert ev is not None and isinstance(ev.plan, TradePlan), ev
        return ev.plan

    async def trades(self) -> list[TradeRecord]:
        return sorted(await self.db.recent_trades(datetime(2000, 1, 1, tzinfo=UTC)), key=lambda t: t.id)

    async def position(self, trade_id: int, broker: str) -> BrokerPosition:
        return next(p for p in await self.db.broker_positions(trade_id) if p.broker == broker)

    def texts(self, severity: Severity | None = None) -> list[str]:
        return self.notifier.texts(severity)


async def prod_env(db: Database) -> Env:
    """The production replica: real config objects on fake venues (fx: 10,000 USD prop account)."""
    cfg = prod_config()
    by = {b.name: b for b in cfg.brokers}
    brokers = {
        "binance": VenueFake(by["binance"], equity=46.9, currency="USDT"),
        "breakout": VenueFake(by["breakout"], equity=1_000_000.0, currency="USDT"),
        "fx": VenueFake(by["fx"], equity=10_000.0, currency="USD", qty_step=0.01),
        "tickmill": VenueFake(by["tickmill"], equity=108.0, currency="USD", qty_step=0.01),
    }
    for name in ("binance", "breakout"):
        brokers[name].quotes.update({"BTC/USDT:USDT": q(73999.0, 74001.0), "ETH/USDT:USDT": q(2231.9, 2232.1)})
    brokers["fx"].quotes.update(FX_QUOTES)
    brokers["tickmill"].quotes.update(FX_QUOTES)
    brokers["tickmill"].quotes["BTCUSD"] = q(73949.0, 73951.0)       # CFD price: 50 USD below Binance
    parser, notifier = FakeParser(), FakeNotifier()
    ref, oracle = Reference({"BTC": 74000.0}), PairOracle(PRICES)
    engine = Engine(cfg, db, parser, list(brokers.values()), notifier, price_oracle=oracle, reference_prices=ref)
    await engine.start(monitor=False)
    assert [b.name for b in engine.brokers] == ["binance", "breakout", "fx", "tickmill"]
    notifier.notifications.clear()
    return Env(cfg, db, brokers, parser, notifier, ref, oracle, engine)


async def plain_env(db: Database, *, routes: list[TopicRoute] | None = None, no_hedge: bool = False,
                    trigger: str = "reference_touch") -> Env:
    """One offset broker ("tickmill", plain FakeBroker naming) and no routes unless given."""
    cfg = AppConfig(
        telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[CHAT], topics=routes or []),
        notifier=NotifierConfig(bot_token="123:abc", owner_user_id=42),
    )
    cfg.monitor_interval_sec = 3600.0
    broker = FakeBroker("tickmill", equity=10_000.0, quote=q(73949.0, 73951.0))
    broker.config.reference_exchange = "binanceusdm"
    broker.config.entry_trigger = trigger            # type: ignore[assignment]
    broker.config.no_hedge = no_hedge
    parser, notifier = FakeParser(), FakeNotifier()
    ref, oracle = Reference({"BTC": 74000.0}), PairOracle(PRICES)
    engine = Engine(cfg, db, parser, [broker], notifier, price_oracle=oracle, reference_prices=ref)
    await engine.start(monitor=False)
    notifier.notifications.clear()
    return Env(cfg, db, {"tickmill": broker}, parser, notifier, ref, oracle, engine)   # type: ignore[dict-item]


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
    await e.engine.stop()


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


def usdjpy_untyped(entry_type: EntryType = EntryType.MARKET) -> ParsedAction:
    """USDJPY long as the model may leave it: base + quote, no symbol, market_type null."""
    if entry_type is EntryType.MARKET:
        return signal("USD", "JPY", Side.LONG, 154.20, 154.00, 154.60)
    return signal("USD", "JPY", Side.LONG, 154.05, 153.85, 154.45, entry_type=EntryType.LIMIT)


def btc(market_type: MarketType | None, entry_type: EntryType = EntryType.MARKET,
        side: Side = Side.LONG) -> ParsedAction:
    """BTCUSDT signal: market long 74000 / SL 73300, limit long 73000 / SL 72300, or market short 74000 / SL 74700."""
    if side is Side.SHORT:
        return signal("BTC", "USDT", Side.SHORT, 74000.0, 74700.0, 72500.0, symbol="BTCUSDT", market_type=market_type)
    if entry_type is EntryType.MARKET:
        return signal("BTC", "USDT", Side.LONG, 74000.0, 73300.0, 75500.0, symbol="BTCUSDT", market_type=market_type)
    return signal("BTC", "USDT", Side.LONG, 73000.0, 72300.0, 75000.0, symbol="BTCUSDT", market_type=market_type,
                  entry_type=EntryType.LIMIT)


def fx_pair(pair: str, side: Side, entry: float, sl: float, tp: float, **kw: Any) -> ParsedAction:
    return signal(pair[:3], pair[3:], side, entry, sl, tp, symbol=pair, market_type=MarketType.FOREX, **kw)


_ids = itertools.count(1)
FX_ROUTE: dict[str, Any] = dict(route="fx", brokers=["fx", "tickmill"])
KRIPTO_ROUTE: dict[str, Any] = dict(route="kripto", brokers=["binance", "breakout", "tickmill"])


def plan_of(base: str, quote: str | None, side: Side, *, route: str | None = None, brokers: list[str] | None = None,
            entry: float | None = 100.0, sl: float = 90.0, tp: float | None = None,
            entry_type: EntryType = EntryType.MARKET, market_type: MarketType | None = None) -> TradePlan:
    """A plan built by hand (what the validator would hand to the engine), with its own id and source message."""
    n = next(_ids)
    return TradePlan(plan_id=f"p{n}", base_asset=base, quote_asset=quote, market_type=market_type, side=side,
                     entry_type=entry_type, entry_price=entry, stop_loss=sl,
                     take_profits=[TakeProfit(price=tp)] if tp is not None else [], risk_r=1.0, risk_pct=2.0,
                     source_chat_id=CHAT, source_message_ids=[90_000 + n], created_at=datetime.now(UTC), route=route,
                     broker_names=list(brokers or []))


# =========================================================================== #
# TASK 1 - price offset / virtual trigger: crypto plans only, decided by the route kind
# =========================================================================== #
async def test_fx_route_untyped_market_plan_opens_unshifted_and_never_asks_the_reference(env: Env) -> None:
    await env.say(usdjpy_untyped(), FX, "USDJPY LONG 1R")

    assert env.ref.calls == []                                   # Binance is never asked for "USD"
    (req,) = env.tickmill.opened
    assert req.entry_type is EntryType.MARKET and req.symbol.broker_symbol == "USDJPY"
    assert req.stop_loss == 154.00 and [tp.price for tp in req.take_profits] == [154.60]
    assert env.tickmill.sized[0]["stop"] == 154.00
    assert len(env.fx.opened) == 1
    (trade,) = await env.trades()
    assert trade.plan.market_type is None and trade.status is TradeStatus.OPEN
    assert "price_offset" not in (await env.position(trade.id, "tickmill")).data
    assert not any("referans" in t for t in env.texts()) and env.notifier.confirmations == []


async def test_fx_route_untyped_limit_plan_is_a_normal_broker_limit_not_a_virtual_trigger(env: Env) -> None:
    await env.say(usdjpy_untyped(EntryType.LIMIT), FX, "USDJPY LONG 1R Limit Emir")

    (req,) = env.tickmill.opened                                 # a real limit order on the broker
    assert req.entry_type is EntryType.LIMIT and req.entry_price == 154.05 and req.stop_loss == 153.85
    (trade,) = await env.trades()
    pos = await env.position(trade.id, "tickmill")
    assert pos.status is PositionStatus.PENDING and not is_virtual_pending(pos)
    assert "virtual_trigger" not in pos.data
    assert not any("sanal tetik" in t for t in env.texts())
    await env.engine.monitor.run_virtual_once()                  # nothing waits on Binance "USD/USDT"
    assert env.ref.calls == []


async def test_crypto_route_spot_market_plan_gets_the_offset(env: Env) -> None:
    await env.say(btc(MarketType.SPOT), KRIPTO, "btc spot 1R long")

    (req,) = env.tickmill.opened
    assert req.stop_loss == pytest.approx(73250.0)               # 73300 shifted by (73950 - 74000)
    assert [tp.price for tp in req.take_profits] == [pytest.approx(75450.0)]
    assert env.ref.calls == [("binanceusdm", "BTC")]
    (trade,) = await env.trades()
    assert trade.plan.market_type is MarketType.SPOT
    offset = (await env.position(trade.id, "tickmill")).data["price_offset"]
    assert offset["mode"] == "additive" and offset["value"] == pytest.approx(-50.0)
    assert env.binance.opened[0].stop_loss == 73300.0            # no reference_exchange: chart levels as they are


async def test_crypto_route_spot_limit_plan_registers_a_virtual_trigger_and_fires_shifted(env: Env) -> None:
    await env.say(btc(MarketType.SPOT, EntryType.LIMIT), KRIPTO, "btc spot 1R long limit emir")

    assert env.tickmill.opened == []                             # no order at the unshifted chart price
    (trade,) = await env.trades()
    pos = await env.position(trade.id, "tickmill")
    assert is_virtual_pending(pos) and pos.data["trigger_price"] == 73000.0
    assert env.binance.opened[0].entry_type is EntryType.LIMIT and env.binance.opened[0].entry_price == 73000.0

    env.ref.prices["BTC"] = 72990.0                              # Binance touches the level
    env.tickmill.quotes["BTCUSD"] = q(72939.0, 72941.0)
    await env.engine.monitor.run_virtual_once()
    (req,) = env.tickmill.opened
    assert req.entry_type is EntryType.MARKET
    assert req.stop_loss == pytest.approx(72250.0) and req.take_profits[0].price == pytest.approx(74950.0)


@pytest.mark.parametrize("base,market_type,entry,sl,tp", [
    ("EUR", MarketType.FOREX, 1.1620, 1.1600, 1.1660),
    ("XAU", MarketType.CFD, 4100.0, 4085.0, 4140.0),
])
async def test_crypto_route_forex_or_cfd_plan_is_never_shifted(
    env: Env, base: str, market_type: MarketType, entry: float, sl: float, tp: float
) -> None:
    plan = plan_of(base, "USD", Side.LONG, entry=entry, sl=sl, tp=tp, market_type=market_type, **KRIPTO_ROUTE)
    await env.engine.execute_open(plan)
    assert env.tickmill.opened[0].stop_loss == sl and env.tickmill.opened[0].entry_type is EntryType.MARKET

    limit = plan_of(base, "USD", Side.LONG, entry=entry * 0.999, sl=sl * 0.999, tp=tp, market_type=market_type,
                    entry_type=EntryType.LIMIT, **KRIPTO_ROUTE)
    await env.engine.execute_open(limit)                         # a broker limit, not a virtual trigger
    assert env.tickmill.opened[1].entry_type is EntryType.LIMIT
    assert env.tickmill.opened[1].entry_price == entry * 0.999 and env.tickmill.opened[1].stop_loss == sl * 0.999
    assert env.ref.calls == []
    assert not any(is_virtual_pending(p) for _tid, p in await env.db.active_broker_positions())


@pytest.mark.parametrize("routes", [None, [TopicRoute(name="hepsi", title_contains=["İşlemleri"], kind="any")]])
async def test_plan_without_route_or_of_an_any_route_keeps_todays_rule(db: Database, routes: Any) -> None:
    env = await plain_env(db, routes=routes)
    try:
        async def run(action: ParsedAction) -> None:
            if not routes:
                plan = await env.plan(action, None)
                assert plan.route is None
            else:
                ev = await env.engine.evaluate(action.model_copy(update={"used_message_ids": [1]}), CHAT,
                                               route=env.cfg.telegram.topics[0])
                assert ev is not None and isinstance(ev.plan, TradePlan) and ev.plan.route == "hepsi"
                plan = ev.plan
            await env.engine.execute_open(plan)

        broker = env.brokers["tickmill"]
        await run(btc(None))                                     # untyped: crypto as before -> shifted
        assert broker.opened[-1].stop_loss == pytest.approx(73250.0)
        await run(btc(MarketType.PERP).model_copy(update={"stop_loss": 73200.0}))
        assert broker.opened[-1].stop_loss == pytest.approx(73150.0)
        calls = len(env.ref.calls)
        await run(btc(MarketType.SPOT).model_copy(update={"stop_loss": 73100.0}))
        assert broker.opened[-1].stop_loss == 73100.0 and len(env.ref.calls) == calls      # as before: no offset
        await run(btc(MarketType.SPOT, EntryType.LIMIT))         # as before: a broker limit
        assert broker.opened[-1].entry_type is EntryType.LIMIT and broker.opened[-1].entry_price == 73000.0
        n = len(broker.opened)
        await run(btc(None, EntryType.LIMIT).model_copy(update={"entry_price": 72900.0}))  # as before: virtual trigger
        assert len(broker.opened) == n
        assert any(is_virtual_pending(p) for _tid, p in await db.active_broker_positions())
    finally:
        await env.engine.stop()


def test_virtual_entry_wanted_keeps_its_signature_and_takes_the_route_kind() -> None:
    broker = FakeBroker("tickmill")
    broker.config.reference_exchange = "binanceusdm"
    broker.config.entry_trigger = "reference_touch"              # type: ignore[assignment]

    def limit(market_type: MarketType | None) -> TradePlan:
        return plan_of("BTC", "USDT", Side.LONG, entry_type=EntryType.LIMIT, market_type=market_type)

    # two-argument call (no route): exactly the old rule
    assert virtual_entry_wanted(broker, limit(None)) and virtual_entry_wanted(broker, limit(MarketType.PERP))
    assert not virtual_entry_wanted(broker, limit(MarketType.SPOT))
    assert not virtual_entry_wanted(broker, limit(MarketType.FOREX))
    # route kind "fx": never; "crypto": unless the plan itself is forex / CFD
    for mt in (None, MarketType.PERP, MarketType.SPOT, MarketType.FOREX, MarketType.CFD):
        assert not virtual_entry_wanted(broker, limit(mt), "fx")
        assert virtual_entry_wanted(broker, limit(mt), "crypto") is (mt not in (MarketType.FOREX, MarketType.CFD))
        assert virtual_entry_wanted(broker, limit(mt), None) is (mt in (None, MarketType.PERP))
    market = plan_of("BTC", "USDT", Side.LONG, entry_type=EntryType.MARKET)
    assert not virtual_entry_wanted(broker, market, "crypto")    # only LIMIT entries wait for a touch
    broker.config.entry_trigger = "broker_limit"                 # type: ignore[assignment]
    assert not virtual_entry_wanted(broker, limit(None), "crypto")


async def test_preview_follows_the_same_rule(env: Env) -> None:
    lines = await env.engine.preview_open(await env.plan(usdjpy_untyped(), "fx"))
    tickmill = next(line for line in lines if line.startswith("tickmill:"))
    assert "referans" not in tickmill and "uygulandı" not in tickmill and "USDJPY miktar" in tickmill
    assert env.ref.calls == [] and env.tickmill.sized[-1]["stop"] == 154.00

    lines = await env.engine.preview_open(await env.plan(btc(MarketType.SPOT), "kripto"))
    tickmill = next(line for line in lines if line.startswith("tickmill:"))
    assert "fark -50.0 uygulandı" in tickmill and env.tickmill.sized[-1]["stop"] == pytest.approx(73250.0)
    assert env.tickmill.opened == []


# =========================================================================== #
# TASK 2 - fx-topic normalisation in the parser
# =========================================================================== #
def fx_ctx() -> ParseContext:
    return ParseContext(now=NOW, topic_title=TITLES[FX], topic_kind="fx")


def parsed(base: str | None, quote: str | None = None, symbol: str | None = None, **kw: Any) -> ParsedAction:
    data: dict[str, Any] = dict(action=ActionType.OPEN, symbol=symbol, base_asset=base, quote_asset=quote,
                                side=Side.LONG, entry_price=1.0, stop_loss=0.9, confidence=0.95, used_message_ids=[1])
    data.update(kw)
    return ParsedAction(**data)


@pytest.mark.parametrize("base,quote,symbol,expected", [
    ("USD", "JPY", None, MarketType.FOREX),
    ("USD", "CHF", "USDCHF", MarketType.FOREX),
    ("USD", "CAD", None, MarketType.FOREX),
    ("EUR", "JPY", None, MarketType.FOREX),          # crosses
    ("GBP", "JPY", "GBPJPY", MarketType.FOREX),
    ("CHF", "JPY", None, MarketType.FOREX),
    ("JPY", None, None, MarketType.FOREX),
    ("EUR", None, None, MarketType.FOREX),
    ("XAU", None, None, MarketType.CFD),
    ("XAG", "USD", "XAGUSD", MarketType.CFD),
    ("GOLD", None, None, MarketType.CFD),            # not canonical: still a CFD
    ("NAS100", None, None, MarketType.CFD),
    ("US30", "USD", None, MarketType.CFD),
    ("GER40", None, "DAX", MarketType.CFD),
    ("USOIL", None, None, MarketType.CFD),
    (None, None, "USDJPY", MarketType.FOREX),        # only the symbol was filled
    (None, None, "NAS100", MarketType.CFD),
])
def test_fx_topic_fills_the_market_type_for_every_base(
    base: Any, quote: Any, symbol: Any, expected: MarketType
) -> None:
    a = _apply_topic_kind(parsed(base, quote, symbol), fx_ctx())
    assert a.market_type is expected
    assert a.ambiguities == [] and a.confidence == 0.95
    if base not in ("EUR", "XAU"):
        assert a.quote_asset == quote                # nothing invented for the quote
    assert a.base_asset == base and a.symbol == symbol


def test_fx_topic_keeps_what_the_model_said_and_only_touches_new_signals() -> None:
    # a market type the model stated is never overwritten
    assert _apply_topic_kind(parsed("USD", "JPY", market_type=MarketType.CFD), fx_ctx()).market_type is MarketType.CFD
    assert _apply_topic_kind(parsed("XAU", market_type=MarketType.FOREX), fx_ctx()).market_type is MarketType.FOREX
    # crypto on an fx topic: the existing ambiguity, nothing filled
    for mt in (MarketType.PERP, MarketType.SPOT):
        a = _apply_topic_kind(parsed("ETH", "USDT", "ETHUSDT.P", market_type=mt), fx_ctx())
        assert a.market_type is mt and a.confidence <= 0.6 and any("Forex konusundaki" in x for x in a.ambiguities)
    # a perpetual written in the symbol is not relabelled either (as before: left alone)
    a = _apply_topic_kind(parsed("ETH", "USDT", "ETHUSDT.P"), fx_ctx())
    assert a.market_type is None and a.ambiguities == []
    # an incomplete signal is normalised like an open one; management and "none" are not touched
    incomplete = _apply_topic_kind(parsed("USD", "JPY", action=ActionType.INCOMPLETE), fx_ctx())
    assert incomplete.market_type is MarketType.FOREX
    for kind in (ActionType.MOVE_SL, ActionType.CLOSE, ActionType.NONE):
        assert _apply_topic_kind(parsed("USD", "JPY", action=kind), fx_ctx()).market_type is None
    # nothing to go on: nothing filled
    assert _apply_topic_kind(parsed(None), fx_ctx()).market_type is None
    # no topic kind: nothing assumed; crypto topic: unchanged (USDT perpetual)
    assert _apply_topic_kind(parsed("USD", "JPY"), ParseContext(now=NOW)).market_type is None
    a = _apply_topic_kind(parsed("ETH"), ParseContext(now=NOW, topic_kind="crypto"))
    assert (a.quote_asset, a.market_type) == ("USDT", MarketType.PERP)


@pytest.mark.parametrize("base,symbol,quote,market_type", [
    ("EUR", "EURGBP", "GBP", MarketType.FOREX),      # the reproduced defect: was rewritten to EUR/USD
    ("EUR", "eurgbp", "GBP", MarketType.FOREX),
    ("EUR", "EUR/GBP", "GBP", MarketType.FOREX),
    ("EUR", "OANDA:EURGBP", "GBP", MarketType.FOREX),
    ("EUR", "EURJPY", "JPY", MarketType.FOREX),
    ("GBP", "GBPJPY", "JPY", MarketType.FOREX),
    ("AUD", "AUDNZD", "NZD", MarketType.FOREX),
    ("NZD", "NZDCAD", "CAD", MarketType.FOREX),
    ("XAU", "XAUEUR", "EUR", MarketType.CFD),
    # "EUR" alone is still EURUSD
    ("EUR", None, "USD", MarketType.FOREX),
    ("EUR", "EUR", "USD", MarketType.FOREX),
    ("EUR", "EURUSD", "USD", MarketType.FOREX),
    ("GBP", "GBP", "USD", MarketType.FOREX),
    ("XAU", "GOLD", "USD", MarketType.CFD),
    ("XAG", None, "USD", MarketType.CFD),
    # a symbol that is not a fiat pair of this base does not change the default
    ("EUR", "EURUSDT", "USD", MarketType.FOREX),
    ("GBP", "EURGBP", "USD", MarketType.FOREX),
])
def test_fx_topic_quote_comes_from_the_symbol_when_it_names_a_fiat_pair(
    base: str, symbol: str | None, quote: str, market_type: MarketType
) -> None:
    a = _apply_topic_kind(parsed(base, None, symbol), fx_ctx())
    assert (a.base_asset, a.quote_asset, a.market_type) == (base, quote, market_type)
    assert a.ambiguities == [] and a.confidence == 0.95


def test_fx_topic_explicit_quote_is_kept() -> None:
    a = _apply_topic_kind(parsed("EUR", "GBP", "EURGBP"), fx_ctx())
    assert (a.quote_asset, a.market_type) == ("GBP", MarketType.FOREX)
    a = _apply_topic_kind(parsed("EUR", "USD", "EURUSD"), fx_ctx())
    assert (a.quote_asset, a.market_type) == ("USD", MarketType.FOREX)


async def test_parser_end_to_end_fx_topic_cross_and_untyped_pair() -> None:
    eurgbp = wire_action(action="open", symbol="EURGBP", base_asset="EUR", side="long", entry_type="limit",
                         entry_price=0.8820, stop_loss=0.8800, used_message_ids=[701], confidence=0.95)
    usdjpy = wire_action(action="open", base_asset="USD", quote_asset="JPY", side="long", entry_price=154.2,
                         stop_loss=154.0, used_message_ids=[702], confidence=0.95)
    parser, _ = parser_with(make_response({"actions": [eurgbp, usdjpy]}))
    assert isinstance(parser, ClaudeSignalParser)
    a, b = (await parser.parse([msg(701, "EURGBP LONG 1R Limit Emir"), msg(702, "USDJPY LONG 1R")], fx_ctx())).actions
    assert (a.base_asset, a.quote_asset, a.market_type) == ("EUR", "GBP", MarketType.FOREX)
    assert (b.base_asset, b.quote_asset, b.market_type) == ("USD", "JPY", MarketType.FOREX)
    assert a.ambiguities == [] and b.ambiguities == []


# =========================================================================== #
# TASK 3 - no_hedge / opposite trade: the same INSTRUMENT, not only the same base currency
# =========================================================================== #
USDJPY_LONG = ("USDJPY", Side.LONG, 154.20, 154.00, 154.60)
USDCAD_SHORT = ("USDCAD", Side.SHORT, 1.4020, 1.4040, 1.3980)
EURUSD_LONG = ("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660)
EURUSD_SHORT = ("EURUSD", Side.SHORT, 1.1620, 1.1640, 1.1580)
EURJPY_SHORT = ("EURJPY", Side.SHORT, 178.90, 179.10, 178.50)
GBPUSD_SHORT = ("GBPUSD", Side.SHORT, 1.3150, 1.3170, 1.3110)
GBPJPY_LONG = ("GBPJPY", Side.LONG, 202.50, 202.30, 202.90)


@pytest.mark.parametrize("first,second", [
    (USDJPY_LONG, USDCAD_SHORT), (EURUSD_LONG, EURJPY_SHORT), (GBPUSD_SHORT, GBPJPY_LONG),
])
async def test_no_hedge_other_pair_sharing_the_base_currency_opens_on_both_brokers(
    env: Env, first: tuple, second: tuple
) -> None:
    await env.say(fx_pair(*first), FX, f"{first[0]} {first[1].value} 1R")
    await env.say(fx_pair(*second), FX, f"{second[0]} {second[1].value} 1R")

    for broker in (env.fx, env.tickmill):
        assert [(r.symbol.broker_symbol, r.plan.side) for r in broker.opened] == [
            (first[0], first[1]), (second[0], second[1])], broker.name
    assert not any("hedging" in t for t in env.texts())
    assert env.notifier.confirmations == []
    assert [t.status for t in await env.trades()] == [TradeStatus.OPEN, TradeStatus.OPEN]


async def test_no_hedge_same_pair_opposite_is_still_refused_on_both_with_the_same_text(env: Env) -> None:
    await env.say(fx_pair(*EURUSD_LONG), FX, "EURUSD LONG 1R")
    env.notifier.notifications.clear()
    await env.say(fx_pair(*EURUSD_SHORT), FX, "EURUSD SHORT 1R")

    assert len(env.fx.opened) == 1 and len(env.tickmill.opened) == 1          # only the long
    assert env.notifier.confirmations == []
    (note,) = env.texts(Severity.WARNING)
    assert note == (
        "ℹ️ <b>#2 EUR SHORT açılmadı</b> (hedging yasağı)\n"
        "• fx: EUR: açık LONG var; ters yönlü SHORT sinyali hedging yasağı nedeniyle açılmadı\n"
        "• tickmill: EUR: açık LONG var; ters yönlü SHORT sinyali hedging yasağı nedeniyle açılmadı")
    assert [t.status for t in await env.trades()] == [TradeStatus.OPEN, TradeStatus.FAILED]


async def test_no_hedge_decides_by_the_broker_symbol_even_when_a_plan_names_no_quote(env: Env) -> None:
    # "EUR long" read without a quote is EURUSD on the broker; an EURJPY short is another instrument there.
    # (The two plans alone could not tell: a missing quote matches any quote.)
    await env.say(signal("EUR", None, Side.LONG, 1.1620, 1.1600, 1.1660), FX, "EUR LONG 1R")
    assert [r.symbol.broker_symbol for r in env.fx.opened] == ["EURUSD"]
    await env.say(fx_pair(*EURJPY_SHORT), FX, "EURJPY SHORT 1R")
    for broker in (env.fx, env.tickmill):
        assert [r.symbol.broker_symbol for r in broker.opened] == ["EURUSD", "EURJPY"], broker.name
    await env.say(fx_pair(*EURUSD_SHORT), FX, "EURUSD SHORT 1R")             # the same instrument: refused
    assert len(env.fx.opened) == 2 and len(env.tickmill.opened) == 2
    assert sum("hedging yasağı" in t for t in env.texts()) == 1 and env.notifier.confirmations == []


async def test_no_hedge_pending_limit_of_the_same_pair_still_blocks_the_opposite_signal(env: Env) -> None:
    limit = dict(entry_type=EntryType.LIMIT)
    await env.say(fx_pair("EURUSD", Side.LONG, 1.1600, 1.1580, 1.1640, **limit), FX, "EUR LONG limit emir")
    await env.say(fx_pair("EURUSD", Side.SHORT, 1.1640, 1.1660, 1.1600, **limit), FX, "EUR SHORT limit emir")
    assert len(env.fx.opened) == 1 and len(env.tickmill.opened) == 1
    assert sum("bekleyen limit emri LONG var" in t for t in env.texts()) == 1   # one notice naming both brokers


async def test_crypto_same_coin_opposite_is_still_asked_and_refused_on_the_no_hedge_broker(env: Env) -> None:
    await env.say(btc(MarketType.PERP), KRIPTO, "btc 1R long")
    assert [len(b.opened) for b in (env.binance, env.breakout, env.tickmill)] == [1, 1, 1]

    await env.say(btc(MarketType.PERP, side=Side.SHORT), KRIPTO, "btc 1R short")
    (conf,) = env.notifier.confirmations                         # binance is one-way: the owner is asked, as before
    assert [i.code for i in conf.issues] == ["opposite_trade_open"]
    assert [len(b.opened) for b in (env.binance, env.breakout, env.tickmill)] == [1, 1, 1]

    result = await env.engine.handle_confirmation(conf.id, True)
    assert "tickmill: BTC: açık LONG var; ters yönlü SHORT sinyali hedging yasağı nedeniyle açılmadı" in result
    assert [len(b.opened) for b in (env.binance, env.breakout, env.tickmill)] == [2, 2, 1]


async def test_single_no_hedge_broker_without_routes_refuses_the_opposite_coin_signal(db: Database) -> None:
    env = await plain_env(db, no_hedge=True, trigger="broker_limit")
    try:
        await env.engine.execute_open(await env.plan(btc(MarketType.PERP), None))
        short = await env.plan(btc(MarketType.PERP, side=Side.SHORT), None)
        ev = await env.engine.evaluate(btc(MarketType.PERP, side=Side.SHORT).model_copy(
            update={"used_message_ids": [9]}), CHAT)
        assert ev is not None and ev.issues == []                # every target broker refuses hedging itself
        result = await env.engine.execute_open(short)
        assert len(env.brokers["tickmill"].opened) == 1
        assert "BTC: açık LONG var; ters yönlü SHORT sinyali hedging yasağı nedeniyle açılmadı" in result
    finally:
        await env.engine.stop()


async def _stored(env: Env, plan: TradePlan, broker: str, broker_symbol: str, *, virtual: bool = False) -> TradeRecord:
    """An active trade of ``plan`` with one position on ``broker`` (``virtual``: a waiting virtual entry)."""
    trade = await env.db.create_trade(plan)
    data: dict[str, Any] = {"virtual_trigger": True, "virtual_state": "waiting"} if virtual else {}
    status = PositionStatus.PENDING if virtual else PositionStatus.OPEN
    await env.db.save_broker_position(trade.id, BrokerPosition(
        broker=broker, broker_symbol=broker_symbol, side=plan.side, status=status, quantity=1.0,
        filled_quantity=0.0 if virtual else 1.0, remaining_quantity=0.0 if virtual else 1.0, data=data))
    return await env.db.update_trade(trade.id, status=TradeStatus.PENDING if virtual else TradeStatus.OPEN)


async def test_hedge_conflict_compares_broker_symbols_and_falls_back_to_base_and_quote(env: Env) -> None:
    await _stored(env, plan_of("USD", "JPY", Side.LONG, **FX_ROUTE), "fx", "USDJPY")
    await _stored(env, plan_of("USD", "JPY", Side.LONG, **FX_ROUTE), "tickmill", "", virtual=True)

    async def conflict(broker: VenueFake, base: str, quote: str | None, symbol: str | None,
                       route: dict[str, Any] = FX_ROUTE) -> str | None:
        plan = plan_of(base, quote, Side.SHORT, **route)
        return await env.engine._hedge_conflict(broker, plan, await env.db.create_trade(plan), symbol)

    # both sides know their broker symbol: only the same symbol is a hedge
    assert await conflict(env.fx, "USD", "CAD", "USDCAD") is None
    assert await conflict(env.fx, "USD", "JPY", "USDJPY") == (
        "USD: açık LONG var; ters yönlü SHORT sinyali hedging yasağı nedeniyle açılmadı")
    assert await conflict(env.fx, "USD", "JPY", "usdjpy") is not None            # same symbol, other spelling
    # the new plan's symbol is unknown: base AND quote of the two plans decide
    assert await conflict(env.fx, "USD", "CAD", None) is None
    assert await conflict(env.fx, "USD", "JPY", None) is not None
    assert await conflict(env.fx, "USD", None, None) is not None                 # a missing quote matches any quote
    assert await conflict(env.fx, "EUR", "JPY", None) is None
    # the stored position (a waiting virtual entry) has no broker symbol: same fallback
    assert await conflict(env.tickmill, "USD", "CAD", "USDCAD") is None
    text = await conflict(env.tickmill, "USD", "JPY", "USDJPY")
    assert text == ("USD: bekleyen sanal tetikli giriş LONG var; ters yönlü SHORT sinyali hedging yasağı "
                    "nedeniyle açılmadı")
    assert await conflict(env.tickmill, "USD", None, "USDJPY") is not None
    assert await conflict(env.tickmill, "EUR", "JPY", "EURJPY") is None
    # a coin stays one instrument whatever quote the signal names (crypto exactly as before)
    await _stored(env, plan_of("BTC", "USDT", Side.LONG, **KRIPTO_ROUTE), "tickmill", "", virtual=True)
    for quote in ("USDT", "USD", "USDC", "EUR", None):
        assert await conflict(env.tickmill, "BTC", quote, None, KRIPTO_ROUTE) is not None, quote
    assert await conflict(env.tickmill, "ETH", "USDT", None, KRIPTO_ROUTE) is None
    # same direction is never a hedge; a broker without no_hedge never refuses
    long_plan = plan_of("USD", "JPY", Side.LONG, **FX_ROUTE)
    assert await env.engine._hedge_conflict(env.fx, long_plan, await env.db.create_trade(long_plan), "USDJPY") is None
    assert await conflict(env.binance, "USD", "JPY", "USDJPY") is None


async def test_opposite_trade_question_needs_the_same_pair(db: Database) -> None:
    env = await plain_env(db, trigger="broker_limit")            # a broker that may hedge: the owner is asked
    try:
        forex, perp = MarketType.FOREX, MarketType.PERP
        await _stored(env, plan_of("USD", "JPY", Side.LONG, market_type=forex), "tickmill", "USDJPY")
        await _stored(env, plan_of("EUR", "USD", Side.LONG, market_type=forex), "tickmill", "EURUSD")
        await _stored(env, plan_of("GBP", None, Side.LONG, market_type=forex), "tickmill", "GBPUSD")
        await _stored(env, plan_of("BTC", "USDT", Side.LONG, market_type=perp), "tickmill", "BTC/USDT:USDT")
        await _stored(env, plan_of("XAU", "USD", Side.LONG, market_type=MarketType.CFD), "tickmill", "XAUUSD")

        async def asked(base: str, quote: str | None, side: Side = Side.SHORT,
                        market_type: MarketType | None = forex) -> bool:
            issues = await env.engine._trade_issues(plan_of(base, quote, side, market_type=market_type), CHAT)
            return any(i.code == "opposite_trade_open" for i in issues)

        assert not await asked("USD", "CAD")                     # USDCAD short vs USDJPY long: another instrument
        assert await asked("USD", "JPY")
        assert not await asked("EUR", "JPY")                     # EURJPY short vs EURUSD long
        assert await asked("EUR", "USD")
        assert await asked("EUR", None)                          # unknown quote on the new signal: counts
        assert await asked("GBP", "JPY")                         # unknown quote on the open trade: counts
        assert not await asked("USD", "JPY", Side.LONG)          # same direction: never
        assert await asked("XAU", "USDT", market_type=MarketType.CFD)            # a dollar is a dollar: XAUUSD
        assert not await asked("XAU", "EUR", market_type=MarketType.CFD)         # gold in euros is another one
        # crypto exactly as before: the same coin, whatever quote the signal was written with
        for quote in ("USDT", None, "USD", "USDC", "EUR"):
            for market_type in (perp, None):
                assert await asked("BTC", quote, market_type=market_type), (quote, market_type)
        # a plan with neither a route nor a market type is not known to be forex: the old rule (base only)
        assert await asked("USD", "CAD", market_type=None)
    finally:
        await env.engine.stop()


# =========================================================================== #
# TASK 4 - a market order's size is capped relative to the signal's own stop distance
# =========================================================================== #
CAP_WARNING = "Fiyat stopa çok yakın olduğu için miktar sınırlandı"
DISTANCES = [(20.0, 20.0), (12.0, 12.0), (10.0, 10.0), (5.0, 10.0), (2.5, 10.0)]   # live pips -> sizing pips (K = 2)


def eur_market(side: Side, **kw: Any) -> TradePlan:
    """EURUSD MARKET plan of the fx route with a 20 pip stop (long: entry 1.1620 / SL 1.1600; short mirrored).
    Built by hand: when a signal becomes a MARKET plan is the validator's business, not the engine's."""
    data: dict[str, Any] = dict(FX_ROUTE, market_type=MarketType.FOREX)
    data.update(kw)
    if side is Side.LONG:
        return plan_of("EUR", "USD", Side.LONG, entry=1.1620, sl=1.1600, tp=1.1660, **data)
    return plan_of("EUR", "USD", Side.SHORT, entry=1.1600, sl=1.1620, tp=1.1560, **data)


def btc_market() -> TradePlan:
    """BTC long MARKET plan of the crypto route: entry 74000, SL 73300 (700), TP 75500."""
    return plan_of("BTC", "USDT", Side.LONG, entry=74000.0, sl=73300.0, tp=75500.0, market_type=MarketType.PERP,
                   **KRIPTO_ROUTE)


def btc_at(env: Env, binance: float) -> None:
    """Binance (and the reference / oracle) at ``binance``; tickmill's CFD 50 USD below it."""
    for name in ("binance", "breakout"):
        env.brokers[name].quotes["BTC/USDT:USDT"] = q(binance - 1.0, binance + 1.0)
    env.tickmill.quotes["BTCUSD"] = q(binance - 51.0, binance - 49.0)
    env.ref.prices["BTC"] = env.oracle.prices["BTC"] = binance


def set_eurusd(env: Env, side: Side, live_pips: float) -> float:
    """Live EURUSD ``live_pips`` away from the stop on the entry side; returns the price a market order fills at."""
    if side is Side.LONG:
        ask = 1.1600 + live_pips * PIP
        quote = q(ask - 0.00002, ask)
    else:
        bid = 1.1620 - live_pips * PIP
        quote = q(bid, bid + 0.00002)
    env.fx.quotes["EURUSD"] = env.tickmill.quotes["EURUSD"] = quote
    env.oracle.prices["EURUSD"] = (quote.bid + quote.ask) / 2.0
    return quote.entry_price_for(side)


@pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])
@pytest.mark.parametrize("live_pips,sizing_pips", DISTANCES)
async def test_market_size_is_capped_by_the_signals_own_stop_distance(
    env: Env, side: Side, live_pips: float, sizing_pips: float
) -> None:
    live = set_eurusd(env, side, live_pips)
    stop = 1.1600 if side is Side.LONG else 1.1620
    expected = stop + sizing_pips * PIP if side is Side.LONG else stop - sizing_pips * PIP
    plan = eur_market(side)

    result = await env.engine.execute_open(plan)

    capped = sizing_pips != live_pips
    for broker, risk in ((env.fx, 100.0), (env.tickmill, 45.0)):
        (sized,) = broker.sized
        assert sized["entry"] == pytest.approx(expected, abs=1e-9), broker.name
        assert sized["stop"] == stop and sized["risk_amount"] == risk
        (req,) = broker.opened                                   # the order itself is unchanged
        assert req.entry_type is EntryType.MARKET and req.entry_price is None and req.stop_loss == stop
        assert [tp.price for tp in req.take_profits] == [tp.price for tp in plan.take_profits]
        assert req.quantity == pytest.approx(risk / (sizing_pips * PIP), rel=1e-3)
        line = next(x for x in result.splitlines() if x.startswith(f"• {broker.name}:"))
        assert (CAP_WARNING in line) is capped, line
        if capped:                                               # what is really at risk: live distance x size
            real = risk * live_pips / sizing_pips
            assert f"gerçek risk ~{real:,.2f} USD (hedefin altında)" in line
    if not capped:
        assert env.fx.sized[0]["entry"] == pytest.approx(live, abs=1e-12)        # exactly the live price, as before


async def test_capped_market_signal_end_to_end_after_the_owner_approves(env: Env) -> None:
    btc_at(env, 73500.0)                                         # 200 above the stop of a 700 USD stop distance
    await env.say(btc(MarketType.PERP), KRIPTO, "btc 1R long")
    (conf,) = env.notifier.confirmations                         # an explicit market entry that far away is asked
    assert "market_far_from_entry" in [i.code for i in conf.issues] and env.binance.opened == []

    result = await env.engine.handle_confirmation(conf.id, True)
    # binance: 10 USDT over 350 (half the signal's distance), not over the live 201 above the stop
    assert env.binance.sized[0]["entry"] == pytest.approx(73650.0)
    assert env.binance.opened[0].quantity == pytest.approx(10.0 / 350.0, abs=0.001)
    assert env.binance.opened[0].entry_type is EntryType.MARKET and env.binance.opened[0].stop_loss == 73300.0
    line = next(x for x in result.splitlines() if x.startswith("• binance:"))
    # Review 1 (glue_engine, G8): the figure of the quantity really sent (0.028 BTC x 201 = 5.63), no longer that of the
    # unrounded size 10 / 350 BTC (10 x 201 / 350 = 5.74).
    assert CAP_WARNING in line and "gerçek risk ~5.63 USDT (hedefin altında)" in line


async def test_automatic_crypto_market_signal_within_tolerance_is_capped(env: Env) -> None:
    # ETH long, entry 2232, SL 2222; the price is 0.4% below the entry (inside the 0.5% market tolerance: nothing
    # is asked) and only 1.1 above the stop.
    for name in ("binance", "breakout"):
        env.brokers[name].quotes["ETH/USDT:USDT"] = q(2223.0, 2223.1)
    env.oracle.prices["ETH"] = 2223.05
    env.breakout.unsupported.add("ETH")
    env.tickmill.unsupported.add("ETH")
    eth = signal("ETH", "USDT", Side.LONG, 2232.0, 2222.0, 2262.0, symbol="ETHUSDT.P", market_type=MarketType.PERP)
    await env.say(eth, KRIPTO, "eth 1R long")

    assert env.notifier.confirmations == []
    (sized,) = env.binance.sized
    assert sized["entry"] == pytest.approx(2227.0)               # 5 above the stop, not 1.1
    assert env.binance.opened[0].quantity == pytest.approx(10.0 / 5.0)


async def test_market_plan_without_an_entry_price_is_sized_on_the_live_price_as_before(env: Env) -> None:
    live = set_eurusd(env, Side.LONG, 2.5)
    plan = plan_of("EUR", "USD", Side.LONG, entry=None, sl=1.1600, market_type=MarketType.FOREX, **FX_ROUTE)
    result = await env.engine.execute_open(plan)
    assert env.fx.sized[0]["entry"] == pytest.approx(live, abs=1e-12)
    assert env.tickmill.sized[0]["entry"] == pytest.approx(live, abs=1e-12)
    assert CAP_WARNING not in result and len(env.fx.opened) == 1


async def test_signal_without_an_entry_price_end_to_end_is_sized_on_the_live_price_as_before(env: Env) -> None:
    # "EUR LONG, stop 1.1600" with no entry: the plan carries the market price of that moment, so the stop
    # distance the trader "planned" is the live one. Nothing is limited, however close the stop is.
    live = set_eurusd(env, Side.LONG, 5.0)        # (closer than 0.03% of the price the owner is asked: stop_too_tight)
    await env.say(signal("EUR", "USD", Side.LONG, None, 1.1600, 1.1660, symbol="EURUSD", market_type=MarketType.FOREX,
                         entry_type=None), FX, "EUR LONG 1R stop 1.1600")
    assert env.notifier.confirmations == []
    assert env.fx.sized[0]["entry"] == pytest.approx(live, abs=1e-12)
    assert env.fx.opened[0].entry_type is EntryType.MARKET
    assert not any(CAP_WARNING in t for t in env.texts())


async def test_live_price_farther_from_the_stop_than_the_signal_entry_is_sized_as_before(env: Env) -> None:
    live = set_eurusd(env, Side.LONG, 23.0)                      # 3 pips above the entry
    result = await env.engine.execute_open(eur_market(Side.LONG))
    assert env.fx.sized[0]["entry"] == pytest.approx(live, abs=1e-12) and CAP_WARNING not in result


@pytest.mark.parametrize("live_pips", [45.0, 2.5])                # resting above the market / already marketable
async def test_limit_entry_is_sized_at_its_own_price_as_before(env: Env, live_pips: float) -> None:
    set_eurusd(env, Side.LONG, live_pips)
    result = await env.engine.execute_open(eur_market(Side.LONG, entry_type=EntryType.LIMIT))
    for broker in (env.fx, env.tickmill):
        assert broker.sized[0]["entry"] == 1.1620                # (what a marketable limit fills at is the adapter's)
        assert broker.opened[0].entry_type is EntryType.LIMIT and broker.opened[0].entry_price == 1.1620
    assert CAP_WARNING not in result


async def test_offset_broker_caps_on_the_shifted_signal_entry(env: Env) -> None:
    btc_at(env, 73500.0)                 # entry 74000, SL 73300 (700); Binance 73500, tickmill's CFD 73450

    result = await env.engine.execute_open(btc_market())

    (sized,) = env.tickmill.sized                                # shifted: stop 73250, signal entry 73950
    assert sized["stop"] == pytest.approx(73250.0)
    assert sized["entry"] == pytest.approx(73250.0 + 350.0)      # not 73250 + (74000 - 73250) / 2
    assert env.tickmill.opened[0].stop_loss == pytest.approx(73250.0)
    assert env.binance.sized[0]["entry"] == pytest.approx(73300.0 + 350.0)
    for name in ("binance", "breakout", "tickmill"):
        line = next(x for x in result.splitlines() if x.startswith(f"• {name}:"))
        assert CAP_WARNING in line, line


async def test_virtual_entry_that_fires_is_capped_like_a_market_order(env: Env) -> None:
    await env.say(btc(MarketType.PERP, EntryType.LIMIT), KRIPTO, "btc 1R long limit emir")     # 73000 / SL 72300
    assert env.tickmill.opened == []
    # Binance gaps through the level to 72500 (200 above the stop); tickmill's CFD is 50 lower
    env.ref.prices["BTC"] = 72500.0
    env.tickmill.quotes["BTCUSD"] = q(72449.0, 72451.0)
    await env.engine.monitor.run_virtual_once()
    (sized,) = env.tickmill.sized
    assert sized["stop"] == pytest.approx(72250.0) and sized["entry"] == pytest.approx(72250.0 + 350.0)
    assert any(CAP_WARNING in t for t in env.texts())


async def test_preview_matches_the_capped_size(env: Env) -> None:
    set_eurusd(env, Side.LONG, 2.5)
    plan = eur_market(Side.LONG)
    lines = await env.engine.preview_open(plan)
    assert env.fx.opened == []
    for broker in (env.fx, env.tickmill):
        assert broker.sized[-1]["entry"] == pytest.approx(1.1610, abs=1e-9)
        line = next(x for x in lines if x.startswith(f"{broker.name}:"))
        assert CAP_WARNING in line
    set_eurusd(env, Side.LONG, 20.0)
    lines = await env.engine.preview_open(plan)
    assert env.fx.sized[-1]["entry"] == pytest.approx(1.1620, abs=1e-9)
    assert not any(CAP_WARNING in line for line in lines)


def test_sizing_entry_helper() -> None:
    f = orch.market_sizing_entry
    # (side, live entry, stop, signal entry, K) -> (entry to size with, limited?)
    assert f(Side.LONG, 105.0, 100.0, 120.0, 2.0) == (110.0, True)
    assert f(Side.LONG, 110.0, 100.0, 120.0, 2.0) == (110.0, False)
    assert f(Side.LONG, 119.0, 100.0, 120.0, 2.0) == (119.0, False)
    assert f(Side.LONG, 130.0, 100.0, 120.0, 2.0) == (130.0, False)
    assert f(Side.SHORT, 95.0, 100.0, 80.0, 2.0) == (90.0, True)
    assert f(Side.SHORT, 85.0, 100.0, 80.0, 2.0) == (85.0, False)
    assert f(Side.LONG, 102.0, 100.0, 120.0, 4.0) == (105.0, True)       # K = 4: a quarter of the distance
    assert f(Side.LONG, 105.0, 100.0, 120.0, 1.0) == (120.0, True)       # K = 1: never above the planned size
    assert f(Side.LONG, 105.0, 100.0, 120.0, 0.5) == (120.0, True)       # below 1 counts as 1: the planned size stands
    assert f(Side.LONG, 120.0, 100.0, 120.0, 0.5) == (120.0, False)
    assert f(Side.LONG, 105.0, 100.0, 120.0, "4") == (105.0, False)      # a number written as text is a number
    assert f(Side.LONG, 105.0, 100.0, None, 2.0) == (105.0, False)       # no signal entry: as before
    assert f(Side.LONG, 105.0, 100.0, 120.0) == (110.0, True)            # default K = 2
    for unusable in (0, -1.0, None, float("nan"), "x"):                  # never switches the cap off
        assert f(Side.LONG, 105.0, 100.0, 120.0, unusable) == (110.0, True)


async def test_size_multiple_from_the_risk_config(db: Database) -> None:
    assert RiskConfig().max_size_multiple == 2.0          # the config field the engine reads, and its default
    env = await prod_env(db)
    try:
        env.cfg.risk.max_size_multiple = 4.0
        set_eurusd(env, Side.LONG, 2.5)
        await env.engine.execute_open(eur_market(Side.LONG))
        assert env.fx.sized[0]["entry"] == pytest.approx(1.1605, abs=1e-9)       # 20 pips / 4
    finally:
        await env.engine.stop()
