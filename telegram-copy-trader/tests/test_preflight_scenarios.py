"""Pre-flight scenarios on a replica of the owner's production setup, end to end through the REAL code:
config loader, Engine, validator, price oracles and the four broker adapters (ccxt / manual / native MT5 /
MetaApi). Only the venues are fake: a ccxt-like exchange object, a MetaTrader5 module and a MetaApi RPC
connection (the fakes of the adapter test files, with the instruments of the production accounts).

    route "kripto" (kind crypto) -> binance (Binance USDT perpetuals), breakout (manual), tickmill
    route "fx"     (kind fx)     -> fx (prop account, native MT5), tickmill

tickmill is a CFD account whose prices differ from the Binance charts the crypto signals come from: on the
crypto route its levels are shifted by (its mid - Binance mid) and LIMIT entries wait for Binance to touch
the level ("virtual trigger"). On the fx route neither applies.

Scenarios (each asserts what every broker of the route received):
  (a) crypto, BTC market long                 (b) crypto, BTC limit long below the market
  (c) fx, EURUSD explicit market 4 pips away  (d) fx, USDJPY with market_type None
  (e) fx, no_hedge: other pair / same pair    (f) fx, live price 2.5 pips above the stop: size cap
  (g) crypto, 1000PEPE and the manual broker's allowed_assets entry "PEPE"
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from copytrader.brokers.ccxt_broker import CcxtBroker
from copytrader.brokers.manual_broker import ManualBroker
from copytrader.brokers.metaapi_broker import MetaApiBroker
from copytrader.brokers.mt5_broker import MT5, Mt5Broker
from copytrader.config import AppConfig, load_config
from copytrader.engine.monitor import is_virtual_pending
from copytrader.engine.orchestrator import Engine
from copytrader.engine.prices import PriceOracle, ReferencePriceOracle
from copytrader.models import (
    ActionType,
    BrokerPosition,
    EntryType,
    MarketType,
    ParsedAction,
    ParseResult,
    PositionStatus,
    Severity,
    Side,
    StoredMessage,
    TakeProfit,
    TradeRecord,
    TradeStatus,
)
from copytrader.parsing.llm_parser import ClaudeSignalParser
from copytrader.storage.db import Database
from copytrader.telegram import formatting as fmt

from fakes import FakeNotifier, FakeParser
from test_ccxt_broker import FakeExchange
from test_llm_parser import make_response, wire_action
from test_metaapi_broker import FakeConn
from test_mt5_broker import SERVER_TIME, FakeMT5, make_symbol

CHAT, TRADER = -1003782294463, 777001
KRIPTO, FX = 11, 12
TITLES = {KRIPTO: "Kasım Ayı Kripto İşlemleri", FX: "Kasım Ayı Fx İşlemleri"}
PIP = 0.0001

# --------------------------------------------------------------------------- #
# The production replica: the owner's config.yaml with the secrets as ${VAR} placeholders, loaded through
# copytrader.config.load_config with dummy environment variables. Keys the owner does not set are the code
# defaults. (The real "breakout" list has 66 coins; a subset is enough here.)
# --------------------------------------------------------------------------- #
PROD_YAML = """\
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
    allowed_assets: [BTC, ETH, SOL, XRP, DOGE, ADA, AVAX, LINK, BNB, LTC, SUI, TON, DOT, NEAR, APT, ARB, OP, PEPE, WIF, HYPE]
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
DUMMY_ENV = {
    "TG_API_ID": "12345", "TG_API_HASH": "0123456789abcdef", "TG_BOT_TOKEN": "123456:dummy", "TG_OWNER_ID": "42",
    "ANTHROPIC_API_KEY": "sk-ant-dummy", "BINANCE_API_KEY": "dummy-key", "BINANCE_API_SECRET": "dummy-secret",
    "MT5_LOGIN": "5001", "MT5_PASSWORD": "dummy-pw", "MT5_SERVER": "Dummy-Server",
    "METAAPI_TOKEN": "dummy-token", "METAAPI_ACCOUNT_ID": "dummy-account",
}


def load_prod_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AppConfig:
    """The replica as the bot would load it: YAML file + environment variables (dummy secrets)."""
    for name, value in DUMMY_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.chdir(tmp_path)                    # load_config also reads a ".env" of the current directory
    path = tmp_path / "config.yaml"
    path.write_text(PROD_YAML, encoding="utf-8")
    cfg = load_config(path)
    cfg.data_dir = str(tmp_path / "data")
    cfg.monitor_interval_sec = 3600.0
    return cfg


# --------------------------------------------------------------------------- #
# Fake venues (illustrative prices, not market data)
# --------------------------------------------------------------------------- #
# MetaTrader instruments: name -> (base, quote, digits, contract size, bid, ask)
MT_SYMBOLS: dict[str, tuple[str, str, int, float, float, float]] = {
    "EURUSD": ("EUR", "USD", 5, 100_000.0, 1.16195, 1.16205),
    "GBPUSD": ("GBP", "USD", 5, 100_000.0, 1.31495, 1.31505),
    "USDJPY": ("USD", "JPY", 3, 100_000.0, 154.195, 154.205),
    "USDCHF": ("USD", "CHF", 5, 100_000.0, 0.80495, 0.80505),
    "USDCAD": ("USD", "CAD", 5, 100_000.0, 1.40195, 1.40205),
    "AUDUSD": ("AUD", "USD", 5, 100_000.0, 0.65195, 0.65205),
    "NZDUSD": ("NZD", "USD", 5, 100_000.0, 0.56495, 0.56505),
    "EURJPY": ("EUR", "JPY", 3, 100_000.0, 178.890, 178.910),
    "XAUUSD": ("XAU", "USD", 2, 100.0, 4099.90, 4100.10),
    "XAGUSD": ("XAG", "USD", 3, 5_000.0, 50.790, 50.810),
    "BTCUSD": ("BTC", "USD", 2, 1.0, 73949.0, 73951.0),          # a CFD: 50 USD below the Binance perpetual
    "ETHUSD": ("ETH", "USD", 2, 1.0, 2230.9, 2231.1),
}
USD_PER = {"USD": 1.0, "JPY": 1 / 154.2, "CHF": 1 / 0.805, "CAD": 1 / 1.402, "EUR": 1.162, "GBP": 1.315,
           "AUD": 0.652, "NZD": 0.565}
# The eleven symbols known to be listed on the friend's Tickmill account.
TICKMILL_LISTED = ("EURUSD", "GBPUSD", "USDCHF", "USDJPY", "AUDUSD", "NZDUSD", "USDCAD", "BTCUSD", "ETHUSD",
                   "XAUUSD", "XAGUSD")


def _swap(base: str, *, amount: float, price: float, min_qty: float, min_cost: float) -> dict[str, Any]:
    """A Binance USDT perpetual as ccxt describes it."""
    return {
        "id": f"{base}USDT", "symbol": f"{base}/USDT:USDT", "base": base, "quote": "USDT", "settle": "USDT",
        "type": "swap", "swap": True, "spot": False, "linear": True, "active": True, "contractSize": 1.0,
        "taker": 0.0005, "precision": {"amount": amount, "price": price},
        "limits": {"amount": {"min": min_qty, "max": 1_000_000.0}, "cost": {"min": min_cost},
                   "leverage": {"min": None, "max": None}, "market": {"max": 1_000_000.0}},
    }


class Binance(FakeExchange):
    """ccxt-like binanceusdm with per-symbol prices. One object plays the three Binance roles of production:
    the "binance" account, the public price source (manual broker, validation) and the reference exchange."""

    def __init__(self) -> None:
        super().__init__("binanceusdm")
        self.markets = {
            "BTC/USDT:USDT": _swap("BTC", amount=0.001, price=0.1, min_qty=0.001, min_cost=100.0),
            "ETH/USDT:USDT": _swap("ETH", amount=0.001, price=0.01, min_qty=0.001, min_cost=20.0),
            "1000PEPE/USDT:USDT": _swap("1000PEPE", amount=1.0, price=0.0000001, min_qty=1.0, min_cost=5.0),
        }
        self.tickers: dict[str, dict[str, float]] = {}
        self.set_price("BTC", 73999.0, 74001.0)
        self.set_price("ETH", 2231.9, 2232.1)
        self.set_price("1000PEPE", 0.011999, 0.012000)
        self.balance = {"USDT": {"total": 46.90, "free": 46.90}, "info": {}}

    def set_price(self, base: str, bid: float, ask: float) -> None:
        self.tickers[f"{base}/USDT:USDT"] = {"bid": bid, "ask": ask, "last": ask}

    async def fetch_ticker(self, symbol: str) -> dict[str, Any]:
        self._check("fetch_ticker", (symbol,))
        if symbol not in self.tickers:
            from ccxt.base import errors as cx
            raise cx.BadSymbol(f"binanceusdm does not have market symbol {symbol}")
        return {"symbol": symbol, **self.tickers[symbol]}

    async def fetch_order_book(self, symbol: str, limit: int | None = None) -> dict[str, Any]:
        self._check("fetch_order_book", (symbol,))
        t = self.tickers[symbol]
        return {"bids": [[t["bid"], 1]], "asks": [[t["ask"], 1]]}

    async def fetch_ohlcv(self, symbol: str, timeframe: str = "1m", since: int | None = None,
                          limit: int | None = None) -> list[list[float]]:
        return []

    async def create_order(self, symbol: str, type: str, side: str, amount: float,
                           price: float | None = None, params: dict | None = None) -> dict[str, Any]:
        self.ticker = dict(self.tickers[symbol])           # FakeExchange fills market orders from self.ticker
        return await super().create_order(symbol, type, side, amount, price, params)

    def entry_orders(self) -> list[dict[str, Any]]:
        return [o for o in self.created() if not o.get("triggerPrice")]

    def trigger_orders(self, key: str) -> list[dict[str, Any]]:
        return [o for o in self.created() if o.get(key)]


class FxTerminal(FakeMT5):
    """MetaTrader5 module of the prop account "fx": 10,000 USD, hedging; profit converted to USD like a terminal."""

    def __init__(self, names: tuple[str, ...] = tuple(MT_SYMBOLS)) -> None:
        super().__init__()
        self.account.login = int(DUMMY_ENV["MT5_LOGIN"])
        self.account.equity = self.account.balance = self.account.margin_free = 10_000.0
        self.symbols, self.ticks = {}, {}
        for name in names:
            base, quote, digits, contract, bid, ask = MT_SYMBOLS[name]
            point = 10.0 ** -digits
            self.symbols[name] = make_symbol(
                name, digits=digits, point=point, trade_tick_size=point, trade_contract_size=contract,
                filling_mode=MT5.SYMBOL_FILLING_IOC, currency_profit=quote, currency_base=base, currency_margin=base,
                volume_max=100.0)
            self.set_price(name, bid, ask)
        self.set_price("BTCUSD", 73990.0, 74010.0)

    def set_price(self, name: str, bid: float, ask: float) -> None:
        self.ticks[name] = SimpleNamespace(bid=bid, ask=ask, last=0.0, time=SERVER_TIME)

    def order_calc_profit(self, action: int, symbol: str, volume: float, price_open: float, price_close: float) -> float:
        self._log("order_calc_profit", action, symbol, volume, price_open, price_close)
        s = self.symbols[symbol]
        sign = 1 if action == MT5.ORDER_TYPE_BUY else -1
        return (price_close - price_open) * volume * s.trade_contract_size * sign * USD_PER[str(s.currency_profit)]

    def order_calc_margin(self, action: int, symbol: str, volume: float, price: float) -> float:
        self._log("order_calc_margin", action, symbol, volume, price)
        s = self.symbols[symbol]
        return volume * s.trade_contract_size * USD_PER.get(str(s.currency_base), price) / 100.0     # 1:100

    def deals_sent(self) -> list[dict[str, Any]]:
        return self.sent_actions(MT5.TRADE_ACTION_DEAL)

    def pendings_sent(self) -> list[dict[str, Any]]:
        return self.sent_actions(MT5.TRADE_ACTION_PENDING)


class Tickmill(FakeConn):
    """MetaApi RPC connection of the friend's Tickmill MT5 account: 108 USD, hedging, leverage 1:1000."""

    def __init__(self, names: tuple[str, ...] = TICKMILL_LISTED) -> None:
        super().__init__()
        self.info.update(balance=108.0, equity=108.0, freeMargin=108.0, broker="Tickmill", leverage=1000)
        self.symbols, self.specs, self.prices = [], {}, {}
        for name in names:
            base, quote, digits, contract, bid, ask = MT_SYMBOLS[name]
            point = 10.0 ** -digits
            self.symbols.append(name)
            self.specs[name] = {
                "symbol": name, "tickSize": point, "point": point, "minVolume": 0.01, "maxVolume": 100.0,
                "volumeStep": 0.01, "contractSize": contract, "digits": digits, "profitCurrency": quote,
                "baseCurrency": base, "marginCurrency": base, "tradeMode": "SYMBOL_TRADE_MODE_FULL", "stopsLevel": 0,
                "allowedExpirationModes": ["SYMBOL_EXPIRATION_GTC", "SYMBOL_EXPIRATION_SPECIFIED"],
                "allowedOrderTypes": ["SYMBOL_ORDER_MARKET", "SYMBOL_ORDER_LIMIT", "SYMBOL_ORDER_STOP"],
            }
            tick_value = point * contract * USD_PER[quote]
            self.prices[name] = {"symbol": name, "bid": bid, "ask": ask, "profitTickValue": tick_value,
                                 "lossTickValue": tick_value}

    def set_price(self, name: str, bid: float, ask: float) -> None:
        self.prices[name].update(bid=bid, ask=ask)

    async def calculate_margin(self, order: dict[str, Any]) -> dict[str, Any]:
        self._rec("calculate_margin", order)
        spec = self.specs[order["symbol"]]
        if spec["baseCurrency"] in USD_PER:                 # currency pairs: 1:1000
            return {"margin": order["volume"] * spec["contractSize"] * USD_PER[spec["baseCurrency"]] / 1000.0}
        return {"margin": order["volume"] * spec["contractSize"] * order["openPrice"] / 100.0}    # CFDs: 1:100

    def orders_sent(self) -> list[tuple[str, tuple[Any, ...], dict[str, Any]]]:
        """Every order request: (method, (symbol, lots[, price]), {"stop_loss", "take_profit", ...})."""
        return [(name, args, kw) for name, args, kw in self.calls if name.startswith("create_")]


class Reference(ReferencePriceOracle):
    """The real reference price oracle (on the fake Binance); every lookup is recorded."""

    def __init__(self, exchange: Any) -> None:
        super().__init__(cache_ttl_sec=0.0, exchanges={"binanceusdm": exchange})
        self.calls: list[tuple[str, str]] = []

    async def mid(self, exchange_id: str, base_asset: str, *, fresh: bool = False) -> float | None:
        self.calls.append((exchange_id, base_asset.upper()))
        return await super().mid(exchange_id, base_asset, fresh=fresh)


class ScriptedModel:
    """Stands in for the Anthropic client: the REAL ClaudeSignalParser reads the scripted wire JSON."""

    def __init__(self) -> None:
        self.queue: list[list[dict[str, Any]]] = []
        self.calls: list[dict[str, Any]] = []
        self.messages = self
        self.beta = SimpleNamespace(messages=self)

    async def create(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        return make_response({"actions": self.queue.pop(0)})


# --------------------------------------------------------------------------- #
# The environment
# --------------------------------------------------------------------------- #
class Prod:
    """The production replica wired like copytrader.app does, on the fake venues."""

    def __init__(self) -> None:
        self.cfg: AppConfig
        self.db: Database
        self.engine: Engine
        self.parser = FakeParser()
        self.notifier = FakeNotifier()
        self.binance = Binance()
        self.terminal: FxTerminal
        self.tickmill: Tickmill
        self.ref: Reference
        self.brokers: dict[str, Any] = {}
        self._mid = 7000

    @classmethod
    async def create(cls, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *,
                     tickmill_symbols: tuple[str, ...] = TICKMILL_LISTED, edit: Any = None,
                     model: ScriptedModel | None = None) -> "Prod":
        """``edit(cfg)``: change the loaded config before the engine is built. ``model``: read the messages with
        the real ClaudeSignalParser on this scripted model instead of the FakeParser."""
        p = cls()
        p.cfg = load_prod_config(tmp_path, monkeypatch)
        if edit is not None:
            edit(p.cfg)
        if model is not None:
            p.parser = ClaudeSignalParser(p.cfg.llm, client=model)       # type: ignore[assignment]
        by = {b.name: b for b in p.cfg.brokers}
        p.terminal, p.tickmill = FxTerminal(), Tickmill(tickmill_symbols)

        async def no_sleep(_seconds: float) -> None:
            return None

        binance = CcxtBroker(by["binance"], exchange=p.binance, sleep=no_sleep)
        breakout = ManualBroker(by["breakout"], p.cfg.data_dir, price_source=p.binance)
        fx = Mt5Broker(by["fx"], mt5_module=p.terminal)
        fx.retry_delay_sec = 0
        tickmill = MetaApiBroker(by["tickmill"], connection=p.tickmill)
        tickmill.retry_delay_sec = 0
        p.brokers = {"binance": binance, "breakout": breakout, "fx": fx, "tickmill": tickmill}
        p.db = Database(":memory:")
        await p.db.open()
        p.ref = Reference(p.binance)
        holder: list[Engine] = []
        oracle = PriceOracle(lambda: holder[0].brokers, p.cfg.price_source_exchange, cache_ttl_sec=0.0,
                             public_exchange=p.binance)
        p.engine = Engine(p.cfg, p.db, p.parser, list(p.brokers.values()), p.notifier, price_oracle=oracle,
                          reference_prices=p.ref)
        holder.append(p.engine)
        await p.engine.start(monitor=False)
        assert p.engine.failed_brokers == {}
        assert [b.name for b in p.engine.brokers] == ["binance", "breakout", "fx", "tickmill"]
        p.notifier.notifications.clear()
        p.ref.calls.clear()
        return p

    async def close(self) -> None:
        await self.engine.stop(grace_sec=1.0)
        await self.db.close()

    # ---- the trader ------------------------------------------------------------
    async def say(self, action: ParsedAction, topic: int, text: str = "sinyal", *, reply_to: int | None = None) -> int:
        """The trader posts ``text`` in ``topic``; the parser reads it as ``action``."""
        self._mid += 1
        mid = self._mid
        self.parser.push(ParseResult(actions=[action.model_copy(update={"used_message_ids": [mid]})], model="fake"))
        return await self._post(mid, topic, text, reply_to)

    async def _post(self, mid: int, topic: int, text: str, reply_to: int | None = None) -> int:
        await self.db.save_message(StoredMessage(
            chat_id=CHAT, message_id=mid, sender_id=TRADER, sender_name="Hoca", date=datetime.now(UTC), text=text,
            reply_to_message_id=reply_to if reply_to is not None else topic, topic_id=topic,
            topic_title=TITLES[topic]))
        await self.engine.on_messages_ready(CHAT, TRADER, topic)
        return mid

    # ---- what happened -----------------------------------------------------------
    async def trades(self) -> list[TradeRecord]:
        return sorted(await self.db.recent_trades(datetime(2000, 1, 1, tzinfo=UTC)), key=lambda t: t.id)

    async def positions(self, trade_id: int) -> dict[str, BrokerPosition]:
        return {p.broker: p for p in await self.db.broker_positions(trade_id)}

    def asked(self) -> list[list[str]]:
        return [[i.code for i in c.issues] for c in self.notifier.confirmations]

    def texts(self, severity: Severity | None = None) -> list[str]:
        return self.notifier.texts(severity)

    def fx_orders(self) -> list[tuple[str, str, float, float, float, float]]:
        """Orders sent to the fx terminal: (kind, symbol, lots, price, sl, tp)."""
        kinds = {(MT5.TRADE_ACTION_DEAL, MT5.ORDER_TYPE_BUY): "BUY", (MT5.TRADE_ACTION_DEAL, MT5.ORDER_TYPE_SELL): "SELL",
                 (MT5.TRADE_ACTION_PENDING, MT5.ORDER_TYPE_BUY_LIMIT): "BUY_LIMIT",
                 (MT5.TRADE_ACTION_PENDING, MT5.ORDER_TYPE_SELL_LIMIT): "SELL_LIMIT"}
        return [(kinds.get((r["action"], r["type"]), f"{r['action']}/{r['type']}"), r["symbol"], r["volume"],
                 r["price"], r["sl"], r["tp"])
                for r in self.terminal.sent if r["action"] in (MT5.TRADE_ACTION_DEAL, MT5.TRADE_ACTION_PENDING)]

    def tickmill_orders(self) -> list[tuple[Any, ...]]:
        """Orders sent to tickmill: (method, symbol, lots[, price], sl, tp)."""
        return [(name.removeprefix("create_").removesuffix("_order"), *args, kw["stop_loss"], kw["take_profit"])
                for name, args, kw in self.tickmill.orders_sent()]


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch) -> None:
    # The fake terminal's ticks carry SERVER_TIME (server = UTC+2); no real waiting for ticks.
    monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: SERVER_TIME - 7200.0))
    monkeypatch.setattr(Mt5Broker, "tick_wait_sec", 0.0)
    # Engine.connect_brokers() sets this module global when a broker uses risk_usd: other test files must not see it.
    monkeypatch.setattr(fmt, "RISK_PER_R_NOTE", fmt.RISK_PER_R_NOTE)


@pytest.fixture
async def prod(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    p = await Prod.create(tmp_path, monkeypatch)
    yield p
    await p.close()


# --------------------------------------------------------------------------- #
# Signals as the parser hands them to the engine
# --------------------------------------------------------------------------- #
def signal(symbol: str | None, base: str, quote: str | None, market_type: MarketType | None, side: Side,
           entry: float | None, sl: float, tp: float, *, entry_type: EntryType | None = None,
           **kw: Any) -> ParsedAction:
    data: dict[str, Any] = dict(
        action=ActionType.OPEN, symbol=symbol, base_asset=base, quote_asset=quote, market_type=market_type,
        side=side, entry_type=entry_type, entry_price=entry, stop_loss=sl, take_profits=[TakeProfit(price=tp)],
        risk_r=1.0, confidence=0.95, summary_tr=f"{symbol or base} {side.value.upper()}")
    data.update(kw)
    return ParsedAction(**data)


def btc(entry_type: EntryType, entry: float, sl: float, tp: float) -> ParsedAction:
    return signal("BTCUSDT.P", "BTC", "USDT", MarketType.PERP, Side.LONG, entry, sl, tp, entry_type=entry_type)


def fx_pair(pair: str, side: Side, entry: float, sl: float, tp: float,
            entry_type: EntryType | None = EntryType.MARKET) -> ParsedAction:
    return signal(pair, pair[:3], pair[3:], MarketType.FOREX, side, entry, sl, tp, entry_type=entry_type)


EURUSD_LONG = ("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660)
EURUSD_SHORT = ("EURUSD", Side.SHORT, 1.1620, 1.1640, 1.1580)
EURJPY_SHORT = ("EURJPY", Side.SHORT, 178.90, 179.10, 178.50)


def set_eurusd(p: Prod, bid: float, ask: float) -> None:
    p.terminal.set_price("EURUSD", bid, ask)
    p.tickmill.set_price("EURUSD", bid, ask)


# =========================================================================== #
# The replica itself
# =========================================================================== #
def test_replica_loads_like_production(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = load_prod_config(tmp_path, monkeypatch)
    assert cfg.dry_run is False
    assert [(r.name, r.kind, r.brokers) for r in cfg.telegram.topics] == [
        ("kripto", "crypto", ["binance", "breakout", "tickmill"]), ("fx", "fx", ["fx", "tickmill"])]
    by = {b.name: b for b in cfg.brokers}
    assert [(b.name, b.type, b.risk_usd, b.no_hedge) for b in cfg.brokers] == [
        ("binance", "ccxt", 10.0, False), ("breakout", "manual", 50.0, False),
        ("fx", "mt5", 100.0, True), ("tickmill", "metaapi", 45.0, True)]
    tm = by["tickmill"]
    assert (tm.optional, tm.reference_exchange, tm.offset_mode, tm.entry_trigger) == (
        True, "binanceusdm", "additive", "reference_touch")
    assert tm.symbol_map == {"BTC": "BTCUSD", "ETH": "ETHUSD"} and "USD" in tm.allowed_assets
    assert (by["fx"].daily_loss_limit_pct, tm.daily_loss_limit_pct) == (3.0, 0.0)
    assert (by["binance"].margin_mode, by["binance"].leverage) == ("cross", 125)
    # the owner sets no risk section and no notifier options: code defaults
    assert cfg.risk.max_size_multiple == 2.0 and cfg.notifier.notify_on_execution is False
    # Optional: COPYTRADER_PROD_REPLICA=<folder with a replica config.yaml> checks the text above against it.
    theirs = Path(os.environ.get("COPYTRADER_PROD_REPLICA") or tmp_path / "none") / "config.yaml"
    if theirs.is_file():
        assert yaml.safe_load(PROD_YAML) == yaml.safe_load(theirs.read_text(encoding="utf-8"))


# =========================================================================== #
# (a) crypto route, BTC market long with SL / TP
# =========================================================================== #
async def test_a_crypto_btc_market_long_opens_on_all_three_and_tickmill_gets_shifted_levels(prod: Prod) -> None:
    # Binance 73999 / 74001, tickmill's CFD 73949 / 73951: 50 USD lower.
    await prod.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO, "btc 1R long")

    assert prod.asked() == []
    (trade,) = await prod.trades()
    assert trade.status is TradeStatus.OPEN and trade.plan.route == "kripto" and trade.note is None
    pos = await prod.positions(trade.id)
    assert sorted(pos) == ["binance", "breakout", "tickmill"]

    # binance: market entry, then the stop and the target as reduce-only trigger orders at the chart levels
    (entry,) = prod.binance.entry_orders()
    assert (entry["symbol"], entry["type"], entry["side"], entry["amount"]) == ("BTC/USDT:USDT", "market", "buy", 0.012)
    assert 0 < entry["amount"] * (74001.0 - 73300.0) <= 10.0                 # 1R = 10 USDT, never more
    (sl,) = prod.binance.trigger_orders("stopLossPrice")
    (tp,) = prod.binance.trigger_orders("takeProfitPrice")
    assert (sl["side"], sl["amount"], sl["triggerPrice"], sl["reduceOnly"]) == ("sell", 0.012, 73300.0, True)
    assert (tp["side"], tp["amount"], tp["triggerPrice"], tp["reduceOnly"]) == ("sell", 0.012, 75500.0, True)
    assert pos["binance"].status is PositionStatus.OPEN and pos["binance"].stop_loss == 73300.0

    # breakout (manual): a message with the quantity for 50 USD of risk, tracked as an open position
    (note,) = [t for t in prod.texts() if "Elle işlem (breakout)" in t]
    assert "0.071 KONTRAT — BTC LONG" in note and "Stop: 73300" in note and "TP: 75500" in note
    assert pos["breakout"].status is PositionStatus.OPEN and pos["breakout"].quantity == 0.071

    # tickmill: the same trade on the CFD, every level moved by (73950 - 74000) = -50
    assert prod.tickmill_orders() == [("market_buy", "BTCUSD", 0.06, 73250.0, 75450.0)]
    assert pos["tickmill"].data["price_offset"] == {"mode": "additive", "value": pytest.approx(-50.0)}
    assert pos["tickmill"].stop_loss == 73250.0 and pos["tickmill"].risk_amount == 45.0
    assert prod.ref.calls == [("binanceusdm", "BTC")]

    assert prod.terminal.sent == []                                          # the fx account is not on this route
    assert prod.texts(Severity.ERROR) == [] and prod.texts(Severity.WARNING) == []


# =========================================================================== #
# (b) crypto route, BTC limit long below the market
# =========================================================================== #
async def test_b_crypto_btc_limit_long_rests_on_binance_and_waits_as_a_virtual_trigger_on_tickmill(prod: Prod) -> None:
    await prod.say(btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0), KRIPTO, "btc 1R long limit emir")

    assert prod.asked() == []
    (trade,) = await prod.trades()
    assert trade.status is TradeStatus.PENDING
    pos = await prod.positions(trade.id)

    (entry,) = prod.binance.created()                                        # only the resting limit order
    assert (entry["type"], entry["side"], entry["amount"], entry["price"], entry["status"]) == (
        "limit", "buy", 0.012, 73000.0, "open")
    assert pos["binance"].status is PositionStatus.PENDING

    (note,) = [t for t in prod.texts() if "Elle işlem (breakout)" in t]
    assert "Giriş: 73000 (LİMİT emir)" in note and pos["breakout"].status is PositionStatus.PENDING

    # tickmill: NO order at the chart price; a virtual entry waits for Binance to touch 73000
    assert prod.tickmill_orders() == []
    virtual = pos["tickmill"]
    assert is_virtual_pending(virtual) and virtual.broker_symbol == "BTCUSD"
    assert virtual.data["trigger_price"] == 73000.0 and virtual.data["reference_exchange"] == "binanceusdm"
    assert (virtual.stop_loss, [tp.price for tp in virtual.take_profits]) == (72300.0, [75000.0])
    assert any("tickmill: sanal tetik kuruldu: Binance fiyatı 73000" in t for t in prod.texts())
    assert prod.ref.calls == []                                              # nothing measured before the touch

    # Binance not there yet: nothing happens
    await prod.engine.monitor.run_virtual_once()
    assert prod.tickmill_orders() == []
    # Binance touches the level: one market order, the offset measured at that moment (72940 - 72990 = -50)
    prod.binance.set_price("BTC", 72989.0, 72991.0)
    prod.tickmill.set_price("BTCUSD", 72939.0, 72941.0)
    await prod.engine.monitor.run_virtual_once()
    assert prod.tickmill_orders() == [("market_buy", "BTCUSD", 0.06, 72250.0, 74950.0)]
    fired = (await prod.positions(trade.id))["tickmill"]
    assert fired.status is PositionStatus.OPEN and fired.data["price_offset"]["value"] == pytest.approx(-50.0)
    await prod.engine.monitor.run_virtual_once()                             # never a second order
    assert len(prod.tickmill_orders()) == 1


# =========================================================================== #
# (c) fx route, EURUSD explicit market, live price 4 pips from the entry
# =========================================================================== #
@pytest.mark.parametrize("bid,ask,fx_lots,tickmill_lots", [
    (1.16235, 1.16245, 0.40, 0.18),          # 4 pips above the entry: 24.5 pips to the stop
    (1.16155, 1.16165, 0.60, 0.27),          # 4 pips below the entry: 16.5 pips to the stop
])
async def test_c_fx_eurusd_explicit_market_four_pips_away_opens_on_both_without_a_question(
    prod: Prod, bid: float, ask: float, fx_lots: float, tickmill_lots: float
) -> None:
    set_eurusd(prod, bid, ask)
    await prod.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")

    assert prod.asked() == []                                                # 0.034% away: inside 0.25R, no question
    (trade,) = await prod.trades()
    assert trade.status is TradeStatus.OPEN and trade.plan.entry_type is EntryType.MARKET
    # both accounts: a market order at the live price with the chart's own stop and target, sized on the live price
    assert prod.fx_orders() == [("BUY", "EURUSD", fx_lots, ask, 1.16, 1.166)]
    assert prod.tickmill_orders() == [("market_buy", "EURUSD", tickmill_lots, 1.16, 1.166)]
    assert fx_lots * (ask - 1.16) * 100_000.0 <= 100.0 and tickmill_lots * (ask - 1.16) * 100_000.0 <= 45.0
    # tickmill is NOT shifted here and Binance is never asked (neither as reference nor for a price)
    pos = await prod.positions(trade.id)
    assert "price_offset" not in pos["tickmill"].data and pos["tickmill"].stop_loss == 1.16
    assert prod.ref.calls == []
    assert [c for c in prod.binance.calls if c[0] in ("fetch_ticker", "fetch_order_book", "create_order")] == []
    assert prod.texts() == []                                                # nothing to report


# =========================================================================== #
# (d) fx route, USDJPY with market_type None
# =========================================================================== #
def usdjpy_untyped(entry_type: EntryType | None, entry: float, sl: float, tp: float) -> ParsedAction:
    """USDJPY as the model may leave it: base and quote, no symbol, market_type null."""
    return signal(None, "USD", "JPY", None, Side.LONG, entry, sl, tp, entry_type=entry_type)


@pytest.mark.parametrize("entry_type", [EntryType.MARKET, None])
async def test_d_fx_usdjpy_without_a_market_type_opens_on_both_and_never_asks_the_reference(
    prod: Prod, entry_type: EntryType | None
) -> None:
    await prod.say(usdjpy_untyped(entry_type, 154.20, 154.00, 154.60), FX, "USDJPY LONG 1R")

    assert prod.asked() == []
    (trade,) = await prod.trades()
    assert trade.plan.market_type is None and trade.plan.route == "fx" and trade.status is TradeStatus.OPEN
    # 20.5 pips to the stop = 132.94 USD per lot at 154.2 yen per dollar
    assert prod.fx_orders() == [("BUY", "USDJPY", 0.75, 154.205, 154.0, 154.6)]
    assert prod.tickmill_orders() == [("market_buy", "USDJPY", 0.33, 154.0, 154.6)]
    pos = await prod.positions(trade.id)
    assert "price_offset" not in pos["tickmill"].data
    assert prod.ref.calls == [] and not any("referans" in t for t in prod.texts())
    assert [c for c in prod.binance.calls if c[0] == "create_order"] == []


async def test_d_fx_usdjpy_read_by_the_real_parser_is_typed_forex_by_the_topic_and_opens_on_both(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The same signal through the real ClaudeSignalParser: the model leaves the market type and the symbol null.
    model = ScriptedModel()
    prod = await Prod.create(tmp_path, monkeypatch, model=model)
    try:
        model.queue.append([wire_action(
            action="open", base_asset="USD", quote_asset="JPY", side="long", entry_type="market",
            entry_price=154.20, stop_loss=154.00, take_profits=[{"price": 154.60, "close_fraction": 1.0}],
            risk_r=1.0, confidence=0.95, summary_tr="USDJPY LONG, giriş 154.20, SL 154.00, TP 154.60, risk 1R")])
        await prod._post(8001, FX, "USDJPY LONG 1R")

        text = "\n".join(b["text"] for b in model.calls[0]["messages"][0]["content"] if b["type"] == "text")
        assert "Konu: Kasım Ayı Fx İşlemleri (tür: fx)" in text          # what the model was told
        (trade,) = await prod.trades()
        assert trade.plan.market_type is MarketType.FOREX and trade.plan.quote_asset == "JPY"
        assert prod.asked() == []
        assert prod.fx_orders() == [("BUY", "USDJPY", 0.75, 154.205, 154.0, 154.6)]
        assert prod.tickmill_orders() == [("market_buy", "USDJPY", 0.33, 154.0, 154.6)]
        assert prod.ref.calls == []
    finally:
        await prod.close()


async def test_d_fx_usdjpy_limit_without_a_market_type_is_a_broker_limit_not_a_virtual_trigger(prod: Prod) -> None:
    await prod.say(usdjpy_untyped(EntryType.LIMIT, 154.05, 153.85, 154.45), FX, "USDJPY LONG 1R Limit Emir")

    assert prod.asked() == []
    assert prod.fx_orders() == [("BUY_LIMIT", "USDJPY", 0.77, 154.05, 153.85, 154.45)]
    assert prod.tickmill_orders() == [("limit_buy", "USDJPY", 0.34, 154.05, 153.85, 154.45)]
    (trade,) = await prod.trades()
    pos = await prod.positions(trade.id)
    assert pos["tickmill"].status is PositionStatus.PENDING and not is_virtual_pending(pos["tickmill"])
    await prod.engine.monitor.run_virtual_once()                             # nothing waits for Binance "USD"
    assert prod.ref.calls == [] and not any("sanal tetik" in t for t in prod.texts())


# =========================================================================== #
# (e) fx route, no_hedge: another pair of the same currency opens, the same pair is refused
# =========================================================================== #
async def test_e_fx_other_pair_of_the_same_currency_opens_and_the_same_pair_opposite_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # (EURJPY is not among the eleven symbols known on the Tickmill account: listed here for this scenario.)
    prod = await Prod.create(tmp_path, monkeypatch, tickmill_symbols=TICKMILL_LISTED + ("EURJPY",))
    try:
        await prod.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
        assert prod.fx_orders() == [("BUY", "EURUSD", 0.48, 1.16205, 1.16, 1.166)]
        assert prod.tickmill_orders() == [("market_buy", "EURUSD", 0.21, 1.16, 1.166)]

        # EURJPY short while EURUSD long is open: another instrument, opens on both
        await prod.say(fx_pair(*EURJPY_SHORT), FX, "EURJPY SHORT 1R")
        assert prod.fx_orders()[1:] == [("SELL", "EURJPY", 0.73, 178.89, 179.1, 178.5)]
        assert prod.tickmill_orders()[1:] == [("market_sell", "EURJPY", 0.33, 179.1, 178.5)]
        assert prod.asked() == [] and not any("hedging" in t for t in prod.texts())

        # EURUSD short while EURUSD long is open: refused on both, nothing asked, one notice
        await prod.say(fx_pair(*EURUSD_SHORT), FX, "EUR SHORT 1R")
        assert len(prod.fx_orders()) == 2 and len(prod.tickmill_orders()) == 2
        assert prod.asked() == []
        (notice,) = prod.texts(Severity.WARNING)
        assert notice == (
            "ℹ️ <b>#3 EUR SHORT açılmadı</b> (hedging yasağı)\n"
            "• fx: EUR: açık LONG var; ters yönlü SHORT sinyali hedging yasağı nedeniyle açılmadı\n"
            "• tickmill: EUR: açık LONG var; ters yönlü SHORT sinyali hedging yasağı nedeniyle açılmadı")
        assert [t.status for t in await prod.trades()] == [TradeStatus.OPEN, TradeStatus.OPEN, TradeStatus.FAILED]
    finally:
        await prod.close()


async def test_e_fx_pair_that_tickmill_does_not_list_opens_on_fx_only_never_as_another_pair(prod: Prod) -> None:
    # The Tickmill symbol list has no EURJPY: tickmill sits this trade out. It is never placed on EURUSD.
    await prod.say(fx_pair(*EURJPY_SHORT), FX, "EURJPY SHORT 1R")

    assert prod.fx_orders() == [("SELL", "EURJPY", 0.73, 178.89, 179.1, 178.5)]
    assert prod.tickmill_orders() == []
    (trade,) = await prod.trades()
    assert trade.status is TradeStatus.OPEN and trade.note == "tickmill: unsupported"
    assert prod.asked() == [] and prod.texts(Severity.ERROR) == []


# =========================================================================== #
# (f) the live price is 2.5 pips above the stop: never more than twice the planned size
# =========================================================================== #
@pytest.mark.parametrize("entry_type", [None, EntryType.MARKET, EntryType.LIMIT])
async def test_f_fx_eurusd_price_close_to_the_stop_is_sized_at_most_twice_the_planned_size(
    prod: Prod, entry_type: EntryType | None
) -> None:
    # EURUSD long, entry 1.1620, SL 1.1600 (20 pips). The ask is 1.16025: 2.5 pips above the stop.
    fx, tickmill = prod.brokers["fx"], prod.brokers["tickmill"]
    planned = {}
    for broker in (fx, tickmill):                       # the size of the signal's own entry-to-stop distance
        sym = await broker.resolve_symbol("EUR", "USD", MarketType.FOREX, "EURUSD")
        size = await broker.size_position(sym, Side.LONG, 1.1620, 1.1600, float(broker.config.risk_usd))
        planned[broker.name] = size.quantity
    assert planned == {"fx": 0.5, "tickmill": 0.22}

    set_eurusd(prod, 1.16015, 1.16025)
    await prod.say(fx_pair("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660, entry_type=entry_type), FX, "EUR LONG 1R")

    assert prod.asked() == []                           # fx route: nothing is asked in any of the three readings
    (fx_order,) = prod.fx_orders()
    (tm_order,) = prod.tickmill_orders()
    assert fx_order == ("BUY", "EURUSD", 1.0, 1.16025, 1.16, 1.166)
    # An explicit MARKET signal at a better price stays a market order: the ENGINE limits its size (45 USD over
    # half the planned 20 pips = 0.45 lots). The other two readings are a marketable limit, limited by the
    # adapter to twice the rounded planned lots (2 x 0.22).
    tm_lots = 0.45 if entry_type is EntryType.MARKET else 0.44
    assert tm_order == ("market_buy", "EURUSD", tm_lots, 1.16, 1.166)
    assert fx_order[2] <= 2 * planned["fx"] and tm_order[2] <= 2 * 45.0 / (20 * PIP * 100_000.0) + 1e-9
    # sized on the 2.5 pips alone it would have been 4 lots on the 10,000 USD account (40 USD per pip)
    assert 100.0 / (2.5 * PIP * 100_000.0) == pytest.approx(4.0) and fx_order[2] < 4.0
    # what is really at risk is below the target: 25 of 100 USD, 11 of 45 USD
    assert fx_order[2] * 2.5 * PIP * 100_000.0 == pytest.approx(25.0)
    assert tm_order[2] * 2.5 * PIP * 100_000.0 == pytest.approx(tm_lots * 25.0)      # 11.25 or 11 USD
    (trade,) = await prod.trades()
    for name, pos in (await prod.positions(trade.id)).items():
        assert pos.status is PositionStatus.OPEN and pos.stop_loss == 1.16, name
        if entry_type is not EntryType.MARKET:              # limited by the adapter: noted on the position
            assert any("sınırlandı; gerçek risk hedefin altında" in w for w in pos.data["warnings"]), name
    if entry_type is EntryType.MARKET:                      # limited by the engine: the owner is told at once
        (notice,) = prod.texts(Severity.WARNING)
        assert notice.startswith("⚠️ <b>#1 EUR LONG açıldı</b>: miktar sınırlandı, gerçek risk hedefin altında")
        assert "• fx: 1 EURUSD" in notice and "gerçek risk ~25.00 USD (hedefin altında)" in notice
        assert "• tickmill: 0.45 EURUSD" in notice and "gerçek risk ~11.25 USD (hedefin altında)" in notice


async def test_f_crypto_market_order_approved_close_to_the_stop_is_sized_at_most_twice_the_planned_size(
    prod: Prod
) -> None:
    # BTC long, entry 74000, SL 73300 (700). Binance is at 73500: 200 above the stop, 0.68% from the entry.
    planned = {}
    for name, risk in (("binance", 10.0), ("breakout", 50.0)):
        sym = await prod.brokers[name].resolve_symbol("BTC", "USDT", MarketType.PERP, "BTCUSDT.P")
        planned[name] = (await prod.brokers[name].size_position(sym, Side.LONG, 74000.0, 73300.0, risk)).quantity
    sym = await prod.brokers["tickmill"].resolve_symbol("BTC", "USDT", MarketType.PERP, "BTCUSDT.P")
    planned["tickmill"] = (await prod.brokers["tickmill"].size_position(sym, Side.LONG, 73950.0, 73250.0, 45.0)).quantity
    assert planned == {"binance": 0.012, "breakout": 0.071, "tickmill": 0.06}

    prod.binance.set_price("BTC", 73499.0, 73501.0)
    prod.tickmill.set_price("BTCUSD", 73449.0, 73451.0)
    await prod.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO, "btc 1R long")
    assert prod.asked() == [["market_far_from_entry"]]                       # crypto route: still a question
    assert prod.binance.created() == [] and prod.tickmill_orders() == []

    result = await prod.engine.handle_confirmation(prod.notifier.confirmations[0].id, True)

    (entry,) = prod.binance.entry_orders()
    assert entry["type"] == "market" and 0 < entry["amount"] <= 2 * planned["binance"]
    (tm_order,) = prod.tickmill_orders()
    assert tm_order == ("market_buy", "BTCUSD", 0.12, 73250.0, 75450.0) and tm_order[2] <= 2 * planned["tickmill"]
    (trade,) = await prod.trades()
    pos = await prod.positions(trade.id)
    assert 0 < pos["breakout"].quantity <= 2 * planned["breakout"]
    for name in ("binance", "breakout", "tickmill"):
        line = next(x for x in result.splitlines() if x.startswith(f"• {name}:"))
        assert "Fiyat stopa çok yakın olduğu için miktar sınırlandı" in line, line


# =========================================================================== #
# (g) crypto route, base 1000PEPE: the manual broker lists "PEPE"
# =========================================================================== #
async def test_g_crypto_1000pepe_is_taken_by_the_manual_broker_that_lists_pepe(prod: Prod) -> None:
    by = {b.name: b for b in prod.cfg.brokers}
    assert "PEPE" in by["breakout"].allowed_assets and "1000PEPE" not in by["breakout"].allowed_assets
    pepe = signal("1000PEPEUSDT.P", "1000PEPE", "USDT", MarketType.PERP, Side.LONG, 0.012, 0.0117, 0.0129,
                  entry_type=EntryType.MARKET)
    await prod.say(pepe, KRIPTO, "1000pepe 1R long")

    assert prod.asked() == []
    (trade,) = await prod.trades()
    pos = await prod.positions(trade.id)
    assert sorted(pos) == ["binance", "breakout"]                            # tickmill does not trade PEPE
    # breakout: 50 USD over a 0.0003 stop distance = 166666 contracts of 1000PEPE
    assert pos["breakout"].status is PositionStatus.OPEN and pos["breakout"].quantity == 166666.0
    (note,) = [t for t in prod.texts() if "Elle işlem (breakout)" in t]
    assert "166666 KONTRAT — 1000PEPE LONG" in note and "Stop: 0.0117" in note
    (entry,) = prod.binance.entry_orders()
    assert (entry["symbol"], entry["type"], entry["side"]) == ("1000PEPE/USDT:USDT", "market", "buy")
    assert 0 < entry["amount"] * (0.012 - 0.0117) <= 10.0
    assert prod.tickmill_orders() == [] and prod.terminal.sent == []
