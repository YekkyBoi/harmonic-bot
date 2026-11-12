"""Pre-flight broker fixes, each reproduced on the adapter fakes:

7.  native MT5: an all-zero first tick (symbol just added to Market Watch) is retried instead of "no price";
8.  symbol candidates of both MetaTrader adapters: index CFDs also try their bare name, a pair the broker does
    not list never falls back to BASE+USD, FOREX/CFD plans do not probe USDT names on MetaApi either;
9.  Broker.accepts_asset ignores a contract multiplier prefix (1000PEPE == PEPE, 1INCH stays 1INCH);
10. a marketable limit that is re-sized from its real fill never exceeds risk.max_size_multiple x the planned lots.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from copytrader.brokers import mt5_broker
from copytrader.brokers.base import BrokerError
from copytrader.brokers.manual_broker import ManualBroker
from copytrader.brokers.mt5_broker import MT5, Mt5Broker, rank_symbol, symbol_candidates
from copytrader.config import ManualBrokerConfig, RiskConfig
from copytrader.models import EntryType, MarketType, PositionStatus, Side, TakeProfit, TradePlan

from fakes import FakeBroker
from test_metaapi_broker import FakeConn
from test_metaapi_broker import connected as metaapi_connected
from test_mt5_broker import SERVER_TIME, FakeMT5, make_cfg, make_req, make_symbol
from test_mt5_broker import connected as mt5_connected
from test_paper_broker import FakePrices

REAL_TICK_WAIT_SEC = Mt5Broker.tick_wait_sec     # read at import time, before the fixture below zeroes it
EUR_BID, EUR_ASK = 1.16195, 1.16205
NO_PRICE = r"EURUSD için fiyat yok \(piyasa kapalı olabilir\)\."


@pytest.fixture(autouse=True)
def _clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fake ticks carry SERVER_TIME; make the broker's "now" match and skip the real tick waits."""
    monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: SERVER_TIME - 7200.0))
    monkeypatch.setattr(Mt5Broker, "tick_wait_sec", 0.0)


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """The adapter's waits with the real ``tick_wait_sec``, recorded instead of slept."""
    slept: list[float] = []
    real_sleep = asyncio.sleep

    async def fake_sleep(delay: float, *args: Any, **kwargs: Any) -> None:
        slept.append(delay)
        await real_sleep(0)

    monkeypatch.setattr(Mt5Broker, "tick_wait_sec", REAL_TICK_WAIT_SEC)
    monkeypatch.setattr(mt5_broker.asyncio, "sleep", fake_sleep)
    return slept


# --------------------------------------------------------------------------- #
# fakes / builders
# --------------------------------------------------------------------------- #
def fx_symbol(name: str, **kw: Any) -> SimpleNamespace:
    base: dict[str, Any] = dict(
        digits=5, point=0.00001, trade_tick_size=0.00001, trade_contract_size=100000.0,
        filling_mode=MT5.SYMBOL_FILLING_IOC, currency_base=name[:3], currency_profit=name[3:6],
        currency_margin=name[:3],
    )
    base.update(kw)
    return make_symbol(name, **base)


def index_symbol(name: str, **kw: Any) -> SimpleNamespace:
    base: dict[str, Any] = dict(digits=1, point=0.1, trade_tick_size=0.1, currency_base="USD",
                                currency_profit="USD", currency_margin="USD")
    base.update(kw)
    return make_symbol(name, **base)


def tick(bid: float, ask: float) -> SimpleNamespace:
    return SimpleNamespace(bid=bid, ask=ask, last=0.0, time=SERVER_TIME)


def empty_tick() -> SimpleNamespace:
    return SimpleNamespace(bid=0.0, ask=0.0, last=0.0, time=0)


def terminal(*symbols: SimpleNamespace, ticks: dict[str, SimpleNamespace] | None = None) -> FakeMT5:
    fake = FakeMT5()
    fake.symbols = {s.name: s for s in symbols}
    fake.ticks = dict(ticks or {})
    return fake


class ColdTerminal(FakeMT5):
    """EURUSD only. The first ``cold`` tick polls answer an all-zero tick (None = for ever), as a symbol that was
    just added to Market Watch does; ``script`` gives the answers of the first polls explicitly instead."""

    def __init__(self, cold: int | None = 0, *, visible: bool = True, script: tuple[Any, ...] = ()) -> None:
        super().__init__()
        self.symbols = {"EURUSD": fx_symbol("EURUSD", visible=visible)}
        self.ticks = {"EURUSD": tick(EUR_BID, EUR_ASK)}
        self.cold = cold
        self.script = list(script)
        self.polls = 0

    def symbol_info_tick(self, name: str) -> Any:
        self._log("symbol_info_tick", name)
        self.polls += 1
        if self.script:
            return self.script.pop(0)
        if self.cold is None or self.polls <= self.cold:
            return empty_tick()
        return self.ticks.get(name)


def eur_plan(side: Side = Side.LONG, entry_type: EntryType = EntryType.LIMIT, entry: float = 1.1620,
             sl: float = 1.1600, tp: float = 1.1660) -> TradePlan:
    return TradePlan(
        plan_id="p1", base_asset="EUR", quote_asset="USD", market_type=MarketType.FOREX, side=side,
        entry_type=entry_type, entry_price=entry, stop_loss=sl, take_profits=[TakeProfit(price=tp, close_fraction=1.0)],
        risk_r=1.0, risk_pct=1.0, source_chat_id=-100123, source_message_ids=[42],
        created_at=datetime(2026, 11, 12, tzinfo=UTC),
    )


def risk_with_multiple(k: float) -> RiskConfig:
    """The adapters' risk config with ``risk.max_size_multiple = k`` (fees off like the other adapter tests)."""
    return RiskConfig(include_fees_in_risk=False, max_size_multiple=k)


# --------------------------------------------------------------------------- #
# TASK 7 - native MT5: an empty first tick is retried
# --------------------------------------------------------------------------- #
async def test_hidden_symbol_with_two_empty_first_ticks_resolves_and_quotes() -> None:
    fake = ColdTerminal(cold=2, visible=False)
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)
    assert sym is not None and sym.broker_symbol == "EURUSD"
    assert ("symbol_select", ("EURUSD", True), {}) in fake.calls
    q = await b.get_quote(sym)                 # used to raise "EURUSD için fiyat yok (piyasa kapalı olabilir)."
    assert (q.bid, q.ask) == (EUR_BID, EUR_ASK)
    await b.close()


async def test_wait_after_symbol_select_polls_until_the_first_priced_tick() -> None:
    fake = ColdTerminal(cold=2, visible=False)
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)
    assert fake.polls == 3                     # two empty ticks, then the first real one
    await b.get_quote(sym)
    assert fake.polls == 4                     # the quote itself needs a single poll
    await b.close()


async def test_quote_retries_an_empty_tick_without_symbol_select() -> None:
    fake = ColdTerminal(cold=2)                # already in Market Watch: no wait happened at resolve time
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)
    assert fake.polls == 0 and not any(c[0] == "symbol_select" for c in fake.calls)
    q = await b.get_quote(sym)
    assert (q.bid, q.ask) == (EUR_BID, EUR_ASK) and fake.polls == 3
    await b.close()


async def test_missing_empty_and_one_sided_ticks_are_all_waited_out() -> None:
    no_ask = SimpleNamespace(bid=EUR_BID, ask=0.0, last=0.0, time=SERVER_TIME)
    no_bid = SimpleNamespace(bid=0.0, ask=EUR_ASK, last=0.0, time=SERVER_TIME)
    fake = ColdTerminal(script=(None, empty_tick(), no_ask, no_bid))
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)
    q = await b.get_quote(sym)
    assert (q.bid, q.ask) == (EUR_BID, EUR_ASK) and fake.polls == 5
    await b.close()


async def test_market_order_opens_after_empty_first_ticks() -> None:
    """The auditors' "cold ticks=3" case: the first EURUSD signal after a restart lost its FundingPips leg."""
    fake = ColdTerminal(cold=3, visible=False)
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)
    assert sym is not None
    size = await b.size_position(sym, Side.LONG, EUR_ASK, 1.1600, 100.0)
    pos = await b.open_position(make_req(sym, size.quantity, eur_plan(entry_type=EntryType.MARKET)))
    (deal,) = fake.sent_actions(MT5.TRADE_ACTION_DEAL)
    assert (deal["symbol"], deal["volume"], deal["price"], deal["sl"]) == ("EURUSD", 0.48, EUR_ASK, 1.1600)
    assert pos.status is PositionStatus.OPEN and pos.error is None
    await b.close()


async def test_always_empty_ticks_still_end_in_the_no_price_error() -> None:
    fake = ColdTerminal(cold=None, visible=False)
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)      # best effort: the symbol is still resolved
    assert sym is not None and sym.broker_symbol == "EURUSD"
    with pytest.raises(BrokerError, match=NO_PRICE):
        await b.get_quote(sym)
    with pytest.raises(BrokerError, match=NO_PRICE):
        await b.open_position(make_req(sym, 0.48, eur_plan(entry_type=EntryType.MARKET)))
    assert fake.sent == []                     # nothing is sent without a price
    await b.close()


async def test_empty_tick_is_polled_for_about_three_seconds_then_gives_up(sleeps: list[float]) -> None:
    fake = ColdTerminal(cold=None, visible=False)
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)      # the wait right after symbol_select
    assert sym is not None
    assert fake.polls == 11 and sleeps == [REAL_TICK_WAIT_SEC] * 10 and sum(sleeps) == pytest.approx(3.0)
    sleeps.clear()
    with pytest.raises(BrokerError, match=NO_PRICE):                  # the quote
        await b.get_quote(sym)
    assert fake.polls == 22 and sleeps == [REAL_TICK_WAIT_SEC] * 10 and sum(sleeps) == pytest.approx(3.0)
    await b.close()


async def test_missing_ticks_before_empty_ones_share_the_three_seconds(sleeps: list[float]) -> None:
    fake = ColdTerminal(cold=None, script=(None, None))
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)
    with pytest.raises(BrokerError, match=NO_PRICE):
        await b.get_quote(sym)
    assert fake.polls == 11 and sum(sleeps) == pytest.approx(3.0)     # 2 missing + 9 empty polls, 10 waits
    await b.close()


async def test_valid_first_tick_returns_at_once(sleeps: list[float]) -> None:
    fake = ColdTerminal()
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)
    q = await b.get_quote(sym)
    assert (q.bid, q.ask) == (EUR_BID, EUR_ASK)
    assert fake.polls == 1 and sleeps == []    # the normal path: one poll, no wait
    await b.close()


async def test_missing_tick_keeps_its_short_retry_budget(sleeps: list[float]) -> None:
    fake = ColdTerminal(script=(None,) * 50)
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)
    with pytest.raises(BrokerError, match="MT5 EURUSD fiyatı alınamadı"):
        await b.get_quote(sym)
    assert fake.polls == 3 and sleeps == [REAL_TICK_WAIT_SEC] * 2     # as before: 3 tries, 2 waits
    await b.close()


# --------------------------------------------------------------------------- #
# TASK 8 - symbol candidates
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("base", "quote", "market_type", "expected"), [
    # the cases named in the task
    ("EUR", "USD", MarketType.FOREX, ["EURUSD"]),
    ("EUR", "GBP", MarketType.FOREX, ["EURGBP"]),                 # was [EURGBP, EURUSD]
    ("GBP", "JPY", None, ["GBPJPY"]),                             # was [GBPJPY, GBPUSD, GBPUSDT]
    ("NAS100", None, MarketType.CFD, ["NAS100USD", "NAS100"]),    # was [NAS100USD]
    ("NAS100", "USD", MarketType.CFD, ["NAS100USD", "NAS100"]),
    ("XAU", "USD", MarketType.CFD, ["XAUUSD"]),
    ("BTC", "USDT", MarketType.PERP, ["BTCUSDT", "BTCUSD"]),
    ("BTC", None, None, ["BTCUSD", "BTCUSDT"]),
    # unchanged for a USD-like quote / no quote
    ("BTC", "USDC", MarketType.PERP, ["BTCUSDC", "BTCUSD", "BTCUSDT"]),
    ("BTC", "USD", None, ["BTCUSD", "BTCUSDT"]),
    ("NAS100", None, None, ["NAS100USD", "NAS100USDT"]),
    ("NAS100", "USD", MarketType.FOREX, ["NAS100USD"]),           # the bare name is for CFDs only
    # never the bare name of a currency or a metal: they are pairs, the quote is part of the instrument
    ("EUR", None, MarketType.CFD, ["EURUSD"]),
    ("XAG", None, MarketType.CFD, ["XAGUSD"]),
    # a quote that is not USD-like: only that pair
    ("eur", "gbp", MarketType.FOREX, ["EURGBP"]),
    ("USD", "JPY", MarketType.FOREX, ["USDJPY"]),
    ("ETH", "BTC", MarketType.PERP, ["ETHBTC"]),
    ("BTC", "EUR", MarketType.CFD, ["BTCEUR"]),                   # a symbol called just "BTC" is not BTC/EUR
    # ... except an index with the currency it is priced in (how normalize_symbol reads "GER40")
    ("GER40", "EUR", MarketType.CFD, ["GER40EUR", "GER40"]),
    ("US30", "USD", MarketType.CFD, ["US30USD", "US30"]),
])
def test_symbol_candidates(base: str, quote: str | None, market_type: MarketType | None, expected: list[str]) -> None:
    assert symbol_candidates(base, quote, market_type) == expected


def test_bare_name_matches_only_the_whole_name_or_a_separated_suffix() -> None:
    cands, whole = ["US30USD", "US30"], ("US30",)
    assert rank_symbol("US30", cands, whole) == (0, 1, 0)
    assert rank_symbol("#US30", cands, whole) == (0, 1, 0)
    assert rank_symbol("US30.cash", cands, whole) == (0, 1, 5)
    assert rank_symbol("US30-ECN", cands, whole) == (0, 1, 4)
    assert rank_symbol("US30YR", cands, whole) is None           # a 30-year bond CFD, not the Dow
    assert rank_symbol("US300", cands, whole) is None
    assert rank_symbol("US30m", cands, whole) is None            # glued letters could be anything: symbol_map
    assert rank_symbol("US30USDm", cands, whole) == (1, 0, 1)    # BASE+QUOTE names match as before
    assert rank_symbol("US30USD.r", cands, whole) == (0, 0, 2)
    assert rank_symbol("US30YR", cands) == (1, 1, 2)             # without ``whole``: the ranking is unchanged


async def test_native_bare_name_does_not_match_a_longer_name() -> None:
    """The Dow listed as DJ30 next to a US 30-year bond CFD, SOL only as "SOLANA", BTC only against EUR."""
    fake = terminal(index_symbol("DJ30"), index_symbol("US30YR"), index_symbol("SOLANA"), fx_symbol("BTCEUR"))
    b = await mt5_connected(fake)
    # Changed by pre-flight batch 2 (task B7, alias table in mt5_broker.CFD_ALIASES): this used to be an error
    # because only the names US30USD / US30 were tried. DJ30 is the SAME instrument (the Dow) under another
    # broker name, so US30 now resolves to it. The point of this test stands: the bond "US30YR" never matches.
    sym = await b.resolve_symbol("US30", "USD", MarketType.CFD)
    assert sym is not None and sym.broker_symbol == "DJ30"
    del fake.symbols["DJ30"]                                     # the Dow under none of its names: still an error
    with pytest.raises(BrokerError, match=r"'US30' için MT5'te sembol bulunamadı \(denenen: US30USD, US30\)"):
        await b.resolve_symbol("US30", "USD", MarketType.CFD)
    with pytest.raises(BrokerError, match=r"denenen: SOLUSD, SOL\)"):
        await b.resolve_symbol("SOL", "USD", MarketType.CFD)
    with pytest.raises(BrokerError, match=r"denenen: BTCUSD, BTC\)"):
        await b.resolve_symbol("BTC", None, MarketType.CFD)
    await b.close()


async def test_metaapi_bare_name_does_not_match_a_longer_name() -> None:
    conn = metaapi_conn("DJ30", "US30YR", "SOLANA", "BTCEUR")
    b = await metaapi_connected(conn)
    assert await b.resolve_symbol("US30", "USD", MarketType.CFD) is None
    assert await b.resolve_symbol("SOL", "USD", MarketType.CFD) is None
    assert await b.resolve_symbol("BTC", None, MarketType.CFD) is None
    assert conn.called("get_symbol_specification") == []
    await b.close()


async def test_native_index_resolves_to_suffixed_bare_name() -> None:
    fake = terminal(fx_symbol("EURUSD"), index_symbol("NAS100.cash"), index_symbol("US30.cash"))
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("NAS100", "USD", MarketType.CFD, "NAS100")
    assert sym is not None and sym.broker_symbol == "NAS100.cash" and sym.base_asset == "NAS100"
    sym = await b.resolve_symbol("US30", None, MarketType.CFD)
    assert sym is not None and sym.broker_symbol == "US30.cash"
    await b.close()


async def test_native_index_resolves_to_exact_bare_name_but_prefers_base_usd() -> None:
    fake = terminal(index_symbol("NAS100"), index_symbol("US30"), index_symbol("US30USD"))
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("NAS100", "USD", MarketType.CFD)
    assert sym is not None and sym.broker_symbol == "NAS100"
    sym = await b.resolve_symbol("US30", "USD", MarketType.CFD)
    assert sym is not None and sym.broker_symbol == "US30USD"        # the bare name is the LAST candidate
    await b.close()


async def test_native_unlisted_cross_is_not_traded_as_base_usd() -> None:
    fake = terminal(fx_symbol("EURUSD"), fx_symbol("GBPUSD"))
    b = await mt5_connected(fake)
    with pytest.raises(BrokerError, match=r"EURGBP paritesi MT5'te bulunamadı \(denenen: EURGBP\)"):
        await b.resolve_symbol("EUR", "GBP", MarketType.FOREX)       # used to return EURUSD
    assert await b.resolve_symbol("EUR", "GBP") is None
    assert await b.resolve_symbol("GBP", "JPY") is None              # used to return GBPUSD
    assert not any(c[0] == "symbol_info" and c[1] in (("EURUSD",), ("GBPUSD",)) for c in fake.calls)
    await b.close()


async def test_native_listed_cross_still_resolves() -> None:
    fake = terminal(fx_symbol("EURUSD"), fx_symbol("EURGBP.r"), fx_symbol("GBPJPY"))
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("EUR", "GBP", MarketType.FOREX)
    assert sym is not None and sym.broker_symbol == "EURGBP.r" and sym.quote_asset == "GBP"
    sym = await b.resolve_symbol("GBP", "JPY", MarketType.FOREX)
    assert sym is not None and sym.broker_symbol == "GBPJPY"
    await b.close()


def metaapi_conn(*symbols: str) -> FakeConn:
    conn = FakeConn()
    template = conn.specs["ETHUSD"]
    conn.symbols = list(symbols)
    conn.specs = {s: {**template, "symbol": s} for s in symbols}
    return conn


async def test_metaapi_index_resolves_to_suffixed_bare_name() -> None:
    b = await metaapi_connected(metaapi_conn("EURUSD", "NAS100.cash", "US30"))
    sym = await b.resolve_symbol("NAS100", "USD", MarketType.CFD, "NAS100")
    assert sym is not None and sym.broker_symbol == "NAS100.cash" and sym.base_asset == "NAS100"
    sym = await b.resolve_symbol("US30", None, MarketType.CFD)
    assert sym is not None and sym.broker_symbol == "US30"
    await b.close()


async def test_metaapi_unlisted_cross_is_not_traded_as_base_usd() -> None:
    conn = metaapi_conn("EURUSD", "GBPUSD")
    b = await metaapi_connected(conn)
    assert await b.resolve_symbol("EUR", "GBP", MarketType.FOREX) is None     # used to return EURUSD
    assert await b.resolve_symbol("GBP", "JPY") is None                       # used to return GBPUSD
    assert conn.called("get_symbol_specification") == []
    await b.close()


async def test_metaapi_forex_does_not_probe_usdt_names() -> None:
    conn = metaapi_conn("EURUSDT", "EURUSD.a")
    b = await metaapi_connected(conn)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)              # used to return EURUSDT
    assert sym is not None and sym.broker_symbol == "EURUSD.a"
    assert conn.called("get_symbol_specification") == [(("EURUSD.a",), {})]
    sym = await b.resolve_symbol("EUR", "USD")                                # no market type: as before
    assert sym is not None and sym.broker_symbol == "EURUSDT"
    await b.close()


# --------------------------------------------------------------------------- #
# TASK 9 - allowed_assets ignores a contract multiplier prefix
# --------------------------------------------------------------------------- #
def allowing(*assets: str) -> FakeBroker:
    return FakeBroker("breakout", allowed_assets=list(assets))


@pytest.mark.parametrize(("allowed", "asset"), [
    ("PEPE", "1000PEPE"),            # the reproduced case: chart 1000PEPEUSDT.P, list says PEPE
    ("1000PEPE", "PEPE"),            # and the reverse
    ("pepe", "1000Pepe"),
    ("SHIB", "1000SHIB"),
    ("SATS", "10000SATS"),
    ("MOG", "1000000MOG"),
    ("BABYDOGE", "1MBABYDOGE"),
    ("1MBABYDOGE", "1000000BABYDOGE"),
    ("X", "1000X"),
    ("1000PEPE", "1000PEPE"),
    ("PEPE", "PEPE"),
    ("1INCH", "1INCH"),
    ("1inch", "1INCH"),
])
def test_accepts_asset_ignores_contract_multiplier_prefix(allowed: str, asset: str) -> None:
    assert allowing("BTC", allowed, "ETH").accepts_asset(asset)


@pytest.mark.parametrize(("allowed", "asset"), [
    ("INCH", "1INCH"),               # "1INCH" is the coin's real name, not 1 x INCH
    ("1INCH", "INCH"),
    ("PEPE", "1000SHIB"),
    ("PEPE", "PEPE1000"),
    ("PEPE", "X1000PEPE"),
    ("PEPE", "1000"),
    ("1000", "PEPE"),
    ("PEPE", "100PEPE"),
    ("PEPE", "BTC"),
])
def test_accepts_asset_still_rejects_other_assets(allowed: str, asset: str) -> None:
    assert not allowing("ETH", allowed).accepts_asset(asset)


def test_empty_allowed_assets_accepts_everything() -> None:
    assert allowing().accepts_asset("1000PEPE") and allowing().accepts_asset("1INCH")


def test_manual_breakout_broker_takes_the_1000pepe_chart(tmp_path: Path) -> None:
    cfg = ManualBrokerConfig(type="manual", name="breakout", risk_usd=50.0, allowed_assets=[
        "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK", "BNB", "LTC", "SUI", "TON", "DOT", "NEAR",
        "APT", "ARB", "OP", "PEPE", "WIF", "HYPE"])
    breakout = ManualBroker(cfg, str(tmp_path), price_source=FakePrices())
    assert breakout.accepts_asset("1000PEPE") and breakout.accepts_asset("PEPE")
    assert not breakout.accepts_asset("1000SHIB") and not breakout.accepts_asset("EUR")


# --------------------------------------------------------------------------- #
# TASK 10 - a marketable limit re-sized from its fill respects the size cap
# --------------------------------------------------------------------------- #
CAP_NOTE = "sınırlandı"
RESIZE_NOTE = "yeniden hesaplandı"


async def open_marketable_limit(bid: float, ask: float, *, risk: Any = None, plan: TradePlan | None = None,
                                lots: float = 0.48) -> tuple[dict[str, Any], Any]:
    """Planned ``lots`` for the signal's own 20-pip stop; the limit is already marketable at ``bid``/``ask``."""
    fake = terminal(fx_symbol("EURUSD"), ticks={"EURUSD": tick(bid, ask)})
    b = await mt5_connected(fake)
    if risk is not None:
        b.set_risk_config(risk)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)
    pos = await b.open_position(make_req(sym, lots, plan or eur_plan()))
    await b.close()
    assert fake.sent_actions(MT5.TRADE_ACTION_PENDING) == []
    (deal,) = fake.sent_actions(MT5.TRADE_ACTION_DEAL)
    return deal, pos


async def test_resize_ten_pips_from_the_stop_doubles_the_lots() -> None:
    deal, pos = await open_marketable_limit(1.1609, 1.1610)           # half the planned stop distance
    assert deal["volume"] == 0.96 and pos.quantity == 0.96
    notes = pos.data["warnings"]
    assert any(RESIZE_NOTE in n for n in notes) and not any(CAP_NOTE in n for n in notes)


@pytest.mark.parametrize(("bid", "ask", "uncapped"), [
    (1.16015, 1.16025, 3.84),        # 2.5 pips from the stop
    (1.16005, 1.16015, 6.4),         # 1.5 pips: the auditors' 6.66-lot row
])
async def test_resize_near_the_stop_is_capped_at_twice_the_planned_lots(
    bid: float, ask: float, uncapped: float
) -> None:
    deal, pos = await open_marketable_limit(bid, ask)
    assert deal["volume"] == 0.96 and pos.quantity == 0.96            # used to be ``uncapped``
    assert deal["volume"] < uncapped
    (note,) = [n for n in pos.data["warnings"] if CAP_NOTE in n]
    assert "2 katıyla" in note and "0.96" in note and "stopa" in note


async def test_resize_cap_on_a_short() -> None:
    plan = eur_plan(side=Side.SHORT, entry=1.1600, sl=1.1620, tp=1.1560)
    deal, pos = await open_marketable_limit(1.16175, 1.16185, plan=plan)      # bid 2.5 pips below the stop
    assert deal["type"] == MT5.ORDER_TYPE_SELL and deal["volume"] == 0.96
    assert any(CAP_NOTE in n for n in pos.data["warnings"])


@pytest.mark.parametrize(("bid", "ask"), [(1.1609, 1.1610), (1.16015, 1.16025), (1.16005, 1.16015)])
async def test_resize_with_multiple_one_never_exceeds_the_planned_lots(bid: float, ask: float) -> None:
    deal, pos = await open_marketable_limit(bid, ask, risk=risk_with_multiple(1.0))
    assert deal["volume"] == 0.48 and pos.quantity == 0.48
    assert any(CAP_NOTE in n and "1 katıyla" in n for n in pos.data["warnings"])


async def test_resize_cap_follows_the_configured_multiple() -> None:
    deal, pos = await open_marketable_limit(1.16015, 1.16025, risk=risk_with_multiple(3.0))
    assert deal["volume"] == 1.44 and pos.quantity == 1.44            # 3 x 0.48, not 3.84
    assert any(CAP_NOTE in n and "3 katıyla" in n for n in pos.data["warnings"])
    deal, pos = await open_marketable_limit(1.1609, 1.1610, risk=risk_with_multiple(3.0))
    assert deal["volume"] == 0.96 and not any(CAP_NOTE in n for n in pos.data["warnings"])


async def test_resize_margin_check_uses_the_capped_lots() -> None:
    fake = terminal(fx_symbol("EURUSD"), ticks={"EURUSD": tick(1.16015, 1.16025)})
    fake.margin_per_lot = 5_000.0              # allowed: 90% of 9000 free = 8100 -> 0.96 lots fit, 3.84 would not
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)
    pos = await b.open_position(make_req(sym, 0.48, eur_plan()))
    (deal,) = fake.sent_actions(MT5.TRADE_ACTION_DEAL)
    assert deal["volume"] == 0.96 and any(CAP_NOTE in n for n in pos.data["warnings"])
    await b.close()

    fake = terminal(fx_symbol("EURUSD"), ticks={"EURUSD": tick(1.16015, 1.16025)})
    fake.margin_per_lot = 10_000.0             # not even the capped lots fit: the planned lots stand, as before
    b = await mt5_connected(fake)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)
    pos = await b.open_position(make_req(sym, 0.48, eur_plan()))
    (deal,) = fake.sent_actions(MT5.TRADE_ACTION_DEAL)
    notes = pos.data["warnings"]
    assert deal["volume"] == 0.48 and any("teminat yetersiz" in n for n in notes)
    assert not any(CAP_NOTE in n for n in notes) and not any(RESIZE_NOTE in n for n in notes)
    await b.close()


@pytest.mark.parametrize(("risk", "expected"), [
    (SimpleNamespace(), 2.0),                              # a risk config without the field: the default
    (SimpleNamespace(max_size_multiple=None), 2.0),
    (SimpleNamespace(max_size_multiple="abc"), 2.0),
    (SimpleNamespace(max_size_multiple=float("nan")), 2.0),
    (SimpleNamespace(max_size_multiple=0), 2.0),
    (SimpleNamespace(max_size_multiple=-3), 2.0),
    (SimpleNamespace(max_size_multiple=0.5), 1.0),         # never below the planned lots
    (SimpleNamespace(max_size_multiple=1), 1.0),
    (SimpleNamespace(max_size_multiple="1.5"), 1.5),
    (SimpleNamespace(max_size_multiple=4), 4.0),
])
def test_max_size_multiple_falls_back_to_the_default(risk: Any, expected: float) -> None:
    b = Mt5Broker(make_cfg(), mt5_module=FakeMT5())
    b.risk_config = risk
    assert b._max_size_multiple() == expected


def test_adapter_reads_risk_max_size_multiple() -> None:
    assert RiskConfig().max_size_multiple == 2.0           # the config field the adapter reads, and its default
    b = Mt5Broker(make_cfg(), mt5_module=FakeMT5())
    assert b._max_size_multiple() == 2.0                   # RiskConfig() as set by the adapter itself
    b.set_risk_config(RiskConfig(max_size_multiple=3.5))
    assert b._max_size_multiple() == 3.5


async def test_resize_cap_is_shared_with_metaapi() -> None:
    conn = metaapi_conn("EURUSD")
    conn.specs["EURUSD"].update(tickSize=0.00001, point=0.00001, digits=5, contractSize=100000.0)
    conn.prices = {"EURUSD": {"symbol": "EURUSD", "bid": 1.16015, "ask": 1.16025, "profitTickValue": 1.0,
                              "lossTickValue": 1.0}}
    b = await metaapi_connected(conn)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)
    pos = await b.open_position(make_req(sym, 0.48, eur_plan()))
    ((args, _kwargs),) = conn.called("create_market_buy_order")
    assert args == ("EURUSD", 0.96) and pos.quantity == 0.96
    assert any(CAP_NOTE in n for n in pos.data["warnings"])
    await b.close()
