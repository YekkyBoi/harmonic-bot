"""Pre-flight batch 3, parser side: validator, prefilter and LLM parser.

P1  A LIMIT signal whose first target lies between its entry and the current price (a pullback limit) is no
    longer doubtful: no ``market_beyond_tp``, so the plan is automatic.
P2  ``explicit_market_drift: follow``: an explicit MARKET signal whose live price is BETTER than its entry
    (between entry and stop) stays a MARKET plan and nothing is asked.
P3  An implausible entry-to-stop distance (a misread stop on the correct side) is asked about.
P4  The prefilter lets short management texts ("Geldi", "Başa baş", a lone check mark...) reach the model.
P5  A photo that could not be downloaded is announced to the model with a note.
P6  A reply cut by the token limit, or an empty / unparsable one, is a transient error (the engine retries).
P7  The plan summary leaves the risk percentage out when it does not apply.

The config used here is an embedded copy of the owner's production config (ids and secrets are dummies).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
import yaml

from copytrader.config import AppConfig, LLMConfig, RiskConfig, TopicRoute, effective_risk
from copytrader.engine.orchestrator import Engine
from copytrader.models import ActionType, EntryType, MarketType, ParsedAction, Side, TakeProfit, TradePlan, ValidationIssue
from copytrader.parsing.validator import build_open_plan
from copytrader.storage.db import Database
from copytrader.telegram import formatting

from fakes import FakeBroker, FakeNotifier, FakeParser, FakePriceOracle

NOW = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
MARKET, LIMIT = EntryType.MARKET, EntryType.LIMIT
LONG, SHORT = Side.LONG, Side.SHORT

# Replica of the owner's production config.yaml (same routes, brokers and values; ids and secrets are dummies).
PROD_REPLICA_YAML = """
dry_run: false
data_dir: data
timezone: Europe/Istanbul
telegram:
  api_id: 1
  api_hash: h
  source_chats: [-1001111111111]
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
  bot_token: "123:abc"
  owner_user_id: 42
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
    allowed_assets: [BTC, ETH, SOL, XRP, DOGE, ADA, AVAX, LINK, BNB, LTC, SUI, TON, DOT, NEAR, APT, ARB, OP, PEPE, WIF, HYPE]
  - name: fx
    type: mt5
    login: 5001
    password: pw
    server: Demo
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
    symbol_map: {BTC: BTCUSD, ETH: ETHUSD}
    allowed_assets: [EUR, GBP, USD, AUD, NZD, BTC, ETH, XAU, XAG]
    no_hedge: true
    daily_loss_limit_pct: 0
"""


def prod_replica(**route_overrides: dict[str, Any]) -> AppConfig:
    """The production replica; ``route_overrides`` = {route name: {field: value}} put on that topic."""
    data = yaml.safe_load(PROD_REPLICA_YAML)
    for topic in data["telegram"]["topics"]:
        topic.update(route_overrides.get(topic["name"], {}))
    return AppConfig.model_validate(data)


def route(name: str, cfg: AppConfig | None = None) -> TopicRoute:
    return next(r for r in (cfg or prod_replica()).telegram.topics if r.name == name)


def route_risk(name: str, **overrides: Any) -> RiskConfig:
    """The risk settings build_open_plan gets for a signal of that production route."""
    cfg = prod_replica(**{name: overrides})
    return effective_risk(cfg.risk, route(name, cfg))


# --------------------------------------------------------------------------- #
# Signals
# --------------------------------------------------------------------------- #
def _open(et: EntryType | None, **kw: Any) -> ParsedAction:
    data: dict[str, Any] = dict(action=ActionType.OPEN, side=LONG, entry_type=et, risk_r=1.0,
                                used_message_ids=[701], confidence=0.95)
    data.update(kw)
    data["take_profits"] = [TakeProfit(price=p) for p in data.pop("tps")]
    return ParsedAction(**data)


def btc(et: EntryType | None, **kw: Any) -> ParsedAction:
    """BTCUSDT.P long, entry 74000, SL 73300 (0.95%), TP 75500."""
    data: dict[str, Any] = dict(symbol="BTCUSDT.P", base_asset="BTC", quote_asset="USDT", market_type=MarketType.PERP,
                                entry_price=74000.0, stop_loss=73300.0, tps=(75500.0,))
    data.update(kw)
    return _open(et, **data)


def btc_short(et: EntryType | None, **kw: Any) -> ParsedAction:
    """BTCUSDT.P short, entry 74000, SL 74700, TP 72500."""
    return btc(et, **{"side": SHORT, "stop_loss": 74700.0, "tps": (72500.0,), **kw})


def eur(et: EntryType | None, **kw: Any) -> ParsedAction:
    """EURUSD long, entry 1.1620, SL 1.1600 (20 pips; 0.25R = 5 pips), TP 1.1680."""
    data: dict[str, Any] = dict(symbol="EURUSD", base_asset="EUR", quote_asset="USD", market_type=MarketType.FOREX,
                                entry_price=1.1620, stop_loss=1.1600, tps=(1.1680,))
    data.update(kw)
    return _open(et, **data)


def eur_short(et: EntryType | None, **kw: Any) -> ParsedAction:
    """EURUSD short, entry 1.1620, SL 1.1640, TP 1.1560."""
    return eur(et, **{"side": SHORT, "stop_loss": 1.1640, "tps": (1.1560,), **kw})


def build(action: ParsedAction, live: float | None, risk: RiskConfig, **kw: Any
          ) -> tuple[TradePlan | None, list[ValidationIssue]]:
    return build_open_plan(action, risk=risk, llm=LLMConfig(), market_price=live, source_chat_id=-100, now=NOW, **kw)


def codes(issues: list[ValidationIssue]) -> list[str]:
    return [i.code for i in issues]


def decide(action: ParsedAction, live: float | None, risk: RiskConfig) -> tuple[EntryType | None, list[str]]:
    """(plan entry type or None when the plan is blocked, issue codes)."""
    plan, issues = build(action, live, risk)
    return (plan.entry_type if plan else None, codes(issues))


async def start_engine(cfg: AppConfig) -> tuple[Engine, FakePriceOracle, Database]:
    db = Database(":memory:")
    await db.open()
    oracle = FakePriceOracle()
    engine = Engine(cfg, db, FakeParser(), [FakeBroker(b.name) for b in cfg.brokers], FakeNotifier(),
                    price_oracle=oracle)
    await engine.start(monitor=False)
    return engine, oracle, db


# --------------------------------------------------------------------------- #
# P1: pullback limit entries
# --------------------------------------------------------------------------- #
# (name, production route, signal, live price)
PULLBACK_LIMITS = [
    # the auditor's cases (parser finding 1)
    ("BTC long limit 72500 / SL 72100 / TP 73300, live 74000", "kripto",
     btc(LIMIT, entry_price=72500.0, stop_loss=72100.0, tps=(73300.0,)), 74000.0),
    ("BTC short limit 75500 / SL 75900 / TP 74700, live 74000", "kripto",
     btc(LIMIT, side=SHORT, entry_price=75500.0, stop_loss=75900.0, tps=(74700.0,)), 74000.0),
    ("EURUSD long limit 1.1570 / SL 1.1555 / TP 1.1595, live 1.1600", "fx",
     eur(LIMIT, entry_price=1.1570, stop_loss=1.1555, tps=(1.1595,)), 1.1600),
    ("EURUSD short limit 1.1630 / SL 1.1645 / TP 1.1605, live 1.1600", "fx",
     eur(LIMIT, side=SHORT, entry_price=1.1630, stop_loss=1.1645, tps=(1.1605,)), 1.1600),
    # two targets: the first one before today's price, the second one beyond it
    ("BTC long limit 72500, TP 73300 / 75000, live 74000", "kripto",
     btc(LIMIT, entry_price=72500.0, stop_loss=72100.0, tps=(73300.0, 75000.0)), 74000.0),
    # the price exactly on the first target
    ("BTC long limit 72500, TP 73300, live 73300", "kripto",
     btc(LIMIT, entry_price=72500.0, stop_loss=72100.0, tps=(73300.0,)), 73300.0),
]


@pytest.mark.parametrize("name,route_name,action,live", PULLBACK_LIMITS, ids=[c[0] for c in PULLBACK_LIMITS])
def test_p1_pullback_limit_with_the_target_before_the_price_is_not_doubtful(
    name: str, route_name: str, action: ParsedAction, live: float
) -> None:
    plan, issues = build(action, live, route_risk(route_name))
    assert codes(issues) == []                       # nothing to ask: the plan is automatic
    assert plan is not None and plan.entry_type is LIMIT and plan.entry_price == action.entry_price
    assert [tp.price for tp in plan.take_profits] == [tp.price for tp in action.take_profits]


def test_p1_market_plan_beyond_its_first_target_is_still_blocked() -> None:
    # explicit market (kripto: "ask"): the target is behind the fill -> blocking, as before
    plan, issues = build(btc(MARKET), 75600.0, route_risk("kripto"))
    assert plan is None
    assert [(i.code, i.blocking) for i in issues if i.code == "market_beyond_tp"] == [("market_beyond_tp", True)]
    # no entry type and the market chosen automatically (price near the entry, target closer still)
    plan, issues = build(btc(None, tps=(74100.0,)), 74200.0, route_risk("kripto"))
    assert plan is None and "market_beyond_tp" in codes(issues)


def test_p1_a_limit_the_signal_did_not_ask_for_keeps_the_question() -> None:
    # No entry type: the bot itself chose the limit because the price is far from the entry. A first target
    # that is already behind the price stays doubtful (the trader may have entered at market long ago).
    plan, issues = build(btc(None, entry_price=72500.0, stop_loss=72100.0, tps=(73300.0,)), 74000.0,
                         route_risk("kripto"))
    assert plan is not None and plan.entry_type is LIMIT
    assert [(i.code, i.blocking) for i in issues] == [("market_beyond_tp", False)]
    # fx route ("follow"): a MARKET signal whose price ran away beyond the first target becomes a limit, asked
    plan, issues = build(eur(MARKET, tps=(1.1650,)), 1.1660, route_risk("fx"))
    assert plan is not None and plan.entry_type is LIMIT
    assert [(i.code, i.blocking) for i in issues] == [("market_beyond_tp", False)]


def test_p1_other_checks_of_a_pullback_limit_still_apply() -> None:
    risk = route_risk("kripto")
    pullback = dict(entry_price=72500.0, stop_loss=72100.0, tps=(73300.0,))
    # entry more than max_entry_distance_pct (10%) from the price
    assert decide(btc(LIMIT, **pullback), 81000.0, risk) == (LIMIT, ["entry_far_from_market"])
    # a target on the wrong side of the entry is still a blocking error
    plan, issues = build(btc(LIMIT, entry_price=72500.0, stop_loss=72100.0, tps=(72400.0,)), 74000.0, risk)
    assert plan is None and "tp_wrong_side" in codes(issues)
    # a limit that is already marketable (price below the long entry) is still asked on the kripto route
    assert decide(btc(LIMIT, **pullback), 72300.0, risk) == (LIMIT, ["limit_entry_marketable"])
    # unknown price: asked, as before
    assert decide(btc(LIMIT, **pullback), None, risk) == (LIMIT, ["market_price_unknown"])
    # low confidence / no target: asked, as before
    assert decide(btc(LIMIT, confidence=0.5, **pullback), 74000.0, risk) == (LIMIT, ["low_confidence"])
    assert decide(btc(LIMIT, entry_price=72500.0, stop_loss=72100.0, tps=()), 74000.0, risk) == (LIMIT, ["no_take_profit"])


async def test_p1_engine_decides_a_pullback_limit_automatically_on_both_production_routes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Engine.start() sets this module global from the brokers' risk_usd: put it back afterwards
    monkeypatch.setattr(formatting, "RISK_PER_R_NOTE", formatting.RISK_PER_R_NOTE)
    cfg = prod_replica()
    engine, oracle, db = await start_engine(cfg)
    try:
        chat = int(cfg.telegram.source_chats[0])
        for name, route_name, action, live in PULLBACK_LIMITS:
            oracle.prices = {str(action.base_asset): live}
            ev = await engine.evaluate(action, chat, route=route(route_name, cfg))
            assert ev is not None and ev.kind == "open", name
            assert codes(ev.issues) == [] and ev.automatic, name
            assert isinstance(ev.plan, TradePlan) and ev.plan.entry_type is LIMIT, name
            assert ev.plan.broker_names == list(route(route_name, cfg).brokers), name
    finally:
        await engine.stop()
        await db.close()


# --------------------------------------------------------------------------- #
# P2: explicit MARKET signal that drifted, "ask" / "follow"
# --------------------------------------------------------------------------- #
# The complete table: route x explicit_market_drift x where the live price is.
# kripto BTC long: entry 74000, SL 73300, TP 75500; tolerance 0.5% (370) -> near = within 73630..74370.
# fx EURUSD long: entry 1.1620, SL 1.1600, TP 1.1680; tolerance 0.03% or 0.25R (5 pips) -> near = 1.1615..1.1625.
# (route, drift, situation, signal, live, expected entry type (None = no plan), expected issue codes)
DRIFT_TABLE: list[tuple[str, str, str, ParsedAction, float, EntryType | None, list[str]]] = [
    # ---- kripto, ask (the production setting of the kripto route) ----------------------------------------
    ("kripto", "ask", "near", btc(MARKET), 74200.0, MARKET, []),
    ("kripto", "ask", "ran away", btc(MARKET), 74500.0, MARKET, ["market_far_from_entry"]),
    ("kripto", "ask", "better price", btc(MARKET), 73500.0, MARKET, ["market_far_from_entry"]),
    ("kripto", "ask", "beyond stop", btc(MARKET), 73200.0, None, ["market_beyond_stop", "market_far_from_entry"]),
    ("kripto", "ask", "beyond TP1", btc(MARKET), 75600.0, None, ["market_far_from_entry", "market_beyond_tp"]),
    # ---- kripto, follow -------------------------------------------------------------------------------------
    ("kripto", "follow", "near", btc(MARKET), 74200.0, MARKET, []),
    ("kripto", "follow", "ran away", btc(MARKET), 74500.0, LIMIT, []),
    ("kripto", "follow", "better price", btc(MARKET), 73500.0, MARKET, []),          # was LIMIT + limit_entry_marketable
    ("kripto", "follow", "beyond stop", btc(MARKET), 73200.0, None, ["market_beyond_stop"]),
    ("kripto", "follow", "beyond TP1", btc(MARKET), 75600.0, LIMIT, ["market_beyond_tp"]),
    # ---- fx, ask ---------------------------------------------------------------------------------------------
    ("fx", "ask", "near", eur(MARKET), 1.1624, MARKET, []),
    ("fx", "ask", "ran away", eur(MARKET), 1.1630, MARKET, ["market_far_from_entry"]),
    ("fx", "ask", "better price", eur(MARKET), 1.1610, MARKET, ["market_far_from_entry"]),
    ("fx", "ask", "beyond stop", eur(MARKET), 1.1598, None, ["market_beyond_stop", "market_far_from_entry"]),
    ("fx", "ask", "beyond TP1", eur(MARKET), 1.1685, None, ["market_far_from_entry", "market_beyond_tp"]),
    # ---- fx, follow (the production setting of the fx route) -------------------------------------------------
    ("fx", "follow", "near", eur(MARKET), 1.1624, MARKET, []),
    ("fx", "follow", "ran away", eur(MARKET), 1.1630, LIMIT, []),
    ("fx", "follow", "better price", eur(MARKET), 1.1610, MARKET, []),               # was LIMIT (marketable, not asked)
    ("fx", "follow", "beyond stop", eur(MARKET), 1.1598, None, ["market_beyond_stop"]),
    ("fx", "follow", "beyond TP1", eur(MARKET), 1.1685, LIMIT, ["market_beyond_tp"]),
]
DRIFT_IDS = [f"{r}-{d}-{s.replace(' ', '_')}" for r, d, s, *_ in DRIFT_TABLE]


def test_p2_production_defaults_of_the_two_routes() -> None:
    assert route_risk("kripto").explicit_market_drift == "ask"
    assert route_risk("fx").explicit_market_drift == "follow"


@pytest.mark.parametrize("route_name,drift,situation,action,live,entry_type,want", DRIFT_TABLE, ids=DRIFT_IDS)
def test_p2_truth_table_ask_follow(
    route_name: str, drift: str, situation: str, action: ParsedAction, live: float,
    entry_type: EntryType | None, want: list[str],
) -> None:
    plan, issues = build(action, live, route_risk(route_name, explicit_market_drift=drift))
    assert (plan.entry_type if plan else None, codes(issues)) == (entry_type, want)
    if plan is not None:
        assert plan.entry_price == action.entry_price       # the plan always carries the signal's entry


@pytest.mark.parametrize("route_name,action,live", [
    ("kripto", btc_short(MARKET), 74500.0),      # short: a better price is ABOVE the entry (stop 74700)
    ("fx", eur_short(MARKET), 1.1630),           # short: 10 pips above the entry (stop 1.1640)
])
def test_p2_better_price_short_side_stays_market_under_follow(route_name: str, action: ParsedAction, live: float) -> None:
    assert decide(action, live, route_risk(route_name, explicit_market_drift="follow")) == (MARKET, [])
    # ... and the other direction (the price ran away below the short entry) is a resting limit
    away = 73500.0 if route_name == "kripto" else 1.1610
    assert decide(action, away, route_risk(route_name, explicit_market_drift="follow")) == (LIMIT, [])


def test_p2_better_price_still_respects_the_distance_limit() -> None:
    # stop 15% below, live 11% below the entry: better price, but beyond max_entry_distance_pct (10%)
    risk = route_risk("kripto", explicit_market_drift="follow")
    action = btc(MARKET, stop_loss=62900.0)
    assert decide(action, 65860.0, risk) == (MARKET, ["entry_far_from_market"])
    # fx: 1% limit; stop 150 pips below, live 120 pips (1.03%) below
    risk = route_risk("fx")
    assert decide(eur(MARKET, stop_loss=1.1470), 1.1500, risk) == (MARKET, ["entry_far_from_market"])


@pytest.mark.parametrize("entry_mode", ["auto", "market", "limit"])
def test_p2_better_price_stays_market_whatever_the_entry_mode(entry_mode: str) -> None:
    risk = route_risk("fx").model_copy(update={"entry_mode": entry_mode})
    assert decide(eur(MARKET), 1.1610, risk) == (MARKET, [])


def test_p2_signals_without_entry_type_and_limit_signals_are_untouched() -> None:
    follow = route_risk("kripto", explicit_market_drift="follow")
    # no entry type, better price: still the automatic choice (a limit that is already marketable -> asked)
    assert decide(btc(None), 73500.0, follow) == (LIMIT, ["limit_entry_marketable"])
    assert decide(btc(LIMIT), 73500.0, follow) == (LIMIT, ["limit_entry_marketable"])
    # fx: not asked, as before
    assert decide(eur(None), 1.1610, route_risk("fx")) == (LIMIT, [])
    assert decide(eur(LIMIT), 1.1610, route_risk("fx")) == (LIMIT, [])


async def test_p2_engine_truth_table_on_the_production_replica(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(formatting, "RISK_PER_R_NOTE", formatting.RISK_PER_R_NOTE)
    for drift in ("ask", "follow"):
        cfg = prod_replica(kripto={"explicit_market_drift": drift}, fx={"explicit_market_drift": drift})
        engine, oracle, db = await start_engine(cfg)
        try:
            chat = int(cfg.telegram.source_chats[0])
            for route_name, d, situation, action, live, entry_type, want in DRIFT_TABLE:
                if d != drift:
                    continue
                oracle.prices = {str(action.base_asset): live}
                ev = await engine.evaluate(action, chat, route=route(route_name, cfg))
                assert ev is not None and ev.kind == "open"
                got = (ev.plan.entry_type if isinstance(ev.plan, TradePlan) else None, codes(ev.issues), ev.automatic)
                assert got == (entry_type, want, not want), f"{route_name} {drift} {situation}: {got}"
        finally:
            await engine.stop()
            await db.close()


# --------------------------------------------------------------------------- #
# P3: implausible entry-to-stop distance
# --------------------------------------------------------------------------- #
def eth(et: EntryType | None, **kw: Any) -> ParsedAction:
    """ETHUSDT.P long as in the group's example screenshot: entry 2232.18, SL 2205.11 (1.21%), TP 2310.60."""
    data: dict[str, Any] = dict(symbol="ETHUSDT.P", base_asset="ETH", quote_asset="USDT", market_type=MarketType.PERP,
                                entry_price=2232.18, stop_loss=2205.11, tps=(2310.60,))
    data.update(kw)
    return _open(et, **data)


def xau(et: EntryType | None, **kw: Any) -> ParsedAction:
    """XAUUSD long, entry 4000, SL 3985 (0.375%), TP 4030."""
    data: dict[str, Any] = dict(symbol="XAUUSD", base_asset="XAU", quote_asset="USD", market_type=MarketType.CFD,
                                entry_price=4000.0, stop_loss=3985.0, tps=(4030.0,))
    data.update(kw)
    return _open(et, **data)


def usdjpy(et: EntryType | None, **kw: Any) -> ParsedAction:
    """USDJPY long, entry 154.20, SL 153.90 (30 pips, 0.19%), TP 155.00."""
    data: dict[str, Any] = dict(symbol="USDJPY", base_asset="USD", quote_asset="JPY", market_type=MarketType.FOREX,
                                entry_price=154.20, stop_loss=153.90, tps=(155.00,))
    data.update(kw)
    return _open(et, **data)


def stop_issue(action: ParsedAction, risk: RiskConfig, live: float | None = None, **kw: Any) -> ValidationIssue | None:
    """The stop-distance issue of ``action`` (live price = its entry unless given), None when there is none."""
    plan, issues = build(action, action.entry_price if live is None else live, risk, **kw)
    found = [i for i in issues if i.code in ("stop_too_tight", "stop_too_wide")]
    assert len(found) <= 1
    if found:
        assert found[0].blocking is False and plan is not None       # a question with an approvable plan
    return found[0] if found else None


def test_p3_misread_stops_of_the_audit_are_asked() -> None:
    # ETH long 2239.20 with the stop read as 2238.11 instead of 2205.11 (parser finding 8, B1)
    plan, issues = build(eth(MARKET, entry_price=2239.20, stop_loss=2238.11), 2239.20, route_risk("kripto"))
    assert plan is not None and codes(issues) == ["stop_too_tight"] and not issues[0].blocking
    assert issues[0].message_tr == (
        "Stop girişe çok yakın: giriş 2239.2, stop 2238.11 — aradaki mesafe %0.04868. Kripto için en az %0.15 "
        "beklenir; stop yanlış okunmuş olabilir (bu mesafeyle pozisyon çok büyük açılır).")
    # EURUSD long 1.16000 with the stop read as 1.15970 (3 pips) instead of 1.15700 (B2)
    plan, issues = build(eur(MARKET, entry_price=1.16, stop_loss=1.1597, tps=(1.166,)), 1.16, route_risk("fx"))
    assert plan is not None and codes(issues) == ["stop_too_tight"]
    assert issues[0].message_tr == (
        "Stop girişe çok yakın: giriş 1.16, stop 1.1597 — aradaki mesafe %0.02586 (3 pip). Döviz pariteleri için "
        "en az %0.03 beklenir; stop yanlış okunmuş olabilir (bu mesafeyle pozisyon çok büyük açılır).")
    # EURUSD long 1.16000 with the stop read as 1.10700 (530 pips) instead of 1.15700 (B3)
    plan, issues = build(eur(MARKET, entry_price=1.16, stop_loss=1.107, tps=(1.166,)), 1.16, route_risk("fx"))
    assert plan is not None and codes(issues) == ["stop_too_wide"] and not issues[0].blocking
    assert issues[0].message_tr == (
        "Stop girişten çok uzak: giriş 1.16, stop 1.107 — aradaki mesafe %4.569 (530 pip). Döviz pariteleri için "
        "en fazla %3 beklenir; stop yanlış okunmuş olabilir.")


# The group's real signals found in the audit evidence / the prompt's examples, and ordinary fx / CFD setups.
NORMAL_SIGNALS = [
    ("group: ETH long 2232.18 / 2205.11 (1.21%, the widest real stop found)", "kripto", eth(MARKET)),
    ("group: ETH short 2239.20 / 2250.60 (0.51%, the tightest real stop found)", "kripto",
     eth(MARKET, side=SHORT, entry_price=2239.20, stop_loss=2250.60, tps=(2205.11,))),
    ("group: BTC short 74868.8 / 75575 (0.94%)", "kripto",
     btc(MARKET, side=SHORT, entry_price=74868.8, stop_loss=75575.0, tps=(73095.0,))),
    ("BTC long 74000 / 73300 (0.95%)", "kripto", btc(MARKET)),
    ("BTC scalp, stop 0.2%", "kripto", btc(MARKET, stop_loss=73852.0)),
    ("SOL swing, stop 12%", "kripto",
     _open(MARKET, symbol="SOLUSDT.P", base_asset="SOL", quote_asset="USDT", market_type=MarketType.PERP,
           entry_price=160.0, stop_loss=140.8, tps=(200.0,))),
    ("EURUSD 20 pips (0.17%)", "fx", eur(MARKET)),
    ("EURUSD 5 pips (0.043%)", "fx", eur(MARKET, stop_loss=1.1615)),
    ("EURUSD 30 pips (0.26%)", "fx", eur(MARKET, entry_price=1.16, stop_loss=1.157, tps=(1.166,))),
    ("EURUSD swing 250 pips (2.2%)", "fx", eur(MARKET, stop_loss=1.1370)),
    ("USDJPY 30 pips (0.19%)", "fx", usdjpy(MARKET)),
    ("XAUUSD 15 USD (0.375%)", "fx", xau(MARKET)),
    ("XAUUSD 4 USD (0.1%)", "fx", xau(MARKET, stop_loss=3996.0)),
    ("XAUUSD 120 USD (3%)", "fx", xau(MARKET, stop_loss=3880.0)),
    ("NAS100 100 points (0.4%)", "fx",
     _open(MARKET, symbol="NAS100", base_asset="NAS100", quote_asset="USD", market_type=MarketType.CFD,
           entry_price=25000.0, stop_loss=24900.0, tps=(25300.0,))),
    ("BTCUSD as a CFD in the fx topic (0.95%)", "fx",
     btc(MARKET, symbol="BTCUSD", quote_asset="USD", market_type=MarketType.CFD)),
]


@pytest.mark.parametrize("name,route_name,action", NORMAL_SIGNALS, ids=[c[0] for c in NORMAL_SIGNALS])
def test_p3_normal_signals_are_never_asked_about_their_stop(name: str, route_name: str, action: ParsedAction) -> None:
    plan, issues = build(action, action.entry_price, route_risk(route_name))
    assert plan is not None and codes(issues) == []


def _with_stop_pct(action: ParsedAction, pct: float) -> ParsedAction:
    """``action`` with its stop ``pct`` percent of the entry away, on the correct side."""
    entry = float(action.entry_price or 0.0)
    delta = entry * pct / 100.0
    return action.model_copy(update={"stop_loss": entry - delta if action.side is LONG else entry + delta})


@pytest.mark.parametrize("route_name,action,lo,hi", [
    ("kripto", btc(MARKET), 0.15, 25.0),
    ("fx", eur(MARKET), 0.03, 3.0),
    ("fx", usdjpy(MARKET), 0.03, 3.0),
    ("fx", xau(MARKET), 0.05, 8.0),
], ids=["crypto", "forex", "forex-jpy", "cfd"])
def test_p3_bounds_per_instrument_class(route_name: str, action: ParsedAction, lo: float, hi: float) -> None:
    risk = route_risk(route_name)
    wide_tp = action.model_copy(update={"take_profits": [TakeProfit(price=float(action.entry_price or 0) * 1.5)]})

    def code(pct: float) -> str | None:
        issue = stop_issue(_with_stop_pct(wide_tp, pct), risk)
        return issue.code if issue else None

    assert code(lo * 0.9) == "stop_too_tight"
    assert code(lo * 1.1) is None
    assert code((lo * hi) ** 0.5) is None
    assert code(hi * 0.95) is None
    assert code(hi * 1.05) == "stop_too_wide"
    short = wide_tp.model_copy(update={"side": SHORT, "take_profits": [TakeProfit(price=float(action.entry_price or 0) * 0.5)]})
    assert stop_issue(_with_stop_pct(short, lo * 0.9), risk).code == "stop_too_tight"       # type: ignore[union-attr]
    assert stop_issue(_with_stop_pct(short, hi * 1.05), risk).code == "stop_too_wide"       # type: ignore[union-attr]
    assert stop_issue(_with_stop_pct(short, lo * 1.1), risk) is None


def test_p3_a_stop_exactly_on_a_bound_is_not_asked() -> None:
    risk = route_risk("kripto")
    # 100 -> 99.85 is 0.15% and 100 -> 75 is 25%: on the bound despite float rounding
    coin = dict(symbol="XYZUSDT.P", base_asset="XYZ", quote_asset="USDT", market_type=MarketType.PERP, entry_price=100.0)
    assert stop_issue(_open(MARKET, stop_loss=99.85, tps=(150.0,), **coin), risk) is None
    assert stop_issue(_open(MARKET, stop_loss=75.0, tps=(150.0,), **coin), risk) is None
    assert stop_issue(_open(MARKET, stop_loss=99.86, tps=(150.0,), **coin), risk).code == "stop_too_tight"  # type: ignore[union-attr]
    assert stop_issue(_open(MARKET, stop_loss=74.9, tps=(150.0,), **coin), risk).code == "stop_too_wide"    # type: ignore[union-attr]


def test_p3_instrument_class_selection() -> None:
    risk = RiskConfig()                                   # no route: class from the market type / the asset
    tight_fx = dict(entry_price=1.16, stop_loss=1.1590, tps=(1.17,))        # 0.086%: fine for forex, tight for crypto
    # market type perp / spot -> crypto bounds
    for mt in (MarketType.PERP, MarketType.SPOT):
        issue = stop_issue(_open(MARKET, symbol="EURUSDT", base_asset="EUR", quote_asset="USDT", market_type=mt, **tight_fx), risk)
        assert issue is not None and issue.code == "stop_too_tight" and "Kripto için en az %0.15" in issue.message_tr
    # a currency pair -> forex bounds, whatever market type the model wrote (forex, cfd or none)
    for mt in (MarketType.FOREX, MarketType.CFD, None):
        assert stop_issue(_open(MARKET, symbol="EURUSD", base_asset="EUR", quote_asset="USD", market_type=mt, **tight_fx), risk) is None
    # the route's kind wins: the same levels in a crypto topic are a crypto signal
    assert stop_issue(_open(MARKET, symbol="EUR", base_asset="EUR", **tight_fx), risk, route_kind="crypto") is not None
    # metals, indices and oil -> CFD bounds (0.05% .. 8%), also when the model wrote "forex" or nothing
    gold = dict(symbol="XAUUSD", base_asset="XAU", quote_asset="USD", entry_price=4000.0, stop_loss=3998.4, tps=(4030.0,))  # 0.04%
    for mt in (MarketType.CFD, MarketType.FOREX, None):
        issue = stop_issue(_open(MARKET, market_type=mt, **gold), risk)
        assert issue is not None and "Altın, endeks, petrol gibi ürünler için en az %0.05" in issue.message_tr
        assert "pip" not in issue.message_tr                     # pips are shown for currency pairs only
    assert stop_issue(_open(MARKET, market_type=None, **{**gold, "stop_loss": 3997.0}), risk) is None           # 0.075%
    oil = dict(symbol="USOIL", base_asset="USOIL", entry_price=60.0, stop_loss=54.0, tps=(70.0,))                # 10%
    assert stop_issue(_open(MARKET, market_type=None, **oil), risk).code == "stop_too_wide"    # type: ignore[union-attr]
    # an unknown ticker without a market type is taken as crypto (10% is fine there); in an fx topic as a CFD
    unknown = dict(symbol="XYZ", base_asset="XYZ", entry_price=60.0, stop_loss=54.0, tps=(70.0,))
    assert stop_issue(_open(MARKET, market_type=None, **unknown), risk) is None
    assert stop_issue(_open(MARKET, market_type=None, **unknown), risk, route_kind="fx").code == "stop_too_wide"  # type: ignore[union-attr]


def test_p3_pips_in_the_question() -> None:
    risk = route_risk("fx")
    # JPY pairs: one pip is 0.01
    issue = stop_issue(usdjpy(MARKET, stop_loss=154.17), risk)
    assert issue is not None and issue.code == "stop_too_tight"
    assert "giriş 154.2, stop 154.17 — aradaki mesafe %0.01946 (3 pip)" in issue.message_tr
    issue = stop_issue(usdjpy(MARKET, stop_loss=148.20), risk)
    assert issue is not None and issue.code == "stop_too_wide" and "(600 pip)" in issue.message_tr
    # half pips are kept
    issue = stop_issue(eur(MARKET, stop_loss=1.16175), risk)
    assert issue is not None and "(2.5 pip)" in issue.message_tr
    # a quote the pip size is not obvious for: percent only
    exotic = _open(MARKET, symbol="USDTRY", base_asset="USD", quote_asset="TRY", market_type=MarketType.FOREX,
                   entry_price=42.0, stop_loss=41.995, tps=(43.0,))
    issue = stop_issue(exotic, risk)
    assert issue is not None and "pip" not in issue.message_tr


def test_p3_market_entry_without_a_price_uses_the_live_price() -> None:
    risk = route_risk("kripto")
    no_entry = btc(MARKET, entry_price=None, stop_loss=73990.0)        # live 74000: 0.0135%
    issue = stop_issue(no_entry, risk, live=74000.0)
    assert issue is not None and issue.code == "stop_too_tight"
    assert issue.message_tr == (
        "Stop anlık fiyata çok yakın: anlık fiyat 74000, stop 73990 — aradaki mesafe %0.01351. Kripto için en az "
        "%0.15 beklenir; stop yanlış okunmuş olabilir (bu mesafeyle pozisyon çok büyük açılır).")
    wide = stop_issue(btc(MARKET, entry_price=None, stop_loss=50000.0), risk, live=74000.0)
    assert wide is not None and wide.message_tr.startswith("Stop anlık fiyattan çok uzak: anlık fiyat 74000, stop 50000 —")
    assert stop_issue(btc(MARKET, entry_price=None), risk, live=74000.0) is None
    # no entry and no live price: blocked for another reason, no crash
    plan, issues = build(no_entry, None, risk)
    assert plan is None and "price_unknown" in codes(issues)


def test_p3_a_stop_on_the_wrong_side_is_only_blocked() -> None:
    plan, issues = build(btc(MARKET, stop_loss=74000.5), 74000.0, route_risk("kripto"))
    assert plan is None and codes(issues) == ["sl_wrong_side", "market_beyond_stop"]      # no stop-distance issue


def test_p3_bounds_come_from_the_risk_config_when_it_has_the_fields() -> None:
    base = route_risk("kripto")
    action = eth(MARKET, entry_price=2239.20, stop_loss=2238.11)         # 0.049%
    assert stop_issue(action, base) is not None
    # 0 switches a bound off; a custom value replaces the default
    assert stop_issue(action, base.model_copy(update={"min_stop_distance_pct_crypto": 0})) is None
    assert stop_issue(action, base.model_copy(update={"min_stop_distance_pct_crypto": 0.04})) is None
    assert stop_issue(eth(MARKET), base.model_copy(update={"min_stop_distance_pct_crypto": 1.5})).code == "stop_too_tight"  # type: ignore[union-attr]
    assert stop_issue(eth(MARKET), base.model_copy(update={"max_stop_distance_pct_crypto": 1.0})).code == "stop_too_wide"   # type: ignore[union-attr]
    wide = btc(MARKET, stop_loss=50000.0)                                 # 32%
    assert stop_issue(wide, base).code == "stop_too_wide"                # type: ignore[union-attr]
    assert stop_issue(wide, base.model_copy(update={"max_stop_distance_pct_crypto": 0})) is None
    # each class has its own pair of fields
    fx = route_risk("fx")
    tight_eur = eur(MARKET, stop_loss=1.1618)                             # 2 pips
    assert stop_issue(tight_eur, fx.model_copy(update={"min_stop_distance_pct_crypto": 0})) is not None
    assert stop_issue(tight_eur, fx.model_copy(update={"min_stop_distance_pct_fx": 0})) is None
    assert stop_issue(xau(MARKET, stop_loss=3999.0), fx.model_copy(update={"min_stop_distance_pct_fx": 0})) is not None
    assert stop_issue(xau(MARKET, stop_loss=3999.0), fx.model_copy(update={"min_stop_distance_pct_cfd": 0})) is None
    assert stop_issue(xau(MARKET, stop_loss=3500.0), fx.model_copy(update={"max_stop_distance_pct_cfd": 20})) is None
    # an unset (None) value keeps the default
    assert stop_issue(action, base.model_copy(update={"min_stop_distance_pct_crypto": None})) is not None


async def test_p3_engine_asks_instead_of_opening_on_the_production_replica(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(formatting, "RISK_PER_R_NOTE", formatting.RISK_PER_R_NOTE)
    cfg = prod_replica()
    engine, oracle, db = await start_engine(cfg)
    try:
        chat = int(cfg.telegram.source_chats[0])
        cases = [
            ("kripto", eth(MARKET, entry_price=2239.20, stop_loss=2238.11), ["stop_too_tight"]),
            ("fx", eur(MARKET, entry_price=1.16, stop_loss=1.1597, tps=(1.166,)), ["stop_too_tight"]),
            ("fx", eur(MARKET, entry_price=1.16, stop_loss=1.107, tps=(1.166,)), ["stop_too_wide"]),
            ("kripto", eth(MARKET), []),
            ("fx", eur(MARKET), []),
        ]
        for route_name, action, want in cases:
            oracle.prices = {str(action.base_asset): float(action.entry_price or 0)}
            ev = await engine.evaluate(action, chat, route=route(route_name, cfg))
            assert ev is not None and isinstance(ev.plan, TradePlan)          # approvable: a question, not a refusal
            assert codes(ev.issues) == want and ev.automatic is (not want)
    finally:
        await engine.stop()
        await db.close()


# --------------------------------------------------------------------------- #
# P7: the plan summary and the risk percentage
# --------------------------------------------------------------------------- #
def test_p7_summary_leaves_the_percentage_out_when_it_does_not_apply() -> None:
    risk = route_risk("kripto")
    plan, _ = build(btc(MARKET), 74000.0, risk, route="kripto", broker_names=["binance", "breakout", "tickmill"],
                    risk_pct_applies=False)
    assert plan is not None
    assert plan.summary_tr == ("BTC LONG (piyasa), giriş 74000, SL 73300, TP 75500, risk 1R · konu kripto → "
                               "binance, breakout, tickmill")
    assert plan.risk_pct == 2.0                              # the field itself is unchanged (percent brokers use it)
    # default: unchanged text
    plan, _ = build(btc(MARKET), 74000.0, risk, route="kripto", broker_names=["binance", "breakout", "tickmill"])
    assert plan is not None
    assert plan.summary_tr == ("BTC LONG (piyasa), giriş 74000, SL 73300, TP 75500, risk 1R (%2) · konu kripto → "
                               "binance, breakout, tickmill")


def test_p7_the_too_much_risk_question_follows_the_same_rule() -> None:
    risk = route_risk("kripto")
    _, issues = build(btc(MARKET, risk_r=3.0), 74000.0, risk, risk_pct_applies=False)
    assert [i.message_tr for i in issues if i.code == "risk_r_too_high"] == [
        "Sinyal riski 3R — izin verilen en fazla 2R. Onaylanırsa 3R ile açılır."]
    _, issues = build(btc(MARKET, risk_r=3.0), 74000.0, risk)
    assert [i.message_tr for i in issues if i.code == "risk_r_too_high"] == [
        "Sinyal riski 3R (%6) — izin verilen en fazla 2R. Onaylanırsa 3R ile açılır."]


# --------------------------------------------------------------------------- #
# P4: the prefilter and short management texts
# --------------------------------------------------------------------------- #
from copytrader.models import StoredMessage  # noqa: E402
from copytrader.parsing.prefilter import looks_relevant  # noqa: E402


def relevant(text: str, **kw: Any) -> bool:
    return looks_relevant(StoredMessage(chat_id=-100, message_id=1, date=NOW, text=text, **kw))


# The auditor's list (parser finding 5): the 33 texts of its "EDGE" list that were dropped before this change ...
DROPPED_BEFORE = [
    "Geldi", "Geldi ✅", "✅", "🎯", "Tamamdır", "Patladı", "Patladık", "Başa baş", "Başabaş yaptık", "Sıfırladık",
    "Tetiklendi", "Açtık", "Açıldı", "Yedik", "Bitti", "Tuttu", "Olmadı", "Geçersiz", "Devam", "Bekliyoruz",
    "Taşıdık", "Kilitledik", "Azalttık", "Küçülttük", "Bırakın", "Dokunmayın", "Tut", "Düzeltme", "Yanlış oldu",
    "Tersine döndük", "done", "hit", "filled",
]
# ... and the 18 that passed already (they must keep passing).
PASSED_BEFORE = [
    "Doldu", "Bozuldu", "Sildik", "Yukarı aldık", "Elden çıkardık", "Sat", "Al", "Ekleme yapıyoruz", "Yarım",
    "Tamamını", "Pardon short olacaktı", "Maliyet", "closed", "out", "cancelled", "be", "sl", "tp1 ✅",
]
# Turkish typing variants: upper case with dotted / dotless i, plain ASCII letters, other suffixes, a filler word.
VARIANTS = [
    "GELDİ", "geldi", "Geldik", "Gelmedi", "TAMAMDIR", "tamamdir", "Tamamdır ✅", "Tamamdır arkadaşlar",
    "PATLADIK", "patladi", "Patlattı", "BAŞA BAŞ", "basa bas", "başabaş", "BAŞABAŞ", "Başabaşa aldık",
    "SIFIRLADIK", "sifirladik", "Sıfırlayın", "Sıfırlandı", "TETİKLENDİ", "tetiklendi", "Tetiklenmedi",
    "AÇTIK", "actik", "Açtım", "AÇILDI", "acildi", "Açılmadı", "YEDİK", "yedik", "BİTTİ", "bitti", "Bitirdik",
    "TUTTU", "tuttu", "Tutuyoruz", "Tutun", "OLMADI", "olmadi", "Yine olmadı", "GEÇERSİZ", "gecersiz",
    "Geçersiz oldu", "TAŞIDIK", "tasidik", "Taşıyoruz", "KİLİTLEDİK", "kilitledik", "Kilitleyin", "AZALTTIK",
    "azalttik", "Azaltın", "KÜÇÜLTTÜK", "kuculttuk", "Küçültün", "TERSİNE DÖNDÜK", "tersine donduk",
    "Tersine dönüyoruz", "Bekleyin", "Bekle", "Bırak", "Bıraktık", "Dokunma", "DÜZELTME", "duzeltme", "Düzelttim",
    "Yanlış", "yanlis oldu", "DONE", "Done ✅", "Hit", "Filled", "FILLED", "✅✅", "🎯🎯🎯", "✔️", "Sonunda geldi",
    "Geldi arkadaşlar", "Açıldı ✅", "Bitti 🎯",
]


@pytest.mark.parametrize("text", DROPPED_BEFORE + VARIANTS)
def test_p4_short_management_texts_reach_the_model(text: str) -> None:
    assert relevant(text) is True


@pytest.mark.parametrize("text", PASSED_BEFORE)
def test_p4_texts_that_passed_before_still_pass(text: str) -> None:
    assert relevant(text) is True


# Chatter: the 26 texts the existing prefilter tests pin as "not relevant" plus 70 everyday group lines, among
# them sentences that merely contain one of the new status words. Measured with the same lists before and
# after this change: 0 of 96 reach the model, both times.
CHATTER = [
    "günaydın 🌞", "bugün piyasa çok sakin", "harika gün", "", "   ", "🚀🚀🚀", "👍", "selam millet", "iyi akşamlar",
    "teşekkürler hocam", "nasılsınız", "GÜNAYDIN", "it's a nice day",
    "yarın görüşürüz", "yarına kadar bekleyin", "tamamdır hocam", "tamam", "eyvallah", "hayırlı cumalar",
    "çok güzel analiz", "ellerine sağlık", "hepinize iyi geceler", "merhaba arkadaşlar", "slm arkadaşlar",
    "kolay gelsin", "geçmiş olsun",
    "günaydın arkadaşlar", "herkese iyi haftalar", "hayırlı sabahlar", "iyi geceler", "selamlar", "merhaba", "sa", "as",
    "teşekkürler", "çok teşekkürler hocam", "sağ olun", "Allah razı olsun", "emeğinize sağlık", "eline sağlık abi",
    "süpersiniz", "harikasınız", "bravo", "helal olsun", "çok iyi", "aynen", "evet", "hayır", "olur", "peki",
    "anladım", "tamamdır abi", "ok", "okey hocam", "👏👏", "🙏", "😂😂", "❤️", "🔥🔥🔥", "💪", "😊",
    "hocam bugün yayın var mı", "akşam görüşürüz", "yarın konuşuruz", "bugün yokum", "geç kaldım kusura bakmayın",
    "ne zaman başlıyoruz", "nerede kaldık", "bekliyoruz hocam", "hocam bekliyoruz", "devam edelim mi hocam",
    "kargo geldi mi", "bugün maç bitti mi", "yemek yedik geliyoruz", "hocam yanlış anlamayın", "tut ki öyle olmadı",
    "dün gece çok güldük", "piyasa bugün çok sıkıcı", "haberler kötü", "fed konuşacak akşam", "veri bekleniyor",
    "sabırlı olun arkadaşlar", "herkes hazır mı", "bayramınız kutlu olsun", "iyi tatiller", "hoş geldiniz",
    "aramıza hoş geldin", "kolay gelsin herkese", "geçmiş olsun hocam", "çok güzel oldu", "bence de", "haklısınız",
    "katılıyorum", "görüşürüz", "hadi bakalım", "inşallah",
]


def test_p4_chatter_is_still_dropped() -> None:
    assert len(CHATTER) == 96
    assert [t for t in CHATTER if relevant(t)] == []


@pytest.mark.parametrize("text", [
    "tamamdır hocam", "tamam", "tamamen katılıyorum",          # "Tamamdır" counts only as the whole message
    "kargo geldi mi", "dün akşam misafir geldi biraz geç kaldım", "olmadı mı", "bitti mi",
    "hocam bekliyoruz", "devam edelim mi hocam", "tut ki öyle olmadı",
    "tam tersine bence", "yeni başa başladık",                 # not "tersine döndük" / "başa baş"
    "👍", "🚀🚀🚀", "🔥", "😂", "🙏🙏",                           # emoji that are not a check mark / target
])
def test_p4_everyday_words_inside_other_sentences_stay_chatter(text: str) -> None:
    assert relevant(text) is False


def test_p4_photo_is_relevant_whatever_its_text() -> None:
    assert relevant("", media_type="photo") is True                # also when its download failed (no media_path)
    assert relevant("günaydın", media_type="photo", media_path="/x.jpg") is True


# --------------------------------------------------------------------------- #
# P5 / P6: the LLM parser (no network: a scripted client, or the real SDK behind a mock transport)
# --------------------------------------------------------------------------- #
import asyncio  # noqa: E402
import json  # noqa: E402
from types import SimpleNamespace  # noqa: E402

import anthropic  # noqa: E402
import httpx2  # noqa: E402

from copytrader.config import NotifierConfig, TelegramConfig  # noqa: E402
from copytrader.engine.orchestrator import is_transient_parse_error  # noqa: E402
from copytrader.errors import ParserError  # noqa: E402
from copytrader.models import ParseContext, Severity  # noqa: E402
from copytrader.parsing import llm_parser, prompts  # noqa: E402
from copytrader.parsing.llm_parser import ClaudeSignalParser  # noqa: E402

CHAT = -1001234567890
SENDER = 777
JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 200
NOT_DOWNLOADED = prompts.IMAGE_UNREADABLE_NOTE.format(reason=prompts.IMAGE_NOT_DOWNLOADED_REASON) \
    if hasattr(prompts, "IMAGE_NOT_DOWNLOADED_REASON") else "<missing constant>"


def wire(**kw: Any) -> dict[str, Any]:
    base: dict[str, Any] = dict(
        action="none", symbol=None, base_asset=None, quote_asset=None, market_type=None, side=None,
        entry_type=None, entry_price=None, stop_loss=None, take_profits=[], risk_r=None,
        new_stop_loss=None, move_sl_to_entry=False, close_fraction=None, refers_to_message_id=None,
        target_trade_id=None, used_message_ids=[], evidence="", ambiguities=[], confidence=0.9,
        summary_tr="",
    )
    base.update(kw)
    return base


ETH_OPEN_WIRE = wire(action="open", symbol="ETHUSDT.P", base_asset="ETH", quote_asset="USDT", market_type="perp",
                     side="long", entry_type="market", entry_price=2232.18, stop_loss=2205.11,
                     take_profits=[{"price": 2310.6, "close_fraction": 1.0}], risk_r=1.0, used_message_ids=[501, 502],
                     confidence=0.95, summary_tr="ETH LONG, giriş 2232.18, SL 2205.11, TP 2310.60, risk 1R")


def reply(payload: Any = None, stop_reason: str | None = "end_turn", stop_details: Any = None, text: str | None = None) -> Any:
    """A Messages API response object as the SDK returns it (only what the parser reads)."""
    body = text if text is not None else (None if payload is None else json.dumps(payload))
    content = [] if body is None else [SimpleNamespace(type="text", text=body)]
    return SimpleNamespace(content=content, stop_reason=stop_reason, stop_details=stop_details, model="claude-opus-5-5")


MAX_TOKENS = reply({"actions": [ETH_OPEN_WIRE]}, stop_reason="max_tokens")
REFUSAL = reply(None, stop_reason="refusal", stop_details=SimpleNamespace(category="cyber", recommended_model=None))


class ScriptedClient:
    """Fake AsyncAnthropic: answers with the scripted items in order (the last one repeats); an exception is raised."""

    def __init__(self, *items: Any) -> None:
        self.items, self.requests = list(items), []
        self.beta = SimpleNamespace(messages=SimpleNamespace(create=self.create))

    async def create(self, **request: Any) -> Any:
        self.requests.append(request)
        item = self.items.pop(0) if len(self.items) > 1 else self.items[0]
        if isinstance(item, BaseException):
            raise item
        return item


def parser_for(*items: Any, **cfg: Any) -> tuple[ClaudeSignalParser, ScriptedClient]:
    client = ScriptedClient(*items)
    return ClaudeSignalParser(LLMConfig(**cfg), client=client), client


def tmsg(mid: int, text: str = "", **kw: Any) -> StoredMessage:
    return StoredMessage(chat_id=CHAT, message_id=mid, sender_id=SENDER, sender_name="Trader",
                         date=datetime(2026, 11, 12, 12, 0, mid % 60, tzinfo=timezone.utc), text=text, **kw)


def user_texts(content: list[dict[str, Any]]) -> list[str]:
    return [b["text"] for b in content if b["type"] == "text"]


# ---- P5 ----------------------------------------------------------------------
async def test_p5_photo_whose_download_failed_is_announced_to_the_model() -> None:
    parser, _ = parser_for(reply({"actions": [wire()]}))
    # the listener stores media_type "photo" and no file when the download fails
    content = await parser._build_content([tmsg(501, media_type="photo"), tmsg(502, "eth 1R long")], ParseContext(now=NOW))
    texts = user_texts(content)
    first = next(t for t in texts if t.startswith("[msg_id=501 "))
    assert first.endswith("(metin yok)\n" + NOT_DOWNLOADED)
    assert NOT_DOWNLOADED == ("[This message has an image that could not be attached (it could not be downloaded "
                              "from Telegram).]")
    assert not any(b["type"] == "image" for b in content)
    second = next(t for t in texts if t.startswith("[msg_id=502 "))
    assert "could not be attached" not in second                       # a plain text message gets no note


async def test_p5_image_document_and_replied_to_message_get_the_note_too() -> None:
    parser, _ = parser_for(reply({"actions": [wire()]}))
    content = await parser._build_content([tmsg(601, "eth 1R long", media_type="image_document")], ParseContext(now=NOW))
    assert NOT_DOWNLOADED in next(t for t in user_texts(content) if t.startswith("[msg_id=601 "))
    # the replied-to signal's screenshot was never downloaded
    ctx = ParseContext(now=NOW, reply_to=tmsg(500, "", media_type="photo"))
    content = await parser._build_content([tmsg(700, "Stop", reply_to_message_id=500)], ctx)
    replied = next(t for t in user_texts(content) if t.startswith(prompts.REPLY_TO_HEADER))
    assert "[msg_id=500 " in replied and replied.endswith(NOT_DOWNLOADED)


async def test_p5_a_downloaded_photo_is_attached_without_a_note(tmp_path: Any) -> None:
    path = tmp_path / "chart.jpg"
    path.write_bytes(JPEG)
    parser, _ = parser_for(reply({"actions": [wire()]}))
    content = await parser._build_content([tmsg(501, media_type="photo", media_path=str(path))], ParseContext(now=NOW))
    assert [b["type"] for b in content].count("image") == 1
    assert not any("could not be attached" in t for t in user_texts(content))


async def test_p5_missing_photos_do_not_use_the_image_budget(tmp_path: Any) -> None:
    path = tmp_path / "chart.jpg"
    path.write_bytes(JPEG)
    parser, _ = parser_for(reply({"actions": [wire()]}), max_images=1)
    msgs = [tmsg(501, media_type="photo", media_path=str(path)), tmsg(502, media_type="photo"), tmsg(503, "eth 1R long")]
    content = await parser._build_content(msgs, ParseContext(now=NOW))
    assert [b["type"] for b in content].count("image") == 1               # the older, readable one is still attached
    assert NOT_DOWNLOADED in next(t for t in user_texts(content) if t.startswith("[msg_id=502 "))


def test_p5_system_prompt_says_what_such_a_note_means() -> None:
    sp = prompts.SYSTEM_PROMPT
    assert "could not be attached: that message HAS a picture, but you cannot see it" in sp
    assert 'return "incomplete" with what the text gives' in sp
    assert "grafik görseli okunamadı" in sp


IMAGE_DOUBT = "Mesajdaki grafik görseli okunamadı (indirilemedi ya da açılamadı); seviyeler görselden doğrulanamadı."


async def test_p5_owner_is_told_that_the_screenshot_was_missing() -> None:
    msgs = [tmsg(501, media_type="photo"), tmsg(502, "eth 1R long")]
    # the model answers "incomplete" without mentioning the picture: the parser adds the reason itself
    incomplete = wire(action="incomplete", base_asset="ETH", side="long", risk_r=1.0, used_message_ids=[501, 502],
                      confidence=0.8, summary_tr="ETH LONG işlemi hazırlanıyor, bilgiler eksik")
    parser, _ = parser_for(reply({"actions": [incomplete]}))
    (action,) = (await parser.parse(msgs, ParseContext(now=NOW))).actions
    assert action.action is ActionType.INCOMPLETE and action.ambiguities == [IMAGE_DOUBT]
    # the same for an "open" built on a message whose picture nobody could see: asked, never automatic
    parser, _ = parser_for(reply({"actions": [ETH_OPEN_WIRE]}))
    (action,) = (await parser.parse(msgs, ParseContext(now=NOW))).actions
    assert action.action is ActionType.OPEN and action.ambiguities == [IMAGE_DOUBT]
    plan, issues = build(action, 2232.5, RiskConfig())
    assert plan is not None and [(i.code, i.message_tr) for i in issues] == [("ambiguity", IMAGE_DOUBT)]
    # the model already said so itself: not repeated
    said = dict(incomplete, ambiguities=["Grafik görseli okunamadı; seviyeler eksik."])
    parser, _ = parser_for(reply({"actions": [said]}))
    (action,) = (await parser.parse(msgs, ParseContext(now=NOW))).actions
    assert action.ambiguities == ["Grafik görseli okunamadı; seviyeler eksik."]


async def test_p5_no_image_doubt_without_a_missing_picture_or_for_other_actions(tmp_path: Any) -> None:
    path = tmp_path / "chart.jpg"
    path.write_bytes(JPEG)
    good = [tmsg(501, media_type="photo", media_path=str(path)), tmsg(502, "eth 1R long")]
    parser, _ = parser_for(reply({"actions": [ETH_OPEN_WIRE]}))
    (action,) = (await parser.parse(good, ParseContext(now=NOW))).actions
    assert action.action is ActionType.OPEN and action.ambiguities == []
    # an open that was built from other messages only
    parser, _ = parser_for(reply({"actions": [dict(ETH_OPEN_WIRE, used_message_ids=[502])]}))
    msgs = [tmsg(400, media_type="photo"), tmsg(502, "eth long 2232.18 stop 2205.11 tp 2310.6 1R")]
    (action,) = (await parser.parse(msgs, ParseContext(now=NOW))).actions
    assert action.used_message_ids == [502] and action.ambiguities == []
    # a management reply to a signal whose screenshot is missing is not a new trade: nothing added
    close = wire(action="close", base_asset="ETH", close_fraction=1.0, refers_to_message_id=500, used_message_ids=[700],
                 confidence=0.9, summary_tr="ETH işlemi kapatılacak")
    parser, _ = parser_for(reply({"actions": [close]}))
    ctx = ParseContext(now=NOW, reply_to=tmsg(500, "", media_type="photo"))
    (action,) = (await parser.parse([tmsg(700, "Stop", reply_to_message_id=500)], ctx)).actions
    assert action.action is ActionType.CLOSE and action.ambiguities == []


# ---- P6 ----------------------------------------------------------------------
async def parse_error(item: Any) -> ParserError:
    parser, _ = parser_for(item)
    with pytest.raises(ParserError) as ei:
        await parser.parse([tmsg(1, "eth 1R long")], ParseContext(now=NOW))
    return ei.value


@pytest.mark.parametrize("item,text", [
    (MAX_TOKENS, "Yapay zekâ yanıtı yarıda kesildi (token limiti); sinyal okunamadı."),
    (reply(None), "Yapay zekâ boş yanıt döndürdü; sinyal okunamadı."),
    (reply(text="   "), "Yapay zekâ boş yanıt döndürdü; sinyal okunamadı."),
    (reply(text="{not json"), "Yapay zekâ geçersiz bir yanıt döndürdü; sinyal okunamadı."),
    (reply(text=json.dumps({"actions": [ETH_OPEN_WIRE]})[:200]), "Yapay zekâ geçersiz bir yanıt döndürdü; sinyal okunamadı."),
    (reply({"actions": [{"action": "open"}]}), "Yapay zekâ geçersiz bir yanıt döndürdü; sinyal okunamadı."),
    (reply(text=json.dumps({"actions": [wire()]}).replace("0.9", "NaN")), "Yapay zekâ geçersiz bir yanıt döndürdü; sinyal okunamadı."),
    ("<html>proxy</html>", "Yapay zekâ servisinden okunamayan bir yanıt geldi; sinyal okunamadı."),
    (None, "Yapay zekâ servisinden okunamayan bir yanıt geldi; sinyal okunamadı."),
    (json.JSONDecodeError("Expecting value", "", 0), "Yapay zekâ servisinden okunamayan bir yanıt geldi; sinyal okunamadı."),
], ids=["max_tokens", "no-text-block", "blank-text", "invalid-json", "cut-json", "schema-mismatch", "nan",
        "html-body", "no-body", "sdk-json-error"])
async def test_p6_cut_empty_and_unparsable_replies_are_transient(item: Any, text: str) -> None:
    err = await parse_error(item)
    assert err.transient is True and str(err) == text
    assert is_transient_parse_error(err) is True              # what the engine's retry loop asks


@pytest.mark.parametrize("item", [
    REFUSAL,
    reply({"actions": [wire()]}, stop_reason="pause_turn"),
    anthropic.BadRequestError("bad", response=httpx2.Response(400, request=httpx2.Request("POST", "https://x")), body=None),
], ids=["refusal", "pause_turn", "http-400"])
async def test_p6_final_failures_stay_final(item: Any) -> None:
    err = await parse_error(item)
    assert err.transient is False and is_transient_parse_error(err) is False


async def test_p6_refusal_while_the_fallback_model_is_busy_is_still_transient() -> None:
    busy = reply(None, stop_reason="refusal", stop_details=SimpleNamespace(category="cyber", recommended_model="claude-opus-4-8"))
    assert (await parse_error(busy)).transient is True


@pytest.mark.parametrize("body,headers", [
    (b"", {"content-type": "application/json"}),
    (b"<html>proxy</html>", {"content-type": "text/html"}),
    (b"{\"id\": \"msg_1\", \"type\": \"mess", {"content-type": "application/json"}),
], ids=["empty-body", "html-body", "cut-body"])
async def test_p6_http_200_with_an_unusable_body_through_the_real_sdk(
    monkeypatch: pytest.MonkeyPatch, body: bytes, headers: dict[str, str]
) -> None:
    calls: list[httpx2.Request] = []

    async def handler(request: httpx2.Request) -> httpx2.Response:
        calls.append(request)
        return httpx2.Response(200, content=body, headers=headers)

    real = anthropic.AsyncAnthropic

    def factory(**kw: Any) -> Any:
        return real(**kw, http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)))

    monkeypatch.setattr(llm_parser.anthropic, "AsyncAnthropic", factory)
    parser = ClaudeSignalParser(LLMConfig(api_key="sk-test"))
    try:
        with pytest.raises(ParserError) as ei:
            await parser.parse([tmsg(1, "eth 1R long")], ParseContext(now=NOW))
    finally:
        await parser.aclose()
    assert ei.value.transient is True and is_transient_parse_error(ei.value) is True
    assert str(ei.value) == "Yapay zekâ servisinden okunamayan bir yanıt geldi; sinyal okunamadı."
    assert len(calls) == 1


async def engine_with_real_parser(*items: Any) -> tuple[Engine, ScriptedClient, FakeBroker, FakeNotifier, Database]:
    """An engine whose parser is the real ClaudeSignalParser on a scripted client; retries without waiting."""
    cfg = AppConfig(telegram=TelegramConfig(api_id=1, api_hash="hash", source_chats=[CHAT]),
                    notifier=NotifierConfig(bot_token="123:abc", owner_user_id=42))
    cfg.monitor_interval_sec = 3600.0
    db = Database(":memory:")
    await db.open()
    parser, client = parser_for(*items)
    broker, notifier = FakeBroker("binance"), FakeNotifier()
    engine = Engine(cfg, db, parser, [broker], notifier, price_oracle=FakePriceOracle({"ETH": 2232.85}))
    engine.retry_delays = tuple(0.0 for _ in engine.retry_delays)
    await engine.start(monitor=False)
    notifier.notifications.clear()
    now = datetime.now(timezone.utc)
    for mid, text in ((501, "eth long 2232.18 stop 2205.11 tp 2310.6"), (502, "eth 1R long")):
        await db.save_message(StoredMessage(chat_id=CHAT, message_id=mid, sender_id=SENDER, sender_name="Trader",
                                            date=now, text=text))
    return engine, client, broker, notifier, db


async def settle() -> None:
    for _ in range(60):
        await asyncio.sleep(0.01)


async def test_p6_engine_retries_a_cut_reply_and_then_opens(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(formatting, "RISK_PER_R_NOTE", formatting.RISK_PER_R_NOTE)
    engine, client, broker, notifier, db = await engine_with_real_parser(
        MAX_TOKENS, reply(None), reply({"actions": [ETH_OPEN_WIRE]}))
    try:
        await engine.on_messages_ready(CHAT, SENDER)
        await settle()
        assert len(client.requests) == 3                         # cut by the token limit, empty, then a good answer
        assert len(broker.opened) == 1 and broker.opened[0].stop_loss == 2205.11
        warnings = notifier.texts(Severity.WARNING)
        assert len(warnings) == 1 and "tekrar denenecek" in warnings[0] and "yarıda kesildi (token limiti)" in warnings[0]
    finally:
        await engine.stop()
        await db.close()


async def test_p6_engine_gives_up_after_all_attempts_with_a_notice(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(formatting, "RISK_PER_R_NOTE", formatting.RISK_PER_R_NOTE)
    engine, client, broker, notifier, db = await engine_with_real_parser(MAX_TOKENS)
    try:
        attempts = len(engine.retry_delays) + 1                  # the first try plus one per retry delay
        await engine.on_messages_ready(CHAT, SENDER)
        await settle()
        assert len(client.requests) == attempts and broker.opened == []
        final = notifier.texts(Severity.WARNING)[-1]
        assert f"Sinyal {attempts} denemede okunamadı" in final and "yarıda kesildi (token limiti)" in final
        assert (await db.get_message(CHAT, 501)).consumed        # type: ignore[union-attr]
    finally:
        await engine.stop()
        await db.close()


async def test_p6_engine_does_not_retry_a_refusal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(formatting, "RISK_PER_R_NOTE", formatting.RISK_PER_R_NOTE)
    engine, client, broker, notifier, db = await engine_with_real_parser(REFUSAL, reply({"actions": [ETH_OPEN_WIRE]}))
    try:
        await engine.on_messages_ready(CHAT, SENDER)
        await settle()
        assert len(client.requests) == 1 and broker.opened == []
        (notice,) = notifier.texts(Severity.WARNING)
        assert notice.startswith("Sinyal okunamadı: Yapay zekâ bu mesajı işlemeyi reddetti")
    finally:
        await engine.stop()
        await db.close()


# ---- P4, second half: what the model reads from a bare status word is never applied without asking ---------
from copytrader.parsing.prefilter import is_bare_status  # noqa: E402

BARE_DOUBT = ("Mesajda yalnızca “{}” yazıyor; ne olduğu (giriş mi doldu, hedef mi geldi, stop mu oldu) belli değil, "
              "bu yüzden sormadan uygulanmadı.")


@pytest.mark.parametrize("text", ["Geldi", "GELDİ", "Geldi ✅", "✅", "🎯", "Tamamdır", "TAMAMDIR", "Açtık", "Açıldı",
                                  "Yedik", "Bitti", "Tuttu", "Olmadı", "Devam", "Bekliyoruz", "Tut", "Yanlış oldu",
                                  "done", "hit", "Geldi arkadaşlar"])
def test_p4_bare_status_words_are_recognised(text: str) -> None:
    assert is_bare_status(tmsg(1, text)) is True and relevant(text) is True


@pytest.mark.parametrize("text", [
    "Stop", "stop olduk", "kapattık", "TP geldi", "Hedef geldi", "Aktif", "2R kapandı", "BE", "eth 1R long",   # known wording
    "Başa baş", "Başabaş yaptık", "Sıfırladık", "Patladık", "Tetiklendi", "Geçersiz", "Kilitledik", "filled",   # say what happened
    "Geldi 2310", "ETH geldi", "tp1 ✅", "günaydın", "tamamdır hocam", "", "👍",
])
def test_p4_messages_that_say_what_happened_or_are_chatter_are_not_bare_status(text: str) -> None:
    assert is_bare_status(tmsg(1, text)) is False


def test_p4_a_picture_is_never_a_bare_status() -> None:
    assert is_bare_status(tmsg(1, "Geldi", media_type="photo")) is False
    assert is_bare_status(tmsg(1, "✅", media_type="photo", media_path="/x.jpg")) is False


OPEN_TRADES = [{"trade_id": 7, "asset": "ETH", "side": "long", "entry": 2232.18, "stop_loss": 2205.11,
                "take_profits": [{"price": 2310.6, "close_fraction": 1.0, "hit": False}], "status": "open",
                "source_message_ids": [501, 502], "opened_at": "2026-11-12T11:00:00Z"}]


async def managed(text: str, **action: Any) -> ParsedAction:
    """What the parser returns for a reply ``text`` to the signal of trade 7 when the model answers ``action``."""
    answer = wire(base_asset="ETH", side="long", refers_to_message_id=502, target_trade_id=7, used_message_ids=[600],
                  confidence=0.95, **action)
    parser, _ = parser_for(reply({"actions": [answer]}))
    ctx = ParseContext(now=NOW, reply_to=tmsg(502, "eth 1R long"), open_trades=OPEN_TRADES)
    (parsed,) = (await parser.parse([tmsg(600, text, reply_to_message_id=502)], ctx)).actions
    return parsed


async def test_p4_management_action_read_from_a_bare_status_word_gets_a_doubt() -> None:
    closed = await managed("Geldi", action="close", close_fraction=1.0, summary_tr="ETH işlemi kapatılacak")
    assert closed.action is ActionType.CLOSE and closed.confidence == 0.95
    assert closed.ambiguities == [BARE_DOUBT.format("Geldi")]
    for text, action in (("✅", dict(action="close", close_fraction=1.0)), ("Tamamdır", dict(action="cancel")),
                         ("Bitti 🎯", dict(action="close", close_fraction=1.0)),
                         ("Tut", dict(action="move_sl", move_sl_to_entry=True))):
        assert (await managed(text, **action)).ambiguities == [BARE_DOUBT.format(text)], text


async def test_p4_known_wording_and_other_actions_get_no_such_doubt() -> None:
    # wording that says what happened: the model's reading stands
    assert (await managed("TP geldi", action="close", close_fraction=1.0)).ambiguities == []
    assert (await managed("Stop", action="close", close_fraction=1.0)).ambiguities == []
    assert (await managed("Başa baş", action="move_sl", move_sl_to_entry=True)).ambiguities == []
    assert (await managed("Geçersiz", action="cancel")).ambiguities == []
    # "none" for a bare word (information only) is left alone
    assert (await managed("Geldi", action="none")).ambiguities == []
    # the model's own doubt is kept, not doubled
    own = await managed("Geldi", action="close", close_fraction=1.0, ambiguities=["Hedef mi geldi, giriş mi doldu?"])
    assert own.ambiguities == ["Hedef mi geldi, giriş mi doldu?"]


async def test_p4_engine_asks_before_closing_on_a_bare_geldi(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(formatting, "RISK_PER_R_NOTE", formatting.RISK_PER_R_NOTE)
    for text, automatic in (("Geldi", False), ("TP geldi", True)):
        close = wire(action="close", base_asset="ETH", side="long", close_fraction=1.0, refers_to_message_id=502,
                     target_trade_id=1, used_message_ids=[600], confidence=0.95, summary_tr="ETH işlemi kapatılacak")
        engine, client, broker, notifier, db = await engine_with_real_parser(
            reply({"actions": [ETH_OPEN_WIRE]}), reply({"actions": [close]}))
        try:
            await engine.on_messages_ready(CHAT, SENDER)             # the signal: opened
            await settle()
            assert len(broker.opened) == 1
            await db.save_message(StoredMessage(chat_id=CHAT, message_id=600, sender_id=SENDER, sender_name="Trader",
                                                date=datetime.now(timezone.utc), text=text, reply_to_message_id=502))
            await engine.on_messages_ready(CHAT, SENDER)             # the reply
            await settle()
            assert len(client.requests) == 2
            closes = broker.calls_of("close_position")
            if automatic:
                assert len(closes) == 1 and notifier.confirmations == [], text
            else:
                assert closes == [], text
                (conf,) = notifier.confirmations
                assert conf.kind == "manage" and BARE_DOUBT.format("Geldi") in [i.message_tr for i in conf.issues]
        finally:
            await engine.stop()
            await db.close()
