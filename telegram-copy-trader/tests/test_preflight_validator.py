"""Pre-flight regression tests: the market / limit entry decision on fx routes.

Reproduced defect: on an fx route the market/limit tolerance is 0.03% of the price (about 3 pips on EURUSD,
1.2 USD on gold at 4100). The parser returns entry_type "market" when the trader enters at the current price;
when the live price had then moved a little more than that, ``build_open_plan`` raised
``market_far_from_entry``: the owner was asked and the trade lapsed after the confirmation timeout, while the
same signal without an entry type was handled automatically.

Now (``config.effective_risk`` + ``validator.build_open_plan``):

* ``market_entry_tolerance_r``: the live price also counts as "at the entry" within this fraction of the
  signal's own entry-to-stop distance (fx routes: 0.25R);
* ``explicit_market_drift: follow`` (fx routes): an explicit MARKET signal outside the tolerance is handled
  like a signal without an entry type (a limit at the trader's entry) instead of a question.

Routes of other kinds keep the global values (R tolerance off, "ask"): nothing changes for them.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, NamedTuple

import pytest
import yaml
from pydantic import ValidationError

import copytrader.config as config
from copytrader.config import AppConfig, LLMConfig, RiskConfig, TopicRoute, effective_risk, route_thresholds
from copytrader.engine.orchestrator import Engine
from copytrader.models import ActionType, EntryType, MarketType, ParsedAction, Side, TakeProfit, TradePlan, ValidationIssue
from copytrader.parsing.validator import build_open_plan
from copytrader.storage.db import Database
from copytrader.telegram import formatting

from fakes import FakeBroker, FakeNotifier, FakeParser, FakePriceOracle

NOW = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
MARKET, LIMIT = EntryType.MARKET, EntryType.LIMIT

# Replica of the owner's production config.yaml (same routes, brokers and values; ids and secrets are dummies).
# It has no "risk:" section and no per-route overrides: every threshold comes from the code defaults.
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


def prod_replica() -> AppConfig:
    return AppConfig.model_validate(yaml.safe_load(PROD_REPLICA_YAML))


def route(name: str, cfg: AppConfig | None = None) -> TopicRoute:
    return next(r for r in (cfg or prod_replica()).telegram.topics if r.name == name)


def fx_route(**kw: Any) -> TopicRoute:
    return TopicRoute(name="fx", title_contains=["Fx İşlemleri"], kind="fx", **kw)


def crypto_route(**kw: Any) -> TopicRoute:
    return TopicRoute(name="kripto", title_contains=["Kripto İşlemleri"], kind="crypto", **kw)


# --------------------------------------------------------------------------- #
# Signals
# --------------------------------------------------------------------------- #
def _open(et: EntryType | None, **kw: Any) -> ParsedAction:
    data: dict[str, Any] = dict(action=ActionType.OPEN, side=Side.LONG, entry_type=et, risk_r=1.0,
                                used_message_ids=[701], confidence=0.95)
    data.update(kw)
    data["take_profits"] = [TakeProfit(price=p) for p in data.pop("tps")]
    return ParsedAction(**data)


def eur(et: EntryType | None, **kw: Any) -> ParsedAction:
    """EURUSD long, entry 1.1620, SL 1.1600 (1R = 20 pips, 0.25R = 5 pips), TP 1.1680."""
    data: dict[str, Any] = dict(symbol="EURUSD", base_asset="EUR", quote_asset="USD", market_type=MarketType.FOREX,
                                entry_price=1.1620, stop_loss=1.1600, tps=(1.1680,))
    data.update(kw)
    return _open(et, **data)


def eur_short(et: EntryType | None, **kw: Any) -> ParsedAction:
    """EURUSD short, entry 1.1620, SL 1.1640, TP 1.1560."""
    return eur(et, **{"side": Side.SHORT, "stop_loss": 1.1640, "tps": (1.1560,), **kw})


def xau(et: EntryType | None, **kw: Any) -> ParsedAction:
    """XAUUSD long, entry 4100, SL 4090 (1R = 10 USD, 0.25R = 2.5 USD), TP 4130."""
    data: dict[str, Any] = dict(symbol="XAUUSD", base_asset="XAU", quote_asset="USD", market_type=MarketType.CFD,
                                entry_price=4100.0, stop_loss=4090.0, tps=(4130.0,))
    data.update(kw)
    return _open(et, **data)


def btc(et: EntryType | None, **kw: Any) -> ParsedAction:
    """BTCUSDT.P long, entry 74000, SL 73300, TP 75500."""
    data: dict[str, Any] = dict(symbol="BTCUSDT.P", base_asset="BTC", quote_asset="USDT", market_type=MarketType.PERP,
                                entry_price=74000.0, stop_loss=73300.0, tps=(75500.0,))
    data.update(kw)
    return _open(et, **data)


def build(action: ParsedAction, live: float | None, risk: RiskConfig) -> tuple[TradePlan | None, list[ValidationIssue]]:
    return build_open_plan(action, risk=risk, llm=LLMConfig(), market_price=live, source_chat_id=-100, now=NOW)


def codes(issues: list[ValidationIssue]) -> list[str]:
    return [i.code for i in issues]


def decide(action: ParsedAction, live: float | None, risk: RiskConfig) -> tuple[EntryType | None, float | None, list[str]]:
    """(plan entry type, plan entry price, issue codes); the first two are None when the plan is blocked."""
    plan, issues = build(action, live, risk)
    return (plan.entry_type if plan else None, plan.entry_price if plan else None, codes(issues))


def fx_risk(**route_kw: Any) -> RiskConfig:
    return effective_risk(RiskConfig(), fx_route(**route_kw))


# --------------------------------------------------------------------------- #
# The case table of the task: (route of the production replica, signal, live price) -> decision.
# ``codes`` empty = handled automatically, anything else = the owner is asked.
# --------------------------------------------------------------------------- #
class Case(NamedTuple):
    name: str
    route: str
    action: ParsedAction
    live: float
    entry_type: EntryType
    codes: list[str]


def _cases() -> list[Case]:
    out: list[Case] = []
    for et, tag in ((MARKET, "explicit market"), (None, "no entry type")):
        out += [
            Case(f"fx EURUSD {tag}, live 2 pips above the entry (0.017%)", "fx", eur(et), 1.1622, MARKET, []),
            # the defect: 0.034% is outside the 0.03% tolerance, but only 0.2R away
            Case(f"fx EURUSD {tag}, live 4 pips above (0.034%, 0.2R)", "fx", eur(et), 1.1624, MARKET, []),
            Case(f"fx EURUSD {tag}, live 6 pips above (0.3R)", "fx", eur(et), 1.1626, LIMIT, []),
            # toward the stop (a better price than the entry): an explicit MARKET signal stays a market order;
            # without an entry type the limit at 1.1620 is already marketable, and fx routes do not ask about that
            Case(f"fx EURUSD {tag}, live 6 pips below (0.3R)", "fx", eur(et), 1.1614,
                 MARKET if et is MARKET else LIMIT, []),
            # 1.03% away is beyond max_entry_distance_pct (1% on fx); with this TP it is also beyond TP1
            Case(f"fx EURUSD {tag}, live 1.03% above", "fx", eur(et), 1.1740, LIMIT,
                 ["entry_far_from_market", "market_beyond_tp"]),
            Case(f"fx XAUUSD {tag}, live 2 USD above (0.049%, 0.2R)", "fx", xau(et), 4102.0, MARKET, []),
            Case(f"fx XAUUSD {tag}, live 3 USD above (0.3R)", "fx", xau(et), 4103.0, LIMIT, []),
        ]
    wide = dict(stop_loss=71000.0, tps=(80000.0,))       # 1R = 3000: 0.81% away is only 0.2R
    out += [
        # the kripto route has no overrides: explicit MARKET that drifted still asks, exactly as before
        Case("kripto BTC explicit market, live 0.54% above", "kripto", btc(MARKET), 74400.0, MARKET,
             ["market_far_from_entry"]),
        Case("kripto BTC explicit market, live 0.54% below", "kripto", btc(MARKET), 73600.0, MARKET,
             ["market_far_from_entry"]),
        Case("kripto BTC explicit market, live 0.47% above", "kripto", btc(MARKET), 74350.0, MARKET, []),
        Case("kripto BTC no entry type, live 0.02% above", "kripto", btc(None), 74015.0, MARKET, []),
        Case("kripto BTC no entry type, live 0.54% above", "kripto", btc(None), 74400.0, LIMIT, []),
        Case("kripto BTC no entry type, live 0.54% below", "kripto", btc(None), 73600.0, LIMIT,
             ["limit_entry_marketable"]),
        # the R tolerance is an fx default only
        Case("kripto BTC explicit market, live 0.81% above = 0.2R", "kripto", btc(MARKET, **wide), 74600.0, MARKET,
             ["market_far_from_entry"]),
        Case("kripto BTC no entry type, live 0.81% above = 0.2R", "kripto", btc(None, **wide), 74600.0, LIMIT, []),
    ]
    return out


CASES = _cases()


# --------------------------------------------------------------------------- #
# config: the new settings and their per-route values (effective_risk)
# --------------------------------------------------------------------------- #
def test_risk_config_new_fields_defaults_and_bounds() -> None:
    risk = RiskConfig()
    assert risk.market_entry_tolerance_r is None            # off: only the percent tolerance applies
    assert risk.explicit_market_drift == "ask"              # old behaviour
    assert risk.max_size_multiple == 2.0
    assert RiskConfig(market_entry_tolerance_r=0.25, explicit_market_drift="follow", max_size_multiple=1).model_dump(
        include={"market_entry_tolerance_r", "explicit_market_drift", "max_size_multiple"}
    ) == {"market_entry_tolerance_r": 0.25, "explicit_market_drift": "follow", "max_size_multiple": 1.0}
    for bad in ({"market_entry_tolerance_r": 0}, {"market_entry_tolerance_r": -0.25},
                {"explicit_market_drift": "market"}, {"max_size_multiple": 0.99}):
        with pytest.raises(ValidationError):
            RiskConfig(**bad)


def test_topic_route_new_overrides_default_to_unset_and_are_validated() -> None:
    r = fx_route()
    assert r.market_entry_tolerance_r is None and r.explicit_market_drift is None
    r = fx_route(market_entry_tolerance_r=0.5, explicit_market_drift="ask")
    assert r.market_entry_tolerance_r == 0.5 and r.explicit_market_drift == "ask"
    for bad in ({"market_entry_tolerance_r": 0}, {"market_entry_tolerance_r": -1}, {"explicit_market_drift": "limit"}):
        with pytest.raises(ValidationError):
            fx_route(**bad)


def test_fx_route_gets_r_tolerance_and_follow() -> None:
    assert config.FX_MARKET_ENTRY_TOLERANCE_R == 0.25
    risk = RiskConfig()
    eff = effective_risk(risk, fx_route())
    assert eff is not risk
    assert eff.market_entry_tolerance_r == 0.25 and eff.explicit_market_drift == "follow"
    assert (eff.market_entry_tolerance_pct, eff.max_entry_distance_pct, eff.ask_if_limit_marketable) == (0.03, 1.0, False)
    # nothing else differs from the global object, and the global object itself is untouched
    changed = {"market_entry_tolerance_pct", "max_entry_distance_pct", "ask_if_limit_marketable",
               "market_entry_tolerance_r", "explicit_market_drift"}
    assert eff.model_dump(exclude=changed) == risk.model_dump(exclude=changed)
    assert risk == RiskConfig()


def test_other_kinds_use_the_global_values_and_the_same_object() -> None:
    risk = RiskConfig()
    for r in (None, crypto_route(), TopicRoute(name="a", title_contains=["x"])):
        assert effective_risk(risk, r) is risk
    custom = RiskConfig(market_entry_tolerance_r=0.1, explicit_market_drift="follow", max_size_multiple=1.5)
    for r in (None, crypto_route(), TopicRoute(name="a", title_contains=["x"])):
        assert effective_risk(custom, r) is custom              # the global values apply as they are
    eff = effective_risk(custom, fx_route())                    # fx: the fx defaults win over the global values
    assert (eff.market_entry_tolerance_r, eff.explicit_market_drift, eff.max_size_multiple) == (0.25, "follow", 1.5)


def test_route_overrides_win_on_every_kind() -> None:
    risk = RiskConfig()
    eff = effective_risk(risk, fx_route(market_entry_tolerance_r=0.5, explicit_market_drift="ask"))
    assert (eff.market_entry_tolerance_r, eff.explicit_market_drift) == (0.5, "ask")
    assert (eff.market_entry_tolerance_pct, eff.ask_if_limit_marketable) == (0.03, False)     # the rest stays fx
    eff = effective_risk(risk, fx_route(explicit_market_drift="ask"))
    assert (eff.market_entry_tolerance_r, eff.explicit_market_drift) == (0.25, "ask")
    eff = effective_risk(risk, crypto_route(market_entry_tolerance_r=0.3, explicit_market_drift="follow"))
    assert (eff.market_entry_tolerance_r, eff.explicit_market_drift) == (0.3, "follow")
    assert (eff.market_entry_tolerance_pct, eff.max_entry_distance_pct, eff.ask_if_limit_marketable) == (0.5, 10.0, True)


def test_effective_risk_returns_the_same_object_only_when_nothing_differs() -> None:
    risk = RiskConfig()
    # a route that differs in ONE new field only must not be mistaken for "nothing differs"
    eff = effective_risk(risk, crypto_route(market_entry_tolerance_r=0.3))
    assert eff is not risk and eff.market_entry_tolerance_r == 0.3 and eff.explicit_market_drift == "ask"
    eff = effective_risk(risk, crypto_route(explicit_market_drift="follow"))
    assert eff is not risk and eff.explicit_market_drift == "follow" and eff.market_entry_tolerance_r is None
    # overrides equal to the global values: still the same object
    same = RiskConfig(market_entry_tolerance_r=0.3, explicit_market_drift="follow")
    assert effective_risk(same, crypto_route(market_entry_tolerance_r=0.3, explicit_market_drift="follow")) is same
    # a global config that already equals the fx values
    fx_like = RiskConfig(market_entry_tolerance_pct=0.03, max_entry_distance_pct=1.0, ask_if_limit_marketable=False,
                         market_entry_tolerance_r=0.25, explicit_market_drift="follow")
    assert effective_risk(fx_like, fx_route()) is fx_like


def test_route_thresholds_is_unchanged() -> None:
    risk = RiskConfig(market_entry_tolerance_r=0.4, explicit_market_drift="follow")
    assert route_thresholds(risk, fx_route(market_entry_tolerance_r=0.5)) == (0.03, 1.0, 0.05)
    assert route_thresholds(risk, crypto_route()) == (0.5, 10.0, 0.5)
    assert route_thresholds(risk, None) == (0.5, 10.0, 0.5)


def test_production_replica_has_no_overrides_and_new_keys_load_from_yaml() -> None:
    cfg = prod_replica()
    assert cfg.risk == RiskConfig()
    for r in cfg.telegram.topics:
        assert (r.market_entry_tolerance_pct, r.max_entry_distance_pct, r.duplicate_tolerance_pct,
                r.market_entry_tolerance_r, r.explicit_market_drift) == (None, None, None, None, None)
    assert effective_risk(cfg.risk, route("kripto", cfg)) is cfg.risk
    eff = effective_risk(cfg.risk, route("fx", cfg))
    assert (eff.market_entry_tolerance_r, eff.explicit_market_drift) == (0.25, "follow")

    raw = yaml.safe_load(PROD_REPLICA_YAML)
    raw["risk"] = {"market_entry_tolerance_r": 0.2, "explicit_market_drift": "follow", "max_size_multiple": 1.5}
    raw["telegram"]["topics"][0].update(market_entry_tolerance_r=0.3, explicit_market_drift="ask")
    raw["telegram"]["topics"][1].update(market_entry_tolerance_r=0.5)
    cfg = AppConfig.model_validate(raw)
    assert (cfg.risk.market_entry_tolerance_r, cfg.risk.explicit_market_drift, cfg.risk.max_size_multiple) == (0.2, "follow", 1.5)
    kripto, fx = (effective_risk(cfg.risk, r) for r in cfg.telegram.topics)
    assert (kripto.market_entry_tolerance_r, kripto.explicit_market_drift) == (0.3, "ask")
    assert (fx.market_entry_tolerance_r, fx.explicit_market_drift) == (0.5, "follow")
    assert kripto.max_size_multiple == fx.max_size_multiple == 1.5


# --------------------------------------------------------------------------- #
# validator: the entry decision (build_open_plan)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("case", CASES, ids=[c.name for c in CASES])
def test_entry_decision_with_the_route_settings_of_the_production_replica(case: Case) -> None:
    cfg = prod_replica()
    risk = effective_risk(cfg.risk, route(case.route, cfg))
    plan, issues = build(case.action, case.live, risk)
    assert plan is not None
    assert (plan.entry_type, codes(issues)) == (case.entry_type, case.codes)
    assert plan.entry_price == case.action.entry_price       # the signal's entry: limit price / market reference
    assert not any(i.blocking for i in issues)


def test_fx_explicit_market_four_pips_away_is_no_longer_a_question() -> None:
    """The reproduced defect, spelled out."""
    plan, issues = build(eur(MARKET), 1.1624, fx_risk())
    assert issues == []
    assert plan is not None and plan.entry_type is MARKET and plan.entry_price == 1.1620
    assert "(piyasa)" in plan.summary_tr


def test_fx_explicit_market_outside_the_tolerance_becomes_a_limit_at_the_signal_entry() -> None:
    plan, issues = build(eur(MARKET), 1.1626, fx_risk())             # the price ran away above the entry
    assert issues == []
    assert plan is not None and plan.entry_type is LIMIT and plan.entry_price == 1.1620
    assert "(limit)" in plan.summary_tr and "giriş 1.162," in plan.summary_tr
    plan, issues = build(eur(MARKET), 1.1614, fx_risk())             # a better price (towards the stop): stays market
    assert issues == []
    assert plan is not None and plan.entry_type is MARKET and plan.entry_price == 1.1620
    assert "(piyasa)" in plan.summary_tr and "giriş 1.162," in plan.summary_tr


def test_following_a_drifted_market_signal_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    """Nothing is asked, so the reason for a limit order on a "market" signal must at least be in the log."""
    name = "copytrader.parsing.validator"
    caplog.set_level(logging.INFO, logger=name)

    def logged() -> list[logging.LogRecord]:
        return [r for r in caplog.records if r.name == name]

    build(eur(MARKET), 1.1624, fx_risk())                      # inside the tolerance: nothing to explain
    build(eur(None), 1.1626, fx_risk())                        # no entry type: nothing to explain
    build(eur(MARKET), 1.1626, fx_risk(explicit_market_drift="ask"))
    assert logged() == []
    build(eur(MARKET), 1.1626, fx_risk())
    (record,) = logged()
    text = record.getMessage()
    assert record.levelno == logging.INFO
    assert text.startswith("EUR: MARKET signal") and "market (1.1626)" in text and "entry (1.162)" in text


def test_followed_market_signal_is_checked_like_a_limit() -> None:
    # toward the stop the price is better than the trader's entry: the signal stays a market order, not asked ...
    assert decide(eur(MARKET), 1.1614, fx_risk()) == (MARKET, 1.1620, [])
    # ... also on a route that asks about marketable limits
    follow = RiskConfig(explicit_market_drift="follow", market_entry_tolerance_pct=0.03)
    assert decide(eur(MARKET), 1.1614, follow) == (MARKET, 1.1620, [])
    assert decide(eur(None), 1.1614, follow) == (LIMIT, 1.1620, ["limit_entry_marketable"])
    # market beyond TP1: a warning as for a limit (blocking only for a market entry)
    plan, issues = build(eur(MARKET), 1.1690, fx_risk())
    assert plan is not None and plan.entry_type is LIMIT
    assert [(i.code, i.blocking) for i in issues] == [("market_beyond_tp", False)]
    # market beyond the stop: blocked whatever the entry type
    plan, issues = build(eur(MARKET), 1.1590, fx_risk())
    assert plan is None and "market_beyond_stop" in codes(issues)


@pytest.mark.parametrize("et", [MARKET, None], ids=["explicit market", "no entry type"])
def test_r_tolerance_edge_is_inclusive_on_both_sides(et: EntryType | None) -> None:
    risk = fx_risk()
    assert decide(eur(et), 1.1625, risk) == (MARKET, 1.1620, [])        # exactly 5 pips = 0.25R above
    assert decide(eur(et), 1.1615, risk) == (MARKET, 1.1620, [])        # exactly 0.25R below
    assert decide(eur(et), 1.16251, risk) == (LIMIT, 1.1620, [])
    # below the entry an explicit MARKET signal stays a market order also outside the tolerance (better price)
    assert decide(eur(et), 1.16149, risk) == (MARKET if et is MARKET else LIMIT, 1.1620, [])
    assert decide(xau(et), 4102.5, risk) == (MARKET, 4100.0, [])
    assert decide(xau(et), 4097.5, risk) == (MARKET, 4100.0, [])
    assert decide(xau(et), 4102.6, risk) == (LIMIT, 4100.0, [])


@pytest.mark.parametrize("et", [MARKET, None], ids=["explicit market", "no entry type"])
def test_short_side_mirror(et: EntryType | None) -> None:
    risk = fx_risk()
    assert decide(eur_short(et), 1.1616, risk) == (MARKET, 1.1620, [])      # 0.2R below (in profit direction)
    assert decide(eur_short(et), 1.1624, risk) == (MARKET, 1.1620, [])      # 0.2R above (toward the stop)
    assert decide(eur_short(et), 1.1614, risk) == (LIMIT, 1.1620, [])       # sell limit above the market: rests
    # above the short entry (towards the stop): explicit MARKET stays market; else a marketable sell limit, not asked
    assert decide(eur_short(et), 1.1626, risk) == (MARKET if et is MARKET else LIMIT, 1.1620, [])


@pytest.mark.parametrize("live", [1.1618, 1.1620, 1.1622, 1.1624, 1.1626, 1.1614])
def test_explicit_limit_is_never_turned_into_a_market_order(live: float) -> None:
    assert decide(eur(LIMIT), live, fx_risk()) == (LIMIT, 1.1620, [])
    assert decide(eur(LIMIT), live, RiskConfig(market_entry_tolerance_r=0.25, explicit_market_drift="follow"))[0] is LIMIT
    # not even where a signal without an entry type would always be a market order
    always_market = fx_risk().model_copy(update={"entry_mode": "market"})
    assert decide(eur(None), live, always_market)[0] is MARKET
    assert decide(eur(LIMIT), live, always_market) == (LIMIT, 1.1620, [])


@pytest.mark.parametrize("et", [MARKET, None], ids=["explicit market", "no entry type"])
def test_percent_tolerance_still_counts_when_the_r_tolerance_is_tighter(et: EntryType | None) -> None:
    """The two tolerances are alternatives: a 4-pip stop makes 0.25R one pip, but 0.03% (3.5 pips) still holds."""
    tight = dict(stop_loss=1.1616)
    assert decide(eur(et, **tight), 1.1622, fx_risk()) == (MARKET, 1.1620, [])      # 0.017%, 0.5R
    assert decide(eur(et, **tight), 1.1618, fx_risk()) == (MARKET, 1.1620, [])
    assert decide(eur(et, **tight), 1.1624, fx_risk()) == (LIMIT, 1.1620, [])       # 0.034%, 1R: outside both


def test_ask_mode_keeps_the_question_and_honours_the_r_tolerance() -> None:
    risk = fx_risk(explicit_market_drift="ask")
    assert decide(eur(MARKET), 1.1624, risk) == (MARKET, 1.1620, [])                            # within 0.25R
    plan, issues = build(eur(MARKET), 1.1626, risk)                                             # outside: as before
    assert plan is not None and plan.entry_type is MARKET and plan.entry_price == 1.1620
    assert [(i.code, i.blocking) for i in issues] == [("market_far_from_entry", False)]
    assert issues[0].message_tr == "Sinyal piyasa emri diyor ama anlık fiyat (1.1626) girişten (1.162) %0.05164 uzak."
    assert decide(eur(None), 1.1626, risk) == (LIMIT, 1.1620, [])


def test_global_defaults_behave_as_before() -> None:
    risk = RiskConfig()
    plan, issues = build(btc(MARKET), 74400.0, risk)
    assert plan is not None and plan.entry_type is MARKET and plan.entry_price == 74000.0
    assert [(i.code, i.blocking) for i in issues] == [("market_far_from_entry", False)]
    assert issues[0].message_tr == "Sinyal piyasa emri diyor ama anlık fiyat (74400) girişten (74000) %0.5405 uzak."
    # with a wide stop 0.81% is only 0.2R: without market_entry_tolerance_r that changes nothing
    wide = dict(stop_loss=71000.0, tps=(80000.0,))
    assert decide(btc(MARKET, **wide), 74600.0, risk) == (MARKET, 74000.0, ["market_far_from_entry"])
    assert decide(btc(None, **wide), 74600.0, risk) == (LIMIT, 74000.0, [])
    # the global R tolerance alone (still "ask"): inside it no question, outside it the old question
    with_r = RiskConfig(market_entry_tolerance_r=0.25)
    assert decide(btc(MARKET, **wide), 74600.0, with_r) == (MARKET, 74000.0, [])
    assert decide(btc(None, **wide), 74600.0, with_r) == (MARKET, 74000.0, [])
    assert decide(btc(MARKET, **wide), 74800.0, with_r) == (MARKET, 74000.0, ["market_far_from_entry"])
    assert decide(btc(None, **wide), 74800.0, with_r) == (LIMIT, 74000.0, [])


def test_entry_far_from_market_is_checked_in_every_mode() -> None:
    far = 1.1740                                    # 1.03% above the entry: beyond the fx max_entry_distance_pct
    tight = dict(stop_loss=1.1400, tps=(1.2000,))   # 0.25R = 55 pips: 120 pips away is outside
    loose = dict(stop_loss=1.1000, tps=(1.2500,))   # 0.25R = 155 pips: 120 pips away is inside
    follow, ask = fx_risk(), fx_risk(explicit_market_drift="ask")
    assert decide(eur(MARKET, **tight), far, follow) == (LIMIT, 1.1620, ["entry_far_from_market"])
    assert decide(eur(None, **tight), far, follow) == (LIMIT, 1.1620, ["entry_far_from_market"])
    assert decide(eur(LIMIT, **tight), far, follow) == (LIMIT, 1.1620, ["entry_far_from_market"])
    assert decide(eur(MARKET, **tight), far, ask) == (MARKET, 1.1620, ["entry_far_from_market", "market_far_from_entry"])
    # (the 620-pip stop of "loose" is 5.3% of the price: beyond the plausible 3% for a currency pair)
    assert decide(eur(MARKET, **loose), far, follow) == (MARKET, 1.1620, ["stop_too_wide", "entry_far_from_market"])
    assert decide(eur(None, **loose), far, follow) == (MARKET, 1.1620, ["stop_too_wide", "entry_far_from_market"])
    assert decide(eur(MARKET, **loose), far, ask) == (MARKET, 1.1620, ["stop_too_wide", "entry_far_from_market"])


def test_follow_means_decide_like_a_signal_without_entry_type() -> None:
    """A followed MARKET signal gets what the same signal without an entry type gets: with entry_mode "auto"
    (the default) and "limit" a limit at the signal's entry, with entry_mode "market" a market order."""
    for mode, expected in (("auto", LIMIT), ("limit", LIMIT), ("market", MARKET)):
        risk = fx_risk().model_copy(update={"entry_mode": mode})
        assert decide(eur(MARKET), 1.1626, risk) == decide(eur(None), 1.1626, risk) == (expected, 1.1620, [])
        assert decide(eur(MARKET), 1.1624, risk) == (MARKET, 1.1620, [])      # inside the tolerance: explicit wins


def test_unknown_market_price_or_entry_keeps_the_explicit_market_type() -> None:
    risk = fx_risk()
    assert decide(eur(MARKET), None, risk) == (MARKET, 1.1620, ["market_price_unknown"])
    assert decide(eur(None), None, risk) == (LIMIT, 1.1620, ["market_price_unknown"])
    assert decide(eur(MARKET, entry_price=None), 1.1622, risk) == (MARKET, 1.1622, [])


def test_r_tolerance_without_a_usable_stop_does_not_crash() -> None:
    risk = fx_risk()
    for stop in (None, 0.0, -1.0):
        plan, issues = build(eur(MARKET, stop_loss=stop), 1.1624, risk)
        assert plan is None and any(i.blocking for i in issues)
        assert "market_far_from_entry" not in codes(issues)


# --------------------------------------------------------------------------- #
# Engine level: Engine.evaluate with the production replica and a fake price oracle
# --------------------------------------------------------------------------- #
async def start_engine(cfg: AppConfig) -> tuple[Engine, FakePriceOracle, Database]:
    db = Database(":memory:")
    await db.open()
    oracle = FakePriceOracle()
    engine = Engine(cfg, db, FakeParser(), [FakeBroker(b.name) for b in cfg.brokers], FakeNotifier(),
                    price_oracle=oracle)
    await engine.start(monitor=False)
    return engine, oracle, db


async def engine_mismatches(engine: Engine, oracle: FakePriceOracle, cases: list[Case]) -> list[str]:
    """Run every case through Engine.evaluate; one line per case whose decision differs from the table."""
    routes = {r.name: r for r in engine.config.telegram.topics}
    chat = int(engine.config.telegram.source_chats[0])
    wrong: list[str] = []
    for case in cases:
        oracle.prices = {str(case.action.base_asset): case.live}
        ev = await engine.evaluate(case.action, chat, route=routes[case.route])
        assert ev is not None and ev.kind == "open" and ev.market_price == case.live
        plan = ev.plan
        got = (plan.entry_type if isinstance(plan, TradePlan) else None,
               plan.entry_price if isinstance(plan, TradePlan) else None,
               plan.route if isinstance(plan, TradePlan) else None, codes(ev.issues), ev.automatic)
        want = (case.entry_type, case.action.entry_price, case.route, case.codes, not case.codes)
        if got != want:
            wrong.append(f"{case.name}: got {got}, expected {want}")
    return wrong


async def test_engine_evaluate_with_the_production_replica(monkeypatch: pytest.MonkeyPatch) -> None:
    # Engine.start() sets this module global from the brokers' risk_usd: put it back afterwards
    monkeypatch.setattr(formatting, "RISK_PER_R_NOTE", formatting.RISK_PER_R_NOTE)
    engine, oracle, db = await start_engine(prod_replica())
    try:
        assert [b.name for b in engine.brokers] == ["binance", "breakout", "fx", "tickmill"]
        wrong = await engine_mismatches(engine, oracle, CASES)
        assert not wrong, "\n".join(wrong)
        assert engine.notifier.confirmations == []           # evaluate() itself never asks or trades
    finally:
        await engine.stop()
        await db.close()
