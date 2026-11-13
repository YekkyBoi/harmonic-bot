import math
from datetime import datetime, timezone

import pytest

from copytrader.config import LLMConfig, RiskConfig
from copytrader.models import (
    ActionType,
    EntryType,
    MarketType,
    ParsedAction,
    Side,
    TakeProfit,
    TradePlan,
    TradeRecord,
    TradeStatus,
)
from copytrader.parsing.validator import build_management_plan, build_open_plan, low_confidence_issues

NOW = datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)
RISK = RiskConfig()
LLM = LLMConfig()
ETH_MARKET = 2232.85  # black current-price label of the example screenshot


def act(**kw) -> ParsedAction:
    """The 'eth 1R long' example as the parser returns it."""
    base = dict(
        action=ActionType.OPEN, symbol="ETHUSDT.P", base_asset="ETH", quote_asset="USDT",
        market_type=MarketType.PERP, side=Side.LONG, entry_price=2232.18, stop_loss=2205.11,
        take_profits=[TakeProfit(price=2310.6)], risk_r=1.0, used_message_ids=[501, 502], confidence=0.95,
    )
    base.update(kw)
    return ParsedAction(**base)


def build(action: ParsedAction, market: float | None = ETH_MARKET, risk: RiskConfig = RISK, **kw):
    return build_open_plan(action, risk=risk, llm=LLM, market_price=market, source_chat_id=-100, now=NOW, **kw)


def codes(issues) -> list[str]:
    return [i.code for i in issues]


def issue(issues, code):
    found = [i for i in issues if i.code == code]
    assert found, f"{code} not in {codes(issues)}"
    return found[0]


def tps(*prices: float, fractions: list[float] | None = None) -> list[TakeProfit]:
    fractions = fractions or [1.0] * len(prices)
    return [TakeProfit(price=p, close_fraction=f) for p, f in zip(prices, fractions)]


# --------------------------------------------------------------------------- #
# Open: happy path
# --------------------------------------------------------------------------- #
def test_eth_example_is_clean_market_plan():
    plan, issues = build(act())
    assert issues == []
    assert plan is not None
    assert plan.base_asset == "ETH" and plan.quote_asset == "USDT" and plan.market_type is MarketType.PERP
    assert plan.side is Side.LONG
    assert plan.entry_type is EntryType.MARKET          # market 0.03% from entry
    assert plan.entry_price == 2232.18                  # signal entry kept as reference
    assert plan.stop_loss == 2205.11
    assert plan.take_profits == [TakeProfit(price=2310.6, close_fraction=1.0)]
    assert plan.risk_r == 1.0 and plan.risk_pct == 2.0
    assert plan.source_chat_id == -100 and plan.source_message_ids == [501, 502]
    assert plan.created_at == NOW
    assert len(plan.plan_id) == 10
    assert "ETH LONG" in plan.summary_tr and "2205.11" in plan.summary_tr and "1R" in plan.summary_tr


def test_plan_id_is_passed_through():
    plan, _ = build(act(), plan_id="abc123")
    assert plan is not None and plan.plan_id == "abc123"


def test_plan_ids_are_unique():
    a, _ = build(act())
    b, _ = build(act())
    assert a.plan_id != b.plan_id


def test_btc_short_text_signal():
    action = act(
        symbol="BTC", base_asset="BTC", quote_asset=None, market_type=None, side=Side.SHORT,
        entry_price=67250, stop_loss=68100, take_profits=tps(64800, 66000), risk_r=0.5,
    )
    plan, issues = build(action, market=67300)
    assert issues == []
    assert plan.entry_type is EntryType.MARKET
    assert [tp.price for tp in plan.take_profits] == [66000, 64800]  # nearest first for a short
    assert [tp.close_fraction for tp in plan.take_profits] == [0.5, 0.5]
    assert plan.risk_pct == 1.0


# --------------------------------------------------------------------------- #
# Open: asset resolution
# --------------------------------------------------------------------------- #
def test_asset_filled_from_symbol_when_missing():
    plan, issues = build(act(symbol="BINANCE:ETHUSDT.P", base_asset=None, quote_asset=None, market_type=None))
    assert issues == []
    assert (plan.base_asset, plan.quote_asset, plan.market_type) == ("ETH", "USDT", MarketType.PERP)


def test_base_asset_canonicalized():
    action = act(symbol=None, base_asset="gold", quote_asset=None, market_type=None, entry_price=2318,
                 stop_loss=2309, take_profits=tps(2340))
    plan, _ = build(action, market=2318.5)
    assert plan.base_asset == "XAU"


def test_quote_and_type_filled_from_matching_symbol():
    action = act(symbol="XAUUSD", base_asset="altın", quote_asset=None, market_type=None, entry_price=2318,
                 stop_loss=2309, take_profits=tps(2340))
    plan, issues = build(action, market=2318.5)
    assert issues == []
    assert (plan.base_asset, plan.quote_asset, plan.market_type) == ("XAU", "USD", MarketType.CFD)


def test_symbol_asset_mismatch_asks():
    plan, issues = build(act(symbol="BTCUSDT.P", base_asset="ETH"))
    assert plan is not None
    assert codes(issues) == ["symbol_asset_mismatch"]
    assert not issues[0].blocking


def test_missing_asset_blocks():
    plan, issues = build(act(symbol=None, base_asset=None))
    assert plan is None
    assert issue(issues, "missing_asset").blocking


def test_missing_side_blocks():
    plan, issues = build(act(side=None))
    assert plan is None
    assert issue(issues, "missing_side").blocking


def test_missing_stop_blocks_with_turkish_message():
    plan, issues = build(act(stop_loss=None))
    assert plan is None
    i = issue(issues, "missing_stop_loss")
    assert i.blocking
    assert i.message_tr == "Stop seviyesi bulunamadı — stopsuz işlem açılmaz."


# --------------------------------------------------------------------------- #
# Open: price sanity
# --------------------------------------------------------------------------- #
def test_long_stop_above_entry_blocks():
    plan, issues = build(act(stop_loss=2240))
    assert plan is None
    i = issue(issues, "sl_wrong_side")
    assert i.blocking
    assert i.message_tr == "Long işlemde stop girişin altında olmalı (giriş 2232.18, stop 2240)."


def test_long_stop_equal_entry_blocks():
    plan, issues = build(act(stop_loss=2232.18))
    assert plan is None and "sl_wrong_side" in codes(issues)


def test_short_stop_below_entry_blocks():
    plan, issues = build(act(side=Side.SHORT, stop_loss=2200, take_profits=tps(2150)))
    assert plan is None
    assert issue(issues, "sl_wrong_side").message_tr == (
        "Short işlemde stop girişin üstünde olmalı (giriş 2232.18, stop 2200)."
    )


def test_long_market_beyond_stop_blocks():
    plan, issues = build(act(), market=2200)
    assert plan is None
    assert issue(issues, "market_beyond_stop").blocking


def test_market_exactly_at_stop_blocks():
    plan, issues = build(act(), market=2205.11)
    assert plan is None and "market_beyond_stop" in codes(issues)


def test_short_market_beyond_stop_blocks():
    action = act(side=Side.SHORT, entry_price=2205.11, stop_loss=2232.18, take_profits=tps(2150))
    plan, issues = build(action, market=2240)
    assert plan is None
    assert issue(issues, "market_beyond_stop").blocking


def test_long_tp_below_entry_blocks():
    plan, issues = build(act(take_profits=tps(2310.6, 2200)))
    assert plan is None
    assert issue(issues, "tp_wrong_side").blocking


def test_short_tp_above_entry_blocks():
    action = act(side=Side.SHORT, entry_price=2205.11, stop_loss=2232.18, take_profits=tps(2150, 2210))
    plan, issues = build(action, market=2205)
    assert plan is None and "tp_wrong_side" in codes(issues)


@pytest.mark.parametrize(
    "override",
    [{"entry_price": 0.0}, {"entry_price": -5.0}, {"stop_loss": -1.0}, {"stop_loss": 0.0},
     {"take_profits": tps(0.0)}, {"entry_price": math.nan}],
)
def test_non_positive_prices_block(override):
    plan, issues = build(act(**override))
    assert plan is None
    assert issue(issues, "invalid_price").blocking


def test_no_entry_and_no_market_price_blocks():
    plan, issues = build(act(entry_price=None), market=None)
    assert plan is None
    i = issue(issues, "price_unknown")
    assert i.blocking and "fiyat bilinmiyor" in i.message_tr


def test_no_entry_uses_market_price_as_reference():
    plan, issues = build(act(entry_price=None))
    assert issues == []
    assert plan.entry_type is EntryType.MARKET
    assert plan.entry_price == ETH_MARKET
    assert "anlık fiyat" in plan.summary_tr


def test_no_entry_side_check_uses_market():
    plan, issues = build(act(entry_price=None, stop_loss=2240))
    assert plan is None
    i = issue(issues, "sl_wrong_side")
    assert "piyasa 2232.85" in i.message_tr


def test_entry_far_from_market_asks_and_uses_limit():
    plan, issues = build(act(), market=2500)
    assert plan is not None
    i = issue(issues, "entry_far_from_market")
    assert not i.blocking and "%12" in i.message_tr
    assert plan.entry_type is EntryType.LIMIT and plan.entry_price == 2232.18


def test_invalid_market_price_treated_as_unknown():
    plan, issues = build(act(), market=0.0)
    assert plan is not None
    assert codes(issues) == ["market_price_unknown"]


def test_market_price_unknown_asks_and_uses_limit():
    plan, issues = build(act(), market=None)
    assert plan is not None
    assert codes(issues) == ["market_price_unknown"]
    assert plan.entry_type is EntryType.LIMIT and plan.entry_price == 2232.18


# --------------------------------------------------------------------------- #
# Open: entry type
# --------------------------------------------------------------------------- #
def test_auto_market_within_tolerance():
    plan, issues = build(act(), market=2232.18 * 1.005)  # exactly 0.5%
    assert plan.entry_type is EntryType.MARKET and issues == []


def test_auto_limit_beyond_tolerance():
    plan, issues = build(act(), market=2250)  # 0.8% above a long entry: wait for the pullback
    assert plan.entry_type is EntryType.LIMIT and plan.entry_price == 2232.18
    assert issues == []


def test_auto_limit_marketable_warns():
    # long entry above the market (breakout): a buy limit would fill immediately
    plan, issues = build(act(stop_loss=2180), market=2210)
    assert plan.entry_type is EntryType.LIMIT
    assert codes(issues) == ["limit_entry_marketable"]


def test_explicit_limit_wins_over_auto():
    plan, issues = build(act(entry_type=EntryType.LIMIT))
    assert plan.entry_type is EntryType.LIMIT and plan.entry_price == 2232.18 and issues == []


def test_explicit_market_wins_over_auto_and_warns_on_drift():
    plan, issues = build(act(entry_type=EntryType.MARKET), market=2250)
    assert plan.entry_type is EntryType.MARKET
    assert plan.entry_price == 2232.18
    assert codes(issues) == ["market_far_from_entry"]


def test_explicit_limit_without_price_blocks():
    plan, issues = build(act(entry_type=EntryType.LIMIT, entry_price=None))
    assert plan is None
    assert issue(issues, "missing_limit_price").blocking


def test_entry_mode_market():
    plan, issues = build(act(), market=2250, risk=RiskConfig(entry_mode="market"))
    assert plan.entry_type is EntryType.MARKET and issues == []


def test_entry_mode_limit():
    plan, issues = build(act(), risk=RiskConfig(entry_mode="limit"))
    assert plan.entry_type is EntryType.LIMIT and plan.entry_price == 2232.18 and issues == []


def test_entry_mode_limit_without_entry_falls_back_to_market():
    plan, issues = build(act(entry_price=None), risk=RiskConfig(entry_mode="limit"))
    assert plan.entry_type is EntryType.MARKET and plan.entry_price == ETH_MARKET
    assert codes(issues) == ["limit_without_entry"]


def test_market_beyond_tp1_limit_asks():
    plan, issues = build(act(take_profits=tps(2310.6, 2400)), market=2320)
    assert plan is not None and plan.entry_type is EntryType.LIMIT
    i = issue(issues, "market_beyond_tp")
    assert not i.blocking


def test_market_beyond_tp1_market_entry_blocks():
    plan, issues = build(act(entry_type=EntryType.MARKET, take_profits=tps(2310.6, 2400)), market=2320)
    assert plan is None
    assert issue(issues, "market_beyond_tp").blocking


# --------------------------------------------------------------------------- #
# Open: risk
# --------------------------------------------------------------------------- #
def test_risk_defaults_to_default_r():
    plan, issues = build(act(risk_r=None))
    assert issues == []
    assert plan.risk_r == 1.0 and plan.risk_pct == 2.0


def test_risk_default_r_configurable():
    plan, _ = build(act(risk_r=None), risk=RiskConfig(default_r=0.5))
    assert plan.risk_r == 0.5 and plan.risk_pct == 1.0


def test_risk_missing_asks_when_configured():
    plan, issues = build(act(risk_r=None), risk=RiskConfig(ask_if_r_missing=True))
    assert plan is not None and plan.risk_r == 1.0
    assert codes(issues) == ["risk_r_missing"]


def test_half_r_is_one_percent():
    plan, _ = build(act(risk_r=0.5))
    assert plan.risk_pct == 1.0


def test_risk_per_r_configurable():
    plan, _ = build(act(risk_r=1.0), risk=RiskConfig(risk_per_r_pct=3.0))
    assert plan.risk_pct == 3.0


def test_risk_above_cap_asks_but_keeps_signal_r():
    plan, issues = build(act(risk_r=3.0))
    assert plan is not None and plan.risk_r == 3.0 and plan.risk_pct == 6.0
    i = issue(issues, "risk_r_too_high")
    assert not i.blocking and "3R" in i.message_tr and "2R" in i.message_tr


def test_risk_at_cap_is_fine():
    plan, issues = build(act(risk_r=2.0))
    assert issues == [] and plan.risk_pct == 4.0


@pytest.mark.parametrize("bad", [0.0, -1.0])
def test_invalid_risk_blocks(bad):
    plan, issues = build(act(risk_r=bad))
    assert plan is None and issue(issues, "invalid_risk").blocking


# --------------------------------------------------------------------------- #
# Open: take profits
# --------------------------------------------------------------------------- #
def test_multi_tp_equal_split_sorted_nearest_first():
    plan, issues = build(act(take_profits=tps(2400, 2310.6, 2350)))
    assert issues == []
    assert [tp.price for tp in plan.take_profits] == [2310.6, 2350, 2400]
    fr = [tp.close_fraction for tp in plan.take_profits]
    assert all(f == pytest.approx(1 / 3) for f in fr)
    assert math.isclose(sum(fr), 1.0, abs_tol=1e-12)


def test_two_tps_split_half_half():
    plan, _ = build(act(take_profits=tps(2310.6, 2350)))
    assert [tp.close_fraction for tp in plan.take_profits] == [0.5, 0.5]


def test_short_tps_sorted_descending():
    action = act(side=Side.SHORT, entry_price=2232.18, stop_loss=2260, take_profits=tps(2100, 2200, 2150))
    plan, _ = build(action, market=2232)
    assert [tp.price for tp in plan.take_profits] == [2200, 2150, 2100]


def test_last_only_split():
    plan, issues = build(act(take_profits=tps(2350, 2310.6, 2400)), risk=RiskConfig(multi_tp_split="last_only"))
    assert issues == []
    assert plan.take_profits == [TakeProfit(price=2400, close_fraction=1.0)]


def test_explicit_split_kept_and_follows_prices():
    plan, issues = build(act(take_profits=tps(2400, 2310.6, 2350, fractions=[0.4, 0.3, 0.3])))
    assert issues == []
    assert [(tp.price, tp.close_fraction) for tp in plan.take_profits] == [
        (2310.6, pytest.approx(0.3)), (2350, pytest.approx(0.3)), (2400, pytest.approx(0.4))
    ]
    assert math.isclose(sum(tp.close_fraction for tp in plan.take_profits), 1.0, abs_tol=1e-12)


def test_explicit_split_wins_over_last_only():
    plan, _ = build(act(take_profits=tps(2310.6, 2400, fractions=[0.5, 0.5])),
                    risk=RiskConfig(multi_tp_split="last_only"))
    assert [tp.close_fraction for tp in plan.take_profits] == [0.5, 0.5]


def test_explicit_split_close_to_one_is_rescaled():
    plan, issues = build(act(take_profits=tps(2310.6, 2350, 2400, fractions=[0.33, 0.33, 0.33])))
    assert issues == []
    assert math.isclose(sum(tp.close_fraction for tp in plan.take_profits), 1.0, abs_tol=1e-12)


def test_invalid_split_asks_and_splits_equally():
    plan, issues = build(act(take_profits=tps(2310.6, 2350, fractions=[0.5, 0.2])))
    assert codes(issues) == ["tp_split_invalid"]
    assert [tp.close_fraction for tp in plan.take_profits] == [0.5, 0.5]


def test_duplicate_tp_prices_merged():
    plan, _ = build(act(take_profits=tps(2310.6, 2310.6, 2350)))
    assert [tp.price for tp in plan.take_profits] == [2310.6, 2350]


def test_no_take_profit_asks():
    plan, issues = build(act(take_profits=[]))
    assert plan is not None and plan.take_profits == []
    i = issue(issues, "no_take_profit")
    assert not i.blocking


# --------------------------------------------------------------------------- #
# Open: confidence / action types
# --------------------------------------------------------------------------- #
def test_low_confidence_and_ambiguities_ask():
    plan, issues = build(act(confidence=0.62, ambiguities=["TP etiketi bulanık."]))
    assert plan is not None
    assert codes(issues) == ["low_confidence", "ambiguity"]
    assert issues[0].message_tr == "Güven düşük (%62)."
    assert issues[1].message_tr == "TP etiketi bulanık."
    assert not any(i.blocking for i in issues)


def test_blocking_result_still_lists_non_blocking_issues():
    plan, issues = build(act(stop_loss=None, confidence=0.5))
    assert plan is None
    assert "missing_stop_loss" in codes(issues) and "low_confidence" in codes(issues)


def test_management_action_rejected_by_open_builder():
    plan, issues = build(act(action=ActionType.CLOSE))
    assert plan is None and issue(issues, "unsupported_action").blocking


def test_incomplete_action_without_stop_blocks():
    plan, issues = build(act(action=ActionType.INCOMPLETE, stop_loss=None, risk_r=None))
    assert plan is None and "missing_stop_loss" in codes(issues)


# --------------------------------------------------------------------------- #
# Management
# --------------------------------------------------------------------------- #
def trade(
    tid: int = 7, *, side: Side = Side.LONG, status: TradeStatus = TradeStatus.OPEN,
    entry: float | None = 2232.18, stop: float = 2205.11, asset: str = "ETH",
) -> TradeRecord:
    plan = TradePlan(
        plan_id=f"p{tid}", base_asset=asset, side=side, entry_type=EntryType.MARKET, entry_price=entry,
        stop_loss=stop, take_profits=tps(2310.6), risk_r=1.0, risk_pct=2.0, source_chat_id=-100,
        source_message_ids=[501, 502], created_at=NOW,
    )
    return TradeRecord(id=tid, plan=plan, status=status, current_stop_loss=stop,
                       current_take_profits=plan.take_profits, created_at=NOW, updated_at=NOW)


def mact(action: ActionType, **kw) -> ParsedAction:
    return ParsedAction(action=action, base_asset="ETH", used_message_ids=[600], confidence=0.95, **kw)


def manage(action: ParsedAction, t: TradeRecord | None = None, market: float | None = 2260.0,
           candidates: list[TradeRecord] | None = None, **kw):
    return build_management_plan(action, trade=t, candidates=candidates or [], market_price=market,
                                 source_chat_id=-100, now=NOW, **kw)


def test_trade_not_found():
    plan, issues = manage(mact(ActionType.MOVE_SL, move_sl_to_entry=True))
    assert plan is None and codes(issues) == ["trade_not_found"] and issues[0].blocking


def test_single_candidate_but_unresolved_is_not_found():
    plan, issues = manage(mact(ActionType.CLOSE, close_fraction=1.0), candidates=[trade(7)])
    assert plan is None and codes(issues) == ["trade_not_found"]


def test_ambiguous_trade():
    plan, issues = manage(mact(ActionType.CLOSE, close_fraction=1.0), candidates=[trade(7), trade(9)])
    assert plan is None
    i = issue(issues, "ambiguous_trade")
    assert i.blocking and "#7" in i.message_tr and "#9" in i.message_tr


def test_move_sl_to_entry():
    plan, issues = manage(mact(ActionType.MOVE_SL, move_sl_to_entry=True), trade(), plan_id="m1")
    assert issues == []
    assert plan.action is ActionType.MOVE_SL and plan.trade_id == 7 and plan.plan_id == "m1"
    assert plan.new_stop_loss == 2232.18 and plan.move_sl_to_entry is True
    assert plan.source_message_ids == [600] and plan.source_chat_id == -100 and plan.created_at == NOW
    assert "#7" in plan.summary_tr


def test_move_sl_to_entry_without_entry_blocks():
    plan, issues = manage(mact(ActionType.MOVE_SL, move_sl_to_entry=True), trade(entry=None))
    assert plan is None and issue(issues, "entry_unknown").blocking


def test_move_sl_explicit_price():
    plan, issues = manage(mact(ActionType.MOVE_SL, new_stop_loss=2220.0), trade())
    assert issues == [] and plan.new_stop_loss == 2220.0 and plan.move_sl_to_entry is False
    assert len(plan.plan_id) == 10


def test_move_sl_without_price_blocks():
    plan, issues = manage(mact(ActionType.MOVE_SL), trade())
    assert plan is None and issue(issues, "missing_new_stop").blocking


@pytest.mark.parametrize("new_sl", [2260.0, 2270.0])
def test_long_new_sl_at_or_above_market_blocks(new_sl):
    plan, issues = manage(mact(ActionType.MOVE_SL, new_stop_loss=new_sl), trade(), market=2260.0)
    assert plan is None and issue(issues, "sl_wrong_side").blocking


def test_move_sl_to_entry_when_long_is_in_loss_blocks():
    plan, issues = manage(mact(ActionType.MOVE_SL, move_sl_to_entry=True), trade(), market=2220.0)
    assert plan is None and "sl_wrong_side" in codes(issues)


def test_short_new_sl_below_market_blocks():
    t = trade(side=Side.SHORT, entry=2232.18, stop=2260.0)
    plan, issues = manage(mact(ActionType.MOVE_SL, new_stop_loss=2200.0), t, market=2210.0)
    assert plan is None and "sl_wrong_side" in codes(issues)
    plan, issues = manage(mact(ActionType.MOVE_SL, new_stop_loss=2232.18), t, market=2210.0)
    assert plan is not None and issues == []


def test_move_sl_market_unknown_asks():
    plan, issues = manage(mact(ActionType.MOVE_SL, new_stop_loss=2220.0), trade(), market=None)
    assert plan is not None and codes(issues) == ["market_price_unknown"]


def test_move_sl_widening_asks():
    plan, issues = manage(mact(ActionType.MOVE_SL, new_stop_loss=2190.0), trade())
    assert plan is not None and codes(issues) == ["sl_widened"]


def test_move_sl_on_pending_trade_checked_against_entry():
    t = trade(status=TradeStatus.PENDING)
    # Review 1 (engine_mgmt, M7, finding YONETIM-6): "stop girişe" while the entries still rest is no stop on the
    # wrong side of the entry: the trader is at break-even and the engine cancels the resting entries (a plan, no
    # block). A stop PRICE beyond the entry of a pending trade is still refused.
    plan, issues = manage(mact(ActionType.MOVE_SL, move_sl_to_entry=True), t, market=2260.0)
    assert plan is not None and "sl_wrong_side" not in codes(issues)
    assert "dolmamış girişler iptal edilecek" in plan.summary_tr
    plan, issues = manage(mact(ActionType.MOVE_SL, new_stop_loss=2240.0), t, market=2260.0)
    assert plan is None and "sl_wrong_side" in codes(issues)
    plan, issues = manage(mact(ActionType.MOVE_SL, new_stop_loss=2215.0), t, market=2260.0)
    assert plan is not None and issues == []


def test_move_tp_sorted_and_split():
    plan, issues = manage(mact(ActionType.MOVE_TP, take_profits=tps(2400, 2350)), trade())
    assert issues == []
    assert [(tp.price, tp.close_fraction) for tp in plan.take_profits] == [(2350, 0.5), (2400, 0.5)]
    assert plan.new_stop_loss is None and plan.close_fraction is None


def test_move_tp_three_levels_sum_to_one():
    plan, _ = manage(mact(ActionType.MOVE_TP, take_profits=tps(2400, 2350, 2310.6)), trade())
    assert math.isclose(sum(tp.close_fraction for tp in plan.take_profits), 1.0, abs_tol=1e-12)


def test_move_tp_explicit_split_kept():
    plan, _ = manage(mact(ActionType.MOVE_TP, take_profits=tps(2400, 2350, fractions=[0.7, 0.3])), trade())
    assert [(tp.price, tp.close_fraction) for tp in plan.take_profits] == [
        (2350, pytest.approx(0.3)), (2400, pytest.approx(0.7))
    ]


def test_move_tp_short_sorted_descending():
    t = trade(side=Side.SHORT, entry=2232.18, stop=2260.0)
    plan, issues = manage(mact(ActionType.MOVE_TP, take_profits=tps(2100, 2150)), t, market=2220.0)
    assert issues == [] and [tp.price for tp in plan.take_profits] == [2150, 2100]


def test_move_tp_without_prices_blocks():
    plan, issues = manage(mact(ActionType.MOVE_TP), trade())
    assert plan is None and issue(issues, "missing_take_profits").blocking


def test_move_tp_behind_market_blocks():
    plan, issues = manage(mact(ActionType.MOVE_TP, take_profits=tps(2250, 2400)), trade(), market=2260.0)
    assert plan is None and issue(issues, "tp_wrong_side").blocking


def test_close_partial():
    plan, issues = manage(mact(ActionType.CLOSE, close_fraction=0.5), trade())
    assert issues == [] and plan.close_fraction == 0.5 and plan.action is ActionType.CLOSE


def test_close_full():
    plan, issues = manage(mact(ActionType.CLOSE, close_fraction=1.0), trade())
    assert issues == [] and plan.close_fraction == 1.0


@pytest.mark.parametrize("fraction", [0.0, -0.2, 1.5])
def test_close_fraction_out_of_range_blocks(fraction):
    plan, issues = manage(mact(ActionType.CLOSE, close_fraction=fraction), trade())
    assert plan is None and issue(issues, "invalid_close_fraction").blocking


def test_close_without_fraction_asks_and_closes_all():
    plan, issues = manage(mact(ActionType.CLOSE), trade())
    assert plan is not None and plan.close_fraction == 1.0
    assert codes(issues) == ["close_fraction_missing"]


def test_cancel_pending():
    plan, issues = manage(mact(ActionType.CANCEL), trade(status=TradeStatus.PENDING))
    assert issues == [] and plan.action is ActionType.CANCEL


def test_cancel_filled_trade_asks():
    plan, issues = manage(mact(ActionType.CANCEL), trade(status=TradeStatus.OPEN))
    assert plan is not None and codes(issues) == ["cancel_filled_trade"]


@pytest.mark.parametrize("status", [TradeStatus.CLOSED, TradeStatus.CANCELLED, TradeStatus.FAILED])
def test_inactive_trade_blocks(status):
    plan, issues = manage(mact(ActionType.CLOSE, close_fraction=1.0), trade(status=status))
    assert plan is None and issue(issues, "trade_not_active").blocking


def test_open_action_rejected_by_management_builder():
    plan, issues = manage(mact(ActionType.OPEN), trade())
    assert plan is None and issue(issues, "unsupported_action").blocking


def test_management_does_not_check_confidence():
    action = mact(ActionType.CLOSE, close_fraction=0.5).model_copy(
        update={"confidence": 0.1, "ambiguities": ["Hangi işlem?"]}
    )
    plan, issues = manage(action, trade())
    assert plan is not None and issues == []


# --------------------------------------------------------------------------- #
# low_confidence_issues
# --------------------------------------------------------------------------- #
def test_low_confidence_issues():
    assert low_confidence_issues(act(confidence=0.95), LLM) == []
    assert low_confidence_issues(act(confidence=0.85), LLM) == []  # at threshold: ok
    issues = low_confidence_issues(act(confidence=0.62), LLM)
    assert [(i.code, i.message_tr, i.blocking) for i in issues] == [("low_confidence", "Güven düşük (%62).", False)]


def test_low_confidence_threshold_configurable():
    assert codes(low_confidence_issues(act(confidence=0.9), LLMConfig(min_confidence=0.95))) == ["low_confidence"]


def test_each_ambiguity_is_an_issue():
    issues = low_confidence_issues(act(ambiguities=["SL etiketi okunamadı.", "  ", "Long mu short mu?"]), LLM)
    assert [(i.code, i.message_tr) for i in issues] == [
        ("ambiguity", "SL etiketi okunamadı."), ("ambiguity", "Long mu short mu?")
    ]
    assert not any(i.blocking for i in issues)
