"""Integration checks of the pre-flight change set on the production replica (real Engine, validator and
broker adapters on fake venues: the environment of test_preflight_scenarios.py).

1. the new risk settings are the ones the validator, the engine and the MetaTrader adapters read, and the
   route's effective settings are used when a signal is evaluated, re-read (correction / edit) and executed;
2. a management message names a PAIR on forex routes: "USDCAD kapattık" never touches an open USDJPY trade;
3. the owner is told when a position's size was limited, also when execution reports are off;
4. the "symbol not found" hint of the native MT5 adapter never suggests remapping a whole currency.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from copytrader.brokers.base import BrokerError
from copytrader.brokers.mt5_broker import Mt5Broker
from copytrader.config import AppConfig, Mt5BrokerConfig, RiskConfig, effective_risk, load_config
from copytrader.models import (
    ActionType,
    EntryType,
    MarketType,
    ParsedAction,
    ParseResult,
    Severity,
    Side,
    TakeProfit,
    TradePlan,
    TradeRecord,
    TradeStatus,
)
from copytrader.parsing import prompts

from test_preflight_scenarios import (
    CHAT,
    DUMMY_ENV,
    EURUSD_LONG,
    FX,
    KRIPTO,
    PROD_YAML,
    FxTerminal,
    Prod,
    _isolate,  # noqa: F401  (autouse fixture: fake terminal clock, no tick waits)
    btc,
    fx_pair,
    load_prod_config,
    prod,  # noqa: F401  (fixture)
    set_eurusd,
)

USDJPY_LONG = ("USDJPY", Side.LONG, 154.20, 154.00, 154.60)
USDCAD_LONG = ("USDCAD", Side.LONG, 1.4020, 1.4000, 1.4060)
CAP_TEXT = "sınırlandı"


def open_symbols(p: Prod) -> dict[str, list[str]]:
    """Symbols of the positions that are open on the two MetaTrader venues right now."""
    return {"fx": sorted(pos.symbol for pos in p.terminal.positions.values()),
            "tickmill": sorted(pos["symbol"] for pos in p.tickmill.positions.values())}


def manage(kind: ActionType, **kw: Any) -> ParsedAction:
    return ParsedAction(action=kind, confidence=0.95, summary_tr=kind.value, **kw)


def close_of(pair: str | None, base: str, quote: str | None = None, **kw: Any) -> ParsedAction:
    """ "<pair> kapattık" as the model returns it: a full close naming the instrument."""
    return manage(ActionType.CLOSE, symbol=pair, base_asset=base, quote_asset=quote, close_fraction=1.0, **kw)


# =========================================================================== #
# 1. the new settings: who reads them, and with which route
# =========================================================================== #
def test_new_risk_settings_exist_with_their_defaults_and_load_from_yaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    risk = RiskConfig()
    assert (risk.max_size_multiple, risk.market_entry_tolerance_r, risk.explicit_market_drift) == (2.0, None, "ask")
    for name, value in DUMMY_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.chdir(tmp_path)
    text = PROD_YAML + (
        "risk:\n  max_size_multiple: 3\n  market_entry_tolerance_r: 0.5\n  explicit_market_drift: follow\n")
    text = text.replace("      kind: fx\n", "      kind: fx\n      market_entry_tolerance_r: 0.1\n"
                                             "      explicit_market_drift: ask\n")
    (tmp_path / "config.yaml").write_text(text, encoding="utf-8")
    cfg = load_config(tmp_path / "config.yaml")
    assert (cfg.risk.max_size_multiple, cfg.risk.market_entry_tolerance_r, cfg.risk.explicit_market_drift) == (
        3.0, 0.5, "follow")
    kripto, fx = cfg.telegram.topics
    assert (fx.market_entry_tolerance_r, fx.explicit_market_drift) == (0.1, "ask")
    # a route's own value wins; a route without one gets the global value
    assert (effective_risk(cfg.risk, fx).market_entry_tolerance_r, effective_risk(cfg.risk, fx).explicit_market_drift) == (
        0.1, "ask")
    assert effective_risk(cfg.risk, kripto) is cfg.risk


def test_effective_risk_of_the_production_routes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = load_prod_config(tmp_path, monkeypatch)
    kripto, fx = cfg.telegram.topics
    on_fx = effective_risk(cfg.risk, fx)
    assert (on_fx.market_entry_tolerance_r, on_fx.explicit_market_drift) == (0.25, "follow")
    assert (on_fx.market_entry_tolerance_pct, on_fx.max_entry_distance_pct, on_fx.ask_if_limit_marketable) == (
        0.03, 1.0, False)
    assert on_fx.max_size_multiple == 2.0
    # the crypto route keeps the global object: nothing differs
    assert effective_risk(cfg.risk, kripto) is cfg.risk
    assert (cfg.risk.market_entry_tolerance_r, cfg.risk.explicit_market_drift) == (None, "ask")


@pytest.mark.parametrize("multiple,fx_lots,tickmill_lots", [(1.0, 0.5, 0.22), (2.0, 1.0, 0.44), (3.0, 1.5, 0.66)])
async def test_max_size_multiple_of_the_config_is_what_the_metatrader_adapters_apply(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, multiple: float, fx_lots: float, tickmill_lots: float
) -> None:
    p = await Prod.create(tmp_path, monkeypatch, edit=lambda cfg: setattr(cfg.risk, "max_size_multiple", multiple))
    try:
        set_eurusd(p, 1.16015, 1.16025)                 # 2.5 pips above the stop: uncapped it would be 4 lots
        await p.say(fx_pair(*EURUSD_LONG, entry_type=None), FX, "EUR LONG 1R")
        assert [o[2] for o in p.fx_orders()] == [fx_lots]
        assert [o[2] for o in p.tickmill_orders()] == [tickmill_lots]
    finally:
        await p.close()


@pytest.mark.parametrize("multiple,binance_max", [(1.0, 0.012), (2.0, 0.024), (4.0, 0.036)])
async def test_max_size_multiple_of_the_config_is_what_the_engine_applies_to_market_orders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, multiple: float, binance_max: float
) -> None:
    p = await Prod.create(tmp_path, monkeypatch, edit=lambda cfg: setattr(cfg.risk, "max_size_multiple", multiple))
    try:
        # BTC long 74000 / SL 73300, Binance at 73500 (200 above the stop): approved by the owner
        p.binance.set_price("BTC", 73499.0, 73501.0)
        p.tickmill.set_price("BTCUSD", 73449.0, 73451.0)
        await p.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO, "btc 1R long")
        await p.engine.handle_confirmation(p.notifier.confirmations[0].id, True)
        (entry,) = p.binance.entry_orders()
        # planned 0.012 BTC; sized on the live 201 it is 0.036 (the fees are part of the risk on Binance)
        assert 0.012 <= entry["amount"] <= binance_max
        if multiple >= 4.0:
            assert entry["amount"] == 0.036             # the cap is not reached: exactly the uncapped size
    finally:
        await p.close()


async def test_a_correction_is_validated_with_the_settings_of_the_signals_route(prod: Prod) -> None:
    # fx route, explicit market, 4 pips above the entry, but no take profit: only THAT is asked.
    set_eurusd(prod, 1.16235, 1.16245)
    no_tp = fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET).model_copy(update={"take_profits": []})
    mid = await prod.say(no_tp, FX, "EUR LONG 1R")
    assert prod.asked() == [["no_take_profit"]] and prod.fx_orders() == []

    # the owner types the target; the corrected reading is validated again, with the fx route's settings
    corrected = fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET).model_copy(update={"used_message_ids": [mid]})
    prod.parser.push(ParseResult(actions=[corrected], model="fake"))
    await prod.engine.handle_correction(prod.notifier.confirmations[0].id, "TP 1.1660")
    assert prod.asked()[1] == []                        # no market_far_from_entry: 0.034% is inside 0.25R on fx
    new = prod.notifier.confirmations[1]
    assert isinstance(new.plan, TradePlan) and new.plan.route == "fx" and new.plan.entry_type is EntryType.MARKET

    await prod.engine.handle_confirmation(new.id, True)
    assert prod.fx_orders() == [("BUY", "EURUSD", 0.4, 1.16245, 1.16, 1.166)]
    assert prod.tickmill_orders() == [("market_buy", "EURUSD", 0.18, 1.16, 1.166)]


async def test_an_edited_signal_is_validated_with_the_settings_of_its_route(prod: Prod) -> None:
    set_eurusd(prod, 1.16235, 1.16245)
    no_tp = fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET).model_copy(update={"take_profits": []})
    mid = await prod.say(no_tp, FX, "EUR LONG 1R")
    assert prod.asked() == [["no_take_profit"]]

    # the trader edits the message and adds the target: the pending question is replaced
    edited = fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET).model_copy(update={"used_message_ids": [mid]})
    prod.parser.push(ParseResult(actions=[edited], model="fake"))
    stored = await prod.db.get_message(CHAT, mid)
    assert stored is not None
    await prod.engine.on_message_edited(stored)
    assert prod.asked()[1] == ["signal_edited"]         # only that it was edited; nothing about the 4 pips
    new = prod.notifier.confirmations[1]
    assert isinstance(new.plan, TradePlan) and new.plan.entry_type is EntryType.MARKET


async def test_an_approved_fx_plan_is_executed_with_the_fx_distance_limit(prod: Prod) -> None:
    no_tp = fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET).model_copy(update={"take_profits": []})
    await prod.say(no_tp, FX, "EUR LONG 1R")
    assert prod.asked() == [["no_take_profit"]]
    # while the question waits the price runs 1.2% away: far inside the global 10%, beyond the fx route's 1%
    set_eurusd(prod, 1.17595, 1.17605)

    result = await prod.engine.handle_confirmation(prod.notifier.confirmations[0].id, True)

    assert prod.fx_orders() == [] and prod.tickmill_orders() == []
    assert "fx: anlık fiyat (1.17605) sinyal girişinden (1.162) %1.21 uzak; bu hesapta işlem açılmadı" in result
    assert "tickmill: anlık fiyat (1.17605) sinyal girişinden (1.162) %1.21 uzak" in result


async def test_a_market_signal_that_ran_away_rests_as_a_limit_and_a_close_report_cancels_it(prod: Prod) -> None:
    # fx route, "piyasadan" long at 1.1620 / SL 1.1600, but the price is already 6 pips higher (0.3 of the stop
    # distance): no question; a limit rests at the trader's entry on both accounts.
    set_eurusd(prod, 1.16255, 1.16265)
    first = await prod.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
    assert prod.asked() == []
    assert prod.fx_orders() == [("BUY_LIMIT", "EURUSD", 0.5, 1.162, 1.16, 1.166)]
    assert prod.tickmill_orders() == [("limit_buy", "EURUSD", 0.22, 1.162, 1.16, 1.166)]
    (trade,) = await prod.trades()
    assert trade.status is TradeStatus.PENDING and trade.plan.entry_type is EntryType.LIMIT
    assert len(prod.terminal.orders) == 1 and len(prod.tickmill.orders) == 1

    # the trader closes his trade before our limit filled: both resting orders are removed, nothing is asked
    await prod.say(manage(ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=first), FX, "kapattık",
                   reply_to=first)
    assert prod.asked() == []
    assert prod.terminal.orders == {} and prod.tickmill.orders == {}
    assert prod.terminal.positions == {} and prod.tickmill.positions == {}
    (trade,) = await prod.trades()
    assert trade.status is TradeStatus.CANCELLED


# =========================================================================== #
# 2. management messages name a pair
# =========================================================================== #
async def test_close_naming_another_pair_never_touches_the_open_pair(prod: Prod) -> None:
    await prod.say(fx_pair(*USDJPY_LONG), FX, "USDJPY LONG 1R")
    assert open_symbols(prod) == {"fx": ["USDJPY"], "tickmill": ["USDJPY"]}

    # not a reply; the bot holds no USDCAD trade
    await prod.say(close_of("USDCAD", "USD", "CAD"), FX, "USDCAD kapattık")

    assert open_symbols(prod) == {"fx": ["USDJPY"], "tickmill": ["USDJPY"]}        # untouched
    (trade,) = await prod.trades()
    assert trade.status is TradeStatus.OPEN
    assert prod.asked() == [["trade_not_found"]]        # the owner is told; nothing can be applied


async def test_close_naming_a_pair_picks_it_among_open_pairs_of_the_same_currency(prod: Prod) -> None:
    await prod.say(fx_pair(*USDJPY_LONG), FX, "USDJPY LONG 1R")
    await prod.say(fx_pair(*USDCAD_LONG), FX, "USDCAD LONG 1R")
    assert open_symbols(prod) == {"fx": ["USDCAD", "USDJPY"], "tickmill": ["USDCAD", "USDJPY"]}

    await prod.say(close_of("USDCAD", "USD", "CAD"), FX, "USDCAD kapattık")           # not a reply, no side

    assert prod.asked() == []                           # the pair says which one: no question
    assert open_symbols(prod) == {"fx": ["USDJPY"], "tickmill": ["USDJPY"]}
    assert [t.status for t in await prod.trades()] == [TradeStatus.OPEN, TradeStatus.CLOSED]


@pytest.mark.parametrize("action", [
    close_of(None, "USD", "CAD"),                       # the quote alone names the pair
    close_of("USDCAD", "USD"),                          # ... or the symbol alone
])
async def test_the_pair_may_be_named_by_the_quote_or_by_the_symbol(prod: Prod, action: ParsedAction) -> None:
    await prod.say(fx_pair(*USDJPY_LONG), FX, "USDJPY LONG 1R")
    await prod.say(fx_pair(*USDCAD_LONG), FX, "USDCAD LONG 1R")
    await prod.say(action, FX, "USDCAD kapattık")
    assert prod.asked() == [] and open_symbols(prod)["fx"] == ["USDJPY"]


async def test_a_target_trade_of_another_pair_is_asked_not_applied(prod: Prod) -> None:
    await prod.say(fx_pair(*USDJPY_LONG), FX, "USDJPY LONG 1R")
    (trade,) = await prod.trades()

    # the model matched the open "USD" trade by its base currency, although the message names USDCAD
    await prod.say(close_of("USDCAD", "USD", "CAD", target_trade_id=trade.id), FX, "USDCAD kapattık")

    assert open_symbols(prod) == {"fx": ["USDJPY"], "tickmill": ["USDJPY"]}
    (conf,) = prod.notifier.confirmations
    (issue,) = [i for i in conf.issues if i.code == "asset_mismatch"]
    assert issue.message_tr == "Talimattaki parite (USDCAD) ile eşleşen işlemin paritesi (USDJPY) farklı."


async def test_management_without_a_pair_works_as_before(prod: Prod) -> None:
    first = await prod.say(fx_pair(*USDJPY_LONG), FX, "USDJPY LONG 1R")
    prod.terminal.set_price("USDJPY", 154.495, 154.505)                               # 30 pips in profit
    prod.tickmill.set_price("USDJPY", 154.495, 154.505)
    # a reply to the signal, nothing named
    await prod.say(manage(ActionType.MOVE_SL, move_sl_to_entry=True, refers_to_message_id=first), FX,
                   "entry stop attık", reply_to=first)
    assert prod.asked() == []
    (pos,) = prod.terminal.positions.values()
    assert pos.sl == pytest.approx(154.205)             # the stop sits on the fill price now
    # only the currency named, one open trade of it
    await prod.say(close_of(None, "USD"), FX, "USD kapattık")
    assert prod.asked() == [] and open_symbols(prod) == {"fx": [], "tickmill": []}


async def test_a_coin_is_still_one_instrument_whatever_quote_the_message_names(prod: Prod) -> None:
    await prod.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO, "btc 1R long")
    (trade,) = await prod.trades()
    assert trade.plan.quote_asset == "USDT"

    await prod.say(close_of("BTCUSD", "BTC", "USD"), KRIPTO, "BTCUSD kapattık")       # written with another quote

    assert prod.asked() == []
    (trade,) = await prod.trades()
    assert trade.status is TradeStatus.CLOSED
    assert prod.tickmill.positions == {}


async def test_open_trades_given_to_the_model_name_the_pair(prod: Prod) -> None:
    await prod.say(fx_pair(*USDJPY_LONG), FX, "USDJPY LONG 1R")
    # any later message of the topic that reaches the model (read as "no instruction" here)
    await prod.say(manage(ActionType.NONE), FX, "usdjpy stop seviyesine dikkat")
    assert len(prod.parser.calls) == 2
    _messages, context = prod.parser.calls[-1]
    (summary,) = context.open_trades
    assert (summary["asset"], summary["quote"], summary["side"]) == ("USD", "JPY", "long")


def test_trade_summary_carries_the_quote_only_when_it_is_known() -> None:
    def record(quote: str | None) -> TradeRecord:
        plan = TradePlan(plan_id="p", base_asset="USD", quote_asset=quote, market_type=MarketType.FOREX,
                         side=Side.LONG, entry_type=EntryType.MARKET, entry_price=154.2, stop_loss=154.0,
                         take_profits=[TakeProfit(price=154.6)], risk_r=1.0, risk_pct=2.0, source_chat_id=CHAT,
                         source_message_ids=[1], created_at=datetime.now(UTC))
        return TradeRecord(id=1, plan=plan, status=TradeStatus.OPEN, current_stop_loss=154.0,
                           current_take_profits=list(plan.take_profits), created_at=plan.created_at,
                           updated_at=plan.created_at)

    assert record("JPY").summary()["quote"] == "JPY"
    assert "quote" not in record(None).summary()
    assert record("JPY").summary()["asset"] == "USD"


def test_the_prompt_explains_the_quote_of_open_trades() -> None:
    sp = prompts.SYSTEM_PROMPT
    assert "trade_id, asset, quote (" in sp
    assert "asset USD with quote JPY is USDJPY" in sp
    assert '"USDCAD' in sp and "is never about an open USDJPY trade" in sp


# =========================================================================== #
# 3. a limited size is always reported
# =========================================================================== #
async def test_the_owner_is_told_when_a_size_was_limited_although_execution_reports_are_off(prod: Prod) -> None:
    assert prod.cfg.notifier.notify_on_execution is False
    set_eurusd(prod, 1.16015, 1.16025)                  # 2.5 pips above the stop
    await prod.say(fx_pair(*EURUSD_LONG, entry_type=None), FX, "EUR LONG 1R")

    assert [o[2] for o in prod.fx_orders()] == [1.0]
    (note,) = prod.texts(Severity.WARNING)              # one message, naming both accounts
    head, *lines = note.splitlines()
    assert head == "⚠️ <b>#1 EUR LONG açıldı</b>: miktar sınırlandı, gerçek risk hedefin altında"
    assert [line.split(":")[0] for line in lines] == ["• fx", "• tickmill"]
    assert all(CAP_TEXT in line and "gerçek risk hedefin altında" in line for line in lines)
    assert "1 EURUSD (açık)" in lines[0] and "0.44 EURUSD (açık)" in lines[1]
    assert prod.texts(Severity.ERROR) == [] and prod.asked() == []


async def test_a_limited_size_is_part_of_the_execution_report_when_reports_are_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    p = await Prod.create(tmp_path, monkeypatch,
                          edit=lambda cfg: setattr(cfg.notifier, "notify_on_execution", True))
    try:
        set_eurusd(p, 1.16015, 1.16025)
        await p.say(fx_pair(*EURUSD_LONG, entry_type=None), FX, "EUR LONG 1R")
        (report,) = p.texts()                           # the usual report carries the warning: no second message
        assert report.startswith("✅ <b>#1 EUR LONG açıldı</b>") and report.count(CAP_TEXT) == 2
    finally:
        await p.close()


async def test_no_size_notice_when_the_planned_size_simply_stands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # max_size_multiple 1 = "never more than the planned size": then a position is never larger than planned,
    # there is nothing unusual to report and the owner must not get a warning with every such trade.
    p = await Prod.create(tmp_path, monkeypatch, edit=lambda cfg: setattr(cfg.risk, "max_size_multiple", 1.0))
    try:
        set_eurusd(p, 1.16015, 1.16025)                 # MetaTrader adapters: a marketable limit near the stop
        await p.say(fx_pair(*EURUSD_LONG, entry_type=None), FX, "EUR LONG 1R")
        assert [o[2] for o in p.fx_orders()] == [0.5] and [o[2] for o in p.tickmill_orders()] == [0.22]
        (trade,) = await p.trades()
        for pos in (await p.positions(trade.id)).values():
            assert any("1 katıyla" in w and CAP_TEXT in w for w in pos.data["warnings"])      # the note stays
            assert "size_capped" not in pos.data
        assert p.texts(Severity.WARNING) == []

        p.binance.set_price("BTC", 73499.0, 73501.0)    # engine: an approved market order near the stop
        p.tickmill.set_price("BTCUSD", 73449.0, 73451.0)
        await p.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO, "btc 1R long")
        result = await p.engine.handle_confirmation(p.notifier.confirmations[0].id, True)
        (entry,) = p.binance.entry_orders()
        assert entry["amount"] == 0.012 and CAP_TEXT in result                               # shown in the result
        assert p.texts(Severity.WARNING) == []
    finally:
        await p.close()


async def test_no_size_notice_for_an_ordinary_trade(prod: Prod) -> None:
    await prod.say(fx_pair(*EURUSD_LONG), FX, "EUR LONG 1R")
    assert len(prod.fx_orders()) == 1 and prod.texts() == []
    # a marketable limit that is re-sized below the cap (GBPUSD 10.5 pips above its stop: 0.5 -> 0.95 lots):
    # still nothing to report
    prod.terminal.set_price("GBPUSD", 1.31395, 1.31405)
    prod.tickmill.set_price("GBPUSD", 1.31395, 1.31405)
    await prod.say(fx_pair("GBPUSD", Side.LONG, 1.3150, 1.3130, 1.3190, entry_type=EntryType.LIMIT), FX,
                   "GBP LONG 1R limit")
    (_eur, gbp) = prod.fx_orders()
    assert gbp[:3] == ("BUY", "GBPUSD", 0.95) and prod.texts() == []
    positions = await prod.positions((await prod.trades())[1].id)
    assert not any(CAP_TEXT in w for w in positions["fx"].data["warnings"])
    assert "size_capped" not in positions["fx"].data


async def test_a_capped_resize_no_longer_claims_the_full_risk(prod: Prod) -> None:
    set_eurusd(prod, 1.16015, 1.16025)
    await prod.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.LIMIT), FX, "EUR LONG 1R limit")
    (trade,) = await prod.trades()
    for name, pos in (await prod.positions(trade.id)).items():
        notes = pos.data["warnings"]
        assert pos.data["size_capped"] is True, name
        assert any("yeniden hesaplandı" in n for n in notes) and any(CAP_TEXT in n for n in notes), name
        assert not any("1R'de kalsın" in n for n in notes), name       # it would contradict the cap note


async def test_an_uncapped_resize_still_says_that_the_risk_stays_one_r(prod: Prod) -> None:
    set_eurusd(prod, 1.16095, 1.16105)                  # 10.5 pips above the stop: 0.5 -> 0.95 lots, below the cap
    await prod.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.LIMIT), FX, "EUR LONG 1R limit")
    assert [o[2] for o in prod.fx_orders()] == [0.95]
    (trade,) = await prod.trades()
    notes = (await prod.positions(trade.id))["fx"].data["warnings"]
    assert any("yeniden hesaplandı (risk 1R'de kalsın)" in n for n in notes)
    assert not any(CAP_TEXT in n for n in notes) and prod.texts() == []


# =========================================================================== #
# 4. "symbol not found" on the native MT5 adapter
# =========================================================================== #
async def _fx_broker(names: tuple[str, ...]) -> Mt5Broker:
    cfg = Mt5BrokerConfig(type="mt5", name="fx", login=int(DUMMY_ENV["MT5_LOGIN"]), password="pw", server="s")
    broker = Mt5Broker(cfg, mt5_module=FxTerminal(names))
    broker.retry_delay_sec = 0
    broker.set_risk_config(RiskConfig())
    await broker.connect()
    return broker


async def test_unlisted_currency_pair_hint_never_suggests_remapping_the_whole_currency() -> None:
    broker = await _fx_broker(("EURUSD", "GBPUSD", "XAUUSD"))
    try:
        with pytest.raises(BrokerError) as err:
            await broker.resolve_symbol("GBP", "JPY", MarketType.FOREX, "GBPJPY")
        text = str(err.value)
        assert "GBPJPY paritesi MT5'te bulunamadı (denenen: GBPJPY)" in text
        assert "symbol_map: {GBPJPY: " in text          # keyed by the pair as the signal writes it
        assert "{GBP:" not in text and "GOLD" not in text
        assert "tüm GBP paritelerini" in text           # and why not by the currency
    finally:
        await broker.close()


async def test_metal_and_index_hint_still_suggests_a_symbol_map_by_asset() -> None:
    broker = await _fx_broker(("EURUSD",))
    try:
        with pytest.raises(BrokerError) as err:
            await broker.resolve_symbol("XAU", "USD", MarketType.CFD, "XAUUSD")
        assert "symbol_map: {XAU: GOLD}" in str(err.value)
        with pytest.raises(BrokerError) as err:
            await broker.resolve_symbol("NAS100", None, MarketType.CFD, "NAS100")
        assert "denenen: NAS100USD, NAS100" in str(err.value) and "symbol_map: {NAS100: " in str(err.value)
    finally:
        await broker.close()


@pytest.mark.parametrize("raw_symbol", ["EURJPY", None])
async def test_symbol_map_keyed_by_the_pair_maps_that_pair_only(raw_symbol: str | None) -> None:
    """What the hint above tells the owner to do must work, whether or not the model returned the symbol."""
    cfg = Mt5BrokerConfig(type="mt5", name="fx", login=int(DUMMY_ENV["MT5_LOGIN"]), password="pw", server="s",
                          symbol_map={"EURJPY": "XAUUSD"})       # (any listed symbol stands in for the odd name)
    broker = Mt5Broker(cfg, mt5_module=FxTerminal(("EURUSD", "XAUUSD")))
    broker.retry_delay_sec = 0
    broker.set_risk_config(RiskConfig())
    await broker.connect()
    try:
        mapped = await broker.resolve_symbol("EUR", "JPY", MarketType.FOREX, raw_symbol)
        assert mapped is not None and mapped.broker_symbol == "XAUUSD"
        # the other pairs of the same currency are not touched by a pair key
        eurusd = await broker.resolve_symbol("EUR", "USD", MarketType.FOREX, "EURUSD" if raw_symbol else None)
        assert eurusd is not None and eurusd.broker_symbol == "EURUSD"
    finally:
        await broker.close()


@pytest.mark.parametrize("raw_symbol", ["EURJPY", None])
async def test_symbol_map_keyed_by_the_pair_on_metaapi(prod: Prod, raw_symbol: str | None) -> None:
    tickmill = prod.brokers["tickmill"]
    tickmill.cfg.symbol_map["EURJPY"] = "XAGUSD"                 # (stand-in name, listed on the fake account)
    mapped = await tickmill.resolve_symbol("EUR", "JPY", MarketType.FOREX, raw_symbol)
    assert mapped is not None and mapped.broker_symbol == "XAGUSD"
    eurusd = await tickmill.resolve_symbol("EUR", "USD", MarketType.FOREX, None)
    assert eurusd is not None and eurusd.broker_symbol == "EURUSD"
    btc_cfd = await tickmill.resolve_symbol("BTC", "USDT", MarketType.PERP, "BTCUSDT.P")     # base key, as before
    assert btc_cfd is not None and btc_cfd.broker_symbol == "BTCUSD"


# =========================================================================== #
# 5. the shipped example config documents the new settings and still loads
# =========================================================================== #
ROOT = Path(__file__).resolve().parents[1]
NEW_RISK_KEYS = {"market_entry_tolerance_r", "explicit_market_drift", "max_size_multiple"}


def test_example_config_loads_and_documents_the_new_risk_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import yaml

    text = (ROOT / "config.example.yaml").read_text(encoding="utf-8")
    for name, value in DUMMY_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text(text, encoding="utf-8")
    cfg = load_config(tmp_path / "config.yaml")                   # the file as shipped is a valid config
    assert isinstance(cfg, AppConfig)
    # the three settings are written out under "risk" with their defaults (an empty value = off)
    raw_risk = yaml.safe_load(text)["risk"]
    assert NEW_RISK_KEYS <= set(raw_risk)
    assert (raw_risk["market_entry_tolerance_r"], raw_risk["explicit_market_drift"], raw_risk["max_size_multiple"]) == (
        None, "ask", 2.0)
    assert cfg.risk == RiskConfig()                               # the example changes no default
    # each has a Turkish comment on its own line
    for key in NEW_RISK_KEYS:
        (line,) = [x for x in text.splitlines() if x.strip().startswith(f"{key}:")]
        assert "#" in line and len(line.split("#", 1)[1].strip()) > 20, line


def test_example_config_topic_example_with_the_new_overrides_is_valid() -> None:
    """The commented "Örnek" block under telegram.topics, with every optional line switched on."""
    import re

    import yaml

    from copytrader.config import TopicRoute

    lines = (ROOT / "config.example.yaml").read_text(encoding="utf-8").splitlines()
    start = next(i for i, line in enumerate(lines) if line.strip() == "# Örnek:") + 1
    block: list[str] = []
    for line in lines[start:]:
        if not line.startswith("  #"):
            break
        body = line[4:] if line.startswith("  # ") else ""
        if re.match(r"\s*# [a-z_]+: \S", body):                    # an optional "# key: value" line
            body = body.replace("# ", "", 1)
        block.append(body)
    assert sum("market_entry_tolerance_r: 0.25" in b or "explicit_market_drift: follow" in b for b in block) == 2
    topics = yaml.safe_load("\n".join(block))["topics"]
    routes = [TopicRoute.model_validate(t) for t in topics]
    fx = next(r for r in routes if r.kind == "fx")
    assert (fx.market_entry_tolerance_r, fx.explicit_market_drift) == (0.25, "follow")
    assert (fx.market_entry_tolerance_pct, fx.max_entry_distance_pct, fx.duplicate_tolerance_pct) == (0.03, 1.0, 0.05)
    # exactly the values an fx topic gets when nothing is written
    plain = TopicRoute(name="fx", title_contains=["Fx"], kind="fx")
    assert effective_risk(RiskConfig(), fx) == effective_risk(RiskConfig(), plain)
