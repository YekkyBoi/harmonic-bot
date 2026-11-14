"""The closing notice opens with the result in R, in the owner's own words (monitor.closing_headline).

R of an account, as the trader counts it = its position's realized_pnl / the position's REAL initial risk (the
money the quantity really opened loses at its stop, stored on the position at the open: data["initial_risk"]),
times the plan's size in R. Not the target amount (risk_amount): a clean 2R trade reads +2R whatever the lot
rounding did. An account whose R is not known is left out, never guessed; the line with the money amounts and the
warnings stay as they were.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from copytrader.brokers import ccxt_broker
from copytrader.engine import monitor
from copytrader.engine.monitor import (
    INITIAL_RISK,
    closing_headline,
    closing_summary_text,
    position_initial_risk,
    position_result_r,
)
from copytrader.engine.orchestrator import capped_real_risk, initial_risk_basis
from copytrader.models import (
    BrokerPosition,
    EntryType,
    PositionStatus,
    Side,
    SizingResult,
    TradePlan,
    TradeRecord,
    TradeStatus,
)

NOW = datetime(2026, 11, 14, 12, 0, tzinfo=UTC)
MONEY = "🏁 #9 ETH SHORT (giriş 1962.15) kapandı. Gerçekleşen K/Z: "
WIN, LOSS, FLAT = "🟢 hooopp {} koyduk başkanım", "🔴 amk {} yedik", "😐 başa baş kapattık ({})"
TARGET = 777.0      # every position's risk_amount (the target the size was computed for): never what R is counted in


def trade(risk_r: float = 1.0, *, note: str | None = None, route: str | None = "kripto",
          status: TradeStatus = TradeStatus.CLOSED) -> TradeRecord:
    plan = TradePlan(plan_id="p9", base_asset="ETH", side=Side.SHORT, entry_type=EntryType.MARKET,
                     entry_price=1962.15, stop_loss=2000.0, risk_r=risk_r, risk_pct=risk_r * 1.0,
                     source_chat_id=-100, created_at=NOW, route=route)
    return TradeRecord(id=9, plan=plan, status=status, current_stop_loss=2000.0, created_at=NOW, updated_at=NOW,
                       note=note)


def pos(broker: str, pnl: float | None, risk: float | None, *, status: PositionStatus = PositionStatus.CLOSED,
        filled: float = 1.0, quantity: float = 1.0, dry_run: bool = False,
        data: dict[str, Any] | None = None) -> BrokerPosition:
    """``risk``: the real initial risk stored at the open for ``quantity`` (None: nothing stored)."""
    d = dict(data or {})
    if risk is not None:
        d[INITIAL_RISK] = {"loss": risk, "quantity": quantity}
    return BrokerPosition(broker=broker, broker_symbol="ETHUSDT", side=Side.SHORT, status=status, quantity=quantity,
                          filled_quantity=filled, realized_pnl=pnl, risk_amount=TARGET, dry_run=dry_run, data=d)


def text(t: TradeRecord, positions: list[BrokerPosition], status: TradeStatus = TradeStatus.CLOSED) -> str:
    return closing_summary_text(t, positions, status)


# --------------------------------------------------------------------------- #
# the three headlines, above the unchanged money line
# --------------------------------------------------------------------------- #
def test_a_win_opens_with_the_r_in_the_owners_words_and_keeps_the_money_line() -> None:
    # binance really risked 10 USD, fx 100 USD; both made twice that
    out = text(trade(), [pos("binance", 20.0, 10.0), pos("fx", 200.0, 100.0)])
    assert out == ("🟢 hooopp +2R koyduk başkanım\n"
                   "🏁 #9 ETH SHORT (giriş 1962.15) kapandı. Gerçekleşen K/Z: binance +20.00, fx +200.00 · 📁 kripto")


def test_a_loss_opens_with_the_r_lost() -> None:
    out = text(trade(), [pos("binance", -10.0, 10.0), pos("fx", -100.0, 100.0)])
    assert out == ("🔴 amk -1R yedik\n"
                   "🏁 #9 ETH SHORT (giriş 1962.15) kapandı. Gerçekleşen K/Z: binance -10.00, fx -100.00 · 📁 kripto")


def test_a_half_r_signal_that_stops_out_reads_minus_half_r() -> None:
    # a 0.5R signal: the positions really risked 5 and 50; losing all of it is half an R in the trader's book
    out = text(trade(0.5), [pos("binance", -5.0, 5.0), pos("fx", -50.0, 50.0)])
    assert out.splitlines()[0] == "🔴 amk -0.5R yedik"
    assert position_result_r(pos("fx", -50.0, 50.0), 0.5) == pytest.approx(-0.5)
    assert position_result_r(pos("fx", 100.0, 50.0), 0.5) == pytest.approx(1.0)     # its 2R target: +1R


def test_a_two_r_signal_that_stops_out_reads_minus_two_r() -> None:
    assert text(trade(2.0), [pos("fx", -200.0, 200.0)]).splitlines()[0] == "🔴 amk -2R yedik"


def test_break_even_within_five_hundredths_of_an_r() -> None:
    out = text(trade(), [pos("binance", 0.4, 10.0), pos("fx", -4.0, 100.0)])       # +0.04R and -0.04R
    assert out == ("😐 başa baş kapattık (0R)\n"
                   "🏁 #9 ETH SHORT (giriş 1962.15) kapandı. Gerçekleşen K/Z: binance +0.40, fx -4.00 · 📁 kripto")


@pytest.mark.parametrize("pnl, first", [
    (5.0, FLAT.format("0R")),               # exactly +0.05R: still flat
    (-5.0, FLAT.format("0R")),              # exactly -0.05R: still flat
    (5.1, WIN.format("+0.1R")),             # just above: a (small) win, never "+0R"
    (-5.1, LOSS.format("-0.1R")),
    (0.0, FLAT.format("0R")),
])
def test_the_edge_between_flat_and_a_result(pnl: float, first: str) -> None:
    assert text(trade(), [pos("fx", pnl, 100.0)]).splitlines()[0] == first


@pytest.mark.parametrize("pnl, shown", [
    (200.0, "+2R"), (150.0, "+1.5R"), (196.0, "+2R"), (104.0, "+1R"), (106.0, "+1.1R"), (1000.0, "+10R"),
    (1234.0, "+12.3R"), (40.0, "+0.4R"),
])
def test_the_number_has_one_decimal_and_no_trailing_zero(pnl: float, shown: str) -> None:
    assert text(trade(), [pos("fx", pnl, 100.0)]).splitlines()[0] == WIN.format(shown)
    assert text(trade(), [pos("fx", -pnl, 100.0)]).splitlines()[0] == LOSS.format(shown.replace("+", "-"))


# --------------------------------------------------------------------------- #
# R is counted in the position's real initial risk, not in the target amount
# --------------------------------------------------------------------------- #
def size(quantity: float, target: float, est: float, fees: float = 0.0) -> SizingResult:
    return SizingResult(quantity=quantity, risk_amount=target, est_loss_at_sl=est, est_fees_at_sl=fees,
                        notional=quantity * 74000.0)


def opened(broker: str, basis: dict[str, float] | None, pnl: float, *, filled: float | None = None,
           target: float = 10.0) -> BrokerPosition:
    """A closed position as the engine leaves it: ``basis`` (initial_risk_basis) stored at the open."""
    quantity = basis["quantity"] if basis else 1.0
    return BrokerPosition(broker=broker, broker_symbol="BTCUSDT", side=Side.LONG, status=PositionStatus.CLOSED,
                          quantity=quantity, filled_quantity=quantity if filled is None else filled,
                          realized_pnl=pnl, risk_amount=target, data={INITIAL_RISK: basis} if basis else {})


def test_a_lot_rounded_down_still_reads_the_traders_r() -> None:
    # BTC long 74000, stop 73300 (700), target 10 USD: 0.01428 BTC, rounded down to 0.012 = 8.40 USD at the stop
    basis = initial_risk_basis(size(0.012, 10.0, 8.4), 1.0, "USDT", "USDT", 74000.0, 74000.0, 73300.0)
    assert basis == {"loss": 8.4, "quantity": 0.012}
    stop = opened("binance", basis, 0.012 * -700.0)
    two_r = opened("binance", basis, 0.012 * 1400.0)
    assert text(trade(), [stop]).splitlines()[0] == "🔴 amk -1R yedik"          # (the target would say -0.8R)
    assert text(trade(), [two_r]).splitlines()[0] == "🟢 hooopp +2R koyduk başkanım"     # (... and +1.7R)


def test_the_brokers_fee_estimate_is_not_part_of_the_r() -> None:
    # binance counts its fees into the size (risk.include_fees_in_risk): 0.012 BTC lose 8.40 by the price and
    # ~0.88 in fees at the stop. The R is the price distance; the fees really paid are in the realized result.
    from copytrader.risk.sizing import compute_linear_size
    s = compute_linear_size(risk_amount=10.0, entry=74000.0, stop=73300.0, qty_step=0.001, min_qty=0.001,
                            fee_rate=0.0005, include_fees=True)
    assert (s.quantity, s.est_loss_at_sl, s.est_fees_at_sl) == (0.012, pytest.approx(9.2838), pytest.approx(0.8838))
    basis = initial_risk_basis(s, 1.0, "USDT", "USDT", 74000.0, 74000.0, 73300.0)
    assert basis == {"loss": pytest.approx(8.4), "quantity": 0.012}
    # the owner's example with 0.89 of fees paid: 25.20 - 0.89 on 8.40 is +2.9R (in the fee-inclusive 9.28: +2.6R)
    assert position_result_r(opened("binance", basis, 25.2 - 0.89), 1.0) == pytest.approx(2.894, abs=0.001)
    # without fees in the size nothing is taken off
    plain = compute_linear_size(risk_amount=10.0, entry=74000.0, stop=73300.0, qty_step=0.001, min_qty=0.001,
                                fee_rate=0.0005, include_fees=False)
    assert (plain.est_loss_at_sl, plain.est_fees_at_sl) == (pytest.approx(9.8), 0.0)


@pytest.mark.parametrize("fees", [-0.1, 8.4, 9.0, float("nan"), float("inf")])
def test_a_fee_estimate_that_does_not_fit_the_loss_stores_nothing(fees: float) -> None:
    assert initial_risk_basis(size(0.012, 10.0, 8.4, fees), 1.0, "USDT", "USDT", 74000.0, 74000.0, 73300.0) is None


def test_a_size_raised_to_the_minimum_lot_reads_the_traders_r_too() -> None:
    # the smallest order loses 18 where 10 was the target: a full stop is still one R of this position
    basis = initial_risk_basis(size(0.002, 10.0, 18.0), 1.0, "USDT", "USDT", 74000.0, 74000.0, 65000.0)
    assert position_result_r(opened("binance", basis, -18.0), 1.0) == pytest.approx(-1.0)


def test_the_owners_example_half_at_two_r_the_rest_at_four_r_is_three_r() -> None:
    # "4R hedef, yarısı 2R'da kapandı (+1R), kalanı 4R'da (+2R): toplam +3R". 0.012 BTC, 700 to the stop:
    # 0.006 x 1400 + 0.006 x 2800 = 25.20 on 8.40 at risk (realized_pnl sums the partial closes)
    basis = initial_risk_basis(size(0.012, 10.0, 8.4), 1.0, "USDT", "USDT", 74000.0, 74000.0, 73300.0)
    done = opened("binance", basis, 0.006 * 1400.0 + 0.006 * 2800.0)
    assert position_result_r(done, 1.0) == pytest.approx(3.0)
    assert text(trade(), [done]).splitlines()[0] == "🟢 hooopp +3R koyduk başkanım"
    # half at 2R, the rest stopped at the entry: +1R; half at 2R, the rest at the first stop: +0.5R
    assert position_result_r(opened("binance", basis, 0.006 * 1400.0), 1.0) == pytest.approx(1.0)
    assert position_result_r(opened("binance", basis, 0.006 * 1400.0 - 0.006 * 700.0), 1.0) == pytest.approx(0.5)


def test_a_size_limited_close_to_the_stop_counts_what_was_really_at_risk() -> None:
    # live price 73500 (200 from the stop), sized as if entered at 73650 (350): 0.028 BTC lose 9.80 from there,
    # 5.60 from the live entry. That is the figure of the owner's warning (capped_real_risk) and the one stored.
    s = size(0.028, 10.0, 9.8)
    basis = initial_risk_basis(s, 1.0, "USDT", "USDT", 73500.0, 73650.0, 73300.0)
    assert basis is not None and basis["loss"] == pytest.approx(5.6)
    assert basis["loss"] == capped_real_risk(s, 1.0, "USDT", "USDT", 73500.0, 73650.0, 73300.0)
    assert position_result_r(opened("binance", basis, 0.028 * -200.0), 1.0) == pytest.approx(-1.0)


def test_a_limit_entry_that_filled_in_part_risked_that_part() -> None:
    # 0.012 sent (8.40 at the stop), 0.004 filled before the rest was cancelled: 2.80 was at risk
    basis = {"loss": 8.4, "quantity": 0.012}
    part = opened("binance", basis, 0.004 * 1400.0, filled=0.004)
    assert position_initial_risk(part) == pytest.approx(2.8)
    assert text(trade(), [part]).splitlines()[0] == "🟢 hooopp +2R koyduk başkanım"


@pytest.mark.parametrize("est", [0.0, -1.0, float("nan"), float("inf")])
def test_no_figure_is_stored_when_the_sizer_gave_no_loss_at_the_stop(est: float) -> None:
    # (capped_real_risk would fall back on the target here: that must never become the R's unit)
    assert initial_risk_basis(size(0.012, 10.0, est), 1.0, "USDT", "USDT", 74000.0, 74000.0, 73300.0) is None


def test_storing_the_figure_never_raises() -> None:
    assert initial_risk_basis(size(0.012, 10.0, 8.4), 1.0, "USDT", "EUR", 74000.0, 73300.0, 73300.0) is None   # x / 0
    assert initial_risk_basis(None, 1.0, "USDT", "USDT", 74000.0, 74000.0, 73300.0) is None   # type: ignore[arg-type]
    assert initial_risk_basis(size(0.0, 10.0, 8.4), 1.0, "USDT", "USDT", 74000.0, 74000.0, 73300.0) is None


# --------------------------------------------------------------------------- #
# several accounts
# --------------------------------------------------------------------------- #
def test_a_win_and_a_loss_have_no_headline() -> None:
    positions = [pos("binance", 12.5, 10.0), pos("fx", -100.0, 100.0)]
    out = text(trade(), positions)
    assert out == "🏁 #9 ETH SHORT (giriş 1962.15) kapandı. Gerçekleşen K/Z: binance +12.50, fx -100.00 · 📁 kripto"
    assert closing_headline(trade(), positions, TradeStatus.CLOSED) is None


def test_a_win_next_to_a_flat_account_is_a_win_with_the_accounts_named() -> None:
    out = text(trade(), [pos("binance", 20.0, 10.0), pos("fx", 2.0, 100.0)])        # +2R and +0.02R
    assert out == ("🟢 hooopp binance +2R · fx 0R koyduk başkanım\n"
                   + MONEY + "binance +20.00, fx +2.00 · 📁 kripto")
    # a flat account a little below zero does not turn the win into "mixed"
    out = text(trade(), [pos("binance", 20.0, 10.0), pos("fx", -4.0, 100.0)])       # +2R and -0.04R
    assert out.splitlines()[0] == "🟢 hooopp binance +2R · fx 0R koyduk başkanım"


def test_a_loss_next_to_a_flat_account_is_a_loss_with_the_accounts_named() -> None:
    # fx stopped while tickmill was closed at about zero (the accounts in the order of the money line below)
    out = text(trade(route="fx"), [pos("fx", -99.75, 100.0), pos("tickmill", -0.0004323, 45.0)])
    assert out == ("🔴 amk fx -1R · tickmill 0R yedik\n"
                   + MONEY + "fx -99.75, tickmill -0.00 · 📁 fx")
    out = text(trade(), [pos("tickmill", 2.0, 45.0), pos("fx", -99.75, 100.0)])      # +0.04R and -1R
    assert out.splitlines()[0] == "🔴 amk tickmill 0R · fx -1R yedik"


def test_a_win_and_a_loss_have_no_headline_whatever_else_is_flat() -> None:
    positions = [pos("binance", 20.0, 10.0), pos("tickmill", 0.0, 45.0), pos("fx", -100.0, 100.0)]
    assert closing_headline(trade(), positions, TradeStatus.CLOSED) is None
    assert text(trade(), positions) == MONEY + "binance +20.00, tickmill +0.00, fx -100.00 · 📁 kripto"
    # just outside the flat band on both sides: still a win and a loss
    edge = [pos("binance", 0.6, 10.0), pos("fx", -6.0, 100.0)]                      # +0.06R and -0.06R
    assert closing_headline(trade(), edge, TradeStatus.CLOSED) is None


def test_several_flat_accounts_share_the_bare_zero() -> None:
    out = text(trade(), [pos("binance", 0.5, 10.0), pos("tickmill", -2.25, 45.0), pos("fx", 0.0, 100.0)])
    assert out.splitlines()[0] == "😐 başa baş kapattık (0R)"


def test_accounts_that_differ_are_each_named() -> None:
    win = text(trade(), [pos("binance", 19.0, 10.0), pos("fx", 210.0, 100.0)])
    assert win.splitlines()[0] == "🟢 hooopp binance +1.9R · fx +2.1R koyduk başkanım"
    loss = text(trade(), [pos("binance", -9.0, 10.0), pos("fx", -110.0, 100.0)])
    assert loss.splitlines()[0] == "🔴 amk binance -0.9R · fx -1.1R yedik"
    assert loss.splitlines()[1] == MONEY + "binance -9.00, fx -110.00 · 📁 kripto"


def test_accounts_that_round_to_the_same_value_share_one_number() -> None:
    out = text(trade(), [pos("binance", 19.6, 10.0), pos("fx", 204.0, 100.0)])       # +1.96R and +2.04R
    assert out.splitlines()[0] == "🟢 hooopp +2R koyduk başkanım"


# --------------------------------------------------------------------------- #
# an account whose R is not known is left out, never guessed
# --------------------------------------------------------------------------- #
def test_a_position_of_an_older_version_has_no_r_the_target_amount_is_not_used_instead() -> None:
    # binance has a result and a target amount (risk_amount) but no stored initial risk: its R is unknown. The
    # headline says whose R it is, so that "+2R" is not read as the result of both; the money line has both.
    old = pos("binance", 12.5, None)
    assert old.risk_amount == TARGET and position_result_r(old, 1.0) is None
    out = text(trade(), [old, pos("fx", 200.0, 100.0)])
    assert out == ("🟢 hooopp fx +2R koyduk başkanım\n"
                   "🏁 #9 ETH SHORT (giriş 1962.15) kapandı. Gerçekleşen K/Z: binance +12.50, fx +200.00 · 📁 kripto")
    assert text(trade(), [old]) == MONEY + "binance +12.50 · 📁 kripto"


def test_a_filled_account_without_a_realized_result_is_left_out_and_the_known_one_is_named() -> None:
    # binance closed outside the bot's orders: its result is unknown (None), it may well be a loss
    out = text(trade(), [pos("binance", None, 10.0), pos("fx", -100.0, 100.0)])
    assert out == ("🔴 amk fx -1R yedik\n"
                   "🏁 #9 ETH SHORT (giriş 1962.15) kapandı. Gerçekleşen K/Z: fx -100.00 · 📁 kripto")
    flat = text(trade(), [pos("binance", None, 10.0), pos("fx", 1.0, 100.0)])
    assert flat.splitlines()[0] == "😐 başa baş kapattık (fx 0R)"


def test_no_account_with_a_known_r_means_no_headline() -> None:
    assert text(trade(), [pos("binance", 12.5, None), pos("fx", -100.0, None)]) == (
        MONEY + "binance +12.50, fx -100.00 · 📁 kripto")
    assert text(trade(), [pos("fx", None, 100.0)]) == MONEY + "bilinmiyor · 📁 kripto"


@pytest.mark.parametrize("basis", [
    {"loss": 0.0, "quantity": 1.0}, {"loss": -100.0, "quantity": 1.0}, {"loss": float("nan"), "quantity": 1.0},
    {"loss": float("inf"), "quantity": 1.0}, {"loss": 100.0, "quantity": 0.0}, {"loss": 100.0}, {"quantity": 1.0},
    {"loss": "x", "quantity": 1.0}, {"loss": 100.0, "quantity": None}, {}, 100.0, "100", None,
    {"loss": 100.0, "quantity": 0.5},           # more filled (1.0) than was sent: the figures do not fit together
])
def test_a_stored_figure_that_cannot_be_used_is_unknown(basis: Any) -> None:
    p = pos("fx", 200.0, None, data={INITIAL_RISK: basis})
    assert position_initial_risk(p) is None and position_result_r(p, 1.0) is None
    assert text(trade(), [p]) == MONEY + "fx +200.00 · 📁 kripto"


def test_a_position_that_never_filled_has_no_initial_risk() -> None:
    assert position_initial_risk(pos("fx", 1.0, 100.0, filled=0.0)) is None


@pytest.mark.parametrize("risk_r", [0.0, -1.0, float("nan"), float("inf")])
def test_a_plan_size_that_is_no_positive_number_is_unknown(risk_r: float) -> None:
    assert position_result_r(pos("fx", 200.0, 100.0), risk_r) is None


def test_an_account_that_never_filled_does_not_stand_in_the_way_of_the_bare_number() -> None:
    # tickmill's entry was cancelled unfilled: it did not trade, so "+2R" speaks for every account that did
    cancelled = pos("tickmill", None, None, status=PositionStatus.CANCELLED, filled=0.0)
    failed = pos("fx2", None, None, status=PositionStatus.FAILED, filled=0.0)
    out = text(trade(), [pos("binance", 20.0, 10.0), cancelled, failed])
    assert out.splitlines()[0] == "🟢 hooopp +2R koyduk başkanım"


# --------------------------------------------------------------------------- #
# a result the adapter itself knows to be partial is not a result
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("data", [
    {"closed_outside": 0.008},                                              # ccxt: part closed outside the bot's orders
    {"pnl_incomplete": True},                                               # ccxt: the trade list did not cover the exits
    {"legs": [{"filled": 0.3, "realized": 10.0}, {"filled": 0.3, "realized": None}]},       # MetaTrader: a leg unknown
    {"legs": [{"filled": 0.3, "realized": 10.0}, {"filled": 0.3}]},
    {"legs": [{"filled": 0.3, "realized": 10.0}, "?"]},                     # unreadable: not taken as complete
    {"closed_outside": "x"},
])
def test_a_partial_result_gives_no_r(data: dict[str, Any]) -> None:
    partial = pos("binance", 1.37, 10.0, data=data)                         # "+0.1R" by its realized_pnl
    assert position_result_r(partial, 1.0) is None
    # the only account: no headline at all, the money line as it was
    assert text(trade(), [partial]) == MONEY + "binance +1.37 · 📁 kripto"
    # next to a known account: that one is named, the headline does not speak for the partial one
    out = text(trade(), [partial, pos("fx", -100.0, 100.0)])
    assert out == "🔴 amk fx -1R yedik\n" + MONEY + "binance +1.37, fx -100.00 · 📁 kripto"


@pytest.mark.parametrize("data", [
    {"closed_outside": 0.0}, {"closed_outside": None}, {"pnl_incomplete": False},
    {"legs": [{"filled": 0.3, "realized": 10.0}, {"filled": 0.0, "realized": None, "state": "cancelled"}]},
    {"legs": []},
])
def test_a_complete_result_is_not_taken_for_a_partial_one(data: dict[str, Any]) -> None:
    assert position_result_r(pos("binance", 20.0, 10.0, data=data), 1.0) == pytest.approx(2.0)


def test_the_monitor_and_the_exchange_adapter_use_the_same_key() -> None:
    assert monitor.PNL_INCOMPLETE == ccxt_broker.PNL_INCOMPLETE == "pnl_incomplete"


# --------------------------------------------------------------------------- #
# simulated results are no real money: the manual (hand-traded) broker, paper, dry-run
# --------------------------------------------------------------------------- #
def test_the_manual_broker_is_left_out_of_the_r_and_stays_in_the_money_line() -> None:
    # The message-only broker tracks the owner's hand trade on a simulator (data["simulated"]): its figure is
    # what the instruction would have made, not what the owner's account did. Here it "lost", the real one won.
    manual = pos("breakout", -50.0, 50.0, data={"simulated": True, "manual_asset": "ETH"})
    out = text(trade(), [pos("binance", 20.0, 10.0), manual])
    assert out == ("🟢 hooopp +2R koyduk başkanım\n"
                   "🏁 #9 ETH SHORT (giriş 1962.15) kapandı. Gerçekleşen K/Z: binance +20.00, breakout -50.00 · 📁 kripto")
    assert position_result_r(manual, 1.0) is None


def test_a_trade_on_simulated_accounts_only_has_no_headline() -> None:
    manual = pos("breakout", 100.0, 50.0, data={"simulated": True, "manual_asset": "ETH"})
    assert text(trade(), [manual]) == MONEY + "breakout +100.00 · 📁 kripto"
    dry = pos("binance", 20.0, 10.0, dry_run=True, data={"simulated": True})
    assert text(trade(), [dry]) == MONEY + "binance +20.00 · 📁 kripto"
    assert text(trade(), [pos("binance", 20.0, 10.0, dry_run=True)]) == MONEY + "binance +20.00 · 📁 kripto"


# --------------------------------------------------------------------------- #
# everything else reads exactly as before
# --------------------------------------------------------------------------- #
def test_cancelled_and_failed_trades_read_exactly_as_before() -> None:
    never = [pos("binance", None, 10.0, status=PositionStatus.CANCELLED, filled=0.0)]
    assert text(trade(), never, TradeStatus.CANCELLED) == (
        "🏁 #9 ETH SHORT (giriş 1962.15) iptal edildi. Gerçekleşen K/Z: yok (giriş hiçbir hesapta dolmadı) · 📁 kripto")
    broken = [pos("binance", None, None, status=PositionStatus.FAILED, filled=0.0)]
    assert text(trade(), broken, TradeStatus.FAILED) == (
        "🏁 #9 ETH SHORT (giriş 1962.15) başarısız. Gerçekleşen K/Z: yok (giriş hiçbir hesapta dolmadı) · 📁 kripto")
    # even with a result in R at hand: the headline is for a CLOSED trade only
    won = [pos("binance", 20.0, 10.0)]
    assert text(trade(), won, TradeStatus.CANCELLED) == (
        "🏁 #9 ETH SHORT (giriş 1962.15) iptal edildi. Gerçekleşen K/Z: binance +20.00 · 📁 kripto")
    assert text(trade(), won, TradeStatus.FAILED) == (
        "🏁 #9 ETH SHORT (giriş 1962.15) başarısız. Gerçekleşen K/Z: binance +20.00 · 📁 kripto")
    assert closing_headline(trade(), won, TradeStatus.CANCELLED) is None


def test_the_unknown_open_warning_lines_stay_below_unchanged() -> None:
    # binance and tickmill may hold an order the bot never managed: the headline names the account it speaks for
    t = trade(note="binance: unknown; tickmill: maybe; fx: ok", route=None)
    out = text(t, [pos("fx", 200.0, 100.0)])
    assert out.splitlines() == [
        "🟢 hooopp fx +2R koyduk başkanım",
        "🏁 #9 ETH SHORT (giriş 1962.15) kapandı. Gerçekleşen K/Z: fx +200.00",
        "⚠️ binance: açılış emrinin sonucu bilinmiyordu; emir açıldıysa pozisyonu elle kapatın (bekleyen emir olarak "
        "duruyorsa iptal edin).",
        "⚠️ tickmill: açılış emri gönderilmiş olabilir, sonucu bilinmiyordu; emir açıldıysa pozisyonu elle kapatın "
        "(bekleyen emir olarak duruyorsa iptal edin).",
    ]


def test_an_unknown_open_that_turned_out_to_be_a_position_does_not_force_the_names() -> None:
    # (the note still says "binance: unknown", but binance has its position and its result: nothing is left open)
    t = trade(note="binance: unknown; fx: ok")
    out = text(t, [pos("binance", 20.0, 10.0), pos("fx", 200.0, 100.0)])
    assert out.splitlines() == ["🟢 hooopp +2R koyduk başkanım",
                                MONEY + "binance +20.00, fx +200.00 · 📁 kripto"]


def test_account_names_are_html_escaped_in_the_headline() -> None:
    out = text(trade(), [pos("a<b>&c", 19.0, 10.0), pos("fx", 210.0, 100.0)])
    assert out.splitlines()[0] == "🟢 hooopp a&lt;b&gt;&amp;c +1.9R · fx +2.1R koyduk başkanım"
    assert "a<b>" not in out
    assert out.splitlines()[1] == MONEY + "a&lt;b&gt;&amp;c +19.00, fx +210.00 · 📁 kripto"


def test_the_notice_never_raises_and_falls_back_to_the_text_of_before(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_a: Any, **_k: Any) -> float:
        raise RuntimeError("x")

    monkeypatch.setattr(monitor, "position_result_r", boom)
    assert text(trade(), [pos("fx", 200.0, 100.0)]) == MONEY + "fx +200.00 · 📁 kripto"


def test_the_text_is_the_same_for_the_same_input() -> None:
    t, positions = trade(), [pos("binance", 20.0, 10.0), pos("fx", 200.0, 100.0)]
    before = [p.model_dump() for p in positions]
    assert text(t, positions) == text(t, positions)
    assert [p.model_dump() for p in positions] == before         # nothing is written to the positions
