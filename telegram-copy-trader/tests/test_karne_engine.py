"""The report card's figures from trades the ENGINE itself opened, managed and closed on the fake venues of the
production replica (tests/test_preflight_scenarios.Prod: the real engine, monitor, storage class and adapters; only
the exchange, the terminals and Telegram are fake). The card must say what the closing notice said.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from test_preflight_scenarios import FX, KRIPTO, Prod, _isolate, btc, fx_pair, set_eurusd  # noqa: F401  (autouse)
from test_review1_engine_mgmt import finals, owner_texts, passes, prod  # noqa: F401  (prod: fixture)

from copytrader.app import real_account_names
from copytrader.engine.monitor import position_result_r
from copytrader.karne.data import MODE_ALL, build_report, render_data, summary_text
from copytrader.models import EntryType, Side, TakeProfit, TradeStatus


def window() -> tuple[datetime, datetime]:
    now = datetime.now(UTC)
    return now - timedelta(hours=1), now + timedelta(hours=1)


async def test_the_owners_example_half_at_two_r_the_rest_at_four_r_is_one_row_of_about_three_r(prod: Prod) -> None:  # noqa: F811
    """BTC long 74000, stop 73300: TP1 75400 (2R, half), TP2 76800 (4R, the rest); binance and tickmill each exit
    at their own venue, breakout is traded by hand (its figure is a simulator's)."""
    await prod.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75400.0).model_copy(update={"take_profits": [
        TakeProfit(price=75400.0, close_fraction=0.5), TakeProfit(price=76800.0, close_fraction=0.5)]}), KRIPTO,
        "btc 1R long piyasadan tp 75400 / 76800")
    tp1, tp2 = sorted(prod.binance.trigger_orders("takeProfitPrice"), key=lambda o: float(o["takeProfitPrice"]))
    low, high = sorted(prod.tickmill.positions, key=lambda pid: float(prod.tickmill.positions[pid]["takeProfit"]))
    prod.binance.set_price("BTC", 75400.0, 75402.0)
    prod.tickmill.set_price("BTCUSD", 75350.0, 75352.0)
    prod.binance.fill(tp1["id"])
    prod.tickmill.hit(low, "DEAL_REASON_TP", 75350.0)
    await passes(prod, 2)
    start, end = window()
    accounts = real_account_names(prod.cfg)
    assert accounts == ["binance", "fx", "tickmill"]                 # breakout (message-only) is no real account

    # half of it is closed, the trade is still open: it is on no card yet (not even with its realized half)
    assert (await prod.trades())[0].status is TradeStatus.OPEN
    assert (await build_report(prod.db, start, end, accounts=accounts)).rows == []

    prod.binance.set_price("BTC", 76800.0, 76802.0)
    prod.tickmill.set_price("BTCUSD", 76750.0, 76752.0)
    prod.binance.fill(tp2["id"])
    prod.tickmill.hit(high, "DEAL_REASON_TP", 76750.0)
    await passes(prod, 3)
    assert (await prod.trades())[0].status is TradeStatus.CLOSED
    (notice,) = finals(owner_texts(prod))
    assert notice.splitlines()[0] == "🟢 hooopp +3R koyduk başkanım"

    report = await build_report(prod.db, start, end, accounts=accounts)
    (row,) = report.rows
    pos = await prod.positions(1)
    own = {name: position_result_r(pos[name], 1.0) for name in ("binance", "tickmill")}
    assert own["binance"] == pytest.approx(2.96, abs=0.01) and own["tickmill"] == pytest.approx(2.99, abs=0.01)
    assert (row.pair, row.side, row.outcome, row.approx) == ("BTC", "LONG", "win", False)
    assert row.r == pytest.approx((own["binance"] + own["tickmill"]) / 2)       # the mean of the two real accounts
    assert row.note == "TP1 · TP2"                                    # both accounts say so (orders / deal reasons)
    assert report.total_r == pytest.approx(row.r) and report.win_rate == pytest.approx(100.0)
    assert [(a.name, a.r) for a in report.accounts] == [
        ("binance", pytest.approx(own["binance"])), ("fx", 0.0), ("tickmill", pytest.approx(own["tickmill"]))]
    text = summary_text(report)
    assert "BTC LONG · <b>+3.0R</b> · TP1 · TP2" in text and "<b>Toplam: +3.0R</b> (1 işlemin R toplamı)" in text
    assert "breakout" not in text and "149.73" not in text and "24.89" not in text      # no simulator, no money
    data = render_data(report)
    assert data["sample"] is False and [t["note"] for t in data["trades"]] == ["TP1 · TP2"]
    # the card of every trade so far shows the same exact figure (nothing is approximate here)
    everything = await build_report(prod.db, None, end, mode=MODE_ALL, accounts=accounts)
    assert everything.approximate == 0 and everything.rows[0].r == pytest.approx(row.r)


async def test_accounts_that_ended_differently_the_mean_of_their_r_and_no_note(prod: Prod) -> None:  # noqa: F811
    """EURUSD short: the prop account (fx) is stopped out at its venue, then the trader closes and the friend's
    tickmill is closed at about its entry. The closing notice named "tickmill 0R · fx -1R"."""
    from copytrader.brokers.mt5_broker import MT5
    from copytrader.models import ActionType
    from test_review1_engine_mgmt import manage

    set_eurusd(prod, 1.10872, 1.10874)
    sig = await prod.say(fx_pair("EURUSD", Side.SHORT, 1.10872, 1.11005, 1.10654, entry_type=EntryType.MARKET), FX,
                         "Eu short")
    (ticket,) = list(prod.terminal.positions)
    prod.terminal.set_price("EURUSD", 1.11003, 1.11005)
    prod.tickmill.set_price("EURUSD", 1.11001, 1.11003)
    prod.terminal.hit(ticket, MT5.DEAL_REASON_SL, 1.11005)
    await passes(prod, 1)
    prod.notifier.notifications.clear()
    await prod.say(manage(ActionType.CLOSE, 1, sig, "EUR", close_fraction=1.0, summary_tr="stop oldu"), FX, "Stop",
                   reply_to=sig)
    assert (await prod.trades())[0].status is TradeStatus.CLOSED
    (notice,) = finals(owner_texts(prod))
    assert notice.splitlines()[0] == "🔴 amk tickmill 0R · fx -1R yedik"

    start, end = window()
    report = await build_report(prod.db, start, end, accounts=real_account_names(prod.cfg))
    (row,) = report.rows
    pos = await prod.positions(1)
    fx_r, tickmill_r = position_result_r(pos["fx"], 1.0), position_result_r(pos["tickmill"], 1.0)
    assert fx_r == pytest.approx(-1.0, abs=0.02) and tickmill_r is not None and abs(tickmill_r) <= 0.05
    assert (row.pair, row.side) == ("EURUSD", "SHORT")
    assert row.r == pytest.approx((fx_r + tickmill_r) / 2) and row.outcome == "loss"      # about -0.5R
    assert row.note == ""                                 # fx: its stop; tickmill: closed by the bot -> no single word
    assert [(a.name, a.r) for a in report.accounts] == [
        ("binance", 0.0), ("fx", pytest.approx(fx_r)), ("tickmill", pytest.approx(tickmill_r))]


# --------------------------------------------------------------------------- #
# Review round (data): the reviewer's engine reproductions (krev/data/test_rev_engine.py) as regression tests
# --------------------------------------------------------------------------- #
async def week_of(p: Prod):  # noqa: F811
    start, end = window()
    return await build_report(p.db, start, end, accounts=real_account_names(p.cfg))


async def test_review_a_stop_out_on_every_account_reads_minus_one_r_and_sl(prod: Prod) -> None:  # noqa: F811
    await prod.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75400.0), KRIPTO, "btc long")
    (sl,) = prod.binance.trigger_orders("stopLossPrice")
    (pid,) = list(prod.tickmill.positions)
    prod.binance.set_price("BTC", 73298.0, 73300.0)
    prod.tickmill.set_price("BTCUSD", 73248.0, 73250.0)
    prod.binance.fill(sl["id"])
    prod.tickmill.hit(pid, "DEAL_REASON_SL", float(prod.tickmill.positions[pid]["stopLoss"]))
    await passes(prod, 3)
    (row,) = (await week_of(prod)).rows
    # (each account's OWN last stop is the signal's stop: binance at the signal's price, tickmill at its offset one)
    assert row.r == pytest.approx(-1.0, abs=0.05) and row.note == "SL" and not row.partial


async def test_review_a_stop_moved_to_the_entry_and_hit_there_reads_be(prod: Prod) -> None:  # noqa: F811
    from copytrader.brokers.mt5_broker import MT5
    from copytrader.models import ActionType
    from test_review1_engine_mgmt import manage

    set_eurusd(prod, 1.10872, 1.10874)
    sig = await prod.say(fx_pair("EURUSD", Side.SHORT, 1.10872, 1.11005, 1.10654, entry_type=EntryType.MARKET), FX,
                         "Eu short")
    set_eurusd(prod, 1.10740, 1.10742)
    await prod.say(manage(ActionType.MOVE_SL, 1, sig, "EUR", move_sl_to_entry=True, summary_tr="stop girişe"), FX,
                   "entry stop", reply_to=sig)
    (ticket,) = list(prod.terminal.positions)
    (pid,) = list(prod.tickmill.positions)
    set_eurusd(prod, 1.10870, 1.10872)
    prod.terminal.hit(ticket, MT5.DEAL_REASON_SL, 1.10872)
    prod.tickmill.hit(pid, "DEAL_REASON_SL", 1.10872)
    await passes(prod, 3)
    (row,) = (await week_of(prod)).rows
    assert row.outcome == "be" and row.note == "BE"
    assert "EURUSD SHORT · <b>0.0R</b> · BE" in summary_text(await week_of(prod))


async def test_review_p1_a_stop_move_one_account_refused_is_not_called_be(prod: Prod) -> None:  # noqa: F811
    """"Stop to entry": the prop terminal (fx) refuses the change; tickmill is stopped at the entry, fx later at its
    ORIGINAL stop with -1R. The closing notice says "tickmill 0R · fx -1R"; the card used to say "−0.5R · BE"."""
    from copytrader.brokers.base import BrokerError
    from copytrader.brokers.mt5_broker import MT5
    from copytrader.models import ActionType
    from test_review1_engine_mgmt import manage

    set_eurusd(prod, 1.10872, 1.10874)
    sig = await prod.say(fx_pair("EURUSD", Side.SHORT, 1.10872, 1.11005, 1.10654, entry_type=EntryType.MARKET), FX,
                         "Eu short")
    fx = prod.brokers["fx"]
    original = fx.update_stop_loss

    async def refuse(*_a: object, **_k: object) -> None:
        raise BrokerError("terminal refused the change")

    fx.update_stop_loss = refuse                    # type: ignore[method-assign]
    set_eurusd(prod, 1.10740, 1.10742)
    await prod.say(manage(ActionType.MOVE_SL, 1, sig, "EUR", move_sl_to_entry=True, summary_tr="stop girişe"), FX,
                   "entry stop", reply_to=sig)
    fx.update_stop_loss = original                  # type: ignore[method-assign]
    (trade,) = await prod.trades()
    assert trade.current_stop_loss == pytest.approx(1.10872)          # the trade's stop: the trader's wish
    (ticket,) = list(prod.terminal.positions)
    (pid,) = list(prod.tickmill.positions)
    set_eurusd(prod, 1.10870, 1.10872)
    prod.tickmill.hit(pid, "DEAL_REASON_SL", 1.10872)
    await passes(prod, 1)
    set_eurusd(prod, 1.11003, 1.11005)
    prod.terminal.hit(ticket, MT5.DEAL_REASON_SL, 1.11005)
    await passes(prod, 3)
    assert (await prod.trades())[0].status is TradeStatus.CLOSED
    pos = await prod.positions(1)
    assert pos["fx"].stop_loss == pytest.approx(1.11005) and pos["tickmill"].stop_loss == pytest.approx(1.10872)
    assert position_result_r(pos["fx"], 1.0) == pytest.approx(-1.0, abs=0.02)
    report = await week_of(prod)
    (row,) = report.rows
    assert row.r == pytest.approx(-0.5, abs=0.03)
    assert row.note == ""                           # fx: its original stop ("SL"); tickmill: at the entry ("BE")
    text = summary_text(report)
    assert "EURUSD SHORT · <b>−0.5R</b>" in text and "BE" not in text


@pytest.mark.parametrize("entry_type", [EntryType.MARKET, EntryType.LIMIT])
@pytest.mark.parametrize("end", ["sl", "tp"])
@pytest.mark.parametrize("earlier_trade", [True, False])
async def test_review_p4_a_limited_size_is_never_turned_into_a_big_win(prod: Prod, entry_type: EntryType,  # noqa: F811
                                                                      end: str, earlier_trade: bool) -> None:
    """Signal EURUSD long 1.1620, stop 1.1600, target 1.1660 (2R); the price is 2.5 pips above the stop when it
    comes: the engine limits the size, the real risk is a quarter of the plan. The weekly card says "—". The card of
    every trade used to print the target as "fx ≈+5.7R".

    ``earlier_trade``: a trade of this version (with a stored real risk) is on record already - the normal case:
    then a position without that figure is never approximated. Without one (the very first trade after the update)
    the trade is taken for an older version's: a size the adapter marked as limited still reads "—", and whatever
    is approximated is marked "≈" and bounded."""
    from copytrader.brokers.mt5_broker import MT5
    from copytrader.karne.data import APPROX_MAX_R

    if earlier_trade:
        await prod.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75400.0), KRIPTO, "btc long")
        (sl,) = prod.binance.trigger_orders("stopLossPrice")
        (first,) = list(prod.tickmill.positions)
        prod.binance.set_price("BTC", 73298.0, 73300.0)
        prod.tickmill.set_price("BTCUSD", 73248.0, 73250.0)
        prod.binance.fill(sl["id"])
        prod.tickmill.hit(first, "DEAL_REASON_SL", float(prod.tickmill.positions[first]["stopLoss"]))
        await passes(prod, 3)
    capped = 2 if earlier_trade else 1
    set_eurusd(prod, 1.16015, 1.16025)
    await prod.say(fx_pair("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660, entry_type=entry_type), FX, "EUR LONG 1R")
    (ticket,) = list(prod.terminal.positions)
    (pid,) = [p for p in prod.tickmill.positions if not earlier_trade or p != first]
    level, reason = (1.1600, "SL") if end == "sl" else (1.1660, "TP")
    set_eurusd(prod, level, level + 0.0001)
    prod.terminal.hit(ticket, getattr(MT5, f"DEAL_REASON_{reason}"), level)
    prod.tickmill.hit(pid, f"DEAL_REASON_{reason}", level)
    await passes(prod, 3)
    assert (await prod.trades())[-1].status is TradeStatus.CLOSED
    pos = await prod.positions(capped)
    week = await week_of(prod)
    assert [row.r for row in week.rows if row.pair == "EURUSD"] == [None]      # no stored real risk: "—"
    everything = await build_report(prod.db, None, window()[1], mode=MODE_ALL, accounts=real_account_names(prod.cfg))
    (row,) = [r for r in everything.rows if r.pair == "EURUSD"]
    if earlier_trade:
        assert row.r is None and not row.approx and all(value is None for value, _a in row.accounts.values())
        assert "≈" not in summary_text(everything)
        return
    for name, (value, approx) in row.accounts.items():
        if pos[name].data.get("size_capped"):
            assert value is None, (name, value)                                # marked by the adapter: never "≈"
        elif value is not None:
            assert approx and abs(value) <= APPROX_MAX_R, (name, value)
    if any(approx for _value, approx in row.accounts.values()):
        assert row.approx and "≈ yaklaşık: 1 işlemde gerçek risk kayıtlı değil" in summary_text(everything)


async def test_review_a_limit_entry_cancelled_before_it_filled_is_on_no_card(prod: Prod) -> None:  # noqa: F811
    from copytrader.models import ActionType
    from test_review1_engine_mgmt import manage

    sig = await prod.say(btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0), KRIPTO, "btc 1R long limit emir")
    await prod.say(manage(ActionType.CANCEL, 1, sig, "BTC", summary_tr="iptal"), KRIPTO, "iptal", reply_to=sig)
    await passes(prod, 2)
    assert (await prod.trades())[0].status in (TradeStatus.CANCELLED, TradeStatus.CLOSED)
    assert (await week_of(prod)).rows == []
    assert (await build_report(prod.db, None, window()[1], mode=MODE_ALL)).rows == []


async def test_review_accounts_that_end_on_different_days_are_on_the_second_weeks_card_whole(prod: Prod) -> None:  # noqa: F811
    from copytrader.brokers.mt5_broker import MT5
    from copytrader.karne.uktime import last_slot
    from copytrader.models import ActionType
    from test_review1_engine_mgmt import manage

    clock = {"now": datetime(2026, 11, 13, 15, 0, tzinfo=UTC)}                 # Friday
    prod.db._clock = lambda: clock["now"]
    set_eurusd(prod, 1.10872, 1.10874)
    sig = await prod.say(fx_pair("EURUSD", Side.SHORT, 1.10872, 1.11005, 1.10654, entry_type=EntryType.MARKET), FX,
                         "Eu short")
    (ticket,) = list(prod.terminal.positions)
    prod.terminal.set_price("EURUSD", 1.11003, 1.11005)
    prod.terminal.hit(ticket, MT5.DEAL_REASON_SL, 1.11005)                     # fx is stopped on Friday
    await passes(prod, 1)
    sunday = last_slot(datetime(2026, 11, 16, 12, 0, tzinfo=UTC))
    accounts = real_account_names(prod.cfg)
    first = await build_report(prod.db, sunday - timedelta(days=7), sunday, accounts=accounts)
    clock["now"] = datetime(2026, 11, 16, 10, 0, tzinfo=UTC)                   # Monday: the trader closes
    await prod.say(manage(ActionType.CLOSE, 1, sig, "EUR", close_fraction=1.0, summary_tr="kapat"), FX, "kapat",
                   reply_to=sig)
    await passes(prod, 2)
    again = await build_report(prod.db, sunday - timedelta(days=7), sunday, accounts=accounts)
    second = await build_report(prod.db, sunday, sunday + timedelta(days=7), accounts=accounts)
    assert first.rows == [] and again.rows == []
    (row,) = second.rows
    assert set(row.accounts) == {"fx", "tickmill"} and all(value is not None for value, _a in row.accounts.values())
    assert render_data(second)["trades"][0]["date"] == "2026-11-16"


# --------------------------------------------------------------------------- #
# Review round (data): a late optional leg that answers after the trade ended (orchestrator._book_late_leg)
# --------------------------------------------------------------------------- #
from test_preflight2_engine import Env, _restore_formatting_global, db, env, late_reports  # noqa: E402,F401  (fixtures)
from test_preflight2_engine import KRIPTO as KRIPTO2  # noqa: E402
from test_preflight2_engine import btc as btc2  # noqa: E402


async def test_review_a_late_leg_that_answers_after_the_card_never_puts_the_trade_on_two_cards(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    """The trade ends on Sunday 21:58 while tickmill's open order is still unanswered; the card is made at 22:00;
    at 22:03 the order answers: the engine books the late position, closes it at once and the trade ends AGAIN, with
    a new end time in the next week. Without care the trade is on the first card without tickmill and on the second
    card once more with another R. With the engine's own look at its orders in flight the trade waits: it is on the
    second card only, whole."""
    from copytrader.engine import orchestrator as orch
    from copytrader.models import BrokerPosition, PositionStatus

    sunday = datetime(2026, 11, 15, 22, 0, tzinfo=UTC)
    week = timedelta(days=7)
    clock = {"now": sunday - timedelta(minutes=5)}
    env.db._clock = lambda: clock["now"]
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    answer = env.tickmill.hold("open_position")
    await env.say(btc2(), KRIPTO2, "btc 1R long")
    (trade,) = await env.trades()

    def stopped_out(pos: BrokerPosition) -> BrokerPosition:
        return pos.model_copy(update={"status": PositionStatus.CLOSED, "remaining_quantity": 0.0,
                                      "realized_pnl": -10.0})

    env.binance.sync_fn = env.breakout.sync_fn = stopped_out
    clock["now"] = sunday - timedelta(minutes=2)
    await env.engine.monitor.run_once()
    ended = await env.trade(trade.id)
    assert ended.status is TradeStatus.CLOSED and ended.closed_at == sunday - timedelta(minutes=2)
    pending = env.engine._has_late_open_leg                 # (what the app hands to the service)
    assert pending(trade.id)

    # Sunday 22:00. As the records alone say it, the trade is this week's (and it lacks tickmill):
    by_records = await build_report(env.db, sunday - week, sunday)
    assert [set(row.accounts) for row in by_records.rows] == [{"binance", "breakout"}]
    # ... with the look at the order in flight it waits for the next card
    first = await build_report(env.db, sunday - week, sunday, pending=pending)
    assert first.rows == [] and first.deferred == [trade.id]

    clock["now"] = sunday + timedelta(minutes=3)            # the late order answers
    answer.set()
    await env.until(lambda: late_reports(env) != [], "the late report")
    assert "geç açılan bu pozisyon hemen kapatıldı" in late_reports(env)[0]
    moved = await env.trade(trade.id)
    assert moved.status is TradeStatus.CLOSED and moved.closed_at == sunday + timedelta(minutes=3)      # a new end
    assert not pending(trade.id)

    # the next Sunday's card: once, with all three accounts
    second = await build_report(env.db, sunday, sunday + week, pending=pending, carried=first.deferred)
    (row,) = second.rows
    assert set(row.accounts) == {"binance", "breakout", "tickmill"} and second.deferred == []
    # the belt: had the first card listed it (no look at the orders in flight), the second would not list it again
    assert (await build_report(env.db, sunday, sunday + week, listed=[trade.id])).rows == []


async def test_review_a_late_leg_that_never_opens_leaves_the_trade_for_the_next_card_all_the_same(
        env: Env, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    """The late order answers "refused": the trade keeps its first end time (before the cut-off). It was left out
    of the first card; the next one takes it although its end is not in that card's period."""
    from copytrader.brokers.base import BrokerError
    from copytrader.engine import orchestrator as orch
    from copytrader.models import BrokerPosition, PositionStatus

    sunday = datetime(2026, 11, 15, 22, 0, tzinfo=UTC)
    week = timedelta(days=7)
    clock = {"now": sunday - timedelta(minutes=5)}
    env.db._clock = lambda: clock["now"]
    monkeypatch.setattr(orch, "LEG_DEADLINE_SEC", 0.2, raising=False)
    answer = asyncio.Event()

    async def refused_late(_req: object) -> BrokerPosition:            # the order's answer comes late, and it is "no"
        await answer.wait()
        raise BrokerError("tickmill emri reddetti")

    env.tickmill.open_position = refused_late                           # type: ignore[method-assign]
    await env.say(btc2(), KRIPTO2, "btc 1R long")
    (trade,) = await env.trades()
    env.binance.sync_fn = env.breakout.sync_fn = lambda pos: pos.model_copy(update={
        "status": PositionStatus.CLOSED, "remaining_quantity": 0.0, "realized_pnl": -10.0})
    clock["now"] = sunday - timedelta(minutes=2)
    await env.engine.monitor.run_once()
    pending = env.engine._has_late_open_leg
    first = await build_report(env.db, sunday - week, sunday, pending=pending)
    assert first.rows == [] and first.deferred == [trade.id]
    clock["now"] = sunday + timedelta(minutes=3)
    answer.set()
    await env.until(lambda: late_reports(env) != [], "the late report")
    assert not pending(trade.id)
    after = await env.trade(trade.id)
    assert after.status is TradeStatus.CLOSED and after.closed_at == sunday - timedelta(minutes=2)      # unchanged
    assert (await build_report(env.db, sunday, sunday + week, pending=pending)).rows == []    # not by its end time
    second = await build_report(env.db, sunday, sunday + week, pending=pending, carried=first.deferred)
    assert [set(row.accounts) for row in second.rows] == [{"binance", "breakout"}]            # ... but carried in
