"""Review round of the reference_broker price watch (tickmill watching fx's own price): each test is a case a review
found wrong, with what must happen now.

    A1  the whole window is judged by its extremes: the stop or (armed) first target passed anywhere in it cancels,
        whatever else the window holds (a touch earlier in it never shadows them)
    A2  the first-target rule counts from the moment it armed (that account's server ms, stored at once): a window
        read again (bars of the current minute, a restart, after a busy look) never counts what came before
    A4  fx's leg filled and already closed before tickmill's look: tickmill is not opened afterwards
    A5  a touch near the level seen while tickmill cannot act (not connected) is read again at the next look
    A6  fx's own order waits and fx is not connected: the owner is told (throttled)
    A3  (decided) Evet after tickmill's own rule ended its waiting entry opens the other accounts as a fresh trade
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import test_preflight_scenarios as tps
from copytrader.engine.monitor import is_virtual_pending
from copytrader.engine.orchestrator import Engine
from copytrader.models import ActionType, BrokerPosition, EntryType, ParsedAction, PositionStatus, Side, TakeProfit
from fakes import FakeNotifier
from test_mt5_bridge import bridge, eth, term  # noqa: F401  (term: fixture)
from test_preflight_scenarios import FX, KRIPTO, _isolate, btc, fx_pair  # noqa: F401  (_isolate: autouse)
from test_reference_broker import (  # noqa: F401  (env, unsure: fixtures)
    ENTRY, STOP, T0, _armed_watch, _eur_limit, _friend_clean, _fx_ticket, _history, _make, _no_confirm, _settle,
    _tick, env, unsure,
)


async def _pullback(p: Any) -> None:
    """A long limit whose first target (1.1615) is below today's price (1.162): the first-target rule waits until fx
    has been between the entry (1.1600) and the target. The watch starts at T0."""
    tps.set_eurusd(p, 1.16195, 1.16205)
    action = fx_pair("EURUSD", Side.LONG, 1.1600, 1.1580, 1.1615, entry_type=EntryType.LIMIT).model_copy(
        update={"confidence": 0.5})
    _tick(p, 1.16195, 1.16205, T0)
    await p.say(action, FX, "EUR LONG 1R Limit Emir")
    assert (await p.positions(1))["tickmill"].data["virtual_tp_armed"] is False
    await p.engine.monitor.run_virtual_once()


# --------------------------------------------------------------------------- #
# A1: the window by its extremes
# --------------------------------------------------------------------------- #
async def test_touch_then_stop_then_back_in_one_window_cancels(unsure) -> None:
    p, _friend = unsure
    _history(p, [(T0 + 0.5, 1.16090, 1.16098),        # touch
                 (T0 + 0.8, 1.15880, 1.15890)])       # then through the stop 1.1590
    await _armed_watch(p)
    _tick(p, 1.16090, 1.16100, T0 + 1.5)               # back at the level now
    p.tickmill.set_price("EURUSD", 1.16105, 1.16115)
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == []
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED
    assert any("stop seviyesini (1.159) geçti" in t for t in p.texts())


async def test_touch_then_first_target_in_one_window_cancels(unsure) -> None:
    p, _friend = unsure
    _history(p, [(T0 + 0.5, 1.16090, 1.16098),        # touch (the price then runs away: not taken)
                 (T0 + 0.8, 1.16505, 1.16515)])       # and reaches the first target 1.1650
    await _armed_watch(p)
    _tick(p, 1.16300, 1.16310, T0 + 1.5)
    await p.engine.monitor.run_virtual_once()
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED
    _tick(p, 1.16090, 1.16100, T0 + 60)                # back at the level later: nothing to open any more
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == []


async def test_continuous_ticks_through_the_entry_to_the_stop_cancel(unsure) -> None:
    """On ticks a long cannot reach its stop without first printing at / below the entry: the stop still wins."""
    p, _friend = unsure
    _history(p, [(T0 + 0.3, 1.16090, 1.16098), (T0 + 0.5, 1.15880, 1.15890)])
    await _armed_watch(p)
    _tick(p, 1.15950, 1.15960, T0 + 1.5)
    p.tickmill.set_price("EURUSD", 1.15960, 1.15970)
    await p.engine.monitor.run_virtual_once()
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED and p.tickmill_orders() == []


# --------------------------------------------------------------------------- #
# A2: the first-target rule from the moment it armed
# --------------------------------------------------------------------------- #
async def test_bars_pullback_the_re_read_minute_never_counts_before_the_arming(tmp_path: Path,
                                                                              monkeypatch: pytest.MonkeyPatch) -> None:
    p, _friend = await _make(tmp_path, monkeypatch, edit=_no_confirm)
    try:
        await _pullback(p)
        minute = -(-T0 // 60) * 60

        def copy_rates_range(symbol: str, timeframe: int, date_from: int, date_to: int) -> list[dict[str, Any]]:
            # the minute's bar: opened above the target 1.1615, dipped between the entry and the target
            bars = [{"time": minute, "open": 1.16195, "high": 1.16200, "low": 1.16100, "close": 1.16120,
                     "spread": 10}]
            return [b for b in bars if date_from <= b["time"] <= date_to]

        p.terminal.copy_rates_range = copy_rates_range                           # type: ignore[method-assign]
        _tick(p, 1.16120, 1.16130, minute + 10)                                  # between the entry and the target
        await p.engine.monitor.run_virtual_once()
        pos = (await p.positions(1))["tickmill"]
        assert is_virtual_pending(pos) and pos.data["virtual_tp_armed"] is True
        assert pos.data["ref_tp_msc"] == (minute + 10) * 1000                   # the moment it armed, stored
        _tick(p, 1.16125, 1.16135, minute + 12)                                  # still between; never at the target
        await p.engine.monitor.run_virtual_once()
        assert is_virtual_pending((await p.positions(1))["tickmill"])
    finally:
        await p.close()


async def test_ticks_pullback_after_a_restart_never_counts_before_the_arming(tmp_path: Path,
                                                                           monkeypatch: pytest.MonkeyPatch) -> None:
    p, _friend = await _make(tmp_path, monkeypatch, edit=_no_confirm)
    engine2 = None
    try:
        await _pullback(p)
        _history(p, [(T0 + 0.5, 1.16195, 1.16205),                               # above the target (before arming)
                     (T0 + 1.0, 1.16100, 1.16110)])                              # between: arms the rule
        _tick(p, 1.16120, 1.16130, T0 + 1.5)
        await p.engine.monitor.run_virtual_once()
        assert (await p.positions(1))["tickmill"].data["virtual_tp_armed"] is True
        engine2 = Engine(p.cfg, p.db, p.parser, list(p.brokers.values()), FakeNotifier(),
                         price_oracle=p.engine.prices, reference_prices=p.ref)
        await engine2.start(monitor=False)
        _tick(p, 1.16125, 1.16135, T0 + 3.0)
        await engine2.monitor.run_virtual_once()                                 # (reads the ticks from T0 again)
        assert is_virtual_pending((await p.positions(1))["tickmill"])
    finally:
        if engine2 is not None:
            await engine2.stop(grace_sec=1.0)
        await p.close()


async def test_ticks_pullback_a_busy_look_arms_then_the_skipped_history_never_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    p, _friend = await _make(tmp_path, monkeypatch, edit=_no_confirm)
    try:
        await _pullback(p)
        _history(p, [(T0 + 0.5, 1.16195, 1.16205), (T0 + 1.0, 1.16100, 1.16110)])
        _tick(p, 1.16120, 1.16130, T0 + 1.5)
        fx = p.brokers["fx"]
        busy = [True]
        real = fx._oldest_call
        other = SimpleNamespace(timed_out=False, running_for=lambda: 0.0, stuck_after=lambda: 1e9,
                                relinking=lambda: False)
        fx._oldest_call = lambda: other if busy[0] else real()                # type: ignore[method-assign]
        await p.engine.monitor.run_virtual_once()                                # busy: only the price now (arms)
        assert (await p.positions(1))["tickmill"].data["virtual_tp_armed"] is True
        busy[0] = False
        _tick(p, 1.16125, 1.16135, T0 + 2.5)
        await p.engine.monitor.run_virtual_once()
        assert is_virtual_pending((await p.positions(1))["tickmill"])
    finally:
        await p.close()


async def test_pullback_reaching_the_target_after_the_arming_still_cancels(tmp_path: Path,
                                                                         monkeypatch: pytest.MonkeyPatch) -> None:
    p, _friend = await _make(tmp_path, monkeypatch, edit=_no_confirm)
    try:
        await _pullback(p)
        _history(p, [(T0 + 1.0, 1.16100, 1.16110)])                              # arms
        _tick(p, 1.16120, 1.16130, T0 + 1.5)
        await p.engine.monitor.run_virtual_once()
        _history(p, [(T0 + 1.0, 1.16100, 1.16110), (T0 + 2.0, 1.16155, 1.16165)])  # then at the target
        _tick(p, 1.16140, 1.16150, T0 + 2.5)
        await p.engine.monitor.run_virtual_once()
        assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED
    finally:
        await p.close()


# --------------------------------------------------------------------------- #
# A4, A5, A6
# --------------------------------------------------------------------------- #
async def test_fx_leg_filled_and_closed_before_the_look_cancels_even_with_another_active_account(env) -> None:
    p, _friend = env
    await _eur_limit(p)
    other = BrokerPosition(broker="fx2", broker_symbol="EURUSD", side=Side.LONG, status=PositionStatus.PENDING,
                           quantity=0.3, remaining_quantity=0.3, stop_loss=STOP, take_profits=[], dry_run=False,
                           data={})
    await p.db.save_broker_position(1, other)                      # (a third account's limit still resting)
    ticket = _fx_ticket(p)
    p.terminal.fill(ticket, ENTRY)
    p.terminal.hit(ticket, 4, STOP)                                # fx filled and stopped out at once
    p.terminal.set_price("EURUSD", 1.15995, 1.16005)
    p.tickmill.set_price("EURUSD", 1.16010, 1.16020)
    for _ in range(3):
        await p.engine.monitor.run_once()
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == []
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED
    assert any("pozisyon bu arada kapanmış" in t for t in p.texts())
    await _settle(p)
    _friend_clean(_friend.texts)                                   # (the friend's feed never names fx)


async def test_a_touch_seen_while_tickmill_is_away_is_decided_once_it_is_back(unsure) -> None:
    p, _friend = unsure
    _history(p, [(T0 + 0.5, 1.16090, 1.16098)])
    await _armed_watch(p)
    _tick(p, 1.16110, 1.16120, T0 + 1.5)                           # back within ~2 pips of the level
    tick_b = next(b for b in p.engine.brokers if b.name == "tickmill")
    p.engine.brokers = [b for b in p.engine.brokers if b.name != "tickmill"]
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == []
    p.engine.brokers.append(tick_b)
    p.tickmill.set_price("EURUSD", 1.16125, 1.16135)
    _tick(p, 1.16110, 1.16120, T0 + 2.0)
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[0] == "market_buy"


async def test_fx_order_resting_while_fx_is_not_connected_is_told_once(env) -> None:
    p, _friend = env
    await _eur_limit(p)                                            # fx's own limit rests: its fill decides
    p.engine.brokers = [b for b in p.engine.brokers if b.name != "fx"]
    for _ in range(3):
        await p.engine.monitor.run_virtual_once()
    told = [t for t in p.texts() if "fx bağlı değil" in t and "dolup dolmadığı görülemiyor" in t]
    assert len(told) == 1 and p.tickmill_orders() == []


# --------------------------------------------------------------------------- #
# A3 (decided): Evet after tickmill's own rule ended its entry, also without reference_broker (crypto)
# --------------------------------------------------------------------------- #
async def test_crypto_yes_after_tickmills_own_rule_ended_its_entry_opens_binance(tmp_path: Path,
                                                                               monkeypatch: pytest.MonkeyPatch) -> None:
    p, _friend = await _make(tmp_path, monkeypatch, reference=False, edit=_no_confirm)
    try:
        action = btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0).model_copy(update={"confidence": 0.5})
        await p.say(action, KRIPTO, "btc 1R long limit emir")
        (conf,) = p.notifier.confirmations
        before = len(p.binance.entry_orders())
        p.binance.set_price("BTC", 75010.0, 75012.0)                  # the first target before the entry
        await p.engine.monitor.run_virtual_once()
        assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED
        await p.engine.monitor.run_once()
        p.binance.set_price("BTC", 73999.0, 74001.0)
        out = await p.engine.handle_confirmation(conf.id, True)
        assert "onay beklerken" not in out
        assert len(p.binance.entry_orders()) == before + 1             # Evet opens binance, as before this feature
        assert p.tickmill_orders() == []
    finally:
        await p.close()


# --------------------------------------------------------------------------- #
# price_since through the real helper process (fake terminal)
# --------------------------------------------------------------------------- #
async def test_price_since_round_trip_through_the_bridge(term) -> None:  # noqa: F811
    b = await bridge(term)
    try:
        sym = await eth(b)
        first = await b.price_since(sym.broker_symbol, None)
        assert first["now"][1] > 0 and first["upto_msc"] is not None and first["busy"] is False
        again = await b.price_since(sym.broker_symbol, int(first["upto_msc"]) - 120_000,
                                    int(first["upto_msc"]) - 120_000)
        assert again["now"][1] > 0
    finally:
        await b.close()


# --------------------------------------------------------------------------- #
# follow-ups of the review round
# --------------------------------------------------------------------------- #
async def _watch_with_mid(p: Any) -> int:
    _tick(p, 1.16195, 1.16205, T0)
    action = fx_pair("EURUSD", Side.LONG, ENTRY, STOP, 1.1650, entry_type=EntryType.LIMIT).model_copy(
        update={"confidence": 0.5})
    mid = await p.say(action, FX, "EUR LONG 1R Limit Emir")
    await p.engine.monitor.run_virtual_once()
    return mid


async def test_a_target_move_never_counts_the_prices_before_it(unsure) -> None:
    """The trader moves the first target of the waiting entry: prices read again from before the move (here: the
    in-memory look position lost, as after a restart) neither arm the rule for the new target nor reach it."""
    p, _friend = unsure
    mid = await _watch_with_mid(p)
    _history(p, [(T0 + 0.5, 1.16410, 1.16420)])                    # (below the first target 1.1650: nothing)
    _tick(p, 1.16195, 1.16205, T0 + 1.5)
    await p.engine.monitor.run_virtual_once()
    move = ParsedAction(action=ActionType.MOVE_TP, take_profits=[TakeProfit(price=1.1640)], refers_to_message_id=mid,
                        confidence=0.95, summary_tr="TP 1.1640")
    await p.say(move, FX, "TP 1.1640", reply_to=mid)
    for conf in p.notifier.confirmations:                          # (asked when the move is doubtful: approved)
        if conf.kind == "manage":
            await p.engine.handle_confirmation(conf.id, True)
    pos = (await p.positions(1))["tickmill"]
    assert [tp.price for tp in pos.take_profits] == [1.1640] and pos.data["virtual_tp_armed"] is False
    assert pos.data["ref_tp_msc"] == int((T0 + 1.5) * 1000)
    p.engine._ref_seen.clear()                                     # (the window is read again from T0)
    _history(p, [(T0 + 0.5, 1.16410, 1.16420), (T0 + 2.0, 1.16190, 1.16200)])
    _tick(p, 1.16195, 1.16205, T0 + 2.5)
    await p.engine.monitor.run_virtual_once()
    pos = (await p.positions(1))["tickmill"]
    assert is_virtual_pending(pos)                                 # 1.1641 at T0 + 0.5 came before the move
    assert pos.data["virtual_tp_armed"] is True                    # between the entry and 1.1640 after it
    _history(p, [(T0 + 3.0, 1.16405, 1.16415)])                    # the new target reached after the move
    _tick(p, 1.16300, 1.16310, T0 + 3.5)
    await p.engine.monitor.run_virtual_once()
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED


async def test_the_first_look_starts_the_watch_even_when_tickmill_cannot_act(unsure) -> None:
    p, _friend = unsure
    _history(p, [])                                                # (fx's tick history: readable, no ticks)
    tick_b = next(b for b in p.engine.brokers if b.name == "tickmill")
    p.engine.brokers = [b for b in p.engine.brokers if b.name != "tickmill"]
    pos = (await p.positions(1))["tickmill"] if await p.trades() else None
    assert pos is None
    p.engine.brokers.append(tick_b)
    _tick(p, 1.16195, 1.16205, T0)
    action = fx_pair("EURUSD", Side.LONG, ENTRY, STOP, 1.1650, entry_type=EntryType.LIMIT).model_copy(
        update={"confidence": 0.5})
    await p.say(action, FX, "EUR LONG 1R Limit Emir")              # (tickmill connected for the signal)
    p.engine.brokers = [b for b in p.engine.brokers if b.name != "tickmill"]
    _tick(p, 1.16095, 1.16100, T0 + 1.0)                           # at the level on the first look, tickmill away
    await p.engine.monitor.run_virtual_once()
    pos = (await p.positions(1))["tickmill"]
    assert is_virtual_pending(pos) and pos.data.get("ref_armed_msc") == int((T0 + 1.0) * 1000)
    p.engine.brokers.append(tick_b)
    p.tickmill.set_price("EURUSD", 1.16105, 1.16115)
    _tick(p, 1.16095, 1.16100, T0 + 2.0)
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[0] == "market_buy"


# --------------------------------------------------------------------------- #
# round 2
# --------------------------------------------------------------------------- #
def _fx_busy(p: Any) -> list[bool]:
    """fx's worker thread has another call (an order, a sync) while the flag is set."""
    fx = p.brokers["fx"]
    flag = [True]
    real = fx._oldest_call
    other = SimpleNamespace(timed_out=False, running_for=lambda: 0.0, stuck_after=lambda: 1e9,
                            relinking=lambda: False)
    fx._oldest_call = lambda: other if flag[0] else real()                       # type: ignore[method-assign]
    return flag


async def test_a_busy_look_never_opens_its_skipped_history_decides_next(unsure) -> None:
    p, _friend = unsure
    _history(p, [(T0 + 0.3, 1.16090, 1.16098), (T0 + 0.5, 1.15880, 1.15890)])   # entry, then through the stop
    await _armed_watch(p)
    _tick(p, 1.15950, 1.15960, T0 + 1.5)                                          # between the stop and the entry
    p.tickmill.set_price("EURUSD", 1.15960, 1.15970)
    busy = _fx_busy(p)
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == [] and is_virtual_pending((await p.positions(1))["tickmill"])
    busy[0] = False
    await p.engine.monitor.run_virtual_once()                                     # the full read: the stop pass
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED and p.tickmill_orders() == []


async def test_a_busy_look_still_cancels_on_the_price_now(unsure) -> None:
    p, _friend = unsure
    await _armed_watch(p)
    _tick(p, 1.15880, 1.15890, T0 + 1.5)                                          # through the stop right now
    _fx_busy(p)
    await p.engine.monitor.run_virtual_once()
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED


async def test_a_first_look_touch_while_tickmill_is_away_is_decided_when_it_is_back(unsure) -> None:
    p, _friend = unsure
    _history(p, [])                                                # (fx's tick history: readable, no ticks)
    tick_b = next(b for b in p.engine.brokers if b.name == "tickmill")
    _tick(p, 1.16195, 1.16205, T0)
    action = fx_pair("EURUSD", Side.LONG, ENTRY, STOP, 1.1650, entry_type=EntryType.LIMIT).model_copy(
        update={"confidence": 0.5})
    await p.say(action, FX, "EUR LONG 1R Limit Emir")
    p.engine.brokers = [b for b in p.engine.brokers if b.name != "tickmill"]
    _tick(p, 1.16095, 1.16100, T0 + 1.0)                     # at the level on the first look, tickmill away
    await p.engine.monitor.run_virtual_once()
    assert (await p.positions(1))["tickmill"].data["ref_touch"] == pytest.approx(1.16100)
    p.engine.brokers.append(tick_b)
    p.tickmill.set_price("EURUSD", 1.16125, 1.16135)
    _tick(p, 1.16110, 1.16120, T0 + 2.0)                     # 2 pips above: still near the level
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[0] == "market_buy"


async def test_a_kept_touch_is_dropped_once_the_price_is_far(unsure) -> None:
    p, _friend = unsure
    _history(p, [])                                                # (fx's tick history: readable, no ticks)
    tick_b = next(b for b in p.engine.brokers if b.name == "tickmill")
    await _armed_watch(p)
    p.engine.brokers = [b for b in p.engine.brokers if b.name != "tickmill"]
    _tick(p, 1.16095, 1.16100, T0 + 1.0)
    await p.engine.monitor.run_virtual_once()
    p.engine.brokers.append(tick_b)
    _tick(p, 1.16245, 1.16255, T0 + 2.0)                     # ran away: never chased
    await p.engine.monitor.run_virtual_once()
    pos = (await p.positions(1))["tickmill"]
    assert p.tickmill_orders() == [] and "ref_touch" not in pos.data and pos.data.get("virtual_missed")


async def test_bars_a_new_high_after_the_arming_in_the_same_minute_reaches_the_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bars cannot be split: the arming minute's bar counts only with an extreme beyond the one it had when the rule
    armed (that part came after it). Its earlier high - from before the arming - never counts."""
    p, _friend = await _make(tmp_path, monkeypatch, edit=_no_confirm)
    try:
        await _pullback(p)
        minute = -(-T0 // 60) * 60
        bar = {"time": minute, "open": 1.16160, "high": 1.16170, "low": 1.16100, "close": 1.16120, "spread": 10}

        def copy_rates_range(symbol: str, timeframe: int, date_from: int, date_to: int) -> list[dict[str, Any]]:
            return [dict(b) for b in [bar] if date_from <= b["time"] <= date_to]

        p.terminal.copy_rates_range = copy_rates_range                           # type: ignore[method-assign]
        _tick(p, 1.16120, 1.16130, minute + 10)                                  # arms (between)
        await p.engine.monitor.run_virtual_once()
        pos = (await p.positions(1))["tickmill"]
        assert pos.data["virtual_tp_armed"] is True and pos.data["ref_tp_bar"]["hi"] == pytest.approx(1.16170)
        bar["close"] = 1.16130                                                   # the bar goes on, same high
        _tick(p, 1.16125, 1.16135, minute + 15)
        await p.engine.monitor.run_virtual_once()
        assert is_virtual_pending((await p.positions(1))["tickmill"])            # (its high is from before)
        bar["high"] = 1.16180                                                    # a new high after the arming
        _tick(p, 1.16120, 1.16130, minute + 20)
        await p.engine.monitor.run_virtual_once()
        assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED
    finally:
        await p.close()


async def test_unreadable_history_never_opens_and_is_read_again(unsure) -> None:
    """Neither fx's ticks nor its bars of the time since the previous look can be read: like a busy look, that time
    stays unread - the price now at the level does not open; the next readable look sees the stop pass in it."""
    p, _friend = unsure
    await _armed_watch(p)                                          # (no tick history in this terminal: bars)
    minute = -(-T0 // 60) * 60
    readable = [False]

    def copy_rates_range(symbol: str, timeframe: int, date_from: int, date_to: int) -> Any:
        if not readable[0]:
            return None
        bars = [{"time": minute, "open": 1.1619, "high": 1.1620, "low": 1.15880, "close": 1.1610, "spread": 10}]
        return [b for b in bars if date_from <= b["time"] <= date_to]

    p.terminal.copy_rates_range = copy_rates_range                  # type: ignore[method-assign]
    _tick(p, 1.16090, 1.16100, minute + 30)                         # at the level now
    p.tickmill.set_price("EURUSD", 1.16105, 1.16115)
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == [] and is_virtual_pending((await p.positions(1))["tickmill"])
    assert any("fiyat geçmişi" in t and "okunamadı" in t for t in p.texts())
    readable[0] = True
    await p.engine.monitor.run_virtual_once()                       # the minute's bar went through the stop 1.1590
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED and p.tickmill_orders() == []


async def test_ticks_unreadable_in_the_watchs_first_minute_never_open(unsure) -> None:
    """The ticks cannot be read and no bar may be read yet (the minute the watch started in): that time stays unread -
    the price now never opens; the next readable look sees the stop pass in it."""
    p, _friend = unsure
    await _armed_watch(p)                                          # watch starts at T0 (20 s into a minute)
    rows = [{"time": int(T0 + 0.5), "time_msc": int((T0 + 0.5) * 1000), "bid": 1.15880, "ask": 1.15890}]
    readable = [False]

    def copy_ticks_from(symbol: str, date_from: int, count: int, flags: int) -> Any:
        return [r for r in rows if r["time"] >= date_from][:count] if readable[0] else None

    p.terminal.copy_ticks_from = copy_ticks_from                    # type: ignore[attr-defined]
    _tick(p, 1.16090, 1.16100, T0 + 1.5)                            # at the level now (history unknown)
    p.tickmill.set_price("EURUSD", 1.16105, 1.16115)
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == [] and is_virtual_pending((await p.positions(1))["tickmill"])
    assert p.engine._ref_seen[(1, "tickmill")] == T0 * 1000        # (the time since T0 stays unread)
    readable[0] = True
    await p.engine.monitor.run_virtual_once()                       # the stop pass at T0 + 0.5
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED and p.tickmill_orders() == []


async def test_ticks_unreadable_past_the_first_minute_a_stop_pass_in_it_still_cancels(unsure) -> None:
    """The ticks stay unreadable into the next minute: the stretch of the watch's first minute can only be seen in
    that minute's bar, which also holds prices from before the watch. It is used for the stop only (a pass there
    cancels - never a touch: it never opens)."""
    p, _friend = unsure
    await _armed_watch(p)                                          # watch starts at T0 (20 s into a minute)
    minute = -(-T0 // 60) * 60

    def copy_ticks_from(symbol: str, date_from: int, count: int, flags: int) -> Any:
        return None

    stop_in_first = [True]

    def copy_rates_range(symbol: str, timeframe: int, date_from: int, date_to: int) -> list[dict[str, Any]]:
        first_low = 1.15880 if stop_in_first[0] else 1.16190
        bars = [{"time": minute - 60, "open": 1.1620, "high": 1.1621, "low": first_low, "close": 1.1619, "spread": 10},
                {"time": minute, "open": 1.1619, "high": 1.1620, "low": 1.16150, "close": 1.1616, "spread": 10}]
        return [b for b in bars if date_from <= b["time"] <= date_to]

    p.terminal.copy_ticks_from = copy_ticks_from                    # type: ignore[attr-defined]
    p.terminal.copy_rates_range = copy_rates_range                  # type: ignore[method-assign]
    _tick(p, 1.16090, 1.16100, minute + 5)                          # the next minute, at the level now
    p.tickmill.set_price("EURUSD", 1.16105, 1.16115)
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == []
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED


async def test_ticks_unreadable_past_the_first_minute_its_bar_never_opens_by_itself(unsure) -> None:
    p, _friend = unsure
    await _armed_watch(p)
    minute = -(-T0 // 60) * 60

    def copy_ticks_from(symbol: str, date_from: int, count: int, flags: int) -> Any:
        return None

    def copy_rates_range(symbol: str, timeframe: int, date_from: int, date_to: int) -> list[dict[str, Any]]:
        # the first minute's bar went down to the level (maybe before the watch), the next one stayed above it
        bars = [{"time": minute - 60, "open": 1.1620, "high": 1.1621, "low": 1.16090, "close": 1.1619, "spread": 10},
                {"time": minute, "open": 1.1619, "high": 1.1620, "low": 1.16180, "close": 1.1619, "spread": 10}]
        return [b for b in bars if date_from <= b["time"] <= date_to]

    p.terminal.copy_ticks_from = copy_ticks_from                    # type: ignore[attr-defined]
    p.terminal.copy_rates_range = copy_rates_range                  # type: ignore[method-assign]
    _tick(p, 1.16185, 1.16195, minute + 5)                          # above the level now (within ~10 pips: no)
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == [] and is_virtual_pending((await p.positions(1))["tickmill"])


async def test_a_touch_on_a_look_that_may_not_open_is_kept_and_decided_at_the_next_full_read(unsure) -> None:
    """Ticks unreadable in the watch's first minute: the look at the level may not open (its history is unread) but
    keeps the touch; the next full read (the first minute's bar for the stop, no pass) opens it - 2 pips above."""
    p, _friend = unsure
    await _armed_watch(p)                                          # watch starts at T0 (20 s into minute ma)
    ma = T0 // 60 * 60

    def copy_ticks_from(symbol: str, date_from: int, count: int, flags: int) -> Any:
        return None

    def copy_rates_range(symbol: str, timeframe: int, date_from: int, date_to: int) -> list[dict[str, Any]]:
        bars = [{"time": ma, "open": 1.16195, "high": 1.16200, "low": 1.16088, "close": 1.16110, "spread": 10},
                {"time": ma + 60, "open": 1.16110, "high": 1.16125, "low": 1.16105, "close": 1.16110, "spread": 10}]
        return [b for b in bars if date_from <= b["time"] <= date_to]

    p.terminal.copy_ticks_from = copy_ticks_from                    # type: ignore[attr-defined]
    p.terminal.copy_rates_range = copy_rates_range                  # type: ignore[method-assign]
    p.tickmill.set_price("EURUSD", 1.16105, 1.16115)
    _tick(p, 1.16088, 1.16098, T0 + 5)                              # at the level now, history unread
    await p.engine.monitor.run_virtual_once()
    pos = (await p.positions(1))["tickmill"]
    assert p.tickmill_orders() == [] and pos.data["ref_touch"] == pytest.approx(1.16098)
    _tick(p, 1.16110, 1.16120, ma + 61)                             # the next minute: 2 pips above (near)
    p.tickmill.set_price("EURUSD", 1.16125, 1.16135)
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[0] == "market_buy"


async def test_a_kept_touch_from_a_busy_look_still_yields_to_a_stop_pass_in_the_skipped_history(unsure) -> None:
    p, _friend = unsure
    _history(p, [(T0 + 0.5, 1.15880, 1.15890)])                     # through the stop since the last look
    await _armed_watch(p)
    _tick(p, 1.16090, 1.16100, T0 + 1.5)                            # at the level now
    p.tickmill.set_price("EURUSD", 1.16105, 1.16115)
    busy = _fx_busy(p)
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == [] and (await p.positions(1))["tickmill"].data.get("ref_touch") is not None
    busy[0] = False
    await p.engine.monitor.run_virtual_once()                       # the full read: the stop pass wins
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED and p.tickmill_orders() == []


async def test_bars_a_short_arming_bar_is_judged_by_its_bid_low_not_its_moving_spread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A short pullback (first target above today's price): the arming minute's bar counts only with a bid low below
    the one it had when the rule armed - a wider spread later in the bar (its ask low unchanged or lower) is no new
    price."""
    p, _friend = await _make(tmp_path, monkeypatch, edit=_no_confirm)
    try:
        tps.set_eurusd(p, 1.16195, 1.16205)
        action = fx_pair("EURUSD", Side.SHORT, 1.1640, 1.1660, 1.1625, entry_type=EntryType.LIMIT).model_copy(
            update={"confidence": 0.5})
        _tick(p, 1.16195, 1.16205, T0)
        await p.say(action, FX, "EUR SHORT 1R Limit Emir")
        assert (await p.positions(1))["tickmill"].data["virtual_tp_armed"] is False
        await p.engine.monitor.run_virtual_once()
        minute = -(-T0 // 60) * 60
        bar = {"time": minute, "open": 1.16250, "high": 1.16300, "low": 1.16240, "close": 1.16280, "spread": 10}

        def copy_rates_range(symbol: str, timeframe: int, date_from: int, date_to: int) -> list[dict[str, Any]]:
            return [dict(b) for b in [bar] if date_from <= b["time"] <= date_to]

        p.terminal.copy_rates_range = copy_rates_range                           # type: ignore[method-assign]
        _tick(p, 1.16280, 1.16290, minute + 10)                                  # between: arms
        await p.engine.monitor.run_virtual_once()
        pos = (await p.positions(1))["tickmill"]
        assert pos.data["virtual_tp_armed"] is True and pos.data["ref_tp_bar"]["lo"] == pytest.approx(1.16240)
        bar["spread"] = 2                                          # the spread narrows: the ask low falls, no new bid
        _tick(p, 1.16280, 1.16290, minute + 15)
        await p.engine.monitor.run_virtual_once()
        assert is_virtual_pending((await p.positions(1))["tickmill"])
        bar["low"] = 1.16230                                       # a new low after the arming: ask <= 1.1625
        _tick(p, 1.16270, 1.16280, minute + 20)
        await p.engine.monitor.run_virtual_once()
        assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED
    finally:
        await p.close()


def test_the_live_config_after_this_update_has_the_friends_settings(tmp_path: Path,
                                                                    monkeypatch: pytest.MonkeyPatch) -> None:
    """The server's config (LIVE_YAML) migrated by this version: it loads, the tickmill block carries the four
    settings (and the earlier 45 -> 50), the owner's accounts are untouched; a second run changes nothing."""
    from copytrader import migrate
    from copytrader.config import load_config

    tps.load_prod_config(tmp_path, monkeypatch, "live")             # (writes config.yaml, sets the variables)
    path = tmp_path / "config.yaml"
    lines = migrate.migrate_config(path, terminal=(None, None))
    assert sum(1 for line in lines if line.startswith("tickmill:")) == 5
    cfg = load_config(path)
    t = next(b for b in cfg.brokers if b.name == "tickmill")
    assert (t.confirm_open, t.fees_in_risk, t.lot_rounding, t.lot_rounding_max_over, t.reference_broker,
            t.risk_usd) == (False, False, "up", 10.0, "fx", 50)
    for name in ("binance", "fx"):
        b = next(x for x in cfg.brokers if x.name == name)
        assert (b.confirm_open, b.fees_in_risk, b.lot_rounding, b.reference_broker) == (True, None, "down", None)
    assert migrate.migrate_config(path, terminal=(None, None)) == []


async def test_an_ended_entry_forgets_its_fx_down_warning(env) -> None:
    p, _friend = env
    await _eur_limit(p)
    p.engine.brokers = [b for b in p.engine.brokers if b.name != "fx"]
    await p.engine.monitor.run_virtual_once()
    assert (1, "tickmill:refdown") in p.engine._virtual_warned
    p.engine._virtual_forget(1, "tickmill")
    assert (1, "tickmill:refdown") not in p.engine._virtual_warned
