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
from copytrader.models import BrokerPosition, EntryType, PositionStatus, Side
from fakes import FakeNotifier
from test_mt5_bridge import bridge, eth, term  # noqa: F401  (term: fixture)
from test_preflight_scenarios import FX, KRIPTO, _isolate, btc, fx_pair  # noqa: F401  (_isolate: autouse)
from test_reference_broker import (  # noqa: F401  (env, unsure: fixtures)
    ENTRY, STOP, T0, _armed_watch, _eur_limit, _fx_ticket, _history, _make, _no_confirm, _tick, env, unsure,
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
