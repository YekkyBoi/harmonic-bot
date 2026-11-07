"""Regression tests (N1): MOVE_TP close fractions are shares of the ORIGINAL size (as in OPEN TRADES). When the
model copies the trade's fractions (hit levels left out, so they do not add up to 1), the trader's split is kept
by renormalizing over the open levels; only a genuinely inconsistent split is asked."""

from __future__ import annotations

from copytrader.models import ActionType, Side, TakeProfit
from copytrader.parsing import prompts

from test_engine import CHAT, SENDER, Env, action, add_msg, db, env, eth_open, result  # noqa: F401  (fixtures)
from test_fix_engine_tp_context import move_tp, trade_with

T = TakeProfit


def tps(*levels: tuple[float, float]) -> list[TakeProfit]:
    return [T(price=p, close_fraction=f) for p, f in levels]


# --------------------------------------------------------------------------- #
# validator: the three r5b cases
# --------------------------------------------------------------------------- #
def test_copied_fraction_plus_default_on_the_moved_level_keeps_70_30() -> None:
    t = trade_with(tps((2300, 0.7), (2350, 0.3)))
    assert move_tp(t, tps((2300, 0.7), (2400, 1.0))) == ([(2300, 0.7), (2400, 0.3)], [])


def test_original_shares_with_the_hit_level_left_out_keep_50_30_20() -> None:
    t = trade_with(tps((2260, 0.5), (2300, 0.3), (2350, 0.2)))
    assert move_tp(t, tps((2300, 0.3), (2400, 0.2)), filled=[2260]) == (
        [(2260, 0.5), (2300, 0.3), (2400, 0.2)], [])


def test_single_open_level_with_its_original_share_after_a_hit() -> None:
    t = trade_with(tps((2260, 0.5), (2310.6, 0.5)))
    assert move_tp(t, tps((2400, 0.5)), filled=[2260]) == ([(2260, 0.5), (2400, 0.5)], [])


# --------------------------------------------------------------------------- #
# validator: other consistent readings are accepted, inconsistent ones asked
# --------------------------------------------------------------------------- #
def test_shares_of_the_remaining_position_are_accepted_too() -> None:
    t = trade_with(tps((2260, 0.5), (2300, 0.3), (2350, 0.2)))
    assert move_tp(t, tps((2300, 0.6), (2400, 0.4)), filled=[2260]) == (
        [(2260, 0.5), (2300, 0.3), (2400, 0.2)], [])


def test_hit_level_copied_with_its_fraction_is_accepted() -> None:
    t = trade_with(tps((2260, 0.5), (2300, 0.3), (2350, 0.2)))
    assert move_tp(t, tps((2260, 0.5), (2300, 0.3), (2400, 0.2)), filled=[2260]) == (
        [(2260, 0.5), (2300, 0.3), (2400, 0.2)], [])


def test_copied_fraction_plus_default_after_a_hit() -> None:
    t = trade_with(tps((2260, 0.5), (2300, 0.3), (2350, 0.2)))
    assert move_tp(t, tps((2300, 0.3), (2400, 1.0)), filled=[2260]) == (
        [(2260, 0.5), (2300, 0.3), (2400, 0.2)], [])


def test_short_side_original_shares() -> None:
    t = trade_with(tps((2200, 0.5), (2150, 0.3), (2100, 0.2)), Side.SHORT)
    assert move_tp(t, tps((2150, 0.3), (2050, 0.2)), market=2210, filled=[2200]) == (
        [(2200, 0.5), (2150, 0.3), (2050, 0.2)], [])


def test_inconsistent_sum_is_asked_with_a_proportional_proposal() -> None:
    t = trade_with(tps((2260, 0.5), (2300, 0.3), (2350, 0.2)))
    levels, codes = move_tp(t, tps((2260, 0.5), (2300, 0.3)))          # TP3 dropped, no hit, sums to 0.8
    assert codes == ["tp_split_invalid"]
    assert levels == [(2260, 0.625), (2300, 0.375)]


def test_level_added_without_a_split_is_asked() -> None:
    t = trade_with(tps((2300, 0.7), (2350, 0.3)))
    levels, codes = move_tp(t, tps((2300, 0.7), (2350, 0.3), (2400, 1.0)))   # nothing left for the new one
    assert codes == ["tp_split_changed"] and len(levels) == 3


def test_stated_more_than_open_with_a_default_level_is_asked() -> None:
    t = trade_with(tps((2260, 0.5), (2300, 0.3), (2350, 0.2)))
    levels, codes = move_tp(t, tps((2300, 0.6), (2400, 1.0)), filled=[2260])   # 0.6 > the 0.5 still open
    assert codes == ["tp_split_invalid"]
    assert levels == [(2260, 0.5), (2300, 0.3), (2400, 0.2)]           # the trade's own split is proposed


def test_out_of_range_fraction_is_asked() -> None:
    t = trade_with(tps((2300, 0.7), (2350, 0.3)))
    levels, codes = move_tp(t, tps((2300, 1.5), (2400, 1.0)))
    assert codes == ["tp_split_invalid"] and levels == [(2300, 0.7), (2400, 0.3)]


def test_existing_behaviour_kept() -> None:
    t = trade_with(tps((2300, 0.7), (2350, 0.3)))
    assert move_tp(t, tps((2300, 1.0), (2400, 1.0))) == ([(2300, 0.7), (2400, 0.3)], [])
    assert move_tp(t, tps((2300, 0.5), (2400, 0.5))) == ([(2300, 0.5), (2400, 0.5)], [])
    assert move_tp(t, tps((2400, 1.0))) == ([(2400, 1.0)], [])


# --------------------------------------------------------------------------- #
# end to end: the model copies what OPEN TRADES shows
# --------------------------------------------------------------------------- #
async def open_with(env: Env, levels: list[tuple[float, float]], hit: list[float]) -> int:
    env.oracle.prices["ETH"] = 2240.0
    env.parser.push(result(eth_open(take_profits=tps(*levels))))
    await env.signal()
    trade = (await env.db.active_trades())[0]
    if hit:
        for pos in await env.db.broker_positions(trade.id):
            done = sum(f for p, f in levels if p in hit)
            await env.db.save_broker_position(trade.id, pos.model_copy(update={
                "remaining_quantity": pos.filled_quantity * (1 - done),
                "data": {**pos.data, "tp_order_ids": [{"price": p, "status": "filled" if p in hit else "open"}
                                                      for p, _ in levels]},
            }))
    return trade.id


async def apply_move(env: Env, trade_id: int, new: list[tuple[float, float]]) -> None:
    env.parser.push(result(action(ActionType.MOVE_TP, base_asset="ETH", side=Side.LONG, target_trade_id=trade_id,
                                  take_profits=tps(*new), used_message_ids=[600])))
    await add_msg(env.db, 600, "eth TP güncelledim")
    await env.engine.on_messages_ready(CHAT, SENDER)


def sent(env: Env) -> list[list[tuple[float, float]]]:
    return [[(tp.price, round(tp.close_fraction, 6)) for tp in b.calls_of("update_take_profits")[-1][1]]
            for b in env.brokers]


async def test_e2e_copied_split_with_default_on_the_moved_level(env: Env) -> None:
    tid = await open_with(env, [(2300.0, 0.7), (2350.0, 0.3)], [])
    await apply_move(env, tid, [(2300.0, 0.7), (2400.0, 1.0)])
    assert env.notifier.confirmations == []
    assert sent(env) == [[(2300.0, 0.7), (2400.0, 0.3)]] * 2


async def test_e2e_three_levels_tp1_hit_left_out(env: Env) -> None:
    tid = await open_with(env, [(2260.0, 0.5), (2300.0, 0.3), (2350.0, 0.2)], [2260.0])
    ctx_tps = (await env.engine._open_trade_summaries())[0]["take_profits"]
    assert ctx_tps[0] == {"price": 2260.0, "close_fraction": 0.5, "hit": True}
    await apply_move(env, tid, [(2300.0, 0.3), (2400.0, 0.2)])
    assert env.notifier.confirmations == []
    assert sent(env) == [[(2300.0, 0.3), (2400.0, 0.2)]] * 2          # TP1 is never re-placed
    stored = await env.db.get_trade(tid)
    assert [(tp.price, round(tp.close_fraction, 6)) for tp in stored.current_take_profits] == [  # type: ignore[union-attr]
        (2260.0, 0.5), (2300.0, 0.3), (2400.0, 0.2)]


async def test_e2e_two_levels_tp1_hit_single_level_returned(env: Env) -> None:
    tid = await open_with(env, [(2260.0, 0.5), (2310.6, 0.5)], [2260.0])
    await apply_move(env, tid, [(2400.0, 0.5)])
    assert env.notifier.confirmations == []
    assert sent(env) == [[(2400.0, 0.5)]] * 2


# --------------------------------------------------------------------------- #
# prompt: the MOVE_TP rule states the scale
# --------------------------------------------------------------------------- #
def test_prompt_move_tp_rule_states_original_size_scale() -> None:
    sp = prompts.SYSTEM_PROMPT
    rule = next(line for line in sp.splitlines() if line.startswith('- "move_tp"'))
    assert "share of the ORIGINAL size" in rule and "leave out levels with hit: true" in rule
    assert "add up to less than 1.0" in rule and "[2310 (0.7), 2400 (0.3)]" in rule
    open_trades = next(line for line in sp.splitlines() if "OPEN TRADES: trades this system" in line)
    assert "close_fraction" in open_trades and "hit" in open_trades and "open_fraction" in open_trades
