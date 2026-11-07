"""Regression tests: MOVE_TP keeps the trader's split and never re-arms a filled level (M4, F9, F10)."""

from __future__ import annotations

from datetime import datetime, timezone

from copytrader.models import (
    ActionType,
    BrokerPosition,
    EntryType,
    ParsedAction,
    Side,
    TakeProfit,
    TradePlan,
    TradeRecord,
    TradeStatus,
)
from copytrader.parsing import validator

from test_engine import CHAT, SENDER, Env, action, add_msg, db, env, eth_open, result  # noqa: F401  (fixtures)

T = TakeProfit
NOW = datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)


def trade_with(tps: list[TakeProfit], side: Side = Side.LONG) -> TradeRecord:
    plan = TradePlan(plan_id="p", base_asset="ETH", side=side, entry_type=EntryType.MARKET, entry_price=2232.18,
                     stop_loss=2205.11 if side is Side.LONG else 2260.0, take_profits=tps, risk_r=1, risk_pct=2,
                     source_chat_id=1, created_at=NOW)
    return TradeRecord(id=7, plan=plan, status=TradeStatus.OPEN, current_stop_loss=plan.stop_loss,
                       current_take_profits=tps, created_at=NOW, updated_at=NOW)


def move_tp(trade: TradeRecord, tps: list[TakeProfit], *, market: float = 2240.0,
            filled: list[float] | None = None):  # type: ignore[no-untyped-def]
    act = ParsedAction(action=ActionType.MOVE_TP, base_asset="ETH", take_profits=tps, confidence=0.95)
    plan, issues = validator.build_management_plan(act, trade=trade, candidates=[trade], market_price=market,
                                                   source_chat_id=1, now=NOW, filled_tp_prices=filled)
    levels = [(tp.price, round(tp.close_fraction, 6)) for tp in plan.take_profits] if plan else None
    return levels, [i.code for i in issues]


# --------------------------------------------------------------------------- #
# validator
# --------------------------------------------------------------------------- #
def test_move_tp_keeps_the_existing_split_for_unchanged_and_replaced_levels() -> None:
    t = trade_with([T(price=2300, close_fraction=0.7), T(price=2350, close_fraction=0.3)])
    assert move_tp(t, [T(price=2300), T(price=2400)]) == ([(2300, 0.7), (2400, 0.3)], [])


def test_move_tp_keeps_an_explicit_new_split() -> None:
    t = trade_with([T(price=2300, close_fraction=0.7), T(price=2350, close_fraction=0.3)])
    assert move_tp(t, [T(price=2300, close_fraction=0.5), T(price=2400, close_fraction=0.5)]) == (
        [(2300, 0.5), (2400, 0.5)], [])


def test_move_tp_asks_when_the_level_count_changes_and_the_split_would_be_lost() -> None:
    t = trade_with([T(price=2300, close_fraction=0.7), T(price=2350, close_fraction=0.3)])
    levels, codes = move_tp(t, [T(price=2300), T(price=2350), T(price=2400)])
    assert len(levels) == 3 and "tp_split_changed" in codes
    t_equal = trade_with([T(price=2300, close_fraction=0.5), T(price=2350, close_fraction=0.5)])
    assert move_tp(t_equal, [T(price=2300), T(price=2350), T(price=2400)])[1] == []
    assert move_tp(t, [T(price=2400)]) == ([(2400, 1.0)], [])      # a single target has no split to lose


def test_move_tp_short_side_pairs_nearest_first() -> None:
    t = trade_with([T(price=2200, close_fraction=0.7), T(price=2150, close_fraction=0.3)], Side.SHORT)
    assert move_tp(t, [T(price=2200), T(price=2100)], market=2230) == ([(2200, 0.7), (2100, 0.3)], [])


def test_move_tp_never_rearms_a_filled_level_and_ignores_its_side() -> None:
    t = trade_with([T(price=2260, close_fraction=0.5), T(price=2310.6, close_fraction=0.5)])
    # model copied the filled TP1 (or left it out): TP1 stays "done" with its share, TP2 gets the rest
    assert move_tp(t, [T(price=2260), T(price=2400)], filled=[2260]) == ([(2260, 0.5), (2400, 0.5)], [])
    assert move_tp(t, [T(price=2400)], filled=[2260]) == ([(2260, 0.5), (2400, 0.5)], [])
    # price still above the filled TP1: the legitimate move is not blocked
    assert move_tp(t, [T(price=2260), T(price=2400)], market=2280, filled=[2260]) == ([(2260, 0.5), (2400, 0.5)], [])
    assert move_tp(t, [T(price=2260), T(price=2400)], market=2280)[1] == ["tp_wrong_side"]   # unknown fill: blocked
    assert move_tp(t, [T(price=2260)], filled=[2260]) == (None, ["tp_all_filled"])


# --------------------------------------------------------------------------- #
# engine: context + execution
# --------------------------------------------------------------------------- #
async def open_split(env: Env) -> TradeRecord:
    env.parser.push(result(eth_open(take_profits=[T(price=2300.0, close_fraction=0.7),
                                                  T(price=2350.0, close_fraction=0.3)])))
    await env.signal()
    return (await env.db.active_trades())[0]


async def test_open_trades_context_carries_fractions_hit_state_and_open_share(env: Env) -> None:
    trade = await open_split(env)
    pos = next(p for p in await env.db.broker_positions(trade.id) if p.broker == "binance")
    await env.db.save_broker_position(trade.id, pos.model_copy(update={
        "remaining_quantity": pos.filled_quantity * 0.3,
        "data": {**pos.data, "tp_order_ids": [{"price": 2300.0, "status": "filled"},
                                              {"price": 2350.0, "status": "open"}]},
    }))
    env.parser.push(result(action(ActionType.NONE, used_message_ids=[600], confidence=0.99)))
    await add_msg(env.db, 600, "eth güzel gidiyor, TP2 2350 bekliyoruz")
    await env.engine.on_messages_ready(CHAT, SENDER)
    ctx = env.parser.calls[-1][1]
    summary = ctx.open_trades[0]
    assert summary["take_profits"] == [{"price": 2300.0, "close_fraction": 0.7, "hit": True},
                                       {"price": 2350.0, "close_fraction": 0.3, "hit": False}]
    assert summary["open_fraction"] == 1.0          # the other broker still holds its whole size
    assert "✔" in await env.engine.positions_text()


async def test_move_tp_keeps_the_traders_split_end_to_end(env: Env) -> None:
    trade = await open_split(env)
    env.parser.push(result(action(ActionType.MOVE_TP, base_asset="ETH", target_trade_id=trade.id,
                                  take_profits=[T(price=2300.0), T(price=2400.0)], used_message_ids=[600])))
    await add_msg(env.db, 600, "eth TP2'yi 2400 yaptım")
    await env.engine.on_messages_ready(CHAT, SENDER)
    for b in env.brokers:
        sent = b.calls_of("update_take_profits")[0][1]
        assert [(tp.price, round(tp.close_fraction, 6)) for tp in sent] == [(2300.0, 0.7), (2400.0, 0.3)]
    stored = await env.db.get_trade(trade.id)
    assert [(tp.price, round(tp.close_fraction, 6)) for tp in stored.current_take_profits] == [  # type: ignore[union-attr]
        (2300.0, 0.7), (2400.0, 0.3)]


async def test_move_tp_after_tp1_filled_does_not_rearm_tp1(env: Env) -> None:
    env.parser.push(result(eth_open(take_profits=[T(price=2260.0, close_fraction=0.5),
                                                  T(price=2310.6, close_fraction=0.5)])))
    await env.signal()
    trade = (await env.db.active_trades())[0]
    # TP1 filled on binance only (bybit's TP1 is still resting)
    pos = next(p for p in await env.db.broker_positions(trade.id) if p.broker == "binance")
    await env.db.save_broker_position(trade.id, pos.model_copy(update={
        "remaining_quantity": pos.filled_quantity / 2,
        "data": {**pos.data, "tp_order_ids": [{"price": 2260.0, "status": "filled"},
                                              {"price": 2310.6, "status": "open"}]},
    }))
    env.oracle.prices["ETH"] = 2240.0
    env.parser.push(result(action(ActionType.MOVE_TP, base_asset="ETH", target_trade_id=trade.id,
                                  take_profits=[T(price=2260.0), T(price=2400.0)], used_message_ids=[600])))
    await add_msg(env.db, 600, "eth TP2'yi 2400 yaptım")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.notifier.confirmations == []
    binance = env.a.calls_of("update_take_profits")[0][1]
    bybit = env.b.calls_of("update_take_profits")[0][1]
    assert [tp.price for tp in binance] == [2400.0]                     # TP1 is not placed again
    assert [(tp.price, round(tp.close_fraction, 6)) for tp in bybit] == [(2260.0, 0.5), (2400.0, 0.5)]


async def test_move_tp_when_every_level_already_filled_on_a_broker_is_skipped_there(env: Env) -> None:
    env.parser.push(result(eth_open(take_profits=[T(price=2260.0, close_fraction=0.5),
                                                  T(price=2310.6, close_fraction=0.5)])))
    await env.signal()
    trade = (await env.db.active_trades())[0]
    pos: BrokerPosition = next(p for p in await env.db.broker_positions(trade.id) if p.broker == "binance")
    await env.db.save_broker_position(trade.id, pos.model_copy(update={
        "data": {**pos.data, "tps": [{"price": 2260.0, "status": "filled"}, {"price": 2310.6, "status": "filled"}]},
    }))
    from copytrader.models import ManagementPlan

    plan = ManagementPlan(plan_id="m", action=ActionType.MOVE_TP, trade_id=trade.id, source_chat_id=CHAT,
                          take_profits=[T(price=2260.0, close_fraction=0.5), T(price=2310.6, close_fraction=0.5)],
                          created_at=NOW)
    text = await env.engine.execute_management(plan)
    assert env.a.calls_of("update_take_profits") == [] and "zaten gerçekleşmiş" in text
