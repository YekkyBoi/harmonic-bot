"""Closure reports in the engine: the trader's "2R kapandı" closes the copy without a question, a
reply chain is followed up to the signal, a reply about a signal that was never copied never closes
another trade silently, and a report about an already closed copy is ignored."""

from __future__ import annotations

from copytrader.models import ActionType, Side, TradeStatus

from test_topics_engine import CHAT, CRYPTO, Env, _env, db, manage, result  # noqa: F401  (db fixture)


async def _open_eth(db) -> tuple[Env, int]:
    env: Env = await _env(db)
    trade_id = await env.crypto_signal()          # ETH short from msgs 610/611, open on binance
    env.notifier.notifications.clear()
    return env, trade_id


def _closes(env: Env) -> int:
    return len(env.binance.calls_of("close_position"))


async def test_non_reply_closure_report_with_target_closes_without_asking(db) -> None:
    env, trade_id = await _open_eth(db)
    try:
        env.parser.push(result(manage(ActionType.CLOSE, close_fraction=1.0, target_trade_id=trade_id,
                                      base_asset="ETH", side=Side.SHORT, confidence=0.92,
                                      used_message_ids=[660])))
        await env.msg(660, "2R kapandı", topic=CRYPTO, photo=True)       # not a reply
        await env.ready(CRYPTO)
        assert env.notifier.confirmations == []
        assert _closes(env) == 1 and env.binance.calls_of("close_position")[0][1] == 1.0
        trade = await env.db.get_trade(trade_id)
        assert trade is not None and trade.status is TradeStatus.CLOSED
    finally:
        await env.engine.stop()


async def test_reply_to_an_earlier_update_is_followed_to_the_signal(db) -> None:
    env, trade_id = await _open_eth(db)
    try:
        await env.msg(620, "Aktif", topic=CRYPTO, reply_to=611)          # an update replying to the signal
        await env.db.mark_consumed(CHAT, [620])
        env.parser.push(result(manage(ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=620,
                                      confidence=0.95, used_message_ids=[630])))
        await env.msg(630, "2R kapandı", topic=CRYPTO, reply_to=620)     # reply to the update, no asset
        await env.ready(CRYPTO)
        assert env.notifier.confirmations == []
        assert _closes(env) == 1
        trade = await env.db.get_trade(trade_id)
        assert trade is not None and trade.status is TradeStatus.CLOSED
    finally:
        await env.engine.stop()


async def test_reply_about_an_uncopied_signal_never_closes_another_trade_silently(db) -> None:
    env, trade_id = await _open_eth(db)
    try:
        await env.msg(650, "Eth short 1R (başka işlem)", topic=CRYPTO, photo=True)   # never copied
        await env.db.mark_consumed(CHAT, [650])
        env.parser.push(result(manage(ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=650,
                                      base_asset="ETH", side=Side.SHORT, confidence=0.9,
                                      used_message_ids=[651])))
        await env.msg(651, "2R kapandı", topic=CRYPTO, reply_to=650)
        await env.ready(CRYPTO)
        assert _closes(env) == 0
        trade = await env.db.get_trade(trade_id)
        assert trade is not None and trade.status is TradeStatus.OPEN
        [conf] = env.notifier.confirmations
        assert "reply_unlinked" in [i.code for i in conf.issues]
    finally:
        await env.engine.stop()


async def test_report_about_an_already_closed_copy_is_ignored(db) -> None:
    env, trade_id = await _open_eth(db)
    try:
        env.parser.push(result(manage(ActionType.CLOSE, close_fraction=1.0, target_trade_id=trade_id,
                                      base_asset="ETH", side=Side.SHORT, confidence=0.92,
                                      used_message_ids=[660])))
        await env.msg(660, "2R kapandı", topic=CRYPTO, photo=True)
        await env.ready(CRYPTO)
        assert _closes(env) == 1
        # the same news again, now with no open ETH copy: the model may still answer "close"
        env.parser.push(result(manage(ActionType.CLOSE, close_fraction=1.0, base_asset="ETH", side=Side.SHORT,
                                      confidence=0.92, used_message_ids=[661])))
        await env.msg(661, "ETH kapandı", topic=CRYPTO)
        await env.ready(CRYPTO)
        assert env.notifier.confirmations == [] and _closes(env) == 1
    finally:
        await env.engine.stop()
