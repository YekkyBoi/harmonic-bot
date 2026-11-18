"""Review round of confirm_open: false (the account that takes an unsure signal before the owner's answer).

    B1  a correction / edit that changed the trade idea (side, asset) opens the others as a trade of their own,
        the early account left out (one line says so); the owner's corrected levels win over the trader's moves,
        the levels he did not touch take the trade's current ones
    B2  a stop the trader moved (towards the entry) never sizes the others' positions: the signal's own stop does
    B3  the early trade closed by the trader while the approval was checked: the others are not opened
    B4  the question says what Evet does: an early account whose attempt sent nothing is tried again
    B5  the "işlem yapılmadı" texts say that the early trade stays
    B6  a "Hayır" while the early account's order is unanswered never calls it open
    B7  an error in the early open keeps the question's link to the trade: never a second trade of the message
    B8  lot_rounding "up" that lifts a size to the smallest order says so on the exchange path too
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

from copytrader.brokers.base import BrokerError
from copytrader.models import ActionType, ParsedAction, Quote, Side, StoredMessage, TakeProfit, TradeStatus
from copytrader.risk.sizing import compute_linear_size
from copytrader.storage.db import Database

from test_confirm_open_flag import CHAT, CRYPTO, KADIR, TITLES, eth_short, make_env, now, result


@pytest.fixture
async def db():
    database = Database(":memory:")
    await database.open()
    yield database
    await database.close()


async def _trades(db: Database) -> list[Any]:
    return sorted(await db.recent_trades(now() - timedelta(hours=1)), key=lambda t: t.id)


# --------------------------------------------------------------------------- #
# B1
# --------------------------------------------------------------------------- #
async def test_a_correction_to_the_other_side_opens_the_others_apart_without_tickmill(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()                                       # ETH SHORT read unsure: tickmill SHORT
        (conf,) = env.notifier.confirmations
        await env.engine.request_correction(conf.id)
        env.parser.push(result(eth_short(side=Side.LONG, stop_loss=2228.0, take_profits=[], summary_tr="ETH LONG",
                                         confidence=0.95)))
        await env.engine.handle_correction(conf.id, "aslında LONG, stop 2228")
        new = env.notifier.confirmations[-1]
        out = await env.engine.handle_confirmation(new.id, True)
        first, second = await _trades(db)
        assert first.plan.side is Side.SHORT and second.plan.side is Side.LONG
        assert {p.broker for p in await db.broker_positions(first.id)} == {"tickmill"}
        assert {p.broker for p in await db.broker_positions(second.id)} == {"binance"}
        assert [(r.plan.side, r.stop_loss) for r in env.binance.opened] == [(Side.LONG, 2228.0)]
        assert len(env.tm.opened) == 1
        assert "ilk okumayla (ETH SHORT) açılmıştı" in out and "tickmill hesabına gönderilmedi" in out
    finally:
        await env.engine.stop()


async def test_a_correction_to_another_asset_opens_the_others_apart(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        await env.engine.request_correction(conf.id)
        env.binance.quotes["BTC"] = Quote(bid=60000.0, ask=60001.0, last=60000.5)
        env.parser.push(result(eth_short(symbol="BTCUSDT.P", base_asset="BTC", entry_price=60000.0,
                                         stop_loss=60500.0, take_profits=[], summary_tr="BTC SHORT", confidence=0.95)))
        await env.engine.handle_correction(conf.id, "BTC aslında")
        await env.engine.handle_confirmation(env.notifier.confirmations[-1].id, True)
        first, second = await _trades(db)
        assert (first.plan.base_asset, second.plan.base_asset) == ("ETH", "BTC")
        assert [r.plan.base_asset for r in env.binance.opened] == ["BTC"] and len(env.tm.opened) == 1
    finally:
        await env.engine.stop()


async def test_an_edit_to_the_other_side_opens_the_others_apart(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        long = eth_short(side=Side.LONG, stop_loss=2228.0, take_profits=[TakeProfit(price=2260.0)],
                         summary_tr="ETH LONG", confidence=0.95)
        env.parser.push(result(long))                                   # (read for the early trade's edit)
        env.parser.push(result(long))                                   # (read for the question's edit)
        msg = StoredMessage(chat_id=CHAT, message_id=611, sender_id=KADIR, sender_name="Kadir", date=now(),
                            text="Eth LONG 1R", reply_to_message_id=CRYPTO, topic_id=CRYPTO,
                            topic_title=TITLES[CRYPTO])
        await db.save_message(msg)
        await env.engine.on_message_edited(msg)
        await env.engine.handle_confirmation(env.notifier.confirmations[-1].id, True)
        first, second = await _trades(db)
        assert (first.plan.side, second.plan.side) == (Side.SHORT, Side.LONG)
        assert {p.broker for p in await db.broker_positions(second.id)} == {"binance"}
    finally:
        await env.engine.stop()


async def test_the_owners_corrected_stop_wins_the_traders_moved_target_stays(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()                                       # SL 2250.6, TP 2205.11
        (conf,) = env.notifier.confirmations
        env.parser.push(result(ParsedAction(action=ActionType.MOVE_TP, take_profits=[TakeProfit(price=2210.0)],
                                            refers_to_message_id=611, used_message_ids=[612], confidence=0.95,
                                            summary_tr="tp 2210")))
        await env.msg(612, "tp 2210", topic=CRYPTO, reply_to=611)
        await env.engine.on_messages_ready(CHAT, KADIR, CRYPTO)
        await env.engine.request_correction(conf.id)
        env.parser.push(result(eth_short(stop_loss=2262.0, confidence=0.95, summary_tr="ETH SHORT SL 2262")))
        await env.engine.handle_correction(conf.id, "stop 2262")
        await env.engine.handle_confirmation(env.notifier.confirmations[-1].id, True)
        (req,) = env.binance.opened
        assert req.stop_loss == 2262.0 and [tp.price for tp in req.take_profits] == [2210.0]
        assert len(await _trades(db)) == 1
    finally:
        await env.engine.stop()


# --------------------------------------------------------------------------- #
# B2
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("girise", [False, True])
async def test_a_stop_moved_towards_the_entry_never_sizes_the_others(db: Database, girise: bool) -> None:
    async def run(flag: bool, move: bool) -> Any:
        database = Database(":memory:")
        await database.open()
        env = await make_env(database, flag=flag)
        try:
            await env.crypto_signal()
            (conf,) = env.notifier.confirmations
            if move:
                if girise:                                              # (the short is in profit: entry stop is valid)
                    for b in (env.binance, env.tm):
                        b.quote = Quote(bid=2236.0, ask=2236.2, last=2236.1)
                    env.engine.prices.prices["ETH"] = 2236.1
                action = (ParsedAction(action=ActionType.MOVE_SL, move_sl_to_entry=True, refers_to_message_id=611,
                                       used_message_ids=[612], confidence=0.95, summary_tr="stop girişe") if girise
                          else ParsedAction(action=ActionType.MOVE_SL, new_stop_loss=2239.5, refers_to_message_id=611,
                                            used_message_ids=[612], confidence=0.95, summary_tr="stop 2239.5"))
                env.parser.push(result(action))
                await env.msg(612, "stop", topic=CRYPTO, reply_to=611)
                await env.engine.on_messages_ready(CHAT, KADIR, CRYPTO)
            for b in (env.binance, env.tm):
                b.quote = Quote(bid=2238.0, ask=2238.2, last=2238.1)
            env.engine.prices.prices["ETH"] = 2238.1
            await env.engine.handle_confirmation(conf.id, True)
            (req,) = env.binance.opened
            return req
        finally:
            await env.engine.stop()
            await database.close()

    plain = await run(False, False)                                     # no early account, no move: the reference
    moved = await run(True, True)
    assert moved.quantity == plain.quantity                             # sized from the signal's own stop
    assert moved.stop_loss < plain.stop_loss                            # but carries the trade's moved stop


# --------------------------------------------------------------------------- #
# B3
# --------------------------------------------------------------------------- #
async def test_the_trader_closing_while_the_approval_is_checked_opens_nothing(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        real = env.engine._approval_price_problem

        async def closing_meanwhile(plan: Any, raw: Any) -> Any:
            env.parser.push(result(ParsedAction(action=ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=611,
                                                base_asset="ETH", side=Side.SHORT, used_message_ids=[613],
                                                confidence=0.95, summary_tr="kapat")))
            await env.msg(613, "kapattık", topic=CRYPTO, reply_to=611)
            await env.engine.on_messages_ready(CHAT, KADIR, CRYPTO)
            return await real(plan, raw)

        env.engine._approval_price_problem = closing_meanwhile          # type: ignore[method-assign]
        out = await env.engine.handle_confirmation(conf.id, True)
        assert "onay beklerken kapandı" in out and env.binance.opened == []
        assert (await db.get_trade(1)).status is TradeStatus.CLOSED      # type: ignore[union-attr]
    finally:
        await env.engine.stop()


# --------------------------------------------------------------------------- #
# B4, B5, B6
# --------------------------------------------------------------------------- #
async def test_an_early_account_refused_by_its_own_rule_is_retried_and_the_question_says_so(db: Database) -> None:
    env = await make_env(db)
    env.tm.config.daily_loss_limit_pct = 3.0
    real = db.realized_pnl_since

    async def lost_today(name: str, start: Any) -> float:
        return -100000.0 if name == "tickmill" else await real(name, start)

    db.realized_pnl_since = lost_today                                  # type: ignore[method-assign]
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        assert conf.early is not None and conf.early["done"] == []
        assert "onaylarsanız tickmill hesabı da yeniden denenir" in conf.early["line"]
        db.realized_pnl_since = real                                    # type: ignore[method-assign]
        await env.engine.handle_confirmation(conf.id, True)
        assert len(env.tm.opened) == 1 and len(env.binance.opened) == 1
    finally:
        await env.engine.stop()


async def test_an_edit_that_withdraws_the_signal_says_the_early_trade_stays(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        none = ParsedAction(action=ActionType.NONE, used_message_ids=[611], confidence=0.9, summary_tr="yok")
        env.parser.push(result(none), result(none))
        msg = StoredMessage(chat_id=CHAT, message_id=611, sender_id=KADIR, sender_name="Kadir", date=now(),
                            text="iptal, girmeyin", reply_to_message_id=CRYPTO, topic_id=CRYPTO,
                            topic_title=TITLES[CRYPTO])
        await db.save_message(msg)
        await env.engine.on_message_edited(msg)
        text = env.notifier.closed_text(conf.id) or ""
        assert "işlem yapılmadı" in text and "tickmill hesabındaki #1 işlemi açık kalır" in text
    finally:
        await env.engine.stop()


async def test_no_while_the_early_order_is_unanswered_does_not_call_it_open(db: Database, monkeypatch: Any) -> None:
    from copytrader.engine import orchestrator

    monkeypatch.setattr(orchestrator, "OPTIONAL_LEG_GRACE_SEC", 0.05)
    env = await make_env(db)
    gate = asyncio.Event()

    async def failing(req: Any) -> Any:
        await gate.wait()
        raise BrokerError("requote")

    env.tm.open_position = failing                                      # type: ignore[method-assign]
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        out = await env.engine.handle_confirmation(conf.id, False)
        assert "yanıtı bekleniyor" in out and "açık kalır" not in out
    finally:
        gate.set()
        await env.engine.stop()


# --------------------------------------------------------------------------- #
# B7
# --------------------------------------------------------------------------- #
async def test_an_error_in_the_early_open_keeps_the_link_one_trade_then_closed_together(db: Database) -> None:
    env = await make_env(db)
    real = db.update_trade
    state = {"n": 0}

    async def flaky(trade_id: int, **kw: Any) -> Any:
        if state["n"] == 0 and "note" in kw:
            state["n"] += 1
            raise RuntimeError("db hiccup")
        return await real(trade_id, **kw)

    db.update_trade = flaky                                             # type: ignore[method-assign]
    try:
        await env.crypto_signal()
        conf = env.notifier.confirmations[-1]
        assert conf.early is not None and conf.early["trade_id"] == 1 and conf.early["done"] == ["tickmill"]
        await env.engine.handle_confirmation(conf.id, True)
        (trade,) = await _trades(db)
        assert {p.broker for p in await db.broker_positions(trade.id)} == {"tickmill", "binance"}
        env.parser.push(result(ParsedAction(action=ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=611,
                                            base_asset="ETH", side=Side.SHORT, used_message_ids=[613],
                                            confidence=0.95, summary_tr="kapat")))
        await env.msg(613, "kapattık", topic=CRYPTO, reply_to=611)
        await env.engine.on_messages_ready(CHAT, KADIR, CRYPTO)
        assert env.tm.calls_of("close_position") and env.binance.calls_of("close_position")
        assert len(env.tm.opened) == 1
    finally:
        await env.engine.stop()


async def test_yes_after_a_restart_adds_the_others_never_tickmill_twice(tmp_path: Any) -> None:
    path = str(tmp_path / "bot.db")
    first = Database(path)
    await first.open()
    env = await make_env(first)
    await env.crypto_signal()
    (conf,) = env.notifier.confirmations
    await env.engine.stop()
    await first.close()
    again = Database(path)
    await again.open()
    env2 = await make_env(again)
    try:
        await env2.engine.handle_confirmation(conf.id, True)
        (trade,) = await _trades(again)
        assert {p.broker for p in await again.broker_positions(trade.id)} == {"tickmill", "binance"}
        assert env2.tm.opened == [] and len(env2.binance.opened) == 1
    finally:
        await env2.engine.stop()
        await again.close()


# --------------------------------------------------------------------------- #
# B8
# --------------------------------------------------------------------------- #
def test_up_rounding_to_the_smallest_order_is_told_on_the_exchange_path() -> None:
    # 1 unit loses 100 at the stop: 4 USD -> 0.04, step / minimum 0.1 -> 0.1 loses 10 (6 over: within 10)
    res = compute_linear_size(risk_amount=4.0, entry=100.0, stop=99.0, qty_step=0.1, min_qty=0.1,
                              contract_size=100.0, rounding="up", include_fees=False)
    assert res.quantity == 0.1
    assert any("minimum emir büyüklüğüne yükseltildi" in w for w in res.warnings)
    plain = compute_linear_size(risk_amount=41.0, entry=100.0, stop=99.0, qty_step=0.1, min_qty=0.1,
                                contract_size=100.0, rounding="up", include_fees=False)
    assert plain.quantity == 0.5 and plain.warnings == []


# --------------------------------------------------------------------------- #
# follow-ups of the review round
# --------------------------------------------------------------------------- #
async def test_a_corrected_idea_takes_an_early_account_whose_first_attempt_sent_nothing(db: Database) -> None:
    env = await make_env(db)
    env.tm.config.daily_loss_limit_pct = 3.0
    real = db.realized_pnl_since

    async def lost_today(name: str, start: Any) -> float:
        return -100000.0 if name == "tickmill" else await real(name, start)

    db.realized_pnl_since = lost_today                                  # type: ignore[method-assign]
    try:
        await env.crypto_signal()                                       # tickmill refused by its daily loss rule
        (conf,) = env.notifier.confirmations
        assert conf.early is not None and conf.early["done"] == []
        db.realized_pnl_since = real                                    # type: ignore[method-assign]
        await env.engine.request_correction(conf.id)
        env.parser.push(result(eth_short(side=Side.LONG, stop_loss=2228.0, take_profits=[], summary_tr="ETH LONG",
                                         confidence=0.95)))
        await env.engine.handle_correction(conf.id, "LONG")
        out = await env.engine.handle_confirmation(env.notifier.confirmations[-1].id, True)
        assert [r.plan.side for r in env.tm.opened] == [Side.LONG]       # (it holds nothing of the first reading)
        assert [r.plan.side for r in env.binance.opened] == [Side.LONG]
        assert "ilk okumayla" not in out
    finally:
        await env.engine.stop()


async def test_after_a_corrected_idea_the_traders_replies_reach_the_new_trade_and_the_line_says_so(
    db: Database
) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        await env.engine.request_correction(conf.id)
        env.parser.push(result(eth_short(side=Side.LONG, stop_loss=2228.0, take_profits=[], summary_tr="ETH LONG",
                                         confidence=0.95)))
        await env.engine.handle_correction(conf.id, "LONG")
        out = await env.engine.handle_confirmation(env.notifier.confirmations[-1].id, True)
        assert "#1 ayrı kalır" in out and "elle yönetin" in out
        env.parser.push(result(ParsedAction(action=ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=611,
                                            base_asset="ETH", side=Side.LONG, used_message_ids=[613],
                                            confidence=0.95, summary_tr="kapat")))
        await env.msg(613, "kapattık", topic=CRYPTO, reply_to=611)
        await env.engine.on_messages_ready(CHAT, KADIR, CRYPTO)
        assert env.binance.calls_of("close_position") and env.tm.calls_of("close_position") == []
        assert (await db.get_trade(1)).status is TradeStatus.OPEN        # type: ignore[union-attr]
    finally:
        await env.engine.stop()


async def test_the_early_trade_failing_while_the_approval_is_checked_opens_the_others_fresh(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        real = env.engine._approval_price_problem

        async def failed_meanwhile(plan: Any, raw: Any) -> Any:
            await db.update_trade(1, status=TradeStatus.FAILED)         # (e.g. its late order answered with a refusal)
            return await real(plan, raw)

        env.engine._approval_price_problem = failed_meanwhile           # type: ignore[method-assign]
        out = await env.engine.handle_confirmation(conf.id, True)
        assert "onay beklerken" not in out and len(env.binance.opened) == 1
        assert len(env.tm.opened) == 1                                  # (tickmill's order went out: not again)
    finally:
        await env.engine.stop()


# --------------------------------------------------------------------------- #
# round 2
# --------------------------------------------------------------------------- #
async def _move_stop(env: Any, price: float) -> None:
    env.parser.push(result(ParsedAction(action=ActionType.MOVE_SL, new_stop_loss=price, refers_to_message_id=611,
                                        used_message_ids=[612], confidence=0.95, summary_tr=f"stop {price}")))
    await env.msg(612, f"stop {price}", topic=CRYPTO, reply_to=611)
    await env.engine.on_messages_ready(CHAT, KADIR, CRYPTO)


def _quote(env: Any, bid: float) -> None:
    for b in (env.binance, env.tm):
        b.quote = Quote(bid=bid, ask=bid + 0.2, last=bid + 0.1)
    env.engine.prices.prices["ETH"] = bid + 0.1


async def _plain_quantity() -> float:
    """binance's size for the same signal and price without any early account or stop move (the reference)."""
    database = Database(":memory:")
    await database.open()
    env = await make_env(database, flag=False)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        _quote(env, 2238.0)
        await env.engine.handle_confirmation(conf.id, True)
        (req,) = env.binance.opened
        return req.quantity
    finally:
        await env.engine.stop()
        await database.close()


async def test_the_fallback_fresh_trade_is_sized_from_the_signals_stop_too(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        _quote(env, 2236.0)
        await _move_stop(env, 2239.5)
        _quote(env, 2238.0)
        real = env.engine._approval_price_problem

        async def failed_meanwhile(plan: Any, raw: Any) -> Any:
            await db.update_trade(1, status=TradeStatus.FAILED)
            return await real(plan, raw)

        env.engine._approval_price_problem = failed_meanwhile           # type: ignore[method-assign]
        await env.engine.handle_confirmation(conf.id, True)
        (req,) = env.binance.opened
        assert req.stop_loss == 2239.5 and req.quantity == await _plain_quantity()
    finally:
        await env.engine.stop()


async def test_a_re_ask_keeps_the_owners_plan_the_moved_stop_never_becomes_a_correction(db: Database) -> None:
    from copytrader.engine.orchestrator import _issue

    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        _quote(env, 2236.0)
        await _move_stop(env, 2239.5)
        _quote(env, 2238.0)
        real = env.engine._trade_issues
        calls = {"n": 0}

        async def once(plan: Any, chat_id: int, **kw: Any) -> Any:
            out = await real(plan, chat_id, **kw)
            calls["n"] += 1
            return out + [_issue("duplicate_signal", "Benzer bir işlem zaten var (#9).")] if calls["n"] == 1 else out

        env.engine._trade_issues = once                                 # type: ignore[method-assign]
        await env.engine.handle_confirmation(conf.id, True)             # something new: asked again
        new = env.notifier.confirmations[-1]
        assert new.id != conf.id and new.plan.stop_loss == 2250.6       # type: ignore[union-attr]
        await env.engine.handle_confirmation(new.id, True)
        (req,) = env.binance.opened
        assert req.stop_loss == 2239.5 and req.quantity == await _plain_quantity()
        assert len(await _trades(db)) == 1 and len(env.tm.opened) == 1
    finally:
        await env.engine.stop()


async def test_yes_while_the_traders_close_of_the_early_trade_is_still_owed_opens_nothing(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        env.tm.fail["close_position"] = BrokerError("market closed / link down")
        env.parser.push(result(ParsedAction(action=ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=611,
                                            base_asset="ETH", side=Side.SHORT, used_message_ids=[613],
                                            confidence=0.95, summary_tr="kapat")))
        await env.msg(613, "kapattık", topic=CRYPTO, reply_to=611)
        await env.engine.on_messages_ready(CHAT, KADIR, CRYPTO)
        trade = await db.get_trade(1)
        assert trade is not None and await env.engine._closing(trade)
        out = await env.engine.handle_confirmation(conf.id, True)
        assert "onay beklerken kapanıyor" in out and env.binance.opened == []
    finally:
        await env.engine.stop()


async def test_a_close_owed_meanwhile_is_found_under_the_lock_too(db: Database) -> None:
    env = await make_env(db)
    try:
        await env.crypto_signal()
        (conf,) = env.notifier.confirmations
        env.tm.fail["close_position"] = BrokerError("link down")
        real = env.engine._approval_price_problem

        async def closing_meanwhile(plan: Any, raw: Any) -> Any:
            env.parser.push(result(ParsedAction(action=ActionType.CLOSE, close_fraction=1.0, refers_to_message_id=611,
                                                base_asset="ETH", side=Side.SHORT, used_message_ids=[613],
                                                confidence=0.95, summary_tr="kapat")))
            await env.msg(613, "kapattık", topic=CRYPTO, reply_to=611)
            await env.engine.on_messages_ready(CHAT, KADIR, CRYPTO)
            return await real(plan, raw)

        env.engine._approval_price_problem = closing_meanwhile          # type: ignore[method-assign]
        out = await env.engine.handle_confirmation(conf.id, True)
        assert "onay beklerken kapanıyor" in out and env.binance.opened == []
    finally:
        await env.engine.stop()
