"""Regression tests: confirmations vs. later actions on the same signal (M2, F0, F3, F4) and edits (F2, F6)."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from copytrader.errors import ParserError
from copytrader.models import (
    ActionType,
    ConfirmationStatus,
    ParseContext,
    ParseResult,
    Severity,
    StoredMessage,
    TradePlan,
)

from test_engine import (  # noqa: F401  (fixtures)
    CHAT,
    IMAGE,
    SENDER,
    Env,
    action,
    add_msg,
    db,
    env,
    eth_open,
    now,
    result,
    wait_for,
)


def codes(conf) -> list[str]:  # type: ignore[no-untyped-def]
    return [i.code for i in conf.issues]


async def ask_s1(env: Env) -> str:
    """S1 (msg 501): screenshot + 'eth 1R long', read with an ambiguity -> the owner is asked."""
    env.parser.push(result(eth_open(used_message_ids=[501], confidence=0.8, ambiguities=["SL etiketi bulanık"])))
    await add_msg(env.db, 501, "eth 1R long", media=IMAGE)
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.opens() == 0 and len(env.notifier.confirmations) == 1
    return env.notifier.confirmations[0].id


# --------------------------------------------------------------------------- #
# M2 / F0: one signal must not open twice through "pending question + automatic path"
# --------------------------------------------------------------------------- #
async def test_reply_built_on_a_pending_signal_is_asked_not_executed(env: Env) -> None:
    s1 = await ask_s1(env)
    env.parser.push(result(eth_open(used_message_ids=[503, 501])))      # 'stop 2205.11' as a reply to 501
    await add_msg(env.db, 503, "stop 2205.11", reply_to=501)
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.opens() == 0
    second = env.notifier.confirmations[-1]
    assert second.id != s1 and "pending_confirmation" in codes(second)


async def test_repost_while_the_first_signal_is_pending_is_asked(env: Env) -> None:
    await ask_s1(env)
    env.parser.push(result(eth_open(used_message_ids=[510])))           # clear repost, other message id
    await add_msg(env.db, 510, "ETH long 2232.18 stop 2205.11 tp 2310.6 1R")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.opens() == 0
    assert "duplicate_pending_signal" in codes(env.notifier.confirmations[-1])


async def test_approval_rechecks_trades_created_meanwhile_and_asks_again(env: Env) -> None:
    s1 = await ask_s1(env)
    # Meanwhile the same signal got executed (e.g. a repost that ran automatically before this fix).
    pending = await env.db.get_confirmation(s1)
    assert pending is not None and isinstance(pending.plan, TradePlan)
    other = pending.plan.model_copy(update={"plan_id": "repost", "source_message_ids": [503, 501]})
    await env.engine.execute_open(other)
    assert env.opens() == 2                                              # one trade on both brokers

    text = await env.engine.handle_confirmation(s1, True)
    assert env.opens() == 2, "approving must not open the same signal a second time"
    assert "durum değişti" in text and "#1" in text
    assert (await env.db.get_confirmation(s1)).status is ConfirmationStatus.SUPERSEDED  # type: ignore[union-attr]
    again = env.notifier.confirmations[-1]
    assert again.id != s1 and {"duplicate_signal", "message_already_traded"} <= set(codes(again))
    # The owner knowingly approves the new question: now it runs.
    assert "açıldı" in await env.engine.handle_confirmation(again.id, True)
    assert env.opens() == 4


async def test_approval_still_executes_when_nothing_changed(env: Env) -> None:
    s1 = await ask_s1(env)
    assert "açıldı" in await env.engine.handle_confirmation(s1, True)
    assert env.opens() == 2


# --------------------------------------------------------------------------- #
# F4: rejected / expired signal must not auto-execute later via a reply
# --------------------------------------------------------------------------- #
async def test_reply_to_a_rejected_signal_is_asked(env: Env) -> None:
    s1 = await ask_s1(env)
    assert "İptal" in await env.engine.handle_confirmation(s1, False)
    env.parser.push(result(eth_open(used_message_ids=[530, 501])))
    await add_msg(env.db, 530, "ETH hâlâ geçerli, girmeyenler girebilir", reply_to=501)
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.opens() == 0
    again = env.notifier.confirmations[-1]
    assert again.id != s1 and "message_previously_decided" in codes(again)
    assert any("reddedilmişti" in i.message_tr for i in again.issues)


async def test_reply_to_an_expired_signal_is_asked(env: Env) -> None:
    env.cfg.notifier.confirm_timeout_sec = 0.1  # type: ignore[assignment]
    s1 = await ask_s1(env)
    await wait_for(lambda: env.notifier.closed_text(s1) is not None)
    env.cfg.notifier.confirm_timeout_sec = 600
    env.parser.push(result(eth_open(used_message_ids=[530, 501])))
    await add_msg(env.db, 530, "ETH hâlâ geçerli", reply_to=501)
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.opens() == 0
    assert any("süresi dolmuştu" in i.message_tr for i in env.notifier.confirmations[-1].issues)


async def test_rejected_management_question_is_asked_again_later(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await add_msg(env.db, 501, media=IMAGE)
    await add_msg(env.db, 502, "eth 1R long")
    await env.engine.on_messages_ready(CHAT, SENDER)
    trade = (await env.db.active_trades())[0]
    env.parser.push(result(action(ActionType.CLOSE, base_asset="ETH", close_fraction=0.5, target_trade_id=trade.id,
                                  used_message_ids=[600], confidence=0.6)))
    await add_msg(env.db, 600, "yarısını alın")
    await env.engine.on_messages_ready(CHAT, SENDER)
    conf = env.notifier.confirmations[-1]
    await env.engine.handle_confirmation(conf.id, False)
    env.parser.push(result(action(ActionType.CLOSE, base_asset="ETH", close_fraction=0.5, target_trade_id=trade.id,
                                  used_message_ids=[601, 600])))
    await add_msg(env.db, 601, "evet, ETH'nin yarısını kapatın", reply_to=600)
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert all(b.calls_of("close_position") == [] for b in env.brokers)
    assert "message_previously_decided" in codes(env.notifier.confirmations[-1])


async def test_management_on_the_trades_own_signal_stays_automatic(env: Env) -> None:
    """The open confirmation's outcome must not make every later management of that trade ask."""
    s1 = await ask_s1(env)
    await env.engine.handle_confirmation(s1, True)
    trade = (await env.db.active_trades())[0]
    env.parser.push(result(action(ActionType.MOVE_SL, base_asset="ETH", new_stop_loss=2220.0,
                                  target_trade_id=trade.id, used_message_ids=[610, 501])))
    await add_msg(env.db, 610, "stop 2220", reply_to=501)
    n = len(env.notifier.confirmations)
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert len(env.notifier.confirmations) == n
    assert all(len(b.calls_of("update_stop_loss")) == 1 for b in env.brokers)


# --------------------------------------------------------------------------- #
# F3: after the final transient failure the stale signal never auto-executes later
# --------------------------------------------------------------------------- #
def transient_error() -> ParserError:
    err = ParserError("Yapay zekâ servisi hata döndürdü (HTTP 529).")
    err.transient = True  # type: ignore[attr-defined]
    return err


async def test_final_transient_failure_consumes_and_tells_the_owner(env: Env) -> None:
    env.engine.retry_delays = (0.01,)
    env.parser.push(transient_error(), transient_error())
    await add_msg(env.db, 501, "eth long 2232.18 stop 2205.11 tp 2310.6 1R")
    await env.engine.on_messages_ready(CHAT, SENDER)
    await wait_for(lambda: len(env.parser.calls) == 2 and len(env.notifier.texts(Severity.WARNING)) == 2)
    final = env.notifier.texts(Severity.WARNING)[-1]
    assert "2 denemede okunamadı" in final and "otomatik açılmayacak" in final
    assert (await env.db.get_message(CHAT, 501)).consumed  # type: ignore[union-attr]

    # Later chatter from the trader: the old signal is not given to the parser again.
    env.parser.push(result(action(ActionType.NONE, used_message_ids=[520], confidence=0.99)))
    await add_msg(env.db, 520, "BTC'de de bir fırsat var, bakıyorum")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert [m.message_id for m in env.parser.calls[-1][0]] == [520]
    # ...and if a reply builds the trade on it, the owner is asked.
    env.parser.push(result(eth_open(used_message_ids=[521, 501])))
    await add_msg(env.db, 521, "eth hâlâ geçerli", reply_to=501)
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.opens() == 0
    assert "message_previously_decided" in codes(env.notifier.confirmations[-1])


# --------------------------------------------------------------------------- #
# F2: an edit that arrives while the message is being parsed is not lost
# --------------------------------------------------------------------------- #
async def test_edit_during_parse_becomes_a_proposal(env: Env) -> None:
    edit_task: list[asyncio.Task[None]] = []
    old = eth_open(used_message_ids=[700], stop_loss=2205.11)
    new = eth_open(used_message_ids=[700], stop_loss=2215.0)

    def first_parse(msgs: list[StoredMessage], ctx: ParseContext) -> ParseResult:
        # The trader edits the stop while the (slow) model call is running.
        edited = msgs[0].model_copy(update={"text": "eth long 2232.18 stop 2215 tp 2310.6 1R", "edited": True})

        async def edit() -> None:
            await env.db.save_message(edited)
            await env.engine.on_message_edited(edited)

        edit_task.append(asyncio.ensure_future(edit()))
        return result(old)

    env.parser.push(first_parse, result(new))
    await add_msg(env.db, 700, "eth long 2232.18 stop 2205.11 tp 2310.6 1R")
    await env.engine.on_messages_ready(CHAT, SENDER)
    await asyncio.wait_for(edit_task[0], 2)
    assert env.opens() == 2
    assert [m.text for m in env.parser.calls[1][0]] == ["eth long 2232.18 stop 2215 tp 2310.6 1R"]
    conf = env.notifier.confirmations[-1]
    assert conf.kind == "manage" and conf.plan is not None and conf.plan.new_stop_loss == 2215.0  # type: ignore[union-attr]
    assert "signal_edited" in codes(conf)


# --------------------------------------------------------------------------- #
# F6: a message read as "none" and then edited into a signal
# --------------------------------------------------------------------------- #
async def test_edit_of_a_none_message_into_a_signal_is_asked(env: Env) -> None:
    env.parser.push(result(action(ActionType.NONE, used_message_ids=[700], confidence=0.98)))
    msg = await add_msg(env.db, 700, "ETH'ye bakıyorum 👀 2232")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert (await env.db.get_message(CHAT, 700)).consumed and env.notifier.confirmations == []  # type: ignore[union-attr]

    edited = msg.model_copy(update={"text": "ETH long giriş 2232.18 stop 2205.11 tp 2310.6 1R", "edited": True,
                                    "consumed": True})
    await env.db.save_message(edited)
    env.parser.push(result(eth_open(used_message_ids=[700])))
    await env.engine.on_message_edited(edited)
    assert env.opens() == 0, "an edit is never executed automatically"
    conf = env.notifier.confirmations[-1]
    assert conf.kind == "open" and "edited_after_read" in codes(conf)
    assert [m.text for m in env.parser.calls[-1][0]] == ["ETH long giriş 2232.18 stop 2205.11 tp 2310.6 1R"]


async def test_edit_of_an_old_none_message_is_ignored(env: Env) -> None:
    old = StoredMessage(chat_id=CHAT, message_id=701, sender_id=SENDER, date=now() - timedelta(hours=3),
                        text="ETH long 2232.18 stop 2205.11", edited=True, consumed=True)
    await env.db.save_message(old)
    await env.engine.on_message_edited(old)
    assert env.parser.calls == [] and env.notifier.confirmations == []


async def test_edit_of_a_none_message_that_is_still_chatter_stays_silent(env: Env) -> None:
    msg = StoredMessage(chat_id=CHAT, message_id=702, sender_id=SENDER, date=now(),
                        text="ETH 2300'ü kırarsa uçar", edited=True, consumed=True)
    await env.db.save_message(msg)
    env.parser.push(result(action(ActionType.NONE, used_message_ids=[702], confidence=0.97)))
    await env.engine.on_message_edited(msg)
    assert env.notifier.confirmations == [] and env.notifier.notifications == []


async def test_unanswered_is_this_a_trade_question_does_not_block_a_later_signal(env: Env) -> None:
    env.cfg.notifier.confirm_timeout_sec = 0.1  # type: ignore[assignment]
    env.parser.push(result(action(ActionType.NONE, used_message_ids=[700], confidence=0.4)))
    await add_msg(env.db, 700, "ETH grafiği 2232 civarı ilginç")
    await env.engine.on_messages_ready(CHAT, SENDER)
    unsure = env.notifier.confirmations[-1]
    await wait_for(lambda: env.notifier.closed_text(unsure.id) is not None)
    env.parser.push(result(eth_open(used_message_ids=[705, 700])))
    await add_msg(env.db, 705, "ETH long 2232.18 stop 2205.11 tp 2310.6 1R", reply_to=700)
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert env.opens() == 2
