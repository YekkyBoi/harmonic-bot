"""Regression tests (N2): an edit of a management message that already ran never proposes the same action again
as new. The same reading is ignored (log only); a different reading is asked with a text that says the original
was already applied; nothing is ever re-applied automatically."""

from __future__ import annotations

from copytrader.models import ActionType, ConfirmationStatus, Severity, Side

from test_engine import CHAT, SENDER, Env, action, add_msg, db, env, eth_open, result  # noqa: F401  (fixtures)


def close(trade_id: int, fraction: float | None, summary: str = "ETH kapat"):  # type: ignore[no-untyped-def]
    return action(ActionType.CLOSE, base_asset="ETH", side=Side.LONG, target_trade_id=trade_id,
                  close_fraction=fraction, used_message_ids=[510], summary_tr=summary)


async def opened(env: Env) -> int:
    env.parser.push(result(eth_open()))
    await env.signal()
    return (await env.db.active_trades())[0].id


async def edit(env: Env, mid: int, text: str) -> None:
    msg = await add_msg(env.db, mid, text)
    msg = msg.model_copy(update={"edited": True, "consumed": True})
    await env.db.save_message(msg)
    await env.engine.on_message_edited(msg)


def close_calls(env: Env) -> list[int]:
    return [len(b.calls_of("close_position")) for b in env.brokers]


async def events(env: Env, kind: str) -> list[dict]:
    async with env.db._read() as conn:  # type: ignore[attr-defined]
        rows = await env.db._fetchall(conn, "SELECT payload_json FROM events WHERE kind = ?", (kind,))  # type: ignore[attr-defined]
    import json

    return [json.loads(r["payload_json"]) for r in rows]


async def test_typo_fix_of_an_applied_partial_close_is_ignored(env: Env) -> None:
    tid = await opened(env)
    env.parser.push(result(close(tid, 0.5)))
    await add_msg(env.db, 510, "eth yarısını kapatım")
    await env.engine.on_messages_ready(CHAT, SENDER)
    assert close_calls(env) == [1, 1] and env.notifier.confirmations == []
    env.parser.push(result(close(tid, 0.5)))                     # the same reading after the typo fix
    await edit(env, 510, "eth yarısını kapattım")
    assert env.notifier.confirmations == [] and close_calls(env) == [1, 1]
    logged = await events(env, "edited_applied_unchanged")
    assert len(logged) == 1 and logged[0]["message_id"] == 510


async def test_a_different_reading_is_asked_and_says_the_original_was_applied(env: Env) -> None:
    tid = await opened(env)
    env.parser.push(result(close(tid, 0.5)))
    await add_msg(env.db, 510, "eth yarısını kapattım")
    await env.engine.on_messages_ready(CHAT, SENDER)
    env.parser.push(result(close(tid, 1.0, "ETH tamamı kapatılacak")))
    await edit(env, 510, "eth tamamını kapattım")
    assert close_calls(env) == [1, 1]                            # nothing re-applied automatically
    conf = env.notifier.confirmations[-1]
    codes = [i.code for i in conf.issues]
    assert codes[0] == "edited_after_applied" and "message_already_applied" not in codes
    text = conf.issues[0].message_tr
    assert "ZATEN UYGULANDI" in text and f"#{tid} ETH LONG: pozisyonun %50'si kapatıldı" in text
    assert "yeni okuma" in text and "tamamı kapatılacak" in text and "geri alınmaz" in text
    assert conf.status is ConfirmationStatus.PENDING


async def test_an_approved_management_question_counts_as_applied(env: Env) -> None:
    tid = await opened(env)
    env.parser.push(result(action(ActionType.MOVE_SL, base_asset="ETH", target_trade_id=tid, new_stop_loss=2220.0,
                                  used_message_ids=[520], confidence=0.7, summary_tr="stop 2220")))
    await add_msg(env.db, 520, "stopu 2220 ye çektm")
    await env.engine.on_messages_ready(CHAT, SENDER)
    conf = env.notifier.confirmations[-1]
    await env.engine.handle_confirmation(conf.id, True)
    assert [len(b.calls_of("update_stop_loss")) for b in env.brokers] == [1, 1]
    n = len(env.notifier.confirmations)
    env.parser.push(result(action(ActionType.MOVE_SL, base_asset="ETH", target_trade_id=tid, new_stop_loss=2220.0,
                                  used_message_ids=[520], confidence=0.95, summary_tr="stop 2220")))
    await edit(env, 520, "stopu 2220 ye çektim")
    assert len(env.notifier.confirmations) == n
    assert [len(b.calls_of("update_stop_loss")) for b in env.brokers] == [1, 1]


async def test_edit_that_removes_the_instruction_warns_the_owner(env: Env) -> None:
    tid = await opened(env)
    env.parser.push(result(close(tid, 0.5)))
    await add_msg(env.db, 510, "eth yarısını kapattım")
    await env.engine.on_messages_ready(CHAT, SENDER)
    env.parser.push(result(action(ActionType.NONE, used_message_ids=[510], confidence=0.95)))
    await edit(env, 510, "eth yarısını kapatmayı düşünüyorum")
    warning = env.notifier.texts(Severity.WARNING)[-1]
    assert "<b>uygulanmış</b>" in warning and "%50'si kapatıldı" in warning and "geri alınmadı" in warning
    assert close_calls(env) == [1, 1] and env.notifier.confirmations == []


async def test_edit_of_a_never_applied_message_is_still_asked_as_before(env: Env) -> None:
    await opened(env)
    env.parser.push(result(action(ActionType.NONE, used_message_ids=[530], confidence=0.95)))
    await add_msg(env.db, 530, "ETH'ye bakıyorum 2232")
    await env.engine.on_messages_ready(CHAT, SENDER)
    env.parser.push(result(eth_open(used_message_ids=[530], base_asset="BTC", symbol="BTCUSDT.P", entry_price=67250.0,
                                    stop_loss=66800.0, take_profits=[])))
    env.oracle.prices["BTC"] = 67250.0
    await edit(env, 530, "BTC long 67250 stop 66800")
    conf = env.notifier.confirmations[-1]
    assert [i.code for i in conf.issues][0] == "edited_after_read"
