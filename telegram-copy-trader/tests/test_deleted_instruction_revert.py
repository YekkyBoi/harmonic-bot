"""A management instruction that already ran is undone when the trader deletes its message in the group
(26.11.2026: "entry stop atıyoruz" was deleted as a false alarm, the bot kept the stop at entry and was stopped
out while the group stayed in the trade). Stop and target moves are put back to what they were; closes cannot be
undone and are only reported. Nothing is undone when a later instruction changed the same level again."""

from __future__ import annotations

from copytrader.models import ActionType, Severity, Side, TakeProfit

from test_engine import CHAT, SENDER, Env, action, add_msg, db, env, eth_open, result  # noqa: F401  (fixtures)

ORIGINAL_SL = 2205.11


async def opened(env: Env) -> int:
    env.parser.push(result(eth_open()))
    await env.signal()
    return (await env.db.active_trades())[0].id


async def move_sl(env: Env, tid: int, mid: int, price: float) -> None:
    env.parser.push(result(action(ActionType.MOVE_SL, base_asset="ETH", target_trade_id=tid, new_stop_loss=price,
                                  used_message_ids=[mid], summary_tr=f"stop {price}")))
    await add_msg(env.db, mid, f"stopu {price} yaptık")
    await env.engine.on_messages_ready(CHAT, SENDER)


async def delete(env: Env, mid: int) -> None:
    msg = await env.db.get_message(CHAT, mid)
    assert msg is not None
    await env.engine.on_message_deleted(msg)


def sl_calls(env: Env) -> list[list[float]]:
    return [[c[1] for c in b.calls_of("update_stop_loss")] for b in env.brokers]


async def test_deleted_stop_move_is_put_back(env: Env) -> None:
    tid = await opened(env)
    await move_sl(env, tid, 520, 2232.18)
    assert sl_calls(env) == [[2232.18], [2232.18]]
    await delete(env, 520)
    assert sl_calls(env) == [[2232.18, ORIGINAL_SL], [2232.18, ORIGINAL_SL]]
    trade = await env.db.get_trade(tid)
    assert trade is not None and trade.current_stop_loss == ORIGINAL_SL
    assert any("silindi" in t and "geri alındı" in t for t in env.notifier.texts())


async def test_deleting_it_twice_undoes_once(env: Env) -> None:
    tid = await opened(env)
    await move_sl(env, tid, 520, 2232.18)
    await delete(env, 520)
    await delete(env, 520)
    assert sl_calls(env) == [[2232.18, ORIGINAL_SL], [2232.18, ORIGINAL_SL]]


async def test_not_undone_when_a_later_instruction_moved_the_stop_again(env: Env) -> None:
    tid = await opened(env)
    await move_sl(env, tid, 520, 2220.0)
    await move_sl(env, tid, 521, 2232.18)
    assert sl_calls(env) == [[2220.0, 2232.18], [2220.0, 2232.18]]
    await delete(env, 520)
    assert sl_calls(env) == [[2220.0, 2232.18], [2220.0, 2232.18]]
    trade = await env.db.get_trade(tid)
    assert trade is not None and trade.current_stop_loss == 2232.18
    assert any("geri alınmadı" in t for t in env.notifier.texts(Severity.WARNING))


async def test_deleted_target_move_is_put_back(env: Env) -> None:
    tid = await opened(env)
    env.parser.push(result(action(ActionType.MOVE_TP, base_asset="ETH", target_trade_id=tid,
                                  take_profits=[TakeProfit(price=2290.0, close_fraction=1.0)],
                                  used_message_ids=[530], summary_tr="tp 2290")))
    await add_msg(env.db, 530, "tp 2290")
    await env.engine.on_messages_ready(CHAT, SENDER)
    trade = await env.db.get_trade(tid)
    assert trade is not None and trade.current_take_profits[0].price == 2290.0
    await delete(env, 530)
    trade = await env.db.get_trade(tid)
    assert trade is not None and trade.current_take_profits[0].price == 2310.6


async def test_deleted_close_is_only_reported(env: Env) -> None:
    tid = await opened(env)
    env.parser.push(result(action(ActionType.CLOSE, base_asset="ETH", side=Side.LONG, target_trade_id=tid,
                                  close_fraction=0.5, used_message_ids=[510], summary_tr="ETH yarısı")))
    await add_msg(env.db, 510, "eth yarısını kapattım")
    await env.engine.on_messages_ready(CHAT, SENDER)
    before = [len(b.calls_of("close_position")) for b in env.brokers]
    await delete(env, 510)
    assert [len(b.calls_of("close_position")) for b in env.brokers] == before
    assert all(not b.calls_of("update_stop_loss") for b in env.brokers)
    assert any("geri alınamaz" in t for t in env.notifier.texts(Severity.WARNING))


async def test_listener_hands_a_processed_deleted_message_to_the_engine(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from types import SimpleNamespace

    from copytrader.models import StoredMessage
    from copytrader.telegram.listener import TelegramListener

    from test_engine import now

    row = StoredMessage(chat_id=CHAT, message_id=540, sender_id=SENDER, sender_name="Trader", date=now(),
                        text="entry stop atıyoruz", consumed=True)

    class StubDB:
        async def get_message(self, chat_id: int, mid: int):  # type: ignore[no-untyped-def]
            return row if mid == 540 else None

        async def set_message_meta(self, *a, **k) -> None:  # type: ignore[no-untyped-def]
            return None

        async def find_trade_by_message(self, *a):  # type: ignore[no-untyped-def]
            return None

    seen: list[int] = []

    async def on_delete(msg: StoredMessage) -> None:
        seen.append(msg.message_id)

    cfg = SimpleNamespace(allowed_sender_ids=[])
    listener = TelegramListener(cfg, StubDB(), str(tmp_path), lambda *a: None, lambda *a: None)  # type: ignore[arg-type]
    listener.set_on_delete(on_delete)
    await listener.handle_deleted(CHAT, [540, 541])
    assert seen == [540]


async def test_switched_off_in_config(env: Env) -> None:
    tid = await opened(env)
    await move_sl(env, tid, 520, 2232.18)
    env.engine.config.telegram.revert_deleted_instructions = False
    await delete(env, 520)
    assert sl_calls(env) == [[2232.18], [2232.18]]
