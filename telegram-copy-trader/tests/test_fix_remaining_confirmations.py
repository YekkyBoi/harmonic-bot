"""Regression tests: approving during shutdown leaves the confirmation actionable (N4); a re-asked confirmation
drops the stale pending-question issue, and refusals made by the bot itself at approval are remembered as such,
not as the owner's "no" (N5)."""

from __future__ import annotations

from copytrader.engine.orchestrator import Engine
from copytrader.interfaces import SHUTDOWN_TEXT
from copytrader.models import ConfirmationStatus
from copytrader.storage.db import Database
from copytrader.telegram.formatting import CORRECTION_HINT

from fakes import FakeBroker, FakeNotifier, FakePriceOracle
from test_engine import CHAT, SENDER, Env, add_msg, db, env, eth_open, make_config, result  # noqa: F401
from test_fix_engine_confirmations import ask_s1, codes
from test_notifier import CbEvent, FakeEngine, make_conf, setup  # noqa: F401  (fixture)


# --------------------------------------------------------------------------- #
# N4: notifier keeps the buttons for SHUTDOWN_TEXT
# --------------------------------------------------------------------------- #
class ShuttingDownEngine(FakeEngine):
    def __init__(self) -> None:
        super().__init__()
        self.shutting_down = True

    async def handle_confirmation(self, conf_id: str, approved: bool) -> str:
        if self.shutting_down:
            self.calls.append(("confirm", conf_id, approved))
            return SHUTDOWN_TEXT
        return await super().handle_confirmation(conf_id, approved)

    async def request_correction(self, conf_id: str) -> str:
        if self.shutting_down:
            self.calls.append(("fix", conf_id))
            return SHUTDOWN_TEXT
        return await super().request_correction(conf_id)


async def test_approve_during_shutdown_keeps_the_buttons(setup) -> None:  # type: ignore[no-untyped-def]
    n, client, _ = setup
    engine = ShuttingDownEngine()
    n._engine = engine
    msg_id = await n.ask_confirmation(make_conf())
    await n.handle_callback(CbEvent(b"ok:c1", message_id=msg_id))
    assert client.edits == []                               # buttons not removed
    assert client.sent[-1].text == SHUTDOWN_TEXT
    await n.handle_callback(CbEvent(b"fix:c1", message_id=msg_id))
    assert client.edits == [] and client.sent[-1].text == SHUTDOWN_TEXT    # no "write the correction" hint
    assert CORRECTION_HINT not in client.sent[-1].text
    # after the restart the same buttons work
    engine.shutting_down = False
    await n.handle_callback(CbEvent(b"ok:c1", message_id=msg_id))
    assert client.edits[-1].msg_id == msg_id and "İşlem açıldı" in client.edits[-1].text


async def test_confirmation_approved_during_shutdown_can_be_approved_after_restart(db: Database) -> None:
    def engine() -> tuple[Engine, FakeBroker, FakeNotifier]:
        broker, notifier = FakeBroker("binance"), FakeNotifier()
        return Engine(make_config(), db, _parser, [broker], notifier,
                      price_oracle=FakePriceOracle({"ETH": 2232.85})), broker, notifier

    from fakes import FakeParser

    _parser = FakeParser(result(eth_open(used_message_ids=[501], confidence=0.7)))
    first, broker1, notifier1 = engine()
    await first.start(monitor=False)
    await add_msg(db, 501, "eth 1R long 2232.18 sl 2205.11 tp 2310.6")
    await first.on_messages_ready(CHAT, SENDER)
    conf = notifier1.confirmations[-1]
    first.begin_shutdown()
    assert await first.handle_confirmation(conf.id, True) == SHUTDOWN_TEXT
    await first.stop()
    assert notifier1.closed == [] and broker1.opened == []
    assert (await db.get_confirmation(conf.id)).status is ConfirmationStatus.PENDING  # type: ignore[union-attr]

    second, broker2, _notifier2 = engine()
    await second.start(monitor=False)
    assert conf.id in second._expiry_tasks                  # restored with its expiry
    assert "açıldı" in await second.handle_confirmation(conf.id, True)
    assert len(broker2.opened) == 1
    await second.stop()


# --------------------------------------------------------------------------- #
# N5: stale pending issue dropped on a re-ask
# --------------------------------------------------------------------------- #
async def test_reask_drops_the_resolved_pending_confirmation_issue(env: Env) -> None:
    s1 = await ask_s1(env)
    env.parser.push(result(eth_open(used_message_ids=[503, 501])))
    await add_msg(env.db, 503, "stop 2205.11", reply_to=501)
    await env.engine.on_messages_ready(CHAT, SENDER)
    s2 = env.notifier.confirmations[-1]
    assert "pending_confirmation" in codes(s2)
    assert "açıldı" in await env.engine.handle_confirmation(s1, True)
    await env.engine.handle_confirmation(s2.id, True)          # conflicts now: re-asked
    again = env.notifier.confirmations[-1]
    assert again.id not in (s1, s2.id)
    assert {"duplicate_signal", "message_already_traded"} <= set(codes(again))
    assert "pending_confirmation" not in codes(again)
    assert env.opens() == 2                                    # one trade, on both brokers


async def test_reask_keeps_a_pending_issue_that_is_still_true(env: Env) -> None:
    s1 = await ask_s1(env)
    env.parser.push(result(eth_open(used_message_ids=[503, 501])))
    await add_msg(env.db, 503, "stop 2205.11", reply_to=501)
    await env.engine.on_messages_ready(CHAT, SENDER)
    s2 = env.notifier.confirmations[-1]
    pending = await env.db.get_confirmation(s1)
    assert pending is not None and pending.plan is not None
    # an unrelated trade on the same message makes s2's approval re-ask while s1 is STILL pending
    await env.engine.execute_open(pending.plan.model_copy(update={"plan_id": "other", "source_message_ids": [501]}))
    await env.engine.handle_confirmation(s2.id, True)
    again = env.notifier.confirmations[-1]
    assert "pending_confirmation" in codes(again) and s1 in " ".join(i.message_tr for i in again.issues)


# --------------------------------------------------------------------------- #
# N5: refusals made by the bot at approval are not "reddedilmişti"
# --------------------------------------------------------------------------- #
async def later_reply_issues(env: Env) -> list[str]:
    env.parser.push(result(eth_open(used_message_ids=[530, 501])))
    await add_msg(env.db, 530, "ETH hâlâ geçerli", reply_to=501)
    await env.engine.on_messages_ready(CHAT, SENDER)
    conf = env.notifier.confirmations[-1]
    assert "message_previously_decided" in codes(conf)
    return [i.message_tr for i in conf.issues if i.code == "message_previously_decided"]


async def test_paused_at_approval_is_remembered_as_auto_refused(env: Env) -> None:
    s1 = await ask_s1(env)
    await env.engine.set_paused(True)
    assert "duraklatılmış" in await env.engine.handle_confirmation(s1, True)
    await env.engine.set_paused(False)
    stored = await env.db.get_state(f"msg_outcomes:{CHAT}")
    assert stored["501"]["status"] == "auto_refused"
    texts = await later_reply_issues(env)
    assert len(texts) == 1 and "reddedilmişti" not in texts[0]
    assert "onay verilmiş ama bot uygulamamıştı" in texts[0] and "sistem duraklatılmıştı" in texts[0]
    assert env.opens() == 0


async def test_max_open_trades_at_approval_is_remembered_as_auto_refused(env: Env) -> None:
    s1 = await ask_s1(env)
    pending = await env.db.get_confirmation(s1)
    assert pending is not None and pending.plan is not None
    await env.engine.execute_open(pending.plan.model_copy(update={"plan_id": "other", "base_asset": "SOL",
                                                                  "source_message_ids": [400]}))
    env.cfg.risk.max_open_trades = 1
    assert "sınırına" in await env.engine.handle_confirmation(s1, True)
    env.cfg.risk.max_open_trades = 10
    texts = await later_reply_issues(env)
    assert "açık işlem sınırı (1) doluydu" in texts[0] and "reddedilmişti" not in texts[0]


async def test_price_check_at_approval_is_remembered_as_auto_refused(env: Env) -> None:
    s1 = await ask_s1(env)
    env.oracle.prices["ETH"] = 2200.0                       # below the stop
    assert "işlem açılmadı" in await env.engine.handle_confirmation(s1, True)
    env.oracle.prices["ETH"] = 2232.85
    texts = await later_reply_issues(env)
    assert "bot uygulamamıştı (anlık fiyat" in texts[0] and "stop seviyesini" in texts[0]


async def test_owner_rejection_is_still_reddedilmisti(env: Env) -> None:
    s1 = await ask_s1(env)
    await env.engine.handle_confirmation(s1, False)
    texts = await later_reply_issues(env)
    assert "reddedilmişti" in texts[0]
