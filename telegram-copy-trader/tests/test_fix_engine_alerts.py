"""Regression tests: adapter problems reach the owner (M1) and stale partial limit entries expire (M6)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from copytrader.engine import monitor as monitor_mod
from copytrader.models import (
    ActionType,
    BrokerPosition,
    EntryType,
    OpenRequest,
    PositionStatus,
    Severity,
    TradeStatus,
)

from fakes import FakeBroker
from test_engine import CHAT, SENDER, Env, action, add_msg, db, env, eth_open, now, result  # noqa: F401  (fixtures)

KRITIK = ("KRİTİK binance: stop emri yerleştirilemedi ve pozisyon kapatılamadı! ETH/USDT:USDT pozisyonunu "
          "borsada hemen kontrol edin. (503 service unavailable)")


class UnprotectedBroker(FakeBroker):
    """Entry fills, but the SL and the emergency close both failed (like CcxtBroker during an incident)."""

    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        pos = await super().open_position(req)
        pos.data["unprotected"] = True
        pos.error = KRITIK
        return pos


def critical(env: Env) -> list[str]:
    return [t for t in env.notifier.texts(Severity.ERROR) if "STOPSUZ" in t]


async def test_unprotected_open_is_reported_at_once_and_repeated_by_monitor(
    env: Env, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(monitor_mod, "UNPROTECTED_REALERT_PASSES", 3)
    bad = UnprotectedBroker("binance")
    env.engine.brokers = [bad]
    assert env.cfg.notifier.notify_on_execution is False
    env.parser.push(result(eth_open()))
    await env.signal()

    alerts = critical(env)
    assert len(alerts) == 1 and "binance" in alerts[0] and "#1 ETH LONG" in alerts[0]
    assert "503 service unavailable" in alerts[0]
    trade = (await env.db.active_trades())[0]
    assert trade.note and "binance: partial" in trade.note

    for _ in range(2):                       # passes 1-2: already told, no repeat yet
        await env.engine.monitor.run_once()
    assert len(critical(env)) == 1
    await env.engine.monitor.run_once()      # pass 3: still without a stop -> told again
    assert len(critical(env)) == 2

    # The adapter finally places the stop: the owner hears that it is protected now.
    bad.sync_fn = lambda p: p.model_copy(update={"data": {**p.data, "unprotected": False}})
    await env.engine.monitor.run_once()
    assert any("artık stop ile korunuyor" in t for t in env.notifier.texts(Severity.INFO))
    for _ in range(5):
        await env.engine.monitor.run_once()
    assert len(critical(env)) == 2


async def test_unprotected_alert_also_when_the_sync_itself_fails(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    trade = (await env.db.active_trades())[0]
    pos = next(p for p in await env.db.broker_positions(trade.id) if p.broker == "binance")
    # Restart case: the position was saved unprotected; the venue is unreachable now.
    await env.db.save_broker_position(trade.id, pos.model_copy(update={"data": {**pos.data, "unprotected": True},
                                                                       "error": KRITIK}))
    env.a.fail["sync_position"] = RuntimeError("timeout")
    await env.engine.monitor.run_once()
    alerts = critical(env)
    assert len(alerts) == 1 and "binance" in alerts[0]


async def test_naked_metatrader_leg_is_reported(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    trade = (await env.db.active_trades())[0]
    pos = next(p for p in await env.db.broker_positions(trade.id) if p.broker == "bybit")
    legs = [{"ticket": 11, "state": "open", "naked": True}, {"ticket": 12, "state": "open", "sl": 2205.11}]
    await env.db.save_broker_position(trade.id, pos.model_copy(update={"data": {**pos.data, "legs": legs}}))
    await env.engine.monitor.run_once()
    alerts = critical(env)
    assert len(alerts) == 1 and "#11" in alerts[0] and "bybit" in alerts[0]


async def test_new_adapter_error_on_open_is_reported(env: Env) -> None:
    class TpFails(FakeBroker):
        async def open_position(self, req: OpenRequest) -> BrokerPosition:
            pos = await super().open_position(req)
            pos.error = "binance: kâr al emri yerleştirilemedi (stop aktif): reddedildi"
            return pos

    env.engine.brokers = [TpFails("binance")]
    env.parser.push(result(eth_open()))
    await env.signal()
    errors = env.notifier.texts(Severity.ERROR)
    assert len(errors) == 1 and "kâr al emri yerleştirilemedi" in errors[0]
    await env.engine.monitor.run_once()              # unchanged error: not repeated
    assert len(env.notifier.texts(Severity.ERROR)) == 1


async def test_monitor_reports_a_changed_adapter_error_once(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    emergency = "binance: stop emri yerleştirilemedi (zaman aşımı); pozisyon piyasa fiyatından kapatıldı."
    env.a.sync_fn = lambda p: p.model_copy(update={"status": PositionStatus.CLOSED, "remaining_quantity": 0.0,
                                                   "error": emergency})
    await env.engine.monitor.run_once()
    errors = env.notifier.texts(Severity.ERROR)
    assert len(errors) == 1 and "piyasa fiyatından kapatıldı" in errors[0] and "#1" in errors[0]


async def test_partial_management_failure_is_not_reported_as_ok(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()

    def some_legs_failed(pos: BrokerPosition, sl: float) -> BrokerPosition:
        return pos.model_copy(update={"stop_loss": sl, "error": "Stop bazı bacaklarda güncellenemedi: #12 reddedildi"})

    original = env.a.update_stop_loss

    async def update_stop_loss(pos: BrokerPosition, new_stop_loss: float) -> BrokerPosition:
        await original(pos, new_stop_loss)
        return some_legs_failed(pos, new_stop_loss)

    env.a.update_stop_loss = update_stop_loss  # type: ignore[method-assign]
    trade = (await env.db.active_trades())[0]
    env.parser.push(result(action(ActionType.MOVE_SL, base_asset="ETH", new_stop_loss=2220.0,
                                  target_trade_id=trade.id, used_message_ids=[600])))
    await add_msg(env.db, 600, "eth stop 2220")
    await env.engine.on_messages_ready(CHAT, SENDER)
    errors = env.notifier.texts(Severity.ERROR)
    assert len(errors) == 1 and "bacaklarda güncellenemedi" in errors[0] and "binance" in errors[0]
    # applied (the stop did move on the other legs / broker), so the trade's stop follows
    assert (await env.db.get_trade(trade.id)).current_stop_loss == 2220.0  # type: ignore[union-attr]


async def test_execute_management_head_says_partial(env: Env) -> None:
    env.parser.push(result(eth_open()))
    await env.signal()
    trade = (await env.db.active_trades())[0]

    async def update_take_profits(pos, tps):  # type: ignore[no-untyped-def]
        return pos.model_copy(update={"take_profits": list(tps), "error": "TP reddedildi"})

    env.a.update_take_profits = update_take_profits  # type: ignore[method-assign]
    from copytrader.models import ManagementPlan, TakeProfit

    plan = ManagementPlan(plan_id="x", action=ActionType.MOVE_TP, trade_id=trade.id,
                          take_profits=[TakeProfit(price=2320.0)], source_chat_id=trade.plan.source_chat_id,
                          created_at=now())
    text = await env.engine.execute_management(plan)
    assert "kısmen uygulandı" in text and "TP reddedildi" in text


# --------------------------------------------------------------------------- #
# M6: unfilled rest of a partially filled limit entry
# --------------------------------------------------------------------------- #
class PartialLimitBroker(FakeBroker):
    """Limit entry that filled 0.3 of its size; the rest keeps resting (entry_done False, like ccxt)."""

    def __init__(self, *a, with_hook: bool = True, **kw) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*a, **kw)
        if with_hook:
            self.cancel_entry_remainder = self._cancel_rest  # type: ignore[attr-defined]
        self.rest_cancelled = 0

    async def open_position(self, req: OpenRequest) -> BrokerPosition:
        pos = await super().open_position(req)
        old = int((now() - timedelta(days=3)).timestamp() * 1000)
        return pos.model_copy(update={
            "status": PositionStatus.OPEN, "filled_quantity": 0.3, "remaining_quantity": 0.3,
            "avg_entry_price": req.entry_price,
            "data": {**pos.data, "entry_type": "limit", "entry_done": False, "entry_created_ms": old,
                     "opened_at": int(now().timestamp() * 1000)},   # first fill just now (ccxt overwrites it)
        })

    async def _cancel_rest(self, pos: BrokerPosition) -> BrokerPosition:
        self.rest_cancelled += 1
        return pos.model_copy(update={"data": {**pos.data, "entry_done": True}})


async def test_monitor_cancels_only_the_rest_of_a_stale_partial_limit_entry(env: Env) -> None:
    env.cfg.risk.pending_entry_expiry_min = 60
    b = PartialLimitBroker("binance")
    env.engine.brokers = [b]
    env.parser.push(result(eth_open(entry_type=EntryType.LIMIT, entry_price=2220.0)))
    await env.signal()
    trade = (await env.db.active_trades())[0]
    assert trade.status is TradeStatus.OPEN
    for _ in range(3):
        await env.engine.monitor.run_once()
    assert b.rest_cancelled == 1 and b.calls_of("cancel_entry") == [] and b.calls_of("close_position") == []
    pos = (await env.db.broker_positions(trade.id))[0]
    assert pos.status is PositionStatus.OPEN and pos.data["entry_done"] is True and pos.filled_quantity == 0.3
    assert any("dolmayan kısmı" in t and "iptal edildi" in t for t in env.notifier.texts(Severity.INFO))


async def test_young_partial_limit_entry_is_left_alone(env: Env) -> None:
    env.cfg.risk.pending_entry_expiry_min = 60 * 24 * 7          # one week
    b = PartialLimitBroker("binance")
    env.engine.brokers = [b]
    env.parser.push(result(eth_open(entry_type=EntryType.LIMIT, entry_price=2220.0)))
    await env.signal()
    await env.engine.monitor.run_once()
    assert b.rest_cancelled == 0


async def test_partial_limit_rest_without_adapter_support_warns_once(env: Env) -> None:
    env.cfg.risk.pending_entry_expiry_min = 60
    b = PartialLimitBroker("binance", with_hook=False)
    env.engine.brokers = [b]
    env.parser.push(result(eth_open(entry_type=EntryType.LIMIT, entry_price=2220.0)))
    await env.signal()
    for _ in range(3):
        await env.engine.monitor.run_once()
    warnings = [t for t in env.notifier.texts(Severity.WARNING) if "elle iptal edin" in t]
    assert len(warnings) == 1
    assert b.calls_of("cancel_entry") == [] and b.calls_of("close_position") == []   # filled part untouched


async def test_a_different_error_soon_after_the_open_alert_still_reaches_the_owner(env: Env) -> None:
    class TpFails(FakeBroker):
        async def open_position(self, req: OpenRequest) -> BrokerPosition:
            pos = await super().open_position(req)
            pos.error = "binance: kâr al emri yerleştirilemedi (stop aktif)"
            return pos

    b = TpFails("binance")
    env.engine.brokers = [b]
    env.parser.push(result(eth_open()))
    await env.signal()
    await env.engine.monitor.run_once()
    b.sync_fn = lambda p: p.model_copy(update={
        "status": PositionStatus.CLOSED, "remaining_quantity": 0.0,
        "error": "binance: stop emri yerleştirilemedi; pozisyon piyasa fiyatından kapatıldı."})
    await env.engine.monitor.run_once()
    errors = env.notifier.texts(Severity.ERROR)
    assert len(errors) == 2 and "piyasa fiyatından kapatıldı" in errors[1]
