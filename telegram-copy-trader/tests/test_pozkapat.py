"""/pozkapat: the owner closes the position of ONE account (or a few), from Telegram, without opening the broker.

  /pozkapat fx1            the account's only position: closed at once (a pending entry: cancelled); several: the bot
                           asks which (one button each, "Hepsini kapat", "Vazgeç")
  /pozkapat fx1,fx2        the same, account by account
  /pozkapat fx2 xau        only that account's XAU position(s); "#13" names a trade
  aliases                  "fx1" is FundingPips's other name (config ``aliases``), never a rename of "fx"

The trade goes on on its other accounts: nothing that stands for the whole trade changes, a later "kapattık" of the
trader closes the rest, and the last account closing gives the trade's closing summary. An unknown or manual account,
or an unreadable filter, closes nothing at all. Buttons: owner only, one use, refused once expired or after a restart.

REAL Engine and REAL BotNotifier; brokers and Telegram are fakes.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fakes import FakeBroker, FakePriceOracle
from test_friend_mirror import CHAT, FRIEND, OWNER, Client, _cfg
from test_notifier import CbEvent, MsgEvent

from copytrader.config import AppConfig, NotifierConfig, TelegramConfig
from copytrader.engine.orchestrator import Engine
from copytrader.models import (
    ActionType,
    EntryType,
    ManagementPlan,
    PositionStatus,
    Side,
    TakeProfit,
    TradePlan,
    TradeStatus,
)
from copytrader.storage.db import Database
from copytrader.telegram import notifier as notifier_mod
from copytrader.telegram.notifier import BotNotifier


def _plan(asset: str, names: list[str], *, quote: str | None = None, pid: str = "p") -> TradePlan:
    return TradePlan(plan_id=pid, base_asset=asset, quote_asset=quote, side=Side.LONG, entry_type=EntryType.MARKET,
                     entry_price=2232.85, stop_loss=2200.0, take_profits=[TakeProfit(price=2300.0, close_fraction=1.0)],
                     risk_r=1.0, risk_pct=2.0, source_chat_id=CHAT, source_message_ids=[1],
                     created_at=datetime.now(UTC), broker_names=names)


class World:
    """fx (alias fx1) + tickmill (optional) + breakout (manual-like: its config type is set to "manual")."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path

    async def up(self) -> World:
        self.db = Database(str(self.tmp_path / "bot.db"))
        await self.db.open()
        self.client = Client()
        self.n = BotNotifier(NotifierConfig(bot_token="123:abc", owner_user_id=OWNER),
                             TelegramConfig(api_id=1, api_hash="x", source_chats=[CHAT]), str(self.tmp_path))
        self.n._client = self.client
        self.fx = FakeBroker("fx")
        self.fx.config.aliases = ["fx1", "FundingPips"]
        self.tm = FakeBroker("tickmill", optional=True)
        self.engine = Engine(_cfg(), self.db, object(), [self.fx, self.tm], self.n,
                             price_oracle=FakePriceOracle({"ETH": 2232.85, "XAU": 2232.85, "EUR": 2232.85}))
        self.n._engine = self.engine
        await self.engine.start(monitor=False)
        await self.settle()
        self.client.sent.clear()
        self.client.edits.clear()
        return self

    async def down(self) -> None:
        await self.engine.stop()
        await self.n.stop()
        await self.db.close()

    async def settle(self) -> None:
        import asyncio
        loop = asyncio.get_running_loop()
        end = loop.time() + 5.0
        while True:
            busy = [t for t in (*self.engine._tasks, *self.engine._critical, *self.n._tasks)
                    if not t.done() and not t.get_name().startswith(("expire-", "retry-"))]
            if not busy:
                return
            assert loop.time() < end, busy
            await asyncio.sleep(0.01)

    async def open(self, asset: str = "ETH", names: tuple[str, ...] = ("fx", "tickmill"), **kw: Any) -> int:
        await self.engine.execute_open(_plan(asset, list(names), pid=f"p{asset}", **kw))
        await self.settle()
        return max(t.id for t in await self.db.active_trades())

    async def say(self, text: str) -> None:
        await self.n.handle_message(MsgEvent(text, sender_id=OWNER))
        await self.settle()

    async def press(self, data: str, msg_id: int, sender: int = OWNER) -> None:
        await self.n.handle_callback(CbEvent(data.encode(), sender_id=sender, message_id=msg_id))
        await self.settle()

    async def status(self, trade_id: int) -> dict[str, PositionStatus]:
        return {p.broker: p.status for p in await self.db.broker_positions(trade_id)}

    def texts(self) -> list[str]:
        return [m.text for m in self.client.to(OWNER)]


@pytest.fixture
async def world(tmp_path: Path):
    w = await World(tmp_path).up()
    yield w
    await w.down()


# --------------------------------------------------------------------------- #
# closing one account
# --------------------------------------------------------------------------- #
async def test_the_only_position_closes_at_once_and_the_trade_goes_on_on_the_other_account(world: World) -> None:
    tid = await world.open()
    await world.say("/pozkapat fx1")
    assert await world.status(tid) == {"fx": PositionStatus.CLOSED, "tickmill": PositionStatus.OPEN}
    trade = await world.db.get_trade(tid)
    assert trade is not None and trade.status is TradeStatus.OPEN
    (reply,) = [t for t in world.texts() if f"#{tid} ETH LONG (fx)" in t]
    assert "✅" in reply and "İşlem diğer hesaplarda sürüyor: tickmill (açık)." in reply
    assert [c for c, _ in world.tm.calls if c == "close_position"] == []

    # the trader's own close later: only what is still open is closed, and the trade ends with its summary
    plan = ManagementPlan(plan_id="tr", action=ActionType.CLOSE, trade_id=tid, close_fraction=1.0,
                          source_chat_id=CHAT, source_message_ids=[9], summary_tr="kapattık",
                          created_at=datetime.now(UTC))
    await world.engine.execute_management(plan)
    await world.settle()
    assert await world.status(tid) == {"fx": PositionStatus.CLOSED, "tickmill": PositionStatus.CLOSED}
    assert len([c for c, _ in world.fx.calls if c == "close_position"]) == 1     # never closed twice
    trade = await world.db.get_trade(tid)
    assert trade is not None and trade.status not in (TradeStatus.OPEN, TradeStatus.PENDING)


async def test_closing_the_last_open_account_ends_the_trade_with_its_summary(world: World) -> None:
    tid = await world.open()
    await world.say("/pozkapat tickmill")
    await world.say("/pozkapat fx")
    trade = await world.db.get_trade(tid)
    assert trade is not None and trade.status not in (TradeStatus.OPEN, TradeStatus.PENDING)
    last = [t for t in world.texts() if f"#{tid} ETH LONG (fx)" in t][-1]
    assert "sürüyor" not in last
    assert f"🏁 #{tid} ETH LONG" in last                         # the trade's closing summary comes with it


async def test_two_accounts_in_one_command(world: World) -> None:
    tid = await world.open()
    await world.say("/pozkapat fx1, tickmill")
    assert await world.status(tid) == {"fx": PositionStatus.CLOSED, "tickmill": PositionStatus.CLOSED}


async def test_a_pending_entry_is_cancelled(world: World) -> None:
    tid = await world.open()
    pos = next(p for p in await world.db.broker_positions(tid) if p.broker == "fx")
    await world.db.save_broker_position(tid, pos.model_copy(update={"status": PositionStatus.PENDING}))
    await world.say("/pozkapat fx1")
    assert (await world.status(tid))["fx"] is PositionStatus.CANCELLED
    assert [c for c, _ in world.fx.calls if c == "cancel_entry"]


# --------------------------------------------------------------------------- #
# several positions: the owner chooses
# --------------------------------------------------------------------------- #
async def test_several_positions_are_asked_and_only_the_chosen_one_closes(world: World) -> None:
    eth = await world.open("ETH")
    xau = await world.open("XAU")
    await world.say("/pozkapat fx1")
    (question,) = [m for m in world.client.to(OWNER) if m.buttons]
    data = [b.type.data for row in question.buttons for b in row]
    assert data == [f"pk:{eth}".encode(), f"pk:{xau}".encode(), b"pk:all", b"pk:x"]
    assert (await world.status(eth))["fx"] is PositionStatus.OPEN          # nothing closed before the choice

    await world.press(f"pk:{xau}", question.id)
    assert (await world.status(xau))["fx"] is PositionStatus.CLOSED
    assert (await world.status(eth))["fx"] is PositionStatus.OPEN
    assert (await world.status(xau))["tickmill"] is PositionStatus.OPEN
    # one use only
    await world.press(f"pk:{eth}", question.id)
    assert (await world.status(eth))["fx"] is PositionStatus.OPEN


async def test_hepsini_kapat_closes_every_offered_position_of_that_account_only(world: World) -> None:
    eth = await world.open("ETH")
    xau = await world.open("XAU")
    await world.say("/pozkapat fx1")
    (question,) = [m for m in world.client.to(OWNER) if m.buttons]
    await world.press("pk:all", question.id)
    for tid in (eth, xau):
        assert await world.status(tid) == {"fx": PositionStatus.CLOSED, "tickmill": PositionStatus.OPEN}


async def test_vazgec_and_an_expired_question_close_nothing(world: World, monkeypatch: pytest.MonkeyPatch) -> None:
    eth = await world.open("ETH")
    await world.open("XAU")
    await world.say("/pozkapat fx1")
    (question,) = [m for m in world.client.to(OWNER) if m.buttons]
    await world.press("pk:x", question.id)
    await world.press(f"pk:{eth}", question.id)
    assert (await world.status(eth))["fx"] is PositionStatus.OPEN
    assert any("Vazgeçildi" in e.text for e in world.client.edits_of(OWNER))

    world.client.sent.clear()
    await world.say("/pozkapat fx1")
    (question,) = [m for m in world.client.to(OWNER) if m.buttons]
    real = time.monotonic
    monkeypatch.setattr(notifier_mod.time, "monotonic", lambda: real() + notifier_mod.POZKAPAT_TTL_SEC + 1)
    await world.press(f"pk:{eth}", question.id)
    assert (await world.status(eth))["fx"] is PositionStatus.OPEN
    assert any("Süresi doldu" in e.text for e in world.client.edits_of(OWNER))


async def test_a_question_from_before_a_restart_closes_nothing(world: World) -> None:
    eth = await world.open("ETH")
    await world.open("XAU")
    await world.say("/pozkapat fx1")
    (question,) = [m for m in world.client.to(OWNER) if m.buttons]
    world.n._pk_prompts.clear()                                     # (what a restart leaves: nothing in memory)
    await world.press(f"pk:{eth}", question.id)
    assert (await world.status(eth))["fx"] is PositionStatus.OPEN


async def test_the_friend_can_never_press_it(world: World) -> None:
    await world.say(f"/arkadas {FRIEND}")
    eth = await world.open("ETH")
    await world.open("XAU")
    await world.say("/pozkapat fx1")
    (question,) = [m for m in world.client.to(OWNER) if m.buttons]
    await world.press(f"pk:{eth}", question.id, sender=FRIEND)
    assert (await world.status(eth))["fx"] is PositionStatus.OPEN
    assert not any(m.buttons for m in world.client.to(FRIEND))


# --------------------------------------------------------------------------- #
# filters and refusals
# --------------------------------------------------------------------------- #
async def test_an_asset_filter_closes_only_that_asset(world: World) -> None:
    eth = await world.open("ETH")
    xau = await world.open("XAU")
    await world.say("/pozkapat fx1 altın")
    assert (await world.status(xau))["fx"] is PositionStatus.CLOSED
    assert (await world.status(eth))["fx"] is PositionStatus.OPEN
    await world.say(f"/pozkapat fx1 #{eth}")
    assert (await world.status(eth))["fx"] is PositionStatus.CLOSED


async def test_a_currency_pair_filter_is_the_pair_not_the_currency(world: World) -> None:
    eur = await world.open("EUR", quote="USD")
    await world.say("/pozkapat fx1 eurgbp")
    assert (await world.status(eur))["fx"] is PositionStatus.OPEN
    assert any("kapatılacak açık pozisyon yok" in t for t in world.texts())
    await world.say("/pozkapat fx1 eurusd")
    assert (await world.status(eur))["fx"] is PositionStatus.CLOSED


@pytest.mark.parametrize("command, expect", [
    ("/pozkapat fx1,fx3", "Bilinmeyen hesap: <b>fx3</b>"),
    ("/pozkapat breakout", "elle işlem hesabı"),
    ("/pozkapat", "Kullanım"),
    ("/pozkapat fx1 ???", "anlaşılamadı"),
    ("/pozkapat fx 1", "işlem numarası için <code>#1</code> yazın"),       # ("fx 1": never trade #1)
    ("/pozkapat fx 7 eth", "anlaşılamadı"),
])
async def test_a_wrong_command_closes_nothing_at_all(world: World, command: str, expect: str,
                                                     monkeypatch: pytest.MonkeyPatch) -> None:
    breakout = FakeBroker("breakout")
    monkeypatch.setattr(breakout.config, "type", "manual", raising=False)
    world.engine.configured_brokers.append(breakout)
    tid = await world.open()
    await world.say(command)
    assert await world.status(tid) == {"fx": PositionStatus.OPEN, "tickmill": PositionStatus.OPEN}
    assert any(expect in t for t in world.texts()), world.texts()


async def test_no_position_on_the_account_is_said(world: World) -> None:
    await world.open(names=("tickmill",))
    await world.say("/pozkapat fx1")
    assert any("<b>fx</b>: kapatılacak açık pozisyon yok" in t for t in world.texts())


# --------------------------------------------------------------------------- #
# config
# --------------------------------------------------------------------------- #
def _app(brokers: list[dict[str, Any]]) -> AppConfig:
    return AppConfig.model_validate({"telegram": {"api_id": 1, "api_hash": "h", "source_chats": [-1001]},
                                     "notifier": {"bot_token": "1:a", "owner_user_id": 1}, "brokers": brokers})


def test_an_alias_can_never_name_another_account() -> None:
    fx = {"name": "fx", "type": "paper", "aliases": ["fx1"]}
    assert _app([fx, {"name": "tickmill", "type": "paper"}]).brokers[0].aliases == ["fx1"]
    with pytest.raises(ValueError, match="'tickmill' hesabında da kullanılıyor"):
        _app([dict(fx, aliases=["TICKMILL"]), {"name": "tickmill", "type": "paper"}])
    with pytest.raises(ValueError, match="geçersiz ad"):
        _app([dict(fx, aliases=["fx 1"])])


def test_an_unrestricted_plan_is_unchanged() -> None:
    plan = ManagementPlan(plan_id="a", action=ActionType.CLOSE, trade_id=1, source_chat_id=1,
                          created_at=datetime.now(UTC))
    assert plan.brokers == []
    # a plan stored before this field existed loads as before
    data = plan.model_dump(mode="json")
    del data["brokers"]
    assert ManagementPlan.model_validate(data).brokers == []


# --------------------------------------------------------------------------- #
# opens still in flight (a broker that has not answered the trade's open yet)
# --------------------------------------------------------------------------- #
async def test_another_accounts_open_in_flight_is_never_marked_to_close(world: World) -> None:
    import asyncio

    from copytrader.engine.orchestrator import _LateLeg
    tid = await world.open(names=("fx",))
    never = asyncio.get_running_loop().create_future()
    task = asyncio.ensure_future(never)
    leg = _LateLeg(trade_id=tid, broker="tickmill", label=f"#{tid} ETH LONG", task=task)
    world.engine._late_legs[(tid, "tickmill")] = leg
    try:
        # fx is the trade's only position: closing it would end the trade and close tickmill's open on arrival
        await world.say("/pozkapat fx1")
        assert (await world.status(tid))["fx"] is PositionStatus.OPEN
        assert leg.close_on_arrival is None and leg.missed_plans == [] and leg.missed == []
        assert any("tickmill hesabının açılış emri henüz yanıtlanmadı" in t and "fx hesabında hiçbir pozisyon kapatılmadı" in t
                   for t in world.texts())
        trade = await world.db.get_trade(tid)
        assert trade is not None and trade.status is TradeStatus.OPEN
        # the account in flight itself: its open is closed on arrival, for the owner's reason; fx stays open
        await world.say("/pozkapat tickmill")
        assert leg.close_on_arrival == "bu hesap için /pozkapat yazdınız"
        assert (await world.status(tid))["fx"] is PositionStatus.OPEN
        assert any("İşlem diğer hesaplarda sürüyor: fx (açık)." in t for t in world.texts())
        # tickmill's open is now marked to close: closing fx (the last position) is no longer refused
        await world.say("/pozkapat fx1")
        assert (await world.status(tid))["fx"] is PositionStatus.CLOSED
    finally:
        world.engine._late_legs.pop((tid, "tickmill"), None)
        task.cancel()



# --------------------------------------------------------------------------- #
# review round 1: the cases found
# --------------------------------------------------------------------------- #
async def test_two_accounts_of_a_trade_are_judged_together_when_a_third_open_is_in_flight(world: World) -> None:
    import asyncio

    from copytrader.engine.orchestrator import _LateLeg
    tid = await world.open()                                       # fx + tickmill open
    task = asyncio.ensure_future(asyncio.get_running_loop().create_future())
    leg = _LateLeg(trade_id=tid, broker="fx2", label=f"#{tid} ETH LONG", task=task)
    world.engine._late_legs[(tid, "fx2")] = leg
    try:
        await world.say("/pozkapat fx1,tickmill")
        assert await world.status(tid) == {"fx": PositionStatus.OPEN, "tickmill": PositionStatus.OPEN}
        assert leg.close_on_arrival is None
        (reply,) = [t for t in world.texts() if "henüz yanıtlanmadı" in t]
        assert "(fx ve tickmill)" in reply and "fx ve tickmill hesabında hiçbir pozisyon kapatılmadı" in reply
        assert not any("✅" in t for t in world.texts())
    finally:
        world.engine._late_legs.pop((tid, "fx2"), None)
        task.cancel()


async def test_a_position_whose_close_is_owed_does_not_count_as_going_on(world: World) -> None:
    import asyncio

    from copytrader.brokers.base import BrokerError
    from copytrader.engine.monitor import close_wanted
    from copytrader.engine.orchestrator import _LateLeg
    tid = await world.open()
    world.tm.fail = {"close_position": BrokerError("market closed (test)")}
    await world.say("/pozkapat tickmill")                          # owed: the monitor tries it again
    world.tm.fail = {}
    tm = next(p for p in await world.db.broker_positions(tid) if p.broker == "tickmill")
    assert close_wanted(tm) is not None
    fx = next(p for p in await world.db.broker_positions(tid) if p.broker == "fx")
    assert close_wanted(fx) is None                                 # never widened to the other account
    task = asyncio.ensure_future(asyncio.get_running_loop().create_future())
    world.engine._late_legs[(tid, "fx2")] = _LateLeg(trade_id=tid, broker="fx2", label="x", task=task)
    try:
        world.client.sent.clear()
        await world.say("/pozkapat fx1")       # tickmill is ending: fx is the last one, fx2's open still in flight
        assert (await world.status(tid))["fx"] is PositionStatus.OPEN
        assert any("fx2 hesabının açılış emri henüz yanıtlanmadı" in t for t in world.texts())
    finally:
        world.engine._late_legs.pop((tid, "fx2"), None)
        task.cancel()


async def test_an_open_with_an_unknown_outcome_is_named_not_called_no_position(world: World) -> None:
    tid = await world.open(names=("fx",))
    await world.db.update_trade(tid, note="tickmill: unknown")
    await world.say("/pozkapat tickmill")
    (reply,) = [t for t in world.texts() if "tickmill" in t]
    assert not any(m.buttons for m in world.client.to(OWNER))         # nothing to choose from
    assert "açılış emrinin sonucu bilinmiyordu" in reply and "elle kapatın" in reply
    assert "kapatılacak açık pozisyon yok" not in reply


async def test_shutting_down_closes_nothing_and_says_so(world: World) -> None:
    tid = await world.open()
    world.engine._accepting = False
    try:
        await world.say("/pozkapat fx1,tickmill")
    finally:
        world.engine._accepting = True
    assert await world.status(tid) == {"fx": PositionStatus.OPEN, "tickmill": PositionStatus.OPEN}
    assert any("Hiçbir pozisyon kapatılmadı" in t for t in world.texts())


async def test_vazgec_while_the_chosen_close_runs_changes_nothing(world: World) -> None:
    eth = await world.open("ETH")
    await world.open("XAU")
    await world.say("/pozkapat fx1")
    (question,) = [m for m in world.client.to(OWNER) if m.buttons]
    world.n._pk_running.add(question.id)                           # (a chosen close is being carried out)
    try:
        await world.press("pk:x", question.id)
        await world.press(f"pk:{eth}", question.id)
    finally:
        world.n._pk_running.discard(question.id)
    assert not any("Vazgeçildi" in e.text for e in world.client.edits_of(OWNER))
    assert (await world.status(eth))["fx"] is PositionStatus.OPEN
    assert question.id in world.n._pk_prompts                       # still answerable once that close is done


async def test_a_restricted_stop_move_never_changes_the_whole_trade(world: World) -> None:
    tid = await world.open()
    plan = ManagementPlan(plan_id="sl", action=ActionType.MOVE_SL, trade_id=tid, new_stop_loss=2210.0,
                          source_chat_id=CHAT, created_at=datetime.now(UTC), brokers=["fx"])
    await world.engine.execute_management(plan, notify=False)
    by = {p.broker: p for p in await world.db.broker_positions(tid)}
    assert by["fx"].stop_loss == 2210.0 and by["tickmill"].stop_loss != 2210.0
    trade = await world.db.get_trade(tid)
    assert trade is not None and trade.current_stop_loss == 2200.0


async def test_the_result_is_written_before_other_presses_are_taken_again(world: World,
                                                                          monkeypatch: pytest.MonkeyPatch) -> None:
    eth = await world.open("ETH")
    await world.open("XAU")
    await world.say("/pozkapat fx1")
    (question,) = [m for m in world.client.to(OWNER) if m.buttons]
    seen: list[bool] = []
    real_edit = world.n._edit

    async def edit(msg_id: int, text: str, **kw: Any) -> bool:
        if msg_id == question.id and f"#{eth} ETH LONG (fx)" in text:     # the final result
            seen.append(msg_id in world.n._pk_running)
        return await real_edit(msg_id, text, **kw)

    monkeypatch.setattr(world.n, "_edit", edit)
    await world.press(f"pk:{eth}", question.id)
    assert seen == [True] and question.id not in world.n._pk_running
