"""The owner's friend (/arkadas) uses his OWN commands in his chat with the bot, for his own account only (tickmill):

  /yardim, /pozisyonlar, /bakiye, /pozkapat [varlık | #işlem | hepsi], /karne [hepsi]

Never the owner's accounts (fx, binance, ...), never the owner's commands; what he closes is told to the owner too.
His /pozkapat lists his open positions with numbers; he answers with the numbers ("1-3" = 1 and 3).

REAL Engine and REAL BotNotifier (tests/test_pozkapat.py's world); brokers and Telegram are fakes.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from test_friend_mirror import FRIEND, OWNER
from test_notifier import MsgEvent
from test_pozkapat import World as _World

from copytrader.models import PositionStatus


class World(_World):
    async def up(self) -> World:
        await super().up()
        await self.say(f"/arkadas {FRIEND}")
        await self.friend_settle()
        self.client.sent.clear()
        return self

    async def friend_settle(self) -> None:
        await self.settle()
        for _ in range(200):
            pump = getattr(self.n, "_friend_pump", None)
            if not self.n._friend_queue and (pump is None or pump.done()):
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.02)

    async def open(self, *args, **kw):  # type: ignore[override]
        """As the base, then the friend's own feed of that open is waited for and set aside."""
        tid = await super().open(*args, **kw)
        await self.friend_settle()
        self.feed = [m.text for m in self.client.to(FRIEND)]
        self.client.sent[:] = [m for m in self.client.sent if m.entity != FRIEND]
        return tid

    async def friend_says(self, text: str) -> None:
        await self.n.handle_message(MsgEvent(text, sender_id=FRIEND))
        await self.friend_settle()

    def to_friend(self) -> list[str]:
        return [m.text for m in self.client.to(FRIEND)]


@pytest.fixture
async def world(tmp_path: Path):
    w = await World(tmp_path).up()
    yield w
    await w.down()


async def test_help_names_his_account_only(world: World) -> None:
    await world.friend_says("/yardim")
    (reply,) = world.to_friend()
    assert "yalnızca kendi hesabınız: tickmill" in reply and "/pozkapat" in reply and "fx" not in reply


async def test_positions_and_balance_show_his_account_only(world: World) -> None:
    await world.open()                                              # fx + tickmill
    await world.friend_says("/pozisyonlar")
    await world.friend_says("/bakiye")
    positions, balance = world.to_friend()
    assert "• tickmill:" in positions and "• fx:" not in positions
    assert "• tickmill:" in balance and "• fx:" not in balance


async def test_he_picks_his_position_from_his_list_the_owners_stays_and_the_owner_is_told(world: World) -> None:
    tid = await world.open()
    await world.friend_says("/pozkapat")
    (listing,) = world.to_friend()
    assert f"1- #{tid} ETH LONG (açık) · tickmill" in listing and "· fx" not in listing
    assert await world.status(tid) == {"fx": PositionStatus.OPEN, "tickmill": PositionStatus.OPEN}
    await world.friend_says("1")
    assert await world.status(tid) == {"fx": PositionStatus.OPEN, "tickmill": PositionStatus.CLOSED}
    reply = world.to_friend()[-1]
    assert f"#{tid} ETH LONG (tickmill)" in reply and "fx" not in reply          # nothing of the owner's accounts
    assert any(t.startswith("👤 Arkadaşınız /pozkapat listesinden <code>1</code> seçti:") for t in world.texts())
    # a shortcut naming his only position closes it at once
    other = await world.open("XAU")
    await world.friend_says("/pozkapat xau")
    assert (await world.status(other))["tickmill"] is PositionStatus.CLOSED


async def test_he_can_never_close_the_owners_account(world: World) -> None:
    tid = await world.open()
    for command in ("/pozkapat fx1", "/pozkapat fx", "/pozkapat tickmill,fx", "/pozkapat fx hepsi"):
        await world.friend_says(command)
    assert await world.status(tid) == {"fx": PositionStatus.OPEN, "tickmill": PositionStatus.OPEN}
    assert all("Hiçbir pozisyon kapatılmadı" in t for t in world.to_friend())
    assert not any("Arkadaşınız" in t for t in world.texts())


async def test_several_positions_are_listed_and_he_names_one_or_all(world: World) -> None:
    eth = await world.open("ETH")
    xau = await world.open("XAU")
    await world.friend_says("/pozkapat")
    (reply,) = world.to_friend()
    assert "1- #" in reply and "2- #" in reply and "henüz hiçbir pozisyon kapatılmadı" in reply
    assert not any(m.buttons for m in world.client.to(FRIEND))
    assert (await world.status(eth))["tickmill"] is PositionStatus.OPEN
    await world.friend_says("/pozkapat xau")
    assert (await world.status(xau))["tickmill"] is PositionStatus.CLOSED
    assert (await world.status(eth))["tickmill"] is PositionStatus.OPEN
    await world.friend_says("/pozkapat hepsi")
    assert (await world.status(eth))["tickmill"] is PositionStatus.CLOSED
    assert (await world.status(eth))["fx"] is PositionStatus.OPEN and (await world.status(xau))["fx"] is PositionStatus.OPEN


async def test_the_owners_commands_stay_the_owners(world: World) -> None:
    tid = await world.open()
    for command in ("/durum", "/hepsini_kapat", "/arkadas sil", "/test eth long", "merhaba"):
        await world.friend_says(command)
    assert await world.status(tid) == {"fx": PositionStatus.OPEN, "tickmill": PositionStatus.OPEN}
    assert set(world.to_friend()) == {"Bu komutu yalnızca bot sahibi kullanabilir. Sizin komutlarınız için /yardim yazın."}
    assert world.texts() == []                                     # nothing ran for the owner


async def test_the_owner_closes_every_position_of_an_account_with_hepsi(world: World) -> None:
    eth = await world.open("ETH")
    xau = await world.open("XAU")
    await world.say("/pozkapat fx1 hepsi")
    assert not any(m.buttons for m in world.client.to(OWNER))
    for tid in (eth, xau):
        assert await world.status(tid) == {"fx": PositionStatus.CLOSED, "tickmill": PositionStatus.OPEN}
    await world.say("/pozkapat fx1 xau hepsi")
    assert any("tek başına yazılır" in t for t in world.texts())


async def test_his_close_that_ends_the_trade_tells_him_nothing_of_the_owners_and_the_owner_gets_the_summary(
        world: World) -> None:
    world.engine.config.notifier.notify_on_execution = True          # (as on the server)
    tid = await world.open()
    await world.say("/pozkapat fx1")                                 # the owner's account is done first
    world.client.sent.clear()
    await world.friend_says("/pozkapat")
    await world.friend_says("1")
    assert await world.status(tid) == {"fx": PositionStatus.CLOSED, "tickmill": PositionStatus.CLOSED}
    reply = world.to_friend()[-1]
    assert "tickmill" in reply and "fx" not in reply and "🏁" not in reply
    assert any(f"🏁 #{tid} ETH LONG" in t for t in world.texts())          # the trade's summary: the owner's


async def test_his_positions_list_leaves_out_trades_he_is_out_of(world: World) -> None:
    tid = await world.open()
    await world.friend_says("/pozkapat")
    await world.friend_says("1")
    world.client.sent.clear()
    await world.friend_says("/pozisyonlar")
    (reply,) = world.to_friend()
    assert reply == "📂 Açık işlem yok." and (await world.status(tid))["fx"] is PositionStatus.OPEN


async def test_an_empty_account_list_is_no_command(world: World) -> None:
    tid = await world.open()
    await world.say("/pozkapat ,")
    assert any(t.startswith("Hangi hesap?") for t in world.texts())
    await world.friend_says("/pozkapat xau 7")
    assert "Hesaplar: tickmill" in world.to_friend()[-1] and "fx" not in world.to_friend()[-1]
    assert await world.status(tid) == {"fx": PositionStatus.OPEN, "tickmill": PositionStatus.OPEN}


# --------------------------------------------------------------------------- #
# his own feed: what happens to his account, nothing of the owner's
# --------------------------------------------------------------------------- #
async def test_his_feed_tells_his_open_stop_move_and_close_and_nothing_of_the_owners(world: World) -> None:
    from datetime import UTC, datetime

    from copytrader.models import ActionType, ManagementPlan
    tid = await world.open()
    assert world.feed == [(f"✅ #{tid} ETH LONG tickmill hesabınızda açıldı: 6.079 ETH/USDT:USDT, giriş ~2232.9, "
                           "stop 2200, hedef 2300")]
    move = ManagementPlan(plan_id="m", action=ActionType.MOVE_SL, trade_id=tid, new_stop_loss=2210.0,
                          source_chat_id=-1001234567890, source_message_ids=[7], summary_tr="stop 2210",
                          created_at=datetime.now(UTC))
    await world.engine.execute_management(move)
    close = ManagementPlan(plan_id="c", action=ActionType.CLOSE, trade_id=tid, close_fraction=1.0,
                           source_chat_id=-1001234567890, source_message_ids=[8], summary_tr="kapattık",
                           created_at=datetime.now(UTC))
    await world.engine.execute_management(close)
    await world.friend_settle()
    texts = world.to_friend()
    assert texts == [f"🔧 #{tid} ETH LONG tickmill: stop 2200 → 2210",
                     f"🏁 #{tid} ETH LONG tickmill hesabınızda kapandı. K/Z +0.00 USDT (+0.0R)"]
    assert not any("fx" in t for t in [*texts, *world.feed])


async def test_the_owners_messages_never_reach_him(world: World) -> None:
    await world.open()
    for command in ("/bakiye", "/pozisyonlar", "/yardim", "/durum", "/karne"):
        await world.say(command)
    await world.n.notify("fx bakiyesi 9973 USD")
    await world.friend_settle()
    assert world.to_friend() == []


def test_a_save_that_changes_nothing_for_him_says_nothing() -> None:
    from copytrader.engine.orchestrator import friend_position_events
    from copytrader.models import BrokerPosition, Side
    pos = BrokerPosition(broker="tickmill", broker_symbol="ETHUSD", side=Side.LONG, status=PositionStatus.OPEN,
                         quantity=1.0, filled_quantity=1.0, remaining_quantity=1.0, avg_entry_price=2000.0,
                         stop_loss=1900.0)
    assert friend_position_events(pos, pos.model_copy(update={"data": {"synced": 1}})) == []
    assert friend_position_events(pos, pos.model_copy(update={"remaining_quantity": 0.5})) == ["partial"]
    assert friend_position_events(pos.model_copy(update={"status": PositionStatus.PENDING}),
                                  pos.model_copy(update={"status": PositionStatus.CANCELLED})) == ["cancelled"]
    assert friend_position_events(None, pos) == ["opened"]


# --------------------------------------------------------------------------- #
# review round 2: refusals told to him, no fake stop moves, no owner account in his close refusal
# --------------------------------------------------------------------------- #
async def test_an_open_his_account_refused_is_told_to_him_with_its_reason(world: World) -> None:
    from copytrader.brokers.base import BrokerError
    world.tm.fail = {"open_position": BrokerError("Yetersiz teminat: 2.36 lot için 177.23 USD gerekiyor (test)")}
    tid = await world.open()
    assert any(f"⚠️ #{tid} ETH LONG tickmill hesabınızda açılmadı:" in t and "Yetersiz teminat" in t
               for t in world.feed), world.feed
    assert not any("fx" in t for t in world.feed)


async def test_his_close_refused_for_an_open_in_flight_names_no_other_account(world: World) -> None:
    import asyncio

    from copytrader.engine.orchestrator import _LateLeg
    tid = await world.open(names=("tickmill",))
    task = asyncio.ensure_future(asyncio.get_running_loop().create_future())
    world.engine._late_legs[(tid, "fx")] = _LateLeg(trade_id=tid, broker="fx", label="x", task=task)
    try:
        await world.friend_says("/pozkapat xau eth")
        reply = world.to_friend()[-1]
        assert "başka bir hesaptaki açılış emri henüz yanıtlanmadı" in reply and "fx" not in reply
        assert (await world.status(tid))["tickmill"] is PositionStatus.OPEN
    finally:
        world.engine._late_legs.pop((tid, "fx"), None)
        task.cancel()


def test_a_fill_tells_its_levels_once_never_as_a_stop_move() -> None:
    from types import SimpleNamespace

    from copytrader.engine.orchestrator import (
        OPEN_UNKNOWN,
        friend_position_events,
        friend_position_text,
    )
    from copytrader.models import BrokerPosition, Side, TakeProfit
    waiting = BrokerPosition(broker="tickmill", broker_symbol="BTCUSD", side=Side.LONG, status=PositionStatus.PENDING,
                             quantity=0.1, filled_quantity=0.0, remaining_quantity=0.0, stop_loss=2200.0,
                             take_profits=[TakeProfit(price=2300.0)])
    filled = waiting.model_copy(update={"status": PositionStatus.OPEN, "filled_quantity": 0.1,
                                        "remaining_quantity": 0.1, "avg_entry_price": 2251.5, "stop_loss": 2201.5,
                                        "take_profits": [TakeProfit(price=2301.5)]})
    assert friend_position_events(waiting, filled) == ["filled"]
    trade = SimpleNamespace(id=7, plan=SimpleNamespace(base_asset="BTC", side=Side.LONG, risk_r=1.0))
    assert friend_position_text(trade, waiting, filled, ["filled"]) == (       # type: ignore[arg-type]
        "✅ #7 BTC LONG tickmill: giriş emri doldu, giriş ~2251.5, stop 2201.5, hedef 2301.5")
    unknown = waiting.model_copy(update={"status": PositionStatus.FAILED, "error": "error",
                                         "data": {OPEN_UNKNOWN: True}})
    text = friend_position_text(trade, waiting, unknown, friend_position_events(waiting, unknown))  # type: ignore[arg-type]
    assert "sonucu bilinmiyor" in text and "açılamadı" not in text
    broken = filled.model_copy(update={"status": PositionStatus.FAILED, "error": "stop yok"})
    assert friend_position_events(filled, broken) == ["broken"]
    assert "hata durumunda" in friend_position_text(trade, filled, broken, ["broken"])  # type: ignore[arg-type]
