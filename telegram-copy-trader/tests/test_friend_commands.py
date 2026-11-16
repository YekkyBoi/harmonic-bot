"""The owner's friend (/arkadas) uses his OWN commands in his chat with the bot, for his own account only (tickmill):

  /yardim, /pozisyonlar, /bakiye, /pozkapat [varlık | #işlem | hepsi], /karne [hepsi]

Never the owner's accounts (fx, binance, ...), never the owner's commands; what he closes is told to the owner too.
His /pozkapat has no buttons: one matching position is closed at once, several are listed and he names one.

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


async def test_his_only_position_closes_at_once_the_owners_stays_and_the_owner_is_told(world: World) -> None:
    tid = await world.open()
    await world.friend_says("/pozkapat")
    assert await world.status(tid) == {"fx": PositionStatus.OPEN, "tickmill": PositionStatus.CLOSED}
    (reply,) = world.to_friend()
    assert f"#{tid} ETH LONG (tickmill)" in reply and "sürüyor: fx (açık)" in reply
    assert any(t.startswith("👤 Arkadaşınız <code>/pozkapat </code> yazdı:") for t in world.texts())


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
    assert "tickmill</b> hesabınızda 2 pozisyon var" in reply and "Hiçbir pozisyon kapatılmadı" in reply
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
