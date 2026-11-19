"""The friend's trade card (/tickmillkart): the owner's /<hesap>kart for the friend's own account, in his own chat.

REAL BotNotifier, REAL TradeCardService on a real database; Telegram, the drawing process and the MetaTrader
terminal (directly, or the mt5_bridge adapter with its helper's answer) are stand-ins.

  * the friend's list holds this week's trades of HIS account only, numbered; a number sends the card to his chat
  * nothing he reads (lists, answers, refusals, the caption, the card's data) names another account
  * he still cannot use any other account's card command (the refusal as before)
  * his list and the owner's lists (cards, /pozkapat) are kept apart, per chat
  * the owner may use /tickmillkart in his own chat; the card goes to the chat that asked
  * the MetaTrader read is the same as fx2's (through the bridge, never while the trading path is busy)
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import tradecard_samples as S
from test_group_mirror import Bot
from test_notifier import FakeEngine, MsgEvent
from test_tradecard import NOW, Clock, Renderer, add_trade

from copytrader import app as app_mod
from copytrader.brokers.mt5_bridge import Mt5BridgeBroker
from copytrader.config import AppConfig, NotifierConfig, TelegramConfig
from copytrader.karne import tradecard_service as svc_mod
from copytrader.karne.tradecard_service import TradeCardService
from copytrader.models import BrokerPosition, PositionStatus, Side, TradeStatus
from copytrader.storage.db import Database
from copytrader.telegram import notifier as notifier_mod
from copytrader.telegram.notifier import BotNotifier

OWNER, FRIEND, NEW_FRIEND = 777, 555, 556
OWNER_ACCOUNTS = ["binance", "fx", "fx2", "breakout"]
# words of the owner's accounts that must never reach the friend
FORBIDDEN = ("binance", "fx2", "/fxkart", "breakout", "fundingpips", "bem", "1r:")
OFFSET = 7200


class Terminal:
    """The tickmill terminal's read-only ``trade_chart`` (the answer of the bridge's helper process)."""

    def __init__(self, answer: dict[str, Any]) -> None:
        self.answer = answer
        self.calls: list[tuple[Any, ...]] = []

    async def trade_chart(self, *args: Any) -> Any:
        self.calls.append(args)
        return self.answer


async def nas_on(db: Database, clock: Clock, broker: str, *, others: tuple[BrokerPosition, ...] = ()
                 ) -> tuple[int, dict[str, Any]]:
    """The NAS100 short sample (closed by the group, +1.70R) on ``broker`` (MetaTrader), with the terminal's
    answer for it."""
    sample = S.nas_short_result()
    pos = sample["pos"].model_copy(deep=True, update={"broker": broker})
    tid = await add_trade(db, clock, sample["trade"], pos, created=datetime(2026, 11, 17, 5, 51, tzinfo=UTC),
                          closed=datetime(2026, 11, 17, 9, 24, tzinfo=UTC), status=TradeStatus.CLOSED, others=others)
    clock.now = datetime(2026, 11, 17, 5, 51, 30, tzinfo=UTC)
    await db.log_event("order_send", {"trade_id": tid, "broker": broker})
    clock.now = datetime(2026, 11, 17, 7, 56, tzinfo=UTC)
    await db.log_event("entry_filled", {"trade_id": tid, "broker": broker})
    clock.now = datetime(2026, 11, 17, 9, 23, tzinfo=UTC)
    await db.log_event("manage_done", {"trade_id": tid, "action": "close",
                                       "outcomes": [{"broker": broker, "kind": "ok"}]})
    clock.now = NOW
    fill_srv = int(datetime(2026, 11, 17, 7, 55, tzinfo=UTC).timestamp()) + OFFSET
    out_srv = int(datetime(2026, 11, 17, 9, 23, tzinfo=UTC).timestamp()) + OFFSET
    answer = {"offset": OFFSET, "bars": [(b[0] + OFFSET, *b[1:]) for b in sample["bars"]],
              "deals": [{"time": fill_srv, "price": 27300.0, "volume": 1.0, "entry": "in", "reason": "manual"},
                        {"time": out_srv, "price": 27055.1, "volume": 1.0, "entry": "out", "reason": "manual"}]}
    return tid, answer


class World:
    async def up(self, tmp_path: Path) -> World:
        self.clock = Clock(NOW)
        self.db = Database(str(tmp_path / "bot.db"), clock=self.clock)
        await self.db.open()
        self.n = BotNotifier(NotifierConfig(bot_token="123:abc", owner_user_id=OWNER),
                             TelegramConfig(api_id=1, api_hash="x", source_chats=[-100]), str(tmp_path))
        self.bot = Bot()
        self.n._client = self.bot
        engine = FakeEngine()
        engine.notifier = self.n
        engine.closed = []

        async def close_on_accounts(pairs: Any, *, hide_others: bool = False) -> str:
            engine.closed.append(list(pairs))
            return "kapatıldı"

        engine.close_on_accounts = close_on_accounts
        engine.friend_account_names = lambda: ["tickmill"]
        self.engine = engine
        self.n._engine = engine
        self.n.set_friend(FRIEND)
        # the week: a tickmill trade that is also on fx2 (the same signal), a binance-only one, an fx2-only one
        fx2_twin = BrokerPosition(broker="fx2", broker_symbol="US100.cash", side=Side.SHORT,
                                  status=PositionStatus.CLOSED, quantity=1, filled_quantity=1,
                                  avg_entry_price=27300.0, realized_pnl=170.0)
        self.tick_id, answer = await nas_on(self.db, self.clock, "tickmill", others=(fx2_twin,))
        self.terminal = Terminal(answer)
        self.fx2_id, fx2_answer = await nas_on(self.db, self.clock, "fx2")
        self.fx2 = Terminal(fx2_answer)
        open_pos = BrokerPosition(broker="binance", broker_symbol="ETH/USDT:USDT", side=Side.LONG,
                                  status=PositionStatus.OPEN, quantity=1, filled_quantity=1, avg_entry_price=3100.0,
                                  stop_loss=3050.0)
        self.binance_id = await add_trade(self.db, self.clock, S.trade(0, S.plan("ETH", Side.LONG, 3100, 3050, [3200]),
                                                                       TradeStatus.OPEN, NOW), open_pos,
                                          created=datetime(2026, 11, 18, 9, 0, tzinfo=UTC))
        self.renderer = Renderer(tmp_path)
        self.mono = [1000.0]
        brokers = {"tickmill": self.terminal, "fx2": self.fx2}
        self.svc = TradeCardService(
            self.db, self.n, tmp_path, accounts=OWNER_ACCOUNTS, friend_accounts=["tickmill"],
            aliases={"fx": ["fx1", "FundingPips"]},
            sources={"binance": ("ccxt", "binanceusdm"), "fx": ("mt5", None), "fx2": ("mt5", None),
                     "breakout": ("ccxt", "binanceusdm"), "tickmill": ("mt5", None)},
            broker=lambda name: brokers.get(name), oracle=None, manual=["breakout"], busy=lambda: False,
            clock=self.clock, renderer=self.renderer, pillow=lambda: True, monotonic=lambda: self.mono[0],
            idle_wait_sec=0.3)
        self.n.set_tradecard(self.svc)
        return self

    async def idle(self) -> None:
        """The card's job done and every waiting message delivered."""
        job = self.svc._job
        if job is not None:
            await asyncio.wait_for(asyncio.gather(job, return_exceptions=True), 10)
        for _ in range(200):
            if not self.n._friend_queue and not self.n._queue:
                break
            await asyncio.sleep(0.01)
        await self.n.drain(2)
        await asyncio.sleep(0.02)

    def friend_texts(self) -> list[str]:
        """The friend's messages (not the photos)."""
        return [m.text or "" for m in self.bot.to(FRIEND) if not hasattr(m, "file")]

    def owner_texts(self) -> list[str]:
        return [m.text or "" for m in self.bot.to(OWNER) if not hasattr(m, "file")]

    async def friend(self, text: str) -> str:
        """The friend writes ``text``; the answer to it (his first new message)."""
        before = len(self.friend_texts())
        await self.n.handle_message(MsgEvent(text, sender_id=FRIEND))
        await self.idle()
        new = self.friend_texts()[before:]
        return new[0] if new else ""

    async def owner(self, text: str) -> str:
        before = len(self.owner_texts())
        await self.n.handle_message(MsgEvent(text, sender_id=OWNER))
        await self.idle()
        new = self.owner_texts()[before:]
        return new[0] if new else ""

    def assert_private(self) -> None:
        """Nothing the friend got (texts, captions) nor any card drawn for him names an account of the owner's."""
        seen = " ".join(m.text or "" for m in self.bot.to(FRIEND)).lower()
        for word in FORBIDDEN:
            assert word not in seen, (word, seen)
        assert re.search(r"\bfx\d?\b", seen) is None, seen

    async def down(self) -> None:
        await self.svc.stop()
        await self.n.drain(2)
        for task in list(self.n._send_tasks) + [t for t in (self.n._pump_task, self.n._friend_pump) if t is not None]:
            task.cancel()
        await self.db.close()


@pytest.fixture
async def world(tmp_path: Path):
    w = await World().up(tmp_path)
    yield w
    await w.down()


# --------------------------------------------------------------------------- #
# The friend's own card
# --------------------------------------------------------------------------- #
async def test_the_friends_list_holds_only_his_accounts_trades(world: World) -> None:
    reply = await world.friend("/tickmillkart")
    lines = [line for line in reply.splitlines() if line[:2] in ("1-", "2-", "3-")]
    assert lines == [line for line in lines if line.startswith("1-")] and len(lines) == 1, reply
    assert lines[0].startswith("1- NAS100 SHORT") and f"#{world.tick_id}" in lines[0] and "kapandı" in lines[0]
    assert f"#{world.fx2_id}" not in reply and f"#{world.binance_id}" not in reply
    assert "<b>tickmill</b>" in reply
    assert world.svc.list_live(friend=True) and not world.svc.list_live()       # his list, not the owner's
    assert world.owner_texts() == []                                             # nothing to the owner
    world.assert_private()


async def test_a_number_sends_his_card_to_his_chat_through_his_terminal(world: World) -> None:
    await world.friend("/tickmillkart")
    assert await world.friend("1") == svc_mod.STARTED
    await world.idle()
    (photo,) = world.bot.files
    assert photo.entity == FRIEND and photo.force_document is False             # a photo, in his chat
    assert photo.text == f"🃏 <b>NAS100 SHORT</b> · tickmill · #{world.tick_id} · Sonuç kartı"
    assert world.owner_texts() == []
    data = world.renderer.data[-1]
    assert data["account"] == "tickmill" and data["kind"] == "result" and data["hero"] == "+1.70R"
    assert data["subtitle"].startswith("US100.cash  ·  15 dakika")             # the signal's 15-minute chart
    blob = json.dumps(data, ensure_ascii=False).lower()
    for word in FORBIDDEN:
        assert word not in blob, word
    assert world.fx2.calls == [] and len(world.terminal.calls) == 1             # HIS terminal only
    symbol, tf, _start, _end, tickets, _anchor = world.terminal.calls[0]
    assert (symbol, tf, tickets) == ("US100.cash", 15, [501])
    assert not world.svc.list_live(friend=True)                                  # one use
    assert "liste" in (await world.friend("/tickmillkart 1")).lower()
    world.assert_private()


async def test_his_card_goes_through_the_bridge_adapter(world: World, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[Any] = []

    async def rpc(self: Any, method: str, *args: Any, timeout: float | None = None, **kw: Any) -> Any:
        seen.append((method, args))
        return world.terminal.answer

    monkeypatch.setattr(Mt5BridgeBroker, "_rpc", rpc)
    bridge = Mt5BridgeBroker.__new__(Mt5BridgeBroker)
    world.svc._broker = lambda name: bridge if name == "tickmill" else None
    await world.friend("/tickmillkart")
    await world.friend("1")
    await world.idle()
    assert [m for m, _a in seen] == ["trade_chart"] and seen[0][1][:2] == ("US100.cash", 15)
    assert world.bot.files[-1].entity == FRIEND and world.renderer.data[-1]["hero"] == "+1.70R"


async def test_his_read_waits_for_the_trading_path_like_fx2(world: World) -> None:
    world.svc._busy = lambda: True
    await world.friend("/tickmillkart")
    assert await world.friend("1") == svc_mod.STARTED
    await world.idle()
    assert world.friend_texts()[-1] == svc_mod.BUSY_TRADING                      # told to HIM, not to the owner
    assert world.terminal.calls == [] and world.bot.files == [] and world.owner_texts() == []
    world.assert_private()


async def test_his_failures_go_to_him_and_name_nothing_else(world: World) -> None:
    world.terminal.answer = {"offset": None, "bars": [], "deals": []}
    await world.friend("/tickmillkart")
    await world.friend("1")
    await world.idle()
    assert "saati belirlenemedi" in world.friend_texts()[-1] and world.owner_texts() == []
    reply = await world.friend("/tickmillkart 1")             # no list any more: his own command, never the owner's
    assert "/tickmillkart" in reply and "önce" in reply
    await world.friend("/tickmillkart")
    assert "listede yok" in await world.friend("7")
    assert "tek bir numara" in await world.friend("1-3")
    assert "Kullanım" in await world.friend("/tickmillkart dün")
    world.mono[0] += svc_mod.CARD_LIST_TTL_SEC + 1
    # expired (or used): a bare number is as with no list at all - the friend's usual refusal line (at most one a
    # minute), as before this feature
    assert await world.friend("1") == notifier_mod.FRIEND_REFUSED
    world.assert_private()


async def test_free_text_reasons_never_reach_the_friend(world: World) -> None:
    """A terminal's / Telegram's / the drawing's own text may name anything: the friend reads a fixed phrase."""
    async def broken_chart(*_args: Any) -> Any:
        raise RuntimeError("binance fx2 breakout FundingPips terminal error")

    world.terminal.trade_chart = broken_chart                             # type: ignore[method-assign]
    await world.friend("/tickmillkart")
    await world.friend("1")
    await world.idle()
    assert world.friend_texts()[-1] == svc_mod.FAILED.format(why=svc_mod.CANDLES_FAILED)
    world.assert_private()

    world.terminal.trade_chart = Terminal(world.terminal.answer).trade_chart   # type: ignore[method-assign]

    async def bad_drawing(data: dict[str, Any], folder: Any, **_kw: Any) -> Any:
        return svc_mod.RenderResult("error", detail="fx2 binance traceback")

    world.svc._renderer = bad_drawing
    await world.friend("/tickmillkart")
    await world.friend("1")
    await world.idle()
    assert world.friend_texts()[-1] == svc_mod.NO_PICTURE.format(why="çizim hatası")
    world.assert_private()

    world.svc._renderer = world.renderer

    async def refused(*_a: Any, **_kw: Any) -> str:
        return "binance fx2 reason"

    world.n.send_friend_photo = refused                                    # type: ignore[method-assign]
    await world.friend("/tickmillkart")
    await world.friend("1")
    await world.idle()
    assert world.friend_texts()[-1] == svc_mod.NOT_SENT.format(why=svc_mod.FRIEND_NOT_SENT)
    world.assert_private()

    async def broken_db(*_a: Any, **_kw: Any) -> Any:
        raise RuntimeError("binance fx2 database text")

    world.svc._db.account_trades = broken_db                               # type: ignore[method-assign]
    assert await world.friend("/tickmillkart") == svc_mod.FAILED.format(why=svc_mod.FRIEND_UNEXPECTED)
    world.assert_private()
    assert "binance fx2 database text" in await world.owner("/tickmillkart")   # (the owner reads the detail)


async def test_the_friend_still_cannot_use_the_owners_card_commands(world: World) -> None:
    for command in ("/binancekart", "/fxkart", "/fx1kart", "/fx2kart", "/breakoutkart", "/paperkart"):
        await world.n.handle_message(MsgEvent(command, sender_id=FRIEND))
    await world.idle()
    assert world.friend_texts() == [notifier_mod.FRIEND_REFUSED]                 # (one refusal line now and then)
    assert not world.svc.list_live(friend=True) and not world.svc.list_live()
    assert world.renderer.data == [] and world.owner_texts() == []


async def test_his_help_lists_his_card_command_only(world: World) -> None:
    reply = await world.friend("/yardim")
    assert "/tickmillkart" in reply and "kart" in reply
    world.assert_private()
    world.n.set_tradecard(None)
    assert "kart" not in await world.friend("/yardim")                          # no card service: no line
    assert "/tickmillkart" in notifier_mod.HELP_TEXT


# --------------------------------------------------------------------------- #
# Lists per chat
# --------------------------------------------------------------------------- #
async def test_the_owners_and_the_friends_lists_never_mix(world: World) -> None:
    await world.owner("/binancekart")
    assert world.svc.list_live() and not world.svc.list_live(friend=True)
    await world.n.handle_message(MsgEvent("1", sender_id=FRIEND))     # the owner's list is not his
    await world.idle()
    assert world.renderer.data == [] and world.svc.list_live()
    world.assert_private()
    await world.friend("/tickmillkart")
    assert world.svc.list_live() and world.svc.list_live(friend=True)
    assert await world.owner("1") == svc_mod.STARTED                    # the owner's number: his binance list
    await world.idle()
    assert world.svc.list_live(friend=True) and not world.svc.list_live()
    assert all(f.entity == OWNER for f in world.bot.files)
    assert await world.friend("1") == svc_mod.STARTED                   # the friend's number: his tickmill list
    await world.idle()
    assert world.bot.files[-1].entity == FRIEND and world.renderer.data[-1]["account"] == "tickmill"
    world.assert_private()


async def test_his_number_with_his_pozkapat_list_waiting_chooses_neither(world: World) -> None:
    await world.friend("/tickmillkart")
    world.n._pk_lists[FRIEND] = (time.monotonic(), [("tickmill", 9, "#9 XAU LONG (açık)")])
    reply = await world.friend("1")
    assert "/pozkapat 1" in reply and "/tickmillkart 1" in reply and "Hiçbir pozisyon kapatılmadı" in reply
    assert world.engine.closed == [] and world.renderer.data == []
    assert FRIEND in world.n._pk_lists and world.svc.list_live(friend=True)
    assert await world.friend("/tickmillkart 1") == svc_mod.STARTED       # explicit: the card
    await world.idle()
    assert world.bot.files[-1].entity == FRIEND and world.engine.closed == []
    world.n._pk_lists[FRIEND] = (time.monotonic() - notifier_mod.POZKAPAT_TTL_SEC - 1, [("tickmill", 9, "x")])
    await world.friend("/tickmillkart")
    assert await world.friend("1") == svc_mod.STARTED                     # an expired /pozkapat list does not count
    world.assert_private()


async def test_without_his_card_list_his_number_goes_to_his_pozkapat_as_before(world: World) -> None:
    world.n._pk_lists[FRIEND] = (time.monotonic(), [("tickmill", 9, "#9 XAU LONG (açık)")])
    await world.friend("1")
    assert world.engine.closed == [[("tickmill", 9)]] and world.renderer.data == []


async def test_the_owners_pozkapat_list_and_the_friends_card_list(world: World) -> None:
    world.n._pk_lists[OWNER] = (time.monotonic(), [("binance", 1, "#1 ETH LONG (açık)")])
    await world.friend("/tickmillkart")
    assert await world.owner("1") == "kapatıldı"                        # the owner's number: his /pozkapat list
    assert world.engine.closed == [[("binance", 1)]] and world.svc.list_live(friend=True)


async def test_a_new_friend_never_gets_the_earlier_friends_list_or_card(world: World) -> None:
    await world.friend("/tickmillkart")
    world.n.set_friend(NEW_FRIEND)
    assert not world.svc.list_live(friend=True)
    assert await world.n.send_friend_photo("x.png", chat=FRIEND) == "arkadaş sohbeti değişti"
    await world.n.tell_friend("eski", chat=FRIEND)
    assert not world.n._friend_queue


# --------------------------------------------------------------------------- #
# The owner
# --------------------------------------------------------------------------- #
async def test_the_owner_uses_tickmillkart_in_his_own_chat(world: World) -> None:
    reply = await world.owner("/tickmillkart")
    assert "1- NAS100 SHORT" in reply and f"#{world.tick_id}" in reply and f"#{world.fx2_id}" not in reply
    assert world.svc.list_live() and not world.svc.list_live(friend=True)
    assert await world.owner("1") == svc_mod.STARTED
    await world.idle()
    (photo,) = world.bot.files
    assert photo.entity == OWNER and "tickmill" in photo.text
    assert world.friend_texts() == []                                     # nothing to the friend


async def test_the_owners_cards_are_as_before(world: World) -> None:
    reply = await world.owner("/fx2kart")
    assert f"#{world.fx2_id}" in reply and f"#{world.tick_id}" in reply   # (the twin's fx2 position is fx2's)
    assert await world.owner("2") == svc_mod.STARTED
    await world.idle()
    assert world.bot.files[-1].entity == OWNER and world.renderer.data[-1]["account"] == "fx2"
    assert len(world.fx2.calls) == 1 and world.terminal.calls == []
    unknown = await world.owner("/nosuchkart")
    assert "yalnızca sizin hesaplarınız" in unknown and "/binancekart" in unknown and "/tickmillkart" in unknown
    assert world.friend_texts() == []


async def test_one_card_at_a_time_whoever_asked(world: World) -> None:
    gate = asyncio.Event()
    draw = world.renderer.__call__

    async def slow(data: dict[str, Any], folder: Any, **kw: Any) -> Any:
        await gate.wait()
        return await draw(data, folder, **kw)

    world.svc._renderer = slow
    await world.friend("/tickmillkart")
    await world.n.handle_message(MsgEvent("1", sender_id=FRIEND))
    await world.n.handle_message(MsgEvent("/fx2kart", sender_id=OWNER))
    await world.n.handle_message(MsgEvent("1", sender_id=OWNER))
    await world.n.drain(2)
    assert world.owner_texts()[-1] == svc_mod.BUSY and world.svc.list_live()   # the owner's list stays
    gate.set()
    await world.idle()
    assert [f.entity for f in world.bot.files] == [FRIEND]
    world.assert_private()


# --------------------------------------------------------------------------- #
# The app's wiring
# --------------------------------------------------------------------------- #
def _config(dry: bool = False, tickmill_type: str = "mt5_bridge") -> AppConfig:
    tickmill: dict[str, Any] = (dict(type="mt5_bridge", name="tickmill", optional=True,
                                     terminal_path="C:/b/terminal64.exe", broker_company="Tickmill")
                                if tickmill_type == "mt5_bridge" else
                                dict(type="metaapi", name="tickmill", optional=True, token="t", account_id="a"))
    return AppConfig(
        telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[-1]),
        notifier=NotifierConfig(bot_token="1:a", owner_user_id=OWNER), dry_run=dry,
        brokers=[dict(type="ccxt", name="binance", exchange="binanceusdm"),
                 dict(type="mt5_bridge", name="fx2", terminal_path="C:/a/terminal64.exe", friend_account=False,
                      broker_company="BEM"),
                 tickmill, dict(type="manual", name="breakout")])


def test_the_app_gives_the_friends_account_its_card_through_the_bridge() -> None:
    config = _config()
    assert app_mod.card_account_names(config) == ["binance", "fx2", "breakout"]          # (unchanged)
    assert app_mod.friend_card_account_names(config) == ["tickmill"]
    assert app_mod.friend_card_account_names(_config(dry=True)) == []
    service = app_mod.build_tradecard(config, None, object(), type("E", (), {"brokers": []})(), None, "/tmp")
    assert service.accounts == ["binance", "fx2", "breakout"] and service.friend_accounts == ["tickmill"]
    assert service._sources["tickmill"] == ("mt5", None)
    assert service.command_account("tickmillkart", friend=True) == "tickmill"
    assert service.command_account("TICKMILLKART") == "tickmill"                          # the owner too
    for owners in ("binancekart", "fx2kart", "breakoutkart"):
        assert service.command_account(owners, friend=True) is None
    assert service.commands_text(friend=True) == "/tickmillkart"


def test_a_metaapi_tickmill_has_no_candles_to_draw() -> None:
    service = app_mod.build_tradecard(_config(tickmill_type="metaapi"), None, object(),
                                      type("E", (), {"brokers": []})(), None, "/tmp")
    assert service.friend_accounts == ["tickmill"] and "tickmill" not in service._sources
