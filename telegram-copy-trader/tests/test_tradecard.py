"""The owner's trade card (/<hesap>kart): one picture of one of his trades on one account.

Part 1 (no Pillow): the command, owner only (never the friend, never the friend's account), the week's numbered list
and its pick (TTL, one use, never confused with /pozkapat's list or an awaited correction), the entry / result card
choice, the card's figures (the ORIGINAL stop, the close price, R, the candles after the close), the chart's bar size
(the parser's new field, 15 by default), the MetaTrader candles (fake terminal, also through the bridge's helper
process), the public candles, the subprocess renderer being used.
Part 2 (Pillow): real pictures (2560 x 1600) through the real drawing process, and the asset name's lining figures
with and without raqm. ``TRADECARD_OUT=<folder>`` keeps the pictures of the real-render test there.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import shutil
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import tradecard_samples as S
from ccxt.base import errors as cx
from test_ccxt_broker import make_broker
from test_mt5_broker import SERVER_TIME, FakeMT5, connected
from test_notifier import FakeBotClient, FakeEngine, MsgEvent

from copytrader import app as app_mod
from copytrader.brokers import mt5_broker, mt5_helper
from copytrader.brokers.mt5_bridge import Mt5BridgeBroker
from copytrader.brokers.mt5_broker import MT5, Mt5Broker
from copytrader.config import AppConfig, NotifierConfig, TelegramConfig
from copytrader.engine.prices import ReferencePriceOracle
from copytrader.karne import render as render_mod
from copytrader.karne import tradecard as tc
from copytrader.karne import tradecard_service as svc_mod
from copytrader.karne.render import OK, RenderResult, render_card
from copytrader.karne.tradecard_service import TradeCardService
from copytrader.models import (
    ActionType,
    BrokerPosition,
    EntryType,
    ParsedAction,
    PositionStatus,
    Side,
    TakeProfit,
    TradeStatus,
)
from copytrader.parsing import llm_parser, prompts
from copytrader.storage.db import Database
from copytrader.telegram import notifier as notifier_mod
from copytrader.telegram.notifier import BotNotifier

OWNER, FRIEND = 777, 555
NOW = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)               # a Wednesday; the week began Sun 15.11 22:00 UK
HAS_PILLOW = importlib.util.find_spec("PIL") is not None
needs_pillow = pytest.mark.skipif(not HAS_PILLOW, reason="Pillow is not installed (optional: the card's picture)")


# =========================================================================== #
# A database with one week of one account's trades
# =========================================================================== #
class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def ms(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


async def add_trade(db: Database, clock: Clock, sample_trade: Any, pos: BrokerPosition, *, created: datetime,
                    closed: datetime | None = None, status: TradeStatus = TradeStatus.OPEN,
                    others: tuple[BrokerPosition, ...] = ()) -> int:
    clock.now = created
    rec = await db.create_trade(sample_trade.plan, TradeStatus.OPENING)
    await db.update_trade(rec.id, status=TradeStatus.OPEN)
    for other in others:
        await db.save_broker_position(rec.id, other)
    if closed is not None:
        clock.now = closed
    await db.save_broker_position(rec.id, pos)
    if status is not TradeStatus.OPEN:
        await db.update_trade(rec.id, status=status)
    clock.now = NOW
    return rec.id


class Notes:
    """The notifier as the service sees it: the owner's lines and photos."""

    def __init__(self) -> None:
        self.lines: list[str] = []
        self.photos: list[tuple[str, str]] = []
        self.refuse: str | None = None

    async def tell_owner(self, text: str) -> None:
        self.lines.append(text)

    async def send_owner_photo(self, path: str, *, caption: str = "", busy: Any = None) -> str | None:
        self.photos.append((path, caption))
        return self.refuse


class Oracle:
    def __init__(self, bars: list[tuple[float, ...]]) -> None:
        self.bars = bars
        self.calls: list[tuple[Any, ...]] = []

    async def ohlcv(self, exchange: str, symbol: str, timeframe: str, since_ms: int, until_ms: int) -> Any:
        self.calls.append((exchange, symbol, timeframe, since_ms, until_ms))
        return [(int(b[0] * 1000), *b[1:5]) for b in self.bars if since_ms <= b[0] * 1000 <= until_ms]


class Renderer:
    def __init__(self, folder: Path) -> None:
        self.folder = folder
        self.data: list[dict[str, Any]] = []

    async def __call__(self, data: dict[str, Any], folder: Any, **_kw: Any) -> RenderResult:
        json.dumps(data, allow_nan=False)                       # (it goes to the drawing process as JSON)
        self.data.append(data)
        path = self.folder / f"card{len(self.data)}.png"
        path.write_bytes(b"png")
        return RenderResult(OK, path=path)


class World:
    """binance (ccxt) and fx2 (MetaTrader) cards on a real database; the outside world faked."""

    async def up(self, tmp_path: Path) -> World:
        self.clock = Clock(NOW)
        self.db = Database(str(tmp_path / "bot.db"), clock=self.clock)
        await self.db.open()
        self.notes = Notes()
        self.long = S.btc_long_result()
        self.oracle = Oracle(self.long["bars"])
        self.renderer = Renderer(tmp_path)
        self.mono = [1000.0]
        self.fx2 = SimpleNamespace(calls=[], answer=None)

        async def trade_chart(*args: Any) -> Any:
            self.fx2.calls.append(args)
            return self.fx2.answer

        self.fx2.trade_chart = trade_chart
        self.svc = TradeCardService(
            self.db, self.notes, tmp_path, accounts=["binance", "fx", "fx2", "breakout"],
            aliases={"fx": ["fx1", "FundingPips"]},
            sources={"binance": ("ccxt", "binanceusdm"), "fx": ("mt5", None), "fx2": ("mt5", None),
                     "breakout": ("ccxt", "binanceusdm")},
            broker=lambda name: self.fx2 if name == "fx2" else None, oracle=self.oracle, manual=["breakout"],
            clock=self.clock, renderer=self.renderer, pillow=lambda: True, monotonic=lambda: self.mono[0])
        return self

    async def week(self) -> dict[str, int]:
        """binance this week: #a closed on Monday (the BTC sample), #b opened last week and still open, #c a limit
        waiting since Tuesday; not listed: #d ended last week, #e an entry that expired unfilled, and tickmill's."""
        t = self.long["trade"]
        start = datetime.fromtimestamp(self.long["bars"][0][0], tz=UTC)
        created = start + timedelta(minutes=15 * 58 + 3)
        fill = created + timedelta(seconds=20)
        close = start + timedelta(minutes=15 * 83 + 7)
        closed_pos = self.long["pos"].model_copy(deep=True)
        closed_pos.data.update(opened_at=ms(fill), closed_at=ms(close), closed_reason="take_profit",
                               exits=[{"price": 91800.0, "qty": 0.05, "kind": "tp"},
                                      {"price": 92200.0, "qty": 0.05, "kind": "tp"}])
        tick = BrokerPosition(broker="tickmill", broker_symbol="BTCUSD", side=Side.LONG, status=PositionStatus.CLOSED,
                              quantity=1, filled_quantity=1, avg_entry_price=91000.0, realized_pnl=1.0)
        ids = {"a": await add_trade(self.db, self.clock, t, closed_pos, created=created, closed=close,
                                    status=TradeStatus.CLOSED, others=(tick,))}
        self.clock.now = start + timedelta(minutes=15 * 70 + 2)
        await self.db.log_event("manage_done", {"trade_id": ids["a"], "action": "move_sl",
                                                "plan": {"move_sl_to_entry": True},
                                                "outcomes": [{"broker": "binance", "kind": "ok", "text": ""}]})
        self.clock.now = NOW
        open_pos = BrokerPosition(broker="binance", broker_symbol="ETH/USDT:USDT", side=Side.LONG,
                                  status=PositionStatus.OPEN, quantity=1, filled_quantity=1, avg_entry_price=3100.0,
                                  stop_loss=3050.0,
                                  data={"opened_at": ms(datetime(2026, 11, 12, 9, 0, 30, tzinfo=UTC))})
        ids["b"] = await add_trade(self.db, self.clock, S.trade(0, S.plan("ETH", Side.LONG, 3100, 3050, [3200]),
                                                                 TradeStatus.OPEN, NOW), open_pos,
                                   created=datetime(2026, 11, 12, 9, 0, tzinfo=UTC))
        pending = BrokerPosition(broker="binance", broker_symbol="SOL/USDT:USDT", side=Side.SHORT,
                                 status=PositionStatus.PENDING, quantity=1)
        ids["c"] = await add_trade(self.db, self.clock, S.trade(0, S.plan("SOL", Side.SHORT, 150, 155, [140],
                                                                           entry_type=EntryType.LIMIT),
                                                                 TradeStatus.PENDING, NOW), pending,
                                   created=datetime(2026, 11, 17, 8, 0, tzinfo=UTC))
        self.clock.now = datetime(2026, 11, 17, 8, 0, 2, tzinfo=UTC)
        await self.db.log_event("order_send", {"trade_id": ids["c"], "broker": "binance"})
        self.clock.now = NOW
        old = open_pos.model_copy(update={"status": PositionStatus.CLOSED, "realized_pnl": 5.0})
        ids["d"] = await add_trade(self.db, self.clock, S.trade(0, S.plan("XRP", Side.LONG, 2.0, 1.9, [2.2]),
                                                                 TradeStatus.CLOSED, NOW), old,
                                   created=datetime(2026, 11, 10, 9, 0, tzinfo=UTC),
                                   closed=datetime(2026, 11, 13, 9, 0, tzinfo=UTC), status=TradeStatus.CLOSED)
        expired = pending.model_copy(update={"status": PositionStatus.CANCELLED})
        ids["e"] = await add_trade(self.db, self.clock, S.trade(0, S.plan("ADA", Side.LONG, 1.0, 0.9, [1.2]),
                                                                 TradeStatus.CANCELLED, NOW), expired,
                                   created=datetime(2026, 11, 16, 9, 0, tzinfo=UTC),
                                   closed=datetime(2026, 11, 16, 21, 0, tzinfo=UTC), status=TradeStatus.CANCELLED)
        return ids

    async def settle(self) -> None:
        job = self.svc._job
        if job is not None:
            await asyncio.wait_for(asyncio.gather(job, return_exceptions=True), 10)

    async def down(self) -> None:
        await self.svc.stop()
        await self.db.close()


@pytest.fixture
async def world(tmp_path: Path):
    w = await World().up(tmp_path)
    yield w
    await w.down()


# =========================================================================== #
# The week and the list
# =========================================================================== #
def test_the_week_is_the_weekly_report_cards_week() -> None:
    assert tc.week_start(NOW) == datetime(2026, 11, 15, 22, 0, tzinfo=UTC)
    assert tc.week_start(datetime(2026, 11, 15, 21, 59, tzinfo=UTC)) == datetime(2026, 11, 8, 22, 0, tzinfo=UTC)
    summer = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)          # British Summer Time: Sunday 22:00 UK = 21:00 UTC
    assert tc.week_start(summer) == datetime(2026, 7, 12, 21, 0, tzinfo=UTC)


async def test_the_list_holds_this_weeks_trades_of_that_account_only(world: World) -> None:
    ids = await world.week()
    reply = await world.svc.command("binance")
    lines = [line for line in reply.splitlines() if line[:2] in ("1-", "2-", "3-", "4-")]
    assert len(lines) == 3, reply
    assert lines[0].startswith("1- ETH LONG") and f"#{ids['b']}" in lines[0] and "açık" in lines[0]
    assert lines[1].startswith("2- BTC LONG") and f"#{ids['a']}" in lines[1] and "kapandı" in lines[1]
    assert lines[2].startswith("3- SOL SHORT") and f"#{ids['c']}" in lines[2] and "bekliyor" in lines[2]
    assert f"#{ids['d']}" not in reply and f"#{ids['e']}" not in reply
    assert world.svc.list_live()
    assert "tickmill" not in reply


async def test_no_trades_this_week_says_so(world: World) -> None:
    reply = await world.svc.command("fx")
    assert "bu hafta" in reply and "işlem yok" in reply
    assert not world.svc.list_live()


async def test_a_number_picks_once_and_the_list_expires(world: World) -> None:
    await world.week()
    await world.svc.command("binance")
    assert "listede yok" in await world.svc.pick("9")
    assert world.svc.list_live()                                       # a wrong number leaves the list
    assert "tek bir numara" in await world.svc.pick("1-3")
    assert await world.svc.pick("2") == svc_mod.STARTED
    await world.settle()
    assert not world.svc.list_live()                                   # one use
    assert "liste" in (await world.svc.pick("2")).lower()
    await world.svc.command("binance")
    world.mono[0] += svc_mod.CARD_LIST_TTL_SEC + 1
    assert not world.svc.list_live()
    assert "süresi doldu" in await world.svc.pick("1")
    assert svc_mod.CARD_LIST_TTL_SEC == notifier_mod.POZKAPAT_TTL_SEC


async def test_an_explicit_number_of_that_accounts_list(world: World) -> None:
    await world.week()
    assert "önce" in await world.svc.command("binance", "2")
    await world.svc.command("binance")
    assert "önce" in await world.svc.command("fx2", "2")               # the list is binance's
    assert await world.svc.command("binance", "2") == svc_mod.STARTED
    await world.settle()
    assert world.renderer.data and world.renderer.data[0]["kind"] == "result"


async def test_a_closed_position_gets_the_result_card(world: World) -> None:
    ids = await world.week()
    await world.svc.command("binance")
    await world.svc.pick("2")
    await world.settle()
    data = world.renderer.data[-1]
    assert data["card"] == "trade" and data["kind"] == "result" and data["no"] == ids["a"]
    assert data["stop"] == 90400.0 and data["target"] == 92200.0      # the ORIGINAL stop, the LAST target
    assert data["exit"] == pytest.approx(92000.0)                     # the recorded close (the chip)
    # the box is drawn to the money's R (118 / 60 = 1.967R from the fill), not to the close price (1.667R)
    assert data["box_far"] == data["played_to"] == pytest.approx(91000.0 + 118.0 / 60.0 * 600.0)
    assert data["box_end"] == data["k_exit"]
    assert data["hero"] == "+1.97R"                                    # position_result_r: 118 / 60
    assert [s[1] for s in data["timeline"]] == ["Giriş · piyasa emri", "Stop girişe çekildi", "Hedef · +1.97R"]
    assert data["chips"][3][1] == "Hedef ✓"
    assert world.notes.photos and "BTC LONG" in world.notes.photos[0][1] and "Sonuç kartı" in world.notes.photos[0][1]
    exchange, symbol, timeframe, since, until = world.oracle.calls[-1]
    assert (exchange, symbol, timeframe) == ("binanceusdm", "BTC/USDT:USDT", "15m")   # no picture size read: 15


async def test_an_open_position_gets_the_entry_card_and_a_pending_one_too(world: World) -> None:
    world.oracle.bars = S.walk(datetime(2026, 11, 11, 18, 0, tzinfo=UTC), 640, 3090.0, seed=5)
    await world.week()
    await world.svc.command("binance")
    await world.svc.pick("1")
    await world.settle()
    data = world.renderer.data[-1]
    assert data["kind"] == "entry" and data["hero"] is None and data["k_exit"] is None
    assert data["box_end"] == data["k_entry"] + tc.ENTRY_BOX_BARS
    assert data["chips"][3][1] == "Açık" and data["last_price"] == data["bars"][-1][4]
    world.oracle.bars = S.walk(datetime(2026, 11, 16, 12, 0, tzinfo=UTC), 180, 151.0, seed=6)
    await world.svc.command("binance")
    await world.svc.pick("3")
    await world.settle()
    data = world.renderer.data[-1]
    assert data["kind"] == "entry" and data["pending"] and not data["arrow_entry"]
    assert data["chips"][3][1] == "Bekliyor" and data["fill"] == 150.0


async def test_the_card_is_chosen_at_the_pick_from_a_fresh_read(world: World) -> None:
    world.oracle.bars = S.walk(datetime(2026, 11, 11, 18, 0, tzinfo=UTC), 640, 3090.0, seed=5)
    ids = await world.week()
    await world.svc.command("binance")                                  # #b is open in the list ...
    rows = await world.db.account_trades("binance", NOW, trade_id=ids["b"])
    closed = rows[0][1].model_copy(deep=True, update={"status": PositionStatus.CLOSED, "realized_pnl": 50.0})
    closed.data.update(closed_at=ms(NOW - timedelta(hours=1)), exits=[{"price": 3150.0, "qty": 1.0, "kind": "close"}])
    await world.db.save_broker_position(ids["b"], closed)               # ... and closes before the number
    await world.svc.pick("1")
    await world.settle()
    data = world.renderer.data[-1]
    assert data["kind"] == "result" and data["exit"] == 3150.0 and data["hero"] == "+1.00R"
    with pytest.raises(tc.CardProblem, match="kapanış fiyatı"):          # no close price on record: no made-up card
        tc.card_data(rows[0][0], closed.model_copy(update={"data": {}, "realized_pnl": None}), "binance",
                     tc.Records(filled=NOW - timedelta(hours=3), closed=NOW), world.oracle.bars, 15)


async def test_failures_are_a_short_line_never_an_exception(world: World) -> None:
    await world.week()
    world.oracle.bars = []
    await world.svc.command("binance")
    await world.svc.pick("2")
    await world.settle()
    assert world.notes.lines and "mumlar okunamadı" in world.notes.lines[-1] and not world.notes.photos
    world.oracle.bars = world.long["bars"]
    world.notes.refuse = "Telegram yok"
    await world.svc.command("binance")
    await world.svc.pick("2")
    await world.settle()
    assert "gönderilemedi" in world.notes.lines[-1]


async def test_without_pillow_no_process_is_started(world: World) -> None:
    await world.week()
    world.svc._pillow = lambda: False
    await world.svc.command("binance")
    await world.svc.pick("2")
    await world.settle()
    assert not world.renderer.data and "Pillow" in world.notes.lines[-1]


async def test_the_card_goes_to_the_subprocess_renderer(world: World, monkeypatch: pytest.MonkeyPatch) -> None:
    """The service draws through karne.render.render_card (another process, its time limit) by default."""
    assert TradeCardService.__init__.__kwdefaults__["renderer"] is render_card
    seen: list[Any] = []

    async def fake_render(data: dict[str, Any], folder: Any, **kw: Any) -> RenderResult:
        seen.append((data["card"], Path(folder).name, kw))
        return RenderResult("failed", detail="x")

    await world.week()
    world.svc._renderer = fake_render
    await world.svc.command("binance")
    await world.svc.pick("2")
    await world.settle()
    assert seen == [("trade", svc_mod.TEMP_DIR, {})] and "çizim hatası" in world.notes.lines[-1]


# =========================================================================== #
# A ccxt position closed by hand on the exchange: the close from the account's own fills
# =========================================================================== #
def binance_on(world: World) -> Any:
    """binance's live adapter: a real CcxtBroker on the fake exchange (its fills: ``fake.trades``)."""
    broker, fake = make_broker()
    world.svc._broker = lambda name: broker if name == "binance" else (world.fx2 if name == "fx2" else None)
    return fake


async def hand_closed_btc(world: World) -> dict[str, Any]:
    """The BTC long (0.1 at 91,000) closed by hand on Binance: the adapter found it flat (no exit of its own, no
    result) four minutes after the close; the only trade of binance this week."""
    t = world.long["trade"]
    start = datetime.fromtimestamp(world.long["bars"][0][0], tz=UTC)
    created = start + timedelta(minutes=15 * 58 + 3)
    fill = created + timedelta(seconds=20)
    seen_flat = start + timedelta(minutes=15 * 83 + 11)
    pos = world.long["pos"].model_copy(deep=True, update={"realized_pnl": None})
    pos.remaining_quantity = 0.0
    pos.data.update(opened_at=ms(fill), closed_at=ms(seen_flat), closed_reason="flat_on_exchange",
                    closed_outside=0.1, exits=[], entry_order_id="E1", sl_order_id="S1")
    tid = await add_trade(world.db, world.clock, t, pos, created=created, closed=seen_flat, status=TradeStatus.CLOSED)
    return {"id": tid, "start": start, "fill": fill, "seen_flat": seen_flat}


def fill_of(side: str, amount: float, price: float, at: datetime, order: str = "H1", **info: Any) -> dict[str, Any]:
    return {"order": order, "side": side, "amount": amount, "price": price, "timestamp": ms(at), "info": dict(info)}


async def card_of_first(world: World) -> dict[str, Any] | None:
    n = len(world.renderer.data)
    await world.svc.command("binance")
    assert await world.svc.pick("1") == svc_mod.STARTED
    await world.settle()
    return world.renderer.data[-1] if len(world.renderer.data) > n else None


async def test_a_close_made_by_hand_on_binance_is_read_from_its_own_fills(world: World) -> None:
    fake = binance_on(world)
    h = await hand_closed_btc(world)
    close1 = h["start"] + timedelta(minutes=15 * 83 + 2)
    close2 = h["start"] + timedelta(minutes=15 * 83 + 7)
    fake.trades = [
        fill_of("sell", 0.1, 90000.0, h["fill"] - timedelta(seconds=30), order="OLD"),   # an earlier trade's close
        fill_of("buy", 0.1, 91000.0, h["fill"], order="E1"),                               # the entry
        fill_of("sell", 0.04, 92050.0, close2),                                            # (out of time order)
        fill_of("sell", 0.06, 91950.0, close1),
        fill_of("sell", 0.2, 91500.0, close2 + timedelta(minutes=1), order="REV"),         # a short opened after it
        fill_of("sell", 0.5, 91000.0, h["seen_flat"] + timedelta(minutes=30), order="LATE"),
    ]
    data = await card_of_first(world)
    assert data is not None, world.notes.lines
    vwap = (0.06 * 91950.0 + 0.04 * 92050.0) / 0.1                                         # 91,990
    assert data["kind"] == "result" and data["exit"] == pytest.approx(vwap)
    # no money R (the result is not known): the price-based R, and the box drawn to that same close price
    assert data["hero"] == tc.fmt_r((vwap - 91000.0) / 600.0) == "+1.65R"
    assert data["played_to"] == data["box_far"] == pytest.approx(vwap)
    assert data["timeline"][-1] == [f"{tc.to_uk(close2):%H:%M}", "Elle kapatıldı (borsada) · +1.65R"]
    assert data["chips"][3][1] == "Kapatıldı" and data["chips"][3][2] == "çıkış 91.990,0"
    k_close = next(k for k, b in enumerate(data["bars"]) if b[0] <= close2.timestamp() < b[0] + 900)
    assert data["k_exit"] == data["box_end"] == k_close                                    # the last fill's candle
    assert ("fetch_my_trades", ("BTC/USDT:USDT", ms(h["fill"]) - 60_000)) in fake.calls
    # only a read: nothing was sent, and the bot's own record of the position is as it was
    assert fake.names() == ["fetch_my_trades"]
    rows = await world.db.account_trades("binance", NOW, trade_id=h["id"])
    assert rows[0][1].data["exits"] == [] and rows[0][1].realized_pnl is None


async def test_fills_that_do_not_cover_the_position_make_no_card(world: World) -> None:
    fake = binance_on(world)
    h = await hand_closed_btc(world)
    fake.trades = [fill_of("sell", 0.09, 91950.0, h["start"] + timedelta(minutes=15 * 83 + 2))]       # 90 %
    assert await card_of_first(world) is None
    assert world.notes.lines[-1] == svc_mod.FAILED.format(why="kapanış fiyatı kayıtlarda yok")
    assert not world.notes.photos and "fetch_my_trades" in fake.names()


async def test_fills_that_cannot_be_read_make_no_card(world: World, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = binance_on(world)
    h = await hand_closed_btc(world)
    fake.trades = [fill_of("sell", 0.1, 91950.0, h["start"] + timedelta(minutes=15 * 83 + 2))]
    refusal = svc_mod.FAILED.format(why="kapanış fiyatı kayıtlarda yok")
    fake.fail_when(lambda method, _args: method == "fetch_my_trades", cx.NetworkError("binance down"))
    assert await card_of_first(world) is None and world.notes.lines[-1] == refusal

    async def never(*_args: Any, **_kw: Any) -> Any:
        await asyncio.sleep(30)

    monkeypatch.setattr(svc_mod, "FILLS_TIMEOUT_SEC", 0.05)
    monkeypatch.setattr(fake, "fetch_my_trades", never)
    assert await card_of_first(world) is None and world.notes.lines[-1] == refusal        # bounded
    monkeypatch.undo()
    # a dry-run (simulated) account's adapter is no live ccxt one: its exchange's fills are not this position's
    calls = len(fake.calls)
    world.svc._broker = lambda name: SimpleNamespace(kind="dry-run:ccxt", exchange=fake)
    assert await card_of_first(world) is None and world.notes.lines[-1] == refusal
    assert len(fake.calls) == calls and not world.notes.photos


async def test_the_bots_own_exits_and_an_open_position_read_no_fills(world: World) -> None:
    world.oracle.bars = world.long["bars"]
    fake = binance_on(world)
    fake.trades = [fill_of("sell", 0.1, 80000.0, NOW - timedelta(days=2))]
    await world.week()
    await world.svc.command("binance")
    await world.svc.pick("2")                                               # #a: closed by its TPs (booked)
    await world.settle()
    data = world.renderer.data[-1]
    assert data["exit"] == pytest.approx(92000.0) and data["hero"] == "+1.97R"
    assert data["timeline"][-1][1] == "Hedef · +1.97R"
    world.oracle.bars = S.walk(datetime(2026, 11, 11, 18, 0, tzinfo=UTC), 640, 3090.0, seed=5)
    await world.svc.command("binance")
    await world.svc.pick("1")                                               # #b: open
    await world.settle()
    assert world.renderer.data[-1]["kind"] == "entry"
    assert "fetch_my_trades" not in fake.names()


def test_the_close_from_the_fills_takes_only_this_positions_close() -> None:
    sample = S.btc_long_result()
    pos = sample["pos"].model_copy(deep=True)
    opened = sample["rec"].filled
    pos.data.update(opened_at=ms(opened), exits=[])
    rec = tc.Records(placed=sample["rec"].placed, filled=opened, closed=opened + timedelta(hours=3), reason="close",
                     moves=[(opened + timedelta(hours=1), "Stop taşındı"), (opened + timedelta(hours=2, minutes=59),
                                                                             "Stop taşındı")])
    at = opened + timedelta(hours=2)
    trades = [
        fill_of("sell", 0.3, 91600.0, opened + timedelta(minutes=30), positionSide="SHORT"),   # hedge: the short's
        fill_of("sell", 0.04, 91500.0, at, positionSide="LONG"),
        fill_of("sell", 0.25, 91800.0, at + timedelta(minutes=5), positionSide="LONG"),         # a flip: 0.06 of it
        {"order": "X", "side": "sell", "amount": 0.1, "price": None, "timestamp": ms(at)},       # unreadable
    ]
    out = tc.venue_close(pos, rec, trades, NOW)
    assert out is not None and out is not rec and rec.exit_price is None
    assert out.exit_price == pytest.approx((0.04 * 91500.0 + 0.06 * 91800.0) / 0.1)
    assert out.closed == at + timedelta(minutes=5) and out.close_by == tc.CLOSED_ON_VENUE
    assert [m[0] for m in out.moves] == [opened + timedelta(hours=1)]       # (none after the real close)
    # one of the bot's own orders (its stop, not booked): the price, but not "by hand"
    pos.data["sl_order_id"] = "S1"
    own = tc.venue_close(pos, rec, [fill_of("sell", 0.1, 90400.0, at, order="S1")], NOW)
    assert own is not None and own.exit_price == 90400.0 and own.close_by is None
    # a close of the group's on record stays the group's
    grouped = tc.venue_close(pos, tc.Records(filled=opened, closed=rec.closed, close_by="group"),
                             [fill_of("sell", 0.1, 91000.0, at)], NOW)
    assert grouped is not None and grouped.close_by == "group"
    # 95 % covers; no open time, nothing filled or nothing in time: none
    assert tc.venue_close(pos, rec, [fill_of("sell", 0.095, 91000.0, at)], NOW) is not None
    assert tc.venue_close(pos, rec, [fill_of("sell", 0.1, 91000.0, rec.closed + timedelta(minutes=11))], NOW) is None
    assert tc.venue_close(pos, rec, [fill_of("buy", 0.1, 91000.0, at)], NOW) is None
    bare = pos.model_copy(deep=True)
    bare.data.pop("opened_at")
    assert tc.venue_close(bare, tc.Records(closed=rec.closed), [fill_of("sell", 0.1, 91000.0, at)], NOW) is None
    assert tc.venue_close(pos.model_copy(update={"filled_quantity": 0.0}), rec,
                          [fill_of("sell", 0.1, 91000.0, at)], NOW) is None
    # no close time on record: fills up to now
    late = tc.venue_close(pos, tc.Records(filled=opened), [fill_of("sell", 0.1, 91000.0, NOW - timedelta(minutes=1))],
                          NOW)
    assert late is not None and late.closed == NOW - timedelta(minutes=1)


async def nas_on_fx2(world: World, *, fill_noted: bool = True) -> int:
    """The NAS100 short of the sample on fx2 (MetaTrader, server = UTC+2), its terminal's answer ready."""
    sample = S.nas_short_result()
    offset = 7200
    t = sample["trade"]
    pos = sample["pos"].model_copy(deep=True)
    tid = await add_trade(world.db, world.clock, t, pos, created=datetime(2026, 11, 17, 5, 51, tzinfo=UTC),
                          closed=datetime(2026, 11, 17, 9, 24, tzinfo=UTC), status=TradeStatus.CLOSED)
    world.clock.now = datetime(2026, 11, 17, 5, 51, 30, tzinfo=UTC)
    await world.db.log_event("order_send", {"trade_id": tid, "broker": "fx2"})
    if fill_noted:
        world.clock.now = datetime(2026, 11, 17, 7, 56, tzinfo=UTC)
        await world.db.log_event("entry_filled", {"trade_id": tid, "broker": "fx2"})
    world.clock.now = datetime(2026, 11, 17, 9, 4, tzinfo=UTC)
    await world.db.log_event("manage_done", {"trade_id": tid, "action": "move_sl", "plan": {"move_sl_to_entry": True},
                                             "outcomes": [{"broker": "fx2", "kind": "ok"}]})
    world.clock.now = datetime(2026, 11, 17, 9, 23, tzinfo=UTC)
    await world.db.log_event("manage_done", {"trade_id": tid, "action": "close",
                                             "outcomes": [{"broker": "fx2", "kind": "ok"}]})
    world.clock.now = NOW
    fill_srv = int(datetime(2026, 11, 17, 7, 55, tzinfo=UTC).timestamp()) + offset
    out_srv = int(datetime(2026, 11, 17, 9, 23, tzinfo=UTC).timestamp()) + offset
    world.fx2.answer = {"offset": offset, "bars": [(b[0] + offset, *b[1:]) for b in sample["bars"]],
                        "deals": [{"time": fill_srv, "price": 27300.0, "volume": 1.0, "entry": "in",
                                   "reason": "manual"},
                                  {"time": out_srv, "price": 27055.1, "volume": 1.0, "entry": "out",
                                   "reason": "manual"}]}
    return tid


async def test_a_metatrader_account_reads_its_own_terminal_and_server_time_becomes_uk_time(world: World) -> None:
    await nas_on_fx2(world)
    await world.svc.command("fx2")
    await world.svc.pick("1")
    await world.settle()
    assert not world.notes.lines, world.notes.lines
    data = world.renderer.data[-1]
    symbol, tf, start, end, tickets, anchor = world.fx2.calls[-1]
    assert (symbol, tf, tickets) == ("US100.cash", 15, [501])
    assert anchor == datetime(2026, 11, 17, 7, 56, tzinfo=UTC).timestamp()     # the bot's own record of the fill
    assert [s[0] for s in data["timeline"]] == ["05:51", "07:55", "09:04", "09:23"]
    assert data["timeline"][-1][1] == "Grup kapattı · +1.70R" and data["hero"] == "+1.70R"
    assert data["stop"] == 27444.06 and data["exit"] == pytest.approx(27055.1)
    assert data["chips"][3][1:3] == ["Kapatıldı", "çıkış 27.055,10"]
    assert data["subtitle"].startswith("US100.cash  ·  15 dakika  ·  17.11.2026")
    assert (start, end) == tuple(t.timestamp() for t in tc.window(
        datetime(2026, 11, 17, 7, 56, tzinfo=UTC), tc.search_end(datetime(2026, 11, 17, 9, 24, tzinfo=UTC), 15, NOW),
        15, closed_market=True))
    world.fx2.answer = {"offset": None, "bars": [], "deals": []}
    await world.svc.command("fx2")
    await world.svc.pick("1")
    await world.settle()
    assert "saati belirlenemedi" in world.notes.lines[-1]


async def test_only_the_bots_record_of_the_fill_tells_the_terminal_its_clock(world: World) -> None:
    await nas_on_fx2(world, fill_noted=False)       # a limit whose fill notice is not on record: only its placing
    await world.svc.command("fx2")
    await world.svc.pick("1")
    await world.settle()
    assert world.fx2.calls[-1][5] is None            # never the time it was placed (two hours before the fill)


# =========================================================================== #
# The MetaTrader read never in the way of trading
# =========================================================================== #
async def test_the_metatrader_read_is_refused_while_the_trading_path_is_busy(world: World) -> None:
    await nas_on_fx2(world)
    world.svc._busy = lambda: True
    world.svc._idle_wait = 0.3
    await world.svc.command("fx2")
    assert await world.svc.pick("1") == svc_mod.STARTED
    await world.settle()
    assert world.notes.lines == [svc_mod.BUSY_TRADING] == ["⏳ Şu an hesapta işlem yürütülüyor, kartı birazdan tekrar "
                                                            "isteyin."]
    assert world.fx2.calls == [] and not world.renderer.data              # the terminal was not asked at all

    def broken() -> bool:
        raise RuntimeError("cannot look")

    world.svc._busy = broken                                              # what cannot be looked at counts as busy
    await world.svc.command("fx2")
    await world.svc.pick("1")
    await world.settle()
    assert world.notes.lines[-1] == svc_mod.BUSY_TRADING and world.fx2.calls == []


async def test_the_metatrader_read_waits_a_moment_for_the_trading_path(world: World) -> None:
    await nas_on_fx2(world)
    looks: list[bool] = []

    def busy() -> bool:                    # busy for the first looks (e.g. the owner's own number being answered)
        looks.append(len(looks) < 3)
        return looks[-1]

    world.svc._busy = busy
    await world.svc.command("fx2")
    await world.svc.pick("1")
    await world.settle()
    assert not world.notes.lines and world.renderer.data and len(world.fx2.calls) == 1
    assert looks == [True, True, True, False]


async def test_a_public_candles_card_is_made_while_the_trading_path_is_busy(world: World) -> None:
    await world.week()
    world.svc._busy = lambda: True
    world.svc._idle_wait = 0.0
    await world.svc.command("binance")
    await world.svc.pick("2")
    await world.settle()
    assert not world.notes.lines and world.renderer.data[-1]["kind"] == "result"


async def test_the_terminal_busy_with_another_call_refuses_the_card(world: World) -> None:
    await nas_on_fx2(world)
    world.fx2.answer = {"offset": None, "bars": [], "deals": [], "busy": True}
    await world.svc.command("fx2")
    await world.svc.pick("1")
    await world.settle()
    assert world.notes.lines == [svc_mod.BUSY_TRADING] and not world.renderer.data


async def test_the_owners_own_number_does_not_refuse_his_card(chat: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Through the real notifier: the number's handler is still sending "Kart hazırlanıyor" (a slow Telegram) when
    the card starts; the read waits for it instead of refusing."""
    world = chat.world
    await nas_on_fx2(world)
    looks: list[bool] = []

    def busy() -> bool:
        looks.append(chat.n.trading_busy())
        return looks[-1]

    world.svc._busy = busy
    sent = chat.client.send_message
    looked_while_reading: list[bool] = []

    async def slow_send(entity: Any, message: Any, **kw: Any) -> Any:
        if message == svc_mod.STARTED:
            await asyncio.sleep(0.6)
        return await sent(entity, message, **kw)

    async def trade_chart(*args: Any) -> Any:
        looked_while_reading.append(chat.n.trading_busy())
        world.fx2.calls.append(args)
        return world.fx2.answer

    monkeypatch.setattr(chat.client, "send_message", slow_send)
    world.fx2.trade_chart = trade_chart
    await chat.n._on_message_event(MsgEvent("/fx2kart", sender_id=OWNER))
    await chat.n.drain(5)
    await chat.n._on_message_event(MsgEvent("1", sender_id=OWNER))       # handled in a task of the notifier
    await chat.n.drain(5)                   # (the card's job exists once the number has been answered)
    await world.settle()
    assert True in looks                    # the owner's own handler was still running when the card began
    assert looked_while_reading == [False] and world.renderer.data and svc_mod.BUSY_TRADING not in world.notes.lines


# =========================================================================== #
# The card's figures (pure)
# =========================================================================== #
def test_the_original_stop_stays_when_the_stop_was_moved() -> None:
    sample = S.nas_short_result()
    assert sample["pos"].stop_loss == 27300.0 and sample["trade"].current_stop_loss == 27300.0   # moved to entry
    data = S.data_of(sample)
    assert data["stop"] == 27444.06 and data["chips"][1][:3] == ["STOP", "27.444,06", "−1R"]
    assert data["chips"][2][:3] == ["HEDEF", "27.010,25", "+2.01R"]
    assert data["timeline"][2] == ["09:04", "Stop girişe çekildi"]


def test_the_result_box_ends_at_the_close_and_reaches_the_moneys_r() -> None:
    data = S.data_of(S.nas_short_result())
    assert data["kind"] == "result" and data["box_end"] == data["k_exit"]
    # the money's R (170 / 100 = 1.70R) from the fill, short: 27300 - 1.70 x 144.06; the close price stays the chip's
    assert data["box_far"] == data["played_to"] == pytest.approx(27300.0 - 1.70 * 144.06)
    assert data["exit"] == 27055.10 and data["chips"][3][2] == "çıkış 27.055,10" and data["target"] == 27010.25
    assert data["hero"] == "+1.70R"
    k_fill = data["k_entry"]
    bar = data["bars"][k_fill]
    assert bar[0] <= datetime(2026, 11, 17, 7, 55, tzinfo=UTC).timestamp() < bar[0] + 900
    assert data["bars"][data["k_exit"]][0] == datetime(2026, 11, 17, 9, 15, tzinfo=UTC).timestamp()


def test_a_losing_result_has_no_profit_side_and_keeps_the_original_stop_line() -> None:
    data = S.data_of(S.nas_short_stopped())
    assert data["hero"] == "−1.02R" and data["chips"][3][1] == "Stop"
    assert data["box_far"] is None and data["exit"] == 27446.0 and data["stop"] == 27444.06
    # the played-out part ends at the money's −1.02R (102 / 100), a little beyond the original stop
    assert data["played_to"] == pytest.approx(27300.0 + 1.02 * 144.06) and data["played_to"] > data["stop"]
    assert data["timeline"][-1][1] == "Stop · −1.02R"
    loss = S.data_of(S.btc_long_closed_at_a_loss())
    assert loss["hero"] == "−0.50R" and loss["box_far"] is None and loss["box_end"] == loss["k_exit"]
    assert loss["timeline"][-1][1] == "Elle kapatıldı · −0.50R" and loss["chips"][3][2] == "çıkış 90.700,0"


def test_the_box_is_drawn_to_the_moneys_r_not_to_the_price_move() -> None:
    sample = S.btc_long_money_r()
    data = S.data_of(sample)
    # price: (92200 - 91000) / 600 = 2.00R; money: 118.20 / 60 = 1.97R - the hero, the box and the line all 1.97R
    assert tc.result_r(sample["trade"], sample["pos"], 91000.0, 90400.0, 92200.0) == pytest.approx(1.97)
    assert data["hero"] == "+1.97R" and data["timeline"][-1][1] == "Hedef · +1.97R"
    assert data["box_far"] == data["played_to"] == pytest.approx(91000.0 + 1.97 * 600.0)     # 92,182
    assert data["exit"] == 92200.0 and data["target"] == 92200.0 and data["chips"][3][1] == "Hedef ✓"
    k_close = next(k for k, b in enumerate(data["bars"]) if b[0] <= sample["rec"].closed.timestamp() < b[0] + 900
                   and k >= data["k_entry"])
    assert data["k_exit"] == data["box_end"] == k_close                    # the exit arrow: the close candle
    # a 0.5R signal: the hero counts in the plan's R (0.985R), the box still shows the money's move per position
    half = dict(sample, trade=sample["trade"].model_copy(deep=True))
    half["trade"].plan.risk_r = 0.5
    halved = S.data_of(half)
    assert halved["hero"] == tc.fmt_r(118.2 * 0.5 / 60.0)
    assert halved["box_far"] == halved["played_to"] == pytest.approx(91000.0 + 1.97 * 600.0)


def test_a_loss_is_drawn_to_the_moneys_r_too() -> None:
    sample = S.btc_long_closed_at_a_loss()
    sample["pos"].realized_pnl = -31.8                      # money: -0.53R; price: (90700 - 91000) / 600 = -0.50R
    data = S.data_of(sample)
    assert data["hero"] == "−0.53R" and data["box_far"] is None
    assert data["played_to"] == pytest.approx(91000.0 - 0.53 * 600.0)          # 90,682
    assert data["exit"] == 90700.0 and data["chips"][3][2] == "çıkış 90.700,0" and data["stop"] == 90400.0


def test_without_the_moneys_r_the_box_is_drawn_to_the_close_price() -> None:
    for change in ({"simulated": True}, {"initial_risk": None}):
        sample = S.btc_long_money_r()
        sample["pos"].data.update(change)
        data = S.data_of(sample)
        assert tc.money_r(sample["trade"], sample["pos"]) is None
        assert data["hero"] == "+2.00R"                                     # the price move
        assert data["box_far"] == data["played_to"] == data["exit"] == 92200.0
    loss = S.btc_long_closed_at_a_loss()
    loss["pos"].data["simulated"] = True
    data = S.data_of(loss)
    assert data["box_far"] is None and data["played_to"] == data["exit"] == 90700.0


def test_no_recorded_close_price_is_no_card() -> None:
    sample = S.btc_long_result()
    sample["rec"].exit_price = None
    with pytest.raises(tc.CardProblem, match="kapanış fiyatı"):
        S.data_of(sample)


def test_a_pending_entry_card() -> None:
    data = S.data_of(S.sol_short_pending())
    assert data["kind"] == "entry" and data["pending"] and not data["arrow_entry"]
    assert data["fill"] == 150.0 and data["box_far"] == 140.0 and data["k_entry"] == tc.BARS_BEFORE
    assert data["chips"][3][1] == "Bekliyor"
    assert [s[1] for s in data["timeline"]] == ["Limit emir kondu", "Giriş bekleniyor"]


def test_after_an_early_close_the_candles_run_on_until_a_level_is_touched() -> None:
    sample = S.nas_short_result()
    bars = sample["bars"]
    data = S.data_of(sample)
    k_exit = data["k_exit"]
    first = round((data["bars"][0][0] - bars[0][0]) / 900)
    ix = first + k_exit
    touch = next(j for j in range(ix + 1, len(bars)) if bars[j][3] <= 27010.25 or bars[j][2] >= 27444.06)
    assert len(data["bars"]) == min(len(bars), max(ix + tc.RESULT_AFTER_BARS, touch + tc.AFTER_TOUCH_BARS) + 1) - first
    assert k_exit - data["k_entry"] >= 4
    assert data["k_entry"] == tc.BARS_BEFORE                            # 58 candles before the entry


def test_a_take_profit_close_stops_the_candles_24_after_it() -> None:
    data = S.data_of(S.btc_long_result())
    assert data["chips"][3][1] == "Hedef ✓"
    assert len(data["bars"]) == data["k_exit"] + tc.RESULT_AFTER_BARS + 1


def test_a_long_trade_shortens_what_is_shown_before_it() -> None:
    sample = S.eth_long_entry()
    sample["bars"] = S.walk(datetime.fromtimestamp(sample["bars"][0][0], tz=UTC), 400, 3080.0, seed=2)
    data = S.data_of(sample)
    assert data["k_entry"] == tc.MIN_BARS_BEFORE                      # 20 before the entry, then up to now
    assert len(data["bars"]) == 400 - (tc.BARS_BEFORE - tc.MIN_BARS_BEFORE) and data["slots"] == len(data["bars"]) + 8


def test_the_entry_card() -> None:
    data = S.data_of(S.eth_long_entry())
    assert data["kind"] == "entry" and data["hero"] is None
    assert data["box_end"] == data["k_entry"] + tc.ENTRY_BOX_BARS and data["box_far"] == data["target"] == 3186.0
    assert data["slots"] >= data["k_entry"] + 1 + tc.ENTRY_ROOM_BARS
    assert data["chips"][3][:3] == ["DURUM", "Açık", "pozisyon açıldı"]
    assert data["chips"][4][0] == "ZAMAN"
    assert data["timeline"] == [["17:31", "Giriş · piyasa emri"]]          # hand-traded: no "brokerda" step
    assert data["footer"] == "İngiltere saati" and "ÖRNEK" not in json.dumps(data, ensure_ascii=False)


def test_levels_in_the_accounts_own_scale_and_the_last_target() -> None:
    sample = S.btc_long_result()
    pos = sample["pos"].model_copy(deep=True)
    pos.data.update(price_multiplier=1000.0, price_offset={"mode": "additive", "value": 5.0})
    assert tc.original_stop(sample["trade"], pos) == 90400.0 * 1000 + 5.0
    assert tc.card_target(sample["trade"], pos) == 92200.0 * 1000 + 5.0    # the LAST of two take profits
    no_tp = sample["trade"].model_copy(deep=True)
    no_tp.plan.take_profits = []
    assert tc.card_target(no_tp, pos) is None


def test_a_hand_traded_account_gets_the_r_of_the_price_move() -> None:
    sample = S.btc_long_result()
    pos = sample["pos"].model_copy(deep=True)
    pos.data["simulated"] = True
    assert tc.result_r(sample["trade"], pos, 91000.0, 90400.0, 92200.0) == pytest.approx(2.0)
    half = sample["trade"].model_copy(deep=True)
    half.plan.risk_r = 0.5
    assert tc.result_r(half, pos, 91000.0, 90400.0, 90400.0) == pytest.approx(-0.5)


def test_the_records_of_an_exchange_position() -> None:
    sample = S.btc_long_result()
    t, pos = sample["trade"], sample["pos"].model_copy(deep=True)
    fill, close = datetime(2026, 11, 16, 15, 33, tzinfo=UTC), datetime(2026, 11, 16, 21, 52, tzinfo=UTC)
    pos.data.update(opened_at=ms(fill), closed_at=ms(close), closed_reason="manual_close",
                    exits=[{"price": 91500.0, "qty": 0.1, "kind": "close"}])
    events = [{"ts": fill - timedelta(seconds=2), "kind": "order_send",
               "payload": {"trade_id": 12, "broker": "binance"}},
              {"ts": close, "kind": "pozkapat", "payload": {"closes": [["binance", 12]]}},
              {"ts": close, "kind": "manage_done", "payload": {"trade_id": 12, "action": "move_sl", "plan": {},
                                                               "outcomes": [{"broker": "fx", "kind": "ok"}]}}]
    rec = tc.read_records(t, pos, None, events)
    assert (rec.filled, rec.closed, rec.exit_price) == (fill, close, 91500.0)
    assert (rec.reason, rec.close_by) == ("close", "owner")
    assert rec.moves == []                                             # another account's stop move


def test_the_chart_bar_size_from_the_signal_picture_else_15() -> None:
    t = S.btc_long_result()["trade"]
    assert tc.timeframe_of(t, svc_mod.CCXT_TIMEFRAMES) == 15
    t.plan.chart_timeframe_min = 60
    assert tc.timeframe_of(t, svc_mod.CCXT_TIMEFRAMES) == 60
    t.plan.chart_timeframe_min = 45                                    # no such bar size on the venue: 15
    assert tc.timeframe_of(t, svc_mod.CCXT_TIMEFRAMES) == 15
    data = S.data_of(S.nas_short_result())
    assert "15 dakika" in data["subtitle"] and data["bar_sec"] == 900
    assert tuple(mt5_broker.TIMEFRAMES) == svc_mod.MT5_TIMEFRAMES


def test_time_labels_are_uk_time() -> None:
    summer = datetime(2026, 7, 15, 9, 0, tzinfo=UTC)
    times = [(summer + timedelta(minutes=15 * k)).timestamp() for k in range(40)]
    labels = dict(tc._time_labels(times, 15))
    assert all(text.endswith(":00") for text in labels.values())
    assert labels[4] == "11:00"                                        # 10:00 UTC in British Summer Time


# =========================================================================== #
# The parser's new field (inert, backward compatible)
# =========================================================================== #
def test_the_parser_reads_the_charts_bar_size_only_when_it_is_a_bar_size() -> None:
    schema = llm_parser.WIRE_SCHEMA["properties"]["actions"]["items"]
    assert "chart_timeframe_min" in schema["properties"] and "chart_timeframe_min" in schema["required"]
    assert prompts.WORKED_EXAMPLE_OUTPUT["actions"][0]["chart_timeframe_min"] is None
    assert "chart_timeframe_min" in prompts.SYSTEM_PROMPT
    action = dict(prompts.WORKED_EXAMPLE_OUTPUT["actions"][0])
    action.pop("chart_timeframe_min")
    assert llm_parser._WireAction.model_validate(action).chart_timeframe_min is None   # an older response
    assert llm_parser._WireAction.model_validate({**action, "chart_timeframe_min": 15}).chart_timeframe_min == 15
    for raw, read in ((15, 15), (60.0, 60), (0, None), (-5, None), (7.5, None), (99999, None), (True, None),
                      (None, None)):
        assert llm_parser.chart_timeframe(raw) == read, raw


def test_the_bar_size_goes_onto_the_plan_and_an_old_plan_reads_15() -> None:
    from copytrader.config import LLMConfig, RiskConfig
    from copytrader.parsing import validator

    action = ParsedAction(action=ActionType.OPEN, base_asset="BTC", side=Side.LONG, entry_type=EntryType.MARKET,
                          entry_price=91000.0, stop_loss=90400.0, take_profits=[TakeProfit(price=92200.0)],
                          risk_r=1.0, confidence=0.97, used_message_ids=[1], chart_timeframe_min=5)
    plan, _issues = validator.build_open_plan(action, risk=RiskConfig(), llm=LLMConfig(), market_price=91000.0,
                                              source_chat_id=-1, now=NOW)
    assert plan is not None and plan.chart_timeframe_min == 5
    old = json.loads(plan.model_dump_json())
    old.pop("chart_timeframe_min")
    assert type(plan).model_validate(old).chart_timeframe_min is None


# =========================================================================== #
# MetaTrader: the terminal's own candles, read only
# =========================================================================== #
@pytest.fixture
def mt5_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fake ticks carry SERVER_TIME (server = UTC+2): the broker's "now" to match (as test_mt5_broker)."""
    monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: SERVER_TIME - 7200.0))
    monkeypatch.setattr(Mt5Broker, "tick_wait_sec", 0.0)


async def test_mt5_trade_chart_reads_bars_and_deals_in_server_time(mt5_clock: None) -> None:
    fake = FakeMT5()
    b = await connected(fake)
    try:
        fake.deals.append(SimpleNamespace(ticket=1, order=9, time=SERVER_TIME - 3600, time_msc=0, type=1,
                                          entry=MT5.DEAL_ENTRY_IN, position_id=77, reason=3, volume=1.0,
                                          price=2230.0))
        fake.deals.append(SimpleNamespace(ticket=2, order=10, time=SERVER_TIME - 600, time_msc=0, type=0,
                                          entry=MT5.DEAL_ENTRY_OUT, position_id=77, reason=MT5.DEAL_REASON_TP,
                                          volume=1.0, price=2200.0))
        utc_now = SERVER_TIME - 7200
        answer = await b.trade_chart("ETHUSD", 15, utc_now - 3600 * 5, utc_now, [77])
        assert answer["offset"] == 7200
        assert 19 <= len(answer["bars"]) <= 21 and answer["bars"][0][0] % 900 == 0
        assert answer["bars"][0][0] >= utc_now - 3600 * 5 + 7200                  # asked in the server's clock
        assert [(d["entry"], d["reason"], d["price"]) for d in answer["deals"]] == [("in", "manual", 2230.0),
                                                                                    ("out", "tp", 2200.0)]
        assert not [c for c in fake.calls if c[0] in ("order_send", "order_check")]      # read only
        sent = [c for c in fake.calls if c[0] == "copy_rates_range"][-1]
        assert sent[1][1] == 15                                                   # TIMEFRAME_M15
        json.dumps(answer)                                                        # plain data (the bridge pickles it)
    finally:
        await b.close()


async def test_mt5_trade_chart_with_a_closed_market(mt5_clock: None, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeMT5()
    b = await connected(fake)
    try:
        monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: SERVER_TIME - 7200 + 3 * 86400))
        fake.deals.append(SimpleNamespace(ticket=1, order=9, time=SERVER_TIME - 3600, time_msc=0, type=1,
                                          entry=MT5.DEAL_ENTRY_IN, position_id=77, reason=3, volume=1.0,
                                          price=2230.0))
        blind = await b.trade_chart("ETHUSD", 15, SERVER_TIME - 90000, SERVER_TIME, [77])
        assert blind["offset"] is None and blind["bars"] == []                     # nothing guessed
        assert not [c for c in fake.calls if c[0] == "copy_rates_range"]
        anchored = await b.trade_chart("ETHUSD", 15, SERVER_TIME - 90000, SERVER_TIME, [77],
                                       anchor_utc=SERVER_TIME - 3600 - 7200 + 40)
        assert anchored["offset"] == 7200 and anchored["bars"]
        with pytest.raises(Exception, match="7 dakikalık"):
            await b.trade_chart("ETHUSD", 7, 0, 1)
    finally:
        await b.close()


@pytest.mark.parametrize("age_min", [55, 60, 65])
async def test_mt5_trade_chart_takes_the_offset_from_the_deals_not_an_hour_old_tick(
        mt5_clock: None, monkeypatch: pytest.MonkeyPatch, age_min: int) -> None:
    """A tick 50-70 minutes old passes the staleness test and would put the server's clock an hour off (UTC+1
    instead of UTC+2); the opening deal against the bot's own record of the fill says UTC+2."""
    fake = FakeMT5()
    b = await connected(fake)
    try:
        utc_now = SERVER_TIME - 7200 + age_min * 60                  # the newest tick is age_min minutes old
        monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: float(utc_now)))
        assert not b._tick_is_stale(SERVER_TIME)                      # (the trap: it looks fresh)
        assert round((SERVER_TIME - utc_now) / 3600.0) * 3600 == 3600  # (and says UTC+1)
        deal_srv = SERVER_TIME - 3600
        fake.deals.append(SimpleNamespace(ticket=1, order=9, time=deal_srv, time_msc=0, type=1,
                                          entry=MT5.DEAL_ENTRY_IN, position_id=77, reason=3, volume=1.0,
                                          price=2230.0))
        fake.calls.clear()
        answer = await b.trade_chart("ETHUSD", 15, utc_now - 3600 * 5, utc_now, [77],
                                     anchor_utc=deal_srv - 7200 + 25)   # the bot noted the fill 25 s later
        assert answer["offset"] == 7200 and answer["bars"]
        assert not [c for c in fake.calls if c[0] == "symbol_info_tick"]    # not even asked: one call less
        sent = [c for c in fake.calls if c[0] == "copy_rates_range"][-1]
        assert sent[1][2] == int(utc_now - 3600 * 5) + 7200
        # no deals (or no record of the fill): the tick, as before
        fake.deals.clear()
        assert (await b.trade_chart("ETHUSD", 15, utc_now - 3600, utc_now, [77],
                                    anchor_utc=deal_srv - 7200 + 25))["offset"] == 3600
    finally:
        await b.close()


async def test_mt5_trade_chart_trusts_only_a_record_close_to_the_fill(mt5_clock: None) -> None:
    fake = FakeMT5()
    b = await connected(fake)
    try:
        deal_srv = SERVER_TIME - 3600
        fake.deals.append(SimpleNamespace(ticket=1, order=9, time=deal_srv, time_msc=0, type=1,
                                          entry=MT5.DEAL_ENTRY_IN, position_id=77, reason=3, volume=1.0,
                                          price=2230.0))
        utc_now = SERVER_TIME - 7200
        # noted 55 minutes after the fill (the bot was down): an hour off by the deal alone - the fresh tick says
        late = await b.trade_chart("ETHUSD", 15, utc_now - 3600, utc_now, [77], anchor_utc=deal_srv - 7200 + 55 * 60)
        assert late["offset"] == 7200 and [c for c in fake.calls if c[0] == "symbol_info_tick"]
        # a market order noted as it was sent, 30 s before its deal: trusted
        fake.calls.clear()
        early = await b.trade_chart("ETHUSD", 15, utc_now - 3600, utc_now, [77], anchor_utc=deal_srv - 7200 - 30)
        assert early["offset"] == 7200 and not [c for c in fake.calls if c[0] == "symbol_info_tick"]
    finally:
        await b.close()


async def test_mt5_trade_chart_stops_when_the_terminal_gets_another_call(mt5_clock: None,
                                                                        monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeMT5()
    b = await connected(fake)
    try:
        fake.deals.append(SimpleNamespace(ticket=1, order=9, time=SERVER_TIME - 3600, time_msc=0, type=1,
                                          entry=MT5.DEAL_ENTRY_IN, position_id=77, reason=3, volume=1.0,
                                          price=2230.0))
        real = b._oldest_call
        other = SimpleNamespace(what="order_send", timed_out=False, running_for=lambda: 0.0,   # (not stuck)
                                stuck_after=lambda: 1e9, relinking=lambda: False)
        for free_looks in (0, 1):                  # busy at once / an order came while the deals were read
            looks: list[int] = []

            def oldest(free_looks: int = free_looks, looks: list[int] = looks) -> Any:
                looks.append(1)
                return real() if len(looks) <= free_looks else other

            monkeypatch.setattr(b, "_oldest_call", oldest)
            fake.calls.clear()
            utc_now = SERVER_TIME - 7200
            answer = await b.trade_chart("ETHUSD", 15, utc_now - 3600, utc_now, [77])
            assert answer == {"offset": None, "bars": [], "deals": [], "busy": True}
            names = [c[0] for c in fake.calls]
            assert "copy_rates_range" not in names and "symbol_info_tick" not in names
            assert names.count("history_deals_get") == free_looks
        monkeypatch.setattr(b, "_oldest_call", real)
        assert "busy" not in await b.trade_chart("ETHUSD", 15, SERVER_TIME - 10800, SERVER_TIME - 7200, [77])
    finally:
        await b.close()


def test_the_metatrader_window_holds_only_the_candles_drawn() -> None:
    # midweek: BARS_BEFORE + 4 candles of market time (+ WINDOW_SLACK) before the fill, no four-day floor
    wed = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
    start, end = tc.window(wed, wed + timedelta(hours=3), 15, closed_market=True)
    need = timedelta(minutes=15 * (tc.BARS_BEFORE + 4))
    assert wed - need * tc.WINDOW_SLACK - timedelta(minutes=15) < start <= wed - need * tc.WINDOW_SLACK
    assert end == wed + timedelta(hours=3)
    assert tc.window(wed, wed, 15, closed_market=False)[0] == wed - need       # (the public candles: as before)
    # just after the weekend open: Friday's candles before it, still far less than four days
    for fill in (datetime(2026, 11, 15, 22, 5, tzinfo=UTC), datetime(2026, 11, 16, 0, 5, tzinfo=UTC)):
        start, _end = tc.window(fill, fill, 15, closed_market=True)
        assert fill - start < timedelta(days=3)
        market = [t for t in (start + timedelta(minutes=15 * k) for k in range(400)) if t < fill
                  and not (t.weekday() == 4 and t.hour >= 21 or t.weekday() == 5
                           or t.weekday() == 6 and t.hour < 22)]
        assert len(market) >= tc.BARS_BEFORE
        bars = [(t.timestamp(), 1.0, 1.1, 0.9, 1.0) for t in market]
        assert tc._bar_index(bars, fill) == len(bars) - 1
    # a daily bar size: (BARS_BEFORE + 4) x WINDOW_SLACK days of market time back, the (wide) weekends not counted
    start, _end = tc.window(wed, wed, 1440, closed_market=True)
    market_days = 62 * tc.WINDOW_SLACK
    assert timedelta(days=market_days) < wed - start <= timedelta(days=market_days * 168 / 117 + 2)
    weekdays = sum(1 for k in range((wed - start).days) if (start + timedelta(days=k)).weekday() < 5)
    assert weekdays >= tc.BARS_BEFORE


def test_the_helper_lets_the_bot_call_only_the_read(monkeypatch: pytest.MonkeyPatch) -> None:
    assert "trade_chart" in mt5_helper.METHODS
    assert "trade_chart" not in {"open_position", "close_position"}           # (a read: READ_TIMEOUT_SEC)


async def test_the_bridge_forwards_the_read_with_a_reads_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[Any] = []

    async def rpc(self: Any, method: str, *args: Any, timeout: float | None = None, **kw: Any) -> Any:
        seen.append((method, args, timeout))
        return {"offset": 0, "bars": [], "deals": []}

    monkeypatch.setattr(Mt5BridgeBroker, "_rpc", rpc)
    bridge = Mt5BridgeBroker.__new__(Mt5BridgeBroker)
    assert (await bridge.trade_chart("EURUSD", 15, 1.0, 2.0, (5,), 3.0))["offset"] == 0
    assert seen == [("trade_chart", ("EURUSD", 15, 1.0, 2.0, [5], 3.0), None)]


@pytest.mark.skipif(sys.platform == "win32", reason="the fake terminal's helper tests are POSIX")
async def test_the_bridge_reads_candles_through_the_real_helper_process(tmp_path: Path,
                                                                        monkeypatch: pytest.MonkeyPatch) -> None:
    from test_mt5_bridge import Terminal, bridge

    from mt5_fake_terminal import TERMINAL_COMPANY_ENV, TERMINAL_CONTROL_ENV, TERMINAL_FOLDER_ENV, TERMINAL_STATE_ENV

    t = Terminal(tmp_path)
    monkeypatch.setenv(mt5_helper.FAKE_ENV, "mt5_fake_terminal:make")
    monkeypatch.setenv(mt5_helper.FAKE_PATH_ENV, str(Path(__file__).resolve().parent))
    monkeypatch.setenv(TERMINAL_STATE_ENV, str(t.state))
    monkeypatch.setenv(TERMINAL_CONTROL_ENV, str(t.control))
    monkeypatch.setenv(TERMINAL_FOLDER_ENV, r"C:\Program Files\Tickmill MT5 Terminal")
    monkeypatch.setenv(TERMINAL_COMPANY_ENV, "Tickmill Ltd")
    b = await bridge(t)
    try:
        now = time.time()
        answer = await asyncio.wait_for(b.trade_chart("EURUSD", 15, now - 7200, now), 25)
        assert answer["offset"] == 7200 and len(answer["bars"]) >= 8
    finally:
        await asyncio.wait_for(b.close(), 25)


# =========================================================================== #
# The public candles
# =========================================================================== #
async def test_the_public_candles_of_the_venues_own_symbol() -> None:
    class Ex:
        def __init__(self) -> None:
            self.calls: list[Any] = []

        async def fetch_ohlcv(self, symbol: str, timeframe: str, since: int, limit: int) -> list[list[float]]:
            self.calls.append((symbol, timeframe, since, limit))
            return [[since + k * 900_000, 1.0, 2.0, 0.5, 1.5, 9.0] for k in range(3)]

        async def close(self) -> None:
            pass

    ex = Ex()
    oracle = ReferencePriceOracle(exchanges={"binanceusdm": ex})
    rows = await oracle.ohlcv("binanceusdm", "1000PEPE/USDT:USDT", "15m", 0, 2 * 900_000)
    assert rows == [(0, 1.0, 2.0, 0.5, 1.5), (900_000, 1.0, 2.0, 0.5, 1.5), (1_800_000, 1.0, 2.0, 0.5, 1.5)]
    assert ex.calls[0][:2] == ("1000PEPE/USDT:USDT", "15m")
    assert await oracle.ohlcv("nosuchexchange", "X", "15m", 0, 1) is None


# =========================================================================== #
# On Telegram: owner only, never the friend, never confused with /pozkapat
# =========================================================================== #
@pytest.fixture
async def chat(world: World, tmp_path: Path):
    n = BotNotifier(NotifierConfig(bot_token="123:abc", owner_user_id=OWNER),
                    TelegramConfig(api_id=1, api_hash="x", source_chats=[-100]), str(tmp_path))
    client, engine = FakeBotClient(), FakeEngine()
    engine.notifier = n
    engine.closed = []

    async def close_on_accounts(pairs: Any, *, hide_others: bool = False) -> str:
        engine.closed.append(list(pairs))
        return "kapatıldı"

    engine.close_on_accounts = close_on_accounts
    engine.friend_account_names = lambda: ["tickmill"]
    n._client = client
    n._engine = engine
    n.set_tradecard(world.svc)
    n.set_friend(FRIEND)
    friend: list[str] = []
    n._friend_put = lambda message: friend.append(message.text)        # type: ignore[method-assign]
    await world.week()
    return SimpleNamespace(n=n, client=client, engine=engine, friend=friend, world=world)


def owner_texts(client: FakeBotClient) -> list[str]:
    return [m.text for m in client.sent if m.entity == OWNER]


async def test_the_owners_command_and_pick_never_reach_the_friend(chat: Any) -> None:
    await chat.n.handle_message(MsgEvent("/binancekart", sender_id=OWNER))
    assert "1- ETH LONG" in owner_texts(chat.client)[-1]
    await chat.n.handle_message(MsgEvent("2", sender_id=OWNER))
    assert owner_texts(chat.client)[-1] == svc_mod.STARTED
    await chat.world.settle()
    assert chat.world.renderer.data[-1]["kind"] == "result"
    assert chat.friend == [] and chat.engine.closed == []


async def test_aliases_and_unknown_or_friend_accounts(chat: Any) -> None:
    await chat.n.handle_message(MsgEvent("/fx1kart", sender_id=OWNER))
    assert "fx" in owner_texts(chat.client)[-1] and "işlem yok" in owner_texts(chat.client)[-1]
    await chat.n.handle_message(MsgEvent("/tickmillkart", sender_id=OWNER))
    last = owner_texts(chat.client)[-1]
    assert "yalnızca sizin hesaplarınız" in last and "/tickmillkart" not in last.split(":", 1)[1]
    assert "/binancekart" in last and "/breakoutkart" in last
    assert chat.friend == []


async def test_the_friend_cannot_use_it(chat: Any) -> None:
    assert "kart" not in " ".join(notifier_mod.FRIEND_COMMANDS) and "kart" not in notifier_mod.FRIEND_HELP
    await chat.n.handle_message(MsgEvent("/tickmillkart", sender_id=FRIEND))
    await chat.n.handle_message(MsgEvent("/binancekart", sender_id=FRIEND))
    assert chat.friend == [notifier_mod.FRIEND_REFUSED]                  # (one refusal line now and then)
    await chat.n.handle_message(MsgEvent("/binancekart", sender_id=OWNER))
    await chat.n.handle_message(MsgEvent("1", sender_id=FRIEND))        # the owner's card list is not his
    assert chat.world.svc.list_live() and not chat.world.renderer.data


async def test_the_help_text_names_the_card_commands() -> None:
    assert "/binancekart" in notifier_mod.HELP_TEXT and "/fx2kart" in notifier_mod.HELP_TEXT
    assert "/breakoutkart" in notifier_mod.HELP_TEXT


@pytest.mark.parametrize("card_first", [True, False])
async def test_a_number_with_both_lists_waiting_chooses_neither(chat: Any, card_first: bool) -> None:
    async def pozkapat_list() -> None:
        chat.n._pk_lists[OWNER] = (time.monotonic(), [("binance", 1, "#1 ETH LONG (açık)")])

    if card_first:
        await chat.n.handle_message(MsgEvent("/binancekart", sender_id=OWNER))
        await pozkapat_list()
    else:
        await pozkapat_list()
        await chat.n.handle_message(MsgEvent("/binancekart", sender_id=OWNER))
    await chat.n.handle_message(MsgEvent("1", sender_id=OWNER))
    last = owner_texts(chat.client)[-1]
    assert "/pozkapat 1" in last and "/binancekart 1" in last and "Hiçbir pozisyon kapatılmadı" in last
    assert chat.engine.closed == [] and not chat.world.renderer.data
    assert OWNER in chat.n._pk_lists and chat.world.svc.list_live()   # both still wait
    await chat.n.handle_message(MsgEvent("/binancekart 2", sender_id=OWNER))
    await chat.world.settle()
    assert chat.world.renderer.data and chat.engine.closed == []
    assert chat.friend == []


async def test_a_number_while_a_correction_is_awaited_is_the_correction(chat: Any) -> None:
    await chat.n.handle_message(MsgEvent("/binancekart", sender_id=OWNER))
    chat.engine.awaiting = "c1"
    await chat.n.handle_message(MsgEvent("2", sender_id=OWNER))
    assert ("correction", "c1", "2") in chat.engine.calls
    assert chat.world.svc.list_live() and not chat.world.renderer.data


async def test_without_a_card_list_a_number_goes_to_pozkapat_as_before(chat: Any) -> None:
    chat.n._pk_lists[OWNER] = (time.monotonic(), [("binance", 1, "#1 ETH LONG (açık)")])
    await chat.n.handle_message(MsgEvent("1", sender_id=OWNER))
    assert chat.engine.closed == [[("binance", 1)]]


def test_the_card_accounts_are_the_owners_real_and_hand_traded_ones() -> None:
    def config(dry: bool = False) -> AppConfig:
        return AppConfig(
            telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[-1]),
            notifier=NotifierConfig(bot_token="1:a", owner_user_id=OWNER), dry_run=dry,
            brokers=[dict(type="ccxt", name="binance", exchange="binanceusdm"),
                     dict(type="mt5", name="fx", login=1, password="x", server="s", aliases=["fx1"]),
                     dict(type="mt5_bridge", name="fx2", terminal_path="C:/a/terminal64.exe", friend_account=False,
                          broker_company="BEM"),
                     dict(type="mt5_bridge", name="tickmill", optional=True, terminal_path="C:/b/terminal64.exe",
                          broker_company="Tickmill"),
                     dict(type="manual", name="breakout"), dict(type="paper", name="paper")])

    assert app_mod.card_account_names(config()) == ["binance", "fx", "fx2", "breakout"]
    assert app_mod.card_account_names(config(dry=True)) == []
    service = app_mod.build_tradecard(config(), None, SimpleNamespace(), SimpleNamespace(brokers=[]), None, "/tmp")
    assert service.accounts == ["binance", "fx", "fx2", "breakout"]
    assert service.command_account("fx1kart") == "fx" and service.command_account("FX2KART") == "fx2"
    assert service.command_account("tickmillkart") is None and service.command_account("paperkart") is None
    assert service._sources == {"binance": ("ccxt", "binanceusdm"), "fx": ("mt5", None), "fx2": ("mt5", None),
                                "breakout": ("ccxt", "binanceusdm")}


async def test_the_apps_card_service_refuses_the_read_while_an_order_flow_runs(tmp_path: Path) -> None:
    """The production wiring: build_tradecard's ``busy`` is trading_path_busy over the engine's running order flows
    (its ``_critical`` tasks) and monitor syncs."""
    config = AppConfig(telegram=TelegramConfig(api_id=1, api_hash="h", source_chats=[-1]),
                       notifier=NotifierConfig(bot_token="1:a", owner_user_id=OWNER),
                       brokers=[dict(type="mt5", name="fx", login=1, password="x", server="s")])
    flow: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    engine = SimpleNamespace(brokers=[], _critical={flow}, monitor=SimpleNamespace(_running={}))
    service = app_mod.build_tradecard(config, None, SimpleNamespace(), engine, SimpleNamespace(), str(tmp_path))
    service._idle_wait = 0.0
    assert service._path_busy()
    with pytest.raises(svc_mod._TradingBusy):
        await service._wait_idle()
    flow.set_result(None)
    sync: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    engine.monitor._running[1] = sync                                        # a monitor sync (virtual fires too)
    assert service._path_busy()
    sync.set_result(None)
    assert not service._path_busy()
    await service._wait_idle()                                               # idle: at once


def test_the_bot_never_imports_the_drawing_module() -> None:
    import subprocess

    argv, env, cwd = render_mod.render_command()
    code = ("import sys\nimport copytrader.app, copytrader.karne.tradecard_service, copytrader.karne.tradecard\n"
            "assert 'copytrader.karne.tradecard_design' not in sys.modules\nprint('ok')\n")
    result = subprocess.run([argv[0], "-B", "-c", code], cwd=cwd, env=env, capture_output=True, text=True,
                            timeout=120)
    assert result.returncode == 0 and result.stdout.strip() == "ok", result.stderr


# =========================================================================== #
# Part 2: real pictures (Pillow)
# =========================================================================== #
@needs_pillow
async def test_real_cards_through_the_drawing_process(tmp_path: Path) -> None:
    from PIL import Image

    keep = os.environ.get("TRADECARD_OUT")
    for name, sample in (("short_result", S.nas_short_result()), ("long_result", S.btc_long_result()),
                         ("entry", S.eth_long_entry()), ("short_stopped", S.nas_short_stopped()),
                         ("long_loss_close", S.btc_long_closed_at_a_loss()), ("pending_entry", S.sol_short_pending()),
                         ("long_moneyR", S.btc_long_money_r())):
        started = time.monotonic()
        result = await render_card(S.data_of(sample), tmp_path / "kart")
        took = time.monotonic() - started
        assert result.ok and result.path is not None, result
        with Image.open(result.path) as image:
            assert image.size == (2560, 1600) and image.format == "PNG"
        size = result.path.stat().st_size
        print(f"\ntrade card {name}: {took:.1f} s in the drawing process, {size / 1024:.0f} kB")
        if keep:
            Path(keep).mkdir(parents=True, exist_ok=True)
            shutil.copy(result.path, Path(keep) / f"{name}.png")
        render_mod.remove_picture(result.path)


@needs_pillow
def test_the_drawing_ends_the_played_out_part_at_the_moneys_r(tmp_path: Path) -> None:
    from PIL import Image, ImageChops

    from copytrader.karne import tradecard_design as design

    data = S.data_of(S.btc_long_money_r())

    def pixels(card: dict[str, Any], name: str) -> Image.Image:
        design.render(card, str(tmp_path / name))
        return Image.open(tmp_path / name).convert("RGB")

    base = pixels(data, "a.png")
    other_exit = pixels({**data, "exit": 91500.0}, "b.png")                 # the recorded close is only the chip's
    assert ImageChops.difference(base, other_exit).getbbox() is None
    other_edge = pixels({**data, "played_to": 92000.0}, "c.png")         # the dashed line and the played-out part
    assert ImageChops.difference(base, other_edge).getbbox() is not None


@needs_pillow
@pytest.mark.parametrize("lnum", [True, False])
def test_the_asset_name_with_and_without_raqm(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lnum: bool) -> None:
    from PIL import Image, features

    from copytrader.karne import tradecard_design as design

    if lnum and not features.check("raqm"):
        pytest.skip("this Pillow has no raqm")
    monkeypatch.setattr(design, "LNUM", lnum)
    if not lnum:
        # a Pillow without raqm refuses OpenType features: make sure none is asked for
        real_text = design.ImageDraw.ImageDraw.text

        def no_features(self: Any, *args: Any, **kw: Any) -> Any:
            assert "features" not in kw, kw
            return real_text(self, *args, **kw)

        monkeypatch.setattr(design.ImageDraw.ImageDraw, "text", no_features)
    cv = design.Canvas()
    width = design.asset_name(cv, (56, 130), "NAS100", 80, design.INK)
    plain = design.Canvas()
    letters = design.asset_name(plain, (56, 130), "NAS", 80, design.INK)
    assert width > letters * 1.6
    out = tmp_path / "card.png"
    design.render(S.data_of(S.nas_short_result()), str(out))
    with Image.open(out) as image:
        assert image.size == design.SIZE == (2560, 1600)


@needs_pillow
def test_the_card_uses_only_the_shipped_fonts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from copytrader.karne import tradecard_design as design

    used: set[str] = set()
    plain = design._font.__wrapped__

    def spy(name: str, px: int) -> Any:
        used.add(name)
        return plain(name, px)

    monkeypatch.setattr(design, "_font", spy)
    for sample in (S.nas_short_result(), S.btc_long_result(), S.eth_long_entry()):
        design.render(S.data_of(sample), str(tmp_path / "x.png"))
    shipped = {p.stem for p in (Path(design.FONTS_DIR)).glob("*.ttf")}
    assert used <= shipped and "CormorantGaramond_600SemiBold" in used
