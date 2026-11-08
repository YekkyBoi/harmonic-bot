"""Tests for copytrader.storage.db.Database (SQLite storage)."""

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from copytrader.interfaces import Storage
from copytrader.models import (
    ActionType,
    BrokerPosition,
    ConfirmationStatus,
    EntryType,
    ManagementPlan,
    MarketType,
    ParsedAction,
    PendingConfirmation,
    PositionStatus,
    Side,
    StoredMessage,
    TakeProfit,
    TradePlan,
    TradeStatus,
    ValidationIssue,
)
from copytrader.storage import db as dbmod
from copytrader.storage.db import SCHEMA_VERSION, Database

UTC = timezone.utc
CHAT = -1001234567890
TRADER = 111
OTHER = 222
T0 = datetime(2026, 11, 7, 12, 0, 0, tzinfo=UTC)


class Clock:
    def __init__(self, start: datetime = T0) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kw: float) -> datetime:
        self.now = self.now + timedelta(**kw)
        return self.now


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
async def db(clock: Clock):
    database = Database(":memory:", clock=clock)
    await database.open()
    try:
        yield database
    finally:
        await database.close()


def is_utc(dt: datetime | None) -> bool:
    return dt is not None and dt.tzinfo is UTC


def msg(message_id: int, *, date: datetime = T0, chat_id: int = CHAT, sender_id: int | None = TRADER,
        text: str = "", **kw) -> StoredMessage:
    return StoredMessage(chat_id=chat_id, message_id=message_id, sender_id=sender_id, date=date, text=text, **kw)


def plan(plan_id: str = "p1", *, base: str = "ETH", side: Side = Side.LONG, msg_ids: list[int] | None = None,
         chat_id: int = CHAT, created_at: datetime = T0) -> TradePlan:
    return TradePlan(
        plan_id=plan_id,
        base_asset=base,
        quote_asset="USDT",
        market_type=MarketType.PERP,
        side=side,
        entry_type=EntryType.MARKET,
        entry_price=2232.18,
        stop_loss=2205.11,
        take_profits=[TakeProfit(price=2310.60, close_fraction=1.0)],
        risk_r=1.0,
        risk_pct=2.0,
        source_chat_id=chat_id,
        source_message_ids=[10] if msg_ids is None else msg_ids,
        summary_tr="ETH long 1R",
        created_at=created_at,
    )


def pos(broker: str = "binance", status: PositionStatus = PositionStatus.OPEN, pnl: float | None = None,
        **kw) -> BrokerPosition:
    return BrokerPosition(broker=broker, broker_symbol="ETH/USDT:USDT", side=Side.LONG, status=status,
                          quantity=1.5, filled_quantity=1.5, remaining_quantity=1.5, avg_entry_price=2232.5,
                          stop_loss=2205.11, take_profits=[TakeProfit(price=2310.6)], realized_pnl=pnl, **kw)


def conf(conf_id: str = "c1", *, created_at: datetime = T0, status: ConfirmationStatus = ConfirmationStatus.PENDING,
         with_plan: bool = True) -> PendingConfirmation:
    action = ParsedAction(action=ActionType.OPEN, symbol="ETHUSDT.P", base_asset="ETH", side=Side.LONG,
                          entry_price=2232.18, stop_loss=2205.11, take_profits=[TakeProfit(price=2310.6)],
                          risk_r=1.0, confidence=0.6, ambiguities=["Giriş fiyatı net değil"],
                          summary_tr="ETH long, giriş 2232.18, SL 2205.11, TP 2310.60, 1R")
    return PendingConfirmation(
        id=conf_id,
        kind="open",
        action=action,
        plan=plan(created_at=created_at) if with_plan else None,
        issues=[ValidationIssue(code="entry_far", message_tr="Giriş fiyatı piyasadan uzak")],
        market_price=2232.85,
        source_chat_id=CHAT,
        source_message_ids=[10],
        created_at=created_at,
        expires_at=created_at + timedelta(minutes=10),
        status=status,
    )


# --------------------------------------------------------------------------- #
# Lifecycle / schema
# --------------------------------------------------------------------------- #
async def test_implements_storage_protocol(db: Database):
    assert isinstance(db, Storage)


async def test_memory_db_open_close_idempotent():
    database = Database(":memory:")
    await database.open()
    await database.open()
    await database.set_state("k", 1)
    assert await database.get_state("k") == 1
    await database.close()
    await database.close()


async def test_methods_before_open_raise():
    database = Database(":memory:")
    with pytest.raises(RuntimeError):
        await database.get_state("x")
    with pytest.raises(RuntimeError):
        await database.save_message(msg(1))


async def test_async_context_manager():
    async with Database(":memory:") as database:
        await database.set_state("a", [1])
        assert await database.get_state("a") == [1]
    with pytest.raises(RuntimeError):
        await database.get_state("a")


async def test_file_db_creates_parent_dir_and_uses_wal(tmp_path: Path):
    path = tmp_path / "nested" / "dir" / "copytrader.db"
    async with Database(str(path)) as database:
        await database.set_state("x", 1)
    assert path.exists()
    with closing(sqlite3.connect(path)) as raw:
        assert raw.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert raw.execute("SELECT version FROM schema_version").fetchall() == [(SCHEMA_VERSION,)]


async def test_refuses_newer_schema(tmp_path: Path):
    path = tmp_path / "db.sqlite"
    async with Database(str(path)):
        pass
    with closing(sqlite3.connect(path)) as raw:
        raw.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION + 1,))
        raw.commit()
    database = Database(str(path))
    with pytest.raises(RuntimeError, match="schema"):
        await database.open()
    with pytest.raises(RuntimeError):
        await database.get_state("x")


async def test_persistence_across_reopen(tmp_path: Path):
    path = str(tmp_path / "data" / "copytrader.db")
    async with Database(path) as d1:
        await d1.save_message(msg(10, text="eth 1R long", consumed=True))
        trade = await d1.create_trade(plan(), TradeStatus.OPEN)
        await d1.save_broker_position(trade.id, pos(pnl=None, data={"order_id": "abc", "tickets": [1, 2]}))
        await d1.save_confirmation(conf("c9"))
        await d1.set_state("last_msg_id:1", 500)
        await d1.log_event("trade_opened", {"trade_id": trade.id})

    async with Database(path) as d2:
        m = await d2.get_message(CHAT, 10)
        assert m is not None and m.text == "eth 1R long" and m.consumed
        t = await d2.get_trade(trade.id)
        assert t is not None and t.status is TradeStatus.OPEN and t.plan == trade.plan
        assert is_utc(t.created_at) and is_utc(t.plan.created_at)
        positions = await d2.broker_positions(trade.id)
        assert positions[0].data == {"order_id": "abc", "tickets": [1, 2]}
        c = await d2.get_confirmation("c9")
        assert c is not None and c.status is ConfirmationStatus.PENDING
        assert await d2.get_state("last_msg_id:1") == 500
        assert [e["kind"] for e in await d2.events()] == ["trade_opened"]
        # ids keep increasing after reopen
        t2 = await d2.create_trade(plan("p2"))
        assert t2.id > trade.id


def test_message_columns_cover_model():
    # Fails when StoredMessage grows a field the table does not store.
    assert set(dbmod._MESSAGE_COLUMNS) == set(StoredMessage.model_fields)


# --------------------------------------------------------------------------- #
# Datetimes
# --------------------------------------------------------------------------- #
def test_to_utc_helper():
    naive = datetime(2026, 1, 1, 10, 0)
    assert dbmod.to_utc(naive) == datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    ist = datetime(2026, 1, 1, 13, 0, tzinfo=ZoneInfo("Europe/Istanbul"))
    out = dbmod.to_utc(ist)
    assert out.tzinfo is UTC and out.hour == 10


async def test_naive_and_non_utc_input_returned_as_aware_utc(db: Database):
    await db.save_message(msg(1, date=datetime(2026, 11, 7, 12, 0)))                      # naive
    await db.save_message(msg(2, date=datetime(2026, 11, 7, 15, 0, tzinfo=ZoneInfo("Europe/Istanbul"))))
    m1, m2 = await db.get_message(CHAT, 1), await db.get_message(CHAT, 2)
    assert m1 is not None and m2 is not None
    assert m1.date == T0 and is_utc(m1.date)
    assert m2.date == T0 and is_utc(m2.date)

    naive_plan = plan(created_at=datetime(2026, 11, 7, 12, 0))
    t = await db.create_trade(naive_plan)
    assert is_utc(t.plan.created_at) and t.plan.created_at == T0
    assert naive_plan.created_at.tzinfo is None  # caller's object not mutated

    c = conf(created_at=datetime(2026, 11, 7, 12, 0))
    await db.save_confirmation(c)
    got = await db.get_confirmation("c1")
    assert got is not None
    assert is_utc(got.created_at) and is_utc(got.expires_at) and is_utc(got.plan.created_at)


async def test_since_filter_uses_real_time_not_string_order(db: Database):
    # Same instant written with and without microseconds / different zones.
    await db.save_message(msg(1, date=T0))
    await db.save_message(msg(2, date=T0 + timedelta(microseconds=500)))
    since = (T0 + timedelta(microseconds=1)).astimezone(ZoneInfo("America/New_York"))
    got = await db.recent_messages(CHAT, since)
    assert [m.message_id for m in got] == [2]


# --------------------------------------------------------------------------- #
# Messages
# --------------------------------------------------------------------------- #
async def test_save_and_get_message_roundtrip(db: Database):
    m = msg(5, text="eth 1R long", sender_name="Trader", media_path="/tmp/a.jpg", media_type="photo",
            reply_to_message_id=4, grouped_id=99)
    await db.save_message(m)
    assert await db.get_message(CHAT, 5) == m
    assert await db.get_message(CHAT, 6) is None
    assert await db.get_message(CHAT + 1, 5) is None


async def test_save_message_edit_updates_text_media_and_keeps_consumed(db: Database):
    await db.save_message(msg(5, text="eth long", sender_name="Trader", grouped_id=7))
    await db.mark_consumed(CHAT, [5])
    edited = msg(5, date=T0 + timedelta(minutes=3), text="eth 1R long", media_path="/x.jpg", media_type="photo",
                 sender_name=None, edited=True, consumed=False)
    await db.save_message(edited)
    got = await db.get_message(CHAT, 5)
    assert got is not None
    assert got.text == "eth 1R long"
    assert got.media_path == "/x.jpg" and got.media_type == "photo"
    assert got.edited is True
    assert got.consumed is True              # never cleared by an upsert
    assert got.date == T0                    # original post time kept
    assert got.sender_name == "Trader"       # metadata only filled in, not wiped
    assert got.grouped_id == 7


async def test_save_message_resave_without_change_is_not_an_edit(db: Database):
    await db.save_message(msg(5, text="same"))
    await db.save_message(msg(5, text="same"))
    got = await db.get_message(CHAT, 5)
    assert got is not None and got.edited is False


async def test_save_message_content_change_marks_edited_even_without_flag(db: Database):
    await db.save_message(msg(5, text="a"))
    await db.save_message(msg(5, text="b"))
    got = await db.get_message(CHAT, 5)
    assert got is not None and got.edited is True and got.text == "b"


async def test_save_message_media_removed(db: Database):
    await db.save_message(msg(5, text="x", media_path="/a.jpg", media_type="photo"))
    await db.save_message(msg(5, text="x", edited=True))
    got = await db.get_message(CHAT, 5)
    assert got is not None and got.media_path is None and got.media_type is None and got.edited


async def test_save_message_consumed_flag_can_be_set_by_upsert(db: Database):
    await db.save_message(msg(5, text="x"))
    await db.save_message(msg(5, text="x", consumed=True))
    got = await db.get_message(CHAT, 5)
    assert got is not None and got.consumed is True


async def test_recent_messages_filters_and_orders_oldest_first(db: Database):
    await db.save_message(msg(1, date=T0 - timedelta(hours=2), text="old"))
    await db.save_message(msg(4, date=T0 + timedelta(seconds=30), text="d"))
    await db.save_message(msg(2, date=T0, text="b"))
    await db.save_message(msg(3, date=T0, text="c", sender_id=OTHER))
    await db.save_message(msg(5, date=T0 + timedelta(seconds=40), text="e", consumed=True))
    await db.save_message(msg(6, date=T0, chat_id=CHAT + 1, text="other chat"))

    since = T0 - timedelta(minutes=5)
    assert [m.message_id for m in await db.recent_messages(CHAT, since)] == [2, 3, 4]
    assert [m.message_id for m in await db.recent_messages(CHAT, since, sender_id=TRADER)] == [2, 4]
    assert [m.message_id for m in await db.recent_messages(CHAT, since, only_unconsumed=False)] == [2, 3, 4, 5]
    everything = await db.recent_messages(CHAT, T0 - timedelta(days=1), only_unconsumed=False)
    assert [m.message_id for m in everything] == [1, 2, 3, 4, 5]
    assert await db.recent_messages(CHAT, T0 + timedelta(hours=1)) == []


async def test_recent_messages_limit_keeps_newest(db: Database):
    for i in range(1, 11):
        await db.save_message(msg(i, date=T0 + timedelta(seconds=i)))
    got = await db.recent_messages(CHAT, T0, limit=3)
    assert [m.message_id for m in got] == [8, 9, 10]


async def test_mark_consumed(db: Database):
    for i in (1, 2, 3):
        await db.save_message(msg(i))
    await db.save_message(msg(2, chat_id=CHAT + 1))
    await db.mark_consumed(CHAT, [1, 3, 3, 999])
    await db.mark_consumed(CHAT, [])
    got = await db.recent_messages(CHAT, T0 - timedelta(minutes=1))
    assert [m.message_id for m in got] == [2]
    other = await db.get_message(CHAT + 1, 2)
    assert other is not None and other.consumed is False
    m1 = await db.get_message(CHAT, 1)
    assert m1 is not None and m1.consumed is True


# --------------------------------------------------------------------------- #
# Trades
# --------------------------------------------------------------------------- #
async def test_create_and_get_trade(db: Database, clock: Clock):
    p = plan(msg_ids=[10, 11])
    t = await db.create_trade(p)
    assert t.id >= 1
    assert t.status is TradeStatus.OPENING
    assert t.plan == p
    assert t.current_stop_loss == p.stop_loss
    assert t.current_take_profits == p.take_profits
    assert t.created_at == clock.now == t.updated_at
    assert is_utc(t.created_at) and is_utc(t.updated_at)
    assert t.closed_at is None and t.note is None
    assert await db.get_trade(t.id) == t
    assert await db.get_trade(t.id + 100) is None


async def test_create_trade_with_terminal_status_sets_closed_at(db: Database, clock: Clock):
    t = await db.create_trade(plan(), TradeStatus.FAILED)
    assert t.status is TradeStatus.FAILED and t.closed_at == clock.now


async def test_find_trade_by_message(db: Database, clock: Clock):
    first = await db.create_trade(plan("p1", msg_ids=[10, 11]))
    clock.advance(minutes=1)
    second = await db.create_trade(plan("p2", msg_ids=[11, 12]))
    other_chat = await db.create_trade(plan("p3", msg_ids=[20], chat_id=CHAT + 1))

    found = await db.find_trade_by_message(CHAT, 10)
    assert found is not None and found.id == first.id
    found = await db.find_trade_by_message(CHAT, 11)
    assert found is not None and found.id == second.id          # most recent first
    assert await db.find_trade_by_message(CHAT, 20) is None      # other chat
    found = await db.find_trade_by_message(CHAT + 1, 20)
    assert found is not None and found.id == other_chat.id
    assert await db.find_trade_by_message(CHAT, 999) is None


async def test_active_trades_filters_status_and_asset(db: Database, clock: Clock):
    eth_open = await db.create_trade(plan("a", base="ETH"), TradeStatus.OPEN)
    clock.advance(seconds=1)
    btc_pending = await db.create_trade(plan("b", base="BTC"), TradeStatus.PENDING)
    clock.advance(seconds=1)
    eth_opening = await db.create_trade(plan("c", base="ETH"))
    clock.advance(seconds=1)
    eth_closed = await db.create_trade(plan("d", base="ETH"), TradeStatus.OPEN)
    await db.update_trade(eth_closed.id, status=TradeStatus.CLOSED)
    await db.create_trade(plan("e", base="ETH"), TradeStatus.CANCELLED)
    await db.create_trade(plan("f", base="ETH"), TradeStatus.FAILED)

    assert [t.id for t in await db.active_trades()] == [eth_opening.id, btc_pending.id, eth_open.id]
    assert [t.id for t in await db.active_trades("ETH")] == [eth_opening.id, eth_open.id]
    assert [t.id for t in await db.active_trades("eth")] == [eth_opening.id, eth_open.id]
    assert [t.id for t in await db.active_trades("BTC")] == [btc_pending.id]
    assert await db.active_trades("XAU") == []


async def test_recent_trades_window_newest_first(db: Database, clock: Clock):
    old = await db.create_trade(plan("old"))
    clock.advance(hours=2)
    mid = await db.create_trade(plan("mid"), TradeStatus.OPEN)
    clock.advance(minutes=5)
    new = await db.create_trade(plan("new"), TradeStatus.CANCELLED)
    got = await db.recent_trades(clock.now - timedelta(hours=1))
    assert [t.id for t in got] == [new.id, mid.id]
    assert [t.id for t in await db.recent_trades(T0 - timedelta(days=1))] == [new.id, mid.id, old.id]
    assert await db.recent_trades(clock.now + timedelta(seconds=1)) == []


async def test_update_trade_fields(db: Database, clock: Clock):
    t = await db.create_trade(plan(), TradeStatus.OPEN)
    clock.advance(minutes=1)
    u = await db.update_trade(t.id, current_stop_loss=2232.18)
    assert u.current_stop_loss == 2232.18
    assert u.updated_at == clock.now and u.created_at == t.created_at
    assert u.status is TradeStatus.OPEN and u.closed_at is None
    assert u.plan.stop_loss == 2205.11                      # original plan untouched

    u = await db.update_trade(t.id, current_take_profits=[{"price": 2300.0, "close_fraction": 0.5},
                                                          TakeProfit(price=2350.0, close_fraction=0.5)])
    assert u.current_take_profits == [TakeProfit(price=2300.0, close_fraction=0.5),
                                      TakeProfit(price=2350.0, close_fraction=0.5)]
    assert u.current_stop_loss == 2232.18                   # unchanged

    u = await db.update_trade(t.id, note="SL girişe çekildi")
    assert u.note == "SL girişe çekildi" and len(u.current_take_profits) == 2

    u = await db.update_trade(t.id, current_take_profits=[])
    assert u.current_take_profits == []
    assert await db.get_trade(t.id) == u


async def test_update_trade_closed_at_semantics(db: Database, clock: Clock):
    t = await db.create_trade(plan(), TradeStatus.OPEN)
    clock.advance(minutes=10)
    closed_time = clock.now
    u = await db.update_trade(t.id, status=TradeStatus.CLOSED)
    assert u.status is TradeStatus.CLOSED and u.closed_at == closed_time and is_utc(u.closed_at)

    clock.advance(minutes=10)
    u = await db.update_trade(t.id, status=TradeStatus.CLOSED, note="again")
    assert u.closed_at == closed_time                       # first close time kept

    for status in (TradeStatus.CANCELLED, TradeStatus.FAILED):
        x = await db.create_trade(plan(f"x-{status.value}"))
        y = await db.update_trade(x.id, status=status)
        assert y.closed_at == clock.now

    u = await db.update_trade(t.id, status=TradeStatus.OPEN)  # reopened (e.g. reconciliation)
    assert u.closed_at is None


async def test_update_trade_no_changes_and_missing(db: Database, clock: Clock):
    t = await db.create_trade(plan())
    clock.advance(minutes=1)
    assert await db.update_trade(t.id) == t
    with pytest.raises(KeyError):
        await db.update_trade(12345, status=TradeStatus.CLOSED)
    with pytest.raises(KeyError):
        await db.update_trade(12345)
    # failed write left no open transaction behind
    assert db._conn is not None and not db._conn.in_transaction
    assert (await db.update_trade(t.id, note="ok")).note == "ok"


async def test_trade_summary_works_on_loaded_record(db: Database):
    t = await db.create_trade(plan(msg_ids=[10]))
    s = (await db.get_trade(t.id)).summary()  # type: ignore[union-attr]
    assert s["asset"] == "ETH" and s["side"] == "long" and s["source_message_ids"] == [10]
    assert s["opened_at"].endswith("+00:00")


# --------------------------------------------------------------------------- #
# Broker positions
# --------------------------------------------------------------------------- #
async def test_broker_positions_upsert_and_order(db: Database):
    t = await db.create_trade(plan())
    await db.save_broker_position(t.id, pos("bybit", PositionStatus.PENDING))
    await db.save_broker_position(t.id, pos("binance", PositionStatus.OPEN, data={"sl_order": "1"}))
    await db.save_broker_position(t.id, pos("bybit", PositionStatus.OPEN, data={"x": 1}))   # update in place

    got = await db.broker_positions(t.id)
    assert [p.broker for p in got] == ["bybit", "binance"]
    assert got[0].status is PositionStatus.OPEN and got[0].data == {"x": 1}
    assert got[1] == pos("binance", PositionStatus.OPEN, data={"sl_order": "1"})
    assert await db.broker_positions(t.id + 1) == []


async def test_active_broker_positions(db: Database):
    t1 = await db.create_trade(plan("a"))
    t2 = await db.create_trade(plan("b"))
    await db.save_broker_position(t1.id, pos("binance", PositionStatus.OPEN))
    await db.save_broker_position(t1.id, pos("mt5", PositionStatus.CLOSED, pnl=-10.0))
    await db.save_broker_position(t2.id, pos("binance", PositionStatus.PENDING))
    await db.save_broker_position(t2.id, pos("mt5", PositionStatus.FAILED))
    await db.save_broker_position(t2.id, pos("paper", PositionStatus.CANCELLED))
    active = await db.active_broker_positions()
    assert [(tid, p.broker, p.status) for tid, p in active] == [
        (t1.id, "binance", PositionStatus.OPEN),
        (t2.id, "binance", PositionStatus.PENDING),
    ]


def _closed_at(database: Database, trade_id: int, broker: str):
    async def run():
        async with database._read() as conn:
            row = await database._fetchone(
                conn, "SELECT closed_at, realized_pnl FROM broker_positions WHERE trade_id=? AND broker=?",
                (trade_id, broker))
        return dbmod._dt_from_db(row["closed_at"]), row["realized_pnl"]
    return run()


async def test_broker_position_closed_at_first_kept_and_pnl_mirrored(db: Database, clock: Clock):
    t = await db.create_trade(plan())
    await db.save_broker_position(t.id, pos("binance", PositionStatus.OPEN, pnl=5.0))
    assert await _closed_at(db, t.id, "binance") == (None, 5.0)

    clock.advance(minutes=30)
    first_close = clock.now
    await db.save_broker_position(t.id, pos("binance", PositionStatus.CLOSED, pnl=-20.0))
    assert await _closed_at(db, t.id, "binance") == (first_close, -20.0)

    clock.advance(minutes=30)
    await db.save_broker_position(t.id, pos("binance", PositionStatus.CLOSED, pnl=-21.5))  # fee correction
    closed_at, pnl = await _closed_at(db, t.id, "binance")
    assert closed_at == first_close and is_utc(closed_at) and pnl == -21.5

    await db.save_broker_position(t.id, pos("binance", PositionStatus.OPEN, pnl=None))     # reopened
    assert await _closed_at(db, t.id, "binance") == (None, None)


async def test_realized_pnl_since_window(db: Database, clock: Clock):
    t1 = await db.create_trade(plan("a"))
    t2 = await db.create_trade(plan("b"))
    t3 = await db.create_trade(plan("c"))
    t4 = await db.create_trade(plan("d"))

    await db.save_broker_position(t1.id, pos("binance", PositionStatus.CLOSED, pnl=-50.0))   # yesterday
    clock.advance(hours=24)
    day_start = clock.now
    clock.advance(hours=1)
    await db.save_broker_position(t2.id, pos("binance", PositionStatus.CLOSED, pnl=-30.0))
    await db.save_broker_position(t2.id, pos("bybit", PositionStatus.CLOSED, pnl=-99.0))     # other broker
    await db.save_broker_position(t3.id, pos("binance", PositionStatus.CLOSED, pnl=12.5))
    await db.save_broker_position(t4.id, pos("binance", PositionStatus.OPEN, pnl=-7.0))      # not closed
    t5 = await db.create_trade(plan("e"))
    await db.save_broker_position(t5.id, pos("binance", PositionStatus.CANCELLED, pnl=None))  # no pnl
    t6 = await db.create_trade(plan("f"))
    # emergency-closed after a failed stop placement: the loss must count
    await db.save_broker_position(t6.id, pos("binance", PositionStatus.FAILED, pnl=-4.0))

    assert await db.realized_pnl_since("binance", day_start) == pytest.approx(-30.0 + 12.5 - 4.0)
    assert await db.realized_pnl_since("binance", day_start - timedelta(days=2)) == pytest.approx(-71.5)
    assert await db.realized_pnl_since("bybit", day_start) == pytest.approx(-99.0)
    assert await db.realized_pnl_since("mt5", day_start) == 0.0
    assert await db.realized_pnl_since("binance", clock.now + timedelta(seconds=1)) == 0.0
    # naive "since" is taken as UTC
    assert await db.realized_pnl_since("binance", day_start.replace(tzinfo=None)) == pytest.approx(-21.5)


async def test_realized_pnl_since_counts_inclusive_boundary(db: Database, clock: Clock):
    t = await db.create_trade(plan())
    await db.save_broker_position(t.id, pos("binance", PositionStatus.CLOSED, pnl=-1.0))
    assert await db.realized_pnl_since("binance", clock.now) == -1.0


# --------------------------------------------------------------------------- #
# Confirmations
# --------------------------------------------------------------------------- #
async def test_confirmation_lifecycle(db: Database):
    c1 = conf("c1", created_at=T0)
    c2 = conf("c2", created_at=T0 - timedelta(minutes=1))
    await db.save_confirmation(c1)
    await db.save_confirmation(c2)
    assert await db.get_confirmation("c1") == c1
    assert await db.get_confirmation("nope") is None
    assert [c.id for c in await db.pending_confirmations()] == ["c2", "c1"]   # oldest first

    # upsert: bot message id + awaiting correction
    c1.bot_message_id = 777
    c1.awaiting_correction = True
    await db.save_confirmation(c1)
    got = await db.get_confirmation("c1")
    assert got is not None and got.bot_message_id == 777 and got.awaiting_correction

    await db.set_confirmation_status("c2", ConfirmationStatus.APPROVED)
    got2 = await db.get_confirmation("c2")
    assert got2 is not None and got2.status is ConfirmationStatus.APPROVED
    assert [c.id for c in await db.pending_confirmations()] == ["c1"]

    await db.set_confirmation_status("c1", ConfirmationStatus.SUPERSEDED)
    assert await db.pending_confirmations() == []

    await db.set_confirmation_status("missing", ConfirmationStatus.EXPIRED)   # no-op, no error

    # saving with a non-pending status keeps it out of the pending list
    await db.save_confirmation(conf("c3", status=ConfirmationStatus.REJECTED))
    assert await db.pending_confirmations() == []


async def test_pending_confirmations_include_expired(db: Database):
    await db.save_confirmation(conf("old", created_at=T0 - timedelta(days=1)))
    assert [c.id for c in await db.pending_confirmations()] == ["old"]


async def test_confirmation_plan_union_roundtrip(db: Database):
    mgmt = ManagementPlan(plan_id="m1", action=ActionType.MOVE_SL, trade_id=3, move_sl_to_entry=True,
                          source_chat_id=CHAT, source_message_ids=[50], created_at=T0)
    c_manage = conf("cm").model_copy(update={"kind": "manage", "plan": mgmt})
    c_open = conf("co")
    c_none = conf("cn", with_plan=False).model_copy(update={"kind": "incomplete"})
    for c in (c_manage, c_open, c_none):
        await db.save_confirmation(c)
    got_m = await db.get_confirmation("cm")
    got_o = await db.get_confirmation("co")
    got_n = await db.get_confirmation("cn")
    assert got_m is not None and isinstance(got_m.plan, ManagementPlan) and got_m.plan == mgmt
    assert got_o is not None and isinstance(got_o.plan, TradePlan) and got_o == c_open
    assert got_n is not None and got_n.plan is None and got_n.kind == "incomplete"
    assert got_o.action.ambiguities == ["Giriş fiyatı net değil"]           # Turkish text intact


# --------------------------------------------------------------------------- #
# State & events
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "value",
    [{"a": 1, "b": [1, 2], "c": {"d": None}}, [1, "x", 2.5, True], True, False, None, 0, 42, "şğüİ", 1.25],
)
async def test_state_roundtrip(db: Database, value):
    await db.set_state("k", value)
    assert await db.get_state("k", default="DEFAULT") == value


async def test_state_default_overwrite_delete_and_invalid(db: Database):
    assert await db.get_state("missing") is None
    assert await db.get_state("missing", default=7) == 7
    await db.set_state("paused", False)
    await db.set_state("paused", True)
    assert await db.get_state("paused") is True
    await db.delete_state("paused")
    assert await db.get_state("paused", "gone") == "gone"
    with pytest.raises(TypeError):
        await db.set_state("bad", object())
    with pytest.raises(ValueError):
        await db.set_state("bad", float("nan"))
    assert await db.get_state("bad", "absent") == "absent"


async def test_log_event_is_permissive_and_ordered(db: Database, clock: Clock):
    await db.log_event("parsed", {"n": 1})
    clock.advance(seconds=1)
    await db.log_event("trade_opened", {"plan": plan(), "side": Side.LONG, "at": T0, "ids": {3}, "obj": object()})
    events = await db.events()
    assert [e["kind"] for e in events] == ["trade_opened", "parsed"]       # newest first
    payload = events[0]["payload"]
    assert payload["plan"]["base_asset"] == "ETH" and payload["side"] == "long"
    assert payload["at"].startswith("2026-11-07T12:00:00") and payload["ids"] == [3]
    assert isinstance(payload["obj"], str)
    assert is_utc(events[0]["ts"]) and events[0]["ts"] == clock.now
    assert [e["kind"] for e in await db.events(kind="parsed")] == ["parsed"]
    assert len(await db.events(limit=1)) == 1


# --------------------------------------------------------------------------- #
# Concurrency / robustness
# --------------------------------------------------------------------------- #
async def test_concurrent_writers(db: Database):
    async def open_trade(i: int) -> int:
        t = await db.create_trade(plan(f"p{i}", msg_ids=[1000 + i, 2000 + i]))
        await db.save_broker_position(t.id, pos("binance", PositionStatus.OPEN))
        await db.update_trade(t.id, status=TradeStatus.OPEN)
        return t.id

    async def store_msg(i: int) -> None:
        await db.save_message(msg(i, text=f"m{i}"))
        await db.mark_consumed(CHAT, [i])
        await db.set_state(f"k{i}", i)
        await db.log_event("e", {"i": i})

    results = await asyncio.gather(*(open_trade(i) for i in range(40)), *(store_msg(i) for i in range(40)))
    ids = [r for r in results if r is not None]
    assert len(set(ids)) == 40
    assert len(await db.active_trades()) == 40
    assert len(await db.active_broker_positions()) == 40
    for i in range(40):
        found = await db.find_trade_by_message(CHAT, 2000 + i)
        assert found is not None and found.plan.plan_id == f"p{i}"
    assert await db.recent_messages(CHAT, T0 - timedelta(hours=1)) == []
    assert len(await db.recent_messages(CHAT, T0 - timedelta(hours=1), only_unconsumed=False, limit=100)) == 40
    assert await db.get_state("k39") == 39
    assert len(await db.events(limit=1000)) == 40


async def test_cancelled_writers_do_not_leak_transactions(db: Database):
    for _ in range(20):
        task = asyncio.create_task(db.create_trade(plan()))
        for _ in range(3):
            await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    assert db._conn is not None and not db._conn.in_transaction
    t = await db.create_trade(plan("after"))
    assert (await db.get_trade(t.id)) is not None
    # atomicity: every stored trade has its message link, and no orphan links exist
    async with db._read() as conn:
        trade_ids = {r[0] for r in await db._fetchall(conn, "SELECT id FROM trades")}
        linked_ids = {r[0] for r in await db._fetchall(conn, "SELECT trade_id FROM trade_messages")}
    assert trade_ids == linked_ids and t.id in trade_ids


async def test_close_lets_queued_writes_finish(clock: Clock):
    database = Database(":memory:", clock=clock)
    await database.open()
    writers = [asyncio.create_task(database.set_state(f"k{i}", i)) for i in range(10)]
    await asyncio.sleep(0)            # every writer is now queued on the lock
    await database.close()
    results = await asyncio.gather(*writers, return_exceptions=True)
    assert results == [None] * 10
    with pytest.raises(RuntimeError):
        await database.set_state("late", 1)


async def test_operation_waiting_on_lock_during_close_fails_cleanly(clock: Clock):
    database = Database(":memory:", clock=clock)
    await database.open()
    closer = asyncio.create_task(database.close())
    await asyncio.sleep(0)            # close() holds the lock now
    late = asyncio.create_task(database.get_state("x"))
    await closer
    with pytest.raises(RuntimeError):
        await late


# --------------------------------------------------------------------------- #
# Forum topics (schema v2)
# --------------------------------------------------------------------------- #
CRYPTO_TOPIC = 3
FX_TOPIC = 7

_V1_MESSAGES = """
CREATE TABLE schema_version (version INTEGER NOT NULL);
INSERT INTO schema_version (version) VALUES (1);
CREATE TABLE messages (
    chat_id              INTEGER NOT NULL,
    message_id           INTEGER NOT NULL,
    sender_id            INTEGER,
    sender_name          TEXT,
    date                 TEXT    NOT NULL,
    text                 TEXT    NOT NULL DEFAULT '',
    media_path           TEXT,
    media_type           TEXT,
    reply_to_message_id  INTEGER,
    grouped_id           INTEGER,
    edited               INTEGER NOT NULL DEFAULT 0,
    consumed             INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (chat_id, message_id)
);
CREATE INDEX ix_messages_chat_date ON messages (chat_id, date);
CREATE TABLE state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO messages (chat_id, message_id, sender_id, date, text, consumed)
    VALUES (-1001234567890, 10, 111, '2026-11-07T12:00:00.000000+00:00', 'eth 1R long', 1);
INSERT INTO state (key, value) VALUES ('last_msg_id:-1001234567890', '10');
"""


def _make_v1_db(path: Path) -> None:
    with closing(sqlite3.connect(path)) as raw:
        raw.executescript(_V1_MESSAGES)
        raw.commit()


async def test_topic_fields_roundtrip(db: Database):
    m = msg(5, text="Eth short\n1R risk", topic_id=CRYPTO_TOPIC, topic_title="💰 Kasım Ayı Kripto İşlemleri")
    await db.save_message(m)
    assert await db.get_message(CHAT, 5) == m
    await db.save_message(msg(6, text="General chatter"))
    got = await db.get_message(CHAT, 6)
    assert got is not None and got.topic_id is None and got.topic_title is None


async def test_topic_kept_on_upsert_without_topic_and_title_updated_on_rename(db: Database):
    await db.save_message(msg(5, text="EUR LONG", topic_id=FX_TOPIC, topic_title="Kasım Ayı Fx İşlemleri"))
    await db.save_message(msg(5, text="EUR LONG 1R Limit Emir", edited=True))        # no topic info
    got = await db.get_message(CHAT, 5)
    assert got is not None and got.topic_id == FX_TOPIC and got.topic_title == "Kasım Ayı Fx İşlemleri"
    await db.save_message(msg(5, text="EUR LONG 1R Limit Emir", topic_id=FX_TOPIC,
                              topic_title="Aralık Ayı Fx İşlemleri"))
    got = await db.get_message(CHAT, 5)
    assert got is not None and got.topic_title == "Aralık Ayı Fx İşlemleri" and got.text == "EUR LONG 1R Limit Emir"


async def test_recent_messages_topic_filter(db: Database):
    await db.save_message(msg(1, date=T0, text="Eth short", topic_id=CRYPTO_TOPIC))
    await db.save_message(msg(2, date=T0 + timedelta(seconds=1), text="EUR LONG", topic_id=FX_TOPIC))
    await db.save_message(msg(3, date=T0 + timedelta(seconds=2), text="general"))
    await db.save_message(msg(4, date=T0 + timedelta(seconds=3), text="Aktif", topic_id=FX_TOPIC, sender_id=OTHER))
    since = T0 - timedelta(minutes=1)
    assert [m.message_id for m in await db.recent_messages(CHAT, since)] == [1, 2, 3, 4]       # None = every topic
    assert [m.message_id for m in await db.recent_messages(CHAT, since, topic_id=CRYPTO_TOPIC)] == [1]
    assert [m.message_id for m in await db.recent_messages(CHAT, since, topic_id=FX_TOPIC)] == [2, 4]
    assert [m.message_id for m in await db.recent_messages(CHAT, since, TRADER, True, 50, FX_TOPIC)] == [2]
    assert await db.recent_messages(CHAT, since, topic_id=999) == []


async def test_migrates_v1_database_in_place(tmp_path: Path):
    path = tmp_path / "copytrader.db"
    _make_v1_db(path)
    async with Database(str(path)) as database:
        old = await database.get_message(CHAT, 10)
        assert old is not None and old.text == "eth 1R long" and old.consumed
        assert old.topic_id is None and old.topic_title is None
        assert await database.get_state(f"last_msg_id:{CHAT}") == 10
        await database.save_message(msg(11, text="Eth short", topic_id=CRYPTO_TOPIC, topic_title="Kripto"))
        await database.create_trade(plan())                                # other tables created as well
    with closing(sqlite3.connect(path)) as raw:
        assert raw.execute("SELECT version FROM schema_version").fetchall() == [(SCHEMA_VERSION,)]
        cols = {r[1] for r in raw.execute("PRAGMA table_info(messages)")}
        assert {"topic_id", "topic_title"} <= cols
    async with Database(str(path)) as database:                           # reopening is a no-op
        got = await database.get_message(CHAT, 11)
        assert got is not None and got.topic_id == CRYPTO_TOPIC and got.topic_title == "Kripto"
        assert [m.message_id for m in await database.recent_messages(CHAT, T0 - timedelta(days=30),
                                                                      only_unconsumed=False,
                                                                      topic_id=CRYPTO_TOPIC)] == [11]


async def test_migration_tolerates_columns_that_already_exist(tmp_path: Path):
    path = tmp_path / "copytrader.db"
    _make_v1_db(path)
    with closing(sqlite3.connect(path)) as raw:                          # e.g. a half-upgraded dev build
        raw.execute("ALTER TABLE messages ADD COLUMN topic_id INTEGER")
        raw.commit()
    async with Database(str(path)) as database:
        await database.save_message(msg(12, topic_id=FX_TOPIC, topic_title="Fx"))
        got = await database.get_message(CHAT, 12)
        assert got is not None and got.topic_id == FX_TOPIC
    with closing(sqlite3.connect(path)) as raw:
        assert raw.execute("SELECT version FROM schema_version").fetchall() == [(SCHEMA_VERSION,)]


def test_schema_version_has_migration_for_every_step():
    assert SCHEMA_VERSION == 2
    assert all(v in dbmod.MIGRATIONS for v in range(2, SCHEMA_VERSION + 1))
