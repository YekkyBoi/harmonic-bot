"""SQLite storage implementing :class:`copytrader.interfaces.Storage`.

Design notes
------------
* One aiosqlite connection in autocommit mode (``isolation_level=None``);
  every multi-statement write runs inside an explicit ``BEGIN IMMEDIATE`` /
  ``COMMIT`` while holding an ``asyncio.Lock``. Reads take the same lock so they
  never observe another coroutine's half-done transaction.
* Datetimes are stored as fixed-width ISO-8601 UTC strings (lexicographically
  sortable) and always returned timezone-aware in UTC. Naive input = UTC.
* File databases use WAL + ``synchronous=FULL`` (trade state must survive a
  power loss). ``":memory:"`` works for tests.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
from collections.abc import AsyncIterator, Callable, Iterable
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import Any

import aiosqlite
from pydantic import BaseModel, TypeAdapter

from ..models import (
    BrokerPosition,
    ConfirmationStatus,
    PendingConfirmation,
    PositionStatus,
    StoredMessage,
    TakeProfit,
    TradePlan,
    TradeRecord,
    TradeStatus,
)

log = logging.getLogger(__name__)

SCHEMA_VERSION = 1

# version -> statements that upgrade from (version - 1). Append, never edit.
MIGRATIONS: dict[int, list[str]] = {}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
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
CREATE INDEX IF NOT EXISTS ix_messages_chat_date ON messages (chat_id, date);

CREATE TABLE IF NOT EXISTS trades (
    id                         INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_json                  TEXT    NOT NULL,
    status                     TEXT    NOT NULL,
    base_asset                 TEXT    NOT NULL,
    side                       TEXT    NOT NULL,
    current_stop_loss          REAL    NOT NULL,
    current_take_profits_json  TEXT    NOT NULL DEFAULT '[]',
    created_at                 TEXT    NOT NULL,
    updated_at                 TEXT    NOT NULL,
    closed_at                  TEXT,
    note                       TEXT
);
CREATE INDEX IF NOT EXISTS ix_trades_status ON trades (status);
CREATE INDEX IF NOT EXISTS ix_trades_created ON trades (created_at);

CREATE TABLE IF NOT EXISTS trade_messages (
    trade_id    INTEGER NOT NULL,
    chat_id     INTEGER NOT NULL,
    message_id  INTEGER NOT NULL,
    PRIMARY KEY (trade_id, chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS ix_trade_messages_msg ON trade_messages (chat_id, message_id);

CREATE TABLE IF NOT EXISTS broker_positions (
    trade_id       INTEGER NOT NULL,
    broker         TEXT    NOT NULL,
    position_json  TEXT    NOT NULL,
    status         TEXT    NOT NULL,
    closed_at      TEXT,
    realized_pnl   REAL,
    updated_at     TEXT    NOT NULL,
    PRIMARY KEY (trade_id, broker)
);
CREATE INDEX IF NOT EXISTS ix_positions_status ON broker_positions (status);
CREATE INDEX IF NOT EXISTS ix_positions_broker_closed ON broker_positions (broker, closed_at);

CREATE TABLE IF NOT EXISTS confirmations (
    id          TEXT PRIMARY KEY,
    json        TEXT NOT NULL,
    status      TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    expires_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_confirmations_status ON confirmations (status);

CREATE TABLE IF NOT EXISTS state (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts            TEXT NOT NULL,
    kind          TEXT NOT NULL,
    payload_json  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_events_ts ON events (ts);
CREATE INDEX IF NOT EXISTS ix_events_kind ON events (kind, id);
"""

_ACTIVE_TRADE = (TradeStatus.OPENING, TradeStatus.PENDING, TradeStatus.OPEN)
_TERMINAL_TRADE = frozenset({TradeStatus.CLOSED, TradeStatus.CANCELLED, TradeStatus.FAILED})
_ACTIVE_POSITION = (PositionStatus.PENDING, PositionStatus.OPEN)
# FAILED is included on purpose: an adapter may emergency-close a filled position
# (no stop could be placed) and report FAILED with a realized loss - the daily
# loss guard must still see it.
_TERMINAL_POSITION = frozenset({PositionStatus.CLOSED, PositionStatus.CANCELLED, PositionStatus.FAILED})

_TP_LIST = TypeAdapter(list[TakeProfit])

# Columns of the messages table, in StoredMessage field order.
_MESSAGE_COLUMNS = (
    "chat_id",
    "message_id",
    "sender_id",
    "sender_name",
    "date",
    "text",
    "media_path",
    "media_type",
    "reply_to_message_id",
    "grouped_id",
    "edited",
    "consumed",
)


# --------------------------------------------------------------------------- #
# Datetime helpers
# --------------------------------------------------------------------------- #
def to_utc(dt: datetime) -> datetime:
    """Aware UTC datetime (``datetime.timezone.utc``); naive input is taken as UTC."""
    if dt.tzinfo is None or dt.utcoffset() is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _dt_to_db(dt: datetime) -> str:
    # Fixed width ("...T12:00:00.000000+00:00") so string comparison == time comparison.
    return to_utc(dt).isoformat(timespec="microseconds")


def _dt_from_db(value: str | None) -> datetime | None:
    if value is None:
        return None
    return to_utc(datetime.fromisoformat(value))


def _aware(value: Any) -> Any:
    """Recursively make every datetime inside ``value`` aware UTC.

    Pydantic models are updated in place (only call on objects we own)."""
    if isinstance(value, datetime):
        return to_utc(value)
    if isinstance(value, BaseModel):
        for name in type(value).model_fields:
            old = getattr(value, name)
            new = _aware(old)
            if new is not old:
                setattr(value, name, new)
        return value
    if isinstance(value, list):
        items = [_aware(v) for v in value]
        return items if any(a is not b for a, b in zip(items, value)) else value
    if isinstance(value, dict):
        out = {k: _aware(v) for k, v in value.items()}
        return out if any(out[k] is not value[k] for k in value) else value
    return value


def _model_to_json(model: BaseModel) -> str:
    """Serialize a copy of ``model`` with all datetimes normalized to UTC."""
    copy = _aware(model.model_copy(deep=True))
    try:
        return copy.model_dump_json()
    except Exception:
        # Never lose broker/trade state because an adapter put an odd object
        # into a free-form dict: store its str() instead.
        log.warning("Non JSON-serializable value in %s; storing str() fallback", type(model).__name__, exc_info=True)
        return copy.model_dump_json(fallback=str)


def _json_default(obj: Any) -> Any:
    """``json.dumps`` default for the audit log: be permissive, never crash."""
    if isinstance(obj, BaseModel):
        return obj.model_dump(mode="json")
    if isinstance(obj, datetime):
        return _dt_to_db(obj)
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, (set, frozenset, tuple)):
        return list(obj)
    return str(obj)


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #
class Database:
    """Async SQLite persistence (see :class:`copytrader.interfaces.Storage`)."""

    def __init__(self, path: str | os.PathLike[str], *, clock: Callable[[], datetime] | None = None) -> None:
        """``path`` = SQLite file; ":memory:" must work (used by tests).

        ``clock`` returns "now" (aware or naive UTC); injectable for tests."""
        self.path = str(path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._conn: aiosqlite.Connection | None = None
        self._lock: asyncio.Lock | None = None

    @property
    def is_memory(self) -> bool:
        return self.path in ("", ":memory:") or self.path.startswith("file::memory:")

    # ---- lifecycle ---------------------------------------------------------
    async def open(self) -> None:
        if self._conn is not None:
            return
        if not self.is_memory:
            Path(self.path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        target = self.path if self.is_memory else str(Path(self.path).expanduser())
        conn = await aiosqlite.connect(target, isolation_level=None, timeout=10.0)
        try:
            conn.row_factory = sqlite3.Row
            await conn.execute("PRAGMA busy_timeout = 10000")
            if not self.is_memory:
                async with conn.execute("PRAGMA journal_mode = WAL") as cur:
                    row = await cur.fetchone()
                if row is None or str(row[0]).lower() != "wal":
                    log.warning("Could not enable WAL for %s (journal_mode=%s)", self.path, row and row[0])
            await conn.execute("PRAGMA synchronous = FULL")
            await self._init_schema(conn)
        except BaseException:
            await conn.close()
            raise
        self._conn = conn
        self._lock = asyncio.Lock()
        log.info("Database open: %s (schema v%d)", self.path, SCHEMA_VERSION)

    async def close(self) -> None:
        conn, lock = self._conn, self._lock
        if conn is None or lock is None:
            return
        # The lock is FIFO: operations queued before close() still complete;
        # later ones fail cleanly in _require()/_guard().
        async with lock:
            if self._conn is not conn:  # closed concurrently
                return
            self._conn = None
            self._lock = None
            await conn.close()

    async def __aenter__(self) -> "Database":
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def _init_schema(self, conn: aiosqlite.Connection) -> None:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            # executescript() would COMMIT implicitly; run statements one by one instead.
            for stmt in _split_sql(_SCHEMA):
                await conn.execute(stmt)
            async with conn.execute("SELECT version FROM schema_version") as cur:
                rows = await cur.fetchall()
            if not rows:
                await conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
            else:
                current = max(int(r[0]) for r in rows)
                if current > SCHEMA_VERSION:
                    raise RuntimeError(
                        f"Database {self.path} has schema v{current}, this program supports v{SCHEMA_VERSION}; "
                        "refusing to open it with an older version"
                    )
                for version in range(current + 1, SCHEMA_VERSION + 1):
                    log.info("Migrating database %s to schema v%d", self.path, version)
                    for stmt in MIGRATIONS.get(version, []):
                        await conn.execute(stmt)
                if current < SCHEMA_VERSION or len(rows) > 1:
                    await conn.execute("DELETE FROM schema_version")
                    await conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
            await conn.execute("COMMIT")
        except BaseException:
            await _rollback_quietly(conn)
            raise

    # ---- internal helpers ------------------------------------------------------
    def _require(self) -> tuple[aiosqlite.Connection, asyncio.Lock]:
        if self._conn is None or self._lock is None:
            raise RuntimeError("Database is not open; call await db.open() first")
        return self._conn, self._lock

    def _now(self) -> datetime:
        return to_utc(self._clock())

    @asynccontextmanager
    async def _write(self) -> AsyncIterator[aiosqlite.Connection]:
        """Exclusive write transaction (lock + BEGIN IMMEDIATE ... COMMIT)."""
        conn, lock = self._require()
        async with lock:
            self._guard(conn)
            try:
                await conn.execute("BEGIN IMMEDIATE")
                yield conn
                await conn.execute("COMMIT")
            except BaseException:
                # Also covers a failed/cancelled COMMIT; a ROLLBACK after a COMMIT
                # that did go through is a harmless "no transaction" error.
                await _rollback_quietly(conn)
                raise

    @asynccontextmanager
    async def _read(self) -> AsyncIterator[aiosqlite.Connection]:
        conn, lock = self._require()
        async with lock:
            self._guard(conn)
            yield conn

    def _guard(self, conn: aiosqlite.Connection) -> None:
        """Called with the lock held: the connection may have been closed while we waited."""
        if self._conn is not conn:
            raise RuntimeError("Database was closed")

    @staticmethod
    async def _fetchall(conn: aiosqlite.Connection, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        async with conn.execute(sql, tuple(params)) as cur:
            return list(await cur.fetchall())

    @staticmethod
    async def _fetchone(conn: aiosqlite.Connection, sql: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
        async with conn.execute(sql, tuple(params)) as cur:
            return await cur.fetchone()

    # ---- source messages ---------------------------------------------------
    async def save_message(self, msg: StoredMessage) -> None:
        """Upsert by (chat_id, message_id).

        On an existing row: text/media are replaced, ``edited`` becomes True if
        the caller says so or the text/media changed, ``consumed`` is never
        cleared (existing OR new - a handled message must not be re-executed).
        The original ``date`` is kept; other metadata is only filled in."""
        values = (
            msg.chat_id,
            msg.message_id,
            msg.sender_id,
            msg.sender_name,
            _dt_to_db(msg.date),
            msg.text or "",
            msg.media_path,
            msg.media_type,
            msg.reply_to_message_id,
            msg.grouped_id,
            int(msg.edited),
            int(msg.consumed),
        )
        sql = f"""
            INSERT INTO messages ({", ".join(_MESSAGE_COLUMNS)})
            VALUES ({", ".join("?" for _ in _MESSAGE_COLUMNS)})
            ON CONFLICT (chat_id, message_id) DO UPDATE SET
                edited = (messages.edited OR excluded.edited
                          OR messages.text IS NOT excluded.text
                          OR messages.media_path IS NOT excluded.media_path
                          OR messages.media_type IS NOT excluded.media_type),
                consumed = (messages.consumed OR excluded.consumed),
                text = excluded.text,
                media_path = excluded.media_path,
                media_type = excluded.media_type,
                sender_id = COALESCE(excluded.sender_id, messages.sender_id),
                sender_name = COALESCE(excluded.sender_name, messages.sender_name),
                reply_to_message_id = COALESCE(excluded.reply_to_message_id, messages.reply_to_message_id),
                grouped_id = COALESCE(excluded.grouped_id, messages.grouped_id)
        """
        async with self._write() as conn:
            await conn.execute(sql, values)

    async def get_message(self, chat_id: int, message_id: int) -> StoredMessage | None:
        async with self._read() as conn:
            row = await self._fetchone(
                conn, "SELECT * FROM messages WHERE chat_id = ? AND message_id = ?", (chat_id, message_id)
            )
        return _row_to_message(row) if row is not None else None

    async def recent_messages(
        self,
        chat_id: int,
        since: datetime,
        sender_id: int | None = None,
        only_unconsumed: bool = True,
        limit: int = 50,
    ) -> list[StoredMessage]:
        """Messages with ``date >= since``, oldest first.

        When more than ``limit`` match, the NEWEST ``limit`` are returned (still
        oldest first) so the latest signal is never cut off."""
        where = ["chat_id = ?", "date >= ?"]
        params: list[Any] = [chat_id, _dt_to_db(since)]
        if sender_id is not None:
            where.append("sender_id = ?")
            params.append(sender_id)
        if only_unconsumed:
            where.append("consumed = 0")
        params.append(int(limit))
        sql = (
            f"SELECT * FROM messages WHERE {' AND '.join(where)} "
            "ORDER BY date DESC, message_id DESC LIMIT ?"
        )
        async with self._read() as conn:
            rows = await self._fetchall(conn, sql, params)
        return [_row_to_message(r) for r in reversed(rows)]

    async def mark_consumed(self, chat_id: int, message_ids: list[int]) -> None:
        ids = list(dict.fromkeys(message_ids))
        if not ids:
            return
        async with self._write() as conn:
            await conn.executemany(
                "UPDATE messages SET consumed = 1 WHERE chat_id = ? AND message_id = ?",
                [(chat_id, mid) for mid in ids],
            )

    # ---- trades --------------------------------------------------------------
    async def create_trade(self, plan: TradePlan, status: TradeStatus = TradeStatus.OPENING) -> TradeRecord:
        status = TradeStatus(status)
        now = _dt_to_db(self._now())
        plan_json = _model_to_json(plan)
        tps_json = _TP_LIST.dump_json(list(plan.take_profits)).decode()
        closed_at = now if status in _TERMINAL_TRADE else None
        async with self._write() as conn:
            async with conn.execute(
                """
                INSERT INTO trades (plan_json, status, base_asset, side, current_stop_loss,
                                    current_take_profits_json, created_at, updated_at, closed_at, note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (plan_json, status.value, plan.base_asset, plan.side.value, float(plan.stop_loss),
                 tps_json, now, now, closed_at),
            ) as cur:
                trade_id = cur.lastrowid
            if trade_id is None:  # pragma: no cover - sqlite always sets it for INSERT
                raise RuntimeError("SQLite did not return the new trade id")
            await conn.executemany(
                "INSERT OR IGNORE INTO trade_messages (trade_id, chat_id, message_id) VALUES (?, ?, ?)",
                [(trade_id, plan.source_chat_id, mid) for mid in plan.source_message_ids],
            )
            record = await self._get_trade(conn, trade_id)
        if record is None:  # pragma: no cover
            raise RuntimeError(f"Trade {trade_id} vanished right after insert")
        log.debug("Trade %d created (%s %s, %s)", trade_id, plan.base_asset, plan.side.value, status.value)
        return record

    async def get_trade(self, trade_id: int) -> TradeRecord | None:
        async with self._read() as conn:
            return await self._get_trade(conn, trade_id)

    async def _get_trade(self, conn: aiosqlite.Connection, trade_id: int) -> TradeRecord | None:
        row = await self._fetchone(conn, "SELECT * FROM trades WHERE id = ?", (trade_id,))
        return _row_to_trade(row) if row is not None else None

    async def find_trade_by_message(self, chat_id: int, message_id: int) -> TradeRecord | None:
        """Most recent trade whose plan.source_message_ids contains ``message_id`` in ``chat_id``."""
        sql = """
            SELECT t.* FROM trades t
            JOIN trade_messages tm ON tm.trade_id = t.id
            WHERE tm.chat_id = ? AND tm.message_id = ?
            ORDER BY t.created_at DESC, t.id DESC
            LIMIT 1
        """
        async with self._read() as conn:
            row = await self._fetchone(conn, sql, (chat_id, message_id))
        return _row_to_trade(row) if row is not None else None

    async def active_trades(self, base_asset: str | None = None) -> list[TradeRecord]:
        """OPENING / PENDING / OPEN trades, newest first; ``base_asset`` is case-insensitive."""
        where = [f"status IN ({', '.join('?' for _ in _ACTIVE_TRADE)})"]
        params: list[Any] = [s.value for s in _ACTIVE_TRADE]
        if base_asset is not None:
            where.append("UPPER(base_asset) = UPPER(?)")
            params.append(base_asset.strip())
        sql = f"SELECT * FROM trades WHERE {' AND '.join(where)} ORDER BY created_at DESC, id DESC"
        async with self._read() as conn:
            rows = await self._fetchall(conn, sql, params)
        return [_row_to_trade(r) for r in rows]

    async def recent_trades(self, since: datetime) -> list[TradeRecord]:
        """Trades CREATED at or after ``since`` (any status), newest first."""
        async with self._read() as conn:
            rows = await self._fetchall(
                conn,
                "SELECT * FROM trades WHERE created_at >= ? ORDER BY created_at DESC, id DESC",
                (_dt_to_db(since),),
            )
        return [_row_to_trade(r) for r in rows]

    async def update_trade(
        self,
        trade_id: int,
        *,
        status: TradeStatus | None = None,
        current_stop_loss: float | None = None,
        current_take_profits: list[Any] | None = None,
        note: str | None = None,
    ) -> TradeRecord:
        """Change the given fields (None = leave as is) and return the updated record.

        A terminal status (CLOSED / CANCELLED / FAILED) sets ``closed_at`` once
        (the first close time is kept); an active status clears it.
        ``current_take_profits`` accepts TakeProfit objects or dicts; ``[]`` clears.
        Raises KeyError if the trade does not exist."""
        sets: list[str] = []
        params: list[Any] = []
        now = _dt_to_db(self._now())
        if status is not None:
            status = TradeStatus(status)
            sets.append("status = ?")
            params.append(status.value)
            if status in _TERMINAL_TRADE:
                sets.append("closed_at = COALESCE(closed_at, ?)")
                params.append(now)
            else:
                sets.append("closed_at = NULL")
        if current_stop_loss is not None:
            sets.append("current_stop_loss = ?")
            params.append(float(current_stop_loss))
        if current_take_profits is not None:
            tps = _TP_LIST.validate_python(
                [tp.model_dump() if isinstance(tp, TakeProfit) else tp for tp in current_take_profits]
            )
            sets.append("current_take_profits_json = ?")
            params.append(_TP_LIST.dump_json(tps).decode())
        if note is not None:
            sets.append("note = ?")
            params.append(note)

        async with self._write() as conn:
            if sets:
                sets.append("updated_at = ?")
                params.append(now)
                async with conn.execute(
                    f"UPDATE trades SET {', '.join(sets)} WHERE id = ?", (*params, trade_id)
                ) as cur:
                    changed = cur.rowcount
                if changed == 0:
                    raise KeyError(f"trade {trade_id} not found")
            record = await self._get_trade(conn, trade_id)
        if record is None:
            raise KeyError(f"trade {trade_id} not found")
        return record

    # ---- broker positions ----------------------------------------------------
    async def save_broker_position(self, trade_id: int, pos: BrokerPosition) -> None:
        """Upsert by (trade_id, pos.broker).

        Terminal statuses (CLOSED / CANCELLED / FAILED) set ``closed_at`` once
        (the first close time is kept); PENDING / OPEN clear it. The
        ``realized_pnl`` column mirrors ``pos.realized_pnl``."""
        status = PositionStatus(pos.status)
        now = _dt_to_db(self._now())
        closed_at = now if status in _TERMINAL_POSITION else None
        pnl = float(pos.realized_pnl) if pos.realized_pnl is not None else None
        sql = """
            INSERT INTO broker_positions (trade_id, broker, position_json, status, closed_at, realized_pnl, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (trade_id, broker) DO UPDATE SET
                position_json = excluded.position_json,
                status = excluded.status,
                closed_at = CASE WHEN excluded.closed_at IS NULL THEN NULL
                                 ELSE COALESCE(broker_positions.closed_at, excluded.closed_at) END,
                realized_pnl = excluded.realized_pnl,
                updated_at = excluded.updated_at
        """
        position_json = _model_to_json(pos)
        async with self._write() as conn:
            await conn.execute(sql, (trade_id, pos.broker, position_json, status.value, closed_at, pnl, now))

    async def broker_positions(self, trade_id: int) -> list[BrokerPosition]:
        """Positions of one trade, in the order they were first saved."""
        async with self._read() as conn:
            rows = await self._fetchall(
                conn, "SELECT position_json FROM broker_positions WHERE trade_id = ? ORDER BY rowid", (trade_id,)
            )
        return [_load_position(r["position_json"]) for r in rows]

    async def active_broker_positions(self) -> list[tuple[int, BrokerPosition]]:
        """(trade_id, position) for PENDING / OPEN positions, oldest trade first."""
        marks = ", ".join("?" for _ in _ACTIVE_POSITION)
        async with self._read() as conn:
            rows = await self._fetchall(
                conn,
                f"SELECT trade_id, position_json FROM broker_positions WHERE status IN ({marks}) "
                "ORDER BY trade_id, rowid",
                [s.value for s in _ACTIVE_POSITION],
            )
        return [(int(r["trade_id"]), _load_position(r["position_json"])) for r in rows]

    async def realized_pnl_since(self, broker: str, since: datetime) -> float:
        """Sum of realized_pnl of ``broker``'s positions with ``closed_at >= since``."""
        async with self._read() as conn:
            row = await self._fetchone(
                conn,
                "SELECT COALESCE(SUM(realized_pnl), 0.0) FROM broker_positions "
                "WHERE broker = ? AND closed_at IS NOT NULL AND closed_at >= ? AND realized_pnl IS NOT NULL",
                (broker, _dt_to_db(since)),
            )
        return float(row[0]) if row is not None else 0.0

    # ---- confirmations ---------------------------------------------------------
    async def save_confirmation(self, conf: PendingConfirmation) -> None:
        """Upsert by id."""
        sql = """
            INSERT INTO confirmations (id, json, status, created_at, expires_at) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                json = excluded.json,
                status = excluded.status,
                created_at = excluded.created_at,
                expires_at = excluded.expires_at
        """
        values = (
            conf.id,
            _model_to_json(conf),
            ConfirmationStatus(conf.status).value,
            _dt_to_db(conf.created_at),
            _dt_to_db(conf.expires_at),
        )
        async with self._write() as conn:
            await conn.execute(sql, values)

    async def get_confirmation(self, conf_id: str) -> PendingConfirmation | None:
        async with self._read() as conn:
            row = await self._fetchone(conn, "SELECT json FROM confirmations WHERE id = ?", (conf_id,))
        return _load_confirmation(row["json"]) if row is not None else None

    async def pending_confirmations(self) -> list[PendingConfirmation]:
        """Status PENDING, oldest first. Expired ones are included (the caller expires them)."""
        async with self._read() as conn:
            rows = await self._fetchall(
                conn,
                "SELECT json FROM confirmations WHERE status = ? ORDER BY created_at, rowid",
                (ConfirmationStatus.PENDING.value,),
            )
        return [_load_confirmation(r["json"]) for r in rows]

    async def set_confirmation_status(self, conf_id: str, status: ConfirmationStatus) -> None:
        """Updates both the status column and the stored model. Unknown id: warning, no-op."""
        status = ConfirmationStatus(status)
        async with self._write() as conn:
            row = await self._fetchone(conn, "SELECT json FROM confirmations WHERE id = ?", (conf_id,))
            if row is None:
                log.warning("set_confirmation_status: unknown confirmation %r", conf_id)
                return
            conf = _load_confirmation(row["json"])
            conf.status = status
            await conn.execute(
                "UPDATE confirmations SET status = ?, json = ? WHERE id = ?",
                (status.value, _model_to_json(conf), conf_id),
            )

    # ---- misc --------------------------------------------------------------------
    async def get_state(self, key: str, default: Any = None) -> Any:
        """Stored value (a stored ``None`` is returned as ``None``), else ``default``."""
        async with self._read() as conn:
            row = await self._fetchone(conn, "SELECT value FROM state WHERE key = ?", (key,))
        if row is None:
            return default
        return json.loads(row["value"])

    async def set_state(self, key: str, value: Any) -> None:
        """JSON-serializable values (raises TypeError/ValueError otherwise)."""
        encoded = json.dumps(value, allow_nan=False)
        async with self._write() as conn:
            await conn.execute(
                "INSERT INTO state (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                (key, encoded),
            )

    async def delete_state(self, key: str) -> None:
        async with self._write() as conn:
            await conn.execute("DELETE FROM state WHERE key = ?", (key,))

    async def log_event(self, kind: str, payload: dict[str, Any]) -> None:
        """Append-only audit log. Non-JSON values are stringified, never rejected."""
        encoded = json.dumps(payload, default=_json_default, ensure_ascii=False)
        async with self._write() as conn:
            await conn.execute(
                "INSERT INTO events (ts, kind, payload_json) VALUES (?, ?, ?)",
                (_dt_to_db(self._now()), kind, encoded),
            )

    async def events(self, kind: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        """Newest audit events first: ``{"id", "ts", "kind", "payload"}`` (for CLI / debugging)."""
        where, params = ("WHERE kind = ?", [kind]) if kind is not None else ("", [])
        async with self._read() as conn:
            rows = await self._fetchall(
                conn, f"SELECT * FROM events {where} ORDER BY id DESC LIMIT ?", [*params, int(limit)]
            )
        return [
            {"id": int(r["id"]), "ts": _dt_from_db(r["ts"]), "kind": r["kind"], "payload": json.loads(r["payload_json"])}
            for r in rows
        ]


# --------------------------------------------------------------------------- #
# Row mapping
# --------------------------------------------------------------------------- #
def _row_to_message(row: sqlite3.Row) -> StoredMessage:
    return StoredMessage(
        chat_id=row["chat_id"],
        message_id=row["message_id"],
        sender_id=row["sender_id"],
        sender_name=row["sender_name"],
        date=_dt_from_db(row["date"]),
        text=row["text"] or "",
        media_path=row["media_path"],
        media_type=row["media_type"],
        reply_to_message_id=row["reply_to_message_id"],
        grouped_id=row["grouped_id"],
        edited=bool(row["edited"]),
        consumed=bool(row["consumed"]),
    )


def _row_to_trade(row: sqlite3.Row) -> TradeRecord:
    plan = _aware(TradePlan.model_validate_json(row["plan_json"]))
    created_at = _dt_from_db(row["created_at"])
    updated_at = _dt_from_db(row["updated_at"])
    assert created_at is not None and updated_at is not None
    return TradeRecord(
        id=int(row["id"]),
        plan=plan,
        status=TradeStatus(row["status"]),
        current_stop_loss=float(row["current_stop_loss"]),
        current_take_profits=_TP_LIST.validate_json(row["current_take_profits_json"] or "[]"),
        created_at=created_at,
        updated_at=updated_at,
        closed_at=_dt_from_db(row["closed_at"]),
        note=row["note"],
    )


def _load_position(raw: str) -> BrokerPosition:
    return _aware(BrokerPosition.model_validate_json(raw))


def _load_confirmation(raw: str) -> PendingConfirmation:
    return _aware(PendingConfirmation.model_validate_json(raw))


def _split_sql(script: str) -> list[str]:
    """Split the static schema script into single statements (no ';' in literals)."""
    return [s.strip() for s in script.split(";") if s.strip()]


async def _rollback_quietly(conn: aiosqlite.Connection) -> None:
    try:
        await conn.execute("ROLLBACK")
    except Exception as exc:  # "no transaction is active" when BEGIN never ran
        log.debug("ROLLBACK skipped: %s", exc)
