"""MetaApiBroker gaps for the optional-friend-account use: symbol_map, quote, TP edit, retry after a failed
connect, missing SDK message, hung RPC."""

from __future__ import annotations

import asyncio
import builtins
from typing import Any

import pytest

from copytrader.brokers import metaapi_broker as mb
from copytrader.brokers.base import BrokerError
from copytrader.brokers.metaapi_broker import MetaApiBroker
from copytrader.models import PositionStatus, TakeProfit

from test_metaapi_broker import (  # noqa: F401  (fixtures)
    ASK, BID, THREE_TPS, FakeConn, broker, conn, connected, make_cfg, make_plan, make_req,
)


async def test_quote_bid_ask(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    q = await broker.get_quote(sym)
    assert (q.bid, q.ask) == (BID, ASK)


async def test_symbol_map_wins(conn: FakeConn) -> None:
    conn.symbols = ["ETHUSD", "ETHUSD.x"]
    conn.specs["ETHUSD.x"] = {**conn.specs["ETHUSD"], "symbol": "ETHUSD.x"}
    b = await connected(conn, make_cfg(symbol_map={"ETH": "ETHUSD.x"}))
    sym = await b.resolve_symbol("ETH", "USDT")
    assert sym is not None and sym.broker_symbol == "ETHUSD.x"


async def test_symbol_map_missing_symbol_is_clear_error(conn: FakeConn) -> None:
    b = await connected(conn, make_cfg(symbol_map={"ETH": "NOPE"}))
    conn.fail["get_symbol_specification"] = [Exception("not found")]
    with pytest.raises(BrokerError, match="symbol_map"):
        await b.resolve_symbol("ETH")


async def test_update_take_profits_modifies_legs(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS)))
    new = await broker.update_take_profits(pos, [TakeProfit(price=2400.0, close_fraction=1.0)])
    assert new.status is PositionStatus.OPEN
    assert conn.called("modify_position")
    assert 2400.0 in {p["takeProfit"] for p in conn.positions.values()}


async def test_full_close_closes_every_leg(conn: FakeConn, broker: MetaApiBroker) -> None:
    sym = await broker.resolve_symbol("ETH")
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS)))
    new = await broker.close_position(pos, 1.0)
    assert not conn.positions and new.status is PositionStatus.CLOSED


async def test_reconnect_after_failed_connect(conn: FakeConn) -> None:
    """A failed connect (investor password) followed by close() lets the SAME object connect again."""
    from test_metaapi_broker import FakeAccount

    account = FakeAccount(conn)

    class Accounts:
        async def get_account(self, _id: str) -> FakeAccount:
            return account

    class Api:
        def __init__(self, token: str, opts: dict[str, Any]) -> None:
            self.metatrader_account_api = Accounts()

        def close(self) -> None: ...

    conn.info["tradeAllowed"] = False
    b = MetaApiBroker(make_cfg(), api_factory=Api)
    with pytest.raises(BrokerError, match="işlem izni yok"):
        await b.connect()
    await b.close()
    assert b._conn is None
    conn.info["tradeAllowed"] = True
    await b.connect()
    assert await b.get_equity() == 10_000.0


async def test_missing_sdk_gives_turkish_error(monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = builtins.__import__

    def fake_import(name: str, *a: Any, **k: Any) -> Any:
        if name.startswith("metaapi_cloud_sdk"):
            raise ImportError("no module", name=name)
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    b = MetaApiBroker(make_cfg())
    with pytest.raises(BrokerError, match="paketi kurulu değil.*pip install metaapi-cloud-sdk"):
        await b.connect()


async def test_hung_rpc_times_out(conn: FakeConn, broker: MetaApiBroker, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mb, "RPC_TIMEOUT_SEC", 0.05)

    async def hang() -> dict[str, Any]:
        await asyncio.sleep(3600)
        return {}

    conn.get_account_information = hang  # type: ignore[method-assign]
    with pytest.raises(BrokerError, match="yanıt gelmedi"):
        await broker.get_equity()


async def test_empty_credentials_clear_error() -> None:
    from copytrader.config import MetaApiBrokerConfig

    cfg = MetaApiBrokerConfig(type="metaapi", name="tickmill", optional=True)
    with pytest.raises(BrokerError, match="METAAPI_TOKEN"):
        await MetaApiBroker(cfg).connect()
    with pytest.raises(ValueError, match="token ve account_id"):
        MetaApiBrokerConfig(type="metaapi", name="x")
