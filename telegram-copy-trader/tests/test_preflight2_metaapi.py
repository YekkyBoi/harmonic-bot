"""Pre-flight batch 2, MetaApi adapter (the friend's optional Tickmill account).

A1  a CancelledError nobody asked for (SDK 29.x while the terminal is disconnected) never leaves the adapter
A2  an order request without a trade answer: looked up by its tag and adopted, never sent again, and if it is
    not found the owner reads "may have been sent, check the account" instead of "could not open"
A3  requote-type answers of a market order are sent again twice; nothing else is
A4  a connect() that did not finish leaves nothing behind; close() waits for the SDK's scheduled close
A5  ValidationException.details / TooManyRequestsException.metadata reach the error text
A6  a partly filled market order is booked with the volume that was really filled
A7  pure reads give up after 30 s, trade requests and the connect chain keep 120 s
A8  the SDK's latency probe pauses between failed attempts instead of spinning
A9  health_problem()
A10 a price without lossTickValue: the loss per lot is converted with the rate of the conversion pair
    (USDJPY, GBPUSD ...) instead of refusing the trade

The venue is a fake RPC connection. Where the SDK's own behaviour matters (A1) the REAL
``LatencyService`` of metaapi-cloud-sdk gates the fake's requests exactly as ``rpc_request`` does.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import socketio
from metaapi_cloud_sdk.clients.error_handler import TooManyRequestsException, ValidationException
from metaapi_cloud_sdk.clients.metaapi import latency_service as sdk_latency
from metaapi_cloud_sdk.clients.metaapi import metaapi_websocket_client as sdk_ws
from metaapi_cloud_sdk.clients.metaapi.latency_service import LatencyService
from metaapi_cloud_sdk.clients.timeout_exception import TimeoutException

from copytrader.brokers import metaapi_broker as mb
from copytrader.brokers.base import BrokerError
from copytrader.brokers.metaapi_broker import MetaApiBroker, map_error
from copytrader.models import EntryType, PositionStatus

from test_metaapi_broker import THREE_TPS, FakeConn, connected, make_cfg, make_plan, make_req, trade_exc

ACC = "acc-1"
REGION = "new-york"
INSTANCE = f"{ACC}:{REGION}:0:ps-mpa-1"
READS = ["get_account_information", "get_symbols", "get_symbol_specification", "get_symbol_price",
         "calculate_margin", "get_positions", "get_orders", "get_history_orders_by_ticket", "get_deals_by_position"]
TRADES = ["create_market_buy_order", "create_market_sell_order", "create_limit_buy_order", "create_limit_sell_order",
          "create_stop_buy_order", "create_stop_sell_order", "modify_position", "modify_order", "close_position",
          "close_position_partially", "cancel_order"]
LIMIT = dict(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))


# --------------------------------------------------------------------------- #
# fakes
# --------------------------------------------------------------------------- #
async def hang(*_a: Any, **_k: Any) -> Any:
    await asyncio.sleep(3600)


async def stray_cancel(*_a: Any, **_k: Any) -> Any:
    """What an SDK call does while the terminal is disconnected and the shared wait future was cancelled by
    somebody else: it awaits a cancelled future. The calling task itself was NOT cancelled."""
    fut = asyncio.get_running_loop().create_future()
    fut.cancel()
    await fut


class Inner:
    """Stands for the SDK's RpcMetaApiConnection: the flags and the state method the adapter looks at."""

    def __init__(self) -> None:
        self._opened = False
        self._closed = False
        self.synchronized = True

    def is_synchronized(self) -> bool:
        return self.synchronized


class TroubleConn(FakeConn):
    """FakeConn with scripted trouble around the order-creating requests.

    ``fail`` (inherited): raised BEFORE anything happens = an explicit rejection.
    ``after``: raised (or "hang") AFTER the venue executed the request = the answer is lost.
    ``partial``: the next market order fills only this volume and answers TRADE_RETCODE_DONE_PARTIAL.
    ``no_ids``: answers carry no order / position id.
    ``lag``: a new position is missing from the next N get_positions answers.
    """

    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.after: dict[str, list[Any]] = {}
        self.partial: float | None = None
        self.no_ids = False
        self.lag = 0
        self._hidden: dict[str, int] = {}
        self.on_executed: Any = None

    async def _after(self, name: str) -> None:
        if self.on_executed is not None:
            self.on_executed()
        queue = self.after.get(name)
        if queue:
            item = queue.pop(0)
            if item == "hang":
                await asyncio.sleep(3600)
            raise item

    async def _market(self, name: str, buy: bool, symbol: str, volume: float, stop_loss: Any, take_profit: Any,
                      options: Any) -> dict[str, Any]:
        self._rec(name, symbol, volume, stop_loss=stop_loss, take_profit=take_profit, options=options)
        pid = self._id()
        res = self._response(orderId=pid, positionId=pid)
        filled = volume
        if self.partial is not None:
            filled, self.partial = self.partial, None
            res.update(numericCode=10010, stringCode="TRADE_RETCODE_DONE_PARTIAL")
        self._open(pid, symbol, buy, filled, self.prices[symbol]["ask" if buy else "bid"], stop_loss, take_profit,
                   options)
        if self.lag:
            self._hidden[pid] = self.lag
        await self._after(name)
        if self.no_ids:
            res.pop("orderId"), res.pop("positionId")
        return res

    async def _pending(self, name: str, otype: str, symbol: str, volume: float, open_price: float, stop_loss: Any,
                       take_profit: Any, options: Any) -> dict[str, Any]:
        res = await super()._pending(name, otype, symbol, volume, open_price, stop_loss, take_profit, options)
        await self._after(name)
        if self.no_ids:
            res.pop("orderId")
        return res

    async def get_positions(self, options: Any = None) -> list[dict[str, Any]]:
        out = await super().get_positions(options)
        hidden = [pid for pid, n in self._hidden.items() if n > 0]
        for pid in hidden:
            self._hidden[pid] -= 1
        return [p for p in out if p["id"] not in hidden]

    def creates(self) -> list[str]:
        return [n for n, *_ in self.calls if n.startswith("create_")]

    def lookups(self) -> int:
        return len(self.called("get_positions"))


class _Ws:
    """The two attributes LatencyService reads from its websocket client."""

    account_replicas: dict[str, Any] = {}
    accounts_by_replica_id: dict[str, Any] = {}


class SdkGatedConn(TroubleConn):
    """Every request first passes the REAL SDK gate, as MetaApiWebsocketClient.rpc_request does
    (metaapi_websocket_client.py:1618-1622): no connected terminal instance -> await the shared future of
    LatencyService.wait_connected_instance (latency_service.py:187-202)."""

    def __init__(self) -> None:
        super().__init__()
        self.latency = LatencyService(_Ws(), "tok", 60)
        self.latency._latency_cache[REGION] = 1.0                     # skip the latency probe (network)
        self._websocket_client = SimpleNamespace(_latency_service=self.latency)   # where the adapter finds it
        for name in READS + TRADES:
            setattr(self, name, self._gate(getattr(self, name)))

    def _gate(self, orig: Any) -> Any:
        async def call(*a: Any, **k: Any) -> Any:
            if not self.latency.get_active_account_instances(ACC):
                await self.latency.wait_connected_instance(ACC)
            return await orig(*a, **k)
        return call

    def waiting(self) -> dict[str, str]:
        return {k: "cancelled" if f.cancelled() else "done" if f.done() else "pending"
                for k, f in self.latency._wait_connect_promises.items()}


@pytest.fixture
def conn() -> TroubleConn:
    return TroubleConn()


@pytest.fixture
async def broker(conn: TroubleConn, monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr(mb, "LOST_REPLY_DELAY_SEC", 0.0, raising=False)
    b = await connected(conn)
    yield b
    await b.close()


@pytest.fixture
async def gated(monkeypatch: pytest.MonkeyPatch) -> Any:
    """(connection, adapter) with the real SDK gate; the terminal starts connected. Timeouts shortened."""
    monkeypatch.setattr(mb, "RPC_TIMEOUT_SEC", 0.2)
    monkeypatch.setattr(mb, "READ_TIMEOUT_SEC", 0.2, raising=False)
    monkeypatch.setattr(mb, "LOST_REPLY_DELAY_SEC", 0.0, raising=False)
    c = SdkGatedConn()
    await c.latency.on_connected(INSTANCE)
    b = await connected(c)
    yield c, b
    await b.close()
    c.latency.stop()


async def outcome(coro: Any) -> str:
    """How a call ended, seen from a task of its own (so a CancelledError cannot end the test itself)."""
    async def run() -> str:
        try:
            await coro
            return "ok"
        except BrokerError as exc:
            return f"BrokerError: {exc}"
    task = asyncio.ensure_future(run())
    await asyncio.wait({task})
    if task.cancelled():
        return "CancelledError"
    return task.result()


async def eth(broker: MetaApiBroker) -> Any:
    sym = await broker.resolve_symbol("ETH")
    assert sym is not None
    return sym


# --------------------------------------------------------------------------- #
# A1  stray CancelledError
# --------------------------------------------------------------------------- #
async def test_a1_real_sdk_gate_outage_never_raises_cancelled_error(gated: Any) -> None:
    c, b = gated
    sym = await eth(b)
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    c.latency.on_disconnected(INSTANCE)                         # MetaApi: the terminal lost the broker

    first = await outcome(b.sync_position(pos))
    assert first.startswith("BrokerError") and "yanıt gelmedi" in first
    assert c.waiting() == {}                                    # the future our timeout cancelled is gone
    for call in (b.sync_position(pos), b.get_equity(), b.close_position(pos, 1.0),
                 b.open_position(make_req(sym, 0.05, make_plan(), tag="ctoutage001"))):
        res = await outcome(call)
        assert res.startswith("BrokerError"), res               # was: CancelledError in the caller's task
    assert c.creates() == ["create_market_buy_order"]           # nothing was sent during the outage

    await c.latency.on_connected(INSTANCE)
    assert (await b.sync_position(pos)).status is PositionStatus.OPEN
    c.hit(pos.data["legs"][0]["ticket"], "DEAL_REASON_TP", 2310.6)
    assert (await b.sync_position(pos)).status is PositionStatus.CLOSED

    c.latency.on_disconnected(INSTANCE)                         # a second outage starts clean as well
    again = await outcome(b.get_equity())
    assert again.startswith("BrokerError") and "yanıt gelmedi" in again


async def test_a1_real_sdk_gate_caller_that_gives_up_does_not_poison_the_others(gated: Any) -> None:
    """/bakiye (20 s limit in the engine) gives up while the monitor's sync waits on the same SDK future."""
    c, b = gated
    c.latency.on_disconnected(INSTANCE)
    patient = asyncio.ensure_future(outcome(b.get_free_margin()))
    await asyncio.sleep(0.02)
    with pytest.raises(asyncio.TimeoutError):                   # the caller's own timeout stays what it is
        await asyncio.wait_for(b.get_equity(), 0.05)
    res = await patient
    assert res.startswith("BrokerError") and "bağlantı kopuk" in res
    assert c.waiting() == {}
    assert (await outcome(b.get_equity())).startswith("BrokerError")


async def test_a1_real_sdk_gate_loop_like_the_monitor_survives_the_outage(gated: Any) -> None:
    c, b = gated
    sym = await eth(b)
    pos = await b.open_position(make_req(sym, 0.05, make_plan()))
    seen: list[str] = []

    async def loop() -> None:                                   # Monitor.run: CancelledError ends it, the rest is logged
        nonlocal pos
        while len(seen) < 6:
            try:
                pos = await b.sync_position(pos)
                seen.append(pos.status.value)
            except BrokerError:
                seen.append("error")
            await asyncio.sleep(0.01)

    c.latency.on_disconnected(INSTANCE)
    task = asyncio.ensure_future(loop())
    await asyncio.sleep(0.7)                                    # three timed-out passes
    assert not task.done() and seen[:3] == ["error"] * 3
    await c.latency.on_connected(INSTANCE)
    c.hit(pos.data["legs"][0]["ticket"], "DEAL_REASON_TP", 2310.6)
    await asyncio.wait_for(task, 5)
    assert not task.cancelled() and seen[-1] == "closed"


@pytest.mark.parametrize("method", ["get_positions", "get_account_information", "get_symbol_price"])
async def test_a1_stray_cancel_of_a_read_is_a_broker_error(conn: TroubleConn, broker: MetaApiBroker,
                                                           method: str) -> None:
    sym = await eth(broker)
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    setattr(conn, method, stray_cancel)
    calls = {"get_positions": broker.sync_position(pos), "get_account_information": broker.get_equity(),
             "get_symbol_price": broker.get_quote(sym)}
    for name, coro in calls.items():
        if name != method:
            coro.close()
    res = await outcome(calls[method])
    assert res.startswith("BrokerError") and "bağlantı kopuk; yeniden bağlanması bekleniyor" in res


async def test_a1_stray_cancel_of_a_trade_request_is_a_broker_error(conn: TroubleConn,
                                                                    broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    conn.close_position = stray_cancel  # type: ignore[method-assign]
    res = await outcome(broker.close_position(pos, 1.0))
    assert res.startswith("BrokerError") and "bağlantı kopuk" in res
    assert len(conn.positions) == 1


async def test_a1_real_cancellation_still_propagates(conn: TroubleConn, broker: MetaApiBroker) -> None:
    conn.get_account_information = hang  # type: ignore[method-assign]
    with pytest.raises(asyncio.TimeoutError):                   # an outer timeout (the engine's) is not swallowed
        await asyncio.wait_for(broker.get_equity(), 0.05)
    task = asyncio.ensure_future(broker.get_equity())
    await asyncio.sleep(0.01)
    task.cancel()                                               # shutdown
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()


async def test_a1_stray_cancel_in_the_connect_chain_is_a_broker_error() -> None:
    conn = TroubleConn()
    conn.wait_synchronized = stray_cancel  # type: ignore[method-assign]
    api = make_api(conn)
    b = MetaApiBroker(make_cfg(), api_factory=api)
    res = await outcome(b.connect())
    assert res == "BrokerError: MetaApi bağlantısı kopuk; yeniden bağlanması bekleniyor."
    await b.close()


# --------------------------------------------------------------------------- #
# A2  lost trade reply
# --------------------------------------------------------------------------- #
def lost_answer() -> TimeoutException:
    return TimeoutException("MetaApi websocket client request abc of type trade timed out. Please make sure your "
                            "account is connected to broker before retrying your request.")


def assert_unsure(text: str, tag: str) -> None:
    assert "GÖNDERİLMİŞ OLABİLİR" in text and "kontrol edin" in text and tag in text
    assert "ikinci kez gönderilmedi" in text
    assert "açılamadı" not in text and "açılmadı" not in text


@pytest.mark.parametrize("lost", ["sdk_timeout", "closed", "own_timeout", "no_code"])
async def test_a2_market_order_executed_but_answer_lost_is_adopted(
    conn: TroubleConn, broker: MetaApiBroker, monkeypatch: pytest.MonkeyPatch, lost: str
) -> None:
    sym = await eth(broker)
    if lost == "own_timeout":
        monkeypatch.setattr(mb, "RPC_TIMEOUT_SEC", 0.05)
    conn.after["create_market_buy_order"] = [{
        "sdk_timeout": lost_answer(), "closed": Exception("MetaApi connection closed"), "own_timeout": "hang",
        "no_code": KeyError("description"),          # SDK trade(): an answer without any return code
    }[lost]]
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    (p,) = conn.positions.values()
    assert pos.status is PositionStatus.OPEN and pos.error is None
    assert [leg["ticket"] for leg in pos.data["legs"]] == [p["id"]] and pos.filled_quantity == 0.05
    assert (pos.data["legs"][0]["sl"], pos.data["legs"][0]["venue_tp"]) == (2205.11, 2310.6)
    assert conn.creates() == ["create_market_buy_order"]                    # never sent again
    assert any("yanıtı kaybolmuştu" in w and p["id"] in w for w in pos.data["warnings"])


async def test_a2_market_order_without_answer_and_not_on_the_account(conn: TroubleConn,
                                                                     broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    conn.fail["create_market_buy_order"] = [lost_answer()]      # no answer; the venue never got the request
    before = conn.lookups()
    with pytest.raises(BrokerError) as ei:
        await broker.open_position(make_req(sym, 0.05, make_plan(), tag="ctlost000001"))
    assert_unsure(str(ei.value), "ctlost000001")
    assert "durumu belirsiz" in str(ei.value) and "görünmüyor" in str(ei.value)
    assert conn.creates() == ["create_market_buy_order"] and conn.positions == {}
    assert conn.lookups() - before == 1 + mb.LOST_REPLY_CHECKS             # idempotency check + 3 look-ups
    assert broker._lost == {}


async def test_a2_order_shows_up_only_at_the_second_look(conn: TroubleConn, broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    conn.lag = 1
    conn.after["create_market_buy_order"] = [lost_answer()]
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    assert pos.status is PositionStatus.OPEN and pos.error is None and len(conn.positions) == 1
    assert conn.creates() == ["create_market_buy_order"]


async def test_a2_account_unreadable_after_the_lost_answer(conn: TroubleConn, broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    conn.after["create_market_buy_order"] = [lost_answer()]

    def offline() -> None:
        conn.fail["get_positions"] = [Exception("socket closed")] * 5
    conn.on_executed = offline
    before = conn.lookups()
    with pytest.raises(BrokerError) as ei:
        await broker.open_position(make_req(sym, 0.05, make_plan(), tag="ctlost000002"))
    assert_unsure(str(ei.value), "ctlost000002")
    assert "okunamadığı için" in str(ei.value) and "ulaşıp ulaşmadığı doğrulanamadı" in str(ei.value)
    assert conn.lookups() - before == 2                         # idempotency check + ONE failed look-up, no more
    assert conn.creates() == ["create_market_buy_order"]


async def test_a2_three_legs_second_answer_lost_all_three_are_tracked(conn: TroubleConn,
                                                                      broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    conn.after["create_market_buy_order"] = [None, lost_answer()]
    orig = conn._after

    async def second_only(name: str) -> None:                   # answer 1 arrives, answer 2 is lost, answer 3 arrives
        queue = conn.after.get(name)
        if queue and queue[0] is None:
            queue.pop(0)
            return
        await orig(name)
    conn._after = second_only  # type: ignore[method-assign]
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS)))
    assert pos.status is PositionStatus.OPEN and pos.error is None and pos.filled_quantity == 0.10
    assert sorted(leg["ticket"] for leg in pos.data["legs"]) == sorted(conn.positions)
    assert len(conn.creates()) == 3
    assert [leg["tp"] for leg in pos.data["legs"]] == [2260.0, 2290.0, 2310.6]


async def test_a2_three_legs_second_not_found_keeps_the_first_and_stops(conn: TroubleConn,
                                                                        broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    conn.fail["create_market_buy_order"] = [lost_answer()]
    orig_rec = conn._rec
    sent = {"n": 0}

    def rec(name: str, *a: Any, **k: Any) -> None:              # only the 2nd create request goes unanswered
        if name == "create_market_buy_order":
            sent["n"] += 1
            if sent["n"] != 2:
                conn.calls.append((name, a, k))
                return
        orig_rec(name, *a, **k)
    conn._rec = rec  # type: ignore[method-assign]
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS), tag="ctlost000003"))
    assert pos.status is PositionStatus.OPEN and len(pos.data["legs"]) == 1 and len(conn.positions) == 1
    assert pos.error and pos.error.startswith("2/3. bacak durumu belirsiz")
    assert_unsure(pos.error, "ctlost000003-2")
    assert len(conn.creates()) == 2                             # the 3rd leg is not sent after an unknown 2nd


@pytest.mark.parametrize("lost", ["sdk_timeout", "no_ids"])
async def test_a2_pending_order_placed_but_answer_lost_is_adopted(conn: TroubleConn, broker: MetaApiBroker,
                                                                  lost: str) -> None:
    sym = await eth(broker)
    if lost == "no_ids":
        conn.no_ids = True
    else:
        conn.after["create_limit_buy_order"] = [lost_answer()]
    pos = await broker.open_position(make_req(sym, 0.05, make_plan(**LIMIT)))
    (o,) = conn.orders.values()
    (leg,) = pos.data["legs"]
    assert pos.status is PositionStatus.PENDING and pos.error is None
    assert (leg["state"], leg["ticket"], leg["order_ticket"], leg["sl"]) == ("pending", o["id"], o["id"], 2180.0)
    assert conn.creates() == ["create_limit_buy_order"]
    conn.fill(o["id"])                                          # and it is followed like any other pending leg
    assert (await broker.sync_position(pos)).status is PositionStatus.OPEN


async def test_a2_pending_order_without_answer_and_not_on_the_account(conn: TroubleConn,
                                                                      broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    conn.fail["create_limit_buy_order"] = [lost_answer()]
    with pytest.raises(BrokerError) as ei:
        await broker.open_position(make_req(sym, 0.05, make_plan(**LIMIT), tag="ctlost000004"))
    assert_unsure(str(ei.value), "ctlost000004")
    assert conn.creates() == ["create_limit_buy_order"] and conn.orders == {}


async def test_a2_market_answer_without_ids_is_looked_up_not_failed(conn: TroubleConn,
                                                                    broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    conn.no_ids = True
    conn.lag = 2                                                # not visible yet when the answer arrives
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    assert pos.status is PositionStatus.OPEN and pos.error is None
    assert [leg["ticket"] for leg in pos.data["legs"]] == list(conn.positions)
    assert conn.creates() == ["create_market_buy_order"]


async def test_a2_timeout_return_code_is_looked_up_like_a_lost_answer(conn: TroubleConn,
                                                                      broker: MetaApiBroker) -> None:
    """TRADE_RETCODE_TIMEOUT (10012, "request cancelled by timeout"): the one trade answer after which the trade
    server may have executed the request anyway (the native MT5 adapter treats it the same way)."""
    sym = await eth(broker)
    conn.after["create_market_buy_order"] = [trade_exc(10012, "TRADE_RETCODE_TIMEOUT", "Request timed out")]
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))          # executed after all: adopted
    assert pos.status is PositionStatus.OPEN and pos.error is None
    assert [leg["ticket"] for leg in pos.data["legs"]] == list(conn.positions)
    assert conn.creates() == ["create_market_buy_order"]

    conn.fail["create_market_buy_order"] = [trade_exc(10012, "TRADE_RETCODE_TIMEOUT", "Request timed out")]
    with pytest.raises(BrokerError) as ei:                                      # not executed: unknown, not sent again
        await broker.open_position(make_req(sym, 0.05, make_plan(), tag="ctlost000005"))
    assert_unsure(str(ei.value), "ctlost000005")
    assert "zaman aşımı" in str(ei.value) and "10012" in str(ei.value)
    assert len(conn.creates()) == 2 and len(conn.positions) == 1


async def test_a2_explicit_rejection_stays_a_plain_failure(conn: TroubleConn, broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    conn.fail["create_market_buy_order"] = [trade_exc(10019, "TRADE_RETCODE_NO_MONEY", "No money")]
    before = conn.lookups()
    with pytest.raises(BrokerError) as ei:
        await broker.open_position(make_req(sym, 0.05, make_plan()))
    assert "yetersiz bakiye" in str(ei.value) and "OLABİLİR" not in str(ei.value)
    assert conn.lookups() - before == 1                         # only the idempotency check: nothing to look up


# --------------------------------------------------------------------------- #
# A3  requotes
# --------------------------------------------------------------------------- #
REQUOTES = [(10004, "TRADE_RETCODE_REQUOTE"), (10020, "TRADE_RETCODE_PRICE_CHANGED"),
            (10021, "TRADE_RETCODE_PRICE_OFF")]


@pytest.mark.parametrize("code, scode", REQUOTES)
async def test_a3_two_requotes_then_the_order_fills(conn: TroubleConn, broker: MetaApiBroker, code: int,
                                                    scode: str) -> None:
    sym = await eth(broker)
    conn.fail["create_market_buy_order"] = [trade_exc(code, scode)] * 2
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    assert pos.status is PositionStatus.OPEN and pos.error is None and len(conn.positions) == 1
    assert len(conn.creates()) == 3


async def test_a3_third_requote_is_final(conn: TroubleConn, broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    conn.fail["create_market_buy_order"] = [trade_exc(10004, "TRADE_RETCODE_REQUOTE", "Requote")] * 3
    with pytest.raises(BrokerError, match="requote") as ei:
        await broker.open_position(make_req(sym, 0.05, make_plan()))
    assert "OLABİLİR" not in str(ei.value)
    assert len(conn.creates()) == 3 and conn.positions == {}


@pytest.mark.parametrize("code, scode", [(10019, "TRADE_RETCODE_NO_MONEY"), (10018, "TRADE_RETCODE_MARKET_CLOSED"),
                                         (10031, "TRADE_RETCODE_CONNECTION"), (10012, "TRADE_RETCODE_TIMEOUT")])
async def test_a3_other_codes_are_not_sent_again(conn: TroubleConn, broker: MetaApiBroker, code: int,
                                                 scode: str) -> None:
    sym = await eth(broker)
    conn.fail["create_market_buy_order"] = [trade_exc(code, scode)] * 3
    with pytest.raises(BrokerError, match=str(code)):
        await broker.open_position(make_req(sym, 0.05, make_plan()))
    assert len(conn.creates()) == 1


async def test_a3_requote_of_a_pending_order_is_not_sent_again(conn: TroubleConn, broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    conn.fail["create_limit_buy_order"] = [trade_exc(10004, "TRADE_RETCODE_REQUOTE")] * 3
    with pytest.raises(BrokerError, match="requote"):
        await broker.open_position(make_req(sym, 0.05, make_plan(**LIMIT)))
    assert len(conn.creates()) == 1


async def test_a3_requote_then_stops_refused_still_opens_and_protects(conn: TroubleConn,
                                                                      broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    conn.fail["create_market_buy_order"] = [trade_exc(10004, "TRADE_RETCODE_REQUOTE"),
                                            trade_exc(10016, "TRADE_RETCODE_INVALID_STOPS")]
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    (p,) = conn.positions.values()
    assert pos.status is PositionStatus.OPEN and (p["stopLoss"], p["takeProfit"]) == (2205.11, 2310.6)
    assert len(conn.creates()) == 3


# --------------------------------------------------------------------------- #
# A4  half-open clients, close()
# --------------------------------------------------------------------------- #
class SdkLikeConn(TroubleConn):
    """close() only SCHEDULES the real close, like RpcMetaApiConnectionInstance.close() of the SDK."""

    def __init__(self, events: list[str], close_takes: float | None = 0.15) -> None:
        super().__init__()
        self.events = events
        self.close_takes = close_takes                          # None: the scheduled close never finishes
        self._meta_api_connection = Inner()
        self._tasks: list[asyncio.Task[None]] = []

    async def connect(self) -> None:
        await super().connect()
        self._meta_api_connection._opened = True

    async def close(self) -> None:
        await super().close()

        async def real_close() -> None:
            await asyncio.sleep(3600 if self.close_takes is None else self.close_takes)
            self._meta_api_connection._closed = True
            self.events.append("sdk connection closed")
        self._tasks.append(asyncio.ensure_future(real_close()))


def make_api(conn: FakeConn, events: list[str] | None = None) -> Any:
    """api_factory for MetaApiBroker: account already deployed, get_rpc_connection() hands out ``conn``."""
    log = events if events is not None else []

    class Account:
        state = "DEPLOYED"

        async def wait_deployed(self) -> None: ...

        async def wait_connected(self) -> None: ...

        def get_rpc_connection(self) -> FakeConn:
            return conn

    class Accounts:
        async def get_account(self, _id: str) -> Account:
            return Account()

    class Api:
        created: list[Any] = []

        def __init__(self, token: str, opts: dict[str, Any]) -> None:
            self.metatrader_account_api = Accounts()
            self.closed = False
            Api.created.append(self)

        def close(self) -> None:
            self.closed = True
            log.append("api closed")

    return Api


async def test_a4_connect_that_timed_out_leaves_nothing_behind() -> None:
    events: list[str] = []
    conn = SdkLikeConn(events)
    conn.wait_synchronized = hang  # type: ignore[method-assign]
    api = make_api(conn, events)
    b = MetaApiBroker(make_cfg(), api_factory=api)
    for attempt in (1, 2, 3):                                   # the engine: 180 s limit, then close(), every 5 min
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(b.connect(), 0.05)
        await b.close()
        assert len(conn.called("close")) == attempt             # the half-open SDK connection was closed
        assert conn._meta_api_connection._closed is True        # ... and its scheduled close has finished
        assert [a.closed for a in api.created] == [True] * attempt
        assert b._half_open is None and b._conn is None and b._api is None
        conn._meta_api_connection._closed = False
    del conn.wait_synchronized                                  # the terminal is back
    await b.connect()
    assert await b.get_equity() == 10_000.0
    await b.close()


async def test_a4_close_waits_for_the_sdk_before_closing_the_api() -> None:
    events: list[str] = []
    conn = SdkLikeConn(events)
    b = MetaApiBroker(make_cfg(), api_factory=make_api(conn, events))
    await b.connect()
    await b.close()
    assert events == ["sdk connection closed", "api closed"]


async def test_a4_close_wait_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mb, "CLOSE_WAIT_SEC", 0.2, raising=False)
    assert mb.CLOSE_WAIT_SEC == 0.2
    events: list[str] = []
    conn = SdkLikeConn(events, close_takes=None)
    b = MetaApiBroker(make_cfg(), api_factory=make_api(conn, events))
    await b.connect()
    t0 = time.monotonic()
    await b.close()
    assert 0.15 <= time.monotonic() - t0 < 2.0 and events == ["api closed"]
    for t in conn._tasks:
        t.cancel()


def test_a4_close_wait_default_is_about_ten_seconds() -> None:
    assert mb.CLOSE_WAIT_SEC == 10.0


@pytest.mark.parametrize("sdk_close_ends", ["KeyError", "normally"])
async def test_a4_sdk_close_that_outlives_the_wait_leaves_no_unretrieved_exception(
    monkeypatch: pytest.MonkeyPatch, sdk_close_ends: str
) -> None:
    """MetaApi unreachable at shutdown / after a failed connect: the SDK's scheduled close (a task nobody holds,
    rpc_metaapi_connection_instance.py:64) cannot finish within the wait; after api.close() it dies with
    KeyError (metaapi_websocket_client.py:1410 after :681). asyncio logged that as "Task exception was never
    retrieved" on every connect attempt of the outage."""
    import gc
    monkeypatch.setattr(mb, "CLOSE_WAIT_SEC", 0.1, raising=False)
    release = asyncio.Event()

    class SdkInner(Inner):
        async def close(self, instance_id: str) -> None:        # RpcMetaApiConnection.close
            await release.wait()
            if sdk_close_ends == "KeyError":
                raise KeyError("acc-1")
            self._closed = True

    class SdkConn(TroubleConn):
        def __init__(self) -> None:
            super().__init__()
            self._meta_api_connection = SdkInner()

        async def connect(self) -> None:
            await super().connect()
            self._meta_api_connection._opened = True

        async def close(self) -> None:                           # RpcMetaApiConnectionInstance.close
            await super().close()
            asyncio.create_task(self._meta_api_connection.close("instance-1"))

    reported: list[str] = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: reported.append(str(context.get("message"))))
    try:
        events: list[str] = []
        b = MetaApiBroker(make_cfg(), api_factory=make_api(SdkConn(), events))
        await b.connect()
        await b.close()
        assert events == ["api closed"]                         # the wait ran out first
        release.set()                                           # ... and only now the SDK's close task ends
        for _ in range(3):
            await asyncio.sleep(0.01)
            gc.collect()
    finally:
        loop.set_exception_handler(previous)
    assert reported == []


async def test_a4_new_connect_first_removes_what_a_failed_one_left() -> None:
    """Even when nobody called close() after the failed attempt."""
    events: list[str] = []
    conn = SdkLikeConn(events)
    conn.wait_synchronized = hang  # type: ignore[method-assign]
    api = make_api(conn, events)
    b = MetaApiBroker(make_cfg(), api_factory=api)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(b.connect(), 0.05)
    del conn.wait_synchronized
    await b.connect()
    assert [a.closed for a in api.created] == [True, False] and len(conn.called("close")) == 1
    await b.close()


# The REAL SDK, in a process of its own: with the old close() the event loop of that process never comes back, and
# a frozen loop cannot fail a test by itself.
_UNREACHABLE_SCRIPT = r"""
import asyncio, os, sys
import socketio
from metaapi_cloud_sdk import MetaApi
from copytrader.brokers import metaapi_broker as mb
from copytrader.brokers.metaapi_broker import MetaApiBroker
from copytrader.config import MetaApiBrokerConfig

DEAD_URL = "http://127.0.0.1:" + sys.argv[1]            # nothing listens there: connection refused at once
attempts = 0
real_connect = socketio.AsyncClient.connect


async def counting(self, *a, **k):
    global attempts
    attempts += 1
    return await real_connect(self, *a, **k)

socketio.AsyncClient.connect = counting

ACCOUNT = {"_id": "acc-1", "state": "DEPLOYED", "magic": 0, "connectionStatus": "CONNECTED", "region": "new-york",
           "reliability": "regular", "type": "cloud-g2", "name": "tickmill", "login": "123456",
           "server": "Tickmill-Live", "version": 5, "platform": "mt5", "application": "MetaApi",
           "accountReplicas": []}


def factory(token, opts):
    api = MetaApi(token, dict(opts))

    async def get_url_settings(instance_number, region):   # the SDK resolves its server the production way
        return {"url": DEAD_URL, "isSharedClientApi": True}

    async def get_account(_id):                            # the provisioning REST call, answered locally
        return dict(ACCOUNT)

    api._metaapi_websocket_client.get_url_settings = get_url_settings
    api.metatrader_account_api._metatrader_account_client.get_account = get_account
    return api


async def main():
    mb.CLOSE_WAIT_SEC = 1.0
    cfg = MetaApiBrokerConfig(type="metaapi", name="tickmill", token="dummy", account_id="acc-1")
    b = MetaApiBroker(cfg, api_factory=factory)
    sys.stdout = open(os.devnull, "w")                     # the SDK print()s two lines per attempt
    try:
        await asyncio.wait_for(b.connect(), 1.5)           # the engine: 180 s, then close()
    except asyncio.TimeoutError:
        sys.stdout = sys.__stdout__
        print("CONNECT TIMED OUT", flush=True)
    print("ATTEMPTS", attempts, flush=True)
    await b.close()
    print("CLOSED", flush=True)
    for _ in range(20):
        await asyncio.sleep(0.05)
    print("LOOP ALIVE", flush=True)
    os._exit(0)

asyncio.run(main())
"""


@pytest.fixture(scope="module")
def unreachable_run() -> str:
    """Output of _UNREACHABLE_SCRIPT ("FROZEN ..." when its event loop never came back)."""
    port = refused_port()
    root = Path(__file__).resolve().parents[1]
    env = {**os.environ, "PYTHONPATH": str(root)}
    try:
        run = subprocess.run([sys.executable, "-c", _UNREACHABLE_SCRIPT, str(port)], cwd=str(root), env=env,
                             capture_output=True, text=True, timeout=40)
    except subprocess.TimeoutExpired as exc:
        out = (exc.stdout or b"").decode("utf-8", "replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        return f"FROZEN (output so far: {out[-300:]!r})"
    own = [line for line in run.stdout.splitlines() if not line.startswith("[")]      # "[<time>] ...": SDK log lines
    return "\n".join(own[-20:]) + "\n--- stderr:\n" + run.stderr[-1000:]


def test_a4_real_sdk_unreachable_endpoint_connect_timeout_then_close_keeps_the_loop_alive(unreachable_run: str) -> None:
    """MetaApi unreachable while the bot (re)connects. The engine gives up after its limit and calls close(). With
    the SDK's subscribe loops still running, api.close() made them retry without ever yielding
    (metaapi_websocket_client.py:650-659 with 2606 after close() emptied the socket list): the event loop, and with
    it every broker and Telegram, stopped for good."""
    assert not unreachable_run.startswith("FROZEN"), f"the event loop froze after close(): {unreachable_run}"
    assert "CONNECT TIMED OUT" in unreachable_run and "CLOSED" in unreachable_run, unreachable_run
    assert "LOOP ALIVE" in unreachable_run, unreachable_run


def test_a8_real_sdk_unreachable_endpoint_trading_sockets_do_not_spin_while_connecting(unreachable_run: str) -> None:
    """The same outage, the 1.5 s before the engine gives up: the SDK retried the first connection of its two
    trading sockets without any pause (metaapi_websocket_client.py:650-659): about 1 800 attempts and 3 600
    console lines a second on one fully used CPU core, for the whole connect limit of every attempt."""
    line = next((x for x in unreachable_run.splitlines() if x.startswith("ATTEMPTS ")), None)
    assert line is not None, unreachable_run
    assert 2 <= int(line.split()[1]) <= 8, line                 # two sockets, at most one attempt a second each


# --------------------------------------------------------------------------- #
# A5  error detail
# --------------------------------------------------------------------------- #
def test_a5_validation_details_are_shown() -> None:
    exc = ValidationException("Validation failed", [{"parameter": "clientId", "message": "invalid characters"}])
    text = str(map_error("piyasa emri", exc))
    assert text.startswith("MetaApi piyasa emri başarısız: Validation failed")
    assert "'parameter': 'clientId'" in text and "invalid characters" in text


def test_a5_too_many_requests_metadata_is_shown() -> None:
    exc = TooManyRequestsException("Too many requests", {
        "periodInMinutes": 1, "requestsPerPeriodAllowed": 10, "type": "LIMIT_REQUEST_RATE_PER_USER",
        "recommendedRetryTime": "2026-11-12T03:00:00.000Z"})
    text = str(map_error("piyasa emri", exc))
    assert "LIMIT_REQUEST_RATE_PER_USER" in text and "recommendedRetryTime" in text


def test_a5_long_details_are_trimmed_and_plain_errors_unchanged() -> None:
    exc = ValidationException("Validation failed", [{"parameter": f"p{i}", "message": "x" * 50} for i in range(40)])
    text = str(map_error("piyasa emri", exc))
    assert text.endswith("…") and len(text) < 450
    assert str(map_error("fiyat", Exception("boom"))) == "MetaApi fiyat başarısız: boom"
    err = map_error("piyasa emri", trade_exc(10019, "TRADE_RETCODE_NO_MONEY", "No money"))
    assert (err.code, err.string_code) == (10019, "TRADE_RETCODE_NO_MONEY") and "yetersiz bakiye" in str(err)


async def test_a5_owner_sees_what_metaapi_refused(conn: TroubleConn, broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    # Was "clientId": a refused label field (clientId, comment) is now sent once more without it
    # (test_review1_clientid.py); a refusal of any other field is final and the owner reads what was refused.
    conn.fail["create_market_buy_order"] = [
        ValidationException("Validation failed", [{"parameter": "volume", "message": "invalid characters"}])]
    with pytest.raises(BrokerError) as ei:
        await broker.open_position(make_req(sym, 0.05, make_plan()))
    assert "volume" in str(ei.value) and "invalid characters" in str(ei.value)
    assert conn.creates() == ["create_market_buy_order"]


# --------------------------------------------------------------------------- #
# A6  partial fill
# --------------------------------------------------------------------------- #
async def test_a6_partly_filled_market_order_books_the_real_volume(conn: TroubleConn, broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    conn.partial = 0.02
    pos = await broker.open_position(make_req(sym, 0.06, make_plan()))
    (leg,) = pos.data["legs"]
    assert (pos.filled_quantity, pos.remaining_quantity) == (0.02, 0.02)
    assert pos.realized_pnl is None                             # nothing was closed: no "closed part with 0 PnL"
    assert (leg["filled"], leg["open_volume"]) == (0.02, 0.02) and pos.status is PositionStatus.OPEN
    (note,) = [w for w in pos.data["warnings"] if "kısmen" in w]
    assert "0.06 lotun 0.02 lotu açıldı, 0.04 lot açılmadı" in note
    again = await broker.sync_position(pos)
    assert (again.filled_quantity, again.remaining_quantity, again.realized_pnl) == (0.02, 0.02, None)
    closed = await broker.close_position(again, 0.5)            # fractions use what was really filled
    assert [(a[0], a[1]) for a, _ in conn.called("close_position_partially")] == [(leg["ticket"], 0.01)]
    assert closed.remaining_quantity == 0.01


async def test_a6_partly_filled_and_not_readable_yet(conn: TroubleConn, broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    conn.partial = 0.02
    conn.lag = 1                                                # the position is not in the first answer
    pos = await broker.open_position(make_req(sym, 0.06, make_plan()))
    assert any("yalnızca bir kısmını" in w and "kontrol edin" in w for w in pos.data["warnings"])
    pos = await broker.sync_position(pos)
    assert (pos.filled_quantity, pos.remaining_quantity, pos.realized_pnl) == (0.02, 0.02, None)


async def test_a6_full_fill_is_untouched(conn: TroubleConn, broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    pos = await broker.open_position(make_req(sym, 0.06, make_plan()))
    assert (pos.filled_quantity, pos.remaining_quantity) == (0.06, 0.06) and pos.data["warnings"] == []
    assert "partial_fill" not in pos.data["legs"][0] and "unfilled" not in pos.data["legs"][0]


async def test_a6_adopted_position_smaller_than_asked_is_reported(conn: TroubleConn, broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    conn.partial = 0.02
    conn.after["create_market_buy_order"] = [lost_answer()]
    pos = await broker.open_position(make_req(sym, 0.06, make_plan()))
    assert (pos.filled_quantity, pos.remaining_quantity, pos.realized_pnl) == (0.02, 0.02, None)
    assert any("0.04 lot açılmadı" in w for w in pos.data["warnings"])


# --------------------------------------------------------------------------- #
# A7  read timeouts
# --------------------------------------------------------------------------- #
def test_a7_defaults() -> None:
    assert (mb.READ_TIMEOUT_SEC, mb.RPC_TIMEOUT_SEC) == (30.0, 120.0)


async def timed(coro: Any) -> tuple[float, str]:
    t0 = time.monotonic()
    res = await outcome(coro)
    return time.monotonic() - t0, res


@pytest.fixture
async def slow(conn: TroubleConn, broker: MetaApiBroker, monkeypatch: pytest.MonkeyPatch) -> Any:
    """An open position and a pending order; reads may take 0.05 s, trade requests 0.4 s."""
    sym = await eth(broker)
    pos = await broker.open_position(make_req(sym, 0.05, make_plan()))
    pend = await broker.open_position(make_req(sym, 0.05, make_plan(**LIMIT), tag="ctpending01"))
    monkeypatch.setattr(mb, "RPC_TIMEOUT_SEC", 0.4)
    monkeypatch.setattr(mb, "READ_TIMEOUT_SEC", 0.05, raising=False)
    return sym, pos, pend


@pytest.mark.parametrize("method", READS)
async def test_a7_reads_give_up_early(conn: TroubleConn, broker: MetaApiBroker, slow: Any, method: str) -> None:
    sym, pos, _ = slow
    broker._specs.clear()
    broker._symbols = None
    setattr(conn, method, hang)
    call = {
        "get_account_information": lambda: broker.get_equity(),
        "get_symbols": lambda: broker.resolve_symbol("DOGE"),
        "get_symbol_specification": lambda: broker.resolve_symbol("ETH"),
        "get_symbol_price": lambda: broker.get_quote(sym),
        "calculate_margin": lambda: broker._margin_for(sym, pos.side, 0.05, 2232.0),
        "get_positions": lambda: broker.sync_position(pos),
        "get_orders": lambda: broker.sync_position(pos),
        "get_history_orders_by_ticket": lambda: broker._history_order("1"),
        "get_deals_by_position": lambda: broker._deal_summary("1"),
    }[method]
    elapsed, res = await timed(call())
    assert elapsed < 0.3, f"{method} waited {elapsed:.2f}s"
    if method != "calculate_margin":                            # that one degrades to "margin unknown"
        assert res.startswith("BrokerError") and "yanıt gelmedi" in res


@pytest.mark.parametrize("method", ["create_market_buy_order", "create_limit_buy_order", "modify_position",
                                    "modify_order", "close_position", "close_position_partially", "cancel_order"])
async def test_a7_trade_requests_keep_the_long_timeout(conn: TroubleConn, broker: MetaApiBroker, slow: Any,
                                                       method: str) -> None:
    sym, pos, pend = slow
    setattr(conn, method, hang)
    call = {
        "create_market_buy_order": lambda: broker.open_position(make_req(sym, 0.05, make_plan(), tag="ctslow00001")),
        "create_limit_buy_order": lambda: broker.open_position(
            make_req(sym, 0.05, make_plan(**LIMIT), tag="ctslow00002")),
        "modify_position": lambda: broker.update_stop_loss(pos, 2210.0),
        "modify_order": lambda: broker.update_stop_loss(pend, 2185.0),
        "close_position": lambda: broker.close_position(pos, 1.0),
        "close_position_partially": lambda: broker.close_position(pos, 0.5),
        "cancel_order": lambda: broker.cancel_entry(pend),
    }[method]
    elapsed, res = await timed(call())
    assert elapsed >= 0.35, f"{method} gave up after {elapsed:.2f}s"
    assert res.startswith("BrokerError")


async def test_a7_connect_chain_keeps_the_long_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mb, "RPC_TIMEOUT_SEC", 0.4)
    monkeypatch.setattr(mb, "READ_TIMEOUT_SEC", 0.05, raising=False)
    conn = TroubleConn()
    conn.get_account_information = hang  # type: ignore[method-assign]
    b = MetaApiBroker(make_cfg(), connection=conn)
    elapsed, res = await timed(b.connect())
    assert elapsed >= 0.35 and res.startswith("BrokerError") and "yanıt gelmedi" in res


# --------------------------------------------------------------------------- #
# A8  latency probe
# --------------------------------------------------------------------------- #
def refused_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]                               # free, and closed again: connections are refused


async def test_a8_real_sdk_latency_probe_pauses_between_failed_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    """The REAL LatencyService._refresh_latency against a refused local port (MetaApi unreachable)."""
    monkeypatch.setattr(sdk_latency, "socketio", sdk_latency.socketio)      # put the SDK modules back afterwards
    monkeypatch.setattr(sdk_ws, "socketio", sdk_ws.socketio)
    monkeypatch.setattr(mb, "SOCKET_RETRY_PAUSE_SEC", 0.2, raising=False)
    api = make_api(TroubleConn())
    api._metaapi_websocket_client = object()                    # what marks the SDK's own MetaApi object
    b = MetaApiBroker(make_cfg(), api_factory=api)
    await b.connect()                                           # installs the guard, as in production
    await b.close()

    attempts = 0
    orig = socketio.AsyncClient.connect

    async def counting(self: Any, *a: Any, **k: Any) -> Any:
        nonlocal attempts
        attempts += 1
        return await orig(self, *a, **k)
    monkeypatch.setattr(socketio.AsyncClient, "connect", counting)

    class Ws(_Ws):
        async def get_url_settings(self, instance_number: int, region: str) -> dict[str, Any]:
            return {"url": f"http://127.0.0.1:{refused_port()}", "isSharedClientApi": True}

    service = LatencyService(Ws(), "tok", 60)
    job = asyncio.ensure_future(service._refresh_latency(REGION))
    ticks = 0
    t0 = time.monotonic()
    while time.monotonic() - t0 < 0.7:
        await asyncio.sleep(0.01)
        ticks += 1
    job.cancel()
    service.stop()
    assert 2 <= attempts <= 5, f"{attempts} connect attempts of the latency probe in 0.7 s"     # was: thousands
    assert ticks >= 40                                          # and the loop stays free for everybody else


async def test_a8_guard_is_not_installed_for_a_fake_api_and_only_once(monkeypatch: pytest.MonkeyPatch) -> None:
    original = sdk_latency.socketio
    assert sdk_ws.socketio is original is socketio              # (no earlier test left a guard behind)
    monkeypatch.setattr(sdk_latency, "socketio", original)
    monkeypatch.setattr(sdk_ws, "socketio", original)
    b = MetaApiBroker(make_cfg(), api_factory=make_api(TroubleConn()))
    await b.connect()
    await b.close()
    assert sdk_latency.socketio is original and sdk_ws.socketio is original     # a test double is not the SDK
    mb._pause_socket_retries()
    guarded, guarded_ws = sdk_latency.socketio, sdk_ws.socketio
    mb._pause_socket_retries()
    assert sdk_latency.socketio is guarded and guarded is not original
    assert sdk_ws.socketio is guarded_ws and guarded_ws is not original         # the trading sockets as well
    for module in (guarded, guarded_ws):
        assert issubclass(module.AsyncClient, socketio.AsyncClient) and module.Client is socketio.Client
        assert module.AsyncClient is not socketio.AsyncClient and module.exceptions is socketio.exceptions


# --------------------------------------------------------------------------- #
# A9  health_problem
# --------------------------------------------------------------------------- #
@pytest.fixture
async def healthy(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr(mb, "HEALTH_TIMEOUT_SEC", 0.3, raising=False)
    monkeypatch.setattr(mb, "HEALTH_SYNC_GRACE_SEC", 0.1, raising=False)
    conn = TroubleConn()
    conn._meta_api_connection = Inner()         # type: ignore[attr-defined]
    b = await connected(conn)
    yield conn, b
    await b.close()


async def health(b: MetaApiBroker) -> Any:
    """As the heartbeat calls it: through getattr; any exception counts as a problem there."""
    return await getattr(b, "health_problem")()


async def test_a9_healthy_connection_has_no_problem(healthy: Any) -> None:
    conn, b = healthy
    before = len(conn.calls)
    assert await health(b) is None
    assert [n for n, *_ in conn.calls[before:]] == ["get_account_information"]      # one cheap read


async def test_a9_works_without_sdk_internals(conn: TroubleConn, broker: MetaApiBroker) -> None:
    assert await health(broker) is None


async def test_a9_not_connected() -> None:
    b = MetaApiBroker(make_cfg(), connection=TroubleConn())
    assert await health(b) == "MetaApi bağlı değil."


async def test_a9_sdk_reports_the_terminal_as_disconnected(healthy: Any) -> None:
    conn, b = healthy
    conn._meta_api_connection.synchronized = False
    before = len(conn.calls)
    t0 = time.monotonic()
    text = await health(b)
    assert text == "MetaApi bağlantısı kopuk; yeniden bağlanması bekleniyor."
    assert 0.08 <= time.monotonic() - t0 < 0.3 and len(conn.calls) == before        # no read while disconnected


async def test_a9_short_reconnect_is_not_reported(healthy: Any) -> None:
    conn, b = healthy
    conn._meta_api_connection.synchronized = False
    asyncio.get_running_loop().call_later(0.04, setattr, conn._meta_api_connection, "synchronized", True)
    assert await health(b) is None


async def test_a9_read_without_answer(healthy: Any) -> None:
    conn, b = healthy
    conn.get_account_information = hang
    t0 = time.monotonic()
    text = await health(b)
    assert text is not None and "yanıt gelmedi" in text and 0.25 <= time.monotonic() - t0 < 1.0


async def test_a9_stray_cancel_is_a_problem_text_not_an_exception(healthy: Any) -> None:
    conn, b = healthy
    conn.get_account_information = stray_cancel
    task = asyncio.ensure_future(health(b))
    await asyncio.wait({task})
    assert not task.cancelled() and "bağlantı kopuk" in task.result()


async def test_a9_any_other_failure_is_described(healthy: Any) -> None:
    conn, b = healthy
    conn.fail["get_account_information"] = [Exception("This connection has been closed")]
    assert "This connection has been closed" in await health(b)
    conn.info["tradeAllowed"] = False
    assert await health(b) == "MetaApi hesabında işlem izni kapalı görünüyor."


async def test_a9_real_cancellation_propagates(healthy: Any) -> None:
    conn, b = healthy
    conn.get_account_information = hang
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(health(b), 0.05)


async def test_a9_real_sdk_gate_outage_and_recovery(gated: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mb, "HEALTH_TIMEOUT_SEC", 0.2, raising=False)
    c, b = gated
    assert await health(b) is None
    c.latency.on_disconnected(INSTANCE)
    for _ in range(3):                                          # every call, not only the first one
        task = asyncio.ensure_future(health(b))
        await asyncio.wait({task})
        assert not task.cancelled() and "yanıt gelmedi" in task.result()
    await c.latency.on_connected(INSTANCE)
    assert await health(b) is None


# --------------------------------------------------------------------------- #
# A10  sizing without lossTickValue
# --------------------------------------------------------------------------- #
# symbol: (digits, base currency, profit currency, bid, ask); 100 000 units per lot, USD account
FX = {
    "EURUSD": (5, "EUR", "USD", 1.08500, 1.08510),
    "GBPUSD": (5, "GBP", "USD", 1.27000, 1.27012),
    "USDJPY": (3, "USD", "JPY", 155.200, 155.215),
    "USDCHF": (5, "USD", "CHF", 0.88400, 0.88412),
    "USDCAD": (5, "USD", "CAD", 1.39500, 1.39515),
    "GBPJPY": (3, "GBP", "JPY", 197.100, 197.125),
    "EURGBP": (5, "EUR", "GBP", 0.85430, 0.85441),
}
FX_LOT = 100_000.0
RISK = 45.0                                                     # the friend's 1R


def usd_per(ccy: str, side: str = "mid") -> float:
    """USD value of one unit of ``ccy`` from the table's own conversion pair."""
    if ccy == "USD":
        return 1.0
    if "USD" + ccy in FX:
        bid, ask = FX["USD" + ccy][3:]
        return 1.0 / {"bid": bid, "ask": ask, "mid": (bid + ask) / 2}[side]
    bid, ask = FX[ccy + "USD"][3:]
    return {"bid": bid, "ask": ask, "mid": (bid + ask) / 2}[side]


class FxConn(TroubleConn):
    """A forex account. ``tick_values``: what the price carries as lossTickValue ("mid": the value MetaTrader
    reports, here from the mid rate of the conversion pair; "none": the field is missing; "zero" / "null")."""

    def __init__(self, tick_values: str = "mid", suffix: str = "", only: tuple[str, ...] | None = None) -> None:
        super().__init__()
        names = only or tuple(FX)
        self.suffix = suffix
        self.symbols = [n + suffix for n in names]
        self.specs, self.prices = {}, {}
        for n in names:
            digits, base, profit, bid, ask = FX[n]
            tick = 10.0 ** -digits
            self.specs[n + suffix] = {
                "symbol": n + suffix, "tickSize": tick, "point": tick, "digits": digits, "minVolume": 0.01,
                "maxVolume": 100.0, "volumeStep": 0.01, "contractSize": FX_LOT, "baseCurrency": base,
                "profitCurrency": profit, "marginCurrency": base, "priceCalculationMode": "SYMBOL_CALC_MODE_FOREX",
                "tradeMode": "SYMBOL_TRADE_MODE_FULL", "stopsLevel": 0,
                "allowedExpirationModes": ["SYMBOL_EXPIRATION_GTC", "SYMBOL_EXPIRATION_SPECIFIED"],
                "allowedOrderTypes": ["SYMBOL_ORDER_MARKET", "SYMBOL_ORDER_LIMIT", "SYMBOL_ORDER_STOP"],
            }
            price: dict[str, Any] = {"symbol": n + suffix, "bid": bid, "ask": ask}
            if tick_values == "mid":
                price["lossTickValue"] = price["profitTickValue"] = tick * FX_LOT * usd_per(profit)
            elif tick_values != "none":
                price["lossTickValue"] = price["profitTickValue"] = {"zero": 0.0, "null": None}[tick_values]
            self.prices[n + suffix] = price

    def priced(self) -> list[str]:
        return [a[0] for a, _ in self.called("get_symbol_price")]


async def fx_lots(conn: FxConn, base: str, quote: str, stop_pct: float = 0.3) -> tuple[float, float, list[str]]:
    """(lots, estimated loss at the stop, symbols whose price was read) for a 1R long, stop ``stop_pct`` % away."""
    from copytrader.models import MarketType, Side
    b = await connected(conn)
    try:
        sym = await b.resolve_symbol(base, quote, MarketType.FOREX)
        assert sym is not None and sym.broker_symbol == base + quote + conn.suffix
        entry = conn.prices[sym.broker_symbol]["ask"]
        before = len(conn.priced())
        res = await b.size_position(sym, Side.LONG, entry, round(entry * (1 - stop_pct / 100), sym.extra["digits"]), RISK)
        return res.quantity, res.est_loss_at_sl, conn.priced()[before:]
    finally:
        await b.close()


@pytest.mark.parametrize("missing", ["none", "zero", "null"])
@pytest.mark.parametrize("pair", ["USDJPY", "USDCHF", "USDCAD"])
async def test_a10_usd_base_pair_is_sized_the_same_without_loss_tick_value(pair: str, missing: str) -> None:
    """USDJPY & co. on a USD account: the instrument's own price is the conversion rate."""
    base, quote = pair[:3], pair[3:]
    with_field, loss_with, _ = await fx_lots(FxConn("mid"), base, quote)
    lots, loss, priced = await fx_lots(FxConn(missing), base, quote)        # was: BrokerError "tick değeri alınamadı"
    assert with_field > 0.05 and abs(lots - with_field) <= 0.01 + 1e-9      # the same lots within one volume step
    assert loss <= RISK * 1.001 and loss == pytest.approx(loss_with, rel=0.08)
    assert set(priced) == {pair}                                            # no other symbol had to be read


async def test_a10_usdjpy_numbers() -> None:
    """45 USD, stop 0.3 % (0.466 JPY) away: 46 600 JPY per lot = 300.26 USD at 155.200 -> 0.14 lot (0.1499 floored)."""
    lots, loss, _ = await fx_lots(FxConn("none"), "USD", "JPY")
    stop = round(155.215 * 0.997, 3)
    per_lot = (155.215 - stop) * FX_LOT / 155.200               # converted at the bid: the dearer side for a loss
    assert lots == 0.14 and loss == pytest.approx(0.14 * per_lot, rel=1e-9)
    assert (await fx_lots(FxConn("mid"), "USD", "JPY"))[0] in (0.14, 0.15)


@pytest.mark.parametrize("pair, rate_pair", [("GBPJPY", "USDJPY"), ("EURGBP", "GBPUSD")])
async def test_a10_cross_is_converted_with_the_rate_of_its_profit_currency(pair: str, rate_pair: str) -> None:
    base, quote = pair[:3], pair[3:]
    with_field, _, priced = await fx_lots(FxConn("mid"), base, quote)
    assert rate_pair not in priced                              # with the field nothing else is read (as before)
    lots, loss, priced = await fx_lots(FxConn("none"), base, quote)
    assert with_field > 0.05 and abs(lots - with_field) <= 0.01 + 1e-9
    assert rate_pair in priced and loss <= RISK * 1.001
    # the dearer side of the conversion: JPY is bought with USD at the USDJPY bid, GBP at the GBPUSD ask
    bid, ask = FX[pair][3:]
    stop = round(ask * 0.997, FX[pair][0])
    worst = (ask - stop) * FX_LOT * (1 / FX["USDJPY"][3] if quote == "JPY" else FX["GBPUSD"][4])
    assert loss == pytest.approx(lots * worst, rel=1e-9)


async def test_a10_conversion_pair_with_a_broker_suffix() -> None:
    lots, _, priced = await fx_lots(FxConn("none", suffix=".r"), "GBP", "JPY")
    assert lots > 0.05 and "USDJPY.r" in priced
    assert (await fx_lots(FxConn("none", suffix=".r"), "USD", "JPY"))[0] == 0.14


async def test_a10_refused_only_when_the_rate_is_not_available_either() -> None:
    from copytrader.models import MarketType, Side
    conn = FxConn("none", only=("GBPJPY", "EURUSD"))            # the account lists neither USDJPY nor JPYUSD
    b = await connected(conn)
    sym = await b.resolve_symbol("GBP", "JPY", MarketType.FOREX)
    with pytest.raises(BrokerError) as ei:
        await b.size_position(sym, Side.LONG, 197.125, 196.5, RISK)
    text = str(ei.value)
    assert "tick değeri alınamadı" in text and "USDJPY" in text and "JPYUSD" in text and "hesaplanamıyor" in text
    await b.close()

    conn = FxConn("none")
    conn.prices["USDJPY"].update(bid=0.0, ask=0.0)              # listed, but not quoting (market closed)
    b = await connected(conn)
    sym = await b.resolve_symbol("GBP", "JPY", MarketType.FOREX)
    with pytest.raises(BrokerError, match="tick değeri alınamadı"):
        await b.size_position(sym, Side.LONG, 197.125, 196.5, RISK)
    conn.prices["USDJPY"].update(bid=155.2, ask=155.215)
    assert (await b.size_position(sym, Side.LONG, 197.125, 196.5, RISK)).quantity > 0.05      # and works again
    del b._specs["GBPJPY"]["contractSize"]                      # nor is an unknown contract size guessed as 1
    with pytest.raises(BrokerError, match="tick değeri alınamadı"):
        await b.size_position(sym, Side.LONG, 197.125, 196.5, RISK)
    await b.close()


async def test_a10_paths_that_need_no_conversion_are_unchanged() -> None:
    lots, loss, priced = await fx_lots(FxConn("none"), "EUR", "USD")       # profit currency = account currency
    stop = round(1.08510 * 0.997, 5)
    assert lots == 0.13 and loss == pytest.approx(0.13 * (1.08510 - stop) * FX_LOT, rel=1e-9)
    assert set(priced) == {"EURUSD"}
    conn = FxConn("mid")
    conn.prices["USDJPY"]["lossTickValue"] = 2 * conn.prices["USDJPY"]["lossTickValue"]
    assert (await fx_lots(conn, "USD", "JPY"))[0] == 0.07       # a tick value the venue reports is still what counts


async def test_a10_futures_style_contract_without_tick_value_is_not_guessed() -> None:
    """MetaTrader computes futures-type contracts from the tick value, not from the contract size."""
    from copytrader.models import MarketType, Side
    conn = FxConn("none")
    conn.specs["USDJPY"]["priceCalculationMode"] = "SYMBOL_CALC_MODE_EXCH_FUTURES"
    conn.specs["EURUSD"]["priceCalculationMode"] = "SYMBOL_CALC_MODE_FUTURES"
    b = await connected(conn)
    sym = await b.resolve_symbol("USD", "JPY", MarketType.FOREX)
    with pytest.raises(BrokerError, match="tick değeri alınamadı"):
        await b.size_position(sym, Side.LONG, 155.215, 154.75, RISK)
    sym = await b.resolve_symbol("EUR", "USD", MarketType.FOREX)            # profit currency = account currency
    with pytest.raises(BrokerError, match="yalnızca tick değeriyle") as ei:    # was: sized from the contract size
        await b.size_position(sym, Side.LONG, 1.08510, 1.08184, RISK)
    assert "SYMBOL_CALC_MODE_FUTURES" in str(ei.value)
    conn.prices["EURUSD"]["lossTickValue"] = 1.0                            # with a tick value it is sized as ever
    assert (await b.size_position(sym, Side.LONG, 1.08510, 1.08184, RISK)).quantity == 0.13
    await b.close()
