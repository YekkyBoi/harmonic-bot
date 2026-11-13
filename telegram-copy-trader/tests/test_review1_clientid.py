"""Review 1, lane "clientid": every tickmill (MetaApi) order failed on the live server.

Live incident, 2026-11-12 16:03 UTC, the first real signal on 62e0719: the fx (native MT5) leg opened; the
tickmill (MetaApi) leg got this answer from the real MetaApi service and the owner was told the order MAY exist:

    Validation failed (5a89b52e6ced4bd0b9d70ff4729554de), check error.details for more information
    [{'parameter': 'clientId', 'value': 'ctd7c454ab04', 'message': 'Invalid value. Value must match required pattern.'}]

C1  The clientId follows MetaApi's documented ${strategyId}_${positionId}_${orderId} format with ASCII letters and
    digits in each part ("CT_d7c454ab04_0", "CT_d7c454ab04L2_0") and is read back as the internal tag wherever the
    adapter reads one (positions, orders): lost-reply adoption, the re-send check, leg matching and the
    unconfirmed-leg look-up keep working. comment + clientId stay within 26 characters.
C2  A refusal MetaApi gives BEFORE it executes a request (ValidationException, UnauthorizedException,
    ForbiddenException, TooManyRequestsException) is final: never "durumu belirsiz ... GÖNDERİLMİŞ OLABİLİR".
    When it names a label field (clientId, comment) the order is sent once more without it.
C3  A STRICT venue: the repo's MetaApi fake refuses a clientId that does not match
    ^[A-Za-z0-9]+_[A-Za-z0-9]+_[A-Za-z0-9]+$ with the SDK's real ValidationException, built by the SDK's own
    MetaApiWebsocketClient._convert_error from a processingError packet shaped like the live answer. Every test of
    this file runs on it (autouse fixture); test_c3_existing_metaapi_scenarios_pass_on_the_strict_venue runs the
    existing MetaApi test files on it (a pytest run whose plugin makes FakeConn strict for the whole run).

This file only uses names that exist in 62e0719 at import time, so it can be run against that version too.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import json
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
from metaapi_cloud_sdk.clients.error_handler import (
    ForbiddenException,
    InternalException,
    NotFoundException,
    TooManyRequestsException,
    UnauthorizedException,
    ValidationException,
)
from metaapi_cloud_sdk.clients.metaapi.metaapi_websocket_client import MetaApiWebsocketClient
from metaapi_cloud_sdk.clients.metaapi.not_connected_exception import NotConnectedException
from metaapi_cloud_sdk.clients.metaapi.not_synchronized_exception import NotSynchronizedException
from metaapi_cloud_sdk.clients.timeout_exception import TimeoutException

import test_metaapi_broker as tmb
from copytrader.brokers import metaapi_broker as mb
from copytrader.brokers.base import BrokerError
from copytrader.brokers.metaapi_broker import MetaApiBroker, map_error
from copytrader.brokers.mt5_broker import leg_tag
from copytrader.config import MetaApiBrokerConfig, RiskConfig
from copytrader.models import EntryType, PositionStatus, Quote, Side, TakeProfit
from test_metaapi_broker import ASK, BID, THREE_TPS, FakeConn, make_plan, make_req
from test_preflight2_metaapi import TroubleConn, lost_answer

TAG = "ctd7c454ab04"                                     # the tag of the live order of 16:03 UTC
LIVE_ERROR_ID = "5a89b52e6ced4bd0b9d70ff4729554de"       # in MetaApi's answer to it
PATTERN_MESSAGE = "Invalid value. Value must match required pattern."
LIVE_ANSWER = (f"Validation failed ({LIVE_ERROR_ID}), check error.details for more information "
               f"[{{'parameter': 'clientId', 'value': '{TAG}', 'message': '{PATTERN_MESSAGE}'}}]")
# What the owner read, after "tickmill: " (62e0719)
LIVE_TEXT = (f"Emir durumu belirsiz: MetaApi piyasa emri başarısız: {LIVE_ANSWER} Emir GÖNDERİLMİŞ OLABİLİR; aynı "
             f"emir ikinci kez gönderilmedi. Hesapta '{TAG}' etiketli emir ya da pozisyon şu an görünmüyor; yine de "
             f"tickmill hesabını MetaTrader'dan kontrol edin.")

# --------------------------------------------------------------------------- #
# the strict venue
# --------------------------------------------------------------------------- #
CLIENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9]+_[A-Za-z0-9]+_[A-Za-z0-9]+$")
OPTION_METHODS = ("create_market_buy_order", "create_market_sell_order", "create_limit_buy_order",
                  "create_limit_sell_order", "create_stop_buy_order", "create_stop_sell_order", "modify_order",
                  "close_position", "close_position_partially")
STRICT: dict[str, list[Any]] = {"checked": [], "refused": []}     # every clientId the venue saw / refused


def sdk_validation_error(parameter: str, value: Any, message: str = PATTERN_MESSAGE) -> ValidationException:
    """What the real SDK raises for MetaApi's answer to a request that failed validation: the 'processingError'
    packet (metaapi_websocket_client.py:569-575) converted by the SDK's own _convert_error (lines 1755-1757)."""
    packet = {"id": 1, "error": "ValidationError", "message": f"Validation failed ({LIVE_ERROR_ID})",
              "details": [{"parameter": parameter, "value": value, "message": message}],
              "requestId": uuid.uuid4().hex}
    client = object.__new__(MetaApiWebsocketClient)      # the ValidationError branch reads nothing of the client
    return client._convert_error(packet)


def _strict(method: Any) -> Any:
    """A FakeConn request method that first does what MetaApi does with the clientId of a trade request."""
    signature = inspect.signature(method)

    @functools.wraps(method)
    async def call(self: Any, *args: Any, **kwargs: Any) -> Any:
        options = signature.bind(self, *args, **kwargs).arguments.get("options")
        if isinstance(options, dict) and "clientId" in options:
            cid = options["clientId"]
            STRICT["checked"].append(cid)
            pattern = getattr(self, "client_id_pattern", CLIENT_ID_PATTERN)
            if not (isinstance(cid, str) and pattern.match(cid)):
                STRICT["refused"].append(cid)
                # MetaApi got the request and executed nothing. Recorded as it was sent: a copy of the options
                # (the SDK copies them into the request at once, metaapi_connection_instance.py:99).
                rec_args = tuple(dict(a) if a is options else a for a in args)
                rec_kwargs = {k: dict(v) if v is options else v for k, v in kwargs.items()}
                self.calls.append((method.__name__, rec_args, rec_kwargs))
                raise sdk_validation_error("clientId", cid)
        return await method(self, *args, **kwargs)

    call.strict = True  # type: ignore[attr-defined]
    return call


def _plain(name: str) -> Any:
    """FakeConn's own method, without strict wrappers."""
    fn = FakeConn.__dict__[name]
    while getattr(fn, "strict", False):
        fn = fn.__wrapped__
    return fn


def strict_methods() -> dict[str, Any]:
    return {name: _strict(_plain(name)) for name in OPTION_METHODS}


def make_strict_for_run() -> None:
    """Used by the plugin of test_c3_existing_...: FakeConn (every subclass included) stays strict for the run."""
    for name, fn in strict_methods().items():
        if not getattr(FakeConn.__dict__[name], "strict", False):
            setattr(FakeConn, name, fn)


@pytest.fixture(autouse=True)
def strict_venue(monkeypatch: pytest.MonkeyPatch) -> None:
    STRICT["checked"].clear()
    STRICT["refused"].clear()
    for name, fn in strict_methods().items():
        monkeypatch.setattr(FakeConn, name, fn)
    monkeypatch.setattr(mb, "LOST_REPLY_DELAY_SEC", 0.0, raising=False)


def tickmill_cfg(**kw: Any) -> MetaApiBrokerConfig:
    return MetaApiBrokerConfig(type="metaapi", name="tickmill", token="tok", account_id="acc-1", **kw)


@pytest.fixture
def venue() -> TroubleConn:
    return TroubleConn()


@pytest.fixture
async def broker(venue: TroubleConn) -> Any:
    b = MetaApiBroker(tickmill_cfg(), connection=venue)
    b.retry_delay_sec = 0
    b.set_risk_config(RiskConfig(include_fees_in_risk=False))
    await b.connect()
    yield b
    await b.close()


async def eth(b: MetaApiBroker) -> Any:
    sym = await b.resolve_symbol("ETH")
    assert sym is not None
    return sym


def sent(venue: FakeConn) -> list[tuple[str, dict[str, Any]]]:
    """(method, options) of every order-creating request the venue got, refused ones included."""
    return [(n, dict(k.get("options") or {})) for n, _a, k in venue.calls if n.startswith("create_")]


async def outcome(coro: Any) -> str:
    try:
        await coro
        return "ok"
    except BrokerError as exc:
        return f"BrokerError: {exc}"


def never_matches(venue: FakeConn) -> None:
    """The venue refuses EVERY clientId (stands for a MetaApi pattern stricter than the documented format)."""
    venue.client_id_pattern = re.compile(r"(?!)")    # type: ignore[attr-defined]


# =========================================================================== #
# C3  the strict venue itself
# =========================================================================== #
async def test_c3_the_strict_venue_answers_the_value_62e0719_sent_like_the_live_service(venue: TroubleConn) -> None:
    opts = {"clientId": TAG, "magic": 770077, "slippage": 20, "comment": "copytrader"}     # 62e0719's options
    with pytest.raises(ValidationException) as ei:
        await venue.create_market_buy_order("ETHUSD", 0.05, 2205.11, 2310.6, options=opts)
    exc = ei.value
    assert str(exc) == f"Validation failed ({LIVE_ERROR_ID}), check error.details for more information"
    assert exc.details == [{"parameter": "clientId", "value": TAG, "message": PATTERN_MESSAGE}]
    assert exc.status_code == 400 and venue.positions == {}
    assert str(map_error("piyasa emri", exc)) == f"MetaApi piyasa emri başarısız: {LIVE_ANSWER}"
    # the documented example, MetaApi's own SDK examples and this bot's new values pass
    for ok in ("RF_EURUSD_GjCy5lk", "TE_GBPUSD_7hyINWqAl", "CT_d7c454ab04_0", "CT_d7c454ab04L2_0"):
        await venue.create_market_buy_order("ETHUSD", 0.01, options={"clientId": ok})
    assert len(venue.positions) == 4 and STRICT["refused"] == [TAG]


async def test_c3_live_signal_opens_on_the_strict_venue(venue: TroubleConn, broker: MetaApiBroker) -> None:
    """The order of 16:03 UTC: one take profit, tag ctd7c454ab04. On 62e0719 this ends with LIVE_TEXT."""
    sym = await eth(broker)
    holder: dict[str, Any] = {}

    async def run() -> None:
        holder["pos"] = await broker.open_position(make_req(sym, 0.05, make_plan(), tag=TAG))
    assert await outcome(run()) == "ok"
    pos = holder["pos"]
    assert pos.status is PositionStatus.OPEN and pos.error is None
    assert sent(venue) == [("create_market_buy_order",
                            {"clientId": "CT_d7c454ab04_0", "magic": 770077, "slippage": 20, "comment": "copytrader"})]
    (p,) = venue.positions.values()
    assert p["clientId"] == "CT_d7c454ab04_0" and pos.data["legs"][0]["tag"] == TAG
    assert STRICT["refused"] == [] and STRICT["checked"] == ["CT_d7c454ab04_0"]


# =========================================================================== #
# C1  format, read-back, every request kind on the strict venue
# =========================================================================== #
def test_c1_client_id_format_and_read_back() -> None:
    enc, dec = mb.encode_client_id, mb.decode_client_id
    assert (enc(TAG), enc(TAG + "-2"), enc(TAG + "-10")) == (
        "CT_d7c454ab04_0", "CT_d7c454ab04L2_0", "CT_d7c454ab04L10_0")
    assert dec("CT_d7c454ab04L2_51234567") == TAG + "-2"          # MetaApi replaced the third part
    assert dec("CT_d7c454ab04_0") == TAG and dec(TAG + "-3") == TAG + "-3" and dec(TAG) == TAG   # 62e0719's form
    for foreign in ("RF_EURUSD_GjCy5lk", "TE_GBPUSD_7hyINWqAlE", "copytrader", "ct_d7_0", "CT_d7-2_0", "", None):
        assert dec(foreign) == "", foreign
    # a tag that would not read back exactly gets no clientId (the comment carries it): never a wrong tag
    assert enc("ctabL2") is None and enc("ct-x") is None and enc("") is None
    # the engine's tags ("ct" + 10 hex characters of a uuid4), every leg count, every leg
    b = MetaApiBroker(tickmill_cfg())
    for _ in range(300):
        tag = "ct" + uuid.uuid4().hex[:10]
        for n in (1, 2, 3, 12):
            for i in range(n):
                t = leg_tag(tag, i, n, MetaApiBroker.tag_limit)
                opts = b._options(t)
                cid = opts["clientId"]
                assert CLIENT_ID_PATTERN.match(cid) and dec(cid) == t and dec(cid[:-1] + "987654") == t
                assert len(cid) + len(opts.get("comment", "")) <= 26
                assert opts["magic"] == 770077 and opts["slippage"] == 20


async def test_c1_tags_are_read_back_from_positions_and_orders(venue: TroubleConn, broker: MetaApiBroker) -> None:
    def put(pid: str, cid: Any, magic: int = 770077, comment: Any = None) -> None:
        venue.positions[pid] = {"id": pid, "type": "POSITION_TYPE_BUY", "symbol": "ETHUSD", "magic": magic,
                                "openPrice": ASK, "volume": 0.01, "stopLoss": 2200.0, "takeProfit": None,
                                "clientId": cid, "comment": comment, "realizedProfit": 0.0}
    put("1", "CT_d7c454ab04L2_51234567")       # a stop loss / take profit fill: MetaApi replaced the third part
    put("2", "CT_d7c454ab04_0")
    put("3", TAG + "-3")                        # 62e0719's form: exactly a tag of this bot
    put("4", "RF_EURUSD_GjCy5lk")               # another application's clientId
    put("5", "CT_d7c454ab04_0", magic=0)        # not this bot's magic
    put("6", None, comment=TAG + "-4")          # sent without clientId (C2): the comment carries the tag
    put("7", None, comment="copytrader")
    venue.orders["8"] = {"id": "8", "type": "ORDER_TYPE_BUY_LIMIT", "symbol": "ETHUSD", "volume": 0.01,
                         "currentVolume": 0.01, "openPrice": 2200.0, "stopLoss": 2180.0, "takeProfit": None,
                         "magic": 770077, "clientId": "CT_d7c454ab04L5_0", "state": "ORDER_STATE_PLACED"}
    positions, orders = await broker._snapshot("ETHUSD")
    assert {k: v["tag"] for k, v in positions.items()} == {
        "1": TAG + "-2", "2": TAG, "3": TAG + "-3", "4": "", "5": "", "6": TAG + "-4", "7": ""}
    assert orders["8"]["tag"] == TAG + "-5"


async def test_c1_three_take_profits_open_and_are_managed(venue: TroubleConn, broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS), tag=TAG))
    assert [o["clientId"] for _, o in sent(venue)] == ["CT_d7c454ab04L1_0", "CT_d7c454ab04L2_0", "CT_d7c454ab04L3_0"]
    assert all(len(o["clientId"]) + len(o.get("comment", "")) <= 26 for _, o in sent(venue))
    assert [leg["tag"] for leg in pos.data["legs"]] == [TAG + "-1", TAG + "-2", TAG + "-3"]
    assert pos.status is PositionStatus.OPEN and pos.error is None and pos.filled_quantity == pytest.approx(0.10)
    pos = await broker.update_stop_loss(pos, 2220.0)                                  # stop moved
    assert {p["stopLoss"] for p in venue.positions.values()} == {2220.0}
    new_tps = [TakeProfit(price=2265.0, close_fraction=1 / 3), TakeProfit(price=2295.0, close_fraction=1 / 3),
               TakeProfit(price=2315.0, close_fraction=1 / 3)]
    pos = await broker.update_take_profits(pos, new_tps)                              # targets moved
    assert sorted(p["takeProfit"] for p in venue.positions.values()) == [2265.0, 2295.0, 2315.0]
    first = pos.data["legs"][0]["ticket"]
    venue.hit(first, "DEAL_REASON_TP", 2265.0)
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(0.07)
    pos = await broker.close_position(pos, 0.5)                                       # partial close
    assert venue.called("close_position_partially") or venue.called("close_position")
    pos = await broker.close_position(pos)                                            # full close
    assert pos.status is PositionStatus.CLOSED and venue.positions == {}
    assert STRICT["refused"] == [] and len(STRICT["checked"]) == 3


async def test_c1_limit_entry_rests_fills_is_managed_and_cancelled(venue: TroubleConn, broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    pos = await broker.open_position(make_req(sym, 0.05, plan, tag=TAG))
    ((name, opts),) = sent(venue)
    assert name == "create_limit_buy_order" and opts["clientId"] == "CT_d7c454ab04_0"
    assert opts["expiration"]["type"] == "ORDER_TIME_SPECIFIED"
    assert pos.status is PositionStatus.PENDING
    pos = await broker.update_stop_loss(pos, 2190.0)                                  # modify_order
    ((args, kw),) = venue.called("modify_order")
    assert kw["stop_loss"] == 2190.0
    venue.fill(pos.data["legs"][0]["order_ticket"])
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN
    (p,) = venue.positions.values()
    assert p["clientId"] == "CT_d7c454ab04_0"
    pos = await broker.close_position(pos)
    assert pos.status is PositionStatus.CLOSED
    other = await broker.open_position(make_req(sym, 0.05, plan, tag="ct0123456789"))
    other = await broker.cancel_entry(other)
    assert other.status is PositionStatus.CANCELLED and len(venue.called("cancel_order")) == 1
    assert STRICT["refused"] == [] and STRICT["checked"] == ["CT_d7c454ab04_0", "CT_0123456789_0"]


async def test_c1_stop_entry_orders_carry_a_valid_client_id(venue: TroubleConn, broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    quote = Quote(bid=BID, ask=ASK, last=(BID + ASK) / 2)
    buy = await broker._open_pending_leg(sym, Side.LONG, 0.05, 2250.0, 2230.0, 2300.0, TAG + "-2", quote)
    sell = await broker._open_pending_leg(sym, Side.SHORT, 0.05, 2220.0, 2240.0, 2200.0, TAG + "-3", quote)
    assert [(n, o["clientId"]) for n, o in sent(venue)] == [
        ("create_stop_buy_order", "CT_d7c454ab04L2_0"), ("create_stop_sell_order", "CT_d7c454ab04L3_0")]
    assert (buy["order_type"], sell["order_type"]) == ("BUY_STOP", "SELL_STOP")
    _, orders = await broker._snapshot("ETHUSD")
    assert (orders[buy["order_ticket"]]["tag"], orders[sell["order_ticket"]]["tag"]) == (TAG + "-2", TAG + "-3")


async def test_c1_order_whose_answer_was_lost_is_found_by_its_client_id(venue: TroubleConn,
                                                                         broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    venue.after["create_market_buy_order"] = [lost_answer()]       # executed; the answer never arrives
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS), tag=TAG))
    assert pos.status is PositionStatus.OPEN and pos.error is None and len(venue.positions) == 3
    assert len(venue.creates()) == 3                                # the lost one was never sent again
    assert any("yanıtı kaybolmuştu" in w for w in pos.data["warnings"])
    assert STRICT["refused"] == []


async def test_c1_a_repeated_request_finds_the_orders_already_on_the_account(venue: TroubleConn,
                                                                             broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS), tag=TAG))
    again = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS), tag=TAG))
    assert again.error == f"Bu işlem etiketiyle ({TAG}) açılmış emir/pozisyon zaten var; tekrar gönderilmedi."
    assert len(venue.creates()) == 3
    assert [leg["tag"] for leg in again.data["legs"]] == [TAG + "-1", TAG + "-2", TAG + "-3"]


async def test_c1_unconfirmed_leg_is_found_by_its_client_id(venue: TroubleConn, broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    pos = await broker.open_position(make_req(sym, 0.05, make_plan(), tag=TAG))
    (leg,) = pos.data["legs"]
    real = leg["ticket"]
    leg.update(ticket="999999", unconfirmed=True)       # tracked under a ticket the account does not list
    positions, _ = await broker._snapshot("ETHUSD")
    await broker._refresh_open(leg, positions)
    assert leg["ticket"] == real and leg["state"] == "open"


# =========================================================================== #
# C2  refusals before execution are final; a refused label is dropped once
# =========================================================================== #
async def test_c2_a_refused_client_id_is_sent_once_more_without_it(venue: TroubleConn, broker: MetaApiBroker) -> None:
    never_matches(venue)
    sym = await eth(broker)
    pos = await broker.open_position(make_req(sym, 0.05, make_plan(), tag=TAG))
    assert [o for _, o in sent(venue)] == [
        {"clientId": "CT_d7c454ab04_0", "magic": 770077, "slippage": 20, "comment": "copytrader"},
        {"magic": 770077, "slippage": 20, "comment": TAG},             # the comment carries the tag
    ]
    assert pos.status is PositionStatus.OPEN and pos.error is None and len(venue.positions) == 1
    (leg,) = pos.data["legs"]
    assert leg["tag"] == TAG and leg["sl"] == 2205.11
    assert (f"MetaApi emir etiketini (clientId) kabul etmedi; emir etiketsiz gönderildi (#{leg['ticket']}). Bot "
            f"bu emri MetaApi'nin verdiği bu numarayla takip ediyor.") in pos.data["warnings"]
    again = await broker.open_position(make_req(sym, 0.05, make_plan(), tag=TAG))    # found by its comment
    assert again.error and "zaten var" in again.error and len(venue.creates()) == 2
    pos = await broker.update_stop_loss(pos, 2220.0)
    pos = await broker.close_position(pos)
    assert pos.status is PositionStatus.CLOSED and venue.positions == {}


async def test_c2_three_legs_without_labels_get_one_note(venue: TroubleConn, broker: MetaApiBroker) -> None:
    never_matches(venue)
    sym = await eth(broker)
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS), tag=TAG))
    assert len(venue.creates()) == 6 and len(venue.positions) == 3            # each leg: refused, then sent
    assert [leg["tag"] for leg in pos.data["legs"]] == [TAG + "-1", TAG + "-2", TAG + "-3"]
    tickets = ", ".join(f"#{leg['ticket']}" for leg in pos.data["legs"])
    notes = [w for w in pos.data["warnings"] if "etiketsiz" in w]
    assert notes == [f"MetaApi emir etiketini (clientId) kabul etmedi; 3 emir etiketsiz gönderildi ({tickets}). "
                     f"Bot bu emirleri MetaApi'nin verdiği bu numaralarla takip ediyor."]
    assert pos.status is PositionStatus.OPEN and pos.error is None


async def test_c2_pending_entry_without_label_is_tracked_by_its_order_number(venue: TroubleConn,
                                                                            broker: MetaApiBroker) -> None:
    never_matches(venue)
    sym = await eth(broker)
    plan = make_plan(entry_type=EntryType.LIMIT, entry=2200.0, sl=2180.0, tps=((2300.0, 1.0),))
    pos = await broker.open_position(make_req(sym, 0.05, plan, tag=TAG))
    assert [("clientId" in o, o.get("comment")) for _, o in sent(venue)] == [(True, "copytrader"), (False, TAG)]
    assert pos.status is PositionStatus.PENDING and pos.error is None
    venue.fill(pos.data["legs"][0]["order_ticket"])
    pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.OPEN and pos.avg_entry_price == pytest.approx(2200.0)


async def test_c2_unlabelled_order_whose_answer_is_lost_is_found_by_its_comment(venue: TroubleConn,
                                                                               broker: MetaApiBroker) -> None:
    never_matches(venue)
    sym = await eth(broker)
    venue.after["create_market_buy_order"] = [lost_answer()]       # the repeat is executed, its answer lost
    pos = await broker.open_position(make_req(sym, 0.05, make_plan(), tag=TAG))
    assert pos.status is PositionStatus.OPEN and pos.error is None and len(venue.positions) == 1
    assert len(venue.creates()) == 2
    assert any("yanıtı kaybolmuştu" in w for w in pos.data["warnings"])


async def test_c2_a_refusal_of_another_field_is_final_and_said_plainly(venue: TroubleConn,
                                                                       broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    venue.fail["create_market_buy_order"] = [sdk_validation_error("volume", 0.05, "Invalid volume")]
    before = venue.lookups()
    res = await outcome(broker.open_position(make_req(sym, 0.05, make_plan(), tag=TAG)))
    assert res == ("BrokerError: MetaApi emri reddetti (geçersiz alan: volume – Invalid volume); bu hesapta işlem "
                   "açılmadı.")
    assert len(venue.creates()) == 1 and venue.positions == {}
    assert venue.lookups() - before == 1                  # the re-send check only: nothing to look for afterwards
    assert broker._lost == {} and broker._refused == {}


async def test_c2_a_second_refusal_after_dropping_the_label_is_final(venue: TroubleConn,
                                                                     broker: MetaApiBroker) -> None:
    never_matches(venue)
    sym = await eth(broker)
    venue.fail["create_market_buy_order"] = [sdk_validation_error("comment", TAG, "Invalid value.")]
    res = await outcome(broker.open_position(make_req(sym, 0.05, make_plan(), tag=TAG)))
    assert res == ("BrokerError: MetaApi emri reddetti (geçersiz alan: comment – Invalid value.); bu hesapta işlem "
                   "açılmadı.")
    assert len(venue.creates()) == 2 and venue.positions == {}          # never a third request


def too_many() -> TooManyRequestsException:
    return TooManyRequestsException("Too many requests", {
        "periodInMinutes": 1, "requestsPerPeriodAllowed": 10, "type": "LIMIT_REQUEST_RATE_PER_USER",
        "recommendedRetryTime": "2026-11-12T16:04:00.000Z"})


@pytest.mark.parametrize("make, reason", [
    (too_many, "MetaApi istek sınırı aşıldı, kısa sürede çok fazla istek gönderildi"),
    (lambda: UnauthorizedException("Invalid auth token"),
     "MetaApi erişim anahtarı (METAAPI_TOKEN) kabul edilmedi – Invalid auth token"),
    (lambda: ForbiddenException("Trading is not allowed"),
     "MetaApi bu hesapta bu işleme izin vermiyor – Trading is not allowed"),
])
async def test_c2_other_refusals_before_execution_are_final(venue: TroubleConn, broker: MetaApiBroker, make: Any,
                                                            reason: str) -> None:
    sym = await eth(broker)
    venue.fail["create_market_buy_order"] = [make()]
    before = venue.lookups()
    res = await outcome(broker.open_position(make_req(sym, 0.05, make_plan(), tag=TAG)))
    assert res == f"BrokerError: MetaApi emri reddetti ({reason}); bu hesapta işlem açılmadı."
    assert len(venue.creates()) == 1 and venue.lookups() - before == 1


@pytest.mark.parametrize("make", [
    lambda: TimeoutException("MetaApi websocket client request abc of type trade timed out."),
    lambda: NotSynchronizedException("not synchronized"),
    lambda: NotConnectedException("account not connected"),
    lambda: InternalException("internal error"),
    lambda: NotFoundException("not found"),
])
async def test_c2_answers_that_leave_the_outcome_unknown_stay_unsure(venue: TroubleConn, broker: MetaApiBroker,
                                                                     make: Any) -> None:
    """Guard (passes on 62e0719 too): these may come after the terminal executed the order."""
    sym = await eth(broker)
    venue.fail["create_market_buy_order"] = [make()]
    res = await outcome(broker.open_position(make_req(sym, 0.05, make_plan(), tag=TAG)))
    assert res.startswith("BrokerError: Emir durumu belirsiz: ") and "GÖNDERİLMİŞ OLABİLİR" in res
    assert len(venue.creates()) == 1


async def test_c2_first_of_three_legs_refused_says_nothing_opened(venue: TroubleConn,
                                                                   broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    venue.fail["create_market_buy_order"] = [too_many()]
    res = await outcome(broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS), tag=TAG)))
    assert res == ("BrokerError: MetaApi emri reddetti (MetaApi istek sınırı aşıldı, kısa sürede çok fazla istek "
                   "gönderildi); bu hesapta işlem açılmadı.")
    assert len(venue.creates()) == 1 and venue.positions == {}


async def test_c2_a_refusal_reason_never_carries_a_token_and_a_url_is_not_a_refused_field(
    venue: TroubleConn, broker: MetaApiBroker
) -> None:
    sym = await eth(broker)
    venue.fail["create_market_buy_order"] = [
        UnauthorizedException("token expired for wss://x/ws?auth-token=SECRET123&clientId=0.42")]
    res = await outcome(broker.open_position(make_req(sym, 0.05, make_plan(), tag=TAG)))
    assert "SECRET123" not in res and "auth-token=***" in res
    assert res.startswith("BrokerError: MetaApi emri reddetti (MetaApi erişim anahtarı (METAAPI_TOKEN) kabul "
                          "edilmedi – token expired") and res.endswith("; bu hesapta işlem açılmadı.")
    assert len(venue.creates()) == 1                    # "clientId=" in a URL is no refused field: no repeat


async def test_c2_a_later_leg_refused_keeps_the_first_and_never_says_nothing_opened(venue: TroubleConn,
                                                                                    broker: MetaApiBroker) -> None:
    sym = await eth(broker)
    orig = venue._market
    count = {"n": 0}

    async def second_refused(name: str, *a: Any, **k: Any) -> Any:
        count["n"] += 1
        if count["n"] == 2:
            venue.calls.append((name, a, k))
            raise too_many()
        return await orig(name, *a, **k)
    venue._market = second_refused  # type: ignore[method-assign]
    pos = await broker.open_position(make_req(sym, 0.10, make_plan(tps=THREE_TPS), tag=TAG))
    assert pos.status is PositionStatus.OPEN and len(pos.data["legs"]) == 1 and len(venue.positions) == 1
    assert pos.error == ("2/3. bacak açılamadı: MetaApi emri reddetti (MetaApi istek sınırı aşıldı, kısa sürede çok "
                         "fazla istek gönderildi)")
    assert len(venue.creates()) == 2


# =========================================================================== #
# C2 / C3  the owner's view: production replica (engine + all four brokers), fx route
# =========================================================================== #
@pytest.fixture
async def prod(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    import test_preflight_scenarios as tps
    from copytrader.brokers.mt5_broker import Mt5Broker
    from copytrader.telegram import formatting as fmt
    monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: tps.SERVER_TIME - 7200.0))
    monkeypatch.setattr(Mt5Broker, "tick_wait_sec", 0.0)
    monkeypatch.setattr(fmt, "RISK_PER_R_NOTE", fmt.RISK_PER_R_NOTE)

    def as_on_the_server(cfg: Any) -> None:
        cfg.notifier.notify_on_execution = True          # prod_now/config.yaml

    p = await tps.Prod.create(tmp_path, monkeypatch, edit=as_on_the_server)
    yield p
    await p.close()


async def eur_long(prod: Any) -> tuple[list[str], dict[str, Any]]:
    import test_preflight_scenarios as tps
    tps.set_eurusd(prod, 1.16235, 1.16245)
    await prod.say(tps.fx_pair(*tps.EURUSD_LONG, entry_type=EntryType.MARKET), tps.FX, "EUR LONG 1R")
    (trade,) = await prod.trades()
    return prod.texts(), await prod.positions(trade.id)


async def test_c3_engine_live_fx_signal_opens_on_fx_and_tickmill(prod: Any) -> None:
    texts, pos = await eur_long(prod)
    assert not [t for t in texts if "durumu belirsiz" in t or "GÖNDERİLMİŞ" in t or "reddetti" in t], texts
    assert pos["fx"].status is PositionStatus.OPEN and pos["tickmill"].status is PositionStatus.OPEN
    (report,) = [t for t in texts if "EUR LONG açıldı" in t]
    assert "• fx: 0.4 EURUSD (açık)" in report and "• tickmill: 0.18 EURUSD (açık)" in report, report
    assert STRICT["refused"] == [] and len(STRICT["checked"]) == 1 and CLIENT_ID_PATTERN.match(STRICT["checked"][0])


async def test_c2_engine_tickmill_refusing_every_label_still_opens_and_the_owner_is_told(prod: Any) -> None:
    never_matches(prod.tickmill)
    texts, pos = await eur_long(prod)
    assert pos["fx"].status is PositionStatus.OPEN and pos["tickmill"].status is PositionStatus.OPEN
    (report,) = [t for t in texts if "EUR LONG açıldı" in t]
    assert "• fx: 0.4 EURUSD (açık)" in report and "• tickmill: 0.18 EURUSD (açık)" in report, report
    assert "MetaApi emir etiketini (clientId) kabul etmedi; emir etiketsiz gönderildi" in report, report
    assert not [t for t in texts if "durumu belirsiz" in t or "GÖNDERİLMİŞ" in t], texts


async def test_c2_engine_definite_refusal_is_told_in_plain_words_and_fx_opens(prod: Any) -> None:
    prod.tickmill.fail["create_market_buy_order"] = [sdk_validation_error("volume", 0.18, "Invalid volume")]
    texts, pos = await eur_long(prod)
    assert pos["fx"].status is PositionStatus.OPEN and "tickmill" not in pos
    assert ("tickmill: <code>MetaApi emri reddetti (geçersiz alan: volume – Invalid volume); bu hesapta işlem "
            "açılmadı.</code>") in texts, texts
    assert not [t for t in texts if "durumu belirsiz" in t or "GÖNDERİLMİŞ" in t], texts


# =========================================================================== #
# C3  the existing MetaApi scenarios, on the strict venue
# =========================================================================== #
STRICT_RUN = ("test_metaapi_broker.py", "test_metaapi_extra.py", "test_preflight2_metaapi.py",
              "test_preflight4_metaapi_reconnect.py", "test_review1_adapters.py", "test_preflight_scenarios.py",
              "test_preflight_integration.py", "test_preflight_brokers.py", "test_incident_day_signals.py")
PLUGIN = '''"""Makes test_metaapi_broker.FakeConn a strict MetaApi venue for the whole run (test_review1_clientid)."""
import json
import os
import sys


def pytest_collection_finish(session):
    import test_review1_clientid
    test_review1_clientid.make_strict_for_run()


def pytest_sessionfinish(session, exitstatus):
    strict = sys.modules.get("test_review1_clientid")
    path = os.environ.get("CT_STRICT_REPORT")
    if strict is not None and path:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(strict.STRICT, fh)
'''


def strict_run(tmp_path: Path, files: list[str], *extra: str) -> tuple[subprocess.CompletedProcess[str], Any]:
    """``files`` (of the tree under test) in a pytest run of their own, FakeConn strict for the whole run."""
    tests = Path(tmb.__file__).resolve().parent          # the tree under test (WORK, or 62e0719 in a base run)
    root = Path(mb.__file__).resolve().parents[2]
    (tmp_path / "strict_venue_plugin.py").write_text(PLUGIN, encoding="utf-8")
    report = tmp_path / "strict.json"
    path = [str(tmp_path), str(Path(__file__).resolve().parent), str(tests), str(root)]
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(path + [os.environ.get("PYTHONPATH", "")]),
           "CT_STRICT_REPORT": str(report), "PYTHONDONTWRITEBYTECODE": "1"}
    args = [str(tests / f) for f in files if (tests / f).exists()]
    proc = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p",
                           "strict_venue_plugin", *extra, *args], cwd=root, env=env, capture_output=True, text=True,
                          timeout=900)
    return proc, json.loads(report.read_text(encoding="utf-8")) if report.exists() else None


def test_c3_existing_metaapi_scenarios_pass_on_the_strict_venue(tmp_path: Path) -> None:
    """market / limit / stop entries, multi-TP legs, partial and full closes, stop and target moves, cancel,
    lost-reply adoption, reconnects, the production replica: all of it, on the strict venue."""
    proc, seen = strict_run(tmp_path, list(STRICT_RUN))
    tail = (proc.stdout[-6000:] + proc.stderr[-2000:])
    assert proc.returncode == 0, tail
    assert seen is not None and seen["refused"] == [], seen and seen["refused"][:10]
    assert len(seen["checked"]) >= 100 and all(CLIENT_ID_PATTERN.match(c) for c in seen["checked"])


def test_c3_the_strict_run_does_refuse_a_bad_client_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Control: in the same kind of run, the clientId form of 62e0719 (the bare tag) is refused by the venue, so the
    green run above means the venue really checked every request."""
    plugin_src = PLUGIN.replace("test_review1_clientid.make_strict_for_run()",
                                "test_review1_clientid.make_strict_for_run()\n"
                                "    from copytrader.brokers import metaapi_broker as mb\n"
                                "    mb.encode_client_id = lambda tag: tag")
    monkeypatch.setattr(sys.modules[__name__], "PLUGIN", plugin_src)
    _proc, seen = strict_run(tmp_path, ["test_metaapi_broker.py"], "-k", "one_leg_per_tp or sync_closed")
    assert seen is not None
    assert seen["refused"] == ["ct12ab34cd-1", "ct12ab34cd-2", "ct12ab34cd-3", "ct12ab34cd-1", "ct12ab34cd-2"]


def test_strict_venue_keeps_the_sdk_signatures() -> None:
    """Guard: the strict wrapper keeps every method's signature (test_fake_matches_installed_sdk_signature)."""
    for name, fn in strict_methods().items():
        assert inspect.signature(fn) == inspect.signature(_plain(name))
        assert asyncio.iscoroutinefunction(fn)
