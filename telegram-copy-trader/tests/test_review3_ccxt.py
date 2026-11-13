"""Review 3 (lane repair3_ccxt): the binance (ccxt) adapter after an order request whose answer was lost.

  X1  an ENTRY that Binance executed while its answer was lost (timeout / binance -1007 / -1006) is looked up by its
      client id for a short, bounded while and adopted when found; when it is not found, or cannot be looked for, it is
      an unknown outcome (OrderOutcomeUnknownError, "durumu belirsiz:"), never "açılmadı". It is never sent twice.
  X2  a CLOSE whose answer was lost is looked up by its client id and booked when found (size, stop and targets
      re-sized), so the stored size follows the venue; otherwise it is an unknown outcome kept by its client id (the
      next syncs book it once it shows up) and the next close never reuses that client id.
  X3  every order-sending path words an unclear answer as "yanıt alınamadı", not "reddedildi" / "edilemedi", and keeps
      track of an order that may exist (no orphan stop, no second close).

Adapter tests run on the in-memory FakeExchange of tests/test_ccxt_broker.py; the engine tests on the production
replica of tests/test_preflight_scenarios.py with the server's config of today (LIVE_YAML).
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import ccxt
import pytest
from ccxt.base import errors as cx

import copytrader.brokers.ccxt_broker as cb
from copytrader.brokers.base import OUTCOME_UNKNOWN_MARK, BrokerError, OrderOutcomeUnknownError, outcome_unknown
from copytrader.models import ActionType, EntryType, ParsedAction, PositionStatus, TakeProfit

from binance_rest_fake import ApiError
from test_ccxt_binance_real import BTC_LONG, BTC_STOP_AND_TP, _connected, _live, _open, _plan, _posts
from test_ccxt_broker import SYM, TWO_TPS, FakeExchange, make_broker, open_limit, open_market
from test_preflight_scenarios import KRIPTO, Prod, _isolate, btc  # noqa: F401  (_isolate: autouse)

BTC = "BTC/USDT:USDT"
LOST = "binanceusdm POST https://fapi.binance.com/fapi/v1/order request timeout"


def binance_error(code: int, msg: str, status: int = 400) -> cx.BaseError:
    """The exception REAL ccxt (binanceusdm) raises for this answer of POST /fapi/v1/order. No network."""
    ex = ccxt.binanceusdm()
    url = "https://fapi.binance.com/fapi/v1/order"
    body = json.dumps({"code": code, "msg": msg})
    try:
        ex.handle_errors(status, "x", url, "POST", {}, body, json.loads(body), {}, "")
        ex.handle_http_status_code(status, "x", url, "POST", body)
    except cx.BaseError as exc:
        return exc
    raise AssertionError("ccxt raised nothing")


def is_entry(t: str, params: dict[str, Any]) -> bool:
    return t == "market" and not params.get("reduceOnly")


def is_close(t: str, params: dict[str, Any]) -> bool:
    return (t == "market" and bool(params.get("reduceOnly")) and not params.get("stopLossPrice")
            and not params.get("takeProfitPrice"))


def is_stop(t: str, params: dict[str, Any]) -> bool:
    return bool(params.get("stopLossPrice"))


def answer_lost(fake: FakeExchange, which: Any, *, execute: bool = True, times: int = 1,
                error: Any = None) -> dict[str, Any]:
    """create_order of the first ``times`` orders that ``which(type, params)`` picks: the venue executes it
    (``execute``) and its answer is lost (``error()``, default a ccxt RequestTimeout). Lost requests that never reached
    the venue are recorded as "create_order:lost" (with their client id)."""
    original = fake.create_order
    state: dict[str, Any] = {"n": 0, "cids": []}

    async def create_order(symbol: str, type: str, side: str, amount: float, price: float | None = None,
                           params: dict | None = None) -> dict[str, Any]:
        p = dict(params or {})
        if state["n"] < times and which(type, p):
            state["n"] += 1
            state["cids"].append(p.get("clientOrderId"))
            if execute:
                await original(symbol, type, side, amount, price, params)
            else:
                fake.calls.append(("create_order:lost", (symbol, type, side, amount, price, p)))
            raise error() if error is not None else cx.RequestTimeout(LOST)
        return await original(symbol, type, side, amount, price, params)
    fake.create_order = create_order        # type: ignore[method-assign]
    return state


def lookups_down(fake: FakeExchange, times: int = 1000) -> None:
    """Every look-up of an order by its client id gets no answer (the connection keeps breaking)."""
    fake.fail_when(lambda m, a: m == "fetch_order" and bool((a[2] or {}).get("clientOrderId")),
                   cx.NetworkError("binanceusdm GET https://fapi.binance.com/fapi/v1/order connection reset"),
                   times=times)


def hidden(fake: FakeExchange, cid: str, looks: int) -> dict[str, int]:
    """The order tagged ``cid`` does not show up for its first ``looks`` look-ups by client id (binance -1007: the
    venue took it, its order query does not know it yet)."""
    state = {"left": looks, "hiding": 0}
    fetch_order, fetch_open, fetch_closed = fake.fetch_order, fake.fetch_open_orders, fake.fetch_closed_orders

    async def order(order_id: str, symbol: str | None = None, params: dict | None = None) -> dict:
        if (params or {}).get("clientOrderId") == cid:
            state["hiding"] = int(state["left"] > 0)        # (the order lists of this same look-up hide it too)
            if state["hiding"]:
                state["left"] -= 1
                fake.calls.append(("fetch_order:hidden", (order_id, symbol, dict(params or {}))))
                raise cx.OrderNotFound('binanceusdm {"code":-2013,"msg":"Order does not exist."}')
        return await fetch_order(order_id, symbol, params)

    async def open_orders(*a: Any, **k: Any) -> list[dict]:
        out = await fetch_open(*a, **k)
        return [o for o in out if not (state["hiding"] and o.get("clientOrderId") == cid)]

    async def closed_orders(*a: Any, **k: Any) -> list[dict]:
        out = await fetch_closed(*a, **k)
        return [o for o in out if not (state["hiding"] and o.get("clientOrderId") == cid)]
    fake.fetch_order, fake.fetch_open_orders, fake.fetch_closed_orders = order, open_orders, closed_orders
    return state


def cid_lookups(fake: FakeExchange) -> int:
    return sum(1 for m, a in fake.calls if m in ("fetch_order", "fetch_order:hidden") and (a[2] or {}).get("clientOrderId"))


def entries_of(fake: FakeExchange) -> list[dict[str, Any]]:
    return [o for o in fake.orders.values() if o["type"] == "market" and not o["reduceOnly"]]


def closes_of(fake: FakeExchange) -> list[dict[str, Any]]:
    return [o for o in fake.orders.values() if is_close(o["type"], o["params"])]


def live(fake: FakeExchange, key: str) -> list[float]:
    return sorted(o["amount"] for o in fake.orders.values() if o["status"] == "open" and o["params"].get(key))


# =========================================================================== #
# X1: an entry whose answer was lost
# =========================================================================== #
async def test_x1_entry_executed_whose_answer_is_lost_and_cannot_be_looked_up_is_unknown_never_not_opened() -> None:
    """The referee's newdefects2b problem 3: Binance executes the market buy, its answer is lost (RequestTimeout,
    binance -1007) and every look-up by the client id fails. d45f96a: 'giriş emri gönderilemedi ... emir
    bulunamadı' (a plain BrokerError: the engine said 'binance hesabında açılmadı' over a stopless position)."""
    broker, fake = make_broker()
    answer_lost(fake, is_entry)
    lookups_down(fake)
    with pytest.raises(BrokerError) as ei:
        await open_market(broker)
    exc = ei.value
    assert isinstance(exc, OrderOutcomeUnknownError) and outcome_unknown(exc)
    text = str(exc)
    assert OUTCOME_UNKNOWN_MARK in text and "giriş emri gönderildi ama yanıtı alınamadı" in text
    assert "borsaya ulaşılamadığı için bu etiketle (ct12ab34cde) aranamadı" in text
    assert "Aynı emir ikinci kez GÖNDERİLMEDİ" in text and "stopu YOK" in text and "gönderilemedi" not in text
    assert len(entries_of(fake)) == 1                                   # never sent twice
    assert cid_lookups(fake) == cb._LOOKUP_ATTEMPTS                      # it kept looking for the whole window


async def test_x1_entry_not_visible_at_first_is_looked_up_again_and_adopted_with_its_stop() -> None:
    """binance -1007: the order exists but its query does not know it yet. d45f96a gave up after the first 'not
    found'. Now it is asked again within the bounded window, adopted and protected."""
    broker, fake = make_broker()
    answer_lost(fake, is_entry)
    hidden(fake, "ct12ab34cde", looks=2)
    pos = await open_market(broker)
    assert pos.status is PositionStatus.OPEN and pos.data["entry_order_id"] == entries_of(fake)[0]["id"]
    assert len(entries_of(fake)) == 1 and live(fake, "stopLossPrice") == [pytest.approx(6.828)]
    assert cid_lookups(fake) == 3


async def test_x1_entry_not_visible_for_the_whole_window_is_unknown() -> None:
    broker, fake = make_broker()
    answer_lost(fake, is_entry)
    hidden(fake, "ct12ab34cde", looks=1000)
    with pytest.raises(OrderOutcomeUnknownError) as ei:
        await open_market(broker)
    assert "bu etiketle (ct12ab34cde) borsada henüz görünmüyor, ama yanıt kaybolduğu için kesin değil" in str(ei.value)
    assert len(entries_of(fake)) == 1 and cid_lookups(fake) == cb._LOOKUP_ATTEMPTS


async def test_x1_limit_entry_unknown_text_names_the_resting_order() -> None:
    broker, fake = make_broker()
    answer_lost(fake, lambda t, p: t == "limit" and not p.get("reduceOnly"))
    lookups_down(fake)
    with pytest.raises(OrderOutcomeUnknownError) as ei:
        await open_limit(broker)
    assert "bekleyen emirlerine ve pozisyonuna elle bakın" in str(ei.value)


async def test_x1_binance_1006_execution_status_unknown_is_looked_up_and_adopted() -> None:
    """binance -1006 'Execution status unknown' is a plain OperationFailed in real ccxt 4.5.x. d45f96a reported it as
    'giriş emri reddedildi' without any look-up, over an executed, stopless position."""
    err = binance_error(-1006, "An unexpected response was received from the message bus. Execution status unknown.",
                        500)
    assert type(err) is cx.OperationFailed                              # (real ccxt's class for it)
    broker, fake = make_broker()
    answer_lost(fake, is_entry, error=lambda: err)
    pos = await open_market(broker)
    assert pos.status is PositionStatus.OPEN and len(entries_of(fake)) == 1
    assert live(fake, "stopLossPrice") == [pytest.approx(6.828)]


async def test_x1_look_up_window_is_bounded_in_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reads that hang (no answer at all) are cut off when the window ends: the leg stays inside the engine's
    deadline. (Here the window is 0.5 s instead of 8 s.)"""
    monkeypatch.setattr(cb, "_LOOKUP_WINDOW_SEC", 0.5, raising=False)
    broker, fake = make_broker()
    answer_lost(fake, is_entry)
    fetch_order = fake.fetch_order

    async def hangs(order_id: str, symbol: str | None = None, params: dict | None = None) -> dict:
        if (params or {}).get("clientOrderId"):
            await asyncio.sleep(3600)
        return await fetch_order(order_id, symbol, params)
    fake.fetch_order = hangs                # type: ignore[method-assign]
    started = time.monotonic()
    with pytest.raises(OrderOutcomeUnknownError) as ei:
        await asyncio.wait_for(open_market(broker), 5.0)
    assert time.monotonic() - started < 2.0
    assert "aranamadı" in str(ei.value) and len(entries_of(fake)) == 1


def test_x1_lookup_window_fits_the_engines_leg_deadline() -> None:
    from copytrader.engine.orchestrator import LEG_DEADLINE_SEC
    ccxt_timeout = ccxt.binanceusdm().timeout / 1000.0                  # the lost request itself (10 s)
    assert cb._LOOKUP_WINDOW_SEC + ccxt_timeout <= LEG_DEADLINE_SEC * 0.6


@pytest.mark.parametrize("error", [
    pytest.param(lambda: binance_error(-1021, "Timestamp for this request is outside of the recvWindow."), id="1021"),
    pytest.param(lambda: cx.DDoSProtection("binanceusdm 429 Too Many Requests"), id="429"),
])
async def test_x1_clear_refusals_that_ccxt_classes_as_network_errors_stay_refusals(error: Any) -> None:
    """binance -1021 (InvalidNonce) and HTTP 429 (DDoSProtection) are NetworkErrors in ccxt, but the venue refused the
    request before taking any order: one look-up (as before), then a refusal - not an unknown outcome."""
    err = error()
    assert isinstance(err, cx.NetworkError)
    broker, fake = make_broker()
    answer_lost(fake, is_entry, execute=False, error=lambda: err)
    with pytest.raises(BrokerError) as ei:
        await open_market(broker)
    assert not outcome_unknown(ei.value) and "giriş emri reddedildi" in str(ei.value)
    assert cid_lookups(fake) == 1 and entries_of(fake) == []


async def test_x1_engine_headline_says_whether_binance_opened_is_unknown(tmp_path: Any,
                                                                       monkeypatch: pytest.MonkeyPatch) -> None:
    """End to end on the live config: the owner reads that the binance order went out and whether it opened is not
    known (check it), never 'binance hesabında açılmadı'."""
    p = await Prod.create(tmp_path, monkeypatch, replica="live")
    try:
        answer_lost(p.binance, is_entry)
        lookups_down(p.binance)
        await p.say(btc(EntryType.MARKET, 74000.0, 73000.0, 76000.0), KRIPTO, "BTC LONG 1R")
        texts = p.texts()
        assert any("emir gönderildi ama sonucu alınamadı; binance hesabında açılıp açılmadığı bilinmiyor, kontrol "
                   "edin" in t for t in texts), texts
        assert not any("binance hesabında açılmadı" in t for t in texts), texts
        assert any("durumu belirsiz:" in t and "stopu YOK" in t for t in texts), texts
        assert len([o for o in entries_of(p.binance) if o["symbol"] == BTC]) == 1
    finally:
        await p.close()


# =========================================================================== #
# X2: a close whose answer was lost
# =========================================================================== #
async def test_x2_partial_close_executed_whose_answer_is_lost_is_found_and_booked() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    answer_lost(fake, is_close)
    pos = await broker.close_position(pos, 0.5)
    assert pos.status is PositionStatus.OPEN and pos.remaining_quantity == pytest.approx(3.414)
    assert fake.net[SYM] == pytest.approx(3.414) and len(closes_of(fake)) == 1
    assert [e["qty"] for e in pos.data["exits"]] == [pytest.approx(3.414)]
    assert live(fake, "stopLossPrice") == [pytest.approx(3.414)]                  # stop re-sized
    assert sum(live(fake, "takeProfitPrice")) == pytest.approx(3.414)              # targets re-sized
    assert not pos.data.get("unconfirmed_closes") and "okunamadı" not in (pos.error or "")


async def test_x2_full_close_executed_whose_answer_is_lost_is_closed_at_once() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    answer_lost(fake, is_close)
    pos = await broker.close_position(pos, 1.0)
    assert pos.status is PositionStatus.CLOSED and pos.remaining_quantity == 0 and fake.net[SYM] == 0
    assert pos.realized_pnl is not None and len(closes_of(fake)) == 1
    assert not [o for o in fake.orders.values() if o["status"] == "open"]


async def test_x2_close_that_never_reached_the_venue_is_unknown_and_its_client_id_is_never_reused() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    lost = answer_lost(fake, is_close, execute=False)
    with pytest.raises(OrderOutcomeUnknownError) as ei:
        await broker.close_position(pos, 0.5)
    text = str(ei.value)
    assert "kapatma emri (3.414 ETH/USDT:USDT) gönderildi ama yanıtı alınamadı" in text and "reddedildi" not in text
    assert OUTCOME_UNKNOWN_MARK in text and "gerçekleşip gerçekleşmediği bilinmiyor" in text
    # The caller's own object (the one the engine stores with the owed change) keeps the record and the sequence.
    (u,) = pos.data["unconfirmed_closes"]
    assert (u["client_id"], u["order_id"], u["qty"], pos.data["close_seq"]) == ("ct12ab34cdc1", None, 3.414, 1)
    assert pos.remaining_quantity == pytest.approx(6.828) and fake.net[SYM] == pytest.approx(6.828)
    pos = await broker.close_position(pos, 0.5)                         # tried again: a NEW client id
    assert lost["cids"] == ["ct12ab34cdc1"] and [o["clientOrderId"] for o in closes_of(fake)] == ["ct12ab34cdc2"]
    assert pos.remaining_quantity == pytest.approx(3.414) and fake.net[SYM] == pytest.approx(3.414)


async def test_x2_lost_close_that_shows_up_later_is_booked_by_the_next_sync() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    answer_lost(fake, is_close)
    hidden(fake, "ct12ab34cdc1", looks=cb._LOOKUP_ATTEMPTS)            # not visible while the adapter looks
    with pytest.raises(OrderOutcomeUnknownError):
        await broker.close_position(pos, 0.5)
    assert fake.net[SYM] == pytest.approx(3.414)                        # it did execute
    assert pos.remaining_quantity == pytest.approx(6.828) and "okunamadı" in (pos.error or "")
    pos = await broker.sync_position(pos)                               # visible now: booked
    assert pos.remaining_quantity == pytest.approx(3.414) and not pos.data["unconfirmed_closes"]
    assert live(fake, "stopLossPrice") == [pytest.approx(3.414)]
    assert sum(live(fake, "takeProfitPrice")) == pytest.approx(3.414)
    assert len(closes_of(fake)) == 1 and "okunamadı" not in (pos.error or "")


async def test_x2_lost_close_never_found_is_dropped_after_three_syncs_and_nothing_is_booked() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    answer_lost(fake, is_close, execute=False)
    with pytest.raises(OrderOutcomeUnknownError):
        await broker.close_position(pos, 0.5)
    # Review 4 (A2a): a close kept by its client id is dropped only once at least 60 s have also passed since it
    # was sent (it may still execute late); the adapter's clock is moved past that for the syncs below.
    clock = broker._now_ms
    broker._now_ms = lambda: clock() + 61_000
    for left in (1, 1, 0):
        pos = await broker.sync_position(pos)
        assert len(pos.data["unconfirmed_closes"]) == left
    assert pos.remaining_quantity == pytest.approx(6.828) and pos.data["exits"] == []
    assert "okunamadı" not in (pos.error or "") and live(fake, "stopLossPrice") == [pytest.approx(6.828)]


async def test_x2_an_earlier_close_booked_only_now_is_never_closed_a_second_time() -> None:
    """An earlier close's result was unknown (unconfirmed by its order id); the next close request was worked out from
    the stored size. When the earlier one turns out to have gone through, nothing new is sent (d45f96a closed the
    same half again: the whole position)."""
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    fake.net[SYM] += 5.0                            # another trade shares the one-way position (no flat shortcut)
    fake.minimal_create_response = True
    fake.fail_when(lambda m, a: m == "fetch_order" and a[0] == str(fake._seq), cx.RequestTimeout("timeout"), times=6)
    pos = await broker.close_position(pos, 0.5)
    assert pos.data["unconfirmed_closes"] and pos.remaining_quantity == pytest.approx(6.828)
    fake.minimal_create_response = False
    fake.failures.clear()
    pos = await broker.close_position(pos, 0.5)
    assert len(closes_of(fake)) == 1 and pos.remaining_quantity == pytest.approx(3.414)
    assert fake.net[SYM] == pytest.approx(3.414 + 5.0)
    assert "yeni kapatma emri gönderilmedi" in (pos.error or "")


async def test_x2_clear_close_refusal_stays_a_refusal() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    fake.fail_when(lambda m, a: m == "create_order" and is_close(a[1], a[5]),
                   binance_error(-2022, "ReduceOnly Order is rejected."))
    with pytest.raises(BrokerError) as ei:
        await broker.close_position(pos, 0.5)
    assert not outcome_unknown(ei.value) and "kapatma emri reddedildi" in str(ei.value)
    assert cid_lookups(fake) == 0 and "unconfirmed_closes" not in pos.data


@pytest.fixture
async def live_prod(tmp_path: Any, monkeypatch: pytest.MonkeyPatch):
    p = await Prod.create(tmp_path, monkeypatch, replica="live")
    yield p
    await p.close()


def half(trade_id: int, sig: int) -> ParsedAction:
    return ParsedAction(action=ActionType.CLOSE, target_trade_id=trade_id, refers_to_message_id=sig, base_asset="BTC",
                        confidence=0.95, summary_tr="yarısını kapat", close_fraction=0.5)


async def test_x2_engine_c1_partial_close_executed_with_its_answer_lost_is_booked_once(live_prod: Prod) -> None:
    """The referee's C1 on the live config: 'yarısını kapattık'; Binance executes the reduce-only close and the answer
    is lost (-1007). The stored size follows the venue at once (0.006, stop and target re-sized), the half is closed
    once, and the owner is told it was closed - not 'reddedildi'."""
    p = live_prod
    sig = await p.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO, "btc 1R long piyasadan")
    (trade,) = await p.trades()
    assert (await p.positions(trade.id))["binance"].remaining_quantity == pytest.approx(0.012)
    answer_lost(p.binance, is_close, error=lambda: binance_error(
        -1007, "Timeout waiting for response from backend server. Send status unknown; execution status unknown.", 408))
    p.notifier.notifications.clear()
    await p.say(half(trade.id, sig), KRIPTO, "yarısını kapattık", reply_to=sig)
    for _ in range(3):
        await p.engine.monitor.run_once()
    pos = (await p.positions(trade.id))["binance"]
    assert len([o for o in closes_of(p.binance) if o["symbol"] == BTC]) == 1
    assert p.binance.net[BTC] == pytest.approx(0.006) and pos.remaining_quantity == pytest.approx(0.006)
    assert live(p.binance, "stopLossPrice") == [pytest.approx(0.006)]
    assert live(p.binance, "takeProfitPrice") == [pytest.approx(0.006)]
    assert "manage_wanted" not in pos.data and not pos.data.get("unconfirmed_closes")
    texts = p.texts()
    assert any("binance: %50 kapatıldı" in t for t in texts), texts
    assert not any("reddedildi" in t or "yine uygulanamadı" in t for t in texts), texts


async def test_x2_engine_partial_close_that_never_reached_binance_is_retried_with_a_new_client_id(
        live_prod: Prod) -> None:
    """Control: the request never reached Binance (RequestTimeout, nothing executed). The owner is told its answer was
    not received (not 'reddedildi'); the bot owes the change and sends it again on the next pass - with a new client
    id (d45f96a re-used the lost one) - and the half is closed once."""
    p = live_prod
    sig = await p.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO, "btc 1R long piyasadan")
    (trade,) = await p.trades()
    lost = answer_lost(p.binance, is_close, execute=False)
    p.notifier.notifications.clear()
    await p.say(half(trade.id, sig), KRIPTO, "yarısını kapattık", reply_to=sig)
    stored = (await p.positions(trade.id))["binance"]
    assert stored.remaining_quantity == pytest.approx(0.012) and p.binance.net[BTC] == pytest.approx(0.012)
    assert stored.data["unconfirmed_closes"][0]["client_id"] == lost["cids"][0]          # kept for the syncs
    texts = p.texts()
    assert any("binance" in t and "yanıtı alınamadı" in t for t in texts), texts
    assert not any("reddedildi" in t for t in texts), texts
    # Review 4 (A2a): a close kept by its client id is dropped only once at least 60 s have also passed since it
    # was sent (it may still execute late); the adapter's clock is moved past that for the syncs below.
    bn = p.brokers["binance"]
    clock = bn._now_ms
    bn._now_ms = lambda: clock() + 61_000
    for _ in range(3):
        await p.engine.monitor.run_once()
    pos = (await p.positions(trade.id))["binance"]
    sent = [o["clientOrderId"] for o in closes_of(p.binance) if o["symbol"] == BTC]
    assert len(sent) == 1 and sent[0] != lost["cids"][0] and sent[0].endswith("c2"), (sent, lost)
    assert p.binance.net[BTC] == pytest.approx(0.006) and pos.remaining_quantity == pytest.approx(0.006)
    assert live(p.binance, "stopLossPrice") == [pytest.approx(0.006)]
    assert not pos.data.get("unconfirmed_closes")                       # never found on 3 syncs: dropped


# =========================================================================== #
# X3: truthful words and no double send on the other paths
# =========================================================================== #
async def test_x3_cancel_without_an_answer_says_so() -> None:
    broker, fake = make_broker()
    pos = await open_limit(broker)
    fake.fail_when(lambda m, a: m == "cancel_order", cx.RequestTimeout("binanceusdm DELETE order request timeout"))
    with pytest.raises(BrokerError) as ei:
        await broker.cancel_entry(pos)
    assert "iptal isteğine yanıt alınamadı" in str(ei.value) and "iptal edilemedi" not in str(ei.value)


async def test_x3_stop_move_whose_answers_are_lost_says_so_and_leaves_no_orphan_stop() -> None:
    """Both new-stop requests reach Binance but their answers are lost and they cannot be looked up now. The owner is
    told the answer was not received (the old stop is active); the two stops are not forgotten with the error: the
    next sync finds them and cancels them, so only the old stop is left (d45f96a left both on the venue)."""
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    old = pos.data["sl_order_id"]
    answer_lost(fake, is_stop, times=2)
    fake.fail_when(lambda m, a: m == "fetch_order" and bool((a[2] or {}).get("clientOrderId")),
                   cx.NetworkError("binanceusdm GET algoOrder connection reset"), times=2)
    with pytest.raises(BrokerError) as ei:
        await broker.update_stop_loss(pos, 2215.0)
    text = str(ei.value)
    assert "yeni stop (2215) emrine yanıt alınamadı" in text and "eski stop yerinde ve aktif" in text
    # (stops and targets share one client-id sequence: s1, t2, t3 at the open)
    assert [lo["cid"] for lo in pos.data["lost_orders"]] == ["ct12ab34cds4", "ct12ab34cds5"]
    assert pos.data["prot_seq"] == 5
    assert len(live(fake, "stopLossPrice")) == 3                         # the old one and the two lost ones
    pos = await broker.sync_position(pos)
    assert [o["id"] for o in fake.orders.values() if o["status"] == "open" and o["params"].get("stopLossPrice")] == [old]
    assert pos.data["lost_orders"] == [] and pos.data["sl_order_id"] == old


async def test_x3_emergency_close_whose_answer_is_lost_is_booked_when_found() -> None:
    """The stop cannot be placed (refused twice) and the emergency close executes but its answer is lost. d45f96a:
    'pozisyon kapatılamadı' (KRİTİK, still open) over a flat account; now the close is found and booked."""
    broker, fake = make_broker()
    fake.fail_when(lambda m, a: m == "create_order" and "stopLossPrice" in a[5],
                   cx.InvalidOrder("Order would immediately trigger"), times=2)
    answer_lost(fake, is_close)
    pos = await open_market(broker, tps=TWO_TPS)
    assert fake.net[SYM] == 0 and pos.status is PositionStatus.FAILED and pos.remaining_quantity == 0
    assert "piyasa fiyatından kapatıldı" in (pos.error or "") and "kapatılamadı" not in (pos.error or "")


async def test_x3_emergency_close_whose_answer_is_lost_and_not_found_is_not_called_failed() -> None:
    broker, fake = make_broker()
    fake.fail_when(lambda m, a: m == "create_order" and "stopLossPrice" in a[5],
                   cx.InvalidOrder("Order would immediately trigger"), times=2)
    answer_lost(fake, is_close, execute=False)
    pos = await open_market(broker, tps=TWO_TPS)
    assert pos.status is PositionStatus.OPEN and pos.data["unprotected"]
    assert "acil kapatma emrinin sonucu okunamadı" in (pos.error or "") and "pozisyon kapatılamadı" not in pos.error
    assert pos.data["unconfirmed_closes"][0]["kind"] == "emergency"


async def test_x3_take_profit_whose_answer_is_lost_says_so() -> None:
    broker, fake = make_broker()
    answer_lost(fake, lambda t, p: bool(p.get("takeProfitPrice")), execute=False)
    pos = await open_market(broker, tps=[TakeProfit(price=2310.60, close_fraction=1.0)])
    assert pos.status is PositionStatus.OPEN and "kâr al" in (pos.error or "") and "yanıt alınamadı" in pos.error


# =========================================================================== #
# The same on the REAL ccxt binanceusdm class (only its HTTP layer is faked: tests/binance_rest_fake.py)
# =========================================================================== #
UNKNOWN_STATUS = {-1007: ("Timeout waiting for response from backend server. Send status unknown; execution status "
                          "unknown.", 408),
                  -1006: ("An unexpected response was received from the message bus. Execution status unknown.", 500)}


def _order_posts(fake: Any, reduce_only: bool) -> list[dict[str, str]]:
    return [q for p, q in _posts(fake) if p == "/fapi/v1/order" and (q.get("reduceOnly") == "true") is reduce_only]


def _executed_then(fake: Any, code: int) -> Any:
    def answer(method: str, path: str, q: dict[str, str]) -> None:
        fake.new_order(q)                   # Binance's matching engine executes it, then the answer is this code
        msg, status = UNKNOWN_STATUS[code]
        raise ApiError(code, msg, status=status)
    return answer


@pytest.mark.parametrize("code", [-1007, -1006])
async def test_x1_real_ccxt_entry_with_execution_status_unknown_is_found_by_its_client_id(code: int) -> None:
    """(-1007 was already looked up before; -1006, a plain OperationFailed in ccxt, was 'reddedildi'.)"""
    broker, fake = await _connected()
    fake.fail(lambda m, p, q: m == "POST" and p == "/fapi/v1/order" and q.get("reduceOnly") != "true",
              _executed_then(fake, code))
    _, pos = await _open(broker, _plan(**BTC_LONG))
    assert pos.status is PositionStatus.OPEN and fake.pos["BTCUSDT"]["amt"] == 0.012
    assert _live(fake) == BTC_STOP_AND_TP
    (entry,) = _order_posts(fake, reduce_only=False)                    # sent once
    looks = [q for m, p, q in fake.log if m == "GET" and p == "/fapi/v1/order" and "origClientOrderId" in q]
    assert looks and looks[0]["origClientOrderId"] == entry["newClientOrderId"]
    await broker.close()


@pytest.mark.parametrize("code", [-1007, -1006])
async def test_x2_real_ccxt_close_with_execution_status_unknown_is_found_and_booked(code: int) -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.fail(lambda m, p, q: m == "POST" and p == "/fapi/v1/order" and q.get("reduceOnly") == "true",
              _executed_then(fake, code))
    pos = await broker.close_position(pos, 0.5)
    assert pos.remaining_quantity == pytest.approx(0.006) and fake.pos["BTCUSDT"]["amt"] == pytest.approx(0.006)
    assert _live(fake) == [("STOP_MARKET", "0.006", "73300"), ("TAKE_PROFIT_MARKET", "0.006", "75500")]
    assert len(_order_posts(fake, reduce_only=True)) == 1
    await broker.close()


async def test_x2_real_ccxt_close_lost_and_unreadable_is_unknown_and_the_retry_uses_a_new_client_id() -> None:
    broker, fake = await _connected()
    _, pos = await _open(broker, _plan(**BTC_LONG))
    fake.fail(lambda m, p, q: m == "POST" and p == "/fapi/v1/order" and q.get("reduceOnly") == "true",
              ccxt.RequestTimeout("binanceusdm POST https://fapi.binance.com/fapi/v1/order request timeout"))
    fake.fail(lambda m, p, q: m == "GET" and p == "/fapi/v1/order" and "origClientOrderId" in q,
              ccxt.NetworkError("binanceusdm GET https://fapi.binance.com/fapi/v1/order connection reset"), times=100)
    with pytest.raises(OrderOutcomeUnknownError) as ei:
        await broker.close_position(pos, 0.5)
    assert "aranamadı" in str(ei.value) and fake.pos["BTCUSDT"]["amt"] == pytest.approx(0.012)
    assert pos.data["close_seq"] == 1 and pos.data["unconfirmed_closes"][0]["order_id"] is None
    fake.hooks.clear()
    pos = await broker.close_position(pos, 0.5)
    cids = [q["newClientOrderId"] for q in _order_posts(fake, reduce_only=True)]
    assert len(cids) == 2 and cids[0] != cids[1] and cids[1].endswith("c2")
    assert fake.pos["BTCUSDT"]["amt"] == pytest.approx(0.006) and pos.remaining_quantity == pytest.approx(0.006)
    await broker.close()
