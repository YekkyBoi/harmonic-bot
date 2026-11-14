"""Review 3b (lane repair3b): the engine's owed-close guard (E1) together with the binance adapter that books a lost
close by its client id (X2), and truthful words for unknown outcomes. On the production replica of
tests/test_preflight_scenarios.py with the REAL CcxtBroker over the scenario fake and the server's config of today
(kripto -> binance, breakout, tickmill; notify_on_execution; tickmill optional).

  F1  a second partial close B that comes while the first one A is still owed (A executed at Binance, its answer was
      lost and it could not be looked up in time) is applied exactly once, to half of what A leaves (0.003 of 0.012),
      never more and never twice - also when A executed only in part or not at all, and when the look-ups stay down.
      When the adapter sends nothing (it has just booked A instead), B stays owed and is sent after the next read.
  F2  a close of the whole trade whose outcome is unknown is never called "kapatılamadı"; its end neither.
  F3  a venue refusal that ccxt files under NetworkError (binance -1021, HTTP 418 / 429) is a refusal for the engine.
  F4  a trade whose binance open outcome was unknown names binance in every later report and in its closing summary.

Every test checks the venue first, then what the owner was told.
"""

from __future__ import annotations

from typing import Any

import pytest
from ccxt.base import errors as cx

import test_preflight_scenarios as tps
from test_review3_ccxt import (  # noqa: F401  (_isolate: autouse)
    BTC,
    KRIPTO,
    Prod,
    _isolate,
    answer_lost,
    binance_error,
    btc,
    cid_lookups,
    closes_of,
    half,
    is_close,
    is_entry,
    live,
    lookups_down,
)

import copytrader.brokers.ccxt_broker as cb
from copytrader.brokers.base import BrokerError, OrderOutcomeUnknownError
from copytrader.engine import orchestrator as orch
from copytrader.engine.orchestrator import Engine, no_answer
from copytrader.models import ActionType, BrokerPosition, EntryType, ManagementPlan, ParsedAction, PositionStatus, Side

MANAGE_WANTED = "manage_wanted"
APPLIED = "daha önce uygulanamayan değişiklik şimdi uygulandı"
LOST_1007 = "Timeout waiting for response from backend server. Send status unknown; execution status unknown."


@pytest.fixture
async def p(tmp_path: Any, monkeypatch: pytest.MonkeyPatch):
    prod = await Prod.create(tmp_path, monkeypatch, replica="live")
    yield prod
    await prod.close()


def lost_1007() -> cx.BaseError:
    return binance_error(-1007, LOST_1007, 408)


def lookups(p: Prod, state: dict[str, int]) -> None:
    """Look-ups of an order by its client id get no answer while ``state["down"]`` > 0 (each one uses one)."""
    def down(m: str, a: tuple[Any, ...]) -> bool:
        if m == "fetch_order" and bool((a[2] or {}).get("clientOrderId")) and state["down"] > 0:
            state["down"] -= 1
            return True
        return False
    p.binance.fail_when(down, cx.NetworkError("binanceusdm GET https://fapi.binance.com/fapi/v1/order connection "
                                              "reset"), times=60)


def close_lost_in_part(p: Prod, executed: float) -> None:
    """The first reduce-only market close: Binance executes only ``executed`` of it (the rest expires) and its answer
    is lost (-1007)."""
    fake = p.binance
    original = fake.create_order
    state = {"n": 0}

    async def create_order(symbol: str, type: str, side: str, amount: float, price: float | None = None,
                           params: dict | None = None) -> dict[str, Any]:
        if state["n"] == 0 and is_close(type, dict(params or {})):
            state["n"] += 1
            order = await original(symbol, type, side, amount, price, params)
            o = fake.orders[order["id"]]
            back = round(o["filled"] - executed, 10)
            o["filled"], o["status"] = executed, "expired"
            fake.net[symbol] = round(fake.net[symbol] + (back if o["side"] == "sell" else -back), 10)
            raise lost_1007()
        return await original(symbol, type, side, amount, price, params)
    fake.create_order = create_order        # type: ignore[method-assign]


def btc_closes(p: Prod) -> list[float]:
    return [o["amount"] for o in closes_of(p.binance) if o["symbol"] == BTC]


def executed_closes(p: Prod) -> float:
    return round(sum(o["filled"] for o in closes_of(p.binance) if o["symbol"] == BTC), 8)


def second_half(trade_id: int) -> ManagementPlan:
    """B: 'kalanın yarısı' as an approval runs it (at once, no venue sync first)."""
    return ManagementPlan(plan_id="f1second", action=ActionType.CLOSE, trade_id=trade_id, close_fraction=0.5,
                          source_chat_id=tps.CHAT, summary_tr=f"#{trade_id} BTC LONG: kalanın yarısı kapatılacak",
                          created_at=tps.datetime.now(tps.UTC))


async def btc_long(p: Prod) -> tuple[int, int]:
    sig = await p.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO, "btc 1R long piyasadan")
    (trade,) = await p.trades()
    assert (await p.positions(trade.id))["binance"].remaining_quantity == pytest.approx(0.012)
    return sig, trade.id


async def a_owed(p: Prod, sig: int, trade_id: int) -> BrokerPosition:
    """A: 'yarısını kapattık'; the close was sent, its answer lost; the bot owes it (MANAGE_WANTED reduce to 0.006)."""
    p.notifier.notifications.clear()
    await p.say(half(trade_id, sig), KRIPTO, "yarısını kapattık", reply_to=sig)
    pos = (await p.positions(trade_id))["binance"]
    assert pos.data[MANAGE_WANTED]["reduce"]["to"] == pytest.approx(0.006)
    assert pos.data.get("unconfirmed_closes"), pos.data          # (the adapter keeps it by its client id)
    p.notifier.notifications.clear()
    return pos


async def passes(p: Prod, n: int = 4) -> None:
    for _ in range(n):
        await p.engine.monitor.run_once()


def b_lines(texts: list[str]) -> list[str]:
    return [line for t in texts for line in t.splitlines() if line.startswith(("• binance", "#1 binance"))]


# =========================================================================== #
# F1
# =========================================================================== #
async def test_f1_second_close_while_the_executed_first_is_owed_and_unbooked_is_applied_once_after_the_read(
        p: Prod) -> None:
    """The coordinator's rig, no monitor pass between A and B. c1e191b: the adapter books A inside B's close and sends
    nothing, B is dropped as done (Binance keeps 0.006) and the owner reads '%50 kapatıldı ... bu kapatma borsadaki
    miktardan yapıldı'. Now B stays owed with its size (0.003 to stay open) and is sent after the next read."""
    sig, tid = await btc_long(p)
    answer_lost(p.binance, is_close, error=lost_1007)
    lookups(p, state := {"down": 6})                     # (A's whole look-up window gets no answer)
    await a_owed(p, sig, tid)
    assert p.binance.net[BTC] == pytest.approx(0.006) and state["down"] == 0
    await p.engine.execute_management(second_half(tid))
    # nothing new was sent with B; it is owed with the size the trader asked for
    assert btc_closes(p) == [pytest.approx(0.006)] and p.binance.net[BTC] == pytest.approx(0.006)
    pos = (await p.positions(tid))["binance"]
    assert pos.remaining_quantity == pytest.approx(0.006)            # (A booked by the adapter)
    assert pos.data[MANAGE_WANTED]["reduce"]["to"] == pytest.approx(0.003)
    texts = p.texts()
    lines = b_lines(texts)
    assert lines and all("kapatıldı" not in line and "bu kapatma" not in line for line in lines), texts
    assert any(line == (
        "• binance: kısmi kapatma henüz tamamlanmadı: borsada 0.006 açık, bu talimatla 0.003 açık kalmalı "
        "(<code>binance: önceki kapatma emri borsada gerçekleşmiş (0.006 kapandı, kalan 0.006); yeni kapatma emri "
        "gönderilmedi.</code>). Bot binance hesabını yeniden okuyup kalan kısmı kapatacak ve bitince haber verecek")
        for line in lines), texts
    assert any(t.startswith("⏳ #1 BTC LONG: uygulandı; binance: bot sonraki kontrollerde yeniden deneyecek")
               for t in texts), texts
    p.notifier.notifications.clear()
    await passes(p)
    assert btc_closes(p) == [pytest.approx(0.006), pytest.approx(0.003)]
    assert p.binance.net[BTC] == pytest.approx(0.003)
    pos = (await p.positions(tid))["binance"]
    assert pos.remaining_quantity == pytest.approx(0.003) and MANAGE_WANTED not in pos.data
    assert live(p.binance, "stopLossPrice") == [pytest.approx(0.003)]
    assert [t for t in p.texts() if APPLIED in t] == [
        f"✅ #{tid} BTC LONG binance: {APPLIED} — 0.003 kapatıldı (kalan 0.003)."]


async def test_f1_second_close_after_a_pass_that_booked_the_first_is_of_what_is_left(p: Prod) -> None:
    """Control (passes on c1e191b too): a monitor pass between A and B books A; B closes half of the rest."""
    sig, tid = await btc_long(p)
    answer_lost(p.binance, is_close, error=lost_1007)
    lookups(p, {"down": 6})
    await a_owed(p, sig, tid)
    await passes(p, 1)
    assert (await p.positions(tid))["binance"].remaining_quantity == pytest.approx(0.006)
    await p.engine.execute_management(second_half(tid))
    await passes(p)
    assert btc_closes(p) == [pytest.approx(0.006), pytest.approx(0.003)] and p.binance.net[BTC] == pytest.approx(0.003)
    assert any("• binance: %50 kapatıldı" in t for t in p.texts())


async def test_f1_second_close_while_the_look_ups_stay_down_never_closes_the_whole_position(p: Prod) -> None:
    """A executed, and Binance's order look-ups keep failing (positions are readable). A pass drops A as done (the
    venue shows its size reached) while the record still says 0.012. c1e191b then sized B from that record: 0.006 sent
    on a 0.006 position - the whole position closed. Now the venue read before B (VENUE_BELOW_RECORD) sizes it from
    the venue: 0.003 closed, 0.003 left; the record catches up once A can be looked up."""
    sig, tid = await btc_long(p)
    answer_lost(p.binance, is_close, error=lost_1007)
    lookups(p, state := {"down": 50})
    await a_owed(p, sig, tid)
    await passes(p, 1)
    pos = (await p.positions(tid))["binance"]
    assert MANAGE_WANTED not in pos.data and pos.remaining_quantity == pytest.approx(0.012)      # (A not booked)
    assert any("(borsada 0.006 açık, kayıtta 0.012); tekrar gönderilmedi" in t for t in p.texts()), p.texts()
    p.notifier.notifications.clear()
    await p.engine.execute_management(second_half(tid))
    assert btc_closes(p) == [pytest.approx(0.006), pytest.approx(0.003)]
    assert p.binance.net[BTC] == pytest.approx(0.003)
    assert any("• binance: %50 kapatıldı (önceki kapatma emri borsada gerçekleşmiş görünüyordu: borsada 0.006 açık, "
               "kayıtta 0.012; o kısım tekrar gönderilmedi" in t for t in p.texts()), p.texts()
    state["down"] = 0                                    # Binance answers the look-ups again
    await passes(p)
    pos = (await p.positions(tid))["binance"]
    assert btc_closes(p) == [pytest.approx(0.006), pytest.approx(0.003)] and p.binance.net[BTC] == pytest.approx(0.003)
    assert pos.remaining_quantity == pytest.approx(0.003) and not pos.data.get("unconfirmed_closes")
    assert MANAGE_WANTED not in pos.data


@pytest.mark.parametrize("found_in_b", [True, False], ids=["a_found_during_b", "a_not_found_during_b"])
async def test_f1_first_close_executed_only_in_part_then_second_close_ends_at_half_of_what_a_leaves(
        p: Prod, found_in_b: bool) -> None:
    """A (0.006) executed only 0.003 at Binance (the rest expired), its answer lost: Binance holds 0.009. B ends at
    half of what A was to leave (0.006 -> 0.003), whether the adapter books A inside B's close (then B waits for the
    next read) or not. c1e191b kept 0.009 (or closed 0.004 of it)."""
    sig, tid = await btc_long(p)
    close_lost_in_part(p, 0.003)
    lookups(p, state := {"down": 6 if found_in_b else 50})
    await a_owed(p, sig, tid)
    assert p.binance.net[BTC] == pytest.approx(0.009)
    await p.engine.execute_management(second_half(tid))
    state["down"] = 0
    await passes(p)
    assert p.binance.net[BTC] == pytest.approx(0.003)
    assert executed_closes(p) == pytest.approx(0.009)                      # 0.003 (A) + 0.006: never more
    pos = (await p.positions(tid))["binance"]
    assert pos.remaining_quantity == pytest.approx(0.003) and MANAGE_WANTED not in pos.data
    assert not [line for line in b_lines(p.texts()) if "kapatıldı" in line and "henüz" in line]


async def test_f1_first_close_that_never_executed_is_merged_and_closed_once(p: Prod) -> None:
    """Control (as on c1e191b): A never reached Binance; B comes before a pass: one close of 0.009, 0.003 left."""
    sig, tid = await btc_long(p)
    answer_lost(p.binance, is_close, execute=False, error=lost_1007)
    await a_owed(p, sig, tid)
    await p.engine.execute_management(second_half(tid))
    # Review 4 (A2a): a close kept by its client id is dropped only once at least 60 s have also passed since it
    # was sent (it may still execute late); the adapter's clock is moved past that for the syncs below.
    bn = p.brokers["binance"]
    clock = bn._now_ms
    bn._now_ms = lambda: clock() + 61_000
    await passes(p)
    assert btc_closes(p) == [pytest.approx(0.009)] and p.binance.net[BTC] == pytest.approx(0.003)
    pos = (await p.positions(tid))["binance"]
    assert pos.remaining_quantity == pytest.approx(0.003) and MANAGE_WANTED not in pos.data


async def test_f1_merged_target_is_not_dropped_as_gone_and_the_adapters_guard_keeps_it_owed(p: Prod) -> None:
    """A executed (answer lost), B comes while Binance's size cannot be read: nothing is sent, A and B are owed
    together (0.003 to stay open). On the next pass the record still says 0.012 (the sync's look-up fails) while
    Binance holds 0.006. c1e191b dropped the owed close as 'gone' there: B was never applied (0.006 kept). Now the
    0.003 above the target is closed, sized from the venue; when the adapter books A inside that close and sends
    nothing, it stays owed and is sent on the pass after."""
    sig, tid = await btc_long(p)
    answer_lost(p.binance, is_close, error=lost_1007)
    lookups(p, state := {"down": 6})
    await a_owed(p, sig, tid)
    bn = p.brokers["binance"]
    read = bn.venue_position_size

    async def unreadable(symbol: str, side: Side) -> float | None:
        return None
    bn.venue_position_size = unreadable                 # type: ignore[method-assign]
    await p.engine.execute_management(second_half(tid))
    bn.venue_position_size = read                       # type: ignore[method-assign]
    pos = (await p.positions(tid))["binance"]
    assert btc_closes(p) == [pytest.approx(0.006)] and pos.data[MANAGE_WANTED]["reduce"]["to"] == pytest.approx(0.003)
    p.notifier.notifications.clear()
    state["down"] = 1                                   # (the next sync's look-up fails, the close's one finds A)
    await p.engine.monitor.run_once()
    pos = (await p.positions(tid))["binance"]
    assert btc_closes(p) == [pytest.approx(0.006)] and p.binance.net[BTC] == pytest.approx(0.006)
    assert pos.remaining_quantity == pytest.approx(0.006)                    # (A booked inside the close)
    assert pos.data[MANAGE_WANTED]["reduce"]["to"] == pytest.approx(0.003)   # still owed, not dropped
    held = [t for t in p.texts() if "yine uygulanamadı" in t]
    assert len(held) == 1 and "kısmi kapatma henüz tamamlanmadı: borsada 0.006 açık, bu talimatla 0.003 açık " \
                              "kalmalı" in held[0], p.texts()
    assert not [t for t in p.texts() if APPLIED in t or "tekrar gönderilmedi" in t], p.texts()
    p.notifier.notifications.clear()
    await passes(p)
    assert btc_closes(p) == [pytest.approx(0.006), pytest.approx(0.003)] and p.binance.net[BTC] == pytest.approx(0.003)
    pos = (await p.positions(tid))["binance"]
    assert pos.remaining_quantity == pytest.approx(0.003) and MANAGE_WANTED not in pos.data
    assert [t for t in p.texts() if APPLIED in t] == [
        f"✅ #{tid} BTC LONG binance: {APPLIED} — 0.003 kapatıldı (kalan 0.003)."]


# =========================================================================== #
# F2
# =========================================================================== #
async def test_f2_close_of_the_trade_whose_outcome_is_unknown_is_never_called_could_not_be_closed(p: Prod) -> None:
    sig, tid = await btc_long(p)
    answer_lost(p.binance, is_close, error=lost_1007)
    lookups_down(p.binance, times=6)
    p.notifier.notifications.clear()
    await p.say(ParsedAction(action=ActionType.CLOSE, target_trade_id=tid, refers_to_message_id=sig, base_asset="BTC",
                             confidence=0.95, summary_tr="kapat", close_fraction=1.0), KRIPTO, "btc kapattık",
                reply_to=sig)
    texts = p.texts()
    assert not [t for t in texts if "kapatılamadı" in t], texts
    line = next(x for x in b_lines(texts) if x.startswith("• binance"))
    assert line.startswith("• binance: kapatma emrinin sonucu alınamadı; pozisyonun kapanıp kapanmadığı bilinmiyor "
                           "(<code>binance: kapatma emri (0.012 BTC/USDT:USDT) gönderildi ama yanıtı alınamadı"), line
    assert line.endswith("</code>); bot binance hesabını her kontrolde okuyup gerekirse yeniden deneyecek ve kapanınca "
                         "haber verecek"), line
    p.notifier.notifications.clear()
    await passes(p, 3)
    assert p.binance.net.get(BTC, 0.0) == pytest.approx(0.0) and btc_closes(p) == [pytest.approx(0.012)]
    done = [t for t in p.texts() if t.startswith(f"✅ #{tid} BTC LONG binance:")]
    assert len(done) == 1 and done[0].startswith(
        f"✅ #{tid} BTC LONG binance: kapatma emrinin sonucu alınamamıştı; pozisyon artık kapalı (K/Z "), p.texts()
    assert not [t for t in p.texts() if "kapatılamayan" in t], p.texts()


async def test_f2_a_definite_refusal_keeps_its_words(p: Prod) -> None:
    """Control (as on c1e191b): Binance refuses the close outright (-2022): 'pozisyon kapatılamadı', and the retry
    that closes it says 'daha önce kapatılamayan pozisyon yeniden denemede kapatıldı'."""
    sig, tid = await btc_long(p)
    p.binance.fail_when(lambda m, a: m == "create_order" and is_close(a[1], a[5]),
                        binance_error(-2022, "ReduceOnly Order is rejected."))
    p.notifier.notifications.clear()
    await p.say(ParsedAction(action=ActionType.CLOSE, target_trade_id=tid, refers_to_message_id=sig, base_asset="BTC",
                             confidence=0.95, summary_tr="kapat", close_fraction=1.0), KRIPTO, "btc kapattık",
                reply_to=sig)
    assert any(x.startswith("• binance: pozisyon kapatılamadı (<code>") and x.endswith(
        "</code>); bot her kontrolde yeniden deneyecek") for x in b_lines(p.texts())), p.texts()
    await passes(p, 2)
    assert any(t.startswith(f"✅ #{tid} BTC LONG binance: daha önce kapatılamayan pozisyon yeniden denemede kapatıldı")
               for t in p.texts()), p.texts()


def test_f2_cancel_of_a_pending_entry_whose_outcome_is_unknown_is_worded_so() -> None:
    pending = BrokerPosition(broker="binance", broker_symbol=BTC, side=Side.LONG, quantity=0.012,
                             status=PositionStatus.PENDING)
    assert Engine._close_failed_text("binance", pending, "x", unknown=True) == (
        "binance: iptal isteğinin sonucu alınamadı; bekleyen girişin iptal edilip edilmediği bilinmiyor "
        "(<code>x</code>); bot binance hesabını her kontrolde okuyup gerekirse yeniden deneyecek ve iptal edilince "
        "haber verecek")
    assert Engine._close_failed_text("binance", pending, "x") == (
        "binance: bekleyen giriş iptal edilemedi (<code>x</code>); bot her kontrolde yeniden deneyecek")


def test_f2_close_on_arrival_whose_outcome_is_unknown_is_never_called_kapatilamadi() -> None:
    """The late-leg notice (E7 close on arrival): an unknown close outcome says so; a refusal keeps its words."""
    late = BrokerPosition(broker="tickmill", broker_symbol="BTCUSD", side=Side.LONG, quantity=0.06,
                          filled_quantity=0.06, remaining_quantity=0.06, status=PositionStatus.OPEN)
    unknown = orch.BrokerOutcome("tickmill", "error", "tickmill: kapatma emrinin sonucu alınamadı; …", late,
                                 unknown=True)
    line, failed = Engine._closed_on_arrival_line("tickmill", "trader işlemi kapattı", late, unknown)
    assert failed and "KAPATILAMADI" not in line and line == (
        "• 🚨 trader işlemi kapattı ama tickmill hesabındaki 0.06 BTCUSD LONG pozisyonunun kapanıp kapanmadığı "
        "BİLİNMİYOR (tickmill: kapatma emrinin sonucu alınamadı; …). tickmill hesabında kontrol edin; açıksa elle de "
        "kapatabilirsiniz.")
    refused = orch.BrokerOutcome("tickmill", "error", "tickmill: pozisyon kapatılamadı (x)", late)
    line, failed = Engine._closed_on_arrival_line("tickmill", "trader işlemi kapattı", late, refused)
    assert failed and line == ("• 🚨 trader işlemi kapattı ama tickmill hesabındaki 0.06 BTCUSD LONG pozisyonu "
                               "KAPATILAMADI (tickmill: pozisyon kapatılamadı (x)). İsterseniz tickmill hesabında elle "
                               "de kapatabilirsiniz.")


# =========================================================================== #
# F3
# =========================================================================== #
def _wrapped(cause: BaseException, cls: type[BrokerError] = BrokerError, text: str = "binance: emir reddedildi") \
        -> BrokerError:
    try:
        raise cause
    except BaseException as c:      # noqa: BLE001
        try:
            raise cls(text) from c
        except BrokerError as e:
            return e


@pytest.mark.parametrize("cause", [
    pytest.param(lambda: binance_error(-1021, "Timestamp for this request is outside of the recvWindow."),
                 id="binance_-1021_invalid_nonce"),
    pytest.param(lambda: binance_error(-1003, "Too many requests; current limit is 2400 requests per minute.", 429),
                 id="binance_-1003_429"),
    pytest.param(lambda: binance_error(-1015, "Too many new orders.", 429), id="binance_-1015_429"),
    pytest.param(lambda: binance_error(-1003, "Way too many requests; IP banned.", 418), id="binance_418_ban"),
])
def test_f3_venue_refusals_that_ccxt_files_as_network_errors_are_refusals(cause: Any) -> None:
    """The exceptions REAL ccxt raises for these binance answers (InvalidNonce, DDoSProtection: both NetworkErrors) are
    refusals for the adapter (ccxt_broker._unclear: 'reddedildi') and now for the engine too."""
    err = cause()
    assert isinstance(err, cx.NetworkError) and not cb._unclear(err)          # (the adapter: a refusal)
    exc = _wrapped(err)
    assert not no_answer(exc)
    assert orch._owed_why(exc) == "refused"


@pytest.mark.parametrize("cause", [
    pytest.param(lambda: cx.RequestTimeout("binanceusdm POST order request timeout"), id="timeout"),
    pytest.param(lambda: cx.NetworkError("connection reset"), id="network"),
])
def test_f3_lost_answers_stay_no_answer(cause: Any) -> None:
    assert no_answer(_wrapped(cause(), text="binance: kapatma emri gönderildi ama yanıtı alınamadı"))
    # an adapter's explicit unknown outcome is "no answer" whatever its cause
    assert no_answer(_wrapped(cx.DDoSProtection("x"), OrderOutcomeUnknownError, "binance: durumu belirsiz: x"))


# =========================================================================== #
# F4
# =========================================================================== #
async def test_f4_an_unknown_binance_open_is_named_in_every_later_report_and_in_the_closing_summary(p: Prod) -> None:
    answer_lost(p.binance, is_entry)
    lookups_down(p.binance, times=6)
    sig = await p.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO, "btc 1R long piyasadan")
    (trade,) = await p.trades()
    assert "binance" not in await p.positions(trade.id) and "binance: unknown" in (trade.note or "")
    p.notifier.notifications.clear()
    await p.say(ParsedAction(action=ActionType.MOVE_SL, target_trade_id=trade.id, refers_to_message_id=sig,
                             base_asset="BTC", confidence=0.95, new_stop_loss=73500.0, summary_tr="stop 73500"),
                KRIPTO, "stop 73500", reply_to=sig)
    report = next(t for t in p.texts() if "🔗 İşlem" in t)
    assert report.splitlines()[-1] == ("• binance: açılış emrinin sonucu bilinmiyordu; bot bu hesapta pozisyon "
                                       "yönetmiyor. Emir açıldıysa bu değişikliği elle uygulayın."), report
    p.notifier.notifications.clear()
    await p.say(ParsedAction(action=ActionType.CLOSE, target_trade_id=trade.id, refers_to_message_id=sig,
                             base_asset="BTC", confidence=0.95, summary_tr="kapat", close_fraction=1.0), KRIPTO,
                "btc kapattık", reply_to=sig)
    texts = p.texts()
    report = next(t for t in texts if "🔗 İşlem" in t)
    assert report.splitlines()[-1] == ("• binance: açılış emrinin sonucu bilinmiyordu; bot bu hesapta pozisyon "
                                       "yönetmiyor. Emir açıldıysa pozisyonu elle kapatın."), report
    (summary,) = [t for t in texts if "🏁" in t]
    # (the result-in-R headline names the account it knows: it does not speak for binance, where an order may be)
    assert summary.splitlines()[0] == "😐 başa baş kapattık (tickmill 0R)", summary
    assert summary.splitlines()[1].startswith("🏁 #1 BTC LONG (giriş 74000) kapandı."), summary
    assert summary.splitlines()[-1] == ("⚠️ binance: açılış emrinin sonucu bilinmiyordu; emir açıldıysa pozisyonu elle "
                                        "kapatın (bekleyen emir olarak duruyorsa iptal edin)."), summary
    assert p.binance.net.get(BTC) == pytest.approx(0.012)      # (the bot never touched it: the owner is told)


async def test_f4_a_trade_without_an_unknown_open_reads_as_before(p: Prod) -> None:
    sig, tid = await btc_long(p)
    p.notifier.notifications.clear()
    await p.say(ParsedAction(action=ActionType.CLOSE, target_trade_id=tid, refers_to_message_id=sig, base_asset="BTC",
                             confidence=0.95, summary_tr="kapat", close_fraction=1.0), KRIPTO, "btc kapattık",
                reply_to=sig)
    texts = p.texts()
    assert not [t for t in texts if "sonucu bilinmiyordu" in t or "gönderilmiş olabilir" in t], texts
    (summary,) = [t for t in texts if "🏁" in t]
    # (closed at once at about the entry price: the result-in-R headline, then the one line as before)
    headline, money = summary.splitlines()
    assert headline == "😐 başa baş kapattık (0R)" and money.startswith("🏁 #1 BTC LONG (giriş 74000) kapandı."), summary
    assert cid_lookups(p.binance) == 0
