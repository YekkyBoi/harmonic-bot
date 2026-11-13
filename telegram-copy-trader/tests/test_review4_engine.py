"""Review 4 (lane repair4_engine): the engine's owed partial close on the owner's binance (ccxt), after round 3.

  N1  Binance holds less than the bot's record (VENUE_BELOW_RECORD: an earlier partial close A executed, its answer
      was lost and its look-ups are down; a pass dropped A as done). The trader's next partial close B is sized from
      Binance (0.003 of 0.006 to stay open). When that one try does not get through - refused (-1003 / 429, -1021),
      its answer lost with B not executed, held because Binance's size cannot be read, cut off, an unexpected error,
      binance not connected - B is owed with the size it was sized to (0.003 to stay open). 80c712e owed it with a
      target worked out from the record (0.006) and the next pass dropped it as done ("tekrar gönderilmedi" /
      "kısmi kapatma tamam (kalan 0.006)"): Binance kept twice what the trader asked.
  N2  While the binance adapter still looks for a lost close of ours by its client id, the owed retry and the
      trader's next partial close send nothing - also when Binance holds the whole record: the lost close may still
      execute. 80c712e sent the close again at the next pass; when the lost one executed after that, the position
      ended flat although the trader wanted half of it open. "wait" is not a failed try, and the owner is told the
      true reason (never "borsada bu yönde pozisyon görünmüyor" while Binance holds the position).
  N3  The VENUE_BELOW_RECORD note goes once a sync has booked what was behind (the record holds what Binance showed).

On the production replica of tests/test_preflight_scenarios.py with the REAL CcxtBroker over the scenario fake and the
server's config of today (LIVE_YAML: kripto -> binance, breakout, tickmill; notify_on_execution; tickmill optional).
Every test checks Binance first, then what the owner was told.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable

import pytest

from test_review3_ccxt import (  # noqa: F401  (_isolate: autouse)
    BTC,
    KRIPTO,
    Prod,
    _isolate,
    answer_lost,
    binance_error,
    half,
    is_close,
)
from test_review3b import APPLIED, MANAGE_WANTED, a_owed, btc_closes, btc_long, lookups, lost_1007, passes, second_half

from copytrader.engine import monitor as mon
from copytrader.engine import orchestrator as orch
from copytrader.engine.orchestrator import Engine
from copytrader.models import ActionType, BrokerPosition, ParsedAction, PositionStatus, Side

BELOW = "venue_below_record"
WAITING = "önceki kapatma emrinin sonucu henüz bilinmiyor (yanıtı alınamadı, bot borsada arıyor)"


@pytest.fixture
async def p(tmp_path: Any, monkeypatch: pytest.MonkeyPatch):
    prod = await Prod.create(tmp_path, monkeypatch, replica="live")
    yield prod
    await prod.close()


def adapter_clock(p: Prod) -> Callable[[float], None]:
    """The binance adapter's own clock (CcxtBroker._now_ms), moved on by hand: a lost close is given up only after
    enough time and answered look-ups (the adapter's rule, whatever its time floor). Returns ``advance(seconds)``."""
    now = {"ms": int(time.time() * 1000)}
    p.brokers["binance"]._now_ms = lambda: now["ms"]

    def advance(seconds: float) -> None:
        now["ms"] += int(seconds * 1000)
    return advance


async def timed_passes(p: Prod, advance: Callable[[float], None], n: int) -> None:
    for _ in range(n):
        advance(61.0)
        await p.engine.monitor.run_once()


def b_message(trade_id: int, sig: int) -> ParsedAction:
    """B as the trader's message ('kalanın yarısını kapattık')."""
    return ParsedAction(action=ActionType.CLOSE, target_trade_id=trade_id, refers_to_message_id=sig, base_asset="BTC",
                        confidence=0.95, summary_tr="kalanın yarısını kapat", close_fraction=0.5)


async def below_record(p: Prod) -> tuple[int, int, dict[str, int]]:
    """The authors' F1 state: A executed at Binance, its answer lost (-1007), its look-ups down, one pass. The record
    says 0.012, Binance holds 0.006, VENUE_BELOW_RECORD is set, nothing is owed."""
    sig, tid = await btc_long(p)
    answer_lost(p.binance, is_close, error=lost_1007)
    lookups(p, state := {"down": 50})
    await a_owed(p, sig, tid)
    await passes(p, 1)
    pos = (await p.positions(tid))["binance"]
    assert p.binance.net[BTC] == pytest.approx(0.006) and pos.remaining_quantity == pytest.approx(0.012)
    assert pos.data[BELOW]["venue"] == pytest.approx(0.006) and MANAGE_WANTED not in pos.data
    p.notifier.notifications.clear()
    return sig, tid, state


def fail_next_close(fake: Any, mode: str) -> dict[str, Any]:
    """The next reduce-only market close does not get through, in the way ``mode`` names."""
    original = fake.create_order
    state: dict[str, Any] = {"armed": True, "hit": []}

    async def create_order(symbol: str, type: str, side: str, amount: float, price: float | None = None,
                           params: dict | None = None) -> dict[str, Any]:
        if state["armed"] and is_close(type, dict(params or {})):
            state["armed"] = False
            state["hit"].append(amount)
            if mode == "refused_429":
                raise binance_error(-1003, "Too many requests; current limit of IP is 2400 requests per minute.", 429)
            if mode == "refused_1021":
                raise binance_error(-1021, "Timestamp for this request is outside of the recvWindow.")
            if mode == "lost_not_executed":
                fake.calls.append(("create_order:lost", (symbol, type, side, amount, price, dict(params or {}))))
                raise lost_1007()
            if mode == "stray_cancel":
                raise asyncio.CancelledError()
            raise RuntimeError("beklenmedik bir yanıt")          # unexpected
        return await original(symbol, type, side, amount, price, params)
    fake.create_order = create_order        # type: ignore[method-assign]
    return state


async def execute_late(p: Prod) -> None:
    """Binance executes, only now, the close whose request was lost earlier (answer_lost(..., execute=False))."""
    (args,) = [a for m, a in p.binance.calls if m == "create_order:lost"]
    await p.binance.create_order(*args)


# =========================================================================== #
# N1
# =========================================================================== #
N1_MODES = ["refused_429", "refused_1021", "lost_not_executed", "venue_unreadable", "stray_cancel", "unexpected",
            "not_connected"]


@pytest.mark.parametrize("mode", N1_MODES)
async def test_n1_b_sized_from_binance_that_fails_once_keeps_its_size_and_is_applied_once(p: Prod, mode: str) -> None:
    """Every keep branch of _manage_position (BrokerError incl. _ReduceHeld and a lost answer, the stray cancel, the
    unexpected error, not connected) owes B with 0.003 to stay open; once Binance answers the look-ups again, A is
    booked and exactly the 0.003 above it is closed. 80c712e: owed to 0.006, dropped as done, Binance kept 0.006."""
    sig, tid, state = await below_record(p)
    advance = adapter_clock(p)
    bn = p.brokers["binance"]
    read = bn.venue_position_size
    engine_brokers = list(p.engine.brokers)
    if mode == "venue_unreadable":
        async def unreadable(symbol: str, side: Side) -> float | None:
            return None
        bn.venue_position_size = unreadable                 # type: ignore[method-assign]
    elif mode == "not_connected":
        p.engine.brokers = [b for b in engine_brokers if b.name != "binance"]
    else:
        fail_next_close(p.binance, mode)
    if mode == "not_connected":
        await p.say(b_message(tid, sig), KRIPTO, "kalanın yarısını kapattık", reply_to=sig)
    else:
        await p.engine.execute_management(second_half(tid))
    bn.venue_position_size = read                           # type: ignore[method-assign]
    p.engine.brokers = engine_brokers
    pos = (await p.positions(tid))["binance"]
    assert p.binance.net[BTC] == pytest.approx(0.006) and btc_closes(p) == [pytest.approx(0.006)]   # B not through
    assert pos.data[MANAGE_WANTED]["reduce"]["to"] == pytest.approx(0.003)      # (80c712e: 0.006, from the record)
    told = p.texts()
    assert any(line.startswith("#1 binance: ") and "haber verecek" in line for t in told for line in t.splitlines()), \
        told
    p.notifier.notifications.clear()
    state["down"] = 0                                       # Binance answers the look-ups again
    await timed_passes(p, advance, 5)
    pos = (await p.positions(tid))["binance"]
    assert p.binance.net[BTC] == pytest.approx(0.003)
    assert btc_closes(p) == [pytest.approx(0.006), pytest.approx(0.003)]
    assert pos.remaining_quantity == pytest.approx(0.003) and MANAGE_WANTED not in pos.data
    assert not pos.data.get("unconfirmed_closes")
    texts = p.texts()
    assert [t for t in texts if APPLIED in t] == [
        f"✅ #{tid} BTC LONG binance: {APPLIED} — 0.003 kapatıldı (kalan 0.003)."], texts
    assert not any("kısmi kapatma tamam" in t or "tekrar gönderilmedi" in t for t in texts), texts


@pytest.mark.parametrize("path", ["message", "approval"])
async def test_n1_judges_case_refused_while_the_look_ups_stay_down(p: Prod, path: str) -> None:
    """newdefects3 D1: B refused once (-1003 / 429) and the look-ups stay down. B is sent again from Binance's size
    (the venue read shows A went through) and Binance ends at 0.003. 80c712e: 'önceki kapatma emri borsada
    gerçekleşmiş görünüyor ... tekrar gönderilmedi' and Binance kept 0.006."""
    sig, tid, _state = await below_record(p)
    st = fail_next_close(p.binance, "refused_429")
    if path == "message":
        await p.say(b_message(tid, sig), KRIPTO, "kalanın yarısını kapattık", reply_to=sig)
    else:
        await p.engine.execute_management(second_half(tid))
    assert st["hit"] == [pytest.approx(0.003)]
    pos = (await p.positions(tid))["binance"]
    assert pos.data[MANAGE_WANTED]["reduce"]["to"] == pytest.approx(0.003)
    assert any("binance değişikliği reddetti" in t for t in p.texts()), p.texts()
    p.notifier.notifications.clear()
    await passes(p, 4)
    pos = (await p.positions(tid))["binance"]
    assert p.binance.net[BTC] == pytest.approx(0.003)
    assert btc_closes(p) == [pytest.approx(0.006), pytest.approx(0.003)] and MANAGE_WANTED not in pos.data
    texts = p.texts()
    assert [t for t in texts if APPLIED in t] == [
        f"✅ #{tid} BTC LONG binance: {APPLIED} — 0.003 kapatıldı (kalan 0.003) (önceki kapatma emri borsada "
        "gerçekleşmiş görünüyordu: borsada 0.006 açık, kayıtta 0.012; o kısım tekrar gönderilmedi, bu kapatma "
        "borsadaki miktardan yapıldı; binance hesabını kontrol edin)."], texts
    assert not any("kısmi kapatma tamam" in t or "tekrar gönderilmedi, binance" in t for t in texts), texts


def _pos(remaining: float, data: dict[str, Any]) -> BrokerPosition:
    return BrokerPosition(broker="binance", broker_symbol=BTC, side=Side.LONG, status=PositionStatus.OPEN,
                          quantity=0.012, filled_quantity=max(0.012, remaining), remaining_quantity=remaining,
                          data={"qty_step": 0.001, "min_qty": 0.001, **data})


def test_n1_reduce_target_is_of_what_binance_showed_while_it_is_below_the_record() -> None:
    note = {BELOW: {"venue": 0.006, "record": 0.012, "at": "2026-11-13T00:00:00+00:00"}}
    # no note: of the record (as before)
    assert Engine._reduce_target(_pos(0.012, {}), 0.5) == pytest.approx(0.006)
    # the venue showed 0.006 of the record's 0.012: of 0.006 (80c712e: 0.006, of the record)
    assert Engine._reduce_target(_pos(0.012, note), 0.5) == pytest.approx(0.003)
    # an owed earlier partial close that leaves less: that one wins (as before)
    owed = {MANAGE_WANTED: {"reduce": {"to": 0.004}}}
    assert Engine._reduce_target(_pos(0.012, {**note, **owed}), 0.5) == pytest.approx(0.002)
    # the record caught up with the venue (A booked): of 0.006
    assert Engine._reduce_target(_pos(0.006, note), 0.5) == pytest.approx(0.003)
    # the record grew since the read (the rest of the entry filled, +0.004): of 0.006 + 0.004
    assert Engine._reduce_target(_pos(0.016, note), 0.5) == pytest.approx(0.005)
    # a note that cannot be read is ignored
    assert Engine._reduce_target(_pos(0.012, {BELOW: {"venue": "?", "record": None}}), 0.5) == pytest.approx(0.006)
    assert Engine._reduce_target(_pos(0.012, {BELOW: {"venue": 0.006}}), 0.5) == pytest.approx(0.003)


# =========================================================================== #
# N2
# =========================================================================== #
async def test_n2_lost_close_that_executes_after_the_next_pass_is_never_sent_again(p: Prod) -> None:
    """newdefects3b C1 'before_resent_close': A's answer is lost, the look-ups do not find it, the next pass reads
    Binance holding the whole record - 80c712e sent A again there (...c2) and Binance, executing A only after that,
    ended flat. Now nothing is sent while the adapter still looks for A; when A executes it is booked and the owed
    close is done: Binance keeps the half the trader wanted."""
    sig, tid = await btc_long(p)
    answer_lost(p.binance, is_close, execute=False)          # (RequestTimeout; Binance has not executed it yet)
    p.notifier.notifications.clear()
    await p.say(half(tid, sig), KRIPTO, "yarısını kapattık", reply_to=sig)
    pos = (await p.positions(tid))["binance"]
    assert pos.data["unconfirmed_closes"][0]["order_id"] is None
    assert pos.data[MANAGE_WANTED]["reduce"]["to"] == pytest.approx(0.006)
    p.notifier.notifications.clear()
    await passes(p, 2)
    pos = (await p.positions(tid))["binance"]
    assert btc_closes(p) == [] and p.binance.net[BTC] == pytest.approx(0.012)   # (80c712e: ...c2 sent at pass 1)
    wish = pos.data[MANAGE_WANTED]
    assert wish["reduce"]["to"] == pytest.approx(0.006) and int(wish.get("attempts") or 0) == 0   # not a failed try
    assert p.texts() == []                                   # (nothing new happened; the owner was told at A)
    await execute_late(p)                                    # Binance executes A only now
    await passes(p, 2)
    pos = (await p.positions(tid))["binance"]
    assert p.binance.net[BTC] == pytest.approx(0.006) and btc_closes(p) == [pytest.approx(0.006)]
    assert pos.remaining_quantity == pytest.approx(0.006) and MANAGE_WANTED not in pos.data
    assert not pos.data.get("unconfirmed_closes")
    texts = p.texts()
    assert [t for t in texts if APPLIED in t] == [
        f"✅ #{tid} BTC LONG binance: {APPLIED} — kısmi kapatma tamam (kalan 0.006)."], texts
    assert not any("0.006 kapatıldı" in t or "yine uygulanamadı" in t for t in texts), texts


async def test_n2_trader_close_while_a_lost_close_is_looked_for_waits_with_the_true_reason(p: Prod) -> None:
    """The trader's next partial close B 'kalanın yarısı' comes while A (lost) is still looked for and Binance holds
    the whole record. 80c712e: the sync before B sent A again (0.006, ...c2) and then B (0.003), so A, executing
    after that, closed more than the trader asked. Now nothing is sent: B is owed with 0.003 to stay open (merged
    with A), the owner reads why, and once A shows up only 0.003 more is closed."""
    sig, tid = await btc_long(p)
    answer_lost(p.binance, is_close, execute=False)
    await p.say(half(tid, sig), KRIPTO, "yarısını kapattık", reply_to=sig)
    p.notifier.notifications.clear()
    await p.say(b_message(tid, sig), KRIPTO, "kalanın yarısını kapattık", reply_to=sig)
    pos = (await p.positions(tid))["binance"]
    assert btc_closes(p) == [] and p.binance.net[BTC] == pytest.approx(0.012)   # (80c712e: 0.006 + 0.003 sent)
    assert pos.data[MANAGE_WANTED]["reduce"]["to"] == pytest.approx(0.003)
    texts = p.texts()
    assert any(f"#1 binance: <code>kısmi kapatma emri gönderilmedi: {WAITING}</code>; değişiklik şimdi uygulanamadı."
               in line for t in texts for line in t.splitlines()), texts
    assert not any("borsada bu yönde pozisyon görünmüyor" in t for t in texts), texts
    p.notifier.notifications.clear()
    await execute_late(p)
    await passes(p, 2)
    pos = (await p.positions(tid))["binance"]
    assert p.binance.net[BTC] == pytest.approx(0.003)
    assert btc_closes(p) == [pytest.approx(0.006), pytest.approx(0.003)]
    assert pos.remaining_quantity == pytest.approx(0.003) and MANAGE_WANTED not in pos.data
    assert [t for t in p.texts() if APPLIED in t] == [
        f"✅ #{tid} BTC LONG binance: {APPLIED} — 0.003 kapatıldı (kalan 0.003)."], p.texts()


async def test_n2_control_a_lost_close_that_never_reached_binance_is_sent_once_when_the_adapter_gives_it_up(
        p: Prod) -> None:
    """Control (also on 80c712e, only earlier): A never reached Binance. The owed close goes out once the adapter has
    given A up (its answered look-ups and its own time rule), with a new client id, and is applied once."""
    sig, tid = await btc_long(p)
    advance = adapter_clock(p)
    lost = answer_lost(p.binance, is_close, execute=False)
    await p.say(half(tid, sig), KRIPTO, "yarısını kapattık", reply_to=sig)
    p.notifier.notifications.clear()
    await timed_passes(p, advance, 6)
    pos = (await p.positions(tid))["binance"]
    sent = [o["clientOrderId"] for o in p.binance.orders.values() if is_close(o["type"], o["params"])]
    assert len(sent) == 1 and sent[0] != lost["cids"][0] and sent[0].endswith("c2"), (sent, lost)
    assert p.binance.net[BTC] == pytest.approx(0.006) and pos.remaining_quantity == pytest.approx(0.006)
    assert MANAGE_WANTED not in pos.data and not pos.data.get("unconfirmed_closes")
    assert [t for t in p.texts() if APPLIED in t] == [
        f"✅ #{tid} BTC LONG binance: {APPLIED} — 0.006 kapatıldı (kalan 0.006)."], p.texts()


async def test_n2_venue_check_unit() -> None:
    """_venue_reduce_check on its own: 'wait' with the reason while a lost close is held and Binance holds the
    record; 'gone' is unchanged (a lost close that went through is booked inside the close, as before)."""
    class _Bn:
        def __init__(self, size: float) -> None:
            self.size = size

        async def venue_position_size(self, symbol: str, side: Side) -> float | None:
            return self.size

    class _Mon:
        async def _shared_venue_doubt(self, *a: Any) -> str | None:
            return None

    held = {"unconfirmed_closes": [{"order_id": None, "client_id": "ctx1c1", "qty": 0.006, "kind": "close"}]}
    booked = {"unconfirmed_closes": [{"order_id": "77", "qty": 0.006, "kind": "close"}]}
    for data, size, want in ((held, 0.012, ("wait", 0.012, f"{WAITING}")), (booked, 0.012, ("send", 0.012, None)),
                             ({}, 0.012, ("send", 0.012, None)), (held, 0.006, ("gone", 0.006, None))):
        eng = object.__new__(Engine)
        bn = _Bn(size)
        eng._broker = lambda name, bn=bn: bn           # type: ignore[method-assign]
        eng.monitor = _Mon()                            # type: ignore[assignment]
        orig = orch.is_live_ccxt
        orch.is_live_ccxt = lambda b: True              # type: ignore[assignment]
        try:
            got = await eng._venue_reduce_check(None, _pos(0.012, data))     # type: ignore[arg-type]
        finally:
            orch.is_live_ccxt = orig                    # type: ignore[assignment]
        assert (got[0], pytest.approx(got[1]), got[2]) == (want[0], pytest.approx(want[1]), want[2]), (data, got)


# =========================================================================== #
# N3
# =========================================================================== #
async def test_n3_venue_note_goes_once_a_sync_books_what_was_behind(p: Prod) -> None:
    """newdefects3b note: the note stayed after a later booking made the record match Binance (80c712e). Now the sync
    that books A drops it; the next partial close is of what is left, read from the record."""
    sig, tid, state = await below_record(p)
    state["down"] = 0
    await passes(p, 1)
    pos = (await p.positions(tid))["binance"]
    assert pos.remaining_quantity == pytest.approx(0.006) and BELOW not in pos.data      # (80c712e: still there)
    await p.engine.execute_management(second_half(tid))
    await passes(p, 1)
    pos = (await p.positions(tid))["binance"]
    assert p.binance.net[BTC] == pytest.approx(0.003) and pos.remaining_quantity == pytest.approx(0.003)
    assert btc_closes(p) == [pytest.approx(0.006), pytest.approx(0.003)] and BELOW not in pos.data


def test_n3_settled_note_unit() -> None:
    note = {BELOW: {"venue": 0.006, "record": 0.012}}
    assert mon.VENUE_BELOW_RECORD == orch.VENUE_BELOW_RECORD == BELOW
    assert not mon.venue_note_settled(_pos(0.012, note))             # still behind
    assert mon.venue_note_settled(_pos(0.006, note))                 # booked: the record holds what Binance showed
    assert mon.venue_note_settled(_pos(0.0066, note)) is False       # (more than half a step above it)
    assert mon.venue_note_settled(_pos(0.0064, {**note, "qty_step": 0.01})) is True   # (within half a step)
    assert mon.venue_note_settled(_pos(0.003, note))                 # below it: says nothing any more
    assert not mon.venue_note_settled(_pos(0.006, {}))
    assert not mon.venue_note_settled(_pos(0.006, {BELOW: {"venue": None}}))
