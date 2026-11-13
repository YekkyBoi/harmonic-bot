"""Review 5 (lane repair5_engine): the referee's round-4 problems in the engine (newdefects4), on the production
replica (REAL Engine, Monitor and the Mt5Broker / MetaApiBroker / CcxtBroker adapters on the fake venues, live config).
  P1  fx (native MT5) and tickmill (MetaApi): a partial close whose answer was lost and that the venue executes only
      after the next monitor pass is never closed a second time. Its owed retry is held about a minute (venue reads
      first); a new partial close meanwhile is merged into it (owed, not sent from the record); a lost stop move is
      still retried at once. The owner is told that the bot waits about a minute and reads the position again.
  P2  binance: an owed partial close that waits because the adapter still holds a lost close whose look-ups get no
      answer tells the owner once after about 5 minutes (and still sends nothing).
  P3  fx multi-leg partial close: when the adapter reports the outcome unknown after one leg closed, the engine owes
      only what is still above the target.
The P1 and P2 tests fail on 6c6ec84 (this file run against a `git archive HEAD` export of it); the P3 tests are
guards of the engine side of the adapter lane's change (they pass there too: without a late execution the old
immediate retry was right).
"""
from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from test_review3_engine import (  # noqa: F401  (_isolate: autouse)
    FX, MANAGE_WANTED, OrderOutcomeUnknownError, Prod, _isolate, due_now, eur_long, fx_closing_deals, half, make,
    stop_at)
from test_review4_adapters import IPC, SDK_TIMEOUT, fx_lost, is_fx_close, is_fx_sltp, passes
from metaapi_cloud_sdk.clients.timeout_exception import TimeoutException

APPLIED = "daha önce uygulanamayan değişiklik şimdi uygulandı"
WAITS = "Bot yaklaşık bir dakika bekleyip pozisyonu yeniden okuyacak"


def shift_clock(p: Prod) -> Any:
    """The engine's and the monitor's clock, moved on by hand. Returns ``advance(seconds)``."""
    base = p.engine._clock
    off = {"s": 0.0}

    def clock() -> Any:
        return base() + timedelta(seconds=off["s"])
    p.engine._clock = clock
    p.engine.monitor._clock = clock

    def advance(seconds: float) -> None:
        off["s"] += seconds
    return advance


def fx_lost_late(p: Prod) -> dict[str, Any]:
    """fx: the first closing deal gets no answer (IPC) and is NOT executed now; ``st["run"]()`` executes it later
    (the terminal processing the lost request on its own)."""
    term = p.terminal
    real = term.order_send
    st: dict[str, Any] = {"n": 0, "late": None}

    def send(req: dict[str, Any]) -> Any:
        if is_fx_close(req):
            st["n"] += 1
            if st["n"] == 1:
                st["late"] = dict(req)
                term.last = IPC
                return None
        return real(req)

    def run() -> None:
        if st["late"] is not None:
            r, st["late"] = st["late"], None
            real(r)
    term.order_send = send
    st["run"] = run
    return st


def tm_lost_late(p: Prod, method: str = "close_position_partially") -> dict[str, Any]:
    """tickmill: the first ``method`` request times out (SDK) and is not executed now; ``await st["run"]()`` executes
    it later."""
    real = getattr(p.tickmill, method)
    st: dict[str, Any] = {"n": 0, "late": None}

    async def lossy(*a: Any, **k: Any) -> Any:
        st["n"] += 1
        if st["n"] == 1:
            st["late"] = (a, k)
            raise TimeoutException(SDK_TIMEOUT)
        return await real(*a, **k)

    async def run() -> None:
        if st["late"] is not None:
            (a, k), st["late"] = st["late"], None
            await real(*a, **k)
    setattr(p.tickmill, method, lossy)
    st["run"] = run
    return st


def tm_partial_calls(p: Prod) -> list[Any]:
    return [c for c in p.tickmill.calls if c[0] == "close_position_partially"]


async def tm_volume(p: Prod) -> float:
    return round(sum(float(x.get("volume") or 0) for x in await p.tickmill.get_positions()), 8)


def fx_volume(p: Prod) -> float:
    return round(sum(x.volume for x in p.terminal.positions.values()), 8)


# =========================================================================== #
# P1: fx / tickmill - a lost partial close is held, never closed twice
# =========================================================================== #
@pytest.mark.parametrize("late", [True, False])
async def test_p1_fx_lost_partial_close_executing_after_the_next_pass_is_closed_once(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, late: bool) -> None:
    """newdefects4 problem 1 (fx): 'yarısını kapattık' -> the closing deal's answer is lost (IPC). Passes within a
    minute send nothing; the lost deal executes after them (``late``) or never. After the hold: exactly one closing
    deal of 0.23 in both cases (6c6ec84: deals [0.23, 0.23], 0.01 left)."""
    p = await make(tmp_path, monkeypatch)
    try:
        advance = shift_clock(p)
        sig, tid = await eur_long(p)
        assert fx_volume(p) == pytest.approx(0.47)
        st = fx_lost_late(p)
        await p.say(half("EUR", tid, "USD"), FX, "yarısını kapattık", reply_to=sig)
        first = [t for t in p.texts() if t.startswith(f"#{tid} fx:")]
        await passes(p, tid, "fx", 2)               # (forced due: still nothing is sent inside the hold)
        assert fx_closing_deals(p) == [] and st["n"] == 1
        if late:
            st["run"]()                             # the lost request executes now, after those reads
        advance(20)
        await passes(p, tid, "fx", 1)
        assert st["n"] == 1
        advance(45)                                 # (the hold is over)
        await passes(p, tid, "fx", 3)
        assert fx_closing_deals(p) == [0.23], fx_closing_deals(p)
        assert fx_volume(p) == pytest.approx(0.24)
        pos = (await p.positions(tid))["fx"]
        assert pos.remaining_quantity == pytest.approx(0.24) and MANAGE_WANTED not in pos.data
        done = [t for t in p.texts() if APPLIED in t and "fx" in t]
        assert len(done) == 1, p.texts()
        assert ("kısmi kapatma tamam (kalan 0.24)" if late else "0.23 kapatıldı (kalan 0.24)") in done[0]
        # what the owner read first: the bot waits about a minute and reads the position again
        assert first and WAITS in first[0] and "Bot yeniden deneyecek ve uygulayınca" not in first[0], first
    finally:
        await p.close()


@pytest.mark.parametrize("late", [True, False])
async def test_p1_tickmill_lost_partial_close_executing_after_the_next_pass_is_closed_once(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, late: bool) -> None:
    """newdefects4 problem 1 (tickmill): the SDK times out on close_position_partially(0.1); it executes later or
    never. One partial close call reaches the venue when it executed, two (the first lost, one retry) when not; the
    venue ends at 0.11 (6c6ec84: two executed calls, 0.01 left)."""
    p = await make(tmp_path, monkeypatch)
    try:
        advance = shift_clock(p)
        sig, tid = await eur_long(p)
        assert await tm_volume(p) == pytest.approx(0.21)
        st = tm_lost_late(p)
        await p.say(half("EUR", tid, "USD"), FX, "yarısını kapattık", reply_to=sig)
        await asyncio.sleep(0.1)
        first = [t for t in p.texts() if t.startswith(f"#{tid} tickmill:")]
        await passes(p, tid, "tickmill", 2)
        assert st["n"] == 1
        if late:
            await st["run"]()
        advance(61)
        await passes(p, tid, "tickmill", 3)
        assert await tm_volume(p) == pytest.approx(0.11)
        assert st["n"] == (1 if late else 2)
        pos = (await p.positions(tid))["tickmill"]
        assert pos.remaining_quantity == pytest.approx(0.11) and MANAGE_WANTED not in pos.data
        assert len([t for t in p.texts() if APPLIED in t and "tickmill" in t]) == 1, p.texts()
        assert first and WAITS in first[0], first
    finally:
        await p.close()


@pytest.mark.parametrize("late", [True, False])
async def test_p1_fx_second_partial_close_inside_the_hold_is_merged_not_sent(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, late: bool) -> None:
    """While the lost 'yarısını kapattık' is held, the trader says 'kalanın yarısını da kapattık': nothing is sent from
    the record (6c6ec84 sends 0.35 at once); after the hold the position ends at 25 % (0.12 lots), whether the lost
    deal executed late or not."""
    p = await make(tmp_path, monkeypatch)
    try:
        advance = shift_clock(p)
        sig, tid = await eur_long(p)
        st = fx_lost_late(p)
        await p.say(half("EUR", tid, "USD"), FX, "yarısını kapattık", reply_to=sig)
        advance(5)
        await p.say(half("EUR", tid, "USD"), FX, "kalanın yarısını da kapattık", reply_to=sig)
        assert fx_closing_deals(p) == [] and st["n"] == 1
        pos = (await p.positions(tid))["fx"]
        assert pos.data[MANAGE_WANTED]["reduce"]["to"] == pytest.approx(0.1175)
        merged = [t for t in p.texts() if t.startswith(f"#{tid} fx:")][-1]
        await passes(p, tid, "fx", 2)
        assert fx_closing_deals(p) == []
        if late:
            st["run"]()
        advance(61)
        await passes(p, tid, "fx", 3)
        assert fx_volume(p) == pytest.approx(0.12), fx_closing_deals(p)
        assert sum(fx_closing_deals(p)) == pytest.approx(0.35)
        assert "bekleyen kısmi kapatmaya eklendi" in merged and "yaklaşık bir dakika" in merged, merged
    finally:
        await p.close()


async def test_p1_lost_stop_move_is_still_retried_at_once_while_a_partial_close_is_held(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A held partial close does not hold a stop move: tickmill's lost partial close (not executed) is held, its stop
    move whose answer is lost (not executed) is set at the next pass; the partial close only after the hold."""
    p = await make(tmp_path, monkeypatch)
    try:
        advance = shift_clock(p)
        sig, tid = await eur_long(p)
        tm_lost_late(p)
        mod = tm_lost_late(p, "modify_position")
        await p.say(half("EUR", tid, "USD"), FX, "yarısını kapattık", reply_to=sig)
        await asyncio.sleep(0.1)
        await p.say(stop_at(tid, 1.1615), FX, "stop 1.1615", reply_to=sig)
        await asyncio.sleep(0.1)
        assert mod["n"] == 1
        await passes(p, tid, "tickmill", 1)
        pos = (await p.positions(tid))["tickmill"]
        assert mod["n"] == 2 and pos.stop_loss == pytest.approx(1.1615)
        assert await tm_volume(p) == pytest.approx(0.21)          # (the partial close is still held)
        assert "reduce" in pos.data[MANAGE_WANTED] and "sl" not in pos.data[MANAGE_WANTED]
        advance(61)
        await passes(p, tid, "tickmill", 2)
        assert await tm_volume(p) == pytest.approx(0.11)
    finally:
        await p.close()


async def test_p1_held_retry_that_loses_its_answer_again_is_held_again(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """fx: the lost partial close (not executed) is retried after the hold, and that retry's answer is lost too and it
    executes late: it is held once more, never sent a third time on top of it."""
    p = await make(tmp_path, monkeypatch)
    try:
        advance = shift_clock(p)
        sig, tid = await eur_long(p)
        term = p.terminal
        real = term.order_send
        st: dict[str, Any] = {"n": 0, "late": None}

        def send(req: dict[str, Any]) -> Any:
            if is_fx_close(req):
                st["n"] += 1
                if st["n"] <= 2:                     # the first two closing requests lose their answer
                    st["late"] = dict(req) if st["n"] == 2 else None
                    term.last = IPC
                    return None
            return real(req)
        term.order_send = send
        await p.say(half("EUR", tid, "USD"), FX, "yarısını kapattık", reply_to=sig)
        advance(61)
        await passes(p, tid, "fx", 1)                # the held retry: its answer is lost too
        assert st["n"] == 2
        await passes(p, tid, "fx", 2)
        assert st["n"] == 2                          # held again
        real(st["late"])                             # the second request executes late
        advance(61)
        await passes(p, tid, "fx", 3)
        assert st["n"] == 2 and fx_closing_deals(p) == [0.23] and fx_volume(p) == pytest.approx(0.24)
        assert MANAGE_WANTED not in (await p.positions(tid))["fx"].data
    finally:
        await p.close()


# =========================================================================== #
# P3: an adapter that reports the outcome unknown after one leg closed
# =========================================================================== #
@pytest.mark.parametrize("executed", [True, False])
async def test_p3_fx_multi_leg_partial_close_reported_unknown_owes_only_the_rest(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, executed: bool) -> None:
    """'%75 kapattık' on fx (legs 0.23 + 0.24): the first leg closes, the second leg's close loses its answer
    (executed or not) and the adapter reports the whole close as unknown (OrderOutcomeUnknownError, the adapter lane's
    contract). The engine owes the target 0.1175 and, after the hold and the venue read, closes only what is still
    above it: the account ends at 0.12 lots, never below."""
    p = await make(tmp_path, monkeypatch)
    try:
        advance = shift_clock(p)
        sig, tid = await eur_long(p)
        fx = p.brokers["fx"]
        real_close = fx.close_position
        term = p.terminal
        real_send = term.order_send
        st = {"n": 0}

        def send(req: dict[str, Any]) -> Any:
            if is_fx_close(req):
                st["n"] += 1
                if st["n"] == 2:
                    if executed:
                        real_send(req)
                    term.last = IPC
                    return None
            return real_send(req)
        term.order_send = send

        async def close_position(pos: Any, fraction: float = 1.0) -> Any:
            if st["n"] == 0:
                await real_close(pos, fraction)
                raise OrderOutcomeUnknownError("Kısmi kapatma emrinin sonucu alınamadı: #1003: MT5 emir yanıtı "
                                               "alınamadı (IPC send failed (kod -10004)).")
            return await real_close(pos, fraction)
        fx.close_position = close_position
        three_quarters = half("EUR", tid, "USD").model_copy(update={"close_fraction": 0.75})
        await p.say(three_quarters, FX, "%75 kapattık", reply_to=sig)
        pos = (await p.positions(tid))["fx"]
        assert pos.data[MANAGE_WANTED]["reduce"]["to"] == pytest.approx(0.1175)
        await passes(p, tid, "fx", 2)
        advance(61)
        await passes(p, tid, "fx", 3)
        assert fx_volume(p) == pytest.approx(0.12), (fx_closing_deals(p), fx_volume(p))
        assert MANAGE_WANTED not in (await p.positions(tid))["fx"].data
    finally:
        await p.close()


# =========================================================================== #
# P2: binance - a long wait for a lost close is told once
# =========================================================================== #
async def test_p2_binance_owed_partial_close_waiting_on_a_lost_close_is_told_once(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """newdefects4 problem 2: 'yarısını kapattık' never reaches Binance and its answer is lost; every client-id
    look-up gets no answer from then on. The bot keeps waiting (nothing sent, 0.012 held) and, once the change has
    been owed about 5 minutes, tells the owner once (6c6ec84: never told)."""
    from test_review3_ccxt import answer_lost, is_close
    from test_review3b import btc_long
    from test_review3_ccxt import KRIPTO, half as bhalf, BTC
    from test_preflight_scenarios import Prod as P0
    from test_review4_engine import adapter_clock
    from ccxt.base import errors as cx

    p = await P0.create(tmp_path, monkeypatch, replica="live")
    try:
        advance = shift_clock(p)
        advance_bn = adapter_clock(p)
        sig, tid = await btc_long(p)
        answer_lost(p.binance, is_close, execute=False)

        def down(m: str, a: tuple[Any, ...]) -> bool:
            return m == "fetch_order" and bool((a[2] or {}).get("clientOrderId"))
        p.binance.fail_when(down, cx.NetworkError("binanceusdm GET https://fapi.binance.com/fapi/v1/order "
                                                  "connection reset"), times=10**6)
        await p.say(bhalf(tid, sig), KRIPTO, "yarısını kapattık", reply_to=sig)
        n0 = len(p.texts())

        async def tick(n: int) -> None:
            for _ in range(n):
                advance(61)
                advance_bn(61)
                await p.engine.monitor.run_once()
        await tick(3)                               # ~3 minutes: no notice yet
        assert len(p.texts()) == n0, p.texts()[n0:]
        await tick(3)                               # past 5 minutes: told once
        told = [t for t in p.texts()[n0:] if "hâlâ gönderilmedi" in t]
        assert len(told) == 1, p.texts()[n0:]
        assert told[0].startswith(f"⚠️ #{tid} BTC LONG binance: bekleyen kısmi kapatma (")
        assert "borsada aranamıyor (sorgulara yanıt yok)" in told[0] and "gerekirse elle kapatın" in told[0]
        await tick(10)
        assert len([t for t in p.texts()[n0:] if "hâlâ gönderilmedi" in t]) == 1
        pos = (await p.positions(tid))["binance"]
        assert p.binance.net.get(BTC) == pytest.approx(0.012) and pos.remaining_quantity == pytest.approx(0.012)
        assert pos.data[MANAGE_WANTED]["reduce"]["to"] == pytest.approx(0.006)
        assert int(pos.data[MANAGE_WANTED].get("attempts") or 0) == 0     # (not a failed try: no back-off)
    finally:
        await p.close()


async def test_p2_flag_cleared_when_the_wait_ends(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The told flag goes once the wait is over (the look-ups come back, the adapter drops the lost close, the owed
    partial close is sent once)."""
    from test_review3_ccxt import answer_lost, is_close
    from test_review3b import btc_long
    from test_review3_ccxt import KRIPTO, half as bhalf, BTC
    from test_preflight_scenarios import Prod as P0
    from test_review4_engine import adapter_clock
    from ccxt.base import errors as cx

    p = await P0.create(tmp_path, monkeypatch, replica="live")
    try:
        advance = shift_clock(p)
        advance_bn = adapter_clock(p)
        sig, tid = await btc_long(p)
        answer_lost(p.binance, is_close, execute=False)
        state = {"down": True}

        def down(m: str, a: tuple[Any, ...]) -> bool:
            return state["down"] and m == "fetch_order" and bool((a[2] or {}).get("clientOrderId"))
        p.binance.fail_when(down, cx.NetworkError("binanceusdm GET https://fapi.binance.com/fapi/v1/order "
                                                  "connection reset"), times=10**6)
        await p.say(bhalf(tid, sig), KRIPTO, "yarısını kapattık", reply_to=sig)

        async def tick(n: int) -> None:
            for _ in range(n):
                advance(61)
                advance_bn(61)
                await p.engine.monitor.run_once()
        await tick(6)
        assert (await p.positions(tid))["binance"].data[MANAGE_WANTED].get("lost_wait_told") is True
        state["down"] = False
        await tick(6)
        pos = (await p.positions(tid))["binance"]
        assert p.binance.net.get(BTC) == pytest.approx(0.006)
        assert MANAGE_WANTED not in pos.data
        assert len([t for t in p.texts() if APPLIED in t and "binance" in t]) == 1
    finally:
        await p.close()
