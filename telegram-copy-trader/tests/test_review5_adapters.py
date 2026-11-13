"""Review 5 (lane repair5_adapters): the referee's round-4 problems in the broker adapters.

  Q1  fx (native MT5, the owner's prop account) and tickmill (MetaApi, the friend's account) open one leg per TP. A
      partial close whose leg close executes with its answer lost (IPC / 10012 / MetaApi timeout) must not go on and
      close the next leg too (0.46 of 0.47 closed, owner told '%50 kapatıldı'): no further leg is closed while one
      close's outcome is unknown; the call's outcome is unknown, the engine owes the change and reads the account
      before anything is sent again.
  Q2  breakout (manual): the close instruction gives the USD value of the amount to close and of what stays open,
      and its percent is the one the bot's own report line gives.
  Q3  fx netting: an emulated TP whose answer was lost is marked done only once the live volume shows the drop.

The engine-level tests run the production replica of tests/test_review3_engine.py (the server's config of today).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from metaapi_cloud_sdk.clients.timeout_exception import TimeoutException

from copytrader.brokers.base import OrderOutcomeUnknownError
from copytrader.models import ActionType, EntryType, MarketType, ParsedAction, PositionStatus, Side, TakeProfit

from test_preflight_scenarios import EURUSD_LONG, fx_pair
from test_review3_engine import (  # noqa: F401  (_isolate: autouse)
    FX,
    MANAGE_WANTED,
    Prod,
    _isolate,
    fx_closing_deals,
    half,
    make,
    set_eurusd,
)
from test_review4_adapters import SDK_TIMEOUT, fx_lost, is_fx_close, no_failure_words, passes, raised_by


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
async def two_tp_long(p: Prod) -> tuple[int, int]:
    """EURUSD long with two TPs: fx opens two legs (0.23 + 0.24), tickmill two legs (0.1 + 0.11)."""
    set_eurusd(p, 1.1619, 1.1621)
    a = fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET)
    a = a.model_copy(update={"take_profits": [TakeProfit(price=1.1640), TakeProfit(price=1.1660)]})
    sig = await p.say(a, FX, "EUR LONG 1R TP1 1.1640 TP2 1.1660")
    trade = (await p.trades())[-1]
    p.notifier.notifications.clear()
    return sig, trade.id


def three_quarters(trade_id: int) -> ParsedAction:
    return ParsedAction(action=ActionType.CLOSE, close_fraction=0.75, base_asset="EUR", quote_asset="USD",
                        side=Side.LONG, target_trade_id=trade_id, confidence=0.95, summary_tr="EUR %75 kapat")


def shift_clock(p: Prod) -> Any:
    """The engine's and the monitor's clock, moved on by hand (an owed partial close whose answer was lost may be held
    about a minute before its retry). Returns ``advance(seconds)``."""
    base = p.engine._clock
    off = {"s": 0.0}

    def clock() -> Any:
        return base() + timedelta(seconds=off["s"])
    p.engine._clock = clock
    p.engine.monitor._clock = clock

    def advance(seconds: float) -> None:
        off["s"] += seconds
    return advance


def fx_open_lots(p: Prod) -> float:
    return round(sum(x.volume for x in p.terminal.positions.values()), 4)


def tickmill_lossy_close(p: Prod, *, execute: bool) -> None:
    """tickmill's first close request (whole leg or part of one) executes (``execute``) and its answer is lost (SDK
    timeout); later ones go through as usual."""
    state = {"n": 0}
    for name in ("close_position", "close_position_partially"):
        real = getattr(p.tickmill, name)

        def mk(real: Any) -> Any:
            async def lossy(*a: Any, **k: Any) -> Any:
                if state["n"] == 0:
                    state["n"] += 1
                    if execute:
                        await real(*a, **k)
                    raise TimeoutException(SDK_TIMEOUT)
                return await real(*a, **k)
            return lossy
        setattr(p.tickmill, name, mk(real))


def tickmill_closes(p: Prod) -> list[tuple[str, Any]]:
    return [(c[0], c[1][1] if c[0] == "close_position_partially" else c[1][0])
            for c in p.tickmill.calls if c[0] in ("close_position", "close_position_partially")]


# =========================================================================== #
# Q1: a lost leg close stops the partial close
# =========================================================================== #
@pytest.mark.parametrize("answer", ["ipc", "10012"])
@pytest.mark.parametrize("execute", [True, False], ids=["executed", "not_executed"])
async def test_q1_fx_partial_close_of_two_legs_first_leg_lost_closes_one_leg_only(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, execute: bool, answer: str) -> None:
    """'yarısını kapattık' on a 2-TP fx position (legs 0.23 + 0.24): the first leg's closing deal executes or not,
    its answer is lost. Before: the next leg was closed too (deals [0.23, 0.23], 0.01 left, 'fx: %50 kapatıldı').
    Now: the second leg is never touched in that call, the outcome is unknown, and after the owed retry exactly one
    closing deal of 0.23 exists and 0.24 stays open."""
    p = await make(tmp_path, monkeypatch)
    try:
        advance = shift_clock(p)
        sig, tid = await two_tp_long(p)
        pos = (await p.positions(tid))["fx"]
        assert [leg["open_volume"] for leg in pos.data["legs"]] == [0.23, 0.24]
        fx_lost(p, is_fx_close, execute=execute, answer=answer)
        seen = raised_by(p.brokers["fx"], "close_position")
        await p.say(half("EUR", tid, "USD"), FX, "yarısını kapattık", reply_to=sig)
        assert fx_closing_deals(p) == ([0.23] if execute else [])        # the second leg was not closed
        (exc,) = seen
        assert isinstance(exc, OrderOutcomeUnknownError), exc
        texts = p.texts()
        assert not any("fx: %50 kapatıldı" in t for t in texts), texts
        assert any("fx: <code>Kısmi kapatma emrinin sonucu alınamadı: #1001" in t for t in texts), texts
        advance(61)
        await passes(p, tid, "fx", 3)
        assert fx_closing_deals(p) == [0.23]
        assert fx_open_lots(p) == pytest.approx(0.24)
        pos = (await p.positions(tid))["fx"]
        assert pos.remaining_quantity == pytest.approx(0.24) and MANAGE_WANTED not in pos.data
        no_failure_words(p.texts(), "fx")
    finally:
        await p.close()


@pytest.mark.parametrize("execute", [True, False], ids=["executed", "not_executed"])
async def test_q1_fx_partial_close_later_leg_lost_is_unknown_never_done(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, execute: bool) -> None:
    """Close 75% (0.35 of 0.47): leg 1 (0.23) closes, then the 0.12 part of leg 2 executes or not with its answer
    lost. Before: the call returned as done ('fx: %75 kapatıldı'); when the 0.12 had not executed nothing ever sent
    it again (0.24 stayed open). Now the call's outcome is unknown, the owner reads that 0.23 was closed and the rest
    is not known, and after the owed retry exactly 0.35 is closed, never more."""
    p = await make(tmp_path, monkeypatch)
    try:
        advance = shift_clock(p)
        sig, tid = await two_tp_long(p)
        real = p.terminal.order_send
        state = {"n": 0}

        def second_close_lost(request: dict[str, Any]) -> Any:
            if is_fx_close(request):
                state["n"] += 1
                if state["n"] == 2:
                    if execute:
                        real(request)
                    p.terminal.last = (-10004, "IPC send failed")
                    return None
            return real(request)
        p.terminal.order_send = second_close_lost          # type: ignore[method-assign]
        seen = raised_by(p.brokers["fx"], "close_position")
        await p.say(three_quarters(tid), FX, "%75 kapattık", reply_to=sig)
        assert fx_closing_deals(p) == ([0.23, 0.12] if execute else [0.23])
        (exc,) = seen
        assert isinstance(exc, OrderOutcomeUnknownError), exc
        assert "0.23 lot kapatıldı" in str(exc) and "bilinmiyor" in str(exc), exc
        known = getattr(exc, "position", None)          # what the account showed after it, handed back
        assert known is not None and known.remaining_quantity == pytest.approx(0.12 if execute else 0.24)
        texts = p.texts()
        assert not any("fx: %75 kapatıldı" in t for t in texts), texts
        assert any("0.23 lot kapatıldı" in t for t in texts), texts
        advance(61)
        await passes(p, tid, "fx", 3)
        assert fx_closing_deals(p) == [0.23, 0.12]
        assert fx_open_lots(p) == pytest.approx(0.12)
        pos = (await p.positions(tid))["fx"]
        assert pos.remaining_quantity == pytest.approx(0.12) and MANAGE_WANTED not in pos.data
        no_failure_words(p.texts(), "fx")
    finally:
        await p.close()


@pytest.mark.parametrize("execute", [True, False], ids=["executed", "not_executed"])
async def test_q1_tickmill_partial_close_of_two_legs_first_leg_lost_closes_one_leg_only(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, execute: bool) -> None:
    """The same on tickmill (legs 0.1 + 0.11, half = 0.1 = the first leg). Before: close_position(leg 1) lost, then
    close_position_partially(0.1) of leg 2: 0.01 of 0.21 left. Now: one close of 0.1 in all."""
    p = await make(tmp_path, monkeypatch)
    try:
        advance = shift_clock(p)
        sig, tid = await two_tp_long(p)
        pos = (await p.positions(tid))["tickmill"]
        assert [leg["open_volume"] for leg in pos.data["legs"]] == [0.1, 0.11]
        first = pos.data["legs"][0]["ticket"]
        tickmill_lossy_close(p, execute=execute)
        seen = raised_by(p.brokers["tickmill"], "close_position")
        await p.say(half("EUR", tid, "USD"), FX, "yarısını kapattık", reply_to=sig)
        await asyncio.sleep(0.2)
        assert tickmill_closes(p) == ([("close_position", first)] if execute else [])
        (exc,) = seen
        assert isinstance(exc, OrderOutcomeUnknownError), exc
        assert not any("tickmill: %50 kapatıldı" in t for t in p.texts()), p.texts()
        advance(61)
        await passes(p, tid, "tickmill", 3)
        assert tickmill_closes(p) == [("close_position", first)]
        pos = (await p.positions(tid))["tickmill"]
        assert pos.remaining_quantity == pytest.approx(0.11) and MANAGE_WANTED not in pos.data
        no_failure_words(p.texts(), "tickmill")
    finally:
        await p.close()


# =========================================================================== #
# Q2: breakout - the close instruction gives USD values, no percent
# =========================================================================== #
async def test_q2_close_instruction_gives_the_usd_value_of_the_close_and_of_what_stays_open(tmp_path: Path) -> None:
    """Like the open instruction (quantity x contract size x price, 'tutar USD olarak giriliyorsa N USD girin'), at
    the price the record closed at now. Before: coin amounts only and 'açık pozisyonun %50'si'."""
    from test_review4_adapters import manual_open
    broker, notifier, pos = await manual_open(tmp_path, "ETH", 0.071)
    new = await broker.close_position(pos, 0.5)
    price = new.data["exits"][-1]["price"]              # the current quote the record closed at
    closed, left = pos.remaining_quantity - new.remaining_quantity, new.remaining_quantity
    assert (closed, left) == (pytest.approx(0.035), pytest.approx(0.036))
    (text,) = [t for t, _ in notifier.notifications]
    assert text == (
        f"<b>❌ ETH: 0.035 ETH KAPAT</b> (≈ <b>{closed * price:.2f} USD</b> tutarında, şu anki fiyatla; tutar USD "
        f"olarak giriliyorsa {closed * price:.2f} USD girin; kapattıktan sonra 0.036 ETH ≈ {left * price:.2f} USD "
        f"açık kalmalı)\n✋ Elle işlem (breakout)")
    assert "%" not in text                  # (was "yaklaşık %49'u" while the bot's report line says "%50")
    notifier.notifications.clear()
    await broker.close_position(new, 1.0)
    (text,) = [t for t, _ in notifier.notifications]
    price = (await broker.get_quote((await broker.resolve_symbol("ETH", "USDT", MarketType.PERP, "ETHUSDT.P")))).ask
    assert text == (f"<b>❌ ETH: POZİSYONUN TAMAMINI KAPAT (0.036 ETH)</b> (≈ {0.036 * price:.2f} USD, şu anki "
                    f"fiyatla)\n✋ Elle işlem (breakout)")


async def test_q2_without_a_price_the_instruction_says_so_instead_of_a_number(tmp_path: Path) -> None:
    """When the record closed nothing at a known price (no new exit), no USD number is made up."""
    from test_review4_adapters import manual_open
    broker, notifier, pos = await manual_open(tmp_path, "ETH", 2.0)
    after = pos.model_copy(deep=True)
    after.remaining_quantity = 1.0
    text = broker._close_text(pos, after)
    assert text == ("<b>❌ ETH: 1 ETH KAPAT</b> (şu anki fiyat alınamadı, USD karşılığı verilemiyor; kapattıktan "
                    "sonra 1 ETH açık kalmalı)\n✋ Elle işlem (breakout)")
    after.remaining_quantity = 0.0
    after.status = PositionStatus.CLOSED
    assert broker._close_text(pos, after) == ("<b>❌ ETH: POZİSYONUN TAMAMINI KAPAT (2 ETH)</b> (şu anki fiyat "
                                              "alınamadı, USD karşılığı verilemiyor)\n✋ Elle işlem (breakout)")


# =========================================================================== #
# Q3a: a stop move whose answer was lost on one leg (not set there) is sent again by the next sync
# =========================================================================== #
def fx_nth_lost(p: Prod, picks: Any, nth: int, *, execute: bool, refuse_after: bool = False) -> dict[str, Any]:
    """The ``nth`` request that ``picks`` selects executes (``execute``) and its answer is lost (IPC). With
    ``refuse_after`` every later one is refused (10016 invalid stops)."""
    from copytrader.brokers.mt5_broker import MT5
    term = p.terminal
    real = term.order_send
    state: dict[str, Any] = {"n": 0, "sent": []}

    def send(request: dict[str, Any]) -> Any:
        if picks(request):
            state["n"] += 1
            state["sent"].append(dict(request))
            if state["n"] == nth:
                if execute:
                    real(request)
                term.last = (-10004, "IPC send failed")
                return None
            if refuse_after and state["n"] > nth:
                return term._result(MT5.TRADE_RETCODE_INVALID_STOPS, comment="Invalid stops")
        return real(request)
    term.order_send = send                  # type: ignore[method-assign]
    return state


@pytest.mark.parametrize("execute", [True, False], ids=["executed", "not_executed"])
async def test_q3a_fx_stop_move_lost_on_the_second_leg_is_sent_again_when_the_leg_kept_the_old_stop(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, execute: bool) -> None:
    """Stop to 1.1610 on a 2-leg fx position; leg 1003's SLTP answer is lost. Before: when it had not been set, the
    leg kept 1.16 for good (nothing owed, nothing re-sent). Now the next sync reads the leg and sends it again only
    when the venue still shows the old stop: both legs end at 1.161, one extra request at most."""
    from test_review3_engine import stop_at
    from test_review4_adapters import is_fx_sltp
    p = await make(tmp_path, monkeypatch)
    try:
        sig, tid = await two_tp_long(p)
        state = fx_nth_lost(p, is_fx_sltp, 2, execute=execute)
        await p.say(stop_at(tid, 1.1610), FX, "stop 1.1610", reply_to=sig)
        sl = {k: v.sl for k, v in p.terminal.positions.items()}
        assert sl == ({1001: 1.161, 1003: 1.161} if execute else {1001: 1.161, 1003: 1.16}), sl
        await passes(p, tid, "fx", 3)
        assert {k: v.sl for k, v in p.terminal.positions.items()} == {1001: 1.161, 1003: 1.161}
        assert [r["position"] for r in state["sent"]] == ([1001, 1003] if execute else [1001, 1003, 1003])
        pos = (await p.positions(tid))["fx"]
        assert pos.stop_loss == pytest.approx(1.161)
        assert not any("sl_wanted" in leg for leg in pos.data["legs"])
    finally:
        await p.close()


async def test_q3a_a_refused_resend_is_reported_once_and_not_tried_again(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from test_review3_engine import stop_at
    from test_review4_adapters import is_fx_sltp
    p = await make(tmp_path, monkeypatch)
    try:
        sig, tid = await two_tp_long(p)
        state = fx_nth_lost(p, is_fx_sltp, 2, execute=False, refuse_after=True)
        await p.say(stop_at(tid, 1.1610), FX, "stop 1.1610", reply_to=sig)
        p.notifier.notifications.clear()
        await passes(p, tid, "fx", 3)
        assert [r["position"] for r in state["sent"]] == [1001, 1003, 1003]
        told = [t for t in p.texts() if "#1003: stop 1.161 bu bacakta yeniden gönderildi ama uygulanamadı" in t]
        assert len(told) == 1 and "stopu 1.16 olarak kaldı" in told[0], p.texts()
        assert {k: v.sl for k, v in p.terminal.positions.items()} == {1001: 1.161, 1003: 1.16}
    finally:
        await p.close()


# =========================================================================== #
# Q3b: netting - an emulated TP whose answer was lost is done only once the volume shows it
# =========================================================================== #
@pytest.mark.parametrize("execute", [True, False], ids=["executed", "not_executed"])
async def test_q3b_netting_emulated_tp_lost_is_done_only_when_the_volume_drop_is_seen(execute: bool) -> None:
    """Before: marked done at once; when it had not executed, TP1 was never taken (0.1 stayed open, 'MetaTrader'da
    kontrol edin'). Now: done once the volume drop shows; not sent again for 60 s; after that, when the account
    still shows the whole volume and the price is still at the target, sent again once."""
    from types import SimpleNamespace

    from test_mt5_broker import SERVER_TIME, FakeMT5, connected, make_plan, make_req
    from test_review4_adapters import IPC
    fake = FakeMT5(margin_mode=0)
    b = await connected(fake)
    clock = {"t": 1_000_000.0}
    b._wall_clock = lambda: clock["t"]                   # type: ignore[method-assign]
    sym = await b.resolve_symbol("ETH")
    pos = await b.open_position(make_req(sym, 0.10, make_plan(tps=((2260.0, 0.5), (2310.6, 0.5)))))
    fake.ticks["ETHUSD"] = SimpleNamespace(bid=2261.0, ask=2261.2, last=0.0, time=SERVER_TIME)
    real_send = fake.order_send
    state = {"lost": 0}

    def send(request: dict[str, Any]) -> Any:
        if is_fx_close(request) and state["lost"] == 0:
            state["lost"] += 1
            if execute:
                real_send(request)
            fake.last = IPC
            return None
        return real_send(request)
    fake.order_send = send                              # type: ignore[method-assign]
    pos = await b.sync_position(pos)
    closes = lambda: [r for r in fake.sent if is_fx_close(r)]     # noqa: E731
    tp1 = pos.data["emulated_tps"][0]
    assert tp1["done"] is execute
    assert "TP1 (2260) kısmi kapatma emrine yanıt alınamadı" in (pos.error or "")
    for _ in range(2):                                  # inside the wait: nothing sent again
        clock["t"] += 20
        pos = await b.sync_position(pos)
    assert len(closes()) == (1 if execute else 0)
    clock["t"] += 30                                    # 70 s after the lost answer
    pos = await b.sync_position(pos)
    pos = await b.sync_position(pos)
    assert len(closes()) == 1
    assert sum(x.volume for x in fake.positions.values()) == pytest.approx(0.05)
    assert pos.remaining_quantity == pytest.approx(0.05) and pos.data["emulated_tps"][0]["done"] is True
    await b.close()
