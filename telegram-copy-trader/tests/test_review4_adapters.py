"""Review 4 (lane repair4_adapters): the referee's round-3 problems in the broker adapters.

  A1  fx (native MT5, the owner's prop account) and tickmill (MetaApi, the friend's account): a close, partial close,
      stop / TP move or cancel whose request went out and whose answer was lost is an unknown outcome
      (OrderOutcomeUnknownError, its cause the lost leg error), never "kapatılamadı" / "reddetti"; the owed retry of a
      partial close never repeats it on the venue; a definite refusal keeps its words.
  A2  binance: a close kept by its client id is held at least 60 s (not only three "not found" syncs); a sync that
      finds it executed tells the owner once; a later execution of a dropped one is not blamed on an outside order.
  A3  breakout (manual): the close instruction names the coin amount to close and what stays open.
  A4  binance: RateLimitExceeded is looked up as unclear (the cautious side), DDoSProtection / InvalidNonce are the
      venue's refusals; the engine's no_answer() agrees.

The engine-level tests run the production replica of tests/test_review3_engine.py (the server's config of today).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from ccxt.base import errors as cx
from metaapi_cloud_sdk.clients.timeout_exception import TimeoutException

import copytrader.brokers.ccxt_broker as cb
from copytrader.brokers.base import BrokerError, OrderOutcomeUnknownError, outcome_unknown
from copytrader.brokers.metaapi_broker import MetaApiError
from copytrader.brokers.mt5_broker import MT5, Mt5Broker, OrderReplyLostError
from copytrader.engine.orchestrator import no_answer
from copytrader.models import EntryType, MarketType, OpenRequest, PositionStatus, Side, TakeProfit, TradePlan

import test_manual_broker as tmb
from test_ccxt_broker import SYM, TWO_TPS, make_broker, open_market
from test_review3_ccxt import answer_lost as ccxt_answer_lost
from test_review3_ccxt import binance_error, closes_of, hidden, is_close
from test_review3_engine import (  # noqa: F401  (_isolate: autouse)
    FX,
    MANAGE_WANTED,
    Prod,
    _isolate,
    close_all,
    due_now,
    eur_long,
    fx_closing_deals,
    half,
    make,
    stop_at,
)

IPC = (-10004, "IPC send failed")
MARKET_CLOSED = 10018              # TRADE_RETCODE_MARKET_CLOSED
INVALID_REQUEST = 10013            # TRADE_RETCODE_INVALID
SDK_TIMEOUT = "MetaApi websocket client request 1a2b3c of type trade timed out"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def fx_lost(p: Prod, picks: Any, *, execute: bool = True, answer: str = "ipc") -> dict[str, int]:
    """The fx terminal executes the first request that ``picks(request)`` selects (``execute``) and its answer is lost:
    order_send returns None with an IPC error ("ipc"), or the server answers 10012 "request cancelled by timeout"
    ("10012")."""
    term = p.terminal
    real = term.order_send
    state = {"lost": 0}

    def send(request: dict[str, Any]) -> Any:
        if state["lost"] == 0 and picks(request):
            state["lost"] += 1
            if execute:
                real(request)
            if answer == "10012":
                return term._result(MT5.TRADE_RETCODE_TIMEOUT, comment="Request timed out")
            term.last = IPC
            return None
        return real(request)
    term.order_send = send                  # type: ignore[method-assign]
    return state


def is_fx_close(request: dict[str, Any]) -> bool:
    return request.get("action") == MT5.TRADE_ACTION_DEAL and "position" in request


def is_fx_sltp(request: dict[str, Any]) -> bool:
    return request.get("action") == MT5.TRADE_ACTION_SLTP


def raised_by(broker: Any, method: str) -> list[BaseException]:
    """Every exception ``broker.<method>`` raises from now on (the engine calls the instance attribute)."""
    seen: list[BaseException] = []
    real = getattr(broker, method)

    async def wrapped(*a: Any, **k: Any) -> Any:
        try:
            return await real(*a, **k)
        except BaseException as exc:
            seen.append(exc)
            raise
    setattr(broker, method, wrapped)
    return seen


async def passes(p: Prod, trade_id: int, broker: str, n: int = 3) -> None:
    for _ in range(n):
        await due_now(p, trade_id, broker)
        await p.engine.monitor.run_once()
        await asyncio.sleep(0.05)           # (an optional broker's sync may run detached)


def no_failure_words(texts: list[str], broker: str) -> None:
    for t in texts:
        if broker in t:
            assert "kapatılamadı" not in t and "KAPATILAMADI" not in t, t
            assert "reddetti" not in t and "kapatılamayan" not in t, t


# =========================================================================== #
# A1: fx / tickmill - a lost answer is an unknown outcome, never a failure
# =========================================================================== #
@pytest.mark.parametrize("answer", ["ipc", "10012"])
async def test_a1_fx_close_executed_with_its_answer_lost_is_reported_unknown_then_closed(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, answer: str) -> None:
    """The referee's D2 (fx_ipc) and the 10012 answer after which MT5 may have executed a close: the deal executes,
    its answer is lost, the trader's 'kapattık' is reported with F2's words and its end too; one closing deal."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, tid = await eur_long(p)
        state = fx_lost(p, is_fx_close, answer=answer)
        seen = raised_by(p.brokers["fx"], "close_position")
        await p.say(close_all(tid), FX, "kapattık", reply_to=sig)
        assert state["lost"] == 1 and not p.terminal.positions          # executed at the venue
        (exc,) = seen
        assert isinstance(exc, OrderOutcomeUnknownError) and isinstance(exc.__cause__, OrderReplyLostError)
        assert outcome_unknown(exc) and str(exc).startswith("Kapatma emrinin sonucu alınamadı: #")
        texts = p.texts()
        assert any("fx: kapatma emrinin sonucu alınamadı; pozisyonun kapanıp kapanmadığı bilinmiyor" in t
                   for t in texts), texts
        await passes(p, tid, "fx", 2)
        texts = p.texts()
        assert any("fx: kapatma emrinin sonucu alınamamıştı; pozisyon artık kapalı" in t for t in texts), texts
        no_failure_words(texts, "fx")
        assert len(fx_closing_deals(p)) == 1
        assert (await p.positions(tid))["fx"].status is PositionStatus.CLOSED
    finally:
        await p.close()


async def test_a1_tickmill_close_executed_with_its_answer_lost_is_reported_unknown_then_closed(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The friend's MetaApi account: the close executes, the SDK times out. 'kapatma emrinin sonucu alınamadı', then
    'sonucu alınamamıştı; pozisyon artık kapalı'; one close request."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, tid = await eur_long(p)
        real = p.tickmill.close_position
        state = {"lost": 0}

        async def close_position(position_id: str, options: Any = None) -> Any:
            res = await real(position_id, options)
            if state["lost"] == 0:
                state["lost"] += 1
                raise TimeoutException(SDK_TIMEOUT)
            return res
        p.tickmill.close_position = close_position          # type: ignore[method-assign]
        seen = raised_by(p.brokers["tickmill"], "close_position")
        await p.say(close_all(tid), FX, "kapattık", reply_to=sig)
        await asyncio.sleep(0.1)
        assert state["lost"] == 1 and p.tickmill.positions == {}
        (exc,) = seen
        assert isinstance(exc, OrderOutcomeUnknownError) and isinstance(exc.__cause__, MetaApiError)
        assert exc.__cause__.no_answer and outcome_unknown(exc)
        assert any("tickmill: kapatma emrinin sonucu alınamadı; pozisyonun kapanıp kapanmadığı bilinmiyor" in t
                   for t in p.texts()), p.texts()
        await passes(p, tid, "tickmill", 2)
        texts = p.texts()
        assert any("tickmill: kapatma emrinin sonucu alınamamıştı; pozisyon artık kapalı" in t for t in texts), texts
        no_failure_words(texts, "tickmill")
        assert len([c for c in p.tickmill.calls if c[0] == "close_position"]) == 1
    finally:
        await p.close()


async def test_a1_fx_partial_close_lost_is_no_answer_and_the_owed_retry_never_repeats_it(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """'yarısını kapattık' on fx: the closing deal executes, its answer is lost (new type: OrderOutcomeUnknownError).
    The owner reads 'yanıt alınamadı', not 'reddetti'; the owed retry reads the live volume first and sends nothing:
    one closing deal, half the lots open, nothing owed any more."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, tid = await eur_long(p)
        lots = (await p.positions(tid))["fx"].remaining_quantity
        fx_lost(p, is_fx_close)
        seen = raised_by(p.brokers["fx"], "close_position")
        await p.say(half("EUR", tid, "USD"), FX, "yarısını kapattık", reply_to=sig)
        (exc,) = seen
        assert isinstance(exc, OrderOutcomeUnknownError) and str(exc).startswith("Kısmi kapatma emrinin sonucu")
        assert isinstance((await p.positions(tid))["fx"].data.get(MANAGE_WANTED, {}).get("reduce"), dict)
        texts = p.texts()
        assert any("fx: <code>Kısmi kapatma emrinin sonucu alınamadı" in t and "fx hesabından yanıt alınamadı" in t
                   for t in texts), texts
        # (review 5: a lost partial close on fx / tickmill is held about a minute before its retry, see
        # tests/test_review5_engine.py: the bot's clock is moved past that hold)
        from datetime import timedelta
        _clk = p.engine._clock
        p.engine._clock = p.engine.monitor._clock = lambda: _clk() + timedelta(seconds=61)
        await passes(p, tid, "fx", 3)
        closed = fx_closing_deals(p)
        assert len(closed) == 1, closed
        open_lots = sum(x.volume for x in p.terminal.positions.values())
        pos = (await p.positions(tid))["fx"]
        assert open_lots == pytest.approx(lots - closed[0]) and pos.remaining_quantity == pytest.approx(open_lots)
        assert MANAGE_WANTED not in pos.data
        no_failure_words(p.texts(), "fx")
    finally:
        await p.close()


async def test_a1_tickmill_partial_close_lost_is_no_answer_and_the_owed_retry_never_repeats_it(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = await make(tmp_path, monkeypatch)
    try:
        sig, tid = await eur_long(p)
        lots = (await p.positions(tid))["tickmill"].remaining_quantity
        real = p.tickmill.close_position_partially

        async def lossy(position_id: str, volume: float, options: Any = None) -> Any:
            await real(position_id, volume, options)
            raise TimeoutException(SDK_TIMEOUT)
        p.tickmill.close_position_partially = lossy         # type: ignore[method-assign]
        seen = raised_by(p.brokers["tickmill"], "close_position")
        await p.say(half("EUR", tid, "USD"), FX, "yarısını kapattık", reply_to=sig)
        await asyncio.sleep(0.1)
        (exc,) = seen
        assert isinstance(exc, OrderOutcomeUnknownError) and getattr(exc.__cause__, "no_answer", False)
        assert any("tickmill hesabından yanıt alınamadı" in t for t in p.texts()), p.texts()
        p.tickmill.close_position_partially = real          # type: ignore[method-assign]
        # (review 5: a lost partial close on fx / tickmill is held about a minute before its retry, see
        # tests/test_review5_engine.py: the bot's clock is moved past that hold)
        from datetime import timedelta
        _clk = p.engine._clock
        p.engine._clock = p.engine.monitor._clock = lambda: _clk() + timedelta(seconds=61)
        await passes(p, tid, "tickmill", 3)
        sent = [c for c in p.tickmill.calls if c[0] == "close_position_partially"]
        assert len(sent) == 1, sent
        pos = (await p.positions(tid))["tickmill"]
        assert pos.remaining_quantity == pytest.approx(lots - sent[0][1][1]) and MANAGE_WANTED not in pos.data
        no_failure_words(p.texts(), "tickmill")
    finally:
        await p.close()


async def test_a1_fx_stop_move_lost_is_no_answer_and_ends_with_the_new_stop(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The stop move executes at the terminal, its answer is lost: 'yanıt alınamadı' (not 'reddetti'); the owed retry
    sets the same level once more (harmless) and the venue ends with the new stop, nothing owed."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, tid = await eur_long(p)
        fx_lost(p, is_fx_sltp)
        seen = raised_by(p.brokers["fx"], "update_stop_loss")
        await p.say(stop_at(tid, 1.1615), FX, "stop 1.1615", reply_to=sig)
        (exc,) = seen
        assert isinstance(exc, OrderOutcomeUnknownError) and str(exc).startswith("Stop değişikliğinin sonucu")
        assert [x.sl for x in p.terminal.positions.values()] == [pytest.approx(1.1615)]
        assert any("fx hesabından yanıt alınamadı" in t for t in p.texts()), p.texts()
        await passes(p, tid, "fx", 2)
        pos = (await p.positions(tid))["fx"]
        assert pos.stop_loss == pytest.approx(1.1615) and MANAGE_WANTED not in pos.data
        assert [x.sl for x in p.terminal.positions.values()] == [pytest.approx(1.1615)]
        no_failure_words(p.texts(), "fx")
    finally:
        await p.close()


async def test_a1_definite_refusals_keep_their_words(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Control: the terminal refuses the close (10018 market closed) and the partial close: 'pozisyon kapatılamadı'
    and 'fx değişikliği reddetti' as before; a plain BrokerError, not an unknown outcome."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, tid = await eur_long(p)
        hook = (lambda r: MARKET_CLOSED if is_fx_close(r) else None)
        p.terminal.hooks.append(hook)
        seen = raised_by(p.brokers["fx"], "close_position")
        await p.say(half("EUR", tid, "USD"), FX, "yarısını kapattık", reply_to=sig)
        assert any("fx değişikliği reddetti" in t for t in p.texts()), p.texts()
        p.notifier.notifications.clear()
        await p.say(close_all(tid), FX, "kapattık", reply_to=sig)
        assert any("fx: pozisyon kapatılamadı" in t for t in p.texts()), p.texts()
        assert seen and all(type(e) is BrokerError and not outcome_unknown(e) for e in seen), seen
        assert str(seen[0]).startswith("Pozisyon kapatılamadı: #")
        assert fx_closing_deals(p) == []
    finally:
        await p.close()


async def test_a1_take_profit_move_and_cancel_folds_are_unknown_only_when_an_answer_was_lost(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The other two fold sites, on the adapter: a TP move whose SLTP answer is lost, a pending entry's cancel whose
    REMOVE answer is lost while the order still shows (it lands later) -> OrderOutcomeUnknownError chained to the
    lost leg error; the same calls refused outright -> a plain BrokerError with the old words."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, tid = await eur_long(p)
        fx: Mt5Broker = p.brokers["fx"]
        pos = (await p.positions(tid))["fx"]
        fx_lost(p, is_fx_sltp, execute=False)
        with pytest.raises(OrderOutcomeUnknownError) as lost_tp:
            await fx.update_take_profits(pos, [TakeProfit(price=1.1700)])
        assert str(lost_tp.value).startswith("TP değişikliğinin sonucu alınamadı: #")
        assert isinstance(lost_tp.value.__cause__, OrderReplyLostError)
        p.terminal.hooks.append(lambda r: MT5.TRADE_RETCODE_INVALID_STOPS if is_fx_sltp(r) else None)
        with pytest.raises(BrokerError) as refused_tp:
            await fx.update_take_profits(pos, [TakeProfit(price=1.1700)])
        assert type(refused_tp.value) is BrokerError and str(refused_tp.value).startswith("TP güncellenemedi: #")
    finally:
        await p.close()
    (tmp_path / "limit").mkdir()
    p = await make(tmp_path / "limit", monkeypatch)
    try:
        from test_preflight_scenarios import EURUSD_LONG, fx_pair, set_eurusd
        set_eurusd(p, 1.1649, 1.1651)
        await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.LIMIT), FX, "EUR LONG LIMIT 1.1620")
        trade = (await p.trades())[-1]
        pos = (await p.positions(trade.id))["fx"]
        assert pos.status is PositionStatus.PENDING
        fx = p.brokers["fx"]
        fx_lost(p, lambda r: r.get("action") == MT5.TRADE_ACTION_REMOVE, execute=False)
        with pytest.raises(OrderOutcomeUnknownError) as lost_cancel:
            await fx.cancel_entry(pos)
        assert str(lost_cancel.value).startswith("Bekleyen emrin iptal isteğinin sonucu alınamadı: #")
        assert outcome_unknown(lost_cancel.value) and p.terminal.orders            # (still resting)
        p.terminal.hooks.append(lambda r: INVALID_REQUEST if r.get("action") == MT5.TRADE_ACTION_REMOVE else None)
        with pytest.raises(BrokerError) as refused_cancel:
            await fx.cancel_entry(pos)
        assert type(refused_cancel.value) is BrokerError
        assert str(refused_cancel.value).startswith("Bekleyen emir iptal edilemedi: #")
    finally:
        await p.close()


@pytest.mark.parametrize("refresh_fails", [False, True])
async def test_a1_netting_emulated_take_profit_whose_answer_was_lost_is_never_sent_again(refresh_fails: bool) -> None:
    """A netting account emulates the first TP with a partial close in sync_position. Its answer is lost (executed):
    it is marked done and never sent again (it was sent on every sync while the price stayed beyond the TP). Also when
    the read after it fails: the sync still returns (and so stores) the done mark."""
    from types import SimpleNamespace

    from test_mt5_broker import SERVER_TIME, FakeMT5, connected, make_plan, make_req
    fake = FakeMT5(margin_mode=0)
    b = await connected(fake)
    sym = await b.resolve_symbol("ETH")
    pos = await b.open_position(make_req(sym, 0.10, make_plan(tps=((2260.0, 0.5), (2310.6, 0.5)))))
    fake.ticks["ETHUSD"] = SimpleNamespace(bid=2261.0, ask=2261.2, last=0.0, time=SERVER_TIME)
    real_send, real_get = fake.order_send, fake.positions_get
    state = {"lost": 0, "reads": 0}

    def send(request: dict[str, Any]) -> Any:
        if is_fx_close(request) and state["lost"] == 0:
            state["lost"] += 1
            real_send(request)              # executed ...
            fake.last = IPC                 # ... its answer lost
            state["reads"] = 1 if refresh_fails else 0
            return None
        return real_send(request)

    def positions_get(*a: Any, **k: Any) -> Any:
        if state["reads"]:
            state["reads"] -= 1
            fake.last = IPC
            return None                     # the read after the lost answer fails too
        return real_get(*a, **k)
    fake.order_send, fake.positions_get = send, positions_get          # type: ignore[method-assign]
    pos = await b.sync_position(pos)
    assert pos.data["emulated_tps"][0]["done"] is True
    assert "TP1 (2260) kısmi kapatma emrine yanıt alınamadı" in (pos.error or "")
    for _ in range(2):
        pos = await b.sync_position(pos)
    closes = [r for r in fake.sent if is_fx_close(r)]
    assert len(closes) == 1 and sum(x.volume for x in fake.positions.values()) == pytest.approx(0.05)
    assert pos.remaining_quantity == pytest.approx(0.05)
    await b.close()


# =========================================================================== #
# A2: binance - a close kept by client id is held 60 s; a late find is told; a late execution is not "outside"
# =========================================================================== #
async def test_a2_lost_close_is_kept_at_least_60_s_even_after_three_not_found_syncs() -> None:
    broker, fake = make_broker()
    now = {"ms": 1_800_000_000_000}
    broker._now_ms = lambda: now["ms"]
    pos = await open_market(broker, tps=TWO_TPS)
    ccxt_answer_lost(fake, is_close, execute=False)
    with pytest.raises(OrderOutcomeUnknownError):
        await broker.close_position(pos, 0.5)
    for _ in range(4):                      # four answered "not found" syncs within a few seconds: still held
        now["ms"] += 5_000
        pos = await broker.sync_position(pos)
        assert len(pos.data["unconfirmed_closes"]) == 1
    now["ms"] += 45_000                     # 65 s after it was sent: given up, nothing booked
    pos = await broker.sync_position(pos)
    assert not pos.data["unconfirmed_closes"] and pos.data["exits"] == []
    assert pos.data["dropped_closes"] == ["ct12ab34cdc1"] and pos.remaining_quantity == pytest.approx(6.828)


async def test_a2_lost_close_found_by_a_sync_leaves_one_note_for_the_owner() -> None:
    broker, fake = make_broker()
    pos = await open_market(broker, tps=TWO_TPS)
    ccxt_answer_lost(fake, is_close)
    hidden(fake, "ct12ab34cdc1", looks=cb._LOOKUP_ATTEMPTS)
    with pytest.raises(OrderOutcomeUnknownError):
        await broker.close_position(pos, 0.5)
    pos = await broker.sync_position(pos)               # found executed now: booked
    note = (f"{broker.name}: yanıtı alınamayan önceki kapatma emri (ct12ab34cdc1) borsada gerçekleşmiş bulundu: "
            "3.414 kapandı, kalan 3.414.")
    assert pos.remaining_quantity == pytest.approx(3.414) and pos.error is not None and note in pos.error
    again = await broker.sync_position(pos)             # not written again: the monitor sees no new error
    assert again.error == pos.error and again.error.count("yanıtı alınamayan önceki kapatma emri") == 1
    assert len(closes_of(fake)) == 1


async def test_a2_monitor_tells_the_owner_once_when_a_sync_finds_the_lost_close(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """End to end on the live replica: 'yarısını kapattık', Binance executes the close, its answer is lost and the
    adapter's look-ups do not see it yet; the monitor's next sync books it and the owner is told so once."""
    from test_review3_ccxt import half as btc_half
    from test_preflight_scenarios import KRIPTO, btc
    p = await Prod.create(tmp_path, monkeypatch, replica="live")
    try:
        sig = await p.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO, "btc 1R long piyasadan")
        (trade,) = await p.trades()
        lost = ccxt_answer_lost(p.binance, is_close)
        cid = p.brokers["binance"]._client_id(
            (await p.positions(trade.id))["binance"].data.get("client_tag", "ct"), "c1")
        hidden(p.binance, cid, looks=cb._LOOKUP_ATTEMPTS)
        await p.say(btc_half(trade.id, sig), KRIPTO, "yarısını kapattık", reply_to=sig)
        assert lost["cids"] == [cid] and (await p.positions(trade.id))["binance"].remaining_quantity == 0.012
        p.notifier.notifications.clear()
        for _ in range(3):
            await p.engine.monitor.run_once()
        told = [t for t in p.texts() if f"yanıtı alınamayan önceki kapatma emri ({cid}) borsada gerçekleşmiş" in t]
        assert len(told) == 1 and "0.006 kapandı, kalan 0.006" in told[0], p.texts()
        assert p.binance.net["BTC/USDT:USDT"] == pytest.approx(0.006)
        assert (await p.positions(trade.id))["binance"].remaining_quantity == pytest.approx(0.006)
    finally:
        await p.close()


async def test_a2_late_execution_of_a_dropped_close_is_not_blamed_on_an_outside_order() -> None:
    broker, fake = make_broker()
    now = {"ms": 1_800_000_000_000}
    broker._now_ms = lambda: now["ms"]
    pos = await open_market(broker, tps=TWO_TPS)
    ccxt_answer_lost(fake, is_close, execute=False)
    with pytest.raises(OrderOutcomeUnknownError):
        await broker.close_position(pos, 1.0)
    for _ in range(4):
        now["ms"] += 20_000
        pos = await broker.sync_position(pos)
    assert not pos.data["unconfirmed_closes"] and pos.data["dropped_closes"] == ["ct12ab34cdc1"]
    fake.net[SYM] = 0.0                     # ... and then it executes after all
    for _ in range(2):
        pos = await broker.sync_position(pos)
    assert pos.status is PositionStatus.CLOSED
    assert "yanıtı alınamayan ve borsada bulunamayan önceki kapatma emrinin (ct12ab34cdc1) sonradan" in pos.error
    assert "botun emirleri dışında kapandı" not in pos.error


# =========================================================================== #
# A3: breakout - the close instruction says how much to close and what stays open
# =========================================================================== #
async def manual_open(tmp_path: Path, base: str = "ETH", qty: float = 2.0) -> tuple[Any, Any, Any]:
    broker, notifier = tmb.make(tmp_path)
    await broker.connect()
    if base == "ETH":
        info = await broker.resolve_symbol("ETH", "USDT", MarketType.PERP, "ETHUSDT.P")
        plan = tmb.eth_short()
        sl = 2250.60
    else:
        info = await broker.resolve_symbol(base, "USDT", MarketType.PERP, f"{base}USDT.P")
        assert info is not None
        price = (await broker.get_quote(info)).bid
        plan = TradePlan(plan_id="m2", base_asset=base, side=Side.LONG, entry_type=EntryType.MARKET,
                         entry_price=price, stop_loss=price * 0.9, take_profits=[TakeProfit(price=price * 1.2)],
                         risk_r=1.0, risk_pct=2.0, source_chat_id=1, created_at=tmb.NOW)
        sl = price * 0.9
    pos = await broker.open_position(OpenRequest(plan=plan, symbol=info, quantity=qty, entry_type=EntryType.MARKET,
                                                 stop_loss=sl, take_profits=plan.take_profits, client_tag="c"))
    notifier.notifications.clear()
    return broker, notifier, pos


async def test_a3_close_instructions_name_the_amount_and_what_stays_open(tmp_path: Path) -> None:
    """'yarısını kapat', then 'kalanın yarısı' (a quarter of the original: on 62e0719 '%25'ini KAPAT', read as a
    share of what is open), then the rest."""
    broker, notifier, pos = await manual_open(tmp_path)
    pos = await broker.close_position(pos, 0.5)
    pos = await broker.close_position(pos, 0.25)
    await broker.close_position(pos, 1.0)
    texts = [t for t, _ in notifier.notifications]
    # Review 5 (Q2): each amount also carries its USD value at the current price (2232.85, the fake's ETH quote) and
    # the percent is gone (it could differ from the bot's own report line); was "(açık pozisyonun %50'si; ...)".
    assert texts == [
        "<b>❌ ETH: 1 ETH KAPAT</b> (≈ <b>2232.85 USD</b> tutarında, şu anki fiyatla; tutar USD olarak giriliyorsa "
        "2232.85 USD girin; kapattıktan sonra 1 ETH ≈ 2232.85 USD açık kalmalı)\n"
        "✋ Elle işlem (breakout)",
        "<b>❌ ETH: 0.5 ETH KAPAT</b> (≈ <b>1116.42 USD</b> tutarında, şu anki fiyatla; tutar USD olarak giriliyorsa "
        "1116.42 USD girin; kapattıktan sonra 0.5 ETH ≈ 1116.42 USD açık kalmalı)\n"
        "✋ Elle işlem (breakout)",
        "<b>❌ ETH: POZİSYONUN TAMAMINI KAPAT (0.5 ETH)</b> (≈ 1116.42 USD, şu anki fiyatla)\n✋ Elle işlem (breakout)",
    ]


async def test_a3_bundled_contract_gives_the_single_coin_count_too(tmp_path: Path) -> None:
    broker, notifier, pos = await manual_open(tmp_path, "1000PEPE", 120.0)
    await broker.close_position(pos, 0.5)
    (text,) = [t for t, _ in notifier.notifications]
    # Review 5 (Q2): the USD values were added and the percent dropped (was "(açık pozisyonun %50'si; ...").
    assert text.startswith("<b>❌ 1000PEPE: 60 1000PEPE = 60000 PEPE KAPAT</b> (≈ <b>")
    assert "kapattıktan sonra 60 1000PEPE = 60000 PEPE ≈ " in text


async def test_a3_cancel_of_an_entry_that_had_filled_tells_to_close_it(tmp_path: Path) -> None:
    """cancel_entry on a position the record shows filled closes it there: the owner must close, not cancel."""
    broker, notifier, pos = await manual_open(tmp_path)
    await broker.cancel_entry(pos)
    (text,) = [t for t, _ in notifier.notifications]
    assert text.startswith("<b>❌ ETH: POZİSYONUN TAMAMINI KAPAT (2 ETH)</b>")


# =========================================================================== #
# A4: RateLimitExceeded is unclear (looked up), DDoSProtection / InvalidNonce are refusals - adapter and engine agree
# =========================================================================== #
@pytest.mark.parametrize("code, status, kind, refusal", [
    (-1003, 429, cx.DDoSProtection, True),          # binance rate limit as it comes: HTTP 429
    (-1015, 429, cx.DDoSProtection, True),
    (-1003, 418, cx.DDoSProtection, True),          # IP ban
    (-1021, 400, cx.InvalidNonce, True),            # outside the time window
    (-1003, 400, cx.RateLimitExceeded, False),      # the same code without 429: ccxt's RateLimitExceeded
    (-1015, 400, cx.RateLimitExceeded, False),
])
def test_a4_rate_limit_classes_adapter_and_engine_agree(code: int, status: int, kind: type, refusal: bool) -> None:
    exc = binance_error(code, "Too many requests.", status)
    assert type(exc) is kind
    assert cb._unclear(exc) is (not refusal)        # unclear: looked up by client id, never sent twice
    wrapped = BrokerError("binance: kapatma emri ...")
    wrapped.__cause__ = exc
    assert no_answer(wrapped) is (not refusal)
    assert not issubclass(cx.RateLimitExceeded, cx.DDoSProtection)
