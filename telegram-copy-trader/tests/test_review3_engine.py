"""Review 3 (lane repair3_engine): the referee's round-2 problems in the engine, on the production replica of
tests/test_preflight_scenarios.py (REAL Engine, Monitor, validator and the CcxtBroker, ManualBroker, Mt5Broker and
MetaApiBroker adapters on the fake venues) loaded with the server's config of today (LIVE_YAML: kripto -> binance,
breakout, tickmill; fx -> fx, tickmill; notify_on_execution; tickmill optional).

  E1  a partial close that Binance executed but whose answer was lost is never sent a second time by the owed retry
      (the venue's size decides; the owner is told both sizes); fx, tickmill and breakout never repeat one either.
  E2  the late-leg catch-up never puts an older stop / target on tickmill over a newer instruction.
  E3  an open that was sent and whose answer was lost stays "unknown" when another broker answers late with a refusal.
  E4  a stop move / close while only tickmill's late open is pending is reported.
  E5  a change owed to a broker is worded after why it was not applied (not connected / refused / no answer).
  E6  shutdown around a late leg: the restart notice is right and names the change that was promised.
  E7  close-on-arrival of a late tickmill position is not run under the trade lock.

Every test checks the venue first, then what the owner was told. The file also imports on the older trees (62e0719,
d45f96a), to show that the tests fail there.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from ccxt.base import errors as cx
from metaapi_cloud_sdk.clients.timeout_exception import TimeoutException

import test_preflight_scenarios as tps
from test_preflight_scenarios import (  # noqa: F401  (_isolate: autouse)
    EURUSD_LONG,
    FX,
    KRIPTO,
    TICKMILL_LISTED,
    FxTerminal,
    Prod,
    Reference,
    Tickmill,
    _isolate,
    btc,
    fx_pair,
    set_eurusd,
)

from copytrader.brokers import base as broker_base
from copytrader.brokers.base import BrokerError
from copytrader.brokers.ccxt_broker import CcxtBroker
from copytrader.brokers.manual_broker import ManualBroker
from copytrader.brokers.metaapi_broker import MetaApiBroker
from copytrader.brokers.mt5_broker import MT5, Mt5Broker
from copytrader.engine.orchestrator import Engine
from copytrader.engine.prices import PriceOracle
from copytrader.models import ActionType, EntryType, ManagementPlan, ParsedAction, PositionStatus, Severity, Side
from copytrader.storage.db import Database

# (spelled out: the file also imports on 62e0719, which has neither)
MANAGE_WANTED = "manage_wanted"
OrderOutcomeUnknownError = getattr(broker_base, "OrderOutcomeUnknownError", BrokerError)
APPLIED = "daha önce uygulanamayan değişiklik şimdi uygulandı"
LOST_1007 = ("binanceusdm -1007 Timeout waiting for response from backend server. Send status unknown; execution "
             "status unknown.")


# --------------------------------------------------------------------------- #
# the replica (live config of today)
# --------------------------------------------------------------------------- #
async def make(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, db_path: str = ":memory:",
               terminal: FxTerminal | None = None, tickmill: Tickmill | None = None, binance: Any = None,
               clear: bool = True) -> Prod:
    """``db_path``, ``terminal``, ``tickmill``, ``binance``: those of an earlier run (a restart). ``clear``: forget
    what the owner was told at the start."""
    p = Prod()
    p.replica = "live"
    monkeypatch.setattr(tps, "PROD_YAML", tps.LIVE_YAML)        # the server's config of today
    p.cfg = tps.load_prod_config(tmp_path, monkeypatch)
    by = {b.name: b for b in p.cfg.brokers}
    p.terminal = terminal or FxTerminal()
    p.tickmill = tickmill or Tickmill(TICKMILL_LISTED)
    if binance is not None:
        p.binance = binance

    async def no_sleep(_s: float) -> None:
        return None

    fx = Mt5Broker(by["fx"], mt5_module=p.terminal)
    fx.retry_delay_sec = 0
    tm = MetaApiBroker(by["tickmill"], connection=p.tickmill)
    tm.retry_delay_sec = 0
    p.brokers = {"binance": CcxtBroker(by["binance"], exchange=p.binance, sleep=no_sleep),
                 "breakout": ManualBroker(by["breakout"], p.cfg.data_dir, price_source=p.binance),
                 "fx": fx, "tickmill": tm}
    p.db = Database(db_path)
    await p.db.open()
    p.ref = Reference(p.binance)
    holder: list[Engine] = []
    oracle = PriceOracle(lambda: holder[0].brokers, p.cfg.price_source_exchange, cache_ttl_sec=0.0,
                         public_exchange=p.binance)
    p.engine = Engine(p.cfg, p.db, p.parser, list(p.brokers.values()), p.notifier, price_oracle=oracle,
                      reference_prices=p.ref)
    holder.append(p.engine)
    await p.engine.start(monitor=False)
    if clear:
        p.notifier.notifications.clear()
    return p


async def until(cond: Callable[[], bool], timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.02)


def half(base: str, trade_id: int, quote: str, side: Side = Side.LONG) -> ParsedAction:
    return ParsedAction(action=ActionType.CLOSE, close_fraction=0.5, base_asset=base, quote_asset=quote, side=side,
                        target_trade_id=trade_id, confidence=0.95, summary_tr=f"{base} yarısını kapat")


def stop_at(trade_id: int, level: float) -> ParsedAction:
    return ParsedAction(action=ActionType.MOVE_SL, new_stop_loss=level, base_asset="EUR", quote_asset="USD",
                        side=Side.LONG, target_trade_id=trade_id, confidence=0.95, summary_tr=f"EUR stop {level}")


def close_all(trade_id: int, base: str = "EUR", quote: str = "USD") -> ParsedAction:
    return ParsedAction(action=ActionType.CLOSE, close_fraction=1.0, base_asset=base, quote_asset=quote,
                        side=Side.LONG, target_trade_id=trade_id, confidence=0.95, summary_tr=f"{base} kapat")


# --------------------------------------------------------------------------- #
# binance (ccxt): what reaches the venue
# --------------------------------------------------------------------------- #
def venue_btc(p: Prod) -> float:
    return round(p.binance.net.get("BTC/USDT:USDT", 0.0), 8)


def btc_closes(p: Prod) -> list[float]:
    """The reduce-only MARKET orders Binance executed for BTC (the bot's closes; stop / TP orders are triggers)."""
    return [o["amount"] for o in p.binance.created()
            if o["symbol"] == "BTC/USDT:USDT" and o["type"] == "market" and o.get("reduceOnly")
            and not o.get("triggerPrice")]


async def btc_long(p: Prod) -> tuple[int, int]:
    """BTC market long 1R on the kripto route: binance 0.012 (the live config's 10 USD per R); (message id, trade)."""
    sig = await p.say(btc(EntryType.MARKET, 74000.0, 73300.0, 75500.0), KRIPTO, "btc 1R long piyasadan")
    trades = await p.trades()
    trade = trades[-1]
    pos = await p.positions(trade.id)
    assert pos["binance"].status is PositionStatus.OPEN and pos["binance"].remaining_quantity == pytest.approx(0.012)
    return sig, trade.id


async def due_now(p: Prod, trade_id: int, broker: str) -> None:
    """Make what is still owed to the position due at once (the back-off after a failed try is 30 s and more)."""
    pos = (await p.positions(trade_id))[broker]
    wish = pos.data.get(MANAGE_WANTED)
    if isinstance(wish, dict):
        wish["next_at"] = wish.get("since") or wish.get("next_at")
        await p.db.save_broker_position(trade_id, pos)


def binance_close_answer_lost(p: Prod, error: Callable[[], BaseException], *, executes: bool = True,
                              times: int = 1) -> dict[str, int]:
    """The binance adapter's close: Binance executes it (``executes``) and its answer is lost - the adapter raises
    ``error()``, so the bot keeps the size it had. Only the first ``times`` closes; later ones go through as usual."""
    bn = p.brokers["binance"]
    real = bn.close_position
    state = {"lost": 0}

    async def close_position(pos: Any, fraction: float = 1.0) -> Any:
        if state["lost"] >= times:
            return await real(pos, fraction)
        state["lost"] += 1
        if executes:
            await real(pos, fraction)       # executed at Binance; the adapter's record of it is lost with the answer
        raise error()
    bn.close_position = close_position      # type: ignore[method-assign]
    return state


# =========================================================================== #
# E1: an owed partial close is never sent again unless the venue shows the earlier try did not execute
# =========================================================================== #
@pytest.mark.parametrize("error", [
    pytest.param(lambda: BrokerError(f"binance: kapatma emri reddedildi: {LOST_1007}"), id="todays_adapter"),
    pytest.param(lambda: OrderOutcomeUnknownError(f"binance: kapatma emri durumu belirsiz: {LOST_1007}"),
                 id="typed_unknown"),
])
async def test_e1_binance_partial_close_executed_with_its_answer_lost_is_never_sent_again(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: Callable[[], BaseException]) -> None:
    """The referee's C1: 'yarısını kapattık'; Binance executes binance's reduce-only close (0.006) but the answer is
    lost (binance -1007), as the adapter of today reports it (a plain BrokerError) and as one that raises the typed
    OrderOutcomeUnknownError. d45f96a sent it again from the monitor and closed the whole position, telling the owner
    '0.006 kapatıldı (kalan 0.006)'; 62e0719 sent it once but told the owner only 'reddedildi'. Now the venue's size
    decides: it holds 0.006 against 0.012 in the record, so the close is not sent again and the owner is told both
    sizes."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, trade_id = await btc_long(p)
        binance_close_answer_lost(p, error)
        p.notifier.notifications.clear()
        await p.say(half("BTC", trade_id, "USDT"), KRIPTO, "yarısını kapattık", reply_to=sig)
        assert venue_btc(p) == pytest.approx(0.006)
        for _ in range(4):
            await due_now(p, trade_id, "binance")       # (no back-off between the passes: a repeat would show)
            await p.engine.monitor.run_once()
        # the venue: one close only; the trader's half stays open
        assert btc_closes(p) == [pytest.approx(0.006)]
        assert venue_btc(p) == pytest.approx(0.006)
        pos = (await p.positions(trade_id))["binance"]
        assert pos.status is PositionStatus.OPEN and MANAGE_WANTED not in pos.data
        # the owner: the truth, with both sizes; never "kapatıldı (kalan 0.006)" for the bot's own (double) close
        texts = p.texts()
        assert any("binance: bekleyen kısmi kapatma" in t and "önceki kapatma emri borsada gerçekleşmiş görünüyor "
                   "(borsada 0.006 açık, kayıtta 0.012); tekrar gönderilmedi, binance hesabını kontrol edin." in t
                   for t in texts), texts
        assert not [t for t in texts if APPLIED in t and "binance" in t], texts
    finally:
        await p.close()


async def test_e1_binance_partial_close_that_never_reached_binance_is_retried_and_closes_exactly_half(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Control: the close never reached Binance (a timeout before it was sent). The venue still holds the whole
    record, so the owed retry is right: it closes exactly the asked half, once, and says so."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, trade_id = await btc_long(p)
        binance_close_answer_lost(p, lambda: BrokerError("binance: kapatma emri reddedildi: binanceusdm "
                                                         "RequestTimeout"), executes=False)
        p.notifier.notifications.clear()
        await p.say(half("BTC", trade_id, "USDT"), KRIPTO, "yarısını kapattık", reply_to=sig)
        assert venue_btc(p) == pytest.approx(0.012) and btc_closes(p) == []
        await p.engine.monitor.run_once()
        await p.engine.monitor.run_once()
        assert btc_closes(p) == [pytest.approx(0.006)]
        assert venue_btc(p) == pytest.approx(0.006)
        pos = (await p.positions(trade_id))["binance"]
        assert pos.remaining_quantity == pytest.approx(0.006) and MANAGE_WANTED not in pos.data
        assert [t for t in p.texts() if APPLIED in t] == [
            f"✅ #{trade_id} BTC LONG binance: {APPLIED} — 0.006 kapatıldı (kalan 0.006)."]
    finally:
        await p.close()


async def test_e1_binance_venue_read_by_the_judge_rig_lost_answer_is_not_sent_twice(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The judges' own setup, one level lower: the fake Binance itself executes the reduce-only close and then raises
    ccxt RequestTimeout (-1007), so the binance adapter's own error handling runs. One close only, whatever the adapter
    does with the lost answer (today: a BrokerError, the close is owed and the venue read stops it)."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, trade_id = await btc_long(p)
        real = p.binance.create_order
        state = {"lost": 0}

        async def lossy(symbol: str, type: str, side: str, amount: float, price: float | None = None,
                        params: dict | None = None) -> Any:
            out = await real(symbol, type, side, amount, price, params)          # executed on the venue
            if type == "market" and (params or {}).get("reduceOnly") and state["lost"] == 0:
                state["lost"] += 1
                raise cx.RequestTimeout(LOST_1007)
            return out
        p.binance.create_order = lossy          # type: ignore[method-assign]
        p.notifier.notifications.clear()
        await p.say(half("BTC", trade_id, "USDT"), KRIPTO, "yarısını kapattık", reply_to=sig)
        owed = MANAGE_WANTED in (await p.positions(trade_id))["binance"].data
        for _ in range(4):
            await due_now(p, trade_id, "binance")
            await p.engine.monitor.run_once()
        assert btc_closes(p) == [pytest.approx(0.006)]
        assert venue_btc(p) == pytest.approx(0.006)
        texts = p.texts()
        assert not [t for t in texts if "0.006 kapatıldı (kalan 0.006)" in t], texts
        if owed:            # (the adapter of today: owed, then the venue read decides)
            assert any("(borsada 0.006 açık, kayıtta 0.012); tekrar gönderilmedi" in t for t in texts), texts
    finally:
        await p.close()


async def test_e1_shared_binance_position_holds_the_owed_partial_close_and_says_nothing_was_sent(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The verifier's shared case: two BTC longs of the bot on the one-way binance account (one venue position). The
    first trade's half close executes, its answer is lost. The venue (0.006 + the second trade's size) no longer
    covers both records, so the earlier close may have gone through: nothing is sent (d45f96a closed 0.006 of the
    OTHER trade's size), the owed close stays held and the owner is told once that no order was sent."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, first = await btc_long(p)
        await p.say(btc(EntryType.MARKET, 74000.0, 73400.0, 76000.0), KRIPTO, "btc 1R long (2)")
        for conf in list(p.notifier.confirmations):         # the duplicate-signal question: the owner approves
            await p.engine.handle_confirmation(conf.id, True)
        trades = await p.trades()
        assert len(trades) == 2
        second = trades[-1].id
        other = (await p.positions(second))["binance"].remaining_quantity
        assert other > 0 and venue_btc(p) == pytest.approx(0.012 + other)
        binance_close_answer_lost(p, lambda: BrokerError(f"binance: kapatma emri reddedildi: {LOST_1007}"))
        p.notifier.notifications.clear()
        await p.say(half("BTC", first, "USDT"), KRIPTO, "yarısını kapattık", reply_to=sig)
        for _ in range(3):
            await due_now(p, first, "binance")          # (held on every pass, told once)
            await p.engine.monitor.run_once()
        assert btc_closes(p) == [pytest.approx(0.006)]
        assert venue_btc(p) == pytest.approx(0.006 + other)
        pos = (await p.positions(first))["binance"]
        assert isinstance(pos.data.get(MANAGE_WANTED, {}).get("reduce"), dict)     # held, not dropped, not sent
        held = [t for t in p.texts() if "yine uygulanamadı" in t]
        assert len(held) == 1, p.texts()
        assert "kısmi kapatma emri gönderilmedi: aynı hesapta aynı yönde BTC/USDT:USDT taşıyan başka bir işlem var" \
               in held[0]
        assert held[0].endswith("Bot kısmi kapatmayı ancak borsadaki miktar önceki emrin gerçekleşmediğini "
                                "gösterirse gönderecek; binance hesabını kontrol edin.")
    finally:
        await p.close()


async def test_e1_unreadable_binance_venue_holds_the_owed_close_until_it_can_be_read(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The close never reached Binance, but at the first retry the venue size cannot be read: nothing is sent then
    (d45f96a sent it without looking). Once the venue answers again and shows the whole record, the half is closed."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, trade_id = await btc_long(p)
        binance_close_answer_lost(p, lambda: BrokerError("binance: kapatma emri reddedildi: binanceusdm "
                                                         "RequestTimeout"), executes=False)
        await p.say(half("BTC", trade_id, "USDT"), KRIPTO, "yarısını kapattık", reply_to=sig)
        bn = p.brokers["binance"]
        read = bn.venue_position_size
        down = {"on": True}

        async def venue_position_size(symbol: str, side: Side) -> float | None:
            return None if down["on"] else await read(symbol, side)
        bn.venue_position_size = venue_position_size        # type: ignore[method-assign]
        p.notifier.notifications.clear()
        await p.engine.monitor.run_once()
        assert btc_closes(p) == [] and venue_btc(p) == pytest.approx(0.012)
        held = [t for t in p.texts() if "yine uygulanamadı" in t]
        assert len(held) == 1 and "borsadaki pozisyon miktarı okunamadı" in held[0], p.texts()
        down["on"] = False
        await due_now(p, trade_id, "binance")           # (due now, not in 30 s)
        await p.engine.monitor.run_once()
        assert btc_closes(p) == [pytest.approx(0.006)] and venue_btc(p) == pytest.approx(0.006)
        assert [t for t in p.texts() if APPLIED in t] == [
            f"✅ #{trade_id} BTC LONG binance: {APPLIED} — 0.006 kapatıldı (kalan 0.006)."]
    finally:
        await p.close()


async def test_e1_approved_second_partial_close_while_the_lost_one_is_owed_is_sized_from_the_venue(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """While the lost half close is still owed (before the monitor's next pass), another partial close is approved
    (an approval runs the plan at once, without a sync first). The owed part went through at Binance: it is not sent
    again, and the new half is of what Binance holds (0.003), never of the stale record (d45f96a sold 0.009 on a
    0.006 position; 62e0719 sold 0.006 and closed it all)."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, trade_id = await btc_long(p)
        binance_close_answer_lost(p, lambda: BrokerError(f"binance: kapatma emri reddedildi: {LOST_1007}"))
        await p.say(half("BTC", trade_id, "USDT"), KRIPTO, "yarısını kapattık", reply_to=sig)
        assert venue_btc(p) == pytest.approx(0.006)
        p.notifier.notifications.clear()
        plan = ManagementPlan(plan_id="e1second", action=ActionType.CLOSE, trade_id=trade_id, close_fraction=0.5,
                              source_chat_id=tps.CHAT, summary_tr=f"#{trade_id} BTC LONG: kalanın yarısı kapatılacak",
                              created_at=tps.datetime.now(tps.UTC))
        await p.engine.execute_management(plan)
        assert btc_closes(p) == [pytest.approx(0.006), pytest.approx(0.003)]
        assert venue_btc(p) == pytest.approx(0.003)
        pos = (await p.positions(trade_id))["binance"]
        assert MANAGE_WANTED not in pos.data
        assert any("binance: %50 kapatıldı (önceki kapatma emri borsada gerçekleşmiş görünüyordu: borsada 0.006 açık, "
                   "kayıtta 0.012; o kısım tekrar gönderilmedi" in t for t in p.texts()), p.texts()
        await p.engine.monitor.run_once()
        assert btc_closes(p) == [pytest.approx(0.006), pytest.approx(0.003)]
    finally:
        await p.close()


async def test_e1_approved_partial_close_while_a_lost_one_is_owed_and_the_venue_is_unreadable_sends_nothing(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The same, but Binance's position size cannot be read at that moment: nothing is sent (the owed part may have
    gone through), the change stays owed, and the owner is told that the bot held it - not that Binance refused it."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, trade_id = await btc_long(p)
        binance_close_answer_lost(p, lambda: BrokerError(f"binance: kapatma emri reddedildi: {LOST_1007}"))
        await p.say(half("BTC", trade_id, "USDT"), KRIPTO, "yarısını kapattık", reply_to=sig)
        bn = p.brokers["binance"]

        async def unreadable(symbol: str, side: Side) -> float | None:
            return None
        bn.venue_position_size = unreadable             # type: ignore[method-assign]
        p.notifier.notifications.clear()
        plan = ManagementPlan(plan_id="e1held", action=ActionType.CLOSE, trade_id=trade_id, close_fraction=0.5,
                              source_chat_id=tps.CHAT, summary_tr=f"#{trade_id} BTC LONG: kalanın yarısı kapatılacak",
                              created_at=tps.datetime.now(tps.UTC))
        await p.engine.execute_management(plan)
        assert btc_closes(p) == [pytest.approx(0.006)] and venue_btc(p) == pytest.approx(0.006)
        assert isinstance((await p.positions(trade_id))["binance"].data.get(MANAGE_WANTED, {}).get("reduce"), dict)
        (line,) = [t for t in p.texts() if t.startswith(f"#{trade_id} binance:")]
        assert line == (f"#{trade_id} binance: <code>kısmi kapatma emri gönderilmedi: borsadaki pozisyon miktarı "
                        "okunamadı; önceki kapatma emrinin gerçekleşip gerçekleşmediği bilinmiyor</code>; değişiklik "
                        "şimdi uygulanamadı. Bot sonraki kontrollerde yeniden deneyecek ve uygulayınca haber verecek")
    finally:
        await p.close()


async def eur_long(p: Prod) -> tuple[int, int]:
    """EURUSD long 1R at the market on fx and tickmill; (signal message id, trade id)."""
    set_eurusd(p, 1.1619, 1.1621)
    sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
    trade = (await p.trades())[-1]
    pos = await p.positions(trade.id)
    assert {b: x.status for b, x in pos.items()} == {"fx": PositionStatus.OPEN, "tickmill": PositionStatus.OPEN}
    p.notifier.notifications.clear()
    return sig, trade.id


def fx_closing_deals(p: Prod) -> list[float]:
    """Volumes of the closing deals the fx terminal executed (a DEAL with a position ticket)."""
    return [d.volume for d in p.terminal.deals if d.entry == MT5.DEAL_ENTRY_OUT]


async def test_e1_fx_partial_close_whose_answer_was_lost_is_not_repeated(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The owner's prop account (native MT5): the closing deal of 'yarısını kapattık' executes, the terminal's answer
    is lost (order_send -> None, IPC timeout), the close is owed. fx's sync reads the live volume before the owed
    retry runs (the adapter rebuilds the size from the account), so the retry finds the half already closed and sends
    nothing: one closing deal, half of the lots still open."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, trade_id = await eur_long(p)
        lots = (await p.positions(trade_id))["fx"].remaining_quantity
        term = p.terminal
        real_send = term.order_send
        state = {"lost": 0}

        def send(request: dict[str, Any]) -> Any:
            if request.get("action") == MT5.TRADE_ACTION_DEAL and "position" in request and state["lost"] == 0:
                state["lost"] += 1
                real_send(request)                  # executed at the server ...
                term.last = (-10005, "IPC timeout")  # ... and the answer never reaches the bot
                return None
            return real_send(request)
        term.order_send = send                      # type: ignore[method-assign]
        await p.say(half("EUR", trade_id, "USD"), FX, "yarısını kapattık", reply_to=sig)
        owed = (await p.positions(trade_id))["fx"].data.get(MANAGE_WANTED)
        # (review 5: a lost partial close on fx / tickmill is held about a minute before its retry, see
        # tests/test_review5_engine.py: the bot's clock is moved past that hold)
        from datetime import timedelta
        _clk = p.engine._clock
        p.engine._clock = p.engine.monitor._clock = lambda: _clk() + timedelta(seconds=61)
        for _ in range(3):
            await due_now(p, trade_id, "fx")
            await p.engine.monitor.run_once()
        assert state["lost"] == 1 and isinstance((owed or {}).get("reduce"), dict)   # (it was owed)
        closed = fx_closing_deals(p)
        assert len(closed) == 1, closed
        pos = (await p.positions(trade_id))["fx"]
        open_lots = sum(x.volume for x in term.positions.values())
        assert open_lots == pytest.approx(lots - closed[0]) and pos.remaining_quantity == pytest.approx(open_lots)
        assert MANAGE_WANTED not in pos.data
    finally:
        await p.close()


async def test_e1_tickmill_partial_close_whose_answer_was_lost_is_not_repeated(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The friend's MetaApi account: the partial close executes but the SDK times out. The sync reads the live volume
    before the owed retry, so nothing is sent again."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, trade_id = await eur_long(p)
        lots = (await p.positions(trade_id))["tickmill"].remaining_quantity
        original = p.tickmill.close_position_partially

        async def lossy(position_id: str, volume: float, options: Any = None) -> Any:
            await original(position_id, volume, options)        # executed at the account ...
            raise TimeoutException("MetaApi websocket client request of type trade timed out")    # ... answer lost
        p.tickmill.close_position_partially = lossy             # type: ignore[method-assign]
        await p.say(half("EUR", trade_id, "USD"), FX, "yarısını kapattık", reply_to=sig)
        owed = (await p.positions(trade_id))["tickmill"].data.get(MANAGE_WANTED)
        # (review 5: a lost partial close on fx / tickmill is held about a minute before its retry, see
        # tests/test_review5_engine.py: the bot's clock is moved past that hold)
        from datetime import timedelta
        _clk = p.engine._clock
        p.engine._clock = p.engine.monitor._clock = lambda: _clk() + timedelta(seconds=61)
        for _ in range(3):
            await due_now(p, trade_id, "tickmill")
            await p.engine.monitor.run_once()
        await asyncio.sleep(0.2)            # (tickmill's sync may run detached)
        sent = [c for c in p.tickmill.calls if c[0] == "close_position_partially"]
        assert isinstance((owed or {}).get("reduce"), dict) and len(sent) == 1, sent
        pos = (await p.positions(trade_id))["tickmill"]
        assert pos.remaining_quantity == pytest.approx(lots - sent[0][1][1])
        assert MANAGE_WANTED not in pos.data
    finally:
        await p.close()


async def test_e1_breakout_manual_instruction_is_never_doubled_by_the_owed_retry(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The manual broker hands its instruction to the notifier only after its own record has taken the close; the
    only failure (no price for the record) comes before any instruction. So the owed retry sends the one instruction
    later, and never a second one."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, trade_id = await btc_long(p)
        sim = p.brokers["breakout"].sim
        quote = sim.quote_fn
        state = {"down": True}

        async def no_price(info: Any) -> Any:
            if state["down"]:
                raise BrokerError("breakout: BTC/USDT:USDT fiyatı alınamadı: binanceusdm GET ticker timed out")
            return await quote(info)
        sim.quote_fn = no_price
        p.notifier.notifications.clear()
        await p.say(half("BTC", trade_id, "USDT"), KRIPTO, "yarısını kapattık", reply_to=sig)
        # Review 4 (A3): the close instruction names the coin amount to close and what stays open (was
        # "<b>❌ BTC: POZİSYONUN %50'ini KAPAT</b>", a share of the original size).
        # Review 5 (Q2): the percent was dropped and USD values added (was "(açık pozisyonun yaklaşık %49'u; ...").
        instruction = "<b>❌ BTC: 0.035 BTC KAPAT</b> (≈ <b>"
        assert not [t for t in p.texts() if instruction in t]
        assert isinstance((await p.positions(trade_id))["breakout"].data.get(MANAGE_WANTED, {}).get("reduce"), dict)
        state["down"] = False
        for _ in range(3):
            await due_now(p, trade_id, "breakout")      # (no back-off between the passes: a repeat would show)
            await p.engine.monitor.run_once()
            await asyncio.sleep(0.05)
        assert len([t for t in p.texts() if instruction in t]) == 1, p.texts()
        assert MANAGE_WANTED not in (await p.positions(trade_id))["breakout"].data
    finally:
        await p.close()


# =========================================================================== #
# E2: the late-leg catch-up never puts an older stop / target back over a newer instruction
# =========================================================================== #
def slow_tickmill_open(p: Prod) -> asyncio.Event:
    """tickmill's market buy is executed at once at the account, but its answer only comes when the returned event
    is set (MetaApi trade answers can take long): its open leg goes on as a late one after the grace."""
    gate = asyncio.Event()
    real_buy = p.tickmill.create_market_buy_order

    async def gated_buy(*a: Any, **k: Any) -> Any:
        res = await real_buy(*a, **k)
        await gate.wait()
        return res
    p.tickmill.create_market_buy_order = gated_buy      # type: ignore[method-assign]
    return gate


def tickmill_answers_during_the_next_refresh(p: Prod, gate: asyncio.Event) -> None:
    """The verifier's natural timeline: the trader's next instruction first syncs the trade (the trade lock is held
    while fx is read, 50 ms here); tickmill's late open answers 10 ms into that read, so its position is stored while
    the next instruction waits for the trade lock - before the late leg is booked."""
    fx = p.brokers["fx"]
    real_sync = fx.sync_position
    loop = asyncio.get_running_loop()
    n = {"sync": 0}

    async def fx_sync(pos: Any) -> Any:
        n["sync"] += 1
        if n["sync"] == 1:
            loop.call_later(0.01, gate.set)
            await asyncio.sleep(0.05)
        return await real_sync(pos)
    fx.sync_position = fx_sync                          # type: ignore[method-assign]


def tp_at(trade_id: int, level: float) -> ParsedAction:
    return ParsedAction(action=ActionType.MOVE_TP, take_profits=[tps.TakeProfit(price=level)], base_asset="EUR",
                        quote_asset="USD", side=Side.LONG, target_trade_id=trade_id, confidence=0.95,
                        summary_tr=f"EUR TP {level}")


def be_eur(trade_id: int) -> ParsedAction:
    return ParsedAction(action=ActionType.MOVE_SL, move_sl_to_entry=True, base_asset="EUR", quote_asset="USD",
                        side=Side.LONG, target_trade_id=trade_id, confidence=0.95, summary_tr="EUR stop girişe")


def tickmill_stops(p: Prod) -> list[float]:
    return [c[2].get("stop_loss") for c in p.tickmill.calls if c[0] == "modify_position"]


async def late_booked(p: Prod) -> None:
    """Until tickmill's late open is booked, its catch-up is over and its notice went out."""
    await until(lambda: not p.engine._late_legs, 10.0, "tickmill's late open to be booked")
    await until(lambda: any("tickmill geç yanıt verdi" in t for t in p.texts()), 10.0, "the late notice")
    await asyncio.sleep(0.1)


@pytest.mark.parametrize("variant", ["stop", "stop_to_entry", "targets", "owed", "refused_there"])
async def test_e2_catch_up_never_replays_an_older_level_over_a_newer_instruction(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, variant: str) -> None:
    """The referee's race (newdefects2b problem 1 / newdefects2 problem 2 / closed2 C3): X is given while tickmill's
    open is unanswered (kept for its catch-up); Y comes next, and tickmill's position is stored while Y waits for the
    trade lock. Y is applied to that position directly; on d45f96a the booking then replayed the OLDER X over it
    (tickmill 1.161 while fx and the trade had 1.1615, also for stop to entry: a loss-side stop the owner believed
    at break-even; an owed Y was overwritten). Now X is dropped wherever Y was applied, is owed or on its way - and
    kept where tickmill refused Y outright (then X is still the closest tickmill gets to the trader's level)."""
    from copytrader.engine import orchestrator as orch
    monkeypatch.setattr(orch, "OPTIONAL_LEG_GRACE_SEC", 0.5)
    p = await make(tmp_path, monkeypatch)
    try:
        set_eurusd(p, 1.1619, 1.1621)
        gate = slow_tickmill_open(p)
        sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        trade_id = (await p.trades())[-1].id
        assert [leg.broker for leg in p.engine._late_legs.values()] == ["tickmill"]
        x, y = {"stop": (stop_at(trade_id, 1.1610), stop_at(trade_id, 1.1615)),
                "stop_to_entry": (stop_at(trade_id, 1.1610), be_eur(trade_id)),
                "targets": (tp_at(trade_id, 1.168), tp_at(trade_id, 1.170)),
                "owed": (stop_at(trade_id, 1.1610), stop_at(trade_id, 1.1615)),
                "refused_there": (stop_at(trade_id, 1.1610), stop_at(trade_id, 1.1615))}[variant]
        await p.say(x, FX, "X", reply_to=sig)
        assert [len(leg.missed_plans) for leg in p.engine._late_legs.values()] == [1]
        if variant == "stop_to_entry":
            set_eurusd(p, 1.1640, 1.1642)           # (the trader is in profit: break even is allowed everywhere)
        tm = p.brokers["tickmill"]
        if variant in ("owed", "refused_there"):
            real_update = tm.update_stop_loss

            async def update_stop_loss(pos: Any, sl: float) -> Any:
                if abs(sl - 1.1615) < 1e-9 and not getattr(update_stop_loss, "done", False):
                    update_stop_loss.done = True    # type: ignore[attr-defined]
                    if variant == "owed":
                        raise BrokerError("tickmill: Stop değiştirilemedi: MetaApi isteği zaman aşımına uğradı")
                    raise orch._LevelRefused("yeni stop (1.1615) anlık fiyatın (1.1614) yanlış tarafında; stop "
                                             "taşınmadı")
                return await real_update(pos, sl)
            tm.update_stop_loss = update_stop_loss      # type: ignore[method-assign]
        tickmill_answers_during_the_next_refresh(p, gate)
        await p.say(y, FX, "Y", reply_to=sig)
        await late_booked(p)
        pos = await p.positions(trade_id)
        trade = (await p.trades())[-1]
        tm_entry = pos["tickmill"].avg_entry_price
        if variant == "stop":
            assert trade.current_stop_loss == pytest.approx(1.1615) and pos["fx"].stop_loss == pytest.approx(1.1615)
            assert pos["tickmill"].stop_loss == pytest.approx(1.1615) and tickmill_stops(p)[-1] == pytest.approx(1.1615)
        elif variant == "stop_to_entry":
            assert pos["fx"].stop_loss == pytest.approx(pos["fx"].avg_entry_price)
            assert pos["tickmill"].stop_loss == pytest.approx(tm_entry) and tickmill_stops(p)[-1] == pytest.approx(
                tm_entry)
        elif variant == "targets":
            assert [tp.price for tp in trade.current_take_profits] == [pytest.approx(1.170)]
            assert pos["tickmill"].take_profits[0].price == pytest.approx(1.170)
            assert [c[2].get("take_profit") for c in p.tickmill.calls if c[0] == "modify_position"][-1] == \
                pytest.approx(1.170)
        elif variant == "owed":
            owed = pos["tickmill"].data.get(MANAGE_WANTED) or {}
            assert (owed.get("sl") or {}).get("price") == pytest.approx(1.1615), owed     # the newer one stays owed
            await due_now(p, trade_id, "tickmill")
            await p.engine.monitor.run_once()
            await until(lambda: any(APPLIED in t and "stop 1.1615" in t for t in p.texts()), 5.0, "the owed stop")
            assert (await p.positions(trade_id))["tickmill"].stop_loss == pytest.approx(1.1615)
            assert tickmill_stops(p)[-1] == pytest.approx(1.1615)
        else:   # refused_there: tickmill refused Y outright; X (applied by the catch-up) is the closest it gets
            assert pos["tickmill"].stop_loss == pytest.approx(1.1610) and tickmill_stops(p) == [pytest.approx(1.161)]
            assert any("şimdi uygulandı: stop 1.161" in t for t in p.texts()), p.texts()
        if variant != "refused_there":      # the late notice never says the older level was applied
            assert not [t for t in p.texts() if "şimdi uygulandı: stop 1.161" in t or "şimdi uygulandı: TP 1.168" in t]
    finally:
        gate.set()
        await p.close()


# =========================================================================== #
# E3: an open whose answer was lost stays "unknown" - also when another broker answers late with a refusal
# =========================================================================== #
def fx_open_answer_lost(p: Prod) -> None:
    """The verifier's V1: fx's market entry REALLY executes on the prop account, the terminal's answer is lost
    (order_send -> None, IPC timeout) and the bot's look-up by tag right after fails too (the link is still down)."""
    term = p.terminal
    real_send, real_positions, real_orders = term.order_send, term.positions_get, term.orders_get
    blind = {"n": 0}

    def send(request: dict[str, Any]) -> Any:
        if request.get("action") == MT5.TRADE_ACTION_DEAL and "position" not in request:
            real_send(request)                  # executed at the server ...
            term.last = (-10005, "IPC timeout")  # ... the answer never reaches the bot
            blind["n"] = 6
            return None
        return real_send(request)

    def positions_get(*a: Any, **k: Any) -> Any:
        if blind["n"] > 0:
            blind["n"] -= 1
            term.last = (-10004, "No IPC connection")
            return None
        return real_positions(*a, **k)

    def orders_get(*a: Any, **k: Any) -> Any:
        if blind["n"] > 0:
            blind["n"] -= 1
            term.last = (-10004, "No IPC connection")
            return None
        return real_orders(*a, **k)
    term.order_send, term.positions_get, term.orders_get = send, positions_get, orders_get   # type: ignore[method-assign]


def tickmill_open_answers_late(p: Prod, end: str) -> asyncio.Event:
    """tickmill's market buy answers only when the returned event is set: "refused" (market closed, nothing opened),
    "lost" (the SDK's own timeout after the request went out: unknown)."""
    from metaapi_cloud_sdk.clients.metaapi.trade_exception import TradeException
    gate = asyncio.Event()

    async def buy(*a: Any, **k: Any) -> Any:
        await gate.wait()
        if end == "refused":
            raise TradeException("Market is closed", 10018, "TRADE_RETCODE_MARKET_CLOSED")
        raise TimeoutException("Timed out waiting for the response")
    p.tickmill.create_market_buy_order = buy        # type: ignore[method-assign]
    return gate


async def eur_long_unknown(p: Prod, tickmill_end: str) -> tuple[int, int, list[str], asyncio.Event]:
    set_eurusd(p, 1.1619, 1.1621)
    fx_open_answer_lost(p)
    gate = tickmill_open_answers_late(p, tickmill_end)
    p.notifier.notifications.clear()
    sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
    trade_id = (await p.trades())[-1].id
    report = [t for t in p.texts() if f"#{trade_id} EUR LONG" in t and "<b>" in t]
    return sig, trade_id, report, gate


async def test_e3_fx_order_whose_answer_was_lost_is_never_called_not_opened_when_tickmill_refuses_late(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The verifier's V1 (newdefects2b problem 2): d45f96a told the owner 'henüz hiçbir brokerda açılmadı', then 'Bu
    işlem hiçbir brokerda açılamadı', stored the note 'fx: error' and answered the trader's later 'kapat' with
    'hiçbir hesapta açılamamıştı; yapılacak bir şey yok' - while a 0.47-lot position sat on the prop account. Now
    every text says that fx's order went out and its result is unknown, and the ignored 'kapat' tells the owner to
    check fx and close it there by hand."""
    from copytrader.engine import orchestrator as orch
    monkeypatch.setattr(orch, "OPTIONAL_LEG_GRACE_SEC", 0.5)
    p = await make(tmp_path, monkeypatch)
    try:
        sig, trade_id, report, gate = await eur_long_unknown(p, "refused")
        lost = "emir gönderildi ama sonucu alınamadı; fx hesabında açılıp açılmadığı bilinmiyor, kontrol edin"
        assert [r.splitlines()[0] for r in report] == [
            f"⏳ <b>#{trade_id} EUR LONG: {lost}</b>; yanıt vermeyen broker bekleniyor."], report
        p.notifier.notifications.clear()
        gate.set()
        await until(lambda: any("tickmill geç yanıt verdi" in t for t in p.texts()), 10.0, "the late notice")
        (late,) = [t for t in p.texts() if "tickmill geç yanıt verdi" in t]
        assert late.splitlines()[-1] == f"• E{lost[1:]}." and "hiçbir brokerda açılamadı" not in late, late
        trade = (await p.trades())[-1]
        assert trade.status.value == "failed" and trade.note == "fx: unknown; tickmill: error"
        assert [(x.symbol, x.volume) for x in p.terminal.positions.values()] == [("EURUSD", pytest.approx(0.47))]
        # the trader closes it: the bot does not manage fx's order, and says so
        p.notifier.notifications.clear()
        await p.say(close_all(trade_id), FX, "EUR kapat", reply_to=sig)
        assert p.texts() == [
            f"⚠️ Yok sayıldı: EUR kapat — #{trade_id} EUR LONG işleminin fx emrinin durumu bilinmiyor (emir "
            "gönderilmişti ama sonucu alınamadı); bot bu işlemi orada yönetmiyor. Kontrol edin: emir fx hesabında "
            "açıldıysa bu talimatı orada elle uygulayın."]
    finally:
        gate.set()
        await p.close()


async def test_e3_both_unknown_are_named_and_the_change_queued_for_the_late_leg_is_reported_not_applied(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """V2 + (c): fx's answer is lost and tickmill's late answer is lost too, after the trader moved the stop while
    tickmill was unanswered. The late notice names both accounts (d45f96a named tickmill only) and says that the stop
    move was not applied to tickmill's order (nobody knows whether it opened)."""
    from copytrader.engine import orchestrator as orch
    monkeypatch.setattr(orch, "OPTIONAL_LEG_GRACE_SEC", 0.5)
    p = await make(tmp_path, monkeypatch)
    try:
        sig, trade_id, _report, gate = await eur_long_unknown(p, "lost")
        p.notifier.notifications.clear()
        await p.say(stop_at(trade_id, 1.1610), FX, "stop 1.1610", reply_to=sig)
        p.notifier.notifications.clear()
        gate.set()
        await until(lambda: any("tickmill geç yanıt verdi" in t for t in p.texts()), 10.0, "the late notice")
        (late,) = [t for t in p.texts() if "tickmill geç yanıt verdi" in t]
        assert ("• ⚠️ Bu emir yanıt beklerken işlemde yapılan şu değişiklikler bu hesaba UYGULANMADI (emrin durumu "
                "bilinmiyor): ") in late and "stop 1.161" in late, late
        assert late.splitlines()[-1] == ("• Emir gönderildi ama sonucu alınamadı; fx ve tickmill hesaplarında açılıp "
                                         "açılmadığı bilinmiyor, kontrol edin."), late
        assert (await p.trades())[-1].note == "fx: unknown; tickmill: unknown"
    finally:
        gate.set()
        await p.close()


async def test_e3_close_while_the_late_leg_is_unanswered_and_its_answer_is_lost_tells_the_owner_to_close_it(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """(c) for a close: the trader closes the trade while tickmill's open is unanswered; its answer is then lost.
    Nothing can be closed (no position is known): the owner is told to close it by hand if it opened."""
    from copytrader.engine import orchestrator as orch
    monkeypatch.setattr(orch, "OPTIONAL_LEG_GRACE_SEC", 0.5)
    p = await make(tmp_path, monkeypatch)
    try:
        set_eurusd(p, 1.1619, 1.1621)
        gate = tickmill_open_answers_late(p, "lost")
        sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        trade_id = (await p.trades())[-1].id
        await p.say(close_all(trade_id), FX, "EUR kapat", reply_to=sig)
        assert (await p.positions(trade_id))["fx"].status is PositionStatus.CLOSED
        p.notifier.notifications.clear()
        gate.set()
        await until(lambda: any("tickmill geç yanıt verdi" in t for t in p.texts()), 10.0, "the late notice")
        (late,) = [t for t in p.texts() if "tickmill geç yanıt verdi" in t]
        assert "• ⚠️ trader işlemi kapattı / iptal etti; bu emir tickmill hesabında açıldıysa elle kapatın." in late, late
    finally:
        gate.set()
        await p.close()


async def test_e3_definite_refusals_keep_hicbir_brokerda_acilamadi(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Guard (V4): fx refuses outright and tickmill refuses late: nothing was opened anywhere, and the owner is told
    so as before ('Bu işlem hiçbir brokerda açılamadı'; the trader's later 'kapat' has nothing to do)."""
    from copytrader.engine import orchestrator as orch
    monkeypatch.setattr(orch, "OPTIONAL_LEG_GRACE_SEC", 0.5)
    p = await make(tmp_path, monkeypatch)
    try:
        set_eurusd(p, 1.1619, 1.1621)
        p.terminal.hooks.append(lambda r: 10019 if r.get("action") == MT5.TRADE_ACTION_DEAL else None)   # No money
        gate = tickmill_open_answers_late(p, "refused")
        sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        trade_id = (await p.trades())[-1].id
        assert any("henüz hiçbir brokerda açılmadı" in t for t in p.texts())
        p.notifier.notifications.clear()
        gate.set()
        await until(lambda: any("tickmill geç yanıt verdi" in t for t in p.texts()), 10.0, "the late notice")
        (late,) = [t for t in p.texts() if "tickmill geç yanıt verdi" in t]
        assert late.splitlines()[-1] == "• Bu işlem hiçbir brokerda açılamadı."
        assert (await p.trades())[-1].note == "fx: error; tickmill: error"
        p.notifier.notifications.clear()
        await p.say(close_all(trade_id), FX, "EUR kapat", reply_to=sig)
        assert p.texts() == [f"💬 Yok sayıldı: EUR kapat — #{trade_id} EUR LONG işlemi hiçbir hesapta açılamamıştı; "
                             "yapılacak bir şey yok."]
    finally:
        gate.set()
        await p.close()


async def test_e3_a_cancelled_open_leg_is_worded_may_have_been_sent_never_sent(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """(b): an open leg task cut off from outside may have stopped before or after its order went out. Its headline
    says 'emir gönderilmiş olabilir', never 'emir gönderildi' (d45f96a)."""
    p = await make(tmp_path, monkeypatch)
    try:
        set_eurusd(p, 1.1619, 1.1621)
        fx = p.brokers["fx"]
        started = asyncio.Event()

        async def hanging_open(req: Any) -> Any:
            started.set()
            await asyncio.sleep(30)
            raise AssertionError("not reached")
        fx.open_position = hanging_open             # type: ignore[method-assign]

        async def cut_the_fx_leg() -> None:
            await started.wait()
            (leg,) = [t for t in asyncio.all_tasks() if t.get_name().startswith("open-") and t.get_name().endswith("-fx")]
            leg.cancel()
        cutter = asyncio.ensure_future(cut_the_fx_leg())
        await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        await cutter
        trade_id = (await p.trades())[-1].id
        heads = [t.splitlines()[0] for t in p.texts() if f"#{trade_id} EUR LONG" in t and "<b>" in t]
        assert heads == [f"⚠️ <b>#{trade_id} EUR LONG kısmen açıldı</b> — emir gönderilmiş olabilir; fx hesabında "
                         "açılıp açılmadığı bilinmiyor, kontrol edin"], heads
        assert (await p.trades())[-1].note == "fx: maybe"
    finally:
        await p.close()


# =========================================================================== #
# E4: no position anywhere yet, only tickmill's late open pending: the owner still gets the management report
# =========================================================================== #
async def test_e4_stop_move_and_close_while_only_tickmills_late_open_is_pending_are_reported(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """newdefects2b problem 4: fx refused the open, tickmill's open is unanswered. A stop move and then a close sent
    the owner no report at all on d45f96a ('notices 0'). Now each one says what will happen to tickmill's order."""
    from copytrader.engine import orchestrator as orch
    monkeypatch.setattr(orch, "OPTIONAL_LEG_GRACE_SEC", 0.5)
    p = await make(tmp_path, monkeypatch)
    try:
        set_eurusd(p, 1.1619, 1.1621)
        gate = slow_tickmill_open(p)

        async def refuse(req: Any) -> Any:
            raise BrokerError("Emir açılamadı: [10019] No money")
        p.brokers["fx"].open_position = refuse          # type: ignore[method-assign]
        sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        trade_id = (await p.trades())[-1].id
        p.notifier.notifications.clear()
        await p.say(stop_at(trade_id, 1.1610), FX, "stop 1.1610", reply_to=sig)
        heads = [t.splitlines()[0] for t in p.texts()]
        assert heads == [f"⏳ #{trade_id} EUR LONG: tickmill açılış emrine yanıt verince ona uygulanacak"], p.texts()
        assert "• ⏳ tickmill: açılış emri henüz yanıtlanmadı; açılırsa bu değişiklik ona hemen uygulanacak" in \
            p.texts()[0]
        p.notifier.notifications.clear()
        await p.say(close_all(trade_id), FX, "kapat", reply_to=sig)
        heads = [t.splitlines()[0] for t in p.texts()]
        assert heads == [f"⏳ #{trade_id} EUR LONG: tickmill açılırsa hemen kapatılacak"], p.texts()
        p.notifier.notifications.clear()
        gate.set()
        await late_booked(p)
        assert (await p.positions(trade_id))["tickmill"].status is PositionStatus.CLOSED
        assert any("geç açılan bu pozisyon hemen kapatıldı" in t for t in p.texts()), p.texts()
    finally:
        gate.set()
        await p.close()


# =========================================================================== #
# E5: an owed change is worded after why it was not applied now
# =========================================================================== #
def tickmill_does_not_answer(p: Prod) -> None:
    """The friend's MetaApi stops answering: every request times out at once (the SDK's TimeoutException)."""
    conn = p.tickmill
    original = conn._rec

    def rec(name: str, *args: Any, **kwargs: Any) -> None:
        conn.calls.append((f"DOWN:{name}", args, kwargs))
        raise TimeoutException(f"MetaApi websocket client request of type {name} timed out")
    conn._rec = rec                                 # type: ignore[method-assign]
    p._tickmill_rec = original                      # type: ignore[attr-defined]


@pytest.mark.parametrize("case", ["tickmill_no_answer", "fx_refused", "tickmill_not_connected", "only_tickmill"])
async def test_e5_owed_change_is_worded_after_why_it_was_not_applied(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str) -> None:
    """newdefects2b problem 5: d45f96a said 'fx hesabı yanıt verince ona da uygulanacak' / 'Bot, fx hesabı yanıt
    verince yeniden deneyecek' also when fx had answered with a refusal ('[10016] Invalid stops') or a broker was not
    connected. Now the headline and the failure line say what happened: no answer, a refusal, not connected."""
    p = await make(tmp_path, monkeypatch)
    try:
        if case == "only_tickmill":
            async def refuse(req: Any) -> Any:
                raise BrokerError("Emir açılamadı: [10019] No money")
            p.brokers["fx"].open_position = refuse      # type: ignore[method-assign]
            set_eurusd(p, 1.1619, 1.1621)
            sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
            trade_id = (await p.trades())[-1].id
            p.notifier.notifications.clear()
        else:
            sig, trade_id = await eur_long(p)
        if case in ("tickmill_no_answer", "only_tickmill"):
            tickmill_does_not_answer(p)
        elif case == "fx_refused":
            async def invalid_stops(pos: Any, sl: float) -> Any:
                raise BrokerError("Stop değiştirilemedi: [10016] Invalid stops")
            p.brokers["fx"].update_stop_loss = invalid_stops        # type: ignore[method-assign]
        else:
            p.engine.brokers.remove(p.brokers["tickmill"])          # (not connected)
        await p.say(stop_at(trade_id, 1.1610), FX, "stop 1.1610", reply_to=sig)
        texts = p.texts()
        head = next(t.splitlines()[0] for t in texts if t.startswith("⏳ "))
        line = next(t for t in texts if t.startswith(f"#{trade_id} "))
        label = f"#{trade_id} EUR LONG"
        if case == "tickmill_no_answer":
            assert head == f"⏳ {label}: uygulandı; tickmill hesabından yanıt alınamadı; bot yeniden deneyecek"
            assert line.endswith("; tickmill hesabından yanıt alınamadı, değişiklik şimdi doğrulanamadı. Bot yeniden "
                                 "deneyecek ve uygulayınca haber verecek"), line
        elif case == "only_tickmill":
            assert head == (f"⏳ {label}: şimdi uygulanamadı; tickmill hesabından yanıt alınamadı; bot yeniden "
                            "deneyecek")
        elif case == "fx_refused":
            assert head == f"⏳ {label}: uygulandı; fx reddetti; bot sonraki kontrollerde yeniden deneyecek"
            assert line == (f"#{trade_id} fx: <code>Stop değiştirilemedi: [10016] Invalid stops</code>; fx değişikliği "
                            "reddetti. Bot sonraki kontrollerde yeniden deneyecek ve uygulayınca haber verecek")
        else:
            assert head == f"⏳ {label}: uygulandı; tickmill bağlanınca ona da uygulanacak"
            assert line == (f"#{trade_id} tickmill: broker bağlı değil; değişiklik şimdi uygulanamadı. tickmill "
                            "bağlanınca bot uygulayacak ve haber verecek")
        assert not [t for t in texts if "yanıt verince" in t], texts
    finally:
        await p.close()


# =========================================================================== #
# E6: shutdown around a late leg
# =========================================================================== #
LATE_KEY = "late_open_legs"
UNANSWERED = "bot durduğunda tickmill emrine henüz yanıt gelmemişti"


async def test_e6_stop_while_the_catch_up_runs_does_not_make_the_restart_call_the_booked_order_unanswered(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """(a): tickmill's late open is booked (its position is stored) and its catch-up hangs on tickmill; the bot stops
    in that window. d45f96a removed the leg from the stored late-leg state only after the catch-up, so the restart
    said 'bot durduğunda tickmill emrine henüz yanıt gelmemişti ... bilinmiyor' about a stored position."""
    from copytrader.engine import orchestrator as orch
    monkeypatch.setattr(orch, "OPTIONAL_LEG_GRACE_SEC", 1.0)
    db_file = str(tmp_path / "bot.sqlite")
    p = await make(tmp_path, monkeypatch, db_path=db_file)
    hang = asyncio.Event()
    try:
        set_eurusd(p, 1.1619, 1.1621)
        gate = slow_tickmill_open(p)
        sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        trade_id = (await p.trades())[-1].id
        await p.say(stop_at(trade_id, 1.1610), FX, "stop 1.1610", reply_to=sig)
        assert list((await p.db.get_state(LATE_KEY, {})).keys()) == [f"{trade_id}:tickmill"]
        real_modify = p.tickmill.modify_position

        async def hanging_modify(*a: Any, **k: Any) -> Any:
            await hang.wait()                       # the catch-up's stop move on tickmill gets no answer
            return await real_modify(*a, **k)
        p.tickmill.modify_position = hanging_modify     # type: ignore[method-assign]
        gate.set()
        await until(lambda: not p.engine._late_legs, 10.0, "tickmill's late open to be booked")
        await asyncio.sleep(0.2)                        # (inside the catch-up's wait now)
        assert (await p.positions(trade_id))["tickmill"].status is PositionStatus.OPEN
        assert await p.db.get_state(LATE_KEY, {}) == {}
        await p.engine.stop(grace_sec=0.3)              # the bot stops while the catch-up still waits for tickmill
    finally:
        hang.set()
        gate.set()
        await p.db.close()
    p2 = await make(tmp_path, monkeypatch, db_path=db_file, terminal=p.terminal, tickmill=p.tickmill,
                    binance=p.binance, clear=False)
    try:
        assert not [t for t in p2.texts() if UNANSWERED in t], p2.texts()
    finally:
        await p2.close()


@pytest.mark.parametrize("what", ["stop", "close"])
async def test_e6_restart_notice_names_what_was_promised_to_an_order_still_unanswered(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, what: str) -> None:
    """(b): the bot stops while tickmill's open is still unanswered, after the trader moved the stop (promised to
    tickmill's position once it is known) or closed the trade (to be closed on arrival). d45f96a kept that only in
    memory: the restart said 'elle kontrol edin' without naming it. Now the stored late-leg state carries it and the
    restart notice names it."""
    from copytrader.engine import orchestrator as orch
    monkeypatch.setattr(orch, "OPTIONAL_LEG_GRACE_SEC", 0.5)
    db_file = str(tmp_path / "bot.sqlite")
    p = await make(tmp_path, monkeypatch, db_path=db_file)
    gate = asyncio.Event()
    try:
        set_eurusd(p, 1.1619, 1.1621)
        gate = slow_tickmill_open(p)
        sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        trade_id = (await p.trades())[-1].id
        if what == "stop":
            await p.say(stop_at(trade_id, 1.1610), FX, "stop 1.1610", reply_to=sig)
        else:
            await p.say(close_all(trade_id), FX, "EUR kapat", reply_to=sig)
        await p.engine.stop(grace_sec=0.3)              # tickmill's open is still unanswered
    finally:
        await p.db.close()
    p2 = await make(tmp_path, monkeypatch, db_path=db_file, terminal=p.terminal, tickmill=p.tickmill,
                    binance=p.binance, clear=False)
    try:
        (notice,) = [t for t in p2.texts() if UNANSWERED in t]
        if what == "stop":
            assert notice.endswith(" Açılırsa şu değişiklikler uygulanacaktı: stop 1.161 seviyesine çekilecek; emir "
                                   "açıldıysa bunları tickmill hesabında elle uygulayın."), notice
        else:
            assert notice.endswith(" Bu arada işlem kapatılmıştı (trader işlemi kapattı / iptal etti): emir "
                                   "açıldıysa tickmill hesabında elle kapatın."), notice
    finally:
        gate.set()
        await p2.close()


# =========================================================================== #
# E7: close-on-arrival of a late tickmill position runs under its position lock only
# =========================================================================== #
async def test_e7_close_on_arrival_of_tickmill_never_holds_the_trade_lock_and_ends_the_trade_as_before(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The trader closes the trade while tickmill's open is unanswered; tickmill then answers and its position is
    closed at once - but tickmill's close hangs. d45f96a (and round 1) sent that close under the trade lock, so
    anything of the owner's for this trade (fx's close retry, a sync, the next instruction) waited for the friend's
    account. Now the booking comes first as before (the position is stored with its close wish when the close goes
    out), the trade lock is free while tickmill's close hangs, and the trade ends CLOSED as before."""
    from copytrader.engine import orchestrator as orch
    monkeypatch.setattr(orch, "OPTIONAL_LEG_GRACE_SEC", 0.5)
    p = await make(tmp_path, monkeypatch)
    release = asyncio.Event()
    gate = asyncio.Event()
    try:
        set_eurusd(p, 1.1619, 1.1621)
        gate = slow_tickmill_open(p)
        sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        trade_id = (await p.trades())[-1].id
        await p.say(close_all(trade_id), FX, "EUR kapat", reply_to=sig)
        assert (await p.positions(trade_id))["fx"].status is PositionStatus.CLOSED
        real_close = p.tickmill.close_position
        seen: dict[str, Any] = {}

        async def hanging_close(position_id: str, options: Any = None) -> Any:
            row = (await p.positions(trade_id)).get("tickmill")
            seen["stored_with_wish"] = row is not None and isinstance(row.data.get("close_wanted"), dict)
            seen["entered"] = True
            await release.wait()                    # the friend's account does not answer for a while
            return await real_close(position_id, options)
        p.tickmill.close_position = hanging_close   # type: ignore[method-assign]
        p.notifier.notifications.clear()
        gate.set()
        await until(lambda: seen.get("entered", False), 10.0, "tickmill's close on arrival")
        assert seen["stored_with_wish"]             # booked first: stored, its close remembered, then sent
        lock = p.engine.trade_lock(trade_id)
        started = time.monotonic()
        await asyncio.wait_for(lock.acquire(), 1.0)     # (d45f96a: held until tickmill answers -> TimeoutError)
        lock.release()
        assert time.monotonic() - started < 0.5
        await until(lambda: any("tickmill geç yanıt verdi" in t for t in p.texts()), 5.0, "the late notice")
        (late,) = [t for t in p.texts() if "tickmill geç yanıt verdi" in t]
        assert late.startswith(f"⚠️ <b>#{trade_id} EUR LONG</b> — tickmill geç yanıt verdi"), late
        assert late.endswith("• trader işlemi kapattı / iptal etti; geç açılan bu pozisyon kapatılıyor; sonuç ayrıca "
                             "bildirilecek."), late
        release.set()
        await until(lambda: any("geç açılan bu pozisyon hemen kapatıldı" in t for t in p.texts()), 5.0,
                    "the result of the close")
        (result,) = [t for t in p.texts() if "geç açılan bu pozisyon hemen kapatıldı" in t]
        assert result.startswith(f"<b>#{trade_id} EUR LONG</b> — tickmill:\n• trader işlemi kapattı / iptal etti; "
                                 "geç açılan bu pozisyon hemen kapatıldı (tickmill: tamamı kapatıldı"), result
        await until(lambda: not p.engine._busy(), 5.0, "the close task to end")
        pos = await p.positions(trade_id)
        assert {b: x.status for b, x in pos.items()} == {"fx": PositionStatus.CLOSED,
                                                         "tickmill": PositionStatus.CLOSED}
        trade = (await p.trades())[-1]
        assert trade.status.value == "closed"
        assert "close_wanted" not in pos["tickmill"].data
    finally:
        release.set()
        gate.set()
        await p.close()


@pytest.mark.parametrize("ends", ["closed", "refused"])
async def test_e7_close_on_arrival_within_the_grace_is_reported_as_before(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ends: str) -> None:
    """Guard: a close on arrival that tickmill answers at once gives the same late notice as before - '⚠️ ...
    hemen kapatıldı' and a CLOSED trade - and one it refuses the same '🚨 ... KAPATILAMADI' notice (ERROR) with the
    close remembered and the trade active, so the monitor tries again."""
    from copytrader.engine import orchestrator as orch
    monkeypatch.setattr(orch, "OPTIONAL_LEG_GRACE_SEC", 0.5)
    p = await make(tmp_path, monkeypatch)
    gate = asyncio.Event()
    try:
        set_eurusd(p, 1.1619, 1.1621)
        gate = slow_tickmill_open(p)
        sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        trade_id = (await p.trades())[-1].id
        await p.say(close_all(trade_id), FX, "EUR kapat", reply_to=sig)
        if ends == "refused":
            from metaapi_cloud_sdk.clients.metaapi.trade_exception import TradeException

            async def refuse(position_id: str, options: Any = None) -> Any:
                raise TradeException("Market is closed", 10018, "TRADE_RETCODE_MARKET_CLOSED")
            p.tickmill.close_position = refuse      # type: ignore[method-assign]
        p.notifier.notifications.clear()
        gate.set()
        await until(lambda: any("tickmill geç yanıt verdi" in t for t in p.texts()), 10.0, "the late notice")
        await until(lambda: not p.engine._busy(), 5.0, "the close task to end")
        (late,) = [t for t in p.texts() if "tickmill geç yanıt verdi" in t]
        severity = next(s for t, s in p.notifier.notifications if t == late)
        pos = (await p.positions(trade_id))["tickmill"]
        trade = (await p.trades())[-1]
        if ends == "closed":
            assert late.startswith(f"⚠️ <b>#{trade_id} EUR LONG</b> — tickmill geç yanıt verdi") and \
                severity is Severity.WARNING
            assert "• trader işlemi kapattı / iptal etti; geç açılan bu pozisyon hemen kapatıldı (tickmill: tamamı " \
                   "kapatıldı" in late
            assert pos.status is PositionStatus.CLOSED and trade.status.value == "closed"
        else:
            assert late.startswith(f"🚨 <b>#{trade_id} EUR LONG</b> — tickmill geç yanıt verdi") and \
                severity is Severity.ERROR
            assert "KAPATILAMADI" in late and "İsterseniz tickmill hesabında elle de kapatabilirsiniz." in late
            assert pos.status is PositionStatus.OPEN and isinstance(pos.data.get("close_wanted"), dict)
            assert trade.status.value == "open"
    finally:
        gate.set()
        await p.close()
