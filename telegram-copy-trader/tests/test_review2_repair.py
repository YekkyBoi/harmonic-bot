"""Review 2 (lane repair2): the referee's round-1 problems with the friend's optional broker (tickmill) and three
smaller points, on the production replica of tests/test_preflight_scenarios.py (REAL Engine, Monitor, validator and
the CcxtBroker, ManualBroker, Mt5Broker and MetaApiBroker adapters on the fake venues) loaded with the server's config
of today (LIVE_YAML: kripto -> binance, breakout, tickmill; fx -> fx, tickmill; notify_on_execution; tickmill optional).

  R1  a stop / target move (stop to entry included) or a partial close that could not be applied to a position - the
      broker was still connecting after a restart, its MetaApi link was down, it refused or did not answer - is
      remembered on the position (MANAGE_WANTED) and applied once the broker answers again: on its connect and after
      a successful venue sync of the monitor (with back-off). The latest instruction wins, a partial close closes only
      what is still above the size that is to stay open (never twice), a position that ended drops it. The owner is
      told at the failure that the bot will apply it, and once when it has.
  R2  a stop / target move made while tickmill had not answered its open yet (the 3 s grace) is applied to the
      position that open brings, instead of "UYGULANMADI ... elle kontrol edin".
  R3  an open order that was SENT and whose answer was lost is never reported as "açılmadı" / "açılamadı": "emir
      gönderildi ama sonucu alınamadı; <broker> hesabında açılıp açılmadığı bilinmiyor, kontrol edin".
  R4  the "bağlı değil, bu işlem atlandı" notice and the brokers that take the trade come from one look at the
      connected brokers: a broker that connects in between does not take a trade it was told it skips.
  R5  a management headline says what really happened ("uygulandı; tickmill hesabı yanıt verince ona da
      uygulanacak"), never a bare "uygulandı" while a line below says a broker did not get it.

The R1 restart and R2 scenarios are the referee's own reproductions (referee1/newdefects/t/test_s1_restart_catchup.py,
test_s3_late_open_mgmt.py) turned into assertions.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from metaapi_cloud_sdk.clients.timeout_exception import TimeoutException

import test_preflight_scenarios as tps
from test_preflight_scenarios import (  # noqa: F401  (_isolate: autouse)
    EURJPY_SHORT,
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

import copytrader.brokers.metaapi_broker as mab
from copytrader.brokers.ccxt_broker import CcxtBroker
from copytrader.brokers.manual_broker import ManualBroker
from copytrader.brokers.metaapi_broker import MetaApiBroker
from copytrader.brokers.mt5_broker import MT5, Mt5Broker
from copytrader.engine import orchestrator as orch
from copytrader.engine.orchestrator import Engine
from copytrader.engine.prices import PriceOracle
from copytrader.models import (
    ActionType,
    EntryType,
    ParsedAction,
    PositionStatus,
    Severity,
    Side,
    TradeStatus,
)
from copytrader.storage.db import Database

# monitor.MANAGE_WANTED / monitor.CLOSE_WANTED, spelled out: the file also imports on 62e0719 (to show it fails there)
MANAGE_WANTED = "manage_wanted"
CLOSE_WANTED = "close_wanted"
APPLIED = "daha önce uygulanamayan değişiklik şimdi uygulandı"


# --------------------------------------------------------------------------- #
# the replica (as the referee's rig: a database file survives a restart, a broker's connect can be slow)
# --------------------------------------------------------------------------- #
async def make(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, db_path: str = ":memory:",
               binance: Any = None, terminal: FxTerminal | None = None, tickmill: Tickmill | None = None,
               slow_connect: dict[str, float] | None = None) -> Prod:
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
    for name, sec in (slow_connect or {}).items():
        broker = p.brokers[name]
        original = broker.connect

        async def slow(_orig: Callable[[], Any] = original, _sec: float = sec) -> None:
            await asyncio.sleep(_sec)
            await _orig()
        broker.connect = slow           # type: ignore[method-assign]
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
    p.notifier.notifications.clear()
    return p


class Outage:
    """The friend's MetaApi stops answering: every request times out at once (the SDK's TimeoutException), nothing
    reaches the account. ``on`` / ``off`` switch it."""

    def __init__(self, conn: Tickmill) -> None:
        self.conn, self.down = conn, False
        original = conn._rec

        def rec(name: str, *args: Any, **kwargs: Any) -> None:
            if self.down:
                conn.calls.append((f"DOWN:{name}", args, kwargs))
                raise TimeoutException(f"MetaApi websocket client request of type {name} timed out")
            original(name, *args, **kwargs)
        conn._rec = rec                 # type: ignore[method-assign]

    def on(self) -> None:
        self.down = True

    def off(self) -> None:
        self.down = False


def be(trade_id: int) -> ParsedAction:
    return ParsedAction(action=ActionType.MOVE_SL, move_sl_to_entry=True, base_asset="EUR", quote_asset="USD",
                        side=Side.LONG, target_trade_id=trade_id, confidence=0.95, summary_tr="EUR stop girişe")


def stop_at(trade_id: int, level: float) -> ParsedAction:
    return ParsedAction(action=ActionType.MOVE_SL, new_stop_loss=level, base_asset="EUR", quote_asset="USD",
                        side=Side.LONG, target_trade_id=trade_id, confidence=0.95, summary_tr=f"EUR stop {level}")


def close_part(trade_id: int, fraction: float) -> ParsedAction:
    return ParsedAction(action=ActionType.CLOSE, close_fraction=fraction, base_asset="EUR", quote_asset="USD",
                        side=Side.LONG, target_trade_id=trade_id, confidence=0.95, summary_tr="EUR yarısını kapat")


def close_all(trade_id: int) -> ParsedAction:
    return ParsedAction(action=ActionType.CLOSE, close_fraction=1.0, base_asset="EUR", quote_asset="USD",
                        side=Side.LONG, target_trade_id=trade_id, confidence=0.95, summary_tr="EUR kapat")


def calls(p: Prod, name: str, since: int = 0) -> list[tuple[str, tuple[Any, ...], dict[str, Any]]]:
    return [c for c in p.tickmill.calls[since:] if c[0] == name]


async def until(cond: Callable[[], bool], timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not cond():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.05)


async def eur_long(p: Prod) -> tuple[int, int]:
    """EURUSD long 1R at the market on fx and tickmill; (signal message id, trade id)."""
    set_eurusd(p, 1.1619, 1.1621)
    sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
    (trade,) = await p.trades()
    pos = await p.positions(trade.id)
    assert {b: x.status for b, x in pos.items()} == {"fx": PositionStatus.OPEN, "tickmill": PositionStatus.OPEN}
    p.notifier.notifications.clear()
    return sig, trade.id


# =========================================================================== #
# R1: changes that could not be applied are owed to the position and applied later
# (each test checks the venue first, then what the owner was told: the first failure on the trees before the fix is
# the money one)
# =========================================================================== #
async def test_r1_restart_stop_to_entry_caught_up_while_tickmill_connects_is_applied_on_its_connect(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The referee's S1: after a restart tickmill connects in the background (3 s here; MetaApi takes several seconds)
    and the held catch-up message "stop girişe" is processed right after start(). Round 1 (565bb89): fx goes to break
    even, tickmill keeps the full-risk stop 1.16 for good ('broker bağlı değil; değişiklik uygulanamadı'). Now the
    change is owed to tickmill's position and applied as soon as tickmill is connected - without any monitor pass.
    (62e0719 waited for tickmill's connect in start() and had no such window.)"""
    db_file = str(tmp_path / "bot.sqlite")
    p1 = await make(tmp_path, monkeypatch, db_path=db_file)
    sig, trade_id = await eur_long(p1)
    await p1.engine.stop(grace_sec=1.0)
    await p1.db.close()

    p2 = await make(tmp_path, monkeypatch, db_path=db_file, binance=p1.binance, terminal=p1.terminal,
                    tickmill=p1.tickmill, slow_connect={"tickmill": 3.0})
    try:
        connected_at_start = [b.name for b in p2.engine.brokers]
        p2._mid = p1._mid
        before = len(p1.tickmill.calls)
        set_eurusd(p2, 1.1640, 1.1642)
        await p2.say(be(trade_id), FX, "stop girişe", reply_to=sig)
        at_once = await p2.positions(trade_id)
        told_at_once = list(p2.texts())
        # tickmill's connect ends (3 s after the start): the stop goes to ITS entry price, nothing else is needed
        await until(lambda: bool(calls(p1, "modify_position", before)), 10.0, "the owed stop move on tickmill")
        await until(lambda: any(APPLIED in t for t in p2.texts()), 5.0, "the notice that it was applied")
        pos = await p2.positions(trade_id)
        assert pos["tickmill"].stop_loss == pytest.approx(1.1621) and MANAGE_WANTED not in pos["tickmill"].data
        modifies = calls(p1, "modify_position", before)
        assert len(modifies) == 1 and modifies[0][2]["stop_loss"] == pytest.approx(1.1621)

        assert connected_at_start == ["binance", "breakout", "fx"]                  # tickmill was still connecting
        assert at_once["fx"].stop_loss == pytest.approx(1.1621)                     # the owner's account: at once
        assert at_once["tickmill"].stop_loss == pytest.approx(1.16)
        wish = at_once["tickmill"].data[MANAGE_WANTED]
        assert wish["sl"]["to_entry"] is True and wish["attempts"] == 0             # (not connected: no failed try)
        (report,) = [t for t in told_at_once if "#1 EUR LONG: uygulandı" in t]
        # (Review 3, E5: the headline says why it is owed - here tickmill is not connected yet)
        assert report.startswith("⏳ #1 EUR LONG: uygulandı; tickmill bağlanınca ona da uygulanacak"), report
        assert ("• tickmill: broker bağlı değil; değişiklik şimdi uygulanamadı. tickmill bağlanınca bot uygulayacak "
                "ve haber verecek") in report
        assert [t for t in told_at_once if t.startswith("#1 tickmill")] == [
            "#1 tickmill: broker bağlı değil; değişiklik şimdi uygulanamadı. tickmill bağlanınca bot uygulayacak ve "
            "haber verecek"]
        texts = p2.texts()
        i_connected = next(i for i, t in enumerate(texts) if t.startswith("✅ <b>tickmill</b> bağlandı"))
        (applied,) = [t for t in texts if APPLIED in t]
        assert applied == f"✅ #1 EUR LONG tickmill: {APPLIED} — stop 1.1621.", applied
        assert texts.index(applied) > i_connected                                    # told after the connect notice
        await p2.engine.monitor.run_once()                                           # a later pass sends nothing
        assert len(calls(p1, "modify_position", before)) == 1
        assert sum(APPLIED in t for t in p2.texts()) == 1
    finally:
        await p2.engine.stop(grace_sec=1.0)
        await p2.db.close()


async def test_r1_stop_to_entry_during_a_metaapi_outage_is_applied_after_the_heartbeat_recovers(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """tickmill's MetaApi stops answering (live: 14:54, 17:54, 18:33 UTC, about a minute each). The heartbeat flags
    it, the stop-to-entry is refused at once by the adapter (A4: 'Bot bu işlem için hesaba emir göndermedi'). Before:
    never applied later. Now: owed, nothing is sent while the link is down, applied by the first monitor pass after the
    account answers again."""
    p = await make(tmp_path, monkeypatch)
    outage = Outage(p.tickmill)
    try:
        sig, trade_id = await eur_long(p)
        outage.on()
        for _ in range(3):
            await p.engine.monitor.heartbeat.check_once()
        flagged = "tickmill" in p.engine.monitor.heartbeat.problems()
        set_eurusd(p, 1.1640, 1.1642)
        await p.say(be(trade_id), FX, "stop girişe", reply_to=sig)
        during = await p.positions(trade_id)
        told_during = list(p.texts())
        await p.engine.monitor.run_once()                   # still down: its sync fails, nothing is sent
        sent_while_down = calls(p, "modify_position")
        outage.off()
        await p.engine.monitor.heartbeat.check_once()
        recovered = "tickmill" not in p.engine.monitor.heartbeat.problems()
        await p.engine.monitor.run_once()
        pos = await p.positions(trade_id)
        assert pos["tickmill"].stop_loss == pytest.approx(1.1621)                   # the friend's copy at break even
        (modify,) = calls(p, "modify_position")
        assert modify[2]["stop_loss"] == pytest.approx(1.1621)

        assert flagged and recovered and sent_while_down == []
        assert any("🔌 <b>tickmill</b>" in t for t in told_during)
        assert during["fx"].stop_loss == pytest.approx(1.1621) and during["tickmill"].stop_loss == pytest.approx(1.16)
        assert during["tickmill"].data[MANAGE_WANTED]["sl"]["to_entry"] is True
        (told,) = [t for t in told_during if t.startswith("#1 tickmill:")]
        assert "Bot bu işlem için hesaba emir göndermedi" in told                     # (the adapter's refusal)
        # (Review 3, E5: the wording says why it is owed - here the account did not answer)
        assert told.endswith("tickmill hesabından yanıt alınamadı, değişiklik şimdi doğrulanamadı. Bot yeniden "
                             "deneyecek ve uygulayınca haber verecek"), told
        (report,) = [t for t in told_during if "#1 EUR LONG: uygulandı" in t]
        assert report.startswith("⏳ #1 EUR LONG: uygulandı; tickmill hesabından yanıt alınamadı; bot yeniden "
                                 "deneyecek")
        assert MANAGE_WANTED not in pos["tickmill"].data
        assert [t for t in p.texts() if APPLIED in t] == [f"✅ #1 EUR LONG tickmill: {APPLIED} — stop 1.1621."]
        await p.engine.monitor.run_once()
        assert len(calls(p, "modify_position")) == 1                                 # never again
    finally:
        outage.off()
        await p.close()


async def test_r1_partial_close_while_tickmill_is_down_closes_its_half_once_after_it_answers(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """'yarısını kapattık' during the outage: fx closes its half at once; tickmill owes the size that is to stay open
    (half of 0.21 lots). After the outage exactly that is closed (the volume step rounds it to 0.1), once."""
    p = await make(tmp_path, monkeypatch)
    outage = Outage(p.tickmill)
    try:
        sig, trade_id = await eur_long(p)
        tm_lots = (await p.positions(trade_id))["tickmill"].remaining_quantity
        outage.on()
        await p.say(close_part(trade_id, 0.5), FX, "yarısını kapattık", reply_to=sig)
        during = await p.positions(trade_id)
        sent_while_down = calls(p, "close_position_partially")
        outage.off()
        await p.engine.monitor.run_once()
        partials = calls(p, "close_position_partially")
        assert [c[1][1] for c in partials] == [pytest.approx(0.1)]                   # floor(0.105) at the 0.01 step
        pos = await p.positions(trade_id)
        assert pos["tickmill"].remaining_quantity == pytest.approx(tm_lots - 0.1)

        assert sent_while_down == []
        assert during["fx"].remaining_quantity < during["fx"].filled_quantity        # fx: its half at once
        assert during["tickmill"].remaining_quantity == pytest.approx(tm_lots)
        assert during["tickmill"].data[MANAGE_WANTED]["reduce"]["to"] == pytest.approx(tm_lots / 2)
        assert MANAGE_WANTED not in pos["tickmill"].data
        assert [t for t in p.texts() if APPLIED in t] == [
            f"✅ #1 EUR LONG tickmill: {APPLIED} — 0.1 kapatıldı (kalan {round(tm_lots - 0.1, 2):g})."]
        for _ in range(2):
            await p.engine.monitor.run_once()
        assert len(calls(p, "close_position_partially")) == 1                        # never a second part
    finally:
        outage.off()
        await p.close()


async def test_r1_partial_close_that_went_through_unanswered_is_never_sent_again(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The partial close executes at the account but its answer is lost. The position owes the size that is to stay
    open; the next venue read shows it is there already: nothing is sent again, the owner is told it is done."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, trade_id = await eur_long(p)
        tm_lots = (await p.positions(trade_id))["tickmill"].remaining_quantity
        original = p.tickmill.close_position_partially

        async def lossy(position_id: str, volume: float, options: Any = None) -> Any:
            await original(position_id, volume, options)        # executed at the account ...
            raise TimeoutException("MetaApi websocket client request of type trade timed out")    # ... answer lost
        p.tickmill.close_position_partially = lossy             # type: ignore[method-assign]
        await p.say(close_part(trade_id, 0.5), FX, "yarısını kapattık", reply_to=sig)
        owed = MANAGE_WANTED in (await p.positions(trade_id))["tickmill"].data
        # (review 5: a lost partial close on fx / tickmill is held about a minute before its retry, see
        # tests/test_review5_engine.py: the bot's clock is moved past that hold)
        from datetime import timedelta
        _clk = p.engine._clock
        p.engine._clock = p.engine.monitor._clock = lambda: _clk() + timedelta(seconds=61)
        await p.engine.monitor.run_once()
        await p.engine.monitor.run_once()
        assert len(calls(p, "close_position_partially")) == 1                        # the venue read: done already
        pos = await p.positions(trade_id)
        assert pos["tickmill"].remaining_quantity == pytest.approx(tm_lots - 0.1)
        assert owed and MANAGE_WANTED not in pos["tickmill"].data
        assert [t for t in p.texts() if APPLIED in t] == [
            f"✅ #1 EUR LONG tickmill: {APPLIED} — kısmi kapatma tamam (kalan {round(tm_lots - 0.1, 2):g})."]
    finally:
        await p.close()


async def test_r1_the_latest_instruction_wins_and_a_close_replaces_what_is_owed(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Two stop moves during the outage: only the latest one (stop to entry) is applied afterwards, never the older
    1.1610. Then "kapattık" while it is still down: the close is owed instead and nothing else is sent."""
    p = await make(tmp_path, monkeypatch)
    outage = Outage(p.tickmill)
    try:
        sig, trade_id = await eur_long(p)
        outage.on()
        await p.say(stop_at(trade_id, 1.1610), FX, "stop 1.1610", reply_to=sig)
        set_eurusd(p, 1.1640, 1.1642)
        await p.say(be(trade_id), FX, "stop girişe", reply_to=sig)
        owed_sl = (await p.positions(trade_id))["tickmill"].data.get(MANAGE_WANTED, {}).get("sl", {})
        outage.off()
        await p.engine.monitor.run_once()
        assert [c[2]["stop_loss"] for c in calls(p, "modify_position")] == [pytest.approx(1.1621)]
        assert owed_sl.get("to_entry") is True              # (the latest: its own entry price, not 1.1610)

        outage.on()
        await p.say(stop_at(trade_id, 1.1630), FX, "stop 1.1630", reply_to=sig)
        owed_after_move = MANAGE_WANTED in (await p.positions(trade_id))["tickmill"].data
        await p.say(close_all(trade_id), FX, "kapattık", reply_to=sig)
        pos = (await p.positions(trade_id))["tickmill"]
        outage.off()
        await p.engine.monitor.run_once()
        assert (await p.positions(trade_id))["tickmill"].status is PositionStatus.CLOSED
        assert len(calls(p, "modify_position")) == 1                                  # 1.1630 never sent
        assert owed_after_move and CLOSE_WANTED in pos.data and MANAGE_WANTED not in pos.data   # replaced by the close
    finally:
        outage.off()
        await p.close()


async def test_r1_an_owed_change_is_dropped_with_one_notice_when_the_position_ends_first(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = await make(tmp_path, monkeypatch)
    outage = Outage(p.tickmill)
    try:
        sig, trade_id = await eur_long(p)
        outage.on()
        set_eurusd(p, 1.1640, 1.1642)
        await p.say(be(trade_id), FX, "stop girişe", reply_to=sig)
        (pid,) = p.tickmill.positions
        p.tickmill.hit(pid, "DEAL_REASON_SL", 1.16)                   # its old stop is hit during the outage
        outage.off()
        await p.engine.monitor.run_once()
        pos = (await p.positions(trade_id))["tickmill"]
        assert pos.status is PositionStatus.CLOSED and calls(p, "modify_position") == []
        (dropped,) = [t for t in p.texts() if "uygulanamadan" in t]
        assert dropped.startswith("ℹ️ #1 EUR LONG tickmill: bekleyen değişiklik (") and dropped.endswith(
            ") uygulanamadan pozisyon kapandı."), dropped
        assert MANAGE_WANTED not in pos.data and not any(APPLIED in t for t in p.texts())
    finally:
        outage.off()
        await p.close()


async def test_r1_the_owners_own_account_gets_a_stop_move_it_refused_once_on_the_next_pass(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Not only the friend's account: fx (the owner's prop account) refuses the stop-to-entry once (MT5 10016,
    'Invalid stops'). Before: never tried again - the owner's position kept the full-risk stop. Now the next venue
    sync applies it and the owner is told."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, trade_id = await eur_long(p)
        set_eurusd(p, 1.1640, 1.1642)
        p.terminal.sltp_fail = 1
        await p.say(be(trade_id), FX, "stop girişe", reply_to=sig)
        during = await p.positions(trade_id)
        told_during = list(p.texts())
        await p.engine.monitor.run_once()
        pos = await p.positions(trade_id)
        assert pos["fx"].stop_loss == pytest.approx(1.1621)                          # the owner's stop at break even
        assert during["fx"].stop_loss == pytest.approx(1.16) and during["tickmill"].stop_loss == pytest.approx(1.1621)
        assert during["fx"].data[MANAGE_WANTED]["sl"]["to_entry"] is True
        (report,) = [t for t in told_during if "#1 EUR LONG: uygulandı" in t]
        # (Review 3, E5: fx answered with a refusal ('Invalid stops'): "reddetti", not "yanıt verince")
        assert report.startswith("⏳ #1 EUR LONG: uygulandı; fx reddetti; bot sonraki kontrollerde yeniden "
                                 "deneyecek"), report
        assert MANAGE_WANTED not in pos["fx"].data
        assert [t for t in p.texts() if APPLIED in t] == [f"✅ #1 EUR LONG fx: {APPLIED} — stop 1.1621."]
    finally:
        await p.close()


async def test_r1_healthy_stop_to_entry_is_applied_at_once_and_reported_as_before(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Both accounts answer: nothing is owed, nothing extra is told, the report reads as on 62e0719 (guard: passes
    there too)."""
    p = await make(tmp_path, monkeypatch)
    try:
        sig, trade_id = await eur_long(p)
        set_eurusd(p, 1.1640, 1.1642)
        await p.say(be(trade_id), FX, "stop girişe", reply_to=sig)
        pos = await p.positions(trade_id)
        assert pos["fx"].stop_loss == pytest.approx(1.1621) and pos["tickmill"].stop_loss == pytest.approx(1.1621)
        assert all(MANAGE_WANTED not in x.data for x in pos.values())
        (report,) = p.texts()
        assert report.startswith("✅ #1 EUR LONG: uygulandı\n"), report
        assert report.endswith("• tickmill: stop 1.1621\n• fx: stop 1.1621"), report
        await p.engine.monitor.run_once()
        assert len(p.texts()) == 1 and len(calls(p, "modify_position")) == 1
    finally:
        await p.close()


# =========================================================================== #
# R2: a change made while tickmill's open was still unanswered reaches the position it opens
# =========================================================================== #
def slow_tickmill_buy(p: Prod, delay: float, *, lost: bool = False) -> None:
    """tickmill answers its market buy only after ``delay`` s (beyond the 3 s grace); ``lost``: and then not at all
    (the SDK's TimeoutException: the order never reached the account)."""
    real = p.tickmill.create_market_buy_order

    async def slow(*args: Any, **kwargs: Any) -> Any:
        await asyncio.sleep(delay)
        if lost:
            raise TimeoutException("MetaApi websocket client request of type trade timed out")
        return await real(*args, **kwargs)
    p.tickmill.create_market_buy_order = slow       # type: ignore[method-assign]


async def test_r2_stop_move_made_while_tickmills_open_was_unanswered_is_applied_when_it_answers(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The referee's S3: tickmill's market buy answers after 4 s; 'stop 1.1610' comes 0.2 s after the signal's flow
    returned (3 s grace). Round 1: fx at 1.161, tickmill kept 1.16 ('UYGULANMADI ... elle kontrol edin'). Now the
    position that late answer brings gets the trade's stop at once."""
    p = await make(tmp_path, monkeypatch)
    try:
        set_eurusd(p, 1.1619, 1.1621)
        slow_tickmill_buy(p, 4.0)
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        flow_sec = loop.time() - t0
        (trade,) = await p.trades()
        await asyncio.sleep(0.2)
        await p.say(stop_at(trade.id, 1.1610), FX, "stop 1.1610", reply_to=sig)
        told_at_move = list(p.texts())
        await until(lambda: any("— tickmill geç yanıt verdi" in t for t in p.texts()), 10.0, "tickmill's late answer")
        pos = await p.positions(trade.id)
        assert pos["tickmill"].stop_loss == pytest.approx(1.161) and pos["fx"].stop_loss == pytest.approx(1.161)
        (modify,) = calls(p, "modify_position")
        assert modify[2]["stop_loss"] == pytest.approx(1.161)

        assert flow_sec < 3.5                                       # (fx never waited for tickmill)
        (report,) = [t for t in told_at_move if "#1 EUR LONG: uygulandı" in t]
        assert report.startswith("⏳ #1 EUR LONG: uygulandı; tickmill açılış emrine yanıt verince ona da uygulanacak"), \
            report
        assert "• ⏳ tickmill: açılış emri henüz yanıtlanmadı; açılırsa bu değişiklik ona hemen uygulanacak" in report
        (late,) = [t for t in p.texts() if "— tickmill geç yanıt verdi" in t]
        assert ("• Bu emir yanıt beklerken işlemde yapılan değişiklikler bu pozisyona şimdi uygulandı: stop 1.161"
                in late), late
        assert "UYGULANMADI" not in late and "elle kontrol edin" not in late
        assert MANAGE_WANTED not in pos["tickmill"].data
    finally:
        await p.close()


async def test_r2_stop_to_entry_that_the_price_does_not_allow_yet_stays_owed_and_is_applied_later(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """'stop girişe' in the same window, but when tickmill's position arrives its price is below its own entry: a stop
    there would close it at once. It is not sent; the owner is told it stays owed; a monitor pass applies it once the
    price allows."""
    p = await make(tmp_path, monkeypatch)
    try:
        set_eurusd(p, 1.1619, 1.1621)
        slow_tickmill_buy(p, 4.0)
        sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        (trade,) = await p.trades()
        set_eurusd(p, 1.1640, 1.1642)
        await p.say(be(trade.id), FX, "stop girişe", reply_to=sig)
        set_eurusd(p, 1.1615, 1.1617)                              # back below the entry when tickmill answers
        await until(lambda: any("— tickmill geç yanıt verdi" in t for t in p.texts()), 10.0, "tickmill's late answer")
        sent_then = calls(p, "modify_position")
        late_pos = (await p.positions(trade.id))["tickmill"]
        set_eurusd(p, 1.1640, 1.1642)
        await p.engine.monitor.run_once()
        pos = (await p.positions(trade.id))["tickmill"]
        assert late_pos.avg_entry_price == pytest.approx(1.1617)                    # (filled when it answered)
        assert pos.stop_loss == pytest.approx(1.1617)                                # its own entry, once allowed
        assert sent_then == [] and len(calls(p, "modify_position")) == 1
        assert late_pos.stop_loss == pytest.approx(1.16) and late_pos.data[MANAGE_WANTED]["sl"]["to_entry"] is True
        (late,) = [t for t in p.texts() if "— tickmill geç yanıt verdi" in t]
        assert "bu pozisyona şimdi uygulanamadı (" in late and "yanlış tarafında" in late, late
        # (Review 3, E5: the catch-up's failure may be a refusal or a price that does not allow it yet - here the
        # stop is on the wrong side - so the wording no longer says "yanıt verince")
        assert "Bot sonraki kontrollerde tickmill hesabında yeniden deneyecek ve uygulayınca haber verecek." in late
        assert [t for t in p.texts() if APPLIED in t] == [f"✅ #1 EUR LONG tickmill: {APPLIED} — stop 1.1617."]
    finally:
        await p.close()


async def test_r2_a_catch_up_hanging_on_tickmill_never_holds_the_owners_next_instruction(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The owner's hard requirement (1) for R2 itself: the catch-up of the late position runs outside the trade lock
    (before the fix there was no catch-up at all: tickmill never got the missed stop). A variant that applied it under
    the trade lock held fx's next stop for the whole 6 s hang (measured: 6.0 s, this assertion fails). The catch-up's
    stop move hangs 6 s at tickmill; the trader's next stop move reaches fx at once (only the 2 s sync wait before
    managing applies), and tickmill ends with the latest stop."""
    p = await make(tmp_path, monkeypatch)
    try:
        set_eurusd(p, 1.1619, 1.1621)
        slow_tickmill_buy(p, 4.0)
        real_modify = p.tickmill.modify_position
        hung = {"n": 0}

        async def first_modify_hangs(*args: Any, **kwargs: Any) -> Any:
            hung["n"] += 1
            if hung["n"] == 1:
                await asyncio.sleep(6.0)
            return await real_modify(*args, **kwargs)
        p.tickmill.modify_position = first_modify_hangs          # type: ignore[method-assign]
        loop = asyncio.get_running_loop()
        t_signal = loop.time()
        sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        (trade,) = await p.trades()
        await p.say(stop_at(trade.id, 1.1610), FX, "stop 1.1610", reply_to=sig)
        await until(lambda: hung["n"] == 1, 10.0, "the catch-up's stop move on tickmill")       # ... now hanging
        t0 = loop.time()
        await p.say(stop_at(trade.id, 1.1612), FX, "stop 1.1612", reply_to=sig)
        took = loop.time() - t0
        fx_now = (await p.positions(trade.id))["fx"].stop_loss
        hang_left = 6.0 - (loop.time() - t_signal - 4.0)
        await until(lambda: (hung["n"] >= 2), 15.0, "tickmill's later stop move")
        await asyncio.sleep(0.5)
        pos = await p.positions(trade.id)
        print(f"\nfx got the stop {took:.2f}s after the post; tickmill hang left {hang_left:.2f}s")
        # (measured: 2.0 s - the 2 s sync wait before managing; with the catch-up under the trade lock: 6.0 s)
        assert fx_now == pytest.approx(1.1612) and took < 4.0, (fx_now, took)
        assert hang_left > 0.5, hang_left                         # (fx got it while tickmill's call still hung)
        assert pos["tickmill"].stop_loss == pytest.approx(1.1612)  # the latest instruction, applied last
    finally:
        await p.close()


# =========================================================================== #
# R3: an order that was sent and got no answer is never "açılmadı"
# =========================================================================== #
UNKNOWN = "emir gönderildi ama sonucu alınamadı; {} hesabında açılıp açılmadığı bilinmiyor, kontrol edin"


def lose_fx_entries(terminal: FxTerminal) -> None:
    """The fx terminal loses the answer of every entry order (IPC failure); nothing reaches the trade server."""
    real = terminal.order_send

    def send(request: dict[str, Any]) -> Any:
        if request.get("action") == MT5.TRADE_ACTION_DEAL and "position" not in request:
            terminal.calls.append(("order_send_lost", (dict(request),), {}))
            terminal.last = (-10001, "IPC send failed")
            return None
        return real(request)
    terminal.order_send = send          # type: ignore[method-assign]


def test_r3_outcome_unknown_reads_the_error_type_its_cause_and_the_metatrader_wording() -> None:
    from copytrader.brokers.base import BrokerError, OrderOutcomeUnknownError, outcome_unknown
    from copytrader.brokers.mt5_broker import OrderReplyLostError

    assert issubclass(OrderReplyLostError, OrderOutcomeUnknownError) and issubclass(OrderOutcomeUnknownError,
                                                                                     BrokerError)
    assert outcome_unknown(OrderReplyLostError("MT5 emir yanıtı alınamadı"))
    wrapped = BrokerError("tickmill: emir")
    wrapped.__cause__ = OrderReplyLostError("x")
    assert outcome_unknown(wrapped)
    assert outcome_unknown(BrokerError("Emir durumu belirsiz: MT5 emir yanıtı alınamadı (IPC send failed)."))
    assert outcome_unknown(BrokerError("2/3. bacak durumu belirsiz: MetaApi piyasa emri başarısız"))
    assert not outcome_unknown(BrokerError("MetaApi emri reddetti (geçersiz alan: volume); bu hesapta işlem açılmadı."))
    assert not outcome_unknown(BrokerError("Emir açılamadı: yetersiz bakiye"))
    assert not outcome_unknown(None)


async def test_r3_open_whose_only_order_answer_was_lost_is_not_reported_as_not_opened(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """fx's EURJPY order is sent and its answer lost (tickmill does not list EURJPY). Before: '🚨 #1 EUR SHORT
    hiçbir brokerda açılamadı' over an adapter text saying the order may have been sent."""
    p = await make(tmp_path, monkeypatch)
    try:
        lose_fx_entries(p.terminal)
        await p.say(fx_pair(*EURJPY_SHORT, entry_type=EntryType.MARKET), FX, "EURJPY SHORT 1R")
        (trade,) = await p.trades()
        assert trade.status is TradeStatus.FAILED
        assert len([c for c in p.terminal.calls if c[0] == "order_send_lost"]) == 1      # sent once, never again
        (head,) = [t for t in p.texts(Severity.ERROR) if t.startswith("🚨 <b>#1 EUR SHORT")]
        assert head.startswith(f"🚨 <b>#1 EUR SHORT: {UNKNOWN.format('fx')}</b>\n"), head
        assert "hiçbir brokerda açılamadı" not in head and "durumu belirsiz" in head
    finally:
        await p.close()


async def test_r3_definite_refusal_keeps_hicbir_brokerda_acilamadi(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Guard (passes on 62e0719 too): a refused order (no money) is still 'açılamadı'."""
    p = await make(tmp_path, monkeypatch)
    try:
        p.terminal.hooks.append(lambda r: 10019 if r["action"] == MT5.TRADE_ACTION_DEAL and "position" not in r
                                else None)
        await p.say(fx_pair(*EURJPY_SHORT, entry_type=EntryType.MARKET), FX, "EURJPY SHORT 1R")
        (head,) = [t for t in p.texts(Severity.ERROR) if t.startswith("🚨 <b>#1 EUR SHORT")]
        assert head.startswith("🚨 <b>#1 EUR SHORT hiçbir brokerda açılamadı</b>"), head
    finally:
        await p.close()


async def test_r3_partly_opened_trade_names_the_account_whose_outcome_is_unknown(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = await make(tmp_path, monkeypatch)
    try:
        set_eurusd(p, 1.1619, 1.1621)
        lose_fx_entries(p.terminal)
        await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        assert (await p.positions(1))["tickmill"].status is PositionStatus.OPEN
        (head,) = [t for t in p.texts() if t.startswith("⚠️ <b>#1 EUR LONG")]
        first = head.split("\n", 1)[0]
        assert first == f"⚠️ <b>#1 EUR LONG kısmen açıldı</b> — {UNKNOWN.format('fx')}", first
        assert "açılmadı (nedeni aşağıda)" not in first
    finally:
        await p.close()


async def test_r3_virtual_fire_whose_answer_was_lost_is_not_reported_as_not_opened(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """not_done item 2: tickmill's virtual entry fires, the market order gets no answer and is not on the account.
    Before: '🧭 ... tetiklendi (...) ama işlem açılmadı, sanal giriş iptal edildi' over 'Emir GÖNDERİLMİŞ OLABİLİR'."""
    monkeypatch.setattr(mab, "LOST_REPLY_DELAY_SEC", 0.0, raising=False)
    p = await make(tmp_path, monkeypatch)
    try:
        await p.say(btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0), KRIPTO, "btc 1R long limit emir")
        (trade,) = await p.trades()
        p.notifier.notifications.clear()
        p.tickmill.fail["create_market_buy_order"] = [TimeoutException(
            "MetaApi websocket client request of type trade timed out")]
        p.binance.set_price("BTC", 72989.0, 72991.0)
        p.tickmill.set_price("BTCUSD", 72939.0, 72941.0)
        await p.engine.monitor.run_virtual_once()
        await p.engine.monitor.run_virtual_once()
        assert len([c for c in p.tickmill.calls if c[0] == "create_market_buy_order"]) == 1   # never sent twice
        assert p.tickmill.positions == {}
        (end,) = [t for t in p.texts() if t.startswith("🧭 #1 BTC LONG tickmill:")]
        assert end.startswith(f"🧭 #1 BTC LONG tickmill: tetiklendi (Binance 72990); {UNKNOWN.format('tickmill')}."), end
        assert "ama işlem açılmadı" not in end and "GÖNDERİLMİŞ OLABİLİR" in end
        assert (await p.positions(trade.id))["tickmill"].status is PositionStatus.FAILED
    finally:
        await p.close()


async def test_r3_late_answer_that_lost_the_order_is_not_reported_as_not_opened(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """fx refuses (no money); tickmill answers only after the grace, and its answer is that the order's reply was lost.
    Before: the late notice ended with '• Bu işlem hiçbir brokerda açılamadı.'"""
    monkeypatch.setattr(mab, "LOST_REPLY_DELAY_SEC", 0.0, raising=False)
    p = await make(tmp_path, monkeypatch)
    try:
        set_eurusd(p, 1.1619, 1.1621)
        p.terminal.hooks.append(lambda r: 10019 if r["action"] == MT5.TRADE_ACTION_DEAL and "position" not in r
                                else None)
        slow_tickmill_buy(p, 3.5, lost=True)
        await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        await until(lambda: any("— tickmill geç yanıt verdi" in t for t in p.texts()), 10.0, "tickmill's late answer")
        (late,) = [t for t in p.texts() if "— tickmill geç yanıt verdi" in t]
        assert late.endswith(f"\n• {UNKNOWN.format('tickmill')[:1].upper()}{UNKNOWN.format('tickmill')[1:]}."), late
        assert "hiçbir brokerda açılamadı" not in late
        assert (await p.trades())[0].status is TradeStatus.FAILED
    finally:
        await p.close()


# =========================================================================== #
# R4: one look at the connected brokers decides both the skip notice and who takes the trade
# =========================================================================== #
async def test_r4_broker_that_connects_during_the_skip_notice_does_not_take_the_trade_it_was_told_it_skips(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = await make(tmp_path, monkeypatch)
    eng = p.engine
    try:
        eng.brokers = [b for b in eng.brokers if b.name != "tickmill"]          # still connecting in the background
        eng.optional_down["tickmill"] = orch._CONNECTING
        real_notify = p.notifier.notify

        async def notify(text: str, severity: Severity = Severity.INFO) -> None:
            await real_notify(text, severity)
            if "tickmill bağlı değil, bu işlem atlandı" in text and "tickmill" in eng.optional_down:
                # ... and its connect ends right now (what Engine._connect_later does when it succeeds)
                eng.optional_down.pop("tickmill", None)
                up = {b.name for b in eng.brokers} | {"tickmill"}
                eng.brokers = [b for b in eng.configured_brokers if b.name in up]
        p.notifier.notify = notify                  # type: ignore[method-assign]
        set_eurusd(p, 1.1619, 1.1621)
        await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        pos = await p.positions(1)
        assert set(pos) == {"fx"} and p.tickmill_orders() == []                 # as the owner was told
        assert sum("tickmill bağlı değil, bu işlem atlandı" in t for t in p.texts()) == 1
        assert [b.name for b in eng.brokers] == ["binance", "breakout", "fx", "tickmill"]   # (the next one: taken)
    finally:
        await p.close()


# =========================================================================== #
# R5: the management headline says what really happened
# =========================================================================== #
async def test_r5_close_while_tickmills_open_is_unanswered_says_it_will_be_closed_on_arrival(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """'kapattık' while tickmill's open is still unanswered: fx is closed at once and tickmill's position will be closed
    when it arrives. Before: '✅ #1 EUR LONG: uygulandı' over the line saying tickmill had not answered yet."""
    p = await make(tmp_path, monkeypatch)
    try:
        set_eurusd(p, 1.1619, 1.1621)
        slow_tickmill_buy(p, 4.0)
        sig = await p.say(fx_pair(*EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
        (trade,) = await p.trades()
        await p.say(close_all(trade.id), FX, "kapattık", reply_to=sig)
        (report,) = [t for t in p.texts() if "#1 EUR LONG: uygulandı" in t]
        assert report.startswith("⏳ #1 EUR LONG: uygulandı; tickmill açılırsa hemen kapatılacak\n"), report
        assert "• ⏳ tickmill: açılış emri henüz yanıtlanmadı; açılırsa hemen kapatılacak" in report
        await until(lambda: any("— tickmill geç yanıt verdi" in t for t in p.texts()), 10.0, "tickmill's late answer")
        assert {b: x.status for b, x in (await p.positions(trade.id)).items()} == {
            "fx": PositionStatus.CLOSED, "tickmill": PositionStatus.CLOSED}
    finally:
        await p.close()
