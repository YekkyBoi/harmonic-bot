"""Review 1, glue lane (engine side): follow-up items of the fix lanes for the engine, the monitor, the ccxt and
manual adapters and the owner's texts.

  G3  the friend's OPTIONAL broker (tickmill) never holds the trade lock while its virtual trigger sends its market
      order: a hanging MetaApi at that moment no longer delays the management of the owner's legs of the trade;
      what the trader said meanwhile is applied to the fired position as soon as it answers
  G11 the management report shows the stop / the targets before -> after (not the new value twice)
  G6  an open report says "kısmen açıldı" when a NON-optional broker of the route left the trade out (one message)
  G7  the PENDING header says what was really placed (resting limit orders, a virtual trigger)
  G9  a cancel that found the entry filled and closed it says so
  G5  "bu hesapta işlem görmüyor" names the pair, not only its base currency
  G8  the breakout instruction and the open report show one risk figure for one position
  G4  the ccxt adapter's venue size read and its "flat_seen" counter are a public contract of the monitor

Most scenarios run on the production replica of tests/test_review1_engine_iso.py (REAL Engine, Monitor and
adapters on the fake venues, the server's config of today, a tickmill link that can be told not to answer).
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

import test_review1_engine_iso as iso
from test_preflight_scenarios import FX, KRIPTO, btc, fx_pair, set_eurusd
from test_mt5_broker import SERVER_TIME

from copytrader.brokers import metaapi_broker
from copytrader.brokers.base import BrokerError
from copytrader.brokers.mt5_broker import Mt5Broker
from copytrader.engine.monitor import is_virtual_pending
from copytrader.models import (
    ActionType,
    EntryType,
    ManagementPlan,
    PositionStatus,
    Severity,
    Side,
    TakeProfit,
    TradeStatus,
)
from copytrader.telegram import formatting as fmt


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Mt5Broker, "_now_epoch", staticmethod(lambda: SERVER_TIME - 7200.0))
    monkeypatch.setattr(Mt5Broker, "tick_wait_sec", 0.0)
    monkeypatch.setattr(fmt, "RISK_PER_R_NOTE", fmt.RISK_PER_R_NOTE)


# =========================================================================== #
# G3: tickmill's virtual trigger fires while MetaApi does not answer
# =========================================================================== #
HOLD_SEC = 8.0      # how long tickmill leaves its market order unanswered (production: up to RPC_TIMEOUT_SEC, 120 s)


async def _limit_with_a_waiting_virtual_entry(p: Any, tl: iso.Timeline) -> tuple[int, Any, str]:
    """kripto BTC LIMIT long 73000 with the price at 74000: binance's limit rests, breakout gets its instruction,
    tickmill waits on a virtual trigger. Returns (signal message id, trade, binance order id)."""
    sid = await iso.post(p, btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0), KRIPTO, "btc 1R long limit 73000",
                         tl=tl, label="S1")
    (trade,) = await p.trades()
    positions = await p.positions(trade.id)
    assert is_virtual_pending(positions["tickmill"]), positions
    assert positions["binance"].status is PositionStatus.PENDING
    return sid, trade, p.binance.created()[0]["id"]


async def _touch_and_hang(p: Any, tl: iso.Timeline, oid: str) -> float:
    """The price comes down to the entry: binance's limit fills and the reference touches 73000, so the monitor's
    fast check (the real loop) fires tickmill's virtual entry, whose market order is not answered. Returns the time
    of the fire."""
    tm: iso.HangingTickmill = p.tickmill
    p.engine._start_monitor()
    await asyncio.sleep(0.3)
    tm.hang_trades = True
    tl.t0 = time.monotonic()
    tl.add("test", "the price touches 73000; binance's limit fills")
    p.binance.set_price("BTC", 72989.0, 72991.0)
    p.tickmill.set_price("BTCUSD", 72939.0, 72941.0)
    p.binance.fill(oid)
    await iso.until(lambda: "create_market_buy_order" in tm.hung, 6.0, "tickmill's market order (the fire)", tl)
    return tl.now()


def _index(texts: list[str], needle: str) -> int:
    found = [i for i, t in enumerate(texts) if needle in t]
    assert found, (needle, texts)
    return found[0]


async def test_g3_kapattik_while_the_fire_hangs_closes_binance_at_once_and_the_fired_position_on_arrival(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Before the fix the fire ran under the trade lock: the trader's "kapattık" 1 s later waited for tickmill's
    answer (here HOLD_SEC; in production up to RPC_TIMEOUT_SEC = 120 s) before binance was closed."""
    tl = iso.Timeline()
    p = await iso.make_replica(tmp_path, monkeypatch, timeline=tl, interval=10.0)
    tm: iso.HangingTickmill = p.tickmill
    try:
        sid, trade, oid = await _limit_with_a_waiting_virtual_entry(p, tl)
        fired = await _touch_and_hang(p, tl, oid)
        await asyncio.sleep(1.0)
        close = asyncio.ensure_future(iso.post(p, iso.manage(ActionType.CLOSE, trade.id, close_fraction=1.0), KRIPTO,
                                               "kapattık", reply_to=sid, tl=tl, label="CLOSE"))
        await iso.until(lambda: tl.first("binance", "close market sell") is not None, HOLD_SEC - 2.0,
                        "binance's close while tickmill's market order hangs", tl)
        t_post, t_close = tl.first("post", "CLOSE:"), tl.first("binance", "close market sell")
        assert t_post is not None and t_close is not None and t_close - t_post < 3.0, tl.dump()
        await iso.until(lambda: close.done(), 5.0, "the kapattik batch", tl)
        before_answer = list(p.texts())
        # tickmill still has not answered: nothing of it has been reported as opened or closed
        assert not any("tickmill hesabında açıldı" in t for t in before_answer), before_answer
        (report,) = [t for t in before_answer if "#1 BTC LONG: uygulandı" in t]
        assert report.startswith("⏳ #1 BTC LONG: uygulandı; tickmill yanıtı bekleniyor"), report
        assert "binance: tamamı kapatıldı" in report and "breakout: tamamı kapatıldı" in report, report
        assert ("⏳ tickmill: sanal tetik az önce tetiklendi, açılış emrine yanıt bekleniyor; pozisyon açılırsa "
                "hemen kapatılacak, sonuç ayrıca bildirilecek") in report, report
        while tl.now() < fired + HOLD_SEC:
            await asyncio.sleep(0.05)
        answered = tl.add("test", "tickmill answers again")
        tm.release()
        await iso.until(lambda: any("— tickmill geç yanıt verdi" in t and "tamamı kapatıldı" in t for t in p.texts()),
                        10.0, "tickmill's close of the fired position", tl)
        await iso.settle(p, 10.0)
        after = next(t for t in await p.trades() if t.id == trade.id)
        positions = await p.positions(trade.id)
        texts = p.texts()
        calls = [name for name, _args, _kw in p.tickmill.calls if name.startswith(("create_", "close_"))]
    finally:
        tm.release()
        await p.close()
    print("\n" + tl.dump())
    # the fired order went out once, and its position was closed as soon as tickmill answered
    assert calls == ["create_market_buy_order", "close_position"], calls
    t_tm_close = tl.first("tickmill", "close_position", after=answered)
    assert t_tm_close is not None and t_tm_close - answered < 1.0, tl.dump()
    assert after.status is TradeStatus.CLOSED, after
    assert {b: x.status for b, x in positions.items()} == {
        "binance": PositionStatus.CLOSED, "breakout": PositionStatus.CLOSED, "tickmill": PositionStatus.CLOSED}
    # the owner hears it in order: the flow's report, the fire (with the instruction that is being applied), the
    # close of the fired position, and one closing summary with every account's result
    i_report = _index(texts, "#1 BTC LONG: uygulandı")
    i_fire = _index(texts, "tickmill hesabında açıldı")
    i_close = _index(texts, "— tickmill geç yanıt verdi")
    assert i_report < i_fire < i_close, texts
    assert ("⏳ Emir yanıt beklerken gelen talimat bu pozisyona şimdi uygulanıyor (pozisyonun tamamı kapatılacak); "
            "sonucu ayrıca bildirilecek.") in texts[i_fire], texts[i_fire]
    assert "• tickmill: tamamı kapatıldı" in texts[i_close] and "İşlem kapandı." in texts[i_close], texts[i_close]
    summaries = [t for t in texts if t.startswith("🏁 #1 BTC LONG")]
    assert len(summaries) == 1 and "tickmill" in summaries[0] and "binance" in summaries[0], summaries


async def test_g3_stop_moved_while_the_fire_hangs_reaches_binance_at_once_and_the_fired_position_on_arrival(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The trader moves the stop to 72600 while tickmill's market order hangs. Before the fix binance's stop moved
    only after tickmill's answer."""
    tl = iso.Timeline()
    p = await iso.make_replica(tmp_path, monkeypatch, timeline=tl, interval=10.0)
    tm: iso.HangingTickmill = p.tickmill
    try:
        sid, trade, oid = await _limit_with_a_waiting_virtual_entry(p, tl)
        fired = await _touch_and_hang(p, tl, oid)
        await iso.until(lambda: tl.first("binance", "stop market sell") is not None, 5.0, "binance's first stop", tl)
        first_stop = tl.first("binance", "stop market sell")
        await asyncio.sleep(1.0)
        move = asyncio.ensure_future(iso.post(p, iso.manage(ActionType.MOVE_SL, trade.id, new_stop_loss=72600.0),
                                              KRIPTO, "stop 72600", reply_to=sid, tl=tl, label="SL"))
        await iso.until(lambda: tl.first("binance", "stop market sell", after=first_stop) is not None,  # type: ignore[arg-type]
                        HOLD_SEC - 2.0, "binance's stop move while tickmill's market order hangs", tl)
        t_post = tl.first("post", "SL:")
        t_move = tl.first("binance", "stop market sell", after=first_stop)    # type: ignore[arg-type]
        assert t_post is not None and t_move is not None and t_move - t_post < 3.0, tl.dump()
        await iso.until(lambda: move.done(), 5.0, "the stop batch", tl)
        (report,) = [t for t in p.texts() if "#1 BTC LONG: uygulandı" in t]
        assert ("⏳ tickmill: sanal tetik az önce tetiklendi, açılış emrine yanıt bekleniyor; pozisyon açılırsa bu "
                "değişiklik ona hemen uygulanacak") in report, report
        while tl.now() < fired + HOLD_SEC:
            await asyncio.sleep(0.05)
        tm.release()
        await iso.until(lambda: any("— tickmill geç yanıt verdi" in t for t in p.texts()), 10.0,
                        "tickmill's stop move on the fired position", tl)
        await iso.settle(p, 10.0)
        positions = await p.positions(trade.id)
        texts = p.texts()
    finally:
        tm.release()
        await p.close()
    print("\n" + tl.dump())
    assert positions["binance"].stop_loss == pytest.approx(72600.0)
    # tickmill's levels carry the offset measured at the fire (72940 - 72990 = -50)
    assert positions["tickmill"].status is PositionStatus.OPEN
    assert positions["tickmill"].stop_loss == pytest.approx(72550.0), positions["tickmill"]
    (late,) = [t for t in texts if "— tickmill geç yanıt verdi" in t]
    assert "• tickmill: stop 72550" in late, late
    (fire,) = [t for t in texts if "tickmill hesabında açıldı" in t]
    assert "gelen talimat bu pozisyona şimdi uygulanıyor (stop 72600 seviyesine çekilecek)" in fire, fire


async def test_g3_fire_that_never_answers_still_lets_binance_close_and_the_owner_is_told_to_check_tickmill(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """tickmill never answers the fired order; MetaApi's own limit (RPC_TIMEOUT_SEC: production 120 s, 5 s here) ends
    the call. Before the fix binance's close waited for that limit. Now binance closes at once, and once the limit
    has passed the owner is told that a position may exist on tickmill and must then be closed by hand."""
    monkeypatch.setattr(metaapi_broker, "RPC_TIMEOUT_SEC", 5.0)
    tl = iso.Timeline()
    p = await iso.make_replica(tmp_path, monkeypatch, timeline=tl, interval=10.0)
    tm: iso.HangingTickmill = p.tickmill
    try:
        sid, trade, oid = await _limit_with_a_waiting_virtual_entry(p, tl)
        await _touch_and_hang(p, tl, oid)
        await asyncio.sleep(1.0)
        await asyncio.wait_for(iso.post(p, iso.manage(ActionType.CLOSE, trade.id, close_fraction=1.0), KRIPTO,
                                        "kapattık", reply_to=sid, tl=tl, label="CLOSE"), 4.0)
        t_post, t_close = tl.first("post", "CLOSE:"), tl.first("binance", "close market sell")
        assert t_post is not None and t_close is not None and t_close - t_post < 3.0, tl.dump()
        await iso.until(lambda: any("— tickmill geç yanıt verdi" in t for t in p.texts()), 15.0,
                        "the waiting close's answer after MetaApi's limit", tl)
        await iso.settle(p, 10.0)
        after = next(t for t in await p.trades() if t.id == trade.id)
        positions = await p.positions(trade.id)
        texts = p.texts()
    finally:
        tm.release()
        await p.close()
    print("\n" + tl.dump())
    assert positions["tickmill"].status is PositionStatus.FAILED and positions["binance"].status is PositionStatus.CLOSED
    assert after.status is TradeStatus.CLOSED
    (late,) = [t for t in texts if "— tickmill geç yanıt verdi" in t]
    assert late.startswith("🚨"), late
    assert ("tickmill: sanal tetik emri bu hesapta pozisyon açmadı ya da sonucu alınamadı (ayrı mesaja bakın); "
            "kapatılacak kayıtlı pozisyon yok. Emir tickmill hesabında açıldıysa elle kapatın") in late, late
    # the broker's own answer was told before (the order may have been sent; the account is to be checked)
    i_broker = _index(texts, "Emir durumu belirsiz")
    assert i_broker < texts.index(late), texts
    assert "değişiklik gerekmedi" not in late
    # this answer ended the trade: said in the notice, and the one closing summary comes after it
    assert "• İşlem kapandı." in late, late
    summaries = [i for i, t in enumerate(texts) if t.startswith("🏁 #1 BTC LONG")]
    assert summaries == [texts.index(late) + 1], texts


# =========================================================================== #
# G11: the management report shows the stop / the targets before -> after
# =========================================================================== #
@pytest.mark.parametrize("what,action_kw,expected", [
    ("stop 73600", {"kind": ActionType.MOVE_SL, "new_stop_loss": 73600.0},
     ["🛑 Stop: 73300 → <b>73600</b>", "🔗 İşlem: #1 BTC LONG · giriş 74000 · stop 73600 · açık"]),
    ("stop girişe", {"kind": ActionType.MOVE_SL, "move_sl_to_entry": True},
     ["🛑 Stop: 73300 → girişe (başabaş) <b>74000</b>", "🔗 İşlem: #1 BTC LONG · giriş 74000 · stop 74000 · açık"]),
    ("tp 76000", {"kind": ActionType.MOVE_TP, "take_profits": [TakeProfit(price=76000.0)]},
     ["Eski TP: 75500", "🎯 TP1: <b>76000</b>"]),
], ids=["stop_move", "stop_to_entry", "new_target"])
async def test_g11_management_report_shows_the_old_and_the_new_level(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, what: str, action_kw: dict[str, Any],
        expected: list[str]) -> None:
    """Before the fix the report was built from the trade AFTER the update: "🛑 Stop: 73600 → <b>73600</b>",
    "Eski TP: 76000"."""
    p = await iso.make_replica(tmp_path, monkeypatch)
    try:
        sid = await iso.post(p, iso.btc_market(), KRIPTO, "btc 1R long piyasadan")
        (trade,) = await p.trades()
        p.binance.set_price("BTC", 74499.0, 74501.0)              # in profit: the stop may go to the entry
        p.tickmill.set_price("BTCUSD", 74449.0, 74451.0)
        p.notifier.notifications.clear()
        kw = dict(action_kw)
        await iso.post(p, iso.manage(kw.pop("kind"), trade.id, **kw), KRIPTO, what, reply_to=sid)
        reports = [t for t in p.texts() if "#1 BTC LONG: uygulandı" in t]
    finally:
        await p.close()
    (report,) = reports
    assert report.startswith("✅"), report
    for line in expected:
        assert line in report, (line, report)


# =========================================================================== #
# G6: an account of the owner left the trade out -> "kısmen açıldı", in one message
# =========================================================================== #
FX_FOUR = (("EURUSD", 1.1620, 1.1600, 1.1660), ("GBPUSD", 1.3150, 1.3130, 1.3190),
           ("AUDUSD", 0.6520, 0.6500, 0.6560), ("NZDUSD", 0.5650, 0.5630, 0.5690))


async def test_g6_prop_account_left_out_by_its_daily_loss_rule_reads_kismen_acildi_in_one_message(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The live config counts the open risk on fx (3% of 10,000): the 4th 1R trade stays off the prop account and
    only tickmill takes it. Before the fix the owner got "✅ <b>#4 NZD LONG açıldı</b>" and the refusal twice (in the
    report and as a separate warning)."""
    p = await iso.make_replica(tmp_path, monkeypatch)
    try:
        for pair, entry, sl, tp in FX_FOUR:
            await iso.post(p, fx_pair(pair, Side.LONG, entry, sl, tp), FX, f"{pair} LONG 1R")
        notes = list(p.notifier.notifications)
        trades = await p.trades()
        fourth = await p.positions(trades[3].id)
    finally:
        await p.close()
    assert sorted(fourth) == ["tickmill"]                       # the prop account did not take the 4th
    about = [(t, s) for t, s in notes if "#4 NZD LONG" in t]
    assert len(about) == 1, about                               # one message about #4
    report, severity = about[0]
    assert report.startswith("⚠️ <b>#4 NZD LONG kısmen açıldı</b> — fx hesabında açılmadı (nedeni aşağıda)"), report
    assert severity is Severity.WARNING
    assert "• fx: günlük zarar sınırı (%3 = 300.00 USD) aşılırdı" in report and "• tickmill: " in report, report
    assert sum("fx: günlük zarar sınırı" in t for t, _s in notes) == 1, notes
    for n, base in ((1, "EUR"), (2, "GBP"), (3, "AUD")):             # opened on both: unchanged
        assert any(t.startswith(f"✅ <b>#{n} {base} LONG açıldı</b>") for t, _s in notes), notes


async def test_g6_owner_broker_error_reads_kismen_acildi(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """binance refuses the order; breakout and tickmill open. Before the fix: "⚠️ <b>#1 BTC LONG açıldı</b>"."""
    p = await iso.make_replica(tmp_path, monkeypatch)
    try:
        async def refused(_req: Any) -> Any:
            raise BrokerError("bakiye yetersiz")

        p.brokers["binance"].open_position = refused            # type: ignore[method-assign]
        await iso.post(p, iso.btc_market(), KRIPTO, "btc 1R long piyasadan")
        (trade,) = await p.trades()
        positions = await p.positions(trade.id)
        texts = p.texts()
    finally:
        await p.close()
    assert sorted(positions) == ["breakout", "tickmill"] and trade.status is TradeStatus.OPEN
    (report,) = [t for t in texts if "#1 BTC LONG" in t and "açıl" in t and "<b>" in t]
    assert report.startswith("⚠️ <b>#1 BTC LONG kısmen açıldı</b> — binance hesabında açılmadı (nedeni aşağıda)"), report
    assert "• binance: <code>bakiye yetersiz</code>" in report, report


async def test_g6_optional_broker_left_out_keeps_the_headline_and_its_line_is_the_one_message(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """tickmill's price is 2% away from Binance's (its offset check refuses the trade); the owner's binance and
    breakout open. The headline stays "✅ ... açıldı"; tickmill's refusal is in the report only (before the fix it
    also came as a separate warning)."""
    p = await iso.make_replica(tmp_path, monkeypatch)
    try:
        p.tickmill.set_price("BTCUSD", 72519.0, 72521.0)
        await iso.post(p, iso.btc_market(), KRIPTO, "btc 1R long piyasadan")
        (trade,) = await p.trades()
        positions = await p.positions(trade.id)
        texts = p.texts()
    finally:
        await p.close()
    assert sorted(positions) == ["binance", "breakout"]
    (report,) = [t for t in texts if t.startswith("✅ <b>#1 BTC LONG açıldı</b>")]
    assert "• tickmill: broker fiyatı" in report and "işlem açılmadı" in report, report
    assert sum("tickmill: broker fiyatı" in t for t in texts) == 1, texts


# =========================================================================== #
# G7: the PENDING headline says what was really placed
# =========================================================================== #
async def test_g7_limit_headline_names_the_resting_orders_and_the_virtual_trigger(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """kripto BTC LIMIT: binance rests a real limit order, breakout gets the manual limit instruction, tickmill waits
    on a virtual trigger. Before the fix: "✅ <b>#1 BTC LONG sanal tetik kuruldu</b>"."""
    p = await iso.make_replica(tmp_path, monkeypatch)
    try:
        await iso.post(p, btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0), KRIPTO, "btc 1R long limit 73000")
        set_eurusd(p, 1.16295, 1.16305)                         # the fx limit below rests on both accounts
        await iso.post(p, fx_pair("EURUSD", Side.LONG, 1.1620, 1.1600, 1.1660, entry_type=EntryType.LIMIT), FX,
                       "EUR LONG limit 1.1620")
        texts = p.texts()
    finally:
        await p.close()
    assert any(t.startswith("✅ <b>#1 BTC LONG: binance ve breakout hesaplarında limit emri yerleştirildi, tickmill "
                            "hesabında sanal tetik kuruldu</b>") for t in texts), texts
    # only real resting orders (fx route: tickmill places a broker limit, no virtual trigger): as before
    assert any(t.startswith("✅ <b>#2 EUR LONG limit emri yerleştirildi</b>") for t in texts), texts


# =========================================================================== #
# G9: a cancel that found the entry filled (and closed it) says so
# =========================================================================== #
@pytest.mark.parametrize("filled,line", [
    (None, "• binance: giriş emri bu arada dolmuştu; pozisyon piyasa fiyatından kapatıldı (K/Z "),
    (0.006, "• binance: girişin dolan kısmı (0.006) piyasa fiyatından kapatıldı, dolmayan kısmı iptal edildi (K/Z "),
], ids=["whole_fill", "partial_fill"])
@pytest.mark.parametrize("kind", [ActionType.CLOSE, ActionType.CANCEL])
async def test_g9_cancel_of_an_entry_that_had_filled_names_the_market_close(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: ActionType, filled: float | None, line: str) -> None:
    """binance's limit fills (wholly or partly) just before the trader's "kapattık" / "iptal" reaches it: the adapter's
    cancel finds the fill and closes it at the market (CLOSED). Before the fix the line read "bekleyen giriş iptal
    edildi"."""
    p = await iso.make_replica(tmp_path, monkeypatch)
    try:
        await iso.post(p, btc(EntryType.LIMIT, 73990.0, 73300.0, 75500.0), KRIPTO, "btc 1R long limit 73990")
        (trade,) = await p.trades()
        oid = p.binance.created()[0]["id"]
        p.binance.set_price("BTC", 73989.0, 73991.0)
        p.binance.fill(oid, amount=filled)                      # not synced yet: the bot still holds it as PENDING
        p.notifier.notifications.clear()
        plan = ManagementPlan(plan_id="g9", action=kind, trade_id=trade.id,
                              close_fraction=1.0 if kind is ActionType.CLOSE else None, source_chat_id=iso.CHAT,
                              summary_tr=f"#{trade.id} BTC LONG: iptal", created_at=datetime.now(UTC))
        result = await p.engine.execute_management(plan)
        binance = (await p.positions(trade.id))["binance"]
    finally:
        await p.close()
    assert binance.status is PositionStatus.CLOSED and binance.remaining_quantity == 0.0, binance
    assert line in result, result
    assert "binance: bekleyen giriş iptal edildi" not in result, result


# =========================================================================== #
# G5: "bu hesapta işlem görmüyor" names the pair
# =========================================================================== #
async def test_g5_unsupported_pair_is_named_as_the_pair(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """tickmill does not list EURJPY (it does list EURUSD). Before the fix the line read "tickmill: EUR bu hesapta
    işlem görmüyor"."""
    p = await iso.make_replica(tmp_path, monkeypatch)
    try:
        await iso.post(p, fx_pair("EURJPY", Side.SHORT, 178.90, 179.10, 178.50), FX, "EURJPY SHORT 1R")
        texts = p.texts()
        (trade,) = await p.trades()
    finally:
        await p.close()
    assert trade.note == "tickmill: unsupported"
    (report,) = [t for t in texts if "#1 EUR SHORT" in t]          # (the trade's label is unchanged)
    assert "• tickmill: EURJPY bu hesapta işlem görmüyor" in report.splitlines(), report
    assert report.startswith("✅ <b>#1 EUR SHORT açıldı</b>"), report      # tickmill is optional: headline as before


# =========================================================================== #
# G8: one risk figure for one position (the size limited near the stop)
# =========================================================================== #
def _figure(text: str, before: str) -> float:
    """The number right after ``before`` in ``text`` ("~28.54 USDT" -> 28.54)."""
    tail = text.split(before, 1)[1]
    return float(tail.split()[0].rstrip(","))


async def test_g8_capped_market_order_shows_what_the_quantity_really_sent_risks(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Scenario f (crypto) on the live config: BTC long, entry 74000, SL 73300, Binance at 73501 (201 above the stop):
    each size is limited to twice the planned one (sized as if entered 350 above the stop) and rounded down to the
    broker's step. Before the fix the report computed "gerçek risk" from the UNROUNDED size (target x 201 / 350):
    breakout 28.71 while its own instruction said 28.54 (0.142 BTC x 201), binance 5.74 while 0.023 BTC really risk
    6.31 with the 0.05% taker fee its sizing counts, tickmill 25.84 for 0.12 x 201 = 24.12."""
    p = await iso.make_replica(tmp_path, monkeypatch)
    try:
        p.binance.set_price("BTC", 73499.0, 73501.0)
        p.tickmill.set_price("BTCUSD", 73449.0, 73451.0)
        await iso.post(p, iso.btc_market(), KRIPTO, "btc 1R long")
        (trade,) = await p.trades()
        positions = await p.positions(trade.id)
        texts = p.texts()
    finally:
        await p.close()
    (instruction,) = [t for t in texts if "Elle işlem (breakout)" in t and "Miktar:" in t]  # (final: the instruction names the unit and the USD value; 'KONTRAT' was typed as USD on 2026-11-12)
    (report,) = [t for t in texts if t.startswith("✅ <b>#1 BTC LONG açıldı</b>")]
    line = {b: next(x for x in report.splitlines() if x.startswith(f"• {b}:")) for b in ("binance", "breakout", "tickmill")}
    qty = {b: positions[b].quantity for b in line}
    assert qty == {"binance": 0.023, "breakout": 0.142, "tickmill": 0.12}
    # breakout: the report and the instruction show the same figure, that of the quantity in the instruction
    assert _figure(instruction, "Risk: ~") == _figure(line["breakout"], "gerçek risk ~") == pytest.approx(28.54)
    assert round(qty["breakout"] * (73501.0 - 73300.0), 2) == 28.54
    # binance: its quantity's loss at the stop from 73501, with the taker fee (0.05%) its sizing counts
    assert _figure(line["binance"], "gerçek risk ~") == pytest.approx(
        round(qty["binance"] * ((73501.0 - 73300.0) + 0.0005 * (73501.0 + 73300.0)), 2))
    # tickmill (CFD, the offset -50 applied: 73451 -> stop 73250)
    assert _figure(line["tickmill"], "gerçek risk ~") == pytest.approx(round(qty["tickmill"] * (73451.0 - 73250.0), 2))
    for b in line:
        assert "(hedefin altında)" in line[b], line[b]


def test_g8_real_risk_falls_back_to_scaling_when_the_quote_is_not_the_account_currency() -> None:
    from copytrader.engine.orchestrator import capped_real_risk, size_cap_warning
    from copytrader.models import SizingResult

    size = SizingResult(quantity=0.5, risk_amount=100.0, est_loss_at_sl=97.0, notional=50_000.0)
    # USDJPY on a USD account: the move between the entries is in JPY, so the loss is scaled by the stop distances
    assert capped_real_risk(size, 100_000.0, "JPY", "USD", 154.10, 154.20, 154.00) == pytest.approx(97.0 * 0.10 / 0.20)
    # quoted in the account currency: the move between the entries is taken off
    assert capped_real_risk(size, 1.0, "USDT", "USDT", 73501.0, 73650.0, 73300.0) == pytest.approx(97.0 - 0.5 * 149.0)
    # a size raised to the minimum lot can risk more than planned even when limited: then it is not "below target"
    text = size_cap_warning(10.0, 73501.0, 73650.0, 73300.0, "USDT", real=12.5)
    assert text.endswith("gerçek risk ~12.50 USDT (hedef 10.00 USDT).") and "hedefin altında" not in text


# =========================================================================== #
# G4: the ccxt adapter's venue size read and "flat_seen" are a public contract of the monitor
# =========================================================================== #
async def test_g4_monitor_close_retry_uses_the_ccxt_adapters_public_contract(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Monitor._retry_close must never send a second reduce-only close for a position the venue already shows flat,
    nor one that could close ANOTHER trade on the same symbol. It reads two things of the exchange adapter: the
    flat-read counter in ``pos.data`` and the venue's position size. Both are pinned here by their public names, so
    a rename on either side fails this test (before: the monitor read the private ``_venue_position_size``)."""
    import inspect

    from copytrader.brokers import ccxt_broker
    from copytrader.brokers.ccxt_broker import CcxtBroker
    from copytrader.engine import monitor as mon

    assert ccxt_broker.FLAT_SEEN == mon.VENUE_FLAT_SEEN == "flat_seen"
    assert inspect.iscoroutinefunction(CcxtBroker.venue_position_size)
    p = await iso.make_replica(tmp_path, monkeypatch)
    try:
        await iso.post(p, iso.btc_market(), KRIPTO, "btc 1R long piyasadan")
        (trade,) = await p.trades()
        broker = p.brokers["binance"]
        pos = (await p.positions(trade.id))["binance"]
        symbol = pos.broker_symbol
        assert await broker.venue_position_size(symbol, Side.LONG) == pytest.approx(0.012)
        assert await broker.venue_position_size(symbol, Side.SHORT) == pytest.approx(-0.012)

        # another active trade of the bot holds the same symbol in the same direction: the guard reads the venue
        # size through the public method (a spy proves it) and holds the retry when the venue holds less
        other = await p.db.create_trade(trade.plan.model_copy(update={"plan_id": "g4other"}))
        await p.db.save_broker_position(other.id, pos.model_copy(deep=True))
        await p.db.update_trade(other.id, status=TradeStatus.OPEN)
        reads: list[tuple[str, Side]] = []
        public = broker.venue_position_size

        async def spy(sym: str, side: Side) -> float | None:
            reads.append((sym, side))
            return await public(sym, side)

        broker.venue_position_size = spy                           # type: ignore[method-assign]
        doubt = await p.engine.monitor._shared_venue_doubt(broker, trade, pos)
        assert reads == [(symbol, Side.LONG)] and doubt is not None and f"#{other.id}" in doubt, (reads, doubt)
        p.binance.net[symbol] = 0.024                              # the venue covers both trades: no doubt
        assert await p.engine.monitor._shared_venue_doubt(broker, trade, pos) is None

        # the venue shows no position: the adapter counts the flat read in pos.data, and the monitor's retry of an
        # owed close sends nothing while it is above 0
        p.binance.net[symbol] = 0.0
        synced = await broker.sync_position(pos)
        assert synced.status is PositionStatus.OPEN and synced.data[ccxt_broker.FLAT_SEEN] == 1
        synced.data[mon.CLOSE_WANTED] = {"attempts": 1, "next_at": "2000-01-01T00:00:00+00:00"}
        before = len(p.binance.created())
        kept = await p.engine.monitor._retry_close(broker, trade, synced, datetime.now(UTC))
        assert kept is synced and len(p.binance.created()) == before
    finally:
        await p.close()


async def test_g6_prop_account_left_out_by_its_hedging_ban_reads_kismen_acildi(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """fx (no_hedge) still holds an EURUSD SHORT of the bot; tickmill's copy of it has closed. A new EURUSD LONG opens
    on tickmill only. Before the fix: "✅ <b>#2 EUR LONG açıldı</b>"."""
    p = await iso.make_replica(tmp_path, monkeypatch)
    try:
        set_eurusd(p, 1.10872, 1.10874)
        await iso.post(p, iso.eur_short(), FX, "Eu short")
        (first,) = await p.trades()
        tick = (await p.positions(first.id))["tickmill"]
        await p.db.save_broker_position(first.id, tick.model_copy(update={
            "status": PositionStatus.CLOSED, "remaining_quantity": 0.0, "realized_pnl": 1.0}))
        p.notifier.notifications.clear()
        await iso.post(p, fx_pair("EURUSD", Side.LONG, 1.10872, 1.10700, 1.11200), FX, "Eu long")
        trades = await p.trades()
        second = await p.positions(trades[-1].id)
        texts = p.texts()
    finally:
        await p.close()
    assert sorted(second) == ["tickmill"]
    (report,) = [t for t in texts if "#2 EUR LONG" in t]
    assert report.startswith("⚠️ <b>#2 EUR LONG kısmen açıldı</b> — fx hesabında açılmadı (nedeni aşağıda)"), report
    assert "• fx: EUR: açık SHORT var; ters yönlü LONG sinyali hedging yasağı nedeniyle açılmadı" in report, report
