"""``reference_broker``: a forex / CFD LIMIT entry on an account (the friend's tickmill) waits for ANOTHER account's leg
of the same trade (the owner's fx) to fill, instead of resting as a broker limit that a brief wick can fill.

    fx route LIMIT        tickmill: no broker order, ever; a virtual entry with ``reference_broker``
    fx order waiting      tickmill waits for its fill (no price polling of fx meanwhile)
    fx leg fills          tickmill: MARKET, stop and every target shifted by (tickmill mid - fx mid) at that moment
    fx leg ends unfilled  tickmill: the waiting entry is cancelled (the existing "other accounts ended" rule)
    fx has no order       (its owner's answer awaited, skipped, refused, not connected) tickmill watches fx's own
                          live price: long ask <= entry / short bid >= entry -> MARKET; unreadable -> waits, told
    fire guards           stop passed -> refused; offset too big / a price unreadable -> skipped (fail closed)

On the production replica (tests/test_preflight_scenarios.py): real Engine, MT5 adapter for fx, MetaApi for tickmill.
"""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from copytrader.config import AppConfig
from copytrader.engine.monitor import is_virtual_pending
from copytrader.engine.orchestrator import Engine
from copytrader.models import EntryType, PositionStatus, Side, TradeStatus

import test_preflight_scenarios as tps
from fakes import FakeNotifier
from test_preflight_scenarios import FX, KRIPTO, Prod, _isolate, btc, fx_pair  # noqa: F401  (_isolate: autouse)

ENTRY, STOP, TP = 1.1610, 1.1590, 1.1650           # an EURUSD long limit below the market (1.16195 / 1.16205)
OWNER_NAMES = re.compile(r"\bfx2?\b|fundingpips|\bbem\b", re.IGNORECASE)


def _reference(cfg: AppConfig) -> None:
    tickmill = next(b for b in cfg.brokers if b.name == "tickmill")
    tickmill.reference_broker = "fx"
    tickmill.virtual_trigger_max_hours = 24


class FriendNotes:
    """What the friend's chat gets (the notifier's friend_note)."""

    def __init__(self, notifier: FakeNotifier) -> None:
        self.texts: list[str] = []
        notifier.friend_note = self.note          # type: ignore[attr-defined]

    async def note(self, text: str) -> None:
        self.texts.append(text)


async def _make(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, reference: bool = True,
                edit: Any = None) -> tuple[Prod, FriendNotes]:
    def change(cfg: AppConfig) -> None:
        if reference:
            _reference(cfg)
        if edit is not None:
            edit(cfg)
    p = await Prod.create(tmp_path, monkeypatch, edit=change)
    return p, FriendNotes(p.notifier)


@pytest.fixture
async def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    p, friend = await _make(tmp_path, monkeypatch)
    yield p, friend
    await p.close()


async def _settle(p: Prod) -> None:
    """Let the friend's feed (tasks of their own) and other spawned notices run."""
    import asyncio
    for _ in range(5):
        await asyncio.sleep(0)
    pending = [t for t in list(p.engine._tasks) if not t.done()]
    if pending:
        await asyncio.wait(pending, timeout=2.0)


async def _eur_limit(p: Prod) -> int:
    return await p.say(fx_pair("EURUSD", Side.LONG, ENTRY, STOP, TP, entry_type=EntryType.LIMIT), FX,
                       "EUR LONG 1R Limit Emir")


def _fx_ticket(p: Prod) -> int:
    (ticket,) = list(p.terminal.orders)
    return ticket


def _ticks_read(p: Prod) -> int:
    return sum(1 for name, _a, _k in p.terminal.calls if name == "symbol_info_tick")


async def _fill_fx(p: Prod, *, fx: tuple[float, float], tickmill: tuple[float, float]) -> None:
    """fx's limit fills at the broker; the next full pass books it (fx syncs under the trade lock, after the
    look of tickmill's entry has started), the fast pass after it fires tickmill."""
    p.terminal.set_price("EURUSD", *fx)
    p.tickmill.set_price("EURUSD", *tickmill)
    p.terminal.fill(_fx_ticket(p), ENTRY)
    await p.engine.monitor.run_once()
    await p.engine.monitor.run_virtual_once()


def _friend_clean(texts: list[str]) -> None:
    for text in texts:
        assert not OWNER_NAMES.search(text), text


# --------------------------------------------------------------------------- #
# waiting
# --------------------------------------------------------------------------- #
async def test_fx_route_limit_waits_on_tickmill_without_a_broker_order_and_never_polls_fx(env) -> None:
    p, friend = env
    await _eur_limit(p)
    assert p.asked() == []
    assert p.fx_orders() == [("BUY_LIMIT", "EURUSD", 0.5, ENTRY, STOP, TP)]
    assert p.tickmill_orders() == []                                   # no limit at tickmill's own price
    (trade,) = await p.trades()
    assert trade.status is TradeStatus.PENDING
    pos = (await p.positions(trade.id))["tickmill"]
    assert is_virtual_pending(pos) and pos.data["reference_broker"] == "fx"
    assert "reference_exchange" not in pos.data and pos.data["trigger_price"] == ENTRY
    assert (pos.stop_loss, [tp.price for tp in pos.take_profits]) == (STOP, [TP])
    assert pos.data["virtual_max_hours"] == 24
    assert any("tickmill: sanal tetik kuruldu (limit emir konmadı): fx hesabındaki giriş emri dolunca" in t
               for t in p.texts())
    # the looks read the database only: no fx quote, no reference exchange
    before = _ticks_read(p)
    for _ in range(3):
        await p.engine.monitor.run_virtual_once()
    assert _ticks_read(p) == before and p.ref.calls == []
    assert p.tickmill_orders() == [] and is_virtual_pending((await p.positions(trade.id))["tickmill"])
    await _settle(p)
    _friend_clean(friend.texts)
    assert any("tickmill hesabınızda bekliyor" in t for t in friend.texts)
    # his /pozisyonlar: his line names no other account
    mine = await p.engine.positions_text(accounts={"tickmill"})
    (line,) = [ln for ln in mine.splitlines() if ln.startswith("• ")]
    assert line.startswith("• tickmill: bekleyen (sanal tetik)") and not OWNER_NAMES.search(line), line
    assert "referans hesap" in line
    owner = await p.engine.positions_text()
    assert "fx hesabındaki giriş dolunca" in owner


async def test_fx_fill_opens_tickmill_at_market_with_stop_and_targets_shifted_by_the_offset_then_never_again(
    env
) -> None:
    p, friend = env
    await _eur_limit(p)
    # fx mid 1.16100, tickmill mid 1.16115: offset +0.00015
    await _fill_fx(p, fx=(1.16095, 1.16105), tickmill=(1.16110, 1.16120))
    (order,) = p.tickmill_orders()
    assert order[:2] == ("market_buy", "EURUSD")
    assert order[-2:] == (pytest.approx(STOP + 0.00015), pytest.approx(TP + 0.00015))
    pos = (await p.positions(1))["tickmill"]
    assert pos.status is PositionStatus.OPEN and pos.data["price_offset"]["value"] == pytest.approx(0.00015)
    assert pos.data["virtual_fired"]["trigger_price"] == ENTRY
    (note,) = [t for t in p.texts() if "#1 EUR LONG tickmill hesabında açıldı" in t]
    assert "fx hesabındaki giriş doldu" in note
    for _ in range(2):
        await p.engine.monitor.run_once()
        await p.engine.monitor.run_virtual_once()
    assert len(p.tickmill_orders()) == 1
    await _settle(p)
    _friend_clean(friend.texts)
    assert any("giriş emri doldu" in t for t in friend.texts)


async def test_fx_entry_ending_unfilled_cancels_the_waiting_entry(env) -> None:
    p, friend = env
    await _eur_limit(p)
    rows = await p.positions(1)
    gone = await p.brokers["fx"].cancel_entry(rows["fx"])               # (expired / cancelled at the broker)
    assert gone.status is PositionStatus.CANCELLED
    await p.db.save_broker_position(1, gone)
    await p.engine.monitor.run_virtual_once()
    rows = await p.positions(1)
    assert rows["tickmill"].status is PositionStatus.CANCELLED and p.tickmill_orders() == []
    assert any("diğer hesaplardaki girişleri dolmadan sona erdi" in t for t in p.texts())
    await _settle(p)
    _friend_clean(friend.texts)
    assert any("bekleyen giriş emri iptal edildi" in t for t in friend.texts)


async def test_fx_filled_and_closed_before_the_look_cancels_the_waiting_entry(env) -> None:
    """The other-accounts rule decides first: fx's position is over (the trade has played out), so tickmill is not
    opened afterwards on its own (fail closed)."""
    p, _friend = env
    await _eur_limit(p)
    ticket = _fx_ticket(p)
    p.terminal.fill(ticket, ENTRY)
    p.terminal.hit(ticket, 4, STOP)                                    # stopped out at once
    for _ in range(3):      # (whichever way the look of tickmill and the sync of fx interleave in a pass)
        await p.engine.monitor.run_once()
    assert (await p.positions(1))["fx"].status is PositionStatus.CLOSED
    await p.engine.monitor.run_virtual_once()
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED
    assert p.tickmill_orders() == []


# --------------------------------------------------------------------------- #
# guards at the fire
# --------------------------------------------------------------------------- #
async def test_stop_passed_at_tickmill_when_fx_fills_is_refused(env) -> None:
    p, friend = env
    await _eur_limit(p)
    # fx mid 1.15895, tickmill 1.15880 / 1.15890 (mid 1.15885): shifted stop 1.1589, tickmill's bid is at it
    await _fill_fx(p, fx=(1.15890, 1.15900), tickmill=(1.15880, 1.15890))
    assert p.tickmill_orders() == []
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED
    (note,) = [t for t in p.texts() if "fx hesabındaki giriş doldu ama işlem açılmadı" in t]
    assert "stop seviyesini" in note
    await _settle(p)
    _friend_clean(friend.texts)


async def test_offset_too_big_at_the_fire_is_skipped(env) -> None:
    p, _friend = env
    await _eur_limit(p)
    await _fill_fx(p, fx=(1.16095, 1.16105), tickmill=(1.18000, 1.18010))    # 1.6 % apart (limit 1 %)
    assert p.tickmill_orders() == []
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED
    (note,) = [t for t in p.texts() if "fx hesabındaki giriş doldu ama işlem açılmadı" in t]
    assert "referans hesabın fiyatı" in note and "arasındaki fark" in note


async def test_unreadable_fx_price_at_the_fire_is_skipped(env) -> None:
    p, _friend = env
    await _eur_limit(p)
    p.terminal.set_price("EURUSD", 0.0, 0.0)                           # no quote from fx right now
    p.tickmill.set_price("EURUSD", 1.16110, 1.16120)
    p.terminal.fill(_fx_ticket(p), ENTRY)
    await p.engine.monitor.run_once()
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == []
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED
    assert any("referans hesabın fiyatı alınamadı" in t for t in p.texts())


# --------------------------------------------------------------------------- #
# no fx order in the trade: never a limit on tickmill; fx's own price decides
# --------------------------------------------------------------------------- #
def _no_confirm(cfg: AppConfig) -> None:
    next(b for b in cfg.brokers if b.name == "tickmill").confirm_open = False


async def _unsure_eur_limit(p: Prod) -> int:
    action = fx_pair("EURUSD", Side.LONG, ENTRY, STOP, TP, entry_type=EntryType.LIMIT).model_copy(
        update={"confidence": 0.5})
    return await p.say(action, FX, "EUR LONG 1R Limit Emir")


def _fx_at_entry(p: Prod) -> None:
    """fx's own price reaches the entry (ask 1.16100 <= 1.1610); tickmill 0.00015 higher (mid 1.16110)."""
    p.terminal.set_price("EURUSD", 1.16090, 1.16100)
    p.tickmill.set_price("EURUSD", 1.16105, 1.16115)


@pytest.fixture
async def unsure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    p, friend = await _make(tmp_path, monkeypatch, edit=_no_confirm)
    yield p, friend
    await p.close()


async def test_fx_waiting_for_the_owner_tickmill_enters_on_fx_price_touch_never_a_limit(unsure) -> None:
    p, friend = unsure
    await _unsure_eur_limit(p)
    (conf,) = p.notifier.confirmations                                # fx waits for the owner's answer
    assert conf.early is not None and p.fx_orders() == [] and p.tickmill_orders() == []
    pos = (await p.positions(1))["tickmill"]
    assert is_virtual_pending(pos) and pos.data["reference_broker"] == "fx"
    assert "sanal tetiği kuruldu" in conf.early["line"]
    for _ in range(3):                                                # fx's price above the entry: waits
        await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == []
    _fx_at_entry(p)
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[:2] == ("market_buy", "EURUSD")                     # at market, never a limit
    assert order[-2:] == (pytest.approx(STOP + 0.00015), pytest.approx(TP + 0.00015))
    assert p.fx_orders() == []
    rows = await p.positions(1)
    assert rows["tickmill"].status is PositionStatus.OPEN and "fx" not in rows
    (note,) = [t for t in p.texts() if "#1 EUR LONG tickmill hesabında açıldı" in t]
    assert "fx fiyatı giriş seviyesine geldi" in note
    # Evet afterwards: fx opens, tickmill never twice
    await p.engine.handle_confirmation(conf.id, True)
    assert len(p.fx_orders()) == 1
    for _ in range(2):
        await p.engine.monitor.run_once()
        await p.engine.monitor.run_virtual_once()
    assert len(p.tickmill_orders()) == 1
    await _settle(p)
    _friend_clean(friend.texts)


async def test_fx_waiting_then_yes_before_the_touch_hands_over_to_fx_fill(unsure) -> None:
    p, _friend = unsure
    await _unsure_eur_limit(p)
    (conf,) = p.notifier.confirmations
    await p.engine.handle_confirmation(conf.id, True)
    assert p.fx_orders() == [("BUY_LIMIT", "EURUSD", 0.5, ENTRY, STOP, TP)] and p.tickmill_orders() == []
    before = _ticks_read(p)
    await p.engine.monitor.run_virtual_once()                         # fx's own order waits: its fill decides
    assert _ticks_read(p) == before and p.tickmill_orders() == []
    await _fill_fx(p, fx=(1.16095, 1.16105), tickmill=(1.16110, 1.16120))
    (order,) = p.tickmill_orders()
    assert order[:2] == ("market_buy", "EURUSD")
    for _ in range(2):
        await p.engine.monitor.run_once()
        await p.engine.monitor.run_virtual_once()
    assert len(p.tickmill_orders()) == 1


async def test_fx_no_leaves_tickmill_watching_fx_price(unsure) -> None:
    p, _friend = unsure
    await _unsure_eur_limit(p)
    (conf,) = p.notifier.confirmations
    await p.engine.handle_confirmation(conf.id, False)
    assert p.fx_orders() == [] and is_virtual_pending((await p.positions(1))["tickmill"])
    _fx_at_entry(p)
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[0] == "market_buy" and p.fx_orders() == []


async def test_fx_not_connected_tickmill_waits_never_a_limit_and_the_owner_is_told(env) -> None:
    p, _friend = env
    p.engine.brokers = [b for b in p.engine.brokers if b.name != "fx"]
    p.engine.failed_brokers["fx"] = "MT5 terminali açık değil"
    await _eur_limit(p)
    assert p.fx_orders() == [] and p.tickmill_orders() == []
    pos = (await p.positions(1))["tickmill"]
    assert is_virtual_pending(pos) and pos.data["reference_broker"] == "fx"
    _fx_at_entry(p)                                                   # (fx's terminal moves, but nobody can read it)
    for _ in range(3):
        await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == []
    told = [t for t in p.texts() if "fx fiyatı okunamıyor" in t]
    assert len(told) == 1 and "limit emir KONMAZ" in told[0]          # once (throttled)
    assert is_virtual_pending((await p.positions(1))["tickmill"])


async def test_fx_skipped_without_an_order_tickmill_follows_fx_price(env) -> None:
    p, _friend = env
    del p.terminal.symbols["EURUSD"]                                   # fx cannot trade it now: no fx row
    await _eur_limit(p)
    rows = await p.positions(1)
    assert "fx" not in rows and is_virtual_pending(rows["tickmill"]) and p.tickmill_orders() == []
    await p.engine.monitor.run_virtual_once()                          # (fx's price cannot be read either)
    assert p.tickmill_orders() == [] and any("fx fiyatı okunamıyor" in t for t in p.texts())


async def test_an_fx_leg_that_failed_without_a_fill_is_no_leg_tickmill_follows_fx_price(env) -> None:
    """fx's order was refused / could not be completed (its row ended FAILED, nothing filled): fx never held the
    trade - not a cancel of tickmill's entry, and never a limit: fx's price decides."""
    p, friend = env
    await _eur_limit(p)
    rows = await p.positions(1)
    failed = rows["fx"].model_copy(update={"status": PositionStatus.FAILED, "filled_quantity": 0.0,
                                           "avg_entry_price": None})
    await p.db.save_broker_position(1, failed)
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == [] and is_virtual_pending((await p.positions(1))["tickmill"])
    assert not any("diğer hesaplardaki girişleri dolmadan sona erdi" in t for t in p.texts())
    _fx_at_entry(p)
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[0] == "market_buy"
    await _settle(p)
    _friend_clean(friend.texts)


async def test_fx_price_watch_reads_lightly_and_backs_off_after_a_failed_read(unsure, caplog) -> None:
    """The owner's own fx account is only read: the instrument once, the quote on each look; a read that fails (no
    price - market closed, terminal trouble) is not tried again for REFERENCE_PRICE_RETRY_SEC and logs no
    traceback."""
    import logging

    p, _friend = unsure
    await _unsure_eur_limit(p)
    fx = p.brokers["fx"]
    calls = {"resolve": 0, "quote": 0}
    real_resolve, real_quote = fx.resolve_symbol, fx.price_since

    async def resolve(*a: Any, **k: Any) -> Any:
        calls["resolve"] += 1
        return await real_resolve(*a, **k)

    async def quote(*a: Any, **k: Any) -> Any:
        calls["quote"] += 1
        return await real_quote(*a, **k)

    fx.resolve_symbol, fx.price_since = resolve, quote                  # type: ignore[method-assign]
    for _ in range(3):
        await p.engine.monitor.run_virtual_once()
    assert calls == {"resolve": 1, "quote": 3} and p.tickmill_orders() == []
    p.terminal.set_price("EURUSD", 0.0, 0.0)                            # no price from fx now
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            await p.engine.monitor.run_virtual_once()
    assert calls["quote"] == 4                                          # one failed read, then the back-off
    assert not any(r.exc_info for r in caplog.records if "reference price" in r.getMessage())
    assert len([t for t in p.texts() if "fx fiyatı okunamıyor" in t]) == 1
    p.engine._ref_retry_at.clear()                                      # (the back-off is over)
    _fx_at_entry(p)
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[0] == "market_buy"


# --------------------------------------------------------------------------- #
# what happened between two looks: fx's tick history (or its 1-minute bars)
# --------------------------------------------------------------------------- #
T0 = tps.SERVER_TIME                                                     # fx's server clock (UTC+2 in the fake)


def _tick(p: Prod, bid: float, ask: float, at: float) -> None:
    """fx's latest tick at server time ``at`` (seconds)."""
    p.terminal.ticks["EURUSD"] = SimpleNamespace(bid=bid, ask=ask, last=0.0, time=int(at), time_msc=int(at * 1000))


def _history(p: Prod, ticks: list[tuple[float, float, float]]) -> list[tuple[Any, ...]]:
    """fx's tick history (server time, bid, ask), read with copy_ticks_from; returns the calls made to it."""
    calls: list[tuple[Any, ...]] = []
    rows = [{"time": int(t), "time_msc": int(t * 1000), "bid": b, "ask": a} for t, b, a in ticks]

    def copy_ticks_from(symbol: str, date_from: int, count: int, flags: int) -> list[dict[str, Any]]:
        calls.append((symbol, date_from, count, flags))
        return [r for r in rows if r["time"] >= date_from][:count]

    p.terminal.copy_ticks_from = copy_ticks_from                         # type: ignore[attr-defined]
    return calls


async def _armed_watch(p: Prod) -> None:
    """A low-confidence EUR long limit (fx asked, tickmill watching fx's price) and its first look at T0."""
    _tick(p, 1.16195, 1.16205, T0)
    await _unsure_eur_limit(p)
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == []


async def test_a_brief_touch_between_two_looks_is_seen_in_fx_ticks(unsure) -> None:
    p, _friend = unsure
    calls = _history(p, [(T0 + 0.5, 1.16090, 1.16098), (T0 + 1.0, 1.16110, 1.16120)])
    await _armed_watch(p)
    _tick(p, 1.16110, 1.16120, T0 + 1.5)                     # back above the entry, but within ~3 pips of it
    p.tickmill.set_price("EURUSD", 1.16125, 1.16135)
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[0] == "market_buy"
    assert calls and calls[-1][1] == T0                       # read from where the previous look ended
    (note,) = [t for t in p.texts() if "#1 EUR LONG tickmill hesabında açıldı" in t]
    assert "kısa süreliğine 1.16098" in note


async def test_a_brief_touch_far_behind_is_not_chased(unsure) -> None:
    p, _friend = unsure
    _history(p, [(T0 + 0.5, 1.16090, 1.16098)])
    await _armed_watch(p)
    _tick(p, 1.16245, 1.16255, T0 + 1.5)                     # the price ran away (15 pips): no chase
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == []
    assert any("tetik seviyesi görüldü ama fiyat uzaklaştı" in t for t in p.texts())
    pos = (await p.positions(1))["tickmill"]
    assert is_virtual_pending(pos) and pos.data["virtual_missed"]
    _tick(p, 1.16090, 1.16100, T0 + 3.0)                     # back at the level: opens
    p.tickmill.set_price("EURUSD", 1.16105, 1.16115)
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[0] == "market_buy"


async def test_without_tick_history_the_one_minute_bars_show_the_touch(unsure) -> None:
    p, _friend = unsure
    await _armed_watch(p)                                     # (the fake terminal has no copy_ticks_from)
    first = -(-T0 // 60) * 60                                 # the first minute that began after the first look
    asked: list[tuple[Any, ...]] = []

    def copy_rates_range(symbol: str, timeframe: int, date_from: int, date_to: int) -> list[dict[str, Any]]:
        asked.append((symbol, timeframe, date_from, date_to))
        return [{"time": first - 60, "open": 1.1619, "high": 1.1620, "low": 1.1600, "close": 1.1619, "spread": 10},
                {"time": first, "open": 1.1619, "high": 1.1620, "low": 1.16085, "close": 1.1612, "spread": 10}]

    p.terminal.copy_rates_range = copy_rates_range            # type: ignore[method-assign]
    _tick(p, 1.16110, 1.16120, first + 30)
    p.tickmill.set_price("EURUSD", 1.16125, 1.16135)
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()                            # low 1.16085 + spread 0.00010 -> ask 1.16095 <= 1.1610
    assert order[0] == "market_buy"
    assert asked == [("EURUSD", 1, first, first + 30)]        # M1, from the first whole minute after the look


async def test_fx_reaching_the_first_target_before_the_entry_cancels_the_waiting_entry(unsure) -> None:
    p, _friend = unsure
    _history(p, [(T0 + 0.5, 1.16505, 1.16515)])               # bid 1.16505 >= TP 1.1650, the entry never touched
    await _armed_watch(p)
    _tick(p, 1.16400, 1.16410, T0 + 1.5)
    await p.engine.monitor.run_virtual_once()
    pos = (await p.positions(1))["tickmill"]
    assert pos.status is PositionStatus.CANCELLED and p.tickmill_orders() == []
    (note,) = [t for t in p.texts() if "sanal tetik iptal edildi" in t]
    assert "fx fiyatı (1.16505) ilk hedefi (1.165) geçti" in note
    assert "giriş tetiklenmeden fiyat uzaklaştı, işlem AÇILMADI" in note


async def test_a_pullback_limit_counts_the_target_only_after_fx_has_been_between(tmp_path: Path,
                                                                               monkeypatch: pytest.MonkeyPatch) -> None:
    """A long limit whose first target is BELOW today's price (a pullback): fx above the target at the signal does
    not cancel it; once fx has been between the entry and the target, reaching the target again does."""
    p, _friend = await _make(tmp_path, monkeypatch, edit=_no_confirm)
    try:
        tps.set_eurusd(p, 1.16195, 1.16205)
        action = fx_pair("EURUSD", Side.LONG, 1.1600, 1.1580, 1.1615, entry_type=EntryType.LIMIT).model_copy(
            update={"confidence": 0.5})
        _tick(p, 1.16195, 1.16205, T0)
        await p.say(action, FX, "EUR LONG 1R Limit Emir")
        pos = (await p.positions(1))["tickmill"]
        assert pos.data["virtual_tp_armed"] is False
        await p.engine.monitor.run_virtual_once()
        assert is_virtual_pending((await p.positions(1))["tickmill"])            # above the target: still waiting
        _history(p, [(T0 + 0.5, 1.16100, 1.16110)])                               # between the entry and the target
        _tick(p, 1.16120, 1.16130, T0 + 1.0)
        await p.engine.monitor.run_virtual_once()
        pos = (await p.positions(1))["tickmill"]
        assert is_virtual_pending(pos) and pos.data["virtual_tp_armed"] is True
        _history(p, [(T0 + 1.5, 1.16155, 1.16165)])                               # now at the target
        _tick(p, 1.16140, 1.16150, T0 + 2.0)
        await p.engine.monitor.run_virtual_once()
        assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED
        assert p.tickmill_orders() == []
    finally:
        await p.close()


async def test_fx_passing_the_stop_before_the_look_cancels_never_opens(unsure) -> None:
    p, _friend = unsure
    _history(p, [(T0 + 0.5, 1.15880, 1.15890)])               # straight through the entry and the stop 1.1590
    await _armed_watch(p)
    _tick(p, 1.15950, 1.15960, T0 + 1.5)
    await p.engine.monitor.run_virtual_once()
    assert (await p.positions(1))["tickmill"].status is PositionStatus.CANCELLED and p.tickmill_orders() == []
    assert any("stop seviyesini (1.159) geçti" in t for t in p.texts())


async def test_fx_busy_with_another_call_skips_the_history_and_reads_it_next_time(unsure) -> None:
    p, _friend = unsure
    calls = _history(p, [(T0 + 0.5, 1.16090, 1.16098)])
    await _armed_watch(p)
    _tick(p, 1.16110, 1.16120, T0 + 1.5)
    fx = p.brokers["fx"]
    busy = [True]
    real = fx._oldest_call
    other = SimpleNamespace(timed_out=False, running_for=lambda: 0.0, stuck_after=lambda: 1e9,
                            relinking=lambda: False)                  # another call running on fx's worker thread
    fx._oldest_call = lambda: other if busy[0] else real()       # type: ignore[method-assign]
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == [] and calls == []          # an order may be on its way: not in its way
    busy[0] = False
    p.tickmill.set_price("EURUSD", 1.16125, 1.16135)
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()                            # the touch at T0 + 0.5 is still found
    assert order[0] == "market_buy" and calls[-1][1] == T0


def test_the_tickmill_bridge_can_serve_price_since_too() -> None:
    from copytrader.brokers import mt5_bridge, mt5_helper
    assert "price_since" in mt5_helper.METHODS and callable(getattr(mt5_bridge.Mt5BridgeBroker, "price_since"))


async def test_a_short_fires_on_fx_bid_at_or_above_the_entry(unsure) -> None:
    p, _friend = unsure
    action = fx_pair("EURUSD", Side.SHORT, 1.1630, 1.1650, 1.1590, entry_type=EntryType.LIMIT).model_copy(
        update={"confidence": 0.5})
    await p.say(action, FX, "EUR SHORT 1R Limit Emir")
    p.terminal.set_price("EURUSD", 1.16290, 1.16300)                    # bid below the entry: waits
    p.tickmill.set_price("EURUSD", 1.16295, 1.16305)
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == []
    p.terminal.set_price("EURUSD", 1.16300, 1.16310)                    # bid at the entry
    p.tickmill.set_price("EURUSD", 1.16305, 1.16315)
    await p.engine.monitor.run_virtual_once()
    (order,) = p.tickmill_orders()
    assert order[0] == "market_sell"


# --------------------------------------------------------------------------- #
# restart
# --------------------------------------------------------------------------- #
async def test_restart_keeps_waiting_with_the_broker_reference_and_fires_on_the_fill(env) -> None:
    p, _friend = env
    await _eur_limit(p)
    notifier2 = FakeNotifier()
    engine2 = Engine(p.cfg, p.db, p.parser, list(p.brokers.values()), notifier2, price_oracle=p.engine.prices,
                     reference_prices=p.ref)
    await engine2.start(monitor=False)
    try:
        await engine2.monitor.run_virtual_once()
        assert p.tickmill_orders() == []
        pos = (await p.positions(1))["tickmill"]
        assert is_virtual_pending(pos) and pos.data["reference_broker"] == "fx"
        p.terminal.set_price("EURUSD", 1.16095, 1.16105)
        p.tickmill.set_price("EURUSD", 1.16110, 1.16120)
        p.terminal.fill(_fx_ticket(p), ENTRY)
        await engine2.monitor.run_once()
        await engine2.monitor.run_virtual_once()
        (order,) = p.tickmill_orders()
        assert order[0] == "market_buy" and order[-1] == pytest.approx(TP + 0.00015)
    finally:
        await engine2.stop(grace_sec=1.0)


# --------------------------------------------------------------------------- #
# unchanged
# --------------------------------------------------------------------------- #
async def test_without_reference_broker_the_fx_route_keeps_the_plain_limit(tmp_path: Path,
                                                                          monkeypatch: pytest.MonkeyPatch) -> None:
    p, _friend = await _make(tmp_path, monkeypatch, reference=False)
    try:
        await _eur_limit(p)
        (order,) = p.tickmill_orders()
        assert order[0] == "limit_buy"
        assert not is_virtual_pending((await p.positions(1))["tickmill"])
    finally:
        await p.close()


async def test_with_reference_broker_a_crypto_limit_still_waits_for_binance(env) -> None:
    p, _friend = env
    await p.say(btc(EntryType.LIMIT, 73000.0, 72300.0, 75000.0), KRIPTO, "btc 1R long limit emir")
    pos = (await p.positions(1))["tickmill"]
    assert is_virtual_pending(pos) and pos.data["reference_exchange"] == "binanceusdm"
    assert "reference_broker" not in pos.data
    p.binance.set_price("BTC", 72989.0, 72991.0)
    p.tickmill.set_price("BTCUSD", 72939.0, 72941.0)
    await p.engine.monitor.run_virtual_once()
    assert p.tickmill_orders() == [("market_buy", "BTCUSD", 0.06, 72250.0, 74950.0)]


async def test_fx_market_entries_are_unchanged(env) -> None:
    p, _friend = env
    tps.set_eurusd(p, 1.16235, 1.16245)
    await p.say(fx_pair(*tps.EURUSD_LONG, entry_type=EntryType.MARKET), FX, "EUR LONG 1R")
    assert p.tickmill_orders() == [("market_buy", "EURUSD", 0.18, 1.16, 1.166)]
    assert "price_offset" not in (await p.positions(1))["tickmill"].data


# --------------------------------------------------------------------------- #
# config
# --------------------------------------------------------------------------- #
BASE_YAML = tps.PROD_YAML


def _config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **tickmill: Any) -> AppConfig:
    data = yaml.safe_load(BASE_YAML)
    data["brokers"].append({"name": "fx2", "type": "mt5_bridge", "optional": True,
                            "terminal_path": r"C:\BEM\terminal64.exe", "broker_company": "BEM"})
    for b in data["brokers"]:
        if b["name"] == "tickmill":
            b.update(tickmill)
    monkeypatch.setattr(tps, "PROD_YAML", yaml.safe_dump(data, allow_unicode=True))
    return tps.load_prod_config(tmp_path, monkeypatch)


def test_config_reference_broker_defaults_off_and_names_another_metatrader_account(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _config(tmp_path, monkeypatch)
    assert all(b.reference_broker is None for b in cfg.brokers)
    for name in ("fx", "fx2"):                                          # type mt5 / mt5_bridge
        cfg = _config(tmp_path, monkeypatch, reference_broker=name)
        assert next(b for b in cfg.brokers if b.name == "tickmill").reference_broker == name


@pytest.mark.parametrize("name,why", [
    ("tickmill", "kendisi"), ("nope", "bilinmeyen"), ("binance", "MetaTrader"), ("breakout", "MetaTrader"),
])
def test_config_reference_broker_is_refused_when_it_is_not_another_metatrader_account(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str, why: str
) -> None:
    with pytest.raises(ValueError, match=why):
        _config(tmp_path, monkeypatch, reference_broker=name)


def test_config_reference_broker_must_be_enabled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data = yaml.safe_load(BASE_YAML)
    for b in data["brokers"]:
        if b["name"] == "tickmill":
            b["reference_broker"] = "fx"
        if b["name"] == "fx":
            b["enabled"] = False
    monkeypatch.setattr(tps, "PROD_YAML", yaml.safe_dump(data, allow_unicode=True))
    with pytest.raises(ValueError, match="etkin"):
        tps.load_prod_config(tmp_path, monkeypatch)


