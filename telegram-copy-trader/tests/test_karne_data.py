"""What the report card says (copytrader/karne/data.py), read from a real database file through the engine's own
records: trades as ``Database.create_trade`` / ``update_trade`` store them, positions as the engine leaves them
(``data["initial_risk"]`` written by ``orchestrator.initial_risk_basis``), R by ``monitor.position_result_r``.

Rules under test (the module text of data.py states them):
* a trade is on the card of the period in which it ended as a whole (``closed_at``; start <= closed_at < end);
* R of a trade = the mean of its real accounts with a known R; none known: listed with "—", in no total;
* break-even = within +-0.05R; win rate = wins / (wins + losses); total = sum of the trades' R;
* an account's figure = the sum of its own known R; simulated accounts are in none of it;
* the card of every trade so far (and only that one) gives a trade without a known R an approximate one from the
  planned risk, marked "≈".
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from copytrader.engine.monitor import CLOSING_FLAT_R, INITIAL_RISK, PNL_INCOMPLETE, position_result_r
from copytrader.engine.orchestrator import initial_risk_basis
from copytrader.karne import TITLE
from copytrader.karne.data import (
    APPROX_MAX_R,
    MODE_ALL,
    MODE_INTERIM,
    MODE_WEEK,
    Report,
    Row,
    approximate_result_r,
    build_report,
    caption,
    exit_note,
    fmt_r,
    fmt_rate,
    footnotes,
    heading,
    more_line,
    render_data,
    summary_text,
    trade_row,
)
from copytrader.karne.uktime import last_slot, previous_slot
from copytrader.models import (
    BrokerPosition,
    EntryType,
    MarketType,
    PositionStatus,
    Side,
    SizingResult,
    TakeProfit,
    TradePlan,
    TradeRecord,
    TradeStatus,
)
from copytrader.storage.db import Database

END = datetime(2026, 11, 15, 22, 0, tzinfo=UTC)          # Sunday 22:00 UK time (winter: 22:00 UTC)
START = END - timedelta(days=7)
ACCOUNTS = ["binance", "fx", "tickmill"]


class Clock:
    def __init__(self) -> None:
        self.now = START

    def __call__(self) -> datetime:
        return self.now


@pytest.fixture
async def world(tmp_path: Path):
    clock = Clock()
    db = Database(tmp_path / "copytrader.db", clock=clock)       # a real file, the bot's own storage class
    await db.open()
    yield db, clock
    await db.close()


def plan(asset: str = "ETH", side: Side = Side.LONG, *, risk_r: float = 1.0, entry: float | None = 2000.0,
         stop: float = 1900.0, tps: list[tuple[float, float]] | None = None, quote: str | None = "USDT",
         market: MarketType | None = MarketType.PERP) -> TradePlan:
    levels = [TakeProfit(price=p, close_fraction=f) for p, f in (tps or [(2200.0, 1.0)])]
    return TradePlan(plan_id=f"p-{asset}", base_asset=asset, quote_asset=quote, market_type=market, side=side,
                     entry_type=EntryType.MARKET, entry_price=entry, stop_loss=stop, take_profits=levels,
                     risk_r=risk_r, risk_pct=risk_r * 2.0, source_chat_id=-100, created_at=START)


def position(broker: str, pnl: float | None, *, risk: float | None = 10.0, target: float | None = 10.0,
             side: Side = Side.LONG, status: PositionStatus = PositionStatus.CLOSED, quantity: float = 1.0,
             filled: float | None = None, dry_run: bool = False, data: dict[str, Any] | None = None,
             entry: float | None = 2000.0, stop: float | None = 1900.0) -> BrokerPosition:
    """A position as the engine leaves it. ``risk``: the real initial risk the engine stored at the open (through
    its own ``initial_risk_basis``; None: nothing stored, as for a position of an older version or a limited
    size); ``target``: its ``risk_amount`` (the planned risk the size was computed for)."""
    d = dict(data or {})
    if risk is not None:
        size = SizingResult(quantity=quantity, risk_amount=target or risk, est_loss_at_sl=risk, notional=1000.0)
        basis = initial_risk_basis(size, 1.0, "USDT", "USDT", 2000.0, 2000.0, 1900.0)
        assert basis == {"loss": pytest.approx(risk), "quantity": quantity}
        d[INITIAL_RISK] = basis
    filled = quantity if filled is None else filled
    return BrokerPosition(broker=broker, broker_symbol="ETHUSDT", side=side, status=status, quantity=quantity,
                          filled_quantity=filled, remaining_quantity=0.0 if status is not PositionStatus.OPEN else filled,
                          avg_entry_price=entry, stop_loss=stop, realized_pnl=pnl, risk_amount=target,
                          dry_run=dry_run, data=d)


async def closed(db: Database, clock: Clock, when: datetime, positions: list[BrokerPosition], *,
                 status: TradeStatus = TradeStatus.CLOSED, stop: float | None = None, **plan_kw: Any) -> TradeRecord:
    """A trade that ended at ``when``, stored the way the engine does it: created while open, its positions saved,
    then the final status (which sets ``closed_at`` to the database's clock)."""
    clock.now = when - timedelta(hours=3)
    trade = await db.create_trade(plan(**plan_kw), status=TradeStatus.OPEN)
    if stop is not None:
        await db.update_trade(trade.id, current_stop_loss=stop)
    clock.now = when
    for pos in positions:
        await db.save_broker_position(trade.id, pos)
    return await db.update_trade(trade.id, status=status)


def exits(*kinds: str) -> dict[str, Any]:
    """Adapter data of an exchange position: its booked exits and the take-profit orders that filled."""
    levels = iter((2200.0, 2400.0, 2600.0))
    return {"exits": [{"order_id": str(i), "qty": 1.0, "price": 0.0, "kind": k} for i, k in enumerate(kinds)],
            "tp_order_ids": [{"id": f"t{i}", "price": next(levels), "status": "filled"}
                             for i, k in enumerate(kinds) if k == "tp"]}


def r_of(report: Report) -> list[tuple[str, float | None]]:
    return [(row.pair, None if row.r is None else round(row.r, 6)) for row in report.rows]


# --------------------------------------------------------------------------- #
# wins, losses, break-even; the mean of the accounts; totals and the accounts row
# --------------------------------------------------------------------------- #
async def test_a_week_of_wins_losses_and_a_break_even(world: Any) -> None:
    db, clock = world
    day = START + timedelta(hours=10)
    # ETH: +2.1R on binance, +1.9R on tickmill -> the trade is their mean, +2.0R
    await closed(db, clock, day, [position("binance", 21.0, data=exits("tp")),
                                  position("tickmill", 85.5, risk=45.0, target=45.0, data=exits("tp"))], asset="ETH")
    # EURUSD short on the prop account only: a full stop, the fees make it -1.02R
    await closed(db, clock, day + timedelta(days=1), [position("fx", -102.0, risk=100.0, side=Side.SHORT,
                                                               data=exits("sl"))],
                 asset="EUR", quote="USD", market=MarketType.FOREX, side=Side.SHORT)
    # BTC: closed at about the entry on both: break-even (within 0.05R), counted in neither wins nor losses
    await closed(db, clock, day + timedelta(days=2), [position("binance", 0.4), position("tickmill", -1.8, risk=45.0)],
                 asset="BTC")
    # SOL 0.5R signal that stops out on binance: -0.5R
    await closed(db, clock, day + timedelta(days=3), [position("binance", -5.0, risk=5.0, target=5.0)], asset="SOL",
                 risk_r=0.5)
    # XAU: +3R on tickmill; fx lost its stop early: -1R -> the trade's R is the mean of the two, +1.0R
    await closed(db, clock, day + timedelta(days=4), [position("fx", -100.0, risk=100.0),
                                                      position("tickmill", 135.0, risk=45.0)],
                 asset="XAU", quote="USD", market=MarketType.CFD)
    report = await build_report(db, START, END, accounts=ACCOUNTS)

    assert r_of(report) == [("ETH", 2.0), ("EURUSD", -1.02), ("BTC", 0.0), ("SOL", -0.5), ("XAUUSD", 1.0)]
    assert [row.outcome for row in report.rows] == ["win", "loss", "be", "loss", "win"]
    assert [row.side for row in report.rows] == ["LONG", "SHORT", "LONG", "LONG", "LONG"]
    assert (report.wins, report.losses, report.flat, report.unknown, report.approximate) == (2, 2, 1, 0, 0)
    assert report.win_rate == pytest.approx(50.0)                         # 2 / (2 + 2): the break-even is left out
    assert report.total_r == pytest.approx(2.0 - 1.02 + 0.0 - 0.5 + 1.0)   # the sum of the trades' R
    assert report.best is not None and (report.best.pair, report.best.r) == ("ETH", pytest.approx(2.0))
    assert report.worst is not None and (report.worst.pair, report.worst.r) == ("EURUSD", pytest.approx(-1.02))
    # each account: the sum of its OWN R (they need not add up to the total, which sums the per-trade means)
    assert [(a.name, a.r, a.approx) for a in report.accounts] == [
        ("binance", pytest.approx(2.1 + 0.04 - 0.5), False),
        ("fx", pytest.approx(-1.02 - 1.0), False),
        ("tickmill", pytest.approx(1.9 - 0.04 + 3.0), False)]


@pytest.mark.parametrize("pnl, outcome", [(0.5, "be"), (-0.5, "be"), (0.49, "be"), (0.51, "win"), (-0.51, "loss"),
                                          (0.0, "be")])
async def test_break_even_is_within_five_hundredths_of_an_r_as_in_the_closing_notice(world: Any, pnl: float,
                                                                                    outcome: str) -> None:
    db, clock = world
    assert CLOSING_FLAT_R == 0.05
    await closed(db, clock, START + timedelta(hours=1), [position("binance", pnl)])       # 10 at risk: pnl / 10 R
    report = await build_report(db, START, END)
    assert [row.outcome for row in report.rows] == [outcome]
    assert report.rows[0].r == pytest.approx(pnl / 10.0)                    # the figure itself is never rounded away
    assert (report.wins + report.losses == 0) == (outcome == "be")
    assert (report.win_rate is None) == (outcome == "be")


async def test_partial_take_profits_sum_into_one_r(world: Any) -> None:
    """The owner's example: half closed at 2R (+1R), the rest at 4R (+2R): the trade is +3R. ``realized_pnl`` is the
    sum of the partial closes; one row, one result."""
    db, clock = world
    data = exits("tp", "tp")
    await closed(db, clock, START + timedelta(days=2), [position("binance", 10.0 + 20.0, data=data)],
                 tps=[(2200.0, 0.5), (2400.0, 0.5)])
    report = await build_report(db, START, END, accounts=["binance"])
    assert r_of(report) == [("ETH", 3.0)] and report.total_r == pytest.approx(3.0)
    assert report.rows[0].note == "TP1 · TP2"
    assert [(a.name, a.r) for a in report.accounts] == [("binance", pytest.approx(3.0))]


async def test_a_limit_entry_that_filled_in_part_counts_the_part_that_was_at_risk(world: Any) -> None:
    db, clock = world
    await closed(db, clock, START + timedelta(days=1), [position("binance", -5.0, quantity=1.0, filled=0.5)])
    report = await build_report(db, START, END)
    assert r_of(report) == [("ETH", -1.0)]                                 # half the size lost half the risk: -1R


# --------------------------------------------------------------------------- #
# a trade without a known R: listed with "—", in no total
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("why, pos", [
    ("opened by an older version: no initial risk stored", position("binance", 25.0, risk=None)),
    ("a size the engine limited: no initial risk stored either", position("binance", -3.0, risk=None, target=10.0)),
    ("part of it closed outside the bot's orders", position("binance", 12.0, data={"closed_outside": 0.4})),
    ("the exchange's trade list did not cover the exits", position("binance", 12.0, data={PNL_INCOMPLETE: True})),
    ("a filled MetaTrader leg without a result", position("fx", 12.0, data={"legs": [
        {"state": "closed", "filled": 0.1, "realized": 12.0, "reason": "tp"},
        {"state": "closed", "filled": 0.1, "realized": None, "reason": "sl"}]})),
    ("no realized result at all", position("binance", None)),
    ("simulated only: dry-run", position("binance", 30.0, dry_run=True)),
    ("simulated only: the paper / message-only account", position("breakout", 30.0, data={"simulated": True})),
])
async def test_a_trade_without_a_known_r_is_listed_and_left_out_of_every_figure(world: Any, why: str,
                                                                               pos: BrokerPosition) -> None:
    db, clock = world
    assert position_result_r(pos, 1.0) is None, why
    await closed(db, clock, START + timedelta(days=1), [position("binance", 20.0)], asset="BTC")     # +2R, known
    await closed(db, clock, START + timedelta(days=2), [pos], asset="ETH")
    report = await build_report(db, START, END, accounts=["binance", "fx"])
    assert r_of(report) == [("BTC", 2.0), ("ETH", None)], why
    assert report.rows[1].outcome == "unknown" and report.unknown == 1
    assert (report.total_r, report.wins, report.losses, report.flat) == (pytest.approx(2.0), 1, 0, 0)
    assert report.win_rate == pytest.approx(100.0)
    assert report.best is report.worst is report.rows[0]
    data = render_data(report)
    assert [t["r"] for t in data["trades"]] == [pytest.approx(2.0), None]
    assert [t["outcome"] for t in data["trades"]] == ["win", "unknown"]
    text = summary_text(report)
    assert "— R’si bilinmeyen 1 işlem toplama ve kazanma oranına girmedi." in text
    assert "<b>Toplam: +2.0R</b> (1 işlemin R toplamı)" in text and "ETH LONG · <b>—</b>" in text
    assert "— R’si bilinmeyen 1 işlem toplama ve kazanma oranına girmedi." in caption(report)


async def test_one_known_account_speaks_for_the_trade_and_the_other_account_shows_no_figure(world: Any) -> None:
    db, clock = world
    await closed(db, clock, START + timedelta(days=1), [position("binance", 15.0),
                                                        position("tickmill", 70.0, risk=None, target=45.0)])
    report = await build_report(db, START, END, accounts=ACCOUNTS)
    assert r_of(report) == [("ETH", 1.5)]                                   # the mean of the accounts with a known R
    assert [(a.name, a.r) for a in report.accounts] == [("binance", pytest.approx(1.5)), ("fx", 0.0),
                                                        ("tickmill", None)]
    # binance: its sum; fx: took no trade this week (0); tickmill: traded, but its R is not known ("—", never 0)
    assert [a["r"] for a in render_data(report)["accounts"]] == [pytest.approx(1.5), 0.0, None]
    assert "Hesaplar: binance +1.5R · fx 0.0R · tickmill —" in summary_text(report)


async def test_simulated_accounts_are_in_no_figure_even_next_to_a_real_one(world: Any) -> None:
    db, clock = world
    await closed(db, clock, START + timedelta(days=1), [
        position("binance", 10.0),                                          # +1R, real
        position("breakout", 999.0, data={"simulated": True}),              # the hand-traded account's simulator
        position("kagit", 500.0, dry_run=True)])
    report = await build_report(db, START, END, accounts=["binance"])
    assert r_of(report) == [("ETH", 1.0)] and report.total_r == pytest.approx(1.0)
    assert [(a.name, a.r) for a in report.accounts] == [("binance", pytest.approx(1.0))]
    assert "breakout" not in json.dumps(render_data(report)) and "kagit" not in summary_text(report)


async def test_an_entry_that_never_filled_is_no_trade_result_but_a_failed_open_that_lost_money_is(world: Any) -> None:
    db, clock = world
    await closed(db, clock, START + timedelta(days=1), [
        position("binance", None, status=PositionStatus.CANCELLED, filled=0.0)], status=TradeStatus.CANCELLED,
        asset="DOGE")
    await closed(db, clock, START + timedelta(days=1, hours=1), [
        position("binance", None, status=PositionStatus.FAILED, filled=0.0, risk=None)], status=TradeStatus.FAILED,
        asset="PEPE")
    # the stop could not be placed: the adapter closed the fill at once and reports FAILED with the loss
    await closed(db, clock, START + timedelta(days=2), [
        position("binance", -3.0, status=PositionStatus.FAILED)], status=TradeStatus.FAILED, asset="AVAX")
    report = await build_report(db, START, END)
    assert r_of(report) == [("AVAX", -0.3)] and report.losses == 1


# --------------------------------------------------------------------------- #
# which week: the moment the trade ended as a whole
# --------------------------------------------------------------------------- #
async def test_the_cut_off_is_exact_to_the_second(world: Any) -> None:
    db, clock = world
    await closed(db, clock, START - timedelta(seconds=1), [position("binance", 10.0)], asset="OLD")
    await closed(db, clock, START, [position("binance", 10.0)], asset="FIRST")              # at the cut-off: this week
    await closed(db, clock, END - timedelta(seconds=1), [position("binance", 10.0)], asset="LAST")
    await closed(db, clock, END, [position("binance", 10.0)], asset="NEXT")                 # at the next: next week
    assert [p for p, _r in r_of(await build_report(db, START, END))] == ["FIRST", "LAST"]
    assert [p for p, _r in r_of(await build_report(db, END, END + timedelta(days=7)))] == ["NEXT"]
    assert [p for p, _r in r_of(await build_report(db, START - timedelta(days=7), START))] == ["OLD"]


@pytest.mark.parametrize("sunday, hours, inside, outside", [
    # the clocks go forward on 29 March 2026: the card's Sunday 22:00 is 21:00 UTC, the week has 167 hours
    (datetime(2026, 3, 29, 21, 0, tzinfo=UTC), 167,
     [datetime(2026, 3, 22, 22, 0, tzinfo=UTC), datetime(2026, 3, 29, 20, 59, 59, tzinfo=UTC)],
     [datetime(2026, 3, 22, 21, 59, 59, tzinfo=UTC), datetime(2026, 3, 29, 21, 0, tzinfo=UTC),
      datetime(2026, 3, 29, 21, 30, tzinfo=UTC)]),
    # the clocks go back on 25 October 2026: the card's Sunday 22:00 is 22:00 UTC, the week has 169 hours
    (datetime(2026, 10, 25, 22, 0, tzinfo=UTC), 169,
     [datetime(2026, 10, 18, 21, 0, tzinfo=UTC), datetime(2026, 10, 25, 21, 30, tzinfo=UTC),
      datetime(2026, 10, 25, 21, 59, 59, tzinfo=UTC)],
     [datetime(2026, 10, 18, 20, 59, 59, tzinfo=UTC), datetime(2026, 10, 25, 22, 0, tzinfo=UTC)]),
])
async def test_the_week_around_both_clock_changes(world: Any, sunday: datetime, hours: int,
                                                  inside: list[datetime], outside: list[datetime]) -> None:
    db, clock = world
    assert last_slot(sunday) == sunday
    start = previous_slot(sunday)
    assert sunday - start == timedelta(hours=hours)
    for i, when in enumerate(sorted(inside + outside)):
        await closed(db, clock, when, [position("binance", 10.0)], asset=f"T{i}")
    report = await build_report(db, start, sunday)
    assert sorted(row.closed_at for row in report.rows) == sorted(inside)
    assert report.total_r == pytest.approx(float(len(inside)))
    # no trade is lost and none is counted twice by the neighbouring weeks
    before = await build_report(db, previous_slot(start), start)
    after = await build_report(db, sunday, sunday + timedelta(days=7))
    assert sorted(r.closed_at for r in before.rows + report.rows + after.rows) == sorted(inside + outside)


async def test_a_trade_still_open_on_one_account_at_the_cut_off_belongs_to_the_week_it_ends_in(world: Any) -> None:
    """binance hit its target on Friday, tickmill was still open at Sunday 22:00 and stopped out on Monday: the trade
    is not on the first card at all and is on the second one whole, with both accounts' results."""
    db, clock = world
    clock.now = START + timedelta(days=4)
    trade = await db.create_trade(plan(asset="XAU", quote="USD", market=MarketType.CFD), status=TradeStatus.OPEN)
    clock.now = START + timedelta(days=5)
    await db.save_broker_position(trade.id, position("binance", 20.0))                          # closed: +2R
    await db.save_broker_position(trade.id, position("tickmill", 9.0, risk=45.0, status=PositionStatus.OPEN))
    assert (await db.get_trade(trade.id)).status is TradeStatus.OPEN                            # type: ignore[union-attr]
    first = await build_report(db, START, END, accounts=ACCOUNTS)
    assert first.rows == [] and first.total_r == 0.0
    assert [(a.name, a.r) for a in first.accounts] == [("binance", 0.0), ("fx", 0.0), ("tickmill", 0.0)]

    clock.now = END + timedelta(hours=14)                                                       # Monday
    await db.save_broker_position(trade.id, position("tickmill", -45.0, risk=45.0))             # stopped out: -1R
    await db.update_trade(trade.id, status=TradeStatus.CLOSED)
    assert (await build_report(db, START, END)).rows == []                                      # still not last week's
    second = await build_report(db, END, END + timedelta(days=7), accounts=ACCOUNTS)
    assert r_of(second) == [("XAUUSD", 0.5)]                                                    # (+2 - 1) / 2
    assert second.rows[0].closed_at == END + timedelta(hours=14)
    assert [(a.name, a.r) for a in second.accounts] == [("binance", pytest.approx(2.0)), ("fx", 0.0),
                                                        ("tickmill", pytest.approx(-1.0))]


async def test_an_empty_week(world: Any) -> None:
    db, _clock = world
    report = await build_report(db, START, END, accounts=ACCOUNTS)
    assert report.rows == [] and report.total_r == 0.0 and report.win_rate is None
    assert report.best is None and report.worst is None and report.unknown == 0
    assert [(a.name, a.r) for a in report.accounts] == [("binance", 0.0), ("fx", 0.0), ("tickmill", 0.0)]
    data = render_data(report)
    assert data["trades"] == [] and data["subtitle"] == "HAFTALIK KARNE · 8–15 Kasım 2026"
    text = summary_text(report)
    assert "<b>Toplam: 0.0R</b> (0 işlemin R toplamı)" in text and "Bu dönemde kapanan işlem yok." in text
    assert "kazanma" not in text.split("Hesaplar")[0]                       # no rate where nothing was decided
    assert "Toplam 0.0R · 0 işlem · kazanma —" in caption(report)


# --------------------------------------------------------------------------- #
# what goes to the drawing process and into the text card
# --------------------------------------------------------------------------- #
async def test_the_card_data_has_no_money_and_is_never_marked_as_sample_data(world: Any) -> None:
    db, clock = world
    await closed(db, clock, START + timedelta(days=1, hours=3), [position("binance", 1234.56, risk=617.28,
                                                                          data=exits("tp"))])
    await closed(db, clock, START + timedelta(days=6, hours=23, minutes=30), [
        position("fx", -9876.54, risk=9876.54, side=Side.SHORT, data=exits("sl"))],
        asset="GBP", quote="USD", market=MarketType.FOREX, side=Side.SHORT)
    report = await build_report(db, START, END, accounts=["binance", "fx"])
    data = render_data(report)
    assert json.loads(json.dumps(data, allow_nan=False)) == data            # plain JSON for the other process
    assert data["sample"] is False and data["title"] == TITLE == "BABÜR'S TRADE PERFORMANCE"
    assert data["subtitle"] == "HAFTALIK KARNE · 8–15 Kasım 2026" and data["footer"] == ""
    assert data["trades"] == [
        {"date": "2026-11-10", "pair": "ETH", "side": "LONG", "r": pytest.approx(2.0), "outcome": "win",
         "note": "TP1", "approx": False, "partial": False},
        {"date": "2026-11-15", "pair": "GBPUSD", "side": "SHORT", "r": pytest.approx(-1.0), "outcome": "loss",
         "note": "SL", "approx": False, "partial": False}]
    assert data["accounts"] == [{"name": "binance", "r": pytest.approx(2.0), "approx": False, "partial": False},
                                {"name": "fx", "r": pytest.approx(-1.0), "approx": False, "partial": False}]
    # the period's figures are GIVEN to the picture (it adds nothing up by itself): the same as in the texts
    assert data["aggregates"] == {
        "wins": 1, "losses": 1, "be": 0, "unknown": 0, "approx": 0, "partial": 0, "total_r": pytest.approx(1.0),
        "win_rate": pytest.approx(50.0),
        "best": {"pair": "ETH", "side": "LONG", "r": pytest.approx(2.0), "approx": False},
        "worst": {"pair": "GBPUSD", "side": "SHORT", "r": pytest.approx(-1.0), "approx": False}}
    assert data["hidden"] == 0 and data["footnotes"] == [] and "more_text" not in data
    for text in (json.dumps(data, ensure_ascii=False), summary_text(report), caption(report)):
        for money in ("1234", "617", "9876", "$", "usd", "USDT", "K/Z"):
            assert money not in text, (money, text)
        assert "ÖRNEK" not in text
    assert summary_text(report).splitlines() == [
        "📊 <b>BABÜR'S TRADE PERFORMANCE</b>",
        "Haftalık karne · 8–15 Kasım 2026",
        "Dönem: 08.11 Paz 22:00 – 15.11 Paz 22:00 (İngiltere saati)",
        "",
        "<b>Toplam: +1.0R</b> (2 işlemin R toplamı)",
        "1 kazanç · 1 kayıp · kazanma oranı %50",
        "En iyi: +2.0R ETH LONG · En kötü: −1.0R GBPUSD SHORT",
        "Hesaplar: binance +2.0R · fx −1.0R",
        "",
        "<b>İşlemler</b>",
        "10.11 Sal · ETH LONG · <b>+2.0R</b> · TP1",
        "15.11 Paz · GBPUSD SHORT · <b>−1.0R</b> · SL"]
    assert caption(report).splitlines() == [
        "📊 <b>Haftalık karne · 8–15 Kasım 2026</b>",
        "Dönem: 08.11 Paz 22:00 – 15.11 Paz 22:00 (İngiltere saati)",
        "Toplam +1.0R · 2 işlem (1 kazanç · 1 kayıp) · kazanma %50"]
    assert summary_text(report, "ℹ️ neden resim yok").endswith("\n\nℹ️ neden resim yok")


async def test_the_row_date_is_the_uk_date_the_trade_ended_on(world: Any) -> None:
    db, clock = world
    summer_end = datetime(2026, 7, 12, 21, 0, tzinfo=UTC)
    await closed(db, clock, datetime(2026, 7, 7, 23, 30, tzinfo=UTC), [position("binance", 10.0)])   # 00:30 on the 8th
    report = await build_report(db, previous_slot(summer_end), summer_end)
    assert render_data(report)["trades"][0]["date"] == "2026-07-08"
    assert "08.07 Çar · ETH LONG" in summary_text(report)


def test_the_figures_as_text() -> None:
    assert [fmt_r(v) for v in (6.2, -1.0, 0.0, 0.04, -0.04, 0.05, 12.349, -0.96)] == [
        "+6.2R", "−1.0R", "0.0R", "0.0R", "0.0R", "+0.1R", "+12.3R", "−1.0R"]
    assert fmt_r(2.14, approx=True) == "≈+2.1R" and fmt_r(-0.5, True) == "≈−0.5R"
    assert [fmt_rate(v) for v in (62.5, 100.0, 0.0, 33.333, None)] == ["%63", "%100", "%0", "%33", "—"]
    assert fmt_rate(66.7, approx=True) == "≈%67"


async def test_a_card_that_covers_more_than_one_week_says_so(world: Any) -> None:
    db, clock = world
    await closed(db, clock, START - timedelta(days=3), [position("binance", 10.0)])
    report = await build_report(db, START - timedelta(days=7), END, accounts=["binance"])
    assert report.weeks == 2 and heading(report) == "2 HAFTALIK KARNE · 1–15 Kasım 2026"
    data = render_data(report)
    assert (data["hero_label"], data["table_label"]) == ("DÖNEM SONUCU", "DÖNEMİN İŞLEMLERİ")
    assert "Önceki karne gönderilemediği için bu karne 2 haftayı kapsıyor." in caption(report)
    late = await build_report(db, START, END, late=True)
    assert "Bu karne pazar 22:00'de gönderilemedi; şimdi gönderildi." in caption(late)
    interim = await build_report(db, START, START + timedelta(days=3, hours=16, minutes=5), mode=MODE_INTERIM)
    assert heading(interim) == "ARA KARNE · 8–12 Kasım 2026"
    assert "Dönem: 08.11 Paz 22:00 – 12.11 Per 14:05 (İngiltere saati)" in caption(interim)
    assert "Hafta sürüyor: yalnızca tamamen kapanmış işlemler sayıldı, açık işlemler dahil değil." in caption(interim)


# --------------------------------------------------------------------------- #
# the note column: only what the adapters recorded, only where the accounts agree
# --------------------------------------------------------------------------- #
def record(positions: list[BrokerPosition], *, stop: float = 1900.0, plan_stop: float | None = None,
           **plan_kw: Any) -> str:
    """The note of a trade with ``positions``. ``stop``: the TRADE's present stop (the trader's wish);
    ``plan_stop``: the signal's own stop (default: the plan helper's)."""
    p = plan(**({"stop": plan_stop} if plan_stop is not None else {}), **plan_kw)
    trade = TradeRecord(id=1, plan=p, status=TradeStatus.CLOSED, current_stop_loss=stop,
                        current_take_profits=list(p.take_profits), created_at=START, updated_at=START, closed_at=START)
    return exit_note(trade, positions)


def legs(*reasons: str, tps: tuple[float, ...] = (2200.0, 2400.0)) -> dict[str, Any]:
    return {"legs": [{"state": "closed", "filled": 0.1, "realized": 1.0, "reason": r,
                      "tp": tps[min(i, len(tps) - 1)]} for i, r in enumerate(reasons)]}


def test_notes_from_an_exchange_position() -> None:
    two = {"tps": [(2200.0, 0.5), (2400.0, 0.5)]}
    assert record([position("binance", 20.0, data=exits("tp"))]) == "TP1"
    assert record([position("binance", 30.0, data=exits("tp", "tp"))], **two) == "TP1 · TP2"
    assert record([position("binance", -10.0, data=exits("sl"))]) == "SL"                  # the signal's own stop
    # the stop had been moved to the entry (the engine stores the plan's entry as the trade's stop): break-even
    assert record([position("binance", 0.1, data=exits("sl"), stop=2000.0)], stop=2000.0) == "BE"
    assert record([position("binance", 5.0, data=exits("tp", "sl"), stop=2000.0)], stop=2000.0, **two) == "TP1 · BE"
    assert record([position("binance", 4.0, data=exits("sl"), stop=2050.0)], stop=2050.0) == "stop"     # moved elsewhere
    assert record([position("binance", 3.0, data=exits("close"))]) == "kapatıldı"          # the trader's close
    assert record([position("binance", 9.0, data=exits("tp", "close"))], **two) == "TP1 · kapatıldı"
    assert record([position("binance", -4.0, data=exits("close", "sl"))]) == "kapatıldı · SL"


def test_notes_from_a_metatrader_position() -> None:
    two = {"tps": [(2200.0, 0.5), (2400.0, 0.5)]}
    assert record([position("fx", 30.0, data=legs("tp", "tp"))], **two) == "TP1 · TP2"
    assert record([position("fx", 5.0, data=legs("tp", "sl"))], **two) == "TP1 · SL"
    assert record([position("fx", -10.0, data=legs("sl", "sl"))], **two) == "SL"
    assert record([position("fx", -30.0, data=legs("stop_out"))]) == "stop-out"
    assert record([position("fx", 2.0, data=legs("manual", "manual"))], **two) == "kapatıldı"
    never = legs("tp")
    never["legs"].append({"state": "cancelled", "filled": 0.0, "reason": "cancelled", "tp": 2400.0})
    assert record([position("fx", 10.0, data=never)], **two) == "TP1"               # a leg that never filled: silent


def test_a_take_profit_at_a_level_the_trade_no_longer_has_is_just_tp() -> None:
    moved = exits("tp")
    moved["tp_order_ids"][0]["price"] = 2333.0                                       # the targets were changed later
    assert record([position("binance", 12.0, data=moved)]) == "TP"


@pytest.mark.parametrize("data", [
    {},                                                                # nothing recorded (a hand-traded account)
    {"exits": [{"kind": "emergency", "qty": 1.0}]},                    # a kind this does not put a word to
    {"exits": ["garbage"]},
    {"legs": [{"state": "closed", "filled": 0.1, "reason": "unknown"}]},
    {"legs": [{"state": "closed", "filled": 0.1, "reason": None}]},
    {"legs": [{"state": "open", "filled": 0.1, "reason": "tp"}]},
    {"legs": ["garbage"]},
    {**exits("sl"), "closed_outside": 0.5},                            # part of it ended outside the bot's orders
])
def test_what_is_not_recorded_clearly_gives_no_note(data: dict[str, Any]) -> None:
    assert record([position("binance", 1.0, data=data)]) == ""


def test_accounts_that_ended_differently_give_no_note_and_simulated_ones_do_not_count() -> None:
    assert record([position("binance", 20.0, data=exits("tp")), position("fx", -10.0, data=legs("sl"))]) == ""
    assert record([position("binance", 20.0, data=exits("tp")), position("fx", 20.0, data=legs("tp"))]) == "TP1"
    # the hand-traded account's simulator says "stop": not asked, the real account speaks
    assert record([position("binance", 20.0, data=exits("tp")),
                   position("breakout", -1.0, data={"simulated": True, **exits("sl")})]) == "TP1"
    assert record([position("kagit", -1.0, data={"simulated": True, **exits("stop")})]) == "SL"     # only simulated
    # an account that never filled has nothing to say
    assert record([position("binance", 20.0, data=exits("tp")),
                   position("fx", None, status=PositionStatus.CANCELLED, filled=0.0)]) == "TP1"


def test_a_note_never_raises() -> None:
    broken = position("binance", 1.0, data={"exits": [{"kind": "tp"}], "tp_order_ids": "x", "price_multiplier": "?"})
    assert record([broken]) in ("", "TP")


# --------------------------------------------------------------------------- #
# /karne hepsi: every trade so far; approximate R where the old positions store no real risk
# --------------------------------------------------------------------------- #
def test_the_approximate_r_of_an_old_position() -> None:
    old = position("binance", 25.0, risk=None, target=10.0)                  # planned 10 at risk, made 25
    assert position_result_r(old, 1.0) is None and approximate_result_r(old, 1.0) == pytest.approx(2.5)
    assert approximate_result_r(old, 0.5) == pytest.approx(1.25)             # a 0.5R signal: half the R
    assert approximate_result_r(position("binance", -10.0, risk=None, target=10.0), 1.0) == pytest.approx(-1.0)
    # the same exclusions as the exact figure: simulated, partial results, no result
    for excluded in (position("binance", 25.0, risk=None, dry_run=True),
                     position("breakout", 25.0, risk=None, data={"simulated": True}),
                     position("binance", 25.0, risk=None, data={"closed_outside": 0.3}),
                     position("binance", 25.0, risk=None, data={PNL_INCOMPLETE: True}),
                     position("fx", 25.0, risk=None, data={"legs": [{"filled": 0.1, "realized": None}]}),
                     position("binance", None, risk=None)):
        assert approximate_result_r(excluded, 1.0) is None
    for target in (None, 0.0, -5.0, float("nan"), float("inf")):
        assert approximate_result_r(position("binance", 25.0, risk=None, target=target), 1.0) is None
    for risk_r in (0.0, -1.0, float("nan")):
        assert approximate_result_r(old, risk_r) is None
    # (review, P4) a size the adapter marked as limited risked a fraction of the plan: its figure would turn a 2R
    # target into "≈+5.7R" - no approximation, the trade reads "—"
    assert approximate_result_r(position("fx", 575.0, risk=None, target=100.0, data={"size_capped": True}), 1.0) is None
    assert approximate_result_r(position("fx", -25.0, risk=None, target=100.0, data={"size_capped": True}), 1.0) is None
    # (review, P4) an implausible value (a planned risk of next to nothing) is not a figure either
    assert APPROX_MAX_R == 20.0
    assert approximate_result_r(position("binance", -5.0, risk=None, target=1e-9), 1.0) is None
    assert approximate_result_r(position("binance", 201.0, risk=None, target=10.0), 1.0) is None
    assert approximate_result_r(position("binance", 200.0, risk=None, target=10.0), 1.0) == pytest.approx(20.0)
    assert approximate_result_r(position("binance", -32.0, risk=None, target=10.0), 1.0) == pytest.approx(-3.2)


async def old_and_new(db: Database, clock: Clock) -> None:
    first = START - timedelta(days=30)
    await closed(db, clock, first, [position("binance", 25.0, risk=None, target=10.0, data=exits("tp"))],
                 asset="BTC")                                                # old version: ≈ +2.5R
    await closed(db, clock, first + timedelta(days=1), [
        position("binance", -10.0, risk=None, target=10.0), position("tickmill", -36.0, risk=None, target=45.0)],
        asset="SOL")                                                         # old, two accounts: ≈ (-1 - 0.8) / 2
    await closed(db, clock, first + timedelta(days=2), [
        position("binance", 5.0, risk=None, data={"closed_outside": 0.2})], asset="DOGE")     # partial: unknown
    await closed(db, clock, first + timedelta(days=3), [position("kagit", 50.0, risk=None, dry_run=True)],
                 asset="PEPE")                                               # simulated only: unknown
    await closed(db, clock, START + timedelta(days=1), [
        position("binance", 21.0), position("tickmill", 103.5, risk=None, target=45.0)], asset="ETH")
    # ^ the first trade of the version that stores the real risk: binance exact +2.1R; tickmill without a stored
    #   risk (a size the engine limited): no approximation for it on any card, the row says "*"
    await closed(db, clock, START + timedelta(days=2), [position("fx", -100.0, risk=100.0)], asset="EUR",
                 quote="USD", market=MarketType.FOREX, side=Side.SHORT)      # this version: -1.0R


async def test_the_card_of_every_trade_marks_what_is_approximate(world: Any) -> None:
    db, clock = world
    await old_and_new(db, clock)
    now = END - timedelta(days=2)
    report = await build_report(db, None, now, mode=MODE_ALL, accounts=ACCOUNTS)
    assert report.mode == MODE_ALL and report.start is None
    # newest first; approximate figures for the trades of the older version only (BTC, SOL), marked
    assert await db.first_trade_with_position_data(INITIAL_RISK) == 5       # ETH: the first with a stored real risk
    assert [(row.pair, None if row.r is None else round(row.r, 6), row.approx) for row in report.rows] == [
        ("EURUSD", -1.0, False), ("ETH", 2.1, False), ("PEPE", None, False), ("DOGE", None, False),
        ("SOL", -0.9, True), ("BTC", 2.5, True)]
    # (DOGE is missing from binance's sum, tickmill from ETH's row and from its own sum)
    assert (report.approximate, report.unknown, report.partial) == (2, 2, 2)
    assert report.total_r == pytest.approx(-1.0 + 2.1 - 0.9 + 2.5)
    assert (report.wins, report.losses) == (2, 2) and report.win_rate == pytest.approx(50.0)
    assert [(a.name, None if a.r is None else round(a.r, 6), a.approx, a.partial) for a in report.accounts] == [
        ("binance", round(2.1 - 1.0 + 2.5, 6), True, True),     # two approximate figures in it, DOGE not in it
        ("fx", -1.0, False, False),                             # exact only, complete: no mark
        ("tickmill", -0.8, True, True)]                         # SOL approximate, ETH not in it
    data = render_data(report)
    assert data["subtitle"] == "TÜM İŞLEMLER · 9 Ekim – 13 Kasım 2026"       # the real dates: first trade .. now
    assert (data["hero_label"], data["table_label"], data["empty_text"]) == (
        "TOPLAM SONUÇ", "İŞLEMLER", "Henüz kapanmış işlem yok")
    assert [(t["pair"], t["approx"], t["partial"]) for t in data["trades"]] == [
        ("EURUSD", False, False), ("ETH", False, True), ("PEPE", False, False), ("DOGE", False, False),
        ("SOL", True, False), ("BTC", True, False)]
    assert [a["approx"] for a in data["accounts"]] == [True, False, True]
    text = summary_text(report)
    assert "<b>Toplam: ≈+2.7R</b> (4 işlemin R toplamı)" in text            # a total with an approximate part: marked
    assert "2 kazanç · 2 kayıp · kazanma oranı ≈%50" in text
    assert "En iyi: ≈+2.5R BTC LONG · En kötü: −1.0R EURUSD SHORT" in text
    assert "Hesaplar: binance ≈+3.6R* · fx −1.0R · tickmill ≈−0.8R*" in text
    assert "SOL LONG · <b>≈−0.9R</b>" in text and "ETH LONG · <b>+2.1R*</b>" in text and "PEPE LONG · <b>—</b>" in text
    assert text.index("EURUSD SHORT ·") < text.index("BTC LONG · <b>≈")      # newest first
    # the SAME sentences under the picture's table, in its caption and in the text card
    notes = ["≈ yaklaşık: 2 işlemde gerçek risk kayıtlı değil; R, hesabın planlanan riskine göre hesaplandı",
             "* 2 işlemde R’si bilinmeyen hesap, işlemin ortalamasına ve kendi toplamına girmedi",
             "— R’si bilinmeyen 2 işlem toplama ve kazanma oranına girmedi"]
    assert footnotes(report) == data["footnotes"] == notes
    for note in notes:
        assert f"{note}." in text.splitlines() and f"{note}." in caption(report).splitlines()
    assert "Dönem: ilk işlemden 13.11.2026 Cum 22:00 anına kadar (İngiltere saati)" in text
    assert caption(report).splitlines()[:3] == [
        "📊 <b>Tüm işlemler · 9 Ekim – 13 Kasım 2026</b>",
        "Dönem: ilk işlemden 13.11.2026 Cum 22:00 anına kadar (İngiltere saati)",
        "Toplam ≈+2.7R · 4 işlem (2 kazanç · 2 kayıp) · kazanma ≈%50"]
    assert data["aggregates"]["approx"] == 2 and data["aggregates"]["total_r"] == pytest.approx(report.total_r)


async def test_the_weekly_card_never_approximates(world: Any) -> None:
    db, clock = world
    await old_and_new(db, clock)
    for mode in (MODE_WEEK, MODE_INTERIM):
        report = await build_report(db, START - timedelta(days=40), END, mode=mode, accounts=ACCOUNTS)
        assert [(row.pair, None if row.r is None else round(row.r, 6), row.approx) for row in report.rows] == [
            ("BTC", None, False), ("SOL", None, False), ("DOGE", None, False), ("PEPE", None, False),
            ("ETH", 2.1, False), ("EURUSD", -1.0, False)], mode
        assert report.approximate == 0 and report.unknown == 4 and report.total_r == pytest.approx(1.1)
        assert [(a.name, a.r, a.approx) for a in report.accounts] == [
            ("binance", pytest.approx(2.1), False), ("fx", pytest.approx(-1.0), False), ("tickmill", None, False)]
        assert "≈" not in summary_text(report) + caption(report) + json.dumps(render_data(report), ensure_ascii=False)
        # ETH: tickmill's R is not known here, so the row is binance's alone and says so ("*")
        assert "ETH LONG · <b>+2.1R*</b>" in summary_text(report)
        # ... and binance's own sum leaves out BTC, SOL and DOGE: four trades with an account missing somewhere
        assert report.partial == 4 and "Hesaplar: binance +2.1R* · fx −1.0R · tickmill —" in summary_text(report)


async def test_every_trade_so_far_without_a_trade(world: Any) -> None:
    db, _clock = world
    report = await build_report(db, None, END, mode=MODE_ALL, accounts=["binance"])
    assert report.rows == [] and render_data(report)["subtitle"] == "TÜM İŞLEMLER · 15 Kasım 2026"
    assert "Henüz kapanmış işlem yok." in summary_text(report)


async def test_the_text_card_lists_the_newest_trades_and_counts_the_rest(world: Any) -> None:
    db, clock = world
    for i in range(45):
        await closed(db, clock, START + timedelta(hours=i + 1), [position("binance", 10.0)], asset=f"C{i:02d}")
    report = await build_report(db, None, END, mode=MODE_ALL)
    text = summary_text(report)
    assert "C44 LONG" in text and "C05 LONG" in text and "C04 LONG" not in text
    assert "… ve 5 işlem daha (toplama dahil)" in text and "<b>Toplam: +45.0R</b> (45 işlemin R toplamı)" in text
    data = render_data(report)                     # the picture gets the 19 rows it draws and the period's figures
    assert [t["pair"] for t in data["trades"]] == [f"C{i:02d}" for i in range(44, 25, -1)] and data["hidden"] == 26
    assert data["more_text"] == "… ve 26 işlem daha (toplama dahil)"
    assert data["aggregates"]["wins"] == 45 and data["aggregates"]["total_r"] == pytest.approx(45.0)


async def test_a_week_with_more_trades_than_the_table_holds_shows_the_most_recent_ones(world: Any) -> None:
    """The picture's table shows the first rows it is given (19 of them once there are more than 20) and counts the
    rest: so a long week is handed over newest first; a week that fits stays oldest first, as the design shows it."""
    db, clock = world
    for i in range(20):
        await closed(db, clock, START + timedelta(hours=i + 1), [position("binance", 10.0)], asset=f"C{i:02d}")
    fits = await build_report(db, START, END)
    assert [t["pair"] for t in render_data(fits)["trades"]] == [f"C{i:02d}" for i in range(20)]
    await closed(db, clock, START + timedelta(hours=30), [position("binance", -10.0)], asset="C20")
    long_week = await build_report(db, START, END)
    assert [row.pair for row in long_week.rows] == [f"C{i:02d}" for i in range(21)]          # the report: oldest first
    data = render_data(long_week)
    assert [t["pair"] for t in data["trades"]] == [f"C{i:02d}" for i in range(20, 1, -1)]     # the 19 most recent
    assert data["hidden"] == 2 and data["more_text"] == "… ve 2 işlem daha (toplama dahil)"
    assert long_week.total_r == pytest.approx(19.0)                                            # every trade counts
    assert data["aggregates"]["total_r"] == pytest.approx(19.0) and data["aggregates"]["wins"] == 20
    interim = await build_report(db, START, END, mode=MODE_INTERIM)
    assert render_data(interim)["trades"][0]["pair"] == "C20"


def test_a_row_needs_an_ended_trade_that_someone_traded() -> None:
    p = plan()
    open_trade = TradeRecord(id=1, plan=p, status=TradeStatus.OPEN, current_stop_loss=1900.0, created_at=START,
                             updated_at=START)
    assert trade_row(open_trade, [position("binance", 10.0)]) is None
    ended = open_trade.model_copy(update={"status": TradeStatus.CLOSED, "closed_at": START})
    assert trade_row(ended, []) is None
    row = trade_row(ended, [position("binance", 10.0)])
    assert row is not None and row.r == pytest.approx(1.0) and row.accounts == {"binance": (pytest.approx(1.0), False)}


# --------------------------------------------------------------------------- #
# Review round (data): the reviewer's reproductions, as regression tests
# --------------------------------------------------------------------------- #
async def test_review_p2_an_account_missing_from_a_row_or_from_its_own_sum_is_marked_and_counted(world: Any) -> None:
    """krev/data/acct_incomplete.py: fx has trade 1 at -1R (known) and trade 2 with a limited size that lost money (R
    unknown); binance made +2R in trade 2. The card used to show "BTC +2.0R" and "fx −1.0R" as if nothing was
    missing."""
    db, clock = world
    await closed(db, clock, END - timedelta(days=3), [position("fx", -10.0)], asset="EUR", quote="USD")
    await closed(db, clock, END - timedelta(days=2), [position("binance", 20.0),
                                                      position("fx", -25.0, risk=None, target=100.0)], asset="BTC")
    report = await build_report(db, START, END, accounts=["binance", "fx"])
    assert r_of(report) == [("EUR", -1.0), ("BTC", 2.0)]
    assert [(row.partial, row.left_out) for row in report.rows] == [(False, ()), (True, ("fx",))]
    assert [(a.name, a.r, a.partial) for a in report.accounts] == [("binance", pytest.approx(2.0), False),
                                                                   ("fx", pytest.approx(-1.0), True)]
    assert (report.unknown, report.partial) == (0, 1)
    note = "* 1 işlemde R’si bilinmeyen hesap, işlemin ortalamasına ve kendi toplamına girmedi"
    text, data = summary_text(report), render_data(report)
    assert "BTC LONG · <b>+2.0R*</b>" in text and "EUR LONG · <b>−1.0R</b>" in text
    assert "Hesaplar: binance +2.0R · fx −1.0R*" in text
    assert f"{note}." in text.splitlines() and f"{note}." in caption(report).splitlines()
    assert data["footnotes"] == [note]
    assert [t["partial"] for t in data["trades"]] == [False, True]
    assert [a["partial"] for a in data["accounts"]] == [False, True] and data["aggregates"]["partial"] == 1


async def test_review_p2_an_account_sum_that_leaves_out_a_wholly_unknown_trade_is_marked_too(world: Any) -> None:
    db, clock = world
    await closed(db, clock, END - timedelta(days=3), [position("fx", -10.0)], asset="EUR", quote="USD")     # -1R
    await closed(db, clock, END - timedelta(days=2), [position("fx", -25.0, risk=None)], asset="GBP", quote="USD")
    await closed(db, clock, END - timedelta(days=1), [position("tickmill", 5.0, risk=None)], asset="XAU")
    report = await build_report(db, START, END, accounts=["fx", "tickmill"])
    assert r_of(report) == [("EUR", -1.0), ("GBP", None), ("XAU", None)]
    # fx: a sum, with a trade left out ("*"); tickmill: no figure at all ("—", nothing to mark)
    assert [(a.name, a.r, a.partial) for a in report.accounts] == [("fx", pytest.approx(-1.0), True),
                                                                   ("tickmill", None, False)]
    assert (report.unknown, report.partial) == (2, 1)           # GBP is missing from fx's sum; XAU from no figure
    assert footnotes(report) == [
        "* 1 işlemde R’si bilinmeyen hesap, işlemin ortalamasına ve kendi toplamına girmedi",
        "— R’si bilinmeyen 2 işlem toplama ve kazanma oranına girmedi"]
    assert "Hesaplar: fx −1.0R* · tickmill —" in summary_text(report)


async def test_review_p4_p8_every_trade_so_far_limited_sizes_absurd_values_and_a_mixed_trade(world: Any) -> None:
    """krev/data/misc_checks.py (b), (c): a minimum lot that risked more than planned reads ≈−3.2R (marked); a tiny
    planned risk used to read "≈−5000000000.0R"; a mixed trade used to leave an account with "≈" and no footnote."""
    db, clock = world
    await closed(db, clock, END - timedelta(days=3), [position("binance", -32.0, risk=None, target=10.0)],
                 asset="MINLOT")
    await closed(db, clock, END - timedelta(days=2), [position("binance", -5.0, risk=None, target=1e-9)], asset="TINY")
    await closed(db, clock, END - timedelta(days=2, hours=1), [position("binance", 5.0, risk=None, target=0.0)],
                 asset="ZERO")
    await closed(db, clock, END - timedelta(days=1, hours=12), [
        position("fx", 575.0, risk=None, target=100.0, data={"size_capped": True})], asset="CAPPED")
    await closed(db, clock, END - timedelta(days=1), [position("binance", 20.0),
                                                      position("fx", -30.0, risk=None, target=10.0)], asset="MIX")
    # MIX is the first trade of the version that stores the real risk (binance has it): its fx position has none for
    # a reason (a size the engine limited) and gets no approximation - the mixed trade of the review cannot exist
    report = await build_report(db, None, END, mode=MODE_ALL, accounts=["binance", "fx"])
    assert [(row.pair, None if row.r is None else round(row.r, 6), row.approx, row.partial) for row in report.rows] == [
        ("MIX", 2.0, False, True), ("CAPPED", None, False, False), ("TINY", None, False, False),
        ("ZERO", None, False, False), ("MINLOT", -3.2, True, False)]
    assert all(row.r is None or abs(row.r) <= APPROX_MAX_R for row in report.rows)
    text = summary_text(report)
    assert "5000000000" not in text and "+5.7" not in text and "+5.8" not in text
    assert "<b>Toplam: ≈−1.2R</b> (2 işlemin R toplamı)" in text
    # P8: an account figure with "≈" always comes with the footnote: a row is approximate whenever one of its
    # accounts' figures is, and the footnote counts those rows
    assert [(a.name, None if a.r is None else round(a.r, 6), a.approx, a.partial) for a in report.accounts] == [
        ("binance", round(2.0 - 3.2, 6), True, True), ("fx", None, False, False)]
    assert "Hesaplar: binance ≈−1.2R* · fx —" in text
    for card in (report, await build_report(db, None, END - timedelta(days=1, hours=1), mode=MODE_ALL)):
        rough_accounts = any(a.approx for a in card.accounts)
        assert rough_accounts == any(row.approx for row in card.rows) == any(n.startswith("≈") for n in footnotes(card))
    notes = footnotes(report)
    assert notes[0] == "≈ yaklaşık: 1 işlemde gerçek risk kayıtlı değil; R, hesabın planlanan riskine göre hesaplandı"
    assert notes[1] == "* 3 işlemde R’si bilinmeyen hesap, işlemin ortalamasına ve kendi toplamına girmedi"
    assert notes[2] == "— R’si bilinmeyen 3 işlem toplama ve kazanma oranına girmedi"
    assert render_data(report)["footnotes"] == notes                        # the picture says the same (P4)
    week = await build_report(db, START, END, accounts=["binance", "fx"])   # the weekly card: strict
    assert [(row.pair, row.r, row.approx, row.partial) for row in week.rows] == [
        ("MINLOT", None, False, False), ("ZERO", None, False, False), ("TINY", None, False, False),
        ("CAPPED", None, False, False), ("MIX", pytest.approx(2.0), False, True)]


def _rows(*values: float | None, start: int = 0) -> list[Row]:
    return [Row(trade_id=start + i + 1, closed_at=START + timedelta(hours=start + i + 1), pair=f"P{start + i:02d}",
                side="LONG", r=value, accounts={"binance": (value, False)}) for i, value in enumerate(values)]


def test_review_p5_best_and_worst_are_the_same_trade_on_the_picture_and_in_the_text() -> None:
    """With more than 20 trades the picture gets its rows newest first; on a tie it used to name another trade than
    the text. Of equal results the one that ended first is named, everywhere."""
    values = [1.0, 2.9, -1.0, 0.5, 2.9, -1.0] + [0.3] * 17                  # 23 trades; ties for best and worst
    week = Report(MODE_WEEK, START, END, _rows(*values), [])
    assert week.best is not None and week.worst is not None
    assert (week.best.pair, week.worst.pair) == ("P01", "P02")
    data = render_data(week)
    assert data["trades"][0]["pair"] == "P22"                               # (handed over newest first)
    assert (data["aggregates"]["best"]["pair"], data["aggregates"]["worst"]["pair"]) == ("P01", "P02")
    assert "En iyi: +2.9R P01 LONG · En kötü: −1.0R P02 LONG" in summary_text(week)
    everything = Report(MODE_ALL, None, END, list(reversed(_rows(*values))), [])
    assert (everything.best.pair, everything.worst.pair) == ("P01", "P02")  # type: ignore[union-attr]
    assert render_data(everything)["aggregates"]["best"]["pair"] == "P01"


def test_review_p6_the_line_that_counts_the_hidden_rows_stays_true_when_some_have_no_r() -> None:
    assert more_line(_rows(1.0, -1.0, 0.0)) == "… ve 3 işlem daha (toplama dahil)"
    assert more_line(_rows(1.0, None, 0.5)) == "… ve 3 işlem daha (2 tanesi toplama dahil, 1 tanesinin R’si bilinmiyor)"
    assert more_line(_rows(None, None)) == "… ve 2 işlem daha (R’leri bilinmiyor: toplamda yok)"
    # the picture's table (19 rows drawn of 22, newest first): the hidden three are the oldest, one of them "—"
    week = Report(MODE_WEEK, START, END, _rows(1.0, None, 1.0, *([0.5] * 19)), [])
    data = render_data(week)
    assert data["hidden"] == 3
    assert data["more_text"] == "… ve 3 işlem daha (2 tanesi toplama dahil, 1 tanesinin R’si bilinmiyor)"
    # the text card (40 rows listed of 43): the same sentence for its own hidden rows
    long = Report(MODE_WEEK, START, END, _rows(None, 1.0, None, *([0.5] * 40)), [])
    assert "… ve 3 işlem daha (1 tanesi toplama dahil, 2 tanesinin R’si bilinmiyor)" in summary_text(long)


@pytest.mark.parametrize("value, text", [(0.05, "0.0R"), (-0.05, "0.0R"), (0.04999, "0.0R"), (0.0500001, "+0.1R"),
                                         (-0.0500001, "−0.1R"), (0.0, "0.0R")])
def test_review_p7_a_trade_inside_the_break_even_band_is_printed_as_zero(value: float, text: str) -> None:
    """A trade at exactly ±0.05R counts as break-even and used to be printed "+0.1R" / "−0.1R"."""
    (row,) = _rows(value)
    week = Report(MODE_WEEK, START, END, [row], [])
    assert (row.outcome == "be") == (text == "0.0R")
    assert f"P00 LONG · <b>{text}</b>" in summary_text(week)
    assert f"En iyi: {text} P00 LONG · En kötü: {text} P00 LONG" in summary_text(week)
    data = render_data(week)
    assert data["trades"][0]["outcome"] == row.outcome and data["trades"][0]["r"] == value     # (the picture prints
    assert data["aggregates"]["best"]["r"] == (0.0 if text == "0.0R" else value)                # 0.0R for "be")
    assert week.total_r == value                                                               # the sum keeps it


# ---- P1: the stop's word comes from each account's OWN last stop --------------------------------------------------
def test_review_p1_an_account_that_refused_the_move_to_entry_is_not_called_break_even() -> None:
    """krev/data/test_rev_engine.py::test_s3: the trader says "stop to entry"; fx refuses the change and is stopped
    at its ORIGINAL stop with -1R; tickmill is stopped at the entry. The trade's stop (the trader's wish) says
    "entry" for both; the note used to read "BE"."""
    short = {"side": Side.SHORT, "entry": 1.10872, "plan_stop": 1.11005, "tps": [(1.10654, 1.0)], "asset": "EUR",
             "quote": "USD", "market": MarketType.FOREX}
    fx = position("fx", -100.0, side=Side.SHORT, entry=1.10872, stop=1.11005, data=legs("sl"))            # refused
    tickmill = position("tickmill", 0.0, side=Side.SHORT, entry=1.10872, stop=1.10872, data=legs("sl"))   # moved
    assert record([fx], stop=1.10872, **short) == "SL"              # with one real account: "−1.0R · SL", not "BE"
    assert record([tickmill], stop=1.10872, **short) == "BE"
    assert record([fx, tickmill], stop=1.10872, **short) == ""      # they differ: no note
    # the other way round: the trade's stop still says the signal's, this account's own stop is at its entry
    assert record([tickmill], stop=1.11005, **short) == "BE"
    # a stop moved elsewhere is neither
    assert record([position("fx", 30.0, side=Side.SHORT, entry=1.10872, stop=1.10700, data=legs("sl"))],
                  stop=1.10700, **short) == "stop"
    # no stop on record: no word at all
    assert record([position("fx", -100.0, side=Side.SHORT, entry=1.10872, stop=None, data=legs("sl"))],
                  stop=1.11005, **short) == ""


def test_review_p1_the_accounts_own_stop_is_read_in_the_signals_price_scale() -> None:
    # 1000PEPE: the venue's prices are 1000 times the signal's; its stop 0.0120 is the signal's 0.0000120
    pepe = {"asset": "PEPE", "entry": 0.0000125, "plan_stop": 0.0000120, "tps": [(0.0000140, 1.0)]}
    at_stop = position("binance", -10.0, entry=0.0125, stop=0.0120, data={**exits("sl"), "price_multiplier": 1000.0})
    at_entry = position("binance", 0.1, entry=0.0125, stop=0.0125, data={**exits("sl"), "price_multiplier": 1000.0})
    assert record([at_stop], stop=0.0000120, **pepe) == "SL"
    assert record([at_entry], stop=0.0000125, **pepe) == "BE"
    # an index CFD quoted 12.5 points above the signal's chart (additive offset)
    nas = {"asset": "NAS100", "quote": None, "market": MarketType.CFD, "entry": 21000.0, "plan_stop": 20900.0,
           "tps": [(21200.0, 1.0)]}
    shifted = {"price_offset": {"mode": "additive", "value": 12.5}}
    assert record([position("fx", -100.0, entry=21012.5, stop=20912.5, data={**legs("sl"), **shifted})],
                  stop=20900.0, **nas) == "SL"
    assert record([position("fx", 0.0, entry=21013.1, stop=21013.1, data={**legs("sl"), **shifted})],
                  stop=21000.0, **nas) == "BE"                      # its own fill price, a little off the signal's
    # tick rounding of the venue (a stop a hair away from the signal's) is still the signal's stop
    assert record([position("fx", -100.0, entry=21012.5, stop=20912.6, data={**legs("sl"), **shifted})],
                  stop=20900.0, **nas) == "SL"
    assert record([position("fx", -60.0, entry=21012.5, stop=20950.0, data={**legs("sl"), **shifted})],
                  stop=20937.5, **nas) == "stop"


# ---- a trade whose end can still change (a late order in flight) ----------------------------------------------------
async def test_a_trade_with_an_order_still_in_flight_waits_and_a_listed_trade_is_never_listed_again(world: Any) -> None:
    db, clock = world
    first = await closed(db, clock, END - timedelta(days=2), [position("binance", 10.0)], asset="AAA")
    second = await closed(db, clock, END - timedelta(minutes=3), [position("binance", -10.0)], asset="BBB")
    card = await build_report(db, START, END, pending=lambda trade_id: trade_id == second.id)
    assert [row.pair for row in card.rows] == ["AAA"] and card.deferred == [second.id]
    line = ("Bir hesabın açılış emri hâlâ yanıt beklediği için 1 işlem bu karneye alınmadı; sonuçlanınca yer "
            "alacak.")
    assert line in caption(card).splitlines() and line in summary_text(card).splitlines()
    # next week: the deferred trade is carried in although it ended before this period, the listed one stays out
    await closed(db, clock, END + timedelta(days=1), [position("binance", 30.0)], asset="CCC")
    following = await build_report(db, END, END + timedelta(days=7), carried=[second.id], listed=[first.id])
    assert [row.pair for row in following.rows] == ["BBB", "CCC"] and following.deferred == []
    # still in flight a week later: carried again, on no card yet
    waiting = await build_report(db, END, END + timedelta(days=7), carried=[second.id], listed=[first.id],
                                 pending=lambda trade_id: trade_id == second.id)
    assert [row.pair for row in waiting.rows] == ["CCC"] and waiting.deferred == [second.id]
    # a trade whose end time was moved into the next period after it had been listed is not listed twice
    moved = await build_report(db, START, END + timedelta(days=7), listed=[first.id, second.id])
    assert [row.pair for row in moved.rows] == ["CCC"]
    # a look that fails is not "nothing in flight": the trades wait (logged once), and after MAX_DEFERRALS cards
    # they are listed with the warning line
    def broken(_trade_id: int) -> bool:
        raise RuntimeError("bakılamadı")
    failed = await build_report(db, START, END, pending=broken)
    assert failed.rows == [] and failed.deferred == [first.id, second.id]
    # a carried id that is unknown, active again or ended after this period is simply not taken
    active = await closed(db, clock, END - timedelta(days=1), [position("binance", 1.0)], asset="DDD",
                          status=TradeStatus.OPEN)
    taken = await build_report(db, END, END + timedelta(days=7), carried=[999, active.id, second.id])
    assert [row.pair for row in taken.rows] == ["BBB", "CCC"]
    # every trade so far: the one in flight is not there yet either (and is counted in the line)
    everything = await build_report(db, None, END + timedelta(days=7), mode=MODE_ALL,
                                    pending=lambda trade_id: trade_id == second.id)
    assert [row.pair for row in everything.rows] == ["CCC", "AAA"] and everything.deferred == [second.id]


# --------------------------------------------------------------------------- #
# Review round (isolation): every trade so far must not hold the event loop
# --------------------------------------------------------------------------- #
async def test_review_the_history_is_turned_into_records_in_slices_with_a_pause_between(
        world: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """``/karne hepsi`` reads every closed trade: 3000 of them were parsed in one block of 123 ms on the event loop.
    Now at most CLOSED_TRADES_SLICE rows are parsed between two pauses for the loop, after the database lock is
    released."""
    from copytrader.storage import db as db_mod

    db, clock = world
    for i in range(23):
        await closed(db, clock, START + timedelta(hours=i + 1), [position("binance", 10.0)], asset=f"C{i:02d}")
    monkeypatch.setattr(db_mod, "CLOSED_TRADES_SLICE", 5)
    events: list[str] = []
    real_row, real_sleep = db_mod._row_to_trade, asyncio.sleep

    def parse(row: Any) -> Any:
        assert not db._lock.locked()                        # (nobody waits for the database meanwhile)
        events.append("row")
        return real_row(row)

    async def pause(seconds: float) -> None:
        events.append("pause")
        await real_sleep(seconds)

    monkeypatch.setattr(db_mod, "_row_to_trade", parse)
    monkeypatch.setattr(db_mod.asyncio, "sleep", pause)
    trades = await db.closed_trades_between(None, END)
    assert [t.plan.base_asset for t in trades] == [f"C{i:02d}" for i in range(23)]
    assert "".join("r" if e == "row" else "|" for e in events) == "rrrrr|rrrrr|rrrrr|rrrrr|rrr"
    assert db_mod.CLOSED_TRADES_SLICE == 5 and await db.closed_trades_between(END, END + timedelta(days=1)) == []


async def test_review_the_card_data_stays_small_however_long_the_history_is(world: Any) -> None:
    db, clock = world
    for i in range(120):
        await closed(db, clock, START - timedelta(hours=i), [position("binance", 10.0 if i % 3 else -10.0)],
                     asset=f"C{i:03d}")
    report = await build_report(db, None, END, mode=MODE_ALL, accounts=["binance"])
    data = render_data(report)
    assert len(report.rows) == 120 and len(data["trades"]) == 19 and data["hidden"] == 101
    assert len(json.dumps(data, ensure_ascii=False)) < 6000
    assert data["aggregates"]["wins"] == 80 and data["aggregates"]["losses"] == 40
    assert data["aggregates"]["total_r"] == pytest.approx(40.0)


# --------------------------------------------------------------------------- #
# Review round 2: a carried trade is marked, a trade is not deferred for ever, a failing look defers
# --------------------------------------------------------------------------- #
async def test_review2_n1_a_carried_trade_is_marked_on_the_card_it_is_listed_on(world: Any) -> None:
    db, clock = world
    carried = await closed(db, clock, START + timedelta(days=3), [position("binance", 10.0)], asset="BTC")   # 11.11
    await closed(db, clock, START + timedelta(days=2), [position("binance", 10.0)], asset="OLDER")           # 10.11
    await closed(db, clock, END + timedelta(days=1), [position("binance", 20.0)], asset="NEW")
    nxt = await build_report(db, END, END + timedelta(days=7), carried=[carried.id], listed=[])
    assert [row.pair for row in nxt.rows] == ["BTC", "NEW"]
    line = "1 işlem önceki haftadan (11.11 tarihinde kapanmış) bu karneye alındı: bir hesabın açılış emri geç yanıt verdi."
    assert line in caption(nxt).splitlines() and line in summary_text(nxt).splitlines()
    # two carried trades on different days, and many days
    both = await build_report(db, END, END + timedelta(days=7), carried=[carried.id, carried.id + 1])
    assert ("2 işlem önceki haftadan (10.11, 11.11 tarihinde kapanmış) bu karneye alındı: bir hesabın açılış emri "
            "geç yanıt verdi.") in caption(both).splitlines()
    # a card whose trades all ended in its own period says nothing of the kind
    plain = await build_report(db, START, END)
    assert not any("önceki haftadan" in text for text in (caption(plain), summary_text(plain)))
    # the card of every trade has no period start: no such line
    everything = await build_report(db, None, END + timedelta(days=7), mode=MODE_ALL)
    assert "önceki haftadan" not in caption(everything) + summary_text(everything)
    # /karne (interim): the same line
    interim = await build_report(db, END, END + timedelta(days=3), mode=MODE_INTERIM, carried=[carried.id])
    assert line in caption(interim).splitlines() and line in summary_text(interim).splitlines()


async def test_review2_n2_a_trade_left_off_two_cards_is_listed_on_the_third_with_a_warning(world: Any) -> None:
    from copytrader.karne.data import MAX_DEFERRALS

    db, clock = world
    stuck = await closed(db, clock, END - timedelta(minutes=2), [position("binance", -10.0)], asset="BBB")
    assert MAX_DEFERRALS == 2
    still = lambda trade_id: trade_id == stuck.id           # noqa: E731 - the order never answers
    for waited in (0, 1):
        card = await build_report(db, END, END + timedelta(days=7), carried=[stuck.id], pending=still,
                                  waited={stuck.id: waited})
        assert card.rows == [] and card.deferred == [stuck.id] and card.unsettled == []
    third = await build_report(db, END, END + timedelta(days=7), carried=[stuck.id], pending=still,
                               waited={stuck.id: 2})
    assert [row.pair for row in third.rows] == ["BBB"] and third.deferred == [] and third.unsettled == [stuck.id]
    warning = ("1 işlem 2 karne boyunca bekletildi ve yine de listelendi: bir hesabın açılış emri hâlâ yanıtsız, o "
               "hesabın sonucu eksik olabilir.")
    assert warning in caption(third).splitlines() and warning in summary_text(third).splitlines()
    # an order that answered meanwhile: listed normally, no warning
    settled = await build_report(db, END, END + timedelta(days=7), carried=[stuck.id], waited={stuck.id: 2})
    assert [row.pair for row in settled.rows] == ["BBB"] and settled.unsettled == []
    assert "bekletildi" not in caption(settled)


async def test_review2_n2b_a_failing_look_defers_the_trade_and_is_logged_once(
        world: Any, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    from copytrader.karne import data as data_mod

    db, clock = world
    first = await closed(db, clock, START + timedelta(days=1), [position("binance", 10.0)], asset="AAA")
    second = await closed(db, clock, START + timedelta(days=2), [position("binance", -10.0)], asset="BBB")
    monkeypatch.setattr(data_mod, "_pending_failure_logged", False)

    def broken(_trade_id: int) -> bool:
        raise RuntimeError("bakılamadı")

    with caplog.at_level("ERROR"):
        card = await build_report(db, START, END, pending=broken)
        again = await build_report(db, START, END, pending=broken)
    assert card.rows == [] and card.deferred == [first.id, second.id] == again.deferred
    assert len([r for r in caplog.records if "could not ask whether trade" in r.getMessage()]) == 1
    # it cannot be deferred for ever: after two cards the trades are listed, with the warning
    third = await build_report(db, START, END, pending=broken, waited={first.id: 2, second.id: 2})
    assert [row.pair for row in third.rows] == ["AAA", "BBB"] and third.unsettled == [first.id, second.id]
