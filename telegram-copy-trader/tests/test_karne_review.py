"""The data reviewer's two long attacks on the report card, kept as regression tests (smaller, seeded):

* ``fuzz``: random reports - the text card, the caption and what the picture is given must say the same figures, the
  same marks and the same sentences, for every row and every account (krev/data/fuzz_render.py; it found the
  break-even trade printed "+0.1R");
* ``period``: the weekly schedule against a fake clock for months, with restarts, down times, crashes in the moment
  after the state was written, and the owner's commands in between: the cards' periods are contiguous Sundays 22:00
  UK time and every trade is on exactly one weekly card (krev/data/sim_period.py).
"""

from __future__ import annotations

import asyncio
import importlib.util
import math
import random
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from test_karne_data import closed, position

from copytrader.karne import TABLE_ROWS, uktime
from copytrader.karne import service as service_mod
from copytrader.karne.data import (
    MODE_ALL,
    MODE_INTERIM,
    MODE_WEEK,
    Report,
    Row,
    account_sums,
    caption,
    fmt_r,
    fmt_rate,
    footnotes,
    heading,
    render_data,
    summary_text,
)
from copytrader.karne.render import OK, RenderResult
from copytrader.karne.service import STATE_KEY, KarneService
from copytrader.storage.db import Database

SUN = datetime(2026, 11, 15, 22, 0, tzinfo=UTC)
HAS_PILLOW = importlib.util.find_spec("PIL") is not None
SPECIALS = [0.05, -0.05, 0.0500001, -0.0500001, 0.049, -0.049, 0.04999, 0.15, 0.25, -0.25, 0.35, 1e-9, -1e-9, 2.675,
            -2.675, 9.95, -9.95, 0.95, 99.95]


def random_report(rnd: random.Random) -> Report:
    count = rnd.choice([0, 1, 2, 3, 5, 19, 20, 21, 22, 45])
    mode = rnd.choice([MODE_WEEK, MODE_INTERIM, MODE_ALL])
    rows = []
    for i in range(count):
        u = rnd.random()
        r = None if u < 0.15 else (rnd.choice(SPECIALS) if u < 0.5 else rnd.uniform(-3, 6))
        approx = mode == MODE_ALL and r is not None and rnd.random() < 0.3
        accounts: dict[str, tuple[float | None, bool]] = {"binance": (r, approx)}
        if rnd.random() < 0.3:
            accounts["fx"] = (None, False) if rnd.random() < 0.5 else (None if r is None else -r, False)
        rows.append(Row(i + 1, SUN - timedelta(hours=rnd.uniform(0, 160)), rnd.choice(["BTC", "EURUSD", "X<b>&Y"]),
                        rnd.choice(["LONG", "SHORT"]), r, approx, rnd.choice(["", "TP1", "SL"]), accounts))
    rows.sort(key=lambda row: (row.closed_at, row.trade_id), reverse=mode == MODE_ALL)
    return Report(mode, None if mode == MODE_ALL else SUN - timedelta(days=7), SUN, rows,
                  account_sums(rows, ["binance", "fx"]))


def test_fuzz_the_text_the_caption_and_the_picture_say_the_same() -> None:
    design: Any = None
    if HAS_PILLOW:
        from copytrader.karne import design
    rnd = random.Random(7)
    for _ in range(600):
        report = random_report(rnd)
        data = render_data(report)
        figures = data["aggregates"]
        text, cap = summary_text(report), caption(report)
        rough = "≈" if figures["approx"] else ""
        known = figures["wins"] + figures["losses"] + figures["be"]
        total = fmt_r(figures["total_r"], bool(rough))
        rate = fmt_rate(figures["win_rate"], bool(rough))
        assert f"<b>Toplam: {total}</b> ({known} işlemin R toplamı)" in text
        assert f"Toplam {total} · {known} işlem" in cap and f"kazanma {rate}" in cap
        assert (figures["unknown"], figures["approx"]) == (report.unknown, report.approximate)
        assert math.isclose(figures["total_r"], report.total_r, abs_tol=1e-9)
        if report.known:
            assert f"{figures['wins']} kazanç · {figures['losses']} kayıp · kazanma oranı {rate}" in text
            best, worst = figures["best"], figures["worst"]
            assert (f"En iyi: {fmt_r(best['r'], best['approx'])} {_esc(best['pair'])} {best['side']} · "
                    f"En kötü: {fmt_r(worst['r'], worst['approx'])} {_esc(worst['pair'])} {worst['side']}") in text
        else:
            assert figures["best"] is None and figures["worst"] is None
        # the rows the picture draws: the most recent ones, each with the figure and the marks of the text
        in_time = sorted(report.rows, key=lambda row: (row.closed_at, row.trade_id))
        if report.mode == MODE_ALL or len(in_time) > TABLE_ROWS:
            in_time.reverse()
        drawn = in_time if len(in_time) <= TABLE_ROWS else in_time[:TABLE_ROWS - 1]
        assert len(data["trades"]) == len(drawn) and data["hidden"] == len(in_time) - len(drawn)
        for given, row in zip(data["trades"], drawn, strict=True):
            assert (given["pair"], given["r"], given["outcome"], given["approx"], given["partial"]) == (
                row.pair, row.r, row.outcome, row.approx, row.partial)
            shown = "—" if row.r is None else fmt_r(row.shown, row.approx, row.partial)
            assert f"{_esc(row.pair)} {row.side} · <b>{shown}</b>" in text
            plain = shown.lstrip("≈").rstrip("*")
            if row.outcome == "win":
                assert plain.startswith("+"), (row.r, shown)
            elif row.outcome == "loss":
                assert plain.startswith("−"), (row.r, shown)
            elif row.outcome == "be":
                assert plain == "0.0R", (row.r, shown)               # (review, P7)
        for given, account in zip(data["accounts"], report.accounts, strict=True):
            figure = "—" if account.r is None else fmt_r(account.r, account.approx, account.partial)
            assert f"{account.name} {figure}" in text
            assert (given["r"], given["approx"], given["partial"]) == (account.r, account.approx, account.partial)
        # every mark on the card has its sentence, the same one everywhere (review, P2 / P4 / P8)
        notes = footnotes(report)
        assert data["footnotes"] == notes
        for note in notes:
            assert f"{note}." in text.splitlines() and f"{note}." in cap.splitlines()
        body = text + cap
        assert ("≈" in body) == any(n.startswith("≈") for n in notes)
        assert ("R*" in body) == any(n.startswith("*") for n in notes)
        assert ("<b>—</b>" in text) == (report.unknown > 0) == any(n.startswith("—") for n in notes)
        # nothing of a pair's name is markup, no money, never the sample mark
        assert "X<b>&Y" not in body and data["sample"] is False and "ÖRNEK" not in body
        assert not re.search(r"\$|€|TL\b|USDT", body.replace("EURUSD", ""))
        if design is not None:                                     # what the picture itself then prints
            summed = design.summarize(data)
            assert (summed["n"], summed["wins"], summed["losses"], summed["be"]) == (
                known, report.wins, report.losses, report.flat)
            assert ("≈" if summed["approx"] else "") + design.fmt_r(summed["total_r"], 1, ".") == total
            if summed["win_rate"] is not None:
                assert ("≈" if summed["approx"] else "") + design.fmt_pct(summed["win_rate"]) == rate
            assert design._footnotes(summed) == notes
            for given in summed["trades"]:
                if given["outcome"] != "unknown":
                    printed = design.fmt_r(0.0 if given["outcome"] == "be" else given["r"], 1, ".")
                    assert printed == fmt_r(0.0 if given["outcome"] == "be" else given["r"])


def _esc(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# --------------------------------------------------------------------------- #
# the schedule over months
# --------------------------------------------------------------------------- #
class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class Owner:
    def __init__(self) -> None:
        self.texts: list[str] = []
        self.files: list[tuple[str, str | None]] = []
        self.crash_on_send = False

    async def tell_owner(self, text: str) -> None:
        if self.crash_on_send:
            raise asyncio.CancelledError()
        self.texts.append(text)

    async def send_owner_document(self, path: str, *, caption: str = "", name: str | None = None,
                                  busy: Any = None) -> str | None:
        if self.crash_on_send:
            raise asyncio.CancelledError()                         # the bot dies right after the state was written
        self.files.append((caption, name))
        return None


@pytest.mark.parametrize("seed, rule_only", [(0, True), (4, False)])     # (from 20 Feb: the March change; from
#                                                                           30 Jul: the October one)
async def test_period_every_trade_is_on_exactly_one_weekly_card_through_restarts_down_times_and_crashes(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, seed: int, rule_only: bool) -> None:
    if rule_only:
        monkeypatch.setattr(uktime, "_ZONE", None)
    elif uktime._ZONE is None:
        pytest.skip("this machine has no tz database (Europe/London)")
    rnd = random.Random(seed)
    weeks = 30
    start = datetime(2026, 2, 20, 9, 0, tzinfo=UTC) + timedelta(days=seed * 40)
    clock = Clock(start)
    db = Database(tmp_path / "x.db", clock=clock)
    await db.open()
    owner = Owner()
    cards: list[dict[str, Any]] = []
    real_build = service_mod.build_report

    async def spy(dbx: Any, s: Any, e: Any, **kw: Any) -> Report:
        report = await real_build(dbx, s, e, **kw)
        cards.append({"mode": kw.get("mode", MODE_WEEK), "start": s, "end": e, "ids": [r.trade_id for r in report.rows],
                      "weeks": report.weeks, "head": heading(report)})
        return report

    monkeypatch.setattr(service_mod, "build_report", spy)

    async def draw(_data: dict[str, Any], folder: Any) -> RenderResult:
        Path(folder).mkdir(parents=True, exist_ok=True)
        path = Path(folder) / f"karne_{len(cards)}.png"
        path.write_bytes(b"x")
        return RenderResult(OK, path=path, seconds=0.1)

    def service() -> KarneService:
        return KarneService(db, owner, tmp_path, accounts=["binance"], clock=clock, renderer=draw, pillow=lambda: True)

    end = start + timedelta(weeks=weeks)
    moments = [start + timedelta(seconds=rnd.uniform(0, (end - start).total_seconds())) for _ in range(weeks * 5)]
    slot = uktime.next_slot(start)
    while slot < end:                                              # ... and trades right at every cut-off
        moments += [slot - timedelta(microseconds=1), slot, slot + timedelta(microseconds=1)]
        slot = uktime.next_slot(slot)
    waiting = sorted(moments)
    trades: dict[int, datetime] = {}
    svc = service()
    sent: list[dict[str, Any]] = []
    lost: list[dict[str, Any]] = []
    try:
        while clock.now < end:
            while waiting and waiting[0] <= clock.now:
                when, keep = waiting.pop(0), clock.now
                trade = await closed(db, clock, when, [position("binance", rnd.choice([20.0, -10.0, 0.0]))])
                clock.now = keep
                trades[trade.id] = when
            before = len(cards)
            owner.crash_on_send = rnd.random() < 0.15
            try:
                delay = await svc._tick()
            except asyncio.CancelledError:                         # the "crash": the bot dies; a new process
                delay, svc = 1.0, service()
            for card in cards[before:]:
                if card["mode"] == MODE_WEEK:
                    (lost if owner.crash_on_send else sent).append(card)
            owner.crash_on_send = False
            if rnd.random() < 0.01:                                # the owner's command: stores nothing
                state = await db.get_state(STATE_KEY)
                await svc._on_demand(rnd.choice([MODE_INTERIM, MODE_ALL]))
                assert await db.get_state(STATE_KEY) == state
            step = max(1.0, min(delay, 300.0))
            roll = rnd.random()
            if roll < 0.0008:                                      # down for a while, then a restart
                step = rnd.choice([30, 3600, 6 * 3600, 23.9 * 3600, 24.1 * 3600, 3 * 86400, 9 * 86400,
                                   rnd.uniform(0, 7 * 86400)])
                svc = service()
            elif roll < 0.005:                                     # a plain restart
                svc = service()
            clock.now += timedelta(seconds=step)
    finally:
        await db.close()

    weekly = sorted(sent + lost, key=lambda card: card["end"])
    assert len(weekly) >= 10 and sent and any(card["weeks"] > 1 for card in weekly)      # (the run did something)
    for earlier, later in zip(weekly, weekly[1:]):
        assert later["start"] == earlier["end"], (earlier, later)                    # contiguous: no gap, no overlap
    seen: dict[int, int] = {}
    for card in weekly:
        for bound in (card["start"], card["end"]):
            assert uktime.to_uk(bound).strftime("%a %H:%M:%S") == "Sun 22:00:00", card
        span = round((card["end"] - card["start"]) / timedelta(days=7))
        assert card["weeks"] == span and (span > 1) == (f"{span} HAFTALIK" in card["head"]), card
        for trade_id in card["ids"]:
            assert card["start"] <= trades[trade_id] < card["end"], (trade_id, card)
            seen[trade_id] = seen.get(trade_id, 0) + 1
    first_cut, last_cut = weekly[0]["start"], weekly[-1]["end"]
    for trade_id, when in trades.items():
        assert seen.get(trade_id, 0) == (1 if first_cut <= when < last_cut else 0), (trade_id, when)
    assert sum("Haftalık karne gönderilmedi" in text for text in owner.texts) >= 1   # (a card was skipped, with a line)
