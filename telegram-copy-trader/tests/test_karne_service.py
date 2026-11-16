"""When a report card goes out (copytrader/karne/service.py): the weekly schedule, its stored cut-off, a missed card,
shutdown, the text card, and the owner's ``/karne`` with its rate limit.

The database is the bot's own storage class on a real file; the clock, the sleeping, the drawing process and
Telegram are stand-ins (the real drawing process has its own tests: tests/test_karne_render.py).
"""

from __future__ import annotations

import asyncio
import functools
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from test_karne_data import closed, position
from test_karne_render import FAKE_PNG, gone, pid_of, stand_in

from copytrader.karne import service as service_mod
from copytrader.karne.data import MODE_ALL, MODE_INTERIM, MODE_WEEK
from copytrader.karne.render import (
    FAILED,
    NO_PILLOW,
    OK,
    TIMEOUT,
    RenderResult,
    render_card,
)
from copytrader.karne.service import COMMAND_GAP_SEC, STATE_KEY, KarneService, file_name
from copytrader.karne.uktime import previous_slot
from copytrader.models import Severity
from copytrader.storage.db import Database

SUNDAY = datetime(2026, 11, 15, 22, 0, tzinfo=UTC)        # a card's time: Sunday 22:00 UK (winter: 22:00 UTC)
WEEK = timedelta(days=7)
ACCOUNTS = ["binance", "fx"]


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class Sleeper:
    """``asyncio.sleep`` of the schedule: the clock jumps instead of waiting. At ``until`` it stops jumping (the
    test looks at what happened so far) until the test moves ``until`` on."""

    def __init__(self, clock: Clock, until: datetime) -> None:
        self.clock, self.until = clock, until
        self.naps: list[float] = []
        self.reached = asyncio.Event()
        self._go = asyncio.Event()

    async def __call__(self, seconds: float) -> None:
        self.naps.append(seconds)
        while self.clock.now + timedelta(seconds=seconds) > self.until:
            self.reached.set()
            self._go.clear()
            await self._go.wait()
        self.clock.now += timedelta(seconds=seconds)
        await asyncio.sleep(0)

    async def run_to(self, until: datetime) -> None:
        self.until = until
        self.reached.clear()
        self._go.set()
        await asyncio.wait_for(self.reached.wait(), 20)


class Owner:
    """The notifier as the service uses it: files and texts for the owner."""

    def __init__(self, db: Database | None = None, clock: Clock | None = None) -> None:
        self.db, self.clock = db, clock
        self.texts: list[str] = []
        self.files: list[dict[str, Any]] = []
        self.file_problem: str | None = None
        self.state_at_send: list[Any] = []
        self.sent_at: list[datetime] = []

    async def _note(self) -> None:
        if self.db is not None:
            self.state_at_send.append(await self.db.get_state(STATE_KEY))
        if self.clock is not None:
            self.sent_at.append(self.clock.now)

    async def tell_owner(self, text: str) -> None:
        await self._note()
        self.texts.append(text)

    async def send_owner_document(self, path: str, *, caption: str = "", name: str | None = None,
                                  busy: Any = None) -> str | None:
        await self._note()
        if self.file_problem is not None:
            return self.file_problem
        self.files.append({"data": Path(path).read_bytes(), "caption": caption, "name": name, "busy": busy})
        return None


class TextOnly:
    """A notifier that cannot send files (as the stand-ins of other tests): the card goes out as text."""

    def __init__(self) -> None:
        self.notices: list[tuple[str, Severity]] = []

    async def notify(self, text: str, severity: Severity = Severity.INFO) -> None:
        self.notices.append((text, severity))


class Drawer:
    """The drawing process: writes a picture (or does not) and remembers the card it was given."""

    def __init__(self) -> None:
        self.cards: list[dict[str, Any]] = []
        self.status = OK
        self.detail = ""
        self.gate: asyncio.Event | None = None
        self.running = 0
        self.most = 0

    async def __call__(self, data: dict[str, Any], folder: Any) -> RenderResult:
        self.cards.append(data)
        self.running += 1
        self.most = max(self.most, self.running)
        try:
            if self.gate is not None:
                await self.gate.wait()
            if self.status != OK:
                return RenderResult(self.status, detail=self.detail)
            Path(folder).mkdir(parents=True, exist_ok=True)
            path = Path(folder) / f"karne_{len(self.cards)}.png"
            path.write_bytes(FAKE_PNG)
            return RenderResult(OK, path=path, seconds=0.1)
        finally:
            self.running -= 1


class World:
    def __init__(self, tmp_path: Path, start: datetime) -> None:
        self.tmp_path = tmp_path
        self.clock = Clock(start)
        self.db = Database(tmp_path / "copytrader.db", clock=self.clock)
        self.owner = Owner(self.db, self.clock)
        self.drawer = Drawer()
        self.sleeper = Sleeper(self.clock, start)
        self.mono = 1000.0
        self.has_pillow = True
        self.in_flight: set[int] = set()            # trades with an open order still unanswered (the engine's look)
        self.service = self.new_service()

    def new_service(self, notifier: Any = None, renderer: Any = None, db: Any = None) -> KarneService:
        return KarneService(db or self.db, notifier or self.owner, self.tmp_path, accounts=ACCOUNTS,
                            busy=lambda: False, pending=lambda trade_id: trade_id in self.in_flight,
                            clock=self.clock, sleep=self.sleeper,
                            renderer=renderer or self.drawer, pillow=lambda: self.has_pillow,
                            monotonic=lambda: self.mono)

    async def trade(self, when: datetime, pnl: float = 10.0, asset: str = "ETH", **kw: Any) -> None:
        now = self.clock.now
        await closed(self.db, self.clock, when, [position("binance", pnl, **kw)], asset=asset)
        self.clock.now = now

    async def state(self) -> dict[str, Any]:
        return await self.db.get_state(STATE_KEY) or {}

    async def settle(self) -> None:
        """Let the on-demand card's task finish."""
        for _ in range(200):
            if not self.service._jobs:
                return
            await asyncio.sleep(0.01)
        raise AssertionError("the on-demand card did not finish")


@pytest.fixture
async def world(tmp_path: Path):
    w = World(tmp_path, SUNDAY - timedelta(days=1, hours=9))        # Saturday 13:00
    await w.db.open()
    yield w
    await w.service.stop()
    await w.db.close()


async def started(w: World, *, handled: datetime | None = SUNDAY - WEEK) -> World:
    """The bot has run before: the previous Sunday's card is on record."""
    if handled is not None:
        await w.db.set_state(STATE_KEY, {"handled": handled.isoformat(), "cutoff": handled.isoformat(), "note": "sent"})
    w.service.start()
    return w


# --------------------------------------------------------------------------- #
# the schedule
# --------------------------------------------------------------------------- #
async def test_the_card_goes_out_once_on_sunday_at_ten_pm_uk_time(world: World) -> None:
    w = world
    await w.trade(SUNDAY - timedelta(days=3), 21.0, "ETH")                      # +2.1R this week
    await w.trade(SUNDAY - timedelta(days=2), -10.0, "BTC")                     # -1.0R this week
    await w.trade(SUNDAY - WEEK - timedelta(hours=1), 50.0, "OLD")              # last week's: not on this card
    await started(w)
    await w.sleeper.run_to(SUNDAY - timedelta(seconds=1))
    assert w.owner.files == [] and w.owner.texts == [] and w.drawer.cards == []
    await w.sleeper.run_to(SUNDAY + timedelta(days=3))

    (card,) = w.owner.files                                                      # one card, as a file
    assert w.owner.texts == [] and w.owner.sent_at == [SUNDAY]                   # at 22:00 sharp
    assert card["data"] == FAKE_PNG and card["name"] == "karne_2026-11-15.png" and callable(card["busy"])
    assert card["caption"].splitlines() == [
        "📊 <b>Haftalık karne · 8–15 Kasım 2026</b>",
        "Dönem: 08.11 Paz 22:00 – 15.11 Paz 22:00 (İngiltere saati)",
        "Toplam +1.1R · 2 işlem (1 kazanç · 1 kayıp) · kazanma %50"]
    (drawn,) = w.drawer.cards
    assert [(t["pair"], t["r"]) for t in drawn["trades"]] == [("ETH", pytest.approx(2.1)), ("BTC", pytest.approx(-1.0))]
    assert drawn["sample"] is False and drawn["subtitle"] == "HAFTALIK KARNE · 8–15 Kasım 2026"
    assert [(a["name"], a["r"]) for a in drawn["accounts"]] == [("binance", pytest.approx(1.1)), ("fx", 0.0)]
    # it was on record BEFORE the first message left: whatever happens after that, never a second time
    assert [s["note"] for s in w.owner.state_at_send] == ["sent"]
    assert (await w.state())["handled"] == (await w.state())["cutoff"] == SUNDAY.isoformat()
    # ... together with the trades it lists (never on a weekly card again) and none left for later
    assert w.owner.state_at_send[0]["listed"] == [1, 2] and w.owner.state_at_send[0]["carry"] == []
    # the schedule looks at the clock at least every five minutes and never spins
    assert max(w.sleeper.naps) <= 300.0 and min(w.sleeper.naps) >= 1.0
    assert not list((w.tmp_path / "karne").glob("*.json"))


async def test_the_next_card_follows_a_week_later_from_the_last_cut_off(world: World) -> None:
    w = world
    await started(w)
    await w.sleeper.run_to(SUNDAY + timedelta(hours=1))
    await w.trade(SUNDAY + timedelta(seconds=1), 10.0, "SOL")                    # after the cut-off: next week's
    await w.trade(SUNDAY + timedelta(days=6), 30.0, "ADA")
    await w.sleeper.run_to(SUNDAY + WEEK + timedelta(hours=1))
    assert [f["name"] for f in w.owner.files] == ["karne_2026-11-15.png", "karne_2026-11-22.png"]
    assert w.owner.sent_at == [SUNDAY, SUNDAY + WEEK]
    assert [t["pair"] for t in w.drawer.cards[0]["trades"]] == []
    assert [t["pair"] for t in w.drawer.cards[1]["trades"]] == ["SOL", "ADA"]
    assert w.drawer.cards[1]["subtitle"] == "HAFTALIK KARNE · 15–22 Kasım 2026"


@pytest.mark.parametrize("start, due", [
    (datetime(2026, 3, 28, 12, 0, tzinfo=UTC), datetime(2026, 3, 29, 21, 0, tzinfo=UTC)),     # clocks forward that day
    (datetime(2026, 10, 24, 12, 0, tzinfo=UTC), datetime(2026, 10, 25, 22, 0, tzinfo=UTC)),   # clocks back that day
    (datetime(2026, 7, 10, 12, 0, tzinfo=UTC), datetime(2026, 7, 12, 21, 0, tzinfo=UTC)),     # summer
])
async def test_the_card_keeps_uk_time_through_the_clock_changes(tmp_path: Path, start: datetime, due: datetime) -> None:
    w = World(tmp_path, start)
    await w.db.open()
    try:
        await started(w, handled=previous_slot(due))                             # (the Sunday before is on record)
        await w.sleeper.run_to(due - timedelta(seconds=1))
        assert w.owner.files == [] and w.owner.texts == []
        await w.sleeper.run_to(due + timedelta(hours=30))
        assert w.owner.sent_at == [due] and len(w.owner.files) == 1 and w.owner.texts == []
    finally:
        await w.service.stop()
        await w.db.close()


async def test_a_restart_never_sends_the_card_again(world: World) -> None:
    w = world
    await started(w)
    await w.sleeper.run_to(SUNDAY + timedelta(minutes=10))
    assert len(w.owner.files) == 1
    await w.service.stop()
    for later in (timedelta(minutes=11), timedelta(hours=5), timedelta(hours=23), timedelta(days=2)):
        w.clock.now = SUNDAY + later
        w.sleeper.until = w.clock.now
        again = w.new_service()
        again.start()
        await w.sleeper.run_to(w.clock.now + timedelta(minutes=30))
        await again.stop()
    assert len(w.owner.files) == 1 and w.owner.texts == [] and len(w.drawer.cards) == 1


async def test_the_first_start_with_this_feature_sends_nothing_and_misses_nothing(world: World) -> None:
    w = world
    await w.trade(SUNDAY - timedelta(days=10), 10.0, "OLD")                      # before the feature: /karne hepsi's
    await w.trade(SUNDAY - timedelta(days=2), 10.0, "NEW")
    w.clock.now = SUNDAY - WEEK + timedelta(hours=3)                             # Monday 01:00, three hours after a slot
    w.sleeper.until = w.clock.now
    w.service.start()                                                            # no state at all
    await w.sleeper.run_to(SUNDAY - timedelta(hours=1))
    assert w.owner.files == [] and w.owner.texts == []                           # no card, no "missed" line
    state = await w.state()
    assert state["note"] == "init" and state["handled"] == state["cutoff"] == (SUNDAY - WEEK).isoformat()
    await w.sleeper.run_to(SUNDAY + timedelta(hours=1))
    (card,) = w.drawer.cards                                                     # the first card: this week's
    assert [t["pair"] for t in card["trades"]] == ["NEW"] and len(w.owner.files) == 1


async def test_a_card_missed_by_less_than_a_day_is_sent_at_the_next_start(world: World) -> None:
    w = world
    await w.trade(SUNDAY - timedelta(days=2), 10.0, "ETH")
    await w.trade(SUNDAY + timedelta(hours=2), 10.0, "LATE")                     # after the cut-off: not on it
    await w.db.set_state(STATE_KEY, {"handled": (SUNDAY - WEEK).isoformat(), "cutoff": (SUNDAY - WEEK).isoformat()})
    w.clock.now = SUNDAY + timedelta(hours=9, minutes=30)                        # the bot was down at 22:00
    w.sleeper.until = w.clock.now + timedelta(hours=1)
    w.service.start()
    await asyncio.wait_for(w.sleeper.reached.wait(), 20)
    (card,) = w.owner.files
    assert w.owner.sent_at == [SUNDAY + timedelta(hours=9, minutes=30)]          # at once, not next Sunday
    assert card["name"] == "karne_2026-11-15.png"                                # the same card, the same period
    assert "Dönem: 08.11 Paz 22:00 – 15.11 Paz 22:00 (İngiltere saati)" in card["caption"]
    assert "Bu karne pazar 22:00'de gönderilemedi; şimdi gönderildi." in card["caption"]
    assert [t["pair"] for t in w.drawer.cards[0]["trades"]] == ["ETH"]
    assert (await w.state())["cutoff"] == SUNDAY.isoformat()


async def test_a_card_missed_by_more_than_a_day_is_skipped_with_one_line_and_its_trades_wait_for_the_next(
        world: World) -> None:
    w = world
    await w.trade(SUNDAY - timedelta(days=2), 20.0, "ETH")                       # the skipped week's trade
    await w.db.set_state(STATE_KEY, {"handled": (SUNDAY - WEEK).isoformat(), "cutoff": (SUNDAY - WEEK).isoformat()})
    w.clock.now = SUNDAY + timedelta(hours=24, minutes=1)
    w.sleeper.until = w.clock.now + timedelta(hours=1)
    w.service.start()
    await asyncio.wait_for(w.sleeper.reached.wait(), 20)
    assert w.owner.files == [] and w.drawer.cards == []
    assert w.owner.texts == [
        "ℹ️ Haftalık karne gönderilmedi: bot 15.11 Paz 22:00 (İngiltere saati) sırasında çalışmıyordu ve üzerinden 24 "
        "saatten fazla geçti. O haftanın işlemleri bir sonraki karnede yer alacak; şu ana kadarki durum için /karne "
        "yazabilirsiniz."]
    state = await w.state()
    assert (state["note"], state["handled"], state["cutoff"]) == ("skipped", SUNDAY.isoformat(),
                                                                  (SUNDAY - WEEK).isoformat())
    # a restart does not say it again
    await w.service.stop()
    w.clock.now += timedelta(hours=3)
    w.sleeper.until = w.clock.now
    w.service = w.new_service()
    w.service.start()
    await w.sleeper.run_to(w.clock.now + timedelta(hours=2))
    assert len(w.owner.texts) == 1
    # the next Sunday's card covers both weeks, and says so
    await w.trade(SUNDAY + timedelta(days=3), -10.0, "BTC")
    await w.sleeper.run_to(SUNDAY + WEEK + timedelta(hours=1))
    (card,) = w.owner.files
    (drawn,) = w.drawer.cards
    assert [t["pair"] for t in drawn["trades"]] == ["ETH", "BTC"]
    assert drawn["subtitle"] == "2 HAFTALIK KARNE · 8–22 Kasım 2026" and drawn["hero_label"] == "DÖNEM SONUCU"
    assert "Önceki karne gönderilemediği için bu karne 2 haftayı kapsıyor." in card["caption"]
    assert (await w.state())["cutoff"] == (SUNDAY + WEEK).isoformat()


async def test_exactly_a_day_late_is_still_sent(world: World) -> None:
    w = world
    await w.db.set_state(STATE_KEY, {"handled": (SUNDAY - WEEK).isoformat(), "cutoff": (SUNDAY - WEEK).isoformat()})
    w.clock.now = SUNDAY + timedelta(hours=24)
    w.sleeper.until = w.clock.now + timedelta(minutes=10)
    w.service.start()
    await asyncio.wait_for(w.sleeper.reached.wait(), 20)
    assert len(w.owner.files) == 1 and w.owner.texts == []


async def test_a_database_that_fails_at_ten_pm_is_tried_again_and_the_card_still_goes_out_once(world: World) -> None:
    w = world
    await w.trade(SUNDAY - timedelta(days=2), 10.0, "ETH")

    class Flaky:
        def __init__(self, db: Database) -> None:
            self.db, self.failures = db, 2

        def __getattr__(self, name: str) -> Any:
            return getattr(self.db, name)

        async def closed_trades_between(self, start: Any, end: Any) -> Any:
            if self.failures:
                self.failures -= 1
                raise OSError("database is locked")
            return await self.db.closed_trades_between(start, end)

    await w.db.set_state(STATE_KEY, {"handled": (SUNDAY - WEEK).isoformat(), "cutoff": (SUNDAY - WEEK).isoformat()})
    w.service = w.new_service(db=Flaky(w.db))
    w.service.start()
    await w.sleeper.run_to(SUNDAY + timedelta(minutes=4))
    assert w.owner.files == [] and (await w.state())["handled"] == (SUNDAY - WEEK).isoformat()     # nothing on record
    await w.sleeper.run_to(SUNDAY + timedelta(hours=2))
    assert len(w.owner.files) == 1 and w.owner.sent_at == [SUNDAY + timedelta(minutes=10)]         # two retries later
    assert "Bu karne pazar 22:00'de gönderilemedi" in w.owner.files[0]["caption"]                        # (10 min late)


# --------------------------------------------------------------------------- #
# shutdown
# --------------------------------------------------------------------------- #
async def test_stop_is_bounded_and_a_card_that_was_being_drawn_goes_out_after_the_restart(world: World) -> None:
    w = world
    await w.trade(SUNDAY - timedelta(days=2), 10.0, "ETH")
    w.drawer.gate = asyncio.Event()                                              # the drawing never finishes
    await started(w)
    w.sleeper.until = SUNDAY + timedelta(hours=1)
    w.sleeper._go.set()
    for _ in range(500):
        if w.drawer.running:
            break
        await asyncio.sleep(0.01)
    assert w.drawer.running == 1
    began = time.monotonic()
    await w.service.stop()                                                       # the bot shuts down mid-card
    assert time.monotonic() - began < 2.0 and w.drawer.running == 0
    assert w.owner.files == [] and w.owner.texts == []
    assert (await w.state())["handled"] == (SUNDAY - WEEK).isoformat()           # nothing went out: not on record
    assert await w.service.command("") == "⏳ Bot kapanıyor; karne şu an hazırlanamıyor."

    w.drawer.gate = None
    w.clock.now = SUNDAY + timedelta(minutes=20)                                 # restarted a little later
    w.sleeper.until = w.clock.now + timedelta(minutes=30)
    w.service = w.new_service()
    w.service.start()
    await asyncio.wait_for(w.sleeper.reached.wait(), 20)
    assert len(w.owner.files) == 1 and (await w.state())["cutoff"] == SUNDAY.isoformat()


async def test_stop_kills_a_real_drawing_process(tmp_path: Path) -> None:
    w = World(tmp_path, SUNDAY - timedelta(hours=1))
    await w.db.open()
    try:
        hanging = functools.partial(render_card, command=stand_in(tmp_path, "time.sleep(600)"), timeout=120.0,
                                    poll_sec=0.05)
        w.service = w.new_service(renderer=hanging)
        await started(w)
        w.sleeper.until = SUNDAY + timedelta(hours=1)
        w.sleeper._go.set()
        deadline = time.monotonic() + 15
        while not (tmp_path / "stand_in.pid").exists() and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        pid = pid_of(tmp_path)
        began = time.monotonic()
        await w.service.stop()
        assert time.monotonic() - began < service_mod.STOP_WAIT_SEC
        assert await gone(pid)                                                   # no drawing process outlives the bot
        assert w.owner.files == [] and w.owner.texts == []
    finally:
        await w.db.close()


async def test_stop_before_start_and_twice_is_harmless(world: World) -> None:
    await world.service.stop()
    world.service.start()
    world.service.start()                                                        # (a second start: the same task)
    await world.service.stop()
    await world.service.stop()


# --------------------------------------------------------------------------- #
# no picture: the card as text
# --------------------------------------------------------------------------- #
async def weekly_text(w: World) -> str:
    await w.trade(SUNDAY - timedelta(days=2), 21.0, "ETH")
    await started(w)
    await w.sleeper.run_to(SUNDAY + timedelta(hours=1))
    assert w.owner.files == []
    (text,) = w.owner.texts
    assert [s["note"] for s in w.owner.state_at_send][-1] == "sent"
    return text


CARD_TEXT = [
    "📊 <b>BABÜR'S TRADE PERFORMANCE</b>",
    "Haftalık karne · 8–15 Kasım 2026",
    "Dönem: 08.11 Paz 22:00 – 15.11 Paz 22:00 (İngiltere saati)",
    "",
    "<b>Toplam: +2.1R</b> (1 işlemin R toplamı)",
    "1 kazanç · 0 kayıp · kazanma oranı %100",
    "En iyi: +2.1R ETH LONG · En kötü: +2.1R ETH LONG",
    "Hesaplar: binance +2.1R · fx 0.0R",
    "",
    "<b>İşlemler</b>",
    "13.11 Cum · ETH LONG · <b>+2.1R</b>",
    ""]


@pytest.mark.parametrize("who_says", ["the bot's own look: no process is started", "the drawing process itself"])
async def test_without_pillow_the_card_is_a_text_summary_with_one_line_on_how_to_get_the_picture(
        world: World, who_says: str) -> None:
    if who_says.startswith("the bot"):
        world.has_pillow = False
    else:
        world.drawer.status, world.drawer.detail = NO_PILLOW, "karne: Pillow is not installed"
    text = await weekly_text(world)
    assert len(world.drawer.cards) == (0 if who_says.startswith("the bot") else 1)
    def quoted(path: str) -> str:                                    # (always: PowerShell needs & "..." for a path)
        return f'"{path}"'

    folder = Path(service_mod.__file__).resolve().parents[2] / "karne_paketleri"      # next to the package
    assert text.splitlines() == [*CARD_TEXT, (
        "ℹ️ Görsel karne için Pillow paketi kurulu değil; bu yüzden karne metin olarak gönderildi. Görsel için şunu "
        "çalıştırın (bot açık kalabilir; paket botun kendi Python ortamına değil, yalnızca karne çizimi için ayrı "
        "bir klasöre kurulur; PowerShell'e olduğu gibi yapıştırılabilir): "
        f"<code>&amp; {quoted(sys.executable)} -m pip install --upgrade --no-deps --only-binary=:all: "
        f"--target {quoted(str(folder))} pillow==13.0.0</code>")]


async def test_a_drawing_that_fails_or_takes_too_long_gives_the_text_card_with_the_reason(tmp_path: Path) -> None:
    for status, detail, line in [
        (FAILED, "exit code 1: OSError: <cannot open resource>",
         "⚠️ Karne görseli hazırlanamadı (çizim hatası: <code>exit code 1: OSError: &lt;cannot open resource&gt;</code>); "
         "bu yüzden karne metin olarak gönderildi."),
        (TIMEOUT, "no picture within 90 s",
         "⚠️ Karne görseli hazırlanamadı (90 sn içinde bitmedi, çizim durduruldu); bu yüzden karne metin olarak "
         "gönderildi."),
    ]:
        folder = tmp_path / status
        folder.mkdir()
        w = World(folder, SUNDAY - timedelta(hours=5))
        await w.db.open()
        try:
            w.drawer.status, w.drawer.detail = status, detail
            assert (await weekly_text(w)).splitlines() == [*CARD_TEXT, line]
        finally:
            await w.service.stop()
            await w.db.close()


async def test_a_picture_telegram_did_not_take_gives_the_text_card_and_the_picture_is_removed(world: World) -> None:
    world.owner.file_problem = "Telegram 120 sn içinde yanıt vermedi; dosya sonradan da gelebilir"
    text = await weekly_text(world)
    assert text.splitlines() == [*CARD_TEXT, (
        "⚠️ Karne görseli gönderilemedi (Telegram 120 sn içinde yanıt vermedi; dosya sonradan da gelebilir); bu yüzden "
        "karne metin olarak gönderildi.")]
    deadline = time.monotonic() + 3
    while list((world.tmp_path / "karne").glob("*.png")) and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    assert not list((world.tmp_path / "karne").glob("*.png"))


async def test_a_notifier_that_sends_no_files_gets_the_text_card_and_no_process_is_started(world: World) -> None:
    w = world
    plain = TextOnly()
    w.service = w.new_service(notifier=plain)
    await w.trade(SUNDAY - timedelta(days=2), 21.0, "ETH")
    await started(w)
    await w.sleeper.run_to(SUNDAY + timedelta(hours=1))
    assert w.drawer.cards == []
    ((text, severity),) = plain.notices
    assert severity is Severity.INFO and text.splitlines() == CARD_TEXT[:-1]


async def test_a_sender_that_raises_never_ends_the_schedule(world: World) -> None:
    w = world

    class Broken(Owner):
        async def send_owner_document(self, *a: Any, **kw: Any) -> str | None:
            raise RuntimeError("bağlantı koptu")

    broken = Broken(w.db, w.clock)
    w.service = w.new_service(notifier=broken)
    await started(w)
    await w.sleeper.run_to(SUNDAY + WEEK + timedelta(hours=1))
    assert len(broken.texts) == 2 and all("⚠️ Karne görseli gönderilemedi (bağlantı koptu)" in t for t in broken.texts)


# --------------------------------------------------------------------------- #
# /karne
# --------------------------------------------------------------------------- #
async def test_karne_sends_the_week_so_far_and_changes_nothing_on_record(world: World) -> None:
    w = world
    await w.trade(SUNDAY - WEEK - timedelta(hours=2), 99.0, "OLD")               # before the last cut-off
    await w.trade(SUNDAY - timedelta(days=3), 21.0, "ETH")
    await started(w)
    await w.sleeper.run_to(w.clock.now)
    before = await w.state()
    assert await w.service.command("") == "⏳ Karne hazırlanıyor; birkaç saniye içinde gelecek."
    await w.settle()
    (card,) = w.owner.files
    assert card["name"] == "karne_ara_2026-11-14.png"
    assert card["caption"].splitlines() == [
        "📊 <b>Ara karne · 8–14 Kasım 2026</b>",
        "Dönem: 08.11 Paz 22:00 – 14.11 Cmt 13:00 (İngiltere saati)",
        "Toplam +2.1R · 1 işlem (1 kazanç · 0 kayıp) · kazanma %100",
        "Hafta sürüyor: yalnızca tamamen kapanmış işlemler sayıldı, açık işlemler dahil değil."]
    (drawn,) = w.drawer.cards
    assert drawn["subtitle"] == "ARA KARNE · 8–14 Kasım 2026" and [t["pair"] for t in drawn["trades"]] == ["ETH"]
    assert await w.state() == before                                             # the weekly card is not touched
    await w.sleeper.run_to(SUNDAY + timedelta(hours=1))
    assert [f["name"] for f in w.owner.files] == ["karne_ara_2026-11-14.png", "karne_2026-11-15.png"]


async def test_karne_hepsi_sends_every_trade_so_far_with_the_approximate_ones_marked(world: World) -> None:
    w = world
    await w.trade(SUNDAY - timedelta(days=40), 25.0, "BTC", risk=None, target=10.0)        # an old position: ≈ +2.5R
    await w.trade(SUNDAY - timedelta(days=3), 21.0, "ETH")                                 # this version: +2.1R
    assert await w.service.command("hepsi") == "⏳ Tüm işlemlerin karnesi hazırlanıyor; birkaç saniye içinde gelecek."
    await w.settle()
    (card,) = w.owner.files
    assert card["name"] == "karne_tum_2026-11-14.png"
    assert card["caption"].splitlines() == [
        "📊 <b>Tüm işlemler · 6 Ekim – 14 Kasım 2026</b>",
        "Dönem: ilk işlemden 14.11.2026 Cmt 13:00 anına kadar (İngiltere saati)",
        "Toplam ≈+4.6R · 2 işlem (2 kazanç · 0 kayıp) · kazanma ≈%100",
        "≈ yaklaşık: 1 işlemde gerçek risk kayıtlı değil; R, hesabın planlanan riskine göre hesaplandı."]
    assert w.drawer.cards[0]["footnotes"] == [card["caption"].splitlines()[3].rstrip(".")]     # the same sentence
    (drawn,) = w.drawer.cards
    assert [(t["pair"], t["approx"]) for t in drawn["trades"]] == [("ETH", False), ("BTC", True)]      # newest first
    assert drawn["subtitle"] == "TÜM İŞLEMLER · 6 Ekim – 14 Kasım 2026" and drawn["hero_label"] == "TOPLAM SONUÇ"
    assert await w.state() == {}                                                 # nothing stored by a command
    # the week's own card for the same data: strict, the old trade (were it this week's) would be "—"
    w.mono += COMMAND_GAP_SEC + 1
    for word in ("HEPSİ", "tüm", "hepsini", " Hepsi "):
        w.mono += COMMAND_GAP_SEC + 1
        assert "Tüm işlemlerin" in await w.service.command(word), word
        await w.settle()


async def test_karne_cannot_be_spammed_into_a_storm_of_drawing_processes(world: World) -> None:
    w = world
    w.drawer.gate = asyncio.Event()                                              # the first card is still being drawn
    replies = [await w.service.command("") for _ in range(30)]
    replies += [await w.service.command("hepsi") for _ in range(30)]
    assert replies[0] == "⏳ Karne hazırlanıyor; birkaç saniye içinde gelecek."
    assert set(replies[1:]) == {"⏳ Bir karne zaten hazırlanıyor; o gelince tekrar isteyebilirsiniz."}
    await asyncio.sleep(0.05)
    assert len(w.drawer.cards) == 1 and w.drawer.most == 1
    w.drawer.gate.set()
    await w.settle()
    assert len(w.owner.files) == 1

    # the card is out; the next one is accepted only a minute after the last accepted command
    assert await w.service.command("") == "⏳ Az önce bir karne istendi; 60 sn sonra tekrar deneyin."
    w.mono += 45.4
    assert await w.service.command("hepsi") == "⏳ Az önce bir karne istendi; 15 sn sonra tekrar deneyin."
    w.mono += COMMAND_GAP_SEC - 45.4 - 0.01
    assert "sonra tekrar deneyin" in await w.service.command("")
    assert len(w.drawer.cards) == 1                                              # the refusals drew nothing
    w.mono += 0.02
    assert await w.service.command("") == "⏳ Karne hazırlanıyor; birkaç saniye içinde gelecek."
    await w.settle()
    assert len(w.drawer.cards) == 2 and w.drawer.most == 1


async def test_karne_while_the_weekly_card_is_being_made_waits_its_turn(world: World) -> None:
    w = world
    w.drawer.gate = asyncio.Event()
    await started(w)
    w.sleeper.until = SUNDAY + timedelta(hours=1)
    w.sleeper._go.set()
    for _ in range(500):
        if w.drawer.running:
            break
        await asyncio.sleep(0.01)
    assert await w.service.command("") == "⏳ Bir karne zaten hazırlanıyor; o gelince tekrar isteyebilirsiniz."
    w.drawer.gate.set()
    await asyncio.wait_for(w.sleeper.reached.wait(), 20)
    assert len(w.owner.files) == 1 and w.drawer.most == 1
    assert await w.service.command("") == "⏳ Karne hazırlanıyor; birkaç saniye içinde gelecek."      # now it may
    await w.settle()
    assert [f["name"] for f in w.owner.files] == ["karne_2026-11-15.png", "karne_ara_2026-11-15.png"]


async def test_karne_with_a_word_it_does_not_know_explains_itself(world: World) -> None:
    for word in ("gecen", "2", "hepsi lütfen"):
        assert await world.service.command(word) == (
            "Kullanım: <code>/karne</code> (bu hafta, şu ana kadar) · <code>/karne hepsi</code> (bugüne kadarki tüm "
            "işlemler)")
    assert world.drawer.cards == []
    assert "hazırlanıyor" in await world.service.command("")                     # (a refusal used up no turn)
    await world.settle()


async def test_a_card_that_cannot_be_made_on_demand_is_told_not_raised(world: World) -> None:
    w = world

    class NoDb:
        async def get_state(self, *_a: Any) -> Any:
            return None

        async def closed_trades_between(self, *_a: Any) -> Any:
            raise OSError("disk <dolu>")

    w.service = w.new_service(db=NoDb())
    assert "hazırlanıyor" in await w.service.command("")
    await w.settle()
    assert w.owner.texts == ["🚨 Karne hazırlanamadı: <code>disk &lt;dolu&gt;</code>"] and w.owner.files == []


def test_the_file_names() -> None:
    from copytrader.karne.data import Report

    end = datetime(2026, 7, 12, 23, 30, tzinfo=UTC)                              # 00:30 on the 13th in the UK
    assert file_name(Report(MODE_WEEK, end - WEEK, end, [], [])) == "karne_2026-07-13.png"
    assert file_name(Report(MODE_INTERIM, end - WEEK, end, [], [])) == "karne_ara_2026-07-13.png"
    assert file_name(Report(MODE_ALL, None, end, [], [])) == "karne_tum_2026-07-13.png"


async def test_files_a_crash_left_behind_are_removed_at_start(world: World) -> None:
    folder = world.tmp_path / "karne"
    folder.mkdir()
    old, fresh, other = folder / "karne_dead.png", folder / "karne_new.json", folder / "notlar.txt"
    for path in (old, fresh, other):
        path.write_bytes(b"x")
    long_ago = time.time() - 7200
    os.utime(old, (long_ago, long_ago))
    os.utime(other, (long_ago, long_ago))
    world.service.start()
    deadline = time.monotonic() + 3
    while old.exists() and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    assert not old.exists() and fresh.exists() and other.exists()               # only its own, only the old ones


# --------------------------------------------------------------------------- #
# Review round
# --------------------------------------------------------------------------- #
async def test_review_p3_karne_after_a_skipped_card_does_not_call_a_longer_period_the_week(world: World) -> None:
    """krev/data/misc_checks.py (a): the card of 08.11 was skipped (the bot came back 30 h late); ``/karne`` on
    Wednesday 11.11 covers 1-11 November. It used to say "HAFTALIK SONUÇ · ŞU ANA KADAR", "HAFTANIN İŞLEMLERİ" and
    "Hafta sürüyor"."""
    w = world
    first_cut = SUNDAY - 2 * WEEK                                                # 01.11 22:00
    await w.db.set_state(STATE_KEY, {"handled": first_cut.isoformat(), "cutoff": first_cut.isoformat(), "note": "init"})
    await w.trade(SUNDAY - WEEK - timedelta(days=2), 20.0, "ETH")                # Fri 06.11: the skipped week's
    await w.trade(SUNDAY - timedelta(days=5), 10.0, "BTC")                       # Tue 10.11: this week's
    w.clock.now = SUNDAY - WEEK + timedelta(hours=30)                            # back 30 h after Sun 08.11 22:00
    w.sleeper.until = w.clock.now + timedelta(minutes=10)
    w.service.start()
    await asyncio.wait_for(w.sleeper.reached.wait(), 20)
    assert len(w.owner.texts) == 1 and "Haftalık karne gönderilmedi" in w.owner.texts[0]
    w.clock.now = SUNDAY - timedelta(days=4)                                     # Wed 11.11 22:00
    assert "hazırlanıyor" in await w.service.command("")
    await w.settle()
    (card,) = w.owner.files
    (drawn,) = w.drawer.cards
    assert drawn["subtitle"] == "ARA KARNE · 1–11 Kasım 2026"
    assert (drawn["hero_label"], drawn["table_label"], drawn["empty_text"]) == (
        "DÖNEM SONUCU · ŞU ANA KADAR", "DÖNEMİN İŞLEMLERİ", "Bu dönemde kapanan işlem yok")
    assert [t["pair"] for t in drawn["trades"]] == ["ETH", "BTC"]
    assert card["caption"].splitlines() == [
        "📊 <b>Ara karne · 1–11 Kasım 2026</b>",
        "Dönem: 01.11 Paz 22:00 – 11.11 Çar 22:00 (İngiltere saati)",
        "Toplam +3.0R · 2 işlem (2 kazanç · 0 kayıp) · kazanma %100",
        "Dönem sürüyor: yalnızca tamamen kapanmış işlemler sayıldı, açık işlemler dahil değil.",
        "Önceki haftalık karne gönderilemediği için o haftanın işlemleri de bu karnede."]
    for wrong in ("HAFTALIK", "HAFTANIN", "Hafta sürüyor", "Bu hafta"):
        assert wrong not in card["caption"] + str(drawn), wrong


async def test_review_a_plain_karne_inside_the_week_keeps_the_weeks_words(world: World) -> None:
    w = world
    await started(w)
    await w.sleeper.run_to(w.clock.now)
    await w.service.command("")
    await w.settle()
    (drawn,) = w.drawer.cards
    assert drawn["hero_label"] == "HAFTALIK SONUÇ · ŞU ANA KADAR" and "table_label" not in drawn
    assert "Hafta sürüyor: yalnızca tamamen kapanmış işlemler sayıldı, açık işlemler dahil değil." in (
        w.owner.files[0]["caption"])


async def test_review_a_trade_with_an_order_in_flight_waits_for_the_next_weekly_card_and_is_on_it_once(
        world: World) -> None:
    """The service's side of the late-leg case (the engine's side: tests/test_karne_engine.py): the trade is left
    out, its id is stored, the next card takes it; a trade a card listed is stored too and never listed again -
    also when its end time has moved into the next week meanwhile."""
    w = world
    await w.trade(SUNDAY - timedelta(days=3), 10.0, "AAA")                       # trade 1
    await w.trade(SUNDAY - timedelta(minutes=2), -10.0, "BBB")                   # trade 2: ended, a leg in flight
    w.in_flight = {2}
    await started(w)
    await w.sleeper.run_to(SUNDAY + timedelta(hours=1))
    assert [t["pair"] for t in w.drawer.cards[0]["trades"]] == ["AAA"]
    assert ("Bir hesabın açılış emri hâlâ yanıt beklediği için 1 işlem bu karneye alınmadı; sonuçlanınca yer "
            "alacak.") in w.owner.files[0]["caption"]
    state = await w.state()
    assert (state["carry"], state["listed"]) == ([2], [1])
    # /karne in between: the week so far would be empty by the end times; the carried trade is still in flight
    assert "hazırlanıyor" in await w.service.command("")
    await w.settle()
    assert w.drawer.cards[1]["trades"] == [] and await w.state() == state        # (a command stores nothing)
    # the late order has answered; trade 1's end time is moved into the new week by something (as a late leg does)
    w.in_flight = set()
    await w.db.update_trade(1, status="open")
    w.clock.now, now = SUNDAY + timedelta(days=2), w.clock.now
    await w.db.update_trade(1, status="closed")
    w.clock.now = now
    await w.trade(SUNDAY + timedelta(days=3), 30.0, "CCC")                       # trade 3
    await w.sleeper.run_to(SUNDAY + timedelta(days=4))                           # Thursday
    w.mono += 1000
    assert "hazırlanıyor" in await w.service.command("")                         # the preview of the next card
    await w.settle()
    assert [t["pair"] for t in w.drawer.cards[2]["trades"]] == ["BBB", "CCC"]
    await w.sleeper.run_to(SUNDAY + WEEK + timedelta(hours=1))
    assert [t["pair"] for t in w.drawer.cards[3]["trades"]] == ["BBB", "CCC"]    # BBB once, AAA not again
    state = await w.state()
    assert (state["carry"], state["listed"], state["cutoff"]) == ([], [2, 3, 1], (SUNDAY + WEEK).isoformat())
    # a skipped card and a restart keep both lists
    await w.service.stop()
    w.clock.now = SUNDAY + 2 * WEEK + timedelta(hours=30)
    w.sleeper.until = w.clock.now + timedelta(minutes=5)
    w.sleeper.reached.clear()
    w.service = w.new_service()
    w.service.start()
    await asyncio.wait_for(w.sleeper.reached.wait(), 20)
    skipped = await w.state()
    assert (skipped["note"], skipped["carry"], skipped["listed"]) == ("skipped", [], [2, 3, 1])


async def test_review_the_stored_trade_ids_are_bounded_and_survive_an_old_state(world: World) -> None:
    w = world
    await w.db.set_state(STATE_KEY, {"handled": (SUNDAY - WEEK).isoformat(), "cutoff": (SUNDAY - WEEK).isoformat(),
                                     "listed": list(range(1000, 1600)), "carry": "bozuk"})
    await w.trade(SUNDAY - timedelta(days=3), 10.0, "AAA")
    w.service.start()
    await w.sleeper.run_to(SUNDAY + timedelta(hours=1))
    state = await w.state()
    assert len(state["listed"]) == service_mod.LISTED_MAX == 500 and state["listed"][:3] == [1, 1000, 1001]
    assert state["carry"] == [] and len(w.owner.files) == 1


async def test_review_a_stray_cancelled_error_does_not_end_the_schedule_silently(
        world: World, caplog: pytest.LogCaptureFixture) -> None:
    """A CancelledError that nobody asked for (it comes out of a library call) used to end the schedule's task
    without a word: no weekly card until the next restart."""
    w = world
    calls = {"n": 0}

    async def drawer(data: dict[str, Any], folder: Any) -> RenderResult:
        calls["n"] += 1
        if calls["n"] == 1:
            raise asyncio.CancelledError()               # out of "a library": this task was never cancelled
        return await w.drawer(data, folder)

    w.service = w.new_service(renderer=drawer)
    await started(w)
    await w.sleeper.run_to(SUNDAY + timedelta(minutes=30))
    task = w.service._task
    assert task is not None and not task.done()          # the schedule is still there
    assert "a CancelledError nobody asked for came out of the schedule" in caplog.text
    assert len(w.owner.files) == 1 and calls["n"] == 2   # ... and the card went out at its next look
    # on demand: told to the owner instead of a task that ends without a word
    calls["n"] = 0
    assert "hazırlanıyor" in await w.service.command("")
    await w.settle()
    assert w.owner.texts[-1] == "🚨 Karne hazırlanamadı: <code>CancelledError</code>"
    # a cancellation that IS asked for still ends it at once
    await w.service.stop()
    assert task.done()


async def test_review2_a_trade_stuck_in_flight_is_counted_and_listed_after_two_cards(world: World) -> None:
    w = world
    await w.trade(SUNDAY - timedelta(days=3), 10.0, "AAA")                       # trade 1
    await w.trade(SUNDAY - timedelta(minutes=2), -10.0, "BBB")                   # trade 2: its order never answers
    w.in_flight = {2}
    await started(w)
    seen = []
    for week in range(3):
        await w.sleeper.run_to(SUNDAY + timedelta(days=7 * week, hours=1))
        seen.append(([t["pair"] for t in w.drawer.cards[week]["trades"]], dict(await w.state())))
    assert [pairs for pairs, _s in seen] == [["AAA"], [], ["BBB"]]
    assert [(s["carry"], s["waited"]) for _p, s in seen] == [([2], {"2": 1}), ([2], {"2": 2}), ([], {})]
    assert ("1 işlem 2 karne boyunca bekletildi ve yine de listelendi: bir hesabın açılış emri hâlâ yanıtsız, o "
            "hesabın sonucu eksik olabilir.") in w.owner.files[2]["caption"]
    assert "önceki haftadan" in w.owner.files[2]["caption"]                       # (and it is a carried trade)


# --------------------------------------------------------------------------- #
# one card per person: the owner's (every account but the friend's) and the friend's (his only), same trades
# --------------------------------------------------------------------------- #
class OwnerAndFriend(Owner):
    def __init__(self, db: Database | None = None, clock: Clock | None = None) -> None:
        super().__init__(db, clock)
        self.friend_files: list[dict[str, Any]] = []
        self.friend_texts: list[str] = []

    def has_friend(self) -> bool:
        return True

    async def send_friend_document(self, path: str, *, caption: str = "", name: str | None = None,
                                   busy: Any = None) -> str | None:
        self.friend_files.append({"caption": caption, "name": name})
        return None

    async def tell_friend(self, text: str) -> None:
        self.friend_texts.append(text)


async def test_the_weekly_cards_of_both_people_are_of_the_same_trades_each_with_his_own_accounts(
        world: World) -> None:
    w = world
    w.owner = OwnerAndFriend(w.db, w.clock)
    looks: list[int] = []

    def pending(trade_id: int) -> bool:          # an answer arrives between two looks: both cards see the first
        looks.append(trade_id)
        return trade_id == 2 and looks.count(2) == 1

    w.service = KarneService(w.db, w.owner, w.tmp_path, accounts=["binance", "fx"], friend_accounts=["tickmill"],
                             busy=lambda: False, pending=pending, clock=w.clock, sleep=w.sleeper,
                             renderer=w.drawer, pillow=lambda: True, monotonic=lambda: w.mono)
    now = w.clock.now
    await closed(w.db, w.clock, SUNDAY - timedelta(days=3), [position("binance", 20.0), position("tickmill", 48.0)],
                 asset="AAA")
    await closed(w.db, w.clock, SUNDAY - timedelta(minutes=2), [position("fx", -10.0), position("tickmill", -5.0)],
                 asset="BBB")
    await closed(w.db, w.clock, SUNDAY - timedelta(days=1), [position("old_account", 10.0)], asset="CCC")
    w.clock.now = now
    await started(w)
    await w.sleeper.run_to(SUNDAY + timedelta(hours=1))
    owner_card, friend_card = w.drawer.cards[0], w.drawer.cards[1]
    assert [t["pair"] for t in owner_card["trades"]] == ["AAA", "CCC"]           # BBB waits on both cards
    assert [a["name"] for a in owner_card["accounts"]] == ["binance", "fx", "old_account"]   # never tickmill
    assert [t["pair"] for t in friend_card["trades"]] == ["AAA"]
    assert [a["name"] for a in friend_card["accounts"]] == ["tickmill"]
    assert len(w.owner.files) == 1 and len(w.owner.friend_files) == 1
    state = await w.state()
    assert (state["carry"], sorted(state["listed"])) == ([2], [1, 3])
    assert looks.count(2) == 1                                                     # looked at once for both
