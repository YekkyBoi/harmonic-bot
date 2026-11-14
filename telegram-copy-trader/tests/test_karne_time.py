"""UK time for the weekly report card (copytrader/karne/uktime.py): Sunday 22:00 in the UK, with and without a time-zone
database (Windows has none of its own).

The rule: British Summer Time (UTC+1) from 01:00 UTC on the last Sunday of March to 01:00 UTC on the last Sunday of
October. Sunday 22:00 UK time is therefore 21:00 UTC in summer and 22:00 UTC in winter; the week that ends on the
last Sunday of March has 167 hours, the one that ends on the last Sunday of October 169.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from copytrader.karne import uktime
from copytrader.karne.uktime import (
    date_range_tr,
    last_slot,
    last_sunday,
    next_slot,
    previous_slot,
    rule_offset,
    stamp_tr,
    to_uk,
    uk_offset,
    uk_to_utc,
)

H = timedelta(hours=1)


def utc(*parts: int) -> datetime:
    return datetime(*parts, tzinfo=UTC)          # type: ignore[misc]


@pytest.fixture(params=["tz database", "rule only"])
def clock_source(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> str:
    """Every slot test runs twice: with this machine's tz database (when it has one) and with the rule alone, as on a
    Windows server without the tzdata package."""
    if request.param == "rule only":
        monkeypatch.setattr(uktime, "_ZONE", None)
    elif uktime._ZONE is None:
        pytest.skip("this machine has no tz database (Europe/London)")
    return str(request.param)


def test_the_last_sundays() -> None:
    assert last_sunday(2026, 3) == date(2026, 3, 29) and last_sunday(2026, 10) == date(2026, 10, 25)
    assert last_sunday(2027, 3) == date(2027, 3, 28) and last_sunday(2027, 10) == date(2027, 10, 31)
    assert last_sunday(2024, 3) == date(2024, 3, 31) and last_sunday(2028, 12) == date(2028, 12, 31)
    for year in range(2020, 2041):
        for month in (3, 10, 12):
            day = last_sunday(year, month)
            assert day.weekday() == 6 and (day + timedelta(days=7)).month != month


def test_the_rule_changes_at_one_o_clock_utc_on_the_last_sundays() -> None:
    assert rule_offset(utc(2026, 3, 29, 0, 59, 59)) == timedelta(0)
    assert rule_offset(utc(2026, 3, 29, 1, 0, 0)) == H
    assert rule_offset(utc(2026, 7, 1, 12)) == H
    assert rule_offset(utc(2026, 10, 25, 0, 59, 59)) == H
    assert rule_offset(utc(2026, 10, 25, 1, 0, 0)) == timedelta(0)
    assert rule_offset(utc(2026, 12, 31, 23)) == timedelta(0) and rule_offset(utc(2026, 1, 1)) == timedelta(0)
    assert rule_offset(datetime(2026, 7, 1, 12)) == H                      # (a naive time is taken as UTC)


def test_the_rule_agrees_with_the_tz_database_every_hour_around_every_change_and_every_sunday() -> None:
    zoneinfo = pytest.importorskip("zoneinfo")
    try:
        london = zoneinfo.ZoneInfo("Europe/London")
    except Exception:      # noqa: BLE001
        pytest.skip("this machine has no tz database (Europe/London)")
    checked = 0
    for year in range(2015, 2041):
        for month in (3, 10):
            change = datetime.combine(last_sunday(year, month), datetime.min.time(), tzinfo=UTC) + H
            for minutes in range(-72 * 60, 72 * 60 + 1, 30):                 # three days around each change
                moment = change + timedelta(minutes=minutes)
                assert rule_offset(moment) == moment.astimezone(london).utcoffset(), moment
                checked += 1
        day = datetime(year, 1, 1, 21, 30, tzinfo=UTC)
        while day.year == year:                                              # every evening of the year
            assert rule_offset(day) == day.astimezone(london).utcoffset(), day
            day += timedelta(days=1)
            checked += 1
    assert checked > 20000


def test_without_a_tz_database_the_rule_is_used(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(uktime, "_ZONE", None)
    assert uk_offset(utc(2026, 7, 1)) == H and uk_offset(utc(2026, 1, 1)) == timedelta(0)

    class Broken:
        def utcoffset(self, _moment: object) -> None:
            raise RuntimeError("no tz data")

        def fromutc(self, _moment: object) -> None:
            raise RuntimeError("no tz data")

    monkeypatch.setattr(uktime, "_ZONE", Broken())                           # a broken database: the rule as well
    assert uk_offset(utc(2026, 7, 1)) == H


def test_sunday_ten_pm_is_21_utc_in_summer_and_22_utc_in_winter(clock_source: str) -> None:
    assert last_slot(utc(2026, 11, 14, 13, 0)) == utc(2026, 11, 8, 22, 0)        # a Saturday in winter
    assert next_slot(utc(2026, 11, 14, 13, 0)) == utc(2026, 11, 15, 22, 0)
    assert last_slot(utc(2026, 7, 15, 9, 0)) == utc(2026, 7, 12, 21, 0)          # a Wednesday in summer
    assert next_slot(utc(2026, 7, 15, 9, 0)) == utc(2026, 7, 19, 21, 0)
    # the slot itself counts as reached; one second earlier it is still the week before
    assert last_slot(utc(2026, 11, 15, 22, 0)) == utc(2026, 11, 15, 22, 0)
    assert last_slot(utc(2026, 11, 15, 21, 59, 59)) == utc(2026, 11, 8, 22, 0)
    assert next_slot(utc(2026, 11, 15, 22, 0)) == utc(2026, 11, 22, 22, 0)
    # the UK's Sunday decides, whatever day it is elsewhere: Monday 01:30 UTC is still the week that began at 22:00
    assert last_slot(datetime(2026, 11, 16, 1, 30, tzinfo=UTC)) == utc(2026, 11, 15, 22, 0)
    # a time with another offset is the same moment (Istanbul's Monday 00:59 = Sunday 21:59 UTC: not yet)
    istanbul = timezone(timedelta(hours=3))
    assert last_slot(datetime(2026, 11, 16, 0, 59, tzinfo=istanbul)) == utc(2026, 11, 8, 22, 0)
    assert last_slot(datetime(2026, 11, 16, 1, 0, tzinfo=istanbul)) == utc(2026, 11, 15, 22, 0)


def test_the_week_the_clocks_go_forward_has_167_hours(clock_source: str) -> None:
    before, change = utc(2026, 3, 22, 22, 0), utc(2026, 3, 29, 21, 0)             # GMT, then BST from 01:00 UTC
    assert last_slot(utc(2026, 3, 29, 20, 59, 59)) == before
    assert last_slot(utc(2026, 3, 29, 21, 0)) == change                           # 22:00 UK = 21:00 UTC that evening
    assert last_slot(utc(2026, 3, 29, 21, 59)) == change                          # (22:00 UTC would be an hour late)
    assert next_slot(before) == change and previous_slot(change) == before
    assert change - before == timedelta(hours=167)
    assert next_slot(change) == utc(2026, 4, 5, 21, 0) and next_slot(change) - change == timedelta(hours=168)


def test_the_week_the_clocks_go_back_has_169_hours(clock_source: str) -> None:
    before, change = utc(2026, 10, 18, 21, 0), utc(2026, 10, 25, 22, 0)           # BST, then GMT from 01:00 UTC
    assert last_slot(utc(2026, 10, 25, 21, 0)) == before                          # 21:00 UTC is only 21:00 UK now
    assert last_slot(utc(2026, 10, 25, 21, 59, 59)) == before
    assert last_slot(utc(2026, 10, 25, 22, 0)) == change
    assert next_slot(before) == change and previous_slot(change) == before
    assert change - before == timedelta(hours=169)
    assert next_slot(change) == utc(2026, 11, 1, 22, 0)


def test_every_slot_of_fifteen_years_is_a_sunday_at_ten_pm_uk_time(clock_source: str) -> None:
    slot = last_slot(utc(2026, 1, 1))
    seen = set()
    for _ in range(15 * 53):
        following = next_slot(slot)
        wall = to_uk(following)
        assert wall.weekday() == 6 and (wall.hour, wall.minute, wall.second) == (22, 0, 0), following
        assert following - slot in (timedelta(hours=167), timedelta(hours=168), timedelta(hours=169))
        assert previous_slot(following) == slot and last_slot(following) == following
        assert last_slot(following - timedelta(seconds=1)) == slot
        assert uk_to_utc(wall) == following
        seen.add((following - slot).total_seconds() / 3600)
        slot = following
    assert seen == {167.0, 168.0, 169.0}


def test_wall_clock_conversion_in_the_changed_hours(clock_source: str) -> None:
    assert to_uk(utc(2026, 7, 1, 12, 0)) == datetime(2026, 7, 1, 13, 0)
    assert to_uk(utc(2026, 1, 1, 12, 0)) == datetime(2026, 1, 1, 12, 0)
    assert uk_to_utc(datetime(2026, 7, 1, 13, 0)) == utc(2026, 7, 1, 12, 0)
    # 01:30 happens twice on the October Sunday: the first one (still summer time) is meant
    assert uk_to_utc(datetime(2026, 10, 25, 1, 30)) == utc(2026, 10, 25, 0, 30)
    # 01:30 does not exist on the March Sunday: read as winter time, never an error
    assert uk_to_utc(datetime(2026, 3, 29, 1, 30)) == utc(2026, 3, 29, 1, 30)


def test_the_dates_and_times_the_owner_reads(clock_source: str) -> None:
    assert date_range_tr(utc(2026, 11, 8, 22), utc(2026, 11, 15, 22)) == "8–15 Kasım 2026"
    assert date_range_tr(utc(2026, 11, 29, 22), utc(2026, 12, 6, 22)) == "29 Kasım – 6 Aralık 2026"
    assert date_range_tr(utc(2026, 12, 27, 22), utc(2027, 1, 3, 22)) == "27 Aralık 2026 – 3 Ocak 2027"
    assert date_range_tr(utc(2026, 11, 14, 9), utc(2026, 11, 14, 18)) == "14 Kasım 2026"
    # the UK date decides: 23:30 UTC in summer is already the next day there
    assert date_range_tr(utc(2026, 7, 12, 21), utc(2026, 7, 14, 23, 30)) == "12–15 Temmuz 2026"
    assert stamp_tr(utc(2026, 11, 15, 22, 0)) == "15.11 Paz 22:00"
    assert stamp_tr(utc(2026, 7, 12, 21, 0)) == "12.07 Paz 22:00"
    assert stamp_tr(utc(2026, 11, 12, 14, 5), year=True) == "12.11.2026 Per 14:05"
