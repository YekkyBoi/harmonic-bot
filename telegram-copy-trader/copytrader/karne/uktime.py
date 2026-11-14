"""UK wall-clock time for the weekly card (Sunday 22:00 in the UK), without a time-zone database.

Windows has no tz database of its own; ``zoneinfo`` works there only with the ``tzdata`` package. The UK rule is
therefore implemented here (:func:`rule_offset`: British Summer Time, UTC+1, from 01:00 UTC on the last Sunday of March
to 01:00 UTC on the last Sunday of October; UTC+0 otherwise) and used whenever ``zoneinfo`` cannot give
"Europe/London". When it can, it is asked (it would also know a future change of the law); tests compare the two.

Every function takes and returns timezone-aware UTC datetimes, except the "wall" values, which are naive UK
wall-clock times.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any

UTC = timezone.utc
SLOT_WEEKDAY = 6                 # Sunday (datetime.weekday(): Monday is 0)
SLOT_TIME = time(22, 0)          # UK wall-clock time of the weekly card
_HOUR = timedelta(hours=1)
_ZERO = timedelta(0)

_MONTHS_TR = ("Ocak", "Şubat", "Mart", "Nisan", "Mayıs", "Haziran", "Temmuz", "Ağustos", "Eylül", "Ekim", "Kasım",
              "Aralık")
_DAYS_TR = ("Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz")


def _london() -> Any:
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo("Europe/London")
    except Exception:          # noqa: BLE001 - no tz database on this machine: the rule below
        return None


_ZONE: Any = _london()


def as_utc(moment: datetime) -> datetime:
    """``moment`` as an aware UTC datetime (a naive one is taken as UTC, like the database's own times)."""
    return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)


def last_sunday(year: int, month: int) -> date:
    """The last Sunday of a month."""
    first_next = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    last = first_next - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - SLOT_WEEKDAY) % 7)


def rule_offset(moment: datetime) -> timedelta:
    """UK time minus UTC at ``moment``, by the rule alone: +1 h from 01:00 UTC on the last Sunday of March until
    01:00 UTC on the last Sunday of October, else 0."""
    moment = as_utc(moment)
    start = datetime.combine(last_sunday(moment.year, 3), time(1, 0), tzinfo=UTC)
    end = datetime.combine(last_sunday(moment.year, 10), time(1, 0), tzinfo=UTC)
    return _HOUR if start <= moment < end else _ZERO


def uk_offset(moment: datetime) -> timedelta:
    """UK time minus UTC at ``moment``: from the tz database when this machine has one, else :func:`rule_offset`."""
    moment = as_utc(moment)
    if _ZONE is not None:
        try:
            offset = moment.astimezone(_ZONE).utcoffset()
            if offset is not None:
                return offset
        except Exception:      # noqa: BLE001 - a broken tz database: the rule
            pass
    return rule_offset(moment)


def to_uk(moment: datetime) -> datetime:
    """The UK wall-clock time (naive) of ``moment``."""
    moment = as_utc(moment)
    return (moment + uk_offset(moment)).replace(tzinfo=None)


def uk_to_utc(wall: datetime) -> datetime:
    """The moment at which UK clocks show ``wall`` (naive). In the hour that happens twice in October the first one
    (summer time) is meant; a time the clocks skip in March is read as winter time."""
    wall = wall.replace(tzinfo=None)
    for offset in (_HOUR, _ZERO):
        moment = wall.replace(tzinfo=UTC) - offset
        if uk_offset(moment) == offset:
            return moment
    return wall.replace(tzinfo=UTC)


def last_slot(now: datetime) -> datetime:
    """The most recent Sunday 22:00 UK time that is not after ``now``."""
    now = as_utc(now)
    wall = to_uk(now)
    sunday = wall.date() - timedelta(days=(wall.weekday() - SLOT_WEEKDAY) % 7)
    slot = uk_to_utc(datetime.combine(sunday, SLOT_TIME))
    if slot > now:
        slot = uk_to_utc(datetime.combine(sunday - timedelta(days=7), SLOT_TIME))
    return slot


def next_slot(now: datetime) -> datetime:
    """The first Sunday 22:00 UK time after ``now``."""
    previous = last_slot(now)
    return uk_to_utc(datetime.combine(to_uk(previous).date() + timedelta(days=7), SLOT_TIME))


def previous_slot(slot: datetime) -> datetime:
    """The Sunday 22:00 UK time one week before the slot ``slot``."""
    return uk_to_utc(datetime.combine(to_uk(slot).date() - timedelta(days=7), SLOT_TIME))


# ---- Turkish texts ----------------------------------------------------------------------------------------------
def date_range_tr(start: datetime, end: datetime) -> str:
    """The UK dates of two moments as one range: "8–15 Kasım 2026", "29 Kasım – 6 Aralık 2026",
    "28 Aralık 2026 – 3 Ocak 2027"; one date when both fall on the same day ("14 Kasım 2026")."""
    a, b = to_uk(start).date(), to_uk(end).date()
    if a == b:
        return f"{b.day} {_MONTHS_TR[b.month - 1]} {b.year}"
    if a.year != b.year:
        return f"{a.day} {_MONTHS_TR[a.month - 1]} {a.year} – {b.day} {_MONTHS_TR[b.month - 1]} {b.year}"
    if a.month != b.month:
        return f"{a.day} {_MONTHS_TR[a.month - 1]} – {b.day} {_MONTHS_TR[b.month - 1]} {b.year}"
    return f"{a.day}–{b.day} {_MONTHS_TR[b.month - 1]} {b.year}"


def stamp_tr(moment: datetime, *, year: bool = False) -> str:
    """A moment in UK time as the owner reads it: "15.11 Paz 22:00" ("15.11.2026 Paz 22:00" with ``year``)."""
    wall = to_uk(moment)
    day = wall.strftime("%d.%m.%Y" if year else "%d.%m")
    return f"{day} {_DAYS_TR[wall.weekday()]} {wall.strftime('%H:%M')}"


__all__ = ["UTC", "as_utc", "date_range_tr", "last_slot", "last_sunday", "next_slot", "previous_slot", "rule_offset",
           "stamp_tr", "to_uk", "uk_offset", "uk_to_utc"]
