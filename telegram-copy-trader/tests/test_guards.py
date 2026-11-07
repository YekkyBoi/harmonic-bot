from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from copytrader.models import EntryType, Side, TradePlan, TradeRecord, TradeStatus
from copytrader.risk.guards import daily_loss_exceeded, find_duplicate, max_open_trades_reached, start_of_day

NOW = datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)


def plan(
    *, asset="ETH", side=Side.LONG, entry: float | None = 2232.18, stop=2205.11, plan_id="new", created=NOW
) -> TradePlan:
    return TradePlan(
        plan_id=plan_id, base_asset=asset, side=side, entry_type=EntryType.MARKET, entry_price=entry,
        stop_loss=stop, risk_r=1.0, risk_pct=2.0, source_chat_id=-100, created_at=created,
    )


def trade(
    tid: int, *, age_sec: float = 60, status=TradeStatus.OPEN, **plan_kw
) -> TradeRecord:
    created = NOW - timedelta(seconds=age_sec)
    p = plan(plan_id=f"old{tid}", created=created, **plan_kw)
    return TradeRecord(
        id=tid, plan=p, status=status, current_stop_loss=p.stop_loss, created_at=created, updated_at=created
    )


# --------------------------------------------------------------------------- #
# find_duplicate
# --------------------------------------------------------------------------- #
def test_duplicate_found_same_asset_side_entry():
    t = trade(1)
    assert find_duplicate(plan(), [t], window_sec=900, now=NOW) is t


def test_duplicate_entry_within_tolerance():
    t = trade(1, entry=2240.0)  # 0.35% away
    assert find_duplicate(plan(), [t], window_sec=900, now=NOW) is t


def test_not_duplicate_entry_outside_tolerance():
    t = trade(1, entry=2250.0)  # ~0.8% away
    assert find_duplicate(plan(), [t], window_sec=900, now=NOW) is None
    assert find_duplicate(plan(), [t], window_sec=900, now=NOW, entry_tolerance_pct=1.0) is t


def test_not_duplicate_outside_window():
    t = trade(1, age_sec=901)
    assert find_duplicate(plan(), [t], window_sec=900, now=NOW) is None
    assert find_duplicate(plan(), [trade(2, age_sec=900)], window_sec=900, now=NOW) is not None


def test_not_duplicate_other_side_or_asset():
    assert find_duplicate(plan(), [trade(1, side=Side.SHORT, stop=2260.0)], window_sec=900, now=NOW) is None
    assert find_duplicate(plan(), [trade(2, asset="BTC")], window_sec=900, now=NOW) is None


def test_asset_compare_is_case_insensitive():
    assert find_duplicate(plan(asset="eth"), [trade(1)], window_sec=900, now=NOW) is not None


@pytest.mark.parametrize("status", [TradeStatus.CANCELLED, TradeStatus.FAILED])
def test_cancelled_and_failed_are_ignored(status):
    assert find_duplicate(plan(), [trade(1, status=status)], window_sec=900, now=NOW) is None


@pytest.mark.parametrize("status", [TradeStatus.OPENING, TradeStatus.PENDING, TradeStatus.OPEN, TradeStatus.CLOSED])
def test_other_statuses_count(status):
    assert find_duplicate(plan(), [trade(1, status=status)], window_sec=900, now=NOW) is not None


def test_stop_used_when_entry_missing():
    assert find_duplicate(plan(entry=None), [trade(1)], window_sec=900, now=NOW) is not None
    assert find_duplicate(plan(entry=None, stop=2180.0), [trade(1)], window_sec=900, now=NOW) is None
    # entry missing on the old trade
    assert find_duplicate(plan(), [trade(2, entry=None)], window_sec=900, now=NOW) is not None


def test_most_recent_match_returned():
    older, newer = trade(1, age_sec=600), trade(2, age_sec=30)
    assert find_duplicate(plan(), [older, newer], window_sec=900, now=NOW) is newer


def test_same_plan_is_not_its_own_duplicate():
    t = trade(1)
    assert find_duplicate(plan(plan_id=t.plan.plan_id), [t], window_sec=900, now=NOW) is None


def test_window_zero_disables():
    assert find_duplicate(plan(), [trade(1)], window_sec=0, now=NOW) is None


def test_naive_datetimes_treated_as_utc():
    t = trade(1)
    naive_now = NOW.replace(tzinfo=None)
    assert find_duplicate(plan(), [t], window_sec=900, now=naive_now) is t


# --------------------------------------------------------------------------- #
# max_open_trades_reached
# --------------------------------------------------------------------------- #
def test_max_open_trades():
    active = [trade(i) for i in range(3)]
    assert max_open_trades_reached(active, 3) is True
    assert max_open_trades_reached(active, 4) is False
    assert max_open_trades_reached([], 1) is False


def test_max_open_trades_counts_only_active():
    trades = [trade(1), trade(2, status=TradeStatus.CLOSED), trade(3, status=TradeStatus.PENDING)]
    assert max_open_trades_reached(trades, 2) is True
    assert max_open_trades_reached(trades, 3) is False


def test_max_open_trades_disabled():
    assert max_open_trades_reached([trade(1)], 0) is False


# --------------------------------------------------------------------------- #
# daily_loss_exceeded
# --------------------------------------------------------------------------- #
def test_daily_loss():
    assert daily_loss_exceeded(-1000.0, 10_000.0, 10.0) is True   # exactly at the limit
    assert daily_loss_exceeded(-1200.0, 10_000.0, 10.0) is True
    assert daily_loss_exceeded(-999.99, 10_000.0, 10.0) is False
    assert daily_loss_exceeded(500.0, 10_000.0, 10.0) is False
    assert daily_loss_exceeded(0.0, 10_000.0, 10.0) is False


def test_daily_loss_disabled():
    assert daily_loss_exceeded(-9000.0, 10_000.0, 0.0) is False
    assert daily_loss_exceeded(-9000.0, 10_000.0, -1.0) is False


def test_daily_loss_without_equity_fails_safe():
    assert daily_loss_exceeded(-10.0, 0.0, 10.0) is True
    assert daily_loss_exceeded(10.0, 0.0, 10.0) is False


# --------------------------------------------------------------------------- #
# start_of_day
# --------------------------------------------------------------------------- #
def test_start_of_day_istanbul():
    sod = start_of_day(NOW, "Europe/Istanbul")  # 12:00 UTC = 15:00 Istanbul
    assert sod.tzinfo is not None
    assert sod == datetime(2026, 11, 7, 0, 0, tzinfo=ZoneInfo("Europe/Istanbul"))
    assert sod == datetime(2026, 11, 6, 21, 0, tzinfo=timezone.utc)


def test_start_of_day_istanbul_after_local_midnight():
    # 22:30 UTC = 01:30 next day in Istanbul (UTC+3)
    sod = start_of_day(datetime(2026, 11, 7, 22, 30, tzinfo=timezone.utc), "Europe/Istanbul")
    assert sod == datetime(2026, 11, 7, 21, 0, tzinfo=timezone.utc)
    assert sod.astimezone(ZoneInfo("Europe/Istanbul")).hour == 0


def test_start_of_day_naive_is_utc():
    assert start_of_day(datetime(2026, 11, 7, 22, 30), "Europe/Istanbul") == datetime(
        2026, 11, 7, 21, 0, tzinfo=timezone.utc
    )


def test_start_of_day_utc_and_dst_zone():
    assert start_of_day(NOW, "UTC") == datetime(2026, 11, 7, tzinfo=timezone.utc)
    # New York in summer (EDT, UTC-4)
    sod = start_of_day(datetime(2026, 7, 1, 3, 0, tzinfo=timezone.utc), "America/New_York")
    assert sod == datetime(2026, 6, 30, 4, 0, tzinfo=timezone.utc)
