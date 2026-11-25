"""Unit tests for number parsing and statistics rows."""

from datetime import datetime, timedelta, timezone

import pytest

from custom_components.maalerportal.parsing import parse_api_timestamp, parse_number
from custom_components.maalerportal.statistics import (
    consumption_by_hour,
    consumption_rows,
    counter_offset,
    counter_readings_by_hour,
    counter_rows,
    total_before,
)

UTC = timezone.utc


def hour(h: int, day: int = 2) -> datetime:
    return datetime(2026, 11, day, h, tzinfo=UTC)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (12, 12.0),
        (12.5, 12.5),
        ("1234.56", 1234.56),
        ("1234,56", 1234.56),
        ("1.234,56", 1234.56),
        ("1,234.56", 1234.56),
        ("1,234,567", 1234567.0),
        (" 12,5 m³", 12.5),
        ("-3,5", -3.5),
        ("", None),
        ("abc", None),
        (None, None),
        (True, None),
        (float("nan"), None),
    ],
)
def test_parse_number(raw, expected):
    assert parse_number(raw) == expected


def test_parse_timestamp():
    assert parse_api_timestamp("2026-11-02T10:05:00Z") == datetime(2026, 11, 2, 10, 5, tzinfo=UTC)
    assert parse_api_timestamp("2026-11-02T11:05:00+01:00") == datetime(2026, 11, 2, 10, 5, tzinfo=UTC)
    assert parse_api_timestamp("2026-11-02T10:05:00") == datetime(2026, 11, 2, 10, 5, tzinfo=UTC)
    assert parse_api_timestamp("nonsense") is None
    assert parse_api_timestamp(None) is None


def _reading(ts: str, value, counter="c1"):
    return {"meterCounterId": counter, "timestamp": ts, "value": value}


def test_counter_readings_move_back_one_hour_newest_wins():
    readings = [
        _reading("2026-11-02T10:05:00Z", 100.0),
        _reading("2026-11-02T10:50:00Z", 100.4),
        _reading("2026-11-02T11:05:00Z", "100,9"),
        _reading("2026-11-02T11:05:00Z", 5.0, counter="other"),
    ]
    assert counter_readings_by_hour(readings, "c1") == {hour(9): 100.4, hour(10): 100.9}


def test_counter_first_import_starts_at_zero():
    by_hour = {hour(9): 100.0, hour(10): 100.5}
    offset = counter_offset(None, by_hour)
    rows = counter_rows(by_hour, offset, {})
    assert rows == {hour(9): (100.0, 0.0), hour(10): (100.5, 0.5)}


def test_counter_next_rounds_continue_without_step():
    """The old code switched from value-baseline to the raw value here."""
    stored = {hour(9): (100.0, 0.0), hour(10): (100.5, 0.5)}
    by_hour = {hour(9): 100.0, hour(10): 100.5, hour(11): 101.0}
    offset = counter_offset(stored[hour(10)], by_hour)
    rows = counter_rows(by_hour, offset, stored)
    # Only the new hour is written, and it continues the series.
    assert rows == {hour(11): (101.0, 1.0)}


def test_counter_late_hour_fits_between_neighbours():
    stored = {hour(9): (100.0, 0.0), hour(11): (101.0, 1.0)}
    by_hour = {hour(9): 100.0, hour(10): 100.6, hour(11): 101.0}
    rows = counter_rows(by_hour, counter_offset(stored[hour(11)], by_hour), stored)
    assert rows == {hour(10): (100.6, 0.6)}


def test_consumption_by_hour_sums_quarters_and_drops_negatives():
    readings = [
        _reading("2026-11-02T10:00:00Z", 0.25),
        _reading("2026-11-02T10:15:00Z", "0,25"),
        _reading("2026-11-02T10:30:00Z", -1),
        _reading("2026-11-02T11:00:00Z", 1.0),
    ]
    assert consumption_by_hour(readings, "c1") == {hour(10): 0.5, hour(11): 1.0}


def test_consumption_first_import():
    rows, total = consumption_rows({hour(10): 1.0, hour(11): 2.0}, {}, total_before({}, None))
    assert rows == {hour(10): (1.0, 1.0), hour(11): (2.0, 3.0)}
    assert total == 3.0


def test_consumption_continues_from_rows_before_window():
    last = (2.0, 50.0)
    rows, total = consumption_rows({hour(12): 1.0}, {}, total_before({}, last))
    assert rows == {hour(12): (1.0, 51.0)}
    assert total == 51.0


def test_consumption_late_hour_rebases_later_rows():
    stored = {hour(10): (1.0, 11.0), hour(12): (3.0, 14.0)}
    api = {hour(10): 1.0, hour(11): 2.0, hour(12): 3.0}
    rows, total = consumption_rows(api, stored, total_before(stored, (3.0, 14.0)))
    assert rows == {hour(11): (2.0, 13.0), hour(12): (3.0, 16.0)}
    assert total == 16.0


def test_consumption_unchanged_window_writes_nothing():
    stored = {hour(10): (1.0, 11.0), hour(11): (2.0, 13.0)}
    rows, total = consumption_rows({hour(10): 1.0, hour(11): 2.0}, stored, total_before(stored, None))
    assert rows == {}
    assert total == 13.0


def test_consumption_late_hour_before_oldest_stored_row():
    stored = {hour(11): (2.0, 12.0)}
    api = {hour(10): 1.0, hour(11): 2.0}
    rows, _ = consumption_rows(api, stored, total_before(stored, (2.0, 12.0)))
    assert rows == {hour(10): (1.0, 11.0), hour(11): (2.0, 13.0)}


def test_hour_buckets_are_utc_across_dst():
    # 2026-10-25 02:30 CEST and 02:30 CET are different UTC hours.
    readings = [
        _reading("2026-10-25T02:30:00+02:00", 1.0),
        _reading("2026-10-25T02:30:00+01:00", 2.0),
    ]
    hours = consumption_by_hour(readings, "c1")
    assert hours == {
        datetime(2026, 10, 25, 0, tzinfo=UTC): 1.0,
        datetime(2026, 10, 25, 1, tzinfo=UTC): 2.0,
    }
    assert min(hours) + timedelta(hours=1) == max(hours)
