"""Build hourly long-term statistics rows from Målerportal readings.

Pure functions without Home Assistant imports, so they can be unit tested.

Rows are ``{start: (state, sum)}`` with ``start`` the aware UTC start of the
hour. The Energy Dashboard only looks at the difference in ``sum`` between
hours, so what matters is that ``sum`` never steps between two neighbouring
rows unless there really was consumption.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta

from .parsing import parse_api_timestamp, parse_number

Row = tuple[float, float]  # (state, sum)

# Stored and new values closer than this are treated as equal.
_EPSILON = 1e-6

# Sums are rounded to this many decimals, so float noise never shows as usage.
_DECIMALS = 6


def _hour_floor(moment: datetime) -> datetime:
    return moment.replace(minute=0, second=0, microsecond=0)


def counter_readings_by_hour(readings: Iterable[dict], counter_id: str) -> dict[datetime, float]:
    """Meter readings of one counter, one per hour.

    A reading is the meter state at the time it was taken, i.e. at the end of
    the hour before, so a reading at 00:05 is stored in the 23:00 hour. If an
    hour has several readings, the newest wins.
    """
    newest: dict[datetime, tuple[datetime, float]] = {}
    for reading in readings:
        if reading.get("meterCounterId") != counter_id:
            continue
        timestamp = parse_api_timestamp(reading.get("timestamp"))
        value = parse_number(reading.get("value"))
        if timestamp is None or value is None:
            continue
        start = _hour_floor(timestamp) - timedelta(hours=1)
        if start not in newest or timestamp >= newest[start][0]:
            newest[start] = (timestamp, value)
    return {start: value for start, (_, value) in newest.items()}


def consumption_by_hour(readings: Iterable[dict], counter_id: str) -> dict[datetime, float]:
    """Consumption of one counter summed per hour.

    Consumption readings are stamped with the start of their period, so they
    are stored in the hour they fall in. Sub-hourly readings are added up.
    Negative values are not consumption and count as zero.
    """
    hours: dict[datetime, float] = {}
    for reading in readings:
        if reading.get("meterCounterId") != counter_id:
            continue
        timestamp = parse_api_timestamp(reading.get("timestamp"))
        value = parse_number(reading.get("value"))
        if timestamp is None or value is None:
            continue
        start = _hour_floor(timestamp)
        hours[start] = hours.get(start, 0.0) + max(value, 0.0)
    return hours


def counter_offset(
    last_stored: Row | None, readings_by_hour: dict[datetime, float]
) -> float:
    """The constant ``sum - state`` for a counter meter.

    With stored statistics the offset of the newest stored row is kept, so new
    rows continue it without a step. On a first import the oldest reading is
    the baseline, so the dashboard starts from zero instead of counting the
    whole meter reading as consumption in the first hour.
    """
    if last_stored is not None:
        state, total = last_stored
        if state is not None and total is not None:
            return total - state
        return 0.0
    if not readings_by_hour:
        return 0.0
    return -readings_by_hour[min(readings_by_hour)]


def counter_rows(
    readings_by_hour: dict[datetime, float],
    offset: float,
    stored: dict[datetime, Row],
) -> dict[datetime, Row]:
    """Rows to import for a counter meter: new hours and changed hours.

    Every row is ``sum = state + offset``, so an hour the utility delivers late
    fits between its neighbours without a step.
    """
    rows: dict[datetime, Row] = {}
    for start, value in readings_by_hour.items():
        row = (value, round(value + offset, _DECIMALS))
        if not _same_row(stored.get(start), row):
            rows[start] = row
    return rows


def consumption_rows(
    api_hours: dict[datetime, float],
    stored: dict[datetime, Row],
    total_before_stored: float,
) -> tuple[dict[datetime, Row], float]:
    """Rows to import for a consumption meter, and the new running total.

    ``stored`` holds the rows already in the database for the window the API
    answered for. ``total_before_stored`` is the running total just before the
    window: the sum of the newest row older than the window, or 0.

    The window is rebuilt hour by hour from the stored rows with the API's
    values on top, and the running total is recomputed. An hour the utility
    delivers late, or corrects, therefore raises the total of every hour after
    it instead of being dropped.
    """
    hours = dict((start, max(row[0] or 0.0, 0.0)) for start, row in stored.items())
    hours.update(api_hours)

    rows: dict[datetime, Row] = {}
    total = total_before_stored
    for start in sorted(hours):
        consumption = hours[start]
        total += consumption
        row = (consumption, round(total, _DECIMALS))
        if not _same_row(stored.get(start), row):
            rows[start] = row
    return rows, round(total, _DECIMALS)


def total_before(stored: dict[datetime, Row], last_overall: Row | None) -> float:
    """Running total just before the oldest stored row of a consumption meter.

    The oldest stored row in the window carries it as ``sum - state``. With no
    stored rows in the window, every stored row is older, so the newest row
    overall gives it.
    """
    if stored:
        state, total = stored[min(stored)]
        return (total or 0.0) - max(state or 0.0, 0.0)
    if last_overall is not None and last_overall[1] is not None:
        return last_overall[1]
    return 0.0


def _same_row(stored: Row | None, new: Row) -> bool:
    if stored is None or stored[0] is None or stored[1] is None:
        return False
    return abs(stored[0] - new[0]) < _EPSILON and abs(stored[1] - new[1]) < _EPSILON
