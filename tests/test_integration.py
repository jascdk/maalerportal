"""Integration tests: a real Home Assistant and recorder against a mocked API."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from freezegun.api import FrozenDateTimeFactory

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import statistics_during_period
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)
from pytest_homeassistant_custom_component.test_util.aiohttp import AiohttpClientMocker

from custom_components.maalerportal.const import DOMAIN

BASE = "https://api.gateway.meterportal.eu/v1/smarthome"
INST = "11111111-2222-3333-4444-555555555555"
LATEST = f"{BASE}/installations/{INST}/readings/latest"
HISTORY = f"{BASE}/installations/{INST}/readings/historical"
NOW = datetime(2026, 11, 25, 12, 30, tzinfo=timezone.utc)

INSTALLATION = {
    "installationId": INST,
    "installationType": "ColdWater",
    "address": "Testvej 1",
    "meterSerial": "123",
    "utilityName": "Testforsyning",
}


def counter(value: Any, *, reading_type: str = "counter", ts: str | None = "2026-11-25T12:05:00Z") -> dict:
    return {
        "meterCounterId": "c1",
        "counterType": "ColdWater",
        "isPrimary": True,
        "readingType": reading_type,
        "unit": "m3",
        "latestValue": value,
        "latestTimestamp": ts,
    }


def reading(ts: datetime, value: Any) -> dict:
    return {"meterCounterId": "c1", "timestamp": ts.isoformat().replace("+00:00", "Z"), "value": value}


def mock_api(
    aioclient_mock: AiohttpClientMocker,
    counters: list[dict] | None = None,
    history: list[dict] | None = None,
    *,
    latest_status: int = 200,
) -> None:
    aioclient_mock.clear_requests()
    aioclient_mock.get(f"{BASE}/addresses", json=[])
    aioclient_mock.get(LATEST, status=latest_status, json={"meterCounters": counters or []})
    aioclient_mock.post(HISTORY, json={"readings": history or []})


def calls(aioclient_mock: AiohttpClientMocker, url: str) -> int:
    return sum(1 for call in aioclient_mock.mock_calls if str(call[1]) == url)


async def setup_entry(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            "api_key": "key",
            "smarthome_base_url": BASE,
            "installations": [INSTALLATION],
        },
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


def entity_id(hass: HomeAssistant, suffix: str) -> str:
    registry = er.async_get(hass)
    found = registry.async_get_entity_id("sensor", DOMAIN, f"{INST}_{suffix}")
    assert found, suffix
    return found


async def stored_rows(hass: HomeAssistant, statistic_id: str) -> list[tuple[datetime, float, float]]:
    await async_wait_recording_done(hass)
    result = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        NOW - timedelta(days=40),
        None,
        {statistic_id},
        "hour",
        None,
        {"state", "sum"},
    )
    return [
        (dt_util.utc_from_timestamp(row["start"]), row["state"], row["sum"])
        for row in result.get(statistic_id, [])
    ]


async def refresh(hass: HomeAssistant) -> None:
    await hass.services.async_call(DOMAIN, "refresh", {}, blocking=True)
    await hass.async_block_till_done()


@pytest.fixture(autouse=True)
async def setup_recorder_and_integration(recorder_mock, enable_custom_integrations):
    """The recorder must start before hass; then custom integrations are allowed.

    The key check at setup is not what these tests are about.
    """
    with patch("custom_components.maalerportal.validate_api_key", AsyncMock()):
        yield


@pytest.fixture(autouse=True)
def frozen_now(freezer: FrozenDateTimeFactory) -> FrozenDateTimeFactory:
    freezer.move_to(NOW)
    return freezer


async def test_values_at_startup_with_one_call(recorder_mock, hass, aioclient_mock):
    """Sensors have their value right after setup; the meter is asked once."""
    battery = {
        "meterCounterId": "b1",
        "counterType": "BatteryDaysRemaining",
        "latestValue": 400,
        "latestTimestamp": "2026-11-25T12:05:00Z",
    }
    mock_api(aioclient_mock, [counter("123,456"), battery])
    await setup_entry(hass)

    assert hass.states.get(entity_id(hass, "main")).state == "123.456"
    assert hass.states.get(entity_id(hass, "battery_days")).state == "400"
    assert calls(aioclient_mock, LATEST) == 1


async def test_null_latest_value_uses_history(recorder_mock, hass, aioclient_mock):
    history = [reading(NOW - timedelta(hours=3), 99.5), reading(NOW - timedelta(hours=1), 100.25)]
    mock_api(aioclient_mock, [counter(None, ts=None)], history)
    await setup_entry(hass)

    assert hass.states.get(entity_id(hass, "main")).state == "100.25"


async def test_null_fallback_never_rewinds(recorder_mock, hass, aioclient_mock):
    mock_api(aioclient_mock, [counter(101.0)], [reading(NOW - timedelta(hours=5), 99.0)])
    entry = await setup_entry(hass)
    main = entity_id(hass, "main")
    assert hass.states.get(main).state == "101.0"

    mock_api(aioclient_mock, [counter(None, ts=None)], [reading(NOW - timedelta(hours=5), 99.0)])
    await hass.data[DOMAIN][entry.entry_id]["coordinators"][0].async_refresh()
    await hass.async_block_till_done()
    assert hass.states.get(main).state == "101.0"


async def test_negative_reading_is_ignored(recorder_mock, hass, aioclient_mock):
    mock_api(aioclient_mock, [counter(50.0)])
    entry = await setup_entry(hass)
    main = entity_id(hass, "main")

    mock_api(aioclient_mock, [counter(-50.0)])
    await hass.data[DOMAIN][entry.entry_id]["coordinators"][0].async_refresh()
    await hass.async_block_till_done()
    assert hass.states.get(main).state == "50.0"


async def test_rate_limit_backs_off_and_keeps_values(recorder_mock, hass, aioclient_mock):
    mock_api(aioclient_mock, [counter(10.0)])
    entry = await setup_entry(hass)
    coordinator = hass.data[DOMAIN][entry.entry_id]["coordinators"][0]
    normal = coordinator.update_interval

    mock_api(aioclient_mock, [counter(10.0)], latest_status=429)
    await coordinator.async_refresh()
    assert coordinator.update_interval == normal * 2
    await coordinator.async_refresh()
    assert coordinator.update_interval == normal * 4
    # The sensor keeps its value through a rate limit.
    assert hass.states.get(entity_id(hass, "main")).state == "10.0"

    mock_api(aioclient_mock, [counter(11.0)])
    await coordinator.async_refresh()
    await hass.async_block_till_done()
    assert coordinator.update_interval == normal
    assert hass.states.get(entity_id(hass, "main")).state == "11.0"


async def test_counter_statistics_stay_continuous(recorder_mock, hass, aioclient_mock, frozen_now):
    """No step in the sum between the first import and later rounds."""
    start = NOW.replace(minute=5) - timedelta(hours=5)
    history = [reading(start + timedelta(hours=i), 1000.0 + i) for i in range(5)]
    mock_api(aioclient_mock, [counter(1004.0)], history)
    await setup_entry(hass)
    stat = entity_id(hass, "coldwater_statistic_primary")

    rows = await stored_rows(hass, stat)
    assert [r[2] for r in rows] == [0.0, 1.0, 2.0, 3.0, 4.0]

    # An hour later the next reading arrives, and a missing hour is delivered late.
    frozen_now.tick(timedelta(hours=1))
    history = [r for r in history if not r["timestamp"].startswith(f"{(start + timedelta(hours=2)):%Y-%m-%dT%H}")]
    history += [reading(start + timedelta(hours=5), 1005.5)]
    mock_api(aioclient_mock, [counter(1005.5)], history)
    await refresh(hass)

    rows = await stored_rows(hass, stat)
    sums = [r[2] for r in rows]
    assert sums == [0.0, 1.0, 2.0, 3.0, 4.0, 5.5]
    assert all(r[2] == pytest.approx(r[1] - 1000.0) for r in rows)

    # After a restart the series still continues.
    frozen_now.tick(timedelta(hours=1))
    history += [reading(start + timedelta(hours=6), 1006.0)]
    mock_api(aioclient_mock, [counter(1006.0)], history)
    entry = hass.config_entries.async_entries(DOMAIN)[0]
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    rows = await stored_rows(hass, stat)
    assert [r[2] for r in rows][-2:] == [5.5, 6.0]


async def test_consumption_statistics_take_late_hours(recorder_mock, hass, aioclient_mock, frozen_now):
    start = NOW.replace(minute=0) - timedelta(hours=6)
    history = [reading(start + timedelta(hours=i), 1.0) for i in (0, 1, 3)]
    mock_api(aioclient_mock, [counter(1.0, reading_type="consumption")], history)
    await setup_entry(hass)
    stat = entity_id(hass, "coldwater_statistic_primary")
    virtual = entity_id(hass, "coldwater_consumption_primary")

    rows = await stored_rows(hass, stat)
    assert [r[2] for r in rows] == [1.0, 2.0, 3.0]
    assert hass.states.get(virtual).state == "3.0"

    # Hour 2 is delivered late, hour 4 on time.
    frozen_now.tick(timedelta(hours=1))
    history += [reading(start + timedelta(hours=2), 0.5), reading(start + timedelta(hours=4), 1.0)]
    mock_api(aioclient_mock, [counter(1.0, reading_type="consumption")], history)
    await refresh(hass)

    rows = await stored_rows(hass, stat)
    assert [(r[1], r[2]) for r in rows] == [(1.0, 1.0), (1.0, 2.0), (0.5, 2.5), (1.0, 3.5), (1.0, 4.5)]
    assert hass.states.get(virtual).state == "4.5"


async def test_virtual_meter_survives_restart(recorder_mock, hass, aioclient_mock, frozen_now):
    start = NOW.replace(minute=0) - timedelta(hours=3)
    history = [reading(start + timedelta(hours=i), 2.0) for i in range(3)]
    mock_api(aioclient_mock, [counter(2.0, reading_type="consumption")], history)
    entry = await setup_entry(hass)
    virtual = entity_id(hass, "coldwater_consumption_primary")
    assert hass.states.get(virtual).state == "6.0"

    frozen_now.tick(timedelta(hours=1))
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    # Same readings again: nothing is counted twice.
    assert hass.states.get(virtual).state == "6.0"


async def test_basic_sensor_until_meter_answers(recorder_mock, hass, aioclient_mock):
    """A failed first fetch gives the basic sensor; the real ones follow by themselves."""
    mock_api(aioclient_mock, latest_status=500)
    entry = await setup_entry(hass)
    assert hass.states.get(entity_id(hass, "basic")) is not None

    mock_api(aioclient_mock, [counter(42.0)])
    await hass.data[DOMAIN][entry.entry_id]["coordinators"][0].async_refresh()
    await hass.async_block_till_done()
    assert hass.states.get(entity_id(hass, "main")).state == "42.0"


async def test_rejected_key_starts_reauth(recorder_mock, hass, aioclient_mock):
    mock_api(aioclient_mock, [counter(1.0)])
    entry = await setup_entry(hass)

    mock_api(aioclient_mock, [counter(1.0)], latest_status=401)
    await hass.data[DOMAIN][entry.entry_id]["coordinators"][0].async_refresh()
    await hass.async_block_till_done()
    flows = hass.config_entries.flow.async_progress()
    assert any(flow["context"].get("source") == "reauth" for flow in flows)
