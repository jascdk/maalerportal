"""One data coordinator per Målerportal installation.

All sensors of an installation share one call to /readings/latest per polling
interval and one cached call to /readings/historical, instead of each sensor
opening its own connection and asking for the same data.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import logging
from typing import Any

import aiohttp

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import API_TIMEOUT_SECONDS, DOMAIN
from .parsing import parse_api_timestamp, parse_number

_LOGGER = logging.getLogger(__name__)

# The API refuses ranges longer than 31 days, so ask for at most 30 whole days.
MAX_HISTORY_DAYS = 30

# How long a historical answer is reused. Sensors that need history ask for it
# at most once an hour, so this lets every sensor of an installation share one
# call while still seeing fresh data on the next round.
HISTORY_CACHE_TTL = timedelta(minutes=50)
FORCED_HISTORY_CACHE_TTL = timedelta(seconds=60)

# Backoff when the gateway rate limits us or no longer knows the installation.
MAX_RATE_LIMIT_INTERVAL = timedelta(hours=4)
MAX_UNAVAILABLE_INTERVAL = timedelta(hours=24)


class MaalerportalCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Fetch the latest readings of one installation for all its sensors."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        installation: dict,
        api_key: str,
        base_url: str,
        polling_interval: timedelta,
    ) -> None:
        """Initialize the coordinator."""
        self.installation = installation
        self.installation_id: str = installation["installationId"]
        self._api_key = api_key
        self._base_url = base_url
        self._session = async_get_clientsession(hass)
        self._polling_interval = polling_interval

        # When we last got an OK answer from the gateway.
        self.last_contact: datetime | None = None

        # Cached /readings/historical answer: (fetched_at, days, readings).
        self._history: tuple[datetime, int, list[dict]] | None = None
        self._history_lock = asyncio.Lock()

        # Newest reading published per counter, as (timestamp, value). A
        # fallback from history that is older than this is not used, so a
        # passing null from /readings/latest never rewinds a sensor.
        self._published: dict[str, tuple[datetime, float]] = {}

        self._inaccessible_logged = False

        super().__init__(
            hass,
            _LOGGER,
            config_entry=entry,
            name=f"{DOMAIN}_{self.installation_id}",
            update_interval=polling_interval,
        )

    @property
    def _headers(self) -> dict[str, str]:
        return {"ApiKey": self._api_key}

    def _back_off(self, maximum: timedelta) -> None:
        """Double the polling interval, up to maximum."""
        current = self.update_interval or self._polling_interval
        self.update_interval = min(current * 2, max(maximum, self._polling_interval))

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch /readings/latest and fill counters the API left empty."""
        url = f"{self._base_url}/installations/{self.installation_id}/readings/latest"
        try:
            async with self._session.get(
                url,
                headers=self._headers,
                timeout=aiohttp.ClientTimeout(total=API_TIMEOUT_SECONDS),
            ) as response:
                if response.status == 401:
                    raise ConfigEntryAuthFailed("API key invalid or expired")
                if response.status == 429:
                    self._back_off(MAX_RATE_LIMIT_INTERVAL)
                    raise UpdateFailed(
                        f"Rate limited by Målerportal, next try in {self.update_interval}"
                    )
                if response.status in (403, 404):
                    self._back_off(MAX_UNAVAILABLE_INTERVAL)
                    if not self._inaccessible_logged:
                        _LOGGER.warning(
                            "Installation %s is not accessible (HTTP %s); retrying with "
                            "growing intervals, next in %s",
                            self.installation_id,
                            response.status,
                            self.update_interval,
                        )
                        self._inaccessible_logged = True
                    raise UpdateFailed(
                        f"Installation {self.installation_id} not accessible (HTTP {response.status})"
                    )
                if response.status >= 400:
                    raise UpdateFailed(f"Målerportal answered HTTP {response.status}")
                data = await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise UpdateFailed(f"Error talking to Målerportal: {err}") from err

        if self._inaccessible_logged:
            _LOGGER.info("Installation %s is accessible again", self.installation_id)
            self._inaccessible_logged = False
        self.update_interval = self._polling_interval
        self.last_contact = datetime.now(timezone.utc)

        if not isinstance(data, dict):
            data = {}
        counters = data.get("meterCounters") or []
        data["meterCounters"] = counters
        await self._fill_missing_latest_values(counters)
        return data

    async def _fill_missing_latest_values(self, counters: list[dict]) -> None:
        """Use the newest historical reading for counters with latestValue null.

        Some installations only deliver data through /readings/historical, so
        /readings/latest answers null and the sensors would stay unknown forever.
        """
        missing: list[dict] = []
        for counter in counters:
            counter_id = counter.get("meterCounterId")
            if not counter_id:
                continue
            if parse_number(counter.get("latestValue")) is None:
                missing.append(counter)
            else:
                self._note_published(counter_id, counter)

        if not missing:
            return

        readings = await self.async_get_history(MAX_HISTORY_DAYS)
        if not readings:
            return

        newest: dict[str, tuple[datetime, Any]] = {}
        for reading in readings:
            counter_id = reading.get("meterCounterId")
            timestamp = parse_api_timestamp(reading.get("timestamp"))
            if counter_id is None or timestamp is None:
                continue
            if parse_number(reading.get("value")) is None:
                continue
            if counter_id not in newest or timestamp > newest[counter_id][0]:
                newest[counter_id] = (timestamp, reading.get("value"))

        for counter in missing:
            counter_id = counter["meterCounterId"]
            if counter_id not in newest:
                continue
            timestamp, value = newest[counter_id]
            published = self._published.get(counter_id)
            if published is not None and timestamp < published[0]:
                # Older than what the sensor already shows: leave it null so the
                # sensor keeps its value instead of going back in time.
                continue
            counter["latestValue"] = value
            counter["latestTimestamp"] = timestamp.isoformat().replace("+00:00", "Z")
            counter["isFallback"] = True
            self._note_published(counter_id, counter)
            _LOGGER.debug(
                "Counter %s had no latest value; using historical reading %s from %s",
                counter_id,
                value,
                timestamp,
            )

    def _note_published(self, counter_id: str, counter: dict) -> None:
        timestamp = parse_api_timestamp(counter.get("latestTimestamp"))
        value = parse_number(counter.get("latestValue"))
        if timestamp is None or value is None:
            return
        previous = self._published.get(counter_id)
        if previous is None or timestamp >= previous[0]:
            self._published[counter_id] = (timestamp, value)

    async def async_get_history(self, days: int, *, force: bool = False) -> list[dict] | None:
        """Return historical readings for the last ``days`` days, cached briefly.

        Returns None if the gateway could not be asked; sensors then try again
        next round.
        """
        days = max(1, min(days, MAX_HISTORY_DAYS))
        async with self._history_lock:
            now = datetime.now(timezone.utc)
            # A forced fetch still shares an answer that is only seconds old, so
            # the refresh service asks once per installation, not once per sensor.
            ttl = FORCED_HISTORY_CACHE_TTL if force else HISTORY_CACHE_TTL
            if (
                self._history is not None
                and self._history[1] >= days
                and now - self._history[0] < ttl
            ):
                return self._history[2]

            start = now - timedelta(days=days)
            body = {
                "from": start.strftime("%Y-%m-%dT00:00:00Z"),
                "to": now.strftime("%Y-%m-%dT23:59:59Z"),
            }
            url = f"{self._base_url}/installations/{self.installation_id}/readings/historical"
            try:
                async with self._session.post(
                    url,
                    json=body,
                    headers=self._headers,
                    timeout=aiohttp.ClientTimeout(total=API_TIMEOUT_SECONDS),
                ) as response:
                    if response.status == 401:
                        if self.config_entry is not None:
                            self.config_entry.async_start_reauth(self.hass)
                        return None
                    if response.status >= 400:
                        _LOGGER.warning(
                            "Historical readings for installation %s failed: HTTP %s",
                            self.installation_id,
                            response.status,
                        )
                        return None
                    data = await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                _LOGGER.warning(
                    "Error fetching historical readings for installation %s: %s",
                    self.installation_id,
                    err,
                )
                return None

            readings = data.get("readings", []) if isinstance(data, dict) else []
            self._history = (now, days, readings)
            self.last_contact = now
            return readings
