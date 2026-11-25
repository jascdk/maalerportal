"""Platform for Målerportal sensor integration."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import Any, Optional

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_import_statistics,
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    EntityCategory,
    UnitOfEnergy,
    UnitOfPower,
    UnitOfTemperature,
    UnitOfTime,
    UnitOfVolume,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import ExtraStoredData, RestoreEntity
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from .const import (
    DEFAULT_POLLING_INTERVAL,
    DOMAIN,
    STALE_ENTITIES_STORAGE_VERSION,
    consumer_app_url,
    stale_entities_storage_key,
)
from .coordinator import MAX_HISTORY_DAYS, MaalerportalCoordinator
from .parsing import parse_api_timestamp, parse_number
from .statistics import (
    consumption_by_hour,
    consumption_rows,
    counter_offset,
    counter_readings_by_hour,
    counter_rows,
    total_before,
)
from .units import LITERS_PER_HOUR, resolve_unit, statistics_unit_class

EVENT_METER_UPDATED = f"{DOMAIN}_meter_updated"

_LOGGER = logging.getLogger(__name__)

# History (statistics and virtual meters) arrives per hour, so it is never
# fetched more often than this.
MIN_HISTORY_INTERVAL = timedelta(hours=1)

# Window re-read on every history round once statistics exist. Hours the
# utility delivers late inside this window are still imported.
RECENT_HISTORY_DAYS = 7


def get_polling_interval(config: ConfigEntry) -> timedelta:
    """Get polling interval from config options."""
    interval = config.options.get("polling_interval", DEFAULT_POLLING_INTERVAL)
    return timedelta(minutes=interval)


async def async_setup_entry(
    hass: HomeAssistant, config: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up the sensor platform."""

    config_data = hass.data[DOMAIN][config.entry_id]
    installations: list[dict] = config_data["installations"]
    api_key: str = config_data["api_key"]
    smarthome_base_url: str = config_data["smarthome_base_url"]
    polling_interval = get_polling_interval(config)

    sensors: list[SensorEntity] = []
    coordinators: list[MaalerportalCoordinator] = []
    # Installations whose counter list we actually got; only those are safe to prune.
    fetched_installation_ids: set[str] = set()

    for installation in installations:
        coordinator = MaalerportalCoordinator(
            hass, config, installation, api_key, smarthome_base_url, polling_interval
        )
        coordinators.append(coordinator)
        # One fetch per installation decides which sensors to create, and the
        # same answer gives them their values right away.
        await coordinator.async_refresh()

        if coordinator.last_update_success and coordinator.data is not None:
            meter_counters = coordinator.data.get("meterCounters", [])
            sensors.extend(create_sensors_from_counters(coordinator, meter_counters))
            # Only a non-empty counter list proves the API knows this
            # installation. Pruning must never act on an empty answer.
            if meter_counters:
                fetched_installation_ids.add(coordinator.installation_id)
        else:
            _LOGGER.warning(
                "Could not fetch meter data for installation %s, creating basic sensor",
                coordinator.installation_id,
            )
            sensors.append(MaalerportalBasicSensor(coordinator))
            _reload_when_counters_arrive(hass, config, coordinator)

    config_data["coordinators"] = coordinators
    await _async_remove_stale_entities(hass, config, sensors, fetched_installation_ids)
    async_add_entities(sensors)


def _reload_when_counters_arrive(
    hass: HomeAssistant, config: ConfigEntry, coordinator: MaalerportalCoordinator
) -> None:
    """Reload the entry once a fallback installation answers, to get its real sensors."""

    reloading = False

    @callback
    def _check() -> None:
        nonlocal reloading
        if reloading or not (coordinator.data and coordinator.data.get("meterCounters")):
            return
        reloading = True
        _LOGGER.info(
            "Meter data for installation %s is available again; reloading to create its sensors",
            coordinator.installation_id,
        )
        hass.config_entries.async_schedule_reload(config.entry_id)

    # Removed when the entry unloads, which the reload does.
    config.async_on_unload(coordinator.async_add_listener(_check))


async def _async_remove_stale_entities(
    hass: HomeAssistant,
    config: ConfigEntry,
    sensors: list[SensorEntity],
    fetched_installation_ids: set[str],
) -> None:
    """Remove registry entries for sensors Målerportal has stopped providing.

    The server decides which counters exist; the entities follow. Two guards
    keep a hiccup from deleting history: only installations whose counter list
    was fetched are considered, and an entity is removed only when it is
    unbacked at two consecutive setups. The first miss is remembered in
    storage; if the counter is back next time, the entity is forgiven.
    This is what retires a counter type the server stops sending, and what
    replaces a "basic" fallback sensor once the real sensors are back.
    """
    registry = er.async_get(hass)
    current_unique_ids = {sensor.unique_id for sensor in sensors}
    store: Store[list[str]] = Store(hass, STALE_ENTITIES_STORAGE_VERSION, stale_entities_storage_key(config))
    previously_missing = set(await store.async_load() or [])

    still_missing: set[str] = set()
    for entry in er.async_entries_for_config_entry(registry, config.entry_id):
        unique_id = str(entry.unique_id)
        # Installation ids are UUIDs, so the first underscore ends the id.
        installation_id = unique_id.split("_", 1)[0]
        if installation_id not in fetched_installation_ids or unique_id in current_unique_ids:
            continue
        if unique_id in previously_missing:
            _LOGGER.info("Removing entity %s, no longer provided by Målerportal", entry.entity_id)
            registry.async_remove(entry.entity_id)
        else:
            _LOGGER.info(
                "Entity %s is not provided by Målerportal right now; it will be removed "
                "if it is still missing at the next setup",
                entry.entity_id,
            )
            still_missing.add(unique_id)

    if still_missing != previously_missing:
        await store.async_save(sorted(still_missing))


def create_sensors_from_counters(
    coordinator: MaalerportalCoordinator, meter_counters: list[dict]
) -> list[SensorEntity]:
    """Create appropriate sensors based on available meter counters."""
    sensors: list[SensorEntity] = []

    # Consumable counter types:
    # - For readingType="counter": meter reading sensor + statistics sensor
    # - For readingType="consumption": Statistics sensor (Energy Dashboard) + Consumption sensor
    consumable_counter_types = [
        "ColdWater", "HotWater", "ElectricityFromGrid", "ElectricityToGrid", "Heat"
    ]

    # Heat meter counter types
    supply_temp_types = ["SupplyTemp", "FlowTemp", "T1", "SupplyTemperature"]
    return_temp_types = ["ReturnTemp", "T2", "ReturnTemperature"]
    temp_diff_types = ["TempDiff", "DeltaT", "TemperatureDifference"]
    heat_power_types = ["Power", "Effect", "HeatPower"]
    heat_volume_types = ["Volume", "V1", "HeatVolume"]

    # Find primary counter for main sensor
    primary_counter = None
    for counter in meter_counters:
        if counter.get("isPrimary", False):
            primary_counter = counter
            break

    if not primary_counter and meter_counters:
        primary_counter = meter_counters[0]

    # Create sensors for primary counter based on reading type
    if primary_counter:
        reading_type = primary_counter.get("readingType", "counter").lower()
        counter_type = primary_counter.get("counterType", "")

        if reading_type == "consumption":
            # Statistics sensor for the Energy Dashboard + virtual meter reading
            if counter_type in consumable_counter_types:
                sensors.append(MaalerportalStatisticSensor(coordinator, primary_counter))
                sensors.append(MaalerportalConsumptionSensor(coordinator, primary_counter))
        else:
            # Meter reading sensor + statistics sensor with history
            sensors.append(MaalerportalMainSensor(coordinator, primary_counter))
            if counter_type in consumable_counter_types:
                sensors.append(MaalerportalStatisticSensor(coordinator, primary_counter))

        # Add price sensor for primary counter if price is available
        if primary_counter.get("pricePerUnit") is not None:
            sensors.append(MaalerportalPriceSensor(coordinator, primary_counter))

    # Create additional sensors for other counter types
    for counter in meter_counters:
        counter_type = counter.get("counterType", "")
        is_primary = counter.get("isPrimary", False)

        if counter_type == "BatteryDaysRemaining":
            sensors.append(MaalerportalBatterySensor(coordinator, counter))
        elif counter_type in ["DailyMaxAmbientTemp", "DailyMinAmbientTemp"]:
            sensors.append(MaalerportalTemperatureSensor(coordinator, counter))
        elif counter_type in ["DailyMaxWaterTemp", "DailyMinWaterTemp"]:
            sensors.append(MaalerportalWaterTemperatureSensor(coordinator, counter))
        elif counter_type in ["DailyMaxFlow1", "DailyMinFlow1"]:
            sensors.append(MaalerportalFlowSensor(coordinator, counter))
        # Heat meter specific sensors
        elif counter_type in supply_temp_types:
            sensors.append(MaalerportalSupplyTempSensor(coordinator, counter))
        elif counter_type in return_temp_types:
            sensors.append(MaalerportalReturnTempSensor(coordinator, counter))
        elif counter_type in temp_diff_types:
            sensors.append(MaalerportalTempDiffSensor(coordinator, counter))
        elif counter_type in heat_power_types:
            sensors.append(MaalerportalHeatPowerSensor(coordinator, counter))
        elif counter_type in heat_volume_types:
            sensors.append(MaalerportalHeatVolumeSensor(coordinator, counter))
        elif not is_primary and counter_type in consumable_counter_types:
            # Secondary meters - sensors depend on reading type
            reading_type = counter.get("readingType", "counter").lower()
            if reading_type == "consumption":
                sensors.append(MaalerportalStatisticSensor(coordinator, counter))
                sensors.append(MaalerportalConsumptionSensor(coordinator, counter))
            else:
                sensors.append(MaalerportalSecondarySensor(coordinator, counter))
                sensors.append(MaalerportalStatisticSensor(coordinator, counter))

    # Diagnostics: when we last reached Målerportal, and when the meter last reported.
    sensors.append(MaalerportalLastApiContactSensor(coordinator, primary_counter))
    sensors.append(MaalerportalLastMeterReadingSensor(coordinator, primary_counter))

    return sensors


class MaalerportalBaseSensor(CoordinatorEntity[MaalerportalCoordinator], SensorEntity):
    """Base class for all Målerportal sensors."""

    _attr_has_entity_name = True

    # Translation map for installation/meter types
    METER_TYPE_TRANSLATIONS = {
        "ColdWater": "Koldt vand",
        "HotWater": "Varmt vand",
        "Electricity": "El",
        "Heat": "Varme",
        "Gas": "Gas",
    }

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict | None = None) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator)
        installation = coordinator.installation
        self._installation = installation
        self._installation_id = installation["installationId"]
        self._installation_type = installation["installationType"]
        self._counter = counter
        self._last_reading_timestamp: Optional[str] = None

        # Create base device name
        self._base_device_name = f"{installation['address']} - {installation['meterSerial']}"
        if installation.get("nickname"):
            self._base_device_name += f" ({installation['nickname']})"

    @property
    def installation_id(self) -> str:
        """The Målerportal installation this sensor belongs to."""
        return self._installation_id

    def _get_translated_meter_type(self) -> str:
        """Get translated meter type for device model."""
        return self.METER_TYPE_TRANSLATIONS.get(self._installation_type, self._installation_type)

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information."""
        return DeviceInfo(
            identifiers={(DOMAIN, self._installation_id)},
            name=self._base_device_name,
            manufacturer=self._installation.get("utilityName", "Unknown"),
            model=self._get_translated_meter_type(),
            serial_number=self._installation.get("meterSerial"),
            configuration_url=consumer_app_url(self._installation_type),
        )

    @property
    def available(self) -> bool:
        """Available once data has arrived.

        A failed poll keeps the last value instead of making the sensor
        unavailable: meters report rarely, and a short outage at the gateway
        should not punch holes in the history.
        """
        return self.coordinator.data is not None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        attrs: dict[str, Any] = {
            "installation_id": self._installation_id,
        }
        if self.coordinator.last_contact:
            attrs["last_contact"] = self.coordinator.last_contact.isoformat()
        if self._last_reading_timestamp:
            attrs["last_reading_timestamp"] = self._last_reading_timestamp
        if self._counter:
            attrs["counter_type"] = self._counter.get("counterType", "")
            attrs["meter_counter_id"] = self._counter.get("meterCounterId", "")
        return attrs

    async def async_added_to_hass(self) -> None:
        """Take the values the coordinator already has, so the sensor starts with them."""
        await super().async_added_to_hass()
        if self.coordinator.data:
            self._update_from_counters(self.coordinator.data.get("meterCounters", []))

    @callback
    def _handle_coordinator_update(self) -> None:
        """Update from the coordinator's latest data."""
        if self.coordinator.data:
            self._update_from_counters(self.coordinator.data.get("meterCounters", []))
        self.async_write_ha_state()

    def _update_from_counters(self, meter_counters: list[dict]) -> None:
        """Update sensor state from meter counter data - implemented by subclasses."""

    async def async_force_refresh(self) -> None:
        """Fetch what only this sensor fetches, ignoring intervals (refresh service)."""

    def _own_counter(self, meter_counters: list[dict]) -> dict | None:
        """This sensor's counter in a list of counters."""
        if not self._counter:
            return None
        counter_id = self._counter.get("meterCounterId")
        for counter in meter_counters:
            if counter.get("meterCounterId") == counter_id:
                return counter
        return None

    def _parse_counter_value(self, counter: dict) -> Optional[float]:
        """Parse a counter's latest value; remember its timestamp."""
        latest_value = counter.get("latestValue")
        if latest_value is None:
            return None
        value = parse_number(latest_value)
        if value is None:
            _LOGGER.warning(
                "Could not read value '%s' of counter %s on installation %s",
                latest_value,
                counter.get("meterCounterId"),
                self._installation_id,
            )
            return None
        if counter.get("latestTimestamp"):
            self._last_reading_timestamp = counter.get("latestTimestamp")
        return value

    def _parse_meter_reading(self, counter: dict) -> Optional[float]:
        """Parse a cumulative meter reading, refusing negative values.

        A meter reading cannot be negative. Turning it positive would show a
        made-up reading, so the sensor keeps its last value instead.
        """
        value = self._parse_counter_value(counter)
        if value is not None and value < 0:
            _LOGGER.warning(
                "Ignoring negative meter reading %s for counter %s on installation %s",
                value,
                counter.get("meterCounterId"),
                self._installation_id,
            )
            return None
        return value

    def _fire_meter_updated(self, old_value: Any, value: float, counter: dict) -> None:
        if self.hass and (old_value is None or old_value != value):
            self.hass.bus.async_fire(EVENT_METER_UPDATED, {
                "installation_id": self._installation_id,
                "meter_value": value,
                "unit": counter.get("unit", ""),
                "counter_type": counter.get("counterType", ""),
                "timestamp": counter.get("latestTimestamp"),
            })
            _LOGGER.debug("Fired %s event for installation %s", EVENT_METER_UPDATED, self._installation_id)

    def _apply_counter_unit(
        self, default_unit: str | None, device_class: SensorDeviceClass | None
    ) -> None:
        """Set unit and device class from the counter's reported unit (see units.py).

        The API unit wins when Home Assistant can handle it for the device class;
        default_unit is only used when the counter reports no unit.
        """
        self._attr_native_unit_of_measurement, self._attr_device_class = resolve_unit(
            self._counter, default_unit, device_class, installation_id=self._installation_id
        )


class MaalerportalMainSensor(MaalerportalBaseSensor):
    """Main sensor for primary meter counter."""

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        """Initialize the main sensor."""
        super().__init__(coordinator, counter)

        self._attr_unique_id = f"{self._installation_id}_main"

        # Translation key and the unit/device class expected for the counter type.
        # The unit the API reports wins when Home Assistant can handle it; the
        # expected unit is only the fallback for counters that report none.
        counter_type = counter.get("counterType", "").lower()
        if counter_type in ["coldwater", "hotwater"]:
            self._attr_translation_key = "meter_reading_water"
            default_unit, device_class = UnitOfVolume.CUBIC_METERS, SensorDeviceClass.WATER
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        elif counter_type in ["electricityfromgrid", "electricitytogrid"]:
            self._attr_translation_key = "meter_reading_electricity"
            default_unit, device_class = UnitOfEnergy.KILO_WATT_HOUR, SensorDeviceClass.ENERGY
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        elif counter_type == "heat":
            self._attr_translation_key = "meter_reading_heat"
            default_unit, device_class = UnitOfEnergy.KILO_WATT_HOUR, SensorDeviceClass.ENERGY
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        else:
            self._attr_translation_key = "meter_reading"
            default_unit, device_class = None, None
            self._attr_state_class = SensorStateClass.MEASUREMENT
        self._apply_counter_unit(default_unit, device_class)

    def _update_from_counters(self, meter_counters: list[dict]) -> None:
        """Update main sensor from primary counter."""
        counter = self._own_counter(meter_counters)
        if counter is None:
            return
        value = self._parse_meter_reading(counter)
        if value is None:
            return
        old_value = self._attr_native_value
        self._attr_native_value = value
        _LOGGER.debug("Updated main sensor value: %s %s", value, counter.get("unit", ""))
        self._fire_meter_updated(old_value, value, counter)


class MaalerportalBasicSensor(MaalerportalBaseSensor):
    """Basic fallback sensor when meter data is not available."""

    def __init__(self, coordinator: MaalerportalCoordinator) -> None:
        """Initialize the basic sensor."""
        super().__init__(coordinator)

        self._attr_unique_id = f"{self._installation_id}_basic"

        # Set default attributes based on installation type
        installation_type = self._installation_type.lower()
        if installation_type in ["coldwater", "hotwater"]:
            self._attr_translation_key = "meter_reading_water"
            self._attr_device_class = SensorDeviceClass.WATER
            self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        elif installation_type == "electricity":
            self._attr_translation_key = "meter_reading_electricity"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        elif installation_type == "heat":
            self._attr_translation_key = "meter_reading_heat"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        else:
            self._attr_translation_key = "meter_reading"
            self._attr_device_class = None
            self._attr_native_unit_of_measurement = None
            self._attr_state_class = SensorStateClass.MEASUREMENT

    def _update_from_counters(self, meter_counters: list[dict]) -> None:
        """Update basic sensor from first available counter."""
        if not meter_counters:
            return
        primary_counter = next(
            (counter for counter in meter_counters if counter.get("isPrimary", False)),
            meter_counters[0],
        )
        value = self._parse_meter_reading(primary_counter)
        if value is not None:
            self._attr_native_value = value
            _LOGGER.debug("Updated basic sensor value: %s %s", value, primary_counter.get("unit", ""))


# Specialized sensor classes for different counter types

class MaalerportalLastApiContactSensor(MaalerportalBaseSensor):
    """When the integration last got a successful answer from Målerportal."""

    _attr_device_class = SensorDeviceClass.TIMESTAMP
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:cloud-check-outline"

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict | None) -> None:
        """Initialize the last API contact sensor."""
        super().__init__(coordinator, counter)
        self._attr_translation_key = "last_api_contact"
        self._attr_unique_id = f"{self._installation_id}_last_api_contact"

    @property
    def native_value(self) -> Optional[datetime]:
        """Return the time of the last successful API response."""
        return self.coordinator.last_contact


class MaalerportalLastMeterReadingSensor(MaalerportalBaseSensor):
    """When the meter itself last reported a value, across all its counters."""

    _attr_device_class = SensorDeviceClass.TIMESTAMP
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:counter"

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict | None) -> None:
        """Initialize the last meter reading sensor."""
        super().__init__(coordinator, counter)
        self._attr_translation_key = "last_meter_reading"
        self._attr_unique_id = f"{self._installation_id}_last_meter_reading"

    def _update_from_counters(self, meter_counters: list[dict]) -> None:
        """Take the newest reading timestamp reported by any counter on the meter."""
        timestamps = [
            parsed
            for counter in meter_counters
            if (parsed := parse_api_timestamp(counter.get("latestTimestamp"))) is not None
        ]
        if timestamps:
            self._attr_native_value = max(timestamps)
            _LOGGER.debug("Updated last meter reading: %s", self._attr_native_value)


class _RoundedCounterSensor(MaalerportalBaseSensor):
    """A sensor showing its counter's latest value, rounded."""

    _round_to: int | None = None

    def _update_from_counters(self, meter_counters: list[dict]) -> None:
        counter = self._own_counter(meter_counters)
        if counter is None:
            return
        value = self._parse_counter_value(counter)
        if value is None:
            return
        self._attr_native_value = round(value, self._round_to) if self._round_to is not None else value
        _LOGGER.debug(
            "Updated %s: %s %s", self.unique_id, value, self._attr_native_unit_of_measurement or ""
        )


class MaalerportalBatterySensor(MaalerportalBaseSensor):
    """Battery days remaining sensor."""

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        """Initialize the battery sensor."""
        super().__init__(coordinator, counter)

        self._attr_translation_key = "battery_days"
        self._attr_unique_id = f"{self._installation_id}_battery_days"
        self._attr_device_class = SensorDeviceClass.DURATION
        self._attr_native_unit_of_measurement = UnitOfTime.DAYS
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:battery"

    def _update_from_counters(self, meter_counters: list[dict]) -> None:
        """Update battery sensor."""
        for counter in meter_counters:
            if counter.get("counterType") == "BatteryDaysRemaining":
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = int(value)
                    _LOGGER.debug("Updated battery days: %s", value)
                break


class MaalerportalTemperatureSensor(_RoundedCounterSensor):
    """Ambient temperature sensor."""

    _round_to = 1

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        """Initialize the temperature sensor."""
        super().__init__(coordinator, counter)

        counter_type = counter.get("counterType", "")
        if "Max" in counter_type:
            self._attr_translation_key = "max_ambient_temperature"
            self._attr_unique_id = f"{self._installation_id}_temp_ambient_max"
        else:
            self._attr_translation_key = "min_ambient_temperature"
            self._attr_unique_id = f"{self._installation_id}_temp_ambient_min"

        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT


class MaalerportalWaterTemperatureSensor(_RoundedCounterSensor):
    """Water temperature sensor."""

    _round_to = 1

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        """Initialize the water temperature sensor."""
        super().__init__(coordinator, counter)

        counter_type = counter.get("counterType", "")
        if "Max" in counter_type:
            self._attr_translation_key = "max_water_temperature"
            self._attr_unique_id = f"{self._installation_id}_temp_water_max"
        else:
            self._attr_translation_key = "min_water_temperature"
            self._attr_unique_id = f"{self._installation_id}_temp_water_min"

        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT


class MaalerportalFlowSensor(_RoundedCounterSensor):
    """Flow sensor."""

    _round_to = 3

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        """Initialize the flow sensor."""
        super().__init__(coordinator, counter)

        counter_type = counter.get("counterType", "")
        if "Max" in counter_type:
            self._attr_translation_key = "max_flow"
            self._attr_unique_id = f"{self._installation_id}_flow_max"
        else:
            self._attr_translation_key = "min_flow"
            self._attr_unique_id = f"{self._installation_id}_flow_min"

        # Flow is L/h unless the utility reports otherwise. With the volume-flow-rate
        # device class Home Assistant converts between m³/h, L/min etc. itself and
        # lets the user pick a display unit on the entity.
        self._apply_counter_unit(LITERS_PER_HOUR, SensorDeviceClass.VOLUME_FLOW_RATE)
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:water-pump"


class MaalerportalSecondarySensor(MaalerportalBaseSensor):
    """Secondary meter sensor."""

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        """Initialize the secondary sensor."""
        super().__init__(coordinator, counter)

        counter_type = counter.get("counterType", "")

        # Translation key and the unit/device class expected for the counter type.
        # The unit the API reports wins when Home Assistant can handle it.
        if counter_type == "ColdWater":
            self._attr_translation_key = "cold_water"
            default_unit, device_class = UnitOfVolume.CUBIC_METERS, SensorDeviceClass.WATER
        elif counter_type == "HotWater":
            self._attr_translation_key = "hot_water"
            default_unit, device_class = UnitOfVolume.CUBIC_METERS, SensorDeviceClass.WATER
        elif counter_type == "ElectricityFromGrid":
            self._attr_translation_key = "electricity_import"
            default_unit, device_class = UnitOfEnergy.KILO_WATT_HOUR, SensorDeviceClass.ENERGY
        elif counter_type == "ElectricityToGrid":
            self._attr_translation_key = "electricity_export"
            default_unit, device_class = UnitOfEnergy.KILO_WATT_HOUR, SensorDeviceClass.ENERGY
        elif counter_type == "Heat":
            self._attr_translation_key = "heat"
            default_unit, device_class = UnitOfEnergy.KILO_WATT_HOUR, SensorDeviceClass.ENERGY
        else:
            self._attr_translation_key = "meter_reading"
            default_unit, device_class = None, None
        self._apply_counter_unit(default_unit, device_class)

        self._attr_unique_id = f"{self._installation_id}_{counter_type.lower()}_secondary"
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING

    def _update_from_counters(self, meter_counters: list[dict]) -> None:
        """Update secondary sensor."""
        counter = self._own_counter(meter_counters)
        if counter is None:
            return
        value = self._parse_meter_reading(counter)
        if value is None:
            return
        old_value = self._attr_native_value
        self._attr_native_value = value
        _LOGGER.debug(
            "Updated secondary sensor %s: %s %s",
            counter.get("counterType"), value, counter.get("unit", ""),
        )
        self._fire_meter_updated(old_value, value, counter)


class MaalerportalPriceSensor(MaalerportalBaseSensor):
    """Price per unit sensor for primary counters."""

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        """Initialize the price sensor."""
        super().__init__(coordinator, counter)

        unit = counter.get("unit", "unit")
        self._attr_translation_key = "price_per_unit"
        self._attr_unique_id = f"{self._installation_id}_price_per_unit"
        self._attr_native_unit_of_measurement = f"kr/{unit}"
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:currency-usd"

    def _update_from_counters(self, meter_counters: list[dict]) -> None:
        """Update price sensor."""
        counter = self._own_counter(meter_counters)
        if counter is None or not counter.get("isPrimary", False):
            return
        price_per_unit = parse_number(counter.get("pricePerUnit"))
        if price_per_unit is not None:
            # Convert from øre to kroner (divide by 100)
            price_in_kroner = price_per_unit / 100
            self._attr_native_value = round(price_in_kroner, 4)
            _LOGGER.debug("Updated price per unit: %s kr/%s", price_in_kroner, counter.get("unit", ""))


# Heat meter specific sensors

class MaalerportalSupplyTempSensor(_RoundedCounterSensor):
    """Supply/flow temperature sensor for heat meters."""

    _round_to = 1

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        """Initialize the supply temperature sensor."""
        super().__init__(coordinator, counter)

        self._attr_translation_key = "supply_temperature"
        self._attr_unique_id = f"{self._installation_id}_temp_supply"
        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:thermometer-chevron-up"


class MaalerportalReturnTempSensor(_RoundedCounterSensor):
    """Return temperature sensor for heat meters."""

    _round_to = 1

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        """Initialize the return temperature sensor."""
        super().__init__(coordinator, counter)

        self._attr_translation_key = "return_temperature"
        self._attr_unique_id = f"{self._installation_id}_temp_return"
        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:thermometer-chevron-down"


class MaalerportalTempDiffSensor(_RoundedCounterSensor):
    """Temperature difference sensor for heat meters."""

    _round_to = 1

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        """Initialize the temperature difference sensor."""
        super().__init__(coordinator, counter)

        self._attr_translation_key = "temperature_difference"
        self._attr_unique_id = f"{self._installation_id}_temp_diff"
        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:thermometer-lines"


class MaalerportalHeatPowerSensor(_RoundedCounterSensor):
    """Heat power/effect sensor for heat meters."""

    _round_to = 2

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        """Initialize the heat power sensor."""
        super().__init__(coordinator, counter)

        self._attr_translation_key = "heat_power"
        self._attr_unique_id = f"{self._installation_id}_heat_power"
        self._apply_counter_unit(UnitOfPower.KILO_WATT, SensorDeviceClass.POWER)
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:fire"


class MaalerportalHeatVolumeSensor(_RoundedCounterSensor):
    """Heat volume sensor for heat meters."""

    _round_to = 3

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        """Initialize the heat volume sensor."""
        super().__init__(coordinator, counter)

        self._attr_translation_key = "heat_volume"
        self._attr_unique_id = f"{self._installation_id}_heat_volume"
        self._apply_counter_unit(UnitOfVolume.CUBIC_METERS, SensorDeviceClass.WATER)
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_icon = "mdi:water-thermometer"


# Sensors built from historical readings

class _HistorySensor(MaalerportalBaseSensor):
    """A sensor that works on /readings/historical, at most once an hour."""

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        super().__init__(coordinator, counter)
        self._history_lock = asyncio.Lock()
        self._last_history_run: datetime | None = None

    @property
    def _history_interval(self) -> timedelta:
        return max(self.coordinator.update_interval or MIN_HISTORY_INTERVAL, MIN_HISTORY_INTERVAL)

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self._schedule_history_run()

    @callback
    def _handle_coordinator_update(self) -> None:
        self._schedule_history_run()
        super()._handle_coordinator_update()

    @callback
    def _schedule_history_run(self) -> None:
        if self._history_lock.locked():
            return
        last = self._last_history_run
        if last is not None and dt_util.utcnow() - last < self._history_interval:
            return
        self.hass.async_create_task(self._async_run_history(), eager_start=False)

    async def async_force_refresh(self) -> None:
        await self._async_run_history(force=True)

    async def _async_run_history(self, force: bool = False) -> None:
        async with self._history_lock:
            now = dt_util.utcnow()
            last = self._last_history_run
            if not force and last is not None and now - last < self._history_interval:
                return
            self._last_history_run = now
            try:
                done = await self._async_process_history(force)
            except Exception:  # noqa: BLE001 - never let one round kill the sensor
                _LOGGER.exception("Error updating history for %s", self.entity_id)
                done = False
            if not done:
                # Try again on the next coordinator update instead of in an hour.
                self._last_history_run = last

    async def _async_process_history(self, force: bool) -> bool:
        """Do one history round; False if the API could not be asked."""
        raise NotImplementedError


@dataclass
class _VirtualMeterData(ExtraStoredData):
    """What the virtual meter keeps across restarts."""

    total: float
    seen: dict[str, float]

    def as_dict(self) -> dict[str, Any]:
        return {"total": self.total, "seen": self.seen}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> _VirtualMeterData | None:
        try:
            return cls(float(data["total"]), {str(k): float(v) for k, v in data["seen"].items()})
        except (KeyError, TypeError, ValueError, AttributeError):
            return None


class MaalerportalConsumptionSensor(_HistorySensor, RestoreEntity):
    """Virtual cumulative meter reading for consumption-type counters.

    Adds up every consumption reading it has seen. Readings are remembered by
    timestamp for a month, so a reading the utility delivers late is still
    added, and a corrected reading adds only the difference.
    """

    # Readings older than this are forgotten; the API never sends older ones
    # on a regular round.
    _SEEN_RETENTION = timedelta(days=MAX_HISTORY_DAYS + 2)

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        """Initialize the consumption sensor."""
        super().__init__(coordinator, counter)

        counter_type = counter.get("counterType", "")
        is_primary = counter.get("isPrimary", False)

        # counter_type may always be "ColdWater" for water meters, so the
        # installation type tells hot from cold water.
        installation_type = coordinator.installation.get("installationType", "").lower()

        if counter_type == "ColdWater" or counter_type == "HotWater":
            if installation_type == "hotwater":
                self._attr_translation_key = "virtual_hot_water_meter"
            else:
                self._attr_translation_key = "virtual_cold_water_meter"
            default_unit, device_class = UnitOfVolume.CUBIC_METERS, SensorDeviceClass.WATER
        elif counter_type == "ElectricityFromGrid":
            self._attr_translation_key = "virtual_electricity_meter"
            default_unit, device_class = UnitOfEnergy.KILO_WATT_HOUR, SensorDeviceClass.ENERGY
        elif counter_type == "ElectricityToGrid":
            self._attr_translation_key = "virtual_electricity_export_meter"
            default_unit, device_class = UnitOfEnergy.KILO_WATT_HOUR, SensorDeviceClass.ENERGY
        elif counter_type == "Heat":
            self._attr_translation_key = "virtual_heat_meter"
            default_unit, device_class = UnitOfEnergy.KILO_WATT_HOUR, SensorDeviceClass.ENERGY
        else:
            self._attr_translation_key = "virtual_meter"
            default_unit, device_class = None, None
        self._apply_counter_unit(default_unit, device_class)

        suffix = "primary" if is_primary else "secondary"
        self._attr_unique_id = f"{self._installation_id}_{counter_type.lower()}_consumption_{suffix}"
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_icon = "mdi:counter"

        self._cumulative_sum: float = 0.0
        # Reading timestamp (ISO, UTC) -> consumption already added.
        self._seen: dict[str, float] = {}
        # Set when restored from an older version that only kept the newest
        # processed timestamp: readings up to it are already in the total.
        self._legacy_cutoff: datetime | None = None

    @property
    def native_value(self) -> Optional[float]:
        """Return the virtual cumulative meter reading."""
        if self._cumulative_sum > 0:
            return round(self._cumulative_sum, 3)
        return None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        attrs = super().extra_state_attributes
        if self._cumulative_sum > 0:
            attrs["cumulative_sum"] = self._cumulative_sum
        if self._seen:
            attrs["last_processed_timestamp"] = max(self._seen)
        attrs["virtual_meter"] = True
        return attrs

    @property
    def extra_restore_state_data(self) -> _VirtualMeterData:
        return _VirtualMeterData(self._cumulative_sum, dict(self._seen))

    async def async_added_to_hass(self) -> None:
        """Restore the running total before the first history round."""
        if (extra := await self.async_get_last_extra_data()) is not None and (
            data := _VirtualMeterData.from_dict(extra.as_dict())
        ) is not None:
            self._cumulative_sum = data.total
            self._seen = data.seen
        elif (last_state := await self.async_get_last_state()) is not None:
            # Restored from a version before this one.
            if (value := parse_number(last_state.state)) is not None:
                self._cumulative_sum = value
            self._legacy_cutoff = parse_api_timestamp(
                last_state.attributes.get("last_processed_timestamp")
            )
            if self._cumulative_sum > 0 and self._legacy_cutoff is None:
                # A total without a timestamp: count only what comes from now on.
                self._legacy_cutoff = dt_util.utcnow()
        await super().async_added_to_hass()

    def _history_days(self) -> int:
        newest = max(
            (ts for key in self._seen if (ts := parse_api_timestamp(key)) is not None),
            default=None,
        )
        if newest is None or self._legacy_cutoff is not None:
            return MAX_HISTORY_DAYS
        behind = (dt_util.utcnow() - newest).days + 2
        return max(RECENT_HISTORY_DAYS, min(behind, MAX_HISTORY_DAYS))

    async def _async_process_history(self, force: bool) -> bool:
        readings = await self.coordinator.async_get_history(self._history_days(), force=force)
        if readings is None:
            return False
        counter_id = self._counter.get("meterCounterId")
        added = self.apply_readings(readings, counter_id)
        if added:
            _LOGGER.debug(
                "Virtual meter %s: added %s, total now %s",
                self.entity_id, added, self._cumulative_sum,
            )
        self.async_write_ha_state()
        return True

    def apply_readings(self, readings: list[dict], counter_id: str) -> float:
        """Add the consumption of readings not counted yet; return what was added."""
        added = 0.0
        for reading in readings:
            if reading.get("meterCounterId") != counter_id:
                continue
            timestamp = parse_api_timestamp(reading.get("timestamp"))
            value = parse_number(reading.get("value"))
            if timestamp is None or value is None:
                continue
            value = max(value, 0.0)
            key = timestamp.isoformat()
            if key in self._seen:
                delta = max(value - self._seen[key], 0.0)
                self._seen[key] = max(value, self._seen[key])
            elif self._legacy_cutoff is not None and timestamp <= self._legacy_cutoff:
                delta = 0.0
                self._seen[key] = value
            else:
                delta = value
                self._seen[key] = value
            added += delta

        self._cumulative_sum += added
        self._legacy_cutoff = None
        oldest_kept = dt_util.utcnow() - self._SEEN_RETENTION
        self._seen = {
            key: value
            for key, value in self._seen.items()
            if (ts := parse_api_timestamp(key)) is not None and ts >= oldest_kept
        }
        return added


class MaalerportalStatisticSensor(_HistorySensor):
    """Imports hourly history into Home Assistant's long-term statistics.

    For the Energy Dashboard. Utilities deliver readings with a delay, so the
    rows are written into the statistics of this entity afterwards instead of
    being recorded from its state, which stays unknown on purpose.

    Everything needed to continue the series is read back from the recorder,
    so restarts and late hours never put a step in the sum.
    """

    def __init__(self, coordinator: MaalerportalCoordinator, counter: dict) -> None:
        """Initialize the statistics sensor."""
        super().__init__(coordinator, counter)

        counter_type = counter.get("counterType", "")
        is_primary = counter.get("isPrimary", False)
        suffix = "primary" if is_primary else "secondary"

        installation_type = coordinator.installation.get("installationType", "").lower()

        if counter_type == "ColdWater" or counter_type == "HotWater":
            if installation_type == "hotwater":
                self._attr_translation_key = "hot_water_statistic"
                self._attr_icon = "mdi:water-thermometer"
            else:
                self._attr_translation_key = "cold_water_statistic"
                self._attr_icon = "mdi:water"
            default_unit, device_class = UnitOfVolume.CUBIC_METERS, SensorDeviceClass.WATER
        elif counter_type == "ElectricityFromGrid":
            self._attr_translation_key = "electricity_statistic" if is_primary else "electricity_import_statistic"
            self._attr_icon = "mdi:transmission-tower-import"
            default_unit, device_class = UnitOfEnergy.KILO_WATT_HOUR, SensorDeviceClass.ENERGY
        elif counter_type == "ElectricityToGrid":
            self._attr_translation_key = "electricity_export_statistic"
            self._attr_icon = "mdi:transmission-tower-export"
            default_unit, device_class = UnitOfEnergy.KILO_WATT_HOUR, SensorDeviceClass.ENERGY
        elif counter_type == "Heat":
            self._attr_translation_key = "heat_statistic"
            self._attr_icon = "mdi:fire"
            default_unit, device_class = UnitOfEnergy.KILO_WATT_HOUR, SensorDeviceClass.ENERGY
        else:
            self._attr_translation_key = "energy_statistic"
            self._attr_icon = "mdi:chart-line-variant"
            default_unit, device_class = UnitOfEnergy.KILO_WATT_HOUR, SensorDeviceClass.ENERGY
        self._apply_counter_unit(default_unit, device_class)
        self._unit_class = statistics_unit_class(self._attr_device_class)

        self._attr_unique_id = f"{self._installation_id}_{counter_type.lower()}_statistic_{suffix}"
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING

        # "counter" = cumulative meter reading, "consumption" = per-period consumption
        self._reading_type = counter.get("readingType", "counter").lower()
        self._stat_unit = self._attr_native_unit_of_measurement
        self._last_stats_update: Optional[datetime] = None

    @property
    def native_value(self) -> Optional[float]:
        """Always None: this sensor only carries imported statistics."""
        return None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        attrs: dict[str, Any] = {"reading_type": self._reading_type}
        if self.entity_id:
            attrs["statistic_id"] = self.entity_id
        if self._last_stats_update:
            attrs["last_statistics_update"] = self._last_stats_update.isoformat()
        return attrs

    @property
    def _units(self) -> dict[str, str] | None:
        """Read statistics back in the unit they are stored in."""
        if self._unit_class and self._stat_unit:
            return {self._unit_class: self._stat_unit}
        return None

    async def _async_last_row(self, statistic_id: str) -> tuple[float, float] | None:
        last = await get_instance(self.hass).async_add_executor_job(
            get_last_statistics, self.hass, 1, statistic_id, False, {"state", "sum"}
        )
        rows = last.get(statistic_id) if last else None
        if not rows:
            return None
        return rows[0].get("state"), rows[0].get("sum")

    async def _async_last_row_start(self, statistic_id: str) -> datetime | None:
        last = await get_instance(self.hass).async_add_executor_job(
            get_last_statistics, self.hass, 1, statistic_id, False, {"sum"}
        )
        rows = last.get(statistic_id) if last else None
        if not rows:
            return None
        return dt_util.utc_from_timestamp(rows[0]["start"])

    async def _async_stored_rows(
        self, statistic_id: str, start: datetime
    ) -> dict[datetime, tuple[float, float]]:
        result = await get_instance(self.hass).async_add_executor_job(
            statistics_during_period,
            self.hass,
            start,
            None,
            {statistic_id},
            "hour",
            self._units,
            {"state", "sum"},
        )
        return {
            dt_util.utc_from_timestamp(row["start"]): (row.get("state"), row.get("sum"))
            for row in result.get(statistic_id, [])
        }

    async def _async_process_history(self, force: bool) -> bool:
        statistic_id = self.entity_id
        counter_id = self._counter.get("meterCounterId")
        if not statistic_id or not counter_id:
            return True

        last_row = await self._async_last_row(statistic_id)
        if last_row is None:
            days = MAX_HISTORY_DAYS
            _LOGGER.info("No statistics yet for %s, importing the last %d days", statistic_id, days)
        else:
            newest = await self._async_last_row_start(statistic_id)
            behind = (dt_util.utcnow() - newest).days + 2 if newest else MAX_HISTORY_DAYS
            days = max(RECENT_HISTORY_DAYS, min(behind, MAX_HISTORY_DAYS))

        readings = await self.coordinator.async_get_history(days, force=force)
        if readings is None:
            return False

        # Same window the coordinator asked the API for, widened by the hour a
        # counter reading is moved back.
        now = dt_util.utcnow()
        window_start = (now - timedelta(days=days)).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(hours=2)
        stored = await self._async_stored_rows(statistic_id, window_start)

        if self._reading_type == "consumption":
            api_hours = consumption_by_hour(readings, counter_id)
            if not api_hours:
                _LOGGER.debug("No readings for counter %s", counter_id)
                return True
            rows, _ = consumption_rows(api_hours, stored, total_before(stored, last_row))
        else:
            by_hour = counter_readings_by_hour(readings, counter_id)
            if not by_hour:
                _LOGGER.debug("No readings for counter %s", counter_id)
                return True
            rows = counter_rows(by_hour, counter_offset(last_row, by_hour), stored)

        self._last_stats_update = now
        if not rows:
            _LOGGER.debug("Statistics for %s are up to date", statistic_id)
            return True

        metadata = StatisticMetaData(
            has_sum=True,
            mean_type=StatisticMeanType.NONE,
            name=self.name if isinstance(self.name, str) else self._base_device_name,
            source="recorder",
            statistic_id=statistic_id,
            unit_of_measurement=self._stat_unit,
            unit_class=self._unit_class,
        )
        statistics = [
            StatisticData(start=start, state=state, sum=total)
            for start, (state, total) in sorted(rows.items())
        ]
        async_import_statistics(self.hass, metadata, statistics)
        _LOGGER.info(
            "Imported %d statistics rows for %s (%s to %s)",
            len(statistics),
            statistic_id,
            statistics[0]["start"].isoformat(),
            statistics[-1]["start"].isoformat(),
        )
        return True
