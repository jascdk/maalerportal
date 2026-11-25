# Changelog

## 2.3.0

### Fixed
- **Energy Dashboard spikes on counter meters** (water, heat, some electricity).
  The first import stored `sum = reading - first reading`, later rounds stored
  the raw reading, so the sum jumped by the whole meter reading between two
  hours. Every row now uses one constant offset, read back from the stored
  statistics, so the series continues across polls and restarts.
- **Hours delivered late were never imported.** Each round re-reads the last
  7 days and compares them with the stored rows. Late hours are added; for
  consumption meters the running total of every later hour is recomputed.
- **Sensors stuck on "unknown" when `/readings/latest` answers
  `latestValue: null`.** The newest historical reading is used instead, never
  one older than what the sensor already shows.
- Sub-hourly consumption readings are added up per hour instead of written as
  several rows for the same hour.
- Values with a decimal comma (`"1234,56"`) were read as 123456.
- Negative meter readings were turned positive; they are now ignored and the
  sensor keeps its last value.
- The virtual meter reading now also counts readings delivered late, and never
  counts a reading twice.

### Changed
- One data coordinator per installation: all sensors share one
  `/readings/latest` call per polling interval and one cached
  `/readings/historical` call, over Home Assistant's shared HTTP session.
  Before, every sensor made its own calls with its own connection.
- Rate limiting (HTTP 429) now really backs off: the polling interval doubles,
  up to 4 hours, and returns to normal on the next good answer. HTTP 403/404
  backs off up to 24 hours.
- Sensors have their value right after setup instead of after the first poll,
  and keep their last value through a failed poll.
- If the first fetch fails, the basic sensor is created as before, and the
  integration reloads by itself once the meter answers, to create the real
  sensors.

Entity IDs and unique IDs are unchanged, so history and Energy Dashboard
settings are kept. Spikes already stored by the old version are not repaired
automatically; fix them under Developer tools → Statistics.
