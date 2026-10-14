"""Synthetic OpenAQ records for tests.

These are made up, but shaped exactly like the documented v3 responses
(https://docs.openaq.org/resources/locations and /resources/measurements), and
they include the kinds of mess the pipeline has to survive: a missing name, a
dead station, null and sentinel values, a duplicate, and a record with no
timestamp.

The same records feed the Python unit tests and the dbt build that CI runs
against a local Snowflake emulator, so both layers are tested on one dataset.
"""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta

# OpenAQ parameter ids, as they appear in the API documentation.
PARAMETERS = {
    "pm25": {"id": 2, "name": "pm25", "units": "µg/m³", "displayName": "PM2.5"},
    "pm10": {"id": 1, "name": "pm10", "units": "µg/m³", "displayName": "PM10"},
    "o3": {"id": 10, "name": "o3", "units": "ppm", "displayName": "O₃"},
    "temperature": {"id": 100, "name": "temperature", "units": "c", "displayName": None},
}

PACIFIC_OFFSET = timedelta(hours=-7)  # PDT; good enough for fixtures


def _stamp(moment: datetime) -> dict[str, str]:
    local = moment + PACIFIC_OFFSET
    return {
        "utc": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "local": local.strftime("%Y-%m-%dT%H:%M:%S") + "-07:00",
    }


def make_location(
    location_id: int,
    name: str | None,
    *,
    sensors: dict[int, str],
    last_seen: datetime,
    locality: str | None = "Seattle",
    is_monitor: bool = True,
    latitude: float = 47.6,
    longitude: float = -122.3,
) -> dict:
    return {
        "id": location_id,
        "name": name,
        "locality": locality,
        "timezone": "America/Los_Angeles",
        "country": {"id": 155, "code": "US", "name": "United States"},
        "owner": {"id": 4, "name": "Unknown Governmental Organization"},
        "provider": {"id": 119, "name": "AirNow"} if is_monitor else {"id": 166, "name": "Clarity"},
        "isMobile": False,
        "isMonitor": is_monitor,
        "instruments": [{"id": 2, "name": "Government Monitor"}],
        "sensors": [
            {
                "id": sensor_id,
                "name": f"{parameter} {PARAMETERS[parameter]['units']}",
                "parameter": PARAMETERS[parameter],
            }
            for sensor_id, parameter in sensors.items()
        ],
        "coordinates": {"latitude": latitude, "longitude": longitude},
        "licenses": None,
        "bounds": [longitude, latitude, longitude, latitude],
        "distance": None,
        "datetimeFirst": _stamp(datetime(2018, 1, 1, 8, tzinfo=UTC)),
        "datetimeLast": _stamp(last_seen),
    }


def make_hour(parameter: str, start: datetime, value: float | None, *, observed: int = 1) -> dict:
    """One record from /v3/sensors/{id}/hours covering [start, start + 1h)."""
    expected = max(observed, 1)
    return {
        "value": value,
        "flagInfo": {"hasFlags": False},
        "parameter": {**PARAMETERS[parameter], "displayName": None},
        "period": {
            "label": "1hour",
            "interval": "01:00:00",
            "datetimeFrom": _stamp(start),
            "datetimeTo": _stamp(start + timedelta(hours=1)),
        },
        "coordinates": None,
        "summary": {
            "min": value,
            "q02": value,
            "q25": value,
            "median": value,
            "q75": value,
            "q98": value,
            "max": value,
            "avg": value,
            "sd": None,
        },
        "coverage": {
            "expectedCount": expected,
            "expectedInterval": "01:00:00",
            "observedCount": observed,
            "observedInterval": "01:00:00",
            "percentComplete": 100.0 * observed / expected,
            "percentCoverage": 100.0 * observed / expected,
            "datetimeFrom": _stamp(start),
            "datetimeTo": _stamp(start + timedelta(hours=1)),
        },
    }


def sample_locations(as_of: date) -> list[dict]:
    recent = datetime(as_of.year, as_of.month, as_of.day, tzinfo=UTC)
    return [
        make_location(
            100, "Seattle - Beacon Hill", sensors={1001: "pm25", 1002: "o3"}, last_seen=recent
        ),
        # No name and no locality: real stations sometimes have neither.
        make_location(
            200,
            None,
            sensors={2001: "pm25", 2002: "temperature"},
            last_seen=recent,
            locality=None,
            is_monitor=False,
            latitude=47.25,
            longitude=-122.44,
        ),
        # Stopped reporting years ago: should never be queried.
        make_location(
            300, "Old Station", sensors={3001: "pm25"}, last_seen=datetime(2019, 6, 1, tzinfo=UTC)
        ),
    ]


def sample_hours(sensor_id: int, parameter: str, start: date, end: date) -> list[dict]:
    """A full set of hourly records for one sensor, with deliberate defects."""
    records = []
    moment = datetime(start.year, start.month, start.day, tzinfo=UTC)
    stop = datetime(end.year, end.month, end.day, tzinfo=UTC) + timedelta(days=1)
    hour_index = 0
    while moment < stop:
        if parameter == "o3":
            value = round(0.03 + 0.01 * math.sin(hour_index / 4), 4)
        else:
            value = round(8 + 5 * math.sin(hour_index / 5) + (sensor_id % 7), 2)
        records.append(make_hour(parameter, moment, value))
        moment += timedelta(hours=1)
        hour_index += 1

    if sensor_id == 1001:
        records[3]["value"] = None  # the sensor reported nothing
        records[5]["value"] = -999.0  # a "no data" sentinel leaking through
        records[7]["value"] = 99999.0  # physically impossible
        records.append(dict(records[10]))  # the API sent the same hour twice
    if sensor_id == 2001:
        broken = make_hour(parameter, datetime(start.year, start.month, start.day, tzinfo=UTC), 5.0)
        broken["period"] = None  # no timestamp at all
        records.append(broken)
        records[2]["flagInfo"] = {"hasFlags": True}  # flagged by the data provider
    return records
