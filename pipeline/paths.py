"""Where raw files live.

The extractor writes these keys and the loader reads them, so the layout is
defined once here. Keys are deterministic for a given date, which is what makes
a re-run overwrite the previous attempt instead of adding a second copy.

    <prefix>/locations/snapshot_date=2026-10-07/locations.jsonl.gz
    <prefix>/measurements/measurement_date=2026-10-06/measurements.jsonl.gz

The `name=value` folder style is Hive partitioning. Athena, Spark, DuckDB and
Snowflake external tables all understand it, so the lake stays usable by tools
other than this pipeline.
"""

from __future__ import annotations

from datetime import date, timedelta

LOCATIONS = "locations"
MEASUREMENTS = "measurements"

_PARTITION_COLUMN = {LOCATIONS: "snapshot_date", MEASUREMENTS: "measurement_date"}


def partition_folder(dataset: str, day: date) -> str:
    """Folder for one day of one dataset, relative to the prefix or stage."""
    return f"{dataset}/{_PARTITION_COLUMN[dataset]}={day.isoformat()}/"


def object_key(prefix: str, dataset: str, day: date) -> str:
    """Full object key for one day of one dataset."""
    return f"{prefix.strip('/')}/{partition_folder(dataset, day)}{dataset}.jsonl.gz"


def date_range(start: date, end: date) -> list[date]:
    """Every date from start to end, both included."""
    if end < start:
        raise ValueError(f"end ({end}) is before start ({start})")
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]
