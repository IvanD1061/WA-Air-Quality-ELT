"""Extract: pull OpenAQ data and land it, untouched, as gzipped NDJSON.

    python -m pipeline.extract --start 2026-10-05 --end 2026-10-06

What one run does:

1. Downloads every monitoring location in the bounding box (a full snapshot).
2. Picks the sensors worth querying: recently active, measuring a pollutant we
   track.
3. Asks each sensor for its hourly values across the whole date window in one
   request, then sorts the records into one file per UTC day.
4. Uploads the files only after every sensor has been fetched.

Nothing is cleaned here. Renaming, casting, de-duplicating and flagging bad
values all happen later in dbt, where the logic is versioned SQL and can be
re-run over the raw files whenever the rules change. That is the "T last" in
ELT.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from pipeline import paths
from pipeline.config import ExtractSettings, StorageSettings
from pipeline.openaq_client import AuthError, NotFoundError, OpenAQClient, OpenAQError
from pipeline.storage import Storage, get_storage

log = logging.getLogger(__name__)


class ExtractError(RuntimeError):
    """The extract did not produce a trustworthy result."""


@dataclass(frozen=True)
class SensorRef:
    sensor_id: int
    location_id: int
    parameter: str


def parse_utc(value: Any) -> datetime | None:
    """Parse an ISO 8601 string such as '2026-10-06T07:00:00Z'. None if it can't be."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def select_sensors(
    locations: Iterable[dict[str, Any]],
    *,
    parameters: frozenset[str],
    active_since: date,
    max_sensors: int | None = None,
) -> list[SensorRef]:
    """Choose which sensors to request measurements for.

    The location catalogue is full of stations that stopped reporting years
    ago. Requesting each of them would spend most of the rate limit on empty
    responses, so anything whose last reading is older than `active_since` is
    skipped.
    """
    chosen: dict[int, SensorRef] = {}
    for location in locations:
        last_seen = parse_utc((location.get("datetimeLast") or {}).get("utc"))
        if last_seen is None or last_seen.date() < active_since:
            continue
        for sensor in location.get("sensors") or []:
            parameter = ((sensor.get("parameter") or {}).get("name") or "").lower()
            if parameter not in parameters or sensor.get("id") is None:
                continue
            chosen[sensor["id"]] = SensorRef(sensor["id"], location["id"], parameter)

    # Sorted so that a `max_sensors` cap picks the same sensors every run.
    ordered = sorted(chosen.values(), key=lambda s: s.sensor_id)
    return ordered[:max_sensors] if max_sensors else ordered


class PartitionedWriter:
    """Streams records into one gzipped NDJSON file per date.

    Records go straight to disk as they arrive, so memory use stays flat
    whether the window is one day or three months.
    """

    def __init__(self, directory: Path, dataset: str) -> None:
        self._directory = directory
        self._dataset = dataset
        self._files: dict[date, Any] = {}
        self._paths: dict[date, Path] = {}
        self.counts: Counter[date] = Counter()

    def write(self, day: date, record: dict[str, Any]) -> None:
        handle = self._files.get(day)
        if handle is None:
            path = self._directory / f"{self._dataset}_{day.isoformat()}.jsonl.gz"
            # Kept open across calls on purpose; close() shuts them all.
            handle = self._files[day] = gzip.open(path, "wt", encoding="utf-8")  # noqa: SIM115
            self._paths[day] = path
        # One JSON document per line. ensure_ascii=False keeps "µg/m³" as is.
        handle.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False))
        handle.write("\n")
        self.counts[day] += 1

    def close(self) -> dict[date, Path]:
        for handle in self._files.values():
            handle.close()
        return dict(self._paths)


def envelope(data: dict[str, Any], *, extracted_at: str, request_path: str, **context: Any) -> dict:
    """Wrap an API record with the context needed to trace it later.

    The record itself goes under `data` exactly as received. The hourly
    endpoint does not repeat the sensor id in its response (it is only in the
    URL), so without this wrapper a measurement could not be tied back to a
    sensor.
    """
    return {**context, "extracted_at": extracted_at, "request_path": request_path, "data": data}


def run_extract(
    *,
    start: date,
    end: date,
    snapshot_date: date,
    settings: ExtractSettings,
    storage_settings: StorageSettings,
    client: OpenAQClient,
    storage: Storage,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Run one extract and return a summary of what happened."""
    started = time.monotonic()
    extracted_at = (now or datetime.now(UTC)).strftime("%Y-%m-%dT%H:%M:%SZ")
    days = paths.date_range(start, end)
    prefix = storage_settings.prefix

    with tempfile.TemporaryDirectory(prefix="openaq_") as tmp:
        tmp_dir = Path(tmp)

        # 1. Location snapshot ------------------------------------------------
        log.info("Fetching locations in bbox %s (iso=%s)", settings.bbox, settings.iso)
        locations = list(
            client.iter_locations(
                bbox=settings.bbox, iso=settings.iso, monitors_only=settings.monitors_only
            )
        )
        if not locations:
            raise ExtractError("The API returned no locations. Check OPENAQ_BBOX and OPENAQ_ISO.")

        location_writer = PartitionedWriter(tmp_dir, paths.LOCATIONS)
        for location in locations:
            location_writer.write(
                snapshot_date,
                envelope(location, extracted_at=extracted_at, request_path="/locations"),
            )
        location_files = location_writer.close()

        # 2. Which sensors to ask ---------------------------------------------
        sensors = select_sensors(
            locations,
            parameters=settings.parameters,
            active_since=start - timedelta(days=settings.active_within_days),
            max_sensors=settings.max_sensors,
        )
        if not sensors:
            raise ExtractError("No active sensors matched. Check OPENAQ_PARAMETERS and the bbox.")
        log.info(
            "%d locations, %d active sensors to query for %s..%s",
            len(locations),
            len(sensors),
            start,
            end,
        )

        # 3. Hourly measurements ----------------------------------------------
        # The window is padded by an hour at the end because the API does not
        # document whether its datetime filter applies to the start or the end
        # of each hour. Whatever it returns, the check below keeps exactly the
        # records whose hour starts inside the window.
        datetime_from = f"{start.isoformat()}T00:00:00Z"
        datetime_to = f"{(end + timedelta(days=1)).isoformat()}T01:00:00Z"

        writer = PartitionedWriter(tmp_dir, paths.MEASUREMENTS)
        failed: list[dict[str, Any]] = []
        outside_window = 0
        no_timestamp = 0

        for index, sensor in enumerate(sensors, start=1):
            request_path = f"/sensors/{sensor.sensor_id}/hours"
            try:
                # Buffered per sensor so a sensor that fails halfway through
                # its pages leaves nothing behind.
                records = list(
                    client.iter_sensor_hours(
                        sensor.sensor_id, datetime_from=datetime_from, datetime_to=datetime_to
                    )
                )
            except AuthError:
                raise  # every remaining request would fail the same way
            except NotFoundError:
                failed.append({"sensor_id": sensor.sensor_id, "error": "404 not found"})
                continue
            except OpenAQError as exc:
                failed.append({"sensor_id": sensor.sensor_id, "error": str(exc)[:200]})
                continue

            for record in records:
                period_start = parse_utc(
                    ((record.get("period") or {}).get("datetimeFrom") or {}).get("utc")
                )
                if period_start is None:
                    # Keep it rather than lose it. dbt drops rows without a
                    # timestamp and the count shows up in the run summary.
                    day = start
                    no_timestamp += 1
                elif start <= period_start.date() <= end:
                    day = period_start.date()
                else:
                    outside_window += 1
                    continue
                writer.write(
                    day,
                    envelope(
                        record,
                        extracted_at=extracted_at,
                        request_path=request_path,
                        sensor_id=sensor.sensor_id,
                        location_id=sensor.location_id,
                    ),
                )

            if index % 50 == 0 or index == len(sensors):
                log.info("Fetched %d/%d sensors", index, len(sensors))

        measurement_files = writer.close()

        failed_pct = 100.0 * len(failed) / len(sensors)
        if failed_pct > settings.max_failed_sensor_pct:
            raise ExtractError(
                f"{len(failed)} of {len(sensors)} sensors failed ({failed_pct:.1f}%), over the "
                f"{settings.max_failed_sensor_pct}% limit. Nothing was uploaded. "
                f"First failures: {failed[:3]}"
            )
        if not measurement_files:
            raise ExtractError(
                f"No measurements came back for {start}..{end}. Nothing was uploaded."
            )

        # 4. Upload -----------------------------------------------------------
        # Only now, after every request succeeded. If the run dies earlier,
        # S3 still holds the previous complete files, not half of the new ones.
        uploaded = []
        for day, path in location_files.items():
            uploaded.append(storage.upload(path, paths.object_key(prefix, paths.LOCATIONS, day)))
        for day in sorted(measurement_files):
            uploaded.append(
                storage.upload(
                    measurement_files[day], paths.object_key(prefix, paths.MEASUREMENTS, day)
                )
            )

    empty_days = [d.isoformat() for d in days if d not in writer.counts]
    if empty_days:
        # An existing file for these days is left alone rather than replaced
        # with nothing.
        log.warning("No records for %s; any earlier files for those days are untouched", empty_days)

    return {
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "snapshot_date": snapshot_date.isoformat(),
        "locations": len(locations),
        "sensors_queried": len(sensors),
        "sensors_failed": len(failed),
        "failed_sensors": failed[:20],
        "api_requests": client.request_count,
        "records": sum(writer.counts.values()),
        "records_by_day": {d.isoformat(): n for d, n in sorted(writer.counts.items())},
        "records_outside_window": outside_window,
        "records_without_timestamp": no_timestamp,
        "days_without_records": empty_days,
        "files_uploaded": uploaded,
        "duration_seconds": round(time.monotonic() - started, 1),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="Extract OpenAQ data to S3.")
    parser.add_argument("--start", type=date.fromisoformat, help="First UTC day (YYYY-MM-DD).")
    parser.add_argument(
        "--end", type=date.fromisoformat, default=yesterday, help="Last UTC day, inclusive."
    )
    parser.add_argument(
        "--snapshot-date",
        type=date.fromisoformat,
        default=datetime.now(UTC).date(),
        help="Date to file the location snapshot under.",
    )
    args = parser.parse_args(argv)
    args.start = args.start or args.end
    if args.end < args.start:
        parser.error("--end is before --start")
    return args


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = parse_args(argv)
    settings = ExtractSettings.from_env()
    storage_settings = StorageSettings.from_env()

    summary = run_extract(
        start=args.start,
        end=args.end,
        snapshot_date=args.snapshot_date,
        settings=settings,
        storage_settings=storage_settings,
        client=OpenAQClient(settings.api_key, settings.base_url),
        storage=get_storage(storage_settings),
    )
    # One machine-readable line that is easy to find in the Airflow task log.
    print("EXTRACT_SUMMARY " + json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
