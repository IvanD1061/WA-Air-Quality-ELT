"""Load: copy raw files from S3 into the Snowflake RAW schema.

    python -m pipeline.load --start 2026-10-05 --end 2026-10-06

Each day is replaced as a unit: delete that day's rows, copy that day's file,
commit. Because both statements sit in one transaction, a failure leaves the
table exactly as it was, and running the same day twice gives the same rows as
running it once. That property is called idempotency, and it is what makes
retries and backfills safe.

The JSON is loaded as-is into a VARIANT column. No fields are picked out here;
that is dbt's job.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

from pipeline import paths
from pipeline.config import SnowflakeSettings

log = logging.getLogger(__name__)

TABLES = {paths.LOCATIONS: "LOCATIONS", paths.MEASUREMENTS: "MEASUREMENTS"}


class LoadError(RuntimeError):
    """The load did not bring in the data it was supposed to."""


def build_copy_sql(settings: SnowflakeSettings, dataset: str, day: date) -> str:
    """COPY statement for one day of one dataset.

    Identifiers and the date folder cannot be bound as parameters in a COPY,
    so they are formatted in. That is safe here because the identifiers are
    validated in config.py and `day` is a date object, not free text.

    The stage path is quoted because the folder name contains an "=".
    """
    schema = f"{settings.database}.{settings.raw_schema}"
    return f"""
copy into {schema}.{TABLES[dataset]}
    (raw_json, partition_date, source_file, source_row_number, loaded_at)
from (
    select
        $1,
        to_date(regexp_substr(metadata$filename, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}')),
        metadata$filename,
        metadata$file_row_number,
        metadata$start_scan_time
    from '@{schema}.{settings.stage}/{paths.partition_folder(dataset, day)}'
)
file_format = (format_name = '{schema}.{settings.file_format}')
on_error = abort_statement
force = true
""".strip()


def _rows_loaded(cursor) -> int:
    """Read the row count out of a COPY result.

    When COPY finds no files it returns a single status message instead of the
    usual per-file columns, so the column has to be looked up by name.
    """
    columns = [column[0].lower() for column in cursor.description or []]
    if "rows_loaded" not in columns:
        return 0
    index = columns.index("rows_loaded")
    return sum(int(row[index] or 0) for row in cursor.fetchall())


def reload_partition(cursor, settings: SnowflakeSettings, dataset: str, day: date) -> int:
    """Replace one day of one raw table. Returns the number of rows loaded."""
    table = f"{settings.database}.{settings.raw_schema}.{TABLES[dataset]}"
    cursor.execute("begin")
    try:
        cursor.execute(f"delete from {table} where partition_date = %s", (day.isoformat(),))
        # force = true because the file name is the same on every run, and
        # COPY normally skips a file name it has loaded before.
        cursor.execute(build_copy_sql(settings, dataset, day))
        loaded = _rows_loaded(cursor)
        if loaded == 0:
            # No file for this day. Roll back so the delete does not wipe out
            # rows that an earlier run loaded.
            cursor.execute("rollback")
            return 0
        cursor.execute("commit")
        return loaded
    except Exception:
        cursor.execute("rollback")
        raise


def run_load(
    *, cursor, settings: SnowflakeSettings, start: date, end: date, snapshot_date: date
) -> dict[str, Any]:
    started = time.monotonic()

    locations = reload_partition(cursor, settings, paths.LOCATIONS, snapshot_date)
    if locations == 0:
        raise LoadError(
            f"No location snapshot found for {snapshot_date}. Did the extract run with the "
            "same --snapshot-date?"
        )
    log.info("Loaded %d location rows for snapshot %s", locations, snapshot_date)

    measurements: dict[str, int] = {}
    for day in paths.date_range(start, end):
        measurements[day.isoformat()] = reload_partition(cursor, settings, paths.MEASUREMENTS, day)
        log.info("Loaded %d measurement rows for %s", measurements[day.isoformat()], day)

    if sum(measurements.values()) == 0:
        raise LoadError(f"No measurement files found for {start}..{end}. Nothing was loaded.")

    return {
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "snapshot_date": snapshot_date.isoformat(),
        "location_rows": locations,
        "measurement_rows": sum(measurements.values()),
        "measurement_rows_by_day": measurements,
        "days_without_files": [day for day, rows in measurements.items() if rows == 0],
        "duration_seconds": round(time.monotonic() - started, 1),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="Load raw OpenAQ files from S3 into Snowflake.")
    parser.add_argument("--start", type=date.fromisoformat, help="First UTC day (YYYY-MM-DD).")
    parser.add_argument(
        "--end", type=date.fromisoformat, default=yesterday, help="Last UTC day, inclusive."
    )
    parser.add_argument(
        "--snapshot-date",
        type=date.fromisoformat,
        default=datetime.now(UTC).date(),
        help="Which location snapshot to load.",
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
    settings = SnowflakeSettings.from_env()

    from pipeline.snowflake_conn import connect

    connection = connect(settings)
    try:
        summary = run_load(
            cursor=connection.cursor(),
            settings=settings,
            start=args.start,
            end=args.end,
            snapshot_date=args.snapshot_date,
        )
    finally:
        connection.close()
    print("LOAD_SUMMARY " + json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
