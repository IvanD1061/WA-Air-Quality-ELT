"""Run the dbt project against a local Snowflake emulator. No account needed.

    python scripts/dbt_emulator.py                       # the full check CI runs
    python scripts/dbt_emulator.py build -s staging      # or any single dbt command

The full check:
  1. Starts fakesnow, an in-memory stand-in that speaks Snowflake's wire
     protocol and SQL dialect (it translates each statement to DuckDB).
  2. Creates the RAW tables and fills them with the synthetic records from
     tests/sample_data.py, wrapped exactly as the real extractor wraps them.
  3. Runs `dbt source freshness` and `dbt build`: every model, every data test,
     every unit test.
  4. Re-loads the most recent day with one corrected value, the way a retry or
     a late-data re-run would, and builds again. The fact table must end up
     with the same number of rows and the corrected value. That is the
     idempotency promise, tested rather than assumed.

What this proves: the models compile, the SQL runs, and the tests pass on
known data. What it does not prove: that real Snowflake agrees with the
emulator on every detail. `make dbt-build` against a real account is still the
final word.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from pipeline import paths  # noqa: E402
from pipeline.extract import envelope  # noqa: E402
from tests import sample_data  # noqa: E402

PORT = int(os.environ.get("EMULATOR_PORT", "8765"))
SENSORS = [(1001, 100, "pm25"), (1002, 100, "o3"), (2001, 200, "pm25")]
CORRECTED_SENSOR = 1002
CORRECTED_VALUE = 0.123

# The emulator cannot run the MERGE statement dbt generates, so CI folds new
# rows in with delete+insert instead. Both strategies end with the same rows.
DBT_VARS = ["--vars", "{incremental_strategy: delete+insert}"]

RAW_TABLE_DDL = """
create table if not exists AIR_QUALITY.RAW.{table} (
    raw_json variant not null,
    partition_date date not null,
    source_file varchar not null,
    source_row_number number not null,
    loaded_at timestamp_ltz not null
)
"""


def apply_emulator_workarounds() -> None:
    """Smooth over two things dbt sends that the emulator cannot run.

    Both only matter to a throwaway in-memory database, and neither changes
    what the models compute:
      - a bare BEGIN (dbt wraps seed loads in a transaction): ignored
      - CREATE TEMPORARY TABLE / VIEW with a database-qualified name (dbt
        unit tests and incremental runs stage their rows this way): created
        as an ordinary table or view instead
    """
    from fakesnow.cursor import FakeSnowflakeCursor

    original_execute = FakeSnowflakeCursor.execute
    # BEGIN, allowing for whitespace and SQL comments in front of it.
    begin = re.compile(r"^(\s|--[^\n]*\n|/\*.*?\*/)*begin\b", re.IGNORECASE | re.DOTALL)
    temporary = re.compile(
        r"\bcreate\s+(or\s+replace\s+)?temporary\s+(table|view)\b", re.IGNORECASE
    )

    def execute(self, command, *args, **kwargs):
        if begin.match(command):
            command = "select 'Statement executed successfully.'"
        command = temporary.sub(lambda m: f"create {m.group(1) or ''}{m.group(2)}", command)
        return original_execute(self, command, *args, **kwargs)

    FakeSnowflakeCursor.execute = execute


def sample_rows(end: date, days: int = 3):
    """Yield (table, partition_date, envelope) for a small, messy dataset."""
    start = end - timedelta(days=days - 1)
    snapshot = end + timedelta(days=1)
    extracted_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    for location in sample_data.sample_locations(end):
        yield (
            "LOCATIONS",
            snapshot,
            envelope(location, extracted_at=extracted_at, request_path="/locations"),
        )

    for sensor_id, location_id, parameter in SENSORS:
        for record in sample_data.sample_hours(sensor_id, parameter, start, end):
            stamp = ((record.get("period") or {}).get("datetimeFrom") or {}).get("utc")
            day = date.fromisoformat(stamp[:10]) if stamp else start
            yield (
                "MEASUREMENTS",
                day,
                envelope(
                    record,
                    extracted_at=extracted_at,
                    request_path=f"/sensors/{sensor_id}/hours",
                    sensor_id=sensor_id,
                    location_id=location_id,
                ),
            )


def insert_rows(cursor, rows) -> dict[str, int]:
    counts = {"LOCATIONS": 0, "MEASUREMENTS": 0}
    for table, day, record in rows:
        dataset = paths.LOCATIONS if table == "LOCATIONS" else paths.MEASUREMENTS
        counts[table] += 1
        cursor.execute(
            f"insert into AIR_QUALITY.RAW.{table} "
            "select parse_json(%s), %s::date, %s, %s, current_timestamp()",
            (
                json.dumps(record, ensure_ascii=False),
                day.isoformat(),
                paths.object_key("raw/openaq", dataset, day),
                counts[table],
            ),
        )
    return counts


def seed_raw(cursor, end: date) -> dict[str, int]:
    cursor.execute("create database if not exists AIR_QUALITY")
    cursor.execute("create schema if not exists AIR_QUALITY.RAW")
    for table in ("LOCATIONS", "MEASUREMENTS"):
        cursor.execute(RAW_TABLE_DDL.format(table=table))
    return insert_rows(cursor, sample_rows(end))


def reload_day(cursor, day: date) -> str:
    """Replace one day in RAW the way load.py does, with one value corrected.

    Returns the UTC hour whose value was changed.
    """
    rows = [row for row in sample_rows(day) if row[0] == "MEASUREMENTS" and row[1] == day]
    corrected_hour = None
    for _, _, record in rows:
        if record["sensor_id"] == CORRECTED_SENSOR and record["data"]["period"]:
            record["data"]["value"] = CORRECTED_VALUE
            corrected_hour = record["data"]["period"]["datetimeFrom"]["utc"]
            break
    cursor.execute(
        "delete from AIR_QUALITY.RAW.MEASUREMENTS where partition_date = %s", (day.isoformat(),)
    )
    insert_rows(cursor, rows)
    return corrected_hour


def fact_rows(cursor) -> int:
    cursor.execute("select count(*) from AIR_QUALITY.CI_MARTS.fct_measurements_hourly")
    return cursor.fetchone()[0]


def run_dbt(*args: str) -> int:
    env = {
        **os.environ,
        "DBT_TARGET": "ci",
        "DBT_PROFILES_DIR": str(REPO / "dbt"),
        "EMULATOR_PORT": str(PORT),
    }
    print(f"\n=== dbt {' '.join(args)} ===", flush=True)
    dbt = str(Path(sys.executable).with_name("dbt"))
    return subprocess.run([dbt, *args], cwd=REPO / "dbt", env=env, check=False).returncode


def full_check(cursor, end: date) -> int:
    if run_dbt("source", "freshness") != 0:
        return 1
    if run_dbt("build", *DBT_VARS) != 0:
        return 1
    before = fact_rows(cursor)

    corrected_hour = reload_day(cursor, end)
    if run_dbt("build", *DBT_VARS) != 0:
        return 1
    after = fact_rows(cursor)

    cursor.execute(
        "select value from AIR_QUALITY.CI_MARTS.fct_measurements_hourly "
        "where sensor_id = %s and measured_at_utc = %s::timestamp_ntz",
        (CORRECTED_SENSOR, corrected_hour[:19]),
    )
    corrected = [row[0] for row in cursor.fetchall()]

    print(f"\nRe-loaded {end}: {before} fact rows before, {after} after.")
    if before != after:
        print("FAILED: re-loading a day changed the number of fact rows.")
        return 1
    if corrected != [CORRECTED_VALUE]:
        print(f"FAILED: expected the corrected value {CORRECTED_VALUE}, found {corrected}.")
        return 1
    print("Idempotency check passed: same rows, and the corrected value replaced the old one.")
    return 0


def main() -> int:
    import fakesnow
    import snowflake.connector

    apply_emulator_workarounds()
    dbt_args = sys.argv[1:]
    # Yesterday, so the "is the data recent" test sees fresh rows.
    end = datetime.now(UTC).date() - timedelta(days=1)

    with fakesnow.server(port=PORT) as connection_options:
        connection = snowflake.connector.connect(**connection_options)
        cursor = connection.cursor()
        counts = seed_raw(cursor, end)
        print(f"Emulator ready on port {PORT}, seeded {counts}", flush=True)
        try:
            if dbt_args:
                return run_dbt(*dbt_args, *DBT_VARS)
            return full_check(cursor, end)
        finally:
            connection.close()


if __name__ == "__main__":
    sys.exit(main())
