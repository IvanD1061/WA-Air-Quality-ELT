"""Print the real numbers behind the README and the resume bullets.

    make stats

Everything here is measured from your own warehouse and project, so the
figures you quote in an interview are ones you can reproduce on the spot.

COST: runs a handful of small queries (seconds of X-Small warehouse time).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from pipeline.config import SnowflakeSettings  # noqa: E402


def one_row(cursor, sql: str) -> tuple:
    cursor.execute(sql)
    return cursor.fetchone()


def warehouse_stats(cursor, database: str, raw_schema: str = "RAW", marts_schema: str = "MARTS"):
    raw = f"{database}.{raw_schema}"
    marts = f"{database}.{marts_schema}"
    stats: dict[str, object] = {}

    total, days, first_day, last_day = one_row(
        cursor,
        f"select count(*), count(distinct partition_date), min(partition_date), "
        f"max(partition_date) from {raw}.MEASUREMENTS",
    )
    stats["raw_measurement_rows"] = total
    stats["days_loaded"] = days
    stats["first_day"] = str(first_day)
    stats["last_day"] = str(last_day)

    average, smallest, largest = one_row(
        cursor,
        f"select round(avg(n)), min(n), max(n) from "
        f"(select partition_date, count(*) as n from {raw}.MEASUREMENTS group by partition_date)",
    )
    stats["raw_rows_per_day_avg"] = int(average or 0)
    stats["raw_rows_per_day_min"] = smallest
    stats["raw_rows_per_day_max"] = largest

    facts, valid, sensors, locations, parameters = one_row(
        cursor,
        f"select count(*), sum(case when is_valid then 1 else 0 end), count(distinct sensor_id), "
        f"count(distinct location_key), count(distinct parameter_key) "
        f"from {marts}.fct_measurements_hourly",
    )
    stats["fact_rows"] = facts
    stats["valid_fact_rows"] = valid
    stats["valid_pct"] = round(100.0 * valid / facts, 1) if facts else 0.0
    stats["sensors"] = sensors
    stats["locations"] = locations
    stats["parameters"] = parameters

    cursor.execute(
        f"select quality_status, count(*) from {marts}.fct_measurements_hourly "
        f"group by quality_status order by count(*) desc"
    )
    stats["quality_breakdown"] = {status: count for status, count in cursor.fetchall()}

    (stats["daily_rows"],) = one_row(cursor, f"select count(*) from {marts}.fct_air_quality_daily")
    (stats["catalogued_locations"],) = one_row(cursor, f"select count(*) from {marts}.dim_location")
    return stats


def dbt_counts() -> dict[str, int]:
    """Count models and tests by asking dbt to list them (no warehouse needed)."""
    dbt = str(Path(sys.executable).with_name("dbt"))
    counts = {}
    for resource in ("model", "seed", "test", "unit_test"):
        result = subprocess.run(
            [dbt, "ls", "--resource-type", resource, "--quiet"],
            cwd=REPO / "dbt",
            capture_output=True,
            text=True,
            check=False,
        )
        counts[resource] = len([line for line in result.stdout.splitlines() if line.strip()])
    return counts


def render(stats: dict, counts: dict[str, int]) -> str:
    tests = counts["test"] + counts["unit_test"]
    per_day = f"min {stats['raw_rows_per_day_min']:,}, max {stats['raw_rows_per_day_max']:,}"
    rows = [
        ("Raw measurement rows", f"{stats['raw_measurement_rows']:,}", ""),
        ("Rows per day (average)", f"{stats['raw_rows_per_day_avg']:,}", per_day),
        ("Fact rows (sensor-hours)", f"{stats['fact_rows']:,}", ""),
        ("Passing quality checks", f"{stats['valid_pct']}%", ""),
        (
            "Locations reporting",
            f"{stats['locations']:,}",
            f"of {stats['catalogued_locations']:,} in the catalogue",
        ),
        ("Sensors reporting", f"{stats['sensors']:,}", ""),
        ("Pollutants", f"{stats['parameters']:,}", ""),
        ("Daily summary rows", f"{stats['daily_rows']:,}", ""),
        ("", "", ""),
        ("dbt models", str(counts["model"]), ""),
        ("dbt seeds", str(counts["seed"]), ""),
        (
            "dbt tests",
            str(tests),
            f"{counts['test']} data tests, {counts['unit_test']} unit tests",
        ),
    ]
    lines = [
        f"Pipeline numbers: {stats['first_day']} to {stats['last_day']}, "
        f"{stats['days_loaded']} days loaded",
        "",
    ]
    for label, value, note in rows:
        lines.append(f"  {label:<28}{value:>12}   {f'({note})' if note else ''}".rstrip())
    lines += ["", "  Readings by quality status"]
    for status, count in stats["quality_breakdown"].items():
        lines.append(f"    {status:<26}{count:>12,}")
    lines += [
        "",
        "Run time is not measured here. Read it from the Airflow UI (the Duration",
        "column of the DAG's runs) or with `make runs`, and quote the median of",
        "several scheduled runs rather than your single best one.",
    ]
    return "\n".join(lines)


def main() -> int:
    from pipeline.snowflake_conn import connect

    settings = SnowflakeSettings.from_env()
    connection = connect(settings)
    try:
        stats = warehouse_stats(connection.cursor(), settings.database, settings.raw_schema)
    finally:
        connection.close()
    print(render(stats, dbt_counts()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
