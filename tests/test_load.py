from datetime import date

import pytest

from pipeline import load, paths
from pipeline.config import ConfigError, SnowflakeSettings, identifier

SETTINGS = SnowflakeSettings(account="acct", user="svc", private_key_path="/dev/null")
DAY = date(2026, 10, 6)


class FakeCursor:
    """Records every statement and plays back canned COPY results."""

    def __init__(self, rows_by_folder=None, fail_on_copy=False):
        self.statements = []
        self.rows_by_folder = rows_by_folder or {}
        self.fail_on_copy = fail_on_copy
        self.description = None
        self._rows = []

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        if sql.startswith("copy into"):
            if self.fail_on_copy:
                raise RuntimeError("stage not found")
            loaded = next((n for folder, n in self.rows_by_folder.items() if folder in sql), 0)
            if loaded:
                self.description = [("file",), ("status",), ("rows_parsed",), ("rows_loaded",)]
                self._rows = [("f.jsonl.gz", "LOADED", loaded, loaded)]
            else:
                self.description = [("status",)]
                self._rows = [("Copy executed with 0 files processed.",)]
        return self

    def fetchall(self):
        return self._rows

    @property
    def verbs(self):
        return [sql.split()[0] for sql, _ in self.statements]


def test_copy_sql_targets_one_day_folder_and_keeps_lineage_columns():
    sql = load.build_copy_sql(SETTINGS, paths.MEASUREMENTS, DAY)

    assert sql.startswith("copy into AIR_QUALITY.RAW.MEASUREMENTS")
    assert "'@AIR_QUALITY.RAW.OPENAQ_STAGE/measurements/measurement_date=2026-10-06/'" in sql
    assert "metadata$filename" in sql
    assert "metadata$file_row_number" in sql
    assert "'[0-9]{4}-[0-9]{2}-[0-9]{2}'" in sql  # braces survived the f-string
    assert "force = true" in sql


def test_reload_replaces_the_day_inside_one_transaction():
    cursor = FakeCursor({"measurement_date=2026-10-06": 240})

    loaded = load.reload_partition(cursor, SETTINGS, paths.MEASUREMENTS, DAY)

    assert loaded == 240
    assert cursor.verbs == ["begin", "delete", "copy", "commit"]
    delete_sql, params = cursor.statements[1]
    assert delete_sql == "delete from AIR_QUALITY.RAW.MEASUREMENTS where partition_date = %s"
    assert params == ("2026-10-06",)


def test_reload_rolls_back_when_there_is_no_file_so_old_rows_survive():
    cursor = FakeCursor()

    assert load.reload_partition(cursor, SETTINGS, paths.MEASUREMENTS, DAY) == 0
    assert cursor.verbs == ["begin", "delete", "copy", "rollback"]


def test_reload_rolls_back_when_copy_fails():
    cursor = FakeCursor(fail_on_copy=True)

    with pytest.raises(RuntimeError, match="stage not found"):
        load.reload_partition(cursor, SETTINGS, paths.MEASUREMENTS, DAY)
    assert cursor.verbs[-1] == "rollback"
    assert "commit" not in cursor.verbs


def test_run_load_loads_the_snapshot_then_each_day():
    cursor = FakeCursor(
        {
            "snapshot_date=2026-10-07": 3,
            "measurement_date=2026-10-05": 100,
            "measurement_date=2026-10-06": 120,
        }
    )

    summary = load.run_load(
        cursor=cursor,
        settings=SETTINGS,
        start=date(2026, 10, 5),
        end=DAY,
        snapshot_date=date(2026, 10, 7),
    )

    assert summary["location_rows"] == 3
    assert summary["measurement_rows"] == 220
    assert summary["measurement_rows_by_day"] == {"2026-10-05": 100, "2026-10-06": 120}
    assert summary["days_without_files"] == []


def test_run_load_reports_days_with_no_file_but_carries_on():
    cursor = FakeCursor({"snapshot_date=2026-10-07": 3, "measurement_date=2026-10-06": 120})

    summary = load.run_load(
        cursor=cursor,
        settings=SETTINGS,
        start=date(2026, 10, 5),
        end=DAY,
        snapshot_date=date(2026, 10, 7),
    )

    assert summary["days_without_files"] == ["2026-10-05"]


def test_run_load_fails_loudly_when_nothing_was_loaded():
    with pytest.raises(load.LoadError, match="No location snapshot"):
        load.run_load(cursor=FakeCursor(), settings=SETTINGS, start=DAY, end=DAY, snapshot_date=DAY)

    with pytest.raises(load.LoadError, match="No measurement files"):
        load.run_load(
            cursor=FakeCursor({"snapshot_date=2026-10-06": 3}),
            settings=SETTINGS,
            start=DAY,
            end=DAY,
            snapshot_date=DAY,
        )


def test_identifiers_are_validated_before_they_reach_sql():
    assert identifier("air_quality", "X") == "AIR_QUALITY"
    with pytest.raises(ConfigError):
        identifier("raw; drop table x", "X")


def test_paths_are_hive_style_and_deterministic():
    assert (
        paths.object_key("raw/openaq/", paths.MEASUREMENTS, DAY)
        == "raw/openaq/measurements/measurement_date=2026-10-06/measurements.jsonl.gz"
    )
    assert paths.partition_folder(paths.LOCATIONS, DAY) == "locations/snapshot_date=2026-10-06/"
    assert paths.date_range(date(2026, 10, 5), DAY) == [date(2026, 10, 5), DAY]
    with pytest.raises(ValueError):
        paths.date_range(DAY, date(2026, 10, 5))
