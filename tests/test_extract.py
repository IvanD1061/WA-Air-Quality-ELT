import gzip
import json
from datetime import UTC, date, datetime

import pytest

from pipeline import extract, paths
from pipeline.config import ExtractSettings, StorageSettings
from pipeline.openaq_client import AuthError, NotFoundError, OpenAQError
from pipeline.storage import LocalStorage
from tests import sample_data

START = date(2026, 10, 5)
END = date(2026, 10, 6)
SNAPSHOT = date(2026, 10, 7)
NOW = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


class FakeClient:
    """Stands in for OpenAQClient so no test touches the network."""

    def __init__(self, locations, errors=None):
        self.locations = locations
        self.errors = errors or {}
        self.hour_requests = []
        self.request_count = 0

    def iter_locations(self, **kwargs):
        self.request_count += 1
        return iter(self.locations)

    def iter_sensor_hours(self, sensor_id, *, datetime_from, datetime_to):
        self.request_count += 1
        self.hour_requests.append((sensor_id, datetime_from, datetime_to))
        if sensor_id in self.errors:
            raise self.errors[sensor_id]
        parameter = "o3" if sensor_id == 1002 else "pm25"
        # An hour either side of the window too, as a loose API filter might return.
        before = sample_data.make_hour(parameter, datetime(2026, 10, 4, 23, tzinfo=UTC), 1.0)
        after = sample_data.make_hour(parameter, datetime(2026, 10, 7, 0, tzinfo=UTC), 1.0)
        inside = sample_data.sample_hours(sensor_id, parameter, START, END)
        return iter([before, *inside, after])


def read_jsonl(path):
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle]


@pytest.fixture
def run(tmp_path):
    def _run(client=None, **overrides):
        client = client or FakeClient(sample_data.sample_locations(END))
        settings = ExtractSettings(api_key="k", **overrides)
        summary = extract.run_extract(
            start=START,
            end=END,
            snapshot_date=SNAPSHOT,
            settings=settings,
            storage_settings=StorageSettings(bucket=None, local_dir=str(tmp_path)),
            client=client,
            storage=LocalStorage(tmp_path),
            now=NOW,
        )
        return summary, client

    return _run


def partition_file(tmp_path, dataset, day):
    return tmp_path / paths.object_key("raw/openaq", dataset, day)


def test_writes_one_file_per_day_plus_a_location_snapshot(run, tmp_path):
    summary, _ = run()

    assert partition_file(tmp_path, paths.LOCATIONS, SNAPSHOT).exists()
    assert partition_file(tmp_path, paths.MEASUREMENTS, START).exists()
    assert partition_file(tmp_path, paths.MEASUREMENTS, END).exists()
    assert len(summary["files_uploaded"]) == 3
    assert summary["days_without_records"] == []


def test_only_recently_active_sensors_with_tracked_parameters_are_queried(run):
    _, client = run()

    queried = [sensor_id for sensor_id, _, _ in client.hour_requests]
    # 3001 belongs to a dead station; 2002 measures temperature, not a pollutant.
    assert queried == [1001, 1002, 2001]


def test_one_request_per_sensor_covers_the_whole_window(run):
    _, client = run()

    assert client.hour_requests[0][1:] == ("2026-10-05T00:00:00Z", "2026-10-07T01:00:00Z")


def test_records_are_filed_by_the_utc_day_their_hour_starts_in(run, tmp_path):
    summary, _ = run()

    for day in (START, END):
        records = read_jsonl(partition_file(tmp_path, paths.MEASUREMENTS, day))
        days_seen = {
            r["data"]["period"]["datetimeFrom"]["utc"][:10] for r in records if r["data"]["period"]
        }
        assert days_seen == {day.isoformat()}

    # The fake API returned one extra hour on each side for each of 3 sensors.
    assert summary["records_outside_window"] == 3 * 2


def test_payload_is_stored_untouched_inside_an_envelope(run, tmp_path):
    run()

    record = read_jsonl(partition_file(tmp_path, paths.MEASUREMENTS, START))[0]
    assert set(record) == {"sensor_id", "location_id", "extracted_at", "request_path", "data"}
    assert record["sensor_id"] == 1001
    assert record["location_id"] == 100
    assert record["extracted_at"] == "2026-10-07T12:00:00Z"
    assert record["request_path"] == "/sensors/1001/hours"
    expected = sample_data.sample_hours(1001, "pm25", START, START)[0]
    assert record["data"] == expected
    assert record["data"]["parameter"]["units"] == "µg/m³"  # unicode survives the round trip


def test_bad_values_are_landed_as_is_not_cleaned(run, tmp_path):
    run()

    values = [
        r["data"]["value"]
        for r in read_jsonl(partition_file(tmp_path, paths.MEASUREMENTS, START))
        if r["sensor_id"] == 1001
    ]
    assert None in values
    assert -999.0 in values
    assert len(values) == 25  # 24 hours plus the duplicate the API sent


def test_records_without_a_timestamp_are_kept_and_counted(run, tmp_path):
    summary, _ = run()

    assert summary["records_without_timestamp"] == 1
    records = read_jsonl(partition_file(tmp_path, paths.MEASUREMENTS, START))
    assert sum(1 for r in records if r["data"]["period"] is None) == 1


def test_location_snapshot_keeps_every_location_even_dead_ones(run, tmp_path):
    summary, _ = run()

    records = read_jsonl(partition_file(tmp_path, paths.LOCATIONS, SNAPSHOT))
    assert [r["data"]["id"] for r in records] == [100, 200, 300]
    assert summary["locations"] == 3


def test_summary_counts(run):
    summary, client = run()

    assert summary["sensors_queried"] == 3
    assert summary["sensors_failed"] == 0
    assert summary["api_requests"] == client.request_count == 4
    # 3 sensors x 48 hours, plus the duplicate hour and the record with no
    # timestamp. Duplicates are landed as received; dbt removes them.
    assert summary["records"] == 3 * 48 + 2
    assert summary["records"] == sum(summary["records_by_day"].values())


def test_a_few_failed_sensors_are_tolerated_and_reported(run, tmp_path):
    client = FakeClient(
        sample_data.sample_locations(END), errors={1002: NotFoundError("404 from /sensors/1002")}
    )
    summary, _ = run(client=client, max_failed_sensor_pct=50.0)

    assert summary["sensors_failed"] == 1
    assert summary["failed_sensors"][0]["sensor_id"] == 1002
    assert partition_file(tmp_path, paths.MEASUREMENTS, START).exists()


def test_too_many_failures_abort_before_anything_is_uploaded(run, tmp_path):
    client = FakeClient(
        sample_data.sample_locations(END),
        errors={1001: OpenAQError("boom"), 1002: OpenAQError("boom")},
    )

    with pytest.raises(extract.ExtractError, match="2 of 3 sensors failed"):
        run(client=client)
    assert list(tmp_path.rglob("*.jsonl.gz")) == []


def test_a_bad_api_key_stops_the_run_immediately(run):
    client = FakeClient(sample_data.sample_locations(END), errors={1001: AuthError("401")})

    with pytest.raises(AuthError):
        run(client=client)
    assert len(client.hour_requests) == 1


def test_no_locations_is_an_error(run):
    with pytest.raises(extract.ExtractError, match="no locations"):
        run(client=FakeClient([]))


def test_max_sensors_caps_deterministically(run):
    _, client = run(max_sensors=2)

    assert [sensor_id for sensor_id, _, _ in client.hour_requests] == [1001, 1002]


def test_parse_utc_handles_junk():
    assert extract.parse_utc("2026-10-06T07:00:00Z") == datetime(2026, 10, 6, 7, tzinfo=UTC)
    assert extract.parse_utc("2026-10-06T00:00:00-07:00") == datetime(2026, 10, 6, 7, tzinfo=UTC)
    assert extract.parse_utc("not a date") is None
    assert extract.parse_utc(None) is None


def test_cli_defaults_to_a_single_day():
    args = extract.parse_args(["--end", "2026-10-06"])
    assert (args.start, args.end) == (END, END)

    with pytest.raises(SystemExit):
        extract.parse_args(["--start", "2026-10-07", "--end", "2026-10-06"])
