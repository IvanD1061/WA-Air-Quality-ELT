import boto3
import pytest
from moto import mock_aws

from pipeline.config import ConfigError, ExtractSettings, StorageSettings
from pipeline.storage import LocalStorage, S3Storage, get_storage


@mock_aws
def test_s3_upload_overwrites_the_same_key(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")
    s3 = boto3.client("s3", region_name="us-west-2")
    s3.create_bucket(Bucket="lake", CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
    storage = S3Storage("lake", client=s3)
    source = tmp_path / "file.jsonl.gz"

    source.write_bytes(b"first")
    uri = storage.upload(source, "raw/openaq/measurements/measurement_date=2026-10-06/m.jsonl.gz")
    source.write_bytes(b"second")
    storage.upload(source, "raw/openaq/measurements/measurement_date=2026-10-06/m.jsonl.gz")

    assert uri == "s3://lake/raw/openaq/measurements/measurement_date=2026-10-06/m.jsonl.gz"
    listing = s3.list_objects_v2(Bucket="lake")
    assert listing["KeyCount"] == 1  # a re-run replaces, it does not add
    body = s3.get_object(Bucket="lake", Key=listing["Contents"][0]["Key"])["Body"].read()
    assert body == b"second"


def test_local_storage_mirrors_the_key_layout(tmp_path):
    source = tmp_path / "in.gz"
    source.write_bytes(b"x")

    where = LocalStorage(tmp_path / "lake").upload(source, "a/b=1/c.gz")

    assert where.endswith("a/b=1/c.gz")
    assert (tmp_path / "lake" / "a" / "b=1" / "c.gz").read_bytes() == b"x"


def test_storage_choice_comes_from_the_environment(tmp_path, monkeypatch):
    monkeypatch.delenv("S3_BUCKET", raising=False)
    monkeypatch.delenv("RAW_LOCAL_DIR", raising=False)
    with pytest.raises(ConfigError, match="S3_BUCKET"):
        StorageSettings.from_env()

    monkeypatch.setenv("RAW_LOCAL_DIR", str(tmp_path))
    assert isinstance(get_storage(StorageSettings.from_env()), LocalStorage)


def test_extract_settings_parse_the_environment(monkeypatch):
    monkeypatch.delenv("OPENAQ_API_KEY", raising=False)
    with pytest.raises(ConfigError, match="OPENAQ_API_KEY"):
        ExtractSettings.from_env()

    monkeypatch.setenv("OPENAQ_API_KEY", "abc")
    monkeypatch.setenv("OPENAQ_PARAMETERS", "PM25, o3 ,")
    monkeypatch.setenv("OPENAQ_MAX_SENSORS", "10")
    monkeypatch.setenv("OPENAQ_MONITORS_ONLY", "true")
    settings = ExtractSettings.from_env()

    assert settings.parameters == frozenset({"pm25", "o3"})
    assert settings.max_sensors == 10
    assert settings.monitors_only is True
    assert settings.bbox.startswith("-124.85")
