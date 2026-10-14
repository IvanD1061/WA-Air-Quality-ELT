"""Runtime settings.

Everything is read from environment variables so the same code runs unchanged
on a laptop, inside the Airflow container, and in CI. Nothing secret is ever
hard-coded or committed; see `.env.example` for the full list.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

# Rough bounding box around Washington State: minLon,minLat,maxLon,maxLat.
# A box is not a state border, so a handful of stations just across the
# Oregon and Idaho lines come along too. `iso=US` keeps Canada out.
DEFAULT_BBOX = "-124.85,45.54,-116.91,49.01"
DEFAULT_PARAMETERS = "pm25,pm10,o3,no2,so2,co"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


class ConfigError(RuntimeError):
    """A required setting is missing or malformed."""


def _get(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def _require(name: str) -> str:
    value = _get(name)
    if value is None:
        raise ConfigError(f"Environment variable {name} is required but not set. See .env.example.")
    return value


def _bool(name: str, default: bool) -> bool:
    value = _get(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "y", "on"}


def _optional_int(name: str) -> int | None:
    value = _get(name)
    return int(value) if value is not None else None


def identifier(value: str, name: str) -> str:
    """Validate a Snowflake identifier that will be formatted into SQL.

    Identifiers cannot be bound as query parameters, so the only safe way to
    build SQL with them is to allow nothing but plain names.
    """
    if not _IDENTIFIER.match(value):
        raise ConfigError(f"{name}={value!r} is not a plain Snowflake identifier.")
    return value.upper()


@dataclass(frozen=True)
class ExtractSettings:
    api_key: str
    base_url: str = "https://api.openaq.org/v3"
    bbox: str = DEFAULT_BBOX
    iso: str | None = "US"
    parameters: frozenset[str] = frozenset(DEFAULT_PARAMETERS.split(","))
    # Query sensors whose location reported data within this many days before
    # the extract window. Most locations in the catalogue are long dead, and
    # asking each one for data would burn the rate limit for nothing.
    active_within_days: int = 7
    monitors_only: bool = False
    max_sensors: int | None = None
    # A few sensors failing should not sink the whole run, but many failing
    # means something is wrong and the run should stop before it uploads.
    max_failed_sensor_pct: float = 5.0

    @classmethod
    def from_env(cls) -> ExtractSettings:
        parameters = _get("OPENAQ_PARAMETERS", DEFAULT_PARAMETERS)
        return cls(
            api_key=_require("OPENAQ_API_KEY"),
            base_url=_get("OPENAQ_BASE_URL", cls.base_url).rstrip("/"),
            bbox=_get("OPENAQ_BBOX", DEFAULT_BBOX),
            iso=_get("OPENAQ_ISO", "US"),
            parameters=frozenset(p.strip().lower() for p in parameters.split(",") if p.strip()),
            active_within_days=int(_get("OPENAQ_ACTIVE_WITHIN_DAYS", "7")),
            monitors_only=_bool("OPENAQ_MONITORS_ONLY", False),
            max_sensors=_optional_int("OPENAQ_MAX_SENSORS"),
            max_failed_sensor_pct=float(_get("OPENAQ_MAX_FAILED_SENSOR_PCT", "5")),
        )


@dataclass(frozen=True)
class StorageSettings:
    bucket: str | None
    prefix: str = "raw/openaq"
    # When set, files are written to this local folder instead of S3. Used by
    # the tests and for a dry run before any cloud account exists.
    local_dir: str | None = None

    @classmethod
    def from_env(cls) -> StorageSettings:
        local_dir = _get("RAW_LOCAL_DIR")
        bucket = _get("S3_BUCKET")
        if local_dir is None and bucket is None:
            raise ConfigError("Set S3_BUCKET (or RAW_LOCAL_DIR for a local dry run).")
        return cls(
            bucket=bucket,
            prefix=_get("S3_PREFIX", "raw/openaq").strip("/"),
            local_dir=local_dir,
        )


@dataclass(frozen=True)
class SnowflakeSettings:
    account: str
    user: str
    private_key_path: str
    private_key_passphrase: str | None = None
    role: str = "ELT_ROLE"
    warehouse: str = "ELT_WH"
    database: str = "AIR_QUALITY"
    raw_schema: str = "RAW"
    stage: str = "OPENAQ_STAGE"
    file_format: str = "NDJSON_GZ"

    @classmethod
    def from_env(cls) -> SnowflakeSettings:
        return cls(
            account=_require("SNOWFLAKE_ACCOUNT"),
            user=_require("SNOWFLAKE_USER"),
            private_key_path=_require("SNOWFLAKE_PRIVATE_KEY_PATH"),
            private_key_passphrase=_get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
            role=identifier(_get("SNOWFLAKE_ROLE", "ELT_ROLE"), "SNOWFLAKE_ROLE"),
            warehouse=identifier(_get("SNOWFLAKE_WAREHOUSE", "ELT_WH"), "SNOWFLAKE_WAREHOUSE"),
            database=identifier(_get("SNOWFLAKE_DATABASE", "AIR_QUALITY"), "SNOWFLAKE_DATABASE"),
            raw_schema=identifier(_get("SNOWFLAKE_RAW_SCHEMA", "RAW"), "SNOWFLAKE_RAW_SCHEMA"),
            stage=identifier(_get("SNOWFLAKE_STAGE", "OPENAQ_STAGE"), "SNOWFLAKE_STAGE"),
            file_format=identifier(
                _get("SNOWFLAKE_FILE_FORMAT", "NDJSON_GZ"), "SNOWFLAKE_FILE_FORMAT"
            ),
        )
