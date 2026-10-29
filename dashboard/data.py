"""Everything the dashboard reads from Snowflake.

The dashboard signs in with the REPORTER role, which can only SELECT from the
MARTS schema. It reads the small daily table for charts, never raw data.

COST: results are cached for 15 minutes, so the warehouse only wakes when the
cache is cold and someone changes a filter. An open browser tab sitting idle
runs no queries.
"""

from __future__ import annotations

import os
import re
from datetime import date

import pandas as pd
import streamlit as st

CACHE_SECONDS = 15 * 60
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _identifier(name: str, default: str) -> str:
    value = os.environ.get(name) or default
    if not _IDENTIFIER.match(value):
        raise ValueError(f"{name}={value!r} is not a plain Snowflake identifier.")
    return value.upper()


def _marts() -> str:
    database = _identifier("SNOWFLAKE_DATABASE", "AIR_QUALITY")
    # Point DASHBOARD_SCHEMA at DEV_MARTS to preview models you are working on.
    schema = _identifier("DASHBOARD_SCHEMA", "MARTS")
    return f"{database}.{schema}"


def _connect():
    import snowflake.connector

    options = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "private_key_file": os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        "role": _identifier("DASHBOARD_ROLE", "REPORTER"),
        "warehouse": _identifier("SNOWFLAKE_WAREHOUSE", "ELT_WH"),
        "application": "wa_air_quality_dashboard",
    }
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    if passphrase:
        options["private_key_file_pwd"] = passphrase
    return snowflake.connector.connect(**options)


def _query(sql: str, params: tuple = ()) -> pd.DataFrame:
    connection = _connect()
    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        columns = [column[0].lower() for column in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)
    finally:
        connection.close()


def _numeric(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Snowflake NUMBER columns arrive as Decimal objects; charts want floats."""
    for column in columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


@st.cache_data(ttl=CACHE_SECONDS, show_spinner=False)
def load_parameters() -> pd.DataFrame:
    """Pollutants that have daily data, most data first."""
    marts = _marts()
    frame = _query(
        f"""
        select
            p.parameter_id,
            p.parameter_name,
            p.display_name,
            p.units,
            count(*) as station_days,
            min(d.date_day) as first_day,
            max(d.date_day) as last_day
        from {marts}.fct_air_quality_daily as f
        inner join {marts}.dim_parameter as p on f.parameter_key = p.parameter_key
        inner join {marts}.dim_date as d on f.date_key = d.date_key
        group by p.parameter_id, p.parameter_name, p.display_name, p.units
        order by station_days desc
        """
    )
    frame = _numeric(frame, ["parameter_id", "station_days"])
    frame["first_day"] = pd.to_datetime(frame["first_day"])
    frame["last_day"] = pd.to_datetime(frame["last_day"])
    return frame


@st.cache_data(ttl=CACHE_SECONDS, show_spinner=False)
def load_daily(parameter_id: int, since: date) -> pd.DataFrame:
    """One row per station per local day for one pollutant."""
    marts = _marts()
    frame = _query(
        f"""
        select
            d.date_day,
            l.location_id,
            l.location_name,
            l.locality,
            l.station_type,
            l.latitude,
            l.longitude,
            f.valid_hours,
            f.is_complete_day,
            f.avg_value,
            f.min_value,
            f.max_value,
            f.aqi_value,
            f.aqi_category
        from {marts}.fct_air_quality_daily as f
        inner join {marts}.dim_date as d on f.date_key = d.date_key
        inner join {marts}.dim_location as l on f.location_key = l.location_key
        inner join {marts}.dim_parameter as p on f.parameter_key = p.parameter_key
        where p.parameter_id = %s and d.date_day >= %s
        order by d.date_day, l.location_name
        """,
        (int(parameter_id), since.isoformat()),
    )
    frame = _numeric(
        frame,
        [
            "location_id",
            "latitude",
            "longitude",
            "valid_hours",
            "avg_value",
            "min_value",
            "max_value",
            "aqi_value",
        ],
    )
    frame["date_day"] = pd.to_datetime(frame["date_day"])
    frame["is_complete_day"] = frame["is_complete_day"].astype(bool)
    return frame


@st.cache_data(ttl=CACHE_SECONDS, show_spinner=False)
def load_quality(parameter_id: int, since: date) -> pd.DataFrame:
    """How many hourly readings landed in each quality status, per station type."""
    marts = _marts()
    frame = _query(
        f"""
        select
            f.quality_status,
            l.station_type,
            count(*) as readings
        from {marts}.fct_measurements_hourly as f
        inner join {marts}.dim_parameter as p on f.parameter_key = p.parameter_key
        inner join {marts}.dim_date as d on f.date_key = d.date_key
        inner join {marts}.dim_location as l on f.location_key = l.location_key
        where p.parameter_id = %s and d.date_day >= %s
        group by f.quality_status, l.station_type
        """,
        (int(parameter_id), since.isoformat()),
    )
    return _numeric(frame, ["readings"])
