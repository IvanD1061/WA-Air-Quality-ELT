"""Washington air quality dashboard.

    make up    ->  http://localhost:8501

Reads the star schema that dbt builds. Nothing is computed here that belongs
in the warehouse: the page filters and summarises a few thousand daily rows
and draws them.
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pydeck as pdk
import streamlit as st

import charts
import data

AQI_ORDER = [
    "Good",
    "Moderate",
    "Unhealthy for Sensitive Groups",
    "Unhealthy",
    "Very Unhealthy",
    "Hazardous",
]
QUALITY_LABELS = {
    "valid": "Passed every check",
    "missing_value": "No value reported",
    "below_plausible_range": "Below plausible range",
    "above_plausible_range": "Above plausible range",
    "provider_flagged": "Flagged by the data provider",
    "low_coverage": "Too few readings in the hour",
}
PERIODS = {"Last 7 days": 7, "Last 30 days": 30, "Last 90 days": 90, "All data": None}
PM25_ID = 2
# PM2.5 map colors always span the same range, so a color means the same
# concentration on every day. 55.5 µg/m³ is where the "Unhealthy" band starts.
PM25_MAP_RANGE = (0.0, 55.5)

st.set_page_config(page_title="Washington air quality", page_icon=None, layout="wide")


def current_theme() -> dict:
    theme = getattr(st.context, "theme", None)
    mode = getattr(theme, "type", None) or "light"
    return charts.THEMES.get(mode, charts.THEMES["light"])


def number_format(scale: float) -> str:
    """A format that suits the pollutant: 12.3 for PM2.5, 0.031 for ozone in ppm."""
    if scale >= 100:
        return ",.0f"
    if scale >= 1:
        return ",.1f"
    return ",.3f"


def table_view(frame: pd.DataFrame, columns: dict[str, str]) -> None:
    """The same numbers as the chart above, for anyone who cannot or would rather not hover."""
    with st.expander("View as table"):
        st.dataframe(frame[list(columns)].rename(columns=columns), hide_index=True, width="stretch")


def legend_scale(theme: dict, low: float, high: float, units: str, capped: bool, fmt: str) -> None:
    stops = ", ".join(theme["ramp"])
    top = f"{high:{fmt}}{'+' if capped else ''}"
    row = f"display:flex;align-items:center;gap:8px;font-size:12px;color:{theme['ink_secondary']}"
    gradient = f"linear-gradient(to right, {stops})"
    bar = f"flex:0 0 180px;height:10px;border-radius:5px;background:{gradient}"
    st.markdown(
        f'<div style="{row}"><span>{low:{fmt}}</span><span style="{bar}"></span>'
        f"<span>{top} {units}</span></div>",
        unsafe_allow_html=True,
    )


theme = current_theme()

st.title("Washington air quality")
st.caption(
    "Daily summaries from OpenAQ monitoring stations, built by a daily ELT pipeline "
    "(S3, Snowflake, dbt, Airflow)."
)

# --- Load the list of pollutants (also proves the connection works) -----------
try:
    parameters = data.load_parameters()
except Exception as exc:
    st.error(
        "Could not read from Snowflake. Check the SNOWFLAKE_ settings in .env, that the "
        "private key is in secrets/, and that the pipeline has run at least once."
    )
    st.exception(exc)
    st.stop()

if parameters.empty:
    st.info("No data yet. Run the pipeline once (`make backfill`), then reload this page.")
    st.stop()

# --- Filters: one row, above everything they affect ---------------------------
filter_pollutant, filter_period, filter_station = st.columns([2, 3, 2])

parameter_ids = parameters["parameter_id"].astype(int).tolist()
with filter_pollutant:
    parameter_id = st.selectbox(
        "Pollutant",
        parameter_ids,
        index=parameter_ids.index(PM25_ID) if PM25_ID in parameter_ids else 0,
        format_func=lambda pid: "{display_name} ({units})".format(
            **parameters.set_index("parameter_id").loc[pid]
        ),
    )
with filter_period:
    period = st.radio("Period", list(PERIODS), index=1, horizontal=True)
with filter_station:
    station_type = st.selectbox(
        "Station type", ["All stations", "Reference monitor", "Low-cost sensor"]
    )

parameter = parameters.set_index("parameter_id").loc[parameter_id]
units = parameter["units"]
name = parameter["display_name"]

# Periods count back from the newest day in the warehouse, not from today, so
# the page still shows something sensible if the pipeline has been paused.
last_day: date = parameter["last_day"].date()
since = (
    date(1900, 1, 1) if PERIODS[period] is None else last_day - timedelta(days=PERIODS[period] - 1)
)

daily = data.load_daily(parameter_id, since)
if station_type != "All stations":
    daily = daily[daily["station_type"] == station_type]
if daily.empty:
    st.info("No data for this combination of filters.")
    st.stop()

# A day counts toward averages only when a station has enough valid hours;
# otherwise a station that reported for one smoky hour would skew the picture.
complete = daily[daily["is_complete_day"]]
basis = complete if not complete.empty else daily
if complete.empty:
    st.warning("No station has a complete day in this period yet, so partial days are shown.")

trend = (
    basis.groupby("date_day")
    .agg(value=("avg_value", "mean"), stations=("location_id", "nunique"))
    .reset_index()
    .sort_values("date_day")
)
latest_day = trend["date_day"].max()
latest = basis[basis["date_day"] == latest_day].sort_values("avg_value", ascending=False)
quality = data.load_quality(parameter_id, since)
if station_type != "All stations":
    quality = quality[quality["station_type"] == station_type]

# --- Headline numbers --------------------------------------------------------
tile_latest, tile_stations, tile_days, tile_quality = st.columns(4)

fmt = number_format(float(trend["value"].abs().max()))
latest_value = trend["value"].iloc[-1]
previous_value = trend["value"].iloc[-2] if len(trend) > 1 else None
tile_latest.metric(
    f"Average {name}, {latest_day:%b %-d}",
    f"{latest_value:{fmt}} {units}",
    delta=None
    if previous_value is None
    else f"{latest_value - previous_value:+{fmt}} vs the day before",
    # For a pollutant, up is worse.
    delta_color="inverse",
)
tile_stations.metric("Stations reporting", f"{daily['location_id'].nunique():,}")
tile_days.metric("Days covered", f"{daily['date_day'].nunique():,}")
total_readings = quality["readings"].sum()
valid_readings = quality.loc[quality["quality_status"] == "valid", "readings"].sum()
tile_quality.metric(
    "Readings passing quality checks",
    f"{100 * valid_readings / total_readings:.1f}%" if total_readings else "n/a",
)

# --- Trend and categories ----------------------------------------------------
column_trend, column_categories = st.columns([3, 2])

with column_trend:
    st.subheader(f"Daily average {name}")
    st.caption(f"Mean of the stations' daily averages, in {units}. Complete days only.")
    st.altair_chart(
        charts.trend_chart(trend, theme, f"{name} ({units})", fmt), width="stretch", theme=None
    )
    table_view(
        trend.assign(date_day=trend["date_day"].dt.date, value=trend["value"].round(3)),
        {"date_day": "Day", "value": f"Average ({units})", "stations": "Stations"},
    )

with column_categories:
    if parameter_id == PM25_ID and basis["aqi_category"].notna().any():
        st.subheader("Days by AQI category")
        st.caption("Station-days in each US EPA category, from daily PM2.5 averages.")
        counts = basis["aqi_category"].value_counts()
        categories = pd.DataFrame(
            {"label": AQI_ORDER, "count": [int(counts.get(c, 0)) for c in AQI_ORDER]}
        )
        st.altair_chart(
            charts.count_bars(categories, theme, "Station-days"), width="stretch", theme=None
        )
        table_view(categories, {"label": "Category", "count": "Station-days"})
    else:
        st.subheader(f"Highest daily averages, {latest_day:%b %-d}")
        st.caption(f"Stations ranked by their {name} average for the day, in {units}.")
        st.dataframe(
            latest[["location_name", "station_type", "avg_value"]]
            .head(10)
            .rename(
                columns={
                    "location_name": "Station",
                    "station_type": "Type",
                    "avg_value": f"Average ({units})",
                }
            )
            .round(3),
            hide_index=True,
            width="stretch",
        )

# --- Map ---------------------------------------------------------------------
st.subheader(f"Stations on {latest_day:%B %-d, %Y}")
st.caption(f"Each dot is a station, shaded by its {name} average for the day.")

stations = latest.dropna(subset=["latitude", "longitude"]).copy()
if stations.empty:
    st.info("No station on this day has coordinates.")
else:
    if parameter_id == PM25_ID:
        low, high = PM25_MAP_RANGE
    else:
        low, high = 0.0, float(stations["avg_value"].max())
    capped = bool((stations["avg_value"] > high).any())
    stations["color"] = stations["avg_value"].apply(
        lambda value: charts.ramp_color(value, low, high, theme["ramp"])
    )
    stations["value_label"] = stations["avg_value"].map(lambda value: f"{value:{fmt}} {units}")
    stations["category_label"] = stations["aqi_category"].fillna("")

    legend_scale(theme, low, high, units, capped, fmt)
    st.pydeck_chart(
        pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(
                latitude=float(stations["latitude"].mean()),
                longitude=float(stations["longitude"].mean()),
                zoom=5.6,
            ),
            layers=[
                pdk.Layer(
                    "ScatterplotLayer",
                    data=stations[
                        [
                            "location_name",
                            "station_type",
                            "latitude",
                            "longitude",
                            "color",
                            "value_label",
                            "category_label",
                        ]
                    ],
                    get_position="[longitude, latitude]",
                    get_fill_color="color",
                    # A ring in the background color keeps overlapping dots apart.
                    stroked=True,
                    get_line_color=list(charts._hex_to_rgb(theme["surface"])),
                    line_width_min_pixels=2,
                    # A fixed 7px dot at every zoom level: a 1 metre radius,
                    # clamped to exactly 7 pixels on screen.
                    get_radius=1,
                    radius_min_pixels=7,
                    radius_max_pixels=7,
                    pickable=True,
                )
            ],
            tooltip={"text": "{location_name}\n{station_type}\n{value_label}  {category_label}"},
        ),
        width="stretch",
    )
    table_view(
        stations.assign(avg_value=stations["avg_value"].round(3)),
        {
            "location_name": "Station",
            "locality": "Locality",
            "station_type": "Type",
            "avg_value": f"Average ({units})",
            "aqi_category": "AQI category",
            "valid_hours": "Valid hours",
        },
    )

# --- Data quality ------------------------------------------------------------
st.subheader("Data quality")
st.caption(
    "Every hourly reading is kept and labelled. Only readings that pass every check feed the "
    "averages above."
)
quality_counts = quality.groupby("quality_status")["readings"].sum()
quality_frame = pd.DataFrame(
    {
        "label": list(QUALITY_LABELS.values()),
        "count": [int(quality_counts.get(status, 0)) for status in QUALITY_LABELS],
    }
)
st.altair_chart(
    charts.count_bars(quality_frame, theme, "Hourly readings"), width="stretch", theme=None
)
table_view(quality_frame, {"label": "Check result", "count": "Hourly readings"})

st.caption(
    "Source: OpenAQ (openaq.org). AQI values here are computed from daily means for comparison "
    "and are not official AirNow figures."
)
