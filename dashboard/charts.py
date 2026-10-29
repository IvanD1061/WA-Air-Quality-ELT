"""Chart builders and the colors they use.

Color rules followed here:
  - Every chart shows one series, so every mark is the same blue. Identity
    comes from titles and axis labels, never from color alone.
  - The map colors stations by concentration with a single-hue ramp (more is
    darker in light mode, lighter in dark mode), next to a labelled scale.
  - Text is always neutral ink, never the data color.
  - Every chart has a table underneath in an expander, so no value is only
    reachable by hovering.
"""

from __future__ import annotations

import altair as alt
import pandas as pd

FONT = "system-ui, -apple-system, 'Segoe UI', sans-serif"

# One blue hue, light to dark.
_BLUE_RAMP = [
    "#cde2fb",
    "#b7d3f6",
    "#9ec5f4",
    "#86b6ef",
    "#6da7ec",
    "#5598e7",
    "#3987e5",
    "#2a78d6",
    "#256abf",
    "#1c5cab",
    "#184f95",
    "#104281",
    "#0d366b",
]

THEMES = {
    "light": {
        "series": "#2a78d6",
        "surface": "#ffffff",
        "ink": "#0b0b0b",
        "ink_secondary": "#52514e",
        "muted": "#898781",
        "grid": "#e1e0d9",
        "axis": "#c3c2b7",
        # Low values start a few steps in so they stay visible on a pale map.
        "ramp": _BLUE_RAMP[3:],
    },
    "dark": {
        "series": "#3987e5",
        "surface": "#0e1117",
        "ink": "#ffffff",
        "ink_secondary": "#c3c2b7",
        "muted": "#898781",
        "grid": "#2c2c2a",
        "axis": "#383835",
        # On a dark surface the scale flips: low is dark, high is light.
        "ramp": list(reversed(_BLUE_RAMP[:11])),
    },
}


def _hex_to_rgb(color: str) -> tuple[int, int, int]:
    color = color.lstrip("#")
    return int(color[0:2], 16), int(color[2:4], 16), int(color[4:6], 16)


def ramp_color(value: float, low: float, high: float, ramp: list[str]) -> list[int]:
    """Place a value on the ramp, blending between its two nearest steps."""
    if pd.isna(value) or high <= low:
        return list(_hex_to_rgb(ramp[0]))
    position = min(max((value - low) / (high - low), 0.0), 1.0) * (len(ramp) - 1)
    index = min(int(position), len(ramp) - 2)
    blend = position - index
    start, end = _hex_to_rgb(ramp[index]), _hex_to_rgb(ramp[index + 1])
    return [round(a + (b - a) * blend) for a, b in zip(start, end, strict=True)]


def _styled(chart: alt.LayerChart, theme: dict, height: int) -> alt.LayerChart:
    return (
        chart.properties(height=height, background="transparent")
        .configure(font=FONT)
        .configure_view(stroke=None)
        .configure_axis(
            labelColor=theme["muted"],
            titleColor=theme["ink_secondary"],
            labelFontSize=12,
            titleFontSize=12,
            titleFontWeight="normal",
            gridColor=theme["grid"],
            gridWidth=1,
            domainColor=theme["axis"],
            tickColor=theme["axis"],
        )
    )


def trend_chart(
    frame: pd.DataFrame, theme: dict, value_title: str, value_format: str = ",.1f"
) -> alt.LayerChart:
    """Daily values as a single line. Expects columns date_day, value, stations."""
    base = alt.Chart(frame).encode(
        x=alt.X(
            "date_day:T",
            title=None,
            # labelFlush=False keeps the first and last labels centred on their
            # ticks instead of nudging them inward into their neighbours.
            axis=alt.Axis(
                format="%b %-d", grid=False, tickCount=6, labelFlush=False, labelOverlap="greedy"
            ),
        ),
        y=alt.Y(
            "value:Q", title=value_title, scale=alt.Scale(zero=True), axis=alt.Axis(tickCount=5)
        ),
    )
    nearest = alt.selection_point(
        fields=["date_day"], nearest=True, on="pointerover", clear="pointerout", empty=False
    )

    line = base.mark_line(
        strokeWidth=2, strokeJoin="round", strokeCap="round", color=theme["series"]
    )
    # A vertical rule that follows the pointer and carries the tooltip, so the
    # reader does not have to land exactly on the line.
    crosshair = (
        base.mark_rule(color=theme["axis"], strokeWidth=1)
        .encode(
            opacity=alt.condition(nearest, alt.value(1), alt.value(0)),
            tooltip=[
                alt.Tooltip("date_day:T", title="Day", format="%a %b %-d, %Y"),
                alt.Tooltip("value:Q", title=value_title, format=value_format),
                alt.Tooltip("stations:Q", title="Stations", format=",d"),
            ],
        )
        .add_params(nearest)
    )
    dot = base.mark_point(
        size=90,
        filled=True,
        opacity=1,
        color=theme["series"],
        stroke=theme["surface"],
        strokeWidth=2,
    ).transform_filter(nearest)

    # Label only the most recent point; the axis and tooltip carry the rest.
    last = frame.loc[[frame["date_day"].idxmax()]]
    end_dot = (
        alt.Chart(last)
        .mark_point(
            size=90,
            filled=True,
            opacity=1,
            color=theme["series"],
            stroke=theme["surface"],
            strokeWidth=2,
        )
        .encode(x="date_day:T", y="value:Q")
    )
    end_label = (
        alt.Chart(last)
        .mark_text(align="left", dx=8, fontSize=12, color=theme["ink_secondary"])
        .encode(x="date_day:T", y="value:Q", text=alt.Text("value:Q", format=value_format))
    )
    # Room on the right for the end label, so it is never clipped.
    chart = (line + crosshair + dot + end_dot + end_label).properties(
        padding={"left": 5, "top": 5, "right": 48, "bottom": 5}
    )
    return _styled(chart, theme, height=300)


def count_bars(frame: pd.DataFrame, theme: dict, count_title: str) -> alt.LayerChart:
    """Horizontal bars. Expects columns label, count, in the order to display."""
    order = frame["label"].tolist()
    base = alt.Chart(frame).encode(
        y=alt.Y(
            "label:N",
            sort=order,
            title=None,
            axis=alt.Axis(labelLimit=240, domain=False, ticks=False, labelPadding=8),
        ),
        x=alt.X(
            "count:Q",
            title=count_title,
            # Headroom on the right so the value labels never clip.
            scale=alt.Scale(domain=[0, max(frame["count"].max(), 1) * 1.15], nice=False),
            axis=alt.Axis(tickCount=4, format=",d"),
        ),
    )
    bars = base.mark_bar(size=18, cornerRadiusEnd=4, color=theme["series"]).encode(
        tooltip=[
            alt.Tooltip("label:N", title="Category"),
            alt.Tooltip("count:Q", title=count_title, format=",d"),
        ]
    )
    labels = base.mark_text(align="left", dx=6, fontSize=12, color=theme["ink_secondary"]).encode(
        text=alt.Text("count:Q", format=",d")
    )
    return _styled(bars + labels, theme, height=34 * len(frame) + 48)
