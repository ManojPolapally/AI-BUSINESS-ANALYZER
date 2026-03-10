"""
chart_selector.py
-----------------
Builds Plotly figure JSON from query results and chart configuration
provided by the LangGraph pipeline.

Responsibilities:
- Map chart_type strings to Plotly figure factories.
- Detect suitable columns when x/y are not explicitly set.
- Return a serialisable Plotly figure dict ready for the API response.
- Handle edge cases (single column, no numeric data, etc.).
"""

import logging
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

logger = logging.getLogger(__name__)

# Mapping from LLM-provided chart type strings to internal keys
_CHART_TYPE_ALIASES: dict[str, str] = {
    "bar": "bar",
    "bar_chart": "bar",
    "barchart": "bar",
    "horizontal_bar": "bar_h",
    "line": "line",
    "line_chart": "line",
    "linechart": "line",
    "pie": "pie",
    "pie_chart": "pie",
    "donut": "pie",
    "scatter": "scatter",
    "scatter_plot": "scatter",
    "scatterplot": "scatter",
    "histogram": "histogram",
    "hist": "histogram",
    "heatmap": "heatmap",
    "heat_map": "heatmap",
    "table": "table",
}

_DEFAULT_CHART = "bar"


# ---------------------------------------------------------------------------
# Column inference helpers
# ---------------------------------------------------------------------------

def _infer_x_y(
    df: pd.DataFrame,
    x_hint: str,
    y_hint: str,
) -> tuple[str | None, str | None]:
    """
    Return (x_col, y_col) using hints from the LLM if valid, otherwise fall
    back to the first categorical + first numeric column.
    """
    cols = list(df.columns)

    x = x_hint if x_hint and x_hint in cols else None
    y = y_hint if y_hint and y_hint in cols else None

    if x is None or y is None:
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        non_numeric = [c for c in cols if c not in numeric_cols]

        if x is None:
            x = non_numeric[0] if non_numeric else (cols[0] if cols else None)
        if y is None:
            y = numeric_cols[0] if numeric_cols else (cols[1] if len(cols) > 1 else cols[0] if cols else None)

    return x, y


# ---------------------------------------------------------------------------
# Chart builders
# ---------------------------------------------------------------------------

def _build_bar(df: pd.DataFrame, x: str, y: str, title: str) -> go.Figure:
    return px.bar(df, x=x, y=y, title=title, template="plotly_white")


def _build_bar_h(df: pd.DataFrame, x: str, y: str, title: str) -> go.Figure:
    return px.bar(df, x=y, y=x, title=title, orientation="h", template="plotly_white")


def _build_line(df: pd.DataFrame, x: str, y: str, title: str) -> go.Figure:
    return px.line(df, x=x, y=y, title=title, markers=True, template="plotly_white")


def _build_scatter(df: pd.DataFrame, x: str, y: str, title: str) -> go.Figure:
    return px.scatter(df, x=x, y=y, title=title, template="plotly_white")


def _build_pie(df: pd.DataFrame, x: str, y: str, title: str) -> go.Figure:
    return px.pie(df, names=x, values=y, title=title, template="plotly_white")


def _build_histogram(df: pd.DataFrame, x: str, y: str, title: str) -> go.Figure:
    col = x or y or df.columns[0]
    return px.histogram(df, x=col, title=title, template="plotly_white")


def _build_heatmap(df: pd.DataFrame, x: str, y: str, title: str) -> go.Figure:
    numeric_df = df.select_dtypes(include="number")
    if numeric_df.empty:
        numeric_df = df
    fig = px.imshow(
        numeric_df.corr(numeric_only=True) if len(numeric_df.columns) > 1 else numeric_df,
        title=title,
        template="plotly_white",
        color_continuous_scale="Blues",
    )
    return fig


def _build_table(df: pd.DataFrame, title: str) -> go.Figure:
    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=list(df.columns),
                    fill_color="#4A90D9",
                    font=dict(color="white", size=12),
                    align="left",
                ),
                cells=dict(
                    values=[df[c].tolist() for c in df.columns],
                    fill_color="lavender",
                    align="left",
                ),
            )
        ]
    )
    fig.update_layout(title=title)
    return fig


_BUILDERS = {
    "bar": _build_bar,
    "bar_h": _build_bar_h,
    "line": _build_line,
    "scatter": _build_scatter,
    "pie": _build_pie,
    "histogram": _build_histogram,
    "heatmap": _build_heatmap,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_chart(
    query_results: list[dict[str, Any]],
    chart_type: str,
    x_axis: str,
    y_axis: str,
    title: str,
) -> dict[str, Any]:
    """
    Build and return a Plotly figure as a JSON-serialisable dict.

    Args:
        query_results: Rows returned from the SQL query.
        chart_type:    Chart type string from the LLM.
        x_axis:        Suggested x-axis column (may be empty or invalid).
        y_axis:        Suggested y-axis column (may be empty or invalid).
        title:         Chart title.

    Returns:
        Plotly figure serialised as dict via fig.to_dict().

    Raises:
        ValueError if query_results is empty.
    """
    if not query_results:
        raise ValueError("Cannot build chart: query returned no rows.")

    df = pd.DataFrame(query_results)

    # Normalise chart type
    ct_key = _CHART_TYPE_ALIASES.get(chart_type.lower().strip(), _DEFAULT_CHART)

    # Handle table separately (no x/y needed)
    if ct_key == "table":
        fig = _build_table(df, title or "Query Results")
        return fig.to_dict()

    x, y = _infer_x_y(df, x_axis, y_axis)

    if x is None or y is None:
        logger.warning(
            "Could not determine x/y columns for chart '%s'. Falling back to table.",
            ct_key,
        )
        fig = _build_table(df, title or "Query Results")
        return fig.to_dict()

    builder = _BUILDERS.get(ct_key, _build_bar)

    try:
        fig = builder(df, x, y, title or "Chart")
    except Exception as exc:
        logger.error("Chart builder '%s' failed: %s. Falling back to table.", ct_key, exc)
        fig = _build_table(df, title or "Query Results")

    return fig.to_dict()
