"""
pipeline.py
-----------
Simple sequential BI pipeline — no LangGraph dependency.

Flow:
  1. Load active schema from SQLite
  2. Call LLM (Gemini / OpenRouter / Groq) to generate SQL + chart config
  3. Validate SQL for safety
  4. Execute SQL against SQLite → list of row dicts
  5. Convert to pandas DataFrame
  6. Build Plotly chart via chart_selector
  7. Call LLM for insights + business recommendations
  8. Return combined result dict

Fallback path (any LLM failure or no API key supplied):
  - Analyse the full dataset directly with pandas
  - Auto-detect relevant columns from question keywords
  - Build appropriate bar / line / scatter / histogram chart
  - Return immediately with status = "success"
"""

import base64
import json
import logging
import struct

import pandas as pd
import plotly.express as px

from backend.chart_selector import build_chart, figure_to_dict
from backend.database import get_active_schema
from backend.llm_service import (
    QuotaExceededError,
    generate_insights_and_recommendations,
    generate_sql_and_chart_config,
)
from backend.query_executor import SQLValidationError, run_query, validate_sql

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pandas fallback helpers
# ---------------------------------------------------------------------------

def _pick_columns(question: str, cat_cols: list[str], num_cols: list[str]):
    """Return (x_col, y_col) by matching question keywords to column names."""
    q = question.lower()

    def score(col: str) -> int:
        return sum(1 for t in col.lower().replace("_", " ").split() if t in q)

    time_kw = ("trend", "month", "monthly", "over time", "time", "date",
                "week", "year", "quarter")
    wants_time = any(kw in q for kw in time_kw)

    date_cols = [c for c in cat_cols if any(kw in c.lower()
                 for kw in ("date", "month", "time", "week", "year", "period"))]

    if wants_time and date_cols:
        x_col = date_cols[0]
    else:
        x_col = max(cat_cols, key=score) if cat_cols else None

    y_col = max(num_cols, key=score) if num_cols else None
    return x_col, y_col


def _agg_op(question: str) -> str:
    q = question.lower()
    if any(kw in q for kw in ("average", "avg", "mean")):
        return "mean"
    return "sum"


def _pandas_fallback(question: str) -> dict:
    """Build a chart directly from the dataset without any LLM call."""
    logger.warning("AI unavailable — using fallback analysis.")
    try:
        rows = run_query("SELECT * FROM dataset LIMIT 2000")
        if not rows:
            return {"status": "error", "error": "Dataset is empty — no data to visualise."}

        df = pd.DataFrame(rows)
        num_cols = df.select_dtypes(include="number").columns.tolist()
        cat_cols = [c for c in df.columns if c not in num_cols]

        q = question.lower()
        wants_time = any(kw in q for kw in ("trend", "monthly", "over time", "time series"))
        wants_scatter = any(kw in q for kw in ("scatter", " vs ", "versus", "correlation"))

        if num_cols and cat_cols:
            x_col, y_col = _pick_columns(question, cat_cols, num_cols)
            x_col = x_col or cat_cols[0]
            y_col = y_col or num_cols[0]
            agg_fn = _agg_op(question)

            agg_df = (df.groupby(x_col, as_index=False)[y_col].mean()
                      if agg_fn == "mean"
                      else df.groupby(x_col, as_index=False)[y_col].sum())
            agg_df = agg_df.sort_values(x_col)

            agg_label = "Average" if agg_fn == "mean" else "Total"
            title = (f"{agg_label} {y_col.replace('_', ' ').title()} "
                     f"by {x_col.replace('_', ' ').title()}")

            if wants_scatter and len(num_cols) >= 2:
                fig = px.scatter(df, x=num_cols[0], y=num_cols[1],
                                 title=f"{num_cols[1].replace('_',' ').title()} "
                                       f"vs {num_cols[0].replace('_',' ').title()}",
                                 template="plotly_white")
                chart_type = "scatter"
            elif wants_time:
                fig = px.line(agg_df, x=x_col, y=y_col, title=title,
                              markers=True, template="plotly_white")
                chart_type = "line"
            else:
                fig = px.bar(agg_df, x=x_col, y=y_col, title=title,
                             template="plotly_white")
                chart_type = "bar"

            peak_row = agg_df.iloc[agg_df[y_col].idxmax()]
            insights = [
                "AI unavailable — using fallback analysis.",
                f"Showing {agg_label.lower()} {y_col} grouped by {x_col}.",
                (f"Peak: {peak_row[x_col]}  "
                 f"({agg_label.lower()} {y_col} = {peak_row[y_col]:,.2f})."),
                f"Overall total {y_col}: {df[y_col].sum():,.2f}.",
            ]

        elif num_cols:
            col = num_cols[0]
            fig = px.histogram(df, x=col,
                               title=f"Distribution of {col.replace('_', ' ').title()}",
                               template="plotly_white")
            chart_type = "histogram"
            insights = [
                "AI unavailable — using fallback analysis.",
                f"Distribution of {col}.",
                f"Mean: {df[col].mean():,.2f}  |  "
                f"Range: {df[col].min():,.2f} – {df[col].max():,.2f}.",
            ]
        else:
            from backend.chart_selector import _build_table
            fig = _build_table(df.head(50), "Dataset Preview")
            chart_type = "table"
            insights = [
                "AI unavailable — using fallback analysis.",
                "No numeric columns detected — showing dataset preview.",
            ]

        return {
            "status": "success",
            "sql_query": "-- Showing auto-detected analysis",
            "chart_type": chart_type,
            "chart_figure": figure_to_dict(fig),
            "query_results": rows[:50],
            "insights": insights,
            "business_recommendations": [
                "Auto-analysis complete. Ask a specific question for deeper AI-powered insights.",
            ],
            "error": None,
        }

    except Exception as exc:
        logger.error("Pandas fallback failed: %s", exc)
        return {"status": "error", "error": f"Fallback analysis failed: {exc}"}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_pipeline(
    question: str,
    api_key: str = "",
) -> dict:
    """
    Execute the full sequential BI pipeline for a natural language question.

    Returns a dict compatible with DashboardResponse:
      status, sql_query, chart_type, chart_figure,
      insights, business_recommendations, error
    """
    # ── 1. Schema ──────────────────────────────────────────────────────────
    schema = get_active_schema()
    if not schema:
        return {
            "status": "error",
            "error": "No dataset has been uploaded yet. Please upload a CSV file first.",
        }

    # ── 2. If no key available, go straight to pandas fallback ────────────
    has_key = bool(api_key.strip())
    if not has_key:
        return _pandas_fallback(question)

    # ── 3. LLM → SQL + chart config ───────────────────────────────────────
    try:
        llm_out = generate_sql_and_chart_config(
            question=question,
            schema=schema,
            api_key=api_key,
        )
    except QuotaExceededError as exc:
        logger.warning("All LLM providers exhausted: %s — falling back to pandas.", exc)
        return _pandas_fallback(question)
    except ValueError as exc:
        msg = str(exc)
        if "UNSUPPORTED_QUERY" in msg:
            return {
                "status": "unsupported",
                "error": ("This question cannot be answered with the available dataset. "
                          "Please try a different question."),
            }
        logger.warning("LLM SQL generation failed: %s — falling back to pandas.", exc)
        return _pandas_fallback(question)
    except Exception as exc:
        logger.warning("LLM error: %s — falling back to pandas.", exc)
        return _pandas_fallback(question)

    sql_raw     = llm_out.get("sql_query", "").strip()
    chart_type  = llm_out.get("chart_type", "bar")
    x_axis      = llm_out.get("x_axis", "")
    y_axis      = llm_out.get("y_axis", "")
    chart_title = llm_out.get("title", "Dashboard")

    if not sql_raw:
        logger.warning("LLM returned no SQL — falling back to pandas.")
        return _pandas_fallback(question)

    # ── 4. Validate SQL ───────────────────────────────────────────────────
    try:
        sql = validate_sql(sql_raw)
    except SQLValidationError as exc:
        return {
            "status": "error",
            "error": f"Generated SQL was unsafe: {exc}  Please rephrase your question.",
        }

    # ── 5. Execute SQL → DataFrame ────────────────────────────────────────
    try:
        rows = run_query(sql)
    except SQLValidationError as exc:
        return {"status": "error", "error": str(exc)}
    except Exception as exc:
        logger.warning("SQL execution error: %s — falling back to pandas.", exc)
        return _pandas_fallback(question)

    if not rows:
        return {
            "status": "empty_result",
            "sql_query": sql,
            "chart_figure": None,
            "insights": ["No data found for this request. Try broadening your query."],
            "business_recommendations": [],
            "error": "No data found for this request.",
        }

    df = pd.DataFrame(rows)  # noqa: F841 — available for future use

    # ── 6. Build Plotly chart ─────────────────────────────────────────────
    try:
        chart_figure = build_chart(
            query_results=rows,
            chart_type=chart_type,
            x_axis=x_axis,
            y_axis=y_axis,
            title=chart_title,
        )
    except Exception as exc:
        logger.warning("Chart building failed: %s — trying pandas fallback chart.", exc)
        return _pandas_fallback(question)

    # ── 7. LLM insights (non-fatal) ───────────────────────────────────────
    insights: list[str] = []
    recs: list[str] = []
    try:
        insight_out = generate_insights_and_recommendations(
            question=question,
            schema=schema,
            query_results=rows,
            api_key=api_key,
        )
        insights = insight_out.get("insights", [])
        recs     = insight_out.get("business_recommendations", [])
    except QuotaExceededError:
        insights = ["AI service is temporarily busy. Please try again in a moment."]
    except Exception as exc:
        logger.warning("Insight generation failed: %s", exc)
        insights = ["Insights could not be generated for this query."]

    # ── 8. Return ─────────────────────────────────────────────────────────
    return {
        "status": "success",
        "sql_query": sql,
        "chart_type": chart_type,
        "chart_figure": chart_figure,
        "query_results": rows[:50],
        "insights": insights,
        "business_recommendations": recs,
        "error": None,
    }
