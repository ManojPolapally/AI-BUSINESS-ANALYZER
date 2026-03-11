"""
langgraph_pipeline.py
---------------------
Defines the LangGraph StateGraph that orchestrates the full BI pipeline.

Nodes (in execution order):
  1. schema_analyzer   — load active schema into state
  2. sql_generator     — call Gemini to produce SQL + chart config
  3. sql_validator     — safety-check the SQL
  4. query_executor    — run the SQL against SQLite
  5. chart_selector    — build Plotly figure JSON
  6. insight_generator — call Gemini to produce insights
  7. recommendation    — call Gemini to produce business recommendations

Conditional edges handle:
  - No schema loaded       → error
  - UNSUPPORTED_QUERY      → error
  - SQL validation failure → error
  - Empty query results    → empty_result
  - Success path           → full chart + insights pipeline
"""

import json
import logging
import sqlite3
from typing import Any, Literal

import pandas as pd
import plotly.express as px
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from backend.chart_selector import build_chart, figure_to_dict
from backend.database import get_active_schema
from backend.llm_service import (
    QuotaExceededError,
    generate_insights_and_recommendations,
    generate_sql_and_chart_config,
)
from backend.query_executor import SQLValidationError, run_query

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared workflow state
# ---------------------------------------------------------------------------

class WorkflowState(TypedDict, total=False):
    # Input
    user_question: str
    api_key: str          # user-supplied Gemini API key; never logged
    openrouter_key: str   # user-supplied OpenRouter API key; never logged
    groq_key: str         # user-supplied Groq API key; never logged

    # Schema
    schema: dict[str, Any]

    # SQL + chart config from LLM
    sql_query: str
    chart_type: str
    x_axis: str
    y_axis: str
    chart_title: str

    # Execution results
    query_results: list[dict[str, Any]]

    # Output
    chart_figure: dict[str, Any]
    insights: list[str]
    business_recommendations: list[str]

    # Error handling
    error: str | None
    status: str   # "success" | "error" | "empty_result" | "unsupported"


# ---------------------------------------------------------------------------
# Node implementations
# ---------------------------------------------------------------------------

def schema_analyzer_node(state: WorkflowState) -> WorkflowState:
    """Load the current active schema into state."""
    schema = get_active_schema()
    if not schema:
        return {
            **state,
            "error": "No dataset has been uploaded yet. Please upload a CSV file first.",
            "status": "error",
        }
    return {**state, "schema": schema, "status": "running"}


def sql_generator_node(state: WorkflowState) -> WorkflowState:
    """Call Gemini to translate the user question into SQL + chart config."""
    try:
        llm_output = generate_sql_and_chart_config(
            question=state["user_question"],
            schema=state["schema"],
            api_key=state.get("api_key", ""),
            openrouter_key=state.get("openrouter_key", ""),
            groq_key=state.get("groq_key", ""),
        )
    except QuotaExceededError as exc:
        return {**state, "error": str(exc), "status": "quota_exceeded"}
    except ValueError as exc:
        msg = str(exc)
        if "UNSUPPORTED_QUERY" in msg:
            return {
                **state,
                "error": (
                    "This question cannot be answered with the available dataset. "
                    "Please try a different question."
                ),
                "status": "unsupported",
            }
        return {**state, "error": f"SQL generation failed: {msg}", "status": "error"}
    except RuntimeError as exc:
        return {**state, "error": str(exc), "status": "error"}

    return {
        **state,
        "sql_query": llm_output.get("sql_query", ""),
        "chart_type": llm_output.get("chart_type", "bar"),
        "x_axis": llm_output.get("x_axis", ""),
        "y_axis": llm_output.get("y_axis", ""),
        "chart_title": llm_output.get("title", "Dashboard"),
        "status": "running",
    }


def sql_validator_node(state: WorkflowState) -> WorkflowState:
    """Validate the generated SQL for safety and schema correctness."""
    from backend.query_executor import validate_sql

    sql = state.get("sql_query", "").strip()
    if not sql:
        return {
            **state,
            "error": "No SQL query was generated. Please rephrase your question.",
            "status": "error",
        }

    try:
        cleaned_sql = validate_sql(sql)
    except SQLValidationError as exc:
        return {
            **state,
            "error": f"Invalid query generated. {exc} Please rephrase your question.",
            "status": "error",
        }

    return {**state, "sql_query": cleaned_sql, "status": "running"}


def query_executor_node(state: WorkflowState) -> WorkflowState:
    """Execute the validated SQL query against SQLite."""
    try:
        results = run_query(state["sql_query"])
    except SQLValidationError as exc:
        return {**state, "error": str(exc), "status": "error"}
    except sqlite3.Error as exc:
        return {
            **state,
            "error": f"Database error while executing query: {exc}",
            "status": "error",
        }

    if not results:
        return {
            **state,
            "query_results": [],
            "status": "empty_result",
        }

    return {**state, "query_results": results, "status": "running"}


def chart_selector_node(state: WorkflowState) -> WorkflowState:
    """Build the Plotly chart from query results and LLM chart config."""
    try:
        figure = build_chart(
            query_results=state["query_results"],
            chart_type=state.get("chart_type", "bar"),
            x_axis=state.get("x_axis", ""),
            y_axis=state.get("y_axis", ""),
            title=state.get("chart_title", "Dashboard"),
        )
    except Exception as exc:
        logger.error("Chart building failed: %s", exc)
        return {
            **state,
            "error": f"Chart generation failed: {exc}",
            "status": "error",
        }

    return {**state, "chart_figure": figure, "status": "running"}


def insight_generator_node(state: WorkflowState) -> WorkflowState:
    """Call Gemini to generate factual insights from query results."""
    try:
        output = generate_insights_and_recommendations(
            question=state["user_question"],
            schema=state["schema"],
            query_results=state["query_results"],
            api_key=state.get("api_key", ""),
            openrouter_key=state.get("openrouter_key", ""),
            groq_key=state.get("groq_key", ""),
        )
    except QuotaExceededError as exc:
        logger.warning("Quota exceeded during insight generation: %s", str(exc))
        # Non-fatal — chart still shown, insights degrade gracefully
        return {
            **state,
            "insights": ["AI service is temporarily busy. Please try again in a few seconds."],
            "business_recommendations": [],
            "status": "running",
        }
    except Exception as exc:
        logger.warning("Insight generation failed: %s", exc)
        return {
            **state,
            "insights": ["Insights could not be generated for this query."],
            "business_recommendations": [],
            "status": "running",
        }

    return {
        **state,
        "insights": output.get("insights", []),
        "business_recommendations": output.get("business_recommendations", []),
        "status": "success",
    }


def _pick_columns_from_question(
    question: str,
    categorical_cols: list[str],
    numeric_cols: list[str],
) -> tuple[str | None, str | None]:
    """
    Scan the question text for column-name keywords and return (x_col, y_col).
    Falls back to the first categorical + first numeric column when no match.
    """
    q_lower = question.lower()

    # Score each column by how many of its tokens appear in the question
    def score(col: str) -> int:
        tokens = col.lower().replace("_", " ").split()
        return sum(1 for t in tokens if t in q_lower)

    # Time / trend keywords → prefer date-like column as x
    time_keywords = ("trend", "month", "monthly", "over time", "time", "date",
                     "week", "year", "quarter", "period")
    wants_time = any(kw in q_lower for kw in time_keywords)

    # Pick best x (categorical / date)
    scored_cat = sorted(categorical_cols, key=score, reverse=True)
    if wants_time:
        # Prefer columns whose name contains date/month/time
        date_cols = [c for c in categorical_cols
                     if any(kw in c.lower() for kw in ("date", "month", "time",
                                                        "week", "year", "period"))]
        x_col = date_cols[0] if date_cols else (scored_cat[0] if scored_cat else None)
    else:
        x_col = scored_cat[0] if scored_cat else None

    # Pick best y (numeric)
    scored_num = sorted(numeric_cols, key=score, reverse=True)
    y_col = scored_num[0] if scored_num else None

    return x_col, y_col


def _agg_operation_from_question(question: str) -> str:
    """Return 'mean', 'max', or 'sum' based on question keywords."""
    q = question.lower()
    if any(kw in q for kw in ("average", "avg", "mean")):
        return "mean"
    if any(kw in q for kw in ("maximum", "highest", "top", "max", "most")):
        return "sum"   # aggregate then sort for top
    return "sum"


def pandas_fallback_node(state: WorkflowState) -> WorkflowState:
    """
    Fallback: when all LLM providers are unavailable, auto-analyse the
    dataset with pandas and build a Plotly chart directly.
    Parses column-name hints from the question to produce a relevant chart.
    """
    logger.warning("AI unavailable — using fallback analysis.")

    question = state.get("user_question", "")

    try:
        rows = run_query("SELECT * FROM dataset LIMIT 2000")
        if not rows:
            return {
                **state,
                "error": "Dataset is empty — no data to visualise.",
                "status": "error",
            }

        df = pd.DataFrame(rows)
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        categorical_cols = [c for c in df.columns if c not in numeric_cols]

        if numeric_cols and categorical_cols:
            x_col, y_col = _pick_columns_from_question(
                question, categorical_cols, numeric_cols
            )
            if x_col is None:
                x_col = categorical_cols[0]
            if y_col is None:
                y_col = numeric_cols[0]

            agg_fn = _agg_operation_from_question(question)

            # Time-trend → line chart; otherwise bar
            q_lower = question.lower()
            wants_time = any(kw in q_lower for kw in
                             ("trend", "monthly", "over time", "time series"))
            wants_scatter = any(kw in q_lower for kw in ("scatter", "vs", "versus",
                                                          "correlation", "compare"))

            if agg_fn == "mean":
                agg_df = df.groupby(x_col, as_index=False)[y_col].mean()
                agg_label = "Average"
            else:
                agg_df = df.groupby(x_col, as_index=False)[y_col].sum()
                agg_label = "Total"

            agg_df = agg_df.sort_values(x_col)

            title = (
                f"{agg_label} {y_col.replace('_', ' ').title()} by "
                f"{x_col.replace('_', ' ').title()}"
            )

            if wants_scatter and len(numeric_cols) >= 2:
                fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1],
                                 title=f"{numeric_cols[1].replace('_',' ').title()} "
                                       f"vs {numeric_cols[0].replace('_',' ').title()}",
                                 template="plotly_white")
                chart_type_used = "scatter"
            elif wants_time:
                fig = px.line(agg_df, x=x_col, y=y_col, title=title,
                              markers=True, template="plotly_white")
                chart_type_used = "line"
            else:
                fig = px.bar(agg_df, x=x_col, y=y_col, title=title,
                             template="plotly_white")
                chart_type_used = "bar"

            peak_row = agg_df.iloc[agg_df[y_col].idxmax()]
            insights = [
                "AI unavailable — using fallback analysis.",
                f"Showing {agg_label.lower()} {y_col} grouped by {x_col}.",
                (f"Peak: {peak_row[x_col]}  "
                 f"({agg_label.lower()} {y_col} = {peak_row[y_col]:,.2f})."),
                f"Overall total {y_col}: {df[y_col].sum():,.2f}.",
            ]

        elif numeric_cols:
            col = numeric_cols[0]
            fig = px.histogram(
                df, x=col,
                title=f"Distribution of {col.replace('_', ' ').title()}",
                template="plotly_white",
            )
            chart_type_used = "histogram"
            insights = [
                "AI unavailable — using fallback analysis.",
                f"Distribution of {col}.",
                (f"Mean: {df[col].mean():,.2f}  |  "
                 f"Range: {df[col].min():,.2f} – {df[col].max():,.2f}."),
            ]
        else:
            from backend.chart_selector import _build_table
            fig = _build_table(df.head(50), "Dataset Preview")
            chart_type_used = "table"
            insights = [
                "AI unavailable — using fallback analysis.",
                "No numeric columns detected — showing dataset preview.",
            ]

        figure_dict = figure_to_dict(fig)

    except Exception as exc:
        logger.error("Pandas fallback failed: %s", exc)
        return {
            **state,
            "error": f"Fallback analysis also failed: {exc}",
            "status": "error",
        }

    return {
        **state,
        "chart_figure": figure_dict,
        "chart_type": chart_type_used,
        "sql_query": "-- AI unavailable; fallback pandas analysis used",
        "query_results": rows[:50],
        "insights": insights,
        "business_recommendations": [
            "Enter a valid API key (Gemini, OpenRouter, or Groq) for AI-powered insights.",
        ],
        "status": "success",
    }


def error_node(state: WorkflowState) -> WorkflowState:
    """Terminal node for error states — passes state through unchanged."""
    logger.error("Pipeline ended with error: %s", state.get("error"))
    return state


def empty_result_node(state: WorkflowState) -> WorkflowState:
    """Terminal node for empty query results."""
    return {
        **state,
        "insights": ["No data found for this request. Try broadening your query."],
        "business_recommendations": [],
    }


# ---------------------------------------------------------------------------
# Conditional edge routing functions
# ---------------------------------------------------------------------------

def route_after_schema(
    state: WorkflowState,
) -> Literal["sql_generator", "error_node"]:
    return "error_node" if state.get("status") == "error" else "sql_generator"


def route_after_sql_gen(
    state: WorkflowState,
) -> Literal["sql_validator", "pandas_fallback", "error_node"]:
    status = state.get("status")
    if status == "unsupported":
        return "error_node"
    if status in ("error", "quota_exceeded"):
        return "pandas_fallback"
    return "sql_validator"


def route_after_sql_validation(
    state: WorkflowState,
) -> Literal["query_executor", "error_node"]:
    return "error_node" if state.get("status") == "error" else "query_executor"


def route_after_execution(
    state: WorkflowState,
) -> Literal["chart_selector", "empty_result_node", "error_node"]:
    status = state.get("status")
    if status == "error":
        return "error_node"
    if status == "empty_result":
        return "empty_result_node"
    return "chart_selector"


def route_after_chart(
    state: WorkflowState,
) -> Literal["insight_generator", "error_node"]:
    return "error_node" if state.get("status") == "error" else "insight_generator"


# ---------------------------------------------------------------------------
# Graph assembly
# ---------------------------------------------------------------------------

def _build_graph() -> StateGraph:
    graph = StateGraph(WorkflowState)

    # Register nodes
    graph.add_node("schema_analyzer", schema_analyzer_node)
    graph.add_node("sql_generator", sql_generator_node)
    graph.add_node("sql_validator", sql_validator_node)
    graph.add_node("query_executor", query_executor_node)
    graph.add_node("chart_selector", chart_selector_node)
    graph.add_node("insight_generator", insight_generator_node)
    graph.add_node("pandas_fallback", pandas_fallback_node)
    graph.add_node("error_node", error_node)
    graph.add_node("empty_result_node", empty_result_node)

    # Entry point
    graph.add_edge(START, "schema_analyzer")

    # Conditional edges
    graph.add_conditional_edges("schema_analyzer", route_after_schema)
    graph.add_conditional_edges("sql_generator", route_after_sql_gen)
    graph.add_conditional_edges("sql_validator", route_after_sql_validation)
    graph.add_conditional_edges("query_executor", route_after_execution)
    graph.add_conditional_edges("chart_selector", route_after_chart)

    # Terminal edges
    graph.add_edge("insight_generator", END)
    graph.add_edge("pandas_fallback", END)
    graph.add_edge("error_node", END)
    graph.add_edge("empty_result_node", END)

    return graph


# Compile once at import time — reused for every request
_compiled_graph = _build_graph().compile()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_pipeline(
    question: str,
    api_key: str = "",
    openrouter_key: str = "",
    groq_key: str = "",
) -> WorkflowState:
    """
    Execute the full LangGraph BI pipeline for a natural language question.

    Args:
        question:       The user's natural language question.
        api_key:        User-supplied Gemini API key (never logged).
        openrouter_key: User-supplied OpenRouter API key (never logged).
        groq_key:       User-supplied Groq API key (never logged).

    Returns:
        Final WorkflowState with chart_figure, insights, recommendations,
        or an error message.
    """
    initial_state: WorkflowState = {
        "user_question": question,
        "api_key": api_key,
        "openrouter_key": openrouter_key,
        "groq_key": groq_key,
        "status": "running",
    }
    final_state: WorkflowState = _compiled_graph.invoke(initial_state)
    return final_state
