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

import logging
import sqlite3
from typing import Any, Literal

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from backend.chart_selector import build_chart
from backend.database import get_active_schema
from backend.llm_service import (
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
        )
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
        )
    except Exception as exc:
        logger.warning("Insight generation failed: %s", exc)
        # Non-fatal — return empty insights rather than killing the pipeline
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
) -> Literal["sql_validator", "error_node"]:
    return "error_node" if state.get("status") in ("error", "unsupported") else "sql_validator"


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
    graph.add_edge("error_node", END)
    graph.add_edge("empty_result_node", END)

    return graph


# Compile once at import time — reused for every request
_compiled_graph = _build_graph().compile()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_pipeline(question: str) -> WorkflowState:
    """
    Execute the full LangGraph BI pipeline for a natural language question.

    Args:
        question: The user's natural language question.

    Returns:
        Final WorkflowState with chart_figure, insights, recommendations,
        or an error message.
    """
    initial_state: WorkflowState = {
        "user_question": question,
        "status": "running",
    }
    final_state: WorkflowState = _compiled_graph.invoke(initial_state)
    return final_state
