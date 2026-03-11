"""
main.py
-------
FastAPI application entry point.

Endpoints:
  POST /upload-csv         — Upload a CSV and replace the active dataset.
  POST /generate-dashboard — Run the LangGraph pipeline on a NL question.
  POST /follow-up          — Alias for /generate-dashboard (follow-up queries).
  GET  /schema             — Return the current active dataset schema.
  GET  /health             — Liveness probe.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from backend.config import MAX_CSV_SIZE_BYTES, GEMINI_API_KEY as _ENV_GEMINI_KEY
from backend.csv_loader import CSVUploadError, load_csv_to_db
from backend.database import get_active_schema, init_db
from backend.pipeline import run_pipeline
from backend.query_executor import run_query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan — initialise DB on startup
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initialising database …")
    init_db()
    logger.info("Backend ready.")
    yield


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="AI Business Intelligence API",
    description=(
        "Conversational BI backend: upload CSV datasets and ask natural "
        "language questions to receive charts, insights, and business "
        "recommendations."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=3,
        max_length=1000,
        description="Natural language question about the uploaded dataset.",
        examples=["What are the top 5 products by total revenue?"],
    )


class UploadResponse(BaseModel):
    status: str
    filename: str
    row_count: int
    column_count: int
    columns: list[str]
    message: str


class DashboardResponse(BaseModel):
    status: str
    question: str
    sql_query: str | None = None
    chart_type: str | None = None
    chart_figure: dict[str, Any] | None = None
    insights: list[str] = []
    business_recommendations: list[str] = []
    error: str | None = None


class SchemaResponse(BaseModel):
    status: str
    table_name: str
    columns: dict[str, Any]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["System"])
def health_check() -> dict[str, str]:
    """Basic liveness probe."""
    return {"status": "ok", "service": "AI Business Analyser Backend"}


@app.post(
    "/upload-csv",
    response_model=UploadResponse,
    status_code=status.HTTP_200_OK,
    tags=["Data"],
    summary="Upload a CSV dataset (replaces any existing dataset)",
)
async def upload_csv(file: UploadFile = File(...)) -> UploadResponse:
    """
    Accept a CSV file upload.

    - Validates file type and size.
    - Drops the existing dataset table if present.
    - Creates a new SQLite table from the CSV.
    - Returns row count, column count, and detected schema.
    """
    # File-type validation
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only .csv files are accepted.",
        )

    raw_bytes = await file.read()

    if len(raw_bytes) == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded file is empty.",
        )

    if len(raw_bytes) > MAX_CSV_SIZE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File exceeds the maximum size limit ({MAX_CSV_SIZE_BYTES // (1024*1024)} MB).",
        )

    try:
        result = load_csv_to_db(raw_bytes, filename=file.filename)
    except CSVUploadError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        logger.exception("Unexpected error during CSV upload.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while processing the file.",
        ) from exc

    return UploadResponse(
        status="success",
        filename=result["filename"],
        row_count=result["row_count"],
        column_count=result["column_count"],
        columns=result["columns"],
        message=(
            f"Dataset '{file.filename}' loaded successfully "
            f"({result['row_count']} rows, {result['column_count']} columns)."
        ),
    )


def _run_dashboard_query(question: str) -> DashboardResponse:
    """Shared logic for /generate-dashboard and /follow-up."""
    # Ensure a dataset exists before calling the pipeline
    schema = get_active_schema()
    if schema is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No dataset has been uploaded yet. Please upload a CSV file first.",
        )

    try:
        final_state = run_pipeline(
            question,
            api_key=_ENV_GEMINI_KEY,
        )
    except Exception as exc:
        logger.exception("Unexpected pipeline error for question: %s", question)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred in the analysis pipeline.",
        ) from exc

    pipeline_status = final_state.get("status", "error")

    if pipeline_status in ("error", "unsupported", "quota_exceeded"):
        return DashboardResponse(
            status=pipeline_status,
            question=question,
            error=final_state.get("error", "An unknown error occurred."),
        )

    if pipeline_status == "empty_result":
        return DashboardResponse(
            status="empty_result",
            question=question,
            sql_query=final_state.get("sql_query"),
            insights=final_state.get("insights", []),
            error="No data found for this request. Try rephrasing or broadening your question.",
        )

    return DashboardResponse(
        status="success",
        question=question,
        sql_query=final_state.get("sql_query"),
        chart_type=final_state.get("chart_type"),
        chart_figure=final_state.get("chart_figure"),
        insights=final_state.get("insights", []),
        business_recommendations=final_state.get("business_recommendations", []),
    )


@app.post(
    "/generate-dashboard",
    response_model=DashboardResponse,
    tags=["Analysis"],
    summary="Generate a dashboard from a natural language question",
)
def generate_dashboard(request: QueryRequest) -> DashboardResponse:
    """
    Run the full LangGraph BI pipeline:
    1. Analyse schema
    2. Generate SQL via Gemini
    3. Validate SQL
    4. Execute query
    5. Build Plotly chart
    6. Generate insights and business recommendations

    Returns chart JSON, insights, recommendations, and the SQL used.
    """
    return _run_dashboard_query(request.question)


@app.post(
    "/follow-up",
    response_model=DashboardResponse,
    tags=["Analysis"],
    summary="Ask a follow-up question about the same dataset",
)
def follow_up_query(request: QueryRequest) -> DashboardResponse:
    """
    Identical pipeline to /generate-dashboard — provided as a semantic alias
    for follow-up conversational queries from the frontend.
    """
    return _run_dashboard_query(request.question)


@app.get(
    "/schema",
    response_model=SchemaResponse,
    tags=["Data"],
    summary="Get the active dataset schema",
)
def get_schema() -> SchemaResponse:
    """
    Return the schema of the currently active dataset, including column names,
    data types, null counts, unique counts, and sample values.
    """
    schema = get_active_schema()
    if schema is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No dataset has been uploaded yet.",
        )
    return SchemaResponse(
        status="success",
        table_name="dataset",
        columns=schema,
    )


@app.get(
    "/data-stats",
    tags=["Data"],
    summary="Get comprehensive statistics for the active dataset",
)
def get_data_stats() -> dict[str, Any]:
    """
    Return column stats, correlation matrix, value counts, distributions,
    and a data preview for the currently active dataset.
    """
    schema = get_active_schema()
    if schema is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No dataset has been uploaded yet.",
        )
    try:
        rows = run_query("SELECT * FROM dataset LIMIT 2000")
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset is empty.",
        )

    df = pd.DataFrame(rows)
    num_cols = df.select_dtypes(include="number").columns.tolist()
    cat_cols = [c for c in df.columns if c not in num_cols]

    # Descriptive stats
    desc: dict[str, Any] = {}
    if num_cols:
        for col in num_cols:
            s = df[col].describe()
            desc[col] = {k: round(float(v), 4) for k, v in s.items()}

    # Correlation matrix (numeric cols only)
    corr: dict[str, Any] = {}
    if len(num_cols) >= 2:
        cm = df[num_cols].corr().round(3)
        corr = {c: {r: float(cm.at[r, c]) for r in cm.index} for c in cm.columns}

    # Value counts for top categorical columns
    value_counts: dict[str, Any] = {}
    for col in cat_cols[:6]:
        vc = df[col].value_counts().head(15)
        value_counts[col] = {str(k): int(v) for k, v in vc.items()}

    # Distribution data for histograms (first 8 numeric cols, capped at 1000 pts)
    distributions: dict[str, Any] = {}
    for col in num_cols[:8]:
        distributions[col] = df[col].dropna().round(4).tolist()[:1000]

    # Null counts per column
    null_counts = {c: int(df[c].isna().sum()) for c in df.columns}

    return {
        "status": "success",
        "row_count": len(df),
        "col_count": len(df.columns),
        "numeric_columns": num_cols,
        "categorical_columns": cat_cols,
        "descriptive_stats": desc,
        "correlation_matrix": corr,
        "value_counts": value_counts,
        "distributions": distributions,
        "null_counts": null_counts,
        "sample_data": df.head(20).fillna("").to_dict(orient="records"),
    }
