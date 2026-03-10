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

from fastapi import FastAPI, File, HTTPException, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from backend.config import MAX_CSV_SIZE_BYTES
from backend.csv_loader import CSVUploadError, load_csv_to_db
from backend.database import get_active_schema, init_db
from backend.langgraph_pipeline import run_pipeline

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
        final_state = run_pipeline(question)
    except Exception as exc:
        logger.exception("Unexpected pipeline error for question: %s", question)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred in the analysis pipeline.",
        ) from exc

    pipeline_status = final_state.get("status", "error")

    if pipeline_status in ("error", "unsupported"):
        return DashboardResponse(
            status="error",
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
