"""
api_client.py
-------------
All HTTP communication between Streamlit and the FastAPI backend.
Raises APIError with user-friendly messages on failures.
"""

from typing import Any

import requests

BASE_URL = "http://localhost:8000"
# (connect_timeout, read_timeout) — connect fast, allow 5 min for LLM pipeline
TIMEOUT = (10, 300)


class APIError(Exception):
    """Raised when the backend returns an error or is unreachable."""


def _handle_response(response: requests.Response) -> dict[str, Any]:
    try:
        response.raise_for_status()
    except requests.HTTPError:
        try:
            detail = response.json().get("detail", response.text)
        except Exception:
            detail = response.text
        raise APIError(f"Backend error ({response.status_code}): {detail}")
    return response.json()


_CONN_ERR = (
    "Cannot reach the backend API.  "
    "Make sure the FastAPI server is running:\n"
    "```\nuvicorn backend.main:app --reload --port 8000\n```"
)

_TIMEOUT_ERR = (
    "The backend took too long to respond. "
    "This usually means the Gemini API is slow or rate-limited. "
    "Please wait a moment and try again."
)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

def upload_csv(file_bytes: bytes, filename: str) -> dict[str, Any]:
    """POST /upload-csv — upload a CSV file."""
    try:
        resp = requests.post(
            f"{BASE_URL}/upload-csv",
            files={"file": (filename, file_bytes, "text/csv")},
            timeout=TIMEOUT,
        )
    except requests.ConnectionError:
        raise APIError(_CONN_ERR)
    return _handle_response(resp)


def generate_dashboard(question: str) -> dict[str, Any]:
    """POST /generate-dashboard — run the full BI pipeline."""
    try:
        resp = requests.post(
            f"{BASE_URL}/generate-dashboard",
            json={"question": question},
            timeout=TIMEOUT,
        )
    except requests.ReadTimeout:
        raise APIError(_TIMEOUT_ERR)
    except requests.ConnectionError:
        raise APIError(_CONN_ERR)
    return _handle_response(resp)


def follow_up_query(question: str) -> dict[str, Any]:
    """POST /follow-up — follow-up query on the same dataset."""
    try:
        resp = requests.post(
            f"{BASE_URL}/follow-up",
            json={"question": question},
            timeout=TIMEOUT,
        )
    except requests.ReadTimeout:
        raise APIError(_TIMEOUT_ERR)
    except requests.ConnectionError:
        raise APIError(_CONN_ERR)
    return _handle_response(resp)


def get_schema() -> dict[str, Any] | None:
    """GET /schema — return active schema, or None if no dataset loaded."""
    try:
        resp = requests.get(f"{BASE_URL}/schema", timeout=10)
    except requests.ConnectionError:
        return None
    if resp.status_code == 404:
        return None
    return _handle_response(resp)


def get_data_stats() -> dict[str, Any]:
    """GET /data-stats — return comprehensive dataset statistics."""
    try:
        resp = requests.get(f"{BASE_URL}/data-stats", timeout=30)
    except requests.ConnectionError:
        raise APIError(_CONN_ERR)
    return _handle_response(resp)


def health_check() -> bool:
    """GET /health — True if backend is reachable and healthy."""
    try:
        resp = requests.get(f"{BASE_URL}/health", timeout=5)
        return resp.status_code == 200
    except Exception:
        return False
