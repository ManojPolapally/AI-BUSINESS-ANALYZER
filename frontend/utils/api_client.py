"""
api_client.py
-------------
All HTTP communication between Streamlit and the FastAPI backend.
Raises APIError with user-friendly messages on failures.
"""

from typing import Any

import requests

BASE_URL = "http://localhost:8000"
TIMEOUT = 120  # seconds — LLM calls can be slow


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


def health_check() -> bool:
    """GET /health — True if backend is reachable and healthy."""
    try:
        resp = requests.get(f"{BASE_URL}/health", timeout=5)
        return resp.status_code == 200
    except Exception:
        return False
