"""
csv_loader.py
-------------
Handles CSV ingestion: validates the file, loads it with Pandas, writes it to
SQLite (replacing any previous dataset), extracts the schema, and records
upload history.
"""

import logging
from io import BytesIO
from typing import Any

import pandas as pd

from backend.config import DATASET_TABLE, MAX_CSV_SIZE_BYTES
from backend.database import (
    drop_dataset_table,
    managed_connection,
    record_upload_history,
    save_active_schema,
)

logger = logging.getLogger(__name__)

# Max number of sample values stored per column in the schema
_SAMPLE_SIZE = 5


class CSVUploadError(Exception):
    """Raised for user-facing upload validation errors."""


# ---------------------------------------------------------------------------
# Schema extraction
# ---------------------------------------------------------------------------

def _extract_schema(df: pd.DataFrame) -> dict[str, Any]:
    """
    Build a schema dict from a DataFrame.

    Returns:
        {
          "column_name": {
            "dtype": "int64",
            "sample_values": [1, 2, 3],
            "null_count": 0,
            "unique_count": 42
          }, ...
        }
    """
    schema: dict[str, Any] = {}
    for col in df.columns:
        series = df[col]
        non_null = series.dropna()
        samples = non_null.head(_SAMPLE_SIZE).tolist()
        # Ensure JSON-serialisable (numpy scalars → Python scalars)
        samples = [
            s.item() if hasattr(s, "item") else s for s in samples
        ]
        schema[col] = {
            "dtype": str(series.dtype),
            "sample_values": samples,
            "null_count": int(series.isna().sum()),
            "unique_count": int(series.nunique()),
        }
    return schema


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_csv_to_db(file_bytes: bytes, filename: str) -> dict[str, Any]:
    """
    Core ingestion routine.

    1. Validate size.
    2. Parse with Pandas (auto-detect separator).
    3. Sanitise column names (strip whitespace, lower-case).
    4. Drop the existing dataset table.
    5. Write the DataFrame to SQLite via to_sql.
    6. Extract and persist the schema.
    7. Record upload history.

    Returns a summary dict with row_count, column_count, and schema.

    Raises:
        CSVUploadError  — user-facing validation problems.
        Exception       — unexpected errors bubble up to the caller.
    """
    # --- Size guard -------------------------------------------------------
    if len(file_bytes) > MAX_CSV_SIZE_BYTES:
        raise CSVUploadError(
            f"File exceeds the maximum allowed size of "
            f"{MAX_CSV_SIZE_BYTES // (1024 * 1024)} MB."
        )

    # --- Parse CSV --------------------------------------------------------
    # Try common encodings in order; the first that succeeds wins.
    _ENCODINGS = ["utf-8-sig", "utf-8", "latin-1", "cp1252", "utf-16"]
    df = None
    last_exc: Exception | None = None
    for enc in _ENCODINGS:
        try:
            df = pd.read_csv(
                BytesIO(file_bytes),
                sep=None,          # auto-detect delimiter
                engine="python",
                encoding=enc,
                on_bad_lines="warn",
            )
            break
        except (UnicodeDecodeError, Exception) as exc:
            last_exc = exc
            continue
    if df is None:
        raise CSVUploadError(f"Could not parse CSV file: {last_exc}") from last_exc

    if df.empty:
        raise CSVUploadError("The uploaded CSV file contains no data rows.")

    if len(df.columns) < 1:
        raise CSVUploadError("The uploaded CSV file has no columns.")

    # --- Sanitise column names --------------------------------------------
    df.columns = [
        str(c).strip().lower().replace(" ", "_").replace("-", "_")
        for c in df.columns
    ]
    # Deduplicate column names (append _2, _3 … if needed)
    seen: dict[str, int] = {}
    new_cols: list[str] = []
    for col in df.columns:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 1
            new_cols.append(col)
    df.columns = new_cols

    # --- Write to SQLite --------------------------------------------------
    with managed_connection() as conn:
        drop_dataset_table(conn)

        # Use pandas to_sql — creates the table with inferred types
        df.to_sql(
            name=DATASET_TABLE,
            con=conn,
            if_exists="replace",   # extra safety (table already dropped)
            index=False,
            chunksize=500,
            method="multi",
        )

        schema = _extract_schema(df)
        save_active_schema(conn, schema)
        record_upload_history(
            conn,
            filename=filename,
            row_count=len(df),
            column_count=len(df.columns),
            schema=schema,
        )
        conn.commit()

    logger.info(
        "Loaded '%s': %d rows × %d cols → table '%s'.",
        filename, len(df), len(df.columns), DATASET_TABLE,
    )

    return {
        "filename": filename,
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "schema": schema,
    }
