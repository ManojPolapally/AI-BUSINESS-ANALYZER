"""
database.py
-----------
Manages the SQLite connection and all database lifecycle operations.

Responsibilities:
- Provide a thread-safe connection to the SQLite database.
- Drop and recreate the dataset table when a new CSV is uploaded.
- Store and retrieve schema metadata.
- Execute validated SELECT queries.
"""

import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from backend.config import DATABASE_PATH, DATASET_TABLE

logger = logging.getLogger(__name__)


def _ensure_data_dir() -> None:
    """Create the data directory if it does not exist."""
    data_dir = os.path.dirname(DATABASE_PATH)
    if data_dir:
        os.makedirs(data_dir, exist_ok=True)


def get_connection() -> sqlite3.Connection:
    """
    Return a new SQLite connection with WAL mode and row_factory set.
    Callers are responsible for closing the connection.
    """
    _ensure_data_dir()
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


@contextmanager
def managed_connection():
    """Context manager that opens a connection and closes it on exit."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema metadata tables (created once on startup)
# ---------------------------------------------------------------------------

INIT_SQL = """
CREATE TABLE IF NOT EXISTS upload_history (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    filename     TEXT    NOT NULL,
    uploaded_at  TEXT    NOT NULL,
    row_count    INTEGER NOT NULL,
    column_count INTEGER NOT NULL,
    schema_json  TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS active_schema (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    schema_json  TEXT    NOT NULL,
    table_name   TEXT    NOT NULL,
    updated_at   TEXT    NOT NULL
);
"""


def init_db() -> None:
    """Initialise the metadata tables. Safe to call multiple times."""
    with managed_connection() as conn:
        conn.executescript(INIT_SQL)
        conn.commit()
    logger.info("Database initialised at %s", DATABASE_PATH)


# ---------------------------------------------------------------------------
# Dataset table lifecycle
# ---------------------------------------------------------------------------

def drop_dataset_table(conn: sqlite3.Connection) -> None:
    """Drop the active dataset table so a fresh one can be created."""
    conn.execute(f"DROP TABLE IF EXISTS {DATASET_TABLE};")
    logger.info("Dropped table '%s' (if it existed).", DATASET_TABLE)


def save_active_schema(
    conn: sqlite3.Connection,
    schema: dict[str, Any],
) -> None:
    """
    Upsert the single-row active_schema record.
    schema is a dict: { column_name: { dtype, sample_values, ... } }
    """
    conn.execute(
        """
        INSERT INTO active_schema (id, schema_json, table_name, updated_at)
        VALUES (1, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            schema_json = excluded.schema_json,
            table_name  = excluded.table_name,
            updated_at  = excluded.updated_at;
        """,
        (json.dumps(schema), DATASET_TABLE, datetime.utcnow().isoformat()),
    )


def record_upload_history(
    conn: sqlite3.Connection,
    filename: str,
    row_count: int,
    column_count: int,
    schema: dict[str, Any],
) -> None:
    """Append an entry to upload_history."""
    conn.execute(
        """
        INSERT INTO upload_history
            (filename, uploaded_at, row_count, column_count, schema_json)
        VALUES (?, ?, ?, ?, ?);
        """,
        (
            filename,
            datetime.utcnow().isoformat(),
            row_count,
            column_count,
            json.dumps(schema),
        ),
    )


# ---------------------------------------------------------------------------
# Schema retrieval
# ---------------------------------------------------------------------------

def get_active_schema() -> dict[str, Any] | None:
    """
    Return the currently active schema dict, or None if no dataset has been
    uploaded yet.
    """
    with managed_connection() as conn:
        row = conn.execute(
            "SELECT schema_json FROM active_schema WHERE id = 1;"
        ).fetchone()
    if row is None:
        return None
    return json.loads(row["schema_json"])


def get_column_names() -> list[str]:
    """Return column names for the active dataset, or empty list."""
    schema = get_active_schema()
    if schema is None:
        return []
    return list(schema.keys())


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

def execute_select(sql: str) -> list[dict[str, Any]]:
    """
    Execute a pre-validated SELECT statement and return rows as a list of
    plain dicts.  Raises sqlite3.Error on any database error.
    """
    with managed_connection() as conn:
        cursor = conn.execute(sql)
        rows = cursor.fetchall()
    return [dict(row) for row in rows]
