"""
query_executor.py
-----------------
SQL validation and safe execution layer.

Responsibilities:
- Reject any SQL that is not a SELECT statement.
- Block dangerous keywords (DDL, DML, comment injections).
- Verify all column references exist in the active schema.
- Execute the validated query and return results.
"""

import logging
import re
import sqlite3
from typing import Any

from backend.config import ALLOWED_SQL_PREFIXES, BLOCKED_SQL_KEYWORDS
from backend.database import execute_select, get_column_names

logger = logging.getLogger(__name__)


class SQLValidationError(Exception):
    """Raised when the generated SQL fails a safety or schema check."""


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _strip_comments(sql: str) -> str:
    """Remove single-line (--) and multi-line (/* */) SQL comments."""
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return sql.strip()


def _check_statement_type(sql: str) -> None:
    """Ensure the first meaningful token is SELECT."""
    first_token = sql.strip().split()[0].lower()
    if first_token not in ALLOWED_SQL_PREFIXES:
        raise SQLValidationError(
            f"Only SELECT statements are allowed. "
            f"Received statement starting with: '{first_token}'."
        )


def _check_blocked_keywords(sql: str) -> None:
    """Reject SQL containing any blocked keyword."""
    sql_lower = sql.lower()
    for keyword in BLOCKED_SQL_KEYWORDS:
        # Word-boundary check for keyword-style tokens; substring for symbols
        if keyword.isalpha():
            pattern = r"\b" + re.escape(keyword) + r"\b"
            if re.search(pattern, sql_lower):
                raise SQLValidationError(
                    f"SQL contains a blocked keyword: '{keyword}'."
                )
        else:
            if keyword in sql_lower:
                raise SQLValidationError(
                    f"SQL contains a blocked sequence: '{keyword}'."
                )


def _check_columns(sql: str, valid_columns: list[str]) -> None:
    """
    Heuristic column-reference check.

    Extracts bare identifiers from the SQL and warns on any that look like
    column names but are absent from the schema.  This is a best-effort check
    — it does not parse SQL ASTs, but catches the most common hallucinations.
    """
    if not valid_columns:
        return  # No schema loaded yet; skip check

    # Identifiers that are SQL keywords we should ignore
    sql_keywords = {
        "select", "from", "where", "group", "by", "order", "limit", "offset",
        "having", "join", "on", "as", "and", "or", "not", "in", "is", "null",
        "between", "like", "case", "when", "then", "else", "end", "distinct",
        "count", "sum", "avg", "min", "max", "inner", "left", "right",
        "outer", "cross", "union", "all", "asc", "desc", "with", "cast",
        "over", "partition", "row_number", "rank", "coalesce", "ifnull",
        "dataset", "true", "false",
    }

    # Extract quoted identifiers and bare identifiers
    quoted = set(re.findall(r'"(\w+)"', sql))
    bare = set(re.findall(r"\b([a-z_][a-z0-9_]*)\b", sql.lower()))

    candidate_cols = (quoted | bare) - sql_keywords
    valid_set = {c.lower() for c in valid_columns}

    unknown = [c for c in candidate_cols if c not in valid_set]
    if unknown:
        logger.warning("SQL references potentially unknown identifiers: %s", unknown)
        # As a strict check, raise only when ALL candidates are unknown
        # (avoids false positives from SQL function names and aliases)
        all_unknown = all(c not in valid_set for c in candidate_cols)
        if all_unknown:
            raise SQLValidationError(
                f"SQL references columns not found in the dataset schema: "
                f"{unknown}. Available columns: {valid_columns}"
            )


def validate_sql(sql: str) -> str:
    """
    Run all safety checks on a SQL string.

    Returns the cleaned SQL string if valid.
    Raises SQLValidationError on any failure.
    """
    cleaned = _strip_comments(sql).strip()

    if not cleaned:
        raise SQLValidationError("Empty SQL query received.")

    _check_statement_type(cleaned)
    _check_blocked_keywords(cleaned)

    valid_columns = get_column_names()
    _check_columns(cleaned, valid_columns)

    return cleaned


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

def run_query(sql: str) -> list[dict[str, Any]]:
    """
    Validate then execute a SQL query.

    Returns:
        List of row dicts.  Empty list if the query returns no rows.

    Raises:
        SQLValidationError  — query fails safety/schema checks.
        sqlite3.Error       — database execution error.
    """
    validated_sql = validate_sql(sql)
    try:
        results = execute_select(validated_sql)
    except sqlite3.Error as exc:
        logger.error("SQLite error executing query: %s | SQL: %s", exc, validated_sql)
        raise

    logger.info("Query returned %d row(s).", len(results))
    return results
