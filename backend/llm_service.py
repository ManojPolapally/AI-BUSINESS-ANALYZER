"""
llm_service.py
--------------
Wraps the Google Gemini API using the current google-genai SDK.

Responsibilities:
- Build the system prompt with strict anti-hallucination rules.
- Inject the current dataset schema into every call.
- Force JSON-only output.
- Parse and validate the structured response.
- Provide helpers for SQL generation, chart config, insights,
  and business recommendations so each LangGraph node stays thin.
"""

import json
import logging
import re
import time
from typing import Any

from google import genai
from google.genai import types

from backend.config import GEMINI_API_KEY, GEMINI_MODEL

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Client initialisation
# ---------------------------------------------------------------------------

if not GEMINI_API_KEY or GEMINI_API_KEY == "your_gemini_api_key_here":
    raise EnvironmentError(
        "GEMINI_API_KEY is not set. "
        "Open the .env file and replace 'your_gemini_api_key_here' "
        "with your actual Gemini API key from https://aistudio.google.com/app/apikey"
    )

_client = genai.Client(api_key=GEMINI_API_KEY)

_GENERATION_CONFIG = types.GenerateContentConfig(
    response_mime_type="application/json",
    temperature=0.2,       # low temperature â†’ deterministic, factual
    top_p=0.8,
    max_output_tokens=2048,
)

# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------

_SYSTEM_RULES = """
You are a precise business intelligence assistant.

STRICT RULES â€” NEVER BREAK THESE:
1. Use ONLY the columns that appear in the DATASET SCHEMA provided.
2. Never invent, guess, or assume columns or table names.
3. The SQLite table name is always: dataset
4. If the user's question cannot be answered with the available columns,
   return exactly: {"error": "UNSUPPORTED_QUERY"}
5. All SQL must be valid SQLite SELECT syntax.
6. Never use DROP, INSERT, UPDATE, DELETE, ALTER, CREATE, or PRAGMA.
7. Do not fabricate statistics, percentages, or numbers.
8. All insights and recommendations must be derived ONLY from the query
   results provided â€” never from prior knowledge.
9. Your entire response MUST be a single valid JSON object. No markdown,
   no explanation text outside the JSON.
""".strip()

_SQL_CHART_TEMPLATE = """
{system_rules}

DATASET SCHEMA:
{schema_str}

USER QUESTION:
{question}

Respond with a JSON object in EXACTLY this format (no other keys):
{{
  "sql_query": "<valid SQLite SELECT statement>",
  "chart_type": "<bar|line|pie|scatter|histogram|heatmap|table>",
  "x_axis": "<column name for x axis, or empty string>",
  "y_axis": "<column name for y axis, or empty string>",
  "title": "<descriptive chart title>"
}}
""".strip()

_INSIGHT_TEMPLATE = """
{system_rules}

DATASET SCHEMA:
{schema_str}

ORIGINAL QUESTION:
{question}

QUERY RESULTS (first 50 rows shown):
{results_str}

Respond with a JSON object in EXACTLY this format:
{{
  "insights": [
    "<concise factual observation 1>",
    "<concise factual observation 2>",
    "<concise factual observation 3>"
  ],
  "business_recommendations": [
    "<actionable business recommendation 1>",
    "<actionable business recommendation 2>",
    "<actionable business recommendation 3>"
  ]
}}

Base EVERY item only on the query results above.
""".strip()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _schema_to_string(schema: dict[str, Any]) -> str:
    """Convert schema dict to a compact human-readable string for the prompt."""
    lines = []
    for col, meta in schema.items():
        samples = ", ".join(str(s) for s in meta.get("sample_values", []))
        lines.append(
            f"  - {col} ({meta['dtype']}) | "
            f"nulls={meta['null_count']} | "
            f"unique={meta['unique_count']} | "
            f"samples=[{samples}]"
        )
    return "\n".join(lines)


def _call_gemini(prompt: str) -> dict[str, Any]:
    """
    Send a prompt to Gemini and return the parsed JSON response.
    Retries up to 3 times on 429 rate-limit errors with backoff.

    Raises:
        ValueError   - model returned invalid JSON or UNSUPPORTED_QUERY.
        RuntimeError - Gemini API error.
    """
    max_retries = 3
    response = None
    for attempt in range(max_retries):
        try:
            response = _client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=_GENERATION_CONFIG,
            )
            break
        except Exception as exc:
            err_str = str(exc)
            if ("429" in err_str or "RESOURCE_EXHAUSTED" in err_str) and attempt < max_retries - 1:
                wait = 45 * (attempt + 1)
                logger.warning(
                    "Rate limited by Gemini. Retrying in %ds (attempt %d/%d)...",
                    wait, attempt + 1, max_retries,
                )
                time.sleep(wait)
                continue
            raise RuntimeError(f"Gemini API error: {exc}") from exc

    raw_text = response.text.strip()

    # Strip any accidental markdown fences the model might add
    raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text, flags=re.IGNORECASE)
    raw_text = re.sub(r"\s*```$", "", raw_text)

    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        logger.error("Gemini returned non-JSON: %s", raw_text)
        raise ValueError(f"Model returned non-JSON output: {exc}") from exc

    if parsed.get("error") == "UNSUPPORTED_QUERY":
        raise ValueError("UNSUPPORTED_QUERY")

    return parsed


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_sql_and_chart_config(
    question: str,
    schema: dict[str, Any],
) -> dict[str, Any]:
    """
    Ask Gemini to translate a natural language question into:
    - a SQLite SELECT query
    - chart_type, x_axis, y_axis, title

    Returns the parsed JSON dict.
    Raises ValueError on UNSUPPORTED_QUERY or bad output.
    """
    schema_str = _schema_to_string(schema)
    prompt = _SQL_CHART_TEMPLATE.format(
        system_rules=_SYSTEM_RULES,
        schema_str=schema_str,
        question=question,
    )
    result = _call_gemini(prompt)

    required = {"sql_query", "chart_type", "x_axis", "y_axis", "title"}
    missing = required - set(result.keys())
    if missing:
        raise ValueError(f"Model response missing keys: {missing}")

    return result


def generate_insights_and_recommendations(
    question: str,
    schema: dict[str, Any],
    query_results: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Ask Gemini to generate insights and business recommendations based on
    actual query results.

    Returns dict with 'insights' and 'business_recommendations' lists.
    """
    schema_str = _schema_to_string(schema)
    results_preview = query_results[:50]
    results_str = json.dumps(results_preview, indent=2, default=str)

    prompt = _INSIGHT_TEMPLATE.format(
        system_rules=_SYSTEM_RULES,
        schema_str=schema_str,
        question=question,
        results_str=results_str,
    )
    result = _call_gemini(prompt)

    if "insights" not in result or "business_recommendations" not in result:
        raise ValueError(
            "Model response missing 'insights' or 'business_recommendations'."
        )

    return result

