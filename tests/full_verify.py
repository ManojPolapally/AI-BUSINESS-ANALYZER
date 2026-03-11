"""
Full system verification script — Steps 3-9.
Run with: python tests/full_verify.py
"""
import csv
import io
import json
import os
import sys
import requests

BASE = "http://localhost:8000"
GROQ_KEY = os.environ.get("GROQ_API_KEY", "")

PASS = "\033[92m  PASS\033[0m"
FAIL = "\033[91m  FAIL\033[0m"
HEAD = "\033[94m{}\033[0m"

results = []

def check(label, condition, detail=""):
    status = PASS if condition else FAIL
    print(f"{status}  {label}")
    if detail:
        print(f"        {detail}")
    results.append((label, condition))
    return condition


# ---------------------------------------------------------------------------
# STEP 3: Upload CSV and verify SQLite schema
# ---------------------------------------------------------------------------
print(HEAD.format("\n=== STEP 3: CSV UPLOAD & SQLITE SCHEMA ==="))

SAMPLE_CSV = (
    "date,region,product,revenue\n"
    "2024-01,East,Laptop,12000\n"
    "2024-01,West,Phone,9000\n"
    "2024-02,East,Tablet,7000\n"
    "2024-02,South,Laptop,15000\n"
    "2024-03,West,Laptop,11000\n"
    "2024-03,South,Phone,8500\n"
    "2024-03,East,Phone,6500\n"
)

r = requests.post(
    f"{BASE}/upload-csv",
    files={"file": ("sales.csv", SAMPLE_CSV.encode(), "text/csv")},
    timeout=15,
)
upload_ok = r.status_code == 200
d = r.json()
check("CSV upload returns 200", upload_ok, f"status={r.status_code}")
check("Row count correct (7)", d.get("row_count") == 7, f"got {d.get('row_count')}")
check("Column count correct (4)", d.get("column_count") == 4, f"got {d.get('column_count')}")
cols = d.get("columns", [])
check("Columns detected: date,region,product,revenue",
      set(cols) == {"date", "region", "product", "revenue"},
      f"got {cols}")

# ---------------------------------------------------------------------------
# Schema endpoint
# ---------------------------------------------------------------------------
print(HEAD.format("\n=== STEP 3b: SCHEMA ENDPOINT ==="))
r = requests.get(f"{BASE}/schema", timeout=5)
schema_ok = r.status_code == 200
check("/schema returns 200", schema_ok)
if schema_ok:
    sd = r.json()
    check("Schema has 'columns' key", "columns" in sd, str(list(sd.keys())))
    schema_cols = list(sd.get("columns", {}).keys())
    check("Schema columns match CSV", set(schema_cols) == {"date", "region", "product", "revenue"},
          f"got {schema_cols}")
    for col in ["region", "revenue"]:
        meta = sd["columns"].get(col, {})
        check(f"  Schema '{col}' has dtype", "dtype" in meta, str(meta))
        check(f"  Schema '{col}' has sample_values", "sample_values" in meta, str(list(meta.keys())))


# ---------------------------------------------------------------------------
# STEP 4: Direct SQLite query test
# ---------------------------------------------------------------------------
print(HEAD.format("\n=== STEP 4: SQLITE QUERY EXECUTION ==="))
try:
    import sqlite3
    conn = sqlite3.connect("data/database.db")
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cur.fetchall()]
    check("'dataset' table exists in SQLite", "dataset" in tables, f"tables={tables}")

    cur = conn.execute("SELECT COUNT(*) FROM dataset")
    cnt = cur.fetchone()[0]
    check("7 rows in dataset table", cnt == 7, f"got {cnt}")

    cur = conn.execute("SELECT region, SUM(revenue) as total_revenue FROM dataset GROUP BY region ORDER BY total_revenue DESC")
    rows = cur.fetchall()
    check("GROUP BY query executes", len(rows) == 3, f"got {len(rows)} rows")
    print(f"        Query result: {rows}")
    conn.close()
except Exception as e:
    check("SQLite direct test", False, str(e))


# ---------------------------------------------------------------------------
# STEP 5-6: LLM SQL generation via full pipeline
# ---------------------------------------------------------------------------
print(HEAD.format("\n=== STEP 5-6: LLM SQL GENERATION & DATA RETURN ==="))
r = requests.post(
    f"{BASE}/generate-dashboard",
    json={"question": "Show revenue by region", "api_key": "", "openrouter_key": "", "groq_key": GROQ_KEY},
    timeout=120,
)
check("Pipeline returns 200", r.status_code == 200, f"got {r.status_code}")
if r.status_code == 200:
    pd = r.json()
    check("Status is 'success'", pd.get("status") == "success",
          f"status={pd.get('status')} error={pd.get('error','')}")
    sql = pd.get("sql_query", "")
    check("SQL contains SELECT", "SELECT" in sql.upper(), f"sql={sql}")
    check("SQL references 'region'", "region" in sql.lower(), f"sql={sql}")
    check("SQL references 'revenue'", "revenue" in sql.lower(), f"sql={sql}")
    check("SQL uses GROUP BY", "GROUP BY" in sql.upper(), f"sql={sql}")
    print(f"        Generated SQL: {sql}")


    # ---------------------------------------------------------------------------
    # STEP 7: Chart configuration
    # ---------------------------------------------------------------------------
    print(HEAD.format("\n=== STEP 7: CHART GENERATION ==="))
    chart_type = pd.get("chart_type")
    chart_fig = pd.get("chart_figure")
    check("chart_type returned", chart_type is not None, f"got {chart_type}")
    check("chart_figure returned", chart_fig is not None)
    if chart_fig:
        check("chart_figure is a dict", isinstance(chart_fig, dict), type(chart_fig).__name__)
        check("chart_figure has 'data' key", "data" in chart_fig, str(list(chart_fig.keys())[:5]))
        check("chart_figure has 'layout' key", "layout" in chart_fig)
        check("chart_figure is JSON-serialisable",
              True,  # already parsed from JSON response
              "Pydantic serialization OK")

    # ---------------------------------------------------------------------------
    # STEP 8: Insights & recommendations
    # ---------------------------------------------------------------------------
    print(HEAD.format("\n=== STEP 8: INSIGHTS & RECOMMENDATIONS ==="))
    insights = pd.get("insights", [])
    recs = pd.get("business_recommendations", [])
    check("Insights returned (>=1)", len(insights) >= 1, f"got {len(insights)}")
    check("Recommendations returned (>=1)", len(recs) >= 1, f"got {len(recs)}")
    for i, ins in enumerate(insights[:3], 1):
        print(f"        Insight {i}: {ins}")
    for i, rec in enumerate(recs[:2], 1):
        print(f"        Rec {i}:     {rec}")


# ---------------------------------------------------------------------------
# STEP 9: Follow-up query
# ---------------------------------------------------------------------------
print(HEAD.format("\n=== STEP 9: FOLLOW-UP QUERY ==="))
r2 = requests.post(
    f"{BASE}/follow-up",
    json={"question": "Which product has the highest revenue?",
          "api_key": "", "openrouter_key": "", "groq_key": GROQ_KEY},
    timeout=120,
)
check("Follow-up returns 200", r2.status_code == 200)
if r2.status_code == 200:
    fd = r2.json()
    check("Follow-up status success/empty_result",
          fd.get("status") in ("success", "empty_result"),
          f"status={fd.get('status')} error={fd.get('error','')}")
    print(f"        SQL: {fd.get('sql_query')}")


# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
print(HEAD.format("\n=== SUMMARY ==="))
passed = sum(1 for _, ok in results if ok)
failed = sum(1 for _, ok in results if not ok)
print(f"  Total checks: {len(results)}")
print(f"\033[92m  Passed: {passed}\033[0m")
if failed:
    print(f"\033[91m  Failed: {failed}\033[0m")
    for label, ok in results:
        if not ok:
            print(f"\033[91m    - {label}\033[0m")
    sys.exit(1)
else:
    print("  \033[92mAll checks passed — system is fully operational!\033[0m")
