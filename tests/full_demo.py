"""
full_demo.py
------------
Autonomous end-to-end demo:
  1. Uploads a 24-row, 8-column sales dataset
  2. Fetches the active schema
  3. Runs 7 distinct analytical queries (no API key → pandas fallback)
  4. Prints SQL, chart type, data sample, insights & recommendations for each
  5. Prints a final pass/fail summary
"""

import io
import time

import requests

BASE = "http://localhost:8000"
DIV  = "=" * 70

# ── EMBEDDED DATASET ─────────────────────────────────────────────────────────
CSV = (
    "date,region,product,category,revenue,units_sold,discount_pct,profit\n"
    "2024-01,North,Laptop,Electronics,150000,12,5.0,45000\n"
    "2024-01,South,Phone,Electronics,90000,45,10.0,18000\n"
    "2024-01,East,Tablet,Electronics,60000,30,0.0,15000\n"
    "2024-01,West,Laptop,Electronics,120000,10,5.0,36000\n"
    "2024-02,North,Phone,Electronics,85000,42,8.0,17000\n"
    "2024-02,South,Tablet,Electronics,55000,28,0.0,13750\n"
    "2024-02,East,Laptop,Electronics,140000,11,5.0,42000\n"
    "2024-02,West,Phone,Electronics,95000,48,10.0,19000\n"
    "2024-03,North,Tablet,Electronics,70000,35,0.0,17500\n"
    "2024-03,South,Laptop,Electronics,160000,13,5.0,48000\n"
    "2024-03,East,Phone,Electronics,88000,44,8.0,17600\n"
    "2024-03,West,Tablet,Electronics,65000,32,0.0,16250\n"
    "2024-04,North,Laptop,Electronics,175000,14,5.0,52500\n"
    "2024-04,South,Phone,Electronics,92000,46,10.0,18400\n"
    "2024-04,East,Tablet,Electronics,72000,36,0.0,18000\n"
    "2024-04,West,Laptop,Electronics,130000,11,5.0,39000\n"
    "2024-05,North,Phone,Electronics,98000,49,8.0,19600\n"
    "2024-05,South,Tablet,Electronics,68000,34,0.0,17000\n"
    "2024-05,East,Laptop,Electronics,155000,13,5.0,46500\n"
    "2024-05,West,Phone,Electronics,102000,51,10.0,20400\n"
    "2024-06,North,Tablet,Electronics,75000,37,0.0,18750\n"
    "2024-06,South,Laptop,Electronics,180000,15,5.0,54000\n"
    "2024-06,East,Phone,Electronics,95000,48,8.0,19000\n"
    "2024-06,West,Tablet,Electronics,70000,35,0.0,17500\n"
)

QUESTIONS = [
    ("Total revenue by region",                      "bar"),
    ("Monthly revenue trend over time",              "line"),
    ("Which product has the highest total revenue?", "bar"),
    ("Compare units sold by product",                "bar"),
    ("Average discount percentage by product",       "bar"),
    ("Show profit by region",                        "bar"),
    ("Top region-product combinations by revenue",   "bar"),
]


# ── HELPERS ───────────────────────────────────────────────────────────────────
def print_section(title: str) -> None:
    print(f"\n{DIV}")
    print(f"  {title}")
    print(DIV)


def chart_sample(cfig: dict) -> tuple[str, list]:
    """Return (chart title, first-4-row data sample) from a Plotly figure dict."""
    traces = cfig.get("data", [])
    if not traces:
        return "", []
    t = traces[0]
    x_raw = t.get("x") or t.get("labels") or []
    y_raw = t.get("y") or t.get("values") or []
    x_vals = list(x_raw)
    y_vals = list(y_raw)
    sample = list(zip(x_vals[:4], y_vals[:4]))
    title_obj = cfig.get("layout", {}).get("title", {})
    title_text = title_obj.get("text", "") if isinstance(title_obj, dict) else str(title_obj)
    return title_text, sample


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main() -> None:
    print(DIV)
    print("  AI BUSINESS ANALYSER — FULL AUTONOMOUS DEMO")
    print("  (pandas fallback — no API key required)")
    print(DIV)

    # ── STEP 1: Upload ────────────────────────────────────────────────────
    print("\n[STEP 1]  Uploading embedded sales_2024 dataset (24 rows, 8 cols)")
    r = requests.post(
        f"{BASE}/upload-csv",
        files={"file": ("sales_2024.csv", io.BytesIO(CSV.encode()), "text/csv")},
        timeout=30,
    )
    info = r.json()
    print(f"  HTTP {r.status_code}")
    print(f"  Message  : {info.get('message')}")
    print(f"  Columns  : {info.get('columns')}")
    assert r.status_code == 200, f"Upload failed: {info}"

    # ── STEP 2: Schema ────────────────────────────────────────────────────
    print("\n[STEP 2]  Active schema")
    s = requests.get(f"{BASE}/schema", timeout=10).json()
    print(f"  Table    : {s.get('table_name')}")
    for col, meta in s.get("columns", {}).items():
        sample_vals = meta.get("sample_values", [])[:3]
        print(f"    {col:22s}  dtype={meta.get('dtype','?'):10s}  sample={sample_vals}")

    # ── STEP 3: Seven queries ─────────────────────────────────────────────
    print(f"\n[STEP 3]  Running {len(QUESTIONS)} analytical queries")

    results: list[tuple[str, bool]] = []

    for idx, (question, expected_chart) in enumerate(QUESTIONS, 1):
        print(f"\n  {'─'*66}")
        print(f"  [{idx}/{len(QUESTIONS)}]  {question}")
        print(f"  {'─'*66}")

        resp = requests.post(
            f"{BASE}/generate-dashboard",
            json={
                "question": question,
                "api_key": "",
                "openrouter_key": "",
                "groq_key": "",
            },
            timeout=60,
        ).json()

        status   = resp.get("status", "error")
        ctype    = resp.get("chart_type", "N/A")
        sql      = resp.get("sql_query", "")
        insights = resp.get("insights", [])
        recs     = resp.get("business_recommendations", [])
        cfig     = resp.get("chart_figure") or {}

        icon = "✅" if status == "success" else "❌"
        match = "✓" if expected_chart in (ctype or "") else "~"
        print(f"  {icon} Status   : {status}")
        print(f"  📊 Chart    : {ctype}  {match}  (expected {expected_chart})")

        if sql:
            display_sql = sql[:120] + ("…" if len(sql) > 120 else "")
            print(f"  🗄  SQL      : {display_sql}")

        title_text, sample = chart_sample(cfig)
        if title_text:
            print(f"  📈 Title    : {title_text}")
        if sample:
            sfmt = "  ".join(f"{x!r}→{y!r}" for x, y in sample)
            print(f"  📋 Data     : {sfmt}{'  …' if len(list((cfig.get('data') or [{}])[0].get('x') or [])) > 4 else ''}")

        if insights:
            print(f"  💡 Insights :")
            for ins in insights[:3]:
                print(f"       • {ins}")

        if recs:
            print(f"  📌 Recs     :")
            for rec in recs[:2]:
                print(f"       • {rec}")

        results.append((question, status == "success"))
        time.sleep(0.25)

    # ── STEP 4: Follow-up query (tests /follow-up endpoint) ──────────────
    print(f"\n  {'─'*66}")
    print(f"  [Follow-up endpoint test]  Which region had the highest profit?")
    print(f"  {'─'*66}")
    fu = requests.post(
        f"{BASE}/follow-up",
        json={"question": "Which region had the highest profit?",
              "api_key": "", "openrouter_key": "", "groq_key": ""},
        timeout=60,
    ).json()
    fu_status = fu.get("status", "error")
    fu_icon   = "✅" if fu_status == "success" else "❌"
    print(f"  {fu_icon} Status   : {fu_status}")
    print(f"  📊 Chart    : {fu.get('chart_type', 'N/A')}")
    fu_ins = fu.get("insights", [])
    if fu_ins:
        print(f"  💡 Top insight: {fu_ins[0]}")
    results.append(("Follow-up: highest profit region", fu_status == "success"))

    # ── STEP 5: Health check ──────────────────────────────────────────────
    hc = requests.get(f"{BASE}/health", timeout=5).json()
    print(f"\n[STEP 4]  Health check  →  {hc}")

    # ── FINAL SUMMARY ─────────────────────────────────────────────────────
    print_section("DEMO RESULTS SUMMARY")
    all_ok = True
    for q, ok in results:
        icon = "✅" if ok else "❌"
        print(f"  {icon}  {q}")
        if not ok:
            all_ok = False

    print(f"\n{DIV}")
    if all_ok:
        print(f"  🎉  ALL {len(results)} QUERIES SUCCEEDED — DASHBOARD FULLY OPERATIONAL")
    else:
        failed = [q for q, ok in results if not ok]
        print(f"  ⚠️  {len(failed)} failed: {failed}")
    print(DIV)
    print(f"  Frontend UI  →  http://localhost:8501")
    print(f"  Upload sales_2024.csv in the sidebar to explore interactively.")
    print(DIV)


if __name__ == "__main__":
    main()
