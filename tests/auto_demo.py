"""
auto_demo.py — Fully automated end-to-end demo.
Uploads CSV, fires 5 questions, prints results with charts stats.
"""
import csv
import io
import json
import os
import requests

BASE      = "http://localhost:8000"
GROQ_KEY  = os.environ.get("GROQ_API_KEY", "")

DEMO_CSV = (
    "date,region,product,revenue,units_sold,discount_pct\n"
    "2024-01,East,Laptop,120000,10,5\n"
    "2024-01,West,Phone,90000,30,10\n"
    "2024-01,South,Tablet,70000,20,0\n"
    "2024-01,North,Laptop,85000,7,5\n"
    "2024-02,East,Phone,65000,22,10\n"
    "2024-02,West,Laptop,110000,9,5\n"
    "2024-02,South,Phone,80000,27,10\n"
    "2024-02,North,Tablet,60000,18,0\n"
    "2024-03,East,Tablet,95000,28,0\n"
    "2024-03,West,Phone,75000,25,10\n"
    "2024-03,South,Laptop,130000,11,5\n"
    "2024-03,North,Phone,55000,19,10\n"
    "2024-04,East,Laptop,140000,12,5\n"
    "2024-04,West,Tablet,85000,24,0\n"
    "2024-04,South,Phone,92000,31,10\n"
    "2024-04,North,Laptop,78000,6,5\n"
    "2024-05,East,Phone,88000,29,10\n"
    "2024-05,West,Laptop,125000,10,5\n"
    "2024-05,South,Tablet,72000,21,0\n"
    "2024-05,North,Phone,63000,22,10\n"
    "2024-06,East,Laptop,155000,13,5\n"
    "2024-06,West,Phone,95000,32,10\n"
    "2024-06,South,Tablet,80000,23,0\n"
    "2024-06,North,Laptop,102000,9,5\n"
)

QUESTIONS = [
    ("Show total revenue by region",                             "bar"),
    ("Which product generates the most total revenue?",         "bar"),
    ("Show the monthly revenue trend over time",                "line"),
    ("Compare units sold by region and product",                "bar"),
    ("What is the average discount percentage by product?",     "bar"),
]

SEP = "=" * 65

def run_query(question):
    r = requests.post(
        f"{BASE}/generate-dashboard",
        json={"question": question, "api_key": "", "openrouter_key": "", "groq_key": GROQ_KEY},
        timeout=120,
    )
    return r.json()


# ── UPLOAD ──────────────────────────────────────────────────────────────────
print(SEP)
print("  STEP 1: UPLOADING DATASET")
print(SEP)
r = requests.post(f"{BASE}/upload-csv",
                  files={"file": ("sales_demo.csv", DEMO_CSV.encode(), "text/csv")},
                  timeout=15)
d = r.json()
print(f"  File    : sales_demo.csv")
print(f"  Rows    : {d['row_count']}")
print(f"  Columns : {d['column_count']}  →  {d['columns']}")
print(f"  Message : {d['message']}")

# ── QUERIES ─────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  STEP 2: RUNNING 5 ANALYTICAL QUERIES  (LLM: Groq llama-3.3-70b)")
print(SEP)

all_ok = True
for i, (question, expected_chart) in enumerate(QUESTIONS, 1):
    print(f"\n[{i}/5] {question}")
    print("-" * 55)
    try:
        res = run_query(question)
    except Exception as e:
        print(f"  ERROR  : {e}")
        all_ok = False
        continue

    status = res.get("status")
    sql    = res.get("sql_query", "")
    ctype  = res.get("chart_type", "")
    cfig   = res.get("chart_figure") or {}
    insights = res.get("insights", [])
    recs     = res.get("business_recommendations", [])

    status_icon = "✅" if status == "success" else "❌"
    print(f"  {status_icon} Status      : {status}")
    print(f"  📊 Chart type  : {ctype}  (expected ~{expected_chart})")
    print(f"  🗄  SQL         : {sql}")

    if cfig:
        traces = len(cfig.get("data", []))
        title  = cfig.get("layout", {}).get("title", {})
        title_text = title.get("text", "") if isinstance(title, dict) else str(title)
        print(f"  📈 Chart data  : {traces} trace(s)  |  title: \"{title_text}\"")
        # Show first data trace summary
        if cfig["data"]:
            t = cfig["data"][0]
            x_raw = t.get("x") or t.get("labels") or []
            y_raw = t.get("y") or t.get("values") or []
            x_vals = list(x_raw) if not isinstance(x_raw, list) else x_raw
            y_vals = list(y_raw) if not isinstance(y_raw, list) else y_raw
            rows = list(zip(x_vals[:4], y_vals[:4]))
            print(f"  📋 Data sample : {rows}{'…' if len(x_vals) > 4 else ''}")

    if insights:
        print(f"  💡 Insights    :")
        for ins in insights[:3]:
            print(f"       • {ins}")

    if recs:
        print(f"  📌 Recs        :")
        for rec in recs[:2]:
            print(f"       • {rec}")

    if status != "success":
        all_ok = False
        if res.get("error"):
            print(f"  ⚠️  Error: {res['error']}")


# ── RESULT ──────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
if all_ok:
    print("  🎉  ALL 5 QUERIES SUCCEEDED — DASHBOARD FULLY OPERATIONAL")
else:
    print("  ⚠️  SOME QUERIES FAILED — CHECK ABOVE")
print(f"{SEP}\n")
print("  Open your browser at  http://localhost:8501")
print("  Enter Groq key in the sidebar and upload sales_demo.csv")
print(f"{SEP}")
