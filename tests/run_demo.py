"""Quick live demo — runs 3 queries through the full pipeline."""
import json
import os
import requests

GROQ_KEY = os.environ.get("GROQ_API_KEY", "")
BASE = "http://localhost:8000"

QUESTIONS = [
    "What is the total revenue by channel?",
    "Which channel has the highest conversion rate?",
    "Show the trend of total sessions over time",
]

for question in QUESTIONS:
    print(f"\n{'='*60}")
    print(f"QUESTION: {question}")
    print("="*60)
    resp = requests.post(
        f"{BASE}/generate-dashboard",
        json={"question": question, "api_key": "", "openrouter_key": "", "groq_key": GROQ_KEY},
        timeout=120,
    )
    d = resp.json()
    print(f"Status     : {d.get('status')}")
    print(f"SQL        : {d.get('sql_query')}")
    print(f"Chart type : {d.get('chart_type')}")
    for i, ins in enumerate(d.get("insights", [])[:3], 1):
        print(f"Insight {i}  : {ins}")
    for i, rec in enumerate(d.get("business_recommendations", [])[:2], 1):
        print(f"Rec {i}      : {rec}")
    if d.get("error"):
        print(f"Error      : {d['error']}")

print("\nDemo complete.")
