"""
app.py
------
Streamlit entry point for the AI Business Analyser.

Page Layout:
┌─────────────────────────────────────────────────────────────────────┐
│  SIDEBAR                                                            │
│  • CSV Upload  • Detected Columns  • Schema  • Session Controls     │
├─────────────────────────────────────────────────────────────────────┤
│  TOP    │ Prompt input + Analyse button                             │
├────────────────────────────────┬────────────────────────────────────┤
│  MAIN (chart)                  │  RIGHT PANEL                       │
│  Interactive Plotly chart      │  AI Insights                       │
│  SQL expander                  │  Business Recommendations          │
├─────────────────────────────────────────────────────────────────────┤
│  BOTTOM │ Suggested follow-up query buttons                         │
└─────────────────────────────────────────────────────────────────────┘
"""

import streamlit as st

from frontend.components.chat_interface import render_prompt_input
from frontend.components.dashboard_view import render_all_charts
from frontend.components.followup_panel import render_followup_suggestions
from frontend.components.sidebar import render_sidebar
from frontend.utils.api_client import health_check
from frontend.utils.styles import apply as _apply_styles, page_header

# ---------------------------------------------------------------------------
# Page config — MUST be the very first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="AI Business Analyser",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Global CSS (shared across all pages)
# ---------------------------------------------------------------------------
_apply_styles()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    render_sidebar()

# ---------------------------------------------------------------------------
# Backend health check — stop early if API is unreachable
# ---------------------------------------------------------------------------
if not health_check():
    st.error(
        "⚠️ **Cannot connect to the backend API.**  \n"
        "Make sure the FastAPI server is running:  \n"
        "```\nuvicorn backend.main:app --reload --port 8000\n```"
    )
    st.stop()

# ---------------------------------------------------------------------------
# Page header
# ---------------------------------------------------------------------------
page_header(
    "📊",
    "Conversational Business Intelligence",
    "Upload a CSV dataset and ask questions in plain English to generate "
    "interactive dashboards, AI insights, and business recommendations.",
    "#6366f1",
    "#7c3aed",
)
st.divider()

# ---------------------------------------------------------------------------
# TOP SECTION — Prompt input
# ---------------------------------------------------------------------------
render_prompt_input()

# ---------------------------------------------------------------------------
# MAIN SECTION — Charts with inline insights
# ---------------------------------------------------------------------------
has_results = any(
    e["response"].get("status") == "success"
    for e in st.session_state.get("history", [])
)

if has_results:
    st.divider()
    st.markdown("### 📈 Interactive Dashboard")
    render_all_charts()

elif st.session_state.get("dataset_loaded"):
    st.markdown(
        """
        <div style="
            text-align:center; padding:70px 20px;
            background: linear-gradient(135deg, #eef2ff, #f5f3ff);
            border-radius: 16px; margin-top: 20px;
            border: 1.5px dashed #c7d2fe;
        ">
            <span style='font-size:3.5rem;'>💬</span>
            <p style='color:#6366f1; font-size:1.1rem; font-weight:600; margin:12px 0 4px;'>
                Ready to analyse!
            </p>
            <p style='color:#818cf8; font-size:0.95rem; margin:0;'>
                Type a question above to generate your first dashboard.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        """
        <div style="
            text-align:center; padding:70px 20px;
            background: linear-gradient(135deg, #f8f9ff, #f0f4ff);
            border-radius: 16px; margin-top: 20px;
            border: 1.5px dashed #c7d2fe;
        ">
            <span style='font-size:3.5rem;'>📂</span>
            <p style='color:#6366f1; font-size:1.1rem; font-weight:600; margin:12px 0 4px;'>
                Get started
            </p>
            <p style='color:#818cf8; font-size:0.95rem; margin:0;'>
                Upload a CSV file from the sidebar to begin.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# BOTTOM SECTION — Suggested follow-up queries
# ---------------------------------------------------------------------------
render_followup_suggestions()
