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
from frontend.components.insight_panel import render_right_panel
from frontend.components.sidebar import render_sidebar
from frontend.utils.api_client import health_check

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
# Global CSS
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
        .stApp { background-color: #f7f9fc; }
        .block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
        hr { border-color: #e0e0e0; }

        /* Metric boxes */
        [data-testid="metric-container"] {
            background: #ffffff;
            border: 1px solid #e8ecf0;
            border-radius: 8px;
            padding: 8px;
        }

        /* Prompt input row */
        .stTextInput > div > div > input {
            font-size: 1rem;
            border-radius: 8px;
            border: 1.5px solid #cbd5e1;
        }
        .stTextInput > div > div > input:focus {
            border-color: #1f77b4;
            box-shadow: 0 0 0 2px rgba(31,119,180,0.15);
        }

        /* Follow-up buttons */
        div[data-testid="stButton"] button {
            border-radius: 20px;
            font-size: 0.85rem;
            text-align: left;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

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
st.markdown(
    """
    <h1 style='margin-bottom:2px; color:#1f2937;'>
        📊 Conversational Business Intelligence
    </h1>
    <p style='color:#6b7280; font-size:0.95rem; margin-top:0;'>
        Upload a CSV dataset and ask questions in plain English to generate
        interactive dashboards, AI insights, and business recommendations.
    </p>
    """,
    unsafe_allow_html=True,
)
st.divider()

# ---------------------------------------------------------------------------
# TOP SECTION — Prompt input
# ---------------------------------------------------------------------------
render_prompt_input()

# ---------------------------------------------------------------------------
# MAIN SECTION — Chart (left) + Insights panel (right)
# ---------------------------------------------------------------------------
has_results = any(
    e["response"].get("status") == "success"
    for e in st.session_state.get("history", [])
)

if has_results:
    st.divider()
    chart_col, insight_col = st.columns([3, 2], gap="large")

    with chart_col:
        st.markdown("### 📈 Interactive Dashboard")
        render_all_charts()

    with insight_col:
        st.markdown("### 🧠 AI Analysis")
        render_right_panel()

elif st.session_state.get("dataset_loaded"):
    st.markdown(
        """
        <div style="
            text-align:center; padding:60px 20px;
            color:#9ca3af; font-size:1.05rem;
        ">
            <span style='font-size:3rem;'>💬</span><br/>
            Type a question above to generate your first dashboard.
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        """
        <div style="
            text-align:center; padding:60px 20px;
            color:#9ca3af; font-size:1.05rem;
        ">
            <span style='font-size:3rem;'>📂</span><br/>
            Upload a CSV file from the sidebar to get started.
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# BOTTOM SECTION — Suggested follow-up queries
# ---------------------------------------------------------------------------
render_followup_suggestions()
