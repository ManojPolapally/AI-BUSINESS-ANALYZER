"""
styles.py
---------
Shared CSS injected into every Streamlit page for a consistent look.
Call `apply()` once near the top of each page (after set_page_config).
"""

import streamlit as st

_CSS = """
<style>
    /* ── Base & text readability ─────────────────────────────────────────── */
    .stApp {
        background: linear-gradient(135deg, #f0f4ff 0%, #fafbff 60%, #f5f0ff 100%) !important;
        min-height: 100vh;
        color: #1e2a45 !important;
    }
    .block-container {
        padding-top: 1.4rem;
        padding-bottom: 2.5rem;
        color: #1e2a45;
    }
    hr { border-color: #dde3f0; }

    /* Force visible text on all Streamlit markdown / text elements */
    .stMarkdown, .stMarkdown p, .stMarkdown li, .stMarkdown span,
    .stMarkdown h1, .stMarkdown h2, .stMarkdown h3, .stMarkdown h4,
    [data-testid="stMarkdownContainer"],
    [data-testid="stMarkdownContainer"] p,
    [data-testid="stMarkdownContainer"] span,
    [data-testid="stText"], label, .stSelectbox label,
    .stCheckbox label, .streamlit-expanderHeader {
        color: #1e2a45 !important;
    }

    /* ── Sidebar ─────────────────────────────────────────────────────────── */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #ffffff 0%, #f8f9ff 100%) !important;
        border-right: 1px solid #e4e8f5;
    }
    [data-testid="stSidebar"],
    [data-testid="stSidebar"] * { color: #1e2a45 !important; }

    /* ── Metric boxes ────────────────────────────────────────────────────── */
    [data-testid="metric-container"] {
        background: #ffffff !important;
        border: 1px solid #e0e7ff;
        border-radius: 12px;
        padding: 10px 14px;
        box-shadow: 0 2px 8px rgba(99,102,241,0.07);
        transition: box-shadow 0.2s ease, transform 0.2s ease;
    }
    [data-testid="metric-container"]:hover {
        box-shadow: 0 6px 18px rgba(99,102,241,0.15);
        transform: translateY(-2px);
    }
    [data-testid="metric-container"] * { color: #1e2a45 !important; }
    [data-testid="stMetricValue"] { color: #4f46e5 !important; font-weight: 700 !important; }
    [data-testid="stMetricLabel"] { color: #6366f1 !important; font-size: 0.84rem !important; }

    /* ── Text input ──────────────────────────────────────────────────────── */
    .stTextInput > div > div > input {
        font-size: 1rem;
        border-radius: 12px;
        border: 1.5px solid #c7d2fe;
        background: #ffffff !important;
        color: #1e2a45 !important;
        transition: border-color 0.2s ease, box-shadow 0.2s ease;
    }
    .stTextInput > div > div > input:focus {
        border-color: #6366f1;
        box-shadow: 0 0 0 3px rgba(99,102,241,0.18);
        background: #fefeff !important;
    }

    /* ── Primary button ──────────────────────────────────────────────────── */
    div[data-testid="stButton"] button[kind="primary"] {
        background: linear-gradient(135deg, #6366f1, #4f46e5) !important;
        border: none;
        border-radius: 12px;
        color: #ffffff !important;
        font-weight: 600;
        letter-spacing: 0.3px;
        box-shadow: 0 4px 14px rgba(99,102,241,0.35);
        transition: box-shadow 0.2s ease, transform 0.15s ease, filter 0.2s ease;
    }
    div[data-testid="stButton"] button[kind="primary"]:hover {
        box-shadow: 0 8px 22px rgba(99,102,241,0.45);
        transform: translateY(-2px);
        filter: brightness(1.08);
    }
    div[data-testid="stButton"] button[kind="primary"]:active {
        transform: translateY(0px);
        box-shadow: 0 2px 8px rgba(99,102,241,0.3);
    }

    /* ── Secondary / pill buttons ────────────────────────────────────────── */
    div[data-testid="stButton"] button:not([kind="primary"]) {
        border-radius: 20px;
        font-size: 0.85rem;
        text-align: left;
        background: #ffffff !important;
        border: 1.5px solid #c7d2fe;
        color: #4f46e5 !important;
        transition: background 0.2s ease, border-color 0.2s ease,
                    box-shadow 0.2s ease, transform 0.15s ease;
    }
    div[data-testid="stButton"] button:not([kind="primary"]):hover {
        background: #eef2ff !important;
        border-color: #6366f1;
        box-shadow: 0 4px 12px rgba(99,102,241,0.18);
        transform: translateY(-1px);
    }

    /* ── Expanders ───────────────────────────────────────────────────────── */
    details > summary {
        border-radius: 10px;
        padding: 6px 12px;
        transition: background 0.2s ease;
        cursor: pointer;
        color: #1e2a45 !important;
    }
    details > summary:hover { background: #eef2ff; }
    details[open] > summary { background: #e0e7ff; color: #4f46e5 !important; }
    .streamlit-expanderContent { background: #ffffff !important; color: #1e2a45 !important; }

    /* ── Alert cards ─────────────────────────────────────────────────────── */
    [data-testid="stAlert"] {
        border-radius: 10px !important;
        transition: box-shadow 0.2s ease;
    }
    [data-testid="stAlert"]:hover { box-shadow: 0 4px 14px rgba(0,0,0,0.08); }
    [data-testid="stAlert"] p { color: inherit !important; }

    /* ── File uploader ───────────────────────────────────────────────────── */
    [data-testid="stFileUploaderDropzone"] {
        border: 2px dashed #c7d2fe !important;
        border-radius: 12px !important;
        background: #f8f9ff !important;
        transition: border-color 0.2s ease, background 0.2s ease;
    }
    [data-testid="stFileUploaderDropzone"]:hover {
        border-color: #6366f1 !important;
        background: #eef2ff !important;
    }
    [data-testid="stFileUploaderDropzone"] * { color: #6366f1 !important; }

    /* ── Plotly chart card ───────────────────────────────────────────────── */
    [data-testid="stPlotlyChart"] {
        border-radius: 14px;
        overflow: hidden;
        box-shadow: 0 4px 20px rgba(99,102,241,0.10);
        transition: box-shadow 0.25s ease;
        background: #ffffff;
    }
    [data-testid="stPlotlyChart"]:hover {
        box-shadow: 0 8px 32px rgba(99,102,241,0.18);
    }

    /* ── Download button ─────────────────────────────────────────────────── */
    [data-testid="stDownloadButton"] button {
        background: #f0f4ff !important;
        border: 1.5px solid #c7d2fe;
        border-radius: 10px;
        color: #4f46e5 !important;
        font-size: 0.85rem;
        transition: background 0.2s ease, box-shadow 0.2s ease, transform 0.15s ease;
    }
    [data-testid="stDownloadButton"] button:hover {
        background: #e0e7ff !important;
        box-shadow: 0 4px 12px rgba(99,102,241,0.18);
        transform: translateY(-1px);
    }

    /* ── Dataframe table ─────────────────────────────────────────────────── */
    [data-testid="stDataFrame"] {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid #e0e7ff;
        box-shadow: 0 2px 12px rgba(99,102,241,0.07);
        background: #ffffff;
    }

    /* ── Tabs ────────────────────────────────────────────────────────────── */
    [data-testid="stTabs"] [data-testid="stTab"] {
        border-radius: 8px 8px 0 0;
        transition: background 0.2s ease;
        color: #1e2a45 !important;
    }
    [data-testid="stTabs"] [data-testid="stTab"]:hover { background: #eef2ff; }

    /* ── Spinner text ────────────────────────────────────────────────────── */
    [data-testid="stSpinner"] p { color: #6366f1 !important; }

    /* ── Scrollbar ───────────────────────────────────────────────────────── */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #f0f4ff; border-radius: 8px; }
    ::-webkit-scrollbar-thumb { background: #c7d2fe; border-radius: 8px; }
    ::-webkit-scrollbar-thumb:hover { background: #6366f1; }
</style>
"""

_NO_DATASET_HTML = """
<div style="
    text-align:center; padding:80px 20px;
    background: linear-gradient(135deg, #eef2ff, #f5f3ff);
    border-radius: 16px; border: 1.5px dashed #c7d2fe; margin-top: 20px;
">
    <span style='font-size:3.5rem;'>📂</span>
    <p style='color:#6366f1; font-size:1.1rem; font-weight:600; margin:12px 0 4px;'>
        No dataset loaded
    </p>
    <p style='color:#818cf8; font-size:0.95rem; margin:0;'>
        Upload a CSV file from the sidebar to get started.
    </p>
</div>
"""


def apply() -> None:
    """Inject the shared stylesheet into the current Streamlit page."""
    st.markdown(_CSS, unsafe_allow_html=True)


def page_header(icon: str, title: str, subtitle: str, color_start: str = "#6366f1", color_end: str = "#7c3aed") -> None:
    """Render a consistent gradient page header banner."""
    st.markdown(
        f"""
        <div style="
            background: linear-gradient(135deg, {color_start} 0%, {color_end} 100%);
            border-radius: 16px;
            padding: 26px 32px 20px 32px;
            margin-bottom: 8px;
            box-shadow: 0 8px 30px rgba(99,102,241,0.25);
        ">
            <h1 style='margin:0 0 6px 0; color:#ffffff; font-size:1.8rem; font-weight:700;'>
                {icon} {title}
            </h1>
            <p style='color:rgba(255,255,255,0.82); font-size:0.97rem; margin:0;'>
                {subtitle}
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def no_dataset_placeholder() -> None:
    """Show the 'no dataset loaded' placeholder."""
    st.markdown(_NO_DATASET_HTML, unsafe_allow_html=True)


def section_card(title: str, color: str = "#6366f1", bg: str = "#eef2ff") -> None:
    """Render a styled section header card."""
    st.markdown(
        f"""
        <div style="
            background: {bg};
            border-left: 4px solid {color};
            border-radius: 0 10px 10px 0;
            padding: 10px 16px;
            margin: 20px 0 12px 0;
        ">
            <span style='font-size:0.97rem; font-weight:700; color:{color};'>{title}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
