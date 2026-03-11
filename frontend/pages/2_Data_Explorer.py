"""
2_Data_Explorer.py
------------------
Interactive visual exploration of the uploaded dataset.
Shows column statistics, distributions, correlations, and a data preview.
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

from frontend.components.sidebar import render_sidebar
from frontend.utils.api_client import APIError, get_data_stats, health_check
from frontend.utils.styles import apply, no_dataset_placeholder, page_header, section_card

st.set_page_config(
    page_title="Data Explorer | AI BI Analyser",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    render_sidebar()

# ── Guards ────────────────────────────────────────────────────────────────────
if not health_check():
    st.error("⚠️ Backend not reachable. Start the FastAPI server first.")
    st.stop()

if not st.session_state.get("dataset_loaded"):
    page_header("🔬", "Data Explorer", "Upload a dataset from the sidebar to explore it here.",
                "#0ea5e9", "#0284c7")
    st.divider()
    no_dataset_placeholder()
    st.stop()

# ── Load stats (cached) ───────────────────────────────────────────────────────
@st.cache_data(ttl=120)
def _load_stats(fingerprint: str | None = None) -> dict:
    return get_data_stats()


with st.spinner("🔬 Loading dataset statistics…"):
    try:
        stats = _load_stats(fingerprint=st.session_state.get("dataset_name"))
    except APIError as exc:
        st.error(f"Failed to load data stats: {exc}")
        st.stop()

# ── Unpack ────────────────────────────────────────────────────────────────────
num_cols: list[str] = stats.get("numeric_columns", [])
cat_cols: list[str] = stats.get("categorical_columns", [])
desc: dict           = stats.get("descriptive_stats", {})
corr: dict           = stats.get("correlation_matrix", {})
val_counts: dict     = stats.get("value_counts", {})
dists: dict          = stats.get("distributions", {})
null_counts: dict    = stats.get("null_counts", {})
sample: list         = stats.get("sample_data", [])

# ── Header ────────────────────────────────────────────────────────────────────
page_header(
    "🔬", "Data Explorer",
    "Column statistics, distributions, correlations, and a live data preview.",
    "#0ea5e9", "#0284c7",
)
st.divider()

# ── 1. Overview metrics ───────────────────────────────────────────────────────
section_card("📋 Dataset Overview", "#0ea5e9", "#e0f2fe")
c1, c2, c3, c4 = st.columns(4)
c1.metric("📦 Total Rows",        f"{stats.get('row_count', 0):,}")
c2.metric("📊 Total Columns",     stats.get("col_count", 0))
c3.metric("🔢 Numeric Cols",      len(num_cols))
c4.metric("🔤 Categorical Cols",  len(cat_cols))

# ── 2. Column quality (null counts) ──────────────────────────────────────────
if null_counts:
    section_card("🧹 Data Quality — Null Values per Column", "#f59e0b", "#fffbeb")
    null_df = (
        pd.Series(null_counts)
        .reset_index()
        .rename(columns={"index": "Column", 0: "Nulls"})
    )
    null_df.columns = ["Column", "Nulls"]
    total_rows = stats.get("row_count", 1)
    null_df["% Missing"] = (null_df["Nulls"] / total_rows * 100).round(2)
    null_df = null_df.sort_values("Nulls", ascending=False)

    max_nulls = null_df["Nulls"].max()
    if max_nulls > 0:
        fig_null = px.bar(
            null_df,
            x="Column",
            y="Nulls",
            text="% Missing",
            color="Nulls",
            color_continuous_scale="OrRd",
            labels={"Nulls": "Null Count"},
            template="plotly_white",
        )
        fig_null.update_traces(texttemplate="%{text}%", textposition="outside")
        fig_null.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=10, t=20, b=10),
            coloraxis_showscale=False,
            height=280,
            showlegend=False,
            xaxis_title="",
        )
        st.plotly_chart(fig_null, use_container_width=True)
    else:
        st.success("✅ No missing values detected — your dataset is complete!")

# ── 3. Descriptive stats table ────────────────────────────────────────────────
if desc:
    section_card("📐 Descriptive Statistics (Numeric Columns)", "#6366f1", "#eef2ff")
    stats_df = pd.DataFrame(desc).T.round(3)
    st.dataframe(
        stats_df.style.background_gradient(cmap="Blues", subset=["mean"]),
        use_container_width=True,
        height=min(400, (len(stats_df) + 1) * 36 + 6),
    )

# ── 4. Correlation heatmap ────────────────────────────────────────────────────
if len(num_cols) >= 2 and corr:
    section_card("🔗 Correlation Matrix", "#8b5cf6", "#f5f3ff")
    corr_df = pd.DataFrame(corr).round(2)

    # Annotated heatmap
    fig_corr = go.Figure(
        data=go.Heatmap(
            z=corr_df.values,
            x=list(corr_df.columns),
            y=list(corr_df.index),
            colorscale="RdBu_r",
            zmid=0,
            zmin=-1, zmax=1,
            text=corr_df.values.round(2),
            texttemplate="%{text}",
            hovertemplate="%{y} ↔ %{x}: <b>%{z:.3f}</b><extra></extra>",
            showscale=True,
        )
    )
    fig_corr.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#fafbfc",
        font=dict(family="Inter, sans-serif", size=11),
        margin=dict(l=20, r=20, t=30, b=20),
        height=max(370, len(num_cols) * 58),
        coloraxis_colorbar=dict(title="r"),
    )
    st.plotly_chart(fig_corr, use_container_width=True)

    # Top correlations table
    corr_pairs = []
    for i, c1 in enumerate(corr_df.columns):
        for j, c2 in enumerate(corr_df.index):
            if i < j:
                corr_pairs.append({"Column A": c1, "Column B": c2, "Pearson r": round(corr_df.at[c2, c1], 3)})
    if corr_pairs:
        corr_pairs_df = (
            pd.DataFrame(corr_pairs)
            .sort_values("Pearson r", key=abs, ascending=False)
        )
        with st.expander("📋 All correlation pairs ranked by strength", expanded=False):
            st.dataframe(corr_pairs_df, use_container_width=True, hide_index=True)

# ── 5. Numeric distributions (2-column grid) ──────────────────────────────────
if dists:
    section_card("📈 Numeric Distributions", "#0ea5e9", "#e0f2fe")
    col_names = list(dists.keys())
    COLS_PER_ROW = 2
    for i in range(0, len(col_names), COLS_PER_ROW):
        row = st.columns(COLS_PER_ROW)
        for j, cname in enumerate(col_names[i : i + COLS_PER_ROW]):
            with row[j]:
                values = dists[cname]
                fig_hist = px.histogram(
                    x=values,
                    nbins=30,
                    title=cname.replace("_", " ").title(),
                    labels={"x": cname, "count": "Frequency"},
                    color_discrete_sequence=["#6366f1"],
                    template="plotly_white",
                )
                # overlay a KDE-like line via violin trace is easier with box
                fig_hist.update_traces(marker_line_width=0.5, marker_line_color="#4f46e5")
                fig_hist.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=10, r=10, t=40, b=10),
                    showlegend=False,
                    height=280,
                    title_font=dict(size=13),
                    bargap=0.06,
                )
                st.plotly_chart(fig_hist, use_container_width=True)

# ── 6. Categorical value counts ───────────────────────────────────────────────
if val_counts:
    section_card("🔤 Categorical Column Breakdown", "#10b981", "#ecfdf5")
    for col_name, vc in val_counts.items():
        with st.expander(
            f"📊  {col_name.replace('_', ' ').title()}  —  {len(vc)} top values", expanded=True
        ):
            vc_df = (
                pd.DataFrame(list(vc.items()), columns=[col_name, "Count"])
                .sort_values("Count", ascending=True)
            )
            vc_df["Percentage"] = (vc_df["Count"] / vc_df["Count"].sum() * 100).round(1)
            fig_bar = px.bar(
                vc_df,
                y=col_name,
                x="Count",
                orientation="h",
                color="Count",
                color_continuous_scale="Blues",
                template="plotly_white",
                text="Percentage",
                labels={"Count": "Records"},
            )
            fig_bar.update_traces(texttemplate="%{text}%", textposition="outside")
            fig_bar.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=20, b=10),
                coloraxis_showscale=False,
                height=max(250, len(vc) * 30),
                xaxis_title="Count",
                yaxis_title="",
            )
            st.plotly_chart(fig_bar, use_container_width=True)

# ── 7. Data preview table ─────────────────────────────────────────────────────
if sample:
    section_card("👁️ Data Preview  (first 20 rows)", "#6366f1", "#eef2ff")
    preview_df = pd.DataFrame(sample)
    # Replace empty strings back to NaN for display
    preview_df = preview_df.replace("", None)
    st.dataframe(preview_df, use_container_width=True, height=420)

    # Download button
    csv_bytes = preview_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="⬇️ Download Preview CSV",
        data=csv_bytes,
        file_name="data_preview.csv",
        mime="text/csv",
    )
