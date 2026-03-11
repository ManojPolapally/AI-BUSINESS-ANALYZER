"""
3_Auto_Insights.py
------------------
Automatically surfaces key insights, patterns, and charts from the uploaded
dataset — no prompts required.  All analysis is rule-based (no LLM call).
"""

import math

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from frontend.components.sidebar import render_sidebar
from frontend.utils.api_client import APIError, get_data_stats, health_check
from frontend.utils.styles import apply, no_dataset_placeholder, page_header, section_card

apply()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    render_sidebar()

# ── Guards ────────────────────────────────────────────────────────────────────
if not health_check():
    st.error("⚠️ Backend not reachable. Start the FastAPI server first.")
    st.stop()

if not st.session_state.get("dataset_loaded"):
    page_header("🤖", "Auto Insights", "Upload a dataset from the sidebar to auto-analyse it.",
                "#7c3aed", "#4f46e5")
    st.divider()
    no_dataset_placeholder()
    st.stop()

# ── Load stats ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=120)
def _load_stats(fingerprint: str | None = None) -> dict:
    return get_data_stats()


with st.spinner("🤖 Analysing your dataset automatically…"):
    try:
        stats = _load_stats(fingerprint=st.session_state.get("dataset_name"))
    except APIError as exc:
        st.error(f"Failed to load insights: {exc}")
        st.stop()

# ── Unpack ────────────────────────────────────────────────────────────────────
row_count: int       = stats.get("row_count", 0)
col_count: int       = stats.get("col_count", 0)
num_cols: list[str]  = stats.get("numeric_columns", [])
cat_cols: list[str]  = stats.get("categorical_columns", [])
desc: dict           = stats.get("descriptive_stats", {})
corr: dict           = stats.get("correlation_matrix", {})
val_counts: dict     = stats.get("value_counts", {})
dists: dict          = stats.get("distributions", {})
null_counts: dict    = stats.get("null_counts", {})

# ── Auto-generate text observations ──────────────────────────────────────────
def _auto_observations() -> list[str]:
    obs: list[str] = []

    # Dataset size
    completeness = 1.0
    if null_counts and row_count and col_count:
        total_cells = row_count * col_count
        total_nulls = sum(null_counts.values())
        completeness = (total_cells - total_nulls) / total_cells
    obs.append(
        f"📦 The dataset contains **{row_count:,} rows** and **{col_count} columns** "
        f"({completeness*100:.1f}% complete, no missing values)" if completeness == 1.0
        else f"📦 The dataset contains **{row_count:,} rows** and **{col_count} columns** "
        f"({completeness*100:.1f}% data completeness)."
    )

    # Best numeric column by mean
    if desc:
        top_col = max(desc, key=lambda c: abs(desc[c].get("mean", 0)))
        d = desc[top_col]
        obs.append(
            f"📊 **{top_col.replace('_', ' ').title()}** has the largest average value "
            f"(mean = {d.get('mean', 0):,.2f}, std = {d.get('std', 0):,.2f})."
        )

        # High variance column
        high_var = max(
            (c for c in desc if desc[c].get("mean", 0) != 0),
            key=lambda c: desc[c].get("std", 0) / max(abs(desc[c].get("mean", 1)), 1),
            default=None,
        )
        if high_var and high_var != top_col:
            cv = desc[high_var]["std"] / max(abs(desc[high_var].get("mean", 1)), 1)
            obs.append(
                f"⚡ **{high_var.replace('_', ' ').title()}** shows high variability "
                f"(coefficient of variation = {cv:.2f}), indicating significant spread."
            )

    # Top correlation pair
    if corr:
        best = None
        best_r = 0.0
        for c1 in corr:
            for c2 in corr[c1]:
                if c1 != c2:
                    r = float(corr[c1][c2])
                    if abs(r) > abs(best_r):
                        best_r = r
                        best = (c1, c2)
        if best and abs(best_r) > 0.4:
            direction = "positively" if best_r > 0 else "negatively"
            strength = "strongly" if abs(best_r) > 0.7 else "moderately"
            obs.append(
                f"🔗 **{best[0].replace('_', ' ').title()}** and "
                f"**{best[1].replace('_', ' ').title()}** are {strength} {direction} "
                f"correlated (r = {best_r:.2f})."
            )

    # Top categorical value
    if val_counts:
        col_name = next(iter(val_counts))
        vc = val_counts[col_name]
        if vc:
            top_val = max(vc, key=lambda k: vc[k])
            top_cnt = vc[top_val]
            total   = sum(vc.values())
            pct     = top_cnt / total * 100
            obs.append(
                f"🏆 In **{col_name.replace('_', ' ').title()}**, the most common value is "
                f"**'{top_val}'** ({pct:.1f}% of records, {top_cnt:,} occurrences)."
            )
            diversity = len(vc) / max(total, 1) * 100
            if diversity < 1:
                obs.append(
                    f"📌 **{col_name.replace('_', ' ').title()}** has very low diversity — "
                    "a few values dominate the column."
                )

    # Outlier hint (values > 3 std from mean)
    if dists and desc:
        for col, values in dists.items():
            if col in desc:
                mean = desc[col].get("mean", 0)
                std  = desc[col].get("std", 1) or 1
                outliers = [v for v in values if abs(v - mean) > 3 * std]
                if len(outliers) > 0:
                    pct = len(outliers) / len(values) * 100
                    obs.append(
                        f"⚠️ **{col.replace('_', ' ').title()}** contains "
                        f"**{len(outliers)} outlier(s)** "
                        f"({pct:.1f}% of sampled values exceed 3σ from the mean)."
                    )
                    break  # report only the first one

    return obs


# ── Header ────────────────────────────────────────────────────────────────────
page_header(
    "🤖", "Auto Insights",
    "Automatic analysis of your dataset — patterns, distributions, and key findings.",
    "#7c3aed", "#4f46e5",
)
st.divider()

# ── 1. KPI row ────────────────────────────────────────────────────────────────
section_card("⚡ At a Glance", "#6366f1", "#eef2ff")
kpi_cols = st.columns(4)
kpi_cols[0].metric("📦 Rows", f"{row_count:,}")
kpi_cols[1].metric("📊 Columns", col_count)

# Completeness
total_cells = row_count * col_count if row_count and col_count else 1
total_nulls = sum(null_counts.values()) if null_counts else 0
completeness_pct = round((total_cells - total_nulls) / total_cells * 100, 1)
kpi_cols[2].metric("✅ Completeness", f"{completeness_pct}%")

# Numeric vs categorical ratio
ratio = f"{len(num_cols)} / {len(cat_cols)}"
kpi_cols[3].metric("🔢 Num / Cat", ratio)

# ── 2. Key observations ───────────────────────────────────────────────────────
section_card("💡 Key Observations", "#6366f1", "#eef2ff")
observations = _auto_observations()
for obs in observations:
    st.markdown(
        f"<div style='"
        f"background:#ffffff; border-left:4px solid #6366f1; border-radius:0 10px 10px 0;"
        f"padding:10px 16px; margin:6px 0; font-size:0.95rem; color:#1e2a45;"
        f"box-shadow:0 2px 8px rgba(99,102,241,0.08);'>"
        f"{obs}</div>",
        unsafe_allow_html=True,
    )

# ── 3. Top-value bar chart (main categorical × main numeric) ──────────────────
if cat_cols and num_cols and val_counts:
    section_card("🏆 Top Performers", "#8b5cf6", "#f5f3ff")

    main_cat = next(iter(val_counts))  # first categorical column
    vc       = val_counts[main_cat]

    top_categories = sorted(vc, key=lambda k: vc[k], reverse=True)[:10]
    counts         = [vc[k] for k in top_categories]

    fig_top = px.bar(
        x=counts,
        y=top_categories,
        orientation="h",
        text=counts,
        labels={"x": "Count", "y": main_cat.replace("_", " ").title()},
        color=counts,
        color_continuous_scale="Purples",
        template="plotly_white",
        title=f"Top 10 by count in '{main_cat.replace('_', ' ').title()}'",
    )
    fig_top.update_traces(texttemplate="%{text:,}", textposition="outside")
    fig_top.update_layout(
        paper_bgcolor="#ffffff",
        margin=dict(l=10, r=60, t=50, b=10),
        coloraxis_showscale=False,
        height=max(300, len(top_categories) * 38),
        yaxis={"categoryorder": "total ascending"},
        xaxis_title="Count",
    )
    st.plotly_chart(fig_top, use_container_width=True)

# ── 4. Main numeric distribution ─────────────────────────────────────────────
if dists:
    section_card("📊 Distribution Spotlight", "#6366f1", "#eef2ff")

    # Pick the column with the highest std for most-interesting distribution
    main_num = (
        max(desc, key=lambda c: desc[c].get("std", 0))
        if desc else next(iter(dists))
    )
    if main_num in dists:
        values = dists[main_num]
        d = desc.get(main_num, {})

        col_a, col_b = st.columns([2, 1])
        with col_a:
            fig_dist = px.histogram(
                x=values,
                nbins=35,
                color_discrete_sequence=["#6366f1"],
                template="plotly_white",
                title=f"Distribution of: {main_num.replace('_', ' ').title()}",
                labels={"x": main_num, "count": "Frequency"},
            )
            fig_dist.add_vline(
                x=d.get("mean", 0),
                line_dash="dash",
                line_color="#4f46e5",
                annotation_text=f"Mean: {d.get('mean', 0):.2f}",
                annotation_position="top right",
            )
            fig_dist.add_vline(
                x=d.get("50%", d.get("mean", 0)),
                line_dash="dot",
                line_color="#818cf8",
                annotation_text=f"Median: {d.get('50%', 0):.2f}",
                annotation_position="top left",
            )
            fig_dist.update_layout(
                paper_bgcolor="#ffffff",
                margin=dict(l=10, r=10, t=50, b=10),
                showlegend=False,
                height=320,
                bargap=0.06,
            )
            st.plotly_chart(fig_dist, use_container_width=True)

        with col_b:
            st.markdown("<p style='font-weight:700; color:#4f46e5; margin-bottom:8px;'>Quick Stats</p>", unsafe_allow_html=True)
            stat_items = [
                ("Min",     d.get("min", "—")),
                ("Q1",      d.get("25%", "—")),
                ("Median",  d.get("50%", "—")),
                ("Mean",    d.get("mean", "—")),
                ("Q3",      d.get("75%", "—")),
                ("Max",     d.get("max", "—")),
                ("Std Dev", d.get("std",  "—")),
            ]
            for label, val in stat_items:
                v = f"{float(val):,.3f}" if isinstance(val, (int, float)) else val
                st.markdown(
                    f"<div style='display:flex; justify-content:space-between; "
                    f"padding:6px 0; border-bottom:1px solid #e0e7ff; font-size:0.9rem;'>"
                    f"<span style='color:#6366f1; font-weight:500;'>{label}</span>"
                    f"<span style='font-weight:700; color:#1e2a45;'>{v}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

# ── 5. Correlation scatter plot (strongest pair) ──────────────────────────────
if len(num_cols) >= 2 and corr and dists:
    section_card("🔗 Correlation Spotlight", "#8b5cf6", "#f5f3ff")

    # Find strongest correlated pair
    best_pair = None
    best_r    = 0.0
    for c1 in corr:
        for c2 in corr[c1]:
            if c1 != c2 and c1 in dists and c2 in dists:
                r = float(corr[c1][c2])
                if abs(r) > abs(best_r):
                    best_r  = r
                    best_pair = (c1, c2)

    if best_pair:
        xs = dists.get(best_pair[0], [])
        ys = dists.get(best_pair[1], [])
        n  = min(len(xs), len(ys), 500)
        scatter_df = pd.DataFrame({best_pair[0]: xs[:n], best_pair[1]: ys[:n]})

        fig_scatter = px.scatter(
            scatter_df,
            x=best_pair[0],
            y=best_pair[1],
            color_discrete_sequence=["#818cf8"],
            template="plotly_white",
            title=(
                f"'{best_pair[0].replace('_',' ').title()}' vs "
                f"'{best_pair[1].replace('_',' ').title()}'  (r = {best_r:.2f})"
            ),
            opacity=0.65,
        )
        # Add numpy-based trendline (no statsmodels needed)
        x_vals = scatter_df[best_pair[0]].dropna().to_numpy()
        y_vals = scatter_df[best_pair[1]].dropna().to_numpy()
        _n = min(len(x_vals), len(y_vals))
        if _n > 1:
            m, b = np.polyfit(x_vals[:_n], y_vals[:_n], 1)
            x_line = np.linspace(x_vals[:_n].min(), x_vals[:_n].max(), 200)
            fig_scatter.add_scatter(
                x=x_line, y=m * x_line + b,
                mode="lines",
                line=dict(color="#4f46e5", width=2),
                name="Trend",
                showlegend=False,
            )
        fig_scatter.update_layout(
            paper_bgcolor="#ffffff",
            margin=dict(l=10, r=10, t=50, b=10),
            height=360,
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.info("Not enough shared data points to build a correlation scatter plot.")

# ── 6. Categorical breakdown pie ──────────────────────────────────────────────
if val_counts:
    section_card("🦴 Category Share", "#8b5cf6", "#f5f3ff")

    main_cat = next(iter(val_counts))
    vc       = val_counts[main_cat]
    top10    = dict(sorted(vc.items(), key=lambda x: x[1], reverse=True)[:10])
    other    = sum(vc.values()) - sum(top10.values())
    if other > 0:
        top10["Other"] = other

    fig_pie = px.pie(
        names=list(top10.keys()),
        values=list(top10.values()),
        color_discrete_sequence=["#6366f1","#8b5cf6","#a78bfa","#818cf8","#4f46e5","#7c3aed","#c4b5fd","#ddd6fe","#ede9fe","#e0e7ff"],
        template="plotly_white",
        title=f"Category distribution: {main_cat.replace('_', ' ').title()}",
        hole=0.4,
    )
    fig_pie.update_traces(
        textposition="inside",
        textinfo="percent+label",
        hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>",
    )
    fig_pie.update_layout(
        paper_bgcolor="#ffffff",
        margin=dict(l=10, r=10, t=50, b=10),
        height=400,
        legend=dict(orientation="v", x=1.05),
    )
    st.plotly_chart(fig_pie, use_container_width=True)

# ── 7. Box-plot overview (numeric columns) ────────────────────────────────────
if dists and len(num_cols) > 1:
    section_card("📦 Spread & Outliers (Box Plots)", "#6366f1", "#eef2ff")

    cols_to_plot = list(dists.keys())[:6]
    fig_box = go.Figure()
    palette  = ["#6366f1", "#8b5cf6", "#a78bfa", "#818cf8", "#4f46e5", "#7c3aed"]
    palette_fill = ["rgba(99,102,241,0.19)", "rgba(139,92,246,0.19)", "rgba(167,139,250,0.19)",
                    "rgba(129,140,248,0.19)", "rgba(79,70,229,0.19)", "rgba(124,58,237,0.19)"]
    for idx, col in enumerate(cols_to_plot):
        fig_box.add_trace(
            go.Box(
                y=dists[col],
                name=col.replace("_", " ").title(),
                boxpoints="outliers",
                marker_color=palette[idx % len(palette)],
                line_color=palette[idx % len(palette)],
                fillcolor=palette_fill[idx % len(palette_fill)],
            )
        )
    fig_box.update_layout(
        paper_bgcolor="#ffffff",
        plot_bgcolor="#fafbfc",
        template="plotly_white",
        height=380,
        margin=dict(l=10, r=10, t=30, b=10),
        showlegend=False,
        xaxis_title="",
        yaxis_title="Value",
    )
    st.plotly_chart(fig_box, use_container_width=True)

# ── Footer note ───────────────────────────────────────────────────────────────
st.divider()
st.markdown(
    "<p style='text-align:center; color:#818cf8; font-size:0.82rem;'>"
    "🤖 All insights above are generated automatically using statistical analysis "
    "— no LLM / external API call required for this page."
    "</p>",
    unsafe_allow_html=True,
)
