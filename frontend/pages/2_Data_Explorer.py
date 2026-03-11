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
                "#6366f1", "#7c3aed")
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
    "#6366f1", "#7c3aed",
)
st.divider()

# ── Executive KPI Overview ────────────────────────────────────────────────────
if sample:
    _s_df = pd.DataFrame(sample)
    _rev_kw  = ("revenue", "sales", "amount", "total", "income", "value", "price")
    _unit_kw = ("unit", "qty", "quantity", "sold", "volume", "count")
    _prod_kw = ("product", "item", "category", "name", "brand", "sku", "type")
    _date_kw = ("date", "time", "day", "month", "year", "week", "period", "timestamp")

    _rev_col  = next((c for c in num_cols if any(k in c.lower() for k in _rev_kw)  and c in _s_df.columns), None)
    _unit_col = next((c for c in num_cols if any(k in c.lower() for k in _unit_kw) and c in _s_df.columns), None)
    _prod_col = next((c for c in cat_cols if c in _s_df.columns and any(k in c.lower() for k in _prod_kw)), None)
    if _prod_col is None:
        _prod_col = next((c for c in cat_cols if c in _s_df.columns and not any(k in c.lower() for k in _date_kw)), None)

    # Scale sample totals up to full dataset
    _row_count  = stats.get("row_count", len(_s_df)) or 1
    _scale      = _row_count / max(len(_s_df), 1)

    _total_rev   = float(_s_df[_rev_col].sum()  * _scale)  if _rev_col  else None
    _total_units = float(_s_df[_unit_col].sum() * _scale)  if _unit_col else None
    _unique_prods = int(_s_df[_prod_col].nunique()) if _prod_col else len(cat_cols)

    # Growth estimate: compare first half vs second half of sample
    def _half_growth(series):
        n = len(series)
        if n < 4:
            return None
        h1 = series.iloc[: n // 2].sum()
        h2 = series.iloc[n // 2 :].sum()
        if h1 == 0:
            return None
        return round((h2 - h1) / h1 * 100, 1)

    _rev_growth  = _half_growth(_s_df[_rev_col])   if _rev_col  else None
    _unit_growth = _half_growth(_s_df[_unit_col])  if _unit_col else None
    _avg_rev     = (_total_rev / _row_count) if _total_rev and _row_count else None

    def _fmt_large(v):
        if v is None: return "—"
        if v >= 1_000_000: return f"₹{v/1_000_000:.1f}M"
        if v >= 1_000:     return f"₹{v/1_000:.1f}K"
        return f"₹{v:,.0f}"

    def _growth_badge(pct, label="vs prior period"):
        if pct is None: return ""
        arrow = "↑" if pct >= 0 else "↓"
        color = "#10b981" if pct >= 0 else "#ef4444"
        sign  = "+" if pct >= 0 else ""
        return (f"<span style='color:{color}; font-size:0.82rem; font-weight:600;'>"
                f"{arrow} {sign}{pct}% {label}</span>")

    _kpi_css = """
    <style>
    .kpi-card {
        background: #ffffff;
        border: 1.5px solid #e0e7ff;
        border-radius: 16px;
        padding: 22px 20px 16px;
        box-shadow: 0 2px 12px rgba(99,102,241,0.08);
        border-top: 4px solid;
        transition: box-shadow 0.2s, transform 0.2s;
    }
    .kpi-card:hover { box-shadow: 0 8px 24px rgba(99,102,241,0.16); transform: translateY(-2px); }
    .kpi-value { font-size: 2rem; font-weight: 800; color: #4f46e5; margin: 0 0 4px; line-height:1.1; }
    .kpi-label { font-size: 0.72rem; font-weight: 700; color: #94a3b8; letter-spacing: 0.08em;
                 text-transform: uppercase; margin-bottom: 6px; }
    .kpi-sub   { font-size: 0.82rem; margin-top: 4px; }
    </style>
    """
    st.markdown(_kpi_css, unsafe_allow_html=True)
    st.markdown("### 📈 Executive KPI Overview")

    _cols = st.columns(4)

    _kpis = [
        {
            "value": _fmt_large(_total_rev) if _total_rev else f"{_row_count:,}",
            "label": "Total Revenue" if _total_rev else "Total Records",
            "sub": _growth_badge(_rev_growth, "vs prior period") if _rev_growth else "<span style='color:#94a3b8;font-size:0.82rem;'>From uploaded dataset</span>",
            "color": "#6366f1",
        },
        {
            "value": f"{int(_total_units):,}" if _total_units else str(_unique_prods),
            "label": f"Total {(_unit_col or 'units').replace('_',' ').title()}" if _total_units else "Unique Products",
            "sub": _growth_badge(_unit_growth, "vs prior period") if _unit_growth else "<span style='color:#94a3b8;font-size:0.82rem;'>Distinct values</span>",
            "color": "#8b5cf6",
        },
        {
            "value": _fmt_large(_avg_rev) if _avg_rev else f"{len(num_cols)}",
            "label": "Avg Revenue / Row" if _avg_rev else "Numeric Columns",
            "sub": ("<span style='color:#10b981;font-size:0.82rem;font-weight:600;'>Per transaction estimate</span>"
                    if _avg_rev else "<span style='color:#94a3b8;font-size:0.82rem;'>Available for analysis</span>"),
            "color": "#4f46e5",
        },
        {
            "value": f"{int(_unique_prods)}",
            "label": f"Unique {(_prod_col or 'Categories').replace('_',' ').title()}",
            "sub": ("<span style='color:#10b981;font-size:0.82rem;font-weight:600;'>"
                    f"Across {_row_count:,} records</span>"),
            "color": "#7c3aed",
        },
    ]

    for _col, _kpi in zip(_cols, _kpis):
        with _col:
            st.markdown(
                f"""<div class="kpi-card" style="border-top-color:{_kpi['color']};">
                    <div class="kpi-value" style="color:{_kpi['color']};">{_kpi['value']}</div>
                    <div class="kpi-label">{_kpi['label']}</div>
                    <div class="kpi-sub">{_kpi['sub']}</div>
                </div>""",
                unsafe_allow_html=True,
            )

    st.markdown("<br>", unsafe_allow_html=True)

st.divider()

# ── 1. Overview metrics ───────────────────────────────────────────────────────
section_card("📋 Dataset Overview", "#6366f1", "#eef2ff")
c1, c2, c3, c4 = st.columns(4)
c1.metric("📦 Total Rows",        f"{stats.get('row_count', 0):,}")
c2.metric("📊 Total Columns",     stats.get("col_count", 0))
c3.metric("🔢 Numeric Cols",      len(num_cols))
c4.metric("🔤 Categorical Cols",  len(cat_cols))

# ── 2. Column quality (null counts) ──────────────────────────────────────────
if null_counts:
    section_card("🧹 Data Quality — Null Values per Column", "#8b5cf6", "#f5f3ff")
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
            color_continuous_scale="Purples",
            labels={"Nulls": "Null Count"},
            template="plotly_white",
        )
        fig_null.update_traces(texttemplate="%{text}%", textposition="outside")
        fig_null.update_layout(
            paper_bgcolor="#ffffff",
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
        stats_df,
        use_container_width=True,
        height=min(400, (len(stats_df) + 1) * 36 + 6),
    )

# ── 4. Sales percentage by product (pie chart) ───────────────────────────────
if val_counts and sample:
    section_card("🥧 Sales Percentage by Product", "#8b5cf6", "#f5f3ff")

    sample_df = pd.DataFrame(sample)

    # Detect product type/category column — skip date/time columns
    date_keywords = ("date", "time", "day", "month", "year", "week", "period", "timestamp", "dt")
    # Priority 1: explicit type/category columns
    type_keywords = ("type", "category", "segment", "brand", "region", "class", "genre", "dept", "department")
    # Priority 2: general product columns
    prod_keywords = ("product", "item", "sku", "model", "name")

    # Prefer type/category columns first, then product columns, then any non-date categorical
    prod_col = (
        next((c for c in cat_cols if c in sample_df.columns and any(kw in c.lower() for kw in type_keywords)), None)
        or next((c for c in cat_cols if c in sample_df.columns and any(kw in c.lower() for kw in prod_keywords)), None)
        or next((c for c in cat_cols if c in sample_df.columns and not any(kw in c.lower() for kw in date_keywords)), None)
        or next((c for c in cat_cols if c in sample_df.columns), None)
    )
    # Detect revenue/sales column (prefer columns with revenue/sales in name)
    rev_keywords = ("revenue", "sales", "amount", "total", "price", "income", "value")
    rev_col = next(
        (c for c in num_cols if any(kw in c.lower() for kw in rev_keywords) and c in sample_df.columns),
        next((c for c in num_cols if c in sample_df.columns), None),
    )

    if prod_col and rev_col and prod_col in sample_df.columns and rev_col in sample_df.columns:
        pie_df = (
            sample_df.groupby(prod_col, as_index=False)[rev_col]
            .sum()
            .rename(columns={rev_col: "Revenue"})
            .sort_values("Revenue", ascending=False)
        )
        # Collapse long tails into "Other"
        top_n = pie_df.head(10).copy()
        other_sum = pie_df["Revenue"].iloc[10:].sum()
        if other_sum > 0:
            top_n = pd.concat(
                [top_n, pd.DataFrame([{prod_col: "Other", "Revenue": other_sum}])],
                ignore_index=True,
            )

        fig_pie = px.pie(
            top_n,
            names=prod_col,
            values="Revenue",
            color_discrete_sequence=[
                "#6366f1", "#8b5cf6", "#a78bfa", "#818cf8",
                "#4f46e5", "#7c3aed", "#c4b5fd", "#ddd6fe", "#ede9fe", "#e0e7ff", "#94a3b8",
            ],
            template="plotly_white",
            title=f"Revenue share by {prod_col.replace('_', ' ').title()}",
            hole=0.4,
        )
        fig_pie.update_traces(
            textposition="inside",
            textinfo="percent+label",
            hovertemplate="%{label}: %{value:,.2f} (%{percent})<extra></extra>",
        )
        fig_pie.update_layout(
            paper_bgcolor="#ffffff",
            margin=dict(l=10, r=10, t=50, b=10),
            height=450,
            font=dict(family="Inter, sans-serif", size=13),
            legend=dict(orientation="v", x=1.02),
        )
        st.plotly_chart(fig_pie, use_container_width=True)

        # Summary table below pie
        top_n["% Share"] = (top_n["Revenue"] / top_n["Revenue"].sum() * 100).round(1)
        top_n["Revenue"] = top_n["Revenue"].map(lambda v: f"{v:,.2f}")
        top_n["% Share"] = top_n["% Share"].map(lambda v: f"{v}%")
        with st.expander("📋 Revenue breakdown table", expanded=False):
            st.dataframe(top_n.rename(columns={prod_col: prod_col.replace("_", " ").title()}),
                         use_container_width=True, hide_index=True)
    elif prod_col and val_counts.get(prod_col):
        # Fallback: use transaction counts when no numeric revenue column
        vc = val_counts[prod_col]
        top10 = dict(sorted(vc.items(), key=lambda x: x[1], reverse=True)[:10])
        other = sum(vc.values()) - sum(top10.values())
        if other > 0:
            top10["Other"] = other
        fig_pie = px.pie(
            names=list(top10.keys()),
            values=list(top10.values()),
            color_discrete_sequence=[
                "#6366f1", "#8b5cf6", "#a78bfa", "#818cf8",
                "#4f46e5", "#7c3aed", "#c4b5fd", "#ddd6fe", "#ede9fe", "#e0e7ff", "#94a3b8",
            ],
            template="plotly_white",
            title=f"Sales distribution by {prod_col.replace('_', ' ').title()}",
            hole=0.4,
        )
        fig_pie.update_traces(
            textposition="inside",
            textinfo="percent+label",
            hovertemplate="%{label}: %{value:,} transactions (%{percent})<extra></extra>",
        )
        fig_pie.update_layout(
            paper_bgcolor="#ffffff",
            margin=dict(l=10, r=10, t=50, b=10),
            height=450,
            font=dict(family="Inter, sans-serif", size=13),
            legend=dict(orientation="v", x=1.02),
        )
        st.plotly_chart(fig_pie, use_container_width=True)

# ── 4b. Product-wise sales — full breakdown pie chart ────────────────────────
if val_counts and sample:
    sample_df2 = pd.DataFrame(sample)
    date_keywords = ("date", "time", "day", "month", "year", "week", "period", "timestamp", "dt")
    type_keywords2 = ("type", "category", "segment", "brand", "region", "class", "genre", "dept", "department")
    prod_keywords2 = ("product", "item", "sku", "model", "name")
    rev_keywords  = ("revenue", "sales", "amount", "total", "price", "income", "value")

    prod_col2 = (
        next((c for c in cat_cols if c in sample_df2.columns and any(kw in c.lower() for kw in type_keywords2)), None)
        or next((c for c in cat_cols if c in sample_df2.columns and any(kw in c.lower() for kw in prod_keywords2)), None)
        or next((c for c in cat_cols if c in sample_df2.columns and not any(kw in c.lower() for kw in date_keywords)), None)
        or next((c for c in cat_cols if c in sample_df2.columns), None)
    )
    units_col = next(
        (c for c in num_cols if any(kw in c.lower() for kw in ("unit", "qty", "quantity", "count", "sold", "volume")) and c in sample_df2.columns),
        None,
    )
    rev_col2 = next(
        (c for c in num_cols if any(kw in c.lower() for kw in rev_keywords) and c in sample_df2.columns),
        next((c for c in num_cols if c in sample_df2.columns), None),
    )

    if prod_col2:
        section_card("📦 Product-wise Sales Breakdown", "#4f46e5", "#eef2ff")
        left, right = st.columns(2, gap="large")

        # Left: Units sold pie
        with left:
            if units_col:
                units_df = (
                    sample_df2.groupby(prod_col2, as_index=False)[units_col]
                    .sum()
                    .rename(columns={units_col: "Units"})
                    .sort_values("Units", ascending=False)
                )
                u_top = units_df.head(10).copy()
                u_other = units_df["Units"].iloc[10:].sum()
                if u_other > 0:
                    u_top = pd.concat([u_top, pd.DataFrame([{prod_col2: "Other", "Units": u_other}])], ignore_index=True)

                fig_units = px.pie(
                    u_top,
                    names=prod_col2,
                    values="Units",
                    color_discrete_sequence=[
                        "#6366f1","#8b5cf6","#a78bfa","#818cf8","#4f46e5",
                        "#7c3aed","#c4b5fd","#ddd6fe","#ede9fe","#e0e7ff","#94a3b8",
                    ],
                    template="plotly_white",
                    title=f"Units Sold by {prod_col2.replace('_',' ').title()}",
                    hole=0.35,
                )
                fig_units.update_traces(
                    textposition="inside",
                    textinfo="percent+label",
                    hovertemplate="%{label}: %{value:,} units (%{percent})<extra></extra>",
                )
                fig_units.update_layout(
                    paper_bgcolor="#ffffff",
                    margin=dict(l=10, r=10, t=50, b=10),
                    height=420,
                    font=dict(family="Inter, sans-serif", size=12),
                    showlegend=False,
                )
                st.plotly_chart(fig_units, use_container_width=True)
            elif val_counts.get(prod_col2):
                vc2 = val_counts[prod_col2]
                top10u = dict(sorted(vc2.items(), key=lambda x: x[1], reverse=True)[:10])
                other_u = sum(vc2.values()) - sum(top10u.values())
                if other_u > 0:
                    top10u["Other"] = other_u
                fig_units = px.pie(
                    names=list(top10u.keys()),
                    values=list(top10u.values()),
                    color_discrete_sequence=[
                        "#6366f1","#8b5cf6","#a78bfa","#818cf8","#4f46e5",
                        "#7c3aed","#c4b5fd","#ddd6fe","#ede9fe","#e0e7ff","#94a3b8",
                    ],
                    template="plotly_white",
                    title=f"Order Count by {prod_col2.replace('_',' ').title()}",
                    hole=0.35,
                )
                fig_units.update_traces(
                    textposition="inside",
                    textinfo="percent+label",
                    hovertemplate="%{label}: %{value:,} orders (%{percent})<extra></extra>",
                )
                fig_units.update_layout(
                    paper_bgcolor="#ffffff",
                    margin=dict(l=10, r=10, t=50, b=10),
                    height=420,
                    font=dict(family="Inter, sans-serif", size=12),
                    showlegend=False,
                )
                st.plotly_chart(fig_units, use_container_width=True)

        # Right: Revenue pie
        with right:
            if rev_col2:
                rev_df2 = (
                    sample_df2.groupby(prod_col2, as_index=False)[rev_col2]
                    .sum()
                    .rename(columns={rev_col2: "Revenue"})
                    .sort_values("Revenue", ascending=False)
                )
                r_top = rev_df2.head(10).copy()
                r_other = rev_df2["Revenue"].iloc[10:].sum()
                if r_other > 0:
                    r_top = pd.concat([r_top, pd.DataFrame([{prod_col2: "Other", "Revenue": r_other}])], ignore_index=True)

                fig_rev = px.pie(
                    r_top,
                    names=prod_col2,
                    values="Revenue",
                    color_discrete_sequence=[
                        "#f59e0b","#f97316","#ef4444","#ec4899","#a855f7",
                        "#14b8a6","#06b6d4","#3b82f6","#84cc16","#22c55e","#94a3b8",
                    ],
                    template="plotly_white",
                    title=f"Revenue by {prod_col2.replace('_',' ').title()}",
                    hole=0.35,
                )
                fig_rev.update_traces(
                    textposition="inside",
                    textinfo="percent+label",
                    hovertemplate="%{label}: %{value:,.2f} (%{percent})<extra></extra>",
                )
                fig_rev.update_layout(
                    paper_bgcolor="#ffffff",
                    margin=dict(l=10, r=10, t=50, b=10),
                    height=420,
                    font=dict(family="Inter, sans-serif", size=12),
                    showlegend=False,
                )
                st.plotly_chart(fig_rev, use_container_width=True)

        # Shared legend + ranked table
        if rev_col2 and units_col:
            combined = (
                sample_df2.groupby(prod_col2, as_index=False)
                .agg(Units=(units_col, "sum"), Revenue=(rev_col2, "sum"))
                .sort_values("Revenue", ascending=False)
            )
            combined["Revenue Share"] = (combined["Revenue"] / combined["Revenue"].sum() * 100).round(1).astype(str) + "%"
            combined["Units Share"]   = (combined["Units"]   / combined["Units"].sum()   * 100).round(1).astype(str) + "%"
            combined["Revenue"] = combined["Revenue"].map(lambda v: f"{v:,.2f}")
            combined["Units"]   = combined["Units"].map(lambda v: f"{v:,}")
            with st.expander("📋 Full product sales table", expanded=False):
                st.dataframe(
                    combined.rename(columns={prod_col2: prod_col2.replace("_"," ").title()}),
                    use_container_width=True, hide_index=True,
                )

# ── 5. Numeric distributions (2-column grid) ──────────────────────────────────
if dists:
    section_card("📈 Numeric Distributions", "#6366f1", "#eef2ff")
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
                fig_hist.update_traces(marker_line_width=0.5, marker_line_color="#4f46e5")
                fig_hist.update_layout(
                    paper_bgcolor="#ffffff",
                    margin=dict(l=10, r=10, t=40, b=10),
                    showlegend=False,
                    height=280,
                    title_font=dict(size=13, color="#4f46e5"),
                    bargap=0.06,
                )
                st.plotly_chart(fig_hist, use_container_width=True)

# ── 6. Categorical value counts ───────────────────────────────────────────────
if val_counts:
    section_card("🔤 Categorical Column Breakdown", "#8b5cf6", "#f5f3ff")
    for col_name, vc in val_counts.items():
        # Limit to top 5 values
        vc_top5 = dict(sorted(vc.items(), key=lambda x: x[1], reverse=True)[:5])
        with st.expander(
            f"📊  {col_name.replace('_', ' ').title()}  —  Top 5 values", expanded=True
        ):
            vc_df = (
                pd.DataFrame(list(vc_top5.items()), columns=[col_name, "Count"])
                .sort_values("Count", ascending=True)
            )
            vc_df["Percentage"] = (vc_df["Count"] / sum(vc.values()) * 100).round(1)
            fig_bar = px.bar(
                vc_df,
                y=col_name,
                x="Count",
                orientation="h",
                color="Count",
                color_continuous_scale="Purples",
                template="plotly_white",
                text="Percentage",
                labels={"Count": "Records"},
            )
            fig_bar.update_traces(texttemplate="%{text}%", textposition="outside")
            fig_bar.update_layout(
                paper_bgcolor="#ffffff",
                margin=dict(l=10, r=10, t=20, b=10),
                coloraxis_showscale=False,
                height=max(200, len(vc_top5) * 50),
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
