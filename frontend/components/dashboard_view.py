"""
dashboard_view.py
-----------------
Renders interactive Plotly charts returned by the backend.

Features:
- Hover tooltips, zooming, and responsive layout via Plotly config.
- PNG download button (requires kaleido).
- SQL transparency expander per chart.
- Most recent chart shown prominently; older ones in collapsible expanders.
"""

import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

# Plotly config enables full interactivity
_PLOTLY_CONFIG = {
    "displayModeBar": True,
    "scrollZoom": True,
    "modeBarButtonsToAdd": ["drawline", "eraseshape"],
    "toImageButtonOptions": {
        "format": "png",
        "filename": "dashboard_chart",
        "height": 600,
        "width": 1200,
        "scale": 2,
    },
}


def _figure_from_dict(figure_dict: dict) -> go.Figure:
    """Reconstruct a Plotly figure from its serialised dict representation."""
    try:
        # pio.from_json is the safest round-trip for any Plotly figure dict
        import json
        return pio.from_json(json.dumps(figure_dict))
    except Exception:
        # Fallback: unpack data + layout keys directly
        return go.Figure(
            data=figure_dict.get("data", []),
            layout=figure_dict.get("layout", {}),
        )


def _apply_theme(fig: go.Figure) -> go.Figure:
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#fafbfc",
        font=dict(family="Inter, sans-serif", size=13),
        margin=dict(l=20, r=20, t=55, b=20),
        hoverlabel=dict(
            bgcolor="white",
            font_size=13,
            font_family="Inter, sans-serif",
        ),
    )
    return fig


def render_chart(response: dict) -> None:
    """
    Render the Plotly chart from a single API response dict,
    followed by textual insights and recommendations below it.
    Skips silently if status is not 'success'.
    """
    if response.get("status") != "success":
        return

    figure_dict = response.get("chart_figure")
    if not figure_dict:
        st.warning("No chart data was returned for this query.")
        return

    fig = _apply_theme(_figure_from_dict(figure_dict))

    st.plotly_chart(
        fig,
        width="stretch",
        config=_PLOTLY_CONFIG,
    )

    # ---- Download as PNG ----
    try:
        img_bytes = pio.to_image(fig, format="png", width=1200, height=600, scale=2)
        st.download_button(
            label="⬇️ Download Chart (PNG)",
            data=img_bytes,
            file_name="chart.png",
            mime="image/png",
        )
    except Exception:
        pass  # kaleido not installed — skip silently

    # ---- SQL Transparency ----
    sql = response.get("sql_query")
    if sql:
        with st.expander("🔍 View SQL Query", expanded=False):
            st.code(sql, language="sql")

    # ---- Textual Chart Explanation (Insights) ----
    insights: list[str] = response.get("insights", [])
    if insights:
        st.markdown(
            """
            <div style="
                background: linear-gradient(135deg, #eef2ff, #e0e7ff);
                border-left: 4px solid #6366f1;
                border-radius: 0 12px 12px 0;
                padding: 12px 18px;
                margin-top: 20px;
            ">
                <span style='font-size:1rem; font-weight:700; color:#4338ca;'>
                    🔎 Chart Explanation
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )
        for insight in insights:
            st.markdown(
                f"""
                <div style="
                    background: #f5f7ff;
                    border: 1px solid #e0e7ff;
                    border-left: 4px solid #818cf8;
                    border-radius: 0 10px 10px 0;
                    padding: 10px 16px;
                    margin-bottom: 8px;
                    font-size: 0.93rem;
                    line-height: 1.55;
                    color: #1e2a45;
                    transition: box-shadow 0.2s ease, transform 0.2s ease;
                "
                onmouseover="this.style.boxShadow='0 4px 14px rgba(99,102,241,0.15)'; this.style.transform='translateX(3px)'"
                onmouseout="this.style.boxShadow='none'; this.style.transform='translateX(0)'"
                >🔎 {insight}</div>
                """,
                unsafe_allow_html=True,
            )

    # ---- Suggestions / Business Recommendations ----
    recs: list[str] = response.get("business_recommendations", [])
    if recs:
        st.markdown(
            """
            <div style="
                background: linear-gradient(135deg, #f0fdf4, #dcfce7);
                border-left: 4px solid #22c55e;
                border-radius: 0 12px 12px 0;
                padding: 12px 18px;
                margin-top: 16px;
            ">
                <span style='font-size:1rem; font-weight:700; color:#15803d;'>
                    💡 Suggestions & Recommendations
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )
        for i, rec in enumerate(recs, start=1):
            st.markdown(
                f"""
                <div style="
                    background: linear-gradient(90deg, #f0fdf4, #f7fef9);
                    border: 1px solid #bbf7d0;
                    border-left: 4px solid #22c55e;
                    border-radius: 0 10px 10px 0;
                    padding: 10px 16px;
                    margin-bottom: 8px;
                    font-size: 0.93rem;
                    line-height: 1.55;
                    color: #1a2e1e;
                    transition: box-shadow 0.2s ease, transform 0.2s ease;
                "
                onmouseover="this.style.boxShadow='0 4px 14px rgba(34,197,94,0.15)'; this.style.transform='translateX(3px)'"
                onmouseout="this.style.boxShadow='none'; this.style.transform='translateX(0)'"
                ><strong style='color:#15803d;'>{i}.</strong> {rec}</div>
                """,
                unsafe_allow_html=True,
            )


def render_latest_chart() -> None:
    """Render only the most recent successful chart (main view)."""
    history = st.session_state.get("history", [])
    for entry in reversed(history):
        if entry["response"].get("status") == "success":
            render_chart(entry["response"])
            return


def render_all_charts() -> None:
    """
    Render all charts: most recent is expanded at top,
    older ones collapse into expanders.
    """
    history = st.session_state.get("history", [])
    successful = [
        e for e in reversed(history)
        if e["response"].get("status") == "success"
    ]

    if not successful:
        return

    # Latest chart — full size
    latest = successful[0]
    st.markdown(f"**Q: {latest['question']}**")
    render_chart(latest["response"])

    # Past charts — collapsible
    for entry in successful[1:]:
        with st.expander(f"📊 {entry['question']}", expanded=False):
            render_chart(entry["response"])
