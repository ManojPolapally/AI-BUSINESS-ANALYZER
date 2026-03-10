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
    return go.Figure(figure_dict)


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
    Render the Plotly chart from a single API response dict.
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
        use_container_width=True,
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
