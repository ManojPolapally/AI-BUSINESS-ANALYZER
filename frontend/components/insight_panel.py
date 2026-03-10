"""
insight_panel.py
----------------
Renders AI insights and business recommendations for the latest query result.
Reads from st.session_state.history — updated by chat_interface.
"""

import streamlit as st


def render_insights(response: dict) -> None:
    """Render AI insights from a single response dict."""
    insights: list[str] = response.get("insights", [])
    if not insights:
        st.caption("No insights available for this query.")
        return

    for insight in insights:
        st.info(f"🔎 {insight}")


def render_recommendations(response: dict) -> None:
    """Render business recommendations from a single response dict."""
    recommendations: list[str] = response.get("business_recommendations", [])
    if not recommendations:
        st.caption("No recommendations available for this query.")
        return

    for i, rec in enumerate(recommendations, start=1):
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(90deg, #e8f4fd, #f0f9ff);
                border-left: 4px solid #1f77b4;
                border-radius: 6px;
                padding: 10px 14px;
                margin-bottom: 8px;
                font-size: 0.93rem;
                line-height: 1.5;
            ">
                <strong>{i}.</strong> {rec}
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_right_panel() -> None:
    """
    Render the full right panel: AI insights + business recommendations
    for the most recent successful query result.
    Reads from st.session_state.history.
    """
    history = st.session_state.get("history", [])
    latest_response = None

    for entry in reversed(history):
        resp = entry["response"]
        if resp.get("status") in ("success", "empty_result"):
            latest_response = resp
            break

    if latest_response is None:
        st.info("Ask a question to see AI insights here.")
        return

    # ---- AI Insights ----
    st.markdown("#### 🔎 AI Insights")
    render_insights(latest_response)

    st.divider()

    # ---- Business Recommendations ----
    st.markdown("#### 💡 Business Recommendations")
    render_recommendations(latest_response)
