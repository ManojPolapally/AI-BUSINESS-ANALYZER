"""
followup_panel.py
-----------------
Generates and renders suggested follow-up questions at the bottom of the page.

How it works:
- After a successful query, derive 4 contextually relevant follow-up questions
  from the current question + schema columns.
- Render them as clickable buttons.
- Clicking a button sets st.session_state.pending_followup, which
  pre-populates the prompt input on the next rerun.
"""

import streamlit as st


# Template follow-up patterns — filled with real column names at runtime
_FOLLOWUP_TEMPLATES = [
    "Show the trend of {metric} over {time_col}",
    "Which {category} has the highest {metric}?",
    "Compare {metric} across different {category} values",
    "What is the average {metric} grouped by {category}?",
    "Show the bottom 5 {category} by {metric}",
    "Show the distribution of {metric}",
    "What percentage of total {metric} does each {category} contribute?",
    "Show monthly {metric} totals",
    "Find outliers in {metric}",
    "Rank all {category} by {metric} descending",
]


def _pick_columns(schema: dict) -> tuple[str, str]:
    """
    From the schema, pick the best candidate for a 'metric' (numeric)
    and a 'category' (text/object) column.
    Returns (metric_col, category_col) — falls back to generic names.
    """
    numeric_cols = [
        c for c, m in schema.items()
        if any(t in m.get("dtype", "") for t in ("int", "float"))
    ]
    text_cols = [
        c for c, m in schema.items()
        if "object" in m.get("dtype", "") or "str" in m.get("dtype", "")
    ]
    time_cols = [
        c for c, m in schema.items()
        if "datetime" in m.get("dtype", "") or "date" in c.lower()
    ]

    metric = numeric_cols[0] if numeric_cols else "value"
    category = text_cols[0] if text_cols else (numeric_cols[1] if len(numeric_cols) > 1 else "category")
    time_col = time_cols[0] if time_cols else (numeric_cols[-1] if numeric_cols else "date")

    return metric, category, time_col


def _generate_suggestions(question: str, schema: dict) -> list[str]:
    """
    Generate up to 4 follow-up question suggestions based on the current
    question and schema. Uses lightweight template filling — no LLM call.
    """
    if not schema:
        return []

    metric, category, time_col = _pick_columns(schema)

    # Build candidates from templates
    candidates = [
        t.format(metric=metric, category=category, time_col=time_col)
        for t in _FOLLOWUP_TEMPLATES
    ]

    # Remove any suggestion that is identical to the current question
    q_lower = question.lower().strip()
    filtered = [s for s in candidates if s.lower() != q_lower]

    return filtered[:4]


def render_followup_suggestions() -> None:
    """
    Render up to 4 suggested follow-up query buttons at the bottom of the page.
    Clicking a button writes to st.session_state.pending_followup and reruns.
    Only shown after at least one successful query.
    """
    history = st.session_state.get("history", [])
    schema = st.session_state.get("schema", {})

    # Only render after at least one successful result
    successful = [e for e in history if e["response"].get("status") == "success"]
    if not successful:
        return

    latest_question = successful[-1]["question"]
    suggestions = _generate_suggestions(latest_question, schema)

    if not suggestions:
        return

    st.divider()
    st.markdown(
        """
        <div style="
            background: linear-gradient(135deg, #eef2ff, #f5f3ff);
            border-radius: 14px;
            padding: 18px 20px 10px 20px;
            border: 1.5px solid #e0e7ff;
            margin-bottom: 8px;
        ">
            <span style='font-size:1rem; font-weight:700; color:#4338ca;'>
                💬 Suggested Follow-up Questions
            </span>
            <p style='color:#6366f1; font-size:0.82rem; margin:4px 0 12px 0;'>
                Click any suggestion to instantly pre-fill the prompt above.
            </p>
        """,
        unsafe_allow_html=True,
    )

    cols = st.columns(2)
    for idx, suggestion in enumerate(suggestions):
        with cols[idx % 2]:
            if st.button(
                f"▶ {suggestion}",
                key=f"followup_{idx}_{suggestion[:20]}",
                use_container_width=True,
            ):
                st.session_state.pending_followup = suggestion
                st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)
