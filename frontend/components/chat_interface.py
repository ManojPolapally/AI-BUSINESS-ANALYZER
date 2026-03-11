"""
chat_interface.py
-----------------
Natural language prompt input bar and loading state.

Handles:
- Text input for new questions
- Pre-filling follow-up suggestions from session state
- Calling the correct backend endpoint (generate-dashboard vs follow-up)
- Persisting results to st.session_state.history
"""

import streamlit as st

from frontend.utils.api_client import APIError, follow_up_query, generate_dashboard


def _call_backend(question: str) -> dict:
    """Route to the correct endpoint based on whether history exists."""
    is_followup = len(st.session_state.get("history", [])) > 0
    if is_followup:
        return follow_up_query(question)
    return generate_dashboard(question)


def render_prompt_input() -> None:
    """
    Render the top-of-page prompt bar.

    - If a follow-up suggestion was clicked, pre-populate the input.
    - Shows a spinner with a descriptive message while the pipeline runs.
    - Appends the full response to st.session_state.history on success.
    - Displays inline error/warning messages for bad outcomes.
    """
    if not st.session_state.get("dataset_loaded"):
        st.info(
            "⬅️ **Upload a CSV dataset** in the sidebar to start asking questions."
        )
        return

    history_count = len(st.session_state.get("history", []))
    placeholder = (
        "Ask a follow-up question about this dataset…"
        if history_count > 0
        else "e.g. What are the top 5 products by revenue?"
    )

    # Pre-fill from a clicked follow-up suggestion
    prefill = st.session_state.pop("pending_followup", None) or ""

    col_input, col_btn = st.columns([5, 1])
    with col_input:
        question = st.text_input(
            "Ask a question",
            value=prefill,
            placeholder=placeholder,
            label_visibility="collapsed",
        )
    with col_btn:
        submitted = st.button("Analyse ▶", use_container_width=True, type="primary")

    # Auto-submit when a follow-up suggestion was clicked (prefill was set)
    if prefill and not submitted:
        submitted = True

    if not submitted or not question.strip():
        return

    question = question.strip()

    # ---- Loading spinner ----
    with st.spinner("🤖 Analysing your data — generating SQL, chart, and insights…"):
        try:
            response = _call_backend(question)
        except APIError as exc:
            st.error(f"**Connection error:** {exc}")
            return

    # ---- Inline result feedback ----
    status = response.get("status", "error")

    if status == "success":
        chart_type = response.get("chart_type", "chart")
        st.success(
            f"✅ Generated a **{chart_type}** chart with insights. "
            "See the dashboard below ↓"
        )
    elif status == "empty_result":
        st.warning(
            "⚠️ **No data found** for this request.  \n"
            "Try rephrasing or broadening your question."
        )
    elif status == "unsupported":
        st.warning(
            "⚠️ **This question cannot be answered** with the available dataset.  \n"
            "Try a different question using the detected columns."
        )
    elif status == "quota_exceeded":
        st.warning(
            "⏳ **AI service temporarily busy** — showing fallback chart below."
        )
    else:
        error_msg = response.get("error", "An unknown error occurred.")
        _render_error(error_msg)
        return  # Don't add error-only responses to history

    # Persist to history
    st.session_state.history.append({"question": question, "response": response})
    st.rerun()


def _render_error(error_msg: str) -> None:
    """Display a structured error message with guidance."""
    # Classify the error for a more helpful message
    low = error_msg.lower()
    if "no dataset" in low or "upload" in low:
        guidance = "Please upload a CSV file first using the sidebar."
    elif "column" in low or "schema" in low:
        guidance = "The query referenced a column that does not exist. Check the detected columns in the sidebar."
    elif "sql" in low or "syntax" in low:
        guidance = "The generated SQL was invalid. Try rephrasing your question more specifically."
    elif "no data" in low or "empty" in low:
        guidance = "The query returned no results. Try broadening your filters."
    else:
        guidance = "Try rephrasing your question or upload a different dataset."

    st.error(
        f"**Error:** {error_msg}  \n\n"
        f"**Suggestion:** {guidance}"
    )



