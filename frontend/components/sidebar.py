"""
sidebar.py
----------
Left-panel component: CSV upload, dataset info (columns + types),
session controls.
"""

import streamlit as st

from frontend.utils.api_client import APIError, get_schema, upload_csv


def _init_session_defaults() -> None:
    defaults = {
        "dataset_loaded": False,
        "dataset_name": None,
        "dataset_rows": 0,
        "dataset_cols": 0,
        "columns": [],
        "schema": {},
        "history": [],
        "pending_followup": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _render_upload_section() -> None:
    st.subheader("📁 Upload Dataset")
    uploaded = st.file_uploader(
        "Choose a CSV file",
        type=["csv"],
        help="Upload a CSV. Any existing dataset will be replaced.",
        label_visibility="collapsed",
    )
    if uploaded is None:
        return

    raw = uploaded.read()
    if len(raw) == 0:
        st.error("The uploaded file is empty.")
        return

    # st.file_uploader returns the same file on every Streamlit rerun.
    # Guard against re-uploading the same file (which would wipe history).
    file_fingerprint = (uploaded.name, len(raw))
    if st.session_state.get("_uploaded_file_fingerprint") == file_fingerprint:
        return  # Already processed — skip to avoid clearing history

    with st.spinner(f"Uploading **{uploaded.name}**…"):
        try:
            result = upload_csv(raw, uploaded.name)
        except APIError as exc:
            st.error(str(exc))
            return

    # Persist to session state
    st.session_state._uploaded_file_fingerprint = file_fingerprint
    st.session_state.dataset_loaded = True
    st.session_state.dataset_name = result["filename"]
    st.session_state.dataset_rows = result["row_count"]
    st.session_state.dataset_cols = result["column_count"]
    st.session_state.columns = result.get("columns", [])
    st.session_state.schema = result.get("schema", {})
    st.session_state.history = []
    st.session_state.pending_followup = None

    # ---- Confirmation message with detected columns ----
    cols_list = "\n".join(f"• {c}" for c in result.get("columns", []))
    st.success(
        f"Dataset uploaded successfully.\n\n"
        f"**Detected columns:**\n{cols_list}"
    )


def _render_dataset_info() -> None:
    """Show dataset stats and always-visible column badges."""
    st.divider()
    st.subheader("✅ Active Dataset")
    st.markdown(f"**{st.session_state.dataset_name}**")

    c1, c2 = st.columns(2)
    c1.metric("Rows", f"{st.session_state.dataset_rows:,}")
    c2.metric("Columns", st.session_state.dataset_cols)

    st.markdown("**Detected columns:**")
    cols = st.session_state.columns
    schema = st.session_state.schema

    if cols:
        for col in cols:
            dtype = schema.get(col, {}).get("dtype", "")
            if any(t in dtype for t in ("int", "float")):
                bg = "#e0e7ff"; border = "#a5b4fc"; dot = "#6366f1"   # indigo — numeric
            elif "datetime" in dtype:
                bg = "#dbeafe"; border = "#93c5fd"; dot = "#3b82f6"   # blue — datetime
            else:
                bg = "#f3f4f6"; border = "#d1d5db"; dot = "#6b7280"   # grey — text/object
            st.markdown(
                f"""
                <div style="
                    display:flex; justify-content:space-between; align-items:center;
                    background:{bg}; border:1px solid {border}; border-radius:8px;
                    padding:5px 10px; margin-bottom:5px; font-size:0.84rem;
                    transition: box-shadow 0.18s ease, transform 0.18s ease;
                    cursor:default;
                "
                onmouseover="this.style.boxShadow='0 3px 10px rgba(99,102,241,0.18)'; this.style.transform='translateX(3px)'"
                onmouseout="this.style.boxShadow='none'; this.style.transform='translateX(0)'"
                >
                    <span><span style='color:{dot}; font-weight:700;'>&#x2022;</span> <strong>{col}</strong></span>
                    <span style='color:#6b7280; font-size:0.73rem; background:rgba(255,255,255,0.6); padding:1px 6px; border-radius:10px;'>{dtype}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
    else:
        st.caption("Column info not available.")

    if schema:
        with st.expander("🔬 Full Schema Details", expanded=False):
            for col, meta in schema.items():
                samples = ", ".join(str(s) for s in meta.get("sample_values", []))
                st.markdown(
                    f"**`{col}`** *({meta['dtype']})*  \n"
                    f"Unique: `{meta['unique_count']}` | "
                    f"Nulls: `{meta['null_count']}`  \n"
                    f"Samples: `{samples}`"
                )
                st.markdown("---")


def render_sidebar() -> None:
    """Render the full sidebar. Call inside `with st.sidebar:`."""
    _init_session_defaults()

    st.markdown(
        """
        <div style='
            text-align:center; padding:14px 0 16px 0;
            background: linear-gradient(135deg, #6366f1, #4f46e5);
            border-radius: 14px; margin-bottom: 4px;
            box-shadow: 0 4px 18px rgba(99,102,241,0.25);
        '>
            <span style='font-size:2.4rem;'>📊</span>
            <h2 style='margin:4px 0 2px 0; color:#ffffff; font-size:1.25rem;'>AI BI Analyser</h2>
            <p style='margin:0; font-size:0.76rem; color:rgba(255,255,255,0.78);'>
                Conversational Dashboard Intelligence
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.divider()

    # ---- AI Status badge -----------------------------------------------
    st.markdown(
        """
        <div style="
            background: linear-gradient(135deg, #eef2ff, #e0e7ff);
            border: 1.5px solid #a5b4fc;
            border-radius: 10px;
            padding: 10px 14px;
            margin-bottom: 4px;
            font-size: 0.88rem;
            transition: box-shadow 0.2s;
        "
        onmouseover="this.style.boxShadow='0 4px 14px rgba(99,102,241,0.2)'"
        onmouseout="this.style.boxShadow='none'">
            🤖 <strong style='color:#4f46e5;'>AI Powered</strong> — Gemini AI enabled<br/>
            <span style="color:#6366f1; font-size:0.78rem;">
                Intelligent analysis with automatic fallback
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.divider()

    _render_upload_section()

    if st.session_state.dataset_loaded:
        _render_dataset_info()
    else:
        # Try to hydrate from persisted backend dataset
        try:
            schema_data = get_schema()
            if schema_data:
                cols = list(schema_data.get("columns", {}).keys())
                st.session_state.dataset_loaded = True
                st.session_state.schema = schema_data.get("columns", {})
                st.session_state.columns = cols
                st.session_state.dataset_cols = len(cols)
                st.info("Previously uploaded dataset is still active.")
        except Exception:
            pass

        if not st.session_state.dataset_loaded:
            st.divider()
            st.info("⬆️ Upload a CSV file above to get started.")

    st.divider()
    st.subheader("⚙️ Session")
    if st.button("🗑️ Clear Chat History", use_container_width=True):
        st.session_state.history = []
        st.session_state.pending_followup = None
        st.rerun()
    st.caption(f"Questions this session: {len(st.session_state.history)}")
