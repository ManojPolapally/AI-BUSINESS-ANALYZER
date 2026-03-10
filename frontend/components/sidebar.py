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
        "columns": [],             # list of column name strings
        "schema": {},              # full schema dict {col: {dtype, ...}}
        "history": [],             # list of result dicts
        "pending_followup": None,  # pre-filled follow-up question
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

    with st.spinner(f"Uploading **{uploaded.name}**…"):
        try:
            result = upload_csv(raw, uploaded.name)
        except APIError as exc:
            st.error(str(exc))
            return

    # Persist to session state
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
                color = "#d4edda"   # green — numeric
            elif "datetime" in dtype:
                color = "#cce5ff"   # blue — datetime
            else:
                color = "#e2e3e5"   # grey — text/object
            st.markdown(
                f"""
                <div style="
                    display:flex; justify-content:space-between;
                    background:{color}; border-radius:6px;
                    padding:4px 10px; margin-bottom:4px; font-size:0.85rem;
                ">
                    <span>&#x2022; <strong>{col}</strong></span>
                    <span style='color:#555; font-size:0.75rem;'>{dtype}</span>
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
        <div style='text-align:center; padding:8px 0 12px 0;'>
            <span style='font-size:2.2rem;'>📊</span>
            <h2 style='margin:2px 0 0 0; color:#1f77b4;'>AI BI Analyser</h2>
            <p style='margin:0; font-size:0.78rem; color:#888;'>
                Conversational Dashboard Intelligence
            </p>
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
