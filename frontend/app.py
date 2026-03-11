"""
app.py
------
Streamlit entry point — defines the multi-page navigation.
All page content lives in frontend/pages/.
"""

import streamlit as st

st.set_page_config(
    page_title="AI Business Analyser",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

pg = st.navigation(
    [
        st.Page("pages/1_Dashboard.py",    title="Dashboard",     icon="📊", default=True),
        st.Page("pages/2_Data_Explorer.py", title="Data Explorer", icon="🔬"),
        st.Page("pages/3_Auto_Insights.py", title="Auto Insights", icon="🤖"),
    ]
)
pg.run()
