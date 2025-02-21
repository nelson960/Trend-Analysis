import os
import streamlit as st
from services.data_loader import load_data
from services.report_generator import generate_brand_report

st.set_page_config(page_title="Report", layout="wide")

st.title("📊 Report of Analysis")

# 📥 Load Data
PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
engagement_score = os.path.join(PROJECT_DIR, "data_lake/engagement_score", "engagement_score.parquet")
final_processed = os.path.join(PROJECT_DIR, "data_lake/processed", "mini_final_with_trends.parquet")
count = os.path.join(PROJECT_DIR, "data_lake/count", "count.parquet")
df_final = load_data(final_processed)
df_eng = load_data(engagement_score)
df_count = load_data(count)

# Explicitly check if each DataFrame is not None
if df_eng is not None and df_final is not None and df_count is not None:
    try:
        # Pass the correct DataFrames to the report generator
        generate_brand_report(final_processed=df_final, engagement_score=df_eng, brand_counts=df_count)
    except Exception as e:
        st.error(f"Data loading error: {str(e)}")
else:
    st.error("⚠️ No data available for visualization.")

# 🔙 Button to return to Home
if st.button("🏠 Back to Dashboard"):
    st.switch_page("home.py")
