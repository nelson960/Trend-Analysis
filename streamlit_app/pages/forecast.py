import os
import streamlit as st
from services.engagement_forecast import visualize_forecast
from services.data_loader import load_data

st.set_page_config(
    page_title="Forecasting",
    page_icon="📈",
    layout="wide"
)

st.title("📊 Engagement Analysis and forecast")

# 📥 Load Data
PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
final_processed = os.path.join(PROJECT_DIR, "data_lake/processed", "mini_final_with_trends.parquet")

df = load_data(final_processed)

if df is not None:
	try:
		visualize_forecast(df)
	except Exception as e:
		st.error(f"Data loading error: {str(e)}")
else:
    st.error("⚠️ No data available for forecast.")

# 🔙 Button to return to Home
if st.button("🏠 Back to Dashboard"):
    st.switch_page("home.py")


