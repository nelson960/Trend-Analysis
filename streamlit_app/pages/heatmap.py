import os
import streamlit as st
from services.analysis import generate_sentiment_heatmap
from services.data_loader import load_data


st.set_page_config(page_title="Sentiment Heatmap", page_icon="🔥", layout="wide")

st.title("📊 Sentiment Heatmap Analysis")

# 📥 Load Data
PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
engagement_score_path = os.path.join(PROJECT_DIR, "data_lake/engagement_score", "engagement_score.parquet")

df = load_data(engagement_score_path)

if df is not None:
	try:
		generate_sentiment_heatmap(df)
	except Exception as e:
		st.error(f"Data loading error: {str(e)}")
else:
    st.error("⚠️ No data available for visualization.")

# 🔙 Button to return to Home
if st.button("🏠 Back to Dashboard"):
    st.switch_page("home.py")
