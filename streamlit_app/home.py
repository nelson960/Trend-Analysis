import os
import sys
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from services.data_loader import load_data

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TREND_ANALYSIS_DIR = os.path.join(PROJECT_DIR, "trend_analysis")

# --- Django Initialization ---
def setup_django():
    if "django_initialized" not in st.session_state:
        sys.path.extend([PROJECT_DIR, TREND_ANALYSIS_DIR])

        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "trend_analysis.settings")
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

        import django
        django.setup()
        st.session_state.django_initialized = True  # Track initialization state

setup_django()

from data_processing.services import (
    process_tweets, count_brand_mentions, process_tweets_column,  
    calculate_engagement_score, get_brand_trends, forecast_trends, search_multiple_brands
)

# --- Streamlit Page Config ---
st.set_page_config(
    page_title="Brand Engagement Dashboard",
    page_icon="📊",
    layout="wide"
)

# --- Header ---
st.markdown(
    "<h1 style='text-align: center; font-size: 36px;'>🚀 Brand Engagement Dashboard</h1>",
    unsafe_allow_html=True
)
st.markdown(
    "<p style='text-align: center; font-size: 24px;'>Analyze brand mentions, engagement, and forecast trends.</p>",
    unsafe_allow_html=True
)

# --- Caching Data Loading ---
@st.cache_data
def load_raw_data():
    raw_data_path = "/Users/nelson/py/ml_App/trend-analysis/temp/test_data_set.parquet"
    return load_data(raw_data_path)

# --- User Input ---
st.markdown("### 🔍 Search for Brands")
input_string = st.text_input("Enter brand names separated by commas:")

if input_string:
    brands = [brand.strip().lower() for brand in input_string.split(",") if brand.strip()]
    raw_data = load_raw_data()

    # Search brands
    @st.cache_data
    def search_brands(data, brand_list):
        return search_multiple_brands(data, brand_list)

    valid_brands, not_available = search_brands(raw_data, brands)

    # --- Display results ---
    if valid_brands:
        st.success(f"✅ Brands found: {', '.join(valid_brands)}")
    else:
        st.info("❌ No matching brands found in tweets.")

    if not_available:
        st.warning(f"⚠️ Brands not found: {', '.join(not_available)}")

    # --- Track Processing State ---
    if "processing" not in st.session_state:
        st.session_state.processing = False

    if "stop_processing" not in st.session_state:
        st.session_state.stop_processing = False

    def run_pipeline():
        """Function to run the data processing pipeline."""
        st.session_state.processing = True  # Mark as processing
        st.session_state.stop_processing = False  # Reset stop flag

        with st.spinner("Processing data..."):
            progress_bar = st.progress(0)

            st.write("🔄 Processing tweets...")
            data = process_tweets_column(raw_data, "tweets")
            progress_bar.progress(20)

            if st.session_state.stop_processing:
                st.warning("⛔ Processing Stopped!")
                st.session_state.processing = False
                return  # Exit the function

            processed_data = process_tweets(data, valid_brands)
            progress_bar.progress(40)

            if st.session_state.stop_processing:
                st.warning("⛔ Processing Stopped!")
                st.session_state.processing = False
                return

            count = count_brand_mentions(processed_data)
            count_F = os.path.join(PROJECT_DIR, "data_lake/count", "count.parquet")
            count.to_parquet(count_F, index=False)
            progress_bar.progress(50)

            enSc_output = calculate_engagement_score(processed_data)
            enSc_output_F = os.path.join(PROJECT_DIR, "data_lake/engagement_score", "engagement_score.parquet")
            enSc_output.to_parquet(enSc_output_F, index=False)
            progress_bar.progress(70)

            if st.session_state.stop_processing:
                st.warning("⛔ Processing Stopped!")
                st.session_state.processing = False
                return

            F_data = get_brand_trends(enSc_output, valid_brands)
            st.write("📊 Forecasting trends for the next 30 days...")
            df = forecast_trends(F_data, 30)
            progress_bar.progress(90)

            output_processed = os.path.join(PROJECT_DIR, "data_lake/processed", "mini_final_with_trends.parquet")
            df.to_parquet(output_processed, index=False)

            progress_bar.progress(100)
            st.success("✅ Data processing complete!")

        st.session_state.processing = False  # Reset processing state


    # --- Display UI ---
    st.markdown("### 🔄 Start Data Processing Pipeline")
    col1, col2 = st.columns([3, 1])

    if col1.button("🚀 Run Data Processing", disabled=st.session_state.processing):
        run_pipeline()

    if st.session_state.processing:
        if col2.button("⛔ Stop Processing"):
            st.session_state.stop_processing = True

    # --- Show Next Steps Only if Valid Brands Exist ---
    if valid_brands:
        st.markdown("### 📈 Next Steps")
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("📄 Generate Report"):
                st.switch_page("pages/report.py")

        with col2:
            if st.button("🗺️ View Heatmap"):
                st.switch_page("pages/heatmap.py")

        with col3:
            if st.button("📊 Engagement & Forecast"):
                st.switch_page("pages/forecast.py")


# --- Delete Processed Files ---
file1_path = os.path.join(PROJECT_DIR, "data_lake/engagement_score", "engagement_score.parquet")
file2_path = os.path.join(PROJECT_DIR, "data_lake/processed", "mini_final_with_trends.parquet")
file3_path = os.path.join(PROJECT_DIR, "data_lake/count", "count.parquet")

def files_exist():
    return os.path.exists(file1_path) and os.path.exists(file2_path) and os.path.exists(file3_path)

def delete_files():
    try:
        os.remove(file1_path)
        os.remove(file2_path)
        os.remove(file3_path)
        st.success("🗑️ Files deleted successfully!")
        st.rerun()
    except Exception as e:
        st.error(f"⚠️ Error deleting files: {e}")

if files_exist():
    st.warning("Processed files are available.")
    if st.button("🗑️ Delete Processed Files"):
        delete_files()
