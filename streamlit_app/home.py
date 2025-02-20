import os
import streamlit as st
import pandas as pd
import sys
import plotly.graph_objects as go

# Django set up
PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TREND_ANALYSIS_DIR = os.path.join(PROJECT_DIR, "trend_analysis")
sys.path.extend([PROJECT_DIR, TREND_ANALYSIS_DIR])

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "trend_analysis.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

import django
django.setup()
from data_processing.services import (
    load_raw_data,
    process_tweets,
    count_brand_mentions,
    process_tweets_column,  
    calculate_engagement_score,
    get_brand_trends,
    forecast_trends,
    search_multiple_brands  # imported search function
)

st.set_page_config(page_title="Brand Engagement Dashboard", page_icon="🏠", layout="wide")
st.title("Welcome to the Brand Engagement Dashboard")

# Get input as comma-separated string from the user
input_string = st.text_input("Enter brands separated by commas")

if input_string:
    # Split input correctly into words (not characters)
    brands = [brand.strip().lower() for brand in input_string.split(',') if brand.strip()]
    
    # Load data
    raw_data_path = "/Users/nelson/py/ml_App/trend-analysis/temp/mini_tweets_trend_analysis.parquet"
    raw_data = load_raw_data(raw_data_path)
    
    # Use the search function with a proper list
    valid_brands, not_available = search_multiple_brands(raw_data, brands)
    
    # Display feedback to the user
    if valid_brands:
        st.success("Brands is available: " + ", ".join(valid_brands))
    else:
        st.info("No matching brands found in the tweets column.")

    if not_available:
        st.warning("The following brands were not found: " + ", ".join(not_available))

    # Continue with your pipeline if there are any valid brands found
    if valid_brands:
        st.title("Starts Processing Data Pipeline")
        if "running" not in st.session_state:
            st.session_state.running = False  # Track if the process is running

        if "pipeline_complete" not in st.session_state:
            st.session_state.pipeline_complete = False  # Track if pipeline has finished

        def run_pipeline():
            try:
                with st.status("Processing...", expanded=True) as status:
                    progress_bar = st.progress(0)

                    st.write("Processing tweets...")
                    data = process_tweets_column(raw_data, "tweets")
                    if not st.session_state.running:
                        return
                    progress_bar.progress(20)

                    processed_data = process_tweets(data, valid_brands)
                    if not st.session_state.running:
                        return
                    progress_bar.progress(40)

                    count = count_brand_mentions(processed_data)
                    st.write("Brand Mentions Count:", count)
                    if not st.session_state.running:
                        return
                    progress_bar.progress(50)

                    enSc_output = calculate_engagement_score(processed_data)
                    enSc_output_F = os.path.join(PROJECT_DIR, "data_lake/engagement_score", "engagement_score.parquet")
                    enSc_output.to_parquet(enSc_output_F, index=False)
                    if not st.session_state.running:
                        return
                    progress_bar.progress(70)

                    F_data = get_brand_trends(enSc_output, valid_brands)
                    st.write("Forecasting trends for the next 30 days...")
                    df = forecast_trends(F_data, 30)
                    st.write("Forecast complete.")
                    if not st.session_state.running:
                        return
                    progress_bar.progress(90)

                    output_processed = os.path.join(PROJECT_DIR, "data_lake/processed", "mini_final_with_trends.parquet")
                    df.to_parquet(output_processed, index=False)
                    if not st.session_state.running:
                        return
                    st.session_state.pipeline_complete = True 

                    status.update(label="Done!", state="complete")
                    progress_bar.progress(100)
            except Exception as e:
                st.error(f"An error occurred: {e}")

        button_label = "Run Data Processing Pipeline" if not st.session_state.running else "Stop Processing"

        if st.button(button_label):
            st.session_state.running = not st.session_state.running  # Toggle state

            if st.session_state.running:
                st.write("Starting data processing...")
                run_pipeline()  # Start processing
            else:
                st.write("Processing stopped.")

        if st.session_state.pipeline_complete:
            st.write("### Next Steps:")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Generate Data Report"):
                    st.write("Generating report...")
            with col2:
                if st.button("Visualize Trends"):
                    st.write("Loading visualization...")

        # Define file paths and add functionality to delete processed files
        file1_path = os.path.join(PROJECT_DIR, "data_lake/engagement_score", "engagement_score.parquet")
        file2_path = os.path.join(PROJECT_DIR, "data_lake/processed", "mini_final_with_trends.parquet")

        def files_exist():
            return os.path.exists(file1_path) and os.path.exists(file2_path)

        def delete_files():
            try:
                os.remove(file1_path)
                os.remove(file2_path)
                st.success("Files deleted successfully!")
                st.experimental_rerun()
            except Exception as e:
                st.error(f"Error deleting files: {e}")

        if files_exist():
            st.write("✅ Processed files are available.")
            if st.button("Delete Processed Files"):
                delete_files()