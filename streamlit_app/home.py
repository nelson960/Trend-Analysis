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

#PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
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
    search_multiple_brands
)


st.set_page_config(page_title="Brand Engagement Dashboard", page_icon="🏠", layout="wide")
st.title("Welcome to the Brand Engagement Dashboard")

# Get input as comma-separated string
brands = []
input_string = st.text_input("Enter items separated by commas")
if input_string:
    brands = [item.strip() for item in input_string.split(',')]

# load the data
raw_data_path = "/Users/nelson/py/ml_App/trend-analysis/temp/mini_tweets_trend_analysis.parquet"
raw_data = load_raw_data(raw_data_path)
valid_brands = search_multiple_brands(df=raw_data, brands=brands)

valid_brands = ["apple", "coca-cola", "nike", "samsung", "google", "microsoft", "amazon"]
# Step 3: Build the Streamlit UI and Run the Pipeline

st.title("Django Processing Pipeline in Streamlit")

# When the button is pressed, run the processing steps
# Initialize session state variables
if "running" not in st.session_state:
    st.session_state.running = False  # Track if the process is running

if "pipeline_complete" not in st.session_state:
    st.session_state.pipeline_complete = False  # Track if pipeline has finished

# Function to process data step by step
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
            enSc_output_F = os.path.join(PROJECT_DIR, "data_lake/engagement_score", "mini_final_with_trends.parquet")
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

            output_processed = os.path.join(PROJECT_DIR, "data_lake/processed", "Engagement_score.parquet")
            df.to_parquet(output_processed, index=False)
            if not st.session_state.running:
                return

            status.update(label="Done!", state="complete")
            progress_bar.progress(100)
    except Exception as e:
        st.error(f"An error occurred: {e}")

# Create a Start/Stop button
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


