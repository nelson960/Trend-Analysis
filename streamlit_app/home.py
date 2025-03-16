import streamlit as st
from services.data_loader import load_data
import requests 


# --- Django API URLs ---
DJANGO_API_BASE_URL = "http://127.0.0.1:8000/api/"
SEARCH_API = DJANGO_API_BASE_URL + "search_brands/"
PROCESS_API = DJANGO_API_BASE_URL + "process_data/"
ENGAGEMENT_API = DJANGO_API_BASE_URL + "engagement_scores/"
FORECAST_API = DJANGO_API_BASE_URL + "forecast_trends/"
DELETE_FILES_API = DJANGO_API_BASE_URL + "delete_files/"

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

# --- User Input ---
st.markdown("### 🔍 Search for Brands")
input_string = st.text_input("Enter brand names separated by commas:")

# --- Function to Call Django API ---
def call_api(endpoint, payload=None, method="GET"):
    try:
        if method == "POST":
            response = requests.post(endpoint, json=payload)
        else:
            response = requests.get(endpoint)
        
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f" API Error: {response.json().get('error', 'Unknown Error')}")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Network Error: {e}")
        return None

if input_string:
    brands = [brand.strip().lower() for brand in input_string.split(",") if brand.strip()]
        
    # Search brands
    @st.cache_data
    def search_brands(brands):
        search_results = call_api(SEARCH_API, {"brands": brands}, method="POST")
        if search_results:
            valid_brands = search_results.get("valid_brands", [])
            not_available = search_results.get("not_available", [])
        
        return valid_brands, not_available

    valid_brands, not_available = search_brands(brands)

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
        """Function to run the data processing pipeline with a proper progress bar."""
        st.session_state.processing = True  
        st.session_state.stop_processing = False 

        with st.spinner("Processing data..."):
            progress_bar = st.progress(0)
            total_steps = 5  
            step_size = 100 // total_steps  # Percentage increment per step

            # Helper function to update progress
            def update_progress(step, message="Processing..."):
                progress_value = step * step_size
                progress_bar.progress(progress_value)
                st.write(f"🔄 {message}")

            # Step 1: Process tweets
            update_progress(1, "Processing tweets...")
            process_response = call_api(PROCESS_API, {"brands": valid_brands}, method="POST")
            if process_response:
                st.success("✅ Data processing complete!")
            else:
                st.error("❌ Error processing data.")
                return  # Stop execution if API fails

            # Step 2: Fetch engagement scores
            update_progress(2, "Fetching engagement scores...")
            engagement_response = call_api(ENGAGEMENT_API)
            if engagement_response:
                st.success("✅ Engagement scores retrieved!")
            else:
                st.error("❌ Error fetching engagement scores.")
                return  # Stop execution if API fails

            # Step 3: Forecast trends
            update_progress(3, "Forecasting trends...")
            forecast_response = call_api(FORECAST_API, {"brands": valid_brands}, method="POST")
            if forecast_response:
                st.success("✅ Trend forecasting complete!")
            else:
                st.error("❌ Error forecasting trends.")
                return  # Stop execution if API fails

            # Step 4: Final processing and cleanup (if applicable)
            update_progress(4, "Finalizing data processing...")
            # Simulate additional processing steps if needed
            st.write("🔄 Finalizing results...")

            # Step 5: Mark completion
            update_progress(5, "Pipeline completed!")
            st.success("🎉 Data processing pipeline completed successfully!")

        # Reset processing state
        st.session_state.processing = False


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
st.markdown("### Reset")
if st.button("🗑️ Delete Processed Data"):
    delete_response = call_api(DELETE_FILES_API, method="POST")
    if delete_response and delete_response.get("message"):
        st.success(delete_response["message"])