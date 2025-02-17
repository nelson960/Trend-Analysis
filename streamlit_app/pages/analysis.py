import streamlit as st
import pandas as pd
import os, sys

st.set_page_config(
    page_title="Data Analysis",
    page_icon="📊",
    layout="wide"
)

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TREND_ANALYSIS_DIR = os.path.join(PROJECT_DIR, "trend_analysis")

sys.path.extend([PROJECT_DIR, TREND_ANALYSIS_DIR])

engagement_score_dir = os.path.join(PROJECT_DIR, "data_lake/engagement_score")
os.makedirs(engagement_score_dir, exist_ok=True)


# engagement_score_path = os.path.join(PROJECT_DIR, "data_lake/engagement_score/Engagement_score.parquet")
# df = pd.read_parquet(engagement_score_path)

# Select brand
selected_brand = st.selectbox("Select a Brand", df["brand"].unique())

# Filter data for the selected brand
brand_data = df[df["brand"] == selected_brand]

# Aggregate sentiment scores by date
heatmap_data = brand_data.groupby(brand_data["date"].dt.date)["sentiment"].mean().reset_index()

# Create heatmap
fig = go.Figure(data=go.Heatmap(
    z=heatmap_data["sentiment"],
    x=heatmap_data["date"],
    y=["Sentiment"],  # Single row (for continuous distribution)
    colorscale="RdYlGn",  # Red (negative) → Yellow (neutral) → Green (positive)
    colorbar=dict(title="Sentiment Score")
))

fig.update_layout(title=f"Sentiment Trend Over Time for {selected_brand}", 
                xaxis_title="Date",
                yaxis_title="",
                yaxis_showticklabels=False)

# Display in Streamlit
st.plotly_chart(fig)