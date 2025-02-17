# import streamlit as st
# import pandas as pd
# import plotly.express as px
# import plotly.graph_objects as go

# st.set_page_config(
#     page_title="Forecasting",
#     page_icon="📈",
#     layout="wide"
# )

# def visualize_forecast(df: pd.DataFrame):
#     """
#     Visulize historical and forecast data for brand engagement.
#     """
#     st.title("Brand Engagement Analysis & Forecast")
#     # Data checks
#     if df.empty:
#         st.error("No data available")
#         return
        
#     required_cols = ["ds", "brand", "type"]
#     if not all(col in df.columns for col in required_cols):
#         st.error(f"Missing columns. Required: {required_cols}")
#         return

#     try:
#         df["ds"] = pd.to_datetime(df["ds"]) 
#         brands = sorted(df["brand"].unique())
        
#         # Sidebar for multiple selections
#         st.sidebar.header("Analysis Controls") 
#         selected_brands = st.sidebar.multiselect(
#             "Select Brands",
#             brands,
#             default=brands[:min(2, len(brands))]
#         )

#         if not selected_brands:
#             st.warning("Select at least one brand")
#             return

#         filtered_data = df[df["brand"].isin(selected_brands)].copy()
        
#         # Create line plot for forecast data
#         fig = px.line(
#             filtered_data,
#             x="ds",
#             y="yhat",
#             color="brand",
#             line_dash="type",
#             title="Brand Engagement Forecast",
#             labels={"ds": "Date", "yhat": "Predicted Value", "brand": "Brand"}
#         )
#         # Add actual data points for each selected brand
#         for brand in selected_brands:
#             actuals = filtered_data[
#                 (filtered_data["brand"] == brand) & 
#                 (filtered_data["type"] == "actual")
#             ]
            
#             # Add scatter points for actual values if available
#             if "y" in actuals.columns and not actuals.empty:
#                 fig.add_trace(
#                     go.Scatter(
#                         x=actuals["ds"],
#                         y=actuals["y"],
#                         mode="markers",
#                         name=f"{brand} (Actual)",
#                         marker=dict(size=8)
#                     )
#                 )
#         # Customize plot layout
#         fig.update_layout(
#             height=600,
#             template="plotly_white",
#             hovermode="x unified",
#             xaxis=dict(rangeslider=dict(visible=True))
#         )

#         # Display interactive Plotly chart
#         st.plotly_chart(fig, use_container_width=True)

#         # Create columns for brand-specific metrics
#         cols = st.columns(len(selected_brands))
#         for idx, brand in enumerate(selected_brands):
#             brand_data = filtered_data[filtered_data["brand"] == brand]

#             # Display brand-specific statistics
#             with cols[idx]:
#                 st.subheader(brand)
#                 if "y" in brand_data.columns:
#                     # Calculate and display actual average
#                     actuals = brand_data[brand_data["type"] == "actual"]["y"]
#                     st.metric("Actual Average", f"{actuals.mean():.2f}" if not actuals.empty else "N/A")
                
#                 # Calculate and display forecast average 
#                 forecasts = brand_data[brand_data["type"] == "forecasted"]["yhat"]
#                 st.metric("Forecast Average", f"{forecasts.mean():.2f}" if not forecasts.empty else "N/A")

#     except Exception as e:
#         st.error(f"Visualization error: {str(e)}")

# try:
#     df = pd.read_parquet("mini_final_with_trends.parquet")
#     visualize_forecast(df)
# except Exception as e:
#     st.error(f"Data loading error: {str(e)}")