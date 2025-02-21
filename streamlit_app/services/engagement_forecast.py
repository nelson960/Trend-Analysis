import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def visualize_forecast(df: pd.DataFrame):
    """
    Visualize historical and forecast data for brand engagement.
    """
    st.title("Brand Engagement Analysis & Forecast")

    # Validate DataFrame is not empty
    if df.empty:
        st.error("No data available")
        return

    # Ensure required columns exist
    required_cols = ["ds", "brand", "type", "yhat"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        st.error(f"Missing columns: {missing_cols}. Required columns: {required_cols}")
        return

    try:
        # Convert 'ds' to datetime
        df["ds"] = pd.to_datetime(df["ds"])

        # Sidebar for controls
        st.sidebar.header("Analysis Controls")
        brands = sorted(df["brand"].unique())
        selected_brands = st.sidebar.multiselect(
            "Select Brands", brands, default=brands[:min(2, len(brands))]
        )
        if not selected_brands:
            st.warning("Please select at least one brand")
            return

        # Date range filter
        min_date, max_date = df["ds"].min(), df["ds"].max()
        date_range = st.sidebar.date_input("Select Date Range", [min_date, max_date])
        if len(date_range) != 2:
            st.error("Please select a valid date range")
            return
        start_date, end_date = date_range
        if start_date > end_date:
            st.error("Start date must be before end date")
            return

        # Filter data by selected brands and date range
        filtered_data = df[
            (df["brand"].isin(selected_brands)) & 
            (df["ds"] >= pd.to_datetime(start_date)) & 
            (df["ds"] <= pd.to_datetime(end_date))
        ].copy()
        if filtered_data.empty:
            st.warning("No data available for the selected filters")
            return

        # Create line plot for forecast data
        fig = px.line(
            filtered_data,
            x="ds",
            y="yhat",
            color="brand",
            line_dash="type",
            title="Brand Engagement Forecast",
            labels={"ds": "Date", "yhat": "Predicted Value", "brand": "Brand"}
        )

        # Add actual data points for each selected brand, if available
        for brand in selected_brands:
            actuals = filtered_data[
                (filtered_data["brand"] == brand) & (filtered_data["type"] == "actual")
            ]
            if "y" in actuals.columns and not actuals.empty:
                fig.add_trace(
                    go.Scatter(
                        x=actuals["ds"],
                        y=actuals["y"],
                        mode="markers",
                        name=f"{brand} (Actual)",
                        marker=dict(size=8)
                    )
                )

        # Update plot layout
        fig.update_layout(
            height=600,
            template="plotly_white",
            hovermode="x unified",
            xaxis=dict(rangeslider=dict(visible=True))
        )

        # Display the chart
        st.plotly_chart(fig, use_container_width=True)

        # Display brand-specific metrics in columns
        st.subheader("Brand Metrics")
        metric_cols = st.columns(len(selected_brands))
        for idx, brand in enumerate(selected_brands):
            brand_data = filtered_data[filtered_data["brand"] == brand]
            with metric_cols[idx]:
                st.markdown(f"### {brand}")
                # Calculate and display actual average if available
                if "y" in brand_data.columns:
                    actuals = brand_data[brand_data["type"] == "actual"]["y"]
                    actual_avg = actuals.mean() if not actuals.empty else None
                    st.metric("Actual Average", f"{actual_avg:.2f}" if actual_avg is not None else "N/A")
                # Calculate and display forecast average
                forecasts = brand_data[brand_data["type"] == "forecasted"]["yhat"]
                forecast_avg = forecasts.mean() if not forecasts.empty else None
                st.metric("Forecast Average", f"{forecast_avg:.2f}" if forecast_avg is not None else "N/A")

    except Exception as e:
        st.error(f"Visualization error: {str(e)}")
