import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def generate_brand_report(final_processed: pd.DataFrame, engagement_score: pd.DataFrame, brand_counts: pd.DataFrame):
    """
    Generates a comprehensive brand report using:
      - A sentiment heatmap from engagement_score.
      - An engagement forecast chart from final_processed.
      - A brand mentions bar chart from brand_counts.
      
    The function creates a unified dashboard with tabs and allows filtering
    by brand via a sidebar multi-select.
    """

    # Ensure date columns are in datetime format
    engagement_score["date"] = pd.to_datetime(engagement_score["date"])
    final_processed.rename(columns={"ds": "date"}, inplace=True)  # Rename 'ds' to 'date' for consistency
    final_processed["date"] = pd.to_datetime(final_processed["date"])

    # Ensure "brand" exists in final_processed (it was missing in the dataset)
    if "brand" not in final_processed.columns:
        final_processed["brand"] = "Unknown"

    # Create a unique list of brands from all datasets
    brands_from_engagement = set(engagement_score["brand"].unique())
    brands_from_forecast = set(final_processed["brand"].unique())
    brands_from_counts = set(brand_counts["brand"].unique())
    all_brands = sorted(brands_from_engagement | brands_from_forecast | brands_from_counts)

    # Sidebar control: select brands for the report
    st.sidebar.header("Report Brand Selection")
    selected_brands = st.sidebar.multiselect("Select Brands", all_brands, default=all_brands[:min(2, len(all_brands))])
    
    if not selected_brands:
        st.sidebar.warning("Please select at least one brand.")
        return

    # Filter each dataset based on the selected brands
    engagement_score_filtered = engagement_score[engagement_score["brand"].isin(selected_brands)].copy()
    final_processed_filtered = final_processed[final_processed["brand"].isin(selected_brands)].copy()
    brand_counts_filtered = brand_counts[brand_counts["brand"].isin(selected_brands)].copy()

    # ------------------------ #
    # TAB 1: Sentiment Heatmap
    # ------------------------ #
    agg_data = engagement_score_filtered.groupby(["date", "brand"])["sentiment"].mean().reset_index()
    pivot_data = agg_data.pivot(index="brand", columns="date", values="sentiment")
    pivot_data = pivot_data.reindex(sorted(pivot_data.columns), axis=1)
    
    def sentiment_label(score):
        if pd.isna(score):
            return ""
        if score >= 0.1:
            return "Positive"
        elif score <= -0.1:
            return "Negative"
        else:
            return "Neutral"
    
    def annotation_text(score):
        if pd.isna(score):
            return ""
        label = sentiment_label(score)
        return f"{label}<br>{score:.2f}"
    
    text_labels = pivot_data.applymap(annotation_text).values
    
    fig_heatmap = go.Figure(data=go.Heatmap(
        z=pivot_data.values,
        x=[str(date) for date in pivot_data.columns],
        y=pivot_data.index.tolist(),
        colorscale="RdYlGn",
        colorbar=dict(
            title="Sentiment",
            tickmode="array",
            tickvals=[-1, 0, 1],
            ticktext=["Negative", "Neutral", "Positive"]
        ),
        text=text_labels,
        texttemplate="%{text}",
        hoverongaps=False
    ))
    
    fig_heatmap.update_layout(
        title=f"Sentiment Trend Over Time for {', '.join(selected_brands)}",
        xaxis_title="Date",
        yaxis_title="Brand",
        height=600,
        template="plotly_dark",
        hovermode="closest",
        xaxis=dict(rangeslider=dict(visible=True), tickangle=45)
    )
    
    fig_heatmap.update_yaxes(tickfont=dict(size=18, color="white", family="Arial Black"))

    # --------------------------- #
    # TAB 2: Engagement Forecast
    # --------------------------- #
    required_final_cols = ["date", "brand", "yhat", "type"]
    if not all(col in final_processed_filtered.columns for col in required_final_cols):
        st.error(f"Final processed DataFrame is missing one of the required columns: {required_final_cols}")
        return

    fig_forecast = px.line(
        final_processed_filtered,
        x="date",
        y="yhat",
        color="brand",
        line_dash="type",
        title="Brand Engagement Forecast",
        labels={"date": "Date", "yhat": "Predicted Engagement", "brand": "Brand"}
    )

    for brand in selected_brands:
        actuals = final_processed_filtered[(final_processed_filtered["brand"] == brand) & (final_processed_filtered["type"] == "actual")]
        if "y" in actuals.columns and not actuals.empty:
            fig_forecast.add_trace(go.Scatter(
                x=actuals["date"],
                y=actuals["y"],
                mode="markers",
                name=f"{brand} (Actual)",
                marker=dict(size=8)
            ))
    
    fig_forecast.update_layout(
        height=600,
        template="plotly_white",
        hovermode="x unified",
        xaxis=dict(rangeslider=dict(visible=True))
    )

    # --------------------------- #
    # TAB 3: Brand Mentions
    # --------------------------- #
    fig_mentions = px.bar(
        brand_counts_filtered,
        x="brand",
        y="mentions",
        title="Brand Mentions",
        labels={"brand": "Brand", "mentions": "Number of Mentions"}
    )
    fig_mentions.update_layout(xaxis_title="Brand", yaxis_title="Mentions")

    # --------------------------- #
    # TAB 4: Full Report
    # --------------------------- #
    tabs = st.tabs(["Sentiment Heatmap", "Engagement Forecast", "Brand Mentions", "Full Report"])
    
    with tabs[0]:
        st.header("Sentiment Heatmap")
        st.plotly_chart(fig_heatmap, key="heatmap_chart")
    
    with tabs[1]:
        st.header("Engagement Forecast")
        st.plotly_chart(fig_forecast, use_container_width=True, key="forecast_chart")
    
    with tabs[2]:
        st.header("Brand Mentions")
        st.plotly_chart(fig_mentions, use_container_width=True, key="mentions_chart")
    
    with tabs[3]:
        st.header("Full Report")
        st.subheader("Sentiment Heatmap")
        st.plotly_chart(fig_heatmap, key="full_heatmap_chart")
        st.subheader("Engagement Forecast")
        st.plotly_chart(fig_forecast, use_container_width=True, key="full_forecast_chart")
        st.subheader("Brand Mentions")
        st.plotly_chart(fig_mentions, use_container_width=True, key="full_mentions_chart")
