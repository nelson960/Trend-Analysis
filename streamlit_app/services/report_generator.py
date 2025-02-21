import pandas as pd
import random
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ===============================
# Decision Engine Functions
# ===============================
def prepare_dataframes(engagement_df, trend_df):
    """
    Converts date columns to datetime, removes timezone info, 
    and adds month columns (as Period objects) for grouping.
    """
    if not pd.api.types.is_datetime64_any_dtype(engagement_df['date']):
        engagement_df['date'] = pd.to_datetime(engagement_df['date'])
    if not pd.api.types.is_datetime64_any_dtype(trend_df['ds']):
        trend_df['ds'] = pd.to_datetime(trend_df['ds'])
    
    if engagement_df['date'].dt.tz is not None:
        engagement_df['date'] = engagement_df['date'].dt.tz_localize(None)
    if trend_df['ds'].dt.tz is not None:
        trend_df['ds'] = trend_df['ds'].dt.tz_localize(None)
    
    engagement_df["month"] = engagement_df["date"].dt.to_period("M")
    trend_df["month"] = trend_df["ds"].dt.to_period("M")
    
    return engagement_df, trend_df

# NLG templates for recommendations
NLG_TEMPLATES = {
    "trend_boost": [
        "🚀 {brand} had **high engagement** with {mentions} mentions! Consider scaling marketing efforts this month.",
        "🔥 {brand} is seeing an **increase in mentions** ({mentions}) and engagement. Leverage this for expansion!"
    ],
    "negative_pr": [
        "⚠️ {brand} is facing **negative sentiment** ({sentiment:.2f}). A strategic PR campaign is recommended.",
        "🚨 {brand} had significant mentions ({mentions}), but sentiment is dropping. Address customer concerns ASAP!"
    ],
    "engagement_drop": [
        "📉 {brand} engagement is **declining ({engagement_score:.2f})**. Time to revisit content and marketing strategy.",
        "🔄 {brand} is seeing **low engagement**. Experiment with new campaign strategies."
    ],
    "viral_content": [
        "🔥 A post about {brand} received **massive engagement**! Amplify this success with targeted ads.",
        "🚀 {brand} had a **viral moment**! Consider replicating this content style."
    ],
    "seasonal_trend": [
        "📅 {brand} is benefiting from **seasonal interest**. Align campaigns to maximize impact.",
        "🌟 {brand} saw **positive seasonality effects**. Plan future campaigns around this pattern."
    ],
    "competitor_rising": [
        "⚡ Competitors are gaining traction while {brand} engagement dipped. Reassess strategy.",
        "📊 {brand} engagement dropped while competitors surged. Consider counter-marketing."
    ],
    "growth_strategy": [
        "✅ {brand} is stable but has **growth potential**. Experiment with new engagement strategies.",
        "💡 {brand} is maintaining consistency. This is a great time to **test new content formats**."
    ]
}

def generate_monthly_recommendations(row):
    """
    Applies a series of rules on a row of aggregated data to generate recommendations.
    """
    recommendations = []
    
    if row["mentions"] > 100 and row["engagement_score"] > 0.5:
        recommendations.append(random.choice(NLG_TEMPLATES["trend_boost"]).format(
            brand=row["brand"], mentions=row["mentions"]))
    
    if row["mentions"] > 50 and row["sentiment"] < -0.3:
        recommendations.append(random.choice(NLG_TEMPLATES["negative_pr"]).format(
            brand=row["brand"], mentions=row["mentions"], sentiment=row["sentiment"]))
    
    if row["engagement_score"] < 0:
        recommendations.append(random.choice(NLG_TEMPLATES["engagement_drop"]).format(
            brand=row["brand"], engagement_score=row["engagement_score"]))
    
    if row["retweetCount"] > 100:
        recommendations.append(random.choice(NLG_TEMPLATES["viral_content"]).format(
            brand=row["brand"]))
    
    if row.get("yhat", 0) > 0.3:
        recommendations.append(random.choice(NLG_TEMPLATES["seasonal_trend"]).format(
            brand=row["brand"]))
    
    if row.get("trend", 0) < -0.2:
        recommendations.append(random.choice(NLG_TEMPLATES["competitor_rising"]).format(
            brand=row["brand"]))
    
    if not recommendations:
        recommendations.append(random.choice(NLG_TEMPLATES["growth_strategy"]).format(
            brand=row["brand"]))
    
    return recommendations

def generate_recommendations_for_months(selected_months, eng_data, tr_data, ment_data):
    """
    Generates recommendations for the selected month(s) and returns a DataFrame.
    """
    eng_data, tr_data = prepare_dataframes(eng_data, tr_data)
    
    available_months = set(eng_data["month"].astype(str).unique())
    missing_months = [m for m in selected_months if m not in available_months]
    if missing_months:
        raise ValueError(f"Selected month(s) {missing_months} are not available in the engagement data.")
    
    results = []
    for month, month_group in eng_data.groupby("month"):
        if str(month) not in selected_months:
            continue
        
        start_date = month_group["date"].min()
        end_date = month_group["date"].max()
        
        filtered_eng = eng_data[(eng_data["date"] >= start_date) & (eng_data["date"] <= end_date)]
        filtered_tr = tr_data[(tr_data["ds"] >= start_date) & (tr_data["ds"] <= end_date)]
        
        month_engagement = filtered_eng.groupby("brand").agg({
            "likeCount": "mean",
            "replyCount": "mean",
            "retweetCount": "mean",
            "viewCount": "mean",
            "sentiment": "mean",
            "engagement_score": "mean"
        }).reset_index()
        
        month_trend = filtered_tr.groupby("brand").agg({
            "trend": "mean",
            "yhat": "mean"
        }).reset_index()
        
        combined_month = month_engagement.merge(month_trend, on="brand", how="left")
        combined_month = combined_month.merge(ment_data, on="brand", how="left")
        combined_month.fillna({"trend": 0, "yhat": 0, "mentions": 0}, inplace=True)
        
        combined_month["month"] = month.to_timestamp()
        combined_month["recommendations"] = combined_month.apply(generate_monthly_recommendations, axis=1)
        
        results.append(combined_month)
    
    final_data = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    return final_data

# ===============================
# Full Report Generation Function
# ===============================
def generate_full_report(raw_engagement: pd.DataFrame, raw_trend: pd.DataFrame, raw_mentions: pd.DataFrame):
    """
    Generates a comprehensive full report that includes:
      - Sentiment Heatmap
      - Engagement Forecast
      - Brand Mentions
      - Decision Engine Recommendation
      
    Uses a single set of global filters for brand and month.
    
    This function accepts only three arguments. Internally, it creates copies of the
    raw data to serve as engagement_score, final_processed, and brand_counts.
    """
    # Create copies from the raw data
    engagement_score = raw_engagement.copy()
    final_processed = raw_trend.copy()
    brand_counts = raw_mentions.copy()
    
    # Standardize date columns
    engagement_score["date"] = pd.to_datetime(engagement_score["date"])
    final_processed.rename(columns={"ds": "date"}, inplace=True)
    final_processed["date"] = pd.to_datetime(final_processed["date"])
    
    if "date" in brand_counts.columns:
        brand_counts["date"] = pd.to_datetime(brand_counts["date"])
        brand_counts["month"] = brand_counts["date"].dt.to_period("M").astype(str)
    
    # Global Brand and Month Filters
    brands_from_engagement = set(engagement_score["brand"].unique())
    brands_from_forecast = set(final_processed["brand"].unique())
    brands_from_counts = set(brand_counts["brand"].unique())
    all_brands = sorted(brands_from_engagement | brands_from_forecast | brands_from_counts)
    
    engagement_score["date"] = pd.to_datetime(engagement_score["date"])
    global_months = sorted(engagement_score["date"].dt.to_period("M").astype(str).unique())
    
    st.sidebar.header("Global Filters")
    selected_brand = st.sidebar.selectbox("Select Brand", all_brands, index=0)
    selected_month = st.sidebar.selectbox("Select Month (YYYY-MM)", global_months, index=0)
    
    engagement_score_filtered = engagement_score[engagement_score["brand"] == selected_brand].copy()
    final_processed_filtered = final_processed[final_processed["brand"] == selected_brand].copy()
    brand_counts_filtered = brand_counts[(brand_counts["brand"] == selected_brand) & 
                                         (brand_counts["month"] == selected_month)].copy()
    
    # ---------------------------
    # Sentiment Heatmap
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
        title=f"Sentiment Trend for {selected_brand}",
        xaxis_title="Date",
        yaxis_title="Brand",
        height=800,
        template="plotly_dark",
        hovermode="closest",
        xaxis=dict(rangeslider=dict(visible=True), tickangle=45)
    )
    fig_heatmap.update_yaxes(tickfont=dict(size=18, color="white", family="Arial Black"))
    
    # ---------------------------
    # Engagement Forecast
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
        title="Engagement Forecast",
        labels={"date": "Date", "yhat": "Predicted Engagement", "brand": "Brand"},
        height=800
    )
    
    actuals = final_processed_filtered[(final_processed_filtered["brand"] == selected_brand) & 
                                       (final_processed_filtered["type"] == "actual")]
    if "y" in actuals.columns and not actuals.empty:
        fig_forecast.add_trace(go.Scatter(
            x=actuals["date"],
            y=actuals["y"],
            mode="markers",
            name=f"{selected_brand} (Actual)",
            marker=dict(size=10)
        ))
    
    fig_forecast.update_layout(
        template="plotly_white",
        hovermode="x unified",
        xaxis=dict(rangeslider=dict(visible=True))
    )
    
    # ---------------------------
    # Brand Mentions
    fig_mentions = px.bar(
        brand_counts_filtered,
        x="brand",
        y="mentions",
        title="Brand Mentions",
        labels={"brand": "Brand", "mentions": "Number of Mentions"},
        height=600
    )
    fig_mentions.update_layout(xaxis_title="Brand", yaxis_title="Mentions")
    
    # ---------------------------
    # Decision Engine Recommendation
    try:
        recommendations_df = generate_recommendations_for_months([selected_month],
                                                                 raw_engagement.copy(),
                                                                 raw_trend.copy(),
                                                                 raw_mentions.copy())
        recommendations_df = recommendations_df[recommendations_df["brand"] == selected_brand]
        if not recommendations_df.empty:
            rec = recommendations_df.iloc[0]["recommendations"]
            rec_text = rec[0] if isinstance(rec, list) and rec else "No recommendation available."
        else:
            rec_text = "No recommendations available for the selected filters."
    except ValueError as ve:
        rec_text = str(ve)
    
    # ============================
    # Display the Full Report
    # ============================
    st.title("Comprehensive Full Report")
    
    st.header("Sentiment Heatmap")
    st.plotly_chart(fig_heatmap, use_container_width=True)
    
    st.header("Engagement Forecast")
    st.plotly_chart(fig_forecast, use_container_width=True)
    
    st.header("Brand Mentions")
    st.plotly_chart(fig_mentions, use_container_width=True)
    
    st.header("Decision Engine Recommendation")
    st.markdown(f"**Recommendation:** {rec_text}")

