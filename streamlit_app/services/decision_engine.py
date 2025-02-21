import pandas as pd
import random

# -------------------------------
# Data Preparation Function
# -------------------------------
def prepare_dataframes(engagement_df, trend_df):
    """
    Converts date columns to datetime if not already, removes timezone info if present,
    and adds month columns for grouping (as Period objects).

    Parameters:
        engagement_df (DataFrame): Engagement data with a 'date' column.
        trend_df (DataFrame): Trend data with a 'ds' column.

    Returns:
        Tuple: The updated engagement_df and trend_df with a new 'month' column.
    """
    # Convert date columns to datetime if not already
    if not pd.api.types.is_datetime64_any_dtype(engagement_df['date']):
        engagement_df['date'] = pd.to_datetime(engagement_df['date'])
    if not pd.api.types.is_datetime64_any_dtype(trend_df['ds']):
        trend_df['ds'] = pd.to_datetime(trend_df['ds'])
    
    # Remove timezone information if present (making them tz-naive)
    if engagement_df['date'].dt.tz is not None:
        engagement_df['date'] = engagement_df['date'].dt.tz_localize(None)
    if trend_df['ds'].dt.tz is not None:
        trend_df['ds'] = trend_df['ds'].dt.tz_localize(None)
    
    # Add month columns for grouping (as Period objects)
    engagement_df["month"] = engagement_df["date"].dt.to_period("M")
    trend_df["month"] = trend_df["ds"].dt.to_period("M")
    
    return engagement_df, trend_df

# -------------------------------
# NLG Templates & Decision Engine
# -------------------------------
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
    recommendations = []
    
    # 🚀 Brand is trending
    if row["mentions"] > 100 and row["engagement_score"] > 0.5:
        recommendations.append(random.choice(NLG_TEMPLATES["trend_boost"]).format(brand=row["brand"], mentions=row["mentions"]))
    
    # ⚠️ Brand facing negative attention
    if row["mentions"] > 50 and row["sentiment"] < -0.3:
        recommendations.append(random.choice(NLG_TEMPLATES["negative_pr"]).format(brand=row["brand"],
                                                                                 mentions=row["mentions"],
                                                                                 sentiment=row["sentiment"]))
    
    # 📉 Brand engagement dropping
    if row["engagement_score"] < 0:
        recommendations.append(random.choice(NLG_TEMPLATES["engagement_drop"]).format(brand=row["brand"],
                                                                                     engagement_score=row["engagement_score"]))
    
    # 🔥 Viral content detected (using retweetCount from engagement data)
    if row["retweetCount"] > 100:
        recommendations.append(random.choice(NLG_TEMPLATES["viral_content"]).format(brand=row["brand"]))
    
    # 📅 Seasonal trends
    if row.get("yhat", 0) > 0.3:
        recommendations.append(random.choice(NLG_TEMPLATES["seasonal_trend"]).format(brand=row["brand"]))
    
    # ⚡ Competitor surge
    if row.get("trend", 0) < -0.2:
        recommendations.append(random.choice(NLG_TEMPLATES["competitor_rising"]).format(brand=row["brand"]))
    
    # ✅ If no strong trends, suggest a growth strategy
    if not recommendations:
        recommendations.append(random.choice(NLG_TEMPLATES["growth_strategy"]).format(brand=row["brand"]))
    
    return recommendations

# -------------------------------
# Function: Generate Recommendations for Selected Months
# -------------------------------
def generate_recommendations_for_months(selected_months, eng_data, tr_data, ment_data):
    """
    Generates recommendations for a list of selected months.
    
    Parameters:
        selected_months (list): List of months in 'YYYY-MM' format.
        eng_data (DataFrame): Engagement data with a 'date' column (and optionally 'month' column).
        tr_data (DataFrame): Trend data with a 'ds' column (and optionally 'month' column).
        ment_data (DataFrame): Mentions data.
    
    Returns:
        DataFrame: Combined data with recommendations for each brand and month.
    
    Raises:
        ValueError: If one or more of the selected months are not available in the engagement data.
    """
    # Prepare dataframes (convert date columns and add 'month' columns)
    eng_data, tr_data = prepare_dataframes(eng_data, tr_data)
    
    # Determine available months in the engagement data as strings
    available_months = set(eng_data["month"].astype(str).unique())
    missing_months = [m for m in selected_months if m not in available_months]
    if missing_months:
        raise ValueError(f"Selected month(s) {missing_months} are not available in the engagement data.")
    
    results = []
    # Process each month in the engagement data
    for month, month_group in eng_data.groupby("month"):
        # Check if the current month (as a string) is one of the selected months
        if str(month) not in selected_months:
            continue

        # Get start and end dates for the current month
        start_date = month_group["date"].min()
        end_date = month_group["date"].max()
        
        # Filter the engagement and trend data based on the month's date range
        filtered_eng = eng_data[(eng_data["date"] >= start_date) & (eng_data["date"] <= end_date)]
        filtered_tr = tr_data[(tr_data["ds"] >= start_date) & (tr_data["ds"] <= end_date)]
        
        # Aggregate Engagement Data by brand for the month
        month_engagement = filtered_eng.groupby("brand").agg({
            "likeCount": "mean",
            "replyCount": "mean",
            "retweetCount": "mean",
            "viewCount": "mean",
            "sentiment": "mean",
            "engagement_score": "mean"
        }).reset_index()
        
        # Aggregate Trend Data by brand for the month
        month_trend = filtered_tr.groupby("brand").agg({
            "trend": "mean",
            "yhat": "mean"
        }).reset_index()
        
        # Merge engagement, trend, and static mentions data
        combined_month = month_engagement.merge(month_trend, on="brand", how="left")
        combined_month = combined_month.merge(ment_data, on="brand", how="left")
        combined_month.fillna({"trend": 0, "yhat": 0, "mentions": 0}, inplace=True)
        
        # Add a column for the month (converted to a Timestamp for clarity)
        combined_month["month"] = month.to_timestamp()
        
        # Apply the decision engine to generate recommendations
        combined_month["recommendations"] = combined_month.apply(generate_monthly_recommendations, axis=1)
        
        results.append(combined_month)
    
    # Combine results from all processed months
    final_data = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    return final_data

# -------------------------------
# Example Usage
# -------------------------------
# Assume you have already loaded your data into the following DataFrames:
#   - engagement_data: DataFrame with a 'date' column.
#   - trend_data: DataFrame with a 'ds' column.
#   - mentions_data: DataFrame containing brand and mention counts.

# Define the selected months for which you want recommendations (as 'YYYY-MM' strings)
selected_months = ['2022-01']

engagement_data = pd.read_parquet("/Users/nelson/py/ml_App/trend-analysis/data_lake/engagement_score/engagement_score.parquet")
trend_data = pd.read_parquet("/Users/nelson/py/ml_App/trend-analysis/data_lake/processed/mini_final_with_trends.parquet")
mentions_data = pd.read_parquet("/Users/nelson/py/ml_App/trend-analysis/data_lake/count/count.parquet")


# Generate recommendations (an error will be raised if any selected month is missing)
final_recommendations = generate_recommendations_for_months(selected_months, engagement_data, trend_data, mentions_data)

# Display the final results using your display tool
import ace_tools_open as tools
tools.display_dataframe_to_user(name="Selected Month Brand Recommendations", dataframe=final_recommendations)
