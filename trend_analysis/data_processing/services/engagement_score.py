import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler, StandardScaler
from sklearn.ensemble import RandomForestRegressor


def calculate_engagement_score(df: pd.DataFrame, target: str = "sentiment") -> pd.DataFrame:
    df = df.copy()

    # Initialize scalers
    robust_scaler = RobustScaler()
    standard_scaler = StandardScaler()

    # Apply robust scaling to followersCount
    df[['followersCount']] = robust_scaler.fit_transform(df[['followersCount']])

    # Define and scale engagement features in one operation
    engagement_features = ['likeCount', 'replyCount', 'retweetCount', 'viewCount']
    df[engagement_features] = standard_scaler.fit_transform(df[engagement_features])

    # Vectorized sentiment transformation
    df['sentiment_impact'] = np.tanh(df['sentiment'] * 2)  

    if target is not None and target in df.columns:
        features = engagement_features + ['sentiment_impact', 'followersCount']
        X = df[features]
        y = df[target]

        model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
        model.fit(X, y)

        weights = model.feature_importances_
        weights /= weights.sum()
        weight_dict = dict(zip(features, weights))
        df.attrs['weights_used'] = weight_dict
    else:
        weight_dict = {
            'likeCount': 0.3,
            'replyCount': 0.2,
            'retweetCount': 0.2,
            'viewCount': 0.1,
            'sentiment_impact': 0.1,  # <--- Updated key
            'followersCount': 0.1
        }

    # Calculate engagement score using vectorized operations
    df['engagement_score'] = (
        df['likeCount'] * weight_dict['likeCount'] +
        df['replyCount'] * weight_dict['replyCount'] +
        df['retweetCount'] * weight_dict['retweetCount'] +
        df['viewCount'] * weight_dict['viewCount'] +
        df['sentiment_impact'] * weight_dict['sentiment_impact'] +  # <--- Use precomputed values
        df['followersCount'] * weight_dict['followersCount']
    )

    # Store scaling statistics
    df.attrs['scaling_stats'] = {
        'followers': robust_scaler.get_params(),
        'engagement_metrics': {
            feature: {
                'mean': standard_scaler.mean_[i],
                'scale': standard_scaler.scale_[i]
            }
            for i, feature in enumerate(engagement_features)
        }
    }

    return df[['date', 'likeCount', 'replyCount', 'retweetCount', 'viewCount',
              'followersCount', 'tweets', 'brand', 'sentiment', 'engagement_score']]



def get_brand_trends(df: pd.DataFrame, predefined_brands: list) -> pd.DataFrame:
    """
    Extract brand trends from the DataFrame based on engagement scores.
    
    Args:
        df (pd.DataFrame): DataFrame containing tweet data.
        predefined_brands (list): List of brand names to filter for trends.
    
    Returns:
        pd.DataFram: A single DataFrame containing trends for all brands..
    """
    # Check if df is a DataFrame
    if not isinstance(df, pd.DataFrame):
        raise TypeError("The 'df' argument must be a pandas DataFrame.")

    # Ensure 'brand' and 'date' are in proper format
    if "brand" not in df.columns or "date" not in df.columns:
        raise ValueError("DataFrame must contain 'brand' and 'date' columns.")
    
    # Check if 'date' is in datetime format, if not convert it
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Check if the 'brand' column contains valid values (e.g., no NaNs or unexpected types)
    if df["brand"].isnull().any():
        raise ValueError("The 'brand' column contains NaN values. Please clean the data.")
    
    brand_trends = [] # To store DataFrames for each brand
    
    for brand in predefined_brands:
        # Filter tweets mentioning the brand
        brand_df = df[df["brand"] == brand]

        # Check that brand_df is a DataFrame
        if not isinstance(brand_df, pd.DataFrame):
            print(f"Warning: Brand '{brand}' returned a non-DataFrame type.")
            continue
        
        # If brand_df is empty, skip the brand
        if brand_df.empty:
            print(f"No tweets found for brand: {brand}")
            continue
        
        # Aggregate daily engagement metrics
        daily_engagement = brand_df.groupby(pd.Grouper(key="date", freq="D")).agg({
            "engagement_score": "sum"
        }).reset_index()
        
        # Prepare data for trend analysis
        prophet_df = daily_engagement[["date", "engagement_score"]].copy()
        prophet_df.columns = ["ds", "y"]  # Prophet requires 'ds' and 'y' as column names
        prophet_df["brand"] = brand  
        
        # Remove timezone info if present
        prophet_df["ds"] = prophet_df["ds"].dt.tz_localize(None)
        
        brand_trends.append(prophet_df)

        # Concatenate all brand trends into a single DataFrame
    if brand_trends:
            result_df = pd.concat(brand_trends, ignore_index=True)
            return result_df
    else:
            print("No data available for the specified brands.")
            return pd.DataFrame(columns=["ds", "y", "brand"]) # Return empty DataFrame with expected columns

    
