import pandas as pd

def sentiment_impact(sentiment: str) ->float:
	if sentiment == "Positive":
		return 0.1
	elif sentiment == "Negative":
		return -0.1
	else:
		return 0.0

def calculate_engagement_score(df: pd.DataFrame) ->pd.Series:
	"""
    Calculate the engagement score for each tweet considering likes, replies, retweets, views, sentiment, and followers count.

    Args:
        df (pd.DataFrame): DataFrame containing tweet data with respective columns.

    Returns:
        pd.Series: A series with engagement scores for each tweet.
    """
	# Define weights for each engagement metric
	like_weight = 0.3
	reply_weight = 0.2
	retweet_weight = 0.2
	view_weight = 0.1
	sentiment_weight = 0.1  # Sentiment influence on engagement score
	followers_weight = 0.1  # Followers influence on engagement score

    # Normalize followers count
	max_followers = df['followersCount'].max()
	min_followers = df['followersCount'].min()
	followers_range = max_followers - min_followers if max_followers != min_followers else 1

    # Calculate engagement score for each tweet
	df = df.copy()
	df.loc[:, 'engagement_score'] = (
		df['likeCount'] * like_weight +
		df['replyCount'] * reply_weight +
		df['retweetCount'] * retweet_weight +
		df['viewCount'] * view_weight +
		df['sentiment'].apply(sentiment_impact) * sentiment_weight +
		((df['followersCount'] - min_followers) / followers_range) * followers_weight
	)

	return df

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

    
