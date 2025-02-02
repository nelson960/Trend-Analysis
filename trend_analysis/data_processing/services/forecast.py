from prophet import Prophet
import pandas as pd

def forecast_trends(all_brand_trends: pd.DataFrame, forecast_periods: int = 30) -> pd.DataFrame:
    """
    Generate trend forecasts for each brand using Facebook Prophet.

    Args:
        all_brand_trends (pd.DataFrame): DataFrame containing brand trend data with columns "brand", "ds" (dates), and "y" (values to forecast).
        forecast_periods (int): Number of future periods to forecast.

    Returns:
        pd.DataFrame: Combined DataFrame with forecasts for all brands, including the "brand" column.
    """
    all_forecasts = []
    
    # Loop through each unique brand
    for brand in all_brand_trends["brand"].unique():
        # Filter data for the current brand
        brand_data = all_brand_trends[all_brand_trends["brand"] == brand][["ds", "y"]].copy()
        
        # Skip if no data for brand
        if len(brand_data) == 0:
            continue
            
        try:
            # Initialize and configure the Prophet model
            model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=False,
                changepoint_prior_scale=0.05,
                seasonality_prior_scale=10,
            )
            
            # Fit the model to the brand's data
            model.fit(brand_data)
            
            # Generate future dates and forecast
            future_dates = model.make_future_dataframe(periods=forecast_periods)
            forecast = model.predict(future_dates)
            
            # Add the brand name to the forecast
            forecast["brand"] = brand
            
            # 'Type' column to differentiate actual vs. forecasted
            forecast["type"] = "forecasted"
            forecast.loc[forecast["ds"].isin(brand_data["ds"]), "type"] = "actual"
            
            all_forecasts.append(forecast)
            
        except Exception as e:
            print(f"Error forecasting for brand {brand}: {str(e)}")
            continue
    
    # Return empty DataFrame if no forecasts were generated
    if not all_forecasts:
        return pd.DataFrame()
        
    # Combine all forecasts into a single DataFrame
    combined_forecasts = pd.concat(all_forecasts, ignore_index=True)
    
    return combined_forecasts