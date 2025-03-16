from rest_framework.decorators import api_view
from rest_framework.response import Response
from .services import process_tweets, count_brand_mentions, process_tweets_column, calculate_engagement_score, get_brand_trends, forecast_trends, search_multiple_brands, load_raw_data
import os 

PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
print(PROJECT_DIR)
# Store processed data globally (for the lifetime of the server)
raw_data = load_raw_data(f"{PROJECT_DIR}/temp/test_data_set.parquet")
processed_data = None  # Initially empty
engagement_scores_cache = None  # Cache engagement scores
brand_trends_cache = None  # Cache brand trends



@api_view(['POST'])
def search_brands(request):
    brands = request.data.get("brands", [])  
    valid_brands, not_available = search_multiple_brands(raw_data, brands)
    return Response({"valid_brands": valid_brands, "not_available": not_available})

@api_view(['POST'])
def process_data(request):
    global processed_data 

    brands = request.data.get("brands", [])

    if processed_data is None:
        processed_data = process_tweets_column(raw_data, "tweets")
        processed_data = process_tweets(processed_data, brands)
        
    count = count_brand_mentions(processed_data)
    count_F = os.path.join(PROJECT_DIR, "data_lake/count", "count.parquet")
    count.to_parquet(count_F, index=False)

    return Response({"message": "Data processing complete"})

@api_view(['GET'])
def engagement_scores(request):
    global engagement_scores_cache

    if processed_data is None:
        return Response({"error": "Data not processed yet. Call /process_data first."}, status=400)
    
    if engagement_scores_cache is None:
        engagement_scores_cache = calculate_engagement_score(processed_data)
        enSc_output_F = os.path.join(PROJECT_DIR, "data_lake/engagement_score", "engagement_score.parquet")
        engagement_scores_cache.to_parquet(enSc_output_F, index=False)

    return Response({"engagement_scores"})


@api_view(['POST'])
def forecast_trends_api(request):
    global engagement_scores_cache
    global brand_trends_cache
    brands = request.data.get("brands", [])

    if processed_data is None:
        return Response({"error": "Data not processed yet. Call /process_data first."}, status=400)
    
    if engagement_scores_cache is None:
        return Response({"error": "Engagement scores not calculated. Call /engagement_scores first."}, status=400)
    
    if brand_trends_cache is None:
        brand_trends_cache = get_brand_trends(engagement_scores_cache, brands)

    forecasted_data = forecast_trends(brand_trends_cache, 30)
    output_processed = os.path.join(PROJECT_DIR, "data_lake/processed", "mini_final_with_trends.parquet")
    forecasted_data.to_parquet(output_processed, index=False)

    return Response({"message": "Trend forecasting complete"})



@api_view(['POST'])
def delete_files(request):
    # Define file paths correctly
    file_paths = [
        os.path.join(PROJECT_DIR, "data_lake/engagement_score", "engagement_score.parquet"),
        os.path.join(PROJECT_DIR, "data_lake/processed", "mini_final_with_trends.parquet"),
        os.path.join(PROJECT_DIR, "data_lake/count", "count.parquet"),
    ]

    try:
        deleted_files = []
        for file in file_paths:
            if os.path.exists(file):
                os.remove(file)
                deleted_files.append(file)

        # If no files were deleted
        if not deleted_files:
            return Response({"message": "No files found to delete."}, status=404)

        return Response({"message": "Processed files deleted successfully!", "deleted_files": deleted_files})
    
    except Exception as e:
        return Response({"error": str(e)}, status=500)