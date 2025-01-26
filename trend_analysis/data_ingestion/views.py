from django.http import JsonResponse
from data_ingestion.services.fetch_tweets import fetch_and_store_tweets

def fetch_tweets_view(request):
	query = request.GET.get("query", "Fashion")
	max_results = int(request.GET.get("Max_results", 10))
	try:
		fetch_and_store_tweets(query, max_results)
		return JsonResponse({"status": "success", "message": "Tweets fectched and stored successfully."})
	except Exception as e:
		return JsonResponse({"status": "error", "message": str(e)}, status=500)
