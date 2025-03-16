from django.urls import path
from .views import search_brands, process_data, engagement_scores, forecast_trends_api, delete_files

urlpatterns = [
	path('search_brands/', search_brands, name='search_brands'),
	path('process_data/', process_data, name='process_data'),
	path('engagement_scores/', engagement_scores, name='engagement_scores'),
	path('forecast_trends/', forecast_trends_api, name='forecast_trends'),
	path('delete_files/', delete_files, name='delete_files'),
]