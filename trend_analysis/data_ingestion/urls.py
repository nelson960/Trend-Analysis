from django.urls import path
from .views import fetch_tweets_view

urlpatterns = [
    path("fetch-tweets/", fetch_tweets_view, name="fetch_tweets"),
]
