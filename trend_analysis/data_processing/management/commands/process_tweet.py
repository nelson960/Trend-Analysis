from django.core.management.base import BaseCommand
from data_processing.services.data_lake_loader import load_raw_data
from data_processing.services.tweet_processor import process_tweets, count_brand_mentions
from data_processing.services.tweets_cleaner import process_tweets_column
from data_processing.services.engagement_score import calculate_engagement_score, get_brand_trends
from data_processing.services.forecast import forecast_trends


import pandas as pd

brands = ["nike", "samsung", "google", "microsoft", "amazon"]

class Command(BaseCommand):
	help = "Process raw tweet and calculate engagement scores"

	def handle(self, *args, **options):
		try:
			#step 1: load raw tweet data from the data lake
			raw_data_path = "/Users/nelson/py/ml_App/trend-analysis/temp/mini_tweets_trend_analysis.parquet"
			self.stdout.write(f"Loading raw data from: {raw_data_path}")
			raw_data = load_raw_data(raw_data_path)
			
			# step 2: process tweets
			self.stdout.write("Cleaning up tweet and tokenize and remove stopwords")
			data = process_tweets_column(raw_data, "tweets")
			self.stdout.write("Processing tweets to extract brand mentions and sentiment")
			processed_data = process_tweets(data, brands)
			self.stdout.write("Counting brand mentions")
			count = count_brand_mentions(processed_data)
			self.stdout.write(count.to_string(index=False))
			self.stdout.write("Calculating engagement score")
			data_with_score = calculate_engagement_score(processed_data)
			#data_with_score.to_parquet("check.parquet", index=False)

			self.stdout.write("Getting the brand trends")
			F_data = get_brand_trends(data_with_score, brands)
			self.stdout.write("forcasting trends")
			df = forecast_trends(F_data, 30)
			df.to_parquet("mini_final_with_trends.parquet", index=False)
		except Exception as e:
			self.stderr.write(f"An error occured: {e}")