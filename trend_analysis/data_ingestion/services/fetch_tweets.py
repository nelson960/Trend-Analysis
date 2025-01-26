import os
import tweepy
import time
from datetime import datetime
from typing import List, Dict
from django.conf import settings
from data_ingestion.services.data_lake_manager import save_raw_data  # Import data lake manager
import logging

#intialize logging
logger = logging.getLogger(__name__)


#Intialize Twitter API client
BEARER_TOKEN = settings.BEARER_TOKEN_API
client = tweepy.Client(bearer_token=BEARER_TOKEN)

def fetch_tweets(query: str, max_results: int = 10, max_retries: int = 3, initial_delay: float = 60) ->List[Dict]:
    """
    Fetch tweets with automatic retry on rate limit.
    
    Args:
        query (str): Search query string
        max_results (int): Maximum number of tweets to fetch
        max_retries (int): Maximum number of retry attempts
        initial_delay (float): Initial delay in seconds before retrying
        
    Returns:
        List[Dict]: List of tweet data dictionaries
    """
    for attempt in range(max_retries):
        try:
            response = client.search_recent_tweets(
                query=query,
                max_results=max_results,
                tweet_fields=["id", "created_at", "text", "public_metrics", "author_id"],
            )
            
            if not response.data:
                logger.info("No tweets found for the query")
                return []

            tweets_data = []
            for tweet in response.data:
                public_metrics = tweet.public_metrics or {}
                
                tweet_data = {
                    "tweet_id": tweet.id,
                    "created_at": tweet.created_at,
                    "author_id": tweet.author_id,
                    "text": tweet.text,
                    "like_count": public_metrics.get("like_count", 0),
                    "reply_count": public_metrics.get("reply_count", 0),
                    "retweet_count": public_metrics.get("retweet_count", 0),
                    "view_count": public_metrics.get("view_count", 0),
                    "collected_at": datetime.now()
                }
                tweets_data.append(tweet_data)
            
            return tweets_data

        except tweepy.TooManyRequests as e:
            if attempt == max_retries - 1:  # Last attempt
                logger.error(f"Rate limit persists after {max_retries} retries. Giving up.")
                raise
            
            # Calculate delay with exponential backoff
            delay = initial_delay * (2 ** attempt)
            logger.warning(f"Rate limit hit. Waiting {delay} seconds before retry {attempt + 1}/{max_retries}")
            time.sleep(delay)
            continue
            
        except tweepy.HTTPException as e:
            logger.error(f"HTTP error occurred: {e}")
            raise

        except Exception as e:
            logger.error(f"Error fetching tweets: {e}")
            raise


def fetch_and_store_tweets(query: str, max_results: int = 10):
    """
    Fetch tweets and store them in the data lake as Parquet files.

    Args:
        query (str): Search query string.
        max_results (int): Maximum number of tweets to fetch.
    """
    tweets = fetch_tweets(query, max_results)
    if tweets:
        #saving the raw tweets to the data lake
        filename = f"tweets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        save_raw_data(tweets, filename) #save tweets in parquet format in the datalake manager
        logger.info(f"Saved {len(tweets)} tweets to the data lake as {filename}")

    else:
        logger.info("No tweets fetched.")