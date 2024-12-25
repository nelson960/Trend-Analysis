import tweepy
from datetime import datetime
import tweepy.client
from db_connector import DBconnector
from data_lake_manager import save_raw_data
from dotenv import load_dotenv
import os
from typing import List, Dict
import time

#Loading the enviroment from the .env file 
load_dotenv()
twitter_db_p = os.getenv('TWITTER_DB')
bearer_token_api = os.getenv('BEARER_TOKEN_API')

#Twitter API Creds
DB_PASS = twitter_db_p
BEARER_TOKEN = bearer_token_api


#Intialize Twitter API client
client = tweepy.Client(bearer_token=BEARER_TOKEN)

def fetch_tweets(query: str, max_results: int = 10, max_retries: int = 3, initial_delay: float = 60):
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
                print(f"Rate limit persists after {max_retries} retries. Giving up.")
                raise
            
            # Calculate delay with exponential backoff
            delay = initial_delay * (2 ** attempt)
            print(f"Rate limit hit. Waiting {delay} seconds before retry {attempt + 1}/{max_retries}")
            time.sleep(delay)
            continue
            
        except tweepy.HTTPException as e:
            print(f"HTTP error occurred: {e}")
            raise
        except Exception as e:
            print(f"Error fetching tweets: {e}")
            raise

def main():
    query = "Fashion"
    max_results = 10 
	
	#Fetch tweets 
    tweets = fetch_tweets(query, max_results=10)
    
	#Save raw data 
    save_raw_data(tweets, f"tweets_{datetime.now().strftime('%Y-%m-%d')}")
    
	#Save to database
    db = DBconnector(db_name="twitter_db", user="postgress", password=DB_PASS)
    db.insert_tweets(tweets)
    db.close()

if __name__ == "__main__":
    main()