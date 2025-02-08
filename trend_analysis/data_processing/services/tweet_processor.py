import spacy 
from textblob import TextBlob
from spacy.matcher import Matcher
import pandas as pd 
import logging

logger = logging.getLogger(__name__)
nlp = spacy.load("en_core_web_sm")

def create_matcher(brands):
	"""
	Create a spacy matcher for brand names.

	Args:
		brands (list): List of brand names to match
	
	Returns:
		Matcher: Configured spacy matcher
	"""
	matcher = Matcher(nlp.vocab)
	for brand in brands:
		#Add multiple pattern matching strategies
		matcher.add(brand, [
					  [{"LOWER": brand.lower()}], #lowercase exact match 
					  [{"TEXT": brand}], #Exact text match
					  [{"LOWER": brand.lower() + "s"}] #Plural variant
					  ])
	return matcher

def extract_brand_mentions(tweet: str, matcher: Matcher):
	"""
	Extract brand mentions from a tweet.
	
	Args:
		tweet (str): Input tweet text
		matcher (Matcher): Configured brand matcher
		
	Returns:
		tuple: (metioned tweet, brand name) or (None, None)
	"""
	try:
		doc = nlp(tweet)
		matches = matcher(doc)
		if matches:
			#Get the first matched brand
			brand = nlp.vocab.strings[matches[0][0]]
			return tweet, brand
		return None, None
	except Exception as e:
		logger.error(f"Error processing tweet: {e}")
		return None, None
	
def analyze_sentiment(tweet: str) ->float:
	"""
	Analyze sentiment of a tweet and return a continuous score.
    
    Args:
        tweet (str): Input tweet text
    
    Returns:
        float: Sentiment score between -1 and 1
            -1: Most negative
             0: Neutral
            +1: Most positive
	"""
	analysis = TextBlob(tweet)
	# TextBlob.sentiment.polarity already returns a value between -1 and 1
	return analysis.sentiment.polarity
	
def process_tweets(data: pd.DataFrame, brands: list):
	"""
    Process tweets for brand mentions and sentiment.

    Args:
        data (pd.DataFrame): DataFrame with tweets
        brands (list): List of brands to track

    Returns:
        pd.DataFrame: Processed tweets with updated columns:
                      'tweets' (filtered to those mentioning brands),
                      'brand' (matched brand),
                      'sentiment' (sentiment analysis result).
    """
	if not isinstance(data, pd.DataFrame):
		raise TypeError("Input must be pandas Dataframe")

	#Create matcher with predefined brands
	matcher = create_matcher(brands)

	#Extract brand mentions using custom function
	def extract_brands(tweet):
		try:
			mentioned_tweet, brand = extract_brand_mentions(tweet, matcher)
			if mentioned_tweet:
				sentiment = analyze_sentiment(mentioned_tweet)
				return pd.Series([mentioned_tweet, brand, sentiment])
			return pd.Series([None, None, None])
		except Exception as e:
			logger.error(f"Error processing tweet:{e}")
			return pd.Series([None, None, None])
	
	# Apply the extraction and filtering
	data[['tweets', 'brand', 'sentiment']] = data['tweets'].apply(extract_brands)

	# Drop rows with no brand mentions
	data = data.dropna(subset=['brand']).reset_index(drop=True)

	return data

def count_brand_mentions(data: pd.DataFrame):
	"""
	Count brand mentions in processed tweets.

	Args:
		data (pd.DataFrame): Processed tweets DataFrame
	
	Returns:
		pd.DataFrame: Brand mention count
	"""
	brand_counts = data['brand'].value_counts().reset_index()
	brand_counts.columns = ['brand', 'brand_mention_count']
	return brand_counts.to_string(index=False)