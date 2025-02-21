import spacy
from textblob import TextBlob
from spacy.matcher import Matcher
import pandas as pd
import logging

logger = logging.getLogger(__name__)
nlp = spacy.load("en_core_web_sm")

def create_matcher(brands):
    """
    Create a spaCy matcher for brand names.

    Args:
        brands (list): List of brand names to match

    Returns:
        Matcher: Configured spaCy matcher
    """
    matcher = Matcher(nlp.vocab)
    for brand in brands:
        matcher.add(brand, [
            [{"LOWER": brand.lower()}],
            [{"TEXT": brand}],
            [{"LOWER": brand.lower() + "s"}]
        ])
    return matcher

def analyze_sentiment(tweet: str) -> float:
    """
    Analyze sentiment of a tweet and return a continuous score.

    Args:
        tweet (str): Input tweet text

    Returns:
        float: Sentiment score between -1 and 1
    """
    analysis = TextBlob(tweet)
    return analysis.sentiment.polarity

def process_tweets(data: pd.DataFrame, brands: list):
    """
    Process tweets for brand mentions and sentiment.
    This function appends new columns 'brand' and 'sentiment' to the original DataFrame,
    then drops rows that do not contain any brand mentions. The 'sentiment' column will
    contain only numeric values.

    Args:
        data (pd.DataFrame): DataFrame with a 'tweets' column.
        brands (list): List of brands to track.

    Returns:
        pd.DataFrame: Original DataFrame updated with 'brand' and 'sentiment' columns,
                      filtered to rows that contain brand mentions.
    """
    if not isinstance(data, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")

    matcher = create_matcher(brands)
    tweets = data['tweets'].tolist()

    # Prepare lists to store results
    brand_list = []
    sentiment_list = []

    # Use nlp.pipe to batch process tweets (disabling components not needed)
    for tweet, doc in zip(tweets, nlp.pipe(tweets, disable=["ner", "parser"])):
        try:
            matches = matcher(doc)
            if matches:
                # Get the first matched brand
                brand = nlp.vocab.strings[matches[0][0]]
                sentiment = analyze_sentiment(tweet)
            else:
                brand = None
                sentiment = None
        except Exception as e:
            logger.error(f"Error processing tweet: {e}")
            brand = None
            sentiment = None

        brand_list.append(brand)
        sentiment_list.append(sentiment)

    # Append the results as new columns in the original DataFrame
    data['brand'] = brand_list
    data['sentiment'] = pd.to_numeric(sentiment_list, errors='coerce')

    # Drop rows without a brand mention
    data = data.dropna(subset=['brand']).reset_index(drop=True)
    return data

def count_brand_mentions(data: pd.DataFrame) -> pd.DataFrame:
    """
    Count brand mentions per month in processed tweets.

    Assumes the DataFrame has at least:
      - 'brand': the brand mentioned.
      - 'date': the tweet date, which will be converted to datetime.

    Returns:
      A DataFrame with columns for the month, brand, and number of mentions.
    """
    # Ensure the 'date' column is in datetime format
    data['date'] = pd.to_datetime(data['date'])
    
    # Group by month (using pd.Grouper) and brand, then count mentions
    brand_counts = (
        data
        .groupby([pd.Grouper(key='date', freq='M'), 'brand'])
        .size()
        .reset_index(name='mentions')
    )
    
    # Optional: Format the date column to display as "YYYY-MM"
    brand_counts['date'] = brand_counts['date'].dt.strftime('%Y-%m')
    
    return brand_counts
