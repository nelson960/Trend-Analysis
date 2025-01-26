import re
import pandas as pd
import spacy

# Load the spaCy language model
nlp = spacy.load("en_core_web_sm")

# Define stop words
stop_words = nlp.Defaults.stop_words

def process_tweets_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Clean, tokenize, and remove stopwords from a specified column in a DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column_name (str): The name of the column to process.

    Returns:
        pd.DataFrame: The DataFrame with the processed tweets column.
    """
    if column_name not in df.columns:
        raise ValueError(f"Error: Column '{column_name}' not found in DataFrame.")
    
    def clean_and_process_text(text: str) -> str:
        if not isinstance(text, str):
            return ""
        
        # Clean text: Remove URLs, mentions, hashtags, special characters, and convert to lowercase
        text = re.sub(r'http\S+|www\S+|https\S+|@\w+|#\w+', '', text, flags=re.MULTILINE)
        text = re.sub(r'[^\w\s]', '', text)
        text = text.lower()
        
        # Tokenize and remove stopwords
        processed_text = ' '.join([token.lemma_ for token in nlp(text) if token.text not in stop_words])
        
        return processed_text

    # Apply the cleaning and processing function to the specified column
    df[column_name] = df[column_name].apply(clean_and_process_text)
    
    return df
