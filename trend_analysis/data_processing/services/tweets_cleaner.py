import re
import pandas as pd
import spacy

# Load the spaCy language model once
nlp = spacy.load("en_core_web_sm")

def process_tweets_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Clean, tokenize, and remove stopwords from a specified column in a DataFrame.
    This version is optimized by batch processing texts with spaCy and precompiling regex patterns.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        column_name (str): The name of the column to process.
    
    Returns:
        pd.DataFrame: The DataFrame with the processed tweets column.
    """
    if column_name not in df.columns:
        raise ValueError(f"Error: Column '{column_name}' not found in DataFrame.")
    
    # Precompile regex patterns for performance
    url_mention_pattern = re.compile(r'http\S+|www\S+|https\S+|@\w+|#\w+', flags=re.MULTILINE)
    special_char_pattern = re.compile(r'[^\w\s]')
    
    # Clean texts using vectorized apply; convert non-strings to empty string and lowercase all text
    cleaned_texts = df[column_name].astype(str).apply(
        lambda text: special_char_pattern.sub('', url_mention_pattern.sub('', text)).lower()
    ).tolist()
    
    # Process texts in batch; disable parser and ner for faster performance
    docs = list(nlp.pipe(cleaned_texts, disable=["parser", "ner"]))
    
    # Lemmatize tokens and remove stopwords using spaCy's built-in is_stop attribute
    processed_texts = [' '.join(token.lemma_ for token in doc if not token.is_stop) for doc in docs]
    
    df[column_name] = processed_texts
    return df
