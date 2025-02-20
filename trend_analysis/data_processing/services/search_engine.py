import pandas as pd
import re
import difflib
import spacy
from collections import defaultdict

# Curated list of genuine brands.
genuine_brands = ['apple', 'coca-cola', 'nike', 'samsung', 'google', 'microsoft', 'amazon']

def validate_brand(brand, genuine_list=genuine_brands, cutoff=0.6):
    """
    Validate (and potentially correct) the brand name using fuzzy matching.
    Returns the lowercase validated brand if found, otherwise None.
    """
    brand_lower = brand.lower()
    if brand_lower in genuine_list:
        return brand_lower
    close_matches = difflib.get_close_matches(brand_lower, genuine_list, n=1, cutoff=cutoff)
    return close_matches[0] if close_matches else None

def build_inverted_index(df, genuine_list, nlp):
    """
    Build an inverted index mapping each genuine brand to a list of tweet indices
    where that brand is mentioned. Uses vectorized regex matching and spaCy's NER.
    """
    inverted_index = defaultdict(list)
    
    # Precompile regex patterns for each genuine brand.
    regex_patterns = {
        brand: re.compile(r'\b' + re.escape(brand) + r'\b', re.IGNORECASE)
        for brand in genuine_list
    }
    
    # Vectorized regex matching.
    lower_tweets = df['tweets'].str.lower()
    for brand, pattern in regex_patterns.items():
        # Use pandas vectorized string matching for each brand.
        matches = lower_tweets.str.contains(pattern)
        indices = df.index[matches].tolist()
        if indices:
            inverted_index[brand].extend(indices)
    
    # Batch process tweets with spaCy for NER.
    docs = list(nlp.pipe(df['tweets'], batch_size=50))
    for idx, doc in enumerate(docs):
        for ent in doc.ents:
            if ent.label_ == "ORG":
                entity = ent.text.lower()
                if entity in genuine_list:
                    inverted_index[entity].append(df.index[idx])
                    
    return dict(inverted_index)

def search_multiple_brands(df, brands, genuine_list=genuine_brands, cutoff=0.6, nlp=None):
    """
    For each brand in the input list, validate it using fuzzy matching and then check
    if the validated brand appears in any tweet (using the precomputed inverted index).
    
    Returns:
      available_list: a list of validated brands that were found in tweets.
      not_available_list: a list of brands that were either not recognized or not found.
    """
    if nlp is None:
        nlp = spacy.load("en_core_web_sm")
    
    inverted_index = build_inverted_index(df, genuine_list, nlp)
    available_list = []
    not_available_list = []
    
    for brand in brands:
        valid_brand = validate_brand(brand, genuine_list, cutoff)
        if valid_brand is None:
            not_available_list.append(brand)
        else:
            if valid_brand in inverted_index and inverted_index[valid_brand]:
                available_list.append(valid_brand)
            else:
                not_available_list.append(valid_brand)
    
    return available_list, not_available_list

# --- Example Usage ---
if __name__ == "__main__":
    # Create a sample DataFrame with tweets.
    data = {
        'tweets': [
            "I just bought a new Apple phone!",
            "Samsung's new release is impressive.",
            "Loving my new Nike sneakers.",
            "Nothing beats a cold Coca-Cola on a hot day.",
            "Just attended a tech conference.",
            "Apple's CEO announced a breakthrough today.",
            "The latest from Nike and Apple is trending."
        ]
    }
    df = pd.read_parquet("/Users/nelson/py/ml_App/trend-analysis/temp/mini_tweets_trend_analysis.parquet")
    
    # Example search: a list of search terms (even with typos).
    search_terms = ["aple", "samsung", "Nike", "nonexistentbrand", "coca cola"]
    available, not_available = search_multiple_brands(df, search_terms)
    
    print(f"Available Brands:{available}")
    
        
    print(f"Not Available Brands:{not_available}")
    