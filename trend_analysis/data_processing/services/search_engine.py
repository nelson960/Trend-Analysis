import pandas as pd
import re
import difflib
import spacy

# curated list of genuine brands.
genuine_brands = ['apple', 'coca-cola', "nike", "samsung", "google", "microsoft", "amazon"]

def validate_brand(brand, genuine_list=genuine_brands, cutoff=0.6):
    """
    Validate (and potentially correct) the brand name using fuzzy matching.
    """
    # If it's an exact match, return it.
    if brand in genuine_list:
        return brand
    # Otherwise, try to find the closest match using fuzzy matching.
    close_matches = difflib.get_close_matches(brand, genuine_list, n=1, cutoff=cutoff)
    return close_matches[0] if close_matches else None

def search_brand_regex(df, brand):
    """
    Search the DataFrame's tweets column for the brand using a regular expression.
    """
    # Build a regex pattern with word boundaries.
    pattern = re.compile(r'\b' + re.escape(brand) + r'\b', re.IGNORECASE)
    return df[df['tweets'].str.contains(pattern, na=False)]

def search_brand_spacy(df, brand, nlp):
    """
    Search the DataFrame's tweets column for the brand using spaCy's Named Entity Recognition (NER).
    
    """
    # Work on a copy so as not to modify the original DataFrame.
    df_temp = df.copy()

    def check_brand(tweet):
        doc = nlp(tweet)
        # Look for entities labeled as ORG that match the brand (case-insensitive).
        for ent in doc.ents:
            if ent.label_ == "ORG" and ent.text.lower() == brand.lower():
                return True
        return False

    df_temp['brand_found'] = df_temp['tweets'].apply(check_brand)
    result = df_temp[df_temp['brand_found']].drop(columns=['brand_found'])
    return result

def search_brand_combined_return_brand(df, brand, genuine_list=genuine_brands, cutoff=0.6, nlp=None):
    """
    Validate the brand using fuzzy matching, then search the tweets column using both regex and spaCy NER.
    If the brand is found in at least one tweet, return the validated brand; otherwise, return None.
    """
    # Validate (or correct) the input brand.
    validated_brand = validate_brand(brand, genuine_list, cutoff)
    if validated_brand is None:
        print(f"Brand '{brand}' is not recognized as a genuine brand.")
        return None

    # Search using regex.
    df_regex = search_brand_regex(df, validated_brand)
    
    # Load spaCy model if not provided.
    if nlp is None:
        nlp = spacy.load("en_core_web_sm")
    # Search using spaCy's NER.
    df_spacy = search_brand_spacy(df, validated_brand, nlp)
    
    # Combine results from both methods.
    df_combined = pd.concat([df_regex, df_spacy]).drop_duplicates()
    
    if df_combined.empty:
        # Brand not found in any tweet.
        return None

    # If at least one tweet contains the brand, return the validated brand.
    return validated_brand

def search_multiple_brands(df, brands, genuine_list=genuine_brands, cutoff=0.6, nlp=None):
    """
    Search for multiple brands in the DataFrame's tweets column.
    
    For each brand in the input list, validate it and check if it appears in any tweet
    (using both regex and spaCy NER). Return a list of validated brands that are found.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing a 'tweets' column.
        brands (list): List of brand strings to search for.
        genuine_list (list): List of genuine brands.
        cutoff (float): Similarity threshold for fuzzy matching.
        nlp: spaCy language model; if None, loads 'en_core_web_sm'.
        
    Returns:
        list: Validated brands that were found in at least one tweet.
    """
    found_brands = []
    # Load the spaCy model once if not provided.
    if nlp is None:
        nlp = spacy.load("en_core_web_sm")
    
    for brand in brands:
        result = search_brand_combined_return_brand(df, brand, genuine_list, cutoff, nlp)
        if result is not None:
            found_brands.append(result)
    
    # Remove duplicates (if any) and return the result.
    return list(set(found_brands))





# # --- Example Usage ---
# if __name__ == "__main__":
#     # Create a sample DataFrame with tweets.
#     data = {
#         'tweets': [
#             "I just bought a new Apple phone!",
#             "Samsung's new release is impressive.",
#             "Loving my new Nike sneakers.",
#             "Nothing beats a cold Coca-Cola on a hot day.",
#             "Just attended a tech conference.",
#             "Apple's CEO announced a breakthrough today.",
#             "The latest from Nike and Apple is trending."
#         ]
#     }
#     df = pd.DataFrame(data)
    
#     # Example search: a list of search terms (even with typos)
#     search_terms = ["aple", "samsung", "Nike", "nonexistentbrand", "coca cola"]
#     found = search_multiple_brands(df, search_terms)
    
#     if found:
#         print("Found the following brands in tweets:")
#         print(found)
#     else:
#         print("None of the searched brands were found in the tweets.")
