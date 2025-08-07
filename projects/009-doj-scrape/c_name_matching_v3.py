# c_name_matching_v2.py

import pandas as pd
import string
import jellyfish
from rapidfuzz import fuzz, process

# List of stopwords
STOPWORDS = set([
    'the', 'corporation', 'healthcare', 'Pharmaceuticals', 'pharma', 'resources', 'inc', 'us', 'sa', 'spa', 'ab', 'gmbh',
    'ag', 'asp', 'co', 'ltd', 'llc', 'bv', 'corp', 'formerly', 'previously', 'by', 'as', 'dba',
    'com', 'laboratories', 'systems', 'global', 'sales', 'products',
    'media group', 'group', 'limited', 'solutions', 'software', 'computing', 'networks',
    'brands', 'project', 'services', 'technologies', 'technology', 'companies', 'holdings', 'studios', 'digital',
    'mobile', 'financial', 'management', 'media', 'maintenance', 'agency', 'support', 'bank', 'reseller', 'account',
    'internal', 'external', 'international', 'university', 'labs', 'enterprises', 'industries',
    'incorporated', 'incorporation', 'entertainment', 'society', 'security', 'interactive', 'capital'
])

FUZZY_MATCH_THRESHOLD = 50
JELLYFISH_SCORE_THRESHOLD = 50
THEFUZZ_SCORE_THRESHOLD = 50
TOKEN_BASED_THRESHOLD = 50  # Score for exact token matches
USE_FIRST_CHARACTER_MATCH = True  # Set this to True or False based on your preference


# Define the enhanced preprocess function
def preprocess(name: str) -> str:
    """Manually preprocess a company name."""

    # Handle missing or NaN values
    if not isinstance(name, str):
        name = ""

    # Convert to lowercase
    name = name.lower()

    # Replace common abbreviations
    name = name.replace("corp.", "corporation")

    # Remove non-letter characters, preserving hyphens and spaces
    name = ''.join([char for char in name if char.isalpha() or char.isspace() or char == '-'])

    # Remove stopwords
    name = ' '.join([word for word in name.split() if word not in STOPWORDS])

    return name


# Define the token-based similarity function
def token_based_similarity(name1: str, name2: str) -> float:
    """Calculate a token-based similarity score between two names."""
    # Tokenize the names
    tokens1 = set(name1.split())
    tokens2 = set(name2.split())

    # Count the number of exact matches
    exact_matches = len(tokens1.intersection(tokens2))

    # Calculate the token-based similarity score
    avg_tokens = (len(tokens1) + len(tokens2)) / 2
    score = (exact_matches / avg_tokens) * 100  # Convert to percentage

    return score


# Define the enhanced find_matches function

def enhanced_find_matches(name_A: pd.Series, name_B: pd.Series, id_B: pd.Series) -> pd.DataFrame:
    """Find matches between two lists of names using multiple similarity measures."""
    # Preprocess the names
    name_A_processed = name_A.apply(preprocess)
    name_B_processed = name_B.apply(preprocess)

    # Placeholder for results
    results = []

    for index, name in name_A_processed.items():
        # Initialize a result dictionary with default values
        result_dict = {
            "Name A": name_A[index],
            "Name A PP": name,
            "ID B": None,
            "Name B": None,
            "Name B PP": None,
            "RapidFuzz Score": None,
            "Jellyfish Score": None,
            "TheFuzz Score": None,
            "Token-Based Score": None,
            "First-Character Score": None
        }

        for j, (id_b, name_b) in enumerate(zip(id_B, name_B_processed)):
            # Calculate similarity scores
            rapidfuzz_score = fuzz.ratio(name, name_b)
            jellyfish_score = jellyfish.jaro_winkler_similarity(name, name_b) * 100
            thefuzz_score = fuzz.token_sort_ratio(name, name_b)
            token_score = token_based_similarity(name, name_b)

            # Calculate First-Character Score if USE_FIRST_CHARACTER_MATCH is set to True
            if USE_FIRST_CHARACTER_MATCH:
                if name and name_b:  # Check that both strings are not empty
                    first_char_score = 100 if name[0] == name_b[0] else 0
                else:
                    first_char_score = 0
            else:
                first_char_score = 0

            # Check if any of the scores meet the thresholds or if First-Character Score is 100% (when enabled)
            if (rapidfuzz_score >= FUZZY_MATCH_THRESHOLD and
                    jellyfish_score >= JELLYFISH_SCORE_THRESHOLD and
                    thefuzz_score >= THEFUZZ_SCORE_THRESHOLD and
                    token_score >= TOKEN_BASED_THRESHOLD and
                    (USE_FIRST_CHARACTER_MATCH or first_char_score == 100)):
                result_dict.update({
                    "ID B": str(id_b),
                    "Name B": name_B[j],
                    "Name B PP": name_b,
                    "RapidFuzz Score": round(rapidfuzz_score, 1),
                    "Jellyfish Score": round(jellyfish_score, 1),
                    "TheFuzz Score": round(thefuzz_score, 1),
                    "Token-Based Score": round(token_score, 1),
                    "First-Character Score": round(first_char_score, 1)
                })

        # Append the result dictionary to the results list
        results.append(result_dict)

    # Convert the results to a DataFrame
    df_results = pd.DataFrame(results)

    # Weighted Scoring
    # Define weights
    w_rapidfuzz = 0.2
    w_jellyfish = 0.3
    w_thefuzz = 0.2
    w_token = 0.2
    w_first_char = 0.1

    # Calculate composite score
    df_results['Composite Score'] = (w_rapidfuzz * df_results['RapidFuzz Score'] +
                                     w_jellyfish * df_results['Jellyfish Score'] +  # Already normalized to 0-100 scale
                                     w_thefuzz * df_results['TheFuzz Score'] +
                                     w_token * df_results['Token-Based Score'] +
                                     w_first_char * df_results['First-Character Score'])
    return df_results
