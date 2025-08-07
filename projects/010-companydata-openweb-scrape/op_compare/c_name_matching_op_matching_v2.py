# Imports
import pandas as pd
import jellyfish
from rapidfuzz import fuzz
from concurrent.futures import ProcessPoolExecutor, as_completed
import numpy as np
import os
from datetime import datetime

# ----------------------------- Configurable Parameters -----------------------------
# Stopwords to be removed during preprocessing
STOPWORDS = set([
    'the', 'surgical', 'headquarters', 'region', 'neurological', 'respiratory', 'biosurgery', 'pharmaceutical',
    'trauma', 'therapy', 'division', 'medicine', 'usa', 'us', 'group', 'nutrition', 'information', 'medical',
    'systems', 'corporation', 'healthcare', 'pharmaceuticals', 'pharma', 'resources', 'inc', 'us', 'sa', 'spa', 'ab',
    'gmbh', 'ag', 'asp', 'co', 'ltd', 'llc', 'bv', 'corp', 'formerly', 'previously', 'by', 'as', 'dba',
    'com', 'laboratories', 'systems', 'global', 'sales', 'products',
    'media group', 'group', 'limited', 'solutions', 'software', 'computing', 'networks',
    'brands', 'project', 'services', 'technologies', 'technology', 'companies', 'holdings', 'studios', 'digital',
    'mobile', 'financial', 'management', 'media', 'maintenance', 'agency', 'support', 'bank', 'reseller', 'account',
    'internal', 'external', 'international', 'university', 'labs', 'enterprises', 'industries',
    'incorporated', 'incorporation', 'entertainment', 'society', 'security', 'interactive', 'capital', 'health',
    'care', 'service', 'center', 'inc', 'ltd', 'company'
])

# Input files and column configurations
# FILE_A refers to the file requiring comparison to OP
FILE_A = r'C:\Users\vince\OneDrive\Documents\Conflixis\conflixis-ai\companydata\op_compare\input_file_a\dPmICX1fw2ZhjOEMaJoX_suppliers_2050211.csv'
# This is the OP file
FILE_B = r'C:\Users\vince\OneDrive\Documents\Conflixis\conflixis-ai\companydata\op_compare\input_op\bquxjob_51ba0854_191e570b00f_allgponamesid.csv'

COLUMN_NAME_A = 'entity_name'  # Default column name for entity names in FILE_A
COLUMN_NAME_B = 'GPOName'  # Column name for entity names in FILE_B
LIMIT = 50000  # Limit for the number of rows to process from FILE_A

# ----------------------------- Main Processing Functions -----------------------------
def preprocess(name: str) -> str:
    """Preprocesses a name by removing stopwords, converting to lowercase, and keeping only alphanumeric characters."""
    if not isinstance(name, str):
        return ""
    name = name.lower()
    name = name.replace("corp.", "corporation")
    name = ''.join([char for char in name if char.isalnum() or char.isspace() or char == '-'])
    name = ' '.join([word for word in name.split() if word not in STOPWORDS])
    return name

def token_based_similarity(name1: str, name2: str) -> float:
    """Calculates token-based similarity between two names."""
    tokens1 = set(name1.split())
    tokens2 = set(name2.split())
    exact_matches = len(tokens1.intersection(tokens2))
    avg_tokens = (len(tokens1) + len(tokens2)) / 2
    return (exact_matches / avg_tokens) * 100 if avg_tokens > 0 else 0

def first_word_similarity(name1: str, name2: str) -> float:
    """Checks if the first word in both names matches."""
    first_word1 = name1.split()[0] if name1 else ""
    first_word2 = name2.split()[0] if name2 else ""
    return 100.0 if first_word1 == first_word2 else 0.0

def match_name_to_df(name: str, name_A: str, row_A: pd.Series, df_B: pd.DataFrame, column_name_b: str) -> dict:
    """Matches a name from df_A to the best possible match in df_B using various similarity metrics."""
    result_dict = row_A.to_dict()
    result_dict.update({
        "Name A PP": name,
        "ID B": None,
        "Name B": None,
        "Name B PP": None,
        "RapidFuzz Score": None,
        "Jellyfish Score": None,
        "TheFuzz Score": None,
        "Token-Based Score": None,
        "First-Word Score": None,
        "Partial Match Score": None,
        "Composite Score": None
    })

    best_match = None
    best_score = 0

    # Iterate over each row in df_B to find the best match
    for j, row_b in df_B.iterrows():
        name_b = row_b[column_name_b]
        name_b_processed = preprocess(name_b)
        rapidfuzz_score = fuzz.ratio(name, name_b_processed)
        jellyfish_score = jellyfish.jaro_winkler_similarity(name, name_b_processed) * 100
        thefuzz_score = fuzz.token_sort_ratio(name, name_b_processed)
        token_score = token_based_similarity(name, name_b_processed)
        first_word_score = first_word_similarity(name, name_b_processed)
        partial_match_score = fuzz.partial_ratio(name, name_b_processed)

        # Composite score calculation with different weightages
        composite_score = (
            0.1 * rapidfuzz_score +
            0.2 * jellyfish_score +
            0.1 * thefuzz_score +
            0.1 * token_score +
            0.4 * first_word_score +
            0.1 * partial_match_score
        )

        # Update best match if a higher composite score is found
        if composite_score > best_score:
            best_score = composite_score
            best_match = {
                "ID B": j,
                "Name B": name_b,
                "Name B PP": name_b_processed,
                "RapidFuzz Score": round(rapidfuzz_score, 1),
                "Jellyfish Score": round(jellyfish_score, 1),
                "TheFuzz Score": round(thefuzz_score, 1),
                "Token-Based Score": round(token_score, 1),
                "First-Word Score": round(first_word_score, 1),
                "Partial Match Score": round(partial_match_score, 1),
                "Composite Score": round(composite_score, 1)
            }

    # Update result dictionary with the best match details
    if best_match:
        result_dict.update(best_match)
        for col in df_B.columns:
            result_dict[f"{col.strip()}_B"] = df_B.loc[best_match["ID B"], col.strip()]

    return result_dict

def find_matches_chunk(chunk: pd.DataFrame, df_B: pd.DataFrame, column_name_a: str, column_name_b: str) -> list:
    """Match a chunk of names from df_A against all names in df_B."""
    results = []
    for _, row in chunk.iterrows():
        name = row[column_name_a]
        name_processed = preprocess(name)
        result = match_name_to_df(name_processed, name, row, df_B, column_name_b)
        results.append(result)
    return results

def enhanced_find_matches(df_A: pd.DataFrame, df_B: pd.DataFrame, column_name_a: str, column_name_b: str, chunk_size: int = 50) -> pd.DataFrame:
    """Find matches using parallel processing for faster execution."""
    results = []
    total_rows = len(df_A)  # Total number of rows to process
    chunks = [df_A.iloc[i * chunk_size:(i + 1) * chunk_size] for i in range((total_rows // chunk_size) + 1)]

    # Use parallel processing to handle chunks of data_4o_websearch
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(find_matches_chunk, chunk, df_B, column_name_a, column_name_b): len(chunk) for chunk in chunks}
        total_processed = 0
        for future in as_completed(futures):
            result = future.result()
            results.extend(result)
            total_processed += futures[future]
            print(f"Progress: {total_processed}/{total_rows} ({(total_processed/total_rows)*100:.2f}%)")

    return pd.DataFrame(results)

# ----------------------------- Script Execution -----------------------------
if __name__ == "__main__":
    print("Loading data_4o_websearch...")

    # Load input files and limit rows for df_A
    df_A = pd.read_csv(FILE_A).head(LIMIT)
    df_B = pd.read_csv(FILE_B, delimiter=',', quotechar='"', skipinitialspace=True)

    # Clean up column names by stripping whitespace
    df_A.columns = df_A.columns.str.strip()
    df_B.columns = df_B.columns.str.strip()

    # Display initial columns and sample rows from both dataframes
    print("Columns in the first file (df_A):")
    print(df_A.columns.tolist())
    print("\nFirst few rows of df_A:")
    print(df_A.head())

    print("\nColumns in the second file (df_B):")
    print(df_B.columns.tolist())
    print("\nFirst few rows of df_B:")
    print(df_B.head())

    # Determine if COLUMN_NAME_A is valid or automatically select a suitable column
    possible_entity_columns = [col for col in df_A.columns if 'name' in col.lower() or 'entity' in col.lower()]
    if possible_entity_columns and COLUMN_NAME_A not in possible_entity_columns:
        print(f"\n'{COLUMN_NAME_A}' is not found in possible entity columns. Using '{possible_entity_columns[0]}' instead.")
        COLUMN_NAME_A = possible_entity_columns[0]

    # Verify that the necessary columns are present in both dataframes
    if COLUMN_NAME_A not in df_A.columns:
        print("\nCouldn't find a suitable column for entity names in df_A. Please specify the correct column name.")
        exit(1)

    if COLUMN_NAME_B not in df_B.columns:
        raise KeyError(f"Column '{COLUMN_NAME_B}' not found in the second file.")

    print("\nPerforming name matching...")
    matched_df = enhanced_find_matches(df_A, df_B, COLUMN_NAME_A, COLUMN_NAME_B)

    if not matched_df.empty:
        # Remove intermediate score columns but keep essential columns for output
        matched_df = matched_df.drop(columns=[
            'RapidFuzz Score', 'Jellyfish Score', 'TheFuzz Score',
            'Token-Based Score', 'First-Word Score', 'Partial Match Score',
            'ID B', 'Name B'
        ])

        # Reorganize columns for better readability in the output file
        reordered_columns = [COLUMN_NAME_A, 'Name A PP', 'Name B PP'] + \
                            [col for col in matched_df.columns if
                             col not in [COLUMN_NAME_A, 'Name A PP', 'Name B PP']]
        matched_df = matched_df[reordered_columns]

        # Generate a unique output file name and save the results
        file_a_base_name = os.path.basename(FILE_A).split('.')[0]
        unique_id = datetime.now().strftime("%Y%m%d%H%M%S")
        output_file = f"C:\\Users\\vince\\OneDrive\\Documents\\Conflixis\\conflixis-ai\\companydata\\op_compare\\output_file_b\\{file_a_base_name}_matched_{unique_id}.csv"

        matched_df.to_csv(output_file, index=False)
        print(f"\nMatching complete. Results saved to {output_file}.")
    else:
        print("\nNo matches found or an error occurred during processing.")
