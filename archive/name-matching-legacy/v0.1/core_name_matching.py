# ----------------------------- Imports -----------------------------
import os
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd
import jellyfish
from rapidfuzz import fuzz

# ----------------------------- Configurable Parameters -----------------------------
script_dir = os.path.dirname(os.path.abspath(__file__))

# Stopwords for preprocessing
# STOPWORDS = set([ "ab, account, ag, agency, as, asp, bank, biosurgery, brands, bv, by, capital, care, center, co, com, companies, computing, corporation, corp, division, digital, dba, entertainment, enterprises, external, financial, formerly, gmbh, global, group, health, healthcare, headquarters, holdings, inc, incorporated, incorporation, information, industries, interactive, internal, international, labs, laboratories, limited, llc, ltd, management, maintenance, media, media group, medical, medicine, mobile, networks, neurological, nutrition, pharma, pharmaceutical, pharmaceuticals, previously, products, project, region, resources, reseller, respiratory, sales, sa, security, service, services, society, software, solutions, spa, studios, support, surgical, systems, technologies, technology, the, therapy, trauma, university, us, usa"])

STOPWORDS = set(['stopword'])


# Input file paths (relative)
FILE_A = os.path.join(script_dir, 'input', 'dh_hospitals.csv')
FILE_B = os.path.join(script_dir, 'input', 'PE Hospital Tracker_trim.csv')

# Output file directory (relative)
OUTPUT_DIR = os.path.join(script_dir, 'output')

# Column names and processing limit
COLUMN_NAME_A = 'name'  # Column in FILE_A
COLUMN_NAME_B = 'Name'  # Column in FILE_B
LIMIT = 100000  # Number of rows to process from FILE_A


# ----------------------------- Main Processing Functions -----------------------------
def preprocess(name: str) -> str:
    """Preprocess a name: lowercase, remove stopwords, and keep only alphanumeric characters."""
    if not isinstance(name, str):
        return ""
    name = name.lower().replace("corp.", "corporation")
    name = ''.join([char for char in name if char.isalnum() or char.isspace() or char == '-'])
    return ' '.join([word for word in name.split() if word not in STOPWORDS])


def token_based_similarity(name1: str, name2: str) -> float:
    """Calculate token-based similarity between two names."""
    tokens1, tokens2 = set(name1.split()), set(name2.split())
    exact_matches = len(tokens1.intersection(tokens2))
    avg_tokens = (len(tokens1) + len(tokens2)) / 2
    return (exact_matches / avg_tokens) * 100 if avg_tokens > 0 else 0


def first_word_similarity(name1: str, name2: str) -> float:
    """Return 100 if the first words match, otherwise 0."""
    first_word1 = name1.split()[0] if name1 else ""
    first_word2 = name2.split()[0] if name2 else ""
    return 100.0 if first_word1 == first_word2 else 0.0


def match_name_to_df(name: str, name_A: str, row_A: pd.Series, df_B: pd.DataFrame, column_name_b: str) -> dict:
    """
    For a given processed name from FILE_A, find the best match from FILE_B.
    The result includes:
      - Original COLUMN_NAME_A from FILE_A
      - Original COLUMN_NAME_B from FILE_B (if a match is found)
      - Processed names ("Name A PP" and "Name B PP")
      - All similarity scores.
    """
    result_dict = {
        COLUMN_NAME_A: name_A,  # Original name from FILE_A
        "Name A PP": name,  # Processed name from FILE_A
        COLUMN_NAME_B: None,  # To be filled with original name from FILE_B
        "Name B PP": None,  # Processed name from FILE_B
        "RapidFuzz Score": None,
        "Jellyfish Score": None,
        "TheFuzz Score": None,
        "Token-Based Score": None,
        "First-Word Score": None,
        "Partial Match Score": None,
        "Composite Score": None
    }

    best_match = None
    best_score = 0

    # Iterate over df_B using the cached preprocessed name 'Name B PP'
    for j, row_b in df_B.iterrows():
        name_b = row_b[column_name_b]
        name_b_processed = row_b['Name B PP']  # Cached preprocessed value
        rapidfuzz_score = fuzz.ratio(name, name_b_processed)
        jellyfish_score = jellyfish.jaro_winkler_similarity(name, name_b_processed) * 100
        thefuzz_score = fuzz.token_sort_ratio(name, name_b_processed)
        token_score = token_based_similarity(name, name_b_processed)
        first_word_score = first_word_similarity(name, name_b_processed)
        partial_match_score = fuzz.partial_ratio(name, name_b_processed)

        composite_score = (
                0.1 * rapidfuzz_score +
                0.2 * jellyfish_score +
                0.1 * thefuzz_score +
                0.1 * token_score +
                0.4 * first_word_score +
                0.1 * partial_match_score
        )

        if composite_score > best_score:
            best_score = composite_score
            best_match = {
                COLUMN_NAME_B: name_b,  # Original name from FILE_B
                "Name B PP": name_b_processed,
                "RapidFuzz Score": round(rapidfuzz_score, 1),
                "Jellyfish Score": round(jellyfish_score, 1),
                "TheFuzz Score": round(thefuzz_score, 1),
                "Token-Based Score": round(token_score, 1),
                "First-Word Score": round(first_word_score, 1),
                "Partial Match Score": round(partial_match_score, 1),
                "Composite Score": round(composite_score, 1)
            }

    if best_match:
        result_dict.update(best_match)
    return result_dict


def find_matches_chunk(chunk: pd.DataFrame, df_B: pd.DataFrame, column_name_a: str, column_name_b: str) -> list:
    """Match a chunk of rows from FILE_A against FILE_B."""
    results = []
    for _, row in chunk.iterrows():
        name = row[column_name_a]
        name_processed = preprocess(name)
        results.append(match_name_to_df(name_processed, name, row, df_B, column_name_b))
    return results


def enhanced_find_matches(df_A: pd.DataFrame, df_B: pd.DataFrame, column_name_a: str, column_name_b: str,
                          chunk_size: int = 50) -> pd.DataFrame:
    """Find matches using parallel processing with detailed progress tracking."""
    results = []
    total_rows = len(df_A)
    chunks = [df_A.iloc[i * chunk_size:(i + 1) * chunk_size] for i in range((total_rows // chunk_size) + 1)]
    print(f"Total rows: {total_rows}, processing in {len(chunks)} chunks (up to {chunk_size} rows each).")

    with ProcessPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(find_matches_chunk, chunk, df_B, column_name_a, column_name_b): len(chunk) for chunk
                   in chunks}
        total_processed = 0
        for future in as_completed(futures):
            chunk_result = future.result()
            results.extend(chunk_result)
            chunk_size_processed = futures[future]
            total_processed += chunk_size_processed
            print(
                f"Finished processing a chunk of {chunk_size_processed} rows. Total processed: {total_processed}/{total_rows} ({(total_processed / total_rows) * 100:.2f}%).")

    return pd.DataFrame(results)


# ----------------------------- Script Execution -----------------------------
if __name__ == "__main__":
    print("Loading input_data...")

    # Load input files
    df_A = pd.read_csv(FILE_A).head(LIMIT)
    df_B = pd.read_csv(FILE_B, delimiter=',', quotechar='"', skipinitialspace=True)

    # Clean up column names
    df_A.columns = df_A.columns.str.strip()
    df_B.columns = df_B.columns.str.strip()

    print("Columns in df_A:", df_A.columns.tolist())
    print("\nFirst few rows of df_A:")
    print(df_A.head())
    print("\nColumns in df_B:", df_B.columns.tolist())
    print("\nFirst few rows of df_B:")
    print(df_B.head())

    # Auto-select valid column for df_A if necessary
    possible_entity_columns = [col for col in df_A.columns if 'name' in col.lower() or 'entity' in col.lower()]
    if possible_entity_columns and COLUMN_NAME_A not in possible_entity_columns:
        print(f"\n'{COLUMN_NAME_A}' not found. Using '{possible_entity_columns[0]}' instead.")
        COLUMN_NAME_A = possible_entity_columns[0]

    if COLUMN_NAME_A not in df_A.columns:
        print("\nSuitable column for entity names in df_A not found. Check your configuration.")
        exit(1)
    if COLUMN_NAME_B not in df_B.columns:
        raise KeyError(f"Column '{COLUMN_NAME_B}' not found in FILE_B.")

    # Cache preprocessed names for df_B
    df_B['Name B PP'] = df_B[COLUMN_NAME_B].apply(preprocess)

    print("\nPerforming name matching...")
    matched_df = enhanced_find_matches(df_A, df_B, COLUMN_NAME_A, COLUMN_NAME_B)

    if not matched_df.empty:
        # Define the output columns
        output_columns = [
            COLUMN_NAME_A,
            "Name A PP",
            COLUMN_NAME_B,
            "Name B PP",
            "RapidFuzz Score",
            "Jellyfish Score",
            "TheFuzz Score",
            "Token-Based Score",
            "First-Word Score",
            "Partial Match Score",
            "Composite Score"
        ]
        matched_df = matched_df[output_columns]

        # Create a unique output file name and save the results
        file_a_base_name = os.path.basename(FILE_A).split('.')[0]
        unique_id = datetime.now().strftime("%Y%m%d%H%M%S")
        output_file = os.path.join(OUTPUT_DIR, f"{file_a_base_name}_matched_{unique_id}.csv")
        matched_df.to_csv(output_file, index=False)

        print(f"\nMatching complete. Results saved to {output_file}.")
    else:
        print("\nNo matches found or an error occurred during processing.")