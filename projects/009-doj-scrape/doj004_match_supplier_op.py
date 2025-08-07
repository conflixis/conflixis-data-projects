import json
import os
import pandas as pd
from tqdm import tqdm
from dojscrape.c_name_matching_v3 import enhanced_find_matches  # Adjusted import path
import time

# Ensure script runs from the project root
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, ".."))  # Move up to conflixis-ai
os.chdir(project_root)  # Set working directory to project root

# ----------------------------------------------
# Configurable Parameters for JSON Parsing
# ----------------------------------------------
input_file_path = os.path.join("dojscrape", "doj003_openai_output.csv")
output_file_path_reordered = os.path.join("dojscrape","doj004a_match_supplier.csv")

# ----------------------------------------------
# Main Processing Functions for JSON Parsing
# ----------------------------------------------
def parse_json_column(df):
    parsed_list = []
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Parsing JSON"):
        try:
            parsed_data = json.loads(row['Parsed_Response'])
        except json.JSONDecodeError:
            parsed_data = {"error": "Could not decode JSON"}
        parsed_list.append(parsed_data)
    return pd.DataFrame(parsed_list)

# Load the full CSV file
df = pd.read_csv(input_file_path)

# Parse the 'Parsed_Response' column
parsed_df = parse_json_column(df)

# Concatenate parsed DataFrame with only the 'URL' and 'LastMod' columns from the original DataFrame
final_df_reorder = pd.concat([df[['URL', 'LastMod']], parsed_df], axis=1)

# Remove duplicate columns
final_df_reorder = final_df_reorder.loc[:, ~final_df_reorder.columns.duplicated()]

# Save the DataFrame without duplicate columns to a new CSV file
final_df_reorder.to_csv(output_file_path_reordered, index=False)

# ----------------------------------------------
# Configurable Parameters for Name Matching
# ----------------------------------------------
file_path_set_a = output_file_path_reordered  # Using the output of the first part as input here
column_name_set_a = "Company Name"
id_column_set_a = "URL"

file_path_set_b = 'common/op_suppliers.txt'
column_index_set_b = 1  # 0-based index
id_column_index_set_b = 0  # 0-based index

output_file_path = 'dojscrape/doj004b_typology.csv'

# ----------------------------------------------
# Main Processing Functions for Name Matching
# ----------------------------------------------
def load_and_match_names():
    df_set_a = pd.read_csv(file_path_set_a)
    names_set_a = df_set_a[column_name_set_a]
    ids_set_a = df_set_a[id_column_set_a]

    df_set_b = pd.read_csv(file_path_set_b, delimiter='|')
    names_set_b = df_set_b.iloc[:, column_index_set_b]
    ids_set_b = df_set_b.iloc[:, id_column_index_set_b]

    start_time = time.time()

    df_results = enhanced_find_matches(names_set_a, names_set_b, ids_set_b)

    print(f"Time taken by enhanced_find_matches: {time.time() - start_time:.2f} seconds")

    df_results['ID A'] = ids_set_a

    cols_to_drop = ['RapidFuzz Score', 'Jellyfish Score', 'TheFuzz Score', 'Token-Based Score', 'First-Character Score']
    df_results.drop(cols_to_drop, axis=1, inplace=True)

    cols = ['ID A', 'ID B'] + [col for col in df_results if col not in ['ID A', 'ID B']]
    df_results = df_results[cols]
    df_results.to_csv(output_file_path, index=False)

# ----------------------------------------------
# Script Execution for Name Matching
# ----------------------------------------------
if __name__ == "__main__":
    load_and_match_names()
