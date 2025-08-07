import os
import json
import pandas as pd


def process_json_files(directory):
    all_data = []  # List to store the expanded data_4o_websearch from each JSON file

    # Loop through each file in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)

            # Read the JSON file
            with open(file_path, 'r', encoding='utf-8') as file:
                json_data = json.load(file)

            # Normalize the JSON data_4o_websearch and append to the list
            normalized_data = pd.json_normalize(json_data)
            all_data.append(normalized_data)

    # Concatenate all data_4o_websearch into a single DataFrame
    expanded_data = pd.concat(all_data, ignore_index=True)
    return expanded_data


# Directory containing the JSON files
json_directory = r'C:\Users\vince\OneDrive\Documents\Conflixis\conflixis-ai\companydata\DATA\Company_JSONs'

# Process the JSON files and get the expanded DataFrame
result_df = process_json_files(json_directory)

# Save the DataFrame to a CSV file
output_csv_path = r'C:\Users\vince\OneDrive\Documents\Conflixis\conflixis-ai\companydata\DATA\expanded_company_data.csv'
result_df.to_csv(output_csv_path, index=False)
