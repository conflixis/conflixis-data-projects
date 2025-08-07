import os
import pandas as pd

# ----------------------------------------------
# Set Working Directory to Project Root
# ----------------------------------------------
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, ".."))  # Move up to conflixis-ai
os.chdir(project_root)  # Set working directory to project root

# ----------------------------------------------
# Configurable Parameters
# ----------------------------------------------
input_file_path = os.path.join("dojscrape", "doj004b_typology.csv")
output_file_path = os.path.join("dojscrape", "doj005a_pp_typology.csv")
supplier_file_path = os.path.join("dojscrape", "doj004a_match_supplier.csv")
final_output_file_path = os.path.join("dojscrape", "doj005b_pp_typology_with_supplier.csv")


# ----------------------------------------------
# Main Processing Functions
# ----------------------------------------------
def filter_csv_by_composite_score():
    # Load the CSV file into a DataFrame
    df = pd.read_csv(input_file_path, low_memory=False)

    # Filter rows where 'Composite Score' is greater than or equal to 85
    filtered_df = df[df['Composite Score'] >= 85]

    # Save the filtered DataFrame to a new CSV file
    filtered_df.to_csv(output_file_path, index=False)


def append_supplier_to_typology():
    # Load the typology and supplier CSV files
    df_typology = pd.read_csv(output_file_path, low_memory=False)
    df_supplier = pd.read_csv(supplier_file_path, low_memory=False)

    # Merge the DataFrames on the 'ID A' column
    merged_df = pd.merge(df_typology, df_supplier, how='left', left_on='ID A', right_on='URL')

    # Save the merged DataFrame to a new CSV file
    merged_df.to_csv(final_output_file_path, index=False)


# ----------------------------------------------
# Script Execution
# ----------------------------------------------
if __name__ == "__main__":
    filter_csv_by_composite_score()
    append_supplier_to_typology()
    print("Script completed. Filtered and appended CSVs have been saved.")
