
import os
import yaml
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json

def load_config():
    """Loads the configuration from the CONFIG.yaml file."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'CONFIG.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_gcp_credentials(env_var='GCP_SERVICE_ACCOUNT_KEY'):
    """Gets GCP credentials from environment variable."""
    key_json = os.getenv(env_var)
    if not key_json:
        raise ValueError(f"Environment variable {env_var} not set.")
    
    try:
        key_dict = json.loads(key_json)
        return service_account.Credentials.from_service_account_info(key_dict)
    except json.JSONDecodeError:
        raise ValueError("Failed to decode GCP service account key from JSON.")
    except Exception as e:
        raise RuntimeError(f"Failed to create credentials from service account info: {e}")


def upload_npi_list_to_bigquery(config, credentials):
    """
    Uploads the provider NPI list from a CSV file to a BigQuery table.
    """
    project_paths = config['project_paths']
    health_system = config['health_system']
    bq_config = config['bigquery']

    # Construct paths
    project_dir = project_paths['project_dir']
    npi_file_path = os.path.join(project_dir, health_system['npi_file'])
    
    # Read the NPI CSV file
    try:
        npi_df = pd.read_csv(npi_file_path)
        print(f"Successfully loaded NPI data from {npi_file_path}")
    except FileNotFoundError:
        print(f"Error: NPI file not found at {npi_file_path}")
        return

    if 'NPI' not in npi_df.columns:
        print("Error: The NPI CSV file must contain a column named 'NPI'.")
        return

    # Set up BigQuery client
    client = bigquery.Client(credentials=credentials, project=bq_config['project_id'])
    
    # Format the table name
    table_name = bq_config['npi_table'].replace('[abbrev]', health_system['short_name'])
    table_id = f"{bq_config['project_id']}.{bq_config['dataset_id']}.{table_name}"

    # Define job config
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Upload the data
    try:
        print(f"Uploading data to BigQuery table: {table_id}...")
        job = client.load_table_from_dataframe(npi_df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        print(f"Successfully uploaded {len(npi_df)} records to {table_id}")
        
        # Verify table exists
        table = client.get_table(table_id)
        print(f"Table {table_id} now has {table.num_rows} rows.")

    except Exception as e:
        print(f"An error occurred while uploading to BigQuery: {e}")


def main():
    """Main function to load config and run the upload process."""
    print("--- Starting Step 1: Upload NPI List to BigQuery ---")
    try:
        config = load_config()
        credentials = get_gcp_credentials()
        upload_npi_list_to_bigquery(config, credentials)
    except (ValueError, RuntimeError, FileNotFoundError) as e:
        print(f"Error: {e}")
    print("--- Finished Step 1 ---")

if __name__ == "__main__":
    # Load environment variables from .env file
    from dotenv import load_dotenv
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)
    main()
