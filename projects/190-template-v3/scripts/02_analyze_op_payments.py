
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

def analyze_op_payments(config, credentials):
    """
    Analyzes Open Payments data for the providers in the NPI list.
    """
    project_paths = config['project_paths']
    health_system = config['health_system']
    bq_config = config['bigquery']

    # Set up BigQuery client
    client = bigquery.Client(credentials=credentials, project=bq_config['project_id'])

    # Format table names
    npi_table_name = bq_config['npi_table'].replace('[abbrev]', health_system['short_name'])
    npi_table_id = f"{bq_config['project_id']}.{bq_config['dataset_id']}.{npi_table_name}"
    op_table_id = f"{bq_config['project_id']}.{bq_config['dataset_id']}.{bq_config['op_table']}"

    # Construct the query
    query = f"""
        SELECT
            op.covered_recipient_npi AS NPI,
            SUM(op.total_amount_of_payment_usdollars) AS total_payments,
            COUNT(DISTINCT op.applicable_manufacturer_or_applicable_gpo_making_payment_name) AS total_manufacturers,
            MIN(op.date_of_payment) AS first_payment_date,
            MAX(op.date_of_payment) AS last_payment_date
        FROM
            `{op_table_id}` AS op
        JOIN
            `{npi_table_id}` AS npi_list
        ON
            op.covered_recipient_npi = npi_list.NPI
        GROUP BY
            op.covered_recipient_npi
        ORDER BY
            total_payments DESC
    """

    try:
        print("Running Open Payments analysis query in BigQuery...")
        df = client.query(query).to_dataframe()
        print(f"Successfully queried {len(df)} records from BigQuery.")
    except Exception as e:
        print(f"An error occurred during the BigQuery query: {e}")
        try:
            print(f"--- Attempting to fetch schema for table: {op_table_id} ---")
            table = client.get_table(op_table_id)
            for field in table.schema:
                print(f"Column name: {field.name}, Data type: {field.field_type}")
        except Exception as schema_e:
            print(f"Could not fetch schema: {schema_e}")
        return

    # Save the results to a CSV file
    output_dir = os.path.join(project_paths['project_dir'], project_paths['output_dir'])
    os.makedirs(output_dir, exist_ok=True)
    
    output_filename = f"{health_system['short_name']}_op_payments_summary.csv"
    output_path = os.path.join(output_dir, output_filename)
    
    df.to_csv(output_path, index=False)
    print(f"Successfully saved Open Payments analysis to {output_path}")

def main():
    """Main function to load config and run the analysis."""
    print("--- Starting Step 2: Analyze Open Payments ---")
    try:
        config = load_config()
        credentials = get_gcp_credentials()
        analyze_op_payments(config, credentials)
    except (ValueError, RuntimeError) as e:
        print(f"Error: {e}")
    print("--- Finished Step 2 ---")

if __name__ == "__main__":
    from dotenv import load_dotenv
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)
    main()
