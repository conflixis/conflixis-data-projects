
import os
from google.cloud import bigquery
from google.oauth2 import service_account
import json
from dotenv import load_dotenv

def get_gcp_credentials(env_var='GCP_SERVICE_ACCOUNT_KEY'):
    """Gets GCP credentials from environment variable."""
    dotenv_path = os.path.join('/home/incent/conflixis-data-projects/projects/190-template-v3', '.env')
    load_dotenv(dotenv_path=dotenv_path)
    key_json = os.getenv(env_var)
    if not key_json:
        raise ValueError(f"Environment variable {env_var} not set.")
    
    key_dict = json.loads(key_json)
    return service_account.Credentials.from_service_account_info(key_dict)

def main():
    """Prints the schema of a BigQuery table."""
    credentials = get_gcp_credentials()
    client = bigquery.Client(credentials=credentials, project='data-analytics-389803')
    table_id = 'data-analytics-389803.conflixis_agent.op_general_all_aggregate_static'
    
    try:
        table = client.get_table(table_id)
        for field in table.schema:
            print(f"Column name: {field.name}, Data type: {field.field_type}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
