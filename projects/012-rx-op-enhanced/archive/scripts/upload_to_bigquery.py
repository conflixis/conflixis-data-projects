#!/usr/bin/env python3
'''
Upload Parquet files to BigQuery
JIRA: DA-167
'''

import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv('../../.env')

# Configuration
PROJECT_ID = 'data-analytics-389803'
DATASET_ID = 'conflixis_agent'
PARQUET_DIR = Path('parquet-data')

# Load credentials from environment
service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Files to upload
files = [
    {
        'parquet_file': 'df_spec_abbvie_internal_medicine.parquet',
        'table_name': 'df_spec_abbvie_internal_medicine',
        'rows': 2149694,
        'columns': 27
    },

]

def upload_to_bigquery():
    '''Upload each Parquet file to BigQuery.'''
    
    for file_info in files:
        parquet_path = PARQUET_DIR / file_info['parquet_file']
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{file_info['table_name']}"
        
        print(f"\nUploading {file_info['parquet_file']} to {table_id}")
        print(f"  Rows: {file_info['rows']:,}, Columns: {file_info['columns']}")
        
        try:
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            
            # Load data
            with open(parquet_path, 'rb') as source_file:
                job = client.load_table_from_file(
                    source_file,
                    table_id,
                    job_config=job_config
                )
            
            # Wait for job to complete
            job.result()
            
            # Get table info
            table = client.get_table(table_id)
            print(f"  ✓ Uploaded successfully. Table has {table.num_rows:,} rows")
            
        except Exception as e:
            print(f"  ✗ Error uploading: {e}")
            continue

if __name__ == "__main__":
    print("Starting BigQuery upload...")
    print(f"Target dataset: {PROJECT_ID}.{DATASET_ID}")
    upload_to_bigquery()
    print("\nUpload complete!")
