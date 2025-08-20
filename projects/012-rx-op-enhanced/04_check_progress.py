#!/usr/bin/env python3
"""
Check current progress of BigQuery upload
"""

from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from dotenv import load_dotenv

load_dotenv('../../.env')

PROJECT_ID = "data-analytics-389803"
DATASET_ID = "conflixis_agent"
TABLE_ID = "rx_op_enhanced_full"

def get_bigquery_client():
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("No service account key found in environment")
    
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)

def check_table():
    client = get_bigquery_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    try:
        table = client.get_table(table_ref)
        print(f"Table: {table_ref}")
        print(f"  Total rows: {table.num_rows:,}")
        print(f"  Total size: {table.num_bytes / (1024**3):.2f} GB")
        print(f"  Created: {table.created}")
        print(f"  Modified: {table.modified}")
        
        # Get unique files processed
        query = f"""
        SELECT 
            COUNT(DISTINCT source_file) as files_processed,
            COUNT(DISTINCT source_manufacturer) as manufacturers,
            COUNT(*) as total_rows,
            MIN(processed_at) as first_processed,
            MAX(processed_at) as last_processed
        FROM `{table_ref}`
        """
        
        print("\nProcessing stats:")
        query_job = client.query(query)
        for row in query_job.result():
            print(f"  Files processed: {row.files_processed}")
            print(f"  Manufacturers: {row.manufacturers}")
            print(f"  Total rows: {row.total_rows:,}")
            print(f"  First processed: {row.first_processed}")
            print(f"  Last processed: {row.last_processed}")
        
        # Get manufacturer breakdown
        query2 = f"""
        SELECT 
            source_manufacturer,
            COUNT(DISTINCT source_file) as files,
            COUNT(*) as row_count
        FROM `{table_ref}`
        GROUP BY source_manufacturer
        ORDER BY row_count DESC
        LIMIT 10
        """
        
        print("\nTop manufacturers:")
        query_job2 = client.query(query2)
        for row in query_job2.result():
            print(f"  {row.source_manufacturer}: {row.files} files, {row.row_count:,} rows")
            
    except Exception as e:
        print(f"Error checking table: {e}")
        print("\nTable might not exist or be incomplete")

if __name__ == "__main__":
    check_table()