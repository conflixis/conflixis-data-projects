#!/usr/bin/env python3
"""
Quick check of columns in rx_op_enhanced_full to find year/date columns.
"""

import os
import json
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

def setup_client():
    """Setup BigQuery client with credentials."""
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        with open(env_file, 'r') as f:
            content = f.read()
            lines = content.split('\n')
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    if key == 'GCP_SERVICE_ACCOUNT_KEY':
                        if value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                    else:
                        value = value.strip().strip("'\"")
                    os.environ[key] = value
    
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    service_account_json = service_account_json.replace('\\\\n', '\\n')
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return bigquery.Client(credentials=credentials, project="data-analytics-389803")

def main():
    client = setup_client()
    
    # Check rx_op_enhanced_full columns
    query = """
    SELECT 
        column_name,
        data_type
    FROM `data-analytics-389803.conflixis_agent.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'rx_op_enhanced_full'
        AND (
            LOWER(column_name) LIKE '%year%' 
            OR LOWER(column_name) LIKE '%date%'
            OR LOWER(column_name) LIKE '%time%'
            OR LOWER(column_name) LIKE '%period%'
        )
    ORDER BY ordinal_position
    """
    
    print("Checking rx_op_enhanced_full for date/year columns...")
    query_job = client.query(query)
    results = list(query_job.result())
    
    for row in results:
        print(f"  {row.column_name}: {row.data_type}")
    
    # Check sample values for year columns
    print("\nChecking sample values...")
    year_query = """
    SELECT DISTINCT
        year,
        CAST(year AS INT64) as year_int
    FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
    WHERE year IS NOT NULL
    ORDER BY year
    LIMIT 10
    """
    
    try:
        query_job = client.query(year_query)
        results = list(query_job.result())
        for row in results:
            print(f"  year: {row.year} -> INT64: {row.year_int}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()