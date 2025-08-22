#!/usr/bin/env python3
"""
Combine Parquet files and upload to BigQuery
JIRA: DA-167
"""

import pandas as pd
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('../../.env')

OUTPUT_DIR = Path("parquet-data")
PROJECT_ID = "data-analytics-389803"
DATASET_ID = "conflixis_agent"
TABLE_ID = "rx_op_enhanced_sample"

def get_bigquery_client():
    """Create BigQuery client with service account."""
    # Get service account JSON from environment
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    
    if not service_account_json:
        raise ValueError("No service account key found in environment")
    
    # Parse the JSON
    service_account_info = json.loads(service_account_json)
    
    # Create credentials
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    
    # Create client
    client = bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials
    )
    
    return client

def combine_parquet_files():
    """Combine all parquet files."""
    print("Combining parquet files...")
    
    parquet_files = list(OUTPUT_DIR.glob("*.parquet"))
    print(f"Found {len(parquet_files)} parquet files")
    
    # Read and combine
    dfs = []
    for f in parquet_files:
        df = pd.read_parquet(f)
        dfs.append(df)
        print(f"  {f.name}: {len(df):,} rows")
    
    combined = pd.concat(dfs, ignore_index=True)
    print(f"\nCombined: {len(combined):,} total rows")
    
    return combined

def upload_to_bigquery(df):
    """Upload dataframe to BigQuery."""
    print("\n" + "=" * 70)
    print("Uploading to BigQuery")
    print("=" * 70)
    
    # Get client
    client = get_bigquery_client()
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Table: {TABLE_ID}")
    
    # Create table reference
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Replace table if exists
        autodetect=True,  # Auto-detect schema
    )
    
    print(f"\nUploading {len(df):,} rows...")
    
    # Load dataframe
    job = client.load_table_from_dataframe(
        df, 
        table_ref, 
        job_config=job_config
    )
    
    # Wait for job to complete
    job.result()
    
    print("✓ Upload complete!")
    
    # Get table info
    table = client.get_table(table_ref)
    print(f"\nTable stats:")
    print(f"  Rows: {table.num_rows:,}")
    print(f"  Size: {table.num_bytes / (1024**2):.1f} MB")
    print(f"  Columns: {len(table.schema)}")
    
    # Run a quick query to verify
    query = f"""
    SELECT 
        source_manufacturer,
        COUNT(*) as row_count
    FROM `{table_ref}`
    GROUP BY source_manufacturer
    ORDER BY row_count DESC
    """
    
    print(f"\nVerifying with query...")
    query_job = client.query(query)
    results = query_job.result()
    
    print("Manufacturer distribution:")
    for row in results:
        print(f"  {row.source_manufacturer}: {row.row_count:,} rows")
    
    print(f"\n✓ Table available at: {table_ref}")
    
    return table

def main():
    """Main execution."""
    print("=" * 70)
    print("Combine and Upload to BigQuery")
    print("=" * 70)
    
    # Combine parquet files
    df = combine_parquet_files()
    
    # Show sample
    print("\nSample data:")
    print(df.head())
    
    print(f"\nData shape: {df.shape}")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / (1024**2):.1f} MB")
    
    # Upload to BigQuery
    table = upload_to_bigquery(df)
    
    print("\n" + "=" * 70)
    print("SUCCESS!")
    print("=" * 70)
    print(f"Sample data uploaded to BigQuery")
    print(f"Table: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print(f"Rows: {len(df):,}")
    
    print("\nNext steps:")
    print("1. Verify data in BigQuery console")
    print("2. Process all 255 files")
    print("3. Upload complete dataset")

if __name__ == "__main__":
    main()