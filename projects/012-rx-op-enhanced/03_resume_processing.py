#!/usr/bin/env python3
"""
Resume processing from interruptions
JIRA: DA-167

This script:
1. Queries BigQuery to find already processed files
2. Identifies remaining unprocessed RDS files
3. Continues processing from where it left off
4. Uses same processing logic as 02_rds_to_bigquery.py
5. Essential for handling the multi-hour processing pipeline
"""

import pyreadr
import pandas as pd
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
import time
import gc
import re
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('../../.env')

LOCAL_DATA_DIR = Path("mfg-spec-data")
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

def get_processed_files(client, table_ref):
    """Get list of already processed files from BigQuery."""
    query = f"""
    SELECT DISTINCT source_file
    FROM `{table_ref}`
    """
    
    query_job = client.query(query)
    processed_files = {row.source_file for row in query_job.result()}
    return processed_files

def standardize_columns(df, manufacturer):
    """Standardize manufacturer-specific column names."""
    col_mapping = {}
    
    for col in df.columns:
        if '_avg_lag' in col or '_avg_lead' in col:
            match = re.match(r'^(.+?)(_avg_(?:lag|lead)\d+)$', col)
            if match:
                prefix = match.group(1)
                suffix = match.group(2)
                
                if manufacturer in prefix or prefix in manufacturer:
                    new_col = f"mfg{suffix}"
                    col_mapping[col] = new_col
    
    if col_mapping:
        df = df.rename(columns=col_mapping)
    
    return df

def process_and_upload_file(file_path, client, table_ref):
    """Process a single file and upload to BigQuery."""
    try:
        file_size_mb = file_path.stat().st_size / (1024**2)
        
        # Read RDS file
        print(f"  Reading ({file_size_mb:.1f} MB)...", end=' ')
        result = pyreadr.read_r(str(file_path))
        df = result[None]
        print(f"{len(df):,} rows")
        
        # Extract metadata
        parts = file_path.stem.replace('df_spec_', '').split('_')
        manufacturer = parts[0] if parts else 'unknown'
        
        # Handle compound manufacturer names
        if len(parts) >= 3:
            if parts[1] in ['biotech', 'lilly', 'myers', 'squibb']:
                manufacturer = f"{parts[0]}_{parts[1]}"
                if parts[1] == 'myers' and len(parts) > 2 and parts[2] == 'squibb':
                    manufacturer = f"{parts[0]}_{parts[1]}_{parts[2]}"
                    specialty = '_'.join(parts[3:]) if len(parts) > 3 else 'unknown'
                else:
                    specialty = '_'.join(parts[2:]) if len(parts) > 2 else 'unknown'
            else:
                specialty = '_'.join(parts[1:]) if len(parts) > 1 else 'unknown'
        else:
            specialty = '_'.join(parts[1:]) if len(parts) > 1 else 'unknown'
        
        # Standardize columns
        df = standardize_columns(df, manufacturer)
        
        # Add metadata
        df['source_file'] = file_path.name
        df['source_manufacturer'] = manufacturer
        df['source_specialty'] = specialty
        df['processed_at'] = datetime.now().isoformat()
        
        # Upload with append mode
        print(f"  Uploading...", end=' ')
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True,
        )
        
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print("✓")
        
        rows_processed = len(df)
        
        # Clear memory
        del df
        del result
        gc.collect()
        
        return rows_processed
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return 0

def main():
    print("=" * 70)
    print("RX-OP Enhanced: Resume Processing")
    print("=" * 70)
    
    # Connect to BigQuery
    print("Connecting to BigQuery...")
    client = get_bigquery_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Get already processed files
    print("Checking already processed files...")
    processed_files = get_processed_files(client, table_ref)
    print(f"Found {len(processed_files)} files already processed")
    
    # Get all RDS files
    all_rds_files = sorted(LOCAL_DATA_DIR.glob("*.rds"))
    total_files = len(all_rds_files)
    
    # Filter to unprocessed files
    unprocessed_files = [f for f in all_rds_files if f.name not in processed_files]
    remaining = len(unprocessed_files)
    
    print(f"Total files: {total_files}")
    print(f"Already processed: {len(processed_files)}")
    print(f"Remaining to process: {remaining}")
    print("-" * 70)
    
    if remaining == 0:
        print("✓ All files already processed!")
        return
    
    # Process remaining files
    total_rows = 0
    processed = 0
    failed = 0
    start_time = time.time()
    
    for i, file_path in enumerate(unprocessed_files, 1):
        progress = len(processed_files) + i
        print(f"[{progress:3d}/{total_files}] {file_path.name}")
        
        rows = process_and_upload_file(file_path, client, table_ref)
        
        if rows > 0:
            processed += 1
            total_rows += rows
        else:
            failed += 1
        
        # Progress update every 10 files
        if i % 10 == 0 or i == remaining:
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (remaining - i) / rate if rate > 0 else 0
            
            print(f"\n--- Progress: {progress}/{total_files} ({progress*100/total_files:.1f}%) ---")
            print(f"    Processed this session: {processed} | Failed: {failed}")
            print(f"    Total rows added: {total_rows:,}")
            print(f"    Elapsed: {elapsed/60:.1f} min | ETA: {eta/60:.1f} min")
            print()
    
    # Final summary
    elapsed_total = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("PROCESSING COMPLETE")
    print("=" * 70)
    print(f"Files processed this session: {processed}/{remaining}")
    print(f"Files failed: {failed}")
    print(f"Rows added: {total_rows:,}")
    print(f"Time elapsed: {elapsed_total/60:.1f} minutes")
    
    # Final verification
    print("\nFinal table verification:")
    table = client.get_table(table_ref)
    print(f"✓ Table: {table_ref}")
    print(f"  Total rows: {table.num_rows:,}")
    print(f"  Total size: {table.num_bytes / (1024**3):.2f} GB")
    
    # Count unique files and manufacturers
    query = f"""
    SELECT 
        COUNT(DISTINCT source_file) as total_files,
        COUNT(DISTINCT source_manufacturer) as total_manufacturers
    FROM `{table_ref}`
    """
    
    query_job = client.query(query)
    for row in query_job.result():
        print(f"  Total files in table: {row.total_files}")
        print(f"  Total manufacturers: {row.total_manufacturers}")
    
    print(f"\n✓ Table available at: {table_ref}")

if __name__ == "__main__":
    main()