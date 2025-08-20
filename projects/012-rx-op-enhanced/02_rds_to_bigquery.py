#!/usr/bin/env python3
"""
Process RDS files and upload to BigQuery
JIRA: DA-167

This script:
1. Reads RDS files directly using pyreadr (no R conversion needed)
2. Processes one file at a time for memory efficiency
3. Standardizes manufacturer-specific columns to mfg_avg_*
4. Uploads immediately to BigQuery after each file
5. Handles all 255 files (~300M rows total)
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

# Load environment variables
load_dotenv('../../.env')

# Configuration
LOCAL_DATA_DIR = Path("mfg-spec-data")
PROJECT_ID = "data-analytics-389803"
DATASET_ID = "conflixis_agent"
TABLE_ID = "rx_op_enhanced_full"

def get_bigquery_client():
    """Create BigQuery client with service account."""
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("No service account key found in environment")
    
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)

def standardize_columns(df, manufacturer):
    """Standardize manufacturer-specific column names using regex."""
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

def process_and_upload_file(file_path, client, table_ref, is_first=False):
    """Process a single file and immediately upload to BigQuery."""
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
        
        # Upload immediately
        print(f"  Uploading...", end=' ')
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE" if is_first else "WRITE_APPEND",
            autodetect=True,
        )
        
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print("✓")
        
        rows_processed = len(df)
        
        # Clear memory immediately
        del df
        del result
        gc.collect()
        
        return rows_processed
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return 0

def main():
    """Process all files with minimal memory usage."""
    print("=" * 70)
    print("RX-OP Enhanced: Optimized Full Processing")
    print("=" * 70)
    print(f"Source: {LOCAL_DATA_DIR}")
    print(f"Target: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print("-" * 70)
    
    # Get all RDS files
    rds_files = sorted(LOCAL_DATA_DIR.glob("*.rds"))
    total_files = len(rds_files)
    
    if total_files == 0:
        print("✗ No RDS files found!")
        return
    
    print(f"Found {total_files} RDS files")
    print("Processing strategy: One file at a time with immediate upload")
    print("-" * 70)
    
    # Get BigQuery client
    print("Connecting to BigQuery...")
    client = get_bigquery_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    print(f"Table: {table_ref}\n")
    
    # Process files
    total_rows = 0
    processed = 0
    failed = 0
    start_time = time.time()
    
    for i, file_path in enumerate(rds_files, 1):
        print(f"[{i:3d}/{total_files}] {file_path.name}")
        
        rows = process_and_upload_file(
            file_path, 
            client, 
            table_ref, 
            is_first=(i == 1)
        )
        
        if rows > 0:
            processed += 1
            total_rows += rows
        else:
            failed += 1
        
        # Progress update every 10 files
        if i % 10 == 0 or i == total_files:
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (total_files - i) / rate if rate > 0 else 0
            
            print(f"\n--- Progress: {i}/{total_files} ({i*100/total_files:.1f}%) ---")
            print(f"    Processed: {processed} | Failed: {failed}")
            print(f"    Total rows: {total_rows:,}")
            print(f"    Elapsed: {elapsed/60:.1f} min | ETA: {eta/60:.1f} min")
            print()
    
    # Final summary
    elapsed_total = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("PROCESSING COMPLETE")
    print("=" * 70)
    print(f"Files processed: {processed}/{total_files}")
    print(f"Files failed: {failed}")
    print(f"Total rows uploaded: {total_rows:,}")
    print(f"Time elapsed: {elapsed_total/60:.1f} minutes")
    
    if processed > 0:
        print(f"Average: {elapsed_total/processed:.1f} sec/file")
        
        # Verify in BigQuery
        print("\nVerifying in BigQuery...")
        try:
            table = client.get_table(table_ref)
            print(f"✓ Table: {table_ref}")
            print(f"  Total rows: {table.num_rows:,}")
            print(f"  Total size: {table.num_bytes / (1024**3):.2f} GB")
            print(f"  Total columns: {len(table.schema)}")
            
            # Quick manufacturer count
            query = f"""
            SELECT COUNT(DISTINCT source_manufacturer) as manufacturers
            FROM `{table_ref}`
            """
            query_job = client.query(query)
            for row in query_job.result():
                print(f"  Unique manufacturers: {row.manufacturers}")
                
        except Exception as e:
            print(f"Could not verify table: {e}")
    
    print(f"\n✓ Done! Table available at: {table_ref}")

if __name__ == "__main__":
    main()