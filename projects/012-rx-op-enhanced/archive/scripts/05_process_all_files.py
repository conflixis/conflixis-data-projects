#!/usr/bin/env python3
"""
Process ALL RDS files and upload to BigQuery
JIRA: DA-167

This script processes all 255 RDS files with:
1. Proper column standardization (handles compound manufacturer names)
2. Memory-efficient processing (one file at a time)
3. Direct upload to BigQuery in batches
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
TEMP_DIR = Path("temp-parquet")
PROJECT_ID = "data-analytics-389803"
DATASET_ID = "conflixis_agent"
TABLE_ID = "rx_op_enhanced_full"  # Full dataset table
BATCH_SIZE = 20  # Process N files at a time before uploading

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
    
    client = bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials
    )
    
    return client

def standardize_columns(df, manufacturer):
    """
    Standardize manufacturer-specific column names using regex.
    Handles compound manufacturer names like 'janssen_biotech'.
    """
    col_mapping = {}
    
    # Create regex pattern to match manufacturer-specific average columns
    # This will match any column that starts with a manufacturer name variant
    # and contains _avg_lag or _avg_lead
    
    # Handle different possible formats of manufacturer name
    # e.g., "janssen_biotech" could appear as "janssen_biotech", "janssen", "biotech"
    manufacturer_parts = manufacturer.split('_')
    
    for col in df.columns:
        # Check if this is a manufacturer-specific average column
        if '_avg_lag' in col or '_avg_lead' in col:
            # Check if any part of the manufacturer name is in the column
            for part in manufacturer_parts + [manufacturer]:
                if col.startswith(f"{part}_avg_"):
                    # Replace the manufacturer-specific prefix with generic 'mfg'
                    new_col = re.sub(r'^[^_]+(_avg_(?:lag|lead)\d+)$', r'mfg\1', col)
                    col_mapping[col] = new_col
                    break
            
            # Also try exact match with full manufacturer name
            if col.startswith(f"{manufacturer}_avg_"):
                new_col = col.replace(f"{manufacturer}_avg_", "mfg_avg_")
                col_mapping[col] = new_col
    
    if col_mapping:
        print(f"      Standardizing {len(col_mapping)} columns")
        df = df.rename(columns=col_mapping)
    
    return df

def process_file(file_path):
    """Process a single RDS file and return dataframe."""
    try:
        # Read RDS file
        result = pyreadr.read_r(str(file_path))
        df = result[None]
        
        # Extract metadata from filename
        parts = file_path.stem.replace('df_spec_', '').split('_')
        manufacturer = parts[0] if parts else 'unknown'
        
        # For compound names, try to get full manufacturer
        if len(parts) > 2:  # Likely has manufacturer_name_specialty format
            # Check if the second part might be part of manufacturer name
            # by looking at common specialty names
            common_specialties = ['cardiology', 'rheumatology', 'ophthalmology', 
                                 'gastroenterology', 'endocrinology', 'pulmonology',
                                 'nephrology', 'infectious', 'hematology', 'orthopedics']
            
            if parts[1] not in common_specialties and not parts[1].startswith('hem'):
                # Likely compound manufacturer name
                manufacturer = f"{parts[0]}_{parts[1]}"
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
        
        return df
        
    except Exception as e:
        print(f"    ✗ Error processing {file_path.name}: {e}")
        return None

def upload_batch_to_bigquery(dfs, client, table_ref, append=True):
    """Upload a batch of dataframes to BigQuery."""
    if not dfs:
        return False
    
    # Combine dataframes
    combined = pd.concat(dfs, ignore_index=True)
    print(f"    Uploading {len(combined):,} rows to BigQuery...")
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND" if append else "WRITE_TRUNCATE",
        autodetect=True,
    )
    
    # Load dataframe
    job = client.load_table_from_dataframe(
        combined, 
        table_ref, 
        job_config=job_config
    )
    
    # Wait for job to complete
    job.result()
    
    # Clean memory
    del combined
    gc.collect()
    
    return True

def process_all_files():
    """Process all RDS files and upload to BigQuery."""
    print("=" * 70)
    print("Processing ALL RDS Files")
    print("=" * 70)
    print(f"Source: {LOCAL_DATA_DIR}")
    print(f"Target: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print(f"Batch size: {BATCH_SIZE} files")
    print("-" * 70)
    
    # Get all RDS files
    rds_files = sorted(LOCAL_DATA_DIR.glob("*.rds"))
    total_files = len(rds_files)
    
    if total_files == 0:
        print("✗ No RDS files found!")
        return False
    
    print(f"Found {total_files} RDS files to process")
    
    # Get BigQuery client
    print("\nConnecting to BigQuery...")
    client = get_bigquery_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Process in batches
    batch_dfs = []
    processed = 0
    failed = 0
    total_rows = 0
    first_batch = True
    
    start_time = time.time()
    
    for i, file_path in enumerate(rds_files, 1):
        file_size_mb = file_path.stat().st_size / (1024**2)
        print(f"\n[{i:3d}/{total_files}] {file_path.name} ({file_size_mb:.1f} MB)")
        
        # Process file
        df = process_file(file_path)
        
        if df is not None:
            print(f"    ✓ Processed {len(df):,} rows")
            batch_dfs.append(df)
            processed += 1
            total_rows += len(df)
            
            # Upload batch when full or last file
            if len(batch_dfs) >= BATCH_SIZE or i == total_files:
                print(f"\n  Batch upload ({len(batch_dfs)} files)...")
                success = upload_batch_to_bigquery(
                    batch_dfs, 
                    client, 
                    table_ref, 
                    append=not first_batch
                )
                
                if success:
                    print(f"    ✓ Batch uploaded successfully")
                    first_batch = False
                else:
                    print(f"    ✗ Batch upload failed")
                
                # Clear batch
                batch_dfs = []
                gc.collect()
                
                # Progress update
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                eta = (total_files - i) / rate if rate > 0 else 0
                
                print(f"\n  Progress: {i}/{total_files} files ({i*100/total_files:.1f}%)")
                print(f"  Elapsed: {elapsed/60:.1f} min | Rate: {rate:.1f} files/sec")
                print(f"  ETA: {eta/60:.1f} min")
        else:
            failed += 1
    
    # Final summary
    elapsed_total = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("PROCESSING COMPLETE")
    print("=" * 70)
    print(f"Files processed: {processed}/{total_files}")
    print(f"Files failed: {failed}")
    print(f"Total rows: {total_rows:,}")
    print(f"Time elapsed: {elapsed_total/60:.1f} minutes")
    print(f"Average: {elapsed_total/processed:.1f} sec/file")
    
    # Verify in BigQuery
    print("\nVerifying in BigQuery...")
    table = client.get_table(table_ref)
    print(f"Table: {table_ref}")
    print(f"  Total rows: {table.num_rows:,}")
    print(f"  Total size: {table.num_bytes / (1024**3):.2f} GB")
    print(f"  Total columns: {len(table.schema)}")
    
    # Query manufacturer distribution
    query = f"""
    SELECT 
        source_manufacturer,
        COUNT(*) as row_count,
        COUNT(DISTINCT source_specialty) as specialties
    FROM `{table_ref}`
    GROUP BY source_manufacturer
    ORDER BY row_count DESC
    """
    
    print("\nManufacturer distribution:")
    query_job = client.query(query)
    for row in query_job.result():
        print(f"  {row.source_manufacturer}: {row.row_count:,} rows, {row.specialties} specialties")
    
    print(f"\n✓ Full dataset available at: {table_ref}")
    
    return True

def main():
    """Main execution."""
    print("RX-OP Enhanced: Full Processing Pipeline")
    print("=" * 70)
    
    # Check for RDS files
    rds_count = len(list(LOCAL_DATA_DIR.glob("*.rds")))
    if rds_count == 0:
        print("✗ No RDS files found. Please run download script first.")
        return
    
    print(f"Ready to process {rds_count} RDS files")
    print("\nThis will:")
    print("1. Process all RDS files with proper column standardization")
    print("2. Upload to BigQuery in batches")
    print("3. Create a single combined table")
    print(f"\nEstimated time: {rds_count * 2 / 60:.1f} minutes")
    
    # Process all files
    success = process_all_files()
    
    if success:
        print("\n" + "=" * 70)
        print("SUCCESS!")
        print("=" * 70)
        print("All files processed and uploaded to BigQuery")
        print(f"Table: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    else:
        print("\n✗ Processing failed")

if __name__ == "__main__":
    main()