#!/usr/bin/env python3
"""
Re-process sample with fixed column standardization
JIRA: DA-167
"""

import pyreadr
import pandas as pd
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
import re
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv('../../.env')

LOCAL_DATA_DIR = Path("mfg-spec-data")
PROJECT_ID = "data-analytics-389803"
DATASET_ID = "conflixis_agent"
TABLE_ID = "rx_op_enhanced_sample_fixed"  # New table name for fixed sample

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
    """
    Fixed standardization that handles compound manufacturer names.
    """
    col_mapping = {}
    
    for col in df.columns:
        if '_avg_lag' in col or '_avg_lead' in col:
            # Use regex to extract pattern and replace manufacturer part
            match = re.match(r'^(.+?)(_avg_(?:lag|lead)\d+)$', col)
            if match:
                prefix = match.group(1)
                suffix = match.group(2)
                
                # Check if the prefix matches or contains the manufacturer
                if manufacturer in prefix or prefix in manufacturer:
                    new_col = f"mfg{suffix}"
                    col_mapping[col] = new_col
    
    if col_mapping:
        print(f"    Standardized {len(col_mapping)} columns")
        df = df.rename(columns=col_mapping)
    
    return df

def process_sample():
    """Process sample files with fixed standardization."""
    print("=" * 70)
    print("Processing Sample with Fixed Column Standardization")
    print("=" * 70)
    
    # Get diverse sample (one from each manufacturer)
    all_files = list(LOCAL_DATA_DIR.glob("*.rds"))
    sample_files = []
    manufacturers_seen = set()
    
    # Priority for compound manufacturer names to test the fix
    priority_manufacturers = ['janssen_biotech', 'eli_lilly', 'bristol_myers_squibb']
    
    for mfg in priority_manufacturers:
        for f in all_files:
            if mfg in f.name.lower():
                sample_files.append(f)
                manufacturers_seen.add(mfg)
                break
    
    # Add more to get to 10 files
    for f in sorted(all_files, key=lambda x: x.stat().st_size)[:20]:
        parts = f.stem.replace('df_spec_', '').split('_')
        manufacturer = parts[0] if parts else 'unknown'
        
        if manufacturer not in manufacturers_seen:
            sample_files.append(f)
            manufacturers_seen.add(manufacturer)
            if len(sample_files) >= 10:
                break
    
    print(f"Selected {len(sample_files)} sample files")
    print(f"Manufacturers: {', '.join(manufacturers_seen)}")
    print("-" * 70)
    
    all_dfs = []
    
    for i, file_path in enumerate(sample_files, 1):
        file_size_mb = file_path.stat().st_size / (1024**2)
        print(f"\n[{i:2d}] {file_path.name} ({file_size_mb:.1f} MB)")
        
        try:
            # Read RDS
            result = pyreadr.read_r(str(file_path))
            df = result[None]
            print(f"    Rows: {len(df):,}")
            
            # Extract metadata with compound name handling
            parts = file_path.stem.replace('df_spec_', '').split('_')
            manufacturer = parts[0] if parts else 'unknown'
            
            # Check for compound manufacturer names
            if len(parts) >= 3:
                # Check if second part might be part of manufacturer
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
            
            print(f"    Manufacturer: {manufacturer}")
            
            # Standardize columns
            df = standardize_columns(df, manufacturer)
            
            # Add metadata
            df['source_file'] = file_path.name
            df['source_manufacturer'] = manufacturer
            df['source_specialty'] = specialty
            df['processed_at'] = datetime.now().isoformat()
            
            all_dfs.append(df)
            
        except Exception as e:
            print(f"    ✗ Error: {e}")
    
    # Combine all dataframes
    print("\n" + "-" * 70)
    print("Combining dataframes...")
    combined = pd.concat(all_dfs, ignore_index=True)
    
    print(f"Combined: {len(combined):,} total rows")
    print(f"Memory usage: {combined.memory_usage(deep=True).sum() / (1024**2):.1f} MB")
    
    # Show standardized columns
    mfg_cols = [col for col in combined.columns if col.startswith('mfg_avg_')]
    print(f"\nStandardized columns found: {len(mfg_cols)}")
    if mfg_cols:
        print("Sample standardized columns:")
        for col in sorted(mfg_cols)[:8]:
            print(f"  - {col}")
    
    return combined

def upload_to_bigquery(df):
    """Upload dataframe to BigQuery."""
    print("\n" + "=" * 70)
    print("Uploading to BigQuery")
    print("=" * 70)
    
    client = get_bigquery_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    print(f"Target table: {table_ref}")
    print(f"Rows to upload: {len(df):,}")
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    
    # Load dataframe
    print("Uploading...")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    
    print("✓ Upload complete!")
    
    # Get table info
    table = client.get_table(table_ref)
    print(f"\nTable stats:")
    print(f"  Rows: {table.num_rows:,}")
    print(f"  Size: {table.num_bytes / (1024**2):.1f} MB")
    print(f"  Columns: {len(table.schema)}")
    
    # Query to verify standardization
    query = f"""
    SELECT 
        source_manufacturer,
        COUNT(*) as row_count,
        COUNT(DISTINCT source_specialty) as specialties
    FROM `{table_ref}`
    GROUP BY source_manufacturer
    ORDER BY row_count DESC
    """
    
    print(f"\nManufacturer distribution:")
    query_job = client.query(query)
    for row in query_job.result():
        print(f"  {row.source_manufacturer}: {row.row_count:,} rows, {row.specialties} specialties")
    
    # Check for standardized columns
    query2 = f"""
    SELECT column_name
    FROM `{PROJECT_ID}.{DATASET_ID}`.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{TABLE_ID}'
      AND column_name LIKE 'mfg_avg_%'
    ORDER BY column_name
    """
    
    print(f"\nStandardized columns in BigQuery:")
    query_job2 = client.query(query2)
    cols = [row.column_name for row in query_job2.result()]
    if cols:
        for col in cols:
            print(f"  - {col}")
    else:
        print("  No mfg_avg_* columns found - checking for issues...")
    
    print(f"\n✓ Table ready at: {table_ref}")
    
    return table

def main():
    """Main execution."""
    print("RX-OP Enhanced: Fixed Sample Processing")
    print("=" * 70)
    
    # Process sample
    df = process_sample()
    
    # Upload to BigQuery
    table = upload_to_bigquery(df)
    
    print("\n" + "=" * 70)
    print("SUCCESS!")
    print("=" * 70)
    print(f"Fixed sample uploaded to: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print("\nYou can now query the table in BigQuery to verify:")
    print("1. All manufacturer-specific columns are now mfg_avg_*")
    print("2. Data from all manufacturers uses consistent column names")

if __name__ == "__main__":
    main()