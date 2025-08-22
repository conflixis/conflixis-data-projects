#!/usr/bin/env python3
"""
Download RDS files from Google Drive and convert to Parquet for BigQuery upload.
JIRA: DA-167
"""

import os
import sys
import gdown
import pyreadr
import pandas as pd
from pathlib import Path
from typing import List, Dict
import re

# Google Drive folder ID from the shared link
# https://drive.google.com/drive/folders/1X2ssg7Bto1gKEKO9e3NFiGJosnm7LMpT
FOLDER_ID = "1X2ssg7Bto1gKEKO9e3NFiGJosnm7LMpT"
LOCAL_DATA_DIR = Path("mfg-spec-data")
PARQUET_DIR = Path("parquet-data")

def download_from_drive_folder():
    """Download all files from Google Drive folder."""
    print(f"Downloading files from Google Drive folder: {FOLDER_ID}")
    
    # Create directories if they don't exist
    LOCAL_DATA_DIR.mkdir(exist_ok=True)
    PARQUET_DIR.mkdir(exist_ok=True)
    
    # Use gdown to download the entire folder
    url = f"https://drive.google.com/drive/folders/{FOLDER_ID}"
    
    try:
        # Download folder to local directory
        gdown.download_folder(url, output=str(LOCAL_DATA_DIR), quiet=False, use_cookies=False)
        print(f"Files downloaded to {LOCAL_DATA_DIR}")
        return True
    except Exception as e:
        print(f"Error downloading from Google Drive: {e}")
        print("You may need to manually download the files or adjust sharing permissions")
        return False

def convert_rds_to_parquet():
    """Convert all RDS files to Parquet format."""
    print("\nConverting RDS files to Parquet...")
    
    # Find all RDS files
    rds_files = list(LOCAL_DATA_DIR.glob("*.rds"))
    
    if not rds_files:
        print(f"No RDS files found in {LOCAL_DATA_DIR}")
        return []
    
    converted_files = []
    
    for rds_file in rds_files:
        print(f"\nProcessing: {rds_file.name}")
        
        try:
            # Read RDS file
            result = pyreadr.read_r(str(rds_file))
            
            # Get the first (and usually only) dataframe
            df_name = list(result.keys())[0]
            df = result[df_name]
            
            print(f"  Shape: {df.shape}")
            print(f"  Columns: {len(df.columns)}")
            
            # Create table name from filename
            # Remove .rds extension and clean up for BigQuery table naming
            table_name = rds_file.stem.lower()
            table_name = re.sub(r'[^a-z0-9_]', '_', table_name)
            
            # Save as Parquet
            parquet_file = PARQUET_DIR / f"{table_name}.parquet"
            df.to_parquet(parquet_file, engine='pyarrow', compression='snappy')
            
            print(f"  Saved to: {parquet_file}")
            
            converted_files.append({
                'rds_file': rds_file.name,
                'parquet_file': parquet_file.name,
                'table_name': table_name,
                'rows': df.shape[0],
                'columns': df.shape[1]
            })
            
        except Exception as e:
            print(f"  Error processing {rds_file.name}: {e}")
            continue
    
    return converted_files

def generate_upload_script(converted_files: List[Dict]):
    """Generate BigQuery upload script."""
    
    script_content = """#!/usr/bin/env python3
'''
Upload Parquet files to BigQuery
JIRA: DA-167
'''

import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from pathlib import Path

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
"""
    
    for file_info in converted_files:
        script_content += f"""    {{
        'parquet_file': '{file_info['parquet_file']}',
        'table_name': '{file_info['table_name']}',
        'rows': {file_info['rows']},
        'columns': {file_info['columns']}
    }},
"""
    
    script_content += """
]

def upload_to_bigquery():
    '''Upload each Parquet file to BigQuery.'''
    
    for file_info in files:
        parquet_path = PARQUET_DIR / file_info['parquet_file']
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{file_info['table_name']}"
        
        print(f"\\nUploading {file_info['parquet_file']} to {table_id}")
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
    print("\\nUpload complete!")
"""
    
    script_path = Path("upload_to_bigquery.py")
    script_path.write_text(script_content)
    print(f"\nGenerated upload script: {script_path}")
    
    return script_path

def main():
    """Main execution."""
    print("RX-OP Enhanced Data Pipeline")
    print("=" * 50)
    
    # Step 1: Download from Google Drive
    # Commenting out for now as it may require authentication
    # success = download_from_drive_folder()
    
    # Step 2: Convert existing RDS files to Parquet
    converted_files = convert_rds_to_parquet()
    
    if converted_files:
        print(f"\n✓ Successfully converted {len(converted_files)} files")
        
        # Step 3: Generate upload script
        generate_upload_script(converted_files)
        
        # Print summary
        print("\nSummary:")
        print("-" * 50)
        for file_info in converted_files:
            print(f"  {file_info['rds_file']} -> {file_info['table_name']}")
            print(f"    Rows: {file_info['rows']:,}, Columns: {file_info['columns']}")
    else:
        print("\n✗ No files were converted")

if __name__ == "__main__":
    main()