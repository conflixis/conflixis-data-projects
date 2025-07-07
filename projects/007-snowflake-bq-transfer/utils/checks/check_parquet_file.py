#!/usr/bin/env python3
"""Check Parquet file structure in GCS."""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
import pyarrow.parquet as pq
import io

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config():
    """Load configuration from environment variables."""
    current_path = Path(__file__).resolve()
    project_root = None
    for parent in current_path.parents:
        if (parent / "pyproject.toml").exists():
            project_root = parent
            break
    
    if project_root:
        env_path = project_root / "common" / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    
    return {
        'GCS_BUCKET': os.getenv('SNOWFLAKE_GCS_BUCKET', 'snowflake_dh_bq'),
        'GOOGLE_APPLICATION_CREDENTIALS': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        'PROJECT_ID': os.getenv('BQ_PROJECT_ID', os.getenv('GOOGLE_CLOUD_PROJECT')),
    }


def check_parquet(config):
    """Check Parquet file structure."""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            config['GOOGLE_APPLICATION_CREDENTIALS']
        )
        storage_client = storage.Client(
            credentials=credentials,
            project=config['PROJECT_ID']
        )
        
        bucket = storage_client.bucket(config['GCS_BUCKET'])
        
        # Get first Parquet file
        prefix = "PHYSICIAN_GROUPS_PRACTICE_LOCATIONS/"
        blobs = list(bucket.list_blobs(prefix=prefix, max_results=1))
        
        if not blobs:
            logger.error("No Parquet files found")
            return
        
        blob = blobs[0]
        logger.info(f"Checking Parquet file: {blob.name}")
        
        # Download to memory
        parquet_bytes = blob.download_as_bytes()
        
        # Read Parquet file
        parquet_file = pq.read_table(io.BytesIO(parquet_bytes))
        
        logger.info(f"\nParquet file info:")
        logger.info(f"  Number of rows: {parquet_file.num_rows:,}")
        logger.info(f"  Number of columns: {parquet_file.num_columns}")
        
        logger.info("\nColumn names in Parquet file:")
        for i, name in enumerate(parquet_file.column_names):
            logger.info(f"  {i+1}. {name}")
        
        # Show first few rows
        logger.info("\nFirst 3 rows of data:")
        df = parquet_file.to_pandas().head(3)
        for idx, row in df.iterrows():
            logger.info(f"\nRow {idx+1}:")
            for col in df.columns[:5]:  # Show first 5 columns
                logger.info(f"  {col}: {row[col]}")
                
    except Exception as e:
        logger.error(f"Error: {e}")


def main():
    """Main function."""
    config = load_config()
    check_parquet(config)


if __name__ == "__main__":
    main()