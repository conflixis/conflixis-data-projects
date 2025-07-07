#!/usr/bin/env python3
"""Check existing files in GCS bucket for the table."""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account

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


def check_files(config):
    """Check files in GCS bucket."""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            config['GOOGLE_APPLICATION_CREDENTIALS']
        )
        storage_client = storage.Client(
            credentials=credentials,
            project=config['PROJECT_ID']
        )
        
        bucket = storage_client.bucket(config['GCS_BUCKET'])
        
        # Check for PHYSICIAN_GROUPS_PRACTICE_LOCATIONS files
        table_name = "PHYSICIAN_GROUPS_PRACTICE_LOCATIONS"
        prefix = f"{table_name}/"
        
        logger.info(f"Checking for files with prefix: {prefix}")
        
        blobs = list(bucket.list_blobs(prefix=prefix))
        if blobs:
            logger.info(f"\nFound {len(blobs)} existing files for {table_name}:")
            for blob in blobs[:10]:  # Show first 10
                logger.info(f"  - {blob.name} (size: {blob.size:,} bytes)")
                
            # Check if we can delete them (test one)
            try:
                test_blob = blobs[0]
                logger.info(f"\nTrying to delete test file: {test_blob.name}")
                test_blob.delete()
                logger.info("✓ Successfully deleted test file")
            except Exception as e:
                logger.warning(f"✗ Cannot delete files: {e}")
                logger.info("Files might be protected by retention policy")
        else:
            logger.info(f"No existing files found for {table_name}")
        
        # List all prefixes in the bucket
        logger.info("\n" + "="*60)
        logger.info("ALL TABLE PREFIXES IN BUCKET:")
        logger.info("="*60)
        
        # Get unique prefixes
        prefixes = set()
        for blob in bucket.list_blobs():
            if "/" in blob.name:
                prefix = blob.name.split("/")[0]
                prefixes.add(prefix)
        
        for prefix in sorted(prefixes):
            logger.info(f"  - {prefix}/")
            
    except Exception as e:
        logger.error(f"Error: {e}")


def main():
    """Main function."""
    config = load_config()
    check_files(config)


if __name__ == "__main__":
    main()