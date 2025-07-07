#!/usr/bin/env python3
"""Delete all files in GCS bucket."""

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


def delete_all_files(config):
    """Delete all files in the bucket."""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            config['GOOGLE_APPLICATION_CREDENTIALS']
        )
        storage_client = storage.Client(
            credentials=credentials,
            project=config['PROJECT_ID']
        )
        
        bucket = storage_client.bucket(config['GCS_BUCKET'])
        logger.info(f"Bucket: gs://{config['GCS_BUCKET']}")
        
        # List all blobs
        blobs = list(bucket.list_blobs())
        total_files = len(blobs)
        
        if total_files == 0:
            logger.info("No files found in bucket.")
            return
        
        logger.info(f"Found {total_files} files to delete")
        
        deleted = 0
        failed = 0
        
        for i, blob in enumerate(blobs, 1):
            try:
                logger.info(f"[{i}/{total_files}] Deleting: {blob.name}")
                blob.delete()
                deleted += 1
            except Exception as e:
                logger.error(f"Failed to delete {blob.name}: {e}")
                failed += 1
        
        logger.info("\n" + "="*60)
        logger.info("SUMMARY:")
        logger.info(f"Total files: {total_files}")
        logger.info(f"Successfully deleted: {deleted}")
        logger.info(f"Failed to delete: {failed}")
        
        if failed > 0:
            logger.warning("\nSome files could not be deleted due to retention policies.")
            logger.warning("These files will be automatically deleted when their retention period expires.")
        
    except Exception as e:
        logger.error(f"Error: {e}")


def main():
    """Main function."""
    config = load_config()
    
    logger.info("=" * 60)
    logger.info("GCS BUCKET FILE DELETION")
    logger.info("=" * 60)
    
    # Confirm before deleting
    response = input(f"\nAre you sure you want to delete ALL files in gs://{config['GCS_BUCKET']}? (yes/no): ")
    
    if response.lower() == 'yes':
        delete_all_files(config)
    else:
        logger.info("Deletion cancelled.")


if __name__ == "__main__":
    main()