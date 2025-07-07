#!/usr/bin/env python3
"""Setup GCS bucket for Snowflake to BigQuery transfer."""

import os
import sys
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
    # Find .env file
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
            logger.info(f"Loaded configuration from {env_path}")
    
    return {
        'GCS_BUCKET': os.getenv('SNOWFLAKE_GCS_BUCKET', 'snowflake_dh_bq'),
        'GOOGLE_APPLICATION_CREDENTIALS': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        'PROJECT_ID': os.getenv('BQ_PROJECT_ID', os.getenv('GOOGLE_CLOUD_PROJECT')),
    }


def create_bucket_if_needed(config):
    """Create GCS bucket if it doesn't exist."""
    if not config['GOOGLE_APPLICATION_CREDENTIALS']:
        logger.error("No GOOGLE_APPLICATION_CREDENTIALS set")
        return False
    
    try:
        # Initialize GCS client
        credentials = service_account.Credentials.from_service_account_file(
            config['GOOGLE_APPLICATION_CREDENTIALS']
        )
        storage_client = storage.Client(
            credentials=credentials,
            project=config['PROJECT_ID']
        )
        
        bucket_name = config['GCS_BUCKET']
        logger.info(f"Checking bucket: gs://{bucket_name}")
        
        try:
            # Try to get the bucket
            bucket = storage_client.bucket(bucket_name)
            bucket.reload()
            logger.info(f"✓ Bucket already exists in project {config['PROJECT_ID']}")
            logger.info(f"  Location: {bucket.location}")
            logger.info(f"  Storage class: {bucket.storage_class}")
            return True
            
        except Exception as e:
            if "404" in str(e):
                # Bucket doesn't exist, create it
                logger.info(f"Bucket doesn't exist. Creating bucket: gs://{bucket_name}")
                
                try:
                    bucket = storage_client.bucket(bucket_name)
                    bucket.location = "US"  # or your preferred location
                    bucket = storage_client.create_bucket(bucket)
                    logger.info(f"✓ Created bucket: gs://{bucket_name}")
                    logger.info(f"  Location: {bucket.location}")
                    logger.info(f"  Storage class: {bucket.storage_class}")
                    return True
                    
                except Exception as create_error:
                    logger.error(f"Failed to create bucket: {create_error}")
                    return False
            else:
                logger.error(f"Error accessing bucket: {e}")
                return False
                
    except Exception as e:
        logger.error(f"GCS client error: {e}")
        return False


def main():
    """Main function."""
    config = load_config()
    
    logger.info("=" * 60)
    logger.info("GCS BUCKET SETUP")
    logger.info("=" * 60)
    
    if create_bucket_if_needed(config):
        logger.info("\n" + "=" * 60)
        logger.info("NEXT STEPS:")
        logger.info("1. Run check_storage_integration.py to get the Snowflake service account")
        logger.info("2. Grant the service account permissions on the bucket")
        logger.info("3. Re-run the transfer script")
        logger.info("=" * 60)
    else:
        logger.error("\nFailed to setup bucket. Please check the errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()