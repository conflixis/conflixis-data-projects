#!/usr/bin/env python3
"""Check GCS access and list available buckets."""

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
        'GOOGLE_APPLICATION_CREDENTIALS': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        'PROJECT_ID': os.getenv('BQ_PROJECT_ID', os.getenv('GOOGLE_CLOUD_PROJECT')),
    }


def check_gcs_access(config):
    """Check GCS access and list buckets."""
    if not config['GOOGLE_APPLICATION_CREDENTIALS']:
        logger.error("No GOOGLE_APPLICATION_CREDENTIALS set")
        return
    
    try:
        # Load credentials
        credentials = service_account.Credentials.from_service_account_file(
            config['GOOGLE_APPLICATION_CREDENTIALS']
        )
        
        # Get service account email
        logger.info(f"Service Account: {credentials.service_account_email}")
        logger.info(f"Project ID: {config['PROJECT_ID']}")
        
        storage_client = storage.Client(
            credentials=credentials,
            project=config['PROJECT_ID']
        )
        
        # Try to list buckets in the project
        logger.info("\nTrying to list buckets in the project...")
        try:
            buckets = list(storage_client.list_buckets())
            if buckets:
                logger.info(f"\nAccessible buckets in project {config['PROJECT_ID']}:")
                for bucket in buckets:
                    logger.info(f"  - gs://{bucket.name} (location: {bucket.location})")
            else:
                logger.info("No buckets found in the project.")
        except Exception as e:
            logger.error(f"Cannot list buckets: {e}")
        
        # Suggest creating a new bucket with a unique name
        suggested_bucket = f"conflixis-snowflake-transfer-{config['PROJECT_ID'][-6:]}"
        logger.info(f"\nSuggested bucket name: gs://{suggested_bucket}")
        logger.info("\nTo create a new bucket, you can:")
        logger.info(f"1. Use gsutil: gsutil mb -p {config['PROJECT_ID']} gs://{suggested_bucket}")
        logger.info(f"2. Or update SNOWFLAKE_GCS_BUCKET in .env to use an existing bucket")
        
    except Exception as e:
        logger.error(f"Error: {e}")


def main():
    """Main function."""
    config = load_config()
    
    logger.info("=" * 60)
    logger.info("GCS ACCESS CHECK")
    logger.info("=" * 60)
    
    check_gcs_access(config)


if __name__ == "__main__":
    main()