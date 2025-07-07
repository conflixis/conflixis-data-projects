#!/usr/bin/env python3
"""Check Snowflake storage integration details for GCS access troubleshooting."""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
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
        'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
        'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
        'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
        'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA'),
        'SNOWFLAKE_PAT': os.getenv('SNOWFLAKE_PAT'),
        'STORAGE_INTEGRATION_NAME': os.getenv('SNOWFLAKE_STORAGE_INTEGRATION', 'GCS_INT'),
        'GCS_BUCKET': os.getenv('SNOWFLAKE_GCS_BUCKET', 'snowflake_dh_bq'),
        'GOOGLE_APPLICATION_CREDENTIALS': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        'PROJECT_ID': os.getenv('BQ_PROJECT_ID', os.getenv('GOOGLE_CLOUD_PROJECT')),
    }


def check_storage_integration(config):
    """Check Snowflake storage integration details."""
    logger.info("=" * 60)
    logger.info("SNOWFLAKE STORAGE INTEGRATION CHECK")
    logger.info("=" * 60)
    
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=config['SNOWFLAKE_USER'],
            password=config['SNOWFLAKE_PAT'],
            account=config['SNOWFLAKE_ACCOUNT'],
            warehouse=config['SNOWFLAKE_WAREHOUSE'],
            database=config['SNOWFLAKE_DATABASE'],
            schema=config['SNOWFLAKE_SCHEMA'],
        )
        logger.info("✓ Connected to Snowflake")
        
        cursor = conn.cursor()
        
        # Check storage integration details
        integration_name = config['STORAGE_INTEGRATION_NAME']
        logger.info(f"\nChecking storage integration: {integration_name}")
        
        try:
            # Use SHOW INTEGRATIONS to get the actual values
            cursor.execute(f"SHOW INTEGRATIONS LIKE '{integration_name}'")
            integration_info = cursor.fetchone()
            
            if integration_info:
                logger.info(f"\nIntegration '{integration_name}' found")
                
                # Now get the detailed properties
                cursor.execute(f"DESC INTEGRATION {integration_name}")
                desc_results = cursor.fetchall()
                
                # The DESC command returns property names and types, not values
                # We need to use a different approach to get the service account
                logger.info("\nTo get the GCS service account, run this SQL in Snowflake:")
                logger.info(f"DESC INTEGRATION {integration_name};")
                logger.info("\nLook for the 'property_value' column in the row where 'property' = 'STORAGE_GCP_SERVICE_ACCOUNT'")
                
                # Try alternative method to get service account
                cursor.execute(f"""
                    SELECT SYSTEM$GET_GCS_SERVICE_ACCOUNT('{integration_name}')
                """)
                result = cursor.fetchone()
                if result and result[0]:
                    gcs_service_account = result[0]
                    logger.info(f"\nFound GCS Service Account: {gcs_service_account}")
                else:
                    logger.info("\nCould not retrieve service account automatically.")
                    logger.info("Please run the DESC INTEGRATION command manually in Snowflake.")
            
            if gcs_service_account:
                logger.info("\n" + "=" * 60)
                logger.info("IMPORTANT: Grant GCS bucket permissions to this service account:")
                logger.info(f"Service Account: {gcs_service_account}")
                logger.info("\nRequired permissions:")
                logger.info("  - Storage Object Admin (or at minimum:")
                logger.info("    - storage.objects.create")
                logger.info("    - storage.objects.delete")
                logger.info("    - storage.objects.get")
                logger.info("    - storage.objects.list")
                logger.info("\nGrant permissions using gcloud:")
                logger.info(f"  gsutil iam ch serviceAccount:{gcs_service_account}:objectAdmin gs://{config['GCS_BUCKET']}")
                logger.info("\nOr in GCP Console:")
                logger.info(f"  1. Go to Cloud Storage > {config['GCS_BUCKET']}")
                logger.info("  2. Click on 'Permissions' tab")
                logger.info("  3. Click 'Grant Access'")
                logger.info(f"  4. Add principal: {gcs_service_account}")
                logger.info("  5. Assign role: Storage Object Admin")
                logger.info("=" * 60)
                
        except Exception as e:
            logger.error(f"Error checking storage integration: {e}")
            logger.info("\nTrying to list all integrations...")
            cursor.execute("SHOW INTEGRATIONS")
            integrations = cursor.fetchall()
            logger.info("Available integrations:")
            for integ in integrations:
                logger.info(f"  - {integ[0]} (Type: {integ[1]})")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Snowflake connection error: {e}")


def check_gcs_bucket(config):
    """Check GCS bucket accessibility."""
    logger.info("\n" + "=" * 60)
    logger.info("GCS BUCKET CHECK")
    logger.info("=" * 60)
    
    if not config['GOOGLE_APPLICATION_CREDENTIALS']:
        logger.warning("No GOOGLE_APPLICATION_CREDENTIALS set")
        return
    
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
            bucket = storage_client.bucket(bucket_name)
            bucket.reload()  # This will fail if bucket doesn't exist or no access
            
            logger.info(f"✓ Bucket exists and is accessible")
            logger.info(f"  Location: {bucket.location}")
            logger.info(f"  Storage class: {bucket.storage_class}")
            
            # Try to list objects (to verify read access)
            blobs = list(bucket.list_blobs(max_results=5))
            logger.info(f"  Number of objects (sample): {len(blobs)}")
            
            # Try to check write access
            test_blob_name = "_snowflake_test_write_access.txt"
            try:
                blob = bucket.blob(test_blob_name)
                blob.upload_from_string("test")
                blob.delete()
                logger.info("✓ Write access confirmed (created and deleted test object)")
            except Exception as e:
                logger.warning(f"✗ No write access: {e}")
                
        except Exception as e:
            logger.error(f"✗ Cannot access bucket: {e}")
            logger.info("\nPossible issues:")
            logger.info("  1. Bucket doesn't exist")
            logger.info("  2. Service account doesn't have access")
            logger.info("  3. Bucket is in a different project")
            
            # List accessible buckets
            logger.info("\nAccessible buckets in project:")
            try:
                for bucket in storage_client.list_buckets():
                    logger.info(f"  - gs://{bucket.name}")
            except Exception:
                logger.info("  (Cannot list buckets)")
                
    except Exception as e:
        logger.error(f"GCS client error: {e}")


def main():
    """Main function."""
    config = load_config()
    
    # Check Snowflake storage integration
    check_storage_integration(config)
    
    # Check GCS bucket
    check_gcs_bucket(config)
    
    logger.info("\n" + "=" * 60)
    logger.info("NEXT STEPS:")
    logger.info("1. Grant the Snowflake service account access to the GCS bucket")
    logger.info("2. Ensure the bucket exists in the correct project")
    logger.info("3. Re-run the transfer script")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()