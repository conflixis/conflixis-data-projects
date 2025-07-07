#!/usr/bin/env python3
"""Transfer table from existing staging to BigQuery (skip Snowflake copy)."""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
from google.cloud import bigquery
from google.oauth2 import service_account

# Import functions from the main script
exec(open('dh_snowflake_bigquery_singlefile.py').read())

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load config
current_path = Path(__file__).resolve()
for parent in current_path.parents:
    if (parent / "pyproject.toml").exists():
        env_path = parent / "common" / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        break

config = {
    'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
    'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
    'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
    'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA'),
    'SNOWFLAKE_PAT': os.getenv('SNOWFLAKE_PAT'),
    'SNOWFLAKE_STAGING_DATABASE': os.getenv('SNOWFLAKE_STAGING_DATABASE', 'CONFLIXIS_STAGE'),
    'SNOWFLAKE_STAGING_SCHEMA': os.getenv('SNOWFLAKE_STAGING_SCHEMA', 'PUBLIC'),
    'GCS_BUCKET': os.getenv('SNOWFLAKE_GCS_BUCKET', 'snowflake_dh_bq'),
    'GCS_STAGE_NAME': os.getenv('SNOWFLAKE_GCS_STAGE_NAME', 'snowflake_dh_bq'),
    'STORAGE_INTEGRATION_NAME': os.getenv('SNOWFLAKE_STORAGE_INTEGRATION', 'GCS_INT'),
    'PROJECT_ID': os.getenv('BQ_PROJECT_ID', os.getenv('GOOGLE_CLOUD_PROJECT')),
    'BQ_TARGET_DATASET': os.getenv('SNOWFLAKE_BQ_TARGET_DATASET', 'CONFLIXIS_309340_STAGING'),
    'GOOGLE_APPLICATION_CREDENTIALS': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
}

# Table to transfer
table_name = "PHYSICIAN_RX_FULL_YEAR"

logger.info(f"Starting transfer of {table_name} from existing staging to BigQuery")

# Connect to Snowflake
sf_conn = snowflake.connector.connect(
    user=config['SNOWFLAKE_USER'],
    password=config['SNOWFLAKE_PAT'],
    account=config['SNOWFLAKE_ACCOUNT'],
    warehouse=config['SNOWFLAKE_WAREHOUSE'],
    database=config['SNOWFLAKE_DATABASE'],
    schema=config['SNOWFLAKE_SCHEMA'],
)
logger.info("Connected to Snowflake")

# Initialize BigQuery client
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config['GOOGLE_APPLICATION_CREDENTIALS']
bq_gcp_creds = service_account.Credentials.from_service_account_file(config['GOOGLE_APPLICATION_CREDENTIALS'])
bq_client = bigquery.Client(credentials=bq_gcp_creds, project=config['PROJECT_ID'])
logger.info("Initialized BigQuery client")

try:
    # Step 1: Export from staging to GCS
    logger.info(f"Exporting {table_name} from staging to GCS...")
    copy_sf_staging_db_to_gcs(
        table_name, 
        sf_conn,
        config['SNOWFLAKE_STAGING_DATABASE'],
        config['SNOWFLAKE_STAGING_SCHEMA'],
        config['GCS_STAGE_NAME']
    )
    
    # Step 2: Get schema from source table
    logger.info(f"Getting schema for table '{table_name}'...")
    sf_schema = get_snowflake_table_schema(
        sf_conn, 
        config['SNOWFLAKE_DATABASE'], 
        config['SNOWFLAKE_SCHEMA'], 
        table_name
    )
    
    # Step 3: Load into BigQuery
    logger.info(f"Loading {table_name} into BigQuery...")
    load_gcs_to_bigquery(
        table_name, 
        bq_client, 
        config['PROJECT_ID'], 
        config['GCS_BUCKET'],
        bq_dataset_name=config['BQ_TARGET_DATASET'], 
        sf_schema=sf_schema
    )
    
    logger.info(f"Successfully transferred {table_name} to BigQuery!")
    
except Exception as e:
    logger.error(f"Error during transfer: {e}")
    raise
finally:
    sf_conn.close()
    logger.info("Closed Snowflake connection")