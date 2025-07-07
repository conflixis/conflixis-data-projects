#!/usr/bin/env python3
"""Check BigQuery table structure and data."""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
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
        'BQ_DATASET': os.getenv('SNOWFLAKE_BQ_TARGET_DATASET', 'CONFLIXIS_309340_STAGING'),
    }


def check_table(config):
    """Check BigQuery table structure and sample data."""
    try:
        # Initialize BigQuery client
        credentials = service_account.Credentials.from_service_account_file(
            config['GOOGLE_APPLICATION_CREDENTIALS']
        )
        client = bigquery.Client(
            credentials=credentials,
            project=config['PROJECT_ID']
        )
        
        table_id = f"{config['PROJECT_ID']}.{config['BQ_DATASET']}.PHYSICIAN_GROUPS_PRACTICE_LOCATIONS"
        logger.info(f"Checking table: {table_id}")
        
        # Get table schema
        table = client.get_table(table_id)
        logger.info(f"\nTable info:")
        logger.info(f"  Total rows: {table.num_rows:,}")
        logger.info(f"  Total size: {table.num_bytes:,} bytes")
        logger.info(f"  Created: {table.created}")
        
        # Show schema
        logger.info("\nTable schema:")
        for i, field in enumerate(table.schema):
            logger.info(f"  {i+1}. {field.name} ({field.field_type})")
        
        # Get first few rows
        logger.info("\nFirst 5 rows:")
        query = f"""
        SELECT * 
        FROM `{table_id}`
        LIMIT 5
        """
        
        results = client.query(query).result()
        for i, row in enumerate(results):
            logger.info(f"\nRow {i+1}:")
            for field in table.schema[:5]:  # Show first 5 fields
                value = row[field.name]
                logger.info(f"  {field.name}: {value}")
        
        # Check if first row looks like headers
        logger.info("\nChecking if first row contains header values...")
        query = f"""
        SELECT *
        FROM `{table_id}`
        LIMIT 1
        """
        
        result = list(client.query(query).result())[0]
        
        # Check if values look like column names
        header_like = 0
        for field in table.schema:
            value = str(result[field.name]).upper()
            field_name = field.name.upper()
            # Check if the value resembles the column name
            if value == field_name or value.replace("_", "") == field_name.replace("_", ""):
                header_like += 1
        
        if header_like > len(table.schema) * 0.5:  # More than 50% match
            logger.warning(f"\n⚠️  First row appears to contain column headers! ({header_like}/{len(table.schema)} fields match)")
            logger.info("You may need to skip the first row when querying this table.")
        else:
            logger.info(f"\n✓ First row appears to contain actual data (only {header_like}/{len(table.schema)} fields match column names)")
        
    except Exception as e:
        logger.error(f"Error: {e}")


def main():
    """Main function."""
    config = load_config()
    check_table(config)


if __name__ == "__main__":
    main()