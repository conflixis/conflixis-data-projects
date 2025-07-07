#!/usr/bin/env python3
"""Debug GCS stage and storage integration issues."""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector

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
    }


def debug_stage_and_integration(config):
    """Debug GCS stage and integration setup."""
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
        
        # 1. Check the storage integration details
        logger.info("\n" + "="*60)
        logger.info("CHECKING STORAGE INTEGRATION")
        logger.info("="*60)
        
        integration_name = config['STORAGE_INTEGRATION_NAME']
        cursor.execute(f"SHOW INTEGRATIONS LIKE '{integration_name}'")
        result = cursor.fetchone()
        if result:
            logger.info(f"Integration '{integration_name}' exists")
            logger.info(f"  Type: {result[1]}")
            logger.info(f"  Enabled: {result[2]}")
        else:
            logger.error(f"Integration '{integration_name}' not found!")
        
        # 2. Check the stage
        logger.info("\n" + "="*60)
        logger.info("CHECKING GCS STAGE")
        logger.info("="*60)
        
        stage_db = config['SNOWFLAKE_STAGING_DATABASE']
        stage_schema = config['SNOWFLAKE_STAGING_SCHEMA']
        stage_name = config['GCS_STAGE_NAME']
        
        # List stages in the staging schema
        cursor.execute(f'USE DATABASE "{stage_db}"')
        cursor.execute(f'USE SCHEMA "{stage_schema}"')
        cursor.execute("SHOW STAGES")
        stages = cursor.fetchall()
        
        logger.info(f"Stages in {stage_db}.{stage_schema}:")
        stage_found = False
        for stage in stages:
            logger.info(f"  - {stage[1]} (URL: {stage[3]})")
            if stage[1] == stage_name:
                stage_found = True
                logger.info(f"    ✓ Stage '{stage_name}' found!")
        
        if not stage_found:
            logger.warning(f"Stage '{stage_name}' not found in {stage_db}.{stage_schema}")
        
        # 3. Try to list files in the stage
        logger.info("\n" + "="*60)
        logger.info("TESTING STAGE ACCESS")
        logger.info("="*60)
        
        stage_path = f'@"{stage_db}"."{stage_schema}"."{stage_name}"'
        try:
            cursor.execute(f"LIST {stage_path}")
            files = cursor.fetchall()
            if files:
                logger.info(f"Files in stage {stage_path}:")
                for file in files[:5]:  # Show first 5 files
                    logger.info(f"  - {file[0]}")
            else:
                logger.info("No files found in stage (this is normal if no data has been exported yet)")
        except Exception as e:
            logger.error(f"Error listing stage files: {e}")
        
        # 4. Test writing to the stage
        logger.info("\n" + "="*60)
        logger.info("TESTING STAGE WRITE ACCESS")
        logger.info("="*60)
        
        # Create a small test table
        test_table = "SNOWFLAKE_GCS_TEST_TABLE"
        try:
            cursor.execute(f'CREATE OR REPLACE TABLE "{stage_db}"."{stage_schema}"."{test_table}" AS SELECT 1 as test_col')
            logger.info(f"Created test table {test_table}")
            
            # Try to copy to stage
            test_stage_path = f'{stage_path}/test/'
            cursor.execute(f"""
                COPY INTO {test_stage_path}
                FROM "{stage_db}"."{stage_schema}"."{test_table}"
                FILE_FORMAT = (TYPE = PARQUET)
                OVERWRITE = TRUE
            """)
            logger.info("✓ Successfully wrote test file to GCS stage!")
            
            # Clean up
            cursor.execute(f'DROP TABLE IF EXISTS "{stage_db}"."{stage_schema}"."{test_table}"')
            
        except Exception as e:
            logger.error(f"Failed to write to stage: {e}")
            logger.info("\nPossible issues:")
            logger.info("1. The storage integration doesn't have the correct permissions")
            logger.info("2. The stage URL doesn't match the allowed locations in the integration")
            logger.info("3. The GCS bucket has restrictions (e.g., retention policy)")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")


def main():
    """Main function."""
    config = load_config()
    debug_stage_and_integration(config)


if __name__ == "__main__":
    main()