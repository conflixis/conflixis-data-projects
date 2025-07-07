#!/usr/bin/env python3
"""Test exporting the specific table that's failing."""

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
        'GCS_STAGE_NAME': os.getenv('SNOWFLAKE_GCS_STAGE_NAME', 'snowflake_dh_bq'),
    }


def test_export(config):
    """Test exporting the problematic table."""
    try:
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
        
        table_name = "PHYSICIAN_GROUPS_PRACTICE_LOCATIONS"
        staging_db = config['SNOWFLAKE_STAGING_DATABASE']
        staging_schema = config['SNOWFLAKE_STAGING_SCHEMA']
        stage_name = config['GCS_STAGE_NAME']
        
        # 1. Check if the table exists in staging
        logger.info(f"\nChecking if {table_name} exists in staging...")
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM "{staging_db}".INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{staging_schema}' 
            AND TABLE_NAME = '{table_name}'
        """)
        exists = cursor.fetchone()[0] > 0
        
        if exists:
            logger.info(f"✓ Table exists in {staging_db}.{staging_schema}")
            
            # Get row count
            cursor.execute(f'SELECT COUNT(*) FROM "{staging_db}"."{staging_schema}"."{table_name}"')
            count = cursor.fetchone()[0]
            logger.info(f"  Row count: {count:,}")
        else:
            logger.error(f"✗ Table does not exist in staging!")
            return
        
        # 2. Try different export approaches
        logger.info("\n" + "="*60)
        logger.info("TESTING DIFFERENT EXPORT APPROACHES")
        logger.info("="*60)
        
        # Approach 1: Export with explicit file format
        logger.info("\nApproach 1: Export with explicit file format...")
        try:
            stage_path = f'@"{staging_db}"."{staging_schema}"."{stage_name}"/{table_name}_test1/'
            cursor.execute(f"""
                COPY INTO {stage_path}
                FROM "{staging_db}"."{staging_schema}"."{table_name}"
                FILE_FORMAT = (TYPE = PARQUET, COMPRESSION = SNAPPY)
                MAX_FILE_SIZE = 268435456
                OVERWRITE = TRUE
            """)
            logger.info("✓ Export with explicit file format succeeded!")
        except Exception as e:
            logger.error(f"✗ Failed: {e}")
        
        # Approach 2: Export a small sample
        logger.info("\nApproach 2: Export small sample (100 rows)...")
        try:
            # Create a sample table
            sample_table = f"{table_name}_SAMPLE"
            cursor.execute(f"""
                CREATE OR REPLACE TABLE "{staging_db}"."{staging_schema}"."{sample_table}" AS
                SELECT * FROM "{staging_db}"."{staging_schema}"."{table_name}" LIMIT 100
            """)
            
            stage_path = f'@"{staging_db}"."{staging_schema}"."{stage_name}"/{sample_table}/'
            cursor.execute(f"""
                COPY INTO {stage_path}
                FROM "{staging_db}"."{staging_schema}"."{sample_table}"
                FILE_FORMAT = (TYPE = PARQUET)
                OVERWRITE = TRUE
            """)
            logger.info("✓ Sample export succeeded!")
            
            # Clean up
            cursor.execute(f'DROP TABLE IF EXISTS "{staging_db}"."{staging_schema}"."{sample_table}"')
            
        except Exception as e:
            logger.error(f"✗ Failed: {e}")
        
        # 3. Check stage properties
        logger.info("\n" + "="*60)
        logger.info("CHECKING STAGE PROPERTIES")
        logger.info("="*60)
        
        cursor.execute(f'DESC STAGE "{staging_db}"."{staging_schema}"."{stage_name}"')
        stage_props = cursor.fetchall()
        for prop in stage_props:
            logger.info(f"  {prop[0]}: {prop[1]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")


def main():
    """Main function."""
    config = load_config()
    test_export(config)


if __name__ == "__main__":
    main()