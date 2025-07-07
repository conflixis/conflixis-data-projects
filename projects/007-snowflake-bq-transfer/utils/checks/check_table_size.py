#!/usr/bin/env python3
"""Check Snowflake table size."""

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
    }


def check_table_size(config, table_name):
    """Check table size and row count."""
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
        
        # Get row count
        cursor.execute(f"""
            SELECT COUNT(*) as row_count
            FROM "{config['SNOWFLAKE_DATABASE']}"."{config['SNOWFLAKE_SCHEMA']}"."{table_name}"
        """)
        row_count = cursor.fetchone()[0]
        
        # Get table size (approximate)
        cursor.execute(f"""
            SELECT 
                TABLE_NAME,
                ROW_COUNT,
                BYTES,
                ROUND(BYTES/1024/1024/1024, 2) as SIZE_GB
            FROM "{config['SNOWFLAKE_DATABASE']}".INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{config['SNOWFLAKE_SCHEMA']}'
            AND TABLE_NAME = '{table_name}'
        """)
        
        result = cursor.fetchone()
        if result:
            logger.info(f"\nTable: {table_name}")
            logger.info(f"  Row count: {row_count:,}")
            logger.info(f"  Estimated rows: {result[1]:,}")
            logger.info(f"  Size: {result[3]} GB ({result[2]:,} bytes)")
        
        # Get column count
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM "{config['SNOWFLAKE_DATABASE']}".INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{config['SNOWFLAKE_SCHEMA']}'
            AND TABLE_NAME = '{table_name}'
        """)
        col_count = cursor.fetchone()[0]
        logger.info(f"  Columns: {col_count}")
        
        # Estimate transfer time
        size_gb = result[3] if result else 0
        estimated_minutes = max(5, size_gb * 2)  # Rough estimate: 2 minutes per GB minimum 5 minutes
        logger.info(f"\nEstimated transfer time: {estimated_minutes:.0f} minutes")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")


def main():
    """Main function."""
    config = load_config()
    check_table_size(config, "PHYSICIAN_RX_FULL_YEAR")


if __name__ == "__main__":
    main()