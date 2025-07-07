#!/usr/bin/env python3
"""Get Snowflake table schema."""

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


def get_schema(config):
    """Get table schema from Snowflake."""
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
        
        # Get column information
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
            FROM "{config['SNOWFLAKE_DATABASE']}".INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{config['SNOWFLAKE_SCHEMA']}'
            AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        
        logger.info(f"\nSchema for {table_name}:")
        logger.info("="*60)
        for col in columns:
            logger.info(f"{col[2]:3d}. {col[0]:40s} {col[1]}")
        
        # Get first row to see sample data
        cursor.execute(f"""
            SELECT * 
            FROM "{config['SNOWFLAKE_DATABASE']}"."{config['SNOWFLAKE_SCHEMA']}"."{table_name}"
            LIMIT 1
        """)
        
        sample = cursor.fetchone()
        logger.info("\nSample data (first row):")
        logger.info("="*60)
        for i, (col, val) in enumerate(zip(columns, sample)):
            logger.info(f"{col[0]:40s} = {str(val)[:50]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")


def main():
    """Main function."""
    config = load_config()
    get_schema(config)


if __name__ == "__main__":
    main()