#!/usr/bin/env python3
"""Check if table exists in staging database."""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
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
}

conn = snowflake.connector.connect(
    user=config['SNOWFLAKE_USER'],
    password=config['SNOWFLAKE_PAT'],
    account=config['SNOWFLAKE_ACCOUNT'],
    warehouse=config['SNOWFLAKE_WAREHOUSE'],
    database=config['SNOWFLAKE_DATABASE'],
    schema=config['SNOWFLAKE_SCHEMA'],
)

cursor = conn.cursor()

table_name = "PHYSICIAN_RX_FULL_YEAR"

# Check if table exists in staging
try:
    cursor.execute(f"""
        SELECT COUNT(*) as row_count
        FROM "{config['SNOWFLAKE_STAGING_DATABASE']}"."{config['SNOWFLAKE_STAGING_SCHEMA']}"."{table_name}"
    """)
    staging_count = cursor.fetchone()[0]
    logger.info(f"Table {table_name} exists in staging with {staging_count:,} rows")
    
    # Check source count
    cursor.execute(f"""
        SELECT COUNT(*) as row_count
        FROM "{config['SNOWFLAKE_DATABASE']}"."{config['SNOWFLAKE_SCHEMA']}"."{table_name}"
    """)
    source_count = cursor.fetchone()[0]
    logger.info(f"Source table has {source_count:,} rows")
    
    if staging_count == source_count:
        logger.info("✓ Staging table is up to date!")
    else:
        logger.info("⚠ Row counts differ - staging table needs to be updated")
        
except Exception as e:
    logger.info(f"Table {table_name} does not exist in staging database")

conn.close()