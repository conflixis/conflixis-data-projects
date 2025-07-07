#!/usr/bin/env python3
"""Check available tables in the source Snowflake database."""

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


def check_source_tables(config):
    """Check available tables in source database."""
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
        logger.info(f"Database: {config['SNOWFLAKE_DATABASE']}")
        logger.info(f"Schema: {config['SNOWFLAKE_SCHEMA']}")
        
        cursor = conn.cursor()
        
        # Check current context
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        db, schema = cursor.fetchone()
        logger.info(f"\nCurrent context: {db}.{schema}")
        
        # List all tables in the schema
        logger.info("\n" + "="*60)
        logger.info("TABLES IN SOURCE SCHEMA")
        logger.info("="*60)
        
        cursor.execute(f"""
            SHOW TABLES IN SCHEMA "{config['SNOWFLAKE_DATABASE']}"."{config['SNOWFLAKE_SCHEMA']}"
        """)
        
        tables = cursor.fetchall()
        logger.info(f"\nFound {len(tables)} tables:")
        
        # Look for tables with "PHYSICIAN" in the name
        physician_tables = []
        for table in tables:
            table_name = table[1]
            if "PHYSICIAN" in table_name.upper():
                physician_tables.append(table_name)
                logger.info(f"  ✓ {table_name}")
            else:
                logger.info(f"  - {table_name}")
        
        if physician_tables:
            logger.info(f"\nFound {len(physician_tables)} tables with 'PHYSICIAN' in the name:")
            for table in physician_tables:
                logger.info(f"  - {table}")
                
        # Check specifically for the table we're trying to transfer
        target_table = "PHYSICIAN_GROUPS_PRACTICE_LOCATIONS"
        if target_table in [t[1] for t in tables]:
            logger.info(f"\n✓ Target table '{target_table}' exists!")
            
            # Get row count
            cursor.execute(f"""
                SELECT COUNT(*) FROM "{config['SNOWFLAKE_DATABASE']}"."{config['SNOWFLAKE_SCHEMA']}"."{target_table}"
            """)
            count = cursor.fetchone()[0]
            logger.info(f"  Row count: {count:,}")
        else:
            logger.warning(f"\n✗ Target table '{target_table}' NOT FOUND!")
            logger.info("\nDid you mean one of these?")
            for table in physician_tables:
                logger.info(f"  - {table}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {e}")


def main():
    """Main function."""
    config = load_config()
    check_source_tables(config)


if __name__ == "__main__":
    main()