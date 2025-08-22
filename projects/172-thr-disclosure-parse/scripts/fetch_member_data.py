#!/usr/bin/env python3
"""
Fetch member data from member_shards_raw_latest_parsed
DA-172: THR Disclosure Parse - Member Data Component
"""

import json
import os
import sys
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from google.oauth2 import service_account
from google.cloud import bigquery
from dotenv import load_dotenv
import config

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def fetch_member_data():
    """
    Fetch all member data for the configured group_id.
    Returns DataFrame with member information including NPI, job titles, etc.
    """
    
    logger.info("=" * 50)
    logger.info("FETCHING MEMBER DATA FROM BIGQUERY")
    logger.info("=" * 50)
    
    try:
        # Load credentials from environment variable
        service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        
        # Initialize BigQuery client
        client = bigquery.Client(credentials=credentials, project=config.PROJECT_ID)
        
        # Query for all member data
        query = f"""
        SELECT 
            group_id,
            member_id,
            npi,
            first_name,
            last_name,
            CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, '')) as full_name,
            LOWER(TRIM(CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, '')))) as normalized_name,
            email,
            entity,
            job_title,
            manager_name,
            archived,
            shard_id,
            last_updated
        FROM `{config.PROJECT_ID}.{config.MEMBERS_TABLE}`
        WHERE group_id = '{config.GROUP_ID}'
        ORDER BY last_name, first_name
        """
        
        logger.info(f"Fetching member data for group_id: {config.GROUP_ID}")
        logger.info(f"Table: {config.PROJECT_ID}.{config.MEMBERS_TABLE}")
        
        # Execute query
        query_job = client.query(query)
        df = query_job.to_dataframe()
        
        logger.info(f"Retrieved {len(df)} member records")
        
        # Save to multiple formats
        timestamp = datetime.now().strftime('%Y-%m-%d')
        
        # Save as CSV
        csv_path = config.RAW_DIR / f"member_shards_{timestamp}.csv"
        df.to_csv(csv_path, index=False)
        logger.info(f"✓ CSV saved to: {csv_path}")
        
        # Save as Parquet for better performance
        parquet_path = config.RAW_DIR / f"member_shards_{timestamp}.parquet"
        df.to_parquet(parquet_path, index=False, engine='pyarrow', compression='snappy')
        logger.info(f"✓ Parquet saved to: {parquet_path}")
        
        # Print summary statistics
        logger.info("\n=== Member Data Statistics ===")
        logger.info(f"Total Members: {len(df)}")
        logger.info(f"Members with NPI: {df['npi'].notna().sum()}")
        logger.info(f"Unique Entities: {df['entity'].nunique()}")
        logger.info(f"Unique Job Titles: {df['job_title'].nunique()}")
        logger.info(f"Archived Members: {df['archived'].sum() if 'archived' in df.columns else 0}")
        
        # Show entity distribution
        entity_counts = df['entity'].value_counts().head(10)
        logger.info("\nTop 10 Entities:")
        for entity, count in entity_counts.items():
            logger.info(f"  - {entity}: {count} members")
        
        # Show sample data
        logger.info("\n=== Sample Records ===")
        sample = df.head(5)
        for _, row in sample.iterrows():
            logger.info(f"  - {row['full_name']}: {row['job_title']} at {row['entity']}")
            if pd.notna(row.get('npi')):
                logger.info(f"    NPI: {row['npi']}")
        
        logger.info("\n✅ Member data successfully fetched and saved!")
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching member data: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    """Main execution function"""
    df = fetch_member_data()
    
    if df is not None:
        print(f"\n✅ Successfully fetched {len(df)} member records")
        print(f"Data saved to: {config.RAW_DIR}")
    else:
        print("\n⚠️ Failed to fetch member data. Check error messages above.")
    
    return df


if __name__ == "__main__":
    main()