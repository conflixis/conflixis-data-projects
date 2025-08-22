#!/usr/bin/env python3
"""
Fetch member data from member_shards_raw_latest_parsed and save to file
"""

import json
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery

# Load environment variables
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw" / "disclosures"

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)

def fetch_member_data():
    """Fetch all member data for the group_id"""
    
    print("=" * 50)
    print("FETCHING MEMBER DATA FROM BIGQUERY")
    print("=" * 50)
    
    try:
        # Load credentials from environment variable
        service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        
        # Initialize BigQuery client
        project_id = 'conflixis-engine'
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        # Query for all member data
        group_id = 'gcO9AHYlNSzFeGTRSFRa'
        
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
        FROM `conflixis-engine.firestore_export.member_shards_raw_latest_parsed`
        WHERE group_id = '{group_id}'
        ORDER BY last_name, first_name
        """
        
        print(f"Fetching member data for group_id: {group_id}")
        
        # Execute query
        query_job = client.query(query)
        df = query_job.to_dataframe()
        
        print(f"Retrieved {len(df)} member records")
        
        # Save to multiple formats
        timestamp = datetime.now().strftime('%Y-%m-%d')
        
        # Save as CSV
        csv_path = DATA_DIR / f"member_shards_{timestamp}.csv"
        df.to_csv(csv_path, index=False)
        print(f"✓ CSV saved to: {csv_path}")
        
        # Save as Parquet
        parquet_path = DATA_DIR / f"member_shards_{timestamp}.parquet"
        df.to_parquet(parquet_path, index=False)
        print(f"✓ Parquet saved to: {parquet_path}")
        
        # Save as JSON
        json_path = DATA_DIR / f"member_shards_{timestamp}.json"
        df.to_json(json_path, orient='records', indent=2)
        print(f"✓ JSON saved to: {json_path}")
        
        # Print summary statistics
        print("\n=== Member Data Statistics ===")
        print(f"Total Members: {len(df)}")
        print(f"Members with NPI: {df['npi'].notna().sum()}")
        print(f"Unique Entities: {df['entity'].nunique()}")
        print(f"Unique Job Titles: {df['job_title'].nunique()}")
        
        # Show sample data
        print("\n=== Sample Records ===")
        sample = df.head(5)
        for _, row in sample.iterrows():
            print(f"  - {row['full_name']}: {row['job_title']} at {row['entity']}")
        
        print("\n✅ Member data successfully fetched and saved!")
        
        return df
        
    except Exception as e:
        print(f"\n❌ Error fetching member data: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    fetch_member_data()