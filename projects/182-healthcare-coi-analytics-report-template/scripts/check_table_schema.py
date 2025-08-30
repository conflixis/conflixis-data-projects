#!/usr/bin/env python3
"""
Check schema of PHYSICIANS_OVERVIEW table
"""

import json
import os
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Setup paths
TEMPLATE_DIR = Path(__file__).parent.parent

# Load environment variables
load_dotenv(TEMPLATE_DIR / '.env')

def create_bigquery_client():
    """Create BigQuery client with credentials"""
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
    
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=['https://www.googleapis.com/auth/bigquery']
    )
    
    return bigquery.Client(project='data-analytics-389803', credentials=credentials)

def check_schema():
    """Check table schema and sample data"""
    
    client = create_bigquery_client()
    
    # Query to get column names and sample data
    query = """
    SELECT *
    FROM `data-analytics-389803.CONFLIXIS_309340.PHYSICIANS_OVERVIEW`
    LIMIT 5
    """
    
    print("Getting table schema and sample data...")
    df = client.query(query).to_dataframe()
    
    print("\nColumns in PHYSICIANS_OVERVIEW:")
    for col in df.columns:
        print(f"  - {col}")
    
    print("\nSample data:")
    print(df.head())

if __name__ == "__main__":
    check_schema()