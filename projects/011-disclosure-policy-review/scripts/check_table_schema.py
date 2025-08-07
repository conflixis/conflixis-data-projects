#!/usr/bin/env python3
"""
Check the actual schema of disclosure tables
"""

import os
import json
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery

# Load environment variables
load_dotenv()

def get_bigquery_client():
    """Initialize BigQuery client"""
    service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return bigquery.Client(credentials=credentials, project='data-analytics-389803')

def check_parsed_table_schema():
    """Check columns in parsed disclosures table"""
    client = get_bigquery_client()
    
    print("\n" + "="*60)
    print("CHECKING PARSED DISCLOSURES TABLE SCHEMA")
    print("="*60)
    
    # Get first row to see actual columns
    query = """
    SELECT *
    FROM `conflixis-engine.firestore_export.disclosures_raw_latest_parse`
    WHERE group_id = 'gcO9AHYlNSzFeGTRSFRa'
    LIMIT 1
    """
    
    results = client.query(query).to_dataframe()
    
    print(f"\nColumns in parsed disclosures table ({len(results.columns)} total):")
    print("-" * 40)
    for i, col in enumerate(results.columns):
        print(f"{i+1:3}. {col}")
    
    # Show data types
    print("\nData types:")
    print(results.dtypes)
    
    # Show sample values for key columns
    print("\nSample values:")
    for col in results.columns[:20]:  # Show first 20 columns
        value = results[col].iloc[0] if len(results) > 0 else None
        if value is not None and str(value) != 'nan':
            print(f"{col}: {str(value)[:100]}")
    
    return results.columns.tolist()

def check_forms_table_schema():
    """Check columns in disclosure forms table"""
    client = get_bigquery_client()
    
    print("\n" + "="*60)
    print("CHECKING DISCLOSURE FORMS TABLE SCHEMA")
    print("="*60)
    
    # Get first row to see actual columns
    query = """
    SELECT *
    FROM `conflixis-engine.firestore_export.disclosure_forms_raw_latest_v3`
    WHERE group_id = 'gcO9AHYlNSzFeGTRSFRa'
    LIMIT 1
    """
    
    results = client.query(query).to_dataframe()
    
    print(f"\nColumns in forms table ({len(results.columns)} total):")
    print("-" * 40)
    for i, col in enumerate(results.columns):
        print(f"{i+1:3}. {col}")
    
    # Show sample values
    print("\nSample values:")
    for col in results.columns:
        value = results[col].iloc[0] if len(results) > 0 else None
        if value is not None and str(value) != 'nan':
            value_str = str(value)[:100]
            if len(str(value)) > 100:
                value_str += "..."
            print(f"{col}: {value_str}")
    
    return results.columns.tolist()

def main():
    """Main execution"""
    print("="*60)
    print("DISCLOSURE TABLE SCHEMA CHECK")
    print("="*60)
    
    # Check both tables
    parsed_cols = check_parsed_table_schema()
    forms_cols = check_forms_table_schema()
    
    print("\n" + "="*60)
    print("SCHEMA CHECK COMPLETE")
    print("="*60)

if __name__ == "__main__":
    main()