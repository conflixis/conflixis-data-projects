#!/usr/bin/env python3
"""
Enrich NPI data with real information from BigQuery PHYSICIANS_OVERVIEW table
"""

import pandas as pd
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

def enrich_npis():
    """Enrich NPIs with real data from BigQuery"""
    
    # Read current NPI file
    npi_file = TEMPLATE_DIR / 'data' / 'inputs' / 'provider_npis.csv'
    df_npis = pd.read_csv(npi_file)
    
    print(f"Found {len(df_npis)} NPIs to enrich")
    
    # Create BigQuery client
    client = create_bigquery_client()
    
    # Get NPI list for query
    npi_list = df_npis['NPI'].astype(str).tolist()
    npi_string = "','".join(npi_list)
    
    # Query to get real provider information
    query = f"""
    SELECT 
        CAST(NPI AS STRING) AS NPI,
        FIRST_NAME,
        LAST_NAME,
        CONCAT(FIRST_NAME, ' ', LAST_NAME) AS Full_Name_BQ,
        SPECIALTY_PRIMARY AS Primary_Specialty,
        CREDENTIAL,
        HQ_STATE
    FROM `data-analytics-389803.CONFLIXIS_309340.PHYSICIANS_OVERVIEW`
    WHERE CAST(NPI AS STRING) IN ('{npi_string}')
    """
    
    print("Querying BigQuery for provider information...")
    df_bq = client.query(query).to_dataframe()
    
    print(f"Found {len(df_bq)} providers in BigQuery")
    
    # Merge the data
    df_npis['NPI'] = df_npis['NPI'].astype(str)
    df_enriched = df_npis.merge(df_bq, on='NPI', how='left')
    
    # Use BigQuery specialties where available
    if 'Primary_Specialty' in df_enriched.columns:
        # Keep original Full_Name, add specialty from BigQuery
        df_final = df_enriched[['Full_Name', 'NPI', 'Primary_Specialty']].copy()
    else:
        df_final = df_enriched[['Full_Name', 'NPI']].copy()
        df_final['Primary_Specialty'] = None
    
    # Fill any missing specialties with "Unknown"
    df_final['Primary_Specialty'] = df_final['Primary_Specialty'].fillna('Unknown')
    
    # Save enriched file
    df_final.to_csv(npi_file, index=False)
    
    print(f"\nEnriched NPI file saved with {len(df_final)} providers")
    print("\nSample of enriched data:")
    print(df_final.head(10))
    
    # Statistics
    specialty_counts = df_final['Primary_Specialty'].value_counts()
    print(f"\nTop specialties:")
    print(specialty_counts.head(10))
    
    return df_final

if __name__ == "__main__":
    try:
        enrich_npis()
    except Exception as e:
        print(f"Error enriching NPIs: {e}")
        print("The NPI file will work without specialties for testing purposes")