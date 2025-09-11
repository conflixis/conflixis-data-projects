#!/usr/bin/env python3
"""
Explore the facility affiliations data to understand structure
"""

import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get service account JSON from environment
service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
service_account_info = json.loads(service_account_json)

# Create credentials
credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=['https://www.googleapis.com/auth/bigquery']
)

# Initialize client
client = bigquery.Client(
    credentials=credentials,
    project=service_account_info.get('project_id')
)

# Query to sample the facility affiliations data
query = """
SELECT *
FROM `data-analytics-389803.conflixis_data_projects.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized`
WHERE PRIMARY_AFFILIATED_FACILITY_FLAG = 'Yes'
LIMIT 100
"""

print("Sampling facility affiliations data...")
query_job = client.query(query)
df = query_job.to_dataframe()

print(f"\nData shape: {df.shape}")
print(f"\nColumns: {list(df.columns)}")
print(f"\nFirst 5 rows:")
print(df.head())

# Check data types and nulls
print("\nData types and null counts:")
print(df.info())

# Check unique values for key columns
print("\nUnique values in key columns:")
print(f"PRIMARY_AFFILIATED_FACILITY_FLAG: {df['PRIMARY_AFFILIATED_FACILITY_FLAG'].unique()}")
print(f"AFFILIATED_FIRM_TYPE unique count: {df['AFFILIATED_FIRM_TYPE'].nunique()}")
print(f"AFFILIATED_HQ_STATE unique count: {df['AFFILIATED_HQ_STATE'].nunique()}")

# Sample some firm types
print("\nSample AFFILIATED_FIRM_TYPE values:")
print(df['AFFILIATED_FIRM_TYPE'].value_counts().head(10))

# Sample some states
print("\nSample AFFILIATED_HQ_STATE values:")
print(df['AFFILIATED_HQ_STATE'].value_counts().head(10))