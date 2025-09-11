#!/usr/bin/env python3
"""
Quick script to check available columns in the optimized table
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

# Query to get column names
query = """
SELECT column_name
FROM `data-analytics-389803.conflixis_data_projects.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'op_general_all_aggregate_static_optimized'
AND (column_name LIKE 'covered_recipient%' OR column_name LIKE 'recipient%')
ORDER BY ordinal_position
"""

print("Checking available columns...")
query_job = client.query(query)
results = query_job.result()

print("\nAvailable recipient-related columns:")
for row in results:
    print(f"  - {row.column_name}")

# Also check for city/state columns
query2 = """
SELECT column_name
FROM `data-analytics-389803.conflixis_data_projects.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'op_general_all_aggregate_static_optimized'
AND (column_name LIKE '%city%' OR column_name LIKE '%state%')
ORDER BY ordinal_position
"""

print("\nChecking for city/state columns...")
query_job2 = client.query(query2)
results2 = query_job2.result()

print("\nAvailable city/state columns:")
for row in results2:
    print(f"  - {row.column_name}")