#!/usr/bin/env python3
"""Test script to identify correct column names"""

from google.cloud import bigquery
import json
import os
from pathlib import Path

# Setup credentials
env_file = Path("/home/incent/conflixis-data-projects/.env")
if env_file.exists():
    with open(env_file, 'r') as f:
        content = f.read()
        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                if key == 'GCP_SERVICE_ACCOUNT_KEY':
                    if value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                else:
                    value = value.strip().strip("'\"")
                os.environ[key] = value

from google.oauth2 import service_account
service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
service_account_json = service_account_json.replace('\\\\n', '\\n')
service_account_info = json.loads(service_account_json)
credentials = service_account.Credentials.from_service_account_info(service_account_info)
client = bigquery.Client(credentials=credentials, project="data-analytics-389803")

# Test query to get column names
query = """
SELECT column_name
FROM `data-analytics-389803.conflixis_data_projects.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized'
ORDER BY ordinal_position
"""

print("Columns in PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized:")
print("-" * 60)
result = client.query(query).to_dataframe()
for col in result['column_name']:
    print(f"  - {col}")