#!/usr/bin/env python3
"""Test script to understand aggregation discrepancy"""

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

print("=" * 80)
print("TESTING AGGREGATION DISCREPANCY")
print("=" * 80)

# Test 1: Direct BCBSMI provider payments (should match main report)
query1 = """
WITH bcbsmi_npis AS (
    SELECT DISTINCT CAST(NPI AS INT64) as NPI 
    FROM `data-analytics-389803.temp.bcbsmi_provider_npis`
)
SELECT 
    COUNT(DISTINCT op.covered_recipient_npi) as unique_providers_with_payments,
    SUM(op.total_amount_of_payment_usdollars) as total_payments,
    COUNT(DISTINCT op.record_id) as total_transactions
FROM bcbsmi_npis b
INNER JOIN `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized` op
    ON b.NPI = op.covered_recipient_npi
WHERE op.program_year BETWEEN 2020 AND 2024
"""

print("\n1. Direct BCBSMI provider payments (2020-2024):")
result1 = client.query(query1).to_dataframe()
print(f"   Unique providers with payments: {result1['unique_providers_with_payments'].values[0]:,}")
print(f"   Total payments: ${result1['total_payments'].values[0]:,.2f}")
print(f"   Total transactions: {result1['total_transactions'].values[0]:,}")

# Test 2: BCBSMI providers at Michigan facilities (current approach)
query2 = """
WITH bcbsmi_npis AS (
    SELECT DISTINCT CAST(NPI AS INT64) as NPI 
    FROM `data-analytics-389803.temp.bcbsmi_provider_npis`
)
SELECT 
    COUNT(DISTINCT f.NPI) as unique_bcbsmi_providers_at_mi_facilities,
    COUNT(DISTINCT op.covered_recipient_npi) as unique_providers_with_payments,
    SUM(op.total_amount_of_payment_usdollars) as total_payments
FROM `data-analytics-389803.conflixis_data_projects.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized` f
INNER JOIN bcbsmi_npis b ON f.NPI = b.NPI
LEFT JOIN `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized` op
    ON f.NPI = op.covered_recipient_npi
    AND op.program_year BETWEEN 2020 AND 2024
WHERE f.AFFILIATED_HQ_STATE = 'MI'
"""

print("\n2. BCBSMI providers at Michigan facilities:")
result2 = client.query(query2).to_dataframe()
print(f"   Unique BCBSMI providers at MI facilities: {result2['unique_bcbsmi_providers_at_mi_facilities'].values[0]:,}")
print(f"   Unique providers with payments: {result2['unique_providers_with_payments'].values[0]:,}")
print(f"   Total payments: ${result2['total_payments'].values[0]:,.2f}")

# Test 3: Count duplicates from multiple facility affiliations
query3 = """
WITH bcbsmi_npis AS (
    SELECT DISTINCT CAST(NPI AS INT64) as NPI 
    FROM `data-analytics-389803.temp.bcbsmi_provider_npis`
),
provider_facility_count AS (
    SELECT 
        f.NPI,
        COUNT(DISTINCT f.AFFILIATED_NAME) as facility_count
    FROM `data-analytics-389803.conflixis_data_projects.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized` f
    INNER JOIN bcbsmi_npis b ON f.NPI = b.NPI
    WHERE f.AFFILIATED_HQ_STATE = 'MI'
    GROUP BY f.NPI
)
SELECT 
    COUNT(*) as providers_with_multiple_facilities,
    AVG(facility_count) as avg_facilities_per_provider,
    MAX(facility_count) as max_facilities_per_provider
FROM provider_facility_count
WHERE facility_count > 1
"""

print("\n3. Multiple facility affiliations:")
result3 = client.query(query3).to_dataframe()
print(f"   Providers with multiple facilities: {result3['providers_with_multiple_facilities'].values[0]:,}")
print(f"   Average facilities per provider: {result3['avg_facilities_per_provider'].values[0]:.2f}")
print(f"   Max facilities per provider: {result3['max_facilities_per_provider'].values[0]}")

print("\n" + "=" * 80)
print("CONCLUSION:")
print("The discrepancy is due to summing payments multiple times when providers")
print("are affiliated with multiple facilities. Each provider-facility combination")
print("is counted separately, inflating the total payment amount.")
print("=" * 80)