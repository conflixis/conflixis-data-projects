#!/usr/bin/env python3
"""Test corrected aggregation using primary facilities"""

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
print("TESTING CORRECTED AGGREGATION WITH PRIMARY FACILITIES")
print("=" * 80)

# Test corrected approach using primary facilities
query = """
WITH bcbsmi_npis AS (
    SELECT DISTINCT CAST(NPI AS INT64) as NPI 
    FROM `data-analytics-389803.temp.bcbsmi_provider_npis`
),
-- Get PRIMARY facility for each provider
primary_facilities AS (
    SELECT 
        f.NPI,
        f.AFFILIATED_NAME as Facility_Name,
        f.AFFILIATED_HQ_CITY as City,
        ROW_NUMBER() OVER (PARTITION BY f.NPI ORDER BY f.AFFILIATED_NAME) as facility_rank
    FROM `data-analytics-389803.conflixis_data_projects.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized` f
    INNER JOIN bcbsmi_npis b ON f.NPI = b.NPI
    WHERE f.AFFILIATED_HQ_STATE = 'MI'
),
-- Get payments for BCBSMI providers  
provider_payments AS (
    SELECT 
        b.NPI,
        SUM(op.total_amount_of_payment_usdollars) as total_provider_payments
    FROM bcbsmi_npis b
    LEFT JOIN `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized` op
        ON b.NPI = op.covered_recipient_npi
        AND op.program_year BETWEEN 2020 AND 2024
    GROUP BY b.NPI
)
SELECT 
    COUNT(DISTINCT pf.NPI) as unique_providers_at_mi_facilities,
    COUNT(DISTINCT CASE WHEN pp.total_provider_payments > 0 THEN pf.NPI END) as providers_with_payments,
    SUM(pp.total_provider_payments) as total_payments
FROM primary_facilities pf
INNER JOIN provider_payments pp ON pf.NPI = pp.NPI
WHERE pf.facility_rank = 1  -- PRIMARY facility only
"""

print("\nCorrected totals using PRIMARY facility assignment:")
result = client.query(query).to_dataframe()
print(f"   Unique BCBSMI providers at MI facilities: {result['unique_providers_at_mi_facilities'].values[0]:,}")
print(f"   Providers with payments: {result['providers_with_payments'].values[0]:,}")
print(f"   Total payments: ${result['total_payments'].values[0]:,.2f}")

print("\n" + "=" * 80)
print("VERIFICATION:")
print(f"Expected from main report: $304,498,887.94")
print(f"Our corrected total: ${result['total_payments'].values[0]:,.2f}")
print(f"Match: {abs(result['total_payments'].values[0] - 304498887.94) < 1}")
print("=" * 80)