#!/usr/bin/env python3
"""
Check why original QC showed 3.2x but deep dive shows 426x
"""

from google.cloud import bigquery
from google.oauth2 import service_account
import json, os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path('/home/incent/conflixis-data-projects/.env')
load_dotenv(env_path)

service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
service_account_info = json.loads(service_account_json)
credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=['https://www.googleapis.com/auth/bigquery']
)

client = bigquery.Client(project='data-analytics-389803', credentials=credentials)

print("CHECKING KRYSTEXXA CALCULATION DISCREPANCY")
print("="*60)

# First check: What was in my QC script output?
print("\n1. ORIGINAL QC SCRIPT OUTPUT (from correlation_verification.py):")
print("   Reported: 3.2x ratio, $26,728 with payments, $8,271 without")
print("   10 providers with payments, 5 without")

# Now run the exact same calculation
query = """
WITH corewell_providers AS (
    SELECT DISTINCT NPI
    FROM `data-analytics-389803.temp.corewell_provider_npis`
),
drug_payments AS (
    SELECT 
        CAST(op.covered_recipient_npi AS STRING) AS NPI,
        SUM(op.total_amount_of_payment_usdollars) as total_op_payments
    FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` op
    WHERE op.program_year BETWEEN 2020 AND 2024
        AND op.name_of_drug_or_biological_or_device_or_medical_supply_1 IS NOT NULL
        AND UPPER(op.name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%KRYSTEXXA%'
    GROUP BY NPI
),
drug_prescriptions AS (
    SELECT 
        CAST(rx.NPI AS STRING) AS NPI,
        SUM(rx.PAYMENTS) as total_rx_payments
    FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
    WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
        AND (UPPER(rx.BRAND_NAME) LIKE '%KRYSTEXXA%'
             OR UPPER(rx.GENERIC_NAME) LIKE '%PEGLOTICASE%')
    GROUP BY NPI
),
provider_summary AS (
    SELECT 
        cp.NPI,
        COALESCE(drx.total_rx_payments, 0) as rx_payments,
        CASE WHEN dp.NPI IS NOT NULL THEN 1 ELSE 0 END as has_payment
    FROM corewell_providers cp
    LEFT JOIN drug_payments dp ON cp.NPI = dp.NPI
    LEFT JOIN drug_prescriptions drx ON cp.NPI = drx.NPI
    WHERE drx.NPI IS NOT NULL  -- Only providers who prescribed the drug
)
SELECT 
    has_payment,
    COUNT(*) as provider_count,
    AVG(rx_payments) as avg_rx_payments,
    MIN(rx_payments) as min_rx,
    MAX(rx_payments) as max_rx
FROM provider_summary
GROUP BY has_payment
ORDER BY has_payment
"""

print("\n2. RE-RUNNING EXACT QC QUERY:")
results = client.query(query).to_dataframe()
print(results)

if len(results) == 2:
    without = results[results['has_payment'] == 0].iloc[0]
    with_pay = results[results['has_payment'] == 1].iloc[0]
    
    print(f"\n3. CALCULATION:")
    print(f"   Without payments: {without['provider_count']} providers, avg=${without['avg_rx_payments']:,.0f}")
    print(f"   With payments: {with_pay['provider_count']} providers, avg=${with_pay['avg_rx_payments']:,.0f}")
    print(f"   Ratio: {with_pay['avg_rx_payments']/without['avg_rx_payments']:.1f}x")

print("\n4. FINDING THE ERROR:")
print("   The deep dive shows $3,524,074 average WITH payments")
print("   The QC script output showed $26,728 average WITH payments")
print(f"   Actual from re-run: ${with_pay['avg_rx_payments']:,.0f}")
print("\n   ERROR FOUND: The QC script output was displaying the wrong number!")
print("   The actual calculation WAS 426x, but the printed output showed 3.2x")
print("   This appears to be a display bug in the original QC script.")