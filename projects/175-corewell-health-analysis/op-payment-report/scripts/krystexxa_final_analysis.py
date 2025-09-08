#!/usr/bin/env python3
"""
Final Analysis: Resolving the Krystexxa 426x vs 3.2x discrepancy
"""

from google.cloud import bigquery
from google.oauth2 import service_account
import json, os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

env_path = Path('/home/incent/conflixis-data-projects/.env')
load_dotenv(env_path)

service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
service_account_info = json.loads(service_account_json)
credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=['https://www.googleapis.com/auth/bigquery']
)

client = bigquery.Client(project='data-analytics-389803', credentials=credentials)

print("="*80)
print("FINAL KRYSTEXXA ANALYSIS - RESOLVING THE DISCREPANCY")
print("="*80)

# Get the actual raw data
query = """
WITH corewell_providers AS (
    SELECT DISTINCT NPI
    FROM `data-analytics-389803.temp.corewell_provider_npis`
),
krystexxa_payments AS (
    SELECT 
        CAST(op.covered_recipient_npi AS STRING) AS NPI,
        SUM(op.total_amount_of_payment_usdollars) as total_op_payments
    FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` op
    WHERE op.program_year BETWEEN 2020 AND 2024
        AND op.name_of_drug_or_biological_or_device_or_medical_supply_1 IS NOT NULL
        AND UPPER(op.name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%KRYSTEXXA%'
    GROUP BY NPI
),
krystexxa_prescriptions AS (
    SELECT 
        CAST(rx.NPI AS STRING) AS NPI,
        rx.CLAIM_YEAR,
        SUM(rx.PAYMENTS) as rx_payments_year
    FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
    WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
        AND (UPPER(rx.BRAND_NAME) LIKE '%KRYSTEXXA%'
             OR UPPER(rx.GENERIC_NAME) LIKE '%PEGLOTICASE%')
    GROUP BY NPI, CLAIM_YEAR
),
krystexxa_total AS (
    SELECT 
        NPI,
        SUM(rx_payments_year) as total_rx_payments
    FROM krystexxa_prescriptions
    GROUP BY NPI
),
provider_detail AS (
    SELECT 
        cp.NPI,
        COALESCE(kt.total_rx_payments, 0) as rx_payments,
        COALESCE(kp.total_op_payments, 0) as op_payments,
        CASE WHEN kp.NPI IS NOT NULL THEN 'WITH payments' ELSE 'WITHOUT payments' END as payment_status
    FROM corewell_providers cp
    LEFT JOIN krystexxa_payments kp ON cp.NPI = kp.NPI
    LEFT JOIN krystexxa_total kt ON cp.NPI = kt.NPI
    WHERE kt.NPI IS NOT NULL  -- Only providers who prescribed Krystexxa
)
SELECT * FROM provider_detail
ORDER BY payment_status, rx_payments DESC
"""

print("\n1. RAW DATA FOR ALL KRYSTEXXA PRESCRIBERS:")
print("-"*60)
df = client.query(query).to_dataframe()

# Group by payment status
with_payments = df[df['payment_status'] == 'WITH payments']
without_payments = df[df['payment_status'] == 'WITHOUT payments']

print(f"\nProviders WITH payments ({len(with_payments)} total):")
for idx, row in with_payments.iterrows():
    print(f"  NPI {row['NPI']}: ${row['rx_payments']:,.2f} Rx, ${row['op_payments']:,.2f} payments")

print(f"\nProviders WITHOUT payments ({len(without_payments)} total):")
for idx, row in without_payments.iterrows():
    print(f"  NPI {row['NPI']}: ${row['rx_payments']:,.2f} Rx")

print("\n2. CALCULATION COMPARISON:")
print("-"*60)

# Method 1: Average of totals (provider-level)
avg_with = with_payments['rx_payments'].mean()
avg_without = without_payments['rx_payments'].mean()

print(f"\nAverage prescription value per provider:")
print(f"  WITH payments: ${avg_with:,.2f}")
print(f"  WITHOUT payments: ${avg_without:,.2f}")
print(f"  Ratio: {avg_with/avg_without:.1f}x")

print("\n3. COMPARISON TO REPORT:")
print("-"*60)
print("Report claims:")
print("  WITH payments: $3,524,074")
print("  WITHOUT payments: $8,271")
print("  Ratio: 426x")

print("\nActual data shows:")
print(f"  WITH payments: ${avg_with:,.2f}")
print(f"  WITHOUT payments: ${avg_without:,.2f}")
print(f"  Ratio: {avg_with/avg_without:.1f}x")

# Check for unit confusion
print("\n4. CHECKING FOR UNIT CONFUSION:")
print("-"*60)

# Are the report numbers in different units?
report_with = 3524074
report_without = 8271
actual_with = avg_with
actual_without = avg_without

print(f"Report WITH / Actual WITH: {report_with / actual_with:.2f}")
print(f"Report WITHOUT / Actual WITHOUT: {report_without / actual_without:.2f}")

if abs(report_with / actual_with - 100) < 10:
    print("\nPOSSIBLE EXPLANATION: Report numbers might be in cents, not dollars!")
    print(f"  Report in dollars: ${report_with/100:,.2f} vs Actual: ${actual_with:,.2f}")
    print(f"  Report in dollars: ${report_without/100:,.2f} vs Actual: ${actual_without:,.2f}")
elif abs(report_with / actual_with - 1000) < 100:
    print("\nPOSSIBLE EXPLANATION: Report numbers might be in thousands!")

print("\n5. FINAL VERDICT:")
print("-"*60)

if abs(avg_with - report_with) < 1000:
    print("✅ THE REPORT IS CORRECT!")
    print(f"   The actual ratio IS approximately 426x ({avg_with/avg_without:.1f}x)")
    print("   My original QC script had an ERROR in displaying the values.")
else:
    print("❌ THE REPORT HAS AN ERROR!")
    print(f"   Actual ratio is {avg_with/avg_without:.1f}x, not 426x")
    print(f"   Actual WITH payments average: ${avg_with:,.2f}")
    print(f"   Actual WITHOUT payments average: ${avg_without:,.2f}")

print("\n" + "="*80)