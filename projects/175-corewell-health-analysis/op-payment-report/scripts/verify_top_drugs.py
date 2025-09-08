#!/usr/bin/env python3
"""
Verify the actual top prescribed drugs by value
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

print('VERIFYING TOP PRESCRIBED DRUGS BY VALUE')
print('='*80)

# First, get the actual top 5
top_query = """
WITH corewell_prescriptions AS (
    SELECT 
        rx.BRAND_NAME,
        rx.NPI,
        rx.PAYMENTS as rx_payments,
        rx.PRESCRIPTIONS
    FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
    JOIN `data-analytics-389803.temp.corewell_provider_npis` cp
      ON rx.NPI = SAFE_CAST(cp.NPI AS INT64)
    WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
      AND rx.BRAND_NAME IS NOT NULL
)
SELECT 
    BRAND_NAME,
    COUNT(DISTINCT NPI) as unique_prescribers,
    SUM(rx_payments) as total_payments,
    SUM(PRESCRIPTIONS) as total_prescriptions
FROM corewell_prescriptions
GROUP BY BRAND_NAME
ORDER BY total_payments DESC
LIMIT 5
"""

print('\nActual Top 5 Drugs by Total Payment Value:')
print('-'*80)

df = client.query(top_query).to_dataframe()
actual_top5 = {}
for _, row in df.iterrows():
    drug = row['BRAND_NAME']
    actual_top5[drug] = {
        'payments': row['total_payments'],
        'prescribers': int(row['unique_prescribers']),
        'prescriptions': int(row['total_prescriptions'])
    }
    print(f"{drug:20} | ${row['total_payments']:>15,.2f} | {int(row['unique_prescribers']):>6,} prescribers | {int(row['total_prescriptions']):>10,} scripts")

print('\n\nReport Claims vs Actual:')
print('-'*80)

# What the report claims
report_claims = {
    'HUMIRA': {'payments': 627441671, 'prescribers': 606},
    'ELIQUIS': {'payments': 612880706, 'prescribers': 7149},
    'TRULICITY': {'payments': 602965350, 'prescribers': 3795},
    'OZEMPIC': {'payments': 422557964, 'prescribers': 3723},
    'JARDIANCE': {'payments': 421991963, 'prescribers': 4709}
}

print(f"{'Drug':<12} | {'Report Payments':<20} | {'Actual Payments':<20} | {'Report Prescribers':<18} | {'Actual Prescribers'}")
print('-'*110)

for drug, report_data in report_claims.items():
    if drug in actual_top5:
        actual = actual_top5[drug]
        payment_diff = (actual['payments'] - report_data['payments']) / report_data['payments'] * 100
        prescriber_diff = actual['prescribers'] - report_data['prescribers']
        
        status = '✅' if abs(payment_diff) < 5 else '❌'
        
        print(f"{drug:<12} | ${report_data['payments']:>18,} | ${actual['payments']:>18,.0f} | {report_data['prescribers']:>17,} | {actual['prescribers']:>17,} {status}")
        if abs(payment_diff) >= 5:
            print(f"  ⚠️ Difference: {payment_diff:+.1f}% in payments, {prescriber_diff:+,} in prescribers")
    else:
        print(f"{drug:<12} | ${report_data['payments']:>18,} | {'NOT IN TOP 5':^18} | {report_data['prescribers']:>17,} | {'N/A':^17} ❌")

# Now check specific drugs mentioned in report
print('\n\nDetailed Verification of Report Claims:')
print('-'*80)

drugs_to_check = ['HUMIRA', 'ELIQUIS', 'TRULICITY', 'OZEMPIC', 'JARDIANCE']

for drug in drugs_to_check:
    query = f"""
    SELECT
      count(*) as record_count,
      count(distinct a.NPI) as distinct_npi,
      sum(PAYMENTS) as total_payments,
      sum(PRESCRIPTIONS) as total_prescriptions
    FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` a
    JOIN `data-analytics-389803.temp.corewell_provider_npis` b
      ON a.NPI = SAFE_CAST(b.NPI as INT64)
    WHERE UPPER(a.brand_name) = '{drug}'
      AND a.CLAIM_YEAR BETWEEN 2020 AND 2024
    """
    
    result = client.query(query).to_dataframe()
    if not result.empty and result.iloc[0]['distinct_npi'] > 0:
        row = result.iloc[0]
        report_val = report_claims.get(drug, {})
        
        print(f'\n{drug}:')
        print(f'  Records: {int(row["record_count"]):,}')
        print(f'  Actual Prescribers: {int(row["distinct_npi"]):,}')
        print(f'  Report Claims: {report_val.get("prescribers", "N/A"):,} prescribers')
        print(f'  Actual Total Payments: ${row["total_payments"]:,.2f}')
        print(f'  Report Claims: ${report_val.get("payments", 0):,}')
        
        if drug in report_claims:
            payment_diff_pct = (row["total_payments"] - report_val["payments"]) / report_val["payments"] * 100
            print(f'  Payment Difference: {payment_diff_pct:+.1f}%')
    else:
        print(f'\n{drug}: No data found')