#!/usr/bin/env python3
"""
Analyze specialties of Krystexxa prescribers with vs without payments
Testing hypothesis: Payment recipients may be specialists who naturally prescribe more
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

# Query to get provider details including specialty
query = """
WITH corewell_providers AS (
  SELECT DISTINCT 
    NPI,
    Primary_Specialty,
    Full_Name,
    SAFE_CAST(NPI AS INT64) as NPI_int
  FROM `data-analytics-389803.temp.corewell_provider_npis`
),
krystexxa_payments AS (
  SELECT
    a.covered_recipient_npi as NPI,
    SUM(a.total_amount_of_payment_usdollars) as total_op_payments,
    COUNT(*) as payment_count
  FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` a
  WHERE LOWER(a.name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE "%krystexxa%"
  GROUP BY a.covered_recipient_npi
),
krystexxa_prescriptions AS (
  SELECT
    rx.NPI,
    SUM(rx.PAYMENTS) as total_rx_payments,
    SUM(rx.PRESCRIPTIONS) as total_prescriptions,
    SUM(rx.UNIQUE_PATIENTS) as total_patients
  FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
  WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
    AND (LOWER(rx.BRAND_NAME) LIKE "%krystexxa%" 
         OR LOWER(rx.GENERIC_NAME) LIKE "%pegloticase%")
  GROUP BY rx.NPI
)
SELECT
  cp.NPI,
  cp.Full_Name,
  cp.Primary_Specialty,
  CASE 
    WHEN kpay.NPI IS NOT NULL THEN 'WITH payments'
    ELSE 'WITHOUT payments'
  END as payment_status,
  COALESCE(kpay.total_op_payments, 0) as op_payments,
  COALESCE(kpay.payment_count, 0) as payment_count,
  COALESCE(krx.total_rx_payments, 0) as rx_payments,
  COALESCE(krx.total_prescriptions, 0) as prescriptions,
  COALESCE(krx.total_patients, 0) as patients
FROM corewell_providers cp
INNER JOIN krystexxa_prescriptions krx
  ON cp.NPI_int = krx.NPI
LEFT JOIN krystexxa_payments kpay
  ON cp.NPI_int = kpay.NPI
WHERE cp.NPI_int IN (
  SELECT NPI FROM krystexxa_prescriptions
)
ORDER BY payment_status, rx_payments DESC
"""

print('KRYSTEXXA PRESCRIBER SPECIALTY ANALYSIS')
print('='*80)
print('\nHypothesis: Providers receiving payments may be specialists who naturally')
print('prescribe more Krystexxa (e.g., rheumatologists vs primary care)\n')

df = client.query(query).to_dataframe()

# Analyze by payment status
print('PROVIDERS WITH PAYMENTS (n=10):')
print('-'*60)
with_payments = df[df['payment_status'] == 'WITH payments']
for _, row in with_payments.iterrows():
    print(f"  {row['Full_Name'][:30]:<30} | {row['Primary_Specialty'][:25]:<25} | ${row['rx_payments']:>10,.0f} Rx | ${row['op_payments']:>8,.0f} OP")

print('\n\nPROVIDERS WITHOUT PAYMENTS (n=5):')
print('-'*60)
without_payments = df[df['payment_status'] == 'WITHOUT payments']
for _, row in without_payments.iterrows():
    print(f"  {row['Full_Name'][:30]:<30} | {row['Primary_Specialty'][:25]:<25} | ${row['rx_payments']:>10,.0f} Rx")

# Specialty summary
print('\n\nSPECIALTY DISTRIBUTION:')
print('='*80)

specialty_summary = df.groupby(['payment_status', 'Primary_Specialty']).agg({
    'NPI': 'count',
    'rx_payments': 'sum',
    'op_payments': 'sum'
}).reset_index()
specialty_summary.columns = ['Payment Status', 'Specialty', 'Provider Count', 'Total Rx', 'Total OP']

print('\nWITH PAYMENTS - Specialty Breakdown:')
with_pay_specs = specialty_summary[specialty_summary['Payment Status'] == 'WITH payments']
for _, row in with_pay_specs.iterrows():
    print(f"  {row['Specialty'][:35]:<35}: {row['Provider Count']} providers, ${row['Total Rx']:,.0f} total Rx")

print('\nWITHOUT PAYMENTS - Specialty Breakdown:')
without_pay_specs = specialty_summary[specialty_summary['Payment Status'] == 'WITHOUT payments']
for _, row in without_pay_specs.iterrows():
    print(f"  {row['Specialty'][:35]:<35}: {row['Provider Count']} providers, ${row['Total Rx']:,.0f} total Rx")

# Check if rheumatologists are overrepresented in payment group
print('\n\nKEY FINDINGS:')
print('='*80)

# Count rheumatology-related specialties
rheum_with = df[(df['payment_status'] == 'WITH payments') & 
                 (df['Primary_Specialty'].str.contains('Rheumat', case=False, na=False))].shape[0]
rheum_without = df[(df['payment_status'] == 'WITHOUT payments') & 
                    (df['Primary_Specialty'].str.contains('Rheumat', case=False, na=False))].shape[0]

print(f"Rheumatologists WITH payments: {rheum_with}/10 ({rheum_with/10*100:.0f}%)")
print(f"Rheumatologists WITHOUT payments: {rheum_without}/5 ({rheum_without/5*100:.0f}%)")

# Check for other specialist patterns
internal_with = df[(df['payment_status'] == 'WITH payments') & 
                    (df['Primary_Specialty'].str.contains('Internal Medicine', case=False, na=False))].shape[0]
internal_without = df[(df['payment_status'] == 'WITHOUT payments') & 
                       (df['Primary_Specialty'].str.contains('Internal Medicine', case=False, na=False))].shape[0]

print(f"\nInternal Medicine WITH payments: {internal_with}/10 ({internal_with/10*100:.0f}%)")
print(f"Internal Medicine WITHOUT payments: {internal_without}/5 ({internal_without/5*100:.0f}%)")

# Average Rx by specialty type
print('\n\nAVERAGE RX VALUE BY SPECIALTY TYPE:')
print('-'*60)
specialty_avg = df.groupby('Primary_Specialty')['rx_payments'].agg(['mean', 'count'])
specialty_avg = specialty_avg.sort_values('mean', ascending=False)
for spec, row in specialty_avg.iterrows():
    if row['count'] > 0:
        print(f"  {spec[:35]:<35}: ${row['mean']:>10,.0f} avg (n={int(row['count'])})")

print('\n\nCONCLUSION:')
print('='*80)
print('Check if specialists who naturally treat severe gout are overrepresented')
print('in the payment group, which would explain the 3.2x difference.')