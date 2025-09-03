#!/usr/bin/env python3
"""
Comprehensive Krystexxa Analysis with ROI
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

# Comprehensive query with ROI
query = """
WITH corewell_providers AS (
  SELECT DISTINCT 
    NPI,
    SAFE_CAST(NPI AS INT64) as NPI_int
  FROM `data-analytics-389803.temp.corewell_provider_npis`
),
krystexxa_payments AS (
  SELECT
    a.covered_recipient_npi,
    COUNT(*) as payment_count,
    SUM(a.total_amount_of_payment_usdollars) as total_op_payments,
    MIN(a.date_of_payment) as first_payment,
    MAX(a.date_of_payment) as last_payment,
    STRING_AGG(DISTINCT a.nature_of_payment_or_transfer_of_value, "; ") as payment_types
  FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` a
  JOIN corewell_providers b
    ON a.covered_recipient_npi = b.NPI_int
  WHERE LOWER(a.name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE "%krystexxa%"
  GROUP BY a.covered_recipient_npi
),
krystexxa_prescriptions AS (
  SELECT
    rx.NPI,
    SUM(rx.PAYMENTS) as total_rx_payments,
    SUM(rx.PRESCRIPTIONS) as total_prescriptions,
    SUM(rx.UNIQUE_PATIENTS) as total_patients,
    COUNT(DISTINCT rx.CLAIM_YEAR) as years_prescribed,
    MIN(rx.claim_date) as first_prescription,
    MAX(rx.claim_date) as last_prescription
  FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
  JOIN corewell_providers cp
    ON rx.NPI = cp.NPI_int
  WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
    AND (LOWER(rx.BRAND_NAME) LIKE "%krystexxa%" 
         OR LOWER(rx.GENERIC_NAME) LIKE "%pegloticase%")
  GROUP BY rx.NPI
),
combined AS (
  SELECT
    COALESCE(CAST(kp.covered_recipient_npi AS STRING), CAST(krx.NPI AS STRING)) as NPI,
    COALESCE(kp.payment_count, 0) as payment_count,
    COALESCE(kp.total_op_payments, 0) as op_payments,
    kp.first_payment,
    kp.last_payment,
    kp.payment_types,
    COALESCE(krx.total_rx_payments, 0) as rx_payments,
    COALESCE(krx.total_prescriptions, 0) as prescriptions,
    COALESCE(krx.total_patients, 0) as patients,
    krx.years_prescribed,
    krx.first_prescription,
    krx.last_prescription,
    CASE 
      WHEN kp.covered_recipient_npi IS NOT NULL AND krx.NPI IS NOT NULL THEN "Both payments and prescriptions"
      WHEN kp.covered_recipient_npi IS NOT NULL THEN "Payments only"
      WHEN krx.NPI IS NOT NULL THEN "Prescriptions only"
    END as provider_type,
    CASE 
      WHEN kp.total_op_payments > 0 THEN krx.total_rx_payments / kp.total_op_payments
      ELSE NULL
    END as individual_roi
  FROM krystexxa_payments kp
  FULL OUTER JOIN krystexxa_prescriptions krx
    ON kp.covered_recipient_npi = krx.NPI
),
baseline AS (
  SELECT AVG(rx_payments) as baseline_rx_avg
  FROM combined
  WHERE provider_type = "Prescriptions only"
)
SELECT 
  provider_type,
  COUNT(*) as provider_count,
  SUM(payment_count) as total_payment_records,
  SUM(op_payments) as total_op_payments,
  AVG(CASE WHEN op_payments > 0 THEN op_payments END) as avg_op_payment_when_paid,
  SUM(rx_payments) as total_rx_payments,
  AVG(rx_payments) as avg_rx_payments,
  SUM(prescriptions) as total_prescriptions,
  SUM(patients) as total_patients,
  AVG(individual_roi) as avg_individual_roi,
  MIN(individual_roi) as min_individual_roi,
  MAX(individual_roi) as max_individual_roi,
  CASE 
    WHEN SUM(op_payments) > 0 AND provider_type = "Both payments and prescriptions" THEN
      (AVG(rx_payments) - (SELECT baseline_rx_avg FROM baseline)) / AVG(CASE WHEN op_payments > 0 THEN op_payments END)
    ELSE NULL
  END as group_roi_vs_baseline,
  CASE 
    WHEN SUM(op_payments) > 0 THEN SUM(rx_payments) / SUM(op_payments)
    ELSE NULL
  END as simple_roi
FROM combined
GROUP BY provider_type
ORDER BY provider_type
"""

print('KRYSTEXXA COMPREHENSIVE ANALYSIS')
print('='*80)
df = client.query(query).to_dataframe()

# Format output nicely
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.float_format', '{:.2f}'.format)

print("\nProvider Type Summary:")
for _, row in df.iterrows():
    print(f"\n{row['provider_type']}:")
    print(f"  Providers: {int(row['provider_count'])}")
    print(f"  Total OP payments: ${row['total_op_payments']:,.2f}" if pd.notna(row['total_op_payments']) else "  Total OP payments: $0")
    print(f"  Total Rx payments: ${row['total_rx_payments']:,.2f}")
    print(f"  Average Rx per provider: ${row['avg_rx_payments']:,.2f}")
    if pd.notna(row['simple_roi']):
        print(f"  Simple ROI: {row['simple_roi']:.2f}x")
    if pd.notna(row['avg_individual_roi']):
        print(f"  Avg Individual ROI: {row['avg_individual_roi']:.2f}x")
    if pd.notna(row['group_roi_vs_baseline']):
        print(f"  Group ROI vs baseline: {row['group_roi_vs_baseline']:.2f}x")

# Now the correlation query
query2 = """
WITH corewell_providers AS (
  SELECT DISTINCT 
    NPI,
    SAFE_CAST(NPI AS INT64) as NPI_int
  FROM `data-analytics-389803.temp.corewell_provider_npis`
),
krystexxa_payments AS (
  SELECT
    a.covered_recipient_npi as NPI,
    SUM(a.total_amount_of_payment_usdollars) as op_payments
  FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` a
  JOIN corewell_providers b
    ON a.covered_recipient_npi = b.NPI_int
  WHERE LOWER(a.name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE "%krystexxa%"
  GROUP BY a.covered_recipient_npi
),
krystexxa_prescriptions AS (
  SELECT
    rx.NPI,
    SUM(rx.PAYMENTS) as rx_payments
  FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
  JOIN corewell_providers cp
    ON rx.NPI = cp.NPI_int
  WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
    AND (LOWER(rx.BRAND_NAME) LIKE "%krystexxa%" 
         OR LOWER(rx.GENERIC_NAME) LIKE "%pegloticase%")
  GROUP BY rx.NPI
),
with_median AS (
  SELECT
    krx.NPI,
    krx.rx_payments,
    CASE WHEN kp.NPI IS NOT NULL THEN 1 ELSE 0 END as has_payment
  FROM krystexxa_prescriptions krx
  LEFT JOIN krystexxa_payments kp
    ON krx.NPI = kp.NPI
)
SELECT
  has_payment,
  COUNT(*) as provider_count,
  AVG(rx_payments) as avg_rx_payments,
  MIN(rx_payments) as min_rx_payments,
  MAX(rx_payments) as max_rx_payments,
  APPROX_QUANTILES(rx_payments, 100)[OFFSET(50)] as median_rx_payments,
  STDDEV(rx_payments) as stddev_rx_payments,
  SUM(rx_payments) as total_rx_payments
FROM with_median
GROUP BY has_payment
ORDER BY has_payment
"""

print('\n\nCORRELATION ANALYSIS:')
print('='*80)
df2 = client.query(query2).to_dataframe()
print(df2.to_string())

if len(df2) == 2:
    without = df2[df2['has_payment'] == 0].iloc[0]
    with_pay = df2[df2['has_payment'] == 1].iloc[0]
    
    mean_ratio = with_pay['avg_rx_payments'] / without['avg_rx_payments']
    median_ratio = with_pay['median_rx_payments'] / without['median_rx_payments']
    
    print(f'\n\nKEY FINDINGS:')
    print(f'='*80)
    print(f'Providers WITH Krystexxa payments: {int(with_pay["provider_count"])} providers')
    print(f'  Mean Rx value: ${with_pay["avg_rx_payments"]:,.2f}')
    print(f'  Median Rx value: ${with_pay["median_rx_payments"]:,.2f}')
    print(f'  Range: ${with_pay["min_rx_payments"]:,.2f} - ${with_pay["max_rx_payments"]:,.2f}')
    print(f'\nProviders WITHOUT Krystexxa payments: {int(without["provider_count"])} providers')
    print(f'  Mean Rx value: ${without["avg_rx_payments"]:,.2f}')
    print(f'  Median Rx value: ${without["median_rx_payments"]:,.2f}')
    print(f'  Range: ${without["min_rx_payments"]:,.2f} - ${without["max_rx_payments"]:,.2f}')
    print(f'\nRATIO CALCULATIONS:')
    print(f'  Mean-based ratio: {mean_ratio:.1f}x')
    print(f'  Median-based ratio: {median_ratio:.1f}x')
    
    print(f'\n\nCOMPARISON TO REPORT:')
    print(f'='*80)
    print(f'Report claimed: 426x ratio')
    print(f'Report claimed: $3,524,074 avg WITH payments')
    print(f'Report claimed: $8,271 avg WITHOUT payments')
    print(f'\nActual data shows: {mean_ratio:.1f}x ratio')
    print(f'Actual: ${with_pay["avg_rx_payments"]:,.2f} avg WITH payments')
    print(f'Actual: ${without["avg_rx_payments"]:,.2f} avg WITHOUT payments')
    
    discrepancy_factor = 3524074 / with_pay["avg_rx_payments"]
    print(f'\nDISCREPANCY: Report\'s WITH payment number is {discrepancy_factor:.1f}x too high')