#!/usr/bin/env python3
"""
QC Verification for Corewell Health Open Payments Report
Part 2: Prescription Patterns Verification
"""

import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
env_path = Path("/home/incent/conflixis-data-projects/.env")
if env_path.exists():
    load_dotenv(env_path)

# Create BigQuery client
service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY') or os.getenv('GCP_SERVICE_ACCOUNT')
if not service_account_json:
    print("No GCP credentials found")
    exit()

service_account_info = json.loads(service_account_json)
credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=['https://www.googleapis.com/auth/bigquery']
)

PROJECT_ID = "data-analytics-389803"
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

print("="*80)
print("COREWELL HEALTH PRESCRIPTION PATTERNS - QC VERIFICATION")
print(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# Query 1: Overall prescription metrics
print("\n1. OVERALL PRESCRIPTION METRICS VERIFICATION")
print("-"*60)

prescription_query = """
WITH corewell_prescriptions AS (
    SELECT 
        rx.NPI,
        rx.CLAIM_YEAR,
        rx.BRAND_NAME,
        rx.GENERIC_NAME,
        rx.PAYMENTS as rx_payments,
        rx.PRESCRIPTIONS,
        rx.UNIQUE_PATIENTS,
        rx.NDC_CODE
    FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
    WHERE CAST(rx.NPI AS STRING) IN (
        SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`
    )
    AND rx.CLAIM_YEAR BETWEEN 2020 AND 2024
),
summary_stats AS (
    SELECT 
        COUNT(DISTINCT NPI) as unique_prescribers,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(rx_payments) as total_prescription_payments,
        COUNT(DISTINCT COALESCE(BRAND_NAME, GENERIC_NAME)) as unique_drugs,
        SUM(UNIQUE_PATIENTS) as total_patient_instances
    FROM corewell_prescriptions
),
provider_coverage AS (
    SELECT COUNT(DISTINCT NPI) as total_corewell_providers
    FROM `data-analytics-389803.temp.corewell_provider_npis`
)
SELECT 
    s.*,
    p.total_corewell_providers,
    ROUND(s.unique_prescribers / p.total_corewell_providers * 100, 1) as pct_providers_prescribing
FROM summary_stats s
CROSS JOIN provider_coverage p
"""

prescription_results = client.query(prescription_query).to_dataframe()
if not prescription_results.empty:
    row = prescription_results.iloc[0]
    print(f"Unique prescribers: {row['unique_prescribers']:,}")
    print(f"Total Corewell providers: {row['total_corewell_providers']:,}")
    print(f"Percentage prescribing: {row['pct_providers_prescribing']:.1f}%")
    print(f"Total prescriptions: {float(row['total_prescriptions'])/1e6:.1f} million")
    print(f"Total prescription payments: ${float(row['total_prescription_payments'])/1e9:.1f} billion")
    print(f"Unique drugs: {row['unique_drugs']:,}")
    
    print("\nREPORT CLAIMS vs ACTUAL:")
    print(f"  Unique prescribers: Report: 13,122 | Actual: {row['unique_prescribers']:,}")
    print(f"  % of providers: Report: 92.6% | Actual: {row['pct_providers_prescribing']:.1f}%")
    print(f"  Total prescriptions: Report: 177.5M | Actual: {float(row['total_prescriptions'])/1e6:.1f}M")
    print(f"  Total payments: Report: $15.5B | Actual: ${float(row['total_prescription_payments'])/1e9:.1f}B")
    print(f"  Unique drugs: Report: 5,537 | Actual: {row['unique_drugs']:,}")

# Query 2: Top prescribed drugs by value
print("\n2. TOP PRESCRIBED DRUGS BY VALUE")
print("-"*60)

top_drugs_query = """
WITH corewell_prescriptions AS (
    SELECT 
        rx.BRAND_NAME,
        rx.NPI,
        rx.PAYMENTS as rx_payments,
        rx.PRESCRIPTIONS
    FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
    WHERE CAST(rx.NPI AS STRING) IN (
        SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`
    )
    AND rx.CLAIM_YEAR BETWEEN 2020 AND 2024
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
LIMIT 10
"""

top_drugs_results = client.query(top_drugs_query).to_dataframe()
if not top_drugs_results.empty:
    print("\nTop 10 Drugs by Total Payment Value:")
    print("Drug Name | Total Payments | Prescribers | Prescriptions")
    print("-" * 70)
    
    report_top5 = {
        'HUMIRA': {'payments': 627441671, 'prescribers': 606},
        'ELIQUIS': {'payments': 612880706, 'prescribers': 7149},
        'TRULICITY': {'payments': 602965350, 'prescribers': 3795},
        'OZEMPIC': {'payments': 422557964, 'prescribers': 3723},
        'JARDIANCE': {'payments': 421991963, 'prescribers': 4709}
    }
    
    for _, row in top_drugs_results.iterrows():
        drug_name = row['BRAND_NAME'][:20]
        print(f"{drug_name:20} | ${row['total_payments']:>13,.0f} | {row['unique_prescribers']:>11,} | {row['total_prescriptions']:>13,.0f}")
        
        # Check if in report's top 5
        if drug_name.upper() in report_top5:
            report_data = report_top5[drug_name.upper()]
            print(f"  ↳ Report: ${report_data['payments']:,}, {report_data['prescribers']:,} prescribers")

# Query 3: Year-over-year prescription trends
print("\n3. YEAR-OVER-YEAR PRESCRIPTION TRENDS")
print("-"*60)

yearly_rx_query = """
WITH corewell_prescriptions AS (
    SELECT 
        rx.CLAIM_YEAR as year,
        rx.NPI,
        rx.PAYMENTS as rx_payments,
        rx.PRESCRIPTIONS
    FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
    WHERE CAST(rx.NPI AS STRING) IN (
        SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`
    )
    AND rx.CLAIM_YEAR BETWEEN 2020 AND 2024
)
SELECT 
    year,
    COUNT(DISTINCT NPI) as unique_prescribers,
    SUM(rx_payments) as total_payments,
    SUM(PRESCRIPTIONS) as total_prescriptions,
    AVG(rx_payments) as avg_payment_per_record
FROM corewell_prescriptions
GROUP BY year
ORDER BY year
"""

yearly_rx_results = client.query(yearly_rx_query).to_dataframe()
if not yearly_rx_results.empty:
    print("\nYearly Prescription Trends:")
    print("Year | Prescribers | Total Payments | Total Prescriptions")
    print("-" * 65)
    
    for _, row in yearly_rx_results.iterrows():
        print(f"{row['year']:4.0f} | {row['unique_prescribers']:>11,} | ${float(row['total_payments'])/1e9:>6.2f}B | {float(row['total_prescriptions'])/1e6:>8.1f}M")

# Query 4: Provider coverage analysis
print("\n4. PROVIDER COVERAGE ANALYSIS")
print("-"*60)

coverage_query = """
WITH provider_categories AS (
    SELECT 
        cs.NPI,
        cs.Primary_Specialty,
        CASE 
            WHEN rx.NPI IS NOT NULL THEN 1 ELSE 0 
        END as has_prescriptions,
        CASE 
            WHEN op.NPI IS NOT NULL THEN 1 ELSE 0 
        END as has_payments
    FROM `data-analytics-389803.temp.corewell_provider_npis` cs
    LEFT JOIN (
        SELECT DISTINCT CAST(NPI AS STRING) AS NPI
        FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024`
        WHERE CLAIM_YEAR BETWEEN 2020 AND 2024
    ) rx ON cs.NPI = rx.NPI
    LEFT JOIN (
        SELECT DISTINCT CAST(covered_recipient_npi AS STRING) AS NPI
        FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static`
        WHERE program_year BETWEEN 2020 AND 2024
    ) op ON cs.NPI = op.NPI
)
SELECT 
    COUNT(*) as total_providers,
    SUM(has_prescriptions) as providers_with_rx,
    SUM(has_payments) as providers_with_payments,
    SUM(CASE WHEN has_prescriptions = 1 AND has_payments = 1 THEN 1 ELSE 0 END) as providers_with_both,
    SUM(CASE WHEN has_prescriptions = 1 AND has_payments = 0 THEN 1 ELSE 0 END) as rx_only,
    SUM(CASE WHEN has_prescriptions = 0 AND has_payments = 1 THEN 1 ELSE 0 END) as payments_only,
    SUM(CASE WHEN has_prescriptions = 0 AND has_payments = 0 THEN 1 ELSE 0 END) as neither
FROM provider_categories
"""

coverage_results = client.query(coverage_query).to_dataframe()
if not coverage_results.empty:
    row = coverage_results.iloc[0]
    print("\nProvider Activity Distribution:")
    print(f"Total Corewell providers: {row['total_providers']:,}")
    print(f"Providers with prescriptions: {row['providers_with_rx']:,} ({row['providers_with_rx']/row['total_providers']*100:.1f}%)")
    print(f"Providers with payments: {row['providers_with_payments']:,} ({row['providers_with_payments']/row['total_providers']*100:.1f}%)")
    print(f"Providers with both: {row['providers_with_both']:,} ({row['providers_with_both']/row['total_providers']*100:.1f}%)")
    print(f"Prescriptions only: {row['rx_only']:,} ({row['rx_only']/row['total_providers']*100:.1f}%)")
    print(f"Payments only: {row['payments_only']:,} ({row['payments_only']/row['total_providers']*100:.1f}%)")
    print(f"Neither: {row['neither']:,} ({row['neither']/row['total_providers']*100:.1f}%)")

print("\n" + "="*80)
print("PRESCRIPTION PATTERNS VERIFICATION COMPLETE")
print("="*80)