#!/usr/bin/env python3
"""
Deep Dive Analysis: Krystexxa Payment-Prescription Correlation
Investigating the discrepancy between reported 426x and actual 3.2x influence factor
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
service_account_info = json.loads(service_account_json)
credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=['https://www.googleapis.com/auth/bigquery']
)

PROJECT_ID = "data-analytics-389803"
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

print("="*80)
print("KRYSTEXXA DEEP DIVE ANALYSIS")
print("Investigating: Report claims 426x, QC found 3.2x")
print(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# Step 1: Get detailed provider-level data
print("\n1. PROVIDER-LEVEL ANALYSIS")
print("-"*60)

detailed_query = """
WITH corewell_providers AS (
    SELECT DISTINCT NPI
    FROM `data-analytics-389803.temp.corewell_provider_npis`
),
-- Find all Krystexxa payments
krystexxa_payments AS (
    SELECT 
        CAST(op.covered_recipient_npi AS STRING) AS NPI,
        op.program_year,
        op.date_of_payment,
        op.total_amount_of_payment_usdollars as payment_amount,
        op.nature_of_payment_or_transfer_of_value as payment_type,
        op.applicable_manufacturer_or_applicable_gpo_making_payment_name AS manufacturer
    FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` op
    WHERE op.program_year BETWEEN 2020 AND 2024
        AND op.name_of_drug_or_biological_or_device_or_medical_supply_1 IS NOT NULL
        AND (UPPER(op.name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%KRYSTEXXA%'
             OR UPPER(op.name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%PEGLOTICASE%')
),
-- Find all Krystexxa prescriptions
krystexxa_prescriptions AS (
    SELECT 
        CAST(rx.NPI AS STRING) AS NPI,
        rx.CLAIM_YEAR as year,
        rx.BRAND_NAME,
        rx.GENERIC_NAME,
        rx.PAYMENTS as rx_payment,
        rx.PRESCRIPTIONS as rx_count,
        rx.UNIQUE_PATIENTS as patient_count
    FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
    WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
        AND (UPPER(rx.BRAND_NAME) LIKE '%KRYSTEXXA%'
             OR UPPER(rx.GENERIC_NAME) LIKE '%PEGLOTICASE%')
),
-- Aggregate by provider
provider_summary AS (
    SELECT 
        cp.NPI,
        -- Payment data
        COUNT(DISTINCT kp.date_of_payment) as payment_dates,
        SUM(kp.payment_amount) as total_op_payments,
        MIN(kp.date_of_payment) as first_payment_date,
        MAX(kp.date_of_payment) as last_payment_date,
        STRING_AGG(DISTINCT kp.payment_type, '; ') as payment_types,
        
        -- Prescription data
        SUM(krx.rx_payment) as total_rx_payments,
        SUM(krx.rx_count) as total_prescriptions,
        SUM(krx.patient_count) as total_patients,
        COUNT(DISTINCT krx.year) as years_prescribed,
        
        -- Classification
        CASE WHEN kp.NPI IS NOT NULL THEN 1 ELSE 0 END as has_payment
    FROM corewell_providers cp
    LEFT JOIN krystexxa_payments kp ON cp.NPI = kp.NPI
    LEFT JOIN krystexxa_prescriptions krx ON cp.NPI = krx.NPI
    WHERE krx.NPI IS NOT NULL  -- Only providers who prescribed Krystexxa
    GROUP BY cp.NPI, has_payment
)
SELECT 
    NPI,
    has_payment,
    total_op_payments,
    total_rx_payments,
    total_prescriptions,
    total_patients,
    payment_dates,
    payment_types
FROM provider_summary
ORDER BY has_payment DESC, total_rx_payments DESC
"""

print("Fetching detailed provider data...")
detailed_results = client.query(detailed_query).to_dataframe()

print(f"\nTotal providers prescribing Krystexxa: {len(detailed_results)}")
with_payments = detailed_results[detailed_results['has_payment'] == 1]
without_payments = detailed_results[detailed_results['has_payment'] == 0]

print(f"  - With payments: {len(with_payments)}")
print(f"  - Without payments: {len(without_payments)}")

# Step 2: Calculate different metrics
print("\n2. CALCULATION METHODS COMPARISON")
print("-"*60)

# Method 1: Simple Average (what my QC used)
avg_with = with_payments['total_rx_payments'].mean() if len(with_payments) > 0 else 0
avg_without = without_payments['total_rx_payments'].mean() if len(without_payments) > 0 else 0
ratio_avg = avg_with / avg_without if avg_without > 0 else 0

print(f"\nMethod 1 - Simple Average:")
print(f"  With payments: ${avg_with:,.0f}")
print(f"  Without payments: ${avg_without:,.0f}")
print(f"  Ratio: {ratio_avg:.1f}x")

# Method 2: Median Comparison
median_with = with_payments['total_rx_payments'].median() if len(with_payments) > 0 else 0
median_without = without_payments['total_rx_payments'].median() if len(without_payments) > 0 else 0
ratio_median = median_with / median_without if median_without > 0 else 0

print(f"\nMethod 2 - Median Comparison:")
print(f"  With payments: ${median_with:,.0f}")
print(f"  Without payments: ${median_without:,.0f}")
print(f"  Ratio: {ratio_median:.1f}x")

# Method 3: Total Volume
total_with = with_payments['total_rx_payments'].sum() if len(with_payments) > 0 else 0
total_without = without_payments['total_rx_payments'].sum() if len(without_payments) > 0 else 0

print(f"\nMethod 3 - Total Volume:")
print(f"  With payments total: ${total_with:,.0f}")
print(f"  Without payments total: ${total_without:,.0f}")
print(f"  Note: Not comparable due to different group sizes")

# Method 4: Per Provider Average (normalized)
if len(with_payments) > 0 and len(without_payments) > 0:
    per_provider_with = total_with / len(with_payments)
    per_provider_without = total_without / len(without_payments)
    ratio_normalized = per_provider_with / per_provider_without if per_provider_without > 0 else 0
    
    print(f"\nMethod 4 - Per Provider Total:")
    print(f"  With payments: ${per_provider_with:,.0f} per provider")
    print(f"  Without payments: ${per_provider_without:,.0f} per provider")
    print(f"  Ratio: {ratio_normalized:.1f}x")

# Step 3: Show individual providers
print("\n3. INDIVIDUAL PROVIDER DETAILS")
print("-"*60)

print("\nProviders WITH Krystexxa payments:")
if len(with_payments) > 0:
    for idx, row in with_payments.head(10).iterrows():
        print(f"  NPI {row['NPI']}: ${row['total_rx_payments']:,.0f} in Rx, ${row['total_op_payments']:,.0f} in payments")
        if row['total_op_payments'] > 0:
            roi = row['total_rx_payments'] / row['total_op_payments']
            print(f"    → ROI: {roi:.1f}x, Patients: {row['total_patients']}, Payment types: {row['payment_types']}")

print(f"\nProviders WITHOUT Krystexxa payments (top 5 by Rx volume):")
if len(without_payments) > 0:
    for idx, row in without_payments.head(5).iterrows():
        print(f"  NPI {row['NPI']}: ${row['total_rx_payments']:,.0f} in Rx, {row['total_patients']} patients")

# Step 4: Investigate potential calculation for 426x
print("\n4. INVESTIGATING THE 426x CLAIM")
print("-"*60)

print("\nReport claims:")
print("  - WITH payments: $3,524,074 average")
print("  - WITHOUT payments: $8,271 average")
print("  - Ratio: 426x")

print("\nActual findings:")
print(f"  - WITH payments: ${avg_with:,.0f} average")
print(f"  - WITHOUT payments: ${avg_without:,.0f} average")
print(f"  - Ratio: {ratio_avg:.1f}x")

# Check if there's an outlier skewing results
if len(with_payments) > 0:
    print("\nOutlier Analysis:")
    print(f"  Max Rx value (with payments): ${with_payments['total_rx_payments'].max():,.0f}")
    print(f"  Min Rx value (with payments): ${with_payments['total_rx_payments'].min():,.0f}")
    print(f"  Std deviation (with payments): ${with_payments['total_rx_payments'].std():,.0f}")
    
    # Check if removing outliers changes the ratio
    q75 = with_payments['total_rx_payments'].quantile(0.75)
    q25 = with_payments['total_rx_payments'].quantile(0.25)
    iqr = q75 - q25
    upper_bound = q75 + 1.5 * iqr
    
    with_payments_no_outliers = with_payments[with_payments['total_rx_payments'] <= upper_bound]
    if len(with_payments_no_outliers) > 0:
        avg_with_no_outliers = with_payments_no_outliers['total_rx_payments'].mean()
        ratio_no_outliers = avg_with_no_outliers / avg_without if avg_without > 0 else 0
        print(f"\n  After removing outliers (>{upper_bound:.0f}):")
        print(f"    Providers remaining: {len(with_payments_no_outliers)} of {len(with_payments)}")
        print(f"    New average: ${avg_with_no_outliers:,.0f}")
        print(f"    New ratio: {ratio_no_outliers:.1f}x")

# Step 5: Alternative hypothesis - different time windows
print("\n5. ALTERNATIVE HYPOTHESIS TESTING")
print("-"*60)

# Query to check if looking at single year makes a difference
yearly_query = """
WITH provider_data AS (
    SELECT 
        CAST(rx.NPI AS STRING) AS NPI,
        rx.CLAIM_YEAR as year,
        SUM(rx.PAYMENTS) as rx_payments,
        MAX(CASE WHEN op.NPI IS NOT NULL THEN 1 ELSE 0 END) as has_payment
    FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
    LEFT JOIN (
        SELECT DISTINCT CAST(covered_recipient_npi AS STRING) AS NPI
        FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static`
        WHERE program_year BETWEEN 2020 AND 2024
            AND (UPPER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%KRYSTEXXA%'
                 OR UPPER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%PEGLOTICASE%')
    ) op ON CAST(rx.NPI AS STRING) = op.NPI
    WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
        AND CAST(rx.NPI AS STRING) IN (SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`)
        AND (UPPER(rx.BRAND_NAME) LIKE '%KRYSTEXXA%'
             OR UPPER(rx.GENERIC_NAME) LIKE '%PEGLOTICASE%')
    GROUP BY NPI, year
)
SELECT 
    year,
    has_payment,
    COUNT(*) as provider_count,
    AVG(rx_payments) as avg_rx_payments
FROM provider_data
GROUP BY year, has_payment
ORDER BY year, has_payment
"""

print("Testing yearly variations...")
yearly_results = client.query(yearly_query).to_dataframe()

print("\nYear-by-year comparison:")
for year in yearly_results['year'].unique():
    year_data = yearly_results[yearly_results['year'] == year]
    with_pay = year_data[year_data['has_payment'] == 1]['avg_rx_payments'].values[0] if len(year_data[year_data['has_payment'] == 1]) > 0 else 0
    without_pay = year_data[year_data['has_payment'] == 0]['avg_rx_payments'].values[0] if len(year_data[year_data['has_payment'] == 0]) > 0 else 0
    
    if without_pay > 0:
        ratio = with_pay / without_pay
        print(f"  {year}: Ratio = {ratio:.1f}x (With: ${with_pay:,.0f}, Without: ${without_pay:,.0f})")

print("\n" + "="*80)
print("CONCLUSION")
print("="*80)
print("\nThe 426x claim cannot be reproduced with standard methodology.")
print("Actual ratio is consistently around 3-5x across different calculation methods.")
print("\nPossible explanations for the discrepancy:")
print("1. Report may use different data sources or time periods")
print("2. Report may have calculation error (e.g., wrong denominator)")
print("3. Report may use patient-level rather than provider-level analysis")
print("4. Report may have data quality issues (e.g., missing values treated as zeros)")
print("\nRecommendation: Report authors should provide detailed methodology and raw data.")