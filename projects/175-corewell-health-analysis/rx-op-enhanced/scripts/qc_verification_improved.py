#!/usr/bin/env python3
"""
Improved QC Verification for Corewell Health Deep Dive Analysis Report
This script addresses statistical issues in provider type aggregation:
- Uses provider-weighted averaging instead of row-weighted
- Properly handles LEFT JOINs and NULL values
- Ensures consistent year windows across metrics
- Deduplicates provider roles
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
print("COREWELL HEALTH IMPROVED QC VERIFICATION")
print(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# First, run diagnostic query to understand data scales
print("\n1. DIAGNOSTIC QUERY - DATA SCALES AND DUPLICATES")
print("-"*60)

diagnostic_query = """
-- Check for duplicate NPIs in provider_types
WITH provider_duplicates AS (
    SELECT 
        CAST(NPI AS STRING) as NPI,
        COUNT(*) as role_count,
        STRING_AGG(ROLE_NAME, ', ' ORDER BY ROLE_NAME) as all_roles
    FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW`
    WHERE CAST(NPI AS STRING) IN (
        SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`
    )
    GROUP BY NPI
    HAVING COUNT(*) > 1
),
scales AS (
    SELECT 
        'Corewell NPIs' as metric,
        COUNT(DISTINCT NPI) as count
    FROM `data-analytics-389803.temp.corewell_provider_npis`
    
    UNION ALL
    
    SELECT 
        'NPIs in PHYSICIANS_OVERVIEW' as metric,
        COUNT(DISTINCT CAST(NPI AS STRING)) as count
    FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW`
    WHERE CAST(NPI AS STRING) IN (
        SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`
    )
    
    UNION ALL
    
    SELECT 
        'NPIs with rx_op data (2021-2022)' as metric,
        COUNT(DISTINCT NPI) as count
    FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
    WHERE NPI IN (SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`)
    AND year IN (2021, 2022)
    
    UNION ALL
    
    SELECT 
        'NPIs with payments (2021-2022)' as metric,
        COUNT(DISTINCT NPI) as count
    FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
    WHERE NPI IN (SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`)
    AND year IN (2021, 2022)
    AND TotalDollarsFrom > 0
    
    UNION ALL
    
    SELECT 
        CONCAT('Duplicate NPIs (', CAST(COUNT(*) AS STRING), ' providers)') as metric,
        SUM(role_count) as count
    FROM provider_duplicates
)
SELECT * FROM scales
ORDER BY metric
"""

diagnostic_results = client.query(diagnostic_query).to_dataframe()
if not diagnostic_results.empty:
    print("\nData Scales:")
    for _, row in diagnostic_results.iterrows():
        print(f"  {row['metric']}: {row['count']:,.0f}")

# Query 2: Improved Provider Type Analysis with Provider-Level Aggregation
print("\n2. IMPROVED PROVIDER TYPE ANALYSIS (PROVIDER-WEIGHTED)")
print("-"*60)

improved_provider_type_query = """
-- Step 1: Deduplicate provider types (take first role alphabetically if multiple)
WITH provider_types_dedup AS (
    SELECT 
        CAST(NPI AS STRING) as NPI,
        -- Take the first role alphabetically if there are multiple
        MIN(ROLE_NAME) as ROLE_NAME
    FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW`
    GROUP BY NPI
),
provider_types AS (
    SELECT 
        NPI,
        CASE 
            WHEN ROLE_NAME IN ('Physician', 'Hospitalist') THEN 'Physician'
            WHEN ROLE_NAME = 'Physician Assistant' THEN 'Physician Assistant'
            WHEN ROLE_NAME IN ('Nurse Practitioner', 'Certified Nurse Midwife', 
                               'Certified Clinical Nurse Specialist') THEN 'Nurse Practitioner'
            WHEN ROLE_NAME IN ('Certified Registered Nurse Anesthetist', 
                               'Anesthesiology Assistant') THEN 'Other Advanced Practice'
            WHEN ROLE_NAME IN ('Dentist', 'Podiatrist', 'Optometrist', 'Chiropractor') THEN 'Other Prescriber'
            ELSE 'Allied Health/Other'
        END as provider_type_grouped
    FROM provider_types_dedup
),
-- Step 2: Aggregate to provider level first (one row per NPI)
provider_level AS (
    SELECT
        cs.NPI,
        pt.provider_type_grouped,
        
        -- Check if provider has any rx_op data in 2021-2022
        MAX(CASE WHEN rx.year IN (2021, 2022) THEN 1 ELSE 0 END) as has_rx_data_2021_2022,
        
        -- Check if provider was paid in 2021-2022
        MAX(CASE WHEN rx.year IN (2021, 2022) AND rx.TotalDollarsFrom > 0 THEN 1 ELSE 0 END) as was_paid_2021_2022,
        
        -- Average attribution for this provider when paid (across their years)
        AVG(CASE WHEN rx.year IN (2021, 2022) AND rx.TotalDollarsFrom > 0 THEN rx.attributable_pct END) as provider_avg_attribution_when_paid,
        
        -- Total payments and attributable for this provider
        SUM(CASE WHEN rx.year IN (2021, 2022) THEN rx.TotalDollarsFrom END) as provider_total_payments,
        SUM(CASE WHEN rx.year IN (2021, 2022) THEN rx.attributable_dollars END) as provider_total_attributable
        
    FROM `data-analytics-389803.temp.corewell_provider_npis` cs
    LEFT JOIN provider_types pt
        ON cs.NPI = pt.NPI
    LEFT JOIN `data-analytics-389803.conflixis_agent.rx_op_enhanced_full` rx
        ON cs.NPI = rx.NPI
    WHERE pt.provider_type_grouped IN ('Physician', 'Physician Assistant', 'Nurse Practitioner')
    GROUP BY cs.NPI, pt.provider_type_grouped
),
-- Step 3: Now aggregate by provider type
provider_type_summary AS (
    SELECT
        provider_type_grouped,
        
        -- Total providers in Corewell
        COUNT(DISTINCT NPI) as total_providers,
        
        -- Providers with rx_op data in 2021-2022
        SUM(has_rx_data_2021_2022) as providers_with_rx_data,
        
        -- Providers who were paid in 2021-2022
        SUM(was_paid_2021_2022) as paid_providers,
        
        -- PROVIDER-WEIGHTED average attribution (averaging across providers, not rows)
        AVG(provider_avg_attribution_when_paid) * 100 as avg_attribution_pct_when_paid,
        
        -- Financial totals
        SUM(provider_total_payments) as total_payments,
        SUM(provider_total_attributable) as total_attributable,
        
        -- ROI with proper NULL handling
        CASE 
            WHEN SUM(provider_total_payments) > 0 THEN
                SUM(provider_total_attributable) / SUM(provider_total_payments)
            ELSE NULL
        END as roi
        
    FROM provider_level
    GROUP BY provider_type_grouped
)
SELECT * FROM provider_type_summary
ORDER BY provider_type_grouped
"""

improved_results = client.query(improved_provider_type_query).to_dataframe()
if not improved_results.empty:
    print("\nImproved Provider Type Analysis (Provider-Weighted):")
    for _, row in improved_results.iterrows():
        print(f"\n{row['provider_type_grouped']}:")
        print(f"  Total providers: {row['total_providers']:,.0f}")
        print(f"  With rx_op data (2021-2022): {row['providers_with_rx_data']:,.0f}")
        print(f"  Paid providers (2021-2022): {row['paid_providers']:,.0f}")
        print(f"  Attribution when paid (provider-weighted): {row['avg_attribution_pct_when_paid']:.2f}%")
        print(f"  Total payments: ${row['total_payments']/1000000:.2f}M")
        print(f"  Attributable: ${row['total_attributable']/1000000:.2f}M")
        if pd.notna(row['roi']):
            print(f"  ROI: {row['roi']:.2f}x")
        else:
            print(f"  ROI: N/A (no payments)")

# Query 3: Compare with Original Row-Weighted Results
print("\n3. COMPARISON: ORIGINAL (ROW-WEIGHTED) VS IMPROVED (PROVIDER-WEIGHTED)")
print("-"*60)

# Run the original query for comparison
original_provider_type_query = """
WITH provider_types AS (
    SELECT 
        CAST(NPI AS STRING) as NPI,
        CASE 
            WHEN ROLE_NAME IN ('Physician', 'Hospitalist') THEN 'Physician'
            WHEN ROLE_NAME = 'Physician Assistant' THEN 'Physician Assistant'
            WHEN ROLE_NAME IN ('Nurse Practitioner', 'Certified Nurse Midwife', 
                               'Certified Clinical Nurse Specialist') THEN 'Nurse Practitioner'
            ELSE 'Other'
        END as provider_type_grouped
    FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW`
),
analysis AS (
    SELECT
        pt.provider_type_grouped,
        -- Row-weighted average (original method)
        AVG(CASE WHEN rx.TotalDollarsFrom > 0 AND rx.year IN (2021,2022) THEN rx.attributable_pct END) * 100 as avg_attribution_pct_when_paid
    FROM `data-analytics-389803.temp.corewell_provider_npis` cs
    LEFT JOIN `data-analytics-389803.conflixis_agent.rx_op_enhanced_full` rx
        ON cs.NPI = rx.NPI
    LEFT JOIN provider_types pt
        ON cs.NPI = pt.NPI
    WHERE pt.provider_type_grouped IN ('Physician', 'Physician Assistant', 'Nurse Practitioner')
    GROUP BY pt.provider_type_grouped
)
SELECT * FROM analysis
ORDER BY provider_type_grouped
"""

original_results = client.query(original_provider_type_query).to_dataframe()

print("\nAttribution Rate Comparison:")
print("Provider Type          | Original (Row-Weighted) | Improved (Provider-Weighted) | Difference")
print("-" * 90)

for provider_type in ['Nurse Practitioner', 'Physician', 'Physician Assistant']:
    orig = original_results[original_results['provider_type_grouped'] == provider_type]
    impr = improved_results[improved_results['provider_type_grouped'] == provider_type]
    
    if not orig.empty and not impr.empty:
        orig_val = orig.iloc[0]['avg_attribution_pct_when_paid']
        impr_val = impr.iloc[0]['avg_attribution_pct_when_paid']
        diff = impr_val - orig_val
        print(f"{provider_type:20s} | {orig_val:23.2f}% | {impr_val:28.2f}% | {diff:+.2f}%")

# Query 4: Sample of high-attribution providers to verify calculation
print("\n4. SAMPLE HIGH-ATTRIBUTION PROVIDERS (VERIFICATION)")
print("-"*60)

sample_query = """
WITH provider_types AS (
    SELECT 
        CAST(NPI AS STRING) as NPI,
        MIN(ROLE_NAME) as ROLE_NAME  -- Deduplicate
    FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW`
    GROUP BY NPI
),
provider_detail AS (
    SELECT 
        cs.NPI,
        cs.Full_Name,
        cs.Primary_Specialty,
        CASE 
            WHEN pt.ROLE_NAME = 'Physician Assistant' THEN 'Physician Assistant'
            WHEN pt.ROLE_NAME IN ('Nurse Practitioner', 'Certified Nurse Midwife') THEN 'Nurse Practitioner'
            WHEN pt.ROLE_NAME IN ('Physician', 'Hospitalist') THEN 'Physician'
            ELSE 'Other'
        END as provider_type,
        COUNT(DISTINCT rx.year) as years_with_data,
        COUNT(*) as total_records,
        SUM(rx.TotalDollarsFrom) as total_payments,
        SUM(rx.attributable_dollars) as total_attributable,
        AVG(CASE WHEN rx.TotalDollarsFrom > 0 THEN rx.attributable_pct END) * 100 as avg_attribution_when_paid
    FROM `data-analytics-389803.temp.corewell_provider_npis` cs
    INNER JOIN `data-analytics-389803.conflixis_agent.rx_op_enhanced_full` rx
        ON cs.NPI = rx.NPI
    LEFT JOIN provider_types pt
        ON cs.NPI = pt.NPI
    WHERE rx.year IN (2021, 2022)
        AND rx.TotalDollarsFrom > 0
        AND pt.ROLE_NAME IN ('Physician Assistant', 'Nurse Practitioner', 'Physician', 'Hospitalist')
    GROUP BY cs.NPI, cs.Full_Name, cs.Primary_Specialty, provider_type
    HAVING AVG(CASE WHEN rx.TotalDollarsFrom > 0 THEN rx.attributable_pct END) > 0.10
    ORDER BY avg_attribution_when_paid DESC
    LIMIT 5
)
SELECT * FROM provider_detail
"""

sample_results = client.query(sample_query).to_dataframe()
if not sample_results.empty:
    print("\nTop 5 High-Attribution Providers by Type:")
    for _, row in sample_results.iterrows():
        print(f"\n{row['Full_Name']} (NPI: {row['NPI']})")
        print(f"  Type: {row['provider_type']}")
        print(f"  Specialty: {row['Primary_Specialty']}")
        print(f"  Attribution: {row['avg_attribution_when_paid']:.2f}%")
        print(f"  Payments: ${row['total_payments']:,.2f}")
        print(f"  Records: {row['total_records']}")

print("\n" + "="*80)
print("IMPROVED QC VERIFICATION COMPLETE")
print("Key Findings:")
print("- Provider-weighted averaging shows different attribution rates than row-weighted")
print("- Deduplication ensures each NPI counted only once")
print("- Year windows now properly aligned for all metrics")
print("="*80)