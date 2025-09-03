#!/usr/bin/env python3
"""
QC Verification for Corewell Health Deep Dive Analysis Report
This script verifies the numbers reported in the analysis document
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
print("COREWELL HEALTH DEEP DIVE ANALYSIS - QC VERIFICATION")
print(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# Query 1: Overall Corewell metrics
print("\n1. OVERALL METRICS VERIFICATION (2021-2022)")
print("-"*60)

overall_query = """
WITH corewell_summary AS (
    SELECT 
        -- Total NPIs
        COUNT(DISTINCT cs.NPI) as total_corewell_npis,
        
        -- NPIs with rx_op data
        COUNT(DISTINCT rx.NPI) as npis_with_rx_data,
        
        -- Paid providers in 2021-2022
        COUNT(DISTINCT CASE WHEN rx.TotalDollarsFrom > 0 AND rx.year IN (2021,2022) THEN rx.NPI END) as paid_providers_2021_2022,
        
        -- Financial metrics for 2021-2022
        SUM(CASE WHEN rx.year IN (2021,2022) THEN rx.TotalDollarsFrom END) as total_payments_2021_2022,
        SUM(CASE WHEN rx.year IN (2021,2022) THEN rx.attributable_dollars END) as total_attributable_2021_2022,
        SUM(CASE WHEN rx.year IN (2021,2022) THEN rx.totalNext6 END) as total_prescribed_2021_2022,
        
        -- ROI
        SAFE_DIVIDE(
            SUM(CASE WHEN rx.year IN (2021,2022) THEN rx.attributable_dollars END),
            SUM(CASE WHEN rx.year IN (2021,2022) THEN rx.TotalDollarsFrom END)
        ) as roi_2021_2022,
        
        -- Attribution rate when paid
        AVG(CASE WHEN rx.TotalDollarsFrom > 0 AND rx.year IN (2021,2022) THEN rx.attributable_pct END) * 100 as avg_attribution_pct_when_paid
        
    FROM `data-analytics-389803.temp.corewell_provider_npis` cs
    LEFT JOIN `data-analytics-389803.conflixis_agent.rx_op_enhanced_full` rx
        ON cs.NPI = rx.NPI
)
SELECT 
    total_corewell_npis,
    npis_with_rx_data,
    ROUND(npis_with_rx_data / total_corewell_npis * 100, 1) as pct_with_data,
    paid_providers_2021_2022,
    ROUND(paid_providers_2021_2022 / npis_with_rx_data * 100, 1) as pct_paid,
    ROUND(total_payments_2021_2022 / 1000000, 2) as payments_millions,
    ROUND(total_attributable_2021_2022 / 1000000, 2) as attributable_millions,
    ROUND(total_prescribed_2021_2022 / 1000000, 2) as prescribed_millions,
    ROUND(roi_2021_2022, 2) as roi,
    ROUND(avg_attribution_pct_when_paid, 2) as avg_attribution_pct_when_paid
FROM corewell_summary
"""

results = client.query(overall_query).to_dataframe()
if not results.empty:
    row = results.iloc[0]
    print(f"Total Corewell NPIs:          {row['total_corewell_npis']:,}")
    print(f"NPIs with rx_op data:         {row['npis_with_rx_data']:,} ({row['pct_with_data']:.1f}%)")
    print(f"Paid providers (2021-2022):   {row['paid_providers_2021_2022']:,} ({row['pct_paid']:.1f}%)")
    print(f"Total payments:               ${row['payments_millions']:.2f}M")
    print(f"Attributable prescriptions:   ${row['attributable_millions']:.2f}M")
    print(f"Total prescribed:             ${row['prescribed_millions']:.2f}M")
    print(f"ROI:                          {row['roi']:.2f}x")
    print(f"Avg attribution when paid:    {row['avg_attribution_pct_when_paid']:.2f}%")
    
    print("\nREPORT CLAIMS vs ACTUAL:")
    print(f"  Providers analyzed:  Report: 7,840 | Actual: {row['npis_with_rx_data']:,}")
    print(f"  % of total NPIs:     Report: 55.3% | Actual: {row['pct_with_data']:.1f}%")
    print(f"  Payments (2021-22):  Report: $5.37M | Actual: ${row['payments_millions']:.2f}M")
    print(f"  Attributable:        Report: $49.44M | Actual: ${row['attributable_millions']:.2f}M")
    print(f"  ROI:                 Report: 9.20x | Actual: {row['roi']:.2f}x")
    print(f"  % paid providers:    Report: 43.6% | Actual: {row['pct_paid']:.1f}%")
    print(f"  Attribution when paid: Report: 2.35% | Actual: {row['avg_attribution_pct_when_paid']:.2f}%")

# Query 2: Provider type analysis
print("\n2. PROVIDER TYPE ATTRIBUTION ANALYSIS")
print("-"*60)

provider_type_query = """
WITH provider_types AS (
    SELECT 
        CAST(NPI AS STRING) as NPI,
        CASE 
            WHEN ROLE_NAME IN ('Physician', 'Hospitalist') THEN 'Physician'
            WHEN ROLE_NAME = 'Physician Assistant' THEN 'Physician Assistant'
            WHEN ROLE_NAME IN ('Nurse Practitioner', 'Certified Nurse Midwife', 
                               'Certified Clinical Nurse Specialist') THEN 'Nurse Practitioner'
            WHEN ROLE_NAME IN ('Certified Registered Nurse Anesthetist', 
                               'Anesthesiology Assistant') THEN 'Other Advanced Practice'
            WHEN ROLE_NAME IN ('Dentist', 'Podiatrist', 'Optometrist', 'Chiropractor') THEN 'Other Prescriber'
            ELSE 'Allied Health/Other'
        END as provider_type_grouped,
        ROLE_NAME as original_role
    FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW`
),
analysis AS (
    SELECT
        pt.provider_type_grouped,
        COUNT(DISTINCT cs.NPI) as corewell_providers,
        COUNT(DISTINCT CASE WHEN rx.TotalDollarsFrom > 0 AND rx.year IN (2021,2022) THEN cs.NPI END) as paid_providers,
        
        -- Attribution metrics for 2021-2022
        AVG(CASE WHEN rx.TotalDollarsFrom > 0 AND rx.year IN (2021,2022) THEN rx.attributable_pct END) * 100 as avg_attribution_pct_when_paid,
        
        -- Financial metrics
        SUM(CASE WHEN rx.year IN (2021,2022) THEN rx.TotalDollarsFrom END) as total_payments,
        SUM(CASE WHEN rx.year IN (2021,2022) THEN rx.attributable_dollars END) as total_attributable,
        
        -- ROI
        SAFE_DIVIDE(
            SUM(CASE WHEN rx.year IN (2021,2022) THEN rx.attributable_dollars END),
            SUM(CASE WHEN rx.year IN (2021,2022) THEN rx.TotalDollarsFrom END)
        ) as roi
        
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

provider_results = client.query(provider_type_query).to_dataframe()
if not provider_results.empty:
    print("\nProvider Type Analysis:")
    for _, row in provider_results.iterrows():
        print(f"\n{row['provider_type_grouped']}:")
        print(f"  Providers: {row['corewell_providers']:,}")
        print(f"  Paid providers: {row['paid_providers']:,}")
        print(f"  Attribution when paid: {row['avg_attribution_pct_when_paid']:.2f}%")
        print(f"  Total payments: ${row['total_payments']/1000000:.2f}M")
        print(f"  Attributable: ${row['total_attributable']/1000000:.2f}M")
        print(f"  ROI: {row['roi']:.2f}x")
    
    print("\nREPORT CLAIMS vs ACTUAL:")
    pa_data = provider_results[provider_results['provider_type_grouped'] == 'Physician Assistant']
    np_data = provider_results[provider_results['provider_type_grouped'] == 'Nurse Practitioner']
    phys_data = provider_results[provider_results['provider_type_grouped'] == 'Physician']
    
    if not pa_data.empty:
        print(f"\nPhysician Assistant:")
        print(f"  Attribution: Report: 5.03% | Actual: {pa_data.iloc[0]['avg_attribution_pct_when_paid']:.2f}%")
        print(f"  ROI:         Report: 71.8x | Actual: {pa_data.iloc[0]['roi']:.2f}x")
    
    if not np_data.empty:
        print(f"\nNurse Practitioner:")
        print(f"  Attribution: Report: 2.11% | Actual: {np_data.iloc[0]['avg_attribution_pct_when_paid']:.2f}%")
    
    if not phys_data.empty:
        print(f"\nPhysician:")
        print(f"  Attribution: Report: 1.68% | Actual: {phys_data.iloc[0]['avg_attribution_pct_when_paid']:.2f}%")

# Query 3: High-risk providers
print("\n3. HIGH-RISK PROVIDER VERIFICATION")
print("-"*60)

high_risk_query = """
WITH provider_risk AS (
    SELECT 
        cs.NPI,
        cs.Full_Name,
        cs.Primary_Specialty,
        po.ROLE_NAME,
        COUNT(DISTINCT rx.source_manufacturer) as num_manufacturers,
        SUM(rx.TotalDollarsFrom) as total_payments,
        SUM(rx.attributable_dollars) as total_attributable,
        AVG(rx.attributable_pct) as avg_attribution_pct,
        MAX(rx.attributable_pct) as max_attribution_pct
    FROM `data-analytics-389803.temp.corewell_provider_npis` cs
    INNER JOIN `data-analytics-389803.conflixis_agent.rx_op_enhanced_full` rx
        ON cs.NPI = rx.NPI
    LEFT JOIN `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW` po
        ON CAST(po.NPI AS STRING) = cs.NPI
    WHERE rx.year IN (2021, 2022) 
        AND rx.TotalDollarsFrom > 0
    GROUP BY cs.NPI, cs.Full_Name, cs.Primary_Specialty, po.ROLE_NAME
    HAVING AVG(rx.attributable_pct) > 0.30
)
SELECT 
    COUNT(*) as high_risk_count,
    MAX(avg_attribution_pct) as max_attribution,
    MIN(total_payments) as min_payments,
    MAX(total_payments) as max_payments
FROM provider_risk
"""

high_risk_results = client.query(high_risk_query).to_dataframe()
if not high_risk_results.empty:
    row = high_risk_results.iloc[0]
    print(f"High-risk providers (>30% attribution): {row['high_risk_count']}")
    print(f"Maximum attribution rate: {row['max_attribution']*100:.1f}%")
    print(f"Payment range: ${row['min_payments']:.2f} - ${row['max_payments']:,.2f}")
    
    print(f"\nREPORT CLAIM vs ACTUAL:")
    print(f"  High-risk count: Report: 23 | Actual: {row['high_risk_count']}")

# Query 4: Dr. Sandra Lerner verification
print("\n4. DR. SANDRA LERNER CASE VERIFICATION")
print("-"*60)

lerner_query = """
SELECT 
    cs.Full_Name,
    cs.NPI,
    cs.Primary_Specialty,
    COUNT(*) as observations,
    SUM(rx.TotalDollarsFrom) as total_payments,
    AVG(rx.attributable_pct) * 100 as avg_attribution_pct,
    MAX(rx.attributable_pct) * 100 as max_attribution_pct
FROM `data-analytics-389803.temp.corewell_provider_npis` cs
INNER JOIN `data-analytics-389803.conflixis_agent.rx_op_enhanced_full` rx
    ON cs.NPI = rx.NPI
WHERE LOWER(cs.Full_Name) LIKE '%sandra%lerner%'
    AND rx.year IN (2021, 2022)
    AND rx.TotalDollarsFrom > 0
GROUP BY cs.Full_Name, cs.NPI, cs.Primary_Specialty
ORDER BY avg_attribution_pct DESC
"""

lerner_results = client.query(lerner_query).to_dataframe()
if not lerner_results.empty:
    print("Dr. Sandra Lerner found:")
    for _, row in lerner_results.iterrows():
        print(f"  Name: {row['Full_Name']}")
        print(f"  NPI: {row['NPI']}")
        print(f"  Specialty: {row['Primary_Specialty']}")
        print(f"  Total payments: ${row['total_payments']:.2f}")
        print(f"  Avg attribution: {row['avg_attribution_pct']:.1f}%")
        print(f"  Max attribution: {row['max_attribution_pct']:.1f}%")
        
        print(f"\nREPORT CLAIM vs ACTUAL:")
        print(f"  Attribution: Report: 90.4% | Actual: {row['avg_attribution_pct']:.1f}%")
        print(f"  Payments:    Report: $630   | Actual: ${row['total_payments']:.2f}")
else:
    print("No provider found with name matching 'Sandra Lerner'")

print("\n" + "="*80)
print("QC VERIFICATION COMPLETE")
print("="*80)