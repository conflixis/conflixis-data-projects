#!/usr/bin/env python3
"""
Examine sample data from rx_op_enhanced to understand structure
"""

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from pathlib import Path
from dotenv import load_dotenv

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
print("EXAMINING RX_OP_ENHANCED DATA STRUCTURE")
print("="*80)

# Query 1: Sample of actual data for high-attribution Corewell providers
sample_query = """
SELECT 
    rx.NPI,
    rx.year,
    rx.month,
    rx.source_manufacturer,
    rx.source_specialty,
    rx.TotalDollarsFrom,
    rx.totalNext6,
    rx.attributable_dollars,
    rx.attributable_pct,
    rx.pred_rx,
    rx.pred_rx_cf,
    rx.delta_rx,
    rx.mfg_avg_lag6,
    rx.mfg_avg_lead6,
    rx.op_lag6
FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full` rx
WHERE rx.NPI IN ('1225018781', '1407833627')  -- Dr. Lerner and Dr. Wang
    AND rx.year IN (2021, 2022)
ORDER BY rx.NPI, rx.year, rx.month, rx.source_manufacturer
LIMIT 20
"""

print("\n1. SAMPLE DATA FOR HIGH-ATTRIBUTION PROVIDERS")
print("-"*60)

sample_results = client.query(sample_query).to_dataframe()
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 30)

print("\nSample data:")
print(sample_results.to_string())

# Query 2: Check unique values in key fields
unique_values_query = """
WITH value_counts AS (
    SELECT 
        -- Count distinct values in key fields
        COUNT(DISTINCT source_manufacturer) as distinct_manufacturers,
        COUNT(DISTINCT source_specialty) as distinct_specialties,
        COUNT(DISTINCT year) as distinct_years,
        MIN(year) as min_year,
        MAX(year) as max_year,
        COUNT(*) as total_rows,
        COUNT(DISTINCT NPI) as distinct_npis,
        
        -- Check range of attribution values
        MIN(attributable_pct) as min_attribution,
        MAX(attributable_pct) as max_attribution,
        AVG(attributable_pct) as avg_attribution,
        
        -- Check payment ranges
        MIN(TotalDollarsFrom) as min_payment,
        MAX(TotalDollarsFrom) as max_payment,
        AVG(CASE WHEN TotalDollarsFrom > 0 THEN TotalDollarsFrom END) as avg_payment_when_paid
        
    FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
    WHERE NPI IN (SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`)
)
SELECT * FROM value_counts
"""

print("\n2. DATA STRUCTURE SUMMARY FOR COREWELL PROVIDERS")
print("-"*60)

structure_results = client.query(unique_values_query).to_dataframe()
for col in structure_results.columns:
    print(f"{col}: {structure_results[col].iloc[0]}")

# Query 3: How many records per provider-manufacturer-year combination?
granularity_query = """
SELECT 
    COUNT(*) as record_count,
    COUNT(DISTINCT NPI) as provider_count,
    COUNT(DISTINCT CONCAT(NPI, '-', source_manufacturer, '-', CAST(year AS STRING))) as unique_combinations
FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
WHERE NPI IN (SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`)
    AND year IN (2021, 2022)
"""

print("\n3. DATA GRANULARITY CHECK")
print("-"*60)

gran_results = client.query(granularity_query).to_dataframe()
print(f"Total records: {gran_results['record_count'].iloc[0]:,}")
print(f"Unique providers: {gran_results['provider_count'].iloc[0]:,}")
print(f"Unique NPI-Manufacturer-Year combinations: {gran_results['unique_combinations'].iloc[0]:,}")

# Query 4: Check if there are multiple records per provider per time period
duplicates_query = """
WITH provider_records AS (
    SELECT 
        NPI,
        year,
        month,
        source_manufacturer,
        COUNT(*) as record_count
    FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
    WHERE NPI IN (SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`)
        AND year IN (2021, 2022)
    GROUP BY NPI, year, month, source_manufacturer
    HAVING COUNT(*) > 1
)
SELECT 
    COUNT(*) as duplicate_combinations,
    MAX(record_count) as max_duplicates,
    AVG(record_count) as avg_duplicates
FROM provider_records
"""

print("\n4. CHECKING FOR DUPLICATE RECORDS")
print("-"*60)

dup_results = client.query(duplicates_query).to_dataframe()
if dup_results['duplicate_combinations'].iloc[0] > 0:
    print(f"Found {dup_results['duplicate_combinations'].iloc[0]:,} duplicate combinations")
    print(f"Max duplicates: {dup_results['max_duplicates'].iloc[0]}")
    print(f"Avg duplicates: {dup_results['avg_duplicates'].iloc[0]:.2f}")
else:
    print("No duplicate records found (good - data is at NPI-Year-Month-Manufacturer granularity)")

print("\n" + "="*80)
print("DATA STRUCTURE EXAMINATION COMPLETE")
print("="*80)