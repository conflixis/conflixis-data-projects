#!/usr/bin/env python3
"""
QC Verification for Corewell Health Open Payments Report
Part 1: Open Payments Metrics Verification
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
print("COREWELL HEALTH OPEN PAYMENTS REPORT - QC VERIFICATION")
print(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# Query 1: Overall metrics (2020-2024)
print("\n1. OVERALL PAYMENT METRICS VERIFICATION (2020-2024)")
print("-"*60)

overall_query = """
WITH corewell_payments AS (
    SELECT 
        op.covered_recipient_npi AS NPI,
        op.program_year,
        op.total_amount_of_payment_usdollars as payment_amount,
        op.nature_of_payment_or_transfer_of_value as payment_type,
        op.applicable_manufacturer_or_applicable_gpo_making_payment_name AS manufacturer,
        cs.NPI as corewell_npi
    FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` op
    INNER JOIN `data-analytics-389803.temp.corewell_provider_npis` cs
        ON CAST(op.covered_recipient_npi AS STRING) = cs.NPI
    WHERE op.program_year BETWEEN 2020 AND 2024
),
summary_stats AS (
    SELECT 
        COUNT(DISTINCT NPI) as unique_providers,
        COUNT(*) as total_transactions,
        SUM(payment_amount) as total_payments,
        AVG(payment_amount) as avg_payment,
        MAX(payment_amount) as max_payment,
        MIN(payment_amount) as min_payment
    FROM corewell_payments
),
provider_coverage AS (
    SELECT COUNT(DISTINCT NPI) as total_corewell_providers
    FROM `data-analytics-389803.temp.corewell_provider_npis`
)
SELECT 
    s.*,
    p.total_corewell_providers,
    ROUND(s.unique_providers / p.total_corewell_providers * 100, 1) as pct_providers_with_payments
FROM summary_stats s
CROSS JOIN provider_coverage p
"""

overall_results = client.query(overall_query).to_dataframe()
if not overall_results.empty:
    row = overall_results.iloc[0]
    print(f"Unique providers receiving payments: {row['unique_providers']:,}")
    print(f"Total Corewell providers: {row['total_corewell_providers']:,}")
    print(f"Percentage with payments: {row['pct_providers_with_payments']:.1f}%")
    print(f"Total transactions: {row['total_transactions']:,}")
    print(f"Total payments: ${row['total_payments']:,.2f}")
    print(f"Average payment: ${row['avg_payment']:.2f}")
    print(f"Maximum payment: ${row['max_payment']:,.2f}")
    
    print("\nREPORT CLAIMS vs ACTUAL:")
    print(f"  Unique providers: Report: 10,424 | Actual: {row['unique_providers']:,}")
    print(f"  % of providers: Report: 73.5% | Actual: {row['pct_providers_with_payments']:.1f}%")
    print(f"  Transactions: Report: 638,567 | Actual: {row['total_transactions']:,}")
    print(f"  Total payments: Report: $86,873,248 | Actual: ${row['total_payments']:,.2f}")
    print(f"  Avg payment: Report: $136.04 | Actual: ${row['avg_payment']:.2f}")
    print(f"  Max payment: Report: $2,407,380 | Actual: ${row['max_payment']:,.2f}")

# Query 2: Yearly payment trends
print("\n2. YEARLY PAYMENT TRENDS VERIFICATION")
print("-"*60)

yearly_query = """
WITH corewell_payments AS (
    SELECT 
        op.covered_recipient_npi AS NPI,
        op.program_year,
        op.total_amount_of_payment_usdollars as payment_amount
    FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` op
    INNER JOIN `data-analytics-389803.temp.corewell_provider_npis` cs
        ON CAST(op.covered_recipient_npi AS STRING) = cs.NPI
    WHERE op.program_year BETWEEN 2020 AND 2024
)
SELECT 
    program_year,
    COUNT(DISTINCT NPI) as unique_providers,
    COUNT(*) as transactions,
    SUM(payment_amount) as total_payments,
    LAG(SUM(payment_amount)) OVER (ORDER BY program_year) as prev_year_payments,
    ROUND((SUM(payment_amount) - LAG(SUM(payment_amount)) OVER (ORDER BY program_year)) / 
          LAG(SUM(payment_amount)) OVER (ORDER BY program_year) * 100, 1) as yoy_growth
FROM corewell_payments
GROUP BY program_year
ORDER BY program_year
"""

yearly_results = client.query(yearly_query).to_dataframe()
if not yearly_results.empty:
    print("\nYear-by-Year Analysis:")
    print("Year | Providers | Total Payments | YoY Growth | Report Claims")
    print("-" * 75)
    
    report_claims = {
        2020: {'providers': 3406, 'payments': 8958909},
        2021: {'providers': 5633, 'payments': 16421338, 'growth': 83.3},
        2022: {'providers': 6679, 'payments': 19001521, 'growth': 15.7},
        2023: {'providers': 7464, 'payments': 20360792, 'growth': 7.1},
        2024: {'providers': 8027, 'payments': 22153114, 'growth': 8.8}
    }
    
    for _, row in yearly_results.iterrows():
        year = int(row['program_year'])
        growth = f"{row['yoy_growth']:.1f}%" if pd.notna(row['yoy_growth']) else "N/A"
        report_growth = f"{report_claims[year].get('growth', 0):.1f}%" if year > 2020 else "N/A"
        
        print(f"{year} | {row['unique_providers']:,} | ${row['total_payments']:,.0f} | {growth} | " +
              f"Report: {report_claims[year]['providers']:,} providers, ${report_claims[year]['payments']:,}, {report_growth}")

# Query 3: Payment categories
print("\n3. PAYMENT CATEGORIES VERIFICATION")
print("-"*60)

categories_query = """
WITH corewell_payments AS (
    SELECT 
        op.nature_of_payment_or_transfer_of_value as payment_type,
        op.total_amount_of_payment_usdollars as payment_amount
    FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` op
    INNER JOIN `data-analytics-389803.temp.corewell_provider_npis` cs
        ON CAST(op.covered_recipient_npi AS STRING) = cs.NPI
    WHERE op.program_year BETWEEN 2020 AND 2024
        AND op.nature_of_payment_or_transfer_of_value IS NOT NULL
),
category_totals AS (
    SELECT 
        payment_type,
        COUNT(*) as transactions,
        SUM(payment_amount) as total_amount,
        SUM(SUM(payment_amount)) OVER () as grand_total
    FROM corewell_payments
    GROUP BY payment_type
)
SELECT 
    payment_type,
    transactions,
    total_amount,
    ROUND(total_amount / grand_total * 100, 1) as pct_of_total
FROM category_totals
ORDER BY total_amount DESC
LIMIT 10
"""

categories_results = client.query(categories_query).to_dataframe()
if not categories_results.empty:
    print("\nTop Payment Categories:")
    print("Category | Amount | % of Total")
    print("-" * 60)
    
    report_categories = {
        'Compensation for services other than consulting': {'amount': 29848798, 'pct': 34.4},
        'Consulting Fee': {'amount': 16140824, 'pct': 18.6},
        'Food and Beverage': {'amount': 14764299, 'pct': 17.0},
        'Royalty or License': {'amount': 7101472, 'pct': 8.2},
        'Travel and Lodging': {'amount': 6190776, 'pct': 7.1}
    }
    
    for _, row in categories_results.head(5).iterrows():
        print(f"{row['payment_type'][:40]} | ${row['total_amount']:,.0f} | {row['pct_of_total']:.1f}%")

# Query 4: Top manufacturers
print("\n4. TOP MANUFACTURERS VERIFICATION")
print("-"*60)

manufacturers_query = """
WITH corewell_payments AS (
    SELECT 
        op.applicable_manufacturer_or_applicable_gpo_making_payment_name AS manufacturer,
        op.total_amount_of_payment_usdollars as payment_amount,
        op.covered_recipient_npi AS NPI
    FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` op
    INNER JOIN `data-analytics-389803.temp.corewell_provider_npis` cs
        ON CAST(op.covered_recipient_npi AS STRING) = cs.NPI
    WHERE op.program_year BETWEEN 2020 AND 2024
        AND op.applicable_manufacturer_or_applicable_gpo_making_payment_name IS NOT NULL
)
SELECT 
    manufacturer,
    COUNT(DISTINCT NPI) as unique_providers,
    COUNT(*) as transactions,
    SUM(payment_amount) as total_amount
FROM corewell_payments
GROUP BY manufacturer
ORDER BY total_amount DESC
LIMIT 10
"""

manufacturers_results = client.query(manufacturers_query).to_dataframe()
if not manufacturers_results.empty:
    print("\nTop 10 Manufacturers by Payment Amount:")
    print("Manufacturer | Total Payments | Providers | Transactions")
    print("-" * 70)
    
    report_top5 = {
        'Stryker Corporation': 3528403,
        'Boston Scientific Corporation': 3422336,
        'AbbVie Inc.': 3372900,
        'Amgen Inc.': 2845447,
        'Arthrex, Inc.': 2745920
    }
    
    for _, row in manufacturers_results.iterrows():
        mfr_name = row['manufacturer'][:30]
        print(f"{mfr_name:30} | ${row['total_amount']:>12,.0f} | {row['unique_providers']:>9,} | {row['transactions']:>12,}")
        
        # Check if in report's top 5
        for report_mfr, report_amount in report_top5.items():
            if report_mfr.lower() in row['manufacturer'].lower():
                print(f"  ↳ Report claim: ${report_amount:,}")
                break

print("\n" + "="*80)
print("OPEN PAYMENTS METRICS VERIFICATION COMPLETE")
print("="*80)