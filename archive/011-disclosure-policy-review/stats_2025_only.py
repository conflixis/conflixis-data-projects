#!/usr/bin/env python3
"""
Analysis of disclosure types and categories for 2025 campaign only
"""

import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

# Load environment variables
load_dotenv('/home/incent/conflixis-data-projects/.env')

# Load credentials
service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
client = bigquery.Client(credentials=credentials, project='conflixis-engine')

print('2025 CAMPAIGN ONLY - DISCLOSURE ANALYSIS')
print('='*80)

group_id = 'gcO9AHYlNSzFeGTRSFRa'
campaign_id_2025 = 'qyH2ggzVV0WLkuRfem7S'  # 2025 Texas Health COI Survey - Leaders/Providers

# 1. Overall statistics for 2025 campaign
overall_query = f"""
SELECT 
    COUNT(*) as total_disclosures,
    COUNT(DISTINCT JSON_VALUE(data, '$.reporter.name')) as unique_reporters,
    COUNT(DISTINCT JSON_VALUE(data, '$.question.title')) as unique_disclosure_types,
    COUNT(DISTINCT JSON_VALUE(data, '$.question.category_label')) as unique_categories,
    JSON_VALUE(data, '$.campaign_title') as campaign_title
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
    AND JSON_VALUE(data, '$.campaign_id') = '{campaign_id_2025}'
GROUP BY campaign_title
"""

print("\n1. Overall Statistics (2025 Campaign Only):")
print("-"*80)
stats_df = client.query(overall_query).to_dataframe()
if len(stats_df) > 0:
    row = stats_df.iloc[0]
    print(f"Campaign: {row['campaign_title']}")
    print(f"Total Disclosures: {row['total_disclosures']}")
    print(f"Unique Reporters: {row['unique_reporters']}")
    print(f"Unique Disclosure Types: {row['unique_disclosure_types']}")
    print(f"Unique Categories: {row['unique_categories']}")

# 2. Category breakdown for 2025
category_query = f"""
SELECT 
    COALESCE(JSON_VALUE(data, '$.question.category_label'), '❌ NO CATEGORY') as category_label,
    COUNT(*) as count,
    COUNT(DISTINCT JSON_VALUE(data, '$.reporter.name')) as unique_reporters,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percent_of_total
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
    AND JSON_VALUE(data, '$.campaign_id') = '{campaign_id_2025}'
GROUP BY category_label
ORDER BY count DESC
"""

print("\n2. Disclosure Categories (2025 Campaign):")
print("-"*80)
cat_df = client.query(category_query).to_dataframe()
for _, row in cat_df.iterrows():
    cat = row['category_label']
    print(f"📁 {cat}:")
    print(f"   Disclosures: {row['count']} ({row['percent_of_total']:.1f}%)")
    print(f"   Unique Reporters: {row['unique_reporters']}")

# 3. Disclosure types breakdown for 2025
types_query = f"""
SELECT 
    JSON_VALUE(data, '$.question.title') as disclosure_type,
    COUNT(*) as count,
    COUNT(DISTINCT JSON_VALUE(data, '$.reporter.name')) as unique_reporters,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percent_of_total
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
    AND JSON_VALUE(data, '$.campaign_id') = '{campaign_id_2025}'
GROUP BY disclosure_type
ORDER BY count DESC
"""

print("\n3. Disclosure Types (2025 Campaign):")
print("-"*80)
types_df = client.query(types_query).to_dataframe()
for _, row in types_df.iterrows():
    dtype = row['disclosure_type'] if row['disclosure_type'] else '(No Type)'
    print(f"📋 {dtype}:")
    print(f"   Count: {row['count']} ({row['percent_of_total']:.1f}%)")
    print(f"   Reporters: {row['unique_reporters']}")

# 4. Complete breakdown by category and type for 2025
combo_query = f"""
SELECT 
    COALESCE(JSON_VALUE(data, '$.question.category_label'), '❌ NO CATEGORY') as category_label,
    JSON_VALUE(data, '$.question.title') as disclosure_type,
    COUNT(*) as count,
    COUNT(DISTINCT JSON_VALUE(data, '$.reporter.name')) as unique_reporters,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percent_of_total,
    -- Get sample company names
    STRING_AGG(DISTINCT JSON_VALUE(data, '$.company_name'), ', ' LIMIT 3) as sample_companies
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
    AND JSON_VALUE(data, '$.campaign_id') = '{campaign_id_2025}'
GROUP BY category_label, disclosure_type
ORDER BY category_label, count DESC
"""

print("\n4. Category + Disclosure Type Breakdown (2025 Campaign):")
print("-"*80)
combo_df = client.query(combo_query).to_dataframe()

current_category = None
category_totals = {}

for _, row in combo_df.iterrows():
    cat = row['category_label']
    
    # Track category totals
    if cat not in category_totals:
        category_totals[cat] = {'count': 0, 'percent': 0}
    category_totals[cat]['count'] += row['count']
    category_totals[cat]['percent'] += row['percent_of_total']
    
    if cat != current_category:
        if current_category is not None:
            print(f"\n  📊 Category Total: {category_totals[current_category]['count']} disclosures ({category_totals[current_category]['percent']:.1f}%)")
        print(f"\n{'='*70}")
        print(f"CATEGORY: {cat}")
        print(f"{'='*70}")
        current_category = cat
    
    dtype = row['disclosure_type'] if row['disclosure_type'] else '(No Type)'
    print(f"  └─ {dtype}")
    print(f"     Count: {row['count']} ({row['percent_of_total']:.1f}%)")
    print(f"     Reporters: {row['unique_reporters']}")
    if row['sample_companies'] and row['sample_companies'] != '':
        print(f"     Sample companies: {row['sample_companies'][:100]}")

# Print last category total
if current_category in category_totals:
    print(f"\n  📊 Category Total: {category_totals[current_category]['count']} disclosures ({category_totals[current_category]['percent']:.1f}%)")

# 5. Financial analysis for 2025 campaign
financial_query = f"""
SELECT 
    JSON_VALUE(data, '$.question.category_label') as category_label,
    JSON_VALUE(data, '$.question.title') as disclosure_type,
    COUNT(*) as count,
    AVG(CAST(JSON_VALUE(data, '$.compensation_value') AS FLOAT64)) as avg_amount,
    MAX(CAST(JSON_VALUE(data, '$.compensation_value') AS FLOAT64)) as max_amount,
    MIN(CAST(JSON_VALUE(data, '$.compensation_value') AS FLOAT64)) as min_amount,
    SUM(CAST(JSON_VALUE(data, '$.compensation_value') AS FLOAT64)) as total_amount
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
    AND JSON_VALUE(data, '$.campaign_id') = '{campaign_id_2025}'
    AND JSON_VALUE(data, '$.compensation_value') IS NOT NULL
    AND CAST(JSON_VALUE(data, '$.compensation_value') AS FLOAT64) > 0
GROUP BY category_label, disclosure_type
HAVING count > 0
ORDER BY total_amount DESC
LIMIT 10
"""

print("\n5. Top 10 Financial Disclosures by Total Value (2025 Campaign):")
print("-"*80)
fin_df = client.query(financial_query).to_dataframe()
if len(fin_df) > 0:
    for _, row in fin_df.iterrows():
        cat = row['category_label'] if row['category_label'] else '(No Category)'
        dtype = row['disclosure_type'] if row['disclosure_type'] else '(No Type)'
        print(f"\n{cat} > {dtype}:")
        print(f"  Count: {row['count']} disclosures")
        print(f"  Total Value: ${row['total_amount']:,.2f}")
        print(f"  Average: ${row['avg_amount']:,.2f}")
        print(f"  Range: ${row['min_amount']:,.2f} - ${row['max_amount']:,.2f}")
else:
    print("No financial disclosures with amounts > 0 found")

# 6. Data quality check for 2025
quality_query = f"""
SELECT 
    COUNT(*) as total,
    COUNT(CASE WHEN JSON_VALUE(data, '$.question.category_label') IS NULL THEN 1 END) as missing_category,
    COUNT(CASE WHEN JSON_VALUE(data, '$.question.title') IS NULL THEN 1 END) as missing_type,
    COUNT(CASE WHEN JSON_VALUE(data, '$.question.category_label') IS NULL 
               AND JSON_VALUE(data, '$.question.title') IS NULL THEN 1 END) as missing_both,
    COUNT(CASE WHEN JSON_VALUE(data, '$.company_name') IS NULL 
               AND JSON_VALUE(data, '$.question.title') NOT LIKE '%Related Part%' THEN 1 END) as missing_company
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
    AND JSON_VALUE(data, '$.campaign_id') = '{campaign_id_2025}'
"""

print("\n6. Data Quality Check (2025 Campaign):")
print("-"*80)
quality_df = client.query(quality_query).to_dataframe()
row = quality_df.iloc[0]
print(f"Total records: {row['total']}")
print(f"Missing category: {row['missing_category']} ({row['missing_category']/row['total']*100:.1f}%)")
print(f"Missing type: {row['missing_type']} ({row['missing_type']/row['total']*100:.1f}%)")
print(f"Missing both: {row['missing_both']} ({row['missing_both']/row['total']*100:.1f}%)")
print(f"Missing company (non-Related Party): {row['missing_company']}")

# 7. Sample reporters with most disclosures
top_reporters_query = f"""
SELECT 
    JSON_VALUE(data, '$.reporter.name') as reporter_name,
    COUNT(*) as disclosure_count,
    STRING_AGG(DISTINCT JSON_VALUE(data, '$.question.title'), ', ' LIMIT 5) as disclosure_types,
    SUM(CAST(JSON_VALUE(data, '$.compensation_value') AS FLOAT64)) as total_compensation
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
    AND JSON_VALUE(data, '$.campaign_id') = '{campaign_id_2025}'
GROUP BY reporter_name
HAVING disclosure_count > 1
ORDER BY disclosure_count DESC
LIMIT 10
"""

print("\n7. Top Reporters by Number of Disclosures (2025 Campaign):")
print("-"*80)
reporters_df = client.query(top_reporters_query).to_dataframe()
if len(reporters_df) > 0:
    for _, row in reporters_df.iterrows():
        print(f"\n{row['reporter_name']}:")
        print(f"  Disclosures: {row['disclosure_count']}")
        print(f"  Types: {row['disclosure_types'][:100]}...")
        if row['total_compensation'] and row['total_compensation'] > 0:
            print(f"  Total Compensation: ${row['total_compensation']:,.2f}")

# 8. Summary
print("\n" + "="*80)
print("2025 CAMPAIGN SUMMARY")
print("="*80)

summary_query = f"""
WITH stats AS (
    SELECT 
        COUNT(*) as total_disclosures,
        COUNT(DISTINCT JSON_VALUE(data, '$.reporter.name')) as unique_reporters,
        COUNT(CASE WHEN JSON_VALUE(data, '$.question.category_label') IS NOT NULL THEN 1 END) as has_category,
        COUNT(CASE WHEN JSON_VALUE(data, '$.question.category_label') IS NULL THEN 1 END) as no_category,
        SUM(CAST(JSON_VALUE(data, '$.compensation_value') AS FLOAT64)) as total_value,
        AVG(CAST(JSON_VALUE(data, '$.compensation_value') AS FLOAT64)) as avg_value
    FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
    WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
        AND JSON_VALUE(data, '$.campaign_id') = '{campaign_id_2025}'
)
SELECT *,
    ROUND(100.0 * has_category / total_disclosures, 1) as category_coverage_percent
FROM stats
"""

summary_df = client.query(summary_query).to_dataframe()
if len(summary_df) > 0:
    row = summary_df.iloc[0]
    print(f"📊 Total Disclosures: {row['total_disclosures']}")
    print(f"👥 Unique Reporters: {row['unique_reporters']}")
    print(f"✅ With Category: {row['has_category']} ({row['category_coverage_percent']:.1f}%)")
    print(f"❌ Without Category: {row['no_category']} ({100 - row['category_coverage_percent']:.1f}%)")
    if row['total_value']:
        print(f"💰 Total Disclosed Value: ${row['total_value']:,.2f}")
        print(f"💵 Average Value per Disclosure: ${row['avg_value']:,.2f}")