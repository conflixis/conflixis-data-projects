#!/usr/bin/env python3
"""
Detailed analysis of disclosures with missing categories
"""

import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv('/home/incent/conflixis-data-projects/.env')

# Load credentials
service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
client = bigquery.Client(credentials=credentials, project='conflixis-engine')

print('DETAILED CATEGORY ANALYSIS')
print('='*80)

group_id = 'gcO9AHYlNSzFeGTRSFRa'

# 1. First, let's see what disclosure types have no category
no_category_query = f"""
SELECT 
    JSON_VALUE(data, '$.question.title') as disclosure_type,
    COUNT(*) as count,
    COUNT(DISTINCT JSON_VALUE(data, '$.reporter.name')) as unique_reporters,
    -- Sample some document IDs
    STRING_AGG(document_id, ', ' LIMIT 3) as sample_doc_ids,
    -- Check campaign
    JSON_VALUE(data, '$.campaign_id') as campaign_id,
    JSON_VALUE(data, '$.campaign_title') as campaign_title
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
    AND JSON_VALUE(data, '$.question.category_label') IS NULL
GROUP BY disclosure_type, campaign_id, campaign_title
ORDER BY count DESC
"""

print("\n1. Disclosure Types WITH NO CATEGORY (The 35.6% mystery):")
print("-"*80)
no_cat_df = client.query(no_category_query).to_dataframe()

total_no_category = no_cat_df['count'].sum()
print(f"Total disclosures without category: {total_no_category}")
print()

for _, row in no_cat_df.iterrows():
    dtype = row['disclosure_type'] if row['disclosure_type'] else '(No Type)'
    print(f"📍 {dtype}:")
    print(f"   Count: {row['count']}")
    print(f"   Campaign: {row['campaign_title'] if row['campaign_title'] else row['campaign_id']}")
    print(f"   Sample docs: {row['sample_doc_ids'][:100]}...")

# 2. Check if "Related Party" vs "Related Parties" is the issue
related_analysis_query = f"""
SELECT 
    JSON_VALUE(data, '$.question.title') as disclosure_type,
    JSON_VALUE(data, '$.question.category_label') as category_label,
    COUNT(*) as count,
    JSON_VALUE(data, '$.campaign_id') as campaign_id,
    MIN(CAST(JSON_VALUE(data, '$.created_at._seconds') AS INT64)) as earliest_timestamp,
    MAX(CAST(JSON_VALUE(data, '$.created_at._seconds') AS INT64)) as latest_timestamp
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
    AND JSON_VALUE(data, '$.question.title') IN ('Related Party', 'Related Parties')
GROUP BY disclosure_type, category_label, campaign_id
ORDER BY disclosure_type, count DESC
"""

print("\n2. 'Related Party' vs 'Related Parties' Analysis:")
print("-"*80)
related_df = client.query(related_analysis_query).to_dataframe()

for _, row in related_df.iterrows():
    print(f"\n• {row['disclosure_type']}:")
    print(f"  Category: {row['category_label'] if row['category_label'] else '❌ NO CATEGORY'}")
    print(f"  Count: {row['count']}")
    print(f"  Campaign: {row['campaign_id']}")
    if row['earliest_timestamp'] and row['latest_timestamp']:
        from datetime import datetime
        earliest = datetime.fromtimestamp(row['earliest_timestamp'])
        latest = datetime.fromtimestamp(row['latest_timestamp'])
        print(f"  Date range: {earliest.date()} to {latest.date()}")

# 3. Let's look at specific examples of "Related Party" with no category
sample_no_cat_query = f"""
SELECT 
    document_id,
    JSON_VALUE(data, '$.reporter.name') as reporter_name,
    JSON_VALUE(data, '$.question.title') as disclosure_type,
    JSON_VALUE(data, '$.question.category_label') as category_label,
    JSON_VALUE(data, '$.campaign_id') as campaign_id,
    JSON_VALUE(data, '$.campaign_title') as campaign_title,
    JSON_VALUE(data, '$.created_at._seconds') as created_seconds,
    -- Get the question structure
    JSON_QUERY(data, '$.question') as question_json
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
    AND JSON_VALUE(data, '$.question.category_label') IS NULL
    AND JSON_VALUE(data, '$.question.title') = 'Related Party'
LIMIT 3
"""

print("\n3. Sample 'Related Party' Records WITHOUT Category:")
print("-"*80)
sample_df = client.query(sample_no_cat_query).to_dataframe()

for _, row in sample_df.iterrows():
    print(f"\n📄 Document: {row['document_id']}")
    print(f"   Reporter: {row['reporter_name']}")
    print(f"   Campaign: {row['campaign_title']}")
    if row['created_seconds']:
        created = datetime.fromtimestamp(int(row['created_seconds']))
        print(f"   Created: {created}")
    
    # Parse question JSON to see structure
    if row['question_json']:
        try:
            question = json.loads(row['question_json'])
            print(f"   Question structure:")
            print(f"     - title: {question.get('title')}")
            print(f"     - category_label: {question.get('category_label', 'MISSING')}")
            print(f"     - category_id: {question.get('category_id', 'MISSING')}")
            print(f"     - type: {question.get('type')}")
            print(f"     - question_id: {question.get('question_id', 'MISSING')}")
            if 'fields' in question:
                print(f"     - fields: {len(question['fields'])} fields defined")
        except:
            print(f"   Could not parse question JSON")

# 4. Complete breakdown by category and type
complete_breakdown_query = f"""
WITH categorized AS (
    SELECT 
        COALESCE(JSON_VALUE(data, '$.question.category_label'), '❌ NO CATEGORY') as category_label,
        JSON_VALUE(data, '$.question.title') as disclosure_type,
        COUNT(*) as count,
        COUNT(DISTINCT JSON_VALUE(data, '$.reporter.name')) as unique_reporters,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percent_of_total
    FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
    WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
    GROUP BY category_label, disclosure_type
)
SELECT *
FROM categorized
ORDER BY category_label, count DESC
"""

print("\n4. COMPLETE BREAKDOWN - Category + Type with Percentages:")
print("-"*80)
breakdown_df = client.query(complete_breakdown_query).to_dataframe()

current_category = None
category_totals = {}

for _, row in breakdown_df.iterrows():
    cat = row['category_label']
    
    # Track category totals
    if cat not in category_totals:
        category_totals[cat] = {'count': 0, 'percent': 0}
    category_totals[cat]['count'] += row['count']
    category_totals[cat]['percent'] += row['percent_of_total']
    
    if cat != current_category:
        if current_category is not None:
            print(f"  📊 Category Total: {category_totals[current_category]['count']} ({category_totals[current_category]['percent']:.1f}%)")
        print(f"\n{'='*60}")
        print(f"CATEGORY: {cat}")
        print(f"{'='*60}")
        current_category = cat
    
    dtype = row['disclosure_type'] if row['disclosure_type'] else '(No Type)'
    print(f"  └─ {dtype}")
    print(f"     Count: {row['count']} ({row['percent_of_total']:.1f}% of all disclosures)")
    print(f"     Reporters: {row['unique_reporters']}")

# Print last category total
if current_category in category_totals:
    print(f"  📊 Category Total: {category_totals[current_category]['count']} ({category_totals[current_category]['percent']:.1f}%)")

# 5. Summary statistics
print("\n" + "="*80)
print("SUMMARY")
print("="*80)

summary_query = f"""
SELECT 
    COUNT(*) as total_disclosures,
    COUNT(CASE WHEN JSON_VALUE(data, '$.question.category_label') IS NULL THEN 1 END) as no_category,
    COUNT(CASE WHEN JSON_VALUE(data, '$.question.category_label') IS NOT NULL THEN 1 END) as has_category,
    ROUND(100.0 * COUNT(CASE WHEN JSON_VALUE(data, '$.question.category_label') IS NULL THEN 1 END) / COUNT(*), 2) as no_category_percent
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
"""

summary_df = client.query(summary_query).to_dataframe()
row = summary_df.iloc[0]

print(f"Total Disclosures: {row['total_disclosures']}")
print(f"With Category: {row['has_category']} ({100 - row['no_category_percent']:.1f}%)")
print(f"Without Category: {row['no_category']} ({row['no_category_percent']:.1f}%)")

print("\nKey Finding:")
print("The disclosures without categories are mostly 'Related Party' (singular)")
print("while 'Related Parties' (plural) correctly have 'External Roles and Relationships' category")
print("This appears to be a data quality issue - possibly from different campaign versions or forms")