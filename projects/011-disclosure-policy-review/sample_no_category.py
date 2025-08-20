#!/usr/bin/env python3
"""
Get sample records with no category from 2025 campaign
"""

import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv('/home/incent/conflixis-data-projects/.env')

# Load credentials
service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
client = bigquery.Client(credentials=credentials, project='conflixis-engine')

print('SAMPLE RECORDS WITH NO CATEGORY/TYPE FROM 2025 CAMPAIGN')
print('='*80)

group_id = 'gcO9AHYlNSzFeGTRSFRa'
campaign_id_2025 = 'qyH2ggzVV0WLkuRfem7S'

# Get sample records with no category
sample_query = f"""
SELECT 
    document_id,
    JSON_VALUE(data, '$.reporter.name') as reporter_name,
    JSON_VALUE(data, '$.reporter.email') as reporter_email,
    JSON_VALUE(data, '$.company_name') as company_name,
    CAST(JSON_VALUE(data, '$.compensation_value') AS FLOAT64) as compensation_value,
    JSON_VALUE(data, '$.source') as source,
    JSON_VALUE(data, '$.status') as status,
    JSON_VALUE(data, '$.created_at._seconds') as created_seconds,
    -- Get the full data to inspect
    data as full_json
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
    AND JSON_VALUE(data, '$.campaign_id') = '{campaign_id_2025}'
    AND JSON_VALUE(data, '$.question.category_label') IS NULL
    AND JSON_VALUE(data, '$.question.title') IS NULL
LIMIT 5
"""

print("\nFetching 5 sample records with no category/type...")
print("-"*80)

df = client.query(sample_query).to_dataframe()

for idx, row in df.iterrows():
    print(f"\n{'='*70}")
    print(f"SAMPLE {idx + 1}: Document ID: {row['document_id']}")
    print(f"{'='*70}")
    
    print(f"\nBasic Info:")
    print(f"  Reporter: {row['reporter_name']} ({row['reporter_email']})")
    print(f"  Company: {row['company_name']}")
    print(f"  Compensation: ${row['compensation_value']:,.2f}" if row['compensation_value'] else "  Compensation: $0")
    print(f"  Source: {row['source']}")
    print(f"  Status: {row['status']}")
    
    if row['created_seconds']:
        created = datetime.fromtimestamp(int(row['created_seconds']))
        print(f"  Created: {created}")
    
    # Parse and inspect the full JSON
    if row['full_json']:
        try:
            data = json.loads(row['full_json'])
            
            print(f"\nJSON Structure Analysis:")
            print(f"  Top-level keys: {list(data.keys())}")
            
            # Check question field
            if 'question' in data:
                question = data['question']
                print(f"\n  Question field contents:")
                print(f"    - Keys in question: {list(question.keys()) if question else 'null/empty'}")
                if question:
                    print(f"    - title: {question.get('title', 'MISSING')}")
                    print(f"    - category_label: {question.get('category_label', 'MISSING')}")
                    print(f"    - category_id: {question.get('category_id', 'MISSING')}")
                    print(f"    - type: {question.get('type', 'MISSING')}")
                    print(f"    - description: {str(question.get('description', ''))[:100]}...")
                    
                    if 'fields' in question:
                        print(f"    - fields: {len(question['fields'])} fields defined")
                        for field in question['fields'][:3]:
                            print(f"      • {field.get('title', 'untitled')}: {field.get('type', 'unknown')}")
                    
                    if 'field_values' in question:
                        print(f"    - field_values: {len(question['field_values'])} values")
                        for i, val in enumerate(question['field_values'][:3]):
                            print(f"      • [{i}]: {str(val.get('value', ''))[:50]}")
            else:
                print(f"\n  ⚠️ No 'question' field in JSON!")
            
            # Check other interesting fields
            print(f"\n  Other relevant fields:")
            print(f"    - compensation_type: {data.get('compensation_type', 'N/A')}")
            print(f"    - service_provided: {data.get('service_provided', 'N/A')}")
            print(f"    - is_research: {data.get('is_research', 'N/A')}")
            print(f"    - disputed: {data.get('disputed', 'N/A')}")
            print(f"    - notes: {str(data.get('notes', ''))[:100]}")
            
            # Check if this might be an imported record
            if data.get('source') == 'import' or data.get('source') == 'open_payments':
                print(f"\n  📌 This appears to be an IMPORTED record (source: {data.get('source')})")
            
            # Print full JSON for first record only
            if idx == 0:
                print(f"\n  Full JSON (first record only):")
                print(json.dumps(data, indent=2, default=str))
                
        except json.JSONDecodeError as e:
            print(f"\n  ❌ Error parsing JSON: {e}")

# Get summary of companies in no-category records
company_query = f"""
SELECT 
    JSON_VALUE(data, '$.company_name') as company_name,
    COUNT(*) as count,
    AVG(CAST(JSON_VALUE(data, '$.compensation_value') AS FLOAT64)) as avg_value,
    JSON_VALUE(data, '$.source') as source
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
    AND JSON_VALUE(data, '$.campaign_id') = '{campaign_id_2025}'
    AND JSON_VALUE(data, '$.question.category_label') IS NULL
    AND JSON_VALUE(data, '$.question.title') IS NULL
    AND JSON_VALUE(data, '$.company_name') IS NOT NULL
GROUP BY company_name, source
ORDER BY count DESC
LIMIT 10
"""

print(f"\n{'='*80}")
print("TOP COMPANIES IN NO-CATEGORY RECORDS")
print('='*80)

company_df = client.query(company_query).to_dataframe()
for _, row in company_df.iterrows():
    print(f"\n{row['company_name']}:")
    print(f"  Count: {row['count']}")
    print(f"  Avg Value: ${row['avg_value']:,.2f}" if row['avg_value'] else "  Avg Value: $0")
    print(f"  Source: {row['source']}")