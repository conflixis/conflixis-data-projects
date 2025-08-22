#!/usr/bin/env python3
"""Explore the disclosures_raw_latest table with JSON blobs."""

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

print('Examining conflixis-engine.firestore_export.disclosures_raw_latest')
print('='*80)

# First, check the table schema
table_ref = client.dataset('firestore_export').table('disclosures_raw_latest')
table = client.get_table(table_ref)

print("\n1. Table Schema:")
print("-"*80)
for field in table.schema:
    print(f"  {field.name}: {field.field_type} {'(REQUIRED)' if field.mode == 'REQUIRED' else ''}")

# Get sample records
sample_query = """
SELECT *
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = 'gcO9AHYlNSzFeGTRSFRa'
LIMIT 5
"""

print("\n2. Sample Records:")
print("-"*80)
df = client.query(sample_query).to_dataframe()
print(f"Found {len(df)} records")

if len(df) > 0:
    # Look at first record
    print("\nFirst record columns:")
    for col in df.columns:
        value = df.iloc[0][col]
        if pd.notna(value):
            if col == 'data':
                print(f"  {col}: <JSON BLOB - see parsed below>")
            else:
                print(f"  {col}: {str(value)[:200]}")
    
    # Parse the JSON data field
    print("\n3. Parsed JSON from 'data' field:")
    print("-"*80)
    
    for i in range(min(3, len(df))):
        print(f"\nRecord {i+1}:")
        data_str = df.iloc[i]['data']
        if pd.notna(data_str):
            try:
                data = json.loads(data_str)
                
                # Extract key fields
                print(f"  Document ID: {df.iloc[i].get('document_id', 'N/A')}")
                
                # Reporter info
                reporter = data.get('reporter', {})
                print(f"  Reporter Name: {reporter.get('name', 'N/A')}")
                print(f"  Reporter Email: {reporter.get('email', 'N/A')}")
                print(f"  Reporter ID: {reporter.get('id', 'N/A')}")
                print(f"  Reporter User ID: {reporter.get('authed_user_id', 'N/A')}")
                
                # Question/Disclosure info
                question = data.get('question', {})
                print(f"  Disclosure Type: {question.get('title', 'N/A')}")
                print(f"  Category: {question.get('category_label', 'N/A')}")
                
                # Company info
                print(f"  Company Name: {data.get('company_name', 'N/A')}")
                print(f"  Compensation Value: {data.get('compensation_value', 'N/A')}")
                
                # Status
                print(f"  Status: {data.get('status', 'N/A')}")
                print(f"  Review Status: {data.get('review_status', 'N/A')}")
                
                # Show all top-level keys
                print(f"  All top-level keys: {list(data.keys())}")
                
            except json.JSONDecodeError as e:
                print(f"  ERROR parsing JSON: {e}")
        else:
            print(f"  No data field for record {i+1}")

# Look for specific reporters
print("\n4. Finding specific reporters:")
print("-"*80)

specific_query = """
SELECT 
    document_id,
    JSON_VALUE(data, '$.reporter.name') as reporter_name,
    JSON_VALUE(data, '$.reporter.email') as reporter_email,
    JSON_VALUE(data, '$.reporter.id') as reporter_id,
    JSON_VALUE(data, '$.reporter.authed_user_id') as reporter_user_id,
    JSON_VALUE(data, '$.question.title') as disclosure_type,
    JSON_VALUE(data, '$.company_name') as company_name
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = 'gcO9AHYlNSzFeGTRSFRa'
    AND JSON_VALUE(data, '$.reporter.name') IN ('Zachary Sypert', 'Audrey Puentes', 'Shawn Parsley')
ORDER BY reporter_name
LIMIT 10
"""

specific_df = client.query(specific_query).to_dataframe()
print(f"Found {len(specific_df)} records for specific reporters")

for _, row in specific_df.iterrows():
    print(f"\n{row['reporter_name']}:")
    print(f"  Document ID: {row['document_id']}")
    print(f"  Email: {row['reporter_email']}")
    print(f"  Reporter ID: {row['reporter_id']}")
    print(f"  User ID: {row['reporter_user_id']}")
    print(f"  Disclosure: {row['disclosure_type']}")
    print(f"  Company: {row['company_name']}")

# Check if we can find the User ID format you mentioned
print("\n5. Checking for User ID format like '8xxiU0YJouhc4X18R8JQkgnGH0k1':")
print("-"*80)

user_id_query = """
SELECT 
    JSON_VALUE(data, '$.reporter.name') as reporter_name,
    JSON_VALUE(data, '$.reporter.id') as reporter_id,
    JSON_VALUE(data, '$.reporter.authed_user_id') as authed_user_id,
    JSON_VALUE(data, '$.person_id') as person_id,
    JSON_VALUE(data, '$.user_id') as user_id
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = 'gcO9AHYlNSzFeGTRSFRa'
    AND (
        JSON_VALUE(data, '$.reporter.id') LIKE '%8xxiU0YJouhc4X18R8JQkgnGH0k1%'
        OR JSON_VALUE(data, '$.reporter.authed_user_id') LIKE '%8xxiU0YJouhc4X18R8JQkgnGH0k1%'
        OR JSON_VALUE(data, '$.person_id') LIKE '%8xxiU0YJouhc4X18R8JQkgnGH0k1%'
        OR JSON_VALUE(data, '$.user_id') LIKE '%8xxiU0YJouhc4X18R8JQkgnGH0k1%'
        OR JSON_VALUE(data, '$.reporter.name') = 'Audrey Puentes'
    )
LIMIT 5
"""

user_df = client.query(user_id_query).to_dataframe()
if len(user_df) > 0:
    print(f"Found {len(user_df)} records:")
    for _, row in user_df.iterrows():
        print(f"\n{row['reporter_name']}:")
        print(f"  reporter.id: {row['reporter_id']}")
        print(f"  reporter.authed_user_id: {row['authed_user_id']}")
        print(f"  person_id: {row['person_id']}")
        print(f"  user_id: {row['user_id']}")
else:
    print("No records found with that User ID format")

# Get full JSON for one Audrey Puentes record
print("\n6. Full JSON for one Audrey Puentes disclosure:")
print("-"*80)

full_json_query = """
SELECT 
    document_id,
    data
FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
WHERE JSON_VALUE(data, '$.group_id') = 'gcO9AHYlNSzFeGTRSFRa'
    AND JSON_VALUE(data, '$.reporter.name') = 'Audrey Puentes'
LIMIT 1
"""

full_df = client.query(full_json_query).to_dataframe()
if len(full_df) > 0:
    doc_id = full_df.iloc[0]['document_id']
    data_str = full_df.iloc[0]['data']
    print(f"Document ID: {doc_id}")
    print("\nFull JSON structure:")
    try:
        data = json.loads(data_str)
        print(json.dumps(data, indent=2))
    except json.JSONDecodeError as e:
        print(f"ERROR parsing JSON: {e}")