#!/usr/bin/env python3
"""
Test access to data sources for disclosure policy review
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery

# Load environment variables
load_dotenv()

def test_disclosure_forms_table():
    """Test access to disclosure forms table"""
    
    print("Testing Disclosure Forms Table...")
    print("-" * 50)
    
    try:
        # Load credentials from environment variable
        service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        
        # Initialize BigQuery client
        project_id = 'data-analytics-389803'  # Your main BigQuery project
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        # Test query on disclosure forms table
        table_id = "conflixis-engine.firestore_export.disclosure_forms_raw_latest_v3"
        
        # First, let's check if we can access the table and get its schema
        query = f"""
        SELECT 
            column_name,
            data_type
        FROM `{table_id}`.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'disclosure_forms_raw_latest_v3'
        LIMIT 10
        """
        
        # Query filtered by specific group_id
        group_id = 'gcO9AHYlNSzFeGTRSFRa'  # Specific group we're analyzing
        query = f"""
        SELECT *
        FROM `{table_id}`
        WHERE group_id = '{group_id}'
        LIMIT 5
        """
        
        print(f"Querying table: {table_id}")
        print(f"Query: {query[:100]}...")
        
        # Execute query
        query_job = client.query(query)
        results = query_job.result()
        
        # Get schema information
        schema = results.schema
        print(f"\nTable Schema ({len(schema)} columns):")
        print("-" * 30)
        for field in schema[:10]:  # Show first 10 fields
            print(f"  {field.name}: {field.field_type}")
        if len(schema) > 10:
            print(f"  ... and {len(schema) - 10} more columns")
        
        # Show sample data
        print(f"\nSample Data (first 5 rows):")
        print("-" * 30)
        row_count = 0
        for row in results:
            row_count += 1
            print(f"Row {row_count}:")
            # Show first few fields
            for key in list(row.keys())[:5]:
                value = row[key]
                if value and len(str(value)) > 50:
                    value = str(value)[:50] + "..."
                print(f"  {key}: {value}")
            print()
            if row_count >= 2:  # Just show 2 rows for brevity
                break
        
        # Get total row count for the specific group
        count_query = f"""
        SELECT 
            COUNT(*) as total_rows,
            '{group_id}' as group_id
        FROM `{table_id}`
        WHERE group_id = '{group_id}'
        """
        
        count_job = client.query(count_query)
        count_results = count_job.result()
        for row in count_results:
            print(f"Total rows for group_id '{row['group_id']}': {row['total_rows']:,}")
        
        print("\n✅ BigQuery access successful!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error accessing BigQuery: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        
        # Check if it's a project/dataset issue
        if "Not found" in str(e):
            print("\nPossible issues:")
            print("1. The table might not exist or you don't have access")
            print("2. The project ID might be incorrect")
            print("3. The dataset name might be incorrect")
            
            # Try to list available datasets
            try:
                print("\nAttempting to list available datasets...")
                datasets = list(client.list_datasets())
                if datasets:
                    print("Available datasets in project:")
                    for dataset in datasets[:5]:
                        print(f"  - {dataset.dataset_id}")
                else:
                    print("No datasets found in the current project")
            except Exception as list_error:
                print(f"Could not list datasets: {list_error}")
        
        return False

def test_disclosures_parsed_table():
    """Test access to parsed disclosures table"""
    
    print("\nTesting Parsed Disclosures Table...")
    print("-" * 50)
    
    try:
        # Load credentials from environment variable
        service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        
        # Initialize BigQuery client
        project_id = 'data-analytics-389803'
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        # Test query on parsed disclosures table
        table_id = "conflixis-engine.firestore_export.disclosures_raw_latest_parse"
        
        # First get sample data
        query = f"""
        SELECT *
        FROM `{table_id}`
        LIMIT 5
        """
        
        print(f"Querying table: {table_id}")
        
        # Execute query
        query_job = client.query(query)
        results = query_job.result()
        
        # Get schema information
        schema = results.schema
        print(f"\nTable Schema ({len(schema)} columns):")
        print("-" * 30)
        for field in schema[:15]:  # Show first 15 fields
            print(f"  {field.name}: {field.field_type}")
        if len(schema) > 15:
            print(f"  ... and {len(schema) - 15} more columns")
        
        # Show sample data
        print(f"\nSample Data (first 2 rows):")
        print("-" * 30)
        row_count = 0
        for row in results:
            row_count += 1
            print(f"Row {row_count}:")
            # Show first few fields
            for key in list(row.keys())[:5]:
                value = row[key]
                if value and len(str(value)) > 50:
                    value = str(value)[:50] + "..."
                print(f"  {key}: {value}")
            print()
            if row_count >= 2:
                break
        
        # Get total row count
        count_query = f"""
        SELECT COUNT(*) as total_rows
        FROM `{table_id}`
        """
        
        count_job = client.query(count_query)
        count_results = count_job.result()
        for row in count_results:
            print(f"Total rows in parsed table: {row['total_rows']:,}")
        
        # Check if there's any relationship with the group_id we're interested in
        group_id = 'gcO9AHYlNSzFeGTRSFRa'
        
        # Check if group_id exists in this table
        check_query = f"""
        SELECT COUNT(*) as matching_rows
        FROM `{table_id}`
        WHERE group_id = '{group_id}'
        LIMIT 1
        """
        
        try:
            check_job = client.query(check_query)
            check_results = check_job.result()
            for row in check_results:
                if row['matching_rows'] > 0:
                    print(f"Found {row['matching_rows']:,} rows with group_id '{group_id}'")
        except:
            # group_id column might not exist
            print("Note: group_id column not found in this table")
        
        print("\n✅ Parsed disclosures table access successful!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error accessing parsed disclosures table: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        return False

def main():
    """Main execution function"""
    print("=" * 50)
    print("DISCLOSURE DATA SOURCE ACCESS TEST")
    print("=" * 50)
    print()
    
    # Test both data sources
    success1 = test_disclosure_forms_table()
    success2 = test_disclosures_parsed_table()
    
    print("\n" + "=" * 50)
    print("TEST SUMMARY")
    print("-" * 50)
    print(f"Disclosure Forms Table: {'✅ Success' if success1 else '❌ Failed'}")
    print(f"Parsed Disclosures Table: {'✅ Success' if success2 else '❌ Failed'}")
    
    if success1 and success2:
        print("\n✅ All data source access tests completed successfully")
    else:
        print("\n⚠️ Some data source access tests encountered issues")
        print("Please check the error messages above")
    print("=" * 50)

if __name__ == "__main__":
    main()