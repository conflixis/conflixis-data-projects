#!/usr/bin/env python3
"""
Test script to verify BigQuery permissions for billing data access.
"""

import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account

def test_bigquery_connection():
    """Test connection to BigQuery and list available datasets."""
    try:
        # Load service account credentials from environment
        service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
        if not service_account_json:
            print("❌ GCP_SERVICE_ACCOUNT_KEY not found in environment variables")
            return False
        
        # Handle escaped newlines in private key
        service_account_json = service_account_json.replace('\\\\n', '\\n')
        
        # Parse the JSON string
        try:
            service_account_info = json.loads(service_account_json)
        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse service account JSON: {e}")
            # Try alternative parsing
            import ast
            try:
                service_account_info = ast.literal_eval(service_account_json)
                if isinstance(service_account_info, str):
                    service_account_info = json.loads(service_account_info)
            except:
                print("❌ Could not parse service account credentials")
                return False
        
        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info
        )
        
        # Initialize BigQuery client
        client = bigquery.Client(
            credentials=credentials,
            project="data-analytics-389803"  # Your default project
        )
        
        print("✅ Successfully connected to BigQuery")
        print(f"   Using service account: {service_account_info.get('client_email')}")
        print(f"   Default project: {client.project}")
        
        return client
    
    except Exception as e:
        print(f"❌ Failed to connect to BigQuery: {str(e)}")
        return None

def test_billing_dataset_access(client):
    """Test access to billing datasets."""
    billing_project = "billing-administration-389502"
    datasets_to_test = [
        "all_billing_data",
        "all_pricing_data"
    ]
    
    results = {}
    
    for dataset_name in datasets_to_test:
        dataset_id = f"{billing_project}.{dataset_name}"
        print(f"\n📊 Testing access to dataset: {dataset_id}")
        
        try:
            # Try to get dataset metadata
            dataset = client.get_dataset(dataset_id)
            print(f"   ✅ Can access dataset: {dataset_name}")
            print(f"      Location: {dataset.location}")
            print(f"      Created: {dataset.created}")
            
            # Try to list tables
            tables = list(client.list_tables(dataset_id))
            print(f"   📋 Found {len(tables)} tables:")
            for table in tables[:5]:  # Show first 5 tables
                print(f"      - {table.table_id}")
            if len(tables) > 5:
                print(f"      ... and {len(tables) - 5} more tables")
            
            results[dataset_name] = {
                "accessible": True,
                "table_count": len(tables),
                "tables": [table.table_id for table in tables]
            }
            
        except Exception as e:
            print(f"   ❌ Cannot access dataset: {str(e)}")
            results[dataset_name] = {
                "accessible": False,
                "error": str(e)
            }
    
    return results

def test_sample_query(client):
    """Test running a simple query on billing data."""
    print("\n🔍 Testing sample query on billing data...")
    
    # Try a simple query to get recent billing data
    query = """
    SELECT 
        DATE(usage_start_time) as usage_date,
        service.description as service_name,
        SUM(cost) as total_cost,
        currency
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_*`
    WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY usage_date, service_name, currency
    ORDER BY usage_date DESC, total_cost DESC
    LIMIT 10
    """
    
    try:
        # Run the query
        query_job = client.query(query)
        results = query_job.result()
        
        print("   ✅ Query executed successfully!")
        print("   Sample results (last 7 days):")
        
        for row in results:
            print(f"      {row.usage_date}: {row.service_name[:30]:30} - {row.currency} {row.total_cost:.2f}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Query failed: {str(e)}")
        
        # Check if it's a permission error
        if "403" in str(e) or "permission" in str(e).lower():
            print("\n   ⚠️ Permission Issue Detected!")
            print("   The service account needs the following permissions:")
            print("   - roles/bigquery.dataViewer on the billing datasets")
            print("   - roles/bigquery.jobUser on the billing project")
            print(f"\n   Grant permissions with these commands:")
            print(f"   gcloud projects add-iam-policy-binding billing-administration-389502 \\")
            print(f"     --member='serviceAccount:vw-cursor@data-analytics-389803.iam.gserviceaccount.com' \\")
            print(f"     --role='roles/bigquery.jobUser'")
            print(f"\n   For dataset-level permissions, use BigQuery Console or:")
            print(f"   bq add-iam-policy-binding \\")
            print(f"     --member='serviceAccount:vw-cursor@data-analytics-389803.iam.gserviceaccount.com' \\")
            print(f"     --role='roles/bigquery.dataViewer' \\")
            print(f"     billing-administration-389502:all_billing_data")
        
        return False

def main():
    """Main test function."""
    print("=" * 60)
    print("GCP BILLING DATA ACCESS PERMISSION TEST")
    print("=" * 60)
    
    # Test BigQuery connection
    client = test_bigquery_connection()
    if not client:
        return
    
    # Test access to billing datasets
    dataset_results = test_billing_dataset_access(client)
    
    # Test sample query if we have access
    if any(r.get("accessible") for r in dataset_results.values()):
        test_sample_query(client)
    else:
        print("\n⚠️ Cannot run sample query - no accessible datasets")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    accessible_count = sum(1 for r in dataset_results.values() if r.get("accessible"))
    total_count = len(dataset_results)
    
    if accessible_count == total_count:
        print("✅ All billing datasets are accessible!")
        print("   You have the necessary permissions to analyze GCP billing data.")
    elif accessible_count > 0:
        print(f"⚠️ Partial access: {accessible_count}/{total_count} datasets accessible")
        print("   You may need additional permissions for full access.")
    else:
        print("❌ No billing datasets are accessible")
        print("   Please request the necessary permissions from your GCP admin.")

if __name__ == "__main__":
    # Load environment variables from .env file
    from pathlib import Path
    import sys
    
    # Check for .env file - try both locations
    env_files = [
        Path(__file__).parent.parent.parent / ".env",  # Main project .env
        Path(__file__).parent / ".env"  # Local project .env
    ]
    
    for env_file in env_files:
        if env_file.exists():
            print(f"Loading environment from: {env_file}")
            # Simple .env file loader
            with open(env_file, 'r') as f:
                content = f.read()
                lines = content.split('\n')
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        # For GCP_SERVICE_ACCOUNT_KEY, handle the special case
                        if key == 'GCP_SERVICE_ACCOUNT_KEY':
                            # Remove only the outermost quotes
                            if value.startswith("'") and value.endswith("'"):
                                value = value[1:-1]
                        else:
                            # Remove quotes if present for other keys
                            value = value.strip().strip("'\"")
                        os.environ[key] = value
            break
    
    # Debug: Check if key is loaded
    if 'GCP_SERVICE_ACCOUNT_KEY' in os.environ:
        print(f"✓ GCP_SERVICE_ACCOUNT_KEY loaded (length: {len(os.environ['GCP_SERVICE_ACCOUNT_KEY'])})")
    else:
        print("✗ GCP_SERVICE_ACCOUNT_KEY not found in environment")
    
    main()