#!/usr/bin/env python3
"""
Test BigQuery connection using service account authentication
"""

import os
import json
import sys
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Add parent directories to path to find .env file
current_dir = Path(__file__).parent
parent_dir = current_dir.parent.parent
sys.path.append(str(parent_dir))

# Try to load .env from multiple locations
env_locations = [
    current_dir / '.env',
    current_dir.parent / '.env',
    parent_dir / '.env'
]

for env_path in env_locations:
    if env_path.exists():
        print(f"Loading environment from: {env_path}")
        load_dotenv(env_path)
        break
else:
    print("Warning: No .env file found, using system environment")
    load_dotenv()


def test_connection():
    """Test BigQuery connection"""
    print("=" * 50)
    print("Testing BigQuery Connection")
    print("=" * 50)

    # Check for service account key
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')

    if not service_account_json:
        print("❌ ERROR: GCP_SERVICE_ACCOUNT_KEY not found in environment")
        print("\nPlease ensure you have:")
        print("1. A .env file with GCP_SERVICE_ACCOUNT_KEY variable")
        print("2. The variable contains valid service account JSON")
        return False

    print("✓ Found GCP_SERVICE_ACCOUNT_KEY in environment")

    try:
        # Parse service account JSON
        service_account_info = json.loads(service_account_json)
        project_id = service_account_info.get('project_id')
        print(f"✓ Parsed service account for project: {project_id}")

        # Create credentials
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/bigquery']
        )
        print("✓ Created credentials successfully")

        # Create BigQuery client
        client = bigquery.Client(
            credentials=credentials,
            project=project_id
        )
        print(f"✓ Created BigQuery client for project: {client.project}")

        # Test listing datasets
        print("\nTesting API access by listing datasets...")
        datasets = list(client.list_datasets(max_results=5))

        if datasets:
            print(f"✓ Found {len(datasets)} datasets:")
            for dataset in datasets:
                print(f"  - {dataset.dataset_id}")
        else:
            print("⚠️  No datasets found (but connection works)")

        # Test specific dataset if configured
        test_dataset = "op_20250702"  # The source dataset from draft.py
        try:
            dataset_ref = f"{project_id}.{test_dataset}"
            dataset = client.get_dataset(dataset_ref)
            print(f"\n✓ Found target dataset: {test_dataset}")
            print(f"  Location: {dataset.location}")
            print(f"  Created: {dataset.created}")

            # List tables in dataset
            tables = list(client.list_tables(dataset_ref, max_results=5))
            print(f"  Tables: {len(tables)} found")
            for table in tables[:3]:
                table_full = client.get_table(f"{dataset_ref}.{table.table_id}")
                print(f"    - {table.table_id}: {table_full.num_rows:,} rows")

        except Exception as e:
            print(f"\n⚠️  Dataset '{test_dataset}' not found or not accessible")
            print(f"  Error: {e}")

        print("\n" + "=" * 50)
        print("✅ CONNECTION TEST SUCCESSFUL")
        print("=" * 50)
        print("\nYou can now run the migration script:")
        print("  python bq_transfer.py")

        return True

    except json.JSONDecodeError as e:
        print(f"\n❌ ERROR: Invalid JSON in GCP_SERVICE_ACCOUNT_KEY")
        print(f"  {e}")
        return False

    except Exception as e:
        print(f"\n❌ ERROR: Failed to connect to BigQuery")
        print(f"  {e}")
        print("\nTroubleshooting:")
        print("1. Check that the service account has BigQuery permissions")
        print("2. Verify the JSON is complete and valid")
        print("3. Ensure the project ID is correct")
        return False


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)