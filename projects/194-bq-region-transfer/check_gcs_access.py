#!/usr/bin/env python3
"""
Check GCS bucket access for the service account
"""

import os
import json
import sys
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_gcs_access():
    """Check GCS access and list available buckets"""
    print("=" * 60)
    print("Checking GCS Access")
    print("=" * 60)

    # Get service account
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        print("❌ GCP_SERVICE_ACCOUNT_KEY not found")
        return False

    try:
        # Parse service account
        service_account_info = json.loads(service_account_json)
        project_id = service_account_info.get('project_id')
        service_account_email = service_account_info.get('client_email')

        print(f"Service Account: {service_account_email}")
        print(f"Project: {project_id}")
        print()

        # Create credentials
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )

        # Create storage client
        storage_client = storage.Client(
            credentials=credentials,
            project=project_id
        )

        # List buckets
        print("Accessible Buckets:")
        print("-" * 40)
        buckets = list(storage_client.list_buckets())

        if buckets:
            for bucket in buckets:
                print(f"  ✓ {bucket.name} (location: {bucket.location})")

                # Try to check write permission
                try:
                    test_blob = bucket.blob('test-write-permission.txt')
                    test_blob.upload_from_string('test', if_generation_match=0)
                    test_blob.delete()
                    print(f"    └─ Write access: ✓ YES")
                except Exception as e:
                    if "412" in str(e):  # Precondition failed means blob exists
                        print(f"    └─ Write access: ✓ YES (blob exists)")
                    else:
                        print(f"    └─ Write access: ✗ NO")
        else:
            print("  No buckets found or accessible")

        # Check specific bucket
        print("\nChecking target bucket: conflixis-temp")
        print("-" * 40)
        try:
            bucket = storage_client.bucket('conflixis-temp')
            bucket.reload()  # This will fail if we don't have access
            print(f"  ✓ Bucket exists and is readable")

            # Check write permission
            try:
                test_blob = bucket.blob('op_20250702/test-write.txt')
                test_blob.upload_from_string('test')
                test_blob.delete()
                print(f"  ✓ Write permission confirmed")
            except Exception as e:
                print(f"  ✗ No write permission: {e}")

        except Exception as e:
            print(f"  ✗ Cannot access bucket: {e}")

        # Suggest alternatives
        print("\n" + "=" * 60)
        print("RECOMMENDATIONS:")
        print("=" * 60)

        if not buckets:
            print("1. Create a new bucket for transfers:")
            print(f"   gsutil mb -p {project_id} -l US gs://{project_id}-bq-transfer")
            print()
            print("2. Or grant Storage Admin role to service account:")
            print(f"   gcloud projects add-iam-policy-binding {project_id} \\")
            print(f"     --member='serviceAccount:{service_account_email}' \\")
            print(f"     --role='roles/storage.admin'")
        else:
            writable_buckets = [b.name for b in buckets]
            if writable_buckets:
                print(f"Use one of these accessible buckets instead:")
                for bucket_name in writable_buckets[:3]:
                    print(f"  - {bucket_name}")
                print()
                print("Update config.yaml or use command line:")
                print(f"  python bq_transfer.py --bucket {writable_buckets[0]}")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    check_gcs_access()