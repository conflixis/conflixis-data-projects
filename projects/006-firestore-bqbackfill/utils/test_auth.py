#!/usr/bin/env python3
"""Test authentication and permissions."""

import os
import sys
from google.cloud import firestore
from google.oauth2 import service_account

# Test different authentication methods
print("Testing authentication methods...")

# Method 1: Service account from env
service_account_path = "/home/incent/conflixis-analytics/common/data-analytics-389803-a6f8a077b407.json"
if os.path.exists(service_account_path):
    print(f"\nTrying service account: {service_account_path}")
    try:
        credentials = service_account.Credentials.from_service_account_file(
            service_account_path
        )
        # Try to access conflixis-web project
        client = firestore.Client(project="conflixis-web", credentials=credentials)
        # Try to list collections
        collections = list(client.collections())
        print(f"Success! Found {len(collections)} collections")
        for col in collections[:5]:  # Show first 5
            print(f"  - {col.id}")
    except Exception as e:
        print(f"Error with service account: {e}")

# Method 2: Try without explicit credentials (uses ADC)
print("\nTrying Application Default Credentials...")
try:
    client = firestore.Client(project="conflixis-web")
    collections = list(client.collections())
    print(f"Success! Found {len(collections)} collections")
    for col in collections[:5]:  # Show first 5
        print(f"  - {col.id}")
except Exception as e:
    print(f"Error with ADC: {e}")

# Method 3: Check if we can at least access the data-analytics project
print("\nTrying to access data-analytics-389803 project...")
try:
    credentials = service_account.Credentials.from_service_account_file(
        service_account_path
    )
    client = firestore.Client(project="data-analytics-389803", credentials=credentials)
    collections = list(client.collections())
    print(f"Success! Found {len(collections)} collections in data-analytics project")
    for col in collections[:5]:  # Show first 5
        print(f"  - {col.id}")
except Exception as e:
    print(f"Error: {e}")

print("\nTo fix the permission issue, you need to:")
print("1. Get a service account key from the conflixis-web project, OR")
print("2. Grant your current service account access to conflixis-web Firestore, OR")
print("3. Use 'gcloud auth application-default login --no-browser' and follow the instructions")