#!/usr/bin/env python3
"""
Test script to verify the environment setup for Risk Assessment notebook
"""
import sys
import os

print("🔍 Testing Conflixis Analytics Setup")
print("=" * 50)

# Test 1: Python version
print("\n1️⃣ Python Version Check:")
print(f"   Python {sys.version}")
if sys.version_info >= (3, 12):
    print("   ✅ Python 3.12+ detected")
else:
    print("   ❌ Python 3.12+ required")

# Test 2: Required packages
print("\n2️⃣ Package Dependencies:")
packages_to_check = [
    'pandas',
    'numpy',
    'matplotlib',
    'seaborn',
    'google.cloud.bigquery',
    'dotenv',
    'openpyxl'
]

all_packages_ok = True
for package in packages_to_check:
    try:
        if package == 'google.cloud.bigquery':
            import google.cloud.bigquery
            print(f"   ✅ {package} installed")
        elif package == 'dotenv':
            import dotenv
            print(f"   ✅ python-dotenv installed")
        else:
            __import__(package)
            print(f"   ✅ {package} installed")
    except ImportError:
        print(f"   ❌ {package} NOT installed")
        all_packages_ok = False

# Test 3: Environment files
print("\n3️⃣ Environment Files:")
env_file = 'common/.env'
creds_file = 'common/data-analytics-389803-a6f8a077b407.json'

if os.path.exists(env_file):
    print(f"   ✅ {env_file} exists")
else:
    print(f"   ❌ {env_file} NOT found")

if os.path.exists(creds_file):
    print(f"   ✅ {creds_file} exists")
else:
    print(f"   ❌ {creds_file} NOT found")

# Test 4: BigQuery connection
print("\n4️⃣ BigQuery Connection Test:")
try:
    from dotenv import load_dotenv
    from google.cloud import bigquery
    
    load_dotenv('common/.env')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    client = bigquery.Client(project='data-analytics-389803')
    
    # Test query
    query = "SELECT 1 as test"
    query_job = client.query(query)
    results = list(query_job)
    
    print(f"   ✅ BigQuery connection successful!")
    print(f"   📊 Project: {client.project}")
except Exception as e:
    print(f"   ❌ BigQuery connection failed: {str(e)}")

# Test 5: Notebook location
print("\n5️⃣ Notebook Location:")
notebook_path = 'projects/001-risk-assessment-new/Risk_assessment_new.ipynb'
if os.path.exists(notebook_path):
    print(f"   ✅ Notebook found at: {notebook_path}")
else:
    print(f"   ❌ Notebook NOT found at: {notebook_path}")

print("\n" + "=" * 50)
print("✨ Setup verification complete!")

if all_packages_ok and os.path.exists(env_file) and os.path.exists(creds_file):
    print("\n🎉 Your environment is ready to run the Risk Assessment notebook!")
    print("\nTo run the notebook:")
    print("1. Activate Poetry environment: poetry shell")
    print("2. Start Jupyter: jupyter notebook")
    print("3. Navigate to: projects/001-risk-assessment-new/Risk_assessment_new.ipynb")
else:
    print("\n⚠️  Please fix the issues above before running the notebook.")