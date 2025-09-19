#!/usr/bin/env python3
"""Check available tables in BigQuery"""

import sys
sys.path.insert(0, '/home/incent/conflixis-data-projects/projects/182-healthcare-coi-analytics-report-template/src')

from data.bigquery_connector import BigQueryConnector

connector = BigQueryConnector()

# Simple query to check what OP tables exist
query = """
SELECT DISTINCT
    Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as manufacturer_name
FROM `data-analytics-389803.temp.northwell_open_payments`
LIMIT 5
"""

print("Checking temp.northwell_open_payments...")
try:
    df = connector.query(query)
    print("Success! Found manufacturers:")
    for _, row in df.iterrows():
        print(f"  {row['manufacturer_name']}")
except Exception as e:
    print(f"Error: {e}")

# Try another known table
query2 = """
SELECT DISTINCT
    manufacturer_name
FROM `data-analytics-389803.temp.op_manufacturer_names`
LIMIT 5
"""

print("\nChecking temp.op_manufacturer_names...")
try:
    df = connector.query(query2)
    print("Success! Found manufacturers:")
    for _, row in df.iterrows():
        print(f"  {row['manufacturer_name']}")
except Exception as e:
    print(f"Error: {e}")

# Check for RX tables
query3 = """
SELECT DISTINCT
    BRND_NAME
FROM `data-analytics-389803.temp.northwell_prescriptions`
LIMIT 5
"""

print("\nChecking temp.northwell_prescriptions...")
try:
    df = connector.query(query3)
    print("Success! Found drug brands:")
    for _, row in df.iterrows():
        print(f"  {row['BRND_NAME']}")
except Exception as e:
    print(f"Error: {e}")