#!/usr/bin/env python3
"""Check column names in manufacturer tables"""

import sys
sys.path.insert(0, '/home/incent/conflixis-data-projects/projects/182-healthcare-coi-analytics-report-template/src')

from data.bigquery_connector import BigQueryConnector

connector = BigQueryConnector()

# Check OP table structure
query1 = """
SELECT *
FROM `data-analytics-389803.conflixis_data_projects.op_manufacturer_names`
LIMIT 1
"""

print("Checking op_manufacturer_names columns...")
try:
    df = connector.query(query1)
    print("Columns in op_manufacturer_names:")
    for col in df.columns:
        print(f"  - {col}")
    print("\nSample row:")
    if not df.empty:
        print(df.iloc[0].to_dict())
except Exception as e:
    print(f"Error: {e}")

# Check RX table structure
query2 = """
SELECT *
FROM `data-analytics-389803.conflixis_data_projects.rx_manufacturer_names`
LIMIT 1
"""

print("\n" + "="*60)
print("Checking rx_manufacturer_names columns...")
try:
    df = connector.query(query2)
    print("Columns in rx_manufacturer_names:")
    for col in df.columns:
        print(f"  - {col}")
    print("\nSample row:")
    if not df.empty:
        print(df.iloc[0].to_dict())
except Exception as e:
    print(f"Error: {e}")