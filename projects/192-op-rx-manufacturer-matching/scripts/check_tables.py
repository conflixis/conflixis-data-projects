#!/usr/bin/env python3
"""Check available tables in BigQuery"""

import sys
sys.path.insert(0, '/home/incent/conflixis-data-projects/projects/182-healthcare-coi-analytics-report-template/src')

from data.bigquery_connector import BigQueryConnector

connector = BigQueryConnector()

# Check for Open Payments tables
query = """
SELECT
    table_catalog,
    table_schema,
    table_name
FROM `data-analytics-389803.conflixis_data_projects.INFORMATION_SCHEMA.TABLES`
WHERE (
    LOWER(table_name) LIKE '%payment%'
    OR LOWER(table_name) LIKE '%prescription%'
    OR LOWER(table_name) LIKE '%med_d%'
)
ORDER BY table_name
"""

print("Checking available tables...")
try:
    df = connector.query(query)
    print("\nFound tables:")
    for _, row in df.iterrows():
        print(f"  {row['table_catalog']}.{row['table_schema']}.{row['table_name']}")
except Exception as e:
    print(f"Error: {e}")

    # Try with temp dataset
    print("\nTrying temp dataset...")
    query2 = """
    SELECT
        table_catalog,
        table_schema,
        table_name
    FROM `data-analytics-389803.temp.INFORMATION_SCHEMA.TABLES`
    WHERE (
        LOWER(table_name) LIKE '%payment%'
        OR LOWER(table_name) LIKE '%prescription%'
        OR LOWER(table_name) LIKE '%med_d%'
    )
    ORDER BY table_name
    """

    try:
        df = connector.query(query2)
        print("\nFound tables in temp:")
        for _, row in df.iterrows():
            print(f"  {row['table_catalog']}.{row['table_schema']}.{row['table_name']}")
    except Exception as e2:
        print(f"Error checking temp: {e2}")