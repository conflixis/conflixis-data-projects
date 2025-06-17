#!/usr/bin/env python3
"""
BigQuery Query Example - Python Script
This script demonstrates querying the PHYSICIANS_OVERVIEW table using a standalone Python script.
"""

import os
import sys
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import argparse


def setup_credentials():
    """Load credentials from environment variable."""
    # Load environment variables from common/.env
    env_path = Path(__file__).parent.parent.parent / 'common' / '.env'
    load_dotenv(env_path)
    
    # Get credentials path
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not credentials_path:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS not found in environment variables")
    
    # Create credentials object
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    
    return credentials


def query_physicians_overview(limit=100, output_format='display'):
    """
    Query the PHYSICIANS_OVERVIEW table.
    
    Args:
        limit (int): Number of rows to retrieve
        output_format (str): Output format - 'display', 'csv', or 'json'
    
    Returns:
        pandas.DataFrame: Query results
    """
    # Setup credentials and client
    credentials = setup_credentials()
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
    
    print(f"Connected to project: {client.project}")
    
    # Define query
    query = f"""
    SELECT * 
    FROM `data-analytics-389803.CONFLIXIS_309340.PHYSICIANS_OVERVIEW` 
    LIMIT {limit}
    """
    
    print(f"Executing query for {limit} rows...")
    
    # Execute query
    df = client.query(query).to_dataframe()
    
    print(f"Query returned {len(df)} rows and {len(df.columns)} columns")
    
    # Handle output based on format
    if output_format == 'display':
        print("\nFirst 10 rows:")
        print(df.head(10))
        print("\nColumn names:")
        print(df.columns.tolist())
        print("\nData types:")
        print(df.dtypes)
    elif output_format == 'csv':
        output_file = 'physicians_overview_sample.csv'
        df.to_csv(output_file, index=False)
        print(f"\nData saved to {output_file}")
    elif output_format == 'json':
        output_file = 'physicians_overview_sample.json'
        df.to_json(output_file, orient='records', indent=2)
        print(f"\nData saved to {output_file}")
    
    return df


def get_table_info():
    """Get information about the PHYSICIANS_OVERVIEW table."""
    credentials = setup_credentials()
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
    
    # Get table reference
    table_ref = client.dataset('CONFLIXIS_309340', project='data-analytics-389803').table('PHYSICIANS_OVERVIEW')
    table = client.get_table(table_ref)
    
    print(f"\nTable Information:")
    print(f"- Full table ID: {table.full_table_id}")
    print(f"- Number of rows: {table.num_rows:,}")
    print(f"- Number of columns: {len(table.schema)}")
    print(f"- Size: {table.num_bytes / (1024**3):.2f} GB")
    print(f"- Created: {table.created}")
    print(f"- Last modified: {table.modified}")
    
    print(f"\nSchema:")
    for field in table.schema:
        print(f"- {field.name} ({field.field_type})")


def main():
    """Main function with command-line interface."""
    parser = argparse.ArgumentParser(
        description='Query PHYSICIANS_OVERVIEW table from BigQuery'
    )
    parser.add_argument(
        '--limit', 
        type=int, 
        default=100,
        help='Number of rows to retrieve (default: 100)'
    )
    parser.add_argument(
        '--output', 
        choices=['display', 'csv', 'json'],
        default='display',
        help='Output format (default: display)'
    )
    parser.add_argument(
        '--info',
        action='store_true',
        help='Show table information instead of querying data'
    )
    
    args = parser.parse_args()
    
    try:
        if args.info:
            get_table_info()
        else:
            query_physicians_overview(limit=args.limit, output_format=args.output)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()