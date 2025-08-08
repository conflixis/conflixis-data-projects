#!/usr/bin/env python3
"""
Check the schema of the disclosure tables to understand available columns
"""

import os
import json
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery

# Load environment variables
load_dotenv()

def check_table_schema():
    """Check schema of the parsed disclosures table"""
    
    try:
        # Load credentials
        service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        
        # Initialize BigQuery client
        project_id = 'data-analytics-389803'
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        # Get table reference
        table_id = "conflixis-engine.firestore_export.disclosures_raw_latest_parse"
        
        # Get table schema
        table = client.get_table(table_id)
        
        print("Table Schema for disclosures_raw_latest_parse:")
        print("=" * 60)
        
        # Group fields by category
        id_fields = []
        provider_fields = []
        entity_fields = []
        financial_fields = []
        date_fields = []
        status_fields = []
        other_fields = []
        
        for field in table.schema:
            name = field.name.lower()
            if 'id' in name or 'document' in name:
                id_fields.append(field)
            elif 'provider' in name or 'physician' in name or 'doctor' in name:
                provider_fields.append(field)
            elif 'entity' in name or 'company' in name or 'organization' in name:
                entity_fields.append(field)
            elif 'amount' in name or 'value' in name or 'payment' in name or 'compensation' in name:
                financial_fields.append(field)
            elif 'date' in name or 'time' in name:
                date_fields.append(field)
            elif 'status' in name or 'review' in name or 'approved' in name:
                status_fields.append(field)
            else:
                other_fields.append(field)
        
        # Print grouped fields
        print("\nIDENTIFIER FIELDS:")
        for field in id_fields:
            print(f"  {field.name}: {field.field_type}")
        
        print("\nPROVIDER FIELDS:")
        for field in provider_fields:
            print(f"  {field.name}: {field.field_type}")
        
        print("\nENTITY FIELDS:")
        for field in entity_fields:
            print(f"  {field.name}: {field.field_type}")
        
        print("\nFINANCIAL FIELDS:")
        for field in financial_fields:
            print(f"  {field.name}: {field.field_type}")
        
        print("\nDATE FIELDS:")
        for field in date_fields:
            print(f"  {field.name}: {field.field_type}")
        
        print("\nSTATUS FIELDS:")
        for field in status_fields:
            print(f"  {field.name}: {field.field_type}")
        
        print("\nOTHER FIELDS:")
        for field in other_fields:
            print(f"  {field.name}: {field.field_type}")
        
        print("\n" + "=" * 60)
        print(f"Total fields: {len(table.schema)}")
        
        # Now get a sample row to see actual data
        print("\nSample data (1 row):")
        print("-" * 60)
        
        query = f"""
        SELECT *
        FROM `{table_id}`
        WHERE group_id = 'gcO9AHYlNSzFeGTRSFRa'
        LIMIT 1
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            for key in row.keys():
                value = row[key]
                if value and len(str(value)) > 50:
                    value = str(value)[:50] + "..."
                print(f"{key}: {value}")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    check_table_schema()