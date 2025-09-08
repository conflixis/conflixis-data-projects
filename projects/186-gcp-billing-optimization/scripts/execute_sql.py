#!/usr/bin/env python3
"""
Execute SQL scripts for BigQuery optimization.
"""

import os
import sys
import json
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

def setup_client():
    """Setup BigQuery client with credentials."""
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        with open(env_file, 'r') as f:
            content = f.read()
            lines = content.split('\n')
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    if key == 'GCP_SERVICE_ACCOUNT_KEY':
                        if value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                    else:
                        value = value.strip().strip("'\"")
                    os.environ[key] = value
    
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
    
    service_account_json = service_account_json.replace('\\\\n', '\\n')
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return bigquery.Client(credentials=credentials, project="data-analytics-389803")

def execute_harmonized_views(client):
    """Execute Week 1 harmonized views creation."""
    
    print("=" * 80)
    print("EXECUTING WEEK 1: HARMONIZED VIEWS")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print()
    
    # Define the harmonized views to create
    views = [
        {
            "name": "v_PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_harmonized",
            "description": "Harmonized view with NPI as INT64",
            "sql": """
            CREATE OR REPLACE VIEW `data-analytics-389803.conflixis_data_projects.v_PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_harmonized` AS
            SELECT 
                CAST(NPI AS INT64) as NPI,
                * EXCEPT(NPI)
            FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT`
            """
        },
        {
            "name": "v_PHYSICIANS_OVERVIEW_harmonized",
            "description": "Harmonized view with NPI as INT64",
            "sql": """
            CREATE OR REPLACE VIEW `data-analytics-389803.conflixis_data_projects.v_PHYSICIANS_OVERVIEW_harmonized` AS
            SELECT 
                CAST(NPI AS INT64) as NPI,
                * EXCEPT(NPI)
            FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW`
            """
        },
        {
            "name": "v_rx_op_enhanced_full_harmonized",
            "description": "Harmonized view with NPI as INT64",
            "sql": """
            CREATE OR REPLACE VIEW `data-analytics-389803.conflixis_data_projects.v_rx_op_enhanced_full_harmonized` AS
            SELECT 
                CAST(NPI AS INT64) as NPI,
                * EXCEPT(NPI)
            FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
            """
        },
        {
            "name": "v_PHYSICIAN_RX_2020_2024_harmonized",
            "description": "Harmonized view (NPI already INT64)",
            "sql": """
            CREATE OR REPLACE VIEW `data-analytics-389803.conflixis_data_projects.v_PHYSICIAN_RX_2020_2024_harmonized` AS
            SELECT 
                *
            FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024`
            """
        },
        {
            "name": "v_op_general_all_aggregate_static_harmonized",
            "description": "Harmonized view with physician.NPI as INT64",
            "sql": """
            CREATE OR REPLACE VIEW `data-analytics-389803.conflixis_data_projects.v_op_general_all_aggregate_static_harmonized` AS
            SELECT 
                CAST(physician.NPI AS INT64) as physician_NPI,
                physician.* EXCEPT(NPI),
                * EXCEPT(physician)
            FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static`
            """
        }
    ]
    
    results = []
    
    for view in views:
        print(f"Creating view: {view['name']}")
        print(f"  Description: {view['description']}")
        
        try:
            query_job = client.query(view['sql'])
            query_job.result()  # Wait for completion
            
            print(f"  ✅ Success! View created.")
            results.append({
                "view": view['name'],
                "status": "SUCCESS",
                "error": None
            })
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)}")
            results.append({
                "view": view['name'],
                "status": "FAILED",
                "error": str(e)
            })
        
        print()
    
    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    successful = sum(1 for r in results if r['status'] == 'SUCCESS')
    failed = sum(1 for r in results if r['status'] == 'FAILED')
    
    print(f"Total views processed: {len(results)}")
    print(f"  ✅ Successful: {successful}")
    print(f"  ❌ Failed: {failed}")
    
    if failed > 0:
        print("\nFailed views:")
        for r in results:
            if r['status'] == 'FAILED':
                print(f"  - {r['view']}: {r['error']}")
    
    print(f"\nCompleted at: {datetime.now()}")
    
    return results

def test_harmonized_views(client):
    """Test the harmonized views with a sample query."""
    
    print("\n" + "=" * 80)
    print("TESTING HARMONIZED VIEWS")
    print("=" * 80)
    
    # Test query without CAST operations
    test_query = """
    SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT rx.NPI) as unique_providers
    FROM `data-analytics-389803.conflixis_data_projects.v_rx_op_enhanced_full_harmonized` rx
    JOIN `data-analytics-389803.conflixis_data_projects.v_PHYSICIANS_OVERVIEW_harmonized` p
    ON rx.NPI = p.NPI  -- Direct INT64 join, no CAST needed!
    LIMIT 10
    """
    
    print("Testing join without CAST operations...")
    print("Query: Direct NPI join between harmonized views")
    
    try:
        query_job = client.query(test_query)
        results = query_job.result()
        
        for row in results:
            print(f"  Records: {row.record_count}")
            print(f"  Unique Providers: {row.unique_providers}")
        
        # Get query statistics
        print(f"\n📊 Query Statistics:")
        print(f"  Bytes Processed: {query_job.total_bytes_processed:,} bytes")
        print(f"  Bytes Billed: {query_job.total_bytes_billed:,} bytes")
        print(f"  Cache Hit: {query_job.cache_hit}")
        print(f"  Slot Milliseconds: {query_job.slot_millis:,}")
        
        gb_processed = query_job.total_bytes_processed / (1024**3)
        estimated_cost = gb_processed * 0.005  # $5 per TB
        print(f"  Estimated Cost: ${estimated_cost:.4f}")
        
        print("\n✅ Test successful! Harmonized views working correctly.")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        
def main():
    """Main execution."""
    
    if len(sys.argv) > 1 and sys.argv[1] == "--test-only":
        print("Running in test-only mode...")
        client = setup_client()
        test_harmonized_views(client)
    else:
        client = setup_client()
        
        # Execute harmonized views
        results = execute_harmonized_views(client)
        
        # Test if all successful
        if all(r['status'] == 'SUCCESS' for r in results):
            test_harmonized_views(client)
        else:
            print("\n⚠️ Skipping tests due to failed view creation.")

if __name__ == "__main__":
    main()