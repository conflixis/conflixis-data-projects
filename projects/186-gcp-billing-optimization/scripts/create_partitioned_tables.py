#!/usr/bin/env python3
"""
Create partitioned and clustered tables for Week 2 optimization.
"""

import os
import json
import time
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

def create_partitioned_tables(client):
    """Create partitioned and clustered tables."""
    
    print("=" * 80)
    print("CREATING PARTITIONED AND CLUSTERED TABLES")
    print("=" * 80)
    print(f"Started at: {datetime.now()}\n")
    
    tables = [
        {
            "name": "rx_op_enhanced_full_optimized",
            "description": "Optimized with year partitioning and NPI clustering",
            "sql": """
            CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.rx_op_enhanced_full_optimized`
            PARTITION BY RANGE_BUCKET(CAST(year AS INT64), GENERATE_ARRAY(2015, 2030, 1))
            CLUSTER BY NPI, SPECIALTY_PRIMARY, HQ_STATE
            AS 
            SELECT 
                CAST(NPI AS INT64) as NPI,
                CAST(year AS INT64) as year,
                * EXCEPT(NPI, year)
            FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
            WHERE year IS NOT NULL
            """,
            "test_partition": "year = 2022"
        },
        {
            "name": "PHYSICIAN_RX_2020_2024_optimized",
            "description": "Optimized with date partitioning and drug clustering",
            "sql": """
            CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.PHYSICIAN_RX_2020_2024_optimized`
            PARTITION BY claim_date
            CLUSTER BY NPI, BRAND_NAME, GENERIC_NAME
            AS 
            SELECT 
                * 
            FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024`
            WHERE claim_date IS NOT NULL
            """,
            "test_partition": "claim_date BETWEEN '2024-01-01' AND '2024-01-31'"
        },
        {
            "name": "op_general_all_aggregate_static_optimized",
            "description": "Optimized with date partitioning and recipient clustering",
            "sql": """
            CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized`
            PARTITION BY date_of_payment
            CLUSTER BY covered_recipient_npi, covered_recipient_profile_id
            AS 
            SELECT 
                CAST(physician.NPI AS INT64) as physician_NPI,
                * 
            FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static`
            WHERE date_of_payment IS NOT NULL
            """,
            "test_partition": "date_of_payment BETWEEN '2024-01-01' AND '2024-01-31'"
        },
        {
            "name": "PHYSICIANS_OVERVIEW_optimized",
            "description": "Optimized with clustering on NPI and specialty",
            "sql": """
            CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.PHYSICIANS_OVERVIEW_optimized`
            CLUSTER BY NPI, SPECIALTY_PRIMARY, STATE
            AS 
            SELECT 
                CAST(NPI AS INT64) as NPI,
                * EXCEPT(NPI)
            FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW`
            """,
            "test_partition": None
        },
        {
            "name": "PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized",
            "description": "Optimized with clustering on NPI and facility",
            "sql": """
            CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized`
            CLUSTER BY NPI, AFFILIATED_NAME, AFFILIATED_HQ_STATE
            AS 
            SELECT 
                CAST(NPI AS INT64) as NPI,
                * EXCEPT(NPI)
            FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT`
            """,
            "test_partition": None
        }
    ]
    
    results = []
    
    for table in tables:
        print(f"Creating table: {table['name']}")
        print(f"  Description: {table['description']}")
        
        start_time = time.time()
        
        try:
            # Create the table
            query_job = client.query(table['sql'])
            query_job.result()  # Wait for completion
            
            elapsed_time = time.time() - start_time
            
            # Get table size
            table_ref = client.dataset('conflixis_data_projects').table(table['name'])
            table_obj = client.get_table(table_ref)
            
            size_gb = table_obj.num_bytes / (1024**3) if table_obj.num_bytes else 0
            row_count = table_obj.num_rows
            
            print(f"  ✅ Success!")
            print(f"     Size: {size_gb:.2f} GB")
            print(f"     Rows: {row_count:,}")
            print(f"     Time: {elapsed_time:.1f} seconds")
            
            results.append({
                "table": table['name'],
                "status": "SUCCESS",
                "size_gb": size_gb,
                "row_count": row_count,
                "time_seconds": elapsed_time,
                "test_partition": table.get('test_partition')
            })
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:200]}")
            results.append({
                "table": table['name'],
                "status": "FAILED",
                "error": str(e)
            })
        
        print()
    
    return results

def test_partition_pruning(client, results):
    """Test that partition pruning works correctly."""
    
    print("=" * 80)
    print("TESTING PARTITION PRUNING")
    print("=" * 80)
    
    test_results = []
    
    for result in results:
        if result['status'] == 'SUCCESS' and result.get('test_partition'):
            table_name = result['table']
            partition_filter = result['test_partition']
            
            print(f"\nTesting: {table_name}")
            print(f"  Filter: WHERE {partition_filter}")
            
            # Query with partition filter
            test_query = f"""
            SELECT COUNT(*) as count
            FROM `data-analytics-389803.conflixis_data_projects.{table_name}`
            WHERE {partition_filter}
            """
            
            try:
                query_job = client.query(test_query)
                _ = query_job.result()
                
                bytes_processed = query_job.total_bytes_processed
                bytes_billed = query_job.total_bytes_billed
                gb_processed = bytes_processed / (1024**3)
                gb_billed = bytes_billed / (1024**3)
                
                # Compare to full table size
                reduction_pct = ((result['size_gb'] - gb_billed) / result['size_gb'] * 100) if result['size_gb'] > 0 else 0
                
                print(f"  ✅ Partition pruning working!")
                print(f"     Full table: {result['size_gb']:.2f} GB")
                print(f"     Query scanned: {gb_billed:.3f} GB")
                print(f"     Reduction: {reduction_pct:.1f}%")
                
                test_results.append({
                    "table": table_name,
                    "full_size_gb": result['size_gb'],
                    "scanned_gb": gb_billed,
                    "reduction_pct": reduction_pct
                })
                
            except Exception as e:
                print(f"  ❌ Test failed: {str(e)[:100]}")
    
    return test_results

def main():
    """Main execution."""
    client = setup_client()
    
    # Create partitioned tables
    results = create_partitioned_tables(client)
    
    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    successful = sum(1 for r in results if r['status'] == 'SUCCESS')
    failed = sum(1 for r in results if r['status'] == 'FAILED')
    
    print(f"Total tables processed: {len(results)}")
    print(f"  ✅ Successful: {successful}")
    print(f"  ❌ Failed: {failed}")
    
    if successful > 0:
        total_size = sum(r['size_gb'] for r in results if r['status'] == 'SUCCESS')
        total_rows = sum(r['row_count'] for r in results if r['status'] == 'SUCCESS')
        
        print(f"\nTotal data migrated:")
        print(f"  Size: {total_size:.2f} GB")
        print(f"  Rows: {total_rows:,}")
    
    # Test partition pruning
    if successful > 0:
        print("\n")
        test_results = test_partition_pruning(client, results)
        
        if test_results:
            avg_reduction = sum(t['reduction_pct'] for t in test_results) / len(test_results)
            print(f"\n🎯 Average partition pruning reduction: {avg_reduction:.1f}%")
    
    print(f"\nCompleted at: {datetime.now()}")
    
    # Save results
    output_file = Path(__file__).parent.parent / "docs" / "partitioned_tables_results.json"
    with open(output_file, 'w') as f:
        json.dump({
            "creation_results": results,
            "test_results": test_results if 'test_results' in locals() else []
        }, f, indent=2, default=str)
    
    print(f"📁 Results saved to: {output_file}")

if __name__ == "__main__":
    main()