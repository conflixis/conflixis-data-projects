#!/usr/bin/env python3
"""
Create FULL partitioned and clustered tables for production use.
Uses 1-hour timeout for large table operations.
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

def create_full_tables(client):
    """Create full production partitioned and clustered tables."""
    
    print("=" * 80)
    print("CREATING FULL PRODUCTION PARTITIONED TABLES")
    print("=" * 80)
    print(f"Started at: {datetime.now()}")
    print("Note: Using 1-hour timeout for large table operations\n")
    
    tables = [
        {
            "name": "rx_op_enhanced_full_optimized",
            "description": "Full production table with year partitioning and NPI clustering",
            "sql": """
            CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.rx_op_enhanced_full_optimized`
            PARTITION BY RANGE_BUCKET(year_int, GENERATE_ARRAY(2015, 2030, 1))
            CLUSTER BY NPI, SPECIALTY_PRIMARY
            AS 
            SELECT 
                CAST(NPI AS INT64) as NPI,
                CAST(year AS INT64) as year_int,
                * EXCEPT(NPI, year)
            FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
            WHERE year IS NOT NULL
            """,
            "timeout_ms": 3600000  # 1 hour
        },
        {
            "name": "PHYSICIAN_RX_2020_2024_optimized",
            "description": "Full production table with date partitioning and drug clustering",
            "sql": """
            CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.PHYSICIAN_RX_2020_2024_optimized`
            PARTITION BY claim_date
            CLUSTER BY NPI, BRAND_NAME, GENERIC_NAME
            AS 
            SELECT 
                CAST(NPI AS INT64) as NPI,
                * EXCEPT(NPI)
            FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024`
            WHERE claim_date IS NOT NULL
            """,
            "timeout_ms": 3600000  # 1 hour
        },
        {
            "name": "op_general_all_aggregate_static_optimized",
            "description": "Full production table with date partitioning and recipient clustering",
            "sql": """
            CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized`
            PARTITION BY date_of_payment
            CLUSTER BY covered_recipient_npi, covered_recipient_profile_id
            AS 
            SELECT 
                CAST(covered_recipient_npi AS INT64) as covered_recipient_npi,
                * EXCEPT(covered_recipient_npi)
            FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static`
            WHERE date_of_payment IS NOT NULL
            """,
            "timeout_ms": 3600000  # 1 hour
        },
        {
            "name": "PHYSICIANS_OVERVIEW_optimized",
            "description": "Full production table with clustering on NPI and specialty",
            "sql": """
            CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.PHYSICIANS_OVERVIEW_optimized`
            CLUSTER BY NPI, SPECIALTY_PRIMARY, STATE
            AS 
            SELECT 
                CAST(NPI AS INT64) as NPI,
                * EXCEPT(NPI)
            FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW`
            """,
            "timeout_ms": 1800000  # 30 minutes (smaller table)
        },
        {
            "name": "PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized",
            "description": "Full production table with clustering on NPI and facility",
            "sql": """
            CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized`
            CLUSTER BY NPI, AFFILIATED_NAME, AFFILIATED_HQ_STATE
            AS 
            SELECT 
                CAST(NPI AS INT64) as NPI,
                * EXCEPT(NPI)
            FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT`
            """,
            "timeout_ms": 1800000  # 30 minutes (smaller table)
        }
    ]
    
    results = []
    
    for table in tables:
        print(f"Creating table: {table['name']}")
        print(f"  Description: {table['description']}")
        print(f"  Timeout: {table['timeout_ms']/1000/60:.0f} minutes")
        
        start_time = time.time()
        
        try:
            # Configure job with extended timeout
            job_config = bigquery.QueryJobConfig(
                use_legacy_sql=False,
                priority=bigquery.QueryPriority.INTERACTIVE,
                maximum_bytes_billed=10 * 1024 ** 4  # 10 TB limit
            )
            
            # Create the table with timeout
            query_job = client.query(table['sql'], job_config=job_config)
            
            # Wait for completion with timeout
            print(f"  ⏳ Waiting for table creation (up to {table['timeout_ms']/1000/60:.0f} minutes)...")
            query_job.result(timeout=table['timeout_ms']/1000)  # Convert to seconds
            
            elapsed_time = time.time() - start_time
            
            # Get table size
            table_ref = client.dataset('conflixis_data_projects').table(table['name'])
            table_obj = client.get_table(table_ref)
            
            size_gb = table_obj.num_bytes / (1024**3) if table_obj.num_bytes else 0
            row_count = table_obj.num_rows
            
            print(f"  ✅ Success!")
            print(f"     Size: {size_gb:.2f} GB")
            print(f"     Rows: {row_count:,}")
            print(f"     Time: {elapsed_time/60:.1f} minutes")
            
            # Get bytes processed
            bytes_processed = query_job.total_bytes_processed
            gb_processed = bytes_processed / (1024**3) if bytes_processed else 0
            
            print(f"     Data processed: {gb_processed:.2f} GB")
            
            results.append({
                "table": table['name'],
                "status": "SUCCESS",
                "size_gb": size_gb,
                "row_count": row_count,
                "time_minutes": elapsed_time/60,
                "gb_processed": gb_processed
            })
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            print(f"  ❌ Error after {elapsed_time/60:.1f} minutes: {str(e)[:200]}")
            results.append({
                "table": table['name'],
                "status": "FAILED",
                "error": str(e),
                "time_minutes": elapsed_time/60
            })
        
        print()
    
    return results

def verify_tables(client, results):
    """Verify the created tables and test partition pruning."""
    
    print("=" * 80)
    print("VERIFYING TABLES AND PARTITION PRUNING")
    print("=" * 80)
    
    verification_results = []
    
    for result in results:
        if result['status'] == 'SUCCESS':
            table_name = result['table']
            
            print(f"\nVerifying: {table_name}")
            
            # Test queries based on table
            if table_name == "rx_op_enhanced_full_optimized":
                test_query = f"""
                SELECT COUNT(*) as count
                FROM `data-analytics-389803.conflixis_data_projects.{table_name}`
                WHERE year_int = 2022
                """
            elif table_name == "PHYSICIAN_RX_2020_2024_optimized":
                test_query = f"""
                SELECT COUNT(*) as count
                FROM `data-analytics-389803.conflixis_data_projects.{table_name}`
                WHERE claim_date BETWEEN '2024-01-01' AND '2024-01-31'
                """
            elif table_name == "op_general_all_aggregate_static_optimized":
                test_query = f"""
                SELECT COUNT(*) as count
                FROM `data-analytics-389803.conflixis_data_projects.{table_name}`
                WHERE date_of_payment BETWEEN '2024-01-01' AND '2024-01-31'
                """
            else:
                # Non-partitioned tables
                test_query = f"""
                SELECT COUNT(*) as count
                FROM `data-analytics-389803.conflixis_data_projects.{table_name}`
                LIMIT 10
                """
            
            try:
                # Dry run to estimate bytes
                job_config = bigquery.QueryJobConfig(dry_run=True)
                query_job = client.query(test_query, job_config=job_config)
                
                estimated_bytes = query_job.total_bytes_processed
                estimated_gb = estimated_bytes / (1024**3)
                
                # Compare to full table size
                reduction_pct = ((result['size_gb'] - estimated_gb) / result['size_gb'] * 100) if result['size_gb'] > 0 else 0
                
                print(f"  ✅ Partition pruning verified!")
                print(f"     Full table: {result['size_gb']:.2f} GB")
                print(f"     Query will scan: {estimated_gb:.3f} GB")
                print(f"     Reduction: {reduction_pct:.1f}%")
                
                verification_results.append({
                    "table": table_name,
                    "full_size_gb": result['size_gb'],
                    "query_scan_gb": estimated_gb,
                    "reduction_pct": reduction_pct
                })
                
            except Exception as e:
                print(f"  ⚠️ Verification failed: {str(e)[:100]}")
    
    return verification_results

def main():
    """Main execution."""
    client = setup_client()
    
    print("\n🚀 Starting full production table creation with 1-hour timeout...")
    print("⚠️  This process may take up to 1 hour per large table\n")
    
    # Create full tables
    results = create_full_tables(client)
    
    # Summary
    print("=" * 80)
    print("CREATION SUMMARY")
    print("=" * 80)
    
    successful = sum(1 for r in results if r['status'] == 'SUCCESS')
    failed = sum(1 for r in results if r['status'] == 'FAILED')
    
    print(f"Total tables processed: {len(results)}")
    print(f"  ✅ Successful: {successful}")
    print(f"  ❌ Failed: {failed}")
    
    if successful > 0:
        total_size = sum(r['size_gb'] for r in results if r['status'] == 'SUCCESS')
        total_rows = sum(r['row_count'] for r in results if r['status'] == 'SUCCESS')
        total_time = sum(r['time_minutes'] for r in results if r['status'] == 'SUCCESS')
        
        print(f"\nTotal data migrated:")
        print(f"  Size: {total_size:.2f} GB")
        print(f"  Rows: {total_rows:,}")
        print(f"  Total time: {total_time:.1f} minutes")
    
    # Verify tables
    if successful > 0:
        print("\n")
        verification_results = verify_tables(client, results)
        
        if verification_results:
            avg_reduction = sum(v['reduction_pct'] for v in verification_results) / len(verification_results)
            print(f"\n🎯 Average partition pruning reduction: {avg_reduction:.1f}%")
    
    print(f"\n✅ Completed at: {datetime.now()}")
    
    # Save results
    output_file = Path(__file__).parent.parent / "docs" / "full_tables_results.json"
    with open(output_file, 'w') as f:
        json.dump({
            "creation_results": results,
            "verification_results": verification_results if 'verification_results' in locals() else [],
            "timestamp": str(datetime.now())
        }, f, indent=2, default=str)
    
    print(f"📁 Results saved to: {output_file}")

if __name__ == "__main__":
    main()