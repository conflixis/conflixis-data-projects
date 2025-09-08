#!/usr/bin/env python3
"""
Fix issues and create remaining partitioned tables.
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
    service_account_json = service_account_json.replace('\\\\n', '\\n')
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return bigquery.Client(credentials=credentials, project="data-analytics-389803")

def check_columns(client, table_name):
    """Check available columns in a table."""
    query = f"""
    SELECT column_name
    FROM `data-analytics-389803.conflixis_agent.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position
    """
    query_job = client.query(query)
    return [row.column_name for row in query_job.result()]

def fix_and_create_tables(client):
    """Fix issues and create remaining tables."""
    
    print("=" * 80)
    print("FIXING AND CREATING REMAINING TABLES")
    print("=" * 80)
    print(f"Started at: {datetime.now()}\n")
    
    # First, check columns in PHYSICIANS_OVERVIEW
    print("Checking columns in PHYSICIANS_OVERVIEW...")
    columns = check_columns(client, 'PHYSICIANS_OVERVIEW')
    print(f"Found {len(columns)} columns")
    
    # Find suitable clustering columns
    clustering_cols = []
    if 'NPI' in columns:
        clustering_cols.append('NPI')
    if 'SPECIALTY_PRIMARY' in columns:
        clustering_cols.append('SPECIALTY_PRIMARY')
    elif 'SPECIALTY' in columns:
        clustering_cols.append('SPECIALTY')
    if 'HQ_STATE' in columns:
        clustering_cols.append('HQ_STATE')
    elif 'STATE' in columns:
        clustering_cols.append('STATE')
    
    print(f"Will use clustering columns: {', '.join(clustering_cols)}\n")
    
    tables = [
        {
            "name": "rx_op_enhanced_full_optimized",
            "description": "Full production table with year partitioning and NPI clustering",
            "drop_first": True,
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
            "description": "Full production table (processing in chunks due to size)",
            "sql": """
            CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.PHYSICIAN_RX_2020_2024_optimized`
            PARTITION BY claim_date
            CLUSTER BY NPI, BRAND_NAME
            AS 
            SELECT 
                CAST(NPI AS INT64) as NPI,
                * EXCEPT(NPI)
            FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024`
            WHERE claim_date IS NOT NULL
                AND claim_date >= '2020-01-01'
            """,
            "timeout_ms": 3600000  # 1 hour
        },
        {
            "name": "PHYSICIANS_OVERVIEW_optimized",
            "description": "Full production table with clustering",
            "sql": f"""
            CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.PHYSICIANS_OVERVIEW_optimized`
            CLUSTER BY {', '.join(clustering_cols)}
            AS 
            SELECT 
                CAST(NPI AS INT64) as NPI,
                * EXCEPT(NPI)
            FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW`
            """,
            "timeout_ms": 1800000  # 30 minutes
        }
    ]
    
    results = []
    
    for table in tables:
        print(f"Creating table: {table['name']}")
        print(f"  Description: {table['description']}")
        
        # Drop table first if needed
        if table.get('drop_first'):
            try:
                print(f"  Dropping existing table first...")
                drop_query = f"DROP TABLE IF EXISTS `data-analytics-389803.conflixis_data_projects.{table['name']}`"
                query_job = client.query(drop_query)
                query_job.result()
                print(f"  ✓ Dropped existing table")
            except Exception as e:
                print(f"  ⚠️ Could not drop table: {str(e)[:100]}")
        
        start_time = time.time()
        
        try:
            # Configure job with extended timeout and higher limits
            job_config = bigquery.QueryJobConfig(
                use_legacy_sql=False,
                priority=bigquery.QueryPriority.INTERACTIVE,
                maximum_bytes_billed=50 * 1024 ** 4  # 50 TB limit
            )
            
            # Create the table
            print(f"  ⏳ Creating table (timeout: {table['timeout_ms']/1000/60:.0f} minutes)...")
            query_job = client.query(table['sql'], job_config=job_config)
            
            # Wait for completion
            query_job.result(timeout=table['timeout_ms']/1000)
            
            elapsed_time = time.time() - start_time
            
            # Get table info
            table_ref = client.dataset('conflixis_data_projects').table(table['name'])
            table_obj = client.get_table(table_ref)
            
            size_gb = table_obj.num_bytes / (1024**3) if table_obj.num_bytes else 0
            row_count = table_obj.num_rows
            
            print(f"  ✅ Success!")
            print(f"     Size: {size_gb:.2f} GB")
            print(f"     Rows: {row_count:,}")
            print(f"     Time: {elapsed_time/60:.1f} minutes")
            
            results.append({
                "table": table['name'],
                "status": "SUCCESS",
                "size_gb": size_gb,
                "row_count": row_count,
                "time_minutes": elapsed_time/60
            })
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            print(f"  ❌ Error after {elapsed_time/60:.1f} minutes: {str(e)[:300]}")
            results.append({
                "table": table['name'],
                "status": "FAILED",
                "error": str(e),
                "time_minutes": elapsed_time/60
            })
        
        print()
    
    return results

def main():
    """Main execution."""
    client = setup_client()
    
    # Create remaining tables
    results = fix_and_create_tables(client)
    
    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    successful = sum(1 for r in results if r['status'] == 'SUCCESS')
    failed = sum(1 for r in results if r['status'] == 'FAILED')
    
    print(f"Tables processed: {len(results)}")
    print(f"  ✅ Successful: {successful}")
    print(f"  ❌ Failed: {failed}")
    
    if successful > 0:
        total_size = sum(r['size_gb'] for r in results if r['status'] == 'SUCCESS')
        total_rows = sum(r['row_count'] for r in results if r['status'] == 'SUCCESS')
        
        print(f"\nNew data migrated:")
        print(f"  Size: {total_size:.2f} GB")
        print(f"  Rows: {total_rows:,}")
    
    print(f"\n✅ Completed at: {datetime.now()}")
    
    # Update results file
    output_file = Path(__file__).parent.parent / "docs" / "full_tables_results.json"
    
    # Load existing results if file exists
    existing_results = {}
    if output_file.exists():
        with open(output_file, 'r') as f:
            existing_results = json.load(f)
    
    # Merge results
    all_results = existing_results.get('creation_results', [])
    for new_result in results:
        # Replace or add
        found = False
        for i, existing in enumerate(all_results):
            if existing['table'] == new_result['table']:
                all_results[i] = new_result
                found = True
                break
        if not found:
            all_results.append(new_result)
    
    # Save updated results
    with open(output_file, 'w') as f:
        json.dump({
            "creation_results": all_results,
            "last_update": str(datetime.now())
        }, f, indent=2, default=str)
    
    print(f"📁 Results updated in: {output_file}")

if __name__ == "__main__":
    main()