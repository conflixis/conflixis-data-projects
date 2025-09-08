#!/usr/bin/env python3
"""
Create sample partitioned tables for testing (with limited data).
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

def create_sample_tables(client):
    """Create sample partitioned tables for quick testing."""
    
    print("=" * 80)
    print("CREATING SAMPLE PARTITIONED TABLES (LIMITED DATA)")
    print("=" * 80)
    print(f"Started at: {datetime.now()}\n")
    
    # First, let's just create the rx_op_enhanced_full table with 2022 data as a test
    print("Creating sample: rx_op_enhanced_full_optimized_sample")
    print("  Filtering to year 2022 only for quick test...")
    
    create_sql = """
    CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.rx_op_enhanced_full_optimized_sample`
    PARTITION BY RANGE_BUCKET(year_int, GENERATE_ARRAY(2015, 2030, 1))
    CLUSTER BY NPI, SPECIALTY_PRIMARY
    AS 
    SELECT 
        CAST(NPI AS INT64) as NPI,
        CAST(year AS INT64) as year_int,
        * EXCEPT(NPI, year)
    FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
    WHERE CAST(year AS INT64) = 2022
    LIMIT 1000000
    """
    
    try:
        start_time = time.time()
        query_job = client.query(create_sql)
        query_job.result()
        elapsed = time.time() - start_time
        
        # Get table info
        table_ref = client.dataset('conflixis_data_projects').table('rx_op_enhanced_full_optimized_sample')
        table = client.get_table(table_ref)
        
        print(f"  ✅ Sample table created!")
        print(f"     Rows: {table.num_rows:,}")
        print(f"     Size: {table.num_bytes / (1024**3):.3f} GB")
        print(f"     Time: {elapsed:.1f} seconds")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error: {str(e)[:200]}")
        return False

def test_partition_performance(client):
    """Test performance difference between partitioned and non-partitioned queries."""
    
    print("\n" + "=" * 80)
    print("TESTING PARTITION PRUNING PERFORMANCE")
    print("=" * 80)
    
    tests = [
        {
            "name": "Query WITHOUT partition (scans all data)",
            "query": """
            SELECT 
                COUNT(*) as count,
                SUM(total_amount) as total
            FROM `data-analytics-389803.conflixis_data_projects.v_rx_op_enhanced_full_harmonized`
            WHERE CAST(year AS INT64) = 2022
            LIMIT 10
            """
        },
        {
            "name": "Query WITH partition (scans only 2022 partition)",
            "query": """
            SELECT 
                COUNT(*) as count,
                SUM(total_amount) as total
            FROM `data-analytics-389803.conflixis_data_projects.rx_op_enhanced_full_optimized_sample`
            WHERE year_int = 2022
            LIMIT 10
            """
        }
    ]
    
    for test in tests:
        print(f"\n{test['name']}")
        print("-" * 60)
        
        try:
            start_time = time.time()
            query_job = client.query(test['query'])
            results = list(query_job.result())
            elapsed = time.time() - start_time
            
            gb_processed = query_job.total_bytes_processed / (1024**3)
            gb_billed = query_job.total_bytes_billed / (1024**3)
            cost = gb_billed * 0.005  # $5 per TB
            
            print(f"  ⏱️  Time: {elapsed:.2f} seconds")
            print(f"  💾 Processed: {gb_processed:.3f} GB")
            print(f"  💵 Billed: {gb_billed:.3f} GB")
            print(f"  💰 Cost: ${cost:.4f}")
            
            for row in results:
                print(f"  📊 Results: count={row.count:,}, total=${row.total:,.2f}")
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:100]}")

def main():
    """Main execution."""
    client = setup_client()
    
    # Create sample table
    success = create_sample_tables(client)
    
    if success:
        # Test performance
        test_partition_performance(client)
    
    print(f"\n✅ Completed at: {datetime.now()}")

if __name__ == "__main__":
    main()