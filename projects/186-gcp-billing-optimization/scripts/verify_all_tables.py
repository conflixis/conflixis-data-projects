#!/usr/bin/env python3
"""
Verify all optimized tables and test partition pruning effectiveness.
"""

import os
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
    service_account_json = service_account_json.replace('\\\\n', '\\n')
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return bigquery.Client(credentials=credentials, project="data-analytics-389803")

def verify_all_tables(client):
    """Verify all created tables and test partition pruning."""
    
    print("=" * 80)
    print("VERIFYING ALL OPTIMIZED TABLES")
    print("=" * 80)
    print(f"Started at: {datetime.now()}\n")
    
    tables = [
        {
            "name": "rx_op_enhanced_full_optimized",
            "test_query": """
            SELECT COUNT(*) as count, AVG(total_amount) as avg_amount
            FROM `data-analytics-389803.conflixis_data_projects.rx_op_enhanced_full_optimized`
            WHERE year_int = 2022
            """,
            "description": "Year-partitioned table (testing 2022 partition)"
        },
        {
            "name": "PHYSICIAN_RX_2020_2024_optimized",
            "test_query": """
            SELECT COUNT(*) as count
            FROM `data-analytics-389803.conflixis_data_projects.PHYSICIAN_RX_2020_2024_optimized`
            WHERE claim_date BETWEEN '2024-01-01' AND '2024-01-31'
            """,
            "description": "Date-partitioned table (testing Jan 2024)"
        },
        {
            "name": "op_general_all_aggregate_static_optimized",
            "test_query": """
            SELECT COUNT(*) as count, SUM(amount_of_payment_usdollars) as total
            FROM `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized`
            WHERE date_of_payment BETWEEN '2024-01-01' AND '2024-01-31'
            """,
            "description": "Date-partitioned table (testing Jan 2024)"
        },
        {
            "name": "PHYSICIANS_OVERVIEW_optimized",
            "test_query": """
            SELECT COUNT(*) as count
            FROM `data-analytics-389803.conflixis_data_projects.PHYSICIANS_OVERVIEW_optimized`
            WHERE NPI IN (1003000126, 1003000134, 1003000142)
            """,
            "description": "Clustered table (testing NPI clustering)"
        },
        {
            "name": "PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized",
            "test_query": """
            SELECT COUNT(*) as count
            FROM `data-analytics-389803.conflixis_data_projects.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized`
            WHERE NPI = 1003000126
            """,
            "description": "Clustered table (testing NPI clustering)"
        }
    ]
    
    results = []
    total_original_size = 0
    total_scanned_size = 0
    
    for table in tables:
        print(f"Table: {table['name']}")
        print(f"  Description: {table['description']}")
        
        try:
            # Get table size
            table_ref = client.dataset('conflixis_data_projects').table(table['name'])
            table_obj = client.get_table(table_ref)
            
            table_size_gb = table_obj.num_bytes / (1024**3) if table_obj.num_bytes else 0
            row_count = table_obj.num_rows
            
            print(f"  Table info:")
            print(f"    Size: {table_size_gb:.2f} GB")
            print(f"    Rows: {row_count:,}")
            
            # Partitioning info
            if table_obj.time_partitioning:
                print(f"    Partitioned by: {table_obj.time_partitioning.field}")
            elif table_obj.range_partitioning:
                print(f"    Range partitioned by: {table_obj.range_partitioning.field}")
            
            # Clustering info
            if table_obj.clustering_fields:
                print(f"    Clustered by: {', '.join(table_obj.clustering_fields)}")
            
            # Test query with dry run
            job_config = bigquery.QueryJobConfig(dry_run=True)
            query_job = client.query(table['test_query'], job_config=job_config)
            
            estimated_bytes = query_job.total_bytes_processed
            estimated_gb = estimated_bytes / (1024**3)
            estimated_cost = estimated_gb * 0.005  # $5 per TB
            
            # Calculate reduction
            reduction_pct = ((table_size_gb - estimated_gb) / table_size_gb * 100) if table_size_gb > 0 else 0
            
            print(f"  Query test (dry run):")
            print(f"    Will scan: {estimated_gb:.3f} GB")
            print(f"    Estimated cost: ${estimated_cost:.4f}")
            print(f"    Reduction: {reduction_pct:.1f}%")
            
            # Actually run the query to get timing
            import time
            start_time = time.time()
            query_job = client.query(table['test_query'])
            result = list(query_job.result())
            elapsed = time.time() - start_time
            
            actual_bytes = query_job.total_bytes_billed
            actual_gb = actual_bytes / (1024**3)
            
            print(f"  Query execution:")
            print(f"    Time: {elapsed:.2f} seconds")
            print(f"    Actual scan: {actual_gb:.3f} GB")
            
            if result:
                row = result[0]
                if hasattr(row, 'count'):
                    print(f"    Records found: {row.count:,}")
            
            results.append({
                "table": table['name'],
                "size_gb": table_size_gb,
                "rows": row_count,
                "scanned_gb": actual_gb,
                "reduction_pct": reduction_pct,
                "query_time_sec": elapsed
            })
            
            total_original_size += table_size_gb
            total_scanned_size += actual_gb
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:200]}")
            results.append({
                "table": table['name'],
                "error": str(e)[:200]
            })
        
        print()
    
    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    successful_tables = [r for r in results if 'error' not in r]
    
    if successful_tables:
        print(f"\n📊 Total Statistics:")
        print(f"  Tables verified: {len(successful_tables)}")
        print(f"  Total table size: {total_original_size:.2f} GB")
        print(f"  Total data scanned in tests: {total_scanned_size:.3f} GB")
        
        avg_reduction = sum(r['reduction_pct'] for r in successful_tables) / len(successful_tables)
        print(f"  Average reduction: {avg_reduction:.1f}%")
        
        # Calculate cost impact
        print(f"\n💰 Cost Impact:")
        print(f"  If querying full tables: ${(total_original_size * 0.005):.2f}")
        print(f"  With optimization: ${(total_scanned_size * 0.005):.4f}")
        print(f"  Savings: ${((total_original_size - total_scanned_size) * 0.005):.2f}")
        
        # Project to daily usage (22 queries/day from analysis)
        queries_per_day = 22
        daily_original_cost = (total_original_size * 0.005) * queries_per_day / len(successful_tables)
        daily_optimized_cost = (total_scanned_size * 0.005) * queries_per_day / len(successful_tables)
        
        print(f"\n📈 Projected Daily Impact (22 queries/day):")
        print(f"  Original daily cost: ${daily_original_cost:.2f}")
        print(f"  Optimized daily cost: ${daily_optimized_cost:.2f}")
        print(f"  Daily savings: ${(daily_original_cost - daily_optimized_cost):.2f}")
        print(f"  Annual savings: ${((daily_original_cost - daily_optimized_cost) * 365):,.2f}")
    
    print(f"\n✅ Completed at: {datetime.now()}")
    
    # Save results
    output_file = Path(__file__).parent.parent / "docs" / "verification_results.json"
    with open(output_file, 'w') as f:
        json.dump({
            "verification_results": results,
            "summary": {
                "total_tables": len(successful_tables),
                "total_size_gb": total_original_size,
                "total_scanned_gb": total_scanned_size,
                "avg_reduction_pct": avg_reduction if successful_tables else 0
            },
            "timestamp": str(datetime.now())
        }, f, indent=2, default=str)
    
    print(f"\n📁 Results saved to: {output_file}")

def main():
    """Main execution."""
    client = setup_client()
    verify_all_tables(client)

if __name__ == "__main__":
    main()