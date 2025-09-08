#!/usr/bin/env python3
"""
Test partitioning effectiveness by comparing scanned data.
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

def test_partitioning(client):
    """Compare partitioned vs non-partitioned query performance."""
    
    print("=" * 80)
    print("PARTITION PRUNING EFFECTIVENESS TEST")
    print("=" * 80)
    print(f"Test started at: {datetime.now()}\n")
    
    tests = [
        {
            "name": "Test 1: Full table scan (no partition)",
            "description": "Query original table with year filter",
            "query": """
            SELECT 
                COUNT(*) as record_count,
                COUNT(DISTINCT NPI) as unique_npis
            FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
            WHERE CAST(year AS INT64) = 2022
            """
        },
        {
            "name": "Test 2: Partition pruning (optimized)",
            "description": "Query partitioned table - should scan only 2022 partition",
            "query": """
            SELECT 
                COUNT(*) as record_count,
                COUNT(DISTINCT NPI) as unique_npis
            FROM `data-analytics-389803.conflixis_data_projects.rx_op_enhanced_full_optimized_sample`
            WHERE year_int = 2022
            """
        },
        {
            "name": "Test 3: Clustering benefit",
            "description": "Query with NPI filter to test clustering",
            "query": """
            SELECT 
                COUNT(*) as record_count
            FROM `data-analytics-389803.conflixis_data_projects.rx_op_enhanced_full_optimized_sample`
            WHERE year_int = 2022
                AND NPI IN (1003000126, 1003000134, 1003000142)
            """
        }
    ]
    
    results = []
    
    for test in tests:
        print(f"{test['name']}")
        print(f"  {test['description']}")
        print("-" * 60)
        
        try:
            # Dry run first to get statistics
            job_config = bigquery.QueryJobConfig(dry_run=True)
            query_job = client.query(test['query'], job_config=job_config)
            
            estimated_bytes = query_job.total_bytes_processed
            estimated_gb = estimated_bytes / (1024**3)
            estimated_cost = estimated_gb * 0.005  # $5 per TB
            
            print(f"  📊 Estimated scan: {estimated_gb:.3f} GB (${estimated_cost:.4f})")
            
            # Now run the actual query
            start_time = time.time()
            query_job = client.query(test['query'])
            result = list(query_job.result())
            elapsed = time.time() - start_time
            
            actual_bytes = query_job.total_bytes_billed
            actual_gb = actual_bytes / (1024**3)
            actual_cost = actual_gb * 0.005
            
            print(f"  ✅ Actual scan: {actual_gb:.3f} GB (${actual_cost:.4f})")
            print(f"  ⏱️  Query time: {elapsed:.2f} seconds")
            
            # Show results
            for row in result:
                if hasattr(row, 'record_count'):
                    print(f"  📈 Records: {row.record_count:,}")
                if hasattr(row, 'unique_npis'):
                    print(f"  👥 Unique NPIs: {row.unique_npis:,}")
            
            results.append({
                "test": test['name'],
                "estimated_gb": estimated_gb,
                "actual_gb": actual_gb,
                "cost": actual_cost,
                "time_seconds": elapsed
            })
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:200]}")
            results.append({
                "test": test['name'],
                "error": str(e)[:200]
            })
        
        print()
    
    # Summary comparison
    if len(results) >= 2 and 'actual_gb' in results[0] and 'actual_gb' in results[1]:
        print("=" * 80)
        print("PARTITIONING IMPACT SUMMARY")
        print("=" * 80)
        
        original_gb = results[0]['actual_gb']
        partitioned_gb = results[1]['actual_gb']
        
        if original_gb > 0:
            reduction_pct = ((original_gb - partitioned_gb) / original_gb) * 100
            cost_savings = results[0]['cost'] - results[1]['cost']
            
            print(f"\n📊 Data Scanned:")
            print(f"  Original table: {original_gb:.3f} GB")
            print(f"  Partitioned table: {partitioned_gb:.3f} GB")
            print(f"  Reduction: {reduction_pct:.1f}%")
            
            print(f"\n💰 Cost Impact:")
            print(f"  Original query: ${results[0]['cost']:.4f}")
            print(f"  Partitioned query: ${results[1]['cost']:.4f}")
            print(f"  Savings per query: ${cost_savings:.4f}")
            
            # Extrapolate to daily usage
            queries_per_day = 22  # From our analysis
            daily_savings = cost_savings * queries_per_day
            
            print(f"\n📈 Projected Daily Impact (22 queries/day):")
            print(f"  Daily savings: ${daily_savings:.2f}")
            print(f"  Monthly savings: ${daily_savings * 30:.2f}")
            print(f"  Annual savings: ${daily_savings * 365:,.2f}")
    
    print(f"\n✅ Test completed at: {datetime.now()}")
    
    return results

def main():
    """Main execution."""
    client = setup_client()
    results = test_partitioning(client)
    
    # Save results
    output_file = Path(__file__).parent.parent / "docs" / "partitioning_test_results.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n📁 Results saved to: {output_file}")

if __name__ == "__main__":
    main()