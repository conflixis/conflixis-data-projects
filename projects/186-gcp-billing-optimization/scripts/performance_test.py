#!/usr/bin/env python3
"""
Performance comparison between original tables (with CAST) and harmonized views (without CAST).
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

def run_performance_tests(client):
    """Run performance comparison tests."""
    
    print("=" * 80)
    print("PERFORMANCE COMPARISON: ORIGINAL vs HARMONIZED")
    print("=" * 80)
    print(f"Test started at: {datetime.now()}\n")
    
    tests = [
        {
            "name": "Test 1: Simple Join",
            "original_query": """
            SELECT 
                COUNT(*) as record_count
            FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full` rx
            JOIN `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW` p
            ON CAST(rx.NPI AS STRING) = CAST(p.NPI AS STRING)
            LIMIT 100
            """,
            "optimized_query": """
            SELECT 
                COUNT(*) as record_count
            FROM `data-analytics-389803.conflixis_data_projects.v_rx_op_enhanced_full_harmonized` rx
            JOIN `data-analytics-389803.conflixis_data_projects.v_PHYSICIANS_OVERVIEW_harmonized` p
            ON rx.NPI = p.NPI
            LIMIT 100
            """
        },
        {
            "name": "Test 2: Join with Aggregation",
            "original_query": """
            SELECT 
                CAST(p.NPI AS STRING) as provider_npi,
                COUNT(*) as payment_count,
                SUM(rx.total_amount) as total_payments
            FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full` rx
            JOIN `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW` p
            ON CAST(rx.NPI AS STRING) = CAST(p.NPI AS STRING)
            WHERE rx.payment_year = 2024
            GROUP BY provider_npi
            LIMIT 100
            """,
            "optimized_query": """
            SELECT 
                p.NPI as provider_npi,
                COUNT(*) as payment_count,
                SUM(rx.total_amount) as total_payments
            FROM `data-analytics-389803.conflixis_data_projects.v_rx_op_enhanced_full_harmonized` rx
            JOIN `data-analytics-389803.conflixis_data_projects.v_PHYSICIANS_OVERVIEW_harmonized` p
            ON rx.NPI = p.NPI
            WHERE rx.payment_year = 2024
            GROUP BY provider_npi
            LIMIT 100
            """
        },
        {
            "name": "Test 3: Three-Table Join",
            "original_query": """
            SELECT 
                COUNT(DISTINCT CAST(rx.NPI AS STRING)) as providers_with_rx_and_facilities
            FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full` rx
            JOIN `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW` p
                ON CAST(rx.NPI AS STRING) = CAST(p.NPI AS STRING)
            JOIN `data-analytics-389803.conflixis_agent.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT` f
                ON CAST(p.NPI AS STRING) = CAST(f.NPI AS STRING)
            WHERE rx.payment_year = 2024
            LIMIT 10
            """,
            "optimized_query": """
            SELECT 
                COUNT(DISTINCT rx.NPI) as providers_with_rx_and_facilities
            FROM `data-analytics-389803.conflixis_data_projects.v_rx_op_enhanced_full_harmonized` rx
            JOIN `data-analytics-389803.conflixis_data_projects.v_PHYSICIANS_OVERVIEW_harmonized` p
                ON rx.NPI = p.NPI
            JOIN `data-analytics-389803.conflixis_data_projects.v_PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_harmonized` f
                ON p.NPI = f.NPI
            WHERE rx.payment_year = 2024
            LIMIT 10
            """
        }
    ]
    
    results = []
    
    for test in tests:
        print(f"\n{test['name']}")
        print("-" * 60)
        
        # Run original query (with CAST)
        print("Running ORIGINAL query (with CAST)...")
        start_time = time.time()
        
        try:
            query_job = client.query(test['original_query'])
            _ = query_job.result()
            original_time = time.time() - start_time
            
            original_stats = {
                "execution_time": original_time,
                "bytes_processed": query_job.total_bytes_processed,
                "bytes_billed": query_job.total_bytes_billed,
                "slot_millis": query_job.slot_millis,
                "cache_hit": query_job.cache_hit
            }
            
            gb_billed = query_job.total_bytes_billed / (1024**3)
            original_cost = gb_billed * 0.005  # $5 per TB
            original_stats["estimated_cost"] = original_cost
            
            print(f"  ⏱️  Time: {original_time:.2f} seconds")
            print(f"  💾 Data: {gb_billed:.3f} GB")
            print(f"  💰 Cost: ${original_cost:.4f}")
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:100]}")
            original_stats = None
            original_time = 0
            original_cost = 0
        
        # Run optimized query (without CAST)
        print("\nRunning OPTIMIZED query (without CAST)...")
        start_time = time.time()
        
        try:
            query_job = client.query(test['optimized_query'])
            _ = query_job.result()
            optimized_time = time.time() - start_time
            
            optimized_stats = {
                "execution_time": optimized_time,
                "bytes_processed": query_job.total_bytes_processed,
                "bytes_billed": query_job.total_bytes_billed,
                "slot_millis": query_job.slot_millis,
                "cache_hit": query_job.cache_hit
            }
            
            gb_billed = query_job.total_bytes_billed / (1024**3)
            optimized_cost = gb_billed * 0.005  # $5 per TB
            optimized_stats["estimated_cost"] = optimized_cost
            
            print(f"  ⏱️  Time: {optimized_time:.2f} seconds")
            print(f"  💾 Data: {gb_billed:.3f} GB")
            print(f"  💰 Cost: ${optimized_cost:.4f}")
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:100]}")
            optimized_stats = None
            optimized_time = 0
            optimized_cost = 0
        
        # Calculate improvements
        if original_stats and optimized_stats:
            time_improvement = ((original_time - optimized_time) / original_time) * 100 if original_time > 0 else 0
            cost_improvement = ((original_cost - optimized_cost) / original_cost) * 100 if original_cost > 0 else 0
            
            print(f"\n🎯 IMPROVEMENTS:")
            print(f"  Time: {time_improvement:.1f}% faster")
            print(f"  Cost: {cost_improvement:.1f}% cheaper")
            
            results.append({
                "test": test['name'],
                "original": original_stats,
                "optimized": optimized_stats,
                "time_improvement": time_improvement,
                "cost_improvement": cost_improvement
            })
    
    # Summary
    print("\n" + "=" * 80)
    print("OVERALL SUMMARY")
    print("=" * 80)
    
    if results:
        avg_time_improvement = sum(r['time_improvement'] for r in results) / len(results)
        avg_cost_improvement = sum(r['cost_improvement'] for r in results) / len(results)
        
        total_original_cost = sum(r['original']['estimated_cost'] for r in results if r['original'])
        total_optimized_cost = sum(r['optimized']['estimated_cost'] for r in results if r['optimized'])
        
        print(f"\n📊 Average Improvements:")
        print(f"  Time: {avg_time_improvement:.1f}% faster")
        print(f"  Cost: {avg_cost_improvement:.1f}% reduction")
        
        print(f"\n💰 Total Test Costs:")
        print(f"  Original (with CAST): ${total_original_cost:.4f}")
        print(f"  Optimized (no CAST): ${total_optimized_cost:.4f}")
        print(f"  Savings: ${total_original_cost - total_optimized_cost:.4f}")
        
        # Extrapolate to daily usage
        if total_original_cost > 0:
            daily_queries = 22  # From analysis
            daily_original = total_original_cost * (daily_queries / len(tests))
            daily_optimized = total_optimized_cost * (daily_queries / len(tests))
            
            print(f"\n📈 Projected Daily Impact (based on 22 queries/day):")
            print(f"  Original: ${daily_original:.2f}/day")
            print(f"  Optimized: ${daily_optimized:.2f}/day")
            print(f"  Daily Savings: ${daily_original - daily_optimized:.2f}")
    
    print(f"\nTest completed at: {datetime.now()}")
    
    return results

def main():
    """Main execution."""
    client = setup_client()
    results = run_performance_tests(client)
    
    # Save results to file
    output_file = Path(__file__).parent.parent / "docs" / "performance_test_results.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n📁 Results saved to: {output_file}")

if __name__ == "__main__":
    main()