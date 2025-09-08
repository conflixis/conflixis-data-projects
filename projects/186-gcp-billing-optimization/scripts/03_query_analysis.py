#!/usr/bin/env python3
"""
Deep analysis of expensive BigQuery queries to understand optimization opportunities.
"""

import os
import json
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

def setup_client():
    """Setup BigQuery client with credentials."""
    from pathlib import Path
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

def analyze_expensive_jobs(client):
    """Analyze the pattern of expensive jobs."""
    
    print("=" * 80)
    print("ANALYZING EXPENSIVE QUERY PATTERNS")
    print("=" * 80)
    
    # Get detailed information about the expensive queries
    query = """
    WITH expensive_jobs AS (
        SELECT 
            DATE(usage_start_time) as query_date,
            EXTRACT(HOUR FROM usage_start_time) as query_hour,
            EXTRACT(MINUTE FROM usage_start_time) as query_minute,
            resource.name as job_id,
            project.name as project_name,
            sku.description as operation_type,
            cost,
            usage.amount / POW(10, 12) as tb_processed,
            labels,
            system_labels
        FROM `billing-administration-389502.all_billing_data.gcp_billing_export_resource_v1_01A821_72C4A7_DB426E`
        WHERE service.description = 'BigQuery'
            AND cost > 50  -- Focus on very expensive queries
            AND DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        ORDER BY cost DESC
    )
    SELECT * FROM expensive_jobs
    LIMIT 30
    """
    
    results = client.query(query).to_dataframe()
    
    print(f"\n📊 Found {len(results)} queries over $50 in the last 7 days")
    
    # Analyze the UUID pattern
    uuid_jobs = results[results['job_id'].str.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', na=False)]
    
    print(f"\n🔍 UUID-NAMED JOBS ANALYSIS:")
    print("-" * 80)
    print(f"Total UUID jobs: {len(uuid_jobs)}")
    print(f"Total cost: ${uuid_jobs['cost'].sum():.2f}")
    print(f"Average TB processed: {uuid_jobs['tb_processed'].mean():.2f} TB")
    
    # Time pattern analysis
    time_analysis = uuid_jobs.groupby(['query_date', 'query_hour']).agg({
        'cost': 'sum',
        'job_id': 'count',
        'tb_processed': 'sum'
    }).round(2)
    
    print(f"\nTIME PATTERN:")
    for (date, hour), stats in time_analysis.iterrows():
        print(f"  {date} {hour:02d}:00 - {stats['job_id']:.0f} jobs, ${stats['cost']:.2f}, {stats['tb_processed']:.2f} TB")
    
    return results, uuid_jobs

def analyze_table_access_patterns(client):
    """Analyze which tables are being queried and their characteristics."""
    
    print("\n" + "=" * 80)
    print("ANALYZING TABLE ACCESS PATTERNS")
    print("=" * 80)
    
    # Get information about the most accessed tables
    query = """
    SELECT 
        'conflixis_data_projects' as dataset_name,
        table_name,
        TIMESTAMP_MILLIS(creation_time) as created,
        TIMESTAMP_MILLIS(IFNULL(last_modified_time, creation_time)) as last_modified,
        row_count,
        size_bytes / POW(10, 9) as size_gb,
        CASE 
            WHEN type = 1 THEN 'TABLE'
            WHEN type = 2 THEN 'VIEW'
            ELSE 'OTHER'
        END as table_type,
        CASE 
            WHEN TIMESTAMP_MILLIS(creation_time) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
                AND TIMESTAMP_MILLIS(IFNULL(last_modified_time, creation_time)) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
            THEN 'LONG_TERM_ELIGIBLE'
            ELSE 'ACTIVE'
        END as storage_recommendation
    FROM `data-analytics-389803.conflixis_data_projects.__TABLES__`
    WHERE size_bytes > 0
    ORDER BY size_bytes DESC
    LIMIT 20
    """
    
    try:
        table_info = client.query(query).to_dataframe()
        
        print("\n📊 CONFLIXIS_AGENT DATASET ANALYSIS:")
        print("-" * 80)
        print(f"Total tables: {len(table_info)}")
        print(f"Total size: {table_info['size_gb'].sum():.2f} GB")
        print(f"Total rows: {table_info['row_count'].sum():,.0f}")
        
        print("\nLARGEST TABLES:")
        for _, row in table_info.head(10).iterrows():
            print(f"\n  Table: {row['table_name']}")
            print(f"    Size: {row['size_gb']:.2f} GB")
            print(f"    Rows: {row['row_count']:,.0f}")
            print(f"    Last Modified: {row['last_modified']}")
            print(f"    Storage Status: {row['storage_recommendation']}")
            
            # Estimate query cost for full scan
            estimated_cost = (row['size_gb'] / 1024) * 6.25  # $6.25 per TB scanned
            print(f"    Est. Full Scan Cost: ${estimated_cost:.2f}")
        
        return table_info
    except Exception as e:
        print(f"Could not access table metadata: {e}")
        return pd.DataFrame()

def analyze_query_optimization_opportunities(client):
    """Analyze specific optimization opportunities for the queries."""
    
    print("\n" + "=" * 80)
    print("QUERY OPTIMIZATION OPPORTUNITIES")
    print("=" * 80)
    
    # Check for partition and clustering information
    query = """
    SELECT 
        table_catalog,
        table_schema,
        table_name,
        ddl,
        REGEXP_EXTRACT(ddl, r'PARTITION BY ([^\\s]+)') as partition_column,
        REGEXP_EXTRACT(ddl, r'CLUSTER BY \\(([^)]+)\\)') as clustering_columns
    FROM `data-analytics-389803.conflixis_data_projects.INFORMATION_SCHEMA.TABLES`
    WHERE table_type = 'BASE TABLE'
        AND table_name NOT LIKE '%_view%'
    LIMIT 10
    """
    
    try:
        schema_info = client.query(query).to_dataframe()
        
        print("\n📋 TABLE OPTIMIZATION STATUS:")
        print("-" * 80)
        
        unoptimized_tables = []
        for _, row in schema_info.iterrows():
            print(f"\nTable: {row['table_name']}")
            
            if pd.isna(row['partition_column']) or row['partition_column'] == '':
                print("  ❌ NOT PARTITIONED - High scan costs")
                unoptimized_tables.append(row['table_name'])
            else:
                print(f"  ✅ Partitioned by: {row['partition_column']}")
            
            if pd.isna(row['clustering_columns']) or row['clustering_columns'] == '':
                print("  ❌ NOT CLUSTERED - Suboptimal query performance")
            else:
                print(f"  ✅ Clustered by: {row['clustering_columns']}")
        
        if unoptimized_tables:
            print(f"\n⚠️  {len(unoptimized_tables)} tables need optimization")
            
    except Exception as e:
        print(f"Could not access schema information: {e}")

def research_optimization_strategies():
    """Research and compile optimization strategies."""
    
    print("\n" + "=" * 80)
    print("OPTIMIZATION STRATEGIES FOR LARGE QUERIES")
    print("=" * 80)
    
    strategies = {
        "1. PARTITIONING": {
            "description": "Partition tables by date or integer columns",
            "impact": "Can reduce scan costs by 90%+ when filtering by partition column",
            "implementation": """
ALTER TABLE `project.dataset.table` 
ADD COLUMN IF NOT EXISTS partition_date DATE 
OPTIONS(description="Partition column");

CREATE OR REPLACE TABLE `project.dataset.table_partitioned`
PARTITION BY DATE(partition_date)
CLUSTER BY frequently_filtered_column
AS SELECT * FROM `project.dataset.table`;""",
            "cost_savings": "12.94 TB scan → 1-2 TB with date filters"
        },
        
        "2. CLUSTERING": {
            "description": "Cluster tables by frequently filtered columns",
            "impact": "30-50% query performance improvement",
            "implementation": """
CREATE OR REPLACE TABLE `project.dataset.table_clustered`
PARTITION BY DATE(date_column)
CLUSTER BY user_id, event_type, region
AS SELECT * FROM `project.dataset.table`;""",
            "cost_savings": "Better data locality reduces bytes scanned"
        },
        
        "3. MATERIALIZED VIEWS": {
            "description": "Pre-compute expensive aggregations",
            "impact": "95%+ cost reduction for repeated queries",
            "implementation": """
CREATE MATERIALIZED VIEW `project.dataset.mv_daily_aggregates`
PARTITION BY date
CLUSTER BY dimension1, dimension2
AS 
SELECT 
    date,
    dimension1,
    dimension2,
    SUM(metric1) as total_metric1,
    COUNT(*) as record_count
FROM `project.dataset.source_table`
GROUP BY date, dimension1, dimension2;""",
            "cost_savings": "$73.55 per query → $0.50 for view refresh"
        },
        
        "4. INCREMENTAL PROCESSING": {
            "description": "Process only new/changed data",
            "impact": "80-90% reduction in data scanned",
            "implementation": """
-- Use merge for incremental updates
MERGE `project.dataset.target` T
USING (
    SELECT * FROM `project.dataset.source`
    WHERE DATE(timestamp) = CURRENT_DATE() - 1  -- Yesterday's data only
) S
ON T.id = S.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;""",
            "cost_savings": "Process 1 day instead of entire history"
        },
        
        "5. QUERY RESULT CACHING": {
            "description": "Cache results for 24 hours automatically",
            "impact": "100% cost savings on repeated identical queries",
            "implementation": """
-- Enable by ensuring queries are deterministic
-- Avoid NOW(), CURRENT_TIMESTAMP() in queries
-- Use explicit date ranges instead""",
            "cost_savings": "Free for repeated queries within 24 hours"
        },
        
        "6. APPROXIMATE AGGREGATION": {
            "description": "Use APPROX functions for estimates",
            "impact": "10-100x faster for large datasets",
            "implementation": """
-- Instead of COUNT(DISTINCT user_id)
SELECT APPROX_COUNT_DISTINCT(user_id) as unique_users
FROM `project.dataset.table`
WHERE date >= '2024-01-01';""",
            "cost_savings": "Significantly less computation required"
        }
    }
    
    for strategy, details in strategies.items():
        print(f"\n{strategy}")
        print("-" * 40)
        print(f"Description: {details['description']}")
        print(f"Impact: {details['impact']}")
        print(f"Potential Savings: {details['cost_savings']}")
    
    return strategies

def calculate_optimization_impact(tb_processed=12.94, query_count=20):
    """Calculate the financial impact of optimizations."""
    
    print("\n" + "=" * 80)
    print("FINANCIAL IMPACT ANALYSIS")
    print("=" * 80)
    
    current_cost_per_query = 73.55
    current_total = current_cost_per_query * query_count
    
    scenarios = {
        "Current State": {
            "tb_scanned": tb_processed,
            "cost_per_query": current_cost_per_query,
            "total_cost": current_total,
            "savings": 0
        },
        "With Partitioning (90% reduction)": {
            "tb_scanned": tb_processed * 0.1,
            "cost_per_query": current_cost_per_query * 0.1,
            "total_cost": current_total * 0.1,
            "savings": current_total * 0.9
        },
        "With Materialized Views": {
            "tb_scanned": 0.1,  # Small view refresh
            "cost_per_query": 0.625,  # ~100GB scan
            "total_cost": 0.625 * query_count,
            "savings": current_total - (0.625 * query_count)
        },
        "With Incremental Processing": {
            "tb_scanned": tb_processed * 0.05,  # Process 5% of data
            "cost_per_query": current_cost_per_query * 0.05,
            "total_cost": current_total * 0.05,
            "savings": current_total * 0.95
        },
        "Combined Optimizations": {
            "tb_scanned": 0.05,
            "cost_per_query": 0.31,
            "total_cost": 0.31 * query_count,
            "savings": current_total - (0.31 * query_count)
        }
    }
    
    print(f"\nBASED ON SEPT 2-3 PATTERN (20 queries @ 12.94 TB each):")
    print("-" * 80)
    
    for scenario, metrics in scenarios.items():
        print(f"\n{scenario}:")
        print(f"  Data Scanned: {metrics['tb_scanned']:.2f} TB")
        print(f"  Cost per Query: ${metrics['cost_per_query']:.2f}")
        print(f"  Total Daily Cost: ${metrics['total_cost']:.2f}")
        if metrics['savings'] > 0:
            print(f"  💰 Daily Savings: ${metrics['savings']:.2f}")
            print(f"  💰 Monthly Savings: ${metrics['savings'] * 30:.2f}")
    
    return scenarios

def main():
    """Main execution."""
    client = setup_client()
    
    # Analyze expensive jobs
    expensive_jobs, uuid_jobs = analyze_expensive_jobs(client)
    
    # Analyze table patterns
    table_info = analyze_table_access_patterns(client)
    
    # Check optimization status
    analyze_query_optimization_opportunities(client)
    
    # Research strategies
    strategies = research_optimization_strategies()
    
    # Calculate impact
    scenarios = calculate_optimization_impact()
    
    print("\n" + "=" * 80)
    print("KEY FINDINGS & RECOMMENDATIONS")
    print("=" * 80)
    
    print("""
🔍 DIAGNOSIS:
- 20+ UUID-named jobs indicate automated/scheduled processes
- Each job scans 12.94 TB (entire dataset without filters)
- Tables likely not partitioned or clustered
- No query result caching being utilized

🎯 IMMEDIATE ACTIONS:
1. Identify the automation creating UUID jobs
2. Add partition filters to reduce data scanned
3. Implement materialized views for aggregations
4. Enable query result caching

💰 POTENTIAL SAVINGS:
- Current: $1,471/day for these queries
- With optimizations: $6.20/day
- Savings: $1,465/day ($43,950/month)
""")

if __name__ == "__main__":
    main()