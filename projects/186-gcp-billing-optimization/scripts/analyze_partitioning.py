#!/usr/bin/env python3
"""
Analyze tables to identify optimal partitioning and clustering columns.
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
    if not service_account_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
    
    service_account_json = service_account_json.replace('\\\\n', '\\n')
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return bigquery.Client(credentials=credentials, project="data-analytics-389803")

def analyze_date_columns(client):
    """Identify date/timestamp columns for partitioning."""
    
    print("=" * 80)
    print("ANALYZING DATE COLUMNS FOR PARTITIONING")
    print("=" * 80)
    
    tables = [
        'PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT',
        'PHYSICIANS_OVERVIEW',
        'rx_op_enhanced_full',
        'PHYSICIAN_RX_2020_2024',
        'op_general_all_aggregate_static'
    ]
    
    results = {}
    
    for table in tables:
        print(f"\n📊 Analyzing: {table}")
        print("-" * 60)
        
        # Get column information
        query = f"""
        SELECT 
            column_name,
            data_type
        FROM `data-analytics-389803.conflixis_agent.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table}'
            AND data_type IN ('DATE', 'DATETIME', 'TIMESTAMP', 'INT64', 'STRING')
        ORDER BY ordinal_position
        """
        
        try:
            query_job = client.query(query)
            columns = list(query_job.result())
            
            date_columns = []
            potential_date_columns = []
            cluster_candidates = []
            
            for col in columns:
                col_name = col.column_name.lower()
                
                # Direct date/timestamp columns
                if col.data_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
                    date_columns.append({
                        'name': col.column_name,
                        'type': col.data_type
                    })
                    print(f"  ✅ Date column found: {col.column_name} ({col.data_type})")
                
                # Potential date columns based on name
                elif any(term in col_name for term in ['year', 'date', 'time', 'created', 'updated', 'modified']):
                    potential_date_columns.append({
                        'name': col.column_name,
                        'type': col.data_type
                    })
                    print(f"  🔍 Potential date column: {col.column_name} ({col.data_type})")
                
                # Good clustering candidates
                if any(term in col_name for term in ['npi', 'physician', 'id', 'name', 'specialty', 'state']):
                    cluster_candidates.append(col.column_name)
            
            # If no direct date columns, check data patterns
            if not date_columns and potential_date_columns:
                print(f"\n  Checking data patterns in potential date columns...")
                for col in potential_date_columns[:3]:  # Check first 3
                    sample_query = f"""
                    SELECT 
                        {col['name']},
                        COUNT(*) as count
                    FROM `data-analytics-389803.conflixis_agent.{table}`
                    GROUP BY {col['name']}
                    ORDER BY count DESC
                    LIMIT 5
                    """
                    try:
                        sample_job = client.query(sample_query)
                        samples = list(sample_job.result())
                        print(f"    Sample values for {col['name']}: {[s[col['name']] for s in samples[:3]]}")
                    except:
                        pass
            
            results[table] = {
                'date_columns': date_columns,
                'potential_date_columns': potential_date_columns,
                'cluster_candidates': cluster_candidates[:4]  # Top 4 clustering columns
            }
            
            print(f"\n  📌 Clustering candidates: {', '.join(cluster_candidates[:4])}")
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)[:100]}")
            results[table] = {
                'date_columns': [],
                'potential_date_columns': [],
                'cluster_candidates': []
            }
    
    return results

def generate_partitioning_strategy(results):
    """Generate optimal partitioning strategy based on analysis."""
    
    print("\n" + "=" * 80)
    print("RECOMMENDED PARTITIONING STRATEGY")
    print("=" * 80)
    
    strategies = []
    
    for table, analysis in results.items():
        print(f"\n📋 {table}")
        
        # Determine partition column
        partition_col = None
        partition_type = None
        
        if analysis['date_columns']:
            partition_col = analysis['date_columns'][0]['name']
            partition_type = 'DATE'
            print(f"  ✅ Partition by: {partition_col} (DATE)")
        elif analysis['potential_date_columns']:
            # Check for year columns
            year_cols = [c for c in analysis['potential_date_columns'] if 'year' in c['name'].lower()]
            if year_cols and year_cols[0]['type'] == 'INT64':
                partition_col = year_cols[0]['name']
                partition_type = 'RANGE'
                print(f"  ✅ Partition by: {partition_col} (INT64 RANGE)")
            else:
                print(f"  ⚠️  No clear partition column - will use ingestion time partitioning")
                partition_type = 'INGESTION_TIME'
        else:
            print(f"  ⚠️  No partition column found - will use ingestion time partitioning")
            partition_type = 'INGESTION_TIME'
        
        # Clustering columns
        cluster_cols = analysis['cluster_candidates']
        if cluster_cols:
            print(f"  ✅ Cluster by: {', '.join(cluster_cols)}")
        
        strategies.append({
            'table': table,
            'partition_column': partition_col,
            'partition_type': partition_type,
            'cluster_columns': cluster_cols
        })
    
    return strategies

def main():
    """Main execution."""
    client = setup_client()
    
    # Analyze date columns
    results = analyze_date_columns(client)
    
    # Generate strategy
    strategies = generate_partitioning_strategy(results)
    
    # Summary
    print("\n" + "=" * 80)
    print("PARTITIONING IMPLEMENTATION PLAN")
    print("=" * 80)
    
    for strategy in strategies:
        print(f"\n{strategy['table']}:")
        if strategy['partition_type'] == 'DATE':
            print(f"  PARTITION BY DATE({strategy['partition_column']})")
        elif strategy['partition_type'] == 'RANGE':
            print(f"  PARTITION BY RANGE_BUCKET({strategy['partition_column']}, GENERATE_ARRAY(2015, 2030, 1))")
        else:
            print(f"  PARTITION BY DATE(_PARTITIONTIME)")
        
        if strategy['cluster_columns']:
            print(f"  CLUSTER BY {', '.join(strategy['cluster_columns'])}")
    
    # Save strategies
    output_file = Path(__file__).parent.parent / "sql" / "partitioning_strategy.json"
    with open(output_file, 'w') as f:
        json.dump(strategies, f, indent=2)
    
    print(f"\n📁 Strategy saved to: {output_file}")
    
    return strategies

if __name__ == "__main__":
    main()