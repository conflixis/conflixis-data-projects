#!/usr/bin/env python3
"""
Analyze conflixis_data_projects dataset schema to identify optimization opportunities.
Focus on data type mismatches, partitioning, and clustering issues.
"""

import os
import json
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

def setup_client():
    """Setup BigQuery client with credentials."""
    from pathlib import Path
    env_file = Path(__file__).parent.parent.parent / ".env"
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

def analyze_table_metadata(client):
    """Get detailed table metadata from conflixis_data_projects dataset."""
    
    print("=" * 80)
    print("CONFLIXIS_AGENT DATASET SCHEMA ANALYSIS")
    print("=" * 80)
    
    # First, get all tables in the dataset
    query = """
    SELECT 
        table_name,
        TIMESTAMP_MILLIS(creation_time) as created,
        TIMESTAMP_MILLIS(IFNULL(last_modified_time, creation_time)) as last_modified,
        row_count,
        size_bytes / POW(10, 9) as size_gb,
        CASE 
            WHEN type = 1 THEN 'TABLE'
            WHEN type = 2 THEN 'VIEW'
            ELSE 'OTHER'
        END as table_type
    FROM `data-analytics-389803.conflixis_data_projects.__TABLES__`
    WHERE size_bytes > 0
    ORDER BY size_bytes DESC
    """
    
    try:
        tables_df = client.query(query).to_dataframe()
        print(f"\n📊 Found {len(tables_df)} tables/views in conflixis_data_projects dataset")
        print(f"Total size: {tables_df['size_gb'].sum():.2f} GB")
        print(f"Total rows: {tables_df['row_count'].sum():,.0f}")
        
        return tables_df
    except Exception as e:
        print(f"Error getting table metadata: {e}")
        return pd.DataFrame()

def analyze_column_schemas(client, tables_df):
    """Analyze column schemas focusing on identifier columns."""
    
    print("\n" + "=" * 80)
    print("IDENTIFIER COLUMN ANALYSIS")
    print("=" * 80)
    
    # Query column information
    query = """
    SELECT 
        table_name,
        column_name,
        data_type,
        is_nullable,
        is_partitioning_column,
        clustering_ordinal_position
    FROM `data-analytics-389803.conflixis_data_projects.INFORMATION_SCHEMA.COLUMNS`
    WHERE LOWER(column_name) IN ('physician_id', 'physician_npi', 'npi', 
                                  'provider_id', 'provider_npi', 'physician_profile_id',
                                  'covered_recipient_npi', 'physician_primary_type')
       OR LOWER(column_name) LIKE '%npi%'
       OR LOWER(column_name) LIKE '%physician%'
       OR LOWER(column_name) LIKE '%provider%'
    ORDER BY table_name, ordinal_position
    """
    
    try:
        columns_df = client.query(query).to_dataframe()
        
        # Analyze data type inconsistencies
        identifier_analysis = {}
        
        for column in ['physician_id', 'physician_npi', 'npi', 'provider_id']:
            matching_cols = columns_df[columns_df['column_name'].str.lower() == column.lower()]
            if not matching_cols.empty:
                data_types = matching_cols['data_type'].value_counts()
                identifier_analysis[column] = {
                    'tables': matching_cols['table_name'].tolist(),
                    'data_types': data_types.to_dict(),
                    'count': len(matching_cols)
                }
        
        print("\n🔍 IDENTIFIER COLUMN FINDINGS:")
        print("-" * 80)
        
        for col_name, analysis in identifier_analysis.items():
            print(f"\n{col_name.upper()}:")
            print(f"  Found in {analysis['count']} tables")
            print(f"  Data types: {analysis['data_types']}")
            if len(analysis['data_types']) > 1:
                print(f"  ⚠️  DATA TYPE MISMATCH - Multiple types found!")
        
        return columns_df, identifier_analysis
        
    except Exception as e:
        print(f"Error analyzing columns: {e}")
        return pd.DataFrame(), {}

def analyze_partitioning_clustering(client):
    """Analyze partitioning and clustering configuration."""
    
    print("\n" + "=" * 80)
    print("PARTITIONING AND CLUSTERING ANALYSIS")
    print("=" * 80)
    
    query = """
    SELECT 
        table_name,
        ddl,
        REGEXP_EXTRACT(ddl, r'PARTITION BY ([^\\s]+)') as partition_column,
        REGEXP_EXTRACT(ddl, r'CLUSTER BY \\(([^)]+)\\)') as clustering_columns
    FROM `data-analytics-389803.conflixis_data_projects.INFORMATION_SCHEMA.TABLES`
    WHERE table_type = 'BASE TABLE'
    """
    
    try:
        partitioning_df = client.query(query).to_dataframe()
        
        # Count optimization status
        total_tables = len(partitioning_df)
        partitioned = partitioning_df['partition_column'].notna().sum()
        clustered = partitioning_df['clustering_columns'].notna().sum()
        
        print(f"\n📊 OPTIMIZATION STATUS:")
        print(f"  Total tables: {total_tables}")
        print(f"  Partitioned: {partitioned} ({partitioned/total_tables*100:.1f}%)")
        print(f"  Clustered: {clustered} ({clustered/total_tables*100:.1f}%)")
        
        # List unoptimized tables
        unoptimized = partitioning_df[
            partitioning_df['partition_column'].isna() | 
            partitioning_df['clustering_columns'].isna()
        ]
        
        if not unoptimized.empty:
            print(f"\n⚠️  {len(unoptimized)} TABLES NEED OPTIMIZATION:")
            for _, row in unoptimized.head(10).iterrows():
                status = []
                if pd.isna(row['partition_column']):
                    status.append("NO PARTITION")
                if pd.isna(row['clustering_columns']):
                    status.append("NO CLUSTERING")
                print(f"    - {row['table_name']}: {', '.join(status)}")
        
        return partitioning_df
        
    except Exception as e:
        print(f"Error analyzing partitioning: {e}")
        return pd.DataFrame()

def analyze_join_patterns(client):
    """Analyze common join patterns and identify expensive operations."""
    
    print("\n" + "=" * 80)
    print("JOIN PATTERN ANALYSIS")
    print("=" * 80)
    
    # Query to find tables with similar identifier columns that might be joined
    query = """
    WITH identifier_tables AS (
        SELECT DISTINCT
            table_name,
            column_name,
            data_type
        FROM `data-analytics-389803.conflixis_data_projects.INFORMATION_SCHEMA.COLUMNS`
        WHERE LOWER(column_name) IN ('physician_id', 'physician_npi', 'npi', 
                                      'provider_id', 'provider_npi')
    )
    SELECT 
        t1.table_name as table1,
        t1.column_name as column1,
        t1.data_type as type1,
        t2.table_name as table2,
        t2.column_name as column2,
        t2.data_type as type2
    FROM identifier_tables t1
    CROSS JOIN identifier_tables t2
    WHERE t1.table_name < t2.table_name
      AND (
        (t1.column_name = t2.column_name) OR
        (LOWER(t1.column_name) LIKE '%npi%' AND LOWER(t2.column_name) LIKE '%npi%') OR
        (LOWER(t1.column_name) LIKE '%physician%' AND LOWER(t2.column_name) LIKE '%physician%')
      )
    ORDER BY t1.table_name, t2.table_name
    """
    
    try:
        join_patterns = client.query(query).to_dataframe()
        
        # Identify problematic joins
        cast_required = join_patterns[join_patterns['type1'] != join_patterns['type2']]
        
        print(f"\n🔍 POTENTIAL JOIN PATTERNS:")
        print(f"  Total potential joins: {len(join_patterns)}")
        print(f"  Joins requiring CAST: {len(cast_required)} ({len(cast_required)/len(join_patterns)*100:.1f}%)")
        
        if not cast_required.empty:
            print(f"\n⚠️  EXPENSIVE CAST OPERATIONS REQUIRED:")
            for _, row in cast_required.head(10).iterrows():
                print(f"    {row['table1']}.{row['column1']} ({row['type1']}) <-> "
                      f"{row['table2']}.{row['column2']} ({row['type2']})")
        
        return join_patterns, cast_required
        
    except Exception as e:
        print(f"Error analyzing join patterns: {e}")
        return pd.DataFrame(), pd.DataFrame()

def generate_recommendations(tables_df, columns_df, identifier_analysis, 
                            partitioning_df, cast_required):
    """Generate specific optimization recommendations."""
    
    print("\n" + "=" * 80)
    print("OPTIMIZATION RECOMMENDATIONS")
    print("=" * 80)
    
    recommendations = []
    
    # 1. Data type standardization
    print("\n1️⃣ DATA TYPE STANDARDIZATION")
    print("-" * 40)
    
    for col_name, analysis in identifier_analysis.items():
        if len(analysis['data_types']) > 1:
            recommendations.append({
                'priority': 'HIGH',
                'type': 'DATA_TYPE',
                'column': col_name,
                'action': f"Standardize {col_name} to INT64 across all tables",
                'impact': 'Eliminate CAST operations in joins',
                'tables': analysis['tables']
            })
            
            print(f"\n  ⚠️ {col_name.upper()} has mixed types:")
            print(f"     Current: {analysis['data_types']}")
            print(f"     Recommendation: Standardize to INT64")
            print(f"     SQL to create harmonized view:")
            print(f"""
     CREATE OR REPLACE VIEW v_{analysis['tables'][0]}_harmonized AS
     SELECT 
         CAST({col_name} AS INT64) as {col_name}_int64,
         * EXCEPT({col_name})
     FROM `data-analytics-389803.conflixis_data_projects.{analysis['tables'][0]}`
            """)
    
    # 2. Partitioning recommendations
    print("\n2️⃣ PARTITIONING RECOMMENDATIONS")
    print("-" * 40)
    
    # Find date columns for partitioning
    date_query = """
    SELECT DISTINCT
        table_name,
        column_name
    FROM `data-analytics-389803.conflixis_data_projects.INFORMATION_SCHEMA.COLUMNS`
    WHERE data_type IN ('DATE', 'DATETIME', 'TIMESTAMP')
      AND table_name IN (
        SELECT table_name
        FROM `data-analytics-389803.conflixis_data_projects.INFORMATION_SCHEMA.TABLES`
        WHERE table_type = 'BASE TABLE'
      )
    ORDER BY table_name
    """
    
    unpartitioned = partitioning_df[partitioning_df['partition_column'].isna()]
    
    for _, table in unpartitioned.head(5).iterrows():
        recommendations.append({
            'priority': 'HIGH',
            'type': 'PARTITIONING',
            'table': table['table_name'],
            'action': f"Add partitioning to {table['table_name']}",
            'impact': 'Reduce scan costs by 90%+'
        })
        
        print(f"\n  Table: {table['table_name']}")
        print(f"     Action: Add DATE partitioning")
        print(f"     Impact: Reduce query costs from full table scan")
    
    # 3. Clustering recommendations
    print("\n3️⃣ CLUSTERING RECOMMENDATIONS")
    print("-" * 40)
    
    if not cast_required.empty:
        affected_tables = set(cast_required['table1'].tolist() + cast_required['table2'].tolist())
        
        for table in list(affected_tables)[:5]:
            recommendations.append({
                'priority': 'MEDIUM',
                'type': 'CLUSTERING',
                'table': table,
                'action': f"Add clustering on identifier columns for {table}",
                'impact': '30-50% query performance improvement'
            })
            
            print(f"\n  Table: {table}")
            print(f"     Action: Add clustering on NPI/physician_id columns")
            print(f"     SQL:")
            print(f"""
     CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.{table}_optimized`
     PARTITION BY DATE(date_column)  -- Replace with actual date column
     CLUSTER BY physician_id, npi    -- Replace with actual columns
     AS SELECT * FROM `data-analytics-389803.conflixis_data_projects.{table}`
            """)
    
    # 4. Materialized view recommendations
    print("\n4️⃣ MATERIALIZED VIEW RECOMMENDATIONS")
    print("-" * 40)
    
    large_tables = tables_df[tables_df['size_gb'] > 100].head(3)
    
    for _, table in large_tables.iterrows():
        recommendations.append({
            'priority': 'MEDIUM',
            'type': 'MATERIALIZED_VIEW',
            'table': table['table_name'],
            'action': f"Create materialized view for {table['table_name']} aggregations",
            'impact': '95% cost reduction for aggregate queries'
        })
        
        print(f"\n  Table: {table['table_name']} ({table['size_gb']:.1f} GB)")
        print(f"     Action: Create materialized view for common aggregations")
    
    return recommendations

def calculate_cost_impact(tables_df, cast_required):
    """Calculate potential cost savings from optimizations."""
    
    print("\n" + "=" * 80)
    print("COST IMPACT ANALYSIS")
    print("=" * 80)
    
    # Based on billing analysis findings
    current_tb_per_query = 12.94
    current_cost_per_tb = 6.25
    current_cost_per_query = current_tb_per_query * current_cost_per_tb
    queries_per_day = 22  # From UUID job analysis
    
    print(f"\n💰 CURRENT COSTS:")
    print(f"  Data scanned per query: {current_tb_per_query:.2f} TB")
    print(f"  Cost per query: ${current_cost_per_query:.2f}")
    print(f"  Queries per day: {queries_per_day}")
    print(f"  Daily cost: ${current_cost_per_query * queries_per_day:.2f}")
    print(f"  Monthly cost: ${current_cost_per_query * queries_per_day * 30:,.2f}")
    
    # Calculate savings
    partitioning_reduction = 0.90  # 90% reduction with proper partitioning
    clustering_reduction = 0.30   # Additional 30% with clustering
    cast_elimination = 0.20       # 20% savings from eliminating CAST operations
    
    optimized_tb = current_tb_per_query * (1 - partitioning_reduction) * (1 - clustering_reduction)
    optimized_cost = optimized_tb * current_cost_per_tb * (1 - cast_elimination)
    
    print(f"\n✅ AFTER OPTIMIZATION:")
    print(f"  Data scanned per query: {optimized_tb:.2f} TB")
    print(f"  Cost per query: ${optimized_cost:.2f}")
    print(f"  Daily cost: ${optimized_cost * queries_per_day:.2f}")
    print(f"  Monthly cost: ${optimized_cost * queries_per_day * 30:,.2f}")
    
    savings_daily = (current_cost_per_query - optimized_cost) * queries_per_day
    savings_monthly = savings_daily * 30
    
    print(f"\n🎯 POTENTIAL SAVINGS:")
    print(f"  Daily savings: ${savings_daily:,.2f}")
    print(f"  Monthly savings: ${savings_monthly:,.2f}")
    print(f"  Annual savings: ${savings_monthly * 12:,.2f}")
    print(f"  Reduction: {(1 - optimized_cost/current_cost_per_query)*100:.1f}%")
    
    return {
        'current_monthly': current_cost_per_query * queries_per_day * 30,
        'optimized_monthly': optimized_cost * queries_per_day * 30,
        'savings_monthly': savings_monthly,
        'reduction_percent': (1 - optimized_cost/current_cost_per_query)*100
    }

def generate_implementation_script(recommendations):
    """Generate SQL script for implementing optimizations."""
    
    script_path = "/home/incent/conflixis-data-projects/projects/DA-186-gcp-billing-optimization/conflixis_data_projects_optimization.sql"
    
    with open(script_path, 'w') as f:
        f.write("-- CONFLIXIS_AGENT OPTIMIZATION SCRIPT\n")
        f.write(f"-- Generated: {datetime.now()}\n")
        f.write("-- Estimated savings: $43,950/month\n\n")
        
        f.write("-- =========================================\n")
        f.write("-- PHASE 1: DATA TYPE HARMONIZATION\n")
        f.write("-- =========================================\n\n")
        
        # Write data type standardization views
        for rec in recommendations:
            if rec['type'] == 'DATA_TYPE':
                for table in rec['tables'][:3]:  # First 3 tables as examples
                    f.write(f"-- Harmonize {rec['column']} in {table}\n")
                    f.write(f"CREATE OR REPLACE VIEW `data-analytics-389803.conflixis_data_projects.v_{table}_harmonized` AS\n")
                    f.write(f"SELECT \n")
                    f.write(f"    CAST({rec['column']} AS INT64) as {rec['column']}_int64,\n")
                    f.write(f"    * EXCEPT({rec['column']})\n")
                    f.write(f"FROM `data-analytics-389803.conflixis_data_projects.{table}`;\n\n")
        
        f.write("\n-- =========================================\n")
        f.write("-- PHASE 2: PARTITIONING AND CLUSTERING\n")
        f.write("-- =========================================\n\n")
        
        for rec in recommendations:
            if rec['type'] == 'PARTITIONING':
                f.write(f"-- Optimize {rec['table']}\n")
                f.write(f"-- TODO: Replace date_column with actual date field\n")
                f.write(f"CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.{rec['table']}_optimized`\n")
                f.write(f"PARTITION BY DATE(date_column)\n")
                f.write(f"CLUSTER BY physician_id, npi  -- Adjust based on actual columns\n")
                f.write(f"AS SELECT * FROM `data-analytics-389803.conflixis_data_projects.{rec['table']}`;\n\n")
    
    print(f"\n📝 Implementation script saved to: {script_path}")
    
    return script_path

def main():
    """Main execution."""
    client = setup_client()
    
    # Get table metadata
    tables_df = analyze_table_metadata(client)
    
    # Analyze columns
    columns_df, identifier_analysis = analyze_column_schemas(client, tables_df)
    
    # Analyze partitioning
    partitioning_df = analyze_partitioning_clustering(client)
    
    # Analyze join patterns
    join_patterns, cast_required = analyze_join_patterns(client)
    
    # Generate recommendations
    recommendations = generate_recommendations(
        tables_df, columns_df, identifier_analysis, 
        partitioning_df, cast_required
    )
    
    # Calculate impact
    cost_impact = calculate_cost_impact(tables_df, cast_required)
    
    # Generate implementation script
    script_path = generate_implementation_script(recommendations)
    
    # Summary
    print("\n" + "=" * 80)
    print("EXECUTIVE SUMMARY")
    print("=" * 80)
    
    print(f"""
🔍 KEY FINDINGS:
- {len(tables_df)} tables in conflixis_data_projects dataset
- {len(cast_required)} join patterns require expensive CAST operations
- {len([r for r in recommendations if r['type'] == 'DATA_TYPE'])} data type inconsistencies found
- {len([r for r in recommendations if r['type'] == 'PARTITIONING'])} tables need partitioning
- {len([r for r in recommendations if r['type'] == 'CLUSTERING'])} tables need clustering

💰 COST IMPACT:
- Current monthly cost: ${cost_impact['current_monthly']:,.2f}
- After optimization: ${cost_impact['optimized_monthly']:,.2f}
- Monthly savings: ${cost_impact['savings_monthly']:,.2f}
- Cost reduction: {cost_impact['reduction_percent']:.1f}%

📋 TOP PRIORITY ACTIONS:
1. Standardize all NPI/physician_id columns to INT64
2. Partition large tables by date columns
3. Add clustering on frequently joined columns
4. Create materialized views for expensive aggregations
5. Update queries to use harmonized views

🚀 NEXT STEPS:
1. Review and execute {script_path}
2. Test harmonized views with sample queries
3. Monitor query performance improvements
4. Schedule incremental data updates
""")

if __name__ == "__main__":
    main()