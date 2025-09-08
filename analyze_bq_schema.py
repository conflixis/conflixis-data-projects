#!/usr/bin/env python3
"""
Analyze BigQuery dataset schema for conflixis_agent to identify:
1. Tables with physician_id, NPI, or similar identifier columns  
2. Data type mismatches (STRING vs INT64)
3. Partitioning and clustering configurations
4. Tables frequently joined together

Focus on healthcare COI analytics optimization.
"""

import os
import json
import logging
import pandas as pd
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BigQuerySchemaAnalyzer:
    def __init__(self):
        """Initialize BigQuery client"""
        self.client = self._get_client()
        self.dataset_id = 'conflixis_agent'
        self.project_id = 'data-analytics-389803'
    
    def _get_client(self):
        """Initialize BigQuery client with credentials"""
        try:
            service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
            if not service_account_json:
                raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
            
            service_account_info = json.loads(service_account_json)
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=['https://www.googleapis.com/auth/bigquery']
            )
            
            return bigquery.Client(
                credentials=credentials,
                project=service_account_info.get('project_id', 'data-analytics-389803')
            )
            
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise
    
    def get_dataset_tables(self):
        """Get all tables in the conflixis_agent dataset"""
        query = f"""
        SELECT 
            table_id as table_name,
            CASE 
                WHEN type = 1 THEN 'TABLE'
                WHEN type = 2 THEN 'VIEW'
                ELSE 'OTHER'
            END as table_type,
            TIMESTAMP_MILLIS(creation_time) as creation_time,
            row_count,
            size_bytes,
            ROUND(size_bytes / 1024 / 1024 / 1024, 2) as size_gb
        FROM `{self.project_id}.{self.dataset_id}.__TABLES__`
        WHERE size_bytes > 0
        ORDER BY size_bytes DESC
        """
        
        logger.info("Getting all tables in conflixis_agent dataset...")
        return self.client.query(query).to_dataframe()
    
    def get_table_columns(self):
        """Get detailed column information for all tables"""
        query = f"""
        SELECT 
            table_name,
            column_name,
            data_type,
            is_nullable,
            is_partitioning_column,
            clustering_ordinal_position
        FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        ORDER BY table_name, ordinal_position
        """
        
        logger.info("Getting column metadata for all tables...")
        return self.client.query(query).to_dataframe()
    
    def get_partitioning_info(self):
        """Get partitioning and clustering information"""
        query = f"""
        SELECT 
            table_name,
            partition_by,
            cluster_by
        FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE partition_id IS NOT NULL
        GROUP BY table_name, partition_by, cluster_by
        """
        
        logger.info("Getting partitioning and clustering information...")
        return self.client.query(query).to_dataframe()
    
    def analyze_identifier_columns(self, columns_df):
        """Analyze tables with physician_id, NPI, or similar identifier columns"""
        
        # Common healthcare identifier patterns
        identifier_patterns = [
            'physician_id', 'npi', 'provider_id', 'doctor_id', 'practitioner_id',
            'prescriber_id', 'hcp_id', 'physician_profile_id', 'provider_npi'
        ]
        
        # Find tables with identifier columns
        identifier_tables = []
        
        for table in columns_df['table_name'].unique():
            table_cols = columns_df[columns_df['table_name'] == table]
            
            for _, col in table_cols.iterrows():
                col_name_lower = col['column_name'].lower()
                
                # Check if column matches identifier patterns
                for pattern in identifier_patterns:
                    if pattern in col_name_lower:
                        identifier_tables.append({
                            'table_name': table,
                            'column_name': col['column_name'],
                            'data_type': col['data_type'],
                            'pattern_matched': pattern,
                            'is_nullable': col['is_nullable'],
                            'is_partitioning_column': col['is_partitioning_column']
                        })
        
        return pd.DataFrame(identifier_tables)
    
    def analyze_data_type_mismatches(self, identifier_df):
        """Identify data type mismatches for identifier columns"""
        
        mismatches = []
        
        # Group by column pattern to find inconsistencies
        for pattern in identifier_df['pattern_matched'].unique():
            pattern_tables = identifier_df[identifier_df['pattern_matched'] == pattern]
            data_types = pattern_tables['data_type'].unique()
            
            if len(data_types) > 1:
                mismatches.append({
                    'pattern': pattern,
                    'tables_with_mismatch': len(pattern_tables),
                    'data_types': ', '.join(data_types),
                    'affected_tables': ', '.join(pattern_tables['table_name'].tolist())
                })
        
        return pd.DataFrame(mismatches)
    
    def analyze_table_relationships(self, identifier_df):
        """Identify tables that are frequently joined together"""
        
        # Group tables by identifier patterns to identify potential joins
        join_candidates = {}
        
        for pattern in identifier_df['pattern_matched'].unique():
            pattern_tables = identifier_df[identifier_df['pattern_matched'] == pattern]['table_name'].unique()
            if len(pattern_tables) > 1:
                join_candidates[pattern] = list(pattern_tables)
        
        return join_candidates
    
    def get_table_ddl(self, table_name):
        """Get DDL statement for a specific table"""
        query = f"""
        SELECT ddl
        FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = '{table_name}'
        """
        
        try:
            result = self.client.query(query).to_dataframe()
            return result.iloc[0]['ddl'] if not result.empty else "DDL not available"
        except Exception as e:
            logger.error(f"Failed to get DDL for table {table_name}: {e}")
            return f"Error retrieving DDL: {e}"
    
    def analyze_expensive_query_patterns(self, identifier_df, columns_df):
        """Identify patterns that could lead to expensive queries"""
        
        expensive_patterns = []
        
        # 1. Large tables without partitioning
        tables_df = self.get_dataset_tables()
        large_tables = tables_df[tables_df['size_gb'] > 1.0]  # Tables > 1GB
        
        for _, table in large_tables.iterrows():
            table_columns = columns_df[columns_df['table_name'] == table['table_name']]
            has_partitioning = table_columns['is_partitioning_column'].any()
            
            if not has_partitioning:
                expensive_patterns.append({
                    'issue_type': 'No partitioning on large table',
                    'table_name': table['table_name'],
                    'size_gb': table['size_gb'],
                    'recommendation': 'Add partitioning by date/year column'
                })
        
        # 2. Identifier columns with different data types
        type_mismatches = self.analyze_data_type_mismatches(identifier_df)
        for _, mismatch in type_mismatches.iterrows():
            expensive_patterns.append({
                'issue_type': 'Data type mismatch for joins',
                'table_name': mismatch['affected_tables'],
                'details': f"Column pattern '{mismatch['pattern']}' has types: {mismatch['data_types']}",
                'recommendation': 'Standardize data types for efficient joins'
            })
        
        # 3. Tables likely to be joined but without clustering
        join_candidates = self.analyze_table_relationships(identifier_df)
        for pattern, tables in join_candidates.items():
            for table in tables:
                table_cols = columns_df[columns_df['table_name'] == table]
                has_clustering = table_cols['clustering_ordinal_position'].notna().any()
                
                if not has_clustering:
                    expensive_patterns.append({
                        'issue_type': 'Missing clustering on joinable table',
                        'table_name': table,
                        'join_column_pattern': pattern,
                        'recommendation': f'Add clustering by {pattern} column'
                    })
        
        return pd.DataFrame(expensive_patterns)
    
    def run_full_analysis(self):
        """Run complete schema analysis"""
        
        logger.info("Starting comprehensive BigQuery schema analysis...")
        
        # Get basic table information
        tables_df = self.get_dataset_tables()
        logger.info(f"Found {len(tables_df)} tables in {self.dataset_id} dataset")
        
        # Get column information
        columns_df = self.get_table_columns()
        logger.info(f"Analyzed {len(columns_df)} columns across all tables")
        
        # Get partitioning information
        try:
            partitioning_df = self.get_partitioning_info()
        except Exception as e:
            logger.warning(f"Could not get partitioning info: {e}")
            partitioning_df = pd.DataFrame()
        
        # Analyze identifier columns
        identifier_df = self.analyze_identifier_columns(columns_df)
        logger.info(f"Found {len(identifier_df)} identifier columns")
        
        # Analyze data type mismatches
        mismatch_df = self.analyze_data_type_mismatches(identifier_df)
        logger.info(f"Found {len(mismatch_df)} potential data type mismatches")
        
        # Analyze table relationships
        join_candidates = self.analyze_table_relationships(identifier_df)
        logger.info(f"Found {len(join_candidates)} potential join patterns")
        
        # Analyze expensive query patterns
        expensive_patterns_df = self.analyze_expensive_query_patterns(identifier_df, columns_df)
        logger.info(f"Identified {len(expensive_patterns_df)} potential performance issues")
        
        # Compile results
        results = {
            'dataset_summary': {
                'total_tables': len(tables_df),
                'total_columns': len(columns_df),
                'total_size_gb': tables_df['size_gb'].sum(),
                'largest_table': tables_df.iloc[0]['table_name'] if not tables_df.empty else None,
                'largest_table_size_gb': tables_df.iloc[0]['size_gb'] if not tables_df.empty else 0
            },
            'tables': tables_df,
            'columns': columns_df,
            'partitioning': partitioning_df,
            'identifier_columns': identifier_df,
            'data_type_mismatches': mismatch_df,
            'join_candidates': join_candidates,
            'expensive_patterns': expensive_patterns_df
        }
        
        return results
    
    def generate_report(self, results):
        """Generate a comprehensive analysis report"""
        
        report = []
        
        report.append("# BigQuery Schema Analysis Report")
        report.append(f"## Dataset: {self.project_id}.{self.dataset_id}")
        report.append("")
        
        # Dataset summary
        summary = results['dataset_summary']
        report.append("## Dataset Summary")
        report.append(f"- **Total Tables**: {summary['total_tables']}")
        report.append(f"- **Total Columns**: {summary['total_columns']}")  
        report.append(f"- **Total Size**: {summary['total_size_gb']:.2f} GB")
        report.append(f"- **Largest Table**: {summary['largest_table']} ({summary['largest_table_size_gb']:.2f} GB)")
        report.append("")
        
        # Tables with identifier columns
        report.append("## Tables with Healthcare Identifiers")
        if not results['identifier_columns'].empty:
            identifier_summary = results['identifier_columns'].groupby('pattern_matched').agg({
                'table_name': 'count',
                'data_type': lambda x: ', '.join(x.unique())
            }).reset_index()
            
            for _, row in identifier_summary.iterrows():
                report.append(f"- **{row['pattern_matched']}**: {row['table_name']} tables, types: {row['data_type']}")
        else:
            report.append("- No healthcare identifier columns found")
        report.append("")
        
        # Data type mismatches
        report.append("## Data Type Mismatches")
        if not results['data_type_mismatches'].empty:
            for _, mismatch in results['data_type_mismatches'].iterrows():
                report.append(f"- **{mismatch['pattern']}**: {mismatch['tables_with_mismatch']} tables with types: {mismatch['data_types']}")
                report.append(f"  - Affected tables: {mismatch['affected_tables']}")
        else:
            report.append("- No data type mismatches found")
        report.append("")
        
        # Join candidates
        report.append("## Potential Join Relationships")
        if results['join_candidates']:
            for pattern, tables in results['join_candidates'].items():
                report.append(f"- **{pattern}**: {len(tables)} tables can be joined")
                report.append(f"  - Tables: {', '.join(tables)}")
        else:
            report.append("- No clear join relationships identified")
        report.append("")
        
        # Performance issues
        report.append("## Performance Optimization Opportunities")
        if not results['expensive_patterns'].empty:
            issue_counts = results['expensive_patterns']['issue_type'].value_counts()
            for issue_type, count in issue_counts.items():
                report.append(f"- **{issue_type}**: {count} instances")
            
            report.append("\n### Detailed Issues:")
            for _, issue in results['expensive_patterns'].iterrows():
                report.append(f"- {issue['table_name']}: {issue.get('details', issue['issue_type'])}")
                report.append(f"  - Recommendation: {issue['recommendation']}")
        else:
            report.append("- No major performance issues identified")
        report.append("")
        
        # Partitioning status
        report.append("## Partitioning Status")
        if not results['partitioning'].empty:
            report.append(f"- **Partitioned Tables**: {len(results['partitioning'])}")
            for _, part in results['partitioning'].iterrows():
                report.append(f"  - {part['table_name']}: partitioned by {part['partition_by']}")
                if pd.notna(part['cluster_by']):
                    report.append(f"    - Clustered by: {part['cluster_by']}")
        else:
            report.append("- No partitioned tables found")
        report.append("")
        
        # Top 10 largest tables
        report.append("## Largest Tables")
        top_tables = results['tables'].head(10)
        for _, table in top_tables.iterrows():
            report.append(f"- **{table['table_name']}**: {table['size_gb']:.2f} GB, {table['row_count']:,} rows")
        
        return "\n".join(report)

def main():
    """Main execution function"""
    try:
        analyzer = BigQuerySchemaAnalyzer()
        results = analyzer.run_full_analysis()
        report = analyzer.generate_report(results)
        
        # Save results
        output_dir = Path("analysis_output")
        output_dir.mkdir(exist_ok=True)
        
        # Save detailed results as JSON
        json_results = {}
        for key, value in results.items():
            if isinstance(value, pd.DataFrame):
                json_results[key] = value.to_dict('records')
            else:
                json_results[key] = value
        
        with open(output_dir / "schema_analysis_detailed.json", 'w') as f:
            json.dump(json_results, f, indent=2, default=str)
        
        # Save readable report
        with open(output_dir / "schema_analysis_report.md", 'w') as f:
            f.write(report)
        
        # Print summary to console
        print(report)
        
        logger.info(f"Analysis complete. Results saved to {output_dir}/")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise

if __name__ == "__main__":
    main()