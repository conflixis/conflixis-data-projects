"""
Data Lineage Tracking for Healthcare COI Analytics
Tracks data sources, transformations, and quality metrics throughout the pipeline
"""

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class DataLineageTracker:
    """Tracks data lineage throughout the analytics pipeline"""
    
    def __init__(self, pipeline_id: Optional[str] = None):
        """
        Initialize data lineage tracker
        
        Args:
            pipeline_id: Unique identifier for this pipeline run
        """
        self.pipeline_id = pipeline_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.execution_timestamp = datetime.now().isoformat()
        
        self.lineage = {
            'pipeline_id': self.pipeline_id,
            'execution_timestamp': self.execution_timestamp,
            'source_data': {},
            'intermediate_tables': {},
            'analysis_steps': [],
            'data_quality': {
                'validation_checks': {},
                'missing_data': {},
                'row_counts': {}
            },
            'execution_metrics': {
                'start_time': self.execution_timestamp,
                'end_time': None,
                'total_duration_seconds': None
            }
        }
        
        self.start_time = datetime.now()
        logger.info(f"Initialized DataLineageTracker with pipeline_id: {self.pipeline_id}")
    
    def add_source_data(self, name: str, details: Dict[str, Any]):
        """
        Add source data information
        
        Args:
            name: Name of the source (e.g., 'open_payments', 'prescriptions')
            details: Dictionary with source details (table, rows, date_range, etc.)
        """
        self.lineage['source_data'][name] = {
            **details,
            'added_at': datetime.now().isoformat()
        }
        logger.info(f"Added source data: {name} with {details.get('rows', 'unknown')} rows")
    
    def add_intermediate_table(self, name: str, details: Dict[str, Any]):
        """
        Add intermediate table information
        
        Args:
            name: Name of the intermediate table
            details: Dictionary with table details (path, rows, query, etc.)
        """
        # Hash the query if provided for tracking changes
        if 'query' in details:
            details['query_hash'] = hashlib.md5(
                details['query'].encode()
            ).hexdigest()[:16]
        
        self.lineage['intermediate_tables'][name] = {
            **details,
            'created_at': datetime.now().isoformat()
        }
        logger.info(f"Added intermediate table: {name} with {details.get('rows', 'unknown')} rows")
    
    def add_analysis_step(self, step_name: str, input_tables: List[str], 
                         output_metrics: Dict[str, Any], execution_time_ms: Optional[int] = None):
        """
        Add an analysis step to the lineage
        
        Args:
            step_name: Name of the analysis step
            input_tables: List of input table names
            output_metrics: Dictionary of output metrics
            execution_time_ms: Execution time in milliseconds
        """
        step = {
            'step': step_name,
            'timestamp': datetime.now().isoformat(),
            'input_tables': input_tables,
            'output_metrics': output_metrics
        }
        
        if execution_time_ms:
            step['execution_time_ms'] = execution_time_ms
        
        self.lineage['analysis_steps'].append(step)
        logger.info(f"Added analysis step: {step_name}")
    
    def add_validation_check(self, check_name: str, status: str, details: Optional[Dict] = None):
        """
        Add a data validation check result
        
        Args:
            check_name: Name of the validation check
            status: Status of the check (passed/failed/warning)
            details: Optional details about the check
        """
        self.lineage['data_quality']['validation_checks'][check_name] = {
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }
        logger.info(f"Validation check '{check_name}': {status}")
    
    def add_missing_data_metric(self, field: str, percentage: float):
        """
        Track missing data percentages
        
        Args:
            field: Field name
            percentage: Percentage of missing data
        """
        self.lineage['data_quality']['missing_data'][field] = f"{percentage:.1f}%"
    
    def add_row_count(self, table_name: str, row_count: int):
        """
        Track row counts for tables
        
        Args:
            table_name: Name of the table
            row_count: Number of rows
        """
        self.lineage['data_quality']['row_counts'][table_name] = row_count
    
    def add_bigquery_job(self, job_id: str, query_type: str, table_name: Optional[str] = None):
        """
        Track BigQuery job IDs for audit trail
        
        Args:
            job_id: BigQuery job ID
            query_type: Type of query (create_table, analysis, etc.)
            table_name: Optional table name associated with job
        """
        if 'bigquery_jobs' not in self.lineage:
            self.lineage['bigquery_jobs'] = []
        
        self.lineage['bigquery_jobs'].append({
            'job_id': job_id,
            'query_type': query_type,
            'table_name': table_name,
            'timestamp': datetime.now().isoformat()
        })
    
    def finalize(self):
        """Finalize the lineage tracking with end time and duration"""
        end_time = datetime.now()
        self.lineage['execution_metrics']['end_time'] = end_time.isoformat()
        self.lineage['execution_metrics']['total_duration_seconds'] = (
            end_time - self.start_time
        ).total_seconds()
        
        logger.info(f"Pipeline completed in {self.lineage['execution_metrics']['total_duration_seconds']:.1f} seconds")
    
    def get_lineage(self) -> Dict[str, Any]:
        """
        Get the complete lineage dictionary
        
        Returns:
            Dictionary containing all lineage information
        """
        return self.lineage
    
    def save_lineage(self, output_path: Optional[Path] = None) -> Path:
        """
        Save lineage to JSON file
        
        Args:
            output_path: Optional output path for lineage file
            
        Returns:
            Path to saved lineage file
        """
        if output_path is None:
            output_dir = Path("data/lineage")
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"lineage_{self.pipeline_id}.json"
        
        with open(output_path, 'w') as f:
            json.dump(self.lineage, f, indent=2, default=str)
        
        logger.info(f"Lineage saved to: {output_path}")
        return output_path
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the lineage for reporting
        
        Returns:
            Dictionary with lineage summary
        """
        summary = {
            'pipeline_id': self.pipeline_id,
            'execution_date': self.execution_timestamp,
            'data_sources': list(self.lineage['source_data'].keys()),
            'source_tables': {},
            'intermediate_tables_count': len(self.lineage['intermediate_tables']),
            'analysis_steps_count': len(self.lineage['analysis_steps']),
            'total_rows_processed': 0,
            'validation_status': 'All Passed',
            'execution_time': self.lineage['execution_metrics'].get('total_duration_seconds')
        }
        
        # Add source table details
        for source, details in self.lineage['source_data'].items():
            summary['source_tables'][source] = {
                'table': details.get('table', details.get('source_file', 'Unknown')),
                'rows': details.get('rows', 0),
                'date_range': details.get('date_range', 'N/A')
            }
            summary['total_rows_processed'] += details.get('rows', 0)
        
        # Check validation status
        for check, result in self.lineage['data_quality']['validation_checks'].items():
            if result['status'] == 'failed':
                summary['validation_status'] = 'Some Failed'
                break
            elif result['status'] == 'warning':
                summary['validation_status'] = 'Warnings Present'
        
        return summary
    
    def generate_lineage_markdown(self) -> str:
        """
        Generate markdown representation of data lineage for reports
        
        Returns:
            Markdown string describing the data lineage
        """
        summary = self.get_summary()
        
        execution_time = summary['execution_time'] if summary['execution_time'] else 0
        markdown = f"""## Data Lineage

### Pipeline Execution
- **Pipeline ID**: {summary['pipeline_id']}
- **Execution Date**: {summary['execution_date']}
- **Total Duration**: {execution_time:.1f} seconds
- **Validation Status**: {summary['validation_status']}

### Source Data
"""
        
        for source, details in summary['source_tables'].items():
            markdown += f"- **{source.replace('_', ' ').title()}**: {details['table']}\n"
            markdown += f"  - Rows: {details['rows']:,}\n"
            markdown += f"  - Date Range: {details['date_range']}\n"
        
        markdown += f"\n### Processing Summary\n"
        markdown += f"- **Total Rows Processed**: {summary['total_rows_processed']:,}\n"
        markdown += f"- **Intermediate Tables Created**: {summary['intermediate_tables_count']}\n"
        markdown += f"- **Analysis Steps Completed**: {summary['analysis_steps_count']}\n"
        
        # Add data quality section
        if self.lineage['data_quality']['validation_checks']:
            markdown += "\n### Data Quality Checks\n"
            for check, result in self.lineage['data_quality']['validation_checks'].items():
                status_emoji = "✅" if result['status'] == 'passed' else "❌" if result['status'] == 'failed' else "⚠️"
                markdown += f"- {check.replace('_', ' ').title()}: {status_emoji} {result['status'].upper()}\n"
        
        # Add missing data metrics if available
        if self.lineage['data_quality']['missing_data']:
            markdown += "\n### Data Completeness\n"
            for field, percentage in self.lineage['data_quality']['missing_data'].items():
                markdown += f"- {field.replace('_', ' ').title()}: {percentage} missing\n"
        
        # Add BigQuery job tracking if available
        if 'bigquery_jobs' in self.lineage and self.lineage['bigquery_jobs']:
            markdown += f"\n### Audit Trail\n"
            markdown += f"- **BigQuery Jobs Executed**: {len(self.lineage['bigquery_jobs'])}\n"
            markdown += f"- **Job IDs Available**: Yes (stored for reproducibility)\n"
        
        return markdown