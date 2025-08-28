"""
BigQuery client utilities for healthcare COI analytics
"""

import os
import json
import logging
from typing import Optional, Dict, Any, List
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

logger = logging.getLogger(__name__)


class BigQueryClient:
    """Wrapper class for BigQuery operations"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize BigQuery client with configuration
        
        Args:
            config: Configuration dictionary from CONFIG.yaml
        """
        self.config = config
        self.project_id = config['bigquery']['project_id']
        self.dataset = config['bigquery']['dataset']
        self.client = self._create_client()
        
    def _create_client(self) -> bigquery.Client:
        """Create authenticated BigQuery client"""
        service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
        if not service_account_json:
            raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
        
        try:
            service_account_info = json.loads(service_account_json)
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=['https://www.googleapis.com/auth/bigquery']
            )
            
            return bigquery.Client(
                project=self.project_id,
                credentials=credentials
            )
        except Exception as e:
            logger.error(f"Failed to create BigQuery client: {e}")
            raise
    
    def execute_query(self, query: str, 
                     timeout: Optional[int] = None) -> pd.DataFrame:
        """
        Execute a query and return results as DataFrame
        
        Args:
            query: SQL query string
            timeout: Query timeout in seconds
            
        Returns:
            Query results as pandas DataFrame
        """
        try:
            if timeout:
                job_config = bigquery.QueryJobConfig(
                    query_timeout_ms=timeout * 1000
                )
                query_job = self.client.query(query, job_config=job_config)
            else:
                query_job = self.client.query(query)
            
            return query_job.to_dataframe()
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.debug(f"Query: {query[:500]}...")  # Log first 500 chars
            raise
    
    def upload_dataframe(self, 
                        df: pd.DataFrame,
                        table_name: str,
                        write_disposition: str = "WRITE_TRUNCATE") -> None:
        """
        Upload a pandas DataFrame to BigQuery
        
        Args:
            df: DataFrame to upload
            table_name: Target table name (without project/dataset prefix)
            write_disposition: How to handle existing table
        """
        table_id = f"{self.project_id}.{self.dataset}.{table_name}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]
        )
        
        try:
            job = self.client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()  # Wait for job to complete
            logger.info(f"Uploaded {len(df)} rows to {table_id}")
            
        except Exception as e:
            logger.error(f"Failed to upload data to {table_id}: {e}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the dataset"""
        table_id = f"{self.project_id}.{self.dataset}.{table_name}"
        
        try:
            self.client.get_table(table_id)
            return True
        except:
            return False
    
    def get_table_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """Get schema of a table"""
        table_id = f"{self.project_id}.{self.dataset}.{table_name}"
        
        try:
            table = self.client.get_table(table_id)
            return table.schema
        except Exception as e:
            logger.error(f"Failed to get schema for {table_id}: {e}")
            raise
    
    def create_table_from_query(self, 
                               query: str,
                               destination_table: str,
                               write_disposition: str = "WRITE_TRUNCATE") -> None:
        """
        Create or replace a table from a query
        
        Args:
            query: SQL query to execute
            destination_table: Name of destination table
            write_disposition: How to handle existing table
        """
        table_id = f"{self.project_id}.{self.dataset}.{destination_table}"
        
        job_config = bigquery.QueryJobConfig(
            destination=table_id,
            write_disposition=write_disposition
        )
        
        try:
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()  # Wait for job to complete
            
            # Get row count
            table = self.client.get_table(table_id)
            logger.info(f"Created table {table_id} with {table.num_rows} rows")
            
        except Exception as e:
            logger.error(f"Failed to create table {table_id}: {e}")
            raise
    
    def get_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table"""
        query = f"""
        SELECT COUNT(*) as count 
        FROM `{self.project_id}.{self.dataset}.{table_name}`
        """
        
        df = self.execute_query(query)
        return df.iloc[0]['count']
    
    def get_table_sample(self, 
                        table_name: str,
                        sample_size: int = 100) -> pd.DataFrame:
        """Get a sample of rows from a table"""
        query = f"""
        SELECT * 
        FROM `{self.project_id}.{self.dataset}.{table_name}`
        LIMIT {sample_size}
        """
        
        return self.execute_query(query)
    
    def batch_insert(self, 
                    df: pd.DataFrame,
                    table_name: str,
                    batch_size: int = 10000) -> None:
        """
        Insert data in batches (useful for large datasets)
        
        Args:
            df: DataFrame to insert
            table_name: Target table name
            batch_size: Number of rows per batch
        """
        table_id = f"{self.project_id}.{self.dataset}.{table_name}"
        
        total_rows = len(df)
        num_batches = (total_rows + batch_size - 1) // batch_size
        
        logger.info(f"Inserting {total_rows} rows in {num_batches} batches")
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i+batch_size]
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND" if i > 0 else "WRITE_TRUNCATE"
            )
            
            job = self.client.load_table_from_dataframe(
                batch, table_id, job_config=job_config
            )
            job.result()
            
            logger.info(f"Inserted batch {i//batch_size + 1}/{num_batches}")
    
    def export_to_gcs(self, 
                      table_name: str,
                      gcs_uri: str,
                      format: str = 'CSV') -> None:
        """
        Export a BigQuery table to Google Cloud Storage
        
        Args:
            table_name: Source table name
            gcs_uri: Destination GCS URI (e.g., gs://bucket/path/file.csv)
            format: Export format (CSV, JSON, AVRO, PARQUET)
        """
        table_id = f"{self.project_id}.{self.dataset}.{table_name}"
        
        job_config = bigquery.ExtractJobConfig()
        
        if format.upper() == 'CSV':
            job_config.destination_format = bigquery.DestinationFormat.CSV
            job_config.field_delimiter = ','
        elif format.upper() == 'JSON':
            job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
        elif format.upper() == 'AVRO':
            job_config.destination_format = bigquery.DestinationFormat.AVRO
        elif format.upper() == 'PARQUET':
            job_config.destination_format = bigquery.DestinationFormat.PARQUET
        
        extract_job = self.client.extract_table(
            table_id,
            gcs_uri,
            job_config=job_config
        )
        
        extract_job.result()  # Wait for job to complete
        logger.info(f"Exported {table_id} to {gcs_uri}")


def create_client(config: Dict[str, Any]) -> BigQueryClient:
    """Factory function to create BigQuery client"""
    return BigQueryClient(config)