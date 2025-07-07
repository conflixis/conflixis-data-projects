"""BigQuery operations for data transfer."""

import logging
from pathlib import Path
from typing import Optional, List
from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)


class BigQueryOperations:
    """Handle BigQuery operations for data transfer."""
    
    def __init__(self, project_id: str, credentials_path: Optional[str] = None):
        """Initialize BigQuery client.
        
        Args:
            project_id: GCP project ID.
            credentials_path: Optional path to service account credentials.
        """
        self.project_id = project_id
        
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            self.client = bigquery.Client(
                credentials=credentials,
                project=project_id
            )
        else:
            # Use default credentials
            self.client = bigquery.Client(project=project_id)
        
        logger.info(f"Initialized BigQuery client for project {project_id}")
    
    def ensure_dataset(self, dataset_id: str, location: str = "US") -> None:
        """Ensure a dataset exists.
        
        Args:
            dataset_id: Dataset ID.
            location: Dataset location (default: US).
        """
        dataset_ref = f"{self.project_id}.{dataset_id}"
        
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_id} already exists")
        except Exception:
            logger.info(f"Creating dataset {dataset_id}...")
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            self.client.create_dataset(dataset)
            logger.info(f"Dataset {dataset_id} created")
    
    def table_exists(self, dataset_id: str, table_id: str) -> bool:
        """Check if a table exists.
        
        Args:
            dataset_id: Dataset ID.
            table_id: Table ID.
            
        Returns:
            True if table exists, False otherwise.
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        
        try:
            self.client.get_table(table_ref)
            return True
        except Exception:
            return False
    
    def load_from_gcs(
        self,
        dataset_id: str,
        table_id: str,
        gcs_bucket: str,
        gcs_prefix: str,
        write_disposition: str = "WRITE_TRUNCATE",
        autodetect: bool = True
    ) -> bigquery.LoadJob:
        """Load data from GCS into BigQuery table.
        
        Args:
            dataset_id: Target dataset ID.
            table_id: Target table ID.
            gcs_bucket: GCS bucket name.
            gcs_prefix: GCS path prefix (e.g., "table_name/").
            write_disposition: How to handle existing data (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY).
            autodetect: Whether to autodetect schema.
            
        Returns:
            Load job object.
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        uri = f"gs://{gcs_bucket}/{gcs_prefix}*"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
            autodetect=autodetect
        )
        
        logger.info(f"Loading data from {uri} into {table_ref}")
        
        try:
            load_job = self.client.load_table_from_uri(
                uri, table_ref, job_config=job_config
            )
            load_job.result()  # Wait for job to complete
            
            # Get table info
            table = self.client.get_table(table_ref)
            logger.info(
                f"Successfully loaded {table.num_rows:,} rows into {table_ref}"
            )
            
            return load_job
            
        except Exception as e:
            logger.error(f"Error loading data from GCS to BigQuery: {e}")
            raise
    
    def get_table_schema(self, dataset_id: str, table_id: str) -> List[bigquery.SchemaField]:
        """Get schema of a table.
        
        Args:
            dataset_id: Dataset ID.
            table_id: Table ID.
            
        Returns:
            List of schema fields.
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        table = self.client.get_table(table_ref)
        return table.schema
    
    def get_table_row_count(self, dataset_id: str, table_id: str) -> int:
        """Get row count for a table.
        
        Args:
            dataset_id: Dataset ID.
            table_id: Table ID.
            
        Returns:
            Number of rows in the table.
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        table = self.client.get_table(table_ref)
        return table.num_rows
    
    def list_tables(self, dataset_id: str) -> List[str]:
        """List all tables in a dataset.
        
        Args:
            dataset_id: Dataset ID.
            
        Returns:
            List of table IDs.
        """
        dataset_ref = f"{self.project_id}.{dataset_id}"
        tables = self.client.list_tables(dataset_ref)
        return [table.table_id for table in tables]
    
    def delete_table(self, dataset_id: str, table_id: str) -> None:
        """Delete a table.
        
        Args:
            dataset_id: Dataset ID.
            table_id: Table ID.
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        
        try:
            self.client.delete_table(table_ref)
            logger.info(f"Deleted table {table_ref}")
        except Exception as e:
            logger.error(f"Error deleting table {table_ref}: {e}")
            raise
    
    def query(self, query: str) -> bigquery.QueryJob:
        """Execute a query.
        
        Args:
            query: SQL query to execute.
            
        Returns:
            Query job object.
        """
        logger.debug(f"Executing query: {query[:100]}...")
        job = self.client.query(query)
        job.result()  # Wait for job to complete
        return job