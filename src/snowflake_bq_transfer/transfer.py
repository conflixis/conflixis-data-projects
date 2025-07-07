"""Main transfer orchestration for Snowflake to BigQuery."""

import logging
import time
from typing import List, Optional, Dict, Any
from pathlib import Path

from .config import Config
from .snowflake_ops import SnowflakeOperations
from .bigquery_ops import BigQueryOperations

logger = logging.getLogger(__name__)


class SnowflakeToBigQueryTransfer:
    """Orchestrate transfer of data from Snowflake to BigQuery."""
    
    def __init__(self, config: Config):
        """Initialize transfer with configuration.
        
        Args:
            config: Configuration object.
        """
        self.config = config
        self.snowflake_ops = SnowflakeOperations(config.get_snowflake_connection_params())
        self.bigquery_ops = BigQueryOperations(
            config.gcp_project_id,
            config.google_application_credentials
        )
        self.transfer_stats: Dict[str, Any] = {
            "tables_processed": [],
            "tables_failed": [],
            "start_time": None,
            "end_time": None
        }
    
    def validate_setup(self) -> None:
        """Validate that all components are properly configured."""
        logger.info("Validating setup...")
        
        # Validate configuration
        self.config.validate()
        
        # Test Snowflake connection
        logger.info("Testing Snowflake connection...")
        self.snowflake_ops.connect()
        self.snowflake_ops.close()
        
        # Test BigQuery connection
        logger.info("Testing BigQuery connection...")
        datasets = list(self.bigquery_ops.client.list_datasets())
        logger.info(f"Successfully connected to BigQuery (found {len(datasets)} datasets)")
        
        logger.info("Setup validation completed successfully")
    
    def prepare_snowflake_staging(self) -> None:
        """Prepare Snowflake staging environment."""
        with self.snowflake_ops as sf:
            # Ensure staging schema exists
            sf.ensure_schema(
                self.config.snowflake_staging_database,
                self.config.snowflake_staging_schema
            )
            
            # Ensure storage integration exists
            sf.ensure_storage_integration(
                self.config.storage_integration,
                self.config.gcs_bucket
            )
            
            # Ensure GCS stage exists
            sf.ensure_gcs_stage(
                self.config.snowflake_staging_database,
                self.config.snowflake_staging_schema,
                self.config.gcs_stage_name,
                self.config.gcs_bucket,
                self.config.storage_integration
            )
    
    def prepare_bigquery_target(self) -> None:
        """Prepare BigQuery target environment."""
        self.bigquery_ops.ensure_dataset(self.config.bq_target_dataset)
    
    def transfer_table(
        self,
        table_name: str,
        source_db: Optional[str] = None,
        source_schema: Optional[str] = None,
        target_dataset: Optional[str] = None,
        overwrite: bool = True
    ) -> bool:
        """Transfer a single table from Snowflake to BigQuery.
        
        Args:
            table_name: Name of the table to transfer.
            source_db: Source database (defaults to config value).
            source_schema: Source schema (defaults to config value).
            target_dataset: Target BigQuery dataset (defaults to config value).
            overwrite: Whether to overwrite existing table in BigQuery.
            
        Returns:
            True if transfer successful, False otherwise.
        """
        source_db = source_db or self.config.snowflake_database
        source_schema = source_schema or self.config.snowflake_schema
        target_dataset = target_dataset or self.config.bq_target_dataset
        
        logger.info(f"Starting transfer of table {table_name}")
        start_time = time.time()
        
        try:
            with self.snowflake_ops as sf:
                # Check if source table exists
                if not sf.table_exists(source_db, source_schema, table_name):
                    raise ValueError(f"Source table {source_db}.{source_schema}.{table_name} does not exist")
                
                # Get source table row count
                source_rows = sf.get_table_row_count(source_db, source_schema, table_name)
                logger.info(f"Source table has {source_rows:,} rows")
                
                # Step 1: Copy table to staging area
                logger.info(f"Copying table to staging area...")
                sf.copy_table(
                    source_db, source_schema, table_name,
                    self.config.snowflake_staging_database,
                    self.config.snowflake_staging_schema,
                    table_name
                )
                
                # Step 2: Export to GCS
                logger.info(f"Exporting table to GCS...")
                sf.export_table_to_gcs(
                    self.config.snowflake_staging_database,
                    self.config.snowflake_staging_schema,
                    table_name,
                    self.config.gcs_stage_name
                )
            
            # Step 3: Load into BigQuery
            logger.info(f"Loading data into BigQuery...")
            write_disposition = "WRITE_TRUNCATE" if overwrite else "WRITE_APPEND"
            self.bigquery_ops.load_from_gcs(
                target_dataset,
                table_name,
                self.config.gcs_bucket,
                f"{table_name}/",
                write_disposition=write_disposition
            )
            
            # Verify transfer
            target_rows = self.bigquery_ops.get_table_row_count(target_dataset, table_name)
            logger.info(f"Target table has {target_rows:,} rows")
            
            if source_rows != target_rows:
                logger.warning(
                    f"Row count mismatch: source={source_rows:,}, target={target_rows:,}"
                )
            
            elapsed_time = time.time() - start_time
            logger.info(
                f"Successfully transferred table {table_name} in {elapsed_time:.2f} seconds"
            )
            
            self.transfer_stats["tables_processed"].append({
                "table": table_name,
                "source_rows": source_rows,
                "target_rows": target_rows,
                "elapsed_time": elapsed_time
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to transfer table {table_name}: {e}")
            self.transfer_stats["tables_failed"].append({
                "table": table_name,
                "error": str(e)
            })
            return False
    
    def transfer_multiple_tables(
        self,
        table_names: List[str],
        source_db: Optional[str] = None,
        source_schema: Optional[str] = None,
        target_dataset: Optional[str] = None,
        overwrite: bool = True,
        continue_on_error: bool = True
    ) -> Dict[str, Any]:
        """Transfer multiple tables from Snowflake to BigQuery.
        
        Args:
            table_names: List of table names to transfer.
            source_db: Source database (defaults to config value).
            source_schema: Source schema (defaults to config value).
            target_dataset: Target BigQuery dataset (defaults to config value).
            overwrite: Whether to overwrite existing tables in BigQuery.
            continue_on_error: Whether to continue if a table fails.
            
        Returns:
            Dictionary with transfer statistics.
        """
        self.transfer_stats["start_time"] = time.time()
        
        # Prepare environments
        self.prepare_snowflake_staging()
        self.prepare_bigquery_target()
        
        # Transfer tables
        for i, table_name in enumerate(table_names, 1):
            logger.info(f"Processing table {i}/{len(table_names)}: {table_name}")
            
            success = self.transfer_table(
                table_name,
                source_db=source_db,
                source_schema=source_schema,
                target_dataset=target_dataset,
                overwrite=overwrite
            )
            
            if not success and not continue_on_error:
                logger.error(f"Stopping transfer due to error with table {table_name}")
                break
        
        self.transfer_stats["end_time"] = time.time()
        self.transfer_stats["total_elapsed_time"] = (
            self.transfer_stats["end_time"] - self.transfer_stats["start_time"]
        )
        
        # Summary
        logger.info("=" * 50)
        logger.info("Transfer Summary:")
        logger.info(f"Tables processed: {len(self.transfer_stats['tables_processed'])}")
        logger.info(f"Tables failed: {len(self.transfer_stats['tables_failed'])}")
        logger.info(
            f"Total time: {self.transfer_stats['total_elapsed_time']:.2f} seconds"
        )
        
        if self.transfer_stats["tables_failed"]:
            logger.error("Failed tables:")
            for failed in self.transfer_stats["tables_failed"]:
                logger.error(f"  - {failed['table']}: {failed['error']}")
        
        return self.transfer_stats
    
    def transfer_all_tables(
        self,
        source_db: Optional[str] = None,
        source_schema: Optional[str] = None,
        target_dataset: Optional[str] = None,
        overwrite: bool = True,
        continue_on_error: bool = True
    ) -> Dict[str, Any]:
        """Transfer all tables from a Snowflake schema to BigQuery.
        
        Args:
            source_db: Source database (defaults to config value).
            source_schema: Source schema (defaults to config value).
            target_dataset: Target BigQuery dataset (defaults to config value).
            overwrite: Whether to overwrite existing tables in BigQuery.
            continue_on_error: Whether to continue if a table fails.
            
        Returns:
            Dictionary with transfer statistics.
        """
        source_db = source_db or self.config.snowflake_database
        source_schema = source_schema or self.config.snowflake_schema
        
        # Get list of tables
        with self.snowflake_ops as sf:
            table_names = sf.get_tables(source_db, source_schema)
        
        if not table_names:
            logger.warning(f"No tables found in {source_db}.{source_schema}")
            return self.transfer_stats
        
        logger.info(f"Found {len(table_names)} tables to transfer")
        
        return self.transfer_multiple_tables(
            table_names,
            source_db=source_db,
            source_schema=source_schema,
            target_dataset=target_dataset,
            overwrite=overwrite,
            continue_on_error=continue_on_error
        )