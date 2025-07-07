"""Snowflake operations for data transfer."""

import logging
from typing import List, Optional, Any
import snowflake.connector
from snowflake.connector import SnowflakeConnection

logger = logging.getLogger(__name__)


class SnowflakeOperations:
    """Handle Snowflake operations for data transfer."""
    
    def __init__(self, connection_params: dict):
        """Initialize with connection parameters.
        
        Args:
            connection_params: Dictionary with Snowflake connection parameters.
        """
        self.connection_params = connection_params
        self.conn: Optional[SnowflakeConnection] = None
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def connect(self) -> None:
        """Establish connection to Snowflake."""
        try:
            self.conn = snowflake.connector.connect(**self.connection_params)
            logger.info(
                f"Connected to Snowflake (Database: {self.conn.database}, "
                f"Schema: {self.conn.schema}, Warehouse: {self.conn.warehouse})"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def close(self) -> None:
        """Close Snowflake connection."""
        if self.conn:
            self.conn.close()
            logger.info("Closed Snowflake connection")
    
    def execute_query(self, query: str, params: Optional[dict] = None) -> Any:
        """Execute a query and return results.
        
        Args:
            query: SQL query to execute.
            params: Optional parameters for the query.
            
        Returns:
            Query results.
        """
        if not self.conn:
            raise RuntimeError("Not connected to Snowflake")
        
        cursor = self.conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()
    
    def execute_command(self, command: str) -> None:
        """Execute a command without returning results.
        
        Args:
            command: SQL command to execute.
        """
        if not self.conn:
            raise RuntimeError("Not connected to Snowflake")
        
        cursor = self.conn.cursor()
        try:
            cursor.execute(command)
            logger.debug(f"Executed command: {command[:100]}...")
        finally:
            cursor.close()
    
    def ensure_schema(self, database: str, schema: str) -> None:
        """Ensure a schema exists in the specified database.
        
        Args:
            database: Database name.
            schema: Schema name.
        """
        try:
            logger.info(f"Ensuring schema {database}.{schema} exists...")
            self.execute_command(f'CREATE SCHEMA IF NOT EXISTS "{database}"."{schema}"')
            logger.info(f"Schema {database}.{schema} ensured")
        except Exception as e:
            logger.error(f"Error ensuring schema {database}.{schema}: {e}")
            raise
    
    def get_tables(self, database: str, schema: str) -> List[str]:
        """Get list of tables in a schema.
        
        Args:
            database: Database name.
            schema: Schema name.
            
        Returns:
            List of table names.
        """
        logger.info(f"Fetching tables from {database}.{schema}...")
        query = f'SHOW TABLES IN SCHEMA "{database}"."{schema}"'
        results = self.execute_query(query)
        tables = [row[1] for row in results]
        logger.info(f"Found {len(tables)} tables")
        return tables
    
    def table_exists(self, database: str, schema: str, table: str) -> bool:
        """Check if a table exists.
        
        Args:
            database: Database name.
            schema: Schema name.
            table: Table name.
            
        Returns:
            True if table exists, False otherwise.
        """
        try:
            query = f"""
                SELECT COUNT(*) 
                FROM "{database}".INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema}' 
                AND TABLE_NAME = '{table}'
            """
            result = self.execute_query(query)
            return result[0][0] > 0
        except Exception:
            # Fallback to SHOW TABLES
            tables = self.get_tables(database, schema)
            return table in tables
    
    def copy_table(
        self,
        source_db: str,
        source_schema: str,
        source_table: str,
        target_db: str,
        target_schema: str,
        target_table: Optional[str] = None
    ) -> None:
        """Copy a table from source to target location.
        
        Args:
            source_db: Source database name.
            source_schema: Source schema name.
            source_table: Source table name.
            target_db: Target database name.
            target_schema: Target schema name.
            target_table: Target table name (defaults to source_table).
        """
        if target_table is None:
            target_table = source_table
        
        source_fqn = f'"{source_db}"."{source_schema}"."{source_table}"'
        target_fqn = f'"{target_db}"."{target_schema}"."{target_table}"'
        
        logger.info(f"Copying table {source_fqn} to {target_fqn}...")
        try:
            sql = f"CREATE OR REPLACE TABLE {target_fqn} AS SELECT * FROM {source_fqn}"
            self.execute_command(sql)
            logger.info(f"Successfully copied table {source_table}")
        except Exception as e:
            logger.error(f"Error copying table {source_table}: {e}")
            raise
    
    def ensure_storage_integration(
        self,
        integration_name: str,
        gcs_bucket: str
    ) -> None:
        """Ensure storage integration exists for GCS.
        
        Args:
            integration_name: Name of the storage integration.
            gcs_bucket: GCS bucket name.
        """
        logger.info(f"Ensuring storage integration {integration_name}...")
        try:
            sql = f"""
                CREATE OR REPLACE STORAGE INTEGRATION {integration_name}
                TYPE = EXTERNAL_STAGE
                STORAGE_PROVIDER = GCS
                ENABLED = TRUE
                STORAGE_ALLOWED_LOCATIONS = ('gcs://{gcs_bucket}/', 'gcs://{gcs_bucket}/*')
            """
            self.execute_command(sql)
            
            # Grant usage to current role
            current_role = self.execute_query("SELECT CURRENT_ROLE()")[0][0]
            self.execute_command(f"GRANT USAGE ON INTEGRATION {integration_name} TO ROLE {current_role}")
            
            logger.info(f"Storage integration {integration_name} ensured")
        except Exception as e:
            logger.error(f"Error ensuring storage integration: {e}")
            raise
    
    def ensure_gcs_stage(
        self,
        database: str,
        schema: str,
        stage_name: str,
        gcs_bucket: str,
        integration_name: str
    ) -> None:
        """Ensure GCS external stage exists.
        
        Args:
            database: Database name.
            schema: Schema name.
            stage_name: Stage name.
            gcs_bucket: GCS bucket name.
            integration_name: Storage integration name.
        """
        stage_fqn = f'"{database}"."{schema}"."{stage_name}"'
        logger.info(f"Ensuring stage {stage_fqn} for bucket gs://{gcs_bucket}/...")
        
        try:
            sql = f"""
                CREATE OR REPLACE STAGE {stage_fqn}
                URL='gcs://{gcs_bucket}/'
                STORAGE_INTEGRATION = {integration_name}
                FILE_FORMAT = (TYPE = PARQUET, COMPRESSION = SNAPPY)
            """
            self.execute_command(sql)
            logger.info(f"Stage {stage_fqn} ensured")
        except Exception as e:
            logger.error(f"Error ensuring GCS stage: {e}")
            raise
    
    def export_table_to_gcs(
        self,
        database: str,
        schema: str,
        table: str,
        stage_name: str
    ) -> None:
        """Export a table to GCS via external stage.
        
        Args:
            database: Database name.
            schema: Schema name.
            table: Table name.
            stage_name: Stage name.
        """
        table_fqn = f'"{database}"."{schema}"."{table}"'
        stage_path = f'@"{database}"."{schema}"."{stage_name}"/{table}/'
        
        logger.info(f"Exporting {table_fqn} to GCS via {stage_path}")
        
        try:
            sql = f"""
                COPY INTO {stage_path}
                FROM {table_fqn}
                FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
                HEADER = TRUE
                OVERWRITE = TRUE
            """
            self.execute_command(sql)
            logger.info(f"Successfully exported {table} to GCS")
        except Exception as e:
            logger.error(f"Error exporting table {table} to GCS: {e}")
            raise
    
    def get_table_row_count(self, database: str, schema: str, table: str) -> int:
        """Get row count for a table.
        
        Args:
            database: Database name.
            schema: Schema name.
            table: Table name.
            
        Returns:
            Number of rows in the table.
        """
        query = f'SELECT COUNT(*) FROM "{database}"."{schema}"."{table}"'
        result = self.execute_query(query)
        return result[0][0] if result else 0