# -------------------- Imports --------------------
import os
import sys
import logging
import argparse
from pathlib import Path
from typing import Optional
import snowflake.connector
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -------------------- Load Configuration --------------------
def load_config():
    """Load configuration from environment variables."""
    # Find .env file
    current_path = Path(__file__).resolve()
    project_root = None
    for parent in current_path.parents:
        if (parent / "pyproject.toml").exists():
            project_root = parent
            break
    
    if project_root:
        env_path = project_root / "common" / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            logger.info(f"Loaded configuration from {env_path}")
    
    # Configuration from environment variables
    config = {
        # Google Cloud
        'PROJECT_ID': os.getenv('BQ_PROJECT_ID', os.getenv('GOOGLE_CLOUD_PROJECT')),
        'BQ_CREDENTIALS_FILE': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        
        # Snowflake
        'SNOWFLAKE_USER': os.getenv('SNOWFLAKE_USER'),
        'SNOWFLAKE_ACCOUNT': os.getenv('SNOWFLAKE_ACCOUNT'),
        'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'SNOWFLAKE_DATABASE': os.getenv('SNOWFLAKE_DATABASE'),
        'SNOWFLAKE_SCHEMA': os.getenv('SNOWFLAKE_SCHEMA'),
        'SNOWFLAKE_PAT': os.getenv('SNOWFLAKE_PAT'),
        
        # Staging
        'SNOWFLAKE_STAGING_DATABASE': os.getenv('SNOWFLAKE_STAGING_DATABASE', 'CONFLIXIS_STAGE'),
        'SNOWFLAKE_STAGING_SCHEMA': os.getenv('SNOWFLAKE_STAGING_SCHEMA', 'PUBLIC'),
        'GCS_BUCKET': os.getenv('SNOWFLAKE_GCS_BUCKET', 'snowflake_dh_bq'),
        'BQ_TARGET_DATASET': os.getenv('SNOWFLAKE_BQ_TARGET_DATASET', 'CONFLIXIS_309340'),
        'GCS_STAGE_OBJECT_NAME': os.getenv('SNOWFLAKE_GCS_STAGE_NAME', 'snowflake_dh_bq'),
        'STORAGE_INTEGRATION_NAME': os.getenv('SNOWFLAKE_STORAGE_INTEGRATION', 'GCS_INT'),
    }
    
    return config

# Load configuration
config = load_config()


# -------------------- Main Processing Functions --------------------
def validate_config(config):
    """Validate that all required configuration values are present."""
    required_fields = [
        'PROJECT_ID', 'BQ_CREDENTIALS_FILE', 'SNOWFLAKE_USER', 'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA', 'SNOWFLAKE_PAT'
    ]
    
    missing_fields = []
    for field in required_fields:
        if not config.get(field):
            missing_fields.append(field)
    
    if missing_fields:
        logger.error(f"Missing required configuration fields: {', '.join(missing_fields)}")
        logger.error("Please check your .env file in the common/ directory")
        sys.exit(1)
    
    # Validate Google credentials file exists
    if config['BQ_CREDENTIALS_FILE']:
        creds_path = Path(config['BQ_CREDENTIALS_FILE'])
        if not creds_path.exists():
            logger.error(f"Google credentials file not found: {config['BQ_CREDENTIALS_FILE']}")
            sys.exit(1)


def create_snowflake_connection(config):
    """
    Creates and returns a Snowflake connection object.
    Connects initially to the database specified in config (source DB).
    """
    try:
        conn = snowflake.connector.connect(
            user=config["SNOWFLAKE_USER"],
            password=config["SNOWFLAKE_PAT"],  # Using PAT as password
            account=config["SNOWFLAKE_ACCOUNT"],
            warehouse=config["SNOWFLAKE_WAREHOUSE"],
            database=config["SNOWFLAKE_DATABASE"],
            schema=config["SNOWFLAKE_SCHEMA"],
        )
        logger.info("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


def ensure_snowflake_schema(conn, database_name, schema_name):
    """
    Ensures a specific schema exists in the given Snowflake database.
    Assumes database_name already exists and the user has permissions.
    """
    cur = conn.cursor()
    try:
        logger.info(f"Ensuring Snowflake schema {database_name}.{schema_name} exists...")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}")
        logger.info(f"Schema {database_name}.{schema_name} ensured.")
    except Exception as e:
        logger.error(f"Error ensuring Snowflake schema {database_name}.{schema_name}: {e}")
        logger.error(f"Please ensure database '{database_name}' exists and user has CREATE SCHEMA privileges in it.")
        raise


def copy_table_to_different_db_staging(conn, table_name,
                                       source_db, source_schema,
                                       staging_db, staging_schema):
    """
    Copies a table from the source database/schema to a staging database/schema.
    """
    cur = conn.cursor()
    # Quote identifiers to handle potential special characters or case sensitivity
    source_table_fqn = f'"{source_db}"."{source_schema}"."{table_name}"'
    staging_table_fqn = f'"{staging_db}"."{staging_schema}"."{table_name}"'

    logger.info(f"Copying table {source_table_fqn} to {staging_table_fqn}...")
    
    try:
        # Set timeout to 60 minutes (3600 seconds)
        cur.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3600")
        logger.info("Set statement timeout to 60 minutes")
        
        # Always create or replace to get the latest copy
        copy_sql = f"CREATE OR REPLACE TABLE {staging_table_fqn} AS SELECT * FROM {source_table_fqn};"
        cur.execute(copy_sql)
        logger.info(f"Table {table_name} copied to {staging_table_fqn}.")
    except Exception as e:
        logger.error(f"Error copying table {table_name} to {staging_table_fqn}: {e}")
        raise


def get_all_tables(conn, database_name, schema_name):
    """Fetches all table names from the specified Snowflake database and schema."""
    cur = conn.cursor()
    qualified_schema_name = f'"{database_name}"."{schema_name}"'
    logger.info(f"Fetching tables from {qualified_schema_name}...")
    cur.execute(f"SHOW TABLES IN SCHEMA {qualified_schema_name}")
    tables = [row[1] for row in cur.fetchall()]
    logger.info(f"Found {len(tables)} tables: {tables if tables else 'None'}")
    return tables

def ensure_gcs_stage_in_db_schema(conn, gcs_bucket_name,
                                  db_for_stage, schema_for_stage,
                                  stage_object_name, integration_name):
    """
    Ensures GCS storage integration and an external stage object exist in the specified
    Snowflake database and schema.
    """
    cur = conn.cursor()

    logger.info(f"Ensuring storage integration {integration_name}...")
    cur.execute(f"""
        CREATE OR REPLACE STORAGE INTEGRATION {integration_name}
          TYPE = EXTERNAL_STAGE
          STORAGE_PROVIDER = GCS
          ENABLED = TRUE
          STORAGE_ALLOWED_LOCATIONS = ('gcs://{gcs_bucket_name}/', 'gcs://{gcs_bucket_name}/*');
    """)
    logger.info(f"Ensured storage integration {integration_name}.")

    cur.execute("SELECT CURRENT_ROLE();")
    role = cur.fetchone()[0]
    logger.info(f"Granting USAGE on integration {integration_name} to role {role}...")
    cur.execute(f"GRANT USAGE ON INTEGRATION {integration_name} TO ROLE {role};")
    logger.info(f"Granted USAGE on integration {integration_name} to role {role}.")

    fully_qualified_stage_name = f'"{db_for_stage}"."{schema_for_stage}"."{stage_object_name}"'
    logger.info(f"Ensuring stage object {fully_qualified_stage_name} for GCS bucket gs://{gcs_bucket_name}/...")
    cur.execute(f"""
        CREATE OR REPLACE STAGE {fully_qualified_stage_name}
          URL='gcs://{gcs_bucket_name}/'
          STORAGE_INTEGRATION = {integration_name} 
          FILE_FORMAT = (TYPE = PARQUET, COMPRESSION = SNAPPY);
    """)
    logger.info(f"Ensured stage object {fully_qualified_stage_name} → gs://{gcs_bucket_name}/")


def copy_sf_staging_db_to_gcs(table_name, conn,
                              snowflake_staging_db, snowflake_staging_schema,
                              gcs_stage_object_name):  # gcs_stage_object_name is "snowflake_dh_bq"
    cur = conn.cursor()

    # CORRECTED LINE: Ensure each part of the stage FQN, especially the object name, is double-quoted
    stage_object_path = f'@"{(snowflake_staging_db)}"."{(snowflake_staging_schema)}"."{(gcs_stage_object_name)}"/{table_name}/'
    # This will produce, e.g., @"CONFLIXIS_STAGE"."PUBLIC"."snowflake_dh_bq"/PHYSICIANS_OVERVIEW/

    table_to_copy_fqn = f'"{snowflake_staging_db}"."{snowflake_staging_schema}"."{table_name}"'

    logger.info(f"Copying {table_to_copy_fqn} → GCS via stage ({stage_object_path})")
    # Note: HEADER = TRUE in Parquet files includes column names in the file metadata, not as a data row
    copy_sql = f"""
        COPY INTO {stage_object_path}
        FROM {table_to_copy_fqn}
        FILE_FORMAT = (TYPE = PARQUET, COMPRESSION = SNAPPY)
        MAX_FILE_SIZE = 268435456
        OVERWRITE = TRUE;
    """
    cur.execute(copy_sql)
    logger.info(f"{table_name} from {table_to_copy_fqn} → gs://{config['GCS_BUCKET']}/{table_name}/")


def get_snowflake_table_schema(conn, database, schema, table_name):
    """Get column names from Snowflake table."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
            FROM "{database}".INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        columns = cursor.fetchall()
        return [(col[0], col[1]) for col in columns]
    finally:
        cursor.close()


def snowflake_to_bq_field_type(sf_type):
    """Convert Snowflake data type to BigQuery data type."""
    sf_type = sf_type.upper()
    if sf_type.startswith('NUMBER') or sf_type == 'INTEGER':
        return 'NUMERIC'
    elif sf_type.startswith('FLOAT') or sf_type.startswith('DOUBLE'):
        return 'FLOAT64'
    elif sf_type in ['TEXT', 'VARCHAR', 'STRING', 'CHAR']:
        return 'STRING'
    elif sf_type == 'BOOLEAN':
        return 'BOOLEAN'
    elif sf_type == 'DATE':
        return 'DATE'
    elif sf_type.startswith('TIMESTAMP'):
        return 'TIMESTAMP'
    elif sf_type == 'TIME':
        return 'TIME'
    else:
        return 'STRING'  # Default to STRING for unknown types


def load_gcs_to_bigquery(table_name, bq_client, gcp_project_id, gcs_bucket_name, bq_dataset_name, sf_schema=None):
    """Loads data from GCS (Parquet files) into a BigQuery table."""
    table_id = f"{gcp_project_id}.{bq_dataset_name}.{table_name}"
    uri = f"gs://{gcs_bucket_name}/{table_name}/*"

    # First, load with autodetect to get the data
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    logger.info(f"Loading data from {uri} into BigQuery table {table_id}")
    try:
        # Ensure BQ dataset exists
        dataset_id_full = f"{gcp_project_id}.{bq_dataset_name}"
        try:
            bq_client.get_dataset(dataset_id_full)
            logger.info(f"BQ Dataset {bq_dataset_name} already exists.")
        except Exception:
            logger.info(f"BQ Dataset {bq_dataset_name} not found, creating it...")
            bq_client.create_dataset(dataset_id_full, exists_ok=True)
            logger.info(f"BQ Dataset {bq_dataset_name} created.")

        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        
        # If we have schema info, rename columns
        if sf_schema:
            logger.info("Renaming columns to match Snowflake schema...")
            
            # Build SQL to create a new table with proper column names
            select_parts = []
            for i, (col_name, col_type) in enumerate(sf_schema):
                select_parts.append(f"_COL_{i} AS {col_name}")
            
            select_sql = ", ".join(select_parts)
            
            # Create temporary table with renamed columns
            temp_table_id = f"{table_id}_temp"
            query = f"""
                CREATE OR REPLACE TABLE `{temp_table_id}` AS
                SELECT {select_sql}
                FROM `{table_id}`
            """
            
            query_job = bq_client.query(query)
            query_job.result()
            
            # Drop original table and rename temp table
            bq_client.delete_table(table_id)
            
            # Copy temp table to original table name
            copy_query = f"""
                CREATE OR REPLACE TABLE `{table_id}` AS
                SELECT * FROM `{temp_table_id}`
            """
            copy_job = bq_client.query(copy_query)
            copy_job.result()
            
            # Drop temp table
            bq_client.delete_table(temp_table_id)
            
            logger.info("Column renaming completed")
        
        destination_table = bq_client.get_table(table_id)
        logger.info(f"{table_name} loaded into {table_id}. Rows: {destination_table.num_rows}")
    except Exception as e:
        logger.error(f"Error loading {table_name} to BigQuery: {e}")
        raise


def main(table_name: Optional[str] = None, dry_run: bool = False, skip_staging_copy: bool = False):
    """Main execution function.
    
    Args:
        table_name: Optional specific table to process. If not provided, will process all tables.
        dry_run: If True, only validate configuration without transferring data.
    """
    # Validate configuration
    validate_config(config)
    
    if config['BQ_CREDENTIALS_FILE']:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config['BQ_CREDENTIALS_FILE']
    
    snowflake_source_db_name = config["SNOWFLAKE_DATABASE"]
    snowflake_source_schema = config["SNOWFLAKE_SCHEMA"]
    snowflake_staging_db_name = config["SNOWFLAKE_STAGING_DATABASE"]
    snowflake_staging_schema_name = config["SNOWFLAKE_STAGING_SCHEMA"]
    bq_final_dataset = config["BQ_TARGET_DATASET"]
    
    # Initialize BigQuery client
    try:
        bq_gcp_creds = service_account.Credentials.from_service_account_file(config['BQ_CREDENTIALS_FILE'])
        bq_client = bigquery.Client(credentials=bq_gcp_creds, project=config['PROJECT_ID'])
        logger.info(f"Initialized BigQuery client for project {config['PROJECT_ID']}. Target BQ Dataset: {bq_final_dataset}")
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        sys.exit(1)
    
    if dry_run:
        logger.info("DRY RUN MODE - Validating setup only...")
        try:
            # Test Snowflake connection
            sf_conn = create_snowflake_connection(config)
            sf_conn.close()
            logger.info("Snowflake connection validated successfully")
            
            # Test BigQuery connection
            datasets = list(bq_client.list_datasets())
            logger.info(f"BigQuery connection validated (found {len(datasets)} datasets)")
            
            logger.info("DRY RUN COMPLETE - All validations passed")
            return
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            sys.exit(1)

    sf_conn = None
    try:
        sf_conn = create_snowflake_connection(config)
        logger.info(
            f"Connected to Snowflake (Initial context: DB={sf_conn.database}, Schema={sf_conn.schema}, Wh={sf_conn.warehouse})")
        
        # Determine tables to process
        if table_name:
            tables_to_process = [table_name]
            logger.info(f"Processing single table: {table_name}")
        else:
            # Get all tables from source schema
            tables_to_process = get_all_tables(sf_conn, snowflake_source_db_name, snowflake_source_schema)
            if not tables_to_process:
                logger.warning(f"No tables found in {snowflake_source_db_name}.{snowflake_source_schema}")
                return
            logger.info(f"Processing {len(tables_to_process)} tables")

        # --- Step 1: Ensure Snowflake Staging Schema Exists IN YOUR STAGING DATABASE ---
        ensure_snowflake_schema(sf_conn, snowflake_staging_db_name, snowflake_staging_schema_name)
        logger.info(f"Snowflake staging schema {snowflake_staging_db_name}.{snowflake_staging_schema_name} is ready.")

        # --- Step 2: Copy tables from Source Database to Staging Database ---
        for i, table in enumerate(tables_to_process, 1):
            logger.info(f"\n--- Processing table {i}/{len(tables_to_process)}: '{table}' ---")
            
            try:
                # Copy table to staging (unless skipped)
                if not skip_staging_copy:
                    copy_table_to_different_db_staging(sf_conn, table,
                                                     snowflake_source_db_name, snowflake_source_schema,
                                                     snowflake_staging_db_name, snowflake_staging_schema_name)
                    logger.info(
                        f"Table '{table}' from {snowflake_source_db_name}.{snowflake_source_schema} copied to {snowflake_staging_db_name}.{snowflake_staging_schema_name}.")
                else:
                    logger.info(f"Skipping staging copy for table '{table}' (--skip-staging-copy flag set)")

                # Ensure GCS Stage object exists (only need to do this once)
                if i == 1:
                    logger.info("Preparing GCS Export infrastructure...")
                    ensure_gcs_stage_in_db_schema(sf_conn, config['GCS_BUCKET'],
                                                db_for_stage=snowflake_staging_db_name,
                                                schema_for_stage=snowflake_staging_schema_name,
                                                stage_object_name=config['GCS_STAGE_OBJECT_NAME'],
                                                integration_name=config['STORAGE_INTEGRATION_NAME'])

                # Export to GCS and load to BigQuery
                logger.info(f"Migrating table '{table}' from Staging Snowflake DB to GCS & BigQuery...")
                copy_sf_staging_db_to_gcs(table, sf_conn,
                                        snowflake_staging_db_name,
                                        snowflake_staging_schema_name,
                                        config['GCS_STAGE_OBJECT_NAME'])

                # Get schema from source table
                logger.info(f"Getting schema for table '{table}'...")
                sf_schema = get_snowflake_table_schema(
                    sf_conn, 
                    snowflake_source_db_name, 
                    snowflake_source_schema, 
                    table
                )
                
                load_gcs_to_bigquery(table, bq_client, config['PROJECT_ID'], config['GCS_BUCKET'],
                                   bq_dataset_name=bq_final_dataset, sf_schema=sf_schema)

                logger.info(f"Migration of table '{table}' completed successfully!")
                
            except Exception as e:
                logger.error(f"Failed to process table '{table}': {e}")
                if len(tables_to_process) == 1:
                    raise  # Re-raise if single table
                # Continue with next table if processing multiple
        
        logger.info(f"\nMigration completed! Processed {len(tables_to_process)} table(s).")

    except snowflake.connector.errors.ProgrammingError as spe:
        logger.error(f"Snowflake Programming Error: {spe}")
        if "does not exist or not authorized" in str(spe):
            logger.error("This often indicates a permissions issue or a misnamed database/schema/table. Please check:")
            logger.error(f"  - Source DB/Schema: {snowflake_source_db_name}.{snowflake_source_schema}")
            logger.error(f"  - Staging DB/Schema: {snowflake_staging_db_name}.{snowflake_staging_schema_name}")
            logger.error(f"  - User '{config.get('SNOWFLAKE_USER')}' permissions.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if sf_conn:
            sf_conn.close()
            logger.info("Closed Snowflake connection.")


# ----------------------------- Script Execution -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transfer tables from Snowflake to BigQuery via GCS"
    )
    parser.add_argument(
        "--table",
        help="Specific table name to transfer. If not provided, transfers all tables.",
        default=None
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without transferring data"
    )
    parser.add_argument(
        "--skip-staging-copy",
        action="store_true",
        help="Skip copying to staging database (use if table already exists in staging)"
    )
    
    args = parser.parse_args()
    
    try:
        main(table_name=args.table, dry_run=args.dry_run, skip_staging_copy=args.skip_staging_copy)
    except KeyboardInterrupt:
        logger.info("\nTransfer interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Transfer failed: {e}")
        sys.exit(1)
