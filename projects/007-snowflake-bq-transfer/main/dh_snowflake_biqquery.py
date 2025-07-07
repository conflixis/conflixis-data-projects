# -------------------- Imports --------------------
import os
import snowflake.connector
from google.cloud import bigquery
from google.oauth2 import service_account

# -------------------- Configurable Parameters --------------------
SF_CREDENTIALS_FILE = r"C:\Users\vince\OneDrive\Documents\Conflixis\conflixis_dataprep\ETL_DH_Snowflake_Biqguery\sf_creds.txt"
BQ_CREDENTIALS_FILE = r"C:\Users\vince\OneDrive\Documents\Conflixis\conflixis_dataprep\ETL_DH_Snowflake_Biqguery\data-analytics-389803-957bcd9f5b35.json"
GCS_BUCKET = "snowflake_dh_bq"
PROJECT_ID = "data-analytics-389803"

# --- Snowflake Staging Configuration ---
# IMPORTANT: This is YOUR OWN Snowflake database where you have write permissions.
SNOWFLAKE_STAGING_DATABASE = "CONFLIXIS_STAGE"  # Your target database
# Schema within your SNOWFLAKE_STAGING_DATABASE to use for staging tables and the GCS stage object.
# Using "PUBLIC" is common if it exists and you have rights, or create a dedicated one.
SNOWFLAKE_STAGING_SCHEMA = "PUBLIC"  # Or e.g., "GCS_EXPORT_SCHEMA"

# --- BigQuery Configuration ---
# Target BigQuery dataset name. If empty, SNOWFLAKE_STAGING_SCHEMA from above will be used.
BQ_TARGET_DATASET = "CONFLIXIS_STAGE_BQ"  # Or set to ""

# --- Snowflake Object Names (within your staging database/schema) ---
# Name for the GCS external stage object in Snowflake
GCS_STAGE_OBJECT_NAME = "MY_GCS_EXPORT_STAGE"  # e.g., CONFLIXIS_GCS_STAGE_OBJ
# Name for the Storage Integration (this is a non-schema specific object)
STORAGE_INTEGRATION_NAME = "GCS_INT"


# -------------------- Main Processing Functions --------------------
def load_snowflake_creds(path):
    """Loads Snowflake credentials from a properties-like file."""
    creds = {}
    with open(path, "r") as f:
        for line in f:
            if "=" in line:
                key, val = line.strip().split(" = ", 1)
                creds[key] = val.strip('"')
    return creds


def create_snowflake_connection(sf_creds):
    """
    Creates and returns a Snowflake connection object.
    Connects initially to the database specified in sf_creds (source DB).
    """
    return snowflake.connector.connect(
        user=sf_creds["SNOWFLAKE_USER"],
        password=sf_creds["SNOWFLAKE_PAT"],  # Using PAT as password
        account=sf_creds["SNOWFLAKE_ACCOUNT"],
        warehouse=sf_creds["SNOWFLAKE_WAREHOUSE"],
        # Initial connection to the source database for reading
        database=sf_creds["SNOWFLAKE_DATABASE"],
        schema=sf_creds["SNOWFLAKE_SCHEMA"],
    )


def ensure_snowflake_schema(conn, database_name, schema_name):
    """
    Ensures a specific schema exists in the given Snowflake database.
    Assumes database_name already exists and the user has permissions.
    """
    cur = conn.cursor()
    try:
        print(f"🔄 Ensuring Snowflake schema {database_name}.{schema_name} exists...")
        # Check if database exists and switch to it, or use fully qualified name
        # For simplicity, assuming user has rights in database_name
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}")
        print(f"✅ Schema {database_name}.{schema_name} ensured.")
    except Exception as e:
        print(f"❌ Error ensuring Snowflake schema {database_name}.{schema_name}: {e}")
        print(f"ℹ️ Please ensure database '{database_name}' exists and user has CREATE SCHEMA privileges in it.")
        raise


def copy_table_to_different_db_staging(conn, table_name,
                                       source_db, source_schema,
                                       staging_db, staging_schema):
    """
    Copies a table from the source database/schema to a staging database/schema.
    """
    cur = conn.cursor()
    source_table_fqn = f'"{source_db}"."{source_schema}"."{table_name}"'  # Quoting for special chars/case
    staging_table_fqn = f'"{staging_db}"."{staging_schema}"."{table_name}"'

    print(f"🔄 Copying table {source_table_fqn} to {staging_table_fqn}...")
    try:
        # Use CREATE OR REPLACE TABLE ... AS SELECT ... for copying
        copy_sql = f"CREATE OR REPLACE TABLE {staging_table_fqn} AS SELECT * FROM {source_table_fqn};"
        cur.execute(copy_sql)
        print(f"✅ Table {table_name} copied to {staging_table_fqn}.")
    except Exception as e:
        print(f"❌ Error copying table {table_name} to {staging_table_fqn}: {e}")
        raise


def get_all_tables(conn, database_name, schema_name):
    """Fetches all table names from the specified Snowflake database and schema."""
    cur = conn.cursor()
    # Using fully qualified schema name for SHOW TABLES
    qualified_schema_name = f'"{database_name}"."{schema_name}"'
    print(f"📋 Fetching tables from {qualified_schema_name}...")
    cur.execute(f"SHOW TABLES IN SCHEMA {qualified_schema_name}")
    tables = [row[1] for row in cur.fetchall()]
    print(f"Found {len(tables)} tables: {tables if tables else 'None'}")
    return tables


def ensure_gcs_stage_in_db_schema(conn, gcs_bucket_name,
                                  db_for_stage, schema_for_stage,  # These define where the STAGE object is created
                                  stage_object_name, integration_name):
    """
    Ensures GCS storage integration and an external stage object exist in the specified
    Snowflake database and schema.
    """
    cur = conn.cursor()

    print(f"🔄 Ensuring storage integration {integration_name}...")
    # Storage Integration is a non-schema object, created once
    cur.execute(f"""
        CREATE OR REPLACE STORAGE INTEGRATION {integration_name}
          TYPE = EXTERNAL_STAGE
          STORAGE_PROVIDER = GCS
          ENABLED = TRUE
          STORAGE_ALLOWED_LOCATIONS = ('gcs://{gcs_bucket_name}/', 'gcs://{gcs_bucket_name}/*');
    """)
    print(f"✅ Ensured storage integration {integration_name}.")

    cur.execute("SELECT CURRENT_ROLE();")
    role = cur.fetchone()[0]
    print(f"🔄 Granting USAGE on integration {integration_name} to role {role}...")
    cur.execute(f"GRANT USAGE ON INTEGRATION {integration_name} TO ROLE {role};")
    print(f"✅ Granted USAGE on integration {integration_name} to role {role}.")

    # Stage object is created within the specified database and schema
    fully_qualified_stage_name = f'"{db_for_stage}"."{schema_for_stage}"."{stage_object_name}"'
    print(f"🔄 Ensuring stage object {fully_qualified_stage_name} for GCS bucket gs://{gcs_bucket_name}/...")
    cur.execute(f"""
        CREATE OR REPLACE STAGE {fully_qualified_stage_name}
          URL='gcs://{gcs_bucket_name}/'
          STORAGE_INTEGRATION = '{integration_name}'
          FILE_FORMAT = (TYPE = PARQUET, COMPRESSION = SNAPPY);
    """)  # Note: integration_name in stage DDL might need quotes if it has special chars or case.
    print(f"✅ Ensured stage object {fully_qualified_stage_name} → gs://{gcs_bucket_name}/")


def copy_sf_staging_db_to_gcs(table_name, conn,
                              snowflake_staging_db, snowflake_staging_schema,
                              # DB/Schema where staged tables and GCS stage object reside
                              gcs_stage_object_name):
    """Copies a table from the Snowflake staging database/schema to GCS via the specified stage object."""
    cur = conn.cursor()

    # Path to the GCS stage object
    stage_object_path = f'@{snowflake_staging_db}.{snowflake_staging_schema}.{gcs_stage_object_name}/{table_name}/'
    # Table to be copied is in the Snowflake staging database/schema
    table_to_copy_fqn = f'"{snowflake_staging_db}"."{snowflake_staging_schema}"."{table_name}"'

    print(f"🔄 Copying {table_to_copy_fqn} → GCS via stage ({stage_object_path})")
    copy_sql = f"""
        COPY INTO {stage_object_path}
        FROM {table_to_copy_fqn}
        FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
        HEADER = TRUE
        OVERWRITE = TRUE;
    """
    cur.execute(copy_sql)
    print(f"✅ {table_name} from {table_to_copy_fqn} → gs://{GCS_BUCKET}/{table_name}/")


def load_gcs_to_bigquery(table_name, bq_client, gcp_project_id, gcs_bucket_name, bq_dataset_name):
    """Loads data from GCS (Parquet files) into a BigQuery table."""
    table_id = f"{gcp_project_id}.{bq_dataset_name}.{table_name}"
    uri = f"gs://{gcs_bucket_name}/{table_name}/*"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    print(f"🔄 Loading data from {uri} into BigQuery table {table_id}")
    try:
        # Optional: Ensure BQ dataset exists
        try:
            bq_client.get_dataset(f"{gcp_project_id}.{bq_dataset_name}")
            print(f"BQ Dataset {bq_dataset_name} already exists.")
        except Exception:  # google.cloud.exceptions.NotFound
            print(f"BQ Dataset {bq_dataset_name} not found, creating it...")
            bq_client.create_dataset(f"{gcp_project_id}.{bq_dataset_name}", exists_ok=True)
            print(f"BQ Dataset {bq_dataset_name} created.")

        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        destination_table = bq_client.get_table(table_id)
        print(f"✅ {table_name} loaded into {table_id}. Rows: {destination_table.num_rows}")
    except Exception as e:
        print(f"❌ Error loading {table_name} to BigQuery: {e}")
        raise


# ----------------------------- Script Execution -----------------------------
if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BQ_CREDENTIALS_FILE

    # --- Load Source Snowflake Credentials ---
    sf_creds = load_snowflake_creds(SF_CREDENTIALS_FILE)
    snowflake_source_db_name = sf_creds["SNOWFLAKE_DATABASE"]  # e.g., CONFLIXIS_309340 (shared, read-only)
    snowflake_source_schema = sf_creds["SNOWFLAKE_SCHEMA"]  # e.g., CONFLIXIS_309340 (schema in shared DB)

    # --- Staging and Target Configuration (from script parameters) ---
    snowflake_staging_db_name = SNOWFLAKE_STAGING_DATABASE  # Your own DB, e.g., CONFLIXIS_STAGE
    snowflake_staging_schema_name = SNOWFLAKE_STAGING_SCHEMA  # Schema in your DB, e.g., PUBLIC

    bq_final_dataset = BQ_TARGET_DATASET if BQ_TARGET_DATASET else snowflake_staging_schema_name

    # --- Initialize BQ Client ---
    bq_gcp_creds = service_account.Credentials.from_service_account_file(BQ_CREDENTIALS_FILE)
    bq_client = bigquery.Client(credentials=bq_gcp_creds, project=PROJECT_ID)
    print(f"✅ Initialized BigQuery client for project {PROJECT_ID}. Target BQ Dataset: {bq_final_dataset}")

    sf_conn = None
    try:
        # --- Connect to Snowflake (initially to source DB for reading) ---
        sf_conn = create_snowflake_connection(sf_creds)
        print(
            f"✅ Connected to Snowflake (Initial context: DB={sf_conn.database}, Schema={sf_conn.schema}, Wh={sf_conn.warehouse})")

        # --- Step 1: Ensure Snowflake Staging Schema Exists IN YOUR STAGING DATABASE ---
        # Assumes SNOWFLAKE_STAGING_DATABASE already exists and user has USAGE on it.
        ensure_snowflake_schema(sf_conn, snowflake_staging_db_name, snowflake_staging_schema_name)
        print(f"✅ Snowflake staging schema {snowflake_staging_db_name}.{snowflake_staging_schema_name} is ready.")

        # --- Step 2: Get tables from Source Shared Database ---
        print(f"\n--- Stage 1: Copying tables from Source to Staging Snowflake Database ---")
        source_tables = get_all_tables(sf_conn, snowflake_source_db_name, snowflake_source_schema)
        if not source_tables:
            print(f"⚠️ No tables found in source {snowflake_source_db_name}.{snowflake_source_schema}. Exiting.")
        else:
            print(
                f"Found {len(source_tables)} tables in source {snowflake_source_db_name}.{snowflake_source_schema} to copy to staging DB.")
            for idx, table_name in enumerate(source_tables, start=1):
                print(f"\nProcessing table for Snowflake staging [{idx}/{len(source_tables)}]: {table_name}")
                copy_table_to_different_db_staging(sf_conn, table_name,
                                                   snowflake_source_db_name, snowflake_source_schema,
                                                   snowflake_staging_db_name, snowflake_staging_schema_name)
            print(
                f"\n✅ All tables from {snowflake_source_db_name}.{snowflake_source_schema} copied to {snowflake_staging_db_name}.{snowflake_staging_schema_name}.")

        # --- Step 3: Ensure GCS Stage object exists IN YOUR STAGING DATABASE/SCHEMA ---
        print(f"\n--- Stage 2: Preparing GCS Export from Staging Snowflake Database ---")
        ensure_gcs_stage_in_db_schema(sf_conn, GCS_BUCKET,
                                      db_for_stage=snowflake_staging_db_name,
                                      schema_for_stage=snowflake_staging_schema_name,
                                      stage_object_name=GCS_STAGE_OBJECT_NAME,
                                      integration_name=STORAGE_INTEGRATION_NAME)

        # --- Step 4 & 5: Copy from YOUR Snowflake Staging DB to GCS, then GCS to BigQuery ---
        print(f"\n--- Stage 3: Migrating tables from Staging Snowflake DB to GCS & BigQuery ---")
        tables_in_staging_db = get_all_tables(sf_conn, snowflake_staging_db_name, snowflake_staging_schema_name)

        if not tables_in_staging_db:
            print(
                f"⚠️ No tables found in Snowflake staging {snowflake_staging_db_name}.{snowflake_staging_schema_name} to process for GCS/BQ. This is unexpected if previous step succeeded.")
        else:
            print(
                f"Found {len(tables_in_staging_db)} tables in staging {snowflake_staging_db_name}.{snowflake_staging_schema_name} to migrate to GCS & BigQuery.")
            total_staging_tables = len(tables_in_staging_db)
            for idx, table_name in enumerate(tables_in_staging_db, start=1):
                print(f"\nProcessing table for GCS/BQ [{idx}/{total_staging_tables}]: {table_name}")
                try:
                    copy_sf_staging_db_to_gcs(table_name, sf_conn,
                                              snowflake_staging_db_name,
                                              snowflake_staging_schema_name,
                                              GCS_STAGE_OBJECT_NAME)

                    load_gcs_to_bigquery(table_name, bq_client, PROJECT_ID, GCS_BUCKET,
                                         bq_dataset_name=bq_final_dataset)

                except Exception as e:
                    print(f"❌ Error processing table {table_name} during GCS/BQ transfer: {e}")
                    print(
                        "➡️ Stopping script. Rerun to resume (some tables might be partially processed or require cleanup).")
                    break
            else:
                print(
                    "\n🎉 Migration from Snowflake staging DB to GCS and BigQuery completed successfully for all tables!")

    except snowflake.connector.errors.ProgrammingError as spe:
        print(f"❌ Snowflake Programming Error: {spe}")
        if "does not exist or not authorized" in str(spe):
            print("ℹ️ This often indicates a permissions issue or a misnamed database/schema. Please check:")
            print(f"  - Source DB/Schema: {snowflake_source_db_name}.{snowflake_source_schema}")
            print(f"  - Staging DB/Schema: {snowflake_staging_db_name}.{snowflake_staging_schema_name}")
            print(f"  - User '{sf_creds.get('SNOWFLAKE_USER')}' permissions.")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
    finally:
        if sf_conn:
            sf_conn.close()
            print("🚪 Closed Snowflake connection.")
