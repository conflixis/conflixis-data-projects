# Snowflake to BigQuery Transfer

This project transfers tables from Snowflake to BigQuery via Google Cloud Storage (GCS).

## Project Structure

```
007-snowflake-bq-transfer/
├── dh_snowflake_bigquery_singlefile.py      # Main transfer script
├── setup/                   # Setup utilities
│   └── setup_gcs_bucket.py                  # Configure GCS bucket
├── utils/                   # Utility scripts
│   ├── checks/             # Validation and monitoring
│   │   ├── check_bq_table.py               # Verify BigQuery tables
│   │   ├── check_gcs_access.py             # Test GCS permissions
│   │   ├── check_gcs_files.py              # List GCS bucket contents
│   │   ├── check_parquet_file.py           # Validate Parquet files
│   │   ├── check_source_tables.py          # Examine Snowflake tables
│   │   ├── check_staging_table.py          # Check staging status
│   │   ├── check_storage_integration.py    # Verify Snowflake integration
│   │   ├── check_table_size.py             # Check table sizes
│   │   └── get_snowflake_schema.py         # Extract table schemas
│   ├── debug/              # Debugging tools
│   │   └── debug_gcs_stage.py              # Debug GCS stage issues
│   └── cleanup/            # Cleanup utilities
│       ├── delete_bq_table.py              # Remove BigQuery tables
│       └── delete_gcs_files.py             # Clean up GCS files
├── .gitignore              # Git ignore file
└── README.md               # This file

```

## Setup

1. **Install dependencies** (from repository root):
   ```bash
   poetry install
   ```

2. **Configure environment variables** in `/common/.env`:
   ```env
   # Snowflake Configuration
   SNOWFLAKE_USER=your_user
   SNOWFLAKE_ACCOUNT=your_account.region.cloud
   SNOWFLAKE_WAREHOUSE=COMPUTE_WH
   SNOWFLAKE_DATABASE=source_database
   SNOWFLAKE_SCHEMA=source_schema
   SNOWFLAKE_PAT=your_personal_access_token

   # Snowflake Staging Configuration
   SNOWFLAKE_STAGING_DATABASE=CONFLIXIS_STAGE
   SNOWFLAKE_STAGING_SCHEMA=PUBLIC
   SNOWFLAKE_GCS_BUCKET=snowflake_dh_bq
   SNOWFLAKE_BQ_TARGET_DATASET=target_dataset
   SNOWFLAKE_GCS_STAGE_NAME=snowflake_dh_bq
   SNOWFLAKE_STORAGE_INTEGRATION=GCS_INT

   # Google Cloud Configuration
   GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
   BQ_PROJECT_ID=your-gcp-project-id
   ```

3. **Set up GCS bucket and storage integration**:
   ```bash
   python setup/setup_gcs_bucket.py
   ```

## Usage

### Primary Transfer Script

Transfer a single table:
```bash
python dh_snowflake_bigquery_singlefile.py --table TABLE_NAME
```

Transfer all tables in the schema:
```bash
python dh_snowflake_bigquery_singlefile.py
```

Validate configuration (dry run):
```bash
python dh_snowflake_bigquery_singlefile.py --dry-run
```

### Large Table Transfers

For very large tables, run in the background:
```bash
nohup python dh_snowflake_bigquery_singlefile.py --table LARGE_TABLE_NAME > transfer.log 2>&1 &
```

## Validation and Monitoring

Before transfer:
```bash
# Check source tables
python utils/checks/check_source_tables.py

# Verify storage integration
python utils/checks/check_storage_integration.py

# Check GCS access
python utils/checks/check_gcs_access.py
```

After transfer:
```bash
# Verify BigQuery table
python utils/checks/check_bq_table.py

# Check GCS files
python utils/checks/check_gcs_files.py

# Validate Parquet files
python utils/checks/check_parquet_file.py
```

## How it works

1. **Connect to Snowflake** using the configured credentials
2. **Copy table(s)** from source database/schema to staging database/schema
3. **Export to GCS** via Snowflake external stage
4. **Load into BigQuery** from GCS using Parquet format

## Features

- ✅ Loads configuration from environment variables
- ✅ Supports single table or batch processing
- ✅ Proper logging with timestamps
- ✅ Error handling and validation
- ✅ Dry-run mode for testing
- ✅ Cross-platform (Windows/Linux/Mac)

## Troubleshooting

- **Permission errors**: Ensure the Snowflake user has appropriate privileges
- **GCS access**: Verify the storage integration is properly configured
- **BigQuery access**: Check that the service account has necessary permissions