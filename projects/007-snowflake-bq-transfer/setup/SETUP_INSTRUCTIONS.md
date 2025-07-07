# Snowflake to BigQuery Transfer - Setup Instructions

## Current Status
The transfer script is working but failing at the GCS export step due to permissions issues.

## Issues Identified

1. **GCS Bucket Access**: The bucket `snowflake_dh_bq` either:
   - Doesn't exist, OR
   - Exists in a different project, OR  
   - The service account doesn't have access

2. **Service Account Permissions**: The service account `vw-cursor@data-analytics-389803.iam.gserviceaccount.com` needs:
   - Storage Object Admin role on the GCS bucket
   - Storage Bucket Creator role (if creating new buckets)

## Setup Steps

### Option 1: Use an Existing Bucket
If you have an existing GCS bucket you want to use:

1. Update the `.env` file:
   ```
   SNOWFLAKE_GCS_BUCKET=your-existing-bucket-name
   ```

2. Grant permissions to the service account:
   ```bash
   gsutil iam ch serviceAccount:vw-cursor@data-analytics-389803.iam.gserviceaccount.com:objectAdmin gs://your-existing-bucket-name
   ```

### Option 2: Create a New Bucket
1. Create a new bucket (requires appropriate permissions):
   ```bash
   gsutil mb -p data-analytics-389803 gs://conflixis-snowflake-transfer-389803
   ```

2. Update the `.env` file:
   ```
   SNOWFLAKE_GCS_BUCKET=conflixis-snowflake-transfer-389803
   ```

3. Grant permissions to the service account:
   ```bash
   gsutil iam ch serviceAccount:vw-cursor@data-analytics-389803.iam.gserviceaccount.com:objectAdmin gs://conflixis-snowflake-transfer-389803
   ```

### Grant Snowflake Access to GCS

1. Get the Snowflake service account by running this SQL in Snowflake:
   ```sql
   DESC INTEGRATION GCS_INT;
   ```
   Look for the `STORAGE_GCP_SERVICE_ACCOUNT` value.

2. Grant the Snowflake service account access to your bucket:
   ```bash
   gsutil iam ch serviceAccount:SNOWFLAKE_SERVICE_ACCOUNT:objectAdmin gs://your-bucket-name
   ```

## Verify Setup

After completing the setup:

1. Run the dry-run to verify connections:
   ```bash
   poetry run python projects/007-snowflake-bq-transfer/dh_snowflake_bigquery_singlefile.py --dry-run
   ```

2. Transfer a single table:
   ```bash
   poetry run python projects/007-snowflake-bq-transfer/dh_snowflake_bigquery_singlefile.py --table PHYSICIAN_GROUPS_PRACTICE_LOCATIONS
   ```

## Troubleshooting

- If you get "access denied" errors, double-check that both service accounts have the correct permissions
- If the bucket is in a different project, ensure cross-project access is configured
- Check that the Snowflake storage integration is using the correct bucket URL