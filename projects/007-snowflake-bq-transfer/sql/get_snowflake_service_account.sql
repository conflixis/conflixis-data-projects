-- Run this SQL in Snowflake to get the GCS service account
-- that Snowflake uses to access your GCS bucket

-- 1. First, check the storage integration details
DESC INTEGRATION GCS_INT;

-- Look for the row where property = 'STORAGE_GCP_SERVICE_ACCOUNT'
-- The property_value column will contain the service account email
-- It will look something like: xxxxxx@gcpuscentral1-1234.iam.gserviceaccount.com

-- 2. Once you have the service account, grant it access to your GCS bucket
-- Run this command in your terminal (replace SERVICE_ACCOUNT with the actual value):
-- gsutil iam ch serviceAccount:SERVICE_ACCOUNT:objectAdmin gs://snowflake_dh_bq

-- 3. Alternative: You can also check all storage integrations
SHOW INTEGRATIONS;

-- 4. If you want to see the allowed locations for the integration
-- Look for STORAGE_ALLOWED_LOCATIONS in the DESC output