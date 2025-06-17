-- BigQuery Query Example - SQL Script
-- This script demonstrates querying the PHYSICIANS_OVERVIEW table using pure SQL
-- 
-- To run this script:
-- 1. Use the bq command-line tool: bq query --use_legacy_sql=false < 03_bigquery_query.sql
-- 2. Or copy/paste into BigQuery Console: https://console.cloud.google.com/bigquery
-- 3. Or use from Python/R by reading this file and executing the query

-- Basic query to retrieve 100 rows from PHYSICIANS_OVERVIEW
SELECT * 
FROM `data-analytics-389803.CONFLIXIS_309340.PHYSICIANS_OVERVIEW` 
LIMIT 100;

-- Example queries (uncomment to use):

-- 1. Count total number of records
-- SELECT COUNT(*) as total_records
-- FROM `data-analytics-389803.CONFLIXIS_309340.PHYSICIANS_OVERVIEW`;

-- 2. Get distinct values from a specific column (update column_name)
-- SELECT DISTINCT column_name, COUNT(*) as count
-- FROM `data-analytics-389803.CONFLIXIS_309340.PHYSICIANS_OVERVIEW`
-- GROUP BY column_name
-- ORDER BY count DESC;

-- 3. Filter records with WHERE clause (update based on your columns)
-- SELECT *
-- FROM `data-analytics-389803.CONFLIXIS_309340.PHYSICIANS_OVERVIEW`
-- WHERE some_column = 'some_value'
-- LIMIT 100;

-- 4. Aggregate data with GROUP BY (update based on your columns)
-- SELECT 
--     grouping_column,
--     COUNT(*) as record_count,
--     AVG(numeric_column) as avg_value
-- FROM `data-analytics-389803.CONFLIXIS_309340.PHYSICIANS_OVERVIEW`
-- GROUP BY grouping_column
-- ORDER BY record_count DESC;

-- 5. Get schema information
-- SELECT 
--     column_name,
--     data_type,
--     is_nullable
-- FROM `data-analytics-389803.CONFLIXIS_309340.INFORMATION_SCHEMA.COLUMNS`
-- WHERE table_name = 'PHYSICIANS_OVERVIEW';

-- Note: When using with bq command-line tool, ensure you have authenticated:
-- gcloud auth application-default login
-- Or set GOOGLE_APPLICATION_CREDENTIALS environment variable