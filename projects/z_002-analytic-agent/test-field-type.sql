-- Test the actual data type of physician_ownership_indicator
SELECT 
  data_type,
  column_name
FROM `data-analytics-389803.conflixis_datasources.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'op_general_all'
  AND column_name = 'physician_ownership_indicator';