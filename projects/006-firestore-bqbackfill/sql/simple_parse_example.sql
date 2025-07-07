-- Simple example to parse member_shards data from BigQuery
-- This shows the basic pattern for extracting JSON data

-- Step 1: View raw data for the specific group
SELECT 
  document_name,
  document_id,
  timestamp,
  -- Show first 500 chars of data to see structure
  SUBSTR(data, 1, 500) AS data_preview
FROM 
  `conflixis-engine.firestore_export.member_shards_raw_changelog`
WHERE 
  document_name LIKE 'groups/gcO9AHYlNSzFeGTRSFRa/%'
LIMIT 5;

-- Step 2: Extract and count members in each shard
SELECT 
  document_id,
  ARRAY_LENGTH(JSON_EXTRACT_ARRAY(data, '$.members')) AS member_count,
  ARRAY_LENGTH(JSON_EXTRACT_ARRAY(data, '$.npi')) AS npi_count
FROM 
  `conflixis-engine.firestore_export.member_shards_raw_changelog`
WHERE 
  document_name LIKE 'groups/gcO9AHYlNSzFeGTRSFRa/%';

-- Step 3: Simple extraction of all members with basic info
SELECT 
  -- Extract member data
  JSON_EXTRACT_SCALAR(member, '$.id') AS member_id,
  JSON_EXTRACT_SCALAR(member, '$.first_name') AS first_name,
  JSON_EXTRACT_SCALAR(member, '$.last_name') AS last_name,
  JSON_EXTRACT_SCALAR(member, '$.npi') AS npi,
  JSON_EXTRACT_SCALAR(member, '$.email') AS email,
  JSON_EXTRACT_SCALAR(member, '$.job_title') AS job_title
FROM 
  `conflixis-engine.firestore_export.member_shards_raw_changelog`,
  -- This UNNEST operation converts the JSON array into rows
  UNNEST(JSON_EXTRACT_ARRAY(data, '$.members')) AS member
WHERE 
  document_name LIKE 'groups/gcO9AHYlNSzFeGTRSFRa/%'
ORDER BY 
  last_name, first_name
LIMIT 20;