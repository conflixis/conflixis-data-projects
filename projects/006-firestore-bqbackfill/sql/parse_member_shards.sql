-- SQL script to parse member_shards data from Firestore raw changelog table
-- This script extracts member information from the JSON data field

-- First, let's see the data structure for the specific group
WITH raw_data AS (
  SELECT 
    timestamp,
    event_id,
    document_name,
    document_id,
    data,
    -- Extract group ID from document path
    REGEXP_EXTRACT(document_name, r'groups/([^/]+)/member_shards') AS group_id
  FROM 
    `conflixis-engine.firestore_export.member_shards_raw_changelog`
  WHERE 
    -- Filter for the specific group
    document_name LIKE 'groups/gcO9AHYlNSzFeGTRSFRa/%'
),

-- Parse the JSON data to extract member arrays
parsed_members AS (
  SELECT 
    timestamp,
    document_id,
    group_id,
    -- Extract members array
    JSON_EXTRACT_ARRAY(data, '$.members') AS members_array,
    -- Extract email array if exists
    JSON_EXTRACT_ARRAY(data, '$.email') AS email_array,
    -- Extract npi array if exists  
    JSON_EXTRACT_ARRAY(data, '$.npi') AS npi_array
  FROM 
    raw_data
),

-- Unnest the members array to get individual member records
member_details AS (
  SELECT
    timestamp,
    document_id,
    group_id,
    -- Parse individual member fields
    JSON_EXTRACT_SCALAR(member, '$.id') AS member_id,
    JSON_EXTRACT_SCALAR(member, '$.npi') AS npi,
    JSON_EXTRACT_SCALAR(member, '$.first_name') AS first_name,
    JSON_EXTRACT_SCALAR(member, '$.last_name') AS last_name,
    JSON_EXTRACT_SCALAR(member, '$.email') AS email,
    JSON_EXTRACT_SCALAR(member, '$.entity') AS entity,
    JSON_EXTRACT_SCALAR(member, '$.job_title') AS job_title,
    JSON_EXTRACT_SCALAR(member, '$.manager_name') AS manager_name,
    CAST(JSON_EXTRACT_SCALAR(member, '$.archived') AS BOOL) AS archived
  FROM 
    parsed_members,
    UNNEST(members_array) AS member
)

-- Final query with all member details
SELECT 
  group_id,
  member_id,
  npi,
  first_name,
  last_name,
  email,
  entity,
  job_title,
  manager_name,
  archived,
  document_id AS shard_id,
  timestamp AS last_updated
FROM 
  member_details
ORDER BY 
  last_name, 
  first_name;

-- Alternative query to get summary statistics for the group
/*
WITH group_data AS (
  SELECT 
    REGEXP_EXTRACT(document_name, r'groups/([^/]+)/member_shards') AS group_id,
    JSON_EXTRACT_ARRAY(data, '$.members') AS members_array
  FROM 
    `conflixis-engine.firestore_export.member_shards_raw_changelog`
  WHERE 
    document_name LIKE 'groups/gcO9AHYlNSzFeGTRSFRa/%'
)
SELECT 
  group_id,
  COUNT(DISTINCT document_name) AS shard_count,
  SUM(ARRAY_LENGTH(members_array)) AS total_members
FROM 
  group_data
GROUP BY 
  group_id;
*/

-- Query to find specific members by name
/*
WITH member_search AS (
  SELECT 
    REGEXP_EXTRACT(document_name, r'groups/([^/]+)/member_shards') AS group_id,
    document_id,
    JSON_EXTRACT_ARRAY(data, '$.members') AS members_array
  FROM 
    `conflixis-engine.firestore_export.member_shards_raw_changelog`
  WHERE 
    document_name LIKE 'groups/gcO9AHYlNSzFeGTRSFRa/%'
)
SELECT 
  group_id,
  JSON_EXTRACT_SCALAR(member, '$.id') AS member_id,
  JSON_EXTRACT_SCALAR(member, '$.first_name') AS first_name,
  JSON_EXTRACT_SCALAR(member, '$.last_name') AS last_name,
  JSON_EXTRACT_SCALAR(member, '$.email') AS email,
  JSON_EXTRACT_SCALAR(member, '$.npi') AS npi
FROM 
  member_search,
  UNNEST(members_array) AS member
WHERE 
  LOWER(JSON_EXTRACT_SCALAR(member, '$.last_name')) LIKE '%taylor%'
  OR LOWER(JSON_EXTRACT_SCALAR(member, '$.first_name')) LIKE '%taylor%';
*/