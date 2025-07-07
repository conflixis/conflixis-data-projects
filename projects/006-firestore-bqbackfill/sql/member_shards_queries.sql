-- Comprehensive SQL queries for analyzing member_shards data in BigQuery
-- Table: conflixis-engine.firestore_export.member_shards_raw_changelog

-- =============================================================================
-- QUERY 1: Get all members for a specific group with full details
-- =============================================================================
WITH member_data AS (
  SELECT 
    REGEXP_EXTRACT(document_name, r'groups/([^/]+)/member_shards') AS group_id,
    document_id AS shard_id,
    timestamp,
    JSON_EXTRACT_ARRAY(data, '$.members') AS members_array
  FROM 
    `conflixis-engine.firestore_export.member_shards_raw_changelog`
  WHERE 
    document_name LIKE 'groups/gcO9AHYlNSzFeGTRSFRa/%'
)
SELECT 
  group_id,
  JSON_EXTRACT_SCALAR(member, '$.id') AS member_id,
  JSON_EXTRACT_SCALAR(member, '$.npi') AS npi,
  JSON_EXTRACT_SCALAR(member, '$.first_name') AS first_name,
  JSON_EXTRACT_SCALAR(member, '$.last_name') AS last_name,
  JSON_EXTRACT_SCALAR(member, '$.email') AS email,
  JSON_EXTRACT_SCALAR(member, '$.entity') AS entity,
  JSON_EXTRACT_SCALAR(member, '$.job_title') AS job_title,
  JSON_EXTRACT_SCALAR(member, '$.manager_name') AS manager_name,
  CAST(JSON_EXTRACT_SCALAR(member, '$.archived') AS BOOL) AS archived,
  shard_id,
  timestamp AS last_updated
FROM 
  member_data,
  UNNEST(members_array) AS member
ORDER BY 
  last_name, first_name;

-- =============================================================================
-- QUERY 2: Group summary statistics
-- =============================================================================
WITH group_stats AS (
  SELECT 
    REGEXP_EXTRACT(document_name, r'groups/([^/]+)/member_shards') AS group_id,
    document_id,
    ARRAY_LENGTH(JSON_EXTRACT_ARRAY(data, '$.members')) AS members_in_shard,
    timestamp
  FROM 
    `conflixis-engine.firestore_export.member_shards_raw_changelog`
)
SELECT 
  group_id,
  COUNT(DISTINCT document_id) AS total_shards,
  SUM(members_in_shard) AS total_members,
  AVG(members_in_shard) AS avg_members_per_shard,
  MIN(members_in_shard) AS min_members_per_shard,
  MAX(members_in_shard) AS max_members_per_shard,
  MAX(timestamp) AS last_updated
FROM 
  group_stats
WHERE 
  group_id = 'gcO9AHYlNSzFeGTRSFRa'
GROUP BY 
  group_id;

-- =============================================================================
-- QUERY 3: Find members by job title
-- =============================================================================
WITH member_data AS (
  SELECT 
    REGEXP_EXTRACT(document_name, r'groups/([^/]+)/member_shards') AS group_id,
    JSON_EXTRACT_ARRAY(data, '$.members') AS members_array
  FROM 
    `conflixis-engine.firestore_export.member_shards_raw_changelog`
  WHERE 
    document_name LIKE 'groups/gcO9AHYlNSzFeGTRSFRa/%'
)
SELECT 
  JSON_EXTRACT_SCALAR(member, '$.job_title') AS job_title,
  COUNT(*) AS count
FROM 
  member_data,
  UNNEST(members_array) AS member
GROUP BY 
  job_title
ORDER BY 
  count DESC;

-- =============================================================================
-- QUERY 4: Find members by manager
-- =============================================================================
WITH member_data AS (
  SELECT 
    JSON_EXTRACT_ARRAY(data, '$.members') AS members_array
  FROM 
    `conflixis-engine.firestore_export.member_shards_raw_changelog`
  WHERE 
    document_name LIKE 'groups/gcO9AHYlNSzFeGTRSFRa/%'
)
SELECT 
  JSON_EXTRACT_SCALAR(member, '$.manager_name') AS manager_name,
  COUNT(*) AS team_size,
  STRING_AGG(
    CONCAT(
      JSON_EXTRACT_SCALAR(member, '$.first_name'), 
      ' ', 
      JSON_EXTRACT_SCALAR(member, '$.last_name')
    ), 
    ', ' 
    ORDER BY JSON_EXTRACT_SCALAR(member, '$.last_name')
  ) AS team_members
FROM 
  member_data,
  UNNEST(members_array) AS member
WHERE 
  JSON_EXTRACT_SCALAR(member, '$.archived') = 'false'
GROUP BY 
  manager_name
HAVING 
  COUNT(*) > 5  -- Show managers with more than 5 team members
ORDER BY 
  team_size DESC;

-- =============================================================================
-- QUERY 5: Search for specific members by name or NPI
-- =============================================================================
WITH member_data AS (
  SELECT 
    REGEXP_EXTRACT(document_name, r'groups/([^/]+)/member_shards') AS group_id,
    document_id,
    JSON_EXTRACT_ARRAY(data, '$.members') AS members_array
  FROM 
    `conflixis-engine.firestore_export.member_shards_raw_changelog`
)
SELECT 
  group_id,
  JSON_EXTRACT_SCALAR(member, '$.id') AS member_id,
  JSON_EXTRACT_SCALAR(member, '$.first_name') AS first_name,
  JSON_EXTRACT_SCALAR(member, '$.last_name') AS last_name,
  JSON_EXTRACT_SCALAR(member, '$.email') AS email,
  JSON_EXTRACT_SCALAR(member, '$.npi') AS npi,
  JSON_EXTRACT_SCALAR(member, '$.entity') AS entity,
  JSON_EXTRACT_SCALAR(member, '$.job_title') AS job_title
FROM 
  member_data,
  UNNEST(members_array) AS member
WHERE 
  -- Search by name (case-insensitive)
  (LOWER(JSON_EXTRACT_SCALAR(member, '$.first_name')) LIKE '%smith%'
   OR LOWER(JSON_EXTRACT_SCALAR(member, '$.last_name')) LIKE '%smith%')
  -- OR search by NPI
  -- JSON_EXTRACT_SCALAR(member, '$.npi') = '1234567890'
  -- OR search by email
  -- LOWER(JSON_EXTRACT_SCALAR(member, '$.email')) LIKE '%@texashealth.org%'
LIMIT 100;

-- =============================================================================
-- QUERY 6: Get all unique entities across all groups
-- =============================================================================
WITH member_data AS (
  SELECT 
    JSON_EXTRACT_ARRAY(data, '$.members') AS members_array
  FROM 
    `conflixis-engine.firestore_export.member_shards_raw_changelog`
)
SELECT 
  JSON_EXTRACT_SCALAR(member, '$.entity') AS entity,
  COUNT(DISTINCT JSON_EXTRACT_SCALAR(member, '$.id')) AS unique_members,
  COUNT(DISTINCT JSON_EXTRACT_SCALAR(member, '$.npi')) AS unique_npis
FROM 
  member_data,
  UNNEST(members_array) AS member
WHERE 
  JSON_EXTRACT_SCALAR(member, '$.entity') IS NOT NULL
GROUP BY 
  entity
ORDER BY 
  unique_members DESC;

-- =============================================================================
-- QUERY 7: Create a materialized view for better performance (optional)
-- =============================================================================
/*
CREATE OR REPLACE VIEW `conflixis-engine.firestore_export.member_shards_parsed` AS
WITH parsed_data AS (
  SELECT 
    REGEXP_EXTRACT(document_name, r'groups/([^/]+)/member_shards') AS group_id,
    document_id AS shard_id,
    timestamp AS last_updated,
    member,
    ROW_NUMBER() OVER (PARTITION BY JSON_EXTRACT_SCALAR(member, '$.id') ORDER BY timestamp DESC) AS rn
  FROM 
    `conflixis-engine.firestore_export.member_shards_raw_changelog`,
    UNNEST(JSON_EXTRACT_ARRAY(data, '$.members')) AS member
)
SELECT 
  group_id,
  shard_id,
  JSON_EXTRACT_SCALAR(member, '$.id') AS member_id,
  JSON_EXTRACT_SCALAR(member, '$.npi') AS npi,
  JSON_EXTRACT_SCALAR(member, '$.first_name') AS first_name,
  JSON_EXTRACT_SCALAR(member, '$.last_name') AS last_name,
  JSON_EXTRACT_SCALAR(member, '$.email') AS email,
  JSON_EXTRACT_SCALAR(member, '$.entity') AS entity,
  JSON_EXTRACT_SCALAR(member, '$.job_title') AS job_title,
  JSON_EXTRACT_SCALAR(member, '$.manager_name') AS manager_name,
  CAST(JSON_EXTRACT_SCALAR(member, '$.archived') AS BOOL) AS archived,
  last_updated
FROM 
  parsed_data
WHERE 
  rn = 1;  -- Get only the latest version of each member
*/