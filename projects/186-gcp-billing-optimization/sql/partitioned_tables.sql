-- =========================================
-- PHASE 2: PARTITIONING AND CLUSTERING
-- =========================================
-- Purpose: Create partitioned and clustered tables for optimal performance
-- Expected Impact: 90% reduction in data scanned
-- =========================================

-- Note: These create new optimized tables in conflixis_data_projects
-- Data is copied from harmonized views created in Phase 1

-- 1. Optimize PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT
-- Assumption: Has a last_updated or similar date field
CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized`
PARTITION BY DATE(CAST(last_updated AS DATE))  -- Adjust field name as needed
CLUSTER BY NPI, SPECIALTY_PRIMARY
AS 
SELECT 
    CAST(NPI AS INT64) as NPI,
    * EXCEPT(NPI)
FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT`;

-- 2. Optimize rx_op_enhanced_full
-- This table likely has payment_year field
CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.rx_op_enhanced_full_optimized`
PARTITION BY payment_year  -- Partition by year
CLUSTER BY NPI, manufacturer_name
AS 
SELECT 
    CAST(NPI AS INT64) as NPI,
    * EXCEPT(NPI)
FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`;

-- 3. Optimize PHYSICIANS_OVERVIEW
-- May not have date field, but cluster on key columns
CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.PHYSICIANS_OVERVIEW_optimized`
CLUSTER BY NPI, SPECIALTY_PRIMARY, STATE
AS 
SELECT 
    CAST(NPI AS INT64) as NPI,
    * EXCEPT(NPI)
FROM `data-analytics-389803.conflixis_agent.PHYSICIANS_OVERVIEW`;

-- 4. Add clustering to already partitioned PHYSICIAN_RX_2020_2024
-- First check if it's already partitioned, then add clustering
CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.PHYSICIAN_RX_2020_2024_optimized`
PARTITION BY EXTRACT(YEAR FROM prescription_date)  -- Assuming prescription_date exists
CLUSTER BY NPI, drug_name
AS 
SELECT 
    NPI,  -- Already INT64
    *
FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024`;

-- 5. Add clustering to op_general_all_aggregate_static
-- Assuming it has year field based on name
CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized`
PARTITION BY year  -- Assuming year field exists
CLUSTER BY physician_id, manufacturer_name
AS 
SELECT 
    CAST(physician_id AS INT64) as physician_id,
    * EXCEPT(physician_id)
FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static`;

-- =========================================
-- VALIDATION QUERIES
-- =========================================

-- Check table partitioning and clustering
/*
SELECT 
    table_name,
    partition_expiration_ms,
    clustering_fields
FROM `data-analytics-389803.conflixis_data_projects.INFORMATION_SCHEMA.TABLES`
WHERE table_name LIKE '%_optimized';
*/

-- Test query with partition pruning
/*
SELECT 
    COUNT(*) as record_count
FROM `data-analytics-389803.conflixis_data_projects.rx_op_enhanced_full_optimized`
WHERE payment_year = 2024;  -- Will only scan 2024 partition
-- Expected: ~1.29 TB instead of 12.94 TB (90% reduction)
*/

-- Test join with clustering
/*
SELECT 
    rx.NPI,
    p.SPECIALTY_PRIMARY,
    COUNT(*) as payment_count
FROM `data-analytics-389803.conflixis_data_projects.rx_op_enhanced_full_optimized` rx
JOIN `data-analytics-389803.conflixis_data_projects.PHYSICIANS_OVERVIEW_optimized` p
ON rx.NPI = p.NPI  -- Will use clustered index
WHERE rx.payment_year = 2024
GROUP BY rx.NPI, p.SPECIALTY_PRIMARY
LIMIT 100;
-- Expected: <0.65 TB scanned, <$4 cost per query
*/