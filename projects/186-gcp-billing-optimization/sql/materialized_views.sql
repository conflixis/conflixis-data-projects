-- =========================================
-- PHASE 3: MATERIALIZED VIEWS
-- =========================================
-- Purpose: Pre-compute expensive aggregations for instant results
-- Expected Impact: 95% cost reduction for aggregate queries
-- =========================================

-- 1. Open Payments Summary by NPI and Year
-- This pre-computes common aggregations for payment analysis
CREATE MATERIALIZED VIEW `data-analytics-389803.conflixis_data_projects.mv_op_summary_by_npi_year`
PARTITION BY payment_year
CLUSTER BY NPI
AS 
SELECT 
    NPI,
    payment_year,
    SUM(total_amount) as total_payments,
    COUNT(DISTINCT manufacturer_name) as unique_manufacturers,
    COUNT(*) as payment_count,
    AVG(total_amount) as avg_payment,
    MAX(total_amount) as max_payment,
    MIN(total_amount) as min_payment,
    -- Add percentile calculations
    APPROX_QUANTILES(total_amount, 100)[OFFSET(50)] as median_payment,
    APPROX_QUANTILES(total_amount, 100)[OFFSET(75)] as p75_payment,
    APPROX_QUANTILES(total_amount, 100)[OFFSET(95)] as p95_payment
FROM `data-analytics-389803.conflixis_data_projects.rx_op_enhanced_full_optimized`
GROUP BY NPI, payment_year;

-- 2. Prescription Patterns by NPI
-- Pre-compute prescription metrics for each provider
CREATE MATERIALIZED VIEW `data-analytics-389803.conflixis_data_projects.mv_rx_patterns_by_npi`
PARTITION BY rx_year
CLUSTER BY NPI, drug_name
AS
SELECT 
    NPI,
    drug_name,
    EXTRACT(YEAR FROM prescription_date) as rx_year,
    SUM(total_claim_count) as total_claims,
    SUM(total_drug_cost) as total_cost,
    AVG(total_drug_cost / NULLIF(total_claim_count, 0)) as avg_cost_per_claim,
    COUNT(DISTINCT DATE_TRUNC(prescription_date, MONTH)) as months_prescribed
FROM `data-analytics-389803.conflixis_data_projects.PHYSICIAN_RX_2020_2024_optimized`
GROUP BY NPI, drug_name, rx_year;

-- 3. Provider Profile Summary
-- Comprehensive provider overview combining multiple sources
CREATE MATERIALIZED VIEW `data-analytics-389803.conflixis_data_projects.mv_provider_profile_summary`
CLUSTER BY NPI
AS
WITH provider_base AS (
    SELECT DISTINCT
        NPI,
        SPECIALTY_PRIMARY,
        STATE,
        CITY
    FROM `data-analytics-389803.conflixis_data_projects.PHYSICIANS_OVERVIEW_optimized`
),
payment_summary AS (
    SELECT 
        NPI,
        SUM(total_payments) as lifetime_payments,
        COUNT(DISTINCT payment_year) as years_with_payments,
        COUNT(DISTINCT unique_manufacturers) as total_unique_manufacturers
    FROM `data-analytics-389803.conflixis_data_projects.mv_op_summary_by_npi_year`
    GROUP BY NPI
),
rx_summary AS (
    SELECT 
        NPI,
        COUNT(DISTINCT drug_name) as unique_drugs_prescribed,
        SUM(total_claims) as total_prescriptions,
        SUM(total_cost) as total_rx_cost
    FROM `data-analytics-389803.conflixis_data_projects.mv_rx_patterns_by_npi`
    GROUP BY NPI
)
SELECT 
    p.NPI,
    p.SPECIALTY_PRIMARY,
    p.STATE,
    p.CITY,
    COALESCE(pay.lifetime_payments, 0) as lifetime_payments,
    COALESCE(pay.years_with_payments, 0) as years_with_payments,
    COALESCE(pay.total_unique_manufacturers, 0) as total_unique_manufacturers,
    COALESCE(rx.unique_drugs_prescribed, 0) as unique_drugs_prescribed,
    COALESCE(rx.total_prescriptions, 0) as total_prescriptions,
    COALESCE(rx.total_rx_cost, 0) as total_rx_cost
FROM provider_base p
LEFT JOIN payment_summary pay ON p.NPI = pay.NPI
LEFT JOIN rx_summary rx ON p.NPI = rx.NPI;

-- 4. High-Value Provider Identification
-- Identify providers with significant payment relationships
CREATE MATERIALIZED VIEW `data-analytics-389803.conflixis_data_projects.mv_high_value_providers`
CLUSTER BY payment_year, NPI
AS
SELECT 
    payment_year,
    NPI,
    total_payments,
    unique_manufacturers,
    payment_count,
    -- Calculate percentile rank within year
    PERCENT_RANK() OVER (PARTITION BY payment_year ORDER BY total_payments) as payment_percentile,
    -- Flag top providers
    CASE 
        WHEN total_payments > 100000 THEN 'Very High'
        WHEN total_payments > 50000 THEN 'High'
        WHEN total_payments > 10000 THEN 'Medium'
        ELSE 'Low'
    END as payment_tier,
    -- Flag based on manufacturer diversity
    CASE 
        WHEN unique_manufacturers >= 10 THEN 'Highly Diverse'
        WHEN unique_manufacturers >= 5 THEN 'Moderate Diversity'
        ELSE 'Low Diversity'
    END as manufacturer_diversity
FROM `data-analytics-389803.conflixis_data_projects.mv_op_summary_by_npi_year`
WHERE total_payments > 1000;  -- Focus on meaningful payments

-- =========================================
-- REFRESH SCHEDULE (to be configured)
-- =========================================

-- Set automatic refresh for materialized views
-- Run these commands to set up daily refresh at 2 AM

/*
ALTER MATERIALIZED VIEW `data-analytics-389803.conflixis_data_projects.mv_op_summary_by_npi_year`
SET OPTIONS (
    enable_refresh = true,
    refresh_interval_minutes = 1440  -- Daily refresh
);

ALTER MATERIALIZED VIEW `data-analytics-389803.conflixis_data_projects.mv_rx_patterns_by_npi`
SET OPTIONS (
    enable_refresh = true,
    refresh_interval_minutes = 1440  -- Daily refresh
);

ALTER MATERIALIZED VIEW `data-analytics-389803.conflixis_data_projects.mv_provider_profile_summary`
SET OPTIONS (
    enable_refresh = true,
    refresh_interval_minutes = 1440  -- Daily refresh
);

ALTER MATERIALIZED VIEW `data-analytics-389803.conflixis_data_projects.mv_high_value_providers`
SET OPTIONS (
    enable_refresh = true,
    refresh_interval_minutes = 1440  -- Daily refresh
);
*/

-- =========================================
-- VALIDATION QUERIES
-- =========================================

-- Test materialized view performance
/*
-- Original query (expensive):
SELECT 
    NPI,
    SUM(total_amount) as total,
    COUNT(DISTINCT manufacturer_name) as manufacturers
FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
WHERE payment_year = 2024
GROUP BY NPI;
-- Cost: ~$73.55, Time: ~120 seconds

-- With materialized view (instant):
SELECT 
    NPI,
    total_payments as total,
    unique_manufacturers as manufacturers
FROM `data-analytics-389803.conflixis_data_projects.mv_op_summary_by_npi_year`
WHERE payment_year = 2024;
-- Cost: ~$0.50, Time: <1 second
*/