-- RX-OP Enhanced: Analysis Queries for Stakeholder Insights
-- BigQuery Table: data-analytics-389803.conflixis_agent.rx_op_enhanced_full
-- JIRA: DA-167

-- ============================================================================
-- QUERY 1: Overall Attribution Summary (Matches Spreadsheet)
-- ============================================================================
-- Recreates the aggregate results from the original analysis
-- Shows overall ROI and attribution rates across all manufacturers

WITH summary AS (
  SELECT 
    source_manufacturer as manufacturer,
    source_specialty as specialty,
    COUNT(DISTINCT NPI) as unique_npis,
    COUNT(*) as observation_months,
    SUM(totalNext6) as total_prescribed,
    SUM(attributable_dollars) as total_attributable,
    SUM(TotalDollarsFrom) as total_op_dollars,
    SAFE_DIVIDE(SUM(attributable_dollars), SUM(totalNext6)) as attrib_pct,
    SAFE_DIVIDE(SUM(attributable_dollars), SUM(TotalDollarsFrom)) as roi
  FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
  GROUP BY manufacturer, specialty
)
SELECT 
  manufacturer,
  specialty,
  unique_npis,
  observation_months,
  ROUND(total_prescribed, 2) as total_prescribed_dollars,
  ROUND(total_attributable, 2) as attributable_dollars,
  ROUND(total_op_dollars, 2) as op_spent,
  ROUND(attrib_pct * 100, 2) as attribution_pct,
  ROUND(roi, 2) as roi_multiplier
FROM summary
ORDER BY total_attributable DESC
LIMIT 20;

-- ============================================================================
-- QUERY 2: High-Value Provider Segments (NPs and PAs Focus)
-- ============================================================================
-- Identifies the most valuable provider segments for marketing efforts
-- Highlights NPs and PAs as strategic targets

SELECT 
  source_specialty as specialty,
  COUNT(DISTINCT NPI) as total_providers,
  COUNT(DISTINCT CASE WHEN TotalDollarsFrom > 0 THEN NPI END) as paid_providers,
  ROUND(AVG(CASE WHEN TotalDollarsFrom > 0 THEN TotalDollarsFrom END), 2) as avg_payment_when_paid,
  ROUND(SUM(totalNext6) / 1e9, 2) as total_prescribed_billions,
  ROUND(SUM(attributable_dollars) / 1e6, 2) as attributable_millions,
  ROUND(SUM(TotalDollarsFrom) / 1e6, 2) as op_spent_millions,
  ROUND(SAFE_DIVIDE(SUM(attributable_dollars), SUM(TotalDollarsFrom)), 2) as roi,
  ROUND(SAFE_DIVIDE(SUM(attributable_dollars), SUM(totalNext6)) * 100, 3) as attribution_pct
FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
WHERE source_specialty IN ('NP', 'PA', 'Internal Medicine', 'Primary Care', 'Gastroenterology')
GROUP BY specialty
ORDER BY roi DESC;

-- ============================================================================
-- QUERY 3: Payment Tier Analysis
-- ============================================================================
-- Shows how different payment amounts affect attribution rates and ROI
-- Identifies optimal payment ranges for maximum effectiveness

WITH payment_tiers AS (
  SELECT 
    CASE 
      WHEN TotalDollarsFrom = 0 THEN '0. No Payment'
      WHEN TotalDollarsFrom <= 100 THEN '1. $1-100'
      WHEN TotalDollarsFrom <= 500 THEN '2. $101-500'
      WHEN TotalDollarsFrom <= 1000 THEN '3. $501-1000'
      WHEN TotalDollarsFrom <= 5000 THEN '4. $1001-5000'
      ELSE '5. $5000+'
    END as payment_tier,
    NPI,
    TotalDollarsFrom,
    totalNext6,
    attributable_dollars,
    attributable_pct
  FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
)
SELECT 
  payment_tier,
  COUNT(DISTINCT NPI) as providers,
  COUNT(*) as observations,
  ROUND(AVG(TotalDollarsFrom), 2) as avg_payment,
  ROUND(AVG(totalNext6), 2) as avg_prescribed,
  ROUND(AVG(attributable_dollars), 2) as avg_attributable,
  ROUND(AVG(attributable_pct) * 100, 2) as avg_attribution_pct,
  ROUND(SAFE_DIVIDE(SUM(attributable_dollars), SUM(TotalDollarsFrom)), 2) as roi
FROM payment_tiers
GROUP BY payment_tier
ORDER BY payment_tier;

-- ============================================================================
-- QUERY 4: Geographic Analysis by State
-- ============================================================================
-- Shows which states have the highest attribution impact
-- Identifies geographic opportunities for marketing optimization

SELECT 
  HQ_STATE as state,
  COUNT(DISTINCT NPI) as total_providers,
  COUNT(DISTINCT CASE WHEN TotalDollarsFrom > 0 THEN NPI END) as paid_providers,
  ROUND(SUM(totalNext6) / 1e9, 2) as total_prescribed_billions,
  ROUND(SUM(attributable_dollars) / 1e6, 2) as attributable_millions,
  ROUND(SUM(TotalDollarsFrom) / 1e6, 2) as op_spent_millions,
  ROUND(AVG(CASE WHEN TotalDollarsFrom > 0 THEN TotalDollarsFrom END), 2) as avg_payment_per_paid_provider,
  ROUND(SAFE_DIVIDE(SUM(attributable_dollars), SUM(TotalDollarsFrom)), 2) as roi,
  ROUND(SAFE_DIVIDE(SUM(attributable_dollars), SUM(totalNext6)) * 100, 3) as attribution_pct
FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
WHERE HQ_STATE IS NOT NULL
GROUP BY state
HAVING total_providers > 1000  -- Focus on states with significant provider base
ORDER BY attributable_millions DESC
LIMIT 20;

-- ============================================================================
-- QUERY 5: Top ROI Manufacturer-Specialty Combinations
-- ============================================================================
-- Identifies the most efficient marketing spend opportunities
-- Shows where $1 of payment generates the highest return

WITH mfg_spec_roi AS (
  SELECT 
    source_manufacturer as manufacturer,
    source_specialty as specialty,
    COUNT(DISTINCT NPI) as providers,
    SUM(TotalDollarsFrom) as total_op,
    SUM(attributable_dollars) as total_attributable,
    SAFE_DIVIDE(SUM(attributable_dollars), SUM(TotalDollarsFrom)) as roi
  FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
  WHERE TotalDollarsFrom > 0  -- Only include observations with payments
  GROUP BY manufacturer, specialty
  HAVING total_op > 100000  -- Minimum $100K spend for significance
)
SELECT 
  manufacturer,
  specialty,
  providers,
  ROUND(total_op / 1e6, 2) as op_spent_millions,
  ROUND(total_attributable / 1e6, 2) as attributable_millions,
  ROUND(roi, 2) as roi_multiplier,
  CONCAT('$', CAST(ROUND(roi, 2) AS STRING)) as roi_display
FROM mfg_spec_roi
WHERE roi > 5  -- Focus on high-performing combinations
ORDER BY roi DESC
LIMIT 20;

-- ============================================================================
-- QUERY 6: Counterfactual Impact Analysis
-- ============================================================================
-- Shows the true intervention effect of payments
-- Compares predicted prescriptions WITH vs WITHOUT payments

SELECT 
  source_manufacturer as manufacturer,
  COUNT(*) as observations,
  COUNT(DISTINCT NPI) as providers,
  
  -- Baseline (counterfactual - what would happen WITHOUT payments)
  ROUND(AVG(pred_rx_cf), 2) as avg_predicted_without_payment,
  
  -- With intervention
  ROUND(AVG(pred_rx), 2) as avg_predicted_with_payment,
  
  -- Lift from payment
  ROUND(AVG(delta_rx), 2) as avg_prescription_lift,
  
  -- Percentage increase
  ROUND(SAFE_DIVIDE(AVG(delta_rx), AVG(pred_rx_cf)) * 100, 2) as avg_pct_increase,
  
  -- ROI
  ROUND(SAFE_DIVIDE(SUM(attributable_dollars), SUM(TotalDollarsFrom)), 2) as overall_roi
  
FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
WHERE TotalDollarsFrom > 0  -- Only paid providers
GROUP BY manufacturer
ORDER BY overall_roi DESC;

-- ============================================================================
-- QUERY 7: High-Impact Payment Interventions
-- ============================================================================
-- Identifies cases where payments had exceptional impact
-- Useful for understanding what makes a payment highly effective

WITH high_impact AS (
  SELECT 
    NPI,
    source_manufacturer,
    source_specialty,
    year,
    month,
    TotalDollarsFrom as payment_amount,
    pred_rx as predicted_with_payment,
    pred_rx_cf as predicted_without_payment,
    delta_rx as prescription_lift,
    SAFE_DIVIDE(delta_rx, pred_rx_cf) as pct_increase,
    attributable_dollars
  FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
  WHERE TotalDollarsFrom > 0
    AND delta_rx > 1000  -- Significant prescription lift
)
SELECT 
  source_manufacturer as manufacturer,
  source_specialty as specialty,
  COUNT(*) as high_impact_cases,
  ROUND(AVG(payment_amount), 2) as avg_payment,
  ROUND(AVG(predicted_with_payment), 0) as avg_pred_with_payment,
  ROUND(AVG(predicted_without_payment), 0) as avg_pred_without_payment,
  ROUND(AVG(prescription_lift), 0) as avg_prescription_lift,
  ROUND(AVG(pct_increase) * 100, 1) as avg_pct_increase,
  ROUND(AVG(attributable_dollars), 0) as avg_attributable_dollars
FROM high_impact
GROUP BY manufacturer, specialty
HAVING high_impact_cases > 10  -- Minimum sample size
ORDER BY avg_pct_increase DESC
LIMIT 20;

-- ============================================================================
-- QUERY 8: Organic High Prescribers (No Payment Needed)
-- ============================================================================
-- Identifies providers who prescribe heavily without receiving payments
-- These represent strong brand loyalty or clinical preference

SELECT 
  source_manufacturer as manufacturer,
  source_specialty as specialty,
  COUNT(DISTINCT NPI) as organic_high_prescribers,
  ROUND(AVG(totalNext6), 0) as avg_prescriptions,
  ROUND(MIN(totalNext6), 0) as min_prescriptions,
  ROUND(MAX(totalNext6), 0) as max_prescriptions,
  ROUND(SUM(totalNext6) / 1e6, 2) as total_prescribed_millions
FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
WHERE TotalDollarsFrom = 0  -- No payments received
  AND totalNext6 > 10000  -- High prescribers (>$10K over 6 months)
GROUP BY manufacturer, specialty
HAVING organic_high_prescribers > 100  -- Significant cohort size
ORDER BY total_prescribed_millions DESC
LIMIT 20;

-- ============================================================================
-- QUERY 9: Year-over-Year Trends
-- ============================================================================
-- Shows how attribution rates and ROI change over time
-- Useful for identifying trends and seasonality

SELECT 
  year,
  EXTRACT(QUARTER FROM DATE(year, month, 1)) as quarter,
  COUNT(DISTINCT NPI) as providers,
  ROUND(SUM(totalNext6) / 1e9, 2) as total_prescribed_billions,
  ROUND(SUM(attributable_dollars) / 1e6, 2) as attributable_millions,
  ROUND(SUM(TotalDollarsFrom) / 1e6, 2) as op_spent_millions,
  ROUND(SAFE_DIVIDE(SUM(attributable_dollars), SUM(TotalDollarsFrom)), 2) as roi,
  ROUND(SAFE_DIVIDE(SUM(attributable_dollars), SUM(totalNext6)) * 100, 3) as attribution_pct
FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
GROUP BY year, quarter
ORDER BY year, quarter;

-- ============================================================================
-- QUERY 10: Provider Responsiveness Segmentation
-- ============================================================================
-- Segments providers by their responsiveness to payments
-- Helps identify which providers to target with payments

WITH provider_segments AS (
  SELECT 
    NPI,
    source_specialty,
    SUM(TotalDollarsFrom) as total_payments_received,
    AVG(attributable_pct) as avg_attribution_rate,
    COUNT(*) as observations,
    CASE 
      WHEN AVG(attributable_pct) > 0.10 THEN 'High Responder (>10%)'
      WHEN AVG(attributable_pct) > 0.05 THEN 'Medium Responder (5-10%)'
      WHEN AVG(attributable_pct) > 0.01 THEN 'Low Responder (1-5%)'
      WHEN AVG(attributable_pct) > 0 THEN 'Minimal Responder (<1%)'
      ELSE 'Non-Responder'
    END as responsiveness_segment
  FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
  WHERE TotalDollarsFrom > 0
  GROUP BY NPI, source_specialty
)
SELECT 
  responsiveness_segment,
  source_specialty as specialty,
  COUNT(DISTINCT NPI) as providers,
  ROUND(AVG(total_payments_received), 2) as avg_total_payments,
  ROUND(AVG(avg_attribution_rate) * 100, 2) as avg_attribution_pct,
  ROUND(AVG(observations), 1) as avg_observations_per_provider
FROM provider_segments
GROUP BY responsiveness_segment, specialty
ORDER BY responsiveness_segment, providers DESC;

-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================

-- Check total rows and completeness
SELECT 
  COUNT(*) as total_rows,
  COUNT(DISTINCT source_file) as unique_files,
  COUNT(DISTINCT source_manufacturer) as unique_manufacturers,
  COUNT(DISTINCT source_specialty) as unique_specialties,
  COUNT(DISTINCT NPI) as unique_providers
FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`;

-- Verify standardization worked correctly
SELECT 
  COUNT(*) as non_standard_columns
FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
WHERE 
  -- Check if any manufacturer-specific columns remain
  -- This should return 0 if standardization was successful
  FALSE; -- Placeholder - would need actual column names to check