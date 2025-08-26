-- Sample queries for Corewell Health NPI analysis
-- Generated: 2025-08-26 17:06:57
-- Table: data-analytics-389803.conflixis_agent.corewell_health_npis

-- Query 1: Join with rx_op_enhanced to get payment attribution
SELECT 
    cs.NPI,
    cs.Full_Name,
    cs.Primary_Specialty,
    cs.Primary_Hospital_Affiliation,
    rx.source_manufacturer,
    rx.year,
    rx.month,
    rx.TotalDollarsFrom as payment_received,
    rx.totalNext6 as prescriptions_next_6mo,
    rx.attributable_dollars,
    rx.attributable_pct,
    SAFE_DIVIDE(rx.attributable_dollars, rx.TotalDollarsFrom) as roi
FROM `data-analytics-389803.conflixis_agent.corewell_health_npis` cs
LEFT JOIN `data-analytics-389803.conflixis_agent.rx_op_enhanced_full` rx
    ON cs.NPI = rx.NPI
WHERE rx.TotalDollarsFrom > 0
ORDER BY rx.attributable_dollars DESC
LIMIT 100;

-- Query 2: Summary statistics for Corewell Health providers
SELECT 
    COUNT(DISTINCT cs.NPI) as total_cs_providers,
    COUNT(DISTINCT rx.NPI) as cs_providers_with_payments,
    SUM(rx.TotalDollarsFrom) as total_payments_received,
    SUM(rx.attributable_dollars) as total_attributable,
    SAFE_DIVIDE(SUM(rx.attributable_dollars), SUM(rx.TotalDollarsFrom)) as overall_roi
FROM `data-analytics-389803.conflixis_agent.corewell_health_npis` cs
LEFT JOIN `data-analytics-389803.conflixis_agent.rx_op_enhanced_full` rx
    ON cs.NPI = rx.NPI;

-- Query 3: High-risk Corewell Health providers (>30% attribution)
SELECT 
    cs.NPI,
    cs.Full_Name,
    cs.Primary_Specialty,
    cs.Primary_Hospital_Affiliation,
    AVG(rx.attributable_pct) as avg_attribution_pct,
    SUM(rx.TotalDollarsFrom) as total_payments,
    SUM(rx.attributable_dollars) as total_attributable
FROM `data-analytics-389803.conflixis_agent.corewell_health_npis` cs
INNER JOIN `data-analytics-389803.conflixis_agent.rx_op_enhanced_full` rx
    ON cs.NPI = rx.NPI
WHERE rx.TotalDollarsFrom > 0
GROUP BY cs.NPI, cs.Full_Name, cs.Primary_Specialty, cs.Primary_Hospital_Affiliation
HAVING AVG(rx.attributable_pct) > 0.30
ORDER BY avg_attribution_pct DESC;
