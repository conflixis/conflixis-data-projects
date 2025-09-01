"""
BigQuery Analysis Module
Runs all analysis directly in BigQuery, returning only aggregated results
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
import logging
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BigQueryAnalyzer:
    """Run analysis directly in BigQuery without downloading large datasets"""
    
    def __init__(self, bq_client: bigquery.Client, config: Dict, start_year: int, end_year: int):
        """
        Initialize BigQuery analyzer
        
        Args:
            bq_client: BigQuery client instance
            config: Configuration dictionary
            start_year: Start year for analysis
            end_year: End year for analysis
        """
        self.client = bq_client
        self.config = config
        self.start_year = start_year
        self.end_year = end_year
        
        # Table references
        temp_dataset = config['bigquery'].get('temp_dataset', 'temp')
        self.op_detailed = f"`{config['bigquery']['project_id']}.{temp_dataset}.{config['health_system']['short_name']}_open_payments_detailed_{start_year}_{end_year}`"
        self.op_summary = f"`{config['bigquery']['project_id']}.{temp_dataset}.{config['health_system']['short_name']}_open_payments_summary_{start_year}_{end_year}`"
        self.rx_detailed = f"`{config['bigquery']['project_id']}.{temp_dataset}.{config['health_system']['short_name']}_prescriptions_detailed_{start_year}_{end_year}`"
        self.rx_summary = f"`{config['bigquery']['project_id']}.{temp_dataset}.{config['health_system']['short_name']}_prescriptions_summary_{start_year}_{end_year}`"
    
    def analyze_open_payments(self) -> Dict[str, Any]:
        """
        Analyze Open Payments data directly in BigQuery
        
        Returns:
            Dictionary with analysis results (small aggregated data only)
        """
        results = {}
        
        # Overall metrics (1 row)
        metrics_query = f"""
        SELECT
            COUNT(DISTINCT physician_id) as unique_providers,
            COUNT(*) as total_transactions,
            SUM(total_amount) as total_payments,
            AVG(total_amount) as avg_payment,
            APPROX_QUANTILES(total_amount, 2)[OFFSET(1)] as median_payment,
            MAX(total_amount) as max_payment,
            MIN(total_amount) as min_payment,
            STDDEV(total_amount) as std_payment
        FROM {self.op_summary}
        """
        metrics_df = self._run_query(metrics_query)
        results['overall_metrics'] = metrics_df.iloc[0].to_dict()
        logger.info(f"Open Payments: {results['overall_metrics']['unique_providers']:,} providers, ${results['overall_metrics']['total_payments']:,.0f} total")
        
        # Yearly trends (5 rows)
        yearly_query = f"""
        SELECT
            payment_year,
            COUNT(DISTINCT physician_id) as providers,
            SUM(total_amount) as total_payments,
            AVG(total_amount) as avg_payment,
            SUM(payment_count) as transaction_count
        FROM {self.op_summary}
        GROUP BY payment_year
        ORDER BY payment_year
        """
        results['yearly_trends'] = self._run_query(yearly_query)
        
        # Payment categories (10-20 rows)
        categories_query = f"""
        SELECT
            payment_category,
            SUM(total_amount) as total_amount,
            AVG(total_amount) as avg_amount,
            SUM(payment_count) as transaction_count,
            COUNT(DISTINCT physician_id) as unique_providers,
            ROUND(100.0 * SUM(total_amount) / (SELECT SUM(total_amount) FROM {self.op_summary}), 1) as pct_of_total
        FROM {self.op_summary}
        GROUP BY payment_category
        ORDER BY total_amount DESC
        """
        results['payment_categories'] = self._run_query(categories_query)
        
        # Top manufacturers (20 rows)
        manufacturers_query = f"""
        SELECT
            manufacturer,
            SUM(total_amount) as total_payments,
            COUNT(DISTINCT physician_id) as unique_providers,
            SUM(payment_count) as transaction_count,
            ROUND(SUM(total_amount) / COUNT(DISTINCT physician_id), 2) as avg_per_provider,
            ROUND(100.0 * SUM(total_amount) / (SELECT SUM(total_amount) FROM {self.op_summary}), 2) as market_share
        FROM {self.op_summary}
        GROUP BY manufacturer
        ORDER BY total_payments DESC
        LIMIT 20
        """
        results['top_manufacturers'] = self._run_query(manufacturers_query)
        
        # Payment distribution tiers (5 rows)
        distribution_query = f"""
        WITH provider_totals AS (
            SELECT 
                physician_id,
                SUM(total_amount) as provider_total
            FROM {self.op_summary}
            GROUP BY physician_id
        )
        SELECT
            CASE 
                WHEN provider_total < 100 THEN '$0-100'
                WHEN provider_total < 500 THEN '$100-500'
                WHEN provider_total < 1000 THEN '$500-1K'
                WHEN provider_total < 5000 THEN '$1K-5K'
                WHEN provider_total < 10000 THEN '$5K-10K'
                ELSE '$10K+'
            END as payment_tier,
            COUNT(*) as provider_count,
            SUM(provider_total) as tier_total,
            AVG(provider_total) as tier_avg
        FROM provider_totals
        GROUP BY payment_tier
        ORDER BY 
            CASE payment_tier
                WHEN '$0-100' THEN 1
                WHEN '$100-500' THEN 2
                WHEN '$500-1K' THEN 3
                WHEN '$1K-5K' THEN 4
                WHEN '$5K-10K' THEN 5
                ELSE 6
            END
        """
        results['payment_distribution'] = self._run_query(distribution_query)
        
        # Consecutive years analysis (1 row)
        consecutive_query = f"""
        WITH provider_years AS (
            SELECT 
                physician_id,
                COUNT(DISTINCT payment_year) as years_received,
                STRING_AGG(CAST(payment_year AS STRING), ',' ORDER BY payment_year) as years_list
            FROM {self.op_summary}
            GROUP BY physician_id
        )
        SELECT
            AVG(years_received) as avg_years,
            COUNT(CASE WHEN years_received = 5 THEN 1 END) as all_5_years,
            COUNT(CASE WHEN years_received >= 3 THEN 1 END) as three_plus_years,
            COUNT(*) as total_providers
        FROM provider_years
        """
        results['consecutive_years'] = self._run_query(consecutive_query).iloc[0].to_dict()
        
        return results
    
    def analyze_prescriptions(self) -> Dict[str, Any]:
        """
        Analyze prescription data directly in BigQuery
        
        Returns:
            Dictionary with analysis results
        """
        results = {}
        
        # Overall metrics (1 row)
        metrics_query = f"""
        SELECT
            COUNT(DISTINCT NPI) as unique_prescribers,
            SUM(total_claims) as total_prescriptions,
            SUM(total_cost) as total_prescription_value,
            AVG(total_cost) as avg_prescription_value,
            SUM(total_beneficiaries) as total_beneficiaries
        FROM {self.rx_summary}
        """
        metrics_df = self._run_query(metrics_query)
        results['overall_metrics'] = metrics_df.iloc[0].to_dict()
        logger.info(f"Prescriptions: {results['overall_metrics']['unique_prescribers']:,} prescribers, ${results['overall_metrics']['total_prescription_value']:,.0f} total")
        
        # Yearly trends (5 rows)
        yearly_query = f"""
        SELECT
            rx_year,
            COUNT(DISTINCT NPI) as prescribers,
            SUM(total_cost) as total_cost,
            SUM(total_claims) as total_claims,
            AVG(avg_cost_per_claim) as avg_cost_per_claim
        FROM {self.rx_summary}
        GROUP BY rx_year
        ORDER BY rx_year
        """
        results['yearly_trends'] = self._run_query(yearly_query)
        
        # Top drugs by cost (20 rows)
        top_drugs_query = f"""
        SELECT
            BRAND_NAME,
            SUM(total_cost) as total_cost,
            SUM(total_claims) as prescription_count,
            COUNT(DISTINCT NPI) as prescriber_count,
            AVG(avg_cost_per_claim) as avg_cost_per_prescription,
            ROUND(100.0 * SUM(total_cost) / (SELECT SUM(total_cost) FROM {self.rx_summary}), 2) as pct_of_total_cost
        FROM {self.rx_summary}
        GROUP BY BRAND_NAME
        ORDER BY total_cost DESC
        LIMIT 20
        """
        results['top_drugs_by_cost'] = self._run_query(top_drugs_query)
        
        # Provider type analysis (4-5 rows)
        provider_type_query = f"""
        SELECT
            provider_type,
            COUNT(DISTINCT NPI) as provider_count,
            SUM(total_cost) as total_cost,
            SUM(total_claims) as total_claims,
            AVG(avg_cost_per_claim) as avg_cost_per_prescription
        FROM {self.rx_summary}
        GROUP BY provider_type
        ORDER BY total_cost DESC
        """
        results['provider_types'] = self._run_query(provider_type_query)
        
        # Specialty analysis (top 10 rows)
        specialty_query = f"""
        SELECT
            specialty,
            COUNT(DISTINCT NPI) as provider_count,
            SUM(total_cost) as total_cost,
            SUM(total_claims) as total_claims,
            AVG(avg_cost_per_claim) as avg_cost_per_prescription
        FROM {self.rx_summary}
        WHERE specialty IS NOT NULL AND specialty != ''
        GROUP BY specialty
        ORDER BY total_cost DESC
        LIMIT 10
        """
        results['top_specialties'] = self._run_query(specialty_query)
        
        return results
    
    def analyze_correlations(self) -> Dict[str, Any]:
        """
        Analyze payment-prescription correlations directly in BigQuery
        
        Returns:
            Dictionary with correlation results
        """
        results = {}
        
        # Overall correlation metrics (1 row)
        correlation_query = f"""
        WITH provider_summary AS (
            SELECT
                COALESCE(op.physician_id, rx.NPI) as provider_id,
                COALESCE(op_total, 0) as total_payments,
                COALESCE(op_count, 0) as payment_transactions,
                COALESCE(rx_cost, 0) as total_rx_cost,
                COALESCE(rx_claims, 0) as total_rx_claims
            FROM (
                SELECT 
                    physician_id,
                    SUM(total_amount) as op_total,
                    SUM(payment_count) as op_count
                FROM {self.op_summary}
                GROUP BY physician_id
            ) op
            FULL OUTER JOIN (
                SELECT
                    NPI,
                    SUM(total_cost) as rx_cost,
                    SUM(total_claims) as rx_claims
                FROM {self.rx_summary}
                GROUP BY NPI
            ) rx
            ON op.physician_id = rx.NPI
        )
        SELECT
            COUNT(*) as total_providers,
            COUNT(CASE WHEN total_payments > 0 AND total_rx_cost > 0 THEN 1 END) as providers_with_both,
            AVG(CASE WHEN total_payments > 0 THEN total_rx_cost ELSE NULL END) as avg_rx_with_payments,
            AVG(CASE WHEN total_payments = 0 THEN total_rx_cost ELSE NULL END) as avg_rx_without_payments,
            SAFE_DIVIDE(
                AVG(CASE WHEN total_payments > 0 THEN total_rx_cost ELSE NULL END),
                AVG(CASE WHEN total_payments = 0 THEN total_rx_cost ELSE NULL END)
            ) as rx_cost_influence,
            CORR(total_payments, total_rx_cost) as payment_rx_correlation
        FROM provider_summary
        """
        correlation_df = self._run_query(correlation_query)
        results['overall_metrics'] = correlation_df.iloc[0].to_dict()
        
        # ROI analysis (1 row)
        roi_query = f"""
        WITH matched_providers AS (
            SELECT
                op.physician_id,
                SUM(op.total_amount) as total_payments,
                SUM(rx.total_cost) as total_rx_cost
            FROM {self.op_summary} op
            INNER JOIN {self.rx_summary} rx
            ON op.physician_id = rx.NPI
            GROUP BY op.physician_id
            HAVING SUM(op.total_amount) > 0
        )
        SELECT
            SUM(total_rx_cost) / SUM(total_payments) as overall_roi,
            AVG(total_rx_cost / total_payments) as avg_provider_roi,
            APPROX_QUANTILES(total_rx_cost / total_payments, 2)[OFFSET(1)] as median_provider_roi
        FROM matched_providers
        """
        results['roi_metrics'] = self._run_query(roi_query).iloc[0].to_dict()
        
        # Provider type influence (4-5 rows)
        provider_influence_query = f"""
        WITH provider_summary AS (
            SELECT
                rx.provider_type,
                rx.NPI,
                SUM(op.total_amount) as payment_total,
                SUM(rx.total_cost) as rx_total
            FROM {self.rx_summary} rx
            LEFT JOIN {self.op_summary} op
            ON rx.NPI = op.physician_id
            GROUP BY rx.provider_type, rx.NPI
        )
        SELECT
            provider_type,
            COUNT(DISTINCT NPI) as provider_count,
            AVG(CASE WHEN payment_total > 0 THEN rx_total ELSE NULL END) as avg_rx_with_payments,
            AVG(CASE WHEN payment_total = 0 THEN rx_total ELSE NULL END) as avg_rx_without_payments,
            SAFE_DIVIDE(
                AVG(CASE WHEN payment_total > 0 THEN rx_total ELSE NULL END),
                AVG(CASE WHEN payment_total = 0 THEN rx_total ELSE NULL END)
            ) as influence_ratio
        FROM provider_summary
        GROUP BY provider_type
        """
        results['provider_type_influence'] = self._run_query(provider_influence_query)
        
        # Top drug correlations (20 rows)
        drug_correlation_query = f"""
        WITH drug_summary AS (
            SELECT
                rx.BRAND_NAME,
                rx.NPI,
                SUM(CASE WHEN op.physician_id IS NOT NULL THEN op.total_amount ELSE 0 END) as payment_total,
                SUM(rx.total_cost) as rx_total
            FROM {self.rx_summary} rx
            LEFT JOIN {self.op_summary} op
            ON rx.NPI = op.physician_id
            GROUP BY rx.BRAND_NAME, rx.NPI
        ),
        drug_metrics AS (
            SELECT
                BRAND_NAME,
                COUNT(DISTINCT NPI) as prescriber_count,
                COUNT(DISTINCT CASE WHEN payment_total > 0 THEN NPI END) as paid_prescriber_count,
                AVG(CASE WHEN payment_total > 0 THEN rx_total ELSE NULL END) as avg_rx_with_payments,
                AVG(CASE WHEN payment_total = 0 THEN rx_total ELSE NULL END) as avg_rx_without_payments,
                SUM(rx_total) as total_rx_cost
            FROM drug_summary
            GROUP BY BRAND_NAME
            HAVING COUNT(DISTINCT NPI) >= 10  -- Minimum prescribers for meaningful analysis
        )
        SELECT
            BRAND_NAME,
            prescriber_count,
            paid_prescriber_count,
            avg_rx_with_payments,
            avg_rx_without_payments,
            SAFE_DIVIDE(avg_rx_with_payments, avg_rx_without_payments) as influence_ratio,
            total_rx_cost
        FROM drug_metrics
        ORDER BY total_rx_cost DESC
        LIMIT 20
        """
        results['drug_correlations'] = self._run_query(drug_correlation_query)
        
        return results
    
    def analyze_risk_assessment(self) -> Dict[str, Any]:
        """
        Perform risk assessment directly in BigQuery
        
        Returns:
            Dictionary with risk metrics
        """
        results = {}
        
        # High-risk providers (providers with high payments and high prescribing)
        risk_query = f"""
        WITH provider_risk AS (
            SELECT
                COALESCE(op.physician_id, rx.NPI) as provider_id,
                COALESCE(op_total, 0) as payment_total,
                COALESCE(rx_cost, 0) as rx_total,
                COALESCE(op_percentile, 0) as payment_percentile,
                COALESCE(rx_percentile, 0) as rx_percentile
            FROM (
                SELECT 
                    physician_id,
                    SUM(total_amount) as op_total,
                    PERCENT_RANK() OVER (ORDER BY SUM(total_amount)) as op_percentile
                FROM {self.op_summary}
                GROUP BY physician_id
            ) op
            FULL OUTER JOIN (
                SELECT
                    NPI,
                    SUM(total_cost) as rx_cost,
                    PERCENT_RANK() OVER (ORDER BY SUM(total_cost)) as rx_percentile
                FROM {self.rx_summary}
                GROUP BY NPI
            ) rx
            ON op.physician_id = rx.NPI
        )
        SELECT
            COUNT(CASE WHEN payment_percentile >= 0.9 AND rx_percentile >= 0.9 THEN 1 END) as high_risk_count,
            COUNT(CASE WHEN payment_percentile >= 0.75 AND rx_percentile >= 0.75 THEN 1 END) as elevated_risk_count,
            COUNT(CASE WHEN payment_percentile >= 0.5 OR rx_percentile >= 0.5 THEN 1 END) as moderate_risk_count,
            COUNT(*) as total_providers,
            AVG(CASE WHEN payment_percentile >= 0.9 AND rx_percentile >= 0.9 THEN rx_total ELSE NULL END) as avg_high_risk_rx_cost
        FROM provider_risk
        """
        risk_df = self._run_query(risk_query)
        results['summary'] = risk_df.iloc[0].to_dict()
        
        # Top risk providers (20 rows)
        top_risk_query = f"""
        WITH provider_risk AS (
            SELECT
                COALESCE(op.physician_id, rx.NPI) as provider_id,
                COALESCE(op.first_name, '') as first_name,
                COALESCE(op.last_name, '') as last_name,
                COALESCE(op.specialty, rx.specialty) as specialty,
                COALESCE(op_total, 0) as payment_total,
                COALESCE(rx_cost, 0) as rx_total,
                (COALESCE(op_total, 0) + COALESCE(rx_cost, 0)) as combined_total
            FROM (
                SELECT 
                    physician_id,
                    ANY_VALUE(first_name) as first_name,
                    ANY_VALUE(last_name) as last_name,
                    ANY_VALUE(specialty) as specialty,
                    SUM(total_amount) as op_total
                FROM {self.op_summary}
                GROUP BY physician_id
            ) op
            FULL OUTER JOIN (
                SELECT
                    NPI,
                    ANY_VALUE(specialty) as specialty,
                    SUM(total_cost) as rx_cost
                FROM {self.rx_summary}
                GROUP BY NPI
            ) rx
            ON op.physician_id = rx.NPI
            WHERE COALESCE(op_total, 0) > 10000 OR COALESCE(rx_cost, 0) > 1000000
        )
        SELECT *
        FROM provider_risk
        ORDER BY combined_total DESC
        LIMIT 20
        """
        results['high_risk_providers'] = self._run_query(top_risk_query)
        
        return results
    
    def _run_query(self, query: str) -> pd.DataFrame:
        """
        Execute query and return results as DataFrame
        
        Args:
            query: SQL query to execute
            
        Returns:
            Query results as DataFrame
        """
        job = self.client.query(query)
        return job.to_dataframe()