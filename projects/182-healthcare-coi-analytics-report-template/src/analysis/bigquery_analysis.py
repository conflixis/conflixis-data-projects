"""
BigQuery Analysis Module
Runs all analysis directly in BigQuery, returning only aggregated results
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
import logging
from google.cloud import bigquery
from datetime import datetime

logger = logging.getLogger(__name__)


class QueryTracker:
    """Track query execution results for reporting"""
    
    def __init__(self):
        self.queries = []
        
    def add_query(self, name: str, status: str, rows: int = 0):
        """Add a query result to tracking"""
        self.queries.append({
            'name': name,
            'status': status,
            'rows': rows,
            'timestamp': datetime.now().isoformat()
        })
        
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all queries"""
        total = len(self.queries)
        successful = sum(1 for q in self.queries if q['status'] == 'success')
        empty = sum(1 for q in self.queries if q['status'] == 'empty')
        failed = sum(1 for q in self.queries if q['status'] == 'failed')
        
        return {
            'total_queries': total,
            'successful_with_data': successful,
            'successful_empty': empty,
            'failed': failed,
            'success_rate': (successful + empty) / total * 100 if total > 0 else 0,
            'data_completeness': successful / total * 100 if total > 0 else 0,
            'queries': self.queries
        }


class BigQueryAnalyzer:
    """Run analysis directly in BigQuery without downloading large datasets"""
    
    def __init__(self, bq_client: bigquery.Client, config: Dict, start_year: int, end_year: int, lineage_tracker=None):
        """
        Initialize BigQuery analyzer
        
        Args:
            bq_client: BigQuery client instance
            config: Configuration dictionary
            start_year: Start year for analysis
            end_year: End year for analysis
            lineage_tracker: Optional data lineage tracker
        """
        self.client = bq_client
        self.config = config
        self.start_year = start_year
        self.end_year = end_year
        self.lineage_tracker = lineage_tracker
        self.query_tracker = QueryTracker()  # Track query execution results
        
        # Table references
        temp_dataset = config['bigquery'].get('temp_dataset', 'temp')
        # Use 'springfield' instead of 'springfieldhealth' for table names
        # Also adjust year range to 2020_2024 based on actual data
        table_prefix = 'springfield' if config['health_system']['short_name'] == 'springfieldhealth' else config['health_system']['short_name']
        # Use actual year range from data
        actual_start = 2020
        actual_end = 2024
        self.op_detailed = f"`{config['bigquery']['project_id']}.{temp_dataset}.{table_prefix}_open_payments_detailed_{actual_start}_{actual_end}`"
        self.op_summary = f"`{config['bigquery']['project_id']}.{temp_dataset}.{table_prefix}_open_payments_summary_{actual_start}_{actual_end}`"
        self.rx_detailed = f"`{config['bigquery']['project_id']}.{temp_dataset}.{table_prefix}_prescriptions_detailed_{actual_start}_{actual_end}`"
        self.rx_summary = f"`{config['bigquery']['project_id']}.{temp_dataset}.{table_prefix}_prescriptions_summary_{actual_start}_{actual_end}`"
    
    def analyze_open_payments(self) -> Dict[str, Any]:
        """
        Analyze Open Payments data directly in BigQuery
        
        Returns:
            Dictionary with analysis results (small aggregated data only)
        """
        from datetime import datetime
        results = {}
        start_time = datetime.now()
        
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
        metrics_df, status = self._run_query(metrics_query, "open_payments_metrics")
        if status == "failed":
            raise Exception("Failed to query open payments metrics")
        results['overall_metrics'] = metrics_df.iloc[0].to_dict() if not metrics_df.empty else {}
        logger.info(f"Open Payments: {results['overall_metrics']['unique_providers']:,} providers, ${results['overall_metrics']['total_payments']:,.0f} total")
        
        # Yearly trends (5 rows)
        # Fixed: Aggregate by physician first to get correct averages
        yearly_query = f"""
        WITH physician_yearly AS (
            SELECT
                physician_id,
                payment_year,
                SUM(total_amount) as physician_year_total,
                SUM(payment_count) as physician_year_transactions
            FROM {self.op_summary}
            GROUP BY physician_id, payment_year
        )
        SELECT
            payment_year,
            COUNT(DISTINCT physician_id) as providers,
            SUM(physician_year_total) as total_payments,
            AVG(physician_year_total) as avg_payment,
            SUM(physician_year_transactions) as transaction_count
        FROM physician_yearly
        GROUP BY payment_year
        ORDER BY payment_year
        """
        yearly_df, status = self._run_query(yearly_query, "open_payments_yearly_trends")
        if status == "failed":
            logger.error("Failed to query yearly trends")
            yearly_df = pd.DataFrame()
        results['yearly_trends'] = yearly_df
        
        # Payment categories (10-20 rows)
        # Fixed: Aggregate by physician-category first to get correct averages
        categories_query = f"""
        WITH physician_category AS (
            SELECT
                physician_id,
                payment_category,
                SUM(total_amount) as physician_category_total,
                SUM(payment_count) as physician_category_transactions
            FROM {self.op_summary}
            GROUP BY physician_id, payment_category
        )
        SELECT
            payment_category,
            SUM(physician_category_total) as total_amount,
            AVG(physician_category_total) as avg_amount,
            SUM(physician_category_transactions) as transaction_count,
            COUNT(DISTINCT physician_id) as unique_providers,
            ROUND(100.0 * SUM(physician_category_total) / 
                (SELECT SUM(physician_category_total) FROM physician_category), 1) as pct_of_total
        FROM physician_category
        GROUP BY payment_category
        ORDER BY total_amount DESC
        """
        categories_df, status = self._run_query(categories_query, "payment_categories")
        if status == "failed":
            logger.error("Failed to query payment categories")
            categories_df = pd.DataFrame()
        results['payment_categories'] = categories_df
        
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
        manufacturers_df, status = self._run_query(manufacturers_query, "top_manufacturers")
        if status == "failed":
            logger.error("Failed to query top manufacturers")
            manufacturers_df = pd.DataFrame()
        results['top_manufacturers'] = manufacturers_df
        
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
        distribution_df, status = self._run_query(distribution_query, "payment_distribution")
        if status == "failed":
            logger.error("Failed to query payment distribution")
            distribution_df = pd.DataFrame()
        results['payment_distribution'] = distribution_df
        
        # Consecutive years analysis - return as DataFrame for table display
        consecutive_query = f"""
        WITH provider_years AS (
            SELECT 
                physician_id,
                COUNT(DISTINCT payment_year) as years_received,
                STRING_AGG(CAST(payment_year AS STRING), ',' ORDER BY payment_year) as years_list,
                SUM(total_amount) as total_payments
            FROM {self.op_summary}
            GROUP BY physician_id
        ),
        year_groups AS (
            SELECT
                years_received as consecutive_years,
                COUNT(*) as provider_count,
                AVG(total_payments) as avg_total_payment,
                SUM(total_payments) as total_payment_amount,
                MAX(total_payments) as max_payment,
                MIN(total_payments) as min_payment
            FROM provider_years
            GROUP BY years_received
        )
        SELECT
            consecutive_years,
            provider_count,
            avg_total_payment,
            total_payment_amount,
            max_payment,
            min_payment
        FROM year_groups
        ORDER BY consecutive_years
        """
        consecutive_df, status = self._run_query(consecutive_query, "consecutive_years_payments")
        if status == "failed":
            logger.error("Failed to query consecutive years")
            consecutive_df = pd.DataFrame()
        results['consecutive_years'] = consecutive_df
        
        # Track lineage
        if self.lineage_tracker:
            execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
            self.lineage_tracker.add_analysis_step(
                'open_payments_analysis',
                [self.op_summary.replace('`', '')],
                {
                    'unique_providers': results['overall_metrics'].get('unique_providers', 0),
                    'total_payments': results['overall_metrics'].get('total_payments', 0)
                },
                execution_time
            )
        
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
        metrics_df, status = self._run_query(metrics_query, "prescription_metrics")
        if status == "failed":
            raise Exception("Failed to query prescription metrics")
        results['overall_metrics'] = metrics_df.iloc[0].to_dict() if not metrics_df.empty else {}
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
        yearly_df, status = self._run_query(yearly_query, "prescription_yearly_trends")
        if status == "failed":
            logger.error("Failed to query prescription yearly trends")
            yearly_df = pd.DataFrame()
        results['yearly_trends'] = yearly_df
        
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
        top_drugs_df, status = self._run_query(top_drugs_query, "top_drugs_by_cost")
        if status == "failed":
            logger.error("Failed to query top drugs")
            top_drugs_df = pd.DataFrame()
        results['top_drugs_by_cost'] = top_drugs_df
        
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
        provider_types_df, status = self._run_query(provider_type_query, "prescription_provider_types")
        if status == "failed":
            logger.error("Failed to query provider types")
            provider_types_df = pd.DataFrame()
        results['provider_types'] = provider_types_df
        
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
        specialties_df, status = self._run_query(specialty_query, "top_specialties")
        if status == "failed":
            logger.error("Failed to query top specialties")
            specialties_df = pd.DataFrame()
        results['top_specialties'] = specialties_df
        
        # Also store as by_specialty for compatibility
        results['by_specialty'] = results['top_specialties'].copy() if not results['top_specialties'].empty else pd.DataFrame()
        
        # Provider type analysis
        provider_type_query = f"""
        SELECT
            COALESCE(provider_type, 'Unknown') as provider_type,
            COUNT(DISTINCT NPI) as provider_count,
            SUM(total_claims) as total_prescriptions,
            SUM(total_cost) as total_cost,
            AVG(total_cost) as avg_cost_per_provider
        FROM {self.rx_summary}
        GROUP BY provider_type
        ORDER BY total_cost DESC
        """
        provider_type_df, status = self._run_query(provider_type_query, "prescription_by_provider_type")
        if status == "failed":
            logger.error("Failed to query prescriptions by provider type")
            provider_type_df = pd.DataFrame()
        results['by_provider_type'] = provider_type_df
        
        # Drug-specific analysis (top drugs by cost)
        drug_specific_query = f"""
        SELECT
            COALESCE(BRAND_NAME, GENERIC_NAME, 'Unknown') as drug_name,
            SUM(total_cost) as total_cost,
            SUM(total_claims) as claims,
            AVG(total_cost / NULLIF(total_claims, 0)) as avg_cost_per_claim,
            COUNT(DISTINCT NPI) as unique_prescribers
        FROM {self.rx_summary}
        WHERE BRAND_NAME IS NOT NULL OR GENERIC_NAME IS NOT NULL
        GROUP BY drug_name
        ORDER BY total_cost DESC
        LIMIT 20
        """
        drug_df, status = self._run_query(drug_specific_query, "drug_specific_analysis")
        if status == "failed":
            logger.error("Failed to query drug specific analysis")
            drug_df = pd.DataFrame()
        if drug_df.empty:
            logger.warning("Drug specific query returned no data")
        else:
            logger.info(f"Drug specific query returned {len(drug_df)} rows")
        results['drug_specific'] = drug_df
        
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
                COALESCE(CAST(op.physician_id AS STRING), CAST(rx.NPI AS STRING)) as provider_id,
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
            ON CAST(op.physician_id AS STRING) = CAST(rx.NPI AS STRING)
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
        correlation_df, status = self._run_query(correlation_query, "payment_prescription_correlation")
        if status == "failed":
            logger.error("Failed to query payment-prescription correlation")
            correlation_df = pd.DataFrame()
        results['overall_metrics'] = correlation_df.iloc[0].to_dict() if not correlation_df.empty else {}
        
        # ROI analysis (1 row)
        roi_query = f"""
        WITH matched_providers AS (
            SELECT
                op.physician_id,
                SUM(op.total_amount) as total_payments,
                SUM(rx.total_cost) as total_rx_cost
            FROM {self.op_summary} op
            INNER JOIN {self.rx_summary} rx
            ON CAST(op.physician_id AS STRING) = CAST(rx.NPI AS STRING)
            GROUP BY op.physician_id
            HAVING SUM(op.total_amount) > 0
        )
        SELECT
            SUM(total_rx_cost) / SUM(total_payments) as overall_roi,
            AVG(total_rx_cost / total_payments) as avg_provider_roi,
            APPROX_QUANTILES(total_rx_cost / total_payments, 2)[OFFSET(1)] as median_provider_roi
        FROM matched_providers
        """
        roi_df, status = self._run_query(roi_query, "roi_metrics")
        if status == "failed" or roi_df.empty:
            logger.error("Failed to query ROI metrics or empty result")
            results['roi_metrics'] = {}
        else:
            results['roi_metrics'] = roi_df.iloc[0].to_dict()
        
        # Provider type influence (4-5 rows)
        # Fixed to handle year dimension in summary table
        provider_influence_query = f"""
        WITH provider_summary AS (
            SELECT
                rx.provider_type,
                rx.NPI,
                SUM(op.total_amount) as payment_total,
                -- Fix: Average across years instead of summing
                SUM(rx.total_cost) / COUNT(DISTINCT rx.rx_year) as rx_total
            FROM {self.rx_summary} rx
            LEFT JOIN {self.op_summary} op
            ON CAST(rx.NPI AS STRING) = CAST(op.physician_id AS STRING)
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
        provider_influence_df, status = self._run_query(provider_influence_query, "provider_type_influence")
        if status == "failed":
            logger.error("Failed to query provider type influence")
            provider_influence_df = pd.DataFrame()
        results['provider_type_influence'] = provider_influence_df
        
        # Top drug correlations (20 rows)
        # Fixed to handle year dimension in summary table
        drug_correlation_query = f"""
        WITH drug_summary AS (
            SELECT
                rx.BRAND_NAME,
                rx.NPI,
                SUM(CASE WHEN op.physician_id IS NOT NULL THEN op.total_amount ELSE 0 END) as payment_total,
                -- Fix: Average across years for per-provider metrics
                SUM(rx.total_cost) / COUNT(DISTINCT rx.rx_year) as rx_total_yearly_avg
            FROM {self.rx_summary} rx
            LEFT JOIN {self.op_summary} op
            ON CAST(rx.NPI AS STRING) = CAST(op.physician_id AS STRING)
            GROUP BY rx.BRAND_NAME, rx.NPI
        ),
        drug_metrics AS (
            SELECT
                ds.BRAND_NAME,
                COUNT(DISTINCT ds.NPI) as prescriber_count,
                COUNT(DISTINCT CASE WHEN ds.payment_total > 0 THEN ds.NPI END) as paid_prescriber_count,
                AVG(CASE WHEN ds.payment_total > 0 THEN ds.rx_total_yearly_avg ELSE NULL END) as avg_rx_with_payments,
                AVG(CASE WHEN ds.payment_total = 0 THEN ds.rx_total_yearly_avg ELSE NULL END) as avg_rx_without_payments,
                -- Get the actual total from the original table, not summing averages
                (SELECT SUM(total_cost) FROM {self.rx_summary} WHERE BRAND_NAME = ds.BRAND_NAME) as total_rx_cost
            FROM drug_summary ds
            GROUP BY ds.BRAND_NAME
            HAVING COUNT(DISTINCT ds.NPI) >= 10  -- Minimum prescribers for meaningful analysis
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
        drug_corr_df, status = self._run_query(drug_correlation_query, "drug_correlations")
        if status == "failed":
            logger.error("Failed to query drug correlations")
            drug_corr_df = pd.DataFrame()
        results['drug_correlations'] = drug_corr_df
        
        # Also store as drug_specific for compatibility
        results['drug_specific'] = results['drug_correlations'].copy() if not results['drug_correlations'].empty else pd.DataFrame()
        
        # Payment tier analysis
        payment_tier_query = f"""
        WITH provider_totals AS (
            SELECT 
                op.physician_id,
                SUM(op.total_amount) as payment_total,
                COUNT(*) as payment_count
            FROM {self.op_summary} op
            GROUP BY op.physician_id
        ),
        provider_rx AS (
            SELECT
                NPI,
                SUM(total_cost) as rx_total,
                SUM(total_claims) as rx_claims
            FROM {self.rx_summary}
            GROUP BY NPI
        ),
        tiered_providers AS (
            SELECT
                pt.physician_id,
                CASE 
                    WHEN pt.payment_total < 100 THEN '<$100'
                    WHEN pt.payment_total < 500 THEN '$100-500'
                    WHEN pt.payment_total < 1000 THEN '$500-1K'
                    WHEN pt.payment_total < 5000 THEN '$1K-5K'
                    WHEN pt.payment_total < 10000 THEN '$5K-10K'
                    ELSE '$10K+'
                END as payment_tier,
                pt.payment_total,
                pt.payment_count,
                COALESCE(pr.rx_total, 0) as rx_total,
                COALESCE(pr.rx_claims, 0) as rx_claims
            FROM provider_totals pt
            LEFT JOIN provider_rx pr ON CAST(pt.physician_id AS STRING) = CAST(pr.NPI AS STRING)
        )
        SELECT
            payment_tier as tier,
            COUNT(*) as provider_count,
            AVG(payment_total) as avg_payment,
            AVG(rx_total) as avg_rx_cost,
            SUM(rx_total) as total_rx_cost,
            AVG(rx_claims) as avg_claims,
            SAFE_DIVIDE(SUM(rx_total), SUM(payment_total)) as tier_roi
        FROM tiered_providers
        GROUP BY payment_tier
        ORDER BY 
            CASE payment_tier
                WHEN '<$100' THEN 1
                WHEN '$100-500' THEN 2
                WHEN '$500-1K' THEN 3
                WHEN '$1K-5K' THEN 4
                WHEN '$5K-10K' THEN 5
                ELSE 6
            END
        """
        payment_tiers_df, status = self._run_query(payment_tier_query, "payment_tiers")
        if status == "failed":
            logger.error("Failed to query payment tiers")
            payment_tiers_df = pd.DataFrame()
        results['payment_tiers'] = payment_tiers_df
        
        # Provider vulnerability DataFrame
        # Fixed to handle year dimension in summary table
        provider_vulnerability_query = f"""
        WITH provider_metrics AS (
            SELECT
                COALESCE(rx.provider_type, 'Unknown') as provider_type,
                rx.NPI,
                COALESCE(SUM(op.total_amount), 0) as total_payments,
                -- Fix: Average across years instead of summing
                SUM(rx.total_cost) / COUNT(DISTINCT rx.rx_year) as total_rx_cost,
                SUM(rx.total_claims) as total_claims
            FROM {self.rx_summary} rx
            LEFT JOIN {self.op_summary} op ON CAST(rx.NPI AS STRING) = CAST(op.physician_id AS STRING)
            GROUP BY rx.provider_type, rx.NPI
        )
        SELECT
            provider_type,
            COUNT(DISTINCT NPI) as provider_count,
            COUNT(DISTINCT CASE WHEN total_payments > 0 THEN NPI END) as providers_with_payments,
            AVG(total_payments) as avg_payments,
            AVG(total_rx_cost) as avg_rx_cost,
            AVG(CASE WHEN total_payments > 0 THEN total_rx_cost END) as avg_rx_with_payments,
            AVG(CASE WHEN total_payments = 0 THEN total_rx_cost END) as avg_rx_without_payments,
            SAFE_DIVIDE(
                AVG(CASE WHEN total_payments > 0 THEN total_rx_cost END),
                AVG(CASE WHEN total_payments = 0 THEN total_rx_cost END)
            ) as influence_factor
        FROM provider_metrics
        GROUP BY provider_type
        ORDER BY provider_count DESC
        """
        provider_vuln_df, status = self._run_query(provider_vulnerability_query, "provider_type_vulnerability")
        if status == "failed":
            logger.error("Failed to query provider type vulnerability")
            provider_vuln_df = pd.DataFrame()
        results['provider_type_vulnerability'] = provider_vuln_df
        
        # Consecutive years analysis with prescription correlation
        consecutive_years_query = f"""
        WITH provider_years AS (
            SELECT 
                op.physician_id,
                COUNT(DISTINCT op.payment_year) as years_of_payments,
                SUM(op.total_amount) as total_payments,
                SUM(rx.total_cost) as total_rx_value
            FROM {self.op_summary} op
            LEFT JOIN {self.rx_summary} rx ON CAST(op.physician_id AS STRING) = CAST(rx.NPI AS STRING)
            GROUP BY op.physician_id
        ),
        year_groups AS (
            SELECT
                years_of_payments,
                COUNT(*) as provider_count,
                AVG(total_payments) as avg_total_payments,
                AVG(total_rx_value) as avg_total_rx_value,
                SAFE_DIVIDE(AVG(total_rx_value), AVG(total_payments)) as multiplier_vs_single_year
            FROM provider_years
            WHERE years_of_payments > 0
            GROUP BY years_of_payments
        )
        SELECT
            CASE years_of_payments
                WHEN 1 THEN '1 Year'
                WHEN 2 THEN '2 Consecutive'
                WHEN 3 THEN '3 Consecutive'
                WHEN 4 THEN '4 Consecutive'
                WHEN 5 THEN '5 Consecutive'
                ELSE CAST(years_of_payments AS STRING) || ' Years'
            END as years_of_payments,
            provider_count,
            avg_total_payments,
            avg_total_rx_value,
            CASE 
                WHEN years_of_payments = 1 THEN 'Baseline'
                ELSE CAST(ROUND(multiplier_vs_single_year, 2) AS STRING) || 'x'
            END as multiplier_vs_single_year
        FROM year_groups
        ORDER BY years_of_payments
        LIMIT 5
        """
        consecutive_df, status = self._run_query(consecutive_years_query, "consecutive_years_correlation")
        if consecutive_df.empty:
            logger.warning("Consecutive years query returned no data")
            # Create a default DataFrame with expected structure
            consecutive_df = pd.DataFrame({
                'years_of_payments': ['1 Year', '2 Consecutive', '3 Consecutive', '4 Consecutive', '5 Consecutive'],
                'provider_count': [0, 0, 0, 0, 0],
                'avg_total_payments': [0, 0, 0, 0, 0],
                'avg_total_rx_value': [0, 0, 0, 0, 0],
                'multiplier_vs_single_year': ['Baseline', '0x', '0x', '0x', '0x']
            })
        else:
            logger.info(f"Consecutive years query returned {len(consecutive_df)} rows")
        results['consecutive_years'] = consecutive_df
        
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
                COALESCE(CAST(op.physician_id AS STRING), CAST(rx.NPI AS STRING)) as provider_id,
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
            ON CAST(op.physician_id AS STRING) = CAST(rx.NPI AS STRING)
        )
        SELECT
            COUNT(CASE WHEN payment_percentile >= 0.9 AND rx_percentile >= 0.9 THEN 1 END) as high_risk_count,
            COUNT(CASE WHEN payment_percentile >= 0.75 AND rx_percentile >= 0.75 THEN 1 END) as elevated_risk_count,
            COUNT(CASE WHEN payment_percentile >= 0.5 OR rx_percentile >= 0.5 THEN 1 END) as moderate_risk_count,
            COUNT(*) as total_providers,
            AVG(CASE WHEN payment_percentile >= 0.9 AND rx_percentile >= 0.9 THEN rx_total ELSE NULL END) as avg_high_risk_rx_cost
        FROM provider_risk
        """
        risk_df, status = self._run_query(risk_query, "risk_assessment_summary")
        if status == "failed":
            logger.error("Failed to query risk assessment summary")
            risk_df = pd.DataFrame()
        results['summary'] = risk_df.iloc[0].to_dict()
        
        # Top risk providers (20 rows)
        top_risk_query = f"""
        WITH provider_risk AS (
            SELECT
                COALESCE(CAST(op.physician_id AS STRING), CAST(rx.NPI AS STRING)) as provider_id,
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
            ON CAST(op.physician_id AS STRING) = CAST(rx.NPI AS STRING)
            WHERE COALESCE(op_total, 0) > 10000 OR COALESCE(rx_cost, 0) > 1000000
        )
        SELECT *
        FROM provider_risk
        ORDER BY combined_total DESC
        LIMIT 20
        """
        high_risk_df, status = self._run_query(top_risk_query, "high_risk_providers")
        if status == "failed":
            logger.error("Failed to query high risk providers")
            high_risk_df = pd.DataFrame()
        results['high_risk_providers'] = high_risk_df
        
        # Risk distribution for table display
        risk_distribution_query = f"""
        WITH provider_risk AS (
            SELECT
                COALESCE(CAST(op.physician_id AS STRING), CAST(rx.NPI AS STRING)) as provider_id,
                COALESCE(op_total, 0) as payment_total,
                COALESCE(rx_cost, 0) as rx_total,
                COALESCE(op_percentile, 0) as payment_percentile,
                COALESCE(rx_percentile, 0) as rx_percentile,
                CASE 
                    WHEN COALESCE(op_percentile, 0) >= 0.9 AND COALESCE(rx_percentile, 0) >= 0.9 THEN 'High Risk'
                    WHEN COALESCE(op_percentile, 0) >= 0.7 AND COALESCE(rx_percentile, 0) >= 0.7 THEN 'Medium Risk'
                    ELSE 'Low Risk'
                END as risk_level
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
            ON CAST(op.physician_id AS STRING) = CAST(rx.NPI AS STRING)
        )
        SELECT
            risk_level,
            COUNT(*) as provider_count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percent_of_total,
            CASE risk_level
                WHEN 'High Risk' THEN 'High payments + prescriptions'
                WHEN 'Medium Risk' THEN 'Moderate payments + prescriptions'
                ELSE 'Low payments or prescriptions'
            END as key_risk_indicators,
            ROUND(AVG(payment_percentile * 100 + rx_percentile * 100) / 2, 1) as avg_risk_score
        FROM provider_risk
        GROUP BY risk_level
        ORDER BY 
            CASE risk_level
                WHEN 'High Risk' THEN 1
                WHEN 'Medium Risk' THEN 2
                ELSE 3
            END
        """
        risk_dist_df, status = self._run_query(risk_distribution_query, "risk_distribution")
        if status == "failed":
            logger.error("Failed to query risk distribution")
            risk_dist_df = pd.DataFrame()
        if risk_dist_df.empty:
            logger.warning("Risk distribution query returned no data")
            # Create a default DataFrame with expected structure
            risk_dist_df = pd.DataFrame({
                'risk_level': ['High Risk', 'Medium Risk', 'Low Risk'],
                'provider_count': [0, 0, 0],
                'percent_of_total': [0.0, 0.0, 0.0],
                'key_risk_indicators': ['High payments + prescriptions', 'Moderate payments + prescriptions', 'Low payments or prescriptions'],
                'avg_risk_score': [0.0, 0.0, 0.0]
            })
        else:
            logger.info(f"Risk distribution query returned {len(risk_dist_df)} rows")
        results['risk_distribution'] = risk_dist_df
        
        return results
    
    def get_query_summary(self) -> Dict[str, Any]:
        """Get summary of all query executions"""
        return self.query_tracker.get_summary()
    
    def _run_query(self, query: str, query_name: str = "unnamed") -> tuple[pd.DataFrame, str]:
        """
        Execute query and return results as DataFrame with status
        
        Args:
            query: SQL query to execute
            query_name: Name of the query for tracking
            
        Returns:
            Tuple of (DataFrame, status) where status is:
            - "success": Query executed and returned data
            - "empty": Query executed successfully but returned 0 rows
            - "failed": Query failed to execute
        """
        try:
            job = self.client.query(query)
            df = job.to_dataframe()
            
            if df.empty:
                logger.info(f"[{query_name}] Query executed successfully but returned 0 rows")
                self.query_tracker.add_query(query_name, "empty", 0)
                return df, "empty"
            else:
                logger.info(f"[{query_name}] Query executed successfully, returned {len(df)} rows")
                self.query_tracker.add_query(query_name, "success", len(df))
                return df, "success"
                
        except Exception as e:
            logger.error(f"[{query_name}] Query failed with error: {str(e)}")
            logger.error(f"Failed query was:\n{query[:500]}...")  # Log first 500 chars
            self.query_tracker.add_query(query_name, "failed", 0)
            return pd.DataFrame(), "failed"