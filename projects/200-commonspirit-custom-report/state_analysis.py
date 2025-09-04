#!/usr/bin/env python3
"""
State-Level Analysis for CommonSpirit Health COI Report
Analyzes payment patterns, prescribing behaviors, and risk profiles by US state
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from typing import Dict, List, Any
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import logging

# Add parent directory to path for imports
sys.path.append('/home/incent/conflixis-data-projects/projects/182-healthcare-coi-analytics-report-template')
from src.data.bigquery_connector import BigQueryConnector

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StateAnalyzer:
    """Perform state-level analysis for CommonSpirit Health providers"""
    
    def __init__(self):
        """Initialize analyzer with BigQuery connection"""
        self.bq = BigQueryConnector()
        self.client = self.bq.client
        self.project_id = "data-analytics-389803"
        self.dataset = "conflixis_agent"
        self.temp_dataset = "temp"
        
    def get_state_distribution(self) -> pd.DataFrame:
        """Get provider distribution by state"""
        query = f"""
        WITH provider_states AS (
            SELECT 
                n.NPI,
                p.HQ_STATE as state,
                p.HQ_CITY as city,
                p.SPECIALTY_PRIMARY,
                p.SPECIALTY_PRIMARY_GROUP,
                p.ROLE_NAME
            FROM `{self.project_id}.{self.temp_dataset}.commonspirit_provider_npis` n
            LEFT JOIN `{self.project_id}.{self.dataset}.PHYSICIANS_OVERVIEW` p
                ON CAST(n.NPI AS STRING) = CAST(p.NPI AS STRING)
            WHERE p.HQ_STATE IS NOT NULL
        )
        SELECT 
            state,
            COUNT(DISTINCT NPI) as provider_count,
            COUNT(DISTINCT city) as city_count,
            COUNT(DISTINCT SPECIALTY_PRIMARY) as specialty_count,
            -- Top specialties in state
            STRING_AGG(DISTINCT SPECIALTY_PRIMARY_GROUP, ', ' ORDER BY SPECIALTY_PRIMARY_GROUP LIMIT 5) as top_specialty_groups
        FROM provider_states
        GROUP BY state
        ORDER BY provider_count DESC
        """
        
        logger.info("Fetching state distribution data...")
        return self.client.query(query).to_dataframe()
    
    def get_state_payment_analysis(self) -> pd.DataFrame:
        """Analyze payment patterns by state"""
        query = f"""
        WITH provider_states AS (
            SELECT DISTINCT
                CAST(n.NPI AS STRING) as npi,
                p.HQ_STATE as state
            FROM `{self.project_id}.{self.temp_dataset}.commonspirit_provider_npis` n
            INNER JOIN `{self.project_id}.{self.dataset}.PHYSICIANS_OVERVIEW` p
                ON CAST(n.NPI AS STRING) = CAST(p.NPI AS STRING)
            WHERE p.HQ_STATE IS NOT NULL
        ),
        state_payments AS (
            SELECT 
                ps.state,
                COUNT(DISTINCT op.physician_id) as providers_with_payments,
                COUNT(DISTINCT op.manufacturer_name) as unique_manufacturers,
                SUM(op.total_amount) as total_payments,
                AVG(op.total_amount) as avg_payment_per_provider,
                SUM(op.payment_count) as total_transactions
            FROM `{self.project_id}.{self.temp_dataset}.commonspirit_open_payments_summary_2020_2024` op
            INNER JOIN provider_states ps
                ON CAST(op.physician_id AS STRING) = ps.npi
            GROUP BY ps.state
        )
        SELECT 
            state,
            providers_with_payments,
            unique_manufacturers,
            total_payments,
            ROUND(avg_payment_per_provider, 2) as avg_payment_per_provider,
            total_transactions,
            ROUND(total_payments / NULLIF(providers_with_payments, 0), 2) as avg_total_per_provider
        FROM state_payments
        ORDER BY total_payments DESC
        """
        
        logger.info("Analyzing state payment patterns...")
        return self.client.query(query).to_dataframe()
    
    def get_state_prescription_patterns(self) -> pd.DataFrame:
        """Analyze prescription patterns by state"""
        query = f"""
        WITH provider_states AS (
            SELECT DISTINCT
                CAST(n.NPI AS STRING) as npi,
                p.HQ_STATE as state,
                p.SPECIALTY_PRIMARY_GROUP
            FROM `{self.project_id}.{self.temp_dataset}.commonspirit_provider_npis` n
            INNER JOIN `{self.project_id}.{self.dataset}.PHYSICIANS_OVERVIEW` p
                ON CAST(n.NPI AS STRING) = CAST(p.NPI AS STRING)
            WHERE p.HQ_STATE IS NOT NULL
        ),
        state_prescriptions AS (
            SELECT 
                ps.state,
                COUNT(DISTINCT rx.npi) as prescribers,
                COUNT(DISTINCT rx.drug_name) as unique_drugs,
                SUM(rx.total_claim_count) as total_claims,
                SUM(rx.total_drug_cost) as total_prescription_cost,
                AVG(rx.total_drug_cost / NULLIF(rx.total_claim_count, 0)) as avg_cost_per_claim
            FROM `{self.project_id}.{self.temp_dataset}.commonspirit_prescriptions_summary_2020_2024` rx
            INNER JOIN provider_states ps
                ON CAST(rx.npi AS STRING) = ps.npi
            GROUP BY ps.state
        )
        SELECT 
            state,
            prescribers,
            unique_drugs,
            total_claims,
            ROUND(total_prescription_cost, 2) as total_prescription_cost,
            ROUND(avg_cost_per_claim, 2) as avg_cost_per_claim,
            ROUND(total_prescription_cost / NULLIF(prescribers, 0), 2) as avg_rx_cost_per_prescriber
        FROM state_prescriptions
        ORDER BY total_prescription_cost DESC
        """
        
        logger.info("Analyzing state prescription patterns...")
        return self.client.query(query).to_dataframe()
    
    def get_state_risk_profiles(self) -> pd.DataFrame:
        """Calculate risk profiles by state"""
        query = f"""
        WITH provider_states AS (
            SELECT DISTINCT
                CAST(n.NPI AS STRING) as npi,
                p.HQ_STATE as state
            FROM `{self.project_id}.{self.temp_dataset}.commonspirit_provider_npis` n
            INNER JOIN `{self.project_id}.{self.dataset}.PHYSICIANS_OVERVIEW` p
                ON CAST(n.NPI AS STRING) = CAST(p.NPI AS STRING)
            WHERE p.HQ_STATE IS NOT NULL
        ),
        provider_metrics AS (
            SELECT 
                ps.npi,
                ps.state,
                COALESCE(op.total_amount, 0) as total_payments,
                COALESCE(op.payment_count, 0) as payment_count,
                COALESCE(op.manufacturer_count, 0) as manufacturer_count,
                COALESCE(rx.total_drug_cost, 0) as prescription_cost,
                COALESCE(rx.drug_count, 0) as drug_count
            FROM provider_states ps
            LEFT JOIN (
                SELECT 
                    CAST(physician_id AS STRING) as physician_id,
                    SUM(total_amount) as total_amount,
                    SUM(payment_count) as payment_count,
                    COUNT(DISTINCT manufacturer_name) as manufacturer_count
                FROM `{self.project_id}.{self.temp_dataset}.commonspirit_open_payments_summary_2020_2024`
                GROUP BY physician_id
            ) op ON ps.npi = op.physician_id
            LEFT JOIN (
                SELECT 
                    CAST(npi AS STRING) as npi,
                    SUM(total_drug_cost) as total_drug_cost,
                    COUNT(DISTINCT drug_name) as drug_count
                FROM `{self.project_id}.{self.temp_dataset}.commonspirit_prescriptions_summary_2020_2024`
                GROUP BY npi
            ) rx ON ps.npi = rx.npi
        ),
        provider_risk AS (
            SELECT 
                state,
                npi,
                total_payments,
                prescription_cost,
                -- Calculate risk category
                CASE 
                    WHEN total_payments > 10000 AND prescription_cost > 1000000 THEN 'HIGH'
                    WHEN total_payments > 5000 AND prescription_cost > 500000 THEN 'MEDIUM'
                    ELSE 'LOW'
                END as risk_category
            FROM provider_metrics
        )
        SELECT 
            state,
            COUNT(DISTINCT npi) as total_providers,
            -- Risk distribution
            COUNT(DISTINCT CASE WHEN risk_category = 'HIGH' THEN npi END) as high_risk_providers,
            COUNT(DISTINCT CASE WHEN risk_category = 'MEDIUM' THEN npi END) as medium_risk_providers,
            COUNT(DISTINCT CASE WHEN risk_category = 'LOW' THEN npi END) as low_risk_providers,
            -- Risk percentages
            ROUND(COUNT(DISTINCT CASE WHEN risk_category = 'HIGH' THEN npi END) * 100.0 / 
                  NULLIF(COUNT(DISTINCT npi), 0), 2) as pct_high_risk,
            -- Financial metrics
            ROUND(AVG(total_payments), 2) as avg_payments_per_provider,
            ROUND(AVG(prescription_cost), 2) as avg_rx_cost_per_provider,
            -- Payment to prescription ratio
            ROUND(SUM(total_payments) / NULLIF(SUM(prescription_cost), 0) * 1000, 2) as payment_per_1000_rx
        FROM provider_risk
        GROUP BY state
        ORDER BY pct_high_risk DESC
        """
        
        logger.info("Calculating state risk profiles...")
        return self.client.query(query).to_dataframe()
    
    def get_state_top_drugs(self) -> pd.DataFrame:
        """Get top drugs prescribed by state"""
        query = f"""
        WITH provider_states AS (
            SELECT DISTINCT
                CAST(n.NPI AS STRING) as npi,
                p.HQ_STATE as state
            FROM `{self.project_id}.{self.temp_dataset}.commonspirit_provider_npis` n
            INNER JOIN `{self.project_id}.{self.dataset}.PHYSICIANS_OVERVIEW` p
                ON CAST(n.NPI AS STRING) = CAST(p.NPI AS STRING)
            WHERE p.HQ_STATE IS NOT NULL
        ),
        state_drugs AS (
            SELECT 
                ps.state,
                rx.drug_name,
                SUM(rx.total_drug_cost) as total_cost,
                SUM(rx.total_claim_count) as total_claims,
                COUNT(DISTINCT rx.npi) as prescriber_count
            FROM `{self.project_id}.{self.temp_dataset}.commonspirit_prescriptions_summary_2020_2024` rx
            INNER JOIN provider_states ps
                ON CAST(rx.npi AS STRING) = ps.npi
            GROUP BY ps.state, rx.drug_name
        ),
        ranked_drugs AS (
            SELECT 
                state,
                drug_name,
                total_cost,
                total_claims,
                prescriber_count,
                ROW_NUMBER() OVER (PARTITION BY state ORDER BY total_cost DESC) as rank
            FROM state_drugs
        )
        SELECT 
            state,
            drug_name,
            ROUND(total_cost, 2) as total_cost,
            total_claims,
            prescriber_count
        FROM ranked_drugs
        WHERE rank <= 5
        ORDER BY state, rank
        """
        
        logger.info("Getting top drugs by state...")
        return self.client.query(query).to_dataframe()
    
    def get_state_specialty_distribution(self) -> pd.DataFrame:
        """Get specialty distribution by state"""
        query = f"""
        WITH provider_states AS (
            SELECT 
                n.NPI,
                p.HQ_STATE as state,
                p.SPECIALTY_PRIMARY_GROUP,
                p.SPECIALTY_PRIMARY
            FROM `{self.project_id}.{self.temp_dataset}.commonspirit_provider_npis` n
            INNER JOIN `{self.project_id}.{self.dataset}.PHYSICIANS_OVERVIEW` p
                ON CAST(n.NPI AS STRING) = CAST(p.NPI AS STRING)
            WHERE p.HQ_STATE IS NOT NULL
                AND p.SPECIALTY_PRIMARY_GROUP IS NOT NULL
        )
        SELECT 
            state,
            SPECIALTY_PRIMARY_GROUP as specialty_group,
            COUNT(DISTINCT NPI) as provider_count,
            ROUND(COUNT(DISTINCT NPI) * 100.0 / SUM(COUNT(DISTINCT NPI)) OVER (PARTITION BY state), 2) as pct_of_state
        FROM provider_states
        GROUP BY state, SPECIALTY_PRIMARY_GROUP
        HAVING COUNT(DISTINCT NPI) >= 5
        ORDER BY state, provider_count DESC
        """
        
        logger.info("Analyzing specialty distribution by state...")
        return self.client.query(query).to_dataframe()
    
    def generate_insights(self, data: Dict[str, pd.DataFrame]) -> str:
        """Generate narrative insights from state analysis"""
        insights = []
        
        # State distribution insights
        if 'distribution' in data and not data['distribution'].empty:
            dist_df = data['distribution']
            top_states = dist_df.head(5)
            insights.append(f"## Geographic Provider Distribution\n")
            insights.append(f"CommonSpirit Health providers are distributed across {len(dist_df)} states, "
                           f"with the highest concentration in:\n")
            for _, row in top_states.iterrows():
                insights.append(f"- **{row['state']}**: {row['provider_count']:,} providers across {row['city_count']:,} cities")
        
        # Payment pattern insights
        if 'payments' in data and not data['payments'].empty:
            payment_df = data['payments']
            total_payments = payment_df['total_payments'].sum()
            insights.append(f"\n## State Payment Patterns\n")
            insights.append(f"Total industry payments of ${total_payments:,.0f} were distributed across states:\n")
            
            top_payment_states = payment_df.head(5)
            for _, row in top_payment_states.iterrows():
                pct_of_total = (row['total_payments'] / total_payments) * 100
                insights.append(f"- **{row['state']}**: ${row['total_payments']:,.0f} "
                              f"({pct_of_total:.1f}% of total) to {row['providers_with_payments']:,} providers")
        
        # Prescription pattern insights
        if 'prescriptions' in data and not data['prescriptions'].empty:
            rx_df = data['prescriptions']
            total_rx_cost = rx_df['total_prescription_cost'].sum()
            insights.append(f"\n## State Prescription Patterns\n")
            insights.append(f"Total prescription costs of ${total_rx_cost:,.0f} across states:\n")
            
            top_rx_states = rx_df.head(5)
            for _, row in top_rx_states.iterrows():
                insights.append(f"- **{row['state']}**: ${row['total_prescription_cost']:,.0f} "
                              f"by {row['prescribers']:,} prescribers")
        
        # Risk profile insights
        if 'risk' in data and not data['risk'].empty:
            risk_df = data['risk']
            high_risk_states = risk_df.nlargest(5, 'pct_high_risk')
            insights.append(f"\n## Geographic Risk Concentration\n")
            insights.append(f"States with highest concentration of high-risk providers:\n")
            for _, row in high_risk_states.iterrows():
                if pd.notna(row['pct_high_risk']):
                    insights.append(f"- **{row['state']}**: {row['pct_high_risk']:.1f}% high-risk "
                                  f"({row['high_risk_providers']} of {row['total_providers']} providers)")
        
        # Top drugs by state
        if 'top_drugs' in data and not data['top_drugs'].empty:
            drugs_df = data['top_drugs']
            insights.append(f"\n## Top Prescribed Drugs by State\n")
            
            # Get top 3 states by total drug cost
            state_totals = drugs_df.groupby('state')['total_cost'].sum().nlargest(3)
            for state in state_totals.index:
                state_drugs = drugs_df[drugs_df['state'] == state].head(3)
                insights.append(f"\n**{state}:**")
                for _, drug in state_drugs.iterrows():
                    insights.append(f"  - {drug['drug_name']}: ${drug['total_cost']:,.0f}")
        
        return "\n".join(insights)
    
    def run_analysis(self) -> Dict[str, Any]:
        """Run complete state-level analysis"""
        logger.info("Starting state-level analysis for CommonSpirit Health...")
        
        results = {}
        
        try:
            # Get all analysis data
            results['distribution'] = self.get_state_distribution()
            logger.info(f"Got distribution data for {len(results['distribution'])} states")
        except Exception as e:
            logger.error(f"Error getting distribution: {e}")
            results['distribution'] = pd.DataFrame()
        
        try:
            results['payments'] = self.get_state_payment_analysis()
            logger.info(f"Got payment data for {len(results['payments'])} states")
        except Exception as e:
            logger.error(f"Error getting payments: {e}")
            results['payments'] = pd.DataFrame()
        
        try:
            results['prescriptions'] = self.get_state_prescription_patterns()
            logger.info(f"Got prescription data for {len(results['prescriptions'])} states")
        except Exception as e:
            logger.error(f"Error getting prescriptions: {e}")
            results['prescriptions'] = pd.DataFrame()
        
        try:
            results['risk'] = self.get_state_risk_profiles()
            logger.info(f"Got risk profiles for {len(results['risk'])} states")
        except Exception as e:
            logger.error(f"Error getting risk profiles: {e}")
            results['risk'] = pd.DataFrame()
        
        try:
            results['top_drugs'] = self.get_state_top_drugs()
            logger.info(f"Got top drugs data")
        except Exception as e:
            logger.error(f"Error getting top drugs: {e}")
            results['top_drugs'] = pd.DataFrame()
        
        try:
            results['specialties'] = self.get_state_specialty_distribution()
            logger.info(f"Got specialty distribution data")
        except Exception as e:
            logger.error(f"Error getting specialties: {e}")
            results['specialties'] = pd.DataFrame()
        
        # Generate insights
        results['insights'] = self.generate_insights(results)
        
        logger.info("State analysis complete!")
        return results


def main():
    """Main execution function"""
    analyzer = StateAnalyzer()
    
    # Run analysis
    results = analyzer.run_analysis()
    
    # Save detailed results to CSV files
    output_dir = Path("/home/incent/conflixis-data-projects/projects/200-commonspirit-custom-report")
    
    for name, df in results.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            output_file = output_dir / f"state_{name}.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Saved {name} to {output_file}")
    
    # Generate markdown report
    report_path = output_dir / "state_analysis_insights.md"
    with open(report_path, 'w') as f:
        f.write("# CommonSpirit Health - State-Level Analysis\n\n")
        f.write(f"*Generated: {datetime.now().strftime('%B %d, %Y')}*\n\n")
        f.write("---\n\n")
        f.write(results['insights'])
        f.write("\n\n---\n\n")
        f.write("## Detailed State Metrics\n\n")
        
        # Add summary tables
        if not results['distribution'].empty:
            f.write("### Top 10 States by Provider Count\n\n")
            dist_df = results['distribution'].head(10)
            f.write(dist_df.to_markdown(index=False))
        
        if not results['payments'].empty:
            f.write("\n\n### Top 10 States by Total Payments\n\n")
            payment_df = results['payments'].head(10)
            payment_subset = payment_df[['state', 'providers_with_payments', 'total_payments', 'total_transactions']]
            f.write(payment_subset.to_markdown(index=False))
        
        if not results['risk'].empty:
            f.write("\n\n### States with Highest Risk Concentration\n\n")
            risk_df = results['risk'].head(10)
            if 'payment_per_1000_rx' in risk_df.columns:
                risk_subset = risk_df[['state', 'total_providers', 'high_risk_providers', 'pct_high_risk', 'payment_per_1000_rx']]
            else:
                risk_subset = risk_df[['state', 'total_providers', 'high_risk_providers', 'pct_high_risk']]
            f.write(risk_subset.to_markdown(index=False))
    
    logger.info(f"State analysis report saved to {report_path}")
    
    print(f"\n✅ State analysis complete!")
    print(f"📊 Results saved to: {output_dir}")
    print(f"📄 Report: {report_path}")


if __name__ == "__main__":
    main()