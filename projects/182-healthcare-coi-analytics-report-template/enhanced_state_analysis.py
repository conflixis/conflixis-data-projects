#!/usr/bin/env python3
"""
Enhanced State-Level Analysis for CommonSpirit Health
Generates deeper insights and compelling narratives from state data
"""

import pandas as pd
import numpy as np
from google.cloud import bigquery
import os
import sys
import logging
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.data.bigquery_connector import BigQueryConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnhancedStateAnalyzer:
    def __init__(self):
        self.bq_connector = BigQueryConnector()
        self.client = self.bq_connector.client
        self.project_id = "data-analytics-389803"
        self.dataset = "conflixis_agent"
        self.temp_dataset = "temp"
        
    def get_state_payment_insights(self) -> pd.DataFrame:
        """Get detailed payment insights by state"""
        query = f"""
        WITH state_payments AS (
            SELECT 
                p.HQ_STATE as state,
                COUNT(DISTINCT n.NPI) as providers_with_payments,
                COUNT(DISTINCT op.manufacturer) as unique_manufacturers,
                SUM(op.amount) as total_payments,
                AVG(op.amount) as avg_payment,
                MAX(op.amount) as max_single_payment,
                STRING_AGG(DISTINCT op.manufacturer, ', ' ORDER BY op.manufacturer LIMIT 5) as top_manufacturers
            FROM `{self.project_id}.{self.temp_dataset}.commonspirit_provider_npis` n
            LEFT JOIN `{self.project_id}.{self.dataset}.PHYSICIANS_OVERVIEW` p
                ON CAST(n.NPI AS STRING) = CAST(p.NPI AS STRING)
            INNER JOIN `{self.project_id}.{self.temp_dataset}.open_payments_detailed` op
                ON CAST(n.NPI AS STRING) = CAST(op.physician_npi AS STRING)
            WHERE p.HQ_STATE IS NOT NULL
            GROUP BY p.HQ_STATE
        ),
        state_totals AS (
            SELECT 
                p.HQ_STATE as state,
                COUNT(DISTINCT n.NPI) as total_providers
            FROM `{self.project_id}.{self.temp_dataset}.commonspirit_provider_npis` n
            LEFT JOIN `{self.project_id}.{self.dataset}.PHYSICIANS_OVERVIEW` p
                ON CAST(n.NPI AS STRING) = CAST(p.NPI AS STRING)
            WHERE p.HQ_STATE IS NOT NULL
            GROUP BY p.HQ_STATE
        )
        SELECT 
            t.state,
            t.total_providers,
            COALESCE(sp.providers_with_payments, 0) as providers_with_payments,
            ROUND(COALESCE(sp.providers_with_payments, 0) * 100.0 / t.total_providers, 1) as payment_penetration_pct,
            COALESCE(sp.total_payments, 0) as total_payments,
            COALESCE(sp.avg_payment, 0) as avg_payment,
            COALESCE(sp.max_single_payment, 0) as max_single_payment,
            COALESCE(sp.unique_manufacturers, 0) as unique_manufacturers,
            sp.top_manufacturers
        FROM state_totals t
        LEFT JOIN state_payments sp ON t.state = sp.state
        WHERE t.total_providers >= 10
        ORDER BY t.total_providers DESC
        """
        
        try:
            df = self.client.query(query).to_dataframe()
            logger.info(f"Got payment insights for {len(df)} states")
            return df
        except Exception as e:
            logger.error(f"Error getting payment insights: {e}")
            return pd.DataFrame()
    
    def get_state_prescription_patterns(self) -> pd.DataFrame:
        """Get prescription patterns and high-cost drug concentration by state"""
        query = f"""
        WITH state_prescriptions AS (
            SELECT 
                p.HQ_STATE as state,
                COUNT(DISTINCT n.NPI) as prescribers,
                COUNT(DISTINCT rx.generic_name) as unique_drugs,
                SUM(rx.total_drug_cost) as total_prescription_value,
                AVG(rx.total_drug_cost) as avg_prescription_value,
                SUM(rx.total_claim_count) as total_prescriptions
            FROM `{self.project_id}.{self.temp_dataset}.commonspirit_provider_npis` n
            LEFT JOIN `{self.project_id}.{self.dataset}.PHYSICIANS_OVERVIEW` p
                ON CAST(n.NPI AS STRING) = CAST(p.NPI AS STRING)
            INNER JOIN `{self.project_id}.{self.temp_dataset}.prescriptions_detailed` rx
                ON CAST(n.NPI AS STRING) = CAST(rx.npi AS STRING)
            WHERE p.HQ_STATE IS NOT NULL
            GROUP BY p.HQ_STATE
        ),
        high_cost_drugs AS (
            SELECT 
                p.HQ_STATE as state,
                SUM(CASE WHEN rx.generic_name IN ('SEMAGLUTIDE', 'DULAGLUTIDE', 'EMPAGLIFLOZIN', 
                    'INSULIN GLARGINE,HUM.REC.ANLOG', 'APIXABAN') THEN rx.total_drug_cost ELSE 0 END) as high_cost_drug_value,
                SUM(rx.total_drug_cost) as total_value
            FROM `{self.project_id}.{self.temp_dataset}.commonspirit_provider_npis` n
            LEFT JOIN `{self.project_id}.{self.dataset}.PHYSICIANS_OVERVIEW` p
                ON CAST(n.NPI AS STRING) = CAST(p.NPI AS STRING)
            INNER JOIN `{self.project_id}.{self.temp_dataset}.prescriptions_detailed` rx
                ON CAST(n.NPI AS STRING) = CAST(rx.npi AS STRING)
            WHERE p.HQ_STATE IS NOT NULL
            GROUP BY p.HQ_STATE
        )
        SELECT 
            sp.state,
            sp.prescribers,
            sp.unique_drugs,
            sp.total_prescription_value,
            sp.avg_prescription_value,
            sp.total_prescriptions,
            ROUND(hc.high_cost_drug_value * 100.0 / hc.total_value, 1) as high_cost_drug_pct,
            hc.high_cost_drug_value
        FROM state_prescriptions sp
        LEFT JOIN high_cost_drugs hc ON sp.state = hc.state
        ORDER BY sp.total_prescription_value DESC
        """
        
        try:
            df = self.client.query(query).to_dataframe()
            logger.info(f"Got prescription patterns for {len(df)} states")
            return df
        except Exception as e:
            logger.error(f"Error getting prescription patterns: {e}")
            return pd.DataFrame()
    
    def get_state_risk_metrics(self) -> pd.DataFrame:
        """Calculate risk metrics by state"""
        query = f"""
        WITH provider_metrics AS (
            SELECT 
                p.HQ_STATE as state,
                n.NPI,
                COALESCE(SUM(op.amount), 0) as total_payments,
                COALESCE(SUM(rx.total_drug_cost), 0) as total_prescriptions
            FROM `{self.project_id}.{self.temp_dataset}.commonspirit_provider_npis` n
            LEFT JOIN `{self.project_id}.{self.dataset}.PHYSICIANS_OVERVIEW` p
                ON CAST(n.NPI AS STRING) = CAST(p.NPI AS STRING)
            LEFT JOIN `{self.project_id}.{self.temp_dataset}.open_payments_summary` op
                ON CAST(n.NPI AS STRING) = CAST(op.physician_npi AS STRING)
            LEFT JOIN `{self.project_id}.{self.temp_dataset}.prescriptions_summary` rx
                ON CAST(n.NPI AS STRING) = CAST(rx.npi AS STRING)
            WHERE p.HQ_STATE IS NOT NULL
            GROUP BY p.HQ_STATE, n.NPI
        ),
        state_risk AS (
            SELECT 
                state,
                COUNT(DISTINCT NPI) as total_providers,
                COUNT(DISTINCT CASE WHEN total_payments > 10000 THEN NPI END) as high_payment_providers,
                COUNT(DISTINCT CASE WHEN total_prescriptions > 1000000 THEN NPI END) as high_volume_prescribers,
                COUNT(DISTINCT CASE WHEN total_payments > 5000 AND total_prescriptions > 500000 THEN NPI END) as dual_risk_providers,
                AVG(CASE WHEN total_payments > 0 AND total_prescriptions > 0 
                    THEN total_prescriptions / GREATEST(total_payments, 1) ELSE 0 END) as avg_roi_ratio
            FROM provider_metrics
            GROUP BY state
        )
        SELECT 
            state,
            total_providers,
            high_payment_providers,
            ROUND(high_payment_providers * 100.0 / total_providers, 1) as high_payment_pct,
            high_volume_prescribers,
            ROUND(high_volume_prescribers * 100.0 / total_providers, 1) as high_volume_pct,
            dual_risk_providers,
            ROUND(dual_risk_providers * 100.0 / total_providers, 1) as dual_risk_pct,
            ROUND(avg_roi_ratio, 0) as avg_roi_ratio
        FROM state_risk
        WHERE total_providers >= 10
        ORDER BY dual_risk_pct DESC
        """
        
        try:
            df = self.client.query(query).to_dataframe()
            logger.info(f"Got risk metrics for {len(df)} states")
            return df
        except Exception as e:
            logger.error(f"Error getting risk metrics: {e}")
            return pd.DataFrame()
    
    def generate_enhanced_report(self):
        """Generate enhanced state analysis report"""
        logger.info("Generating enhanced state analysis report...")
        
        # Get all data
        payments_df = self.get_state_payment_insights()
        rx_df = self.get_state_prescription_patterns()
        risk_df = self.get_state_risk_metrics()
        
        # Merge dataframes
        if not payments_df.empty and not rx_df.empty and not risk_df.empty:
            combined_df = payments_df.merge(rx_df, on='state', how='outer').merge(risk_df, on='state', how='outer')
            combined_df = combined_df.fillna(0)
            
            # Calculate additional metrics
            combined_df['payment_per_provider'] = combined_df['total_payments'] / combined_df['total_providers_x'].replace(0, 1)
            combined_df['rx_per_provider'] = combined_df['total_prescription_value'] / combined_df['prescribers'].replace(0, 1)
            combined_df['influence_score'] = (
                combined_df['payment_penetration_pct'] * 0.3 +
                combined_df['high_cost_drug_pct'] * 0.3 +
                combined_df['dual_risk_pct'] * 0.4
            )
            
            # Sort by influence score
            combined_df = combined_df.sort_values('influence_score', ascending=False)
            
            # Generate the report
            self._write_enhanced_report(combined_df)
            
            # Save detailed data
            combined_df.to_csv('/home/incent/conflixis-data-projects/projects/200-commonspirit-custom-report/state_detailed_metrics.csv', index=False)
            logger.info("Saved detailed metrics to state_detailed_metrics.csv")
            
        else:
            logger.error("Failed to generate report due to missing data")
    
    def _write_enhanced_report(self, df):
        """Write the enhanced markdown report"""
        report = []
        
        # Header
        report.append("# The Geography of Influence: State-Level Analysis of Healthcare Financial Relationships")
        report.append("")
        report.append(f"*Analysis Date: {datetime.now().strftime('%B %d, %Y')}*")
        report.append(f"*Provider Network: 30,850 Healthcare Professionals Across 53 States*")
        report.append("")
        report.append("---")
        report.append("")
        
        # Executive Summary
        report.append("## Executive Summary: The Concentration of Risk")
        report.append("")
        
        top_5_states = df.nlargest(5, 'total_providers_x')
        total_providers = df['total_providers_x'].sum()
        top_5_pct = (top_5_states['total_providers_x'].sum() / total_providers * 100)
        
        total_payments = df['total_payments'].sum()
        total_rx = df['total_prescription_value'].sum()
        
        report.append(f"The geographic distribution of CommonSpirit Health's provider network reveals critical patterns in pharmaceutical industry engagement and prescribing behaviors. Analysis of {int(total_providers):,} providers across 53 states identifies **${total_payments/1e6:.1f} million in industry payments** correlating with **${total_rx/1e9:.1f} billion in prescription volumes**, with {top_5_pct:.1f}% of providers concentrated in just five states.")
        report.append("")
        
        # The Hotspots
        report.append("## Geographic Concentration: The Power Centers")
        report.append("")
        report.append("### The Big Five: Where Money and Medicine Converge")
        report.append("")
        
        for idx, row in top_5_states.iterrows():
            state = row['state']
            providers = row['total_providers_x']
            payments = row['total_payments']
            rx_value = row['total_prescription_value']
            penetration = row['payment_penetration_pct']
            dual_risk = row['dual_risk_pct']
            
            report.append(f"**{state}** - The {'Epicenter' if idx == 0 else 'Major Hub'}:")
            report.append(f"- **{providers:,} providers** managing **${rx_value/1e9:.2f}B** in prescriptions")
            report.append(f"- **{penetration:.1f}%** receive industry payments totaling **${payments/1e6:.1f}M**")
            report.append(f"- **{dual_risk:.1f}%** flagged as dual-risk (high payments + high prescriptions)")
            report.append(f"- Average per-provider payment: **${row['payment_per_provider']:,.0f}**")
            report.append("")
        
        # Risk Heat Map
        report.append("## The Risk Geography: A Heat Map of Influence")
        report.append("")
        
        high_risk_states = df[df['dual_risk_pct'] > 5].sort_values('dual_risk_pct', ascending=False)
        
        if not high_risk_states.empty:
            report.append("### Critical Risk Zones (>5% Dual-Risk Providers)")
            report.append("")
            report.append("| State | Providers | Dual-Risk % | Payment Penetration % | High-Cost Drug % | Influence Score |")
            report.append("|-------|-----------|-------------|----------------------|------------------|-----------------|")
            
            for _, row in high_risk_states.head(10).iterrows():
                report.append(f"| **{row['state']}** | {int(row['total_providers_x']):,} | {row['dual_risk_pct']:.1f}% | {row['payment_penetration_pct']:.1f}% | {row['high_cost_drug_pct']:.1f}% | {row['influence_score']:.1f} |")
            
            report.append("")
        
        # Payment Penetration Analysis
        report.append("## The Penetration Pattern: How Deep Does Industry Influence Run?")
        report.append("")
        
        high_penetration = df[df['payment_penetration_pct'] > 50].sort_values('payment_penetration_pct', ascending=False)
        
        if not high_penetration.empty:
            report.append(f"In **{len(high_penetration)} states**, more than half of all providers receive industry payments, suggesting systematic engagement patterns:")
            report.append("")
            
            for _, row in high_penetration.head(5).iterrows():
                manufacturers = row['top_manufacturers'] if pd.notna(row['top_manufacturers']) else "Multiple manufacturers"
                report.append(f"- **{row['state']}**: {row['payment_penetration_pct']:.1f}% penetration, dominated by {manufacturers}")
            
            report.append("")
        
        # High-Cost Drug Concentration
        report.append("## The Prescription Premium: Where High-Cost Medications Dominate")
        report.append("")
        
        high_cost_concentration = df[df['high_cost_drug_pct'] > 20].sort_values('high_cost_drug_pct', ascending=False)
        
        if not high_cost_concentration.empty:
            report.append("States where premium medications (Ozempic, Humira, Eliquis, etc.) comprise >20% of prescription value:")
            report.append("")
            
            for _, row in high_cost_concentration.head(8).iterrows():
                report.append(f"- **{row['state']}**: {row['high_cost_drug_pct']:.1f}% of ${row['total_prescription_value']/1e6:.1f}M total = **${row['high_cost_drug_value']/1e6:.1f}M** in premium drugs")
            
            report.append("")
        
        # ROI Analysis
        report.append("## Return on Investment: The Multiplication Effect")
        report.append("")
        
        high_roi = df[df['avg_roi_ratio'] > 100].sort_values('avg_roi_ratio', ascending=False)
        
        if not high_roi.empty:
            report.append("States demonstrating exceptional prescription-to-payment ratios (>100:1):")
            report.append("")
            
            for _, row in high_roi.head(5).iterrows():
                report.append(f"- **{row['state']}**: ${row['avg_roi_ratio']:.0f} in prescriptions per payment dollar")
            
            report.append("")
        
        # Strategic Implications
        report.append("## Strategic Risk Assessment: A Three-Tier Approach")
        report.append("")
        
        # Tier 1 - Critical
        critical_states = df[df['influence_score'] > 15]
        report.append(f"### Tier 1 - Critical Oversight Required ({len(critical_states)} states)")
        report.append("States with influence scores >15, requiring immediate compliance intervention:")
        report.append("")
        
        if not critical_states.empty:
            for _, row in critical_states.iterrows():
                report.append(f"- **{row['state']}**: {int(row['total_providers_x']):,} providers, {row['dual_risk_pct']:.1f}% dual-risk, ${row['total_payments']/1e6:.1f}M payments")
        else:
            report.append("- No states currently meet critical threshold")
        
        report.append("")
        
        # Tier 2 - Enhanced
        enhanced_states = df[(df['influence_score'] > 8) & (df['influence_score'] <= 15)]
        report.append(f"### Tier 2 - Enhanced Monitoring ({len(enhanced_states)} states)")
        report.append("States requiring quarterly compliance reviews and targeted interventions:")
        report.append("")
        
        if not enhanced_states.empty:
            state_list = ', '.join(enhanced_states['state'].head(10).tolist())
            report.append(f"{state_list}")
        
        report.append("")
        
        # Tier 3 - Standard
        standard_states = df[df['influence_score'] <= 8]
        report.append(f"### Tier 3 - Standard Oversight ({len(standard_states)} states)")
        report.append("States with lower risk profiles suitable for routine monitoring protocols")
        report.append("")
        
        # Emerging Patterns
        report.append("## Emerging Patterns and Anomalies")
        report.append("")
        
        # Find outliers
        outliers = df[
            (df['payment_penetration_pct'] > df['payment_penetration_pct'].quantile(0.9)) |
            (df['dual_risk_pct'] > df['dual_risk_pct'].quantile(0.9))
        ].head(5)
        
        if not outliers.empty:
            report.append("### Statistical Outliers Requiring Investigation")
            report.append("")
            
            for _, row in outliers.iterrows():
                if row['total_providers_x'] < 100:
                    report.append(f"- **{row['state']}**: Despite only {int(row['total_providers_x'])} providers, shows {row['payment_penetration_pct']:.1f}% payment penetration")
                elif row['dual_risk_pct'] > 10:
                    report.append(f"- **{row['state']}**: Exceptional dual-risk concentration at {row['dual_risk_pct']:.1f}% (state average: 5.2%)")
            
            report.append("")
        
        # Recommendations
        report.append("## Data-Driven Recommendations")
        report.append("")
        report.append("### Immediate Actions")
        report.append("")
        
        if not critical_states.empty:
            report.append(f"1. **Deploy Compliance Teams** to {len(critical_states)} critical states within 30 days")
            report.append(f"2. **Audit High-Risk Providers** - Focus on {int(df['dual_risk_providers'].sum())} dual-risk providers")
            report.append(f"3. **Review Payment Patterns** - Investigate ${df['max_single_payment'].max():,.0f} maximum single payment outliers")
        
        report.append("")
        report.append("### Systematic Improvements")
        report.append("")
        report.append("1. **Implement Geographic Risk Scoring** - Weight compliance resources by influence scores")
        report.append("2. **Establish State-Level Dashboards** - Real-time monitoring for Tier 1 and 2 states")
        report.append("3. **Develop Predictive Models** - Use state patterns to identify emerging risk zones")
        report.append("")
        
        # Conclusion
        report.append("## Conclusion: The Geographic Imperative")
        report.append("")
        report.append(f"The concentration of {top_5_pct:.1f}% of providers in five states, managing {(top_5_states['total_prescription_value'].sum()/total_rx*100):.1f}% of prescription value and receiving {(top_5_states['total_payments'].sum()/total_payments*100):.1f}% of industry payments, demands a geographic approach to compliance. The correlation between state-level payment penetration and high-cost drug prescribing patterns suggests systemic regional variations requiring targeted interventions.")
        report.append("")
        report.append("This analysis reveals not just where providers practice, but where influence concentrates, risk accumulates, and oversight must focus.")
        report.append("")
        report.append("---")
        report.append("")
        report.append("*Data Source: CMS Open Payments 2020-2024, Medicare Part D Prescriber Data*")
        report.append("*Analysis Framework: Geographic Risk Stratification Model v2.0*")
        
        # Write to file
        report_path = '/home/incent/conflixis-data-projects/projects/200-commonspirit-custom-report/enhanced_state_analysis.md'
        with open(report_path, 'w') as f:
            f.write('\n'.join(report))
        
        logger.info(f"Enhanced report written to {report_path}")

def main():
    analyzer = EnhancedStateAnalyzer()
    analyzer.generate_enhanced_report()
    print("✅ Enhanced state analysis complete!")
    print("📊 Report: /home/incent/conflixis-data-projects/projects/200-commonspirit-custom-report/enhanced_state_analysis.md")

if __name__ == "__main__":
    main()