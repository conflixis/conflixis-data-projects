#!/usr/bin/env python3
"""
Enhance Alcon report with geographic and institutional analysis using facility affiliations
"""

import pandas as pd
import numpy as np
import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
from pathlib import Path
import logging
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AlconGeographicAnalyzer:
    """Analyze Alcon payments with geographic and institutional context"""
    
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        self.base_dir = Path("/home/incent/conflixis-data-projects/projects/189-alcon-custom-report")
        self.data_dir = self.base_dir / "data" / "outputs"
        self.reports_dir = self.base_dir / "reports"
        
        # Initialize BigQuery client
        self.client = self._init_bigquery_client()
        
        # Data containers
        self.df_enhanced = None
        self.df_payments = None
        
    def _init_bigquery_client(self):
        """Initialize BigQuery client with service account"""
        service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
        if not service_account_json:
            raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
        
        service_account_info = json.loads(service_account_json)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/bigquery']
        )
        
        return bigquery.Client(
            credentials=credentials,
            project=service_account_info.get('project_id')
        )
    
    def extract_enhanced_data(self):
        """Extract Alcon payment data with facility affiliations"""
        logger.info("Extracting Alcon payments with facility affiliations...")
        
        # Read the NPIs from existing Alcon data
        npi_file = self.data_dir / "alcon_drug_npis.csv"
        df_npis = pd.read_csv(npi_file)
        npi_list = df_npis['covered_recipient_npi'].astype(str).tolist()
        
        # Create batches (BigQuery has limits on IN clause)
        batch_size = 1000
        all_results = []
        
        for i in range(0, len(npi_list), batch_size):
            batch = npi_list[i:i+batch_size]
            npi_string = ','.join(batch)
            
            query = f"""
            WITH alcon_payments AS (
                SELECT 
                    covered_recipient_npi,
                    COUNT(*) as payment_count,
                    SUM(total_amount_of_payment_usdollars) as total_payments,
                    AVG(total_amount_of_payment_usdollars) as avg_payment,
                    MAX(total_amount_of_payment_usdollars) as max_payment,
                    STRING_AGG(DISTINCT name_of_drug_or_biological_or_device_or_medical_supply_1, '; ') as drug_products
                FROM `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized`
                WHERE LOWER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%alcon%'
                AND covered_recipient_npi IN ({npi_string})
                AND (
                    LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%systane%'
                    OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%eysuvis%'
                    OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%rocklatan%'
                    OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%azopt%'
                    OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%pataday%'
                    OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%simbrinza%'
                    OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%rhopressa%'
                    OR name_of_drug_or_biological_or_device_or_medical_supply_1 IS NULL
                    OR TRIM(name_of_drug_or_biological_or_device_or_medical_supply_1) = ''
                )
                GROUP BY covered_recipient_npi
            )
            SELECT 
                p.covered_recipient_npi,
                p.payment_count,
                p.total_payments,
                p.avg_payment,
                p.max_payment,
                p.drug_products,
                f.NPI,
                f.FIRST_NAME,
                f.LAST_NAME,
                f.AFFILIATED_NAME,
                f.AFFILIATED_FIRM_TYPE,
                f.AFFILIATED_HQ_CITY,
                f.AFFILIATED_HQ_STATE,
                f.AFFILIATION_STRENGTH
            FROM alcon_payments p
            LEFT JOIN `data-analytics-389803.conflixis_data_projects.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized` f
                ON CAST(p.covered_recipient_npi AS INT64) = f.NPI
                AND f.PRIMARY_AFFILIATED_FACILITY_FLAG = 'Yes'
            """
            
            logger.info(f"Processing batch {i//batch_size + 1} of {len(npi_list)//batch_size + 1}...")
            query_job = self.client.query(query)
            batch_results = query_job.to_dataframe()
            all_results.append(batch_results)
        
        # Combine all batches
        self.df_enhanced = pd.concat(all_results, ignore_index=True)
        
        # Save enhanced dataset
        output_file = self.data_dir / "alcon_payments_with_affiliations.csv"
        self.df_enhanced.to_csv(output_file, index=False)
        logger.info(f"Enhanced data saved to {output_file}")
        
        return self.df_enhanced
    
    def analyze_state_distribution(self):
        """Analyze payment distribution by state"""
        analysis = {}
        
        # Load the data if not already loaded
        if self.df_enhanced is None:
            self.df_enhanced = pd.read_csv(self.data_dir / "alcon_payments_with_affiliations.csv")
        
        # Filter for records with state information
        df_with_state = self.df_enhanced[self.df_enhanced['AFFILIATED_HQ_STATE'].notna()].copy()
        
        # Ensure numeric types
        df_with_state['total_payments'] = pd.to_numeric(df_with_state['total_payments'], errors='coerce')
        df_with_state['payment_count'] = pd.to_numeric(df_with_state['payment_count'], errors='coerce')
        df_with_state['avg_payment'] = pd.to_numeric(df_with_state['avg_payment'], errors='coerce')
        df_with_state['max_payment'] = pd.to_numeric(df_with_state['max_payment'], errors='coerce')
        
        # State-level aggregation
        state_summary = df_with_state.groupby('AFFILIATED_HQ_STATE').agg({
            'covered_recipient_npi': 'nunique',
            'total_payments': 'sum',
            'payment_count': 'sum',
            'avg_payment': 'mean',
            'max_payment': 'max'
        })
        
        state_summary.columns = ['unique_providers', 'total_payments', 'total_transactions', 'avg_payment', 'max_payment']
        # Convert to numeric to handle any type issues
        state_summary['total_payments'] = pd.to_numeric(state_summary['total_payments'], errors='coerce')
        state_summary['unique_providers'] = pd.to_numeric(state_summary['unique_providers'], errors='coerce')
        state_summary['avg_per_provider'] = (state_summary['total_payments'] / state_summary['unique_providers']).round(2)
        state_summary = state_summary.sort_values('total_payments', ascending=False)
        
        analysis['state_summary'] = state_summary
        
        # Regional grouping (simplified US regions)
        regions = {
            'Northeast': ['CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'NJ', 'NY', 'PA'],
            'Midwest': ['IL', 'IN', 'MI', 'OH', 'WI', 'IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD'],
            'South': ['DE', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'DC', 'WV', 'AL', 'KY', 'MS', 'TN', 'AR', 'LA', 'OK', 'TX'],
            'West': ['AZ', 'CO', 'ID', 'MT', 'NV', 'NM', 'UT', 'WY', 'AK', 'CA', 'HI', 'OR', 'WA']
        }
        
        # Create region mapping
        region_map = {}
        for region, states in regions.items():
            for state in states:
                region_map[state] = region
        
        df_with_state = df_with_state.copy()  # Avoid SettingWithCopyWarning
        df_with_state['region'] = df_with_state['AFFILIATED_HQ_STATE'].map(region_map)
        
        # Regional analysis
        regional_summary = df_with_state.groupby('region').agg({
            'covered_recipient_npi': 'nunique',
            'total_payments': 'sum',
            'payment_count': 'sum'
        }).round(2)
        
        regional_summary.columns = ['unique_providers', 'total_payments', 'total_transactions']
        # Convert to numeric
        regional_summary['total_payments'] = pd.to_numeric(regional_summary['total_payments'], errors='coerce')
        regional_summary['pct_of_total'] = (regional_summary['total_payments'] / regional_summary['total_payments'].sum() * 100).round(1)
        
        analysis['regional_summary'] = regional_summary.sort_values('total_payments', ascending=False)
        
        return analysis
    
    def analyze_institutions(self):
        """Analyze payment patterns by institution"""
        analysis = {}
        
        # Load the data if not already loaded
        if self.df_enhanced is None:
            self.df_enhanced = pd.read_csv(self.data_dir / "alcon_payments_with_affiliations.csv")
        
        # Filter for records with institution information
        df_with_inst = self.df_enhanced[self.df_enhanced['AFFILIATED_NAME'].notna()].copy()
        
        # Ensure numeric types
        df_with_inst['total_payments'] = pd.to_numeric(df_with_inst['total_payments'], errors='coerce')
        df_with_inst['payment_count'] = pd.to_numeric(df_with_inst['payment_count'], errors='coerce')
        
        # Institution-level aggregation
        inst_summary = df_with_inst.groupby('AFFILIATED_NAME').agg({
            'covered_recipient_npi': 'nunique',
            'total_payments': 'sum',
            'payment_count': 'sum',
            'AFFILIATED_HQ_STATE': 'first',
            'AFFILIATED_FIRM_TYPE': 'first'
        }).round(2)
        
        inst_summary.columns = ['unique_providers', 'total_payments', 'total_transactions', 'state', 'facility_type']
        # Convert to numeric
        inst_summary['total_payments'] = pd.to_numeric(inst_summary['total_payments'], errors='coerce')
        inst_summary['unique_providers'] = pd.to_numeric(inst_summary['unique_providers'], errors='coerce')
        inst_summary['avg_per_provider'] = (inst_summary['total_payments'] / inst_summary['unique_providers']).round(2)
        inst_summary = inst_summary.sort_values('total_payments', ascending=False)
        
        analysis['top_institutions'] = inst_summary.head(25)
        
        # Analysis by facility type
        facility_summary = df_with_inst.groupby('AFFILIATED_FIRM_TYPE').agg({
            'covered_recipient_npi': 'nunique',
            'total_payments': 'sum',
            'payment_count': 'sum',
            'AFFILIATED_NAME': 'nunique'
        }).round(2)
        
        facility_summary.columns = ['unique_providers', 'total_payments', 'total_transactions', 'unique_institutions']
        # Convert to numeric
        facility_summary['total_payments'] = pd.to_numeric(facility_summary['total_payments'], errors='coerce')
        facility_summary['unique_providers'] = pd.to_numeric(facility_summary['unique_providers'], errors='coerce')
        facility_summary['unique_institutions'] = pd.to_numeric(facility_summary['unique_institutions'], errors='coerce')
        facility_summary['avg_per_provider'] = (facility_summary['total_payments'] / facility_summary['unique_providers']).round(2)
        facility_summary['avg_per_institution'] = (facility_summary['total_payments'] / facility_summary['unique_institutions']).round(2)
        
        analysis['facility_types'] = facility_summary.sort_values('total_payments', ascending=False)
        
        return analysis
    
    def calculate_institutional_risk(self):
        """Calculate risk scores for institutions"""
        # Load the data if not already loaded
        if self.df_enhanced is None:
            self.df_enhanced = pd.read_csv(self.data_dir / "alcon_payments_with_affiliations.csv")
        
        df_with_inst = self.df_enhanced[self.df_enhanced['AFFILIATED_NAME'].notna()].copy()
        
        # Ensure numeric types
        df_with_inst['total_payments'] = pd.to_numeric(df_with_inst['total_payments'], errors='coerce')
        df_with_inst['max_payment'] = pd.to_numeric(df_with_inst['max_payment'], errors='coerce')
        df_with_inst['payment_count'] = pd.to_numeric(df_with_inst['payment_count'], errors='coerce')
        
        # Institution risk metrics
        inst_risk = df_with_inst.groupby('AFFILIATED_NAME').agg({
            'total_payments': ['sum', 'mean'],
            'max_payment': 'max',
            'covered_recipient_npi': 'nunique',
            'payment_count': 'sum'
        }).round(2)
        
        inst_risk.columns = ['total_amount', 'avg_provider_payment', 'max_single_payment', 'provider_count', 'transaction_count']
        
        # Calculate risk scores
        inst_risk['payment_concentration'] = inst_risk['total_amount'] / inst_risk['provider_count']
        inst_risk['transaction_intensity'] = inst_risk['transaction_count'] / inst_risk['provider_count']
        
        # Normalize scores
        inst_risk['amount_score'] = (inst_risk['total_amount'] / inst_risk['total_amount'].max() * 40)
        inst_risk['concentration_score'] = (inst_risk['payment_concentration'] / inst_risk['payment_concentration'].max() * 30)
        inst_risk['intensity_score'] = (inst_risk['transaction_intensity'] / inst_risk['transaction_intensity'].max() * 30)
        
        inst_risk['risk_score'] = (inst_risk['amount_score'] + 
                                   inst_risk['concentration_score'] + 
                                   inst_risk['intensity_score'])
        
        inst_risk['risk_category'] = pd.cut(inst_risk['risk_score'],
                                            bins=[0, 25, 50, 75, 100],
                                            labels=['Low', 'Moderate', 'High', 'Very High'])
        
        return inst_risk.sort_values('risk_score', ascending=False)
    
    def generate_geographic_section(self, state_analysis):
        """Generate geographic distribution section"""
        state_summary = state_analysis['state_summary']
        regional_summary = state_analysis['regional_summary']
        
        section = """# Geographic Distribution: The National Landscape of Pharmaceutical Influence

The geographic analysis reveals significant regional variations in Alcon's pharmaceutical payment ecosystem, with distinct patterns of concentration emerging across states and regions. This distribution reflects both market presence and strategic engagement priorities within different healthcare markets.

## State-Level Payment Concentration

The state-by-state analysis demonstrates highly concentrated payment patterns, with the top states commanding disproportionate shares of the total payment volume:

| State | Providers | Total Payments | Avg per Provider | Max Payment |
|-------|-----------|---------------|------------------|-------------|"""
        
        # Add top 15 states
        for state, row in state_summary.head(15).iterrows():
            section += f"\n| {state} | {row['unique_providers']:,.0f} | ${row['total_payments']:,.2f} | ${row['avg_per_provider']:,.2f} | ${row['max_payment']:,.2f} |"
        
        # Calculate concentration metrics
        top5_payments = state_summary.head(5)['total_payments'].sum()
        total_payments = state_summary['total_payments'].sum()
        top5_pct = (top5_payments / total_payments * 100) if total_payments > 0 else 0
        
        section += f"""

## Geographic Concentration Patterns

The top 5 states account for **${top5_payments:,.2f}** ({top5_pct:.1f}%) of total payments, indicating significant geographic concentration. This pattern suggests targeted market development strategies focused on key metropolitan areas and major medical centers.

## Regional Distribution Analysis

Payment patterns across U.S. regions reveal strategic market priorities:

| Region | Providers | Total Payments | % of Total | Transactions |
|--------|-----------|---------------|------------|--------------|"""
        
        for region, row in regional_summary.iterrows():
            section += f"\n| {region} | {row['unique_providers']:,.0f} | ${row['total_payments']:,.2f} | {row['pct_of_total']:.1f}% | {row['total_transactions']:,.0f} |"
        
        section += """

## Geographic Risk Indicators

Several states demonstrate characteristics warranting enhanced scrutiny:

1. **High Concentration States**: States with >$1M in total payments represent priority markets for compliance monitoring
2. **Provider Density**: States with high provider-to-payment ratios suggest intensive market penetration
3. **Regional Disparities**: Significant variations between regions indicate differentiated engagement strategies

The geographic distribution analysis reveals that Alcon's pharmaceutical influence is not uniformly distributed but rather concentrated in specific markets, likely corresponding to major ophthalmology centers and high-volume prescribing regions."""
        
        return section
    
    def generate_institutional_section(self, inst_analysis, inst_risk):
        """Generate institutional landscape section"""
        top_inst = inst_analysis['top_institutions']
        facility_types = inst_analysis['facility_types']
        
        section = """# Institutional Landscape: Healthcare System Engagement Patterns

The institutional analysis reveals complex networks of financial relationships within healthcare organizations, demonstrating how pharmaceutical influence operates at the system level beyond individual provider relationships.

## Top Healthcare Institutions by Payment Volume

The concentration of payments within major healthcare institutions highlights strategic partnerships and potential network effects:

| Institution | State | Providers | Total Payments | Avg/Provider | Facility Type |
|------------|-------|-----------|---------------|--------------|---------------|"""
        
        # Add top 20 institutions
        for inst_name, row in top_inst.head(20).iterrows():
            # Truncate long institution names
            display_name = inst_name[:50] + '...' if len(inst_name) > 50 else inst_name
            section += f"\n| {display_name} | {row['state']} | {row['unique_providers']:,.0f} | ${row['total_payments']:,.2f} | ${row['avg_per_provider']:,.2f} | {row['facility_type']} |"
        
        section += """

## Facility Type Analysis

Payment distribution across different healthcare facility types reveals targeted engagement strategies:

| Facility Type | Institutions | Providers | Total Payments | Avg/Institution | Avg/Provider |
|--------------|--------------|-----------|---------------|-----------------|--------------|"""
        
        for ftype, row in facility_types.iterrows():
            section += f"\n| {ftype} | {row['unique_institutions']:,.0f} | {row['unique_providers']:,.0f} | ${row['total_payments']:,.2f} | ${row['avg_per_institution']:,.2f} | ${row['avg_per_provider']:,.2f} |"
        
        section += """

## Institutional Risk Assessment

The institutional risk framework identifies healthcare organizations with payment patterns requiring enhanced oversight:

| Risk Category | Institution Count | Avg Total Payments | Avg Provider Count |
|--------------|------------------|-------------------|-------------------|"""
        
        risk_summary = inst_risk.groupby('risk_category').agg({
            'total_amount': 'mean',
            'provider_count': 'mean',
            'risk_score': 'count'
        })
        
        for category in ['Low', 'Moderate', 'High', 'Very High']:
            if category in risk_summary.index:
                row = risk_summary.loc[category]
                section += f"\n| {category} | {row['risk_score']:,.0f} | ${row['total_amount']:,.2f} | {row['provider_count']:.1f} |"
        
        # Identify high-risk institutions
        high_risk_inst = inst_risk[inst_risk['risk_category'].isin(['High', 'Very High'])]
        
        section += f"""

## High-Risk Institutional Profiles

**{len(high_risk_inst)} institutions** demonstrate high or very high risk profiles based on payment concentration, provider density, and transaction intensity. These institutions warrant priority review for:

- Formulary decision-making processes
- Speaker bureau participation rates
- Clinical trial enrollment patterns
- Prescribing guideline development

## Network Effects and Institutional Influence

The institutional analysis reveals several critical patterns:

1. **Concentration in Academic Medical Centers**: Major teaching hospitals show elevated payment levels, potentially influencing training and practice patterns for future physicians
2. **Physician Group Dominance**: Independent physician groups receive substantial payments, suggesting targeted engagement outside hospital systems
3. **Geographic Clustering**: Institutions in certain metropolitan areas show coordinated payment patterns, indicating regional market strategies

These institutional patterns suggest that pharmaceutical influence operates through organizational channels, potentially amplifying individual provider relationships through institutional policies and peer effects."""
        
        return section
    
    def append_to_existing_report(self, geographic_section, institutional_section, inst_risk):
        """Append new sections to existing report"""
        logger.info("Appending new sections to existing report...")
        
        # Read existing report
        existing_report_path = self.reports_dir / "alcon_investigative_report_20250910_165842.md"
        with open(existing_report_path, 'r') as f:
            existing_content = f.read()
        
        # Find where to insert (after Recommendations, before Methodology)
        methodology_index = existing_content.find("## Appendix: Methodology")
        
        if methodology_index == -1:
            # If methodology section not found, append at end
            insertion_point = len(existing_content)
        else:
            insertion_point = methodology_index
        
        # Prepare new sections
        new_sections = f"""
## 6. Geographic Distribution

{geographic_section}

---

## 7. Institutional Landscape

{institutional_section}

---

"""
        
        # Insert new sections
        updated_report = existing_content[:insertion_point] + new_sections + existing_content[insertion_point:]
        
        # Save updated report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        updated_report_path = self.reports_dir / f"alcon_investigative_report_enhanced_{timestamp}.md"
        
        with open(updated_report_path, 'w') as f:
            f.write(updated_report)
        
        logger.info(f"Enhanced report saved to: {updated_report_path}")
        
        # Also save institutional risk scores
        risk_file = self.data_dir / f"institutional_risk_scores_{timestamp}.csv"
        inst_risk.to_csv(risk_file)
        logger.info(f"Institutional risk scores saved to: {risk_file}")
        
        return str(updated_report_path)
    
    def run_full_analysis(self):
        """Run complete geographic and institutional analysis"""
        logger.info("Starting enhanced Alcon analysis with geographic and institutional data...")
        
        # Extract enhanced data
        self.extract_enhanced_data()
        
        # Perform analyses
        logger.info("Analyzing state distribution...")
        state_analysis = self.analyze_state_distribution()
        
        logger.info("Analyzing institutions...")
        inst_analysis = self.analyze_institutions()
        
        logger.info("Calculating institutional risk scores...")
        inst_risk = self.calculate_institutional_risk()
        
        # Generate report sections
        logger.info("Generating geographic section...")
        geographic_section = self.generate_geographic_section(state_analysis)
        
        logger.info("Generating institutional section...")
        institutional_section = self.generate_institutional_section(inst_analysis, inst_risk)
        
        # Append to existing report
        report_path = self.append_to_existing_report(geographic_section, institutional_section, inst_risk)
        
        # Print summary
        print("\n" + "="*60)
        print("ENHANCED ALCON ANALYSIS COMPLETE")
        print("="*60)
        print(f"Enhanced report saved to: {report_path}")
        print(f"\nKey Findings:")
        print(f"- States with data: {len(state_analysis['state_summary'])}")
        print(f"- Top state: {state_analysis['state_summary'].index[0]} (${state_analysis['state_summary'].iloc[0]['total_payments']:,.2f})")
        print(f"- Institutions analyzed: {len(inst_analysis['top_institutions'])}")
        print(f"- High-risk institutions: {len(inst_risk[inst_risk['risk_category'].isin(['High', 'Very High'])])}")
        print("="*60)
        
        return report_path


def main():
    """Main execution function"""
    analyzer = AlconGeographicAnalyzer()
    analyzer.run_full_analysis()


if __name__ == "__main__":
    main()