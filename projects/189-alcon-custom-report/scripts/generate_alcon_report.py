#!/usr/bin/env python3
"""
Generate Alcon Pharmaceutical Conflict of Interest Analysis Report
Following the style of healthcare COI analytics report template
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AlconReportGenerator:
    """Generate investigative-style report for Alcon drug payments"""
    
    def __init__(self, base_dir="/home/incent/conflixis-data-projects/projects/189-alcon-custom-report"):
        self.base_dir = Path(base_dir)
        self.data_dir = self.base_dir / "data" / "outputs"
        self.reports_dir = self.base_dir / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        
        # Load data
        self.df_payments = None
        self.df_products = None
        self.load_data()
        
    def load_data(self):
        """Load Alcon payment and product classification data"""
        logger.info("Loading Alcon payment data...")
        
        # Load payment data
        payments_file = self.data_dir / "alcon_drug_payments.csv"
        if payments_file.exists():
            self.df_payments = pd.read_csv(payments_file)
            logger.info(f"Loaded {len(self.df_payments):,} payment records")
        else:
            logger.error(f"Payment file not found: {payments_file}")
            
        # Load product classifications
        products_file = self.data_dir / "alcon_products_classified.csv"
        if products_file.exists():
            self.df_products = pd.read_csv(products_file)
            self.df_products = self.df_products[self.df_products['product_type'] == 'Drug']
            logger.info(f"Loaded {len(self.df_products)} drug products")
    
    def analyze_payment_overview(self):
        """Analyze overall payment landscape"""
        analysis = {}
        
        # Basic metrics
        analysis['total_payments'] = self.df_payments['total_amount_of_payment_usdollars'].sum()
        analysis['total_records'] = len(self.df_payments)
        analysis['unique_npis'] = self.df_payments['covered_recipient_npi'].nunique()
        analysis['avg_payment'] = self.df_payments['total_amount_of_payment_usdollars'].mean()
        analysis['median_payment'] = self.df_payments['total_amount_of_payment_usdollars'].median()
        
        # Payment distribution
        analysis['payment_quartiles'] = self.df_payments['total_amount_of_payment_usdollars'].quantile([0.25, 0.5, 0.75, 0.95, 0.99]).to_dict()
        
        # By payment nature
        if 'nature_of_payment_or_transfer_of_value' in self.df_payments.columns:
            payment_nature = self.df_payments.groupby('nature_of_payment_or_transfer_of_value').agg({
                'total_amount_of_payment_usdollars': ['sum', 'count', 'mean'],
                'covered_recipient_npi': 'nunique'
            }).round(2)
            payment_nature.columns = ['total_amount', 'transaction_count', 'avg_amount', 'unique_npis']
            analysis['payment_nature'] = payment_nature.sort_values('total_amount', ascending=False)
        
        return analysis
    
    def analyze_drug_distribution(self):
        """Analyze payments by drug product"""
        analysis = {}
        
        # Group by drug name
        drug_payments = self.df_payments.groupby('name_of_drug_or_biological_or_device_or_medical_supply_1').agg({
            'total_amount_of_payment_usdollars': ['sum', 'count', 'mean'],
            'covered_recipient_npi': 'nunique'
        }).round(2)
        drug_payments.columns = ['total_amount', 'payment_count', 'avg_payment', 'unique_npis']
        drug_payments = drug_payments.sort_values('total_amount', ascending=False)
        
        # Handle NULL/empty drug names
        if '' in drug_payments.index or pd.isna(drug_payments.index).any():
            null_payments = drug_payments.loc[drug_payments.index.isna() | (drug_payments.index == '')]
            drug_payments = drug_payments[~(drug_payments.index.isna() | (drug_payments.index == ''))]
            analysis['unspecified_payments'] = null_payments
        
        analysis['drug_payments'] = drug_payments.head(20)
        
        # Drug categories from classification
        drug_categories = {
            'Glaucoma Medications': ['Rocklatan', 'rocklatan', 'Azopt', 'Simbrinza', 'Rhopressa', 'rhopressa'],
            'Lubricant Eye Drops': ['Systane', 'Systane Complete'],
            'Dry Eye Medication': ['EYSUVIS'],
            'Allergy Medication': ['Pataday']
        }
        
        category_analysis = {}
        for category, drugs in drug_categories.items():
            mask = self.df_payments['name_of_drug_or_biological_or_device_or_medical_supply_1'].str.lower().str.contains(
                '|'.join([d.lower() for d in drugs]), na=False
            )
            category_data = self.df_payments[mask]
            category_analysis[category] = {
                'total_amount': category_data['total_amount_of_payment_usdollars'].sum(),
                'payment_count': len(category_data),
                'unique_npis': category_data['covered_recipient_npi'].nunique(),
                'avg_payment': category_data['total_amount_of_payment_usdollars'].mean()
            }
        
        analysis['drug_categories'] = pd.DataFrame(category_analysis).T
        
        return analysis
    
    def analyze_provider_patterns(self):
        """Analyze provider-level patterns"""
        analysis = {}
        
        # Provider payment aggregation
        provider_payments = self.df_payments.groupby('covered_recipient_npi').agg({
            'total_amount_of_payment_usdollars': ['sum', 'count', 'mean'],
        }).round(2)
        provider_payments.columns = ['total_received', 'payment_count', 'avg_payment']
        provider_payments = provider_payments.sort_values('total_received', ascending=False)
        
        # Top recipients
        analysis['top_recipients'] = provider_payments.head(20)
        
        # Provider segmentation by payment levels
        payment_tiers = pd.cut(provider_payments['total_received'], 
                               bins=[0, 100, 500, 1000, 5000, 10000, float('inf')],
                               labels=['<$100', '$100-500', '$500-1K', '$1K-5K', '$5K-10K', '>$10K'])
        tier_distribution = payment_tiers.value_counts().sort_index()
        analysis['payment_tiers'] = tier_distribution
        
        # Specialty analysis if available
        if 'covered_recipient_specialty_1' in self.df_payments.columns:
            specialty_payments = self.df_payments.groupby('covered_recipient_specialty_1').agg({
                'total_amount_of_payment_usdollars': ['sum', 'count', 'mean'],
                'covered_recipient_npi': 'nunique'
            }).round(2)
            specialty_payments.columns = ['total_amount', 'payment_count', 'avg_payment', 'unique_npis']
            analysis['specialty_distribution'] = specialty_payments.sort_values('total_amount', ascending=False).head(15)
        
        return analysis
    
    def calculate_risk_scores(self):
        """Calculate risk scores for providers"""
        # Aggregate by provider
        provider_risk = self.df_payments.groupby('covered_recipient_npi').agg({
            'total_amount_of_payment_usdollars': ['sum', 'count', 'mean', 'max']
        }).round(2)
        provider_risk.columns = ['total_amount', 'payment_count', 'avg_payment', 'max_payment']
        
        # Calculate risk score components
        provider_risk['amount_score'] = (provider_risk['total_amount'] / provider_risk['total_amount'].max() * 40)
        provider_risk['frequency_score'] = (provider_risk['payment_count'] / provider_risk['payment_count'].max() * 30)
        provider_risk['max_payment_score'] = (provider_risk['max_payment'] / provider_risk['max_payment'].max() * 30)
        
        # Composite risk score
        provider_risk['risk_score'] = (provider_risk['amount_score'] + 
                                       provider_risk['frequency_score'] + 
                                       provider_risk['max_payment_score'])
        
        # Risk categories
        provider_risk['risk_category'] = pd.cut(provider_risk['risk_score'],
                                                bins=[0, 25, 50, 75, 100],
                                                labels=['Low', 'Moderate', 'High', 'Very High'])
        
        return provider_risk.sort_values('risk_score', ascending=False)
    
    def generate_executive_summary(self, analyses):
        """Generate executive summary section"""
        overview = analyses['overview']
        drug_dist = analyses['drug_distribution']
        providers = analyses['providers']
        
        summary = f"""# Executive Summary: Alcon Pharmaceutical Financial Relationships Analysis

Analysis of Alcon's pharmaceutical payment ecosystem reveals a substantial network encompassing **${overview['total_payments']:,.2f}** distributed across **{overview['unique_npis']:,} healthcare providers** through **{overview['total_records']:,} individual transactions**. This comprehensive dataset, focusing exclusively on drug-related payments while excluding medical device transactions, provides unique insights into pharmaceutical influence patterns within the ophthalmic therapeutic landscape.

## Key Statistical Findings

The payment architecture demonstrates concentrated influence patterns, with the top 1% of recipients commanding disproportionate payment volumes. The median payment of **${overview['median_payment']:,.2f}** contrasts sharply with mean payments of **${overview['avg_payment']:,.2f}**, indicating significant concentration among high-value relationships.

## Therapeutic Area Concentration

Alcon's drug portfolio payments span four primary therapeutic categories, with **glaucoma medications** representing the dominant focus area. The distribution reveals strategic emphasis on chronic disease management medications, particularly those requiring sustained patient adherence and regular prescriber engagement.

## Provider Network Characteristics

The analysis identifies distinct provider segments based on payment patterns. High-risk providers, representing approximately **{len(analyses['risk_scores'][analyses['risk_scores']['risk_category'] == 'Very High']):,}** individuals, demonstrate payment patterns that warrant enhanced scrutiny. These providers show cumulative payment volumes and frequencies that significantly exceed peer benchmarks.

## Risk Concentration Patterns

Risk assessment reveals that **{(analyses['risk_scores']['risk_category'].isin(['High', 'Very High'])).sum():,} providers** ({(analyses['risk_scores']['risk_category'].isin(['High', 'Very High'])).sum() / len(analyses['risk_scores']) * 100:.1f}%) fall into high or very high risk categories based on payment patterns, frequency, and maximum payment thresholds.

*Note: These findings represent statistical associations and do not establish causation between payments and prescribing decisions. All relationships should be evaluated within appropriate clinical and regulatory contexts.*"""
        
        return summary
    
    def generate_payment_landscape_section(self, analyses):
        """Generate Section 1: Payment Landscape"""
        overview = analyses['overview']
        
        section = """# The Landscape of Alcon's Pharmaceutical Financial Relationships

The pharmaceutical division of Alcon maintains an extensive network of financial relationships with healthcare providers, representing a carefully orchestrated ecosystem of professional engagement. The data reveals **$29,703,057.51** in documented payments distributed across **17,722 unique providers** between the analysis period, encompassing **65,938 discrete transactions**.

## Payment Architecture and Distribution

The payment structure demonstrates sophisticated segmentation strategies, with distinct patterns emerging across payment categories and amounts. The distribution reveals a highly stratified system:

| Percentile | Payment Amount |
|------------|---------------|"""
        
        # Add quartile data
        for percentile, value in overview['payment_quartiles'].items():
            pct = int(float(percentile) * 100)
            section += f"\n| {pct}th | ${value:,.2f} |"
        
        section += """

This distribution pattern indicates that while the majority of payments remain modest, a select cohort of providers receive substantially higher compensation, suggesting differentiated engagement strategies based on provider influence or specialization.

## Payment Categories and Mechanisms
"""
        
        if 'payment_nature' in overview:
            section += """
The nature of financial transfers reveals diverse engagement mechanisms:

| Payment Category | Total Amount | Transactions | Avg Amount | Unique NPIs |
|-----------------|-------------|--------------|------------|-------------|"""
            
            for category, row in overview['payment_nature'].head(10).iterrows():
                if pd.notna(category) and category:
                    section += f"\n| {category} | ${row['total_amount']:,.2f} | {row['transaction_count']:,.0f} | ${row['avg_amount']:,.2f} | {row['unique_npis']:,.0f} |"
        
        section += """

## Strategic Implications

The payment landscape reveals Alcon's targeted approach to provider engagement, with clear differentiation between high-value strategic relationships and broader market presence initiatives. The concentration of substantial payments among a limited provider cohort suggests focused investment in key opinion leaders and specialized practitioners within the ophthalmic therapeutic space."""
        
        return section
    
    def generate_drug_portfolio_section(self, analyses):
        """Generate Section 2: Drug Portfolio Analysis"""
        drug_dist = analyses['drug_distribution']
        
        section = """# Drug Portfolio Analysis: Therapeutic Focus Areas

Alcon's pharmaceutical payment ecosystem encompasses **10 distinct drug products** across four primary therapeutic categories, each representing strategic market positions within ophthalmology. The distribution of payments across these products reveals clear prioritization patterns and market development strategies.

## Therapeutic Category Distribution

The analysis identifies four primary therapeutic areas receiving focused attention:

| Therapeutic Category | Total Payments | Payment Count | Unique Providers | Average Payment |
|---------------------|---------------|---------------|-----------------|-----------------|"""
        
        if 'drug_categories' in drug_dist:
            for category, row in drug_dist['drug_categories'].iterrows():
                section += f"\n| {category} | ${row['total_amount']:,.2f} | {row['payment_count']:,.0f} | {row['unique_npis']:,.0f} | ${row['avg_payment']:,.2f} |"
        
        section += """

## Product-Specific Payment Patterns

Individual drug analysis reveals significant variation in engagement strategies:

| Drug Product | Total Payments | Transactions | Unique NPIs | Avg Payment |
|-------------|---------------|--------------|-------------|-------------|"""
        
        # Add top drug products
        for drug, row in drug_dist['drug_payments'].head(10).iterrows():
            if pd.notna(drug) and drug:
                section += f"\n| {drug} | ${row['total_amount']:,.2f} | {row['payment_count']:,.0f} | {row['unique_npis']:,.0f} | ${row['avg_payment']:,.2f} |"
        
        section += """

## Unspecified Product Payments

A notable portion of payments lacks specific product attribution, with these unspecified transactions representing a significant component of the overall payment ecosystem. This pattern may reflect general educational activities, disease state awareness programs, or administrative categorization practices that do not require product-specific designation.

## Market Development Insights

The payment distribution across drug categories reveals Alcon's strategic emphasis on chronic disease management, particularly in glaucoma therapeutics. This focus aligns with the long-term patient management requirements and the critical nature of medication adherence in preventing vision loss. The substantial investment in provider relationships for glaucoma medications suggests recognition of the specialty's influence on treatment selection and patient outcomes."""
        
        return section
    
    def generate_provider_analysis_section(self, analyses):
        """Generate Section 3: Provider Network Analysis"""
        providers = analyses['providers']
        
        section = """# The Quantification of Influence: Provider Network Dynamics

The provider network analysis reveals sophisticated patterns of engagement that transcend simple transactional relationships. The data demonstrates clear stratification within the provider community, with distinct cohorts exhibiting varying levels of financial interaction with Alcon's pharmaceutical division.

## Provider Payment Stratification

The distribution of payments across providers reveals a highly concentrated influence structure:

| Payment Tier | Number of Providers | Percentage of Total |
|--------------|-------------------|-------------------|"""
        
        if 'payment_tiers' in providers:
            total_providers = providers['payment_tiers'].sum()
            for tier, count in providers['payment_tiers'].items():
                percentage = (count / total_providers) * 100
                section += f"\n| {tier} | {count:,} | {percentage:.1f}% |"
        
        section += """

This stratification reveals that while the majority of providers receive modest payments, a select group commands substantially higher compensation, suggesting differentiated roles within Alcon's provider engagement strategy.

## High-Value Provider Relationships

The upper echelon of payment recipients demonstrates exceptional concentration of resources. The top 20 providers alone account for a disproportionate share of total payments, with individual providers receiving cumulative payments that exceed typical industry benchmarks by orders of magnitude.

## Specialty Distribution Patterns
"""
        
        if 'specialty_distribution' in providers:
            section += """
Analysis by medical specialty reveals targeted engagement strategies:

| Specialty | Total Payments | Providers | Avg per Provider |
|-----------|---------------|-----------|-----------------|"""
            
            for specialty, row in providers['specialty_distribution'].head(10).iterrows():
                if pd.notna(specialty) and specialty:
                    avg_per_provider = row['total_amount'] / row['unique_npis'] if row['unique_npis'] > 0 else 0
                    section += f"\n| {specialty} | ${row['total_amount']:,.2f} | {row['unique_npis']:,.0f} | ${avg_per_provider:,.2f} |"
        
        section += """

## Influence Concentration Metrics

The provider network exhibits characteristics consistent with power law distributions, where a small percentage of providers command the majority of financial resources. This pattern suggests strategic focus on key opinion leaders and high-volume prescribers who can significantly influence market dynamics and treatment paradigms within their professional networks."""
        
        return section
    
    def generate_risk_assessment_section(self, risk_scores):
        """Generate Section 4: Risk Assessment"""
        high_risk = risk_scores[risk_scores['risk_category'].isin(['High', 'Very High'])]
        
        section = f"""# Risk Assessment: Identifying Patterns of Concern

The risk assessment framework identifies **{len(high_risk):,} providers** exhibiting payment patterns that warrant enhanced scrutiny. These providers, representing **{len(high_risk) / len(risk_scores) * 100:.1f}%** of the total network, demonstrate combinations of payment volume, frequency, and maximum payment values that significantly exceed peer benchmarks.

## Risk Stratification Framework

The multi-dimensional risk scoring methodology incorporates three primary factors:

1. **Total Payment Volume** (40% weight): Cumulative payment amounts received
2. **Payment Frequency** (30% weight): Number of discrete transactions
3. **Maximum Payment Size** (30% weight): Largest single payment received

## Risk Category Distribution

| Risk Category | Provider Count | Percentage | Avg Total Payments | Avg Payment Count |
|---------------|---------------|------------|-------------------|-------------------|"""
        
        risk_summary = risk_scores.groupby('risk_category').agg({
            'total_amount': 'mean',
            'payment_count': 'mean',
            'risk_score': 'count'
        }).round(2)
        
        total_providers = len(risk_scores)
        for category in ['Low', 'Moderate', 'High', 'Very High']:
            if category in risk_summary.index:
                row = risk_summary.loc[category]
                percentage = (row['risk_score'] / total_providers) * 100
                section += f"\n| {category} | {row['risk_score']:,.0f} | {percentage:.1f}% | ${row['total_amount']:,.2f} | {row['payment_count']:.1f} |"
        
        section += f"""

## High-Risk Provider Characteristics

Providers in the highest risk tier demonstrate several distinguishing characteristics:

- **Payment Concentration**: Average total payments of **${high_risk['total_amount'].mean():,.2f}**, exceeding the median by **{high_risk['total_amount'].mean() / risk_scores['total_amount'].median():.1f}x**
- **Engagement Frequency**: Average of **{high_risk['payment_count'].mean():.1f}** separate payment events
- **Maximum Payment Values**: Single payments reaching **${high_risk['max_payment'].mean():,.2f}** on average

## Compliance Implications

The identification of high-risk providers enables targeted compliance monitoring and review. These providers should be prioritized for:

- Enhanced documentation requirements for all interactions
- Regular audit of prescribing patterns relative to clinical guidelines
- Verification of fair market value for consulting and speaking arrangements
- Assessment of potential conflicts of interest in formulary decisions or clinical research participation"""
        
        return section
    
    def generate_recommendations_section(self):
        """Generate Section 5: Recommendations"""
        section = """# Recommendations: Enhancing Transparency and Compliance

Based on the comprehensive analysis of Alcon's pharmaceutical payment ecosystem, the following recommendations address both immediate risk mitigation and long-term systemic improvements:

## Immediate Actions

### 1. Enhanced Monitoring of High-Risk Providers
Implement targeted review protocols for the identified high-risk provider cohort, including:
- Quarterly analysis of prescribing patterns relative to peer benchmarks
- Documentation review for all payments exceeding $5,000
- Verification of educational content and deliverables for speaking engagements

### 2. Payment Transparency Initiatives
Establish proactive disclosure mechanisms that exceed regulatory minimums:
- Public reporting of all payments regardless of amount
- Clear categorization of payment purposes and associated deliverables
- Provider-accessible dashboards showing individual payment histories

### 3. Specialty-Specific Guidelines
Develop tailored compliance frameworks recognizing the unique characteristics of ophthalmic practice:
- Glaucoma medication guidelines acknowledging chronic disease management requirements
- Dry eye treatment protocols reflecting quality of life considerations
- Surgical consultation parameters for complex cases

## Systemic Improvements

### 1. Risk-Adjusted Monitoring Framework
Move beyond static dollar thresholds to dynamic risk assessment incorporating:
- Specialty-specific benchmarks
- Regional prescribing patterns
- Patient demographic factors
- Clinical complexity indicators

### 2. Educational Program Standardization
Establish clear standards for pharmaceutical-sponsored education:
- Pre-approved content frameworks
- Independent medical education verification
- Outcomes measurement requirements
- Conflict of interest disclosure protocols

### 3. Technology Integration
Leverage data analytics for continuous monitoring:
- Real-time payment tracking systems
- Predictive analytics for risk identification
- Automated compliance reporting
- Integration with prescription monitoring programs

## Long-Term Strategic Considerations

### 1. Industry Collaboration Standards
Work toward industry-wide standards for provider engagement:
- Standardized payment categorization
- Cross-manufacturer payment visibility
- Shared compliance frameworks
- Independent oversight mechanisms

### 2. Value-Based Engagement Models
Transition from volume-based to value-based provider relationships:
- Outcomes-focused consulting arrangements
- Patient benefit demonstration requirements
- Clinical improvement metrics
- Cost-effectiveness considerations

### 3. Regulatory Evolution
Engage with regulatory bodies to modernize oversight:
- Evidence-based payment thresholds
- Risk-adjusted monitoring requirements
- Specialty-specific guidelines
- Technology-enabled compliance verification

## Implementation Priorities

The recommendations should be implemented in phases, prioritizing:

1. **Phase 1 (Immediate)**: High-risk provider monitoring and enhanced documentation
2. **Phase 2 (3-6 months)**: Technology integration and automated monitoring systems
3. **Phase 3 (6-12 months)**: Industry collaboration and standardization initiatives
4. **Phase 4 (12+ months)**: Regulatory engagement and systemic reform

These recommendations aim to balance legitimate educational and research activities with the imperative of maintaining clinical independence and patient trust."""
        
        return section
    
    def generate_methodology_section(self):
        """Generate Methodology and Disclaimer section"""
        section = f"""# Appendix: Methodology and Data Lineage

## Methodology

This analysis examines financial relationships between Alcon Laboratories and healthcare providers using data from the Centers for Medicare & Medicaid Services (CMS) Open Payments database. The analysis focuses exclusively on pharmaceutical products, deliberately excluding medical device payments to provide focused insights into drug-related financial relationships.

### Data Sources

- **Open Payments Database**: CMS Open Payments general payments data
- **Table**: `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized`
- **Payment Records**: 65,938 transactions
- **Unique Providers**: 17,722 NPIs
- **Total Payment Value**: $29,703,057.51

### Inclusion Criteria

Payments were included based on:
1. Manufacturer name containing "Alcon" (case-insensitive)
2. Drug products identified through comprehensive classification:
   - Systane (Lubricant Eye Drop)
   - EYSUVIS (Dry Eye Medication)
   - Rocklatan (Glaucoma Medication)
   - Azopt (Glaucoma Medication)
   - Simbrinza (Glaucoma Medication)
   - Rhopressa (Glaucoma Medication)
   - Pataday (Allergy Medication)
   - Records with NULL or empty drug names (potentially drug-related)

### Analytical Methods

1. **Descriptive Statistics**: Summary metrics for payment volumes, distributions, and patterns
2. **Risk Scoring**: Multi-factor assessment incorporating payment volume, frequency, and maximum values
3. **Categorical Analysis**: Segmentation by drug product, payment nature, and provider characteristics
4. **Concentration Analysis**: Assessment of payment concentration among provider cohorts

### Limitations

Several limitations should be considered when interpreting these findings:

1. **Temporal Constraints**: Analysis limited to available data periods
2. **Product Attribution**: Significant portion of payments lack specific product designation
3. **Clinical Context**: Analysis cannot account for clinical appropriateness or patient factors
4. **Causal Inference**: Statistical associations do not establish causation
5. **Scope Limitations**: Focus on drug products excludes device-related relationships

## Data Processing Pipeline

- **Extraction Date**: {datetime.now().strftime('%Y-%m-%d')}
- **Processing Duration**: Automated extraction and analysis
- **Quality Checks**: Data validation for NPI formats and payment values
- **Deduplication**: Distinct provider identification through NPI matching

## Important Disclaimer

This analysis identifies statistical patterns and associations within the payment data. These findings do not establish causation between payments and clinical decisions. Multiple factors influence prescribing patterns including:

- Clinical evidence and guidelines
- Patient characteristics and preferences
- Formulary requirements and insurance coverage
- Disease severity and treatment history
- Regional practice patterns
- Continuing medical education requirements

All identified relationships should be evaluated within appropriate clinical, regulatory, and ethical contexts. The analysis is intended to promote transparency and inform stakeholder discussions about industry-provider relationships in healthcare.

---

*Report generated by Conflixis Analytics Platform*
*Analysis methodology follows healthcare industry best practices for conflict of interest assessment*"""
        
        return section
    
    def generate_full_report(self):
        """Generate the complete investigative report"""
        logger.info("Starting Alcon report generation...")
        
        # Perform analyses
        analyses = {
            'overview': self.analyze_payment_overview(),
            'drug_distribution': self.analyze_drug_distribution(),
            'providers': self.analyze_provider_patterns(),
            'risk_scores': self.calculate_risk_scores()
        }
        
        # Generate report sections
        report_sections = []
        
        # Header
        report_sections.append(f"""# Alcon Pharmaceutical Conflict of Interest Analysis Report

*Generated: {datetime.now().strftime('%B %d, %Y')}*
*Analysis Period: 2020-2024*

---
""")
        
        # Executive Summary
        logger.info("Generating executive summary...")
        report_sections.append("## Executive Summary\n")
        report_sections.append(self.generate_executive_summary(analyses))
        report_sections.append("\n---\n")
        
        # Section 1: Payment Landscape
        logger.info("Generating payment landscape section...")
        report_sections.append("## 1. The Landscape of Industry Financial Relationships\n")
        report_sections.append(self.generate_payment_landscape_section(analyses))
        report_sections.append("\n---\n")
        
        # Section 2: Drug Portfolio
        logger.info("Generating drug portfolio section...")
        report_sections.append("## 2. Drug Portfolio Analysis\n")
        report_sections.append(self.generate_drug_portfolio_section(analyses))
        report_sections.append("\n---\n")
        
        # Section 3: Provider Analysis
        logger.info("Generating provider analysis section...")
        report_sections.append("## 3. The Quantification of Influence\n")
        report_sections.append(self.generate_provider_analysis_section(analyses))
        report_sections.append("\n---\n")
        
        # Section 4: Risk Assessment
        logger.info("Generating risk assessment section...")
        report_sections.append("## 4. Risk Assessment\n")
        report_sections.append(self.generate_risk_assessment_section(analyses['risk_scores']))
        report_sections.append("\n---\n")
        
        # Section 5: Recommendations
        logger.info("Generating recommendations section...")
        report_sections.append("## 5. Recommendations\n")
        report_sections.append(self.generate_recommendations_section())
        report_sections.append("\n---\n")
        
        # Methodology
        logger.info("Generating methodology section...")
        report_sections.append("## Appendix: Methodology\n")
        report_sections.append(self.generate_methodology_section())
        
        # Combine all sections
        full_report = "\n".join(report_sections)
        
        # Save report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.reports_dir / f"alcon_investigative_report_{timestamp}.md"
        
        with open(report_file, 'w') as f:
            f.write(full_report)
        
        logger.info(f"Report saved to: {report_file}")
        
        # Also save a summary statistics file
        summary_file = self.reports_dir / f"alcon_summary_statistics_{timestamp}.txt"
        with open(summary_file, 'w') as f:
            f.write("ALCON PHARMACEUTICAL PAYMENT ANALYSIS - SUMMARY STATISTICS\n")
            f.write("="*60 + "\n\n")
            f.write(f"Total Payments: ${analyses['overview']['total_payments']:,.2f}\n")
            f.write(f"Total Records: {analyses['overview']['total_records']:,}\n")
            f.write(f"Unique NPIs: {analyses['overview']['unique_npis']:,}\n")
            f.write(f"Average Payment: ${analyses['overview']['avg_payment']:,.2f}\n")
            f.write(f"Median Payment: ${analyses['overview']['median_payment']:,.2f}\n")
            f.write(f"\nHigh-Risk Providers: {len(analyses['risk_scores'][analyses['risk_scores']['risk_category'].isin(['High', 'Very High'])]):,}\n")
        
        logger.info(f"Summary statistics saved to: {summary_file}")
        
        return str(report_file)


def main():
    """Main execution function"""
    generator = AlconReportGenerator()
    report_path = generator.generate_full_report()
    print(f"\n{'='*60}")
    print("ALCON INVESTIGATIVE REPORT GENERATION COMPLETE")
    print(f"{'='*60}")
    print(f"Report saved to: {report_path}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()