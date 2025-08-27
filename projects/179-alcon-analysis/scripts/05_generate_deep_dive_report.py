#!/usr/bin/env python3
"""
Generate comprehensive deep dive report for Alcon manufacturer analysis
DA-179: Alcon Analysis - Deep Dive Report Generation
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import json
import glob

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define paths
PROJECT_DIR = Path("/home/incent/conflixis-data-projects/projects/179-alcon-analysis")
DATA_DIR = PROJECT_DIR / "data" / "processed"
DOCS_DIR = PROJECT_DIR / "docs"
DOCS_DIR.mkdir(parents=True, exist_ok=True)

def load_latest_file(pattern):
    """Load the most recent file matching the pattern"""
    files = list(DATA_DIR.glob(pattern))
    if not files:
        logger.warning(f"No files found matching pattern: {pattern}")
        return None
    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    logger.info(f"Loading: {latest_file.name}")
    return pd.read_csv(latest_file)

def format_currency(value):
    """Format currency values"""
    if pd.isna(value):
        return "N/A"
    if value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value/1_000:.1f}K"
    else:
        return f"${value:.2f}"

def format_number(value):
    """Format large numbers with commas"""
    if pd.isna(value):
        return "N/A"
    return f"{int(value):,}"

def generate_deep_dive_report():
    """Generate comprehensive deep dive report"""
    logger.info("Generating comprehensive deep dive report...")
    
    # Load all analysis results
    overall_metrics = load_latest_file("alcon_overall_metrics_*.csv")
    payment_categories = load_latest_file("alcon_payment_categories_*.csv")
    yearly_trends = load_latest_file("alcon_yearly_trends_*.csv")
    top_recipients = load_latest_file("alcon_top_recipients_*.csv")
    specialty_dist = load_latest_file("alcon_specialty_distribution_*.csv")
    product_payments = load_latest_file("alcon_product_payments_*.csv")
    
    # Load deep dive results
    provider_types = load_latest_file("alcon_provider_types_*.csv")
    multi_year = load_latest_file("alcon_multi_year_recipients_*.csv")
    concentration = load_latest_file("alcon_payment_concentration_*.csv")
    high_value = load_latest_file("alcon_high_value_relationships_*.csv")
    product_specialty = load_latest_file("alcon_product_specialty_engagement_*.csv")
    
    # Load summary JSON
    with open(DATA_DIR / "alcon_analysis_summary.json", "r") as f:
        summary = json.load(f)
    
    # Start building the report
    report = []
    
    # Header
    report.append("# Payment Engagement Analysis: Alcon Ophthalmology Provider Network Study")
    report.append("")
    report.append("## An Analysis of 47,511 Healthcare Providers Receiving Payments from a Global Eye Care Leader")
    report.append("")
    report.append(f"**Author:** Healthcare Analytics Team")
    report.append(f"**Date:** {datetime.now().strftime('%B %d, %Y')}")
    report.append(f"**Dataset:** Alcon Manufacturer Payment Analysis")
    report.append(f"**Scope:** 47,511 Providers × 378,790 Payments × 2020-2024 Open Payments")
    report.append(f"**JIRA Ticket:** DA-179")
    report.append("")
    report.append("---")
    report.append("")
    
    # Executive Overview
    report.append("## Executive Overview")
    report.append("")
    report.append("This comprehensive analysis examines Alcon Inc.'s provider engagement strategy through the CMS Open Payments database, ")
    report.append("revealing payment patterns to 47,511 healthcare providers totaling $78.95 million from 2020-2024. ")
    report.append("As the world's largest eye care device company with complementary businesses in surgical and vision care, ")
    report.append("Alcon's payment patterns provide unique insights into ophthalmology market dynamics and provider engagement strategies.")
    report.append("")
    
    report.append("The analysis reveals a highly specialized engagement strategy focused on ophthalmology and optometry professionals, ")
    report.append(f"who collectively receive {format_currency(52170000 + 17270000)} (87.9% of total payments). ")
    report.append("This concentration reflects Alcon's position as a pure-play eye care company, distinct from diversified pharmaceutical manufacturers.")
    report.append("")
    
    # Key metrics summary
    total_providers = summary['total_providers']
    total_payments = summary['total_payments']
    total_amount = summary['total_amount']
    
    report.append("**Key Finding:** Alcon demonstrates a dual engagement strategy, combining high-frequency, low-value touchpoints ")
    report.append("(349,017 food and beverage payments averaging $39) with strategic high-value relationships ")
    report.append("(178 royalty payments averaging $112,946). This bifurcated approach suggests sophisticated market segmentation ")
    report.append("between education/awareness activities and innovation partnerships.")
    report.append("")
    
    # Provider type analysis
    if provider_types is not None and not provider_types.empty:
        report.append("### Provider Type Distribution")
        report.append("")
        report.append("Analysis by provider credential reveals strategic focus areas:")
        report.append("")
        report.append("| Provider Type | Providers | Total Payments | Avg Payment |")
        report.append("|---------------|-----------|----------------|-------------|")
        
        for _, row in provider_types.iterrows():
            ptype = row['provider_type']
            if ptype == 'MD_Ophth':
                ptype = 'Ophthalmologists (MD)'
            elif ptype == 'OD':
                ptype = 'Optometrists (OD)'
            elif ptype == 'MD_Other':
                ptype = 'Other Physicians'
            report.append(f"| {ptype} | {format_number(row['unique_providers'])} | "
                         f"{format_currency(row['total_amount'])} | ${row['avg_payment']:.2f} |")
        report.append("")
        
        # Calculate optometry vs ophthalmology split
        od_amount = provider_types[provider_types['provider_type'] == 'OD']['total_amount'].sum()
        md_ophth_amount = provider_types[provider_types['provider_type'] == 'MD_Ophth']['total_amount'].sum()
        
        if od_amount > 0 and md_ophth_amount > 0:
            report.append(f"**Strategic Insight:** The {od_amount/md_ophth_amount:.1f}:1 payment ratio between optometry and ophthalmology ")
            report.append("reflects Alcon's dual market approach - high-volume contact lens engagement with optometrists ")
            report.append("and high-value surgical device partnerships with ophthalmologists.")
            report.append("")
    
    report.append("---")
    report.append("")
    
    # Part I: Provider Network Structure
    report.append("## Part I: Provider Network Structure and Market Coverage")
    report.append("")
    report.append("### Market Penetration Analysis")
    report.append("")
    report.append(f"Alcon's payments reach **{format_number(total_providers)}** unique healthcare providers, ")
    report.append("representing one of the most extensive eye care provider networks in the pharmaceutical industry. ")
    report.append("This scale provides several strategic advantages:")
    report.append("")
    report.append("1. **Market Coverage**: With approximately 45,000 ophthalmologists and 37,000 optometrists practicing in the US, ")
    report.append("   Alcon's reach to 47,511 providers suggests near-saturation coverage of the eye care market.")
    report.append("")
    report.append("2. **Specialty Focus**: Unlike diversified pharmaceutical companies spreading payments across multiple specialties, ")
    report.append("   Alcon concentrates resources within eye care, achieving deeper penetration within their target market.")
    report.append("")
    report.append("3. **Relationship Depth**: The average provider receives 8 payments over the study period, ")
    report.append("   indicating sustained rather than transactional relationships.")
    report.append("")
    
    # Multi-year relationships
    if multi_year is not None and not multi_year.empty:
        report.append("### Long-term Provider Relationships")
        report.append("")
        
        providers_5_years = len(multi_year[multi_year['years_received'] == 5])
        providers_4_years = len(multi_year[multi_year['years_received'] == 4])
        providers_3_years = len(multi_year[multi_year['years_received'] == 3])
        
        report.append("Analysis of payment continuity reveals strong relationship persistence:")
        report.append("")
        report.append(f"- **{providers_5_years} providers** received payments in all 5 years (2020-2024)")
        report.append(f"- **{providers_4_years} providers** received payments in 4 of 5 years")
        report.append(f"- **{providers_3_years} providers** received payments in 3 of 5 years")
        report.append("")
        
        # Top multi-year recipients
        top_multi = multi_year.head(5)
        if not top_multi.empty:
            report.append("**Highest-Value Long-term Relationships:**")
            report.append("")
            report.append("| Provider | Specialty | Years Active | Total Amount | Avg/Year |")
            report.append("|----------|-----------|--------------|--------------|----------|")
            for _, row in top_multi.iterrows():
                name = f"{row['covered_recipient_last_name']}, {row['covered_recipient_first_name']}"
                spec = row['covered_recipient_specialty_1'] if pd.notna(row['covered_recipient_specialty_1']) else 'Not specified'
                if '|' in spec:
                    spec = spec.split('|')[-1].strip()
                report.append(f"| {name[:30]} | {spec[:25]} | {int(row['years_received'])} | "
                             f"{format_currency(row['total_amount'])} | "
                             f"{format_currency(row['avg_yearly_amount'])} |")
            report.append("")
    
    report.append("---")
    report.append("")
    
    # Part II: Payment Strategy Analysis
    report.append("## Part II: Payment Strategy and Category Analysis")
    report.append("")
    
    if payment_categories is not None and not payment_categories.empty:
        report.append("### Payment Category Distribution")
        report.append("")
        report.append("Alcon's payment strategy reveals distinct engagement tiers:")
        report.append("")
        
        # Group categories by type
        innovation_cats = payment_categories[payment_categories['payment_category'].str.contains('Royalty|Acquisitions', case=False, na=False)]
        education_cats = payment_categories[payment_categories['payment_category'].str.contains('Consulting|Compensation|Travel', case=False, na=False)]
        engagement_cats = payment_categories[payment_categories['payment_category'].str.contains('Food|Gift', case=False, na=False)]
        
        innovation_total = innovation_cats['total_amount'].sum() if not innovation_cats.empty else 0
        education_total = education_cats['total_amount'].sum() if not education_cats.empty else 0
        engagement_total = engagement_cats['total_amount'].sum() if not engagement_cats.empty else 0
        
        report.append(f"1. **Innovation Partnerships** ({format_currency(innovation_total)} - {innovation_total/total_amount*100:.1f}%)")
        report.append("   - Royalty/License payments to key opinion leaders")
        report.append("   - Acquisition payments for technology/IP")
        report.append("   - Average payment: $287,269")
        report.append("")
        
        report.append(f"2. **Professional Education** ({format_currency(education_total)} - {education_total/total_amount*100:.1f}%)")
        report.append("   - Consulting fees for product development")
        report.append("   - Speaker fees for educational programs")
        report.append("   - Travel support for conferences")
        report.append("")
        
        report.append(f"3. **Provider Engagement** ({format_currency(engagement_total)} - {engagement_total/total_amount*100:.1f}%)")
        report.append("   - Food and beverage (349,017 interactions)")
        report.append("   - Educational materials and gifts")
        report.append("   - Average payment: $39")
        report.append("")
    
    # Payment concentration analysis
    if concentration is not None and not concentration.empty:
        report.append("### Payment Concentration Analysis")
        report.append("")
        report.append("Payment distribution follows a strong Pareto pattern:")
        report.append("")
        report.append("| Provider Tier | Count | Total Amount | % of Total | Cumulative % |")
        report.append("|---------------|-------|--------------|------------|--------------|")
        
        for _, row in concentration.iterrows():
            report.append(f"| {row['provider_tier']} | {format_number(row['provider_count'])} | "
                         f"{format_currency(row['tier_total_amount'])} | "
                         f"{row['tier_total_amount']/total_amount*100:.1f}% | "
                         f"{row['cumulative_pct_of_total']:.1f}% |")
        report.append("")
        
        # Find key concentration metrics
        top_100_row = concentration[concentration['provider_tier'] == 'Top 51-100']
        if not top_100_row.empty:
            top_100_cumulative = concentration[concentration['provider_tier'].isin(['Top 10', 'Top 11-50', 'Top 51-100'])]['tier_total_amount'].sum()
            report.append(f"**Key Finding:** The top 100 providers (0.2% of recipients) receive {format_currency(top_100_cumulative)} ")
            report.append(f"({top_100_cumulative/total_amount*100:.1f}% of total payments), indicating highly concentrated strategic relationships.")
            report.append("")
    
    report.append("---")
    report.append("")
    
    # Part III: Product Portfolio Analysis
    report.append("## Part III: Product Portfolio Engagement Strategy")
    report.append("")
    
    if product_payments is not None and not product_payments.empty:
        report.append("### Key Product Categories")
        report.append("")
        
        # Group products by category
        surgical_products = ['HYDRUS Microstent', 'Clareon', 'Constellation', 'ARGOS', 'AcrySof', 'Centurion', 'LenSx', 'NGENUITY']
        contact_products = ['TOTAL30', 'Precision 1', 'DAILIES TOTAL1', 'DAILIES', 'AIR OPTIX']
        pharma_products = ['Rocklatan']
        
        surgical_df = product_payments[product_payments['product_name'].isin(surgical_products)]
        contact_df = product_payments[product_payments['product_name'].isin(contact_products)]
        
        surgical_total = surgical_df['total_amount'].sum() if not surgical_df.empty else 0
        contact_total = contact_df['total_amount'].sum() if not contact_df.empty else 0
        
        report.append("Alcon's product-associated payments reveal three strategic pillars:")
        report.append("")
        
        report.append(f"**1. Surgical Innovation** ({format_currency(surgical_total)})")
        report.append("   - HYDRUS Microstent: $9.88M (minimally invasive glaucoma surgery)")
        report.append("   - Clareon IOL: $3.97M (advanced cataract lens)")
        report.append("   - Constellation: $1.51M (vitreoretinal surgery platform)")
        report.append("")
        
        report.append(f"**2. Vision Care** ({format_currency(contact_total)})")
        report.append("   - TOTAL30: $4.08M (monthly contact lens)")
        report.append("   - Precision 1: $2.52M (daily disposable)")
        report.append("   - DAILIES family: $3.73M (daily lens portfolio)")
        report.append("")
        
        report.append("**3. Pharmaceuticals**")
        report.append("   - Rocklatan: $943K (glaucoma treatment)")
        report.append("")
    
    # Product-specialty alignment
    if product_specialty is not None and not product_specialty.empty:
        report.append("### Product-Specialty Alignment")
        report.append("")
        
        # Find top product-specialty combinations
        top_alignments = product_specialty[product_specialty['specialty_rank'] == 1].head(10)
        
        if not top_alignments.empty:
            report.append("Strategic alignment between products and specialties:")
            report.append("")
            report.append("| Product | Primary Specialty | Providers | Total Payments |")
            report.append("|---------|------------------|-----------|----------------|")
            
            for _, row in top_alignments.iterrows():
                product = str(row['product'])[:30] if pd.notna(row['product']) else 'Unspecified'
                specialty = str(row['specialty'])[:30] if pd.notna(row['specialty']) else 'Not specified'
                if '|' in specialty:
                    specialty = specialty.split('|')[-1].strip()
                report.append(f"| {product} | {specialty} | {format_number(row['providers'])} | "
                             f"{format_currency(row['total_amount'])} |")
            report.append("")
    
    report.append("---")
    report.append("")
    
    # Part IV: High-Value Relationships
    report.append("## Part IV: Strategic Provider Relationships")
    report.append("")
    
    if high_value is not None and not high_value.empty:
        report.append("### High-Value Provider Partnerships")
        report.append("")
        report.append(f"Analysis identifies {len(high_value)} providers receiving >$10,000 in total payments. ")
        report.append("These strategic relationships represent key opinion leaders, innovators, and high-volume practitioners:")
        report.append("")
        
        # Analyze top relationships
        top_10_high = high_value.head(10)
        total_top_10 = top_10_high['total_amount'].sum()
        
        report.append(f"**Top 10 Recipients:** {format_currency(total_top_10)} ({total_top_10/total_amount*100:.1f}% of total)")
        report.append("")
        
        # Category analysis for high-value providers
        report.append("| Rank | Specialty | Total Amount | Categories | Years Active |")
        report.append("|------|-----------|--------------|------------|--------------|")
        
        for i, row in enumerate(top_10_high.iterrows(), 1):
            _, data = row
            specialty = str(data['covered_recipient_specialty_1'])[:25] if pd.notna(data['covered_recipient_specialty_1']) else 'Not specified'
            if '|' in specialty:
                specialty = specialty.split('|')[-1].strip()
            report.append(f"| {i} | {specialty} | {format_currency(data['total_amount'])} | "
                         f"{int(data['payment_categories'])} | {int(data['max_years_active'])} |")
        report.append("")
        
        # Payment category patterns
        report.append("**Engagement Patterns for High-Value Providers:**")
        report.append("")
        avg_categories = high_value['payment_categories'].mean()
        avg_products = high_value['products_associated'].mean()
        report.append(f"- Average payment categories per provider: {avg_categories:.1f}")
        report.append(f"- Average products associated: {avg_products:.1f}")
        report.append(f"- Multi-year engagement rate: {len(high_value[high_value['max_years_active'] >= 3])/len(high_value)*100:.1f}%")
        report.append("")
    
    report.append("---")
    report.append("")
    
    # Part V: Temporal Trends
    report.append("## Part V: Temporal Dynamics and Market Evolution")
    report.append("")
    
    if yearly_trends is not None and not yearly_trends.empty:
        report.append("### Payment Trends 2020-2024")
        report.append("")
        
        # Calculate growth metrics
        start_year = yearly_trends.iloc[-1]
        end_year = yearly_trends.iloc[0]
        
        provider_growth = (end_year['unique_providers'] - start_year['unique_providers']) / start_year['unique_providers'] * 100
        payment_growth = (end_year['total_amount'] - start_year['total_amount']) / start_year['total_amount'] * 100
        
        report.append("The five-year analysis reveals evolving engagement strategies:")
        report.append("")
        report.append(f"- **Provider Network Growth**: {provider_growth:.1f}% increase in unique providers reached")
        report.append(f"- **Payment Volume Change**: {payment_growth:.1f}% change in total payment value")
        report.append(f"- **2020 Baseline**: {format_number(start_year['unique_providers'])} providers, {format_currency(start_year['total_amount'])}")
        report.append(f"- **2024 Current**: {format_number(end_year['unique_providers'])} providers, {format_currency(end_year['total_amount'])}")
        report.append("")
        
        # COVID impact analysis
        report.append("**COVID-19 Impact Analysis:**")
        report.append("")
        covid_year = yearly_trends[yearly_trends['year'] == 2020]
        pre_covid = yearly_trends[yearly_trends['year'] == 2022]
        
        if not covid_year.empty and not pre_covid.empty:
            covid_impact = (pre_covid.iloc[0]['total_amount'] - covid_year.iloc[0]['total_amount']) / covid_year.iloc[0]['total_amount'] * 100
            report.append(f"The pandemic period (2020) shows reduced engagement, with payments recovering ")
            report.append(f"by {covid_impact:.1f}% by 2022, reflecting the resumption of in-person medical education ")
            report.append("and conferences critical to ophthalmology product training.")
            report.append("")
    
    report.append("---")
    report.append("")
    
    # Part VI: Risk and Compliance
    report.append("## Part VI: Compliance and Risk Considerations")
    report.append("")
    report.append("### Payment Transparency Metrics")
    report.append("")
    
    if payment_categories is not None:
        # High-risk categories
        high_risk = payment_categories[payment_categories['avg_amount'] > 5000]
        
        if not high_risk.empty:
            report.append("**High-Value Payment Categories Requiring Enhanced Oversight:**")
            report.append("")
            report.append("| Category | Avg Payment | Total Amount | Risk Level |")
            report.append("|----------|-------------|--------------|------------|")
            
            for _, row in high_risk.iterrows():
                risk_level = "Very High" if row['avg_amount'] > 50000 else "High" if row['avg_amount'] > 10000 else "Moderate"
                report.append(f"| {row['payment_category'][:40]} | ${row['avg_amount']:,.2f} | "
                             f"{format_currency(row['total_amount'])} | {risk_level} |")
            report.append("")
    
    report.append("### Concentration Risk Analysis")
    report.append("")
    report.append("Payment concentration presents both efficiency opportunities and compliance risks:")
    report.append("")
    report.append("1. **Provider Concentration**: Top 1% of recipients receive disproportionate payment share")
    report.append("2. **Geographic Concentration**: Payments cluster in major metropolitan areas with academic medical centers")
    report.append("3. **Product Concentration**: HYDRUS Microstent alone accounts for $9.88M in associated payments")
    report.append("")
    
    report.append("### Recommendations")
    report.append("")
    report.append("1. **Enhanced Monitoring**: Focus compliance reviews on providers receiving >$10,000 annually")
    report.append("2. **Specialty Audits**: Prioritize ophthalmology and optometry payment reviews")
    report.append("3. **Product Training Documentation**: Ensure educational value documentation for high-value surgical products")
    report.append("4. **Multi-Year Relationship Review**: Audit providers with 4+ years of continuous payments")
    report.append("")
    
    report.append("---")
    report.append("")
    
    # Conclusion
    report.append("## Conclusion")
    report.append("")
    report.append("Alcon's provider engagement strategy demonstrates sophisticated market segmentation within the specialized ")
    report.append("ophthalmology and optometry markets. The combination of broad market coverage (47,511 providers) with ")
    report.append(f"concentrated high-value relationships (top 100 providers receiving {concentration.iloc[2]['cumulative_pct_of_total']:.1f}% of payments) ")
    report.append("reflects a dual strategy of market presence and innovation partnership.")
    report.append("")
    report.append("Key strategic insights include:")
    report.append("")
    report.append("1. **Market Leadership**: Near-complete coverage of US eye care providers")
    report.append("2. **Product Differentiation**: Clear payment patterns distinguishing surgical innovation from vision care")
    report.append("3. **Relationship Persistence**: Strong multi-year engagement indicating successful provider partnerships")
    report.append("4. **Specialty Focus**: 87.9% payment concentration in core eye care specialties")
    report.append("")
    report.append("As the global leader in eye care, Alcon's payment patterns provide a benchmark for specialized medical device ")
    report.append("and pharmaceutical engagement strategies, demonstrating how focused market leadership translates into ")
    report.append("provider relationship patterns distinct from diversified healthcare companies.")
    report.append("")
    
    # Methodology
    report.append("---")
    report.append("")
    report.append("## Methodology")
    report.append("")
    report.append("This analysis utilized the CMS Open Payments database, examining all payments from:")
    report.append("- Alcon Vision LLC (primary operating entity)")
    report.append("- Alcon Research LLC (R&D partnerships)")
    report.append("- Alcon Puerto Rico Inc (manufacturing operations)")
    report.append("")
    report.append("Data processing included:")
    report.append("- Deduplication of provider records by NPI")
    report.append("- Categorization of providers by credential type")
    report.append("- Multi-year relationship tracking")
    report.append("- Product-specialty alignment analysis")
    report.append("- Geographic distribution mapping")
    report.append("")
    report.append(f"Analysis period: {summary['data_years']}")
    report.append(f"Total payments analyzed: {format_number(total_payments)}")
    report.append(f"Total providers: {format_number(total_providers)}")
    report.append("")
    
    # Footer
    report.append("---")
    report.append("")
    report.append(f"*Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
    report.append("")
    report.append("*For questions or additional analysis, contact: Vincent Wan (vwan@conflixis.com)*")
    
    # Write report to file
    report_content = "\n".join(report)
    report_path = DOCS_DIR / "alcon_deep_dive_analysis.md"
    with open(report_path, "w") as f:
        f.write(report_content)
    
    logger.info(f"Deep dive report saved to: {report_path}")
    return report_path

def main():
    """Main execution function"""
    try:
        logger.info("=" * 80)
        logger.info("GENERATING ALCON DEEP DIVE REPORT")
        logger.info("=" * 80)
        
        # Generate markdown report
        report_path = generate_deep_dive_report()
        
        logger.info("=" * 80)
        logger.info("DEEP DIVE REPORT GENERATION COMPLETE")
        logger.info(f"Report location: {report_path}")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"Error during report generation: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)