#!/usr/bin/env python3
"""
Generate comprehensive report for Alcon Open Payments Analysis
DA-179: Alcon Analysis - Report Generation
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

def generate_markdown_report():
    """Generate comprehensive markdown report"""
    logger.info("Generating comprehensive report...")
    
    # Load analysis results
    overall_metrics = load_latest_file("alcon_overall_metrics_*.csv")
    payment_categories = load_latest_file("alcon_payment_categories_*.csv")
    yearly_trends = load_latest_file("alcon_yearly_trends_*.csv")
    top_recipients = load_latest_file("alcon_top_recipients_*.csv")
    specialty_dist = load_latest_file("alcon_specialty_distribution_*.csv")
    product_payments = load_latest_file("alcon_product_payments_*.csv")
    geographic_dist = load_latest_file("alcon_geographic_distribution_*.csv")
    
    # Load summary JSON
    with open(DATA_DIR / "alcon_analysis_summary.json", "r") as f:
        summary = json.load(f)
    
    # Start building the report
    report = []
    report.append("# Alcon Open Payments Analysis Report")
    report.append(f"\n**Generated:** {datetime.now().strftime('%B %d, %Y')}")
    report.append(f"**Jira Ticket:** DA-179")
    report.append(f"**Analysis Period:** {summary['data_years']}")
    report.append("")
    
    # Executive Summary
    report.append("## Executive Summary")
    report.append("")
    report.append("This report analyzes payments from Alcon Inc., a global leader in eye care and ophthalmology products, ")
    report.append("to healthcare providers through the CMS Open Payments database. Alcon manufactures and markets ")
    report.append("surgical equipment, contact lenses, and pharmaceutical products for eye care.")
    report.append("")
    
    # Key Metrics
    report.append("### Key Metrics")
    report.append("")
    report.append(f"- **Total Providers Receiving Payments:** {summary['total_providers']:,}")
    report.append(f"- **Total Number of Payments:** {summary['total_payments']:,}")
    report.append(f"- **Total Payment Amount:** {format_currency(summary['total_amount'])}")
    report.append(f"- **Average Payment:** ${overall_metrics['avg_payment'].iloc[0]:,.2f}")
    report.append(f"- **Median Payment:** ${overall_metrics['median_payment'].iloc[0]:,.2f}")
    report.append(f"- **Maximum Single Payment:** {format_currency(overall_metrics['max_payment'].iloc[0])}")
    report.append("")
    
    # Payment Trends
    report.append("## Payment Trends Over Time")
    report.append("")
    if yearly_trends is not None and not yearly_trends.empty:
        report.append("| Year | Providers | Payments | Total Amount | Avg Payment |")
        report.append("|------|-----------|----------|--------------|-------------|")
        for _, row in yearly_trends.iterrows():
            report.append(f"| {int(row['year'])} | {row['unique_providers']:,} | {row['payment_count']:,} | "
                         f"{format_currency(row['total_amount'])} | ${row['avg_amount']:,.2f} |")
        report.append("")
        
        # Trend Analysis
        yoy_growth = ((yearly_trends.iloc[0]['total_amount'] - yearly_trends.iloc[-1]['total_amount']) 
                     / yearly_trends.iloc[-1]['total_amount'] * 100)
        report.append(f"**Trend Analysis:** Payment volume has {'increased' if yoy_growth > 0 else 'decreased'} "
                     f"by {abs(yoy_growth):.1f}% from {int(yearly_trends.iloc[-1]['year'])} to {int(yearly_trends.iloc[0]['year'])}.")
        report.append("")
    
    # Payment Categories
    report.append("## Payment Categories")
    report.append("")
    report.append("Alcon's payments span various categories, reflecting different types of provider engagement:")
    report.append("")
    if payment_categories is not None and not payment_categories.empty:
        top_categories = payment_categories.head(10)
        report.append("| Category | Payments | Providers | Total Amount | Avg Payment |")
        report.append("|----------|----------|-----------|--------------|-------------|")
        for _, row in top_categories.iterrows():
            report.append(f"| {row['payment_category']} | {row['payment_count']:,} | "
                         f"{row['unique_providers']:,} | {format_currency(row['total_amount'])} | "
                         f"${row['avg_amount']:,.2f} |")
        report.append("")
        
        # Highlight key categories
        top_cat = payment_categories.iloc[0]
        report.append(f"**Key Finding:** The largest payment category is **{top_cat['payment_category']}**, ")
        report.append(f"representing {format_currency(top_cat['total_amount'])} ")
        report.append(f"({top_cat['total_amount']/summary['total_amount']*100:.1f}% of total payments).")
        report.append("")
    
    # Medical Specialties
    report.append("## Provider Specialty Distribution")
    report.append("")
    report.append("Analysis of payments by medical specialty reveals Alcon's focus on eye care professionals:")
    report.append("")
    if specialty_dist is not None and not specialty_dist.empty:
        top_specialties = specialty_dist.head(10)
        report.append("| Specialty | Providers | Total Payments |")
        report.append("|-----------|-----------|----------------|")
        for _, row in top_specialties.iterrows():
            specialty_name = row['specialty'] if pd.notna(row['specialty']) else 'Not Specified'
            # Truncate long specialty names
            if isinstance(specialty_name, str) and '|' in specialty_name:
                specialty_name = specialty_name.split('|')[-1].strip()
            report.append(f"| {str(specialty_name)[:50]} | {row['unique_providers']:,} | "
                         f"{format_currency(row['total_amount'])} |")
        report.append("")
        
        # Calculate ophthalmology focus
        ophth_specialties = specialty_dist[specialty_dist['specialty'].str.contains('Ophth', case=False, na=False)]
        if not ophth_specialties.empty:
            ophth_total = ophth_specialties['total_amount'].sum()
            ophth_providers = ophth_specialties['unique_providers'].sum()
            report.append(f"**Ophthalmology Focus:** {ophth_providers:,} ophthalmology specialists received ")
            report.append(f"{format_currency(ophth_total)} ({ophth_total/summary['total_amount']*100:.1f}% of total), ")
            report.append("confirming Alcon's specialization in eye care.")
            report.append("")
    
    # Product Analysis
    report.append("## Product-Specific Payments")
    report.append("")
    report.append("Top Alcon products associated with provider payments:")
    report.append("")
    if product_payments is not None and not product_payments.empty:
        top_products = product_payments[product_payments['product_name'].notna()].head(15)
        report.append("| Product | Providers | Payments | Total Amount |")
        report.append("|---------|-----------|----------|--------------|")
        for _, row in top_products.iterrows():
            product = row['product_name'] if row['product_name'] else 'Unspecified'
            report.append(f"| {product[:40]} | {row['unique_providers']:,} | "
                         f"{row['payment_count']:,} | {format_currency(row['total_amount'])} |")
        report.append("")
        
        # Highlight key products
        report.append("**Key Products:**")
        report.append("- **HYDRUS Microstent**: Minimally invasive glaucoma surgery device")
        report.append("- **TOTAL30**: Monthly replacement contact lenses")
        report.append("- **Clareon**: Advanced IOL (intraocular lens) for cataract surgery")
        report.append("- **Constellation**: Vitreoretinal surgical system")
        report.append("")
    
    # Geographic Distribution
    report.append("## Geographic Distribution")
    report.append("")
    if geographic_dist is not None and not geographic_dist.empty:
        top_states = geographic_dist.head(10)
        report.append("Top 10 states by payment volume:")
        report.append("")
        report.append("| State | Providers | Total Amount |")
        report.append("|-------|-----------|--------------|")
        for _, row in top_states.iterrows():
            report.append(f"| {row['state']} | {row['unique_providers']:,} | "
                         f"{format_currency(row['total_amount'])} |")
        report.append("")
    
    # Top Recipients
    report.append("## High-Value Provider Relationships")
    report.append("")
    if top_recipients is not None and not top_recipients.empty:
        # Aggregate statistics
        top_10 = top_recipients.head(10)
        top_10_total = top_10['total_amount'].sum()
        report.append(f"The top 10 payment recipients received {format_currency(top_10_total)} ")
        report.append(f"({top_10_total/summary['total_amount']*100:.1f}% of total payments).")
        report.append("")
        
        # Multi-year recipients
        multi_year = top_recipients[top_recipients['years_received'] >= 3]
        if not multi_year.empty:
            report.append(f"**Long-term Relationships:** {len(multi_year)} providers have received ")
            report.append(f"payments for 3 or more years, indicating sustained partnerships.")
            report.append("")
    
    # Risk and Compliance Considerations
    report.append("## Risk and Compliance Considerations")
    report.append("")
    report.append("### Payment Concentration")
    report.append("")
    
    # Calculate concentration metrics
    if top_recipients is not None and not top_recipients.empty:
        top_100_total = top_recipients.head(100)['total_amount'].sum()
        concentration_pct = top_100_total / summary['total_amount'] * 100
        report.append(f"- Top 100 providers represent {concentration_pct:.1f}% of total payment value")
        report.append(f"- Average payment to top 100: {format_currency(top_100_total/100)}")
        report.append("")
    
    report.append("### High-Risk Payment Categories")
    report.append("")
    if payment_categories is not None:
        high_value_cats = payment_categories[payment_categories['avg_amount'] > 1000]
        if not high_value_cats.empty:
            report.append("Payment categories with average values exceeding $1,000:")
            report.append("")
            for _, row in high_value_cats.iterrows():
                report.append(f"- **{row['payment_category']}**: Avg ${row['avg_amount']:,.2f}")
            report.append("")
    
    # Recommendations
    report.append("## Recommendations")
    report.append("")
    report.append("1. **Enhanced Monitoring**: Focus compliance monitoring on ophthalmology specialists ")
    report.append("   receiving consulting fees and royalty payments")
    report.append("")
    report.append("2. **Product-Specific Analysis**: Conduct detailed analysis of HYDRUS Microstent ")
    report.append("   and other high-payment surgical devices")
    report.append("")
    report.append("3. **Geographic Patterns**: Investigate concentration of payments in top states ")
    report.append("   for potential regional influence patterns")
    report.append("")
    report.append("4. **Long-term Relationships**: Review multi-year payment recipients for ")
    report.append("   appropriate disclosure and potential conflicts of interest")
    report.append("")
    
    # Methodology
    report.append("## Methodology")
    report.append("")
    report.append("This analysis utilized the CMS Open Payments database, focusing on all payments from:")
    report.append("- Alcon Vision LLC")
    report.append("- Alcon Research LLC")
    report.append("- Alcon Puerto Rico Inc")
    report.append("")
    report.append(f"Data covers the period from {summary['data_years']} and includes general payments ")
    report.append("to physicians and teaching hospitals. Research payments were analyzed separately.")
    report.append("")
    
    # Footer
    report.append("---")
    report.append(f"*Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
    report.append("")
    report.append("*For questions or additional analysis, contact: Vincent Wan (vwan@conflixis.com)*")
    
    # Write report to file
    report_content = "\n".join(report)
    report_path = DOCS_DIR / "alcon_open_payments_report.md"
    with open(report_path, "w") as f:
        f.write(report_content)
    
    logger.info(f"Report saved to: {report_path}")
    return report_path

def main():
    """Main execution function"""
    try:
        logger.info("=" * 80)
        logger.info("GENERATING ALCON ANALYSIS REPORT")
        logger.info("=" * 80)
        
        # Generate markdown report
        report_path = generate_markdown_report()
        
        logger.info("=" * 80)
        logger.info("REPORT GENERATION COMPLETE")
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