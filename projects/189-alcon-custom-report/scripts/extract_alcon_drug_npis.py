#!/usr/bin/env python3
"""
Extract distinct NPIs for healthcare providers who received payments from Alcon for drug products.
This script reads the classified products CSV to identify all drug products and queries BigQuery
to extract payment data.
"""

import pandas as pd
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import logging
import json
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_drug_products(classified_file):
    """
    Load drug products from the classified CSV file
    """
    logger.info(f"Loading drug products from {classified_file}")
    df = pd.read_csv(classified_file)
    
    # Filter for drug products only
    drug_products = df[df['product_type'] == 'Drug']['name_of_drug_or_biological_or_device_or_medical_supply_1'].tolist()
    
    # Remove empty/null values
    drug_products = [p for p in drug_products if pd.notna(p) and str(p).strip()]
    
    logger.info(f"Found {len(drug_products)} drug products: {drug_products}")
    return drug_products

def build_drug_query(drug_products):
    """
    Build SQL query to extract full payment records for drug products
    """
    # Create LIKE conditions for each drug product
    drug_conditions = []
    for drug in drug_products:
        # Use lowercase for case-insensitive matching
        drug_lower = str(drug).lower().strip()
        drug_conditions.append(f"LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%{drug_lower}%'")
    
    # Add conditions for NULL or empty drug names
    drug_conditions.append("name_of_drug_or_biological_or_device_or_medical_supply_1 IS NULL")
    drug_conditions.append("TRIM(name_of_drug_or_biological_or_device_or_medical_supply_1) = ''")
    
    # Join conditions with OR
    drug_filter = " OR ".join(drug_conditions)
    
    # Build complete query - get ALL payment records matching criteria
    query = f"""
    SELECT *
    FROM `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized`
    WHERE LOWER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%alcon%'
    AND ({drug_filter})
    AND covered_recipient_npi IS NOT NULL
    ORDER BY covered_recipient_npi
    """
    
    return query

def get_payment_summary_query(drug_products):
    """
    Build query to get payment summary by drug product
    """
    drug_conditions = []
    for drug in drug_products:
        drug_lower = str(drug).lower().strip()
        drug_conditions.append(f"LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%{drug_lower}%'")
    
    drug_filter = " OR ".join(drug_conditions)
    
    query = f"""
    SELECT 
        name_of_drug_or_biological_or_device_or_medical_supply_1 as drug_name,
        COUNT(DISTINCT covered_recipient_npi) as unique_npis,
        COUNT(*) as payment_count,
        SUM(total_amount_of_payment_usdollars) as total_payment_amount,
        AVG(total_amount_of_payment_usdollars) as avg_payment_amount,
        MIN(total_amount_of_payment_usdollars) as min_payment,
        MAX(total_amount_of_payment_usdollars) as max_payment
    FROM `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized`
    WHERE LOWER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%alcon%'
    AND ({drug_filter})
    AND covered_recipient_npi IS NOT NULL
    GROUP BY 1
    ORDER BY total_payment_amount DESC
    """
    
    return query

def execute_query(client, query, job_config=None):
    """
    Execute BigQuery query and return results as DataFrame
    """
    logger.info("Executing BigQuery query...")
    query_job = client.query(query, job_config=job_config)
    df = query_job.to_dataframe()
    logger.info(f"Query returned {len(df)} rows")
    return df

def main():
    """
    Main execution function
    """
    # Load environment variables
    load_dotenv()
    
    # Set up paths
    base_dir = "/home/incent/conflixis-data-projects/projects/189-alcon-custom-report"
    classified_file = os.path.join(base_dir, "data/outputs/alcon_products_classified.csv")
    output_dir = os.path.join(base_dir, "data/outputs")
    
    # Initialize BigQuery client with service account credentials
    logger.info("Initializing BigQuery client with service account...")
    
    try:
        # Get service account JSON from environment
        service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
        if not service_account_json:
            raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
        
        # Parse the JSON string
        service_account_info = json.loads(service_account_json)
        
        # Create credentials
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/bigquery']
        )
        
        # Initialize client with credentials
        client = bigquery.Client(
            credentials=credentials,
            project=service_account_info.get('project_id')
        )
        logger.info("BigQuery client initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        raise
    
    # Load drug products from classified file
    drug_products = load_drug_products(classified_file)
    
    # Build and execute main query
    logger.info("Building main query for drug NPIs...")
    main_query = build_drug_query(drug_products)
    
    # Save query for reference
    query_file = os.path.join(output_dir, "alcon_drug_npi_query.sql")
    with open(query_file, 'w') as f:
        f.write(main_query)
    logger.info(f"Query saved to {query_file}")
    
    # Execute main query
    df_payments = execute_query(client, main_query)
    
    # Save full payment records
    payments_file = os.path.join(output_dir, "alcon_drug_payments.csv")
    df_payments.to_csv(payments_file, index=False)
    logger.info(f"Payment records saved to {payments_file} - Total records: {len(df_payments)}")
    
    # Also extract and save distinct NPIs
    distinct_npis = df_payments['covered_recipient_npi'].unique()
    npi_df = pd.DataFrame({'covered_recipient_npi': distinct_npis})
    npi_file = os.path.join(output_dir, "alcon_drug_npis.csv")
    npi_df.to_csv(npi_file, index=False)
    logger.info(f"Distinct NPI list saved to {npi_file} - Total NPIs: {len(distinct_npis)}")
    
    # Calculate summary statistics
    total_payment_amount = df_payments['total_amount_of_payment_usdollars'].sum() if 'total_amount_of_payment_usdollars' in df_payments.columns else 0
    
    # Print summary statistics
    print("\n" + "="*60)
    print("ALCON DRUG PAYMENT EXTRACTION SUMMARY")
    print("="*60)
    print(f"Total Drug Products Searched: {len(drug_products)}")
    print(f"Drug Products: {', '.join([d for d in drug_products if 'NULL' not in str(d) and 'TRIM' not in str(d)])}")
    print(f"Total Payment Records: {len(df_payments):,}")
    print(f"Total Unique NPIs: {len(distinct_npis):,}")
    if total_payment_amount > 0:
        print(f"Total Payment Amount: ${total_payment_amount:,.2f}")
    print("="*60)
    print(f"\nPayment records saved to: {payments_file}")
    print(f"NPI list saved to: {npi_file}")

def generate_analysis_report(df_npis, df_summary, distinct_npis, drug_products, output_dir):
    """
    Generate comprehensive analysis report
    """
    logger.info("Generating analysis report...")
    
    report_content = f"""# Alcon Drug Payment Analysis Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Executive Summary

- **Total Drug Products Analyzed**: {len(drug_products)}
- **Total Unique Healthcare Providers (NPIs)**: {len(distinct_npis):,}
- **Total Payment Records**: {len(df_npis):,}
- **Total Payment Amount**: ${df_npis['total_payment_amount'].sum():,.2f}
- **Average Payment per Provider**: ${df_npis.groupby('covered_recipient_npi')['total_payment_amount'].sum().mean():,.2f}

## Drug Products Included

The following Alcon drug products were included in this analysis:
{chr(10).join([f"- {drug}" for drug in sorted(drug_products)])}

## Payment Summary by Drug Product

| Drug Name | Unique NPIs | Total Payments | Total Amount | Average Payment |
|-----------|-------------|----------------|--------------|-----------------|
"""
    
    for _, row in df_summary.iterrows():
        report_content += f"| {row['drug_name']} | {row['unique_npis']:,} | {row['payment_count']:,} | ${row['total_payment_amount']:,.2f} | ${row['avg_payment_amount']:,.2f} |\n"
    
    # Add specialty breakdown
    specialty_breakdown = df_npis.groupby('covered_recipient_specialty_1').agg({
        'covered_recipient_npi': 'nunique',
        'total_payment_amount': 'sum'
    }).sort_values('total_payment_amount', ascending=False).head(10)
    
    report_content += f"""

## Top 10 Specialties by Payment Amount

| Specialty | Unique NPIs | Total Amount |
|-----------|-------------|--------------|
"""
    
    for specialty, row in specialty_breakdown.iterrows():
        report_content += f"| {specialty} | {row['covered_recipient_npi']:,} | ${row['total_payment_amount']:,.2f} |\n"
    
    # Add geographic distribution
    state_breakdown = df_npis.groupby('covered_recipient_state').agg({
        'covered_recipient_npi': 'nunique',
        'total_payment_amount': 'sum'
    }).sort_values('total_payment_amount', ascending=False).head(10)
    
    report_content += f"""

## Top 10 States by Payment Amount

| State | Unique NPIs | Total Amount |
|-------|-------------|--------------|
"""
    
    for state, row in state_breakdown.iterrows():
        report_content += f"| {state} | {row['covered_recipient_npi']:,} | ${row['total_payment_amount']:,.2f} |\n"
    
    # Add top recipients
    top_recipients = df_npis.groupby(['covered_recipient_npi', 'covered_recipient_first_name', 
                                      'covered_recipient_last_name', 'covered_recipient_specialty_1']).agg({
        'total_payment_amount': 'sum'
    }).sort_values('total_payment_amount', ascending=False).head(10)
    
    report_content += f"""

## Top 10 Recipients by Total Payment Amount

| NPI | Name | Specialty | Total Amount |
|-----|------|-----------|--------------|
"""
    
    for (npi, first, last, specialty), row in top_recipients.iterrows():
        report_content += f"| {npi} | {first} {last} | {specialty} | ${row['total_payment_amount']:,.2f} |\n"
    
    report_content += f"""

## Data Sources

- **BigQuery Table**: `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized`
- **Manufacturer Filter**: Alcon (case-insensitive)
- **Product Classification**: Based on research and FDA classifications
- **Analysis Date**: {datetime.now().strftime('%Y-%m-%d')}

## Notes

1. All drug products are included based on the classification in `alcon_products_classified.csv`
2. Case-insensitive matching is used for both manufacturer and product names
3. Only records with valid NPIs are included in the analysis
4. Payment amounts are aggregated across all payment types and dates

---
*Report generated by extract_alcon_drug_npis.py*
"""
    
    report_file = os.path.join(output_dir, "alcon_drug_analysis_report.md")
    with open(report_file, 'w') as f:
        f.write(report_content)
    
    logger.info(f"Analysis report saved to {report_file}")

if __name__ == "__main__":
    main()