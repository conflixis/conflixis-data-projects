#!/usr/bin/env python3
"""
Analyze Open Payments to healthcare providers
This script is generalized from the Corewell Health analysis
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import json
import yaml
import os
import sys
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Setup paths
TEMPLATE_DIR = Path(__file__).parent.parent
sys.path.append(str(TEMPLATE_DIR))

# Load environment variables
load_dotenv(TEMPLATE_DIR / '.env')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration"""
    with open(TEMPLATE_DIR / 'CONFIG.yaml', 'r') as f:
        config = yaml.safe_load(f)
    return config

def create_bigquery_client(config):
    """Create BigQuery client with credentials"""
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
    
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=['https://www.googleapis.com/auth/bigquery']
    )
    
    project_id = config['bigquery']['project_id']
    return bigquery.Client(project=project_id, credentials=credentials)

def upload_npis_to_bigquery(client, config):
    """Upload provider NPIs to BigQuery for analysis"""
    logger.info("Uploading provider NPIs to BigQuery...")
    
    # Read NPI file
    npi_file = TEMPLATE_DIR / config['health_system']['npi_file']
    df_npis = pd.read_csv(npi_file)
    
    # Ensure NPI is string
    df_npis['NPI'] = df_npis['NPI'].astype(str)
    
    # Create table reference
    dataset_id = config['bigquery']['dataset']
    table_name = config['bigquery']['tables']['provider_npis'].replace('{{SHORT_NAME}}', 
                                                                      config['health_system']['short_name'])
    table_id = f"{config['bigquery']['project_id']}.{dataset_id}.{table_name}"
    
    # Upload to BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    
    job = client.load_table_from_dataframe(df_npis, table_id, job_config=job_config)
    job.result()
    
    logger.info(f"✅ Uploaded {len(df_npis)} NPIs to {table_id}")
    return table_id, len(df_npis)

def analyze_overall_payments(client, config, npi_table):
    """Analyze overall payment metrics"""
    
    op_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['open_payments']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH provider_payments AS (
        SELECT 
            op.covered_recipient_npi AS NPI,
            op.program_year,
            op.date_of_payment,
            op.nature_of_payment_or_transfer_of_value AS payment_category,
            op.total_amount_of_payment_usdollars AS payment_amount,
            op.applicable_manufacturer_or_applicable_gpo_making_payment_name AS manufacturer,
            op.name_of_drug_or_biological_or_device_or_medical_supply_1 AS product_name,
            n.Full_Name,
            n.Primary_Specialty
        FROM `{op_table}` op
        INNER JOIN `{npi_table}` n
            ON CAST(op.covered_recipient_npi AS STRING) = n.NPI
        WHERE op.program_year BETWEEN {start_year} AND {end_year}
    )
    SELECT 
        COUNT(DISTINCT NPI) as unique_providers_paid,
        COUNT(*) as total_transactions,
        SUM(payment_amount) as total_payments,
        AVG(payment_amount) as avg_payment,
        MEDIAN(payment_amount) OVER() as median_payment,
        MAX(payment_amount) as max_payment,
        MIN(payment_amount) as min_payment,
        STDDEV(payment_amount) as stddev_payment
    FROM provider_payments
    """
    
    logger.info(f"Analyzing overall payment metrics for {config['health_system']['name']}...")
    df = client.query(query).to_dataframe()
    
    if not df.empty:
        row = df.iloc[0]
        logger.info(f"  Unique Providers Receiving Payments: {row['unique_providers_paid']:,.0f}")
        logger.info(f"  Total Transactions: {row['total_transactions']:,.0f}")
        logger.info(f"  Total Payments: ${row['total_payments']:,.2f}")
        logger.info(f"  Average Payment: ${row['avg_payment']:,.2f}")
        logger.info(f"  Max Single Payment: ${row['max_payment']:,.2f}")
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'op_overall_metrics_{timestamp}.csv'
    df.to_csv(output_file, index=False)
    
    return df

def analyze_payment_categories(client, config, npi_table):
    """Analyze payments by category"""
    
    op_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['open_payments']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH provider_payments AS (
        SELECT 
            op.covered_recipient_npi AS NPI,
            op.nature_of_payment_or_transfer_of_value AS payment_category,
            op.total_amount_of_payment_usdollars AS payment_amount
        FROM `{op_table}` op
        INNER JOIN `{npi_table}` n
            ON CAST(op.covered_recipient_npi AS STRING) = n.NPI
        WHERE op.program_year BETWEEN {start_year} AND {end_year}
    )
    SELECT 
        payment_category,
        COUNT(DISTINCT NPI) as unique_providers,
        COUNT(*) as transaction_count,
        SUM(payment_amount) as total_amount,
        AVG(payment_amount) as avg_amount,
        MAX(payment_amount) as max_amount,
        ROUND(SUM(payment_amount) * 100.0 / SUM(SUM(payment_amount)) OVER(), 2) as pct_of_total
    FROM provider_payments
    GROUP BY payment_category
    ORDER BY total_amount DESC
    """
    
    logger.info("Analyzing payment categories...")
    df = client.query(query).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'op_payment_categories_{timestamp}.csv'
    df.to_csv(output_file, index=False)
    
    # Log top categories
    logger.info("\nTop Payment Categories:")
    for idx, row in df.head(5).iterrows():
        logger.info(f"  {row['payment_category']}: ${row['total_amount']:,.0f} ({row['pct_of_total']}%)")
    
    return df

def analyze_yearly_trends(client, config, npi_table):
    """Analyze payment trends by year"""
    
    op_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['open_payments']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH provider_payments AS (
        SELECT 
            op.covered_recipient_npi AS NPI,
            op.program_year,
            op.total_amount_of_payment_usdollars AS payment_amount
        FROM `{op_table}` op
        INNER JOIN `{npi_table}` n
            ON CAST(op.covered_recipient_npi AS STRING) = n.NPI
        WHERE op.program_year BETWEEN {start_year} AND {end_year}
    )
    SELECT 
        program_year,
        COUNT(DISTINCT NPI) as unique_providers,
        COUNT(*) as transaction_count,
        SUM(payment_amount) as total_payments,
        AVG(payment_amount) as avg_payment,
        MAX(payment_amount) as max_payment
    FROM provider_payments
    GROUP BY program_year
    ORDER BY program_year
    """
    
    logger.info("Analyzing yearly payment trends...")
    df = client.query(query).to_dataframe()
    
    # Calculate year-over-year growth
    df['yoy_growth'] = df['total_payments'].pct_change() * 100
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'op_yearly_trends_{timestamp}.csv'
    df.to_csv(output_file, index=False)
    
    # Log trends
    logger.info("\nYearly Payment Trends:")
    for idx, row in df.iterrows():
        growth = f" ({row['yoy_growth']:.1f}% YoY)" if pd.notna(row['yoy_growth']) else ""
        logger.info(f"  {row['program_year']}: ${row['total_payments']:,.0f}{growth}")
    
    return df

def analyze_top_manufacturers(client, config, npi_table):
    """Analyze top pharmaceutical/device manufacturers"""
    
    op_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['open_payments']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH provider_payments AS (
        SELECT 
            op.covered_recipient_npi AS NPI,
            op.applicable_manufacturer_or_applicable_gpo_making_payment_name AS manufacturer,
            op.total_amount_of_payment_usdollars AS payment_amount
        FROM `{op_table}` op
        INNER JOIN `{npi_table}` n
            ON CAST(op.covered_recipient_npi AS STRING) = n.NPI
        WHERE op.program_year BETWEEN {start_year} AND {end_year}
    )
    SELECT 
        manufacturer,
        COUNT(DISTINCT NPI) as unique_providers,
        COUNT(*) as transaction_count,
        SUM(payment_amount) as total_payments,
        AVG(payment_amount) as avg_payment,
        MAX(payment_amount) as max_payment
    FROM provider_payments
    GROUP BY manufacturer
    ORDER BY total_payments DESC
    LIMIT 20
    """
    
    logger.info("Analyzing top manufacturers...")
    df = client.query(query).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'op_top_manufacturers_{timestamp}.csv'
    df.to_csv(output_file, index=False)
    
    # Log top manufacturers
    logger.info("\nTop 5 Manufacturers by Total Payments:")
    for idx, row in df.head(5).iterrows():
        logger.info(f"  {row['manufacturer']}: ${row['total_payments']:,.0f} to {row['unique_providers']} providers")
    
    return df

def analyze_payment_tiers(client, config, npi_table):
    """Analyze providers by payment tier"""
    
    op_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['open_payments']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH provider_totals AS (
        SELECT 
            n.NPI,
            n.Full_Name,
            n.Primary_Specialty,
            SUM(op.total_amount_of_payment_usdollars) AS total_payments,
            COUNT(*) as transaction_count
        FROM `{npi_table}` n
        LEFT JOIN `{op_table}` op
            ON CAST(op.covered_recipient_npi AS STRING) = n.NPI
            AND op.program_year BETWEEN {start_year} AND {end_year}
        GROUP BY n.NPI, n.Full_Name, n.Primary_Specialty
    )
    SELECT 
        CASE 
            WHEN total_payments IS NULL OR total_payments = 0 THEN 'No Payments'
            WHEN total_payments <= 100 THEN '$1-100'
            WHEN total_payments <= 500 THEN '$101-500'
            WHEN total_payments <= 1000 THEN '$501-1,000'
            WHEN total_payments <= 5000 THEN '$1,001-5,000'
            WHEN total_payments <= 10000 THEN '$5,001-10,000'
            ELSE '$10,000+'
        END AS payment_tier,
        COUNT(*) as provider_count,
        AVG(total_payments) as avg_total_payment,
        SUM(total_payments) as tier_total_payments
    FROM provider_totals
    GROUP BY payment_tier
    ORDER BY 
        CASE payment_tier
            WHEN 'No Payments' THEN 0
            WHEN '$1-100' THEN 1
            WHEN '$101-500' THEN 2
            WHEN '$501-1,000' THEN 3
            WHEN '$1,001-5,000' THEN 4
            WHEN '$5,001-10,000' THEN 5
            ELSE 6
        END
    """
    
    logger.info("Analyzing payment tiers...")
    df = client.query(query).to_dataframe()
    
    # Calculate percentages
    total_providers = df['provider_count'].sum()
    df['pct_of_providers'] = (df['provider_count'] / total_providers * 100).round(2)
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'op_payment_tiers_{timestamp}.csv'
    df.to_csv(output_file, index=False)
    
    # Log tiers
    logger.info("\nProvider Distribution by Payment Tier:")
    for idx, row in df.iterrows():
        logger.info(f"  {row['payment_tier']}: {row['provider_count']:,} providers ({row['pct_of_providers']}%)")
    
    return df

def analyze_consecutive_years(client, config, npi_table):
    """Analyze providers receiving payments in consecutive years"""
    
    op_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['open_payments']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH yearly_providers AS (
        SELECT 
            CAST(op.covered_recipient_npi AS STRING) AS NPI,
            op.program_year,
            SUM(op.total_amount_of_payment_usdollars) AS yearly_payment
        FROM `{op_table}` op
        INNER JOIN `{npi_table}` n
            ON CAST(op.covered_recipient_npi AS STRING) = n.NPI
        WHERE op.program_year BETWEEN {start_year} AND {end_year}
        GROUP BY NPI, program_year
    ),
    consecutive_counts AS (
        SELECT 
            NPI,
            COUNT(DISTINCT program_year) as years_with_payments,
            MIN(program_year) as first_year,
            MAX(program_year) as last_year,
            SUM(yearly_payment) as total_all_years
        FROM yearly_providers
        GROUP BY NPI
    )
    SELECT 
        years_with_payments,
        COUNT(*) as provider_count,
        AVG(total_all_years) as avg_total_payment,
        MAX(total_all_years) as max_total_payment,
        MIN(total_all_years) as min_total_payment
    FROM consecutive_counts
    GROUP BY years_with_payments
    ORDER BY years_with_payments DESC
    """
    
    logger.info("Analyzing consecutive year payment patterns...")
    df = client.query(query).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'op_consecutive_years_{timestamp}.csv'
    df.to_csv(output_file, index=False)
    
    # Log results
    logger.info("\nProviders by Years with Payments:")
    for idx, row in df.iterrows():
        logger.info(f"  {row['years_with_payments']} years: {row['provider_count']:,} providers (avg ${row['avg_total_payment']:,.0f})")
    
    # Find providers with payments every year
    total_years = end_year - start_year + 1
    all_years_providers = df[df['years_with_payments'] == total_years]
    if not all_years_providers.empty:
        count = all_years_providers.iloc[0]['provider_count']
        logger.info(f"\n⚠️  {count:,} providers received payments EVERY year ({start_year}-{end_year})")
    
    return df

def generate_summary_stats(config, total_npis):
    """Generate summary statistics from all analyses"""
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Read the saved files to compile summary
    processed_dir = TEMPLATE_DIR / 'data' / 'processed'
    
    # Find most recent files
    overall_file = max(processed_dir.glob('op_overall_metrics_*.csv'))
    df_overall = pd.read_csv(overall_file)
    
    # Calculate key metrics
    providers_paid = df_overall.iloc[0]['unique_providers_paid']
    total_payments = df_overall.iloc[0]['total_payments']
    pct_providers_paid = (providers_paid / total_npis * 100)
    
    # Create summary
    summary = {
        'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'health_system': config['health_system']['name'],
        'analysis_period': f"{config['analysis']['start_year']}-{config['analysis']['end_year']}",
        'total_providers': total_npis,
        'providers_receiving_payments': providers_paid,
        'percent_providers_paid': round(pct_providers_paid, 1),
        'total_payments': total_payments,
        'avg_payment_per_provider': total_payments / providers_paid if providers_paid > 0 else 0,
        'total_transactions': df_overall.iloc[0]['total_transactions']
    }
    
    # Save summary
    summary_file = TEMPLATE_DIR / 'data' / 'processed' / f'op_analysis_summary_{timestamp}.json'
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    
    logger.info(f"\n📊 Summary saved to: {summary_file}")
    
    return summary

def main():
    """Main execution function"""
    
    logger.info("=" * 60)
    logger.info("OPEN PAYMENTS ANALYSIS")
    logger.info("=" * 60)
    
    # Load configuration
    config = load_config()
    logger.info(f"Health System: {config['health_system']['name']}")
    logger.info(f"Analysis Period: {config['analysis']['start_year']}-{config['analysis']['end_year']}")
    
    # Create BigQuery client
    client = create_bigquery_client(config)
    
    # Upload NPIs to BigQuery
    npi_table, total_npis = upload_npis_to_bigquery(client, config)
    
    # Run analyses
    logger.info("\n" + "=" * 60)
    logger.info("Running Analyses...")
    logger.info("=" * 60)
    
    analyze_overall_payments(client, config, npi_table)
    analyze_payment_categories(client, config, npi_table)
    analyze_yearly_trends(client, config, npi_table)
    analyze_top_manufacturers(client, config, npi_table)
    analyze_payment_tiers(client, config, npi_table)
    analyze_consecutive_years(client, config, npi_table)
    
    # Generate summary
    summary = generate_summary_stats(config, total_npis)
    
    logger.info("\n" + "=" * 60)
    logger.info("ANALYSIS COMPLETE")
    logger.info("=" * 60)
    logger.info(f"✅ {summary['percent_providers_paid']}% of providers received industry payments")
    logger.info(f"✅ Total payments: ${summary['total_payments']:,.0f}")
    logger.info(f"✅ Results saved to: data/processed/")

if __name__ == "__main__":
    main()