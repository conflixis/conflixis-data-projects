#!/usr/bin/env python3
"""
Analyze Open Payments to Corewell Health Providers
DA-175: Corewell Health Open Payments Report - Payment Analysis

This script analyzes payments from pharmaceutical and medical device manufacturers
to Corewell Health providers using the Open Payments database.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import json
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define paths
PROJECT_DIR = Path("/home/incent/conflixis-data-projects/projects/175-corewell-health-analysis")
OUTPUT_DIR = PROJECT_DIR / "op-payment-report" / "data" / "processed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# BigQuery configuration
PROJECT_ID = "data-analytics-389803"
DATASET_ID = "conflixis_agent"
NPI_TABLE = f"{PROJECT_ID}.{DATASET_ID}.corewell_health_npis"
OP_TABLE = f"{PROJECT_ID}.{DATASET_ID}.op_general_all_aggregate_static"

def create_bigquery_client():
    """Create BigQuery client with credentials"""
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
    
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=['https://www.googleapis.com/auth/bigquery']
    )
    
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)

def analyze_overall_payments(client):
    """Analyze overall payment metrics for Corewell providers"""
    
    query = f"""
    WITH corewell_payments AS (
        SELECT 
            op.covered_recipient_npi AS NPI,
            op.program_year,
            op.date_of_payment,
            op.nature_of_payment_or_transfer_of_value,
            op.total_amount_of_payment_usdollars,
            op.applicable_manufacturer_or_applicable_gpo_making_payment_name AS manufacturer,
            op.Company_Type,
            op.name_of_drug_or_biological_or_device_or_medical_supply_1 AS drug_name,
            cs.Primary_Specialty,
            cs.Full_Name
        FROM `{OP_TABLE}` op
        INNER JOIN `{NPI_TABLE}` cs
            ON CAST(op.covered_recipient_npi AS STRING) = cs.NPI
        WHERE op.program_year BETWEEN 2020 AND 2024
    )
    SELECT 
        COUNT(DISTINCT NPI) as unique_providers,
        COUNT(*) as total_transactions,
        SUM(total_amount_of_payment_usdollars) as total_payments,
        AVG(total_amount_of_payment_usdollars) as avg_payment,
        MAX(total_amount_of_payment_usdollars) as max_payment,
        MIN(program_year) as min_year,
        MAX(program_year) as max_year
    FROM corewell_payments
    """
    
    logger.info("Analyzing overall payment metrics...")
    df = client.query(query).to_dataframe()
    
    if not df.empty:
        row = df.iloc[0]
        logger.info(f"  Unique Providers: {row['unique_providers']:,.0f}")
        logger.info(f"  Total Transactions: {row['total_transactions']:,.0f}")
        logger.info(f"  Total Payments: ${row['total_payments']:,.2f}")
        logger.info(f"  Average Payment: ${row['avg_payment']:,.2f}")
        logger.info(f"  Max Payment: ${row['max_payment']:,.2f}")
    
    return df

def analyze_payments_by_year(client):
    """Analyze payment trends by year"""
    
    query = f"""
    WITH corewell_payments AS (
        SELECT 
            op.covered_recipient_npi AS NPI,
            op.program_year,
            op.total_amount_of_payment_usdollars,
            cs.Primary_Specialty
        FROM `{OP_TABLE}` op
        INNER JOIN `{NPI_TABLE}` cs
            ON CAST(op.covered_recipient_npi AS STRING) = cs.NPI
        WHERE op.program_year BETWEEN 2020 AND 2024
    )
    SELECT 
        program_year,
        COUNT(DISTINCT NPI) as unique_providers,
        COUNT(*) as transactions,
        SUM(total_amount_of_payment_usdollars) as total_payments,
        AVG(total_amount_of_payment_usdollars) as avg_payment,
        PERCENTILE_CONT(total_amount_of_payment_usdollars, 0.5) OVER (PARTITION BY program_year) as median_payment,
        PERCENTILE_CONT(total_amount_of_payment_usdollars, 0.95) OVER (PARTITION BY program_year) as p95_payment
    FROM corewell_payments
    GROUP BY program_year, total_amount_of_payment_usdollars
    ORDER BY program_year
    """
    
    logger.info("\nAnalyzing payments by year...")
    df = client.query(query).to_dataframe()
    
    # Aggregate to year level
    yearly = df.groupby('program_year').agg({
        'unique_providers': 'max',
        'transactions': 'sum',
        'total_payments': 'sum',
        'avg_payment': 'mean',
        'median_payment': 'max',
        'p95_payment': 'max'
    }).reset_index()
    
    logger.info("\nYearly Payment Trends:")
    for _, row in yearly.iterrows():
        logger.info(f"  {row['program_year']}: ${row['total_payments']:,.0f} to {row['unique_providers']:,.0f} providers")
    
    return yearly

def analyze_payment_categories(client):
    """Analyze payments by nature/category"""
    
    query = f"""
    WITH corewell_payments AS (
        SELECT 
            op.nature_of_payment_or_transfer_of_value AS payment_type,
            op.total_amount_of_payment_usdollars,
            op.covered_recipient_npi AS NPI
        FROM `{OP_TABLE}` op
        INNER JOIN `{NPI_TABLE}` cs
            ON CAST(op.covered_recipient_npi AS STRING) = cs.NPI
        WHERE op.program_year BETWEEN 2020 AND 2024
    )
    SELECT 
        payment_type,
        COUNT(DISTINCT NPI) as unique_providers,
        COUNT(*) as transactions,
        SUM(total_amount_of_payment_usdollars) as total_amount,
        AVG(total_amount_of_payment_usdollars) as avg_amount,
        MAX(total_amount_of_payment_usdollars) as max_amount
    FROM corewell_payments
    GROUP BY payment_type
    ORDER BY total_amount DESC
    """
    
    logger.info("\nAnalyzing payment categories...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nTop Payment Categories:")
    for i, row in df.head(10).iterrows():
        logger.info(f"  {row['payment_type']}: ${row['total_amount']:,.0f} ({row['transactions']:,} transactions)")
    
    return df

def analyze_top_manufacturers(client):
    """Identify top manufacturers making payments"""
    
    query = f"""
    WITH corewell_payments AS (
        SELECT 
            op.applicable_manufacturer_or_applicable_gpo_making_payment_name AS manufacturer,
            op.Company_Type,
            op.total_amount_of_payment_usdollars,
            op.covered_recipient_npi AS NPI
        FROM `{OP_TABLE}` op
        INNER JOIN `{NPI_TABLE}` cs
            ON CAST(op.covered_recipient_npi AS STRING) = cs.NPI
        WHERE op.program_year BETWEEN 2020 AND 2024
    )
    SELECT 
        manufacturer,
        Company_Type,
        COUNT(DISTINCT NPI) as unique_providers,
        COUNT(*) as transactions,
        SUM(total_amount_of_payment_usdollars) as total_payments,
        AVG(total_amount_of_payment_usdollars) as avg_payment
    FROM corewell_payments
    WHERE manufacturer IS NOT NULL
    GROUP BY manufacturer, Company_Type
    ORDER BY total_payments DESC
    LIMIT 30
    """
    
    logger.info("\nAnalyzing top manufacturers...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nTop 10 Manufacturers by Total Payments:")
    for i, row in df.head(10).iterrows():
        logger.info(f"  {row['manufacturer']} ({row['Company_Type']}): ${row['total_payments']:,.0f}")
    
    return df

def analyze_high_value_payments(client):
    """Analyze high-value payments and meal patterns"""
    
    query = f"""
    WITH corewell_payments AS (
        SELECT 
            op.covered_recipient_npi AS NPI,
            op.program_year,
            op.nature_of_payment_or_transfer_of_value AS payment_type,
            op.total_amount_of_payment_usdollars AS amount,
            op.applicable_manufacturer_or_applicable_gpo_making_payment_name AS manufacturer,
            cs.Full_Name,
            cs.Primary_Specialty
        FROM `{OP_TABLE}` op
        INNER JOIN `{NPI_TABLE}` cs
            ON CAST(op.covered_recipient_npi AS STRING) = cs.NPI
        WHERE op.program_year BETWEEN 2020 AND 2024
    )
    SELECT 
        CASE 
            WHEN amount >= 10000 THEN '$10,000+'
            WHEN amount >= 5000 THEN '$5,000-$10,000'
            WHEN amount >= 1000 THEN '$1,000-$5,000'
            WHEN amount >= 500 THEN '$500-$1,000'
            WHEN amount >= 200 THEN '$200-$500'
            WHEN amount >= 100 THEN '$100-$200'
            ELSE '<$100'
        END as payment_tier,
        COUNT(DISTINCT NPI) as unique_providers,
        COUNT(*) as transactions,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM corewell_payments
    GROUP BY payment_tier
    ORDER BY 
        CASE payment_tier
            WHEN '$10,000+' THEN 1
            WHEN '$5,000-$10,000' THEN 2
            WHEN '$1,000-$5,000' THEN 3
            WHEN '$500-$1,000' THEN 4
            WHEN '$200-$500' THEN 5
            WHEN '$100-$200' THEN 6
            ELSE 7
        END
    """
    
    logger.info("\nAnalyzing payment value tiers...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nPayment Tier Distribution:")
    for _, row in df.iterrows():
        logger.info(f"  {row['payment_tier']}: {row['transactions']:,} transactions, ${row['total_amount']:,.0f}")
    
    return df

def analyze_consecutive_year_payments(client):
    """Identify providers receiving payments in consecutive years"""
    
    query = f"""
    WITH provider_years AS (
        SELECT 
            CAST(op.covered_recipient_npi AS STRING) AS NPI,
            op.program_year,
            cs.Full_Name,
            cs.Primary_Specialty,
            COUNT(*) as transactions,
            SUM(op.total_amount_of_payment_usdollars) as total_amount
        FROM `{OP_TABLE}` op
        INNER JOIN `{NPI_TABLE}` cs
            ON CAST(op.covered_recipient_npi AS STRING) = cs.NPI
        WHERE op.program_year BETWEEN 2020 AND 2024
        GROUP BY 1, 2, 3, 4
    ),
    consecutive_years AS (
        SELECT 
            NPI,
            Full_Name,
            Primary_Specialty,
            COUNT(DISTINCT program_year) as years_with_payments,
            STRING_AGG(CAST(program_year AS STRING), ', ' ORDER BY program_year) as years,
            SUM(transactions) as total_transactions,
            SUM(total_amount) as total_amount_all_years
        FROM provider_years
        GROUP BY 1, 2, 3
    )
    SELECT 
        years_with_payments,
        COUNT(DISTINCT NPI) as provider_count,
        AVG(total_amount_all_years) as avg_total_amount,
        SUM(total_amount_all_years) as sum_total_amount
    FROM consecutive_years
    GROUP BY years_with_payments
    ORDER BY years_with_payments DESC
    """
    
    logger.info("\nAnalyzing consecutive year payment patterns...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nConsecutive Year Payment Patterns:")
    for _, row in df.iterrows():
        logger.info(f"  {row['years_with_payments']} years: {row['provider_count']:,} providers, ${row['sum_total_amount']:,.0f} total")
    
    return df

def main():
    """Main execution function"""
    logger.info("=" * 80)
    logger.info("COREWELL HEALTH OPEN PAYMENTS ANALYSIS")
    logger.info("=" * 80)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        # Create BigQuery client
        logger.info("Connecting to BigQuery...")
        client = create_bigquery_client()
        
        # Run analyses
        results = {}
        
        # Overall metrics
        results['overall'] = analyze_overall_payments(client)
        results['overall'].to_csv(OUTPUT_DIR / f"op_overall_metrics_{timestamp}.csv", index=False)
        
        # Yearly trends
        results['yearly'] = analyze_payments_by_year(client)
        results['yearly'].to_csv(OUTPUT_DIR / f"op_yearly_trends_{timestamp}.csv", index=False)
        
        # Payment categories
        results['categories'] = analyze_payment_categories(client)
        results['categories'].to_csv(OUTPUT_DIR / f"op_payment_categories_{timestamp}.csv", index=False)
        
        # Top manufacturers
        results['manufacturers'] = analyze_top_manufacturers(client)
        results['manufacturers'].to_csv(OUTPUT_DIR / f"op_top_manufacturers_{timestamp}.csv", index=False)
        
        # High-value payments
        results['high_value'] = analyze_high_value_payments(client)
        results['high_value'].to_csv(OUTPUT_DIR / f"op_payment_tiers_{timestamp}.csv", index=False)
        
        # Consecutive year patterns
        results['consecutive'] = analyze_consecutive_year_payments(client)
        results['consecutive'].to_csv(OUTPUT_DIR / f"op_consecutive_years_{timestamp}.csv", index=False)
        
        logger.info(f"\nAll results saved to: {OUTPUT_DIR}")
        logger.info("=" * 80)
        logger.info("Analysis Complete!")
        
        return results
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        raise

if __name__ == "__main__":
    results = main()