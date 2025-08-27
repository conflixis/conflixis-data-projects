#!/usr/bin/env python3
"""
Analyze Open Payments from Alcon to Healthcare Providers
DA-179: Alcon Analysis - Payment Analysis

This script analyzes payments from Alcon (ophthalmology/eye care manufacturer)
to healthcare providers using the Open Payments database.
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
PROJECT_DIR = Path("/home/incent/conflixis-data-projects/projects/179-alcon-analysis")
OUTPUT_DIR = PROJECT_DIR / "data" / "processed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# BigQuery configuration
PROJECT_ID = "data-analytics-389803"
DATASET_ID = "conflixis_agent"
OP_TABLE = f"{PROJECT_ID}.{DATASET_ID}.op_general_all_aggregate_static"

# Alcon manufacturer names (from discovery)
ALCON_MANUFACTURERS = [
    "Alcon Vision LLC",
    "Alcon Research LLC", 
    "Alcon Puerto Rico Inc"
]

def create_bigquery_client():
    """Create BigQuery client with credentials"""
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
    
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def analyze_overall_metrics(client):
    """Analyze overall Alcon payment metrics"""
    logger.info("Analyzing overall Alcon payment metrics...")
    
    query = f"""
    WITH alcon_payments AS (
        SELECT 
            *,
            CASE 
                WHEN applicable_manufacturer_or_applicable_gpo_making_payment_name = 'Alcon Vision LLC' THEN 'Alcon Vision'
                WHEN applicable_manufacturer_or_applicable_gpo_making_payment_name = 'Alcon Research LLC' THEN 'Alcon Research'
                WHEN applicable_manufacturer_or_applicable_gpo_making_payment_name = 'Alcon Puerto Rico Inc' THEN 'Alcon Puerto Rico'
                ELSE 'Other Alcon'
            END as alcon_entity
        FROM `{OP_TABLE}`
        WHERE applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
    )
    SELECT
        COUNT(DISTINCT covered_recipient_profile_id) as unique_providers,
        COUNT(DISTINCT covered_recipient_npi) as unique_npis,
        COUNT(*) as total_payments,
        SUM(total_amount_of_payment_usdollars) as total_amount,
        AVG(total_amount_of_payment_usdollars) as avg_payment,
        APPROX_QUANTILES(total_amount_of_payment_usdollars, 2)[OFFSET(1)] as median_payment,
        MAX(total_amount_of_payment_usdollars) as max_payment,
        MIN(EXTRACT(YEAR FROM date_of_payment)) as min_year,
        MAX(EXTRACT(YEAR FROM date_of_payment)) as max_year,
        COUNT(DISTINCT alcon_entity) as distinct_alcon_entities
    FROM alcon_payments
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Log summary
    if not results.empty:
        logger.info(f"Total unique providers receiving Alcon payments: {results['unique_providers'].iloc[0]:,}")
        logger.info(f"Total payments: {results['total_payments'].iloc[0]:,}")
        logger.info(f"Total amount: ${results['total_amount'].iloc[0]:,.2f}")
        logger.info(f"Average payment: ${results['avg_payment'].iloc[0]:,.2f}")
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results.to_csv(OUTPUT_DIR / f"alcon_overall_metrics_{timestamp}.csv", index=False)
    
    return results

def analyze_payment_categories(client):
    """Analyze payment categories"""
    logger.info("Analyzing payment categories...")
    
    query = f"""
    SELECT 
        nature_of_payment_or_transfer_of_value as payment_category,
        COUNT(*) as payment_count,
        COUNT(DISTINCT covered_recipient_profile_id) as unique_providers,
        SUM(total_amount_of_payment_usdollars) as total_amount,
        AVG(total_amount_of_payment_usdollars) as avg_amount,
        APPROX_QUANTILES(total_amount_of_payment_usdollars, 2)[OFFSET(1)] as median_amount,
        STDDEV(total_amount_of_payment_usdollars) as stddev_amount
    FROM `{OP_TABLE}`
    WHERE applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
    GROUP BY payment_category
    ORDER BY total_amount DESC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results.to_csv(OUTPUT_DIR / f"alcon_payment_categories_{timestamp}.csv", index=False)
    
    return results

def analyze_yearly_trends(client):
    """Analyze yearly payment trends"""
    logger.info("Analyzing yearly trends...")
    
    query = f"""
    SELECT 
        EXTRACT(YEAR FROM date_of_payment) as year,
        COUNT(DISTINCT covered_recipient_profile_id) as unique_providers,
        COUNT(*) as payment_count,
        SUM(total_amount_of_payment_usdollars) as total_amount,
        AVG(total_amount_of_payment_usdollars) as avg_amount,
        APPROX_QUANTILES(total_amount_of_payment_usdollars, 2)[OFFSET(1)] as median_amount
    FROM `{OP_TABLE}`
    WHERE applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
    GROUP BY year
    ORDER BY year DESC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results.to_csv(OUTPUT_DIR / f"alcon_yearly_trends_{timestamp}.csv", index=False)
    
    return results

def analyze_top_recipients(client, top_n=100):
    """Analyze top payment recipients"""
    logger.info(f"Analyzing top {top_n} payment recipients...")
    
    query = f"""
    WITH provider_totals AS (
        SELECT 
            covered_recipient_profile_id,
            covered_recipient_npi,
            covered_recipient_first_name,
            covered_recipient_last_name,
            covered_recipient_specialty_1 as specialty,
            COUNT(*) as payment_count,
            SUM(total_amount_of_payment_usdollars) as total_amount,
            AVG(total_amount_of_payment_usdollars) as avg_payment,
            COUNT(DISTINCT EXTRACT(YEAR FROM date_of_payment)) as years_received
        FROM `{OP_TABLE}`
        WHERE applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
        GROUP BY 
            covered_recipient_profile_id,
            covered_recipient_npi,
            covered_recipient_first_name,
            covered_recipient_last_name,
            covered_recipient_specialty_1
    )
    SELECT *
    FROM provider_totals
    ORDER BY total_amount DESC
    LIMIT @top_n
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS),
            bigquery.ScalarQueryParameter("top_n", "INT64", top_n)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results.to_csv(OUTPUT_DIR / f"alcon_top_recipients_{timestamp}.csv", index=False)
    
    return results

def analyze_specialty_distribution(client):
    """Analyze payment distribution by medical specialty"""
    logger.info("Analyzing specialty distribution...")
    
    query = f"""
    SELECT 
        covered_recipient_specialty_1 as specialty,
        COUNT(DISTINCT covered_recipient_profile_id) as unique_providers,
        COUNT(*) as payment_count,
        SUM(total_amount_of_payment_usdollars) as total_amount,
        AVG(total_amount_of_payment_usdollars) as avg_amount,
        APPROX_QUANTILES(total_amount_of_payment_usdollars, 2)[OFFSET(1)] as median_amount
    FROM `{OP_TABLE}`
    WHERE applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
        AND covered_recipient_specialty_1 IS NOT NULL
    GROUP BY specialty
    ORDER BY total_amount DESC
    LIMIT 20
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results.to_csv(OUTPUT_DIR / f"alcon_specialty_distribution_{timestamp}.csv", index=False)
    
    return results

def analyze_product_payments(client):
    """Analyze payments by Alcon products"""
    logger.info("Analyzing product-specific payments...")
    
    query = f"""
    SELECT 
        name_of_drug_or_biological_or_device_or_medical_supply_1 as product_name,
        COUNT(DISTINCT covered_recipient_profile_id) as unique_providers,
        COUNT(*) as payment_count,
        SUM(total_amount_of_payment_usdollars) as total_amount,
        AVG(total_amount_of_payment_usdollars) as avg_amount,
        APPROX_QUANTILES(total_amount_of_payment_usdollars, 2)[OFFSET(1)] as median_amount
    FROM `{OP_TABLE}`
    WHERE applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
        AND name_of_drug_or_biological_or_device_or_medical_supply_1 IS NOT NULL
        AND name_of_drug_or_biological_or_device_or_medical_supply_1 != ''
    GROUP BY product_name
    ORDER BY total_amount DESC
    LIMIT 30
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results.to_csv(OUTPUT_DIR / f"alcon_product_payments_{timestamp}.csv", index=False)
    
    return results

def analyze_geographic_distribution(client):
    """Analyze geographic distribution of payments"""
    logger.info("Analyzing geographic distribution...")
    
    query = f"""
    WITH provider_locations AS (
        SELECT 
            p.covered_recipient_profile_id,
            p.total_amount_of_payment_usdollars,
            -- Extract state from physician affiliations
            ARRAY_AGG(
                aff.AFFILIATED_HQ_STATE 
                ORDER BY aff.PRIMARY_AFFILIATED_FACILITY_FLAG DESC
                LIMIT 1
            )[SAFE_OFFSET(0)] as provider_state
        FROM `{OP_TABLE}` p,
        UNNEST(physician.affiliations) as aff
        WHERE p.applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
        GROUP BY p.covered_recipient_profile_id, p.total_amount_of_payment_usdollars
    )
    SELECT 
        provider_state as state,
        COUNT(DISTINCT covered_recipient_profile_id) as unique_providers,
        COUNT(*) as payment_count,
        SUM(total_amount_of_payment_usdollars) as total_amount,
        AVG(total_amount_of_payment_usdollars) as avg_amount
    FROM provider_locations
    WHERE provider_state IS NOT NULL
    GROUP BY state
    ORDER BY total_amount DESC
    LIMIT 30
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results.to_csv(OUTPUT_DIR / f"alcon_geographic_distribution_{timestamp}.csv", index=False)
    
    return results

def main():
    """Main execution function"""
    try:
        logger.info("=" * 80)
        logger.info("ALCON OPEN PAYMENTS ANALYSIS")
        logger.info("=" * 80)
        
        # Create BigQuery client
        client = create_bigquery_client()
        
        # Run analyses
        overall_metrics = analyze_overall_metrics(client)
        payment_categories = analyze_payment_categories(client)
        yearly_trends = analyze_yearly_trends(client)
        top_recipients = analyze_top_recipients(client)
        specialty_dist = analyze_specialty_distribution(client)
        product_payments = analyze_product_payments(client)
        geographic_dist = analyze_geographic_distribution(client)
        
        logger.info("=" * 80)
        logger.info("ANALYSIS COMPLETE")
        logger.info(f"Results saved to: {OUTPUT_DIR}")
        logger.info("=" * 80)
        
        # Create summary report
        summary = {
            "execution_time": datetime.now().isoformat(),
            "manufacturers_analyzed": ALCON_MANUFACTURERS,
            "total_providers": int(overall_metrics['unique_providers'].iloc[0]),
            "total_payments": int(overall_metrics['total_payments'].iloc[0]),
            "total_amount": float(overall_metrics['total_amount'].iloc[0]),
            "data_years": f"{int(overall_metrics['min_year'].iloc[0])}-{int(overall_metrics['max_year'].iloc[0])}"
        }
        
        with open(OUTPUT_DIR / "alcon_analysis_summary.json", "w") as f:
            json.dump(summary, f, indent=2)
        
        return True
        
    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)