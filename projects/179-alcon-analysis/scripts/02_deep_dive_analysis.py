#!/usr/bin/env python3
"""
Deep Dive Analysis for Alcon Provider Engagement
DA-179: Alcon Analysis - Enhanced Provider Analysis

This script performs detailed analysis of Alcon's provider engagement patterns,
payment distributions, and relationship metrics.
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

# Alcon manufacturer names
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

def analyze_provider_types(client):
    """Analyze payments by provider type (MD, DO, NP, PA, OD)"""
    logger.info("Analyzing provider types...")
    
    query = f"""
    WITH provider_types AS (
        SELECT 
            covered_recipient_profile_id,
            covered_recipient_npi,
            covered_recipient_first_name,
            covered_recipient_last_name,
            covered_recipient_specialty_1,
            CASE 
                WHEN LOWER(covered_recipient_specialty_1) LIKE '%physician assistant%' THEN 'PA'
                WHEN LOWER(covered_recipient_specialty_1) LIKE '%nurse practitioner%' THEN 'NP'
                WHEN LOWER(covered_recipient_specialty_1) LIKE '%optometr%' THEN 'OD'
                WHEN LOWER(covered_recipient_specialty_1) LIKE '%ophthalmolog%' THEN 'MD_Ophth'
                WHEN covered_recipient_specialty_1 IS NOT NULL THEN 'MD_Other'
                ELSE 'Unknown'
            END as provider_type,
            total_amount_of_payment_usdollars,
            nature_of_payment_or_transfer_of_value,
            date_of_payment
        FROM `{OP_TABLE}`
        WHERE applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
    )
    SELECT 
        provider_type,
        COUNT(DISTINCT covered_recipient_profile_id) as unique_providers,
        COUNT(*) as payment_count,
        SUM(total_amount_of_payment_usdollars) as total_amount,
        AVG(total_amount_of_payment_usdollars) as avg_payment,
        APPROX_QUANTILES(total_amount_of_payment_usdollars, 2)[OFFSET(1)] as median_payment,
        MAX(total_amount_of_payment_usdollars) as max_payment
    FROM provider_types
    GROUP BY provider_type
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
    results.to_csv(OUTPUT_DIR / f"alcon_provider_types_{timestamp}.csv", index=False)
    
    return results

def analyze_consecutive_year_recipients(client):
    """Identify providers receiving payments in multiple years"""
    logger.info("Analyzing consecutive year recipients...")
    
    query = f"""
    WITH yearly_recipients AS (
        SELECT 
            covered_recipient_profile_id,
            covered_recipient_npi,
            covered_recipient_first_name,
            covered_recipient_last_name,
            covered_recipient_specialty_1,
            EXTRACT(YEAR FROM date_of_payment) as year,
            SUM(total_amount_of_payment_usdollars) as yearly_amount
        FROM `{OP_TABLE}`
        WHERE applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
        GROUP BY 1,2,3,4,5,6
    ),
    multi_year AS (
        SELECT 
            covered_recipient_profile_id,
            covered_recipient_npi,
            covered_recipient_first_name,
            covered_recipient_last_name,
            covered_recipient_specialty_1,
            COUNT(DISTINCT year) as years_received,
            STRING_AGG(CAST(year AS STRING), ', ' ORDER BY year) as years_list,
            SUM(yearly_amount) as total_amount,
            AVG(yearly_amount) as avg_yearly_amount,
            MIN(year) as first_year,
            MAX(year) as last_year
        FROM yearly_recipients
        GROUP BY 1,2,3,4,5
        HAVING COUNT(DISTINCT year) >= 2
    )
    SELECT *
    FROM multi_year
    ORDER BY years_received DESC, total_amount DESC
    LIMIT 500
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results.to_csv(OUTPUT_DIR / f"alcon_multi_year_recipients_{timestamp}.csv", index=False)
    
    return results

def analyze_payment_concentration(client):
    """Analyze payment concentration (Pareto analysis)"""
    logger.info("Analyzing payment concentration...")
    
    query = f"""
    WITH provider_totals AS (
        SELECT 
            covered_recipient_profile_id,
            covered_recipient_npi,
            covered_recipient_first_name,
            covered_recipient_last_name,
            covered_recipient_specialty_1,
            SUM(total_amount_of_payment_usdollars) as total_amount,
            COUNT(*) as payment_count
        FROM `{OP_TABLE}`
        WHERE applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
        GROUP BY 1,2,3,4,5
    ),
    ranked_providers AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (ORDER BY total_amount DESC) as rank,
            SUM(total_amount) OVER (ORDER BY total_amount DESC) as cumulative_amount,
            SUM(total_amount) OVER () as grand_total
        FROM provider_totals
    )
    SELECT 
        CASE 
            WHEN rank <= 10 THEN 'Top 10'
            WHEN rank <= 50 THEN 'Top 11-50'
            WHEN rank <= 100 THEN 'Top 51-100'
            WHEN rank <= 500 THEN 'Top 101-500'
            WHEN rank <= 1000 THEN 'Top 501-1000'
            ELSE 'Beyond 1000'
        END as provider_tier,
        COUNT(*) as provider_count,
        SUM(total_amount) as tier_total_amount,
        AVG(total_amount) as avg_amount_per_provider,
        MAX(cumulative_amount/grand_total * 100) as cumulative_pct_of_total
    FROM ranked_providers
    GROUP BY provider_tier
    ORDER BY MIN(rank)
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results.to_csv(OUTPUT_DIR / f"alcon_payment_concentration_{timestamp}.csv", index=False)
    
    return results

def analyze_high_value_relationships(client, threshold=10000):
    """Identify and analyze high-value provider relationships"""
    logger.info(f"Analyzing high-value relationships (>${threshold})...")
    
    query = f"""
    WITH provider_details AS (
        SELECT 
            covered_recipient_profile_id,
            covered_recipient_npi,
            covered_recipient_first_name,
            covered_recipient_last_name,
            covered_recipient_specialty_1,
            nature_of_payment_or_transfer_of_value,
            name_of_drug_or_biological_or_device_or_medical_supply_1 as product,
            SUM(total_amount_of_payment_usdollars) as category_amount,
            COUNT(*) as payment_count,
            COUNT(DISTINCT EXTRACT(YEAR FROM date_of_payment)) as years_active
        FROM `{OP_TABLE}`
        WHERE applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
        GROUP BY 1,2,3,4,5,6,7
    ),
    provider_summary AS (
        SELECT 
            covered_recipient_profile_id,
            covered_recipient_npi,
            covered_recipient_first_name,
            covered_recipient_last_name,
            covered_recipient_specialty_1,
            SUM(category_amount) as total_amount,
            COUNT(DISTINCT nature_of_payment_or_transfer_of_value) as payment_categories,
            COUNT(DISTINCT product) as products_associated,
            MAX(years_active) as max_years_active,
            STRING_AGG(DISTINCT nature_of_payment_or_transfer_of_value, ', ' ORDER BY nature_of_payment_or_transfer_of_value LIMIT 5) as top_categories
        FROM provider_details
        GROUP BY 1,2,3,4,5
        HAVING SUM(category_amount) >= @threshold
    )
    SELECT *
    FROM provider_summary
    ORDER BY total_amount DESC
    LIMIT 200
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS),
            bigquery.ScalarQueryParameter("threshold", "FLOAT64", threshold)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results.to_csv(OUTPUT_DIR / f"alcon_high_value_relationships_{timestamp}.csv", index=False)
    
    return results

def analyze_product_specialist_engagement(client):
    """Analyze which products are associated with which specialties"""
    logger.info("Analyzing product-specialty engagement patterns...")
    
    query = f"""
    WITH product_specialty AS (
        SELECT 
            name_of_drug_or_biological_or_device_or_medical_supply_1 as product,
            covered_recipient_specialty_1 as specialty,
            COUNT(DISTINCT covered_recipient_profile_id) as providers,
            COUNT(*) as payments,
            SUM(total_amount_of_payment_usdollars) as total_amount
        FROM `{OP_TABLE}`
        WHERE applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
            AND name_of_drug_or_biological_or_device_or_medical_supply_1 IS NOT NULL
            AND name_of_drug_or_biological_or_device_or_medical_supply_1 != ''
        GROUP BY 1,2
        HAVING providers >= 5  -- Filter for meaningful patterns
    ),
    ranked AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY total_amount DESC) as specialty_rank,
            ROW_NUMBER() OVER (PARTITION BY specialty ORDER BY total_amount DESC) as product_rank
        FROM product_specialty
    )
    SELECT 
        product,
        specialty,
        providers,
        payments,
        total_amount,
        specialty_rank,
        product_rank
    FROM ranked
    WHERE specialty_rank <= 5 OR product_rank <= 5
    ORDER BY product, specialty_rank
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results.to_csv(OUTPUT_DIR / f"alcon_product_specialty_engagement_{timestamp}.csv", index=False)
    
    return results

def analyze_state_penetration(client):
    """Analyze market penetration by state"""
    logger.info("Analyzing state-level market penetration...")
    
    query = f"""
    WITH state_metrics AS (
        SELECT 
            aff.AFFILIATED_HQ_STATE as state,
            COUNT(DISTINCT p.covered_recipient_profile_id) as unique_providers,
            COUNT(*) as payment_count,
            SUM(p.total_amount_of_payment_usdollars) as total_amount,
            AVG(p.total_amount_of_payment_usdollars) as avg_payment,
            COUNT(DISTINCT p.name_of_drug_or_biological_or_device_or_medical_supply_1) as unique_products
        FROM `{OP_TABLE}` p,
        UNNEST(physician.affiliations) as aff
        WHERE p.applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
            AND aff.PRIMARY_AFFILIATED_FACILITY_FLAG = 'Y'
            AND aff.AFFILIATED_HQ_STATE IS NOT NULL
        GROUP BY state
    )
    SELECT 
        state,
        unique_providers,
        payment_count,
        total_amount,
        avg_payment,
        unique_products,
        ROUND(total_amount / unique_providers, 2) as avg_per_provider,
        ROUND(100.0 * unique_providers / SUM(unique_providers) OVER (), 2) as pct_of_providers,
        ROUND(100.0 * total_amount / SUM(total_amount) OVER (), 2) as pct_of_payments
    FROM state_metrics
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
    results.to_csv(OUTPUT_DIR / f"alcon_state_penetration_{timestamp}.csv", index=False)
    
    return results

def main():
    """Main execution function"""
    try:
        logger.info("=" * 80)
        logger.info("ALCON DEEP DIVE ANALYSIS")
        logger.info("=" * 80)
        
        # Create BigQuery client
        client = create_bigquery_client()
        
        # Run analyses
        provider_types = analyze_provider_types(client)
        logger.info(f"Provider types analyzed: {len(provider_types)} categories")
        
        multi_year = analyze_consecutive_year_recipients(client)
        logger.info(f"Multi-year recipients: {len(multi_year)} providers")
        
        concentration = analyze_payment_concentration(client)
        logger.info(f"Payment concentration tiers: {len(concentration)}")
        
        high_value = analyze_high_value_relationships(client)
        logger.info(f"High-value relationships: {len(high_value)} providers")
        
        product_specialty = analyze_product_specialist_engagement(client)
        logger.info(f"Product-specialty patterns: {len(product_specialty)} combinations")
        
        state_penetration = analyze_state_penetration(client)
        logger.info(f"State penetration: {len(state_penetration)} states")
        
        logger.info("=" * 80)
        logger.info("DEEP DIVE ANALYSIS COMPLETE")
        logger.info(f"Results saved to: {OUTPUT_DIR}")
        logger.info("=" * 80)
        
        # Create summary metrics
        summary = {
            "execution_time": datetime.now().isoformat(),
            "provider_types_analyzed": len(provider_types),
            "multi_year_recipients": len(multi_year),
            "high_value_relationships": len(high_value),
            "states_covered": len(state_penetration),
            "product_specialty_patterns": len(product_specialty)
        }
        
        with open(OUTPUT_DIR / "alcon_deep_dive_summary.json", "w") as f:
            json.dump(summary, f, indent=2)
        
        return True
        
    except Exception as e:
        logger.error(f"Error during deep dive analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)