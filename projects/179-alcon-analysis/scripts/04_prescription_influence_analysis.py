#!/usr/bin/env python3
"""
Analyze Payment-Prescription Correlations for Alcon Products
DA-179: Alcon Analysis - Payment Influence Analysis

This script analyzes the correlation between Open Payments and prescribing patterns
for Alcon products, adapting the methodology from payment influence analysis.
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
RX_TABLE = f"{PROJECT_ID}.{DATASET_ID}.PHYSICIAN_RX_2020_2024"

# Alcon manufacturers
ALCON_MANUFACTURERS = [
    "Alcon Vision LLC",
    "Alcon Research LLC", 
    "Alcon Puerto Rico Inc"
]

# Key Alcon products identified in prescription data
ALCON_PRODUCTS = [
    'RHOPRESSA', 'ROCKLATAN', 'CIPRODEX', 'SIMBRINZA', 'AZOPT',
    'DUREZOL', 'PAZEO', 'EYSUVIS', 'TRAVATAN Z', 'PATADAY',
    'SYSTANE', 'VIGAMOX', 'NEVANAC', 'ILEVRO', 'COMBIGAN'
]

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

def analyze_drug_payment_correlation(client, drug_name):
    """
    Analyze correlation between payments and prescribing for a specific Alcon drug.
    Implements 180-day window methodology.
    """
    
    query = f"""
    WITH 
    -- Get Open Payments for this drug from Alcon
    drug_payments AS (
        SELECT 
            CAST(op.covered_recipient_npi AS STRING) AS NPI,
            op.covered_recipient_specialty_1 as specialty,
            op.program_year,
            op.date_of_payment,
            op.total_amount_of_payment_usdollars as payment_amount,
            op.applicable_manufacturer_or_applicable_gpo_making_payment_name AS manufacturer
        FROM `{OP_TABLE}` op
        WHERE op.program_year BETWEEN 2020 AND 2024
            AND op.applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
            AND op.name_of_drug_or_biological_or_device_or_medical_supply_1 IS NOT NULL
            AND UPPER(op.name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE UPPER('%{drug_name}%')
    ),
    -- Get prescriptions for this drug
    drug_prescriptions AS (
        SELECT 
            CAST(rx.NPI AS STRING) AS NPI,
            rx.CLAIM_YEAR,
            rx.CLAIM_MONTH,
            rx.PAYMENTS as rx_payments,
            rx.PRESCRIPTIONS,
            rx.UNIQUE_PATIENTS,
            DATE(CAST(rx.CLAIM_YEAR AS INT64), 
                 CASE rx.CLAIM_MONTH
                    WHEN 'January' THEN 1 WHEN 'February' THEN 2 WHEN 'March' THEN 3
                    WHEN 'April' THEN 4 WHEN 'May' THEN 5 WHEN 'June' THEN 6
                    WHEN 'July' THEN 7 WHEN 'August' THEN 8 WHEN 'September' THEN 9
                    WHEN 'October' THEN 10 WHEN 'November' THEN 11 WHEN 'December' THEN 12
                 END, 1) as rx_date
        FROM `{RX_TABLE}` rx
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
            AND (UPPER(rx.BRAND_NAME) LIKE UPPER('%{drug_name}%')
                 OR (UPPER(rx.MANUFACTURER) LIKE '%ALCON%' 
                     AND UPPER(rx.BRAND_NAME) = UPPER('{drug_name}')))
    ),
    -- Match payments to subsequent prescriptions within 180 days
    matched_data AS (
        SELECT 
            p.NPI,
            p.specialty,
            p.payment_amount,
            p.date_of_payment,
            r.rx_date,
            r.rx_payments,
            r.PRESCRIPTIONS,
            r.UNIQUE_PATIENTS,
            DATE_DIFF(r.rx_date, p.date_of_payment, DAY) as days_after_payment
        FROM drug_payments p
        INNER JOIN drug_prescriptions r ON p.NPI = r.NPI
        WHERE DATE_DIFF(r.rx_date, p.date_of_payment, DAY) BETWEEN 0 AND 180
    ),
    -- Aggregate results
    correlation_summary AS (
        SELECT 
            COUNT(DISTINCT NPI) as providers_with_correlation,
            COUNT(*) as payment_rx_pairs,
            SUM(payment_amount) as total_payments,
            SUM(rx_payments) as total_rx_value,
            SUM(PRESCRIPTIONS) as total_prescriptions,
            SUM(UNIQUE_PATIENTS) as total_patients,
            AVG(days_after_payment) as avg_days_to_prescription,
            CORR(payment_amount, rx_payments) as payment_rx_correlation
        FROM matched_data
    ),
    -- Get providers who prescribed but didn't receive payments
    prescribers_no_payment AS (
        SELECT 
            COUNT(DISTINCT r.NPI) as prescribers_without_payment,
            SUM(r.rx_payments) as rx_value_without_payment,
            SUM(r.PRESCRIPTIONS) as prescriptions_without_payment
        FROM drug_prescriptions r
        LEFT JOIN drug_payments p ON r.NPI = p.NPI
        WHERE p.NPI IS NULL
    )
    SELECT 
        '{drug_name}' as drug_name,
        c.*,
        p.prescribers_without_payment,
        p.rx_value_without_payment,
        p.prescriptions_without_payment
    FROM correlation_summary c
    CROSS JOIN prescribers_no_payment p
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS)
        ]
    )
    
    return client.query(query, job_config=job_config).to_dataframe()

def analyze_specialty_prescribing_patterns(client):
    """
    Analyze prescribing patterns by specialty for Alcon products
    """
    
    query = f"""
    WITH alcon_prescriptions AS (
        SELECT 
            CAST(rx.NPI AS STRING) AS NPI,
            rx.BRAND_NAME,
            rx.MANUFACTURER,
            rx.CLAIM_YEAR,
            SUM(rx.PAYMENTS) as total_payments,
            SUM(rx.PRESCRIPTIONS) as total_prescriptions,
            SUM(rx.UNIQUE_PATIENTS) as total_patients
        FROM `{RX_TABLE}` rx
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
            AND (UPPER(rx.MANUFACTURER) LIKE '%ALCON%'
                 OR UPPER(rx.BRAND_NAME) IN UNNEST(@products))
        GROUP BY 1,2,3,4
    ),
    provider_specialties AS (
        SELECT DISTINCT
            CAST(covered_recipient_npi AS STRING) AS NPI,
            covered_recipient_specialty_1 as specialty
        FROM `{OP_TABLE}`
        WHERE covered_recipient_npi IS NOT NULL
            AND covered_recipient_specialty_1 IS NOT NULL
    ),
    specialty_summary AS (
        SELECT 
            COALESCE(ps.specialty, 'Unknown') as specialty,
            COUNT(DISTINCT ap.NPI) as prescribers,
            SUM(ap.total_payments) as rx_value,
            SUM(ap.total_prescriptions) as prescriptions,
            SUM(ap.total_patients) as patients,
            COUNT(DISTINCT ap.BRAND_NAME) as unique_products
        FROM alcon_prescriptions ap
        LEFT JOIN provider_specialties ps ON ap.NPI = ps.NPI
        GROUP BY 1
    )
    SELECT *
    FROM specialty_summary
    ORDER BY rx_value DESC
    LIMIT 20
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("products", "STRING", ALCON_PRODUCTS)
        ]
    )
    
    return client.query(query, job_config=job_config).to_dataframe()

def analyze_payment_tiers_influence(client):
    """
    Analyze how different payment tiers correlate with prescribing volumes
    """
    
    query = f"""
    WITH provider_payments AS (
        SELECT 
            CAST(covered_recipient_npi AS STRING) AS NPI,
            covered_recipient_specialty_1 as specialty,
            SUM(total_amount_of_payment_usdollars) as total_payments,
            COUNT(*) as payment_count
        FROM `{OP_TABLE}`
        WHERE program_year BETWEEN 2020 AND 2024
            AND applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
            AND covered_recipient_npi IS NOT NULL
        GROUP BY 1,2
    ),
    provider_prescriptions AS (
        SELECT 
            CAST(NPI AS STRING) AS NPI,
            SUM(PAYMENTS) as total_rx_value,
            SUM(PRESCRIPTIONS) as total_prescriptions,
            SUM(UNIQUE_PATIENTS) as total_patients
        FROM `{RX_TABLE}`
        WHERE CLAIM_YEAR BETWEEN 2020 AND 2024
            AND (UPPER(MANUFACTURER) LIKE '%ALCON%'
                 OR UPPER(BRAND_NAME) IN UNNEST(@products))
        GROUP BY 1
    ),
    matched_providers AS (
        SELECT 
            pp.NPI,
            pp.specialty,
            pp.total_payments,
            pp.payment_count,
            COALESCE(pr.total_rx_value, 0) as rx_value,
            COALESCE(pr.total_prescriptions, 0) as prescriptions,
            COALESCE(pr.total_patients, 0) as patients,
            CASE 
                WHEN pp.total_payments = 0 THEN 'No Payment'
                WHEN pp.total_payments <= 100 THEN '$1-100'
                WHEN pp.total_payments <= 500 THEN '$101-500'
                WHEN pp.total_payments <= 1000 THEN '$501-1,000'
                WHEN pp.total_payments <= 5000 THEN '$1,001-5,000'
                WHEN pp.total_payments <= 10000 THEN '$5,001-10,000'
                ELSE '$10,000+'
            END as payment_tier
        FROM provider_payments pp
        LEFT JOIN provider_prescriptions pr ON pp.NPI = pr.NPI
    )
    SELECT 
        payment_tier,
        COUNT(DISTINCT NPI) as providers,
        AVG(total_payments) as avg_payment,
        SUM(rx_value) as total_rx_value,
        SUM(prescriptions) as total_prescriptions,
        SUM(patients) as total_patients,
        AVG(rx_value) as avg_rx_value_per_provider,
        AVG(prescriptions) as avg_prescriptions_per_provider,
        SUM(rx_value) / NULLIF(SUM(total_payments), 0) as roi_ratio
    FROM matched_providers
    GROUP BY payment_tier
    ORDER BY 
        CASE payment_tier
            WHEN 'No Payment' THEN 0
            WHEN '$1-100' THEN 1
            WHEN '$101-500' THEN 2
            WHEN '$501-1,000' THEN 3
            WHEN '$1,001-5,000' THEN 4
            WHEN '$5,001-10,000' THEN 5
            ELSE 6
        END
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS),
            bigquery.ArrayQueryParameter("products", "STRING", ALCON_PRODUCTS)
        ]
    )
    
    return client.query(query, job_config=job_config).to_dataframe()

def analyze_consecutive_year_influence(client):
    """
    Analyze providers receiving payments in consecutive years and their prescribing patterns
    """
    
    query = f"""
    WITH yearly_payments AS (
        SELECT 
            CAST(covered_recipient_npi AS STRING) AS NPI,
            covered_recipient_specialty_1 as specialty,
            program_year,
            SUM(total_amount_of_payment_usdollars) as yearly_payment
        FROM `{OP_TABLE}`
        WHERE program_year BETWEEN 2020 AND 2024
            AND applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
            AND covered_recipient_npi IS NOT NULL
        GROUP BY 1,2,3
    ),
    provider_year_count AS (
        SELECT 
            NPI,
            specialty,
            COUNT(DISTINCT program_year) as years_received,
            SUM(yearly_payment) as total_payments,
            AVG(yearly_payment) as avg_yearly_payment
        FROM yearly_payments
        GROUP BY 1,2
    ),
    provider_prescriptions AS (
        SELECT 
            CAST(NPI AS STRING) AS NPI,
            SUM(PAYMENTS) as total_rx_value,
            SUM(PRESCRIPTIONS) as total_prescriptions
        FROM `{RX_TABLE}`
        WHERE CLAIM_YEAR BETWEEN 2020 AND 2024
            AND (UPPER(MANUFACTURER) LIKE '%ALCON%'
                 OR UPPER(BRAND_NAME) IN UNNEST(@products))
        GROUP BY 1
    )
    SELECT 
        pyc.years_received,
        COUNT(DISTINCT pyc.NPI) as providers,
        AVG(pyc.total_payments) as avg_total_payments,
        AVG(pyc.avg_yearly_payment) as avg_payment_per_year,
        SUM(pr.total_rx_value) as total_rx_value,
        SUM(pr.total_prescriptions) as total_prescriptions,
        AVG(pr.total_rx_value) as avg_rx_value_per_provider,
        AVG(pr.total_prescriptions) as avg_prescriptions_per_provider
    FROM provider_year_count pyc
    LEFT JOIN provider_prescriptions pr ON pyc.NPI = pr.NPI
    GROUP BY years_received
    ORDER BY years_received
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS),
            bigquery.ArrayQueryParameter("products", "STRING", ALCON_PRODUCTS)
        ]
    )
    
    return client.query(query, job_config=job_config).to_dataframe()

def analyze_np_pa_vulnerability(client):
    """
    Analyze NP/PA providers who may be more susceptible to payment influence
    """
    
    query = f"""
    WITH np_pa_providers AS (
        SELECT DISTINCT
            CAST(covered_recipient_npi AS STRING) AS NPI,
            covered_recipient_specialty_1 as specialty,
            covered_recipient_first_name,
            covered_recipient_last_name
        FROM `{OP_TABLE}`
        WHERE covered_recipient_specialty_1 IN (
            'Physician Assistant', 
            'Nurse Practitioner',
            'Optometry'
        )
    ),
    payments_to_np_pa AS (
        SELECT 
            CAST(op.covered_recipient_npi AS STRING) AS NPI,
            SUM(op.total_amount_of_payment_usdollars) as total_payments,
            COUNT(*) as payment_count
        FROM `{OP_TABLE}` op
        INNER JOIN np_pa_providers npp ON CAST(op.covered_recipient_npi AS STRING) = npp.NPI
        WHERE op.program_year BETWEEN 2020 AND 2024
            AND op.applicable_manufacturer_or_applicable_gpo_making_payment_name IN UNNEST(@manufacturers)
        GROUP BY 1
    ),
    prescriptions_by_np_pa AS (
        SELECT 
            npp.NPI,
            npp.specialty,
            SUM(rx.PAYMENTS) as rx_value,
            SUM(rx.PRESCRIPTIONS) as prescriptions,
            SUM(rx.UNIQUE_PATIENTS) as patients
        FROM np_pa_providers npp
        INNER JOIN `{RX_TABLE}` rx ON npp.NPI = CAST(rx.NPI AS STRING)
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
            AND (UPPER(rx.MANUFACTURER) LIKE '%ALCON%'
                 OR UPPER(rx.BRAND_NAME) IN UNNEST(@products))
        GROUP BY 1,2
    )
    SELECT 
        p.specialty,
        COUNT(DISTINCT p.NPI) as providers,
        COUNT(DISTINCT pay.NPI) as providers_with_payments,
        SUM(pay.total_payments) as total_payments,
        SUM(p.rx_value) as total_rx_value,
        SUM(p.prescriptions) as total_prescriptions,
        AVG(CASE WHEN pay.NPI IS NOT NULL THEN p.rx_value END) as avg_rx_with_payment,
        AVG(CASE WHEN pay.NPI IS NULL THEN p.rx_value END) as avg_rx_without_payment,
        (AVG(CASE WHEN pay.NPI IS NOT NULL THEN p.rx_value END) - 
         AVG(CASE WHEN pay.NPI IS NULL THEN p.rx_value END)) as rx_difference
    FROM prescriptions_by_np_pa p
    LEFT JOIN payments_to_np_pa pay ON p.NPI = pay.NPI
    GROUP BY p.specialty
    ORDER BY total_rx_value DESC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("manufacturers", "STRING", ALCON_MANUFACTURERS),
            bigquery.ArrayQueryParameter("products", "STRING", ALCON_PRODUCTS)
        ]
    )
    
    return client.query(query, job_config=job_config).to_dataframe()

def main():
    """Main execution function"""
    try:
        logger.info("=" * 80)
        logger.info("ALCON PRESCRIPTION PAYMENT INFLUENCE ANALYSIS")
        logger.info("=" * 80)
        
        # Create BigQuery client
        client = create_bigquery_client()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Analyze key drugs
        logger.info("\nAnalyzing payment-prescription correlations for key Alcon products...")
        key_drugs = ['ROCKLATAN', 'RHOPRESSA', 'CIPRODEX', 'SIMBRINZA', 'COMBIGAN']
        
        all_correlations = []
        for drug in key_drugs:
            logger.info(f"  Analyzing {drug}...")
            try:
                correlation = analyze_drug_payment_correlation(client, drug)
                if not correlation.empty:
                    all_correlations.append(correlation)
                    logger.info(f"    - Providers with correlation: {correlation['providers_with_correlation'].iloc[0]:,}")
                    if pd.notna(correlation['payment_rx_correlation'].iloc[0]):
                        logger.info(f"    - Correlation coefficient: {correlation['payment_rx_correlation'].iloc[0]:.3f}")
            except Exception as e:
                logger.warning(f"    - Error analyzing {drug}: {str(e)}")
        
        if all_correlations:
            combined = pd.concat(all_correlations, ignore_index=True)
            combined.to_csv(OUTPUT_DIR / f"alcon_drug_correlations_{timestamp}.csv", index=False)
            logger.info(f"  Saved drug correlations: {len(combined)} drugs analyzed")
        
        # Analyze specialty prescribing patterns
        logger.info("\nAnalyzing specialty prescribing patterns...")
        specialty_patterns = analyze_specialty_prescribing_patterns(client)
        specialty_patterns.to_csv(OUTPUT_DIR / f"alcon_specialty_prescribing_{timestamp}.csv", index=False)
        logger.info(f"  Analyzed {len(specialty_patterns)} specialties")
        
        # Analyze payment tiers
        logger.info("\nAnalyzing payment tier influence...")
        payment_tiers = analyze_payment_tiers_influence(client)
        payment_tiers.to_csv(OUTPUT_DIR / f"alcon_payment_tiers_influence_{timestamp}.csv", index=False)
        logger.info(f"  Analyzed {len(payment_tiers)} payment tiers")
        
        # Analyze consecutive years
        logger.info("\nAnalyzing consecutive year recipients...")
        consecutive = analyze_consecutive_year_influence(client)
        consecutive.to_csv(OUTPUT_DIR / f"alcon_consecutive_years_influence_{timestamp}.csv", index=False)
        logger.info(f"  Analyzed {len(consecutive)} year cohorts")
        
        # Analyze NP/PA vulnerability
        logger.info("\nAnalyzing NP/PA/Optometry vulnerability...")
        np_pa = analyze_np_pa_vulnerability(client)
        np_pa.to_csv(OUTPUT_DIR / f"alcon_np_pa_vulnerability_{timestamp}.csv", index=False)
        logger.info(f"  Analyzed {len(np_pa)} provider types")
        
        # Generate summary report
        logger.info("\n" + "=" * 80)
        logger.info("PRESCRIPTION INFLUENCE ANALYSIS SUMMARY")
        logger.info("=" * 80)
        
        if not payment_tiers.empty:
            logger.info("\nPayment Tier Analysis:")
            for _, row in payment_tiers.iterrows():
                if pd.notna(row['roi_ratio']):
                    logger.info(f"  {row['payment_tier']}: {row['providers']:,} providers, ROI: {row['roi_ratio']:.2f}x")
        
        if not consecutive.empty:
            logger.info("\nMulti-Year Engagement Impact:")
            for _, row in consecutive.iterrows():
                if row['years_received'] >= 3:
                    logger.info(f"  {row['years_received']} years: {row['providers']:,} providers, "
                              f"Avg Rx: ${row['avg_rx_value_per_provider']:,.2f}")
        
        logger.info("\n" + "=" * 80)
        logger.info("ANALYSIS COMPLETE")
        logger.info(f"Results saved to: {OUTPUT_DIR}")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"Error during prescription influence analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)