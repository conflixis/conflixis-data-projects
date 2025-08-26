#!/usr/bin/env python3
"""
Analyze Payment-Prescription Correlations for Corewell Health
DA-175: Corewell Health Open Payments Report - Payment Influence Analysis

This script analyzes the correlation between Open Payments and prescribing patterns,
matching the methodology from the Conflixis Open Payments Report.
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
RX_TABLE = f"{PROJECT_ID}.{DATASET_ID}.PHYSICIAN_RX_2020_2024"

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
    Analyze correlation between payments and prescribing for a specific drug.
    Implements the 180-day window methodology from the report.
    """
    
    query = f"""
    WITH corewell_providers AS (
        SELECT DISTINCT NPI
        FROM `{NPI_TABLE}`
    ),
    -- Get Open Payments for this drug
    drug_payments AS (
        SELECT 
            CAST(op.covered_recipient_npi AS STRING) AS NPI,
            op.program_year,
            op.date_of_payment,
            op.total_amount_of_payment_usdollars as payment_amount,
            op.applicable_manufacturer_or_applicable_gpo_making_payment_name AS manufacturer
        FROM `{OP_TABLE}` op
        WHERE op.program_year BETWEEN 2020 AND 2024
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
            AND UPPER(rx.BRAND_NAME) LIKE UPPER('%{drug_name}%')
    ),
    -- Aggregate providers with and without payments
    provider_summary AS (
        SELECT 
            cp.NPI,
            COALESCE(SUM(dp.payment_amount), 0) as total_op_payments,
            COUNT(DISTINCT dp.date_of_payment) as payment_dates,
            COALESCE(SUM(drx.rx_payments), 0) as total_rx_payments,
            COALESCE(SUM(drx.PRESCRIPTIONS), 0) as total_prescriptions,
            COALESCE(SUM(drx.UNIQUE_PATIENTS), 0) as total_patients,
            CASE WHEN COUNT(dp.NPI) > 0 THEN 1 ELSE 0 END as received_payment
        FROM corewell_providers cp
        LEFT JOIN drug_payments dp ON cp.NPI = dp.NPI
        LEFT JOIN drug_prescriptions drx ON cp.NPI = drx.NPI
        GROUP BY cp.NPI
    )
    SELECT 
        received_payment,
        COUNT(DISTINCT NPI) as provider_count,
        AVG(total_rx_payments) as avg_rx_payments,
        AVG(total_prescriptions) as avg_prescriptions,
        AVG(total_patients) as avg_patients,
        SUM(total_rx_payments) as sum_rx_payments,
        SUM(total_prescriptions) as sum_prescriptions,
        AVG(total_op_payments) as avg_op_payment
    FROM provider_summary
    WHERE total_prescriptions > 0  -- Only providers who prescribed the drug
    GROUP BY received_payment
    ORDER BY received_payment
    """
    
    logger.info(f"\nAnalyzing payment correlation for {drug_name}...")
    df = client.query(query).to_dataframe()
    
    if len(df) >= 2:
        no_payment = df[df['received_payment'] == 0].iloc[0] if len(df[df['received_payment'] == 0]) > 0 else None
        with_payment = df[df['received_payment'] == 1].iloc[0] if len(df[df['received_payment'] == 1]) > 0 else None
        
        if no_payment is not None and with_payment is not None:
            ratio = with_payment['avg_rx_payments'] / no_payment['avg_rx_payments'] if no_payment['avg_rx_payments'] > 0 else 0
            logger.info(f"  Providers WITH payments: ${with_payment['avg_rx_payments']:,.0f} avg")
            logger.info(f"  Providers WITHOUT payments: ${no_payment['avg_rx_payments']:,.0f} avg")
            logger.info(f"  Ratio: {ratio:.2f}x more prescribed by paid providers")
            
            # Calculate ROI if we have payment data
            if with_payment['avg_op_payment'] > 0:
                roi = (with_payment['avg_rx_payments'] - no_payment['avg_rx_payments']) / with_payment['avg_op_payment']
                logger.info(f"  ROI: ${roi:.0f} per dollar of payments")
    
    return df

def analyze_key_drugs(client):
    """Analyze key drugs from the Open Payments Report"""
    
    # Key drugs highlighted in the report
    key_drugs = [
        'Ozempic',
        'Trelegy',
        'Krystexxa',
        'Farxiga',
        'Mounjaro',
        'Jardiance',
        'Humira',
        'Enbrel',
        'Stelara',
        'Xarelto',
        'Eliquis',
        'Entresto'
    ]
    
    results = {}
    for drug in key_drugs:
        try:
            df = analyze_drug_payment_correlation(client, drug)
            results[drug] = df
        except Exception as e:
            logger.warning(f"Could not analyze {drug}: {e}")
    
    return results

def analyze_consecutive_year_influence(client):
    """Analyze providers receiving payments in consecutive years and their prescribing"""
    
    query = f"""
    WITH corewell_providers AS (
        SELECT DISTINCT NPI
        FROM `{NPI_TABLE}`
    ),
    -- Get payment years per provider
    provider_payment_years AS (
        SELECT 
            CAST(op.covered_recipient_npi AS STRING) AS NPI,
            op.program_year,
            SUM(op.total_amount_of_payment_usdollars) as yearly_payment
        FROM `{OP_TABLE}` op
        INNER JOIN corewell_providers cp
            ON CAST(op.covered_recipient_npi AS STRING) = cp.NPI
        WHERE op.program_year BETWEEN 2020 AND 2024
        GROUP BY 1, 2
    ),
    -- Calculate consecutive years
    consecutive_years AS (
        SELECT 
            NPI,
            COUNT(DISTINCT program_year) as years_with_payments,
            STRING_AGG(CAST(program_year AS STRING), ',' ORDER BY program_year) as years,
            AVG(yearly_payment) as avg_yearly_payment,
            SUM(yearly_payment) as total_payments
        FROM provider_payment_years
        GROUP BY NPI
    ),
    -- Get prescriptions
    provider_rx AS (
        SELECT 
            CAST(rx.NPI AS STRING) AS NPI,
            SUM(rx.PAYMENTS) as total_rx_payments,
            SUM(rx.PRESCRIPTIONS) as total_prescriptions
        FROM `{RX_TABLE}` rx
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
        GROUP BY 1
    )
    SELECT 
        cy.years_with_payments,
        COUNT(DISTINCT cy.NPI) as provider_count,
        AVG(cy.total_payments) as avg_total_op_payments,
        AVG(pr.total_rx_payments) as avg_rx_payments,
        AVG(pr.total_prescriptions) as avg_prescriptions,
        SUM(pr.total_rx_payments) as sum_rx_payments
    FROM consecutive_years cy
    INNER JOIN provider_rx pr ON cy.NPI = pr.NPI
    GROUP BY cy.years_with_payments
    ORDER BY cy.years_with_payments DESC
    """
    
    logger.info("\nAnalyzing consecutive year payment influence...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nConsecutive Year Payment Influence:")
    for _, row in df.iterrows():
        logger.info(f"  {row['years_with_payments']} years: {row['provider_count']:,} providers, ${row['avg_rx_payments']:,.0f} avg Rx")
    
    return df

def analyze_payment_tiers_influence(client):
    """Analyze influence by payment tier (matching report methodology)"""
    
    query = f"""
    WITH corewell_providers AS (
        SELECT DISTINCT NPI
        FROM `{NPI_TABLE}`
    ),
    -- Calculate total payments per provider
    provider_payments AS (
        SELECT 
            CAST(op.covered_recipient_npi AS STRING) AS NPI,
            SUM(op.total_amount_of_payment_usdollars) as total_op_payments
        FROM `{OP_TABLE}` op
        INNER JOIN corewell_providers cp
            ON CAST(op.covered_recipient_npi AS STRING) = cp.NPI
        WHERE op.program_year BETWEEN 2020 AND 2024
        GROUP BY 1
    ),
    -- Get prescriptions
    provider_rx AS (
        SELECT 
            CAST(rx.NPI AS STRING) AS NPI,
            SUM(rx.PAYMENTS) as total_rx_payments,
            SUM(rx.PRESCRIPTIONS) as total_prescriptions
        FROM `{RX_TABLE}` rx
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
        GROUP BY 1
    ),
    -- Combine and create tiers
    provider_summary AS (
        SELECT 
            cp.NPI,
            COALESCE(pp.total_op_payments, 0) as total_op_payments,
            COALESCE(pr.total_rx_payments, 0) as total_rx_payments,
            COALESCE(pr.total_prescriptions, 0) as total_prescriptions,
            CASE 
                WHEN COALESCE(pp.total_op_payments, 0) = 0 THEN '0. No Payment'
                WHEN pp.total_op_payments < 100 THEN '1. $1-100'
                WHEN pp.total_op_payments < 500 THEN '2. $101-500'
                WHEN pp.total_op_payments < 1000 THEN '3. $501-1000'
                WHEN pp.total_op_payments < 5000 THEN '4. $1001-5000'
                ELSE '5. $5000+'
            END as payment_tier
        FROM corewell_providers cp
        LEFT JOIN provider_payments pp ON cp.NPI = pp.NPI
        LEFT JOIN provider_rx pr ON cp.NPI = pr.NPI
    )
    SELECT 
        payment_tier,
        COUNT(DISTINCT NPI) as provider_count,
        AVG(total_op_payments) as avg_op_payment,
        AVG(total_rx_payments) as avg_rx_payment,
        AVG(total_prescriptions) as avg_prescriptions,
        SUM(total_rx_payments) as sum_rx_payments,
        AVG(CASE WHEN total_op_payments > 0 
            THEN total_rx_payments / total_op_payments 
            ELSE 0 END) as roi
    FROM provider_summary
    WHERE total_rx_payments > 0  -- Only providers with prescriptions
    GROUP BY payment_tier
    ORDER BY payment_tier
    """
    
    logger.info("\nAnalyzing payment tier influence...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nPayment Tier Influence:")
    for _, row in df.iterrows():
        logger.info(f"  {row['payment_tier']}: {row['provider_count']:,} providers, ${row['avg_rx_payment']:,.0f} avg Rx, ROI: {row['roi']:.1f}x")
    
    return df

def analyze_np_pa_vulnerability(client):
    """Analyze NP/PA vulnerability to payment influence (key finding from report)"""
    
    query = f"""
    WITH provider_types AS (
        SELECT 
            NPI,
            Primary_Specialty,
            CASE 
                WHEN Primary_Specialty LIKE '%Nurse Practitioner%' THEN 'NP'
                WHEN Primary_Specialty LIKE '%Physician Assistant%' THEN 'PA'
                WHEN Primary_Specialty IS NOT NULL THEN 'Physician'
                ELSE 'Other'
            END as provider_type
        FROM `{NPI_TABLE}`
    ),
    -- Get payments by provider type
    provider_payments AS (
        SELECT 
            pt.provider_type,
            pt.NPI,
            SUM(op.total_amount_of_payment_usdollars) as total_op_payments,
            COUNT(*) as payment_count
        FROM `{OP_TABLE}` op
        INNER JOIN provider_types pt
            ON CAST(op.covered_recipient_npi AS STRING) = pt.NPI
        WHERE op.program_year BETWEEN 2020 AND 2024
        GROUP BY 1, 2
    ),
    -- Get prescriptions by provider type
    provider_rx AS (
        SELECT 
            pt.provider_type,
            pt.NPI,
            SUM(rx.PAYMENTS) as total_rx_payments,
            SUM(rx.PRESCRIPTIONS) as total_prescriptions
        FROM `{RX_TABLE}` rx
        INNER JOIN provider_types pt
            ON CAST(rx.NPI AS STRING) = pt.NPI
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
        GROUP BY 1, 2
    ),
    -- Combine and analyze
    combined AS (
        SELECT 
            COALESCE(pp.provider_type, pr.provider_type) as provider_type,
            COALESCE(pp.NPI, pr.NPI) as NPI,
            COALESCE(pp.total_op_payments, 0) as total_op_payments,
            COALESCE(pr.total_rx_payments, 0) as total_rx_payments,
            CASE WHEN pp.total_op_payments > 0 THEN 1 ELSE 0 END as received_payment
        FROM provider_payments pp
        FULL OUTER JOIN provider_rx pr
            ON pp.NPI = pr.NPI AND pp.provider_type = pr.provider_type
    )
    SELECT 
        provider_type,
        received_payment,
        COUNT(DISTINCT NPI) as provider_count,
        AVG(total_op_payments) as avg_op_payment,
        AVG(total_rx_payments) as avg_rx_payment,
        SUM(total_rx_payments) as sum_rx_payments,
        AVG(CASE WHEN total_op_payments > 0 
            THEN total_rx_payments / total_op_payments 
            ELSE 0 END) as roi
    FROM combined
    WHERE total_rx_payments > 0
    GROUP BY provider_type, received_payment
    ORDER BY provider_type, received_payment
    """
    
    logger.info("\nAnalyzing NP/PA payment vulnerability...")
    df = client.query(query).to_dataframe()
    
    # Calculate vulnerability metrics
    for provider_type in ['NP', 'PA', 'Physician']:
        type_data = df[df['provider_type'] == provider_type]
        if len(type_data) >= 2:
            no_payment = type_data[type_data['received_payment'] == 0].iloc[0] if len(type_data[type_data['received_payment'] == 0]) > 0 else None
            with_payment = type_data[type_data['received_payment'] == 1].iloc[0] if len(type_data[type_data['received_payment'] == 1]) > 0 else None
            
            if no_payment is not None and with_payment is not None:
                vulnerability = (with_payment['avg_rx_payment'] - no_payment['avg_rx_payment']) / no_payment['avg_rx_payment'] * 100 if no_payment['avg_rx_payment'] > 0 else 0
                logger.info(f"\n{provider_type} Vulnerability:")
                logger.info(f"  With payments: ${with_payment['avg_rx_payment']:,.0f}")
                logger.info(f"  Without payments: ${no_payment['avg_rx_payment']:,.0f}")
                logger.info(f"  Vulnerability: {vulnerability:.1f}% increase with payments")
                if with_payment['avg_op_payment'] > 0:
                    logger.info(f"  ROI: {with_payment['roi']:.1f}x")
    
    return df

def main():
    """Main execution function"""
    logger.info("=" * 80)
    logger.info("COREWELL HEALTH PAYMENT-PRESCRIPTION CORRELATION ANALYSIS")
    logger.info("=" * 80)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        # Create BigQuery client
        logger.info("Connecting to BigQuery...")
        client = create_bigquery_client()
        
        # Run analyses
        results = {}
        
        # Analyze key drugs
        logger.info("\n" + "=" * 60)
        logger.info("KEY DRUG PAYMENT CORRELATIONS")
        logger.info("=" * 60)
        key_drug_results = analyze_key_drugs(client)
        
        # Save individual drug results
        for drug, df in key_drug_results.items():
            if not df.empty:
                df.to_csv(OUTPUT_DIR / f"correlation_{drug.lower()}_{timestamp}.csv", index=False)
        
        # Consecutive year influence
        results['consecutive'] = analyze_consecutive_year_influence(client)
        results['consecutive'].to_csv(OUTPUT_DIR / f"correlation_consecutive_years_{timestamp}.csv", index=False)
        
        # Payment tier influence
        results['tiers'] = analyze_payment_tiers_influence(client)
        results['tiers'].to_csv(OUTPUT_DIR / f"correlation_payment_tiers_{timestamp}.csv", index=False)
        
        # NP/PA vulnerability
        results['np_pa'] = analyze_np_pa_vulnerability(client)
        results['np_pa'].to_csv(OUTPUT_DIR / f"correlation_np_pa_vulnerability_{timestamp}.csv", index=False)
        
        logger.info(f"\nAll results saved to: {OUTPUT_DIR}")
        logger.info("=" * 80)
        logger.info("Payment Correlation Analysis Complete!")
        
        return results
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        raise

if __name__ == "__main__":
    results = main()