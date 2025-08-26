#!/usr/bin/env python3
"""
Analyze Prescription Patterns for Corewell Health Providers
DA-175: Corewell Health Open Payments Report - Prescription Analysis

This script analyzes prescription patterns for Corewell Health providers
using claims data from 2020-2024.
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

def analyze_overall_prescribing(client):
    """Analyze overall prescribing metrics for Corewell providers"""
    
    query = f"""
    WITH corewell_rx AS (
        SELECT 
            rx.NPI,
            rx.CLAIM_YEAR,
            rx.BRAND_NAME,
            rx.MANUFACTURER,
            rx.CHARGES,
            rx.PAYMENTS,
            rx.PRESCRIPTIONS,
            rx.UNIQUE_PATIENTS,
            cs.Primary_Specialty,
            cs.Full_Name
        FROM `{RX_TABLE}` rx
        INNER JOIN `{NPI_TABLE}` cs
            ON CAST(rx.NPI AS STRING) = cs.NPI
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
    )
    SELECT 
        COUNT(DISTINCT NPI) as unique_prescribers,
        COUNT(DISTINCT BRAND_NAME) as unique_drugs,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        SUM(CHARGES) as total_charges,
        SUM(UNIQUE_PATIENTS) as total_patients,
        AVG(PAYMENTS) as avg_payment_per_claim,
        MIN(CLAIM_YEAR) as min_year,
        MAX(CLAIM_YEAR) as max_year
    FROM corewell_rx
    """
    
    logger.info("Analyzing overall prescribing metrics...")
    df = client.query(query).to_dataframe()
    
    if not df.empty:
        row = df.iloc[0]
        logger.info(f"  Unique Prescribers: {row['unique_prescribers']:,.0f}")
        logger.info(f"  Unique Drugs: {row['unique_drugs']:,.0f}")
        logger.info(f"  Total Prescriptions: {row['total_prescriptions']:,.0f}")
        logger.info(f"  Total Payments: ${row['total_payments']:,.2f}")
        logger.info(f"  Total Charges: ${row['total_charges']:,.2f}")
    
    return df

def analyze_prescribing_by_year(client):
    """Analyze prescribing trends by year"""
    
    query = f"""
    WITH corewell_rx AS (
        SELECT 
            rx.NPI,
            rx.CLAIM_YEAR,
            rx.PAYMENTS,
            rx.PRESCRIPTIONS,
            rx.UNIQUE_PATIENTS
        FROM `{RX_TABLE}` rx
        INNER JOIN `{NPI_TABLE}` cs
            ON CAST(rx.NPI AS STRING) = cs.NPI
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
    )
    SELECT 
        CLAIM_YEAR,
        COUNT(DISTINCT NPI) as unique_prescribers,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        SUM(UNIQUE_PATIENTS) as total_patients,
        AVG(PAYMENTS) as avg_payment,
        SUM(PAYMENTS) / NULLIF(SUM(PRESCRIPTIONS), 0) as avg_cost_per_prescription
    FROM corewell_rx
    GROUP BY CLAIM_YEAR
    ORDER BY CLAIM_YEAR
    """
    
    logger.info("\nAnalyzing prescribing by year...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nYearly Prescribing Trends:")
    for _, row in df.iterrows():
        logger.info(f"  {row['CLAIM_YEAR']}: ${row['total_payments']:,.0f} for {row['total_prescriptions']:,.0f} prescriptions")
    
    return df

def analyze_top_drugs(client):
    """Identify top prescribed drugs by Corewell providers"""
    
    query = f"""
    WITH corewell_rx AS (
        SELECT 
            rx.BRAND_NAME,
            rx.GENERIC_NAME,
            rx.MANUFACTURER,
            rx.NPI,
            rx.PAYMENTS,
            rx.PRESCRIPTIONS,
            rx.UNIQUE_PATIENTS
        FROM `{RX_TABLE}` rx
        INNER JOIN `{NPI_TABLE}` cs
            ON CAST(rx.NPI AS STRING) = cs.NPI
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
    )
    SELECT 
        BRAND_NAME,
        GENERIC_NAME,
        MANUFACTURER,
        COUNT(DISTINCT NPI) as prescriber_count,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        SUM(UNIQUE_PATIENTS) as total_patients,
        AVG(PAYMENTS / NULLIF(PRESCRIPTIONS, 0)) as avg_cost_per_prescription
    FROM corewell_rx
    WHERE BRAND_NAME IS NOT NULL
    GROUP BY BRAND_NAME, GENERIC_NAME, MANUFACTURER
    ORDER BY total_payments DESC
    LIMIT 50
    """
    
    logger.info("\nAnalyzing top prescribed drugs...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nTop 10 Drugs by Total Payments:")
    for i, row in df.head(10).iterrows():
        logger.info(f"  {row['BRAND_NAME']}: ${row['total_payments']:,.0f} ({row['prescriber_count']:,} prescribers)")
    
    return df

def analyze_high_cost_drugs(client):
    """Identify high-cost drugs including those from the report"""
    
    # Key drugs from the Open Payments report
    target_drugs = ['Ozempic', 'Trelegy', 'Krystexxa', 'Farxiga', 'Mounjaro', 'Jardiance', 
                   'Humira', 'Enbrel', 'Stelara', 'Xarelto', 'Eliquis', 'Entresto']
    
    drugs_str = "', '".join(target_drugs)
    
    query = f"""
    WITH corewell_rx AS (
        SELECT 
            rx.NPI,
            rx.CLAIM_YEAR,
            rx.BRAND_NAME,
            rx.MANUFACTURER,
            rx.PAYMENTS,
            rx.PRESCRIPTIONS,
            rx.UNIQUE_PATIENTS,
            cs.Primary_Specialty,
            cs.Full_Name
        FROM `{RX_TABLE}` rx
        INNER JOIN `{NPI_TABLE}` cs
            ON CAST(rx.NPI AS STRING) = cs.NPI
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
            AND UPPER(rx.BRAND_NAME) IN ('{drugs_str.upper()}')
    )
    SELECT 
        BRAND_NAME,
        MANUFACTURER,
        CLAIM_YEAR,
        COUNT(DISTINCT NPI) as prescriber_count,
        SUM(PRESCRIPTIONS) as prescriptions,
        SUM(PAYMENTS) as total_payments,
        SUM(UNIQUE_PATIENTS) as patients,
        AVG(PAYMENTS / NULLIF(PRESCRIPTIONS, 0)) as avg_cost_per_rx
    FROM corewell_rx
    GROUP BY BRAND_NAME, MANUFACTURER, CLAIM_YEAR
    ORDER BY BRAND_NAME, CLAIM_YEAR
    """
    
    logger.info("\nAnalyzing key high-cost drugs...")
    df = client.query(query).to_dataframe()
    
    if not df.empty:
        drug_summary = df.groupby('BRAND_NAME').agg({
            'prescriber_count': 'sum',
            'prescriptions': 'sum', 
            'total_payments': 'sum'
        }).reset_index()
        
        logger.info("\nKey Drug Analysis:")
        for _, row in drug_summary.iterrows():
            logger.info(f"  {row['BRAND_NAME']}: ${row['total_payments']:,.0f}")
    
    return df

def analyze_specialty_prescribing(client):
    """Analyze prescribing patterns by specialty"""
    
    query = f"""
    WITH corewell_rx AS (
        SELECT 
            cs.Primary_Specialty,
            rx.NPI,
            rx.PAYMENTS,
            rx.PRESCRIPTIONS,
            rx.UNIQUE_PATIENTS
        FROM `{RX_TABLE}` rx
        INNER JOIN `{NPI_TABLE}` cs
            ON CAST(rx.NPI AS STRING) = cs.NPI
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
    )
    SELECT 
        Primary_Specialty,
        COUNT(DISTINCT NPI) as prescriber_count,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        SUM(UNIQUE_PATIENTS) as total_patients,
        AVG(PAYMENTS) as avg_payment,
        SUM(PAYMENTS) / NULLIF(SUM(PRESCRIPTIONS), 0) as avg_cost_per_prescription
    FROM corewell_rx
    WHERE Primary_Specialty IS NOT NULL
    GROUP BY Primary_Specialty
    ORDER BY total_payments DESC
    """
    
    logger.info("\nAnalyzing prescribing by specialty...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nTop Specialties by Prescription Payments:")
    for i, row in df.head(10).iterrows():
        logger.info(f"  {row['Primary_Specialty']}: ${row['total_payments']:,.0f} ({row['prescriber_count']:,} prescribers)")
    
    return df

def analyze_np_pa_prescribing(client):
    """Analyze NP and PA prescribing patterns (key vulnerability from report)"""
    
    query = f"""
    WITH provider_types AS (
        SELECT 
            cs.NPI,
            cs.Primary_Specialty,
            CASE 
                WHEN Primary_Specialty LIKE '%Nurse Practitioner%' THEN 'NP'
                WHEN Primary_Specialty LIKE '%Physician Assistant%' THEN 'PA'
                WHEN Primary_Specialty IS NOT NULL THEN 'Physician'
                ELSE 'Other'
            END as provider_type
        FROM `{NPI_TABLE}` cs
    ),
    corewell_rx AS (
        SELECT 
            pt.provider_type,
            rx.NPI,
            rx.CLAIM_YEAR,
            rx.PAYMENTS,
            rx.PRESCRIPTIONS,
            rx.UNIQUE_PATIENTS
        FROM `{RX_TABLE}` rx
        INNER JOIN provider_types pt
            ON CAST(rx.NPI AS STRING) = pt.NPI
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
    )
    SELECT 
        provider_type,
        CLAIM_YEAR,
        COUNT(DISTINCT NPI) as provider_count,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        SUM(UNIQUE_PATIENTS) as total_patients,
        AVG(PAYMENTS / NULLIF(PRESCRIPTIONS, 0)) as avg_cost_per_rx
    FROM corewell_rx
    GROUP BY provider_type, CLAIM_YEAR
    ORDER BY provider_type, CLAIM_YEAR
    """
    
    logger.info("\nAnalyzing NP/PA prescribing patterns...")
    df = client.query(query).to_dataframe()
    
    if not df.empty:
        summary = df.groupby('provider_type').agg({
            'provider_count': 'mean',
            'total_prescriptions': 'sum',
            'total_payments': 'sum'
        }).reset_index()
        
        logger.info("\nProvider Type Prescribing Summary:")
        for _, row in summary.iterrows():
            logger.info(f"  {row['provider_type']}: ${row['total_payments']:,.0f}")
    
    return df

def main():
    """Main execution function"""
    logger.info("=" * 80)
    logger.info("COREWELL HEALTH PRESCRIPTION PATTERN ANALYSIS")
    logger.info("=" * 80)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        # Create BigQuery client
        logger.info("Connecting to BigQuery...")
        client = create_bigquery_client()
        
        # Run analyses
        results = {}
        
        # Overall metrics
        results['overall'] = analyze_overall_prescribing(client)
        results['overall'].to_csv(OUTPUT_DIR / f"rx_overall_metrics_{timestamp}.csv", index=False)
        
        # Yearly trends
        results['yearly'] = analyze_prescribing_by_year(client)
        results['yearly'].to_csv(OUTPUT_DIR / f"rx_yearly_trends_{timestamp}.csv", index=False)
        
        # Top drugs
        results['top_drugs'] = analyze_top_drugs(client)
        results['top_drugs'].to_csv(OUTPUT_DIR / f"rx_top_drugs_{timestamp}.csv", index=False)
        
        # High-cost targeted drugs
        results['high_cost'] = analyze_high_cost_drugs(client)
        results['high_cost'].to_csv(OUTPUT_DIR / f"rx_high_cost_drugs_{timestamp}.csv", index=False)
        
        # Specialty analysis
        results['specialty'] = analyze_specialty_prescribing(client)
        results['specialty'].to_csv(OUTPUT_DIR / f"rx_by_specialty_{timestamp}.csv", index=False)
        
        # NP/PA analysis
        results['np_pa'] = analyze_np_pa_prescribing(client)
        results['np_pa'].to_csv(OUTPUT_DIR / f"rx_np_pa_analysis_{timestamp}.csv", index=False)
        
        logger.info(f"\nAll results saved to: {OUTPUT_DIR}")
        logger.info("=" * 80)
        logger.info("Prescription Analysis Complete!")
        
        return results
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        raise

if __name__ == "__main__":
    results = main()