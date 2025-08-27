#!/usr/bin/env python3
"""
Analyze Prescription Patterns for Alcon Products
DA-179: Alcon Analysis - Prescription Patterns Analysis

This script analyzes prescription patterns for Alcon products
using claims data from 2020-2024, following the comprehensive methodology
from provider-level analysis adapted for manufacturer analysis.
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
RX_TABLE = f"{PROJECT_ID}.{DATASET_ID}.PHYSICIAN_RX_2020_2024"
OP_TABLE = f"{PROJECT_ID}.{DATASET_ID}.op_general_all_aggregate_static"

# Alcon manufacturers and key products
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
        scopes=['https://www.googleapis.com/auth/bigquery']
    )
    
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)

def analyze_overall_prescribing(client):
    """Analyze overall prescribing metrics for Alcon products"""
    
    query = f"""
    WITH alcon_rx AS (
        SELECT 
            NPI,
            CLAIM_YEAR,
            BRAND_NAME,
            GENERIC_NAME,
            MANUFACTURER,
            CHARGES,
            PAYMENTS,
            PRESCRIPTIONS,
            UNIQUE_PATIENTS,
            DAYS_SUPPLY
        FROM `{RX_TABLE}`
        WHERE CLAIM_YEAR BETWEEN 2020 AND 2024
            AND (UPPER(MANUFACTURER) LIKE '%ALCON%'
                OR UPPER(BRAND_NAME) IN (
                    'RHOPRESSA', 'ROCKLATAN', 'CIPRODEX', 'SIMBRINZA', 'AZOPT',
                    'DUREZOL', 'PAZEO', 'EYSUVIS', 'TRAVATAN Z', 'PATADAY',
                    'SYSTANE', 'VIGAMOX', 'NEVANAC', 'ILEVRO', 'COMBIGAN'
                ))
    )
    SELECT 
        COUNT(DISTINCT NPI) as unique_prescribers,
        COUNT(DISTINCT BRAND_NAME) as unique_drugs,
        COUNT(DISTINCT MANUFACTURER) as unique_manufacturers,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        SUM(CHARGES) as total_charges,
        SUM(UNIQUE_PATIENTS) as total_patients,
        SUM(DAYS_SUPPLY) as total_days_supply,
        AVG(PAYMENTS/NULLIF(PRESCRIPTIONS, 0)) as avg_payment_per_prescription,
        MIN(CLAIM_YEAR) as min_year,
        MAX(CLAIM_YEAR) as max_year
    FROM alcon_rx
    """
    
    logger.info("Analyzing overall Alcon prescribing metrics...")
    df = client.query(query).to_dataframe()
    
    if not df.empty:
        row = df.iloc[0]
        logger.info(f"  Unique Prescribers: {row['unique_prescribers']:,.0f}")
        logger.info(f"  Unique Drugs: {row['unique_drugs']:,.0f}")
        logger.info(f"  Total Prescriptions: {row['total_prescriptions']:,.0f}")
        logger.info(f"  Total Payments: ${row['total_payments']:,.2f}")
        logger.info(f"  Total Patients: {row['total_patients']:,.0f}")
        logger.info(f"  Avg Cost per Prescription: ${row['avg_payment_per_prescription']:,.2f}")
    
    return df

def analyze_prescribing_by_year(client):
    """Analyze Alcon prescribing trends by year"""
    
    query = f"""
    WITH alcon_rx AS (
        SELECT 
            NPI,
            CLAIM_YEAR,
            BRAND_NAME,
            PAYMENTS,
            PRESCRIPTIONS,
            UNIQUE_PATIENTS
        FROM `{RX_TABLE}`
        WHERE CLAIM_YEAR BETWEEN 2020 AND 2024
            AND (UPPER(MANUFACTURER) LIKE '%ALCON%'
                OR UPPER(BRAND_NAME) IN (
                    'RHOPRESSA', 'ROCKLATAN', 'CIPRODEX', 'SIMBRINZA', 'AZOPT',
                    'DUREZOL', 'PAZEO', 'EYSUVIS', 'TRAVATAN Z', 'PATADAY',
                    'SYSTANE', 'VIGAMOX', 'NEVANAC', 'ILEVRO', 'COMBIGAN'
                ))
    )
    SELECT 
        CLAIM_YEAR,
        COUNT(DISTINCT NPI) as unique_prescribers,
        COUNT(DISTINCT BRAND_NAME) as unique_products,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        SUM(UNIQUE_PATIENTS) as total_patients,
        AVG(PAYMENTS/NULLIF(PRESCRIPTIONS, 0)) as avg_cost_per_prescription,
        -- Year-over-year growth
        LAG(SUM(PAYMENTS)) OVER (ORDER BY CLAIM_YEAR) as prev_year_payments,
        (SUM(PAYMENTS) - LAG(SUM(PAYMENTS)) OVER (ORDER BY CLAIM_YEAR)) / 
            NULLIF(LAG(SUM(PAYMENTS)) OVER (ORDER BY CLAIM_YEAR), 0) * 100 as yoy_growth_pct
    FROM alcon_rx
    GROUP BY CLAIM_YEAR
    ORDER BY CLAIM_YEAR
    """
    
    logger.info("\nAnalyzing Alcon prescribing by year...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nYearly Prescribing Trends:")
    for _, row in df.iterrows():
        growth_str = f" ({row['yoy_growth_pct']:.1f}% YoY)" if pd.notna(row['yoy_growth_pct']) else ""
        logger.info(f"  {row['CLAIM_YEAR']}: ${row['total_payments']:,.0f} for {row['total_prescriptions']:,.0f} prescriptions{growth_str}")
    
    return df

def analyze_top_drugs(client):
    """Identify top Alcon drugs by various metrics"""
    
    query = f"""
    WITH alcon_rx AS (
        SELECT 
            BRAND_NAME,
            GENERIC_NAME,
            MANUFACTURER,
            NPI,
            PAYMENTS,
            PRESCRIPTIONS,
            UNIQUE_PATIENTS,
            CHARGES
        FROM `{RX_TABLE}`
        WHERE CLAIM_YEAR BETWEEN 2020 AND 2024
            AND (UPPER(MANUFACTURER) LIKE '%ALCON%'
                OR UPPER(BRAND_NAME) IN (
                    'RHOPRESSA', 'ROCKLATAN', 'CIPRODEX', 'SIMBRINZA', 'AZOPT',
                    'DUREZOL', 'PAZEO', 'EYSUVIS', 'TRAVATAN Z', 'PATADAY',
                    'SYSTANE', 'VIGAMOX', 'NEVANAC', 'ILEVRO', 'COMBIGAN'
                ))
    )
    SELECT 
        BRAND_NAME,
        GENERIC_NAME,
        MAX(MANUFACTURER) as MANUFACTURER,
        COUNT(DISTINCT NPI) as unique_prescribers,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        SUM(CHARGES) as total_charges,
        SUM(UNIQUE_PATIENTS) as total_patients,
        AVG(PAYMENTS/NULLIF(PRESCRIPTIONS, 0)) as avg_cost_per_prescription,
        SUM(PAYMENTS)/NULLIF(SUM(PRESCRIPTIONS), 0) as weighted_avg_cost
    FROM alcon_rx
    GROUP BY BRAND_NAME, GENERIC_NAME
    ORDER BY total_payments DESC
    LIMIT 20
    """
    
    logger.info("\nAnalyzing top Alcon drugs...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nTop 10 Alcon Drugs by Payment Volume:")
    for _, row in df.head(10).iterrows():
        logger.info(f"  {row['BRAND_NAME']}: ${row['total_payments']:,.0f} ({row['unique_prescribers']:,} prescribers)")
    
    return df

def analyze_prescribing_by_specialty(client):
    """Analyze Alcon prescribing patterns by medical specialty"""
    
    query = f"""
    WITH provider_specialties AS (
        SELECT DISTINCT
            CAST(covered_recipient_npi AS STRING) AS NPI,
            covered_recipient_specialty_1 as specialty
        FROM `{OP_TABLE}`
        WHERE covered_recipient_npi IS NOT NULL
            AND covered_recipient_specialty_1 IS NOT NULL
    ),
    alcon_rx AS (
        SELECT 
            rx.NPI,
            rx.BRAND_NAME,
            rx.PAYMENTS,
            rx.PRESCRIPTIONS,
            rx.UNIQUE_PATIENTS,
            ps.specialty
        FROM `{RX_TABLE}` rx
        LEFT JOIN provider_specialties ps ON CAST(rx.NPI AS STRING) = ps.NPI
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
            AND (UPPER(rx.MANUFACTURER) LIKE '%ALCON%'
                OR UPPER(rx.BRAND_NAME) IN (
                    'RHOPRESSA', 'ROCKLATAN', 'CIPRODEX', 'SIMBRINZA', 'AZOPT',
                    'DUREZOL', 'PAZEO', 'EYSUVIS', 'TRAVATAN Z', 'PATADAY',
                    'SYSTANE', 'VIGAMOX', 'NEVANAC', 'ILEVRO', 'COMBIGAN'
                ))
    )
    SELECT 
        COALESCE(specialty, 'Unknown') as specialty,
        COUNT(DISTINCT NPI) as unique_prescribers,
        COUNT(DISTINCT BRAND_NAME) as unique_drugs,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        SUM(UNIQUE_PATIENTS) as total_patients,
        AVG(PAYMENTS/NULLIF(PRESCRIPTIONS, 0)) as avg_cost_per_prescription,
        SUM(PAYMENTS)/COUNT(DISTINCT NPI) as avg_payments_per_provider
    FROM alcon_rx
    GROUP BY specialty
    ORDER BY total_payments DESC
    LIMIT 20
    """
    
    logger.info("\nAnalyzing Alcon prescribing by specialty...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nTop Specialties Prescribing Alcon Products:")
    for _, row in df.head(10).iterrows():
        logger.info(f"  {row['specialty']}: ${row['total_payments']:,.0f} ({row['unique_prescribers']:,} prescribers)")
    
    return df

def analyze_high_volume_prescribers(client):
    """Identify high-volume Alcon prescribers"""
    
    query = f"""
    WITH provider_specialties AS (
        SELECT DISTINCT
            CAST(covered_recipient_npi AS STRING) AS NPI,
            covered_recipient_specialty_1 as specialty,
            covered_recipient_first_name as first_name,
            covered_recipient_last_name as last_name
        FROM `{OP_TABLE}`
        WHERE covered_recipient_npi IS NOT NULL
    ),
    alcon_prescribers AS (
        SELECT 
            CAST(rx.NPI AS STRING) AS NPI,
            SUM(rx.PAYMENTS) as total_payments,
            SUM(rx.PRESCRIPTIONS) as total_prescriptions,
            SUM(rx.UNIQUE_PATIENTS) as total_patients,
            COUNT(DISTINCT rx.BRAND_NAME) as unique_drugs,
            COUNT(DISTINCT rx.CLAIM_YEAR) as years_active
        FROM `{RX_TABLE}` rx
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
            AND (UPPER(rx.MANUFACTURER) LIKE '%ALCON%'
                OR UPPER(rx.BRAND_NAME) IN (
                    'RHOPRESSA', 'ROCKLATAN', 'CIPRODEX', 'SIMBRINZA', 'AZOPT',
                    'DUREZOL', 'PAZEO', 'EYSUVIS', 'TRAVATAN Z', 'PATADAY',
                    'SYSTANE', 'VIGAMOX', 'NEVANAC', 'ILEVRO', 'COMBIGAN'
                ))
        GROUP BY rx.NPI
    )
    SELECT 
        ap.NPI,
        ps.specialty,
        ps.first_name,
        ps.last_name,
        ap.total_payments,
        ap.total_prescriptions,
        ap.total_patients,
        ap.unique_drugs,
        ap.years_active,
        ap.total_payments/NULLIF(ap.total_prescriptions, 0) as avg_cost_per_prescription
    FROM alcon_prescribers ap
    LEFT JOIN provider_specialties ps ON ap.NPI = ps.NPI
    ORDER BY ap.total_payments DESC
    LIMIT 100
    """
    
    logger.info("\nIdentifying high-volume Alcon prescribers...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nTop 10 Alcon Prescribers by Volume:")
    for _, row in df.head(10).iterrows():
        name = f"{row['first_name'] or ''} {row['last_name'] or ''}".strip() or "Unknown"
        specialty = row['specialty'] or "Unknown"
        logger.info(f"  {name} ({specialty}): ${row['total_payments']:,.0f} for {row['total_prescriptions']:,} Rx")
    
    return df

def analyze_drug_combinations(client):
    """Analyze common Alcon drug combinations prescribed together"""
    
    query = f"""
    WITH alcon_prescribers AS (
        SELECT 
            NPI,
            BRAND_NAME,
            SUM(PAYMENTS) as payments,
            SUM(PRESCRIPTIONS) as prescriptions
        FROM `{RX_TABLE}`
        WHERE CLAIM_YEAR BETWEEN 2020 AND 2024
            AND (UPPER(MANUFACTURER) LIKE '%ALCON%'
                OR UPPER(BRAND_NAME) IN (
                    'RHOPRESSA', 'ROCKLATAN', 'CIPRODEX', 'SIMBRINZA', 'AZOPT',
                    'DUREZOL', 'PAZEO', 'EYSUVIS', 'TRAVATAN Z', 'PATADAY',
                    'SYSTANE', 'VIGAMOX', 'NEVANAC', 'ILEVRO', 'COMBIGAN'
                ))
        GROUP BY NPI, BRAND_NAME
    ),
    providers_multi_drug AS (
        SELECT 
            NPI,
            COUNT(DISTINCT BRAND_NAME) as drug_count,
            STRING_AGG(BRAND_NAME, ', ' ORDER BY payments DESC LIMIT 5) as top_drugs,
            SUM(payments) as total_payments,
            SUM(prescriptions) as total_prescriptions
        FROM alcon_prescribers
        GROUP BY NPI
        HAVING COUNT(DISTINCT BRAND_NAME) >= 2
    )
    SELECT 
        drug_count,
        COUNT(DISTINCT NPI) as provider_count,
        SUM(total_payments) as total_payments,
        SUM(total_prescriptions) as total_prescriptions,
        AVG(total_payments) as avg_payments_per_provider
    FROM providers_multi_drug
    GROUP BY drug_count
    ORDER BY drug_count
    """
    
    logger.info("\nAnalyzing Alcon drug combinations...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nProviders Prescribing Multiple Alcon Products:")
    for _, row in df.iterrows():
        if row['drug_count'] <= 10:  # Limit display
            logger.info(f"  {row['drug_count']} drugs: {row['provider_count']:,} providers, ${row['avg_payments_per_provider']:,.0f} avg")
    
    return df

def analyze_geographic_distribution(client):
    """Analyze geographic distribution of Alcon prescribing"""
    
    query = f"""
    WITH provider_locations AS (
        SELECT DISTINCT
            CAST(covered_recipient_npi AS STRING) AS NPI,
            aff.AFFILIATED_HQ_STATE as state
        FROM `{OP_TABLE}` op,
        UNNEST(physician.affiliations) as aff
        WHERE covered_recipient_npi IS NOT NULL
            AND aff.PRIMARY_AFFILIATED_FACILITY_FLAG = 'Y'
            AND aff.AFFILIATED_HQ_STATE IS NOT NULL
    ),
    alcon_rx_by_state AS (
        SELECT 
            pl.state,
            COUNT(DISTINCT rx.NPI) as unique_prescribers,
            SUM(rx.PAYMENTS) as total_payments,
            SUM(rx.PRESCRIPTIONS) as total_prescriptions,
            SUM(rx.UNIQUE_PATIENTS) as total_patients
        FROM `{RX_TABLE}` rx
        INNER JOIN provider_locations pl ON CAST(rx.NPI AS STRING) = pl.NPI
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
            AND (UPPER(rx.MANUFACTURER) LIKE '%ALCON%'
                OR UPPER(rx.BRAND_NAME) IN (
                    'RHOPRESSA', 'ROCKLATAN', 'CIPRODEX', 'SIMBRINZA', 'AZOPT',
                    'DUREZOL', 'PAZEO', 'EYSUVIS', 'TRAVATAN Z', 'PATADAY',
                    'SYSTANE', 'VIGAMOX', 'NEVANAC', 'ILEVRO', 'COMBIGAN'
                ))
        GROUP BY pl.state
    )
    SELECT 
        state,
        unique_prescribers,
        total_payments,
        total_prescriptions,
        total_patients,
        total_payments/NULLIF(unique_prescribers, 0) as avg_payment_per_prescriber,
        100.0 * total_payments / SUM(total_payments) OVER() as pct_of_total_payments
    FROM alcon_rx_by_state
    ORDER BY total_payments DESC
    LIMIT 20
    """
    
    logger.info("\nAnalyzing geographic distribution of Alcon prescribing...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nTop 10 States by Alcon Prescription Volume:")
    for _, row in df.head(10).iterrows():
        logger.info(f"  {row['state']}: ${row['total_payments']:,.0f} ({row['pct_of_total_payments']:.1f}% of total)")
    
    return df

def analyze_market_share(client):
    """Analyze Alcon's market share in key therapeutic categories"""
    
    query = f"""
    -- Analyze glaucoma market share
    WITH glaucoma_drugs AS (
        SELECT 
            BRAND_NAME,
            MANUFACTURER,
            SUM(PAYMENTS) as total_payments,
            SUM(PRESCRIPTIONS) as total_prescriptions,
            COUNT(DISTINCT NPI) as prescribers
        FROM `{RX_TABLE}`
        WHERE CLAIM_YEAR BETWEEN 2020 AND 2024
            AND UPPER(BRAND_NAME) IN (
                'ROCKLATAN', 'RHOPRESSA', 'SIMBRINZA', 'AZOPT', 'TRAVATAN Z',
                'COMBIGAN', 'LATANOPROST', 'TIMOLOL', 'XALATAN', 'LUMIGAN',
                'ALPHAGAN', 'COSOPT', 'VYZULTA', 'ZIOPTAN'
            )
        GROUP BY BRAND_NAME, MANUFACTURER
    )
    SELECT 
        CASE 
            WHEN UPPER(MANUFACTURER) LIKE '%ALCON%' 
                OR UPPER(BRAND_NAME) IN ('ROCKLATAN', 'RHOPRESSA', 'SIMBRINZA', 'AZOPT', 'TRAVATAN Z', 'COMBIGAN')
            THEN 'Alcon'
            ELSE 'Other'
        END as manufacturer_group,
        SUM(total_payments) as total_payments,
        SUM(total_prescriptions) as total_prescriptions,
        COUNT(DISTINCT BRAND_NAME) as unique_products,
        100.0 * SUM(total_payments) / SUM(SUM(total_payments)) OVER() as market_share_pct
    FROM glaucoma_drugs
    GROUP BY manufacturer_group
    ORDER BY total_payments DESC
    """
    
    logger.info("\nAnalyzing Alcon market share in glaucoma category...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nAlcon Market Share in Glaucoma:")
    for _, row in df.iterrows():
        logger.info(f"  {row['manufacturer_group']}: ${row['total_payments']:,.0f} ({row['market_share_pct']:.1f}% market share)")
    
    return df

def main():
    """Main execution function"""
    try:
        logger.info("=" * 80)
        logger.info("ALCON PRESCRIPTION PATTERNS ANALYSIS")
        logger.info("=" * 80)
        
        # Create BigQuery client
        client = create_bigquery_client()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Run analyses
        logger.info("\n1. OVERALL PRESCRIBING METRICS")
        overall = analyze_overall_prescribing(client)
        overall.to_csv(OUTPUT_DIR / f"alcon_overall_prescribing_{timestamp}.csv", index=False)
        
        logger.info("\n2. YEARLY TRENDS")
        yearly = analyze_prescribing_by_year(client)
        yearly.to_csv(OUTPUT_DIR / f"alcon_yearly_trends_{timestamp}.csv", index=False)
        
        logger.info("\n3. TOP PRODUCTS")
        top_drugs = analyze_top_drugs(client)
        top_drugs.to_csv(OUTPUT_DIR / f"alcon_top_drugs_{timestamp}.csv", index=False)
        
        logger.info("\n4. SPECIALTY ANALYSIS")
        specialty = analyze_prescribing_by_specialty(client)
        specialty.to_csv(OUTPUT_DIR / f"alcon_specialty_prescribing_{timestamp}.csv", index=False)
        
        logger.info("\n5. HIGH-VOLUME PRESCRIBERS")
        high_volume = analyze_high_volume_prescribers(client)
        high_volume.to_csv(OUTPUT_DIR / f"alcon_high_volume_prescribers_{timestamp}.csv", index=False)
        
        logger.info("\n6. DRUG COMBINATIONS")
        combinations = analyze_drug_combinations(client)
        combinations.to_csv(OUTPUT_DIR / f"alcon_drug_combinations_{timestamp}.csv", index=False)
        
        logger.info("\n7. GEOGRAPHIC DISTRIBUTION")
        geographic = analyze_geographic_distribution(client)
        geographic.to_csv(OUTPUT_DIR / f"alcon_geographic_distribution_{timestamp}.csv", index=False)
        
        logger.info("\n8. MARKET SHARE ANALYSIS")
        market_share = analyze_market_share(client)
        market_share.to_csv(OUTPUT_DIR / f"alcon_market_share_{timestamp}.csv", index=False)
        
        # Summary statistics
        logger.info("\n" + "=" * 80)
        logger.info("ANALYSIS SUMMARY")
        logger.info("=" * 80)
        
        if not overall.empty:
            logger.info(f"\nTotal Alcon Market:")
            logger.info(f"  Prescribers: {overall['unique_prescribers'].iloc[0]:,.0f}")
            logger.info(f"  Products: {overall['unique_drugs'].iloc[0]:,.0f}")
            logger.info(f"  Total Value: ${overall['total_payments'].iloc[0]:,.2f}")
            logger.info(f"  Total Prescriptions: {overall['total_prescriptions'].iloc[0]:,.0f}")
            logger.info(f"  Total Patients: {overall['total_patients'].iloc[0]:,.0f}")
        
        if not yearly.empty:
            latest = yearly.iloc[-1]
            logger.info(f"\n2024 Performance:")
            logger.info(f"  Prescriptions: {latest['total_prescriptions']:,.0f}")
            logger.info(f"  Value: ${latest['total_payments']:,.2f}")
            if pd.notna(latest['yoy_growth_pct']):
                logger.info(f"  YoY Growth: {latest['yoy_growth_pct']:.1f}%")
        
        logger.info("\n" + "=" * 80)
        logger.info("PRESCRIPTION PATTERNS ANALYSIS COMPLETE")
        logger.info(f"Results saved to: {OUTPUT_DIR}")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"Error during prescription patterns analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)