#!/usr/bin/env python3
"""
Analyze Corewell Health providers using rx_op_enhanced data
DA-175: Corewell Health Analysis - Payment Attribution Analysis

This script performs comprehensive analysis including:
1. Payment attribution metrics
2. ROI calculations
3. High-risk provider identification
4. Specialty and geographic patterns
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
OUTPUT_DIR = PROJECT_DIR / "rx-op-enhanced" / "data" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# BigQuery configuration
PROJECT_ID = "data-analytics-389803"
DATASET_ID = "conflixis_agent"
NPI_TABLE = f"{PROJECT_ID}.{DATASET_ID}.corewell_health_npis"
RX_OP_TABLE = f"{PROJECT_ID}.{DATASET_ID}.rx_op_enhanced_full"

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
    
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    return client

def analyze_overall_metrics(client):
    """Analyze overall payment and attribution metrics for Corewell Health providers"""
    logger.info("Analyzing overall metrics...")
    
    query = f"""
    WITH cs_payments AS (
        SELECT 
            cs.NPI,
            cs.Full_Name,
            cs.Primary_Specialty,
            cs.Primary_Hospital_Affiliation,
            cs.source_hospital_system,
            rx.source_manufacturer,
            rx.year,
            rx.month,
            rx.TotalDollarsFrom,
            rx.totalNext6,
            rx.attributable_dollars,
            rx.attributable_pct,
            rx.pred_rx,
            rx.pred_rx_cf,
            rx.delta_rx
        FROM `{NPI_TABLE}` cs
        INNER JOIN `{RX_OP_TABLE}` rx
            ON cs.NPI = rx.NPI
    ),
    summary AS (
        SELECT 
            COUNT(DISTINCT NPI) as unique_providers,
            COUNT(DISTINCT CASE WHEN TotalDollarsFrom > 0 THEN NPI END) as providers_with_payments,
            COUNT(*) as total_observations,
            
            -- Payment metrics
            SUM(TotalDollarsFrom) as total_payments_received,
            AVG(CASE WHEN TotalDollarsFrom > 0 THEN TotalDollarsFrom END) as avg_payment_when_paid,
            
            -- Prescription metrics
            SUM(totalNext6) as total_prescribed,
            SUM(attributable_dollars) as total_attributable,
            
            -- Attribution metrics
            AVG(attributable_pct) as avg_attribution_rate,
            AVG(CASE WHEN TotalDollarsFrom > 0 THEN attributable_pct END) as avg_attribution_when_paid,
            
            -- ROI
            SAFE_DIVIDE(SUM(attributable_dollars), SUM(TotalDollarsFrom)) as overall_roi
        FROM cs_payments
    )
    SELECT * FROM summary
    """
    
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        row = results.iloc[0]
        logger.info("\n" + "=" * 60)
        logger.info("COREWELL HEALTH OVERALL METRICS")
        logger.info("=" * 60)
        logger.info(f"Unique Providers: {row['unique_providers']:,}")
        logger.info(f"Providers with Payments: {row['providers_with_payments']:,} ({row['providers_with_payments']/row['unique_providers']*100:.1f}%)")
        logger.info(f"Total Observations: {row['total_observations']:,}")
        logger.info(f"\nPayment Metrics:")
        logger.info(f"  Total Payments Received: ${row['total_payments_received']:,.2f}")
        logger.info(f"  Average Payment (when paid): ${row['avg_payment_when_paid']:,.2f}")
        logger.info(f"\nPrescription Metrics:")
        logger.info(f"  Total Prescribed: ${row['total_prescribed']:,.2f}")
        logger.info(f"  Total Attributable: ${row['total_attributable']:,.2f}")
        logger.info(f"  Attribution Rate: {row['avg_attribution_rate']*100:.3f}%")
        logger.info(f"  Attribution Rate (when paid): {row['avg_attribution_when_paid']*100:.3f}%")
        logger.info(f"\nROI: {row['overall_roi']:.2f}x")
    
    return results

def analyze_by_manufacturer(client):
    """Analyze metrics by manufacturer"""
    logger.info("\nAnalyzing by manufacturer...")
    
    query = f"""
    WITH cs_mfg AS (
        SELECT 
            rx.source_manufacturer,
            COUNT(DISTINCT cs.NPI) as unique_providers,
            COUNT(*) as observations,
            SUM(rx.TotalDollarsFrom) as total_payments,
            SUM(rx.totalNext6) as total_prescribed,
            SUM(rx.attributable_dollars) as total_attributable,
            AVG(rx.attributable_pct) as avg_attribution,
            SAFE_DIVIDE(SUM(rx.attributable_dollars), SUM(rx.TotalDollarsFrom)) as roi
        FROM `{NPI_TABLE}` cs
        INNER JOIN `{RX_OP_TABLE}` rx
            ON cs.NPI = rx.NPI
        WHERE rx.TotalDollarsFrom > 0
        GROUP BY rx.source_manufacturer
    )
    SELECT 
        source_manufacturer,
        unique_providers,
        observations,
        ROUND(total_payments, 2) as total_payments,
        ROUND(total_prescribed, 2) as total_prescribed,
        ROUND(total_attributable, 2) as total_attributable,
        ROUND(avg_attribution * 100, 3) as avg_attribution_pct,
        ROUND(roi, 2) as roi
    FROM cs_mfg
    WHERE total_payments > 10000  -- Significant relationships only
    ORDER BY total_attributable DESC
    LIMIT 20
    """
    
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        logger.info("\n" + "=" * 60)
        logger.info("TOP MANUFACTURERS BY ATTRIBUTABLE REVENUE")
        logger.info("=" * 60)
        for _, row in results.head(10).iterrows():
            logger.info(f"\n{row['source_manufacturer']}:")
            logger.info(f"  Providers: {row['unique_providers']:,}")
            logger.info(f"  Payments: ${row['total_payments']:,.0f}")
            logger.info(f"  Attributable: ${row['total_attributable']:,.0f}")
            logger.info(f"  Attribution: {row['avg_attribution_pct']:.2f}%")
            logger.info(f"  ROI: {row['roi']:.1f}x")
    
    return results

def analyze_by_specialty(client):
    """Analyze metrics by specialty"""
    logger.info("\nAnalyzing by specialty...")
    
    query = f"""
    WITH cs_spec AS (
        SELECT 
            cs.Primary_Specialty,
            COUNT(DISTINCT cs.NPI) as unique_providers,
            COUNT(DISTINCT CASE WHEN rx.TotalDollarsFrom > 0 THEN cs.NPI END) as paid_providers,
            SUM(rx.TotalDollarsFrom) as total_payments,
            SUM(rx.totalNext6) as total_prescribed,
            SUM(rx.attributable_dollars) as total_attributable,
            AVG(CASE WHEN rx.TotalDollarsFrom > 0 THEN rx.attributable_pct END) as avg_attribution_when_paid,
            SAFE_DIVIDE(SUM(rx.attributable_dollars), SUM(rx.TotalDollarsFrom)) as roi
        FROM `{NPI_TABLE}` cs
        LEFT JOIN `{RX_OP_TABLE}` rx
            ON cs.NPI = rx.NPI
        WHERE cs.Primary_Specialty IS NOT NULL
        GROUP BY cs.Primary_Specialty
    )
    SELECT 
        Primary_Specialty,
        unique_providers,
        paid_providers,
        ROUND(total_payments, 2) as total_payments,
        ROUND(total_prescribed, 2) as total_prescribed,
        ROUND(total_attributable, 2) as total_attributable,
        ROUND(avg_attribution_when_paid * 100, 3) as avg_attribution_pct,
        ROUND(roi, 2) as roi
    FROM cs_spec
    WHERE unique_providers >= 10  -- Minimum sample size
    ORDER BY total_attributable DESC NULLS LAST
    LIMIT 20
    """
    
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        logger.info("\n" + "=" * 60)
        logger.info("TOP SPECIALTIES BY ATTRIBUTABLE REVENUE")
        logger.info("=" * 60)
        for _, row in results.head(10).iterrows():
            logger.info(f"\n{row['Primary_Specialty']}:")
            logger.info(f"  Providers: {row['unique_providers']:,} ({row['paid_providers']:,} paid)")
            logger.info(f"  Payments: ${row['total_payments']:,.0f}")
            logger.info(f"  Attributable: ${row['total_attributable']:,.0f}")
            if pd.notna(row['avg_attribution_pct']):
                logger.info(f"  Attribution: {row['avg_attribution_pct']:.2f}%")
            if pd.notna(row['roi']) and row['roi'] > 0:
                logger.info(f"  ROI: {row['roi']:.1f}x")
    
    return results

def identify_high_risk_providers(client):
    """Identify high-risk providers with >30% attribution"""
    logger.info("\nIdentifying high-risk providers...")
    
    query = f"""
    WITH provider_risk AS (
        SELECT 
            cs.NPI,
            cs.Full_Name,
            cs.Primary_Specialty,
            cs.Primary_Hospital_Affiliation,
            COUNT(DISTINCT rx.source_manufacturer) as num_manufacturers,
            COUNT(*) as observations,
            SUM(rx.TotalDollarsFrom) as total_payments,
            SUM(rx.attributable_dollars) as total_attributable,
            AVG(rx.attributable_pct) as avg_attribution_pct,
            MAX(rx.attributable_pct) as max_attribution_pct
        FROM `{NPI_TABLE}` cs
        INNER JOIN `{RX_OP_TABLE}` rx
            ON cs.NPI = rx.NPI
        WHERE rx.TotalDollarsFrom > 0
        GROUP BY cs.NPI, cs.Full_Name, cs.Primary_Specialty, cs.Primary_Hospital_Affiliation
        HAVING AVG(rx.attributable_pct) > 0.30  -- High attribution threshold
    )
    SELECT 
        NPI,
        Full_Name,
        Primary_Specialty,
        Primary_Hospital_Affiliation,
        num_manufacturers,
        observations,
        ROUND(total_payments, 2) as total_payments,
        ROUND(total_attributable, 2) as total_attributable,
        ROUND(avg_attribution_pct * 100, 2) as avg_attribution_pct,
        ROUND(max_attribution_pct * 100, 2) as max_attribution_pct
    FROM provider_risk
    ORDER BY avg_attribution_pct DESC
    LIMIT 100
    """
    
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        logger.info("\n" + "=" * 60)
        logger.info("HIGH-RISK PROVIDERS (>30% Attribution)")
        logger.info("=" * 60)
        logger.info(f"Total High-Risk Providers: {len(results)}")
        
        for _, row in results.head(10).iterrows():
            logger.info(f"\nNPI: {row['NPI']}")
            logger.info(f"  Name: {row['Full_Name']}")
            logger.info(f"  Specialty: {row['Primary_Specialty']}")
            logger.info(f"  Hospital: {row['Primary_Hospital_Affiliation']}")
            logger.info(f"  Manufacturers: {row['num_manufacturers']}")
            logger.info(f"  Avg Attribution: {row['avg_attribution_pct']:.1f}%")
            logger.info(f"  Max Attribution: {row['max_attribution_pct']:.1f}%")
            logger.info(f"  Total Payments: ${row['total_payments']:,.0f}")
            logger.info(f"  Attributable Revenue: ${row['total_attributable']:,.0f}")
    
    return results

def analyze_payment_tiers(client):
    """Analyze effectiveness by payment tier"""
    logger.info("\nAnalyzing payment tiers...")
    
    query = f"""
    WITH payment_tiers AS (
        SELECT 
            CASE 
                WHEN rx.TotalDollarsFrom = 0 THEN '0. No Payment'
                WHEN rx.TotalDollarsFrom <= 100 THEN '1. $1-100'
                WHEN rx.TotalDollarsFrom <= 500 THEN '2. $101-500'
                WHEN rx.TotalDollarsFrom <= 1000 THEN '3. $501-1000'
                WHEN rx.TotalDollarsFrom <= 5000 THEN '4. $1001-5000'
                ELSE '5. $5000+'
            END as payment_tier,
            cs.NPI,
            rx.TotalDollarsFrom,
            rx.totalNext6,
            rx.attributable_dollars,
            rx.attributable_pct
        FROM `{NPI_TABLE}` cs
        INNER JOIN `{RX_OP_TABLE}` rx
            ON cs.NPI = rx.NPI
    )
    SELECT 
        payment_tier,
        COUNT(DISTINCT NPI) as providers,
        COUNT(*) as observations,
        ROUND(AVG(TotalDollarsFrom), 2) as avg_payment,
        ROUND(AVG(totalNext6), 2) as avg_prescribed,
        ROUND(AVG(attributable_dollars), 2) as avg_attributable,
        ROUND(AVG(attributable_pct) * 100, 3) as avg_attribution_pct,
        ROUND(SAFE_DIVIDE(SUM(attributable_dollars), SUM(TotalDollarsFrom)), 2) as roi
    FROM payment_tiers
    GROUP BY payment_tier
    ORDER BY payment_tier
    """
    
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        logger.info("\n" + "=" * 60)
        logger.info("PAYMENT TIER ANALYSIS")
        logger.info("=" * 60)
        for _, row in results.iterrows():
            logger.info(f"\n{row['payment_tier']}:")
            logger.info(f"  Providers: {row['providers']:,}")
            logger.info(f"  Observations: {row['observations']:,}")
            logger.info(f"  Avg Payment: ${row['avg_payment']:,.0f}")
            logger.info(f"  Avg Attribution: {row['avg_attribution_pct']:.2f}%")
            if pd.notna(row['roi']) and row['payment_tier'] != '0. No Payment':
                logger.info(f"  ROI: {row['roi']:.1f}x")
    
    return results

def analyze_np_pa_vulnerability(client):
    """Special analysis for NPs and PAs"""
    logger.info("\nAnalyzing NP/PA vulnerability...")
    
    query = f"""
    WITH np_pa AS (
        SELECT 
            CASE 
                WHEN cs.Primary_Specialty LIKE '%Nurse Practitioner%' THEN 'NP'
                WHEN cs.Primary_Specialty LIKE '%Physician Assistant%' THEN 'PA'
                ELSE 'Other'
            END as provider_type,
            cs.NPI,
            rx.source_manufacturer,
            rx.TotalDollarsFrom,
            rx.attributable_dollars,
            rx.attributable_pct
        FROM `{NPI_TABLE}` cs
        INNER JOIN `{RX_OP_TABLE}` rx
            ON cs.NPI = rx.NPI
        WHERE cs.Primary_Specialty IN (
            'Nurse - Nurse Practitioner',
            'Physician Assistant'
        )
    )
    SELECT 
        provider_type,
        COUNT(DISTINCT NPI) as unique_providers,
        COUNT(DISTINCT CASE WHEN TotalDollarsFrom > 0 THEN NPI END) as paid_providers,
        SUM(TotalDollarsFrom) as total_payments,
        SUM(attributable_dollars) as total_attributable,
        AVG(CASE WHEN TotalDollarsFrom > 0 THEN attributable_pct END) as avg_attribution_when_paid,
        SAFE_DIVIDE(SUM(attributable_dollars), SUM(TotalDollarsFrom)) as roi,
        
        -- Compare to overall average
        (SELECT AVG(attributable_pct) 
         FROM `{NPI_TABLE}` cs2
         INNER JOIN `{RX_OP_TABLE}` rx2 ON cs2.NPI = rx2.NPI
         WHERE rx2.TotalDollarsFrom > 0
         AND cs2.Primary_Specialty NOT IN ('Nurse - Nurse Practitioner', 'Physician Assistant')
        ) as physician_avg_attribution
    FROM np_pa
    WHERE provider_type IN ('NP', 'PA')
    GROUP BY provider_type
    """
    
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        logger.info("\n" + "=" * 60)
        logger.info("NP/PA VULNERABILITY ANALYSIS")
        logger.info("=" * 60)
        
        physician_avg = results.iloc[0]['physician_avg_attribution'] if 'physician_avg_attribution' in results.columns else 0
        
        for _, row in results.iterrows():
            logger.info(f"\n{row['provider_type']}:")
            logger.info(f"  Total Providers: {row['unique_providers']:,}")
            logger.info(f"  Paid Providers: {row['paid_providers']:,}")
            logger.info(f"  Total Payments: ${row['total_payments']:,.0f}")
            logger.info(f"  Attributable Revenue: ${row['total_attributable']:,.0f}")
            logger.info(f"  Attribution Rate: {row['avg_attribution_when_paid']*100:.3f}%")
            logger.info(f"  ROI: {row['roi']:.2f}x")
            
            if physician_avg > 0:
                vulnerability = (row['avg_attribution_when_paid'] - physician_avg) / physician_avg * 100
                logger.info(f"  Vulnerability vs Physicians: {vulnerability:+.1f}%")
    
    return results

def save_results(dfs_dict):
    """Save all results to CSV files"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for name, df in dfs_dict.items():
        if df is not None and not df.empty:
            filename = OUTPUT_DIR / f"corewell_health_{name}_{timestamp}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"Saved {name} to {filename}")

def main():
    """Main execution function"""
    logger.info("=" * 80)
    logger.info("Corewell Health Payment Attribution Analysis")
    logger.info("=" * 80)
    
    try:
        # Create BigQuery client
        client = create_bigquery_client()
        
        # Run all analyses
        results = {}
        
        results['overall'] = analyze_overall_metrics(client)
        results['by_manufacturer'] = analyze_by_manufacturer(client)
        results['by_specialty'] = analyze_by_specialty(client)
        results['high_risk'] = identify_high_risk_providers(client)
        results['payment_tiers'] = analyze_payment_tiers(client)
        results['np_pa'] = analyze_np_pa_vulnerability(client)
        
        # Save results
        save_results(results)
        
        logger.info("\n" + "=" * 80)
        logger.info("Analysis Complete!")
        logger.info("=" * 80)
        
        return results
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    results = main()