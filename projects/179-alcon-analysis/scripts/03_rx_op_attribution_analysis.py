#!/usr/bin/env python3
"""
Rx-OP Attribution Analysis for Alcon
DA-179: Alcon Analysis - Prescription Attribution Analysis

This script analyzes the rx_op_enhanced data to find Alcon-related
prescription attribution patterns.
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
RX_OP_TABLE = f"{PROJECT_ID}.{DATASET_ID}.rx_op_enhanced_full"
PHYSICIAN_RX_TABLE = f"{PROJECT_ID}.{DATASET_ID}.PHYSICIAN_RX_2020_2024"

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

def check_alcon_in_rx_op(client):
    """Check if Alcon exists in the rx_op_enhanced table"""
    logger.info("Checking for Alcon data in rx_op_enhanced table...")
    
    # First, let's check what manufacturers are in the table
    query = f"""
    SELECT DISTINCT
        manufacturer
    FROM `{RX_OP_TABLE}`
    WHERE UPPER(manufacturer) LIKE '%ALCON%'
    ORDER BY manufacturer
    """
    
    try:
        results = client.query(query).to_dataframe()
        
        if not results.empty:
            logger.info(f"Found {len(results)} Alcon-related manufacturers in rx_op_enhanced")
            for mfg in results['manufacturer']:
                logger.info(f"  - {mfg}")
            return results
        else:
            logger.warning("No Alcon data found in rx_op_enhanced table")
            
            # Let's check all unique manufacturers to understand what's available
            query_all = f"""
            SELECT DISTINCT
                manufacturer,
                COUNT(*) as record_count
            FROM `{RX_OP_TABLE}`
            GROUP BY manufacturer
            ORDER BY manufacturer
            LIMIT 50
            """
            all_mfgs = client.query(query_all).to_dataframe()
            logger.info(f"Available manufacturers in rx_op_enhanced (first 50):")
            for _, row in all_mfgs.iterrows():
                logger.info(f"  - {row['manufacturer']}: {row['record_count']:,} records")
            return None
            
    except Exception as e:
        logger.error(f"Error checking rx_op_enhanced: {str(e)}")
        return None

def check_alcon_in_physician_rx(client):
    """Check if Alcon products exist in the PHYSICIAN_RX table"""
    logger.info("Checking for Alcon products in PHYSICIAN_RX_2020_2024 table...")
    
    # Check for Alcon products by manufacturer and brand name
    query = f"""
    SELECT 
        MANUFACTURER,
        BRAND_NAME,
        GENERIC_NAME,
        COUNT(DISTINCT NPI) as prescriber_count,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        SUM(CHARGES) as total_charges,
        SUM(UNIQUE_PATIENTS) as total_patients
    FROM `{PHYSICIAN_RX_TABLE}`
    WHERE UPPER(MANUFACTURER) LIKE '%ALCON%'
       OR UPPER(BRAND_NAME) IN ('ROCKLATAN', 'SYSTANE', 'PATADAY', 'DUREZOL', 
                                 'VIGAMOX', 'TRAVATAN', 'AZOPT', 'NEVANAC', 'ILEVRO',
                                 'SIMBRINZA', 'COMBIGAN', 'ALPHAGAN')
    GROUP BY MANUFACTURER, BRAND_NAME, GENERIC_NAME
    ORDER BY total_payments DESC
    LIMIT 50
    """
    
    try:
        results = client.query(query).to_dataframe()
        
        if not results.empty:
            logger.info(f"Found {len(results)} Alcon products in PHYSICIAN_RX")
            total_payments = results['total_payments'].sum()
            total_prescriptions = results['total_prescriptions'].sum()
            total_patients = results['total_patients'].sum()
            logger.info(f"Total payments: ${total_payments:,.2f}")
            logger.info(f"Total prescriptions: {total_prescriptions:,.0f}")
            logger.info(f"Total patients: {total_patients:,.0f}")
            logger.info("\nTop Alcon products:")
            for _, row in results.head(10).iterrows():
                logger.info(f"  - {row['MANUFACTURER']} / {row['BRAND_NAME']}: {row['prescriber_count']:,} prescribers, ${row['total_payments']:,.2f}")
            return results
        else:
            logger.warning("No Alcon products found in PHYSICIAN_RX table")
            return None
            
    except Exception as e:
        logger.error(f"Error checking PHYSICIAN_RX: {str(e)}")
        return None

def analyze_alcon_attribution(client, manufacturer_name):
    """Analyze attribution data for a specific Alcon entity"""
    logger.info(f"Analyzing attribution for: {manufacturer_name}")
    
    query = f"""
    WITH alcon_data AS (
        SELECT 
            NPI,
            SPECIALTY_PRIMARY,
            HQ_STATE,
            year,
            month,
            -- Prescription metrics
            mfg_avg_lag3,
            mfg_avg_lag6,
            mfg_avg_lead3,
            mfg_avg_lead6,
            -- Payment metrics
            op_lag6,  -- Payment data
            TotalDollarsFrom,
            -- Attribution metrics
            pred_rx,  -- Predicted prescriptions
            pred_rx_cf,  -- Counterfactual (no payment)
            delta_rx,  -- Attribution difference
            attributable_pct,
            attributable_dollars,
            manufacturer
        FROM `{RX_OP_TABLE}`
        WHERE manufacturer = @manufacturer
    )
    SELECT 
        COUNT(DISTINCT NPI) as unique_providers,
        COUNT(*) as total_records,
        
        -- Payment metrics
        SUM(op_lag6) as total_payments,
        AVG(op_lag6) as avg_payment,
        
        -- Prescription metrics
        SUM(pred_rx) as total_prescriptions,
        SUM(delta_rx) as total_attributable,
        SUM(attributable_dollars) as total_attributable_dollars,
        
        -- Attribution metrics
        AVG(CASE WHEN op_lag6 > 0 THEN attributable_pct ELSE NULL END) as avg_attribution_when_paid,
        
        -- ROI calculation
        SAFE_DIVIDE(SUM(attributable_dollars), NULLIF(SUM(op_lag6), 0)) as roi_ratio,
        
        -- By specialty
        COUNT(DISTINCT SPECIALTY_PRIMARY) as unique_specialties,
        
        -- Temporal coverage
        MIN(year) as min_year,
        MAX(year) as max_year
        
    FROM alcon_data
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("manufacturer", "STRING", manufacturer_name)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = manufacturer_name.replace(" ", "_").replace(",", "")
    results.to_csv(OUTPUT_DIR / f"alcon_rx_op_summary_{safe_name}_{timestamp}.csv", index=False)
    
    return results

def analyze_specialty_attribution(client, manufacturer_name):
    """Analyze attribution by medical specialty"""
    logger.info(f"Analyzing specialty attribution for: {manufacturer_name}")
    
    query = f"""
    SELECT 
        SPECIALTY_PRIMARY as specialty,
        COUNT(DISTINCT NPI) as unique_providers,
        
        -- Payment metrics
        SUM(op_lag6) as total_payments,
        AVG(op_lag6) as avg_payment,
        
        -- Prescription metrics  
        SUM(pred_rx) as total_prescriptions,
        SUM(delta_rx) as attributable_prescriptions,
        
        -- Attribution metrics
        AVG(CASE WHEN op_lag6 > 0 THEN attributable_pct ELSE NULL END) as avg_attribution_rate,
        
        -- ROI
        SAFE_DIVIDE(SUM(attributable_dollars), NULLIF(SUM(op_lag6), 0)) as roi_ratio
        
    FROM `{RX_OP_TABLE}`
    WHERE manufacturer_name = @manufacturer_name
        AND SPECIALTY_PRIMARY IS NOT NULL
    GROUP BY specialty
    ORDER BY attributable_prescriptions DESC
    LIMIT 20
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("manufacturer", "STRING", manufacturer_name)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = manufacturer_name.replace(" ", "_").replace(",", "")
    results.to_csv(OUTPUT_DIR / f"alcon_rx_op_specialty_{safe_name}_{timestamp}.csv", index=False)
    
    return results

def analyze_provider_types_attribution(client, manufacturer_name):
    """Analyze attribution by provider type (MD, DO, NP, PA)"""
    logger.info(f"Analyzing provider type attribution for: {manufacturer_name}")
    
    query = f"""
    WITH provider_typed AS (
        SELECT 
            *,
            CASE 
                WHEN LOWER(SPECIALTY_PRIMARY) LIKE '%physician assistant%' THEN 'PA'
                WHEN LOWER(SPECIALTY_PRIMARY) LIKE '%nurse practitioner%' THEN 'NP'
                WHEN LOWER(SPECIALTY_PRIMARY) LIKE '%optometr%' THEN 'OD'
                WHEN LOWER(SPECIALTY_PRIMARY) LIKE '%ophthalmolog%' THEN 'MD_Ophth'
                WHEN SPECIALTY_PRIMARY IS NOT NULL THEN 'MD_Other'
                ELSE 'Unknown'
            END as provider_type
        FROM `{RX_OP_TABLE}`
        WHERE manufacturer_name = @manufacturer_name
    )
    SELECT 
        provider_type,
        COUNT(DISTINCT NPI) as unique_providers,
        
        -- Payment metrics
        SUM(op_lag6) as total_payments,
        AVG(op_lag6) as avg_payment,
        
        -- Prescription metrics  
        SUM(pred_rx) as total_prescriptions,
        SUM(delta_rx) as attributable_prescriptions,
        
        -- Attribution metrics
        AVG(CASE WHEN op_lag6 > 0 THEN attributable_pct ELSE NULL END) as avg_attribution_rate,
        
        -- ROI
        SAFE_DIVIDE(SUM(attributable_dollars), NULLIF(SUM(op_lag6), 0)) as roi_ratio
        
    FROM provider_typed
    GROUP BY provider_type
    ORDER BY attributable_prescriptions DESC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("manufacturer", "STRING", manufacturer_name)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = manufacturer_name.replace(" ", "_").replace(",", "")
    results.to_csv(OUTPUT_DIR / f"alcon_rx_op_provider_types_{safe_name}_{timestamp}.csv", index=False)
    
    return results

def analyze_high_attribution_providers(client, manufacturer_name, top_n=100):
    """Identify providers with highest attribution rates"""
    logger.info(f"Analyzing top {top_n} high-attribution providers for: {manufacturer_name}")
    
    query = f"""
    WITH provider_summary AS (
        SELECT 
            NPI,
            SPECIALTY_PRIMARY,
            HQ_STATE,
            
            -- Total metrics
            SUM(op_avg_lag6) as total_payments,
            SUM(y_actual) as total_prescriptions,
            SUM(y_attrib) as attributable_prescriptions,
            
            -- Attribution rate
            AVG(CASE WHEN op_avg_lag6 > 0 THEN pct_attrib ELSE NULL END) as avg_attribution_rate,
            
            -- ROI
            SAFE_DIVIDE(SUM(y_attrib), NULLIF(SUM(op_avg_lag6), 0)) as roi_ratio,
            
            -- Activity
            COUNT(DISTINCT CONCAT(year, '-', month)) as months_active
            
        FROM `{RX_OP_TABLE}`
        WHERE manufacturer_name = @manufacturer_name
        GROUP BY NPI, SPECIALTY_PRIMARY, HQ_STATE
        HAVING total_payments > 0 AND attributable_prescriptions > 0
    )
    SELECT *
    FROM provider_summary
    ORDER BY attributable_prescriptions DESC
    LIMIT @top_n
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("manufacturer", "STRING", manufacturer_name),
            bigquery.ScalarQueryParameter("top_n", "INT64", top_n)
        ]
    )
    
    results = client.query(query, job_config=job_config).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = manufacturer_name.replace(" ", "_").replace(",", "")
    results.to_csv(OUTPUT_DIR / f"alcon_rx_op_top_providers_{safe_name}_{timestamp}.csv", index=False)
    
    return results

def main():
    """Main execution function"""
    try:
        logger.info("=" * 80)
        logger.info("ALCON RX-OP ATTRIBUTION ANALYSIS")
        logger.info("=" * 80)
        
        # Create BigQuery client
        client = create_bigquery_client()
        
        # Check for Alcon data in rx_op_enhanced
        alcon_manufacturers = check_alcon_in_rx_op(client)
        
        # Check for Alcon products in PHYSICIAN_RX
        alcon_products = check_alcon_in_physician_rx(client)
        
        if alcon_manufacturers is not None and not alcon_manufacturers.empty:
            # Analyze each Alcon entity found
            for manufacturer in alcon_manufacturers['manufacturer']:
                logger.info(f"\nAnalyzing: {manufacturer}")
                logger.info("-" * 40)
                
                # Run analyses
                summary = analyze_alcon_attribution(client, manufacturer)
                if not summary.empty:
                    logger.info(f"  Providers: {summary['unique_providers'].iloc[0]:,}")
                    logger.info(f"  Total Attributable: ${summary['total_attributable'].iloc[0]:,.2f}")
                    if pd.notna(summary['roi_ratio'].iloc[0]):
                        logger.info(f"  ROI Ratio: {summary['roi_ratio'].iloc[0]:.2f}x")
                    if pd.notna(summary['avg_attribution_when_paid'].iloc[0]):
                        logger.info(f"  Avg Attribution: {summary['avg_attribution_when_paid'].iloc[0]:.2%}")
                
                specialty = analyze_specialty_attribution(client, manufacturer)
                logger.info(f"  Specialties analyzed: {len(specialty)}")
                
                provider_types = analyze_provider_types_attribution(client, manufacturer)
                logger.info(f"  Provider types analyzed: {len(provider_types)}")
                
                top_providers = analyze_high_attribution_providers(client, manufacturer)
                logger.info(f"  Top providers identified: {len(top_providers)}")
            
            logger.info("\n" + "=" * 80)
            logger.info("RX-OP ATTRIBUTION ANALYSIS COMPLETE")
            logger.info(f"Results saved to: {OUTPUT_DIR}")
            logger.info("=" * 80)
            
            # Save summary
            summary_data = {
                "execution_time": datetime.now().isoformat(),
                "manufacturers_found": list(alcon_manufacturers['manufacturer']),
                "products_in_physician_rx": len(alcon_products) if alcon_products is not None else 0,
                "analysis_complete": True
            }
            
            with open(OUTPUT_DIR / "alcon_rx_op_analysis_summary.json", "w") as f:
                json.dump(summary_data, f, indent=2)
                
        else:
            logger.warning("No Alcon data found in rx_op_enhanced table")
            logger.info("This may be because:")
            logger.info("1. Alcon products are not included in the rx_op_enhanced dataset")
            logger.info("2. Alcon may be listed under a different name (e.g., Novartis)")
            logger.info("3. The dataset may only include certain pharmaceutical manufacturers")
            
            # Save summary indicating no attribution data
            summary_data = {
                "execution_time": datetime.now().isoformat(),
                "manufacturers_found": [],
                "products_in_physician_rx": len(alcon_products) if alcon_products is not None else 0,
                "analysis_complete": False,
                "note": "No Alcon data found in rx_op_enhanced table, but prescription data may exist"
            }
            
            with open(OUTPUT_DIR / "alcon_rx_op_analysis_summary.json", "w") as f:
                json.dump(summary_data, f, indent=2)
        
        return True
        
    except Exception as e:
        logger.error(f"Error during rx-op analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)