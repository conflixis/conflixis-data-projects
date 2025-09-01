#!/usr/bin/env python3
"""
Analyze Prescription Patterns for Healthcare Providers
This script analyzes prescription patterns using claims data
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

def get_npi_table(config):
    """Get the NPI table name"""
    short_name = config['health_system'].get('short_name', 'health_system')
    return f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{short_name}_provider_npis"

def analyze_overall_prescribing(client, config, npi_table):
    """Analyze overall prescribing metrics"""
    
    rx_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['prescriptions']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH provider_rx AS (
        SELECT 
            rx.NPI,
            rx.CLAIM_YEAR,
            rx.BRAND_NAME,
            rx.GENERIC_NAME,
            rx.MANUFACTURER,
            rx.CHARGES,
            rx.PAYMENTS,
            rx.PRESCRIPTIONS,
            rx.UNIQUE_PATIENTS,
            n.Primary_Specialty,
            n.Full_Name
        FROM `{rx_table}` rx
        INNER JOIN `{npi_table}` n
            ON CAST(rx.NPI AS STRING) = n.NPI
        WHERE rx.CLAIM_YEAR BETWEEN {start_year} AND {end_year}
    )
    SELECT 
        COUNT(DISTINCT NPI) as unique_prescribers,
        COUNT(DISTINCT BRAND_NAME) as unique_drugs,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        SUM(CHARGES) as total_charges,
        SUM(UNIQUE_PATIENTS) as total_patients,
        AVG(PAYMENTS) as avg_payment_per_claim,
        AVG(PAYMENTS / NULLIF(PRESCRIPTIONS, 0)) as avg_cost_per_prescription
    FROM provider_rx
    """
    
    logger.info(f"Analyzing overall prescribing metrics for {config['health_system']['name']}...")
    df = client.query(query).to_dataframe()
    
    if not df.empty:
        row = df.iloc[0]
        logger.info(f"  Unique Prescribers: {row['unique_prescribers']:,.0f}")
        logger.info(f"  Unique Drugs: {row['unique_drugs']:,.0f}")
        logger.info(f"  Total Prescriptions: {row['total_prescriptions']:,.0f}")
        logger.info(f"  Total Payments: ${row['total_payments']:,.2f}")
        logger.info(f"  Total Charges: ${row['total_charges']:,.2f}")
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'rx_overall_metrics_{timestamp}.csv'
    df.to_csv(output_file, index=False)
    
    return df

def analyze_prescribing_by_year(client, config, npi_table):
    """Analyze prescribing trends by year"""
    
    rx_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['prescriptions']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH provider_rx AS (
        SELECT 
            rx.NPI,
            rx.CLAIM_YEAR,
            rx.PAYMENTS,
            rx.PRESCRIPTIONS,
            rx.UNIQUE_PATIENTS
        FROM `{rx_table}` rx
        INNER JOIN `{npi_table}` n
            ON CAST(rx.NPI AS STRING) = n.NPI
        WHERE rx.CLAIM_YEAR BETWEEN {start_year} AND {end_year}
    )
    SELECT 
        CLAIM_YEAR,
        COUNT(DISTINCT NPI) as unique_prescribers,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        SUM(UNIQUE_PATIENTS) as total_patients,
        AVG(PAYMENTS) as avg_payment,
        SUM(PAYMENTS) / NULLIF(SUM(PRESCRIPTIONS), 0) as avg_cost_per_prescription
    FROM provider_rx
    GROUP BY CLAIM_YEAR
    ORDER BY CLAIM_YEAR
    """
    
    logger.info("Analyzing prescribing trends by year...")
    df = client.query(query).to_dataframe()
    
    # Calculate year-over-year growth
    df['yoy_growth_payments'] = df['total_payments'].pct_change() * 100
    df['yoy_growth_prescriptions'] = df['total_prescriptions'].pct_change() * 100
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'rx_yearly_trends_{timestamp}.csv'
    df.to_csv(output_file, index=False)
    
    logger.info("\nYearly Prescribing Trends:")
    for _, row in df.iterrows():
        growth = f" ({row['yoy_growth_payments']:.1f}% YoY)" if pd.notna(row['yoy_growth_payments']) else ""
        logger.info(f"  {row['CLAIM_YEAR']}: ${row['total_payments']:,.0f}{growth}")
    
    return df

def analyze_top_drugs(client, config, npi_table):
    """Identify top prescribed drugs"""
    
    rx_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['prescriptions']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH provider_rx AS (
        SELECT 
            rx.BRAND_NAME,
            rx.GENERIC_NAME,
            rx.MANUFACTURER,
            rx.NPI,
            rx.PAYMENTS,
            rx.PRESCRIPTIONS,
            rx.UNIQUE_PATIENTS
        FROM `{rx_table}` rx
        INNER JOIN `{npi_table}` n
            ON CAST(rx.NPI AS STRING) = n.NPI
        WHERE rx.CLAIM_YEAR BETWEEN {start_year} AND {end_year}
    )
    SELECT 
        BRAND_NAME,
        GENERIC_NAME,
        MANUFACTURER,
        COUNT(DISTINCT NPI) as unique_prescribers,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        SUM(UNIQUE_PATIENTS) as total_patients,
        AVG(PAYMENTS) as avg_payment,
        SUM(PAYMENTS) / NULLIF(SUM(PRESCRIPTIONS), 0) as avg_cost_per_prescription
    FROM provider_rx
    GROUP BY BRAND_NAME, GENERIC_NAME, MANUFACTURER
    ORDER BY total_payments DESC
    LIMIT 50
    """
    
    logger.info("Analyzing top prescribed drugs...")
    df = client.query(query).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'rx_top_drugs_{timestamp}.csv'
    df.to_csv(output_file, index=False)
    
    logger.info("\nTop 10 Drugs by Total Payments:")
    for idx, row in df.head(10).iterrows():
        logger.info(f"  {row['BRAND_NAME']}: ${row['total_payments']:,.0f} ({row['unique_prescribers']} prescribers)")
    
    return df

def analyze_high_cost_drugs(client, config, npi_table):
    """Identify high-cost drugs with significant spending"""
    
    rx_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['prescriptions']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    # Get focus drugs from config
    focus_drugs = config.get('focus_drugs', [])
    drug_names = [drug['name'] for drug in focus_drugs]
    drug_list = "','".join(drug_names)
    
    query = f"""
    WITH provider_rx AS (
        SELECT 
            rx.BRAND_NAME,
            rx.NPI,
            rx.PAYMENTS,
            rx.PRESCRIPTIONS,
            rx.UNIQUE_PATIENTS,
            rx.PAYMENTS / NULLIF(rx.PRESCRIPTIONS, 0) as cost_per_prescription
        FROM `{rx_table}` rx
        INNER JOIN `{npi_table}` n
            ON CAST(rx.NPI AS STRING) = n.NPI
        WHERE rx.CLAIM_YEAR BETWEEN {start_year} AND {end_year}
            AND UPPER(rx.BRAND_NAME) IN ('{drug_list.upper()}')
    )
    SELECT 
        BRAND_NAME,
        COUNT(DISTINCT NPI) as unique_prescribers,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        AVG(cost_per_prescription) as avg_cost_per_prescription,
        MAX(cost_per_prescription) as max_cost_per_prescription,
        STDDEV(cost_per_prescription) as stddev_cost
    FROM provider_rx
    GROUP BY BRAND_NAME
    ORDER BY total_payments DESC
    """
    
    logger.info("Analyzing high-cost drugs...")
    df = client.query(query).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'rx_high_cost_drugs_{timestamp}.csv'
    df.to_csv(output_file, index=False)
    
    return df

def analyze_by_specialty(client, config, npi_table):
    """Analyze prescribing patterns by specialty"""
    
    rx_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['prescriptions']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH provider_rx AS (
        SELECT 
            n.Primary_Specialty,
            rx.NPI,
            rx.PAYMENTS,
            rx.PRESCRIPTIONS,
            rx.UNIQUE_PATIENTS
        FROM `{rx_table}` rx
        INNER JOIN `{npi_table}` n
            ON CAST(rx.NPI AS STRING) = n.NPI
        WHERE rx.CLAIM_YEAR BETWEEN {start_year} AND {end_year}
    )
    SELECT 
        Primary_Specialty,
        COUNT(DISTINCT NPI) as unique_prescribers,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        AVG(PAYMENTS) as avg_payment,
        SUM(PAYMENTS) / NULLIF(SUM(PRESCRIPTIONS), 0) as avg_cost_per_prescription,
        AVG(PRESCRIPTIONS) as avg_prescriptions_per_provider
    FROM provider_rx
    WHERE Primary_Specialty IS NOT NULL
    GROUP BY Primary_Specialty
    HAVING COUNT(DISTINCT NPI) >= 5  -- Only specialties with 5+ providers
    ORDER BY total_payments DESC
    """
    
    logger.info("Analyzing prescribing by specialty...")
    df = client.query(query).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'rx_by_specialty_{timestamp}.csv'
    df.to_csv(output_file, index=False)
    
    logger.info("\nTop 5 Specialties by Total Prescription Payments:")
    for idx, row in df.head(5).iterrows():
        logger.info(f"  {row['Primary_Specialty']}: ${row['total_payments']:,.0f} ({row['unique_prescribers']} providers)")
    
    return df

def analyze_np_pa_prescribing(client, config, npi_table):
    """Analyze prescribing patterns for NPs and PAs vs physicians"""
    
    rx_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['prescriptions']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH provider_rx AS (
        SELECT 
            CASE 
                WHEN n.Primary_Specialty LIKE '%Physician Assistant%' 
                    OR n.Primary_Specialty LIKE '%PA%' THEN 'Physician Assistant'
                WHEN n.Primary_Specialty LIKE '%Nurse Practitioner%' 
                    OR n.Primary_Specialty LIKE '%NP%' THEN 'Nurse Practitioner'
                WHEN n.Primary_Specialty LIKE '%MD%' 
                    OR n.Primary_Specialty LIKE '%DO%' 
                    OR n.Primary_Specialty LIKE '%Physician%' THEN 'Physician'
                ELSE 'Other'
            END AS provider_type,
            rx.NPI,
            rx.PAYMENTS,
            rx.PRESCRIPTIONS,
            rx.UNIQUE_PATIENTS
        FROM `{rx_table}` rx
        INNER JOIN `{npi_table}` n
            ON CAST(rx.NPI AS STRING) = n.NPI
        WHERE rx.CLAIM_YEAR BETWEEN {start_year} AND {end_year}
    )
    SELECT 
        provider_type,
        COUNT(DISTINCT NPI) as unique_prescribers,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments,
        AVG(PAYMENTS) as avg_payment,
        AVG(PRESCRIPTIONS) as avg_prescriptions_per_provider,
        SUM(PAYMENTS) / NULLIF(SUM(PRESCRIPTIONS), 0) as avg_cost_per_prescription
    FROM provider_rx
    WHERE provider_type != 'Other'
    GROUP BY provider_type
    ORDER BY total_payments DESC
    """
    
    logger.info("Analyzing NP/PA prescribing patterns...")
    df = client.query(query).to_dataframe()
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'rx_np_pa_analysis_{timestamp}.csv'
    df.to_csv(output_file, index=False)
    
    logger.info("\nProvider Type Prescribing Patterns:")
    for idx, row in df.iterrows():
        logger.info(f"  {row['provider_type']}: ${row['total_payments']:,.0f} avg ${row['avg_payment']:,.0f}/provider")
    
    return df

def generate_summary_stats(config, total_npis):
    """Generate summary statistics from prescription analyses"""
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Read the saved files to compile summary
    processed_dir = TEMPLATE_DIR / 'data' / 'processed'
    
    # Find most recent files
    overall_file = max(processed_dir.glob('rx_overall_metrics_*.csv'))
    df_overall = pd.read_csv(overall_file)
    
    # Calculate key metrics
    unique_prescribers = df_overall.iloc[0]['unique_prescribers'] if not df_overall.empty else 0
    total_prescriptions = df_overall.iloc[0]['total_prescriptions'] if not df_overall.empty else 0
    total_payments = df_overall.iloc[0]['total_payments'] if not df_overall.empty else 0
    pct_prescribers = (unique_prescribers / total_npis * 100) if total_npis > 0 else 0
    
    # Create summary
    summary = {
        'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'health_system': config['health_system']['name'],
        'analysis_period': f"{config['analysis']['start_year']}-{config['analysis']['end_year']}",
        'total_providers': total_npis,
        'unique_prescribers': unique_prescribers,
        'percent_prescribers': round(pct_prescribers, 1),
        'total_prescriptions': total_prescriptions,
        'total_prescription_payments': total_payments,
        'avg_payment_per_prescriber': total_payments / unique_prescribers if unique_prescribers > 0 else 0
    }
    
    # Save summary
    summary_file = TEMPLATE_DIR / 'data' / 'processed' / f'rx_analysis_summary_{timestamp}.json'
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    
    logger.info(f"\n📊 Summary saved to: {summary_file}")
    
    return summary

def main():
    """Main execution function"""
    
    logger.info("=" * 60)
    logger.info("PRESCRIPTION PATTERN ANALYSIS")
    logger.info("=" * 60)
    
    # Load configuration
    config = load_config()
    logger.info(f"Health System: {config['health_system']['name']}")
    logger.info(f"Analysis Period: {config['analysis']['start_year']}-{config['analysis']['end_year']}")
    
    # Create BigQuery client
    client = create_bigquery_client(config)
    
    # Get NPI table
    npi_table = get_npi_table(config)
    
    # Get total NPI count
    query = f"SELECT COUNT(DISTINCT NPI) as count FROM `{npi_table}`"
    total_npis = client.query(query).to_dataframe().iloc[0]['count']
    logger.info(f"Total Providers: {total_npis}")
    
    # Run analyses
    logger.info("\n" + "=" * 60)
    logger.info("Running Analyses...")
    logger.info("=" * 60)
    
    if config.get('analysis', {}).get('components', {}).get('prescriptions', True):
        analyze_overall_prescribing(client, config, npi_table)
        analyze_prescribing_by_year(client, config, npi_table)
        analyze_top_drugs(client, config, npi_table)
        analyze_high_cost_drugs(client, config, npi_table)
        analyze_by_specialty(client, config, npi_table)
        analyze_np_pa_prescribing(client, config, npi_table)
    else:
        logger.info("Prescription analysis disabled in configuration")
        return
    
    # Generate summary
    summary = generate_summary_stats(config, total_npis)
    
    logger.info("\n" + "=" * 60)
    logger.info("ANALYSIS COMPLETE")
    logger.info("=" * 60)
    logger.info(f"✅ {summary['percent_prescribers']}% of providers are active prescribers")
    logger.info(f"✅ Total prescriptions: {summary['total_prescriptions']:,.0f}")
    logger.info(f"✅ Total prescription payments: ${summary['total_prescription_payments']:,.0f}")
    logger.info(f"✅ Results saved to: data/processed/")

if __name__ == "__main__":
    main()