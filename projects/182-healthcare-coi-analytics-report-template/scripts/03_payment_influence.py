#!/usr/bin/env python3
"""
Analyze Payment-Prescription Correlations
This script analyzes the correlation between industry payments and prescribing patterns
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

def analyze_drug_payment_correlation(client, config, drug_name, npi_table):
    """
    Analyze correlation between payments and prescribing for a specific drug.
    Implements the 180-day window methodology.
    """
    
    op_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['open_payments']}"
    rx_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['prescriptions']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH provider_list AS (
        SELECT DISTINCT NPI, Full_Name, Primary_Specialty
        FROM `{npi_table}`
    ),
    -- Get Open Payments for this drug with 180-day window
    drug_payments AS (
        SELECT 
            CAST(op.covered_recipient_npi AS STRING) AS NPI,
            op.program_year,
            op.date_of_payment,
            op.total_amount_of_payment_usdollars as payment_amount,
            op.applicable_manufacturer_or_applicable_gpo_making_payment_name AS manufacturer,
            -- Create 180-day influence window
            DATE_ADD(op.date_of_payment, INTERVAL 180 DAY) as influence_end_date
        FROM `{op_table}` op
        WHERE op.program_year BETWEEN {start_year} AND {end_year}
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
            -- Convert month name to date for correlation
            DATE(CAST(rx.CLAIM_YEAR AS INT64), 
                 CASE rx.CLAIM_MONTH
                    WHEN 'January' THEN 1 WHEN 'February' THEN 2 WHEN 'March' THEN 3
                    WHEN 'April' THEN 4 WHEN 'May' THEN 5 WHEN 'June' THEN 6
                    WHEN 'July' THEN 7 WHEN 'August' THEN 8 WHEN 'September' THEN 9
                    WHEN 'October' THEN 10 WHEN 'November' THEN 11 WHEN 'December' THEN 12
                 END, 15) as rx_date  -- Use middle of month
        FROM `{rx_table}` rx
        WHERE rx.CLAIM_YEAR BETWEEN {start_year} AND {end_year}
            AND UPPER(rx.BRAND_NAME) LIKE UPPER('%{drug_name}%')
    ),
    -- Calculate influenced prescriptions (within 180 days of payment)
    influenced_prescriptions AS (
        SELECT 
            drx.NPI,
            drx.rx_payments,
            drx.PRESCRIPTIONS,
            CASE 
                WHEN EXISTS (
                    SELECT 1 FROM drug_payments dp 
                    WHERE dp.NPI = drx.NPI 
                    AND drx.rx_date BETWEEN dp.date_of_payment 
                                        AND dp.influence_end_date
                ) THEN 1 
                ELSE 0 
            END as within_influence_window
        FROM drug_prescriptions drx
    ),
    -- Aggregate providers with and without payments
    provider_summary AS (
        SELECT 
            p.NPI,
            p.Full_Name,
            p.Primary_Specialty,
            COALESCE(SUM(dp.payment_amount), 0) as total_op_payments,
            COUNT(DISTINCT dp.date_of_payment) as payment_dates,
            COALESCE(SUM(drx.rx_payments), 0) as total_rx_payments,
            COALESCE(SUM(drx.PRESCRIPTIONS), 0) as total_prescriptions,
            COALESCE(SUM(drx.UNIQUE_PATIENTS), 0) as total_patients,
            COALESCE(SUM(CASE WHEN ip.within_influence_window = 1 
                        THEN ip.PRESCRIPTIONS ELSE 0 END), 0) as influenced_prescriptions,
            CASE WHEN COUNT(dp.NPI) > 0 THEN 1 ELSE 0 END as received_payment
        FROM provider_list p
        LEFT JOIN drug_payments dp ON p.NPI = dp.NPI
        LEFT JOIN drug_prescriptions drx ON p.NPI = drx.NPI
        LEFT JOIN influenced_prescriptions ip ON p.NPI = ip.NPI
        GROUP BY p.NPI, p.Full_Name, p.Primary_Specialty
    )
    SELECT 
        received_payment,
        COUNT(DISTINCT NPI) as provider_count,
        AVG(total_rx_payments) as avg_rx_payments,
        AVG(total_prescriptions) as avg_prescriptions,
        AVG(total_patients) as avg_patients,
        SUM(total_rx_payments) as sum_rx_payments,
        SUM(total_prescriptions) as sum_prescriptions,
        AVG(total_op_payments) as avg_op_payment,
        AVG(influenced_prescriptions) as avg_influenced_rx,
        STDDEV(total_rx_payments) as stddev_rx_payments
    FROM provider_summary
    WHERE total_prescriptions > 0  -- Only providers who prescribed the drug
    GROUP BY received_payment
    ORDER BY received_payment
    """
    
    logger.info(f"\nAnalyzing payment correlation for {drug_name}...")
    df = client.query(query).to_dataframe()
    
    correlation_result = {
        'drug_name': drug_name,
        'providers_with_payments': 0,
        'providers_without_payments': 0,
        'avg_rx_with_payments': 0,
        'avg_rx_without_payments': 0,
        'influence_ratio': 0,
        'roi_per_dollar': 0,
        'avg_payment_amount': 0
    }
    
    if len(df) >= 2:
        no_payment = df[df['received_payment'] == 0].iloc[0] if len(df[df['received_payment'] == 0]) > 0 else None
        with_payment = df[df['received_payment'] == 1].iloc[0] if len(df[df['received_payment'] == 1]) > 0 else None
        
        if no_payment is not None and with_payment is not None:
            ratio = with_payment['avg_rx_payments'] / no_payment['avg_rx_payments'] if no_payment['avg_rx_payments'] > 0 else 0
            
            correlation_result.update({
                'providers_with_payments': with_payment['provider_count'],
                'providers_without_payments': no_payment['provider_count'],
                'avg_rx_with_payments': with_payment['avg_rx_payments'],
                'avg_rx_without_payments': no_payment['avg_rx_payments'],
                'influence_ratio': ratio,
                'avg_payment_amount': with_payment['avg_op_payment']
            })
            
            logger.info(f"  Providers WITH payments: ${with_payment['avg_rx_payments']:,.0f} avg")
            logger.info(f"  Providers WITHOUT payments: ${no_payment['avg_rx_payments']:,.0f} avg")
            logger.info(f"  Ratio: {ratio:.2f}x more prescribed by paid providers")
            
            # Calculate ROI
            if with_payment['avg_op_payment'] > 0:
                roi = (with_payment['avg_rx_payments'] - no_payment['avg_rx_payments']) / with_payment['avg_op_payment']
                correlation_result['roi_per_dollar'] = roi
                logger.info(f"  ROI: ${roi:.0f} per dollar of payments")
            
            # Check influenced prescriptions
            if with_payment['avg_influenced_rx'] > 0:
                influence_pct = (with_payment['avg_influenced_rx'] / with_payment['avg_prescriptions']) * 100
                logger.info(f"  Influenced prescriptions: {influence_pct:.1f}% within 180-day window")
    
    return pd.DataFrame([correlation_result])

def analyze_provider_type_vulnerability(client, config, npi_table):
    """Analyze differential vulnerability by provider type (MD/DO vs NP/PA)"""
    
    op_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['open_payments']}"
    rx_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['prescriptions']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH provider_categories AS (
        SELECT 
            NPI,
            CASE 
                WHEN Primary_Specialty LIKE '%Physician Assistant%' 
                    OR Primary_Specialty LIKE '%PA%' THEN 'Physician Assistant'
                WHEN Primary_Specialty LIKE '%Nurse Practitioner%' 
                    OR Primary_Specialty LIKE '%NP%' THEN 'Nurse Practitioner'
                WHEN Primary_Specialty LIKE '%MD%' 
                    OR Primary_Specialty LIKE '%DO%' 
                    OR Primary_Specialty LIKE '%Physician%' THEN 'Physician'
                ELSE 'Other'
            END AS provider_type,
            Primary_Specialty
        FROM `{npi_table}`
    ),
    provider_payments AS (
        SELECT 
            CAST(op.covered_recipient_npi AS STRING) AS NPI,
            SUM(op.total_amount_of_payment_usdollars) as total_payments,
            COUNT(*) as payment_count
        FROM `{op_table}` op
        WHERE op.program_year BETWEEN {start_year} AND {end_year}
        GROUP BY NPI
    ),
    provider_prescriptions AS (
        SELECT 
            CAST(rx.NPI AS STRING) AS NPI,
            SUM(rx.PAYMENTS) as total_rx_payments,
            SUM(rx.PRESCRIPTIONS) as total_prescriptions
        FROM `{rx_table}` rx
        WHERE rx.CLAIM_YEAR BETWEEN {start_year} AND {end_year}
        GROUP BY NPI
    ),
    combined_data AS (
        SELECT 
            pc.provider_type,
            pc.NPI,
            COALESCE(pp.total_payments, 0) as op_payments,
            COALESCE(pp.payment_count, 0) as payment_count,
            COALESCE(prx.total_rx_payments, 0) as rx_payments,
            COALESCE(prx.total_prescriptions, 0) as prescriptions,
            CASE WHEN pp.total_payments > 0 THEN 1 ELSE 0 END as received_payment
        FROM provider_categories pc
        LEFT JOIN provider_payments pp ON pc.NPI = pp.NPI
        LEFT JOIN provider_prescriptions prx ON pc.NPI = prx.NPI
        WHERE pc.provider_type != 'Other'
    )
    SELECT 
        provider_type,
        received_payment,
        COUNT(DISTINCT NPI) as provider_count,
        AVG(rx_payments) as avg_rx_payments,
        AVG(prescriptions) as avg_prescriptions,
        AVG(op_payments) as avg_op_payment,
        STDDEV(rx_payments) as stddev_rx_payments,
        SUM(rx_payments) as total_rx_payments
    FROM combined_data
    WHERE prescriptions > 0  -- Active prescribers only
    GROUP BY provider_type, received_payment
    ORDER BY provider_type, received_payment
    """
    
    logger.info("\nAnalyzing provider type vulnerability...")
    df = client.query(query).to_dataframe()
    
    # Calculate vulnerability metrics for each provider type
    vulnerability_results = []
    
    for provider_type in df['provider_type'].unique():
        type_data = df[df['provider_type'] == provider_type]
        
        no_payment = type_data[type_data['received_payment'] == 0]
        with_payment = type_data[type_data['received_payment'] == 1]
        
        if not no_payment.empty and not with_payment.empty:
            no_pay_avg = no_payment.iloc[0]['avg_rx_payments']
            with_pay_avg = with_payment.iloc[0]['avg_rx_payments']
            
            influence_increase = ((with_pay_avg - no_pay_avg) / no_pay_avg * 100) if no_pay_avg > 0 else 0
            
            # Calculate ROI
            avg_payment = with_payment.iloc[0]['avg_op_payment']
            roi = (with_pay_avg - no_pay_avg) / avg_payment if avg_payment > 0 else 0
            
            vulnerability_results.append({
                'provider_type': provider_type,
                'providers_with_payments': with_payment.iloc[0]['provider_count'],
                'providers_without_payments': no_payment.iloc[0]['provider_count'],
                'avg_rx_with_payments': with_pay_avg,
                'avg_rx_without_payments': no_pay_avg,
                'influence_increase_pct': influence_increase,
                'roi_per_dollar': roi
            })
            
            logger.info(f"  {provider_type}:")
            logger.info(f"    With payments: ${with_pay_avg:,.0f}")
            logger.info(f"    Without payments: ${no_pay_avg:,.0f}")
            logger.info(f"    Influence increase: {influence_increase:.1f}%")
            logger.info(f"    ROI: ${roi:.0f} per dollar")
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_df = pd.DataFrame(vulnerability_results)
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'correlation_np_pa_vulnerability_{timestamp}.csv'
    results_df.to_csv(output_file, index=False)
    
    return results_df

def analyze_payment_tier_correlation(client, config, npi_table):
    """Analyze correlation by payment tier amount"""
    
    op_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['open_payments']}"
    rx_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['prescriptions']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH provider_payments AS (
        SELECT 
            CAST(op.covered_recipient_npi AS STRING) AS NPI,
            SUM(op.total_amount_of_payment_usdollars) as total_payments
        FROM `{op_table}` op
        WHERE op.program_year BETWEEN {start_year} AND {end_year}
        GROUP BY NPI
    ),
    provider_prescriptions AS (
        SELECT 
            CAST(rx.NPI AS STRING) AS NPI,
            SUM(rx.PAYMENTS) as total_rx_payments,
            SUM(rx.PRESCRIPTIONS) as total_prescriptions
        FROM `{rx_table}` rx
        WHERE rx.CLAIM_YEAR BETWEEN {start_year} AND {end_year}
        GROUP BY NPI
    ),
    tiered_providers AS (
        SELECT 
            n.NPI,
            COALESCE(pp.total_payments, 0) as op_payments,
            COALESCE(prx.total_rx_payments, 0) as rx_payments,
            COALESCE(prx.total_prescriptions, 0) as prescriptions,
            CASE 
                WHEN pp.total_payments IS NULL OR pp.total_payments = 0 THEN 'No Payment'
                WHEN pp.total_payments <= 100 THEN '$1-100'
                WHEN pp.total_payments <= 500 THEN '$101-500'
                WHEN pp.total_payments <= 1000 THEN '$501-1,000'
                WHEN pp.total_payments <= 5000 THEN '$1,001-5,000'
                WHEN pp.total_payments <= 10000 THEN '$5,001-10,000'
                ELSE '$10,000+'
            END AS payment_tier
        FROM `{npi_table}` n
        LEFT JOIN provider_payments pp ON n.NPI = pp.NPI
        LEFT JOIN provider_prescriptions prx ON n.NPI = prx.NPI
    )
    SELECT 
        payment_tier,
        COUNT(DISTINCT NPI) as provider_count,
        AVG(rx_payments) as avg_rx_payments,
        AVG(prescriptions) as avg_prescriptions,
        AVG(op_payments) as avg_op_payment,
        SUM(rx_payments) as total_rx_payments,
        SUM(op_payments) as total_op_payments,
        STDDEV(rx_payments) as stddev_rx_payments
    FROM tiered_providers
    WHERE prescriptions > 0  -- Active prescribers only
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
    
    logger.info("\nAnalyzing payment tier correlations...")
    df = client.query(query).to_dataframe()
    
    # Calculate ROI for each tier
    baseline_rx = df[df['payment_tier'] == 'No Payment']['avg_rx_payments'].iloc[0] if not df[df['payment_tier'] == 'No Payment'].empty else 0
    
    tier_results = []
    for idx, row in df.iterrows():
        if row['payment_tier'] != 'No Payment' and row['avg_op_payment'] > 0:
            roi = (row['avg_rx_payments'] - baseline_rx) / row['avg_op_payment']
        else:
            roi = 0
        
        tier_results.append({
            'payment_tier': row['payment_tier'],
            'provider_count': row['provider_count'],
            'avg_rx_payments': row['avg_rx_payments'],
            'avg_prescriptions': row['avg_prescriptions'],
            'avg_op_payment': row['avg_op_payment'],
            'roi_per_dollar': roi
        })
        
        logger.info(f"  {row['payment_tier']}: {row['provider_count']} providers, "
                   f"${row['avg_rx_payments']:,.0f} avg Rx, ROI: ${roi:.0f}")
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_df = pd.DataFrame(tier_results)
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'correlation_payment_tiers_{timestamp}.csv'
    results_df.to_csv(output_file, index=False)
    
    return results_df

def analyze_consecutive_year_patterns(client, config, npi_table):
    """Analyze correlation for providers receiving payments in consecutive years"""
    
    op_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['open_payments']}"
    rx_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['tables']['prescriptions']}"
    start_year = config['analysis']['start_year']
    end_year = config['analysis']['end_year']
    
    query = f"""
    WITH yearly_payments AS (
        SELECT 
            CAST(op.covered_recipient_npi AS STRING) AS NPI,
            op.program_year,
            SUM(op.total_amount_of_payment_usdollars) as yearly_payment
        FROM `{op_table}` op
        WHERE op.program_year BETWEEN {start_year} AND {end_year}
        GROUP BY NPI, program_year
    ),
    consecutive_counts AS (
        SELECT 
            NPI,
            COUNT(DISTINCT program_year) as years_with_payments,
            SUM(yearly_payment) as total_payments_all_years,
            AVG(yearly_payment) as avg_yearly_payment
        FROM yearly_payments
        GROUP BY NPI
    ),
    provider_prescriptions AS (
        SELECT 
            CAST(rx.NPI AS STRING) AS NPI,
            SUM(rx.PAYMENTS) as total_rx_payments,
            SUM(rx.PRESCRIPTIONS) as total_prescriptions
        FROM `{rx_table}` rx
        WHERE rx.CLAIM_YEAR BETWEEN {start_year} AND {end_year}
        GROUP BY NPI
    ),
    combined AS (
        SELECT 
            COALESCE(cc.years_with_payments, 0) as years_with_payments,
            n.NPI,
            COALESCE(cc.total_payments_all_years, 0) as total_op_payments,
            COALESCE(prx.total_rx_payments, 0) as rx_payments,
            COALESCE(prx.total_prescriptions, 0) as prescriptions
        FROM `{npi_table}` n
        LEFT JOIN consecutive_counts cc ON n.NPI = cc.NPI
        LEFT JOIN provider_prescriptions prx ON n.NPI = prx.NPI
    )
    SELECT 
        years_with_payments,
        COUNT(DISTINCT NPI) as provider_count,
        AVG(rx_payments) as avg_rx_payments,
        AVG(prescriptions) as avg_prescriptions,
        AVG(total_op_payments) as avg_op_payments,
        SUM(rx_payments) as total_rx_payments,
        STDDEV(rx_payments) as stddev_rx_payments
    FROM combined
    WHERE prescriptions > 0  -- Active prescribers only
    GROUP BY years_with_payments
    ORDER BY years_with_payments DESC
    """
    
    logger.info("\nAnalyzing consecutive year payment patterns...")
    df = client.query(query).to_dataframe()
    
    # Calculate influence multiplier
    baseline_rx = df[df['years_with_payments'] == 0]['avg_rx_payments'].iloc[0] if not df[df['years_with_payments'] == 0].empty else 0
    
    consecutive_results = []
    for idx, row in df.iterrows():
        if baseline_rx > 0:
            influence_multiple = row['avg_rx_payments'] / baseline_rx
        else:
            influence_multiple = 1
        
        consecutive_results.append({
            'years_with_payments': row['years_with_payments'],
            'provider_count': row['provider_count'],
            'avg_rx_payments': row['avg_rx_payments'],
            'avg_prescriptions': row['avg_prescriptions'],
            'avg_op_payments': row['avg_op_payments'],
            'influence_multiple': influence_multiple
        })
        
        logger.info(f"  {row['years_with_payments']} years: {row['provider_count']} providers, "
                   f"${row['avg_rx_payments']:,.0f} avg Rx, {influence_multiple:.2f}x baseline")
    
    # Highlight sustained relationships
    total_years = end_year - start_year + 1
    all_years_providers = df[df['years_with_payments'] == total_years]
    if not all_years_providers.empty:
        count = all_years_providers.iloc[0]['provider_count']
        avg_rx = all_years_providers.iloc[0]['avg_rx_payments']
        logger.info(f"\n⚠️  {count} providers received payments EVERY year, averaging ${avg_rx:,.0f} in prescriptions")
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_df = pd.DataFrame(consecutive_results)
    output_file = TEMPLATE_DIR / 'data' / 'processed' / f'correlation_consecutive_years_{timestamp}.csv'
    results_df.to_csv(output_file, index=False)
    
    return results_df

def main():
    """Main execution function"""
    
    logger.info("=" * 60)
    logger.info("PAYMENT-PRESCRIPTION CORRELATION ANALYSIS")
    logger.info("=" * 60)
    
    # Load configuration
    config = load_config()
    logger.info(f"Health System: {config['health_system']['name']}")
    logger.info(f"Analysis Period: {config['analysis']['start_year']}-{config['analysis']['end_year']}")
    
    # Create BigQuery client
    client = create_bigquery_client(config)
    
    # Get NPI table
    npi_table = get_npi_table(config)
    
    # Run analyses
    logger.info("\n" + "=" * 60)
    logger.info("Running Correlation Analyses...")
    logger.info("=" * 60)
    
    if config.get('analysis', {}).get('components', {}).get('correlations', True):
        # Analyze key drugs from config
        focus_drugs = config.get('focus_drugs', [])
        drug_results = []
        
        for drug_info in focus_drugs[:3]:  # Limit to top 3 drugs for testing
            drug_name = drug_info['name']
            result = analyze_drug_payment_correlation(client, config, drug_name, npi_table)
            drug_results.append(result)
        
        # Combine and save drug correlation results
        if drug_results:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            all_drug_results = pd.concat(drug_results, ignore_index=True)
            
            # Save individual drug results
            for drug_name in all_drug_results['drug_name'].unique():
                drug_data = all_drug_results[all_drug_results['drug_name'] == drug_name]
                safe_drug_name = drug_name.lower().replace(' ', '_').replace('/', '_')
                output_file = TEMPLATE_DIR / 'data' / 'processed' / f'correlation_{safe_drug_name}_{timestamp}.csv'
                drug_data.to_csv(output_file, index=False)
        
        # Run other correlation analyses
        if config.get('analysis', {}).get('components', {}).get('provider_vulnerability', True):
            analyze_provider_type_vulnerability(client, config, npi_table)
        
        if config.get('analysis', {}).get('components', {}).get('payment_tiers', True):
            analyze_payment_tier_correlation(client, config, npi_table)
        
        if config.get('analysis', {}).get('components', {}).get('consecutive_years', True):
            analyze_consecutive_year_patterns(client, config, npi_table)
    else:
        logger.info("Correlation analysis disabled in configuration")
    
    logger.info("\n" + "=" * 60)
    logger.info("CORRELATION ANALYSIS COMPLETE")
    logger.info("=" * 60)
    logger.info("✅ Results saved to: data/processed/")

if __name__ == "__main__":
    main()