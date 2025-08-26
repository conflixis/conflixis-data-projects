#!/usr/bin/env python3
"""
Compare Ozempic with other GLP-1 agonists at Corewell Health
Analyze market share, substitution patterns, and manufacturer influence
"""

import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import numpy as np
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_bigquery_client():
    """Create BigQuery client with credentials"""
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
    
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    return bigquery.Client(credentials=credentials, project='data-analytics-389803')

# Initialize BigQuery client
client = create_bigquery_client()

# Define tables
OP_TABLE = "data-analytics-389803.conflixis_agent.op_general_all_aggregate_static"
NPI_TABLE = "data-analytics-389803.conflixis_agent.corewell_health_npis"
RX_TABLE = "data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024"

# Output directory
output_dir = '/home/incent/conflixis-data-projects/projects/175-corewell-health-analysis/ozempic-deep-dive/data'
os.makedirs(output_dir, exist_ok=True)

def analyze_glp1_market_share():
    """Analyze market share of all GLP-1 agonists at Corewell"""
    
    print("Analyzing GLP-1 agonist market share at Corewell Health...")
    
    query = f"""
    WITH glp1_drugs AS (
        SELECT 
            rx.NPI AS npi,
            rx.CLAIM_YEAR AS year,
            rx.BRAND_NAME,
            rx.GENERIC_NAME,
            rx.MANUFACTURER,
            CASE 
                WHEN UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%' THEN 'Ozempic'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%WEGOVY%' THEN 'Wegovy'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%TRULICITY%' THEN 'Trulicity'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%MOUNJARO%' THEN 'Mounjaro'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%VICTOZA%' THEN 'Victoza'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%RYBELSUS%' THEN 'Rybelsus'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%SAXENDA%' THEN 'Saxenda'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%BYDUREON%' THEN 'Bydureon'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%BYETTA%' THEN 'Byetta'
                ELSE 'Other GLP-1'
            END AS drug_name,
            CASE 
                WHEN UPPER(rx.MANUFACTURER) LIKE '%NOVO%' THEN 'Novo Nordisk'
                WHEN UPPER(rx.MANUFACTURER) LIKE '%LILLY%' THEN 'Eli Lilly'
                WHEN UPPER(rx.MANUFACTURER) LIKE '%ASTRA%' THEN 'AstraZeneca'
                ELSE 'Other'
            END AS manufacturer_group,
            SUM(rx.UNIQUE_PATIENTS) AS unique_patients,
            SUM(rx.PRESCRIPTIONS) AS prescriptions,
            SUM(rx.PAYMENTS) AS total_payment,
            AVG(rx.DAYS_SUPPLY) AS avg_days_supply
        FROM `{RX_TABLE}` rx
        WHERE rx.NPI IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
            AND (
                UPPER(rx.BRAND_NAME) IN ('OZEMPIC', 'WEGOVY', 'TRULICITY', 'MOUNJARO', 'VICTOZA', 
                                         'RYBELSUS', 'SAXENDA', 'BYDUREON', 'BYETTA')
                OR UPPER(rx.GENERIC_NAME) LIKE '%SEMAGLUTIDE%'
                OR UPPER(rx.GENERIC_NAME) LIKE '%DULAGLUTIDE%'
                OR UPPER(rx.GENERIC_NAME) LIKE '%TIRZEPATIDE%'
                OR UPPER(rx.GENERIC_NAME) LIKE '%LIRAGLUTIDE%'
                OR UPPER(rx.GENERIC_NAME) LIKE '%EXENATIDE%'
            )
        GROUP BY 1,2,3,4,5,6,7
    )
    SELECT 
        drug_name,
        manufacturer_group,
        year,
        COUNT(DISTINCT npi) AS prescribers,
        SUM(unique_patients) AS total_patients,
        SUM(prescriptions) AS total_prescriptions,
        SUM(total_payment) AS total_payment
    FROM glp1_drugs
    GROUP BY 1,2,3
    ORDER BY year, total_payment DESC
    """
    
    df = client.query(query).to_dataframe()
    
    # Save raw market share data
    df.to_csv(f"{output_dir}/glp1_market_share.csv", index=False)
    
    print("\n=== GLP-1 MARKET OVERVIEW AT COREWELL ===")
    overall_summary = df.groupby('drug_name').agg({
        'prescribers': lambda x: x.max(),
        'total_patients': 'sum',
        'total_prescriptions': 'sum',
        'total_payment': 'sum'
    }).sort_values('total_payment', ascending=False)
    print(overall_summary)
    
    # Calculate market share percentages
    df['total_payment'] = pd.to_numeric(df['total_payment'], errors='coerce').fillna(0)
    total_revenue = df['total_payment'].sum()
    if total_revenue > 0:
        overall_summary['total_payment'] = pd.to_numeric(overall_summary['total_payment'], errors='coerce').fillna(0)
        overall_summary['market_share_pct'] = (overall_summary['total_payment'] / total_revenue * 100).round(1)
        print("\n=== MARKET SHARE BY REVENUE ===")
        print(overall_summary[['total_payment', 'market_share_pct']].head(10))
    
    # Manufacturer analysis
    print("\n=== MARKET SHARE BY MANUFACTURER ===")
    manufacturer_summary = df.groupby('manufacturer_group')['total_payment'].sum().sort_values(ascending=False)
    if manufacturer_summary.sum() > 0:
        manufacturer_pct = (manufacturer_summary / manufacturer_summary.sum() * 100).round(1)
        print(manufacturer_pct)
    
    # Temporal trends
    print("\n=== YEARLY TRENDS FOR TOP GLP-1 DRUGS ===")
    yearly_trends = df.pivot_table(
        index='year',
        columns='drug_name',
        values='total_payment',
        aggfunc='sum',
        fill_value=0
    )
    # Show only top drugs
    top_drugs = overall_summary.head(5).index.tolist()
    available_drugs = [drug for drug in top_drugs if drug in yearly_trends.columns]
    if available_drugs:
        print(yearly_trends[available_drugs])
    
    return df

def analyze_provider_loyalty():
    """Analyze provider loyalty to manufacturers based on payments"""
    
    print("\n\nAnalyzing provider loyalty patterns...")
    
    query = f"""
    WITH glp1_prescribing AS (
        SELECT 
            rx.NPI AS npi,
            CASE 
                WHEN UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%' THEN 'Ozempic'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%WEGOVY%' THEN 'Wegovy'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%TRULICITY%' THEN 'Trulicity'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%MOUNJARO%' THEN 'Mounjaro'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%VICTOZA%' THEN 'Victoza'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%RYBELSUS%' THEN 'Rybelsus'
                ELSE 'Other'
            END AS drug_name,
            CASE 
                WHEN UPPER(rx.MANUFACTURER) LIKE '%NOVO%' THEN 'Novo Nordisk'
                WHEN UPPER(rx.MANUFACTURER) LIKE '%LILLY%' THEN 'Eli Lilly'
                ELSE 'Other'
            END AS manufacturer,
            SUM(rx.PRESCRIPTIONS) AS prescriptions,
            SUM(rx.PAYMENTS) AS total_payment
        FROM `{RX_TABLE}` rx
        WHERE rx.NPI IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
            AND UPPER(rx.BRAND_NAME) IN ('OZEMPIC', 'WEGOVY', 'TRULICITY', 'MOUNJARO', 'VICTOZA', 'RYBELSUS')
        GROUP BY 1,2,3
    ),
    manufacturer_payments AS (
        SELECT 
            CAST(covered_recipient_npi AS STRING) AS npi,
            CASE 
                WHEN UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%NOVO%' THEN 'Novo Nordisk'
                WHEN UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%LILLY%' THEN 'Eli Lilly'
                ELSE 'Other'
            END AS paying_manufacturer,
            SUM(total_amount_of_payment_usdollars) AS payment_amount
        FROM `{OP_TABLE}`
        WHERE covered_recipient_npi IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
            AND (UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%NOVO%' 
                 OR UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%LILLY%')
        GROUP BY 1,2
    ),
    provider_profiles AS (
        SELECT 
            gp.npi,
            gp.manufacturer AS prescribing_manufacturer,
            SUM(gp.prescriptions) AS total_prescriptions,
            SUM(gp.total_payment) AS total_rx_payment
        FROM glp1_prescribing gp
        GROUP BY 1,2
    )
    SELECT 
        COALESCE(mp.paying_manufacturer, 'No Payments') AS payment_source,
        pp.prescribing_manufacturer,
        COUNT(DISTINCT pp.npi) AS providers,
        SUM(pp.total_prescriptions) AS total_prescriptions,
        SUM(pp.total_rx_payment) AS total_payment,
        AVG(pp.total_prescriptions) AS avg_prescriptions_per_provider
    FROM provider_profiles pp
    LEFT JOIN manufacturer_payments mp ON CAST(pp.npi AS STRING) = mp.npi
    GROUP BY 1,2
    ORDER BY 1,2
    """
    
    df = client.query(query).to_dataframe()
    
    # Save loyalty data
    df.to_csv(f"{output_dir}/glp1_provider_loyalty.csv", index=False)
    
    print("\n=== PROVIDER LOYALTY ANALYSIS ===")
    print(df.pivot_table(
        index='payment_source',
        columns='prescribing_manufacturer',
        values='total_prescriptions',
        aggfunc='sum',
        fill_value=0
    ))
    
    # Calculate loyalty metrics
    print("\n=== LOYALTY METRICS ===")
    for manufacturer in ['Novo Nordisk', 'Eli Lilly']:
        mfr_data = df[df['payment_source'] == manufacturer]
        if not mfr_data.empty:
            same_mfr = mfr_data[mfr_data['prescribing_manufacturer'] == manufacturer]['total_prescriptions'].sum() if not mfr_data[mfr_data['prescribing_manufacturer'] == manufacturer].empty else 0
            total_mfr = mfr_data['total_prescriptions'].sum()
            if total_mfr > 0:
                loyalty_rate = (same_mfr / total_mfr * 100)
                print(f"{manufacturer} payment recipients: {loyalty_rate:.1f}% prescribe same manufacturer")
    
    return df

def analyze_ozempic_vs_competitors():
    """Direct comparison of Ozempic vs main competitors"""
    
    print("\n\nAnalyzing Ozempic vs competitors...")
    
    query = f"""
    WITH drug_comparison AS (
        SELECT 
            rx.NPI AS npi,
            rx.CLAIM_YEAR AS year,
            SUM(CASE WHEN UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%' THEN rx.PRESCRIPTIONS ELSE 0 END) AS ozempic_rx,
            SUM(CASE WHEN UPPER(rx.BRAND_NAME) LIKE '%WEGOVY%' THEN rx.PRESCRIPTIONS ELSE 0 END) AS wegovy_rx,
            SUM(CASE WHEN UPPER(rx.BRAND_NAME) LIKE '%TRULICITY%' THEN rx.PRESCRIPTIONS ELSE 0 END) AS trulicity_rx,
            SUM(CASE WHEN UPPER(rx.BRAND_NAME) LIKE '%MOUNJARO%' THEN rx.PRESCRIPTIONS ELSE 0 END) AS mounjaro_rx,
            SUM(CASE WHEN UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%' THEN rx.PAYMENTS ELSE 0 END) AS ozempic_payment,
            SUM(CASE WHEN UPPER(rx.BRAND_NAME) LIKE '%WEGOVY%' THEN rx.PAYMENTS ELSE 0 END) AS wegovy_payment,
            SUM(CASE WHEN UPPER(rx.BRAND_NAME) LIKE '%TRULICITY%' THEN rx.PAYMENTS ELSE 0 END) AS trulicity_payment,
            SUM(CASE WHEN UPPER(rx.BRAND_NAME) LIKE '%MOUNJARO%' THEN rx.PAYMENTS ELSE 0 END) AS mounjaro_payment
        FROM `{RX_TABLE}` rx
        WHERE rx.NPI IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
            AND UPPER(rx.BRAND_NAME) IN ('OZEMPIC', 'WEGOVY', 'TRULICITY', 'MOUNJARO')
        GROUP BY 1,2
    ),
    novo_payments AS (
        SELECT 
            CAST(covered_recipient_npi AS STRING) AS npi,
            1 AS has_novo_payment,
            SUM(total_amount_of_payment_usdollars) AS novo_payment_amount
        FROM `{OP_TABLE}`
        WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%NOVO%'
            AND covered_recipient_npi IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
        GROUP BY 1
    ),
    lilly_payments AS (
        SELECT 
            CAST(covered_recipient_npi AS STRING) AS npi,
            1 AS has_lilly_payment,
            SUM(total_amount_of_payment_usdollars) AS lilly_payment_amount
        FROM `{OP_TABLE}`
        WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%LILLY%'
            AND covered_recipient_npi IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
        GROUP BY 1
    )
    SELECT 
        dc.year,
        CASE 
            WHEN np.has_novo_payment = 1 AND lp.has_lilly_payment = 1 THEN 'Both Manufacturers'
            WHEN np.has_novo_payment = 1 THEN 'Novo Only'
            WHEN lp.has_lilly_payment = 1 THEN 'Lilly Only'
            ELSE 'No Payments'
        END AS payment_status,
        COUNT(DISTINCT dc.npi) AS providers,
        SUM(dc.ozempic_rx) AS total_ozempic_rx,
        SUM(dc.wegovy_rx) AS total_wegovy_rx,
        SUM(dc.trulicity_rx) AS total_trulicity_rx,
        SUM(dc.mounjaro_rx) AS total_mounjaro_rx,
        SUM(dc.ozempic_payment + dc.wegovy_payment) AS total_novo_product_revenue,
        SUM(dc.trulicity_payment + dc.mounjaro_payment) AS total_lilly_product_revenue
    FROM drug_comparison dc
    LEFT JOIN novo_payments np ON CAST(dc.npi AS STRING) = np.npi
    LEFT JOIN lilly_payments lp ON CAST(dc.npi AS STRING) = lp.npi
    GROUP BY 1,2
    ORDER BY 1,2
    """
    
    df = client.query(query).to_dataframe()
    
    # Save comparison data
    df.to_csv(f"{output_dir}/ozempic_vs_competitors.csv", index=False)
    
    print("\n=== OZEMPIC VS COMPETITORS BY PAYMENT STATUS ===")
    summary = df.groupby('payment_status').agg({
        'providers': 'sum',
        'total_ozempic_rx': 'sum',
        'total_trulicity_rx': 'sum',
        'total_mounjaro_rx': 'sum'
    })
    print(summary)
    
    # Calculate market share by payment status
    print("\n=== MARKET SHARE BY PAYMENT STATUS ===")
    for status in df['payment_status'].unique():
        status_data = df[df['payment_status'] == status]
        total_rx = (status_data['total_ozempic_rx'].sum() + 
                   status_data['total_wegovy_rx'].sum() + 
                   status_data['total_trulicity_rx'].sum() + 
                   status_data['total_mounjaro_rx'].sum())
        
        if total_rx > 0:
            ozempic_share = status_data['total_ozempic_rx'].sum() / total_rx * 100
            print(f"{status}: Ozempic has {ozempic_share:.1f}% market share")
    
    # Yearly trend
    print("\n=== YEARLY COMPETITION TRENDS ===")
    yearly = df.groupby('year').agg({
        'total_ozempic_rx': 'sum',
        'total_trulicity_rx': 'sum',
        'total_mounjaro_rx': 'sum'
    })
    print(yearly)
    
    return df

def analyze_switching_patterns():
    """Analyze providers switching between GLP-1 drugs"""
    
    print("\n\nAnalyzing drug switching patterns...")
    
    query = f"""
    WITH provider_drug_years AS (
        SELECT 
            rx.NPI AS npi,
            rx.CLAIM_YEAR AS year,
            CASE 
                WHEN UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%' THEN 'Ozempic'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%WEGOVY%' THEN 'Wegovy'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%TRULICITY%' THEN 'Trulicity'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%MOUNJARO%' THEN 'Mounjaro'
                WHEN UPPER(rx.BRAND_NAME) LIKE '%VICTOZA%' THEN 'Victoza'
                ELSE 'Other'
            END AS drug_name,
            SUM(rx.PRESCRIPTIONS) AS prescriptions
        FROM `{RX_TABLE}` rx
        WHERE rx.NPI IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
            AND UPPER(rx.BRAND_NAME) IN ('OZEMPIC', 'WEGOVY', 'TRULICITY', 'MOUNJARO', 'VICTOZA')
            AND CAST(rx.CLAIM_YEAR AS INT64) >= 2020
        GROUP BY 1,2,3
        HAVING SUM(rx.PRESCRIPTIONS) >= 10  -- Meaningful volume
    ),
    provider_patterns AS (
        SELECT 
            npi,
            year,
            STRING_AGG(DISTINCT drug_name ORDER BY drug_name) AS drugs_prescribed,
            COUNT(DISTINCT drug_name) AS num_drugs
        FROM provider_drug_years
        GROUP BY 1,2
    )
    SELECT 
        year,
        drugs_prescribed,
        COUNT(DISTINCT npi) AS provider_count,
        AVG(num_drugs) AS avg_drugs_per_provider
    FROM provider_patterns
    GROUP BY 1,2
    HAVING COUNT(DISTINCT npi) >= 5  -- Meaningful sample
    ORDER BY year, provider_count DESC
    """
    
    df = client.query(query).to_dataframe()
    
    print("\n=== PRESCRIBING PATTERNS BY YEAR ===")
    pattern_summary = df.groupby('year')['provider_count'].sum()
    print(pattern_summary)
    
    # Show most common combinations
    print("\n=== MOST COMMON DRUG COMBINATIONS ===")
    top_combinations = df.groupby('drugs_prescribed')['provider_count'].sum().sort_values(ascending=False).head(10)
    print(top_combinations)
    
    df.to_csv(f"{output_dir}/glp1_switching_patterns.csv", index=False)
    
    return df

if __name__ == "__main__":
    print("="*60)
    print("GLP-1 COMPARATIVE ANALYSIS - COREWELL HEALTH")
    print("="*60)
    
    try:
        # Run analyses
        market_share_df = analyze_glp1_market_share()
        loyalty_df = analyze_provider_loyalty()
        competition_df = analyze_ozempic_vs_competitors()
        switching_df = analyze_switching_patterns()
        
        print("\n" + "="*60)
        print("COMPARATIVE ANALYSIS COMPLETE")
        print("="*60)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()