#!/usr/bin/env python3
"""
Analyze Ozempic prescribing patterns at Corewell Health
Compare providers with and without Novo Nordisk payments
Using correct column names from data dictionary
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

def analyze_ozempic_prescribing():
    """Comprehensive analysis of Ozempic prescribing at Corewell"""
    
    print("Analyzing Ozempic prescribing patterns at Corewell Health...")
    
    query = f"""
    WITH corewell_ozempic AS (
        SELECT 
            rx.NPI as npi,
            rx.CLAIM_YEAR as year,
            rx.BRAND_NAME,
            rx.GENERIC_NAME,
            rx.MANUFACTURER,
            SUM(rx.UNIQUE_PATIENTS) AS unique_patients,
            SUM(rx.PRESCRIPTIONS) AS total_prescriptions,
            SUM(rx.PAYMENTS) AS total_payment,
            SUM(rx.DAYS_SUPPLY) AS total_days_supply
        FROM `{RX_TABLE}` rx
        WHERE UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%'
            AND rx.NPI IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
        GROUP BY 1,2,3,4,5
    ),
    provider_info AS (
        SELECT 
            co.*,
            cs.Full_Name AS provider_name,
            cs.Primary_Specialty,
            cs.Primary_Hospital_Affiliation
        FROM corewell_ozempic co
        LEFT JOIN `{NPI_TABLE}` cs
            ON CAST(co.npi AS STRING) = cs.NPI
    ),
    novo_payments AS (
        SELECT 
            CAST(covered_recipient_npi AS STRING) AS npi,
            SUM(total_amount_of_payment_usdollars) AS total_novo_payments,
            COUNT(*) AS novo_transaction_count,
            SUM(CASE 
                WHEN UPPER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%OZEMPIC%' 
                THEN total_amount_of_payment_usdollars 
                ELSE 0 
            END) AS ozempic_specific_payments
        FROM `{OP_TABLE}`
        WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%NOVO%NORDISK%'
            AND covered_recipient_npi IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
        GROUP BY 1
    )
    SELECT 
        pi.*,
        CASE WHEN np.npi IS NOT NULL THEN 1 ELSE 0 END AS received_novo_payment,
        COALESCE(np.total_novo_payments, 0) AS total_novo_payments,
        COALESCE(np.novo_transaction_count, 0) AS novo_transaction_count,
        COALESCE(np.ozempic_specific_payments, 0) AS ozempic_specific_payments
    FROM provider_info pi
    LEFT JOIN novo_payments np ON CAST(pi.npi AS STRING) = np.npi
    ORDER BY pi.total_payment DESC
    """
    
    df = client.query(query).to_dataframe()
    
    # Save raw prescribing data
    df.to_csv(f"{output_dir}/ozempic_prescribing_detailed.csv", index=False)
    
    print("\n=== OZEMPIC PRESCRIBING OVERVIEW AT COREWELL ===")
    print(f"Total Ozempic prescribers: {df['npi'].nunique():,}")
    print(f"Total prescriptions: {df['total_prescriptions'].sum():,}")
    print(f"Total patients: {df['unique_patients'].sum():,}")
    print(f"Total revenue: ${df['total_payment'].sum():,.2f}")
    if df['total_prescriptions'].sum() > 0:
        print(f"Average cost per prescription: ${df['total_payment'].sum() / df['total_prescriptions'].sum():,.2f}")
    
    # Yearly trends
    yearly = df.groupby('year').agg({
        'npi': 'nunique',
        'unique_patients': 'sum',
        'total_prescriptions': 'sum',
        'total_payment': 'sum'
    }).reset_index()
    print("\n=== YEARLY OZEMPIC TRENDS AT COREWELL ===")
    print(yearly)
    yearly.to_csv(f"{output_dir}/ozempic_yearly_trends.csv", index=False)
    
    # Payment influence analysis
    payment_influence = df.groupby('received_novo_payment').agg({
        'npi': 'nunique',
        'unique_patients': 'sum',
        'total_prescriptions': 'sum',
        'total_payment': 'sum',
        'total_novo_payments': 'sum'
    }).reset_index()
    
    if len(payment_influence) > 0:
        payment_influence['avg_rx_per_provider'] = payment_influence['total_prescriptions'] / payment_influence['npi']
        payment_influence['avg_payment_per_provider'] = payment_influence['total_payment'] / payment_influence['npi']
        payment_influence['avg_patients_per_provider'] = payment_influence['unique_patients'] / payment_influence['npi']
    
    print("\n=== PAYMENT INFLUENCE ON OZEMPIC PRESCRIBING ===")
    print(payment_influence)
    payment_influence.to_csv(f"{output_dir}/ozempic_payment_influence.csv", index=False)
    
    # Calculate influence ratio
    if len(payment_influence) == 2:
        no_payment = payment_influence[payment_influence['received_novo_payment'] == 0].iloc[0] if not payment_influence[payment_influence['received_novo_payment'] == 0].empty else None
        with_payment = payment_influence[payment_influence['received_novo_payment'] == 1].iloc[0] if not payment_influence[payment_influence['received_novo_payment'] == 1].empty else None
        
        if no_payment is not None and with_payment is not None:
            if no_payment['avg_rx_per_provider'] > 0:
                rx_ratio = with_payment['avg_rx_per_provider'] / no_payment['avg_rx_per_provider']
                print(f"\n=== KEY INFLUENCE METRICS ===")
                print(f"Prescription ratio (paid/unpaid): {rx_ratio:.1f}x")
            if no_payment['avg_payment_per_provider'] > 0:
                payment_ratio = with_payment['avg_payment_per_provider'] / no_payment['avg_payment_per_provider']
                print(f"Revenue ratio (paid/unpaid): {payment_ratio:.1f}x")
            if no_payment['avg_patients_per_provider'] > 0:
                patient_ratio = with_payment['avg_patients_per_provider'] / no_payment['avg_patients_per_provider']
                print(f"Patient ratio (paid/unpaid): {patient_ratio:.1f}x")
            
            total_providers = with_payment['npi'] + no_payment['npi']
            print(f"Providers with payments: {with_payment['npi']:,} ({with_payment['npi']/total_providers*100:.1f}%)")
            
            # Calculate ROI
            if with_payment['total_novo_payments'] > 0:
                roi = with_payment['total_payment'] / with_payment['total_novo_payments']
                print(f"ROI for Novo Nordisk: ${roi:.2f} per dollar of payments")
    
    # Specialty analysis
    specialty = df.groupby(['Primary_Specialty', 'received_novo_payment']).agg({
        'npi': 'nunique',
        'total_prescriptions': 'sum',
        'total_payment': 'sum'
    }).reset_index()
    print("\n=== TOP SPECIALTIES PRESCRIBING OZEMPIC ===")
    top_specialties = df.groupby('Primary_Specialty')['total_payment'].sum().sort_values(ascending=False).head(10)
    print(top_specialties)
    specialty.to_csv(f"{output_dir}/ozempic_by_specialty.csv", index=False)
    
    # Top prescribers
    top_prescribers = df.groupby(['npi', 'provider_name', 'Primary_Specialty', 'received_novo_payment']).agg({
        'unique_patients': 'sum',
        'total_prescriptions': 'sum',
        'total_payment': 'sum',
        'total_novo_payments': 'first'
    }).sort_values('total_payment', ascending=False).reset_index()
    
    print("\n=== TOP 20 OZEMPIC PRESCRIBERS AT COREWELL ===")
    print(top_prescribers.head(20)[['provider_name', 'Primary_Specialty', 'total_prescriptions', 'total_payment', 'received_novo_payment']])
    top_prescribers.to_csv(f"{output_dir}/ozempic_top_prescribers.csv", index=False)
    
    return df

def analyze_payment_tiers():
    """Analyze prescribing by payment amount tiers"""
    
    print("\n\nAnalyzing prescribing patterns by payment tiers...")
    
    query = f"""
    WITH ozempic_rx AS (
        SELECT 
            CAST(NPI AS STRING) AS npi,
            SUM(PRESCRIPTIONS) AS total_prescriptions,
            SUM(PAYMENTS) AS total_rx_payment,
            SUM(UNIQUE_PATIENTS) AS unique_patients,
            AVG(DAYS_SUPPLY) AS avg_days_supply
        FROM `{RX_TABLE}`
        WHERE UPPER(BRAND_NAME) LIKE '%OZEMPIC%'
            AND NPI IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
        GROUP BY 1
    ),
    novo_payments AS (
        SELECT 
            CAST(covered_recipient_npi AS STRING) AS npi,
            SUM(total_amount_of_payment_usdollars) AS total_payments,
            COUNT(DISTINCT program_year) AS years_receiving,
            COUNT(*) AS transaction_count
        FROM `{OP_TABLE}`
        WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%NOVO%NORDISK%'
            AND covered_recipient_npi IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
        GROUP BY 1
    )
    SELECT 
        CASE 
            WHEN np.total_payments IS NULL THEN 'No Payment'
            WHEN np.total_payments < 100 THEN '$1-99'
            WHEN np.total_payments < 500 THEN '$100-499'
            WHEN np.total_payments < 1000 THEN '$500-999'
            WHEN np.total_payments < 5000 THEN '$1,000-4,999'
            WHEN np.total_payments < 10000 THEN '$5,000-9,999'
            WHEN np.total_payments < 50000 THEN '$10,000-49,999'
            ELSE '$50,000+'
        END AS payment_tier,
        COUNT(DISTINCT orx.npi) AS provider_count,
        SUM(orx.total_prescriptions) AS total_prescriptions,
        SUM(orx.total_rx_payment) AS total_revenue,
        AVG(orx.total_prescriptions) AS avg_rx_per_provider,
        AVG(orx.total_rx_payment) AS avg_revenue_per_provider,
        SUM(orx.unique_patients) AS total_patients
    FROM ozempic_rx orx
    LEFT JOIN novo_payments np ON orx.npi = np.npi
    GROUP BY 1
    ORDER BY 
        CASE payment_tier
            WHEN 'No Payment' THEN 0
            WHEN '$1-99' THEN 1
            WHEN '$100-499' THEN 2
            WHEN '$500-999' THEN 3
            WHEN '$1,000-4,999' THEN 4
            WHEN '$5,000-9,999' THEN 5
            WHEN '$10,000-49,999' THEN 6
            ELSE 7
        END
    """
    
    df = client.query(query).to_dataframe()
    
    print("\n=== PRESCRIBING BY PAYMENT TIER ===")
    print(df[['payment_tier', 'provider_count', 'avg_rx_per_provider', 'avg_revenue_per_provider']])
    
    # Calculate ROI for each tier
    if 'No Payment' in df['payment_tier'].values:
        baseline = df[df['payment_tier'] == 'No Payment']['avg_revenue_per_provider'].iloc[0] if not df[df['payment_tier'] == 'No Payment'].empty else 0
        if baseline > 0:
            df['revenue_increase'] = df['avg_revenue_per_provider'] - baseline
            df['influence_factor'] = df['avg_revenue_per_provider'] / baseline
            
            print("\n=== INFLUENCE FACTOR BY PAYMENT TIER ===")
            print(df[df['payment_tier'] != 'No Payment'][['payment_tier', 'provider_count', 'influence_factor']])
    
    df.to_csv(f"{output_dir}/ozempic_payment_tiers.csv", index=False)
    
    return df

if __name__ == "__main__":
    print("="*60)
    print("OZEMPIC PRESCRIBING ANALYSIS - COREWELL HEALTH")
    print("="*60)
    
    try:
        # Run analyses
        prescribing_df = analyze_ozempic_prescribing()
        payment_tiers_df = analyze_payment_tiers()
        
        print("\n" + "="*60)
        print("PRESCRIBING ANALYSIS COMPLETE")
        print("="*60)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()