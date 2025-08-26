#!/usr/bin/env python3
"""
Analyze payor patterns for Ozempic at Corewell Health
Since patient demographics are not available, focus on insurance/payor analysis
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

def analyze_payor_patterns():
    """Analyze insurance/payor patterns for Ozempic"""
    
    print("Analyzing payor patterns for Ozempic at Corewell Health...")
    
    query = f"""
    WITH ozempic_payors AS (
        SELECT 
            rx.NPI AS npi,
            rx.CLAIM_YEAR AS year,
            rx.PAYOR_TYPE,
            rx.PAYOR_NAME,
            SUM(rx.PRESCRIPTIONS) AS prescriptions,
            SUM(rx.PAYMENTS) AS payments,
            SUM(rx.CHARGES) AS charges,
            SUM(rx.UNIQUE_PATIENTS) AS patients,
            SUM(rx.DAYS_SUPPLY) AS days_supply
        FROM `{RX_TABLE}` rx
        WHERE UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%'
            AND rx.NPI IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
        GROUP BY 1,2,3,4
    ),
    provider_payments AS (
        SELECT 
            CAST(covered_recipient_npi AS STRING) AS npi,
            1 AS has_novo_payment,
            SUM(total_amount_of_payment_usdollars) AS payment_amount
        FROM `{OP_TABLE}`
        WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%NOVO%NORDISK%'
            AND covered_recipient_npi IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
        GROUP BY 1
    )
    SELECT 
        op.PAYOR_TYPE,
        op.year,
        CASE WHEN pp.has_novo_payment = 1 THEN 'Provider With Payments' ELSE 'Provider No Payments' END AS provider_payment_status,
        COUNT(DISTINCT op.npi) AS unique_prescribers,
        SUM(op.prescriptions) AS total_prescriptions,
        SUM(op.payments) AS total_payments,
        SUM(op.charges) AS total_charges,
        SUM(op.patients) AS total_patients,
        AVG(op.days_supply / NULLIF(op.prescriptions, 0)) AS avg_days_supply,
        AVG(op.payments / NULLIF(op.prescriptions, 0)) AS avg_cost_per_rx
    FROM ozempic_payors op
    LEFT JOIN provider_payments pp ON CAST(op.npi AS STRING) = pp.npi
    GROUP BY 1,2,3
    ORDER BY 2,1,3
    """
    
    df = client.query(query).to_dataframe()
    
    # Save payor data
    df.to_csv(f"{output_dir}/ozempic_payor_patterns.csv", index=False)
    
    print("\n=== PAYOR TYPE OVERVIEW ===")
    payor_summary = df.groupby('PAYOR_TYPE').agg({
        'total_patients': 'sum',
        'total_prescriptions': 'sum',
        'total_payments': 'sum',
        'avg_cost_per_rx': 'mean'
    }).sort_values('total_patients', ascending=False)
    print(payor_summary)
    
    # Compare payor mix by provider payment status
    print("\n=== PAYOR MIX BY PROVIDER PAYMENT STATUS ===")
    payor_mix = df.pivot_table(
        index='PAYOR_TYPE',
        columns='provider_payment_status',
        values='total_patients',
        aggfunc='sum',
        fill_value=0
    )
    
    if not payor_mix.empty and len(payor_mix.columns) > 0:
        for col in payor_mix.columns:
            col_name = f'{col}_Percentage'
            payor_mix[col] = pd.to_numeric(payor_mix[col], errors='coerce').fillna(0)
            payor_mix[col_name] = (payor_mix[col] / payor_mix[col].sum() * 100).round(1)
        
        print(payor_mix)
    
    # Temporal analysis
    print("\n=== YEARLY TRENDS BY PAYOR TYPE ===")
    yearly_payors = df.groupby(['year', 'PAYOR_TYPE'])['total_prescriptions'].sum().unstack(fill_value=0)
    print(yearly_payors)
    
    return df

def analyze_top_payors():
    """Analyze top insurance companies paying for Ozempic"""
    
    print("\n\nAnalyzing top insurance payors...")
    
    query = f"""
    WITH ozempic_by_payor_name AS (
        SELECT 
            rx.PAYOR_NAME,
            rx.PAYOR_TYPE,
            rx.NPI AS npi,
            SUM(rx.PRESCRIPTIONS) AS prescriptions,
            SUM(rx.PAYMENTS) AS payments,
            SUM(rx.UNIQUE_PATIENTS) AS patients
        FROM `{RX_TABLE}` rx
        WHERE UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%'
            AND rx.NPI IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
        GROUP BY 1,2,3
    ),
    provider_payments AS (
        SELECT 
            CAST(covered_recipient_npi AS STRING) AS npi,
            1 AS has_payment
        FROM `{OP_TABLE}`
        WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%NOVO%NORDISK%'
        GROUP BY 1
    )
    SELECT 
        obp.PAYOR_NAME,
        obp.PAYOR_TYPE,
        COUNT(DISTINCT obp.npi) AS unique_prescribers,
        SUM(obp.prescriptions) AS total_prescriptions,
        SUM(obp.payments) AS total_payments,
        SUM(obp.patients) AS total_patients,
        SUM(CASE WHEN pp.has_payment = 1 THEN obp.prescriptions ELSE 0 END) AS prescriptions_by_paid_providers,
        SUM(CASE WHEN pp.has_payment IS NULL THEN obp.prescriptions ELSE 0 END) AS prescriptions_by_unpaid_providers
    FROM ozempic_by_payor_name obp
    LEFT JOIN provider_payments pp ON CAST(obp.npi AS STRING) = pp.npi
    GROUP BY 1,2
    HAVING SUM(obp.patients) >= 100  -- Focus on payors with meaningful volume
    ORDER BY total_payments DESC
    LIMIT 20
    """
    
    df = client.query(query).to_dataframe()
    
    print("\n=== TOP 20 INSURANCE PAYORS FOR OZEMPIC ===")
    print(df[['PAYOR_NAME', 'PAYOR_TYPE', 'total_patients', 'total_payments']])
    
    # Calculate percentage from paid providers
    if 'prescriptions_by_paid_providers' in df.columns and 'total_prescriptions' in df.columns:
        df['prescriptions_by_paid_providers'] = pd.to_numeric(df['prescriptions_by_paid_providers'], errors='coerce').fillna(0)
        df['total_prescriptions'] = pd.to_numeric(df['total_prescriptions'], errors='coerce').fillna(0)
        df['pct_from_paid_providers'] = (df['prescriptions_by_paid_providers'] / df['total_prescriptions'] * 100).round(1)
        print("\n=== INFLUENCE ON TOP PAYORS ===")
        print(df[['PAYOR_NAME', 'pct_from_paid_providers']].head(10))
    
    df.to_csv(f"{output_dir}/ozempic_top_payors.csv", index=False)
    
    return df

def analyze_coverage_patterns():
    """Analyze coverage and reimbursement patterns"""
    
    print("\n\nAnalyzing coverage and reimbursement patterns...")
    
    query = f"""
    WITH ozempic_coverage AS (
        SELECT 
            rx.NPI AS npi,
            rx.CLAIM_YEAR AS year,
            rx.PAYOR_TYPE,
            SUM(rx.CHARGES) AS total_charges,
            SUM(rx.PAYMENTS) AS total_payments,
            SUM(rx.PRESCRIPTIONS) AS total_prescriptions,
            AVG(rx.PAYMENTS / NULLIF(rx.CHARGES, 0)) AS avg_reimbursement_rate
        FROM `{RX_TABLE}` rx
        WHERE UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%'
            AND rx.NPI IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
            AND rx.CHARGES > 0
        GROUP BY 1,2,3
    ),
    provider_payments AS (
        SELECT 
            CAST(covered_recipient_npi AS STRING) AS npi,
            1 AS has_payment
        FROM `{OP_TABLE}`
        WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%NOVO%NORDISK%'
        GROUP BY 1
    )
    SELECT 
        oc.PAYOR_TYPE,
        oc.year,
        CASE WHEN pp.has_payment = 1 THEN 'With Payments' ELSE 'No Payments' END AS provider_status,
        COUNT(DISTINCT oc.npi) AS prescribers,
        SUM(oc.total_charges) AS total_charges,
        SUM(oc.total_payments) AS total_reimbursements,
        AVG(oc.avg_reimbursement_rate) AS avg_reimbursement_rate,
        SUM(oc.total_prescriptions) AS total_prescriptions
    FROM ozempic_coverage oc
    LEFT JOIN provider_payments pp ON CAST(oc.npi AS STRING) = pp.npi
    GROUP BY 1,2,3
    ORDER BY 1,2,3
    """
    
    df = client.query(query).to_dataframe()
    
    # Calculate actual reimbursement rates
    df['actual_reimbursement_rate'] = (df['total_reimbursements'] / df['total_charges'] * 100).round(1)
    
    print("\n=== REIMBURSEMENT RATES BY PAYOR TYPE ===")
    reimbursement_summary = df.groupby('PAYOR_TYPE').agg({
        'total_charges': 'sum',
        'total_reimbursements': 'sum',
        'actual_reimbursement_rate': 'mean'
    })
    reimbursement_summary['overall_rate'] = (reimbursement_summary['total_reimbursements'] / reimbursement_summary['total_charges'] * 100).round(1)
    print(reimbursement_summary[['overall_rate']])
    
    # Compare by payment status
    print("\n=== REIMBURSEMENT BY PROVIDER PAYMENT STATUS ===")
    payment_comparison = df.groupby('provider_status').agg({
        'total_charges': 'sum',
        'total_reimbursements': 'sum'
    })
    payment_comparison['reimbursement_rate'] = (payment_comparison['total_reimbursements'] / payment_comparison['total_charges'] * 100).round(1)
    print(payment_comparison)
    
    df.to_csv(f"{output_dir}/ozempic_coverage_patterns.csv", index=False)
    
    return df

def analyze_access_disparities():
    """Analyze access patterns across different payor types"""
    
    print("\n\nAnalyzing access disparities...")
    
    query = f"""
    WITH provider_metrics AS (
        SELECT 
            rx.NPI AS npi,
            cs.Primary_Specialty,
            rx.PAYOR_TYPE,
            SUM(rx.PRESCRIPTIONS) AS prescriptions,
            SUM(rx.PAYMENTS) AS payments,
            SUM(rx.UNIQUE_PATIENTS) AS patients
        FROM `{RX_TABLE}` rx
        INNER JOIN `{NPI_TABLE}` cs ON CAST(rx.NPI AS STRING) = cs.NPI
        WHERE UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%'
        GROUP BY 1,2,3
    ),
    provider_payments AS (
        SELECT 
            CAST(covered_recipient_npi AS STRING) AS npi,
            1 AS has_payment,
            SUM(total_amount_of_payment_usdollars) AS payment_amount
        FROM `{OP_TABLE}`
        WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%NOVO%NORDISK%'
        GROUP BY 1
    )
    SELECT 
        pm.PAYOR_TYPE,
        CASE WHEN pp.has_payment = 1 THEN 'Provider With Payments' ELSE 'Provider No Payments' END AS provider_status,
        COUNT(DISTINCT pm.npi) AS unique_prescribers,
        SUM(pm.patients) AS total_patients,
        SUM(pm.prescriptions) AS total_prescriptions,
        SUM(pm.payments) AS total_payments,
        AVG(pm.payments / NULLIF(pm.prescriptions, 0)) AS avg_cost_per_rx
    FROM provider_metrics pm
    LEFT JOIN provider_payments pp ON CAST(pm.npi AS STRING) = pp.npi
    GROUP BY 1,2
    ORDER BY 1,2
    """
    
    df = client.query(query).to_dataframe()
    
    print("\n=== ACCESS BY PAYOR TYPE AND PROVIDER STATUS ===")
    access_matrix = df.pivot_table(
        index='PAYOR_TYPE',
        columns='provider_status',
        values='total_patients',
        fill_value=0
    )
    print(access_matrix)
    
    # Calculate disparity metrics
    print("\n=== KEY DISPARITY METRICS ===")
    
    for payor_type in df['PAYOR_TYPE'].unique():
        payor_data = df[df['PAYOR_TYPE'] == payor_type]
        with_payments = payor_data[payor_data['provider_status'] == 'Provider With Payments']
        without_payments = payor_data[payor_data['provider_status'] == 'Provider No Payments']
        
        if not with_payments.empty and not without_payments.empty:
            with_patients = with_payments['total_patients'].iloc[0]
            without_patients = without_payments['total_patients'].iloc[0]
            total_patients = with_patients + without_patients
            
            if total_patients > 100:  # Only report on meaningful volumes
                with_pct = (with_patients / total_patients * 100)
                print(f"{payor_type}: {with_pct:.1f}% of patients with paid providers")
    
    df.to_csv(f"{output_dir}/ozempic_access_disparities.csv", index=False)
    
    return df

if __name__ == "__main__":
    print("="*60)
    print("PAYOR & ACCESS ANALYSIS - COREWELL HEALTH OZEMPIC")
    print("="*60)
    
    try:
        # Run analyses
        payor_df = analyze_payor_patterns()
        top_payors_df = analyze_top_payors()
        coverage_df = analyze_coverage_patterns()
        disparities_df = analyze_access_disparities()
        
        print("\n" + "="*60)
        print("PAYOR ANALYSIS COMPLETE")
        print("="*60)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()