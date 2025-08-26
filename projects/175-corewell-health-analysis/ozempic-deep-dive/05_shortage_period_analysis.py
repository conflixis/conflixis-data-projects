#!/usr/bin/env python3
"""
Analyze Ozempic prescribing patterns during shortage periods at Corewell Health
Focus on allocation decisions and payment influence during scarcity
Using available columns from the RX table
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

# Define shortage periods based on FDA shortage database
# Ozempic shortage began approximately March 2022
SHORTAGE_START_YEAR = 2022
SHORTAGE_START_MONTH = 3

def analyze_shortage_timeline():
    """Analyze prescribing patterns before and during shortage"""
    
    print("Analyzing Ozempic shortage timeline at Corewell Health...")
    
    query = f"""
    WITH monthly_data AS (
        SELECT 
            rx.NPI AS npi,
            rx.CLAIM_YEAR AS year,
            rx.CLAIM_MONTH AS month,
            -- Create shortage period flag
            CASE 
                WHEN rx.CLAIM_YEAR < {SHORTAGE_START_YEAR} THEN 'Pre-Shortage'
                WHEN rx.CLAIM_YEAR = {SHORTAGE_START_YEAR} AND 
                     CASE rx.CLAIM_MONTH
                        WHEN 'January' THEN 1 WHEN 'February' THEN 2 WHEN 'March' THEN 3
                        WHEN 'April' THEN 4 WHEN 'May' THEN 5 WHEN 'June' THEN 6
                        WHEN 'July' THEN 7 WHEN 'August' THEN 8 WHEN 'September' THEN 9
                        WHEN 'October' THEN 10 WHEN 'November' THEN 11 WHEN 'December' THEN 12
                     END < {SHORTAGE_START_MONTH} THEN 'Pre-Shortage'
                ELSE 'Shortage Period'
            END AS shortage_status,
            SUM(rx.UNIQUE_PATIENTS) AS unique_patients,
            SUM(rx.PRESCRIPTIONS) AS prescriptions,
            SUM(rx.PAYMENTS) AS revenue,
            AVG(rx.DAYS_SUPPLY / NULLIF(rx.PRESCRIPTIONS, 0)) AS avg_days_supply
        FROM `{RX_TABLE}` rx
        WHERE UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%'
            AND rx.NPI IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
            AND rx.CLAIM_YEAR >= 2021
        GROUP BY 1,2,3,4
    ),
    provider_payments AS (
        SELECT 
            CAST(covered_recipient_npi AS STRING) AS npi,
            1 AS has_novo_payment,
            SUM(total_amount_of_payment_usdollars) AS total_payments
        FROM `{OP_TABLE}`
        WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%NOVO%NORDISK%'
            AND covered_recipient_npi IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
        GROUP BY 1
    )
    SELECT 
        md.shortage_status,
        md.year,
        md.month,
        CASE WHEN pp.has_novo_payment = 1 THEN 'Paid Providers' ELSE 'Unpaid Providers' END AS provider_type,
        COUNT(DISTINCT md.npi) AS active_prescribers,
        SUM(md.unique_patients) AS total_patients,
        SUM(md.prescriptions) AS total_prescriptions,
        SUM(md.revenue) AS total_revenue,
        AVG(md.prescriptions) AS avg_rx_per_provider,
        AVG(md.avg_days_supply) AS avg_days_supply
    FROM monthly_data md
    LEFT JOIN provider_payments pp ON CAST(md.npi AS STRING) = pp.npi
    GROUP BY 1,2,3,4
    ORDER BY 2,
        CASE md.month
            WHEN 'January' THEN 1 WHEN 'February' THEN 2 WHEN 'March' THEN 3
            WHEN 'April' THEN 4 WHEN 'May' THEN 5 WHEN 'June' THEN 6
            WHEN 'July' THEN 7 WHEN 'August' THEN 8 WHEN 'September' THEN 9
            WHEN 'October' THEN 10 WHEN 'November' THEN 11 WHEN 'December' THEN 12
        END,
        4
    """
    
    df = client.query(query).to_dataframe()
    
    # Save detailed timeline
    df.to_csv(f"{output_dir}/ozempic_shortage_timeline.csv", index=False)
    
    # Aggregate by shortage period
    print("\n=== SHORTAGE IMPACT SUMMARY ===")
    summary = df.groupby(['shortage_status', 'provider_type']).agg({
        'active_prescribers': 'mean',
        'total_patients': 'sum',
        'total_prescriptions': 'sum',
        'total_revenue': 'sum',
        'avg_rx_per_provider': 'mean'
    }).round(1)
    print(summary)
    
    # Calculate year-over-year changes
    yearly = df.groupby(['year', 'provider_type']).agg({
        'total_prescriptions': 'sum',
        'total_patients': 'sum',
        'active_prescribers': 'mean'
    })
    print("\n=== YEARLY TRENDS BY PROVIDER TYPE ===")
    print(yearly)
    
    return df

def analyze_shortage_vs_normal():
    """Compare prescribing patterns pre-shortage vs during shortage"""
    
    print("\n\nAnalyzing pre-shortage vs shortage period patterns...")
    
    query = f"""
    WITH period_data AS (
        SELECT 
            rx.NPI AS npi,
            CASE 
                WHEN rx.CLAIM_YEAR < {SHORTAGE_START_YEAR} THEN 'Pre-Shortage'
                WHEN rx.CLAIM_YEAR = {SHORTAGE_START_YEAR} AND 
                     CASE rx.CLAIM_MONTH
                        WHEN 'January' THEN 1 WHEN 'February' THEN 2 WHEN 'March' THEN 3
                        WHEN 'April' THEN 4 WHEN 'May' THEN 5 WHEN 'June' THEN 6
                        WHEN 'July' THEN 7 WHEN 'August' THEN 8 WHEN 'September' THEN 9
                        WHEN 'October' THEN 10 WHEN 'November' THEN 11 WHEN 'December' THEN 12
                     END < {SHORTAGE_START_MONTH} THEN 'Pre-Shortage'
                ELSE 'Shortage Period'
            END AS period,
            SUM(rx.UNIQUE_PATIENTS) AS patients,
            SUM(rx.PRESCRIPTIONS) AS prescriptions,
            SUM(rx.PAYMENTS) AS revenue,
            SUM(rx.DAYS_SUPPLY) AS days_supply
        FROM `{RX_TABLE}` rx
        WHERE UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%'
            AND rx.NPI IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
            AND rx.CLAIM_YEAR >= 2021
        GROUP BY 1,2
    ),
    provider_info AS (
        SELECT 
            pd.*,
            cs.Primary_Specialty,
            CASE WHEN np.npi IS NOT NULL THEN 'With Payments' ELSE 'No Payments' END AS payment_status
        FROM period_data pd
        LEFT JOIN `{NPI_TABLE}` cs ON CAST(pd.npi AS STRING) = cs.NPI
        LEFT JOIN (
            SELECT DISTINCT CAST(covered_recipient_npi AS STRING) AS npi
            FROM `{OP_TABLE}`
            WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%NOVO%NORDISK%'
                AND covered_recipient_npi IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
        ) np ON CAST(pd.npi AS STRING) = np.npi
    )
    SELECT 
        period,
        payment_status,
        COUNT(DISTINCT npi) AS providers,
        SUM(patients) AS total_patients,
        SUM(prescriptions) AS total_prescriptions,
        SUM(revenue) AS total_revenue,
        AVG(prescriptions) AS avg_rx_per_provider,
        AVG(days_supply / NULLIF(prescriptions, 0)) AS avg_days_per_rx
    FROM provider_info
    GROUP BY 1,2
    ORDER BY 1,2
    """
    
    df = client.query(query).to_dataframe()
    
    print("\n=== PRE-SHORTAGE VS SHORTAGE COMPARISON ===")
    print(df)
    
    # Calculate change percentages
    if len(df) >= 4:  # Need both periods and both payment statuses
        pre_paid = df[(df['period'] == 'Pre-Shortage') & (df['payment_status'] == 'With Payments')]
        pre_unpaid = df[(df['period'] == 'Pre-Shortage') & (df['payment_status'] == 'No Payments')]
        short_paid = df[(df['period'] == 'Shortage Period') & (df['payment_status'] == 'With Payments')]
        short_unpaid = df[(df['period'] == 'Shortage Period') & (df['payment_status'] == 'No Payments')]
        
        if not pre_paid.empty and not short_paid.empty:
            paid_change = (short_paid['total_prescriptions'].iloc[0] - pre_paid['total_prescriptions'].iloc[0]) / pre_paid['total_prescriptions'].iloc[0] * 100
            print(f"\nPaid providers prescription change: {paid_change:.1f}%")
            
        if not pre_unpaid.empty and not short_unpaid.empty:
            unpaid_change = (short_unpaid['total_prescriptions'].iloc[0] - pre_unpaid['total_prescriptions'].iloc[0]) / pre_unpaid['total_prescriptions'].iloc[0] * 100
            print(f"Unpaid providers prescription change: {unpaid_change:.1f}%")
    
    df.to_csv(f"{output_dir}/ozempic_shortage_comparison.csv", index=False)
    
    return df

def analyze_specialty_shortage_response():
    """Analyze how different specialties responded to shortage"""
    
    print("\n\nAnalyzing specialty response to shortage...")
    
    query = f"""
    WITH shortage_data AS (
        SELECT 
            rx.NPI AS npi,
            cs.Primary_Specialty,
            CASE 
                WHEN rx.CLAIM_YEAR < {SHORTAGE_START_YEAR} THEN 'Pre-Shortage'
                WHEN rx.CLAIM_YEAR = {SHORTAGE_START_YEAR} AND 
                     CASE rx.CLAIM_MONTH
                        WHEN 'January' THEN 1 WHEN 'February' THEN 2 WHEN 'March' THEN 3
                        WHEN 'April' THEN 4 WHEN 'May' THEN 5 WHEN 'June' THEN 6
                        WHEN 'July' THEN 7 WHEN 'August' THEN 8 WHEN 'September' THEN 9
                        WHEN 'October' THEN 10 WHEN 'November' THEN 11 WHEN 'December' THEN 12
                     END < {SHORTAGE_START_MONTH} THEN 'Pre-Shortage'
                ELSE 'Shortage Period'
            END AS period,
            SUM(rx.PRESCRIPTIONS) AS prescriptions,
            SUM(rx.UNIQUE_PATIENTS) AS patients,
            SUM(rx.PAYMENTS) AS revenue
        FROM `{RX_TABLE}` rx
        INNER JOIN `{NPI_TABLE}` cs ON CAST(rx.NPI AS STRING) = cs.NPI
        WHERE UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%'
        GROUP BY 1,2,3
    ),
    payment_flag AS (
        SELECT 
            sd.*,
            CASE WHEN np.npi IS NOT NULL THEN 'With Payments' ELSE 'No Payments' END AS payment_status
        FROM shortage_data sd
        LEFT JOIN (
            SELECT DISTINCT CAST(covered_recipient_npi AS STRING) AS npi
            FROM `{OP_TABLE}`
            WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%NOVO%NORDISK%'
        ) np ON CAST(sd.npi AS STRING) = np.npi
    )
    SELECT 
        Primary_Specialty,
        period,
        payment_status,
        COUNT(DISTINCT npi) AS providers,
        SUM(prescriptions) AS total_prescriptions,
        SUM(patients) AS total_patients,
        AVG(prescriptions) AS avg_rx_per_provider
    FROM payment_flag
    WHERE Primary_Specialty IN (
        'Endocrinology', 'Internal Medicine', 'Family Practice', 
        'Nurse - Nurse Practitioner', 'Physician Assistant'
    )
    GROUP BY 1,2,3
    HAVING SUM(prescriptions) > 100
    ORDER BY 1,2,3
    """
    
    df = client.query(query).to_dataframe()
    
    print("\n=== SPECIALTY RESPONSE TO SHORTAGE ===")
    
    # Calculate specialty-specific impacts
    for specialty in df['Primary_Specialty'].unique():
        spec_data = df[df['Primary_Specialty'] == specialty]
        print(f"\n{specialty}:")
        
        pre_data = spec_data[spec_data['period'] == 'Pre-Shortage']
        short_data = spec_data[spec_data['period'] == 'Shortage Period']
        
        if not pre_data.empty and not short_data.empty:
            pre_total = pre_data['total_prescriptions'].sum()
            short_total = short_data['total_prescriptions'].sum()
            change = (short_total - pre_total) / pre_total * 100
            print(f"  Overall change: {change:.1f}%")
            
            # Compare paid vs unpaid within specialty
            pre_paid = pre_data[pre_data['payment_status'] == 'With Payments']
            short_paid = short_data[short_data['payment_status'] == 'With Payments']
            
            if not pre_paid.empty and not short_paid.empty:
                paid_change = (short_paid['total_prescriptions'].iloc[0] - pre_paid['total_prescriptions'].iloc[0]) / pre_paid['total_prescriptions'].iloc[0] * 100
                print(f"  Paid providers: {paid_change:.1f}% change")
    
    df.to_csv(f"{output_dir}/ozempic_specialty_shortage_response.csv", index=False)
    
    return df

def analyze_payor_mix_during_shortage():
    """Analyze how payor mix changed during shortage"""
    
    print("\n\nAnalyzing payor mix changes during shortage...")
    
    query = f"""
    WITH payor_data AS (
        SELECT 
            rx.NPI AS npi,
            rx.PAYOR_TYPE,
            CASE 
                WHEN rx.CLAIM_YEAR < {SHORTAGE_START_YEAR} THEN 'Pre-Shortage'
                WHEN rx.CLAIM_YEAR = {SHORTAGE_START_YEAR} AND 
                     CASE rx.CLAIM_MONTH
                        WHEN 'January' THEN 1 WHEN 'February' THEN 2 WHEN 'March' THEN 3
                        WHEN 'April' THEN 4 WHEN 'May' THEN 5 WHEN 'June' THEN 6
                        WHEN 'July' THEN 7 WHEN 'August' THEN 8 WHEN 'September' THEN 9
                        WHEN 'October' THEN 10 WHEN 'November' THEN 11 WHEN 'December' THEN 12
                     END < {SHORTAGE_START_MONTH} THEN 'Pre-Shortage'
                ELSE 'Shortage Period'
            END AS period,
            SUM(rx.PRESCRIPTIONS) AS prescriptions,
            SUM(rx.UNIQUE_PATIENTS) AS patients,
            SUM(rx.PAYMENTS) AS revenue,
            SUM(rx.CHARGES) AS charges
        FROM `{RX_TABLE}` rx
        WHERE UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%'
            AND rx.NPI IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
            AND rx.CLAIM_YEAR >= 2021
        GROUP BY 1,2,3
    ),
    provider_payments AS (
        SELECT DISTINCT CAST(covered_recipient_npi AS STRING) AS npi
        FROM `{OP_TABLE}`
        WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%NOVO%NORDISK%'
            AND covered_recipient_npi IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
    )
    SELECT 
        pd.period,
        pd.PAYOR_TYPE,
        CASE WHEN pp.npi IS NOT NULL THEN 'Paid Providers' ELSE 'Unpaid Providers' END AS provider_type,
        COUNT(DISTINCT pd.npi) AS providers,
        SUM(pd.patients) AS total_patients,
        SUM(pd.prescriptions) AS total_prescriptions,
        SUM(pd.revenue) AS total_revenue,
        AVG(pd.revenue / NULLIF(pd.prescriptions, 0)) AS avg_cost_per_rx
    FROM payor_data pd
    LEFT JOIN provider_payments pp ON CAST(pd.npi AS STRING) = pp.npi
    WHERE pd.PAYOR_TYPE IN ('Commercial', 'Medicare', 'Medicaid', 'PBM', 'Discount/Coupon')
    GROUP BY 1,2,3
    ORDER BY 1,2,3
    """
    
    df = client.query(query).to_dataframe()
    
    print("\n=== PAYOR MIX DURING SHORTAGE ===")
    
    # Analyze payor distribution changes
    pre_shortage = df[df['period'] == 'Pre-Shortage']
    shortage = df[df['period'] == 'Shortage Period']
    
    for payor in ['Commercial', 'Medicare', 'Medicaid']:
        if payor in df['PAYOR_TYPE'].values:
            pre_payor = pre_shortage[pre_shortage['PAYOR_TYPE'] == payor]
            short_payor = shortage[shortage['PAYOR_TYPE'] == payor]
            
            if not pre_payor.empty and not short_payor.empty:
                pre_total = pre_payor['total_prescriptions'].sum()
                short_total = short_payor['total_prescriptions'].sum()
                
                print(f"\n{payor}:")
                print(f"  Pre-shortage: {pre_total:,.0f} prescriptions")
                print(f"  Shortage: {short_total:,.0f} prescriptions")
                print(f"  Change: {(short_total - pre_total) / pre_total * 100:.1f}%")
    
    df.to_csv(f"{output_dir}/ozempic_payor_shortage_changes.csv", index=False)
    
    return df

def generate_shortage_summary():
    """Generate summary statistics for shortage analysis"""
    
    print("\n\n" + "="*60)
    print("SHORTAGE ANALYSIS SUMMARY")
    print("="*60)
    
    query = f"""
    WITH summary_data AS (
        SELECT 
            CASE 
                WHEN rx.CLAIM_YEAR < {SHORTAGE_START_YEAR} THEN 'Pre-Shortage'
                WHEN rx.CLAIM_YEAR = {SHORTAGE_START_YEAR} AND 
                     CASE rx.CLAIM_MONTH
                        WHEN 'January' THEN 1 WHEN 'February' THEN 2 WHEN 'March' THEN 3
                        WHEN 'April' THEN 4 WHEN 'May' THEN 5 WHEN 'June' THEN 6
                        WHEN 'July' THEN 7 WHEN 'August' THEN 8 WHEN 'September' THEN 9
                        WHEN 'October' THEN 10 WHEN 'November' THEN 11 WHEN 'December' THEN 12
                     END < {SHORTAGE_START_MONTH} THEN 'Pre-Shortage'
                ELSE 'Shortage Period'
            END AS period,
            COUNT(DISTINCT rx.NPI) AS unique_prescribers,
            SUM(rx.UNIQUE_PATIENTS) AS total_patients,
            SUM(rx.PRESCRIPTIONS) AS total_prescriptions,
            SUM(rx.PAYMENTS) AS total_revenue,
            AVG(rx.DAYS_SUPPLY / NULLIF(rx.PRESCRIPTIONS, 0)) AS avg_days_supply
        FROM `{RX_TABLE}` rx
        WHERE UPPER(rx.BRAND_NAME) LIKE '%OZEMPIC%'
            AND rx.NPI IN (SELECT CAST(NPI AS INT64) FROM `{NPI_TABLE}`)
            AND rx.CLAIM_YEAR >= 2021
        GROUP BY 1
    )
    SELECT * FROM summary_data
    ORDER BY period
    """
    
    df = client.query(query).to_dataframe()
    
    print("\n=== OVERALL SHORTAGE IMPACT ===")
    print(df)
    
    if len(df) == 2:
        pre = df[df['period'] == 'Pre-Shortage'].iloc[0]
        short = df[df['period'] == 'Shortage Period'].iloc[0]
        
        print("\n=== KEY METRICS ===")
        print(f"Prescriber growth: {(short['unique_prescribers'] - pre['unique_prescribers']) / pre['unique_prescribers'] * 100:.1f}%")
        print(f"Patient growth: {(short['total_patients'] - pre['total_patients']) / pre['total_patients'] * 100:.1f}%")
        print(f"Prescription growth: {(short['total_prescriptions'] - pre['total_prescriptions']) / pre['total_prescriptions'] * 100:.1f}%")
        print(f"Revenue growth: {(short['total_revenue'] - pre['total_revenue']) / pre['total_revenue'] * 100:.1f}%")
        
        # Annualize the shortage period data for fair comparison
        # Pre-shortage: ~14 months (Jan 2021 - Feb 2022)
        # Shortage: ~34 months (Mar 2022 - Dec 2024)
        pre_monthly_avg = pre['total_prescriptions'] / 14
        short_monthly_avg = short['total_prescriptions'] / 34
        
        print(f"\nMonthly prescription average:")
        print(f"  Pre-shortage: {pre_monthly_avg:,.0f}")
        print(f"  During shortage: {short_monthly_avg:,.0f}")
        print(f"  Monthly growth rate: {(short_monthly_avg - pre_monthly_avg) / pre_monthly_avg * 100:.1f}%")
    
    df.to_csv(f"{output_dir}/ozempic_shortage_summary.csv", index=False)
    
    return df

if __name__ == "__main__":
    print("="*60)
    print("OZEMPIC SHORTAGE PERIOD ANALYSIS - COREWELL HEALTH")
    print(f"Shortage Period Defined as: {SHORTAGE_START_YEAR}-{SHORTAGE_START_MONTH:02d}-01 onwards")
    print("="*60)
    
    try:
        # Run analyses
        timeline_df = analyze_shortage_timeline()
        comparison_df = analyze_shortage_vs_normal()
        specialty_df = analyze_specialty_shortage_response()
        payor_df = analyze_payor_mix_during_shortage()
        summary_df = generate_shortage_summary()
        
        print("\n" + "="*60)
        print("SHORTAGE ANALYSIS COMPLETE")
        print("="*60)
        print("\nKey findings saved to data/ directory")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()