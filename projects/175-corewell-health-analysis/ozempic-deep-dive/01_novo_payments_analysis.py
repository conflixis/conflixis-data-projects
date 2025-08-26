#!/usr/bin/env python3
"""
Analyze Novo Nordisk payments to Corewell Health providers
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

# Define tables (from data dictionary)
OP_TABLE = "data-analytics-389803.conflixis_agent.op_general_all_aggregate_static"
NPI_TABLE = "data-analytics-389803.conflixis_agent.corewell_health_npis"
RX_TABLE = "data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024"

# Create output directory
output_dir = '/home/incent/conflixis-data-projects/projects/175-corewell-health-analysis/ozempic-deep-dive/data'
os.makedirs(output_dir, exist_ok=True)

def analyze_novo_payments_to_corewell():
    """Analyze all Novo Nordisk payments to Corewell providers"""
    
    print("Analyzing Novo Nordisk payments to Corewell Health providers...")
    
    query = f"""
    WITH novo_payments AS (
        SELECT 
            op.covered_recipient_npi AS npi,
            op.program_year,
            op.date_of_payment,
            op.applicable_manufacturer_or_applicable_gpo_making_payment_name AS manufacturer,
            op.nature_of_payment_or_transfer_of_value AS payment_type,
            op.name_of_drug_or_biological_or_device_or_medical_supply_1 AS product_name,
            op.total_amount_of_payment_usdollars AS payment_amount,
            cs.Full_Name AS provider_name,
            cs.Primary_Specialty,
            cs.Primary_Hospital_Affiliation
        FROM `{OP_TABLE}` op
        INNER JOIN `{NPI_TABLE}` cs
            ON CAST(op.covered_recipient_npi AS STRING) = cs.NPI
        WHERE UPPER(op.applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%NOVO%NORDISK%'
            AND op.program_year BETWEEN 2020 AND 2024
    )
    SELECT 
        npi,
        provider_name,
        Primary_Specialty,
        Primary_Hospital_Affiliation,
        program_year,
        payment_type,
        CASE
            WHEN UPPER(product_name) LIKE '%OZEMPIC%' THEN 'Ozempic'
            WHEN UPPER(product_name) LIKE '%WEGOVY%' THEN 'Wegovy'
            WHEN UPPER(product_name) LIKE '%VICTOZA%' THEN 'Victoza'
            WHEN UPPER(product_name) LIKE '%RYBELSUS%' THEN 'Rybelsus'
            WHEN product_name IS NULL THEN 'Not Specified'
            ELSE 'Other Novo Products'
        END AS product_category,
        COUNT(*) as transaction_count,
        SUM(payment_amount) as total_payment,
        AVG(payment_amount) as avg_payment
    FROM novo_payments
    GROUP BY 1,2,3,4,5,6,7
    ORDER BY total_payment DESC
    """
    
    df = client.query(query).to_dataframe()
    
    # Save detailed data
    df.to_csv(f"{output_dir}/novo_payments_detailed.csv", index=False)
    
    # Summary statistics
    print("\n=== NOVO NORDISK PAYMENT SUMMARY TO COREWELL ===")
    print(f"Total unique providers receiving payments: {df['npi'].nunique():,}")
    print(f"Total payment amount: ${df['total_payment'].sum():,.2f}")
    print(f"Total transactions: {df['transaction_count'].sum():,}")
    print(f"Average payment per transaction: ${df['total_payment'].sum()/df['transaction_count'].sum():.2f}")
    
    # By year
    yearly = df.groupby('program_year').agg({
        'npi': 'nunique',
        'total_payment': 'sum',
        'transaction_count': 'sum'
    }).reset_index()
    print("\n=== YEARLY NOVO PAYMENTS ===")
    print(yearly)
    yearly.to_csv(f"{output_dir}/novo_payments_yearly.csv", index=False)
    
    # By product
    product_summary = df.groupby('product_category').agg({
        'npi': 'nunique',
        'total_payment': 'sum',
        'transaction_count': 'sum'
    }).sort_values('total_payment', ascending=False)
    print("\n=== PAYMENTS BY PRODUCT ===")
    print(product_summary)
    product_summary.to_csv(f"{output_dir}/novo_payments_by_product.csv", index=False)
    
    # By payment type
    payment_type = df.groupby('payment_type').agg({
        'npi': 'nunique',
        'total_payment': 'sum',
        'transaction_count': 'sum'
    }).sort_values('total_payment', ascending=False).head(10)
    print("\n=== TOP PAYMENT TYPES ===")
    print(payment_type)
    payment_type.to_csv(f"{output_dir}/novo_payments_by_type.csv", index=False)
    
    # Top specialties
    specialty = df.groupby('Primary_Specialty').agg({
        'npi': 'nunique',
        'total_payment': 'sum',
        'transaction_count': 'sum'
    }).sort_values('total_payment', ascending=False).head(10)
    print("\n=== TOP SPECIALTIES RECEIVING PAYMENTS ===")
    print(specialty)
    specialty.to_csv(f"{output_dir}/novo_payments_by_specialty.csv", index=False)
    
    # Ozempic-specific analysis
    ozempic_payments = df[df['product_category'] == 'Ozempic']
    if not ozempic_payments.empty:
        print(f"\n=== OZEMPIC-SPECIFIC PAYMENTS ===")
        print(f"Providers receiving Ozempic-related payments: {ozempic_payments['npi'].nunique():,}")
        print(f"Total Ozempic-related payments: ${ozempic_payments['total_payment'].sum():,.2f}")
        print(f"Percentage of total Novo payments: {ozempic_payments['total_payment'].sum() / df['total_payment'].sum() * 100:.1f}%")
    
    return df

if __name__ == "__main__":
    print("="*60)
    print("NOVO NORDISK PAYMENT ANALYSIS - COREWELL HEALTH")
    print("="*60)
    
    try:
        payments_df = analyze_novo_payments_to_corewell()
        
        print("\n" + "="*60)
        print("ANALYSIS COMPLETE - Files saved to ozempic-deep-dive/data/")
        print("="*60)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()