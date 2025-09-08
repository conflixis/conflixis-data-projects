#!/usr/bin/env python3
"""
QC Verification for Corewell Health Open Payments Report
Part 3: Payment-Prescription Correlation Analysis
"""

import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
env_path = Path("/home/incent/conflixis-data-projects/.env")
if env_path.exists():
    load_dotenv(env_path)

# Create BigQuery client
service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY') or os.getenv('GCP_SERVICE_ACCOUNT')
if not service_account_json:
    print("No GCP credentials found")
    exit()

service_account_info = json.loads(service_account_json)
credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=['https://www.googleapis.com/auth/bigquery']
)

PROJECT_ID = "data-analytics-389803"
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

print("="*80)
print("PAYMENT-PRESCRIPTION CORRELATION ANALYSIS - QC VERIFICATION")
print(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

def analyze_drug_correlation(drug_name, display_name=None):
    """Analyze correlation for a specific drug"""
    if display_name is None:
        display_name = drug_name
    
    query = f"""
    WITH corewell_providers AS (
        SELECT DISTINCT NPI
        FROM `data-analytics-389803.temp.corewell_provider_npis`
    ),
    drug_payments AS (
        SELECT 
            CAST(op.covered_recipient_npi AS STRING) AS NPI,
            SUM(op.total_amount_of_payment_usdollars) as total_op_payments,
            COUNT(*) as payment_count
        FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` op
        WHERE op.program_year BETWEEN 2020 AND 2024
            AND op.name_of_drug_or_biological_or_device_or_medical_supply_1 IS NOT NULL
            AND UPPER(op.name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE UPPER('%{drug_name}%')
        GROUP BY NPI
    ),
    drug_prescriptions AS (
        SELECT 
            CAST(rx.NPI AS STRING) AS NPI,
            SUM(rx.PAYMENTS) as total_rx_payments,
            SUM(rx.PRESCRIPTIONS) as total_prescriptions
        FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
            AND (UPPER(rx.BRAND_NAME) LIKE UPPER('%{drug_name}%')
                 OR UPPER(rx.GENERIC_NAME) LIKE UPPER('%{drug_name}%'))
        GROUP BY NPI
    ),
    provider_summary AS (
        SELECT 
            cp.NPI,
            COALESCE(dp.total_op_payments, 0) as op_payments,
            COALESCE(drx.total_rx_payments, 0) as rx_payments,
            COALESCE(drx.total_prescriptions, 0) as prescriptions,
            CASE WHEN dp.NPI IS NOT NULL THEN 1 ELSE 0 END as has_payment
        FROM corewell_providers cp
        LEFT JOIN drug_payments dp ON cp.NPI = dp.NPI
        LEFT JOIN drug_prescriptions drx ON cp.NPI = drx.NPI
        WHERE drx.NPI IS NOT NULL  -- Only providers who prescribed the drug
    )
    SELECT 
        has_payment,
        COUNT(*) as provider_count,
        AVG(rx_payments) as avg_rx_payments,
        AVG(prescriptions) as avg_prescriptions,
        AVG(CASE WHEN has_payment = 1 THEN op_payments END) as avg_op_payment,
        SUM(rx_payments) as total_rx_payments,
        SUM(op_payments) as total_op_payments
    FROM provider_summary
    GROUP BY has_payment
    ORDER BY has_payment
    """
    
    results = client.query(query).to_dataframe()
    
    if len(results) >= 2:
        no_payment = results[results['has_payment'] == 0].iloc[0] if len(results[results['has_payment'] == 0]) > 0 else None
        with_payment = results[results['has_payment'] == 1].iloc[0] if len(results[results['has_payment'] == 1]) > 0 else None
        
        if no_payment is not None and with_payment is not None:
            print(f"\n{display_name}:")
            print(f"  Providers WITH payments: ${with_payment['avg_rx_payments']:,.0f} avg ({with_payment['provider_count']} providers)")
            print(f"  Providers WITHOUT payments: ${no_payment['avg_rx_payments']:,.0f} avg ({no_payment['provider_count']} providers)")
            
            if no_payment['avg_rx_payments'] > 0:
                ratio = with_payment['avg_rx_payments'] / no_payment['avg_rx_payments']
                print(f"  Influence Factor: {ratio:.1f}x")
            else:
                print(f"  Influence Factor: N/A (no prescriptions without payments)")
            
            # Calculate ROI
            if with_payment['avg_op_payment'] and with_payment['avg_op_payment'] > 0:
                additional_rx = with_payment['avg_rx_payments'] - no_payment['avg_rx_payments']
                roi = additional_rx / with_payment['avg_op_payment']
                print(f"  Avg payment: ${with_payment['avg_op_payment']:,.2f}")
                print(f"  ROI: ${roi:.0f} per dollar")
            
            return {
                'drug': display_name,
                'with_payment_avg': with_payment['avg_rx_payments'],
                'without_payment_avg': no_payment['avg_rx_payments'],
                'ratio': ratio if no_payment['avg_rx_payments'] > 0 else None,
                'roi': roi if with_payment['avg_op_payment'] and with_payment['avg_op_payment'] > 0 else None
            }
    
    return None

# Analyze key drugs from the report
print("\n1. EXTREME INFLUENCE CASES VERIFICATION")
print("-"*60)

key_drugs = [
    ('Krystexxa', 'Krystexxa'),
    ('Enbrel', 'Enbrel'),
    ('Trelegy', 'Trelegy'),
    ('Xarelto', 'Xarelto'),
    ('Ozempic', 'Ozempic'),
    ('Humira', 'Humira'),
    ('Farxiga', 'Farxiga'),
    ('Jardiance', 'Jardiance'),
    ('Mounjaro', 'Mounjaro'),
    ('Entresto', 'Entresto')
]

results_list = []
for drug_search, drug_display in key_drugs:
    try:
        result = analyze_drug_correlation(drug_search, drug_display)
        if result:
            results_list.append(result)
    except Exception as e:
        print(f"\nError analyzing {drug_display}: {e}")

# Compare with report claims
print("\n2. REPORT CLAIMS COMPARISON")
print("-"*60)

report_claims = {
    'Krystexxa': {'ratio': 426, 'roi': 4},
    'Enbrel': {'ratio': 218, 'roi': 99},
    'Trelegy': {'ratio': 115, 'roi': 22},
    'Xarelto': {'ratio': 114, 'roi': 7},
    'Ozempic': {'ratio': 92, 'roi': 25}
}

print("\nDrug | Report Ratio | Actual Ratio | Report ROI | Actual ROI")
print("-" * 65)
for result in results_list:
    if result['drug'] in report_claims:
        report = report_claims[result['drug']]
        actual_ratio = f"{result['ratio']:.0f}x" if result['ratio'] else "N/A"
        actual_roi = f"${result['roi']:.0f}" if result['roi'] else "N/A"
        print(f"{result['drug']:12} | {report['ratio']:>12}x | {actual_ratio:>12} | ${report['roi']:>10} | {actual_roi:>10}")

# Payment tier analysis
print("\n3. PAYMENT TIER ANALYSIS")
print("-"*60)

tier_query = """
WITH provider_payments AS (
    SELECT 
        CAST(op.covered_recipient_npi AS STRING) AS NPI,
        SUM(op.total_amount_of_payment_usdollars) as total_payments
    FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` op
    WHERE op.program_year BETWEEN 2020 AND 2024
        AND CAST(op.covered_recipient_npi AS STRING) IN (
            SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`
        )
    GROUP BY NPI
),
provider_prescriptions AS (
    SELECT 
        CAST(rx.NPI AS STRING) AS NPI,
        SUM(rx.PAYMENTS) as total_rx_payments
    FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
    WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
        AND CAST(rx.NPI AS STRING) IN (
            SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`
        )
    GROUP BY NPI
),
provider_summary AS (
    SELECT 
        cs.NPI,
        COALESCE(pp.total_payments, 0) as op_payments,
        COALESCE(prx.total_rx_payments, 0) as rx_payments,
        CASE 
            WHEN pp.total_payments IS NULL OR pp.total_payments = 0 THEN 'No Payment'
            WHEN pp.total_payments <= 100 THEN '$1-100'
            WHEN pp.total_payments <= 500 THEN '$101-500'
            WHEN pp.total_payments <= 1000 THEN '$501-1,000'
            WHEN pp.total_payments <= 5000 THEN '$1,001-5,000'
            ELSE '$5,000+'
        END as payment_tier
    FROM `data-analytics-389803.temp.corewell_provider_npis` cs
    LEFT JOIN provider_payments pp ON cs.NPI = pp.NPI
    LEFT JOIN provider_prescriptions prx ON cs.NPI = prx.NPI
    WHERE prx.total_rx_payments > 0  -- Only providers with prescriptions
)
SELECT 
    payment_tier,
    COUNT(*) as provider_count,
    AVG(rx_payments) as avg_prescriptions,
    AVG(CASE WHEN op_payments > 0 THEN op_payments END) as avg_payment
FROM provider_summary
GROUP BY payment_tier
ORDER BY 
    CASE payment_tier
        WHEN 'No Payment' THEN 0
        WHEN '$1-100' THEN 1
        WHEN '$101-500' THEN 2
        WHEN '$501-1,000' THEN 3
        WHEN '$1,001-5,000' THEN 4
        WHEN '$5,000+' THEN 5
    END
"""

tier_results = client.query(tier_query).to_dataframe()

print("\nPayment Tier Analysis:")
print("Tier | Providers | Avg Prescriptions | Avg Payment | ROI")
print("-" * 70)

baseline = None
for _, row in tier_results.iterrows():
    if row['payment_tier'] == 'No Payment':
        baseline = row['avg_prescriptions']
        print(f"{row['payment_tier']:13} | {row['provider_count']:>9,} | ${row['avg_prescriptions']:>16,.0f} | N/A | Baseline")
    else:
        if baseline and row['avg_payment'] and row['avg_payment'] > 0:
            additional = row['avg_prescriptions'] - baseline
            roi = additional / row['avg_payment']
            print(f"{row['payment_tier']:13} | {row['provider_count']:>9,} | ${row['avg_prescriptions']:>16,.0f} | ${row['avg_payment']:>11,.0f} | {roi:>8,.0f}x")
        else:
            print(f"{row['payment_tier']:13} | {row['provider_count']:>9,} | ${row['avg_prescriptions']:>16,.0f} | ${row['avg_payment']:>11,.0f} | N/A")

# Consecutive year analysis
print("\n4. CONSECUTIVE YEAR PAYMENT PATTERNS")
print("-"*60)

consecutive_query = """
WITH provider_payment_years AS (
    SELECT 
        CAST(op.covered_recipient_npi AS STRING) AS NPI,
        COUNT(DISTINCT op.program_year) as payment_years,
        STRING_AGG(DISTINCT CAST(op.program_year AS STRING), ',' ORDER BY CAST(op.program_year AS STRING)) as years_list,
        SUM(op.total_amount_of_payment_usdollars) as total_payments
    FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` op
    WHERE op.program_year BETWEEN 2020 AND 2024
        AND CAST(op.covered_recipient_npi AS STRING) IN (
            SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`
        )
    GROUP BY NPI
),
provider_prescriptions AS (
    SELECT 
        CAST(rx.NPI AS STRING) AS NPI,
        SUM(rx.PAYMENTS) as total_rx_payments
    FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
    WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
        AND CAST(rx.NPI AS STRING) IN (
            SELECT NPI FROM `data-analytics-389803.temp.corewell_provider_npis`
        )
    GROUP BY NPI
),
consecutive_analysis AS (
    SELECT 
        cs.NPI,
        COALESCE(ppy.payment_years, 0) as years_with_payments,
        CASE 
            WHEN ppy.years_list = '2020,2021,2022,2023,2024' THEN 5
            WHEN ppy.payment_years = 4 THEN 4
            WHEN ppy.payment_years = 3 THEN 3
            WHEN ppy.payment_years = 2 THEN 2
            WHEN ppy.payment_years = 1 THEN 1
            ELSE 0
        END as consecutive_years,
        COALESCE(prx.total_rx_payments, 0) as rx_payments
    FROM `data-analytics-389803.temp.corewell_provider_npis` cs
    LEFT JOIN provider_payment_years ppy ON cs.NPI = ppy.NPI
    LEFT JOIN provider_prescriptions prx ON cs.NPI = prx.NPI
    WHERE prx.total_rx_payments > 0  -- Only providers with prescriptions
)
SELECT 
    consecutive_years,
    COUNT(*) as provider_count,
    AVG(rx_payments) as avg_prescriptions
FROM consecutive_analysis
GROUP BY consecutive_years
ORDER BY consecutive_years DESC
"""

consecutive_results = client.query(consecutive_query).to_dataframe()

print("\nConsecutive Year Payment Analysis:")
print("Years | Providers | Avg Prescriptions | Multiple vs Baseline")
print("-" * 65)

baseline = None
for _, row in consecutive_results.iterrows():
    if row['consecutive_years'] == 0:
        baseline = row['avg_prescriptions']
        print(f"{row['consecutive_years']} years | {row['provider_count']:>9,} | ${row['avg_prescriptions']:>16,.0f} | Baseline (1.00x)")
    else:
        if baseline:
            multiple = row['avg_prescriptions'] / baseline
            print(f"{row['consecutive_years']} years | {row['provider_count']:>9,} | ${row['avg_prescriptions']:>16,.0f} | {multiple:.2f}x")
        else:
            print(f"{row['consecutive_years']} years | {row['provider_count']:>9,} | ${row['avg_prescriptions']:>16,.0f} | N/A")

print("\n" + "="*80)
print("CORRELATION ANALYSIS VERIFICATION COMPLETE")
print("="*80)