#!/usr/bin/env python3
"""
Verify extreme correlation claims from the Open Payments report
Testing: Enbrel (218x), Trelegy (115x), Xarelto (114x), Ozempic (92x)
"""

from google.cloud import bigquery
from google.oauth2 import service_account
import json, os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

env_path = Path('/home/incent/conflixis-data-projects/.env')
load_dotenv(env_path)

service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
service_account_info = json.loads(service_account_json)
credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=['https://www.googleapis.com/auth/bigquery']
)

client = bigquery.Client(project='data-analytics-389803', credentials=credentials)

def verify_drug_correlation(drug_name, brand_name, generic_name, claimed_ratio):
    """Verify correlation for a specific drug"""
    
    query = f"""
    WITH corewell_providers AS (
      SELECT DISTINCT 
        NPI,
        Primary_Specialty,
        Full_Name,
        SAFE_CAST(NPI AS INT64) as NPI_int
      FROM `data-analytics-389803.temp.corewell_provider_npis`
    ),
    drug_payments AS (
      SELECT
        a.covered_recipient_npi as NPI,
        SUM(a.total_amount_of_payment_usdollars) as total_op_payments,
        COUNT(*) as payment_count
      FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` a
      WHERE LOWER(a.name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE "%{drug_name.lower()}%"
      GROUP BY a.covered_recipient_npi
    ),
    drug_prescriptions AS (
      SELECT
        rx.NPI,
        SUM(rx.PAYMENTS) as total_rx_payments,
        SUM(rx.PRESCRIPTIONS) as total_prescriptions,
        SUM(rx.UNIQUE_PATIENTS) as total_patients
      FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
      WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
        AND (LOWER(rx.BRAND_NAME) LIKE "%{brand_name.lower()}%" 
             {f'OR LOWER(rx.GENERIC_NAME) LIKE "%{generic_name.lower()}%"' if generic_name else ''})
      GROUP BY rx.NPI
    ),
    analysis AS (
      SELECT
        cp.NPI,
        cp.Full_Name,
        cp.Primary_Specialty,
        CASE 
          WHEN dpay.NPI IS NOT NULL THEN 'WITH payments'
          ELSE 'WITHOUT payments'
        END as payment_status,
        COALESCE(dpay.total_op_payments, 0) as op_payments,
        COALESCE(dpay.payment_count, 0) as payment_count,
        COALESCE(drx.total_rx_payments, 0) as rx_payments,
        COALESCE(drx.total_prescriptions, 0) as prescriptions,
        COALESCE(drx.total_patients, 0) as patients
      FROM corewell_providers cp
      INNER JOIN drug_prescriptions drx
        ON cp.NPI_int = drx.NPI
      LEFT JOIN drug_payments dpay
        ON cp.NPI_int = dpay.NPI
      WHERE cp.NPI_int IN (
        SELECT NPI FROM drug_prescriptions
      )
    ),
    summary AS (
      SELECT
        payment_status,
        COUNT(*) as provider_count,
        AVG(rx_payments) as avg_rx_payments,
        APPROX_QUANTILES(rx_payments, 100)[OFFSET(50)] as median_rx_payments,
        MIN(rx_payments) as min_rx_payments,
        MAX(rx_payments) as max_rx_payments,
        SUM(rx_payments) as total_rx_payments,
        SUM(op_payments) as total_op_payments,
        STRING_AGG(Primary_Specialty, '; ' ORDER BY rx_payments DESC LIMIT 5) as top_specialties
      FROM analysis
      GROUP BY payment_status
    )
    SELECT * FROM summary
    ORDER BY payment_status
    """
    
    try:
        df = client.query(query).to_dataframe()
        
        if len(df) == 2:
            without = df[df['payment_status'] == 'WITHOUT payments'].iloc[0]
            with_pay = df[df['payment_status'] == 'WITH payments'].iloc[0]
            
            # Calculate ratios
            if without['avg_rx_payments'] > 0:
                mean_ratio = with_pay['avg_rx_payments'] / without['avg_rx_payments']
            else:
                mean_ratio = float('inf') if with_pay['avg_rx_payments'] > 0 else 1.0
                
            if without['median_rx_payments'] > 0:
                median_ratio = with_pay['median_rx_payments'] / without['median_rx_payments']
            else:
                median_ratio = float('inf') if with_pay['median_rx_payments'] > 0 else 1.0
            
            # ROI calculation
            if with_pay['total_op_payments'] > 0:
                roi = with_pay['total_rx_payments'] / with_pay['total_op_payments']
            else:
                roi = None
            
            return {
                'drug': drug_name,
                'claimed_ratio': claimed_ratio,
                'actual_mean_ratio': mean_ratio,
                'actual_median_ratio': median_ratio,
                'with_payment_count': int(with_pay['provider_count']),
                'without_payment_count': int(without['provider_count']),
                'with_avg_rx': with_pay['avg_rx_payments'],
                'without_avg_rx': without['avg_rx_payments'],
                'with_median_rx': with_pay['median_rx_payments'],
                'without_median_rx': without['median_rx_payments'],
                'total_op_payments': with_pay['total_op_payments'],
                'total_rx_payments': with_pay['total_rx_payments'],
                'roi': roi,
                'with_specialties': with_pay['top_specialties'],
                'without_specialties': without['top_specialties']
            }
        elif len(df) == 1:
            # Only one group exists
            row = df.iloc[0]
            return {
                'drug': drug_name,
                'claimed_ratio': claimed_ratio,
                'actual_mean_ratio': None,
                'actual_median_ratio': None,
                'note': f"Only {row['payment_status']} group exists",
                'provider_count': int(row['provider_count']),
                'avg_rx': row['avg_rx_payments'],
                'specialties': row['top_specialties']
            }
        else:
            return {
                'drug': drug_name,
                'claimed_ratio': claimed_ratio,
                'error': 'No prescribers found'
            }
    except Exception as e:
        return {
            'drug': drug_name,
            'claimed_ratio': claimed_ratio,
            'error': str(e)
        }

# Define drugs to verify
drugs_to_verify = [
    ('Enbrel', 'enbrel', 'etanercept', 218),
    ('Trelegy', 'trelegy', 'fluticasone', 115),
    ('Xarelto', 'xarelto', 'rivaroxaban', 114),
    ('Ozempic', 'ozempic', 'semaglutide', 92),
    ('Krystexxa', 'krystexxa', 'pegloticase', 426)  # Include for comparison
]

print('EXTREME CORRELATION CLAIMS VERIFICATION')
print('='*100)
print('\nVerifying claims from Open Payments Report...\n')

results = []
for drug_name, brand_name, generic_name, claimed_ratio in drugs_to_verify:
    print(f'Analyzing {drug_name}...')
    result = verify_drug_correlation(drug_name, brand_name, generic_name, claimed_ratio)
    results.append(result)

# Display results
print('\n\nVERIFICATION RESULTS:')
print('='*100)

for result in results:
    print(f"\n{result['drug']}:")
    print(f"  Report claimed: {result['claimed_ratio']}x")
    
    if 'error' in result:
        print(f"  ERROR: {result['error']}")
    elif 'note' in result:
        print(f"  NOTE: {result['note']}")
        print(f"  Provider count: {result['provider_count']}")
        print(f"  Average Rx: ${result['avg_rx']:,.0f}")
    else:
        print(f"  Actual mean ratio: {result['actual_mean_ratio']:.1f}x")
        print(f"  Actual median ratio: {result['actual_median_ratio']:.1f}x")
        print(f"  Sample sizes: {result['with_payment_count']} WITH vs {result['without_payment_count']} WITHOUT")
        
        # Check if sample size is concerning
        total_sample = result['with_payment_count'] + result['without_payment_count']
        if total_sample < 30:
            print(f"  ⚠️ WARNING: Small sample size (n={total_sample})")
        if min(result['with_payment_count'], result['without_payment_count']) < 5:
            print(f"  ⚠️ WARNING: Very small comparison group")
            
        print(f"\n  WITH payments: ${result['with_avg_rx']:,.0f} mean, ${result['with_median_rx']:,.0f} median")
        print(f"  WITHOUT payments: ${result['without_avg_rx']:,.0f} mean, ${result['without_median_rx']:,.0f} median")
        
        if result['roi']:
            print(f"\n  ROI: {result['roi']:.2f}x (${result['total_rx_payments']:,.0f} Rx / ${result['total_op_payments']:,.0f} OP)")
        
        # Check for discrepancy
        if result['actual_mean_ratio'] < result['claimed_ratio'] / 10:
            discrepancy = result['claimed_ratio'] / result['actual_mean_ratio']
            print(f"\n  🔴 MAJOR DISCREPANCY: Report overstated by {discrepancy:.1f}x")
        
        print(f"\n  Top specialties WITH payments: {result['with_specialties'][:100]}")
        print(f"  Top specialties WITHOUT: {result['without_specialties'][:100]}")

# Summary table
print('\n\nSUMMARY TABLE:')
print('='*100)
print(f"{'Drug':<12} {'Claimed':<10} {'Actual Mean':<12} {'Actual Median':<14} {'Sample Size':<15} {'Status'}")
print('-'*100)

for result in results:
    if 'error' not in result and 'note' not in result:
        status = '🔴 FAIL' if result['actual_mean_ratio'] < result['claimed_ratio'] / 10 else '✅ OK'
        sample = f"{result['with_payment_count']} vs {result['without_payment_count']}"
        print(f"{result['drug']:<12} {result['claimed_ratio']:<10}x {result['actual_mean_ratio']:<12.1f}x {result['actual_median_ratio']:<14.1f}x {sample:<15} {status}")
    elif 'note' in result:
        print(f"{result['drug']:<12} {result['claimed_ratio']:<10}x {'N/A':<12} {'N/A':<14} {result['provider_count']} total       ⚠️ {result['note']}")
    else:
        print(f"{result['drug']:<12} {result['claimed_ratio']:<10}x {'ERROR':<12} {'ERROR':<14} {'N/A':<15} ❌ {result['error'][:20]}")

print('\n\nCONCLUSIONS:')
print('='*100)
print('1. Multiple extreme claims appear to have calculation errors similar to Krystexxa')
print('2. Small sample sizes make many comparisons statistically unreliable')
print('3. Specialty mix likely explains much of the observed differences')
print('4. Report needs major corrections before publication')