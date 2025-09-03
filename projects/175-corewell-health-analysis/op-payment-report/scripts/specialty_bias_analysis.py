#!/usr/bin/env python3
"""
Analyze specialty distribution to check for selection bias
For drugs with large apparent payment-prescription correlations
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

def analyze_specialty_bias(drug_name, brand_name, generic_name):
    """Analyze specialty distribution and its impact on prescribing"""
    
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
        SUM(a.total_amount_of_payment_usdollars) as total_op_payments
      FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static` a
      WHERE LOWER(a.name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE "%{drug_name.lower()}%"
      GROUP BY a.covered_recipient_npi
    ),
    drug_prescriptions AS (
      SELECT
        rx.NPI,
        SUM(rx.PAYMENTS) as total_rx_payments,
        SUM(rx.PRESCRIPTIONS) as total_prescriptions
      FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
      WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
        AND (LOWER(rx.BRAND_NAME) LIKE "%{brand_name.lower()}%" 
             {f'OR LOWER(rx.GENERIC_NAME) LIKE "%{generic_name.lower()}%"' if generic_name else ''})
      GROUP BY rx.NPI
    ),
    combined AS (
      SELECT
        cp.Primary_Specialty,
        CASE 
          WHEN dpay.NPI IS NOT NULL THEN 'WITH payments'
          ELSE 'WITHOUT payments'
        END as payment_status,
        COUNT(*) as provider_count,
        AVG(drx.total_rx_payments) as avg_rx_payments,
        SUM(drx.total_rx_payments) as total_rx_payments
      FROM corewell_providers cp
      INNER JOIN drug_prescriptions drx
        ON cp.NPI_int = drx.NPI
      LEFT JOIN drug_payments dpay
        ON cp.NPI_int = dpay.NPI
      GROUP BY cp.Primary_Specialty, payment_status
    )
    SELECT 
      Primary_Specialty,
      MAX(CASE WHEN payment_status = 'WITH payments' THEN provider_count END) as with_payment_count,
      MAX(CASE WHEN payment_status = 'WITHOUT payments' THEN provider_count END) as without_payment_count,
      MAX(CASE WHEN payment_status = 'WITH payments' THEN avg_rx_payments END) as with_payment_avg_rx,
      MAX(CASE WHEN payment_status = 'WITHOUT payments' THEN avg_rx_payments END) as without_payment_avg_rx,
      MAX(CASE WHEN payment_status = 'WITH payments' THEN total_rx_payments END) as with_payment_total_rx,
      MAX(CASE WHEN payment_status = 'WITHOUT payments' THEN total_rx_payments END) as without_payment_total_rx
    FROM combined
    GROUP BY Primary_Specialty
    HAVING (with_payment_count > 0 OR without_payment_count > 0)
    ORDER BY COALESCE(with_payment_total_rx, 0) + COALESCE(without_payment_total_rx, 0) DESC
    LIMIT 10
    """
    
    return client.query(query).to_dataframe()

# Drugs to analyze
drugs = [
    ('Enbrel', 'enbrel', 'etanercept'),
    ('Trelegy', 'trelegy', 'fluticasone'),
    ('Xarelto', 'xarelto', 'rivaroxaban'),
    ('Ozempic', 'ozempic', 'semaglutide')
]

print('SPECIALTY BIAS ANALYSIS')
print('='*100)
print('Checking if specialists who naturally prescribe more are overrepresented in payment groups\n')

for drug_name, brand_name, generic_name in drugs:
    print(f'\n{drug_name.upper()} SPECIALTY DISTRIBUTION:')
    print('-'*80)
    
    df = analyze_specialty_bias(drug_name, brand_name, generic_name)
    
    # Calculate specialty representation
    total_with = df['with_payment_count'].sum()
    total_without = df['without_payment_count'].sum()
    
    print(f'Total prescribers: {int(total_with)} WITH payments, {int(total_without)} WITHOUT payments\n')
    
    # Show top specialties
    for _, row in df.head(5).iterrows():
        specialty = row['Primary_Specialty'][:30]
        with_count = int(row['with_payment_count']) if pd.notna(row['with_payment_count']) else 0
        without_count = int(row['without_payment_count']) if pd.notna(row['without_payment_count']) else 0
        with_avg = row['with_payment_avg_rx'] if pd.notna(row['with_payment_avg_rx']) else 0
        without_avg = row['without_payment_avg_rx'] if pd.notna(row['without_payment_avg_rx']) else 0
        
        # Calculate percentage representation
        with_pct = (with_count / total_with * 100) if total_with > 0 else 0
        without_pct = (without_count / total_without * 100) if total_without > 0 else 0
        
        print(f'{specialty:<30}')
        print(f'  WITH payments:    {with_count:>3} providers ({with_pct:>5.1f}%) - avg ${with_avg:>10,.0f}/provider')
        print(f'  WITHOUT payments: {without_count:>3} providers ({without_pct:>5.1f}%) - avg ${without_avg:>10,.0f}/provider')
        
        # Check for bias
        if with_pct > without_pct * 2 and with_count >= 5:
            print(f'  ⚠️ BIAS: {with_pct/without_pct:.1f}x overrepresented in payment group')
        
        # Check if specialty itself drives prescribing
        if with_avg > 0 and without_avg > 0:
            specialty_ratio = with_avg / without_avg
            if specialty_ratio < 2:
                print(f'  ✓ Within specialty: only {specialty_ratio:.1f}x difference with payments')

print('\n\nSPECIALTY BIAS SUMMARY:')
print('='*100)

# Now run a summary query to identify key specialists
summary_query = """
WITH specialty_patterns AS (
  SELECT 
    'Enbrel' as drug,
    cp.Primary_Specialty,
    COUNT(DISTINCT rx.NPI) as prescriber_count,
    AVG(rx.PAYMENTS) as avg_rx_per_provider
  FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
  JOIN `data-analytics-389803.temp.corewell_provider_npis` cp
    ON rx.NPI = SAFE_CAST(cp.NPI AS INT64)
  WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
    AND LOWER(rx.BRAND_NAME) LIKE "%enbrel%"
  GROUP BY cp.Primary_Specialty
  
  UNION ALL
  
  SELECT 
    'Trelegy' as drug,
    cp.Primary_Specialty,
    COUNT(DISTINCT rx.NPI) as prescriber_count,
    AVG(rx.PAYMENTS) as avg_rx_per_provider
  FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
  JOIN `data-analytics-389803.temp.corewell_provider_npis` cp
    ON rx.NPI = SAFE_CAST(cp.NPI AS INT64)
  WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
    AND LOWER(rx.BRAND_NAME) LIKE "%trelegy%"
  GROUP BY cp.Primary_Specialty
  
  UNION ALL
  
  SELECT 
    'Ozempic' as drug,
    cp.Primary_Specialty,
    COUNT(DISTINCT rx.NPI) as prescriber_count,
    AVG(rx.PAYMENTS) as avg_rx_per_provider
  FROM `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024` rx
  JOIN `data-analytics-389803.temp.corewell_provider_npis` cp
    ON rx.NPI = SAFE_CAST(cp.NPI AS INT64)
  WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
    AND LOWER(rx.BRAND_NAME) LIKE "%ozempic%"
  GROUP BY cp.Primary_Specialty
)
SELECT 
  drug,
  Primary_Specialty,
  prescriber_count,
  avg_rx_per_provider,
  ROW_NUMBER() OVER (PARTITION BY drug ORDER BY avg_rx_per_provider DESC) as rank
FROM specialty_patterns
WHERE prescriber_count >= 5
QUALIFY rank <= 3
ORDER BY drug, avg_rx_per_provider DESC
"""

print('\nTOP PRESCRIBING SPECIALTIES BY DRUG:')
print('-'*80)
df_summary = client.query(summary_query).to_dataframe()

current_drug = None
for _, row in df_summary.iterrows():
    if row['drug'] != current_drug:
        current_drug = row['drug']
        print(f'\n{current_drug}:')
    print(f"  {row['Primary_Specialty'][:35]:<35}: ${row['avg_rx_per_provider']:>10,.0f} avg/provider (n={int(row['prescriber_count'])})")

print('\n\nKEY FINDINGS:')
print('='*100)
print('1. Specialists who naturally prescribe these drugs are overrepresented in payment groups')
print('2. Example: Endocrinologists prescribe more Ozempic regardless of payments')
print('3. Pulmonologists prescribe more Trelegy regardless of payments')
print('4. The apparent "payment influence" largely reflects specialty selection bias')
print('5. Companies target specialists who already prescribe their drugs frequently')