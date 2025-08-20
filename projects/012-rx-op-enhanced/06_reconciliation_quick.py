#!/usr/bin/env python3
"""
Quick reconciliation using expected counts
JIRA: DA-167
"""

from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv('../../.env')

PROJECT_ID = "data-analytics-389803"
DATASET_ID = "conflixis_agent"
TABLE_ID = "rx_op_enhanced_full"
LOCAL_DATA_DIR = Path("mfg-spec-data")

# Expected counts per manufacturer (15 files each, standard row counts per specialty)
EXPECTED_ROWS_PER_SPECIALTY = {
    'Cardiology': 681093,
    'Dermatology': 309460,
    'Endocrinology': 213412,
    'Gastroenterology': 375195,
    'Internal.Medicine': 2149694,
    'NP': 4923188,
    'Nephrology': 220776,
    'Neurology': 394963,
    'Oncology': 478076,
    'PA': 2371439,
    'Primary.Care': 2559163,
    'Psychiatry': 869928,
    'Pulmonary': 251025,
    'Surgery': 1545151,
    'Urology': 233654
}

EXPECTED_MANUFACTURERS = [
    'abbvie', 'allergan', 'astrazeneca', 'boehringer', 
    'celgene', 'eli_lilly', 'gilead', 'gsk',
    'janssen_biotech', 'janssen_pharma', 'merck', 'novartis',
    'novo_nordisk', 'sanofi', 'squibb', 'takeda'
]

def get_bigquery_client():
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("No service account key found in environment")
    
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)

def quick_reconcile():
    print("=" * 70)
    print("RX-OP Enhanced: Quick Reconciliation")
    print("=" * 70)
    
    # Calculate expected totals
    total_expected_rows = sum(EXPECTED_ROWS_PER_SPECIALTY.values()) * len(EXPECTED_MANUFACTURERS)
    expected_files = len(EXPECTED_ROWS_PER_SPECIALTY) * len(EXPECTED_MANUFACTURERS)
    
    print(f"\nExpected:")
    print(f"  Files: {expected_files}")
    print(f"  Rows: {total_expected_rows:,}")
    print(f"  Manufacturers: {len(EXPECTED_MANUFACTURERS)}")
    
    # Get BigQuery stats
    client = get_bigquery_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Overall stats
    query = f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT source_file) as total_files,
        COUNT(DISTINCT source_manufacturer) as total_manufacturers
    FROM `{table_ref}`
    """
    
    print(f"\nBigQuery Actual:")
    result = client.query(query).result()
    for row in result:
        print(f"  Files: {row.total_files}")
        print(f"  Rows: {row.total_rows:,}")
        print(f"  Manufacturers: {row.total_manufacturers}")
        
        bq_rows = row.total_rows
        bq_files = row.total_files
        bq_manufacturers = row.total_manufacturers
    
    # Check local files
    rds_files = list(LOCAL_DATA_DIR.glob("*.rds"))
    print(f"\nLocal RDS files: {len(rds_files)}")
    
    # Manufacturer breakdown
    query2 = f"""
    SELECT 
        source_manufacturer,
        COUNT(DISTINCT source_file) as files,
        COUNT(*) as row_count
    FROM `{table_ref}`
    GROUP BY source_manufacturer
    ORDER BY source_manufacturer
    """
    
    print("\n" + "-" * 70)
    print("Manufacturer Breakdown:")
    print(f"{'Manufacturer':<20} {'Files':>10} {'Rows':>15} {'Expected Rows':>15} {'Match':>8}")
    print("-" * 70)
    
    result = client.query(query2).result()
    bq_manufacturers_list = []
    total_match = True
    
    for row in result:
        bq_manufacturers_list.append(row.source_manufacturer)
        expected_rows = sum(EXPECTED_ROWS_PER_SPECIALTY.values())
        match = '✓' if row.row_count == expected_rows else '✗'
        if row.row_count != expected_rows:
            total_match = False
        print(f"{row.source_manufacturer:<20} {row.files:>10} {row.row_count:>15,} {expected_rows:>15,} {match:>8}")
    
    # Check for missing manufacturers
    missing = set(EXPECTED_MANUFACTURERS) - set(bq_manufacturers_list)
    extra = set(bq_manufacturers_list) - set(EXPECTED_MANUFACTURERS)
    
    if missing:
        print(f"\n✗ Missing manufacturers: {missing}")
    if extra:
        print(f"\n✗ Extra/unexpected manufacturers: {extra}")
    
    # Column verification
    query3 = f"""
    SELECT column_name
    FROM `{PROJECT_ID}.{DATASET_ID}`.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{TABLE_ID}'
      AND column_name LIKE 'mfg_avg_%'
    ORDER BY column_name
    """
    
    print("\n" + "-" * 70)
    print("Column Standardization Check:")
    
    result = client.query(query3).result()
    mfg_cols = [row.column_name for row in result]
    
    expected_mfg_cols = [
        'mfg_avg_lag3', 'mfg_avg_lag6', 'mfg_avg_lag9', 'mfg_avg_lag12',
        'mfg_avg_lead3', 'mfg_avg_lead6', 'mfg_avg_lead9', 'mfg_avg_lead12'
    ]
    
    print(f"Standardized columns found: {len(mfg_cols)}")
    print(f"Expected: {len(expected_mfg_cols)}")
    
    if set(mfg_cols) == set(expected_mfg_cols):
        print("✓ All expected standardized columns present")
    else:
        missing_cols = set(expected_mfg_cols) - set(mfg_cols)
        extra_cols = set(mfg_cols) - set(expected_mfg_cols)
        if missing_cols:
            print(f"✗ Missing columns: {missing_cols}")
        if extra_cols:
            print(f"✗ Extra columns: {extra_cols}")
    
    # Check for non-standardized manufacturer columns
    query4 = f"""
    SELECT column_name
    FROM `{PROJECT_ID}.{DATASET_ID}`.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{TABLE_ID}'
      AND (column_name LIKE '%_avg_lag%' OR column_name LIKE '%_avg_lead%')
      AND NOT column_name LIKE 'mfg_avg_%'
    """
    
    result = client.query(query4).result()
    non_standard = [row.column_name for row in result]
    
    if non_standard:
        print(f"\n✗ Found {len(non_standard)} non-standardized manufacturer columns:")
        for col in non_standard[:5]:
            print(f"  - {col}")
    else:
        print("✓ No non-standardized manufacturer columns found")
    
    # Final Summary
    print("\n" + "=" * 70)
    print("RECONCILIATION SUMMARY")
    print("=" * 70)
    
    issues = []
    
    # File count check
    if bq_files == expected_files:
        print(f"✓ File count matches: {bq_files}")
    else:
        print(f"✗ File count mismatch: Expected {expected_files}, Got {bq_files}")
        issues.append("file_count")
    
    # Row count check
    if bq_rows == total_expected_rows:
        print(f"✓ Row count matches: {bq_rows:,}")
    else:
        diff = bq_rows - total_expected_rows
        print(f"✗ Row count mismatch: Expected {total_expected_rows:,}, Got {bq_rows:,} (Diff: {diff:+,})")
        issues.append("row_count")
    
    # Manufacturer check
    if not missing and not extra:
        print(f"✓ All manufacturers present: {bq_manufacturers}")
    else:
        issues.append("manufacturers")
    
    # Column standardization
    if not non_standard and set(mfg_cols) == set(expected_mfg_cols):
        print(f"✓ Column standardization complete")
    else:
        issues.append("columns")
    
    if not issues:
        print("\n" + "=" * 70)
        print("✅ PERFECT RECONCILIATION!")
        print("=" * 70)
        print("All data successfully transferred from RDS to BigQuery")
    else:
        print(f"\n⚠ Issues found in: {', '.join(issues)}")
    
    return len(issues) == 0

if __name__ == "__main__":
    quick_reconcile()