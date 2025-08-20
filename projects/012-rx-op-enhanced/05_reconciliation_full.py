#!/usr/bin/env python3
"""
Reconciliation script to verify RDS to BigQuery transfer
JIRA: DA-167

This script:
1. Counts rows in each RDS file
2. Compares with BigQuery counts
3. Verifies column presence and standardization
4. Reports any discrepancies
"""

import pyreadr
import pandas as pd
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from dotenv import load_dotenv
from collections import defaultdict
import re

# Load environment variables
load_dotenv('../../.env')

LOCAL_DATA_DIR = Path("mfg-spec-data")
PROJECT_ID = "data-analytics-389803"
DATASET_ID = "conflixis_agent"
TABLE_ID = "rx_op_enhanced_full"

def get_bigquery_client():
    """Create BigQuery client with service account."""
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("No service account key found in environment")
    
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)

def extract_manufacturer_from_filename(filename):
    """Extract manufacturer name from filename."""
    parts = filename.replace('df_spec_', '').replace('.rds', '').split('_')
    manufacturer = parts[0] if parts else 'unknown'
    
    # Handle compound manufacturer names
    if len(parts) >= 3:
        if parts[1] in ['biotech', 'lilly', 'myers', 'squibb']:
            manufacturer = f"{parts[0]}_{parts[1]}"
            if parts[1] == 'myers' and len(parts) > 2 and parts[2] == 'squibb':
                manufacturer = f"{parts[0]}_{parts[1]}_{parts[2]}"
    
    return manufacturer

def count_rds_files():
    """Count rows in each RDS file and aggregate by manufacturer."""
    print("=" * 70)
    print("Counting rows in RDS files...")
    print("-" * 70)
    
    rds_files = sorted(LOCAL_DATA_DIR.glob("*.rds"))
    
    file_counts = {}
    manufacturer_totals = defaultdict(lambda: {'files': 0, 'rows': 0})
    total_rows_rds = 0
    columns_by_file = {}
    
    for i, file_path in enumerate(rds_files, 1):
        print(f"[{i:3d}/{len(rds_files)}] {file_path.name}...", end=' ')
        
        try:
            # Read RDS file
            result = pyreadr.read_r(str(file_path))
            df = result[None]
            
            rows = len(df)
            manufacturer = extract_manufacturer_from_filename(file_path.name)
            
            # Store counts
            file_counts[file_path.name] = {
                'rows': rows,
                'manufacturer': manufacturer
            }
            
            # Store column info (check for mfg standardization)
            mfg_cols = [col for col in df.columns if '_avg_lag' in col or '_avg_lead' in col]
            columns_by_file[file_path.name] = {
                'total_cols': len(df.columns),
                'mfg_specific_cols': mfg_cols
            }
            
            # Aggregate by manufacturer
            manufacturer_totals[manufacturer]['files'] += 1
            manufacturer_totals[manufacturer]['rows'] += rows
            total_rows_rds += rows
            
            print(f"{rows:,} rows")
            
            # Clear memory
            del df
            del result
            
        except Exception as e:
            print(f"ERROR: {e}")
            file_counts[file_path.name] = {'rows': 0, 'manufacturer': 'error', 'error': str(e)}
    
    print(f"\nTotal RDS files: {len(rds_files)}")
    print(f"Total rows in RDS: {total_rows_rds:,}")
    
    return file_counts, manufacturer_totals, total_rows_rds, columns_by_file

def get_bigquery_counts(client):
    """Get row counts from BigQuery."""
    print("\n" + "=" * 70)
    print("Getting counts from BigQuery...")
    print("-" * 70)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Get overall stats
    query_overall = f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT source_file) as total_files,
        COUNT(DISTINCT source_manufacturer) as total_manufacturers
    FROM `{table_ref}`
    """
    
    print("Overall stats:")
    result = client.query(query_overall).result()
    overall_stats = {}
    for row in result:
        overall_stats = {
            'total_rows': row.total_rows,
            'total_files': row.total_files,
            'total_manufacturers': row.total_manufacturers
        }
        print(f"  Total rows: {row.total_rows:,}")
        print(f"  Total files: {row.total_files}")
        print(f"  Total manufacturers: {row.total_manufacturers}")
    
    # Get counts by file
    query_by_file = f"""
    SELECT 
        source_file,
        source_manufacturer,
        COUNT(*) as row_count
    FROM `{table_ref}`
    GROUP BY source_file, source_manufacturer
    ORDER BY source_file
    """
    
    file_counts_bq = {}
    manufacturer_totals_bq = defaultdict(lambda: {'files': 0, 'rows': 0})
    
    print("\nCounting by file...")
    result = client.query(query_by_file).result()
    for row in result:
        file_counts_bq[row.source_file] = {
            'rows': row.row_count,
            'manufacturer': row.source_manufacturer
        }
        manufacturer_totals_bq[row.source_manufacturer]['files'] += 1
        manufacturer_totals_bq[row.source_manufacturer]['rows'] += row.row_count
    
    # Check for standardized columns
    query_columns = f"""
    SELECT column_name
    FROM `{PROJECT_ID}.{DATASET_ID}`.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{TABLE_ID}'
    ORDER BY ordinal_position
    """
    
    columns_bq = []
    result = client.query(query_columns).result()
    for row in result:
        columns_bq.append(row.column_name)
    
    mfg_standardized_cols = [col for col in columns_bq if col.startswith('mfg_avg_')]
    
    print(f"\nColumns in BigQuery: {len(columns_bq)}")
    print(f"Standardized mfg columns: {len(mfg_standardized_cols)}")
    
    return overall_stats, file_counts_bq, manufacturer_totals_bq, columns_bq

def reconcile():
    """Main reconciliation logic."""
    print("RX-OP Enhanced: Data Reconciliation")
    print("=" * 70)
    
    # Count RDS files
    file_counts_rds, manufacturer_totals_rds, total_rows_rds, columns_by_file = count_rds_files()
    
    # Get BigQuery counts
    client = get_bigquery_client()
    overall_stats_bq, file_counts_bq, manufacturer_totals_bq, columns_bq = get_bigquery_counts(client)
    
    # Reconciliation Report
    print("\n" + "=" * 70)
    print("RECONCILIATION REPORT")
    print("=" * 70)
    
    # Overall comparison
    print("\n1. OVERALL TOTALS")
    print("-" * 40)
    print(f"RDS Files:        {len(file_counts_rds)}")
    print(f"BigQuery Files:   {overall_stats_bq['total_files']}")
    print(f"Match:            {'✓ YES' if len(file_counts_rds) == overall_stats_bq['total_files'] else '✗ NO'}")
    print()
    print(f"RDS Rows:         {total_rows_rds:,}")
    print(f"BigQuery Rows:    {overall_stats_bq['total_rows']:,}")
    print(f"Match:            {'✓ YES' if total_rows_rds == overall_stats_bq['total_rows'] else '✗ NO'}")
    
    if total_rows_rds != overall_stats_bq['total_rows']:
        diff = overall_stats_bq['total_rows'] - total_rows_rds
        print(f"Difference:       {diff:+,} rows")
    
    # File-level comparison
    print("\n2. FILE-LEVEL COMPARISON")
    print("-" * 40)
    
    missing_in_bq = []
    row_mismatches = []
    
    for filename, rds_info in file_counts_rds.items():
        if filename not in file_counts_bq:
            missing_in_bq.append(filename)
        elif rds_info['rows'] != file_counts_bq[filename]['rows']:
            row_mismatches.append({
                'file': filename,
                'rds_rows': rds_info['rows'],
                'bq_rows': file_counts_bq[filename]['rows'],
                'diff': file_counts_bq[filename]['rows'] - rds_info['rows']
            })
    
    if missing_in_bq:
        print(f"✗ Files missing in BigQuery: {len(missing_in_bq)}")
        for f in missing_in_bq[:5]:  # Show first 5
            print(f"  - {f}")
    else:
        print("✓ All files present in BigQuery")
    
    if row_mismatches:
        print(f"\n✗ Files with row count mismatches: {len(row_mismatches)}")
        for m in row_mismatches[:5]:  # Show first 5
            print(f"  - {m['file']}: RDS={m['rds_rows']:,}, BQ={m['bq_rows']:,}, Diff={m['diff']:+,}")
    else:
        print("✓ All files have matching row counts")
    
    # Manufacturer-level comparison
    print("\n3. MANUFACTURER-LEVEL COMPARISON")
    print("-" * 40)
    
    all_manufacturers = set(manufacturer_totals_rds.keys()) | set(manufacturer_totals_bq.keys())
    
    print(f"{'Manufacturer':<20} {'RDS Files':>10} {'BQ Files':>10} {'RDS Rows':>15} {'BQ Rows':>15} {'Match':>8}")
    print("-" * 88)
    
    for mfg in sorted(all_manufacturers):
        rds_files = manufacturer_totals_rds[mfg]['files']
        rds_rows = manufacturer_totals_rds[mfg]['rows']
        bq_files = manufacturer_totals_bq[mfg]['files']
        bq_rows = manufacturer_totals_bq[mfg]['rows']
        
        match = '✓' if rds_files == bq_files and rds_rows == bq_rows else '✗'
        
        print(f"{mfg:<20} {rds_files:>10} {bq_files:>10} {rds_rows:>15,} {bq_rows:>15,} {match:>8}")
    
    # Column verification
    print("\n4. COLUMN STANDARDIZATION")
    print("-" * 40)
    
    mfg_cols_bq = [col for col in columns_bq if col.startswith('mfg_avg_')]
    expected_mfg_cols = ['mfg_avg_lag3', 'mfg_avg_lag6', 'mfg_avg_lag9', 'mfg_avg_lag12',
                         'mfg_avg_lead3', 'mfg_avg_lead6', 'mfg_avg_lead9', 'mfg_avg_lead12']
    
    print(f"Standardized columns in BigQuery: {len(mfg_cols_bq)}")
    print(f"Expected standardized columns: {len(expected_mfg_cols)}")
    
    missing_cols = set(expected_mfg_cols) - set(mfg_cols_bq)
    if missing_cols:
        print(f"✗ Missing expected columns: {missing_cols}")
    else:
        print("✓ All expected standardized columns present")
    
    # Check for any non-standardized manufacturer columns
    non_standard_cols = []
    for col in columns_bq:
        if ('_avg_lag' in col or '_avg_lead' in col) and not col.startswith('mfg_avg_'):
            non_standard_cols.append(col)
    
    if non_standard_cols:
        print(f"\n✗ Found {len(non_standard_cols)} non-standardized manufacturer columns:")
        for col in non_standard_cols[:5]:
            print(f"  - {col}")
    else:
        print("✓ No non-standardized manufacturer columns found")
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    issues = []
    if len(file_counts_rds) != overall_stats_bq['total_files']:
        issues.append(f"File count mismatch: {len(file_counts_rds)} RDS vs {overall_stats_bq['total_files']} BQ")
    if total_rows_rds != overall_stats_bq['total_rows']:
        issues.append(f"Row count mismatch: {total_rows_rds:,} RDS vs {overall_stats_bq['total_rows']:,} BQ")
    if missing_in_bq:
        issues.append(f"{len(missing_in_bq)} files missing in BigQuery")
    if row_mismatches:
        issues.append(f"{len(row_mismatches)} files with row count mismatches")
    if non_standard_cols:
        issues.append(f"{len(non_standard_cols)} non-standardized columns")
    
    if issues:
        print("Issues found:")
        for issue in issues:
            print(f"  ✗ {issue}")
    else:
        print("✓ PERFECT RECONCILIATION!")
        print("  - All files transferred")
        print("  - All row counts match")
        print("  - All columns properly standardized")
    
    return len(issues) == 0

def main():
    """Main execution."""
    success = reconcile()
    
    if success:
        print("\n✓ Data transfer verified successfully!")
    else:
        print("\n⚠ Some discrepancies found. Please review the report above.")
    
    return success

if __name__ == "__main__":
    main()