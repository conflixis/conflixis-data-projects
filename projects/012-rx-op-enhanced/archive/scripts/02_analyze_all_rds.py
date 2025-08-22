#!/usr/bin/env python3
"""
Analyze all RDS files to ensure compatibility
JIRA: DA-167
"""

import pyreadr
import pandas as pd
from pathlib import Path
import json
from datetime import datetime

LOCAL_DATA_DIR = Path("mfg-spec-data")

def analyze_rds_files():
    """Analyze all RDS files for structure and compatibility."""
    print("=" * 70)
    print("Analyzing All RDS Files")
    print("=" * 70)
    
    # Get all RDS files
    rds_files = sorted(LOCAL_DATA_DIR.glob("*.rds"))
    print(f"Found {len(rds_files)} RDS files")
    print("-" * 70)
    
    results = []
    all_columns = set()
    column_types = {}
    issues = []
    
    # Sample first 10 for detailed output
    sample_size = 10
    
    for i, file_path in enumerate(rds_files, 1):
        try:
            # Read file
            result = pyreadr.read_r(str(file_path))
            df = result[None]  # Get the dataframe
            
            # Extract manufacturer and specialty from filename
            parts = file_path.stem.replace('df_spec_', '').split('_')
            manufacturer = parts[0] if parts else 'unknown'
            specialty = '_'.join(parts[1:]) if len(parts) > 1 else 'unknown'
            
            file_info = {
                'file': file_path.name,
                'manufacturer': manufacturer,
                'specialty': specialty,
                'rows': len(df),
                'columns': len(df.columns),
                'size_mb': file_path.stat().st_size / (1024 * 1024),
                'column_names': list(df.columns),
                'dtypes': {col: str(df[col].dtype) for col in df.columns}
            }
            
            # Track all columns
            all_columns.update(df.columns)
            
            # Track column types
            for col, dtype in file_info['dtypes'].items():
                if col not in column_types:
                    column_types[col] = set()
                column_types[col].add(dtype)
            
            results.append(file_info)
            
            # Print progress for first few files
            if i <= sample_size:
                print(f"[{i:3d}] {file_info['file'][:50]:50s} | {file_info['rows']:8,d} rows | {file_info['size_mb']:6.1f} MB")
            elif i % 50 == 0:
                print(f"[{i:3d}] Processing... {i}/{len(rds_files)} files analyzed")
                
        except Exception as e:
            issues.append({
                'file': file_path.name,
                'error': str(e)
            })
            print(f"[{i:3d}] ERROR: {file_path.name} - {str(e)[:50]}")
    
    # Analyze results
    print("\n" + "=" * 70)
    print("ANALYSIS SUMMARY")
    print("=" * 70)
    
    # Check if all files have same columns
    column_counts = {}
    for r in results:
        cols_key = tuple(sorted(r['column_names']))
        if cols_key not in column_counts:
            column_counts[cols_key] = []
        column_counts[cols_key].append(r['file'])
    
    print(f"\nTotal files analyzed: {len(results)}")
    print(f"Files with errors: {len(issues)}")
    
    if len(column_counts) == 1:
        print("✅ All files have the SAME column structure!")
    else:
        print(f"⚠️  Found {len(column_counts)} different column structures")
        for i, (cols, files) in enumerate(column_counts.items(), 1):
            print(f"  Structure {i}: {len(files)} files, {len(cols)} columns")
    
    # Column consistency
    print(f"\nUnique columns across all files: {len(all_columns)}")
    print("Columns:", ", ".join(sorted(all_columns)))
    
    # Type consistency
    print("\nColumn type consistency:")
    inconsistent = []
    for col, types in column_types.items():
        if len(types) > 1:
            inconsistent.append(col)
            print(f"  ⚠️  {col}: {types}")
    
    if not inconsistent:
        print("  ✅ All columns have consistent types across files")
    
    # Statistics
    total_rows = sum(r['rows'] for r in results)
    total_size = sum(r['size_mb'] for r in results)
    
    print(f"\nTotal rows across all files: {total_rows:,}")
    print(f"Total size: {total_size:.1f} MB ({total_size/1024:.2f} GB)")
    print(f"Average rows per file: {total_rows//len(results):,}")
    print(f"Average file size: {total_size/len(results):.1f} MB")
    
    # Manufacturers summary
    manufacturers = {}
    for r in results:
        mfg = r['manufacturer']
        if mfg not in manufacturers:
            manufacturers[mfg] = {'count': 0, 'rows': 0, 'size_mb': 0}
        manufacturers[mfg]['count'] += 1
        manufacturers[mfg]['rows'] += r['rows']
        manufacturers[mfg]['size_mb'] += r['size_mb']
    
    print(f"\nManufacturers ({len(manufacturers)}):")
    for mfg, stats in sorted(manufacturers.items()):
        print(f"  {mfg:20s}: {stats['count']:2d} files, {stats['rows']:10,d} rows, {stats['size_mb']:7.1f} MB")
    
    # Save detailed report
    report = {
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_files': len(results),
            'total_rows': total_rows,
            'total_size_mb': total_size,
            'unique_columns': len(all_columns),
            'column_structures': len(column_counts),
            'files_with_errors': len(issues)
        },
        'columns': sorted(list(all_columns)),
        'column_types': {col: list(types) for col, types in column_types.items()},
        'manufacturers': manufacturers,
        'issues': issues
    }
    
    with open('rds_analysis_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\n✓ Detailed report saved to: rds_analysis_report.json")
    
    return results, issues

if __name__ == "__main__":
    results, issues = analyze_rds_files()
    
    if issues:
        print("\n" + "=" * 70)
        print("ISSUES FOUND")
        print("=" * 70)
        for issue in issues:
            print(f"  {issue['file']}: {issue['error']}")
    
    print("\n" + "=" * 70)
    print("Analysis Complete")
    print("=" * 70)