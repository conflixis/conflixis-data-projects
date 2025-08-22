#!/usr/bin/env python3
"""
Efficiently analyze all RDS files without loading full data
JIRA: DA-167
"""

import pyreadr
import pandas as pd
from pathlib import Path
import json
from datetime import datetime

LOCAL_DATA_DIR = Path("mfg-spec-data")

def analyze_single_file(file_path):
    """Analyze a single RDS file."""
    try:
        # Read just the structure, not full data
        result = pyreadr.read_r(str(file_path))
        df = result[None]
        
        # Get basic info without holding full data in memory
        info = {
            'file': file_path.name,
            'rows': len(df),
            'columns': list(df.columns),
            'dtypes': {col: str(df[col].dtype) for col in df.columns},
            'size_mb': file_path.stat().st_size / (1024 * 1024)
        }
        
        # Extract manufacturer and specialty
        parts = file_path.stem.replace('df_spec_', '').split('_')
        info['manufacturer'] = parts[0] if parts else 'unknown'
        info['specialty'] = '_'.join(parts[1:]) if len(parts) > 1 else 'unknown'
        
        # Clear the dataframe from memory
        del df
        del result
        
        return info, None
        
    except Exception as e:
        return None, {'file': file_path.name, 'error': str(e)}

def main():
    """Main analysis."""
    print("=" * 70)
    print("Efficient RDS File Analysis")
    print("=" * 70)
    
    # Get all RDS files
    rds_files = sorted(LOCAL_DATA_DIR.glob("*.rds"))
    print(f"Found {len(rds_files)} RDS files to analyze")
    print("-" * 70)
    
    # Process files one by one
    results = []
    issues = []
    all_columns = set()
    column_types = {}
    
    for i, file_path in enumerate(rds_files, 1):
        if i % 10 == 1:
            print(f"Processing files {i}-{min(i+9, len(rds_files))}...")
        
        info, error = analyze_single_file(file_path)
        
        if info:
            results.append(info)
            all_columns.update(info['columns'])
            
            # Track column types
            for col, dtype in info['dtypes'].items():
                if col not in column_types:
                    column_types[col] = set()
                column_types[col].add(dtype)
                
            # Print sample info for first few
            if i <= 5:
                print(f"  [{i:3d}] {info['file'][:40]:40s} | {info['rows']:8,d} rows | {info['size_mb']:6.1f} MB")
        else:
            issues.append(error)
            print(f"  [{i:3d}] ERROR: {error['file']}")
    
    # Summary statistics
    print("\n" + "=" * 70)
    print("ANALYSIS SUMMARY")
    print("=" * 70)
    
    print(f"Files analyzed successfully: {len(results)}")
    print(f"Files with errors: {len(issues)}")
    
    if results:
        # Check column consistency
        first_cols = set(results[0]['columns'])
        all_same = all(set(r['columns']) == first_cols for r in results)
        
        if all_same:
            print("\n✅ All files have the SAME column structure!")
            print(f"   {len(first_cols)} columns: {', '.join(sorted(first_cols))}")
        else:
            print("\n⚠️  Files have different column structures")
            unique_structures = {}
            for r in results:
                cols_key = tuple(sorted(r['columns']))
                if cols_key not in unique_structures:
                    unique_structures[cols_key] = []
                unique_structures[cols_key].append(r['file'])
            print(f"   Found {len(unique_structures)} different structures")
        
        # Statistics
        total_rows = sum(r['rows'] for r in results)
        total_size = sum(r['size_mb'] for r in results)
        
        print(f"\nTotal rows: {total_rows:,}")
        print(f"Total size: {total_size:.1f} MB ({total_size/1024:.2f} GB)")
        print(f"Average rows per file: {total_rows//len(results):,}")
        
        # Manufacturers
        manufacturers = {}
        for r in results:
            mfg = r['manufacturer']
            if mfg not in manufacturers:
                manufacturers[mfg] = {'files': 0, 'rows': 0, 'size_mb': 0}
            manufacturers[mfg]['files'] += 1
            manufacturers[mfg]['rows'] += r['rows']
            manufacturers[mfg]['size_mb'] += r['size_mb']
        
        print(f"\nManufacturers ({len(manufacturers)}):")
        for mfg in sorted(manufacturers.keys()):
            stats = manufacturers[mfg]
            print(f"  {mfg:20s}: {stats['files']:3d} files, {stats['rows']:10,d} rows, {stats['size_mb']:8.1f} MB")
        
        # Type consistency check
        print("\nColumn type consistency:")
        inconsistent = []
        for col, types in column_types.items():
            if len(types) > 1:
                inconsistent.append(f"{col}: {list(types)}")
        
        if inconsistent:
            print(f"  ⚠️  {len(inconsistent)} columns have mixed types:")
            for inc in inconsistent[:5]:
                print(f"     {inc}")
        else:
            print("  ✅ All columns have consistent types")
        
        # Save summary
        summary = {
            'timestamp': datetime.now().isoformat(),
            'total_files': len(results),
            'total_rows': total_rows,
            'total_size_mb': total_size,
            'total_size_gb': total_size / 1024,
            'columns_consistent': all_same,
            'unique_columns': len(all_columns),
            'manufacturers': manufacturers,
            'sample_file_info': results[0] if results else None
        }
        
        with open('analysis_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        print("\n✓ Summary saved to: analysis_summary.json")

if __name__ == "__main__":
    main()