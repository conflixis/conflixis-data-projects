#!/usr/bin/env python3
"""
Step 2: Analyze RDS files structure
JIRA: DA-167

This script analyzes all downloaded RDS files to understand their structure
and determine how to combine them into a single table.
"""

import os
import sys
import pyreadr
import pandas as pd
from pathlib import Path
from typing import List, Dict
import json

LOCAL_DATA_DIR = Path("mfg-spec-data")
ANALYSIS_OUTPUT = Path("rds_analysis_report.json")

def analyze_rds_file(file_path: Path) -> Dict:
    """Analyze a single RDS file."""
    print(f"\nAnalyzing: {file_path.name}")
    
    try:
        # Read RDS file
        result = pyreadr.read_r(str(file_path))
        
        # Get the first (and usually only) dataframe
        df_name = list(result.keys())[0]
        df = result[df_name]
        
        # Extract manufacturer and specialty from filename if possible
        # Expected format: df_spec_<manufacturer>_<specialty>.rds
        filename_parts = file_path.stem.split('_')
        manufacturer = "unknown"
        specialty = "unknown"
        
        if len(filename_parts) >= 3:
            if filename_parts[0] == 'df' and filename_parts[1] == 'spec':
                manufacturer = filename_parts[2] if len(filename_parts) > 2 else "unknown"
                specialty = '_'.join(filename_parts[3:]) if len(filename_parts) > 3 else "unknown"
        
        # Analyze structure
        analysis = {
            'filename': file_path.name,
            'dataframe_name': df_name,
            'rows': len(df),
            'columns': len(df.columns),
            'column_names': list(df.columns),
            'column_types': {col: str(df[col].dtype) for col in df.columns},
            'manufacturer': manufacturer,
            'specialty': specialty,
            'file_size_mb': file_path.stat().st_size / (1024 * 1024),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024),
            'sample_values': {}
        }
        
        # Get sample values for key columns
        key_columns = ['manufacturer', 'core_specialty', 'SPECIALTY_PRIMARY', 'year', 'month']
        for col in key_columns:
            if col in df.columns:
                unique_vals = df[col].dropna().unique()
                if len(unique_vals) <= 10:
                    analysis['sample_values'][col] = list(unique_vals[:10])
                else:
                    analysis['sample_values'][col] = {
                        'count': len(unique_vals),
                        'samples': list(unique_vals[:5])
                    }
        
        print(f"  ✓ Rows: {analysis['rows']:,}, Columns: {analysis['columns']}")
        print(f"  ✓ Manufacturer: {manufacturer}, Specialty: {specialty}")
        
        return analysis
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return {
            'filename': file_path.name,
            'error': str(e)
        }

def compare_schemas(analyses: List[Dict]) -> Dict:
    """Compare schemas across all files."""
    print("\n" + "=" * 70)
    print("Schema Comparison")
    print("=" * 70)
    
    # Get all unique column sets
    all_schemas = {}
    for analysis in analyses:
        if 'error' not in analysis:
            columns_tuple = tuple(sorted(analysis['column_names']))
            if columns_tuple not in all_schemas:
                all_schemas[columns_tuple] = []
            all_schemas[columns_tuple].append(analysis['filename'])
    
    # Report findings
    if len(all_schemas) == 1:
        print("✓ All files have the SAME schema!")
        columns = list(list(all_schemas.keys())[0])
        print(f"  Common columns ({len(columns)}): {', '.join(columns[:5])}...")
        return {
            'schemas_match': True,
            'num_unique_schemas': 1,
            'common_columns': columns
        }
    else:
        print(f"⚠ Found {len(all_schemas)} different schemas:")
        
        # Find common columns
        all_column_sets = [set(schema) for schema in all_schemas.keys()]
        common_columns = list(set.intersection(*all_column_sets))
        all_columns = list(set.union(*all_column_sets))
        
        for i, (schema, files) in enumerate(all_schemas.items(), 1):
            print(f"\n  Schema {i} ({len(files)} files):")
            print(f"    Files: {', '.join(files[:3])}...")
            print(f"    Columns: {len(schema)}")
        
        print(f"\n  Common columns across all files: {len(common_columns)}")
        print(f"  Total unique columns: {len(all_columns)}")
        
        return {
            'schemas_match': False,
            'num_unique_schemas': len(all_schemas),
            'common_columns': common_columns,
            'all_columns': all_columns,
            'schema_groups': {i: files for i, (_, files) in enumerate(all_schemas.items(), 1)}
        }

def generate_combination_strategy(analyses: List[Dict], schema_comparison: Dict) -> Dict:
    """Generate strategy for combining files."""
    print("\n" + "=" * 70)
    print("Combination Strategy")
    print("=" * 70)
    
    total_rows = sum(a['rows'] for a in analyses if 'rows' in a)
    total_size_mb = sum(a['file_size_mb'] for a in analyses if 'file_size_mb' in a)
    
    strategy = {
        'total_files': len(analyses),
        'total_rows': total_rows,
        'total_size_mb': total_size_mb,
        'estimated_memory_gb': (total_size_mb * 2.5) / 1024,  # Rough estimate
        'can_combine': True,
        'method': 'concatenate',
        'notes': []
    }
    
    if schema_comparison['schemas_match']:
        strategy['notes'].append("All files have identical schemas - simple concatenation possible")
    else:
        strategy['notes'].append("Files have different schemas - will use common columns or add NaN for missing")
        strategy['method'] = 'concatenate_with_alignment'
    
    # Check if manufacturer/specialty are in the data or need to be extracted from filename
    has_manufacturer_col = any('manufacturer' in a.get('column_names', []) for a in analyses if 'error' not in a)
    has_specialty_col = any('core_specialty' in a.get('column_names', []) or 
                           'SPECIALTY_PRIMARY' in a.get('column_names', []) 
                           for a in analyses if 'error' not in a)
    
    if not has_manufacturer_col:
        strategy['notes'].append("Need to add manufacturer column from filename")
    if not has_specialty_col:
        strategy['notes'].append("Need to add/standardize specialty column from filename")
    
    print(f"  Total files to combine: {strategy['total_files']}")
    print(f"  Total rows: {strategy['total_rows']:,}")
    print(f"  Total size: {strategy['total_size_mb']:.2f} MB")
    print(f"  Estimated memory needed: {strategy['estimated_memory_gb']:.2f} GB")
    print(f"  Combination method: {strategy['method']}")
    print(f"  Notes:")
    for note in strategy['notes']:
        print(f"    - {note}")
    
    return strategy

def main():
    """Main execution."""
    print("=" * 70)
    print("RX-OP Enhanced: RDS File Analysis")
    print("=" * 70)
    
    # Find all RDS files
    rds_files = list(LOCAL_DATA_DIR.glob("*.rds"))
    
    if not rds_files:
        print(f"\n✗ No RDS files found in {LOCAL_DATA_DIR}")
        print("  Please run 01_download_from_drive.py first")
        sys.exit(1)
    
    print(f"\nFound {len(rds_files)} RDS files to analyze")
    print("-" * 70)
    
    # Analyze each file
    analyses = []
    for file in rds_files:
        analysis = analyze_rds_file(file)
        analyses.append(analysis)
    
    # Compare schemas
    schema_comparison = compare_schemas(analyses)
    
    # Generate combination strategy
    strategy = generate_combination_strategy(analyses, schema_comparison)
    
    # Save analysis report
    report = {
        'file_analyses': analyses,
        'schema_comparison': schema_comparison,
        'combination_strategy': strategy
    }
    
    with open(ANALYSIS_OUTPUT, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print("\n" + "=" * 70)
    print(f"✓ Analysis complete!")
    print(f"  Report saved to: {ANALYSIS_OUTPUT}")
    print(f"  Next step: Run 03_combine_to_single_table.py")
    print("=" * 70)

if __name__ == "__main__":
    main()