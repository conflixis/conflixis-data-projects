#!/usr/bin/env python3
"""
Sample combination of 10 RDS files
JIRA: DA-167

This script combines a sample of 10 RDS files to test the process.
"""

import pyreadr
import pandas as pd
from pathlib import Path
import time
from datetime import datetime

LOCAL_DATA_DIR = Path("mfg-spec-data")
OUTPUT_DIR = Path("parquet-data")

def standardize_columns(df, manufacturer):
    """Standardize manufacturer-specific column names."""
    # Column mapping for manufacturer-specific columns
    col_mapping = {}
    
    # Find manufacturer-specific columns and create generic names
    for col in df.columns:
        if f'{manufacturer}_avg_' in col:
            # Replace manufacturer name with 'mfg'
            new_col = col.replace(f'{manufacturer}_avg_', 'mfg_avg_')
            col_mapping[col] = new_col
    
    # Rename columns if mapping exists
    if col_mapping:
        df = df.rename(columns=col_mapping)
    
    return df

def process_sample_files():
    """Process a sample of 10 RDS files."""
    print("=" * 70)
    print("Sample RDS File Combination")
    print("=" * 70)
    
    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Get sample of files (diverse manufacturers)
    all_files = list(LOCAL_DATA_DIR.glob("*.rds"))
    
    # Select diverse sample
    sample_files = []
    manufacturers_seen = set()
    
    for f in all_files:
        # Extract manufacturer
        parts = f.stem.replace('df_spec_', '').split('_')
        manufacturer = parts[0] if parts else 'unknown'
        
        # Add one file per manufacturer until we have 10
        if manufacturer not in manufacturers_seen:
            sample_files.append(f)
            manufacturers_seen.add(manufacturer)
            if len(sample_files) >= 10:
                break
    
    print(f"Selected {len(sample_files)} sample files from different manufacturers")
    print("-" * 70)
    
    # Process each file
    dfs = []
    total_rows = 0
    
    for i, file_path in enumerate(sample_files, 1):
        print(f"[{i:2d}] Processing {file_path.name}...", end=' ')
        start = time.time()
        
        try:
            # Read RDS file
            result = pyreadr.read_r(str(file_path))
            df = result[None]
            
            # Extract manufacturer
            parts = file_path.stem.replace('df_spec_', '').split('_')
            manufacturer = parts[0] if parts else 'unknown'
            specialty = '_'.join(parts[1:]) if len(parts) > 1 else 'unknown'
            
            # Standardize columns
            df = standardize_columns(df, manufacturer)
            
            # Add source columns
            df['source_file'] = file_path.name
            df['source_manufacturer'] = manufacturer
            df['source_specialty'] = specialty
            
            rows = len(df)
            total_rows += rows
            dfs.append(df)
            
            elapsed = time.time() - start
            print(f"✓ {rows:,} rows ({elapsed:.1f}s)")
            
        except Exception as e:
            print(f"✗ Error: {e}")
    
    # Combine all dataframes
    print(f"\nCombining {len(dfs)} dataframes...")
    start = time.time()
    
    combined_df = pd.concat(dfs, ignore_index=True)
    
    elapsed = time.time() - start
    print(f"✓ Combined in {elapsed:.1f}s")
    
    # Statistics
    print("\n" + "=" * 70)
    print("SAMPLE STATISTICS")
    print("=" * 70)
    print(f"Total rows: {len(combined_df):,}")
    print(f"Total columns: {len(combined_df.columns)}")
    print(f"Memory usage: {combined_df.memory_usage(deep=True).sum() / (1024**2):.1f} MB")
    
    # Column info
    print(f"\nColumns ({len(combined_df.columns)}):")
    for col in sorted(combined_df.columns):
        dtype = combined_df[col].dtype
        null_pct = (combined_df[col].isna().sum() / len(combined_df)) * 100
        print(f"  {col:30s} {str(dtype):15s} {null_pct:5.1f}% null")
    
    # Manufacturer distribution
    print(f"\nManufacturer distribution:")
    mfg_counts = combined_df['source_manufacturer'].value_counts()
    for mfg, count in mfg_counts.items():
        pct = (count / len(combined_df)) * 100
        print(f"  {mfg:20s}: {count:8,} rows ({pct:5.1f}%)")
    
    # Save to parquet
    output_file = OUTPUT_DIR / "sample_combined.parquet"
    print(f"\nSaving to {output_file}...")
    start = time.time()
    
    combined_df.to_parquet(output_file, engine='pyarrow', compression='snappy')
    
    elapsed = time.time() - start
    file_size = output_file.stat().st_size / (1024**2)
    print(f"✓ Saved in {elapsed:.1f}s ({file_size:.1f} MB)")
    
    # Summary
    print("\n" + "=" * 70)
    print("SAMPLE COMBINATION COMPLETE")
    print("=" * 70)
    print(f"Input files: {len(sample_files)}")
    print(f"Output file: {output_file}")
    print(f"Total rows: {len(combined_df):,}")
    print(f"File size: {file_size:.1f} MB")
    print(f"Compression ratio: {(combined_df.memory_usage(deep=True).sum() / (1024**2)) / file_size:.1f}x")
    
    return combined_df

if __name__ == "__main__":
    df = process_sample_files()
    print("\n✓ Sample combination complete!")
    print(f"  Next step: Upload {OUTPUT_DIR}/sample_combined.parquet to BigQuery")