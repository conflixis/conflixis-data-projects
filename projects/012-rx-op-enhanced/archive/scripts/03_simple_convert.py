#!/usr/bin/env python3
"""
Simple RDS to Parquet conversion - one file at a time
JIRA: DA-167
"""

import pyreadr
import pandas as pd
from pathlib import Path
import gc
import sys

LOCAL_DATA_DIR = Path("mfg-spec-data")
OUTPUT_DIR = Path("parquet-data")

def convert_single_file(rds_file):
    """Convert a single RDS file to Parquet."""
    try:
        print(f"Reading {rds_file.name}...", end=' ')
        
        # Read RDS
        result = pyreadr.read_r(str(rds_file))
        df = result[None]
        
        print(f"{len(df):,} rows", end=' ')
        
        # Extract metadata
        parts = rds_file.stem.replace('df_spec_', '').split('_')
        manufacturer = parts[0] if parts else 'unknown'
        specialty = '_'.join(parts[1:]) if len(parts) > 1 else 'unknown'
        
        # Standardize manufacturer-specific columns
        for col in df.columns:
            if f'{manufacturer}_avg_' in col:
                new_col = col.replace(f'{manufacturer}_avg_', 'mfg_avg_')
                df.rename(columns={col: new_col}, inplace=True)
        
        # Add metadata
        df['source_file'] = rds_file.name
        df['source_manufacturer'] = manufacturer
        df['source_specialty'] = specialty
        
        # Save to parquet
        output_file = OUTPUT_DIR / f"{rds_file.stem}.parquet"
        df.to_parquet(output_file, engine='pyarrow', compression='snappy')
        
        size_mb = output_file.stat().st_size / (1024**2)
        print(f"→ {size_mb:.1f} MB parquet")
        
        # Clean memory
        del df
        del result
        gc.collect()
        
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

def main():
    """Convert sample of RDS files."""
    print("=" * 70)
    print("Simple RDS → Parquet Conversion")
    print("=" * 70)
    
    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Get smallest files for sample
    rds_files = sorted(LOCAL_DATA_DIR.glob("*.rds"), 
                      key=lambda x: x.stat().st_size)[:10]
    
    print(f"Converting {len(rds_files)} smallest files")
    print("-" * 70)
    
    success = 0
    for i, rds_file in enumerate(rds_files, 1):
        size_mb = rds_file.stat().st_size / (1024**2)
        print(f"[{i:2d}] {rds_file.name[:40]:40s} ({size_mb:5.1f} MB) ", end='')
        
        if convert_single_file(rds_file):
            success += 1
    
    print("\n" + "=" * 70)
    print(f"Converted {success}/{len(rds_files)} files")
    
    # List output files
    parquet_files = list(OUTPUT_DIR.glob("*.parquet"))
    total_size = sum(f.stat().st_size for f in parquet_files) / (1024**2)
    
    print(f"Output: {len(parquet_files)} parquet files, {total_size:.1f} MB total")
    
    if parquet_files:
        print("\nNext: Combine parquet files for BigQuery upload")

if __name__ == "__main__":
    main()