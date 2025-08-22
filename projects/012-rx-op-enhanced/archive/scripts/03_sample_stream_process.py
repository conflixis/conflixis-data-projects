#!/usr/bin/env python3
"""
Stream process sample RDS files with minimal memory usage
JIRA: DA-167

Process files one at a time and append to parquet files.
"""

import pyreadr
import pandas as pd
from pathlib import Path
import time
import gc

LOCAL_DATA_DIR = Path("mfg-spec-data")
OUTPUT_DIR = Path("parquet-data")

def standardize_columns(df, manufacturer):
    """Standardize manufacturer-specific column names."""
    col_mapping = {}
    
    # Handle various manufacturer name formats
    mfg_variants = [manufacturer, manufacturer.replace('_', ''), manufacturer.replace('_', ' ')]
    
    for col in df.columns:
        for variant in mfg_variants:
            if f'{variant}_avg_' in col:
                new_col = col.replace(f'{variant}_avg_', 'mfg_avg_')
                col_mapping[col] = new_col
                break
    
    if col_mapping:
        df = df.rename(columns=col_mapping)
    
    return df

def stream_process_sample():
    """Process sample files with streaming to minimize memory."""
    print("=" * 70)
    print("Stream Processing Sample RDS Files")
    print("=" * 70)
    
    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Clean existing sample file
    output_file = OUTPUT_DIR / "sample_combined_stream.parquet"
    if output_file.exists():
        output_file.unlink()
    
    # Get sample files (smaller ones first)
    all_files = sorted(LOCAL_DATA_DIR.glob("*.rds"), 
                      key=lambda x: x.stat().st_size)[:10]  # Take 10 smallest files
    
    print(f"Processing {len(all_files)} smallest files")
    print("-" * 70)
    
    total_rows = 0
    first_file = True
    
    for i, file_path in enumerate(all_files, 1):
        file_size_mb = file_path.stat().st_size / (1024**2)
        print(f"\n[{i:2d}] {file_path.name} ({file_size_mb:.1f} MB)")
        
        try:
            start = time.time()
            
            # Read RDS file
            print("    Reading...", end=' ')
            result = pyreadr.read_r(str(file_path))
            df = result[None]
            print(f"✓ {len(df):,} rows")
            
            # Extract metadata
            parts = file_path.stem.replace('df_spec_', '').split('_')
            manufacturer = parts[0] if parts else 'unknown'
            specialty = '_'.join(parts[1:]) if len(parts) > 1 else 'unknown'
            
            # Standardize columns
            print("    Standardizing columns...", end=' ')
            df = standardize_columns(df, manufacturer)
            print("✓")
            
            # Add source columns
            df['source_file'] = file_path.name
            df['source_manufacturer'] = manufacturer
            df['source_specialty'] = specialty
            
            # Write to parquet (append mode)
            print("    Writing to parquet...", end=' ')
            if first_file:
                # Create new file for first dataframe
                df.to_parquet(output_file, engine='pyarrow', compression='snappy')
                first_file = False
            else:
                # Append to existing file by reading, concatenating, and rewriting
                # For production, use a better append strategy
                existing_df = pd.read_parquet(output_file)
                combined = pd.concat([existing_df, df], ignore_index=True)
                combined.to_parquet(output_file, engine='pyarrow', compression='snappy')
                del existing_df
                del combined
            
            total_rows += len(df)
            elapsed = time.time() - start
            print(f"✓ ({elapsed:.1f}s)")
            
            # Clear memory
            del df
            del result
            gc.collect()
            
        except Exception as e:
            print(f"    ✗ Error: {e}")
            continue
    
    # Summary
    print("\n" + "=" * 70)
    print("STREAM PROCESSING COMPLETE")
    print("=" * 70)
    
    if output_file.exists():
        final_size_mb = output_file.stat().st_size / (1024**2)
        
        # Verify
        print("Verifying final file...")
        df_final = pd.read_parquet(output_file)
        
        print(f"Output file: {output_file}")
        print(f"Total rows: {len(df_final):,}")
        print(f"Total columns: {len(df_final.columns)}")
        print(f"File size: {final_size_mb:.1f} MB")
        print(f"Memory usage: {df_final.memory_usage(deep=True).sum() / (1024**2):.1f} MB")
        
        # Show sample
        print(f"\nSample columns: {list(df_final.columns[:10])}")
        print(f"\nManufacturers in sample:")
        for mfg, count in df_final['source_manufacturer'].value_counts().items():
            print(f"  {mfg}: {count:,} rows")
        
        return df_final
    else:
        print("✗ No output file created")
        return None

if __name__ == "__main__":
    df = stream_process_sample()
    if df is not None:
        print("\n✓ Sample ready for BigQuery upload!")