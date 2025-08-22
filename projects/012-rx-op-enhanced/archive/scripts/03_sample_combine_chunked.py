#!/usr/bin/env python3
"""
Sample combination of 10 RDS files with chunking
JIRA: DA-167

This script combines a sample of 10 RDS files into chunked parquet files.
"""

import pyreadr
import pandas as pd
from pathlib import Path
import time
from datetime import datetime
import gc

LOCAL_DATA_DIR = Path("mfg-spec-data")
OUTPUT_DIR = Path("parquet-data")
CHUNK_SIZE = 1_000_000  # 1M rows per chunk

def standardize_columns(df, manufacturer):
    """Standardize manufacturer-specific column names."""
    col_mapping = {}
    
    # Find manufacturer-specific columns and create generic names
    for col in df.columns:
        if f'{manufacturer}_avg_' in col:
            new_col = col.replace(f'{manufacturer}_avg_', 'mfg_avg_')
            col_mapping[col] = new_col
    
    if col_mapping:
        df = df.rename(columns=col_mapping)
    
    return df

def process_sample_files_chunked():
    """Process sample files and save as chunked parquet."""
    print("=" * 70)
    print("Sample RDS File Combination (Chunked)")
    print("=" * 70)
    print(f"Chunk size: {CHUNK_SIZE:,} rows per file")
    print("-" * 70)
    
    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Clean any existing sample files
    for f in OUTPUT_DIR.glob("sample_chunk_*.parquet"):
        f.unlink()
    
    # Get sample of files
    all_files = list(LOCAL_DATA_DIR.glob("*.rds"))
    
    # Select diverse sample (one per manufacturer)
    sample_files = []
    manufacturers_seen = set()
    
    for f in all_files:
        parts = f.stem.replace('df_spec_', '').split('_')
        manufacturer = parts[0] if parts else 'unknown'
        
        if manufacturer not in manufacturers_seen:
            sample_files.append(f)
            manufacturers_seen.add(manufacturer)
            if len(sample_files) >= 10:
                break
    
    print(f"Selected {len(sample_files)} sample files")
    print("Manufacturers:", ', '.join(manufacturers_seen))
    print("-" * 70)
    
    # Process files and write chunks
    chunk_num = 0
    current_chunk = []
    current_rows = 0
    total_rows = 0
    
    for i, file_path in enumerate(sample_files, 1):
        print(f"\n[{i:2d}/{len(sample_files)}] Processing {file_path.name}")
        start = time.time()
        
        try:
            # Read RDS file
            result = pyreadr.read_r(str(file_path))
            df = result[None]
            
            # Extract metadata
            parts = file_path.stem.replace('df_spec_', '').split('_')
            manufacturer = parts[0] if parts else 'unknown'
            specialty = '_'.join(parts[1:]) if len(parts) > 1 else 'unknown'
            
            # Standardize columns
            df = standardize_columns(df, manufacturer)
            
            # Add source columns
            df['source_file'] = file_path.name
            df['source_manufacturer'] = manufacturer
            df['source_specialty'] = specialty
            
            file_rows = len(df)
            print(f"    Rows: {file_rows:,}")
            
            # Process in batches
            for start_idx in range(0, file_rows, CHUNK_SIZE):
                end_idx = min(start_idx + CHUNK_SIZE, file_rows)
                batch = df.iloc[start_idx:end_idx]
                
                current_chunk.append(batch)
                current_rows += len(batch)
                total_rows += len(batch)
                
                # Write chunk if it's large enough
                if current_rows >= CHUNK_SIZE:
                    chunk_df = pd.concat(current_chunk, ignore_index=True)
                    chunk_file = OUTPUT_DIR / f"sample_chunk_{chunk_num:03d}.parquet"
                    
                    print(f"    Writing chunk {chunk_num}: {len(chunk_df):,} rows...", end=' ')
                    chunk_df.to_parquet(chunk_file, engine='pyarrow', compression='snappy')
                    print(f"✓ ({chunk_file.stat().st_size / (1024**2):.1f} MB)")
                    
                    # Clear memory
                    current_chunk = []
                    current_rows = 0
                    chunk_num += 1
                    del chunk_df
                    gc.collect()
            
            # Clear the dataframe from memory
            del df
            del result
            gc.collect()
            
            elapsed = time.time() - start
            print(f"    Processed in {elapsed:.1f}s")
            
        except Exception as e:
            print(f"    ✗ Error: {e}")
    
    # Write any remaining data
    if current_chunk:
        chunk_df = pd.concat(current_chunk, ignore_index=True)
        chunk_file = OUTPUT_DIR / f"sample_chunk_{chunk_num:03d}.parquet"
        print(f"\nWriting final chunk {chunk_num}: {len(chunk_df):,} rows...", end=' ')
        chunk_df.to_parquet(chunk_file, engine='pyarrow', compression='snappy')
        print(f"✓ ({chunk_file.stat().st_size / (1024**2):.1f} MB)")
        chunk_num += 1
    
    # Summary
    print("\n" + "=" * 70)
    print("SAMPLE CHUNKING COMPLETE")
    print("=" * 70)
    print(f"Input files: {len(sample_files)}")
    print(f"Output chunks: {chunk_num}")
    print(f"Total rows: {total_rows:,}")
    
    # List chunk files
    chunk_files = sorted(OUTPUT_DIR.glob("sample_chunk_*.parquet"))
    total_size = sum(f.stat().st_size for f in chunk_files) / (1024**2)
    
    print(f"\nChunk files:")
    for f in chunk_files:
        size_mb = f.stat().st_size / (1024**2)
        print(f"  {f.name}: {size_mb:.1f} MB")
    
    print(f"\nTotal size: {total_size:.1f} MB")
    
    # Verify by reading back
    print("\nVerifying chunks...")
    verify_rows = 0
    for f in chunk_files:
        df_check = pd.read_parquet(f)
        verify_rows += len(df_check)
    
    print(f"✓ Verified {verify_rows:,} rows across {len(chunk_files)} files")
    
    return chunk_files

if __name__ == "__main__":
    chunks = process_sample_files_chunked()
    print("\n✓ Sample chunking complete!")
    print(f"  Next step: Upload {len(chunks)} parquet files to BigQuery")