#!/usr/bin/env python3
"""
Test column standardization fix
"""

import pyreadr
import pandas as pd
from pathlib import Path
import re

LOCAL_DATA_DIR = Path("mfg-spec-data")

def standardize_columns_improved(df, manufacturer):
    """
    Improved standardization that handles compound manufacturer names.
    """
    col_mapping = {}
    
    print(f"  Manufacturer: {manufacturer}")
    print(f"  Checking columns for standardization...")
    
    for col in df.columns:
        # Check if this is a manufacturer-specific average column
        if '_avg_lag' in col or '_avg_lead' in col:
            # Use regex to extract the pattern and replace manufacturer part
            # Pattern: anything_avg_lag/lead + number
            match = re.match(r'^(.+?)(_avg_(?:lag|lead)\d+)$', col)
            if match:
                prefix = match.group(1)
                suffix = match.group(2)
                
                # Check if the prefix matches or contains the manufacturer
                if manufacturer in prefix or prefix in manufacturer:
                    new_col = f"mfg{suffix}"
                    col_mapping[col] = new_col
                    print(f"    {col} → {new_col}")
    
    if col_mapping:
        df = df.rename(columns=col_mapping)
    
    return df, col_mapping

# Test with janssen_biotech file
test_file = LOCAL_DATA_DIR / "df_spec_janssen_biotech_Cardiology.rds"

if test_file.exists():
    print(f"Testing with: {test_file.name}")
    print("-" * 50)
    
    # Read file
    result = pyreadr.read_r(str(test_file))
    df = result[None]
    
    # Extract manufacturer
    parts = test_file.stem.replace('df_spec_', '').split('_')
    
    # Handle compound manufacturer names
    if len(parts) >= 3 and parts[1] == 'biotech':
        manufacturer = f"{parts[0]}_{parts[1]}"  # janssen_biotech
    else:
        manufacturer = parts[0]
    
    print(f"Extracted manufacturer: {manufacturer}")
    
    # Get columns with manufacturer-specific averages
    mfg_cols = [col for col in df.columns if manufacturer in col and ('lag' in col or 'lead' in col)]
    print(f"\nOriginal manufacturer columns ({len(mfg_cols)}):")
    for col in mfg_cols[:5]:  # Show first 5
        print(f"  - {col}")
    
    # Test standardization
    df_std, mapping = standardize_columns_improved(df, manufacturer)
    
    print(f"\nStandardization mapping ({len(mapping)} columns):")
    for old, new in list(mapping.items())[:5]:  # Show first 5
        print(f"  {old} → {new}")
    
    # Verify
    mfg_std_cols = [col for col in df_std.columns if col.startswith('mfg_avg_')]
    print(f"\nStandardized columns ({len(mfg_std_cols)}):")
    for col in mfg_std_cols[:5]:  # Show first 5
        print(f"  - {col}")
    
    print("\n✓ Test complete!")
else:
    print(f"Test file not found: {test_file}")
    print("\nTrying with any available file...")
    
    # Get any file with compound manufacturer name
    for f in LOCAL_DATA_DIR.glob("*_biotech_*.rds"):
        print(f"Found: {f.name}")
        break