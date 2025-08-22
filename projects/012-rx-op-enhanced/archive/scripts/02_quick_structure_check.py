#!/usr/bin/env python3
"""
Quick structure check of RDS files
JIRA: DA-167
"""

import pyreadr
import pandas as pd
from pathlib import Path

# Test with one small file first
test_file = Path("mfg-spec-data/df_spec_abbvie_Urology.rds")

print(f"Testing with: {test_file.name}")
print(f"File size: {test_file.stat().st_size / (1024*1024):.2f} MB")
print("-" * 50)

# Read the file
result = pyreadr.read_r(str(test_file))

# Get dataframe
df_name = list(result.keys())[0]
df = result[df_name]

print(f"Dataframe name: {df_name}")
print(f"Shape: {df.shape}")
print(f"Columns ({len(df.columns)}):")
for col in df.columns:
    print(f"  - {col}: {df[col].dtype}")

print("\nFirst 5 rows:")
print(df.head())

print("\nMemory usage:")
print(f"  {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB in memory")