#!/usr/bin/env python3
"""
Remove duplicate NPIs from the provider file
"""

import pandas as pd
from pathlib import Path

# Read the file
file_path = Path(__file__).parent.parent / 'data' / 'inputs' / 'provider_npis.csv'
df = pd.read_csv(file_path)

print(f"Original rows: {len(df)}")
print(f"Duplicate NPIs: {df.NPI.duplicated().sum()}")

# Remove duplicates, keeping first occurrence
df_unique = df.drop_duplicates(subset=['NPI'], keep='first')

print(f"After deduplication: {len(df_unique)} rows")

# Save back
df_unique.to_csv(file_path, index=False)
print(f"Saved unique NPIs to {file_path}")