#!/usr/bin/env python3
"""
Test data types in the enhanced dataset
"""

import pandas as pd

# Load the enhanced data
df = pd.read_csv('/home/incent/conflixis-data-projects/projects/189-alcon-custom-report/data/outputs/alcon_payments_with_affiliations.csv')

print("Data shape:", df.shape)
print("\nColumns:", list(df.columns))
print("\nData types:")
print(df.dtypes)

print("\nFirst few rows of key columns:")
print(df[['covered_recipient_npi', 'total_payments', 'payment_count', 'AFFILIATED_HQ_STATE']].head())

print("\nUnique data types in total_payments:")
print(df['total_payments'].apply(type).value_counts())