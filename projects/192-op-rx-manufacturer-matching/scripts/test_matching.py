#!/usr/bin/env python3
"""Test simple matching without imports"""

import pandas as pd
from pathlib import Path
import logging
from rapidfuzz import fuzz

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load data
input_dir = Path(__file__).parent.parent / "data" / "input"
op_df = pd.read_csv(input_dir / "op_manufacturers.csv")
rx_df = pd.read_csv(input_dir / "rx_manufacturers.csv")

logger.info(f"Loaded {len(op_df)} OP manufacturers and {len(rx_df)} RX manufacturers")

# Test blocking function
def get_blocking_key(name):
    if pd.isna(name) or not name:
        return 'UNKNOWN'
    name = str(name).strip().upper()
    # Remove common prefixes
    for prefix in ['THE ', 'A ', 'AN ']:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break
    if name:
        first_char = name[0]
        if first_char.isdigit():
            return 'NUMERIC'
        elif first_char.isalpha():
            return first_char
        else:
            return 'SPECIAL'
    return 'UNKNOWN'

# Add blocking keys
op_df['blocking_key'] = op_df['manufacturer_name'].apply(get_blocking_key)
rx_df['blocking_key'] = rx_df['manufacturer_name'].apply(get_blocking_key)

# Show blocking statistics
op_blocks = op_df['blocking_key'].value_counts()
rx_blocks = rx_df['blocking_key'].value_counts()

print("\nOP Blocking Distribution:")
print(op_blocks.head(10))

print("\nRX Blocking Distribution:")
print(rx_blocks.head(10))

# Calculate reduction
total_comparisons = 0
for key in op_df['blocking_key'].unique():
    op_count = len(op_df[op_df['blocking_key'] == key])
    rx_count = len(rx_df[rx_df['blocking_key'] == key]) if key in rx_df['blocking_key'].values else 0
    total_comparisons += op_count * rx_count

naive_comparisons = len(op_df) * len(rx_df)
reduction = 1 - (total_comparisons / naive_comparisons)

print(f"\nComparison Statistics:")
print(f"  Naive comparisons: {naive_comparisons:,}")
print(f"  With blocking: {total_comparisons:,}")
print(f"  Reduction: {reduction:.1%}")

# Test a few exact matches
print("\nTesting some known exact matches:")
test_cases = [
    ("Abbott Laboratories", "Abbott Laboratories"),
    ("Pfizer Inc.", "Pfizer Inc"),
    ("Johnson & Johnson", "Johnson & Johnson"),
]

for op_name, rx_name in test_cases:
    if op_name in op_df['manufacturer_name'].values and rx_name in rx_df['manufacturer_name'].values:
        score = fuzz.ratio(op_name, rx_name)
        print(f"  {op_name} <-> {rx_name}: {score}%")
    else:
        print(f"  {op_name} or {rx_name} not found in data")