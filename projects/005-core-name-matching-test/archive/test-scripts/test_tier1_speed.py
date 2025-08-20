#!/usr/bin/env python3
"""
Test Tier 1 fuzzy matching speed
"""

import os
import sys
import time
import pandas as pd
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.tier1_fuzzy import fuzzy_match

def test_tier1_speed():
    """Test Tier 1 speed on full dataset"""
    
    test_file = 'test-data/test-data-inputs/test_dataset.csv'
    
    print(f"Loading {test_file}...")
    df = pd.read_csv(test_file)
    total_rows = len(df)
    print(f"Loaded {total_rows} rows")
    
    print("\nRunning Tier 1 fuzzy matching...")
    start_time = time.time()
    
    results = []
    for idx, row in df.iterrows():
        if idx % 100 == 0:
            elapsed = time.time() - start_time
            print(f"  Processed {idx}/{total_rows} rows ({elapsed:.2f}s)")
        
        fuzzy_score, _ = fuzzy_match(row['reference_name'], row['variant_name'])
        results.append(fuzzy_score)
    
    total_time = time.time() - start_time
    print(f"\nCompleted {total_rows} rows in {total_time:.2f}s")
    print(f"Average: {total_time/total_rows*1000:.2f}ms per row")
    print(f"Throughput: {total_rows/total_time:.1f} rows/sec")

if __name__ == "__main__":
    test_tier1_speed()