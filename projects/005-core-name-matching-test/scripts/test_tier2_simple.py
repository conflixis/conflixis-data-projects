#!/usr/bin/env python3
"""
Simple test to debug the issue
"""

import os
import sys
import pandas as pd
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

def test_loading():
    """Test loading the dataset"""
    
    test_file = 'test_data/test_dataset.csv'
    
    print(f"Loading {test_file}...")
    
    if not os.path.exists(test_file):
        print(f"File not found: {test_file}")
        return
    
    df = pd.read_csv(test_file)
    print(f"Loaded {len(df)} rows")
    print(f"Columns: {df.columns.tolist()}")
    print(f"First row: {df.iloc[0].to_dict()}")

if __name__ == "__main__":
    test_loading()