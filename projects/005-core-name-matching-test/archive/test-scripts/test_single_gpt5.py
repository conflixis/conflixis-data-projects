#!/usr/bin/env python3
"""
Test a single name match with gpt-5-mini to check performance
"""

import os
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.tier2_openai import openai_match

# Load environment variables
load_dotenv()

def test_single():
    """Test a single match with timing"""
    
    # Set the model
    os.environ['TIER2_MODEL'] = 'gpt-5-mini'
    
    name_a = "Sonendo Inc"
    name_b = "Sonedno Inc"
    
    print(f"Testing with gpt-5-mini")
    print(f"Name A: {name_a}")
    print(f"Name B: {name_b}")
    print("-" * 50)
    
    start_time = time.time()
    confidence, details = openai_match(name_a, name_b)
    elapsed = time.time() - start_time
    
    print(f"Confidence: {confidence}")
    print(f"Details: {details}")
    print(f"Time taken: {elapsed:.2f} seconds")
    
    # Calculate throughput
    if elapsed > 0:
        per_minute = 60 / elapsed
        print(f"Throughput: {per_minute:.1f} matches per minute")
        print(f"For 1000 matches: {1000 * elapsed / 60:.1f} minutes")

if __name__ == "__main__":
    test_single()