#!/usr/bin/env python3
"""
Debug Tier 2 matching to understand why scores are 0.0
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.tier2_openai import openai_match

# Load environment variables
load_dotenv()

def test_single_match():
    """Test a single name match"""
    
    # Test with a case that should match
    name_a = "Sonendo Inc"
    name_b = "Sonedno Inc"  # Typo variant
    
    print(f"Testing: '{name_a}' vs '{name_b}'")
    print("-" * 50)
    
    # Set the model
    os.environ['TIER2_MODEL'] = 'gpt-4o-mini'
    
    confidence, details = openai_match(name_a, name_b)
    
    print(f"Confidence: {confidence}")
    print(f"Details: {details}")
    
    # Test with gpt-5-mini
    print("\n" + "=" * 50)
    print("Testing with gpt-5-mini:")
    os.environ['TIER2_MODEL'] = 'gpt-5-mini'
    
    confidence, details = openai_match(name_a, name_b)
    
    print(f"Confidence: {confidence}")
    print(f"Details: {details}")

if __name__ == "__main__":
    test_single_match()