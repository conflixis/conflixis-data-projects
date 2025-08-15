#!/usr/bin/env python3
"""
Test single gpt-5-mini call to debug the issue
"""

import os
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

# Load environment variables
load_dotenv()

def test_single():
    """Test a single gpt-5-mini call"""
    
    # Set the model
    os.environ['TIER2_MODEL'] = 'gpt-5-mini'
    
    print("Setting up...")
    from src.tier2_openai import openai_match
    
    name_a = "KAISER FOUNDATION HOSPITAL"
    name_b = "KAISER PERMANENTE"
    
    print(f"Testing gpt-5-mini with:")
    print(f"  Name A: {name_a}")
    print(f"  Name B: {name_b}")
    print("-" * 50)
    
    start_time = time.time()
    print("Calling OpenAI API...")
    
    try:
        confidence, details = openai_match(name_a, name_b)
        elapsed = time.time() - start_time
        
        print(f"Success!")
        print(f"  Confidence: {confidence}")
        print(f"  Details: {details}")
        print(f"  Time: {elapsed:.2f}s")
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"Error after {elapsed:.2f}s: {e}")

if __name__ == "__main__":
    test_single()