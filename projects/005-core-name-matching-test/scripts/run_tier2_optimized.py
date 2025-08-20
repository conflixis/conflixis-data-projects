#!/usr/bin/env python3
"""
Run Tier 2 matching only on cases where Tier 1 failed or had low confidence
Uses concurrent requests for better performance
"""

import os
import sys
import time
import pandas as pd
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.tier1_fuzzy import fuzzy_match
from src.tier2_openai import openai_match

# Load environment variables
load_dotenv()

def process_batch_concurrent(batch: List[Tuple[str, str]], model: str, max_workers: int = 10) -> List[Dict]:
    """Process a batch of name pairs concurrently"""
    results = []
    
    def process_pair(name_a: str, name_b: str) -> Dict:
        """Process a single pair"""
        try:
            start = time.time()
            confidence, details = openai_match(name_a, name_b)
            elapsed = time.time() - start
            return {
                'name_a': name_a,
                'name_b': name_b,
                'openai_score': confidence,
                'details': details,
                'processing_time': elapsed,
                'success': True
            }
        except Exception as e:
            return {
                'name_a': name_a,
                'name_b': name_b,
                'openai_score': 0,
                'details': {'error': str(e)},
                'processing_time': 0,
                'success': False
            }
    
    # Use ThreadPoolExecutor for concurrent API calls
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_pair = {
            executor.submit(process_pair, name_a, name_b): (name_a, name_b)
            for name_a, name_b in batch
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_pair):
            result = future.result()
            results.append(result)
    
    return results

def run_tier2_on_failures(test_file: str, model: str = 'gpt-4o-mini', 
                          fuzzy_threshold: float = 85.0,
                          max_workers: int = 10):
    """
    Run Tier 2 only on cases where Tier 1 (fuzzy) failed or had low confidence
    """
    
    # Set the model
    os.environ['TIER2_MODEL'] = model
    
    # Load test data
    df = pd.read_csv(test_file)
    total_rows = len(df)
    
    print(f"Running Tier 2 matching with model: {model}")
    print(f"Total test cases: {total_rows}")
    print(f"Fuzzy threshold: {fuzzy_threshold}%")
    print("=" * 60)
    
    # First, run Tier 1 (fuzzy matching) on all pairs
    print("\nPhase 1: Running Tier 1 (Fuzzy) matching...")
    tier1_results = []
    tier1_start = time.time()
    
    for idx, row in df.iterrows():
        fuzzy_score, _ = fuzzy_match(row['reference_name'], row['variant_name'])
        tier1_results.append({
            'index': idx,
            'name_a': row['reference_name'],
            'name_b': row['variant_name'],
            'expected_match': row['expected_match'],
            'fuzzy_score': fuzzy_score,
            'fuzzy_passed': fuzzy_score >= fuzzy_threshold
        })
    
    tier1_time = time.time() - tier1_start
    tier1_df = pd.DataFrame(tier1_results)
    
    # Identify cases that need Tier 2
    needs_tier2 = tier1_df[tier1_df['fuzzy_score'] < fuzzy_threshold]
    tier1_passed = tier1_df[tier1_df['fuzzy_score'] >= fuzzy_threshold]
    
    print(f"Tier 1 completed in {tier1_time:.2f}s")
    print(f"  - Passed (>={fuzzy_threshold}%): {len(tier1_passed)} cases")
    print(f"  - Need Tier 2 (<{fuzzy_threshold}%): {len(needs_tier2)} cases")
    
    if len(needs_tier2) == 0:
        print("\nAll cases passed Tier 1! No need for Tier 2.")
        return tier1_df
    
    # Run Tier 2 on failed cases
    print(f"\nPhase 2: Running Tier 2 (Fuzzy + OpenAI) on {len(needs_tier2)} cases...")
    print(f"Using {max_workers} concurrent workers")
    print(f"Model: {model}")
    
    # Prepare batch
    name_pairs = [(row['name_a'], row['name_b']) for _, row in needs_tier2.iterrows()]
    print(f"Prepared {len(name_pairs)} name pairs for processing")
    
    tier2_start = time.time()
    batch_size = max_workers * 2  # Process in chunks
    all_tier2_results = []
    
    print(f"Batch size: {batch_size}")
    print("Starting batch processing...")
    
    for i in range(0, len(name_pairs), batch_size):
        batch_end = min(i + batch_size, len(name_pairs))
        batch = name_pairs[i:batch_end]
        
        print(f"\nProcessing batch {i//batch_size + 1}: cases {i+1} to {batch_end} of {len(name_pairs)}")
        print(f"  Submitting {len(batch)} requests to OpenAI...")
        batch_start = time.time()
        
        # Process batch concurrently
        batch_results = process_batch_concurrent(batch, model, max_workers)
        print(f"  Received {len(batch_results)} responses")
        all_tier2_results.extend(batch_results)
        
        batch_time = time.time() - batch_start
        print(f"  Batch completed in {batch_time:.2f}s ({batch_time/len(batch):.2f}s per case)")
        
        # Show progress
        completed = len(all_tier2_results)
        remaining = len(name_pairs) - completed
        if completed > 0:
            avg_time = (time.time() - tier2_start) / completed
            eta = remaining * avg_time
            print(f"  Progress: {completed}/{len(name_pairs)} ({100*completed/len(name_pairs):.1f}%)")
            print(f"  ETA: {eta/60:.1f} minutes")
    
    tier2_time = time.time() - tier2_start
    
    # Combine results
    print("\n" + "=" * 60)
    print("Combining Tier 1 and Tier 2 results...")
    
    # Add Tier 2 results to the dataframe
    tier2_lookup = {(r['name_a'], r['name_b']): r for r in all_tier2_results}
    
    final_results = []
    for _, row in tier1_df.iterrows():
        result = {
            'name_a': row['name_a'],
            'name_b': row['name_b'],
            'expected_match': row['expected_match'],
            'fuzzy_score': row['fuzzy_score'],
            'tier1_passed': row['fuzzy_passed']
        }
        
        # Add Tier 2 results if available
        key = (row['name_a'], row['name_b'])
        if key in tier2_lookup:
            tier2_data = tier2_lookup[key]
            result['tier2_score'] = tier2_data['openai_score']
            # Weighted combination: 40% fuzzy + 60% OpenAI
            result['final_score'] = 0.4 * row['fuzzy_score'] + 0.6 * tier2_data['openai_score']
            result['tier2_used'] = True
        else:
            # Tier 1 passed, use fuzzy score as final
            result['tier2_score'] = None
            result['final_score'] = row['fuzzy_score']
            result['tier2_used'] = False
        
        result['predicted_match'] = result['final_score'] >= 50
        final_results.append(result)
    
    # Save results
    results_df = pd.DataFrame(final_results)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    model_suffix = model.replace('-', '_').replace('.', '_')
    output_file = f"test-data/test-results/test_results_tier2_optimized_{model_suffix}_{timestamp}.csv"
    results_df.to_csv(output_file, index=False)
    
    # Calculate metrics
    results_df['expected_match_bool'] = results_df['expected_match'].apply(
        lambda x: x in [True, 'True', 'TRUE', 'true', 1, '1', 'yes', 'Yes', 'YES']
    )
    
    accuracy = (results_df['predicted_match'] == results_df['expected_match_bool']).mean()
    
    # Summary
    print("\nSummary:")
    print(f"  - Tier 1 time: {tier1_time:.2f}s ({total_rows/tier1_time:.1f} cases/sec)")
    print(f"  - Tier 2 time: {tier2_time:.2f}s ({len(needs_tier2)/tier2_time:.1f} cases/sec)")
    print(f"  - Total time: {tier1_time + tier2_time:.2f}s")
    print(f"  - Tier 1 handled: {len(tier1_passed)} cases")
    print(f"  - Tier 2 handled: {len(needs_tier2)} cases")
    print(f"  - Final accuracy: {accuracy:.2%}")
    print(f"  - Results saved to: {output_file}")
    
    return results_df

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-file', default='test-data/test-data-inputs/test_dataset_100.csv')
    parser.add_argument('--model', default='gpt-4o-mini')
    parser.add_argument('--fuzzy-threshold', type=float, default=85.0,
                       help='Fuzzy score threshold for Tier 1 (default: 85.0)')
    parser.add_argument('--max-workers', type=int, default=10,
                       help='Maximum concurrent OpenAI requests (default: 10)')
    args = parser.parse_args()
    
    run_tier2_on_failures(args.test_file, args.model, 
                         args.fuzzy_threshold, args.max_workers)