#!/usr/bin/env python3
"""
Test Tier 2 with batch processing and progress tracking
"""

import os
import sys
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.tier1_fuzzy import fuzzy_match
from src.tier2_openai import openai_match

def tier2_match(name_a: str, name_b: str):
    """Tier 2 matching: Fuzzy + OpenAI with weighted aggregation"""
    # Get fuzzy score
    fuzzy_score = fuzzy_match(name_a, name_b)
    
    # Get OpenAI score
    openai_score, details = openai_match(name_a, name_b)
    
    # Weighted aggregation (40% fuzzy, 60% OpenAI)
    confidence = (0.4 * fuzzy_score) + (0.6 * openai_score)
    
    return confidence, fuzzy_score, openai_score, details

# Load environment variables
load_dotenv()

def run_batch_test(test_file: str, model: str, batch_size: int = 10):
    """Run Tier 2 test in batches with progress tracking"""
    
    # Set the model
    os.environ['TIER2_MODEL'] = model
    
    # Load test data
    df = pd.read_csv(test_file)
    total_rows = len(df)
    
    print(f"Testing with model: {model}")
    print(f"Total test cases: {total_rows}")
    print("=" * 60)
    
    results = []
    start_time = time.time()
    
    for i in range(0, total_rows, batch_size):
        batch_end = min(i + batch_size, total_rows)
        batch = df.iloc[i:batch_end]
        batch_start_time = time.time()
        
        print(f"\nProcessing batch {i//batch_size + 1}: rows {i+1} to {batch_end}")
        
        for idx, row in batch.iterrows():
            try:
                # Run tier2 match
                row_start = time.time()
                confidence, fuzzy_score, openai_score, details = tier2_match(
                    row['reference_name'], 
                    row['variant_name']
                )
                row_time = time.time() - row_start
                
                results.append({
                    'name_a': row['reference_name'],
                    'name_b': row['variant_name'],
                    'expected_match': row['expected_match'],
                    'tier2_confidence': confidence,
                    'fuzzy_score': fuzzy_score,
                    'openai_score': openai_score,
                    'processing_time': row_time
                })
                
                print(f"  {idx+1}/{total_rows}: {row['reference_name'][:30]} vs {row['variant_name'][:30]} "
                      f"-> {confidence:.1f}% (time: {row_time:.2f}s)")
                
            except Exception as e:
                print(f"  Error processing row {idx+1}: {e}")
                results.append({
                    'name_a': row['reference_name'],
                    'name_b': row['variant_name'],
                    'expected_match': row['expected_match'],
                    'tier2_confidence': 0,
                    'fuzzy_score': 0,
                    'openai_score': 0,
                    'processing_time': 0
                })
        
        batch_time = time.time() - batch_start_time
        avg_time = batch_time / len(batch)
        total_elapsed = time.time() - start_time
        rows_processed = batch_end
        rows_remaining = total_rows - rows_processed
        estimated_remaining = (rows_remaining * total_elapsed / rows_processed) if rows_processed > 0 else 0
        
        print(f"  Batch completed in {batch_time:.2f}s (avg: {avg_time:.2f}s per row)")
        print(f"  Progress: {rows_processed}/{total_rows} ({100*rows_processed/total_rows:.1f}%)")
        print(f"  Estimated time remaining: {estimated_remaining/60:.1f} minutes")
    
    # Save results
    results_df = pd.DataFrame(results)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    model_suffix = model.replace('-', '_').replace('.', '_')
    output_file = f"test_data/test_results_tier2_{model_suffix}_{timestamp}.csv"
    results_df.to_csv(output_file, index=False)
    
    # Calculate metrics
    results_df['predicted_match'] = results_df['tier2_confidence'] >= 50
    results_df['expected_match_bool'] = results_df['expected_match'].apply(
        lambda x: x in [True, 'True', 'TRUE', 'true', 1, '1', 'yes', 'Yes', 'YES']
    )
    
    accuracy = (results_df['predicted_match'] == results_df['expected_match_bool']).mean()
    total_time = time.time() - start_time
    avg_time = total_time / total_rows
    
    print("\n" + "=" * 60)
    print(f"Test completed!")
    print(f"Results saved to: {output_file}")
    print(f"Accuracy: {accuracy:.2%}")
    print(f"Total time: {total_time:.2f}s")
    print(f"Average time per row: {avg_time:.2f}s")
    print(f"Rows per minute: {60/avg_time:.1f}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-file', default='test_data/test_dataset_100.csv')
    parser.add_argument('--model', default='gpt-5-mini')
    parser.add_argument('--batch-size', type=int, default=10)
    args = parser.parse_args()
    
    run_batch_test(args.test_file, args.model, args.batch_size)