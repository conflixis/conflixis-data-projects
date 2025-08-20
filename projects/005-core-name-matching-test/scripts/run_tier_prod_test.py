#!/usr/bin/env python3
"""
Test Runner for Tier-prod (PR1362 Production Approach)
Implements full Elasticsearch-style + AI enhancement matching
"""

import os
import sys
import time
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.tier_prod_matching import tier_prod_match

# Load environment variables
load_dotenv()


def calculate_metrics(y_true, y_pred):
    """Calculate accuracy, precision, recall, F1 score"""
    # Convert to numpy arrays
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    
    # Calculate confusion matrix elements
    tp = np.sum((y_true == 1) & (y_pred == 1))
    tn = np.sum((y_true == 0) & (y_pred == 0))
    fp = np.sum((y_true == 0) & (y_pred == 1))
    fn = np.sum((y_true == 1) & (y_pred == 0))
    
    # Calculate metrics
    accuracy = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0 else 0
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    return {
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1_score': f1,
        'true_positives': int(tp),
        'true_negatives': int(tn),
        'false_positives': int(fp),
        'false_negatives': int(fn)
    }


def process_batch_concurrent(batch: List[Tuple[str, str]], model: str, max_workers: int = 5) -> List[Dict]:
    """Process a batch of name pairs concurrently"""
    results = []
    
    def process_pair(name_a: str, name_b: str) -> Dict:
        """Process a single pair"""
        try:
            start = time.time()
            confidence, details = tier_prod_match(name_a, name_b, model)
            elapsed = time.time() - start
            
            return {
                'name_a': name_a,
                'name_b': name_b,
                'confidence': confidence,
                'details': details,
                'processing_time': elapsed,
                'success': True
            }
        except Exception as e:
            print(f"Error processing {name_a} vs {name_b}: {e}")
            return {
                'name_a': name_a,
                'name_b': name_b,
                'confidence': 0,
                'details': {'error': str(e)},
                'processing_time': 0,
                'success': False
            }
    
    # Use ThreadPoolExecutor for concurrent processing
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


def run_tier_prod_test(test_file: str, model: str = 'gpt-4o-mini', 
                       decision_threshold: float = 50.0,
                       max_workers: int = 5):
    """
    Run Tier-prod matching on test dataset
    """
    
    print("=" * 80)
    print("TIER-PROD (PR1362) MATCHING TEST")
    print("=" * 80)
    print(f"Model: {model}")
    print(f"Decision threshold: {decision_threshold}%")
    print(f"Max workers: {max_workers}")
    
    # Set the model
    os.environ['TIER_PROD_MODEL'] = model
    
    # Load test data
    print(f"\nLoading test data from {test_file}...")
    df = pd.read_csv(test_file)
    total_rows = len(df)
    print(f"Loaded {total_rows} test pairs")
    
    # Process all pairs
    print(f"\nProcessing {total_rows} pairs with Tier-prod approach...")
    
    # Track tier usage
    tier_stats = {
        'fast_path': 0,
        'ai_enhanced': 0,
        'no_match': 0,
        'errors': 0
    }
    
    all_results = []
    batch_size = max_workers * 2
    
    for i in range(0, total_rows, batch_size):
        batch_end = min(i + batch_size, total_rows)
        batch_df = df.iloc[i:batch_end]
        
        # Prepare batch
        name_pairs = [(row['reference_name'], row['variant_name']) for _, row in batch_df.iterrows()]
        
        print(f"\nProcessing batch: {i+1} to {batch_end} of {total_rows}")
        batch_start = time.time()
        
        # Process batch
        batch_results = process_batch_concurrent(name_pairs, model, max_workers)
        
        # Add expected match info and collect results
        for j, result in enumerate(batch_results):
            row_idx = i + j
            result['expected_match'] = df.iloc[row_idx]['expected_match']
            result['variant_type'] = df.iloc[row_idx].get('variant_type', 'unknown')
            result['index'] = row_idx
            
            # Track tier usage
            if 'details' in result and 'tier_reached' in result['details']:
                tier = result['details']['tier_reached']
                if tier in tier_stats:
                    tier_stats[tier] += 1
            
            all_results.append(result)
        
        batch_time = time.time() - batch_start
        print(f"  Batch completed in {batch_time:.2f}s ({batch_time/len(batch_results):.2f}s per pair)")
        
        # Show progress
        completed = len(all_results)
        print(f"  Progress: {completed}/{total_rows} ({100*completed/total_rows:.1f}%)")
        print(f"  Tier usage so far: Fast path: {tier_stats['fast_path']}, "
              f"AI enhanced: {tier_stats['ai_enhanced']}, No match: {tier_stats['no_match']}")
    
    # Process results
    print("\n" + "=" * 80)
    print("Processing results...")
    
    results_data = []
    for result in all_results:
        # Convert expected_match to boolean
        expected_bool = result['expected_match'] in [True, 'True', 'TRUE', 'true', 1, '1', 'yes', 'Yes']
        
        # Make prediction based on threshold
        predicted_bool = result['confidence'] >= decision_threshold
        
        results_data.append({
            'index': result['index'],
            'name_a': result['name_a'],
            'name_b': result['name_b'],
            'expected_match': result['expected_match'],
            'variant_type': result['variant_type'],
            'confidence': result['confidence'],
            'predicted_match': predicted_bool,
            'tier_reached': result['details'].get('tier_reached', 'unknown'),
            'initial_confidence': result['details'].get('initial_confidence', None),
            'ai_confidence': result['details'].get('ai_confidence', None),
            'final_confidence': result['details'].get('final_confidence', result['confidence']),
            'processing_time': result['processing_time']
        })
    
    # Create DataFrame
    results_df = pd.DataFrame(results_data)
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    model_suffix = model.replace('-', '_').replace('.', '_')
    output_file = f"test-data/test-results/test_results_tier_prod_{model_suffix}_{timestamp}.csv"
    results_df.to_csv(output_file, index=False)
    print(f"\nResults saved to: {output_file}")
    
    # Calculate metrics
    y_true = [1 if row['expected_match'] in [True, 'True', 'TRUE', 'true', 1, '1', 'yes', 'Yes'] else 0 
              for _, row in results_df.iterrows()]
    y_pred = [1 if row['predicted_match'] else 0 for _, row in results_df.iterrows()]
    
    metrics = calculate_metrics(y_true, y_pred)
    
    # Print summary
    print("\n" + "=" * 80)
    print("TIER-PROD TEST SUMMARY")
    print("=" * 80)
    print(f"Total test pairs: {total_rows}")
    print(f"Overall Accuracy: {metrics['accuracy']:.4f} ({metrics['accuracy']*100:.2f}%)")
    print(f"Precision: {metrics['precision']:.4f}")
    print(f"Recall: {metrics['recall']:.4f}")
    print(f"F1 Score: {metrics['f1_score']:.4f}")
    
    print(f"\nConfusion Matrix:")
    print(f"  True Positives:  {metrics['true_positives']}")
    print(f"  False Positives: {metrics['false_positives']}")
    print(f"  True Negatives:  {metrics['true_negatives']}")
    print(f"  False Negatives: {metrics['false_negatives']}")
    
    print(f"\nTier Usage Statistics:")
    print(f"  Fast Path (>95% confidence):    {tier_stats['fast_path']} ({100*tier_stats['fast_path']/total_rows:.1f}%)")
    print(f"  AI Enhanced (30-95%):           {tier_stats['ai_enhanced']} ({100*tier_stats['ai_enhanced']/total_rows:.1f}%)")
    print(f"  No Match (<30%):                {tier_stats['no_match']} ({100*tier_stats['no_match']/total_rows:.1f}%)")
    
    # Performance by variant type
    print(f"\nAccuracy by Variant Type:")
    for vtype in results_df['variant_type'].unique():
        type_df = results_df[results_df['variant_type'] == vtype]
        type_true = [1 if row['expected_match'] in [True, 'True', 'TRUE', 'true', 1, '1'] else 0 
                     for _, row in type_df.iterrows()]
        type_pred = [1 if row['predicted_match'] else 0 for _, row in type_df.iterrows()]
        type_accuracy = sum([1 for t, p in zip(type_true, type_pred) if t == p]) / len(type_true) if type_true else 0
        print(f"  {vtype:25s}: {type_accuracy:.4f} ({len(type_df)} samples)")
    
    # Timing analysis
    total_time = results_df['processing_time'].sum()
    avg_time = results_df['processing_time'].mean()
    print(f"\nTiming Analysis:")
    print(f"  Total processing time: {total_time:.2f}s")
    print(f"  Average time per pair: {avg_time:.4f}s")
    
    # Compare tier timings
    for tier in ['fast_path', 'ai_enhanced', 'no_match']:
        tier_df = results_df[results_df['tier_reached'] == tier]
        if len(tier_df) > 0:
            tier_avg_time = tier_df['processing_time'].mean()
            print(f"  Average time for {tier}: {tier_avg_time:.4f}s")
    
    print("=" * 80)
    
    return results_df, metrics


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-file', default='test-data/test-data-inputs/test_dataset.csv',
                       help='Path to test dataset CSV')
    parser.add_argument('--model', default='gpt-4o-mini',
                       help='OpenAI model to use')
    parser.add_argument('--threshold', type=float, default=50.0,
                       help='Decision threshold for match (default: 50.0)')
    parser.add_argument('--max-workers', type=int, default=5,
                       help='Maximum concurrent workers (default: 5)')
    
    args = parser.parse_args()
    
    run_tier_prod_test(
        args.test_file,
        args.model,
        args.threshold,
        args.max_workers
    )