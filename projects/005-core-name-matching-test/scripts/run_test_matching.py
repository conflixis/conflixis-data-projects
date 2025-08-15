#!/usr/bin/env python3
"""
Test Runner for Name Matching Algorithms
Runs matching algorithms on test dataset and records results
"""

import pandas as pd
import sys
import os
from datetime import datetime
from typing import Dict, Tuple
import argparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path to import matching modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))

# Import the matching modules
from tier1_fuzzy import fuzzy_match
from tier2_openai import openai_match

class TestRunner:
    """Run matching algorithms on test dataset."""
    
    def __init__(self, test_file: str, algorithm: str = 'fuzzy'):
        """
        Initialize test runner.
        
        Args:
            test_file: Path to test dataset CSV
            algorithm: Algorithm to use ('fuzzy', 'tier2', 'multi_tier')
        """
        self.test_df = pd.read_csv(test_file)
        self.algorithm = algorithm
        self.results = []
        self.tier_stats = {'tier1_only': 0, 'tier2_used': 0, 'api_calls': 0}
        
    def run_fuzzy_matching(self, name_a: str, name_b: str, threshold: float = 85.0) -> Tuple[bool, float]:
        """
        Run fuzzy matching algorithm.
        
        Returns:
            Tuple of (is_match, confidence_score)
        """
        score, details = fuzzy_match(name_a, name_b)
        
        # Determine match based on threshold
        is_match = score >= threshold
        
        return is_match, score
    
    def run_simple_matching(self, name_a: str, name_b: str) -> Tuple[bool, float]:
        """
        Run a simple exact matching algorithm for baseline.
        
        Returns:
            Tuple of (is_match, confidence_score)
        """
        # Normalize for comparison
        norm_a = name_a.lower().strip()
        norm_b = name_b.lower().strip()
        
        if norm_a == norm_b:
            return True, 100.0
        
        # Check if one is contained in the other
        if norm_a in norm_b or norm_b in norm_a:
            return True, 80.0
        
        return False, 0.0
    
    def run_tier2_matching(self, name_a: str, name_b: str) -> Tuple[bool, float, Dict]:
        """
        Run Tier 2 matching: Fuzzy first, then OpenAI if needed.
        
        Returns:
            Tuple of (is_match, final_score, tier_details)
        """
        # Step 1: Run fuzzy matching
        fuzzy_score, fuzzy_details = fuzzy_match(name_a, name_b)
        
        tier_details = {
            'tier1_score': round(fuzzy_score, 2),
            'tier2_score': None,
            'aggregated_score': None,
            'tier_reached': 1,
            'tier1_details': fuzzy_details,
            'model_used': None
        }
        
        # If fuzzy score >= 90%, return as matched (Tier 1 only)
        if fuzzy_score >= 90.0:
            self.tier_stats['tier1_only'] += 1
            tier_details['final_decision'] = 'Matched at Tier 1'
            return True, fuzzy_score, tier_details
        
        # Step 2: Run OpenAI analysis since fuzzy < 90%
        self.tier_stats['tier2_used'] += 1
        self.tier_stats['api_calls'] += 1
        
        try:
            openai_score, openai_details = openai_match(name_a, name_b)
            tier_details['tier2_score'] = round(openai_score, 2)
            tier_details['tier2_details'] = openai_details
            tier_details['tier_reached'] = 2
            
            # Extract model used from OpenAI details
            if 'model_used' in openai_details:
                tier_details['model_used'] = openai_details['model_used']
                if not hasattr(self, 'openai_model'):
                    self.openai_model = openai_details['model_used']
            
            # Calculate aggregated score (40% fuzzy + 60% OpenAI)
            aggregated_score = (fuzzy_score * 0.4) + (openai_score * 0.6)
            tier_details['aggregated_score'] = round(aggregated_score, 2)
            
            # Tier 2 should never be worse than Tier 1 alone
            # If fuzzy would have matched at 85%, keep that decision
            if fuzzy_score >= 85.0:
                # Tier 1 would have matched, so at minimum we match
                is_match = True
                tier_details['final_decision'] = 'Matched (Tier 1 baseline met)'
            else:
                # Tier 1 wouldn't have matched, use aggregated score
                is_match = aggregated_score >= 85.0
                tier_details['final_decision'] = 'Matched at Tier 2' if is_match else 'Needs Review'
            
            return is_match, aggregated_score, tier_details
            
        except Exception as e:
            print(f"    OpenAI API error: {str(e)}")
            # Fall back to fuzzy score if OpenAI fails
            tier_details['tier2_error'] = str(e)
            tier_details['final_decision'] = 'OpenAI failed, using Tier 1 only'
            return fuzzy_score >= 85.0, fuzzy_score, tier_details
    
    def run_tests(self):
        """Run matching algorithm on all test pairs."""
        print(f"Running {self.algorithm} matching on {len(self.test_df)} test pairs...")
        
        for idx, row in self.test_df.iterrows():
            variant = row['variant_name']
            reference = row['reference_name']
            # Handle both string and boolean types for expected_match
            if isinstance(row['expected_match'], bool):
                expected = row['expected_match']
            else:
                expected = str(row['expected_match']).upper() == 'TRUE'
            
            # Run matching based on selected algorithm
            if self.algorithm == 'fuzzy':
                predicted, score = self.run_fuzzy_matching(variant, reference)
                tier_info = {}
            elif self.algorithm == 'simple':
                predicted, score = self.run_simple_matching(variant, reference)
                tier_info = {}
            elif self.algorithm == 'tier2':
                predicted, score, tier_info = self.run_tier2_matching(variant, reference)
            else:
                # Default to fuzzy for now
                predicted, score = self.run_fuzzy_matching(variant, reference)
                tier_info = {}
            
            # Store results
            result_entry = {
                'reference_id': row.get('reference_id', ''),  # Include ID if present
                'reference_name': reference,
                'variant_name': variant,
                'expected_match': row['expected_match'],  # Keep as string 'TRUE' or 'FALSE'
                'predicted_match': 'TRUE' if predicted else 'FALSE',
                'confidence_score': round(score, 2),
                'variant_type': row['variant_type'],
                'correct': predicted == expected
            }
            
            # Add tier information if available
            if tier_info:
                result_entry['tier_reached'] = tier_info.get('tier_reached', 1)
                result_entry['tier1_score'] = tier_info.get('tier1_score')
                result_entry['tier2_score'] = tier_info.get('tier2_score')
                result_entry['aggregated_score'] = tier_info.get('aggregated_score')
                result_entry['model_used'] = tier_info.get('model_used')
            
            self.results.append(result_entry)
            
            # Progress indicator - more frequent for tier2
            if self.algorithm == 'tier2' and (idx + 1) % 10 == 0:
                print(f"  Processed {idx + 1}/{len(self.test_df)} pairs (API calls: {self.tier_stats['api_calls']})...")
            elif self.algorithm != 'tier2' and (idx + 1) % 100 == 0:
                print(f"  Processed {idx + 1}/{len(self.test_df)} pairs...")
        
        print(f"Completed processing {len(self.test_df)} pairs")
        
    def save_results(self, output_dir: str = 'test_data'):
        """Save test results to CSV."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create more descriptive algorithm names
        if self.algorithm == 'tier2':
            # Include model name in filename
            model_suffix = getattr(self, 'openai_model', 'gpt-4o-mini').replace('-', '_').replace('.', '_')
            algo_name = f'tier2_fuzzy_{model_suffix}_90pct'
        else:
            algo_names = {
                'fuzzy': 'fuzzy_matching_85pct_threshold',
                'simple': 'simple_exact_matching',
                'multi_tier': 'multi_tier_fuzzy_openai_web'
            }
            algo_name = algo_names.get(self.algorithm, self.algorithm)
        
        output_file = os.path.join(output_dir, f'test_results_{algo_name}_{timestamp}.csv')
        
        results_df = pd.DataFrame(self.results)
        results_df.to_csv(output_file, index=False)
        
        print(f"Results saved to {output_file}")
        
        # Also save as latest for easy access
        latest_file = os.path.join(output_dir, f'test_results_{algo_name}_latest.csv')
        results_df.to_csv(latest_file, index=False)
        
        return output_file
    
    def print_summary(self):
        """Print summary statistics."""
        results_df = pd.DataFrame(self.results)
        
        total = len(results_df)
        correct = results_df['correct'].sum()
        accuracy = (correct / total) * 100
        
        # Calculate metrics for true/false positives/negatives
        # Handle both string and boolean types
        if results_df['expected_match'].dtype == 'bool':
            expected_true = results_df['expected_match'] == True
            expected_false = results_df['expected_match'] == False
        else:
            expected_true = results_df['expected_match'] == 'TRUE'
            expected_false = results_df['expected_match'] == 'FALSE'
            
        if results_df['predicted_match'].dtype == 'bool':
            predicted_true = results_df['predicted_match'] == True
            predicted_false = results_df['predicted_match'] == False
        else:
            predicted_true = results_df['predicted_match'] == 'TRUE'
            predicted_false = results_df['predicted_match'] == 'FALSE'
        
        tp = (expected_true & predicted_true).sum()
        fp = (expected_false & predicted_true).sum()
        tn = (expected_false & predicted_false).sum()
        fn = (expected_true & predicted_false).sum()
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
        
        print("\n" + "="*50)
        # Create more descriptive summary titles
        if self.algorithm == 'tier2':
            model_name = getattr(self, 'openai_model', 'gpt-4o-mini')
            print(f"TEST RESULTS SUMMARY - TIER2 (FUZZY + {model_name.upper()})")
        else:
            print(f"TEST RESULTS SUMMARY - {self.algorithm.upper()}")
        print("="*50)
        print(f"Total test pairs: {total}")
        print(f"Correct predictions: {correct}")
        print(f"Overall Accuracy: {accuracy:.2f}%")
        print(f"\nConfusion Matrix:")
        print(f"  True Positives:  {tp}")
        print(f"  False Positives: {fp}")
        print(f"  True Negatives:  {tn}")
        print(f"  False Negatives: {fn}")
        print(f"\nMetrics:")
        print(f"  Precision: {precision:.3f}")
        print(f"  Recall:    {recall:.3f}")
        print(f"  F1 Score:  {f1:.3f}")
        
        # Performance by variant type
        print(f"\nAccuracy by Variant Type:")
        for vtype in results_df['variant_type'].unique():
            type_df = results_df[results_df['variant_type'] == vtype]
            type_accuracy = (type_df['correct'].sum() / len(type_df)) * 100
            print(f"  {vtype:25s}: {type_accuracy:6.2f}% ({len(type_df)} samples)")
        
        # Confidence score analysis
        print(f"\nConfidence Score Analysis:")
        print(f"  Mean confidence (correct):   {results_df[results_df['correct']]['confidence_score'].mean():.2f}")
        print(f"  Mean confidence (incorrect): {results_df[~results_df['correct']]['confidence_score'].mean():.2f}")
        
        # Tier statistics if using tier2
        if self.algorithm == 'tier2':
            print(f"\nTier Usage Statistics:")
            print(f"  Matched at Tier 1 (fuzzy only): {self.tier_stats['tier1_only']}")
            print(f"  Required Tier 2 (OpenAI):       {self.tier_stats['tier2_used']}")
            print(f"  Total API calls made:           {self.tier_stats['api_calls']}")
            if self.tier_stats['api_calls'] > 0:
                # Cost estimates per model (per 1K tokens, rough average)
                model_used = getattr(self, 'openai_model', 'gpt-4o-mini')
                cost_per_call = {
                    'gpt-4o-mini': 0.0002,
                    'gpt-4o': 0.005,
                    'gpt-4': 0.03,
                    'gpt-3.5-turbo': 0.0015,
                    'gpt-5-nano': 0.0001,  # Estimated - likely cheaper than 4o-mini
                    'gpt-5-mini': 0.0003,  # Estimated
                    'gpt-5': 0.01  # Estimated
                }.get(model_used, 0.0002)
                estimated_cost = self.tier_stats['api_calls'] * cost_per_call
                print(f"  Model used:                     {getattr(self, 'openai_model', 'gpt-4o-mini')}")
                print(f"  Estimated API cost:             ${estimated_cost:.4f}")
        
        print("="*50)


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Run name matching tests')
    parser.add_argument('--test-file', default='test_data/test_dataset.csv',
                       help='Path to test dataset CSV')
    parser.add_argument('--algorithm', default='fuzzy',
                       choices=['fuzzy', 'simple', 'tier2', 'multi_tier'],
                       help='Matching algorithm to use')
    parser.add_argument('--output-dir', default='test_data',
                       help='Directory to save results')
    parser.add_argument('--model', default=None,
                       help='OpenAI model to use for Tier 2/3 (e.g., gpt-4o-mini, gpt-4o, gpt-5-nano)')
    
    args = parser.parse_args()
    
    # Set the model environment variable if specified
    if args.model:
        os.environ['TIER2_MODEL'] = args.model
        os.environ['TIER3_MODEL'] = args.model
        print(f"Using OpenAI model: {args.model}")
    
    # Check if test file exists
    if not os.path.exists(args.test_file):
        print(f"Error: Test file not found at {args.test_file}")
        print("Please run test_dataset_generator.py first")
        sys.exit(1)
    
    # Run tests
    runner = TestRunner(args.test_file, args.algorithm)
    runner.run_tests()
    runner.save_results(args.output_dir)
    runner.print_summary()


if __name__ == "__main__":
    main()