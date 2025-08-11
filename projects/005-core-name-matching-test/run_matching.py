#!/usr/bin/env python3
"""
Multi-Tier Name Matching Pipeline
Processes name pairs through progressive tiers of matching algorithms
"""

import pandas as pd
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Import tier implementations
from tier1_fuzzy import fuzzy_match
from tier2_openai import openai_match
from tier3_websearch import websearch_match

# Load environment variables
load_dotenv()


class NameMatchingPipeline:
    """Main orchestrator for multi-tier name matching."""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the matching pipeline.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or self.get_default_config()
        self.results = []
        self.tier_stats = {1: 0, 2: 0, 3: 0, 'review': 0}
        
    def get_default_config(self) -> Dict:
        """Get default configuration settings."""
        return {
            'confidence_threshold': 90.0,
            'tier1_weight': 1.0,  # When only tier 1 is used
            'tier2_weights': {'tier1': 0.4, 'tier2': 0.6},
            'tier3_weights': {'tier1': 0.3, 'tier2': 0.4, 'tier3': 0.3},
            'save_intermediate': True,
            'verbose': True
        }
    
    def process_pair(self, name_a: str, name_b: str, index: int = 0, total: int = 0) -> Dict:
        """
        Process a single name pair through the multi-tier system.
        
        Args:
            name_a: First organization name
            name_b: Second organization name
            index: Current pair index (for progress tracking)
            total: Total number of pairs (for progress tracking)
            
        Returns:
            Dictionary containing match results
        """
        if self.config['verbose'] and total > 0:
            print(f"\n[{index}/{total}] Processing: {name_a} vs {name_b}")
        
        result = {
            'name_a': name_a,
            'name_b': name_b,
            'tier_reached': 0,
            'tier1_score': None,
            'tier2_score': None,
            'tier3_score': None,
            'final_score': 0,
            'status': 'PENDING',
            'method': '',
            'processing_time': datetime.now().isoformat()
        }
        
        # Tier 1: Fuzzy Matching
        if self.config['verbose']:
            print("  Tier 1: Running fuzzy matching...")
        
        tier1_score, tier1_details = fuzzy_match(name_a, name_b)
        result['tier1_score'] = round(tier1_score, 2)
        result['tier1_details'] = tier1_details
        
        if self.config['verbose']:
            print(f"    Score: {tier1_score:.1f}")
        
        if tier1_score >= self.config['confidence_threshold']:
            result['tier_reached'] = 1
            result['final_score'] = round(tier1_score, 2)
            result['status'] = 'MATCHED'
            result['method'] = 'Fuzzy Matching'
            self.tier_stats[1] += 1
            
            if self.config['verbose']:
                print(f"    ✓ MATCHED at Tier 1 (score: {tier1_score:.1f})")
            
            return result
        
        # Tier 2: OpenAI Analysis
        if self.config['verbose']:
            print("  Tier 2: Running OpenAI analysis...")
        
        tier2_score, tier2_details = openai_match(name_a, name_b)
        result['tier2_score'] = round(tier2_score, 2)
        result['tier2_details'] = tier2_details
        
        # Calculate aggregated score
        weights = self.config['tier2_weights']
        agg_score = (
            tier1_score * weights['tier1'] + 
            tier2_score * weights['tier2']
        )
        
        if self.config['verbose']:
            print(f"    Score: {tier2_score:.1f}, Aggregated: {agg_score:.1f}")
        
        if agg_score >= self.config['confidence_threshold']:
            result['tier_reached'] = 2
            result['final_score'] = round(agg_score, 2)
            result['status'] = 'MATCHED'
            result['method'] = 'OpenAI Analysis'
            self.tier_stats[2] += 1
            
            if self.config['verbose']:
                print(f"    ✓ MATCHED at Tier 2 (score: {agg_score:.1f})")
            
            return result
        
        # Tier 3: Web Search Validation
        if self.config['verbose']:
            print("  Tier 3: Running web search validation...")
        
        tier3_score, tier3_details = websearch_match(name_a, name_b)
        result['tier3_score'] = round(tier3_score, 2)
        result['tier3_details'] = tier3_details
        
        # Calculate final aggregated score
        weights = self.config['tier3_weights']
        final_score = (
            tier1_score * weights['tier1'] + 
            tier2_score * weights['tier2'] + 
            tier3_score * weights['tier3']
        )
        
        result['tier_reached'] = 3
        result['final_score'] = round(final_score, 2)
        
        if self.config['verbose']:
            print(f"    Score: {tier3_score:.1f}, Final: {final_score:.1f}")
        
        if final_score >= self.config['confidence_threshold']:
            result['status'] = 'MATCHED'
            result['method'] = 'Web Search Validation'
            self.tier_stats[3] += 1
            
            if self.config['verbose']:
                print(f"    ✓ MATCHED at Tier 3 (score: {final_score:.1f})")
        else:
            result['status'] = 'REVIEW_NEEDED'
            result['method'] = 'Requires Human Review'
            self.tier_stats['review'] += 1
            
            if self.config['verbose']:
                print(f"    ⚠ Needs human review (score: {final_score:.1f})")
        
        return result
    
    def process_file(self, input_file: str) -> pd.DataFrame:
        """
        Process all name pairs from a CSV file.
        
        Args:
            input_file: Path to input CSV with 'name_a' and 'name_b' columns
            
        Returns:
            DataFrame with all results
        """
        # Load input data
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        df = pd.read_csv(input_file)
        
        if 'name_a' not in df.columns or 'name_b' not in df.columns:
            raise ValueError("Input CSV must have 'name_a' and 'name_b' columns")
        
        total_pairs = len(df)
        print(f"\nProcessing {total_pairs} name pairs...")
        print("=" * 60)
        
        # Process each pair
        results = []
        for idx, row in df.iterrows():
            result = self.process_pair(
                row['name_a'], 
                row['name_b'],
                index=idx + 1,
                total=total_pairs
            )
            results.append(result)
        
        # Create results DataFrame
        results_df = pd.DataFrame(results)
        
        # Save results
        self.save_results(results_df)
        
        # Print summary
        self.print_summary(results_df)
        
        return results_df
    
    def save_results(self, results_df: pd.DataFrame):
        """Save results to CSV files."""
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        # Save main results with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_file = f'data/results_{timestamp}.csv'
        results_df.to_csv(results_file, index=False)
        
        # Save latest results (for dashboard)
        results_df.to_csv('data/results_latest.csv', index=False)
        
        # Create review queue
        review_df = results_df[results_df['status'] == 'REVIEW_NEEDED']
        if not review_df.empty:
            review_df.to_csv('data/review_queue.csv', index=False)
        
        print(f"\nResults saved to: {results_file}")
        if not review_df.empty:
            print(f"Review queue saved to: data/review_queue.csv")
    
    def print_summary(self, results_df: pd.DataFrame):
        """Print processing summary."""
        print("\n" + "=" * 60)
        print("PROCESSING COMPLETE")
        print("=" * 60)
        
        total = len(results_df)
        matched = len(results_df[results_df['status'] == 'MATCHED'])
        review = len(results_df[results_df['status'] == 'REVIEW_NEEDED'])
        
        print(f"Total processed: {total}")
        print(f"Total matched: {matched} ({matched/total*100:.1f}%)")
        print(f"  - Matched at Tier 1: {self.tier_stats[1]}")
        print(f"  - Matched at Tier 2: {self.tier_stats[2]}")
        print(f"  - Matched at Tier 3: {self.tier_stats[3]}")
        print(f"Need review: {review} ({review/total*100:.1f}%)")
        
        # Calculate average scores by tier
        for tier in [1, 2, 3]:
            col = f'tier{tier}_score'
            tier_scores = results_df[col].dropna()
            if not tier_scores.empty:
                avg_score = tier_scores.mean()
                print(f"\nAverage Tier {tier} score: {avg_score:.1f}")


def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Multi-tier name matching pipeline')
    parser.add_argument(
        '--input', 
        default='data/input.csv',
        help='Input CSV file with name_a and name_b columns'
    )
    parser.add_argument(
        '--threshold',
        type=float,
        default=90.0,
        help='Confidence threshold for matching (0-100)'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress verbose output'
    )
    
    args = parser.parse_args()
    
    # Configure pipeline
    config = {
        'confidence_threshold': args.threshold,
        'verbose': not args.quiet,
        'tier1_weight': 1.0,
        'tier2_weights': {'tier1': 0.4, 'tier2': 0.6},
        'tier3_weights': {'tier1': 0.3, 'tier2': 0.4, 'tier3': 0.3}
    }
    
    # Check for API key
    if not os.getenv('OPENAI_API_KEY'):
        print("Warning: OPENAI_API_KEY not set in environment or .env file")
        print("Tiers 2 and 3 will not function properly without it.")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            return
    
    # Run pipeline
    pipeline = NameMatchingPipeline(config)
    
    try:
        results = pipeline.process_file(args.input)
        print("\nPipeline completed successfully!")
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure the input file exists.")
        
    except Exception as e:
        print(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()