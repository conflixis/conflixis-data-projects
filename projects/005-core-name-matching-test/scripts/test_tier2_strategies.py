#!/usr/bin/env python3
"""
Test different combination strategies for Tier 2 matching
Compares 5 different ways to combine fuzzy and OpenAI scores
"""

import os
import sys
import time
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.tier1_fuzzy import fuzzy_match
from src.tier2_openai import openai_match

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


def strategy_weighted_current(fuzzy_score, openai_score, fuzzy_threshold=85.0):
    """Strategy 1: Current weighted combination (40% fuzzy + 60% OpenAI)"""
    if fuzzy_score >= fuzzy_threshold:
        return fuzzy_score, "fuzzy_high_conf"
    else:
        return 0.4 * fuzzy_score + 0.6 * openai_score, "weighted"


def strategy_pure_cascade(fuzzy_score, openai_score, fuzzy_threshold=85.0):
    """Strategy 2: Pure cascade - trust fuzzy above threshold, else trust OpenAI completely"""
    if fuzzy_score >= fuzzy_threshold:
        return fuzzy_score, "fuzzy_high_conf"
    else:
        return openai_score, "openai_only"


def strategy_confidence_weighted(fuzzy_score, openai_score, fuzzy_threshold=85.0):
    """Strategy 3: Confidence-weighted combination"""
    if fuzzy_score >= fuzzy_threshold:
        return fuzzy_score, "fuzzy_high_conf"
    elif fuzzy_score < 30:
        return openai_score, "openai_low_fuzzy"
    else:
        # Weight by how close fuzzy is to the threshold
        weight = (fuzzy_score - 30) / 55  # Normalize 30-85 to 0-1
        final = weight * fuzzy_score + (1 - weight) * openai_score
        return final, "confidence_weighted"


def strategy_maximum_confidence(fuzzy_score, openai_score, fuzzy_threshold=85.0):
    """Strategy 4: Maximum confidence - take the higher score"""
    if fuzzy_score >= fuzzy_threshold:
        return fuzzy_score, "fuzzy_high_conf"
    else:
        if fuzzy_score > openai_score:
            return fuzzy_score, "max_fuzzy"
        else:
            return openai_score, "max_openai"


def strategy_bayesian(fuzzy_score, openai_score, fuzzy_threshold=85.0):
    """Strategy 5: Bayesian combination"""
    if fuzzy_score >= fuzzy_threshold:
        return fuzzy_score, "fuzzy_high_conf"
    else:
        # Convert to probabilities
        p_fuzzy = fuzzy_score / 100
        p_openai = openai_score / 100
        
        # Avoid division by zero
        if p_fuzzy == 0 and p_openai == 0:
            return 0, "bayesian_both_zero"
        elif p_fuzzy == 1 and p_openai == 1:
            return 100, "bayesian_both_one"
        else:
            # Bayesian combination assuming independence
            numerator = p_fuzzy * p_openai
            denominator = numerator + (1 - p_fuzzy) * (1 - p_openai)
            if denominator == 0:
                return 50, "bayesian_undefined"
            p_match = numerator / denominator
            return p_match * 100, "bayesian"


def process_batch_concurrent(batch: List[Tuple[str, str]], model: str, max_workers: int = 10) -> List[Dict]:
    """Process a batch of name pairs concurrently with OpenAI"""
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
            print(f"Error processing {name_a} vs {name_b}: {e}")
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


def test_all_strategies(test_file: str, model: str = 'gpt-4o-mini', 
                        fuzzy_threshold: float = 85.0,
                        decision_threshold: float = 50.0,
                        max_workers: int = 10):
    """Test all combination strategies on the same dataset"""
    
    print("=" * 80)
    print("TIER 2 COMBINATION STRATEGIES TEST")
    print("=" * 80)
    
    # Set the model for OpenAI
    os.environ['TIER2_MODEL'] = model
    
    # Load test data
    print(f"\nLoading test data from {test_file}...")
    df = pd.read_csv(test_file)
    total_rows = len(df)
    print(f"Loaded {total_rows} test pairs")
    
    # Phase 1: Run Tier 1 (fuzzy) on all pairs
    print(f"\nPhase 1: Running Tier 1 (Fuzzy) matching on all {total_rows} pairs...")
    tier1_results = []
    tier1_start = time.time()
    
    for idx, row in df.iterrows():
        if idx % 100 == 0:
            print(f"  Processed {idx}/{total_rows} pairs...")
        
        fuzzy_score, _ = fuzzy_match(row['reference_name'], row['variant_name'])
        tier1_results.append({
            'index': idx,
            'name_a': row['reference_name'],
            'name_b': row['variant_name'],
            'expected_match': row['expected_match'],
            'variant_type': row.get('variant_type', 'unknown'),
            'fuzzy_score': fuzzy_score,
            'fuzzy_passed': fuzzy_score >= fuzzy_threshold
        })
    
    tier1_time = time.time() - tier1_start
    tier1_df = pd.DataFrame(tier1_results)
    
    # Identify cases that need Tier 2
    needs_tier2 = tier1_df[tier1_df['fuzzy_score'] < fuzzy_threshold]
    tier1_passed = tier1_df[tier1_df['fuzzy_score'] >= fuzzy_threshold]
    
    print(f"\nTier 1 Results:")
    print(f"  - Completed in {tier1_time:.2f}s")
    print(f"  - High confidence (>={fuzzy_threshold}%): {len(tier1_passed)} cases")
    print(f"  - Need Tier 2 (<{fuzzy_threshold}%): {len(needs_tier2)} cases")
    
    # Phase 2: Run OpenAI on low-confidence cases
    if len(needs_tier2) > 0:
        print(f"\nPhase 2: Running OpenAI ({model}) on {len(needs_tier2)} low-confidence cases...")
        print(f"  Using {max_workers} concurrent workers")
        
        # Prepare batch
        name_pairs = [(row['name_a'], row['name_b']) for _, row in needs_tier2.iterrows()]
        
        tier2_start = time.time()
        batch_size = max_workers * 2
        all_tier2_results = []
        
        for i in range(0, len(name_pairs), batch_size):
            batch_end = min(i + batch_size, len(name_pairs))
            batch = name_pairs[i:batch_end]
            
            print(f"  Processing batch: cases {i+1} to {batch_end} of {len(name_pairs)}")
            batch_results = process_batch_concurrent(batch, model, max_workers)
            all_tier2_results.extend(batch_results)
        
        tier2_time = time.time() - tier2_start
        print(f"  OpenAI processing completed in {tier2_time:.2f}s")
        
        # Create lookup for OpenAI results
        openai_lookup = {(r['name_a'], r['name_b']): r['openai_score'] for r in all_tier2_results}
    else:
        print("\nNo cases need Tier 2 - all passed Tier 1!")
        openai_lookup = {}
        tier2_time = 0
    
    # Phase 3: Apply all strategies and compare
    print("\nPhase 3: Applying combination strategies...")
    
    strategies = [
        ("Weighted (Current)", strategy_weighted_current),
        ("Pure Cascade", strategy_pure_cascade),
        ("Confidence Weighted", strategy_confidence_weighted),
        ("Maximum Confidence", strategy_maximum_confidence),
        ("Bayesian", strategy_bayesian)
    ]
    
    results_by_strategy = {}
    
    for strategy_name, strategy_func in strategies:
        print(f"\n  Testing {strategy_name}...")
        
        predictions = []
        scores = []
        sources = []
        
        for _, row in tier1_df.iterrows():
            fuzzy_score = row['fuzzy_score']
            
            # Get OpenAI score if available
            key = (row['name_a'], row['name_b'])
            openai_score = openai_lookup.get(key, 0)
            
            # Apply strategy
            final_score, source = strategy_func(fuzzy_score, openai_score, fuzzy_threshold)
            
            scores.append(final_score)
            sources.append(source)
            predictions.append(1 if final_score >= decision_threshold else 0)
        
        # Convert expected_match to binary
        y_true = tier1_df['expected_match'].apply(
            lambda x: 1 if x in [True, 'True', 'TRUE', 'true', 1, '1', 'yes', 'Yes', 'YES'] else 0
        ).values
        
        # Calculate metrics
        metrics = calculate_metrics(y_true, predictions)
        
        # Store results
        results_by_strategy[strategy_name] = {
            'metrics': metrics,
            'scores': scores,
            'sources': sources,
            'predictions': predictions
        }
        
        print(f"    Accuracy: {metrics['accuracy']:.4f}")
        print(f"    Precision: {metrics['precision']:.4f}")
        print(f"    Recall: {metrics['recall']:.4f}")
        print(f"    F1 Score: {metrics['f1_score']:.4f}")
    
    # Phase 4: Generate comparison report
    print("\n" + "=" * 80)
    print("RESULTS COMPARISON")
    print("=" * 80)
    
    # Create comparison dataframe
    comparison_data = []
    for strategy_name, results in results_by_strategy.items():
        metrics = results['metrics']
        comparison_data.append({
            'Strategy': strategy_name,
            'Accuracy': f"{metrics['accuracy']:.4f}",
            'Precision': f"{metrics['precision']:.4f}",
            'Recall': f"{metrics['recall']:.4f}",
            'F1 Score': f"{metrics['f1_score']:.4f}",
            'TP': metrics['true_positives'],
            'TN': metrics['true_negatives'],
            'FP': metrics['false_positives'],
            'FN': metrics['false_negatives']
        })
    
    comparison_df = pd.DataFrame(comparison_data)
    
    # Print comparison table
    print("\n" + comparison_df.to_string(index=False))
    
    # Save detailed results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save strategy comparison
    comparison_file = f"test-data/test-results/tier2_strategies_comparison_{timestamp}.csv"
    comparison_df.to_csv(comparison_file, index=False)
    print(f"\nComparison saved to: {comparison_file}")
    
    # Save detailed results for each strategy
    for strategy_name, results in results_by_strategy.items():
        strategy_df = tier1_df.copy()
        strategy_df['final_score'] = results['scores']
        strategy_df['predicted_match'] = results['predictions']
        strategy_df['decision_source'] = results['sources']
        strategy_df['openai_score'] = [openai_lookup.get((row['name_a'], row['name_b']), None) 
                                        for _, row in strategy_df.iterrows()]
        
        # Clean strategy name for filename
        clean_name = strategy_name.replace(" ", "_").replace("(", "").replace(")", "").lower()
        detail_file = f"test-data/test-results/tier2_strategy_{clean_name}_{timestamp}.csv"
        strategy_df.to_csv(detail_file, index=False)
        print(f"  {strategy_name} details saved to: {detail_file}")
    
    # Generate HTML report
    generate_html_report(comparison_df, results_by_strategy, tier1_df, timestamp)
    
    print("\n" + "=" * 80)
    print("TEST COMPLETE")
    print("=" * 80)
    print(f"Total time: {tier1_time + tier2_time:.2f}s")
    print(f"  - Tier 1 time: {tier1_time:.2f}s")
    print(f"  - Tier 2 time: {tier2_time:.2f}s")
    
    # Find best strategy
    best_strategy = comparison_df.loc[comparison_df['Accuracy'].astype(float).idxmax(), 'Strategy']
    best_accuracy = comparison_df['Accuracy'].astype(float).max()
    print(f"\nBest Strategy: {best_strategy} with {best_accuracy:.4f} accuracy")
    
    return comparison_df, results_by_strategy


def generate_html_report(comparison_df, results_by_strategy, tier1_df, timestamp):
    """Generate an HTML comparison report"""
    
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Tier 2 Strategies Comparison Report</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }
        h1, h2 {
            color: #0c343a;
        }
        .summary-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }
        .card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .card h3 {
            margin-top: 0;
            color: #0c343a;
            font-size: 18px;
        }
        .metric {
            font-size: 32px;
            font-weight: bold;
            color: #eab96d;
            margin: 10px 0;
        }
        .comparison-table {
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin: 30px 0;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th {
            background: #0c343a;
            color: white;
            padding: 12px;
            text-align: left;
        }
        td {
            padding: 12px;
            border-bottom: 1px solid #eee;
        }
        tr:hover {
            background: #f9f9f9;
        }
        .best-row {
            background: #fff9e6 !important;
            font-weight: bold;
        }
        .timestamp {
            color: #666;
            font-size: 14px;
            margin-top: 20px;
        }
    </style>
</head>
<body>
    <h1>🔬 Tier 2 Combination Strategies Comparison</h1>
    <p class="timestamp">Generated: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
    
    <div class="summary-cards">
"""
    
    # Add summary cards for each strategy
    for strategy_name, results in results_by_strategy.items():
        metrics = results['metrics']
        html_content += f"""
        <div class="card">
            <h3>{strategy_name}</h3>
            <div class="metric">{metrics['accuracy']:.2%}</div>
            <p>Accuracy</p>
            <div style="font-size: 14px; color: #666;">
                P: {metrics['precision']:.3f} | R: {metrics['recall']:.3f} | F1: {metrics['f1_score']:.3f}
            </div>
        </div>
"""
    
    html_content += """
    </div>
    
    <h2>📊 Detailed Comparison</h2>
    <div class="comparison-table">
        <table>
            <thead>
                <tr>
                    <th>Strategy</th>
                    <th>Accuracy</th>
                    <th>Precision</th>
                    <th>Recall</th>
                    <th>F1 Score</th>
                    <th>True Pos</th>
                    <th>True Neg</th>
                    <th>False Pos</th>
                    <th>False Neg</th>
                </tr>
            </thead>
            <tbody>
"""
    
    # Find best accuracy for highlighting
    best_accuracy = comparison_df['Accuracy'].astype(float).max()
    
    # Add table rows
    for _, row in comparison_df.iterrows():
        row_class = 'class="best-row"' if float(row['Accuracy']) == best_accuracy else ''
        html_content += f"""
                <tr {row_class}>
                    <td>{row['Strategy']}</td>
                    <td>{row['Accuracy']}</td>
                    <td>{row['Precision']}</td>
                    <td>{row['Recall']}</td>
                    <td>{row['F1 Score']}</td>
                    <td>{row['TP']}</td>
                    <td>{row['TN']}</td>
                    <td>{row['FP']}</td>
                    <td>{row['FN']}</td>
                </tr>
"""
    
    html_content += """
            </tbody>
        </table>
    </div>
    
    <h2>📈 Key Insights</h2>
    <div class="card">
        <ul>
            <li><strong>Best Overall:</strong> """ + comparison_df.loc[comparison_df['Accuracy'].astype(float).idxmax(), 'Strategy'] + """ (""" + f"{best_accuracy:.4f}" + """ accuracy)</li>
            <li><strong>Best Precision:</strong> """ + comparison_df.loc[comparison_df['Precision'].astype(float).idxmax(), 'Strategy'] + """ (""" + comparison_df['Precision'].astype(float).max().astype(str) + """)</li>
            <li><strong>Best Recall:</strong> """ + comparison_df.loc[comparison_df['Recall'].astype(float).idxmax(), 'Strategy'] + """ (""" + comparison_df['Recall'].astype(float).max().astype(str) + """)</li>
            <li><strong>Best F1:</strong> """ + comparison_df.loc[comparison_df['F1 Score'].astype(float).idxmax(), 'Strategy'] + """ (""" + comparison_df['F1 Score'].astype(float).max().astype(str) + """)</li>
        </ul>
    </div>
    
    <h2>🔍 Strategy Descriptions</h2>
    <div class="card">
        <ul>
            <li><strong>Weighted (Current):</strong> 40% fuzzy + 60% OpenAI for low-confidence cases</li>
            <li><strong>Pure Cascade:</strong> Trust fuzzy above 85%, else trust OpenAI completely</li>
            <li><strong>Confidence Weighted:</strong> Variable weighting based on fuzzy confidence level</li>
            <li><strong>Maximum Confidence:</strong> Take the higher of fuzzy or OpenAI scores</li>
            <li><strong>Bayesian:</strong> Probabilistic combination using Bayes' rule</li>
        </ul>
    </div>
</body>
</html>
"""
    
    # Save HTML report
    html_file = f"test-data/reports/tier2_strategies_comparison_{timestamp}.html"
    with open(html_file, 'w') as f:
        f.write(html_content)
    
    print(f"\nHTML report saved to: {html_file}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-file', default='test-data/test-data-inputs/test_dataset.csv',
                       help='Path to test dataset CSV')
    parser.add_argument('--model', default='gpt-4o-mini',
                       help='OpenAI model to use')
    parser.add_argument('--fuzzy-threshold', type=float, default=85.0,
                       help='Fuzzy score threshold for Tier 1 (default: 85.0)')
    parser.add_argument('--decision-threshold', type=float, default=50.0,
                       help='Final score threshold for match decision (default: 50.0)')
    parser.add_argument('--max-workers', type=int, default=10,
                       help='Maximum concurrent OpenAI requests (default: 10)')
    
    args = parser.parse_args()
    
    test_all_strategies(
        args.test_file,
        args.model,
        args.fuzzy_threshold,
        args.decision_threshold,
        args.max_workers
    )