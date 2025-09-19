#!/usr/bin/env python3
"""
Full population Tier2 matching with true parallelization
Uses batch processing to maximize throughput with 30 concurrent workers
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
import logging
import json
from datetime import datetime
import yaml
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

# Add scripts directory to path so we can import tier2_matcher
sys.path.insert(0, str(Path(__file__).parent))

# Import Tier2 matcher
from tier2_matcher import Tier2NameMatcher

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_config(config_path: Path) -> dict:
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def get_blocking_key(name: str) -> str:
    """
    Get blocking key for a manufacturer name
    Returns the first character (letter or number) in uppercase
    """
    if pd.isna(name) or not name:
        return 'UNKNOWN'

    name = str(name).strip().upper()

    # Remove common prefixes
    for prefix in ['THE ', 'A ', 'AN ']:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break

    if name:
        first_char = name[0]
        if first_char.isdigit():
            return 'NUMERIC'
        elif first_char.isalpha():
            return first_char
        else:
            return 'SPECIAL'

    return 'UNKNOWN'


def prepare_comparison_pairs(op_df: pd.DataFrame, rx_df: pd.DataFrame) -> List[Tuple]:
    """
    Prepare all comparison pairs with blocking
    Returns list of (op_id, op_name, rx_name, blocking_key) tuples
    """

    # Add blocking keys
    op_df['blocking_key'] = op_df['manufacturer_name'].apply(get_blocking_key)
    rx_df['blocking_key'] = rx_df['manufacturer_name'].apply(get_blocking_key)

    # Create all valid pairs with blocking
    pairs = []
    for blocking_key in op_df['blocking_key'].unique():
        if blocking_key not in rx_df['blocking_key'].values:
            continue

        op_block = op_df[op_df['blocking_key'] == blocking_key]
        rx_block = rx_df[rx_df['blocking_key'] == blocking_key]

        for _, op_row in op_block.iterrows():
            for _, rx_row in rx_block.iterrows():
                pairs.append((
                    op_row['manufacturer_id'],
                    op_row['manufacturer_name'],
                    rx_row['manufacturer_name'],
                    blocking_key
                ))

    logger.info(f"Created {len(pairs):,} comparison pairs with blocking")
    return pairs


def process_batch(matcher: Tier2NameMatcher, batch: List[Tuple], use_ai: bool) -> List[Dict]:
    """
    Process a batch of comparisons
    """
    results = []
    for op_id, op_name, rx_name, blocking_key in batch:
        try:
            # Run Tier2 matching
            match_result = matcher.match_pair(op_name, rx_name, use_ai=use_ai)

            # Add metadata
            match_result['op_manufacturer_id'] = op_id
            match_result['op_manufacturer_name'] = op_name
            match_result['rx_manufacturer_name'] = rx_name
            match_result['blocking_key'] = blocking_key

            results.append(match_result)
        except Exception as e:
            logger.error(f"Error matching {op_name} with {rx_name}: {e}")
            continue

    return results


def run_tier2_full_parallel(op_df: pd.DataFrame, rx_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Run Tier2 matching with true parallel processing on full population
    """

    # Get matching configuration
    match_config = config['matching']

    # Initialize Tier2 matcher
    matcher = Tier2NameMatcher(
        fuzzy_threshold=match_config['fuzzy_threshold'],
        decision_threshold=match_config['decision_threshold'],
        model=match_config['ai_model'],
        max_workers=1  # Each matcher instance uses 1 worker, we parallelize at higher level
    )

    logger.info("Starting FULL POPULATION parallel Tier2 name matching...")
    logger.info(f"Configuration:")
    logger.info(f"  Fuzzy threshold: {match_config['fuzzy_threshold']}")
    logger.info(f"  Decision threshold: {match_config['decision_threshold']}")
    logger.info(f"  AI model: {match_config['ai_model']}")
    logger.info(f"  Parallel workers: {match_config['max_workers']}")

    # Prepare all comparison pairs
    pairs = prepare_comparison_pairs(op_df.copy(), rx_df.copy())

    # Split pairs into batches for parallel processing
    batch_size = max(1, len(pairs) // (match_config['max_workers'] * 20))  # Create more batches than workers
    batches = [pairs[i:i+batch_size] for i in range(0, len(pairs), batch_size)]
    logger.info(f"Split into {len(batches)} batches of ~{batch_size} comparisons each")

    # Process batches in parallel
    all_results = []
    start_time = time.time()
    last_log_time = time.time()

    with ThreadPoolExecutor(max_workers=match_config['max_workers']) as executor:
        # Submit all batches
        futures = {
            executor.submit(process_batch, matcher, batch, config['processing']['use_ai_enhancement']): i
            for i, batch in enumerate(batches)
        }

        completed = 0
        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                batch_results = future.result()
                all_results.extend(batch_results)
                completed += 1

                # Progress logging every 10 seconds
                current_time = time.time()
                if current_time - last_log_time >= 10:
                    elapsed = current_time - start_time
                    progress_pct = (completed / len(batches)) * 100
                    rate = len(all_results) / elapsed if elapsed > 0 else 0
                    eta = (len(pairs) - len(all_results)) / rate if rate > 0 else 0

                    logger.info(f"Progress: {completed}/{len(batches)} batches ({progress_pct:.1f}%) | "
                               f"Comparisons: {len(all_results):,}/{len(pairs):,} | "
                               f"Rate: {rate:.0f} comp/sec | ETA: {eta/60:.1f} min")

                    last_log_time = current_time

            except Exception as e:
                logger.error(f"Batch {batch_idx} failed: {e}")

    total_time = time.time() - start_time

    # Filter results by decision threshold and group by OP manufacturer
    logger.info("Filtering and ranking matches...")
    filtered_results = []
    op_matches = {}

    for result in all_results:
        if result['final_score'] >= match_config['decision_threshold']:
            op_id = result['op_manufacturer_id']
            if op_id not in op_matches:
                op_matches[op_id] = []
            op_matches[op_id].append(result)

    # Take top N matches per OP manufacturer
    for op_id, matches in op_matches.items():
        # Sort by score
        matches.sort(key=lambda x: x['final_score'], reverse=True)
        # Take top N and add rank
        for rank, match in enumerate(matches[:match_config['top_n_matches']], 1):
            match['match_rank'] = rank
            filtered_results.append(match)

    # Get statistics
    stats = matcher.get_statistics()

    logger.info(f"\n{'='*60}")
    logger.info(f"FULL POPULATION MATCHING COMPLETE")
    logger.info(f"{'='*60}")
    logger.info(f"Runtime: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
    logger.info(f"Total comparisons: {len(all_results):,}")
    logger.info(f"Processing rate: {len(all_results)/total_time:.1f} comparisons/second")
    logger.info(f"Matches found: {len(filtered_results)}")
    logger.info(f"Unique OP manufacturers matched: {len(op_matches)}")
    logger.info(f"\nMatching Statistics:")
    logger.info(f"  Fuzzy only: {stats['fuzzy_only']} ({stats['fuzzy_only_pct']:.1f}%)")
    logger.info(f"  AI enhanced: {stats['ai_enhanced']} ({stats['ai_enhanced_pct']:.1f}%)")

    # Calculate API cost
    api_calls = stats['ai_enhanced']
    cost = (api_calls / 1000) * 0.15  # $0.15 per 1000 for gpt-4o-mini
    logger.info(f"\nAPI Usage:")
    logger.info(f"  Total API calls: {api_calls:,}")
    logger.info(f"  Estimated cost: ${cost:.2f}")

    return pd.DataFrame(filtered_results)


def save_results(results_df: pd.DataFrame, output_dir: Path, runtime: float, op_total: int):
    """Save matching results"""

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if results_df.empty:
        logger.warning("No matches found!")
        return {}

    # Reorder columns
    column_order = [
        'op_manufacturer_id',
        'op_manufacturer_name',
        'rx_manufacturer_name',
        'match_rank',
        'final_score',
        'is_match',
        'confidence_source',
        'fuzzy_score',
        'ai_score',
        'blocking_key'
    ]

    columns_to_use = [col for col in column_order if col in results_df.columns]
    results_df = results_df[columns_to_use]

    # Save full results
    full_file = output_dir / f"tier2_full_population_{timestamp}.csv"
    results_df.to_csv(full_file, index=False)
    logger.info(f"Saved {len(results_df)} matches to {full_file}")

    # Save best matches only
    best_matches = results_df[results_df['match_rank'] == 1].copy()
    best_file = output_dir / f"tier2_full_best_matches_{timestamp}.csv"
    best_matches.to_csv(best_file, index=False)
    logger.info(f"Saved {len(best_matches)} best matches to {best_file}")

    # Create summary
    summary = {
        "timestamp": timestamp,
        "method": "Tier2 Parallel (Full Population)",
        "runtime_seconds": runtime,
        "runtime_minutes": runtime / 60,
        "total_op_manufacturers": op_total,
        "op_manufacturers_matched": len(results_df['op_manufacturer_id'].unique()),
        "match_rate": f"{len(results_df['op_manufacturer_id'].unique()) / op_total * 100:.1f}%",
        "total_matches": len(results_df),
        "best_matches": len(best_matches),
        "avg_final_score": float(results_df['final_score'].mean()),
        "confidence_sources": results_df['confidence_source'].value_counts().to_dict(),
        "score_distribution": {
            "95-100": len(results_df[results_df['final_score'] >= 95]),
            "90-95": len(results_df[(results_df['final_score'] >= 90) & (results_df['final_score'] < 95)]),
            "85-90": len(results_df[(results_df['final_score'] >= 85) & (results_df['final_score'] < 90)]),
            "80-85": len(results_df[(results_df['final_score'] >= 80) & (results_df['final_score'] < 85)]),
            "70-80": len(results_df[(results_df['final_score'] >= 70) & (results_df['final_score'] < 80)]),
            "60-70": len(results_df[(results_df['final_score'] >= 60) & (results_df['final_score'] < 70)]),
            "50-60": len(results_df[(results_df['final_score'] >= 50) & (results_df['final_score'] < 60)])
        }
    }

    # Save summary
    summary_file = output_dir / f"tier2_full_summary_{timestamp}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    logger.info(f"Saved summary to {summary_file}")

    return summary


def main():
    """Main execution"""

    logger.info("="*60)
    logger.info("TIER2 FULL POPULATION MATCHING")
    logger.info("="*60)

    # Set paths
    project_dir = Path(__file__).parent.parent
    config_path = project_dir / "config.yaml"
    input_dir = project_dir / "data" / "input"
    output_dir = project_dir / "data" / "output"

    # Load configuration
    config = load_config(config_path)

    # Load FULL data
    op_df = pd.read_csv(input_dir / "op_manufacturers.csv")
    rx_df = pd.read_csv(input_dir / "rx_manufacturers.csv")

    logger.info(f"Loaded {len(op_df)} OP manufacturers and {len(rx_df)} RX manufacturers")

    # Run parallel matching
    start_time = time.time()
    results_df = run_tier2_full_parallel(op_df, rx_df, config)
    runtime = time.time() - start_time

    # Save results
    if not results_df.empty:
        summary = save_results(results_df, output_dir, runtime, len(op_df))

        # Print summary
        logger.info("\n" + "="*60)
        logger.info("FINAL SUMMARY")
        logger.info("="*60)
        logger.info(f"Runtime: {summary['runtime_minutes']:.1f} minutes")
        logger.info(f"Match rate: {summary['match_rate']}")
        logger.info(f"Total matches: {summary['total_matches']}")
        logger.info(f"Best matches: {summary['best_matches']}")
        logger.info(f"Average score: {summary['avg_final_score']:.1f}")
    else:
        logger.warning("No matches found!")


if __name__ == "__main__":
    main()