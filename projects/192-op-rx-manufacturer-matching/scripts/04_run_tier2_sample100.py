#!/usr/bin/env python3
"""
Run Tier2 name matching with LLM enhancement on 100-manufacturer sample
Optimized version for testing with reduced dataset
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


def create_blocking_groups(op_df: pd.DataFrame, rx_df: pd.DataFrame):
    """Create blocking groups to reduce unnecessary comparisons"""

    # Add blocking keys
    op_df['blocking_key'] = op_df['manufacturer_name'].apply(get_blocking_key)
    rx_df['blocking_key'] = rx_df['manufacturer_name'].apply(get_blocking_key)

    # Group by blocking key
    op_groups = op_df.groupby('blocking_key')
    rx_groups = rx_df.groupby('blocking_key')

    # Calculate statistics
    total_comparisons = 0
    for key in op_df['blocking_key'].unique():
        if key in rx_df['blocking_key'].values:
            op_count = len(op_df[op_df['blocking_key'] == key])
            rx_count = len(rx_df[rx_df['blocking_key'] == key])
            total_comparisons += op_count * rx_count

    naive_comparisons = len(op_df) * len(rx_df)
    reduction = 1 - (total_comparisons / naive_comparisons)

    logger.info(f"Blocking statistics:")
    logger.info(f"  OP manufacturers grouped into {len(op_groups)} blocks")
    logger.info(f"  RX manufacturers grouped into {len(rx_groups)} blocks")
    logger.info(f"  Total comparisons: {total_comparisons:,} (vs {naive_comparisons:,} naive)")
    logger.info(f"  Reduction: {reduction:.1%}")

    return op_groups, rx_groups, total_comparisons


def run_tier2_matching_sample100(op_df: pd.DataFrame, rx_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Run Tier2 matching with blocking and LLM enhancement on 100-sample
    """

    # Get matching configuration
    match_config = config['matching']

    # Initialize Tier2 matcher with optimized settings
    matcher = Tier2NameMatcher(
        fuzzy_threshold=match_config['fuzzy_threshold'],
        decision_threshold=match_config['decision_threshold'],
        model=match_config['ai_model'],
        max_workers=match_config['max_workers']  # Now 30 workers
    )

    logger.info("Starting Tier2 name matching with LLM enhancement (100-sample)...")
    logger.info(f"Configuration:")
    logger.info(f"  Fuzzy threshold: {match_config['fuzzy_threshold']} (use fuzzy only if >= this)")
    logger.info(f"  Decision threshold: {match_config['decision_threshold']} (min score to accept)")
    logger.info(f"  AI model: {match_config['ai_model']}")
    logger.info(f"  Max workers: {match_config['max_workers']}")
    logger.info(f"  LLM enhancement: {config['processing']['use_ai_enhancement']}")

    # Create blocking groups
    op_groups, rx_groups, total_comparisons = create_blocking_groups(op_df.copy(), rx_df.copy())

    # Prepare results
    all_matches = []
    total_processed = 0
    start_time = time.time()

    # Track progress for API cost estimation
    api_calls_made = 0
    fuzzy_only_count = 0

    # Process each blocking group
    for blocking_key in sorted(op_groups.groups.keys()):
        if blocking_key not in rx_groups.groups:
            logger.info(f"Skipping block '{blocking_key}': No RX manufacturers")
            continue

        op_block = op_groups.get_group(blocking_key)
        rx_block = rx_groups.get_group(blocking_key)

        block_comparisons = len(op_block) * len(rx_block)
        logger.info(f"\nProcessing block '{blocking_key}': {len(op_block)} × {len(rx_block)} = {block_comparisons} comparisons")

        # For each OP manufacturer in this block
        for _, op_row in op_block.iterrows():
            op_name = op_row['manufacturer_name']
            op_id = op_row['manufacturer_id']

            matches_for_this_op = []

            # Match against RX manufacturers in same block
            for _, rx_row in rx_block.iterrows():
                rx_name = rx_row['manufacturer_name']

                # Run Tier2 matching (fuzzy + AI enhancement)
                match_result = matcher.match_pair(
                    op_name,
                    rx_name,
                    use_ai=config['processing']['use_ai_enhancement']
                )

                # Track API usage
                if match_result.get('confidence_source') == 'ai_enhanced':
                    api_calls_made += 1
                elif match_result.get('confidence_source') == 'fuzzy_only':
                    fuzzy_only_count += 1

                # Only keep matches above decision threshold
                if match_result['final_score'] >= match_config['decision_threshold']:
                    match_result['op_manufacturer_name'] = op_name
                    match_result['op_manufacturer_id'] = op_id
                    match_result['rx_manufacturer_name'] = rx_name
                    match_result['blocking_key'] = blocking_key
                    matches_for_this_op.append(match_result)

            # Sort by final score and take top N
            if matches_for_this_op:
                matches_for_this_op.sort(key=lambda x: x['final_score'], reverse=True)
                top_matches = matches_for_this_op[:match_config['top_n_matches']]

                for rank, match in enumerate(top_matches, 1):
                    match['match_rank'] = rank
                    all_matches.append(match)

            total_processed += 1

            # Log progress every 10 manufacturers
            if total_processed % 10 == 0:
                elapsed = time.time() - start_time
                rate = total_processed / elapsed if elapsed > 0 else 0
                eta = (len(op_df) - total_processed) / rate if rate > 0 else 0
                logger.info(f"  Progress: {total_processed}/{len(op_df)} OP manufacturers")
                logger.info(f"  Speed: {rate:.1f} OP/sec | ETA: {eta:.0f}s")
                logger.info(f"  API calls: {api_calls_made} | Fuzzy only: {fuzzy_only_count}")

    # Calculate final runtime
    total_time = time.time() - start_time

    # Get final statistics
    stats = matcher.get_statistics()
    logger.info(f"\n{'='*60}")
    logger.info(f"MATCHING COMPLETE")
    logger.info(f"{'='*60}")
    logger.info(f"Runtime: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
    logger.info(f"Processing rate: {len(op_df)/total_time:.1f} OP manufacturers/second")
    logger.info(f"\nMatching Statistics:")
    logger.info(f"  Total processed: {stats['total_processed']}")
    logger.info(f"  Fuzzy only (high confidence): {stats['fuzzy_only']} ({stats['fuzzy_only_pct']:.1f}%)")
    logger.info(f"  AI enhanced: {stats['ai_enhanced']} ({stats['ai_enhanced_pct']:.1f}%)")
    if stats['api_errors'] > 0:
        logger.warning(f"  API errors: {stats['api_errors']} ({stats['api_error_rate']:.1f}%)")

    # Estimate cost
    cost_per_1000 = 0.15  # Using gpt-4o-mini
    estimated_cost = (api_calls_made / 1000) * cost_per_1000
    logger.info(f"\nAPI Usage:")
    logger.info(f"  Total API calls: {api_calls_made}")
    logger.info(f"  Estimated cost: ${estimated_cost:.2f}")

    return pd.DataFrame(all_matches)


def save_results(results_df: pd.DataFrame, output_dir: Path, config: dict, runtime: float):
    """Save matching results in multiple formats"""

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if results_df.empty:
        logger.warning("No matches found!")
        return {}

    # Reorder columns for clarity
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

    # Only include existing columns
    columns_to_use = [col for col in column_order if col in results_df.columns]
    results_df = results_df[columns_to_use]

    # Save full results
    full_file = output_dir / f"tier2_sample100_full_{timestamp}.csv"
    results_df.to_csv(full_file, index=False)
    logger.info(f"Saved {len(results_df)} matches to {full_file}")

    # Save best matches only (rank 1)
    best_matches = results_df[results_df['match_rank'] == 1].copy()
    best_file = output_dir / f"tier2_sample100_best_{timestamp}.csv"
    best_matches.to_csv(best_file, index=False)
    logger.info(f"Saved {len(best_matches)} best matches to {best_file}")

    # Create summary statistics
    summary = {
        "timestamp": timestamp,
        "method": "Tier2 with LLM Enhancement (100-sample)",
        "runtime_seconds": runtime,
        "runtime_minutes": runtime / 60,
        "total_op_manufacturers": len(results_df['op_manufacturer_id'].unique()),
        "total_matches": len(results_df),
        "best_matches": len(best_matches),
        "match_rate": f"{len(best_matches) / 100 * 100:.1f}%",
        "avg_final_score": float(results_df['final_score'].mean()),
        "avg_fuzzy_score": float(results_df['fuzzy_score'].mean()),
        "avg_ai_score": float(results_df['ai_score'].mean()) if 'ai_score' in results_df.columns else None,
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
    summary_file = output_dir / f"tier2_sample100_summary_{timestamp}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    logger.info(f"Saved summary to {summary_file}")

    return summary


def main():
    """Main execution workflow"""

    logger.info("="*60)
    logger.info("Starting OP-RX Manufacturer Tier2 Matching (100-Sample)")
    logger.info("="*60)

    # Set paths
    project_dir = Path(__file__).parent.parent
    config_path = project_dir / "config.yaml"
    input_dir = project_dir / "data" / "input"
    output_dir = project_dir / "data" / "output"

    # Load configuration
    config = load_config(config_path)

    # Load manufacturer data
    op_sample_df = pd.read_csv(input_dir / "op_manufacturers_sample100.csv")
    rx_df = pd.read_csv(input_dir / "rx_manufacturers.csv")

    logger.info(f"Loaded {len(op_sample_df)} OP manufacturers (sample) and {len(rx_df)} RX manufacturers")

    # Start timing
    start_time = time.time()

    # Run Tier2 matching with blocking
    results_df = run_tier2_matching_sample100(op_sample_df, rx_df, config)

    # Calculate runtime
    runtime = time.time() - start_time

    # Save results
    if not results_df.empty:
        summary = save_results(results_df, output_dir, config, runtime)

        # Print final summary
        logger.info("\n" + "="*60)
        logger.info("TIER2 SAMPLE100 MATCHING COMPLETE")
        logger.info("="*60)
        logger.info(f"Runtime: {summary['runtime_minutes']:.1f} minutes")
        logger.info(f"Total matches: {summary['total_matches']}")
        logger.info(f"Best matches: {summary['best_matches']} ({summary['match_rate']})")
        logger.info(f"Average final score: {summary['avg_final_score']:.1f}")
        logger.info(f"\nScore distribution:")
        for range_name, count in summary['score_distribution'].items():
            if count > 0:
                logger.info(f"  {range_name}%: {count}")
        logger.info(f"\nConfidence sources:")
        for source, count in summary['confidence_sources'].items():
            logger.info(f"  {source}: {count}")
    else:
        logger.warning("No matches found!")


if __name__ == "__main__":
    main()