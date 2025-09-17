#!/usr/bin/env python3
"""
Run Tier2 name matching for OP and RX manufacturer names
Creates a mapping table between Open Payments and Prescription manufacturers
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

# Add parent directories to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'src' / 'analysis' / '01-core-name-matching'))

# Import Tier2 matcher
from tier2_matcher import Tier2NameMatcher

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_config(config_path: Path) -> dict:
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def load_manufacturers(input_dir: Path):
    """Load manufacturer data from CSV files"""

    # Load Open Payments manufacturers
    op_file = input_dir / "op_manufacturers.csv"
    op_df = pd.read_csv(op_file)
    logger.info(f"Loaded {len(op_df)} Open Payments manufacturers")

    # Load Prescription manufacturers
    rx_file = input_dir / "rx_manufacturers.csv"
    rx_df = pd.read_csv(rx_file)
    logger.info(f"Loaded {len(rx_df)} Prescription manufacturers")

    return op_df, rx_df


def get_blocking_key(name: str) -> str:
    """
    Get blocking key for a manufacturer name
    Returns the first character (letter or number) in uppercase
    Handles special cases like 'The', 'A', etc.
    """
    if pd.isna(name) or not name:
        return 'UNKNOWN'

    # Clean and uppercase
    name = str(name).strip().upper()

    # Remove common prefixes that might not be consistent
    prefixes_to_skip = ['THE ', 'A ', 'AN ']
    for prefix in prefixes_to_skip:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break

    # Get first character
    if name:
        first_char = name[0]
        # Group numbers together
        if first_char.isdigit():
            return 'NUMERIC'
        elif first_char.isalpha():
            return first_char
        else:
            return 'SPECIAL'  # For special characters

    return 'UNKNOWN'


def create_blocking_groups(op_df: pd.DataFrame, rx_df: pd.DataFrame):
    """
    Create blocking groups to reduce unnecessary comparisons
    """
    # Add blocking keys
    op_df['blocking_key'] = op_df['manufacturer_name'].apply(get_blocking_key)
    rx_df['blocking_key'] = rx_df['manufacturer_name'].apply(get_blocking_key)

    # Group by blocking key
    op_groups = op_df.groupby('blocking_key')
    rx_groups = rx_df.groupby('blocking_key')

    # Log statistics
    logger.info(f"Blocking statistics:")
    logger.info(f"  OP manufacturers grouped into {len(op_groups)} blocks")
    logger.info(f"  RX manufacturers grouped into {len(rx_groups)} blocks")

    blocking_stats = []
    for key in op_df['blocking_key'].unique():
        op_count = len(op_groups.get_group(key)) if key in op_groups.groups else 0
        rx_count = len(rx_groups.get_group(key)) if key in rx_groups.groups else 0
        comparisons = op_count * rx_count
        if comparisons > 0:
            blocking_stats.append({
                'key': key,
                'op_count': op_count,
                'rx_count': rx_count,
                'comparisons': comparisons
            })

    total_comparisons = sum(s['comparisons'] for s in blocking_stats)
    naive_comparisons = len(op_df) * len(rx_df)
    reduction = 1 - (total_comparisons / naive_comparisons)

    logger.info(f"  Total comparisons: {total_comparisons:,} (vs {naive_comparisons:,} naive)")
    logger.info(f"  Reduction: {reduction:.1%}")

    return op_groups, rx_groups


def run_matching(op_df: pd.DataFrame, rx_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Run Tier2 matching between OP and RX manufacturers with intelligent blocking

    Returns DataFrame with match results
    """

    # Get matching configuration
    match_config = config['matching']

    # Initialize matcher
    matcher = Tier2NameMatcher(
        fuzzy_threshold=match_config['fuzzy_threshold'],
        decision_threshold=match_config['decision_threshold'],
        model=match_config['ai_model'],
        max_workers=match_config['max_workers']
    )

    logger.info("Starting Tier2 name matching with blocking...")
    logger.info(f"Configuration: fuzzy_threshold={match_config['fuzzy_threshold']}, "
                f"decision_threshold={match_config['decision_threshold']}")

    # Create blocking groups
    op_groups, rx_groups = create_blocking_groups(op_df.copy(), rx_df.copy())

    # Prepare results list
    all_matches = []
    total_processed = 0

    # Process each blocking group
    for blocking_key in sorted(op_groups.groups.keys()):
        if blocking_key not in rx_groups.groups:
            logger.info(f"Skipping block '{blocking_key}': No RX manufacturers to match")
            continue

        op_block = op_groups.get_group(blocking_key)
        rx_block = rx_groups.get_group(blocking_key)

        logger.info(f"\nProcessing block '{blocking_key}': "
                   f"{len(op_block)} OP × {len(rx_block)} RX = {len(op_block) * len(rx_block)} comparisons")

        # For each OP manufacturer in this block
        for idx, op_row in op_block.iterrows():
            op_name = op_row['manufacturer_name']
            op_id = op_row['manufacturer_id']

            # Match against RX manufacturers IN THE SAME BLOCK ONLY
            matches_for_this_op = []

            for _, rx_row in rx_block.iterrows():
                rx_name = rx_row['manufacturer_name']

                # Run matching
                match_result = matcher.match_pair(
                    op_name,
                    rx_name,
                    use_ai=config['processing']['use_ai_enhancement']
                )

                # Add metadata
                match_result['op_manufacturer_name'] = op_name
                match_result['op_manufacturer_id'] = op_id
                match_result['rx_manufacturer_name'] = rx_name
                match_result['blocking_key'] = blocking_key

                matches_for_this_op.append(match_result)

            # Sort by final score and take top N matches
            matches_for_this_op.sort(key=lambda x: x['final_score'], reverse=True)
            top_matches = matches_for_this_op[:match_config['top_n_matches']]

            # Add rank
            for rank, match in enumerate(top_matches, 1):
                match['match_rank'] = rank
                all_matches.append(match)

            total_processed += 1

            # Log progress every 50 manufacturers
            if total_processed % 50 == 0:
                logger.info(f"  Progress: {total_processed} OP manufacturers processed")

    # Log statistics
    stats = matcher.get_statistics()
    logger.info(f"\nMatching Statistics:")
    logger.info(f"  Total processed: {stats['total_processed']}")
    logger.info(f"  Fuzzy only: {stats['fuzzy_only']} ({stats['fuzzy_only_pct']:.1f}%)")
    logger.info(f"  AI enhanced: {stats['ai_enhanced']} ({stats['ai_enhanced_pct']:.1f}%)")
    if stats['api_errors'] > 0:
        logger.warning(f"  API errors: {stats['api_errors']} ({stats['api_error_rate']:.1f}%)")

    # Convert to DataFrame
    results_df = pd.DataFrame(all_matches)

    # Reorder columns
    column_order = [
        'op_manufacturer_id',
        'op_manufacturer_name',
        'rx_manufacturer_name',
        'match_rank',
        'final_score',
        'is_match',
        'fuzzy_score',
        'ai_score',
        'confidence_source'
    ]

    # Only include columns that exist
    columns_to_use = [col for col in column_order if col in results_df.columns]
    results_df = results_df[columns_to_use]

    return results_df


def save_results(results_df: pd.DataFrame, output_dir: Path, config: dict):
    """Save matching results in multiple formats"""

    output_dir.mkdir(parents=True, exist_ok=True)

    # Filter by minimum confidence if specified
    min_confidence = config['output']['min_confidence']
    filtered_df = results_df[results_df['final_score'] >= min_confidence].copy()

    logger.info(f"Filtered to {len(filtered_df)} matches with confidence >= {min_confidence}")

    # Save formats
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if 'csv' in config['output']['formats']:
        # Save full results
        full_file = output_dir / f"manufacturer_matching_full_{timestamp}.csv"
        results_df.to_csv(full_file, index=False)
        logger.info(f"Saved full results to {full_file}")

        # Save filtered results
        filtered_file = output_dir / f"manufacturer_matching_filtered_{timestamp}.csv"
        filtered_df.to_csv(filtered_file, index=False)
        logger.info(f"Saved filtered results to {filtered_file}")

        # Save best matches only (rank 1)
        best_matches = filtered_df[filtered_df['match_rank'] == 1].copy()
        best_file = output_dir / f"manufacturer_matching_best_{timestamp}.csv"
        best_matches.to_csv(best_file, index=False)
        logger.info(f"Saved best matches to {best_file}")

    if 'json' in config['output']['formats']:
        json_file = output_dir / f"manufacturer_matching_{timestamp}.json"
        filtered_df.to_json(json_file, orient='records', indent=2)
        logger.info(f"Saved JSON results to {json_file}")

    # Create summary statistics
    summary = {
        "run_timestamp": timestamp,
        "total_op_manufacturers": len(results_df['op_manufacturer_id'].unique()),
        "total_rx_manufacturers": len(results_df['rx_manufacturer_name'].unique()),
        "total_matches_tested": len(results_df),
        "high_confidence_matches": len(filtered_df),
        "unique_op_with_matches": len(filtered_df['op_manufacturer_id'].unique()),
        "match_rate": len(filtered_df['op_manufacturer_id'].unique()) / len(results_df['op_manufacturer_id'].unique()),
        "confidence_distribution": {
            "90-100": len(filtered_df[filtered_df['final_score'] >= 90]),
            "80-90": len(filtered_df[(filtered_df['final_score'] >= 80) & (filtered_df['final_score'] < 90)]),
            "70-80": len(filtered_df[(filtered_df['final_score'] >= 70) & (filtered_df['final_score'] < 80)]),
            "60-70": len(filtered_df[(filtered_df['final_score'] >= 60) & (filtered_df['final_score'] < 70)])
        }
    }

    # Save summary
    summary_file = output_dir / f"matching_summary_{timestamp}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Saved summary to {summary_file}")

    return summary


def main():
    """Main execution workflow"""

    logger.info("="*60)
    logger.info("Starting OP-RX Manufacturer Name Matching")
    logger.info("="*60)

    # Set paths
    project_dir = Path(__file__).parent.parent
    config_path = project_dir / "config.yaml"
    input_dir = project_dir / "data" / "input"
    output_dir = project_dir / "data" / "output"

    # Load configuration
    config = load_config(config_path)

    # Load manufacturer data
    op_df, rx_df = load_manufacturers(input_dir)

    # Run matching
    logger.info("\n" + "="*60)
    logger.info("Running Tier2 Name Matching")
    logger.info("="*60)

    results_df = run_matching(op_df, rx_df, config)

    # Save results
    logger.info("\n" + "="*60)
    logger.info("Saving Results")
    logger.info("="*60)

    summary = save_results(results_df, output_dir, config)

    # Print summary
    logger.info("\n" + "="*60)
    logger.info("MATCHING COMPLETE")
    logger.info("="*60)
    logger.info(f"Total OP manufacturers: {summary['total_op_manufacturers']}")
    logger.info(f"Matched manufacturers: {summary['unique_op_with_matches']}")
    logger.info(f"Match rate: {summary['match_rate']:.1%}")
    logger.info(f"\nConfidence distribution:")
    for range_name, count in summary['confidence_distribution'].items():
        logger.info(f"  {range_name}%: {count} matches")
    logger.info("="*60)


if __name__ == "__main__":
    main()