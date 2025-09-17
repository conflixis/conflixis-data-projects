#!/usr/bin/env python3
"""
Run simple fuzzy matching for OP and RX manufacturer names
Uses blocking to reduce comparisons and basic fuzzy matching
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
import json
from datetime import datetime
import yaml
from rapidfuzz import fuzz
import jellyfish
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_config(config_path: Path) -> dict:
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def preprocess_name(name: str) -> str:
    """Basic preprocessing for manufacturer names"""
    if pd.isna(name) or not name:
        return ""

    name = str(name).lower().strip()

    # Remove common suffixes
    suffixes = ['inc', 'incorporated', 'corp', 'corporation', 'llc', 'ltd', 'limited', 'co', 'company']
    for suffix in suffixes:
        if name.endswith(f' {suffix}') or name.endswith(f' {suffix}.'):
            name = name.replace(f' {suffix}.', '').replace(f' {suffix}', '')

    # Remove punctuation
    name = name.replace(',', '').replace('.', '').replace('&', 'and')

    # Normalize whitespace
    name = ' '.join(name.split())

    return name


def fuzzy_match(name_a: str, name_b: str) -> dict:
    """Perform fuzzy matching between two names"""

    # Preprocess names
    clean_a = preprocess_name(name_a)
    clean_b = preprocess_name(name_b)

    # Calculate various similarity scores
    scores = {
        'exact': 100.0 if clean_a == clean_b else 0.0,
        'ratio': fuzz.ratio(clean_a, clean_b),
        'partial': fuzz.partial_ratio(clean_a, clean_b),
        'token_sort': fuzz.token_sort_ratio(clean_a, clean_b),
        'token_set': fuzz.token_set_ratio(clean_a, clean_b),
        'jaro_winkler': jellyfish.jaro_winkler_similarity(clean_a, clean_b) * 100 if clean_a and clean_b else 0
    }

    # Weighted average
    weights = {
        'exact': 0.25,
        'ratio': 0.20,
        'partial': 0.10,
        'token_sort': 0.15,
        'token_set': 0.15,
        'jaro_winkler': 0.15
    }

    final_score = sum(scores[k] * weights[k] for k in scores)

    return {
        'fuzzy_score': final_score,
        'scores': scores
    }


def get_blocking_key(name: str) -> str:
    """Get blocking key for a manufacturer name"""
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


def run_matching_with_blocking(op_df: pd.DataFrame, rx_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Run matching with blocking to reduce comparisons"""

    # Add blocking keys
    op_df['blocking_key'] = op_df['manufacturer_name'].apply(get_blocking_key)
    rx_df['blocking_key'] = rx_df['manufacturer_name'].apply(get_blocking_key)

    # Group by blocking key
    op_groups = op_df.groupby('blocking_key')
    rx_groups = rx_df.groupby('blocking_key')

    # Log statistics
    logger.info(f"Blocking statistics:")
    unique_blocks = set(op_df['blocking_key'].unique()) | set(rx_df['blocking_key'].unique())
    logger.info(f"  Total blocks: {len(unique_blocks)}")

    # Calculate total comparisons
    total_comparisons = 0
    for key in op_df['blocking_key'].unique():
        if key in rx_df['blocking_key'].values:
            op_count = len(op_df[op_df['blocking_key'] == key])
            rx_count = len(rx_df[rx_df['blocking_key'] == key])
            total_comparisons += op_count * rx_count

    naive_comparisons = len(op_df) * len(rx_df)
    reduction = 1 - (total_comparisons / naive_comparisons)
    logger.info(f"  Total comparisons: {total_comparisons:,} (vs {naive_comparisons:,} naive)")
    logger.info(f"  Reduction: {reduction:.1%}")

    # Matching parameters
    min_score_threshold = config['matching']['decision_threshold']
    top_n = config['matching']['top_n_matches']

    all_matches = []
    processed = 0

    # Process each blocking group
    for blocking_key in sorted(op_groups.groups.keys()):
        if blocking_key not in rx_groups.groups:
            continue

        op_block = op_groups.get_group(blocking_key)
        rx_block = rx_groups.get_group(blocking_key)

        logger.info(f"\nProcessing block '{blocking_key}': {len(op_block)} × {len(rx_block)} = {len(op_block) * len(rx_block)} comparisons")

        # For each OP manufacturer in this block
        for _, op_row in op_block.iterrows():
            op_name = op_row['manufacturer_name']
            op_id = op_row['manufacturer_id']

            matches_for_this_op = []

            # Match against RX manufacturers in same block
            for _, rx_row in rx_block.iterrows():
                rx_name = rx_row['manufacturer_name']

                # Perform fuzzy matching
                match_result = fuzzy_match(op_name, rx_name)

                # Only keep if above threshold
                if match_result['fuzzy_score'] >= min_score_threshold:
                    matches_for_this_op.append({
                        'op_manufacturer_id': op_id,
                        'op_manufacturer_name': op_name,
                        'rx_manufacturer_name': rx_name,
                        'final_score': match_result['fuzzy_score'],
                        'is_match': match_result['fuzzy_score'] >= config['matching']['fuzzy_threshold'],
                        'blocking_key': blocking_key
                    })

            # Sort and take top N
            if matches_for_this_op:
                matches_for_this_op.sort(key=lambda x: x['final_score'], reverse=True)
                top_matches = matches_for_this_op[:top_n]

                for rank, match in enumerate(top_matches, 1):
                    match['match_rank'] = rank
                    all_matches.append(match)

            processed += 1
            if processed % 100 == 0:
                logger.info(f"  Processed {processed} OP manufacturers")

    logger.info(f"\nTotal matches found: {len(all_matches)}")

    return pd.DataFrame(all_matches)


def save_results(results_df: pd.DataFrame, output_dir: Path, config: dict):
    """Save matching results"""

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if results_df.empty:
        logger.warning("No matches found!")
        return {}

    # Save full results
    full_file = output_dir / f"manufacturer_matching_{timestamp}.csv"
    results_df.to_csv(full_file, index=False)
    logger.info(f"Saved {len(results_df)} matches to {full_file}")

    # Save best matches only
    best_matches = results_df[results_df['match_rank'] == 1].copy()
    best_file = output_dir / f"manufacturer_best_matches_{timestamp}.csv"
    best_matches.to_csv(best_file, index=False)
    logger.info(f"Saved {len(best_matches)} best matches to {best_file}")

    # Create summary
    summary = {
        "timestamp": timestamp,
        "total_op_manufacturers": len(results_df['op_manufacturer_id'].unique()),
        "total_matches": len(results_df),
        "best_matches": len(best_matches),
        "avg_score": float(results_df['final_score'].mean()),
        "score_distribution": {
            "90-100": len(results_df[results_df['final_score'] >= 90]),
            "80-90": len(results_df[(results_df['final_score'] >= 80) & (results_df['final_score'] < 90)]),
            "70-80": len(results_df[(results_df['final_score'] >= 70) & (results_df['final_score'] < 80)]),
            "60-70": len(results_df[(results_df['final_score'] >= 60) & (results_df['final_score'] < 70)])
        }
    }

    # Save summary
    summary_file = output_dir / f"matching_summary_{timestamp}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Saved summary to {summary_file}")

    return summary


def main():
    """Main execution"""

    logger.info("="*60)
    logger.info("Starting OP-RX Manufacturer Name Matching (Simple Fuzzy)")
    logger.info("="*60)

    # Set paths
    project_dir = Path(__file__).parent.parent
    config_path = project_dir / "config.yaml"
    input_dir = project_dir / "data" / "input"
    output_dir = project_dir / "data" / "output"

    # Load configuration
    config = load_config(config_path)

    # Load data
    op_df = pd.read_csv(input_dir / "op_manufacturers.csv")
    rx_df = pd.read_csv(input_dir / "rx_manufacturers.csv")

    logger.info(f"Loaded {len(op_df)} OP manufacturers and {len(rx_df)} RX manufacturers")

    # Run matching
    results_df = run_matching_with_blocking(op_df, rx_df, config)

    # Save results
    if not results_df.empty:
        summary = save_results(results_df, output_dir, config)

        # Print summary
        logger.info("\n" + "="*60)
        logger.info("MATCHING COMPLETE")
        logger.info("="*60)
        logger.info(f"Total matches: {summary['total_matches']}")
        logger.info(f"Average score: {summary['avg_score']:.1f}")
        logger.info(f"Score distribution:")
        for range_name, count in summary['score_distribution'].items():
            logger.info(f"  {range_name}%: {count}")
    else:
        logger.warning("No matches found!")


if __name__ == "__main__":
    main()