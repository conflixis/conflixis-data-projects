#!/usr/bin/env python3
"""
Full population Tier2 matching with checkpointing for resumable processing
Saves progress every batch and can resume from last checkpoint
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
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from typing import Dict, List, Tuple

# Add scripts directory to path so we can import tier2_matcher
sys.path.insert(0, str(Path(__file__).parent))

# Import Tier2 matcher
from tier2_matcher import Tier2NameMatcher

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Checkpoint file
CHECKPOINT_FILE = "checkpoint/tier2_checkpoint.pkl"
RESULTS_FILE = "checkpoint/tier2_results.pkl"


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


def save_checkpoint(checkpoint_data: dict):
    """Save checkpoint to file"""
    checkpoint_path = Path("checkpoint") / "tier2_checkpoint.pkl"
    with open(checkpoint_path, 'wb') as f:
        pickle.dump(checkpoint_data, f)
    logger.info(f"Checkpoint saved: {len(checkpoint_data['completed_batches'])}/{checkpoint_data['total_batches']} batches completed")


def load_checkpoint():
    """Load checkpoint if exists"""
    checkpoint_path = Path(CHECKPOINT_FILE)
    if checkpoint_path.exists():
        with open(checkpoint_path, 'rb') as f:
            checkpoint = pickle.load(f)
        logger.info(f"Checkpoint loaded: {checkpoint['completed_batches']}/{checkpoint['total_batches']} batches already completed")
        return checkpoint
    return None


def save_results(results: List[Dict]):
    """Save accumulated results to file"""
    results_path = Path("checkpoint") / "tier2_results.pkl"

    # Load existing results if any
    existing_results = []
    if results_path.exists():
        with open(results_path, 'rb') as f:
            existing_results = pickle.load(f)

    # Append new results
    all_results = existing_results + results

    with open(results_path, 'wb') as f:
        pickle.dump(all_results, f)

    logger.info(f"Saved {len(results)} new results (total: {len(all_results)})")


def load_all_results():
    """Load all accumulated results"""
    results_path = Path(RESULTS_FILE)
    if results_path.exists():
        with open(results_path, 'rb') as f:
            return pickle.load(f)
    return []


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


def process_batch(matcher: Tier2NameMatcher, batch: List[Tuple], use_ai: bool, batch_idx: int) -> Tuple[int, List[Dict]]:
    """
    Process a batch of comparisons with retry logic for API errors
    Returns batch_idx and results
    """
    results = []
    for op_id, op_name, rx_name, blocking_key in batch:
        retry_count = 0
        max_retries = 3
        while retry_count < max_retries:
            try:
                # Run Tier2 matching
                match_result = matcher.match_pair(op_name, rx_name, use_ai=use_ai)

                # Add metadata
                match_result['op_manufacturer_id'] = op_id
                match_result['op_manufacturer_name'] = op_name
                match_result['rx_manufacturer_name'] = rx_name
                match_result['blocking_key'] = blocking_key

                results.append(match_result)
                break  # Success, exit retry loop
            except Exception as e:
                error_str = str(e)
                if '503' in error_str or 'Service Unavailable' in error_str:
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = 2 ** retry_count  # Exponential backoff
                        logger.warning(f"API 503 error, retrying in {wait_time}s (attempt {retry_count}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Max retries reached for {op_name} with {rx_name}: {e}")
                else:
                    logger.error(f"Error matching {op_name} with {rx_name}: {e}")
                    break

    return batch_idx, results


def run_tier2_with_checkpoint(op_df: pd.DataFrame, rx_df: pd.DataFrame, config: dict) -> tuple:
    """
    Run Tier2 matching with checkpointing support
    Returns: (results_df, is_complete) tuple
    """
    # Create checkpoint directory upfront
    Path("checkpoint").mkdir(exist_ok=True)

    # Get matching configuration
    match_config = config['matching']

    # Initialize Tier2 matcher
    matcher = Tier2NameMatcher(
        fuzzy_threshold=match_config['fuzzy_threshold'],
        decision_threshold=match_config['decision_threshold'],
        model=match_config['ai_model'],
        max_workers=1
    )

    # Prepare all comparison pairs
    pairs = prepare_comparison_pairs(op_df.copy(), rx_df.copy())

    # Split pairs into batches of 100 for frequent checkpointing
    batch_size = 100  # Fixed batch size of 100 items
    batches = [pairs[i:i+batch_size] for i in range(0, len(pairs), batch_size)]

    logger.info(f"Split into {len(batches)} batches of ~{batch_size} comparisons each")

    # Try to load checkpoint
    checkpoint = load_checkpoint()
    if checkpoint:
        completed_batches = checkpoint['completed_batches']
        start_batch = checkpoint['last_batch_idx'] + 1
        logger.info(f"Resuming from batch {start_batch}/{len(batches)}")
    else:
        completed_batches = set()
        start_batch = 0
        # Initialize checkpoint
        checkpoint = {
            'total_batches': len(batches),
            'completed_batches': completed_batches,
            'last_batch_idx': -1,
            'start_time': time.time()
        }

    # Process batches - HARDCODED TO RUN 500 BATCHES PER SESSION
    batch_results = []
    start_time = time.time()
    last_checkpoint_time = time.time()

    # Calculate batch limit for this session
    max_batches_per_session = 500
    session_start_count = len(completed_batches)  # Track how many were already done
    target_batch_count = min(session_start_count + max_batches_per_session, len(batches))
    batches_to_process_this_session = target_batch_count - session_start_count
    logger.info(f"Starting from batch {session_start_count}, will process {batches_to_process_this_session} new batches (up to batch {target_batch_count})")

    with ThreadPoolExecutor(max_workers=match_config['max_workers']) as executor:
        futures = {}
        next_batch_to_submit = start_batch
        batches_processed_this_session = 0

        # Keep processing until we hit the session limit OR run out of batches
        while batches_processed_this_session < max_batches_per_session and len(completed_batches) < len(batches):
            # Submit batches up to max_workers limit
            while (len(futures) < match_config['max_workers'] and
                   next_batch_to_submit < len(batches) and
                   batches_processed_this_session < max_batches_per_session):
                if next_batch_to_submit not in completed_batches:
                    future = executor.submit(process_batch, matcher, batches[next_batch_to_submit],
                                           config['processing']['use_ai_enhancement'], next_batch_to_submit)
                    futures[future] = next_batch_to_submit
                next_batch_to_submit += 1

            # If no futures are running and no more batches to submit, we're done
            if not futures:
                if next_batch_to_submit >= len(batches) or batches_processed_this_session >= max_batches_per_session:
                    break
                else:
                    # Skip to next uncompleted batch
                    while next_batch_to_submit < len(batches) and next_batch_to_submit in completed_batches:
                        next_batch_to_submit += 1
                    continue

            # Wait for at least one future to complete
            from concurrent.futures import wait, FIRST_COMPLETED
            done, pending = wait(futures.keys(), return_when=FIRST_COMPLETED, timeout=1)

            # Process completed futures
            for future in done:
                batch_idx = futures[future]
                try:
                    idx, results = future.result()
                    batch_results.extend(results)
                    completed_batches.add(idx)
                    batches_processed_this_session += 1

                # Update checkpoint
                checkpoint['completed_batches'] = completed_batches
                checkpoint['last_batch_idx'] = max(completed_batches)

                # Save checkpoint and results immediately after each batch
                checkpoint['elapsed_before'] = checkpoint.get('elapsed_before', 0) + (time.time() - checkpoint.get('session_start', start_time))
                checkpoint['session_start'] = time.time()
                save_checkpoint(checkpoint)
                if batch_results:
                    save_results(batch_results)
                    batch_results = []  # Clear after saving

                # Log progress
                elapsed = time.time() - start_time
                progress_pct = (len(completed_batches) / len(batches)) * 100
                total_results = len(load_all_results())
                rate = total_results / (elapsed + checkpoint.get('elapsed_before', 0)) if (elapsed + checkpoint.get('elapsed_before', 0)) > 0 else 0
                eta = (len(pairs) - total_results) / rate if rate > 0 else 0

                logger.info(f"Progress: {len(completed_batches)}/{len(batches)} batches ({progress_pct:.1f}%) | "
                           f"Results: {total_results:,}/{len(pairs):,} | "
                           f"Rate: {rate:.0f} comp/sec | ETA: {eta/60:.1f} min")

                last_checkpoint_time = time.time()

                    # Remove completed future from tracking dict
                    del futures[future]

                    # Log session progress every 10 batches
                    if batches_processed_this_session % 10 == 0:
                        logger.info(f"Session progress: {batches_processed_this_session}/{max_batches_per_session} batches processed this session")

                except Exception as e:
                    logger.error(f"Batch {batch_idx} failed: {e}")
                    del futures[future]

        # Log session completion
        logger.info(f"Session complete - processed {batches_processed_this_session} batches in this session")
        logger.info(f"Total completed: {len(completed_batches)}/{len(batches)} batches")

    # Save any remaining results
    if batch_results:
        save_results(batch_results)

    # Load all results
    all_results = load_all_results()

    total_time = time.time() - start_time + checkpoint.get('elapsed_before', 0)

    # Filter and rank results
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
        matches.sort(key=lambda x: x['final_score'], reverse=True)
        for rank, match in enumerate(matches[:match_config['top_n_matches']], 1):
            match['match_rank'] = rank
            filtered_results.append(match)

    # Get statistics
    stats = matcher.get_statistics()

    logger.info(f"\n{'='*60}")
    logger.info(f"MATCHING COMPLETE")
    logger.info(f"{'='*60}")
    logger.info(f"Total runtime: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
    logger.info(f"Total comparisons: {len(all_results):,}")
    logger.info(f"Processing rate: {len(all_results)/total_time:.1f} comparisons/second")
    logger.info(f"Matches found: {len(filtered_results)}")
    logger.info(f"Unique OP manufacturers matched: {len(op_matches)}")

    # Check if all batches were completed
    is_complete = len(completed_batches) == len(batches)
    if is_complete:
        logger.info("ALL BATCHES COMPLETED SUCCESSFULLY")
    else:
        logger.info(f"RUN INTERRUPTED - Completed {len(completed_batches)}/{len(batches)} batches")
        logger.info("Checkpoint files preserved for resumption")

    return pd.DataFrame(filtered_results), is_complete


def save_final_results(results_df: pd.DataFrame, output_dir: Path, runtime: float, op_total: int):
    """Save final matching results"""

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
    full_file = output_dir / f"tier2_checkpoint_full_{timestamp}.csv"
    results_df.to_csv(full_file, index=False)
    logger.info(f"Saved {len(results_df)} matches to {full_file}")

    # Save best matches only
    best_matches = results_df[results_df['match_rank'] == 1].copy()
    best_file = output_dir / f"tier2_checkpoint_best_{timestamp}.csv"
    best_matches.to_csv(best_file, index=False)
    logger.info(f"Saved {len(best_matches)} best matches to {best_file}")

    # Create summary
    summary = {
        "timestamp": timestamp,
        "method": "Tier2 with Checkpoint (Full Population)",
        "runtime_seconds": runtime,
        "runtime_minutes": runtime / 60,
        "total_op_manufacturers": op_total,
        "op_manufacturers_matched": len(results_df['op_manufacturer_id'].unique()),
        "match_rate": f"{len(results_df['op_manufacturer_id'].unique()) / op_total * 100:.1f}%",
        "total_matches": len(results_df),
        "best_matches": len(best_matches),
        "avg_final_score": float(results_df['final_score'].mean()) if not results_df.empty else 0,
        "confidence_sources": results_df['confidence_source'].value_counts().to_dict() if not results_df.empty else {},
    }

    # Save summary
    summary_file = output_dir / f"tier2_checkpoint_summary_{timestamp}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    logger.info(f"Saved summary to {summary_file}")

    return summary


def main():
    """Main execution"""

    logger.info("="*60)
    logger.info("TIER2 MATCHING WITH CHECKPOINT SUPPORT")
    logger.info("="*60)

    # Set paths
    project_dir = Path(__file__).parent.parent
    config_path = project_dir / "config.yaml"
    input_dir = project_dir / "data" / "input"
    output_dir = project_dir / "data" / "output"

    # Load configuration
    config = load_config(config_path)

    # Check for existing checkpoint
    checkpoint = load_checkpoint()
    if checkpoint:
        logger.info("Found existing checkpoint - RESUMING from last position")
        logger.info(f"Previously completed: {len(checkpoint['completed_batches'])} batches")
    else:
        logger.info("No checkpoint found - STARTING fresh")

    # Load data
    op_df = pd.read_csv(input_dir / "op_manufacturers.csv")
    rx_df = pd.read_csv(input_dir / "rx_manufacturers.csv")

    logger.info(f"Loaded {len(op_df)} OP manufacturers and {len(rx_df)} RX manufacturers")

    # Run matching with checkpoint
    start_time = time.time()
    results_df, is_complete = run_tier2_with_checkpoint(op_df, rx_df, config)
    runtime = time.time() - start_time

    # Save final results
    if not results_df.empty:
        summary = save_final_results(results_df, output_dir, runtime, len(op_df))

        # Print summary
        logger.info("\n" + "="*60)
        logger.info("FINAL SUMMARY")
        logger.info("="*60)
        logger.info(f"Runtime: {summary['runtime_minutes']:.1f} minutes")
        logger.info(f"Match rate: {summary['match_rate']}")
        logger.info(f"Total matches: {summary['total_matches']}")
        logger.info(f"Best matches: {summary['best_matches']}")

        # Only clean up checkpoint files if ALL batches completed
        if is_complete:
            logger.info("\nAll batches completed - cleaning up checkpoint files...")
            checkpoint_dir = Path("checkpoint")
            if checkpoint_dir.exists():
                for f in checkpoint_dir.glob("*.pkl"):
                    f.unlink()
                logger.info("Checkpoint files cleaned up")
        else:
            logger.info("\nRun was interrupted - checkpoint files preserved")
            logger.info("Re-run the script to resume from the last checkpoint")
    else:
        logger.warning("No matches found!")


if __name__ == "__main__":
    main()