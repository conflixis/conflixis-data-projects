#!/usr/bin/env python3
"""
Name Matching Runner Script
Executes the core name matching module with configuration support.
Each execution creates a timestamped output file.
"""

import os
import sys
import yaml
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# Add the src directory to Python path to import the core module
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import from the new module structure
# Python doesn't allow brackets in module names, so we need to add the specific directory
sys.path.insert(0, str(project_root / 'src' / 'analysis' / '01-core-name-matching'))

# Try to import the enhanced v2 module first, then fall back to v1, then original
try:
    from name_matcher_v2 import EnhancedNameMatcher, enhanced_find_matches_v2
    use_module_version = 'v2'
except ImportError:
    try:
        from name_matcher import NameMatcher
        use_module_version = 'v1'
    except ImportError:
        from core_name_matching import enhanced_find_matches, preprocess
        use_module_version = 'original'

import pandas as pd
import numpy as np


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML or JSON file."""
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        if config_path.suffix.lower() in ['.yaml', '.yml']:
            return yaml.safe_load(f)
        elif config_path.suffix.lower() == '.json':
            return json.load(f)
        else:
            raise ValueError(f"Unsupported config file format: {config_path.suffix}")


def validate_config(config: Dict[str, Any]) -> None:
    """Validate the configuration structure and values."""
    required_sections = ['input', 'output', 'processing', 'scoring_weights']
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required configuration section: {section}")
    
    # Validate scoring weights sum to 1.0
    weights = config['scoring_weights']
    total_weight = sum(weights.values())
    if not (0.99 <= total_weight <= 1.01):  # Allow small floating point errors
        raise ValueError(f"Scoring weights must sum to 1.0, got {total_weight}")


def create_output_directory(base_dir: Path, output_config: Dict[str, Any]) -> Path:
    """Create and return the output directory path."""
    output_dir = base_dir / output_config['directory']
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def log_run_info(output_dir: Path, config: Dict[str, Any], start_time: datetime, 
                 end_time: datetime, results_file: str, num_matches: int) -> None:
    """Log information about the run to a history file."""
    log_file = output_dir / "run_history.log"
    
    run_info = {
        "timestamp": start_time.isoformat(),
        "duration_seconds": (end_time - start_time).total_seconds(),
        "config_used": config,
        "output_file": results_file,
        "num_matches": num_matches
    }
    
    # Append to log file
    with open(log_file, 'a') as f:
        f.write(json.dumps(run_info) + "\n")


def run_name_matching(config: Dict[str, Any], base_dir: Path) -> None:
    """Execute the name matching process with the given configuration."""
    print(f"Starting name matching process (using {use_module_version} module)...")
    start_time = datetime.now()
    
    # Extract configuration values
    input_config = config['input']
    output_config = config['output']
    processing_config = config['processing']
    weights = config['scoring_weights']
    matching_config = config.get('matching', {})
    advanced = config.get('advanced', {})
    
    # Set up file paths
    file_a_path = base_dir / input_config['file_a']
    file_b_path = base_dir / input_config['file_b']
    output_dir = create_output_directory(base_dir, output_config)
    
    # Validate input files exist
    if not file_a_path.exists():
        raise FileNotFoundError(f"Input file A not found: {file_a_path}")
    if not file_b_path.exists():
        raise FileNotFoundError(f"Input file B not found: {file_b_path}")
    
    print(f"Loading input files...")
    print(f"  File A: {file_a_path}")
    print(f"  File B: {file_b_path}")
    
    # Load data
    limit = processing_config.get('limit')
    df_A = pd.read_csv(file_a_path)
    if limit and limit > 0:
        df_A = df_A.head(limit)
    
    df_B = pd.read_csv(file_b_path, delimiter=',', quotechar='"', skipinitialspace=True)
    
    # Clean column names
    df_A.columns = df_A.columns.str.strip()
    df_B.columns = df_B.columns.str.strip()
    
    print(f"\nLoaded {len(df_A)} rows from File A")
    print(f"Loaded {len(df_B)} rows from File B")
    
    # Validate columns exist
    column_a = input_config['column_a']
    column_b = input_config['column_b']
    
    if column_a not in df_A.columns:
        available_cols = [col for col in df_A.columns if 'name' in col.lower()]
        if available_cols:
            print(f"Warning: Column '{column_a}' not found. Using '{available_cols[0]}' instead.")
            column_a = available_cols[0]
        else:
            raise ValueError(f"Column '{column_a}' not found in File A. Available: {df_A.columns.tolist()}")
    
    if column_b not in df_B.columns:
        raise ValueError(f"Column '{column_b}' not found in File B. Available: {df_B.columns.tolist()}")
    
    # Perform matching based on available module
    print(f"\nPerforming name matching with {processing_config['max_workers']} workers...")
    
    if use_module_version == 'v2':
        # Use the enhanced v2 module with threshold support
        print(f"Using enhanced matcher with threshold={matching_config.get('min_score_threshold', 60.0)}")
        print(f"Returning top {matching_config.get('return_top_n_matches', 1)} matches per name")
        
        matched_df = enhanced_find_matches_v2(
            df_A, df_B, column_a, column_b, config
        )
        
    elif use_module_version == 'v1':
        # Use the v1 NameMatcher class
        stopwords = set(config.get('stopwords', ['stopword']))
        matcher = NameMatcher(stopwords=stopwords, scoring_weights=weights)
        
        # Cache preprocessed names for File B
        if advanced.get('cache_preprocessed', True):
            print("Preprocessing names from File B...")
            df_B['Name B PP'] = df_B[column_b].apply(matcher.preprocess)
        
        matched_df = matcher.find_matches(
            df_A, df_B, column_a, column_b,
            chunk_size=processing_config['chunk_size'],
            max_workers=processing_config['max_workers'],
            cache_preprocessed=False  # Already cached above
        )
    else:
        # Use the original functions
        if advanced.get('cache_preprocessed', True):
            print("Preprocessing names from File B...")
            df_B['Name B PP'] = df_B[column_b].apply(preprocess)
        
        matched_df = enhanced_find_matches(
            df_A, df_B, column_a, column_b,
            chunk_size=processing_config['chunk_size']
        )
    
    if not matched_df.empty:
        # Handle output based on module version
        if use_module_version == 'v2':
            # V2 has different column structure with potential multiple matches
            # No need to rename columns, v2 already uses the correct column names
            print(f"\nMatched {len(matched_df)} total results")
            
            # Count unique names and confidence levels
            if 'Confidence_match_1' in matched_df.columns:
                confidence_counts = matched_df['Confidence_match_1'].value_counts()
                print("\nMatch confidence distribution:")
                for level, count in confidence_counts.items():
                    print(f"  {level}: {count}")
        
        elif use_module_version == 'v1':
            # Column names from v1 module
            output_columns = [
                "Name A Original",
                "Name A PP",
                "Name B Original",
                "Name B PP",
                "RapidFuzz Score",
                "Jellyfish Score",
                "TheFuzz Score",
                "Token-Based Score",
                "First-Word Score",
                "Partial Match Score",
                "Composite Score"
            ]
            # Rename columns to match expected output
            matched_df = matched_df.rename(columns={
                "Name A Original": column_a,
                "Name B Original": column_b
            })
            # Select only the output columns we want
            final_columns = [
                column_a,
                "Name A PP",
                column_b,
                "Name B PP",
                "RapidFuzz Score",
                "Jellyfish Score",
                "TheFuzz Score",
                "Token-Based Score",
                "First-Word Score",
                "Partial Match Score",
                "Composite Score"
            ]
            matched_df = matched_df[final_columns]
            
        else:
            # Original module output format
            # Select standard output columns
            final_columns = [
                column_a,
                "Name A PP",
                column_b,
                "Name B PP",
                "RapidFuzz Score",
                "Jellyfish Score",
                "TheFuzz Score",
                "Token-Based Score",
                "First-Word Score",
                "Partial Match Score",
                "Composite Score"
            ]
            matched_df = matched_df[final_columns]
        
        # Apply minimum score threshold if specified (not needed for v2 as it's handled internally)
        if use_module_version != 'v2':
            min_threshold = matching_config.get('min_score_threshold', 0.0)
            if min_threshold > 0 and 'Composite Score' in matched_df.columns:
                original_count = len(matched_df)
                matched_df = matched_df[matched_df['Composite Score'] >= min_threshold]
                print(f"Filtered {original_count - len(matched_df)} matches below threshold {min_threshold}")
        
        # Create timestamped output file
        file_a_base = Path(file_a_path).stem
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        output_file = output_dir / f"{file_a_base}_matched_{timestamp}.csv"
        
        # Save results
        matched_df.to_csv(output_file, index=False)
        print(f"\nMatching complete! Results saved to: {output_file}")
        print(f"Total matches: {len(matched_df)}")
        
        # Log run information
        end_time = datetime.now()
        log_run_info(output_dir, config, start_time, end_time, 
                    str(output_file), len(matched_df))
        
        # Display summary statistics
        if use_module_version == 'v2':
            # For v2, show statistics for the primary match
            if 'Composite Score_match_1' in matched_df.columns:
                print(f"\nPrimary Match Score Statistics:")
                scores = matched_df['Composite Score_match_1']
                # Filter out 0 scores (no match results)
                valid_scores = scores[scores > 0]
                if len(valid_scores) > 0:
                    print(f"  Mean: {valid_scores.mean():.2f}")
                    print(f"  Median: {valid_scores.median():.2f}")
                    print(f"  Min: {valid_scores.min():.2f}")
                    print(f"  Max: {valid_scores.max():.2f}")
        else:
            # For v1 and original modules
            if 'Composite Score' in matched_df.columns:
                print(f"\nComposite Score Statistics:")
                print(f"  Mean: {matched_df['Composite Score'].mean():.2f}")
                print(f"  Median: {matched_df['Composite Score'].median():.2f}")
                print(f"  Min: {matched_df['Composite Score'].min():.2f}")
                print(f"  Max: {matched_df['Composite Score'].max():.2f}")
        
    else:
        print("\nNo matches found or an error occurred during processing.")


def main():
    """Main entry point with command-line interface."""
    parser = argparse.ArgumentParser(
        description='Run name matching with configuration support'
    )
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Override the limit of rows to process from File A'
    )
    parser.add_argument(
        '--workers',
        type=int,
        help='Override the number of parallel workers'
    )
    parser.add_argument(
        '--output-dir',
        help='Override the output directory'
    )
    
    args = parser.parse_args()
    
    # Determine base directory (where this script is located)
    base_dir = Path(__file__).parent
    
    try:
        # Load configuration
        config_path = base_dir / args.config if not Path(args.config).is_absolute() else Path(args.config)
        config = load_config(config_path)
        validate_config(config)
        
        # Apply command-line overrides
        if args.limit is not None:
            config['processing']['limit'] = args.limit
        if args.workers is not None:
            config['processing']['max_workers'] = args.workers
        if args.output_dir is not None:
            config['output']['directory'] = args.output_dir
        
        # Run the matching process
        run_name_matching(config, base_dir)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()