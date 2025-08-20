#!/usr/bin/env python3
"""
Update all script paths to use the new test-data directory structure
"""

import os
import re
from pathlib import Path

# Define path replacements
REPLACEMENTS = [
    # Input files
    ('test_data/test_dataset.csv', 'test-data/test-data-inputs/test_dataset.csv'),
    ('test_data/test_dataset_100.csv', 'test-data/test-data-inputs/test_dataset_100.csv'),
    ('test_data/test_dataset_small.csv', 'test-data/test-data-inputs/test_dataset_small.csv'),
    ('test_data/test_dataset_50.csv', 'test-data/test-data-inputs/test_dataset_50.csv'),
    
    # Output files - results
    ('test_data/test_results_', 'test-data/test-results/test_results_'),
    
    # Reports
    ('test_data/reports/', 'test-data/reports/'),
    ('test_data/test_comparison_dashboard.html', 'test-data/reports/test_comparison_dashboard.html'),
    
    # Log files
    ('test_data/gpt5_mini_run.log', 'test-data/gpt5_mini_run.log'),
    
    # Generic test_data directory references
    ('\'test_data\'', '\'test-data/test-data-inputs\''),
    ('"test_data"', '"test-data/test-data-inputs"'),
    ('\'test_data/', '\'test-data/'),
    ('"test_data/', '"test-data/'),
]

def update_file(filepath):
    """Update a single file with new paths"""
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    original = content
    
    for old_path, new_path in REPLACEMENTS:
        content = content.replace(old_path, new_path)
    
    if content != original:
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"✓ Updated: {filepath}")
        return True
    else:
        print(f"  No changes: {filepath}")
        return False

def main():
    """Update all Python scripts in the scripts directory"""
    
    scripts_dir = Path(__file__).parent
    
    updated_count = 0
    for py_file in scripts_dir.glob('*.py'):
        if py_file.name == 'update_paths.py':
            continue
        if update_file(py_file):
            updated_count += 1
    
    print(f"\nUpdated {updated_count} files")

if __name__ == "__main__":
    main()