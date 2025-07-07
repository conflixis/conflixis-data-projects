# Core Name Matching Module

This module provides fuzzy name matching capabilities for comparing names between two datasets. It uses multiple string similarity algorithms to find the best matches, with a particular focus on organization/entity name matching.

## Latest Updates (v2)

The enhanced `name_matcher_v2.py` module adds:
- **Minimum Score Thresholds**: Configurable threshold below which matches are considered "No Match"
- **Top-N Matches**: Return multiple potential matches instead of just the best one
- **Confidence Levels**: Automatic classification of matches as High/Medium/Low/No Match
- **Better Logging**: Structured logging instead of print statements
- **Improved Preprocessing**: More abbreviation handling (LLC, Ltd., Inc.)
- **No Forced Matches**: Returns "No Match" when no good matches exist above threshold

## Overview

The name matching module uses a combination of similarity algorithms:
- **RapidFuzz**: Basic string similarity using Levenshtein distance
- **Jellyfish**: Jaro-Winkler similarity for phonetic matching
- **TheFuzz**: Token-based matching that handles word order variations
- **Token-Based**: Custom algorithm for matching individual words
- **First-Word**: Gives high weight to matching first words (important for organizations)
- **Partial Match**: Handles cases where one name is a subset of another

## Files

- `core_name_matching.py` - Original standalone script
- `name_matcher.py` - Refactored class-based implementation (v1)
- `name_matcher_v2.py` - Enhanced implementation with thresholds and confidence levels
- `test_name_matcher.py` - Unit tests for all implementations
- `__init__.py` - Module initialization

## Usage

### As a Standalone Script

The original `core_name_matching.py` can be run directly, but it has hardcoded paths and configuration.

### As an Imported Module

#### Using the Enhanced v2 Module (Recommended)

```python
from name_matcher_v2 import EnhancedNameMatcher
import pandas as pd

# Initialize with threshold and confidence settings
matcher = EnhancedNameMatcher(
    min_score_threshold=60.0,     # Minimum score to consider a match
    return_top_n=3,               # Return top 3 matches per name
    confidence_thresholds={
        'high': 80.0,
        'medium': 60.0,
        'low': 40.0
    }
)

# Load your data
df_a = pd.read_csv('file_a.csv')
df_b = pd.read_csv('file_b.csv')

# Perform matching
results = matcher.find_matches(
    df_a, df_b,
    column_name_a='name',
    column_name_b='Name',
    chunk_size=50,
    max_workers=10
)

# Results include confidence levels and multiple matches
print(results[['name', 'Name_match_1', 'Composite Score_match_1', 'Confidence_match_1']])
```

#### Using the Original Module

```python
from name_matcher import NameMatcher
import pandas as pd

# Initialize the matcher
matcher = NameMatcher(
    stopwords={'inc', 'corp', 'llc', 'ltd'},
    scoring_weights={
        'rapidfuzz': 0.1,
        'jellyfish': 0.2,
        'thefuzz': 0.1,
        'token_based': 0.1,
        'first_word': 0.4,
        'partial_match': 0.1
    }
)

# Perform matching (always returns best match, no threshold)
results = matcher.find_matches(df_a, df_b, 'name', 'Name')
```

### Using the Runner Script

For the most flexible usage, use the runner script in your project:

1. Copy the example configuration from `projects/005-core-name-matching-test/config.yaml`
2. Create a runner script (see `projects/005-core-name-matching-test/run_name_matching.py`)
3. Run with: `poetry run python run_name_matching.py`

## Configuration Options

### Stopwords
Words to ignore during preprocessing (e.g., 'inc', 'corporation', 'llc')

### Scoring Weights
Adjust the importance of each algorithm (must sum to 1.0):
- `rapidfuzz`: Basic string similarity (default: 0.1)
- `jellyfish`: Phonetic similarity (default: 0.2)
- `thefuzz`: Token-based similarity (default: 0.1)
- `token_based`: Word matching (default: 0.1)
- `first_word`: First word exact match (default: 0.4)
- `partial_match`: Substring matching (default: 0.1)

### Processing Parameters
- `chunk_size`: Number of rows to process in parallel (default: 50)
- `max_workers`: Maximum parallel workers (default: 10)
- `limit`: Maximum rows to process from first file (optional)

## Output Format

The module generates a CSV file with the following columns:
- Original name from File A
- Preprocessed name from File A
- Best matching name from File B
- Preprocessed name from File B
- Individual algorithm scores (0-100)
- Composite score (weighted average)

## Performance Tips

1. **Use parallel processing**: Adjust `max_workers` based on your CPU cores
2. **Preprocess File B**: The module caches preprocessed names for better performance
3. **Limit initial tests**: Use the `limit` parameter to test with smaller datasets first
4. **Adjust chunk size**: Larger chunks may be more efficient for large datasets

## Dependencies

```bash
poetry add jellyfish rapidfuzz pandas numpy
```

## Example Project

See `/projects/005-core-name-matching-test/` for a complete example implementation with:
- Configuration file (`config.yaml`)
- Runner script (`run_name_matching.py`)
- Sample input/output data

## Algorithm Details

### Preprocessing
1. Convert to lowercase
2. Replace common abbreviations (e.g., "corp." → "corporation")
3. Remove non-alphanumeric characters (keeping spaces and hyphens)
4. Remove stopwords

### Composite Score Calculation
The final score is a weighted average of all algorithm scores. The default weights emphasize first-word matching (40%) as this is often most important for organization names.

### Parallel Processing
The module processes data in chunks using Python's multiprocessing, significantly improving performance for large datasets.

## Troubleshooting

### Common Issues

1. **Module not found errors**: Ensure you're running with `poetry run python` or in an activated poetry shell
2. **Memory issues**: Reduce `chunk_size` or process smaller batches using `limit`
3. **Poor matches**: Adjust scoring weights to emphasize different algorithms
4. **Missing dependencies**: Run `poetry add jellyfish rapidfuzz`

### Performance Optimization

For very large datasets (>100k rows):
1. Increase `chunk_size` to 100-200
2. Use more workers (up to CPU core count)
3. Consider preprocessing names in advance
4. Filter datasets to relevant entries before matching