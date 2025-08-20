# Archived Legacy Name Matching Implementations

This folder contains legacy name matching implementations that have been superseded by the production-ready `tier2_matcher.py` module.

## Archived Files

### core_name_matching.py
- **Status**: Legacy
- **Purpose**: Original standalone script with hardcoded paths
- **Accuracy**: ~70-80% (fuzzy only)
- **Why archived**: Replaced by class-based implementations

### name_matcher.py
- **Status**: Legacy v1
- **Purpose**: First class-based refactoring
- **Accuracy**: ~80-85% (fuzzy only)
- **Why archived**: No confidence thresholds, always forces a match

### name_matcher_v2.py
- **Status**: Legacy v2
- **Purpose**: Enhanced fuzzy matcher with confidence levels
- **Accuracy**: ~85% (fuzzy only, no AI)
- **Why archived**: Superseded by Tier 2 with AI enhancement

### test_name_matcher.py
- **Status**: Legacy tests
- **Purpose**: Unit tests for legacy implementations
- **Why archived**: Tests for deprecated modules

## Migration Guide

To migrate from legacy modules to the production Tier 2 matcher:

### From name_matcher.py:
```python
# Old
from name_matcher import NameMatcher
matcher = NameMatcher()
results = matcher.find_matches(df_a, df_b, 'name_a', 'name_b')

# New
from tier2_matcher import Tier2NameMatcher
matcher = Tier2NameMatcher()
results = matcher.match_dataframes(df_a, df_b, 'name_a', 'name_b')
```

### From name_matcher_v2.py:
```python
# Old
from name_matcher_v2 import EnhancedNameMatcher
matcher = EnhancedNameMatcher(min_score_threshold=60.0)

# New
from tier2_matcher import Tier2NameMatcher
matcher = Tier2NameMatcher(decision_threshold=60.0)
```

## Performance Comparison

| Module | Accuracy | Uses AI | Production Ready |
|--------|----------|---------|------------------|
| core_name_matching.py | ~75% | No | No |
| name_matcher.py | ~80% | No | No |
| name_matcher_v2.py | ~85% | No | No |
| **tier2_matcher.py** | **96.9%** | **Yes** | **Yes** |

## Note

These files are kept for reference only. For any new development, use the production `tier2_matcher.py` module which achieves 96.9% accuracy by combining fuzzy matching with AI enhancement.