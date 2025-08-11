# Archived Name Matching Implementations

This directory contains archived versions of the name matching module that have been superseded by the new multi-tier implementation.

## Version History

### v0.1 - Original Implementation (`core_name_matching.py`)
- **Date Archived**: January 2025
- **Description**: Original standalone script with hardcoded paths and configuration
- **Key Features**: Basic fuzzy matching with RapidFuzz and Jellyfish
- **Limitations**: Hardcoded paths, no modularity, single-tier matching

### v0.2 - Class-Based Refactor (`name_matcher.py`)
- **Date Archived**: January 2025
- **Description**: First refactored version with class-based design
- **Key Features**: Configurable stopwords and scoring weights
- **Limitations**: No confidence thresholds, always returns best match

### v0.3 - Enhanced Version (`name_matcher_v2.py`)
- **Date Archived**: January 2025
- **Description**: Enhanced implementation with thresholds and confidence levels
- **Key Features**: 
  - Minimum score thresholds
  - Top-N matches
  - Confidence level classification
  - Better logging
- **Limitations**: Still single-tier, no LLM integration

## Current Implementation

The current multi-tier implementation is located in `/projects/005-core-name-matching-test/` and includes:
- Tier 1: Enhanced fuzzy matching
- Tier 2: OpenAI GPT-4 entity analysis
- Tier 3: OpenAI web search validation
- Tier 4: Human review queue

See the main project README for details on the new implementation.