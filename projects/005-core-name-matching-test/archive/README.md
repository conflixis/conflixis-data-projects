# Archived Files

This directory contains files that are no longer actively used but are preserved for reference.

## Directory Structure

### reference-implementations/
- **PR1362-conflixis-engine/**: Original TypeScript implementation from conflixis-engine PR#1362
- **PR1362-conflixis-engine-test/**: Python port attempt of PR1362 (incomplete)

### test-scripts/
Development and testing scripts used during POC development:
- `test_openai_connection.py`: API connection testing
- `test_single_gpt5.py`, `test_gpt5_single.py`: GPT-5 model testing
- `test_tier1_speed.py`: Performance testing for Tier 1
- `test_tier2_simple.py`, `test_tier2_batch.py`: Early Tier 2 tests
- `debug_tier2.py`: Debugging utilities
- `update_paths.py`: Path migration script

### legacy-scripts/
Older versions of scripts that have been superseded:
- `generate_detailed_comparison.py`: Old comparison report generator
- `generate_report_card.py`: Old report card generator
- `run_matching.py`: Original matching runner
- `tier3_websearch.py`: Unused Tier 3 web search implementation

## Migration Notes

### Active Scripts (Keep Using)
The following scripts in the main `scripts/` folder are production-ready:
- `run_test_matching.py` - Tier 1 testing
- `run_tier2_optimized.py` - Tier 2 production runner
- `run_tier_prod_test.py` - Tier-prod (PR1362 approach) testing
- `test_tier2_strategies.py` - Strategy comparison testing
- `compare_all_tiers.py` - Comprehensive comparison analysis
- `test_dataset_generator.py` - Test data generation

### Active Source Files
The following in `src/` are production implementations:
- `tier1_fuzzy.py` - Fuzzy matching implementation
- `tier2_openai.py` - OpenAI enhancement layer
- `tier_prod_matching.py` - PR1362-style implementation

## Why These Were Archived

1. **Reference implementations**: Kept for understanding the original PR1362 approach
2. **Test scripts**: Development/debugging scripts not needed for production
3. **Legacy scripts**: Superseded by better implementations
4. **Tier 3**: Web search approach was not pursued after Tier 2 achieved 96.9% accuracy