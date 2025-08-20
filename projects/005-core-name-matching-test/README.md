# Healthcare Entity Name Matching: Multi-Tier Approach Analysis

## Executive Summary

This project evaluates three different approaches for matching healthcare entity names, comparing accuracy, performance, and complexity. After testing on 1,000 healthcare organization name pairs, we found that a simple two-tier approach (fuzzy matching + AI enhancement) achieves 96.9% accuracy, outperforming more complex implementations.

## 📊 Performance Comparison

| Approach | Accuracy | Precision | Recall | F1 Score | API Calls | Implementation Complexity |
|----------|----------|-----------|--------|----------|-----------|---------------------------|
| **Tier 1** | 84.7% | 1.000 | 0.618 | 0.764 | 0 | Simple |
| **Tier 2** ⭐ | **96.9%** | 0.940 | **0.985** | **0.962** | 753 | Simple |
| **Tier-prod** | 96.5% | 0.929 | 0.988 | 0.958 | 698 | Complex |

## Approach Descriptions

### Tier 1: Fuzzy Matching Only
- **Method**: Multiple fuzzy matching algorithms (Levenshtein, Jaro-Winkler, token-based)
- **Threshold**: 85% confidence required for match
- **Strengths**: Perfect precision (no false positives), no API costs
- **Weaknesses**: Misses 38% of true matches
- **Use Case**: High-stakes scenarios where false positives are unacceptable

### Tier 2: Fuzzy + AI Enhancement (Recommended) ⭐
- **Method**: Fuzzy matching, then OpenAI for cases below 85% confidence
- **Model**: gpt-4o-mini
- **Thresholds**: 
  - ≥85%: Accept fuzzy result
  - <85%: Send to OpenAI
  - ≥50%: Accept as match after AI review
- **Strengths**: Best overall accuracy, simple implementation, excellent balance
- **Use Case**: Production systems requiring high accuracy with minimal manual review

### Tier-prod: Production Approach (PR#1362)
- **Method**: Elasticsearch-style multi-strategy matching + AI enhancement
- **Model**: gpt-4o-mini
- **Thresholds**:
  - >95%: Fast path (skip AI)
  - 30-95%: AI enhancement
  - <30%: No match
- **Note**: Based on conflixis-engine PR#1362 production implementation
- **Finding**: Added complexity doesn't improve results over Tier 2

### Tier 3: Web Search (Not Implemented)
- **Status**: Intentionally skipped
- **Reason**: Tier 2 already achieved 96.9% accuracy, making web search unnecessary
- **Note**: Implementation exists in `archive/legacy-scripts/tier3_websearch.py` for future reference
- **Consideration**: Web search would add significant latency and cost without meaningful accuracy improvement

## Key Findings

### 1. AI Enhancement is Crucial
- Adding AI to fuzzy matching improves accuracy by 12.2% (84.7% → 96.9%)
- Both Tier 2 and Tier-prod achieve ~96.5% accuracy with AI enhancement
- The specific AI model (gpt-4o-mini) provides excellent cost/performance balance

### 2. Simplicity Wins
- Tier 2's simple approach outperforms the complex Elasticsearch-style matching
- Multi-strategy search with boost scores (Tier-prod) doesn't improve accuracy
- Simpler code is easier to maintain and debug

### 3. Threshold Strategy Matters Less Than Expected
- Tier 2: AI for <85% confidence cases (753 API calls)
- Tier-prod: AI for 30-95% confidence cases (698 API calls)
- Similar API usage and accuracy despite different thresholds

### 4. Perfect Precision Has a Cost
- Tier 1 never makes false positives but misses 38% of matches
- Accepting some false positives (Tier 2: 25/1000) dramatically improves recall
- The trade-off is worth it for most business applications

## Test Dataset Characteristics

The 1,000-sample test dataset includes various name variation types:
- **300** completely different names
- **300** similar but different organizations
- **67** case variations
- **62** word order changes
- **62** extra words added/removed
- **59** combined variations
- **53** punctuation differences
- **50** typos
- **47** abbreviations

## Performance by Variation Type

### Tier 2 Accuracy by Type:
- **100%**: Case changes, word order, punctuation
- **98%**: Typos, abbreviations
- **96.8%**: Extra words
- **100%**: Combined variations
- **90%**: Similar but different organizations (hardest category)

## Implementation Details

### Project Structure
```
005-core-name-matching-test/
├── src/
│   ├── tier1_fuzzy.py              # Fuzzy matching implementation
│   ├── tier2_openai.py             # OpenAI enhancement layer
│   └── tier_prod_matching.py       # PR1362 production approach
├── scripts/
│   ├── run_test_matching.py        # Tier 1 test runner
│   ├── run_tier2_optimized.py      # Tier 2 test runner
│   ├── run_tier_prod_test.py       # Tier-prod test runner
│   └── compare_all_tiers.py        # Comparison analysis
├── test-data/
│   ├── test-data-inputs/           # Test datasets
│   └── test-results/               # Test results
└── PR1362-conflixis-engine-test/   # Reference implementation

```

### Requirements
```python
openai>=1.0.0
rapidfuzz>=3.0.0
jellyfish>=0.9.0
pandas>=1.5.0
numpy>=1.20.0
python-dotenv>=0.19.0
```

### Environment Variables
```bash
OPENAI_API_KEY=your-api-key-here
```

## Running the Tests

### Test Individual Tiers
```bash
# Tier 1 (Fuzzy only)
python scripts/run_test_matching.py --test-file test-data/test-data-inputs/test_dataset.csv --algorithm fuzzy

# Tier 2 (Fuzzy + OpenAI)
python scripts/run_tier2_optimized.py --test-file test-data/test-data-inputs/test_dataset.csv --model gpt-4o-mini

# Tier-prod (ES-style + OpenAI)
python scripts/run_tier_prod_test.py --test-file test-data/test-data-inputs/test_dataset.csv --model gpt-4o-mini
```

### Compare All Approaches
```bash
python scripts/compare_all_tiers.py
```

## Cost Analysis

Based on gpt-4o-mini pricing (~$0.15 per 1M input tokens):
- **Tier 1**: $0.00 (no API calls)
- **Tier 2**: ~$0.15 per 1,000 comparisons
- **Tier-prod**: ~$0.14 per 1,000 comparisons

## Recommendations

### For Production Use: Tier 2
✅ **Use Tier 2** for production systems because it offers:
- Highest accuracy (96.9%)
- Simple, maintainable code
- Excellent precision/recall balance
- Proven approach validated by similar results from PR1362

### Optimization Opportunities
1. **Add caching**: Cache exact matches to reduce API calls
2. **Batch processing**: Process multiple pairs in parallel
3. **Threshold tuning**: Adjust 85% threshold based on precision/recall needs
4. **Model selection**: Consider gpt-3.5-turbo for cost savings if accuracy permits

### When to Use Each Tier
- **Tier 1**: When false positives are absolutely unacceptable
- **Tier 2**: General production use with high accuracy requirements
- **Tier-prod**: Not recommended (unnecessary complexity)

## Metrics Explained

- **Accuracy**: Overall percentage of correct predictions (higher is better)
- **Precision**: When system says "match", how often is it correct? (reduces false alarms)
- **Recall**: Of all true matches, how many did we find? (reduces missed matches)
- **F1 Score**: Harmonic mean balancing precision and recall (overall quality metric)

## Confusion Matrix Components

- **TP (True Positives)**: Correctly identified matches
- **FP (False Positives)**: Incorrectly said "match" when different
- **FN (False Negatives)**: Missed matches
- **TN (True Negatives)**: Correctly identified non-matches

## Conclusion

Our analysis demonstrates that a simple two-tier approach combining fuzzy matching with AI enhancement achieves optimal results for healthcare entity name matching. The Tier 2 approach's 96.9% accuracy, combined with its straightforward implementation, makes it the clear choice for production deployment.

The validation against PR1362's production implementation confirms that our simpler approach achieves comparable results without unnecessary complexity, proving that sometimes the simplest solution is the best solution.

## 📁 Project Structure

### Active Directories

#### `/src/` - Core Implementations
- `tier1_fuzzy.py` - Fuzzy matching (84.7% accuracy)
- `tier2_openai.py` - AI-enhanced matching (96.9% accuracy) ⭐
- `tier_prod_matching.py` - PR1362-style approach (96.5% accuracy)

#### `/scripts/` - Production Scripts
**Testing**:
- `run_test_matching.py` - Run Tier 1 tests
- `run_tier2_optimized.py` - Run Tier 2 tests ⭐
- `run_tier_prod_test.py` - Run Tier-prod tests
- `test_tier2_strategies.py` - Compare combination strategies

**Analysis**:
- `compare_all_tiers.py` - Generate comparison reports
- `test_dataset_generator.py` - Create test datasets

#### `/test-data/` - Test Data & Results
- `test-data-inputs/` - Test datasets (committed to repo)
  - `test_dataset.csv` - 1000 samples
  - `test_dataset_100.csv` - 100 samples
  - `test_dataset_small.csv` - 10 samples
- `test-results/` - Generated results (gitignored)
- `reports/` - HTML reports (kept for documentation)

#### Other Files
- `.gitignore` - Git ignore rules
- `requirements.txt` - Python dependencies
- `config.yaml` - Configuration file
- `run_name_matching.py` - Main runner script

### Archived Files (`/archive/`)

The archive directory contains files no longer actively used but preserved for reference:

#### `reference-implementations/`
- PR1362 TypeScript implementation and Python port attempts

#### `test-scripts/`
- Development and debugging scripts used during POC

#### `legacy-scripts/`
- Superseded implementations and utilities

See `archive/README.md` for details about archived files.

## Contact

For questions or improvements, please reference:
- **Project**: DA-156 Name Matching POC
- **Repository**: conflixis-data-projects
- **Related PR**: conflixis-engine#1362