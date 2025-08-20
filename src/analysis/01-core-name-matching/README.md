# Core Name Matching Module

Production-ready name matching module for healthcare organizations, featuring the **Tier 2 approach** that achieves 96.9% accuracy by combining fuzzy matching with AI enhancement.

## 🎯 Quick Start with Tier 2 Matcher (Recommended)

The `tier2_matcher.py` module is our production-ready implementation based on extensive testing with 1,000+ healthcare entity pairs.

### Installation

```bash
pip install -r requirements.txt
```

### Basic Usage

```python
from tier2_matcher import Tier2NameMatcher

# Initialize the matcher
matcher = Tier2NameMatcher(
    fuzzy_threshold=85.0,    # Use fuzzy only if confidence >= 85%
    decision_threshold=50.0,  # Final match threshold
    model='gpt-4o-mini'      # OpenAI model
)

# Match two names
result = matcher.match_pair(
    "Johns Hopkins Hospital",
    "Johns Hopkins Medical Center"
)

print(f"Match: {result['is_match']}")
print(f"Confidence: {result['final_score']:.1f}%")
```

### Batch Processing

```python
import pandas as pd
from tier2_matcher import Tier2NameMatcher

# Load your data
df_providers = pd.read_csv('providers.csv')
df_entities = pd.read_csv('entities.csv')

# Initialize matcher
matcher = Tier2NameMatcher()

# Match all providers against entities
results = matcher.match_dataframes(
    df_providers, 
    df_entities,
    name_col_a='provider_name',
    name_col_b='entity_name',
    use_ai=True,  # Enable AI enhancement
    top_n=3       # Return top 3 matches per name
)

# Save results
results.to_csv('match_results.csv', index=False)
```

## 📊 Performance Metrics

Based on testing with 1,000 healthcare organization name pairs:

| Approach | Accuracy | Precision | Recall | F1 Score | API Calls |
|----------|----------|-----------|--------|----------|-----------|
| **Tier 2** | **96.9%** | 0.940 | 0.985 | 0.962 | 75.3% |
| Fuzzy Only | 84.7% | 1.000 | 0.618 | 0.764 | 0% |

## 🏗️ Module Architecture

### Production Module (tier2_matcher.py) - RECOMMENDED

Our flagship implementation combining:
- **Multiple fuzzy algorithms**: RapidFuzz, Jellyfish, token-based matching
- **Healthcare preprocessing**: Handles medical abbreviations and terminology
- **AI enhancement**: Uses OpenAI for low-confidence cases
- **Optimized thresholds**: Based on empirical testing
- **Parallel processing**: Concurrent API calls for speed

Key features:
- 96.9% accuracy on healthcare entities
- Configurable confidence thresholds
- Built-in statistics tracking
- Error handling and fallbacks
- Batch processing support

### Archived Legacy Modules

Legacy implementations have been moved to the `archive/` folder:
- `archive/name_matcher_v2.py` - Enhanced fuzzy matcher (85% accuracy, no AI)
- `archive/name_matcher.py` - Original class-based implementation (80% accuracy)
- `archive/core_name_matching.py` - Initial standalone script (75% accuracy)
- `archive/test_name_matcher.py` - Legacy unit tests

See `archive/README.md` for migration guide.

## 🔧 Configuration

### Environment Variables

Create a `.env` file:
```bash
OPENAI_API_KEY=your-api-key-here
```

### Tier 2 Parameters

```python
matcher = Tier2NameMatcher(
    fuzzy_threshold=85.0,      # Min confidence for fuzzy-only match
    decision_threshold=50.0,    # Min confidence for final match
    model='gpt-4o-mini',       # OpenAI model (gpt-4o-mini recommended)
    max_workers=5              # Concurrent API workers
)
```

### Preprocessing Options

The Tier 2 matcher automatically handles:
- Healthcare abbreviations (hosp→hospital, med→medical)
- Common suffixes (Inc., LLC, Ltd.)
- Special characters and punctuation
- Case normalization

## 📈 How It Works

### Two-Tier Approach

1. **Tier 1: Fuzzy Matching**
   - Runs multiple algorithms (Levenshtein, Jaro-Winkler, token-based)
   - Calculates weighted composite score
   - If score ≥ 85%, accept match immediately

2. **Tier 2: AI Enhancement**
   - For scores < 85%, send to OpenAI
   - AI considers context, abbreviations, typos
   - Final score = 40% fuzzy + 60% AI
   - Accept if final score ≥ 50%

### Algorithm Weights

Default weights optimized for healthcare entities:
- Exact match: 25%
- Ratio (Levenshtein): 15%
- Token sort: 15%
- Jaro-Winkler: 15%
- Partial match: 10%
- Token set: 10%
- First word match: 10%

## 💰 Cost Analysis

Using gpt-4o-mini (recommended):
- ~$0.15 per 1,000 name pairs
- 75% of pairs need AI enhancement
- ~750 API calls per 1,000 pairs

## 🚀 Advanced Usage

### Custom Preprocessing

```python
class CustomMatcher(Tier2NameMatcher):
    def preprocess_name(self, name: str) -> str:
        # Add your custom preprocessing
        name = super().preprocess_name(name)
        # Additional custom logic
        return name
```

### Monitoring Performance

```python
# Process names
results = matcher.match_batch(name_pairs)

# Get statistics
stats = matcher.get_statistics()
print(f"Fuzzy only: {stats['fuzzy_only_pct']:.1f}%")
print(f"AI enhanced: {stats['ai_enhanced_pct']:.1f}%")
print(f"API errors: {stats['api_error_rate']:.1f}%")
```

### Error Handling

```python
try:
    result = matcher.match_pair(name_a, name_b)
except Exception as e:
    # Fallback to fuzzy-only matching
    result = matcher.match_pair(name_a, name_b, use_ai=False)
```

## 📝 Output Format

Each match result includes:

```python
{
    'name_a': 'Original Name A',
    'name_b': 'Original Name B',
    'fuzzy_score': 75.5,           # Initial fuzzy score
    'ai_score': 95.0,              # AI confidence (if used)
    'final_score': 87.3,           # Combined score
    'is_match': True,              # Final decision
    'confidence_source': 'ai_enhanced',  # or 'fuzzy_high_confidence'
    'fuzzy_details': {...},        # Individual algorithm scores
    'ai_reasoning': 'AI enhanced'  # AI explanation
}
```

## 🧪 Testing

Run the module directly for basic tests:

```bash
python tier2_matcher.py
```

Run full test suite:

```bash
python test_name_matcher.py
```

## 📚 Related Projects

See the complete implementation and testing at:
- `/projects/005-core-name-matching-test/` - Full POC with test datasets
- Results showing 96.9% accuracy on 1,000 healthcare entity pairs

## 🔍 Troubleshooting

### Common Issues

1. **Module not found**: Ensure all requirements are installed
   ```bash
   pip install -r requirements.txt
   ```

2. **API key errors**: Set environment variable
   ```bash
   export OPENAI_API_KEY=your-key-here
   ```

3. **Rate limits**: Reduce `max_workers` parameter
   ```python
   matcher = Tier2NameMatcher(max_workers=2)
   ```

4. **Memory issues**: Process in smaller batches
   ```python
   for chunk in pd.read_csv('large_file.csv', chunksize=100):
       results = matcher.match_dataframes(chunk, reference_df, ...)
   ```

## 📊 Performance Tips

1. **Cache reference data**: Preprocess reference names once
2. **Batch API calls**: Use `match_batch()` for multiple pairs
3. **Adjust thresholds**: Based on your precision/recall needs
4. **Monitor costs**: Track API usage with `get_statistics()`

## 🎯 When to Use Each Module

- **tier2_matcher.py**: Production use, highest accuracy (96.9%)
- **name_matcher_v2.py**: Fuzzy-only with confidence levels
- **name_matcher.py**: Simple fuzzy matching
- **core_name_matching.py**: Legacy reference implementation

## 📈 Accuracy by Match Type

Tier 2 performance on different name variations:
- **Exact matches**: 100%
- **Typos**: 98%
- **Abbreviations**: 98%
- **Word order changes**: 100%
- **Extra words**: 96.8%
- **Different organizations**: 90% (correctly identified as non-matches)

## License

Internal use only - Conflixis proprietary