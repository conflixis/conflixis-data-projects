# Multi-Tier Name Matching POC

A sophisticated name matching system that combines traditional fuzzy matching algorithms with OpenAI's language models to achieve high-accuracy entity resolution for healthcare organizations.

## Overview

This proof-of-concept implements a 4-tier progressive matching system:

1. **Tier 1**: Python-based fuzzy matching (RapidFuzz, Jellyfish)
2. **Tier 2**: OpenAI GPT-4 analysis for entity recognition
3. **Tier 3**: OpenAI with web search for real-time validation
4. **Tier 4**: Human review queue for low-confidence matches

Each tier progressively increases in sophistication and cost, with automatic escalation only when needed.

## Features

- **Multi-algorithm fuzzy matching** with healthcare-specific preprocessing
- **OpenAI integration** for intelligent entity analysis
- **Web search validation** for real-time verification
- **Confidence scoring** with configurable thresholds
- **CSV-based storage** for easy integration
- **Interactive dashboard** for reviewing results
- **Audit trail** of all matching decisions

## Installation

### Prerequisites

- Python 3.8 or higher
- OpenAI API key

### Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure OpenAI API key:
```bash
cp .env.example .env
# Edit .env and add your OpenAI API key
```

## Usage

### Basic Usage

1. Prepare your input data in CSV format with columns `name_a` and `name_b`:
```csv
name_a,name_b
"St. Mary's Hospital","Saint Marys Medical Center"
"ABC Healthcare LLC","ABC Health System"
```

2. Run the matching pipeline:
```bash
python run_matching.py --input data/input.csv
```

3. View results in the dashboard:
```bash
open ui/dashboard.html
```

### Command Line Options

```bash
python run_matching.py [options]

Options:
  --input FILE      Input CSV file (default: data/input.csv)
  --threshold N     Confidence threshold 0-100 (default: 90)
  --quiet          Suppress verbose output
```

### Testing Individual Tiers

Test Tier 1 (Fuzzy Matching):
```bash
python src/tier1_fuzzy.py
```

Test Tier 2 (OpenAI Analysis):
```bash
python src/tier2_openai.py
```

Test Tier 3 (Web Search):
```bash
python src/tier3_websearch.py
```

## How It Works

### Processing Flow

1. **Input**: Name pairs are loaded from CSV
2. **Tier 1**: Each pair is first processed through fuzzy matching algorithms
   - If confidence ≥ 90%, mark as MATCHED
   - Otherwise, proceed to Tier 2
3. **Tier 2**: OpenAI analyzes the names for entity matching
   - Aggregated confidence = 40% Tier 1 + 60% Tier 2
   - If confidence ≥ 90%, mark as MATCHED
   - Otherwise, proceed to Tier 3
4. **Tier 3**: OpenAI searches the web for validation
   - Final confidence = 30% Tier 1 + 40% Tier 2 + 30% Tier 3
   - If confidence ≥ 90%, mark as MATCHED
   - Otherwise, add to REVIEW queue
5. **Output**: Results saved to timestamped CSV files

### Tier 1: Fuzzy Matching

Uses multiple algorithms:
- **RapidFuzz**: Token-based similarity
- **Jellyfish**: Jaro-Winkler similarity
- **Custom preprocessing**: Healthcare-specific abbreviations

### Tier 2: OpenAI Analysis

- Model: GPT-4o-mini (cost-efficient)
- Analyzes: Abbreviations, subsidiaries, name variations
- Returns: Confidence score and reasoning

### Tier 3: Web Search Validation

- Model: GPT-4o (with web search)
- Researches: Official websites, mergers, addresses
- Returns: Confidence score with evidence

## Output Files

- `data/results_YYYYMMDD_HHMMSS.csv`: Timestamped results
- `data/results_latest.csv`: Most recent results (for dashboard)
- `data/review_queue.csv`: Items needing human review
- `data/websearch_evidence.jsonl`: Web search audit trail

## Dashboard Features

The interactive dashboard (`ui/dashboard.html`) provides:

- **Summary statistics**: Match rates by tier
- **Results table**: All processed pairs with scores
- **Review queue**: Items needing human verification
- **Analytics**: Score distributions and averages
- **Export**: Download filtered results as CSV

## Configuration

Edit `run_matching.py` or pass command-line arguments:

```python
config = {
    'confidence_threshold': 90.0,  # Minimum score for auto-match
    'tier1_weight': 1.0,           # Weight when only Tier 1 used
    'tier2_weights': {              # Weights for Tier 2 aggregation
        'tier1': 0.4,
        'tier2': 0.6
    },
    'tier3_weights': {              # Weights for Tier 3 aggregation
        'tier1': 0.3,
        'tier2': 0.4,
        'tier3': 0.3
    }
}
```

## Cost Estimation

- **Tier 1**: Free (local processing)
- **Tier 2**: ~$0.001 per name pair (GPT-4o-mini)
- **Tier 3**: ~$0.02 per name pair (GPT-4o with web search)

Typical distribution (based on test data):
- 30-40% matched at Tier 1 (no cost)
- 20-30% matched at Tier 2 (~$0.001 each)
- 10-20% processed at Tier 3 (~$0.02 each)
- 10-20% sent to human review

**Average cost**: ~$0.005-0.01 per name pair

## Performance

- **Tier 1**: <0.1 seconds per pair
- **Tier 2**: 1-2 seconds per pair
- **Tier 3**: 3-5 seconds per pair
- **Total throughput**: ~10-20 pairs per minute

## Troubleshooting

### OpenAI API Key Not Working
- Ensure the key is set in `.env` file
- Check API key permissions at https://platform.openai.com
- Verify you have credits/billing set up

### No Results Appearing
- Check that input CSV has correct column names (`name_a`, `name_b`)
- Ensure no extra spaces in column headers
- Verify CSV is properly formatted

### Dashboard Not Loading
- Ensure you've run the pipeline at least once
- Check that `data/results_latest.csv` exists
- Open dashboard from the correct path

## Development

### Project Structure
```
005-core-name-matching-test/
├── src/                    # Core matching logic
│   ├── tier1_fuzzy.py     # Fuzzy matching algorithms
│   ├── tier2_openai.py    # OpenAI entity analysis
│   └── tier3_websearch.py # Web search validation
├── data/                   # Input/output data
│   ├── input.csv          # Input name pairs
│   └── results_*.csv      # Output results
├── ui/                     # Web dashboard
│   └── dashboard.html     # Results viewer
├── run_matching.py        # Main orchestrator
├── requirements.txt       # Python dependencies
├── .env.example          # Environment template
└── README.md             # This file
```

### Adding New Matching Algorithms

To add new algorithms to Tier 1, edit `src/tier1_fuzzy.py`:

```python
def fuzzy_match(name_a, name_b):
    scores = {
        'existing_algo': ...,
        'your_new_algo': your_algorithm(name_a, name_b)
    }
    
    weights = {
        'existing_algo': 0.X,
        'your_new_algo': 0.Y  # Ensure weights sum to 1.0
    }
```

## License

Copyright 2025 Conflixis. All rights reserved.

## Support

For issues or questions, contact the Data Analytics team or create a ticket in the DA Jira project.