# OP-RX Manufacturer Name Matching Project

## Project Overview
Match manufacturer names between CMS Open Payments (OP) and Medicare Part D Prescriptions (RX) databases using Tier 2 name matching methodology.

## Objectives
- Extract unique manufacturer names from both databases
- Apply fuzzy matching with AI enhancement
- Create definitive mapping table for manufacturer name reconciliation
- Achieve >95% accuracy in matching

## Data Sources
- **Open Payments**: `data-analytics-389803.OPR_PHYS_OTH.GNRL_PAYMENTS_optimized`
- **Prescriptions**: `data-analytics-389803.MDRX_PRESCRIPTIONS.MED_D_DATASETS_optimized`

## Methodology
Using Tier2NameMatcher from `src/analysis/01-core-name-matching/tier2_matcher.py`:
1. Fuzzy matching (85% threshold)
2. AI enhancement for low-confidence cases
3. Manual review for edge cases

## Project Structure
```
192-op-rx-manufacturer-matching/
├── scripts/           # Data extraction and matching scripts
├── data/
│   ├── input/        # Raw manufacturer name lists
│   ├── interim/      # Processing artifacts
│   └── output/       # Final mapping tables
├── src/              # Custom matching logic
├── docs/             # Documentation
└── tests/            # Test cases
```

## Expected Output
- Comprehensive manufacturer mapping table
- Match confidence scores
- Unmatched manufacturer analysis
- Data quality report