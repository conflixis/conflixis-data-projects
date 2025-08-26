# Corewell Health Analysis (DA-175)

## Overview
Analysis of Corewell Health provider data and pharmaceutical payment influence patterns across Michigan's largest integrated health system.

## Project Structure
```
175-corewell-health-analysis/
├── data/
│   ├── raw/         # Raw data files
│   ├── processed/   # Cleaned and transformed data
│   └── output/      # Analysis outputs and reports
├── scripts/         # Python scripts for analysis
├── notebooks/       # Jupyter notebooks for exploration
└── docs/           # Documentation and reports
```

## Objectives
- Analyze Corewell Health provider data (14,175 unique NPIs)
- Identify pharmaceutical payment influence patterns
- Assess vulnerability of NPs and PAs to payment influence
- Generate comprehensive analysis report and actionable insights

## Key Findings
- **$49.4 million** in payment-attributable prescriptions from $5.4M in payments
- **9.20x ROI** - 420% higher than national average
- **71.8x ROI for PAs** - extreme vulnerability in mid-level providers
- **23 high-risk providers** with >30% attribution rates

## Deliverables
- Comprehensive analysis report (corewell_health_deep_dive_analysis.md)
- BigQuery NPI table (corewell_health_npis)
- Python analysis scripts for reproducibility
- CSV data exports with detailed metrics

## Getting Started

### Prerequisites
```bash
# Activate virtual environment
source ../../../venv/bin/activate

# Install required packages (if needed)
pip install -r requirements.txt
```

### Data Sources
- BigQuery: `data-analytics-389803`
- Firestore: `conflixis-web`
- Snowflake: External data warehouse

## Jira Ticket
[DA-175](https://conflixis.atlassian.net/browse/DA-175)

## Status
🟢 Active - In Development