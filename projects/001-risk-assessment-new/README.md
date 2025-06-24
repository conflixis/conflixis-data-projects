# Risk Assessment Notebook - Setup and Usage Guide

## Overview
This notebook analyzes BCBS NPI payment data (7.5M records) stored in Google BigQuery. All heavy computations are performed in BigQuery with only aggregated results downloaded locally.

## Prerequisites
- Python 3.12+
- Poetry (package manager)
- Google Cloud credentials with BigQuery access
- Active internet connection for BigQuery queries

## Quick Start

### 1. Navigate to Project Directory
```bash
cd /home/incent/conflixis-analytics
```

### 2. Activate Poetry Environment
```bash
poetry shell
```

### 3. Verify Setup (Optional)
```bash
poetry run python test_setup.py
```

### 4. Start Jupyter Notebook
```bash
jupyter notebook
```

### 5. Open the Notebook
Navigate to: `projects/001-risk-assessment-new/Risk_assessment_new.ipynb`

## Running the Notebook

### Execution Order
Run cells in sequential order:
1. **Initialization** - Sets up environment and BigQuery client
2. **Query Configuration** - Creates helper functions
3. **Data Analysis** - Run queries and generate visualizations
4. **Summary & Export** - View insights and export results

### Expected Outputs
- Visualizations will appear inline
- Query performance metrics shown for each query
- Results exported to `outputs/` directory:
  - `bcbs_risk_assessment_summary.xlsx`
  - `top_entities_by_count.csv`
  - `top_entities_by_value.csv`
  - `specialty_analysis.csv`
  - `executive_summary.txt`

## Troubleshooting

### BigQuery Authentication Error
If you see authentication errors:
1. Check that `GOOGLE_APPLICATION_CREDENTIALS` is set correctly
2. Verify the service account JSON file exists in `common/`
3. Ensure the service account has BigQuery access

### Missing Dependencies
If packages are missing:
```bash
poetry install
poetry add <missing-package>
```

### Jupyter Not Starting
If Jupyter won't start:
```bash
poetry add jupyter notebook ipykernel
poetry run python -m ipykernel install --user --name=conflixis-analytics
```

## Key Features
- ✅ All computations in BigQuery (no large data downloads)
- ✅ Professional visualizations with matplotlib/seaborn
- ✅ Error handling and performance tracking
- ✅ Automated report generation
- ✅ Excel and CSV export functionality

## Data Overview
- **Total Records**: 7,514,026
- **Main Table**: `data-analytics-389803.Conflixis_sandbox.bcbs_npi_ra_v2`
- **Key Analyses**: Entity payments, specialty distributions, payment categories

## Notes
- The notebook uses BigQuery's caching to optimize repeated queries
- All queries are designed to aggregate data before download
- Visualizations are generated from aggregated data only

## Support
For issues or questions:
1. Check the implementation tracking document: `IMPLEMENTATION_TRACKING.md`
2. Verify setup with: `poetry run python test_setup.py`
3. Review BigQuery logs in the Google Cloud Console