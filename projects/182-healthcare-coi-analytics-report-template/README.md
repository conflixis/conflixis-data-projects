# Healthcare COI Analytics Report Template

## Overview
This template provides a comprehensive framework for analyzing healthcare provider conflicts of interest (COI) through pharmaceutical/medical device payment data and prescribing patterns. Based on proven methodologies from multiple health system analyses, this template enables rapid deployment of COI analytics for any healthcare organization.

## Features
- **Payment Analysis**: Comprehensive Open Payments data analysis
- **Prescription Patterns**: Medicare Part D and commercial claims analysis  
- **Correlation Analytics**: Payment-prescription influence calculations
- **Risk Assessment**: Multi-factor risk scoring for providers and drugs
- **Provider Vulnerability**: Differential analysis by provider type (MD/DO vs NP/PA)
- **Automated Reporting**: Generate professional reports in multiple formats

## Quick Start

### 1. Prerequisites
```bash
# Python 3.8+
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your GCP credentials
```

### 2. Configure Your Analysis
Edit `CONFIG.yaml`:
```yaml
health_system:
  name: "Your Health System Name"
  npi_file: "data/inputs/your_provider_npis.csv"
  
analysis:
  start_year: 2020
  end_year: 2024
```

### 3. Prepare Your Data
Place your provider NPI list in `data/inputs/`:
```csv
NPI,Full_Name,Primary_Specialty
1234567890,John Doe MD,Internal Medicine
```

### 4. Run Analysis
```bash
# Validate setup
python scripts/00_validate_setup.py

# Run full analysis pipeline
python scripts/run_full_analysis.py

# Or run individual components
python scripts/01_analyze_op_payments.py
python scripts/02_analyze_prescriptions.py
python scripts/03_payment_influence.py
python scripts/04_risk_assessment.py
python scripts/05_generate_report.py
```

### 5. View Results
Reports are generated in `data/output/`:
- `[TIMESTAMP]_coi_report.md` - Full markdown report
- `[TIMESTAMP]_coi_report.pdf` - PDF version
- `[TIMESTAMP]_coi_analysis.xlsx` - Excel workbook with all data

## Project Structure
```
182-healthcare-coi-analytics-report-template/
├── CONFIG.yaml                    # Main configuration
├── requirements.txt              # Python dependencies
├── .env.example                  # Environment variables template
│
├── scripts/                      # Analysis scripts
│   ├── 00_validate_setup.py    # Validate environment and data
│   ├── 01_analyze_op_payments.py
│   ├── 02_analyze_prescriptions.py
│   ├── 03_payment_influence.py
│   ├── 04_risk_assessment.py
│   ├── 05_generate_report.py
│   ├── run_full_analysis.py    # Run complete pipeline
│   └── utils/                   # Utility modules
│
├── templates/                    # Report templates
│   ├── full_report.md
│   └── sections/                # Individual sections
│
├── config/                       # Configuration files
│   ├── queries/                 # SQL query templates
│   ├── drug_categories.yaml    # Drug categorizations
│   └── risk_thresholds.yaml    # Risk scoring thresholds
│
├── data/                        # Data directories
│   ├── inputs/                 # Your input files
│   ├── processed/              # Intermediate results
│   └── output/                 # Final reports
│
└── docs/                        # Documentation
    ├── USER_GUIDE.md
    ├── METHODOLOGY.md
    └── examples/
```

## Key Metrics Analyzed

### Payment Metrics
- Total payments by category
- Payment trends over time
- Top manufacturers
- Consecutive year relationships
- Payment tier analysis

### Prescription Metrics
- Total prescription volume and cost
- Top prescribed drugs
- Specialty-specific patterns
- Provider type analysis

### Correlation Metrics
- Payment-prescription correlations by drug
- Return on investment (ROI) calculations
- Provider vulnerability scores
- Risk assessments

## Customization

### Adding Custom Queries
Add SQL queries to `config/queries/` and reference in your scripts.

### Modifying Risk Thresholds
Edit `config/risk_thresholds.yaml`:
```yaml
payment_risk:
  low: 0-1000
  medium: 1001-5000
  high: 5001+
  
prescription_risk:
  volume_threshold: 1000
  cost_threshold: 100000
```

### Custom Report Sections
Add markdown templates to `templates/sections/` and update `CONFIG.yaml`:
```yaml
reports:
  sections:
    - executive_summary
    - payment_overview
    - custom_section  # Your new section
```

## Data Sources

### Required
- **Provider NPIs**: List of National Provider Identifiers
- **BigQuery Access**: CMS Open Payments and Medicare Part D data

### Optional
- **Commercial Claims**: Additional prescription data
- **Provider Specialties**: If not in BigQuery

## Support
For questions or issues, contact the Conflixis Data Analytics team.

## License
Proprietary - Conflixis Inc.

---
*Template Version: 1.0.0*  
*Based on DA-175 Corewell Health Analysis*