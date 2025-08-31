# Healthcare COI Analytics Template v2.0

A professional, enterprise-grade template for analyzing healthcare provider conflicts of interest using CMS Open Payments and Medicare Part D prescription data.

## 🚀 Features

### Core Capabilities
- **Comprehensive Data Integration**: BigQuery-based analysis of Open Payments and Medicare Part D data
- **Advanced Statistical Analysis**: Correlation analysis with confidence intervals, effect sizes, and multiple hypothesis testing
- **ML-Based Risk Scoring**: Anomaly detection and multi-factor risk assessment using machine learning
- **Specialty-Specific Analysis**: Vulnerability assessment by medical specialty and provider type
- **Professional Reporting**: Multiple report formats (investigative, compliance, executive)
- **Data Visualization**: Publication-ready charts with Conflixis branding

### Key Analyses
- Payment-prescription correlations with influence factors (up to 400x+)
- Provider vulnerability assessment (MD vs NP/PA differential analysis)
- Payment tier ROI analysis (revealing 2,300x returns on <$100 payments)
- Temporal trend analysis with year-over-year growth metrics
- Specialty-specific vulnerability patterns
- Risk scoring and anomaly detection using Isolation Forest

## 📁 New Modular Architecture (v2.0)

```
182-healthcare-coi-analytics-template/
├── src/                          # Core analysis modules
│   ├── data/                     # Data management
│   │   ├── bigquery_connector.py # Singleton BigQuery client with caching
│   │   ├── data_loader.py        # Unified data loading
│   │   └── data_validator.py     # Data quality validation
│   ├── analysis/                 # Analysis engines
│   │   ├── open_payments.py      # Payment analysis
│   │   ├── prescriptions.py      # Prescription analysis
│   │   ├── correlations.py       # Statistical correlations
│   │   ├── risk_scoring.py       # ML risk assessment
│   │   └── specialty_analysis.py # Specialty patterns
│   └── reporting/                # Report generation
│       ├── report_generator.py   # Multi-format reports
│       └── visualizations.py     # Chart generation
├── pipelines/                    # Analysis orchestration
│   └── full_analysis.py          # Complete pipeline
├── config/                       # Configuration files
├── data/                        # Data directories
├── reports/                     # Generated reports
└── docs/                        # Documentation
```

## 🔧 Quick Start

### Prerequisites
```bash
# Python 3.8+
# Google Cloud Project with BigQuery access
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