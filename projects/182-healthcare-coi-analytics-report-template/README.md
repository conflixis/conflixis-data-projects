# Healthcare COI Analytics Template v2.0

A professional, enterprise-grade template for analyzing healthcare provider conflicts of interest using CMS Open Payments and Medicare Part D prescription data.

## 🚀 Features

### Core Capabilities
- **Comprehensive Data Integration**: BigQuery-based analysis of Open Payments and Medicare Part D data
- **Advanced Statistical Analysis**: Correlation analysis with confidence intervals, effect sizes, and multiple hypothesis testing
- **ML-Based Risk Scoring**: Anomaly detection and multi-factor risk assessment using machine learning
- **Specialty-Specific Analysis**: Vulnerability assessment by medical specialty and provider type
- **LLM-Powered Narrative Generation**: Claude API integration for investigative journalism-style reports with anti-hallucination safeguards
- **Modular Section Regeneration**: Ability to regenerate individual report sections without re-running full analysis
- **Data Accuracy Validation**: Multi-layer validation to ensure LLM uses only actual data, never fictional numbers
- **Professional Reporting**: Multiple report formats (investigative, compliance, executive) with markdown tables
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
│       ├── llm_client.py         # Claude API integration
│       ├── data_mapper.py        # Maps analysis data to report sections
│       ├── output_validator.py   # Validates LLM output for accuracy
│       ├── section_prompts.yaml  # LLM prompts for each section
│       └── visualizations.py     # Chart generation
├── pipelines/                    # Analysis orchestration
│   └── full_analysis.py          # Complete pipeline
├── regenerate_section.py         # Regenerate individual report sections
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

#### Using the CLI Interface
```bash
# Validate setup and data quality
python cli.py validate

# Run full analysis with investigative report style
python cli.py analyze --style investigative

# Other report styles available
python cli.py analyze --style compliance    # For compliance teams
python cli.py analyze --style executive     # For C-suite executives

# Additional options
python cli.py analyze --force-reload        # Force reload data from BigQuery
python cli.py analyze --no-viz              # Skip visualization generation

# Export specific data
python cli.py export payments --output exports/payments.csv
python cli.py export prescriptions --output exports/rx.parquet

# Check system info
python cli.py info

# Clean old cache files
python cli.py clean --days 7
```

#### Regenerating Individual Sections
If you need to regenerate a specific section of an existing report (e.g., to improve the executive summary):

```bash
# Regenerate just the executive summary
python regenerate_section.py executive_summary reports/existing_report.md reports/updated_report.md

# Available sections:
# - executive_summary
# - payment_overview
# - prescription_patterns
# - influence_analysis
# - payment_tiers
# - provider_types
# - consecutive_years
# - risk_assessment
# - recommendations
```

### 5. View Results
Reports are generated in `data/output/`:
- `ramc_coi_report_[timestamp].md` - Full investigative report
- `ramc_coi_report_[timestamp].pdf` - PDF version (if configured)
- `ramc_coi_analysis_[timestamp].xlsx` - Excel workbook with all data

## Current Project Structure
```
182-healthcare-coi-analytics-report-template/
├── cli.py                        # Main CLI interface
├── CONFIG.yaml                   # Main configuration
├── requirements.txt              # Python dependencies
├── .env.example                  # Environment variables template
│
├── src/                          # Core analysis modules
│   ├── data/                     # Data management
│   │   ├── bigquery_connector.py # Singleton BigQuery client
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
│       ├── llm_client.py         # Claude API integration
│       ├── data_mapper.py        # Maps analysis data to report sections
│       ├── output_validator.py   # Validates LLM output for accuracy
│       ├── section_prompts.yaml  # LLM prompts for each section
│       └── visualizations.py     # Chart generation
│
├── pipelines/                    # Analysis orchestration
│   └── full_analysis.py          # Complete pipeline
├── regenerate_section.py         # Regenerate individual report sections
│
├── config/                       # Configuration files
│   └── queries/                  # SQL query templates
│
├── scripts/                      # Utility scripts
│   ├── queries/                  # SQL templates
│   └── utils/                    # Helper utilities
│
├── data/                         # Data directories
│   ├── inputs/                  # Your input files
│   ├── processed/               # Intermediate results
│   └── output/                  # Final reports
│
└── docs/                         # Documentation
    └── instructions/             # Report style instructions
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

## CLI Commands Reference

### Main Commands
- `python cli.py analyze` - Run full COI analysis pipeline
- `python cli.py validate` - Validate data quality and configuration
- `python cli.py clean` - Clean old cached and processed files
- `python cli.py export` - Export analysis results to file
- `python cli.py info` - Display system and configuration information

### Analyze Command Options
```bash
python cli.py analyze [OPTIONS]

Options:
  --config PATH           Configuration file path (default: CONFIG.yaml)
  --force-reload         Force reload data from BigQuery
  --style [investigative|compliance|executive]  Report style (default: investigative)
  --format [markdown|html]  Output format (default: markdown)
  --no-viz               Skip visualization generation
```

### Export Command Options
```bash
python cli.py export DATA_TYPE [OPTIONS]

Arguments:
  DATA_TYPE  [payments|prescriptions|correlations]

Options:
  --output PATH  Output file path (supports .csv and .parquet)
```

## 🛡️ Anti-Hallucination Measures

The system implements multiple layers of protection against LLM hallucination:

### 1. System-Level Constraints
- **Explicit Year Guidance**: LLM is told data spans 2020-2024 only
- **Number Validation**: Every statistic must come from provided data
- **Fallback Requirements**: LLM must write "[data not available]" when data is missing

### 2. Prompt Engineering
- **CRITICAL warnings** in prompts about using exact numbers
- **FORBIDDEN list** of common hallucinations (e.g., fictional provider counts)
- **REQUIRED TABLES** specifications with exact data fields

### 3. Data Formatting
- **Clear Data Labels**: "=== ACTUAL DATA (USE THESE EXACT NUMBERS) ==="
- **Explicit Values**: Numbers pre-formatted with exact precision
- **Sample Rows**: First 10 rows shown to guide table creation

### 4. Output Validation
- **OutputValidator class** checks for known hallucination patterns
- **Number verification** against source data
- **Table structure validation** for required columns

### Example Anti-Hallucination Prompt:
```yaml
CRITICAL DATA ACCURACY REQUIREMENTS:
1. Use ONLY the exact numbers provided in the data
2. Every statistic MUST come directly from the provided data
3. If a specific number is not in the data, write "[data not available]"
4. Do NOT create hypothetical scenarios or example numbers
5. The data is from years 2020-2024. Use ONLY these years.

FORBIDDEN:
- Making up provider counts
- Creating fictional dollar amounts
- Inventing multipliers or percentages
- Adding example data or hypothetical cases
```

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
Add new sections by editing `src/reporting/section_prompts.yaml`:

```yaml
custom_section:
  context: "Custom Analysis Section"
  prompt: |
    Generate a custom analysis section using the following data:
    {data}
    
    REQUIRED TABLES:
    1. Key Metrics Table
       - Columns: Metric | Value | Change
       - Include top 5 metrics
  constraints:
    max_length: 500
    style: "investigative"
  required_data:
    - custom_metrics
    - trend_data
```

Then update `CONFIG.yaml` to include your section:
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

## 📊 Report Generation System

### LLM Integration
The system uses Claude API to generate compelling narratives from data analysis:

1. **Data Analysis**: Python scripts analyze BigQuery data
2. **Data Mapping**: `data_mapper.py` prepares data for each section
3. **LLM Generation**: Claude generates narrative with markdown tables
4. **Validation**: Output validator checks for accuracy
5. **Assembly**: Final report combines all sections

### Section Configuration
Each section in `section_prompts.yaml` includes:
- **context**: Section purpose
- **prompt**: Template with `{data}` placeholder
- **constraints**: Length and style requirements
- **required_data**: Data keys needed
- **REQUIRED TABLES**: Specific table formats

### Markdown Table Generation
The LLM automatically generates markdown tables based on data:

```markdown
| Provider Type | Influence Increase | Vulnerability Score |
|---------------|-------------------|--------------------|
| PA            | 407.6%            | High               |
| NP            | 223.3%            | High               |
| MD            | Baseline          | Low                |
```

## 🔧 Troubleshooting

### Common Issues

#### LLM Using Wrong Years
- **Issue**: Report shows 2014-2018 instead of 2020-2024
- **Fix**: System message explicitly states correct years

#### Missing Tables in Sections
- **Issue**: Sections lack data tables
- **Fix**: REQUIRED TABLES specifications in prompts

#### Executive Summary Quality
- **Issue**: Poor executive summary
- **Fix**: Use `regenerate_section.py` to regenerate just that section

#### Hallucinated Numbers
- **Issue**: LLM invents statistics
- **Fix**: Multi-layer validation system prevents this

## Support
For questions or issues, contact the Conflixis Data Analytics team.

## License
Proprietary - Conflixis Inc.

---
*Template Version: 2.0.0*  
*Based on DA-175 Corewell Health Analysis*  
*Refactored with modular architecture and CLI interface*