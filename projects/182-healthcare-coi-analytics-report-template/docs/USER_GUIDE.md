# Healthcare COI Analytics - User Guide

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Setup Instructions](#setup-instructions)
4. [Running Your First Analysis](#running-your-first-analysis)
5. [Understanding the Output](#understanding-the-output)
6. [Customization Options](#customization-options)
7. [Troubleshooting](#troubleshooting)
8. [Best Practices](#best-practices)

---

## Overview

The Healthcare COI Analytics Report Template enables rapid analysis of potential conflicts of interest in healthcare provider relationships with pharmaceutical and medical device manufacturers. This template analyzes:

- **Open Payments data**: Industry payments to providers
- **Prescription patterns**: Medicare Part D and commercial claims
- **Correlations**: Relationships between payments and prescribing
- **Risk assessment**: Identification of high-risk providers and patterns

## Prerequisites

### Required Software
- Python 3.8 or higher
- pip (Python package manager)
- Git (for version control)

### Required Access
- Google Cloud Platform account with BigQuery access
- Access to CMS Open Payments data in BigQuery
- Access to prescription claims data (Medicare Part D or commercial)

### Required Data
- List of provider NPIs (National Provider Identifiers) for your health system
- Optional: Provider names and specialties

## Setup Instructions

### 1. Clone or Copy the Template

```bash
# If using git
git clone <repository-url>
cd 182-healthcare-coi-analytics-report-template

# Or copy the template folder to your working directory
cp -r 182-healthcare-coi-analytics-report-template/ my-health-system-analysis/
cd my-health-system-analysis
```

### 2. Install Dependencies

```bash
# Create a virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required packages
pip install -r requirements.txt
```

### 3. Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials
nano .env  # or use your preferred editor
```

Add your GCP service account key JSON to the `.env` file:
```
GCP_SERVICE_ACCOUNT_KEY='{"type": "service_account", ...}'
```

### 4. Prepare Your Data

Create a CSV file with your provider NPIs:

```csv
NPI,Full_Name,Primary_Specialty
1234567890,John Smith MD,Internal Medicine
2345678901,Jane Doe NP,Family Practice
```

Save this file to `data/inputs/provider_npis.csv`

### 5. Configure Analysis

Edit `CONFIG.yaml` with your health system details:

```yaml
health_system:
  name: "Your Health System Name"
  short_name: "YHS"
  npi_file: "data/inputs/provider_npis.csv"
  
analysis:
  start_year: 2020
  end_year: 2024
```

## Running Your First Analysis

### Step 1: Validate Setup

```bash
python scripts/00_validate_setup.py
```

This will check:
- Environment variables are set correctly
- Required packages are installed
- NPI file is properly formatted
- BigQuery connection works

### Step 2: Run Open Payments Analysis

```bash
python scripts/01_analyze_op_payments.py
```

This analyzes:
- Total payments to your providers
- Payment categories and trends
- Top manufacturers
- Payment tiers

### Step 3: Run Prescription Analysis (Optional)

```bash
python scripts/02_analyze_prescriptions.py
```

This analyzes:
- Prescribing volumes and costs
- Top prescribed drugs
- Provider type patterns

### Step 4: Run Correlation Analysis

```bash
python scripts/03_payment_influence.py
```

This calculates:
- Payment-prescription correlations
- ROI per payment dollar
- Provider vulnerability scores

### Step 5: Generate Report

```bash
python scripts/05_generate_report.py
```

This creates:
- Comprehensive markdown report
- Executive summary
- Risk assessments
- Recommendations

### Run Everything at Once

```bash
python scripts/run_full_analysis.py
```

## Understanding the Output

### File Structure

After running the analysis, you'll find:

```
data/
├── processed/          # Intermediate analysis files
│   ├── op_overall_metrics_[timestamp].csv
│   ├── op_payment_categories_[timestamp].csv
│   ├── op_yearly_trends_[timestamp].csv
│   └── ...
└── output/            # Final reports
    ├── [health_system]_coi_report_[timestamp].md
    └── [health_system]_coi_report_[timestamp].pdf
```

### Key Metrics Explained

#### Payment Metrics
- **Unique Providers Paid**: Number of your providers receiving any payments
- **Payment Penetration**: Percentage of providers with industry relationships
- **Average Payment**: Mean payment amount per transaction
- **Payment Growth**: Year-over-year change in total payments

#### Correlation Metrics
- **Influence Factor**: How much more paid providers prescribe vs unpaid
- **ROI**: Return on investment per dollar of payments
- **Vulnerability Score**: Susceptibility to payment influence (0-100)

#### Risk Indicators
- **Low Risk (0-30)**: Minimal payment influence detected
- **Medium Risk (31-60)**: Moderate correlations requiring monitoring
- **High Risk (61-80)**: Strong correlations requiring intervention
- **Critical Risk (81-100)**: Extreme patterns requiring immediate action

## Customization Options

### Adjusting Thresholds

Edit `CONFIG.yaml` to modify risk thresholds:

```yaml
thresholds:
  payment:
    high_single_payment: 5000  # Flag payments over this amount
    suspicious_roi: 1000       # Flag ROI over this threshold
  risk_score:
    high: 70                   # Score threshold for high risk
```

### Adding Custom Drugs

Focus on specific medications by editing `CONFIG.yaml`:

```yaml
focus_drugs:
  - name: "CUSTOM_DRUG"
    category: "Category"
    high_cost: true
```

### Customizing Report Sections

Control which sections appear in the report:

```yaml
reports:
  sections:
    - executive_summary
    - payment_overview
    # - prescription_patterns  # Commented out to skip
    - correlation_analysis
```

### Adding Custom Queries

Place SQL files in `config/queries/` and reference them in scripts:

```python
with open('config/queries/custom_query.sql', 'r') as f:
    query = f.read()
results = client.execute_query(query)
```

## Troubleshooting

### Common Issues

#### "GCP_SERVICE_ACCOUNT_KEY not found"
- Ensure `.env` file exists and contains your GCP credentials
- Check that credentials are valid JSON

#### "NPI file not found"
- Verify file path in `CONFIG.yaml` matches actual location
- Ensure file is CSV format with 'NPI' column

#### "BigQuery table not found"
- Verify table names in `CONFIG.yaml`
- Check that you have access to the specified datasets

#### "No data returned"
- Check that your NPIs match those in the Open Payments database
- Verify analysis date range includes available data

### Debug Mode

Enable debug mode for detailed logging:

```yaml
# In CONFIG.yaml
advanced:
  debug_mode: true
```

Or set environment variable:
```bash
export DEBUG=true
```

## Best Practices

### Data Quality
1. **Validate NPIs**: Ensure all NPIs are 10-digit numbers
2. **Include Specialties**: Helps with provider type analysis
3. **Remove Duplicates**: Each NPI should appear only once
4. **Update Regularly**: Refresh NPI lists quarterly

### Performance
1. **Batch Processing**: For >10,000 providers, enable batching
2. **Caching**: Enable cache for repeated analyses
3. **Sampling**: Use sampling for initial testing

```yaml
advanced:
  use_sampling: true
  sample_size: 1000
```

### Security
1. **Never commit `.env`**: Keep credentials out of version control
2. **Limit access**: Restrict report distribution to authorized personnel
3. **Anonymize if needed**: Remove provider names for broader sharing

### Compliance
1. **Document sources**: Keep record of data sources and dates
2. **Maintain audit trail**: Archive all generated reports
3. **Regular updates**: Re-run analyses quarterly or annually
4. **Legal review**: Have legal team review before external distribution

## Getting Help

### Resources
- **Documentation**: See `/docs` folder for additional guides
- **Examples**: Check `/docs/examples` for sample configurations
- **Scripts**: Each script has `--help` option for usage details

### Support Contacts
- Technical Issues: data-analytics@conflixis.com
- Methodology Questions: provider-intelligence@conflixis.com
- Access Requests: it-support@conflixis.com

### Troubleshooting Checklist
1. ✅ Run validation script first
2. ✅ Check logs in `/logs` folder
3. ✅ Verify data in `/data/processed`
4. ✅ Enable debug mode if needed
5. ✅ Contact support with error messages and logs

---

## Appendix

### Required BigQuery Permissions
- `bigquery.datasets.get`
- `bigquery.tables.get`
- `bigquery.tables.getData`
- `bigquery.jobs.create`

### File Format Specifications

#### NPI CSV Format
- **Required**: NPI column (10-digit numbers)
- **Recommended**: Full_Name, Primary_Specialty
- **Optional**: Secondary_Specialty, Degree, Organization

#### Output Report Formats
- **Markdown (.md)**: Primary format, human-readable
- **PDF (.pdf)**: For distribution (requires additional setup)
- **Excel (.xlsx)**: Detailed data tables
- **JSON (.json)**: Machine-readable summaries

---

*Last Updated: {{CURRENT_DATE}}*  
*Version: 1.0.0*