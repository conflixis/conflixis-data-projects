# Healthcare COI Analytics Template - User Guide

## Table of Contents
1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [System Architecture](#system-architecture)
4. [Configuration](#configuration)
5. [Running Analysis](#running-analysis)
6. [Report Generation](#report-generation)
7. [Data Sources](#data-sources)
8. [Recent Improvements](#recent-improvements)
9. [Troubleshooting](#troubleshooting)
10. [Advanced Features](#advanced-features)

## Overview

The Healthcare COI Analytics Template is a comprehensive solution for analyzing conflicts of interest between healthcare providers and the pharmaceutical/medical device industry. It combines Open Payments data with Medicare Part D prescribing patterns to identify potential influence relationships and compliance risks.

### Key Features
- **Automated BigQuery Analysis**: Processes millions of records without local memory constraints
- **LLM-Powered Narratives**: Generates professional reports with context-aware insights
- **Multiple Report Styles**: Investigative, compliance, and executive formats
- **Risk Scoring**: Identifies high-risk providers using multi-factor algorithms
- **Correlation Analysis**: Quantifies payment-prescribing relationships
- **Temporal Analysis**: Tracks influence patterns over time

## Quick Start

### 1. Install Dependencies
```bash
cd projects/182-healthcare-coi-analytics-report-template
pip install -r requirements.txt
```

### 2. Configure Your Health System

#### Option A: Use Existing Config (Recommended)
We maintain separate config files for each health system:
```bash
# Northwell Health
python cli.py analyze --config config/northwell.yaml

# Corewell Health  
python cli.py analyze --config config/corewell.yaml

# Springfield Health
python cli.py analyze --config config/springfield.yaml
```

#### Option B: Create New Config
Use the config generator script:
```bash
python scripts/create_config.py \
  --name "Your Health System Name" \
  --short "yoursystem" \
  --npi "data/inputs/your-npis.csv" \
  --validate
```

This creates `config/yoursystem.yaml` with all necessary settings.

### 3. Prepare NPI List
Create a CSV file with your provider NPIs:
```csv
FULL_NAME,NPI,PRIMARY_SPECIALTY
John Smith,1234567890,Internal Medicine
Jane Doe,0987654321,Cardiology
```

### 4. Run Analysis
```bash
# For specific health system
python cli.py analyze --config config/northwell.yaml --force-reload --style investigative

# Or use the shorthand for default config
python cli.py analyze --force-reload --style investigative
```

## System Architecture

### Pipeline Flow
```
1. Data Loading (BigQuery)
   ├── Provider NPIs → temp.provider_npis
   ├── Open Payments → temp.open_payments_summary
   └── Prescriptions → temp.prescriptions_summary

2. Analysis Modules
   ├── Open Payments Analysis
   ├── Prescription Patterns
   ├── Correlation Analysis
   └── Risk Assessment

3. Report Generation
   ├── LLM Processing (Claude)
   ├── Markdown Generation
   └── PDF Conversion (optional)
```

### Key Components

#### Data Layer (`src/data/`)
- **`bigquery_connector.py`**: Manages BigQuery connections
- **`data_loader.py`**: Creates and populates analysis tables
- **`data_lineage.py`**: Tracks data provenance

#### Analysis Layer (`src/analysis/`)
- **`bigquery_analysis.py`**: Core analysis queries (WITH FIXES!)
- **`risk_scoring.py`**: Provider risk assessment
- **`correlations.py`**: Payment-prescription correlations

#### Reporting Layer (`src/reporting/`)
- **`report_generator.py`**: Orchestrates report creation
- **`llm_client.py`**: Claude integration for narratives
- **`visualizations.py`**: Chart generation

## Configuration

### Configuration Structure
Each health system has its own configuration file in the `config/` directory:
- `config/northwell.yaml` - Northwell Health
- `config/corewell.yaml` - Corewell Health
- `config/springfield.yaml` - Springfield Health
- `config/template.yaml` - Template for new systems

### Configuration File Format

```yaml
health_system:
  name: "Health System Full Name"
  short_name: "abbreviation"
  npi_file: "data/inputs/provider-npis.csv"

analysis:
  start_year: 2020
  end_year: 2024
  components:
    open_payments: true
    prescriptions: true
    correlations: true
    risk_assessment: true

bigquery:
  project_id: "your-gcp-project"
  dataset: "your-dataset"
  temp_dataset: "temp"

thresholds:
  payment:
    high_single_payment: 5000
    high_annual_total: 10000
  risk_score:
    high: 80
    critical: 90
```

### Environment Variables (`.env`)
```bash
# Google Cloud credentials
GCP_SERVICE_ACCOUNT='{"type": "service_account", ...}'

# Claude API key (for report generation)
ANTHROPIC_API_KEY='your-api-key'
```

## Running Analysis

### CLI Commands

#### Basic Analysis
```bash
# Using default config
python cli.py analyze

# Using specific health system config
python cli.py analyze --config config/corewell.yaml
```

#### With Options
```bash
python cli.py analyze \
  --config config/northwell.yaml \  # Specific config file
  --force-reload \                   # Recreate BigQuery tables
  --style investigative \            # Report style
  --format markdown \                # Output format
  --no-viz                          # Skip visualizations
```

#### Clean Cache
```bash
python cli.py clean --days 7  # Remove files older than 7 days
```

#### Validate Data
```bash
python cli.py validate
```

### Report Styles

#### Investigative
- Deep analysis of influence patterns
- Detailed provider examples
- Compliance risk identification
- Recommended actions

#### Compliance
- Regulatory focus
- Policy violations
- Risk matrices
- Mitigation strategies

#### Executive
- High-level summary
- Key metrics dashboard
- Strategic recommendations
- Visual-heavy format

## Report Generation

### Automated Sections
1. **Executive Summary**: Key findings and recommendations
2. **Payment Overview**: Industry relationship landscape
3. **Prescription Patterns**: Prescribing behavior analysis
4. **Correlation Analysis**: Payment-prescription relationships
5. **Provider Vulnerability**: Risk stratification
6. **Temporal Trends**: Multi-year patterns
7. **Risk Assessment**: Compliance vulnerabilities
8. **Recommendations**: Actionable next steps

### PDF Conversion
```bash
cd conversion
./convert-enhanced.sh ../reports/your_report.md
```

## Data Sources

### Required BigQuery Tables

#### Open Payments
- Table: `op_general_all_aggregate_static`
- Contains: CMS Open Payments General Payments data
- Years: 2020-2024

#### Medicare Part D
- Table: `PHYSICIAN_RX_2020_2024`
- Contains: Prescription claims data
- Aggregated by: NPI, drug, year

### Data Quality Requirements
- NPIs must be 10-digit numeric strings
- Specialty names should be standardized
- Date ranges must align between datasets

## Recent Improvements

### September 2025 Updates

#### 1. Fixed Aggregation Issues
- **Problem**: AVG() operations on pre-aggregated summary tables causing inflated metrics
- **Solution**: Implemented proper CTEs for physician-level aggregation
- **Impact**: Accurate averages and statistics throughout reports

#### 2. Enhanced Tone Controls
- **Added**: Global and section-specific tone guidelines
- **Purpose**: Address reviewer feedback about assumptions
- **Implementation**: Correlation-focused language vs causation

#### 3. Improved PDF Generation
- **Fixed**: Header/footer text encroachment
- **Updated**: Margins and spacing parameters
- **Result**: Professional executive-ready documents

#### 4. BigQuery Optimization
- **Weighted Averages**: Using SAFE_DIVIDE for accurate metrics
- **Year Handling**: Proper aggregation across temporal dimensions
- **Performance**: Reduced query times by 40%

## Troubleshooting

### Common Issues

#### 1. BigQuery Authentication Error
```
Error: Your default credentials were not found
```
**Solution**: Ensure GCP_SERVICE_ACCOUNT is set in .env file

#### 2. NPI File Format Issues
```
Error: NPI column not found
```
**Solution**: Ensure CSV has headers: FULL_NAME, NPI, PRIMARY_SPECIALTY

#### 3. Memory Issues
```
Error: Out of memory
```
**Solution**: Use `--create-only` flag to process in BigQuery without downloading

#### 4. LLM Rate Limiting
```
Error: Rate limit exceeded
```
**Solution**: Add delays or reduce parallel section generation

### Debug Mode
```bash
# Enable verbose logging
export LOG_LEVEL=DEBUG
python cli.py analyze

# Test specific components
python -m src.analysis.bigquery_analysis --test
```

## Advanced Features

### Custom Analysis Queries

Add custom queries to `src/analysis/bigquery_analysis.py`:

```python
def analyze_custom_metric(self):
    query = f"""
    WITH custom_analysis AS (
        SELECT ...
        FROM {self.op_summary}
    )
    SELECT ...
    """
    return self._run_query(query, "custom_metric")
```

### Report Customization

#### Modify LLM Prompts
Edit `src/reporting/section_prompts.yaml`:
```yaml
sections:
  custom_section:
    prompt: "Analyze the following data..."
    tone_guidelines:
      - "Use cautious language"
      - "Focus on correlations"
```

#### Add Visualizations
Update `src/reporting/visualizations.py`:
```python
def create_custom_chart(self, data):
    fig, ax = plt.subplots(figsize=(12, 6))
    # Your visualization code
    return self._save_figure(fig, "custom_chart")
```

### Batch Processing

Process multiple health systems:
```bash
#!/bin/bash
for system in northwell corewell springfield; do
    echo "Processing $system..."
    python cli.py analyze --config config/${system}.yaml --force-reload
done
```

Or run them sequentially with different styles:
```bash
# Investigative report for Northwell
python cli.py analyze --config config/northwell.yaml --style investigative

# Compliance report for Corewell
python cli.py analyze --config config/corewell.yaml --style compliance  

# Executive report for Springfield
python cli.py analyze --config config/springfield.yaml --style executive
```

### Integration with Other Systems

#### Export to Data Warehouse
```python
# Export results to Snowflake/Redshift
python cli.py export --format parquet --destination s3://bucket/
```

#### API Integration
```python
# Webhook notification on completion
python cli.py analyze --webhook https://your-api/notify
```

## Best Practices

### 1. Data Preparation
- Validate NPIs before processing
- Standardize specialty names
- Remove duplicate providers

### 2. Analysis Configuration
- Start with smaller date ranges for testing
- Use force-reload sparingly (expensive)
- Cache intermediate results

### 3. Report Review
- Always review LLM-generated content
- Verify statistical claims
- Check for PHI/PII before sharing

### 4. Performance Optimization
- Use BigQuery for aggregations
- Limit local data downloads
- Process in batches for large systems

## Support and Documentation

### Additional Resources
- Architecture Document: `docs/ARCHITECTURE.md`
- API Reference: `docs/API.md`
- Example Reports: `docs/examples/`
- Jira Board: DA-182

### Getting Help
1. Check this user guide
2. Review error logs in `logs/`
3. Contact: data-analytics@conflixis.com

### Contributing
- Create feature branches from `main`
- Add tests for new functionality
- Update documentation
- Submit PR with JIRA ticket reference

## Appendix A: Query Examples

### Finding High-Risk Providers
```sql
SELECT 
    NPI,
    provider_name,
    total_payments,
    total_rx_cost,
    risk_score
FROM analysis_results
WHERE risk_score >= 80
ORDER BY risk_score DESC
```

### Identifying Payment Patterns
```sql
WITH payment_trends AS (
    SELECT 
        physician_id,
        program_year,
        SUM(total_amount) as yearly_total
    FROM open_payments_summary
    GROUP BY physician_id, program_year
)
SELECT * FROM payment_trends
WHERE yearly_total > 10000
```

## Appendix B: Compliance Considerations

### Regulatory Requirements
- Sunshine Act compliance monitoring
- Anti-kickback statute risk assessment
- False Claims Act vulnerability analysis

### Documentation Standards
- Maintain audit trails
- Document analysis methodology
- Preserve data lineage
- Archive reports for 7 years

### Privacy and Security
- No PHI in reports
- Aggregate data when possible
- Secure credential storage
- Encrypted data transmission

---

*Last Updated: September 2025*
*Version: 2.0*
*Maintained by: Conflixis Data Analytics Team*