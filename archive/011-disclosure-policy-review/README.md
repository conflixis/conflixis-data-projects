# COI Disclosure Policy Review System

## Overview
This project implements a comprehensive Conflict of Interest (COI) disclosure review system for healthcare organizations, based on Texas Health Resources Corporate Policy.

## Project Structure

```
011-disclosure-policy-review/
├── config/                          # Configuration files
│   ├── coi-policies.yaml           # Policy definitions with clause references
│   ├── coi-operational-thresholds.yaml  # Risk thresholds and scoring
│   └── research-policies.yaml      # Research-specific COI policies
├── scripts/                         # Data pipeline scripts
│   └── bigquery_pipeline.py        # Main ETL pipeline
├── data/                           # Data storage (GITIGNORED)
│   ├── raw/                        # Raw data from BigQuery
│   │   ├── *.csv                  # CSV for audit trail
│   │   └── *.parquet              # Parquet for fast reprocessing
│   ├── processed/                  # Processed with risk scores
│   │   └── *.parquet              # Parquet format only
│   └── staging/                    # UI-ready data
│       ├── *.parquet              # Optimized Parquet
│       └── disclosure_data.json   # JSON for web consumption
├── output/                         # Reports and exports
├── docs/                           # Policy documents
│   └── *.pdf                      # Source policy PDFs
├── dashboard-coi-compliance-overview.html  # Main dashboard
└── disclosure-data-viewer.html    # Detailed disclosure viewer

```

## Data Pipeline Architecture

### Storage Strategy

The pipeline implements a three-tier storage strategy for optimal performance:

1. **Raw Data Layer** (`/data/raw/`)
   - **CSV Format**: Complete audit trail, human-readable
   - **Parquet Format**: Compressed binary for fast reprocessing
   - Both formats saved for every BigQuery extraction

2. **Processed Data Layer** (`/data/processed/`)
   - **Parquet Format Only**: Risk scores calculated, data enriched
   - Optimized for analytical queries
   - ~5-10x compression vs CSV

3. **UI Layer** (`/data/staging/`)
   - **Parquet Format**: Optimized column selection for UI
   - **JSON Format**: Web-compatible for dashboard consumption
   - Pre-sorted by risk priority

### Performance Benefits

- **Parquet Advantages**:
  - Columnar storage: Only load needed columns
  - Built-in compression: 5-10x smaller than CSV
  - Type preservation: Maintains data types
  - Fast queries: Optimized for analytical workloads
  
- **Typical Compression Ratios**:
  - Raw CSV: 100 MB
  - Raw Parquet: 10-20 MB
  - UI JSON: 30-40 MB

## Running the Pipeline

### Prerequisites

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export GCP_PROJECT_ID="data-analytics-389803"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

### Basic Usage

```bash
# Run full pipeline (extract from BigQuery)
python scripts/bigquery_pipeline.py

# Use cached data (skip BigQuery)
python scripts/bigquery_pipeline.py --skip-bigquery

# Specify date range
python scripts/bigquery_pipeline.py \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

### Pipeline Workflow

1. **Extract**: Pull integrated disclosure data from BigQuery
2. **Save Raw**: Store as both CSV (audit) and Parquet (performance)
3. **Process**: Calculate risk scores using operational thresholds
4. **Optimize**: Prepare UI-specific dataset with needed columns
5. **Export**: Save as Parquet (performance) and JSON (web)

## UI Components

### Main Dashboard
- **URL**: `dashboard-coi-compliance-overview.html`
- **Purpose**: High-level compliance overview
- **Features**: 
  - Summary statistics
  - Insurmountable conflicts
  - Policy requirements
  - Covered persons by category

### Disclosure Data Viewer
- **URL**: `disclosure-data-viewer.html`
- **Purpose**: Detailed disclosure records
- **Features**:
  - Searchable/filterable table
  - Risk tier visualization
  - Export to CSV
  - Detail modal for each disclosure
  - Loads from `/data/staging/disclosure_data.json`

## Configuration Files

### coi-policies.yaml
- Direct interpretation of Texas Health policy
- Every section linked to policy clause
- No thresholds (qualitative only)

### coi-operational-thresholds.yaml
- Configurable risk thresholds
- 4-tier risk model ($1k, $5k, $25k, $100k)
- Risk scoring matrix
- Industry-specific adjustments

## Data Security

- `/data/` directory is completely gitignored
- No sensitive data in version control
- Use sample data generators for testing
- Follow HIPAA guidelines for PHI

## Development Workflow

1. **Local Development**:
   - Use `--skip-bigquery` flag with cached data
   - Sample data generated automatically if no cache

2. **Production**:
   - Run pipeline on schedule (daily/weekly)
   - Monitor `/output/` for compliance reports
   - Review high-risk disclosures in UI

## Jira Integration

- **Epic**: DA-141 (Healthcare Provider Disclosure Policy Review)
- **Tickets**:
  - DA-150: COI policies YAML ✓
  - DA-151: Policy Decision Engine (pending)
  - DA-152: BigQuery Data Pipeline ✓
  - DA-153: Real-time Dashboard Integration (pending)

## Support

For questions or issues, contact the Compliance Technology team.