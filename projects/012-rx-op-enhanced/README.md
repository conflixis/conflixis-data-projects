# RX-OP Enhanced: Pharmaceutical Payment Attribution Data Pipeline

**JIRA Ticket**: DA-167  
**Created**: August 2024  
**Status**: ✅ Complete  
**Data Source**: Google Drive Folder ID: `1X2ssg7Bto1gKEKO9e3NFiGJosnm7LMpT`

## Overview

This project implements a robust data pipeline to transfer pharmaceutical payment attribution data from RDS files (R data format) to BigQuery. The data represents counterfactual analysis showing how pharmaceutical company payments influence provider prescribing patterns across different specialties and medications.

## Final Results

- **255 RDS files** successfully processed
- **298.8 million rows** uploaded to BigQuery
- **87.07 GB** of data
- **17 manufacturers** × **15 specialties** each
- **100% data transfer** verified through reconciliation

### BigQuery Table
```
Project: data-analytics-389803
Dataset: conflixis_agent
Table: rx_op_enhanced_full
```

## Data Description

Each RDS file contains provider-level prescription data with payment attribution metrics:
- **Provider Information**: NPI, specialty, geography
- **Payment Data**: Manufacturer-specific payment averages over time
- **Attribution Metrics**: Counterfactual analysis showing payment influence
- **Time-based Features**: Lag (before) and lead (after) payment averages

### Column Standardization
All manufacturer-specific columns were standardized from `{manufacturer}_avg_*` to `mfg_avg_*`:
- `mfg_avg_lag3`, `mfg_avg_lag6`, `mfg_avg_lag9`, `mfg_avg_lag12`
- `mfg_avg_lead3`, `mfg_avg_lead6`, `mfg_avg_lead9`, `mfg_avg_lead12`

## Pipeline Scripts

### Core Scripts (Production)
1. **`01_download_rds_files.py`** - Downloads all RDS files from Google Drive using service account authentication
2. **`02_rds_to_bigquery.py`** - Reads RDS files directly with pyreadr and uploads to BigQuery one at a time
3. **`03_resume_processing.py`** - Resumes processing from interruptions (checks BigQuery for completed files)
4. **`04_check_progress.py`** - Monitors upload progress and table statistics
5. **`05_reconciliation_full.py`** - Full data reconciliation (reads all RDS files)
6. **`06_reconciliation_quick.py`** - Quick reconciliation using expected counts

### Archived Scripts
Located in `archive/scripts/` - initial versions, tests, and superseded implementations

## Directory Structure
```
012-rx-op-enhanced/
├── 01_download_rds_files.py        # Download from Google Drive
├── 02_rds_to_bigquery.py          # Main processing pipeline
├── 03_resume_processing.py        # Resume from interruptions
├── 04_check_progress.py           # Monitor upload progress
├── 05_reconciliation_full.py      # Full data verification
├── 06_reconciliation_quick.py     # Quick verification
├── README.md                       # This documentation
├── rx-op-enhanced-data_dictionary.md  # Detailed data dictionary
├── mfg-spec-data/                 # 255 RDS source files (5.7 GB)
│   └── df_spec_*.rds
└── archive/                       # Archived development files
    ├── scripts/                   # Initial versions and tests
    ├── logs/                      # Processing logs
    └── temp/                      # Temporary files
```

## Processing Pipeline

### 1. Download Phase
```bash
python 01_download_rds_files.py
```
- Uses Google Cloud service account authentication
- Downloads 255 RDS files from Google Drive
- Total size: 5.7 GB

### 2. Processing Phase
```bash
python 02_rds_to_bigquery.py
```
- Reads RDS files directly using pyreadr (Python library)
- Processes files one at a time (memory efficient)
- Standardizes manufacturer columns to `mfg_avg_*`
- Uploads immediately to BigQuery (append mode)
- No R conversion needed - pyreadr handles RDS format natively

### 3. Resume (if needed)
```bash
python 03_resume_processing.py
```
- Automatically detects processed files
- Resumes from last successful file
- No duplicate processing

### 4. Verification
```bash
python 06_reconciliation_quick.py
```
- Verifies all files transferred
- Confirms row counts match
- Validates column standardization

## Key Technical Achievements

### Memory Optimization
- Process one file at a time (avoid loading 5.7GB into memory)
- Immediate upload after each file processing
- Aggressive garbage collection between files
- Maximum ~2-3 GB RAM usage (vs 32GB+ if loaded all at once)

### Fault Tolerance
- Automatic resume from any interruption point
- Queries BigQuery to identify completed files
- File-level atomicity (all-or-nothing per file)
- No duplicate data on resume
- Successfully handled multiple process crashes during development

### Column Standardization
- Regex-based pattern matching for manufacturer columns
- Handles compound manufacturer names (e.g., `janssen_biotech`, `eli_lilly`, `bristol_myers_squibb`)
- Converts all `{manufacturer}_avg_*` columns to uniform `mfg_avg_*` pattern
- Preserves data integrity while enabling cross-manufacturer analysis

## Manufacturers Processed

1. AbbVie
2. Allergan
3. AstraZeneca
4. Boehringer Ingelheim
5. Celgene
6. Eli Lilly
7. Gilead
8. GSK
9. Janssen (Biotech & Pharma)
10. Merck
11. Novartis
12. Novo Nordisk
13. Pfizer
14. Sanofi
15. Bristol Myers Squibb
16. Takeda

## Technical Implementation

### Core Technologies
- **Python 3.12+** - Primary language
- **pyreadr** - Direct RDS file reading (no R installation needed)
- **pandas** - Data manipulation and transformation
- **google-cloud-bigquery** - BigQuery client library
- **Google Cloud Service Account** - Authentication and API access

### Environment Configuration
Create `.env` file with:
```bash
GCP_SERVICE_ACCOUNT_KEY='{"type": "service_account", ...}'  # Full JSON key
```

### Performance Metrics
- **Download Speed**: ~50 MB/s from Google Drive
- **Processing Rate**: 20-30 seconds per file
- **Upload Speed**: 2-3 million rows/minute to BigQuery
- **Total Pipeline Time**: ~3 hours (including interruptions)
- **Memory Usage**: Peak 2.3 GB (vs 32GB+ if batch processed)

## Data Quality Checks

✅ **All verifications passed:**
- 255/255 files processed
- 298,795,689 rows uploaded
- All columns standardized to `mfg_avg_*`
- No non-standardized manufacturer columns
- Consistent row counts per manufacturer (17,576,217 each)

## Usage Examples

### Query standardized data in BigQuery:
```sql
SELECT 
    npi,
    specialty,
    source_manufacturer,
    mfg_avg_lag3,
    mfg_avg_lead3,
    claim_count
FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
WHERE source_manufacturer = 'abbvie'
    AND specialty = 'Cardiology'
LIMIT 100;
```

### Aggregate payment influence analysis:
```sql
SELECT 
    source_manufacturer,
    AVG(mfg_avg_lag3) as avg_payment_before,
    AVG(mfg_avg_lead3) as avg_payment_after,
    COUNT(DISTINCT npi) as provider_count
FROM `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`
GROUP BY source_manufacturer
ORDER BY provider_count DESC;
```

## Troubleshooting

### Common Issues and Solutions

#### Memory Errors (Exit Code 137)
```bash
# Solution: Use resume script
python 03_resume_processing.py
```
The pipeline automatically handles memory issues by processing one file at a time.

#### Google Drive Rate Limits
- Already solved using service account authentication
- If issues persist, check service account permissions

#### Verification Failures
```bash
# Quick check (uses expected counts)
python 06_reconciliation_quick.py

# Full verification (reads all RDS files)
python 05_reconciliation_full.py
```

#### Monitor Progress
```bash
# Check current status
python 04_check_progress.py
```

## Lessons Learned

1. **Service Account > OAuth** - Avoids Google Drive rate limits for large downloads
2. **One-at-a-time Processing** - Critical for memory management with large datasets
3. **pyreadr Library** - Can read RDS files directly without R installation
4. **Resume Capability** - Essential for multi-hour pipelines
5. **Column Standardization** - Regex patterns handle complex manufacturer naming

## Future Enhancements

- Parallel processing with memory constraints
- Incremental updates (only new/changed files)
- Data validation rules for quality checks
- Automated scheduling via Cloud Composer/Airflow

## Contact

**JIRA**: DA-167  
**Project**: RX-OP Enhanced Attribution Analysis  
**Dataset**: `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`

---
*Pipeline developed and tested August 2024*  
*Successfully processed 298.8M rows across 255 files*