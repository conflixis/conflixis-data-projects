# THR Disclosure Parse Project

**Jira Ticket**: DA-172  
**Branch**: `feature/DA-013-thr-disclosure-parse`

## Overview

This project extracts and parses Texas Health Resources (THR) Conflict of Interest (COI) disclosure data from BigQuery. The data originates from Firestore and contains healthcare provider disclosures including financial relationships, external roles, and other potential conflicts of interest.

## Data Sources

### 1. Disclosure Data
- **Table**: `conflixis-engine.firestore_export.disclosures_raw_latest`
- **Filters**:
  - `group_id`: `gcO9AHYlNSzFeGTRSFRa` (Texas Health Resources)
  - `campaign_id`: `qyH2ggzVV0WLkuRfem7S` (2025 Texas Health COI Survey)
- **Contains**: Disclosure details, financial information, relationships, review status

### 2. Member Data
- **Table**: `conflixis-engine.firestore_export.member_shards_raw_latest_parsed`
- **Filter**: Same `group_id` as above
- **Contains**: Provider NPIs, job titles, departments, manager names
- **Purpose**: Enriches disclosure data with provider details

## Setup

### Prerequisites

1. Python 3.8 or higher
2. Google Cloud credentials with BigQuery access
3. Environment variables configured in `.env` file

### Installation

```bash
# Navigate to project directory
cd projects/013-thr-disclosure-parse

# Create virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Environment Configuration

Create a `.env` file in the project root with:

```env
# Google Cloud Service Account (JSON string)
GCP_SERVICE_ACCOUNT_KEY='{"type":"service_account",...}'

# Optional: Logging level
LOG_LEVEL=INFO
```

## Usage

### Run Complete Extraction

```bash
# From project directory
python scripts/fetch_thr_disclosures.py
```

This will:
1. Fetch member data from BigQuery
2. Fetch disclosure data from BigQuery
3. Join the datasets on provider names
4. Export to CSV (and optionally Parquet/JSON/Excel)

### Run Individual Components

```bash
# Fetch only member data
python scripts/fetch_member_data.py

# Main extraction (will use cached member data if available)
python scripts/fetch_thr_disclosures.py
```

## Output Files

All outputs are saved in `data/output/` directory:

- **Primary Output**: `thr_disclosures_[timestamp].csv`
  - Human-readable CSV with all disclosure information
  
- **Additional Formats** (if configured):
  - `thr_disclosures_[timestamp].parquet` - High-performance binary format
  - `thr_disclosures_[timestamp].json` - JSON with metadata

### CSV Column Descriptions

| Column | Description |
|--------|-------------|
| `id` | Unique disclosure document ID |
| `provider_name` | Full name of the provider |
| `provider_npi` | National Provider Identifier (from member data) |
| `provider_email` | Provider's email address |
| `job_title` | Current job title (from member data) |
| `department` | Department/entity (from member data) |
| `manager_name` | Manager's name |
| `entity_name` | Company/entity disclosed |
| `relationship_type` | Type of relationship disclosed |
| `category_label` | Category of disclosure |
| `financial_amount` | Dollar amount of financial interest |
| `compensation_type` | Type of compensation |
| `disclosure_date` | Date of disclosure |
| `relationship_start_date` | When relationship began |
| `relationship_ongoing` | Whether relationship is active |
| `status` | Disclosure status |
| `review_status` | Review status |
| `is_research` | Research-related flag |
| `disputed` | Whether disclosure is disputed |
| `notes` | Additional notes |


## Data Processing Pipeline

1. **Extract Member Data**
   - Query `member_shards_raw_latest_parsed` table
   - Create normalized name field for joining
   - Cache locally for reuse

2. **Extract Disclosure Data**
   - Query `disclosures_raw_latest` with filters
   - Parse JSON fields using BigQuery SQL functions
   - Extract all relevant fields from nested JSON

3. **Join Datasets**
   - Match disclosures to members by normalized names
   - Add NPI, job title, department from member data
   - Handle unmatched records gracefully

4. **Export Results**
   - Primary CSV format for analysis
   - Optional Parquet for performance
   - Optional JSON with metadata

## Statistics and Validation

The script provides detailed statistics including:
- Total records processed
- Provider counts and match rates
- Financial amount distributions
- Category breakdowns

## Troubleshooting

### Common Issues

1. **Authentication Error**
   - Ensure `GCP_SERVICE_ACCOUNT_KEY` is properly set in `.env`
   - Verify service account has BigQuery Data Viewer permissions

2. **No Data Retrieved**
   - Check group_id and campaign_id filters
   - Verify table names and project ID

3. **Member Data Not Matching**
   - Names may have slight variations
   - Check normalization logic in join

### Logging

Detailed logs are written to `thr_disclosure_parse.log` in the project root.

## Project Structure

```
013-thr-disclosure-parse/
├── README.md                      # This file
├── requirements.txt              # Python dependencies
├── scripts/
│   ├── config.py                # Configuration constants
│   ├── fetch_member_data.py    # Member data extraction
│   └── fetch_thr_disclosures.py # Main extraction script
├── data/
│   ├── raw/                    # Raw data cache
│   └── output/                 # Final outputs
└── thr_disclosure_parse.log    # Execution log
```

## Contact

For questions or issues, reference Jira ticket DA-172 or contact the Data Analytics team.

## License

Internal use only - Conflixis proprietary.