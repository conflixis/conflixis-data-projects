# Data Directory - EXCLUDED FROM GIT

This directory contains sensitive healthcare data and is intentionally excluded from version control.

## Directory Structure

```
data/
├── raw/                    # Raw data exports
│   ├── disclosures/        # CSV exports and backups
│   └── providers/          # Provider master data
├── processed/              # Cleaned and transformed data
├── staging/                # Ready for API/dashboard consumption
│   ├── disclosure_data.json         # Main disclosure data (1,274 records)
│   └── disclosures_json_*.parquet   # Parquet format for performance
└── cache/                  # Temporary query results
```

## Current Data Pipeline

### Primary Data Source
**BigQuery Table:** `conflixis-engine.firestore_export.disclosures_raw_latest`
- Structure: JSON blobs in `data` field
- Campaign: 2025 Texas Health COI Survey (qyH2ggzVV0WLkuRfem7S)
- Group: gcO9AHYlNSzFeGTRSFRa

### Data Extraction Process
1. **Run:** `python scripts/fetch_json_data.py`
2. **Extracts from BigQuery:**
   - 1,274 disclosures from 831 unique reporters
   - Parses JSON structure including Open Payments imports
   - Categorizes into 5 categories × 12 disclosure types
3. **Outputs to:**
   - `data/staging/disclosure_data.json` - Main data file for API
   - `data/raw/disclosures/json_disclosures_YYYY-MM-DD.csv` - Backup
   - `data/staging/disclosures_json_YYYY-MM-DD.parquet` - Performance format

### Data Categories (2025 Campaign)
- **External Roles & Relationships** (51.9%) - Related Parties, Governance
- **Financial & Investment Interests** (34.1%) - Ownership, Remuneration, Compensation
- **Open Payments** (10.8%) - CMS automated imports
- **Political, Community & Advocacy** (2.8%) - Elected Office
- **Legal, Regulatory & Compliance** (0.3%) - Legal Proceedings, Awareness

### Additional Data Sources (Still Active)
- **Member Data:** `member_shards_raw_latest` - For NPI and enrichment
- **Form Metadata:** `disclosure_forms_raw_latest_v3` - Form configuration

## Data Security

- **This entire directory is gitignored**
- Never commit actual data files
- All data access requires proper authentication via service account
- Follow HIPAA guidelines for PHI handling
- Service account key stored in environment variable: `GCP_SERVICE_ACCOUNT_KEY`

## API Integration

The FastAPI backend (`app/main.py`) serves data from `staging/disclosure_data.json`:
- Endpoint: `/api/disclosures`
- No fallbacks - API must have data loaded
- Frontend only loads from API (no static file fallback)

## Key Statistics (Current)
- Total Disclosures: 1,274
- Unique Reporters: 831
- Data Coverage: 89.2% properly categorized
- Open Payments: 138 imports (10.8%)
- Total Value: $20.2M
- Average per Disclosure: $15,853

## Updating Data

To refresh data from BigQuery:
```bash
# From project root
cd projects/011-disclosure-policy-review
python scripts/fetch_json_data.py

# Then restart the API to load new data
python app/main.py
```

## Data Quality Notes
- "Related Parties" (plural) = properly categorized under External Roles
- Open Payments imports have `question.type = "open_payments"` (not regular questions)
- Each disclosure has exactly one category and one type
- Document IDs are unique identifiers for each disclosure