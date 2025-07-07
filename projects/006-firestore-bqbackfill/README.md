# Firestore to BigQuery Backfill

This project provides scripts to manually backfill Firestore data to BigQuery, as an alternative to using the Firebase extension `fs-bq-import-collection`.

## Overview

The process consists of two main steps:

1. **Download** - Extract data from Firestore collection and format for BigQuery
2. **Upload** - Load formatted data into BigQuery with predefined schema

## Project Structure

```
006-firestore-bqbackfill/
├── download_firestore_data.py    # Main script to download from Firestore
├── upload_to_bigquery.py         # Main script to upload to BigQuery
├── config.py                     # Central configuration file
├── requirements.txt              # Python dependencies
├── sql/                          # BigQuery SQL queries
│   ├── member_shards_queries.sql # Comprehensive query collection
│   ├── parse_member_shards.sql   # Advanced parsing examples
│   └── simple_parse_example.sql  # Basic parsing examples
├── utils/                        # Utility scripts
│   └── test_auth.py             # Test authentication methods
├── data/                         # Downloaded data (git-ignored)
│   ├── firestore_data.jsonl     # Formatted data for BigQuery
│   └── firestore_data_metadata.json # Download metadata
├── backfill.log                  # Execution logs
└── README.md                     # This file
```

## Prerequisites

1. Python 3.12+ with Poetry installed
2. Google Cloud SDK configured with appropriate credentials
3. Access to both Firebase (`conflixis-web`) and BigQuery (`conflixis-engine`) projects

## Installation

From the repository root:

```bash
# Install Python dependencies
poetry install

# Install additional requirements for this specific project
cd projects/006-firestore-bqbackfill
pip install -r requirements.txt
```

## Configuration

Edit `config.py` to adjust settings:

- **Firebase settings**: Project ID, collection path, collection group query
- **BigQuery settings**: Project ID, dataset, table prefix, location
- **Processing settings**: Batch sizes, worker threads
- **File paths**: Data directory, output files

## Usage

### Step 1: Download Firestore Data

```bash
python download_firestore_data.py
```

This script:
- Connects to Firebase project `conflixis-web`
- Downloads all documents from `member_shards` collection using collection group query
- Formats each document for the BigQuery schema:
  - `timestamp`: Current timestamp when downloaded
  - `event_id`: Unique UUID for each record
  - `document_name`: Full Firestore document path
  - `operation`: Set to "import"
  - `data`: Document data as JSON string
  - `old_data`: null (for imports)
  - `document_id`: Document ID
  - `path_params`: Collection metadata as JSON
- Saves data to `data/firestore_data.jsonl` (newline-delimited JSON)
- Creates metadata file `data/firestore_data_metadata.json`

### Step 2: Upload to BigQuery

```bash
python upload_to_bigquery.py
```

This script:
- Creates BigQuery dataset `firestore_export` if needed (in US location)
- Creates table `member_shards_raw_changelog` with the predefined schema
- Uploads the JSONL file directly to BigQuery
- Table is partitioned by `timestamp` field for better performance

## Data Directory Structure

```
data/
├── firestore_data.jsonl          # Formatted data for BigQuery (JSONL format)
└── firestore_data_metadata.json  # Download metadata and statistics
```

## BigQuery Schema

The data is loaded into BigQuery with this fixed schema:

| Field | Type | Description |
|-------|------|-------------|
| timestamp | TIMESTAMP | When the document was downloaded |
| event_id | STRING | Unique identifier for this record |
| document_name | STRING | Full Firestore document path |
| operation | STRING | Always "import" for backfill |
| data | STRING | Document data as JSON string |
| old_data | STRING | Always null for imports |
| document_id | STRING | Firestore document ID |
| path_params | STRING | Collection metadata as JSON |

You can parse the JSON data in BigQuery using `JSON_EXTRACT` functions.

## Querying the Data

The `sql/` directory contains example queries for working with the imported data:

1. **`simple_parse_example.sql`** - Basic queries to get started:
   - View raw data
   - Extract member counts
   - Simple member extraction

2. **`parse_member_shards.sql`** - Advanced parsing with CTEs:
   - Structured data extraction
   - Full member details parsing

3. **`member_shards_queries.sql`** - Production-ready queries:
   - Full member details with all fields
   - Group statistics and member counts
   - Job title analysis
   - Manager team queries
   - Member search functionality
   - Entity relationship analysis
   - Includes CREATE VIEW statement for materialized views

Example query to extract member data:
```sql
SELECT 
  JSON_EXTRACT_SCALAR(data, '$.id') as group_id,
  JSON_EXTRACT_SCALAR(member, '$.email') as email,
  JSON_EXTRACT_SCALAR(member, '$.name') as name
FROM `conflixis-engine.firestore_export.member_shards_raw_changelog`,
UNNEST(JSON_EXTRACT_ARRAY(data, '$.members')) as member
```

## Monitoring

All scripts write logs to:
- Console output (stdout)
- `backfill.log` file

## Error Handling

- Scripts include retry logic for transient failures
- Upload script uses BigQuery's built-in error handling with max_bad_records setting

## Notes

- The scripts use service account credentials from `GOOGLE_APPLICATION_CREDENTIALS` environment variable or default credentials
- Data is uploaded with `WRITE_TRUNCATE` disposition (replaces existing table)
- Table is partitioned by `timestamp` field for better query performance
- All Firestore special types (timestamps, references, geopoints, bytes) are preserved in the JSON data field