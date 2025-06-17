# BigQuery Examples

This folder contains example scripts demonstrating different ways to query the PHYSICIANS_OVERVIEW table in BigQuery.

## Prerequisites

1. **Environment Setup**: Ensure you have the `GOOGLE_APPLICATION_CREDENTIALS` environment variable set in `/common/.env` pointing to your service account key file.

2. **Dependencies**:
   - Python: `google-cloud-bigquery`, `pandas`, `python-dotenv`
   - R: `bigrquery`, `tidyverse`, `DBI`

## Example Scripts

### 1. R Script (`01_bigquery_query.R`)
- Demonstrates querying BigQuery using R
- Shows two methods: DBI interface and bq_project_query
- Includes basic data exploration

**Run with:**
```bash
Rscript 01_bigquery_query.R
```

### 2. Jupyter Notebook (`02_bigquery_query.ipynb`)
- Interactive notebook for exploratory data analysis
- Includes data visualization examples
- Shows parameterized queries

**Run with:**
```bash
jupyter notebook 02_bigquery_query.ipynb
```

### 3. SQL Script (`03_bigquery_query.sql`)
- Pure SQL queries for BigQuery
- Can be used with bq command-line tool or BigQuery Console
- Includes commented examples for common query patterns

**Run with:**
```bash
bq query --use_legacy_sql=false < 03_bigquery_query.sql
```

### 4. Python Script (`04_bigquery_query.py`)
- Standalone Python script with command-line interface
- Supports different output formats (display, CSV, JSON)
- Includes table metadata inspection

**Run with:**
```bash
# Basic query (display first 100 rows)
python 04_bigquery_query.py

# Query 500 rows and save to CSV
python 04_bigquery_query.py --limit 500 --output csv

# Get table information
python 04_bigquery_query.py --info
```

## Authentication

All scripts use the service account key specified in `GOOGLE_APPLICATION_CREDENTIALS`. Make sure this is properly configured in your `common/.env` file:

```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
```

## Query Details

All examples query the same table:
- **Project**: data-analytics-389803
- **Dataset**: CONFLIXIS_309340
- **Table**: PHYSICIANS_OVERVIEW

The basic query used is:
```sql
SELECT * FROM `data-analytics-389803.CONFLIXIS_309340.PHYSICIANS_OVERVIEW` LIMIT 100
```

## Next Steps

1. Copy one of these examples to create your own analysis
2. Modify the queries based on your specific needs
3. Explore the table schema to understand available columns
4. Join with other tables in the dataset as needed