# DA-194: BigQuery Region Transfer

## Overview
This project handles the transfer of BigQuery datasets between regions to optimize costs and improve query performance. It uses service account authentication (same as project 182) for secure access to BigQuery.

## Features
- ✅ Service account authentication via environment variables
- ✅ Progress tracking and resumable transfers
- ✅ Automatic verification of transferred data
- ✅ GCS cleanup after successful transfers
- ✅ Comprehensive logging and error handling
- ✅ Configuration file support
- ✅ Dry-run mode for planning

## Quick Start

### 1. Install Dependencies
```bash
cd projects/194-bq-region-transfer
pip install -r requirements.txt
```

### 2. Setup Authentication
Ensure you have the `GCP_SERVICE_ACCOUNT_KEY` environment variable set in your `.env` file:
```bash
# The .env file should be at the project root: /home/incent/conflixis-data-projects/.env
GCP_SERVICE_ACCOUNT_KEY='{"type": "service_account", "project_id": "...", ...}'
```

### 3. Test Connection
```bash
python test_connection.py
```

### 4. Run Transfer

#### Using the simple script:
```bash
python bq_transfer.py
```

#### Using the enhanced script with config:
```bash
# Dry run to see what will be transferred
python transfer_with_config.py --dry-run

# Transfer all tables
python transfer_with_config.py

# Transfer specific table
python transfer_with_config.py --table table_name

# Override configuration
python transfer_with_config.py --source-dataset my_dataset --dest-dataset my_dataset_US
```

## Project Files

```
194-bq-region-transfer/
├── README.md                  # This file
├── config.yaml               # Transfer configuration
├── requirements.txt          # Python dependencies
├── .gitignore               # Git ignore rules
├── draft.py                 # Original CLI-based transfer script
├── bq_transfer.py           # New SDK-based transfer script
├── transfer_with_config.py  # Enhanced script with config support
├── test_connection.py       # Connection testing script
└── migration_progress.json  # Progress tracking (auto-generated)
```

## Configuration

Edit `config.yaml` to customize transfer settings:

```yaml
transfer:
  source:
    dataset: "op_20250702"
    location: "us-east4"
  destination:
    dataset: "op_20250702_US"
    location: "US"
  gcs:
    bucket: "conflixis-temp"
    cleanup_after_transfer: true
```

## Scripts

### `bq_transfer.py`
Main transfer script using BigQuery Python SDK with:
- Service account authentication
- Progress tracking
- Automatic retry on failures
- Row count verification
- GCS cleanup

### `transfer_with_config.py`
Enhanced version that adds:
- YAML configuration support
- Command-line argument overrides
- Dry-run mode
- Single table transfer option

### `test_connection.py`
Tests BigQuery connection and lists available datasets.

### `draft.py`
Original CLI-based script (kept for reference).

## Transfer Process

1. **Authentication**: Uses service account JSON from environment
2. **Export**: Tables exported to GCS as Parquet files
3. **Import**: Parquet files imported to destination dataset
4. **Verification**: Row counts compared between source and destination
5. **Cleanup**: Temporary GCS files removed (optional)

## Progress Tracking

The script maintains a `migration_progress.json` file that tracks:
- Completed tables
- Failed tables
- Last update timestamp

If the transfer is interrupted, it will resume from where it left off.

## Monitoring

Check `transfer.log` for detailed transfer progress and any errors.

## Troubleshooting

1. **Authentication Error**: Ensure `GCP_SERVICE_ACCOUNT_KEY` is set correctly
2. **Permission Error**: Verify service account has BigQuery Data Editor role
3. **GCS Error**: Check that the bucket exists and service account has access
4. **Location Error**: Ensure source and destination locations are valid

## Status
- **Jira Ticket**: DA-194
- **Branch**: feature/DA-194-bq-region-transfer
- **Status**: Implementation Complete