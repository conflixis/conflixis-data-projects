"""
Configuration settings for Firestore to BigQuery backfill scripts.
"""

import os
from typing import Optional

# Firebase settings
FIREBASE_PROJECT_ID = "conflixis-web"
COLLECTION_PATH = "member_shards"  # This is correct for collection group queries
USE_COLLECTION_GROUP = True

# BigQuery settings
BQ_PROJECT_ID = "conflixis-engine"
BQ_DATASET_ID = "firestore_export"
BQ_TABLE_PREFIX = "member_shards_raw_changelog"
BQ_DATASET_LOCATION = "US"

# Processing settings
BATCH_SIZE = 100  # Number of documents to process at once
DOWNLOAD_BATCH_SIZE = 300  # Number of documents to fetch from Firestore at once
MAX_WORKERS = 1  # Number of parallel workers (set to 1 for single-threaded)

# File paths
DATA_DIR = "data"
DOWNLOAD_FILE = os.path.join(DATA_DIR, "firestore_data.json")

# Logging settings
LOG_LEVEL = "INFO"
LOG_FILE = "backfill.log"

# BigQuery constraints
BQ_MAX_COLUMNS = 10000
BQ_MAX_NESTING_DEPTH = 15
BQ_FIELD_NAME_PATTERN = r"^[a-zA-Z_][a-zA-Z0-9_]*$"

# Optional: Service account key path (if not using default credentials)
SERVICE_ACCOUNT_KEY_PATH: Optional[str] = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds