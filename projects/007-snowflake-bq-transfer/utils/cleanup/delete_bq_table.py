#!/usr/bin/env python3
"""Delete BigQuery table."""

import os
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# Load config
current_path = Path(__file__).resolve()
for parent in current_path.parents:
    if (parent / "pyproject.toml").exists():
        env_path = parent / "common" / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        break

# Setup client
credentials = service_account.Credentials.from_service_account_file(
    os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
)
client = bigquery.Client(
    credentials=credentials,
    project=os.getenv('BQ_PROJECT_ID')
)

# Delete table
table_id = f"{os.getenv('BQ_PROJECT_ID')}.{os.getenv('SNOWFLAKE_BQ_TARGET_DATASET')}.PHYSICIAN_GROUPS_PRACTICE_LOCATIONS"
try:
    client.delete_table(table_id)
    print(f"Deleted table {table_id}")
except Exception as e:
    print(f"Error deleting table: {e}")