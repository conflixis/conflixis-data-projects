#!/usr/bin/env python3
"""
Configuration for THR Disclosure Parse Project
DA-172: Extract and Join COI Data
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = DATA_DIR / "output"
RAW_DIR = DATA_DIR / "raw"

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

# BigQuery Configuration
PROJECT_ID = 'conflixis-engine'
DISCLOSURES_TABLE = 'firestore_export.disclosures_raw_latest'
MEMBERS_TABLE = 'firestore_export.member_shards_raw_latest_parsed'

# Filter Configuration
GROUP_ID = 'gcO9AHYlNSzFeGTRSFRa'  # Texas Health Resources group
CAMPAIGN_ID = 'qyH2ggzVV0WLkuRfem7S'  # 2025 Texas Health COI Survey

# Export Configuration
EXPORT_FORMATS = ['csv', 'parquet', 'json', 'excel']  # Formats to export
DATE_FORMAT = '%Y-%m-%d'
TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = PROJECT_ROOT / 'thr_disclosure_parse.log'