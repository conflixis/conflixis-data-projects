#!/usr/bin/env python3
"""
Upload Firestore data to BigQuery.

This script uploads the formatted Firestore data to BigQuery using the
predefined schema with data as JSON strings.
"""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from tqdm import tqdm

import config

# Set up logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)




class BigQueryUploader:
    """Upload data to BigQuery."""
    
    def __init__(self):
        self.client = bigquery.Client(project=config.BQ_PROJECT_ID)
        self.dataset_ref = None
        self.table_ref = None
    
    def create_dataset_if_needed(self) -> bigquery.Dataset:
        """Create BigQuery dataset if it doesn't exist."""
        dataset_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}"
        
        try:
            dataset = self.client.get_dataset(dataset_id)
            logger.info(f"Dataset {dataset_id} already exists")
        except Exception:
            # Create dataset
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = config.BQ_DATASET_LOCATION
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {dataset_id}")
        
        self.dataset_ref = dataset.reference
        return dataset
    
    def get_schema(self) -> List[bigquery.SchemaField]:
        """Define the fixed BigQuery schema."""
        return [
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("document_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("operation", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("data", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("old_data", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("document_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("path_params", "STRING", mode="NULLABLE"),
        ]
    
    def create_table_if_needed(self) -> bigquery.Table:
        """Create BigQuery table if it doesn't exist."""
        table_id = f"{config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_PREFIX}"
        
        try:
            table = self.client.get_table(table_id)
            logger.info(f"Table {table_id} already exists")
        except Exception:
            # Create table with fixed schema
            schema = self.get_schema()
            table = bigquery.Table(table_id, schema=schema)
            
            # Set up partitioning by timestamp
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="timestamp"
            )
            
            table = self.client.create_table(table, timeout=30)
            logger.info(f"Created table {table_id}")
        
        self.table_ref = table.reference
        return table
    
    def upload_from_jsonl(self, jsonl_file: str) -> None:
        """Upload data from JSONL file to BigQuery."""
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=self.get_schema(),
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Replace table
            max_bad_records=10,
        )
        
        # Load from file
        with open(jsonl_file, "rb") as f:
            job = self.client.load_table_from_file(
                f,
                self.table_ref,
                job_config=job_config
            )
        
        # Wait for job to complete
        logger.info(f"Starting BigQuery load job...")
        job.result(timeout=600)  # 10 minutes timeout
        
        if job.errors:
            logger.error(f"Job errors: {job.errors}")
            raise Exception(f"BigQuery job failed with errors: {job.errors}")
        
        logger.info(f"Loaded {job.output_rows} rows to {self.table_ref.path}")


def main():
    """Main function."""
    try:
        logger.info("=" * 50)
        logger.info("Starting BigQuery upload")
        logger.info("=" * 50)
        
        # Get the JSONL file path
        jsonl_file = config.DOWNLOAD_FILE.replace('.json', '.jsonl')
        
        if not os.path.exists(jsonl_file):
            logger.error(f"JSONL file not found: {jsonl_file}")
            logger.error("Please run download_firestore_data.py first")
            sys.exit(1)
        
        # Upload to BigQuery
        uploader = BigQueryUploader()
        
        # Create dataset and table
        uploader.create_dataset_if_needed()
        uploader.create_table_if_needed()
        
        # Upload from JSONL file
        logger.info(f"Uploading data from {jsonl_file}")
        uploader.upload_from_jsonl(jsonl_file)
        
        logger.info("=" * 50)
        logger.info("Upload completed successfully!")
        logger.info(f"Dataset: {config.BQ_PROJECT_ID}.{config.BQ_DATASET_ID}")
        logger.info(f"Table: {config.BQ_TABLE_PREFIX}")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Error during upload: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()