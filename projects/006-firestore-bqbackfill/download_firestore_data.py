#!/usr/bin/env python3
"""
Download Firestore data from the member_shards collection and format for BigQuery.

This script connects to Firebase and downloads all documents, formatting them
for the specific BigQuery schema with data as JSON strings.
"""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List
import uuid

import firebase_admin
from firebase_admin import credentials, firestore
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


def initialize_firebase() -> firestore.Client:
    """Initialize Firebase Admin SDK and return Firestore client."""
    try:
        # Check if already initialized
        firebase_admin.get_app()
        logger.info("Firebase already initialized")
    except ValueError:
        # Initialize Firebase
        if config.SERVICE_ACCOUNT_KEY_PATH:
            logger.info(f"Initializing Firebase with service account: {config.SERVICE_ACCOUNT_KEY_PATH}")
            cred = credentials.Certificate(config.SERVICE_ACCOUNT_KEY_PATH)
            # Explicitly set the project ID for Firebase
            firebase_admin.initialize_app(cred, {
                'projectId': config.FIREBASE_PROJECT_ID,
            })
        else:
            logger.info("Initializing Firebase with default credentials")
            # Try to use Google Cloud Firestore client directly
            try:
                from google.cloud import firestore as gcp_firestore
                logger.info(f"Using direct Firestore client for project: {config.FIREBASE_PROJECT_ID}")
                return gcp_firestore.Client(project=config.FIREBASE_PROJECT_ID)
            except Exception as e:
                logger.warning(f"Direct Firestore client failed: {e}")
                firebase_admin.initialize_app(options={
                    'projectId': config.FIREBASE_PROJECT_ID,
                })
    
    return firestore.client()


def serialize_document(doc_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Serialize Firestore document data to JSON-compatible format.
    
    Handles special Firestore types like timestamps, references, etc.
    """
    def serialize_value(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, firestore.DocumentReference):
            return {
                "_type": "reference",
                "path": value.path
            }
        elif isinstance(value, firestore.GeoPoint):
            return {
                "_type": "geopoint",
                "latitude": value.latitude,
                "longitude": value.longitude
            }
        elif isinstance(value, bytes):
            return {
                "_type": "bytes",
                "value": value.hex()
            }
        elif isinstance(value, dict):
            return {k: serialize_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [serialize_value(item) for item in value]
        else:
            return value
    
    return serialize_value(doc_data)


def download_and_format_documents(db: firestore.Client) -> List[Dict[str, Any]]:
    """Download all documents and format them for BigQuery schema."""
    formatted_records = []
    
    logger.info(f"Starting download from collection: {config.COLLECTION_PATH}")
    logger.info(f"Using collection group query: {config.USE_COLLECTION_GROUP}")
    
    if config.USE_COLLECTION_GROUP:
        # Use collection group query
        query = db.collection_group(config.COLLECTION_PATH)
    else:
        # Use regular collection query
        query = db.collection(config.COLLECTION_PATH)
    
    # Count total documents first
    logger.info("Counting total documents...")
    total_count = 0
    for _ in query.stream():
        total_count += 1
    logger.info(f"Total documents to download: {total_count}")
    
    # Download documents in batches
    last_doc = None
    with tqdm(total=total_count, desc="Downloading documents") as pbar:
        while True:
            # Build query with pagination
            batch_query = query.limit(config.DOWNLOAD_BATCH_SIZE)
            if last_doc:
                batch_query = batch_query.start_after(last_doc)
            
            # Fetch batch
            docs = list(batch_query.stream())
            if not docs:
                break
            
            # Process documents
            for doc in docs:
                # Serialize the document data
                doc_data = serialize_document(doc.to_dict() or {})
                
                # Format for BigQuery schema
                record = {
                    "timestamp": datetime.now().isoformat(),
                    "event_id": str(uuid.uuid4()),
                    "document_name": doc.reference.path,
                    "operation": "import",  # This is an import operation
                    "data": json.dumps(doc_data),  # Document data as JSON string
                    "old_data": None,  # No old data for imports
                    "document_id": doc.id,
                    "path_params": json.dumps({
                        "collection": config.COLLECTION_PATH,
                        "collection_group": config.USE_COLLECTION_GROUP
                    })
                }
                
                formatted_records.append(record)
                pbar.update(1)
            
            # Update pagination cursor
            last_doc = docs[-1]
            
            # Break if we got less than a full batch
            if len(docs) < config.DOWNLOAD_BATCH_SIZE:
                break
    
    logger.info(f"Downloaded and formatted {len(formatted_records)} documents")
    return formatted_records


def save_documents(records: List[Dict[str, Any]], output_file: str) -> None:
    """Save formatted records to JSONL file for BigQuery."""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Save as newline-delimited JSON (JSONL) for BigQuery
    jsonl_file = output_file.replace('.json', '.jsonl')
    with open(jsonl_file, 'w', encoding='utf-8') as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
    
    logger.info(f"Saved {len(records)} records to {jsonl_file}")
    
    # Also save metadata separately
    metadata_file = output_file.replace('.json', '_metadata.json')
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump({
            "download_time": datetime.now().isoformat(),
            "project_id": config.FIREBASE_PROJECT_ID,
            "collection": config.COLLECTION_PATH,
            "collection_group": config.USE_COLLECTION_GROUP,
            "record_count": len(records),
            "output_file": jsonl_file
        }, f, indent=2)
    
    logger.info(f"Saved metadata to {metadata_file}")


def main():
    """Main function."""
    try:
        logger.info("=" * 50)
        logger.info("Starting Firestore data download")
        logger.info("=" * 50)
        
        # Initialize Firebase
        db = initialize_firebase()
        
        # Download and format documents
        records = download_and_format_documents(db)
        
        if not records:
            logger.warning("No documents found to download")
            return
        
        # Save to file
        save_documents(records, config.DOWNLOAD_FILE)
        
        logger.info("Download completed successfully")
        
    except Exception as e:
        logger.error(f"Error during download: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()