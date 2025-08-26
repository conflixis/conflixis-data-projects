#!/usr/bin/env python3
"""
Create BigQuery table for Corewell Health NPIs
DA-175: Corewell Health Analysis - BigQuery NPI Table Creation

This script:
1. Reads consolidated NPI list
2. Creates BigQuery table for efficient querying
3. Enables joining with rx_op_enhanced_full
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import json
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define paths
PROJECT_DIR = Path("/home/incent/conflixis-data-projects/projects/175-corewell-health-analysis")
DATA_DIR = PROJECT_DIR / "rx-op-enhanced" / "data" / "processed"

# BigQuery configuration
PROJECT_ID = "data-analytics-389803"
DATASET_ID = "conflixis_agent"
TABLE_ID = "corewell_health_npis"

def get_latest_npi_file():
    """Get the most recent NPI list file"""
    npi_files = list(DATA_DIR.glob("corewell_health_npi_list_*.csv"))
    if not npi_files:
        raise FileNotFoundError("No NPI list files found. Run 01_consolidate_npis.py first.")
    
    latest_file = max(npi_files, key=lambda p: p.stat().st_mtime)
    logger.info(f"Using NPI file: {latest_file.name}")
    return latest_file

def create_bigquery_client():
    """Create BigQuery client with credentials"""
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
    
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=['https://www.googleapis.com/auth/bigquery']
    )
    
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    return client

def create_and_upload_table(client, df):
    """Create BigQuery table and upload NPI data"""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Rename columns to remove spaces for BigQuery compatibility
    df = df.rename(columns={
        'Primary Specialty': 'Primary_Specialty',
        'Primary Hospital Affiliation': 'Primary_Hospital_Affiliation'
    })
    
    # Define schema
    schema = [
        bigquery.SchemaField("NPI", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Full_Name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Primary_Specialty", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Primary_Hospital_Affiliation", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source_hospital_system", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("HQ_STATE", "STRING", mode="NULLABLE"),
    ]
    
    # Configure job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite if exists
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header
    )
    
    # Load data
    logger.info(f"Creating/updating table: {table_ref}")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    
    # Wait for job to complete
    job.result()
    
    # Get table info
    table = client.get_table(table_ref)
    logger.info(f"Table created/updated successfully:")
    logger.info(f"  - Total rows: {table.num_rows}")
    logger.info(f"  - Size: {table.num_bytes / 1024 / 1024:.2f} MB")
    
    return table

def verify_upload(client):
    """Verify the upload with sample queries"""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Count total NPIs
    query = f"""
    SELECT COUNT(DISTINCT NPI) as total_npis
    FROM `{table_ref}`
    """
    result = client.query(query).result()
    for row in result:
        logger.info(f"\nVerification - Total NPIs: {row.total_npis}")
    
    # Count by specialty
    query = f"""
    SELECT 
        Primary_Specialty,
        COUNT(*) as count
    FROM `{table_ref}`
    WHERE Primary_Specialty IS NOT NULL
    GROUP BY Primary_Specialty
    ORDER BY count DESC
    LIMIT 5
    """
    
    logger.info("\nTop 5 Specialties:")
    result = client.query(query).result()
    for row in result:
        logger.info(f"  - {row.Primary_Specialty}: {row.count}")
    
    # Count by hospital system
    query = f"""
    SELECT 
        source_hospital_system,
        COUNT(*) as count
    FROM `{table_ref}`
    GROUP BY source_hospital_system
    ORDER BY count DESC
    """
    
    logger.info("\nProviders by Hospital System:")
    result = client.query(query).result()
    for row in result:
        logger.info(f"  - {row.source_hospital_system}: {row.count}")

def main():
    """Main execution function"""
    logger.info("=" * 80)
    logger.info("Corewell Health BigQuery NPI Table Creation")
    logger.info("=" * 80)
    
    try:
        # Get latest NPI file
        npi_file = get_latest_npi_file()
        
        # Read NPI data
        logger.info("Loading NPI data...")
        df = pd.read_csv(npi_file)
        logger.info(f"  - Loaded {len(df)} NPIs")
        
        # Create BigQuery client
        logger.info("\nConnecting to BigQuery...")
        client = create_bigquery_client()
        logger.info("  - Connected successfully")
        
        # Create and upload table
        logger.info("\nUploading to BigQuery...")
        table = create_and_upload_table(client, df)
        
        # Verify upload
        logger.info("\nVerifying upload...")
        verify_upload(client)
        
        # Create sample query file
        query_file = PROJECT_DIR / "rx-op-enhanced" / "scripts" / "sample_queries.sql"
        with open(query_file, 'w') as f:
            f.write(f"""-- Sample queries for Corewell Health NPI analysis
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Table: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}

-- Query 1: Join with rx_op_enhanced to get payment attribution
SELECT 
    cs.NPI,
    cs.Full_Name,
    cs.Primary_Specialty,
    cs.Primary_Hospital_Affiliation,
    rx.source_manufacturer,
    rx.year,
    rx.month,
    rx.TotalDollarsFrom as payment_received,
    rx.totalNext6 as prescriptions_next_6mo,
    rx.attributable_dollars,
    rx.attributable_pct,
    SAFE_DIVIDE(rx.attributable_dollars, rx.TotalDollarsFrom) as roi
FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` cs
LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.rx_op_enhanced_full` rx
    ON cs.NPI = rx.NPI
WHERE rx.TotalDollarsFrom > 0
ORDER BY rx.attributable_dollars DESC
LIMIT 100;

-- Query 2: Summary statistics for Corewell Health providers
SELECT 
    COUNT(DISTINCT cs.NPI) as total_cs_providers,
    COUNT(DISTINCT rx.NPI) as cs_providers_with_payments,
    SUM(rx.TotalDollarsFrom) as total_payments_received,
    SUM(rx.attributable_dollars) as total_attributable,
    SAFE_DIVIDE(SUM(rx.attributable_dollars), SUM(rx.TotalDollarsFrom)) as overall_roi
FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` cs
LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.rx_op_enhanced_full` rx
    ON cs.NPI = rx.NPI;

-- Query 3: High-risk Corewell Health providers (>30% attribution)
SELECT 
    cs.NPI,
    cs.Full_Name,
    cs.Primary_Specialty,
    cs.Primary_Hospital_Affiliation,
    AVG(rx.attributable_pct) as avg_attribution_pct,
    SUM(rx.TotalDollarsFrom) as total_payments,
    SUM(rx.attributable_dollars) as total_attributable
FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` cs
INNER JOIN `{PROJECT_ID}.{DATASET_ID}.rx_op_enhanced_full` rx
    ON cs.NPI = rx.NPI
WHERE rx.TotalDollarsFrom > 0
GROUP BY cs.NPI, cs.Full_Name, cs.Primary_Specialty, cs.Primary_Hospital_Affiliation
HAVING AVG(rx.attributable_pct) > 0.30
ORDER BY avg_attribution_pct DESC;
""")
        logger.info(f"\nSample queries saved to: {query_file}")
        
        logger.info("\n" + "=" * 80)
        logger.info("BigQuery table creation complete!")
        logger.info(f"Table: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()