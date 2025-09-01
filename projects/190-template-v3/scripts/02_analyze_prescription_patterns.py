#!/usr/bin/env python3
"""
Analyze Prescription Patterns for Health System Providers
Part 2 of the COI Analytics Report Template

This script analyzes prescription patterns for a specified health system's providers
using claims data. It is driven by a CONFIG.yaml file.
"""

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import json
import os
import yaml
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config():
    """Load the configuration from CONFIG.yaml"""
    config_path = Path(__file__).resolve().parent.parent / "CONFIG.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"CONFIG.yaml not found at {config_path}")
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def create_bigquery_client(project_id):
    """Create BigQuery client with credentials"""
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
    
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=['https://www.googleapis.com/auth/bigquery']
    )
    
    return bigquery.Client(project=project_id, credentials=credentials)

def analyze_top_drugs(client, bq_config, health_system_name):
    """Identify top prescribed drugs by the system's providers"""
    npi_table = f"{bq_config['project_id']}.{bq_config['dataset_id']}.{bq_config['npi_table']}"
    rx_table = f"{bq_config['project_id']}.{bq_config['dataset_id']}.{bq_config['rx_table']}"
    
    query = f"""
    WITH system_rx AS (
        SELECT 
            rx.BRAND_NAME,
            rx.GENERIC_NAME,
            rx.MANUFACTURER,
            rx.NPI,
            rx.PAYMENTS,
            rx.PRESCRIPTIONS
        FROM `{rx_table}` rx
        INNER JOIN `{npi_table}` cs ON CAST(rx.NPI AS STRING) = cs.NPI
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
    )
    SELECT 
        BRAND_NAME,
        GENERIC_NAME,
        MANUFACTURER,
        COUNT(DISTINCT NPI) as prescriber_count,
        SUM(PRESCRIPTIONS) as total_prescriptions,
        SUM(PAYMENTS) as total_payments
    FROM system_rx
    WHERE BRAND_NAME IS NOT NULL
    GROUP BY BRAND_NAME, GENERIC_NAME, MANUFACTURER
    ORDER BY total_payments DESC
    LIMIT 50
    """
    
    logger.info(f"\nAnalyzing top prescribed drugs for {health_system_name}...")
    df = client.query(query).to_dataframe()
    
    logger.info("\nTop 10 Drugs by Total Payments:")
    for i, row in df.head(10).iterrows():
        logger.info(f"  {row['BRAND_NAME']}: ${row['total_payments']:,.0f}")
    
    return df

# ... (Other analysis functions would be refactored similarly) ...

def main():
    """Main execution function"""
    try:
        config = load_config()
        hs_config = config['health_system']
        path_config = config['project_paths']
        bq_config = config['bigquery']

        bq_config['npi_table'] = bq_config['npi_table'].replace('[abbrev]', hs_config['short_name'])

        logger.info("=" * 80)
        logger.info(f"STARTING PRESCRIPTION PATTERN ANALYSIS FOR: {hs_config['name']}")
        logger.info("=" * 80)
        
        project_dir = Path(path_config['project_dir'])
        output_dir = project_dir / path_config['output_dir']
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        logger.info("Connecting to BigQuery...")
        client = create_bigquery_client(bq_config['project_id'])
        
        results = {}
        
        # Example of a refactored function call
        results['top_drugs'] = analyze_top_drugs(client, bq_config, hs_config['name'])
        results['top_drugs'].to_csv(output_dir / f"{hs_config['short_name']}_rx_top_drugs_{timestamp}.csv", index=False)
        
        # ... (calls to other refactored analysis functions) ...
        
        logger.info(f"\nAll results saved to: {output_dir}")
        logger.info("=" * 80)
        logger.info("Prescription Analysis Complete!")
        
        return results
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        raise

if __name__ == "__main__":
    results = main()
