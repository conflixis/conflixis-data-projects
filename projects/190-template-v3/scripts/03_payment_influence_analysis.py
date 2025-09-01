#!/usr/bin/env python3
"""
Analyze Payment-Prescription Correlations for a Health System
Part 3 of the COI Analytics Report Template

This script analyzes the correlation between Open Payments and prescribing patterns.
It is driven by a CONFIG.yaml file.
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

def analyze_drug_payment_correlation(client, bq_config, drug_name):
    """Analyze correlation between payments and prescribing for a specific drug."""
    npi_table = f"{bq_config['project_id']}.{bq_config['dataset_id']}.{bq_config['npi_table']}"
    op_table = f"{bq_config['project_id']}.{bq_config['dataset_id']}.{bq_config['op_table']}"
    rx_table = f"{bq_config['project_id']}.{bq_config['dataset_id']}.{bq_config['rx_table']}"
    
    query = f"""
    WITH system_providers AS (
        SELECT DISTINCT NPI FROM `{npi_table}`
    ),
    drug_payments AS (
        SELECT CAST(op.covered_recipient_npi AS STRING) AS NPI,
               SUM(op.total_amount_of_payment_usdollars) as total_op_payments
        FROM `{op_table}` op
        WHERE op.program_year BETWEEN 2020 AND 2024
          AND UPPER(op.name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE UPPER('%{drug_name}%')
        GROUP BY NPI
    ),
    drug_prescriptions AS (
        SELECT CAST(rx.NPI AS STRING) AS NPI,
               SUM(rx.PAYMENTS) as total_rx_payments
        FROM `{rx_table}` rx
        WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
          AND UPPER(rx.BRAND_NAME) LIKE UPPER('%{drug_name}%')
        GROUP BY NPI
    ),
    provider_summary AS (
        SELECT 
            sp.NPI,
            CASE WHEN dp.NPI IS NOT NULL THEN 1 ELSE 0 END as received_payment,
            COALESCE(drx.total_rx_payments, 0) as total_rx_payments
        FROM system_providers sp
        LEFT JOIN drug_payments dp ON sp.NPI = dp.NPI
        LEFT JOIN drug_prescriptions drx ON sp.NPI = drx.NPI
    )
    SELECT 
        received_payment,
        COUNT(DISTINCT NPI) as provider_count,
        AVG(total_rx_payments) as avg_rx_payments
    FROM provider_summary
    WHERE total_rx_payments > 0
    GROUP BY received_payment
    """
    
    logger.info(f"\nAnalyzing payment correlation for {drug_name}...")
    df = client.query(query).to_dataframe()
    
    return df

def main():
    """Main execution function"""
    try:
        config = load_config()
        hs_config = config['health_system']
        path_config = config['project_paths']
        bq_config = config['bigquery']
        report_config = config['report_settings']

        bq_config['npi_table'] = bq_config['npi_table'].replace('[abbrev]', hs_config['short_name'])

        logger.info("=" * 80)
        logger.info(f"STARTING CORRELATION ANALYSIS FOR: {hs_config['name']}")
        logger.info("=" * 80)
        
        project_dir = Path(path_config['project_dir'])
        output_dir = project_dir / path_config['output_dir']
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        logger.info("Connecting to BigQuery...")
        client = create_bigquery_client(bq_config['project_id'])
        
        key_drugs = report_config['key_drugs']
        all_drug_results = []

        for drug in key_drugs:
            df = analyze_drug_payment_correlation(client, bq_config, drug)
            df['drug_name'] = drug
            all_drug_results.append(df)
        
        if all_drug_results:
            final_df = pd.concat(all_drug_results)
            output_path = output_dir / f"{hs_config['short_name']}_correlation_by_drug_{timestamp}.csv"
            final_df.to_csv(output_path, index=False)
            logger.info(f"Saved drug correlation results to {output_path}")

        # ... (calls to other refactored analysis functions) ...
        
        logger.info(f"\nAll results saved to: {output_dir}")
        logger.info("=" * 80)
        logger.info("Correlation Analysis Complete!")
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        raise

if __name__ == "__main__":
    main()
