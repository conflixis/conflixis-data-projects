#!/usr/bin/env python3
"""
Extract unique manufacturer names from Open Payments and Prescriptions databases
Uses existing BigQuery connector from project 182
"""

import os
import sys
import pandas as pd
from pathlib import Path
import logging
import json

# Add the project 182 src to path to use its bigquery_connector
sys.path.insert(0, '/home/incent/conflixis-data-projects/projects/182-healthcare-coi-analytics-report-template/src')

from data.bigquery_connector import BigQueryConnector

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_op_manufacturers(connector) -> pd.DataFrame:
    """Extract manufacturer names and IDs from Open Payments"""

    query = """
    SELECT DISTINCT
        applicable_manufacturer_or_applicable_gpo_making_payment_name as manufacturer_name,
        applicable_manufacturer_or_applicable_gpo_making_payment_id as manufacturer_id
    FROM `data-analytics-389803.conflixis_data_projects.op_manufacturer_names`
    ORDER BY applicable_manufacturer_or_applicable_gpo_making_payment_name
    """

    logger.info("Extracting Open Payments manufacturers...")
    df = connector.query(query)
    logger.info(f"Found {len(df)} unique manufacturers in Open Payments")

    return df


def extract_rx_manufacturers(connector) -> pd.DataFrame:
    """Extract unique manufacturer names from Prescriptions"""

    query = """
    SELECT DISTINCT
        MANUFACTURER as manufacturer_name
    FROM `data-analytics-389803.conflixis_data_projects.rx_manufacturer_names`
    ORDER BY MANUFACTURER
    """

    logger.info("Extracting Prescription manufacturers...")
    df = connector.query(query)
    logger.info(f"Found {len(df)} unique manufacturers in Prescriptions")

    return df


def save_extracted_data(op_df: pd.DataFrame, rx_df: pd.DataFrame, output_dir: Path):
    """Save extracted manufacturer data to CSV files"""

    # Create output directory if not exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save Open Payments manufacturers
    op_file = output_dir / "op_manufacturers.csv"
    op_df.to_csv(op_file, index=False)
    logger.info(f"Saved {len(op_df)} Open Payments manufacturers to {op_file}")

    # Save Prescription manufacturer names
    rx_file = output_dir / "rx_manufacturers.csv"
    rx_df.to_csv(rx_file, index=False)
    logger.info(f"Saved {len(rx_df)} Prescription manufacturers to {rx_file}")

    # Create summary statistics
    summary = {
        "extraction_date": pd.Timestamp.now().isoformat(),
        "open_payments": {
            "unique_manufacturers": len(op_df),
            "top_10_manufacturers": op_df.head(10)['manufacturer_name'].tolist()
        },
        "prescriptions": {
            "unique_manufacturers": len(rx_df),
            "top_10_manufacturers": rx_df.head(10)['manufacturer_name'].tolist()
        }
    }

    # Save summary
    summary_file = output_dir / "extraction_summary.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    logger.info(f"Saved extraction summary to {summary_file}")

    return summary


def main():
    """Main extraction workflow"""

    logger.info("="*60)
    logger.info("Starting manufacturer name extraction...")
    logger.info("="*60)

    # Initialize BigQuery connector using existing auth
    try:
        connector = BigQueryConnector()
        logger.info("Successfully connected to BigQuery")
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery connector: {e}")
        raise

    # Set output directory
    output_dir = Path(__file__).parent.parent / "data" / "input"

    try:
        # Extract manufacturers from Open Payments
        logger.info("\n1. Extracting Open Payments manufacturers...")
        op_manufacturers = extract_op_manufacturers(connector)

        # Extract drug/brand names from Prescriptions
        logger.info("\n2. Extracting Prescription drug brands...")
        rx_drugs = extract_rx_manufacturers(connector)

        # Save extracted data
        logger.info("\n3. Saving extracted data...")
        summary = save_extracted_data(op_manufacturers, rx_drugs, output_dir)

        # Print summary
        logger.info("\n" + "="*60)
        logger.info("EXTRACTION COMPLETE")
        logger.info("="*60)
        logger.info(f"Open Payments: {summary['open_payments']['unique_manufacturers']} manufacturers (with IDs)")
        if summary['open_payments']['top_10_manufacturers']:
            logger.info(f"  Sample: {', '.join(summary['open_payments']['top_10_manufacturers'][:3])}...")
        logger.info(f"\nPrescriptions: {summary['prescriptions']['unique_manufacturers']} manufacturers")
        if summary['prescriptions']['top_10_manufacturers']:
            logger.info(f"  Sample: {', '.join(summary['prescriptions']['top_10_manufacturers'][:3])}...")
        logger.info("\n" + "="*60)
        logger.info("Files saved to: " + str(output_dir))
        logger.info("Next step: Run Tier2 name matching to create manufacturer mapping")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise


if __name__ == "__main__":
    main()