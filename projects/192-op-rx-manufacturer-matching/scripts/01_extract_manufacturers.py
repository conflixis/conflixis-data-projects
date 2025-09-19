#!/usr/bin/env python3
"""
Extract unique manufacturer names from Open Payments and Prescriptions databases
"""

import os
import sys
import pandas as pd
from pathlib import Path
from google.cloud import bigquery
from dotenv import load_dotenv
import logging

# Add parent directories to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.data.bigquery_connector import BigQueryConnector

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def extract_op_manufacturers(client: bigquery.Client) -> pd.DataFrame:
    """Extract unique manufacturer names from Open Payments"""

    query = """
    SELECT DISTINCT
        UPPER(TRIM(Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name)) as manufacturer_name,
        COUNT(*) as payment_count,
        SUM(Total_Amount_of_Payment_USDollars) as total_payments,
        COUNT(DISTINCT Physician_Profile_ID) as unique_providers
    FROM `data-analytics-389803.OPR_PHYS_OTH.GNRL_PAYMENTS_optimized`
    WHERE Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name IS NOT NULL
        AND Payment_Publication_Date >= '2020-01-01'
    GROUP BY 1
    HAVING payment_count > 10  -- Filter out very rare manufacturers
    ORDER BY total_payments DESC
    """

    logger.info("Extracting Open Payments manufacturers...")
    df = client.query(query).to_dataframe()
    logger.info(f"Found {len(df)} unique manufacturers in Open Payments")

    return df


def extract_rx_manufacturers(client: bigquery.Client) -> pd.DataFrame:
    """Extract unique manufacturer names from Prescriptions"""

    query = """
    SELECT DISTINCT
        UPPER(TRIM(BRND_NAME)) as drug_brand,
        UPPER(TRIM(GNRC_NAME)) as generic_name,
        UPPER(TRIM(PRSCRBR_NPI)) as prescriber_npi,
        COUNT(*) as prescription_count,
        SUM(TOT_30_DAY_FILL_CNT) as total_fills,
        SUM(TOT_DRUG_CST) as total_cost
    FROM `data-analytics-389803.MDRX_PRESCRIPTIONS.MED_D_DATASETS_optimized`
    WHERE BRND_NAME IS NOT NULL
        AND TOT_DRUG_CST > 0
    GROUP BY 1, 2, 3
    """

    logger.info("Extracting Prescription drug/manufacturer data...")
    df = client.query(query).to_dataframe()

    # Extract unique brand names (these will need to be mapped to manufacturers)
    unique_brands = df[['drug_brand']].drop_duplicates()
    unique_brands['source'] = 'brand_name'

    # Also get unique generic names
    unique_generics = df[['generic_name']].drop_duplicates()
    unique_generics.columns = ['drug_brand']
    unique_generics['source'] = 'generic_name'

    # Combine and deduplicate
    all_drug_names = pd.concat([unique_brands, unique_generics]).drop_duplicates(subset=['drug_brand'])

    logger.info(f"Found {len(all_drug_names)} unique drug/brand names in Prescriptions")

    return all_drug_names


def extract_drug_manufacturer_mapping(client: bigquery.Client) -> pd.DataFrame:
    """
    Extract any existing drug-to-manufacturer mappings if available
    This would typically come from a reference table or FDA database
    """

    # Check if we have a drug-manufacturer mapping table
    query = """
    -- Check for existing mapping tables
    SELECT table_name
    FROM `data-analytics-389803.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE '%drug%manufacturer%'
        OR table_name LIKE '%brand%company%'
        OR table_name LIKE '%ndc%'
    LIMIT 10
    """

    try:
        tables = client.query(query).to_dataframe()
        if not tables.empty:
            logger.info("Found potential mapping tables:")
            for table in tables['table_name']:
                logger.info(f"  - {table}")
    except Exception as e:
        logger.warning(f"Could not check for mapping tables: {e}")

    # For now, return empty DataFrame - will need manual mapping or external data source
    return pd.DataFrame()


def save_extracted_data(op_df: pd.DataFrame, rx_df: pd.DataFrame, output_dir: Path):
    """Save extracted manufacturer data to CSV files"""

    # Create output directory if not exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save Open Payments manufacturers
    op_file = output_dir / "op_manufacturers.csv"
    op_df.to_csv(op_file, index=False)
    logger.info(f"Saved Open Payments manufacturers to {op_file}")

    # Save Prescription drug/brand names
    rx_file = output_dir / "rx_drug_names.csv"
    rx_df.to_csv(rx_file, index=False)
    logger.info(f"Saved Prescription drug names to {rx_file}")

    # Create summary statistics
    summary = {
        "Open Payments": {
            "unique_manufacturers": len(op_df),
            "total_payments": f"${op_df['total_payments'].sum():,.2f}",
            "total_payment_records": op_df['payment_count'].sum(),
            "avg_payments_per_manufacturer": f"${op_df['total_payments'].mean():,.2f}"
        },
        "Prescriptions": {
            "unique_drug_names": len(rx_df),
            "brand_names": len(rx_df[rx_df['source'] == 'brand_name']),
            "generic_names": len(rx_df[rx_df['source'] == 'generic_name'])
        }
    }

    # Save summary
    import json
    summary_file = output_dir / "extraction_summary.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Saved extraction summary to {summary_file}")

    return summary


def main():
    """Main extraction workflow"""

    logger.info("Starting manufacturer name extraction...")

    # Initialize BigQuery client using existing connector
    connector = BigQueryConnector()
    client = connector.client

    # Set output directory
    output_dir = Path(__file__).parent.parent / "data" / "input"

    try:
        # Extract manufacturers from Open Payments
        op_manufacturers = extract_op_manufacturers(client)

        # Extract drug/brand names from Prescriptions
        rx_drugs = extract_rx_manufacturers(client)

        # Check for existing mappings
        existing_mappings = extract_drug_manufacturer_mapping(client)

        # Save extracted data
        summary = save_extracted_data(op_manufacturers, rx_drugs, output_dir)

        # Print summary
        logger.info("\n" + "="*60)
        logger.info("EXTRACTION COMPLETE")
        logger.info("="*60)
        logger.info(f"Open Payments: {summary['Open Payments']['unique_manufacturers']} manufacturers")
        logger.info(f"Prescriptions: {summary['Prescriptions']['unique_drug_names']} drug names")
        logger.info("\nNOTE: Prescription data contains drug NAMES, not manufacturer NAMES.")
        logger.info("Additional mapping will be required to match drugs to manufacturers.")
        logger.info("Consider using FDA Orange Book or NDC database for drug-to-manufacturer mapping.")

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise


if __name__ == "__main__":
    main()