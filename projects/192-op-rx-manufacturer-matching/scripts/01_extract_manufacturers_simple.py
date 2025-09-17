#!/usr/bin/env python3
"""
Extract unique manufacturer names from Open Payments and Prescriptions databases
Simplified version without external dependencies
"""

import os
import pandas as pd
from pathlib import Path
from google.cloud import bigquery
import logging
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set up authentication using gcloud
os.environ['GCLOUD_PROJECT'] = 'data-analytics-389803'

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
    """Extract unique manufacturer names from Prescriptions

    Note: Prescriptions table has drug names, not manufacturer names directly.
    We'll need to map these later.
    """

    query = """
    -- Get top drug brands by total cost
    SELECT DISTINCT
        UPPER(TRIM(BRND_NAME)) as drug_brand,
        SUM(TOT_DRUG_CST) as total_cost,
        SUM(TOT_30_DAY_FILL_CNT) as total_fills,
        COUNT(DISTINCT PRSCRBR_NPI) as unique_prescribers
    FROM `data-analytics-389803.MDRX_PRESCRIPTIONS.MED_D_DATASETS_optimized`
    WHERE BRND_NAME IS NOT NULL
        AND BRND_NAME != ''
        AND TOT_DRUG_CST > 0
    GROUP BY 1
    HAVING total_cost > 10000  -- Filter out rare drugs
    ORDER BY total_cost DESC
    LIMIT 5000  -- Get top 5000 drug brands
    """

    logger.info("Extracting Prescription drug brand data...")
    df = client.query(query).to_dataframe()
    logger.info(f"Found {len(df)} unique drug brands in Prescriptions")

    return df


def save_extracted_data(op_df: pd.DataFrame, rx_df: pd.DataFrame, output_dir: Path):
    """Save extracted manufacturer data to CSV files"""

    # Create output directory if not exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save Open Payments manufacturers
    op_file = output_dir / "op_manufacturers.csv"
    op_df.to_csv(op_file, index=False)
    logger.info(f"Saved {len(op_df)} Open Payments manufacturers to {op_file}")

    # Save Prescription drug/brand names
    rx_file = output_dir / "rx_drug_brands.csv"
    rx_df.to_csv(rx_file, index=False)
    logger.info(f"Saved {len(rx_df)} Prescription drug brands to {rx_file}")

    # Create summary statistics
    summary = {
        "extraction_date": pd.Timestamp.now().isoformat(),
        "open_payments": {
            "unique_manufacturers": len(op_df),
            "total_payments": float(op_df['total_payments'].sum()),
            "total_payment_records": int(op_df['payment_count'].sum()),
            "avg_payments_per_manufacturer": float(op_df['total_payments'].mean()),
            "top_10_manufacturers": op_df.head(10)['manufacturer_name'].tolist()
        },
        "prescriptions": {
            "unique_drug_brands": len(rx_df),
            "total_drug_cost": float(rx_df['total_cost'].sum()),
            "total_fills": int(rx_df['total_fills'].sum()),
            "top_10_drugs": rx_df.head(10)['drug_brand'].tolist()
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

    # Initialize BigQuery client - will use gcloud default credentials
    try:
        # First try with project specification
        client = bigquery.Client(project="data-analytics-389803")
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        logger.info("Please ensure you have authenticated with: gcloud auth application-default login")
        raise

    # Set output directory
    output_dir = Path(__file__).parent.parent / "data" / "input"

    try:
        # Extract manufacturers from Open Payments
        logger.info("\n1. Extracting Open Payments manufacturers...")
        op_manufacturers = extract_op_manufacturers(client)

        # Extract drug/brand names from Prescriptions
        logger.info("\n2. Extracting Prescription drug brands...")
        rx_drugs = extract_rx_manufacturers(client)

        # Save extracted data
        logger.info("\n3. Saving extracted data...")
        summary = save_extracted_data(op_manufacturers, rx_drugs, output_dir)

        # Print summary
        logger.info("\n" + "="*60)
        logger.info("EXTRACTION COMPLETE")
        logger.info("="*60)
        logger.info(f"Open Payments: {summary['open_payments']['unique_manufacturers']} manufacturers")
        logger.info(f"  Total payments: ${summary['open_payments']['total_payments']:,.2f}")
        logger.info(f"  Top manufacturers: {', '.join(summary['open_payments']['top_10_manufacturers'][:3])}...")
        logger.info(f"\nPrescriptions: {summary['prescriptions']['unique_drug_brands']} drug brands")
        logger.info(f"  Total drug cost: ${summary['prescriptions']['total_drug_cost']:,.2f}")
        logger.info(f"  Top drugs: {', '.join(summary['prescriptions']['top_10_drugs'][:3])}...")
        logger.info("\n" + "="*60)
        logger.info("IMPORTANT NOTES:")
        logger.info("1. Open Payments contains MANUFACTURER names")
        logger.info("2. Prescriptions contains DRUG/BRAND names (not manufacturer names)")
        logger.info("3. Next step: Map drug brands to their manufacturers")
        logger.info("4. Consider using FDA Orange Book or NDC database for mapping")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise


if __name__ == "__main__":
    main()