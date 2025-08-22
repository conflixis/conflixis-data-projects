#!/usr/bin/env python3
"""
Fetch and parse Open Payments transaction-level data from THR disclosures
DA-172: THR Disclosure Parse - Open Payments Transactions Script
"""

import json
import os
import sys
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from google.oauth2 import service_account
from google.cloud import bigquery
from dotenv import load_dotenv
import config

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def fetch_op_transactions():
    """
    Fetch Open Payments transaction-level data from BigQuery.
    Returns one row per transaction instead of one row per disclosure.
    """
    
    logger.info("Fetching Open Payments transaction data from BigQuery...")
    
    try:
        # Load credentials from environment variable
        service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        
        # Initialize BigQuery client
        client = bigquery.Client(credentials=credentials, project=config.PROJECT_ID)
        
        # Query to extract individual transactions
        query = f"""
        WITH op_disclosures AS (
            SELECT 
                document_id,
                JSON_VALUE(data, '$.reporter.name') as reporter_name,
                JSON_VALUE(data, '$.reporter.email') as reporter_email,
                JSON_VALUE(data, '$.reporter.id') as reporter_id,
                JSON_VALUE(data, '$.company_name') as company_name,
                JSON_VALUE(data, '$.company_id') as submitting_entity_id,
                JSON_VALUE(data, '$.disclosure_timeframe_start_date') as disclosure_start,
                JSON_VALUE(data, '$.disclosure_timeframe_end_date') as disclosure_end,
                JSON_VALUE(data, '$.signature.full_name') as signature_name,
                JSON_VALUE(data, '$.signature_date._seconds') as signature_date_seconds,
                JSON_VALUE(data, '$.created_at._seconds') as created_at_seconds,
                JSON_EXTRACT_ARRAY(data, '$.question.disclosures[0].transactions') as transactions
            FROM `{config.PROJECT_ID}.{config.DISCLOSURES_TABLE}`
            WHERE JSON_VALUE(data, '$.group_id') = '{config.GROUP_ID}'
                AND JSON_VALUE(data, '$.campaign_id') = '{config.CAMPAIGN_ID}'
                AND JSON_VALUE(data, '$.question.category_label') IS NULL  -- Open Payments only
        ),
        transactions_unnested AS (
            SELECT
                document_id,
                reporter_name,
                reporter_email,
                reporter_id,
                company_name,
                submitting_entity_id,
                disclosure_start,
                disclosure_end,
                signature_name,
                signature_date_seconds,
                created_at_seconds,
                JSON_VALUE(transaction, '$.record_id') as record_id,
                JSON_VALUE(transaction, '$.recipient_npi') as recipient_npi,
                JSON_VALUE(transaction, '$.payment_date') as payment_date,
                CAST(JSON_VALUE(transaction, '$.payment_total_usd') AS FLOAT64) as payment_amount,
                JSON_VALUE(transaction, '$.payment_nature') as payment_nature,
                JSON_VALUE(transaction, '$.payment_form') as payment_form,
                JSON_VALUE(transaction, '$.payment_count') as payment_count,
                JSON_VALUE(transaction, '$.program_year') as program_year,
                JSON_VALUE(transaction, '$.payment_publication_date') as payment_publication_date,
                JSON_VALUE(transaction, '$.name_of_study') as name_of_study,
                JSON_VALUE(transaction, '$.source') as transaction_source
            FROM op_disclosures
            CROSS JOIN UNNEST(transactions) AS transaction
        )
        SELECT 
            document_id,
            reporter_name,
            reporter_email,
            reporter_id,
            recipient_npi,
            company_name,
            submitting_entity_id,
            record_id,
            payment_date,
            payment_amount,
            payment_nature,
            payment_form,
            CAST(payment_count AS INT64) as payment_count,
            program_year,
            payment_publication_date,
            name_of_study,
            transaction_source,
            disclosure_start,
            disclosure_end,
            signature_name,
            TIMESTAMP_SECONDS(CAST(signature_date_seconds AS INT64)) as signature_date,
            TIMESTAMP_SECONDS(CAST(created_at_seconds AS INT64)) as created_at
        FROM transactions_unnested
        ORDER BY reporter_name, payment_date
        """
        
        logger.info(f"Querying Open Payments transactions for group_id: {config.GROUP_ID}, campaign_id: {config.CAMPAIGN_ID}")
        
        # Execute query
        query_job = client.query(query)
        df = query_job.to_dataframe()
        
        logger.info(f"Retrieved {len(df)} transaction records from {df['document_id'].nunique()} disclosures")
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching Open Payments transaction data: {e}")
        import traceback
        traceback.print_exc()
        return None


def enrich_with_npi_data(df):
    """
    Enrich transaction data with NPI information from member data.
    """
    
    logger.info("Enriching with NPI data...")
    
    try:
        # Load member data to get NPIs
        from fetch_member_data import fetch_member_data
        member_df = fetch_member_data()
        
        if member_df is not None and not member_df.empty:
            # Normalize names for matching
            df['normalized_reporter'] = df['reporter_name'].str.lower().str.strip()
            member_df['normalized_name'] = member_df['full_name'].str.lower().str.strip()
            
            # Join to get provider NPIs
            df = df.merge(
                member_df[['normalized_name', 'npi', 'job_title', 'entity']].drop_duplicates('normalized_name'),
                left_on='normalized_reporter',
                right_on='normalized_name',
                how='left',
                suffixes=('', '_member')
            )
            
            # Use member NPI if recipient_npi is empty
            df['provider_npi'] = df['recipient_npi'].fillna(df['npi'])
            df['provider_job_title'] = df['job_title'].fillna('Not Specified')
            df['provider_entity'] = df['entity'].fillna('Texas Health')
            
            # Drop temporary columns
            df = df.drop(columns=['normalized_reporter', 'normalized_name', 'npi', 'job_title', 'entity'], errors='ignore')
            
            logger.info(f"Matched {(df['provider_npi'].notna()).sum()} transactions with NPI data")
        else:
            df['provider_npi'] = df['recipient_npi']
            df['provider_job_title'] = 'Not Specified'
            df['provider_entity'] = 'Texas Health'
    
    except Exception as e:
        logger.warning(f"Could not enrich with NPI data: {e}")
        df['provider_npi'] = df['recipient_npi']
        df['provider_job_title'] = 'Not Specified'
        df['provider_entity'] = 'Texas Health'
    
    return df


def clean_and_format_transactions(df):
    """
    Clean and format transaction data for export.
    """
    
    logger.info("Cleaning and formatting transaction data...")
    
    # Convert dates
    date_columns = ['payment_date', 'payment_publication_date', 'disclosure_start', 
                   'disclosure_end', 'signature_date', 'created_at']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            if col in ['signature_date', 'created_at']:
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                df[col] = df[col].dt.strftime('%Y-%m-%d')
    
    # Fill NaN values
    df = df.fillna({
        'provider_npi': '',
        'payment_amount': 0,
        'payment_count': 1,
        'name_of_study': '',
        'payment_nature': 'Not Specified',
        'payment_form': 'Not Specified',
        'program_year': '',
        'transaction_source': 'op-general'
    })
    
    # Order columns
    column_order = [
        'document_id', 'reporter_name', 'reporter_email', 'provider_npi',
        'provider_job_title', 'provider_entity', 'company_name', 
        'submitting_entity_id', 'record_id', 'payment_date', 'payment_amount',
        'payment_nature', 'payment_form', 'payment_count', 'program_year',
        'payment_publication_date', 'name_of_study', 'transaction_source',
        'disclosure_start', 'disclosure_end', 'signature_name', 
        'signature_date', 'created_at'
    ]
    
    # Reorder columns (only include columns that exist)
    available_columns = [col for col in column_order if col in df.columns]
    df = df[available_columns]
    
    return df


def export_transactions(df):
    """
    Export transaction data to CSV and other formats.
    """
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Export to CSV
    csv_path = config.OUTPUT_DIR / f"thr_op_transactions_{timestamp}.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"✓ Transaction CSV exported to: {csv_path}")
    
    # Export to Parquet for performance
    if 'parquet' in config.EXPORT_FORMATS:
        parquet_path = config.OUTPUT_DIR / f"thr_op_transactions_{timestamp}.parquet"
        df.to_parquet(parquet_path, index=False, engine='pyarrow', compression='snappy')
        logger.info(f"✓ Transaction Parquet exported to: {parquet_path}")
    
    return csv_path


def print_transaction_statistics(df):
    """
    Print summary statistics about the transaction data.
    """
    
    logger.info("\n" + "=" * 50)
    logger.info("OPEN PAYMENTS TRANSACTION STATISTICS")
    logger.info("=" * 50)
    
    stats = {
        'Total Transactions': len(df),
        'Unique Disclosures': df['document_id'].nunique(),
        'Unique Providers': df['reporter_name'].nunique(),
        'Unique Companies': df['company_name'].nunique(),
        'Total Payment Amount': f"${df['payment_amount'].sum():,.2f}",
        'Average Payment': f"${df['payment_amount'].mean():,.2f}",
        'Median Payment': f"${df['payment_amount'].median():,.2f}",
        'Max Payment': f"${df['payment_amount'].max():,.2f}"
    }
    
    for key, value in stats.items():
        logger.info(f"{key}: {value}")
    
    # Payment nature distribution
    logger.info("\nPayment Nature Distribution:")
    for nature, count in df['payment_nature'].value_counts().head(10).items():
        pct = count / len(df) * 100
        logger.info(f"  {nature}: {count} ({pct:.1f}%)")
    
    # Top companies by transaction count
    logger.info("\nTop Companies by Transaction Count:")
    for company, count in df['company_name'].value_counts().head(5).items():
        total = df[df['company_name'] == company]['payment_amount'].sum()
        logger.info(f"  {company}: {count} transactions (${total:,.2f})")
    
    # Top providers by total amount
    logger.info("\nTop Providers by Total Amount:")
    provider_totals = df.groupby('reporter_name')['payment_amount'].sum().sort_values(ascending=False).head(5)
    for provider, total in provider_totals.items():
        count = len(df[df['reporter_name'] == provider])
        logger.info(f"  {provider}: ${total:,.2f} ({count} transactions)")


def main():
    """
    Main execution function.
    """
    
    logger.info("=" * 50)
    logger.info("THR OPEN PAYMENTS TRANSACTION EXTRACTION")
    logger.info("=" * 50)
    logger.info(f"Group ID: {config.GROUP_ID}")
    logger.info(f"Campaign ID: {config.CAMPAIGN_ID}")
    
    try:
        # Step 1: Fetch transaction data
        logger.info("\nStep 1: Fetching Open Payments transaction data...")
        transaction_df = fetch_op_transactions()
        
        if transaction_df is None or transaction_df.empty:
            logger.warning("No Open Payments transaction data found")
            return True  # Not an error, just no OP data
        
        # Step 2: Enrich with NPI data
        logger.info("\nStep 2: Enriching with NPI data...")
        transaction_df = enrich_with_npi_data(transaction_df)
        
        # Step 3: Clean and format
        logger.info("\nStep 3: Cleaning and formatting data...")
        final_df = clean_and_format_transactions(transaction_df)
        
        # Step 4: Export data
        logger.info("\nStep 4: Exporting transaction data...")
        output_path = export_transactions(final_df)
        
        # Step 5: Print statistics
        print_transaction_statistics(final_df)
        
        logger.info("\n" + "=" * 50)
        logger.info("✅ TRANSACTION EXTRACTION COMPLETED SUCCESSFULLY")
        logger.info(f"Output saved to: {output_path}")
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f"Fatal error during transaction extraction: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)