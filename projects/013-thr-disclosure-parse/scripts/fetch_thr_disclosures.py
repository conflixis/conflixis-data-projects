#!/usr/bin/env python3
"""
Fetch and parse THR disclosure data from BigQuery
DA-172: THR Disclosure Parse - Main Script
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
from tqdm import tqdm
import config
from fetch_member_data import fetch_member_data

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


def fetch_disclosure_data():
    """
    Fetch THR disclosure data from BigQuery JSON table.
    Parses JSON fields and extracts all relevant information.
    """
    
    logger.info("Fetching disclosure data from BigQuery...")
    
    try:
        # Load credentials from environment variable
        service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        
        # Initialize BigQuery client
        client = bigquery.Client(credentials=credentials, project=config.PROJECT_ID)
        
        # Query to parse disclosure data
        query = f"""
        WITH parsed_disclosures AS (
            SELECT 
                document_id as id,
                timestamp,
                
                -- Parse reporter information
                JSON_VALUE(data, '$.reporter.name') as provider_name,
                JSON_VALUE(data, '$.reporter.email') as provider_email,
                JSON_VALUE(data, '$.reporter.id') as reporter_user_id,
                JSON_VALUE(data, '$.reporter.authed_user_id') as authed_user_id,
                
                -- Parse disclosure type and category
                COALESCE(
                    NULLIF(JSON_VALUE(data, '$.question.title'), ''),
                    CASE 
                        WHEN JSON_VALUE(data, '$.question.category_label') IS NULL THEN 'Open Payments Import'
                        ELSE 'Not Specified'
                    END
                ) as relationship_type,
                
                -- Fix category names to match standard format
                CASE
                    WHEN JSON_VALUE(data, '$.question.category_label') IS NULL THEN 'Open Payments (CMS Imports)'
                    WHEN JSON_VALUE(data, '$.question.category_label') = 'External Roles and Relationships' THEN 'External Roles & Relationships'
                    WHEN JSON_VALUE(data, '$.question.category_label') = 'Financial and Investment Interests' THEN 'Financial & Investment Interests'
                    ELSE JSON_VALUE(data, '$.question.category_label')
                END as category_label,
                JSON_VALUE(data, '$.question.description') as question_description,
                
                -- Parse company/entity information
                JSON_VALUE(data, '$.company_name') as company_name,
                JSON_VALUE(data, '$.company_id') as company_id,
                
                -- Parse financial information
                CAST(JSON_VALUE(data, '$.compensation_value') AS FLOAT64) as financial_amount,
                JSON_VALUE(data, '$.compensation_type') as compensation_type,
                JSON_VALUE(data, '$.compensation_received_by') as compensation_received_by,
                CAST(JSON_VALUE(data, '$.compensation_received_by_self') AS BOOL) as compensation_received_by_self,
                
                -- Parse status and review information
                JSON_VALUE(data, '$.status') as status,
                JSON_VALUE(data, '$.review_status') as review_status,
                JSON_VALUE(data, '$.reviewer') as reviewer,
                JSON_VALUE(data, '$.review_date') as review_date,
                
                -- Parse dates
                JSON_VALUE(data, '$.service_start_date') as relationship_start_date,
                JSON_VALUE(data, '$.service_end_date') as relationship_end_date,
                CAST(JSON_VALUE(data, '$.is_relationship_concluded') AS BOOL) as is_relationship_concluded,
                
                -- Parse research and dispute flags
                CAST(JSON_VALUE(data, '$.is_research') AS BOOL) as is_research,
                CAST(JSON_VALUE(data, '$.disputed') AS BOOL) as disputed,
                
                -- Parse notes
                JSON_VALUE(data, '$.notes') as notes,
                
                -- Parse signature
                JSON_VALUE(data, '$.signature.full_name') as signature_name,
                JSON_VALUE(data, '$.signature.initials') as signature_initials,
                
                -- Parse person_id and manager
                JSON_VALUE(data, '$.person_id') as person_id,
                JSON_VALUE(data, '$.manager') as manager_name,
                
                -- Get the full question fields for entity extraction
                JSON_QUERY(data, '$.question.field_values') as field_values_json,
                JSON_QUERY(data, '$.question.fields') as fields_json
                
            FROM `{config.PROJECT_ID}.{config.DISCLOSURES_TABLE}`
            WHERE JSON_VALUE(data, '$.group_id') = '{config.GROUP_ID}'
                AND JSON_VALUE(data, '$.campaign_id') = '{config.CAMPAIGN_ID}'
        ),
        enriched_disclosures AS (
            SELECT 
                *,
                -- Extract entity name based on disclosure type
                CASE
                    -- For Related Parties, extract first and last name from field_values
                    WHEN relationship_type = 'Related Parties' THEN
                        CONCAT(
                            IFNULL(JSON_VALUE(field_values_json, '$[0].value'), ''),
                            ' ',
                            IFNULL(JSON_VALUE(field_values_json, '$[1].value'), '')
                        )
                    -- For other types, use company name
                    ELSE COALESCE(company_name, 'Not Disclosed')
                END as entity_name,
                
                -- Extract job title for Related Parties
                CASE
                    WHEN relationship_type = 'Related Parties' THEN
                        JSON_VALUE(field_values_json, '$[3].value')
                    ELSE NULL
                END as related_job_title,
                
                -- Extract entity location for Related Parties
                CASE
                    WHEN relationship_type = 'Related Parties' THEN
                        JSON_VALUE(field_values_json, '$[2].value')
                    ELSE NULL
                END as related_entity_location
                
            FROM parsed_disclosures
        )
        SELECT 
            id,
            provider_name,
            provider_email,
            reporter_user_id,
            entity_name,
            relationship_type,
            category_label,
            COALESCE(financial_amount, 0) as financial_amount,
            compensation_type,
            compensation_received_by,
            compensation_received_by_self,
            status,
            review_status,
            reviewer,
            review_date,
            CAST(timestamp AS STRING) as disclosure_date,
            relationship_start_date,
            relationship_end_date,
            NOT is_relationship_concluded as relationship_ongoing,
            is_research,
            disputed,
            notes,
            signature_name,
            person_id,
            manager_name,
            related_job_title,
            related_entity_location,
            timestamp as created_at
        FROM enriched_disclosures
        ORDER BY timestamp DESC
        """
        
        logger.info(f"Querying data for group_id: {config.GROUP_ID}, campaign_id: {config.CAMPAIGN_ID}")
        
        # Execute query
        query_job = client.query(query)
        df = query_job.to_dataframe()
        
        logger.info(f"Retrieved {len(df)} disclosure records")
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching disclosure data: {e}")
        import traceback
        traceback.print_exc()
        return None


def join_with_member_data(disclosure_df, member_df):
    """
    Join disclosure data with member data to enrich with NPI, job titles, etc.
    """
    
    if member_df is None or member_df.empty:
        logger.warning("No member data available for joining")
        return disclosure_df
    
    logger.info("Joining disclosure data with member data...")
    
    # Normalize provider names in disclosure data
    disclosure_df['normalized_provider_name'] = disclosure_df['provider_name'].str.lower().str.strip()
    
    # Ensure member data has normalized names
    if 'normalized_name' not in member_df.columns:
        member_df['normalized_name'] = member_df['full_name'].str.lower().str.strip()
    
    # Join with member data
    enriched_df = disclosure_df.merge(
        member_df[['normalized_name', 'npi', 'job_title', 'entity', 'manager_name']].drop_duplicates('normalized_name'),
        left_on='normalized_provider_name',
        right_on='normalized_name',
        how='left',
        suffixes=('', '_member')
    )
    
    # Update fields with member data
    enriched_df['provider_npi'] = enriched_df['npi'].fillna('')
    enriched_df['job_title'] = enriched_df['job_title'].fillna('Not Specified')
    enriched_df['department'] = enriched_df['entity'].fillna('Texas Health')
    
    # Use member manager if disclosure manager is empty
    enriched_df['manager_name'] = enriched_df['manager_name'].fillna(enriched_df['manager_name_member'])
    
    # Drop temporary columns
    columns_to_drop = ['normalized_provider_name', 'normalized_name', 'npi', 
                      'entity', 'manager_name_member']
    enriched_df = enriched_df.drop(columns=[col for col in columns_to_drop if col in enriched_df.columns])
    
    logger.info(f"Joined with member data: {(enriched_df['provider_npi'] != '').sum()} records matched")
    
    return enriched_df


def calculate_risk_metrics(df):
    """
    Calculate risk tiers and scores based on financial amounts.
    """
    
    logger.info("Calculating risk metrics...")
    
    # Calculate risk tier
    df['risk_tier'] = df['financial_amount'].apply(config.get_risk_tier)
    
    # Calculate risk score (0-100 scale)
    df['risk_score'] = df['financial_amount'].apply(
        lambda x: min(100, int((x / 1000) if pd.notna(x) and x > 0 else 0))
    )
    
    # Add review dates
    today = datetime.now().strftime(config.DATE_FORMAT)
    df['last_review_date'] = df['review_date'].fillna(today)
    df['next_review_date'] = '2025-12-31'  # End of year review
    
    return df


def clean_and_format_data(df):
    """
    Clean and format the data for export.
    """
    
    logger.info("Cleaning and formatting data...")
    
    # Clean up date formats
    date_columns = ['disclosure_date', 'relationship_start_date', 'relationship_end_date', 
                   'review_date', 'last_review_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime(config.DATE_FORMAT)
    
    # Fill NaN values
    df = df.fillna({
        'provider_npi': '',
        'financial_amount': 0,
        'risk_score': 0,
        'entity_name': 'Not Disclosed',
        'notes': '',
        'compensation_type': '',
        'review_status': 'Pending',
        'job_title': 'Not Specified',
        'department': 'Texas Health'
    })
    
    # Convert boolean columns
    bool_columns = ['relationship_ongoing', 'is_research', 'disputed', 'compensation_received_by_self']
    for col in bool_columns:
        if col in df.columns:
            df[col] = df[col].astype(bool)
    
    # Order columns for readability
    column_order = [
        'id', 'provider_name', 'provider_npi', 'provider_email', 'job_title', 'department',
        'manager_name', 'entity_name', 'relationship_type', 'category_label',
        'financial_amount', 'compensation_type', 'compensation_received_by',
        'compensation_received_by_self', 'risk_tier', 'risk_score',
        'disclosure_date', 'relationship_start_date', 'relationship_end_date',
        'relationship_ongoing', 'status', 'review_status', 'reviewer',
        'last_review_date', 'next_review_date', 'is_research', 'disputed',
        'notes', 'signature_name', 'person_id', 'created_at'
    ]
    
    # Reorder columns (only include columns that exist)
    available_columns = [col for col in column_order if col in df.columns]
    df = df[available_columns]
    
    return df


def export_data(df):
    """
    Export the data to multiple formats.
    """
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Export to CSV (primary format)
    csv_path = config.OUTPUT_DIR / f"thr_disclosures_{timestamp}.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"✓ CSV exported to: {csv_path}")
    
    # Export to Parquet for performance
    if 'parquet' in config.EXPORT_FORMATS:
        parquet_path = config.OUTPUT_DIR / f"thr_disclosures_{timestamp}.parquet"
        df.to_parquet(parquet_path, index=False, engine='pyarrow', compression='snappy')
        logger.info(f"✓ Parquet exported to: {parquet_path}")
    
    # Export to JSON for web consumption
    if 'json' in config.EXPORT_FORMATS:
        json_data = {
            'metadata': {
                'generated': datetime.now().isoformat(),
                'record_count': len(df),
                'group_id': config.GROUP_ID,
                'campaign_id': config.CAMPAIGN_ID,
                'risk_distribution': df['risk_tier'].value_counts().to_dict(),
                'total_financial_amount': float(df['financial_amount'].sum()),
                'average_financial_amount': float(df['financial_amount'].mean())
            },
            'disclosures': df.to_dict('records')
        }
        
        json_path = config.OUTPUT_DIR / f"thr_disclosures_{timestamp}.json"
        with open(json_path, 'w') as f:
            json.dump(json_data, f, indent=2, default=str)
        logger.info(f"✓ JSON exported to: {json_path}")
    
    return csv_path


def print_statistics(df):
    """
    Print summary statistics about the data.
    """
    
    logger.info("\n" + "=" * 50)
    logger.info("DATA STATISTICS")
    logger.info("=" * 50)
    
    stats = {
        'Total Records': len(df),
        'Unique Providers': df['provider_name'].nunique(),
        'Providers with NPI': (df['provider_npi'] != '').sum(),
        'Total Financial Amount': f"${df['financial_amount'].sum():,.2f}",
        'Average Amount': f"${df['financial_amount'].mean():,.2f}",
        'Median Amount': f"${df['financial_amount'].median():,.2f}",
        'Max Amount': f"${df['financial_amount'].max():,.2f}"
    }
    
    for key, value in stats.items():
        logger.info(f"{key}: {value}")
    
    # Risk distribution
    logger.info("\nRisk Distribution:")
    risk_counts = df['risk_tier'].value_counts()
    for tier in ['none', 'low', 'moderate', 'high', 'critical']:
        if tier in risk_counts.index:
            count = risk_counts[tier]
            pct = count / len(df) * 100
            logger.info(f"  {tier.capitalize()}: {count} ({pct:.1f}%)")
    
    # Category distribution
    logger.info("\nCategory Distribution:")
    for category, count in df['category_label'].value_counts().head(5).items():
        logger.info(f"  {category}: {count}")
    
    # Sample records
    logger.info("\nSample Records:")
    sample = df.nlargest(5, 'financial_amount')[['provider_name', 'entity_name', 'financial_amount', 'risk_tier']]
    for _, row in sample.iterrows():
        logger.info(f"  {row['provider_name']} - {row['entity_name']}: ${row['financial_amount']:,.2f} ({row['risk_tier']})")


def main():
    """
    Main execution function.
    """
    
    logger.info("=" * 50)
    logger.info("THR DISCLOSURE DATA EXTRACTION")
    logger.info("=" * 50)
    logger.info(f"Group ID: {config.GROUP_ID}")
    logger.info(f"Campaign ID: {config.CAMPAIGN_ID}")
    
    try:
        # Step 1: Fetch member data
        logger.info("\nStep 1: Fetching member data...")
        member_df = fetch_member_data()
        
        # Step 2: Fetch disclosure data
        logger.info("\nStep 2: Fetching disclosure data...")
        disclosure_df = fetch_disclosure_data()
        
        if disclosure_df is None or disclosure_df.empty:
            logger.error("No disclosure data retrieved")
            return False
        
        # Step 3: Join with member data
        logger.info("\nStep 3: Joining with member data...")
        enriched_df = join_with_member_data(disclosure_df, member_df)
        
        # Step 4: Calculate risk metrics
        logger.info("\nStep 4: Calculating risk metrics...")
        enriched_df = calculate_risk_metrics(enriched_df)
        
        # Step 5: Clean and format
        logger.info("\nStep 5: Cleaning and formatting data...")
        final_df = clean_and_format_data(enriched_df)
        
        # Step 6: Export data
        logger.info("\nStep 6: Exporting data...")
        output_path = export_data(final_df)
        
        # Step 7: Print statistics
        print_statistics(final_df)
        
        logger.info("\n" + "=" * 50)
        logger.info("✅ EXTRACTION COMPLETED SUCCESSFULLY")
        logger.info(f"Output saved to: {output_path}")
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f"Fatal error during extraction: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)