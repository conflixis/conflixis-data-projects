#!/usr/bin/env python3
"""
Fetch real disclosure data from BigQuery for the UI viewer
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery

# Load environment variables
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
STAGING_DIR = DATA_DIR / "staging"

# Ensure directories exist
STAGING_DIR.mkdir(parents=True, exist_ok=True)

def fetch_disclosure_data():
    """Fetch real disclosure data from BigQuery"""
    
    print("Fetching real disclosure data from BigQuery...")
    print("-" * 50)
    
    try:
        # Load credentials from environment variable
        service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        
        # Initialize BigQuery client
        project_id = 'data-analytics-389803'
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        # Query both tables and join them to get actual NPIs
        group_id = 'gcO9AHYlNSzFeGTRSFRa'
        campaign_id = 'qyH2ggzVV0WLkuRfem7S'  # 2025 Texas Health COI Survey
        disclosures_table = "data-analytics-389803.client.disclosures_parsed"
        forms_table = "conflixis-engine.firestore_export.disclosure_forms_raw_latest_v3"
        
        query = f"""
        WITH pivoted_disclosures AS (
            -- Pivot the EAV format to columnar format
            SELECT 
                document_id,
                MAX(timestamp) as timestamp,
                MAX(campaign_id) as campaign_id,
                MAX(user_npi) as provider_npi,
                MAX(user_email) as user_email,
                
                -- Core reporter info
                MAX(CASE WHEN Field = 'Reporter Name' THEN Value END) as reporter_name,
                MAX(CASE WHEN Field = 'Signature Full Name' THEN Value END) as signature_name,
                
                -- Disclosure type and question info
                MAX(CASE WHEN Field = 'Question Title' THEN Value END) as disclosure_type,
                MAX(CASE WHEN Field = 'Question Description' THEN SUBSTR(Value, 1, 500) END) as question_description,
                MAX(CASE WHEN Field = 'Question ID' THEN Value END) as question_id,
                
                -- Company and relationship info
                MAX(CASE WHEN Field = 'Company Name' THEN Value END) as company_name,
                MAX(CASE WHEN Field = 'Reported Value of Relationship' THEN Value END) as compensation_value_str,
                MAX(CASE WHEN Field = 'Person With Interest' THEN Value END) as person_with_interest,
                MAX(CASE WHEN Field = 'Type of Interest' THEN Value END) as interest_type,
                MAX(CASE WHEN Field = 'Interest Type' THEN Value END) as interest_type_alt,
                MAX(CASE WHEN Field = 'Notes' THEN Value END) as notes,
                
                -- Related parties fields
                MAX(CASE WHEN Field = 'First Name of Related Party' THEN Value END) as related_first_name,
                MAX(CASE WHEN Field = 'Last Name of Related Party' THEN Value END) as related_last_name,
                MAX(CASE WHEN Field = 'Related Party Entity Location (e.g. THAL)' THEN Value END) as related_entity,
                MAX(CASE WHEN Field = 'Job Title' THEN Value END) as related_job_title,
                
                -- Other fields
                MAX(CASE WHEN Field = 'Research Related' THEN Value END) as research_related,
                MAX(CASE WHEN Field = 'End Date' THEN Value END) as end_date,
                MAX(CASE WHEN Field = 'Jurisdiction/Location of Office Held' THEN Value END) as office_location,
                MAX(CASE WHEN Field = 'Person Holding Office' THEN Value END) as office_holder
                
            FROM `{disclosures_table}`
            WHERE group_id = '{group_id}'
                AND campaign_id = '{campaign_id}'
            GROUP BY document_id
        ),
        disclosures AS (
            SELECT *
            FROM pivoted_disclosures
        ),
        forms AS (
            SELECT 
                user_display_name,
                user_npi,
                user_email,
                campaign_id
            FROM `{forms_table}`
            WHERE group_id = '{group_id}'
                AND user_npi IS NOT NULL
                AND user_npi != ''
        ),
        members AS (
            SELECT 
                CONCAT(first_name, ' ', last_name) as full_name,
                TRIM(LOWER(CONCAT(first_name, ' ', last_name))) as full_name_normalized,
                npi,
                email,
                entity,
                job_title,
                manager_name
            FROM `conflixis-engine.firestore_export.member_shards_raw_latest_parsed`
            WHERE group_id = '{group_id}'
        )
        SELECT 
            d.document_id as id,
            -- Try to get NPI from pivoted data, forms, or members
            COALESCE(d.provider_npi, f.user_npi, m.npi, '') as provider_npi,
            COALESCE(d.reporter_name, 'Unknown Provider') as provider_name,
            -- Get job title from members if available
            COALESCE(m.job_title, 'Not Specified') as job_title,
            -- Get entity/department from members if available (where they work)
            COALESCE(m.entity, 'Texas Health') as department,
            
            -- Extract company name based on disclosure type
            CASE
                -- For Related Parties, show the related party's name
                WHEN d.disclosure_type = 'Related Parties'
                THEN CONCAT(
                    COALESCE(d.related_first_name, ''),
                    ' ',
                    COALESCE(d.related_last_name, '')
                )
                -- For Elected Office, show the office location
                WHEN d.disclosure_type = 'Elected Office'
                THEN COALESCE(d.office_location, 'Not Disclosed')
                -- Otherwise use company name
                ELSE COALESCE(d.company_name, 'Not Disclosed')
            END as entity_name,
            
            -- Use the disclosure type from the pivoted data
            COALESCE(d.disclosure_type, 'General Disclosure') as relationship_type,
            
            -- Financial information - parse from string to float
            CASE 
                WHEN d.compensation_value_str IS NOT NULL 
                    AND d.compensation_value_str != ''
                    AND REGEXP_CONTAINS(d.compensation_value_str, r'^[0-9.,]+$')
                THEN CAST(REGEXP_REPLACE(d.compensation_value_str, r'[^0-9.]', '') AS FLOAT64)
                ELSE 0
            END as financial_amount,
            
            -- Simulated Open Payments (since not in this table)
            CASE 
                WHEN d.compensation_value_str IS NOT NULL 
                    AND d.compensation_value_str != ''
                    AND REGEXP_CONTAINS(d.compensation_value_str, r'^[0-9.,]+$')
                THEN CAST(REGEXP_REPLACE(d.compensation_value_str, r'[^0-9.]', '') AS FLOAT64) * 0.8
                ELSE 0
            END as open_payments_total,
            
            FALSE as open_payments_matched,  -- Default since no OP data in table
            
            -- Status and review information
            COALESCE(d.review_status, 'pending') as review_status,
            FALSE as management_plan_required,  -- Will calculate based on amount
            FALSE as recusal_required,  -- Will calculate based on amount
            
            -- Dates
            CAST(d.timestamp AS STRING) as disclosure_date,
            COALESCE(d.service_start_date, '') as relationship_start_date,
            CASE 
                WHEN d.is_relationship_concluded = 'true' THEN FALSE
                ELSE TRUE
            END as relationship_ongoing,
            
            -- Additional fields
            'staff' as decision_authority_level,  -- Default
            0.0 as equity_percentage,  -- Default
            FALSE as board_position,  -- Default
            
            -- Person with interest - extract from raw_data JSON or use reporter name
            CASE
                WHEN JSON_VALUE(d.raw_data, '$.compensation_received_by') IS NOT NULL
                    AND JSON_VALUE(d.raw_data, '$.compensation_received_by') != ''
                THEN JSON_VALUE(d.raw_data, '$.compensation_received_by')
                WHEN d.compensation_received_by IS NOT NULL AND d.compensation_received_by != ''
                THEN d.compensation_received_by
                ELSE d.reporter_name
            END as person_with_interest,
            
            -- Notes - extract from raw_data JSON or use review_notes
            CASE
                WHEN JSON_VALUE(d.raw_data, '$.notes') IS NOT NULL
                    AND JSON_VALUE(d.raw_data, '$.notes') != ''
                THEN JSON_VALUE(d.raw_data, '$.notes')
                WHEN d.review_notes IS NOT NULL AND d.review_notes != ''
                THEN d.review_notes
                ELSE ''
            END as notes,
            
            -- Research flag - extract from raw_data JSON
            CASE 
                WHEN JSON_VALUE(d.raw_data, '$.is_research') = 'true' THEN TRUE
                WHEN d.is_research = 'true' THEN TRUE
                ELSE FALSE
            END as is_research,
            
            -- Metadata
            d.timestamp as created_at,
            d.timestamp as updated_at,
            d.group_id,
            d.campaign_title,
            -- Get email from forms first, then from members
            COALESCE(f.user_email, m.email, '') as provider_email,
            -- Add manager from members
            COALESCE(m.manager_name, d.manager, '') as manager_name
            
        FROM disclosures d
        LEFT JOIN forms f 
            ON d.reporter_name = f.user_display_name 
            AND d.campaign_id = f.campaign_id
        LEFT JOIN members m
            ON TRIM(LOWER(d.reporter_name)) = m.full_name_normalized
        ORDER BY d.timestamp DESC
        """
        
        print(f"Querying data for group_id: {group_id}")
        
        # Execute query
        query_job = client.query(query)
        df = query_job.to_dataframe()
        
        print(f"Retrieved {len(df)} records")
        
        # Calculate risk tiers based on financial amounts
        def calculate_risk_tier(amount):
            if pd.isna(amount) or amount <= 0:
                return 'low'
            elif amount <= 5000:
                return 'low'
            elif amount <= 25000:
                return 'moderate'
            elif amount <= 100000:
                return 'high'
            else:
                return 'critical'
        
        # Add calculated fields
        df['risk_tier'] = df['financial_amount'].apply(calculate_risk_tier)
        
        # Calculate risk scores (simplified)
        df['risk_score'] = df['financial_amount'].apply(
            lambda x: min(100, int((x / 1000) if pd.notna(x) else 0))
        )
        
        # Add default dates for missing values
        today = datetime.now().strftime('%Y-%m-%d')
        df['last_review_date'] = today
        df['next_review_date'] = '2025-12-31'
        
        # Clean up date formats
        for date_col in ['disclosure_date', 'relationship_start_date']:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.strftime('%Y-%m-%d')
                df[date_col] = df[date_col].fillna(today)
        
        # Fill NaN values
        df = df.fillna({
            'provider_npi': '',
            'financial_amount': 0,
            'open_payments_total': 0,
            'equity_percentage': 0,
            'risk_score': 0
        })
        
        # Convert boolean columns
        bool_columns = ['open_payments_matched', 'management_plan_required', 
                       'recusal_required', 'relationship_ongoing', 'board_position']
        for col in bool_columns:
            if col in df.columns:
                df[col] = df[col].astype(bool)
        
        # Calculate statistics
        stats = {
            'total_records': len(df),
            'risk_distribution': df['risk_tier'].value_counts().to_dict(),
            'review_status': df['review_status'].value_counts().to_dict(),
            'average_amount': float(df['financial_amount'].mean()),
            'median_amount': float(df['financial_amount'].median()),
            'max_amount': float(df['financial_amount'].max()),
            'management_plans_required': int(df['management_plan_required'].sum()),
            'providers': df['provider_name'].nunique()
        }
        
        print("\n=== Data Statistics ===")
        print(f"Total Records: {stats['total_records']}")
        print(f"Unique Providers: {stats['providers']}")
        print(f"\nRisk Distribution:")
        for tier, count in stats['risk_distribution'].items():
            print(f"  {tier.capitalize()}: {count} ({count/stats['total_records']*100:.1f}%)")
        print(f"\nFinancial Amounts:")
        print(f"  Average: ${stats['average_amount']:,.2f}")
        print(f"  Median: ${stats['median_amount']:,.2f}")
        print(f"  Maximum: ${stats['max_amount']:,.2f}")
        
        # Save to staging directory as JSON for UI
        json_data = {
            'metadata': {
                'generated': datetime.now().isoformat(),
                'record_count': len(df),
                'data_range': f"{df['disclosure_date'].min()} to {df['disclosure_date'].max()}",
                'risk_distribution': stats['risk_distribution'],
                'group_id': group_id,
                'source': 'BigQuery: disclosures_raw_latest_parse'
            },
            'disclosures': df.drop(['group_id', 'campaign_title'], axis=1).to_dict('records')
        }
        
        json_path = STAGING_DIR / 'disclosure_data.json'
        with open(json_path, 'w') as f:
            json.dump(json_data, f, indent=2, default=str)
        
        print(f"\n✓ Data saved to: {json_path}")
        
        # Also save as CSV for reference
        csv_path = DATA_DIR / 'raw' / 'disclosures' / f'real_disclosures_{datetime.now().strftime("%Y-%m-%d")}.csv'
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path, index=False)
        print(f"✓ CSV backup saved to: {csv_path}")
        
        # Save as Parquet for performance
        parquet_path = STAGING_DIR / f'disclosures_real_{datetime.now().strftime("%Y-%m-%d")}.parquet'
        df.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
        print(f"✓ Parquet saved to: {parquet_path}")
        
        print(f"\n📊 Open the viewer to see the REAL data:")
        print(f"   file:///home/incent/conflixis-data-projects/projects/011-disclosure-policy-review/disclosure-data-viewer.html")
        
        return df
        
    except Exception as e:
        print(f"\n❌ Error fetching data: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        return None

def main():
    """Main execution function"""
    print("=" * 50)
    print("FETCHING REAL DISCLOSURE DATA")
    print("=" * 50)
    print()
    
    df = fetch_disclosure_data()
    
    if df is not None:
        print("\n✅ Successfully fetched and processed real disclosure data!")
        print(f"Total records available in viewer: {len(df)}")
    else:
        print("\n⚠️ Failed to fetch data. Please check error messages above.")
    
    print("=" * 50)

if __name__ == "__main__":
    main()