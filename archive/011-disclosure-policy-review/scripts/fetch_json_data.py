#!/usr/bin/env python3
"""
Fetch real disclosure data from BigQuery JSON table for the UI viewer
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

def load_member_data():
    """Load member data from local file"""
    member_file = DATA_DIR / "raw" / "disclosures" / "member_shards_2025-08-09.parquet"
    if member_file.exists():
        print("Loading member data from local file...")
        return pd.read_parquet(member_file)
    else:
        print("Warning: Member data file not found. Job titles and entities will be missing.")
        return pd.DataFrame()

def fetch_disclosure_data():
    """Fetch real disclosure data from BigQuery JSON table"""
    
    print("Fetching real disclosure data from BigQuery JSON table...")
    print("-" * 50)
    
    try:
        # Load credentials from environment variable
        service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        
        # Initialize BigQuery client
        project_id = 'conflixis-engine'
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        # Query the JSON table
        group_id = 'gcO9AHYlNSzFeGTRSFRa'
        campaign_id = 'qyH2ggzVV0WLkuRfem7S'  # 2025 Texas Health COI Survey
        
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
                
                -- Will be joined from member_shards
                '' as provider_npi_placeholder,
                
                -- Parse disclosure type and category
                -- For Open Payments imports without a type, use "Open Payments Import"
                COALESCE(
                    NULLIF(JSON_VALUE(data, '$.question.title'), ''),
                    CASE 
                        WHEN JSON_VALUE(data, '$.question.category_label') IS NULL THEN 'Open Payments Import'
                        ELSE 'Not Specified'
                    END
                ) as relationship_type,
                -- Fix category names to match standard format (& instead of "and")
                -- Also handle Open Payments imports that have no category
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
                
                -- Parse person_id
                JSON_VALUE(data, '$.person_id') as person_id,
                
                -- Parse manager
                JSON_VALUE(data, '$.manager') as manager_name,
                
                -- Get the full question fields for entity extraction
                JSON_QUERY(data, '$.question.field_values') as field_values_json,
                JSON_QUERY(data, '$.question.fields') as fields_json,
                
                -- Keep raw data for reference
                data as raw_json
                
            FROM `conflixis-engine.firestore_export.disclosures_raw_latest`
            WHERE JSON_VALUE(data, '$.group_id') = '{group_id}'
                AND JSON_VALUE(data, '$.campaign_id') = '{campaign_id}'
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
            '' as provider_npi,  -- Will be added from member data join
            provider_name,
            provider_email,
            reporter_user_id,
            
            -- Job title will be added from member data  
            'Not Specified' as job_title,
            
            entity_name,
            relationship_type,
            category_label,
            
            -- Financial information
            COALESCE(financial_amount, 0) as financial_amount,
            
            -- Status
            review_status,
            
            -- Dates
            CAST(timestamp AS STRING) as disclosure_date,
            COALESCE(relationship_start_date, '') as relationship_start_date,
            NOT is_relationship_concluded as relationship_ongoing,
            
            -- Additional fields
            
            -- Person with interest
            CASE
                WHEN compensation_received_by IS NOT NULL AND compensation_received_by != ''
                THEN compensation_received_by
                ELSE provider_name
            END as person_with_interest,
            
            -- Notes
            COALESCE(notes, '') as notes,
            
            -- Research flag
            COALESCE(is_research, FALSE) as is_research,
            
            -- Metadata
            timestamp as created_at,
            timestamp as updated_at,
            '{group_id}' as group_id,
            '{campaign_id}' as campaign_id,
            manager_name,
            
            -- Additional Related Parties fields
            related_job_title,
            related_entity_location,
            
            -- Keep document ID visible
            id as document_id
            
        FROM enriched_disclosures
        ORDER BY timestamp DESC
        """
        
        print(f"Querying data for group_id: {group_id}")
        
        # Execute query
        query_job = client.query(query)
        df = query_job.to_dataframe()
        
        print(f"Retrieved {len(df)} records")
        
        # Load and join member data
        member_df = load_member_data()
        if not member_df.empty:
            # Prepare member data for joining
            member_df['normalized_name_member'] = member_df['normalized_name'].astype(str)
            
            # Normalize provider names in disclosure data
            df['normalized_provider_name'] = df['provider_name'].str.lower().str.strip()
            
            # Join with member data
            df = df.merge(
                member_df[['normalized_name_member', 'npi', 'job_title', 'entity', 'manager_name']].drop_duplicates('normalized_name_member'),
                left_on='normalized_provider_name',
                right_on='normalized_name_member',
                how='left',
                suffixes=('', '_member')
            )
            
            # Update fields with member data
            df['provider_npi'] = df['npi'].fillna('')
            df['job_title'] = df['job_title_member'].fillna(df['job_title'])
            df['department'] = df['entity'].fillna('Texas Health')
            df['manager_name'] = df['manager_name_member'].fillna(df['manager_name'])
            
            # Drop temporary columns
            df = df.drop(columns=['normalized_provider_name', 'normalized_name_member', 'npi', 
                                 'job_title_member', 'entity', 'manager_name_member'], errors='ignore')
            
            print(f"Joined with member data: {(df['provider_npi'] != '').sum()} records matched")
        
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
            'risk_score': 0,
            'entity_name': 'Not Disclosed',
            'notes': ''
        })
        
        # Convert boolean columns
        bool_columns = ['relationship_ongoing', 'is_research']
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
        
        # Sample some specific providers
        print("\n=== Sample Records ===")
        sample_providers = ['Audrey Puentes', 'Zachary Sypert', 'Shawn Parsley']
        for provider in sample_providers:
            provider_records = df[df['provider_name'] == provider]
            if len(provider_records) > 0:
                print(f"\n{provider}: {len(provider_records)} disclosure(s)")
                for _, record in provider_records.iterrows():
                    print(f"  - {record['relationship_type']}: {record['entity_name']}")
                    print(f"    Amount: ${record['financial_amount']:,.2f}")
                    print(f"    Doc ID: {record['document_id']}")
        
        # Save to staging directory as JSON for UI
        json_data = {
            'metadata': {
                'generated': datetime.now().isoformat(),
                'record_count': len(df),
                'data_range': f"{df['disclosure_date'].min()} to {df['disclosure_date'].max()}",
                'risk_distribution': stats['risk_distribution'],
                'group_id': group_id,
                'source': 'BigQuery: disclosures_raw_latest (JSON)'
            },
            'disclosures': df.drop(['group_id', 'campaign_id', 'related_job_title', 'related_entity_location'], axis=1, errors='ignore').to_dict('records')
        }
        
        json_path = STAGING_DIR / 'disclosure_data.json'
        with open(json_path, 'w') as f:
            json.dump(json_data, f, indent=2, default=str)
        
        print(f"\n✓ Data saved to: {json_path}")
        
        # Also save as CSV for reference
        csv_path = DATA_DIR / 'raw' / 'disclosures' / f'json_disclosures_{datetime.now().strftime("%Y-%m-%d")}.csv'
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path, index=False)
        print(f"✓ CSV backup saved to: {csv_path}")
        
        # Save as Parquet for performance
        parquet_path = STAGING_DIR / f'disclosures_json_{datetime.now().strftime("%Y-%m-%d")}.parquet'
        df.to_parquet(parquet_path, engine='pyarrow', compression='snappy')
        print(f"✓ Parquet saved to: {parquet_path}")
        
        print(f"\n📊 Open the viewer to see the data:")
        print(f"   Open index.html in the browser")
        
        return df
        
    except Exception as e:
        print(f"\n❌ Error fetching data: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main execution function"""
    print("=" * 50)
    print("FETCHING REAL DISCLOSURE DATA FROM JSON TABLE")
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