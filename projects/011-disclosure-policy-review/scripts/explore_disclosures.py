#!/usr/bin/env python3
"""
Explore disclosure data and create sample review interface
"""

import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery

# Load environment variables
load_dotenv()

def get_bigquery_client():
    """Initialize BigQuery client"""
    service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return bigquery.Client(credentials=credentials, project='data-analytics-389803')

def explore_parsed_disclosures():
    """Explore the parsed disclosures table which has the main data"""
    client = get_bigquery_client()
    group_id = 'gcO9AHYlNSzFeGTRSFRa'
    
    print("\n" + "="*60)
    print("EXPLORING PARSED DISCLOSURES DATA")
    print("="*60)
    
    # Get sample disclosures with risk categorization
    query = f"""
    WITH disclosure_data AS (
        SELECT 
            document_id,
            campaign_id,
            name as provider_name,
            email,
            external_entity_name,
            relationship_category,
            nature_of_relationship,
            service_provided_description,
            CAST(service_value_of_compensation AS FLOAT64) as compensation_value,
            is_research_related,
            is_current_relationship,
            service_start_date,
            service_end_date,
            timestamp as disclosure_date
        FROM `conflixis-engine.firestore_export.disclosures_raw_latest_parse`
        WHERE group_id = '{group_id}'
            AND service_value_of_compensation IS NOT NULL
            AND service_value_of_compensation != ''
            AND SAFE_CAST(service_value_of_compensation AS FLOAT64) IS NOT NULL
    )
    SELECT 
        *,
        CASE 
            WHEN compensation_value < 5000 THEN 'Low Risk (<$5k)'
            WHEN compensation_value < 10000 THEN 'Tier 1 ($5k-$10k)'
            WHEN compensation_value < 50000 THEN 'Tier 2 ($10k-$50k)'
            ELSE 'Tier 3 (>$50k)'
        END as risk_tier,
        CASE 
            WHEN compensation_value >= 50000 THEN 'Executive Review Required'
            WHEN compensation_value >= 10000 AND is_research_related = 'Yes' THEN 'Management Plan Required'
            WHEN compensation_value >= 5000 THEN 'Standard Review'
            ELSE 'No Action Required'
        END as review_requirement
    FROM disclosure_data
    ORDER BY compensation_value DESC
    LIMIT 20
    """
    
    results = client.query(query).to_dataframe()
    
    print(f"\nFound {len(results)} sample disclosures")
    print("\nRisk Tier Distribution:")
    print(results['risk_tier'].value_counts())
    print("\nReview Requirements:")
    print(results['review_requirement'].value_counts())
    
    # Save sample data for interface
    output_dir = Path(__file__).parent.parent / 'data'
    output_dir.mkdir(exist_ok=True)
    
    # Convert to dict for JSON
    disclosures_dict = results.to_dict('records')
    
    # Clean up datetime objects for JSON serialization
    for record in disclosures_dict:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
            elif isinstance(value, (pd.Timestamp, datetime)):
                record[key] = str(value)
    
    output_file = output_dir / 'sample_disclosures_for_review.json'
    with open(output_file, 'w') as f:
        json.dump(disclosures_dict, f, indent=2)
    
    print(f"\nSample data saved to: {output_file}")
    
    return results

def analyze_provider_summary():
    """Analyze provider-level summary for compliance dashboard"""
    client = get_bigquery_client()
    group_id = 'gcO9AHYlNSzFeGTRSFRa'
    
    print("\n" + "="*60)
    print("PROVIDER COMPLIANCE SUMMARY")
    print("="*60)
    
    query = f"""
    WITH provider_summary AS (
        SELECT 
            name as provider_name,
            email,
            COUNT(*) as total_disclosures,
            COUNT(DISTINCT external_entity_name) as unique_entities,
            SUM(CAST(service_value_of_compensation AS FLOAT64)) as total_compensation,
            MAX(CAST(service_value_of_compensation AS FLOAT64)) as max_disclosure,
            SUM(CASE WHEN is_research_related = 'Yes' THEN 1 ELSE 0 END) as research_disclosures,
            SUM(CASE WHEN is_current_relationship = 'Yes' THEN 1 ELSE 0 END) as active_relationships
        FROM `conflixis-engine.firestore_export.disclosures_raw_latest_parse`
        WHERE group_id = '{group_id}'
            AND service_value_of_compensation IS NOT NULL
            AND SAFE_CAST(service_value_of_compensation AS FLOAT64) IS NOT NULL
        GROUP BY name, email
    )
    SELECT 
        provider_name,
        email,
        total_disclosures,
        unique_entities,
        ROUND(total_compensation, 2) as total_compensation,
        ROUND(max_disclosure, 2) as max_disclosure,
        research_disclosures,
        active_relationships,
        CASE 
            WHEN total_compensation >= 50000 THEN 'High Risk'
            WHEN total_compensation >= 10000 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END as overall_risk_level
    FROM provider_summary
    ORDER BY total_compensation DESC
    LIMIT 10
    """
    
    results = client.query(query).to_dataframe()
    
    print("\nTop 10 Providers by Total Compensation:")
    print(results[['provider_name', 'total_compensation', 'overall_risk_level']].to_string())
    
    # Save provider summary
    output_dir = Path(__file__).parent.parent / 'data'
    output_file = output_dir / 'provider_compliance_summary.json'
    
    summary_dict = results.to_dict('records')
    for record in summary_dict:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
    
    with open(output_file, 'w') as f:
        json.dump(summary_dict, f, indent=2)
    
    print(f"\nProvider summary saved to: {output_file}")
    
    return results

def main():
    """Main execution"""
    print("="*60)
    print("DISCLOSURE DATA EXPLORATION AND ANALYSIS")
    print("="*60)
    
    # Explore parsed disclosures
    disclosures = explore_parsed_disclosures()
    
    # Analyze provider summaries
    provider_summary = analyze_provider_summary()
    
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)
    print("\nNext Steps:")
    print("1. Use sample_disclosures_for_review.json to build review interface")
    print("2. Use provider_compliance_summary.json for dashboard")
    print("3. Apply Texas Health policy rules for automated compliance checking")

if __name__ == "__main__":
    main()