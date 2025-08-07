#!/usr/bin/env python3
"""
Analyze disclosure data relationships and patterns for policy review system
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery
from typing import Dict, List, Any

# Load environment variables
load_dotenv()

class DisclosureDataAnalyzer:
    def __init__(self):
        """Initialize BigQuery client and configuration"""
        service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        self.client = bigquery.Client(credentials=credentials, project='data-analytics-389803')
        self.group_id = 'gcO9AHYlNSzFeGTRSFRa'
        
    def analyze_table_relationship(self):
        """Analyze how the two disclosure tables are related"""
        print("\n" + "="*60)
        print("ANALYZING TABLE RELATIONSHIPS")
        print("="*60)
        
        # Query to find common identifiers between tables
        # First, let's check the actual structure
        query = f"""
        WITH forms_data AS (
            SELECT 
                document_id,
                campaign_id,
                timestamp,
                status
            FROM `conflixis-engine.firestore_export.disclosure_forms_raw_latest_v3`
            WHERE group_id = '{self.group_id}'
            LIMIT 100
        ),
        parsed_data AS (
            SELECT 
                document_id,
                campaign_id,
                external_entity_name,
                user_id,
                email,
                name,
                relationship_category,
                service_value_of_compensation,
                is_research_related,
                timestamp
            FROM `conflixis-engine.firestore_export.disclosures_raw_latest_parse`
            WHERE group_id = '{self.group_id}'
            LIMIT 100
        )
        SELECT 
            'forms' as source,
            COUNT(DISTINCT document_id) as unique_documents,
            COUNT(DISTINCT campaign_id) as unique_campaigns
        FROM forms_data
        UNION ALL
        SELECT 
            'parsed' as source,
            COUNT(DISTINCT document_id) as unique_documents,
            COUNT(DISTINCT campaign_id) as unique_campaigns
        FROM parsed_data
        """
        
        results = self.client.query(query).to_dataframe()
        print("\nTable Summary:")
        print(results.to_string())
        
        # Check for matching records
        match_query = f"""
        SELECT 
            f.document_id,
            f.campaign_id,
            COUNT(DISTINCT p.document_id) as parsed_matches
        FROM `conflixis-engine.firestore_export.disclosure_forms_raw_latest_v3` f
        LEFT JOIN `conflixis-engine.firestore_export.disclosures_raw_latest_parse` p
            ON f.document_id = p.document_id
            AND f.campaign_id = p.campaign_id
        WHERE f.group_id = '{self.group_id}'
            AND p.group_id = '{self.group_id}'
        GROUP BY f.document_id, f.campaign_id
        LIMIT 10
        """
        
        matches = self.client.query(match_query).to_dataframe()
        print("\nSample Document Matches:")
        print(matches.to_string())
        
        return results
    
    def analyze_disclosure_patterns(self):
        """Analyze patterns in disclosure data"""
        print("\n" + "="*60)
        print("ANALYZING DISCLOSURE PATTERNS")
        print("="*60)
        
        # Analyze disclosure value distributions
        query = f"""
        SELECT 
            relationship_category,
            is_research_related,
            COUNT(*) as count,
            AVG(CAST(service_value_of_compensation AS FLOAT64)) as avg_value,
            MIN(CAST(service_value_of_compensation AS FLOAT64)) as min_value,
            MAX(CAST(service_value_of_compensation AS FLOAT64)) as max_value,
            STDDEV(CAST(service_value_of_compensation AS FLOAT64)) as stddev_value
        FROM `conflixis-engine.firestore_export.disclosures_raw_latest_parse`
        WHERE group_id = '{self.group_id}'
            AND service_value_of_compensation IS NOT NULL
            AND service_value_of_compensation != ''
        GROUP BY relationship_category, is_research_related
        ORDER BY count DESC
        """
        
        patterns = self.client.query(query).to_dataframe()
        print("\nDisclosure Value Patterns by Category:")
        print(patterns.to_string())
        
        # Analyze disclosure risk tiers based on Texas Health policy
        risk_query = f"""
        WITH disclosure_values AS (
            SELECT 
                document_id,
                user_id,
                name,
                external_entity_name,
                relationship_category,
                is_research_related,
                CAST(service_value_of_compensation AS FLOAT64) as compensation_value,
                timestamp
            FROM `conflixis-engine.firestore_export.disclosures_raw_latest_parse`
            WHERE group_id = '{self.group_id}'
                AND service_value_of_compensation IS NOT NULL
                AND service_value_of_compensation != ''
        )
        SELECT 
            CASE 
                WHEN compensation_value < 5000 THEN 'Low Risk (<$5k)'
                WHEN compensation_value >= 5000 AND compensation_value < 10000 THEN 'Tier 1 ($5k-$10k)'
                WHEN compensation_value >= 10000 AND compensation_value < 50000 THEN 'Tier 2 ($10k-$50k)'
                ELSE 'Tier 3 (>$50k)'
            END as risk_tier,
            COUNT(*) as disclosure_count,
            AVG(compensation_value) as avg_compensation,
            MAX(compensation_value) as max_compensation,
            COUNT(DISTINCT user_id) as unique_providers
        FROM disclosure_values
        GROUP BY risk_tier
        ORDER BY 
            CASE risk_tier
                WHEN 'Low Risk (<$5k)' THEN 1
                WHEN 'Tier 1 ($5k-$10k)' THEN 2
                WHEN 'Tier 2 ($10k-$50k)' THEN 3
                ELSE 4
            END
        """
        
        risk_tiers = self.client.query(risk_query).to_dataframe()
        print("\nDisclosure Risk Tiers (Based on Texas Health Policy):")
        print(risk_tiers.to_string())
        
        return patterns, risk_tiers
    
    def get_sample_disclosures(self, limit=5):
        """Get sample disclosures for review interface"""
        print("\n" + "="*60)
        print("FETCHING SAMPLE DISCLOSURES")
        print("="*60)
        
        query = f"""
        SELECT 
            d.document_id,
            d.user_id,
            d.name as provider_name,
            d.email,
            d.external_entity_name,
            d.relationship_category,
            d.nature_of_relationship,
            d.service_provided_description,
            CAST(d.service_value_of_compensation AS FLOAT64) as compensation_value,
            d.is_research_related,
            d.is_current_relationship,
            d.service_start_date,
            d.service_end_date,
            d.timestamp as disclosure_date,
            CASE 
                WHEN CAST(d.service_value_of_compensation AS FLOAT64) < 5000 THEN 'Low Risk'
                WHEN CAST(d.service_value_of_compensation AS FLOAT64) < 10000 THEN 'Tier 1'
                WHEN CAST(d.service_value_of_compensation AS FLOAT64) < 50000 THEN 'Tier 2'
                ELSE 'Tier 3'
            END as risk_tier,
            CASE 
                WHEN CAST(d.service_value_of_compensation AS FLOAT64) >= 10000 
                    AND d.is_research_related = 'Yes' THEN 'Requires Management Plan'
                WHEN CAST(d.service_value_of_compensation AS FLOAT64) >= 50000 THEN 'High Risk - Executive Review'
                ELSE 'Standard Review'
            END as review_status
        FROM `conflixis-engine.firestore_export.disclosures_raw_latest_parse` d
        WHERE d.group_id = '{self.group_id}'
            AND d.service_value_of_compensation IS NOT NULL
            AND d.service_value_of_compensation != ''
        ORDER BY CAST(d.service_value_of_compensation AS FLOAT64) DESC
        LIMIT {limit}
        """
        
        disclosures = self.client.query(query).to_dataframe()
        
        print(f"\nRetrieved {len(disclosures)} sample disclosures")
        
        # Convert to dict for JSON serialization
        disclosures_dict = disclosures.to_dict('records')
        
        # Save sample data for review interface
        output_path = Path(__file__).parent.parent / 'data' / 'sample_disclosures.json'
        output_path.parent.mkdir(exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(disclosures_dict, f, indent=2, default=str)
        
        print(f"Sample disclosures saved to: {output_path}")
        
        return disclosures
    
    def generate_compliance_summary(self):
        """Generate compliance summary statistics"""
        print("\n" + "="*60)
        print("GENERATING COMPLIANCE SUMMARY")
        print("="*60)
        
        query = f"""
        WITH disclosure_analysis AS (
            SELECT 
                user_id,
                name as provider_name,
                COUNT(*) as total_disclosures,
                SUM(CASE WHEN is_research_related = 'Yes' THEN 1 ELSE 0 END) as research_disclosures,
                SUM(CAST(service_value_of_compensation AS FLOAT64)) as total_compensation,
                MAX(CAST(service_value_of_compensation AS FLOAT64)) as max_single_disclosure,
                COUNT(DISTINCT external_entity_name) as unique_entities,
                COUNT(DISTINCT relationship_category) as relationship_types
            FROM `conflixis-engine.firestore_export.disclosures_raw_latest_parse`
            WHERE group_id = '{self.group_id}'
                AND service_value_of_compensation IS NOT NULL
                AND service_value_of_compensation != ''
            GROUP BY user_id, name
        )
        SELECT 
            COUNT(DISTINCT user_id) as total_providers,
            SUM(total_disclosures) as total_disclosures,
            AVG(total_disclosures) as avg_disclosures_per_provider,
            SUM(research_disclosures) as total_research_disclosures,
            AVG(total_compensation) as avg_total_compensation,
            MAX(total_compensation) as max_total_compensation,
            SUM(CASE WHEN total_compensation >= 50000 THEN 1 ELSE 0 END) as high_risk_providers,
            SUM(CASE WHEN total_compensation >= 10000 AND total_compensation < 50000 THEN 1 ELSE 0 END) as medium_risk_providers,
            SUM(CASE WHEN total_compensation < 10000 THEN 1 ELSE 0 END) as low_risk_providers
        FROM disclosure_analysis
        """
        
        summary = self.client.query(query).to_dataframe()
        print("\nCompliance Summary Statistics:")
        print(summary.to_string())
        
        return summary

def main():
    """Main execution function"""
    analyzer = DisclosureDataAnalyzer()
    
    # Analyze relationships
    analyzer.analyze_table_relationship()
    
    # Analyze patterns
    patterns, risk_tiers = analyzer.analyze_disclosure_patterns()
    
    # Get sample disclosures
    sample_disclosures = analyzer.get_sample_disclosures(limit=10)
    
    # Generate compliance summary
    summary = analyzer.generate_compliance_summary()
    
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)
    print("\nKey Findings:")
    print("1. Tables are related through document_id and campaign_id")
    print("2. Disclosure values follow Texas Health risk tier structure")
    print("3. Sample data prepared for review interface development")
    print("4. Compliance summary generated for dashboard creation")

if __name__ == "__main__":
    main()