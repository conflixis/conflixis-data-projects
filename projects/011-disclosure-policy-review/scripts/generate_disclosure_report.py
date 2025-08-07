#!/usr/bin/env python3
"""
Generate disclosure analysis report using actual table structure
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

class DisclosureReportGenerator:
    def __init__(self):
        """Initialize BigQuery client"""
        service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        self.client = bigquery.Client(credentials=credentials, project='data-analytics-389803')
        self.group_id = 'gcO9AHYlNSzFeGTRSFRa'
        
    def analyze_disclosure_values(self):
        """Analyze compensation values and risk tiers"""
        print("\n" + "="*60)
        print("ANALYZING DISCLOSURE VALUES AND RISK TIERS")
        print("="*60)
        
        query = f"""
        WITH parsed_values AS (
            SELECT 
                document_id,
                external_entity_name,
                SAFE_CAST(compensation_value AS FLOAT64) as comp_value,
                is_research,
                review_status,
                campaign_title
            FROM `conflixis-engine.firestore_export.disclosures_raw_latest_parse`
            WHERE group_id = '{self.group_id}'
        ),
        risk_categorized AS (
            SELECT 
                *,
                CASE 
                    WHEN comp_value IS NULL OR comp_value = 0 THEN 'No Compensation'
                    WHEN comp_value < 5000 THEN 'Low Risk (<$5k)'
                    WHEN comp_value < 10000 THEN 'Tier 1 ($5k-$10k)'
                    WHEN comp_value < 50000 THEN 'Tier 2 ($10k-$50k)'
                    ELSE 'Tier 3 (>$50k)'
                END as risk_tier
            FROM parsed_values
        )
        SELECT 
            risk_tier,
            COUNT(*) as disclosure_count,
            COUNT(CASE WHEN is_research = 'true' THEN 1 END) as research_count,
            ROUND(AVG(comp_value), 2) as avg_compensation,
            ROUND(MAX(comp_value), 2) as max_compensation,
            COUNT(DISTINCT external_entity_name) as unique_entities
        FROM risk_categorized
        GROUP BY risk_tier
        ORDER BY 
            CASE risk_tier
                WHEN 'No Compensation' THEN 0
                WHEN 'Low Risk (<$5k)' THEN 1
                WHEN 'Tier 1 ($5k-$10k)' THEN 2
                WHEN 'Tier 2 ($10k-$50k)' THEN 3
                ELSE 4
            END
        """
        
        results = self.client.query(query).to_dataframe()
        
        print("\nRisk Tier Distribution:")
        print(results.to_string())
        
        # Calculate percentages
        total_disclosures = results['disclosure_count'].sum()
        results['percentage'] = (results['disclosure_count'] / total_disclosures * 100).round(1)
        
        return results
    
    def get_high_risk_disclosures(self):
        """Get disclosures requiring immediate review"""
        print("\n" + "="*60)
        print("HIGH RISK DISCLOSURES REQUIRING REVIEW")
        print("="*60)
        
        query = f"""
        SELECT 
            document_id,
            external_entity_name,
            SAFE_CAST(compensation_value AS FLOAT64) as comp_value,
            is_research,
            review_status,
            reporter_name,
            manager,
            service_start_date,
            service_end_date,
            timestamp
        FROM `conflixis-engine.firestore_export.disclosures_raw_latest_parse`
        WHERE group_id = '{self.group_id}'
            AND SAFE_CAST(compensation_value AS FLOAT64) >= 10000
        ORDER BY SAFE_CAST(compensation_value AS FLOAT64) DESC
        LIMIT 20
        """
        
        results = self.client.query(query).to_dataframe()
        
        print(f"\nFound {len(results)} high-value disclosures (>=$10,000)")
        
        # Show top 5
        if len(results) > 0:
            print("\nTop 5 Highest Value Disclosures:")
            for idx, row in results.head(5).iterrows():
                print(f"\n{idx+1}. Entity: {row['external_entity_name']}")
                print(f"   Amount: ${row['comp_value']:,.2f}" if pd.notna(row['comp_value']) else "   Amount: Not specified")
                print(f"   Research: {row['is_research']}")
                print(f"   Status: {row['review_status']}")
                
                # Determine action required
                if row['comp_value'] >= 50000:
                    print("   ⚠️ ACTION: Executive Committee Review Required (Tier 3)")
                elif row['comp_value'] >= 10000 and row['is_research'] == 'true':
                    print("   ⚠️ ACTION: Management Plan Required (Tier 2 + Research)")
                else:
                    print("   ⚠️ ACTION: Standard Management Plan Required (Tier 2)")
        
        return results
    
    def analyze_by_campaign(self):
        """Analyze disclosures by campaign"""
        print("\n" + "="*60)
        print("ANALYSIS BY CAMPAIGN")
        print("="*60)
        
        query = f"""
        SELECT 
            campaign_title,
            campaign_id,
            COUNT(*) as disclosure_count,
            COUNT(DISTINCT document_id) as unique_disclosures,
            COUNT(CASE WHEN is_research = 'true' THEN 1 END) as research_count,
            COUNT(CASE WHEN SAFE_CAST(compensation_value AS FLOAT64) >= 10000 THEN 1 END) as high_value_count
        FROM `conflixis-engine.firestore_export.disclosures_raw_latest_parse`
        WHERE group_id = '{self.group_id}'
        GROUP BY campaign_title, campaign_id
        ORDER BY disclosure_count DESC
        """
        
        results = self.client.query(query).to_dataframe()
        
        print("\nDisclosures by Campaign:")
        print(results.to_string())
        
        return results
    
    def analyze_providers(self):
        """Analyze provider-level statistics from forms table"""
        print("\n" + "="*60)
        print("PROVIDER ANALYSIS FROM DISCLOSURE FORMS")
        print("="*60)
        
        query = f"""
        WITH provider_stats AS (
            SELECT 
                user_display_name,
                user_email,
                user_npi,
                COUNT(DISTINCT document_id) as form_count,
                MAX(end_date) as latest_disclosure_date,
                COUNT(CASE WHEN status = 'complete' THEN 1 END) as completed_forms,
                COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_forms
            FROM `conflixis-engine.firestore_export.disclosure_forms_raw_latest_v3`
            WHERE group_id = '{self.group_id}'
                AND user_display_name IS NOT NULL
            GROUP BY user_display_name, user_email, user_npi
        )
        SELECT *
        FROM provider_stats
        ORDER BY form_count DESC
        LIMIT 10
        """
        
        results = self.client.query(query).to_dataframe()
        
        print(f"\nTop 10 Providers by Form Submissions:")
        if len(results) > 0:
            for idx, row in results.iterrows():
                print(f"\n{idx+1}. {row['user_display_name']}")
                print(f"   Email: {row['user_email']}")
                print(f"   NPI: {row['user_npi']}")
                print(f"   Forms: {row['form_count']} (Completed: {row['completed_forms']}, Pending: {row['pending_forms']})")
        
        return results
    
    def generate_summary_report(self):
        """Generate overall summary statistics"""
        print("\n" + "="*60)
        print("EXECUTIVE SUMMARY")
        print("="*60)
        
        # Get overall stats
        stats_query = f"""
        WITH summary AS (
            SELECT 
                COUNT(*) as total_disclosures,
                COUNT(DISTINCT document_id) as unique_documents,
                COUNT(DISTINCT external_entity_name) as unique_entities,
                COUNT(CASE WHEN is_research = 'true' THEN 1 END) as research_disclosures,
                COUNT(CASE WHEN SAFE_CAST(compensation_value AS FLOAT64) >= 50000 THEN 1 END) as tier3_count,
                COUNT(CASE WHEN SAFE_CAST(compensation_value AS FLOAT64) >= 10000 
                      AND SAFE_CAST(compensation_value AS FLOAT64) < 50000 THEN 1 END) as tier2_count,
                COUNT(CASE WHEN SAFE_CAST(compensation_value AS FLOAT64) >= 5000 
                      AND SAFE_CAST(compensation_value AS FLOAT64) < 10000 THEN 1 END) as tier1_count,
                COUNT(CASE WHEN review_status = 'pending' THEN 1 END) as pending_reviews,
                COUNT(CASE WHEN review_status = 'approved' THEN 1 END) as approved_reviews
            FROM `conflixis-engine.firestore_export.disclosures_raw_latest_parse`
            WHERE group_id = '{self.group_id}'
        )
        SELECT * FROM summary
        """
        
        summary = self.client.query(stats_query).to_dataframe()
        
        if len(summary) > 0:
            row = summary.iloc[0]
            print(f"""
Executive Summary for Group: {self.group_id}
{'='*50}
Total Disclosures: {row['total_disclosures']:,}
Unique Documents: {row['unique_documents']:,}
Unique External Entities: {row['unique_entities']:,}

Risk Tier Breakdown:
- Tier 3 (>$50k): {row['tier3_count']:,} disclosures - EXECUTIVE REVIEW REQUIRED
- Tier 2 ($10-50k): {row['tier2_count']:,} disclosures - Management Plan Required
- Tier 1 ($5-10k): {row['tier1_count']:,} disclosures - Standard Review

Research-Related: {row['research_disclosures']:,} disclosures
Pending Reviews: {row['pending_reviews']:,}
Approved Reviews: {row['approved_reviews']:,}

COMPLIANCE ACTIONS REQUIRED:
1. {row['tier3_count']} disclosures require immediate executive committee review
2. {row['tier2_count'] + row['tier1_count']} disclosures require management plans
3. {row['research_disclosures']} research-related disclosures need special attention
            """)
        
        return summary
    
    def save_report_data(self):
        """Save analysis data for dashboard"""
        output_dir = Path(__file__).parent.parent / 'data'
        output_dir.mkdir(exist_ok=True)
        
        # Get all analysis data
        risk_tiers = self.analyze_disclosure_values()
        high_risk = self.get_high_risk_disclosures()
        campaigns = self.analyze_by_campaign()
        providers = self.analyze_providers()
        summary = self.generate_summary_report()
        
        # Save to JSON files
        report_data = {
            'generated_at': datetime.now().isoformat(),
            'group_id': self.group_id,
            'risk_tiers': risk_tiers.to_dict('records'),
            'high_risk_count': len(high_risk),
            'campaigns': campaigns.to_dict('records'),
            'top_providers': providers.head(5).to_dict('records'),
            'summary': summary.to_dict('records')[0] if len(summary) > 0 else {}
        }
        
        # Clean NaN values for JSON serialization
        def clean_dict(d):
            if isinstance(d, dict):
                return {k: clean_dict(v) for k, v in d.items()}
            elif isinstance(d, list):
                return [clean_dict(item) for item in d]
            elif pd.isna(d):
                return None
            else:
                return d
        
        report_data = clean_dict(report_data)
        
        output_file = output_dir / 'disclosure_analysis_report.json'
        with open(output_file, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        print(f"\n✅ Report data saved to: {output_file}")
        
        return report_data

def main():
    """Main execution"""
    print("="*60)
    print("DISCLOSURE COMPLIANCE REPORT GENERATOR")
    print("="*60)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    generator = DisclosureReportGenerator()
    report_data = generator.save_report_data()
    
    print("\n" + "="*60)
    print("REPORT GENERATION COMPLETE")
    print("="*60)
    print("\n✅ All analysis complete. Dashboard can now display:")
    print("   - Risk tier distribution with actual data")
    print("   - High-risk disclosures requiring review")
    print("   - Provider compliance statistics")
    print("   - Campaign-level analysis")
    print("\nAccess the dashboard at: disclosure-review-dashboard.html")

if __name__ == "__main__":
    main()