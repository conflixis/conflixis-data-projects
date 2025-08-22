#!/usr/bin/env python3
"""
Identify providers with extreme attribution rates suggesting potential conflicts of interest
JIRA: DA-167 - Research Deep Dive

This script identifies:
1. Providers with >30% attribution rates (high influence)
2. Providers receiving payments from 5+ manufacturers (professional influencers)
3. Provider-manufacturer pairs with extreme ROI
4. Geographic clusters of high-influence providers
"""

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('../../../.env')

# Setup BigQuery client
service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
service_account_info = json.loads(service_account_json)
credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=['https://www.googleapis.com/auth/bigquery']
)

client = bigquery.Client(project='data-analytics-389803', credentials=credentials)
TABLE_REF = "data-analytics-389803.conflixis_agent.rx_op_enhanced_full"

def identify_high_attribution_providers():
    """Find providers with attribution rates >30%"""
    
    query = f"""
    WITH provider_attribution AS (
        SELECT 
            NPI,
            source_manufacturer,
            source_specialty,
            HQ_STATE,
            COUNT(*) as observations,
            AVG(attributable_pct) as avg_attribution_pct,
            SUM(TotalDollarsFrom) as total_payments,
            SUM(attributable_dollars) as total_attributable,
            SUM(totalNext6) as total_prescribed
        FROM `{TABLE_REF}`
        WHERE TotalDollarsFrom > 0
        GROUP BY NPI, source_manufacturer, source_specialty, HQ_STATE
        HAVING AVG(attributable_pct) > 0.30
    )
    SELECT 
        NPI,
        source_manufacturer,
        source_specialty,
        HQ_STATE,
        observations,
        ROUND(avg_attribution_pct * 100, 2) as attribution_pct,
        ROUND(total_payments, 2) as total_payments,
        ROUND(total_attributable, 2) as attributable_revenue,
        ROUND(total_prescribed, 2) as total_prescribed,
        ROUND(SAFE_DIVIDE(total_attributable, total_payments), 2) as roi
    FROM provider_attribution
    ORDER BY attribution_pct DESC
    LIMIT 100
    """
    
    print("=" * 80)
    print("HIGH ATTRIBUTION PROVIDERS (>30% of prescriptions payment-influenced)")
    print("=" * 80)
    
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        print(f"\nFound {len(results)} high-risk provider-manufacturer relationships")
        print("\nTop 10 by Attribution Rate:")
        print("-" * 80)
        
        for idx, row in results.head(10).iterrows():
            print(f"\nNPI: {row['NPI']}")
            print(f"  Manufacturer: {row['source_manufacturer']}")
            print(f"  Specialty: {row['source_specialty']}")
            print(f"  State: {row['HQ_STATE']}")
            print(f"  Attribution: {row['attribution_pct']}%")
            print(f"  Payments Received: ${row['total_payments']:,.2f}")
            print(f"  Attributable Revenue: ${row['attributable_revenue']:,.2f}")
            print(f"  ROI: {row['roi']}x")
        
        # Save full results
        results.to_csv('../findings/high_attribution_providers.csv', index=False)
        print(f"\nFull results saved to: findings/high_attribution_providers.csv")
    
    return results

def identify_multi_manufacturer_influencers():
    """Find providers receiving payments from multiple manufacturers"""
    
    query = f"""
    WITH provider_manufacturers AS (
        SELECT 
            NPI,
            COUNT(DISTINCT source_manufacturer) as num_manufacturers,
            COUNT(DISTINCT source_specialty) as num_specialties,
            SUM(TotalDollarsFrom) as total_payments,
            AVG(attributable_pct) as avg_attribution,
            SUM(attributable_dollars) as total_attributable,
            STRING_AGG(DISTINCT source_manufacturer, ', ' LIMIT 10) as manufacturers
        FROM `{TABLE_REF}`
        WHERE TotalDollarsFrom > 0
        GROUP BY NPI
        HAVING COUNT(DISTINCT source_manufacturer) >= 5
    )
    SELECT 
        NPI,
        num_manufacturers,
        num_specialties,
        ROUND(total_payments, 2) as total_payments,
        ROUND(avg_attribution * 100, 2) as avg_attribution_pct,
        ROUND(total_attributable, 2) as total_attributable,
        ROUND(SAFE_DIVIDE(total_attributable, total_payments), 2) as roi,
        manufacturers
    FROM provider_manufacturers
    ORDER BY num_manufacturers DESC, total_payments DESC
    LIMIT 100
    """
    
    print("\n" + "=" * 80)
    print("MULTI-MANUFACTURER INFLUENCERS (Receiving from 5+ companies)")
    print("=" * 80)
    
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        print(f"\nFound {len(results)} providers receiving from 5+ manufacturers")
        print("\nTop 10 Multi-Manufacturer Recipients:")
        print("-" * 80)
        
        for idx, row in results.head(10).iterrows():
            print(f"\nNPI: {row['NPI']}")
            print(f"  Manufacturers: {row['num_manufacturers']}")
            print(f"  Total Payments: ${row['total_payments']:,.2f}")
            print(f"  Avg Attribution: {row['avg_attribution_pct']}%")
            print(f"  Total Attributable: ${row['total_attributable']:,.2f}")
            print(f"  Companies: {row['manufacturers'][:100]}...")
        
        results.to_csv('../findings/multi_manufacturer_influencers.csv', index=False)
        print(f"\nFull results saved to: findings/multi_manufacturer_influencers.csv")
    
    return results

def identify_extreme_roi_relationships():
    """Find provider-manufacturer pairs with extreme ROI (>20x)"""
    
    query = f"""
    WITH extreme_roi AS (
        SELECT 
            NPI,
            source_manufacturer,
            source_specialty,
            HQ_STATE,
            COUNT(*) as observations,
            SUM(TotalDollarsFrom) as total_payments,
            SUM(attributable_dollars) as total_attributable,
            SAFE_DIVIDE(SUM(attributable_dollars), SUM(TotalDollarsFrom)) as roi,
            AVG(attributable_pct) as avg_attribution
        FROM `{TABLE_REF}`
        WHERE TotalDollarsFrom > 100  -- Minimum payment threshold
        GROUP BY NPI, source_manufacturer, source_specialty, HQ_STATE
        HAVING SAFE_DIVIDE(SUM(attributable_dollars), SUM(TotalDollarsFrom)) > 20
    )
    SELECT 
        NPI,
        source_manufacturer,
        source_specialty,
        HQ_STATE,
        observations,
        ROUND(total_payments, 2) as total_payments,
        ROUND(total_attributable, 2) as attributable_revenue,
        ROUND(roi, 2) as roi_multiplier,
        ROUND(avg_attribution * 100, 2) as attribution_pct
    FROM extreme_roi
    WHERE total_payments > 1000  -- Focus on significant relationships
    ORDER BY roi DESC
    LIMIT 100
    """
    
    print("\n" + "=" * 80)
    print("EXTREME ROI RELATIONSHIPS (>20x return on payment)")
    print("=" * 80)
    
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        print(f"\nFound {len(results)} extreme ROI relationships")
        print("\nTop 10 Extreme ROI Cases:")
        print("-" * 80)
        
        for idx, row in results.head(10).iterrows():
            print(f"\nNPI: {row['NPI']}")
            print(f"  Manufacturer: {row['source_manufacturer']}")
            print(f"  Specialty: {row['source_specialty']}")
            print(f"  State: {row['HQ_STATE']}")
            print(f"  ROI: {row['roi_multiplier']}x")
            print(f"  Investment: ${row['total_payments']:,.2f}")
            print(f"  Return: ${row['attributable_revenue']:,.2f}")
            print(f"  Attribution: {row['attribution_pct']}%")
        
        results.to_csv('../findings/extreme_roi_relationships.csv', index=False)
        print(f"\nFull results saved to: findings/extreme_roi_relationships.csv")
    
    return results

def identify_geographic_hotspots():
    """Identify geographic clusters of high-influence providers"""
    
    query = f"""
    WITH state_metrics AS (
        SELECT 
            HQ_STATE,
            COUNT(DISTINCT NPI) as num_providers,
            COUNT(DISTINCT CASE WHEN TotalDollarsFrom > 0 THEN NPI END) as paid_providers,
            AVG(CASE WHEN TotalDollarsFrom > 0 THEN attributable_pct END) as avg_attribution,
            SUM(TotalDollarsFrom) as total_payments,
            SUM(attributable_dollars) as total_attributable,
            SAFE_DIVIDE(SUM(attributable_dollars), SUM(TotalDollarsFrom)) as state_roi
        FROM `{TABLE_REF}`
        WHERE HQ_STATE IS NOT NULL
        GROUP BY HQ_STATE
        HAVING COUNT(DISTINCT NPI) > 1000  -- Significant provider presence
    )
    SELECT 
        HQ_STATE,
        num_providers,
        paid_providers,
        ROUND(avg_attribution * 100, 3) as avg_attribution_pct,
        ROUND(total_payments / 1e6, 2) as payments_millions,
        ROUND(total_attributable / 1e6, 2) as attributable_millions,
        ROUND(state_roi, 2) as roi,
        ROUND(SAFE_DIVIDE(paid_providers, num_providers) * 100, 1) as pct_providers_paid
    FROM state_metrics
    WHERE avg_attribution > 0.001  -- Focus on states with measurable influence
    ORDER BY avg_attribution_pct DESC
    """
    
    print("\n" + "=" * 80)
    print("GEOGRAPHIC INFLUENCE HOTSPOTS")
    print("=" * 80)
    
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        print(f"\nAnalyzed {len(results)} states")
        print("\nTop 10 States by Average Attribution Rate:")
        print("-" * 80)
        
        for idx, row in results.head(10).iterrows():
            print(f"\n{row['HQ_STATE']}:")
            print(f"  Avg Attribution: {row['avg_attribution_pct']}%")
            print(f"  Providers: {row['num_providers']:,} ({row['pct_providers_paid']}% receive payments)")
            print(f"  Total Payments: ${row['payments_millions']}M")
            print(f"  Attributable Revenue: ${row['attributable_millions']}M")
            print(f"  State ROI: {row['roi']}x")
        
        results.to_csv('../findings/geographic_hotspots.csv', index=False)
        print(f"\nFull results saved to: findings/geographic_hotspots.csv")
    
    return results

def generate_risk_scores():
    """Generate composite risk scores for providers"""
    
    query = f"""
    WITH provider_risk_factors AS (
        SELECT 
            NPI,
            MAX(source_specialty) as primary_specialty,
            MAX(HQ_STATE) as state,
            
            -- Risk Factor 1: High attribution rate
            AVG(CASE WHEN TotalDollarsFrom > 0 THEN attributable_pct END) as avg_attribution,
            
            -- Risk Factor 2: Multiple manufacturers
            COUNT(DISTINCT CASE WHEN TotalDollarsFrom > 0 THEN source_manufacturer END) as num_manufacturers,
            
            -- Risk Factor 3: High payment amounts
            SUM(TotalDollarsFrom) as total_payments,
            
            -- Risk Factor 4: Extreme ROI generation
            MAX(SAFE_DIVIDE(attributable_dollars, NULLIF(TotalDollarsFrom, 0))) as max_roi,
            
            -- Risk Factor 5: Payment frequency
            COUNT(CASE WHEN TotalDollarsFrom > 0 THEN 1 END) as payment_observations,
            
            -- Overall metrics
            SUM(attributable_dollars) as total_attributable,
            SUM(totalNext6) as total_prescribed
            
        FROM `{TABLE_REF}`
        GROUP BY NPI
        HAVING SUM(TotalDollarsFrom) > 0
    ),
    risk_scored AS (
        SELECT 
            *,
            -- Composite risk score (0-100 scale)
            LEAST(100, 
                (CASE WHEN avg_attribution > 0.30 THEN 25 ELSE avg_attribution * 83.33 END) +
                (CASE WHEN num_manufacturers >= 5 THEN 25 ELSE num_manufacturers * 5 END) +
                (CASE WHEN total_payments > 50000 THEN 25 ELSE total_payments / 2000 END) +
                (CASE WHEN max_roi > 20 THEN 25 ELSE max_roi * 1.25 END)
            ) as risk_score
        FROM provider_risk_factors
    )
    SELECT 
        NPI,
        primary_specialty,
        state,
        ROUND(risk_score, 1) as risk_score,
        ROUND(avg_attribution * 100, 2) as attribution_pct,
        num_manufacturers,
        ROUND(total_payments, 2) as total_payments,
        ROUND(max_roi, 2) as max_roi,
        payment_observations,
        ROUND(total_attributable, 2) as total_attributable,
        CASE 
            WHEN risk_score >= 75 THEN 'CRITICAL'
            WHEN risk_score >= 50 THEN 'HIGH'
            WHEN risk_score >= 25 THEN 'MODERATE'
            ELSE 'LOW'
        END as risk_category
    FROM risk_scored
    WHERE risk_score >= 50  -- Focus on high-risk providers
    ORDER BY risk_score DESC
    LIMIT 200
    """
    
    print("\n" + "=" * 80)
    print("PROVIDER RISK SCORING (Composite Conflict of Interest Risk)")
    print("=" * 80)
    
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        risk_distribution = results['risk_category'].value_counts()
        
        print(f"\nRisk Distribution:")
        for category, count in risk_distribution.items():
            print(f"  {category}: {count} providers")
        
        print("\nTop 10 Highest Risk Providers:")
        print("-" * 80)
        
        for idx, row in results.head(10).iterrows():
            print(f"\nNPI: {row['NPI']}")
            print(f"  Risk Score: {row['risk_score']}/100 ({row['risk_category']})")
            print(f"  Specialty: {row['primary_specialty']}")
            print(f"  State: {row['state']}")
            print(f"  Attribution: {row['attribution_pct']}%")
            print(f"  Manufacturers: {row['num_manufacturers']}")
            print(f"  Total Payments: ${row['total_payments']:,.2f}")
            print(f"  Max ROI Generated: {row['max_roi']}x")
        
        results.to_csv('../findings/provider_risk_scores.csv', index=False)
        print(f"\nFull results saved to: findings/provider_risk_scores.csv")
    
    return results

def main():
    """Run all outlier identification analyses"""
    
    print("OUTLIER IDENTIFICATION ANALYSIS")
    print("=" * 80)
    print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Dataset: {TABLE_REF}")
    print("=" * 80)
    
    # Run analyses
    high_attribution = identify_high_attribution_providers()
    multi_manufacturer = identify_multi_manufacturer_influencers()
    extreme_roi = identify_extreme_roi_relationships()
    hotspots = identify_geographic_hotspots()
    risk_scores = generate_risk_scores()
    
    # Summary statistics
    print("\n" + "=" * 80)
    print("ANALYSIS SUMMARY")
    print("=" * 80)
    
    if not high_attribution.empty:
        print(f"High Attribution Providers (>30%): {len(high_attribution)}")
    
    if not multi_manufacturer.empty:
        print(f"Multi-Manufacturer Influencers (5+): {len(multi_manufacturer)}")
    
    if not extreme_roi.empty:
        print(f"Extreme ROI Relationships (>20x): {len(extreme_roi)}")
    
    if not hotspots.empty:
        top_state = hotspots.iloc[0]
        print(f"Highest Influence State: {top_state['HQ_STATE']} ({top_state['avg_attribution_pct']}% avg attribution)")
    
    if not risk_scores.empty:
        critical_count = len(risk_scores[risk_scores['risk_category'] == 'CRITICAL'])
        print(f"Critical Risk Providers: {critical_count}")
    
    print("\n" + "=" * 80)
    print("Analysis complete. Results saved to /findings/ directory")
    print("=" * 80)

if __name__ == "__main__":
    main()