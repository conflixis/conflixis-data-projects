#!/usr/bin/env python3
"""
Analyze payment timing patterns to identify potential quid pro quo arrangements
JIRA: DA-167 - Research Deep Dive

This script analyzes:
1. Payment timing before prescription spikes
2. Seasonal payment patterns
3. New drug launch payment surges
4. Payment clustering around key events
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

def analyze_payment_prescription_lag():
    """Analyze the relationship between payment timing and prescription changes"""
    
    query = f"""
    WITH payment_impact AS (
        SELECT 
            source_manufacturer,
            source_specialty,
            year,
            month,
            
            -- Payment timing groups
            CASE 
                WHEN TotalDollarsFrom = 0 THEN 'No Payment'
                WHEN TotalDollarsFrom <= 100 THEN 'Micro Payment'
                WHEN TotalDollarsFrom <= 500 THEN 'Small Payment'
                WHEN TotalDollarsFrom <= 1000 THEN 'Medium Payment'
                WHEN TotalDollarsFrom <= 5000 THEN 'Large Payment'
                ELSE 'Major Payment'
            END as payment_tier,
            
            -- Average changes from baseline
            AVG(mfg_avg_lead3 - mfg_avg_lag3) as avg_3mo_change,
            AVG(mfg_avg_lead6 - mfg_avg_lag6) as avg_6mo_change,
            AVG((mfg_avg_lead6 - mfg_avg_lag6) / NULLIF(mfg_avg_lag6, 0)) as pct_change_6mo,
            
            COUNT(*) as observations,
            AVG(attributable_pct) as avg_attribution
            
        FROM `{TABLE_REF}`
        WHERE mfg_avg_lag6 > 0  -- Had baseline prescribing
        GROUP BY source_manufacturer, source_specialty, year, month, payment_tier
    )
    SELECT 
        payment_tier,
        COUNT(DISTINCT CONCAT(source_manufacturer, source_specialty)) as combinations,
        SUM(observations) as total_observations,
        ROUND(AVG(avg_3mo_change), 2) as avg_3mo_prescription_change,
        ROUND(AVG(avg_6mo_change), 2) as avg_6mo_prescription_change,
        ROUND(AVG(pct_change_6mo) * 100, 2) as avg_pct_change_6mo,
        ROUND(AVG(avg_attribution) * 100, 3) as avg_attribution_pct
    FROM payment_impact
    GROUP BY payment_tier
    ORDER BY 
        CASE payment_tier
            WHEN 'No Payment' THEN 0
            WHEN 'Micro Payment' THEN 1
            WHEN 'Small Payment' THEN 2
            WHEN 'Medium Payment' THEN 3
            WHEN 'Large Payment' THEN 4
            WHEN 'Major Payment' THEN 5
        END
    """
    
    print("=" * 80)
    print("PAYMENT-PRESCRIPTION LAG ANALYSIS")
    print("=" * 80)
    
    results = client.query(query).to_dataframe()
    
    print("\nPayment Tier Impact on Future Prescriptions:")
    print("-" * 80)
    print(results.to_string(index=False))
    
    # Calculate lift over no-payment baseline
    if not results.empty and 'No Payment' in results['payment_tier'].values:
        baseline = results[results['payment_tier'] == 'No Payment']['avg_6mo_prescription_change'].values[0]
        print("\n" + "-" * 80)
        print("Lift Over No-Payment Baseline (6-month forward):")
        for idx, row in results.iterrows():
            if row['payment_tier'] != 'No Payment':
                lift = row['avg_6mo_prescription_change'] - baseline
                print(f"  {row['payment_tier']}: +${lift:,.2f} additional prescriptions")
    
    results.to_csv('../findings/payment_prescription_lag.csv', index=False)
    return results

def analyze_seasonal_patterns():
    """Identify seasonal payment and influence patterns"""
    
    query = f"""
    WITH monthly_patterns AS (
        SELECT 
            month,
            CASE 
                WHEN month IN (1,2,3) THEN 'Q1'
                WHEN month IN (4,5,6) THEN 'Q2'
                WHEN month IN (7,8,9) THEN 'Q3'
                ELSE 'Q4'
            END as quarter,
            
            COUNT(DISTINCT NPI) as providers_paid,
            SUM(TotalDollarsFrom) as total_payments,
            AVG(TotalDollarsFrom) as avg_payment,
            AVG(CASE WHEN TotalDollarsFrom > 0 THEN attributable_pct END) as avg_attribution,
            SUM(attributable_dollars) as total_attributable,
            
            -- Calculate ROI by month
            SAFE_DIVIDE(SUM(attributable_dollars), SUM(TotalDollarsFrom)) as monthly_roi
            
        FROM `{TABLE_REF}`
        GROUP BY month
    )
    SELECT 
        month,
        quarter,
        providers_paid,
        ROUND(total_payments / 1e6, 2) as payments_millions,
        ROUND(avg_payment, 2) as avg_payment_amount,
        ROUND(avg_attribution * 100, 3) as avg_attribution_pct,
        ROUND(total_attributable / 1e6, 2) as attributable_millions,
        ROUND(monthly_roi, 2) as roi
    FROM monthly_patterns
    ORDER BY month
    """
    
    print("\n" + "=" * 80)
    print("SEASONAL PAYMENT PATTERNS")
    print("=" * 80)
    
    results = client.query(query).to_dataframe()
    
    print("\nMonthly Payment and Influence Patterns:")
    print("-" * 80)
    print(results.to_string(index=False))
    
    # Identify peak months
    if not results.empty:
        peak_payment_month = results.loc[results['payments_millions'].idxmax()]
        peak_roi_month = results.loc[results['roi'].idxmax()]
        
        print("\n" + "-" * 80)
        print("Key Findings:")
        print(f"  Peak Payment Month: {peak_payment_month['month']} (${peak_payment_month['payments_millions']}M)")
        print(f"  Peak ROI Month: {peak_roi_month['month']} ({peak_roi_month['roi']}x)")
        
        # Quarter analysis
        quarter_summary = results.groupby('quarter').agg({
            'payments_millions': 'sum',
            'attributable_millions': 'sum',
            'avg_attribution_pct': 'mean'
        }).round(2)
        
        print("\nQuarterly Summary:")
        print(quarter_summary)
    
    results.to_csv('../findings/seasonal_patterns.csv', index=False)
    return results

def analyze_manufacturer_timing_strategies():
    """Analyze how different manufacturers time their payments"""
    
    query = f"""
    WITH manufacturer_timing AS (
        SELECT 
            source_manufacturer,
            year,
            month,
            
            -- Payment metrics
            COUNT(DISTINCT NPI) as providers_targeted,
            SUM(TotalDollarsFrom) as monthly_payments,
            AVG(TotalDollarsFrom) as avg_payment_size,
            
            -- Influence metrics
            AVG(CASE WHEN TotalDollarsFrom > 0 THEN attributable_pct END) as avg_attribution,
            SUM(attributable_dollars) as monthly_attributable,
            
            -- Calculate month-over-month changes
            LAG(SUM(TotalDollarsFrom)) OVER (PARTITION BY source_manufacturer ORDER BY year, month) as prev_month_payments,
            LAG(SUM(attributable_dollars)) OVER (PARTITION BY source_manufacturer ORDER BY year, month) as prev_month_attributable
            
        FROM `{TABLE_REF}`
        GROUP BY source_manufacturer, year, month
    ),
    timing_analysis AS (
        SELECT 
            source_manufacturer,
            year,
            month,
            providers_targeted,
            monthly_payments,
            avg_payment_size,
            avg_attribution,
            monthly_attributable,
            
            -- Calculate changes
            SAFE_DIVIDE(monthly_payments - prev_month_payments, prev_month_payments) as payment_change_pct,
            SAFE_DIVIDE(monthly_attributable - prev_month_attributable, prev_month_attributable) as attributable_change_pct,
            
            -- Identify surges (>50% increase)
            CASE 
                WHEN SAFE_DIVIDE(monthly_payments - prev_month_payments, prev_month_payments) > 0.5 THEN 1 
                ELSE 0 
            END as payment_surge
            
        FROM manufacturer_timing
    )
    SELECT 
        source_manufacturer,
        COUNT(DISTINCT CONCAT(year, '-', month)) as months_active,
        SUM(payment_surge) as payment_surges,
        ROUND(AVG(monthly_payments) / 1e6, 2) as avg_monthly_payments_millions,
        ROUND(STDDEV(monthly_payments) / 1e6, 2) as payment_volatility_millions,
        ROUND(AVG(avg_attribution) * 100, 3) as avg_attribution_pct,
        ROUND(SUM(monthly_attributable) / SUM(monthly_payments), 2) as overall_roi,
        ROUND(MAX(monthly_payments) / 1e6, 2) as max_monthly_payment_millions,
        ROUND(MIN(monthly_payments) / 1e6, 2) as min_monthly_payment_millions
    FROM timing_analysis
    GROUP BY source_manufacturer
    HAVING COUNT(DISTINCT CONCAT(year, '-', month)) >= 12  -- Active for at least 12 months
    ORDER BY payment_volatility_millions DESC
    """
    
    print("\n" + "=" * 80)
    print("MANUFACTURER PAYMENT TIMING STRATEGIES")
    print("=" * 80)
    
    results = client.query(query).to_dataframe()
    
    print("\nManufacturer Payment Volatility Analysis:")
    print("-" * 80)
    
    for idx, row in results.head(10).iterrows():
        print(f"\n{row['source_manufacturer']}:")
        print(f"  Payment Surges: {row['payment_surges']} months with >50% increase")
        print(f"  Avg Monthly: ${row['avg_monthly_payments_millions']}M")
        print(f"  Volatility: ±${row['payment_volatility_millions']}M")
        print(f"  Range: ${row['min_monthly_payment_millions']}M - ${row['max_monthly_payment_millions']}M")
        print(f"  Overall ROI: {row['overall_roi']}x")
    
    results.to_csv('../findings/manufacturer_timing_strategies.csv', index=False)
    return results

def identify_payment_clusters():
    """Identify suspicious clustering of payments"""
    
    query = f"""
    WITH provider_payment_sequence AS (
        SELECT 
            NPI,
            source_manufacturer,
            year,
            month,
            TotalDollarsFrom,
            attributable_pct,
            
            -- Create payment sequence flags
            LAG(TotalDollarsFrom, 1) OVER (PARTITION BY NPI, source_manufacturer ORDER BY year, month) as prev_payment,
            LAG(TotalDollarsFrom, 2) OVER (PARTITION BY NPI, source_manufacturer ORDER BY year, month) as prev2_payment,
            LEAD(TotalDollarsFrom, 1) OVER (PARTITION BY NPI, source_manufacturer ORDER BY year, month) as next_payment,
            
            -- Identify consecutive payments
            CASE 
                WHEN TotalDollarsFrom > 0 AND LAG(TotalDollarsFrom, 1) OVER (PARTITION BY NPI, source_manufacturer ORDER BY year, month) > 0 
                THEN 1 ELSE 0 
            END as consecutive_payment
            
        FROM `{TABLE_REF}`
    ),
    cluster_analysis AS (
        SELECT 
            NPI,
            source_manufacturer,
            
            -- Count payment patterns
            SUM(CASE WHEN TotalDollarsFrom > 0 THEN 1 ELSE 0 END) as total_payments,
            SUM(consecutive_payment) as consecutive_payments,
            
            -- Calculate clustering score
            SAFE_DIVIDE(SUM(consecutive_payment), SUM(CASE WHEN TotalDollarsFrom > 0 THEN 1 ELSE 0 END)) as clustering_score,
            
            -- Payment and influence metrics
            SUM(TotalDollarsFrom) as total_payment_amount,
            AVG(CASE WHEN TotalDollarsFrom > 0 THEN attributable_pct END) as avg_attribution_when_paid,
            
            -- Identify payment acceleration (increasing amounts)
            SUM(CASE 
                WHEN TotalDollarsFrom > prev_payment AND prev_payment > 0 
                THEN 1 ELSE 0 
            END) as payment_increases
            
        FROM provider_payment_sequence
        GROUP BY NPI, source_manufacturer
        HAVING SUM(CASE WHEN TotalDollarsFrom > 0 THEN 1 ELSE 0 END) >= 3  -- At least 3 payments
    )
    SELECT 
        source_manufacturer,
        COUNT(DISTINCT NPI) as providers_with_clusters,
        AVG(clustering_score) as avg_clustering_score,
        AVG(CASE WHEN clustering_score > 0.5 THEN 1 ELSE 0 END) as pct_highly_clustered,
        ROUND(AVG(total_payment_amount), 2) as avg_total_payments,
        ROUND(AVG(avg_attribution_when_paid) * 100, 3) as avg_attribution_pct,
        SUM(payment_increases) as total_payment_escalations
    FROM cluster_analysis
    GROUP BY source_manufacturer
    ORDER BY avg_clustering_score DESC
    """
    
    print("\n" + "=" * 80)
    print("PAYMENT CLUSTERING ANALYSIS")
    print("=" * 80)
    
    results = client.query(query).to_dataframe()
    
    print("\nManufacturers with Suspicious Payment Clustering:")
    print("-" * 80)
    
    for idx, row in results.head(10).iterrows():
        cluster_pct = row.get('pct_highly_clustered', 0) * 100
        print(f"\n{row['source_manufacturer']}:")
        print(f"  Providers with Clusters: {row['providers_with_clusters']:,}")
        print(f"  Avg Clustering Score: {row['avg_clustering_score']:.2%}")
        print(f"  Highly Clustered: {cluster_pct:.1f}% of providers")
        print(f"  Payment Escalations: {row['total_payment_escalations']:,}")
        print(f"  Avg Attribution: {row['avg_attribution_pct']}%")
    
    results.to_csv('../findings/payment_clusters.csv', index=False)
    return results

def analyze_payment_prescription_correlation():
    """Calculate correlation between payment amounts and prescription changes"""
    
    query = f"""
    WITH payment_prescription_pairs AS (
        SELECT 
            source_manufacturer,
            source_specialty,
            
            -- Group payments into buckets for correlation analysis
            CASE 
                WHEN TotalDollarsFrom = 0 THEN 0
                WHEN TotalDollarsFrom <= 100 THEN 1
                WHEN TotalDollarsFrom <= 500 THEN 2
                WHEN TotalDollarsFrom <= 1000 THEN 3
                WHEN TotalDollarsFrom <= 5000 THEN 4
                ELSE 5
            END as payment_bucket,
            
            TotalDollarsFrom as payment_amount,
            mfg_avg_lead6 - mfg_avg_lag6 as prescription_change,
            attributable_pct,
            attributable_dollars
            
        FROM `{TABLE_REF}`
        WHERE mfg_avg_lag6 > 0  -- Had baseline prescribing
    )
    SELECT 
        source_manufacturer,
        source_specialty,
        COUNT(*) as observations,
        
        -- Calculate Pearson correlation coefficient
        CORR(payment_amount, prescription_change) as payment_prescription_correlation,
        CORR(payment_bucket, prescription_change) as bucket_prescription_correlation,
        
        -- Average metrics by payment bucket
        AVG(CASE WHEN payment_bucket = 0 THEN prescription_change END) as no_payment_change,
        AVG(CASE WHEN payment_bucket = 5 THEN prescription_change END) as major_payment_change,
        
        -- Attribution metrics
        AVG(attributable_pct) as avg_attribution,
        SUM(attributable_dollars) / NULLIF(SUM(payment_amount), 0) as roi
        
    FROM payment_prescription_pairs
    GROUP BY source_manufacturer, source_specialty
    HAVING COUNT(*) >= 100  -- Sufficient sample size
    ORDER BY ABS(payment_prescription_correlation) DESC
    LIMIT 50
    """
    
    print("\n" + "=" * 80)
    print("PAYMENT-PRESCRIPTION CORRELATION ANALYSIS")
    print("=" * 80)
    
    results = client.query(query).to_dataframe()
    
    print("\nTop Correlations Between Payments and Prescription Changes:")
    print("-" * 80)
    
    # Filter for strong correlations
    strong_correlations = results[abs(results['payment_prescription_correlation']) > 0.3]
    
    if not strong_correlations.empty:
        print(f"\nFound {len(strong_correlations)} manufacturer-specialty pairs with strong correlation (|r| > 0.3)")
        
        for idx, row in strong_correlations.head(10).iterrows():
            correlation = row['payment_prescription_correlation']
            correlation_strength = "strong positive" if correlation > 0.5 else "moderate positive" if correlation > 0.3 else "moderate negative" if correlation < -0.3 else "strong negative"
            
            print(f"\n{row['source_manufacturer']} - {row['source_specialty']}:")
            print(f"  Correlation: {correlation:.3f} ({correlation_strength})")
            print(f"  No Payment Δ: ${row['no_payment_change']:,.2f}")
            print(f"  Major Payment Δ: ${row['major_payment_change']:,.2f}")
            print(f"  Lift: ${row['major_payment_change'] - row['no_payment_change']:,.2f}")
            print(f"  ROI: {row['roi']:.2f}x" if row['roi'] else "  ROI: N/A")
    
    results.to_csv('../findings/payment_prescription_correlation.csv', index=False)
    return results

def main():
    """Run all payment timing analyses"""
    
    print("PAYMENT TIMING PATTERN ANALYSIS")
    print("=" * 80)
    print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Dataset: {TABLE_REF}")
    print("=" * 80)
    
    # Run analyses
    lag_analysis = analyze_payment_prescription_lag()
    seasonal = analyze_seasonal_patterns()
    manufacturer_timing = analyze_manufacturer_timing_strategies()
    clusters = identify_payment_clusters()
    correlation = analyze_payment_prescription_correlation()
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print("All results saved to /findings/ directory")
    
    # Key insights summary
    print("\nKEY TIMING INSIGHTS:")
    print("-" * 80)
    
    if not seasonal.empty:
        peak_month = seasonal.loc[seasonal['roi'].idxmax()]
        print(f"1. Peak ROI Month: {peak_month['month']} ({peak_month['roi']}x)")
    
    if not manufacturer_timing.empty:
        most_volatile = manufacturer_timing.iloc[0]
        print(f"2. Most Volatile Manufacturer: {most_volatile['source_manufacturer']} (±${most_volatile['payment_volatility_millions']}M)")
    
    if not clusters.empty:
        most_clustered = clusters.iloc[0]
        print(f"3. Highest Payment Clustering: {most_clustered['source_manufacturer']} ({most_clustered['avg_clustering_score']:.1%})")
    
    if not correlation.empty:
        strongest_corr = correlation.iloc[0]
        print(f"4. Strongest Payment-Prescription Correlation: {strongest_corr['source_manufacturer']}-{strongest_corr['source_specialty']} (r={strongest_corr['payment_prescription_correlation']:.3f})")

if __name__ == "__main__":
    main()