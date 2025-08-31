#!/usr/bin/env python3
"""
Temporal Pattern Analysis for RAMC
Identifies payment timing patterns, trends, and potential anomalies
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json

def analyze_yearly_growth_patterns():
    """Analyze year-over-year growth patterns and trends"""
    
    # Load yearly trends data
    df = pd.read_csv('../../processed/op_yearly_trends_20250831_215958.csv')
    
    # Calculate growth metrics
    df = df.sort_values('program_year')
    df['yoy_growth'] = df['total_payments'].pct_change() * 100
    df['yoy_provider_growth'] = df['unique_providers'].pct_change() * 100
    df['payment_per_provider'] = df['total_payments'] / df['unique_providers']
    df['payment_per_provider_growth'] = df['payment_per_provider'].pct_change() * 100
    
    # Calculate compound annual growth rate (CAGR)
    n_years = len(df) - 1
    if n_years > 0:
        cagr_payments = ((df['total_payments'].iloc[-1] / df['total_payments'].iloc[0]) ** (1/n_years) - 1) * 100
        cagr_providers = ((df['unique_providers'].iloc[-1] / df['unique_providers'].iloc[0]) ** (1/n_years) - 1) * 100
    else:
        cagr_payments = 0
        cagr_providers = 0
    
    # Identify trend patterns
    payment_trend = "increasing" if df['total_payments'].iloc[-1] > df['total_payments'].iloc[0] else "decreasing"
    provider_trend = "expanding" if df['unique_providers'].iloc[-1] > df['unique_providers'].iloc[0] else "contracting"
    
    # Check for anomalies (years with unusual growth)
    mean_growth = df['yoy_growth'].mean()
    std_growth = df['yoy_growth'].std()
    df['growth_z_score'] = (df['yoy_growth'] - mean_growth) / std_growth if std_growth > 0 else 0
    anomaly_years = df[abs(df['growth_z_score']) > 1.5]
    
    return {
        'yearly_metrics': df[['program_year', 'total_payments', 'unique_providers', 
                             'yoy_growth', 'yoy_provider_growth']].to_dict('records'),
        'growth_summary': {
            'payment_cagr': round(cagr_payments, 2),
            'provider_cagr': round(cagr_providers, 2),
            'payment_trend': payment_trend,
            'provider_trend': provider_trend,
            'total_growth_pct': round(
                ((df['total_payments'].iloc[-1] - df['total_payments'].iloc[0]) / 
                 df['total_payments'].iloc[0] * 100), 2
            )
        },
        'anomaly_years': anomaly_years[['program_year', 'yoy_growth', 'growth_z_score']].to_dict('records'),
        'strategy_shift': {
            'description': 'Shift from high-value to broad engagement' if df['payment_per_provider'].iloc[-1] < df['payment_per_provider'].iloc[0] else 'Maintained payment intensity',
            'initial_avg_payment': float(df['payment_per_provider'].iloc[0]),
            'final_avg_payment': float(df['payment_per_provider'].iloc[-1]),
            'change_pct': round(
                ((df['payment_per_provider'].iloc[-1] - df['payment_per_provider'].iloc[0]) / 
                 df['payment_per_provider'].iloc[0] * 100), 2
            )
        }
    }

def analyze_payment_persistence():
    """Analyze sustained payment relationships over time"""
    
    # Load consecutive years data
    df = pd.read_csv('../../processed/op_consecutive_years_20250831_220007.csv')
    
    # Calculate persistence metrics
    total_providers = df['provider_count'].sum()
    
    # Categorize persistence levels
    persistent_5yr = df[df['years_with_payments'] == 5]['provider_count'].sum()
    persistent_4yr = df[df['years_with_payments'] == 4]['provider_count'].sum()
    persistent_3yr = df[df['years_with_payments'] >= 3]['provider_count'].sum()
    transient = df[df['years_with_payments'] <= 2]['provider_count'].sum()
    
    # Calculate payment concentration by persistence
    df['total_payments'] = df['provider_count'] * df['avg_total_payment']
    total_payments = df['total_payments'].sum()
    
    persistent_payment_share = df[df['years_with_payments'] >= 3]['total_payments'].sum() / total_payments * 100
    
    # Calculate payment escalation pattern
    escalation_factor = (
        df[df['years_with_payments'] == 5]['avg_total_payment'].values[0] /
        df[df['years_with_payments'] == 1]['avg_total_payment'].values[0]
        if len(df[df['years_with_payments'] == 1]) > 0 else 0
    )
    
    return {
        'persistence_levels': {
            'five_year_continuous': {
                'count': int(persistent_5yr),
                'percentage': round(persistent_5yr / total_providers * 100, 2)
            },
            'four_year': {
                'count': int(persistent_4yr),
                'percentage': round(persistent_4yr / total_providers * 100, 2)
            },
            'three_plus_years': {
                'count': int(persistent_3yr),
                'percentage': round(persistent_3yr / total_providers * 100, 2)
            },
            'transient_relationships': {
                'count': int(transient),
                'percentage': round(transient / total_providers * 100, 2)
            }
        },
        'payment_concentration': {
            'persistent_payment_share_pct': round(persistent_payment_share, 2),
            'payment_escalation_factor': round(escalation_factor, 1)
        },
        'relationship_distribution': df[['years_with_payments', 'provider_count', 'avg_total_payment']].to_dict('records')
    }

def analyze_payment_categories_evolution():
    """Analyze how payment categories have evolved over time"""
    
    # Load payment categories data
    df = pd.read_csv('../../processed/op_payment_categories_20250831_215957.csv')
    
    # Calculate category concentration
    total = df['total_amount'].sum()
    df['percentage'] = df['total_amount'] / total * 100
    
    # Identify dominant categories
    top_categories = df.nlargest(5, 'total_amount')
    
    # Categorize by payment type
    service_categories = ['Compensation for services', 'Consulting Fee', 'Honoraria']
    education_categories = ['Education', 'Travel and Lodging']
    hospitality_categories = ['Food and Beverage']
    ownership_categories = ['Royalty or License', 'Current or prospective ownership']
    
    service_total = df[df['payment_category'].str.contains('|'.join(service_categories), case=False, na=False)]['total_amount'].sum()
    education_total = df[df['payment_category'].str.contains('|'.join(education_categories), case=False, na=False)]['total_amount'].sum()
    hospitality_total = df[df['payment_category'].str.contains('|'.join(hospitality_categories), case=False, na=False)]['total_amount'].sum()
    ownership_total = df[df['payment_category'].str.contains('|'.join(ownership_categories), case=False, na=False)]['total_amount'].sum()
    
    return {
        'top_categories': top_categories[['payment_category', 'total_amount', 'percentage']].to_dict('records'),
        'category_groups': {
            'service_payments': {
                'total': float(service_total),
                'percentage': round(service_total / total * 100, 2)
            },
            'education_payments': {
                'total': float(education_total),
                'percentage': round(education_total / total * 100, 2)
            },
            'hospitality_payments': {
                'total': float(hospitality_total),
                'percentage': round(hospitality_total / total * 100, 2)
            },
            'ownership_payments': {
                'total': float(ownership_total),
                'percentage': round(ownership_total / total * 100, 2)
            }
        },
        'payment_strategy_insights': {
            'high_value_focus': round((service_total + ownership_total) / total * 100, 2),
            'broad_engagement_focus': round((hospitality_total + education_total) / total * 100, 2)
        }
    }

def main():
    """Run all temporal analyses"""
    
    print("Running Temporal Pattern Analysis...")
    
    # Run analyses
    growth = analyze_yearly_growth_patterns()
    persistence = analyze_payment_persistence()
    categories = analyze_payment_categories_evolution()
    
    # Combine results
    results = {
        'analysis_date': datetime.now().isoformat(),
        'growth_patterns': growth,
        'payment_persistence': persistence,
        'category_evolution': categories
    }
    
    # Save results
    with open('temporal_analysis_results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # Generate insights
    print("\n=== TEMPORAL PATTERN INSIGHTS ===")
    print(f"Payment Growth (5-year CAGR): {growth['growth_summary']['payment_cagr']}%")
    print(f"Provider Expansion (5-year CAGR): {growth['growth_summary']['provider_cagr']}%")
    print(f"Strategy Shift: {growth['strategy_shift']['description']}")
    print(f"  Average payment change: ${growth['strategy_shift']['initial_avg_payment']:,.0f} → ${growth['strategy_shift']['final_avg_payment']:,.0f}")
    
    print(f"\nPayment Persistence:")
    print(f"  5-year continuous relationships: {persistence['persistence_levels']['five_year_continuous']['count']:,} providers ({persistence['persistence_levels']['five_year_continuous']['percentage']}%)")
    print(f"  Payment concentration in persistent relationships: {persistence['payment_concentration']['persistent_payment_share_pct']}%")
    print(f"  Payment escalation factor (5yr vs 1yr): {persistence['payment_concentration']['payment_escalation_factor']}x")
    
    print(f"\nPayment Strategy Mix:")
    print(f"  High-value focus (services + ownership): {categories['payment_strategy_insights']['high_value_focus']}%")
    print(f"  Broad engagement (hospitality + education): {categories['payment_strategy_insights']['broad_engagement_focus']}%")
    
    if growth['anomaly_years']:
        print(f"\n⚠️ Anomaly Years Detected:")
        for year in growth['anomaly_years']:
            print(f"  - {int(year['program_year'])}: {year['yoy_growth']:.1f}% growth (z-score: {year['growth_z_score']:.1f})")

if __name__ == "__main__":
    main()