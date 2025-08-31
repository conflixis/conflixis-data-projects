#!/usr/bin/env python3
"""
Payment Concentration Analysis for RAMC
Identifies concentration patterns and outliers in manufacturer payments
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json

class NumpyEncoder(json.JSONEncoder):
    """Custom encoder to handle numpy types"""
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)

def analyze_manufacturer_concentration():
    """Analyze concentration of payments among manufacturers"""
    
    # Load manufacturer data
    df = pd.read_csv('../../processed/op_top_manufacturers_20250830_225531.csv')
    
    # Calculate concentration metrics
    total_payments = df['total_payments'].sum()
    
    # Top 10 concentration
    top10_payments = df.head(10)['total_payments'].sum()
    top10_concentration = (top10_payments / total_payments) * 100
    
    # Herfindahl-Hirschman Index (HHI) for market concentration
    df['market_share'] = (df['total_payments'] / total_payments) * 100
    hhi = (df['market_share'] ** 2).sum()
    
    # Identify device vs pharma manufacturers
    device_keywords = ['surgical', 'stryker', 'medtronic', 'davol', 'globus', 'arthrex', 'ignite']
    df['is_device'] = df['manufacturer'].str.lower().apply(
        lambda x: any(keyword in x for keyword in device_keywords)
    )
    
    device_payments = df[df['is_device']]['total_payments'].sum()
    pharma_payments = df[~df['is_device']]['total_payments'].sum()
    
    # Calculate payment per provider ratios
    df['payment_per_provider'] = df['total_payments'] / df['unique_providers']
    
    # Identify outliers (>3 SD from mean)
    mean_ppp = df['payment_per_provider'].mean()
    std_ppp = df['payment_per_provider'].std()
    df['z_score'] = (df['payment_per_provider'] - mean_ppp) / std_ppp
    outliers = df[df['z_score'] > 3]
    
    results = {
        'concentration_metrics': {
            'top10_concentration_pct': round(top10_concentration, 2),
            'hhi_index': round(hhi, 2),
            'market_interpretation': 'Highly concentrated' if hhi > 2500 else 'Moderately concentrated' if hhi > 1500 else 'Competitive'
        },
        'device_vs_pharma': {
            'device_total': round(device_payments, 2),
            'pharma_total': round(pharma_payments, 2),
            'device_pct': round((device_payments / total_payments) * 100, 2),
            'device_avg_per_provider': round(df[df['is_device']]['payment_per_provider'].mean(), 2),
            'pharma_avg_per_provider': round(df[~df['is_device']]['payment_per_provider'].mean(), 2)
        },
        'outliers': outliers[['manufacturer', 'total_payments', 'unique_providers', 'payment_per_provider', 'z_score']].to_dict('records'),
        'duplicate_entries': identify_duplicate_manufacturers(df)
    }
    
    return results

def identify_duplicate_manufacturers(df):
    """Identify potential duplicate manufacturer entries"""
    
    # Normalize manufacturer names
    df['normalized_name'] = df['manufacturer'].str.upper().str.replace(r'[,.\s]+', ' ', regex=True).str.strip()
    
    # Find potential duplicates
    duplicates = []
    seen = set()
    
    for idx, row in df.iterrows():
        base_name = row['normalized_name'].split()[0] if row['normalized_name'] else ''
        if base_name and len(base_name) > 4:  # Skip short names
            similar = df[df['normalized_name'].str.contains(base_name, na=False)]
            if len(similar) > 1:
                group = similar['manufacturer'].tolist()
                group_key = tuple(sorted(group))
                if group_key not in seen:
                    seen.add(group_key)
                    duplicates.append({
                        'potential_duplicates': group,
                        'combined_total': similar['total_payments'].sum(),
                        'combined_providers': similar['unique_providers'].sum()
                    })
    
    return duplicates

def analyze_payment_persistence():
    """Analyze sustained payment relationships"""
    
    # Load consecutive years data
    df = pd.read_csv('../../processed/op_consecutive_years_20250830_225534.csv')
    
    total_providers = df['provider_count'].sum()
    
    # Calculate persistence metrics
    five_year_providers = df[df['years_with_payments'] == 5]['provider_count'].values[0]
    five_year_avg = df[df['years_with_payments'] == 5]['avg_total_payment'].values[0]
    one_year_providers = df[df['years_with_payments'] == 1]['provider_count'].values[0]
    one_year_avg = df[df['years_with_payments'] == 1]['avg_total_payment'].values[0]
    
    # Calculate total payments by persistence level
    df['total_payments'] = df['provider_count'] * df['avg_total_payment']
    sustained_payments = df[df['years_with_payments'] >= 3]['total_payments'].sum()
    total_payments = df['total_payments'].sum()
    
    return {
        'sustained_relationships': {
            'five_year_providers': five_year_providers,
            'five_year_pct': round((five_year_providers / total_providers) * 100, 2),
            'five_year_avg_payment': round(five_year_avg, 2),
            'payment_ratio_5yr_vs_1yr': round(five_year_avg / one_year_avg, 1),
            'sustained_payment_concentration': round((sustained_payments / total_payments) * 100, 2)
        },
        'provider_distribution': df[['years_with_payments', 'provider_count', 'avg_total_payment']].to_dict('records')
    }

def main():
    """Run all concentration analyses"""
    
    print("Running Payment Concentration Analysis...")
    
    # Run analyses
    concentration = analyze_manufacturer_concentration()
    persistence = analyze_payment_persistence()
    
    # Combine results
    results = {
        'analysis_date': datetime.now().isoformat(),
        'concentration_analysis': concentration,
        'persistence_analysis': persistence
    }
    
    # Save results
    with open('concentration_analysis_results.json', 'w') as f:
        json.dump(results, f, indent=2, cls=NumpyEncoder)
    
    # Generate summary
    print("\n=== KEY FINDINGS ===")
    print(f"Market Concentration: {concentration['concentration_metrics']['market_interpretation']} (HHI: {concentration['concentration_metrics']['hhi_index']})")
    print(f"Top 10 manufacturers control {concentration['concentration_metrics']['top10_concentration_pct']}% of payments")
    print(f"Device manufacturers: {concentration['device_vs_pharma']['device_pct']}% of total payments")
    print(f"Device avg payment per provider: ${concentration['device_vs_pharma']['device_avg_per_provider']:,.0f}")
    print(f"Pharma avg payment per provider: ${concentration['device_vs_pharma']['pharma_avg_per_provider']:,.0f}")
    print(f"\nSustained relationships: {persistence['sustained_relationships']['five_year_providers']:,} providers ({persistence['sustained_relationships']['five_year_pct']}%)")
    print(f"Five-year providers receive {persistence['sustained_relationships']['payment_ratio_5yr_vs_1yr']}x more than one-year providers")
    
    if concentration['outliers']:
        print(f"\n⚠️ Found {len(concentration['outliers'])} extreme outliers requiring investigation:")
        for outlier in concentration['outliers'][:3]:  # Show top 3
            print(f"  - {outlier['manufacturer']}: ${outlier['payment_per_provider']:,.0f}/provider (z-score: {outlier['z_score']:.1f})")
    
    if concentration['duplicate_entries']:
        print(f"\n⚠️ Found {len(concentration['duplicate_entries'])} potential duplicate manufacturer entries")
        for dup in concentration['duplicate_entries'][:2]:  # Show first 2
            print(f"  - {', '.join(dup['potential_duplicates'][:2])}: Combined ${dup['combined_total']:,.0f}")

if __name__ == "__main__":
    main()