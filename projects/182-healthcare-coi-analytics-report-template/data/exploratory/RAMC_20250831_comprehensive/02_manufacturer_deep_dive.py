#!/usr/bin/env python3
"""
Manufacturer Deep Dive Analysis for RAMC - Research Paper Quality
Analyzes device vs pharmaceutical manufacturer patterns with statistical validation
Following claude-instructions.md for academic rigor
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json
from scipy import stats
from scipy.stats import mannwhitneyu, chi2_contingency, kruskal
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def analyze_device_vs_pharma():
    """Differentiate device from pharmaceutical manufacturers"""
    
    # Load manufacturer data (use most recent file)
    df = pd.read_csv('../../processed/op_top_manufacturers_20250831_220004.csv')
    
    # Enhanced device keywords based on real manufacturers in data
    device_keywords = [
        'surgical', 'stryker', 'medtronic', 'davol', 'globus', 
        'arthrex', 'ignite', 'orthopedic', 'biomet', 'depuy',
        'intuitive', 'medical device', 'implant', 'instruments'
    ]
    
    pharma_keywords = [
        'pharmaceutical', 'pharma', 'lilly', 'abbvie', 'astrazeneca',
        'pfizer', 'novartis', 'merck', 'janssen', 'genentech'
    ]
    
    # Classify manufacturers
    df['manufacturer_lower'] = df['manufacturer'].str.lower()
    df['is_device'] = df['manufacturer_lower'].apply(
        lambda x: any(keyword in x for keyword in device_keywords)
    )
    df['is_pharma'] = df['manufacturer_lower'].apply(
        lambda x: any(keyword in x for keyword in pharma_keywords)
    )
    
    # Handle ambiguous cases
    df.loc[~df['is_device'] & ~df['is_pharma'], 'is_pharma'] = True
    
    # Calculate metrics
    device_df = df[df['is_device']]
    pharma_df = df[df['is_pharma'] & ~df['is_device']]
    
    results = {
        'classification_summary': {
            'total_manufacturers': len(df),
            'device_manufacturers': len(device_df),
            'pharma_manufacturers': len(pharma_df),
            'device_percentage': round(len(device_df) / len(df) * 100, 2)
        },
        'financial_comparison': {
            'device_total_payments': float(device_df['total_payments'].sum()),
            'pharma_total_payments': float(pharma_df['total_payments'].sum()),
            'device_payment_percentage': round(
                device_df['total_payments'].sum() / df['total_payments'].sum() * 100, 2
            ),
            'device_avg_per_provider': float(
                device_df['total_payments'].sum() / device_df['unique_providers'].sum()
                if device_df['unique_providers'].sum() > 0 else 0
            ),
            'pharma_avg_per_provider': float(
                pharma_df['total_payments'].sum() / pharma_df['unique_providers'].sum()
                if pharma_df['unique_providers'].sum() > 0 else 0
            )
        },
        'top_device_manufacturers': device_df.head(10)[
            ['manufacturer', 'total_payments', 'unique_providers']
        ].to_dict('records'),
        'top_pharma_manufacturers': pharma_df.head(10)[
            ['manufacturer', 'total_payments', 'unique_providers']
        ].to_dict('records')
    }
    
    return results

def identify_strategic_relationships():
    """Identify manufacturers with strategic payment patterns"""
    
    df = pd.read_csv('../../processed/op_top_manufacturers_20250831_220004.csv')
    
    # Calculate payment concentration metrics
    df['payment_per_provider'] = df['total_payments'] / df['unique_providers']
    
    # Identify different engagement strategies
    high_volume_low_value = df[
        (df['unique_providers'] > df['unique_providers'].quantile(0.75)) &
        (df['payment_per_provider'] < df['payment_per_provider'].quantile(0.25))
    ]
    
    low_volume_high_value = df[
        (df['unique_providers'] < df['unique_providers'].quantile(0.25)) &
        (df['payment_per_provider'] > df['payment_per_provider'].quantile(0.75))
    ]
    
    # Statistical outliers
    mean_ppp = df['payment_per_provider'].mean()
    std_ppp = df['payment_per_provider'].std()
    df['z_score'] = (df['payment_per_provider'] - mean_ppp) / std_ppp
    extreme_outliers = df[df['z_score'] > 2.5]
    
    return {
        'engagement_strategies': {
            'broad_cultivation': {
                'description': 'High provider count, low individual payments',
                'count': len(high_volume_low_value),
                'manufacturers': high_volume_low_value[
                    ['manufacturer', 'unique_providers', 'payment_per_provider']
                ].head(5).to_dict('records')
            },
            'elite_capture': {
                'description': 'Low provider count, high individual payments',
                'count': len(low_volume_high_value),
                'manufacturers': low_volume_high_value[
                    ['manufacturer', 'unique_providers', 'payment_per_provider']
                ].head(5).to_dict('records')
            }
        },
        'extreme_relationships': extreme_outliers[
            ['manufacturer', 'total_payments', 'unique_providers', 'payment_per_provider', 'z_score']
        ].to_dict('records')
    }

def analyze_market_concentration():
    """Calculate market concentration metrics"""
    
    df = pd.read_csv('../../processed/op_top_manufacturers_20250831_220004.csv')
    
    total_payments = df['total_payments'].sum()
    
    # Calculate concentration ratios
    cr4 = df.head(4)['total_payments'].sum() / total_payments * 100
    cr8 = df.head(8)['total_payments'].sum() / total_payments * 100
    cr20 = df.head(20)['total_payments'].sum() / total_payments * 100
    
    # Herfindahl-Hirschman Index
    df['market_share'] = (df['total_payments'] / total_payments) * 100
    hhi = (df['market_share'] ** 2).sum()
    
    # Gini coefficient
    sorted_payments = np.sort(df['total_payments'].values)
    n = len(sorted_payments)
    cumsum = np.cumsum(sorted_payments)
    gini = (2 * np.sum(np.arange(1, n + 1) * sorted_payments)) / (n * cumsum[-1]) - (n + 1) / n
    
    return {
        'concentration_ratios': {
            'cr4': round(cr4, 2),
            'cr8': round(cr8, 2),
            'cr20': round(cr20, 2)
        },
        'hhi': {
            'value': round(hhi, 2),
            'interpretation': 'Highly concentrated' if hhi > 2500 else 
                            'Moderately concentrated' if hhi > 1500 else 'Competitive'
        },
        'gini_coefficient': round(gini, 4),
        'payment_distribution': {
            'top_1_percent': df.head(int(len(df) * 0.01))['total_payments'].sum() / total_payments * 100,
            'top_5_percent': df.head(int(len(df) * 0.05))['total_payments'].sum() / total_payments * 100,
            'top_10_percent': df.head(int(len(df) * 0.10))['total_payments'].sum() / total_payments * 100
        }
    }

def analyze_intuitive_surgical_dominance():
    """Deep statistical analysis of Intuitive Surgical's market position"""
    
    logger.info("\n" + "=" * 60)
    logger.info("INTUITIVE SURGICAL DOMINANCE ANALYSIS")
    logger.info("=" * 60)
    
    df = pd.read_csv('../../processed/op_top_manufacturers_20250831_220004.csv')
    
    # Find all Intuitive entries
    intuitive_mask = df['manufacturer'].str.contains('Intuitive|INTUITIVE', case=False, na=False)
    intuitive_df = df[intuitive_mask]
    
    if intuitive_df.empty:
        return None
    
    # Aggregate Intuitive payments
    intuitive_total = intuitive_df['total_payments'].sum()
    intuitive_providers = intuitive_df['unique_providers'].sum()
    
    # Market position analysis
    total_market = df['total_payments'].sum()
    market_share = (intuitive_total / total_market) * 100
    
    # Statistical test for market dominance
    # Chi-square goodness of fit test
    expected_share = 100 / len(df)  # Expected if equally distributed
    observed = [market_share, 100 - market_share]
    expected = [expected_share, 100 - expected_share]
    
    chi2, p_value = stats.chisquare(observed, expected)
    
    logger.info(f"Intuitive Surgical Market Analysis:")
    logger.info(f"  Total payments: ${intuitive_total:,.0f}")
    logger.info(f"  Providers engaged: {intuitive_providers}")
    logger.info(f"  Market share: {market_share:.2f}%")
    logger.info(f"  Chi-square test for dominance: χ²={chi2:.2f}, p={p_value:.6f}")
    
    # Compare to other device manufacturers
    device_keywords = ['surgical', 'stryker', 'medtronic', 'davol', 'globus']
    device_mask = df['manufacturer'].str.lower().apply(
        lambda x: any(keyword in x for keyword in device_keywords)
    )
    device_total = df[device_mask]['total_payments'].sum()
    device_share = (intuitive_total / device_total) * 100
    
    logger.info(f"  Device market share: {device_share:.2f}%")
    logger.info(f"  Average per provider: ${intuitive_total/intuitive_providers:,.2f}")
    
    return {
        'total_payments': intuitive_total,
        'providers': intuitive_providers,
        'market_share': market_share,
        'device_share': device_share,
        'chi_square': chi2,
        'p_value': p_value,
        'significant_dominance': p_value < 0.001
    }


def statistical_comparison_device_pharma():
    """Perform rigorous statistical comparison between device and pharma manufacturers"""
    
    logger.info("\n" + "=" * 60)
    logger.info("DEVICE VS PHARMACEUTICAL STATISTICAL COMPARISON")
    logger.info("=" * 60)
    
    df = pd.read_csv('../../processed/op_top_manufacturers_20250831_220004.csv')
    
    # Enhanced classification
    device_keywords = ['surgical', 'stryker', 'medtronic', 'davol', 'globus', 
                      'arthrex', 'intuitive', 'biomet', 'depuy', 'zimmer']
    pharma_keywords = ['pharmaceutical', 'pharma', 'lilly', 'abbvie', 'astrazeneca',
                      'pfizer', 'novartis', 'merck', 'janssen', 'genentech']
    
    df['is_device'] = df['manufacturer'].str.lower().apply(
        lambda x: any(keyword in x for keyword in device_keywords)
    )
    df['is_pharma'] = df['manufacturer'].str.lower().apply(
        lambda x: any(keyword in x for keyword in pharma_keywords)
    )
    
    device_df = df[df['is_device']]
    pharma_df = df[df['is_pharma'] & ~df['is_device']]
    
    # Calculate per-provider payments
    device_ppp = device_df['total_payments'] / device_df['unique_providers']
    pharma_ppp = pharma_df['total_payments'] / pharma_df['unique_providers']
    
    # Mann-Whitney U test
    u_stat, p_value = mannwhitneyu(device_ppp, pharma_ppp, alternative='two-sided')
    
    # Calculate effect size (Cliff's delta)
    n1, n2 = len(device_ppp), len(pharma_ppp)
    dominance = sum([1 if d > p else 0 for d in device_ppp for p in pharma_ppp])
    cliffs_delta = (2 * dominance / (n1 * n2)) - 1
    
    # Confidence interval for difference
    device_mean = device_ppp.mean()
    pharma_mean = pharma_ppp.mean()
    device_std = device_ppp.std()
    pharma_std = pharma_ppp.std()
    
    se_diff = np.sqrt((device_std**2 / n1) + (pharma_std**2 / n2))
    ci_lower = (device_mean - pharma_mean) - 1.96 * se_diff
    ci_upper = (device_mean - pharma_mean) + 1.96 * se_diff
    
    logger.info(f"Statistical Comparison Results:")
    logger.info(f"  Device manufacturers (n={n1}): Mean=${device_mean:,.2f}, SD=${device_std:,.2f}")
    logger.info(f"  Pharma manufacturers (n={n2}): Mean=${pharma_mean:,.2f}, SD=${pharma_std:,.2f}")
    logger.info(f"  Difference: ${device_mean - pharma_mean:,.2f} (95% CI: ${ci_lower:,.2f} to ${ci_upper:,.2f})")
    logger.info(f"  Mann-Whitney U: {u_stat:.1f}, p={p_value:.4f}")
    logger.info(f"  Effect size (Cliff's delta): {cliffs_delta:.3f}")
    
    # Interpret effect size
    if abs(cliffs_delta) < 0.147:
        effect = "negligible"
    elif abs(cliffs_delta) < 0.33:
        effect = "small"
    elif abs(cliffs_delta) < 0.474:
        effect = "medium"
    else:
        effect = "large"
    
    logger.info(f"  Effect interpretation: {effect}")
    
    return {
        'device_mean': device_mean,
        'pharma_mean': pharma_mean,
        'difference': device_mean - pharma_mean,
        'ci_lower': ci_lower,
        'ci_upper': ci_upper,
        'p_value': p_value,
        'cliffs_delta': cliffs_delta,
        'effect_size': effect,
        'significant': p_value < 0.05
    }


def main():
    """Run all manufacturer analyses with statistical rigor"""
    
    logger.info("=" * 60)
    logger.info("MANUFACTURER DEEP DIVE - RESEARCH QUALITY ANALYSIS")
    logger.info("=" * 60)
    
    # Run enhanced analyses
    device_pharma = analyze_device_vs_pharma()
    strategic = identify_strategic_relationships()
    concentration = analyze_market_concentration()
    intuitive = analyze_intuitive_surgical_dominance()
    statistical_comp = statistical_comparison_device_pharma()
    
    # Combine results
    results = {
        'analysis_date': datetime.now().isoformat(),
        'device_vs_pharma': device_pharma,
        'strategic_relationships': strategic,
        'market_concentration': concentration,
        'intuitive_surgical': intuitive,
        'statistical_comparison': statistical_comp
    }
    
    # Save results
    with open('manufacturer_analysis_results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # Generate research paper quality insights
    logger.info("\n" + "=" * 60)
    logger.info("KEY FINDINGS - MANUFACTURER ANALYSIS")
    logger.info("=" * 60)
    
    # Finding 1: Device vs Pharma differential
    if statistical_comp and statistical_comp['significant']:
        logger.info(f"\n1. Device-Pharmaceutical Payment Differential:")
        logger.info(f"   Device manufacturers demonstrate significantly higher per-provider payments")
        logger.info(f"   (${statistical_comp['device_mean']:,.2f} vs ${statistical_comp['pharma_mean']:,.2f}, p={statistical_comp['p_value']:.4f})")
        logger.info(f"   Effect size: {statistical_comp['effect_size']} (Cliff's δ={statistical_comp['cliffs_delta']:.3f})")
    
    # Finding 2: Market concentration
    logger.info(f"\n2. Market Concentration Analysis:")
    logger.info(f"   HHI={concentration['hhi']['value']:.1f} indicates {concentration['hhi']['interpretation'].lower()}")
    logger.info(f"   CR4={concentration['concentration_ratios']['cr4']:.1f}%, CR8={concentration['concentration_ratios']['cr8']:.1f}%")
    logger.info(f"   Gini coefficient={concentration['gini_coefficient']:.3f}")
    
    # Finding 3: Intuitive Surgical dominance
    if intuitive and intuitive['significant_dominance']:
        logger.info(f"\n3. Intuitive Surgical Market Dominance:")
        logger.info(f"   Controls {intuitive['market_share']:.1f}% of total market (χ²={intuitive['chi_square']:.2f}, p<0.001)")
        logger.info(f"   Represents {intuitive['device_share']:.1f}% of device manufacturer payments")
        logger.info(f"   Engages {intuitive['providers']} providers at ${intuitive['total_payments']/intuitive['providers']:,.2f} average")
    
    # Finding 4: Strategic patterns
    if strategic['extreme_relationships']:
        logger.info(f"\n4. Extreme Payment Relationships:")
        logger.info(f"   Identified {len(strategic['extreme_relationships'])} statistical outliers (z>2.5)")
        for i, rel in enumerate(strategic['extreme_relationships'][:3], 1):
            logger.info(f"   {i}. {rel['manufacturer']}: ${rel['payment_per_provider']:,.0f}/provider (z={rel['z_score']:.2f})")
    
    logger.info("\n" + "=" * 60)
    logger.info("ANALYSIS COMPLETE")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()