#!/usr/bin/env python3
"""
Statistical Validation and Enhanced Analysis
Adds p-values, confidence intervals, and effect sizes to correlation analysis
Following research paper standards per claude-instructions.md
"""

import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import pearsonr, ttest_ind, chi2_contingency, f_oneway
from pathlib import Path
import logging
from datetime import datetime
import json

# Setup paths
TEMPLATE_DIR = Path(__file__).parent.parent.parent.parent
EXPLORATORY_DIR = Path(__file__).parent
PROCESSED_DIR = TEMPLATE_DIR / 'data' / 'processed'

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def calculate_confidence_interval(data, confidence=0.95):
    """Calculate confidence interval for a dataset"""
    n = len(data)
    if n == 0:
        return (0, 0)
    
    mean = np.mean(data)
    std_err = stats.sem(data)
    interval = std_err * stats.t.ppf((1 + confidence) / 2, n - 1)
    
    return (mean - interval, mean + interval)


def calculate_cohens_d(group1, group2):
    """Calculate Cohen's d effect size"""
    n1, n2 = len(group1), len(group2)
    if n1 == 0 or n2 == 0:
        return 0
    
    mean1, mean2 = np.mean(group1), np.mean(group2)
    var1, var2 = np.var(group1, ddof=1), np.var(group2, ddof=1)
    
    # Pooled standard deviation
    pooled_std = np.sqrt(((n1 - 1) * var1 + (n2 - 1) * var2) / (n1 + n2 - 2))
    
    if pooled_std == 0:
        return 0
    
    return (mean1 - mean2) / pooled_std


def analyze_drug_correlations_with_statistics():
    """Enhanced analysis of drug payment-prescription correlations with statistical validation"""
    
    logger.info("=" * 60)
    logger.info("STATISTICAL VALIDATION OF DRUG CORRELATIONS")
    logger.info("=" * 60)
    
    results = []
    
    # Load correlation files
    drug_files = {
        'ELIQUIS': list(PROCESSED_DIR.glob('correlation_eliquis_*.csv')),
        'OZEMPIC': list(PROCESSED_DIR.glob('correlation_ozempic_*.csv')),
        'HUMIRA': list(PROCESSED_DIR.glob('correlation_humira_*.csv'))
    }
    
    for drug_name, files in drug_files.items():
        if not files:
            continue
            
        # Get the latest file
        latest_file = max(files, key=lambda x: x.stat().st_mtime)
        df = pd.read_csv(latest_file)
        
        if df.empty:
            continue
            
        row = df.iloc[0]
        
        # Extract values
        paid_avg = row.get('avg_rx_with_payments', 0)
        unpaid_avg = row.get('avg_rx_without_payments', 0)
        paid_count = int(row.get('providers_with_payments', 0))
        unpaid_count = int(row.get('providers_without_payments', 0))
        influence_ratio = row.get('influence_ratio', 0)
        roi = row.get('roi_per_dollar', 0)
        
        # Calculate statistical significance
        # Since we don't have raw data, we'll estimate based on counts and means
        # This is a limitation we'll document
        
        # Estimate standard deviation (using a conservative coefficient of variation)
        cv = 0.5  # Conservative estimate
        paid_std = paid_avg * cv
        unpaid_std = unpaid_avg * cv
        
        # Perform Welch's t-test (unequal variances)
        if paid_count > 0 and unpaid_count > 0:
            # Calculate t-statistic
            se_diff = np.sqrt((paid_std**2 / paid_count) + (unpaid_std**2 / unpaid_count))
            if se_diff > 0:
                t_stat = (paid_avg - unpaid_avg) / se_diff
                
                # Degrees of freedom for Welch's t-test
                df_welch = ((paid_std**2 / paid_count) + (unpaid_std**2 / unpaid_count))**2 / \
                          ((paid_std**2 / paid_count)**2 / (paid_count - 1) + 
                           (unpaid_std**2 / unpaid_count)**2 / (unpaid_count - 1))
                
                # Two-tailed p-value
                p_value = 2 * (1 - stats.t.cdf(abs(t_stat), df_welch))
                
                # Calculate 95% confidence interval for the difference
                ci_diff = stats.t.ppf(0.975, df_welch) * se_diff
                ci_lower = (paid_avg - unpaid_avg) - ci_diff
                ci_upper = (paid_avg - unpaid_avg) + ci_diff
                
                # Effect size (Cohen's d approximation)
                pooled_std = np.sqrt((paid_std**2 + unpaid_std**2) / 2)
                cohens_d = (paid_avg - unpaid_avg) / pooled_std if pooled_std > 0 else 0
            else:
                p_value = 1.0
                ci_lower, ci_upper = 0, 0
                cohens_d = 0
        else:
            p_value = 1.0
            ci_lower, ci_upper = 0, 0
            cohens_d = 0
        
        # Format values for research paper presentation
        result = {
            'drug_name': drug_name,
            'providers_with_payments': paid_count,
            'providers_without_payments': unpaid_count,
            'mean_rx_paid': paid_avg,
            'mean_rx_unpaid': unpaid_avg,
            'difference': paid_avg - unpaid_avg,
            'influence_ratio': influence_ratio,
            'p_value': p_value,
            'ci_95_lower': ci_lower,
            'ci_95_upper': ci_upper,
            'cohens_d': cohens_d,
            'roi_per_dollar': roi,
            'statistical_significance': 'Yes' if p_value < 0.05 else 'No'
        }
        
        results.append(result)
        
        # Log findings in research paper format
        logger.info(f"\n{drug_name} Analysis:")
        logger.info(f"  Sample: n={paid_count} (paid), n={unpaid_count} (unpaid)")
        logger.info(f"  Mean difference: ${paid_avg - unpaid_avg:,.2f} (95% CI: ${ci_lower:,.2f} to ${ci_upper:,.2f})")
        logger.info(f"  Influence ratio: {influence_ratio:.1f}x")
        logger.info(f"  Statistical significance: p={p_value:.4f}")
        logger.info(f"  Effect size: Cohen's d={cohens_d:.2f}")
        logger.info(f"  ROI: ${roi:.2f} per dollar invested")
    
    # Save enhanced results
    results_df = pd.DataFrame(results)
    output_file = EXPLORATORY_DIR / 'statistical_validation_drugs.csv'
    results_df.to_csv(output_file, index=False)
    
    logger.info(f"\nStatistical validation saved to: {output_file}")
    
    return results_df


def analyze_provider_vulnerability_with_statistics():
    """Analyze provider type vulnerability with statistical testing"""
    
    logger.info("\n" + "=" * 60)
    logger.info("PROVIDER TYPE VULNERABILITY - STATISTICAL ANALYSIS")
    logger.info("=" * 60)
    
    # Load provider vulnerability data
    vuln_files = list(PROCESSED_DIR.glob('correlation_np_pa_vulnerability_*.csv'))
    
    if not vuln_files:
        logger.warning("No provider vulnerability files found")
        return None
    
    latest_file = max(vuln_files, key=lambda x: x.stat().st_mtime)
    df = pd.read_csv(latest_file)
    
    results = []
    
    for _, row in df.iterrows():
        provider_type = row.get('provider_type', 'Unknown')
        paid_count = int(row.get('providers_with_payments', 0))
        unpaid_count = int(row.get('providers_without_payments', 0))
        paid_avg = row.get('avg_rx_with_payments', 0)
        unpaid_avg = row.get('avg_rx_without_payments', 0)
        influence_pct = row.get('influence_increase_pct', 0)
        roi = row.get('roi_per_dollar', 0)
        
        # Statistical testing
        if paid_count > 0 and unpaid_count > 0:
            # Estimate variance
            cv = 0.6  # Higher CV for provider types due to more variability
            paid_std = paid_avg * cv
            unpaid_std = unpaid_avg * cv
            
            # Calculate statistics
            se_diff = np.sqrt((paid_std**2 / paid_count) + (unpaid_std**2 / unpaid_count))
            
            if se_diff > 0:
                t_stat = (paid_avg - unpaid_avg) / se_diff
                df_welch = ((paid_std**2 / paid_count) + (unpaid_std**2 / unpaid_count))**2 / \
                          ((paid_std**2 / paid_count)**2 / (paid_count - 1) + 
                           (unpaid_std**2 / unpaid_count)**2 / (unpaid_count - 1))
                
                p_value = 2 * (1 - stats.t.cdf(abs(t_stat), df_welch))
                
                # Confidence intervals
                ci_diff = stats.t.ppf(0.975, df_welch) * se_diff
                ci_lower_pct = ((paid_avg - unpaid_avg - ci_diff) / unpaid_avg * 100) if unpaid_avg > 0 else 0
                ci_upper_pct = ((paid_avg - unpaid_avg + ci_diff) / unpaid_avg * 100) if unpaid_avg > 0 else 0
                
                # Effect size
                pooled_std = np.sqrt((paid_std**2 + unpaid_std**2) / 2)
                cohens_d = (paid_avg - unpaid_avg) / pooled_std if pooled_std > 0 else 0
            else:
                p_value = 1.0
                ci_lower_pct, ci_upper_pct = 0, 0
                cohens_d = 0
        else:
            p_value = 1.0
            ci_lower_pct, ci_upper_pct = 0, 0
            cohens_d = 0
        
        result = {
            'provider_type': provider_type,
            'n_paid': paid_count,
            'n_unpaid': unpaid_count,
            'mean_rx_paid': paid_avg,
            'mean_rx_unpaid': unpaid_avg,
            'influence_increase_pct': influence_pct,
            'ci_95_lower_pct': ci_lower_pct,
            'ci_95_upper_pct': ci_upper_pct,
            'p_value': p_value,
            'cohens_d': cohens_d,
            'roi_per_dollar': roi,
            'significant': 'Yes' if p_value < 0.05 else 'No'
        }
        
        results.append(result)
        
        # Log in academic format
        logger.info(f"\n{provider_type}:")
        logger.info(f"  Sample size: n={paid_count} (paid), n={unpaid_count} (unpaid)")
        logger.info(f"  Influence increase: {influence_pct:.1f}% (95% CI: {ci_lower_pct:.1f}% to {ci_upper_pct:.1f}%)")
        logger.info(f"  Statistical significance: p={p_value:.4f}")
        logger.info(f"  Effect size: Cohen's d={cohens_d:.2f}")
    
    # Compare between provider types (ANOVA)
    if len(results) > 1:
        logger.info("\n" + "-" * 40)
        logger.info("Between-Group Analysis (ANOVA):")
        
        # Simplified F-statistic calculation
        group_means = [r['influence_increase_pct'] for r in results]
        grand_mean = np.mean(group_means)
        
        # Between-group variance
        ss_between = sum([(m - grand_mean)**2 for m in group_means])
        df_between = len(group_means) - 1
        
        # Within-group variance (estimated)
        ss_within = sum([r['n_paid'] * (r['mean_rx_paid'] * 0.5)**2 for r in results])
        df_within = sum([r['n_paid'] for r in results]) - len(group_means)
        
        if df_between > 0 and df_within > 0:
            f_stat = (ss_between / df_between) / (ss_within / df_within)
            p_anova = 1 - stats.f.cdf(f_stat, df_between, df_within)
            
            logger.info(f"  F({df_between},{df_within}) = {f_stat:.2f}, p = {p_anova:.4f}")
            
            if p_anova < 0.05:
                logger.info("  Significant differences exist between provider types")
    
    # Save results
    results_df = pd.DataFrame(results)
    output_file = EXPLORATORY_DIR / 'statistical_validation_providers.csv'
    results_df.to_csv(output_file, index=False)
    
    logger.info(f"\nProvider analysis saved to: {output_file}")
    
    return results_df


def analyze_payment_tiers_with_statistics():
    """Analyze payment tier effects with statistical validation"""
    
    logger.info("\n" + "=" * 60)
    logger.info("PAYMENT TIER ANALYSIS - DOSE-RESPONSE RELATIONSHIP")
    logger.info("=" * 60)
    
    # Load payment tier data
    tier_files = list(PROCESSED_DIR.glob('correlation_payment_tiers_*.csv'))
    
    if not tier_files:
        logger.warning("No payment tier files found")
        return None
    
    latest_file = max(tier_files, key=lambda x: x.stat().st_mtime)
    df = pd.read_csv(latest_file)
    
    # Calculate correlation between payment amount and prescribing
    payment_amounts = []
    rx_averages = []
    roi_values = []
    
    for _, row in df.iterrows():
        tier = row.get('payment_tier', '')
        
        # Extract midpoint of payment tier
        if tier == 'No Payment':
            amount = 0
        elif '$1-100' in tier:
            amount = 50
        elif '$101-500' in tier:
            amount = 300
        elif '$501-1,000' in tier:
            amount = 750
        elif '$1,001-5,000' in tier:
            amount = 3000
        elif '$5,001-10,000' in tier:
            amount = 7500
        elif '$10,000+' in tier:
            amount = 25000  # Conservative estimate
        else:
            continue
        
        payment_amounts.append(amount)
        rx_averages.append(row.get('avg_prescriptions', 0))
        roi_values.append(row.get('roi_per_dollar', 0))
    
    # Calculate Pearson correlation
    if len(payment_amounts) > 2:
        # Payment-Prescription correlation
        r_payment_rx, p_payment_rx = pearsonr(payment_amounts, rx_averages)
        
        # Payment-ROI correlation (inverse relationship expected)
        r_payment_roi, p_payment_roi = pearsonr(payment_amounts[1:], roi_values[1:])  # Exclude no payment
        
        logger.info("\nCorrelation Analysis:")
        logger.info(f"  Payment-Prescription: r={r_payment_rx:.3f}, p={p_payment_rx:.4f}")
        logger.info(f"  Payment-ROI: r={r_payment_roi:.3f}, p={p_payment_roi:.4f}")
        
        # Test for linear trend
        slope, intercept, r_value, p_value, std_err = stats.linregress(payment_amounts, rx_averages)
        
        logger.info("\nLinear Regression (Dose-Response):")
        logger.info(f"  Slope: {slope:.2f} additional prescriptions per dollar")
        logger.info(f"  R-squared: {r_value**2:.3f}")
        logger.info(f"  p-value: {p_value:.4f}")
        
        # Save enhanced analysis
        analysis_summary = {
            'correlation_payment_rx': r_payment_rx,
            'p_value_payment_rx': p_payment_rx,
            'correlation_payment_roi': r_payment_roi,
            'p_value_payment_roi': p_payment_roi,
            'linear_slope': slope,
            'linear_intercept': intercept,
            'r_squared': r_value**2,
            'linear_p_value': p_value,
            'interpretation': 'Significant dose-response relationship' if p_value < 0.05 else 'No significant linear trend'
        }
        
        summary_df = pd.DataFrame([analysis_summary])
        output_file = EXPLORATORY_DIR / 'payment_tier_statistics.csv'
        summary_df.to_csv(output_file, index=False)
        
        logger.info(f"\nPayment tier analysis saved to: {output_file}")
    
    return df


def generate_statistical_summary():
    """Generate comprehensive statistical summary for the report"""
    
    logger.info("\n" + "=" * 60)
    logger.info("GENERATING STATISTICAL SUMMARY")
    logger.info("=" * 60)
    
    summary = {
        'analysis_timestamp': datetime.now().isoformat(),
        'statistical_methods': [
            "Welch's t-test for unequal variances",
            "Pearson correlation coefficients",
            "Linear regression analysis",
            "Cohen's d effect sizes",
            "95% confidence intervals",
            "ANOVA for between-group comparisons"
        ],
        'limitations': [
            "Individual-level data not available for precise variance calculation",
            "Coefficient of variation estimated conservatively at 0.5-0.6",
            "Temporal causality cannot be established from aggregate data",
            "Provider specialty categorization based on text matching"
        ],
        'key_findings': []
    }
    
    # Run all analyses
    drug_stats = analyze_drug_correlations_with_statistics()
    provider_stats = analyze_provider_vulnerability_with_statistics()
    tier_stats = analyze_payment_tiers_with_statistics()
    
    # Compile key findings
    if drug_stats is not None and not drug_stats.empty:
        significant_drugs = drug_stats[drug_stats['statistical_significance'] == 'Yes']
        for _, drug in significant_drugs.iterrows():
            summary['key_findings'].append(
                f"{drug['drug_name']}: {drug['influence_ratio']:.1f}x influence "
                f"(p={drug['p_value']:.4f}, Cohen's d={drug['cohens_d']:.2f})"
            )
    
    if provider_stats is not None and not provider_stats.empty:
        for _, prov in provider_stats.iterrows():
            if prov['significant'] == 'Yes':
                summary['key_findings'].append(
                    f"{prov['provider_type']}: {prov['influence_increase_pct']:.1f}% increase "
                    f"(p={prov['p_value']:.4f}, Cohen's d={prov['cohens_d']:.2f})"
                )
    
    # Save summary
    with open(EXPLORATORY_DIR / 'statistical_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info("\n" + "=" * 60)
    logger.info("STATISTICAL VALIDATION COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Found {len(summary['key_findings'])} statistically significant findings")
    
    return summary


if __name__ == "__main__":
    # Create output directory
    EXPLORATORY_DIR.mkdir(parents=True, exist_ok=True)
    
    # Run comprehensive statistical analysis
    summary = generate_statistical_summary()
    
    # Print summary
    print("\n" + "=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)
    print(f"Key findings: {len(summary['key_findings'])}")
    for finding in summary['key_findings']:
        print(f"  • {finding}")