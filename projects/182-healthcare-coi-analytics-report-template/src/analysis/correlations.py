"""
Correlation Analysis Module
Analyzes correlations between payments and prescribing patterns
"""

import pandas as pd
import numpy as np
from scipy import stats
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class CorrelationAnalyzer:
    """Advanced correlation and statistical analysis"""
    
    def __init__(self, payments_data: pd.DataFrame, prescription_data: pd.DataFrame):
        """
        Initialize analyzer with payment and prescription data
        
        Args:
            payments_data: DataFrame with Open Payments data
            prescription_data: DataFrame with prescription data
        """
        self.payments = payments_data
        self.prescriptions = prescription_data
        self.merged_data = None
        self.results = {}
        
    def prepare_data(self) -> pd.DataFrame:
        """
        Merge and prepare payment and prescription data for analysis
        
        Returns:
            Merged DataFrame
        """
        logger.info("Preparing data for correlation analysis")
        
        # Aggregate payments by provider
        payment_summary = self.payments.groupby('physician_id').agg({
            'total_amount': 'sum',
            'payment_count': 'sum',
            'manufacturer': 'nunique',
            'payment_category': 'nunique',
            'payment_year': 'nunique'
        }).reset_index()
        
        payment_summary.columns = ['NPI', 'total_payments', 'payment_transactions',
                                  'unique_manufacturers', 'unique_categories', 'payment_years']
        
        # Aggregate prescriptions by provider
        rx_summary = self.prescriptions.groupby('NPI').agg({
            'total_claims': 'sum',
            'total_cost': 'sum',
            'BRAND_NAME': 'nunique',
            'total_beneficiaries': 'sum'
        }).reset_index()
        
        rx_summary.columns = ['NPI', 'total_rx_claims', 'total_rx_cost',
                              'unique_drugs', 'total_beneficiaries']
        
        # Merge datasets
        self.merged_data = pd.merge(
            rx_summary,
            payment_summary,
            on='NPI',
            how='outer'
        )
        
        # Fill missing values (providers with no payments)
        payment_cols = ['total_payments', 'payment_transactions', 
                       'unique_manufacturers', 'unique_categories', 'payment_years']
        self.merged_data[payment_cols] = self.merged_data[payment_cols].fillna(0)
        
        # Create binary payment indicator
        self.merged_data['received_payments'] = (self.merged_data['total_payments'] > 0).astype(int)
        
        logger.info(f"Prepared data for {len(self.merged_data)} providers")
        return self.merged_data
    
    def analyze_all(self) -> Dict[str, Any]:
        """
        Run all correlation analyses
        
        Returns:
            Dictionary with all analysis results
        """
        logger.info("Starting comprehensive correlation analysis")
        
        if self.merged_data is None:
            self.prepare_data()
        
        self.results['basic_correlations'] = self.calculate_basic_correlations()
        self.results['drug_specific'] = self.analyze_drug_specific_correlations()
        self.results['payment_tiers'] = self.analyze_payment_tier_effects()
        self.results['provider_type_vulnerability'] = self.analyze_provider_vulnerability()
        self.results['temporal_patterns'] = self.analyze_temporal_correlations()
        self.results['statistical_tests'] = self.perform_statistical_tests()
        self.results['influence_metrics'] = self.calculate_influence_metrics()
        
        logger.info("Correlation analysis complete")
        return self.results
    
    def calculate_basic_correlations(self) -> Dict[str, Any]:
        """Calculate basic correlation metrics"""
        correlations = {}
        
        # Pearson correlations
        pearson_vars = ['total_payments', 'payment_transactions', 'total_rx_cost', 'total_rx_claims']
        pearson_matrix = self.merged_data[pearson_vars].corr(method='pearson')
        
        # Spearman correlations (for non-linear relationships)
        spearman_matrix = self.merged_data[pearson_vars].corr(method='spearman')
        
        # Key correlations
        correlations['payment_rx_cost_pearson'] = pearson_matrix.loc['total_payments', 'total_rx_cost']
        correlations['payment_rx_cost_spearman'] = spearman_matrix.loc['total_payments', 'total_rx_cost']
        correlations['payment_rx_volume_pearson'] = pearson_matrix.loc['total_payments', 'total_rx_claims']
        correlations['payment_rx_volume_spearman'] = spearman_matrix.loc['total_payments', 'total_rx_claims']
        
        # Calculate confidence intervals for correlations
        n = len(self.merged_data)
        for key in ['payment_rx_cost_pearson', 'payment_rx_volume_pearson']:
            r = correlations[key]
            if not np.isnan(r):
                # Fisher transformation for CI
                z = np.arctanh(r)
                se = 1 / np.sqrt(n - 3)
                ci_low = np.tanh(z - 1.96 * se)
                ci_high = np.tanh(z + 1.96 * se)
                correlations[f'{key}_ci'] = (ci_low, ci_high)
        
        logger.info("Calculated basic correlations")
        return correlations
    
    def analyze_drug_specific_correlations(self, top_n: int = 20) -> pd.DataFrame:
        """
        Analyze correlations for specific drugs
        
        Args:
            top_n: Number of top drugs to analyze
            
        Returns:
            DataFrame with drug-specific correlations
        """
        # Get top drugs by total cost
        top_drugs = self.prescriptions.groupby('BRAND_NAME')['total_cost'].sum().nlargest(top_n).index
        
        drug_correlations = []
        
        for drug in top_drugs:
            # Get prescribers of this drug
            drug_rx = self.prescriptions[self.prescriptions['BRAND_NAME'] == drug].groupby('NPI').agg({
                'total_cost': 'sum',
                'total_claims': 'sum'
            }).reset_index()
            
            # Merge with payment data
            drug_merged = pd.merge(
                drug_rx,
                self.merged_data[['NPI', 'total_payments', 'received_payments']],
                on='NPI',
                how='left'
            )
            drug_merged['total_payments'] = drug_merged['total_payments'].fillna(0)
            drug_merged['received_payments'] = drug_merged['received_payments'].fillna(0)
            
            # Calculate metrics
            with_payments = drug_merged[drug_merged['received_payments'] == 1]
            without_payments = drug_merged[drug_merged['received_payments'] == 0]
            
            if len(with_payments) > 0 and len(without_payments) > 0:
                # T-test for difference in means
                t_stat, p_value = stats.ttest_ind(
                    with_payments['total_cost'],
                    without_payments['total_cost'],
                    equal_var=False
                )
                
                # Effect size (Cohen's d)
                pooled_std = np.sqrt(
                    (with_payments['total_cost'].std()**2 + without_payments['total_cost'].std()**2) / 2
                )
                cohens_d = (with_payments['total_cost'].mean() - without_payments['total_cost'].mean()) / pooled_std
                
                # Influence factor
                influence_factor = (
                    with_payments['total_cost'].mean() / 
                    max(without_payments['total_cost'].mean(), 1)  # Avoid division by zero
                )
                
                drug_correlations.append({
                    'drug': drug,
                    'prescribers_with_payments': len(with_payments),
                    'prescribers_without_payments': len(without_payments),
                    'avg_rx_value_with_payments': with_payments['total_cost'].mean(),
                    'avg_rx_value_without_payments': without_payments['total_cost'].mean(),
                    'influence_factor': influence_factor,
                    't_statistic': t_stat,
                    'p_value': p_value,
                    'cohens_d': cohens_d
                })
        
        drug_corr_df = pd.DataFrame(drug_correlations)
        drug_corr_df = drug_corr_df.sort_values('influence_factor', ascending=False)
        
        logger.info(f"Analyzed correlations for {len(drug_corr_df)} drugs")
        return drug_corr_df
    
    def analyze_payment_tier_effects(self) -> pd.DataFrame:
        """Analyze the effect of different payment tiers on prescribing"""
        # Define payment tiers
        tiers = [
            (0, 0, 'No Payment'),
            (0.01, 100, '$1-100'),
            (100, 500, '$100-500'),
            (500, 1000, '$500-1K'),
            (1000, 5000, '$1K-5K'),
            (5000, 10000, '$5K-10K'),
            (10000, 50000, '$10K-50K'),
            (50000, float('inf'), '$50K+')
        ]
        
        tier_analysis = []
        
        for min_val, max_val, label in tiers:
            if min_val == 0 and max_val == 0:
                # No payment tier
                tier_data = self.merged_data[self.merged_data['total_payments'] == 0]
            else:
                tier_data = self.merged_data[
                    (self.merged_data['total_payments'] > min_val) & 
                    (self.merged_data['total_payments'] <= max_val)
                ]
            
            if not tier_data.empty:
                tier_analysis.append({
                    'payment_tier': label,
                    'provider_count': len(tier_data),
                    'avg_rx_cost': tier_data['total_rx_cost'].mean(),
                    'median_rx_cost': tier_data['total_rx_cost'].median(),
                    'avg_rx_claims': tier_data['total_rx_claims'].mean(),
                    'median_rx_claims': tier_data['total_rx_claims'].median(),
                    'total_rx_value': tier_data['total_rx_cost'].sum(),
                    'pct_of_providers': len(tier_data) / len(self.merged_data) * 100
                })
        
        tier_df = pd.DataFrame(tier_analysis)
        
        # Calculate ROI for each tier
        baseline_rx = tier_df[tier_df['payment_tier'] == 'No Payment']['avg_rx_cost'].values[0]
        for idx, row in tier_df.iterrows():
            if row['payment_tier'] != 'No Payment':
                # Extract average payment for this tier
                tier_providers = self.merged_data[
                    self.merged_data['total_payments'].between(
                        tiers[idx][0], tiers[idx][1]
                    )
                ]
                if not tier_providers.empty:
                    avg_payment = tier_providers['total_payments'].mean()
                    incremental_rx = row['avg_rx_cost'] - baseline_rx
                    tier_df.loc[idx, 'roi'] = incremental_rx / max(avg_payment, 1)
        
        logger.info(f"Analyzed {len(tier_df)} payment tiers")
        return tier_df
    
    def analyze_provider_vulnerability(self) -> pd.DataFrame:
        """Analyze vulnerability to payment influence by provider type"""
        if 'provider_type' not in self.prescriptions.columns:
            logger.warning("Provider type not available for vulnerability analysis")
            return pd.DataFrame()
        
        # Get provider types
        provider_types = self.prescriptions.groupby('NPI')['provider_type'].first()
        self.merged_data['provider_type'] = self.merged_data['NPI'].map(provider_types)
        
        vulnerability = []
        
        for ptype in self.merged_data['provider_type'].dropna().unique():
            type_data = self.merged_data[self.merged_data['provider_type'] == ptype]
            
            with_payments = type_data[type_data['received_payments'] == 1]
            without_payments = type_data[type_data['received_payments'] == 0]
            
            if len(with_payments) > 0 and len(without_payments) > 0:
                # Calculate influence metrics
                rx_influence = (
                    with_payments['total_rx_cost'].mean() / 
                    max(without_payments['total_rx_cost'].mean(), 1) - 1
                ) * 100  # Percentage increase
                
                volume_influence = (
                    with_payments['total_rx_claims'].mean() / 
                    max(without_payments['total_rx_claims'].mean(), 1) - 1
                ) * 100
                
                vulnerability.append({
                    'provider_type': ptype,
                    'total_providers': len(type_data),
                    'providers_with_payments': len(with_payments),
                    'payment_rate': len(with_payments) / len(type_data) * 100,
                    'avg_rx_with_payments': with_payments['total_rx_cost'].mean(),
                    'avg_rx_without_payments': without_payments['total_rx_cost'].mean(),
                    'rx_cost_influence_pct': rx_influence,
                    'rx_volume_influence_pct': volume_influence
                })
        
        vulnerability_df = pd.DataFrame(vulnerability)
        if not vulnerability_df.empty and 'rx_cost_influence_pct' in vulnerability_df.columns:
            vulnerability_df = vulnerability_df.sort_values('rx_cost_influence_pct', ascending=False)
        else:
            # Return empty DataFrame with expected structure
            vulnerability_df = pd.DataFrame(columns=['provider_type', 'providers_with_payments', 
                                                    'providers_without_payments', 'avg_payments_received', 
                                                    'avg_rx_with_payments', 'avg_rx_without_payments',
                                                    'rx_cost_influence_pct', 'rx_volume_influence_pct'])
        
        logger.info(f"Analyzed vulnerability for {len(vulnerability_df)} provider types")
        return vulnerability_df
    
    def validate_results(self, df: pd.DataFrame, metric_name: str) -> pd.DataFrame:
        """
        Validate analysis results for data quality issues
        
        Args:
            df: Results dataframe
            metric_name: Name of the metric being validated
            
        Returns:
            DataFrame with validation flags added
        """
        try:
            from src.config.analysis_config import ANALYSIS_THRESHOLDS
        except ImportError:
            # Fallback to defaults if config not found
            ANALYSIS_THRESHOLDS = {
                'max_reasonable_influence': 10,
                'max_reasonable_avg_rx': 5_000_000
            }
        
        df = df.copy()
        df['data_quality_flags'] = ''
        
        # Check influence factors
        if 'influence_factor' in df.columns:
            mask = df['influence_factor'] > ANALYSIS_THRESHOLDS['max_reasonable_influence']
            df.loc[mask, 'data_quality_flags'] += 'HIGH_INFLUENCE;'
            if mask.any():
                logger.warning(f"{metric_name}: {mask.sum()} rows with influence factor > {ANALYSIS_THRESHOLDS['max_reasonable_influence']}x")
        
        # Check average Rx values
        if 'avg_rx_with_payments' in df.columns:
            mask = df['avg_rx_with_payments'] > ANALYSIS_THRESHOLDS['max_reasonable_avg_rx']
            df.loc[mask, 'data_quality_flags'] += 'HIGH_RX_VALUE;'
            if mask.any():
                logger.warning(f"{metric_name}: {mask.sum()} rows with avg Rx > ${ANALYSIS_THRESHOLDS['max_reasonable_avg_rx']:,}")
        
        # Check for percentage-based influence metrics
        if 'rx_cost_influence_pct' in df.columns:
            # Convert percentage to factor (e.g., 100% = 2x)
            factor_equivalent = (df['rx_cost_influence_pct'] / 100) + 1
            mask = factor_equivalent > ANALYSIS_THRESHOLDS['max_reasonable_influence']
            df.loc[mask, 'data_quality_flags'] += 'HIGH_INFLUENCE_PCT;'
            if mask.any():
                logger.warning(f"{metric_name}: {mask.sum()} rows with influence percentage > {(ANALYSIS_THRESHOLDS['max_reasonable_influence'] - 1) * 100}%")
        
        return df
    
    def analyze_temporal_correlations(self) -> Dict[str, Any]:
        """Analyze how correlations change over time"""
        temporal = {}
        
        # Group by year if available
        if 'payment_year' in self.payments.columns:
            years = sorted(self.payments['payment_year'].unique())
            
            yearly_correlations = []
            for year in years:
                year_payments = self.payments[self.payments['payment_year'] == year]
                year_rx = self.prescriptions[self.prescriptions['rx_year'] == year]
                
                # Aggregate for the year
                year_pay_summary = year_payments.groupby('physician_id')['total_amount'].sum()
                year_rx_summary = year_rx.groupby('NPI')['total_cost'].sum()
                
                # Merge
                year_merged = pd.merge(
                    year_pay_summary.reset_index(),
                    year_rx_summary.reset_index(),
                    left_on='physician_id',
                    right_on='NPI',
                    how='inner'
                )
                
                if len(year_merged) > 10:  # Need sufficient data for correlation
                    corr, p_value = stats.pearsonr(
                        year_merged['total_amount'],
                        year_merged['total_cost']
                    )
                    
                    yearly_correlations.append({
                        'year': year,
                        'correlation': corr,
                        'p_value': p_value,
                        'n_providers': len(year_merged)
                    })
            
            temporal['yearly_correlations'] = pd.DataFrame(yearly_correlations)
            
            # Trend analysis
            if len(yearly_correlations) > 2:
                years_numeric = [d['year'] for d in yearly_correlations]
                correlations_list = [d['correlation'] for d in yearly_correlations]
                
                # Linear regression on correlation trend
                slope, intercept, r_value, p_value, std_err = stats.linregress(
                    years_numeric, correlations_list
                )
                
                temporal['correlation_trend'] = {
                    'slope': slope,
                    'intercept': intercept,
                    'r_squared': r_value**2,
                    'p_value': p_value,
                    'interpretation': 'Increasing' if slope > 0 else 'Decreasing'
                }
        
        logger.info("Analyzed temporal correlation patterns")
        return temporal
    
    def perform_statistical_tests(self) -> Dict[str, Any]:
        """Perform various statistical tests"""
        tests = {}
        
        # Mann-Whitney U test (non-parametric alternative to t-test)
        with_payments = self.merged_data[self.merged_data['received_payments'] == 1]
        without_payments = self.merged_data[self.merged_data['received_payments'] == 0]
        
        if len(with_payments) > 0 and len(without_payments) > 0:
            # Test for prescription cost differences
            u_stat, p_value = stats.mannwhitneyu(
                with_payments['total_rx_cost'],
                without_payments['total_rx_cost'],
                alternative='two-sided'
            )
            
            tests['mann_whitney_rx_cost'] = {
                'u_statistic': u_stat,
                'p_value': p_value,
                'significant': p_value < 0.05
            }
            
            # Test for prescription volume differences
            u_stat, p_value = stats.mannwhitneyu(
                with_payments['total_rx_claims'],
                without_payments['total_rx_claims'],
                alternative='two-sided'
            )
            
            tests['mann_whitney_rx_volume'] = {
                'u_statistic': u_stat,
                'p_value': p_value,
                'significant': p_value < 0.05
            }
            
            # Kolmogorov-Smirnov test for distribution differences
            ks_stat, p_value = stats.ks_2samp(
                with_payments['total_rx_cost'],
                without_payments['total_rx_cost']
            )
            
            tests['ks_test_rx_cost'] = {
                'ks_statistic': ks_stat,
                'p_value': p_value,
                'significant': p_value < 0.05
            }
        
        # Chi-square test for independence
        if 'specialty' in self.prescriptions.columns:
            specialty_counts = self.prescriptions.groupby('NPI')['specialty'].first()
            self.merged_data['specialty'] = self.merged_data['NPI'].map(specialty_counts)
            
            # Create contingency table
            contingency = pd.crosstab(
                self.merged_data['specialty'],
                self.merged_data['received_payments']
            )
            
            # Only perform chi-square if we have data
            if contingency.size > 0 and contingency.shape[0] > 1 and contingency.shape[1] > 1:
                chi2, p_value, dof, expected = stats.chi2_contingency(contingency)
            else:
                chi2, p_value, dof, expected = 0, 1.0, 0, None
            
            tests['chi_square_specialty_payment'] = {
                'chi2_statistic': chi2,
                'p_value': p_value,
                'degrees_of_freedom': dof,
                'significant': p_value < 0.05
            }
        
        logger.info("Performed statistical tests")
        return tests
    
    def calculate_influence_metrics(self) -> Dict[str, Any]:
        """Calculate various influence metrics"""
        metrics = {}
        
        # Overall influence factor
        with_payments = self.merged_data[self.merged_data['received_payments'] == 1]
        without_payments = self.merged_data[self.merged_data['received_payments'] == 0]
        
        if len(with_payments) > 0 and len(without_payments) > 0:
            metrics['overall_rx_cost_influence'] = (
                with_payments['total_rx_cost'].mean() / 
                max(without_payments['total_rx_cost'].mean(), 1)
            )
            
            metrics['overall_rx_volume_influence'] = (
                with_payments['total_rx_claims'].mean() / 
                max(without_payments['total_rx_claims'].mean(), 1)
            )
            
            # ROI calculation
            total_payments = with_payments['total_payments'].sum()
            incremental_rx = (
                with_payments['total_rx_cost'].sum() - 
                without_payments['total_rx_cost'].mean() * len(with_payments)
            )
            metrics['overall_roi'] = incremental_rx / max(total_payments, 1)
            
            # Payment efficiency (rx dollars per payment dollar)
            metrics['payment_efficiency'] = (
                with_payments['total_rx_cost'].sum() / 
                max(with_payments['total_payments'].sum(), 1)
            )
        
        logger.info("Calculated influence metrics")
        return metrics
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """Get summary statistics for reporting"""
        if not self.results:
            self.analyze_all()
        
        summary = {
            'basic_correlations': self.results.get('basic_correlations', {}),
            'top_drug_correlations': self.results.get('drug_specific', pd.DataFrame()).head(10).to_dict('records'),
            'payment_tiers': self.results.get('payment_tiers', pd.DataFrame()).to_dict('records'),
            'provider_vulnerability': self.results.get('provider_type_vulnerability', pd.DataFrame()).to_dict('records'),
            'influence_metrics': self.results.get('influence_metrics', {}),
            'statistical_tests': self.results.get('statistical_tests', {})
        }
        
        return summary