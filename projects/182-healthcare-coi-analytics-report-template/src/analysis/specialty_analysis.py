"""
Specialty Analysis Module
Analyzes patterns specific to medical specialties
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from scipy import stats

logger = logging.getLogger(__name__)


class SpecialtyAnalyzer:
    """Specialty-specific pattern analysis"""
    
    def __init__(self, payments_data: pd.DataFrame, prescription_data: pd.DataFrame):
        """
        Initialize analyzer with payment and prescription data
        
        Args:
            payments_data: Open Payments data with specialty info
            prescription_data: Prescription data with specialty info
        """
        self.payments = payments_data
        self.prescriptions = prescription_data
        self.results = {}
        
    def analyze_all(self) -> Dict[str, Any]:
        """
        Run all specialty analyses
        
        Returns:
            Dictionary with all analysis results
        """
        logger.info("Starting comprehensive specialty analysis")
        
        self.results['specialty_overview'] = self.generate_specialty_overview()
        self.results['payment_patterns'] = self.analyze_payment_patterns_by_specialty()
        self.results['prescription_patterns'] = self.analyze_prescription_patterns_by_specialty()
        self.results['specialty_correlations'] = self.analyze_specialty_correlations()
        self.results['outlier_specialties'] = self.identify_outlier_specialties()
        self.results['specialty_drug_preferences'] = self.analyze_specialty_drug_preferences()
        self.results['specialty_vulnerability'] = self.assess_specialty_vulnerability()
        
        logger.info("Specialty analysis complete")
        return self.results
    
    def generate_specialty_overview(self) -> pd.DataFrame:
        """Generate overview statistics by specialty"""
        # Payment statistics by specialty
        if 'specialty' in self.payments.columns:
            payment_stats = self.payments.groupby('specialty').agg({
                'physician_id': 'nunique',
                'total_amount': ['sum', 'mean', 'median'],
                'payment_count': 'sum',
                'manufacturer': 'nunique'
            }).round(2)
            
            payment_stats.columns = ['providers_with_payments', 'total_payments', 
                                    'avg_payment', 'median_payment', 'transaction_count',
                                    'unique_manufacturers']
        else:
            payment_stats = pd.DataFrame()
        
        # Prescription statistics by specialty
        if 'specialty' in self.prescriptions.columns:
            rx_stats = self.prescriptions.groupby('specialty').agg({
                'NPI': 'nunique',
                'total_claims': 'sum',
                'total_cost': 'sum',
                'BRAND_NAME': 'nunique',
                'total_beneficiaries': 'sum'
            }).round(2)
            
            rx_stats.columns = ['prescribers', 'total_rx_claims', 'total_rx_cost',
                               'unique_drugs', 'total_beneficiaries']
        else:
            rx_stats = pd.DataFrame()
        
        # Merge if both available
        if not payment_stats.empty and not rx_stats.empty:
            overview = pd.merge(
                payment_stats,
                rx_stats,
                left_index=True,
                right_index=True,
                how='outer'
            )
            
            # Calculate derived metrics
            overview['payment_penetration'] = (
                overview['providers_with_payments'] / overview['prescribers'] * 100
            ).round(1)
            
            overview['avg_rx_per_provider'] = (
                overview['total_rx_cost'] / overview['prescribers']
            ).round(0)
            
            overview['payment_to_rx_ratio'] = (
                overview['total_payments'] / overview['total_rx_cost']
            ).round(4)
            
            # Sort by total prescription cost
            overview = overview.sort_values('total_rx_cost', ascending=False)
        else:
            overview = payment_stats if not payment_stats.empty else rx_stats
        
        logger.info(f"Generated overview for {len(overview)} specialties")
        return overview
    
    def analyze_payment_patterns_by_specialty(self) -> pd.DataFrame:
        """Analyze payment patterns unique to each specialty"""
        if 'specialty' not in self.payments.columns:
            logger.warning("Specialty not available in payment data")
            return pd.DataFrame()
        
        patterns = []
        
        for specialty in self.payments['specialty'].dropna().unique():
            spec_data = self.payments[self.payments['specialty'] == specialty]
            
            # Payment distribution
            payment_dist = spec_data['total_amount'].describe()
            
            # Top payment categories
            top_categories = spec_data.groupby('payment_category')['total_amount'].sum().nlargest(3)
            
            # Top manufacturers
            top_manufacturers = spec_data.groupby('manufacturer')['total_amount'].sum().nlargest(3)
            
            patterns.append({
                'specialty': specialty,
                'providers': spec_data['physician_id'].nunique(),
                'total_payments': spec_data['total_amount'].sum(),
                'mean_payment': payment_dist['mean'],
                'median_payment': payment_dist['50%'],
                'p75_payment': payment_dist['75%'],
                'p95_payment': spec_data['total_amount'].quantile(0.95),
                'top_category': top_categories.index[0] if len(top_categories) > 0 else None,
                'top_category_pct': top_categories.iloc[0] / spec_data['total_amount'].sum() * 100 if len(top_categories) > 0 else 0,
                'top_manufacturer': top_manufacturers.index[0] if len(top_manufacturers) > 0 else None,
                'top_manufacturer_pct': top_manufacturers.iloc[0] / spec_data['total_amount'].sum() * 100 if len(top_manufacturers) > 0 else 0
            })
        
        patterns_df = pd.DataFrame(patterns)
        patterns_df = patterns_df.sort_values('total_payments', ascending=False)
        
        logger.info(f"Analyzed payment patterns for {len(patterns_df)} specialties")
        return patterns_df
    
    def analyze_prescription_patterns_by_specialty(self) -> pd.DataFrame:
        """Analyze prescription patterns unique to each specialty"""
        if 'specialty' not in self.prescriptions.columns:
            logger.warning("Specialty not available in prescription data")
            return pd.DataFrame()
        
        patterns = []
        
        for specialty in self.prescriptions['specialty'].dropna().unique():
            spec_data = self.prescriptions[self.prescriptions['specialty'] == specialty]
            
            # Top drugs for this specialty
            top_drugs = spec_data.groupby('BRAND_NAME')['total_cost'].sum().nlargest(5)
            
            # Cost distribution
            provider_costs = spec_data.groupby('NPI')['total_cost'].sum()
            
            patterns.append({
                'specialty': specialty,
                'prescribers': spec_data['NPI'].nunique(),
                'total_rx_cost': spec_data['total_cost'].sum(),
                'total_rx_claims': spec_data['total_claims'].sum(),
                'unique_drugs': spec_data['BRAND_NAME'].nunique(),
                'avg_cost_per_claim': spec_data['total_cost'].sum() / spec_data['total_claims'].sum(),
                'top_drug': top_drugs.index[0] if len(top_drugs) > 0 else None,
                'top_drug_pct': top_drugs.iloc[0] / spec_data['total_cost'].sum() * 100 if len(top_drugs) > 0 else 0,
                'top_5_drugs_concentration': top_drugs.sum() / spec_data['total_cost'].sum() * 100 if len(top_drugs) > 0 else 0,
                'avg_cost_per_provider': provider_costs.mean(),
                'median_cost_per_provider': provider_costs.median()
            })
        
        patterns_df = pd.DataFrame(patterns)
        # Sort by existing column if available
        if 'total_rx_cost' in patterns_df.columns:
            patterns_df = patterns_df.sort_values('total_rx_cost', ascending=False)
        elif 'avg_cost_per_claim' in patterns_df.columns:
            patterns_df = patterns_df.sort_values('avg_cost_per_claim', ascending=False)
        
        logger.info(f"Analyzed prescription patterns for {len(patterns_df)} specialties")
        return patterns_df
    
    def analyze_specialty_correlations(self) -> pd.DataFrame:
        """Analyze payment-prescription correlations by specialty"""
        correlations = []
        
        # Merge payment and prescription data by provider
        if 'specialty' in self.payments.columns and 'specialty' in self.prescriptions.columns:
            # Aggregate payments by provider and specialty
            pay_by_provider = self.payments.groupby(['physician_id', 'specialty'])['total_amount'].sum().reset_index()
            pay_by_provider.columns = ['NPI', 'specialty', 'total_payments']
            
            # Aggregate prescriptions by provider and specialty
            rx_by_provider = self.prescriptions.groupby(['NPI', 'specialty'])['total_cost'].sum().reset_index()
            rx_by_provider.columns = ['NPI', 'specialty', 'total_rx_cost']
            
            # Merge
            merged = pd.merge(pay_by_provider, rx_by_provider, on=['NPI', 'specialty'], how='outer')
            merged = merged.fillna(0)
            
            # Calculate correlations by specialty
            for specialty in merged['specialty'].dropna().unique():
                spec_data = merged[merged['specialty'] == specialty]
                
                if len(spec_data) > 10:  # Need sufficient data
                    # Pearson correlation
                    if spec_data['total_payments'].std() > 0 and spec_data['total_rx_cost'].std() > 0:
                        corr, p_value = stats.pearsonr(spec_data['total_payments'], spec_data['total_rx_cost'])
                    else:
                        corr, p_value = 0, 1
                    
                    # Compare with and without payments
                    with_payments = spec_data[spec_data['total_payments'] > 0]
                    without_payments = spec_data[spec_data['total_payments'] == 0]
                    
                    if len(with_payments) > 0 and len(without_payments) > 0:
                        influence_factor = (
                            with_payments['total_rx_cost'].mean() /
                            max(without_payments['total_rx_cost'].mean(), 1)
                        )
                    else:
                        influence_factor = 1
                    
                    correlations.append({
                        'specialty': specialty,
                        'providers': len(spec_data),
                        'providers_with_payments': len(with_payments),
                        'correlation': corr,
                        'p_value': p_value,
                        'influence_factor': influence_factor,
                        'avg_payment': spec_data['total_payments'].mean(),
                        'avg_rx_cost': spec_data['total_rx_cost'].mean()
                    })
        
        correlations_df = pd.DataFrame(correlations)
        if not correlations_df.empty:
            correlations_df = correlations_df.sort_values('influence_factor', ascending=False)
        
        logger.info(f"Analyzed correlations for {len(correlations_df)} specialties")
        return correlations_df
    
    def identify_outlier_specialties(self) -> Dict[str, pd.DataFrame]:
        """Identify specialties with unusual patterns"""
        outliers = {}
        
        overview = self.generate_specialty_overview()
        
        if not overview.empty:
            # High payment penetration
            if 'payment_penetration' in overview.columns:
                high_penetration = overview[
                    overview['payment_penetration'] > overview['payment_penetration'].quantile(0.90)
                ]
                outliers['high_payment_penetration'] = high_penetration[
                    ['payment_penetration', 'providers_with_payments', 'prescribers']
                ]
            
            # High payment to prescription ratio
            if 'payment_to_rx_ratio' in overview.columns:
                high_ratio = overview[
                    overview['payment_to_rx_ratio'] > overview['payment_to_rx_ratio'].quantile(0.90)
                ]
                outliers['high_payment_rx_ratio'] = high_ratio[
                    ['payment_to_rx_ratio', 'total_payments', 'total_rx_cost']
                ]
            
            # High average prescription cost
            if 'avg_rx_per_provider' in overview.columns:
                high_rx = overview[
                    overview['avg_rx_per_provider'] > overview['avg_rx_per_provider'].quantile(0.90)
                ]
                outliers['high_avg_rx_cost'] = high_rx[
                    ['avg_rx_per_provider', 'prescribers', 'total_rx_cost']
                ]
        
        logger.info(f"Identified {len(outliers)} types of outlier patterns")
        return outliers
    
    def analyze_specialty_drug_preferences(self, top_n: int = 10) -> Dict[str, pd.DataFrame]:
        """Analyze drug preferences by specialty"""
        if 'specialty' not in self.prescriptions.columns:
            logger.warning("Specialty not available for drug preference analysis")
            return {}
        
        preferences = {}
        
        # Get top specialties by prescription volume
        top_specialties = (
            self.prescriptions.groupby('specialty')['total_cost'].sum()
            .nlargest(top_n).index
        )
        
        for specialty in top_specialties:
            spec_data = self.prescriptions[self.prescriptions['specialty'] == specialty]
            
            # Top drugs for this specialty
            drug_prefs = spec_data.groupby('BRAND_NAME').agg({
                'total_cost': 'sum',
                'total_claims': 'sum',
                'NPI': 'nunique'
            }).round(2)
            
            drug_prefs.columns = ['total_cost', 'total_claims', 'prescribers']
            drug_prefs['market_share'] = (drug_prefs['total_cost'] / drug_prefs['total_cost'].sum() * 100).round(1)
            drug_prefs['avg_cost_per_claim'] = (drug_prefs['total_cost'] / drug_prefs['total_claims']).round(2)
            
            # Sort by total cost and get top drugs
            drug_prefs = drug_prefs.sort_values('total_cost', ascending=False).head(20)
            
            preferences[specialty] = drug_prefs
        
        logger.info(f"Analyzed drug preferences for {len(preferences)} specialties")
        return preferences
    
    def assess_specialty_vulnerability(self) -> pd.DataFrame:
        """Assess vulnerability to payment influence by specialty"""
        vulnerability = []
        
        correlations = self.analyze_specialty_correlations()
        
        if not correlations.empty:
            overview = self.generate_specialty_overview()
            
            for _, row in correlations.iterrows():
                specialty = row['specialty']
                
                # Get specialty overview data
                if not overview.empty and specialty in overview.index:
                    spec_overview = overview.loc[specialty]
                    
                    # Calculate vulnerability score
                    vulnerability_score = 0
                    
                    # High correlation
                    if row['correlation'] > 0.5:
                        vulnerability_score += 30
                    
                    # High influence factor
                    if row['influence_factor'] > 2:
                        vulnerability_score += 30
                    
                    # High payment penetration
                    if 'payment_penetration' in spec_overview and pd.notna(spec_overview['payment_penetration']) and spec_overview['payment_penetration'] > 75:
                        vulnerability_score += 20
                    
                    # Significant p-value
                    if row['p_value'] < 0.05:
                        vulnerability_score += 20
                    
                    vulnerability.append({
                        'specialty': specialty,
                        'vulnerability_score': vulnerability_score,
                        'correlation': row['correlation'],
                        'influence_factor': row['influence_factor'],
                        'providers': row['providers'],
                        'payment_penetration': spec_overview.get('payment_penetration', 0),
                        'risk_level': 'High' if vulnerability_score >= 70 else 'Medium' if vulnerability_score >= 40 else 'Low'
                    })
        
        vulnerability_df = pd.DataFrame(vulnerability)
        if not vulnerability_df.empty:
            vulnerability_df = vulnerability_df.sort_values('vulnerability_score', ascending=False)
        
        logger.info(f"Assessed vulnerability for {len(vulnerability_df)} specialties")
        return vulnerability_df
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """Get summary statistics for reporting"""
        if not self.results:
            self.analyze_all()
        
        summary = {
            'overview': self.results.get('specialty_overview', pd.DataFrame()).head(20).to_dict('records'),
            'payment_patterns': self.results.get('payment_patterns', pd.DataFrame()).head(10).to_dict('records'),
            'prescription_patterns': self.results.get('prescription_patterns', pd.DataFrame()).head(10).to_dict('records'),
            'correlations': self.results.get('specialty_correlations', pd.DataFrame()).head(10).to_dict('records'),
            'vulnerability': self.results.get('specialty_vulnerability', pd.DataFrame()).head(10).to_dict('records')
        }
        
        return summary