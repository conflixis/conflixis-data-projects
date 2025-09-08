"""
Prescription Analysis Module
Analyzes Medicare Part D prescription patterns
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class PrescriptionAnalyzer:
    """Comprehensive prescription pattern analysis"""
    
    def __init__(self, data: pd.DataFrame):
        """
        Initialize analyzer with prescription data
        
        Args:
            data: DataFrame with prescription data
        """
        self.data = data
        self.results = {}
        
    def analyze_all(self) -> Dict[str, Any]:
        """
        Run all prescription analyses
        
        Returns:
            Dictionary with all analysis results
        """
        logger.info("Starting comprehensive prescription analysis")
        
        self.results['overall_metrics'] = self.calculate_overall_metrics()
        self.results['yearly_trends'] = self.analyze_yearly_trends()
        self.results['top_drugs'] = self.identify_top_drugs()
        self.results['high_cost_drugs'] = self.analyze_high_cost_drugs()
        self.results['specialty_patterns'] = self.analyze_by_specialty()
        self.results['provider_type_patterns'] = self.analyze_by_provider_type()
        self.results['drug_categories'] = self.categorize_drugs()
        self.results['outlier_prescribers'] = self.identify_outlier_prescribers()
        
        logger.info("Prescription analysis complete")
        return self.results
    
    def calculate_overall_metrics(self) -> Dict[str, Any]:
        """Calculate overall prescription metrics"""
        metrics = {
            'unique_prescribers': self.data['NPI'].nunique(),
            'total_prescriptions': self.data['total_claims'].sum(),
            'total_prescription_value': self.data['total_cost'].sum(),
            'unique_drugs': self.data['BRAND_NAME'].nunique(),
            'unique_generics': self.data['GENERIC_NAME'].nunique(),
            'total_beneficiaries': self.data['total_beneficiaries'].sum(),
            'avg_cost_per_prescription': (
                self.data['total_cost'].sum() / self.data['total_claims'].sum()
            ),
            'avg_days_supply': (
                self.data['total_days_supply'].sum() / self.data['total_claims'].sum()
            )
        }
        
        # Provider-level metrics
        provider_totals = self.data.groupby('NPI').agg({
            'total_claims': 'sum',
            'total_cost': 'sum',
            'total_beneficiaries': 'sum'
        })
        
        metrics['avg_claims_per_provider'] = provider_totals['total_claims'].mean()
        metrics['median_claims_per_provider'] = provider_totals['total_claims'].median()
        metrics['avg_cost_per_provider'] = provider_totals['total_cost'].mean()
        metrics['median_cost_per_provider'] = provider_totals['total_cost'].median()
        
        # Distribution metrics
        percentiles = [10, 25, 50, 75, 90, 95, 99]
        for p in percentiles:
            metrics[f'p{p}_provider_cost'] = provider_totals['total_cost'].quantile(p/100)
        
        logger.info(f"Calculated metrics for {metrics['unique_prescribers']:,} prescribers")
        return metrics
    
    def analyze_yearly_trends(self) -> pd.DataFrame:
        """Analyze prescription trends over years"""
        yearly = self.data.groupby('rx_year').agg({
            'NPI': 'nunique',
            'total_claims': 'sum',
            'total_cost': 'sum',
            'total_beneficiaries': 'sum',
            'BRAND_NAME': 'nunique'
        }).round(2)
        
        yearly.columns = ['prescribers', 'total_claims', 'total_cost', 
                         'total_beneficiaries', 'unique_drugs']
        
        # Calculate derived metrics
        yearly['avg_cost_per_claim'] = yearly['total_cost'] / yearly['total_claims']
        yearly['avg_claims_per_prescriber'] = yearly['total_claims'] / yearly['prescribers']
        
        # Year-over-year growth
        yearly['cost_yoy_growth'] = yearly['total_cost'].pct_change() * 100
        yearly['volume_yoy_growth'] = yearly['total_claims'].pct_change() * 100
        yearly['prescriber_yoy_growth'] = yearly['prescribers'].pct_change() * 100
        
        logger.info(f"Analyzed trends across {len(yearly)} years")
        return yearly
    
    def identify_top_drugs(self, n: int = 50) -> pd.DataFrame:
        """
        Identify top prescribed drugs by various metrics
        
        Args:
            n: Number of top drugs to return
            
        Returns:
            DataFrame with top drugs
        """
        drug_stats = self.data.groupby('BRAND_NAME').agg({
            'total_claims': 'sum',
            'total_cost': 'sum',
            'total_beneficiaries': 'sum',
            'NPI': 'nunique',
            'GENERIC_NAME': 'first'
        }).round(2)
        
        drug_stats.columns = ['total_claims', 'total_cost', 'total_beneficiaries',
                              'unique_prescribers', 'generic_name']
        
        # Calculate derived metrics
        drug_stats['avg_cost_per_claim'] = (
            drug_stats['total_cost'] / drug_stats['total_claims']
        ).round(2)
        
        drug_stats['market_share_by_cost'] = (
            drug_stats['total_cost'] / drug_stats['total_cost'].sum() * 100
        ).round(2)
        
        drug_stats['market_share_by_volume'] = (
            drug_stats['total_claims'] / drug_stats['total_claims'].sum() * 100
        ).round(2)
        
        # Sort by total cost and get top N
        top_drugs = drug_stats.sort_values('total_cost', ascending=False).head(n)
        
        logger.info(f"Identified top {n} drugs by cost")
        return top_drugs
    
    def analyze_high_cost_drugs(self, cost_threshold: float = 1000) -> pd.DataFrame:
        """
        Analyze high-cost drugs
        
        Args:
            cost_threshold: Minimum average cost per claim to be considered high-cost
            
        Returns:
            DataFrame with high-cost drug analysis
        """
        # Calculate average cost per claim for each drug
        drug_costs = self.data.groupby('BRAND_NAME').agg({
            'total_cost': 'sum',
            'total_claims': 'sum',
            'NPI': 'nunique',
            'GENERIC_NAME': 'first'
        })
        
        drug_costs['avg_cost_per_claim'] = (
            drug_costs['total_cost'] / drug_costs['total_claims']
        )
        
        # Filter high-cost drugs
        high_cost = drug_costs[drug_costs['avg_cost_per_claim'] >= cost_threshold].copy()
        
        high_cost = high_cost.sort_values('avg_cost_per_claim', ascending=False)
        high_cost.columns = ['total_cost', 'total_claims', 'prescribers', 
                             'generic_name', 'avg_cost_per_claim']
        
        # Add therapeutic category if available
        high_cost['cost_category'] = pd.cut(
            high_cost['avg_cost_per_claim'],
            bins=[0, 1000, 5000, 10000, 50000, float('inf')],
            labels=['$1K-5K', '$5K-10K', '$10K-50K', '$50K+', 'Ultra-high']
        )
        
        logger.info(f"Identified {len(high_cost)} high-cost drugs (>${cost_threshold}/claim)")
        return high_cost
    
    def analyze_by_specialty(self) -> pd.DataFrame:
        """Analyze prescription patterns by specialty"""
        if 'specialty' not in self.data.columns:
            logger.warning("Specialty column not found in data")
            return pd.DataFrame()
        
        specialty_stats = self.data.groupby('specialty').agg({
            'NPI': 'nunique',
            'total_claims': 'sum',
            'total_cost': 'sum',
            'total_beneficiaries': 'sum',
            'BRAND_NAME': 'nunique'
        }).round(2)
        
        specialty_stats.columns = ['prescribers', 'total_claims', 'total_cost',
                                  'beneficiaries', 'unique_drugs']
        
        # Calculate per-provider metrics
        specialty_stats['avg_claims_per_provider'] = (
            specialty_stats['total_claims'] / specialty_stats['prescribers']
        ).round(0)
        
        specialty_stats['avg_cost_per_provider'] = (
            specialty_stats['total_cost'] / specialty_stats['prescribers']
        ).round(0)
        
        specialty_stats['avg_cost_per_claim'] = (
            specialty_stats['total_cost'] / specialty_stats['total_claims']
        ).round(2)
        
        # Sort by total cost
        specialty_stats = specialty_stats.sort_values('total_cost', ascending=False)
        
        logger.info(f"Analyzed {len(specialty_stats)} specialties")
        return specialty_stats
    
    def analyze_by_provider_type(self) -> pd.DataFrame:
        """Analyze prescription patterns by provider type (MD, NP, PA, etc.)"""
        if 'provider_type' not in self.data.columns:
            logger.warning("Provider type column not found in data")
            return pd.DataFrame()
        
        provider_type_stats = self.data.groupby('provider_type').agg({
            'NPI': 'nunique',
            'total_claims': 'sum',
            'total_cost': 'sum',
            'total_beneficiaries': 'sum',
            'BRAND_NAME': 'nunique'
        }).round(2)
        
        provider_type_stats.columns = ['providers', 'total_claims', 'total_cost',
                                       'beneficiaries', 'unique_drugs']
        
        # Calculate per-provider metrics
        provider_type_stats['avg_claims_per_provider'] = (
            provider_type_stats['total_claims'] / provider_type_stats['providers']
        ).round(0)
        
        provider_type_stats['avg_cost_per_provider'] = (
            provider_type_stats['total_cost'] / provider_type_stats['providers']
        ).round(0)
        
        # Market share
        provider_type_stats['cost_market_share'] = (
            provider_type_stats['total_cost'] / provider_type_stats['total_cost'].sum() * 100
        ).round(1)
        
        logger.info(f"Analyzed {len(provider_type_stats)} provider types")
        return provider_type_stats
    
    def categorize_drugs(self) -> Dict[str, pd.DataFrame]:
        """
        Categorize drugs into therapeutic classes
        
        Returns:
            Dictionary of drug categories with statistics
        """
        categories = {}
        
        # Common drug categories based on naming patterns
        category_patterns = {
            'Diabetes': ['METFORMIN', 'INSULIN', 'OZEMPIC', 'JARDIANCE', 'TRULICITY', 
                        'MOUNJARO', 'JANUVIA', 'FARXIGA', 'GLUCOPHAGE'],
            'Anticoagulants': ['ELIQUIS', 'XARELTO', 'WARFARIN', 'PRADAXA', 'COUMADIN'],
            'Statins': ['ATORVASTATIN', 'SIMVASTATIN', 'ROSUVASTATIN', 'PRAVASTATIN',
                       'LIPITOR', 'CRESTOR', 'ZOCOR'],
            'Antibiotics': ['AMOXICILLIN', 'AZITHROMYCIN', 'CIPROFLOXACIN', 'DOXYCYCLINE',
                          'CEPHALEXIN', 'LEVOFLOXACIN'],
            'Pain_Management': ['OXYCODONE', 'HYDROCODONE', 'TRAMADOL', 'MORPHINE',
                              'FENTANYL', 'GABAPENTIN', 'PREGABALIN'],
            'Mental_Health': ['SERTRALINE', 'ESCITALOPRAM', 'FLUOXETINE', 'CITALOPRAM',
                            'DULOXETINE', 'VENLAFAXINE', 'BUPROPION'],
            'Respiratory': ['ALBUTEROL', 'ADVAIR', 'SYMBICORT', 'SPIRIVA', 'TRELEGY'],
            'Biologics': ['HUMIRA', 'ENBREL', 'REMICADE', 'STELARA', 'COSENTYX']
        }
        
        for category, patterns in category_patterns.items():
            # Find drugs matching patterns
            mask = self.data['BRAND_NAME'].str.upper().str.contains('|'.join(patterns), na=False)
            category_data = self.data[mask]
            
            if not category_data.empty:
                category_stats = category_data.groupby('BRAND_NAME').agg({
                    'total_claims': 'sum',
                    'total_cost': 'sum',
                    'NPI': 'nunique'
                }).round(2)
                
                category_stats.columns = ['total_claims', 'total_cost', 'prescribers']
                category_stats = category_stats.sort_values('total_cost', ascending=False)
                
                categories[category] = category_stats
                logger.info(f"Categorized {len(category_stats)} drugs in {category}")
        
        return categories
    
    def identify_outlier_prescribers(self, std_threshold: float = 3.0) -> pd.DataFrame:
        """
        Identify prescribers with unusual patterns
        
        Args:
            std_threshold: Number of standard deviations from mean to flag as outlier
            
        Returns:
            DataFrame with outlier prescribers
        """
        # Aggregate by prescriber
        prescriber_stats = self.data.groupby('NPI').agg({
            'total_claims': 'sum',
            'total_cost': 'sum',
            'BRAND_NAME': 'nunique',
            'total_beneficiaries': 'sum',
            'PROVIDER_LAST_NAME': 'first',
            'PROVIDER_FIRST_NAME': 'first',
            'specialty': lambda x: x.mode()[0] if not x.empty and len(x.mode()) > 0 else 'Unknown'
        }).round(2)
        
        # Calculate z-scores for key metrics
        for col in ['total_claims', 'total_cost']:
            mean = prescriber_stats[col].mean()
            std = prescriber_stats[col].std()
            prescriber_stats[f'{col}_zscore'] = (prescriber_stats[col] - mean) / std
        
        # Identify outliers
        outliers = prescriber_stats[
            (abs(prescriber_stats['total_claims_zscore']) > std_threshold) |
            (abs(prescriber_stats['total_cost_zscore']) > std_threshold)
        ].copy()
        
        # Sort by total cost z-score
        outliers = outliers.sort_values('total_cost_zscore', ascending=False)
        
        # Clean up columns
        outliers.columns = ['total_claims', 'total_cost', 'unique_drugs', 'beneficiaries',
                           'last_name', 'first_name', 'specialty', 
                           'claims_zscore', 'cost_zscore']
        
        logger.info(f"Identified {len(outliers)} outlier prescribers")
        return outliers
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """Get summary statistics for reporting"""
        if not self.results:
            self.analyze_all()
        
        summary = {
            'metrics': self.results.get('overall_metrics', {}),
            'trends': self.results.get('yearly_trends', pd.DataFrame()).to_dict('records'),
            'top_drugs': self.results.get('top_drugs', pd.DataFrame()).head(20).to_dict('records'),
            'high_cost_drugs': self.results.get('high_cost_drugs', pd.DataFrame()).head(10).to_dict('records'),
            'specialty_patterns': self.results.get('specialty_patterns', pd.DataFrame()).head(10).to_dict('records')
        }
        
        return summary