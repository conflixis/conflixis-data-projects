"""
Open Payments Analysis Module
Analyzes industry payments to healthcare providers
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class OpenPaymentsAnalyzer:
    """Comprehensive Open Payments analysis"""
    
    def __init__(self, data: pd.DataFrame):
        """
        Initialize analyzer with Open Payments data
        
        Args:
            data: DataFrame with Open Payments data
        """
        self.data = data
        self.results = {}
        
    def analyze_all(self) -> Dict[str, Any]:
        """
        Run all Open Payments analyses
        
        Returns:
            Dictionary with all analysis results
        """
        logger.info("Starting comprehensive Open Payments analysis")
        
        self.results['overall_metrics'] = self.calculate_overall_metrics()
        self.results['yearly_trends'] = self.analyze_yearly_trends()
        self.results['payment_categories'] = self.analyze_payment_categories()
        self.results['top_manufacturers'] = self.identify_top_manufacturers()
        self.results['payment_distribution'] = self.analyze_payment_distribution()
        self.results['provider_concentration'] = self.analyze_provider_concentration()
        self.results['consecutive_years'] = self.analyze_consecutive_years()
        
        logger.info("Open Payments analysis complete")
        return self.results
    
    def calculate_overall_metrics(self) -> Dict[str, Any]:
        """Calculate overall payment metrics"""
        metrics = {
            'unique_providers': self.data['physician_id'].nunique(),
            'total_transactions': len(self.data),
            'total_payments': self.data['total_amount'].sum(),
            'avg_payment': self.data['total_amount'].mean(),
            'median_payment': self.data['total_amount'].median(),
            'max_payment': self.data['total_amount'].max(),
            'min_payment': self.data['total_amount'].min(),
            'std_payment': self.data['total_amount'].std()
        }
        
        # Payment distribution percentiles
        percentiles = [10, 25, 50, 75, 90, 95, 99]
        for p in percentiles:
            metrics[f'p{p}_payment'] = self.data['total_amount'].quantile(p/100)
        
        # Provider-level metrics
        provider_totals = self.data.groupby('physician_id')['total_amount'].sum()
        metrics['avg_per_provider'] = provider_totals.mean()
        metrics['median_per_provider'] = provider_totals.median()
        metrics['max_per_provider'] = provider_totals.max()
        
        logger.info(f"Calculated metrics for {metrics['unique_providers']:,} providers")
        return metrics
    
    def analyze_yearly_trends(self) -> pd.DataFrame:
        """Analyze payment trends over years"""
        yearly = self.data.groupby('payment_year').agg({
            'physician_id': 'nunique',
            'total_amount': ['sum', 'mean', 'median'],
            'payment_count': 'sum'
        }).round(2)
        
        yearly.columns = ['providers', 'total_payments', 'avg_payment', 'median_payment', 'transaction_count']
        
        # Calculate year-over-year growth
        yearly['yoy_growth'] = yearly['total_payments'].pct_change() * 100
        yearly['provider_growth'] = yearly['providers'].pct_change() * 100
        
        logger.info(f"Analyzed trends across {len(yearly)} years")
        return yearly
    
    def analyze_payment_categories(self) -> pd.DataFrame:
        """Analyze payments by category"""
        categories = self.data.groupby('payment_category').agg({
            'total_amount': ['sum', 'mean', 'count'],
            'physician_id': 'nunique'
        }).round(2)
        
        categories.columns = ['total_amount', 'avg_amount', 'transaction_count', 'unique_providers']
        categories['pct_of_total'] = (categories['total_amount'] / categories['total_amount'].sum() * 100).round(1)
        
        # Sort by total amount
        categories = categories.sort_values('total_amount', ascending=False)
        
        logger.info(f"Analyzed {len(categories)} payment categories")
        return categories
    
    def identify_top_manufacturers(self, n: int = 20) -> pd.DataFrame:
        """
        Identify top manufacturers by payment volume
        
        Args:
            n: Number of top manufacturers to return
            
        Returns:
            DataFrame with top manufacturers
        """
        manufacturers = self.data.groupby('manufacturer').agg({
            'total_amount': 'sum',
            'physician_id': 'nunique',
            'payment_count': 'sum'
        }).round(2)
        
        manufacturers.columns = ['total_payments', 'unique_providers', 'transaction_count']
        manufacturers['avg_per_provider'] = (
            manufacturers['total_payments'] / manufacturers['unique_providers']
        ).round(2)
        
        # Calculate market share
        manufacturers['market_share'] = (
            manufacturers['total_payments'] / manufacturers['total_payments'].sum() * 100
        ).round(2)
        
        # Sort and get top N
        manufacturers = manufacturers.sort_values('total_payments', ascending=False).head(n)
        
        logger.info(f"Identified top {n} manufacturers")
        return manufacturers
    
    def analyze_payment_distribution(self) -> Dict[str, Any]:
        """Analyze distribution of payments"""
        distribution = {}
        
        # Payment tiers
        tiers = [
            (0, 100, '$0-100'),
            (100, 500, '$100-500'),
            (500, 1000, '$500-1K'),
            (1000, 5000, '$1K-5K'),
            (5000, 10000, '$5K-10K'),
            (10000, 50000, '$10K-50K'),
            (50000, float('inf'), '$50K+')
        ]
        
        tier_stats = []
        for min_val, max_val, label in tiers:
            mask = (self.data['total_amount'] > min_val) & (self.data['total_amount'] <= max_val)
            tier_data = self.data[mask]
            
            if not tier_data.empty:
                tier_stats.append({
                    'tier': label,
                    'min': min_val,
                    'max': max_val if max_val != float('inf') else None,
                    'count': len(tier_data),
                    'providers': tier_data['physician_id'].nunique(),
                    'total_amount': tier_data['total_amount'].sum(),
                    'avg_amount': tier_data['total_amount'].mean(),
                    'pct_of_transactions': len(tier_data) / len(self.data) * 100,
                    'pct_of_total_amount': tier_data['total_amount'].sum() / self.data['total_amount'].sum() * 100
                })
        
        distribution['tiers'] = pd.DataFrame(tier_stats)
        
        # Concentration metrics
        provider_totals = self.data.groupby('physician_id')['total_amount'].sum().sort_values(ascending=False)
        
        # Gini coefficient
        distribution['gini_coefficient'] = self._calculate_gini(provider_totals.values)
        
        # Top percentile concentration
        for pct in [1, 5, 10, 20]:
            n_providers = int(len(provider_totals) * pct / 100)
            top_sum = provider_totals.head(n_providers).sum()
            distribution[f'top_{pct}pct_share'] = top_sum / provider_totals.sum() * 100
        
        logger.info("Analyzed payment distribution")
        return distribution
    
    def analyze_provider_concentration(self) -> pd.DataFrame:
        """Analyze provider-level payment concentration"""
        # Aggregate by provider
        provider_summary = self.data.groupby('physician_id').agg({
            'total_amount': 'sum',
            'payment_count': 'sum',
            'manufacturer': 'nunique',
            'payment_category': 'nunique',
            'payment_year': 'nunique'
        }).round(2)
        
        provider_summary.columns = [
            'total_received',
            'transaction_count', 
            'unique_manufacturers',
            'unique_categories',
            'years_active'
        ]
        
        # Calculate averages
        provider_summary['avg_per_transaction'] = (
            provider_summary['total_received'] / provider_summary['transaction_count']
        ).round(2)
        
        # Categorize providers
        provider_summary['payment_tier'] = pd.cut(
            provider_summary['total_received'],
            bins=[0, 1000, 5000, 10000, 50000, float('inf')],
            labels=['<$1K', '$1-5K', '$5-10K', '$10-50K', '$50K+']
        )
        
        # Get summary statistics by tier
        tier_summary = provider_summary.groupby('payment_tier').agg({
            'total_received': ['count', 'sum', 'mean'],
            'transaction_count': 'mean',
            'unique_manufacturers': 'mean',
            'years_active': 'mean'
        }).round(2)
        
        logger.info(f"Analyzed concentration for {len(provider_summary)} providers")
        return tier_summary
    
    def analyze_consecutive_years(self) -> pd.DataFrame:
        """Analyze providers receiving payments in consecutive years"""
        # Get unique years per provider
        provider_years = self.data.groupby('physician_id')['payment_year'].apply(set)
        
        # Count consecutive year patterns
        consecutive_counts = {}
        for years in provider_years:
            years_list = sorted(list(years))
            max_consecutive = 1
            current_consecutive = 1
            
            for i in range(1, len(years_list)):
                if years_list[i] == years_list[i-1] + 1:
                    current_consecutive += 1
                    max_consecutive = max(max_consecutive, current_consecutive)
                else:
                    current_consecutive = 1
            
            if max_consecutive not in consecutive_counts:
                consecutive_counts[max_consecutive] = 0
            consecutive_counts[max_consecutive] += 1
        
        # Create summary DataFrame
        consecutive_df = pd.DataFrame([
            {'consecutive_years': k, 'provider_count': v}
            for k, v in consecutive_counts.items()
        ]).sort_values('consecutive_years')
        
        # Add payment statistics for each group
        for years in consecutive_df['consecutive_years'].unique():
            providers = []
            for pid, pid_years in provider_years.items():
                years_list = sorted(list(pid_years))
                max_consecutive = 1
                current_consecutive = 1
                
                for i in range(1, len(years_list)):
                    if years_list[i] == years_list[i-1] + 1:
                        current_consecutive += 1
                        max_consecutive = max(max_consecutive, current_consecutive)
                    else:
                        current_consecutive = 1
                
                if max_consecutive == years:
                    providers.append(pid)
            
            if providers:
                provider_data = self.data[self.data['physician_id'].isin(providers)]
                avg_payment = provider_data.groupby('physician_id')['total_amount'].sum().mean()
                consecutive_df.loc[
                    consecutive_df['consecutive_years'] == years, 
                    'avg_total_payment'
                ] = avg_payment
        
        logger.info(f"Analyzed consecutive year patterns")
        return consecutive_df
    
    def _calculate_gini(self, values: np.ndarray) -> float:
        """
        Calculate Gini coefficient for inequality measurement
        
        Args:
            values: Array of values
            
        Returns:
            Gini coefficient (0 = perfect equality, 1 = perfect inequality)
        """
        sorted_values = np.sort(values)
        n = len(values)
        cumsum = np.cumsum(sorted_values)
        return (2 * np.sum((np.arange(1, n+1)) * sorted_values)) / (n * cumsum[-1]) - (n + 1) / n
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """Get summary statistics for reporting"""
        if not self.results:
            self.analyze_all()
        
        summary = {
            'metrics': self.results.get('overall_metrics', {}),
            'trends': self.results.get('yearly_trends', pd.DataFrame()).to_dict('records'),
            'top_categories': self.results.get('payment_categories', pd.DataFrame()).head(5).to_dict('records'),
            'top_manufacturers': self.results.get('top_manufacturers', pd.DataFrame()).head(10).to_dict('records'),
            'distribution': self.results.get('payment_distribution', {})
        }
        
        return summary