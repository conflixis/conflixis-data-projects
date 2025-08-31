"""
Risk Scoring Module
Advanced risk assessment for healthcare providers
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class RiskScorer:
    """Multi-factor risk scoring and anomaly detection"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize risk scorer with configuration
        
        Args:
            config: Configuration dictionary with risk thresholds
        """
        self.config = config
        self.thresholds = config.get('thresholds', {})
        self.risk_scores = None
        self.anomaly_detector = None
        
    def score_providers(
        self, 
        payments_data: pd.DataFrame,
        prescription_data: pd.DataFrame,
        correlation_data: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        Calculate comprehensive risk scores for providers
        
        Args:
            payments_data: Open Payments data
            prescription_data: Prescription data
            correlation_data: Optional correlation analysis results
            
        Returns:
            DataFrame with risk scores and components
        """
        logger.info("Starting provider risk scoring")
        
        # Prepare base data
        risk_df = self._prepare_provider_data(payments_data, prescription_data)
        
        # Calculate individual risk components
        risk_df['payment_risk'] = self._calculate_payment_risk(risk_df)
        risk_df['prescription_risk'] = self._calculate_prescription_risk(risk_df)
        risk_df['relationship_risk'] = self._calculate_relationship_risk(risk_df)
        risk_df['behavioral_risk'] = self._calculate_behavioral_risk(risk_df)
        
        # Add correlation-based risk if available
        if correlation_data is not None:
            risk_df['correlation_risk'] = self._calculate_correlation_risk(
                risk_df, correlation_data
            )
        else:
            risk_df['correlation_risk'] = 50  # Default medium risk
        
        # Calculate composite risk score
        risk_components = [
            ('payment_risk', 0.25),
            ('prescription_risk', 0.25),
            ('relationship_risk', 0.20),
            ('behavioral_risk', 0.15),
            ('correlation_risk', 0.15)
        ]
        
        risk_df['composite_risk_score'] = sum(
            risk_df[component] * weight 
            for component, weight in risk_components
        )
        
        # Categorize risk levels
        risk_df['risk_level'] = pd.cut(
            risk_df['composite_risk_score'],
            bins=[0, 30, 60, 80, 90, 100],
            labels=['Low', 'Medium', 'High', 'Critical', 'Extreme']
        )
        
        # Detect anomalies
        risk_df['is_anomaly'] = self._detect_anomalies(risk_df)
        
        # Sort by risk score
        risk_df = risk_df.sort_values('composite_risk_score', ascending=False)
        
        self.risk_scores = risk_df
        logger.info(f"Calculated risk scores for {len(risk_df)} providers")
        
        return risk_df
    
    def _prepare_provider_data(
        self, 
        payments: pd.DataFrame,
        prescriptions: pd.DataFrame
    ) -> pd.DataFrame:
        """Prepare merged provider data for risk scoring"""
        # Aggregate payments by provider
        payment_agg = payments.groupby('physician_id').agg({
            'total_amount': 'sum',
            'payment_count': 'sum',
            'manufacturer': 'nunique',
            'payment_category': 'nunique',
            'payment_year': 'nunique',
            'max_amount': 'max'
        }).reset_index()
        
        payment_agg.columns = ['NPI', 'total_payments', 'payment_count',
                               'unique_manufacturers', 'payment_categories',
                               'payment_years', 'max_single_payment']
        
        # Aggregate prescriptions by provider
        rx_agg = prescriptions.groupby('NPI').agg({
            'total_claims': 'sum',
            'total_cost': 'sum',
            'BRAND_NAME': 'nunique',
            'total_beneficiaries': 'sum'
        }).reset_index()
        
        rx_agg.columns = ['NPI', 'total_rx_claims', 'total_rx_cost',
                          'unique_drugs', 'total_beneficiaries']
        
        # Merge data
        risk_df = pd.merge(rx_agg, payment_agg, on='NPI', how='outer')
        
        # Fill missing values
        payment_cols = ['total_payments', 'payment_count', 'unique_manufacturers',
                       'payment_categories', 'payment_years', 'max_single_payment']
        risk_df[payment_cols] = risk_df[payment_cols].fillna(0)
        
        rx_cols = ['total_rx_claims', 'total_rx_cost', 'unique_drugs', 'total_beneficiaries']
        risk_df[rx_cols] = risk_df[rx_cols].fillna(0)
        
        return risk_df
    
    def _calculate_payment_risk(self, df: pd.DataFrame) -> pd.Series:
        """Calculate payment-based risk component (0-100)"""
        risk = pd.Series(index=df.index, dtype=float)
        
        # High single payment risk
        high_payment_threshold = self.thresholds.get('payment', {}).get('high_single_payment', 5000)
        risk += (df['max_single_payment'] > high_payment_threshold) * 20
        
        # High annual total risk
        high_annual_threshold = self.thresholds.get('payment', {}).get('high_annual_total', 10000)
        risk += (df['total_payments'] > high_annual_threshold) * 20
        
        # Payment frequency risk (many small payments)
        avg_payment = df['total_payments'] / df['payment_count'].replace(0, 1)
        risk += (avg_payment < 100) & (df['payment_count'] > 50) * 15
        
        # Multiple manufacturer relationships
        risk += np.clip(df['unique_manufacturers'] / 10 * 15, 0, 15)
        
        # Multiple payment categories
        risk += np.clip(df['payment_categories'] / 5 * 10, 0, 10)
        
        # Sustained relationships (multiple years)
        risk += np.clip(df['payment_years'] / 5 * 20, 0, 20)
        
        return np.clip(risk, 0, 100)
    
    def _calculate_prescription_risk(self, df: pd.DataFrame) -> pd.Series:
        """Calculate prescription-based risk component (0-100)"""
        risk = pd.Series(index=df.index, dtype=float)
        
        # High volume prescribing
        volume_percentile = df['total_rx_claims'].quantile(0.90)
        risk += (df['total_rx_claims'] > volume_percentile) * 20
        
        # High cost prescribing
        cost_percentile = df['total_rx_cost'].quantile(0.90)
        risk += (df['total_rx_cost'] > cost_percentile) * 25
        
        # Cost per claim (expensive drugs)
        avg_cost_per_claim = df['total_rx_cost'] / df['total_rx_claims'].replace(0, 1)
        high_cost_threshold = avg_cost_per_claim.quantile(0.90)
        risk += (avg_cost_per_claim > high_cost_threshold) * 20
        
        # Limited drug diversity (potential favoritism)
        drug_diversity = df['unique_drugs'] / df['total_rx_claims'].replace(0, 1)
        risk += (drug_diversity < 0.01) & (df['total_rx_claims'] > 100) * 15
        
        # Unusual beneficiary patterns
        claims_per_beneficiary = df['total_rx_claims'] / df['total_beneficiaries'].replace(0, 1)
        unusual_threshold = claims_per_beneficiary.quantile(0.95)
        risk += (claims_per_beneficiary > unusual_threshold) * 20
        
        return np.clip(risk, 0, 100)
    
    def _calculate_relationship_risk(self, df: pd.DataFrame) -> pd.Series:
        """Calculate relationship pattern risk (0-100)"""
        risk = pd.Series(index=df.index, dtype=float)
        
        # Concentration risk (few manufacturers dominate)
        # This would require more detailed data, so using proxy
        manufacturer_concentration = 1 / df['unique_manufacturers'].replace(0, 1)
        risk += np.clip(manufacturer_concentration * 30, 0, 30)
        
        # Payment to prescription ratio
        payment_rx_ratio = df['total_payments'] / df['total_rx_cost'].replace(0, 1)
        suspicious_ratio = payment_rx_ratio.quantile(0.95)
        risk += (payment_rx_ratio > suspicious_ratio) * 30
        
        # Sustained high-value relationships
        multi_year_high_payment = (df['payment_years'] >= 3) & (df['total_payments'] > 5000)
        risk += multi_year_high_payment * 40
        
        return np.clip(risk, 0, 100)
    
    def _calculate_behavioral_risk(self, df: pd.DataFrame) -> pd.Series:
        """Calculate behavioral pattern risk (0-100)"""
        risk = pd.Series(index=df.index, dtype=float)
        
        # Sudden changes in prescribing volume
        # This would require time series data, using proxy
        high_volume = df['total_rx_claims'] > df['total_rx_claims'].quantile(0.75)
        high_payment = df['total_payments'] > df['total_payments'].quantile(0.75)
        risk += (high_volume & high_payment) * 30
        
        # Unusual payment patterns (many small payments)
        if 'payment_count' in df.columns:
            small_payment_pattern = (
                (df['payment_count'] > 20) & 
                (df['total_payments'] / df['payment_count'].replace(0, 1) < 50)
            )
            risk += small_payment_pattern * 35
        
        # Exclusive relationships
        exclusive = (df['unique_manufacturers'] == 1) & (df['total_payments'] > 1000)
        risk += exclusive * 35
        
        return np.clip(risk, 0, 100)
    
    def _calculate_correlation_risk(
        self, 
        df: pd.DataFrame,
        correlation_data: pd.DataFrame
    ) -> pd.Series:
        """Calculate correlation-based risk (0-100)"""
        risk = pd.Series(index=df.index, dtype=float)
        
        # This would use correlation analysis results
        # For now, using payment presence as proxy
        has_payments = df['total_payments'] > 0
        risk += has_payments * 50
        
        # High payment correlation
        high_payment_providers = df['total_payments'] > df['total_payments'].quantile(0.75)
        high_rx_providers = df['total_rx_cost'] > df['total_rx_cost'].quantile(0.75)
        risk += (high_payment_providers & high_rx_providers) * 50
        
        return np.clip(risk, 0, 100)
    
    def _detect_anomalies(self, df: pd.DataFrame) -> pd.Series:
        """
        Detect anomalous providers using Isolation Forest
        
        Args:
            df: DataFrame with risk components
            
        Returns:
            Series with anomaly flags
        """
        # Select features for anomaly detection
        features = [
            'total_payments', 'payment_count', 'total_rx_claims',
            'total_rx_cost', 'unique_manufacturers', 'unique_drugs'
        ]
        
        # Prepare data
        X = df[features].fillna(0)
        
        # Standardize features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Train Isolation Forest
        self.anomaly_detector = IsolationForest(
            contamination=0.05,  # Expect 5% anomalies
            random_state=42
        )
        
        # Predict anomalies (-1 for anomaly, 1 for normal)
        predictions = self.anomaly_detector.fit_predict(X_scaled)
        
        return predictions == -1
    
    def get_high_risk_providers(self, top_n: int = 100) -> pd.DataFrame:
        """
        Get highest risk providers
        
        Args:
            top_n: Number of providers to return
            
        Returns:
            DataFrame with high-risk providers
        """
        if self.risk_scores is None:
            raise ValueError("Risk scores not calculated. Run score_providers first.")
        
        high_risk = self.risk_scores.nlargest(top_n, 'composite_risk_score')
        
        # Select key columns for reporting
        report_columns = [
            'NPI', 'composite_risk_score', 'risk_level',
            'total_payments', 'total_rx_cost', 'payment_risk',
            'prescription_risk', 'relationship_risk', 'is_anomaly'
        ]
        
        return high_risk[report_columns]
    
    def get_risk_distribution(self) -> pd.DataFrame:
        """Get distribution of risk levels"""
        if self.risk_scores is None:
            raise ValueError("Risk scores not calculated. Run score_providers first.")
        
        distribution = self.risk_scores['risk_level'].value_counts().reset_index()
        distribution.columns = ['risk_level', 'provider_count']
        
        # Add percentages
        distribution['percentage'] = (
            distribution['provider_count'] / len(self.risk_scores) * 100
        ).round(1)
        
        # Add summary statistics
        distribution = distribution.append({
            'risk_level': 'Mean Score',
            'provider_count': len(self.risk_scores),
            'percentage': self.risk_scores['composite_risk_score'].mean()
        }, ignore_index=True)
        
        return distribution
    
    def generate_risk_report(self) -> Dict[str, Any]:
        """Generate comprehensive risk assessment report"""
        if self.risk_scores is None:
            raise ValueError("Risk scores not calculated. Run score_providers first.")
        
        report = {
            'summary': {
                'total_providers': len(self.risk_scores),
                'mean_risk_score': self.risk_scores['composite_risk_score'].mean(),
                'median_risk_score': self.risk_scores['composite_risk_score'].median(),
                'high_risk_count': len(self.risk_scores[self.risk_scores['risk_level'].isin(['High', 'Critical', 'Extreme'])]),
                'anomaly_count': self.risk_scores['is_anomaly'].sum()
            },
            'distribution': self.get_risk_distribution().to_dict('records'),
            'top_risks': self.get_high_risk_providers(20).to_dict('records'),
            'risk_components': {
                'payment_risk_mean': self.risk_scores['payment_risk'].mean(),
                'prescription_risk_mean': self.risk_scores['prescription_risk'].mean(),
                'relationship_risk_mean': self.risk_scores['relationship_risk'].mean(),
                'behavioral_risk_mean': self.risk_scores['behavioral_risk'].mean(),
                'correlation_risk_mean': self.risk_scores['correlation_risk'].mean()
            }
        }
        
        logger.info("Generated risk assessment report")
        return report