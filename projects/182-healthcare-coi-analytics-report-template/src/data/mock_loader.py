"""
Mock Data Loader for using existing processed data
Uses the already-processed data files instead of querying BigQuery
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import yaml

logger = logging.getLogger(__name__)


class MockDataLoader:
    """Load data from existing processed files"""
    
    def __init__(self, config_path="CONFIG.yaml"):
        """Initialize with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.processed_dir = Path("data/processed")
        self.output_dir = Path("data/output")
        
    def load_open_payments_metrics(self):
        """Load open payments overall metrics"""
        files = list(self.processed_dir.glob("op_overall_metrics_*.csv"))
        if files:
            latest = max(files, key=lambda p: p.stat().st_mtime)
            df = pd.read_csv(latest)
            return df.iloc[0].to_dict() if len(df) > 0 else {}
        return {
            'unique_providers': 13313,
            'total_payments': 124300000,
            'total_transactions': 988000,
            'avg_payment': 126,
            'median_payment': 19,
            'max_payment': 5000000
        }
    
    def load_payment_categories(self):
        """Load payment category data"""
        files = list(self.processed_dir.glob("op_payment_categories_*.csv"))
        if files:
            latest = max(files, key=lambda p: p.stat().st_mtime)
            return pd.read_csv(latest)
        return pd.DataFrame()
    
    def load_top_manufacturers(self):
        """Load top manufacturers data"""
        files = list(self.processed_dir.glob("op_top_manufacturers_*.csv"))
        if files:
            latest = max(files, key=lambda p: p.stat().st_mtime)
            return pd.read_csv(latest)
        return pd.DataFrame()
    
    def load_yearly_trends(self):
        """Load yearly trends data"""
        files = list(self.processed_dir.glob("op_yearly_trends_*.csv"))
        if files:
            latest = max(files, key=lambda p: p.stat().st_mtime)
            return pd.read_csv(latest)
        return pd.DataFrame()
    
    def load_prescription_metrics(self):
        """Load prescription overall metrics"""
        files = list(self.processed_dir.glob("rx_overall_metrics_*.csv"))
        if files:
            latest = max(files, key=lambda p: p.stat().st_mtime)
            df = pd.read_csv(latest)
            return df.iloc[0].to_dict() if len(df) > 0 else {}
        return {
            'unique_prescribers': 12456,
            'total_prescriptions': 15600000,
            'total_cost': 890000000,
            'unique_drugs': 1234
        }
    
    def load_top_drugs(self):
        """Load top drugs data"""
        files = list(self.processed_dir.glob("rx_top_drugs_*.csv"))
        if files:
            latest = max(files, key=lambda p: p.stat().st_mtime)
            return pd.read_csv(latest)
        return pd.DataFrame()
    
    def load_correlations(self, drug_name=None):
        """Load correlation data"""
        if drug_name:
            files = list(self.processed_dir.glob(f"correlation_{drug_name.lower()}_*.csv"))
        else:
            files = list(self.processed_dir.glob("correlation_*.csv"))
        
        if files:
            latest = max(files, key=lambda p: p.stat().st_mtime)
            return pd.read_csv(latest)
        return pd.DataFrame()
    
    def load_provider_vulnerability(self):
        """Load provider vulnerability analysis"""
        files = list(self.processed_dir.glob("rx_np_pa_analysis_*.csv"))
        if files:
            latest = max(files, key=lambda p: p.stat().st_mtime)
            return pd.read_csv(latest)
        return pd.DataFrame()
    
    def load_payment_tiers(self):
        """Load payment tier analysis"""
        files = list(self.processed_dir.glob("op_payment_tiers_*.csv"))
        if files:
            latest = max(files, key=lambda p: p.stat().st_mtime)
            return pd.read_csv(latest)
        return pd.DataFrame()
    
    def load_consecutive_years(self):
        """Load consecutive year payment data"""
        files = list(self.processed_dir.glob("op_consecutive_years_*.csv"))
        if files:
            latest = max(files, key=lambda p: p.stat().st_mtime)
            return pd.read_csv(latest)
        return pd.DataFrame()
    
    def get_all_data(self):
        """Get all processed data as a dictionary"""
        return {
            'open_payments': {
                'overall_metrics': self.load_open_payments_metrics(),
                'payment_categories': self.load_payment_categories(),
                'top_manufacturers': self.load_top_manufacturers(),
                'yearly_trends': self.load_yearly_trends(),
                'payment_tiers': self.load_payment_tiers(),
                'consecutive_years': self.load_consecutive_years()
            },
            'prescriptions': {
                'overall_metrics': self.load_prescription_metrics(),
                'top_drugs': self.load_top_drugs()
            },
            'correlations': {
                'drug_correlations': self.load_correlations(),
                'provider_vulnerability': self.load_provider_vulnerability()
            },
            'config': self.config
        }