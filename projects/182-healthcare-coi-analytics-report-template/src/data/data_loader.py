"""
Data Loader Module
Handles loading and preprocessing of various data sources
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime
import yaml

from .bigquery_connector import BigQueryConnector

logger = logging.getLogger(__name__)


class DataLoader:
    """Unified data loading and preprocessing"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize data loader with configuration"""
        self.config = self._load_config(config_path)
        self.bq = BigQueryConnector()
        self.data_dir = Path("data")
        self.processed_dir = self.data_dir / "processed"
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration file"""
        config_file = Path(config_path)
        if not config_file.exists():
            # Try old location
            config_file = Path("CONFIG.yaml")
        
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    
    def load_provider_npis(self, force_reload: bool = False) -> pd.DataFrame:
        """
        Load provider NPIs from file or BigQuery
        
        Args:
            force_reload: Force reload from source
            
        Returns:
            DataFrame with provider NPIs and metadata
        """
        cache_file = self.processed_dir / "provider_npis.parquet"
        
        if not force_reload and cache_file.exists():
            logger.info("Loading provider NPIs from cache")
            return pd.read_parquet(cache_file)
        
        # Check for local file first
        npi_file = Path(self.config['health_system']['npi_file'])
        if npi_file.exists():
            logger.info(f"Loading NPIs from file: {npi_file}")
            df = pd.read_csv(npi_file)
            
            # Standardize column names
            df.columns = df.columns.str.upper()
            if 'NPI' not in df.columns:
                raise ValueError("NPI column not found in provider file")
            
            # Ensure NPI is string
            df['NPI'] = df['NPI'].astype(str)
            
        else:
            # Load from BigQuery
            logger.info("Loading NPIs from BigQuery")
            query = f"""
            SELECT DISTINCT
                NPI,
                PROVIDER_NAME,
                SPECIALTY,
                PROVIDER_TYPE
            FROM `{self.config['bigquery']['dataset']}.{self.config['bigquery']['tables']['provider_npis']}`
            """
            df = self.bq.query(query)
        
        # Cache the result
        df.to_parquet(cache_file, index=False)
        logger.info(f"Loaded {len(df):,} provider NPIs")
        
        return df
    
    def load_open_payments(
        self, 
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        force_reload: bool = False
    ) -> pd.DataFrame:
        """
        Load Open Payments data for specified providers and years
        
        Args:
            start_year: Start year for data (default from config)
            end_year: End year for data (default from config)
            force_reload: Force reload from BigQuery
            
        Returns:
            DataFrame with Open Payments data
        """
        start_year = start_year or self.config['analysis']['start_year']
        end_year = end_year or self.config['analysis']['end_year']
        
        cache_file = self.processed_dir / f"open_payments_{start_year}_{end_year}.parquet"
        
        if not force_reload and cache_file.exists():
            logger.info("Loading Open Payments from cache")
            return pd.read_parquet(cache_file)
        
        # Load provider NPIs
        providers = self.load_provider_npis()
        npi_list = providers['NPI'].unique().tolist()
        
        # Build query - limit to 100 NPIs for testing
        npi_str = "','".join(npi_list[:100])  # Limit for testing
        
        query = f"""
        WITH provider_payments AS (
            SELECT 
                covered_recipient_npi as physician_id,
                covered_recipient_first_name as first_name,
                covered_recipient_last_name as last_name,
                physician as provider_type,
                covered_recipient_specialty_1 as specialty,
                applicable_manufacturer_or_applicable_gpo_making_payment_name as manufacturer,
                total_amount_of_payment_usdollars as payment_amount,
                date_of_payment as payment_date,
                nature_of_payment_or_transfer_of_value as payment_category,
                name_of_drug_or_biological_or_device_or_medical_supply_1 as Product_Name,
                program_year as payment_year
            FROM 
                `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{self.config['bigquery']['tables']['open_payments']}`
            WHERE 
                CAST(covered_recipient_npi AS STRING) IN ('{npi_str}')
                AND program_year BETWEEN {start_year} AND {end_year}
                AND total_amount_of_payment_usdollars > 0
        )
        SELECT 
            physician_id,
            first_name,
            last_name,
            provider_type,
            specialty,
            manufacturer,
            payment_year,
            payment_category,
            Product_Name,
            COUNT(*) as payment_count,
            SUM(payment_amount) as total_amount,
            AVG(payment_amount) as avg_amount,
            MIN(payment_amount) as min_amount,
            MAX(payment_amount) as max_amount
        FROM provider_payments
        GROUP BY 1,2,3,4,5,6,7,8,9
        """
        
        logger.info(f"Querying Open Payments data for {len(npi_list):,} providers")
        df = self.bq.query(query)
        
        # Convert decimal types to float for pandas compatibility
        for col in df.select_dtypes(include=['object']).columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass
        
        # Ensure numeric columns are float
        numeric_cols = ['total_amount', 'avg_amount', 'min_amount', 'max_amount', 'payment_count']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Save to cache
        df.to_parquet(cache_file, index=False)
        logger.info(f"Loaded {len(df):,} Open Payments records")
        
        return df
    
    def load_prescriptions(
        self,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        force_reload: bool = False
    ) -> pd.DataFrame:
        """
        Load Medicare Part D prescription data
        
        Args:
            start_year: Start year for data
            end_year: End year for data
            force_reload: Force reload from BigQuery
            
        Returns:
            DataFrame with prescription data
        """
        start_year = start_year or self.config['analysis']['start_year']
        end_year = end_year or self.config['analysis']['end_year']
        
        cache_file = self.processed_dir / f"prescriptions_{start_year}_{end_year}.parquet"
        
        if not force_reload and cache_file.exists():
            logger.info("Loading prescriptions from cache")
            return pd.read_parquet(cache_file)
        
        # Load provider NPIs
        providers = self.load_provider_npis()
        npi_list = providers['NPI'].unique().tolist()
        
        # Build query - limit to 100 NPIs for testing  
        npi_str = "','".join(npi_list[:100])  # Keep as quoted strings
        
        query = f"""
        WITH provider_rx AS (
            SELECT
                NPI,
                physician,
                PAYOR_NAME,
                BRAND_NAME,
                GENERIC_NAME,
                CLAIM_YEAR,
                CLAIM_MONTH,
                SUM(PRESCRIPTIONS) as prescriptions,
                SUM(DAYS_SUPPLY) as days_supply,
                SUM(PAYMENTS) as total_cost,
                SUM(UNIQUE_PATIENTS) as unique_patients,
                AVG(PAYMENTS / NULLIF(PRESCRIPTIONS, 0)) as avg_cost_per_rx
            FROM 
                `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{self.config['bigquery']['tables']['prescriptions']}`
            WHERE 
                CAST(NPI AS STRING) IN ('{npi_str}')
                AND CLAIM_YEAR BETWEEN {start_year} AND {end_year}
                AND PRESCRIPTIONS > 0
            GROUP BY NPI, physician, PAYOR_NAME, BRAND_NAME, GENERIC_NAME, CLAIM_YEAR, CLAIM_MONTH
        )
        SELECT 
            NPI,
            physician as PROVIDER_NAME,
            '' as PROVIDER_LAST_NAME,
            '' as PROVIDER_FIRST_NAME,
            '' as specialty,
            '' as provider_type,
            BRAND_NAME,
            GENERIC_NAME,
            CLAIM_YEAR as rx_year,
            SUM(prescriptions) as total_claims,
            SUM(days_supply) as total_days_supply,
            SUM(total_cost) as total_cost,
            SUM(unique_patients) as total_beneficiaries,
            AVG(avg_cost_per_rx) as avg_cost_per_claim
        FROM provider_rx
        GROUP BY 1,2,3,4,5,6,7,8,9
        """
        
        logger.info(f"Querying prescription data for {len(npi_list):,} providers")
        df = self.bq.query(query)
        
        # Convert decimal types to float for pandas compatibility
        for col in df.select_dtypes(include=['object']).columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass
        
        # Ensure numeric columns are float
        numeric_cols = ['total_claims', 'total_days_supply', 'total_cost', 'total_beneficiaries', 'avg_cost_per_claim']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Save to cache
        df.to_parquet(cache_file, index=False)
        logger.info(f"Loaded {len(df):,} prescription records")
        
        return df
    
    def load_analysis_results(self, analysis_type: str) -> pd.DataFrame:
        """
        Load previously computed analysis results
        
        Args:
            analysis_type: Type of analysis (e.g., 'correlations', 'risk_scores')
            
        Returns:
            DataFrame with analysis results
        """
        result_file = self.processed_dir / f"{analysis_type}.parquet"
        
        if not result_file.exists():
            raise FileNotFoundError(f"Analysis results not found: {analysis_type}")
        
        return pd.read_parquet(result_file)
    
    def save_analysis_results(
        self, 
        df: pd.DataFrame, 
        analysis_type: str,
        timestamp: bool = True
    ):
        """
        Save analysis results to processed directory
        
        Args:
            df: DataFrame with results
            analysis_type: Type of analysis
            timestamp: Whether to include timestamp in filename
        """
        if timestamp:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{analysis_type}_{ts}.parquet"
        else:
            filename = f"{analysis_type}.parquet"
        
        output_file = self.processed_dir / filename
        df.to_parquet(output_file, index=False)
        logger.info(f"Saved {len(df):,} rows to {output_file}")
    
    def get_available_datasets(self) -> List[str]:
        """List available cached datasets"""
        datasets = []
        for file in self.processed_dir.glob("*.parquet"):
            datasets.append(file.stem)
        return sorted(datasets)
    
    def clean_old_files(self, days: int = 7):
        """Remove old processed files"""
        cutoff = datetime.now().timestamp() - (days * 24 * 3600)
        
        for file in self.processed_dir.glob("*_20*.parquet"):
            if file.stat().st_mtime < cutoff:
                file.unlink()
                logger.info(f"Removed old file: {file.name}")