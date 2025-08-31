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
        
        # Build query
        npi_str = "','".join(npi_list[:10000])  # BigQuery limit
        
        query = f"""
        WITH provider_payments AS (
            SELECT 
                Physician_Profile_ID as physician_id,
                Physician_First_Name as first_name,
                Physician_Last_Name as last_name,
                Physician_Primary_Type as provider_type,
                Physician_Specialty as specialty,
                Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as manufacturer,
                Total_Amount_of_Payment_USDollars as payment_amount,
                Date_of_Payment as payment_date,
                Nature_of_Payment_or_Transfer_of_Value as payment_category,
                Product_Name,
                EXTRACT(YEAR FROM Date_of_Payment) as payment_year
            FROM 
                `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{self.config['bigquery']['tables']['open_payments']}`
            WHERE 
                Physician_Profile_ID IN ('{npi_str}')
                AND EXTRACT(YEAR FROM Date_of_Payment) BETWEEN {start_year} AND {end_year}
                AND Total_Amount_of_Payment_USDollars > 0
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
        
        # Build query
        npi_str = "','".join(npi_list[:10000])
        
        query = f"""
        WITH provider_rx AS (
            SELECT
                NPI,
                PROVIDER_LAST_NAME,
                PROVIDER_FIRST_NAME,
                PROVIDER_CITY,
                PROVIDER_STATE,
                SPECIALTY_DESC,
                TYPE_DESC,
                BRAND_NAME,
                GENERIC_NAME,
                YEAR,
                TOTAL_CLAIM_COUNT,
                TOTAL_DAY_SUPPLY,
                TOTAL_DRUG_COST,
                BENE_COUNT,
                GE65_BENE_COUNT,
                TOTAL_CLAIM_COUNT_GE65,
                TOTAL_DRUG_COST_GE65,
                TOTAL_DAY_SUPPLY_GE65
            FROM 
                `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{self.config['bigquery']['tables']['prescriptions']}`
            WHERE 
                NPI IN ('{npi_str}')
                AND YEAR BETWEEN {start_year} AND {end_year}
                AND TOTAL_CLAIM_COUNT >= 11  -- CMS suppression threshold
        )
        SELECT 
            NPI,
            PROVIDER_LAST_NAME,
            PROVIDER_FIRST_NAME,
            SPECIALTY_DESC as specialty,
            TYPE_DESC as provider_type,
            BRAND_NAME,
            GENERIC_NAME,
            YEAR as rx_year,
            SUM(TOTAL_CLAIM_COUNT) as total_claims,
            SUM(TOTAL_DAY_SUPPLY) as total_days_supply,
            SUM(TOTAL_DRUG_COST) as total_cost,
            SUM(BENE_COUNT) as total_beneficiaries,
            AVG(TOTAL_DRUG_COST / NULLIF(TOTAL_CLAIM_COUNT, 0)) as avg_cost_per_claim
        FROM provider_rx
        GROUP BY 1,2,3,4,5,6,7,8
        """
        
        logger.info(f"Querying prescription data for {len(npi_list):,} providers")
        df = self.bq.query(query)
        
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