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
from google.cloud import bigquery

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
        Load provider NPIs from file and upload to BigQuery if needed
        
        Args:
            force_reload: Force reload from source
            
        Returns:
            DataFrame with provider NPIs and metadata
        """
        cache_file = self.processed_dir / "provider_npis.parquet"
        
        if not force_reload and cache_file.exists():
            logger.info("Loading provider NPIs from cache")
            return pd.read_parquet(cache_file)
        
        # Always load from local file
        npi_file = Path(self.config['health_system']['npi_file'])
        if not npi_file.exists():
            raise FileNotFoundError(f"NPI file not found: {npi_file}")
        
        logger.info(f"Loading NPIs from file: {npi_file}")
        df = pd.read_csv(npi_file)
        
        # Standardize column names
        df.columns = df.columns.str.upper()
        if 'NPI' not in df.columns:
            raise ValueError("NPI column not found in provider file")
        
        # Ensure NPI is string
        df['NPI'] = df['NPI'].astype(str)
        
        # Upload to BigQuery for efficient querying (always do this to ensure table exists)
        self._upload_npis_to_bigquery(df)
        
        # Cache the result
        df.to_parquet(cache_file, index=False)
        logger.info(f"Loaded {len(df):,} provider NPIs")
        
        return df
    
    def _upload_npis_to_bigquery(self, df: pd.DataFrame):
        """Upload NPIs to BigQuery table for efficient joins"""
        table_name = f"{self.config['health_system']['short_name']}_provider_npis"
        table_id = f"{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{table_name}"
        
        logger.info(f"Uploading {len(df):,} NPIs to BigQuery table: {table_id}")
        
        # Configure upload job
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Replace table if exists
            schema=[
                bigquery.SchemaField("NPI", "STRING"),
                bigquery.SchemaField("FULL_NAME", "STRING"),
                bigquery.SchemaField("PRIMARY_SPECIALTY", "STRING"),
            ]
        )
        
        # Upload to BigQuery
        job = self.bq.client.load_table_from_dataframe(
            df[['NPI', 'FULL_NAME', 'PRIMARY_SPECIALTY']] if 'FULL_NAME' in df.columns else df[['NPI']],
            table_id,
            job_config=job_config
        )
        job.result()  # Wait for job to complete
        
        logger.info(f"Successfully uploaded NPIs to {table_id}")
    
    def load_open_payments(
        self, 
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        force_reload: bool = False,
        summary_only: bool = False
    ) -> pd.DataFrame:
        """
        Load Open Payments data for specified providers and years
        
        Args:
            start_year: Start year for data (default from config)
            end_year: End year for data (default from config)
            force_reload: Force reload from BigQuery
            summary_only: Return aggregated summary instead of detailed data
            
        Returns:
            DataFrame with Open Payments data (summary or detailed)
        """
        start_year = start_year or self.config['analysis']['start_year']
        end_year = end_year or self.config['analysis']['end_year']
        
        # Separate cache files for detailed and summary data
        detailed_cache = self.processed_dir / f"open_payments_detailed_{start_year}_{end_year}.parquet"
        summary_cache = self.processed_dir / f"open_payments_summary_{start_year}_{end_year}.parquet"
        
        # If caches exist and not forcing reload
        if not force_reload:
            if summary_only and summary_cache.exists():
                logger.info("Loading Open Payments summary from cache")
                return pd.read_parquet(summary_cache)
            elif detailed_cache.exists():
                if summary_only:
                    # Create summary from detailed if needed
                    if not summary_cache.exists():
                        logger.info("Creating summary from detailed Open Payments data")
                        detailed_df = pd.read_parquet(detailed_cache)
                        summary_df = self._create_open_payments_summary(detailed_df)
                        summary_df.to_parquet(summary_cache, index=False)
                    return pd.read_parquet(summary_cache)
                else:
                    logger.info("Loading detailed Open Payments from cache")
                    return pd.read_parquet(detailed_cache)
        
        # Load provider NPIs (this will create BigQuery table if needed)
        providers = self.load_provider_npis()
        
        # Use the NPI table for efficient querying
        npi_table = f"{self.config['health_system']['short_name']}_provider_npis"
        
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
                `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{self.config['bigquery']['tables']['open_payments']}` op
            WHERE 
                EXISTS (
                    SELECT 1 
                    FROM `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{npi_table}` npi
                    WHERE CAST(op.covered_recipient_npi AS STRING) = npi.NPI
                )
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
        
        logger.info(f"Querying Open Payments data for {len(providers):,} providers")
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
        
        logger.info(f"Loaded {len(df):,} Open Payments records from BigQuery")
        
        # Save detailed data to parquet first (before any memory issues)
        df.to_parquet(detailed_cache, index=False)
        logger.info(f"Saved detailed data to {detailed_cache.name}")
        
        # Create and save summary
        summary_df = self._create_open_payments_summary(df)
        summary_df.to_parquet(summary_cache, index=False)
        logger.info(f"Saved summary data to {summary_cache.name} ({len(summary_df):,} rows)")
        
        # Return based on what was requested
        if summary_only:
            return summary_df
        else:
            return df
    
    def _create_open_payments_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create summary version of Open Payments data by aggregating out Product_Name
        
        Args:
            df: Detailed Open Payments DataFrame
            
        Returns:
            Summary DataFrame with Product_Name aggregated out
        """
        summary = df.groupby([
            'physician_id', 'first_name', 'last_name',
            'provider_type', 'specialty', 'manufacturer',
            'payment_year', 'payment_category'
        ]).agg({
            'payment_count': 'sum',
            'total_amount': 'sum',
            'avg_amount': 'mean',
            'min_amount': 'min',
            'max_amount': 'max'
        }).reset_index()
        
        return summary
    
    def load_open_payments_by_drug(
        self,
        drug_name: str,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Load Open Payments data for a specific drug/product
        
        Args:
            drug_name: Name of drug/product to filter
            start_year: Start year for data
            end_year: End year for data
            
        Returns:
            DataFrame with Open Payments for specific drug
        """
        start_year = start_year or self.config['analysis']['start_year']
        end_year = end_year or self.config['analysis']['end_year']
        
        detailed_cache = self.processed_dir / f"open_payments_detailed_{start_year}_{end_year}.parquet"
        
        if not detailed_cache.exists():
            logger.info(f"Detailed cache not found, loading full data first")
            self.load_open_payments(start_year, end_year, summary_only=False)
        
        logger.info(f"Loading Open Payments for drug: {drug_name}")
        # Use pyarrow filters for efficient loading
        import pyarrow.parquet as pq
        df = pd.read_parquet(
            detailed_cache,
            filters=[('Product_Name', '==', drug_name)]
        )
        
        logger.info(f"Loaded {len(df):,} records for {drug_name}")
        return df
    
    def load_prescriptions(
        self,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        force_reload: bool = False,
        summary_only: bool = False
    ) -> pd.DataFrame:
        """
        Load Medicare Part D prescription data
        
        Args:
            start_year: Start year for data
            end_year: End year for data
            force_reload: Force reload from BigQuery
            summary_only: Return aggregated summary instead of detailed data
            
        Returns:
            DataFrame with prescription data (summary or detailed)
        """
        start_year = start_year or self.config['analysis']['start_year']
        end_year = end_year or self.config['analysis']['end_year']
        
        # Separate cache files for detailed and summary data
        detailed_cache = self.processed_dir / f"prescriptions_detailed_{start_year}_{end_year}.parquet"
        summary_cache = self.processed_dir / f"prescriptions_summary_{start_year}_{end_year}.parquet"
        
        # If caches exist and not forcing reload
        if not force_reload:
            if summary_only and summary_cache.exists():
                logger.info("Loading prescriptions summary from cache")
                return pd.read_parquet(summary_cache)
            elif detailed_cache.exists():
                if summary_only:
                    # Create summary from detailed if needed
                    if not summary_cache.exists():
                        logger.info("Creating summary from detailed prescriptions data")
                        detailed_df = pd.read_parquet(detailed_cache)
                        summary_df = self._create_prescriptions_summary(detailed_df)
                        summary_df.to_parquet(summary_cache, index=False)
                    return pd.read_parquet(summary_cache)
                else:
                    logger.info("Loading detailed prescriptions from cache")
                    return pd.read_parquet(detailed_cache)
        
        # Load provider NPIs (this will create BigQuery table if needed)
        providers = self.load_provider_npis()
        
        # Use the NPI table for efficient querying
        npi_table = f"{self.config['health_system']['short_name']}_provider_npis"
        
        query = f"""
        WITH provider_rx AS (
            SELECT
                rx.NPI,
                rx.physician,
                rx.PAYOR_NAME,
                rx.BRAND_NAME,
                rx.GENERIC_NAME,
                rx.CLAIM_YEAR,
                rx.CLAIM_MONTH,
                SUM(rx.PRESCRIPTIONS) as prescriptions,
                SUM(rx.DAYS_SUPPLY) as days_supply,
                SUM(rx.PAYMENTS) as total_cost,
                SUM(rx.UNIQUE_PATIENTS) as unique_patients,
                AVG(rx.PAYMENTS / NULLIF(rx.PRESCRIPTIONS, 0)) as avg_cost_per_rx
            FROM 
                `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{self.config['bigquery']['tables']['prescriptions']}` rx
            INNER JOIN 
                `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{npi_table}` npi
                ON CAST(rx.NPI AS STRING) = npi.NPI
            WHERE 
                rx.CLAIM_YEAR BETWEEN {start_year} AND {end_year}
                AND rx.PRESCRIPTIONS > 0
            GROUP BY rx.NPI, rx.physician, rx.PAYOR_NAME, rx.BRAND_NAME, rx.GENERIC_NAME, rx.CLAIM_YEAR, rx.CLAIM_MONTH
        ),
        provider_info AS (
            SELECT 
                CAST(po.NPI AS STRING) AS NPI,
                po.CREDENTIAL,
                po.SPECIALTY_PRIMARY,
                CASE 
                    WHEN po.CREDENTIAL IN ('MD', 'DO') THEN 'Physician'
                    WHEN po.CREDENTIAL IN ('NP', 'CNP', 'ARNP', 'FNP', 'APRN') THEN 'Nurse Practitioner'
                    WHEN po.CREDENTIAL IN ('PA', 'PA-C') THEN 'Physician Assistant'
                    ELSE 'Other'
                END AS provider_type_category
            FROM `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.PHYSICIANS_OVERVIEW` po
            INNER JOIN 
                `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{npi_table}` npi
                ON CAST(po.NPI AS STRING) = npi.NPI
        )
        SELECT 
            r.NPI,
            r.physician as PROVIDER_NAME,
            '' as PROVIDER_LAST_NAME,
            '' as PROVIDER_FIRST_NAME,
            COALESCE(p.SPECIALTY_PRIMARY, '') as specialty,
            COALESCE(p.provider_type_category, 'Unknown') as provider_type,
            r.BRAND_NAME,
            r.GENERIC_NAME,
            r.CLAIM_YEAR as rx_year,
            SUM(r.prescriptions) as total_claims,
            SUM(r.days_supply) as total_days_supply,
            SUM(r.total_cost) as total_cost,
            SUM(r.unique_patients) as total_beneficiaries,
            AVG(r.avg_cost_per_rx) as avg_cost_per_claim
        FROM provider_rx r
        LEFT JOIN provider_info p ON CAST(r.NPI AS STRING) = CAST(p.NPI AS STRING)
        GROUP BY 1,2,3,4,5,6,7,8,9
        """
        
        logger.info(f"Querying prescription data for {len(providers):,} providers")
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
        
        logger.info(f"Loaded {len(df):,} prescription records from BigQuery")
        
        # Save detailed data to parquet first (before any memory issues)
        df.to_parquet(detailed_cache, index=False)
        logger.info(f"Saved detailed data to {detailed_cache.name}")
        
        # Create and save summary
        summary_df = self._create_prescriptions_summary(df)
        summary_df.to_parquet(summary_cache, index=False)
        logger.info(f"Saved summary data to {summary_cache.name} ({len(summary_df):,} rows)")
        
        # Return based on what was requested
        if summary_only:
            return summary_df
        else:
            return df
    
    def _create_prescriptions_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create summary version of prescriptions data by aggregating monthly to yearly
        
        Args:
            df: Detailed prescriptions DataFrame
            
        Returns:
            Summary DataFrame aggregated to yearly level
        """
        # Note: We keep BRAND_NAME and GENERIC_NAME for drug analysis
        # But could aggregate out PAYOR_NAME if it exists
        summary = df.groupby([
            'NPI', 'PROVIDER_NAME', 'specialty', 'provider_type',
            'BRAND_NAME', 'GENERIC_NAME', 'rx_year'
        ]).agg({
            'total_claims': 'sum',
            'total_days_supply': 'sum',
            'total_cost': 'sum',
            'total_beneficiaries': 'sum',
            'avg_cost_per_claim': 'mean'
        }).reset_index()
        
        return summary
    
    def load_prescriptions_by_drug(
        self,
        drug_name: str,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        use_brand: bool = True
    ) -> pd.DataFrame:
        """
        Load prescription data for a specific drug
        
        Args:
            drug_name: Name of drug to filter
            start_year: Start year for data
            end_year: End year for data
            use_brand: Filter by BRAND_NAME (True) or GENERIC_NAME (False)
            
        Returns:
            DataFrame with prescriptions for specific drug
        """
        start_year = start_year or self.config['analysis']['start_year']
        end_year = end_year or self.config['analysis']['end_year']
        
        detailed_cache = self.processed_dir / f"prescriptions_detailed_{start_year}_{end_year}.parquet"
        
        if not detailed_cache.exists():
            logger.info(f"Detailed cache not found, loading full data first")
            self.load_prescriptions(start_year, end_year, summary_only=False)
        
        column = 'BRAND_NAME' if use_brand else 'GENERIC_NAME'
        logger.info(f"Loading prescriptions for {column}: {drug_name}")
        
        df = pd.read_parquet(
            detailed_cache,
            filters=[(column, '==', drug_name)]
        )
        
        logger.info(f"Loaded {len(df):,} prescription records for {drug_name}")
        return df
    
    def load_drug_attribution_data(
        self,
        drug_name: str,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Load matched Open Payments and prescription data for attribution analysis
        
        Args:
            drug_name: Name of drug for attribution analysis
            start_year: Start year for data
            end_year: End year for data
            
        Returns:
            DataFrame with matched payment and prescription data
        """
        start_year = start_year or self.config['analysis']['start_year']
        end_year = end_year or self.config['analysis']['end_year']
        
        # Use the NPI table for efficient querying
        npi_table = f"{self.config['health_system']['short_name']}_provider_npis"
        
        query = f"""
        WITH drug_payments AS (
            SELECT 
                CAST(covered_recipient_npi AS STRING) as physician_id,
                SUM(total_amount_of_payment_usdollars) as op_amount,
                COUNT(*) as op_count,
                AVG(total_amount_of_payment_usdollars) as op_avg
            FROM `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{self.config['bigquery']['tables']['open_payments']}`
            WHERE name_of_drug_or_biological_or_device_or_medical_supply_1 = '{drug_name}'
                AND program_year BETWEEN {start_year} AND {end_year}
                AND EXISTS (
                    SELECT 1 FROM `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{npi_table}` npi
                    WHERE CAST(covered_recipient_npi AS STRING) = npi.NPI
                )
            GROUP BY 1
        ),
        drug_prescriptions AS (
            SELECT
                CAST(NPI AS STRING) as physician_id,
                SUM(PAYMENTS) as rx_cost,
                SUM(PRESCRIPTIONS) as rx_count,
                SUM(UNIQUE_PATIENTS) as rx_patients,
                AVG(PAYMENTS / NULLIF(PRESCRIPTIONS, 0)) as rx_avg_cost
            FROM `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{self.config['bigquery']['tables']['prescriptions']}`
            WHERE BRAND_NAME = '{drug_name}'
                AND CLAIM_YEAR BETWEEN {start_year} AND {end_year}
                AND EXISTS (
                    SELECT 1 FROM `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{npi_table}` npi
                    WHERE CAST(NPI AS STRING) = npi.NPI
                )
            GROUP BY 1
        )
        SELECT 
            COALESCE(p.physician_id, r.physician_id) as physician_id,
            COALESCE(op_amount, 0) as payment_amount,
            COALESCE(op_count, 0) as payment_count,
            COALESCE(op_avg, 0) as payment_avg,
            COALESCE(rx_cost, 0) as prescription_cost,
            COALESCE(rx_count, 0) as prescription_count,
            COALESCE(rx_patients, 0) as prescription_patients,
            COALESCE(rx_avg_cost, 0) as prescription_avg_cost
        FROM drug_payments p
        FULL OUTER JOIN drug_prescriptions r
        ON p.physician_id = r.physician_id
        """
        
        logger.info(f"Loading attribution data for {drug_name}")
        df = self.bq.query(query)
        
        # Convert to numeric
        for col in df.columns:
            if col != 'physician_id':
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        logger.info(f"Loaded attribution data for {len(df):,} providers with {drug_name} activity")
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