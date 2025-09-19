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
import hashlib
import json
import os

from .bigquery_connector import BigQueryConnector
from .data_lineage import DataLineageTracker

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
        
        # Lineage tracker (will be set by pipeline)
        self.lineage_tracker: Optional[DataLineageTracker] = None
        
        # Cache metadata file
        self.cache_metadata_file = self.processed_dir / '.cache_metadata.json'
        
    def set_lineage_tracker(self, tracker: DataLineageTracker):
        """Set the lineage tracker for this data loader"""
        self.lineage_tracker = tracker
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of a file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def _get_file_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Get metadata for a file including hash and modification time"""
        return {
            'file_path': str(file_path),
            'file_hash': self._calculate_file_hash(file_path),
            'modified_time': os.path.getmtime(file_path),
            'modified_datetime': datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat(),
            'file_size': os.path.getsize(file_path)
        }
    
    def _load_cache_metadata(self) -> Dict[str, Any]:
        """Load cache metadata from file"""
        if self.cache_metadata_file.exists():
            with open(self.cache_metadata_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_cache_metadata(self, metadata: Dict[str, Any]):
        """Save cache metadata to file"""
        with open(self.cache_metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
    
    def _is_cache_valid(self, cache_key: str, source_file: Path) -> bool:
        """Check if cache is valid for a given source file"""
        metadata = self._load_cache_metadata()
        
        if cache_key not in metadata:
            logger.info(f"No cache metadata found for {cache_key}")
            return False
        
        cached_info = metadata[cache_key]
        current_hash = self._calculate_file_hash(source_file)
        
        if cached_info.get('file_hash') != current_hash:
            logger.info(f"File hash changed for {source_file.name}: cache invalid")
            return False
        
        if cached_info.get('file_path') != str(source_file):
            logger.info(f"File path changed: cache invalid")
            return False
        
        logger.info(f"Cache is valid for {cache_key}")
        return True
    
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
        cache_key = "provider_npis"
        npi_file = Path(self.config['health_system']['npi_file'])
        
        if not npi_file.exists():
            raise FileNotFoundError(f"NPI file not found: {npi_file}")
        
        # Check if cache is valid
        use_cache = (not force_reload and 
                    cache_file.exists() and 
                    self._is_cache_valid(cache_key, npi_file))
        
        if use_cache:
            logger.info("Loading provider NPIs from cache (validated)")
            df = pd.read_parquet(cache_file)
            
            # Track lineage even when loading from cache
            metadata = self._load_cache_metadata().get(cache_key, {})
            if self.lineage_tracker:
                self.lineage_tracker.add_source_data('provider_npis', {
                    'source_file': str(npi_file),
                    'rows': len(df),
                    'columns': list(df.columns),
                    'loaded_from': 'cache',
                    'file_hash': metadata.get('file_hash', 'unknown')
                })
            
            # Always update BigQuery table to ensure consistency
            self._upload_npis_to_bigquery(df)
            
            return df
        
        logger.info(f"Loading NPIs from file: {npi_file}")
        df = pd.read_csv(npi_file)
        
        # Get file metadata for caching
        file_metadata = self._get_file_metadata(npi_file)
        
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
        
        # Update cache metadata
        metadata = self._load_cache_metadata()
        metadata[cache_key] = {
            **file_metadata,
            'rows': len(df),
            'columns': list(df.columns),
            'cached_at': datetime.now().isoformat()
        }
        self._save_cache_metadata(metadata)
        
        # Track lineage with file hash
        if self.lineage_tracker:
            self.lineage_tracker.add_source_data('provider_npis', {
                'source_file': str(npi_file),
                'rows': len(df),
                'columns': list(df.columns),
                'file_hash': file_metadata['file_hash'],
                'loaded_from': 'source'
            })
        
        return df
    
    def _get_npi_hash_for_tables(self) -> str:
        """Get the NPI hash that was used to create current BigQuery tables"""
        metadata = self._load_cache_metadata()
        if 'bigquery_tables' in metadata:
            return metadata['bigquery_tables'].get('npi_hash', '')
        return ''
    
    def _npis_changed_since_table_creation(self) -> bool:
        """Check if NPIs have changed since BigQuery tables were created"""
        npi_file = Path(self.config['health_system']['npi_file'])
        if not npi_file.exists():
            return False
        
        current_hash = self._calculate_file_hash(npi_file)
        stored_hash = self._get_npi_hash_for_tables()
        
        if stored_hash and current_hash != stored_hash:
            logger.warning(f"NPI file has changed since tables were created!")
            logger.warning(f"  Previous hash: {stored_hash[:8]}...")
            logger.warning(f"  Current hash:  {current_hash[:8]}...")
            return True
        
        return False
    
    def _upload_npis_to_bigquery(self, df: pd.DataFrame):
        """Upload NPIs to BigQuery table for efficient joins"""
        table_name = f"{self.config['health_system']['short_name']}_provider_npis"
        temp_dataset = self.config['bigquery'].get('temp_dataset', 'temp')
        table_id = f"{self.config['bigquery']['project_id']}.{temp_dataset}.{table_name}"
        
        logger.info(f"Uploading {len(df):,} NPIs to BigQuery table: {table_id}")
        
        # Determine available columns and create appropriate schema
        schema_fields = [bigquery.SchemaField("NPI", "STRING")]
        upload_columns = ['NPI']
        
        # Check for name columns (different formats)
        if 'FULL_NAME' in df.columns:
            schema_fields.append(bigquery.SchemaField("FULL_NAME", "STRING"))
            upload_columns.append('FULL_NAME')
        elif 'EMPLOYEE NAME' in df.columns:
            # Map EMPLOYEE NAME to FULL_NAME for consistency
            df['FULL_NAME'] = df['EMPLOYEE NAME']
            schema_fields.append(bigquery.SchemaField("FULL_NAME", "STRING"))
            upload_columns.append('FULL_NAME')
        
        # Check for specialty column
        if 'PRIMARY_SPECIALTY' in df.columns:
            schema_fields.append(bigquery.SchemaField("PRIMARY_SPECIALTY", "STRING"))
            upload_columns.append('PRIMARY_SPECIALTY')
        
        # Configure upload job
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Replace table if exists
            schema=schema_fields
        )
        
        # Upload to BigQuery
        job = self.bq.client.load_table_from_dataframe(
            df[upload_columns],
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
        summary_only: bool = False,
        create_only: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        Load Open Payments data for specified providers and years
        Uses BigQuery temp dataset to avoid memory issues
        
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
        
        # Table names in BigQuery temp dataset
        temp_dataset = self.config['bigquery'].get('temp_dataset', 'temp')
        detailed_table = f"{self.config['health_system']['short_name']}_open_payments_detailed_{start_year}_{end_year}"
        summary_table = f"{self.config['health_system']['short_name']}_open_payments_summary_{start_year}_{end_year}"
        
        # Full table paths
        detailed_table_path = f"`{self.config['bigquery']['project_id']}.{temp_dataset}.{detailed_table}`"
        summary_table_path = f"`{self.config['bigquery']['project_id']}.{temp_dataset}.{summary_table}`"
        
        # Check if NPIs have changed
        npis_changed = self._npis_changed_since_table_creation()
        if npis_changed:
            logger.warning("NPIs have changed - forcing table recreation")
            force_reload = True
        
        # Check if tables exist and create if needed or force_reload
        if force_reload or not self._table_exists(temp_dataset, detailed_table):
            logger.info(f"Creating Open Payments tables in BigQuery temp dataset")
            self._create_open_payments_tables(start_year, end_year, detailed_table_path, summary_table_path)
        else:
            logger.info(f"Using existing Open Payments tables in BigQuery")
            # Track that we're using existing tables
            if self.lineage_tracker:
                self.lineage_tracker.add_intermediate_table('open_payments_existing', {
                    'detailed_table': detailed_table_path.replace('`', ''),
                    'summary_table': summary_table_path.replace('`', ''),
                    'status': 'reused_existing',
                    'npi_hash': self._get_npi_hash_for_tables()
                })
        
        # If create_only, we're done - don't download any data
        if create_only:
            logger.info("Tables created in BigQuery temp dataset, skipping data download")
            return None
        
        # Query from BigQuery table instead of loading all data
        if summary_only:
            logger.info(f"Querying summary table: {summary_table}")
            query = f"SELECT * FROM {summary_table_path}"
        else:
            logger.info(f"Querying detailed table: {detailed_table}")
            query = f"SELECT * FROM {detailed_table_path}"
        
        df = self.bq.query(query)
        logger.info(f"Loaded {len(df):,} Open Payments records from BigQuery")
        return df
    
    def _table_exists(self, dataset: str, table_name: str) -> bool:
        """Check if a table exists in BigQuery"""
        try:
            table_ref = f"{self.config['bigquery']['project_id']}.{dataset}.{table_name}"
            self.bq.client.get_table(table_ref)
            return True
        except:
            return False
    
    def _create_open_payments_tables(
        self,
        start_year: int,
        end_year: int,
        detailed_table_path: str,
        summary_table_path: str
    ):
        """Create Open Payments tables in BigQuery temp dataset"""
        # Load provider NPIs (this will create BigQuery table if needed)
        providers = self.load_provider_npis()
        
        # Use the NPI table for efficient querying
        temp_dataset = self.config['bigquery'].get('temp_dataset', 'temp')
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
                    FROM `{self.config['bigquery']['project_id']}.{temp_dataset}.{npi_table}` npi
                    WHERE op.covered_recipient_npi = CAST(npi.NPI AS INT64)
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
        
        # Drop existing table to ensure clean recreation with new schema
        drop_query = f"DROP TABLE IF EXISTS {detailed_table_path}"
        try:
            self.bq.client.query(drop_query).result()
            logger.info(f"Dropped existing detailed table")
        except:
            pass  # Table might not exist
        
        # Create detailed table query with full aggregation
        create_detailed_query = f"""
        CREATE OR REPLACE TABLE {detailed_table_path} AS
        {query}
        """
        
        logger.info(f"Creating detailed Open Payments table for {len(providers):,} providers")
        job = self.bq.client.query(create_detailed_query)
        job.result()  # Wait for completion
        
        # Get row count
        count_query = f"SELECT COUNT(*) as count FROM {detailed_table_path}"
        result = self.bq.client.query(count_query).result()
        row_count = list(result)[0].count
        logger.info(f"Created detailed table with {row_count:,} rows")
        
        # Get current NPI hash for tracking
        npi_file = Path(self.config['health_system']['npi_file'])
        current_npi_hash = self._calculate_file_hash(npi_file) if npi_file.exists() else 'unknown'
        
        # Update cache metadata with table creation info
        metadata = self._load_cache_metadata()
        if 'bigquery_tables' not in metadata:
            metadata['bigquery_tables'] = {}
        metadata['bigquery_tables'].update({
            'npi_hash': current_npi_hash,
            'tables_created_at': datetime.now().isoformat(),
            'open_payments_detailed': detailed_table_path.replace('`', ''),
            'open_payments_summary': summary_table_path.replace('`', '')
        })
        self._save_cache_metadata(metadata)
        
        # Track lineage for detailed table
        if self.lineage_tracker:
            self.lineage_tracker.add_intermediate_table('open_payments_detailed', {
                'table': detailed_table_path.replace('`', ''),
                'rows': row_count,
                'derived_from': f"{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{self.config['bigquery']['tables']['open_payments']}",
                'date_range': f"{start_year}-{end_year}",
                'npi_hash': current_npi_hash,
                'created_at': datetime.now().isoformat()
            })
            self.lineage_tracker.add_source_data('open_payments', {
                'table': f"{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{self.config['bigquery']['tables']['open_payments']}",
                'rows': row_count,
                'date_range': f"{start_year}-{end_year}"
            })
        
        # Drop existing summary table to ensure clean recreation
        drop_summary_query = f"DROP TABLE IF EXISTS {summary_table_path}"
        try:
            self.bq.client.query(drop_summary_query).result()
            logger.info(f"Dropped existing summary table")
        except:
            pass  # Table might not exist
        
        # Create summary table from detailed
        create_summary_query = f"""
        CREATE OR REPLACE TABLE {summary_table_path} AS
        SELECT 
            physician_id, first_name, last_name,
            provider_type, specialty, manufacturer,
            payment_year, payment_category,
            SUM(payment_count) as payment_count,
            SUM(total_amount) as total_amount,
            AVG(avg_amount) as avg_amount,
            MIN(min_amount) as min_amount,
            MAX(max_amount) as max_amount
        FROM {detailed_table_path}
        GROUP BY 1,2,3,4,5,6,7,8
        """
        
        logger.info("Creating summary Open Payments table")
        job = self.bq.client.query(create_summary_query)
        job.result()  # Wait for completion
        
        # Get summary row count
        count_query = f"SELECT COUNT(*) as count FROM {summary_table_path}"
        result = self.bq.client.query(count_query).result()
        summary_count = list(result)[0].count
        logger.info(f"Created summary table with {summary_count:,} rows")
    
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
        summary_only: bool = False,
        create_only: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        Load Medicare Part D prescription data
        Uses BigQuery temp dataset to avoid memory issues
        
        Args:
            start_year: Start year for data
            end_year: End year for data
            force_reload: Force reload from BigQuery
            summary_only: Return aggregated summary instead of detailed data
            create_only: Only create tables in BigQuery, don't download data
            
        Returns:
            DataFrame with prescription data (summary or detailed), or None if create_only
        """
        start_year = start_year or self.config['analysis']['start_year']
        end_year = end_year or self.config['analysis']['end_year']
        
        # Table names in BigQuery temp dataset
        temp_dataset = self.config['bigquery'].get('temp_dataset', 'temp')
        detailed_table = f"{self.config['health_system']['short_name']}_prescriptions_detailed_{start_year}_{end_year}"
        summary_table = f"{self.config['health_system']['short_name']}_prescriptions_summary_{start_year}_{end_year}"
        
        # Full table paths
        detailed_table_path = f"`{self.config['bigquery']['project_id']}.{temp_dataset}.{detailed_table}`"
        summary_table_path = f"`{self.config['bigquery']['project_id']}.{temp_dataset}.{summary_table}`"
        
        # Check if NPIs have changed
        npis_changed = self._npis_changed_since_table_creation()
        if npis_changed:
            logger.warning("NPIs have changed - forcing prescription table recreation")
            force_reload = True
        
        # Check if tables exist and create if needed or force_reload
        if force_reload or not self._table_exists(temp_dataset, detailed_table):
            logger.info(f"Creating Prescriptions tables in BigQuery temp dataset")
            self._create_prescriptions_tables(start_year, end_year, detailed_table_path, summary_table_path)
        else:
            logger.info(f"Using existing Prescriptions tables in BigQuery")
            # Track that we're using existing tables
            if self.lineage_tracker:
                self.lineage_tracker.add_intermediate_table('prescriptions_existing', {
                    'detailed_table': detailed_table_path.replace('`', ''),
                    'summary_table': summary_table_path.replace('`', ''),
                    'status': 'reused_existing',
                    'npi_hash': self._get_npi_hash_for_tables()
                })
        
        # If create_only, we're done - don't download any data
        if create_only:
            logger.info("Tables created in BigQuery temp dataset, skipping data download")
            return None
        
        # Query from BigQuery table instead of loading all data
        if summary_only:
            logger.info(f"Querying summary table: {summary_table}")
            query = f"SELECT * FROM {summary_table_path}"
        else:
            logger.info(f"Querying detailed table: {detailed_table}")
            query = f"SELECT * FROM {detailed_table_path}"
        
        df = self.bq.query(query)
        logger.info(f"Loaded {len(df):,} prescription records from BigQuery")
        return df
    
    def _create_prescriptions_tables(
        self,
        start_year: int,
        end_year: int,
        detailed_table_path: str,
        summary_table_path: str
    ):
        """Create Prescriptions tables in BigQuery temp dataset"""
        # Load provider NPIs (this will create BigQuery table if needed)
        providers = self.load_provider_npis()
        
        # Use the NPI table for efficient querying
        temp_dataset = self.config['bigquery'].get('temp_dataset', 'temp')
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
                `{self.config['bigquery']['project_id']}.{temp_dataset}.{npi_table}` npi
                ON rx.NPI = CAST(npi.NPI AS INT64)
            WHERE 
                rx.CLAIM_YEAR BETWEEN {start_year} AND {end_year}
                AND rx.PRESCRIPTIONS > 0
            GROUP BY rx.NPI, rx.physician, rx.PAYOR_NAME, rx.BRAND_NAME, rx.GENERIC_NAME, rx.CLAIM_YEAR, rx.CLAIM_MONTH
        ),
        provider_info AS (
            SELECT 
                po.NPI,
                po.CREDENTIAL,
                po.SPECIALTY_PRIMARY,
                CASE
                    WHEN po.ROLE_NAME = 'Physician' THEN 'Physician'
                    WHEN po.ROLE_NAME = 'Hospitalist' THEN 'Physician'  -- Hospitalists are physicians
                    WHEN po.ROLE_NAME = 'Nurse Practitioner' THEN 'Nurse Practitioner'
                    WHEN po.ROLE_NAME = 'Physician Assistant' THEN 'Physician Assistant'
                    WHEN po.ROLE_NAME IN ('Certified Registered Nurse Anesthetist', 'Certified Nurse Midwife') THEN 'Advanced Practice Nurse'
                    WHEN po.ROLE_NAME = 'Dentist' THEN 'Dentist'
                    ELSE 'Other Healthcare Professional'
                END AS provider_type_category
            FROM `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.PHYSICIANS_OVERVIEW_optimized` po
            INNER JOIN 
                `{self.config['bigquery']['project_id']}.{temp_dataset}.{npi_table}` npi
                ON po.NPI = CAST(npi.NPI AS INT64)
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
        LEFT JOIN provider_info p ON r.NPI = p.NPI
        GROUP BY 1,2,3,4,5,6,7,8,9
        """
        
        # Drop existing table to ensure clean recreation with new schema
        drop_query = f"DROP TABLE IF EXISTS {detailed_table_path}"
        try:
            self.bq.client.query(drop_query).result()
            logger.info(f"Dropped existing detailed prescriptions table")
        except:
            pass  # Table might not exist
        
        # Create detailed table with all prescription data
        create_detailed_query = f"""
        CREATE OR REPLACE TABLE {detailed_table_path} AS
        {query}
        """
        
        logger.info(f"Creating detailed Prescriptions table for {len(providers):,} providers")
        job = self.bq.client.query(create_detailed_query)
        job.result()  # Wait for completion
        
        # Get row count
        count_query = f"SELECT COUNT(*) as count FROM {detailed_table_path}"
        result = self.bq.client.query(count_query).result()
        row_count = list(result)[0].count
        logger.info(f"Created detailed table with {row_count:,} rows")
        
        # Get current NPI hash for tracking
        npi_file = Path(self.config['health_system']['npi_file'])
        current_npi_hash = self._calculate_file_hash(npi_file) if npi_file.exists() else 'unknown'
        
        # Update cache metadata with prescription table info
        metadata = self._load_cache_metadata()
        if 'bigquery_tables' not in metadata:
            metadata['bigquery_tables'] = {}
        metadata['bigquery_tables'].update({
            'npi_hash': current_npi_hash,
            'prescriptions_tables_created_at': datetime.now().isoformat(),
            'prescriptions_detailed': detailed_table_path.replace('`', ''),
            'prescriptions_summary': summary_table_path.replace('`', '')
        })
        self._save_cache_metadata(metadata)
        
        # Track lineage for detailed table
        if self.lineage_tracker:
            self.lineage_tracker.add_intermediate_table('prescriptions_detailed', {
                'table': detailed_table_path.replace('`', ''),
                'rows': row_count,
                'derived_from': f"{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{self.config['bigquery']['tables']['prescriptions']}",
                'date_range': f"{start_year}-{end_year}",
                'npi_hash': current_npi_hash,
                'created_at': datetime.now().isoformat()
            })
            self.lineage_tracker.add_source_data('prescriptions', {
                'table': f"{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{self.config['bigquery']['tables']['prescriptions']}",
                'rows': row_count,
                'date_range': f"{start_year}-{end_year}"
            })
        
        # Drop existing summary table to ensure clean recreation
        drop_summary_query = f"DROP TABLE IF EXISTS {summary_table_path}"
        try:
            self.bq.client.query(drop_summary_query).result()
            logger.info(f"Dropped existing prescriptions summary table")
        except:
            pass  # Table might not exist
        
        # Create summary table from detailed
        create_summary_query = f"""
        CREATE OR REPLACE TABLE {summary_table_path} AS
        SELECT 
            NPI, PROVIDER_NAME, specialty, provider_type,
            BRAND_NAME, GENERIC_NAME, rx_year,
            SUM(total_claims) as total_claims,
            SUM(total_days_supply) as total_days_supply,
            SUM(total_cost) as total_cost,
            SUM(total_beneficiaries) as total_beneficiaries,
            AVG(avg_cost_per_claim) as avg_cost_per_claim
        FROM {detailed_table_path}
        GROUP BY 1,2,3,4,5,6,7
        """
        
        logger.info("Creating summary Prescriptions table")
        job = self.bq.client.query(create_summary_query)
        job.result()  # Wait for completion
        
        # Get summary row count
        count_query = f"SELECT COUNT(*) as count FROM {summary_table_path}"
        result = self.bq.client.query(count_query).result()
        summary_count = list(result)[0].count
        logger.info(f"Created summary table with {summary_count:,} rows")
    
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
        Uses temp dataset tables for efficiency
        
        Args:
            drug_name: Name of drug for attribution analysis
            start_year: Start year for data
            end_year: End year for data
            
        Returns:
            DataFrame with matched payment and prescription data
        """
        start_year = start_year or self.config['analysis']['start_year']
        end_year = end_year or self.config['analysis']['end_year']
        
        # Use temp dataset tables
        temp_dataset = self.config['bigquery'].get('temp_dataset', 'temp')
        op_detailed_table = f"`{self.config['bigquery']['project_id']}.{temp_dataset}.{self.config['health_system']['short_name']}_open_payments_detailed_{start_year}_{end_year}`"
        rx_detailed_table = f"`{self.config['bigquery']['project_id']}.{temp_dataset}.{self.config['health_system']['short_name']}_prescriptions_detailed_{start_year}_{end_year}`"
        
        query = f"""
        WITH drug_payments AS (
            SELECT 
                physician_id,
                SUM(total_amount) as op_amount,
                SUM(payment_count) as op_count,
                AVG(avg_amount) as op_avg
            FROM {op_detailed_table}
            WHERE Product_Name = '{drug_name}'
            GROUP BY physician_id
        ),
        drug_prescriptions AS (
            SELECT
                NPI as physician_id,
                SUM(total_cost) as rx_cost,
                SUM(total_claims) as rx_count,
                SUM(total_beneficiaries) as rx_patients,
                AVG(avg_cost_per_claim) as rx_avg_cost
            FROM {rx_detailed_table}
            WHERE BRAND_NAME = '{drug_name}'
            GROUP BY NPI
        )
        SELECT 
            COALESCE(p.physician_id, r.physician_id) as physician_id,
            COALESCE(op_amount, 0) as payment_amount,
            COALESCE(op_count, 0) as payment_count,
            COALESCE(op_avg, 0) as payment_avg,
            COALESCE(rx_cost, 0) as prescription_cost,
            COALESCE(rx_count, 0) as prescription_count,
            COALESCE(rx_patients, 0) as prescription_patients,
            COALESCE(rx_avg_cost, 0) as prescription_avg_cost,
            CASE WHEN op_amount > 0 THEN rx_cost / op_amount ELSE NULL END as roi
        FROM drug_payments p
        FULL OUTER JOIN drug_prescriptions r
        ON p.physician_id = r.physician_id
        """
        
        logger.info(f"Loading attribution data for {drug_name} from temp tables")
        df = self.bq.query(query)
        
        # Convert to numeric
        for col in df.columns:
            if col != 'physician_id':
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        logger.info(f"Loaded attribution data for {len(df):,} providers with {drug_name} activity")
        return df
    
    def create_monthly_analysis_table(
        self,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> str:
        """
        Create monthly granularity table in BigQuery for time-series analysis
        
        Args:
            start_year: Start year for data
            end_year: End year for data
            
        Returns:
            Table name created
        """
        start_year = start_year or self.config['analysis']['start_year']
        end_year = end_year or self.config['analysis']['end_year']
        
        # Table name in temp dataset
        temp_dataset = self.config['bigquery'].get('temp_dataset', 'temp')
        monthly_table = f"{self.config['health_system']['short_name']}_monthly_analysis_{start_year}_{end_year}"
        monthly_table_path = f"`{self.config['bigquery']['project_id']}.{temp_dataset}.{monthly_table}`"
        
        # Use the NPI table for filtering
        npi_table = f"{self.config['health_system']['short_name']}_provider_npis"
        
        query = f"""
        CREATE OR REPLACE TABLE {monthly_table_path} AS
        SELECT 
            covered_recipient_npi as physician_id,
            program_year as payment_year,
            EXTRACT(MONTH FROM CAST(date_of_payment AS DATE)) as payment_month,
            nature_of_payment_or_transfer_of_value as payment_category,
            applicable_manufacturer_or_applicable_gpo_making_payment_name as manufacturer,
            name_of_drug_or_biological_or_device_or_medical_supply_1 as Product_Name,
            COUNT(*) as payment_count,
            SUM(total_amount_of_payment_usdollars) as total_amount,
            AVG(total_amount_of_payment_usdollars) as avg_amount
        FROM `{self.config['bigquery']['project_id']}.{self.config['bigquery']['dataset']}.{self.config['bigquery']['tables']['open_payments']}`
        WHERE program_year BETWEEN {start_year} AND {end_year}
            AND total_amount_of_payment_usdollars > 0
            AND EXISTS (
                SELECT 1 
                FROM `{self.config['bigquery']['project_id']}.{temp_dataset}.{npi_table}` npi
                WHERE covered_recipient_npi = CAST(npi.NPI AS INT64)
            )
        GROUP BY 1,2,3,4,5,6
        """
        
        logger.info(f"Creating monthly analysis table: {monthly_table}")
        job = self.bq.client.query(query)
        job.result()  # Wait for completion
        
        # Get row count
        count_query = f"SELECT COUNT(*) as count FROM {monthly_table_path}"
        result = self.bq.client.query(count_query).result()
        row_count = list(result)[0].count
        logger.info(f"Created monthly analysis table with {row_count:,} rows")
        
        return monthly_table
    
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