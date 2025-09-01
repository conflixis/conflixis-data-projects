"""
BigQuery Connector Module
Handles all BigQuery connections and queries with proper error handling and caching
"""

import json
import os
import hashlib
import pickle
from pathlib import Path
from typing import Optional, Dict, Any, Union
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


class BigQueryConnector:
    """Singleton BigQuery connection manager with caching and retry logic"""
    
    _instance = None
    _client = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize BigQuery connector"""
        if self._client is None:
            self._initialize_client()
            self.cache_dir = Path("data/cache")
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            self.cache_ttl = timedelta(hours=24)
    
    def _initialize_client(self):
        """Initialize BigQuery client with credentials"""
        try:
            service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
            if not service_account_json:
                raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
            
            service_account_info = json.loads(service_account_json)
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=['https://www.googleapis.com/auth/bigquery']
            )
            
            self._client = bigquery.Client(
                credentials=credentials,
                project=service_account_info.get('project_id')
            )
            logger.info("BigQuery client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise
    
    @property
    def client(self) -> bigquery.Client:
        """Get BigQuery client instance"""
        if self._client is None:
            self._initialize_client()
        return self._client
    
    def _get_cache_key(self, query: str, params: Optional[Dict] = None) -> str:
        """Generate cache key for query"""
        cache_string = query
        if params:
            cache_string += str(sorted(params.items()))
        return hashlib.md5(cache_string.encode()).hexdigest()
    
    def _get_cached_result(self, cache_key: str) -> Optional[pd.DataFrame]:
        """Retrieve cached query result if valid"""
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        if cache_file.exists():
            try:
                mod_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
                if datetime.now() - mod_time < self.cache_ttl:
                    with open(cache_file, 'rb') as f:
                        logger.debug(f"Using cached result for {cache_key}")
                        return pickle.load(f)
            except Exception as e:
                logger.warning(f"Failed to load cache {cache_key}: {e}")
        return None
    
    def _save_to_cache(self, cache_key: str, df: pd.DataFrame):
        """Save query result to cache"""
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(df, f)
            logger.debug(f"Saved result to cache: {cache_key}")
        except Exception as e:
            logger.warning(f"Failed to save cache {cache_key}: {e}")
    
    def query(
        self, 
        query: str, 
        params: Optional[Dict] = None,
        use_cache: bool = True,
        max_retries: int = 3
    ) -> pd.DataFrame:
        """
        Execute BigQuery query with caching and retry logic
        
        Args:
            query: SQL query string
            params: Optional query parameters
            use_cache: Whether to use cached results
            max_retries: Maximum number of retry attempts
            
        Returns:
            Query results as DataFrame
        """
        # Check cache first
        if use_cache:
            cache_key = self._get_cache_key(query, params)
            cached_result = self._get_cached_result(cache_key)
            if cached_result is not None:
                return cached_result
        
        # Execute query with retry logic
        for attempt in range(max_retries):
            try:
                logger.info(f"Executing query (attempt {attempt + 1}/{max_retries})")
                
                # Configure query
                job_config = bigquery.QueryJobConfig()
                if params:
                    job_config.query_parameters = [
                        bigquery.ScalarQueryParameter(k, "STRING", v) 
                        for k, v in params.items()
                    ]
                
                # Execute query
                query_job = self.client.query(query, job_config=job_config)
                df = query_job.to_dataframe()
                
                logger.info(f"Query returned {len(df):,} rows")
                
                # Save to cache
                if use_cache:
                    self._save_to_cache(cache_key, df)
                
                return df
                
            except Exception as e:
                logger.error(f"Query attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                continue
    
    def get_table_info(self, dataset_id: str, table_id: str) -> Dict[str, Any]:
        """Get information about a BigQuery table"""
        try:
            table_ref = f"{self.client.project}.{dataset_id}.{table_id}"
            table = self.client.get_table(table_ref)
            
            return {
                'num_rows': table.num_rows,
                'num_bytes': table.num_bytes,
                'created': table.created,
                'modified': table.modified,
                'schema': [{'name': field.name, 'type': field.field_type} 
                          for field in table.schema]
            }
        except Exception as e:
            logger.error(f"Failed to get table info for {dataset_id}.{table_id}: {e}")
            raise
    
    def table_exists(self, dataset_id: str, table_id: str) -> bool:
        """Check if a table exists in BigQuery"""
        try:
            table_ref = f"{self.client.project}.{dataset_id}.{table_id}"
            self.client.get_table(table_ref)
            return True
        except Exception:
            return False
    
    def create_table_from_dataframe(
        self, 
        df: pd.DataFrame, 
        dataset_id: str, 
        table_id: str,
        overwrite: bool = False
    ):
        """Create or replace a BigQuery table from a DataFrame"""
        try:
            table_ref = f"{self.client.project}.{dataset_id}.{table_id}"
            
            job_config = bigquery.LoadJobConfig()
            if overwrite:
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            else:
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            
            job = self.client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            job.result()  # Wait for job to complete
            
            logger.info(f"Created/updated table {table_ref} with {len(df):,} rows")
            
        except Exception as e:
            logger.error(f"Failed to create table {dataset_id}.{table_id}: {e}")
            raise
    
    def clear_cache(self, older_than: Optional[timedelta] = None):
        """Clear cached query results"""
        try:
            if older_than:
                cutoff = datetime.now() - older_than
                for cache_file in self.cache_dir.glob("*.pkl"):
                    if datetime.fromtimestamp(cache_file.stat().st_mtime) < cutoff:
                        cache_file.unlink()
                        logger.debug(f"Removed old cache file: {cache_file.name}")
            else:
                for cache_file in self.cache_dir.glob("*.pkl"):
                    cache_file.unlink()
                logger.info("Cleared all cache files")
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")