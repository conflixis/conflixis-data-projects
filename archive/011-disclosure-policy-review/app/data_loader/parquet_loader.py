"""
Efficient Parquet data loader with caching
"""
import os
import json
from pathlib import Path
from typing import Optional, Dict, Any
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime


class ParquetDataLoader:
    """Handles loading and caching of Parquet data files"""
    
    def __init__(self, data_dir: Optional[Path] = None):
        """Initialize the data loader
        
        Args:
            data_dir: Path to data directory. If None, uses ../data/staging
        """
        if data_dir is None:
            # Default to the staging directory relative to app
            self.data_dir = Path(__file__).parent.parent.parent / "data" / "staging"
        else:
            self.data_dir = Path(data_dir)
        
        self._cache: Dict[str, pd.DataFrame] = {}
        self._metadata: Dict[str, Any] = {}
        self._last_load_time: Dict[str, datetime] = {}
        
    def load_disclosures(self, force_reload: bool = False) -> pd.DataFrame:
        """Load disclosure data from Parquet file
        
        Args:
            force_reload: Force reload from disk even if cached
            
        Returns:
            DataFrame with disclosure data
        """
        cache_key = "disclosures"
        
        # Check if we have cached data and it's recent
        if not force_reload and cache_key in self._cache:
            return self._cache[cache_key]
        
        # Find the most recent Parquet file
        parquet_files = list(self.data_dir.glob("disclosures_*.parquet"))
        if not parquet_files:
            # Fallback to JSON if no Parquet files
            return self._load_from_json()
        
        # Sort by modification time and get the most recent
        latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)
        
        # Load the Parquet file
        df = pd.read_parquet(latest_file)
        
        # Cache the data
        self._cache[cache_key] = df
        self._last_load_time[cache_key] = datetime.now()
        
        # Load metadata if available
        self._load_metadata()
        
        return df
    
    def _load_from_json(self) -> pd.DataFrame:
        """Fallback method to load from JSON file"""
        json_path = self.data_dir / "disclosure_data.json"
        
        if not json_path.exists():
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=[
                'id', 'provider_npi', 'provider_name', 'specialty', 'department',
                'entity_name', 'relationship_type', 'financial_amount',
                'open_payments_total', 'open_payments_matched', 'review_status',
                'risk_tier', 'risk_score', 'management_plan_required',
                'recusal_required', 'disclosure_date', 'relationship_start_date',
                'relationship_ongoing', 'last_review_date', 'next_review_date',
                'decision_authority_level', 'equity_percentage', 'board_position',
                'person_with_interest', 'notes', 'is_research'
            ])
        
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        # Extract metadata
        if 'metadata' in data:
            self._metadata = data['metadata']
        
        # Convert to DataFrame
        if 'disclosures' in data:
            df = pd.DataFrame(data['disclosures'])
        else:
            df = pd.DataFrame(data)
        
        return df
    
    def _load_metadata(self):
        """Load metadata from JSON file if available"""
        json_path = self.data_dir / "disclosure_data.json"
        
        if json_path.exists():
            try:
                with open(json_path, 'r') as f:
                    data = json.load(f)
                    if 'metadata' in data:
                        self._metadata = data['metadata']
            except Exception:
                pass
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get cached metadata"""
        return self._metadata
    
    def filter_disclosures(
        self,
        df: pd.DataFrame,
        provider_name: Optional[str] = None,
        entity_name: Optional[str] = None,
        risk_tier: Optional[str] = None,
        review_status: Optional[str] = None,
        min_amount: Optional[float] = None,
        max_amount: Optional[float] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        management_plan_required: Optional[bool] = None,
        is_research: Optional[bool] = None
    ) -> pd.DataFrame:
        """Apply filters to disclosure DataFrame
        
        Args:
            df: DataFrame to filter
            Various filter parameters
            
        Returns:
            Filtered DataFrame
        """
        result = df.copy()
        
        if provider_name:
            result = result[
                result['provider_name'].str.contains(provider_name, case=False, na=False)
            ]
        
        if entity_name:
            result = result[
                result['entity_name'].str.contains(entity_name, case=False, na=False)
            ]
        
        if risk_tier:
            result = result[result['risk_tier'] == risk_tier]
        
        if review_status:
            result = result[result['review_status'] == review_status]
        
        if min_amount is not None:
            result = result[result['financial_amount'] >= min_amount]
        
        if max_amount is not None:
            result = result[result['financial_amount'] <= max_amount]
        
        if start_date:
            result = result[pd.to_datetime(result['disclosure_date']) >= start_date]
        
        if end_date:
            result = result[pd.to_datetime(result['disclosure_date']) <= end_date]
        
        if management_plan_required is not None:
            result = result[result['management_plan_required'] == management_plan_required]
        
        if is_research is not None:
            result = result[result['is_research'] == is_research]
        
        return result
    
    def calculate_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate statistics from disclosure DataFrame
        
        Args:
            df: DataFrame with disclosure data
            
        Returns:
            Dictionary with statistics
        """
        if df.empty:
            return {
                'total_records': 0,
                'risk_distribution': {},
                'review_status_distribution': {},
                'average_amount': 0,
                'median_amount': 0,
                'max_amount': 0,
                'management_plans_required': 0,
                'unique_providers': 0,
                'unique_entities': 0,
                'open_payments_matched': 0,
                'research_disclosures': 0
            }
        
        return {
            'total_records': len(df),
            'risk_distribution': df['risk_tier'].value_counts().to_dict(),
            'review_status_distribution': df['review_status'].value_counts().to_dict(),
            'average_amount': float(df['financial_amount'].mean()),
            'median_amount': float(df['financial_amount'].median()),
            'max_amount': float(df['financial_amount'].max()),
            'management_plans_required': int(df['management_plan_required'].sum()),
            'unique_providers': df['provider_name'].nunique(),
            'unique_entities': df['entity_name'].nunique(),
            'open_payments_matched': int(df['open_payments_matched'].sum()),
            'research_disclosures': int(df['is_research'].sum())
        }
    
    def clear_cache(self):
        """Clear all cached data"""
        self._cache.clear()
        self._metadata.clear()
        self._last_load_time.clear()