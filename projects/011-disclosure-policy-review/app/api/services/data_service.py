"""
Data service for handling disclosure data operations
"""
from typing import List, Optional, Dict, Any
from pathlib import Path
import pandas as pd
from data_loader import JSONDataLoader
from api.models import (
    DisclosureRecord, DisclosureFilter, DisclosureResponse,
    DisclosureStats
)


class DataService:
    """Service for managing disclosure data operations"""
    
    def __init__(self, data_dir: Optional[Path] = None):
        """Initialize the data service
        
        Args:
            data_dir: Optional path to data directory
        """
        self.loader = JSONDataLoader(data_dir)
        self._df: Optional[pd.DataFrame] = None
    
    def initialize(self) -> bool:
        """Initialize the data service by loading data
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self._df = self.loader.load_disclosures()
            return True
        except Exception as e:
            print(f"Failed to initialize data service: {e}")
            return False
    
    def get_disclosures(
        self,
        page: int = 1,
        page_size: int = 50,
        filters: Optional[DisclosureFilter] = None
    ) -> DisclosureResponse:
        """Get paginated disclosure records
        
        Args:
            page: Page number (1-indexed)
            page_size: Number of records per page
            filters: Optional filters to apply
            
        Returns:
            DisclosureResponse with paginated data
        """
        if self._df is None:
            self.initialize()
        
        # Apply filters if provided
        if filters:
            filtered_df = self.loader.filter_disclosures(
                self._df,
                provider_name=filters.provider_name,
                entity_name=filters.entity_name,
                risk_tier=filters.risk_tier.value if filters.risk_tier else None,
                review_status=filters.review_status.value if filters.review_status else None,
                min_amount=filters.min_amount,
                max_amount=filters.max_amount,
                start_date=filters.start_date,
                end_date=filters.end_date,
                management_plan_required=filters.management_plan_required,
                is_research=filters.is_research
            )
        else:
            filtered_df = self._df
        
        # Calculate pagination
        total = len(filtered_df)
        total_pages = (total + page_size - 1) // page_size
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total)
        
        # Get page of data
        page_df = filtered_df.iloc[start_idx:end_idx]
        
        # Convert to Pydantic models
        records = [
            DisclosureRecord(**row.to_dict())
            for _, row in page_df.iterrows()
        ]
        
        return DisclosureResponse(
            total=total,
            page=page,
            page_size=page_size,
            pages=total_pages,
            data=records
        )
    
    def get_disclosure_by_id(self, disclosure_id: str) -> Optional[DisclosureRecord]:
        """Get a specific disclosure by ID
        
        Args:
            disclosure_id: The disclosure ID
            
        Returns:
            DisclosureRecord if found, None otherwise
        """
        if self._df is None:
            self.initialize()
        
        matches = self._df[self._df['id'] == disclosure_id]
        
        if matches.empty:
            return None
        
        row = matches.iloc[0]
        return DisclosureRecord(**row.to_dict())
    
    def search_disclosures(
        self,
        query: str,
        page: int = 1,
        page_size: int = 50
    ) -> DisclosureResponse:
        """Search disclosures by text query
        
        Args:
            query: Search query string
            page: Page number
            page_size: Records per page
            
        Returns:
            DisclosureResponse with search results
        """
        if self._df is None:
            self.initialize()
        
        # Search across multiple text fields
        mask = (
            self._df['provider_name'].str.contains(query, case=False, na=False) |
            self._df['entity_name'].str.contains(query, case=False, na=False) |
            self._df['category_label'].str.contains(query, case=False, na=False) |
            self._df['relationship_type'].str.contains(query, case=False, na=False) |
            self._df['notes'].str.contains(query, case=False, na=False)
        )
        
        filtered_df = self._df[mask]
        
        # Calculate pagination
        total = len(filtered_df)
        total_pages = (total + page_size - 1) // page_size
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total)
        
        # Get page of data
        page_df = filtered_df.iloc[start_idx:end_idx]
        
        # Convert to Pydantic models
        records = [
            DisclosureRecord(**row.to_dict())
            for _, row in page_df.iterrows()
        ]
        
        return DisclosureResponse(
            total=total,
            page=page,
            page_size=page_size,
            pages=total_pages,
            data=records
        )
    
    def get_statistics(self, filters: Optional[DisclosureFilter] = None) -> DisclosureStats:
        """Get statistics about disclosures
        
        Args:
            filters: Optional filters to apply before calculating stats
            
        Returns:
            DisclosureStats with calculated statistics
        """
        if self._df is None:
            self.initialize()
        
        # Apply filters if provided
        if filters:
            filtered_df = self.loader.filter_disclosures(
                self._df,
                provider_name=filters.provider_name,
                entity_name=filters.entity_name,
                risk_tier=filters.risk_tier.value if filters.risk_tier else None,
                review_status=filters.review_status.value if filters.review_status else None,
                min_amount=filters.min_amount,
                max_amount=filters.max_amount,
                start_date=filters.start_date,
                end_date=filters.end_date,
                management_plan_required=filters.management_plan_required,
                is_research=filters.is_research
            )
        else:
            filtered_df = self._df
        
        stats_dict = self.loader.get_statistics(filtered_df)
        return DisclosureStats(**stats_dict)
    
    def reload_data(self) -> bool:
        """Force reload data from disk
        
        Returns:
            True if successful
        """
        try:
            self._df = self.loader.load_disclosures()
            return True
        except Exception as e:
            print(f"Failed to reload data: {e}")
            return False
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get data metadata
        
        Returns:
            Dictionary with metadata
        """
        return self.loader.get_metadata()