"""
JSON data loader for disclosure data
"""
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
import pandas as pd
from datetime import datetime


class JSONDataLoader:
    """Load disclosure data from JSON files"""
    
    def __init__(self, data_dir: Optional[Path] = None):
        """Initialize the loader
        
        Args:
            data_dir: Optional path to data directory
        """
        if data_dir is None:
            # Default to project data directory
            self.data_dir = Path(__file__).parent.parent.parent / "data"
        else:
            self.data_dir = Path(data_dir)
            
        self.staging_dir = self.data_dir / "staging"
    
    def load_disclosures(self) -> pd.DataFrame:
        """Load disclosure data from JSON file
        
        Returns:
            DataFrame with disclosure records
        """
        json_path = self.staging_dir / "disclosure_data.json"
        
        if not json_path.exists():
            raise FileNotFoundError(f"Data file not found: {json_path}")
        
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        # Extract disclosures array
        disclosures = data.get('disclosures', [])
        
        if not disclosures:
            raise ValueError("No disclosure data found in JSON file")
        
        # Convert to DataFrame
        df = pd.DataFrame(disclosures)
        
        # Ensure required columns exist with defaults
        required_columns = {
            'id': '',
            'provider_name': '',
            'provider_npi': '',
            'provider_email': '',
            'category_label': 'Open Payments',
            'relationship_type': 'Not Specified',
            'entity_name': '',
            'financial_amount': 0.0,
            'risk_tier': 'low',
            'review_status': 'pending',
            'management_plan_required': False,
            'recusal_required': False,
            'relationship_ongoing': False,
            'is_research': False,
            'notes': '',
            'disclosure_date': datetime.now().strftime('%Y-%m-%d'),
            'last_review_date': datetime.now().strftime('%Y-%m-%d'),
            'next_review_date': '2025-12-31',
            'job_title': 'Not Specified',
            'department': 'Texas Health',
            'open_payments_total': 0.0,
            'open_payments_matched': False,
            'risk_score': 0,
            'decision_authority_level': 'staff',
            'equity_percentage': 0.0,
            'board_position': False,
            'person_with_interest': '',
            'relationship_start_date': datetime.now().strftime('%Y-%m-%d'),
            'document_id': ''
        }
        
        for col, default in required_columns.items():
            if col not in df.columns:
                df[col] = default
            else:
                # Fill None/NaN values with defaults for string columns
                if isinstance(default, str):
                    df[col] = df[col].fillna(default).astype(str)
                    # Replace 'None' string with default
                    df[col] = df[col].replace('None', default)
                    df[col] = df[col].replace('', default) if default else df[col]
        
        # Convert data types
        df['financial_amount'] = pd.to_numeric(df['financial_amount'], errors='coerce').fillna(0)
        df['management_plan_required'] = df['management_plan_required'].astype(bool)
        df['recusal_required'] = df['recusal_required'].astype(bool)
        df['relationship_ongoing'] = df['relationship_ongoing'].astype(bool)
        df['is_research'] = df['is_research'].astype(bool)
        
        return df
    
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
        is_research: Optional[bool] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Filter disclosure dataframe
        
        Args:
            df: DataFrame to filter
            Various filter parameters
            
        Returns:
            Filtered DataFrame
        """
        filtered = df.copy()
        
        if provider_name:
            filtered = filtered[
                filtered['provider_name'].str.contains(provider_name, case=False, na=False)
            ]
        
        if entity_name:
            filtered = filtered[
                filtered['entity_name'].str.contains(entity_name, case=False, na=False)
            ]
        
        if risk_tier:
            filtered = filtered[filtered['risk_tier'] == risk_tier]
        
        if review_status:
            filtered = filtered[filtered['review_status'] == review_status]
        
        if min_amount is not None:
            filtered = filtered[filtered['financial_amount'] >= min_amount]
        
        if max_amount is not None:
            filtered = filtered[filtered['financial_amount'] <= max_amount]
        
        if management_plan_required is not None:
            filtered = filtered[filtered['management_plan_required'] == management_plan_required]
        
        if is_research is not None:
            filtered = filtered[filtered['is_research'] == is_research]
        
        if start_date:
            filtered = filtered[filtered['disclosure_date'] >= start_date]
        
        if end_date:
            filtered = filtered[filtered['disclosure_date'] <= end_date]
        
        return filtered
    
    def get_statistics(self, df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """Calculate statistics for disclosure data
        
        Args:
            df: Optional DataFrame, will load if not provided
            
        Returns:
            Dictionary with statistics
        """
        if df is None:
            df = self.load_disclosures()
        
        return {
            'total_records': len(df),
            'unique_providers': df['provider_name'].nunique(),
            'unique_entities': df['entity_name'].nunique(),
            'risk_distribution': df['risk_tier'].value_counts().to_dict(),
            'review_status_distribution': df['review_status'].value_counts().to_dict(),
            'average_amount': float(df['financial_amount'].mean()),
            'median_amount': float(df['financial_amount'].median()),
            'max_amount': float(df['financial_amount'].max()),
            'management_plans_required': int(df['management_plan_required'].sum()),
            'open_payments_matched': int(df['open_payments_matched'].sum()),
            'research_disclosures': int(df['is_research'].sum())
        }