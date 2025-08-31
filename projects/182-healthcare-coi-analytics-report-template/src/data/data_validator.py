"""
Data Validator Module
Validates data quality and integrity
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DataValidator:
    """Data quality validation and reporting"""
    
    def __init__(self):
        """Initialize data validator"""
        self.validation_results = []
        self.errors = []
        self.warnings = []
    
    def validate_dataframe(
        self, 
        df: pd.DataFrame, 
        name: str,
        required_columns: Optional[List[str]] = None,
        numeric_columns: Optional[List[str]] = None,
        date_columns: Optional[List[str]] = None,
        id_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Comprehensive DataFrame validation
        
        Args:
            df: DataFrame to validate
            name: Name of the dataset
            required_columns: Columns that must be present
            numeric_columns: Columns that should be numeric
            date_columns: Columns that should be dates
            id_columns: Columns that should be unique identifiers
            
        Returns:
            Validation report dictionary
        """
        report = {
            'dataset': name,
            'timestamp': datetime.now().isoformat(),
            'rows': len(df),
            'columns': len(df.columns),
            'checks': [],
            'passed': True
        }
        
        # Check for empty DataFrame
        if df.empty:
            report['checks'].append({
                'check': 'not_empty',
                'passed': False,
                'message': 'DataFrame is empty'
            })
            report['passed'] = False
            self.errors.append(f"{name}: DataFrame is empty")
            return report
        
        # Check required columns
        if required_columns:
            missing = set(required_columns) - set(df.columns)
            check = {
                'check': 'required_columns',
                'passed': len(missing) == 0,
                'missing_columns': list(missing)
            }
            report['checks'].append(check)
            if missing:
                report['passed'] = False
                self.errors.append(f"{name}: Missing columns {missing}")
        
        # Check numeric columns
        if numeric_columns:
            for col in numeric_columns:
                if col in df.columns:
                    is_numeric = pd.api.types.is_numeric_dtype(df[col])
                    check = {
                        'check': f'numeric_{col}',
                        'passed': is_numeric,
                        'dtype': str(df[col].dtype)
                    }
                    report['checks'].append(check)
                    if not is_numeric:
                        self.warnings.append(f"{name}: Column {col} is not numeric")
        
        # Check date columns
        if date_columns:
            for col in date_columns:
                if col in df.columns:
                    is_datetime = pd.api.types.is_datetime64_any_dtype(df[col])
                    check = {
                        'check': f'datetime_{col}',
                        'passed': is_datetime,
                        'dtype': str(df[col].dtype)
                    }
                    report['checks'].append(check)
                    if not is_datetime:
                        self.warnings.append(f"{name}: Column {col} is not datetime")
        
        # Check ID columns for uniqueness
        if id_columns:
            for col in id_columns:
                if col in df.columns:
                    duplicates = df[col].duplicated().sum()
                    check = {
                        'check': f'unique_{col}',
                        'passed': duplicates == 0,
                        'duplicates': int(duplicates)
                    }
                    report['checks'].append(check)
                    if duplicates > 0:
                        self.warnings.append(f"{name}: Column {col} has {duplicates} duplicates")
        
        # Data quality checks
        quality_checks = self._check_data_quality(df)
        report['quality'] = quality_checks
        
        # Store validation result
        self.validation_results.append(report)
        
        return report
    
    def _check_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform data quality checks
        
        Args:
            df: DataFrame to check
            
        Returns:
            Quality metrics dictionary
        """
        quality = {
            'null_counts': {},
            'zero_counts': {},
            'negative_counts': {},
            'outliers': {}
        }
        
        for col in df.columns:
            # Null counts
            null_count = df[col].isnull().sum()
            if null_count > 0:
                quality['null_counts'][col] = int(null_count)
            
            # For numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                # Zero counts
                zero_count = (df[col] == 0).sum()
                if zero_count > 0:
                    quality['zero_counts'][col] = int(zero_count)
                
                # Negative counts
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    quality['negative_counts'][col] = int(negative_count)
                
                # Outlier detection using IQR
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
                if outliers > 0:
                    quality['outliers'][col] = {
                        'count': int(outliers),
                        'lower_bound': float(lower_bound),
                        'upper_bound': float(upper_bound)
                    }
        
        return quality
    
    def validate_npi(self, npi: str) -> bool:
        """
        Validate NPI using Luhn algorithm
        
        Args:
            npi: NPI string
            
        Returns:
            True if valid NPI
        """
        if not npi or len(npi) != 10:
            return False
        
        try:
            # NPI validation using Luhn algorithm
            npi_digits = [int(d) for d in npi]
            check_digit = npi_digits[-1]
            
            # Double every other digit starting from right
            total = 0
            for i in range(8, -1, -1):
                if (8 - i) % 2 == 0:
                    doubled = npi_digits[i] * 2
                    total += doubled if doubled < 10 else doubled - 9
                else:
                    total += npi_digits[i]
            
            # Add constant 24 (per NPI standard)
            total += 24
            
            # Check digit should make total divisible by 10
            return (total + check_digit) % 10 == 0
            
        except (ValueError, IndexError):
            return False
    
    def validate_provider_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate provider-specific data
        
        Args:
            df: Provider DataFrame
            
        Returns:
            Validation report
        """
        report = self.validate_dataframe(
            df, 
            'Provider Data',
            required_columns=['NPI'],
            id_columns=['NPI']
        )
        
        # Validate NPIs
        if 'NPI' in df.columns:
            invalid_npis = []
            for npi in df['NPI'].dropna().unique():
                if not self.validate_npi(str(npi)):
                    invalid_npis.append(npi)
            
            if invalid_npis:
                report['invalid_npis'] = invalid_npis[:10]  # Show first 10
                report['invalid_npi_count'] = len(invalid_npis)
                self.warnings.append(f"Found {len(invalid_npis)} invalid NPIs")
        
        return report
    
    def validate_payment_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate Open Payments data
        
        Args:
            df: Payments DataFrame
            
        Returns:
            Validation report
        """
        report = self.validate_dataframe(
            df,
            'Payment Data',
            required_columns=['physician_id', 'payment_amount', 'payment_year'],
            numeric_columns=['payment_amount', 'payment_year']
        )
        
        # Check for reasonable payment amounts
        if 'payment_amount' in df.columns:
            unreasonable = df[
                (df['payment_amount'] < 0) | 
                (df['payment_amount'] > 10000000)
            ]
            if not unreasonable.empty:
                report['unreasonable_payments'] = len(unreasonable)
                self.warnings.append(f"Found {len(unreasonable)} unreasonable payment amounts")
        
        # Check year range
        if 'payment_year' in df.columns:
            min_year = df['payment_year'].min()
            max_year = df['payment_year'].max()
            current_year = datetime.now().year
            
            if min_year < 2013 or max_year > current_year:
                report['year_range_issue'] = f"Years range from {min_year} to {max_year}"
                self.warnings.append(f"Suspicious year range: {min_year}-{max_year}")
        
        return report
    
    def validate_prescription_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate prescription data
        
        Args:
            df: Prescription DataFrame
            
        Returns:
            Validation report
        """
        report = self.validate_dataframe(
            df,
            'Prescription Data',
            required_columns=['NPI', 'BRAND_NAME', 'total_claims', 'total_cost'],
            numeric_columns=['total_claims', 'total_cost']
        )
        
        # Check for reasonable values
        if 'total_cost' in df.columns and 'total_claims' in df.columns:
            # Check for negative values
            negative_cost = (df['total_cost'] < 0).sum()
            negative_claims = (df['total_claims'] < 0).sum()
            
            if negative_cost > 0:
                report['negative_costs'] = int(negative_cost)
                self.errors.append(f"Found {negative_cost} negative cost values")
            
            if negative_claims > 0:
                report['negative_claims'] = int(negative_claims)
                self.errors.append(f"Found {negative_claims} negative claim counts")
            
            # Check cost per claim
            df['cost_per_claim'] = df['total_cost'] / df['total_claims'].replace(0, np.nan)
            unreasonable_cpp = df[
                (df['cost_per_claim'] < 1) | 
                (df['cost_per_claim'] > 100000)
            ]
            if not unreasonable_cpp.empty:
                report['unreasonable_cost_per_claim'] = len(unreasonable_cpp)
                self.warnings.append(f"Found {len(unreasonable_cpp)} unreasonable cost-per-claim values")
        
        return report
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """
        Get summary of all validation results
        
        Returns:
            Summary dictionary
        """
        return {
            'timestamp': datetime.now().isoformat(),
            'datasets_validated': len(self.validation_results),
            'total_errors': len(self.errors),
            'total_warnings': len(self.warnings),
            'errors': self.errors,
            'warnings': self.warnings,
            'results': self.validation_results
        }
    
    def print_summary(self):
        """Print validation summary to console"""
        summary = self.get_validation_summary()
        
        print("\n" + "="*60)
        print("DATA VALIDATION SUMMARY")
        print("="*60)
        print(f"Datasets Validated: {summary['datasets_validated']}")
        print(f"Errors: {summary['total_errors']}")
        print(f"Warnings: {summary['total_warnings']}")
        
        if self.errors:
            print("\nERRORS:")
            for error in self.errors:
                print(f"  ❌ {error}")
        
        if self.warnings:
            print("\nWARNINGS:")
            for warning in self.warnings:
                print(f"  ⚠️  {warning}")
        
        if not self.errors and not self.warnings:
            print("\n✅ All validations passed!")
        
        print("="*60)