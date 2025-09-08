#!/usr/bin/env python3
"""Test script to diagnose missing data in report tables"""

import sys
from pathlib import Path
import pandas as pd
import yaml

# Add src to path
sys.path.append(str(Path(__file__).parent))

from src.data import BigQueryConnector
from src.analysis.bigquery_analysis import BigQueryAnalyzer

def main():
    # Load config
    config_path = Path(__file__).parent / 'config' / 'config.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize
    print("Initializing BigQuery connection...")
    bq = BigQueryConnector()
    analyzer = BigQueryAnalyzer(bq.client, config, 2020, 2024)
    
    # Test each problematic query
    print("\n" + "="*60)
    print("TESTING PRESCRIPTION ANALYSIS")
    print("="*60)
    
    try:
        results = analyzer.analyze_prescriptions()
        print(f"\nKeys returned: {list(results.keys())}")
        
        # Check drug_specific
        if 'drug_specific' in results:
            df = results['drug_specific']
            print(f"\ndrug_specific:")
            print(f"  Type: {type(df)}")
            if isinstance(df, pd.DataFrame):
                print(f"  Shape: {df.shape}")
                if df.empty:
                    print("  ⚠️  DataFrame is EMPTY - Need to check query")
                else:
                    print(f"  Columns: {list(df.columns)}")
                    print(f"  Sample:\n{df.head(3)}")
        else:
            print("\n❌ drug_specific NOT FOUND in results")
        
        # Check by_provider_type
        if 'by_provider_type' in results:
            df = results['by_provider_type']
            print(f"\nby_provider_type:")
            print(f"  Type: {type(df)}")
            if isinstance(df, pd.DataFrame):
                print(f"  Shape: {df.shape}")
                if df.empty:
                    print("  ⚠️  DataFrame is EMPTY - Need to check query")
                else:
                    print(f"  Columns: {list(df.columns)}")
                    print(f"  Sample:\n{df.head(3)}")
        else:
            print("\n❌ by_provider_type NOT FOUND in results")
            
    except Exception as e:
        print(f"❌ Error in prescription analysis: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*60)
    print("TESTING CORRELATION ANALYSIS")
    print("="*60)
    
    try:
        results = analyzer.analyze_correlations()
        print(f"\nKeys returned: {list(results.keys())}")
        
        # Check consecutive_years
        if 'consecutive_years' in results:
            df = results['consecutive_years']
            print(f"\nconsecutive_years:")
            print(f"  Type: {type(df)}")
            if isinstance(df, pd.DataFrame):
                print(f"  Shape: {df.shape}")
                if df.empty:
                    print("  ⚠️  DataFrame is EMPTY - Need to check query")
                else:
                    print(f"  Columns: {list(df.columns)}")
                    print(f"  Sample:\n{df.head()}")
        else:
            print("\n❌ consecutive_years NOT FOUND in results")
            
    except Exception as e:
        print(f"❌ Error in correlation analysis: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*60)
    print("TESTING RISK ASSESSMENT")
    print("="*60)
    
    try:
        results = analyzer.analyze_risk_assessment()
        print(f"\nKeys returned: {list(results.keys())}")
        
        # Check risk_distribution
        if 'risk_distribution' in results:
            df = results['risk_distribution']
            print(f"\nrisk_distribution:")
            print(f"  Type: {type(df)}")
            if isinstance(df, pd.DataFrame):
                print(f"  Shape: {df.shape}")
                if df.empty:
                    print("  ⚠️  DataFrame is EMPTY - Need to check query")
                else:
                    print(f"  Columns: {list(df.columns)}")
                    print(f"  Sample:\n{df.head(3)}")
        else:
            print("\n❌ risk_distribution NOT FOUND in results")
            
    except Exception as e:
        print(f"❌ Error in risk assessment: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print("\nIssues to fix:")
    print("1. Check if queries are returning empty results")
    print("2. Verify table names in queries match actual BigQuery tables")
    print("3. Ensure proper joins between tables")
    print("4. Check if data exists for the specified year range")

if __name__ == '__main__':
    main()