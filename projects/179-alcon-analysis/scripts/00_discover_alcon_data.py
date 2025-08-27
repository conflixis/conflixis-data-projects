#!/usr/bin/env python3
"""
Discover Alcon manufacturer variations in BigQuery Open Payments data
"""

import os
import sys
import json
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

def create_bigquery_client():
    """Create BigQuery client with credentials"""
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    if not service_account_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment")
    
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    
    return bigquery.Client(credentials=credentials, project="data-analytics-389803")

# Initialize BigQuery client
client = create_bigquery_client()

def discover_alcon_manufacturers():
    """Find all Alcon variations in the manufacturer field"""
    
    print("=" * 80)
    print("DISCOVERING ALCON MANUFACTURER VARIATIONS")
    print("=" * 80)
    
    # Query to find all Alcon-related manufacturer names
    # Using the correct table: conflixis_agent.op_general_all_aggregate_static
    query = """
    SELECT DISTINCT
        applicable_manufacturer_or_applicable_gpo_making_payment_name as manufacturer_name,
        COUNT(*) as payment_count,
        SUM(total_amount_of_payment_usdollars) as total_payments,
        MIN(EXTRACT(YEAR FROM date_of_payment)) as min_year,
        MAX(EXTRACT(YEAR FROM date_of_payment)) as max_year
    FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static`
    WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%ALCON%'
    GROUP BY manufacturer_name
    ORDER BY total_payments DESC
    """
    
    print("\nQuerying for Alcon manufacturer variations...")
    results = client.query(query).to_dataframe()
    
    print(f"\nFound {len(results)} Alcon-related manufacturer variations:")
    print("-" * 80)
    
    for idx, row in results.iterrows():
        print(f"\nManufacturer: {row['manufacturer_name']}")
        print(f"  Payment Count: {row['payment_count']:,}")
        print(f"  Total Payments: ${row['total_payments']:,.2f}")
        print(f"  Years: {int(row['min_year'])} - {int(row['max_year'])}")
    
    # Save results
    output_path = Path(__file__).parent.parent / "data" / "processed"
    output_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results.to_csv(output_path / f"alcon_manufacturer_variations_{timestamp}.csv", index=False)
    
    return results

def check_alcon_products():
    """Check for Alcon products in general payments"""
    
    print("\n" + "=" * 80)
    print("CHECKING ALCON PRODUCTS")
    print("=" * 80)
    
    # Check for products in the name_of_drug_or_biological_or_device_or_medical_supply_1 field
    query = """
    SELECT DISTINCT
        name_of_drug_or_biological_or_device_or_medical_supply_1 as product_name,
        COUNT(*) as payment_count,
        SUM(total_amount_of_payment_usdollars) as total_payments
    FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static`
    WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%ALCON%'
        AND name_of_drug_or_biological_or_device_or_medical_supply_1 IS NOT NULL
    GROUP BY product_name
    ORDER BY total_payments DESC
    LIMIT 20
    """
    
    print("\nQuerying for Alcon products...")
    results = client.query(query).to_dataframe()
    
    if not results.empty:
        print(f"\nTop Alcon products:")
        print("-" * 80)
        for idx, row in results.iterrows():
            print(f"\nProduct: {row['product_name']}")
            print(f"  Payment Count: {row['payment_count']:,}")
            print(f"  Total Payments: ${row['total_payments']:,.2f}")
    
    return results

def check_data_coverage():
    """Check available years and data volume"""
    
    print("\n" + "=" * 80)
    print("DATA COVERAGE ANALYSIS")
    print("=" * 80)
    
    query = """
    SELECT 
        EXTRACT(YEAR FROM date_of_payment) as year,
        COUNT(*) as payment_count,
        COUNT(DISTINCT covered_recipient_profile_id) as unique_providers,
        SUM(total_amount_of_payment_usdollars) as total_payments,
        AVG(total_amount_of_payment_usdollars) as avg_payment
    FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static`
    WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%ALCON%'
    GROUP BY year
    ORDER BY year DESC
    """
    
    print("\nAnalyzing data coverage by year...")
    results = client.query(query).to_dataframe()
    
    print(f"\nAlcon Payment Data Coverage:")
    print("-" * 80)
    for idx, row in results.iterrows():
        print(f"\nYear {int(row['year'])}:")
        print(f"  Payments: {row['payment_count']:,}")
        print(f"  Unique Providers: {row['unique_providers']:,}")
        print(f"  Total Amount: ${row['total_payments']:,.2f}")
        print(f"  Average Payment: ${row['avg_payment']:,.2f}")
    
    return results

def check_payment_categories():
    """Analyze payment categories for Alcon"""
    
    print("\n" + "=" * 80)
    print("PAYMENT CATEGORIES ANALYSIS")
    print("=" * 80)
    
    query = """
    SELECT 
        nature_of_payment_or_transfer_of_value as payment_category,
        COUNT(*) as payment_count,
        SUM(total_amount_of_payment_usdollars) as total_amount,
        AVG(total_amount_of_payment_usdollars) as avg_amount
    FROM `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static`
    WHERE UPPER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%ALCON%'
    GROUP BY payment_category
    ORDER BY total_amount DESC
    """
    
    print("\nAnalyzing payment categories...")
    results = client.query(query).to_dataframe()
    
    print(f"\nAlcon Payment Categories:")
    print("-" * 80)
    for idx, row in results.iterrows():
        print(f"\nCategory: {row['payment_category']}")
        print(f"  Count: {row['payment_count']:,}")
        print(f"  Total: ${row['total_amount']:,.2f}")
        print(f"  Average: ${row['avg_amount']:,.2f}")
    
    return results

if __name__ == "__main__":
    try:
        # Run discovery functions
        manufacturers = discover_alcon_manufacturers()
        products = check_alcon_products()
        coverage = check_data_coverage()
        categories = check_payment_categories()
        
        print("\n" + "=" * 80)
        print("DISCOVERY COMPLETE")
        print("=" * 80)
        print(f"\nResults saved to: projects/179-alcon-analysis/data/processed/")
        
    except Exception as e:
        print(f"\nError during discovery: {str(e)}")
        import traceback
        traceback.print_exc()