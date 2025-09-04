#!/usr/bin/env python3
"""
Research script to analyze GCP billing data for the last 2 weeks.
This script performs comprehensive analysis to understand costs and patterns.
"""

import os
import json
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

def setup_client():
    """Setup BigQuery client with credentials."""
    # Load environment variables
    from pathlib import Path
    env_file = Path(__file__).parent.parent.parent / ".env"
    if env_file.exists():
        with open(env_file, 'r') as f:
            content = f.read()
            lines = content.split('\n')
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    if key == 'GCP_SERVICE_ACCOUNT_KEY':
                        if value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                    else:
                        value = value.strip().strip("'\"")
                    os.environ[key] = value
    
    # Get service account JSON
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    service_account_json = service_account_json.replace('\\\\n', '\\n')
    
    # Parse and create credentials
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    
    # Create client
    return bigquery.Client(credentials=credentials, project="data-analytics-389803")

def get_table_schema(client):
    """Get schema information for billing tables."""
    print("=" * 80)
    print("ANALYZING TABLE STRUCTURE")
    print("=" * 80)
    
    # Get schema for the main billing table
    tables = [
        "billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E",
        "billing-administration-389502.all_billing_data.gcp_billing_export_resource_v1_01A821_72C4A7_DB426E"
    ]
    
    for table_id in tables:
        try:
            table = client.get_table(table_id)
            print(f"\n📊 Table: {table_id.split('.')[-1]}")
            print(f"   Total rows: {table.num_rows:,}")
            print(f"   Size: {table.num_bytes / (1024**3):.2f} GB")
            print(f"   Created: {table.created}")
            print(f"   Last modified: {table.modified}")
            
            print("\n   Key fields available:")
            important_fields = [
                'usage_start_time', 'usage_end_time', 'service', 'sku', 'project',
                'cost', 'usage', 'credits', 'invoice', 'cost_type', 'currency',
                'location', 'labels', 'system_labels', 'tags', 'resource'
            ]
            
            for field in table.schema:
                if any(name in field.name.lower() for name in important_fields):
                    print(f"   - {field.name}: {field.field_type} {field.mode}")
        except Exception as e:
            print(f"   ❌ Error getting table info: {e}")

def analyze_last_two_weeks(client):
    """Comprehensive analysis of the last 2 weeks of billing data."""
    print("\n" + "=" * 80)
    print("LAST 2 WEEKS COMPREHENSIVE BILLING ANALYSIS")
    print("=" * 80)
    
    # 1. Total costs by service
    query1 = """
    SELECT 
        service.description as service_name,
        SUM(cost) as total_cost,
        SUM(CASE WHEN DATE(usage_start_time) = CURRENT_DATE() THEN cost ELSE 0 END) as today_cost,
        SUM(CASE WHEN DATE(usage_start_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN cost ELSE 0 END) as yesterday_cost,
        COUNT(DISTINCT DATE(usage_start_time)) as active_days,
        COUNT(DISTINCT project.id) as num_projects
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
    WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    GROUP BY service_name
    HAVING total_cost > 0
    ORDER BY total_cost DESC
    """
    
    print("\n📊 SERVICE COST BREAKDOWN (Last 14 days):")
    print("-" * 80)
    results = client.query(query1).to_dataframe()
    
    # Format and display results
    results['total_cost'] = results['total_cost'].round(2)
    results['today_cost'] = results['today_cost'].round(2)
    results['yesterday_cost'] = results['yesterday_cost'].round(2)
    
    print(results.to_string(index=False))
    print(f"\n💰 TOTAL SPEND (14 days): ${results['total_cost'].sum():.2f}")
    
    # 2. Daily spending trend
    query2 = """
    SELECT 
        DATE(usage_start_time) as usage_date,
        service.description as service_name,
        SUM(cost) as daily_cost
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
    WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    GROUP BY usage_date, service_name
    ORDER BY usage_date DESC, daily_cost DESC
    """
    
    print("\n📈 DAILY SPENDING TRENDS:")
    print("-" * 80)
    daily_results = client.query(query2).to_dataframe()
    
    # Pivot to show daily totals
    daily_pivot = daily_results.pivot_table(
        index='usage_date', 
        values='daily_cost', 
        aggfunc='sum'
    ).sort_index(ascending=False)
    
    print("\nDaily Totals:")
    for date, cost in daily_pivot.iterrows():
        print(f"  {date}: ${cost['daily_cost']:.2f}")
    
    # 3. Project-level costs
    query3 = """
    SELECT 
        project.name as project_name,
        service.description as service_name,
        SUM(cost) as total_cost,
        COUNT(DISTINCT DATE(usage_start_time)) as active_days
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
    WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        AND project.name IS NOT NULL
    GROUP BY project_name, service_name
    HAVING total_cost > 0.01
    ORDER BY total_cost DESC
    LIMIT 20
    """
    
    print("\n🏗️ TOP PROJECT & SERVICE COMBINATIONS:")
    print("-" * 80)
    project_results = client.query(query3).to_dataframe()
    print(project_results.to_string(index=False))
    
    # 4. SKU-level analysis for top service (BigQuery)
    query4 = """
    SELECT 
        sku.description as sku_name,
        SUM(cost) as total_cost,
        SUM(usage.amount) as total_usage,
        usage.unit as usage_unit,
        COUNT(*) as transaction_count
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
    WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        AND service.description = 'BigQuery'
    GROUP BY sku_name, usage_unit
    HAVING total_cost > 0
    ORDER BY total_cost DESC
    """
    
    print("\n🔍 BIGQUERY DETAILED SKU BREAKDOWN:")
    print("-" * 80)
    sku_results = client.query(query4).to_dataframe()
    print(sku_results.to_string(index=False))
    
    # 5. Credit analysis
    query5 = """
    SELECT 
        credits.name as credit_type,
        SUM(credits.amount) as credit_amount,
        COUNT(*) as credit_count
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`,
        UNNEST(credits) as credits
    WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    GROUP BY credit_type
    ORDER BY credit_amount DESC
    """
    
    print("\n💳 CREDITS & DISCOUNTS APPLIED:")
    print("-" * 80)
    try:
        credit_results = client.query(query5).to_dataframe()
        if not credit_results.empty:
            print(credit_results.to_string(index=False))
            print(f"\nTotal credits: ${credit_results['credit_amount'].sum():.2f}")
        else:
            print("No credits applied in this period")
    except:
        print("No credits data available")
    
    # 6. Resource-level analysis
    query6 = """
    SELECT 
        service.description as service_name,
        resource.name as resource_name,
        SUM(cost) as total_cost
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_resource_v1_01A821_72C4A7_DB426E`
    WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        AND resource.name IS NOT NULL
        AND cost > 0
    GROUP BY service_name, resource_name
    ORDER BY total_cost DESC
    LIMIT 15
    """
    
    print("\n🖥️ TOP RESOURCES BY COST:")
    print("-" * 80)
    try:
        resource_results = client.query(query6).to_dataframe()
        print(resource_results.to_string(index=False))
    except Exception as e:
        print(f"Resource-level data not available: {e}")
    
    # 7. Location-based costs
    query7 = """
    SELECT 
        location.location as region,
        service.description as service_name,
        SUM(cost) as total_cost
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
    WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        AND location.location IS NOT NULL
    GROUP BY region, service_name
    HAVING total_cost > 0.01
    ORDER BY total_cost DESC
    LIMIT 15
    """
    
    print("\n🌍 COSTS BY REGION:")
    print("-" * 80)
    location_results = client.query(query7).to_dataframe()
    print(location_results.to_string(index=False))
    
    return results, daily_results, project_results

def analyze_cost_patterns(client):
    """Analyze cost patterns and anomalies."""
    print("\n" + "=" * 80)
    print("COST PATTERNS & INSIGHTS")
    print("=" * 80)
    
    # Week-over-week comparison
    query = """
    WITH weekly_costs AS (
        SELECT 
            CASE 
                WHEN DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN 'This Week'
                ELSE 'Last Week'
            END as week_period,
            service.description as service_name,
            SUM(cost) as total_cost
        FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
        WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        GROUP BY week_period, service_name
    )
    SELECT 
        service_name,
        SUM(CASE WHEN week_period = 'This Week' THEN total_cost ELSE 0 END) as this_week,
        SUM(CASE WHEN week_period = 'Last Week' THEN total_cost ELSE 0 END) as last_week,
        SUM(CASE WHEN week_period = 'This Week' THEN total_cost ELSE 0 END) - 
        SUM(CASE WHEN week_period = 'Last Week' THEN total_cost ELSE 0 END) as change,
        CASE 
            WHEN SUM(CASE WHEN week_period = 'Last Week' THEN total_cost ELSE 0 END) > 0
            THEN ((SUM(CASE WHEN week_period = 'This Week' THEN total_cost ELSE 0 END) / 
                  SUM(CASE WHEN week_period = 'Last Week' THEN total_cost ELSE 0 END)) - 1) * 100
            ELSE 0
        END as percent_change
    FROM weekly_costs
    GROUP BY service_name
    HAVING (this_week > 0 OR last_week > 0)
    ORDER BY ABS(change) DESC
    """
    
    print("\n📊 WEEK-OVER-WEEK COMPARISON:")
    print("-" * 80)
    comparison = client.query(query).to_dataframe()
    
    # Format the output
    comparison['this_week'] = comparison['this_week'].round(2)
    comparison['last_week'] = comparison['last_week'].round(2)
    comparison['change'] = comparison['change'].round(2)
    comparison['percent_change'] = comparison['percent_change'].round(1)
    
    print(comparison.to_string(index=False))
    
    # Identify significant changes
    print("\n⚠️ SIGNIFICANT CHANGES:")
    significant = comparison[abs(comparison['percent_change']) > 20]
    if not significant.empty:
        for _, row in significant.iterrows():
            trend = "📈" if row['change'] > 0 else "📉"
            print(f"  {trend} {row['service_name']}: {row['percent_change']:.1f}% change (${row['change']:.2f})")
    else:
        print("  No significant changes detected (>20% change)")

def main():
    """Main execution function."""
    print("=" * 80)
    print("GCP BILLING ANALYSIS - LAST 2 WEEKS")
    print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Setup client
    client = setup_client()
    
    # Get table schema information
    get_table_schema(client)
    
    # Analyze last two weeks
    service_costs, daily_costs, project_costs = analyze_last_two_weeks(client)
    
    # Analyze patterns
    analyze_cost_patterns(client)
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    main()