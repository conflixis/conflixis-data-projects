#!/usr/bin/env python3
"""
Deep dive analysis into BigQuery costs - the primary cost driver.
"""

import os
import json
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

def setup_client():
    """Setup BigQuery client with credentials."""
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
    
    service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
    service_account_json = service_account_json.replace('\\\\n', '\\n')
    service_account_info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return bigquery.Client(credentials=credentials, project="data-analytics-389803")

def analyze_bigquery_usage(client):
    """Deep analysis of BigQuery usage patterns."""
    
    print("=" * 80)
    print("BIGQUERY DEEP DIVE ANALYSIS")
    print("=" * 80)
    
    # 1. BigQuery costs by project and operation type
    query1 = """
    SELECT 
        project.name as project_name,
        sku.description as operation_type,
        DATE(usage_start_time) as usage_date,
        SUM(cost) as daily_cost,
        SUM(usage.amount) as usage_amount,
        usage.unit as usage_unit
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
    WHERE service.description = 'BigQuery'
        AND DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    GROUP BY project_name, operation_type, usage_date, usage_unit
    ORDER BY usage_date DESC, daily_cost DESC
    """
    
    print("\n📊 BIGQUERY COSTS BY PROJECT AND OPERATION TYPE:")
    print("-" * 80)
    bq_details = client.query(query1).to_dataframe()
    
    # Summarize by project
    project_summary = bq_details.groupby('project_name').agg({
        'daily_cost': 'sum',
        'usage_amount': 'sum'
    }).round(2).sort_values('daily_cost', ascending=False)
    
    print("\nBy Project:")
    print(project_summary.to_string())
    
    # Summarize by operation type
    operation_summary = bq_details.groupby('operation_type').agg({
        'daily_cost': 'sum',
        'usage_amount': 'sum',
        'usage_unit': 'first'
    }).round(2).sort_values('daily_cost', ascending=False)
    
    print("\nBy Operation Type:")
    print(operation_summary.to_string())
    
    # 2. Identify specific queries or jobs causing high costs
    query2 = """
    WITH daily_analysis AS (
        SELECT 
            DATE(usage_start_time) as usage_date,
            EXTRACT(HOUR FROM usage_start_time) as hour,
            project.name as project_name,
            sku.description as sku_type,
            SUM(cost) as hourly_cost,
            SUM(usage.amount) as usage_amount
        FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
        WHERE service.description = 'BigQuery'
            AND DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
            AND cost > 0
        GROUP BY usage_date, hour, project_name, sku_type
    )
    SELECT 
        usage_date,
        hour,
        project_name,
        sku_type,
        hourly_cost,
        usage_amount
    FROM daily_analysis
    WHERE hourly_cost > 10  -- Focus on hours with significant costs
    ORDER BY hourly_cost DESC
    LIMIT 20
    """
    
    print("\n⏰ HIGH-COST PERIODS (Hourly costs > $10):")
    print("-" * 80)
    high_cost_periods = client.query(query2).to_dataframe()
    if not high_cost_periods.empty:
        print(high_cost_periods.to_string(index=False))
    else:
        print("No single hour exceeded $10 in costs")
    
    # 3. Storage vs Compute costs
    query3 = """
    SELECT 
        CASE 
            WHEN LOWER(sku.description) LIKE '%storage%' THEN 'Storage'
            WHEN LOWER(sku.description) LIKE '%analysis%' THEN 'Analysis/Compute'
            WHEN LOWER(sku.description) LIKE '%streaming%' THEN 'Streaming'
            ELSE 'Other'
        END as cost_category,
        SUM(cost) as total_cost,
        COUNT(*) as transaction_count,
        AVG(cost) as avg_cost_per_transaction
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
    WHERE service.description = 'BigQuery'
        AND DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    GROUP BY cost_category
    ORDER BY total_cost DESC
    """
    
    print("\n💾 STORAGE VS COMPUTE BREAKDOWN:")
    print("-" * 80)
    category_breakdown = client.query(query3).to_dataframe()
    print(category_breakdown.to_string(index=False))
    
    # 4. Dataset-level analysis
    query4 = """
    SELECT 
        resource.name as dataset_or_table,
        project.name as project_name,
        sku.description as operation,
        SUM(cost) as total_cost,
        COUNT(*) as operation_count
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_resource_v1_01A821_72C4A7_DB426E`
    WHERE service.description = 'BigQuery'
        AND DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        AND resource.name IS NOT NULL
        AND cost > 0
    GROUP BY dataset_or_table, project_name, operation
    ORDER BY total_cost DESC
    LIMIT 25
    """
    
    print("\n📁 TOP DATASETS/TABLES BY COST:")
    print("-" * 80)
    dataset_costs = client.query(query4).to_dataframe()
    print(dataset_costs.to_string(index=False))
    
    # 5. Recent spike analysis (Sept 2-3)
    query5 = """
    SELECT 
        DATE(usage_start_time) as usage_date,
        EXTRACT(HOUR FROM usage_start_time) as hour,
        project.name as project_name,
        sku.description as operation,
        resource.name as resource_name,
        SUM(cost) as cost,
        SUM(usage.amount) as usage_amount,
        usage.unit as unit
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_resource_v1_01A821_72C4A7_DB426E`
    WHERE service.description = 'BigQuery'
        AND DATE(usage_start_time) IN ('2025-09-02', '2025-09-03')
        AND cost > 5  -- Focus on significant costs
    GROUP BY usage_date, hour, project_name, operation, resource_name, unit
    ORDER BY cost DESC
    LIMIT 30
    """
    
    print("\n🔴 RECENT SPIKE ANALYSIS (Sept 2-3 High Costs):")
    print("-" * 80)
    spike_analysis = client.query(query5).to_dataframe()
    print(spike_analysis.to_string(index=False))
    
    return bq_details, high_cost_periods, dataset_costs

def analyze_anomalies(client):
    """Identify anomalies and unusual patterns."""
    
    print("\n" + "=" * 80)
    print("ANOMALY DETECTION")
    print("=" * 80)
    
    # Look for unusual patterns
    query = """
    WITH daily_costs AS (
        SELECT 
            DATE(usage_start_time) as usage_date,
            service.description as service_name,
            SUM(cost) as daily_cost
        FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
        WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY usage_date, service_name
    ),
    stats AS (
        SELECT 
            service_name,
            AVG(daily_cost) as avg_daily_cost,
            STDDEV(daily_cost) as stddev_cost
        FROM daily_costs
        WHERE usage_date < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        GROUP BY service_name
    )
    SELECT 
        d.usage_date,
        d.service_name,
        d.daily_cost,
        s.avg_daily_cost,
        s.stddev_cost,
        (d.daily_cost - s.avg_daily_cost) / NULLIF(s.stddev_cost, 0) as z_score
    FROM daily_costs d
    JOIN stats s ON d.service_name = s.service_name
    WHERE d.usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        AND ABS((d.daily_cost - s.avg_daily_cost) / NULLIF(s.stddev_cost, 0)) > 2
    ORDER BY ABS((d.daily_cost - s.avg_daily_cost) / NULLIF(s.stddev_cost, 0)) DESC
    """
    
    print("\n⚠️ ANOMALOUS SPENDING DAYS (>2 standard deviations from mean):")
    print("-" * 80)
    anomalies = client.query(query).to_dataframe()
    
    if not anomalies.empty:
        for _, row in anomalies.iterrows():
            if row['z_score'] > 0:
                print(f"📈 {row['usage_date']}: {row['service_name']} - ${row['daily_cost']:.2f} "
                      f"(usual: ${row['avg_daily_cost']:.2f}, {row['z_score']:.1f} std devs above)")
            else:
                print(f"📉 {row['usage_date']}: {row['service_name']} - ${row['daily_cost']:.2f} "
                      f"(usual: ${row['avg_daily_cost']:.2f}, {abs(row['z_score']):.1f} std devs below)")
    else:
        print("No significant anomalies detected")

def get_recommendations(client):
    """Generate cost optimization recommendations."""
    
    print("\n" + "=" * 80)
    print("COST OPTIMIZATION RECOMMENDATIONS")
    print("=" * 80)
    
    # Check for optimization opportunities
    recommendations = []
    
    # 1. Check long-term storage optimization
    query1 = """
    SELECT 
        SUM(CASE WHEN sku.description LIKE '%Active Logical Storage%' THEN cost ELSE 0 END) as active_storage_cost,
        SUM(CASE WHEN sku.description LIKE '%Long Term Logical Storage%' THEN cost ELSE 0 END) as longterm_storage_cost,
        SUM(CASE WHEN sku.description LIKE '%Active Logical Storage%' THEN usage.amount ELSE 0 END) as active_storage_bytes,
        SUM(CASE WHEN sku.description LIKE '%Long Term Logical Storage%' THEN usage.amount ELSE 0 END) as longterm_storage_bytes
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
    WHERE service.description = 'BigQuery'
        AND DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    """
    
    storage_analysis = client.query(query1).to_dataframe()
    
    if not storage_analysis.empty:
        active_cost = storage_analysis['active_storage_cost'].iloc[0]
        longterm_cost = storage_analysis['longterm_storage_cost'].iloc[0]
        
        if active_cost > longterm_cost * 2:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Storage Optimization',
                'finding': f"Active storage costs (${active_cost:.2f}) are significantly higher than long-term storage (${longterm_cost:.2f})",
                'recommendation': "Review tables not accessed in 90+ days and consider partitioning or archiving strategies",
                'potential_savings': f"${(active_cost * 0.3):.2f}/month"
            })
    
    # 2. Check for repeated queries
    query2 = """
    SELECT 
        sku.description,
        COUNT(*) as query_count,
        SUM(cost) as total_cost,
        AVG(cost) as avg_cost
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
    WHERE service.description = 'BigQuery'
        AND sku.description LIKE '%Analysis%'
        AND DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY sku.description
    HAVING COUNT(*) > 50
    """
    
    query_patterns = client.query(query2).to_dataframe()
    
    if not query_patterns.empty:
        total_analysis_cost = query_patterns['total_cost'].sum()
        if total_analysis_cost > 500:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Query Optimization',
                'finding': f"High BigQuery analysis costs (${total_analysis_cost:.2f} in last 7 days)",
                'recommendation': "Consider implementing query result caching, materialized views, or scheduled queries for frequently-run analyses",
                'potential_savings': f"${(total_analysis_cost * 0.4):.2f}/week"
            })
    
    # 3. Check for cost spikes
    if True:  # Based on earlier analysis showing 342% increase
        recommendations.append({
            'priority': 'CRITICAL',
            'category': 'Cost Spike',
            'finding': "BigQuery costs increased 342% week-over-week ($1,817.77 increase)",
            'recommendation': "Investigate large analysis jobs on Sept 2-3, implement query cost controls and slot reservations",
            'potential_savings': "$900/week if reduced to baseline"
        })
    
    # Print recommendations
    for i, rec in enumerate(recommendations, 1):
        print(f"\n{i}. [{rec['priority']}] {rec['category']}")
        print(f"   Finding: {rec['finding']}")
        print(f"   Recommendation: {rec['recommendation']}")
        print(f"   Potential Savings: {rec['potential_savings']}")
    
    return recommendations

def main():
    """Main execution."""
    client = setup_client()
    
    # Deep dive into BigQuery
    bq_details, high_cost_periods, dataset_costs = analyze_bigquery_usage(client)
    
    # Detect anomalies
    analyze_anomalies(client)
    
    # Get recommendations
    recommendations = get_recommendations(client)
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    main()