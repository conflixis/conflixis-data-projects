#!/usr/bin/env python3
"""
Cost optimization scripts for GCP billing management.
Provides tools to analyze and optimize BigQuery costs.
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

def analyze_query_costs(client, days=7):
    """Analyze recent query costs to identify expensive operations."""
    
    print("=" * 80)
    print("BIGQUERY QUERY COST ANALYZER")
    print("=" * 80)
    
    query = f"""
    WITH query_costs AS (
        SELECT 
            DATE(usage_start_time) as query_date,
            EXTRACT(HOUR FROM usage_start_time) as query_hour,
            project.name as project_name,
            resource.name as job_or_dataset,
            sku.description as operation_type,
            cost,
            usage.amount as bytes_processed,
            usage.unit as unit
        FROM `billing-administration-389502.all_billing_data.gcp_billing_export_resource_v1_01A821_72C4A7_DB426E`
        WHERE service.description = 'BigQuery'
            AND DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
            AND sku.description LIKE '%Analysis%'
            AND cost > 0
        ORDER BY cost DESC
    )
    SELECT * FROM query_costs
    LIMIT 50
    """
    
    results = client.query(query).to_dataframe()
    
    if results.empty:
        print("No expensive queries found in the specified period.")
        return results
    
    # Analyze patterns
    print(f"\n📊 TOP EXPENSIVE QUERIES (Last {days} days):")
    print("-" * 80)
    
    for _, row in results.head(10).iterrows():
        print(f"Date: {row['query_date']} {row['query_hour']:02d}:00")
        print(f"  Project: {row['project_name']}")
        print(f"  Job/Dataset: {row['job_or_dataset']}")
        print(f"  Cost: ${row['cost']:.2f}")
        print(f"  Data Processed: {row['bytes_processed']/1e12:.2f} TB")
        print()
    
    # Identify patterns
    print("🔍 COST PATTERNS DETECTED:")
    print("-" * 80)
    
    # Check for repeated expensive queries
    job_costs = results.groupby('job_or_dataset').agg({
        'cost': ['sum', 'count', 'mean']
    }).round(2)
    
    job_costs.columns = ['total_cost', 'run_count', 'avg_cost']
    job_costs = job_costs.sort_values('total_cost', ascending=False)
    
    repeated_jobs = job_costs[job_costs['run_count'] > 1].head(5)
    if not repeated_jobs.empty:
        print("\n⚠️  Repeated Expensive Jobs:")
        for job_id, stats in repeated_jobs.iterrows():
            if stats['avg_cost'] > 5:  # Only show jobs costing more than $5
                print(f"  {job_id[:50]}...")
                print(f"    Runs: {int(stats['run_count'])}, Total: ${stats['total_cost']:.2f}, Avg: ${stats['avg_cost']:.2f}")
    
    # Peak hours analysis
    hourly_costs = results.groupby('query_hour')['cost'].sum().sort_values(ascending=False)
    print(f"\n⏰ Most Expensive Hours:")
    for hour, cost in hourly_costs.head(3).items():
        print(f"  {hour:02d}:00 - ${cost:.2f}")
    
    return results

def identify_storage_optimization(client):
    """Identify BigQuery tables that can be optimized for storage costs."""
    
    print("\n" + "=" * 80)
    print("STORAGE OPTIMIZATION ANALYZER")
    print("=" * 80)
    
    query = """
    WITH storage_costs AS (
        SELECT 
            resource.name as table_or_dataset,
            sku.description as storage_type,
            SUM(cost) as total_cost,
            SUM(usage.amount) as total_bytes,
            COUNT(DISTINCT DATE(usage_start_time)) as days_charged
        FROM `billing-administration-389502.all_billing_data.gcp_billing_export_resource_v1_01A821_72C4A7_DB426E`
        WHERE service.description = 'BigQuery'
            AND sku.description LIKE '%Storage%'
            AND DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY table_or_dataset, storage_type
    ),
    storage_summary AS (
        SELECT 
            table_or_dataset,
            SUM(CASE WHEN storage_type LIKE '%Active%' THEN total_cost ELSE 0 END) as active_cost,
            SUM(CASE WHEN storage_type LIKE '%Long Term%' THEN total_cost ELSE 0 END) as longterm_cost,
            SUM(CASE WHEN storage_type LIKE '%Active%' THEN total_bytes ELSE 0 END) as active_bytes,
            SUM(CASE WHEN storage_type LIKE '%Long Term%' THEN total_bytes ELSE 0 END) as longterm_bytes,
            MAX(days_charged) as days_charged
        FROM storage_costs
        GROUP BY table_or_dataset
    )
    SELECT 
        table_or_dataset,
        active_cost,
        longterm_cost,
        active_bytes / 1e12 as active_tb,
        longterm_bytes / 1e12 as longterm_tb,
        days_charged,
        CASE 
            WHEN active_cost > longterm_cost * 2 AND active_cost > 10 
            THEN 'HIGH OPTIMIZATION POTENTIAL'
            WHEN active_cost > longterm_cost AND active_cost > 5
            THEN 'MEDIUM OPTIMIZATION POTENTIAL'
            ELSE 'OPTIMIZED'
        END as optimization_status
    FROM storage_summary
    WHERE active_cost + longterm_cost > 1
    ORDER BY active_cost DESC
    LIMIT 20
    """
    
    storage_results = client.query(query).to_dataframe()
    
    if storage_results.empty:
        print("No significant storage costs found.")
        return []
    
    print("\n📦 STORAGE COST ANALYSIS:")
    print("-" * 80)
    
    recommendations = []
    total_savings = 0
    
    for _, row in storage_results.iterrows():
        if row['optimization_status'] != 'OPTIMIZED':
            print(f"\nDataset/Table: {row['table_or_dataset']}")
            print(f"  Active Storage: ${row['active_cost']:.2f} ({row['active_tb']:.2f} TB)")
            print(f"  Long-term Storage: ${row['longterm_cost']:.2f} ({row['longterm_tb']:.2f} TB)")
            print(f"  Status: {row['optimization_status']}")
            
            if row['active_cost'] > row['longterm_cost'] * 2:
                potential_savings = row['active_cost'] * 0.5
                total_savings += potential_savings
                print(f"  💡 Recommendation: Move to long-term storage")
                print(f"     Potential Savings: ${potential_savings:.2f}/month")
                
                recommendations.append({
                    'dataset': row['table_or_dataset'],
                    'current_cost': row['active_cost'],
                    'potential_savings': potential_savings,
                    'action': 'Move inactive data to long-term storage'
                })
    
    print(f"\n💰 TOTAL POTENTIAL STORAGE SAVINGS: ${total_savings:.2f}/month")
    
    return recommendations

def generate_cost_alerts(client, threshold=100):
    """Generate alerts for unusual cost patterns."""
    
    print("\n" + "=" * 80)
    print("COST ALERT GENERATOR")
    print("=" * 80)
    
    # Check for recent cost spikes
    query = f"""
    WITH daily_costs AS (
        SELECT 
            DATE(usage_start_time) as usage_date,
            service.description as service_name,
            SUM(cost) as daily_cost
        FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
        WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        GROUP BY usage_date, service_name
    ),
    cost_comparison AS (
        SELECT 
            usage_date,
            service_name,
            daily_cost,
            LAG(daily_cost) OVER (PARTITION BY service_name ORDER BY usage_date) as prev_day_cost
        FROM daily_costs
    )
    SELECT 
        usage_date,
        service_name,
        daily_cost,
        prev_day_cost,
        daily_cost - prev_day_cost as cost_increase,
        CASE 
            WHEN prev_day_cost > 0 
            THEN ((daily_cost - prev_day_cost) / prev_day_cost) * 100 
            ELSE 0 
        END as percent_increase
    FROM cost_comparison
    WHERE daily_cost - prev_day_cost > {threshold}
        OR (prev_day_cost > 0 AND ((daily_cost - prev_day_cost) / prev_day_cost) > 1)  -- 100% increase
    ORDER BY cost_increase DESC
    """
    
    alerts = client.query(query).to_dataframe()
    
    if alerts.empty:
        print(f"✅ No cost alerts (threshold: ${threshold})")
        return []
    
    print(f"\n🚨 COST ALERTS DETECTED:")
    print("-" * 80)
    
    alert_list = []
    for _, row in alerts.iterrows():
        severity = 'CRITICAL' if row['cost_increase'] > 500 else 'HIGH' if row['cost_increase'] > 200 else 'MEDIUM'
        
        print(f"\n[{severity}] {row['service_name']} on {row['usage_date']}")
        print(f"  Current: ${row['daily_cost']:.2f}")
        print(f"  Previous: ${row['prev_day_cost']:.2f}")
        print(f"  Increase: ${row['cost_increase']:.2f} ({row['percent_increase']:.1f}%)")
        
        alert_list.append({
            'date': str(row['usage_date']),
            'service': row['service_name'],
            'severity': severity,
            'current_cost': row['daily_cost'],
            'increase': row['cost_increase'],
            'percent_increase': row['percent_increase']
        })
    
    return alert_list

def generate_optimization_script(recommendations, output_file='optimization_actions.sh'):
    """Generate a shell script with BigQuery commands for optimization."""
    
    print("\n" + "=" * 80)
    print("GENERATING OPTIMIZATION SCRIPT")
    print("=" * 80)
    
    script_content = """#!/bin/bash
# GCP BigQuery Optimization Script
# Generated: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """
# 
# This script contains recommended BigQuery optimization commands.
# Review each command before execution.

set -e  # Exit on error

echo "========================================="
echo "BigQuery Cost Optimization Script"
echo "========================================="

# Function to confirm actions
confirm() {
    read -p "$1 (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Skipping..."
        return 1
    fi
    return 0
}

"""
    
    # Add storage optimization commands
    if recommendations:
        script_content += """
# =========================================
# STORAGE OPTIMIZATION COMMANDS
# =========================================

"""
        for rec in recommendations:
            dataset = rec['dataset']
            savings = rec['potential_savings']
            
            script_content += f"""
# Dataset: {dataset}
# Potential Savings: ${savings:.2f}/month
if confirm "Optimize storage for {dataset}?"; then
    echo "Analyzing table age for {dataset}..."
    
    # Command to identify tables not accessed in 90 days
    # Note: Adjust dataset and project names as needed
    bq query --use_legacy_sql=false '
    SELECT 
        table_name,
        TIMESTAMP_MILLIS(creation_time) as created,
        TIMESTAMP_MILLIS(GREATEST(creation_time, IFNULL(last_modified_time, 0))) as last_modified,
        size_bytes / POW(10, 9) as size_gb,
        CASE 
            WHEN type = 1 THEN "TABLE"
            WHEN type = 2 THEN "VIEW"
            ELSE "OTHER"
        END as table_type
    FROM `{dataset}.__TABLES__`
    WHERE TIMESTAMP_MILLIS(GREATEST(creation_time, IFNULL(last_modified_time, 0))) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    ORDER BY size_bytes DESC'
    
    echo "Review the above tables for archival or deletion."
fi

"""
    
    # Add query optimization recommendations
    script_content += """
# =========================================
# QUERY OPTIMIZATION SETTINGS
# =========================================

echo ""
echo "Recommended BigQuery Settings:"
echo "1. Enable query result caching (automatic)"
echo "2. Set maximum bytes billed per query:"
echo "   bq update --project_id=data-analytics-389803 --default_query_job_timeout=3600"
echo ""
echo "3. Create materialized views for frequently accessed data:"
echo "   Consider creating materialized views for datasets queried multiple times daily"
echo ""
echo "4. Enable BI Engine reservation for dashboard queries:"
echo "   Visit: https://console.cloud.google.com/bigquery/bi-engine"

# Set query size limit (10TB) to prevent runaway queries
if confirm "Set 10TB query size limit for project?"; then
    echo "Setting query size limit..."
    bq update --project_id=data-analytics-389803 --maximum_bytes_billed=10995116277760
    echo "✅ Query size limit set to 10TB"
fi

# Enable required APIs for cost monitoring
if confirm "Enable BigQuery Reservation API for slot management?"; then
    gcloud services enable bigqueryreservation.googleapis.com --project=data-analytics-389803
    echo "✅ BigQuery Reservation API enabled"
fi

echo ""
echo "========================================="
echo "Optimization script complete!"
echo "========================================="
"""
    
    # Save script
    with open(output_file, 'w') as f:
        f.write(script_content)
    
    os.chmod(output_file, 0o755)  # Make executable
    
    print(f"✅ Optimization script generated: {output_file}")
    print(f"   Review and run with: ./{output_file}")
    
    return output_file

def main():
    """Main execution for cost optimization analysis."""
    print("=" * 80)
    print("GCP COST OPTIMIZATION ANALYZER")
    print(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    client = setup_client()
    
    # 1. Analyze query costs
    print("\n[1/3] Analyzing query costs...")
    query_analysis = analyze_query_costs(client, days=7)
    
    # 2. Identify storage optimizations
    print("\n[2/3] Identifying storage optimizations...")
    storage_recommendations = identify_storage_optimization(client)
    
    # 3. Generate cost alerts
    print("\n[3/3] Checking for cost alerts...")
    alerts = generate_cost_alerts(client, threshold=50)
    
    # Generate optimization script
    if storage_recommendations:
        script_path = generate_optimization_script(storage_recommendations)
    
    # Summary
    print("\n" + "=" * 80)
    print("OPTIMIZATION SUMMARY")
    print("=" * 80)
    
    if storage_recommendations:
        total_savings = sum(r['potential_savings'] for r in storage_recommendations)
        print(f"✅ Found {len(storage_recommendations)} storage optimization opportunities")
        print(f"   Potential monthly savings: ${total_savings:.2f}")
    
    if alerts:
        print(f"⚠️  Generated {len(alerts)} cost alerts")
        critical_alerts = [a for a in alerts if a['severity'] == 'CRITICAL']
        if critical_alerts:
            print(f"   Including {len(critical_alerts)} CRITICAL alerts")
    
    print("\n📋 Next Steps:")
    print("1. Review the HTML report: billing_analysis_report.html")
    if storage_recommendations:
        print("2. Execute optimization script: ./optimization_actions.sh")
    print("3. Set up BigQuery slot reservations for predictable costs")
    print("4. Implement query cost pre-checks for expensive operations")
    
    return {
        'storage_recommendations': storage_recommendations,
        'alerts': alerts,
        'query_analysis': len(query_analysis) if not query_analysis.empty else 0
    }

if __name__ == "__main__":
    main()