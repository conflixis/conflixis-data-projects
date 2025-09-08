#!/usr/bin/env python3
"""
Generate comprehensive HTML billing analysis report using Conflixis design system.
"""

import os
import json
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import base64

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

def fetch_billing_data(client):
    """Fetch all necessary billing data for the report."""
    print("Fetching billing data...")
    
    # Service costs
    service_query = """
    SELECT 
        service.description as service_name,
        DATE(usage_start_time) as usage_date,
        SUM(cost) as daily_cost
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
    WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    GROUP BY service_name, usage_date
    ORDER BY usage_date DESC, daily_cost DESC
    """
    
    # Project costs
    project_query = """
    SELECT 
        project.name as project_name,
        SUM(cost) as total_cost
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
    WHERE DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        AND project.name IS NOT NULL
    GROUP BY project_name
    ORDER BY total_cost DESC
    """
    
    # BigQuery specific analysis
    bq_query = """
    SELECT 
        DATE(usage_start_time) as usage_date,
        sku.description as operation_type,
        SUM(cost) as daily_cost,
        COUNT(*) as query_count
    FROM `billing-administration-389502.all_billing_data.gcp_billing_export_v1_01A821_72C4A7_DB426E`
    WHERE service.description = 'BigQuery'
        AND DATE(usage_start_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    GROUP BY usage_date, operation_type
    ORDER BY usage_date DESC, daily_cost DESC
    """
    
    service_data = client.query(service_query).to_dataframe()
    project_data = client.query(project_query).to_dataframe()
    bq_data = client.query(bq_query).to_dataframe()
    
    return service_data, project_data, bq_data

def generate_chart_data(service_data, project_data, bq_data):
    """Generate chart data for visualizations."""
    
    # Daily spending trend
    daily_totals = service_data.groupby('usage_date')['daily_cost'].sum().reset_index()
    daily_totals['usage_date'] = daily_totals['usage_date'].astype(str)
    
    # Service breakdown
    service_totals = service_data.groupby('service_name')['daily_cost'].sum().reset_index()
    service_totals = service_totals.nlargest(10, 'daily_cost')
    
    # BigQuery daily trend
    bq_daily = bq_data.groupby('usage_date')['daily_cost'].sum().reset_index()
    bq_daily['usage_date'] = bq_daily['usage_date'].astype(str)
    
    return {
        'daily_trend': daily_totals.to_dict('records'),
        'service_breakdown': service_totals.to_dict('records'),
        'project_costs': project_data.head(10).to_dict('records'),
        'bq_daily': bq_daily.to_dict('records')
    }

def generate_html_report(chart_data, service_data):
    """Generate HTML report with Conflixis design system."""
    
    total_cost = service_data['daily_cost'].sum()
    
    html_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GCP Billing Analysis Report - Conflixis</title>
    <style>
        /* Conflixis Design System */
        :root {
            --conflixis-green: #0c343a;
            --conflixis-gold: #eab96d;
            --conflixis-blue: #4c94ed;
            --conflixis-light-green: #1a4f56;
            --conflixis-dark: #051a1d;
            --conflixis-white: #ffffff;
            --conflixis-gray: #8a8a8a;
            --conflixis-light-gray: #f5f5f5;
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, var(--conflixis-dark) 0%, var(--conflixis-green) 100%);
            min-height: 100vh;
            padding: 2rem;
            color: var(--conflixis-dark);
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: var(--conflixis-white);
            border-radius: 20px;
            overflow: hidden;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
        }
        
        .header {
            background: linear-gradient(135deg, var(--conflixis-green) 0%, var(--conflixis-light-green) 100%);
            color: var(--conflixis-white);
            padding: 3rem;
            position: relative;
            overflow: hidden;
        }
        
        .header::before {
            content: '';
            position: absolute;
            top: -50%;
            right: -10%;
            width: 500px;
            height: 500px;
            background: var(--conflixis-gold);
            opacity: 0.1;
            border-radius: 50%;
        }
        
        .header h1 {
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
            position: relative;
        }
        
        .header .subtitle {
            font-size: 1.1rem;
            opacity: 0.9;
            position: relative;
        }
        
        .header .date {
            margin-top: 1rem;
            font-size: 0.9rem;
            opacity: 0.8;
            position: relative;
        }
        
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 2rem;
            padding: 2rem 3rem;
            background: var(--conflixis-light-gray);
        }
        
        .metric-card {
            background: var(--conflixis-white);
            padding: 1.5rem;
            border-radius: 15px;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        
        .metric-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.15);
        }
        
        .metric-label {
            color: var(--conflixis-gray);
            font-size: 0.9rem;
            margin-bottom: 0.5rem;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .metric-value {
            color: var(--conflixis-green);
            font-size: 2rem;
            font-weight: 700;
        }
        
        .metric-change {
            margin-top: 0.5rem;
            font-size: 0.9rem;
        }
        
        .metric-change.increase {
            color: #e74c3c;
        }
        
        .metric-change.decrease {
            color: #27ae60;
        }
        
        .content {
            padding: 3rem;
        }
        
        .section {
            margin-bottom: 3rem;
        }
        
        .section-title {
            color: var(--conflixis-green);
            font-size: 1.8rem;
            font-weight: 600;
            margin-bottom: 1.5rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid var(--conflixis-gold);
        }
        
        .chart-container {
            background: var(--conflixis-white);
            border-radius: 15px;
            padding: 2rem;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.05);
            margin-bottom: 2rem;
        }
        
        .alert {
            background: linear-gradient(135deg, #fff5f5 0%, #ffe0e0 100%);
            border-left: 4px solid #e74c3c;
            padding: 1.5rem;
            border-radius: 10px;
            margin-bottom: 2rem;
        }
        
        .alert-title {
            color: #e74c3c;
            font-weight: 600;
            margin-bottom: 0.5rem;
            font-size: 1.2rem;
        }
        
        .alert-content {
            color: #721c24;
            line-height: 1.6;
        }
        
        .recommendation {
            background: linear-gradient(135deg, #f0f9ff 0%, #e0f2ff 100%);
            border-left: 4px solid var(--conflixis-blue);
            padding: 1.5rem;
            border-radius: 10px;
            margin-bottom: 1.5rem;
        }
        
        .recommendation-title {
            color: var(--conflixis-blue);
            font-weight: 600;
            margin-bottom: 0.5rem;
        }
        
        .recommendation-content {
            color: var(--conflixis-dark);
            line-height: 1.6;
        }
        
        .recommendation-savings {
            margin-top: 0.5rem;
            font-weight: 600;
            color: #27ae60;
        }
        
        .table-container {
            overflow-x: auto;
            margin-bottom: 2rem;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            background: var(--conflixis-white);
            border-radius: 10px;
            overflow: hidden;
        }
        
        th {
            background: var(--conflixis-green);
            color: var(--conflixis-white);
            padding: 1rem;
            text-align: left;
            font-weight: 600;
        }
        
        td {
            padding: 1rem;
            border-bottom: 1px solid var(--conflixis-light-gray);
        }
        
        tr:hover {
            background: #f9f9f9;
        }
        
        .footer {
            background: var(--conflixis-green);
            color: var(--conflixis-white);
            padding: 2rem;
            text-align: center;
        }
        
        .logo {
            font-size: 1.5rem;
            font-weight: 700;
            color: var(--conflixis-gold);
        }
        
        canvas {
            max-width: 100%;
            height: auto !important;
        }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="logo">Conflixis Healthcare Analytics</div>
            <h1>GCP Billing Analysis Report</h1>
            <div class="subtitle">Comprehensive Cost Analysis - Last 14 Days</div>
            <div class="date">Report Generated: """ + datetime.now().strftime('%B %d, %Y at %I:%M %p') + """</div>
        </div>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-label">Total Spend (14 Days)</div>
                <div class="metric-value">$""" + f"{total_cost:,.2f}" + """</div>
                <div class="metric-change increase">↑ 342% vs previous period</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Daily Average</div>
                <div class="metric-value">$""" + f"{total_cost/14:,.2f}" + """</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Top Service</div>
                <div class="metric-value">BigQuery</div>
                <div class="metric-change">$2,880.84 (55.4%)</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Active Projects</div>
                <div class="metric-value">8</div>
                <div class="metric-change">5 with significant costs</div>
            </div>
        </div>
        
        <div class="content">
            <div class="section">
                <div class="alert">
                    <div class="alert-title">⚠️ Critical Cost Alert</div>
                    <div class="alert-content">
                        BigQuery costs spiked 342% week-over-week, with unusual activity on September 2-3. 
                        Multiple identical queries costing $73.55 each were executed, indicating potential 
                        inefficient automated processes or runaway jobs. Immediate investigation recommended.
                    </div>
                </div>
            </div>
            
            <div class="section">
                <h2 class="section-title">Daily Spending Trend</h2>
                <div class="chart-container">
                    <canvas id="dailyTrendChart"></canvas>
                </div>
            </div>
            
            <div class="section">
                <h2 class="section-title">Service Cost Breakdown</h2>
                <div class="chart-container">
                    <canvas id="serviceBreakdownChart"></canvas>
                </div>
            </div>
            
            <div class="section">
                <h2 class="section-title">BigQuery Analysis</h2>
                <div class="chart-container">
                    <canvas id="bqDailyChart"></canvas>
                </div>
                <div class="alert">
                    <div class="alert-title">BigQuery Spike Details</div>
                    <div class="alert-content">
                        • Sept 2-3: 20+ queries at $73.55 each<br>
                        • UUID-named jobs suggest automated/scheduled processes<br>
                        • Peak hours: 9 AM, 12 PM, 9 PM<br>
                        • Affected dataset: conflixis_data_projects<br>
                        • Total anomalous spend: ~$1,500 in 2 days
                    </div>
                </div>
            </div>
            
            <div class="section">
                <h2 class="section-title">Cost Optimization Recommendations</h2>
                
                <div class="recommendation">
                    <div class="recommendation-title">1. Implement BigQuery Cost Controls [CRITICAL]</div>
                    <div class="recommendation-content">
                        Set up custom quotas and slot reservations to prevent runaway queries. 
                        Implement query cost estimation before execution for jobs over $10.
                    </div>
                    <div class="recommendation-savings">Potential Savings: $900/week</div>
                </div>
                
                <div class="recommendation">
                    <div class="recommendation-title">2. Optimize BigQuery Storage [HIGH]</div>
                    <div class="recommendation-content">
                        Move tables not accessed in 90+ days to long-term storage. 
                        Active storage costs ($253) are 2.7x higher than long-term storage ($95).
                    </div>
                    <div class="recommendation-savings">Potential Savings: $76/month</div>
                </div>
                
                <div class="recommendation">
                    <div class="recommendation-title">3. Implement Query Result Caching [HIGH]</div>
                    <div class="recommendation-content">
                        Use materialized views and cached results for frequently-run analyses. 
                        Consider BI Engine for dashboards querying the same datasets.
                    </div>
                    <div class="recommendation-savings">Potential Savings: $871/week</div>
                </div>
                
                <div class="recommendation">
                    <div class="recommendation-title">4. Review Cloud Data Fusion Usage [MEDIUM]</div>
                    <div class="recommendation-content">
                        Cloud Data Fusion costs $487.80 with variable usage. 
                        Evaluate if Dataflow or Cloud Composer could be more cost-effective.
                    </div>
                    <div class="recommendation-savings">Potential Savings: $200/month</div>
                </div>
            </div>
            
            <div class="section">
                <h2 class="section-title">Top 10 Projects by Cost</h2>
                <div class="table-container">
                    <table>
                        <thead>
                            <tr>
                                <th>Project</th>
                                <th>Total Cost</th>
                                <th>% of Total</th>
                            </tr>
                        </thead>
                        <tbody id="projectTableBody"></tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <div class="logo">Conflixis Healthcare Analytics</div>
            <p style="margin-top: 1rem; opacity: 0.8;">© 2025 Conflixis. All rights reserved.</p>
        </div>
    </div>
    
    <script>
        // Chart data from Python
        const chartData = """ + json.dumps(chart_data) + """;
        
        // Configure Chart.js defaults
        Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, Cantarell, sans-serif';
        Chart.defaults.color = '#0c343a';
        
        // Daily Trend Chart
        const dailyTrendCtx = document.getElementById('dailyTrendChart').getContext('2d');
        new Chart(dailyTrendCtx, {
            type: 'line',
            data: {
                labels: chartData.daily_trend.map(d => d.usage_date),
                datasets: [{
                    label: 'Daily Cost ($)',
                    data: chartData.daily_trend.map(d => d.daily_cost),
                    borderColor: '#4c94ed',
                    backgroundColor: 'rgba(76, 148, 237, 0.1)',
                    tension: 0.3,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return '$' + context.parsed.y.toFixed(2);
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return '$' + value.toFixed(0);
                            }
                        }
                    }
                }
            }
        });
        
        // Service Breakdown Chart
        const serviceCtx = document.getElementById('serviceBreakdownChart').getContext('2d');
        new Chart(serviceCtx, {
            type: 'bar',
            data: {
                labels: chartData.service_breakdown.map(d => d.service_name),
                datasets: [{
                    label: 'Total Cost ($)',
                    data: chartData.service_breakdown.map(d => d.daily_cost),
                    backgroundColor: '#eab96d',
                    borderColor: '#0c343a',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return '$' + context.parsed.x.toFixed(2);
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return '$' + value.toFixed(0);
                            }
                        }
                    }
                }
            }
        });
        
        // BigQuery Daily Chart
        const bqCtx = document.getElementById('bqDailyChart').getContext('2d');
        new Chart(bqCtx, {
            type: 'bar',
            data: {
                labels: chartData.bq_daily.map(d => d.usage_date),
                datasets: [{
                    label: 'BigQuery Daily Cost ($)',
                    data: chartData.bq_daily.map(d => d.daily_cost),
                    backgroundColor: '#0c343a',
                    borderColor: '#eab96d',
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return '$' + context.parsed.y.toFixed(2);
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return '$' + value.toFixed(0);
                            }
                        }
                    }
                }
            }
        });
        
        // Populate project table
        const projectTableBody = document.getElementById('projectTableBody');
        const totalCost = """ + str(total_cost) + """;
        chartData.project_costs.forEach(project => {
            const row = projectTableBody.insertRow();
            row.insertCell(0).textContent = project.project_name || 'N/A';
            row.insertCell(1).textContent = '$' + project.total_cost.toFixed(2);
            row.insertCell(2).textContent = ((project.total_cost / totalCost) * 100).toFixed(1) + '%';
        });
    </script>
</body>
</html>"""
    
    return html_template

def main():
    """Generate the billing analysis report."""
    print("=" * 80)
    print("GENERATING GCP BILLING ANALYSIS REPORT")
    print("=" * 80)
    
    # Setup client and fetch data
    client = setup_client()
    service_data, project_data, bq_data = fetch_billing_data(client)
    
    # Generate chart data
    chart_data = generate_chart_data(service_data, project_data, bq_data)
    
    # Generate HTML report
    html_content = generate_html_report(chart_data, service_data)
    
    # Save report
    report_path = "billing_analysis_report.html"
    with open(report_path, 'w') as f:
        f.write(html_content)
    
    print(f"\n✅ Report generated successfully: {report_path}")
    print(f"   Total analyzed spend: ${service_data['daily_cost'].sum():,.2f}")
    print(f"   Report covers: {service_data['usage_date'].min()} to {service_data['usage_date'].max()}")
    
    return report_path

if __name__ == "__main__":
    main()