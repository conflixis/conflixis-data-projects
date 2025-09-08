#!/bin/bash
# GCP BigQuery Optimization Script
# Generated: 2025-09-04 10:07:15
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


# =========================================
# STORAGE OPTIMIZATION COMMANDS
# =========================================


# Dataset: conflixis_data_projects
# Potential Savings: $86.87/month
if confirm "Optimize storage for conflixis_data_projects?"; then
    echo "Analyzing table age for conflixis_data_projects..."
    
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
    FROM `conflixis_data_projects.__TABLES__`
    WHERE TIMESTAMP_MILLIS(GREATEST(creation_time, IFNULL(last_modified_time, 0))) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    ORDER BY size_bytes DESC'
    
    echo "Review the above tables for archival or deletion."
fi


# Dataset: CONFLIXIS_309340
# Potential Savings: $65.91/month
if confirm "Optimize storage for CONFLIXIS_309340?"; then
    echo "Analyzing table age for CONFLIXIS_309340..."
    
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
    FROM `CONFLIXIS_309340.__TABLES__`
    WHERE TIMESTAMP_MILLIS(GREATEST(creation_time, IFNULL(last_modified_time, 0))) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    ORDER BY size_bytes DESC'
    
    echo "Review the above tables for archival or deletion."
fi


# Dataset: Conflixis_op_report_2025
# Potential Savings: $18.06/month
if confirm "Optimize storage for Conflixis_op_report_2025?"; then
    echo "Analyzing table age for Conflixis_op_report_2025..."
    
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
    FROM `Conflixis_op_report_2025.__TABLES__`
    WHERE TIMESTAMP_MILLIS(GREATEST(creation_time, IFNULL(last_modified_time, 0))) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    ORDER BY size_bytes DESC'
    
    echo "Review the above tables for archival or deletion."
fi


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
