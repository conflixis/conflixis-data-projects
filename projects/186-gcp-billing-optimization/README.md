# BigQuery Cost Optimization Project

## Overview

This project optimizes the `conflixis_agent` BigQuery dataset to reduce daily query costs by 95% (from $1,471/day to ~$74/day when queries run).

**Jira Ticket**: DA-186  
**Target Dataset**: `data-analytics-389803.conflixis_data_projects`

## Problem Statement

The current `conflixis_agent` dataset has critical inefficiencies:
- 83% of joins require expensive CAST operations
- 60% of tables lack partitioning (causing 12.94 TB scans)
- 0% of tables have clustering
- NPI columns have 3 different data types across tables

## Solution Approach

Creating optimized tables in the new `conflixis_data_projects` dataset with:
1. **Harmonized data types** (all NPI columns as INT64)
2. **Partitioning** on date columns
3. **Clustering** on join columns
4. **Materialized views** for expensive aggregations

## Project Structure

```
186-gcp-billing-optimization/
├── docs/                    # Documentation
│   ├── REQUIREMENTS.md      # Detailed optimization requirements
│   ├── IMPLEMENTATION_PROGRESS.md  # Progress tracking
│   └── reports/            # Analysis reports
├── scripts/                # Python scripts
│   ├── 01_schema_analysis.py   # Analyze current schema
│   ├── 02_cost_optimizer.py    # Cost optimization analysis
│   ├── 03_query_analysis.py    # Query pattern analysis
│   └── optimization.sh         # Shell optimization script
├── sql/                    # SQL scripts
│   ├── harmonized_views.sql    # Data type harmonization
│   ├── partitioned_tables.sql  # Partitioning & clustering
│   └── materialized_views.sql  # Materialized views
└── archive/                # Old/redundant files
```

## Quick Start

### Prerequisites

1. Set up environment:
```bash
pip install -r requirements.txt
```

2. Ensure `.env` file has GCP credentials:
```bash
GCP_SERVICE_ACCOUNT_KEY='{"type": "service_account", ...}'
```

### Running the Analysis

1. **Analyze current schema**:
```bash
python scripts/01_schema_analysis.py
```

2. **Analyze cost optimization opportunities**:
```bash
python scripts/02_cost_optimizer.py
```

3. **Analyze query patterns**:
```bash
python scripts/03_query_analysis.py
```

### Implementation Phases

#### Week 1: Data Harmonization
- Create harmonized views with consistent INT64 types
- Expected: 20-30% cost reduction

#### Week 2: Partitioning & Clustering
- Create partitioned and clustered tables
- Expected: 90% reduction in data scanned

#### Week 3: Materialized Views
- Pre-compute expensive aggregations
- Expected: 95% cost reduction for aggregate queries

#### Week 4: Migration & Testing
- Update production queries
- Validate cost savings

## Monitoring Progress

Track implementation progress in `docs/IMPLEMENTATION_PROGRESS.md`

## Key Metrics

| Metric | Before | After (Target) | Reduction |
|--------|--------|----------------|-----------|
| Daily Cost | $1,471 | $74 | 95% |
| Data Scanned | 12.94 TB | 0.65 TB | 95% |
| Query Time | ~120s | <10s | 92% |
| CAST Operations | 83% | 0% | 100% |

## Important Notes

- Original tables remain in `conflixis_agent` for rollback
- All optimized tables created in `conflixis_data_projects`
- Side-by-side comparison possible before full migration

## Support

For questions or issues, contact the Data Analytics team or refer to the Jira ticket DA-186.

---

*Last Updated: 2025-09-08*