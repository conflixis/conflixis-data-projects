# BigQuery Schema Analysis Report
## Dataset: data-analytics-389803.conflixis_agent

## Dataset Summary
- **Total Tables**: 5
- **Total Columns**: 146
- **Total Size**: 13446.12 GB
- **Largest Table**: PHYSICIAN_RX_2020_2024 (13285.98 GB)

## Tables with Healthcare Identifiers
- **npi**: 5 tables, types: NUMERIC, INT64, STRING

## Data Type Mismatches
- **npi**: 5 tables with types: NUMERIC, INT64, STRING
  - Affected tables: PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT, PHYSICIANS_OVERVIEW, PHYSICIAN_RX_2020_2024, op_general_all_aggregate_static, rx_op_enhanced_full

## Potential Join Relationships
- **npi**: 5 tables can be joined
  - Tables: PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT, PHYSICIANS_OVERVIEW, PHYSICIAN_RX_2020_2024, op_general_all_aggregate_static, rx_op_enhanced_full

## Performance Optimization Opportunities
- **Missing clustering on joinable table**: 3 instances
- **Data type mismatch for joins**: 1 instances

### Detailed Issues:
- PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT, PHYSICIANS_OVERVIEW, PHYSICIAN_RX_2020_2024, op_general_all_aggregate_static, rx_op_enhanced_full: Column pattern 'npi' has types: NUMERIC, INT64, STRING
  - Recommendation: Standardize data types for efficient joins
- PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT: nan
  - Recommendation: Add clustering by npi column
- PHYSICIANS_OVERVIEW: nan
  - Recommendation: Add clustering by npi column
- rx_op_enhanced_full: nan
  - Recommendation: Add clustering by npi column

## Partitioning Status
- No partitioned tables found

## Largest Tables
- **PHYSICIAN_RX_2020_2024**: 13285.98 GB, 10,668,624,488 rows
- **rx_op_enhanced_full**: 87.07 GB, 298,795,689 rows
- **op_general_all_aggregate_static**: 69.97 GB, 60,810,628 rows
- **PHYSICIANS_OVERVIEW**: 1.77 GB, 3,069,438 rows
- **PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT**: 1.33 GB, 7,903,751 rows