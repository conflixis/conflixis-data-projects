# Exploratory Analysis Findings - RAMC - 2025-08-31

## Scripts Created
1. `01_payment_concentration_analysis.py` - Analyzed payment concentration and manufacturer patterns

## Key Discoveries

### 1. Market Concentration Analysis
- **Market Structure**: Competitive market (HHI: 683.17) despite high payment values
- **Top 10 Control**: 69.32% of total payments concentrated in top 10 manufacturers
- **Device vs Pharma Split**: Device manufacturers control 60% of payments despite engaging fewer providers

### 2. Payment Per Provider Disparities
- **Device Manufacturers**: Average $246,268 per provider
- **Pharmaceutical Companies**: Average $104,414 per provider
- **Ratio**: Device payments are 2.4x higher per provider, suggesting different engagement models

### 3. Sustained Relationship Patterns
- **Five-Year Relationships**: 4,320 providers (32.5%) received payments all 5 years
- **Payment Concentration**: These sustained relationships account for 90.4% of total payment value
- **Payment Differential**: Five-year providers receive 189x more than one-year providers ($26,028 vs $137)

### 4. Critical Outliers Requiring Investigation
- **Ignite Orthopedics, LLC**: Single provider received $2.1M (z-score: 3.7) - extreme outlier
- **Omnia Medical, LLC**: 2 providers received $2.3M total (average $1.14M per provider)
- **These outliers suggest potential consulting arrangements or royalty agreements requiring review**

### 5. Data Quality Issues
- **Duplicate Entries Identified**:
  - Intuitive Surgical appears twice (combined $9.4M)
  - Medtronic appears in multiple forms (combined $5.8M)
  - These duplicates may understate true concentration levels

## Data Limitations Encountered
- Could not perform full network analysis - individual payment dates not available
- Department vulnerability analysis limited - no specialty tags in aggregate data
- Cannot correlate payments with specific events without temporal detail

## Recommendations for Future Analysis
1. Obtain provider-level payment data for network analysis
2. Request specialty/department tags for vulnerability assessment
3. Get conference/event dates to correlate with payment patterns
4. Investigate the extreme outliers (Ignite Orthopedics, Omnia Medical)
5. Consolidate duplicate manufacturer entries for accurate concentration metrics

## Statistical Validation
All findings based on actual data from:
- `op_top_manufacturers_20250830_225531.csv`
- `op_consecutive_years_20250830_225534.csv`
- No fabricated statistics or patterns