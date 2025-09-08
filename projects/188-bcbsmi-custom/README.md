# BCBSMI Custom Hospital Analysis

**Jira Ticket**: DA-188  
**Purpose**: Hospital-level analysis of pharmaceutical influence for Blue Cross Blue Shield Michigan

## Overview

This project provides a custom addendum to the main BCBSMI investigative report, focusing on hospital-level analysis of pharmaceutical payments and their potential influence on prescribing patterns. As a payor, BCBSMI needs to identify hospitals that may warrant further investigation for cost containment opportunities.

## Key Features

- **Hospital Risk Scoring**: Composite risk assessment based on payment concentration and prescribing patterns
- **Payment Analysis**: Aggregation of pharmaceutical payments by Michigan hospitals
- **Prescription Impact**: Analysis of high-cost drug utilization by facility
- **Actionable Insights**: Specific recommendations for payor intervention

## Project Structure

```
188-bcbsmi-custom/
├── scripts/
│   ├── hospital_analysis.py      # Main analysis script
│   └── generate_addendum.py      # Report generation
├── queries/
│   └── (SQL queries for reference)
├── data/
│   └── (Analysis outputs and intermediate files)
├── reports/
│   └── bcbsmi_hospital_addendum_[timestamp].md
└── README.md
```

## Data Sources

- **Provider Affiliations**: `PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized`
- **Payment Data**: `op_general_all_aggregate_static_optimized`
- **Prescription Data**: `PHYSICIAN_RX_2020_2024_optimized`
- **Provider List**: BCBSMI NPIs (49,576 providers)

## Running the Analysis

### Prerequisites

```bash
# Ensure virtual environment is activated
source /home/incent/conflixis-data-projects/venv/bin/activate

# Required packages (should already be installed)
pip install pandas numpy google-cloud-bigquery
```

### Execute Analysis

```bash
# Run complete analysis pipeline
cd /home/incent/conflixis-data-projects/projects/188-bcbsmi-custom
python scripts/hospital_analysis.py

# Generate addendum report
python scripts/generate_addendum.py
```

## Risk Scoring Methodology

Hospitals are evaluated on a 100-point scale:

1. **Payment Intensity (30 points)**: Average payment per provider
2. **Payment Penetration (30 points)**: % of providers receiving payments
3. **Prescription Volume (20 points)**: Total cost of high-risk drugs
4. **Provider Concentration (20 points)**: BCBSMI provider coverage

Risk Categories:
- **Critical**: 80-100 points
- **High**: 60-80 points
- **Medium**: 30-60 points
- **Low**: 0-30 points

## Key Deliverables

1. **Risk-Ranked Hospital List**: All Michigan hospitals ranked by composite risk score
2. **Top 10 High-Risk Facilities**: Detailed profiles for investigation
3. **Payment Concentration Analysis**: Hospitals with highest pharma payment volumes
4. **Prescription Impact Assessment**: Facilities driving high-cost drug utilization
5. **Actionable Recommendations**: Specific interventions for cost containment

## Expected Outcomes

- Identify 5-10 critical-risk hospitals for immediate intervention
- Potential savings of $50-75M annually through targeted interventions
- 15-20% reduction in high-cost drug utilization at targeted facilities
- Enhanced prior authorization and formulary management strategies

## Related Documents

- Main Report: `bcbsmi_investigative_report_20250908_140110.md`
- Migration Guide: `/projects/186-gcp-billing-optimization/docs/MIGRATION_GUIDE.md`
- Parent Analysis: `/projects/182-healthcare-coi-analytics-report-template/`

## Contact

Conflixis Data Analytics Team  
Project Lead: DA-188