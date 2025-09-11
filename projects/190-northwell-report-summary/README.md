# Northwell Health COI Analysis Report - DA-190

## Summary
Successfully generated comprehensive conflict of interest analysis for Northwell Health using the healthcare COI template.

## Key Metrics
- **Analysis Period**: 2020-2024
- **Total Providers Analyzed**: 19,969 (from NPIs file)
- **Providers with Payments**: 11,178
- **Total Payment Value**: $114,735,791
- **Total Transactions**: 585,855
- **Prescription Value**: $11,497,794,974
- **High-Risk Providers Identified**: 326

## Key Findings

### Payment Analysis
- 73% of Northwell providers received industry payments
- Peak payment year: 2023 with $28.1M distributed
- Top payment category: Speaker/Faculty Services ($33.3M)
- Top manufacturer: Zimmer Biomet Holdings ($10.9M)

### Prescription Correlations
- OZEMPIC: 71.7x difference between providers with/without payments
- TRULICITY: 97.6x variation
- ELIQUIS: 96.5x difference
- FARXIGA: 111.6x correlation (highest)

### Provider Type Variations
- Physicians: 189.1x influence factor
- Physician Assistants: 48.6x factor
- Nurse Practitioners: 36.6x variation

### Sustained Engagement
- 5-year payment recipients: $147M avg prescription costs
- 1-year recipients: $564K avg prescription costs

## Files Generated
- `northwell_investigative_report_20250910_194600.md` - Full investigative report
- `northwell_investigative_report_20250910_194600.pdf` - PDF version

## Analysis Method
Used the healthcare COI template (project 182) with:
- BigQuery data extraction
- Open Payments database analysis
- Medicare Part D prescription correlation
- Risk scoring algorithms
- LLM-generated investigative narrative

## Technical Details
- **Runtime**: ~8 minutes
- **Queries Executed**: 23 (all successful)
- **Data Sources**: 
  - CMS Open Payments (2020-2024)
  - Medicare Part D Claims
  - Northwell provider NPIs

## Next Steps
- Review high-risk provider list for compliance monitoring
- Implement recommendations from report
- Compare findings with peer health systems
- Schedule quarterly updates

---
*Generated: September 10, 2025*
*Jira Ticket: DA-190*