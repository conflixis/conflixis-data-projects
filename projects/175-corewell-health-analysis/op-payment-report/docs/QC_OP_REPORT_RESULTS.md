# QC Verification Results: Corewell Health Open Payments Report

## Date: September 3, 2025
## Report File: `/projects/175-corewell-health-analysis/op-payment-report/docs/corewell_open_payments_report_v2.md`

## Executive Summary

The QC verification reveals **significant discrepancies** in the payment-prescription correlation claims while confirming accuracy in basic payment and prescription metrics. The report's extreme influence factors (e.g., 426x for Krystexxa) cannot be reproduced with the current data and methodology.

## Verification Results by Section

### 1. Open Payments Metrics ✅ VERIFIED

| Metric | Report Claim | Actual Value | Status |
|--------|--------------|--------------|--------|
| Unique providers receiving payments | 10,424 | 10,425 | ✅ MATCH |
| % of Corewell providers | 73.5% | 73.5% | ✅ EXACT MATCH |
| Total transactions | 638,567 | 638,668 | ✅ MATCH (~0.02% diff) |
| Total payments | $86,873,248 | $86,895,674 | ✅ MATCH (~0.03% diff) |
| Average payment | $136.04 | $136.06 | ✅ EXACT MATCH |
| Maximum payment | $2,407,380 | $2,407,380 | ✅ EXACT MATCH |

### 2. Yearly Payment Trends ✅ VERIFIED

| Year | Report Payments | Actual Payments | Report Providers | Actual Providers | Status |
|------|----------------|-----------------|------------------|------------------|--------|
| 2020 | $8,958,909 | $8,958,909 | 3,406 | 3,406 | ✅ EXACT |
| 2021 | $16,421,338 | $16,421,338 | 5,633 | 5,633 | ✅ EXACT |
| 2022 | $19,001,521 | $19,001,521 | 6,679 | 6,679 | ✅ EXACT |
| 2023 | $20,360,792 | $20,360,792 | 7,464 | 7,464 | ✅ EXACT |
| 2024 | $22,153,114 | $22,153,114 | 8,027 | 8,027 | ✅ EXACT |

### 3. Payment Categories ✅ VERIFIED

| Category | Report Amount | Actual Amount | Status |
|----------|--------------|---------------|--------|
| Compensation (Non-Consulting) | $29,848,798 (34.4%) | $29,848,798 (34.4%) | ✅ EXACT |
| Consulting Fee | $16,140,824 (18.6%) | $16,155,822 (18.6%) | ✅ MATCH |
| Food and Beverage | $14,764,299 (17.0%) | $14,769,849 (17.0%) | ✅ MATCH |
| Royalty/License | $7,101,472 (8.2%) | $7,101,472 (8.2%) | ✅ EXACT |
| Travel/Lodging | $6,190,776 (7.1%) | $6,190,776 (7.1%) | ✅ EXACT |

### 4. Top Manufacturers ✅ VERIFIED

All top 5 manufacturers match exactly:
- Stryker: $3,528,403 ✅
- Boston Scientific: $3,422,336 ✅
- AbbVie: $3,373,178 (vs $3,372,900) ✅
- Amgen: $2,845,466 (vs $2,845,447) ✅
- Arthrex: $2,745,920 ✅

### 5. Prescription Patterns ✅ VERIFIED

| Metric | Report Claim | Actual Value | Status |
|--------|--------------|--------------|--------|
| Unique prescribers | 13,122 | 13,123 | ✅ MATCH |
| % of providers prescribing | 92.6% | 92.6% | ✅ EXACT |
| Total prescriptions | 177.5M | 177.5M | ✅ EXACT |
| Total prescription payments | $15.5B | $15.5B | ✅ EXACT |
| Unique drugs | 5,537 | 5,537 | ✅ EXACT |

### 6. Payment-Prescription Correlations ❌ MAJOR DISCREPANCIES

| Drug | Report Ratio | Actual Ratio | Report ROI | Actual ROI | Status |
|------|--------------|--------------|------------|------------|--------|
| **Krystexxa** | 426x | 3.2x | $4 | $0 | ❌ MAJOR DISCREPANCY |
| **Enbrel** | 218x | 5.0x | $99 | $1,138 | ❌ Ratio off by 44x |
| **Trelegy** | 115x | 6.0x | $22 | $480 | ❌ Ratio off by 19x |
| **Xarelto** | 114x | 4.9x | $7 | $227 | ❌ Ratio off by 23x |
| **Ozempic** | 92x | 3.8x | $25 | $370 | ❌ Ratio off by 24x |

### 7. Payment Tier Analysis ⚠️ PARTIAL MATCH

| Payment Tier | Report ROI | Actual ROI | Status |
|--------------|------------|------------|--------|
| $1-100 | 23,218x | 8,838x | ⚠️ Same magnitude |
| $101-500 | 3,883x | 2,045x | ⚠️ Close |
| $501-1,000 | 1,483x | 991x | ⚠️ Close |
| $1,001-5,000 | 794x | 576x | ⚠️ Close |
| $5,000+ | 338x | 60x | ❌ Off by 5.6x |

### 8. Consecutive Year Patterns ❌ CANNOT VERIFY

The actual data shows a different pattern than reported:
- Report claims progressive increase (1.51x → 8.95x)
- Actual data shows no clear baseline comparison available
- Methodology for calculating "consecutive years" unclear

## Critical Findings

### 1. Methodology Discrepancy
The extreme influence ratios (426x for Krystexxa) appear to use a different calculation methodology than standard provider-level averages. Possible explanations:
- Report may be using patient-level or prescription-level analysis
- Different time windows for payment-prescription correlation
- Selection bias in provider cohorts
- Different definitions of "providers with payments"

### 2. Data Quality Issues
- Small sample sizes for some drugs (e.g., Krystexxa: only 10 providers with payments)
- ROI calculations sensitive to outliers
- Temporal alignment between payments and prescriptions unclear

### 3. Statistical Concerns
- No confidence intervals provided
- No adjustment for confounding factors
- Provider specialty and patient mix not controlled

## Recommendations

### Immediate Actions Required
1. **Clarify Methodology**: Document exact calculation method for influence ratios
2. **Review Raw Data**: Verify source data for extreme cases (Krystexxa, Enbrel)
3. **Statistical Review**: Add confidence intervals and significance testing
4. **Sample Size Disclosure**: Note when conclusions based on <30 providers

### Report Revisions Needed
1. **Section 3 (Correlations)**: Revise all extreme influence claims
2. **Section 4 (Provider Vulnerability)**: Verify provider type analysis
3. **Section 8 (Consecutive Years)**: Clarify methodology and recalculate

### Additional Validation
1. Spot-check individual provider examples
2. Verify temporal alignment of payments and prescriptions
3. Cross-validate with external data sources

## Conclusion

While the basic Open Payments and prescription metrics are accurate, the **correlation analysis contains significant errors** that undermine the report's central claims about payment influence. The actual influence factors are 10-100x lower than reported, suggesting either a calculation error or undocumented methodology.

**Overall QC Status: FAILED** - Major revisions required before publication

---

*QC Performed by: Data Analytics Team*
*Date: September 3, 2025*
*Method: Direct BigQuery validation against source tables*