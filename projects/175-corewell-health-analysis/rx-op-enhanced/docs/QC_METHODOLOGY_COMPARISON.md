# QC Methodology Comparison: Row-Weighted vs Provider-Weighted Averaging

## Date: September 3, 2025
## Analysis Context: Corewell Health Provider Type Attribution

## Executive Summary

The original QC verification used **row-weighted averaging** which counts each provider-manufacturer-month record equally. The improved methodology uses **provider-weighted averaging** which first aggregates to the provider level, then averages across providers. This difference is significant given the data structure.

## Data Structure Understanding

### Key Facts About rx_op_enhanced_full
- **Granularity**: NPI × Year × Month × Manufacturer
- **Scale for Corewell**: 
  - 7,840 unique providers
  - 2,580,957 total records (2021-2022)
  - Average ~329 records per provider
  - 17 manufacturers tracked
  - 45 specialties represented

### Why This Matters
Each provider can have up to 17 manufacturer relationships per month (17 manufacturers × 12 months × 2 years = 408 potential records). Providers with more manufacturer relationships get more weight in row-based averaging.

## Methodology Comparison

### Original Method (Row-Weighted)
```sql
AVG(CASE WHEN rx.TotalDollarsFrom > 0 THEN rx.attributable_pct END)
```
- Averages across all 2.58M records
- Each record gets equal weight
- Providers with more manufacturer relationships have more influence
- Can be biased by providers who work with many manufacturers

### Improved Method (Provider-Weighted)
```sql
-- Step 1: Aggregate to provider level
provider_avg = AVG(attributable_pct) per NPI
-- Step 2: Average across providers  
final_avg = AVG(provider_avg) across NPIs
```
- First calculates each provider's average attribution
- Then averages across providers equally
- Each provider gets one vote regardless of manufacturer count
- More representative of typical provider behavior

## Results Comparison

| Provider Type | Row-Weighted | Provider-Weighted | Difference | Interpretation |
|--------------|--------------|-------------------|------------|----------------|
| **Physician Assistant** | 5.01% | 4.82% | -0.19% | Row method slightly inflated by high-engagement PAs |
| **Nurse Practitioner** | 2.11% | 2.31% | +0.20% | Provider method shows slightly higher typical NP attribution |
| **Physician** | 1.67% | 1.54% | -0.14% | Row method slightly inflated by physicians with multiple manufacturer relationships |

### ROI Comparison
| Provider Type | Original ROI | Provider-Weighted ROI | Status |
|--------------|--------------|----------------------|--------|
| Physician Assistant | 71.42x | 71.42x | Unchanged - financial totals unaffected |
| Nurse Practitioner | 11.22x | 11.22x | Unchanged - financial totals unaffected |
| Physician | 2.44x | 2.44x | Unchanged - financial totals unaffected |

## Key Findings

### 1. Attribution Rate Differences Are Modest
- All differences are < 0.2 percentage points
- Both methods show the same ranking: PA > NP > Physician
- The report's claims remain directionally correct

### 2. Financial Metrics Unchanged
- ROI calculations are unaffected (based on totals, not averages)
- Total payment and attributable dollar amounts remain the same
- The 9.20x overall ROI for Corewell is confirmed

### 3. Statistical Validity Improved
- Provider-weighted method is more statistically sound
- Better represents "typical provider" behavior
- Reduces bias from providers with many manufacturer relationships

## Validation: High-Attribution Cases

Both methods correctly identify extreme cases:

| Provider | Attribution | Payments | Verification |
|----------|------------|----------|--------------|
| Dr. Sandra Lerner | 90.4% | $630 | ✅ Confirmed in both methods |
| Dr. Ping Wang | 73.5% | $23,381 | ✅ Confirmed in both methods |
| Dr. James Weintraub | 71.6% | $44,506 | ✅ Confirmed in both methods |

## Recommendations

### 1. For Current Report
- **No changes required** - differences are immaterial (< 0.2%)
- Consider adding footnote about methodology if publishing academically
- ROI figures (9.20x) remain valid and impressive

### 2. For Future Analyses
- Use provider-weighted averaging for attribution percentages
- Clearly document aggregation methodology
- Consider reporting both methods for transparency

### 3. Additional Validation Performed
- ✅ Data structure examined (NPI × Year × Month × Manufacturer)
- ✅ No duplicate records found
- ✅ Provider type deduplication implemented (preventing double-counting)
- ✅ Year windows properly aligned (2021-2022)
- ✅ NULL handling corrected in ROI calculations

## Conclusion

The original report's numbers are **substantially correct**. The methodological difference between row-weighted and provider-weighted averaging produces only minor variations (< 0.2 percentage points) in attribution rates. The key business findings remain valid:

1. **Corewell's 9.20x ROI is confirmed**
2. **Provider type rankings unchanged**: PA > NP > Physician  
3. **High-risk providers correctly identified**
4. **Financial calculations accurate**

The improved methodology provides better statistical rigor but does not materially change the report's conclusions.