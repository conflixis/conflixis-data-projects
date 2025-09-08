# Remediation Plan for Corewell Open Payments Report v2

**Document Date**: September 3, 2025  
**Original Report**: `/projects/175-corewell-health-analysis/op-payment-report/docs/corewell_open_payments_report_v2.md`  
**QC Status**: ❌ **FAILED** - Major corrections required before publication

---

## Executive Summary

The Corewell Open Payments Report contains **critical errors** that fundamentally undermine its central claims about pharmaceutical payment influence. While basic payment and prescription metrics are accurate, the correlation analysis contains calculation errors of 100x+ magnitude and fails to account for confounding variables like medical specialty.

### Impact Assessment
- **High Risk**: False claims could lead to regulatory scrutiny or legal liability
- **Credibility Damage**: 131.8x calculation error for headline claim (Krystexxa)
- **Misleading Conclusions**: Correlation presented as causation without controlling for specialty

### Priority Actions
1. 🔴 **CRITICAL**: Correct all extreme correlation claims (426x → 3.2x)
2. 🔴 **CRITICAL**: Add specialty-adjusted analysis
3. 🟡 **HIGH**: Add sample size disclosures and confidence intervals
4. 🟡 **HIGH**: Revise causation language to correlation
5. 🟢 **MEDIUM**: Fix SQL queries and methodology documentation

---

## Section 1: Critical Errors Requiring Immediate Correction

### 1.1 Krystexxa Claim (Page 109-115)

**Current Report Claims:**
- Providers WITH payments: $3,524,074 average
- Providers WITHOUT payments: $8,271 average  
- Influence Factor: **426x**
- ROI: $4 per dollar

**Actual Data Shows:**
- Providers WITH payments: $26,728 average (n=10)
- Providers WITHOUT payments: $8,271 average (n=5)
- Influence Factor: **3.2x**
- ROI: **Negative** (spent $428k, generated $267k)

**Root Cause**: 131.8x multiplication error in "WITH payments" calculation

**Required Correction:**
```markdown
OLD: "Providers WITH payments: $3,524,074 average prescription value"
NEW: "Providers WITH payments: $26,728 average prescription value"

OLD: "Influence Factor: 426x increased prescribing"
NEW: "Observed Difference: 3.2x higher prescribing (not adjusted for specialty)"
```

### 1.2 Specialty Bias Not Disclosed

**Critical Finding**: The 3.2x difference is entirely explained by specialty mix
- WITH payments: 60% Rheumatologists (who naturally prescribe more Krystexxa)
- WITHOUT payments: 0% Rheumatologists (mostly Nurse Practitioners)

**Required Addition:**
```markdown
"Note: The observed difference is largely explained by medical specialty. 
All 6 rheumatologists in our sample received payments and prescribed an 
average of $33,039 in Krystexxa, while nurse practitioners (who comprised 
60% of the non-payment group) prescribed an average of $6,188."
```

### 1.3 Other Extreme Claims Requiring Verification

Based on the Krystexxa error pattern, these claims likely have similar issues:

| Drug | Report Claim | Status | Action Required |
|------|--------------|--------|-----------------|
| Enbrel | 218x | 🔴 UNVERIFIED | Re-calculate with specialty adjustment |
| Trelegy | 115x | 🔴 UNVERIFIED | Re-calculate with specialty adjustment |
| Xarelto | 114x | 🔴 UNVERIFIED | Re-calculate with specialty adjustment |
| Ozempic | 92x | 🔴 UNVERIFIED | Re-calculate with specialty adjustment |

---

## Section 2: Statistical and Methodological Issues

### 2.1 Sample Size Problems

**Issue**: Conclusions based on tiny samples without disclosure
- Krystexxa: Only 15 total prescribers (10 vs 5)
- No confidence intervals provided
- No significance testing

**Required Changes:**
- Add sample sizes in parentheses for ALL comparisons
- Remove claims where n<30 or add "preliminary finding" disclaimer
- Add confidence intervals where possible

### 2.2 Correlation Presented as Causation

**Current Language**: "Payment relationships fundamentally alter prescribing behavior"

**Required Language**: "Providers who receive payments tend to prescribe more, though this correlation may reflect specialty differences, patient populations, or reverse causation (companies targeting high prescribers)"

### 2.3 Selection Bias

**Issue**: Comparing different provider populations without controls
- No adjustment for specialty
- No adjustment for patient volume
- No adjustment for practice setting

**Required Addition**: Add section on "Limitations and Confounding Factors"

---

## Section 3: Data Quality and Technical Issues

### 3.1 SQL Query Problems

**Issues Found:**
- Case sensitivity: `LIKE '%Krystexxa%'` returns 0 (should be `LOWER()` on both sides)
- Data type mismatches: NPI stored as both STRING and INT64
- JOIN conditions placing WHERE filters incorrectly

**Fix Template:**
```sql
-- CORRECT PATTERN
WHERE LOWER(drug_name) LIKE '%krystexxa%'  -- lowercase in LIKE
ON CAST(a.npi AS STRING) = b.NPI           -- consistent casting
```

### 3.2 Calculation Methodology

**Issue**: Aggregation method not documented

**Required Documentation:**
- Mean vs Median (they tell opposite stories)
- Row-weighted vs Provider-weighted
- Treatment of outliers
- Handling of zeros/nulls

---

## Section 4: Recommended Corrections by Report Section

### Open Payments Overview (Section 1)
✅ **No changes needed** - Basic metrics verified as accurate

### Prescription Patterns (Section 2)  
✅ **No changes needed** - Prescription totals verified as accurate

### Payment-Prescription Correlations (Section 3)
🔴 **COMPLETE REWRITE NEEDED**
- Recalculate all drug correlations
- Add specialty adjustment
- Add sample sizes
- Change language from causation to correlation

### Provider Type Vulnerability (Section 4)
🟡 **NEEDS VERIFICATION**
- Re-run analysis with PHYSICIANS_OVERVIEW table
- Check for specialty confounding
- Add confidence intervals

### Payment Tier Analysis (Section 5)
🟡 **NEEDS ADJUSTMENT**
- ROI calculations need baseline correction
- Add sample sizes for each tier
- Note selection bias

### Consecutive Year Patterns (Section 6)
🟡 **NEEDS CLARIFICATION**
- Define "consecutive" methodology
- Account for provider turnover
- Add statistical significance

---

## Section 5: Implementation Checklist

### Priority 1: Critical Corrections (Due immediately)

- [ ] Fix Krystexxa numbers (426x → 3.2x)
- [ ] Add specialty bias disclosure
- [ ] Re-calculate Enbrel correlation
- [ ] Re-calculate Trelegy correlation  
- [ ] Re-calculate Xarelto correlation
- [ ] Re-calculate Ozempic correlation
- [ ] Change all causation language to correlation

### Priority 2: Statistical Improvements (Due within 1 week)

- [ ] Add sample sizes throughout
- [ ] Calculate confidence intervals where n>30
- [ ] Add "Limitations" section
- [ ] Document methodology in appendix
- [ ] Add outlier analysis

### Priority 3: Technical Fixes (Due within 2 weeks)

- [ ] Update all SQL queries for case sensitivity
- [ ] Fix NPI data type handling
- [ ] Create reproducible analysis scripts
- [ ] Implement peer review process
- [ ] Add version control for reports

---

## Section 6: Verification Scripts to Run

```bash
# 1. Re-verify Krystexxa with specialty adjustment
python scripts/krystexxa_specialty_analysis.py

# 2. Re-run other drug correlations
python scripts/qc_correlation_verification.py

# 3. Check provider type analysis
python scripts/qc_provider_type_analysis.py

# 4. Generate corrected statistics
python scripts/generate_corrected_stats.py
```

---

## Section 7: Future Prevention Measures

### QC Process Improvements

1. **Mandatory Peer Review**: Two analysts must verify extreme claims
2. **Statistical Standards**: 
   - Minimum n=30 for statistical claims
   - Confidence intervals required
   - Multiple testing correction for many comparisons
3. **Automated Checks**:
   - Flag any ratio >10x for manual review
   - Verify sample sizes meet minimums
   - Check for specialty/demographic confounding

### Documentation Requirements

1. **Methodology Section**: Full SQL queries and aggregation methods
2. **Limitations Section**: Known biases and confounders
3. **Raw Data Appendix**: Provider-level data for spot-checking

### Review Checkpoints

- [ ] Statistical review by qualified statistician
- [ ] Clinical review by medical professional
- [ ] Legal review for compliance/liability
- [ ] Executive review for business implications

---

## Conclusion and Next Steps

This report contains errors that **must be corrected** before any distribution. The headline claim of 426x influence for Krystexxa is wrong by a factor of 131.8x, and the actual 3.2x difference disappears when adjusting for medical specialty.

### Immediate Actions:
1. **STOP** any planned distribution of current report
2. **NOTIFY** stakeholders of pending corrections
3. **IMPLEMENT** Priority 1 corrections within 48 hours
4. **SCHEDULE** statistical review before re-release

### Estimated Timeline:
- Critical corrections: 2 days
- Full statistical review: 1 week
- Complete revision: 2 weeks

### Contact for Questions:
Data Analytics Team - QC Division

---

*This remediation plan is version 1.0 as of September 3, 2025*