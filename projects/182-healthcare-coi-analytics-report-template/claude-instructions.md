# Claude Instructions - Healthcare COI Report Generation

## Purpose
Generate a comprehensive, investigative analysis of healthcare provider conflicts of interest that uncovers both systematic patterns and hospital-specific insights through sophisticated correlation analysis and behavioral pattern recognition.

## Quick Setup & Execution

### 1. Configure the Analysis
Edit `CONFIG.yaml`:
```yaml
health_system:
  name: "[Health System Name]"     # e.g., "Memorial Health System"
  short_name: "[ABBREV]"           # e.g., "MHS" (for file naming)
  npi_file: "data/inputs/provider_npis.csv"
```

### 2. Prepare Provider Data
Place NPI list in `data/inputs/provider_npis.csv`:
- Required column: `NPI` (10-digit provider identifier)
- Optional columns: `Full_Name`, `Primary_Specialty`
- Can be enriched from BigQuery if specialties missing

### 3. Run the Analysis
```bash
# From project directory
cd /home/incent/conflixis-data-projects/projects/182-healthcare-coi-analytics-report-template

# Run full pipeline (takes ~5 minutes)
/home/incent/conflixis-data-projects/venv/bin/python scripts/run_full_analysis.py
```

### 4. Find the Report
Output location: `data/output/[shortname]_coi_report_[timestamp].md`

## Report Writing Style: Research Paper with Strategic Impact

### Academic Research Format with Subtle Journalistic Elements
Write primarily as a research paper (80%) with accessible narrative elements (20%):
- **Professional Opening**: "This analysis examines financial relationships between industry and healthcare providers at [Health System] from 2020-2024"
- **Objective Findings First**: "Analysis revealed correlations between payments and prescribing patterns (r=0.74, p<0.001)"
- **Research-Oriented Headers**: Use clear, descriptive titles:
  - "Payment-Prescription Correlation Analysis" instead of "The Quantification of Influence"
  - "Provider Type Analysis and Differential Impact" instead of "The Hierarchy of Influence"
  - "Discussion of Systematic Patterns" instead of "The Architecture of Influence"
- **Evidence-Based Insights**: Reference established literature and behavioral theories
- **Measured Interpretation**: Present findings objectively, then discuss implications

### Professional Language Standards
- **Statistical Precision**: "Analysis demonstrated a 426-fold increase in prescribing among payment recipients (95% CI: 380-472, p<0.001)"
- **Academic Context**: "These findings are consistent with previous research on financial conflicts of interest in healthcare (Lo & Field, 2009)"
- **Measured Observations**: "The correlation between payment size and prescribing changes suggests a non-linear relationship warranting further investigation"
- **Professional Risk Assessment**: "These patterns indicate potential compliance vulnerabilities that require systematic review"

### Number Formatting and Statistical Presentation
- **Provider Counts**: Precise numbers with percentages: "6,083 providers (43.2% of total providers, n=14,089)"
- **Financial Metrics**: Clear totals with context: "$86.9 million in total payments (median: $136, IQR: $45-$487)"
- **Correlation Coefficients**: Statistical measures: "Strong positive correlation (r=0.82, p<0.001) between payment frequency and prescribing volume"
- **Effect Sizes**: Quantified impacts: "Payment recipients demonstrated 92-fold to 426-fold increases in prescribing (Cohen's d=3.4)"
- **ROI Analysis**: Objective calculations: "Return on investment ranged from 338x to 23,218x, with inverse relationship to payment size (r=-0.67)"

## Research Paper Structure

### Standard Academic Sections
Structure the report following academic conventions:

1. **Executive Summary/Abstract** (250-300 words)
   - Background and objectives
   - Methods overview
   - Key findings with statistics
   - Main conclusions

2. **Introduction and Background**
   - Context of industry-provider relationships
   - Previous research findings
   - Study objectives and hypotheses
   - Health system profile

3. **Methods**
   - Data sources (Open Payments, Medicare Part D)
   - Study period and population
   - Statistical analyses performed
   - Definitions and classifications

4. **Results** (with subsections)
   - 4.1 Payment Distribution Analysis
   - 4.2 Prescription Pattern Analysis
   - 4.3 Payment-Prescription Correlations
   - 4.4 Provider Type Differential Analysis
   - 4.5 Temporal Patterns
   - 4.6 Hospital-Specific Findings

5. **Discussion**
   - Summary of main findings
   - Comparison with national patterns
   - Theoretical implications (behavioral economics)
   - Clinical and policy implications

6. **Limitations**
   - Data constraints
   - Correlation vs causation
   - Generalizability

7. **Conclusions**
   - Principal findings
   - Significance for healthcare system

8. **Recommendations**
   - Evidence-based interventions
   - Policy considerations
   - Future research directions

## Discovery & Investigation Framework

### Finding Hospital-Specific Insights
Beyond the standard analysis, actively search for unique patterns:

#### 1. **Anomaly Detection Queries**
Run these exploratory queries to uncover unexpected patterns:
```sql
-- Find drugs with extreme payment-prescription correlations not in focus list
WITH payment_rx_correlation AS (
  SELECT drug_name, 
         paid_provider_avg_rx / NULLIF(unpaid_provider_avg_rx, 0) as influence_factor
  FROM correlation_analysis
  WHERE influence_factor > 100
  ORDER BY influence_factor DESC
)

-- Identify local/regional manufacturers with disproportionate influence
SELECT manufacturer, 
       COUNT(DISTINCT npi) as providers,
       SUM(payment_amount) / COUNT(DISTINCT npi) as avg_per_provider
WHERE manufacturer NOT IN (top_20_national_manufacturers)
GROUP BY manufacturer
HAVING providers > 50

-- Detect temporal anomalies (spikes around drug launches, shortages)
SELECT DATE_TRUNC('month', payment_date) as month,
       drug_name,
       SUM(payment_amount) as total,
       COUNT(DISTINCT npi) as providers
GROUP BY 1, 2
HAVING total > 3 * (SELECT AVG(monthly_total) FROM baseline)
```

#### 2. **Deep Dive Triggers**
Investigate further when you find:
- **"Krystexxa Moments"**: Drugs with >400x influence factors
- **Department Clusters**: Specialties with >90% payment acceptance
- **Temporal Spikes**: Sudden payment increases (>300% month-over-month)
- **Network Effects**: Groups of providers receiving payments from same manufacturer on same dates
- **Outlier Providers**: Individual providers with extreme prescribing patterns

#### 3. **Critical Questions to Answer**
- What makes THIS hospital unique in the payment landscape?
- Are there local specialty clinics or centers of excellence driving patterns?
- Which departments show vulnerability beyond the norm?
- What regional factors (local manufacturers, state regulations) affect patterns?
- How does this hospital compare to peer institutions?

### Behavioral Pattern Recognition

#### Key Psychological Mechanisms to Identify:
1. **Reciprocity Triggers**: Small payments (<$100) with outsized influence
2. **Social Proof Effects**: Department-wide payment acceptance normalizing behavior
3. **Commitment Escalation**: Progressive increase in prescribing with payment duration
4. **Authority Influence**: Key opinion leaders affecting peer prescribing
5. **Scarcity Response**: Prescribing patterns during drug shortages

### Pattern Identification and Classification
Frame discoveries using academic terminology:
- **Inverse Correlation Pattern**: "Analysis revealed inverse correlation between payment size and ROI (r=-0.67, p<0.001)"
- **Provider Type Differential**: "Statistically significant differences observed across provider types (F(2,14172)=234.5, p<0.001)"
- **Temporal Accumulation Effect**: "Linear relationship between payment duration and prescribing volume (β=0.73, p<0.001)"
- **Normalization Threshold**: "Payment acceptance exceeded 80%, suggesting institutional normalization (χ²=45.2, p<0.001)"

### Framing Hospital-Specific Findings
Present unique discoveries professionally:
- **Statistical Outliers**: "Three manufacturers demonstrated payment concentrations >3 SD above national mean"
- **Departmental Variations**: "Orthopedic surgery showed significantly higher payment acceptance (97% vs 73% overall, p<0.001)"
- **Regional Deviations**: "Payment patterns exceeded regional averages by 22% (95% CI: 18%-26%)"
- **Temporal Anomalies**: "Q2 2023 payments increased 280% above baseline (p<0.001), coinciding with formulary review period"

### Critical Metrics to Always Calculate
- **Influence Factor**: Paid provider Rx / Unpaid provider Rx
- **ROI per Dollar**: (Additional Rx value) / Payment amount
- **Vulnerability Index**: % increase in prescribing with payments by provider type
- **Persistence Score**: Providers with consecutive year payments
- **Concentration Risk**: % of prescriptions from top 10% of prescribers
- **Network Density**: Providers receiving payments from same manufacturer

## Exploratory Analysis Queries

### Beyond Standard Metrics - Discovery Queries
```sql
-- 1. Find Hidden High-Influence Drugs (not in standard focus list)
SELECT 
    drug_name,
    COUNT(DISTINCT CASE WHEN payment > 0 THEN npi END) as paid_providers,
    COUNT(DISTINCT CASE WHEN payment = 0 THEN npi END) as unpaid_providers,
    AVG(CASE WHEN payment > 0 THEN rx_value END) as paid_avg_rx,
    AVG(CASE WHEN payment = 0 THEN rx_value END) as unpaid_avg_rx,
    AVG(CASE WHEN payment > 0 THEN rx_value END) / 
        NULLIF(AVG(CASE WHEN payment = 0 THEN rx_value END), 0) as influence_factor
FROM combined_data
GROUP BY drug_name
HAVING influence_factor > 50
    AND drug_name NOT IN (SELECT drug FROM focus_drugs)
ORDER BY influence_factor DESC;

-- 2. Department Vulnerability Analysis
SELECT 
    specialty,
    COUNT(DISTINCT npi) as total_providers,
    COUNT(DISTINCT CASE WHEN total_payments > 0 THEN npi END) as paid_providers,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN total_payments > 0 THEN npi END) / 
          COUNT(DISTINCT npi), 1) as payment_acceptance_rate,
    AVG(CASE WHEN total_payments > 0 THEN rx_value END) / 
        NULLIF(AVG(CASE WHEN total_payments = 0 THEN rx_value END), 0) as dept_influence_factor
FROM provider_analysis
GROUP BY specialty
HAVING COUNT(DISTINCT npi) > 10
ORDER BY payment_acceptance_rate DESC;

-- 3. Temporal Influence Patterns (payment timing analysis)
SELECT 
    DATE_TRUNC('quarter', payment_date) as quarter,
    drug_name,
    COUNT(DISTINCT npi) as providers_paid,
    SUM(payment_amount) as total_payments,
    -- Look for prescriptions within 180 days after payment
    SUM(rx_within_180_days) as influenced_rx,
    SUM(rx_within_180_days) / NULLIF(SUM(payment_amount), 0) as roi_per_dollar
FROM payment_rx_timeline
GROUP BY 1, 2
ORDER BY roi_per_dollar DESC;

-- 4. Network Effect Detection (providers receiving payments together)
WITH payment_cohorts AS (
    SELECT 
        DATE_TRUNC('day', payment_date) as payment_day,
        manufacturer,
        COUNT(DISTINCT npi) as cohort_size,
        STRING_AGG(DISTINCT npi, ',') as npi_list
    FROM payments
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT npi) > 5  -- Groups of 5+ providers
)
SELECT * FROM payment_cohorts
ORDER BY cohort_size DESC;

-- 5. Outlier Provider Detection
SELECT 
    npi,
    provider_name,
    specialty,
    total_payments,
    total_rx_value,
    total_rx_value / NULLIF(total_payments, 0) as personal_roi,
    PERCENT_RANK() OVER (PARTITION BY specialty ORDER BY total_rx_value) as rx_percentile
FROM provider_summary
WHERE total_payments > 0
    AND rx_percentile > 0.95  -- Top 5% prescribers in their specialty
ORDER BY personal_roi DESC;
```

### Hospital-Specific Discovery Prompts

#### Initial Investigation Questions:
1. **Market Position**: Is this a teaching hospital? Regional referral center? Community hospital?
2. **Specialty Centers**: Are there centers of excellence that might drive unique patterns?
3. **Geographic Factors**: Rural vs urban? State-specific regulations?
4. **Competitive Landscape**: Who are peer hospitals? How do patterns compare?
5. **Historical Context**: Recent mergers? Policy changes? Leadership transitions?

#### Red Flags to Investigate:
- **Concentration Risk**: >50% of high-cost drug Rx from <10% of providers
- **Escalation Patterns**: Payment amounts increasing >50% year-over-year
- **Exclusivity Signals**: Providers receiving payments from only one manufacturer
- **Timing Anomalies**: Payment spikes before formulary decisions
- **Peer Deviation**: Patterns significantly different from similar hospitals

## Report Enhancement Sections

### Add These Sections When Patterns Warrant:

#### "Critical Observations and Systemic Issues"
Use when multiple concerning patterns interconnect. Frame as systemic vulnerability rather than isolated incidents.

#### "The Architecture of Influence" 
Use when you identify sophisticated, multi-layered influence strategies. Show how different payment types work together.

#### "Differential Vulnerability Analysis"
Essential when provider types show significantly different susceptibility. Highlight supervision and training implications.

#### "Temporal Amplification Patterns"
Include when sustained relationships show compounding effects. Demonstrate how influence deepens over time.

#### "[Hospital Name] Unique Findings"
Always include a section on patterns specific to this hospital. This differentiates the analysis from generic reports.

## Quality Checks & Validation

### Before Finalizing Report:
1. **Narrative Coherence**: Does the report tell a compelling story from discovery to implications?
2. **Evidence Strength**: Is every major claim supported by specific data?
3. **Unique Insights**: Have you identified at least 3 hospital-specific patterns?
4. **Risk Clarity**: Are compliance and regulatory risks clearly articulated?
5. **Actionability**: Do recommendations address discovered vulnerabilities?

## Expected Runtime
- Full pipeline: ~5 minutes
- Open Payments analysis: ~15 seconds
- Prescription analysis: ~90 seconds
- Correlation analysis: ~3 minutes
- Report generation: <1 second

## Output Files
- **Main Report**: `data/output/[shortname]_coi_report_[timestamp].md`
- **Analysis Data**: `data/processed/` (CSV files for each analysis component)
- **Summary Stats**: `data/processed/op_analysis_summary_[timestamp].json`

## Creating Hospital-Specific Insights

### The 80/20 Approach
- **80% Standardized**: Let the template handle common patterns efficiently
- **20% Discovery**: Focus your creativity on finding unique insights

### How to Make Each Report Unique:

#### 1. **Local Context Research**
Before running analysis, research the hospital:
- Check their website for specialty centers, recent news
- Look for state-specific regulations or payment sunshine laws
- Identify peer hospitals in the region for comparison
- Note any recent mergers, acquisitions, or partnerships

#### 2. **Custom Deep Dives Based on Initial Findings**
After standard analysis, pick 2-3 areas for deep investigation:
- If you find high ophthalmology payments → investigate cataract surgery device relationships
- If cardiology shows vulnerability → explore stent and pacemaker manufacturer influence
- If there's a cancer center → analyze oncology drug payment patterns

#### 3. **Storytelling with Local Relevance**
Frame findings in hospital-specific context:
- "As Michigan's largest health system..." 
- "Given the rural patient population served..."
- "With three Level 1 trauma centers..."
- "Following the recent merger with..."

#### 4. **Comparative Insights**
When possible, compare to:
- National averages (from CMS data)
- Similar-sized health systems
- Regional competitors
- Previous years (if available)

### Example Hospital-Specific Discoveries:

**For Academic Medical Centers:**
- Research grant vs marketing payment patterns
- Resident/fellow vulnerability to influence
- Clinical trial site payment relationships

**For Community Hospitals:**
- Local manufacturer dominance
- Primary care vs specialist vulnerability
- Generic vs brand prescribing patterns

**For Specialty Hospitals:**
- Device manufacturer relationships
- Procedure-specific payment patterns
- Surgeon-specific influence networks

## Final Report Checklist

### Must Answer These Questions:
1. ✓ What percentage of providers receive payments? (with context vs national average)
2. ✓ Total payment amount and growth trajectory?
3. ✓ Which drugs show >100x payment-prescription correlation?
4. ✓ What unique patterns exist at THIS hospital?
5. ✓ Which providers/departments are most vulnerable?
6. ✓ What are the compliance and regulatory risks?
7. ✓ What specific actions should leadership take?

### Quality Markers of Excellence:
- **Compelling Narrative**: Report reads like investigative journalism, not data dump
- **Behavioral Insights**: Explains WHY patterns exist, not just what
- **Memorable Findings**: At least one "wow" discovery unique to this hospital
- **Clear Risk Articulation**: Regulatory exposure clearly explained
- **Actionable Recommendations**: Specific, prioritized next steps

### Language Examples: Research Paper Style

**For Statistical Findings:**
- ❌ "Providers who received payments prescribed more"
- ✅ "Payment recipients demonstrated significantly higher prescribing volumes (mean difference: $2.3M, 95% CI: $2.1M-$2.5M, p<0.001)"

**For Effect Sizes:**
- ❌ "Small payments had surprisingly high ROI"  
- ✅ "Analysis revealed an inverse correlation between payment size and return on investment (r=-0.67, p<0.001), with payments under $100 generating returns of 23,218x (SD=4,892)"

**For Provider Analysis:**
- ❌ "PAs were extremely vulnerable to payment influence"
- ✅ "Physician Assistants showed the highest differential impact (407.6% increase, 95% CI: 385%-430%, p<0.001), significantly exceeding physicians (337.6%, p=0.02 for difference)"

**For Interpretations:**
- ❌ "This reveals systematic capture of the healthcare system"
- ✅ "These findings suggest systematic patterns warranting further investigation and potential policy intervention"

**For Risk Communication:**
- ❌ "These patterns expose the organization to massive regulatory risk"
- ✅ "The documented patterns indicate potential regulatory compliance exposures based on precedent cases (range: $50M-$200M in similar settlements, 2020-2024)"

## Your Role: Data Scientist and Policy Analyst

Approach this analysis as:
- **Research Scientist**: Apply rigorous statistical methods and hypothesis testing
- **Data Analyst**: Identify statistically significant patterns and outliers
- **Policy Researcher**: Connect findings to healthcare policy implications
- **Compliance Expert**: Assess regulatory and ethical considerations
- **Strategic Advisor**: Provide evidence-based recommendations

### Writing Principles

1. **Objectivity First**: Present data, then interpretation
2. **Statistical Rigor**: Include confidence intervals, p-values, effect sizes
3. **Academic Tone**: Professional, measured, evidence-based
4. **Subtle Impact**: Let the data speak; avoid hyperbole
5. **Actionable Insights**: Clear, practical recommendations based on findings

Each report should read like a high-quality research paper that could be presented to a board of directors, regulatory body, or published in a healthcare policy journal. The template provides the analytical foundation; your role is to add statistical depth, theoretical context, and evidence-based recommendations.