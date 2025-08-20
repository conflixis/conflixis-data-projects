# RX-OP Enhanced: Pharmaceutical Payment Attribution Dataset (Complete)

## Overview
This dataset contains the complete pharmaceutical payment attribution analysis, quantifying the relationship between industry payments to healthcare providers and their subsequent prescribing patterns across all major manufacturers and medical specialties.

**BigQuery Location:** `data-analytics-389803.conflixis_agent.rx_op_enhanced_full`  
**Dataset Dimensions:** 298,795,689 rows × 31 columns  
**Source Files:** 255 RDS files (17 manufacturers × 15 specialties)  
**Time Period:** 2021-2022  
**Total Data Size:** 87.07 GB in BigQuery  

## Attribution Methodology

The dataset employs a counterfactual analysis approach to quantify pharmaceutical payment influence:

1. **Monthly Tracking**: For each provider (NPI), track monthly prescription dollars and Open Payments (OP) received from each manufacturer
2. **Temporal Windows**: Calculate 6-month moving averages before (lag) and after (lead) each payment event
3. **Regression Analysis**: Model the relationship between payments and future prescribing, controlling for:
   - Medical specialty
   - Time period (year/month)
   - Baseline prescribing patterns (lag values)
4. **Counterfactual Estimation**: Calculate predicted prescriptions WITH observed payments vs. WITHOUT payments (if OP = $0)
5. **Attribution Calculation**: The difference between actual and counterfactual predictions represents prescriptions attributable to payments

This methodology isolates the causal impact of payments from organic prescribing patterns, providing a rigorous measure of marketing effectiveness.

## Column Definitions

### Provider Identifiers
| Column Name | Data Type | Description |
|------------|-----------|-------------|
| `NPI` | Character | National Provider Identifier - unique 10-digit identification number for healthcare providers |
| `SPECIALTY_PRIMARY` | Character | Primary medical specialty of the provider (Internal Medicine in this dataset) |
| `HQ_STATE` | Character | State where the provider's headquarters/primary practice is located (2-letter state code) |

### Temporal Variables
| Column Name | Data Type | Description | Range |
|------------|-----------|-------------|-------|
| `year` | Integer | Calendar year of the observation | 2021-2022 |
| `month` | Integer | Calendar month of the observation | 1-12 |

### Prescription Volume Metrics (Lagged) - STANDARDIZED
Historical prescription averages looking backward from the current time period. These columns have been standardized from manufacturer-specific names (e.g., `abbvie_avg_lag3`) to generic format.

| Column Name | Data Type | Description | Units |
|------------|-----------|-------------|-------|
| `mfg_avg_lag3` | Numeric | Average manufacturer prescriptions over previous 3 months | Prescription dollars |
| `mfg_avg_lag6` | Numeric | Average manufacturer prescriptions over previous 6 months | Prescription dollars |
| `mfg_avg_lag9` | Numeric | Average manufacturer prescriptions over previous 9 months | Prescription dollars |
| `mfg_avg_lag12` | Numeric | Average manufacturer prescriptions over previous 12 months | Prescription dollars |

### Prescription Volume Metrics (Leading) - STANDARDIZED
Future prescription averages looking forward from the current time period. These columns have been standardized from manufacturer-specific names (e.g., `abbvie_avg_lead3`) to generic format.

| Column Name | Data Type | Description | Units |
|------------|-----------|-------------|-------|
| `mfg_avg_lead3` | Numeric | Average manufacturer prescriptions over next 3 months | Prescription dollars |
| `mfg_avg_lead6` | Numeric | Average manufacturer prescriptions over next 6 months | Prescription dollars |
| `mfg_avg_lead9` | Numeric | Average manufacturer prescriptions over next 9 months | Prescription dollars |
| `mfg_avg_lead12` | Numeric | Average manufacturer prescriptions over next 12 months | Prescription dollars |

### Payment Data
| Column Name | Data Type | Description | Range |
|------------|-----------|-------------|-------|
| `TotalDollarsFrom` | Numeric | Total payment amount received from pharmaceutical company | $0 - $35,100.71 |
| `op_lag6` | Numeric | Open payments (industry payments to providers) over previous 6 months | $0 - $35,100.71 |

### Classification Variables
| Column Name | Data Type | Description |
|------------|-----------|-------------|
| `manufacturer` | Character | Pharmaceutical manufacturer name from original analysis |
| `core_specialty` | Character | Core medical specialty classification |

### Transformed Metrics
| Column Name | Data Type | Description | Range |
|------------|-----------|-------------|-------|
| `log_rx_lag6` | Numeric | Natural logarithm of prescription volume over previous 6 months | 0 - 13.537 |
| `log_rx_lead6` | Numeric | Natural logarithm of prescription volume over next 6 months | 0 - 13.550 |

### Predictive Model Variables
| Column Name | Data Type | Description | Range |
|------------|-----------|-------------|-------|
| `pred_rx` | Numeric | Predicted prescription volume from model | 1.0 - 1,232,389.1 |
| `pred_rx_cf` | Numeric | Counterfactual predicted prescription volume (baseline without intervention) | 1.0 - 35,443.93 |
| `delta_rx` | Numeric | Difference between actual and counterfactual predictions (intervention effect) | 0 - 1,230,729.2 |

### Attribution Metrics
| Column Name | Data Type | Description | Range |
|------------|-----------|-------------|-------|
| `attributable_pct` | Numeric | Percentage of prescriptions attributable to payments/intervention | 0.0 - 0.9999 |
| `attributable_pct2` | Numeric | Alternative calculation of attributable percentage | 0.0 - 0.9999 |
| `attributable_dollars` | Numeric | Dollar value of prescriptions attributable to payments | $0 - $112,823.45 |
| `attributable_dollars2` | Numeric | Alternative calculation of attributable dollar value | $0 - $112,823.45 |

### Summary Metrics
| Column Name | Data Type | Description | Range |
|------------|-----------|-------------|-------|
| `totalNext6` | Numeric | Total prescription volume/value over next 6 months | 0 - 766,465.0 |

### Processing Metadata (Added During Pipeline)
| Column Name | Data Type | Description |
|------------|-----------|-------------|
| `source_file` | Character | Original RDS filename (e.g., df_spec_abbvie_Cardiology.rds) |
| `source_manufacturer` | Character | Extracted manufacturer name (abbvie, gilead, novartis, etc.) |
| `source_specialty` | Character | Extracted medical specialty from filename |
| `processed_at` | Timestamp | When this row was uploaded to BigQuery |

## Data Quality Notes

- **Zero Values:** Many prescription and payment fields contain zeros, indicating providers with no prescriptions or payments during the period
- **Log Transformations:** Log-transformed values use natural logarithm; zero values remain as 0 in transformed columns
- **Temporal Coverage:** Complete monthly coverage from 2021-2022
- **Geographic Coverage:** Includes all US states based on HQ_STATE field
- **Standardization:** All manufacturer-specific column names have been converted to generic `mfg_avg_*` format

## Usage Considerations

1. **Lagged vs Leading Variables:** The dataset includes both historical (lag) and future (lead) metrics, useful for predictive modeling and causal inference
2. **Attribution Analysis:** The attributable metrics suggest this data is used for analyzing the causal impact of pharmaceutical payments on prescribing behavior
3. **Counterfactual Analysis:** The presence of `pred_rx_cf` indicates counterfactual modeling to estimate what prescriptions would have been without intervention
4. **Time Series Analysis:** Monthly granularity allows for detailed temporal analysis of prescribing patterns

## Aggregate Results (2-Year Analysis)

The complete dataset reveals the following aggregate patterns across all manufacturers and specialties:

### Overall Impact
- **Total Prescriptions:** $220.3 billion prescribed across all manufacturers
- **Attributable Prescriptions:** $1.16 billion attributable to payments
- **Total Payments (OP):** $654 million in industry payments
- **Overall ROI:** 1.77x (every $1 in payments generates $1.77 in attributable prescriptions)
- **Attribution Rate:** 0.52% of total prescriptions are payment-influenced

### Top Manufacturer-Specialty Combinations by Attribution
| Manufacturer | Specialty | Attributable $ | OP Spent | Attribution % | ROI |
|-------------|-----------|---------------|----------|--------------|-----|
| Gilead | Internal Medicine | $74.4M | $3.4M | 2.92% | 22.1x |
| Novo Nordisk | NP | $73.0M | $6.9M | 0.95% | 10.6x |
| Gilead | NP | $70.4M | $2.4M | 1.95% | 29.6x |
| AbbVie | PA | $57.2M | $9.9M | 2.71% | 5.8x |
| Janssen Biotech | Gastroenterology | $57.0M | $1.3M | 1.95% | 43.5x |

### Strategic Insights
- **High-Value Segments:** NPs (Nurse Practitioners) and PAs (Physician Assistants) consistently show high attribution rates and ROI
- **Efficiency Varies:** ROI ranges from 0.1x to 46.5x depending on manufacturer-specialty combination
- **Volume vs. Value:** Some specialties (Primary Care, Internal Medicine) offer high volume, while others (NP, PA) offer high attribution rates

## Key Business Questions This Data Can Answer

- What is the relationship between pharmaceutical payments and prescribing patterns?
- How do prescribing behaviors change over time following payments?
- What portion of prescriptions can be attributed to industry relationships?
- How do prescribing patterns vary by geographic location and specialty?
- What is the monetary impact and ROI of pharmaceutical marketing efforts?
- Which provider segments offer the highest return on marketing investment?

## Real Data Insights - 4 Examples

### Example 1: The Payment-Prescription Multiplier Effect
**Finding:** Providers receiving payments prescribe 6.5x more AbbVie medications than those who don't.

- **Paid providers** (128,360 observations): Average $57.22 in payments → Average 2,923 prescriptions over next 6 months
- **Unpaid providers** (2,021,334 observations): $0 in payments → Average 447 prescriptions over next 6 months
- **Key insight:** Only 6% of provider observations involve payments, yet these account for a disproportionate share of prescriptions. The average attribution rate of 1.14% seems low, but when applied to the high prescription volumes of paid providers, it translates to significant revenue impact.

### Example 2: Payment Tiers Show Exponential Returns
**Finding:** Higher payment amounts yield exponentially better attribution rates, with a clear threshold effect at $1,000.

- **$1-100 payments:** 0.7% attribution rate → $24 attributable revenue (0.24x ROI)
- **$101-500 payments:** 3.5% attribution rate → $205 attributable revenue (0.6x ROI)  
- **$501-1,000 payments:** 18.7% attribution rate → $1,795 attributable revenue (2.4x ROI)
- **$1,001-5,000 payments:** 42.9% attribution rate → $5,292 attributable revenue (2.1x ROI)
- **$5,000+ payments:** 86.3% attribution rate → $6,912 attributable revenue (0.7x ROI)

**Key insight:** The sweet spot appears to be $501-5,000 payments, where attribution rates jump dramatically. Payments below $500 show minimal impact, while payments above $5,000 show diminishing returns despite high attribution rates.

### Example 3: Geographic Concentration and Organic Prescribers
**Finding:** California generates the highest total attributable revenue ($1.69M) despite having the lowest average payment per provider ($2.75), while 14,574 providers prescribe >10,000 AbbVie products annually without receiving any payments.

**Top 5 states by total impact:**
1. California: $1.69M total impact, $2.75 avg payment
2. New York: $1.25M total impact, $5.63 avg payment  
3. Illinois: $935K total impact, $4.43 avg payment
4. Texas: $764K total impact, $2.86 avg payment
5. Kentucky: $611K total impact, $4.23 avg payment

**Organic high prescribers:** 14,574 provider observations show >10,000 prescriptions (avg: 32,536) with zero payments, suggesting strong brand loyalty or clinical preference independent of marketing efforts.

**Key insight:** California and Texas achieve high total impact through volume (many providers, small payments), while states like Kentucky achieve impact through higher per-provider investments. The existence of high-volume organic prescribers suggests that factors beyond payments (formulary placement, clinical guidelines, patient demographics) play a significant role in prescribing patterns.

### Example 4: Counterfactual Analysis - The True Intervention Effect
**Finding:** Payment interventions increase prescriptions by 14.1% on average, but high-value interventions can boost prescriptions by nearly 600%.

**Overall intervention effect (128,360 payment observations):**
- **Predicted prescriptions WITH payment:** 329 prescriptions
- **Predicted prescriptions WITHOUT payment (counterfactual):** 288 prescriptions  
- **Average lift:** 41 prescriptions (14.1% increase)

**High-impact interventions (259 cases with >1,000 prescription lift):**
- **Average payment:** $4,630
- **Predicted WITH intervention:** 21,417 prescriptions
- **Predicted WITHOUT intervention:** 3,133 prescriptions
- **Average lift:** 18,285 prescriptions (583.6% increase)

**ROI Distribution:**
- Median ROI: 0.1x (most payments don't fully pay for themselves)
- Mean ROI: 0.9x (approaching break-even on average)
- 1.7% of payments achieve >10x ROI (1,911 cases)

**Key insight:** The counterfactual analysis reveals a bimodal distribution of payment effectiveness. Most payments (98.3%) generate modest returns below the payment amount, serving more as relationship maintenance. However, a small subset (1.7%) of strategically placed payments generate extraordinary returns, suggesting that identifying and targeting high-potential providers is crucial for ROI optimization. The 259 high-impact cases show that when payments connect with the right providers at the right time, they can transform modest prescribers into major revenue generators.