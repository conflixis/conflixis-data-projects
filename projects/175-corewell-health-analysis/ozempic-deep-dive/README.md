# Ozempic Deep-Dive Analysis - Corewell Health

## Overview
This folder contains a comprehensive analysis of Ozempic prescribing patterns and pharmaceutical influence at Corewell Health (2020-2024), examining the relationship between Novo Nordisk payments and provider prescribing behavior during a period of critical drug shortages.

## Main Report
- **`ozempic_influence_analysis.md`** - Complete analysis report with findings, data visualizations, and implications

## Key Findings
- 3.2x prescribing differential between providers with/without Novo Nordisk payments
- $422.6 million in Ozempic revenue across 546,294 prescriptions
- 44.2% of Ozempic prescribers received manufacturer payments
- $39.73 ROI per dollar of Novo Nordisk investment

## Analysis Scripts

### 1. Payment Analysis (`01_novo_payments_analysis.py`)
Analyzes Novo Nordisk payments to Corewell providers
- Total payments: $1.76M to 2,066 providers
- Ozempic-specific: $549K (31.2% of total)
- Output: Payment patterns by year, specialty, product

### 2. Prescribing Analysis (`02_ozempic_prescribing_analysis.py`)
Examines Ozempic prescribing patterns and payment influence
- 3,723 total prescribers
- Payment tier analysis showing dose-response relationship
- Output: Prescribing metrics, payment influence, specialty patterns

### 3. Payor Analysis (`03_payor_analysis.py`)
Analyzes insurance coverage and reimbursement patterns
- Commercial insurance: 41.2% of patients
- Medicare: 27.8% of patients
- Output: Payor mix, reimbursement rates, access disparities

### 4. GLP-1 Market Analysis (`04_glp1_comparison.py`)
Compares Ozempic with competing GLP-1 agonists
- Market share: Ozempic 23.3%, Trulicity 33.3%
- Novo vs Lilly duopoly dynamics
- Output: Market share trends, manufacturer loyalty

### 5. Shortage Period Analysis (`05_shortage_period_analysis.py`)
*Note: Partially implemented - requires additional data fields*

## Running the Analysis

### Prerequisites
```bash
# Activate virtual environment
source ../../../venv/bin/activate

# Ensure environment variables are set
# Requires: GCP_SERVICE_ACCOUNT_KEY in .env file
```

### Execute Scripts
```bash
# Run individual analyses
python 01_novo_payments_analysis.py
python 02_ozempic_prescribing_analysis.py
python 03_payor_analysis.py
python 04_glp1_comparison.py
```

## Data Outputs
All analysis results are saved in the `data/` subfolder:

### Payment Data
- `novo_payments_detailed.csv` - All Novo Nordisk payments
- `novo_payments_yearly.csv` - Annual payment trends
- `novo_payments_by_product.csv` - Product-specific payments
- `novo_payments_by_specialty.csv` - Specialty distribution
- `novo_payments_by_type.csv` - Payment type breakdown

### Prescribing Data
- `ozempic_prescribing_detailed.csv` - Complete prescribing records
- `ozempic_yearly_trends.csv` - Annual prescribing trends
- `ozempic_payment_influence.csv` - Payment impact analysis
- `ozempic_payment_tiers.csv` - Tier-based influence analysis
- `ozempic_top_prescribers.csv` - Highest volume prescribers
- `ozempic_by_specialty.csv` - Specialty prescribing patterns

### Market Analysis Data
- `glp1_market_share.csv` - GLP-1 market distribution
- `glp1_provider_loyalty.csv` - Manufacturer loyalty patterns
- `glp1_switching_patterns.csv` - Drug switching analysis
- `ozempic_vs_competitors.csv` - Competitive positioning

### Payor Data
- `ozempic_payor_patterns.csv` - Insurance coverage patterns

## Data Sources
- **Open Payments Database**: CMS payment transparency data
- **Prescription Claims**: PHYSICIAN_RX_2020_2024 table
- **Provider Registry**: Corewell Health NPIs (14,175 providers)

## Notes
- Analysis period: 2020-2024
- Focus on Corewell Health system specifically
- Correlational analysis - causation not established
- Shortage period began March 2022