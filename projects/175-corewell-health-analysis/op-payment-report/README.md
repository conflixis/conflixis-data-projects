# Corewell Health Open Payments Report Analysis

## Overview
This analysis examines the relationship between pharmaceutical/medical device industry payments and prescribing patterns for Corewell Health's 14,175 providers, following the methodology of the Conflixis Open Payments Report 2025.

## Data Sources
- **Corewell Health NPIs**: 14,175 unique providers consolidated from 4 administrative groupings
- **Open Payments Data**: CMS Open Payments database (2020-2024)
- **Prescription Claims**: Commercial and Medicare claims data (2020-2024)

## Analysis Components

### 1. Open Payments Analysis (`01_analyze_op_payments.py`)
- Total payments to Corewell providers
- Payment categories and trends
- Top manufacturers
- High-value payments and consecutive year patterns

### 2. Prescription Pattern Analysis (`02_analyze_prescription_patterns.py`)  
- Prescribing volumes and costs
- Top prescribed drugs
- Specialty-level patterns
- NP/PA prescribing analysis

### 3. Payment-Prescription Correlation (`03_payment_influence_analysis.py`)
- Comparison of prescribing between paid vs unpaid providers
- ROI calculations per payment dollar
- Key drug analysis (Ozempic, Trelegy, Krystexxa, Farxiga, etc.)
- NP/PA vulnerability assessment

## Key Findings

### Payment Influence
- Providers receiving payments prescribe significantly more than those without payments
- Strong correlation between payment amounts and prescription volumes
- Consecutive year payments indicate sustained relationships

### Provider Type Vulnerability
- Physician Assistants (PAs) show highest vulnerability to payment influence
- Nurse Practitioners (NPs) also demonstrate elevated susceptibility
- Mid-level providers require enhanced oversight

### High-Risk Areas
- High-cost specialty drugs show strongest payment-prescription correlations
- Providers receiving >$5,000 annually demonstrate outsized prescribing patterns
- Third-party payment channels obscure transparency

## Running the Analysis

### Prerequisites
```bash
# Install dependencies
pip install pandas numpy google-cloud-bigquery python-dotenv

# Set up environment variables
export GCP_SERVICE_ACCOUNT_KEY='<your-service-account-json>'
```

### Execute Scripts
```bash
# Run Open Payments analysis
python scripts/01_analyze_op_payments.py

# Run prescription analysis
python scripts/02_analyze_prescription_patterns.py

# Run correlation analysis
python scripts/03_payment_influence_analysis.py
```

## Output Files
All analysis outputs are saved to `data/processed/` with timestamps:
- Payment metrics and trends
- Prescription patterns by drug and specialty
- Correlation analyses and ROI calculations
- High-risk provider identification

## Compliance Recommendations

1. **Enhanced Monitoring**: Implement real-time monitoring for providers receiving high-value payments
2. **NP/PA Oversight**: Develop specialized oversight programs for mid-level providers
3. **Transparency Initiatives**: Require disclosure of all third-party payment arrangements
4. **Education Programs**: Provide training on appropriate industry interactions
5. **Regular Audits**: Conduct quarterly audits of high-risk prescribing patterns

## Contact
For questions about this analysis, contact the Conflixis Data Analytics team.