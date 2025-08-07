# Healthcare Provider Disclosure Policy Review

**Jira Epic**: [DA-141](https://conflixis.atlassian.net/browse/DA-141)

## Overview

This project develops an automated system to review healthcare provider (HCP) disclosures and determine whether they comply with institutional disclosure policies. The system will analyze submitted disclosures, compare them against defined policy rules, and categorize them as within or outside policy guidelines.

## Objectives

- **Automated Compliance Checking**: Analyze HCP disclosures against institutional policies
- **Risk Identification**: Flag high-risk or non-compliant disclosures for review
- **Reporting**: Generate comprehensive compliance reports
- **Pattern Detection**: Identify trends in disclosure compliance

## Project Structure

```
011-disclosure-policy-review/
├── scripts/           # Processing and analysis scripts
├── data/              # Sample data and datasets
├── config/            # Configuration files and policy definitions
├── notebooks/         # Jupyter notebooks for analysis
├── docs/              # Documentation and specifications
├── tests/             # Unit and integration tests
└── README.md          # This file
```

## Key Components

### 1. Policy Engine
- Define and maintain disclosure policies
- Rule-based compliance checking
- Policy versioning and updates

### 2. Disclosure Parser
- Extract data from various disclosure formats
- Standardize disclosure information
- Handle multiple data sources

### 3. Compliance Analyzer
- Compare disclosures against policies
- Categorize compliance status
- Generate risk scores

### 4. Reporting System
- Compliance dashboards
- Detailed violation reports
- Trend analysis

## Data Sources

### Primary Data Sources

#### 1. Disclosure Forms Table
- **BigQuery Table**: `conflixis-engine.firestore_export.disclosure_forms_raw_latest_v3`
  - **Group ID**: `gcO9AHYlNSzFeGTRSFRa` (specific group for analysis)
  - **Total Rows for Group**: 7,609 disclosure forms
  - Contains raw disclosure form submissions from healthcare providers
  - Fields include: document_id, timestamp, group_id, campaign_id, status, start_date, end_date, data, old_data

#### 2. Parsed Disclosures Table  
- **BigQuery Table**: `conflixis-engine.firestore_export.disclosures_raw_latest_parse`
  - **Total Rows**: 13,867 total parsed disclosures
  - **Group ID Rows**: 2,413 rows for group `gcO9AHYlNSzFeGTRSFRa`
  - Contains structured/parsed disclosure data with 48 columns including:
    - External entity information (name, compensation value)
    - Service dates and relationship status
    - Research indicators and review status
    - Detailed disclosure fields parsed from form submissions

### Policy Documents
- **Texas Health Resources COI Policy** (Dec 2013): Located in `/docs/dec_2013_conflict_of_interest.pdf`
- Comprehensive three-tier management system for research-related conflicts of interest

### Additional Sources (Planned)
- Open Payments database for cross-referencing
- Historical compliance records for trend analysis

## Technical Stack

- **Languages**: Python 3.12+
- **Data Processing**: Pandas, NumPy
- **NLP**: spaCy, NLTK for policy text analysis
- **Database**: BigQuery for data storage
- **ML/AI**: Scikit-learn for pattern detection
- **Visualization**: Matplotlib, Seaborn for reports

## Getting Started

1. **Setup Environment**
```bash
cd projects/011-disclosure-policy-review
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. **Configure Policies**
- Edit `config/policies.yaml` to define disclosure policies
- Set up data source connections in `.env`

3. **Run Analysis**
```bash
python scripts/analyze_disclosures.py
```

## Development Roadmap

### Phase 1: Foundation (Weeks 1-2)
- [ ] Set up project infrastructure
- [ ] Define policy schema and rules engine
- [ ] Create disclosure data models

### Phase 2: Core Development (Weeks 3-4)
- [ ] Implement disclosure parser
- [ ] Build policy compliance engine
- [ ] Develop basic reporting

### Phase 3: Enhancement (Weeks 5-6)
- [ ] Add NLP for unstructured disclosure analysis
- [ ] Implement ML for pattern detection
- [ ] Create interactive dashboards

### Phase 4: Testing & Deployment (Week 7-8)
- [ ] Comprehensive testing
- [ ] Performance optimization
- [ ] Documentation and training materials

## Success Metrics

- **Accuracy**: >95% correct classification of disclosures
- **Processing Speed**: <5 seconds per disclosure
- **Coverage**: Support for all major disclosure types
- **User Satisfaction**: Reduced manual review time by 70%

## Contributing

Please refer to the main repository guidelines in `/CLAUDE.md` for development practices.

## Contact

For questions or issues, please create a ticket in the [DA Jira project](https://conflixis.atlassian.net/jira/software/projects/DA/boards/1).