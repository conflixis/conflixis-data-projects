# Conflixis Data Projects

This repository contains various data science and analytics projects for Conflixis.

## Project Management

This repository is managed through Jira: [DA Project Board](https://conflixis.atlassian.net/jira/software/projects/DA/boards/1)

## Projects

### Active Projects
- **001-risk-assessment-new**: Risk assessment analysis notebooks
- **003-datadictionary**: Data dictionary extraction tools
- **005-core-name-matching-test**: Name matching utilities
- **006-firestore-bqbackfill**: Firestore to BigQuery backfill tools
- **007-snowflake-bq-transfer**: Snowflake to BigQuery transfer utilities
- **examples**: Example scripts and notebooks

### Archived Projects
Deprecated projects have been moved to the `archive/` directory:
- **z_002-analytic-agent**: Original analytics agent (moved to [conflixis/conflixis-analytics](https://github.com/conflixis/conflixis-analytics))
- **z_004-gcp-datascience-with-multipleagents**: Multi-agent data science framework

## Setup

### Prerequisites
- Python 3.12 or higher
- pip package manager

### Installation

1. Clone the repository:
```bash
git clone https://github.com/conflixis/conflixis-data-projects.git
cd conflixis-data-projects
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. For development, also install dev dependencies:
```bash
pip install -r requirements-dev.txt
```

5. Set up environment variables:
```bash
cp .env.example .env  # Edit .env with your configuration
```

## Structure

```
conflixis-data-projects/
├── projects/         # Active projects
│   ├── 001-risk-assessment-new/
│   ├── 003-datadictionary/
│   ├── 005-core-name-matching-test/
│   ├── 006-firestore-bqbackfill/
│   ├── 007-snowflake-bq-transfer/
│   └── examples/
├── src/              # Shared Python modules
├── archive/          # Deprecated projects and temp files
├── requirements.txt  # Python dependencies
├── requirements-dev.txt  # Development dependencies
└── README.md
```

---
*Repository restructured on 2025-08-06*
