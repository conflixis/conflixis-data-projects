# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a healthcare data analytics repository focused on provider intelligence and compliance analysis. The codebase integrates with multiple data sources (BigQuery, Snowflake, Firestore) and performs various data operations including name matching, risk assessment, and data transfers.

## Project Setup
This repository is managed through Jira:
- **DA Project Board**: https://conflixis.atlassian.net/jira/software/projects/DA/boards/1
- A jira epic must be created for each new project under /project
- Tickets must be created, updated, status updated
- Commit often on the branch

## Key Commands

### Setup and Installation
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# For development
pip install -r requirements-dev.txt

# Set up environment variables
cp .env.example .env  # Then edit .env with actual credentials
```

### Running Jupyter Notebooks
```bash
# Start Jupyter for risk assessment
jupyter notebook projects/001-risk-assessment-new/Risk_assessment_new.ipynb
```

### Name Matching Operations
```bash
# Run name matching with config file
cd projects/005-core-name-matching-test
python run_name_matching.py
```

## Architecture and Data Flow

### Primary Data Sources
1. **BigQuery** (`data-analytics-389803`): main Bigquery data environment
2. **Firestore** (`conflixis-web`): Real-time database for member and entity data
3. **Snowflake**: External data warehouse requiring periodic sync to BigQuery, contains definitive healthcare data that is periodically synced.

### Key Projects Structure

#### projects
Contains project folders with different initiatives

### Shared Modules (`src/`)
- `analysis/01-core-name-matching/`: Reusable name matching components
- `snowflake_bq_transfer/`: Transfer operation modules
- `visualization/`: Placeholder for visualization utilities

## Environment Variables
Use `.env` file

## Archived Projects
- Deprecated projects and documents in `archive/`

## Important Notes

- Always check for existing dependencies before adding new ones
- BigQuery operations should aggregate data server-side before downloading
- Firestore downloads use collection group queries for sharded collections
- Snowflake transfers require proper GCS bucket and storage integration setup
