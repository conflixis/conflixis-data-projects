# Conflixis Data Projects

This repository contains various data science and analytics projects for Conflixis, focusing on healthcare data analytics, provider intelligence, and compliance analysis.

## Project Management

This repository is managed through Jira: [DA Project Board](https://conflixis.atlassian.net/jira/software/projects/DA/boards/1)

## Projects

### Active Projects

#### Data Processing & ETL
- **001-risk-assessment-new**: Healthcare provider risk assessment analysis using Jupyter notebooks
- **003-datadictionary**: Automated extraction and parsing of Open Payments data dictionaries from PDF sources
- **006-firestore-bqbackfill**: Tools for backing up Firestore collections to BigQuery with sharding support
- **007-snowflake-bq-transfer**: Automated pipeline for transferring Snowflake tables to BigQuery via GCS staging

#### Entity Resolution & Matching
- **005-core-name-matching-test**: Healthcare entity name matching and resolution utilities using fuzzy matching algorithms

#### Web Scraping & Analysis
- **009-doj-scrape**: Department of Justice press release scraping and analysis pipeline for compliance monitoring
- **010-companydata-openweb-scrape**: Company data enrichment through web scraping and OpenAI parsing

#### Examples & Documentation
- **examples**: Sample code for BigQuery operations in Python, R, SQL, and Jupyter notebooks

#### Compliance & Policy Review
- **011-disclosure-policy-review**: Healthcare provider disclosure policy compliance analysis

### Archived Projects
Deprecated projects have been moved to the `archive/` directory:
- **z_002-analytic-agent**: Original analytics agent (moved to [conflixis/conflixis-analytics](https://github.com/conflixis/conflixis-analytics))
- **z_004-gcp-datascience-with-multipleagents**: Multi-agent data science framework

## Setup

### Prerequisites
- Python 3.12 or higher
- pip package manager
- Google Cloud SDK (optional, for GCP operations)

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

### Configuration

The `.env` file should contain:
- **LLM API Keys**: OpenAI, Gemini, Claude/Anthropic API keys
- **GCP Credentials**: Service account credentials as JSON string in `GCP_SERVICE_ACCOUNT_KEY`
- **Project IDs**: Google Cloud project and BigQuery dataset configurations
- **Additional Settings**: Snowflake credentials (if using), Firebase project ID, etc.

#### Using GCP Credentials

The GCP service account credentials are stored as a JSON string in the environment variable. To use them in Python:

```python
import os
import json
from google.oauth2 import service_account

# Load credentials from environment variable
service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
credentials = service_account.Credentials.from_service_account_info(service_account_info)

# Use with BigQuery client
from google.cloud import bigquery
client = bigquery.Client(credentials=credentials, project=os.getenv('GCP_PROJECT_ID'))
```

## Structure

```
conflixis-data-projects/
├── projects/              # Active projects
│   ├── 001-risk-assessment-new/     # Risk assessment notebooks
│   ├── 003-datadictionary/          # PDF data dictionary extraction
│   ├── 005-core-name-matching-test/ # Name matching utilities
│   ├── 006-firestore-bqbackfill/    # Firestore to BigQuery sync
│   ├── 007-snowflake-bq-transfer/   # Snowflake to BigQuery ETL
│   ├── 009-doj-scrape/              # DOJ compliance monitoring
│   ├── 010-companydata-openweb-scrape/ # Company data enrichment
│   └── examples/                    # Code samples
├── src/                   # Shared Python modules
│   ├── analysis/          
│   │   └── 01-core-name-matching/   # Core name matching library
│   ├── snowflake_bq_transfer/      # Transfer operation modules
│   └── visualization/               # Visualization utilities
├── archive/               # Deprecated projects
│   ├── temp-datadictionary/         # Historical data dictionary files
│   ├── z_002-analytic-agent/        # Original analytics agent
│   └── z_004-gcp-datascience-with-multipleagents/ # Multi-agent framework
├── docs/                  # Documentation
│   ├── design-system/     # Conflixis Design System (UI components, colors, fonts)
│   │   └── conflixis-design-system/ # Complete design system assets
│   ├── DEPRECATED_PROJECTS.md       # Archive documentation
│   └── security_review.md           # Security analysis
├── reference/             # Reference materials (gitignored)
├── .env.example           # Environment variables template
├── .gitignore             # Git ignore rules
├── CLAUDE.md              # AI assistant guidelines
├── requirements.txt       # Python dependencies
├── requirements-dev.txt   # Development dependencies
└── README.md              # This file
```

## Data Sources

- **BigQuery** (`data-analytics-389803`): Primary data warehouse for analytics and reporting
- **Firestore** (`conflixis-web`): Real-time NoSQL database for member and entity data
- **Snowflake**: Healthcare data warehouse (Definitive Healthcare) with periodic sync to BigQuery
- **Open Payments**: CMS transparency data for healthcare provider payments
- **DOJ Press Releases**: Compliance and enforcement action monitoring

## Key Technologies

- **Languages**: Python 3.12+, SQL, R
- **Cloud Platforms**: Google Cloud Platform (BigQuery, GCS, Firestore), Snowflake
- **Data Processing**: Pandas, NumPy, Jupyter
- **AI/ML**: OpenAI API, Anthropic Claude API, Google Gemini
- **Web Scraping**: BeautifulSoup, Requests
- **Entity Matching**: FuzzyWuzzy, custom matching algorithms

## Conflixis Design System

**IMPORTANT**: All UI development MUST use the Conflixis Design System located at `/docs/design-system/conflixis-design-system/`.

### Design System Resources
- **Complete Showcase**: View the full design system at `/docs/design-system/conflixis-design-system/examples/conflixis-design-showcase.html`
- **Colors**: Official brand colors (Conflixis Green #0c343a, Gold #eab96d, Blue #4c94ed, etc.)
- **Typography**: Soehne (Leicht/Kraftig) and Ivar Display fonts
- **Components**: Pre-built buttons, cards, badges, alerts with Conflixis styling
- **Animations**: Professional animations (fade-in, pulse, wave, glow effects)

### Using the Design System
For any new UI or webpage:
1. Reference design system assets using relative paths: `../../docs/design-system/conflixis-design-system/`
2. Use the standalone HTML example as a template for simple pages
3. Use the React example for more complex applications
4. Always apply Conflixis colors and fonts to maintain brand consistency

## Development Guidelines

- See `CLAUDE.md` for AI-assisted development guidelines and repository conventions
- **ALWAYS use the Conflixis Design System for ANY UI development**
- Create a Jira epic for each new project under `/projects`
- Use virtual environments for Python dependencies
- Store credentials in environment variables, never in code
- BigQuery operations should aggregate data server-side before downloading
- Firestore downloads use collection group queries for sharded collections
- Follow existing code patterns and naming conventions
- Commit often with descriptive messages on feature branches

---
*Repository restructured on 2025-08-07*
