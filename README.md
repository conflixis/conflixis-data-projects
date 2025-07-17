# Conflixis Data Projects

This repository contains various data science and analytics projects for Conflixis.

## Projects

### Active Projects
- **001-risk-assessment-new**: Risk assessment analysis notebooks
- **003-datadictionary**: Data dictionary extraction tools
- **005-core-name-matching-test**: Name matching utilities
- **006-firestore-bqbackfill**: Firestore to BigQuery backfill tools
- **007-snowflake-bq-transfer**: Snowflake to BigQuery transfer utilities
- **examples**: Example scripts and notebooks

### Deprecated Projects
- **z_002-analytic-agent**: Original analytics agent (deprecated)
- **z_004-gcp-datascience-with-multipleagents**: Multi-agent data science (deprecated)

## Setup

This repository uses Poetry for Python dependency management:

```bash
poetry install
poetry shell
```

## Structure

```
conflixis-data-projects/
├── 001-risk-assessment-new/
├── 003-datadictionary/
├── 005-core-name-matching-test/
├── 006-firestore-bqbackfill/
├── 007-snowflake-bq-transfer/
├── examples/
├── src/              # Shared Python modules
├── poetry.lock       # Python dependencies
└── pyproject.toml    # Poetry configuration
```

## Note

The Analytics Agent project has been moved to its own repository at [conflixis/conflixis-analytics](https://github.com/conflixis/conflixis-analytics).

---
*Repository restructured on 2025-07-17 as part of Phase 14*
