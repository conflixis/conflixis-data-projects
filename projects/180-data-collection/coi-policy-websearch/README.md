# COI Policy WebSearch

**Jira Issue**: [DA-181](https://conflixis.atlassian.net/browse/DA-181)  
**Parent Epic**: [DA-180](https://conflixis.atlassian.net/browse/DA-180)

## Overview
Automated web search and collection of Conflict of Interest (COI) policies from healthcare organizations.

## Objectives
- Search and identify healthcare organization COI policies
- Extract policy documents and metadata
- Parse and structure policy content
- Store in standardized format for analysis

## Data Sources
- Hospital and health system websites
- Medical school policy repositories
- Professional association guidelines
- Public policy databases

## Project Structure
```
coi-policy-websearch/
├── data/
│   ├── raw/        # Raw HTML/PDF files from websites
│   ├── processed/  # Parsed and structured policy data
│   └── output/     # Final analysis results
├── scripts/        # Web scraping and processing scripts
├── docs/          # Documentation
├── src/           # Source code modules
├── tests/         # Unit tests
└── config/        # Configuration files
```

## Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Configure sources
cp config/sources.example.yaml config/sources.yaml
# Edit config/sources.yaml with target websites
```

## Usage
```bash
# Search for COI policies
python scripts/search_policies.py

# Extract content from collected files
python scripts/extract_content.py

# Process and analyze policies
python scripts/analyze_policies.py
```

## Output Format
Processed policies are stored in JSON format with the following structure:
```json
{
  "organization": "Organization Name",
  "url": "https://source-url",
  "date_collected": "2024-01-01",
  "policy_type": "COI",
  "content": {
    "title": "Policy Title",
    "sections": [],
    "key_provisions": []
  }
}
```