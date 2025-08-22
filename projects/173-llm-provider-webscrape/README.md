# Provider Profile Web Enrichment Tool

**Project**: DA-173 - LLM Provider Web Search POC  
**Status**: Development  
**Version**: 1.0.0

## Overview

A production-grade web search tool that uses OpenAI's web search capabilities to enrich healthcare provider profiles with comprehensive, cited information from public web sources. The tool emphasizes data quality, source citation, and comprehensive logging for audit trails.

## Features

- **Comprehensive Profile Extraction**: Captures professional affiliations, education, research, industry relationships, and more
- **Clear Source Citations**: Every data point includes its web source URL for verification
- **Quality Scoring**: Confidence scores and completeness metrics for data validation
- **Flexible Input**: Single provider or bulk processing from CSV/JSON
- **Checkpoint Support**: Resume interrupted bulk operations
- **Detailed Logging**: Full audit trails of requests, responses, and metrics
- **Production Ready**: Error handling, rate limiting, and retry logic

## Quick Start

### Prerequisites

- Python 3.8+
- OpenAI API key with web search preview access
- Virtual environment (recommended)

### Installation

```bash
# Clone the repository
cd projects/173-llm-provider-webscrape

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
export OPENAI_API_KEY="your-api-key-here"
```

### Basic Usage

#### Single Provider Search

```bash
# Basic search with name only
python scrape_single_provider.py --name "Dr. Jane Smith"

# Search with additional context
python scrape_single_provider.py \
  --name "Dr. Jane Smith" \
  --institution "Mayo Clinic" \
  --specialty "Cardiology" \
  --location "Rochester, MN"

# Verbose output
python scrape_single_provider.py --name "Dr. Jane Smith" --verbose

# Dry run (no API calls)
python scrape_single_provider.py --name "Dr. Jane Smith" --dry-run
```

#### Bulk Provider Search

```bash
# Process CSV file
python scrape_bulk_providers.py --input sample_data/sample_providers.csv

# Process JSON file
python scrape_bulk_providers.py --input sample_data/sample_providers.json

# Resume from checkpoint
python scrape_bulk_providers.py --input providers.csv --resume

# Limit processing
python scrape_bulk_providers.py --input providers.csv --limit 10

# Custom rate limiting
python scrape_bulk_providers.py --input providers.csv --rate-limit 20
```

### Testing

```bash
# Run component tests and live search test
python test_websearch.py

# Test output will be saved to test_output/
```

## Project Structure

```
173-llm-provider-webscrape/
├── config/
│   ├── config.yaml          # Main configuration
│   └── prompts.yaml         # System prompts for LLM
├── src/
│   ├── __init__.py
│   ├── provider_scraper.py  # Core web search engine
│   ├── profile_parser.py    # Response parsing logic
│   ├── citation_extractor.py # Citation extraction
│   └── logger.py            # Logging utilities
├── sample_data/
│   ├── sample_providers.csv # Sample CSV input
│   └── sample_providers.json # Sample JSON input
├── data/
│   ├── output/              # Search results (gitignored)
│   ├── logs/                # Application logs (gitignored)
│   └── checkpoints/         # Checkpoint files (gitignored)
├── scrape_single_provider.py # Single provider script
├── scrape_bulk_providers.py  # Bulk processing script
├── test_websearch.py        # Test suite
├── requirements.txt         # Python dependencies
├── BUSINESS_SCOPE.md       # Business objectives
├── DATA_DICTIONARY.md      # Field specifications
└── README.md               # This file
```

## Configuration

### config/config.yaml

Key configuration options:

```yaml
openai:
  model: gpt-4o              # Model to use
  max_tokens: 3000          # Response size limit
  temperature: 0            # Deterministic responses

web_search:
  search_context_size: high  # Amount of context to retrieve
  max_sources: 20           # Maximum sources to consider

quality:
  min_confidence_threshold: 0.7  # Minimum confidence for data
  require_citations: true       # Enforce citation requirements

processing:
  rate_limit_per_minute: 30     # API rate limiting
  checkpoint_frequency: 5        # Save checkpoint every N providers
```

### config/prompts.yaml

Contains the system prompts that guide the LLM's search and extraction behavior. Modify to adjust:
- Information categories to extract
- Search strategies
- Citation requirements
- Output formatting

## Output Format

### Profile Structure

```json
{
  "profile": {
    "basic": {
      "full_name": "Dr. Jane Smith",
      "credentials": ["MD", "PhD"],
      "primary_specialty": "Cardiology",
      "npi": "1234567890"
    },
    "professional": {
      "current_positions": [...],
      "hospital_affiliations": [...],
      "practice_locations": [...]
    },
    "education": {
      "medical_school": {...},
      "residency": {...},
      "fellowship": [...]
    },
    "research": {
      "publications": [...],
      "clinical_trials": [...]
    }
  },
  "citations": [
    {
      "url": "https://example.com/profile",
      "domain": "example.com",
      "source_type": "hospital_website",
      "confidence": 0.95,
      "data_points": ["position_title", "affiliation"]
    }
  ],
  "metadata": {
    "request_id": "uuid",
    "scraped_at": "2024-01-15T10:30:00",
    "overall_confidence": 0.85,
    "field_completeness": 0.75,
    "processing_time_seconds": 35.2
  }
}
```

## Input Formats

### CSV Format

```csv
name,institution,specialty,npi,location
"Dr. Jane Smith","Mayo Clinic","Cardiology","1234567890","Rochester, MN"
"Dr. John Doe","Johns Hopkins","Oncology",,"Baltimore, MD"
```

### JSON Format

```json
{
  "providers": [
    {
      "name": "Dr. Jane Smith",
      "institution": "Mayo Clinic",
      "specialty": "Cardiology",
      "npi": "1234567890",
      "location": "Rochester, MN"
    }
  ]
}
```

## Quality Metrics

### Confidence Scoring

- **1.0**: Government databases, official hospital sites
- **0.9**: University websites, research databases
- **0.8**: Professional directories (Healthgrades, Doximity)
- **0.6**: News articles
- **< 0.6**: Other sources

### Completeness Scoring

Weighted scoring based on field importance:
- Required fields (name, specialty): 90-100% weight
- Important fields (education, certifications): 70-80% weight
- Nice-to-have fields (awards, speaking): 40-50% weight

### Manual Review Triggers

Profiles are flagged for manual review when:
- Overall confidence < 70%
- Field completeness < 60%
- High-value positions detected (Chief, Director, President)
- Conflicting information found

## Logging and Monitoring

### Log Files

- **Application logs**: `data/logs/[logger_name].log`
- **Structured logs**: `data/logs/[logger_name]_structured.jsonl`
- **Audit logs**: `data/logs/audit_[date].jsonl`
- **Metrics**: `data/output/metrics.json`

### Monitoring Metrics

- Total requests and success rate
- Average processing time
- Citation counts and confidence scores
- Source type distribution
- Error types and frequencies

## Error Handling

The tool includes comprehensive error handling:

- **Retry logic**: Automatic retries with exponential backoff
- **Checkpoint recovery**: Resume bulk operations after failures
- **Graceful degradation**: Partial results returned on errors
- **Detailed error logging**: Full stack traces and context

## API Usage and Costs

- **Rate limiting**: Default 30 requests/minute (configurable)
- **Token usage**: Tracked in metadata (~3000 tokens per request)
- **Cost estimation**: ~$0.15-0.30 per provider at current pricing

## Security and Compliance

- **No PHI/PII storage**: Only public professional information
- **Source verification**: All data linked to public sources
- **Audit trails**: Complete logging of all operations
- **Configurable exclusions**: Block certain domains

## Troubleshooting

### Common Issues

1. **"OpenAI API key not found"**
   - Set environment variable: `export OPENAI_API_KEY="your-key"`
   - Or add to `.env` file in project root

2. **"Model does not support web_search_preview"**
   - Ensure you have access to OpenAI's web search features
   - Check model compatibility in config.yaml

3. **Rate limit errors**
   - Reduce `rate_limit_per_minute` in config
   - Use checkpoint recovery for bulk operations

4. **Low confidence/completeness scores**
   - Provide more context (institution, specialty)
   - Adjust thresholds in config.yaml
   - Review excluded/trusted domains

## Development

### Running Tests

```bash
# Component tests only
python test_websearch.py

# With live API test
export OPENAI_API_KEY="your-key"
python test_websearch.py
```

### Adding New Fields

1. Update `DATA_DICTIONARY.md` with field specifications
2. Modify extraction prompts in `config/prompts.yaml`
3. Update parser in `src/profile_parser.py`
4. Add field weights in `config/config.yaml`

### Customizing Source Credibility

Edit `source_credibility` weights in `config/config.yaml`:

```yaml
source_credibility:
  hospital_website: 1.0      # Highest credibility
  government_database: 1.0
  professional_directory: 0.8
  news_article: 0.6          # Lower credibility
```

## Support

For issues or questions:
- Review `BUSINESS_SCOPE.md` for objectives
- Check `DATA_DICTIONARY.md` for field definitions
- See test examples in `test_websearch.py`
- Review logs in `data/logs/` for debugging

## License

Proprietary - Conflixis Healthcare Analytics

## Changelog

### Version 1.0.0 (2024-01-15)
- Initial POC implementation
- Core web search functionality
- Single and bulk processing
- Comprehensive logging and metrics
- Citation extraction and validation