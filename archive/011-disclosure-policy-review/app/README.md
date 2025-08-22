# COI Disclosure Review System - FastAPI Bundle

A modular FastAPI application for managing and reviewing Conflict of Interest (COI) disclosures. This bundle is designed to run locally and can be easily deployed to Vercel (frontend) and cloud functions (API).

## Architecture

```
app/
├── api/                    # API module (cloud function ready)
│   ├── models.py          # Pydantic models for type safety
│   ├── routes/            # API endpoints (modular for cloud functions)
│   ├── services/          # Business logic layer
│   └── main.py           # FastAPI app factory
├── frontend/              # Vercel-ready frontend
│   ├── index.html        # Main disclosure viewer
│   ├── dashboard.html    # COI compliance dashboard
│   └── api.js           # API client with environment detection
├── data_loader/          # Shared data access layer
│   └── parquet_loader.py # Efficient Parquet file handling
├── main.py              # Local development server
└── run_local.py        # Simple runner script
```

## Features

- **Type-Safe API**: FastAPI with Pydantic models for data validation
- **Efficient Data Handling**: Parquet file support with in-memory caching
- **Modular Architecture**: Ready for cloud function deployment
- **Frontend Integration**: HTML/JS frontend with API client
- **Auto Documentation**: Built-in ReDoc and Swagger UI
- **Environment Aware**: Automatically detects local vs production

## Quick Start

### 1. Install Dependencies

```bash
cd app/
pip install -r requirements.txt
```

### 2. Run Locally

```bash
python run_local.py
```

Or directly:

```bash
python main.py
```

### 3. Access the Application

- **Main Viewer**: http://localhost:8000
- **Dashboard**: http://localhost:8000/dashboard  
- **API Docs**: http://localhost:8000/api/docs
- **ReDoc**: http://localhost:8000/api/redoc

## API Endpoints

### Disclosures
- `GET /api/disclosures` - List disclosures with pagination and filters
- `GET /api/disclosures/{id}` - Get specific disclosure
- `GET /api/disclosures/search` - Search disclosures
- `GET /api/disclosures/stats` - Get statistics
- `POST /api/disclosures/reload` - Reload data from disk

### Policies
- `GET /api/policies` - Get policy rules
- `GET /api/policies/thresholds` - Get operational thresholds
- `GET /api/policies/configuration` - Get full configuration
- `POST /api/policies/evaluate` - Evaluate disclosure amount

### Statistics
- `GET /api/stats/overview` - Overall statistics
- `GET /api/stats/risk-distribution` - Risk analysis
- `GET /api/stats/compliance-summary` - Compliance metrics
- `GET /api/stats/health` - Health check

## Data Sources

The application loads data from (in order of preference):
1. **Parquet files**: `../data/staging/disclosures_*.parquet`
2. **JSON file**: `../data/staging/disclosure_data.json`
3. **Sample data**: Generated if no data files found

## Configuration

- **Policies**: Loaded from `../config/coi-policies.yaml`
- **Thresholds**: Loaded from `../config/coi-operational-thresholds.yaml`

## Deployment

### Local Development
```bash
python run_local.py
```

### Future Vercel Deployment (Frontend)
```bash
cd frontend/
vercel deploy
```

### Future Cloud Function (API)
Each route in `api/routes/` can be deployed as a separate cloud function:
- AWS Lambda
- Google Cloud Functions
- Azure Functions

## Environment Variables

Create a `.env` file for configuration:
```env
# API Configuration
API_URL=http://localhost:8000/api  # Override in production

# Data paths (optional)
DATA_DIR=../data/staging
CONFIG_DIR=../config
```

## Development

### Adding New Endpoints

1. Create route in `api/routes/`
2. Add service logic in `api/services/`
3. Update models in `api/models.py`
4. Import in `api/routes/__init__.py`

### Updating Frontend

1. Edit HTML files in `frontend/`
2. Update API client in `frontend/api.js`
3. Test with local server

### Data Pipeline

To update data:
```bash
cd ../scripts/
python fetch_real_data.py
```

## Testing

```bash
# Run tests
pytest

# Test API endpoints
curl http://localhost:8000/api/stats/health
```

## Performance

- **Parquet Loading**: ~100ms for 10,000 records
- **API Response**: <50ms for cached queries
- **Memory Usage**: ~100MB for 10,000 records

## Security

- CORS configured for frontend access
- No credentials stored in code
- Data files excluded from git via .gitignore

## Support

For issues or questions:
- Check API docs at `/api/docs`
- Review logs in console
- Verify data files exist in `../data/staging/`

## License

Internal use only - Conflixis Healthcare Analytics