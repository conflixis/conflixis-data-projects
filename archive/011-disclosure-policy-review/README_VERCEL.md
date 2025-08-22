# Vercel Deployment Guide for COI Disclosure Review System

## Overview
This guide outlines the deployment process for the COI Disclosure Review System with Policy Decision Engine to Vercel.

## Current Status
- ✅ Policy Decision Engine implemented
- ✅ Configuration management UI created
- ✅ Vercel configuration files prepared
- ⏳ Clerk authentication integration pending
- ⏳ Static asset migration pending

## Project Structure for Vercel

```
011-disclosure-policy-review/
├── api/                    # Serverless API functions
│   └── index.py           # Main API entry point (FastAPI + Mangum)
├── public/                # Static assets (to be populated)
│   ├── index.html         # Main frontend
│   ├── configuration.html # Configuration UI
│   └── data/             # Static data files
├── app/                   # Existing application code
│   ├── api/              # API routes and services
│   │   └── services/
│   │       └── policy_engine.py  # Policy Decision Engine
│   └── frontend/         # Frontend files
├── config/               # Configuration files
│   ├── coi-operational-thresholds.yaml
│   └── coi-policies.yaml
├── vercel.json          # Vercel configuration
└── requirements.txt     # Python dependencies
```

## Deployment Steps

### 1. Prepare Static Assets
Move frontend files to public directory:
```bash
cp -r app/frontend/* public/
cp -r data/staging public/data/
```

### 2. Environment Variables
Set in Vercel Dashboard:
- `GOOGLE_APPLICATION_CREDENTIALS` - Base64 encoded service account key
- `CLERK_API_KEY` - (When implementing authentication)
- `CLERK_API_SECRET` - (When implementing authentication)

### 3. Deploy to Vercel
```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
vercel --prod
```

## API Endpoints

### Policy Engine Endpoints
- `GET /api/health` - Health check
- `GET /api/policy-engine/status` - Policy engine status
- `POST /api/v1/policies/evaluate` - Evaluate disclosure
- `GET /api/v1/configuration` - Get configuration
- `PUT /api/v1/configuration` - Update configuration

### Data Endpoints
- `GET /api/v1/disclosures` - List disclosures
- `GET /api/v1/disclosures/{id}` - Get disclosure details
- `GET /api/v1/stats` - Get statistics

## Configuration Management

The Policy Decision Engine uses two YAML configuration files:

1. **coi-operational-thresholds.yaml**: Defines risk tiers and thresholds
2. **coi-policies.yaml**: Contains policy clauses from Texas Health COI Policy

These are managed through the Configuration UI at `/configuration`.

## Next Steps

1. **Authentication Integration**
   - Uncomment Clerk dependencies in requirements.txt
   - Add authentication middleware to API routes
   - Implement user session management

2. **Data Pipeline Migration**
   - Convert bigquery_pipeline.py to Vercel Cron Job
   - Setup GCS for persistent data storage
   - Configure scheduled data refreshes

3. **Frontend Migration**
   - Move all HTML files to public/
   - Update asset paths for Vercel structure
   - Test all frontend functionality

## Testing Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run with Vercel CLI
vercel dev
```

## Monitoring

- Use Vercel Analytics for performance monitoring
- Check `/api/health` for service health
- Monitor `/api/policy-engine/status` for engine readiness

## Support

For issues or questions about the Policy Decision Engine:
- Check Jira ticket: DA-151
- Review configuration at `/configuration`
- Verify policy mappings in YAML files