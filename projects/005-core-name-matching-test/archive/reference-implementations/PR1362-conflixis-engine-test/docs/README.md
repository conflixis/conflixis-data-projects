# Company Matching API

A hybrid company matching API that combines Elasticsearch search with AI-powered enhancement for intelligent company name matching.

## Features

- **Fast Algorithmic Matching**: Uses Elasticsearch for initial fuzzy matching
- **AI Enhancement**: Selectively applies OpenAI function calling for uncertain matches
- **Abbreviation Detection**: Recognizes common company abbreviations (IBM, GE, etc.)
- **Context-Aware Matching**: Uses industry and regional context to improve accuracy
- **Batch Processing**: Support for matching multiple companies in a single request
- **Performance Optimized**: Sub-200ms response for non-AI matches
- **Debug Mode**: Track AI token usage for cost monitoring and optimization

## API Endpoints

### Single Company Match

```
POST /match
```

**Request:**

```json
{
  "companyName": "IBM",
  "context": {
    "industry": "Technology",
    "region": "US",
    "size": "large"
  },
  "debug": true // Optional: Returns AI token usage metrics
}
```

**Response (High Confidence):**

```json
{
  "success": true,
  "requestId": "req_1234567890_abc123",
  "status": "match",
  "match": {
    "id": "company_123",
    "name": "International Business Machines Corporation",
    "aliases": ["IBM", "IBM Corp"],
    "domain": "ibm.com",
    "industry": "Technology",
    "confidence": 0.95,
    "matchType": "abbreviation",
    "reasoning": "Recognized as common abbreviation"
  }
}
```

**Response (Multiple Potential Matches):**

```json
{
  "success": true,
  "requestId": "req_1234567890_abc123",
  "status": "potential_matches",
  "potentialMatches": [
    {
      "id": "company_456",
      "name": "Apple Inc.",
      "confidence": 0.82,
      "matchType": "variation"
    },
    {
      "id": "company_789",
      "name": "Apple Bank",
      "confidence": 0.75,
      "matchType": "fuzzy"
    }
  ]
}
```

### Batch Company Match

```
POST /batch
```

**Request:**

```json
{
  "companies": [
    { "companyName": "Microsoft" },
    { "companyName": "GOOG", "context": { "industry": "Technology" } }
  ]
}
```

### Health Check

```
GET /health
```

## Setup Instructions

### Prerequisites

1. Node.js 20 or higher
2. Elastic Cloud account with deployment
3. OpenAI API key
4. Google Cloud SDK (for deployment)

### Local Development

1. Clone the repository:

```bash
cd functions/company-matching-api
```

2. Install dependencies:

```bash
npm install
```

3. Set up environment variables:

```bash
cp .env.example .env
# Edit .env with your Elastic Cloud configuration
```

Add your Elastic Cloud credentials to `.env`:

```env
ELASTIC_CLOUD_ID=your-cloud-id
ELASTIC_API_KEY_ID=your-api-key-id
ELASTIC_API_KEY=your-api-key
ES_INDEX_NAME=companies
```

**Getting Elastic Cloud Credentials:**

1. Log in to your [Elastic Cloud console](https://cloud.elastic.co)
2. Find your deployment and copy the Cloud ID
3. Create an API key in Kibana: Stack Management → API Keys
4. Note both the API key ID and the API key value

5. Test the Elasticsearch connection:

```bash
npm run test:connection
```

6. Seed sample data:

```bash
npm run seed
```

7. Run the development server:

```bash
npm run dev
```

8. Test the API:

```bash
curl -X POST http://localhost:8080/match \
  -H "Content-Type: application/json" \
  -d '{"companyName": "Apple"}'
```

### Running Tests

```bash
# Run all tests
npm test

# Run tests with coverage
npm run test:coverage

# Run tests in watch mode
npm run test:watch
```

### Deployment to Google Cloud Functions

1. Configure Google Cloud:

```bash
gcloud config set project YOUR_PROJECT_ID
```

2. Set up secrets in Google Secret Manager:

```bash
gcloud secrets create openai-api-key --data-file=- <<< "your-api-key"
gcloud secrets create elasticsearch-password --data-file=- <<< "your-password"
```

3. Deploy the function:

```bash
npm run deploy
```

4. Or deploy with custom settings:

```bash
gcloud functions deploy company-matching-api \
  --runtime nodejs20 \
  --trigger-http \
  --allow-unauthenticated \
  --entry-point=matchCompanies \
  --source=. \
  --memory=512MB \
  --timeout=60s \
  --set-env-vars="NODE_ENV=production,ES_INDEX_NAME=companies" \
  --set-secrets="OPENAI_API_KEY=openai-api-key:latest,ES_PASSWORD=elasticsearch-password:latest"
```

## Configuration

### Environment Variables

#### Elastic Cloud Configuration (Required)

| Variable             | Description                      | Default     |
| -------------------- | -------------------------------- | ----------- |
| `ELASTIC_CLOUD_ID`   | Your Elastic Cloud deployment ID | Required    |
| `ELASTIC_API_KEY_ID` | Your API key ID                  | Required    |
| `ELASTIC_API_KEY`    | Your API key                     | Required    |
| `ES_INDEX_NAME`      | Index name for companies         | `companies` |

#### Other Configuration

| Variable         | Description                | Default               |
| ---------------- | -------------------------- | --------------------- |
| `OPENAI_API_KEY` | OpenAI API key             | Required              |
| `OPENAI_MODEL`   | OpenAI model to use        | `gpt-4-turbo-preview` |
| `API_KEY`        | API key for authentication | Optional              |
| `LOG_LEVEL`      | Logging level              | `info`                |

### Matching Thresholds

The matching algorithm uses configurable confidence thresholds:

- **High Confidence**: ≥ 0.95 (immediate match, no AI)
- **AI Enhancement Range**: 0.70 - 0.95
- **Low Confidence**: < 0.30 (no match returned)
- **Ambiguity Threshold**: 0.15 (difference between top matches)

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Client    │────▶│     API      │────▶│Elasticsearch│
└─────────────┘     └──────┬───────┘     └─────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │  AI Service  │
                    │   (OpenAI)   │
                    └──────────────┘
```

### Matching Pipeline

1. **Normalize Input**: Clean and standardize company name
2. **Elasticsearch Search**: Multiple query strategies (exact, fuzzy, wildcard)
3. **Initial Scoring**: Calculate confidence based on ES score and string similarity
4. **AI Decision**: Determine if AI enhancement is needed
5. **AI Enhancement**: Apply function calling tools if applicable
6. **Final Scoring**: Combine all confidence factors
7. **Response Format**: Return appropriate response based on confidence

## Performance

- **Without AI**: < 200ms average response time
- **With AI Enhancement**: < 2s average response time
- **Batch Processing**: Linear scaling with batch size
- **Concurrent Requests**: Supports 100+ concurrent requests

## Monitoring

The API logs structured JSON for easy parsing:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "info",
  "message": "API Request",
  "method": "POST",
  "path": "/match",
  "statusCode": 200,
  "duration": 145,
  "requestId": "req_1234567890_abc123"
}
```

## Error Handling

The API returns consistent error responses:

```json
{
  "success": false,
  "error": "Validation Error",
  "message": "Invalid request body",
  "details": [
    {
      "path": "companyName",
      "message": "Required field"
    }
  ],
  "requestId": "req_1234567890_abc123"
}
```

## Security

- API key authentication (optional)
- Rate limiting (100 requests/minute per IP)
- Input sanitization and validation
- Helmet.js for security headers
- CORS support

## License

Copyright (c) 2024 Conflixis. All rights reserved.
