# Company Matching API Documentation

A high-performance API for intelligent company name matching, powered by Elasticsearch and enhanced with AI capabilities. Now with advanced typo detection, corporate structure resolution, historical name tracking, and smart query rewriting.

## Key Features

- **Typo Detection & Correction**: Automatically detects and corrects common typos using phonetic matching, keyboard distance algorithms, and pattern recognition
- **Corporate Structure Resolution**: Identifies subsidiaries, divisions, and parent companies to find matches even when the exact entity name isn't in the database
- **Historical Name Tracking**: Detects outdated company names and suggests current ones, handling mergers, acquisitions, and rebranding
- **Smart Query Rewriting**: Generates intelligent query variations including punctuation differences, abbreviations, word order changes, and spelling variations
- **AI-Enhanced Matching**: Uses OpenAI models for semantic understanding and complex disambiguation

## Base URL

```
Production: https://YOUR_REGION-YOUR_PROJECT.cloudfunctions.net/company-matching-api
Development: http://localhost:8080
```

## Authentication

API key authentication is optional but recommended for production use.

Include the API key in the request header:

```
X-API-Key: your-api-key-here
```

Or as a query parameter:

```
?apiKey=your-api-key-here
```

## Endpoints

### 1. Match Single Company

Match a single company name against the database.

**Endpoint:** `POST /match`

**Request Body:**

```typescript
{
  "companyName": string,          // Required: Company name to match
  "context"?: {                   // Optional: Context to improve matching
    "industry"?: string,          // Industry hint (e.g., "Technology", "Finance")
    "region"?: string,            // Geographic region (e.g., "US", "UK", "Asia")
    "size"?: string              // Company size (e.g., "small", "medium", "large")
  },
  "debug"?: boolean,              // Optional: Include AI token usage metrics
  "model"?: string,               // Optional: AI model to use (default: gpt-4-0125-preview)
                                  // Allowed models: gpt-4-0125-preview, gpt-4-turbo-preview,
                                  // gpt-4, gpt-4-32k, gpt-3.5-turbo, gpt-3.5-turbo-16k,
                                  // gpt-3.5-turbo-0125
  "skipCache"?: boolean           // Optional: Force cache miss for testing/debugging (default: false)
}
```

**Response:**

_High Confidence Match (confidence ≥ 90%):_

```json
{
  "success": true,
  "requestId": "req_1234567890_abc123",
  "status": "match",
  "match": {
    "id": "company_123",
    "name": "Apple Inc.",
    "aliases": ["Apple", "Apple Computer"],
    "domain": "apple.com",
    "industry": "Technology",
    "confidence": 0.95,
    "matchType": "exact" | "abbreviation" | "variation" | "semantic" | "fuzzy",
    "reasoning": "Exact name match found"
  },
  "debug": {                        // Only included if debug: true in request
    "aiCalls": 2,
    "esSearches": 1,                // Number of Elasticsearch queries performed
    "totalTokens": {
      "promptTokens": 450,
      "completionTokens": 120,
      "totalTokens": 570
    },
    "aiCallDetails": [
      {
        "function": "evaluateCompanyMatch",
        "tokens": {
          "promptTokens": 350,
          "completionTokens": 80,
          "totalTokens": 430
        },
        "duration": 1250
      }
    ],
    "esSearchDetails": [            // Details of each Elasticsearch query
      {
        "query": "Apple",
        "resultsFound": 5,
        "duration": 45
      }
    ]
  }
}
```

_Multiple Potential Matches (confidence 30-89%):_

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
      "confidence": 0.65,
      "matchType": "fuzzy"
    }
  ]
}
```

_No Match Found (confidence < 30%):_

```json
{
  "success": true,
  "requestId": "req_1234567890_abc123",
  "status": "no_match",
  "message": "No matches found"
}
```

### 2. Batch Match Companies

Match multiple company names in a single request.

**Endpoint:** `POST /batch`

**Request Body:**

```typescript
{
  "companies": [                   // Array of companies to match (max 100)
    {
      "companyName": string,
      "context"?: {
        "industry"?: string,
        "region"?: string,
        "size"?: string
      },
      "debug"?: boolean            // Optional: Include AI token usage metrics
    }
  ],
  "model"?: string                 // Optional: AI model to use for all companies in the batch
                                   // (default: gpt-4-0125-preview)
}
```

**Response:**

```json
{
  "success": true,
  "requestId": "req_1234567890_abc123",
  "results": [
    {
      "input": "Microsoft",
      "status": "match",
      "match": {
        "id": "company_001",
        "name": "Microsoft Corporation",
        "confidence": 0.98,
        "matchType": "variation"
      }
    },
    {
      "input": "Unknown Corp",
      "status": "no_match",
      "message": "No matches found"
    }
  ],
  "processed": 2,
  "duration": 345
}
```

### 3. Health Check

Check the health status of the API and its dependencies.

**Endpoint:** `GET /health`

**Response:**

```json
{
  "status": "healthy" | "degraded" | "unhealthy",
  "services": {
    "elasticCloud": "connected" | "disconnected",
    "openai": "configured"
  },
  "timestamp": "2024-01-15T10:30:00.000Z"
}
```

## Error Responses

### 400 Bad Request

```json
{
  "success": false,
  "error": "Validation Error",
  "message": "Invalid request body",
  "details": [
    {
      "path": "companyName",
      "message": "String must contain at least 1 character(s)"
    }
  ],
  "requestId": "req_1234567890_abc123"
}
```

### 401 Unauthorized

```json
{
  "success": false,
  "error": "Unauthorized",
  "message": "Invalid or missing API key",
  "requestId": "req_1234567890_abc123"
}
```

### 429 Too Many Requests

```json
{
  "success": false,
  "error": "Too Many Requests",
  "message": "Rate limit exceeded. Please try again later.",
  "retryAfter": 45,
  "requestId": "req_1234567890_abc123"
}
```

### 500 Internal Server Error

```json
{
  "success": false,
  "error": "Internal Server Error",
  "message": "An unexpected error occurred",
  "requestId": "req_1234567890_abc123"
}
```

### 503 Service Unavailable

```json
{
  "success": false,
  "error": "Service Unavailable",
  "message": "Elastic Cloud service is temporarily unavailable",
  "requestId": "req_1234567890_abc123"
}
```

## Match Types

- **exact**: Company names are identical or near-identical
- **abbreviation**: One name is a recognized abbreviation of the other
- **variation**: Names are variations of the same company (e.g., "Apple" vs "Apple Inc.")
- **semantic**: Names are semantically similar based on AI analysis
- **fuzzy**: Names have some similarity but lower confidence

## Enhanced Matching Capabilities

### Typo Detection

The API automatically detects and corrects:

- Keyboard typos (e.g., "Goofle" → "Google")
- Phonetic variations (e.g., "Mikrosoft" → "Microsoft")
- Common misspellings (e.g., "recieve" → "receive")
- Transposed characters (e.g., "teh" → "the")

### Corporate Structure Resolution

Handles complex corporate relationships:

- Subsidiaries (e.g., "YouTube" → "Google/Alphabet")
- Divisions (e.g., "AWS" → "Amazon")
- Regional entities (e.g., "Microsoft UK" → "Microsoft")
- Business units (e.g., "GE Healthcare" → "General Electric")

### Historical Name Detection

Recognizes outdated names and suggests current ones:

- Mergers (e.g., "Exxon" + "Mobil" → "ExxonMobil")
- Rebranding (e.g., "Facebook" → "Meta")
- Acquisitions (e.g., "WhatsApp" → "Meta/Facebook")
- Spin-offs (e.g., "PayPal" was part of "eBay")

### Query Rewriting

Automatically generates variations for:

- Punctuation differences ("AT&T" vs "AT and T")
- Abbreviations ("IBM" ↔ "International Business Machines")
- Word order ("Bank of America" ↔ "America Bank")
- Common suffixes ("Microsoft" ↔ "Microsoft Corp")

## Confidence Scores

- **0.95-1.0**: Very high confidence - likely the correct match
- **0.80-0.94**: High confidence - probably correct
- **0.60-0.79**: Medium confidence - possibly correct, review recommended
- **0.30-0.59**: Low confidence - unlikely but possible
- **< 0.30**: Very low confidence - not returned as a match

## Rate Limits

- **Default**: 100 requests per minute per IP address
- **Authenticated**: Higher limits available with API key
- **Batch endpoint**: Each company in the batch counts as one request

## Examples

### Example 1: Matching an Abbreviation

```bash
curl -X POST https://api.example.com/match \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "companyName": "IBM"
  }'
```

### Example 2: Matching with Context and Model Selection

```bash
curl -X POST https://api.example.com/match \
  -H "Content-Type: application/json" \
  -d '{
    "companyName": "Apple",
    "context": {
      "industry": "Technology",
      "region": "US"
    },
    "model": "gpt-4"
  }'
```

### Example 3: Batch Matching with Model Selection

```bash
curl -X POST https://api.example.com/batch \
  -H "Content-Type: application/json" \
  -d '{
    "companies": [
      { "companyName": "Microsoft" },
      { "companyName": "Google", "context": { "industry": "Technology" } },
      { "companyName": "Chase" }
    ],
    "model": "gpt-3.5-turbo"
  }'
```

### Example 4: Matching with Typo

```bash
curl -X POST https://api.example.com/match \
  -H "Content-Type: application/json" \
  -d '{
    "companyName": "Mircosoft",
    "debug": true
  }'
# Will automatically detect typo and match to "Microsoft"
```

### Example 5: Matching Subsidiary

```bash
curl -X POST https://api.example.com/match \
  -H "Content-Type: application/json" \
  -d '{
    "companyName": "Instagram",
    "context": {
      "industry": "Social Media"
    }
  }'
# Will identify as subsidiary and potentially match to "Meta" or "Facebook"
```

### Example 6: Matching Historical Name

```bash
curl -X POST https://api.example.com/match \
  -H "Content-Type: application/json" \
  -d '{
    "companyName": "Philip Morris",
    "debug": true
  }'
# Will detect historical name and suggest "Altria Group" (post-2003 name)
```

### Example 7: Force Cache Miss for Testing

```bash
curl -X POST https://api.example.com/match \
  -H "Content-Type: application/json" \
  -d '{
    "companyName": "Microsoft",
    "skipCache": true,
    "debug": true
  }'
# Will bypass cache lookup and perform full matching process
```

## Debug Mode

When `debug: true` is included in the request, the API returns detailed information about AI token usage:

```json
{
  "debug": {
    "aiCalls": 3, // Total number of AI calls made
    "esSearches": 2, // Total number of Elasticsearch searches (includes recursive searches)
    "totalTokens": {
      "promptTokens": 1250, // Total prompt tokens used
      "completionTokens": 320, // Total completion tokens generated
      "totalTokens": 1570 // Total tokens (prompt + completion)
    },
    "aiCallDetails": [
      // Breakdown by AI function
      {
        "function": "evaluateCompanyMatch",
        "tokens": {
          "promptTokens": 450,
          "completionTokens": 120,
          "totalTokens": 570
        },
        "duration": 1250 // Time in milliseconds
      },
      {
        "function": "checkAbbreviationMatch",
        "tokens": {
          "promptTokens": 200,
          "completionTokens": 50,
          "totalTokens": 250
        },
        "duration": 800
      }
    ],
    "esSearchDetails": [
      // Breakdown by Elasticsearch query
      {
        "query": "J&J",
        "resultsFound": 3,
        "duration": 52
      },
      {
        "query": "Johnson & Johnson", // Recursive search from AI suggestion
        "resultsFound": 1,
        "duration": 38
      }
    ]
  }
}
```

This is useful for:

- Monitoring AI costs (OpenAI charges per token)
- Tracking Elasticsearch query performance and frequency
- Identifying recursive searches triggered by AI suggestions
- Performance optimization
- Debugging matching logic
- Understanding when and why AI enhancement is triggered

## Best Practices

1. **Use Context When Available**: Providing industry, region, or size context improves matching accuracy
2. **Batch Requests**: Use the batch endpoint for multiple companies to reduce latency
3. **Handle All Response Types**: Your application should handle match, potential_matches, and no_match responses
4. **Review Medium Confidence**: Matches with 60-80% confidence may need human review
5. **Monitor Token Usage**: Use debug mode in development to understand AI costs
6. **Model Selection**: Use faster models (e.g., gpt-3.5-turbo) for high-volume, lower-accuracy needs; use GPT-4 for higher accuracy requirements
