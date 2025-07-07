# Healthcare Compliance Analysis Bot

An AI-powered web application for detecting fraud, waste, and abuse patterns in healthcare payment data using TypeScript, Next.js, Claude AI, and Google BigQuery.

## Project Structure

```
002-analytic-agent/
├── src/
│   ├── app/                     # Next.js app directory
│   │   ├── api/                 # API routes
│   │   │   ├── analyze/         # Main analysis endpoint
│   │   │   └── suggested-queries/ # Suggested queries endpoint
│   │   ├── globals.css          # Global styles
│   │   ├── layout.tsx           # Root layout
│   │   └── page.tsx             # Main chat interface
│   ├── components/              # React components (future)
│   ├── lib/                     # Core business logic
│   │   ├── bigquery-client.ts  # BigQuery integration
│   │   ├── claude-client.ts    # Claude AI integration
│   │   ├── orchestrator.ts     # Hybrid orchestration logic
│   │   └── safety-checker.ts   # Query validation & safety
│   └── types/                   # TypeScript type definitions
├── claude.md                    # AI persona and BigQuery schema
├── data dictionary/             # Source documentation
├── docs/                        # Project documentation
├── schema/                      # Database schemas
├── scripts/                     # Utility scripts
└── temp/                        # Temporary files
```

## Tech Stack

- **Frontend**: Next.js 14, React 18, TypeScript, Tailwind CSS
- **Backend**: Next.js API Routes, TypeScript
- **AI**: Anthropic Claude 3 (Sonnet)
- **Database**: Google BigQuery
- **Styling**: Tailwind CSS with custom components
- **Code Highlighting**: React Syntax Highlighter
- **Markdown**: React Markdown

## Prerequisites

- Node.js 18+ 
- npm or yarn
- Google Cloud account with BigQuery access
- Anthropic API key for Claude

## Setup Instructions

1. Install dependencies:
```bash
npm install
```

2. Set up environment variables:
```bash
cp .env.example .env
# Edit .env and add your keys:
# - ANTHROPIC_API_KEY
# - GOOGLE_CLOUD_PROJECT
```

3. Ensure Google Cloud authentication:
```bash
gcloud auth application-default login
```

## Running the Application

Development mode:
```bash
npm run dev
```

Production build:
```bash
npm run build
npm start
```

The application will be available at http://localhost:3000

## Available Scripts

```bash
npm run dev       # Start development server
npm run build     # Build for production
npm start         # Start production server
npm run lint      # Run ESLint
npm run type-check # TypeScript type checking
npm run format    # Format code with Prettier
```

## Features

- **Natural Language Queries**: Ask questions in plain English about healthcare payments
- **Compliance-Focused Analysis**: AI trained to identify fraud, waste, and abuse patterns
- **Real-time Query Validation**: Safety checks and cost estimation before execution
- **Interactive Chat Interface**: Modern UI with syntax highlighting and markdown support
- **Suggested Queries**: Pre-built compliance scenarios to get started
- **Cost Control**: Automatic query cost limits to prevent expensive operations

## Architecture

The application uses a hybrid approach:

1. **Claude AI** generates SQL queries based on natural language input
2. **Safety Layer** validates queries and estimates costs
3. **BigQuery** executes the validated queries
4. **Claude AI** interprets results with compliance expertise

## Example Queries

- "Show me physicians with suspicious speaker fee patterns in Texas"
- "Which doctors with ownership interests received the highest payments?"
- "Find payment spikes that might indicate new kickback arrangements"
- "Analyze travel payments to international destinations for compliance risks"

## Security Considerations

- All queries are validated for SQL injection attempts
- Table access is restricted to authorized datasets only
- Cost limits prevent runaway queries
- No data is stored locally - all processing is ephemeral

## Development

To add new features:

1. Types go in `src/types/`
2. API routes go in `src/app/api/`
3. Business logic goes in `src/lib/`
4. UI components go in `src/components/`

## License

MIT