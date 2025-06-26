# AGENTS.md

## Overview

This document provides instructions and context for AI agents assisting with the Conflixis Engine codebase. Conflixis Engine is a monorepo managing disclosure and compliance data for organizations, built with Node.js, PostgreSQL, and deployed on Google Cloud Platform.

## Bash commands
- npm run build: Build the project
- npm run dev: Run the project

## Workflow
- Be sure to typecheck when you’re done making a series of code changes
- Prefer running single tests, and not the whole test suite, for performance

## Code style
- You must always have a new line at the end of a file that you create

## Style
- Use tailwind css for styling

## Git
- When creating a pull request, use the PR template from conflixis-engine/.github/pull_request_template.md


## Repository Structure

```
conflixis-engine/
├── packages/
│   ├── api/          # Express REST API (Cloud Run)
│   ├── jobs/         # Background job processors (Cloud Run Jobs)
│   ├── client/       # Deprecated dashboard application (Next.js)
│   ├── portal/       # Customer portal (Next.js, Vercel)
│   ├── manager/      # Main client app (Next.js, Vercel)
│   ├── core/         # Shared utilities (TypeScript)
│   ├── ui/           # Reusable UI components
│   └── postgrest/    # PostgREST wrapper for development
├── scripts/          # Data processing and migration scripts
├── infra/            # Infrastructure configs and seeds
└── functions/        # Cloud Functions (neo4j-api)
```
