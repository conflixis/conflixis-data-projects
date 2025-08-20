# Company Matching API - Reference Implementation

This directory contains the company name matching implementation from PR #1362 of the conflixis-engine repository.

## Source
- **Repository**: conflixis/conflixis-engine
- **PR**: #1362 (AI company matching function endpoint)
- **Author**: jbergen
- **Status**: MERGED

## Overview

This is a production-grade company name matching system that uses a multi-tier approach:

1. **Elasticsearch Search**: Fast initial candidate retrieval
2. **Confidence Calculation**: Multiple signals combined (string similarity, context, abbreviations)
3. **AI Enhancement**: OpenAI-powered analysis for medium-confidence cases
4. **Caching**: Exact match caching for performance

## Key Components

### Services
- `matchingService.ts` - Core orchestrator for the matching workflow
- `aiEnhancementService.ts` - AI-powered enhancement for ambiguous matches
- `elasticsearchService.ts` - Database search functionality
- `exactMatchCacheService.ts` - Caching layer for performance

### AI Tools
- `aiToolDefinitions.ts` - OpenAI tool configurations
- `corporateStructure.ts` - Parent/subsidiary detection
- `historicalNames.ts` - Historical name matching
- `queryRewriting.ts` - Smart query optimization
- `typoDetection.ts` - Typo detection and correction

### Utilities
- `confidence.ts` - Confidence score calculation
- `normalization.ts` - Name normalization logic
- `validation.ts` - Input validation
- `debugTracker.ts` - Performance and cost tracking

## Key Features

1. **Multi-Model Support**: Allows choosing between gpt-4o and gpt-4o-mini
2. **Confidence Thresholds**:
   - ≥90%: Single high-confidence match
   - 30-89%: Multiple potential matches
   - <30%: No match
3. **Fast Path**: Returns immediately for very high confidence (>95%)
4. **Context-Aware**: Uses industry, region, and size hints
5. **Batch Processing**: Supports batch matching with model selection

## Comparison with Our Implementation

### Similarities
- Multi-tier approach (search → confidence → AI)
- Model selection capability
- Confidence-based thresholds
- Batch processing support

### Differences
- They use Elasticsearch; we use fuzzy matching
- They have corporate structure detection; we focus on healthcare entities
- They have caching layer; we could add this
- They use Vercel AI SDK; we use OpenAI directly

## Lessons for Our Healthcare Entity Matching

1. **Caching**: We should implement exact match caching
2. **Debug Tracking**: Their debug tracker is excellent for monitoring costs
3. **Tool Definitions**: Well-structured AI tools for specific tasks
4. **Confidence Calculation**: More sophisticated multi-signal approach
5. **Query Rewriting**: Smart query optimization before search
6. **Historical Names**: Important for healthcare org mergers/acquisitions

## Files Included

- 29 TypeScript files covering the complete implementation
- API documentation (API.md)
- Setup and usage guide (README.md)
- Comprehensive test suite

## Note

This is reference code from a production system. To use it:
1. Review the architecture and patterns
2. Adapt relevant components for healthcare context
3. Consider their caching and optimization strategies
4. Learn from their test patterns