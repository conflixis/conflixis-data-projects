import { z } from 'zod';
import { config } from '../config/environment';
import { ExactMatchCacheEntry } from './cache';

// Request schemas
export const CompanyMatchRequestSchema = z.object({
  companyName: z.string().min(1).max(500),
  context: z
    .object({
      industry: z.string().optional(),
      region: z.string().optional(),
      size: z.string().optional(),
    })
    .optional(),
  debug: z.boolean().optional(),
  model: z
    .string()
    .optional()
    .refine(val => !val || config.openai.allowedModels.includes(val), {
      message: `Model must be one of: ${config.openai.allowedModels.join(', ')}`,
    })
    .describe('Optional AI model to use (e.g., gpt-4, gpt-3.5-turbo)'),
  skipCache: z.boolean().optional().describe('Force a cache miss for testing/debugging'),
});

export const BatchMatchRequestSchema = z.object({
  companies: z.array(CompanyMatchRequestSchema).max(100),
  model: z
    .string()
    .optional()
    .refine(val => !val || config.openai.allowedModels.includes(val), {
      message: `Model must be one of: ${config.openai.allowedModels.join(', ')}`,
    })
    .describe('Optional AI model to use for all companies in the batch'),
});

// Response schemas
export const CompanyMatchSchema = z.object({
  id: z.string(),
  name: z.string(),
  confidence: z.number().min(0).max(1),
  matchType: z.enum(['exact', 'abbreviation', 'variation', 'semantic', 'fuzzy']),
  reasoning: z.string().optional(),
});

export const CompanyMatchResponseSchema = z.discriminatedUnion('status', [
  z.object({
    status: z.literal('match'),
    match: CompanyMatchSchema,
  }),
  z.object({
    status: z.literal('potential_matches'),
    potentialMatches: z.array(CompanyMatchSchema),
  }),
  z.object({
    status: z.literal('no_match'),
    message: z.string(),
  }),
]);

// Internal types
export interface ElasticsearchCompany {
  id: string;
  display_name: string;
  name?: string;
  parent_display_name?: string;
  parent_id?: string;
  submitting_name?: string | string[];
  description?: string;
  country?: string;
  state?: string;
  ai_match?: string;
  ai_matches?: string;
  aiMatch?: string;
  match_ids?: string[];
  simkey?: string;
  is_parent?: boolean;
  verified?: boolean;
  created_at?: string;
  updated_at?: string;
  external_urls?: Array<{
    description: string;
    url: string;
  }>;
  exact_match_cache?: ExactMatchCacheEntry[];
}

export interface MatchingContext {
  userProvidedContext?: z.infer<typeof CompanyMatchRequestSchema>['context'];
  elasticsearchResults?: ElasticsearchCompany[];
  algorithmicScores?: Map<string, number>;
}

export interface ConfidenceFactors {
  elasticsearchScore: number;
  stringSimilarity: number;
  aiConfidence?: number;
  contextMatch?: number;
}

// AI Tool types
export interface EvaluateCompanyMatchParams {
  candidateName: string;
  potentialMatch: {
    name: string;
  };
  context?: {
    userIndustryHint?: string;
    geographicRegion?: string;
    sizeCategory?: string;
  };
}

export interface EvaluateCompanyMatchResult {
  confidence: number;
  reasoning: string;
  matchType: 'exact' | 'abbreviation' | 'variation' | 'unlikely';
  suggestedCompanyName?: string;
}

export interface CheckAbbreviationMatchParams {
  abbreviation: string;
  fullName: string;
  industryContext?: string;
}

export interface CheckAbbreviationMatchResult {
  isValidAbbreviation: boolean;
  confidence: number;
  commonUsage: boolean;
  suggestedCompanyName?: string;
}

export interface DisambiguateMultipleMatchesParams {
  candidateName: string;
  competingMatches: Array<{
    name: string;
    confidenceScore: number;
  }>;
  userContext?: Record<string, unknown>;
}

export interface DisambiguateMultipleMatchesResult {
  bestMatchIndex: number;
  confidence: number;
  reasoning: string;
}

export interface GuessCompanyFromAbbreviationParams {
  abbreviation: string;
  context?: {
    industry?: string;
    region?: string;
  };
}

export interface GuessCompanyFromAbbreviationResult {
  possibleCompanies: Array<{
    name: string;
    confidence: number;
    reasoning: string;
  }>;
}

// Typo Detection types
export interface TypoDetectionResult {
  isLikelyTypo: boolean;
  confidence: number;
  suggestedCorrection?: string;
  detectionMethod: 'phonetic' | 'keyboard' | 'pattern' | 'none';
  details?: string;
}

// Corporate Structure types
export interface IdentifyParentCompanyParams {
  companyName: string;
  context?: {
    industry?: string;
    region?: string;
    description?: string;
  };
}

export interface IdentifyParentCompanyResult {
  hasParent: boolean;
  parentCompanyName?: string;
  confidence: number;
  relationshipType:
    | 'subsidiary'
    | 'division'
    | 'business_unit'
    | 'brand'
    | 'regional_office'
    | 'unknown';
  reasoning: string;
  searchSuggestions?: string[];
  tokenUsage?: TokenUsage;
}

export interface ResolveCorporateStructureParams {
  companyName: string;
  failedMatches?: string[];
  context?: {
    industry?: string;
    size?: string;
    description?: string;
  };
}

export interface ResolveCorporateStructureResult {
  structure: {
    type: 'standalone' | 'subsidiary' | 'parent' | 'conglomerate';
    parentCompany?: string;
    knownSubsidiaries?: string[];
    businessUnits?: string[];
  };
  confidence: number;
  reasoning: string;
  alternativeNames: string[];
  tokenUsage?: TokenUsage;
}

export interface CorporateStructureDetectionResult {
  isSubsidiary: boolean;
  parentCompanySuggestions: string[];
  confidence: number;
  indicators: string[];
  variations: string[];
}

// Historical Names types
export interface DetectHistoricalCompanyParams {
  companyName: string;
  context?: {
    timeframe?: string;
    industry?: string;
    region?: string;
  };
}

export interface DetectHistoricalCompanyResult {
  isHistorical: boolean;
  currentName?: string;
  historicalType: 'merger' | 'acquisition' | 'rebrand' | 'spinoff' | 'bankruptcy' | 'unknown';
  confidence: number;
  yearOfChange?: string;
  reasoning: string;
  alternativeSearches: string[];
  tokenUsage?: TokenUsage;
}

export interface ResolveMergerHistoryParams {
  companyNames: string[];
  timeframe?: {
    startYear?: number;
    endYear?: number;
  };
  industry?: string;
}

export interface ResolveMergerHistoryResult {
  mergerHistory: Array<{
    year?: string;
    event: string;
    companies: string[];
    resultingEntity: string;
  }>;
  currentEntity: string;
  confidence: number;
  searchSuggestions: string[];
  tokenUsage?: TokenUsage;
}

export interface HistoricalNameDetectionResult {
  isLikelyHistorical: boolean;
  currentNameSuggestions: string[];
  confidence: number;
  indicators: string[];
  variations: string[];
  historicalType?: 'merger' | 'rebrand' | 'spinoff' | 'unknown';
}

// Query Rewriting types
export interface GenerateSmartQueriesParams {
  companyName: string;
  context?: {
    industry?: string;
    previousFailures?: string[];
    userIntent?: string;
  };
}

export interface GenerateSmartQueriesResult {
  queries: Array<{
    query: string;
    confidence: number;
    type: 'exact' | 'abbreviation' | 'variation' | 'semantic' | 'industry_specific';
    reasoning: string;
  }>;
  industryContext?: string;
  commonAliases?: string[];
  tokenUsage?: TokenUsage;
}

export interface QueryRewritingResult {
  variations: string[];
  primaryVariation?: string;
  variationTypes: {
    punctuation: string[];
    abbreviation: string[];
    wordOrder: string[];
    spelling: string[];
  };
}

// Debug types
export interface TokenUsage {
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
}

export interface DebugInfo {
  aiCalls: number;
  esSearches: number;
  totalTokens: TokenUsage;
  aiCallDetails: Array<{
    function: string;
    tokens: TokenUsage;
    duration: number;
  }>;
  esSearchDetails: Array<{
    query: string;
    resultsFound: number;
    duration: number;
  }>;
}

// Type exports
export type CompanyMatchRequest = z.infer<typeof CompanyMatchRequestSchema>;
export type BatchMatchRequest = z.infer<typeof BatchMatchRequestSchema>;
export type CompanyMatchResponse = z.infer<typeof CompanyMatchResponseSchema>;
export type CompanyMatch = z.infer<typeof CompanyMatchSchema>;
