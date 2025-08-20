/**
 * Historical Names and Merger Detection Tool
 *
 * This module provides tools to handle company name changes, mergers, and acquisitions:
 * - Detect if a company name might be outdated
 * - Identify merger and acquisition patterns
 * - Suggest current company names for historical ones
 * - Track rebranding and spin-offs
 */
import { generateObject } from 'ai';
import { z } from 'zod';
import { getModel } from '../config/ai';
import { config } from '../config/environment';
import { TokenUsage } from '../types';

// Helper function to convert AI SDK usage to our TokenUsage type
function convertUsage(usage: unknown): TokenUsage | undefined {
  if (!usage || typeof usage !== 'object') return undefined;

  const usageObj = usage as Record<string, unknown>;

  const promptTokens = Number(usageObj.promptTokens || usageObj.inputTokens || 0);
  const completionTokens = Number(usageObj.completionTokens || usageObj.outputTokens || 0);
  const totalTokens = Number(usageObj.totalTokens || promptTokens + completionTokens || 0);

  return {
    promptTokens,
    completionTokens,
    totalTokens,
  };
}

// Common patterns that indicate historical names or mergers
export const HISTORICAL_PATTERNS = [
  // Formerly patterns
  /(.+)\s*\(formerly\s+(.+)\)/i,
  /(.+)\s*\(previously\s+(.+)\)/i,
  /(.+)\s*\(f\.?k\.?a\.?\s+(.+)\)/i, // f.k.a. (formerly known as)
  /(.+)\s*\(now\s+(.+)\)/i,

  // Merger patterns
  /(.+)\s*\(merged\s+with\s+(.+)\)/i,
  /(.+)\s*\(acquired\s+by\s+(.+)\)/i,
  /(.+)\s*\(part\s+of\s+(.+)\)/i,

  // Pre/Post patterns
  /(.+)\s*\(pre-merger\)/i,
  /(.+)\s*\(post-merger\)/i,
  /(.+)\s*\(before\s+\d{4}\)/i,

  // Legacy patterns
  /legacy\s+(.+)/i,
  /old\s+(.+)/i,
  /former\s+(.+)/i,
];

// Common merger and acquisition indicators
export const MERGER_INDICATORS = {
  historical: [
    'formerly',
    'previously',
    'originally',
    'f.k.a.',
    'fka',
    'used to be',
    'was known as',
    'before',
    'legacy',
    'old',
  ],
  merger: [
    'merged',
    'merger',
    'acquired',
    'acquisition',
    'bought',
    'combined',
    'joined',
    'consolidated',
    'amalgamated',
  ],
  current: [
    'now',
    'currently',
    'became',
    'rebranded',
    'renamed',
    'changed to',
    'operating as',
    'doing business as',
  ],
  spinoff: [
    'spun off',
    'spin-off',
    'spinoff',
    'separated',
    'divested',
    'split from',
    'carved out',
    'demerged',
  ],
};

// Common company name evolution patterns
export const NAME_EVOLUTION_PATTERNS = [
  // Abbreviation to full name
  { from: /^([A-Z]{2,})\s*$/i, to: (match: string) => `Expand abbreviation: ${match}` },

  // Removal of geographic identifiers after merger
  { from: /(.+)\s+(North|South|East|West|Global|International)\s*$/i, to: '$1' },

  // Addition of "Group" or "Holdings" after merger
  { from: /^(.+?)(?:\s+(?:Inc|Corp|Ltd|LLC))?\s*$/i, to: '$1 Group' },
];

/**
 * Extract historical company names from patterns
 */
export function extractHistoricalNames(companyName: string): {
  current?: string;
  historical?: string;
  pattern: string;
} | null {
  for (const pattern of HISTORICAL_PATTERNS) {
    const match = companyName.match(pattern);
    if (match) {
      return {
        current: match[1]?.trim(),
        historical: match[2]?.trim(),
        pattern: pattern.source,
      };
    }
  }

  return null;
}

/**
 * Detect if a company name contains historical indicators
 */
export function detectHistoricalIndicators(companyName: string): {
  hasHistoricalIndicators: boolean;
  indicators: string[];
  confidence: number;
  possibleCurrentName?: string;
} {
  const lowerName = companyName.toLowerCase();
  const indicators: string[] = [];
  let possibleCurrentName: string | undefined;

  // Check for historical keywords
  for (const keyword of MERGER_INDICATORS.historical) {
    if (lowerName.includes(keyword)) {
      indicators.push(`historical: ${keyword}`);
    }
  }

  // Check for merger keywords
  for (const keyword of MERGER_INDICATORS.merger) {
    if (lowerName.includes(keyword)) {
      indicators.push(`merger: ${keyword}`);
    }
  }

  // Check for spinoff keywords
  for (const keyword of MERGER_INDICATORS.spinoff) {
    if (lowerName.includes(keyword)) {
      indicators.push(`spinoff: ${keyword}`);
    }
  }

  // Extract possible current name from patterns
  const extracted = extractHistoricalNames(companyName);
  if (extracted) {
    if (extracted.current) possibleCurrentName = extracted.current;
    indicators.push('matches historical pattern');
  }

  // Check if it starts with "Legacy" or "Old"
  if (lowerName.startsWith('legacy ') || lowerName.startsWith('old ')) {
    const cleaned = companyName.replace(/^(legacy|old)\s+/i, '').trim();
    if (!possibleCurrentName) possibleCurrentName = cleaned;
    indicators.push('legacy prefix');
  }

  const confidence = Math.min(indicators.length * 0.35, 1.0);

  return {
    hasHistoricalIndicators: indicators.length > 0,
    indicators,
    confidence,
    possibleCurrentName,
  };
}

// AI Tool Schemas
const detectHistoricalCompanySchema = z.object({
  isHistorical: z.boolean(),
  currentName: z.string().optional(),
  historicalType: z.enum(['merger', 'acquisition', 'rebrand', 'spinoff', 'bankruptcy', 'unknown']),
  confidence: z.number().min(0).max(1),
  yearOfChange: z.string().optional(),
  reasoning: z.string(),
  alternativeSearches: z.array(z.string()),
});

const resolveMergerHistorySchema = z.object({
  mergerHistory: z.array(
    z.object({
      year: z.string().optional(),
      event: z.string(),
      companies: z.array(z.string()),
      resultingEntity: z.string(),
    })
  ),
  currentEntity: z.string(),
  confidence: z.number().min(0).max(1),
  searchSuggestions: z.array(z.string()),
});

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

/**
 * AI tool to detect if a company name is historical
 */
export async function detectHistoricalCompany(
  params: DetectHistoricalCompanyParams,
  model?: string
): Promise<DetectHistoricalCompanyResult> {
  try {
    const prompt = `
You are an expert in corporate history, mergers, acquisitions, and company name changes.

Analyze if this company name might be outdated or historical:
Company Name: "${params.companyName}"

${
  params.context
    ? `
Context:
${params.context.timeframe ? `- Timeframe: ${params.context.timeframe}` : ''}
${params.context.industry ? `- Industry: ${params.context.industry}` : ''}
${params.context.region ? `- Region: ${params.context.region}` : ''}
`
    : ''
}

Consider:
1. Major mergers and acquisitions (e.g., Exxon + Mobil → ExxonMobil)
2. Rebranding efforts (e.g., Facebook → Meta, Google → Alphabet)
3. Bankruptcies and restructurings (e.g., Lehman Brothers, Enron)
4. Spin-offs and divestitures (e.g., PayPal from eBay, Kyndryl from IBM)
5. Regional consolidations (e.g., various Bell companies → AT&T)

Provide:
- isHistorical: Whether this appears to be an outdated name
- currentName: The current company name (if known)
- historicalType: Type of change that occurred
- confidence: Your confidence level (0.0-1.0)
- yearOfChange: Approximate year of the change (if known)
- reasoning: Clear explanation
- alternativeSearches: List of names to search for (current name, variations, related entities)

Recent notable changes to be aware of:
- Twitter → X (2023)
- Facebook → Meta (2021)
- Weight Watchers → WW (2018)
- Tribune Publishing → Tronc → Tribune Publishing (2016-2018)`;

    const result = await generateObject({
      model: getModel(model),
      schema: detectHistoricalCompanySchema,
      system:
        'You are a corporate history expert with knowledge of mergers, acquisitions, and rebranding.',
      prompt,
      temperature: config.openai.temperature,
    } as Parameters<typeof generateObject>[0]);

    const object = result.object as z.infer<typeof detectHistoricalCompanySchema>;
    const usage = result.usage;

    return { ...object, tokenUsage: convertUsage(usage) };
  } catch (error) {
    console.error('Error in detectHistoricalCompany:', error);
    return {
      isHistorical: false,
      historicalType: 'unknown',
      confidence: 0,
      reasoning: 'Unable to analyze due to AI service error',
      alternativeSearches: [],
    };
  }
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

/**
 * AI tool to resolve complex merger histories
 */
export async function resolveMergerHistory(
  params: ResolveMergerHistoryParams,
  model?: string
): Promise<ResolveMergerHistoryResult> {
  try {
    const prompt = `
You are an expert in corporate mergers, acquisitions, and company evolution.

Analyze the merger history for these companies:
Companies: ${params.companyNames.map(name => `"${name}"`).join(', ')}

${
  params.timeframe
    ? `
Timeframe: ${params.timeframe.startYear || 'earliest'} to ${params.timeframe.endYear || 'present'}
`
    : ''
}
${params.industry ? `Industry: ${params.industry}` : ''}

Provide:
1. A chronological history of mergers, acquisitions, and name changes
2. The current entity name that resulted from these changes
3. Search suggestions for finding the company in modern databases

Consider:
- Mergers of equals (e.g., Daimler-Benz + Chrysler)
- Acquisitions where the name changed (e.g., SBC acquired AT&T but took the AT&T name)
- Multiple rounds of consolidation (e.g., banking industry consolidations)
- Failed mergers that were later unwound
- Spin-offs that became independent again

Focus on major, well-documented corporate events.`;

    const result = await generateObject({
      model: getModel(model),
      schema: resolveMergerHistorySchema,
      system:
        'You are a corporate history expert. Provide accurate historical information about mergers and acquisitions.',
      prompt,
      temperature: config.openai.temperature,
    } as Parameters<typeof generateObject>[0]);

    const object = result.object as z.infer<typeof resolveMergerHistorySchema>;
    const usage = result.usage;

    return { ...object, tokenUsage: convertUsage(usage) };
  } catch (error) {
    console.error('Error in resolveMergerHistory:', error);
    return {
      mergerHistory: [],
      currentEntity: params.companyNames[0],
      confidence: 0,
      searchSuggestions: params.companyNames,
    };
  }
}

/**
 * Generate variations based on historical patterns
 */
export function generateHistoricalVariations(companyName: string): string[] {
  const variations = new Set<string>();

  // Remove historical indicators
  for (const keyword of [...MERGER_INDICATORS.historical, ...MERGER_INDICATORS.merger]) {
    const pattern = new RegExp(`\\b${keyword}\\b`, 'gi');
    const cleaned = companyName.replace(pattern, '').replace(/\s+/g, ' ').trim();
    if (cleaned !== companyName && cleaned.length > 3) {
      variations.add(cleaned);
    }
  }

  // Remove parenthetical historical information
  const withoutParens = companyName.replace(/\s*\([^)]*\)\s*/g, ' ').trim();
  if (withoutParens !== companyName && withoutParens.length > 3) {
    variations.add(withoutParens);
  }

  // Extract from "X formerly Y" patterns
  const formerlyMatch = companyName.match(/(.+?)\s+formerly\s+(.+)/i);
  if (formerlyMatch) {
    variations.add(formerlyMatch[1].trim());
    variations.add(formerlyMatch[2].trim());
  }

  // Try adding/removing common suffixes that change in mergers
  const baseName = companyName
    .replace(/\s+(Group|Holdings|Worldwide|Global|International)$/i, '')
    .trim();
  if (baseName !== companyName) {
    variations.add(baseName);
  } else {
    // Try adding these if not present
    variations.add(`${companyName} Group`);
    variations.add(`${companyName} Holdings`);
  }

  // Handle "X and Y" or "X & Y" merger patterns
  const andPattern = companyName.match(/(.+?)\s+(?:and|&)\s+(.+)/i);
  if (andPattern) {
    variations.add(andPattern[1].trim());
    variations.add(andPattern[2].trim());
    // Also try the reverse order
    variations.add(`${andPattern[2].trim()} ${andPattern[1].trim()}`);
  }

  return Array.from(variations).filter(v => v !== companyName && v.length > 2);
}

/**
 * Main interface for historical name detection
 */
export interface HistoricalNameDetectionResult {
  isLikelyHistorical: boolean;
  currentNameSuggestions: string[];
  confidence: number;
  indicators: string[];
  variations: string[];
  historicalType?: 'merger' | 'rebrand' | 'spinoff' | 'unknown';
}

/**
 * Detect historical names and suggest current alternatives
 */
export function detectHistoricalName(companyName: string): HistoricalNameDetectionResult {
  const { hasHistoricalIndicators, indicators, confidence, possibleCurrentName } =
    detectHistoricalIndicators(companyName);

  const variations = generateHistoricalVariations(companyName);
  const currentNameSuggestions: string[] = [];

  if (possibleCurrentName) {
    currentNameSuggestions.push(possibleCurrentName);
  }

  // Add variations as possible current names
  for (const variation of variations) {
    if (!currentNameSuggestions.includes(variation)) {
      currentNameSuggestions.push(variation);
    }
  }

  // Determine historical type based on indicators
  let historicalType: 'merger' | 'rebrand' | 'spinoff' | 'unknown' = 'unknown';
  if (indicators.some(i => i.includes('merger'))) {
    historicalType = 'merger';
  } else if (indicators.some(i => i.includes('spinoff'))) {
    historicalType = 'spinoff';
  } else if (indicators.some(i => i.includes('historical') || i.includes('legacy'))) {
    historicalType = 'rebrand';
  }

  return {
    isLikelyHistorical: hasHistoricalIndicators,
    currentNameSuggestions,
    confidence,
    indicators,
    variations,
    historicalType,
  };
}
