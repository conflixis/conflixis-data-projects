/**
 * Smart Query Rewriting Tool
 *
 * This module provides intelligent query rewriting capabilities:
 * - Generate alternative queries based on common variations
 * - Handle special characters, punctuation, and spacing
 * - Create word order permutations
 * - Expand common abbreviations
 * - Generate industry-specific variations
 */
import { generateObject } from 'ai';
import { z } from 'zod';
import { getModel } from '../config/ai';
import { config } from '../config/environment';
import { TokenUsage } from '../types';
import { extractAbbreviation, normalizeCompanyName } from '../utils/normalization';

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

// Common variations in company names
export const VARIATION_PATTERNS = {
  // Punctuation variations
  punctuation: [
    { pattern: /\s*&\s*/g, replacements: [' & ', ' and ', ' + ', ' '] },
    { pattern: /\s*\+\s*/g, replacements: [' + ', ' plus ', ' and ', ' '] },
    { pattern: /\s*-\s*/g, replacements: ['-', ' - ', ' ', ''] },
    { pattern: /\s*\.\s*/g, replacements: ['.', '. ', ' ', ''] },
    { pattern: /\s*,\s*/g, replacements: [', ', ' ', ''] },
    { pattern: /\s*'\s*/g, replacements: ["'", ' ', ''] },
  ],

  // Common abbreviations
  abbreviations: [
    { full: 'incorporated', abbr: 'inc' },
    { full: 'corporation', abbr: 'corp' },
    { full: 'limited', abbr: 'ltd' },
    { full: 'company', abbr: 'co' },
    { full: 'international', abbr: 'intl' },
    { full: 'technologies', abbr: 'tech' },
    { full: 'services', abbr: 'svcs' },
    { full: 'solutions', abbr: 'soln' },
    { full: 'management', abbr: 'mgmt' },
    { full: 'development', abbr: 'dev' },
    { full: 'manufacturing', abbr: 'mfg' },
    { full: 'pharmaceutical', abbr: 'pharma' },
  ],

  // Word variations
  wordVariations: [
    { words: ['center', 'centre'] },
    { words: ['theater', 'theatre'] },
    { words: ['color', 'colour'] },
    { words: ['labor', 'labour'] },
    { words: ['defense', 'defence'] },
    { words: ['tech', 'technology', 'technologies'] },
    { words: ['info', 'information'] },
    { words: ['comm', 'communication', 'communications'] },
  ],
};

/**
 * Generate punctuation variations
 */
export function generatePunctuationVariations(companyName: string): string[] {
  const variations = new Set<string>();

  for (const { pattern, replacements } of VARIATION_PATTERNS.punctuation) {
    if (pattern.test(companyName)) {
      for (const replacement of replacements) {
        const variant = companyName.replace(pattern, replacement).trim();
        if (variant !== companyName && variant.length > 2) {
          variations.add(variant);
        }
      }
    }
  }

  // Special handling for dots in abbreviations
  if (/[A-Z]\.[A-Z]/.test(companyName)) {
    // Remove dots from abbreviations (I.B.M. → IBM)
    variations.add(companyName.replace(/([A-Z])\.(?=[A-Z])/g, '$1'));
    // Add spaces between letters (I.B.M. → I B M)
    variations.add(companyName.replace(/([A-Z])\.(?=[A-Z])/g, '$1 '));
  }

  // Handle apostrophes in names
  if (companyName.includes("'")) {
    variations.add(companyName.replace(/'/g, ''));
    variations.add(companyName.replace(/'/g, 's'));
  }

  return Array.from(variations);
}

/**
 * Known stock ticker to company name mappings
 */
const TICKER_MAPPINGS: { [key: string]: string[] } = {
  MMM: ['3M', '3M Company', 'Minnesota Mining and Manufacturing'],
  AAPL: ['Apple', 'Apple Inc.'],
  MSFT: ['Microsoft', 'Microsoft Corporation'],
  GOOGL: ['Google', 'Alphabet'],
  GOOG: ['Google', 'Alphabet'],
  FB: ['Facebook', 'Meta', 'Meta Platforms'],
  META: ['Meta', 'Meta Platforms', 'Facebook'],
  AMZN: ['Amazon', 'Amazon.com'],
  JPM: ['JPMorgan Chase', 'JP Morgan', 'Chase'],
  JNJ: ['Johnson & Johnson', 'J&J'],
  PG: ['Procter & Gamble', 'P&G'],
  V: ['Visa'],
  MA: ['Mastercard'],
  CVX: ['Chevron'],
  XOM: ['Exxon Mobil', 'ExxonMobil'],
  BAC: ['Bank of America'],
  WMT: ['Walmart'],
  DIS: ['Disney', 'Walt Disney'],
  CSCO: ['Cisco', 'Cisco Systems'],
  VZ: ['Verizon'],
  T: ['AT&T'],
};

/**
 * Generate abbreviation variations
 */
export function generateAbbreviationVariations(companyName: string): string[] {
  const variations = new Set<string>();

  // Check if it's a known stock ticker
  const upperName = companyName.toUpperCase();
  if (TICKER_MAPPINGS[upperName]) {
    TICKER_MAPPINGS[upperName].forEach(name => variations.add(name));
  }

  // Expand known abbreviations
  for (const { full, abbr } of VARIATION_PATTERNS.abbreviations) {
    const abbrPattern = new RegExp(`\\b${abbr}\\b\\.?`, 'gi');
    const fullPattern = new RegExp(`\\b${full}\\b`, 'gi');

    if (abbrPattern.test(companyName)) {
      variations.add(companyName.replace(abbrPattern, full));
    }
    if (fullPattern.test(companyName)) {
      variations.add(companyName.replace(fullPattern, abbr));
      variations.add(companyName.replace(fullPattern, `${abbr}.`));
    }
  }

  // Generate acronym from multi-word names
  const words = companyName.split(/\s+/).filter(w => w.length > 2);
  if (words.length >= 2 && words.length <= 5) {
    const acronym = extractAbbreviation(companyName);
    if (acronym.length >= 2) {
      variations.add(acronym);
      variations.add(acronym.split('').join('.') + '.');
    }
  }

  // Handle specific patterns like "J&J" for "Johnson & Johnson"
  if (companyName.includes(' & ') || companyName.includes(' and ')) {
    const parts = companyName.split(/\s+(?:&|and)\s+/i);
    if (parts.length === 2) {
      const abbr = parts[0][0] + '&' + parts[1][0];
      variations.add(abbr.toUpperCase());
    }
  }

  return Array.from(variations);
}

/**
 * Generate word order variations
 */
export function generateWordOrderVariations(companyName: string): string[] {
  const variations = new Set<string>();
  const words = companyName.split(/\s+/);

  // Only generate permutations for 2-3 word names
  if (words.length === 2) {
    variations.add(`${words[1]} ${words[0]}`);
  } else if (words.length === 3) {
    // Common patterns for 3-word names
    variations.add(`${words[1]} ${words[2]} ${words[0]}`);
    variations.add(`${words[2]} ${words[0]} ${words[1]}`);

    // If middle word is "of", "for", etc., try without it
    const middleWord = words[1].toLowerCase();
    if (['of', 'for', 'and', '&', 'the'].includes(middleWord)) {
      variations.add(`${words[0]} ${words[2]}`);
      variations.add(`${words[2]} ${words[0]}`);
    }
  }

  // Handle "Company Name, Inc." → "Company Name" and vice versa
  const suffixPattern = /,?\s+(inc|corp|ltd|llc)\.?$/i;
  if (suffixPattern.test(companyName)) {
    variations.add(companyName.replace(suffixPattern, ''));
  } else {
    const commonSuffixes = ['Inc', 'Corp', 'LLC', 'Ltd'];
    for (const suffix of commonSuffixes) {
      variations.add(`${companyName}, ${suffix}.`);
      variations.add(`${companyName} ${suffix}`);
    }
  }

  return Array.from(variations).filter(v => v !== companyName);
}

/**
 * Generate spelling variations
 */
export function generateSpellingVariations(companyName: string): string[] {
  const variations = new Set<string>();

  // Apply word variations
  for (const { words } of VARIATION_PATTERNS.wordVariations) {
    for (const word of words) {
      const pattern = new RegExp(`\\b${word}\\b`, 'gi');
      if (pattern.test(companyName)) {
        for (const alternative of words) {
          if (alternative !== word) {
            variations.add(companyName.replace(pattern, alternative));
          }
        }
      }
    }
  }

  // Handle 's' vs 'z' variations (American vs British)
  if (companyName.includes('ization')) {
    variations.add(companyName.replace(/ization/g, 'isation'));
  }
  if (companyName.includes('isation')) {
    variations.add(companyName.replace(/isation/g, 'ization'));
  }

  // Handle common misspellings
  const commonMisspellings = [
    { correct: 'receive', wrong: 'recieve' },
    { correct: 'achieve', wrong: 'acheive' },
    { correct: 'believe', wrong: 'beleive' },
  ];

  for (const { correct, wrong } of commonMisspellings) {
    if (companyName.toLowerCase().includes(correct)) {
      variations.add(companyName.replace(new RegExp(correct, 'gi'), wrong));
    }
    if (companyName.toLowerCase().includes(wrong)) {
      variations.add(companyName.replace(new RegExp(wrong, 'gi'), correct));
    }
  }

  return Array.from(variations);
}

// AI Tool Schemas
const generateSmartQueriesSchema = z.object({
  queries: z.array(
    z.object({
      query: z.string(),
      confidence: z.number().min(0).max(1),
      type: z.enum(['exact', 'abbreviation', 'variation', 'semantic', 'industry_specific']),
      reasoning: z.string(),
    })
  ),
  industryContext: z.string().optional(),
  commonAliases: z.array(z.string()).optional(),
});

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

/**
 * AI tool to generate smart query variations
 */
export async function generateSmartQueries(
  params: GenerateSmartQueriesParams,
  model?: string
): Promise<GenerateSmartQueriesResult> {
  try {
    const prompt = `
You are an expert at generating search query variations for company names.

Generate smart query variations for: "${params.companyName}"

${
  params.context
    ? `
Context:
${params.context.industry ? `- Industry: ${params.context.industry}` : ''}
${params.context.previousFailures ? `- Already tried: ${params.context.previousFailures.join(', ')}` : ''}
${params.context.userIntent ? `- User intent: ${params.context.userIntent}` : ''}
`
    : ''
}

Generate query variations considering:
1. Common abbreviations and expansions
2. Industry-specific naming conventions
3. International variations (US vs UK spelling)
4. Common aliases and trade names
5. Formal vs informal names
6. Parent/subsidiary relationships

For each query, provide:
- The query string
- Confidence level (0.0-1.0)
- Type of variation
- Brief reasoning

Examples:
- "IBM" → "International Business Machines", "IBM Corporation"
- "J&J" → "Johnson & Johnson", "Johnson and Johnson", "JNJ"
- "GE" → "General Electric", "GE Company", "General Electric Company"
- "P&G" → "Procter & Gamble", "Procter and Gamble", "PG"

Prioritize variations that are most likely to yield results in a business database.`;

    const result = await generateObject({
      model: getModel(model),
      schema: generateSmartQueriesSchema,
      system:
        'You are a search query expert. Generate practical variations that would help find companies in databases.',
      prompt,
      temperature: config.openai.temperature,
    } as Parameters<typeof generateObject>[0]);

    const object = result.object as z.infer<typeof generateSmartQueriesSchema>;
    const usage = result.usage;

    return { ...object, tokenUsage: convertUsage(usage) };
  } catch (error) {
    console.error('Error in generateSmartQueries:', error);
    return {
      queries: [
        {
          query: params.companyName,
          confidence: 1.0,
          type: 'exact',
          reasoning: 'Fallback to original query due to AI error',
        },
      ],
    };
  }
}

/**
 * Generate all query variations
 */
export function generateQueryVariations(companyName: string, limit: number = 10): string[] {
  const allVariations = new Set<string>();

  // Add normalized version
  allVariations.add(normalizeCompanyName(companyName));

  // Add punctuation variations
  generatePunctuationVariations(companyName).forEach(v => allVariations.add(v));

  // Add abbreviation variations
  generateAbbreviationVariations(companyName).forEach(v => allVariations.add(v));

  // Add word order variations
  generateWordOrderVariations(companyName).forEach(v => allVariations.add(v));

  // Add spelling variations
  generateSpellingVariations(companyName).forEach(v => allVariations.add(v));

  // Remove the original name and empty strings
  allVariations.delete(companyName);
  allVariations.delete('');

  // Convert to array and limit
  return Array.from(allVariations)
    .filter(v => v.length > 2)
    .slice(0, limit);
}

/**
 * Main query rewriting interface
 */
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

/**
 * Rewrite query with smart variations
 */
export function rewriteQuery(companyName: string): QueryRewritingResult {
  const punctuation = generatePunctuationVariations(companyName);
  const abbreviation = generateAbbreviationVariations(companyName);
  const wordOrder = generateWordOrderVariations(companyName);
  const spelling = generateSpellingVariations(companyName);

  // Combine all variations
  const allVariations = new Set<string>([
    ...punctuation,
    ...abbreviation,
    ...wordOrder,
    ...spelling,
  ]);

  // Also add normalized version
  const normalized = normalizeCompanyName(companyName);
  if (normalized !== companyName) {
    allVariations.add(normalized);
  }

  const variations = Array.from(allVariations).filter(v => v.length > 2);

  // Determine primary variation (most likely to succeed)
  let primaryVariation: string | undefined;
  if (abbreviation.length > 0 && companyName.length <= 5) {
    // If input looks like abbreviation, try expanded form first
    primaryVariation = abbreviation.find(a => a.length > companyName.length);
  } else if (normalized !== companyName) {
    primaryVariation = normalized;
  } else if (punctuation.length > 0) {
    primaryVariation = punctuation[0];
  }

  return {
    variations,
    primaryVariation,
    variationTypes: {
      punctuation,
      abbreviation,
      wordOrder,
      spelling,
    },
  };
}
